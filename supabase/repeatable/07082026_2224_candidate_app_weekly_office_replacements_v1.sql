-- CloudTMS Candidate App existing weekly/office authority replacements v1.

-- Complete replacement definitions extracted mechanically from the reviewed current source.

-- Install only after the Candidate App foundation/evidence migrations and private helpers.

CREATE OR REPLACE FUNCTION public.contract_week_manual_draft_upsert_atomic_v1(
  p_week_id uuid,
  p_expected_row_signature text,
  p_totals_json jsonb,
  p_planned_schedule_json jsonb DEFAULT NULL::jsonb,
  p_replace_planned_schedule boolean DEFAULT false,
  p_force_adjustment boolean DEFAULT false,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_week public.contract_weeks%ROWTYPE;
  v_before_signature_json jsonb;
  v_after_signature_json jsonb;
  v_current_row_signature text;
  v_after_row_signature text;
  v_expected_row_signature text := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  v_status text;
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_candidate_guard jsonb := '{}'::jsonb;
BEGIN
  IF p_week_id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object('field', 'p_week_id')::text;
  END IF;

  IF v_expected_row_signature IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  IF p_totals_json IS NULL OR jsonb_typeof(p_totals_json) <> 'object' THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object('field', 'p_totals_json', 'reason', 'object_required')::text;
  END IF;

  IF COALESCE(p_replace_planned_schedule, false)
     AND (p_planned_schedule_json IS NULL OR jsonb_typeof(p_planned_schedule_json) <> 'array') THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object('field', 'p_planned_schedule_json', 'reason', 'array_required')::text;
  END IF;

  SELECT cw.*
    INTO v_week
  FROM public.contract_weeks AS cw
  WHERE cw.id = p_week_id
  FOR UPDATE;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_NOT_FOUND',
      DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  IF v_week.timesheet_id IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_MOVED',
      DETAIL = jsonb_build_object(
        'contract_week_id', v_week.id,
        'current_timesheet_id', v_week.timesheet_id
      )::text;
  END IF;

  v_status := UPPER(BTRIM(COALESCE(v_week.status::text, '')));
  IF v_status NOT IN ('PLANNED', 'OPEN') THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_DRAFT_NOT_EDITABLE',
      DETAIL = jsonb_build_object(
        'contract_week_id', v_week.id,
        'status', v_status
      )::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(
    NULL::uuid,
    v_week.id,
    false
  );
  v_current_row_signature := NULLIF(BTRIM(COALESCE(
    v_before_signature_json ->> 'backend_row_signature',
    v_before_signature_json ->> 'row_signature',
    v_before_signature_json ->> 'signature',
    ''
  )), '');

  IF v_current_row_signature IS NULL
     OR v_current_row_signature IS DISTINCT FROM v_expected_row_signature THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object(
        'contract_week_id', v_week.id,
        'expected_row_signature', v_expected_row_signature,
        'current_row_signature', v_current_row_signature
      )::text;
  END IF;

  v_candidate_guard := private._candidate_draft_totals_guard_v1(v_week.id,p_totals_json);

  UPDATE public.contract_weeks AS cw
  SET totals_json = p_totals_json,
      planned_schedule_json = CASE
        WHEN COALESCE(p_replace_planned_schedule, false) THEN p_planned_schedule_json
        ELSE cw.planned_schedule_json
      END,
      is_adjustment = CASE
        WHEN COALESCE(p_force_adjustment, false) THEN true
        ELSE cw.is_adjustment
      END,
      updated_at = v_now
  WHERE cw.id = v_week.id
    AND cw.timesheet_id IS NULL
  RETURNING cw.* INTO v_week;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_MOVED',
      DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(
    NULL::uuid,
    v_week.id,
    false
  );
  v_after_row_signature := NULLIF(BTRIM(COALESCE(
    v_after_signature_json ->> 'backend_row_signature',
    v_after_signature_json ->> 'row_signature',
    v_after_signature_json ->> 'signature',
    ''
  )), '');

  IF v_after_row_signature IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'PLANNED_CONTRACT_WEEK_SIGNATURE_MISSING',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id)::text;
  END IF;

  RETURN to_jsonb(v_week) || jsonb_build_object(
    'ok', true,
    'updated', true,
    'contract_week_id', v_week.id,
    'current_timesheet_id', NULL,
    'backend_row_signature', v_after_row_signature,
    'mutation_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'expected_row_signature', v_after_row_signature,
    'planned_contract_week_authority_complete', true,
    'planned_contract_week_authority_contract_week_id', v_week.id,
    'refresh_required', false,
    'affected_rows', jsonb_build_array(jsonb_build_object(
      'row_key', 'contract_week:' || v_week.id::text,
      'contract_week_id', v_week.id,
      'timesheet_id', NULL,
      'backend_row_signature', v_after_row_signature,
      'row_signature', v_after_row_signature,
      'planned_contract_week_authority_complete', true
    ))
  ) || CASE
    WHEN private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') THEN
      jsonb_build_object(
        'candidate_record_role',v_candidate_guard->>'record_role',
        'candidate_final_state_guard',v_candidate_guard)
    ELSE '{}'::jsonb END;
END;
$function$;

CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_atomic(p_week_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_timesheet_create_json jsonb DEFAULT NULL::jsonb, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_rotation_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_materialise_staged_evidence boolean DEFAULT true, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text, p_queue_timesheet_materialisation_json jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_week public.contract_weeks%ROWTYPE;
  v_contract public.contracts%ROWTYPE;
  v_pointer_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_create_json jsonb := CASE WHEN p_timesheet_create_json IS NULL THEN NULL WHEN jsonb_typeof(p_timesheet_create_json) = 'object' THEN p_timesheet_create_json ELSE NULL END;
  v_patch_json jsonb := CASE WHEN p_timesheet_patch_json IS NULL THEN '{}'::jsonb WHEN jsonb_typeof(p_timesheet_patch_json) = 'object' THEN p_timesheet_patch_json ELSE NULL END;
  v_week_patch_json jsonb := CASE WHEN p_contract_week_patch_json IS NULL THEN '{}'::jsonb WHEN jsonb_typeof(p_contract_week_patch_json) = 'object' THEN p_contract_week_patch_json ELSE NULL END;
  v_tsfin_snapshot_json jsonb := CASE WHEN p_tsfin_snapshot_json IS NULL THEN NULL WHEN jsonb_typeof(p_tsfin_snapshot_json) = 'object' THEN p_tsfin_snapshot_json ELSE NULL END;
  v_rotation_json jsonb := CASE WHEN p_rotation_json IS NULL THEN NULL WHEN jsonb_typeof(p_rotation_json) = 'object' THEN p_rotation_json ELSE NULL END;
  v_queue_timesheet_materialisation_json jsonb := CASE WHEN p_queue_timesheet_materialisation_json IS NULL THEN NULL WHEN jsonb_typeof(p_queue_timesheet_materialisation_json) = 'object' THEN p_queue_timesheet_materialisation_json ELSE NULL END;
  v_suppress_timesheet_evidence_materialisation boolean := false;
  v_has_selected_queue_timesheet_materialisation boolean := false;
  v_create_rec public.timesheets%ROWTYPE;
  v_patch_rec public.timesheets%ROWTYPE;
  v_week_patch_rec public.contract_weeks%ROWTYPE;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_created_now boolean := false;
  v_was_stale boolean := false;
  v_previous_contract_week_status text := NULL;
  v_previous_processing_status text := NULL;
  v_segment_invoice_lock boolean := false;
  v_rotation_action text := NULL;
  v_rotation_new_timesheet_id uuid := NULL;
  v_rotation_pending_qr boolean := false;
  v_rotation_revoke_reason text := NULL;
  v_next_version integer := NULL;
  v_rotated_ts public.timesheets%ROWTYPE;
  v_original_booking_id text := NULL;
  v_existing_same_booking public.timesheets%ROWTYPE;
  v_queue_item public.manual_timesheet_queue%ROWTYPE;
  v_queue_kind text := NULL;
  v_queue_storage_key text := NULL;
  v_primary_timesheet_storage_key text := NULL;
  v_primary_timesheet_rotation_raw integer := 0;
  v_primary_timesheet_rotation_deg integer := 0;
  v_primary_timesheet_queue_id uuid := NULL;
  v_timesheet_stage_key_count integer := 0;
  v_attached_evidence_count integer := 0;
  v_attached_queue_count integer := 0;
  v_duplicate_queue_count integer := 0;
  v_selected_queue_id_text text := NULL;
  v_selected_queue_id uuid := NULL;
  v_selected_queue_storage_key text := NULL;
  v_selected_queue_contract_week_text text := NULL;
  v_existing_timesheet_evidence_conflict_count integer := 0;
  v_tsfin_result jsonb := '{}'::jsonb;
  v_summary_refresh_result jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_error_constraint text := NULL;
  v_temp_log_enabled boolean := false;
  v_signature_diag_json jsonb := '{}'::jsonb;
  v_diag_started_at timestamp with time zone := clock_timestamp();
  v_diag_stage_started_at timestamp with time zone := clock_timestamp();
  v_diag_elapsed_ms numeric := 0;
  v_diag_duration_ms numeric := 0;
  v_diag_step_index integer := 0;
  v_staged_evidence_loop_count integer := 0;
  v_candidate_final_state_guard jsonb := '{}'::jsonb;
  v_candidate_route_guard jsonb := '{}'::jsonb;
  v_candidate_capability_guard jsonb := '{}'::jsonb;
  v_candidate_workflow_id uuid := NULL;
  v_candidate_workflow_kind text := NULL;
  v_candidate_workflow_route text := NULL;
  v_candidate_electronic_context boolean :=
    COALESCE(current_setting('cloudtms.candidate_electronic_finalise', true), '') <> ''
    AND private._candidate_feature_enabled_current_v1('candidate_app_writes');
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);
  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'manual_timesheet_save', true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);
  PERFORM set_config('cloudtms.summary_refresh_mode', 'ordinary_manual_save_lightweight', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', COALESCE(p_expected_timesheet_id::text, ''), true);

  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  IF p_week_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_week_id')::text;
  END IF;
  IF p_timesheet_patch_json IS NOT NULL AND v_patch_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_patch_json')::text;
  END IF;
  IF p_contract_week_patch_json IS NOT NULL AND v_week_patch_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_contract_week_patch_json')::text;
  END IF;
  IF p_timesheet_create_json IS NOT NULL AND v_create_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_create_json')::text;
  END IF;
  IF NOT v_candidate_electronic_context AND (
       COALESCE(v_create_json,'{}'::jsonb) ?| ARRAY[
         'candidate_workflow_id','candidate_workflow_generation','candidate_manager_approved_at_utc']
       OR COALESCE(v_patch_json,'{}'::jsonb) ?| ARRAY[
         'candidate_workflow_id','candidate_workflow_generation','candidate_manager_approved_at_utc']
     ) THEN
    RAISE EXCEPTION USING MESSAGE='CANDIDATE_FINALISE_CONTEXT_REQUIRED';
  END IF;
  IF p_tsfin_snapshot_json IS NOT NULL AND v_tsfin_snapshot_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_tsfin_snapshot_json')::text;
  END IF;
  IF p_rotation_json IS NOT NULL AND v_rotation_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_rotation_json')::text;
  END IF;
  IF p_queue_timesheet_materialisation_json IS NOT NULL AND v_queue_timesheet_materialisation_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_queue_timesheet_materialisation_json')::text;
  END IF;

  IF v_queue_timesheet_materialisation_json IS NOT NULL THEN
    v_suppress_timesheet_evidence_materialisation := LOWER(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'suppress_timesheet_evidence_materialisation', v_queue_timesheet_materialisation_json ->> 'suppressTimesheetEvidenceMaterialisation', ''))) IN ('true','1','yes','y','on');
    v_has_selected_queue_timesheet_materialisation := NOT v_suppress_timesheet_evidence_materialisation
      AND NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'queue_id', v_queue_timesheet_materialisation_json ->> 'queueId', '')), '') IS NOT NULL
      AND NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storageKey', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'r2_key', '')), ''), ''), '^/+', ''), '') IS NOT NULL;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'entry_payload_validated',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'has_create_json', v_create_json IS NOT NULL,
          'has_patch_json', v_patch_json <> '{}'::jsonb,
          'has_week_patch_json', v_week_patch_json <> '{}'::jsonb,
          'has_tsfin_snapshot_json', v_tsfin_snapshot_json IS NOT NULL,
          'has_rotation_json', v_rotation_json IS NOT NULL,
          'materialise_staged_evidence', p_materialise_staged_evidence,
          'has_queue_timesheet_materialisation_json', v_queue_timesheet_materialisation_json IS NOT NULL,
          'suppress_timesheet_evidence_materialisation', v_suppress_timesheet_evidence_materialisation,
          'has_selected_queue_timesheet_materialisation', v_has_selected_queue_timesheet_materialisation,
          'expected_row_signature_present', NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NOT NULL
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  SELECT cw.*
    INTO v_week
  FROM public.contract_weeks AS cw
  WHERE cw.id = p_week_id
  FOR UPDATE;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;
  v_previous_contract_week_status := v_week.status::text;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_week_locked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_id', v_week.contract_id,
          'week_status', v_week.status::text,
          'contract_week_timesheet_id', v_week.timesheet_id,
          'week_ending_date', v_week.week_ending_date,
          'previous_contract_week_status', v_previous_contract_week_status
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('contract_week_staged_timesheet:' || v_week.id::text));

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'staged_timesheet_advisory_lock_acquired',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'advisory_lock_scope', 'contract_week_staged_timesheet',
          'contract_id', v_week.contract_id,
          'week_status', v_week.status::text
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  SELECT c.*
    INTO v_contract
  FROM public.contracts AS c
  WHERE c.id = v_week.contract_id
  FOR UPDATE;

  IF v_contract.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_id', v_week.contract_id)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_locked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_id', v_contract.id,
          'candidate_id', v_contract.candidate_id,
          'client_id', v_contract.client_id,
          'pay_method_snapshot', v_contract.pay_method_snapshot,
          'default_submission_mode', v_contract.default_submission_mode
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_week.status IN ('AUTHORISED'::public.contract_week_status_enum, 'INVOICED'::public.contract_week_status_enum, 'CANCELLED'::public.contract_week_status_enum) THEN
    RAISE EXCEPTION USING
      MESSAGE = CASE WHEN v_week.status = 'INVOICED'::public.contract_week_status_enum THEN 'INVOICED_OR_LOCKED' ELSE 'TIMESHEET_LOCKED_OR_PAID' END,
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'contract_week_status', v_week.status::text)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_week_status_gate_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_week_status', v_week.status::text,
          'locked_status_gate_passed', true
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_week.timesheet_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_pointer_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = v_week.timesheet_id
    FOR UPDATE;

    IF v_pointer_ts.timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'timesheet_id', v_week.timesheet_id)::text;
    END IF;

    IF COALESCE(v_pointer_ts.is_current, false) THEN
      v_current_ts := v_pointer_ts;
    ELSE
      SELECT ts.*
        INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_pointer_ts.booking_id
        AND ts.is_current = true
      ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;
      IF v_current_ts.timesheet_id IS NULL THEN
        v_current_ts := v_pointer_ts;
      ELSE
        v_was_stale := true;
      END IF;
    END IF;

    IF p_expected_timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
    END IF;
    IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
      RAISE EXCEPTION USING
        MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH',
        DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
    END IF;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'current_timesheet_resolved',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_week_pointer_timesheet_id', v_week.timesheet_id,
          'pointer_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id END,
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'booking_id', CASE WHEN v_current_ts.booking_id IS NULL THEN NULL ELSE v_current_ts.booking_id END,
          'timesheet_version', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.version END,
          'timesheet_is_current', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.is_current END,
          'was_stale', v_was_stale
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    IF v_current_ts.archived_at_utc IS NOT NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
    END IF;
    IF v_current_ts.authorised_at_server IS NOT NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
    END IF;

    SELECT tf.*
      INTO v_current_tsfin
    FROM public.timesheets_financials AS tf
    WHERE tf.timesheet_id = v_current_ts.timesheet_id
      AND tf.is_current = true
    ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_current_tsfin.id IS NOT NULL THEN
      v_previous_processing_status := v_current_tsfin.processing_status::text;
      IF v_current_tsfin.locked_by_invoice_id IS NOT NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'invoice_id', v_current_tsfin.locked_by_invoice_id)::text;
      END IF;

      SELECT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment_json)
        WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) INTO v_segment_invoice_lock;

      IF COALESCE(v_segment_invoice_lock, false) THEN
        RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'lock_scope', 'segment')::text;
      END IF;
    END IF;
  END IF;

  IF v_candidate_electronic_context THEN
    BEGIN
      v_candidate_workflow_id := split_part(
        current_setting('cloudtms.candidate_electronic_finalise', true),
        ':',
        1
      )::uuid;
    EXCEPTION WHEN invalid_text_representation THEN
      RAISE EXCEPTION 'CANDIDATE_FINALISE_CONTEXT_INVALID' USING ERRCODE='42501';
    END;

    SELECT upper(w.workflow_kind),upper(w.route)
      INTO v_candidate_workflow_kind,v_candidate_workflow_route
    FROM public.candidate_submission_workflows AS w
    WHERE w.id=v_candidate_workflow_id
      AND w.contract_week_id=v_week.id
      AND w.state IN ('READY_TO_FINALISE','RECEIVED')
    FOR SHARE;

    IF NOT FOUND OR v_candidate_workflow_kind NOT IN ('CONTRACT_HOURS','CONTRACT_COMBINED') THEN
      RAISE EXCEPTION 'CANDIDATE_FINALISE_WORKFLOW_INVALID' USING ERRCODE='42501';
    END IF;

    v_candidate_route_guard := private._candidate_route_family_v1(
      CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      v_week.id
    );
    v_candidate_capability_guard := private._candidate_record_capabilities_v1(
      CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      v_week.id
    );

    IF NOT COALESCE((v_candidate_capability_guard->>'can_edit_hours')::boolean,false)
       OR v_candidate_route_guard->>'route_family' IN ('IMPORT_AUTHORITATIVE','MANUAL_NON_QR')
       OR (v_candidate_workflow_route='ELECTRONIC'
           AND v_candidate_route_guard->>'route_family'<>'ELECTRONIC')
       OR (v_candidate_workflow_route='PAPER'
           AND NOT COALESCE(
             (v_candidate_route_guard->>'candidate_paper_submission_allowed')::boolean,
             false
           )) THEN
      RAISE EXCEPTION 'CANDIDATE_ROUTE_NOT_ALLOWED'
        USING ERRCODE='42501',
              DETAIL=jsonb_build_object(
                'workflow_id',v_candidate_workflow_id,
                'workflow_route',v_candidate_workflow_route,
                'route_family',v_candidate_route_guard->>'route_family'
              )::text;
    END IF;
  END IF;

  v_candidate_final_state_guard := private._candidate_weekly_final_state_guard_v1(
    v_week.id,
    CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
    v_create_json,
    v_patch_json,
    v_tsfin_snapshot_json
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'current_tsfin_and_lock_gate_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'current_tsfin_id', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.id END,
          'previous_processing_status', v_previous_processing_status,
          'processing_status', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.processing_status::text END,
          'paid_at_utc', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.paid_at_utc END,
          'locked_by_invoice_id', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.locked_by_invoice_id END,
          'segment_invoice_lock', v_segment_invoice_lock
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'before_signature_generation_started',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'expected_row_signature_input', NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), ''),
          'patch_backend_row_signature', NULLIF(BTRIM(COALESCE(v_patch_json ->> 'backend_row_signature', '')), ''),
          'patch_row_signature', NULLIF(BTRIM(COALESCE(v_patch_json ->> 'row_signature', '')), ''),
          'week_patch_backend_row_signature', NULLIF(BTRIM(COALESCE(v_week_patch_json ->> 'backend_row_signature', '')), ''),
          'week_patch_row_signature', NULLIF(BTRIM(COALESCE(v_week_patch_json ->> 'row_signature', '')), '')
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END, v_week.id, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, v_patch_json ->> 'backend_row_signature', v_patch_json ->> 'row_signature', v_week_patch_json ->> 'backend_row_signature', v_week_patch_json ->> 'row_signature', '')), '');

  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, v_week.id::text),
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'row_signature_mismatch_before_manual_upsert',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3),
          'duration_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3),
          'contract_week_id', v_week.id,
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json
        ))
      );
    END IF;
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'contract_week_id', v_week.id, 'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'row_signature_guard_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'current_row_signature', v_current_row_signature,
          'expected_row_signature', v_expected_row_signature,
          'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
          'signature_match', CASE WHEN v_expected_row_signature IS NULL THEN true ELSE COALESCE(v_current_row_signature, '') IS NOT DISTINCT FROM v_expected_row_signature END
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_current_ts.timesheet_id IS NULL THEN
    IF v_create_json IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_create_json', 'reason', 'required_when_no_current_timesheet')::text;
    END IF;

    v_create_rec := jsonb_populate_record(NULL::public.timesheets, v_create_json);
    IF NULLIF(BTRIM(COALESCE(v_create_rec.booking_id, '')), '') IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'booking_id')::text;
    END IF;
    IF COALESCE(v_create_rec.week_ending_date, v_week.week_ending_date) IS DISTINCT FROM v_week.week_ending_date THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'week_ending_date', 'expected_value', v_week.week_ending_date)::text;
    END IF;
    IF COALESCE(v_create_rec.contract_id, v_week.contract_id) IS DISTINCT FROM v_week.contract_id THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'contract_id', 'expected_value', v_week.contract_id)::text;
    END IF;

    v_original_booking_id := NULLIF(BTRIM(v_create_rec.booking_id), '');
    PERFORM pg_advisory_xact_lock(hashtext(v_original_booking_id));

    SELECT ts.*
      INTO v_existing_same_booking
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_original_booking_id
      AND ts.contract_id = v_week.contract_id
      AND ts.week_ending_date = v_week.week_ending_date
      AND ts.is_current = true
    ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_existing_same_booking.timesheet_id IS NOT NULL THEN
      v_current_ts := v_existing_same_booking;
      v_was_stale := true;
      v_patch_json := (v_create_json - 'timesheet_id' - 'booking_id' - 'version' - 'is_current' - 'contract_id' - 'week_ending_date' - 'created_at') || COALESCE(v_patch_json, '{}'::jsonb);
    ELSE
      SELECT COALESCE(MAX(ts.version), 0) + 1
        INTO v_create_rec.version
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_original_booking_id;

      INSERT INTO public.timesheets (
        timesheet_id,
        booking_id,
        occupant_key_norm,
        hospital_norm,
        ward_norm,
        job_title_norm,
        shift_label_norm,
        week_ending_date,
        authorised_at_server,
        auth_name,
        auth_job_title,
        r2_nurse_key,
        r2_auth_key,
        img_sha256_nurse,
        img_sha256_auth,
        candidate_workflow_id,
        candidate_workflow_generation,
        candidate_manager_approved_at_utc,
        status,
        created_at,
        updated_at,
        version,
        is_current,
        contract_id,
        submission_mode,
        manual_pdf_r2_key,
        line_type,
        sheet_scope,
        actual_schedule_json,
        additional_units_week,
        additional_units_per_day,
        qr_token,
        qr_status,
        qr_payload_json,
        qr_generated_at,
        qr_scanned_at,
        qr_scan_info_json,
        qr_r2_key,
        day_references_json,
        manual_pdf_rotation_degrees,
        qr_last_sent_hash,
        qr_last_sent_at_utc,
        qr_signed_hash,
        qr_signed_at_utc,
        candidate_hint_text,
        band,
        is_adjustment
      )
      VALUES (
        COALESCE(v_create_rec.timesheet_id, gen_random_uuid()),
        v_original_booking_id,
        COALESCE(v_create_rec.occupant_key_norm, ''),
        COALESCE(v_create_rec.hospital_norm, ''),
        COALESCE(v_create_rec.ward_norm, ''),
        COALESCE(v_create_rec.job_title_norm, ''),
        COALESCE(v_create_rec.shift_label_norm, 'weekly'),
        v_week.week_ending_date,
        v_create_rec.authorised_at_server,
        v_create_rec.auth_name,
        v_create_rec.auth_job_title,
        v_create_rec.r2_nurse_key,
        v_create_rec.r2_auth_key,
        v_create_rec.img_sha256_nurse,
        v_create_rec.img_sha256_auth,
        v_create_rec.candidate_workflow_id,
        v_create_rec.candidate_workflow_generation,
        v_create_rec.candidate_manager_approved_at_utc,
        COALESCE(v_create_rec.status, 'RECEIVED'::public.timesheet_status_enum),
        COALESCE(v_create_rec.created_at, v_now),
        v_now,
        COALESCE(v_create_rec.version, 1),
        true,
        v_week.contract_id,
        COALESCE(v_create_rec.submission_mode, 'MANUAL'::public.submission_mode_enum),
        v_create_rec.manual_pdf_r2_key,
        COALESCE(v_create_rec.line_type, 'HOURS'::public.timesheet_line_type_enum),
        COALESCE(v_create_rec.sheet_scope, 'WEEKLY'::public.timesheet_scope_enum),
        COALESCE(v_create_rec.actual_schedule_json, '[]'::jsonb),
        COALESCE(v_create_rec.additional_units_week, '{}'::jsonb),
        COALESCE(v_create_rec.additional_units_per_day, '{}'::jsonb),
        v_create_rec.qr_token,
        v_create_rec.qr_status,
        COALESCE(v_create_rec.qr_payload_json, '{}'::jsonb),
        v_create_rec.qr_generated_at,
        v_create_rec.qr_scanned_at,
        v_create_rec.qr_scan_info_json,
        v_create_rec.qr_r2_key,
        v_create_rec.day_references_json,
        COALESCE(v_create_rec.manual_pdf_rotation_degrees, 0),
        v_create_rec.qr_last_sent_hash,
        v_create_rec.qr_last_sent_at_utc,
        v_create_rec.qr_signed_hash,
        v_create_rec.qr_signed_at_utc,
        v_create_rec.candidate_hint_text,
        v_create_rec.band,
        COALESCE(v_create_rec.is_adjustment, COALESCE(v_week.is_adjustment, false))
      )
      RETURNING * INTO v_current_ts;
      v_created_now := true;
    END IF;
  ELSE
    IF v_rotation_json IS NOT NULL THEN
      IF p_actor_user_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id', 'reason', 'required_for_rotation')::text;
      END IF;
      v_rotation_action := UPPER(NULLIF(BTRIM(COALESCE(v_rotation_json ->> 'qr_action', '')), ''));
      IF v_rotation_action NOT IN ('INVALIDATE', 'REISSUE', 'REVOKE_TO_MANUAL') THEN
        RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_rotation_json.qr_action')::text;
      END IF;
      v_rotation_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_rotation_json ->> 'new_timesheet_id', '')), '')::uuid;
      IF v_rotation_new_timesheet_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_rotation_json.new_timesheet_id')::text;
      END IF;
      v_rotation_pending_qr := v_rotation_action IN ('INVALIDATE', 'REISSUE');
      v_rotation_revoke_reason := NULLIF(BTRIM(COALESCE(v_rotation_json ->> 'revoke_reason', '')), '');
      PERFORM pg_advisory_xact_lock(hashtext(v_current_ts.booking_id));
      SELECT ts.version
        INTO v_next_version
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_current_ts.booking_id
      ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;
      v_next_version := COALESCE(v_next_version, COALESCE(v_current_ts.version, 1)) + 1;

      UPDATE public.timesheets AS ts
         SET is_current = false,
             status = 'REVOKED'::public.timesheet_status_enum,
             revoked_reason = v_rotation_revoke_reason,
             revoked_by = p_actor_user_id::text,
             updated_at = v_now
       WHERE ts.timesheet_id = v_current_ts.timesheet_id
         AND ts.is_current = true;

      v_rotated_ts := v_current_ts;
      v_rotated_ts.timesheet_id := v_rotation_new_timesheet_id;
      v_rotated_ts.version := v_next_version;
      v_rotated_ts.is_current := true;
      v_rotated_ts.status := 'RECEIVED'::public.timesheet_status_enum;
      v_rotated_ts.revoked_at := NULL;
      v_rotated_ts.revoked_reason := NULL;
      v_rotated_ts.revoked_by := NULL;
      v_rotated_ts.authorised_at_server := NULL;
      v_rotated_ts.auth_name := NULL;
      v_rotated_ts.auth_job_title := NULL;
      v_rotated_ts.r2_nurse_key := NULL;
      v_rotated_ts.r2_auth_key := NULL;
      v_rotated_ts.img_sha256_nurse := NULL;
      v_rotated_ts.img_sha256_auth := NULL;
      v_rotated_ts.candidate_workflow_id := NULL;
      v_rotated_ts.candidate_workflow_generation := NULL;
      v_rotated_ts.candidate_manager_approved_at_utc := NULL;
      v_rotated_ts.qr_token := NULL;
      v_rotated_ts.qr_status := CASE WHEN v_rotation_pending_qr THEN 'PENDING'::public.timesheet_qr_status_enum ELSE NULL END;
      v_rotated_ts.qr_payload_json := '{}'::jsonb;
      v_rotated_ts.qr_generated_at := NULL;
      v_rotated_ts.qr_scanned_at := NULL;
      v_rotated_ts.qr_scan_info_json := NULL;
      v_rotated_ts.qr_r2_key := NULL;
      v_rotated_ts.qr_last_sent_hash := NULL;
      v_rotated_ts.qr_last_sent_at_utc := NULL;
      v_rotated_ts.qr_signed_hash := NULL;
      v_rotated_ts.qr_signed_at_utc := NULL;
      v_rotated_ts.manual_pdf_r2_key := NULL;
      v_rotated_ts.created_at := v_now;
      v_rotated_ts.updated_at := v_now;
      INSERT INTO public.timesheets SELECT (v_rotated_ts).* RETURNING * INTO v_current_ts;
    END IF;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'timesheet_identity_ready_for_patch',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'booking_id', v_current_ts.booking_id,
          'timesheet_version', v_current_ts.version,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'rotation_action', v_rotation_action,
          'rotation_new_timesheet_id', v_rotation_new_timesheet_id,
          'rotation_pending_qr', v_rotation_pending_qr,
          'has_patch_json', v_patch_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  END IF;

  IF v_patch_json <> '{}'::jsonb THEN
    v_patch_rec := jsonb_populate_record(v_current_ts, v_patch_json);
    UPDATE public.timesheets AS ts
       SET occupant_key_norm = COALESCE(v_patch_rec.occupant_key_norm, ts.occupant_key_norm),
           hospital_norm = COALESCE(v_patch_rec.hospital_norm, ts.hospital_norm),
           ward_norm = COALESCE(v_patch_rec.ward_norm, ts.ward_norm),
           job_title_norm = COALESCE(v_patch_rec.job_title_norm, ts.job_title_norm),
           shift_label_norm = v_patch_rec.shift_label_norm,
           authorised_at_server = v_patch_rec.authorised_at_server,
           auth_name = v_patch_rec.auth_name,
           auth_job_title = v_patch_rec.auth_job_title,
           r2_nurse_key = v_patch_rec.r2_nurse_key,
           r2_auth_key = v_patch_rec.r2_auth_key,
           img_sha256_nurse = v_patch_rec.img_sha256_nurse,
           img_sha256_auth = v_patch_rec.img_sha256_auth,
           candidate_workflow_id = v_patch_rec.candidate_workflow_id,
           candidate_workflow_generation = v_patch_rec.candidate_workflow_generation,
           candidate_manager_approved_at_utc = v_patch_rec.candidate_manager_approved_at_utc,
           status = COALESCE(v_patch_rec.status, ts.status),
           submission_mode = COALESCE(v_patch_rec.submission_mode, ts.submission_mode),
           manual_pdf_r2_key = v_patch_rec.manual_pdf_r2_key,
           line_type = COALESCE(v_patch_rec.line_type, ts.line_type),
           sheet_scope = COALESCE(v_patch_rec.sheet_scope, ts.sheet_scope),
           actual_schedule_json = COALESCE(v_patch_rec.actual_schedule_json, ts.actual_schedule_json, '[]'::jsonb),
           additional_units_week = COALESCE(v_patch_rec.additional_units_week, ts.additional_units_week, '{}'::jsonb),
           additional_units_per_day = COALESCE(v_patch_rec.additional_units_per_day, ts.additional_units_per_day, '{}'::jsonb),
           qr_token = v_patch_rec.qr_token,
           qr_status = v_patch_rec.qr_status,
           qr_payload_json = COALESCE(v_patch_rec.qr_payload_json, '{}'::jsonb),
           qr_generated_at = v_patch_rec.qr_generated_at,
           qr_scanned_at = v_patch_rec.qr_scanned_at,
           qr_scan_info_json = v_patch_rec.qr_scan_info_json,
           qr_r2_key = v_patch_rec.qr_r2_key,
           day_references_json = v_patch_rec.day_references_json,
           manual_pdf_rotation_degrees = COALESCE(v_patch_rec.manual_pdf_rotation_degrees, ts.manual_pdf_rotation_degrees, 0),
           qr_last_sent_hash = v_patch_rec.qr_last_sent_hash,
           qr_last_sent_at_utc = v_patch_rec.qr_last_sent_at_utc,
           qr_signed_hash = v_patch_rec.qr_signed_hash,
           qr_signed_at_utc = v_patch_rec.qr_signed_at_utc,
           candidate_hint_text = v_patch_rec.candidate_hint_text,
           band = COALESCE(v_patch_rec.band, ts.band),
           is_adjustment = COALESCE(v_patch_rec.is_adjustment, ts.is_adjustment),
           updated_at = v_now
     WHERE ts.timesheet_id = v_current_ts.timesheet_id
       AND ts.is_current = true
     RETURNING * INTO v_current_ts;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'timesheet_patch_applied',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'booking_id', v_current_ts.booking_id,
          'timesheet_version', v_current_ts.version,
          'timesheet_status', v_current_ts.status::text,
          'submission_mode', v_current_ts.submission_mode::text,
          'sheet_scope', v_current_ts.sheet_scope::text,
          'line_type', v_current_ts.line_type::text,
          'actual_schedule_count', CASE WHEN jsonb_typeof(v_current_ts.actual_schedule_json) = 'array' THEN jsonb_array_length(v_current_ts.actual_schedule_json) ELSE NULL END,
          'has_patch_json', v_patch_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF p_materialise_staged_evidence AND v_has_selected_queue_timesheet_materialisation THEN
    v_selected_queue_id_text := NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'queue_id', v_queue_timesheet_materialisation_json ->> 'queueId', '')), '');
    v_selected_queue_storage_key := NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storageKey', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'r2_key', '')), ''), ''), '^/+', ''), '');
    v_selected_queue_contract_week_text := NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'contract_week_id', v_queue_timesheet_materialisation_json ->> 'contractWeekId', '')), '');

    IF v_selected_queue_id_text IS NULL OR v_selected_queue_storage_key IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISSING', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id_text, 'storage_key', v_selected_queue_storage_key, 'reason', 'missing_queue_identity')::text;
    END IF;

    BEGIN
      v_selected_queue_id := v_selected_queue_id_text::uuid;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISSING', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id_text, 'storage_key', v_selected_queue_storage_key, 'reason', 'invalid_queue_id')::text;
    END;

    IF v_selected_queue_contract_week_text IS NOT NULL AND v_selected_queue_contract_week_text <> v_week.id::text THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISMATCH', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'supplied_contract_week_id', v_selected_queue_contract_week_text, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'reason', 'active_contract_week_mismatch')::text;
    END IF;

    SELECT mq.*
      INTO v_queue_item
      FROM public.manual_timesheet_queue AS mq
     WHERE mq.id = v_selected_queue_id
     FOR UPDATE;

    IF v_queue_item.id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISSING', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'reason', 'queue_row_not_found')::text;
    END IF;

    v_queue_storage_key := NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_item.r2_key, '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'r2_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'file_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'canonical_key', '')), ''), ''), '^/+', ''), '');
    IF v_queue_storage_key IS NULL OR v_queue_storage_key IS DISTINCT FROM v_selected_queue_storage_key THEN
      RAISE EXCEPTION USING MESSAGE = 'QUEUE_ITEM_STORAGE_MISMATCH', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'expected_storage_key', v_selected_queue_storage_key, 'actual_storage_key', v_queue_storage_key)::text;
    END IF;

    IF UPPER(COALESCE(v_queue_item.status, '')) <> 'QUEUED' OR v_queue_item.timesheet_id IS NOT NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'QUEUE_ITEM_NOT_AVAILABLE', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'status', v_queue_item.status, 'timesheet_id', v_queue_item.timesheet_id)::text;
    END IF;

    v_queue_kind := 'TIMESHEET';

    IF v_primary_timesheet_storage_key IS NULL THEN
      v_primary_timesheet_storage_key := v_queue_storage_key;
      v_primary_timesheet_queue_id := v_queue_item.id;
      v_primary_timesheet_rotation_raw := ((COALESCE(v_queue_item.last_rotation_deg, 0)::integer % 360) + 360) % 360;
      v_primary_timesheet_rotation_deg := CASE WHEN v_primary_timesheet_rotation_raw >= 315 OR v_primary_timesheet_rotation_raw < 45 THEN 0 WHEN v_primary_timesheet_rotation_raw >= 45 AND v_primary_timesheet_rotation_raw < 135 THEN 90 WHEN v_primary_timesheet_rotation_raw >= 135 AND v_primary_timesheet_rotation_raw < 225 THEN 180 ELSE 270 END;
    ELSIF v_queue_storage_key IS DISTINCT FROM v_primary_timesheet_storage_key THEN
      RAISE EXCEPTION USING MESSAGE = 'STAGED_TIMESHEET_CONFLICT', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'existing_storage_key', v_primary_timesheet_storage_key, 'conflicting_storage_key', v_queue_storage_key, 'queue_id', v_queue_item.id)::text;
    END IF;
    v_timesheet_stage_key_count := v_timesheet_stage_key_count + 1;

    SELECT COUNT(*)
      INTO v_existing_timesheet_evidence_conflict_count
      FROM public.timesheet_evidence AS te
     WHERE te.timesheet_id = v_current_ts.timesheet_id
       AND UPPER(COALESCE(te.kind, '')) = 'TIMESHEET'
       AND NULLIF(regexp_replace(COALESCE(te.storage_key, ''), '^/+', ''), '') IS DISTINCT FROM v_queue_storage_key;
    IF COALESCE(v_existing_timesheet_evidence_conflict_count, 0) > 0 THEN
      RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_EVIDENCE_ALREADY_EXISTS', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'timesheet_id', v_current_ts.timesheet_id, 'queue_id', v_queue_item.id, 'storage_key', v_queue_storage_key)::text;
    END IF;

    IF NOT EXISTS (
      SELECT 1
        FROM public.timesheet_evidence AS te
       WHERE te.timesheet_id = v_current_ts.timesheet_id
         AND UPPER(COALESCE(te.kind, '')) = 'TIMESHEET'
         AND NULLIF(regexp_replace(COALESCE(te.storage_key, ''), '^/+', ''), '') = v_queue_storage_key
    ) THEN
      INSERT INTO public.timesheet_evidence(timesheet_id, kind, display_name, storage_key, created_at, created_by)
      VALUES (v_current_ts.timesheet_id, 'TIMESHEET', v_queue_item.original_filename, v_queue_storage_key, COALESCE(v_queue_item.uploaded_at_utc, v_now), COALESCE(v_queue_item.uploaded_by_user_id, p_actor_user_id));
      v_attached_evidence_count := v_attached_evidence_count + 1;
    ELSE
      v_duplicate_queue_count := v_duplicate_queue_count + 1;
    END IF;

    UPDATE public.manual_timesheet_queue AS mq
       SET status = 'ATTACHED',
           timesheet_id = v_current_ts.timesheet_id,
           r2_key = v_queue_storage_key,
           meta_json = (COALESCE(mq.meta_json, '{}'::jsonb) - 'deferred_target_timesheet_id' - 'materialisation_deferred_at_utc' - 'deferred_rotation_degrees' - 'dematerialised_from_timesheet_id' - 'dematerialised_from_booking_id' - 'dematerialised_at_utc')
             || jsonb_build_object(
               'contract_week_id', v_week.id::text,
               'staged_kind', 'TIMESHEET',
               'selected_queue_timesheet_materialisation', true,
               'materialisation_deferred_to_backend', false,
               'materialised_to_timesheet_id', v_current_ts.timesheet_id::text,
               'materialised_at_utc', v_now,
               'materialised_storage_key', v_queue_storage_key,
               'materialised_from_process_preview', true,
               'preview_selection_key', COALESCE(v_queue_timesheet_materialisation_json ->> 'preview_selection_key', v_queue_timesheet_materialisation_json ->> 'previewSelectionKey'),
               'preview_identity', COALESCE(v_queue_timesheet_materialisation_json ->> 'preview_identity', v_queue_timesheet_materialisation_json ->> 'previewIdentity'),
               'active_identity', COALESCE(v_queue_timesheet_materialisation_json ->> 'active_identity', v_queue_timesheet_materialisation_json ->> 'activeIdentity')
             )
     WHERE mq.id = v_queue_item.id
       AND mq.status = 'QUEUED'
       AND mq.timesheet_id IS NULL;
    IF NOT FOUND THEN
      RAISE EXCEPTION USING MESSAGE = 'QUEUE_ITEM_NOT_AVAILABLE', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'reason', 'conditional_attach_failed')::text;
    END IF;
    v_attached_queue_count := v_attached_queue_count + 1;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'selected_queue_materialisation_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'materialise_staged_evidence', p_materialise_staged_evidence,
          'has_selected_queue_timesheet_materialisation', v_has_selected_queue_timesheet_materialisation,
          'selected_queue_id', v_selected_queue_id,
          'selected_queue_storage_key', v_selected_queue_storage_key,
          'primary_timesheet_queue_id', v_primary_timesheet_queue_id,
          'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
          'attached_evidence_count', v_attached_evidence_count,
          'attached_queue_count', v_attached_queue_count,
          'duplicate_queue_count', v_duplicate_queue_count,
          'timesheet_stage_key_count', v_timesheet_stage_key_count
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF p_materialise_staged_evidence THEN
    FOR v_queue_item IN
      SELECT mq.*
      FROM public.manual_timesheet_queue AS mq
      WHERE mq.status = 'STAGED'
        AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
      ORDER BY mq.uploaded_at_utc ASC NULLS LAST, mq.id ASC
      FOR UPDATE
    LOOP
      v_staged_evidence_loop_count := v_staged_evidence_loop_count + 1;
      v_queue_kind := UPPER(COALESCE(NULLIF(BTRIM(v_queue_item.meta_json ->> 'staged_kind'), ''), NULLIF(BTRIM(v_queue_item.meta_json ->> 'kind'), ''), NULLIF(BTRIM(v_queue_item.meta_json ->> 'attached_kind'), ''), 'TIMESHEET'));
      IF v_queue_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') THEN
        v_queue_kind := 'OTHER';
      END IF;
      IF v_queue_kind = 'TIMESHEET' AND (v_suppress_timesheet_evidence_materialisation OR v_has_selected_queue_timesheet_materialisation) THEN
        CONTINUE;
      END IF;
      v_queue_storage_key := NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_item.r2_key, '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'r2_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'file_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'canonical_key', '')), ''), ''), '^/+', ''), '');
      IF v_queue_storage_key IS NULL THEN
        IF v_queue_kind = 'TIMESHEET' THEN
          RAISE EXCEPTION USING MESSAGE = 'INVALID_TIMESHEET_EVIDENCE', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_queue_item.id, 'reason', 'missing_storage_key')::text;
        END IF;
        CONTINUE;
      END IF;

      IF v_queue_kind = 'TIMESHEET' THEN
        IF v_primary_timesheet_storage_key IS NULL THEN
          v_primary_timesheet_storage_key := v_queue_storage_key;
          v_primary_timesheet_queue_id := v_queue_item.id;
          v_primary_timesheet_rotation_raw := ((COALESCE(v_queue_item.last_rotation_deg, 0)::integer % 360) + 360) % 360;
          v_primary_timesheet_rotation_deg := CASE WHEN v_primary_timesheet_rotation_raw >= 315 OR v_primary_timesheet_rotation_raw < 45 THEN 0 WHEN v_primary_timesheet_rotation_raw >= 45 AND v_primary_timesheet_rotation_raw < 135 THEN 90 WHEN v_primary_timesheet_rotation_raw >= 135 AND v_primary_timesheet_rotation_raw < 225 THEN 180 ELSE 270 END;
        ELSIF v_queue_storage_key IS DISTINCT FROM v_primary_timesheet_storage_key THEN
          RAISE EXCEPTION USING MESSAGE = 'STAGED_TIMESHEET_CONFLICT', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'existing_storage_key', v_primary_timesheet_storage_key, 'conflicting_storage_key', v_queue_storage_key, 'queue_id', v_queue_item.id)::text;
        END IF;
        v_timesheet_stage_key_count := v_timesheet_stage_key_count + 1;
      END IF;

      IF NOT EXISTS (
        SELECT 1
        FROM public.timesheet_evidence AS te
        WHERE te.timesheet_id = v_current_ts.timesheet_id
          AND te.kind = v_queue_kind
          AND te.storage_key = v_queue_storage_key
      ) THEN
        INSERT INTO public.timesheet_evidence(timesheet_id, kind, display_name, storage_key, created_at, created_by)
        VALUES (v_current_ts.timesheet_id, v_queue_kind, v_queue_item.original_filename, v_queue_storage_key, COALESCE(v_queue_item.uploaded_at_utc, v_now), COALESCE(v_queue_item.uploaded_by_user_id, p_actor_user_id));
        v_attached_evidence_count := v_attached_evidence_count + 1;
      ELSE
        v_duplicate_queue_count := v_duplicate_queue_count + 1;
      END IF;

      UPDATE public.manual_timesheet_queue AS mq
         SET status = 'ATTACHED',
             timesheet_id = v_current_ts.timesheet_id,
             r2_key = v_queue_storage_key,
             meta_json = (COALESCE(mq.meta_json, '{}'::jsonb) - 'deferred_target_timesheet_id' - 'materialisation_deferred_at_utc' - 'deferred_rotation_degrees' - 'dematerialised_from_timesheet_id' - 'dematerialised_from_booking_id' - 'dematerialised_at_utc')
               || jsonb_build_object(
                 'contract_week_id', v_week.id::text,
                 'staged_kind', v_queue_kind,
                 'materialisation_deferred_to_backend', false,
                 'materialised_to_timesheet_id', v_current_ts.timesheet_id::text,
                 'materialised_at_utc', v_now,
                 'materialised_storage_key', v_queue_storage_key,
                 'duplicate_timesheet_evidence_identity', CASE WHEN v_queue_kind = 'TIMESHEET' AND v_queue_item.id IS DISTINCT FROM v_primary_timesheet_queue_id THEN true ELSE false END,
                 'duplicate_of_queue_item_id', CASE WHEN v_queue_kind = 'TIMESHEET' AND v_queue_item.id IS DISTINCT FROM v_primary_timesheet_queue_id THEN v_primary_timesheet_queue_id::text ELSE NULL END,
                 'materialisation_noop_reason', CASE WHEN v_queue_kind = 'TIMESHEET' AND v_queue_item.id IS DISTINCT FROM v_primary_timesheet_queue_id THEN 'same_storage_key_duplicate' ELSE NULL END
               )
       WHERE mq.id = v_queue_item.id;
      v_attached_queue_count := v_attached_queue_count + 1;
    END LOOP;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'staged_evidence_materialisation_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'materialise_staged_evidence', p_materialise_staged_evidence,
          'staged_evidence_loop_count', v_staged_evidence_loop_count,
          'attached_evidence_count', v_attached_evidence_count,
          'attached_queue_count', v_attached_queue_count,
          'duplicate_queue_count', v_duplicate_queue_count,
          'timesheet_stage_key_count', v_timesheet_stage_key_count,
          'primary_timesheet_queue_id', v_primary_timesheet_queue_id,
          'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
          'primary_timesheet_rotation_degrees', v_primary_timesheet_rotation_deg
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_primary_timesheet_storage_key IS NOT NULL THEN
    UPDATE public.timesheets AS ts
       SET manual_pdf_r2_key = v_primary_timesheet_storage_key,
           manual_pdf_rotation_degrees = v_primary_timesheet_rotation_deg,
           updated_at = v_now
     WHERE ts.timesheet_id = v_current_ts.timesheet_id
       AND ts.is_current = true
     RETURNING * INTO v_current_ts;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'primary_timesheet_storage_applied',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
          'primary_timesheet_rotation_degrees', v_primary_timesheet_rotation_deg,
          'manual_pdf_r2_key', v_current_ts.manual_pdf_r2_key,
          'manual_pdf_rotation_degrees', v_current_ts.manual_pdf_rotation_degrees
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_week_patch_rec := jsonb_populate_record(v_week, v_week_patch_json);
  UPDATE public.contract_weeks AS cw
     SET status = COALESCE(v_week_patch_rec.status, 'SUBMITTED'::public.contract_week_status_enum),
         submission_mode_snapshot = COALESCE(v_week_patch_rec.submission_mode_snapshot, cw.submission_mode_snapshot, 'MANUAL'::public.submission_mode_enum),
         timesheet_id = v_current_ts.timesheet_id,
         uploaded_pdf_r2_key = COALESCE(v_week_patch_rec.uploaded_pdf_r2_key, v_primary_timesheet_storage_key, cw.uploaded_pdf_r2_key),
         day_entries_json = COALESCE(v_week_patch_rec.day_entries_json, cw.day_entries_json),
         totals_json = COALESCE(v_week_patch_rec.totals_json, cw.totals_json),
         planned_schedule_json = COALESCE(v_week_patch_rec.planned_schedule_json, cw.planned_schedule_json),
         is_adjustment = COALESCE(v_week_patch_rec.is_adjustment, cw.is_adjustment),
         enforce_day_partition = COALESCE(v_week_patch_rec.enforce_day_partition, cw.enforce_day_partition),
         allowed_days_mask = COALESCE(v_week_patch_rec.allowed_days_mask, cw.allowed_days_mask),
         split_boundary_date = COALESCE(v_week_patch_rec.split_boundary_date, cw.split_boundary_date),
         worker_note = COALESCE(v_week_patch_rec.worker_note, cw.worker_note),
         split_group_key = COALESCE(v_week_patch_rec.split_group_key, cw.split_group_key),
         updated_at = v_now
   WHERE cw.id = v_week.id
   RETURNING * INTO v_week;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_week_update_done',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_week_id', v_week.id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'new_contract_week_status', v_week.status::text,
          'submission_mode_snapshot', v_week.submission_mode_snapshot::text,
          'uploaded_pdf_r2_key', v_week.uploaded_pdf_r2_key,
          'has_week_patch_json', v_week_patch_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_tsfin_snapshot_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_tsfin_snapshot_json')::text;
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'client_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'client_id', '')), '')::uuid IS DISTINCT FROM v_contract.client_id THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH', DETAIL = jsonb_build_object('field', 'client_id', 'expected_value', v_contract.client_id, 'supplied_value', v_tsfin_snapshot_json ->> 'client_id')::text;
  END IF;
  IF v_contract.candidate_id IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'candidate_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'candidate_id', '')), '')::uuid IS DISTINCT FROM v_contract.candidate_id THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH', DETAIL = jsonb_build_object('field', 'candidate_id', 'expected_value', v_contract.candidate_id, 'supplied_value', v_tsfin_snapshot_json ->> 'candidate_id')::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_snapshot_validated',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'contract_client_id', v_contract.client_id,
          'contract_candidate_id', v_contract.candidate_id,
          'snapshot_client_id', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'client_id', '')), ''),
          'snapshot_candidate_id', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'candidate_id', '')), ''),
          'snapshot_processing_status', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''),
          'snapshot_total_pay_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_pay_ex_vat', '')), ''),
          'snapshot_total_charge_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_charge_ex_vat', '')), '')
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_tsfin_snapshot_json := v_tsfin_snapshot_json || jsonb_build_object(
    'timesheet_id', v_current_ts.timesheet_id::text,
    'timesheet_version', v_current_ts.version,
    'processing_status', CASE
      WHEN COALESCE(NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''), 'PENDING_AUTH') = 'AWAITING_MANUAL_SIGNATURE'
       AND v_current_ts.submission_mode = 'MANUAL'::public.submission_mode_enum
       AND v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
       AND v_current_ts.qr_status IS NULL
      THEN 'PENDING_AUTH'
      ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''), 'PENDING_AUTH')
    END
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_write_started',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'snapshot_processing_status', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''),
          'snapshot_basis', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'basis', '')), ''),
          'snapshot_total_hours', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_hours', '')), ''),
          'snapshot_total_pay_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_pay_ex_vat', '')), ''),
          'snapshot_total_charge_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_charge_ex_vat', '')), ''),
          'snapshot_expenses_pay_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'expenses_pay_ex_vat', '')), '')
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_tsfin_result := public.tsfin_write_current_snapshot_single_bounded(
    p_timesheet_id => v_current_ts.timesheet_id,
    p_timesheet_version => v_current_ts.version,
    p_snapshot_json => v_tsfin_snapshot_json,
    p_actor_user_id => p_actor_user_id,
    p_now_utc => v_now
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_write_completed',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'tsfin_result_ok', COALESCE((v_tsfin_result ->> 'ok')::boolean, false),
          'tsfin_result_code', NULLIF(BTRIM(COALESCE(v_tsfin_result ->> 'code', v_tsfin_result ->> 'error_code', '')), ''),
          'tsfin_result_message', NULLIF(BTRIM(COALESCE(v_tsfin_result ->> 'message', v_tsfin_result ->> 'error', '')), ''),
          'tsfin_result_keys', (SELECT jsonb_agg(result_key ORDER BY result_key) FROM jsonb_object_keys(COALESCE(v_tsfin_result, '{}'::jsonb)) AS result_keys(result_key))
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE((v_tsfin_result ->> 'ok')::boolean, false) IS DISTINCT FROM true THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_UPDATE_FAILED', DETAIL = COALESCE(v_tsfin_result, '{}'::jsonb)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_write_result_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'tsfin_result_ok', COALESCE((v_tsfin_result ->> 'ok')::boolean, false)
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'current_tsfin_reloaded_after_write',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'current_tsfin_id', v_current_tsfin.id,
          'processing_status', v_current_tsfin.processing_status::text,
          'total_hours', v_current_tsfin.total_hours,
          'total_pay_ex_vat', v_current_tsfin.total_pay_ex_vat,
          'total_charge_ex_vat', v_current_tsfin.total_charge_ex_vat,
          'margin_ex_vat', v_current_tsfin.margin_ex_vat,
          'computed_at_utc', v_current_tsfin.computed_at_utc
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_summary_refresh_result := public.pay_timesheet_summary_pay_state_refresh(
    ARRAY[v_current_ts.timesheet_id]::uuid[],
    p_actor_user_id
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'summary_pay_state_lightweight_patch_done',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'summary_refresh_result', v_summary_refresh_result
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'after_signature_generation_started',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'previous_row_signature', v_current_row_signature
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, v_week.id, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  IF COALESCE(v_temp_log_enabled, false) THEN
    PERFORM public._temp_diag_log(
      'TIMESHEET_SAVE_SIGNATURE_DIAG',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_strip_nulls(jsonb_build_object(
        'tag', 'TIMESHEET_SAVE_SIGNATURE_DIAG',
        'function_name', 'contract_week_manual_upsert_atomic',
        'stage', 'after_manual_upsert_signature_generated',
        'action', 'manual_upsert',
        'route_family', 'contract_week_manual_upsert',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3),
        'duration_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3),
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', v_week.id,
        'previous_row_signature', v_current_row_signature,
        'new_row_signature', v_after_row_signature,
        'signature_payload', v_after_signature_json
      ))
    );
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'after_signature_generated',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'previous_row_signature', v_current_row_signature,
          'new_row_signature', v_after_row_signature,
          'signature_payload_present', v_after_signature_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  PERFORM public._audit_insert(
    'contract_week',
    v_week.id::text,
    CASE WHEN v_created_now THEN 'CONTRACT_WEEK_MANUAL_TIMESHEET_CREATED_PROCESSED' ELSE 'CONTRACT_WEEK_MANUAL_TIMESHEET_UPDATED_PROCESSED' END,
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'previous_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id END,
      'previous_contract_week_status', v_previous_contract_week_status,
      'previous_processing_status', v_previous_processing_status,
      'previous_row_signature', v_current_row_signature
    ),
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'timesheet_id', v_current_ts.timesheet_id,
      'new_contract_week_status', v_week.status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_row_signature', v_after_row_signature,
      'created_now', v_created_now,
      'attached_evidence_count', v_attached_evidence_count,
      'attached_queue_count', v_attached_queue_count,
      'duplicate_queue_count', v_duplicate_queue_count,
      'primary_timesheet_storage_key', v_primary_timesheet_storage_key
    ),
    'WEEKLY_MANUAL_PROCESS',
    p_actor_user_id
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'audit_done',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'audit_action', CASE WHEN v_created_now THEN 'CONTRACT_WEEK_MANUAL_TIMESHEET_CREATED_PROCESSED' ELSE 'CONTRACT_WEEK_MANUAL_TIMESHEET_UPDATED_PROCESSED' END,
          'new_processing_status', v_current_tsfin.processing_status::text,
          'new_row_signature', v_after_row_signature
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'return',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'operation', CASE WHEN v_created_now THEN 'weekly_manual_process_create' ELSE 'weekly_manual_process_update' END,
          'new_contract_week_status', v_week.status::text,
          'new_processing_status', v_current_tsfin.processing_status::text,
          'new_row_signature', v_after_row_signature,
          'attached_evidence_count', v_attached_evidence_count,
          'attached_queue_count', v_attached_queue_count,
          'duplicate_queue_count', v_duplicate_queue_count
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', CASE WHEN v_created_now THEN 'weekly_manual_process_create' ELSE 'weekly_manual_process_update' END,
    'contract_week_id', v_week.id,
    'contract_id', v_week.contract_id,
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_version', v_current_ts.version,
    'was_stale', v_was_stale,
    'created_now', v_created_now,
    'previous_contract_week_status', v_previous_contract_week_status,
    'new_contract_week_status', v_week.status::text,
    'previous_processing_status', v_previous_processing_status,
    'processing_status', v_current_tsfin.processing_status::text,
    'new_processing_status', v_current_tsfin.processing_status::text,
    'timesheet_financials_id', v_current_tsfin.id,
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'expected_row_signature', v_after_row_signature,
    'lifecycle_signature_stable', v_after_row_signature IS NOT NULL,
    'lifecycle_signature_pending_reason', CASE WHEN v_after_row_signature IS NULL THEN 'POST_SAVE_ROW_SIGNATURE_UNAVAILABLE' ELSE NULL::text END,
    'requires_authorise_preflight', v_after_row_signature IS NULL,
    'requires_affected_row_refresh', v_after_row_signature IS NULL,
    'refresh_required', v_after_row_signature IS NULL,
    'permission_state_patch_complete', v_after_row_signature IS NOT NULL,
    'priority_badges_patch_complete', v_after_row_signature IS NOT NULL,
    'immediate_lifecycle_patch_available', v_after_row_signature IS NOT NULL,
    'lifecycle_patch', jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', v_week.id,
      'booking_id', v_current_ts.booking_id,
      'timesheet_status', v_current_ts.status::text,
      'status', v_current_ts.status::text,
      'contract_week_status', v_week.status::text,
      'processing_status', v_current_tsfin.processing_status::text,
      'tsfin_processing_status', v_current_tsfin.processing_status::text,
      'timesheet_financials_id', v_current_tsfin.id,
      'is_current', v_current_ts.is_current,
      'tsfin_is_current', v_current_tsfin.is_current,
      'authorised_at_server', v_current_ts.authorised_at_server,
      'revoked_at', v_current_ts.revoked_at,
      'row_signature', v_after_row_signature,
      'backend_row_signature', v_after_row_signature,
      'expected_row_signature', v_after_row_signature,
      'lifecycle_signature_stable', v_after_row_signature IS NOT NULL,
      'lifecycle_signature_pending_reason', CASE WHEN v_after_row_signature IS NULL THEN 'POST_SAVE_ROW_SIGNATURE_UNAVAILABLE' ELSE NULL::text END,
      'permission_state_patch_complete', v_after_row_signature IS NOT NULL,
      'priority_badges_patch_complete', v_after_row_signature IS NOT NULL,
      'immediate_lifecycle_patch_available', v_after_row_signature IS NOT NULL,
      'requires_network_before_authorise', v_after_row_signature IS NULL,
      'refresh_required', v_after_row_signature IS NULL,
      'row_stale', v_after_row_signature IS NULL,
      'lifecycle_refresh_failed', v_after_row_signature IS NULL
    )),
    'timesheet', jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', v_week.id,
      'booking_id', v_current_ts.booking_id,
      'status', v_current_ts.status::text,
      'is_current', v_current_ts.is_current,
      'authorised_at_server', v_current_ts.authorised_at_server,
      'revoked_at', v_current_ts.revoked_at,
      'row_signature', v_after_row_signature,
      'backend_row_signature', v_after_row_signature
    )),
    'tsfin', jsonb_strip_nulls(jsonb_build_object(
      'id', v_current_tsfin.id,
      'timesheet_id', v_current_tsfin.timesheet_id,
      'is_current', v_current_tsfin.is_current,
      'processing_status', v_current_tsfin.processing_status::text,
      'total_hours', v_current_tsfin.total_hours,
      'total_pay_ex_vat', v_current_tsfin.total_pay_ex_vat,
      'total_charge_ex_vat', v_current_tsfin.total_charge_ex_vat,
      'margin_ex_vat', v_current_tsfin.margin_ex_vat,
      'computed_at_utc', v_current_tsfin.computed_at_utc,
      'updated_at', v_current_tsfin.updated_at
    )),
    'summary_pay_state_refresh', COALESCE(v_summary_refresh_result, '{}'::jsonb),
    'evidence_summary', jsonb_build_object(
      'attached_evidence_count', v_attached_evidence_count,
      'attached_queue_count', v_attached_queue_count,
      'duplicate_queue_count', v_duplicate_queue_count,
      'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
      'primary_timesheet_rotation_degrees', v_primary_timesheet_rotation_deg,
      'selected_queue_timesheet_queue_id', v_selected_queue_id,
      'selected_queue_timesheet_storage_key', v_selected_queue_storage_key
    ),
    'affected_rows', jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', v_week.id,
      'booking_id', v_current_ts.booking_id,
      'timesheet_status', v_current_ts.status::text,
      'contract_week_status', v_week.status::text,
      'processing_status', v_current_tsfin.processing_status::text,
      'tsfin_processing_status', v_current_tsfin.processing_status::text,
      'row_signature', v_after_row_signature,
      'backend_row_signature', v_after_row_signature,
      'expected_row_signature', v_after_row_signature,
      'permission_state_patch_complete', v_after_row_signature IS NOT NULL,
      'priority_badges_patch_complete', v_after_row_signature IS NOT NULL,
      'immediate_lifecycle_patch_available', v_after_row_signature IS NOT NULL,
      'requires_network_before_authorise', v_after_row_signature IS NULL,
      'row_key', 'timesheet:' || v_current_ts.timesheet_id::text
    ))),
    'cache_invalidation_hints', jsonb_build_object(
      'changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'timesheet_summary_pay_state_cache', 'timesheet_pay_state', 'contract_weeks', 'timesheet_evidence', 'manual_timesheet_queue'),
      'contract_week_id', v_week.id,
      'timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_current_ts.booking_id
    )
  ) || CASE
    WHEN private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') THEN
      jsonb_build_object(
        'candidate_record_role',v_candidate_final_state_guard->>'record_role',
        'candidate_expected_line_type',v_candidate_final_state_guard->>'expected_line_type',
        'candidate_final_state_guard',v_candidate_final_state_guard)
    ELSE '{}'::jsonb END;
EXCEPTION
  WHEN unique_violation THEN
    GET STACKED DIAGNOSTICS v_error_constraint = CONSTRAINT_NAME;

    IF COALESCE(v_temp_log_enabled, false) THEN
      v_diag_step_index := v_diag_step_index + 1;
      v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
      v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
      PERFORM public._temp_diag_log(
        'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'exception_unique_violation',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'contract_week_id', p_week_id,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'constraint_name', v_error_constraint
        ))
      );
      v_diag_stage_started_at := clock_timestamp();
    END IF;
    IF v_error_constraint = 'uq_manual_timesheet_queue_one_active_staged_timesheet_per_contr' THEN
      RAISE EXCEPTION USING MESSAGE = 'STAGED_TIMESHEET_CONFLICT', DETAIL = jsonb_build_object('contract_week_id', p_week_id, 'constraint_name', v_error_constraint)::text;
    END IF;
    RAISE;
  WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;

    IF COALESCE(v_temp_log_enabled, false) THEN
      v_diag_step_index := v_diag_step_index + 1;
      v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
      v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
      PERFORM public._temp_diag_log(
        'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'exception_others',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'contract_week_id', p_week_id,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'error_state', v_error_state,
          'error_message', v_error_message
        ))
      );
      v_diag_stage_started_at := clock_timestamp();
    END IF;
    IF v_error_state = '55P03' THEN
      RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('contract_week_id', p_week_id, 'error_state', v_error_state)::text;
    END IF;
    RAISE;
END;
$function$;

CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_process_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_patch_json jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_previous_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := 'PENDING_AUTH'::public.ts_fin_processing_status_enum;
  v_has_segment_invoice_lock boolean := false;
  v_timesheet_patch jsonb := COALESCE(p_timesheet_patch_json, '{}'::jsonb);
  v_tsfin_patch jsonb := COALESCE(p_tsfin_patch_json, '{}'::jsonb);
  v_forbidden_tsfin_patch_keys text[] := ARRAY[
    'candidate_id','client_id','pay_method','candidate_assignment','basis','policy_snapshot_json','rate_source_refs_json',
    'normal_hours','unsocial_hours','saturday_hours','sunday_hours','bank_holiday_hours','sleep_in_units','on_call_units','mileage_units','expenses_units',
    'total_hours','total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat','net_delta_ex_vat','normal_pay_rate','unsocial_pay_rate','saturday_pay_rate','sunday_pay_rate','bank_holiday_pay_rate',
    'normal_charge_rate','unsocial_charge_rate','saturday_charge_rate','sunday_charge_rate','bank_holiday_charge_rate','mileage_pay_rate','mileage_charge_rate','expenses_pay','expenses_charge',
    'has_rate_issue','has_pay_channel_issue','hours_day','hours_night','hours_sat','hours_sun','hours_bh','pay_day','pay_night','pay_sat','pay_sun','pay_bh','charge_day','charge_night','charge_sat','charge_sun','charge_bh',
    'expenses_pay_ex_vat','expenses_charge_ex_vat','mileage_pay_ex_vat','mileage_charge_ex_vat','travel_pay_ex_vat','travel_charge_ex_vat','accommodation_pay_ex_vat','accommodation_charge_ex_vat','other_pay_ex_vat','other_charge_ex_vat','additional_pay_ex_vat','additional_charge_ex_vat','additional_margin_ex_vat'
  ];
  v_financial_affecting_timesheet_patch_keys text[] := ARRAY[
    'worked_start_iso','worked_end_iso','break_start_iso','break_end_iso','break_minutes','worked_minutes','actual_schedule_json','additional_units_week','additional_units_per_day','scheduled_start_iso','scheduled_end_iso','week_ending_date','worked_date','work_date'
  ];
  v_patch_key text := NULL;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_candidate_electronic_context boolean :=
    COALESCE(current_setting('cloudtms.candidate_electronic_finalise', true), '') <> ''
    AND private._candidate_feature_enabled_current_v1('candidate_daily_finalisation');
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_ID_REQUIRED', 'message', 'p_timesheet_id is required.');
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'EXPECTED_TIMESHEET_ID_REQUIRED', 'message', 'p_expected_timesheet_id is required.');
  END IF;
  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'message', 'p_actor_user_id is required.');
  END IF;
  IF jsonb_typeof(v_timesheet_patch) <> 'object' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_PATCH_MUST_BE_OBJECT', 'message', 'p_timesheet_patch_json must be a JSON object.');
  END IF;
  IF jsonb_typeof(v_tsfin_patch) <> 'object' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TSFIN_PATCH_MUST_BE_OBJECT', 'message', 'p_tsfin_patch_json must be a JSON object.');
  END IF;

  IF NOT v_candidate_electronic_context
     AND v_timesheet_patch ?| ARRAY[
       'submission_mode','auth_name','auth_job_title','r2_nurse_key','r2_auth_key',
       'img_sha256_nurse','img_sha256_auth','candidate_workflow_id',
       'candidate_workflow_generation','candidate_manager_approved_at_utc'
     ] THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process',
      'error_code', 'CANDIDATE_ELECTRONIC_FIELDS_FORBIDDEN',
      'message', 'Candidate electronic document fields require the bounded Candidate App finalisation authority.');
  END IF;

  FOREACH v_patch_key IN ARRAY v_forbidden_tsfin_patch_keys LOOP
    IF v_tsfin_patch ? v_patch_key THEN
      RETURN jsonb_build_object('ok', false, 'success', false, 'operation', 'daily_manual_process', 'error_code', 'TSFIN_PATCH_FORBIDDEN_FIELD', 'message', 'Cannot process: TSFIN patch contains an authoritative or financial field.', 'field', v_patch_key, 'timesheet_id', p_timesheet_id);
    END IF;
  END LOOP;

  FOREACH v_patch_key IN ARRAY v_financial_affecting_timesheet_patch_keys LOOP
    IF v_timesheet_patch ? v_patch_key THEN
      RETURN jsonb_build_object('ok', false, 'success', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_PATCH_REQUIRES_RECALCULATION', 'message', 'Cannot process while changing worked time, schedule, break, work date, or additional units. Save and recalculate the row before processing.', 'field', v_patch_key, 'timesheet_id', p_timesheet_id);
    END IF;
  END LOOP;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_NOT_FOUND', 'message', 'Timesheet was not found.', 'requested_timesheet_id', p_timesheet_id);
  END IF;

  SELECT ts.*
    INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.booking_id = v_requested_ts.booking_id
    AND ts.is_current = true
  ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.timesheet_id IS NULL THEN
    v_current_ts := v_requested_ts;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_MOVED', 'message', 'Timesheet has moved to a newer current row.', 'requested_timesheet_id', p_timesheet_id, 'expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id, 'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id);
  END IF;

  IF v_current_ts.sheet_scope <> 'DAILY'::public.timesheet_scope_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NOT_DAILY', 'message', 'Timesheet is not DAILY; daily manual process only applies to DAILY sheets.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_ts.submission_mode <> 'MANUAL'::public.submission_mode_enum
     AND NOT (v_candidate_electronic_context
       AND v_current_ts.submission_mode = 'ELECTRONIC'::public.submission_mode_enum) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NOT_MANUAL', 'message', 'Timesheet must be MANUAL before processing.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_ALREADY_AUTHORISED', 'message', 'This timesheet is already authorised.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NO_TSFIN', 'message', 'No current financial snapshot exists for this timesheet.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  v_previous_status := v_current_tsfin.processing_status;
  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_LOCKED_BY_INVOICE', 'message', 'Timesheet is invoice-locked; cannot process.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_previous_status <> 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'NOT_UNPROCESSED', 'message', 'Timesheet is not in UNPROCESSED state.', 'current_timesheet_id', v_current_ts.timesheet_id, 'previous_status', v_previous_status::text);
  END IF;
  IF v_current_tsfin.candidate_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'CANDIDATE_MISSING', 'message', 'Cannot process: candidate is missing from TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_tsfin.client_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'CLIENT_MISSING', 'message', 'Cannot process: client is missing from TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF COALESCE(v_current_tsfin.has_rate_issue, false) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'RATE_ISSUE', 'message', 'Cannot process: TSFIN has a rate issue.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF COALESCE(v_current_tsfin.has_pay_channel_issue, false) THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'PAY_CHANNEL_ISSUE', 'message', 'Cannot process: TSFIN has a pay channel issue.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_current_tsfin.pay_method, '')), '') IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'PAY_METHOD_MISSING', 'message', 'Cannot process: pay method is missing from TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;
  IF v_current_tsfin.candidate_assignment = 'UNASSIGNED'::public.candidate_assignment_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'CANDIDATE_ASSIGNMENT_UNRESOLVED', 'message', 'Cannot process: candidate assignment is unresolved in TSFIN context.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, NULL::uuid, false);
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, v_timesheet_patch ->> 'backend_row_signature', v_timesheet_patch ->> 'row_signature', v_tsfin_patch ->> 'backend_row_signature', v_tsfin_patch ->> 'row_signature', '')), '');

  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'ROW_SIGNATURE_MISMATCH', 'message', 'Timesheet changed after it was loaded. Refresh the row and try again.', 'current_timesheet_id', v_current_ts.timesheet_id, 'expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature);
  END IF;

  UPDATE public.timesheets AS ts
     SET reference_number = CASE WHEN v_timesheet_patch ? 'reference_number' THEN NULLIF(BTRIM(v_timesheet_patch ->> 'reference_number'), '') ELSE ts.reference_number END,
         reference_set_at = CASE
           WHEN v_timesheet_patch ? 'reference_number' THEN
             CASE WHEN NULLIF(BTRIM(v_timesheet_patch ->> 'reference_number'), '') IS NULL THEN NULL::timestamp with time zone
                  WHEN NULLIF(BTRIM(v_timesheet_patch ->> 'reference_number'), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(ts.reference_number, '')), '') THEN v_now
                  ELSE COALESCE(ts.reference_set_at, v_now)
             END
           ELSE ts.reference_set_at
         END,
         submission_mode = CASE WHEN v_candidate_electronic_context
           THEN 'ELECTRONIC'::public.submission_mode_enum ELSE ts.submission_mode END,
         auth_name = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'auth_name'),'') ELSE ts.auth_name END,
         auth_job_title = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'auth_job_title'),'') ELSE ts.auth_job_title END,
         r2_nurse_key = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'r2_nurse_key'),'') ELSE ts.r2_nurse_key END,
         r2_auth_key = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'r2_auth_key'),'') ELSE ts.r2_auth_key END,
         img_sha256_nurse = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'img_sha256_nurse'),'') ELSE ts.img_sha256_nurse END,
         img_sha256_auth = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(BTRIM(v_timesheet_patch->>'img_sha256_auth'),'') ELSE ts.img_sha256_auth END,
         candidate_workflow_id = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(v_timesheet_patch->>'candidate_workflow_id','')::uuid ELSE ts.candidate_workflow_id END,
         candidate_workflow_generation = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(v_timesheet_patch->>'candidate_workflow_generation','')::integer ELSE ts.candidate_workflow_generation END,
         candidate_manager_approved_at_utc = CASE WHEN v_candidate_electronic_context
           THEN NULLIF(v_timesheet_patch->>'candidate_manager_approved_at_utc','')::timestamptz
           ELSE ts.candidate_manager_approved_at_utc END,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         processed_by_user_id = p_actor_user_id,
         processed_at_utc = v_now,
         authorised_by_user_id = NULL,
         authorised_at_utc = NULL,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  v_after_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, NULL::uuid, false);
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_DAILY_MANUAL_PROCESSED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'previous_processing_status', v_previous_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'new_processing_status', v_new_status::text, 'processed_at_utc', v_now, 'processed_by_user_id', p_actor_user_id, 'new_row_signature', v_after_row_signature),
    'DAILY_MANUAL_PROCESS',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'daily_manual_process',
    'processed', true,
    'requested_timesheet_id', p_timesheet_id,
    'expected_timesheet_id', p_expected_timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'timesheet_id', v_current_ts.timesheet_id,
    'timesheet_financials_id', v_current_tsfin.id,
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_status', v_previous_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'status_transition', jsonb_build_object('from', v_previous_status::text, 'to', v_new_status::text, 'processed_at_utc', v_now, 'processed_by_user_id', p_actor_user_id),
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'count_deltas', jsonb_build_object('unprocessed', -1, 'processed', 1),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials'), 'timesheet_id', v_current_ts.timesheet_id)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_process', 'error_code', 'LOCK_TIMEOUT', 'message', 'The timesheet is currently locked by another operation.', 'timesheet_id', p_timesheet_id);
  END IF;
  RAISE;
END;
$function$;

CREATE OR REPLACE FUNCTION public.bulk_process_dataset_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_show_weekly_manual boolean := TRUE;
  v_show_daily_manual boolean := TRUE;
  v_bucket text := NULL;
  v_limit_text text := NULL;
  v_offset_text text := NULL;
  v_week_ending_from_text text := NULL;
  v_week_ending_to_text text := NULL;
  v_week_ending_from date := NULL;
  v_week_ending_to date := NULL;
  v_limit integer := NULL;
  v_offset integer := 0;
  v_out jsonb;
BEGIN
  v_show_weekly_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_weekly_manual', v_filters->>'showWeeklyManual', v_filters->>'show_weekly', v_filters->>'showWeekly', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_daily_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_daily_manual', v_filters->>'showDailyManual', v_filters->>'show_daily', v_filters->>'showDaily', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_bucket := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'bucket', v_filters->>'bulk_process_bucket', v_filters->>'bulkProcessBucket', '')), ''));
  IF v_bucket NOT IN ('UNPROCESSED', 'PROCESSED') THEN
    v_bucket := NULL;
  END IF;

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

  v_week_ending_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '');
  v_week_ending_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '');

  BEGIN
    IF v_week_ending_from_text IS NOT NULL THEN
      v_week_ending_from := v_week_ending_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_from := NULL;
  END;

  BEGIN
    IF v_week_ending_to_text IS NOT NULL THEN
      v_week_ending_to := v_week_ending_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_to := NULL;
  END;

  WITH lightweight_rows AS MATERIALIZED (
    SELECT summary_row.*
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters || JSONB_BUILD_OBJECT(
        'disable_paging', TRUE,
        'disablePaging', TRUE,
        'apply_paging', FALSE,
        'applyPaging', FALSE,
        'profile', 'list',
        'context_profile', 'list',
        'include_evidence', FALSE,
        'include_compare', FALSE,
        'include_import_source_rows', FALSE
      )
    ) AS summary_row
    WHERE (summary_row.timesheet_id IS NOT NULL
       OR summary_row.contract_week_id IS NOT NULL)
      AND UPPER(COALESCE(summary_row.tools_stage, '')) <> 'ARCHIVED'
  ),
  retention_unit_members AS MATERIALIZED (
    SELECT
      lightweight_rows.timesheet_id AS row_timesheet_id,
      lightweight_rows.timesheet_id AS member_timesheet_id
    FROM lightweight_rows
    WHERE lightweight_rows.timesheet_id IS NOT NULL

    UNION

    SELECT
      lightweight_rows.timesheet_id AS row_timesheet_id,
      unit_timesheet.timesheet_id AS member_timesheet_id
    FROM lightweight_rows
    JOIN public.timesheets AS current_timesheet
      ON current_timesheet.timesheet_id = lightweight_rows.timesheet_id
     AND current_timesheet.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
    JOIN public.timesheets AS unit_timesheet
      ON unit_timesheet.booking_id = current_timesheet.booking_id
    WHERE lightweight_rows.timesheet_id IS NOT NULL
  ),
  retention_by_row AS MATERIALIZED (
    SELECT
      retention_unit_members.row_timesheet_id,
      BOOL_OR(retention.timesheet_id IS NOT NULL) AS has_retained_financial_history
    FROM retention_unit_members
    LEFT JOIN public.timesheet_financial_retention AS retention
      ON retention.timesheet_id = retention_unit_members.member_timesheet_id
    GROUP BY retention_unit_members.row_timesheet_id
  ),
  source_rows AS MATERIALIZED (
    SELECT
      lightweight_rows.*,
      current_timesheet.is_adjustment AS current_timesheet_is_adjustment,
      current_timesheet.parent_timesheet_id AS current_timesheet_parent_timesheet_id,
      current_timesheet.adjustment_origin AS current_timesheet_adjustment_origin,
      current_timesheet.correction_id AS current_timesheet_correction_id,
      current_timesheet.correction_kind AS current_timesheet_correction_kind,
      (
        COALESCE(current_timesheet.is_adjustment, FALSE) = TRUE
        AND (
          LEFT(UPPER(COALESCE(NULLIF(BTRIM(current_timesheet.adjustment_origin::text), ''), '')), 7) = 'IMPORT_'
          OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_id::text, '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_kind::text, '')), '') IS NOT NULL
        )
      ) AS is_import_derived_adjustment_calc,
      (
        (
          COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE
          OR COALESCE(current_timesheet.is_adjustment, FALSE) = TRUE
          OR current_timesheet.parent_timesheet_id IS NOT NULL
        )
        AND (
          UPPER(COALESCE(lightweight_rows.route_family, '')) IN ('MANUAL', 'MANUAL_NON_QR', 'QR')
          OR UPPER(COALESCE(lightweight_rows.submission_mode, lightweight_rows.submission_mode_snapshot, '')) = 'MANUAL'
          OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE
          OR UPPER(COALESCE(lightweight_rows.qr_status, '')) IN ('PENDING', 'SIGNED', 'SCANNED', 'USED')
        )
        AND NOT (
          COALESCE(current_timesheet.is_adjustment, FALSE) = TRUE
          AND (
            LEFT(UPPER(COALESCE(NULLIF(BTRIM(current_timesheet.adjustment_origin::text), ''), '')), 7) = 'IMPORT_'
            OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_id::text, '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_kind::text, '')), '') IS NOT NULL
          )
        )
      ) AS is_user_created_manual_qr_adjustment_calc,
      COALESCE(retention_by_row.has_retained_financial_history, FALSE) AS has_retained_financial_history_calc
    FROM lightweight_rows
    LEFT JOIN public.timesheets AS current_timesheet
      ON current_timesheet.timesheet_id = lightweight_rows.timesheet_id
    LEFT JOIN retention_by_row
      ON retention_by_row.row_timesheet_id = lightweight_rows.timesheet_id
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      source_rows.*,
      CASE
        WHEN source_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || source_rows.timesheet_id::text
        WHEN source_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || source_rows.contract_week_id::text
        ELSE NULL::text
      END AS row_key_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'QR' OR COALESCE(source_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(source_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') THEN 'NHSP'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'QR' OR COALESCE(source_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(source_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.underlying_channel_family, source_rows.route_family, '')) = 'QR' OR COALESCE(source_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.underlying_channel_family, source_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(source_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      CASE
        WHEN UPPER(COALESCE(source_rows.sheet_scope, source_rows.route_subfamily, '')) = 'DAILY' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type_calc,
      CASE
        WHEN source_rows.timesheet_id IS NULL
          OR UPPER(COALESCE(source_rows.processing_status, source_rows.tools_stage, source_rows.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(source_rows.summary_stage, '')) = 'UNPROCESSED' THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(source_rows.invoice_is_paid, FALSE) = TRUE
        OR COALESCE(source_rows.invoice_segments_locked, 0) > 0
      ) AS locked_calc,
      COALESCE(source_rows.is_authorised, FALSE) AS authorised_calc,
      (
        UPPER(COALESCE(source_rows.processing_status, '')) = 'PENDING_AUTH'
        OR UPPER(COALESCE(source_rows.processing_status, '')) = 'READY_FOR_HR'
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(source_rows.qr_status, '')) = 'PENDING'
        AND source_rows.timesheet_id IS NOT NULL
      ) AS qr_unsigned_blocked_calc,
      (
        COALESCE(source_rows.validation_status, '') <> ''
        AND UPPER(COALESCE(source_rows.validation_status, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN', 'OK', 'VALID')
      ) AS hr_validation_awaiting_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HR'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'HR'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      MD5(CONCAT_WS('|',
        COALESCE(source_rows.timesheet_id::text, ''),
        COALESCE(source_rows.contract_week_id::text, ''),
        COALESCE(source_rows.processing_status, ''),
        COALESCE(source_rows.summary_stage, ''),
        COALESCE(source_rows.tools_stage, ''),
        COALESCE(source_rows.authorised_at_utc::text, ''),
        COALESCE(source_rows.authorised_at_server::text, ''),
        COALESCE(source_rows.processed_at_utc::text, ''),
        COALESCE(source_rows.paid_at_utc::text, ''),
        COALESCE(source_rows.invoice_segments_locked::text, ''),
        COALESCE(source_rows.route_family, ''),
        COALESCE(source_rows.route_subfamily, ''),
        COALESCE(source_rows.validation_status, ''),
        COALESCE(source_rows.attached_evidence_count::text, ''),
        COALESCE(source_rows.primary_artifact_storage_key, ''),
        COALESCE(source_rows.is_import_derived_adjustment_calc::text, ''),
        COALESCE(source_rows.is_user_created_manual_qr_adjustment_calc::text, ''),
        COALESCE(source_rows.current_timesheet_adjustment_origin::text, ''),
        COALESCE(source_rows.current_timesheet_correction_id::text, ''),
        COALESCE(source_rows.current_timesheet_correction_kind::text, ''),
        COALESCE(source_rows.has_retained_financial_history_calc::text, 'false')
      )) AS row_signature_calc
    FROM source_rows
    WHERE (v_week_ending_from IS NULL OR source_rows.week_ending_date >= v_week_ending_from)
      AND (v_week_ending_to IS NULL OR source_rows.week_ending_date <= v_week_ending_to)
  ),
  eligibility_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      (
        (
          classified_rows.route_family_calc = 'MANUAL_NON_QR'
          OR COALESCE(classified_rows.is_user_created_manual_qr_adjustment_calc, FALSE) = TRUE
        )
        AND COALESCE(classified_rows.is_import_derived_adjustment_calc, FALSE) = FALSE
        AND classified_rows.route_family_calc NOT IN ('IMPORT_AUTHORITATIVE', 'ELECTRONIC')
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
      ) AS bulk_process_user_adjustment_eligible_calc
    FROM classified_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      eligibility_rows.*,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      ) AS can_save_calc,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      ) AS can_edit_timesheet_data_calc,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
        AND eligibility_rows.bulk_process_bucket_calc = 'UNPROCESSED'
      ) AS can_process_calc,
      (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
        AND eligibility_rows.bulk_process_bucket_calc = 'PROCESSED'
      ) AS unprocess_action_visible_calc,
      (
        (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
        AND eligibility_rows.bulk_process_bucket_calc = 'PROCESSED'
      )
        AND COALESCE(eligibility_rows.has_retained_financial_history_calc, FALSE) = FALSE
      ) AS can_unprocess_calc,
      CASE
        WHEN (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
        AND eligibility_rows.bulk_process_bucket_calc = 'PROCESSED'
      )
         AND COALESCE(eligibility_rows.has_retained_financial_history_calc, FALSE) = TRUE
        THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'::text
        ELSE NULL::text
      END AS unprocess_block_reason_calc,
      (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.requires_authorisation_calc = TRUE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.qr_unsigned_blocked_calc = FALSE
      ) AS can_bulk_authorise_calc,
      (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = TRUE
      ) AS can_bulk_unauthorise_calc,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      ) AS can_manage_evidence_calc,
      (
        eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND COALESCE(eligibility_rows.is_adjusted, FALSE) = FALSE
      ) AS can_add_additional_manual_calc,
      (
        eligibility_rows.locked_calc = TRUE
        OR eligibility_rows.authorised_calc = TRUE
        OR eligibility_rows.bulk_process_user_adjustment_eligible_calc = FALSE
      ) AS review_only_calc
    FROM eligibility_rows
  ),
  evidence_target_rows AS MATERIALIZED (
    SELECT DISTINCT
      decision_rows.row_key_calc AS row_key_calc,
      decision_rows.timesheet_id AS timesheet_id,
      decision_rows.contract_week_id AS contract_week_id
    FROM decision_rows
    WHERE decision_rows.row_key_calc IS NOT NULL
      AND decision_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      AND (
        (UPPER(COALESCE(decision_rows.period_type_calc, decision_rows.sheet_scope, '')) = 'WEEKLY' AND v_show_weekly_manual = TRUE)
        OR (UPPER(COALESCE(decision_rows.period_type_calc, decision_rows.sheet_scope, '')) = 'DAILY' AND v_show_daily_manual = TRUE)
        OR UPPER(COALESCE(decision_rows.period_type_calc, decision_rows.sheet_scope, '')) NOT IN ('WEEKLY', 'DAILY')
      )
      AND (
        (
          decision_rows.bulk_process_bucket_calc = 'UNPROCESSED'
          AND (v_bucket IS NULL OR v_bucket = 'UNPROCESSED')
          AND decision_rows.can_process_calc = TRUE
        )
        OR (
          decision_rows.bulk_process_bucket_calc = 'PROCESSED'
          AND (v_bucket IS NULL OR v_bucket = 'PROCESSED')
          AND decision_rows.timesheet_id IS NOT NULL
          AND decision_rows.unprocess_action_visible_calc = TRUE
        )
      )
  ),
  evidence_kind_rows AS MATERIALIZED (
    SELECT
      evidence_target_rows.row_key_calc AS row_key_calc,
      evidence_target_rows.timesheet_id AS timesheet_id,
      evidence_target_rows.contract_week_id AS contract_week_id,
      timesheet_evidence.id AS evidence_id,
      CASE
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('TIMESHEET', 'TS') THEN 'TIMESHEET'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('MILEAGE', 'MILES', 'MILE') THEN 'MILEAGE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) = 'TRAVEL' THEN 'TRAVEL'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('ACCOMMODATION', 'ACCOM') THEN 'ACCOMMODATION'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) = 'OTHER' THEN 'OTHER'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('EXPENSE', 'EXPENSES') THEN 'EXPENSE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IS NOT NULL THEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), ''))
        ELSE 'OTHER'
      END AS evidence_kind_norm,
      NULLIF(BTRIM(COALESCE(timesheet_evidence.display_name, '')), '') AS evidence_display_name,
      NULLIF(BTRIM(COALESCE(timesheet_evidence.storage_key, '')), '') AS evidence_storage_key,
      timesheet_evidence.created_at AS evidence_created_at
    FROM evidence_target_rows AS evidence_target_rows
    JOIN public.timesheet_evidence AS timesheet_evidence
      ON timesheet_evidence.timesheet_id = evidence_target_rows.timesheet_id
    WHERE evidence_target_rows.timesheet_id IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(timesheet_evidence.storage_key, '')), '') IS NOT NULL

    UNION ALL

    SELECT
      evidence_target_rows.row_key_calc AS row_key_calc,
      NULL::uuid AS timesheet_id,
      evidence_target_rows.contract_week_id AS contract_week_id,
      manual_timesheet_queue.id AS evidence_id,
      CASE
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('TIMESHEET', 'TS') THEN 'TIMESHEET'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('MILEAGE', 'MILES', 'MILE') THEN 'MILEAGE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) = 'TRAVEL' THEN 'TRAVEL'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('ACCOMMODATION', 'ACCOM') THEN 'ACCOMMODATION'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) = 'OTHER' THEN 'OTHER'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('EXPENSE', 'EXPENSES') THEN 'EXPENSE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IS NOT NULL THEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), ''))
        ELSE 'OTHER'
      END AS evidence_kind_norm,
      NULLIF(BTRIM(COALESCE(manual_timesheet_queue.original_filename, '')), '') AS evidence_display_name,
      NULLIF(BTRIM(COALESCE(manual_timesheet_queue.r2_key, '')), '') AS evidence_storage_key,
      manual_timesheet_queue.uploaded_at_utc AS evidence_created_at
    FROM evidence_target_rows AS evidence_target_rows
    JOIN public.manual_timesheet_queue AS manual_timesheet_queue
      ON evidence_target_rows.timesheet_id IS NULL
     AND evidence_target_rows.contract_week_id IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'contract_week_id', '')), '') = evidence_target_rows.contract_week_id::text
    WHERE UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.status, '')), '')) = 'STAGED'
      AND manual_timesheet_queue.timesheet_id IS NULL
      AND NULLIF(BTRIM(COALESCE(manual_timesheet_queue.r2_key, '')), '') IS NOT NULL
  ),
  evidence_kind_ranked AS MATERIALIZED (
    SELECT
      evidence_kind_rows.*,
      ROW_NUMBER() OVER (
        PARTITION BY evidence_kind_rows.row_key_calc
        ORDER BY
          CASE evidence_kind_rows.evidence_kind_norm
            WHEN 'TIMESHEET' THEN 1
            WHEN 'MILEAGE' THEN 2
            WHEN 'TRAVEL' THEN 3
            WHEN 'ACCOMMODATION' THEN 4
            WHEN 'OTHER' THEN 5
            ELSE 6
          END,
          evidence_kind_rows.evidence_kind_norm ASC,
          evidence_kind_rows.evidence_created_at ASC NULLS LAST,
          evidence_kind_rows.evidence_id ASC
      ) AS evidence_rank
    FROM evidence_kind_rows
  ),
  evidence_kind_counts AS MATERIALIZED (
    SELECT
      evidence_kind_ranked.row_key_calc AS row_key_calc,
      evidence_kind_ranked.evidence_kind_norm AS evidence_kind_norm,
      COUNT(*)::integer AS evidence_kind_count,
      CASE evidence_kind_ranked.evidence_kind_norm
        WHEN 'TIMESHEET' THEN 1
        WHEN 'MILEAGE' THEN 2
        WHEN 'TRAVEL' THEN 3
        WHEN 'ACCOMMODATION' THEN 4
        WHEN 'OTHER' THEN 5
        ELSE 6
      END AS evidence_kind_sort
    FROM evidence_kind_ranked
    GROUP BY evidence_kind_ranked.row_key_calc, evidence_kind_ranked.evidence_kind_norm
  ),
  evidence_kind_extra_badges AS MATERIALIZED (
    SELECT
      evidence_kind_counts.row_key_calc AS row_key_calc,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'kind', evidence_kind_counts.evidence_kind_norm,
          'present', evidence_kind_counts.evidence_kind_count > 0,
          'has_evidence', evidence_kind_counts.evidence_kind_count > 0,
          'count', evidence_kind_counts.evidence_kind_count
        )
        ORDER BY evidence_kind_counts.evidence_kind_sort, evidence_kind_counts.evidence_kind_norm
      ) AS extra_evidence_badges
    FROM evidence_kind_counts
    WHERE evidence_kind_counts.evidence_kind_norm NOT IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER')
    GROUP BY evidence_kind_counts.row_key_calc
  ),
  evidence_kind_summary AS MATERIALIZED (
    SELECT
      evidence_kind_ranked.row_key_calc AS row_key_calc,
      NULLIF(MAX(evidence_kind_ranked.timesheet_id::text), '')::uuid AS timesheet_id,
      NULLIF(MAX(evidence_kind_ranked.contract_week_id::text), '')::uuid AS contract_week_id,
      COUNT(*)::integer AS total_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'TIMESHEET')::integer AS timesheet_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'MILEAGE')::integer AS mileage_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'TRAVEL')::integer AS travel_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'ACCOMMODATION')::integer AS accommodation_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'OTHER')::integer AS other_evidence_count,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN evidence_kind_ranked.evidence_id::text ELSE NULL::text END) AS primary_artifact_id,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN evidence_kind_ranked.evidence_kind_norm ELSE NULL::text END) AS primary_artifact_kind,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN evidence_kind_ranked.evidence_storage_key ELSE NULL::text END) AS primary_artifact_storage_key,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN COALESCE(evidence_kind_ranked.evidence_display_name, REGEXP_REPLACE(evidence_kind_ranked.evidence_storage_key, '^.*/', '')) ELSE NULL::text END) AS primary_artifact_display_name
    FROM evidence_kind_ranked
    GROUP BY evidence_kind_ranked.row_key_calc
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      (JSONB_BUILD_OBJECT(
        'row_key',
        decision_rows.row_key_calc,
        'stable_row_id',
        COALESCE(decision_rows.timesheet_id::text, decision_rows.contract_week_id::text),
        'row_type',
        CASE WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet' ELSE 'contract_week' END,
        'timesheet_id',
        decision_rows.timesheet_id,
        'current_timesheet_id',
        decision_rows.timesheet_id,
        'requested_timesheet_id',
        decision_rows.timesheet_id,
        'expected_timesheet_id',
        decision_rows.timesheet_id,
        'contract_week_id',
        decision_rows.contract_week_id,
        'contract_id',
        decision_rows.contract_id,
        'candidate_id',
        decision_rows.candidate_id,
        'candidate_name',
        decision_rows.candidate_name,
        'candidate_display_name',
        decision_rows.candidate_display_name,
        'candidate_first_name',
        NULL::text,
        'candidate_surname',
        NULL::text,
        'client_id',
        decision_rows.client_id,
        'client_name',
        decision_rows.client_name,
        'booking_id',
        decision_rows.booking_id,
        'external_ref',
        decision_rows.booking_id,
        'occupant_key_norm',
        decision_rows.occupant_key_norm,
        'hospital_norm',
        decision_rows.hospital_norm,
        'candidate_hint_text',
        COALESCE(decision_rows.candidate_hint_text, '{}'::jsonb),
        'week_ending_date',
        decision_rows.week_ending_date,
        'contract_week_ending_date',
        decision_rows.week_ending_date,
        'work_date',
        decision_rows.work_date,
        'date',
        COALESCE(decision_rows.work_date, decision_rows.week_ending_date),
        'shift_date',
        decision_rows.work_date,
        'period_type',
        decision_rows.period_type_calc,
        'sheet_scope',
        decision_rows.sheet_scope,
        'submission_mode',
        decision_rows.submission_mode,
        'submission_mode_snapshot',
        decision_rows.submission_mode_snapshot,
        'basis',
        decision_rows.basis,
        'route_type',
        decision_rows.route_type,
        'route_display',
        decision_rows.route_display,
        'route_family',
        decision_rows.route_family_calc,
        'route_subfamily',
        decision_rows.route_subfamily_calc,
        'underlying_channel_family',
        decision_rows.underlying_channel_family_calc,
        'bulk_process_user_adjustment_eligible',
        decision_rows.bulk_process_user_adjustment_eligible_calc,
        'is_import_authoritative',
        decision_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
        'is_import_derived_adjustment',
        COALESCE(decision_rows.is_import_derived_adjustment_calc, FALSE),
        'is_user_created_manual_qr_adjustment',
        COALESCE(decision_rows.is_user_created_manual_qr_adjustment_calc, FALSE),
        'adjustment_source',
        CASE
          WHEN decision_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_DERIVED'
          WHEN decision_rows.is_user_created_manual_qr_adjustment_calc = TRUE THEN 'USER_CREATED_MANUAL_QR'
          ELSE NULL::text
        END,
        'bulk_process_excluded_reason',
        CASE
          WHEN decision_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_AUTHORITATIVE_ADJUSTED_HOURS'
          ELSE NULL::text
        END,
        'current_timesheet_is_adjustment',
        decision_rows.current_timesheet_is_adjustment,
        'parent_timesheet_id',
        decision_rows.current_timesheet_parent_timesheet_id,
        'adjustment_origin',
        decision_rows.current_timesheet_adjustment_origin,
        'correction_id',
        decision_rows.current_timesheet_correction_id,
        'correction_kind',
        decision_rows.current_timesheet_correction_kind,
        'summary_stage',
        decision_rows.summary_stage,
        'tools_stage',
        decision_rows.tools_stage,
        'processing_status',
        decision_rows.processing_status
      )
      || JSONB_BUILD_OBJECT(
        'processing_status_display',
        decision_rows.processing_status_display
      )
      || JSONB_BUILD_OBJECT(
        'bulk_process_bucket',
        decision_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification',
        decision_rows.bulk_authorise_classification_calc,
        'bulk_authorise_section',
        CASE WHEN decision_rows.can_bulk_authorise_calc THEN 'processed_eligible' WHEN decision_rows.can_bulk_unauthorise_calc THEN 'authorised_eligible' ELSE NULL::text END,
        'authorised_at_utc',
        decision_rows.authorised_at_utc,
        'authorised_at_server',
        decision_rows.authorised_at_server,
        'processed_at_utc',
        decision_rows.processed_at_utc,
        'is_authorised',
        decision_rows.authorised_calc,
        'locked',
        decision_rows.locked_calc,
        'total_hours',
        decision_rows.total_hours,
        'total_pay_ex_vat',
        decision_rows.total_pay_ex_vat,
        'total_charge_ex_vat',
        decision_rows.total_charge_ex_vat,
        'margin_ex_vat',
        decision_rows.margin_ex_vat,
        'net_delta_ex_vat',
        decision_rows.net_delta_ex_vat,
        'paid_at_utc',
        decision_rows.paid_at_utc,
        'pay_icon_code',
        decision_rows.pay_icon_code,
        'pay_status_code',
        decision_rows.pay_status_code,
        'pay_paid_at_utc',
        decision_rows.pay_paid_at_utc,
        'ready_to_pay',
        FALSE,
        'pay_on_hold',
        FALSE,
        'invoice_is_paid',
        decision_rows.invoice_is_paid,
        'invoice_issue_stage',
        decision_rows.invoice_issue_stage,
        'invoice_segment_stage',
        decision_rows.invoice_segment_stage,
        'invoice_segments_total',
        COALESCE(decision_rows.invoice_segments_total, 0),
        'invoice_segments_locked',
        COALESCE(decision_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked',
        COALESCE(decision_rows.invoice_segments_unlocked, 0),
        'issue_codes',
        COALESCE(TO_JSONB(decision_rows.issue_codes), '[]'::jsonb),
        'validation_status',
        decision_rows.validation_status,
        'validation_summary',
        decision_rows.validation_summary,
        'validation_pre_validated',
        FALSE,
        'hr_validation_awaiting',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_required_for_invoice',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_satisfied',
        NOT decision_rows.hr_validation_awaiting_calc,
        'hr_crosscheck_status',
        decision_rows.hr_crosscheck_status,
        'hr_crosscheck_issues',
        COALESCE(TO_JSONB(decision_rows.hr_crosscheck_issues), '[]'::jsonb),
        'qr_status',
        decision_rows.qr_status,
        'is_qr',
        decision_rows.is_qr,
        'qr_signed_at_utc',
        NULL::text,
        'can_allow_qr_again',
        FALSE,
        'can_allow_electronic_again',
        FALSE,
        'can_switch_to_manual',
        FALSE
      )
      || JSONB_BUILD_OBJECT(
        'can_revert_to_electronic',
        FALSE,
        'can_convert_qr_to_manual_only',
        FALSE,
        'qr_email_can_send_now',
        FALSE,
        'qr_email_recipient_available',
        FALSE,
        'is_adjusted',
        (COALESCE(decision_rows.current_timesheet_is_adjustment, FALSE) OR COALESCE(decision_rows.is_adjusted, FALSE)),
        'is_adjustment',
        (COALESCE(decision_rows.current_timesheet_is_adjustment, FALSE) OR COALESCE(decision_rows.is_adjusted, FALSE)),
        'needs_attention',
        decision_rows.needs_attention,
        'has_rate_issue',
        decision_rows.has_rate_issue,
        'has_pay_channel_issue',
        decision_rows.has_pay_channel_issue,
        'client_no_timesheet_required',
        decision_rows.client_no_timesheet_required,
        'client_autoprocess_hr',
        decision_rows.client_autoprocess_hr,
        'client_is_nhsp',
        decision_rows.client_is_nhsp,
        'has_deviation_marker',
        FALSE,
        'deviation_marker_reason',
        NULL::text,
        'nhsp_highlight_red',
        FALSE,
        'nhsp_highlight_reason',
        NULL::text,
        'nhsp_deviation_pct',
        NULL::numeric,
        'nhsp_is_ad_hoc',
        FALSE,
        'has_any_evidence',
        COALESCE(evidence_kind_summary.total_evidence_count > 0, decision_rows.has_any_evidence),
        'attached_evidence_count',
        COALESCE(evidence_kind_summary.total_evidence_count, decision_rows.attached_evidence_count, 0),
        'evidence_count',
        COALESCE(evidence_kind_summary.total_evidence_count, decision_rows.attached_evidence_count, 0),
        'primary_artifact_id',
        evidence_kind_summary.primary_artifact_id,
        'primary_artifact_kind',
        evidence_kind_summary.primary_artifact_kind,
        'primary_artifact_storage_key',
        COALESCE(evidence_kind_summary.primary_artifact_storage_key, decision_rows.primary_artifact_storage_key),
        'primary_artifact_display_name',
        COALESCE(evidence_kind_summary.primary_artifact_display_name, decision_rows.primary_artifact_display_name),
        'primary_artifact_preview_mode',
        decision_rows.primary_artifact_preview_mode,
        'manual_pdf_r2_key',
        NULL::text,
        'uploaded_pdf_r2_key',
        NULL::text,
        'generated_pdf_at_utc',
        NULL::text,
        'manual_pdf_rotation_degrees',
        0,
        'queue_staged_count',
        0,
        'evidence_document_locked',
        decision_rows.locked_calc,
        'evidence_lock_reason',
        CASE WHEN decision_rows.locked_calc THEN 'invoice_locked' ELSE NULL::text END,
        'has_timesheet',
        decision_rows.timesheet_id IS NOT NULL,
        'is_contract_week_only',
        decision_rows.timesheet_id IS NULL AND decision_rows.contract_week_id IS NOT NULL,
        'timesheet_version',
        NULL::integer,
        'updated_at',
        NULL::text,
        'is_current',
        TRUE,
        'was_stale',
        FALSE,
        'timesheet_type_sort_key',
        NULL::text
      )
      || JSONB_BUILD_OBJECT(
        'processed_by_user_id',
        NULL::text,
        'processed_by_display',
        NULL::text,
        'authorised_by_user_id',
        NULL::text,
        'authorised_by_display',
        NULL::text,
        'can_save',
        decision_rows.can_save_calc,
        'can_process',
        decision_rows.can_process_calc,
        'can_unprocess',
        decision_rows.can_unprocess_calc,
        'has_retained_financial_history',
        COALESCE(decision_rows.has_retained_financial_history_calc, FALSE),
        'unprocess_block_reason',
        decision_rows.unprocess_block_reason_calc,
        'unprocess_action_visible',
        decision_rows.unprocess_action_visible_calc,
        'can_bulk_authorise',
        decision_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise',
        decision_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data',
        decision_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence',
        decision_rows.can_manage_evidence_calc,
        'can_add_additional_manual',
        decision_rows.can_add_additional_manual_calc,
        'review_only',
        decision_rows.review_only_calc,
        'row_signature',
        MD5(CONCAT_WS('|',
          decision_rows.row_signature_calc,
          COALESCE(decision_rows.bulk_process_user_adjustment_eligible_calc::text, ''),
          COALESCE(evidence_kind_summary.primary_artifact_kind, ''),
          COALESCE(evidence_kind_summary.timesheet_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.mileage_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.travel_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.accommodation_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.other_evidence_count::text, '0'),
          COALESCE(evidence_kind_extra_badges.extra_evidence_badges::text, '[]')
        ))
      )
      || JSONB_BUILD_OBJECT(
        'backend_row_signature',
        COALESCE(lifecycle_signature.signature_text, MD5(CONCAT_WS('|',
          decision_rows.row_signature_calc,
          COALESCE(decision_rows.bulk_process_user_adjustment_eligible_calc::text, ''),
          COALESCE(evidence_kind_summary.primary_artifact_kind, ''),
          COALESCE(evidence_kind_summary.timesheet_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.mileage_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.travel_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.accommodation_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.other_evidence_count::text, '0'),
          COALESCE(evidence_kind_extra_badges.extra_evidence_badges::text, '[]')
        ))),
        'mutation_row_signature',
        COALESCE(lifecycle_signature.signature_text, MD5(CONCAT_WS('|',
          decision_rows.row_signature_calc,
          COALESCE(decision_rows.bulk_process_user_adjustment_eligible_calc::text, ''),
          COALESCE(evidence_kind_summary.primary_artifact_kind, ''),
          COALESCE(evidence_kind_summary.timesheet_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.mileage_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.travel_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.accommodation_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.other_evidence_count::text, '0'),
          COALESCE(evidence_kind_extra_badges.extra_evidence_badges::text, '[]')
        )))
      )
      || JSONB_BUILD_OBJECT(
        'evidence_badges',
        (
          JSONB_BUILD_ARRAY(
            JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.mileage_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.travel_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.other_evidence_count, 0))
          )
          || COALESCE(evidence_kind_extra_badges.extra_evidence_badges, '[]'::jsonb)
        ),
        'artifact_hints',
        JSONB_BUILD_OBJECT(
          'has_any_evidence', COALESCE(evidence_kind_summary.total_evidence_count > 0, decision_rows.has_any_evidence),
          'attached_evidence_count', COALESCE(evidence_kind_summary.total_evidence_count, decision_rows.attached_evidence_count, 0),
          'primary_artifact_id', evidence_kind_summary.primary_artifact_id,
          'primary_artifact_kind', evidence_kind_summary.primary_artifact_kind,
          'primary_artifact_storage_key', COALESCE(evidence_kind_summary.primary_artifact_storage_key, decision_rows.primary_artifact_storage_key),
          'primary_artifact_display_name', COALESCE(evidence_kind_summary.primary_artifact_display_name, decision_rows.primary_artifact_display_name),
          'primary_artifact_preview_mode', decision_rows.primary_artifact_preview_mode,
          'evidence_badges', (
            JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.mileage_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.travel_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.other_evidence_count, 0))
            )
            || COALESCE(evidence_kind_extra_badges.extra_evidence_badges, '[]'::jsonb)
          )
        ),
        'action_flags',
        JSONB_BUILD_OBJECT(
          'can_save', decision_rows.can_save_calc,
          'can_process', decision_rows.can_process_calc,
          'can_unprocess', decision_rows.can_unprocess_calc,
          'has_retained_financial_history', COALESCE(decision_rows.has_retained_financial_history_calc, FALSE),
          'unprocess_block_reason', decision_rows.unprocess_block_reason_calc,
          'unprocess_action_visible', decision_rows.unprocess_action_visible_calc,
          'can_bulk_authorise', decision_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', decision_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', decision_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', decision_rows.can_manage_evidence_calc,
          'can_add_additional_manual', decision_rows.can_add_additional_manual_calc,
          'review_only', decision_rows.review_only_calc
        ),
        'row_patch',
        JSONB_BUILD_OBJECT(
          'timesheet_id', decision_rows.timesheet_id,
          'row_key', decision_rows.row_key_calc,
          'has_retained_financial_history', COALESCE(decision_rows.has_retained_financial_history_calc, FALSE),
          'can_unprocess', decision_rows.can_unprocess_calc,
          'unprocess_block_reason', decision_rows.unprocess_block_reason_calc,
          'unprocess_action_visible', decision_rows.unprocess_action_visible_calc
        ),
        'cache_invalidation_hints',
        JSONB_BUILD_OBJECT(),
        'count_deltas',
        JSONB_BUILD_OBJECT(),
        'header_loaded',
        TRUE,
        'header_only',
        TRUE,
        'editor_loaded',
        FALSE,
        'evidence_loaded',
        FALSE,
        'compare_loaded',
        FALSE,
        'full_loaded',
        FALSE,
        'schedule_pending',
        TRUE,
        'schedule_authoritative',
        FALSE,
        'loaded_layers',
        JSONB_BUILD_ARRAY('dataset_row'),
        'dataset_source',
        'timesheet_summary_lightweight_rows_v1'
      )) AS row_json
    FROM decision_rows
    LEFT JOIN LATERAL (
      SELECT NULLIF(BTRIM(COALESCE(
        lifecycle_signature_source.signature_json->>'backend_row_signature',
        lifecycle_signature_source.signature_json->>'row_signature',
        lifecycle_signature_source.signature_json->>'signature',
        ''
      )), '') AS signature_text
      FROM (
        SELECT public.timesheet_lifecycle_signature_v1(decision_rows.timesheet_id, decision_rows.contract_week_id, false) AS signature_json
      ) AS lifecycle_signature_source
    ) AS lifecycle_signature ON TRUE
    LEFT JOIN evidence_kind_summary AS evidence_kind_summary
      ON evidence_kind_summary.row_key_calc = decision_rows.row_key_calc
    LEFT JOIN evidence_kind_extra_badges AS evidence_kind_extra_badges
      ON evidence_kind_extra_badges.row_key_calc = decision_rows.row_key_calc
  ),
  manual_rows AS MATERIALIZED (
    SELECT payload_rows.row_json
    FROM payload_rows
    WHERE COALESCE((payload_rows.row_json->>'bulk_process_user_adjustment_eligible')::boolean, FALSE) = TRUE
      AND COALESCE((payload_rows.row_json->>'is_import_authoritative')::boolean, FALSE) = FALSE
      AND COALESCE(payload_rows.row_json->>'adjustment_source', '') <> 'IMPORT_DERIVED'
      AND (
        (UPPER(COALESCE(payload_rows.row_json->>'period_type', payload_rows.row_json->>'sheet_scope', '')) = 'WEEKLY' AND v_show_weekly_manual = TRUE)
        OR (UPPER(COALESCE(payload_rows.row_json->>'period_type', payload_rows.row_json->>'sheet_scope', '')) = 'DAILY' AND v_show_daily_manual = TRUE)
        OR UPPER(COALESCE(payload_rows.row_json->>'period_type', payload_rows.row_json->>'sheet_scope', '')) NOT IN ('WEEKLY', 'DAILY')
      )
  ),
  unprocessed_rows AS MATERIALIZED (
    SELECT manual_rows.row_json
    FROM manual_rows
    WHERE UPPER(COALESCE(manual_rows.row_json->>'bulk_process_bucket', '')) = 'UNPROCESSED'
      AND (v_bucket IS NULL OR v_bucket = 'UNPROCESSED')
      AND COALESCE((manual_rows.row_json->>'can_process')::boolean, FALSE) = TRUE
  ),
  processed_rows AS MATERIALIZED (
    SELECT manual_rows.row_json
    FROM manual_rows
    WHERE UPPER(COALESCE(manual_rows.row_json->>'bulk_process_bucket', '')) = 'PROCESSED'
      AND (v_bucket IS NULL OR v_bucket = 'PROCESSED')
      AND NULLIF(BTRIM(COALESCE(manual_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND COALESCE((manual_rows.row_json->>'unprocess_action_visible')::boolean, FALSE) = TRUE
  ),
  paged_unprocessed_rows AS MATERIALIZED (
    SELECT unprocessed_rows.row_json
    FROM unprocessed_rows
    ORDER BY unprocessed_rows.row_json->>'week_ending_date', unprocessed_rows.row_json->>'client_name', unprocessed_rows.row_json->>'candidate_name', unprocessed_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  paged_processed_rows AS MATERIALIZED (
    SELECT processed_rows.row_json
    FROM processed_rows
    ORDER BY processed_rows.row_json->>'week_ending_date', processed_rows.row_json->>'client_name', processed_rows.row_json->>'candidate_name', processed_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  counts AS (
    SELECT
      (SELECT COUNT(*)::integer FROM unprocessed_rows) AS unprocessed_count,
      (SELECT COUNT(*)::integer FROM processed_rows) AS processed_count
  )
  SELECT JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'show_weekly_manual', v_show_weekly_manual,
      'show_daily_manual', v_show_daily_manual,
      'bucket', v_bucket,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'week_ending_from', v_week_ending_from,
      'week_ending_to', v_week_ending_to,
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'unprocessed', counts.unprocessed_count,
      'processed', counts.processed_count,
      'total', counts.unprocessed_count + counts.processed_count
    ),
    'unprocessed_rows', COALESCE((
      SELECT JSONB_AGG(paged_unprocessed_rows.row_json ORDER BY paged_unprocessed_rows.row_json->>'week_ending_date', paged_unprocessed_rows.row_json->>'client_name', paged_unprocessed_rows.row_json->>'candidate_name', paged_unprocessed_rows.row_json->>'row_key')
      FROM paged_unprocessed_rows
    ), '[]'::jsonb),
    'processed_rows', COALESCE((
      SELECT JSONB_AGG(paged_processed_rows.row_json ORDER BY paged_processed_rows.row_json->>'week_ending_date', paged_processed_rows.row_json->>'client_name', paged_processed_rows.row_json->>'candidate_name', paged_processed_rows.row_json->>'row_key')
      FROM paged_processed_rows
    ), '[]'::jsonb)
  )
  INTO v_out
  FROM counts;

  RETURN private._candidate_dataset_overlay_v1(COALESCE(v_out, JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'show_weekly_manual', v_show_weekly_manual,
      'show_daily_manual', v_show_daily_manual,
      'bucket', v_bucket,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'week_ending_from', v_week_ending_from,
      'week_ending_to', v_week_ending_to,
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT('unprocessed', 0, 'processed', 0, 'total', 0),
    'unprocessed_rows', '[]'::jsonb,
    'processed_rows', '[]'::jsonb
  )));
END;
$function$;

CREATE OR REPLACE FUNCTION public.bulk_authorise_dataset_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_classification text := NULL;
  v_show_daily boolean := TRUE;
  v_show_weekly boolean := TRUE;
  v_show_manual boolean := TRUE;
  v_show_qr boolean := TRUE;
  v_show_electronic boolean := TRUE;
  v_validation_already boolean := TRUE;
  v_validation_awaiting boolean := TRUE;
  v_show_authorised_invoiced_unissued boolean := FALSE;
  v_limit_text text := NULL;
  v_offset_text text := NULL;
  v_limit integer := NULL;
  v_offset integer := 0;
  v_out jsonb;
BEGIN
  v_classification := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'classification', v_filters->>'classificationRaw', '')), ''));
  IF v_classification NOT IN ('TIMESHEETS', 'NHSP', 'HR') THEN
    v_classification := NULL;
  END IF;

  v_show_daily := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_daily', v_filters->>'showDaily', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_weekly := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_weekly', v_filters->>'showWeekly', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_manual', v_filters->>'showManual', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_qr := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_qr', v_filters->>'showQr', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_electronic := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_electronic', v_filters->>'showElectronic', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_validation_already := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'validation_already', v_filters->>'validationAlready', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_validation_awaiting := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'validation_awaiting', v_filters->>'validationAwaiting', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_authorised_invoiced_unissued := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_authorised_invoiced_unissued', v_filters->>'showAuthorisedInvoicedUnissued', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

  IF EXISTS(
    SELECT 1
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters||jsonb_build_object('disable_paging',true,'apply_paging',false,'profile','list','include_evidence',false,'include_compare',false,'include_import_source_rows',false)
    ) s
    JOIN public.timesheets AS correction_timesheet
      ON correction_timesheet.timesheet_id = s.timesheet_id
    WHERE correction_timesheet.is_current=true and correction_timesheet.archived_at_utc is null
      and coalesce(correction_timesheet.is_adjustment,false) and correction_timesheet.parent_timesheet_id is not null
      and correction_timesheet.correction_id is not null and upper(coalesce(correction_timesheet.adjustment_origin,''))='IMPORT_CORRECTION'
      and correction_timesheet.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      and ((upper(coalesce(s.route_type,'')) like '%NHSP%' or upper(coalesce(s.basis,'')) like 'NHSP%'
          or upper(coalesce(correction_timesheet.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='NHSP')
        = (upper(coalesce(s.route_type,'')) like '%HEALTHROSTER%' or upper(coalesce(s.basis,'')) like 'HEALTHROSTER%'
          or upper(coalesce(correction_timesheet.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER'))
  ) THEN
    RAISE EXCEPTION 'BULK_AUTHORISE_CORRECTION_SOURCE_CONFLICT' USING ERRCODE='22023';
  END IF;

  WITH lightweight_rows AS MATERIALIZED (
    SELECT
      summary_row.*,
      correction_timesheet.is_current AS correction_is_current,
      correction_timesheet.archived_at_utc AS correction_archived_at_utc,
      correction_timesheet.is_adjustment AS correction_is_adjustment,
      correction_timesheet.parent_timesheet_id AS correction_parent_timesheet_id,
      correction_timesheet.correction_id,
      correction_timesheet.adjustment_origin,
      correction_timesheet.correction_kind,
      correction_timesheet.candidate_hint_text AS correction_candidate_hint_text
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters || JSONB_BUILD_OBJECT(
        'disable_paging', TRUE,
        'disablePaging', TRUE,
        'apply_paging', FALSE,
        'applyPaging', FALSE,
        'profile', 'list',
        'context_profile', 'list',
        'include_evidence', FALSE,
        'include_compare', FALSE,
        'include_import_source_rows', FALSE
      )
    ) AS summary_row
    LEFT JOIN public.timesheets AS correction_timesheet
      ON correction_timesheet.timesheet_id = summary_row.timesheet_id
    WHERE (summary_row.timesheet_id IS NOT NULL
       OR summary_row.contract_week_id IS NOT NULL)
      AND UPPER(COALESCE(summary_row.tools_stage, '')) <> 'ARCHIVED'
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      lightweight_rows.*,
      CASE
        WHEN lightweight_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || lightweight_rows.timesheet_id::text
        WHEN lightweight_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || lightweight_rows.contract_week_id::text
        ELSE NULL::text
      END AS row_key_calc,
      CASE
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          and lightweight_rows.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          and (upper(coalesce(lightweight_rows.route_type,'')) like '%HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.basis,'')) like 'HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.correction_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER')
        THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
        THEN 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT'
        WHEN UPPER(COALESCE(lightweight_rows.underlying_channel_family, lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.underlying_channel_family, lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.sheet_scope, lightweight_rows.route_subfamily, '')) = 'DAILY' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type_calc,
      CASE
        WHEN lightweight_rows.timesheet_id IS NULL
          OR UPPER(COALESCE(lightweight_rows.processing_status, lightweight_rows.tools_stage, lightweight_rows.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(lightweight_rows.summary_stage, '')) = 'UNPROCESSED' THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(lightweight_rows.invoice_is_paid, FALSE) = TRUE
        OR COALESCE(lightweight_rows.invoice_segments_locked, 0) > 0
        OR EXISTS (
          SELECT 1
          FROM public.invoice_lines AS issued_invoice_line
          JOIN public.invoices AS issued_invoice
            ON issued_invoice.id = issued_invoice_line.invoice_id
          WHERE issued_invoice_line.timesheet_id = lightweight_rows.timesheet_id
            AND (
              issued_invoice.issued_at_utc IS NOT NULL
              OR UPPER(COALESCE(issued_invoice.status::text, '')) <> 'DRAFT'
            )
        )
      ) AS locked_calc,
      EXISTS (
        SELECT 1
        FROM public.invoice_lines AS draft_invoice_line
        JOIN public.invoices AS draft_invoice
          ON draft_invoice.id = draft_invoice_line.invoice_id
        WHERE draft_invoice_line.timesheet_id = lightweight_rows.timesheet_id
          AND draft_invoice.issued_at_utc IS NULL
          AND UPPER(COALESCE(draft_invoice.status::text, '')) = 'DRAFT'
      ) AS has_unissued_invoice_calc,
      EXISTS (
        SELECT 1
        FROM public.invoice_lines AS issued_invoice_line
        JOIN public.invoices AS issued_invoice
          ON issued_invoice.id = issued_invoice_line.invoice_id
        WHERE issued_invoice_line.timesheet_id = lightweight_rows.timesheet_id
          AND (
            issued_invoice.issued_at_utc IS NOT NULL
            OR UPPER(COALESCE(issued_invoice.status::text, '')) <> 'DRAFT'
          )
      ) AS has_issued_invoice_calc,
      COALESCE(lightweight_rows.is_authorised, FALSE) AS authorised_calc,
      (
        UPPER(COALESCE(lightweight_rows.processing_status, '')) = 'PENDING_AUTH'
        OR UPPER(COALESCE(lightweight_rows.processing_status, '')) = 'READY_FOR_HR'
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(lightweight_rows.qr_status, '')) = 'PENDING'
        AND lightweight_rows.timesheet_id IS NOT NULL
      ) AS qr_unsigned_blocked_calc,
      (
        COALESCE(lightweight_rows.validation_status, '') <> ''
        AND UPPER(COALESCE(lightweight_rows.validation_status, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN', 'OK', 'VALID')
      ) AS hr_validation_awaiting_calc,
      CASE
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          and (upper(coalesce(lightweight_rows.route_type,'')) like '%HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.basis,'')) like 'HEALTHROSTER%'
            or upper(coalesce(lightweight_rows.correction_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER') then 'HR'
        WHEN lightweight_rows.correction_is_current=true and lightweight_rows.correction_archived_at_utc is null
          and coalesce(lightweight_rows.correction_is_adjustment,false) and lightweight_rows.correction_parent_timesheet_id is not null
          and lightweight_rows.correction_id is not null and upper(coalesce(lightweight_rows.adjustment_origin,''))='IMPORT_CORRECTION' then 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HR'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      MD5(CONCAT_WS('|',
        COALESCE(lightweight_rows.timesheet_id::text, ''),
        COALESCE(lightweight_rows.contract_week_id::text, ''),
        COALESCE(lightweight_rows.processing_status, ''),
        COALESCE(lightweight_rows.summary_stage, ''),
        COALESCE(lightweight_rows.tools_stage, ''),
        COALESCE(lightweight_rows.authorised_at_utc::text, ''),
        COALESCE(lightweight_rows.authorised_at_server::text, ''),
        COALESCE(lightweight_rows.processed_at_utc::text, ''),
        COALESCE(lightweight_rows.paid_at_utc::text, ''),
        COALESCE(lightweight_rows.invoice_segments_locked::text, ''),
        COALESCE(lightweight_rows.route_family, ''),
        COALESCE(lightweight_rows.route_subfamily, ''),
        COALESCE(lightweight_rows.validation_status, ''),
        COALESCE(lightweight_rows.attached_evidence_count::text, ''),
        COALESCE(lightweight_rows.primary_artifact_storage_key, '')
      )) AS row_signature_calc
    FROM lightweight_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.requires_authorisation_calc = TRUE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.qr_unsigned_blocked_calc = FALSE
        AND classified_rows.has_unissued_invoice_calc = FALSE
        AND classified_rows.has_issued_invoice_calc = FALSE
      ) AS can_bulk_authorise_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = TRUE
        AND classified_rows.has_unissued_invoice_calc = FALSE
        AND classified_rows.has_issued_invoice_calc = FALSE
      ) AS can_bulk_unauthorise_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_save_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
        AND classified_rows.bulk_process_bucket_calc = 'UNPROCESSED'
      ) AS can_process_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
        AND classified_rows.bulk_process_bucket_calc = 'PROCESSED'
      ) AS can_unprocess_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR (classified_rows.contract_week_id IS NOT NULL AND classified_rows.route_family_calc = 'MANUAL_NON_QR'))
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.route_family_calc <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence_calc,
      (
        classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND COALESCE(classified_rows.is_adjusted, FALSE) = FALSE
      ) AS can_add_additional_manual_calc,
      (
        classified_rows.locked_calc = TRUE
        OR classified_rows.authorised_calc = TRUE
        OR classified_rows.route_family_calc <> 'MANUAL_NON_QR'
      ) AS review_only_calc
    FROM classified_rows
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      (JSONB_BUILD_OBJECT(
        'row_key',
        decision_rows.row_key_calc,
        'stable_row_id',
        COALESCE(decision_rows.timesheet_id::text, decision_rows.contract_week_id::text),
        'row_type',
        CASE WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet' ELSE 'contract_week' END,
        'timesheet_id',
        decision_rows.timesheet_id,
        'current_timesheet_id',
        decision_rows.timesheet_id,
        'requested_timesheet_id',
        decision_rows.timesheet_id,
        'expected_timesheet_id',
        decision_rows.timesheet_id,
        'contract_week_id',
        decision_rows.contract_week_id,
        'contract_id',
        decision_rows.contract_id,
        'candidate_id',
        decision_rows.candidate_id,
        'candidate_name',
        decision_rows.candidate_name,
        'candidate_display_name',
        decision_rows.candidate_display_name,
        'candidate_first_name',
        NULL::text,
        'candidate_surname',
        NULL::text,
        'client_id',
        decision_rows.client_id,
        'client_name',
        decision_rows.client_name,
        'booking_id',
        decision_rows.booking_id,
        'external_ref',
        decision_rows.booking_id,
        'occupant_key_norm',
        decision_rows.occupant_key_norm,
        'hospital_norm',
        decision_rows.hospital_norm,
        'candidate_hint_text',
        COALESCE(decision_rows.candidate_hint_text, '{}'::jsonb),
        'week_ending_date',
        decision_rows.week_ending_date,
        'contract_week_ending_date',
        decision_rows.week_ending_date,
        'work_date',
        decision_rows.work_date,
        'date',
        COALESCE(decision_rows.work_date, decision_rows.week_ending_date),
        'shift_date',
        decision_rows.work_date,
        'period_type',
        decision_rows.period_type_calc,
        'sheet_scope',
        decision_rows.sheet_scope,
        'submission_mode',
        decision_rows.submission_mode,
        'submission_mode_snapshot',
        decision_rows.submission_mode_snapshot,
        'basis',
        decision_rows.basis,
        'route_type',
        decision_rows.route_type,
        'route_display',
        decision_rows.route_display,
        'route_family',
        decision_rows.route_family_calc,
        'route_subfamily',
        decision_rows.route_subfamily_calc,
        'underlying_channel_family',
        decision_rows.underlying_channel_family_calc,
        'summary_stage',
        decision_rows.summary_stage,
        'tools_stage',
        decision_rows.tools_stage,
        'processing_status',
        decision_rows.processing_status,
        'processing_status_display',
        decision_rows.processing_status_display
      )
      || JSONB_BUILD_OBJECT(
        'bulk_process_bucket',
        decision_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification',
        decision_rows.bulk_authorise_classification_calc,
        'correction_id',decision_rows.correction_id,
        'correction_kind',decision_rows.correction_kind,
        'adjustment_origin',decision_rows.adjustment_origin,
        'correction_source_system',case when decision_rows.correction_is_current=true and decision_rows.correction_archived_at_utc is null
          and coalesce(decision_rows.correction_is_adjustment,false) and decision_rows.correction_parent_timesheet_id is not null
          and decision_rows.correction_id is not null and upper(coalesce(decision_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          then case when decision_rows.bulk_authorise_classification_calc='HR' then 'HEALTHROSTER' else 'NHSP' end end,
        'correction_display_label',case when decision_rows.correction_is_current=true and decision_rows.correction_archived_at_utc is null
          and coalesce(decision_rows.correction_is_adjustment,false) and decision_rows.correction_parent_timesheet_id is not null
          and decision_rows.correction_id is not null and upper(coalesce(decision_rows.adjustment_origin,''))='IMPORT_CORRECTION'
          then case
            when decision_rows.bulk_authorise_classification_calc='HR' and decision_rows.correction_kind='CHANGED_HOURS_REVERSAL' then 'HealthRoster Reversal'
            when decision_rows.bulk_authorise_classification_calc='HR' and decision_rows.correction_kind='CHANGED_HOURS_REPLACEMENT' then 'HealthRoster Corrected Hours'
            when decision_rows.bulk_authorise_classification_calc='NHSP' and decision_rows.correction_kind='CHANGED_HOURS_REVERSAL' then 'NHSP Reversal'
            when decision_rows.bulk_authorise_classification_calc='NHSP' and decision_rows.correction_kind='CHANGED_HOURS_REPLACEMENT' then 'NHSP Corrected Hours' end end,
        'bulk_authorise_section',
        CASE
          WHEN decision_rows.can_bulk_authorise_calc THEN 'processed_eligible'
          WHEN decision_rows.timesheet_id IS NOT NULL
            AND decision_rows.authorised_calc = TRUE
            AND decision_rows.locked_calc = FALSE
            AND decision_rows.has_issued_invoice_calc = FALSE
            AND (decision_rows.has_unissued_invoice_calc = FALSE OR v_show_authorised_invoiced_unissued = TRUE)
            THEN 'authorised_eligible'
          ELSE NULL::text
        END,
        'authorised_at_utc',
        decision_rows.authorised_at_utc,
        'authorised_at_server',
        decision_rows.authorised_at_server,
        'processed_at_utc',
        decision_rows.processed_at_utc,
        'is_authorised',
        decision_rows.authorised_calc,
        'locked',
        decision_rows.locked_calc,
        'total_hours',
        decision_rows.total_hours,
        'total_pay_ex_vat',
        decision_rows.total_pay_ex_vat,
        'total_charge_ex_vat',
        decision_rows.total_charge_ex_vat,
        'margin_ex_vat',
        decision_rows.margin_ex_vat,
        'net_delta_ex_vat',
        decision_rows.net_delta_ex_vat,
        'paid_at_utc',
        decision_rows.paid_at_utc,
        'pay_icon_code',
        decision_rows.pay_icon_code,
        'pay_status_code',
        decision_rows.pay_status_code,
        'pay_paid_at_utc',
        decision_rows.pay_paid_at_utc,
        'ready_to_pay',
        FALSE,
        'pay_on_hold',
        FALSE,
        'invoice_is_paid',
        decision_rows.invoice_is_paid,
        'invoice_issue_stage',
        CASE
          WHEN decision_rows.has_issued_invoice_calc THEN 'ISSUED'
          WHEN decision_rows.has_unissued_invoice_calc THEN 'DRAFT'
          ELSE NULL::text
        END,
        'has_unissued_invoice',
        decision_rows.has_unissued_invoice_calc,
        'has_issued_invoice',
        decision_rows.has_issued_invoice_calc,
        'is_invoiced',
        decision_rows.has_unissued_invoice_calc OR decision_rows.has_issued_invoice_calc,
        'invoice_segment_stage',
        decision_rows.invoice_segment_stage,
        'invoice_segments_total',
        COALESCE(decision_rows.invoice_segments_total, 0),
        'invoice_segments_locked',
        COALESCE(decision_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked',
        COALESCE(decision_rows.invoice_segments_unlocked, 0),
        'issue_codes',
        COALESCE(TO_JSONB(decision_rows.issue_codes), '[]'::jsonb),
        'validation_status',
        decision_rows.validation_status,
        'validation_summary',
        decision_rows.validation_summary,
        'validation_pre_validated',
        FALSE,
        'hr_validation_awaiting',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_required_for_invoice',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_satisfied',
        NOT decision_rows.hr_validation_awaiting_calc,
        'hr_crosscheck_status',
        decision_rows.hr_crosscheck_status,
        'hr_crosscheck_issues',
        COALESCE(TO_JSONB(decision_rows.hr_crosscheck_issues), '[]'::jsonb),
        'qr_status',
        decision_rows.qr_status,
        'is_qr',
        decision_rows.is_qr,
        'qr_signed_at_utc',
        NULL::text,
        'can_allow_qr_again',
        FALSE,
        'can_allow_electronic_again',
        FALSE,
        'can_switch_to_manual',
        FALSE
      )
      || JSONB_BUILD_OBJECT(
        'can_revert_to_electronic',
        FALSE,
        'can_convert_qr_to_manual_only',
        FALSE,
        'qr_email_can_send_now',
        FALSE,
        'qr_email_recipient_available',
        FALSE,
        'is_adjusted',
        decision_rows.is_adjusted,
        'is_adjustment',
        decision_rows.is_adjusted,
        'needs_attention',
        decision_rows.needs_attention,
        'has_rate_issue',
        decision_rows.has_rate_issue,
        'has_pay_channel_issue',
        decision_rows.has_pay_channel_issue,
        'client_no_timesheet_required',
        decision_rows.client_no_timesheet_required,
        'client_autoprocess_hr',
        decision_rows.client_autoprocess_hr,
        'client_is_nhsp',
        decision_rows.client_is_nhsp,
        'has_deviation_marker',
        FALSE,
        'deviation_marker_reason',
        NULL::text,
        'nhsp_highlight_red',
        FALSE,
        'nhsp_highlight_reason',
        NULL::text,
        'nhsp_deviation_pct',
        NULL::numeric,
        'nhsp_is_ad_hoc',
        FALSE,
        'has_any_evidence',
        decision_rows.has_any_evidence,
        'attached_evidence_count',
        decision_rows.attached_evidence_count,
        'evidence_count',
        decision_rows.attached_evidence_count,
        'primary_artifact_id',
        NULL::text,
        'primary_artifact_kind',
        CASE WHEN decision_rows.primary_artifact_storage_key IS NOT NULL THEN 'TIMESHEET' ELSE NULL::text END,
        'primary_artifact_storage_key',
        decision_rows.primary_artifact_storage_key,
        'primary_artifact_display_name',
        decision_rows.primary_artifact_display_name,
        'primary_artifact_preview_mode',
        decision_rows.primary_artifact_preview_mode,
        'manual_pdf_r2_key',
        NULL::text,
        'uploaded_pdf_r2_key',
        NULL::text,
        'generated_pdf_at_utc',
        NULL::text,
        'manual_pdf_rotation_degrees',
        0,
        'queue_staged_count',
        0,
        'evidence_document_locked',
        decision_rows.locked_calc,
        'evidence_lock_reason',
        CASE WHEN decision_rows.locked_calc THEN 'invoice_locked' ELSE NULL::text END,
        'has_timesheet',
        decision_rows.timesheet_id IS NOT NULL,
        'is_contract_week_only',
        decision_rows.timesheet_id IS NULL AND decision_rows.contract_week_id IS NOT NULL,
        'timesheet_version',
        NULL::integer,
        'updated_at',
        NULL::text,
        'is_current',
        TRUE,
        'was_stale',
        FALSE,
        'timesheet_type_sort_key',
        NULL::text
      )
      || JSONB_BUILD_OBJECT(
        'processed_by_user_id',
        NULL::text,
        'processed_by_display',
        NULL::text,
        'authorised_by_user_id',
        NULL::text,
        'authorised_by_display',
        NULL::text,
        'can_save',
        decision_rows.can_save_calc,
        'can_process',
        decision_rows.can_process_calc,
        'can_unprocess',
        decision_rows.can_unprocess_calc,
        'can_bulk_authorise',
        decision_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise',
        decision_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data',
        decision_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence',
        decision_rows.can_manage_evidence_calc,
        'can_add_additional_manual',
        decision_rows.can_add_additional_manual_calc,
        'review_only',
        decision_rows.review_only_calc,
        'row_signature',
        decision_rows.row_signature_calc
      )
      || JSONB_BUILD_OBJECT(
        'backend_row_signature',
        COALESCE(lifecycle_signature.signature_text, decision_rows.row_signature_calc),
        'mutation_row_signature',
        COALESCE(lifecycle_signature.signature_text, decision_rows.row_signature_calc)
      )
      || JSONB_BUILD_OBJECT(
        'evidence_badges',
        JSONB_BUILD_ARRAY(
          JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(decision_rows.has_any_evidence, FALSE), 'has_evidence', COALESCE(decision_rows.has_any_evidence, FALSE)),
          JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', FALSE, 'has_evidence', FALSE)
        ),
        'artifact_hints',
        JSONB_BUILD_OBJECT(
          'has_any_evidence', decision_rows.has_any_evidence,
          'attached_evidence_count', decision_rows.attached_evidence_count,
          'primary_artifact_storage_key', decision_rows.primary_artifact_storage_key,
          'primary_artifact_display_name', decision_rows.primary_artifact_display_name,
          'primary_artifact_preview_mode', decision_rows.primary_artifact_preview_mode
        ),
        'action_flags',
        JSONB_BUILD_OBJECT(
          'can_save', decision_rows.can_save_calc,
          'can_process', decision_rows.can_process_calc,
          'can_unprocess', decision_rows.can_unprocess_calc,
          'can_bulk_authorise', decision_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', decision_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', decision_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', decision_rows.can_manage_evidence_calc,
          'can_add_additional_manual', decision_rows.can_add_additional_manual_calc,
          'review_only', decision_rows.review_only_calc
        ),
        'row_patch',
        JSONB_BUILD_OBJECT(),
        'cache_invalidation_hints',
        JSONB_BUILD_OBJECT(),
        'count_deltas',
        JSONB_BUILD_OBJECT(),
        'header_loaded',
        TRUE,
        'header_only',
        TRUE,
        'editor_loaded',
        FALSE,
        'evidence_loaded',
        FALSE,
        'compare_loaded',
        FALSE,
        'full_loaded',
        FALSE,
        'schedule_pending',
        TRUE,
        'schedule_authoritative',
        FALSE,
        'loaded_layers',
        JSONB_BUILD_ARRAY('dataset_row'),
        'dataset_source',
        'timesheet_summary_lightweight_rows_v1'
      )) AS row_json
    FROM decision_rows
    LEFT JOIN LATERAL (
      SELECT NULLIF(BTRIM(COALESCE(
        lifecycle_signature_source.signature_json->>'backend_row_signature',
        lifecycle_signature_source.signature_json->>'row_signature',
        lifecycle_signature_source.signature_json->>'signature',
        ''
      )), '') AS signature_text
      FROM (
        SELECT public.timesheet_lifecycle_signature_v1(decision_rows.timesheet_id, decision_rows.contract_week_id, false) AS signature_json
      ) AS lifecycle_signature_source
    ) AS lifecycle_signature ON TRUE
  ),
  canonical_authorise_signature_rows AS MATERIALIZED (
    SELECT canonical_patch.row_json
    FROM public.bulk_timesheet_row_patch_v1(
      JSONB_BUILD_OBJECT(
        'dataset_mode', 'authorise',
        'projection', 'dataset_row',
        'row_keys', COALESCE((
          SELECT JSONB_AGG(payload_rows.row_json->>'row_key' ORDER BY payload_rows.row_json->>'row_key')
          FROM payload_rows
          WHERE NULLIF(BTRIM(COALESCE(payload_rows.row_json->>'row_key', '')), '') IS NOT NULL
        ), '[]'::jsonb)
      )
    ) AS canonical_patch(row_json)
  ),
  canonical_payload_rows AS MATERIALIZED (
    SELECT
      (
        payload_rows.row_json
        || jsonb_strip_nulls(jsonb_build_object(
          'row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'backend_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'backend_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'mutation_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'mutation_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
          'summary_stage', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'summary_stage', '')), ''),
          'tools_stage', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', '')), ''),
          'processing_status', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'processing_status', '')), '')
        ))
        || jsonb_build_object(
          'has_retained_financial_history', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'can_unprocess', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'unprocess_action_visible', CASE
            WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
            ELSE FALSE
          END,
          'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
          'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), ''),
          'is_archived', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
          'read_only', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
          'can_archive', FALSE,
          'can_unarchive', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED'
        )
        || jsonb_build_object(
          'action_flags',
            COALESCE(payload_rows.row_json->'action_flags', '{}'::jsonb)
            || COALESCE(canonical_authorise_signature_rows.row_json->'action_flags', '{}'::jsonb)
            || jsonb_build_object(
              'has_retained_financial_history', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'can_unprocess', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_action_visible', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
              'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), ''),
              'is_archived', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
              'read_only', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED',
              'can_archive', FALSE,
              'can_unarchive', UPPER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'tools_stage', ''))) = 'ARCHIVED'
            ),
          'row_patch',
            COALESCE(payload_rows.row_json->'row_patch', '{}'::jsonb)
            || COALESCE(canonical_authorise_signature_rows.row_json->'row_patch', '{}'::jsonb)
            || jsonb_strip_nulls(jsonb_build_object(
              'row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
              'backend_row_signature', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'backend_row_signature', canonical_authorise_signature_rows.row_json->>'row_signature', '')), ''),
              'has_retained_financial_history', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'can_unprocess', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'can_unprocess', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_action_visible', CASE
                WHEN LOWER(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_action_visible', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_action_visible}', payload_rows.row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN TRUE
                ELSE FALSE
              END,
              'unprocess_block_reason', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_reason', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_reason}', '')), ''),
              'unprocess_block_message', NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'unprocess_block_message', canonical_authorise_signature_rows.row_json#>>'{action_flags,unprocess_block_message}', '')), '')
            ))
        )
      ) AS row_json
    FROM payload_rows
    LEFT JOIN canonical_authorise_signature_rows
      ON canonical_authorise_signature_rows.row_json->>'row_key' = payload_rows.row_json->>'row_key'
  ),
  eligible_rows_before_classification AS MATERIALIZED (
    SELECT canonical_payload_rows.row_json
    FROM canonical_payload_rows
    WHERE NULLIF(BTRIM(COALESCE(canonical_payload_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND UPPER(COALESCE(canonical_payload_rows.row_json->>'bulk_process_bucket', '')) <> 'UNPROCESSED'
      AND NULLIF(BTRIM(COALESCE(canonical_payload_rows.row_json->>'bulk_authorise_section', '')), '') IS NOT NULL
  ),
  classification_filtered_rows AS MATERIALIZED (
    SELECT eligible_rows_before_classification.row_json
    FROM eligible_rows_before_classification
    WHERE v_classification IS NULL
       OR UPPER(COALESCE(eligible_rows_before_classification.row_json->>'bulk_authorise_classification', '')) = v_classification
  ),
  visible_rows AS MATERIALIZED (
    SELECT classification_filtered_rows.row_json
    FROM classification_filtered_rows
    WHERE (
        UPPER(COALESCE(classification_filtered_rows.row_json->>'bulk_authorise_classification', '')) <> 'TIMESHEETS'
        OR (
          CASE
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'period_type', classification_filtered_rows.row_json->>'sheet_scope', '')) = 'DAILY' THEN v_show_daily
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'period_type', classification_filtered_rows.row_json->>'sheet_scope', '')) = 'WEEKLY' THEN v_show_weekly
            ELSE TRUE
          END
          AND CASE
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'MANUAL_NON_QR' THEN v_show_manual
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'QR' THEN v_show_qr
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'ELECTRONIC' THEN v_show_electronic
            ELSE v_show_manual AND v_show_qr AND v_show_electronic
          END
          AND CASE
            WHEN COALESCE((classification_filtered_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE THEN v_validation_awaiting
            ELSE v_validation_already
          END
        )
      )
  ),
  paged_visible_rows AS MATERIALIZED (
    SELECT visible_rows.row_json
    FROM visible_rows
    ORDER BY visible_rows.row_json->>'week_ending_date', visible_rows.row_json->>'client_name', visible_rows.row_json->>'candidate_name', visible_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  visible_counts AS (
    SELECT
      COUNT(*)::integer AS total_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_section' = 'processed_eligible')::integer AS processed_eligible_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_section' = 'authorised_eligible')::integer AS authorised_eligible_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'MANUAL_NON_QR')::integer AS manual_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'QR')::integer AS qr_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'ELECTRONIC')::integer AS electronic_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = FALSE)::integer AS already_validated_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE)::integer AS awaiting_validation_count
    FROM visible_rows
  ),
  eligible_counts AS (
    SELECT
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'TIMESHEETS')::integer AS timesheets_count,
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'NHSP')::integer AS nhsp_count,
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'HR')::integer AS hr_count
    FROM eligible_rows_before_classification
  ),
  rows_payload AS (
    SELECT COALESCE(
      JSONB_AGG(
        paged_visible_rows.row_json
        ORDER BY paged_visible_rows.row_json->>'week_ending_date', paged_visible_rows.row_json->>'client_name', paged_visible_rows.row_json->>'candidate_name', paged_visible_rows.row_json->>'row_key'
      ),
      '[]'::jsonb
    ) AS rows_json
    FROM paged_visible_rows
  )
  SELECT JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'classification', v_classification,
      'show_daily', v_show_daily,
      'show_weekly', v_show_weekly,
      'show_manual', v_show_manual,
      'show_qr', v_show_qr,
      'show_electronic', v_show_electronic,
      'validation_already', v_validation_already,
      'validation_awaiting', v_validation_awaiting,
      'show_authorised_invoiced_unissued', v_show_authorised_invoiced_unissued,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', COALESCE(visible_counts.total_count, 0),
      'processed_eligible', COALESCE(visible_counts.processed_eligible_count, 0),
      'authorised_eligible', COALESCE(visible_counts.authorised_eligible_count, 0),
      'by_classification', JSONB_BUILD_OBJECT(
        'TIMESHEETS', COALESCE(eligible_counts.timesheets_count, 0),
        'NHSP', COALESCE(eligible_counts.nhsp_count, 0),
        'HR', COALESCE(eligible_counts.hr_count, 0)
      ),
      'timesheets_by_type', JSONB_BUILD_OBJECT(
        'manual', COALESCE(visible_counts.manual_count, 0),
        'qr', COALESCE(visible_counts.qr_count, 0),
        'electronic', COALESCE(visible_counts.electronic_count, 0)
      ),
      'validation', JSONB_BUILD_OBJECT(
        'already_validated', COALESCE(visible_counts.already_validated_count, 0),
        'awaiting_validation', COALESCE(visible_counts.awaiting_validation_count, 0),
        'scope', 'visible_rows_after_classification_and_toggle_filters'
      ),
      'scope', JSONB_BUILD_OBJECT(
        'total', 'visible_rows_after_classification_and_toggle_filters',
        'by_classification', 'eligible_rows_before_classification_filter',
        'timesheets_by_type', 'visible_rows_after_classification_and_toggle_filters',
        'validation', 'visible_rows_after_classification_and_toggle_filters'
      )
    ),
    'rows', COALESCE(rows_payload.rows_json, '[]'::jsonb)
  )
  INTO v_out
  FROM visible_counts
  CROSS JOIN eligible_counts
  CROSS JOIN rows_payload;

  RETURN private._candidate_dataset_overlay_v1(COALESCE(v_out, JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'classification', v_classification,
      'show_daily', v_show_daily,
      'show_weekly', v_show_weekly,
      'show_manual', v_show_manual,
      'show_qr', v_show_qr,
      'show_electronic', v_show_electronic,
      'validation_already', v_validation_already,
      'validation_awaiting', v_validation_awaiting,
      'show_authorised_invoiced_unissued', v_show_authorised_invoiced_unissued,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', 0,
      'processed_eligible', 0,
      'authorised_eligible', 0,
      'by_classification', JSONB_BUILD_OBJECT('TIMESHEETS', 0, 'NHSP', 0, 'HR', 0),
      'timesheets_by_type', JSONB_BUILD_OBJECT('manual', 0, 'qr', 0, 'electronic', 0),
      'validation', JSONB_BUILD_OBJECT('already_validated', 0, 'awaiting_validation', 0)
    ),
    'rows', '[]'::jsonb
  )));
END;
$function$;

CREATE OR REPLACE FUNCTION public.bulk_timesheet_row_patch_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(row_json jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := COALESCE(p_filters, '{}'::jsonb);

  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

  v_dataset_mode text := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'dataset_mode', v_filters->>'datasetMode', '')), ''));
  v_projection text := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'projection', v_filters->>'profile', 'status_patch')), ''));

  v_row_keys text[] := NULL;
  v_previous_row_key text := NULL;
  v_actor_user_id uuid := NULL;

  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_has_timesheet_filter boolean := FALSE;
  v_has_contract_week_filter boolean := FALSE;
  v_has_row_key_filter boolean := FALSE;

  v_changed_domains text[] := ARRAY[]::text[];
  v_status_only_hint boolean := FALSE;
  v_manual_changed_hint boolean := FALSE;
  v_evidence_changed_hint boolean := FALSE;
  v_storage_changed_hint boolean := FALSE;
  v_identity_changed_hint boolean := FALSE;
BEGIN
  IF v_dataset_mode NOT IN ('process', 'authorise') THEN
    v_dataset_mode := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'mode', '')), ''));
  END IF;

  IF v_dataset_mode NOT IN ('process', 'authorise') THEN
    v_dataset_mode := NULL;
  END IF;

  IF v_projection NOT IN ('status_patch', 'dataset_row', 'summary_row', 'active_row_header') THEN
    v_projection := 'status_patch';
  END IF;

  v_previous_row_key := NULLIF(BTRIM(COALESCE(v_filters->>'previous_row_key', v_filters->>'previousRowKey', '')), '');

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId', '')), '') IS NOT NULL
       AND COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId') ~* v_uuid_re THEN
      v_actor_user_id := COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId')::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_actor_user_id := NULL;
  END;

  v_has_row_key_filter :=
    (v_filters ? 'row_key')
    OR (v_filters ? 'rowKey')
    OR (v_filters ? 'row_keys')
    OR (v_filters ? 'rowKeys');

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value ORDER BY row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value ORDER BY row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'rowKeys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey')), '')];
  END IF;

  IF v_has_row_key_filter AND COALESCE(ARRAY_LENGTH(v_row_keys, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_row_keys, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('row_keys', to_jsonb(v_row_keys));
  END IF;

  v_has_timesheet_filter :=
    (v_filters ? 'timesheet_id')
    OR (v_filters ? 'timesheetId')
    OR (v_filters ? 'timesheet_ids')
    OR (v_filters ? 'timesheetIds');

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheetIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', '')), '') IS NOT NULL
        AND COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId')::uuid];
  END IF;

  IF v_has_timesheet_filter AND COALESCE(ARRAY_LENGTH(v_timesheet_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_timesheet_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('timesheet_ids', to_jsonb(v_timesheet_ids));
  END IF;

  v_has_contract_week_filter :=
    (v_filters ? 'contract_week_id')
    OR (v_filters ? 'contractWeekId')
    OR (v_filters ? 'contract_week_ids')
    OR (v_filters ? 'contractWeekIds');

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contractWeekIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', '')), '') IS NOT NULL
        AND COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId')::uuid];
  END IF;

  IF v_has_contract_week_filter AND COALESCE(ARRAY_LENGTH(v_contract_week_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_contract_week_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('contract_week_ids', to_jsonb(v_contract_week_ids));
  END IF;

  IF v_filters ? 'changed_domains' AND jsonb_typeof(v_filters->'changed_domains') = 'array' THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(input_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM jsonb_array_elements_text(v_filters->'changed_domains') AS input_values(value)
    WHERE NULLIF(BTRIM(input_values.value), '') IS NOT NULL;
  ELSIF v_filters ? 'changedDomains' AND jsonb_typeof(v_filters->'changedDomains') = 'array' THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(input_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM jsonb_array_elements_text(v_filters->'changedDomains') AS input_values(value)
    WHERE NULLIF(BTRIM(input_values.value), '') IS NOT NULL;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'changed_domains', v_filters->>'changedDomains', '')), '') IS NOT NULL THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(split_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM unnest(regexp_split_to_array(COALESCE(v_filters->>'changed_domains', v_filters->>'changedDomains'), '\s*,\s*')) AS split_values(value)
    WHERE NULLIF(BTRIM(split_values.value), '') IS NOT NULL;
  END IF;

  v_identity_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['identity','adoption','row_identity','contract_week_to_timesheet','contract-week-to-timesheet','created_timesheet']::text[], FALSE);

  v_evidence_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['evidence','attached_evidence','queue_evidence','document','documents']::text[], FALSE);

  v_storage_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['storage','storage_key','storage-key','preview','artifact','primary_artifact']::text[], FALSE);

  v_manual_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['manual','schedule','editor','save','process','unprocess','financials','tsfin','additional_units','expenses']::text[], FALSE);

  -- Process-mode mutation callers may request a lightweight status_patch without
  -- explicit changed_domains. Treat those as manual/editor-affecting so the
  -- frontend fetches one fresh active_row_visible context instead of preserving
  -- stale schedule/expense/editor state as if this were a pure status patch.
  IF NOT v_manual_changed_hint
     AND COALESCE(ARRAY_LENGTH(v_changed_domains, 1), 0) = 0
     AND v_dataset_mode = 'process'
     AND v_projection = 'status_patch' THEN
    v_manual_changed_hint := TRUE;
  END IF;

  v_status_only_hint :=
    (
      COALESCE(v_changed_domains && ARRAY['status','authorise','authorize','unauthorise','unauthorize','processing_status','authorisation','authorization']::text[], FALSE)
      OR (
        COALESCE(ARRAY_LENGTH(v_changed_domains, 1), 0) = 0
        AND v_dataset_mode = 'authorise'
        AND v_projection = 'status_patch'
      )
    )
    AND NOT v_identity_changed_hint
    AND NOT v_evidence_changed_hint
    AND NOT v_storage_changed_hint
    AND NOT v_manual_changed_hint;

  IF EXISTS(
    SELECT 1
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) source_row
    JOIN public.timesheets t ON t.timesheet_id=source_row.timesheet_id
    LEFT JOIN public.timesheets_financials tf ON tf.timesheet_id=t.timesheet_id AND tf.is_current
    WHERE t.is_current AND t.archived_at_utc IS NULL AND coalesce(t.is_adjustment,false)
      AND t.parent_timesheet_id IS NOT NULL AND t.correction_id IS NOT NULL
      AND upper(coalesce(t.adjustment_origin,''))='IMPORT_CORRECTION'
      AND t.correction_kind IN ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      AND ((upper(coalesce(source_row.route_type,'')) like '%NHSP%' or upper(coalesce(tf.basis::text,'')) like 'NHSP%'
          or upper(coalesce(t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='NHSP')
       = (upper(coalesce(source_row.route_type,'')) like '%HEALTHROSTER%' or upper(coalesce(tf.basis::text,'')) like 'HEALTHROSTER%'
          or upper(coalesce(t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER'))
  ) THEN
    RAISE EXCEPTION 'BULK_AUTHORISE_CORRECTION_SOURCE_CONFLICT' USING ERRCODE='22023';
  END IF;

  RETURN QUERY
  WITH source_rows AS MATERIALIZED (
    SELECT
      source_row.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) AS source_row
  ),
  retention_unit_members AS MATERIALIZED (
    SELECT
      source_rows.timesheet_id AS row_timesheet_id,
      source_rows.timesheet_id AS member_timesheet_id
    FROM source_rows
    WHERE source_rows.timesheet_id IS NOT NULL

    UNION

    SELECT
      source_rows.timesheet_id AS row_timesheet_id,
      unit_timesheet.timesheet_id AS member_timesheet_id
    FROM source_rows
    JOIN public.timesheets AS anchor_timesheet
      ON anchor_timesheet.timesheet_id = source_rows.timesheet_id
     AND anchor_timesheet.is_current = TRUE
     AND anchor_timesheet.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
    JOIN public.timesheets AS unit_timesheet
      ON unit_timesheet.booking_id = anchor_timesheet.booking_id
  ),
  retention_by_row AS MATERIALIZED (
    SELECT
      retention_unit_members.row_timesheet_id,
      COALESCE(BOOL_OR(marker.timesheet_id IS NOT NULL), FALSE) AS has_retained_financial_history
    FROM retention_unit_members
    LEFT JOIN public.timesheet_financial_retention AS marker
      ON marker.timesheet_id = retention_unit_members.member_timesheet_id
    GROUP BY retention_unit_members.row_timesheet_id
  ),
  enriched_rows AS MATERIALIZED (
    SELECT
      source_rows.*,
      timesheet_row.version AS timesheet_version,
      timesheet_row.updated_at AS timesheet_updated_at,
      timesheet_row.is_current AS timesheet_is_current,
      timesheet_row.archived_at_utc AS timesheet_archived_at_utc,
      timesheet_row.manual_pdf_r2_key,
      timesheet_row.qr_r2_key,
      timesheet_row.generated_pdf_at_utc,
      timesheet_row.manual_pdf_rotation_degrees,
      timesheet_row.qr_token AS timesheet_qr_token,
      timesheet_row.qr_generated_at AS timesheet_qr_generated_at,
      timesheet_row.qr_scanned_at AS timesheet_qr_scanned_at,
      COALESCE(retention_unit.has_retained_financial_history, FALSE) AS has_retained_financial_history,
      contract_week_row.updated_at AS contract_week_updated_at,
      contract_week_row.uploaded_pdf_r2_key,
      contract_week_row.planned_schedule_json AS contract_week_planned_schedule_json,
      contract_week_row.totals_json AS contract_week_totals_json,
      contract_week_row.is_adjustment AS contract_week_is_adjustment,
      contract_week_row.additional_seq AS contract_week_additional_seq,
      timesheet_row.actual_schedule_json AS timesheet_actual_schedule_json,
      timesheet_row.is_adjustment AS timesheet_is_adjustment,
      timesheet_row.parent_timesheet_id AS timesheet_parent_timesheet_id,
      timesheet_row.correction_id AS timesheet_correction_id,
      timesheet_row.correction_kind AS timesheet_correction_kind,
      timesheet_row.adjustment_origin AS timesheet_adjustment_origin,
      timesheet_row.candidate_hint_text AS timesheet_candidate_hint_text,
      financial_row.basis::text AS tsfin_basis_text,
      financial_row.updated_at AS tsfin_updated_at,
      financial_row.actual_schedule_json AS tsfin_actual_schedule_json,
      financial_row.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
      financial_row.total_hours AS tsfin_total_hours,
      financial_row.expenses_pay_ex_vat AS tsfin_expenses_pay_ex_vat,
      financial_row.expenses_charge_ex_vat AS tsfin_expenses_charge_ex_vat,
      financial_row.mileage_units AS tsfin_mileage_units,
      financial_row.mileage_pay_ex_vat AS tsfin_mileage_pay_ex_vat,
      financial_row.mileage_charge_ex_vat AS tsfin_mileage_charge_ex_vat,
      financial_row.travel_pay_ex_vat AS tsfin_travel_pay_ex_vat,
      financial_row.travel_charge_ex_vat AS tsfin_travel_charge_ex_vat,
      financial_row.accommodation_pay_ex_vat AS tsfin_accommodation_pay_ex_vat,
      financial_row.accommodation_charge_ex_vat AS tsfin_accommodation_charge_ex_vat,
      financial_row.other_pay_ex_vat AS tsfin_other_pay_ex_vat,
      financial_row.other_charge_ex_vat AS tsfin_other_charge_ex_vat,
      financial_row.authorised_at_utc AS tsfin_authorised_at_utc,
      financial_row.processed_at_utc AS tsfin_processed_at_utc,
      COALESCE(evidence_summary.attached_evidence_count, 0)::integer AS evidence_attached_count,
      evidence_summary.primary_storage_key AS evidence_primary_storage_key,
      evidence_summary.primary_display_name AS evidence_primary_display_name,
      CASE
        WHEN evidence_summary.evidence_updated_at IS NOT NULL
         AND staged_evidence_summary.staged_evidence_updated_at IS NOT NULL
          THEN GREATEST(evidence_summary.evidence_updated_at, staged_evidence_summary.staged_evidence_updated_at)
        ELSE COALESCE(evidence_summary.evidence_updated_at, staged_evidence_summary.staged_evidence_updated_at)
      END AS evidence_updated_at,
      COALESCE(evidence_summary.has_attached_timesheet_evidence, FALSE) AS evidence_has_attached_timesheet,
      COALESCE(evidence_summary.has_attached_mileage_evidence, FALSE) AS evidence_has_attached_mileage,
      COALESCE(evidence_summary.has_attached_travel_evidence, FALSE) AS evidence_has_attached_travel,
      COALESCE(evidence_summary.has_attached_accommodation_evidence, FALSE) AS evidence_has_attached_accommodation,
      COALESCE(evidence_summary.has_attached_other_evidence, FALSE) AS evidence_has_attached_other,
      COALESCE(staged_evidence_summary.staged_evidence_count, 0)::integer AS evidence_staged_count,
      staged_evidence_summary.primary_staged_storage_key AS evidence_primary_staged_storage_key,
      staged_evidence_summary.primary_staged_display_name AS evidence_primary_staged_display_name,
      staged_evidence_summary.primary_staged_kind AS evidence_primary_staged_kind,
      COALESCE(staged_evidence_summary.has_staged_timesheet_evidence, FALSE) AS evidence_has_staged_timesheet,
      COALESCE(staged_evidence_summary.has_staged_mileage_evidence, FALSE) AS evidence_has_staged_mileage,
      COALESCE(staged_evidence_summary.has_staged_travel_evidence, FALSE) AS evidence_has_staged_travel,
      COALESCE(staged_evidence_summary.has_staged_accommodation_evidence, FALSE) AS evidence_has_staged_accommodation,
      COALESCE(staged_evidence_summary.has_staged_other_evidence, FALSE) AS evidence_has_staged_other,
      UPPER(COALESCE(source_rows.route_type, '')) AS route_type_upper,
      UPPER(COALESCE(source_rows.submission_mode::text, '')) AS submission_mode_upper,
      CASE
        WHEN UPPER(COALESCE(source_rows.sheet_scope::text, '')) IN ('DAILY', 'WEEKLY') THEN UPPER(COALESCE(source_rows.sheet_scope::text, ''))
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE 'DAILY\_%' ESCAPE '\' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type,
      (
        COALESCE(source_rows.is_qr, FALSE)
        OR source_rows.qr_status IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(timesheet_row.qr_token, '')), '') IS NOT NULL
        OR timesheet_row.qr_generated_at IS NOT NULL
      ) AS is_qr_route,
      (
        UPPER(COALESCE(source_rows.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'MANUAL'
        AND (
          COALESCE(timesheet_row.is_adjustment, FALSE) = TRUE
          OR COALESCE(contract_week_row.is_adjustment, FALSE) = TRUE
          OR COALESCE(contract_week_row.additional_seq, source_rows.additional_seq, 0) > 0
          OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
          OR timesheet_row.parent_timesheet_id IS NOT NULL
        )
        AND NOT (
          UPPER(COALESCE(timesheet_row.adjustment_origin, '')) IN ('IMPORT_CORRECTION', 'IMPORT_CANCELLATION')
          OR NULLIF(BTRIM(COALESCE(timesheet_row.correction_kind, '')), '') IS NOT NULL
          OR timesheet_row.correction_id IS NOT NULL
        )
      ) AS is_manual_additional_adjustment_calc
    FROM source_rows
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = source_rows.timesheet_id
     AND timesheet_row.is_current = TRUE
    LEFT JOIN retention_by_row AS retention_unit
      ON retention_unit.row_timesheet_id = source_rows.timesheet_id
    LEFT JOIN public.contract_weeks AS contract_week_row
      ON contract_week_row.id = source_rows.contract_week_id
    LEFT JOIN public.timesheets_financials AS financial_row
      ON financial_row.timesheet_id = source_rows.timesheet_id
     AND financial_row.is_current = TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(timesheet_evidence_row.id)::integer AS attached_evidence_count,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET'), FALSE) AS has_attached_timesheet_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'MILEAGE'), FALSE) AS has_attached_mileage_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TRAVEL'), FALSE) AS has_attached_travel_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'ACCOMMODATION'), FALSE) AS has_attached_accommodation_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'OTHER'), FALSE) AS has_attached_other_evidence,
        (ARRAY_AGG(
          timesheet_evidence_row.storage_key
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_storage_key,
        (ARRAY_AGG(
          COALESCE(NULLIF(timesheet_evidence_row.display_name, ''), timesheet_evidence_row.kind, 'Evidence')
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_display_name,
        MAX(timesheet_evidence_row.created_at) AS evidence_updated_at
      FROM public.timesheet_evidence AS timesheet_evidence_row
      WHERE timesheet_evidence_row.timesheet_id = source_rows.timesheet_id
    ) AS evidence_summary ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(staged_queue_row.id)::integer AS staged_evidence_count,
        MAX(staged_queue_row.uploaded_at_utc) AS staged_evidence_updated_at,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'TIMESHEET'), FALSE) AS has_staged_timesheet_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'MILEAGE'), FALSE) AS has_staged_mileage_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'TRAVEL'), FALSE) AS has_staged_travel_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'ACCOMMODATION'), FALSE) AS has_staged_accommodation_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'OTHER'), FALSE) AS has_staged_other_evidence,
        (ARRAY_AGG(
          staged_queue_row.r2_key
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_storage_key,
        (ARRAY_AGG(
          staged_queue_row.display_name
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_display_name,
        (ARRAY_AGG(
          staged_queue_row.staged_kind_upper
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_kind
      FROM (
        SELECT
          manual_queue_row.id,
          manual_queue_row.r2_key,
          manual_queue_row.uploaded_at_utc,
          COALESCE(
            NULLIF(BTRIM(COALESCE(manual_queue_row.original_filename, '')), ''),
            NULLIF(BTRIM(COALESCE(manual_queue_row.meta_json->>'display_name', manual_queue_row.meta_json->>'displayName', manual_queue_row.meta_json->>'filename', manual_queue_row.meta_json->>'original_filename', manual_queue_row.meta_json->>'originalFilename', '')), '')
          ) AS display_name,
          CASE
            WHEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(
              manual_queue_row.meta_json->>'staged_kind',
              manual_queue_row.meta_json->>'stagedKind',
              manual_queue_row.meta_json->>'evidence_kind',
              manual_queue_row.meta_json->>'evidenceKind',
              manual_queue_row.meta_json->>'kind',
              manual_queue_row.meta_json->>'type',
              manual_queue_row.meta_json->>'evidence_type',
              manual_queue_row.meta_json->>'evidenceType',
              ''
            )), ''), 'TIMESHEET')) IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER')
              THEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(
                manual_queue_row.meta_json->>'staged_kind',
                manual_queue_row.meta_json->>'stagedKind',
                manual_queue_row.meta_json->>'evidence_kind',
                manual_queue_row.meta_json->>'evidenceKind',
                manual_queue_row.meta_json->>'kind',
                manual_queue_row.meta_json->>'type',
                manual_queue_row.meta_json->>'evidence_type',
                manual_queue_row.meta_json->>'evidenceType',
                ''
              )), ''), 'TIMESHEET'))
            ELSE 'OTHER'
          END AS staged_kind_upper
        FROM public.manual_timesheet_queue AS manual_queue_row
        WHERE source_rows.timesheet_id IS NULL
          AND source_rows.contract_week_id IS NOT NULL
          AND UPPER(COALESCE(manual_queue_row.status, '')) = 'STAGED'
          AND NULLIF(BTRIM(COALESCE(manual_queue_row.meta_json->>'contract_week_id', '')), '') = source_rows.contract_week_id::text
      ) AS staged_queue_row
    ) AS staged_evidence_summary ON TRUE
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      enriched_rows.*,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
            AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE
          )
        ) THEN 'IMPORT_AUTHORITATIVE'
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
        ) THEN 'NHSP'
        WHEN enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
         AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
            AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE
          )
        ) THEN NULL::text
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      (
        enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
        AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) <> TRUE
      ) AS compare_block_required_calc,
      (
        enriched_rows.timesheet_id IS NULL
        AND enriched_rows.contract_week_id IS NOT NULL
        AND (
          UPPER(COALESCE(enriched_rows.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.contract_week_status::text, '')) IN ('OPEN', 'PLANNED')
        )
      ) AS is_planned_week_unprocessed_calc,
      (
        enriched_rows.timesheet_id IS NOT NULL
        AND UPPER(COALESCE(enriched_rows.tools_stage, '')) <> 'ARCHIVED'
        AND (
          UPPER(COALESCE(enriched_rows.processing_status::text, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(enriched_rows.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.tools_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.processing_status_display, '')) = 'UNPROCESSED'
        )
      ) AS is_real_row_unprocessed_calc,
      (
        UPPER(COALESCE(enriched_rows.tools_stage, '')) = 'ARCHIVED'
        OR COALESCE(enriched_rows.locked_by_invoice_id, NULL) IS NOT NULL
        OR COALESCE(enriched_rows.invoice_segments_locked, 0) > 0
        OR COALESCE(enriched_rows.invoice_is_paid, FALSE) = TRUE
      ) AS locked_calc,
      (
        enriched_rows.authorised_at_server IS NOT NULL
        OR enriched_rows.tsfin_authorised_at_utc IS NOT NULL
      ) AS is_authorised_calc,
      (
        UPPER(COALESCE(enriched_rows.processing_status::text, '')) = 'PENDING_AUTH'
        OR (
          COALESCE(enriched_rows.client_requires_hr, FALSE) = TRUE
          AND COALESCE(enriched_rows.client_autoprocess_hr, FALSE) = FALSE
          AND UPPER(COALESCE(enriched_rows.processing_status::text, '')) = 'READY_FOR_HR'
        )
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(enriched_rows.qr_status::text, '')) = 'PENDING'
        AND (
          NULLIF(BTRIM(COALESCE(enriched_rows.timesheet_qr_token, '')), '') IS NOT NULL
          OR enriched_rows.timesheet_qr_generated_at IS NOT NULL
        )
        AND enriched_rows.timesheet_qr_scanned_at IS NULL
      ) AS qr_pending_awaiting_signature_calc,
      (
        UPPER(COALESCE(enriched_rows.qr_status::text, '')) = 'USED'
        AND enriched_rows.timesheet_qr_scanned_at IS NOT NULL
      ) AS qr_signed_returned_calc,
      COALESCE(
        enriched_rows.evidence_primary_storage_key,
        enriched_rows.evidence_primary_staged_storage_key,
        NULLIF(enriched_rows.manual_pdf_r2_key, ''),
        NULLIF(enriched_rows.qr_r2_key, ''),
        NULLIF(enriched_rows.uploaded_pdf_r2_key, '')
      ) AS primary_artifact_storage_key_calc,
      COALESCE(
        enriched_rows.evidence_primary_display_name,
        enriched_rows.evidence_primary_staged_display_name,
        CASE WHEN NULLIF(enriched_rows.manual_pdf_r2_key, '') IS NOT NULL THEN 'Manual timesheet PDF' END,
        CASE WHEN NULLIF(enriched_rows.qr_r2_key, '') IS NOT NULL THEN 'QR timesheet' END,
        CASE WHEN NULLIF(enriched_rows.uploaded_pdf_r2_key, '') IS NOT NULL THEN 'Uploaded weekly PDF' END
      ) AS primary_artifact_display_name_calc
    FROM enriched_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      CASE
        WHEN classified_rows.timesheet_is_current=true
         AND classified_rows.timesheet_archived_at_utc is null
         AND coalesce(classified_rows.timesheet_is_adjustment,false)
         AND classified_rows.timesheet_parent_timesheet_id is not null
         AND classified_rows.timesheet_correction_id is not null
         AND upper(coalesce(classified_rows.timesheet_adjustment_origin,''))='IMPORT_CORRECTION'
         AND classified_rows.timesheet_correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
         THEN case
          when (classified_rows.route_type_upper like '%HEALTHROSTER%'
              or upper(coalesce(classified_rows.tsfin_basis_text,'')) like 'HEALTHROSTER%'
              or upper(coalesce(classified_rows.timesheet_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER')
            and not (classified_rows.route_type_upper like '%NHSP%'
              or upper(coalesce(classified_rows.tsfin_basis_text,'')) like 'NHSP%'
              or upper(coalesce(classified_rows.timesheet_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='NHSP') then 'HR'
          when (classified_rows.route_type_upper like '%NHSP%'
              or upper(coalesce(classified_rows.tsfin_basis_text,'')) like 'NHSP%'
              or upper(coalesce(classified_rows.timesheet_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='NHSP')
            and not (classified_rows.route_type_upper like '%HEALTHROSTER%'
              or upper(coalesce(classified_rows.tsfin_basis_text,'')) like 'HEALTHROSTER%'
              or upper(coalesce(classified_rows.timesheet_candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_system}',''))='HEALTHROSTER') then 'NHSP'
          else 'TIMESHEETS' end
        WHEN classified_rows.route_family_calc = 'IMPORT_AUTHORITATIVE'
         AND classified_rows.route_subfamily_calc = 'HEALTHROSTER_NO_TIMESHEET' THEN 'HR'
        WHEN classified_rows.route_family_calc = 'IMPORT_AUTHORITATIVE' THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      (classified_rows.is_planned_week_unprocessed_calc OR classified_rows.is_real_row_unprocessed_calc) AS is_unprocessed_calc,
      CASE
        WHEN (classified_rows.is_planned_week_unprocessed_calc OR classified_rows.is_real_row_unprocessed_calc) THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(classified_rows.client_hr_validation_required, FALSE) = TRUE
        AND UPPER(COALESCE(classified_rows.validation_status::text, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN')
      ) AS hr_validation_awaiting_calc,
      (
        classified_rows.qr_pending_awaiting_signature_calc = TRUE
        OR UPPER(COALESCE(classified_rows.processing_status::text, '')) = 'AWAITING_MANUAL_SIGNATURE'
        OR 'Awaiting signed QR timesheet' = ANY(COALESCE(classified_rows.issue_codes, ARRAY[]::text[]))
      ) AS qr_unsigned_blocked_calc,
      CASE
        WHEN classified_rows.primary_artifact_storage_key_calc IS NOT NULL THEN 'document'
        ELSE NULL::text
      END AS primary_artifact_preview_mode_calc
    FROM classified_rows
  ),
  final_rows AS MATERIALIZED (
    SELECT
      decision_rows.*,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || decision_rows.timesheet_id::text
        WHEN decision_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || decision_rows.contract_week_id::text
        ELSE ''::text
      END AS row_key_calc,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL THEN decision_rows.timesheet_id::text
        WHEN decision_rows.contract_week_id IS NOT NULL THEN decision_rows.contract_week_id::text
        ELSE NULL::text
      END AS stable_row_id_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.requires_authorisation_calc = TRUE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.qr_unsigned_blocked_calc = FALSE
        AND (decision_rows.route_family_calc <> 'QR' OR decision_rows.qr_signed_returned_calc = TRUE)
      ) AS can_bulk_authorise_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = TRUE
        AND (decision_rows.route_family_calc <> 'QR' OR decision_rows.qr_signed_returned_calc = TRUE)
      ) AS can_bulk_unauthorise_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_save_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = FALSE
      ) AS unprocess_action_visible_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = FALSE
        AND COALESCE(decision_rows.has_retained_financial_history, FALSE) = FALSE
      ) AS can_unprocess_calc,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL
         AND decision_rows.locked_calc = FALSE
         AND decision_rows.is_authorised_calc = FALSE
         AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
         AND decision_rows.is_unprocessed_calc = FALSE
         AND COALESCE(decision_rows.has_retained_financial_history, FALSE) = TRUE
        THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'::text
        ELSE NULL::text
      END AS unprocess_block_reason,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = TRUE
      ) AS can_process_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR (decision_rows.contract_week_id IS NOT NULL AND decision_rows.route_family_calc = 'MANUAL_NON_QR'))
        AND UPPER(COALESCE(decision_rows.tools_stage, '')) <> 'ARCHIVED'
        AND (decision_rows.locked_by_invoice_id IS NOT NULL OR COALESCE(decision_rows.invoice_segments_locked, 0) > 0) = FALSE
        AND decision_rows.route_family_calc <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence_calc,
      (
        decision_rows.locked_calc = TRUE
        OR decision_rows.is_authorised_calc = TRUE
        OR decision_rows.route_family_calc <> 'MANUAL_NON_QR'
      ) AS review_only_calc,
      (
        decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND COALESCE(decision_rows.is_adjusted, FALSE) = FALSE
        AND (
          (decision_rows.period_type = 'WEEKLY' AND decision_rows.contract_week_id IS NOT NULL)
          OR (decision_rows.period_type = 'DAILY' AND decision_rows.timesheet_id IS NOT NULL)
        )
        AND (
          decision_rows.route_family_calc = 'IMPORT_AUTHORITATIVE'
          OR (decision_rows.route_family_calc = 'MANUAL_NON_QR' AND decision_rows.submission_mode_upper = 'MANUAL' AND decision_rows.is_qr_route = FALSE)
        )
      ) AS can_add_additional_manual_calc
    FROM decision_rows
  ),
  signed_rows_base AS MATERIALIZED (
    SELECT
      final_rows.*,
      md5(concat_ws('|',
        COALESCE(final_rows.timesheet_id::text, ''),
        COALESCE(final_rows.contract_week_id::text, ''),
        COALESCE(final_rows.timesheet_version::text, final_rows.timesheet_version::text, ''),
        COALESCE(final_rows.timesheet_updated_at::text, ''),
        COALESCE(final_rows.contract_week_updated_at::text, ''),
        COALESCE(final_rows.tsfin_updated_at::text, ''),
        COALESCE(final_rows.evidence_updated_at::text, ''),
        COALESCE(final_rows.evidence_staged_count::text, ''),
        COALESCE(final_rows.evidence_has_staged_timesheet::text, ''),
        COALESCE(final_rows.evidence_has_staged_mileage::text, ''),
        COALESCE(final_rows.evidence_has_staged_travel::text, ''),
        COALESCE(final_rows.evidence_has_staged_accommodation::text, ''),
        COALESCE(final_rows.evidence_has_staged_other::text, ''),
        COALESCE(final_rows.processing_status::text, ''),
        COALESCE(final_rows.summary_stage, ''),
        COALESCE(final_rows.tools_stage, ''),
        COALESCE(final_rows.authorised_at_server::text, ''),
        COALESCE(final_rows.tsfin_authorised_at_utc::text, ''),
        COALESCE(final_rows.is_authorised_calc::text, ''),
        COALESCE(final_rows.locked_calc::text, ''),
        COALESCE(final_rows.has_retained_financial_history::text, ''),
        COALESCE(final_rows.unprocess_action_visible_calc::text, ''),
        COALESCE(final_rows.can_unprocess_calc::text, ''),
        COALESCE(final_rows.unprocess_block_reason, ''),
        COALESCE(final_rows.route_family_calc, ''),
        COALESCE(final_rows.route_subfamily_calc, ''),
        COALESCE(final_rows.bulk_process_bucket_calc, ''),
        COALESCE(final_rows.bulk_authorise_classification_calc, ''),
        COALESCE(final_rows.primary_artifact_storage_key_calc, ''),
        COALESCE(final_rows.total_hours::text, ''),
        COALESCE(final_rows.total_pay_ex_vat::text, ''),
        COALESCE(final_rows.total_charge_ex_vat::text, ''),
        COALESCE(final_rows.margin_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_expenses_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_expenses_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_mileage_units::text, ''),
        COALESCE(final_rows.tsfin_mileage_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_mileage_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_travel_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_travel_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_accommodation_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_accommodation_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_other_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_other_charge_ex_vat::text, ''),
        COALESCE(final_rows.issue_codes::text, '')
      )) AS row_signature_calc
    FROM final_rows
  ),
  signed_rows AS MATERIALIZED (
    SELECT
      signed_rows_base.*,
      COALESCE(lifecycle_signature.signature_text, signed_rows_base.row_signature_calc) AS backend_row_signature_calc,
      COALESCE(lifecycle_signature.signature_text, signed_rows_base.row_signature_calc) AS mutation_row_signature_calc
    FROM signed_rows_base
    LEFT JOIN LATERAL (
      SELECT NULLIF(BTRIM(COALESCE(
        lifecycle_signature_source.signature_json->>'backend_row_signature',
        lifecycle_signature_source.signature_json->>'row_signature',
        lifecycle_signature_source.signature_json->>'signature',
        ''
      )), '') AS signature_text
      FROM (
        SELECT public.timesheet_lifecycle_signature_v1(signed_rows_base.timesheet_id, signed_rows_base.contract_week_id, false) AS signature_json
      ) AS lifecycle_signature_source
    ) AS lifecycle_signature ON TRUE
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      signed_rows.*,
      CASE
        WHEN signed_rows.can_bulk_authorise_calc THEN 'processed_eligible'
        WHEN signed_rows.can_bulk_unauthorise_calc THEN 'authorised_eligible'
        ELSE NULL::text
      END AS bulk_authorise_section_calc,
      (
        v_identity_changed_hint
        OR (
          v_previous_row_key IS NOT NULL
          AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
        )
      ) AS identity_changed_calc,
      (
        v_status_only_hint
        AND NOT (
          v_identity_changed_hint
          OR (
            v_previous_row_key IS NOT NULL
            AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
          )
        )
      ) AS status_only_calc,
      jsonb_build_object(
        'status_only', (
          v_status_only_hint
          AND NOT (
            v_identity_changed_hint
            OR (
              v_previous_row_key IS NOT NULL
              AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
            )
          )
        ),
        'manual_changed', v_manual_changed_hint,
        'evidence_changed', v_evidence_changed_hint,
        'storage_changed', v_storage_changed_hint,
        'identity_changed', (
          v_identity_changed_hint
          OR (
            v_previous_row_key IS NOT NULL
            AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
          )
        ),
        'invalidate_context', false,
        'invalidate_row_context', false,
        'invalidate_preview', CASE WHEN v_storage_changed_hint OR v_evidence_changed_hint THEN true ELSE false END,
        'invalidate_evidence', CASE WHEN v_evidence_changed_hint OR v_storage_changed_hint THEN true ELSE false END,
        'invalidate_editor_context', CASE WHEN v_manual_changed_hint THEN true ELSE false END,
        'row_keys', CASE
          WHEN v_previous_row_key IS NOT NULL
           AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
            THEN jsonb_build_array(v_previous_row_key, signed_rows.row_key_calc)
          ELSE jsonb_build_array(signed_rows.row_key_calc)
        END,
        'timesheet_ids', CASE WHEN signed_rows.timesheet_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.timesheet_id) END,
        'contract_week_ids', CASE WHEN signed_rows.contract_week_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.contract_week_id) END,
        'storage_keys', CASE WHEN signed_rows.primary_artifact_storage_key_calc IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.primary_artifact_storage_key_calc) END,
        'row_signature', signed_rows.row_signature_calc,
        'backend_row_signature', signed_rows.backend_row_signature_calc,
        'mutation_row_signature', signed_rows.mutation_row_signature_calc,
        'datasets', CASE
          WHEN v_dataset_mode = 'authorise' THEN jsonb_build_array('bulk_authorise')
          WHEN v_dataset_mode = 'process' THEN jsonb_build_array('bulk_process')
          ELSE jsonb_build_array('bulk_process', 'bulk_authorise')
        END
      ) AS cache_hints_json,
      jsonb_build_object(
        'processed_eligible', 0,
        'authorised_eligible', 0,
        'unprocessed', 0,
        'processed', 0,
        'total', 0
      ) AS count_deltas_json,
      (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) AS keep_additional_manual_adjustment_schedule_empty_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN '[]'::jsonb
        ELSE COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json, '[]'::jsonb)
      END AS actual_schedule_json_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN '[]'::jsonb
        ELSE COALESCE(signed_rows.contract_week_planned_schedule_json, '[]'::jsonb)
      END AS planned_schedule_json_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN 0::numeric
        ELSE COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric)
      END AS total_hours_calc,
      CASE
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
         AND (
           UPPER(COALESCE(signed_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
           OR UPPER(COALESCE(signed_rows.basis::text, '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
           OR COALESCE(signed_rows.client_is_nhsp, FALSE) = TRUE
         ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
         AND (
           UPPER(COALESCE(signed_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
           OR UPPER(COALESCE(signed_rows.basis::text, '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
           OR COALESCE(signed_rows.client_autoprocess_hr, FALSE) = TRUE
         ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'WEEKLY_MANUAL_ADJUSTMENT'
        ELSE signed_rows.route_type
      END AS effective_route_type_calc,
      (
        COALESCE(signed_rows.timesheet_is_adjustment, FALSE) = TRUE
        OR COALESCE(signed_rows.contract_week_is_adjustment, FALSE) = TRUE
        OR COALESCE(signed_rows.contract_week_additional_seq, signed_rows.additional_seq, 0) > 0
        OR COALESCE(signed_rows.is_adjustment, FALSE) = TRUE
        OR signed_rows.timesheet_parent_timesheet_id IS NOT NULL
        OR signed_rows.timesheet_correction_id IS NOT NULL
        OR signed_rows.timesheet_correction_kind IS NOT NULL
      ) AS effective_is_adjustment_calc
    FROM signed_rows
  )
  SELECT
    private._candidate_office_context_overlay_v1((
      jsonb_build_object(
        'id', COALESCE(payload_rows.timesheet_id::text, payload_rows.contract_week_id::text),
        'row_key', payload_rows.row_key_calc,
        'previous_row_key', v_previous_row_key,
        'new_row_key', payload_rows.row_key_calc,
        'stable_row_id', payload_rows.stable_row_id_calc,
        'timesheet_id', payload_rows.timesheet_id,
        'current_timesheet_id', payload_rows.timesheet_id,
        'requested_timesheet_id', payload_rows.timesheet_id,
        'expected_timesheet_id', payload_rows.timesheet_id,
        'contract_week_id', payload_rows.contract_week_id,
        'contract_id', COALESCE((SELECT contract_lookup.contract_id FROM public.timesheets AS contract_lookup WHERE contract_lookup.timesheet_id = payload_rows.timesheet_id AND contract_lookup.is_current = TRUE LIMIT 1), (SELECT contract_week_lookup.contract_id FROM public.contract_weeks AS contract_week_lookup WHERE contract_week_lookup.id = payload_rows.contract_week_id LIMIT 1)),
        'row_signature', payload_rows.row_signature_calc,
        'backend_row_signature', payload_rows.backend_row_signature_calc,
        'mutation_row_signature', payload_rows.mutation_row_signature_calc,
        'render_signature', payload_rows.row_signature_calc,
        'previous_row_signature', NULL::text,
        'timesheet_version', payload_rows.timesheet_version,
        'current_version', payload_rows.timesheet_version,
        'updated_at', COALESCE(payload_rows.timesheet_updated_at, payload_rows.contract_week_updated_at, payload_rows.tsfin_updated_at),
        'is_current', COALESCE(payload_rows.timesheet_is_current, TRUE),
        'was_stale', FALSE,
        'actor_user_id', v_actor_user_id,
        'projection', v_projection,
        'dataset_mode', v_dataset_mode
      )
      || jsonb_build_object(
        'candidate_id', payload_rows.candidate_id,
        'candidate_name', payload_rows.candidate_name,
        'candidate_display_name', payload_rows.candidate_name,
        'client_id', payload_rows.client_id,
        'client_name', payload_rows.client_name,
        'client_display_name', payload_rows.client_name,
        'booking_id', payload_rows.booking_id,
        'booking_ref', payload_rows.booking_id,
        'occupant_key_norm', payload_rows.occupant_key_norm,
        'hospital_name', payload_rows.hospital_norm,
        'hospital_norm', payload_rows.hospital_norm,
        'week_ending_date', COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date),
        'work_date', CASE WHEN payload_rows.period_type = 'DAILY' THEN payload_rows.week_ending_date ELSE NULL::date END,
        'period_type', payload_rows.period_type,
        'sheet_scope', payload_rows.sheet_scope,
        'timesheet_scope', payload_rows.sheet_scope,
        'submission_mode', payload_rows.submission_mode,
        'submission_mode_snapshot', payload_rows.submission_mode,
        'basis', payload_rows.basis,
        'route_type', payload_rows.effective_route_type_calc,
        'route_display', CASE
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('NHSP', 'WEEKLY_NHSP') THEN 'NHSP'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'NHSP Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('HEALTHROSTER', 'WEEKLY_HEALTHROSTER') THEN 'HealthRoster'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'HealthRoster Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'QR' THEN 'QR'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
          WHEN COALESCE(payload_rows.effective_route_type_calc, '') <> '' THEN initcap(replace(payload_rows.effective_route_type_calc, '_', ' '))
          ELSE 'Manual'
        END,
        'route_family', payload_rows.route_family_calc,
        'route_subfamily', payload_rows.route_subfamily_calc,
        'underlying_channel_family', payload_rows.underlying_channel_family_calc,
        'is_import_authoritative', payload_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
        'compare_block_required', payload_rows.compare_block_required_calc,
        'is_adjustment', payload_rows.effective_is_adjustment_calc,
        'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
        'actual_schedule_json', payload_rows.actual_schedule_json_calc,
        'planned_schedule_json', payload_rows.planned_schedule_json_calc,
        'contract_week_totals_json', COALESCE(payload_rows.contract_week_totals_json, '{}'::jsonb),
        'suppress_standard_schedule_fallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        'keep_additional_manual_adjustment_schedule_empty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        '__suppressStandardScheduleFallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        '__keepAdditionalManualAdjustmentScheduleEmpty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc
      )
      || jsonb_build_object(
        'processing_status', payload_rows.processing_status::text,
        'processing_status_display', payload_rows.processing_status_display,
        'summary_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.summary_stage
        END,
        'tools_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.tools_stage
        END,
        'bulk_process_bucket', payload_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification', payload_rows.bulk_authorise_classification_calc,
        'bulk_authorise_section', payload_rows.bulk_authorise_section_calc,
        'correction_id',payload_rows.timesheet_correction_id,
        'correction_kind',payload_rows.timesheet_correction_kind,
        'adjustment_origin',payload_rows.timesheet_adjustment_origin,
        'correction_source_system',case when payload_rows.timesheet_is_current=true and payload_rows.timesheet_archived_at_utc is null
          and coalesce(payload_rows.timesheet_is_adjustment,false) and payload_rows.timesheet_parent_timesheet_id is not null
          and payload_rows.timesheet_correction_id is not null and upper(coalesce(payload_rows.timesheet_adjustment_origin,''))='IMPORT_CORRECTION'
          and payload_rows.timesheet_correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          then case when payload_rows.bulk_authorise_classification_calc='HR' then 'HEALTHROSTER' else 'NHSP' end end,
        'correction_display_label',case when payload_rows.timesheet_is_current=true and payload_rows.timesheet_archived_at_utc is null
          and coalesce(payload_rows.timesheet_is_adjustment,false) and payload_rows.timesheet_parent_timesheet_id is not null
          and payload_rows.timesheet_correction_id is not null and upper(coalesce(payload_rows.timesheet_adjustment_origin,''))='IMPORT_CORRECTION'
          then case
            when payload_rows.bulk_authorise_classification_calc='HR' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REVERSAL' then 'HealthRoster Reversal'
            when payload_rows.bulk_authorise_classification_calc='HR' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REPLACEMENT' then 'HealthRoster Corrected Hours'
            when payload_rows.bulk_authorise_classification_calc='NHSP' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REVERSAL' then 'NHSP Reversal'
            when payload_rows.bulk_authorise_classification_calc='NHSP' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REPLACEMENT' then 'NHSP Corrected Hours' end end,
        'is_authorised', payload_rows.is_authorised_calc,
        'authorised_at_utc', COALESCE(payload_rows.tsfin_authorised_at_utc, payload_rows.authorised_at_server),
        'authorised_at_server', payload_rows.authorised_at_server,
        'processed_at_utc', COALESCE(payload_rows.tsfin_processed_at_utc, NULL::timestamp with time zone),
        'requires_authorisation', payload_rows.requires_authorisation_calc,
        'locked', payload_rows.locked_calc,
        'locked_by_invoice_id', payload_rows.locked_by_invoice_id,
        'paid_at_utc', payload_rows.paid_at_utc,
        'review_only', payload_rows.review_only_calc,
        'can_save', payload_rows.can_save_calc,
        'can_process', payload_rows.can_process_calc,
        'has_retained_financial_history', COALESCE(payload_rows.has_retained_financial_history, FALSE),
        'can_unprocess', payload_rows.can_unprocess_calc,
        'unprocess_block_reason', payload_rows.unprocess_block_reason,
        'unprocess_action_visible', payload_rows.unprocess_action_visible_calc,
        'unprocess_block_message', CASE WHEN payload_rows.unprocess_block_reason = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS' THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.' ELSE NULL::text END,
        'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence', payload_rows.can_manage_evidence_calc,
        'can_add_additional_manual', payload_rows.can_add_additional_manual_calc
      )
      || jsonb_build_object(
        'total_hours', COALESCE(payload_rows.total_hours_calc, 0::numeric),
        'total_pay_ex_vat', COALESCE(payload_rows.total_pay_ex_vat, 0::numeric),
        'total_charge_ex_vat', COALESCE(payload_rows.total_charge_ex_vat, 0::numeric),
        'margin_ex_vat', COALESCE(payload_rows.margin_ex_vat, 0::numeric),
        'expenses_pay_ex_vat', payload_rows.tsfin_expenses_pay_ex_vat,
        'expenses_charge_ex_vat', payload_rows.tsfin_expenses_charge_ex_vat,
        'mileage_units', payload_rows.tsfin_mileage_units,
        'mileage_pay_ex_vat', payload_rows.tsfin_mileage_pay_ex_vat,
        'mileage_charge_ex_vat', payload_rows.tsfin_mileage_charge_ex_vat,
        'travel_pay_ex_vat', payload_rows.tsfin_travel_pay_ex_vat,
        'travel_charge_ex_vat', payload_rows.tsfin_travel_charge_ex_vat,
        'accommodation_pay_ex_vat', payload_rows.tsfin_accommodation_pay_ex_vat,
        'accommodation_charge_ex_vat', payload_rows.tsfin_accommodation_charge_ex_vat,
        'other_pay_ex_vat', payload_rows.tsfin_other_pay_ex_vat,
        'other_charge_ex_vat', payload_rows.tsfin_other_charge_ex_vat,
        'net_delta_ex_vat', COALESCE(payload_rows.net_delta_ex_vat, COALESCE(payload_rows.total_charge_ex_vat, 0::numeric) - COALESCE(payload_rows.total_pay_ex_vat, 0::numeric)),
        'invoice_is_paid', COALESCE(payload_rows.invoice_is_paid, FALSE),
        'invoice_segments_total', COALESCE(payload_rows.invoice_segments_total, 0),
        'invoice_segments_locked', COALESCE(payload_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked', COALESCE(payload_rows.invoice_segments_unlocked, 0),
        'invoice_segment_stage', payload_rows.invoice_segment_stage,
        'pay_icon_code', payload_rows.pay_icon_code,
        'pay_status_code', payload_rows.pay_status_code,
        'pay_paid_at_utc', payload_rows.pay_paid_at_utc,
        'issue_codes', COALESCE(to_jsonb(payload_rows.issue_codes), '[]'::jsonb),
        'validation_status', payload_rows.validation_status::text,
        'hr_crosscheck_status', payload_rows.hr_crosscheck_status,
        'hr_crosscheck_issues', COALESCE(to_jsonb(payload_rows.hr_crosscheck_issues), '[]'::jsonb),
        'hr_validation_awaiting', payload_rows.hr_validation_awaiting_calc,
        'qr_unsigned_blocked', payload_rows.qr_unsigned_blocked_calc,
        'qr_signed_returned', payload_rows.qr_signed_returned_calc
      )
      || jsonb_build_object(
        'is_qr', payload_rows.is_qr_route,
        'qr_status', payload_rows.qr_status::text,
        'qr_generated_at', payload_rows.timesheet_qr_generated_at,
        'qr_scanned_at', payload_rows.timesheet_qr_scanned_at,
        'is_adjusted', COALESCE(payload_rows.is_adjusted, FALSE),
        'needs_attention', COALESCE(payload_rows.needs_attention, FALSE),
        'has_rate_issue', COALESCE(payload_rows.has_rate_issue, FALSE),
        'has_pay_channel_issue', COALESCE(payload_rows.has_pay_channel_issue, FALSE),
        'client_requires_hr', COALESCE(payload_rows.client_requires_hr, FALSE),
        'client_no_timesheet_required', COALESCE(payload_rows.client_no_timesheet_required, FALSE),
        'client_autoprocess_hr', COALESCE(payload_rows.client_autoprocess_hr, FALSE),
        'client_is_nhsp', COALESCE(payload_rows.client_is_nhsp, FALSE),
        'has_any_evidence', (
          COALESCE(payload_rows.evidence_attached_count, 0) > 0
          OR COALESCE(payload_rows.evidence_staged_count, 0) > 0
          OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL
        ),
        'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
        'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0),
        'evidence_count', COALESCE(payload_rows.evidence_attached_count, 0) + COALESCE(payload_rows.evidence_staged_count, 0),
        'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
        'primary_artifact_display_name', payload_rows.primary_artifact_display_name_calc,
        'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
        'evidence_badges', jsonb_build_array(
          jsonb_build_object('kind', 'TIMESHEET', 'present', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
          jsonb_build_object('kind', 'MILEAGE', 'present', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE))),
          jsonb_build_object('kind', 'TRAVEL', 'present', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE))),
          jsonb_build_object('kind', 'ACCOMMODATION', 'present', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE))),
          jsonb_build_object('kind', 'OTHER', 'present', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)))
        )
      )
      || jsonb_build_object(
        'count_deltas', payload_rows.count_deltas_json,
        'cache_invalidation_hints', payload_rows.cache_hints_json,
        'action_flags', jsonb_build_object(
          'can_save', payload_rows.can_save_calc,
          'can_process', payload_rows.can_process_calc,
          'has_retained_financial_history', COALESCE(payload_rows.has_retained_financial_history, FALSE),
          'can_unprocess', payload_rows.can_unprocess_calc,
          'unprocess_block_reason', payload_rows.unprocess_block_reason,
          'unprocess_action_visible', payload_rows.unprocess_action_visible_calc,
          'unprocess_block_message', CASE WHEN payload_rows.unprocess_block_reason = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS' THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.' ELSE NULL::text END,
          'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', payload_rows.can_manage_evidence_calc,
          'can_add_additional_manual', payload_rows.can_add_additional_manual_calc,
          'review_only', payload_rows.review_only_calc,
          'is_adjustment', payload_rows.effective_is_adjustment_calc,
          'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
          'supportsUnprocessedExpenseDraft', (
            payload_rows.is_manual_additional_adjustment_calc = TRUE
            AND payload_rows.contract_week_id IS NOT NULL
            AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
            AND payload_rows.locked_calc = FALSE
            AND payload_rows.is_authorised_calc = FALSE
            AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE)
          ),
          'supports_unprocessed_expense_draft', (
            payload_rows.is_manual_additional_adjustment_calc = TRUE
            AND payload_rows.contract_week_id IS NOT NULL
            AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
            AND payload_rows.locked_calc = FALSE
            AND payload_rows.is_authorised_calc = FALSE
            AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE)
          ),
          'expense_storage_target', CASE
            WHEN payload_rows.is_manual_additional_adjustment_calc = TRUE
             AND payload_rows.contract_week_id IS NOT NULL
             AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
             AND payload_rows.locked_calc = FALSE
             AND payload_rows.is_authorised_calc = FALSE
             AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE) THEN 'CONTRACT_WEEK_DRAFT'
            WHEN payload_rows.timesheet_id IS NOT NULL AND payload_rows.route_family_calc = 'MANUAL_NON_QR' THEN 'TSFIN'
            ELSE NULL::text
          END,
          'expense_evidence_storage_target', CASE
            WHEN payload_rows.is_manual_additional_adjustment_calc = TRUE
             AND payload_rows.contract_week_id IS NOT NULL
             AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
             AND payload_rows.locked_calc = FALSE
             AND payload_rows.is_authorised_calc = FALSE
             AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE) THEN 'CONTRACT_WEEK_STAGED_EVIDENCE'
            WHEN payload_rows.timesheet_id IS NOT NULL AND payload_rows.route_family_calc = 'MANUAL_NON_QR' THEN 'TIMESHEET_EVIDENCE'
            ELSE NULL::text
          END
        ),
        'artifact_hints', jsonb_build_object(
          'route_family', payload_rows.route_family_calc,
          'route_subfamily', payload_rows.route_subfamily_calc,
          'underlying_channel_family', payload_rows.underlying_channel_family_calc,
          'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
          'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
          'has_any_evidence', (COALESCE(payload_rows.evidence_attached_count, 0) > 0 OR COALESCE(payload_rows.evidence_staged_count, 0) > 0 OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL),
          'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
          'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0)
        ),
        'row_patch', (
          jsonb_build_object(
            'previous_row_key', v_previous_row_key,
            'row_key', payload_rows.row_key_calc,
            'new_row_key', payload_rows.row_key_calc,
            'stable_row_id', payload_rows.stable_row_id_calc,
            'timesheet_id', payload_rows.timesheet_id,
            'current_timesheet_id', payload_rows.timesheet_id,
            'requested_timesheet_id', payload_rows.timesheet_id,
            'expected_timesheet_id', payload_rows.timesheet_id,
            'contract_week_id', payload_rows.contract_week_id,
            'row_signature', payload_rows.row_signature_calc,
            'backend_row_signature', payload_rows.backend_row_signature_calc,
            'mutation_row_signature', payload_rows.mutation_row_signature_calc,
            'render_signature', payload_rows.row_signature_calc,
            'previous_row_signature', NULL::text,
            'timesheet_version', payload_rows.timesheet_version,
            'current_version', payload_rows.timesheet_version,
            'updated_at', COALESCE(payload_rows.timesheet_updated_at, payload_rows.contract_week_updated_at, payload_rows.tsfin_updated_at),
            'is_current', COALESCE(payload_rows.timesheet_is_current, TRUE),
            'was_stale', FALSE,
            'candidate_id', payload_rows.candidate_id,
            'candidate_name', payload_rows.candidate_name,
            'candidate_display_name', payload_rows.candidate_name,
            'client_id', payload_rows.client_id,
            'client_name', payload_rows.client_name,
            'client_display_name', payload_rows.client_name,
            'booking_id', payload_rows.booking_id,
            'booking_ref', payload_rows.booking_id,
            'occupant_key_norm', payload_rows.occupant_key_norm,
            'hospital_name', payload_rows.hospital_norm,
            'hospital_norm', payload_rows.hospital_norm,
            'week_ending_date', COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date),
            'work_date', CASE WHEN payload_rows.period_type = 'DAILY' THEN payload_rows.week_ending_date ELSE NULL::date END,
            'period_type', payload_rows.period_type,
            'sheet_scope', payload_rows.sheet_scope,
            'timesheet_scope', payload_rows.sheet_scope,
            'submission_mode', payload_rows.submission_mode,
            'submission_mode_snapshot', payload_rows.submission_mode,
            'basis', payload_rows.basis,
            'route_type', payload_rows.effective_route_type_calc,
            'route_display', CASE
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('NHSP', 'WEEKLY_NHSP') THEN 'NHSP'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'NHSP Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('HEALTHROSTER', 'WEEKLY_HEALTHROSTER') THEN 'HealthRoster'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'HealthRoster Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'QR' THEN 'QR'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
          WHEN COALESCE(payload_rows.effective_route_type_calc, '') <> '' THEN initcap(replace(payload_rows.effective_route_type_calc, '_', ' '))
          ELSE 'Manual'
        END,
            'route_family', payload_rows.route_family_calc,
            'route_subfamily', payload_rows.route_subfamily_calc
          )
          || jsonb_build_object(
            'underlying_channel_family', payload_rows.underlying_channel_family_calc,
            'is_import_authoritative', payload_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
            'compare_block_required', payload_rows.compare_block_required_calc,
            'is_adjustment', payload_rows.effective_is_adjustment_calc,
            'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
            'actual_schedule_json', payload_rows.actual_schedule_json_calc,
            'planned_schedule_json', payload_rows.planned_schedule_json_calc,
            'contract_week_totals_json', COALESCE(payload_rows.contract_week_totals_json, '{}'::jsonb),
            'suppress_standard_schedule_fallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            'keep_additional_manual_adjustment_schedule_empty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            '__suppressStandardScheduleFallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            '__keepAdditionalManualAdjustmentScheduleEmpty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            'processing_status', payload_rows.processing_status::text,
            'processing_status_display', payload_rows.processing_status_display,
            'summary_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.summary_stage
        END,
            'tools_stage', CASE
          WHEN UPPER(COALESCE(payload_rows.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED'
          ELSE payload_rows.tools_stage
        END,
            'bulk_process_bucket', payload_rows.bulk_process_bucket_calc,
            'previous_bulk_process_bucket', NULL::text,
            'bulk_authorise_classification', payload_rows.bulk_authorise_classification_calc,
            'bulk_authorise_section', payload_rows.bulk_authorise_section_calc,
            'correction_id',payload_rows.timesheet_correction_id,
            'correction_kind',payload_rows.timesheet_correction_kind,
            'adjustment_origin',payload_rows.timesheet_adjustment_origin,
            'correction_source_system',case when payload_rows.timesheet_is_current=true and payload_rows.timesheet_archived_at_utc is null
              and coalesce(payload_rows.timesheet_is_adjustment,false) and payload_rows.timesheet_parent_timesheet_id is not null
              and payload_rows.timesheet_correction_id is not null and upper(coalesce(payload_rows.timesheet_adjustment_origin,''))='IMPORT_CORRECTION'
              and payload_rows.timesheet_correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
              then case when payload_rows.bulk_authorise_classification_calc='HR' then 'HEALTHROSTER' else 'NHSP' end end,
            'correction_display_label',case when payload_rows.timesheet_is_current=true and payload_rows.timesheet_archived_at_utc is null
              and coalesce(payload_rows.timesheet_is_adjustment,false) and payload_rows.timesheet_parent_timesheet_id is not null
              and payload_rows.timesheet_correction_id is not null and upper(coalesce(payload_rows.timesheet_adjustment_origin,''))='IMPORT_CORRECTION'
              then case
                when payload_rows.bulk_authorise_classification_calc='HR' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REVERSAL' then 'HealthRoster Reversal'
                when payload_rows.bulk_authorise_classification_calc='HR' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REPLACEMENT' then 'HealthRoster Corrected Hours'
                when payload_rows.bulk_authorise_classification_calc='NHSP' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REVERSAL' then 'NHSP Reversal'
                when payload_rows.bulk_authorise_classification_calc='NHSP' and payload_rows.timesheet_correction_kind='CHANGED_HOURS_REPLACEMENT' then 'NHSP Corrected Hours' end end,
            'previous_bulk_authorise_section', NULL::text,
            'is_authorised', payload_rows.is_authorised_calc,
            'authorised_at_utc', COALESCE(payload_rows.tsfin_authorised_at_utc, payload_rows.authorised_at_server),
            'authorised_at_server', payload_rows.authorised_at_server,
            'processed_at_utc', COALESCE(payload_rows.tsfin_processed_at_utc, NULL::timestamp with time zone),
            'requires_authorisation', payload_rows.requires_authorisation_calc,
            'locked', payload_rows.locked_calc,
            'locked_by_invoice_id', payload_rows.locked_by_invoice_id,
            'paid_at_utc', payload_rows.paid_at_utc,
            'review_only', payload_rows.review_only_calc,
            'can_save', payload_rows.can_save_calc,
            'can_process', payload_rows.can_process_calc,
            'has_retained_financial_history', COALESCE(payload_rows.has_retained_financial_history, FALSE),
            'can_unprocess', payload_rows.can_unprocess_calc,
            'unprocess_block_reason', payload_rows.unprocess_block_reason,
            'unprocess_action_visible', payload_rows.unprocess_action_visible_calc,
            'unprocess_block_message', CASE WHEN payload_rows.unprocess_block_reason = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS' THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.' ELSE NULL::text END,
            'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
            'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
            'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
            'can_manage_evidence', payload_rows.can_manage_evidence_calc,
            'can_add_additional_manual', payload_rows.can_add_additional_manual_calc
          )
          || jsonb_build_object(
            'total_hours', COALESCE(payload_rows.total_hours_calc, 0::numeric),
            'total_pay_ex_vat', COALESCE(payload_rows.total_pay_ex_vat, 0::numeric),
            'total_charge_ex_vat', COALESCE(payload_rows.total_charge_ex_vat, 0::numeric),
            'margin_ex_vat', COALESCE(payload_rows.margin_ex_vat, 0::numeric),
            'net_delta_ex_vat', COALESCE(payload_rows.net_delta_ex_vat, COALESCE(payload_rows.total_charge_ex_vat, 0::numeric) - COALESCE(payload_rows.total_pay_ex_vat, 0::numeric)),
            'invoice_is_paid', COALESCE(payload_rows.invoice_is_paid, FALSE),
            'invoice_segments_total', COALESCE(payload_rows.invoice_segments_total, 0),
            'invoice_segments_locked', COALESCE(payload_rows.invoice_segments_locked, 0),
            'invoice_segments_unlocked', COALESCE(payload_rows.invoice_segments_unlocked, 0),
            'invoice_segment_stage', payload_rows.invoice_segment_stage,
            'pay_icon_code', payload_rows.pay_icon_code,
            'pay_status_code', payload_rows.pay_status_code,
            'pay_paid_at_utc', payload_rows.pay_paid_at_utc,
            'issue_codes', COALESCE(to_jsonb(payload_rows.issue_codes), '[]'::jsonb),
            'validation_status', payload_rows.validation_status::text,
            'hr_crosscheck_status', payload_rows.hr_crosscheck_status,
            'hr_crosscheck_issues', COALESCE(to_jsonb(payload_rows.hr_crosscheck_issues), '[]'::jsonb),
            'hr_validation_awaiting', payload_rows.hr_validation_awaiting_calc,
            'qr_unsigned_blocked', payload_rows.qr_unsigned_blocked_calc,
            'qr_signed_returned', payload_rows.qr_signed_returned_calc,
            'is_qr', payload_rows.is_qr_route,
            'qr_status', payload_rows.qr_status::text,
            'qr_generated_at', payload_rows.timesheet_qr_generated_at,
            'qr_scanned_at', payload_rows.timesheet_qr_scanned_at,
            'is_adjusted', COALESCE(payload_rows.is_adjusted, FALSE),
            'needs_attention', COALESCE(payload_rows.needs_attention, FALSE),
            'has_rate_issue', COALESCE(payload_rows.has_rate_issue, FALSE),
            'has_pay_channel_issue', COALESCE(payload_rows.has_pay_channel_issue, FALSE)
          )
          || jsonb_build_object(
            'client_requires_hr', COALESCE(payload_rows.client_requires_hr, FALSE),
            'client_no_timesheet_required', COALESCE(payload_rows.client_no_timesheet_required, FALSE),
            'client_autoprocess_hr', COALESCE(payload_rows.client_autoprocess_hr, FALSE),
            'client_is_nhsp', COALESCE(payload_rows.client_is_nhsp, FALSE),
            'has_any_evidence', (
              COALESCE(payload_rows.evidence_attached_count, 0) > 0
              OR COALESCE(payload_rows.evidence_staged_count, 0) > 0
              OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL
            ),
            'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
            'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0),
            'evidence_count', COALESCE(payload_rows.evidence_attached_count, 0) + COALESCE(payload_rows.evidence_staged_count, 0),
            'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
            'previous_primary_artifact_storage_key', NULL::text,
            'primary_artifact_display_name', payload_rows.primary_artifact_display_name_calc,
            'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
            'evidence_badges', jsonb_build_array(
              jsonb_build_object('kind', 'TIMESHEET', 'present', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
              jsonb_build_object('kind', 'MILEAGE', 'present', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE))),
              jsonb_build_object('kind', 'TRAVEL', 'present', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE))),
              jsonb_build_object('kind', 'ACCOMMODATION', 'present', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE))),
              jsonb_build_object('kind', 'OTHER', 'present', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)))
            ),
            'count_deltas', payload_rows.count_deltas_json,
            'cache_invalidation_hints', payload_rows.cache_hints_json
          )
        )
      )
    )) AS row_json
  FROM payload_rows
  ORDER BY
    COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date) ASC NULLS LAST,
    payload_rows.client_name ASC NULLS LAST,
    payload_rows.candidate_name ASC NULLS LAST,
    payload_rows.row_key_calc ASC;
END;
$function$;

CREATE OR REPLACE FUNCTION public.bulk_process_row_context_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_decision_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_has_identity boolean := FALSE;
  v_row_key text := NULL;
  v_id_text text := NULL;
  v_timesheet_id_text text := NULL;
  v_contract_week_id_text text := NULL;
  v_include_evidence boolean := FALSE;
  v_include_compare boolean := FALSE;
  v_include_import_source_rows boolean := FALSE;
  v_profile text := NULL;
  v_base_only boolean := FALSE;
  v_header_row_json jsonb := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_out jsonb;
  v_editor_layer jsonb := NULL;
  v_evidence_layer jsonb := NULL;
  v_compare_layer jsonb := NULL;
  v_layer_errors jsonb := '[]'::jsonb;
  v_layer_names jsonb := '[]'::jsonb;
  v_effective_has_any_evidence boolean := FALSE;
  v_effective_badge_timesheet boolean := FALSE;
  v_effective_badge_mileage boolean := FALSE;
  v_effective_badge_travel boolean := FALSE;
  v_effective_badge_accommodation boolean := FALSE;
  v_effective_badge_other boolean := FALSE;
  v_effective_evidence_badges jsonb := '[]'::jsonb;
  v_effective_evidence_meta jsonb := '{}'::jsonb;
  v_effective_row_patch jsonb := '{}'::jsonb;
  v_effective_row jsonb := '{}'::jsonb;
  v_effective_data_row jsonb := '{}'::jsonb;
  v_effective_artifact_hints jsonb := '{}'::jsonb;
  v_effective_primary_artifact jsonb := NULL;
  v_effective_primary_artifact_storage_key text := NULL;
  v_effective_primary_artifact_preview_mode text := NULL;
  v_effective_primary_artifact_id text := NULL;
  v_effective_primary_artifact_kind text := NULL;
  v_effective_primary_artifact_display_name text := NULL;
  v_effective_uploaded_pdf_r2_key text := NULL;
  v_effective_action_flags jsonb := '{}'::jsonb;
  v_effective_details jsonb := '{}'::jsonb;
  v_effective_left_pane jsonb := '{}'::jsonb;
  v_effective_attached_evidence_count integer := 0;
  v_canonical_row_json jsonb := NULL;
  v_canonical_row_signature text := NULL;
  v_canonical_row_key text := NULL;
  v_canonical_timesheet_id text := NULL;
  v_canonical_contract_week_id text := NULL;
BEGIN
  v_has_identity := (
    NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'expected_timesheet_id', v_filters->>'expectedTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'week_id', v_filters->>'weekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '') IS NOT NULL
    OR (v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' AND jsonb_array_length(v_filters->'row_keys') > 0)
    OR (v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' AND jsonb_array_length(v_filters->'timesheet_ids') > 0)
    OR (v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' AND jsonb_array_length(v_filters->'contract_week_ids') > 0)
    OR NULLIF(BTRIM(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds', '')), '') IS NOT NULL
    OR (v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' AND jsonb_array_length(v_filters->'rowKeys') > 0)
    OR (v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' AND jsonb_array_length(v_filters->'timesheetIds') > 0)
    OR (v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' AND jsonb_array_length(v_filters->'contractWeekIds') > 0)
    OR (v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' AND jsonb_array_length(v_filters->'ids') > 0)
    OR NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL
  );

  IF v_has_identity = FALSE THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'context_kind', 'bulk_process_row_context',
      'context_profile', 'status_header',
      'profile', 'status_header',
      'soft_failure', TRUE,
      'context_degraded', TRUE,
      'degraded_reason', 'ROW_CONTEXT_IDENTITY_REQUIRED',
      'header_loaded', FALSE,
      'header_only', FALSE,
      'editor_loaded', FALSE,
      'evidence_loaded', FALSE,
      'compare_loaded', FALSE,
      'full_loaded', FALSE,
      'schedule_pending', TRUE,
      'schedule_authoritative', FALSE,
      'loaded_layers', '[]'::jsonb,
      'error', 'ROW_CONTEXT_IDENTITY_REQUIRED',
      'message', 'bulk_process_row_context_v1 requires row_key, timesheet_id, or contract_week_id.',
      'filters', v_filters
    );
  END IF;

  v_row_key := NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '');
  v_timesheet_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', v_filters->>'expected_timesheet_id', v_filters->>'expectedTimesheetId', '')), '');
  v_contract_week_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', v_filters->>'week_id', v_filters->>'weekId', '')), '');
  v_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '');

  IF v_row_key IS NULL AND v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT NULLIF(BTRIM(row_key_values.value), '')
      INTO v_row_key
    FROM jsonb_array_elements_text(v_filters->'row_keys') WITH ORDINALITY AS row_key_values(value, ordinal_position)
    WHERE NULLIF(BTRIM(row_key_values.value), '') IS NOT NULL
    ORDER BY row_key_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_row_key IS NULL AND v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT NULLIF(BTRIM(row_key_values.value), '')
      INTO v_row_key
    FROM jsonb_array_elements_text(v_filters->'rowKeys') WITH ORDINALITY AS row_key_values(value, ordinal_position)
    WHERE NULLIF(BTRIM(row_key_values.value), '') IS NOT NULL
    ORDER BY row_key_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_row_key IS NULL AND NULLIF(BTRIM(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys', '')), '') IS NOT NULL THEN
    v_row_key := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys'), ',', 1)), '');
  END IF;

  IF v_timesheet_id_text IS NULL AND v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT NULLIF(BTRIM(timesheet_id_values.value), '')
      INTO v_timesheet_id_text
    FROM jsonb_array_elements_text(v_filters->'timesheet_ids') WITH ORDINALITY AS timesheet_id_values(value, ordinal_position)
    WHERE timesheet_id_values.value ~* v_uuid_re
    ORDER BY timesheet_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_timesheet_id_text IS NULL AND v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT NULLIF(BTRIM(timesheet_id_values.value), '')
      INTO v_timesheet_id_text
    FROM jsonb_array_elements_text(v_filters->'timesheetIds') WITH ORDINALITY AS timesheet_id_values(value, ordinal_position)
    WHERE timesheet_id_values.value ~* v_uuid_re
    ORDER BY timesheet_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_timesheet_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds'), ',', 1)), '') ~* v_uuid_re THEN
    v_timesheet_id_text := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds'), ',', 1)), '');
  END IF;

  IF v_contract_week_id_text IS NULL AND v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT NULLIF(BTRIM(contract_week_id_values.value), '')
      INTO v_contract_week_id_text
    FROM jsonb_array_elements_text(v_filters->'contract_week_ids') WITH ORDINALITY AS contract_week_id_values(value, ordinal_position)
    WHERE contract_week_id_values.value ~* v_uuid_re
    ORDER BY contract_week_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_contract_week_id_text IS NULL AND v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT NULLIF(BTRIM(contract_week_id_values.value), '')
      INTO v_contract_week_id_text
    FROM jsonb_array_elements_text(v_filters->'contractWeekIds') WITH ORDINALITY AS contract_week_id_values(value, ordinal_position)
    WHERE contract_week_id_values.value ~* v_uuid_re
    ORDER BY contract_week_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_contract_week_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds'), ',', 1)), '') ~* v_uuid_re THEN
    v_contract_week_id_text := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds'), ',', 1)), '');
  END IF;

  IF v_id_text IS NULL AND v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' THEN
    SELECT NULLIF(BTRIM(id_values.value), '')
      INTO v_id_text
    FROM jsonb_array_elements_text(v_filters->'ids') WITH ORDINALITY AS id_values(value, ordinal_position)
    WHERE id_values.value ~* v_uuid_re
    ORDER BY id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(v_filters->>'ids', ',', 1)), '') ~* v_uuid_re THEN
    v_id_text := NULLIF(BTRIM(SPLIT_PART(v_filters->>'ids', ',', 1)), '');
  END IF;

  IF v_row_key IS NOT NULL AND v_timesheet_id_text IS NULL AND v_row_key LIKE 'timesheet:%' THEN
    v_timesheet_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 11)), '');
  END IF;

  IF v_row_key IS NOT NULL AND v_contract_week_id_text IS NULL AND v_row_key LIKE 'contract_week:%' THEN
    v_contract_week_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 15)), '');
  END IF;

  IF v_timesheet_id_text IS NULL AND v_contract_week_id_text IS NULL AND v_id_text IS NOT NULL AND v_id_text ~* v_uuid_re THEN
    IF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('contract_week', 'contract-week', 'week', 'contractweek')
       OR LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_contract_week_only', v_filters->>'isContractWeekOnly', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN
      v_contract_week_id_text := v_id_text;
    ELSIF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('timesheet', 'timesheet_id', 'timesheet-id', 'ts') THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.timesheets AS identity_ts
      WHERE identity_ts.timesheet_id = v_id_text::uuid
        AND identity_ts.is_current = TRUE
    ) THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.contract_weeks AS identity_cw
      WHERE identity_cw.id = v_id_text::uuid
    ) THEN
      v_contract_week_id_text := v_id_text;
    ELSE
      v_timesheet_id_text := v_id_text;
    END IF;
  END IF;

  IF v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('timesheet_id', v_timesheet_id_text);
  END IF;

  IF v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('contract_week_id', v_contract_week_id_text);
  END IF;

  IF v_row_key IS NOT NULL THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('row_key', v_row_key);
  END IF;

  v_base_only := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'base_only', v_filters->>'baseOnly', '')), '')) IN ('true', '1', 'yes', 'y', 'on');

  v_profile := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'profile', v_filters->>'context_profile', v_filters->>'contextProfile', '')), ''));

  IF v_profile IS NULL THEN
    v_profile := 'status_header';
  END IF;

  IF v_profile NOT IN ('active_row_visible', 'status_header', 'editor', 'evidence', 'compare_import', 'full') THEN
    v_profile := 'status_header';
  END IF;

  v_include_evidence := CASE
    WHEN v_profile = 'evidence' THEN TRUE
    WHEN v_profile = 'full' THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_evidence', v_filters->>'includeEvidence', v_filters->>'load_evidence', v_filters->>'loadEvidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_include_compare := CASE
    WHEN v_profile IN ('compare_import', 'full') THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_compare', v_filters->>'includeCompare', v_filters->>'load_compare', v_filters->>'loadCompare', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_include_import_source_rows := CASE
    WHEN v_profile IN ('compare_import', 'full') THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_import_source_rows', v_filters->>'includeImportSourceRows', v_filters->>'load_import_source_rows', v_filters->>'loadImportSourceRows', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;


  IF v_profile = 'active_row_visible' AND (
    COALESCE(v_include_evidence, FALSE) = TRUE
    OR COALESCE(v_include_compare, FALSE) = TRUE
    OR COALESCE(v_include_import_source_rows, FALSE) = TRUE
  ) THEN
    v_layer_errors := '[]'::jsonb;
    v_layer_names := JSONB_BUILD_ARRAY('header', 'editor');

    v_editor_layer := public.bulk_process_row_context_v1(
      v_decision_filters
      || JSONB_BUILD_OBJECT(
           'profile', 'editor',
           'context_profile', 'editor',
           'include_evidence', FALSE,
           'include_compare', FALSE,
           'include_import_source_rows', FALSE
         )
    );

    IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_editor_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = FALSE THEN
      RETURN COALESCE(v_editor_layer, JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', 'active_row_visible',
        'profile', 'active_row_visible',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', COALESCE(v_editor_layer->>'degraded_reason', v_editor_layer->>'error', 'EDITOR_LAYER_FAILED'),
        'header_loaded', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_editor_layer->>'header_loaded', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'loaded_layers', COALESCE(v_editor_layer->'loaded_layers', '[]'::jsonb),
        'filters', v_filters
      );
    END IF;

    v_out := v_editor_layer || JSONB_BUILD_OBJECT(
      'context_kind', 'bulk_process_row_context',
      'context_profile', 'active_row_visible',
      'profile', 'active_row_visible',
      'context_type', 'bulk_process',
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'evidence_loaded', FALSE,
      'compare_loaded', FALSE,
      'full_loaded', FALSE,
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'soft_failure', FALSE,
      'context_degraded', FALSE,
      'degraded_reason', NULL::text,
      'loaded_layers', v_layer_names
    );

    IF COALESCE(v_include_evidence, FALSE) = TRUE THEN
      v_evidence_layer := public.bulk_process_row_context_v1(
        v_decision_filters
        || JSONB_BUILD_OBJECT(
             'profile', 'evidence',
             'context_profile', 'evidence',
             'include_evidence', TRUE,
             'include_compare', FALSE,
             'include_import_source_rows', FALSE
           )
      );

      IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
        v_layer_names := v_layer_names || JSONB_BUILD_ARRAY('evidence');
        v_out := v_out
          || JSONB_BUILD_OBJECT(
            'evidence_loaded', TRUE,
            'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
            'attached_evidence', COALESCE(v_evidence_layer->'attached_evidence', v_evidence_layer->'evidence', '[]'::jsonb),
            'attachedRows', COALESCE(v_evidence_layer->'attachedRows', v_evidence_layer->'attached_evidence', v_evidence_layer->'evidence', '[]'::jsonb),
            'primary_artifact', COALESCE(v_evidence_layer->'primary_artifact', JSONB_BUILD_OBJECT()),
            'preview_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), '')),
            'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), '')),
            'primary_artifact_preview_mode', NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_preview_mode', '')), ''),
            'has_any_evidence', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'has_any_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'attached_evidence_count', CASE WHEN COALESCE(v_evidence_layer->>'attached_evidence_count', '') ~ '^[0-9]+$' THEN (v_evidence_layer->>'attached_evidence_count')::integer ELSE COALESCE(JSONB_ARRAY_LENGTH(COALESCE(v_evidence_layer->'evidence', '[]'::jsonb)), 0) END,
            'evidence_count', CASE WHEN COALESCE(v_evidence_layer->>'evidence_count', '') ~ '^[0-9]+$' THEN (v_evidence_layer->>'evidence_count')::integer ELSE COALESCE(JSONB_ARRAY_LENGTH(COALESCE(v_evidence_layer->'evidence', '[]'::jsonb)), 0) END,
            'evidence_badges', COALESCE(v_evidence_layer->'evidence_badges', v_evidence_layer#>'{evidence_meta,evidence_badges}', '[]'::jsonb),
            'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE),
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'row', JSONB_BUILD_OBJECT()),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'data_row', v_evidence_layer->'row', JSONB_BUILD_OBJECT()),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'row_patch', JSONB_BUILD_OBJECT()),
            'action_flags', COALESCE(v_out->'action_flags', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'action_flags', JSONB_BUILD_OBJECT()),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
              'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
              'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE),
              'primary_artifact', COALESCE(v_evidence_layer->'primary_artifact', JSONB_BUILD_OBJECT()),
              'preview_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), '')),
              'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), '')),
              'primary_artifact_preview_mode', NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_preview_mode', '')), ''),
              'uploaded_pdf_r2_key', NULLIF(BTRIM(COALESCE(v_evidence_layer#>>'{details,uploaded_pdf_r2_key}', v_evidence_layer->>'uploaded_pdf_r2_key', '')), '')
            ),
            'left_pane', COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
              'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
              'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE),
              'primary_artifact', COALESCE(v_evidence_layer->'primary_artifact', JSONB_BUILD_OBJECT()),
              'preview_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), '')),
              'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), '')),
              'primary_artifact_preview_mode', NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_preview_mode', '')), ''),
              'uploaded_pdf_r2_key', NULLIF(BTRIM(COALESCE(v_evidence_layer#>>'{left_pane,uploaded_pdf_r2_key}', v_evidence_layer->>'uploaded_pdf_r2_key', '')), '')
            )
          );

        v_effective_has_any_evidence := COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'has_any_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE);
        v_effective_attached_evidence_count := CASE
          WHEN COALESCE(v_evidence_layer->>'attached_evidence_count', '') ~ '^[0-9]+$' THEN (v_evidence_layer->>'attached_evidence_count')::integer
          WHEN COALESCE(v_evidence_layer->>'evidence_count', '') ~ '^[0-9]+$' THEN (v_evidence_layer->>'evidence_count')::integer
          ELSE COALESCE(JSONB_ARRAY_LENGTH(COALESCE(v_evidence_layer->'evidence', '[]'::jsonb)), 0)
        END;
        v_effective_evidence_badges := COALESCE(v_evidence_layer->'evidence_badges', v_evidence_layer#>'{evidence_meta,evidence_badges}', '[]'::jsonb);
        v_effective_evidence_meta := COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT(
          'evidence_loaded', TRUE,
          'has_any_evidence', v_effective_has_any_evidence,
          'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
          'evidence_badges', v_effective_evidence_badges
        );
        v_effective_primary_artifact := COALESCE(v_evidence_layer->'primary_artifact', JSONB_BUILD_OBJECT());
        v_effective_primary_artifact_storage_key := COALESCE(
          NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), ''),
          NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), ''),
          NULLIF(BTRIM(COALESCE(v_effective_primary_artifact->>'storage_key', '')), ''),
          NULLIF(BTRIM(COALESCE(v_effective_primary_artifact->>'r2_key', '')), '')
        );
        v_effective_primary_artifact_preview_mode := COALESCE(
          NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_preview_mode', '')), ''),
          NULLIF(BTRIM(COALESCE(v_effective_primary_artifact->>'preview_mode', '')), '')
        );

        IF COALESCE(v_effective_attached_evidence_count, 0) <= 0 THEN
          v_effective_primary_artifact := JSONB_BUILD_OBJECT();
          v_effective_primary_artifact_storage_key := NULL;
          v_effective_primary_artifact_preview_mode := NULL;
        END IF;

        v_effective_primary_artifact_id := NULLIF(BTRIM(COALESCE(
          v_effective_primary_artifact->>'id',
          v_effective_primary_artifact->>'evidence_id',
          v_effective_primary_artifact->>'queue_id',
          ''
        )), '');
        v_effective_primary_artifact_kind := NULLIF(BTRIM(COALESCE(
          v_effective_primary_artifact->>'kind',
          v_effective_primary_artifact->>'staged_kind',
          ''
        )), '');
        v_effective_primary_artifact_display_name := NULLIF(BTRIM(COALESCE(
          v_effective_primary_artifact->>'display_name',
          v_effective_primary_artifact->>'filename',
          v_effective_primary_artifact->>'original_filename',
          ''
        )), '');
        v_effective_uploaded_pdf_r2_key := CASE
          WHEN NULLIF(BTRIM(COALESCE(
            v_out->>'timesheet_id',
            v_out->>'current_timesheet_id',
            v_out#>>'{row,timesheet_id}',
            v_out#>>'{row,current_timesheet_id}',
            v_out#>>'{data_row,timesheet_id}',
            v_out#>>'{data_row,current_timesheet_id}',
            ''
          )), '') IS NULL THEN NULLIF(BTRIM(COALESCE(
            v_evidence_layer->>'uploaded_pdf_r2_key',
            v_evidence_layer#>>'{details,uploaded_pdf_r2_key}',
            v_evidence_layer#>>'{details,contract_week,uploaded_pdf_r2_key}',
            v_evidence_layer#>>'{left_pane,uploaded_pdf_r2_key}',
            ''
          )), '')
          ELSE NULL::text
        END;
        v_effective_artifact_hints := JSONB_BUILD_OBJECT(
          'has_any_evidence', v_effective_has_any_evidence,
          'evidence_badges', v_effective_evidence_badges,
          'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
          'primary_artifact', v_effective_primary_artifact,
          'primary_artifact_id', v_effective_primary_artifact_id,
          'primary_artifact_kind', v_effective_primary_artifact_kind,
          'primary_artifact_display_name', v_effective_primary_artifact_display_name,
          'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
          'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
          'preview_storage_key', v_effective_primary_artifact_storage_key,
          'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key
        );
        v_effective_action_flags := COALESCE(v_out->'action_flags', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'action_flags', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
          'has_any_evidence', v_effective_has_any_evidence,
          'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
          'evidence_badges', v_effective_evidence_badges
        );
        v_effective_row_patch := COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
          'has_any_evidence', v_effective_has_any_evidence,
          'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
          'evidence_badges', v_effective_evidence_badges,
          'artifact_hints', v_effective_artifact_hints,
          'primary_artifact', v_effective_primary_artifact,
          'primary_artifact_id', v_effective_primary_artifact_id,
          'primary_artifact_kind', v_effective_primary_artifact_kind,
          'primary_artifact_display_name', v_effective_primary_artifact_display_name,
          'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
          'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
          'preview_storage_key', v_effective_primary_artifact_storage_key,
          'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key
        );
        v_effective_row := COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
          'has_any_evidence', v_effective_has_any_evidence,
          'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
          'evidence_badges', v_effective_evidence_badges,
          'artifact_hints', v_effective_artifact_hints,
          'action_flags', COALESCE(v_out#>'{row,action_flags}', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer#>'{row,action_flags}', JSONB_BUILD_OBJECT()) || v_effective_action_flags,
          'row_patch', v_effective_row_patch,
          'primary_artifact', v_effective_primary_artifact,
          'primary_artifact_id', v_effective_primary_artifact_id,
          'primary_artifact_kind', v_effective_primary_artifact_kind,
          'primary_artifact_display_name', v_effective_primary_artifact_display_name,
          'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
          'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
          'preview_storage_key', v_effective_primary_artifact_storage_key,
          'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key
        );
        v_effective_data_row := COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'data_row', v_evidence_layer->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
          'has_any_evidence', v_effective_has_any_evidence,
          'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
          'evidence_badges', v_effective_evidence_badges,
          'artifact_hints', v_effective_artifact_hints,
          'action_flags', COALESCE(v_out#>'{data_row,action_flags}', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer#>'{data_row,action_flags}', JSONB_BUILD_OBJECT()) || v_effective_action_flags,
          'row_patch', v_effective_row_patch,
          'primary_artifact', v_effective_primary_artifact,
          'primary_artifact_id', v_effective_primary_artifact_id,
          'primary_artifact_kind', v_effective_primary_artifact_kind,
          'primary_artifact_display_name', v_effective_primary_artifact_display_name,
          'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
          'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
          'preview_storage_key', v_effective_primary_artifact_storage_key,
          'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key
        );
        v_effective_details := COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
          'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
          'evidence_meta', v_effective_evidence_meta,
          'artifact_hints', v_effective_artifact_hints,
          'primary_artifact', v_effective_primary_artifact,
          'primary_artifact_id', v_effective_primary_artifact_id,
          'primary_artifact_kind', v_effective_primary_artifact_kind,
          'primary_artifact_display_name', v_effective_primary_artifact_display_name,
          'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
          'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
          'preview_storage_key', v_effective_primary_artifact_storage_key,
          'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key
        );
        v_effective_left_pane := COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
          'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
          'evidence_meta', v_effective_evidence_meta,
          'artifact_hints', v_effective_artifact_hints,
          'primary_artifact', v_effective_primary_artifact,
          'primary_artifact_id', v_effective_primary_artifact_id,
          'primary_artifact_kind', v_effective_primary_artifact_kind,
          'primary_artifact_display_name', v_effective_primary_artifact_display_name,
          'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
          'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
          'preview_storage_key', v_effective_primary_artifact_storage_key,
          'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key
        );

        v_out := v_out || JSONB_BUILD_OBJECT(
          'has_any_evidence', v_effective_has_any_evidence,
          'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
          'evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
          'evidence_badges', v_effective_evidence_badges,
          'evidence_meta', v_effective_evidence_meta,
          'artifact_hints', v_effective_artifact_hints,
          'action_flags', v_effective_action_flags,
          'row_patch', v_effective_row_patch,
          'row', v_effective_row,
          'data_row', v_effective_data_row,
          'details', v_effective_details,
          'left_pane', v_effective_left_pane,
          'primary_artifact', v_effective_primary_artifact,
          'primary_artifact_id', v_effective_primary_artifact_id,
          'primary_artifact_kind', v_effective_primary_artifact_kind,
          'primary_artifact_display_name', v_effective_primary_artifact_display_name,
          'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
          'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
          'preview_storage_key', v_effective_primary_artifact_storage_key,
          'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key
        );
      ELSE
        v_layer_errors := v_layer_errors || JSONB_BUILD_ARRAY(COALESCE(v_evidence_layer->>'degraded_reason', v_evidence_layer->>'error', 'EVIDENCE_LAYER_FAILED'));
        v_out := v_out || JSONB_BUILD_OBJECT(
          'evidence_loaded', FALSE,
          'soft_failure', TRUE,
          'context_degraded', TRUE,
          'degraded_reason', 'EVIDENCE_LAYER_FAILED',
          'evidence_layer_failure', COALESCE(v_evidence_layer, JSONB_BUILD_OBJECT())
        );
      END IF;
    END IF;

    IF COALESCE(v_include_compare, FALSE) = TRUE OR COALESCE(v_include_import_source_rows, FALSE) = TRUE THEN
      v_compare_layer := public.bulk_process_row_context_v1(
        v_decision_filters
        || JSONB_BUILD_OBJECT(
             'profile', 'compare_import',
             'context_profile', 'compare_import',
             'include_evidence', FALSE,
             'include_compare', COALESCE(v_include_compare, FALSE),
             'include_import_source_rows', COALESCE(v_include_import_source_rows, FALSE)
           )
      );

      IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_compare_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
        v_layer_names := v_layer_names || JSONB_BUILD_ARRAY('compare_import');
        v_out := v_out || JSONB_BUILD_OBJECT(
          'compare_loaded', TRUE,
          'compare', COALESCE(v_compare_layer->'compare', JSONB_BUILD_OBJECT()),
          'compare_payload', COALESCE(v_compare_layer->'compare_payload', v_compare_layer->'compare', JSONB_BUILD_OBJECT()),
          'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
            'source_rows', COALESCE(v_compare_layer#>'{details,source_rows}', v_compare_layer#>'{compare,source_rows}', '[]'::jsonb),
            'external_source_rows_json', COALESCE(v_compare_layer#>'{details,external_source_rows_json}', v_compare_layer#>'{compare,external_source_rows_json}', '[]'::jsonb)
          ),
          'left_pane', COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
            'source_items', COALESCE(v_compare_layer#>'{left_pane,source_items}', v_compare_layer#>'{details,source_rows}', v_compare_layer#>'{compare,source_rows}', '[]'::jsonb)
          )
        );
      ELSE
        v_layer_errors := v_layer_errors || JSONB_BUILD_ARRAY(COALESCE(v_compare_layer->>'degraded_reason', v_compare_layer->>'error', 'COMPARE_IMPORT_LAYER_FAILED'));
        v_out := v_out || JSONB_BUILD_OBJECT(
          'compare_loaded', FALSE,
          'soft_failure', TRUE,
          'context_degraded', TRUE,
          'degraded_reason', CASE WHEN JSONB_ARRAY_LENGTH(v_layer_errors) > 0 THEN 'LAYER_FAILURE' ELSE 'COMPARE_IMPORT_LAYER_FAILED' END,
          'compare_import_layer_failure', COALESCE(v_compare_layer, JSONB_BUILD_OBJECT())
        );
      END IF;
    END IF;

    v_out := v_out || JSONB_BUILD_OBJECT(
      'context_kind', 'bulk_process_row_context',
      'context_profile', 'active_row_visible',
      'profile', 'active_row_visible',
      'context_type', 'bulk_process',
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'full_loaded', FALSE,
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'loaded_layers', v_layer_names,
      'layer_errors', v_layer_errors,
      'filters', v_filters
    );


    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      v_canonical_row_json := NULL;
      v_canonical_row_signature := NULL;
      v_canonical_row_key := NULLIF(BTRIM(COALESCE(v_out->>'row_key', v_out#>>'{row,row_key}', v_out#>>'{data_row,row_key}', v_out#>>'{row_patch,row_key}', '')), '');
      v_canonical_timesheet_id := NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', v_out->>'current_timesheet_id', v_out#>>'{row,timesheet_id}', v_out#>>'{row,current_timesheet_id}', v_out#>>'{data_row,timesheet_id}', v_out#>>'{data_row,current_timesheet_id}', v_out#>>'{row_patch,timesheet_id}', v_out#>>'{row_patch,current_timesheet_id}', '')), '');
      v_canonical_contract_week_id := CASE
        WHEN v_canonical_timesheet_id IS NULL THEN NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', v_out#>>'{row,contract_week_id}', v_out#>>'{data_row,contract_week_id}', v_out#>>'{row_patch,contract_week_id}', '')), '')
        ELSE NULL
      END;

      IF v_canonical_row_key IS NOT NULL OR v_canonical_timesheet_id IS NOT NULL OR v_canonical_contract_week_id IS NOT NULL THEN
        SELECT canonical_result.row_json
        INTO v_canonical_row_json
        FROM public.bulk_timesheet_row_patch_v1(
          JSONB_STRIP_NULLS(
            JSONB_BUILD_OBJECT(
              'dataset_mode', 'process',
              'projection', 'active_row_header',
              'profile', COALESCE(NULLIF(BTRIM(v_profile), ''), 'status_header'),
              'row_key', v_canonical_row_key,
              'timesheet_id', v_canonical_timesheet_id,
              'current_timesheet_id', v_canonical_timesheet_id,
              'requested_timesheet_id', v_canonical_timesheet_id,
              'expected_timesheet_id', v_canonical_timesheet_id,
              'contract_week_id', v_canonical_contract_week_id
            )
          )
        ) AS canonical_result(row_json)
        WHERE canonical_result.row_json IS NOT NULL
        ORDER BY canonical_result.row_json->>'row_key'
        LIMIT 1;

        v_canonical_row_signature := NULLIF(BTRIM(COALESCE(v_canonical_row_json->>'row_signature', '')), '');
        IF v_canonical_row_signature IS NOT NULL THEN
          v_out := v_out || JSONB_BUILD_OBJECT(
            'row_signature', v_canonical_row_signature,
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature)
          );
        END IF;
      END IF;
    END IF;

    RETURN private._candidate_office_context_overlay_v1(public.bulk_process_retention_contract_patch_v1(v_out));
  END IF;


  IF v_profile IN ('status_header', 'editor') OR (v_profile = 'active_row_visible' AND COALESCE(v_include_evidence, FALSE) = FALSE) THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM (
        SELECT input_ids.*
        FROM input_ids
        WHERE input_ids.input_timesheet_id IS NULL
          AND input_ids.input_contract_week_id IS NULL
      ) AS summary_ids
      CROSS JOIN LATERAL public.timesheet_summary_lightweight_rows_v1(
        v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)
      ) AS summary_source
      WHERE (
          summary_ids.input_timesheet_id IS NULL
          OR summary_source.timesheet_id = summary_ids.input_timesheet_id
        )
        AND (
          summary_ids.input_contract_week_id IS NULL
          OR summary_source.contract_week_id = summary_ids.input_contract_week_id
        )
      ORDER BY
        CASE WHEN summary_ids.input_timesheet_id IS NOT NULL AND summary_source.timesheet_id = summary_ids.input_timesheet_id THEN 0 ELSE 1 END,
        CASE WHEN summary_ids.input_contract_week_id IS NOT NULL AND summary_source.contract_week_id = summary_ids.input_contract_week_id THEN 0 ELSE 1 END,
        summary_source.timesheet_id NULLS LAST,
        summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (
          cw_ids.input_contract_week_id IS NOT NULL
          AND cw0.id = cw_ids.input_contract_week_id
        )
        OR (
          cw_ids.input_timesheet_id IS NOT NULL
          AND cw0.timesheet_id = cw_ids.input_timesheet_id
        )
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    tsfin_row AS (
      SELECT tf0.*
      FROM public.timesheets_financials AS tf0
      LEFT JOIN timesheet_row AS ts_for_tf ON TRUE
      WHERE tf0.is_current = TRUE
        AND ts_for_tf.timesheet_id IS NOT NULL
        AND tf0.timesheet_id = ts_for_tf.timesheet_id
      ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      LIMIT 1
    ),
    contract_row AS (
      SELECT ct0.*
      FROM public.contracts AS ct0
      LEFT JOIN timesheet_row AS ts_for_ct ON TRUE
      LEFT JOIN contract_week_row AS cw_for_ct ON TRUE
      WHERE ct0.id = COALESCE(ts_for_ct.contract_id, cw_for_ct.contract_id)
      ORDER BY ct0.updated_at DESC NULLS LAST, ct0.created_at DESC NULLS LAST, ct0.id DESC
      LIMIT 1
    ),
    candidate_row AS (
      SELECT cand0.*
      FROM public.candidates AS cand0
      LEFT JOIN contract_row AS ct_for_cand ON TRUE
      WHERE cand0.id = ct_for_cand.candidate_id
      LIMIT 1
    ),
    client_row AS (
      SELECT cli0.*
      FROM public.clients AS cli0
      LEFT JOIN contract_row AS ct_for_cli ON TRUE
      WHERE cli0.id = ct_for_cli.client_id
      LIMIT 1
    ),
    validation_rows AS (
      SELECT tv0.*
      FROM public.timesheet_validations AS tv0
      LEFT JOIN timesheet_row AS ts_for_tv ON TRUE
      WHERE ts_for_tv.timesheet_id IS NOT NULL
        AND tv0.timesheet_id = ts_for_tv.timesheet_id
      ORDER BY tv0.validated_at_utc DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
    ),
    validation_payload AS (
      SELECT
        COALESCE(JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'id', validation_rows.id,
            'timesheet_id', validation_rows.timesheet_id,
            'booking_id', validation_rows.booking_id,
            'status', validation_rows.status,
            'reason_code', validation_rows.reason_code,
            'hr_request_id', validation_rows.hr_request_id,
            'validated_at_utc', validation_rows.validated_at_utc,
            'override_requested_at_utc', validation_rows.override_requested_at_utc,
            'override_requested_by', validation_rows.override_requested_by,
            'override_confirmed_at_utc', validation_rows.override_confirmed_at_utc,
            'override_confirmed_by', validation_rows.override_confirmed_by,
            'override_reason', validation_rows.override_reason,
            'override_cleared_at_utc', validation_rows.override_cleared_at_utc,
            'last_source', validation_rows.last_source,
            'created_at', validation_rows.created_at,
            'updated_at', validation_rows.updated_at,
            'hr_request_source', validation_rows.hr_request_source,
            'hr_request_set_by', validation_rows.hr_request_set_by,
            'hr_request_set_at_utc', validation_rows.hr_request_set_at_utc,
            'pre_validated', validation_rows.pre_validated
          ) ORDER BY validation_rows.validated_at_utc DESC NULLS LAST, validation_rows.created_at DESC NULLS LAST, validation_rows.id DESC
        ), '[]'::jsonb) AS validations_json,
        (
          SELECT JSONB_BUILD_OBJECT(
            'id', validation_latest.id,
            'status', validation_latest.status,
            'reason_code', validation_latest.reason_code,
            'hr_request_id', validation_latest.hr_request_id,
            'validated_at_utc', validation_latest.validated_at_utc,
            'pre_validated', COALESCE(validation_latest.pre_validated, FALSE),
            'updated_at', validation_latest.updated_at
          )
          FROM validation_rows AS validation_latest
          ORDER BY validation_latest.validated_at_utc DESC NULLS LAST, validation_latest.created_at DESC NULLS LAST, validation_latest.id DESC
          LIMIT 1
        ) AS latest_validation_json
      FROM validation_rows
    ),
    core_row AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id, contract_row.id) AS resolved_contract_id,
        COALESCE(summary_row.candidate_id, contract_row.candidate_id, candidate_row.id) AS resolved_candidate_id,
        COALESCE(summary_row.client_id, contract_row.client_id, client_row.id) AS resolved_client_id,
        COALESCE(NULLIF(BTRIM(summary_row.candidate_name), ''), NULLIF(BTRIM(summary_row.candidate_display_name), ''), NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', candidate_row.first_name, candidate_row.last_name)), '')) AS candidate_name,
        COALESCE(NULLIF(BTRIM(summary_row.candidate_display_name), ''), NULLIF(BTRIM(summary_row.candidate_name), ''), NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', candidate_row.first_name, candidate_row.last_name)), '')) AS candidate_display_name,
        COALESCE(NULLIF(BTRIM(summary_row.client_name), ''), NULLIF(BTRIM(client_row.name), '')) AS client_name,
        COALESCE(summary_row.week_ending_date, timesheet_row.week_ending_date, contract_week_row.week_ending_date) AS week_ending_date,
        COALESCE(summary_row.work_date, timesheet_row.worked_start_iso::date, contract_week_row.week_ending_date) AS work_date,
        COALESCE(summary_row.booking_id, timesheet_row.booking_id) AS booking_id,
        COALESCE(summary_row.occupant_key_norm, timesheet_row.occupant_key_norm, tsfin_row.occupant_key_norm) AS occupant_key_norm,
        COALESCE(summary_row.hospital_norm, timesheet_row.hospital_norm) AS hospital_norm,
        COALESCE(summary_row.candidate_hint_text, timesheet_row.candidate_hint_text, JSONB_BUILD_OBJECT()) AS candidate_hint_text,
        COALESCE(summary_row.sheet_scope, timesheet_row.sheet_scope::text) AS sheet_scope,
        COALESCE(summary_row.submission_mode, timesheet_row.submission_mode::text) AS submission_mode,
        COALESCE(summary_row.submission_mode_snapshot, contract_week_row.submission_mode_snapshot::text) AS submission_mode_snapshot,
        COALESCE(summary_row.basis, tsfin_row.basis::text) AS basis,
        summary_row.route_type AS route_type,
        summary_row.route_display AS route_display,
        COALESCE(summary_row.route_family, CASE WHEN COALESCE(timesheet_row.qr_status::text, '') <> '' THEN 'QR' WHEN UPPER(COALESCE(timesheet_row.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'ELECTRONIC' THEN 'ELECTRONIC' ELSE 'MANUAL_NON_QR' END) AS route_family,
        summary_row.route_subfamily AS route_subfamily,
        summary_row.underlying_channel_family AS underlying_channel_family,
        COALESCE(summary_row.summary_stage, CASE WHEN timesheet_row.timesheet_id IS NULL THEN 'UNPROCESSED' ELSE 'PROCESSED' END) AS summary_stage,
        COALESCE(
          summary_row.tools_stage,
          CASE
            WHEN timesheet_row.archived_at_utc IS NOT NULL THEN 'ARCHIVED'
            WHEN timesheet_row.timesheet_id IS NULL THEN 'UNPROCESSED'
            ELSE COALESCE(tsfin_row.processing_status::text, timesheet_row.status::text)
          END
        ) AS tools_stage,
        COALESCE(summary_row.processing_status, tsfin_row.processing_status::text) AS processing_status,
        summary_row.processing_status_display AS processing_status_display,
        COALESCE(summary_row.authorised_at_utc, tsfin_row.authorised_at_utc) AS authorised_at_utc,
        COALESCE(summary_row.authorised_at_server, timesheet_row.authorised_at_server) AS authorised_at_server,
        COALESCE(summary_row.processed_at_utc, tsfin_row.processed_at_utc) AS processed_at_utc,
        COALESCE(summary_row.is_authorised, tsfin_row.authorised_at_utc IS NOT NULL, timesheet_row.authorised_at_server IS NOT NULL, FALSE) AS is_authorised,
        COALESCE(summary_row.total_hours, tsfin_row.total_hours) AS total_hours,
        COALESCE(summary_row.total_pay_ex_vat, tsfin_row.total_pay_ex_vat) AS total_pay_ex_vat,
        COALESCE(summary_row.total_charge_ex_vat, tsfin_row.total_charge_ex_vat) AS total_charge_ex_vat,
        COALESCE(summary_row.margin_ex_vat, tsfin_row.margin_ex_vat) AS margin_ex_vat,
        summary_row.net_delta_ex_vat AS net_delta_ex_vat,
        COALESCE(summary_row.paid_at_utc, tsfin_row.paid_at_utc) AS paid_at_utc,
        summary_row.pay_icon_code AS pay_icon_code,
        summary_row.pay_status_code AS pay_status_code,
        summary_row.pay_paid_at_utc AS pay_paid_at_utc,
        COALESCE(summary_row.invoice_is_paid, FALSE) AS invoice_is_paid,
        summary_row.invoice_issue_stage AS invoice_issue_stage,
        summary_row.invoice_segment_stage AS invoice_segment_stage,
        COALESCE(summary_row.invoice_segments_total, 0) AS invoice_segments_total,
        COALESCE(summary_row.invoice_segments_locked, 0) AS invoice_segments_locked,
        COALESCE(summary_row.invoice_segments_unlocked, 0) AS invoice_segments_unlocked,
        COALESCE(TO_JSONB(summary_row.issue_codes), '[]'::jsonb) AS issue_codes_json,
        COALESCE(summary_row.validation_status, validation_payload.latest_validation_json->>'status') AS validation_status,
        summary_row.validation_summary AS validation_summary,
        COALESCE(summary_row.hr_crosscheck_status, tsfin_row.hr_crosscheck_status) AS hr_crosscheck_status,
        COALESCE(TO_JSONB(summary_row.hr_crosscheck_issues), TO_JSONB(tsfin_row.hr_crosscheck_issues), '[]'::jsonb) AS hr_crosscheck_issues_json,
        COALESCE(summary_row.qr_status, timesheet_row.qr_status::text) AS qr_status,
        COALESCE(summary_row.is_qr, timesheet_row.qr_status IS NOT NULL, FALSE) AS is_qr,
        COALESCE(summary_row.is_adjusted, timesheet_row.is_adjustment, contract_week_row.is_adjustment, FALSE) AS is_adjusted,
        COALESCE(summary_row.needs_attention, FALSE) AS needs_attention,
        COALESCE(summary_row.has_rate_issue, tsfin_row.has_rate_issue, FALSE) AS has_rate_issue,
        COALESCE(summary_row.has_pay_channel_issue, tsfin_row.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
        COALESCE(summary_row.client_no_timesheet_required, contract_row.no_timesheet_required, FALSE) AS client_no_timesheet_required,
        COALESCE(summary_row.client_autoprocess_hr, contract_row.autoprocess_hr, FALSE) AS client_autoprocess_hr,
        COALESCE(summary_row.client_is_nhsp, contract_row.is_nhsp, FALSE) AS client_is_nhsp,
        COALESCE(summary_row.has_any_evidence, FALSE) AS has_any_evidence,
        COALESCE(summary_row.attached_evidence_count, 0) AS attached_evidence_count,
        summary_row.primary_artifact_storage_key AS primary_artifact_storage_key,
        summary_row.primary_artifact_display_name AS primary_artifact_display_name,
        summary_row.primary_artifact_preview_mode AS primary_artifact_preview_mode,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.updated_at AS contract_week_updated_at,
        tsfin_row.updated_at AS tsfin_updated_at,
        timesheet_row.version AS timesheet_version,
        timesheet_row.status AS timesheet_status,
        timesheet_row.manual_pdf_r2_key AS manual_pdf_r2_key,
        timesheet_row.qr_r2_key AS qr_r2_key,
        timesheet_row.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
        timesheet_row.generated_pdf_at_utc AS generated_pdf_at_utc,
        timesheet_row.actual_schedule_json AS timesheet_actual_schedule_json,
        timesheet_row.additional_units_week AS timesheet_additional_units_week,
        timesheet_row.additional_units_per_day AS timesheet_additional_units_per_day,
        timesheet_row.sheet_scope AS timesheet_sheet_scope,
        timesheet_row.worked_start_iso AS timesheet_worked_start_iso,
        timesheet_row.worked_end_iso AS timesheet_worked_end_iso,
        timesheet_row.break_start_iso AS timesheet_break_start_iso,
        timesheet_row.break_end_iso AS timesheet_break_end_iso,
        timesheet_row.break_minutes AS timesheet_break_minutes,
        timesheet_row.worked_minutes AS timesheet_worked_minutes,
        timesheet_row.auth_name AS timesheet_auth_name,
        timesheet_row.auth_job_title AS timesheet_auth_job_title,
        timesheet_row.reference_number AS timesheet_reference_number,
        timesheet_row.reference_set_at AS timesheet_reference_set_at,
        timesheet_row.created_at AS timesheet_created_at,
        timesheet_row.is_adjustment AS timesheet_is_adjustment,
        timesheet_row.parent_timesheet_id AS timesheet_parent_timesheet_id,
        timesheet_row.adjustment_origin AS timesheet_adjustment_origin,
        CASE
          WHEN COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) IS NULL THEN contract_week_row.uploaded_pdf_r2_key
          ELSE NULL::text
        END AS uploaded_pdf_r2_key,
        contract_week_row.day_entries_json AS contract_week_day_entries_json,
        contract_week_row.totals_json AS contract_week_totals_json,
        contract_week_row.planned_schedule_json AS contract_week_planned_schedule_json,
        contract_week_row.additional_seq AS contract_week_additional_seq,
        contract_week_row.status AS contract_week_status,
        contract_week_row.created_at AS contract_week_created_at,
        contract_week_row.is_adjustment AS contract_week_is_adjustment,
        contract_week_row.enforce_day_partition AS contract_week_enforce_day_partition,
        contract_week_row.allowed_days_mask AS contract_week_allowed_days_mask,
        contract_week_row.split_boundary_date AS contract_week_split_boundary_date,
        contract_week_row.worker_note AS contract_week_worker_note,
        contract_week_row.split_group_key AS contract_week_split_group_key,
        contract_row.id AS contract_id,
        contract_row.role AS contract_role,
        contract_row.band AS contract_band,
        contract_row.display_site AS contract_display_site,
        contract_row.ward_hint AS contract_ward_hint,
        contract_row.default_submission_mode AS contract_default_submission_mode,
        contract_row.std_schedule_json AS contract_std_schedule_json,
        contract_row.additional_rates_json AS contract_additional_rates_json,
        contract_row.weekly_timesheet_source AS contract_weekly_timesheet_source,
        contract_row.no_timesheet_required AS contract_no_timesheet_required,
        contract_row.autoprocess_hr AS contract_autoprocess_hr,
        contract_row.requires_hr AS contract_requires_hr,
        contract_row.hr_attach_to_invoice AS contract_hr_attach_to_invoice,
        contract_row.ts_attach_to_invoice AS contract_ts_attach_to_invoice,
        contract_row.is_nhsp AS contract_is_nhsp,
        candidate_row.id AS candidate_id,
        candidate_row.tms_ref AS candidate_tms_ref,
        candidate_row.first_name AS candidate_first_name,
        candidate_row.last_name AS candidate_last_name,
        candidate_row.display_name AS candidate_display_name_raw,
        candidate_row.email AS candidate_email,
        candidate_row.phone AS candidate_phone,
        candidate_row.key_norm AS candidate_key_norm,
        candidate_row.band AS candidate_band,
        client_row.id AS client_id,
        client_row.cli_ref AS client_cli_ref,
        client_row.name AS client_name_raw,
        client_row.vat_chargeable AS client_vat_chargeable,
        client_row.ts_queries_email AS client_ts_queries_email,
        tsfin_row.id AS tsfin_id,
        tsfin_row.timesheet_version AS tsfin_timesheet_version,
        tsfin_row.basis AS tsfin_basis,
        tsfin_row.is_current AS tsfin_is_current,
        tsfin_row.is_stale AS tsfin_is_stale,
        tsfin_row.stale_reason AS tsfin_stale_reason,
        tsfin_row.actual_schedule_json AS tsfin_actual_schedule_json,
        tsfin_row.actual_minutes_by_day_json AS tsfin_actual_minutes_by_day_json,
        tsfin_row.additional_units_json AS tsfin_additional_units_json,
        tsfin_row.processing_status AS tsfin_processing_status,
        tsfin_row.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
        tsfin_row.locked_at_utc AS tsfin_locked_at_utc,
        tsfin_row.paid_at_utc AS tsfin_paid_at_utc,
        tsfin_row.processed_at_utc AS tsfin_processed_at_utc,
        tsfin_row.authorised_at_utc AS tsfin_authorised_at_utc,
        tsfin_row.external_source_rows_json AS tsfin_external_source_rows_json,
        tsfin_row.total_pay_ex_vat AS tsfin_total_pay_ex_vat,
        tsfin_row.total_charge_ex_vat AS tsfin_total_charge_ex_vat,
        tsfin_row.margin_ex_vat AS tsfin_margin_ex_vat,
        tsfin_row.expenses_pay_ex_vat AS tsfin_expenses_pay_ex_vat,
        tsfin_row.expenses_charge_ex_vat AS tsfin_expenses_charge_ex_vat,
        tsfin_row.mileage_units AS tsfin_mileage_units,
        tsfin_row.mileage_pay_ex_vat AS tsfin_mileage_pay_ex_vat,
        tsfin_row.mileage_charge_ex_vat AS tsfin_mileage_charge_ex_vat,
        tsfin_row.travel_pay_ex_vat AS tsfin_travel_pay_ex_vat,
        tsfin_row.travel_charge_ex_vat AS tsfin_travel_charge_ex_vat,
        tsfin_row.accommodation_pay_ex_vat AS tsfin_accommodation_pay_ex_vat,
        tsfin_row.accommodation_charge_ex_vat AS tsfin_accommodation_charge_ex_vat,
        tsfin_row.other_pay_ex_vat AS tsfin_other_pay_ex_vat,
        tsfin_row.other_charge_ex_vat AS tsfin_other_charge_ex_vat,
        tsfin_row.additional_pay_ex_vat AS tsfin_additional_pay_ex_vat,
        tsfin_row.additional_charge_ex_vat AS tsfin_additional_charge_ex_vat,
        tsfin_row.additional_margin_ex_vat AS tsfin_additional_margin_ex_vat,
        validation_payload.validations_json AS validations_json,
        validation_payload.latest_validation_json AS latest_validation_json
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN tsfin_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
      LEFT JOIN contract_row ON TRUE
      LEFT JOIN candidate_row ON TRUE
      LEFT JOIN client_row ON TRUE
      LEFT JOIN validation_payload ON TRUE
    ),
    flags AS (
      SELECT
        core_row.*,
        (core_row.resolved_timesheet_id IS NOT NULL OR core_row.resolved_contract_week_id IS NOT NULL) AS row_found,
        (
          UPPER(COALESCE(core_row.tools_stage, '')) = 'ARCHIVED'
          OR core_row.tsfin_locked_by_invoice_id IS NOT NULL
          OR COALESCE(core_row.invoice_segments_locked, 0) > 0
          OR COALESCE(core_row.invoice_is_paid, FALSE) = TRUE
        ) AS locked_bool,
        COALESCE(core_row.is_authorised, FALSE) AS authorised_bool,
        (
          core_row.resolved_timesheet_id IS NULL
          OR UPPER(COALESCE(core_row.processing_status, core_row.tools_stage, core_row.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
        ) AS unprocessed_bool,
        (
          UPPER(COALESCE(core_row.processing_status, '')) = 'PENDING_AUTH'
          OR (
            COALESCE(core_row.contract_requires_hr, FALSE) = TRUE
            AND COALESCE(core_row.contract_autoprocess_hr, FALSE) = FALSE
            AND UPPER(COALESCE(core_row.processing_status, '')) = 'READY_FOR_HR'
          )
        ) AS requires_authorisation_bool,
        (
          UPPER(COALESCE(core_row.qr_status, '')) = 'PENDING'
          AND core_row.resolved_timesheet_id IS NOT NULL
        ) AS qr_unsigned_blocked_bool,
        (
          COALESCE(core_row.timesheet_is_adjustment, core_row.contract_week_is_adjustment, FALSE) = TRUE
          AND COALESCE(core_row.contract_week_additional_seq, 0) > 0
          AND (
            core_row.timesheet_actual_schedule_json IS NULL
            OR core_row.timesheet_actual_schedule_json = '[]'::jsonb
            OR (jsonb_typeof(core_row.timesheet_actual_schedule_json) = 'array' AND jsonb_array_length(core_row.timesheet_actual_schedule_json) = 0)
          )
        ) AS keep_blank_additional_schedule_bool
      FROM core_row
    ),
    row_payload AS (
      SELECT
        JSONB_BUILD_OBJECT(
          'ok', TRUE,
          'context_kind', 'bulk_process_row_context',
          'context_profile', v_profile,
          'profile', v_profile,
          'context_type', CASE WHEN 'bulk_process_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
          'slim_context', TRUE,
          'header_loaded', TRUE,
          'header_only', (v_profile = 'status_header'),
          'editor_loaded', (v_profile IN ('editor', 'active_row_visible')),
          'evidence_loaded', FALSE,
          'compare_loaded', FALSE,
          'full_loaded', FALSE,
          'schedule_pending', NOT (v_profile IN ('editor', 'active_row_visible')),
          'schedule_authoritative', (v_profile IN ('editor', 'active_row_visible')),
          'loaded_layers', CASE WHEN v_profile = 'status_header' THEN JSONB_BUILD_ARRAY('header') ELSE JSONB_BUILD_ARRAY('header', 'editor') END,
          'soft_failure', FALSE,
          'context_degraded', FALSE,
          'degraded_reason', NULL::text,
          'row_key', CASE WHEN flags.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || flags.resolved_timesheet_id::text ELSE 'contract_week:' || flags.resolved_contract_week_id::text END,
          'timesheet_id', flags.resolved_timesheet_id,
          'current_timesheet_id', flags.resolved_timesheet_id,
          'requested_timesheet_id', flags.resolved_timesheet_id,
          'expected_timesheet_id', flags.resolved_timesheet_id,
          'contract_week_id', flags.resolved_contract_week_id,
          'row_signature', MD5(CONCAT_WS('|', COALESCE(flags.resolved_timesheet_id::text, ''), COALESCE(flags.resolved_contract_week_id::text, ''), COALESCE(flags.timesheet_version::text, ''), COALESCE(flags.timesheet_updated_at::text, ''), COALESCE(flags.contract_week_updated_at::text, ''), COALESCE(flags.tsfin_updated_at::text, ''), v_profile)),
          'filters', v_filters
        )
        || JSONB_BUILD_OBJECT(
          'row', (
            JSONB_BUILD_OBJECT(
              'row_key', CASE WHEN flags.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || flags.resolved_timesheet_id::text ELSE 'contract_week:' || flags.resolved_contract_week_id::text END,
              'timesheet_id', flags.resolved_timesheet_id,
              'current_timesheet_id', flags.resolved_timesheet_id,
              'requested_timesheet_id', flags.resolved_timesheet_id,
              'expected_timesheet_id', flags.resolved_timesheet_id,
              'contract_week_id', flags.resolved_contract_week_id,
              'contract_id', flags.resolved_contract_id,
              'candidate_id', flags.resolved_candidate_id,
              'candidate_name', flags.candidate_name,
              'candidate_display_name', flags.candidate_display_name,
              'client_id', flags.resolved_client_id,
              'client_name', flags.client_name,
              'booking_id', flags.booking_id,
              'occupant_key_norm', flags.occupant_key_norm,
              'hospital_norm', flags.hospital_norm,
              'candidate_hint_text', flags.candidate_hint_text,
              'week_ending_date', flags.week_ending_date,
              'work_date', flags.work_date,
              'sheet_scope', flags.sheet_scope,
              'submission_mode', flags.submission_mode,
              'submission_mode_snapshot', flags.submission_mode_snapshot,
              'basis', flags.basis,
              'status', flags.timesheet_status,
              'contract_week_status', flags.contract_week_status,
              'route_type', flags.route_type,
              'route_display', flags.route_display,
              'route_family', flags.route_family,
              'route_subfamily', flags.route_subfamily,
              'underlying_channel_family', flags.underlying_channel_family,
              'summary_stage', flags.summary_stage,
              'tools_stage', flags.tools_stage,
              'processing_status', flags.processing_status,
              'processing_status_display', flags.processing_status_display,
              'authorised_at_utc', flags.authorised_at_utc,
              'authorised_at_server', flags.authorised_at_server,
              'processed_at_utc', flags.processed_at_utc,
              'is_authorised', flags.authorised_bool,
              'locked', flags.locked_bool,
              'has_timesheet', flags.resolved_timesheet_id IS NOT NULL
            )
            || JSONB_BUILD_OBJECT(
              'total_hours', flags.total_hours,
              'total_pay_ex_vat', flags.total_pay_ex_vat,
              'total_charge_ex_vat', flags.total_charge_ex_vat,
              'margin_ex_vat', flags.margin_ex_vat,
              'net_delta_ex_vat', flags.net_delta_ex_vat,
              'paid_at_utc', flags.paid_at_utc,
              'pay_icon_code', flags.pay_icon_code,
              'pay_status_code', flags.pay_status_code,
              'pay_paid_at_utc', flags.pay_paid_at_utc,
              'invoice_is_paid', flags.invoice_is_paid,
              'invoice_issue_stage', flags.invoice_issue_stage,
              'invoice_segment_stage', flags.invoice_segment_stage,
              'invoice_segments_total', flags.invoice_segments_total,
              'invoice_segments_locked', flags.invoice_segments_locked,
              'invoice_segments_unlocked', flags.invoice_segments_unlocked,
              'issue_codes', flags.issue_codes_json,
              'validation_status', flags.validation_status,
              'validation_summary', flags.validation_summary,
              'hr_crosscheck_status', flags.hr_crosscheck_status,
              'hr_crosscheck_issues', flags.hr_crosscheck_issues_json,
              'qr_status', flags.qr_status,
              'is_qr', flags.is_qr,
              'is_adjusted', flags.is_adjusted,
              'needs_attention', flags.needs_attention,
              'has_rate_issue', flags.has_rate_issue,
              'has_pay_channel_issue', flags.has_pay_channel_issue,
              'client_no_timesheet_required', flags.client_no_timesheet_required,
              'client_autoprocess_hr', flags.client_autoprocess_hr,
              'client_is_nhsp', flags.client_is_nhsp
            )
            || JSONB_BUILD_OBJECT(
              'has_any_evidence', flags.has_any_evidence,
              'attached_evidence_count', flags.attached_evidence_count,
              'evidence_count', flags.attached_evidence_count,
              'primary_artifact_storage_key', flags.primary_artifact_storage_key,
              'primary_artifact_display_name', flags.primary_artifact_display_name,
              'primary_artifact_preview_mode', flags.primary_artifact_preview_mode,
              'manual_pdf_r2_key', flags.manual_pdf_r2_key,
              'qr_r2_key', flags.qr_r2_key,
              'uploaded_pdf_r2_key', flags.uploaded_pdf_r2_key,
              'generated_pdf_at_utc', flags.generated_pdf_at_utc,
              'manual_pdf_rotation_degrees', flags.manual_pdf_rotation_degrees,
              'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
              'additional_seq', flags.contract_week_additional_seq,
              'period_type', CASE WHEN flags.resolved_contract_week_id IS NOT NULL THEN 'WEEKLY' ELSE 'DAILY' END,
              'suppress_standard_schedule_fallback', flags.keep_blank_additional_schedule_bool,
              'keep_additional_manual_adjustment_schedule_empty', flags.keep_blank_additional_schedule_bool,
              '__suppressStandardScheduleFallback', flags.keep_blank_additional_schedule_bool,
              '__keepAdditionalManualAdjustmentScheduleEmpty', flags.keep_blank_additional_schedule_bool
            )
            || CASE WHEN v_profile IN ('editor', 'active_row_visible') THEN JSONB_BUILD_OBJECT(
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'contract_week_totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
              'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
              'additional_units_json', COALESCE(flags.tsfin_additional_units_json, flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb)
            ) ELSE JSONB_BUILD_OBJECT() END
            || JSONB_BUILD_OBJECT(
              'action_flags', JSONB_BUILD_OBJECT(
                'can_save', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
                'can_process', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = TRUE),
                'can_unprocess', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = FALSE),
                'can_bulk_authorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.requires_authorisation_bool = TRUE AND flags.authorised_bool = FALSE AND flags.qr_unsigned_blocked_bool = FALSE),
                'can_bulk_unauthorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = TRUE),
                'can_edit_timesheet_data', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
                'can_manage_evidence', ((flags.resolved_timesheet_id IS NOT NULL OR (flags.resolved_contract_week_id IS NOT NULL AND flags.route_family = 'MANUAL_NON_QR')) AND flags.locked_bool = FALSE AND flags.route_family <> 'IMPORT_AUTHORITATIVE'),
                'can_add_additional_manual', (flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND COALESCE(flags.is_adjusted, FALSE) = FALSE),
                'review_only', (flags.locked_bool = TRUE OR flags.authorised_bool = TRUE OR flags.route_family <> 'MANUAL_NON_QR'),
                'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
                'additional_seq', flags.contract_week_additional_seq,
                'has_any_evidence', flags.has_any_evidence,
                'attached_evidence_count', flags.attached_evidence_count
              ),
              'row_patch', JSONB_BUILD_OBJECT(),
              'artifact_hints', JSONB_BUILD_OBJECT(
                'has_any_evidence', flags.has_any_evidence,
                'attached_evidence_count', flags.attached_evidence_count,
                'primary_artifact_storage_key', flags.primary_artifact_storage_key,
                'primary_artifact_display_name', flags.primary_artifact_display_name,
                'primary_artifact_preview_mode', flags.primary_artifact_preview_mode
              ),
              'evidence_badges', JSONB_BUILD_ARRAY(
                JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(flags.has_any_evidence, FALSE), 'has_evidence', COALESCE(flags.has_any_evidence, FALSE)),
                JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', FALSE, 'has_evidence', FALSE)
              )
            )
          ),
          'evidence', '[]'::jsonb
        )
        || JSONB_BUILD_OBJECT(
          'row_patch', JSONB_BUILD_OBJECT(),
          'action_flags', JSONB_BUILD_OBJECT(
            'can_save', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
            'can_process', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = TRUE),
            'can_unprocess', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = FALSE),
            'can_bulk_authorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.requires_authorisation_bool = TRUE AND flags.authorised_bool = FALSE AND flags.qr_unsigned_blocked_bool = FALSE),
            'can_bulk_unauthorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = TRUE),
            'can_edit_timesheet_data', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
            'can_manage_evidence', ((flags.resolved_timesheet_id IS NOT NULL OR (flags.resolved_contract_week_id IS NOT NULL AND flags.route_family = 'MANUAL_NON_QR')) AND flags.locked_bool = FALSE AND flags.route_family <> 'IMPORT_AUTHORITATIVE'),
            'can_add_additional_manual', (flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND COALESCE(flags.is_adjusted, FALSE) = FALSE),
            'review_only', (flags.locked_bool = TRUE OR flags.authorised_bool = TRUE OR flags.route_family <> 'MANUAL_NON_QR'),
            'has_any_evidence', flags.has_any_evidence,
            'attached_evidence_count', flags.attached_evidence_count
          ),
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
          'count_deltas', JSONB_BUILD_OBJECT()
        )
        || CASE WHEN v_profile IN ('editor', 'active_row_visible') THEN JSONB_BUILD_OBJECT(
          'details', JSONB_BUILD_OBJECT(
            'requested_timesheet_id', flags.resolved_timesheet_id,
            'current_timesheet_id', flags.resolved_timesheet_id,
            'expected_timesheet_id', flags.resolved_timesheet_id,
            'current_version', flags.timesheet_version,
            'was_stale', COALESCE(flags.tsfin_is_stale, FALSE),
            'booking_id', flags.booking_id,
            'timesheet', JSONB_BUILD_OBJECT(
              'timesheet_id', flags.resolved_timesheet_id,
              'booking_id', flags.booking_id,
              'occupant_key_norm', flags.occupant_key_norm,
              'hospital_norm', flags.hospital_norm,
              'worked_start_iso', flags.timesheet_worked_start_iso,
              'worked_end_iso', flags.timesheet_worked_end_iso,
              'break_start_iso', flags.timesheet_break_start_iso,
              'break_end_iso', flags.timesheet_break_end_iso,
              'break_minutes', flags.timesheet_break_minutes,
              'worked_minutes', flags.timesheet_worked_minutes,
              'week_ending_date', flags.week_ending_date,
              'auth_name', flags.timesheet_auth_name,
              'auth_job_title', flags.timesheet_auth_job_title,
              'authorised_at_server', flags.authorised_at_server,
              'reference_number', flags.timesheet_reference_number,
              'reference_set_at', flags.timesheet_reference_set_at,
              'status', flags.timesheet_status,
              'created_at', flags.timesheet_created_at,
              'updated_at', flags.timesheet_updated_at,
              'version', flags.timesheet_version,
              'is_current', flags.resolved_timesheet_id IS NOT NULL,
              'contract_id', flags.resolved_contract_id,
              'submission_mode', flags.submission_mode,
              'manual_pdf_r2_key', flags.manual_pdf_r2_key,
              'sheet_scope', flags.sheet_scope,
              'actual_schedule_json', COALESCE(flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb),
              'qr_status', flags.qr_status,
              'qr_r2_key', flags.qr_r2_key,
              'manual_pdf_rotation_degrees', flags.manual_pdf_rotation_degrees,
              'generated_pdf_at_utc', flags.generated_pdf_at_utc,
              'candidate_hint_text', flags.candidate_hint_text,
              'is_adjustment', flags.timesheet_is_adjustment,
              'parent_timesheet_id', flags.timesheet_parent_timesheet_id,
              'adjustment_origin', flags.timesheet_adjustment_origin
            ),
            'tsfin', JSONB_BUILD_OBJECT(
              'id', flags.tsfin_id,
              'timesheet_id', flags.resolved_timesheet_id,
              'timesheet_version', flags.tsfin_timesheet_version,
              'basis', flags.tsfin_basis,
              'is_current', flags.tsfin_is_current,
              'is_stale', flags.tsfin_is_stale,
              'stale_reason', flags.tsfin_stale_reason,
              'processing_status', flags.tsfin_processing_status,
              'total_hours', flags.total_hours,
              'total_pay_ex_vat', flags.tsfin_total_pay_ex_vat,
              'total_charge_ex_vat', flags.tsfin_total_charge_ex_vat,
              'margin_ex_vat', flags.tsfin_margin_ex_vat,
              'expenses_pay_ex_vat', flags.tsfin_expenses_pay_ex_vat,
              'expenses_charge_ex_vat', flags.tsfin_expenses_charge_ex_vat,
              'mileage_units', flags.tsfin_mileage_units,
              'mileage_pay_ex_vat', flags.tsfin_mileage_pay_ex_vat,
              'mileage_charge_ex_vat', flags.tsfin_mileage_charge_ex_vat,
              'travel_pay_ex_vat', flags.tsfin_travel_pay_ex_vat,
              'travel_charge_ex_vat', flags.tsfin_travel_charge_ex_vat,
              'accommodation_pay_ex_vat', flags.tsfin_accommodation_pay_ex_vat,
              'accommodation_charge_ex_vat', flags.tsfin_accommodation_charge_ex_vat,
              'other_pay_ex_vat', flags.tsfin_other_pay_ex_vat,
              'other_charge_ex_vat', flags.tsfin_other_charge_ex_vat,
              'additional_pay_ex_vat', flags.tsfin_additional_pay_ex_vat,
              'additional_charge_ex_vat', flags.tsfin_additional_charge_ex_vat,
              'additional_margin_ex_vat', flags.tsfin_additional_margin_ex_vat,
              'locked_by_invoice_id', flags.tsfin_locked_by_invoice_id,
              'locked_at_utc', flags.tsfin_locked_at_utc,
              'paid_at_utc', flags.tsfin_paid_at_utc,
              'processed_at_utc', flags.tsfin_processed_at_utc,
              'authorised_at_utc', flags.tsfin_authorised_at_utc,
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
              'additional_units_json', COALESCE(flags.tsfin_additional_units_json, '{}'::jsonb),
              'has_rate_issue', flags.has_rate_issue,
              'has_pay_channel_issue', flags.has_pay_channel_issue,
              'hr_crosscheck_status', flags.hr_crosscheck_status,
              'hr_crosscheck_issues', flags.hr_crosscheck_issues_json
            ),
            'validations', COALESCE(flags.validations_json, '[]'::jsonb),
            'validation_summary', JSONB_BUILD_OBJECT(
              'status', flags.validation_status,
              'pre_validated', COALESCE((flags.latest_validation_json->>'pre_validated')::boolean, FALSE),
              'latest', flags.latest_validation_json
            ),
            'contract_week_id', flags.resolved_contract_week_id,
            'contract_week', JSONB_BUILD_OBJECT(
              'id', flags.resolved_contract_week_id,
              'contract_id', flags.resolved_contract_id,
              'week_ending_date', flags.week_ending_date,
              'additional_seq', flags.contract_week_additional_seq,
              'status', flags.contract_week_status,
              'submission_mode_snapshot', flags.submission_mode_snapshot,
              'timesheet_id', flags.resolved_timesheet_id,
              'uploaded_pdf_r2_key', flags.uploaded_pdf_r2_key,
              'day_entries_json', COALESCE(flags.contract_week_day_entries_json, '[]'::jsonb),
              'totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
              'created_at', flags.contract_week_created_at,
              'updated_at', flags.contract_week_updated_at,
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'is_adjustment', flags.contract_week_is_adjustment,
              'enforce_day_partition', flags.contract_week_enforce_day_partition,
              'allowed_days_mask', flags.contract_week_allowed_days_mask,
              'split_boundary_date', flags.contract_week_split_boundary_date,
              'worker_note', flags.contract_week_worker_note,
              'split_group_key', flags.contract_week_split_group_key
            ),
            'related', JSONB_BUILD_OBJECT(
              'contract', JSONB_BUILD_OBJECT(
                'id', flags.resolved_contract_id,
                'candidate_id', flags.resolved_candidate_id,
                'client_id', flags.resolved_client_id,
                'role', flags.contract_role,
                'band', flags.contract_band,
                'display_site', flags.contract_display_site,
                'ward_hint', flags.contract_ward_hint,
                'default_submission_mode', flags.contract_default_submission_mode,
                'std_schedule_json', COALESCE(flags.contract_std_schedule_json, '[]'::jsonb),
                'additional_rates_json', COALESCE(flags.contract_additional_rates_json, '{}'::jsonb),
                'weekly_timesheet_source', flags.contract_weekly_timesheet_source,
                'no_timesheet_required', flags.contract_no_timesheet_required,
                'autoprocess_hr', flags.contract_autoprocess_hr,
                'requires_hr', flags.contract_requires_hr,
                'hr_attach_to_invoice', flags.contract_hr_attach_to_invoice,
                'ts_attach_to_invoice', flags.contract_ts_attach_to_invoice,
                'is_nhsp', flags.contract_is_nhsp
              ),
              'candidate', JSONB_BUILD_OBJECT(
                'id', flags.resolved_candidate_id,
                'tms_ref', flags.candidate_tms_ref,
                'first_name', flags.candidate_first_name,
                'last_name', flags.candidate_last_name,
                'display_name', flags.candidate_display_name_raw,
                'email', flags.candidate_email,
                'phone', flags.candidate_phone,
                'key_norm', flags.candidate_key_norm,
                'band', flags.candidate_band
              ),
              'client', JSONB_BUILD_OBJECT(
                'id', flags.resolved_client_id,
                'cli_ref', flags.client_cli_ref,
                'name', flags.client_name_raw,
                'vat_chargeable', flags.client_vat_chargeable,
                'ts_queries_email', flags.client_ts_queries_email
              )
            ),
            'evidence', '[]'::jsonb,
            'policy', JSONB_BUILD_OBJECT(
              'weekly_mode', flags.contract_weekly_timesheet_source,
              'requires_hr', flags.contract_requires_hr,
              'autoprocess_hr', flags.contract_autoprocess_hr,
              'no_timesheet_required', flags.contract_no_timesheet_required,
              'is_nhsp', flags.contract_is_nhsp
            ),
            'effective', JSONB_BUILD_OBJECT(
              'route_type', flags.route_type,
              'route_display', flags.route_display,
              'route_family', flags.route_family,
              'route_subfamily', flags.route_subfamily,
              'underlying_channel_family', flags.underlying_channel_family,
              'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
              'additional_seq', flags.contract_week_additional_seq,
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'suppress_standard_schedule_fallback', flags.keep_blank_additional_schedule_bool,
              'keep_additional_manual_adjustment_schedule_empty', flags.keep_blank_additional_schedule_bool,
              'summary_stage', flags.summary_stage,
              'client_requires_hr', flags.contract_requires_hr,
              'client_autoprocess_hr', flags.contract_autoprocess_hr,
              'client_no_timesheet_required', flags.contract_no_timesheet_required,
              'client_is_nhsp', flags.contract_is_nhsp,
              'contract_id', flags.resolved_contract_id,
              'issue_codes', flags.issue_codes_json
            )
          ),
          'timesheet', JSONB_BUILD_OBJECT(
            'timesheet_id', flags.resolved_timesheet_id,
            'booking_id', flags.booking_id,
            'worked_start_iso', flags.timesheet_worked_start_iso,
            'worked_end_iso', flags.timesheet_worked_end_iso,
            'break_start_iso', flags.timesheet_break_start_iso,
            'break_end_iso', flags.timesheet_break_end_iso,
            'break_minutes', flags.timesheet_break_minutes,
            'worked_minutes', flags.timesheet_worked_minutes,
            'week_ending_date', flags.week_ending_date,
            'actual_schedule_json', COALESCE(flags.timesheet_actual_schedule_json, '[]'::jsonb),
            'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
            'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb)
          ),
          'tsfin', JSONB_BUILD_OBJECT(
            'id', flags.tsfin_id,
            'timesheet_id', flags.resolved_timesheet_id,
            'basis', flags.tsfin_basis,
            'processing_status', flags.tsfin_processing_status,
            'total_hours', flags.total_hours,
            'total_pay_ex_vat', flags.tsfin_total_pay_ex_vat,
            'total_charge_ex_vat', flags.tsfin_total_charge_ex_vat,
            'margin_ex_vat', flags.tsfin_margin_ex_vat,
            'expenses_pay_ex_vat', flags.tsfin_expenses_pay_ex_vat,
            'expenses_charge_ex_vat', flags.tsfin_expenses_charge_ex_vat,
            'mileage_units', flags.tsfin_mileage_units,
            'mileage_pay_ex_vat', flags.tsfin_mileage_pay_ex_vat,
            'mileage_charge_ex_vat', flags.tsfin_mileage_charge_ex_vat,
            'travel_pay_ex_vat', flags.tsfin_travel_pay_ex_vat,
            'travel_charge_ex_vat', flags.tsfin_travel_charge_ex_vat,
            'accommodation_pay_ex_vat', flags.tsfin_accommodation_pay_ex_vat,
            'accommodation_charge_ex_vat', flags.tsfin_accommodation_charge_ex_vat,
            'other_pay_ex_vat', flags.tsfin_other_pay_ex_vat,
            'other_charge_ex_vat', flags.tsfin_other_charge_ex_vat,
            'additional_pay_ex_vat', flags.tsfin_additional_pay_ex_vat,
            'additional_charge_ex_vat', flags.tsfin_additional_charge_ex_vat,
            'additional_margin_ex_vat', flags.tsfin_additional_margin_ex_vat,
            'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
            'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
            'additional_units_json', COALESCE(flags.tsfin_additional_units_json, '{}'::jsonb),
            'locked_by_invoice_id', flags.tsfin_locked_by_invoice_id,
            'paid_at_utc', flags.tsfin_paid_at_utc,
            'processed_at_utc', flags.tsfin_processed_at_utc,
            'authorised_at_utc', flags.tsfin_authorised_at_utc
          ),
          'contract_week', JSONB_BUILD_OBJECT(
            'id', flags.resolved_contract_week_id,
            'contract_id', flags.resolved_contract_id,
            'week_ending_date', flags.week_ending_date,
            'day_entries_json', COALESCE(flags.contract_week_day_entries_json, '[]'::jsonb),
            'totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
            'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
            'additional_seq', flags.contract_week_additional_seq,
            'is_adjustment', flags.contract_week_is_adjustment
          ),
          'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count),
          'left_pane', JSONB_BUILD_OBJECT('evidence', '[]'::jsonb, 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count))
        ) ELSE JSONB_BUILD_OBJECT(
          'details', JSONB_BUILD_OBJECT(),
          'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count),
          'left_pane', JSONB_BUILD_OBJECT('evidence', '[]'::jsonb, 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count))
        ) END AS payload_json
      FROM flags
      WHERE flags.row_found = TRUE
    )
    SELECT row_payload.payload_json || JSONB_BUILD_OBJECT(
        'data_row', row_payload.payload_json->'row',
        'row_patch', COALESCE(row_payload.payload_json->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_key', row_payload.payload_json->>'row_key')
      )
      INTO v_out
    FROM row_payload;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', v_profile,
        'profile', v_profile,
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk process row context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;


    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      v_canonical_row_json := NULL;
      v_canonical_row_signature := NULL;
      v_canonical_row_key := NULLIF(BTRIM(COALESCE(v_out->>'row_key', v_out#>>'{row,row_key}', v_out#>>'{data_row,row_key}', v_out#>>'{row_patch,row_key}', '')), '');
      v_canonical_timesheet_id := NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', v_out->>'current_timesheet_id', v_out#>>'{row,timesheet_id}', v_out#>>'{row,current_timesheet_id}', v_out#>>'{data_row,timesheet_id}', v_out#>>'{data_row,current_timesheet_id}', v_out#>>'{row_patch,timesheet_id}', v_out#>>'{row_patch,current_timesheet_id}', '')), '');
      v_canonical_contract_week_id := CASE
        WHEN v_canonical_timesheet_id IS NULL THEN NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', v_out#>>'{row,contract_week_id}', v_out#>>'{data_row,contract_week_id}', v_out#>>'{row_patch,contract_week_id}', '')), '')
        ELSE NULL
      END;

      IF v_canonical_row_key IS NOT NULL OR v_canonical_timesheet_id IS NOT NULL OR v_canonical_contract_week_id IS NOT NULL THEN
        SELECT canonical_result.row_json
        INTO v_canonical_row_json
        FROM public.bulk_timesheet_row_patch_v1(
          JSONB_STRIP_NULLS(
            JSONB_BUILD_OBJECT(
              'dataset_mode', 'process',
              'projection', 'active_row_header',
              'profile', COALESCE(NULLIF(BTRIM(v_profile), ''), 'status_header'),
              'row_key', v_canonical_row_key,
              'timesheet_id', v_canonical_timesheet_id,
              'current_timesheet_id', v_canonical_timesheet_id,
              'requested_timesheet_id', v_canonical_timesheet_id,
              'expected_timesheet_id', v_canonical_timesheet_id,
              'contract_week_id', v_canonical_contract_week_id
            )
          )
        ) AS canonical_result(row_json)
        WHERE canonical_result.row_json IS NOT NULL
        ORDER BY canonical_result.row_json->>'row_key'
        LIMIT 1;

        v_canonical_row_signature := NULLIF(BTRIM(COALESCE(v_canonical_row_json->>'row_signature', '')), '');
        IF v_canonical_row_signature IS NOT NULL THEN
          v_out := v_out || JSONB_BUILD_OBJECT(
            'row_signature', v_canonical_row_signature,
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature)
          );
        END IF;
      END IF;
    END IF;

    RETURN private._candidate_office_context_overlay_v1(public.bulk_process_retention_contract_patch_v1(v_out));
  END IF;

  IF v_profile = 'evidence' THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (cw_ids.input_contract_week_id IS NOT NULL AND cw0.id = cw_ids.input_contract_week_id)
         OR (cw_ids.input_timesheet_id IS NOT NULL AND cw0.timesheet_id = cw_ids.input_timesheet_id)
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM (
        SELECT input_ids.*
        FROM input_ids
        WHERE input_ids.input_timesheet_id IS NULL
          AND input_ids.input_contract_week_id IS NULL
      ) AS summary_ids
      CROSS JOIN LATERAL public.timesheet_summary_lightweight_rows_v1(
        v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)
      ) AS summary_source
      WHERE (
          summary_ids.input_timesheet_id IS NULL
          OR summary_source.timesheet_id = summary_ids.input_timesheet_id
        )
        AND (
          summary_ids.input_contract_week_id IS NULL
          OR summary_source.contract_week_id = summary_ids.input_contract_week_id
        )
      ORDER BY
        CASE WHEN summary_ids.input_timesheet_id IS NOT NULL AND summary_source.timesheet_id = summary_ids.input_timesheet_id THEN 0 ELSE 1 END,
        CASE WHEN summary_ids.input_contract_week_id IS NOT NULL AND summary_source.contract_week_id = summary_ids.input_contract_week_id THEN 0 ELSE 1 END,
        summary_source.timesheet_id NULLS LAST,
        summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    resolved AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id) AS resolved_contract_id,
        summary_row.candidate_id AS candidate_id,
        summary_row.client_id AS client_id,
        summary_row.candidate_name AS candidate_name,
        summary_row.candidate_display_name AS candidate_display_name,
        summary_row.client_name AS client_name,
        COALESCE(summary_row.week_ending_date, timesheet_row.week_ending_date, contract_week_row.week_ending_date) AS week_ending_date,
        COALESCE(summary_row.route_family, CASE WHEN COALESCE(timesheet_row.qr_status::text, '') <> '' THEN 'QR' WHEN UPPER(COALESCE(timesheet_row.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'ELECTRONIC' THEN 'ELECTRONIC' ELSE 'MANUAL_NON_QR' END) AS route_family,
        COALESCE(summary_row.has_any_evidence, FALSE) AS summary_has_any_evidence,
        COALESCE(summary_row.attached_evidence_count, 0) AS summary_attached_evidence_count,
        summary_row.primary_artifact_storage_key AS summary_primary_artifact_storage_key,
        summary_row.primary_artifact_display_name AS summary_primary_artifact_display_name,
        summary_row.primary_artifact_preview_mode AS summary_primary_artifact_preview_mode,
        timesheet_row.manual_pdf_r2_key AS manual_pdf_r2_key,
        timesheet_row.qr_r2_key AS qr_r2_key,
        timesheet_row.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
        timesheet_row.updated_at AS timesheet_updated_at,
        CASE
          WHEN COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) IS NULL THEN contract_week_row.uploaded_pdf_r2_key
          ELSE NULL::text
        END AS uploaded_pdf_r2_key,
        contract_week_row.updated_at AS contract_week_updated_at
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
    ),
    evidence_items AS (
      SELECT
        0::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:manual_pdf:' || resolved.resolved_timesheet_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Timesheet PDF',
          'filename', 'Timesheet PDF',
          'storage_key', resolved.manual_pdf_r2_key,
          'r2_key', resolved.manual_pdf_r2_key,
          'file_key', resolved.manual_pdf_r2_key,
          'download_storage_key', resolved.manual_pdf_r2_key,
          'original_filename', 'Timesheet PDF',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.timesheet_updated_at,
          'rotation_degrees', COALESCE(resolved.manual_pdf_rotation_degrees, 0),
          'last_rotation_deg', COALESCE(resolved.manual_pdf_rotation_degrees, 0),
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.manual_pdf_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_manual_pdf_duplicate
          WHERE te_manual_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND NULLIF(regexp_replace(BTRIM(COALESCE(te_manual_pdf_duplicate.storage_key, '')), '^/+', ''), '') =
                NULLIF(regexp_replace(BTRIM(COALESCE(resolved.manual_pdf_r2_key, '')), '^/+', ''), '')
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.manual_timesheet_queue AS mq_manual_pdf_returned
          WHERE NULLIF(regexp_replace(BTRIM(COALESCE(mq_manual_pdf_returned.r2_key, '')), '^/+', ''), '') =
                NULLIF(regexp_replace(BTRIM(COALESCE(resolved.manual_pdf_r2_key, '')), '^/+', ''), '')
            AND UPPER(COALESCE(mq_manual_pdf_returned.status, '')) = 'QUEUED'
            AND (
              mq_manual_pdf_returned.timesheet_id = resolved.resolved_timesheet_id
              OR NULLIF(BTRIM(COALESCE(mq_manual_pdf_returned.meta_json ->> 'returned_from_timesheet_id', '')), '') = resolved.resolved_timesheet_id::text
              OR NULLIF(BTRIM(COALESCE(mq_manual_pdf_returned.meta_json ->> 'dematerialised_from_timesheet_id', '')), '') = resolved.resolved_timesheet_id::text
              OR NULLIF(BTRIM(COALESCE(mq_manual_pdf_returned.meta_json ->> 'attached_to_timesheet_id', '')), '') = resolved.resolved_timesheet_id::text
              OR NULLIF(BTRIM(COALESCE(mq_manual_pdf_returned.meta_json ->> 'materialised_to_timesheet_id', '')), '') = resolved.resolved_timesheet_id::text
            )
        )
      UNION ALL
      SELECT
        1::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:qr_pdf:' || resolved.resolved_timesheet_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Signed QR Timesheet',
          'filename', 'Signed QR Timesheet',
          'storage_key', resolved.qr_r2_key,
          'r2_key', resolved.qr_r2_key,
          'file_key', resolved.qr_r2_key,
          'download_storage_key', resolved.qr_r2_key,
          'original_filename', 'Signed QR Timesheet',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.timesheet_updated_at,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.qr_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_qr_pdf_duplicate
          WHERE te_qr_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_qr_pdf_duplicate.storage_key = resolved.qr_r2_key
        )
      UNION ALL
      SELECT
        2::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:contract_week_pdf:' || resolved.resolved_contract_week_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Uploaded timesheet PDF',
          'filename', 'Uploaded timesheet PDF',
          'storage_key', resolved.uploaded_pdf_r2_key,
          'r2_key', resolved.uploaded_pdf_r2_key,
          'file_key', resolved.uploaded_pdf_r2_key,
          'download_storage_key', resolved.uploaded_pdf_r2_key,
          'original_filename', 'Uploaded timesheet PDF',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.contract_week_updated_at,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_contract_week_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.uploaded_pdf_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_contract_week_pdf_duplicate
          WHERE te_contract_week_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_contract_week_pdf_duplicate.storage_key = resolved.uploaded_pdf_r2_key
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.manual_timesheet_queue AS mq_contract_week_pdf_returned
          WHERE NULLIF(regexp_replace(BTRIM(COALESCE(mq_contract_week_pdf_returned.r2_key, '')), '^/+', ''), '') =
                NULLIF(regexp_replace(BTRIM(COALESCE(resolved.uploaded_pdf_r2_key, '')), '^/+', ''), '')
            AND UPPER(COALESCE(mq_contract_week_pdf_returned.status, '')) = 'QUEUED'
            AND (
              (resolved.resolved_timesheet_id IS NOT NULL AND mq_contract_week_pdf_returned.timesheet_id = resolved.resolved_timesheet_id)
              OR (resolved.resolved_timesheet_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_contract_week_pdf_returned.meta_json ->> 'returned_from_timesheet_id', '')), '') = resolved.resolved_timesheet_id::text)
              OR (resolved.resolved_timesheet_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_contract_week_pdf_returned.meta_json ->> 'dematerialised_from_timesheet_id', '')), '') = resolved.resolved_timesheet_id::text)
              OR (resolved.resolved_timesheet_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_contract_week_pdf_returned.meta_json ->> 'attached_to_timesheet_id', '')), '') = resolved.resolved_timesheet_id::text)
              OR (resolved.resolved_timesheet_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_contract_week_pdf_returned.meta_json ->> 'materialised_to_timesheet_id', '')), '') = resolved.resolved_timesheet_id::text)
              OR (resolved.resolved_timesheet_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_contract_week_pdf_returned.meta_json #>> '{return_queue_previous_meta,materialised_to_timesheet_id}', '')), '') = resolved.resolved_timesheet_id::text)
              OR (resolved.resolved_contract_week_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_contract_week_pdf_returned.meta_json ->> 'contract_week_id', '')), '') = resolved.resolved_contract_week_id::text)
              OR (resolved.resolved_contract_week_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_contract_week_pdf_returned.meta_json #>> '{return_queue_previous_meta,contract_week_id}', '')), '') = resolved.resolved_contract_week_id::text)
              OR (resolved.resolved_contract_week_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_contract_week_pdf_returned.meta_json #>> '{return_queue_previous_meta,active_identity}', '')), '') = 'contract_week:' || resolved.resolved_contract_week_id::text)
              OR (resolved.resolved_contract_week_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_contract_week_pdf_returned.meta_json #>> '{return_queue_previous_meta,preview_identity}', '')), '') = 'contract_week:' || resolved.resolved_contract_week_id::text)
            )
        )
      UNION ALL
      SELECT
        10::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', te0.id,
          'evidence_id', te0.id,
          'queue_id', NULL::uuid,
          'timesheet_id', te0.timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', UPPER(COALESCE(NULLIF(BTRIM(te0.kind), ''), 'TIMESHEET')),
          'display_name', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'storage_key', te0.storage_key,
          'r2_key', te0.storage_key,
          'file_key', te0.storage_key,
          'download_storage_key', te0.storage_key,
          'original_filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'mime_type', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
            ELSE NULL::text
          END,
          'content_type', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
            ELSE NULL::text
          END,
          'uploaded_at_utc', te0.created_at,
          'created_at', te0.created_at,
          'created_by', te0.created_by,
          'rotation', 0,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', FALSE,
          'is_view_only', FALSE,
          'can_delete', TRUE,
          'can_reclassify', TRUE,
          'can_edit_kind', TRUE,
          'can_edit_type', TRUE,
          'can_return_to_queue', TRUE,
          'preview_mode', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'PDF'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.(png|jpe?g|gif|webp|bmp|svg|tif|tiff|heic|heif|avif)($|[?#])' THEN 'IMAGE'
            ELSE 'FILE'
          END,
          'source_label', 'Attached',
          'source_badge', 'Attached'
        ) AS item_json
      FROM public.timesheet_evidence AS te0
      CROSS JOIN resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND te0.timesheet_id = resolved.resolved_timesheet_id
      UNION ALL
      SELECT
        20::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', mq0.id,
          'evidence_id', NULL::uuid,
          'queue_id', mq0.id,
          'timesheet_id', NULL::uuid,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
          'staged_kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
          'display_name', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
          'filename', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
          'original_filename', mq0.original_filename,
          'storage_key', mq0.r2_key,
          'r2_key', mq0.r2_key,
          'file_key', mq0.r2_key,
          'download_storage_key', mq0.r2_key,
          'mime_type', mq0.mime_type,
          'content_type', mq0.mime_type,
          'content_hash', mq0.content_hash,
          'uploaded_at_utc', mq0.uploaded_at_utc,
          'staged_at_utc', COALESCE(mq0.meta_json->>'staged_at_utc', mq0.uploaded_at_utc::text),
          'staged_by_user_id', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'staged_by_user_id'), ''), mq0.uploaded_by_user_id::text),
          'rotation_degrees', COALESCE(mq0.last_rotation_deg::integer, 0),
          'last_rotation_deg', COALESCE(mq0.last_rotation_deg::integer, 0),
          'page_count', CASE WHEN COALESCE(mq0.meta_json->>'page_count', '') ~ '^[0-9]+$' THEN (mq0.meta_json->>'page_count')::integer ELSE NULL::integer END,
          'pages', '[]'::jsonb,
          'status', mq0.status,
          'system', FALSE,
          'is_view_only', FALSE,
          'can_delete', TRUE,
          'can_reclassify', TRUE,
          'can_edit_kind', TRUE,
          'can_edit_type', TRUE,
          'can_return_to_queue', TRUE,
          'is_staged_context', TRUE,
          'preview_mode', CASE
            WHEN LOWER(COALESCE(mq0.mime_type, '')) LIKE 'image/%' THEN 'IMAGE'
            WHEN LOWER(COALESCE(mq0.mime_type, '')) = 'application/pdf' THEN 'PDF'
            ELSE 'FILE'
          END,
          'source_label', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged'),
          'source_badge', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged')
        ) AS item_json
      FROM public.manual_timesheet_queue AS mq0
      CROSS JOIN resolved
      WHERE resolved.resolved_timesheet_id IS NULL
        AND resolved.resolved_contract_week_id IS NOT NULL
        AND UPPER(COALESCE(mq0.status, '')) = 'STAGED'
        AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') = resolved.resolved_contract_week_id::text
    ),
    evidence_ranked AS (
      SELECT evidence_items.sort_order, evidence_items.item_json
      FROM evidence_items
      WHERE NULLIF(BTRIM(COALESCE(evidence_items.item_json->>'storage_key', evidence_items.item_json->>'r2_key', '')), '') IS NOT NULL
    ),
    evidence_payload AS (
      SELECT
        COALESCE(JSONB_AGG(evidence_ranked.item_json ORDER BY evidence_ranked.sort_order ASC, evidence_ranked.item_json->>'id' ASC), '[]'::jsonb) AS evidence_json,
        COUNT(*)::integer AS evidence_count,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'TIMESHEET') AS has_timesheet,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'MILEAGE') AS has_mileage,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'TRAVEL') AS has_travel,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'ACCOMMODATION') AS has_accommodation,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'OTHER') AS has_other,
        (SELECT er1.item_json FROM evidence_ranked AS er1 ORDER BY er1.sort_order ASC, er1.item_json->>'id' ASC LIMIT 1) AS primary_evidence_json
      FROM evidence_ranked
    ),
    final_payload AS (
      SELECT
        JSONB_BUILD_OBJECT(
          'ok', TRUE,
          'context_kind', 'bulk_process_row_context',
          'context_profile', 'evidence',
          'profile', 'evidence',
          'context_type', CASE WHEN 'bulk_process_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
          'slim_context', TRUE,
          'header_loaded', FALSE,
          'header_only', FALSE,
          'editor_loaded', FALSE,
          'evidence_loaded', TRUE,
          'compare_loaded', FALSE,
          'full_loaded', FALSE,
          'schedule_pending', TRUE,
          'schedule_authoritative', FALSE,
          'loaded_layers', JSONB_BUILD_ARRAY('evidence'),
          'soft_failure', FALSE,
          'context_degraded', FALSE,
          'degraded_reason', NULL::text,
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'row_signature', MD5(CONCAT_WS('|', COALESCE(resolved.resolved_timesheet_id::text, ''), COALESCE(resolved.resolved_contract_week_id::text, ''), COALESCE(resolved.timesheet_updated_at::text, ''), COALESCE(resolved.contract_week_updated_at::text, ''), 'evidence')),
          'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'attached_evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'attachedRows', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'primary_artifact', COALESCE(evidence_payload.primary_evidence_json, JSONB_BUILD_OBJECT()),
          'preview_storage_key', NULLIF(BTRIM(COALESCE(evidence_payload.primary_evidence_json->>'storage_key', '')), ''),
          'primary_artifact_storage_key', NULLIF(BTRIM(COALESCE(evidence_payload.primary_evidence_json->>'storage_key', '')), ''),
          'primary_artifact_preview_mode', NULLIF(BTRIM(COALESCE(evidence_payload.primary_evidence_json->>'preview_mode', '')), ''),
          'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
          'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
          'evidence_count', COALESCE(evidence_payload.evidence_count, 0),
          'evidence_badges', JSONB_BUILD_ARRAY(
            JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
          ),
          'evidence_meta', JSONB_BUILD_OBJECT(
            'evidence_loaded', TRUE,
            'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
            'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
            'evidence_badges', JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
            )
          ),
          'row', JSONB_BUILD_OBJECT(
            'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
            'timesheet_id', resolved.resolved_timesheet_id,
            'current_timesheet_id', resolved.resolved_timesheet_id,
            'requested_timesheet_id', resolved.resolved_timesheet_id,
            'expected_timesheet_id', resolved.resolved_timesheet_id,
            'contract_week_id', resolved.resolved_contract_week_id,
            'contract_id', resolved.resolved_contract_id,
            'candidate_id', resolved.candidate_id,
            'candidate_name', resolved.candidate_name,
            'candidate_display_name', resolved.candidate_display_name,
            'client_id', resolved.client_id,
            'client_name', resolved.client_name,
            'week_ending_date', resolved.week_ending_date,
            'route_family', resolved.route_family,
            'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
            'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
            'primary_artifact', COALESCE(evidence_payload.primary_evidence_json, JSONB_BUILD_OBJECT()),
            'primary_artifact_storage_key', NULLIF(BTRIM(COALESCE(evidence_payload.primary_evidence_json->>'storage_key', '')), ''),
            'primary_artifact_preview_mode', NULLIF(BTRIM(COALESCE(evidence_payload.primary_evidence_json->>'preview_mode', '')), ''),
            'evidence_badges', JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
            )
          ),
          'row_patch', JSONB_BUILD_OBJECT(),
          'action_flags', JSONB_BUILD_OBJECT('has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0)),
          'details', JSONB_BUILD_OBJECT('evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb), 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE, 'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0))),
          'left_pane', JSONB_BUILD_OBJECT('evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb), 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE, 'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0))),
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
          'count_deltas', JSONB_BUILD_OBJECT(),
          'filters', v_filters
        ) AS payload_json
      FROM resolved
      LEFT JOIN evidence_payload ON TRUE
      WHERE resolved.resolved_timesheet_id IS NOT NULL OR resolved.resolved_contract_week_id IS NOT NULL
    )
    SELECT final_payload.payload_json || JSONB_BUILD_OBJECT('data_row', final_payload.payload_json->'row')
      INTO v_out
    FROM final_payload;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', 'evidence',
        'profile', 'evidence',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk process evidence context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;


    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      v_canonical_row_json := NULL;
      v_canonical_row_signature := NULL;
      v_canonical_row_key := NULLIF(BTRIM(COALESCE(v_out->>'row_key', v_out#>>'{row,row_key}', v_out#>>'{data_row,row_key}', v_out#>>'{row_patch,row_key}', '')), '');
      v_canonical_timesheet_id := NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', v_out->>'current_timesheet_id', v_out#>>'{row,timesheet_id}', v_out#>>'{row,current_timesheet_id}', v_out#>>'{data_row,timesheet_id}', v_out#>>'{data_row,current_timesheet_id}', v_out#>>'{row_patch,timesheet_id}', v_out#>>'{row_patch,current_timesheet_id}', '')), '');
      v_canonical_contract_week_id := CASE
        WHEN v_canonical_timesheet_id IS NULL THEN NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', v_out#>>'{row,contract_week_id}', v_out#>>'{data_row,contract_week_id}', v_out#>>'{row_patch,contract_week_id}', '')), '')
        ELSE NULL
      END;

      IF v_canonical_row_key IS NOT NULL OR v_canonical_timesheet_id IS NOT NULL OR v_canonical_contract_week_id IS NOT NULL THEN
        SELECT canonical_result.row_json
        INTO v_canonical_row_json
        FROM public.bulk_timesheet_row_patch_v1(
          JSONB_STRIP_NULLS(
            JSONB_BUILD_OBJECT(
              'dataset_mode', 'process',
              'projection', 'active_row_header',
              'profile', COALESCE(NULLIF(BTRIM(v_profile), ''), 'status_header'),
              'row_key', v_canonical_row_key,
              'timesheet_id', v_canonical_timesheet_id,
              'current_timesheet_id', v_canonical_timesheet_id,
              'requested_timesheet_id', v_canonical_timesheet_id,
              'expected_timesheet_id', v_canonical_timesheet_id,
              'contract_week_id', v_canonical_contract_week_id
            )
          )
        ) AS canonical_result(row_json)
        WHERE canonical_result.row_json IS NOT NULL
        ORDER BY canonical_result.row_json->>'row_key'
        LIMIT 1;

        v_canonical_row_signature := NULLIF(BTRIM(COALESCE(v_canonical_row_json->>'row_signature', '')), '');
        IF v_canonical_row_signature IS NOT NULL THEN
          v_out := v_out || JSONB_BUILD_OBJECT(
            'row_signature', v_canonical_row_signature,
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature)
          );
        END IF;
      END IF;
    END IF;

    RETURN private._candidate_office_context_overlay_v1(public.bulk_process_retention_contract_patch_v1(v_out));
  END IF;

  IF v_profile = 'compare_import' THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (cw_ids.input_contract_week_id IS NOT NULL AND cw0.id = cw_ids.input_contract_week_id)
         OR (cw_ids.input_timesheet_id IS NOT NULL AND cw0.timesheet_id = cw_ids.input_timesheet_id)
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    tsfin_row AS (
      SELECT tf0.*
      FROM public.timesheets_financials AS tf0
      LEFT JOIN timesheet_row AS ts_for_tf ON TRUE
      WHERE tf0.is_current = TRUE
        AND ts_for_tf.timesheet_id IS NOT NULL
        AND tf0.timesheet_id = ts_for_tf.timesheet_id
      ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      LIMIT 1
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM public.timesheet_summary_lightweight_rows_v1(v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)) AS summary_source
      CROSS JOIN input_ids AS summary_ids
      WHERE (summary_ids.input_timesheet_id IS NULL OR summary_source.timesheet_id = summary_ids.input_timesheet_id)
        AND (summary_ids.input_contract_week_id IS NULL OR summary_source.contract_week_id = summary_ids.input_contract_week_id)
      ORDER BY summary_source.timesheet_id NULLS LAST, summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    resolved AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id) AS resolved_contract_id,
        summary_row.candidate_id AS candidate_id,
        summary_row.client_id AS client_id,
        summary_row.candidate_name AS candidate_name,
        summary_row.client_name AS client_name,
        summary_row.week_ending_date AS week_ending_date,
        summary_row.route_family AS route_family,
        tsfin_row.external_source_rows_json AS external_source_rows_json,
        tsfin_row.updated_at AS tsfin_updated_at,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.updated_at AS contract_week_updated_at
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
      LEFT JOIN tsfin_row ON TRUE
    )
    SELECT JSONB_BUILD_OBJECT(
        'ok', TRUE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', 'compare_import',
        'profile', 'compare_import',
        'context_type', CASE WHEN 'bulk_process_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
        'slim_context', TRUE,
        'header_loaded', TRUE,
        'header_only', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', TRUE,
        'full_loaded', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', JSONB_BUILD_ARRAY('header', 'compare_import'),
        'soft_failure', FALSE,
        'context_degraded', FALSE,
        'degraded_reason', NULL::text,
        'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
        'timesheet_id', resolved.resolved_timesheet_id,
        'current_timesheet_id', resolved.resolved_timesheet_id,
        'requested_timesheet_id', resolved.resolved_timesheet_id,
        'expected_timesheet_id', resolved.resolved_timesheet_id,
        'contract_week_id', resolved.resolved_contract_week_id,
        'row_signature', MD5(CONCAT_WS('|', COALESCE(resolved.resolved_timesheet_id::text, ''), COALESCE(resolved.resolved_contract_week_id::text, ''), COALESCE(resolved.timesheet_updated_at::text, ''), COALESCE(resolved.contract_week_updated_at::text, ''), COALESCE(resolved.tsfin_updated_at::text, ''), 'compare_import')),
        'row', JSONB_BUILD_OBJECT(
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'contract_id', resolved.resolved_contract_id,
          'candidate_id', resolved.candidate_id,
          'candidate_name', resolved.candidate_name,
          'client_id', resolved.client_id,
          'client_name', resolved.client_name,
          'week_ending_date', resolved.week_ending_date,
          'route_family', resolved.route_family
        ),
        'data_row', JSONB_BUILD_OBJECT(
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'contract_id', resolved.resolved_contract_id,
          'candidate_id', resolved.candidate_id,
          'candidate_name', resolved.candidate_name,
          'client_id', resolved.client_id,
          'client_name', resolved.client_name,
          'week_ending_date', resolved.week_ending_date,
          'route_family', resolved.route_family
        ),
        'row_patch', JSONB_BUILD_OBJECT(),
        'action_flags', JSONB_BUILD_OBJECT(),
        'compare', JSONB_BUILD_OBJECT(
          'source_rows', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'external_source_rows_json', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'include_import_source_rows', v_include_import_source_rows
        ),
        'details', JSONB_BUILD_OBJECT(
          'source_rows', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'external_source_rows_json', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'evidence', '[]'::jsonb
        ),
        'left_pane', JSONB_BUILD_OBJECT('source_items', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END, 'evidence', '[]'::jsonb),
        'evidence', '[]'::jsonb,
        'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE),
        'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
        'count_deltas', JSONB_BUILD_OBJECT(),
        'filters', v_filters
      )
      INTO v_out
    FROM resolved
    WHERE resolved.resolved_timesheet_id IS NOT NULL OR resolved.resolved_contract_week_id IS NOT NULL;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', 'compare_import',
        'profile', 'compare_import',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk process compare/import context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;


    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      v_canonical_row_json := NULL;
      v_canonical_row_signature := NULL;
      v_canonical_row_key := NULLIF(BTRIM(COALESCE(v_out->>'row_key', v_out#>>'{row,row_key}', v_out#>>'{data_row,row_key}', v_out#>>'{row_patch,row_key}', '')), '');
      v_canonical_timesheet_id := NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', v_out->>'current_timesheet_id', v_out#>>'{row,timesheet_id}', v_out#>>'{row,current_timesheet_id}', v_out#>>'{data_row,timesheet_id}', v_out#>>'{data_row,current_timesheet_id}', v_out#>>'{row_patch,timesheet_id}', v_out#>>'{row_patch,current_timesheet_id}', '')), '');
      v_canonical_contract_week_id := CASE
        WHEN v_canonical_timesheet_id IS NULL THEN NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', v_out#>>'{row,contract_week_id}', v_out#>>'{data_row,contract_week_id}', v_out#>>'{row_patch,contract_week_id}', '')), '')
        ELSE NULL
      END;

      IF v_canonical_row_key IS NOT NULL OR v_canonical_timesheet_id IS NOT NULL OR v_canonical_contract_week_id IS NOT NULL THEN
        SELECT canonical_result.row_json
        INTO v_canonical_row_json
        FROM public.bulk_timesheet_row_patch_v1(
          JSONB_STRIP_NULLS(
            JSONB_BUILD_OBJECT(
              'dataset_mode', 'process',
              'projection', 'active_row_header',
              'profile', COALESCE(NULLIF(BTRIM(v_profile), ''), 'status_header'),
              'row_key', v_canonical_row_key,
              'timesheet_id', v_canonical_timesheet_id,
              'current_timesheet_id', v_canonical_timesheet_id,
              'requested_timesheet_id', v_canonical_timesheet_id,
              'expected_timesheet_id', v_canonical_timesheet_id,
              'contract_week_id', v_canonical_contract_week_id
            )
          )
        ) AS canonical_result(row_json)
        WHERE canonical_result.row_json IS NOT NULL
        ORDER BY canonical_result.row_json->>'row_key'
        LIMIT 1;

        v_canonical_row_signature := NULLIF(BTRIM(COALESCE(v_canonical_row_json->>'row_signature', '')), '');
        IF v_canonical_row_signature IS NOT NULL THEN
          v_out := v_out || JSONB_BUILD_OBJECT(
            'row_signature', v_canonical_row_signature,
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature)
          );
        END IF;
      END IF;
    END IF;

    RETURN private._candidate_office_context_overlay_v1(public.bulk_process_retention_contract_patch_v1(v_out));
  END IF;

  WITH decision_row AS (
    SELECT patch_result.row_json
    FROM public.bulk_timesheet_row_patch_v1(
      v_decision_filters
      || JSONB_BUILD_OBJECT(
           'dataset_mode', 'process',
           'projection', 'active_row_header',
           'profile', v_profile
         )
    ) AS patch_result(row_json)
    ORDER BY patch_result.row_json->>'row_key'
    LIMIT 1
  ),
  row_ids AS (
    SELECT
      decision_row.row_json,
      CASE
        WHEN COALESCE(decision_row.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'timesheet_id')::uuid
        ELSE NULL::uuid
      END AS timesheet_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_week_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_week_id')::uuid
        ELSE NULL::uuid
      END AS contract_week_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_id')::uuid
        ELSE NULL::uuid
      END AS contract_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'candidate_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'candidate_id')::uuid
        ELSE NULL::uuid
      END AS candidate_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'client_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'client_id')::uuid
        ELSE NULL::uuid
      END AS client_id
    FROM decision_row
  ),
  timesheet_row AS (
    SELECT
      ts0.timesheet_id,
      ts0.booking_id,
      ts0.occupant_key_norm,
      ts0.hospital_norm,
      ts0.ward_norm,
      ts0.job_title_norm,
      ts0.shift_label_norm,
      ts0.scheduled_start_iso,
      ts0.scheduled_end_iso,
      ts0.worked_start_iso,
      ts0.worked_end_iso,
      ts0.break_start_iso,
      ts0.break_end_iso,
      ts0.break_minutes,
      ts0.worked_minutes,
      ts0.week_ending_date,
      ts0.auth_name,
      ts0.auth_job_title,
      ts0.authorised_at_server,
      ts0.r2_nurse_key,
      ts0.r2_auth_key,
      ts0.reference_number,
      ts0.reference_set_at,
      ts0.status,
      ts0.created_at,
      ts0.updated_at,
      ts0.version,
      ts0.is_current,
      ts0.contract_id,
      ts0.submission_mode,
      ts0.manual_pdf_r2_key,
      ts0.line_type,
      ts0.sheet_scope,
      ts0.actual_schedule_json,
      ts0.additional_units_week,
      ts0.additional_units_per_day,
      ts0.qr_status,
      ts0.qr_payload_json,
      ts0.qr_generated_at,
      ts0.qr_scanned_at,
      ts0.qr_scan_info_json,
      ts0.qr_r2_key,
      ts0.day_references_json,
      ts0.manual_pdf_rotation_degrees,
      ts0.qr_last_sent_hash,
      ts0.qr_last_sent_at_utc,
      ts0.qr_signed_hash,
      ts0.qr_signed_at_utc,
      ts0.candidate_hint_text,
      ts0.band,
      ts0.generated_pdf_at_utc,
      ts0.generated_pdf_refs_sig,
      ts0.generated_pdf_refs_snapshot_json,
      ts0.generated_pdf_refs_captured_at_utc,
      ts0.qr_sent_refs_sig,
      ts0.qr_sent_refs_snapshot_json,
      ts0.qr_sent_refs_captured_at_utc,
      ts0.is_adjustment,
      ts0.parent_timesheet_id,
      ts0.correction_id,
      ts0.correction_kind,
      ts0.adjustment_origin
    FROM public.timesheets AS ts0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND ts0.timesheet_id = ids.timesheet_id
      AND ts0.is_current = TRUE
    ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
    LIMIT 1
  ),
  tsfin_row AS (
    SELECT
      tf0.id,
      tf0.timesheet_id,
      tf0.timesheet_version,
      tf0.basis,
      tf0.is_current,
      tf0.is_stale,
      tf0.stale_reason,
      tf0.worked_start_iso,
      tf0.worked_end_iso,
      tf0.break_start_iso,
      tf0.break_end_iso,
      tf0.break_minutes,
      tf0.candidate_id,
      tf0.client_id,
      tf0.role,
      tf0.band,
      tf0.pay_method,
      tf0.policy_snapshot_json,
      tf0.rate_source_refs_json,
      tf0.hours_day,
      tf0.hours_night,
      tf0.hours_sat,
      tf0.hours_sun,
      tf0.hours_bh,
      tf0.pay_day,
      tf0.pay_night,
      tf0.pay_sat,
      tf0.pay_sun,
      tf0.pay_bh,
      tf0.charge_day,
      tf0.charge_night,
      tf0.charge_sat,
      tf0.charge_sun,
      tf0.charge_bh,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.computed_at_utc,
      tf0.locked_by_invoice_id,
      tf0.locked_at_utc,
      tf0.unlocked_by_credit_note_id,
      tf0.created_at,
      tf0.updated_at,
      tf0.occupant_key_norm,
      tf0.candidate_assignment,
      tf0.processing_status,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_charge_ex_vat,
      tf0.expenses_description,
      tf0.expenses_evidence_r2_key,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.mileage_evidence_r2_key,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.po_number,
      tf0.pay_on_hold,
      tf0.pay_on_hold_reason,
      tf0.pay_on_hold_since_utc,
      tf0.paid_at_utc,
      tf0.paid_by_user_id,
      tf0.payment_reference,
      tf0.remittance_last_sent_at_utc,
      tf0.remittance_send_count,
      tf0.pay_wtr_rate_pct_snapshot,
      tf0.pay_vat_rate_pct_snapshot,
      tf0.pay_vat_amount_snapshot,
      tf0.pay_total_inc_vat_snapshot,
      tf0.processed_by_user_id,
      tf0.processed_at_utc,
      tf0.authorised_by_user_id,
      tf0.authorised_at_utc,
      tf0.expenses_evidence_manifest,
      tf0.mileage_evidence_manifest,
      tf0.actual_schedule_json,
      tf0.actual_minutes_by_day_json,
      tf0.additional_units_json,
      tf0.additional_pay_ex_vat,
      tf0.additional_charge_ex_vat,
      tf0.additional_margin_ex_vat,
      tf0.invoice_breakdown_json,
      tf0.nhsp_import_id,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.external_source_rows_json,
      tf0.mileage_units,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat
    FROM public.timesheets_financials AS tf0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tf0.timesheet_id = ids.timesheet_id
      AND tf0.is_current = TRUE
    ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
    LIMIT 1
  ),
  contract_week_row AS (
    SELECT
      cw0.id,
      cw0.contract_id,
      cw0.week_ending_date,
      cw0.additional_seq,
      cw0.status,
      cw0.submission_mode_snapshot,
      cw0.timesheet_id,
      cw0.uploaded_pdf_r2_key,
      cw0.day_entries_json,
      cw0.totals_json,
      cw0.created_at,
      cw0.updated_at,
      cw0.planned_schedule_json,
      cw0.is_adjustment,
      cw0.enforce_day_partition,
      cw0.allowed_days_mask,
      cw0.split_boundary_date,
      cw0.worker_note,
      cw0.split_group_key
    FROM public.contract_weeks AS cw0
    CROSS JOIN row_ids AS ids
    WHERE (ids.contract_week_id IS NOT NULL AND cw0.id = ids.contract_week_id)
       OR (ids.contract_week_id IS NULL AND ids.timesheet_id IS NOT NULL AND cw0.timesheet_id = ids.timesheet_id)
    ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
    LIMIT 1
  ),
  contract_row AS (
    SELECT
      ct0.id,
      ct0.candidate_id,
      ct0.client_id,
      ct0.role,
      ct0.band,
      ct0.display_site,
      ct0.ward_hint,
      ct0.start_date,
      ct0.end_date,
      ct0.pay_method_snapshot,
      ct0.rates_json,
      ct0.std_hours_json,
      ct0.default_submission_mode,
      ct0.week_ending_weekday_snapshot,
      ct0.auto_invoice,
      ct0.require_reference_to_pay,
      ct0.require_reference_to_invoice,
      ct0.created_at,
      ct0.updated_at,
      ct0.bucket_labels_json,
      ct0.std_schedule_json,
      ct0.mileage_pay_rate,
      ct0.mileage_charge_rate,
      ct0.additional_rates_json,
      ct0.self_bill,
      ct0.weekly_timesheet_source,
      ct0.no_timesheet_required,
      ct0.daily_calc_of_invoices,
      ct0.group_nightsat_sunbh,
      ct0.is_nhsp,
      ct0.autoprocess_hr,
      ct0.requires_hr,
      ct0.hr_attach_to_invoice,
      ct0.ts_attach_to_invoice,
      ct0.overrideclientsettings,
      ct0.reference_number_required_to_issue_invoice,
      ct0.send_manual_invoices_to_different_email,
      ct0.manual_invoices_alt_email_address,
      ct0.is_ad_hoc
    FROM public.contracts AS ct0
    CROSS JOIN row_ids AS ids
    LEFT JOIN timesheet_row AS ts1 ON TRUE
    LEFT JOIN contract_week_row AS cw1 ON TRUE
    WHERE ct0.id = COALESCE(ids.contract_id, ts1.contract_id, cw1.contract_id)
    ORDER BY ct0.updated_at DESC NULLS LAST, ct0.created_at DESC NULLS LAST, ct0.id DESC
    LIMIT 1
  ),
  candidate_row AS (
    SELECT
      cand0.id,
      cand0.tms_ref,
      cand0.first_name,
      cand0.last_name,
      cand0.display_name,
      cand0.email,
      cand0.phone,
      cand0.pay_method,
      cand0.umbrella_id,
      cand0.active,
      cand0.key_norm,
      cand0.mileage_pay_rate,
      cand0.job_title_id,
      cand0.prof_reg_number,
      cand0.prof_reg_type,
      cand0.ni_number,
      cand0.date_of_birth,
      cand0.gender,
      cand0.title,
      cand0.opt_in_email,
      cand0.opt_in_sms,
      cand0.opt_in_whatsapp,
      cand0.band,
      cand0.tms_ref_num
    FROM public.candidates AS cand0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cand0.id = COALESCE(ids.candidate_id, ct1.candidate_id)
    LIMIT 1
  ),
  client_row AS (
    SELECT
      cli0.id,
      cli0.cli_ref,
      cli0.name,
      cli0.invoice_address,
      cli0.primary_invoice_email,
      cli0.ap_phone,
      cli0.vat_chargeable,
      cli0.payment_terms_days,
      cli0.mileage_charge_rate,
      cli0.ts_queries_email,
      cli0.client_address,
      cli0.contact_title,
      cli0.contact_known_as,
      cli0.contact_forename,
      cli0.contact_surname,
      cli0.contact_job_title,
      cli0.contact_tel,
      cli0.contact_mobile,
      cli0.contact_email,
      cli0.website
    FROM public.clients AS cli0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cli0.id = COALESCE(ids.client_id, ct1.client_id)
    LIMIT 1
  ),
  validation_rows AS (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.booking_id,
      tv0.status,
      tv0.reason_code,
      tv0.hr_request_id,
      tv0.validated_at_utc,
      tv0.override_requested_at_utc,
      tv0.override_requested_by,
      tv0.override_confirmed_at_utc,
      tv0.override_confirmed_by,
      tv0.override_reason,
      tv0.override_cleared_at_utc,
      tv0.last_source,
      tv0.created_at,
      tv0.updated_at,
      tv0.hr_request_source,
      tv0.hr_request_set_by,
      tv0.hr_request_set_at_utc,
      tv0.pre_validated
    FROM public.timesheet_validations AS tv0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tv0.timesheet_id = ids.timesheet_id
    ORDER BY tv0.validated_at_utc DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
  ),
  validation_payload AS (
    SELECT
      COALESCE(JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', validation_rows.id,
          'timesheet_id', validation_rows.timesheet_id,
          'booking_id', validation_rows.booking_id,
          'status', validation_rows.status,
          'reason_code', validation_rows.reason_code,
          'hr_request_id', validation_rows.hr_request_id,
          'validated_at_utc', validation_rows.validated_at_utc,
          'override_requested_at_utc', validation_rows.override_requested_at_utc,
          'override_requested_by', validation_rows.override_requested_by,
          'override_confirmed_at_utc', validation_rows.override_confirmed_at_utc,
          'override_confirmed_by', validation_rows.override_confirmed_by,
          'override_reason', validation_rows.override_reason,
          'override_cleared_at_utc', validation_rows.override_cleared_at_utc,
          'last_source', validation_rows.last_source,
          'created_at', validation_rows.created_at,
          'updated_at', validation_rows.updated_at,
          'hr_request_source', validation_rows.hr_request_source,
          'hr_request_set_by', validation_rows.hr_request_set_by,
          'hr_request_set_at_utc', validation_rows.hr_request_set_at_utc,
          'pre_validated', validation_rows.pre_validated
        ) ORDER BY validation_rows.validated_at_utc DESC NULLS LAST, validation_rows.created_at DESC NULLS LAST, validation_rows.id DESC
      ), '[]'::jsonb) AS validations_json,
      (
        SELECT JSONB_BUILD_OBJECT(
          'id', vr1.id,
          'status', vr1.status,
          'reason_code', vr1.reason_code,
          'hr_request_id', vr1.hr_request_id,
          'validated_at_utc', vr1.validated_at_utc,
          'pre_validated', COALESCE(vr1.pre_validated, FALSE),
          'updated_at', vr1.updated_at
        )
        FROM validation_rows AS vr1
        ORDER BY vr1.validated_at_utc DESC NULLS LAST, vr1.created_at DESC NULLS LAST, vr1.id DESC
        LIMIT 1
      ) AS latest_validation_json
    FROM validation_rows
  ),
  evidence_items AS (
    SELECT
      0::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_id'), ''), 'sys:timesheet:' || COALESCE(row_ids.timesheet_id::text, row_ids.contract_week_id::text)),
        'evidence_id', NULL::uuid,
        'queue_id', NULL::uuid,
        'timesheet_id', row_ids.timesheet_id,
        'contract_week_id', row_ids.contract_week_id,
        'kind', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_kind'), ''), 'TIMESHEET'),
        'display_name', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'filename', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'storage_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'r2_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'file_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'download_storage_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'original_filename', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''), 'Timesheet PDF'),
        'mime_type', CASE
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.webp($|[?#])' THEN 'image/webp'
          ELSE NULL::text
        END,
        'content_type', CASE
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.webp($|[?#])' THEN 'image/webp'
          ELSE NULL::text
        END,
        'uploaded_at_utc', row_ids.row_json->>'updated_at',
        'rotation_degrees', CASE WHEN COALESCE(row_ids.row_json->>'manual_pdf_rotation_degrees', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'manual_pdf_rotation_degrees')::integer ELSE 0 END,
        'last_rotation_deg', CASE WHEN COALESCE(row_ids.row_json->>'manual_pdf_rotation_degrees', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'manual_pdf_rotation_degrees')::integer ELSE 0 END,
        'page_count', NULL::integer,
        'pages', '[]'::jsonb,
        'system', TRUE,
        'is_view_only', TRUE,
        'can_delete', FALSE,
        'can_reclassify', FALSE,
        'can_edit_kind', FALSE,
        'can_edit_type', FALSE,
        'can_return_to_queue', FALSE,
        'preview_mode', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_preview_mode'), ''), 'PDF'),
        'source_label', 'System',
        'source_badge', 'System'
      ) AS item_json
    FROM row_ids
    WHERE v_include_evidence = TRUE
      AND NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '') IS NOT NULL
      AND NOT (
        NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') LIKE 'evidence:%'
        OR (
          row_ids.timesheet_id IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM public.timesheet_evidence AS te_primary_artifact
            WHERE te_primary_artifact.timesheet_id = row_ids.timesheet_id
              AND te_primary_artifact.storage_key = NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '')
          )
        )
      )
      AND NOT (
        UPPER(COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_kind'), ''), 'TIMESHEET')) = 'TIMESHEET'
        AND row_ids.timesheet_id IS NOT NULL
        AND EXISTS (
          SELECT 1
          FROM public.manual_timesheet_queue AS mq_primary_artifact_returned
          WHERE NULLIF(regexp_replace(BTRIM(COALESCE(mq_primary_artifact_returned.r2_key, '')), '^/+', ''), '') =
                NULLIF(regexp_replace(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')), '^/+', ''), '')
            AND UPPER(COALESCE(mq_primary_artifact_returned.status, '')) = 'QUEUED'
            AND (
              mq_primary_artifact_returned.timesheet_id = row_ids.timesheet_id
              OR NULLIF(BTRIM(COALESCE(mq_primary_artifact_returned.meta_json ->> 'returned_from_timesheet_id', '')), '') = row_ids.timesheet_id::text
              OR NULLIF(BTRIM(COALESCE(mq_primary_artifact_returned.meta_json ->> 'dematerialised_from_timesheet_id', '')), '') = row_ids.timesheet_id::text
              OR NULLIF(BTRIM(COALESCE(mq_primary_artifact_returned.meta_json ->> 'attached_to_timesheet_id', '')), '') = row_ids.timesheet_id::text
              OR NULLIF(BTRIM(COALESCE(mq_primary_artifact_returned.meta_json ->> 'materialised_to_timesheet_id', '')), '') = row_ids.timesheet_id::text
              OR NULLIF(BTRIM(COALESCE(mq_primary_artifact_returned.meta_json #>> '{return_queue_previous_meta,materialised_to_timesheet_id}', '')), '') = row_ids.timesheet_id::text
              OR (row_ids.contract_week_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_primary_artifact_returned.meta_json ->> 'contract_week_id', '')), '') = row_ids.contract_week_id::text)
              OR (row_ids.contract_week_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_primary_artifact_returned.meta_json #>> '{return_queue_previous_meta,contract_week_id}', '')), '') = row_ids.contract_week_id::text)
              OR (row_ids.contract_week_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_primary_artifact_returned.meta_json #>> '{return_queue_previous_meta,active_identity}', '')), '') = 'contract_week:' || row_ids.contract_week_id::text)
              OR (row_ids.contract_week_id IS NOT NULL AND NULLIF(BTRIM(COALESCE(mq_primary_artifact_returned.meta_json #>> '{return_queue_previous_meta,preview_identity}', '')), '') = 'contract_week:' || row_ids.contract_week_id::text)
            )
        )
      )
    UNION ALL
    SELECT
      10::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', te0.id,
        'evidence_id', te0.id,
        'queue_id', NULL::uuid,
        'timesheet_id', te0.timesheet_id,
        'contract_week_id', NULL::uuid,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(te0.kind), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
        'filename', COALESCE(
          NULLIF(
            BTRIM(
              CASE
                WHEN NULLIF(BTRIM(COALESCE(te0.display_name, '')), '') IS NOT NULL
                  AND LOWER(NULLIF(BTRIM(COALESCE(te0.display_name, '')), '')) ~ '\.(pdf|png|jpe?g|gif|webp|bmp|svg|tif|tiff|heic|heif|avif)$'
                  THEN te0.display_name
                WHEN NULLIF(BTRIM(COALESCE(te0.storage_key, '')), '') IS NOT NULL
                  THEN REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')
                ELSE te0.display_name
              END
            ),
            ''
          ),
          'Evidence'
        ),
        'storage_key', te0.storage_key,
        'r2_key', te0.storage_key,
        'file_key', te0.storage_key,
        'download_storage_key', te0.storage_key,
        'original_filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
        'mime_type', CASE
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.bmp($|[?#])' THEN 'image/bmp'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.svg($|[?#])' THEN 'image/svg+xml'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.tiff?($|[?#])' THEN 'image/tiff'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.heic($|[?#])' THEN 'image/heic'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.heif($|[?#])' THEN 'image/heif'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.avif($|[?#])' THEN 'image/avif'
          ELSE NULL::text
        END,
        'content_type', CASE
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.bmp($|[?#])' THEN 'image/bmp'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.svg($|[?#])' THEN 'image/svg+xml'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.tiff?($|[?#])' THEN 'image/tiff'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.heic($|[?#])' THEN 'image/heic'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.heif($|[?#])' THEN 'image/heif'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.avif($|[?#])' THEN 'image/avif'
          ELSE NULL::text
        END,
        'uploaded_at_utc', te0.created_at,
        'created_at', te0.created_at,
        'created_by', te0.created_by,
        'rotation', 0,
        'rotation_degrees', 0,
        'last_rotation_deg', 0,
        'page_count', NULL::integer,
        'pages', '[]'::jsonb,
        'system', FALSE,
        'is_view_only', NOT COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_delete', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_reclassify', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_edit_kind', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_edit_type', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_return_to_queue', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'preview_mode', CASE
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'PDF'
          WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.(png|jpe?g|gif|webp|bmp|svg|tif|tiff|heic|heif|avif)($|[?#])' THEN 'IMAGE'
          ELSE 'FILE'
        END,
        'source_label', 'Attached',
        'source_badge', 'Attached'
      ) AS item_json
    FROM public.timesheet_evidence AS te0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NOT NULL
      AND te0.timesheet_id = ids.timesheet_id
    UNION ALL
    SELECT
      20::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', mq0.id,
        'evidence_id', NULL::uuid,
        'queue_id', mq0.id,
        'timesheet_id', NULL::uuid,
        'contract_week_id', ids.contract_week_id,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'staged_kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'filename', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'original_filename', mq0.original_filename,
        'storage_key', mq0.r2_key,
        'r2_key', mq0.r2_key,
        'file_key', mq0.r2_key,
        'download_storage_key', mq0.r2_key,
        'mime_type', COALESCE(NULLIF(BTRIM(mq0.mime_type), ''), CASE
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.webp($|[?#])' THEN 'image/webp'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.bmp($|[?#])' THEN 'image/bmp'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.svg($|[?#])' THEN 'image/svg+xml'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.tiff?($|[?#])' THEN 'image/tiff'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.heic($|[?#])' THEN 'image/heic'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.heif($|[?#])' THEN 'image/heif'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.avif($|[?#])' THEN 'image/avif'
          ELSE NULL::text
        END),
        'content_type', COALESCE(NULLIF(BTRIM(mq0.mime_type), ''), CASE
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.pdf($|[?#])' THEN 'application/pdf'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.png($|[?#])' THEN 'image/png'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.gif($|[?#])' THEN 'image/gif'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.webp($|[?#])' THEN 'image/webp'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.bmp($|[?#])' THEN 'image/bmp'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.svg($|[?#])' THEN 'image/svg+xml'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.tiff?($|[?#])' THEN 'image/tiff'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.heic($|[?#])' THEN 'image/heic'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.heif($|[?#])' THEN 'image/heif'
          WHEN LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.avif($|[?#])' THEN 'image/avif'
          ELSE NULL::text
        END),
        'content_hash', mq0.content_hash,
        'uploaded_at_utc', mq0.uploaded_at_utc,
        'staged_at_utc', COALESCE(mq0.meta_json->>'staged_at_utc', mq0.uploaded_at_utc::text),
        'staged_by_user_id', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'staged_by_user_id'), ''), mq0.uploaded_by_user_id::text),
        'rotation_degrees', COALESCE(mq0.last_rotation_deg::integer, 0),
        'last_rotation_deg', COALESCE(mq0.last_rotation_deg::integer, 0),
        'page_count', CASE WHEN COALESCE(mq0.meta_json->>'page_count', '') ~ '^[0-9]+$' THEN (mq0.meta_json->>'page_count')::integer ELSE NULL::integer END,
        'pages', '[]'::jsonb,
        'status', mq0.status,
        'system', FALSE,
        'is_view_only', NOT COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_delete', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_reclassify', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_edit_kind', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_edit_type', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_return_to_queue', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'is_staged_context', TRUE,
        'preview_mode', CASE
          WHEN LOWER(COALESCE(mq0.mime_type, '')) LIKE 'image/%'
            OR LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.(png|jpe?g|gif|webp|bmp|svg|tif|tiff|heic|heif|avif)($|[?#])' THEN 'IMAGE'
          WHEN LOWER(COALESCE(mq0.mime_type, '')) = 'application/pdf'
            OR LOWER(CONCAT_WS(' ', COALESCE(mq0.r2_key, ''), COALESCE(mq0.original_filename, ''))) ~ '\.pdf($|[?#])' THEN 'PDF'
          ELSE 'FILE'
        END,
        'source_label', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged'),
        'source_badge', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged')
      ) AS item_json
    FROM public.manual_timesheet_queue AS mq0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NULL
      AND ids.contract_week_id IS NOT NULL
      AND UPPER(COALESCE(mq0.status, '')) = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') = ids.contract_week_id::text
  ),
  evidence_ranked AS (
    SELECT
      evidence_items.sort_order,
      evidence_items.item_json,
      ROW_NUMBER() OVER (ORDER BY evidence_items.sort_order ASC, evidence_items.item_json->>'id' ASC) AS rn
    FROM evidence_items
  ),
  evidence_payload AS (
    SELECT
      COALESCE(JSONB_AGG(evidence_ranked.item_json ORDER BY evidence_ranked.sort_order ASC, evidence_ranked.item_json->>'id' ASC), '[]'::jsonb) AS evidence_json,
      (
        SELECT er1.item_json
        FROM evidence_ranked AS er1
        WHERE er1.rn = 1
      ) AS primary_evidence_json
    FROM evidence_ranked
  ),
  timesheet_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'timesheet_id', timesheet_row.timesheet_id,
        'booking_id', timesheet_row.booking_id,
        'occupant_key_norm', timesheet_row.occupant_key_norm,
        'hospital_norm', timesheet_row.hospital_norm,
        'ward_norm', timesheet_row.ward_norm,
        'job_title_norm', timesheet_row.job_title_norm,
        'shift_label_norm', timesheet_row.shift_label_norm,
        'scheduled_start_iso', timesheet_row.scheduled_start_iso,
        'scheduled_end_iso', timesheet_row.scheduled_end_iso,
        'worked_start_iso', timesheet_row.worked_start_iso,
        'worked_end_iso', timesheet_row.worked_end_iso,
        'break_start_iso', timesheet_row.break_start_iso,
        'break_end_iso', timesheet_row.break_end_iso,
        'break_minutes', timesheet_row.break_minutes,
        'worked_minutes', timesheet_row.worked_minutes,
        'week_ending_date', timesheet_row.week_ending_date,
        'auth_name', timesheet_row.auth_name,
        'auth_job_title', timesheet_row.auth_job_title,
        'authorised_at_server', timesheet_row.authorised_at_server,
        'r2_nurse_key', timesheet_row.r2_nurse_key,
        'r2_auth_key', timesheet_row.r2_auth_key
      )
      || JSONB_BUILD_OBJECT(
        'reference_number', timesheet_row.reference_number,
        'reference_set_at', timesheet_row.reference_set_at,
        'status', timesheet_row.status,
        'created_at', timesheet_row.created_at,
        'updated_at', timesheet_row.updated_at,
        'version', timesheet_row.version,
        'is_current', timesheet_row.is_current,
        'contract_id', timesheet_row.contract_id,
        'submission_mode', timesheet_row.submission_mode,
        'manual_pdf_r2_key', timesheet_row.manual_pdf_r2_key,
        'line_type', timesheet_row.line_type,
        'sheet_scope', timesheet_row.sheet_scope,
        'actual_schedule_json', timesheet_row.actual_schedule_json,
        'additional_units_week', timesheet_row.additional_units_week,
        'additional_units_per_day', timesheet_row.additional_units_per_day,
        'qr_status', timesheet_row.qr_status,
        'qr_payload_json', timesheet_row.qr_payload_json,
        'qr_generated_at', timesheet_row.qr_generated_at,
        'qr_scanned_at', timesheet_row.qr_scanned_at,
        'qr_scan_info_json', timesheet_row.qr_scan_info_json,
        'qr_r2_key', timesheet_row.qr_r2_key,
        'day_references_json', timesheet_row.day_references_json,
        'manual_pdf_rotation_degrees', timesheet_row.manual_pdf_rotation_degrees
      )
      || JSONB_BUILD_OBJECT(
        'qr_last_sent_hash', timesheet_row.qr_last_sent_hash,
        'qr_last_sent_at_utc', timesheet_row.qr_last_sent_at_utc,
        'qr_signed_hash', timesheet_row.qr_signed_hash,
        'qr_signed_at_utc', timesheet_row.qr_signed_at_utc,
        'candidate_hint_text', timesheet_row.candidate_hint_text,
        'band', timesheet_row.band,
        'generated_pdf_at_utc', timesheet_row.generated_pdf_at_utc,
        'generated_pdf_refs_sig', timesheet_row.generated_pdf_refs_sig,
        'generated_pdf_refs_snapshot_json', timesheet_row.generated_pdf_refs_snapshot_json,
        'generated_pdf_refs_captured_at_utc', timesheet_row.generated_pdf_refs_captured_at_utc,
        'qr_sent_refs_sig', timesheet_row.qr_sent_refs_sig,
        'qr_sent_refs_snapshot_json', timesheet_row.qr_sent_refs_snapshot_json,
        'qr_sent_refs_captured_at_utc', timesheet_row.qr_sent_refs_captured_at_utc,
        'is_adjustment', timesheet_row.is_adjustment,
        'parent_timesheet_id', timesheet_row.parent_timesheet_id,
        'correction_id', timesheet_row.correction_id,
        'correction_kind', timesheet_row.correction_kind,
        'adjustment_origin', timesheet_row.adjustment_origin
      ) AS timesheet_json
    FROM timesheet_row
  ),
  tsfin_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', tsfin_row.id,
        'timesheet_id', tsfin_row.timesheet_id,
        'timesheet_version', tsfin_row.timesheet_version,
        'basis', tsfin_row.basis,
        'is_current', tsfin_row.is_current,
        'is_stale', tsfin_row.is_stale,
        'stale_reason', tsfin_row.stale_reason,
        'worked_start_iso', tsfin_row.worked_start_iso,
        'worked_end_iso', tsfin_row.worked_end_iso,
        'break_start_iso', tsfin_row.break_start_iso,
        'break_end_iso', tsfin_row.break_end_iso,
        'break_minutes', tsfin_row.break_minutes,
        'candidate_id', tsfin_row.candidate_id,
        'client_id', tsfin_row.client_id,
        'role', tsfin_row.role,
        'band', tsfin_row.band,
        'pay_method', tsfin_row.pay_method,
        'policy_snapshot_json', tsfin_row.policy_snapshot_json,
        'rate_source_refs_json', tsfin_row.rate_source_refs_json,
        'hours_day', tsfin_row.hours_day,
        'hours_night', tsfin_row.hours_night,
        'hours_sat', tsfin_row.hours_sat,
        'hours_sun', tsfin_row.hours_sun,
        'hours_bh', tsfin_row.hours_bh
      )
      || JSONB_BUILD_OBJECT(
        'pay_day', tsfin_row.pay_day,
        'pay_night', tsfin_row.pay_night,
        'pay_sat', tsfin_row.pay_sat,
        'pay_sun', tsfin_row.pay_sun,
        'pay_bh', tsfin_row.pay_bh,
        'charge_day', tsfin_row.charge_day,
        'charge_night', tsfin_row.charge_night,
        'charge_sat', tsfin_row.charge_sat,
        'charge_sun', tsfin_row.charge_sun,
        'charge_bh', tsfin_row.charge_bh,
        'total_hours', tsfin_row.total_hours,
        'total_pay_ex_vat', tsfin_row.total_pay_ex_vat,
        'total_charge_ex_vat', tsfin_row.total_charge_ex_vat,
        'margin_ex_vat', tsfin_row.margin_ex_vat,
        'computed_at_utc', tsfin_row.computed_at_utc,
        'locked_by_invoice_id', tsfin_row.locked_by_invoice_id,
        'locked_at_utc', tsfin_row.locked_at_utc,
        'unlocked_by_credit_note_id', tsfin_row.unlocked_by_credit_note_id,
        'created_at', tsfin_row.created_at,
        'updated_at', tsfin_row.updated_at,
        'occupant_key_norm', tsfin_row.occupant_key_norm,
        'candidate_assignment', tsfin_row.candidate_assignment,
        'processing_status', tsfin_row.processing_status
      )
      || JSONB_BUILD_OBJECT(
        'expenses_pay_ex_vat', tsfin_row.expenses_pay_ex_vat,
        'expenses_charge_ex_vat', tsfin_row.expenses_charge_ex_vat,
        'expenses_description', tsfin_row.expenses_description,
        'expenses_evidence_r2_key', tsfin_row.expenses_evidence_r2_key,
        'mileage_pay_ex_vat', tsfin_row.mileage_pay_ex_vat,
        'mileage_charge_ex_vat', tsfin_row.mileage_charge_ex_vat,
        'mileage_evidence_r2_key', tsfin_row.mileage_evidence_r2_key,
        'mileage_pay_rate', tsfin_row.mileage_pay_rate,
        'mileage_charge_rate', tsfin_row.mileage_charge_rate,
        'mileage_units', tsfin_row.mileage_units,
        'travel_pay_ex_vat', tsfin_row.travel_pay_ex_vat,
        'travel_charge_ex_vat', tsfin_row.travel_charge_ex_vat,
        'accommodation_pay_ex_vat', tsfin_row.accommodation_pay_ex_vat,
        'accommodation_charge_ex_vat', tsfin_row.accommodation_charge_ex_vat,
        'other_pay_ex_vat', tsfin_row.other_pay_ex_vat,
        'other_charge_ex_vat', tsfin_row.other_charge_ex_vat,
        'po_number', tsfin_row.po_number,
        'pay_on_hold', tsfin_row.pay_on_hold,
        'pay_on_hold_reason', tsfin_row.pay_on_hold_reason,
        'pay_on_hold_since_utc', tsfin_row.pay_on_hold_since_utc,
        'paid_at_utc', tsfin_row.paid_at_utc,
        'paid_by_user_id', tsfin_row.paid_by_user_id,
        'payment_reference', tsfin_row.payment_reference
      )
      || JSONB_BUILD_OBJECT(
        'remittance_last_sent_at_utc', tsfin_row.remittance_last_sent_at_utc,
        'remittance_send_count', tsfin_row.remittance_send_count,
        'pay_wtr_rate_pct_snapshot', tsfin_row.pay_wtr_rate_pct_snapshot,
        'pay_vat_rate_pct_snapshot', tsfin_row.pay_vat_rate_pct_snapshot,
        'pay_vat_amount_snapshot', tsfin_row.pay_vat_amount_snapshot,
        'pay_total_inc_vat_snapshot', tsfin_row.pay_total_inc_vat_snapshot,
        'processed_by_user_id', tsfin_row.processed_by_user_id,
        'processed_at_utc', tsfin_row.processed_at_utc,
        'authorised_by_user_id', tsfin_row.authorised_by_user_id,
        'authorised_at_utc', tsfin_row.authorised_at_utc,
        'expenses_evidence_manifest', tsfin_row.expenses_evidence_manifest,
        'mileage_evidence_manifest', tsfin_row.mileage_evidence_manifest,
        'actual_schedule_json', tsfin_row.actual_schedule_json,
        'actual_minutes_by_day_json', tsfin_row.actual_minutes_by_day_json,
        'additional_units_json', tsfin_row.additional_units_json,
        'additional_pay_ex_vat', tsfin_row.additional_pay_ex_vat,
        'additional_charge_ex_vat', tsfin_row.additional_charge_ex_vat,
        'additional_margin_ex_vat', tsfin_row.additional_margin_ex_vat,
        'invoice_breakdown_json', tsfin_row.invoice_breakdown_json,
        'nhsp_import_id', tsfin_row.nhsp_import_id,
        'has_rate_issue', tsfin_row.has_rate_issue,
        'has_pay_channel_issue', tsfin_row.has_pay_channel_issue,
        'hr_crosscheck_status', tsfin_row.hr_crosscheck_status,
        'hr_crosscheck_issues', tsfin_row.hr_crosscheck_issues,
        'external_source_rows_json', tsfin_row.external_source_rows_json
      ) AS tsfin_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN NULL::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN JSONB_BUILD_OBJECT('mode', 'SEGMENTS', 'segments', tsfin_row.invoice_breakdown_json)
        ELSE NULL::jsonb
      END AS invoice_breakdown_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND UPPER(COALESCE(tsfin_row.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS' THEN TRUE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN TRUE
        ELSE FALSE
      END AS is_segments_mode,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json->'segments') = 'array' THEN tsfin_row.invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END AS segments_json
    FROM tsfin_row
  ),
  contract_week_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_week_row.id,
        'contract_id', contract_week_row.contract_id,
        'week_ending_date', contract_week_row.week_ending_date,
        'additional_seq', contract_week_row.additional_seq,
        'status', contract_week_row.status,
        'submission_mode_snapshot', contract_week_row.submission_mode_snapshot,
        'timesheet_id', contract_week_row.timesheet_id,
        'uploaded_pdf_r2_key', CASE WHEN row_ids.timesheet_id IS NULL THEN contract_week_row.uploaded_pdf_r2_key ELSE NULL::text END,
        'day_entries_json', contract_week_row.day_entries_json,
        'totals_json', contract_week_row.totals_json,
        'created_at', contract_week_row.created_at,
        'updated_at', contract_week_row.updated_at,
        'planned_schedule_json', contract_week_row.planned_schedule_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'is_adjustment', contract_week_row.is_adjustment,
        'enforce_day_partition', contract_week_row.enforce_day_partition,
        'allowed_days_mask', contract_week_row.allowed_days_mask,
        'split_boundary_date', contract_week_row.split_boundary_date,
        'worker_note', contract_week_row.worker_note,
        'split_group_key', contract_week_row.split_group_key,
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display'
      ) AS contract_week_json
    FROM contract_week_row
    CROSS JOIN row_ids
    LEFT JOIN contract_row ON TRUE
  ),
  contract_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_row.id,
        'candidate_id', contract_row.candidate_id,
        'client_id', contract_row.client_id,
        'role', contract_row.role,
        'band', contract_row.band,
        'display_site', contract_row.display_site,
        'ward_hint', contract_row.ward_hint,
        'start_date', contract_row.start_date,
        'end_date', contract_row.end_date,
        'pay_method_snapshot', contract_row.pay_method_snapshot,
        'rates_json', contract_row.rates_json,
        'std_hours_json', contract_row.std_hours_json,
        'default_submission_mode', contract_row.default_submission_mode,
        'week_ending_weekday_snapshot', contract_row.week_ending_weekday_snapshot,
        'auto_invoice', contract_row.auto_invoice,
        'require_reference_to_pay', contract_row.require_reference_to_pay,
        'require_reference_to_invoice', contract_row.require_reference_to_invoice,
        'bucket_labels_json', contract_row.bucket_labels_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'mileage_pay_rate', contract_row.mileage_pay_rate,
        'mileage_charge_rate', contract_row.mileage_charge_rate,
        'additional_rates_json', contract_row.additional_rates_json,
        'self_bill', contract_row.self_bill,
        'weekly_timesheet_source', contract_row.weekly_timesheet_source,
        'no_timesheet_required', contract_row.no_timesheet_required,
        'daily_calc_of_invoices', contract_row.daily_calc_of_invoices,
        'group_nightsat_sunbh', contract_row.group_nightsat_sunbh,
        'is_nhsp', contract_row.is_nhsp,
        'autoprocess_hr', contract_row.autoprocess_hr,
        'requires_hr', contract_row.requires_hr,
        'hr_attach_to_invoice', contract_row.hr_attach_to_invoice,
        'ts_attach_to_invoice', contract_row.ts_attach_to_invoice,
        'overrideclientsettings', contract_row.overrideclientsettings,
        'reference_number_required_to_issue_invoice', contract_row.reference_number_required_to_issue_invoice,
        'send_manual_invoices_to_different_email', contract_row.send_manual_invoices_to_different_email,
        'manual_invoices_alt_email_address', contract_row.manual_invoices_alt_email_address,
        'is_ad_hoc', contract_row.is_ad_hoc,
        'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
        'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour'
      ) AS contract_json
    FROM contract_row
    CROSS JOIN row_ids
  ),
  related_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'counts', JSONB_BUILD_OBJECT(),
        'candidate', CASE WHEN candidate_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', candidate_row.id,
          'tms_ref', candidate_row.tms_ref,
          'first_name', candidate_row.first_name,
          'last_name', candidate_row.last_name,
          'display_name', candidate_row.display_name,
          'email', candidate_row.email,
          'phone', candidate_row.phone,
          'pay_method', candidate_row.pay_method,
          'umbrella_id', candidate_row.umbrella_id,
          'active', candidate_row.active,
          'key_norm', candidate_row.key_norm,
          'mileage_pay_rate', candidate_row.mileage_pay_rate,
          'job_title_id', candidate_row.job_title_id,
          'prof_reg_number', candidate_row.prof_reg_number,
          'prof_reg_type', candidate_row.prof_reg_type,
          'ni_number', candidate_row.ni_number,
          'date_of_birth', candidate_row.date_of_birth,
          'gender', candidate_row.gender,
          'title', candidate_row.title,
          'opt_in_email', candidate_row.opt_in_email,
          'opt_in_sms', candidate_row.opt_in_sms,
          'opt_in_whatsapp', candidate_row.opt_in_whatsapp,
          'band', candidate_row.band,
          'tms_ref_num', candidate_row.tms_ref_num
        ) END,
        'client', CASE WHEN client_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', client_row.id,
          'cli_ref', client_row.cli_ref,
          'name', client_row.name,
          'invoice_address', client_row.invoice_address,
          'primary_invoice_email', client_row.primary_invoice_email,
          'ap_phone', client_row.ap_phone,
          'vat_chargeable', client_row.vat_chargeable,
          'payment_terms_days', client_row.payment_terms_days,
          'mileage_charge_rate', client_row.mileage_charge_rate,
          'ts_queries_email', client_row.ts_queries_email,
          'client_address', client_row.client_address,
          'contact_title', client_row.contact_title,
          'contact_known_as', client_row.contact_known_as,
          'contact_forename', client_row.contact_forename,
          'contact_surname', client_row.contact_surname,
          'contact_job_title', client_row.contact_job_title,
          'contact_tel', client_row.contact_tel,
          'contact_mobile', client_row.contact_mobile,
          'contact_email', client_row.contact_email,
          'website', client_row.website
        ) END,
        'contract', COALESCE(contract_payload.contract_json, NULL::jsonb),
        'invoices', '[]'::jsonb,
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT(),
        'invoice', NULL::jsonb,
        'umbrella', NULL::jsonb,
        'series', '[]'::jsonb
      ) AS related_json
    FROM row_ids
    LEFT JOIN candidate_row ON TRUE
    LEFT JOIN client_row ON TRUE
    LEFT JOIN contract_payload ON TRUE
  )
,
  base_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'ok', TRUE,
        'context_kind', 'bulk_process_row_context',
        'context_profile', v_profile,
        'profile', v_profile,
        'context_type', 'bulk_process',
        'slim_context', TRUE,
        'evidence_loaded', v_include_evidence,
        'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
        'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
        'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
        'current_version', CASE WHEN COALESCE(row_ids.row_json->>'timesheet_version', '') ~ '^[0-9]+$' THEN (row_ids.row_json->>'timesheet_version')::integer ELSE NULL::integer END,
        'contract_week_id', row_ids.row_json->>'contract_week_id',
        'row_key', row_ids.row_json->>'row_key',
        'row_signature', row_ids.row_json->>'row_signature',
        'was_stale', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'was_stale', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'has_timesheet', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'has_timesheet', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'locked', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'locked', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'bulk_process_bucket', row_ids.row_json->>'bulk_process_bucket',
        'bulk_authorise_classification', row_ids.row_json->>'bulk_authorise_classification',
        'bulk_authorise_section', row_ids.row_json->>'bulk_authorise_section',
        'route_family', row_ids.row_json->>'route_family',
        'route_subfamily', row_ids.row_json->>'route_subfamily',
        'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
        'is_import_authoritative', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_import_authoritative', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'compare_block_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'compare_block_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'is_adjustment', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_adjustment', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'additional_seq', CASE WHEN COALESCE(row_ids.row_json->>'additional_seq', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'additional_seq')::integer ELSE NULL::integer END,
        'actual_schedule_json', COALESCE(row_ids.row_json->'actual_schedule_json', '[]'::jsonb),
        'planned_schedule_json', COALESCE(row_ids.row_json->'planned_schedule_json', '[]'::jsonb),
        'contract_week_totals_json', COALESCE(row_ids.row_json->'contract_week_totals_json', '{}'::jsonb),
        'total_hours', CASE WHEN COALESCE(row_ids.row_json->>'total_hours', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN (row_ids.row_json->>'total_hours')::numeric ELSE NULL::numeric END,
        'suppress_standard_schedule_fallback', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'keep_additional_manual_adjustment_schedule_empty', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        '__suppressStandardScheduleFallback', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'__suppressStandardScheduleFallback', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        '__keepAdditionalManualAdjustmentScheduleEmpty', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'__keepAdditionalManualAdjustmentScheduleEmpty', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'period_type', row_ids.row_json->>'period_type',
        'timesheet_type_sort_key', CASE WHEN COALESCE(row_ids.row_json->>'timesheet_type_sort_key', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'timesheet_type_sort_key')::integer ELSE NULL::integer END,
        'can_save', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_save', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_process', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_process', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_unprocess', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_unprocess', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_edit_timesheet_data', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_edit_timesheet_data', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_manage_evidence', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_manage_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'can_add_additional_manual', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'can_add_additional_manual', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'review_only', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'review_only', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'hr_validation_required_for_invoice', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'hr_validation_required_for_invoice', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'validation_status', row_ids.row_json->>'validation_status',
        'validation_pre_validated', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'validation_pre_validated', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'has_deviation_marker', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'has_deviation_marker', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'deviation_marker_reason', row_ids.row_json->>'deviation_marker_reason'
      )
      || JSONB_BUILD_OBJECT(
        'row', row_ids.row_json || JSONB_BUILD_OBJECT(
          'uploaded_pdf_r2_key', CASE WHEN row_ids.timesheet_id IS NULL THEN row_ids.row_json->>'uploaded_pdf_r2_key' ELSE NULL::text END
        ),
        'row_patch', COALESCE(row_ids.row_json->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
          'uploaded_pdf_r2_key', CASE WHEN row_ids.timesheet_id IS NULL THEN row_ids.row_json->>'uploaded_pdf_r2_key' ELSE NULL::text END
        ),
        'details', (
          JSONB_BUILD_OBJECT(
            'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
            'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
            'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
            'current_version', CASE WHEN COALESCE(row_ids.row_json->>'timesheet_version', '') ~ '^[0-9]+$' THEN (row_ids.row_json->>'timesheet_version')::integer ELSE NULL::integer END,
            'was_stale', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'was_stale', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'booking_id', row_ids.row_json->>'booking_id',
            'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
            'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
            'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
            'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
            'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT()
          )
          || JSONB_BUILD_OBJECT(
            'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
            'validation_summary', JSONB_BUILD_OBJECT(
              'status', row_ids.row_json->>'validation_status',
              'pre_validated', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'validation_pre_validated', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'hr_validation_satisfied', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'hr_validation_satisfied', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'hr_validation_awaiting', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'hr_validation_awaiting', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
            ),
            'shifts', '[]'::jsonb,
            'contract_week_id', row_ids.row_json->>'contract_week_id',
            'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
            'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
            'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb)
          )
          || JSONB_BUILD_OBJECT(
            'policy', (
              CASE
                WHEN tsfin_payload.tsfin_json IS NOT NULL
                 AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
                ELSE JSONB_BUILD_OBJECT()
              END
              || JSONB_BUILD_OBJECT(
                'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
                'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
                'requires_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_requires_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
                'autoprocess_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_autoprocess_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
                'no_timesheet_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_no_timesheet_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
                'is_nhsp', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_is_nhsp', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE)
              )
            ),
            'effective', JSONB_BUILD_OBJECT(
              'route_type', row_ids.row_json->>'route_type',
              'route_display', row_ids.row_json->>'route_display',
              'route_family', row_ids.row_json->>'route_family',
              'route_subfamily', row_ids.row_json->>'route_subfamily',
              'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
              'is_import_authoritative', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_import_authoritative', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'is_adjustment', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_adjustment', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'additional_seq', CASE WHEN COALESCE(row_ids.row_json->>'additional_seq', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'additional_seq')::integer ELSE NULL::integer END,
              'actual_schedule_json', COALESCE(row_ids.row_json->'actual_schedule_json', '[]'::jsonb),
              'planned_schedule_json', COALESCE(row_ids.row_json->'planned_schedule_json', '[]'::jsonb),
              'suppress_standard_schedule_fallback', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'keep_additional_manual_adjustment_schedule_empty', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'summary_stage', row_ids.row_json->>'summary_stage',
              'client_requires_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_requires_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'client_autoprocess_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_autoprocess_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'client_no_timesheet_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_no_timesheet_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'client_is_nhsp', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_is_nhsp', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'contract_id', row_ids.row_json->>'contract_id',
              'ready_to_pay', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'ready_to_pay', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
              'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
            ),
            'ready_to_pay', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'ready_to_pay', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'summary_stage', row_ids.row_json->>'summary_stage',
            'route_type', row_ids.row_json->>'route_type',
            'route_display', row_ids.row_json->>'route_display',
            'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
            'sheet_scope', row_ids.row_json->>'sheet_scope',
            'period_type', row_ids.row_json->>'period_type'
          )
          || JSONB_BUILD_OBJECT(
            'qr_status', row_ids.row_json->>'qr_status',
            'qr_generated_at', row_ids.row_json->>'qr_generated_at',
            'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
            'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
            'uploaded_pdf_r2_key', CASE WHEN row_ids.timesheet_id IS NULL THEN row_ids.row_json->>'uploaded_pdf_r2_key' ELSE NULL::text END,
            'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
            'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees',
            'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
            'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
            'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
            'primary_artifact', CASE
              WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
                'id', row_ids.row_json->>'primary_artifact_id',
                'kind', row_ids.row_json->>'primary_artifact_kind',
                'display_name', row_ids.row_json->>'primary_artifact_display_name',
                'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
                'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
              )
              ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
            END,
            'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
            'pay_state', NULL::jsonb,
            'segment_snoozes', '[]'::jsonb
          )
        ),
        'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
        'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
        'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
        'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT(),
        'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
        'validation_summary', JSONB_BUILD_OBJECT(
          'status', row_ids.row_json->>'validation_status',
          'pre_validated', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'validation_pre_validated', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'hr_validation_satisfied', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'hr_validation_satisfied', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'hr_validation_awaiting', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'hr_validation_awaiting', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
        ),
        'shifts', '[]'::jsonb
      )
      || JSONB_BUILD_OBJECT(
        'effective', JSONB_BUILD_OBJECT(
          'route_type', row_ids.row_json->>'route_type',
          'route_display', row_ids.row_json->>'route_display',
          'route_family', row_ids.row_json->>'route_family',
          'route_subfamily', row_ids.row_json->>'route_subfamily',
          'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_import_authoritative', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'is_adjustment', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_adjustment', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'additional_seq', CASE WHEN COALESCE(row_ids.row_json->>'additional_seq', '') ~ '^-?[0-9]+$' THEN (row_ids.row_json->>'additional_seq')::integer ELSE NULL::integer END,
          'actual_schedule_json', COALESCE(row_ids.row_json->'actual_schedule_json', '[]'::jsonb),
          'planned_schedule_json', COALESCE(row_ids.row_json->'planned_schedule_json', '[]'::jsonb),
          'suppress_standard_schedule_fallback', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'keep_additional_manual_adjustment_schedule_empty', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'summary_stage', row_ids.row_json->>'summary_stage',
          'client_requires_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_requires_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'client_autoprocess_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_autoprocess_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'client_no_timesheet_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_no_timesheet_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'client_is_nhsp', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_is_nhsp', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'contract_id', row_ids.row_json->>'contract_id',
          'ready_to_pay', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'ready_to_pay', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
        ),
        'ready_to_pay', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'ready_to_pay', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'summary_stage', row_ids.row_json->>'summary_stage',
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display',
        'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
        'sheet_scope', row_ids.row_json->>'sheet_scope',
        'period_type', row_ids.row_json->>'period_type',
        'qr_status', row_ids.row_json->>'qr_status',
        'qr_generated_at', row_ids.row_json->>'qr_generated_at',
        'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
        'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
        'uploaded_pdf_r2_key', CASE WHEN row_ids.timesheet_id IS NULL THEN row_ids.row_json->>'uploaded_pdf_r2_key' ELSE NULL::text END,
        'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
        'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees'
      )
      || JSONB_BUILD_OBJECT(
        'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
        'policy', (
          CASE
            WHEN tsfin_payload.tsfin_json IS NOT NULL
             AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
            ELSE JSONB_BUILD_OBJECT()
          END
          || JSONB_BUILD_OBJECT(
            'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
            'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
            'requires_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_requires_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'autoprocess_hr', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_autoprocess_hr', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'no_timesheet_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_no_timesheet_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'is_nhsp', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'client_is_nhsp', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE)
          )
        ),
        'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
        'evidence_meta', JSONB_BUILD_OBJECT(
          'has_any_evidence', (
            COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'has_any_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE)
            OR jsonb_array_length(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) > 0
          ),
          'evidence_badges', JSONB_BUILD_ARRAY(
            JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'TIMESHEET' LIMIT 1), FALSE), 'has_evidence', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'TIMESHEET' LIMIT 1), FALSE)),
            JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'MILEAGE' LIMIT 1), FALSE), 'has_evidence', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'MILEAGE' LIMIT 1), FALSE)),
            JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'TRAVEL' LIMIT 1), FALSE), 'has_evidence', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'TRAVEL' LIMIT 1), FALSE)),
            JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'ACCOMMODATION' LIMIT 1), FALSE), 'has_evidence', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'ACCOMMODATION' LIMIT 1), FALSE)),
            JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'OTHER' LIMIT 1), FALSE), 'has_evidence', COALESCE((SELECT TRUE FROM jsonb_array_elements(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)) AS evidence_badge_item(item_json) WHERE UPPER(COALESCE(evidence_badge_item.item_json->>'kind', '')) = 'OTHER' LIMIT 1), FALSE))
          ),
          'attached_evidence_count', GREATEST(
            CASE WHEN COALESCE(row_ids.row_json->>'attached_evidence_count', '') ~ '^[0-9]+$' THEN (row_ids.row_json->>'attached_evidence_count')::integer ELSE 0 END,
            COALESCE(jsonb_array_length(COALESCE(evidence_payload.evidence_json, '[]'::jsonb)), 0)
          ),
          'queue_staged_count', CASE WHEN COALESCE(row_ids.row_json->>'queue_staged_count', '') ~ '^[0-9]+$' THEN (row_ids.row_json->>'queue_staged_count')::integer ELSE 0 END,
          'evidence_document_locked', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'evidence_document_locked', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'evidence_lock_reason', row_ids.row_json->>'evidence_lock_reason'
        ),
        'primary_artifact', CASE
          WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'id', row_ids.row_json->>'primary_artifact_id',
            'kind', row_ids.row_json->>'primary_artifact_kind',
            'display_name', row_ids.row_json->>'primary_artifact_display_name',
            'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
          )
          ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
        END,
        'primary_artifact_id', row_ids.row_json->>'primary_artifact_id',
        'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_artifact_preview_mode', row_ids.row_json->>'primary_artifact_preview_mode',
        'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', row_ids.row_json->>'route_family',
          'route_subfamily', row_ids.row_json->>'route_subfamily',
          'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'is_import_authoritative', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'compare_block_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'compare_block_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'primary_artifact', CASE
            WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
              'id', row_ids.row_json->>'primary_artifact_id',
              'kind', row_ids.row_json->>'primary_artifact_kind',
              'display_name', row_ids.row_json->>'primary_artifact_display_name',
              'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
              'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
            )
            ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
          END,
          'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
          'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
          'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb)
        )
      )
      || JSONB_BUILD_OBJECT(
        'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
        'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
        'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
        'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
        'pay_state', NULL::jsonb,
        'segment_snoozes', '[]'::jsonb,
        'is_advanced', FALSE,
        'can_unadvance', FALSE,
        'advanced_consumed_by_batch_id', NULL::text,
        'is_snoozed', FALSE,
        'snooze_until_date', NULL::text,
        'snooze_is_indefinite', FALSE,
        'snooze_note', NULL::text
      ) AS payload_json
    FROM decision_row
    CROSS JOIN row_ids
    LEFT JOIN timesheet_payload ON TRUE
    LEFT JOIN tsfin_payload ON TRUE
    LEFT JOIN contract_week_payload ON TRUE
    LEFT JOIN validation_payload ON TRUE
    LEFT JOIN evidence_payload ON TRUE
    LEFT JOIN related_payload ON TRUE
  ),
  effective_evidence_payload AS (
    SELECT
      base_payload.payload_json,
      COALESCE(LOWER(NULLIF(BTRIM(COALESCE(base_payload.payload_json#>>'{evidence_meta,has_any_evidence}', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) AS effective_has_any_evidence,
      COALESCE(base_payload.payload_json#>'{evidence_meta,evidence_badges}', '[]'::jsonb) AS effective_evidence_badges,
      COALESCE(base_payload.payload_json->'primary_artifact', base_payload.payload_json#>'{details,primary_artifact}', NULL::jsonb) AS effective_primary_artifact,
      COALESCE(
        NULLIF(BTRIM(COALESCE(base_payload.payload_json->>'primary_artifact_storage_key', '')), ''),
        NULLIF(BTRIM(COALESCE(base_payload.payload_json#>>'{primary_artifact,storage_key}', '')), ''),
        NULLIF(BTRIM(COALESCE(base_payload.payload_json#>>'{details,primary_artifact,storage_key}', '')), ''),
        NULLIF(BTRIM(COALESCE(base_payload.payload_json->>'preview_storage_key', '')), '')
      ) AS effective_primary_artifact_storage_key,
      COALESCE(
        NULLIF(BTRIM(COALESCE(base_payload.payload_json->>'primary_artifact_preview_mode', '')), ''),
        NULLIF(BTRIM(COALESCE(base_payload.payload_json#>>'{primary_artifact,preview_mode}', '')), ''),
        NULLIF(BTRIM(COALESCE(base_payload.payload_json#>>'{details,primary_artifact,preview_mode}', '')), '')
      ) AS effective_primary_artifact_preview_mode
    FROM base_payload
  ),
  final_payload AS (
    SELECT
      effective_evidence_payload.payload_json
      || JSONB_BUILD_OBJECT(
        'row',
          COALESCE(effective_evidence_payload.payload_json->'row', JSONB_BUILD_OBJECT())
          || JSONB_BUILD_OBJECT(
            'has_any_evidence', effective_evidence_payload.effective_has_any_evidence,
            'evidence_badges', effective_evidence_payload.effective_evidence_badges,
            'primary_artifact', effective_evidence_payload.effective_primary_artifact,
            'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode
          ),
        'data_row',
          COALESCE(effective_evidence_payload.payload_json->'row', JSONB_BUILD_OBJECT())
          || JSONB_BUILD_OBJECT(
            'has_any_evidence', effective_evidence_payload.effective_has_any_evidence,
            'evidence_badges', effective_evidence_payload.effective_evidence_badges,
            'primary_artifact', effective_evidence_payload.effective_primary_artifact,
            'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode
          ),
        'row_patch',
          COALESCE(effective_evidence_payload.payload_json->'row_patch', JSONB_BUILD_OBJECT())
          || JSONB_BUILD_OBJECT(
            'has_any_evidence', effective_evidence_payload.effective_has_any_evidence,
            'evidence_badges', effective_evidence_payload.effective_evidence_badges,
            'primary_artifact', effective_evidence_payload.effective_primary_artifact,
            'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode
          ),
        'artifact_hints',
          COALESCE(effective_evidence_payload.payload_json->'artifact_hints', JSONB_BUILD_OBJECT())
          || JSONB_BUILD_OBJECT(
            'has_any_evidence', effective_evidence_payload.effective_has_any_evidence,
            'evidence_badges', effective_evidence_payload.effective_evidence_badges,
            'primary_artifact', effective_evidence_payload.effective_primary_artifact,
            'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode
          ),
        'details',
          COALESCE(effective_evidence_payload.payload_json->'details', JSONB_BUILD_OBJECT())
          || JSONB_BUILD_OBJECT(
            'primary_artifact', effective_evidence_payload.effective_primary_artifact,
            'preview_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
            'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode,
            'evidence_meta', COALESCE(effective_evidence_payload.payload_json->'evidence_meta', JSONB_BUILD_OBJECT()),
            'artifact_hints',
              COALESCE(effective_evidence_payload.payload_json#>'{details,artifact_hints}', JSONB_BUILD_OBJECT())
              || JSONB_BUILD_OBJECT(
                'has_any_evidence', effective_evidence_payload.effective_has_any_evidence,
                'evidence_badges', effective_evidence_payload.effective_evidence_badges,
                'primary_artifact', effective_evidence_payload.effective_primary_artifact,
                'primary_artifact_storage_key', effective_evidence_payload.effective_primary_artifact_storage_key,
                'primary_artifact_preview_mode', effective_evidence_payload.effective_primary_artifact_preview_mode
              )
          ),
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', effective_evidence_payload.payload_json->>'route_family',
          'route_subfamily', effective_evidence_payload.payload_json->>'route_subfamily',
          'underlying_channel_family', effective_evidence_payload.payload_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(effective_evidence_payload.payload_json->>'is_import_authoritative', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'compare_block_required', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(effective_evidence_payload.payload_json->>'compare_block_required', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
          'primary_artifact', effective_evidence_payload.effective_primary_artifact,
          'source_items', '[]'::jsonb,
          'evidence', COALESCE(effective_evidence_payload.payload_json->'evidence', '[]'::jsonb),
          'evidence_meta', COALESCE(effective_evidence_payload.payload_json->'evidence_meta', JSONB_BUILD_OBJECT()),
          'primary_left_pane_mode', effective_evidence_payload.payload_json->>'primary_left_pane_mode'
        )
      ) AS payload_json
    FROM effective_evidence_payload
  )
  SELECT final_payload.payload_json
    INTO v_out
  FROM final_payload;

  IF v_out IS NOT NULL THEN
    v_out := v_out || JSONB_BUILD_OBJECT(
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'evidence_loaded', COALESCE(v_include_evidence, FALSE),
      'compare_loaded', COALESCE(v_include_compare, FALSE),
      'full_loaded', (v_profile = 'full'),
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'soft_failure', FALSE,
      'context_degraded', FALSE,
      'degraded_reason', NULL::text,
      'loaded_layers', CASE
        WHEN v_profile = 'full' THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence', 'compare_import', 'full')
        WHEN COALESCE(v_include_evidence, FALSE) = TRUE AND COALESCE(v_include_compare, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence', 'compare_import')
        WHEN COALESCE(v_include_evidence, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence')
        WHEN COALESCE(v_include_compare, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'compare_import')
        ELSE JSONB_BUILD_ARRAY('header', 'editor')
      END
    );
  END IF;

  IF v_out IS NOT NULL AND COALESCE(v_include_evidence, FALSE) = TRUE THEN
    SELECT
      (COALESCE(jsonb_array_length(COALESCE(v_out->'evidence', '[]'::jsonb)), 0) > 0),
      COALESCE((
        SELECT TRUE
        FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS evidence_item(item_json)
        WHERE UPPER(COALESCE(evidence_item.item_json->>'kind', evidence_item.item_json->>'staged_kind', '')) = 'TIMESHEET'
        LIMIT 1
      ), FALSE),
      COALESCE((
        SELECT TRUE
        FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS evidence_item(item_json)
        WHERE UPPER(COALESCE(evidence_item.item_json->>'kind', evidence_item.item_json->>'staged_kind', '')) = 'MILEAGE'
        LIMIT 1
      ), FALSE),
      COALESCE((
        SELECT TRUE
        FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS evidence_item(item_json)
        WHERE UPPER(COALESCE(evidence_item.item_json->>'kind', evidence_item.item_json->>'staged_kind', '')) = 'TRAVEL'
        LIMIT 1
      ), FALSE),
      COALESCE((
        SELECT TRUE
        FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS evidence_item(item_json)
        WHERE UPPER(COALESCE(evidence_item.item_json->>'kind', evidence_item.item_json->>'staged_kind', '')) = 'ACCOMMODATION'
        LIMIT 1
      ), FALSE),
      COALESCE((
        SELECT TRUE
        FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS evidence_item(item_json)
        WHERE UPPER(COALESCE(evidence_item.item_json->>'kind', evidence_item.item_json->>'staged_kind', '')) = 'OTHER'
        LIMIT 1
      ), FALSE)
    INTO
      v_effective_has_any_evidence,
      v_effective_badge_timesheet,
      v_effective_badge_mileage,
      v_effective_badge_travel,
      v_effective_badge_accommodation,
      v_effective_badge_other;

    v_effective_attached_evidence_count := COALESCE(jsonb_array_length(COALESCE(v_out->'evidence', '[]'::jsonb)), 0);

    v_effective_primary_artifact := COALESCE(v_out->'primary_artifact', v_out#>'{details,primary_artifact}', NULL::jsonb);
    v_effective_primary_artifact_storage_key := NULLIF(BTRIM(COALESCE(
      v_out->>'primary_artifact_storage_key',
      v_effective_primary_artifact->>'storage_key',
      v_effective_primary_artifact->>'r2_key',
      v_out->>'preview_storage_key',
      ''
    )), '');
    v_effective_primary_artifact_preview_mode := NULLIF(BTRIM(COALESCE(
      v_out->>'primary_artifact_preview_mode',
      v_effective_primary_artifact->>'preview_mode',
      ''
    )), '');

    IF v_effective_primary_artifact_storage_key IS NULL
       OR NOT EXISTS (
         SELECT 1
         FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) AS primary_evidence_item(item_json)
         WHERE NULLIF(regexp_replace(BTRIM(COALESCE(primary_evidence_item.item_json->>'storage_key', primary_evidence_item.item_json->>'r2_key', '')), '^/+', ''), '') =
               NULLIF(regexp_replace(BTRIM(COALESCE(v_effective_primary_artifact_storage_key, '')), '^/+', ''), '')
         LIMIT 1
       ) THEN
      SELECT evidence_item.item_json
        INTO v_effective_primary_artifact
      FROM jsonb_array_elements(COALESCE(v_out->'evidence', '[]'::jsonb)) WITH ORDINALITY AS evidence_item(item_json, ordinal_position)
      WHERE NULLIF(BTRIM(COALESCE(evidence_item.item_json->>'storage_key', evidence_item.item_json->>'r2_key', '')), '') IS NOT NULL
      ORDER BY evidence_item.ordinal_position ASC
      LIMIT 1;

      v_effective_primary_artifact_storage_key := NULLIF(BTRIM(COALESCE(v_effective_primary_artifact->>'storage_key', v_effective_primary_artifact->>'r2_key', '')), '');
      v_effective_primary_artifact_preview_mode := NULLIF(BTRIM(COALESCE(v_effective_primary_artifact->>'preview_mode', '')), '');
    END IF;

    v_effective_primary_artifact_id := NULLIF(BTRIM(COALESCE(
      v_effective_primary_artifact->>'id',
      v_effective_primary_artifact->>'evidence_id',
      v_effective_primary_artifact->>'queue_id',
      ''
    )), '');
    v_effective_primary_artifact_kind := NULLIF(BTRIM(COALESCE(
      v_effective_primary_artifact->>'kind',
      v_effective_primary_artifact->>'staged_kind',
      ''
    )), '');
    v_effective_primary_artifact_display_name := NULLIF(BTRIM(COALESCE(
      v_effective_primary_artifact->>'display_name',
      v_effective_primary_artifact->>'filename',
      v_effective_primary_artifact->>'original_filename',
      ''
    )), '');

    v_effective_uploaded_pdf_r2_key := CASE
      WHEN NULLIF(BTRIM(COALESCE(
        v_out->>'timesheet_id',
        v_out->>'current_timesheet_id',
        v_out#>>'{row,timesheet_id}',
        v_out#>>'{row,current_timesheet_id}',
        v_out#>>'{data_row,timesheet_id}',
        v_out#>>'{data_row,current_timesheet_id}',
        ''
      )), '') IS NULL THEN NULLIF(BTRIM(COALESCE(
        v_out->>'uploaded_pdf_r2_key',
        v_out#>>'{row,uploaded_pdf_r2_key}',
        v_out#>>'{data_row,uploaded_pdf_r2_key}',
        v_out#>>'{details,uploaded_pdf_r2_key}',
        v_out#>>'{details,contract_week,uploaded_pdf_r2_key}',
        ''
      )), '')
      ELSE NULL::text
    END;

    v_effective_evidence_badges := JSONB_BUILD_ARRAY(
      JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(v_effective_badge_timesheet, FALSE), 'has_evidence', COALESCE(v_effective_badge_timesheet, FALSE)),
      JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(v_effective_badge_mileage, FALSE), 'has_evidence', COALESCE(v_effective_badge_mileage, FALSE)),
      JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(v_effective_badge_travel, FALSE), 'has_evidence', COALESCE(v_effective_badge_travel, FALSE)),
      JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(v_effective_badge_accommodation, FALSE), 'has_evidence', COALESCE(v_effective_badge_accommodation, FALSE)),
      JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(v_effective_badge_other, FALSE), 'has_evidence', COALESCE(v_effective_badge_other, FALSE))
    );

    v_effective_evidence_meta := COALESCE(v_out->'evidence_meta', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0)
    );

    v_effective_artifact_hints := COALESCE(v_out->'artifact_hints', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'primary_artifact', COALESCE(v_effective_primary_artifact, JSONB_BUILD_OBJECT()),
      'primary_artifact_id', v_effective_primary_artifact_id,
      'primary_artifact_kind', v_effective_primary_artifact_kind,
      'primary_artifact_display_name', v_effective_primary_artifact_display_name,
      'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
      'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode
    );

    v_effective_action_flags := COALESCE(v_out->'action_flags', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0)
    );

    v_effective_row_patch := COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'artifact_hints', v_effective_artifact_hints,
      'primary_artifact', COALESCE(v_effective_primary_artifact, JSONB_BUILD_OBJECT()),
      'primary_artifact_id', v_effective_primary_artifact_id,
      'primary_artifact_kind', v_effective_primary_artifact_kind,
      'primary_artifact_display_name', v_effective_primary_artifact_display_name,
      'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
      'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
      'preview_storage_key', v_effective_primary_artifact_storage_key,
      'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key
    );

    v_effective_row := COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'primary_artifact', COALESCE(v_effective_primary_artifact, JSONB_BUILD_OBJECT()),
      'primary_artifact_id', v_effective_primary_artifact_id,
      'primary_artifact_kind', v_effective_primary_artifact_kind,
      'primary_artifact_display_name', v_effective_primary_artifact_display_name,
      'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
      'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
      'preview_storage_key', v_effective_primary_artifact_storage_key,
      'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key,
      'artifact_hints', v_effective_artifact_hints,
      'action_flags', COALESCE(v_out->'row'->'action_flags', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
        'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
        'evidence_badges', v_effective_evidence_badges
      ),
      'row_patch', v_effective_row_patch
    );

    v_effective_data_row := COALESCE(v_out->'data_row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'primary_artifact', COALESCE(v_effective_primary_artifact, JSONB_BUILD_OBJECT()),
      'primary_artifact_id', v_effective_primary_artifact_id,
      'primary_artifact_kind', v_effective_primary_artifact_kind,
      'primary_artifact_display_name', v_effective_primary_artifact_display_name,
      'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
      'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
      'preview_storage_key', v_effective_primary_artifact_storage_key,
      'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key,
      'artifact_hints', v_effective_artifact_hints,
      'action_flags', COALESCE(v_out->'data_row'->'action_flags', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
        'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
        'evidence_badges', v_effective_evidence_badges
      ),
      'row_patch', v_effective_row_patch
    );

    v_effective_details := COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'evidence', COALESCE(v_out->'evidence', '[]'::jsonb),
      'evidence_meta', v_effective_evidence_meta,
      'primary_artifact', COALESCE(v_effective_primary_artifact, JSONB_BUILD_OBJECT()),
      'primary_artifact_id', v_effective_primary_artifact_id,
      'primary_artifact_kind', v_effective_primary_artifact_kind,
      'primary_artifact_display_name', v_effective_primary_artifact_display_name,
      'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
      'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
      'preview_storage_key', v_effective_primary_artifact_storage_key,
      'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key,
      'artifact_hints', v_effective_artifact_hints,
      'action_flags', COALESCE(v_out->'details'->'action_flags', v_effective_action_flags) || JSONB_BUILD_OBJECT(
        'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
        'evidence_badges', v_effective_evidence_badges
      )
    );

    v_effective_left_pane := COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
      'evidence', COALESCE(v_out->'evidence', '[]'::jsonb),
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'primary_artifact', COALESCE(v_effective_primary_artifact, JSONB_BUILD_OBJECT()),
      'primary_artifact_id', v_effective_primary_artifact_id,
      'primary_artifact_kind', v_effective_primary_artifact_kind,
      'primary_artifact_display_name', v_effective_primary_artifact_display_name,
      'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
      'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
      'preview_storage_key', v_effective_primary_artifact_storage_key,
      'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key,
      'artifact_hints', v_effective_artifact_hints
    );

    v_out := v_out || JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(v_effective_has_any_evidence, FALSE),
      'evidence_badges', v_effective_evidence_badges,
      'attached_evidence_count', COALESCE(v_effective_attached_evidence_count, 0),
      'primary_artifact', COALESCE(v_effective_primary_artifact, JSONB_BUILD_OBJECT()),
      'primary_artifact_id', v_effective_primary_artifact_id,
      'primary_artifact_kind', v_effective_primary_artifact_kind,
      'primary_artifact_display_name', v_effective_primary_artifact_display_name,
      'primary_artifact_storage_key', v_effective_primary_artifact_storage_key,
      'primary_artifact_preview_mode', v_effective_primary_artifact_preview_mode,
      'preview_storage_key', v_effective_primary_artifact_storage_key,
      'uploaded_pdf_r2_key', v_effective_uploaded_pdf_r2_key,
      'evidence_meta', v_effective_evidence_meta,
      'artifact_hints', v_effective_artifact_hints,
      'action_flags', v_effective_action_flags,
      'row_patch', v_effective_row_patch,
      'row', v_effective_row,
      'data_row', v_effective_data_row,
      'details', v_effective_details,
      'left_pane', v_effective_left_pane
    );
  END IF;


  IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
    v_canonical_row_json := NULL;
    v_canonical_row_signature := NULL;
    v_canonical_row_key := NULLIF(BTRIM(COALESCE(v_out->>'row_key', v_out#>>'{row,row_key}', v_out#>>'{data_row,row_key}', v_out#>>'{row_patch,row_key}', '')), '');
    v_canonical_timesheet_id := NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', v_out->>'current_timesheet_id', v_out#>>'{row,timesheet_id}', v_out#>>'{row,current_timesheet_id}', v_out#>>'{data_row,timesheet_id}', v_out#>>'{data_row,current_timesheet_id}', v_out#>>'{row_patch,timesheet_id}', v_out#>>'{row_patch,current_timesheet_id}', '')), '');
    v_canonical_contract_week_id := CASE
      WHEN v_canonical_timesheet_id IS NULL THEN NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', v_out#>>'{row,contract_week_id}', v_out#>>'{data_row,contract_week_id}', v_out#>>'{row_patch,contract_week_id}', '')), '')
      ELSE NULL
    END;

    IF v_canonical_row_key IS NOT NULL OR v_canonical_timesheet_id IS NOT NULL OR v_canonical_contract_week_id IS NOT NULL THEN
      SELECT canonical_result.row_json
      INTO v_canonical_row_json
      FROM public.bulk_timesheet_row_patch_v1(
        JSONB_STRIP_NULLS(
          JSONB_BUILD_OBJECT(
            'dataset_mode', 'process',
            'projection', 'active_row_header',
            'profile', COALESCE(NULLIF(BTRIM(v_profile), ''), 'status_header'),
            'row_key', v_canonical_row_key,
            'timesheet_id', v_canonical_timesheet_id,
            'current_timesheet_id', v_canonical_timesheet_id,
            'requested_timesheet_id', v_canonical_timesheet_id,
            'expected_timesheet_id', v_canonical_timesheet_id,
            'contract_week_id', v_canonical_contract_week_id
          )
        )
      ) AS canonical_result(row_json)
      WHERE canonical_result.row_json IS NOT NULL
      ORDER BY canonical_result.row_json->>'row_key'
      LIMIT 1;

      v_canonical_row_signature := NULLIF(BTRIM(COALESCE(v_canonical_row_json->>'row_signature', '')), '');
      IF v_canonical_row_signature IS NOT NULL THEN
        v_out := v_out || JSONB_BUILD_OBJECT(
          'row_signature', v_canonical_row_signature,
          'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
          'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
          'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature),
          'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_signature', v_canonical_row_signature)
        );
      END IF;
    END IF;
  END IF;

  IF v_out IS NOT NULL THEN
    RETURN public.bulk_process_retention_contract_patch_v1(v_out);
  END IF;

  RETURN JSONB_BUILD_OBJECT(
    'ok', FALSE,
    'context_kind', 'bulk_process_row_context',
    'context_profile', v_profile,
    'profile', v_profile,
    'soft_failure', TRUE,
    'context_degraded', TRUE,
    'degraded_reason', 'ROW_NOT_FOUND',
    'header_loaded', FALSE,
    'header_only', FALSE,
    'editor_loaded', FALSE,
    'evidence_loaded', FALSE,
    'compare_loaded', FALSE,
    'full_loaded', FALSE,
    'schedule_pending', TRUE,
    'schedule_authoritative', FALSE,
    'loaded_layers', '[]'::jsonb,
    'error', 'ROW_NOT_FOUND',
    'message', 'No bulk process row context was found for the supplied identity',
    'filters', v_filters
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.bulk_authorise_row_context_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_decision_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_has_identity boolean := FALSE;
  v_row_key text := NULL;
  v_id_text text := NULL;
  v_timesheet_id_text text := NULL;
  v_contract_week_id_text text := NULL;
  v_include_evidence boolean := FALSE;
  v_include_compare boolean := FALSE;
  v_include_import_source_rows boolean := FALSE;
  v_profile text := NULL;
  v_base_only boolean := FALSE;
  v_header_row_json jsonb := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_out jsonb;
  v_editor_layer jsonb := NULL;
  v_evidence_layer jsonb := NULL;
  v_compare_layer jsonb := NULL;
  v_layer_errors jsonb := '[]'::jsonb;
  v_layer_names jsonb := '[]'::jsonb;
  v_canonical_authorise_row_json jsonb := NULL;
  v_canonical_authorise_row_signature text := NULL;
  v_canonical_lifecycle_overlay jsonb := '{}'::jsonb;
  v_canonical_action_flags jsonb := '{}'::jsonb;
  v_canonical_row_patch jsonb := '{}'::jsonb;
  v_canonical_is_archived boolean := FALSE;
  v_canonical_retained boolean := FALSE;
  v_canonical_can_unprocess boolean := FALSE;
  v_canonical_unprocess_visible boolean := FALSE;
  v_canonical_unprocess_block_reason text := NULL;
  v_canonical_unprocess_block_message text := NULL;
BEGIN
  v_has_identity := (
    NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'expected_timesheet_id', v_filters->>'expectedTimesheetId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'week_id', v_filters->>'weekId', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '') IS NOT NULL
    OR (v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' AND jsonb_array_length(v_filters->'row_keys') > 0)
    OR (v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' AND jsonb_array_length(v_filters->'timesheet_ids') > 0)
    OR (v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' AND jsonb_array_length(v_filters->'contract_week_ids') > 0)
    OR NULLIF(BTRIM(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds', '')), '') IS NOT NULL
    OR NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds', '')), '') IS NOT NULL
    OR (v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' AND jsonb_array_length(v_filters->'rowKeys') > 0)
    OR (v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' AND jsonb_array_length(v_filters->'timesheetIds') > 0)
    OR (v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' AND jsonb_array_length(v_filters->'contractWeekIds') > 0)
    OR (v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' AND jsonb_array_length(v_filters->'ids') > 0)
    OR NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL
  );

  IF v_has_identity = FALSE THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'context_kind', 'bulk_authorise_row_context',
      'context_profile', 'status_header',
      'profile', 'status_header',
      'soft_failure', TRUE,
      'context_degraded', TRUE,
      'degraded_reason', 'ROW_CONTEXT_IDENTITY_REQUIRED',
      'header_loaded', FALSE,
      'header_only', FALSE,
      'editor_loaded', FALSE,
      'evidence_loaded', FALSE,
      'compare_loaded', FALSE,
      'full_loaded', FALSE,
      'schedule_pending', TRUE,
      'schedule_authoritative', FALSE,
      'loaded_layers', '[]'::jsonb,
      'error', 'ROW_CONTEXT_IDENTITY_REQUIRED',
      'message', 'bulk_authorise_row_context_v1 requires row_key, timesheet_id, or contract_week_id.',
      'filters', v_filters
    );
  END IF;

  v_row_key := NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '');
  v_timesheet_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', v_filters->>'current_timesheet_id', v_filters->>'currentTimesheetId', v_filters->>'requested_timesheet_id', v_filters->>'requestedTimesheetId', v_filters->>'expected_timesheet_id', v_filters->>'expectedTimesheetId', '')), '');
  v_contract_week_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', v_filters->>'week_id', v_filters->>'weekId', '')), '');
  v_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'id', '')), '');

  IF v_row_key IS NULL AND v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT NULLIF(BTRIM(row_key_values.value), '')
      INTO v_row_key
    FROM jsonb_array_elements_text(v_filters->'row_keys') WITH ORDINALITY AS row_key_values(value, ordinal_position)
    WHERE NULLIF(BTRIM(row_key_values.value), '') IS NOT NULL
    ORDER BY row_key_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_row_key IS NULL AND v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT NULLIF(BTRIM(row_key_values.value), '')
      INTO v_row_key
    FROM jsonb_array_elements_text(v_filters->'rowKeys') WITH ORDINALITY AS row_key_values(value, ordinal_position)
    WHERE NULLIF(BTRIM(row_key_values.value), '') IS NOT NULL
    ORDER BY row_key_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_row_key IS NULL AND NULLIF(BTRIM(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys', '')), '') IS NOT NULL THEN
    v_row_key := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'row_keys', v_filters->>'rowKeys'), ',', 1)), '');
  END IF;

  IF v_timesheet_id_text IS NULL AND v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT NULLIF(BTRIM(timesheet_id_values.value), '')
      INTO v_timesheet_id_text
    FROM jsonb_array_elements_text(v_filters->'timesheet_ids') WITH ORDINALITY AS timesheet_id_values(value, ordinal_position)
    WHERE timesheet_id_values.value ~* v_uuid_re
    ORDER BY timesheet_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_timesheet_id_text IS NULL AND v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT NULLIF(BTRIM(timesheet_id_values.value), '')
      INTO v_timesheet_id_text
    FROM jsonb_array_elements_text(v_filters->'timesheetIds') WITH ORDINALITY AS timesheet_id_values(value, ordinal_position)
    WHERE timesheet_id_values.value ~* v_uuid_re
    ORDER BY timesheet_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_timesheet_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds'), ',', 1)), '') ~* v_uuid_re THEN
    v_timesheet_id_text := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'timesheet_ids', v_filters->>'timesheetIds'), ',', 1)), '');
  END IF;

  IF v_contract_week_id_text IS NULL AND v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT NULLIF(BTRIM(contract_week_id_values.value), '')
      INTO v_contract_week_id_text
    FROM jsonb_array_elements_text(v_filters->'contract_week_ids') WITH ORDINALITY AS contract_week_id_values(value, ordinal_position)
    WHERE contract_week_id_values.value ~* v_uuid_re
    ORDER BY contract_week_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_contract_week_id_text IS NULL AND v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT NULLIF(BTRIM(contract_week_id_values.value), '')
      INTO v_contract_week_id_text
    FROM jsonb_array_elements_text(v_filters->'contractWeekIds') WITH ORDINALITY AS contract_week_id_values(value, ordinal_position)
    WHERE contract_week_id_values.value ~* v_uuid_re
    ORDER BY contract_week_id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_contract_week_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds'), ',', 1)), '') ~* v_uuid_re THEN
    v_contract_week_id_text := NULLIF(BTRIM(SPLIT_PART(COALESCE(v_filters->>'contract_week_ids', v_filters->>'contractWeekIds'), ',', 1)), '');
  END IF;

  IF v_id_text IS NULL AND v_filters ? 'ids' AND jsonb_typeof(v_filters->'ids') = 'array' THEN
    SELECT NULLIF(BTRIM(id_values.value), '')
      INTO v_id_text
    FROM jsonb_array_elements_text(v_filters->'ids') WITH ORDINALITY AS id_values(value, ordinal_position)
    WHERE id_values.value ~* v_uuid_re
    ORDER BY id_values.ordinal_position ASC
    LIMIT 1;
  END IF;

  IF v_id_text IS NULL
     AND NULLIF(BTRIM(COALESCE(v_filters->>'ids', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(SPLIT_PART(v_filters->>'ids', ',', 1)), '') ~* v_uuid_re THEN
    v_id_text := NULLIF(BTRIM(SPLIT_PART(v_filters->>'ids', ',', 1)), '');
  END IF;

  IF v_row_key IS NOT NULL AND v_timesheet_id_text IS NULL AND v_row_key LIKE 'timesheet:%' THEN
    v_timesheet_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 11)), '');
  END IF;

  IF v_row_key IS NOT NULL AND v_contract_week_id_text IS NULL AND v_row_key LIKE 'contract_week:%' THEN
    v_contract_week_id_text := NULLIF(BTRIM(SUBSTRING(v_row_key FROM 15)), '');
  END IF;

  IF v_timesheet_id_text IS NULL AND v_contract_week_id_text IS NULL AND v_id_text IS NOT NULL AND v_id_text ~* v_uuid_re THEN
    IF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('contract_week', 'contract-week', 'week', 'contractweek')
       OR LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'is_contract_week_only', v_filters->>'isContractWeekOnly', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN
      v_contract_week_id_text := v_id_text;
    ELSIF LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'identity_kind', v_filters->>'identityKind', v_filters->>'kind', '')), '')) IN ('timesheet', 'timesheet_id', 'timesheet-id', 'ts') THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.timesheets AS identity_ts
      WHERE identity_ts.timesheet_id = v_id_text::uuid
        AND identity_ts.is_current = TRUE
    ) THEN
      v_timesheet_id_text := v_id_text;
    ELSIF EXISTS (
      SELECT 1
      FROM public.contract_weeks AS identity_cw
      WHERE identity_cw.id = v_id_text::uuid
    ) THEN
      v_contract_week_id_text := v_id_text;
    ELSE
      v_timesheet_id_text := v_id_text;
    END IF;
  END IF;

  IF v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('timesheet_id', v_timesheet_id_text);
  END IF;

  IF v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('contract_week_id', v_contract_week_id_text);
  END IF;

  IF v_row_key IS NOT NULL THEN
    v_decision_filters := v_decision_filters || JSONB_BUILD_OBJECT('row_key', v_row_key);
  END IF;

  v_base_only := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'base_only', v_filters->>'baseOnly', '')), '')) IN ('true', '1', 'yes', 'y', 'on');

  v_profile := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'profile', v_filters->>'context_profile', v_filters->>'contextProfile', '')), ''));

  IF v_profile IS NULL THEN
    v_profile := 'status_header';
  END IF;

  IF v_profile NOT IN ('active_row_visible', 'status_header', 'editor', 'evidence', 'compare_import', 'full') THEN
    v_profile := 'status_header';
  END IF;

  v_include_evidence := CASE
    WHEN v_profile = 'evidence' THEN TRUE
    WHEN v_profile = 'full' THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_evidence', v_filters->>'includeEvidence', v_filters->>'load_evidence', v_filters->>'loadEvidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_include_compare := CASE
    WHEN v_profile IN ('compare_import', 'full') THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_compare', v_filters->>'includeCompare', v_filters->>'load_compare', v_filters->>'loadCompare', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;

  v_include_import_source_rows := CASE
    WHEN v_profile IN ('compare_import', 'full') THEN TRUE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'include_import_source_rows', v_filters->>'includeImportSourceRows', v_filters->>'load_import_source_rows', v_filters->>'loadImportSourceRows', '')), '')) IN ('true', '1', 'yes', 'y', 'on') THEN TRUE
    ELSE FALSE
  END;


  IF v_profile = 'active_row_visible' AND (
    COALESCE(v_include_evidence, FALSE) = TRUE
    OR COALESCE(v_include_compare, FALSE) = TRUE
    OR COALESCE(v_include_import_source_rows, FALSE) = TRUE
  ) THEN
    v_layer_errors := '[]'::jsonb;
    v_layer_names := JSONB_BUILD_ARRAY('header', 'editor');

    v_editor_layer := public.bulk_authorise_row_context_v1(
      v_decision_filters
      || JSONB_BUILD_OBJECT(
           'profile', 'editor',
           'context_profile', 'editor',
           'include_evidence', FALSE,
           'include_compare', FALSE,
           'include_import_source_rows', FALSE
         )
    );

    IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_editor_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = FALSE THEN
      RETURN COALESCE(v_editor_layer, JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', 'active_row_visible',
        'profile', 'active_row_visible',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', COALESCE(v_editor_layer->>'degraded_reason', v_editor_layer->>'error', 'EDITOR_LAYER_FAILED'),
        'header_loaded', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_editor_layer->>'header_loaded', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'loaded_layers', COALESCE(v_editor_layer->'loaded_layers', '[]'::jsonb),
        'filters', v_filters
      );
    END IF;

    v_out := v_editor_layer || JSONB_BUILD_OBJECT(
      'context_kind', 'bulk_authorise_row_context',
      'context_profile', 'active_row_visible',
      'profile', 'active_row_visible',
      'context_type', 'bulk_authorise',
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'evidence_loaded', FALSE,
      'compare_loaded', FALSE,
      'full_loaded', FALSE,
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'soft_failure', FALSE,
      'context_degraded', FALSE,
      'degraded_reason', NULL::text,
      'loaded_layers', v_layer_names
    );

    IF COALESCE(v_include_evidence, FALSE) = TRUE THEN
      v_evidence_layer := public.bulk_authorise_row_context_v1(
        v_decision_filters
        || JSONB_BUILD_OBJECT(
             'profile', 'evidence',
             'context_profile', 'evidence',
             'include_evidence', TRUE,
             'include_compare', FALSE,
             'include_import_source_rows', FALSE
           )
      );

      IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
        v_layer_names := v_layer_names || JSONB_BUILD_ARRAY('evidence');
        v_out := v_out
          || JSONB_BUILD_OBJECT(
            'evidence_loaded', TRUE,
            'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
            'attached_evidence', COALESCE(v_evidence_layer->'attached_evidence', v_evidence_layer->'evidence', '[]'::jsonb),
            'attachedRows', COALESCE(v_evidence_layer->'attachedRows', v_evidence_layer->'attached_evidence', v_evidence_layer->'evidence', '[]'::jsonb),
            'primary_artifact', COALESCE(v_evidence_layer->'primary_artifact', v_out->'primary_artifact', v_out#>'{details,primary_artifact}', JSONB_BUILD_OBJECT()),
            'preview_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_out->>'preview_storage_key', '')), '')),
            'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_evidence_layer->>'preview_storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_out->>'primary_artifact_storage_key', '')), '')),
            'primary_artifact_preview_mode', COALESCE(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'primary_artifact_preview_mode', '')), ''), NULLIF(BTRIM(COALESCE(v_out->>'primary_artifact_preview_mode', '')), '')),
            'has_any_evidence', COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_evidence_layer->>'has_any_evidence', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE),
            'attached_evidence_count', CASE WHEN COALESCE(v_evidence_layer->>'attached_evidence_count', '') ~ '^[0-9]+$' THEN (v_evidence_layer->>'attached_evidence_count')::integer ELSE COALESCE(JSONB_ARRAY_LENGTH(COALESCE(v_evidence_layer->'evidence', '[]'::jsonb)), 0) END,
            'evidence_count', CASE WHEN COALESCE(v_evidence_layer->>'evidence_count', '') ~ '^[0-9]+$' THEN (v_evidence_layer->>'evidence_count')::integer ELSE COALESCE(JSONB_ARRAY_LENGTH(COALESCE(v_evidence_layer->'evidence', '[]'::jsonb)), 0) END,
            'evidence_badges', COALESCE(v_evidence_layer->'evidence_badges', v_evidence_layer#>'{evidence_meta,evidence_badges}', '[]'::jsonb),
            'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE),
            'row', COALESCE(v_out->'row', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'row', JSONB_BUILD_OBJECT()),
            'data_row', COALESCE(v_out->'data_row', v_out->'row', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'data_row', v_evidence_layer->'row', JSONB_BUILD_OBJECT()),
            'row_patch', COALESCE(v_out->'row_patch', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'row_patch', JSONB_BUILD_OBJECT()),
            'action_flags', COALESCE(v_out->'action_flags', JSONB_BUILD_OBJECT()) || COALESCE(v_evidence_layer->'action_flags', JSONB_BUILD_OBJECT()),
            'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
              'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
              'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE)
            ),
            'left_pane', COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
              'evidence', COALESCE(v_evidence_layer->'evidence', '[]'::jsonb),
              'evidence_meta', COALESCE(v_evidence_layer->'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE)) || JSONB_BUILD_OBJECT('evidence_loaded', TRUE)
            )
          );
      ELSE
        v_layer_errors := v_layer_errors || JSONB_BUILD_ARRAY(COALESCE(v_evidence_layer->>'degraded_reason', v_evidence_layer->>'error', 'EVIDENCE_LAYER_FAILED'));
        v_out := v_out || JSONB_BUILD_OBJECT(
          'evidence_loaded', FALSE,
          'soft_failure', TRUE,
          'context_degraded', TRUE,
          'degraded_reason', 'EVIDENCE_LAYER_FAILED',
          'evidence_layer_failure', COALESCE(v_evidence_layer, JSONB_BUILD_OBJECT())
        );
      END IF;
    END IF;

    IF COALESCE(v_include_compare, FALSE) = TRUE OR COALESCE(v_include_import_source_rows, FALSE) = TRUE THEN
      v_compare_layer := public.bulk_authorise_row_context_v1(
        v_decision_filters
        || JSONB_BUILD_OBJECT(
             'profile', 'compare_import',
             'context_profile', 'compare_import',
             'include_evidence', FALSE,
             'include_compare', COALESCE(v_include_compare, FALSE),
             'include_import_source_rows', COALESCE(v_include_import_source_rows, FALSE)
           )
      );

      IF COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_compare_layer->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
        v_layer_names := v_layer_names || JSONB_BUILD_ARRAY('compare_import');
        v_out := v_out || JSONB_BUILD_OBJECT(
          'compare_loaded', TRUE,
          'compare', COALESCE(v_compare_layer->'compare', JSONB_BUILD_OBJECT()),
          'compare_payload', COALESCE(v_compare_layer->'compare_payload', v_compare_layer->'compare', JSONB_BUILD_OBJECT()),
          'details', COALESCE(v_out->'details', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
            'source_rows', COALESCE(v_compare_layer#>'{details,source_rows}', v_compare_layer#>'{compare,source_rows}', '[]'::jsonb),
            'external_source_rows_json', COALESCE(v_compare_layer#>'{details,external_source_rows_json}', v_compare_layer#>'{compare,external_source_rows_json}', '[]'::jsonb)
          ),
          'left_pane', COALESCE(v_out->'left_pane', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT(
            'source_items', COALESCE(v_compare_layer#>'{left_pane,source_items}', v_compare_layer#>'{details,source_rows}', v_compare_layer#>'{compare,source_rows}', '[]'::jsonb)
          )
        );
      ELSE
        v_layer_errors := v_layer_errors || JSONB_BUILD_ARRAY(COALESCE(v_compare_layer->>'degraded_reason', v_compare_layer->>'error', 'COMPARE_IMPORT_LAYER_FAILED'));
        v_out := v_out || JSONB_BUILD_OBJECT(
          'compare_loaded', FALSE,
          'soft_failure', TRUE,
          'context_degraded', TRUE,
          'degraded_reason', CASE WHEN JSONB_ARRAY_LENGTH(v_layer_errors) > 0 THEN 'LAYER_FAILURE' ELSE 'COMPARE_IMPORT_LAYER_FAILED' END,
          'compare_import_layer_failure', COALESCE(v_compare_layer, JSONB_BUILD_OBJECT())
        );
      END IF;
    END IF;

    v_out := v_out || JSONB_BUILD_OBJECT(
      'context_kind', 'bulk_authorise_row_context',
      'context_profile', 'active_row_visible',
      'profile', 'active_row_visible',
      'context_type', 'bulk_authorise',
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'full_loaded', FALSE,
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'loaded_layers', v_layer_names,
      'layer_errors', v_layer_errors,
      'filters', v_filters
    );


    v_canonical_authorise_row_json := NULL;
    v_canonical_authorise_row_signature := NULL;

    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      SELECT canonical_patch.row_json,
             canonical_patch.row_json->>'row_signature'
        INTO v_canonical_authorise_row_json,
             v_canonical_authorise_row_signature
      FROM public.bulk_timesheet_row_patch_v1(
        (
          v_decision_filters
          - 'row_key' - 'rowKey' - 'row_keys' - 'rowKeys'
          - 'timesheet_id' - 'timesheetId' - 'timesheet_ids' - 'timesheetIds'
          - 'current_timesheet_id' - 'currentTimesheetId'
          - 'requested_timesheet_id' - 'requestedTimesheetId'
          - 'expected_timesheet_id' - 'expectedTimesheetId'
          - 'contract_week_id' - 'contractWeekId' - 'contract_week_ids' - 'contractWeekIds'
          - 'week_id' - 'weekId' - 'id' - 'ids'
        )
        || jsonb_strip_nulls(JSONB_BUILD_OBJECT(
             'dataset_mode', 'authorise',
             'projection', 'active_row_header',
             'profile', COALESCE(NULLIF(BTRIM(COALESCE(v_out->>'profile', '')), ''), v_profile, 'status_header'),
             'row_key', NULLIF(BTRIM(COALESCE(v_out->>'row_key', '')), ''),
             'timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', '')), ''),
             'current_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'current_timesheet_id', '')), ''),
             'requested_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'requested_timesheet_id', '')), ''),
             'expected_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'expected_timesheet_id', '')), ''),
             'contract_week_id', NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', '')), '')
           ))
      ) AS canonical_patch(row_json)
      WHERE NULLIF(BTRIM(COALESCE(canonical_patch.row_json->>'row_signature', '')), '') IS NOT NULL
      ORDER BY
        CASE
          WHEN NULLIF(BTRIM(COALESCE(v_out->>'row_key', '')), '') IS NOT NULL
           AND canonical_patch.row_json->>'row_key' = v_out->>'row_key'
            THEN 0
          ELSE 1
        END,
        canonical_patch.row_json->>'row_key'
      LIMIT 1;

      IF NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_signature, '')), '') IS NOT NULL THEN
        v_canonical_is_archived := UPPER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'tools_stage', ''))) = 'ARCHIVED';
        v_canonical_retained := LOWER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_can_unprocess := NOT v_canonical_is_archived
          AND NOT v_canonical_retained
          AND LOWER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_unprocess_visible := NOT v_canonical_is_archived
          AND LOWER(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_action_visible',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_action_visible}',
            CASE WHEN v_canonical_retained THEN 'true' ELSE v_canonical_authorise_row_json->>'can_unprocess' END,
            'false'
          ))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_unprocess_block_reason := CASE
          WHEN v_canonical_is_archived THEN 'TIMESHEET_ARCHIVED'
          WHEN v_canonical_retained THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'
          ELSE NULLIF(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_block_reason',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_block_reason}',
            ''
          )), '')
        END;
        v_canonical_unprocess_block_message := CASE
          WHEN v_canonical_is_archived THEN 'Archived timesheets must be Unarchived before lifecycle actions.'
          WHEN v_canonical_retained THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.'
          ELSE NULLIF(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_block_message',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_block_message}',
            ''
          )), '')
        END;

        v_canonical_lifecycle_overlay := jsonb_strip_nulls(
          jsonb_build_object(
            'row_signature', v_canonical_authorise_row_signature,
            'backend_row_signature', COALESCE(NULLIF(BTRIM(v_canonical_authorise_row_json->>'backend_row_signature'), ''), v_canonical_authorise_row_signature),
            'mutation_row_signature', COALESCE(NULLIF(BTRIM(v_canonical_authorise_row_json->>'mutation_row_signature'), ''), v_canonical_authorise_row_signature),
            'summary_stage', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'summary_stage', '')), ''),
            'tools_stage', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'tools_stage', '')), ''),
            'processing_status', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'processing_status', '')), ''),
            'has_retained_financial_history', v_canonical_retained,
            'can_unprocess', v_canonical_can_unprocess,
            'unprocess_action_visible', v_canonical_unprocess_visible,
            'unprocess_block_reason', v_canonical_unprocess_block_reason,
            'unprocess_block_message', v_canonical_unprocess_block_message,
            'permission_state_patch_complete', TRUE,
            'priority_badges_patch_complete', TRUE,
            'lifecycle_authority_complete', TRUE,
            'immediate_lifecycle_patch_available', TRUE,
            'refresh_required', FALSE,
            'requires_affected_row_refresh', FALSE
          )
          || jsonb_build_object(
            'is_archived', v_canonical_is_archived,
            'read_only', v_canonical_is_archived,
            'can_archive', FALSE,
            'can_unarchive', v_canonical_is_archived
          )
        );

        v_canonical_action_flags := COALESCE(v_canonical_authorise_row_json->'action_flags', '{}'::jsonb)
          || jsonb_build_object(
            'has_retained_financial_history', v_canonical_retained,
            'can_unprocess', v_canonical_can_unprocess,
            'unprocess_action_visible', v_canonical_unprocess_visible,
            'unprocess_block_reason', v_canonical_unprocess_block_reason,
            'unprocess_block_message', v_canonical_unprocess_block_message,
            'is_archived', v_canonical_is_archived,
            'read_only', v_canonical_is_archived,
            'can_archive', FALSE,
            'can_unarchive', v_canonical_is_archived,
            'permission_state_patch_complete', TRUE,
            'priority_badges_patch_complete', TRUE,
            'lifecycle_authority_complete', TRUE,
            'immediate_lifecycle_patch_available', TRUE,
            'refresh_required', FALSE,
            'requires_affected_row_refresh', FALSE
          );
        v_canonical_row_patch := COALESCE(v_canonical_authorise_row_json->'row_patch', '{}'::jsonb)
          || v_canonical_lifecycle_overlay;

        v_out := v_out || v_canonical_lifecycle_overlay;
        v_out := JSONB_SET(v_out, '{action_flags}', COALESCE(v_out->'action_flags', '{}'::jsonb) || v_canonical_action_flags, TRUE);
        v_out := JSONB_SET(v_out, '{row_patch}', COALESCE(v_out->'row_patch', '{}'::jsonb) || v_canonical_row_patch, TRUE);

        IF JSONB_TYPEOF(v_out->'row') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{row}',
            COALESCE(v_out->'row', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{row,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{row,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;

        IF JSONB_TYPEOF(v_out->'data_row') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{data_row}',
            COALESCE(v_out->'data_row', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{data_row,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{data_row,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;

        IF JSONB_TYPEOF(v_out->'details') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{details}',
            COALESCE(v_out->'details', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{details,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{details,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;
      END IF;
    END IF;

    RETURN private._candidate_office_context_overlay_v1(v_out);
  END IF;


  IF v_profile IN ('status_header', 'editor') OR (v_profile = 'active_row_visible' AND COALESCE(v_include_evidence, FALSE) = FALSE) THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM (
        SELECT input_ids.*
        FROM input_ids
        WHERE input_ids.input_timesheet_id IS NULL
          AND input_ids.input_contract_week_id IS NULL
      ) AS summary_ids
      CROSS JOIN LATERAL public.timesheet_summary_lightweight_rows_v1(
        v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)
      ) AS summary_source
      WHERE (
          summary_ids.input_timesheet_id IS NULL
          OR summary_source.timesheet_id = summary_ids.input_timesheet_id
        )
        AND (
          summary_ids.input_contract_week_id IS NULL
          OR summary_source.contract_week_id = summary_ids.input_contract_week_id
        )
      ORDER BY
        CASE WHEN summary_ids.input_timesheet_id IS NOT NULL AND summary_source.timesheet_id = summary_ids.input_timesheet_id THEN 0 ELSE 1 END,
        CASE WHEN summary_ids.input_contract_week_id IS NOT NULL AND summary_source.contract_week_id = summary_ids.input_contract_week_id THEN 0 ELSE 1 END,
        summary_source.timesheet_id NULLS LAST,
        summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (
          cw_ids.input_contract_week_id IS NOT NULL
          AND cw0.id = cw_ids.input_contract_week_id
        )
        OR (
          cw_ids.input_timesheet_id IS NOT NULL
          AND cw0.timesheet_id = cw_ids.input_timesheet_id
        )
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    tsfin_row AS (
      SELECT tf0.*
      FROM public.timesheets_financials AS tf0
      LEFT JOIN timesheet_row AS ts_for_tf ON TRUE
      WHERE tf0.is_current = TRUE
        AND ts_for_tf.timesheet_id IS NOT NULL
        AND tf0.timesheet_id = ts_for_tf.timesheet_id
      ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      LIMIT 1
    ),
    contract_row AS (
      SELECT ct0.*
      FROM public.contracts AS ct0
      LEFT JOIN timesheet_row AS ts_for_ct ON TRUE
      LEFT JOIN contract_week_row AS cw_for_ct ON TRUE
      WHERE ct0.id = COALESCE(ts_for_ct.contract_id, cw_for_ct.contract_id)
      ORDER BY ct0.updated_at DESC NULLS LAST, ct0.created_at DESC NULLS LAST, ct0.id DESC
      LIMIT 1
    ),
    candidate_row AS (
      SELECT cand0.*
      FROM public.candidates AS cand0
      LEFT JOIN contract_row AS ct_for_cand ON TRUE
      WHERE cand0.id = ct_for_cand.candidate_id
      LIMIT 1
    ),
    client_row AS (
      SELECT cli0.*
      FROM public.clients AS cli0
      LEFT JOIN contract_row AS ct_for_cli ON TRUE
      WHERE cli0.id = ct_for_cli.client_id
      LIMIT 1
    ),
    validation_rows AS (
      SELECT tv0.*
      FROM public.timesheet_validations AS tv0
      LEFT JOIN timesheet_row AS ts_for_tv ON TRUE
      WHERE ts_for_tv.timesheet_id IS NOT NULL
        AND tv0.timesheet_id = ts_for_tv.timesheet_id
      ORDER BY tv0.validated_at_utc DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
    ),
    validation_payload AS (
      SELECT
        COALESCE(JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'id', validation_rows.id,
            'timesheet_id', validation_rows.timesheet_id,
            'booking_id', validation_rows.booking_id,
            'status', validation_rows.status,
            'reason_code', validation_rows.reason_code,
            'hr_request_id', validation_rows.hr_request_id,
            'validated_at_utc', validation_rows.validated_at_utc,
            'override_requested_at_utc', validation_rows.override_requested_at_utc,
            'override_requested_by', validation_rows.override_requested_by,
            'override_confirmed_at_utc', validation_rows.override_confirmed_at_utc,
            'override_confirmed_by', validation_rows.override_confirmed_by,
            'override_reason', validation_rows.override_reason,
            'override_cleared_at_utc', validation_rows.override_cleared_at_utc,
            'last_source', validation_rows.last_source,
            'created_at', validation_rows.created_at,
            'updated_at', validation_rows.updated_at,
            'hr_request_source', validation_rows.hr_request_source,
            'hr_request_set_by', validation_rows.hr_request_set_by,
            'hr_request_set_at_utc', validation_rows.hr_request_set_at_utc,
            'pre_validated', validation_rows.pre_validated
          ) ORDER BY validation_rows.validated_at_utc DESC NULLS LAST, validation_rows.created_at DESC NULLS LAST, validation_rows.id DESC
        ), '[]'::jsonb) AS validations_json,
        (
          SELECT JSONB_BUILD_OBJECT(
            'id', validation_latest.id,
            'status', validation_latest.status,
            'reason_code', validation_latest.reason_code,
            'hr_request_id', validation_latest.hr_request_id,
            'validated_at_utc', validation_latest.validated_at_utc,
            'pre_validated', COALESCE(validation_latest.pre_validated, FALSE),
            'updated_at', validation_latest.updated_at
          )
          FROM validation_rows AS validation_latest
          ORDER BY validation_latest.validated_at_utc DESC NULLS LAST, validation_latest.created_at DESC NULLS LAST, validation_latest.id DESC
          LIMIT 1
        ) AS latest_validation_json
      FROM validation_rows
    ),
    core_row AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id, contract_row.id) AS resolved_contract_id,
        COALESCE(summary_row.candidate_id, contract_row.candidate_id, candidate_row.id) AS resolved_candidate_id,
        COALESCE(summary_row.client_id, contract_row.client_id, client_row.id) AS resolved_client_id,
        COALESCE(NULLIF(BTRIM(summary_row.candidate_name), ''), NULLIF(BTRIM(summary_row.candidate_display_name), ''), NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', candidate_row.first_name, candidate_row.last_name)), '')) AS candidate_name,
        COALESCE(NULLIF(BTRIM(summary_row.candidate_display_name), ''), NULLIF(BTRIM(summary_row.candidate_name), ''), NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', candidate_row.first_name, candidate_row.last_name)), '')) AS candidate_display_name,
        COALESCE(NULLIF(BTRIM(summary_row.client_name), ''), NULLIF(BTRIM(client_row.name), '')) AS client_name,
        COALESCE(summary_row.week_ending_date, timesheet_row.week_ending_date, contract_week_row.week_ending_date) AS week_ending_date,
        COALESCE(summary_row.work_date, timesheet_row.worked_start_iso::date, contract_week_row.week_ending_date) AS work_date,
        COALESCE(summary_row.booking_id, timesheet_row.booking_id) AS booking_id,
        COALESCE(summary_row.occupant_key_norm, timesheet_row.occupant_key_norm, tsfin_row.occupant_key_norm) AS occupant_key_norm,
        COALESCE(summary_row.hospital_norm, timesheet_row.hospital_norm) AS hospital_norm,
        COALESCE(summary_row.candidate_hint_text, timesheet_row.candidate_hint_text, JSONB_BUILD_OBJECT()) AS candidate_hint_text,
        COALESCE(summary_row.sheet_scope, timesheet_row.sheet_scope::text) AS sheet_scope,
        COALESCE(summary_row.submission_mode, timesheet_row.submission_mode::text) AS submission_mode,
        COALESCE(summary_row.submission_mode_snapshot, contract_week_row.submission_mode_snapshot::text) AS submission_mode_snapshot,
        COALESCE(summary_row.basis, tsfin_row.basis::text) AS basis,
        summary_row.route_type AS route_type,
        summary_row.route_display AS route_display,
        COALESCE(summary_row.route_family, CASE WHEN COALESCE(timesheet_row.qr_status::text, '') <> '' THEN 'QR' WHEN UPPER(COALESCE(timesheet_row.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'ELECTRONIC' THEN 'ELECTRONIC' ELSE 'MANUAL_NON_QR' END) AS route_family,
        summary_row.route_subfamily AS route_subfamily,
        summary_row.underlying_channel_family AS underlying_channel_family,
        COALESCE(summary_row.summary_stage, CASE WHEN timesheet_row.timesheet_id IS NULL THEN 'UNPROCESSED' ELSE 'PROCESSED' END) AS summary_stage,
        COALESCE(
          summary_row.tools_stage,
          CASE
            WHEN timesheet_row.archived_at_utc IS NOT NULL THEN 'ARCHIVED'
            WHEN timesheet_row.timesheet_id IS NULL THEN 'UNPROCESSED'
            ELSE COALESCE(tsfin_row.processing_status::text, timesheet_row.status::text)
          END
        ) AS tools_stage,
        COALESCE(summary_row.processing_status, tsfin_row.processing_status::text) AS processing_status,
        summary_row.processing_status_display AS processing_status_display,
        COALESCE(summary_row.authorised_at_utc, tsfin_row.authorised_at_utc) AS authorised_at_utc,
        COALESCE(summary_row.authorised_at_server, timesheet_row.authorised_at_server) AS authorised_at_server,
        COALESCE(summary_row.processed_at_utc, tsfin_row.processed_at_utc) AS processed_at_utc,
        COALESCE(summary_row.is_authorised, tsfin_row.authorised_at_utc IS NOT NULL, timesheet_row.authorised_at_server IS NOT NULL, FALSE) AS is_authorised,
        COALESCE(summary_row.total_hours, tsfin_row.total_hours) AS total_hours,
        COALESCE(summary_row.total_pay_ex_vat, tsfin_row.total_pay_ex_vat) AS total_pay_ex_vat,
        COALESCE(summary_row.total_charge_ex_vat, tsfin_row.total_charge_ex_vat) AS total_charge_ex_vat,
        COALESCE(summary_row.margin_ex_vat, tsfin_row.margin_ex_vat) AS margin_ex_vat,
        summary_row.net_delta_ex_vat AS net_delta_ex_vat,
        COALESCE(summary_row.paid_at_utc, tsfin_row.paid_at_utc) AS paid_at_utc,
        summary_row.pay_icon_code AS pay_icon_code,
        summary_row.pay_status_code AS pay_status_code,
        summary_row.pay_paid_at_utc AS pay_paid_at_utc,
        COALESCE(summary_row.invoice_is_paid, FALSE) AS invoice_is_paid,
        summary_row.invoice_issue_stage AS invoice_issue_stage,
        summary_row.invoice_segment_stage AS invoice_segment_stage,
        COALESCE(summary_row.invoice_segments_total, 0) AS invoice_segments_total,
        COALESCE(summary_row.invoice_segments_locked, 0) AS invoice_segments_locked,
        COALESCE(summary_row.invoice_segments_unlocked, 0) AS invoice_segments_unlocked,
        COALESCE(TO_JSONB(summary_row.issue_codes), '[]'::jsonb) AS issue_codes_json,
        COALESCE(summary_row.validation_status, validation_payload.latest_validation_json->>'status') AS validation_status,
        summary_row.validation_summary AS validation_summary,
        COALESCE(summary_row.hr_crosscheck_status, tsfin_row.hr_crosscheck_status) AS hr_crosscheck_status,
        COALESCE(TO_JSONB(summary_row.hr_crosscheck_issues), TO_JSONB(tsfin_row.hr_crosscheck_issues), '[]'::jsonb) AS hr_crosscheck_issues_json,
        COALESCE(summary_row.qr_status, timesheet_row.qr_status::text) AS qr_status,
        COALESCE(summary_row.is_qr, timesheet_row.qr_status IS NOT NULL, FALSE) AS is_qr,
        COALESCE(summary_row.is_adjusted, timesheet_row.is_adjustment, contract_week_row.is_adjustment, FALSE) AS is_adjusted,
        COALESCE(summary_row.needs_attention, FALSE) AS needs_attention,
        COALESCE(summary_row.has_rate_issue, tsfin_row.has_rate_issue, FALSE) AS has_rate_issue,
        COALESCE(summary_row.has_pay_channel_issue, tsfin_row.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
        COALESCE(summary_row.client_no_timesheet_required, contract_row.no_timesheet_required, FALSE) AS client_no_timesheet_required,
        COALESCE(summary_row.client_autoprocess_hr, contract_row.autoprocess_hr, FALSE) AS client_autoprocess_hr,
        COALESCE(summary_row.client_is_nhsp, contract_row.is_nhsp, FALSE) AS client_is_nhsp,
        COALESCE(summary_row.has_any_evidence, FALSE) AS has_any_evidence,
        COALESCE(summary_row.attached_evidence_count, 0) AS attached_evidence_count,
        summary_row.primary_artifact_storage_key AS primary_artifact_storage_key,
        summary_row.primary_artifact_display_name AS primary_artifact_display_name,
        summary_row.primary_artifact_preview_mode AS primary_artifact_preview_mode,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.updated_at AS contract_week_updated_at,
        tsfin_row.updated_at AS tsfin_updated_at,
        timesheet_row.version AS timesheet_version,
        timesheet_row.status AS timesheet_status,
        timesheet_row.manual_pdf_r2_key AS manual_pdf_r2_key,
        timesheet_row.qr_r2_key AS qr_r2_key,
        timesheet_row.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
        timesheet_row.generated_pdf_at_utc AS generated_pdf_at_utc,
        timesheet_row.actual_schedule_json AS timesheet_actual_schedule_json,
        timesheet_row.additional_units_week AS timesheet_additional_units_week,
        timesheet_row.additional_units_per_day AS timesheet_additional_units_per_day,
        timesheet_row.sheet_scope AS timesheet_sheet_scope,
        timesheet_row.worked_start_iso AS timesheet_worked_start_iso,
        timesheet_row.worked_end_iso AS timesheet_worked_end_iso,
        timesheet_row.break_start_iso AS timesheet_break_start_iso,
        timesheet_row.break_end_iso AS timesheet_break_end_iso,
        timesheet_row.break_minutes AS timesheet_break_minutes,
        timesheet_row.worked_minutes AS timesheet_worked_minutes,
        timesheet_row.auth_name AS timesheet_auth_name,
        timesheet_row.auth_job_title AS timesheet_auth_job_title,
        timesheet_row.reference_number AS timesheet_reference_number,
        timesheet_row.reference_set_at AS timesheet_reference_set_at,
        timesheet_row.created_at AS timesheet_created_at,
        timesheet_row.is_adjustment AS timesheet_is_adjustment,
        timesheet_row.parent_timesheet_id AS timesheet_parent_timesheet_id,
        timesheet_row.adjustment_origin AS timesheet_adjustment_origin,
        contract_week_row.uploaded_pdf_r2_key AS uploaded_pdf_r2_key,
        contract_week_row.day_entries_json AS contract_week_day_entries_json,
        contract_week_row.totals_json AS contract_week_totals_json,
        contract_week_row.planned_schedule_json AS contract_week_planned_schedule_json,
        contract_week_row.additional_seq AS contract_week_additional_seq,
        contract_week_row.status AS contract_week_status,
        contract_week_row.created_at AS contract_week_created_at,
        contract_week_row.is_adjustment AS contract_week_is_adjustment,
        contract_week_row.enforce_day_partition AS contract_week_enforce_day_partition,
        contract_week_row.allowed_days_mask AS contract_week_allowed_days_mask,
        contract_week_row.split_boundary_date AS contract_week_split_boundary_date,
        contract_week_row.worker_note AS contract_week_worker_note,
        contract_week_row.split_group_key AS contract_week_split_group_key,
        contract_row.id AS contract_id,
        contract_row.role AS contract_role,
        contract_row.band AS contract_band,
        contract_row.display_site AS contract_display_site,
        contract_row.ward_hint AS contract_ward_hint,
        contract_row.default_submission_mode AS contract_default_submission_mode,
        contract_row.std_schedule_json AS contract_std_schedule_json,
        contract_row.additional_rates_json AS contract_additional_rates_json,
        contract_row.weekly_timesheet_source AS contract_weekly_timesheet_source,
        contract_row.no_timesheet_required AS contract_no_timesheet_required,
        contract_row.autoprocess_hr AS contract_autoprocess_hr,
        contract_row.requires_hr AS contract_requires_hr,
        contract_row.hr_attach_to_invoice AS contract_hr_attach_to_invoice,
        contract_row.ts_attach_to_invoice AS contract_ts_attach_to_invoice,
        contract_row.is_nhsp AS contract_is_nhsp,
        candidate_row.id AS candidate_id,
        candidate_row.tms_ref AS candidate_tms_ref,
        candidate_row.first_name AS candidate_first_name,
        candidate_row.last_name AS candidate_last_name,
        candidate_row.display_name AS candidate_display_name_raw,
        candidate_row.email AS candidate_email,
        candidate_row.phone AS candidate_phone,
        candidate_row.key_norm AS candidate_key_norm,
        candidate_row.band AS candidate_band,
        client_row.id AS client_id,
        client_row.cli_ref AS client_cli_ref,
        client_row.name AS client_name_raw,
        client_row.vat_chargeable AS client_vat_chargeable,
        client_row.ts_queries_email AS client_ts_queries_email,
        tsfin_row.id AS tsfin_id,
        tsfin_row.timesheet_version AS tsfin_timesheet_version,
        tsfin_row.basis AS tsfin_basis,
        tsfin_row.is_current AS tsfin_is_current,
        tsfin_row.is_stale AS tsfin_is_stale,
        tsfin_row.stale_reason AS tsfin_stale_reason,
        tsfin_row.actual_schedule_json AS tsfin_actual_schedule_json,
        tsfin_row.actual_minutes_by_day_json AS tsfin_actual_minutes_by_day_json,
        tsfin_row.additional_units_json AS tsfin_additional_units_json,
        tsfin_row.processing_status AS tsfin_processing_status,
        tsfin_row.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
        tsfin_row.locked_at_utc AS tsfin_locked_at_utc,
        tsfin_row.paid_at_utc AS tsfin_paid_at_utc,
        tsfin_row.processed_at_utc AS tsfin_processed_at_utc,
        tsfin_row.authorised_at_utc AS tsfin_authorised_at_utc,
        tsfin_row.external_source_rows_json AS tsfin_external_source_rows_json,
        validation_payload.validations_json AS validations_json,
        validation_payload.latest_validation_json AS latest_validation_json
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN tsfin_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
      LEFT JOIN contract_row ON TRUE
      LEFT JOIN candidate_row ON TRUE
      LEFT JOIN client_row ON TRUE
      LEFT JOIN validation_payload ON TRUE
    ),
    flags AS (
      SELECT
        core_row.*,
        (core_row.resolved_timesheet_id IS NOT NULL OR core_row.resolved_contract_week_id IS NOT NULL) AS row_found,
        (
          UPPER(COALESCE(core_row.tools_stage, '')) = 'ARCHIVED'
          OR core_row.tsfin_locked_by_invoice_id IS NOT NULL
          OR COALESCE(core_row.invoice_segments_locked, 0) > 0
          OR COALESCE(core_row.invoice_is_paid, FALSE) = TRUE
        ) AS locked_bool,
        COALESCE(core_row.is_authorised, FALSE) AS authorised_bool,
        (
          core_row.resolved_timesheet_id IS NULL
          OR UPPER(COALESCE(core_row.processing_status, core_row.tools_stage, core_row.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
        ) AS unprocessed_bool,
        (
          UPPER(COALESCE(core_row.processing_status, '')) = 'PENDING_AUTH'
          OR (
            COALESCE(core_row.contract_requires_hr, FALSE) = TRUE
            AND COALESCE(core_row.contract_autoprocess_hr, FALSE) = FALSE
            AND UPPER(COALESCE(core_row.processing_status, '')) = 'READY_FOR_HR'
          )
        ) AS requires_authorisation_bool,
        (
          UPPER(COALESCE(core_row.qr_status, '')) = 'PENDING'
          AND core_row.resolved_timesheet_id IS NOT NULL
        ) AS qr_unsigned_blocked_bool,
        (
          COALESCE(core_row.timesheet_is_adjustment, core_row.contract_week_is_adjustment, FALSE) = TRUE
          AND COALESCE(core_row.contract_week_additional_seq, 0) > 0
          AND (
            core_row.timesheet_actual_schedule_json IS NULL
            OR core_row.timesheet_actual_schedule_json = '[]'::jsonb
            OR (jsonb_typeof(core_row.timesheet_actual_schedule_json) = 'array' AND jsonb_array_length(core_row.timesheet_actual_schedule_json) = 0)
          )
        ) AS keep_blank_additional_schedule_bool
      FROM core_row
    ),
    row_payload AS (
      SELECT
        JSONB_BUILD_OBJECT(
          'ok', TRUE,
          'context_kind', 'bulk_authorise_row_context',
          'context_profile', v_profile,
          'profile', v_profile,
          'context_type', CASE WHEN 'bulk_authorise_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
          'slim_context', TRUE,
          'header_loaded', TRUE,
          'header_only', (v_profile = 'status_header'),
          'editor_loaded', (v_profile IN ('editor', 'active_row_visible')),
          'evidence_loaded', FALSE,
          'compare_loaded', FALSE,
          'full_loaded', FALSE,
          'schedule_pending', NOT (v_profile IN ('editor', 'active_row_visible')),
          'schedule_authoritative', (v_profile IN ('editor', 'active_row_visible')),
          'loaded_layers', CASE WHEN v_profile = 'status_header' THEN JSONB_BUILD_ARRAY('header') ELSE JSONB_BUILD_ARRAY('header', 'editor') END,
          'soft_failure', FALSE,
          'context_degraded', FALSE,
          'degraded_reason', NULL::text,
          'row_key', CASE WHEN flags.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || flags.resolved_timesheet_id::text ELSE 'contract_week:' || flags.resolved_contract_week_id::text END,
          'timesheet_id', flags.resolved_timesheet_id,
          'current_timesheet_id', flags.resolved_timesheet_id,
          'requested_timesheet_id', flags.resolved_timesheet_id,
          'expected_timesheet_id', flags.resolved_timesheet_id,
          'contract_week_id', flags.resolved_contract_week_id,
          'row_signature', MD5(CONCAT_WS('|', COALESCE(flags.resolved_timesheet_id::text, ''), COALESCE(flags.resolved_contract_week_id::text, ''), COALESCE(flags.timesheet_version::text, ''), COALESCE(flags.timesheet_updated_at::text, ''), COALESCE(flags.contract_week_updated_at::text, ''), COALESCE(flags.tsfin_updated_at::text, ''), v_profile)),
          'filters', v_filters
        )
        || JSONB_BUILD_OBJECT(
          'row', (
            JSONB_BUILD_OBJECT(
              'row_key', CASE WHEN flags.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || flags.resolved_timesheet_id::text ELSE 'contract_week:' || flags.resolved_contract_week_id::text END,
              'timesheet_id', flags.resolved_timesheet_id,
              'current_timesheet_id', flags.resolved_timesheet_id,
              'requested_timesheet_id', flags.resolved_timesheet_id,
              'expected_timesheet_id', flags.resolved_timesheet_id,
              'contract_week_id', flags.resolved_contract_week_id,
              'contract_id', flags.resolved_contract_id,
              'candidate_id', flags.resolved_candidate_id,
              'candidate_name', flags.candidate_name,
              'candidate_display_name', flags.candidate_display_name,
              'client_id', flags.resolved_client_id,
              'client_name', flags.client_name,
              'booking_id', flags.booking_id,
              'occupant_key_norm', flags.occupant_key_norm,
              'hospital_norm', flags.hospital_norm,
              'candidate_hint_text', flags.candidate_hint_text,
              'week_ending_date', flags.week_ending_date,
              'work_date', flags.work_date,
              'sheet_scope', flags.sheet_scope,
              'submission_mode', flags.submission_mode,
              'submission_mode_snapshot', flags.submission_mode_snapshot,
              'basis', flags.basis,
              'status', flags.timesheet_status,
              'contract_week_status', flags.contract_week_status,
              'route_type', flags.route_type,
              'route_display', flags.route_display,
              'route_family', flags.route_family,
              'route_subfamily', flags.route_subfamily,
              'underlying_channel_family', flags.underlying_channel_family,
              'summary_stage', flags.summary_stage,
              'tools_stage', flags.tools_stage,
              'processing_status', flags.processing_status,
              'processing_status_display', flags.processing_status_display,
              'authorised_at_utc', flags.authorised_at_utc,
              'authorised_at_server', flags.authorised_at_server,
              'processed_at_utc', flags.processed_at_utc,
              'is_authorised', flags.authorised_bool,
              'locked', flags.locked_bool,
              'has_timesheet', flags.resolved_timesheet_id IS NOT NULL
            )
            || JSONB_BUILD_OBJECT(
              'total_hours', flags.total_hours,
              'total_pay_ex_vat', flags.total_pay_ex_vat,
              'total_charge_ex_vat', flags.total_charge_ex_vat,
              'margin_ex_vat', flags.margin_ex_vat,
              'net_delta_ex_vat', flags.net_delta_ex_vat,
              'paid_at_utc', flags.paid_at_utc,
              'pay_icon_code', flags.pay_icon_code,
              'pay_status_code', flags.pay_status_code,
              'pay_paid_at_utc', flags.pay_paid_at_utc,
              'invoice_is_paid', flags.invoice_is_paid,
              'invoice_issue_stage', flags.invoice_issue_stage,
              'invoice_segment_stage', flags.invoice_segment_stage,
              'invoice_segments_total', flags.invoice_segments_total,
              'invoice_segments_locked', flags.invoice_segments_locked,
              'invoice_segments_unlocked', flags.invoice_segments_unlocked,
              'issue_codes', flags.issue_codes_json,
              'validation_status', flags.validation_status,
              'validation_summary', flags.validation_summary,
              'hr_crosscheck_status', flags.hr_crosscheck_status,
              'hr_crosscheck_issues', flags.hr_crosscheck_issues_json,
              'qr_status', flags.qr_status,
              'is_qr', flags.is_qr,
              'is_adjusted', flags.is_adjusted,
              'needs_attention', flags.needs_attention,
              'has_rate_issue', flags.has_rate_issue,
              'has_pay_channel_issue', flags.has_pay_channel_issue,
              'client_no_timesheet_required', flags.client_no_timesheet_required,
              'client_autoprocess_hr', flags.client_autoprocess_hr,
              'client_is_nhsp', flags.client_is_nhsp
            )
            || JSONB_BUILD_OBJECT(
              'has_any_evidence', flags.has_any_evidence,
              'attached_evidence_count', flags.attached_evidence_count,
              'evidence_count', flags.attached_evidence_count,
              'primary_artifact_storage_key', flags.primary_artifact_storage_key,
              'primary_artifact_display_name', flags.primary_artifact_display_name,
              'primary_artifact_preview_mode', flags.primary_artifact_preview_mode,
              'manual_pdf_r2_key', flags.manual_pdf_r2_key,
              'qr_r2_key', flags.qr_r2_key,
              'uploaded_pdf_r2_key', flags.uploaded_pdf_r2_key,
              'generated_pdf_at_utc', flags.generated_pdf_at_utc,
              'manual_pdf_rotation_degrees', flags.manual_pdf_rotation_degrees,
              'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
              'additional_seq', flags.contract_week_additional_seq,
              'period_type', CASE WHEN flags.resolved_contract_week_id IS NOT NULL THEN 'WEEKLY' ELSE 'DAILY' END,
              'suppress_standard_schedule_fallback', flags.keep_blank_additional_schedule_bool,
              'keep_additional_manual_adjustment_schedule_empty', flags.keep_blank_additional_schedule_bool,
              '__suppressStandardScheduleFallback', flags.keep_blank_additional_schedule_bool,
              '__keepAdditionalManualAdjustmentScheduleEmpty', flags.keep_blank_additional_schedule_bool
            )
            || CASE WHEN v_profile IN ('editor', 'active_row_visible') THEN JSONB_BUILD_OBJECT(
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'contract_week_totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
              'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
              'additional_units_json', COALESCE(flags.tsfin_additional_units_json, flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb)
            ) ELSE JSONB_BUILD_OBJECT() END
            || JSONB_BUILD_OBJECT(
              'action_flags', JSONB_BUILD_OBJECT(
                'can_save', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
                'can_process', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = TRUE),
                'can_unprocess', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = FALSE),
                'can_bulk_authorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.requires_authorisation_bool = TRUE AND flags.authorised_bool = FALSE AND flags.qr_unsigned_blocked_bool = FALSE),
                'can_bulk_unauthorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = TRUE),
                'can_edit_timesheet_data', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
                'can_manage_evidence', ((flags.resolved_timesheet_id IS NOT NULL OR (flags.resolved_contract_week_id IS NOT NULL AND flags.route_family = 'MANUAL_NON_QR')) AND flags.locked_bool = FALSE AND flags.route_family <> 'IMPORT_AUTHORITATIVE'),
                'can_add_additional_manual', (flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND COALESCE(flags.is_adjusted, FALSE) = FALSE),
                'review_only', (flags.locked_bool = TRUE OR flags.authorised_bool = TRUE OR flags.route_family <> 'MANUAL_NON_QR'),
                'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
                'additional_seq', flags.contract_week_additional_seq,
                'has_any_evidence', flags.has_any_evidence,
                'attached_evidence_count', flags.attached_evidence_count
              ),
              'row_patch', JSONB_BUILD_OBJECT(),
              'artifact_hints', JSONB_BUILD_OBJECT(
                'has_any_evidence', flags.has_any_evidence,
                'attached_evidence_count', flags.attached_evidence_count,
                'primary_artifact_storage_key', flags.primary_artifact_storage_key,
                'primary_artifact_display_name', flags.primary_artifact_display_name,
                'primary_artifact_preview_mode', flags.primary_artifact_preview_mode
              ),
              'evidence_badges', JSONB_BUILD_ARRAY(
                JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(flags.has_any_evidence, FALSE), 'has_evidence', COALESCE(flags.has_any_evidence, FALSE)),
                JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', FALSE, 'has_evidence', FALSE),
                JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', FALSE, 'has_evidence', FALSE)
              )
            )
          ),
          'evidence', '[]'::jsonb
        )
        || JSONB_BUILD_OBJECT(
          'row_patch', JSONB_BUILD_OBJECT(),
          'action_flags', JSONB_BUILD_OBJECT(
            'can_save', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
            'can_process', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = TRUE),
            'can_unprocess', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR' AND flags.unprocessed_bool = FALSE),
            'can_bulk_authorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.requires_authorisation_bool = TRUE AND flags.authorised_bool = FALSE AND flags.qr_unsigned_blocked_bool = FALSE),
            'can_bulk_unauthorise', (flags.resolved_timesheet_id IS NOT NULL AND flags.locked_bool = FALSE AND flags.authorised_bool = TRUE),
            'can_edit_timesheet_data', ((flags.resolved_timesheet_id IS NOT NULL OR flags.resolved_contract_week_id IS NOT NULL) AND flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND flags.route_family = 'MANUAL_NON_QR'),
            'can_manage_evidence', ((flags.resolved_timesheet_id IS NOT NULL OR (flags.resolved_contract_week_id IS NOT NULL AND flags.route_family = 'MANUAL_NON_QR')) AND flags.locked_bool = FALSE AND flags.route_family <> 'IMPORT_AUTHORITATIVE'),
            'can_add_additional_manual', (flags.locked_bool = FALSE AND flags.authorised_bool = FALSE AND COALESCE(flags.is_adjusted, FALSE) = FALSE),
            'review_only', (flags.locked_bool = TRUE OR flags.authorised_bool = TRUE OR flags.route_family <> 'MANUAL_NON_QR'),
            'has_any_evidence', flags.has_any_evidence,
            'attached_evidence_count', flags.attached_evidence_count
          ),
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
          'count_deltas', JSONB_BUILD_OBJECT()
        )
        || CASE WHEN v_profile IN ('editor', 'active_row_visible') THEN JSONB_BUILD_OBJECT(
          'details', JSONB_BUILD_OBJECT(
            'requested_timesheet_id', flags.resolved_timesheet_id,
            'current_timesheet_id', flags.resolved_timesheet_id,
            'expected_timesheet_id', flags.resolved_timesheet_id,
            'current_version', flags.timesheet_version,
            'was_stale', COALESCE(flags.tsfin_is_stale, FALSE),
            'booking_id', flags.booking_id,
            'timesheet', JSONB_BUILD_OBJECT(
              'timesheet_id', flags.resolved_timesheet_id,
              'booking_id', flags.booking_id,
              'occupant_key_norm', flags.occupant_key_norm,
              'hospital_norm', flags.hospital_norm,
              'worked_start_iso', flags.timesheet_worked_start_iso,
              'worked_end_iso', flags.timesheet_worked_end_iso,
              'break_start_iso', flags.timesheet_break_start_iso,
              'break_end_iso', flags.timesheet_break_end_iso,
              'break_minutes', flags.timesheet_break_minutes,
              'worked_minutes', flags.timesheet_worked_minutes,
              'week_ending_date', flags.week_ending_date,
              'auth_name', flags.timesheet_auth_name,
              'auth_job_title', flags.timesheet_auth_job_title,
              'authorised_at_server', flags.authorised_at_server,
              'reference_number', flags.timesheet_reference_number,
              'reference_set_at', flags.timesheet_reference_set_at,
              'status', flags.timesheet_status,
              'created_at', flags.timesheet_created_at,
              'updated_at', flags.timesheet_updated_at,
              'version', flags.timesheet_version,
              'is_current', flags.resolved_timesheet_id IS NOT NULL,
              'contract_id', flags.resolved_contract_id,
              'submission_mode', flags.submission_mode,
              'manual_pdf_r2_key', flags.manual_pdf_r2_key,
              'sheet_scope', flags.sheet_scope,
              'actual_schedule_json', COALESCE(flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
              'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb),
              'qr_status', flags.qr_status,
              'qr_r2_key', flags.qr_r2_key,
              'manual_pdf_rotation_degrees', flags.manual_pdf_rotation_degrees,
              'generated_pdf_at_utc', flags.generated_pdf_at_utc,
              'candidate_hint_text', flags.candidate_hint_text,
              'is_adjustment', flags.timesheet_is_adjustment,
              'parent_timesheet_id', flags.timesheet_parent_timesheet_id,
              'adjustment_origin', flags.timesheet_adjustment_origin
            ),
            'tsfin', JSONB_BUILD_OBJECT(
              'id', flags.tsfin_id,
              'timesheet_id', flags.resolved_timesheet_id,
              'timesheet_version', flags.tsfin_timesheet_version,
              'basis', flags.tsfin_basis,
              'is_current', flags.tsfin_is_current,
              'is_stale', flags.tsfin_is_stale,
              'stale_reason', flags.tsfin_stale_reason,
              'processing_status', flags.tsfin_processing_status,
              'total_hours', flags.total_hours,
              'locked_by_invoice_id', flags.tsfin_locked_by_invoice_id,
              'locked_at_utc', flags.tsfin_locked_at_utc,
              'paid_at_utc', flags.tsfin_paid_at_utc,
              'processed_at_utc', flags.tsfin_processed_at_utc,
              'authorised_at_utc', flags.tsfin_authorised_at_utc,
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
              'additional_units_json', COALESCE(flags.tsfin_additional_units_json, '{}'::jsonb),
              'has_rate_issue', flags.has_rate_issue,
              'has_pay_channel_issue', flags.has_pay_channel_issue,
              'hr_crosscheck_status', flags.hr_crosscheck_status,
              'hr_crosscheck_issues', flags.hr_crosscheck_issues_json
            ),
            'validations', COALESCE(flags.validations_json, '[]'::jsonb),
            'validation_summary', JSONB_BUILD_OBJECT(
              'status', flags.validation_status,
              'pre_validated', COALESCE((flags.latest_validation_json->>'pre_validated')::boolean, FALSE),
              'latest', flags.latest_validation_json
            ),
            'contract_week_id', flags.resolved_contract_week_id,
            'contract_week', JSONB_BUILD_OBJECT(
              'id', flags.resolved_contract_week_id,
              'contract_id', flags.resolved_contract_id,
              'week_ending_date', flags.week_ending_date,
              'additional_seq', flags.contract_week_additional_seq,
              'status', flags.contract_week_status,
              'submission_mode_snapshot', flags.submission_mode_snapshot,
              'timesheet_id', flags.resolved_timesheet_id,
              'uploaded_pdf_r2_key', flags.uploaded_pdf_r2_key,
              'day_entries_json', COALESCE(flags.contract_week_day_entries_json, '[]'::jsonb),
              'totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
              'created_at', flags.contract_week_created_at,
              'updated_at', flags.contract_week_updated_at,
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'is_adjustment', flags.contract_week_is_adjustment,
              'enforce_day_partition', flags.contract_week_enforce_day_partition,
              'allowed_days_mask', flags.contract_week_allowed_days_mask,
              'split_boundary_date', flags.contract_week_split_boundary_date,
              'worker_note', flags.contract_week_worker_note,
              'split_group_key', flags.contract_week_split_group_key
            ),
            'related', JSONB_BUILD_OBJECT(
              'contract', JSONB_BUILD_OBJECT(
                'id', flags.resolved_contract_id,
                'candidate_id', flags.resolved_candidate_id,
                'client_id', flags.resolved_client_id,
                'role', flags.contract_role,
                'band', flags.contract_band,
                'display_site', flags.contract_display_site,
                'ward_hint', flags.contract_ward_hint,
                'default_submission_mode', flags.contract_default_submission_mode,
                'std_schedule_json', COALESCE(flags.contract_std_schedule_json, '[]'::jsonb),
                'additional_rates_json', COALESCE(flags.contract_additional_rates_json, '{}'::jsonb),
                'weekly_timesheet_source', flags.contract_weekly_timesheet_source,
                'no_timesheet_required', flags.contract_no_timesheet_required,
                'autoprocess_hr', flags.contract_autoprocess_hr,
                'requires_hr', flags.contract_requires_hr,
                'hr_attach_to_invoice', flags.contract_hr_attach_to_invoice,
                'ts_attach_to_invoice', flags.contract_ts_attach_to_invoice,
                'is_nhsp', flags.contract_is_nhsp
              ),
              'candidate', JSONB_BUILD_OBJECT(
                'id', flags.resolved_candidate_id,
                'tms_ref', flags.candidate_tms_ref,
                'first_name', flags.candidate_first_name,
                'last_name', flags.candidate_last_name,
                'display_name', flags.candidate_display_name_raw,
                'email', flags.candidate_email,
                'phone', flags.candidate_phone,
                'key_norm', flags.candidate_key_norm,
                'band', flags.candidate_band
              ),
              'client', JSONB_BUILD_OBJECT(
                'id', flags.resolved_client_id,
                'cli_ref', flags.client_cli_ref,
                'name', flags.client_name_raw,
                'vat_chargeable', flags.client_vat_chargeable,
                'ts_queries_email', flags.client_ts_queries_email
              )
            ),
            'evidence', '[]'::jsonb,
            'policy', JSONB_BUILD_OBJECT(
              'weekly_mode', flags.contract_weekly_timesheet_source,
              'requires_hr', flags.contract_requires_hr,
              'autoprocess_hr', flags.contract_autoprocess_hr,
              'no_timesheet_required', flags.contract_no_timesheet_required,
              'is_nhsp', flags.contract_is_nhsp
            ),
            'effective', JSONB_BUILD_OBJECT(
              'route_type', flags.route_type,
              'route_display', flags.route_display,
              'route_family', flags.route_family,
              'route_subfamily', flags.route_subfamily,
              'underlying_channel_family', flags.underlying_channel_family,
              'is_adjustment', COALESCE(flags.timesheet_is_adjustment, flags.contract_week_is_adjustment, FALSE),
              'additional_seq', flags.contract_week_additional_seq,
              'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
              'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
              'suppress_standard_schedule_fallback', flags.keep_blank_additional_schedule_bool,
              'keep_additional_manual_adjustment_schedule_empty', flags.keep_blank_additional_schedule_bool,
              'summary_stage', flags.summary_stage,
              'client_requires_hr', flags.contract_requires_hr,
              'client_autoprocess_hr', flags.contract_autoprocess_hr,
              'client_no_timesheet_required', flags.contract_no_timesheet_required,
              'client_is_nhsp', flags.contract_is_nhsp,
              'contract_id', flags.resolved_contract_id,
              'issue_codes', flags.issue_codes_json
            )
          ),
          'timesheet', JSONB_BUILD_OBJECT(
            'timesheet_id', flags.resolved_timesheet_id,
            'booking_id', flags.booking_id,
            'worked_start_iso', flags.timesheet_worked_start_iso,
            'worked_end_iso', flags.timesheet_worked_end_iso,
            'break_start_iso', flags.timesheet_break_start_iso,
            'break_end_iso', flags.timesheet_break_end_iso,
            'break_minutes', flags.timesheet_break_minutes,
            'worked_minutes', flags.timesheet_worked_minutes,
            'week_ending_date', flags.week_ending_date,
            'actual_schedule_json', COALESCE(flags.timesheet_actual_schedule_json, '[]'::jsonb),
            'additional_units_week', COALESCE(flags.timesheet_additional_units_week, '{}'::jsonb),
            'additional_units_per_day', COALESCE(flags.timesheet_additional_units_per_day, '{}'::jsonb)
          ),
          'tsfin', JSONB_BUILD_OBJECT(
            'id', flags.tsfin_id,
            'timesheet_id', flags.resolved_timesheet_id,
            'basis', flags.tsfin_basis,
            'processing_status', flags.tsfin_processing_status,
            'total_hours', flags.total_hours,
            'actual_schedule_json', COALESCE(flags.tsfin_actual_schedule_json, flags.timesheet_actual_schedule_json, '[]'::jsonb),
            'actual_minutes_by_day_json', COALESCE(flags.tsfin_actual_minutes_by_day_json, '{}'::jsonb),
            'additional_units_json', COALESCE(flags.tsfin_additional_units_json, '{}'::jsonb),
            'locked_by_invoice_id', flags.tsfin_locked_by_invoice_id,
            'paid_at_utc', flags.tsfin_paid_at_utc,
            'processed_at_utc', flags.tsfin_processed_at_utc,
            'authorised_at_utc', flags.tsfin_authorised_at_utc
          ),
          'contract_week', JSONB_BUILD_OBJECT(
            'id', flags.resolved_contract_week_id,
            'contract_id', flags.resolved_contract_id,
            'week_ending_date', flags.week_ending_date,
            'day_entries_json', COALESCE(flags.contract_week_day_entries_json, '[]'::jsonb),
            'totals_json', COALESCE(flags.contract_week_totals_json, '{}'::jsonb),
            'planned_schedule_json', COALESCE(flags.contract_week_planned_schedule_json, '[]'::jsonb),
            'additional_seq', flags.contract_week_additional_seq,
            'is_adjustment', flags.contract_week_is_adjustment
          ),
          'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count),
          'left_pane', JSONB_BUILD_OBJECT('evidence', '[]'::jsonb, 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count))
        ) ELSE JSONB_BUILD_OBJECT(
          'details', JSONB_BUILD_OBJECT(),
          'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count),
          'left_pane', JSONB_BUILD_OBJECT('evidence', '[]'::jsonb, 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE, 'has_any_evidence', flags.has_any_evidence, 'attached_evidence_count', flags.attached_evidence_count))
        ) END AS payload_json
      FROM flags
      WHERE flags.row_found = TRUE
    )
    SELECT row_payload.payload_json || JSONB_BUILD_OBJECT(
        'data_row', row_payload.payload_json->'row',
        'row_patch', COALESCE(row_payload.payload_json->'row_patch', JSONB_BUILD_OBJECT()) || JSONB_BUILD_OBJECT('row_key', row_payload.payload_json->>'row_key')
      )
      INTO v_out
    FROM row_payload;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', v_profile,
        'profile', v_profile,
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk authorise row context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;


    v_canonical_authorise_row_json := NULL;
    v_canonical_authorise_row_signature := NULL;

    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      SELECT canonical_patch.row_json,
             canonical_patch.row_json->>'row_signature'
        INTO v_canonical_authorise_row_json,
             v_canonical_authorise_row_signature
      FROM public.bulk_timesheet_row_patch_v1(
        (
          v_decision_filters
          - 'row_key' - 'rowKey' - 'row_keys' - 'rowKeys'
          - 'timesheet_id' - 'timesheetId' - 'timesheet_ids' - 'timesheetIds'
          - 'current_timesheet_id' - 'currentTimesheetId'
          - 'requested_timesheet_id' - 'requestedTimesheetId'
          - 'expected_timesheet_id' - 'expectedTimesheetId'
          - 'contract_week_id' - 'contractWeekId' - 'contract_week_ids' - 'contractWeekIds'
          - 'week_id' - 'weekId' - 'id' - 'ids'
        )
        || jsonb_strip_nulls(JSONB_BUILD_OBJECT(
             'dataset_mode', 'authorise',
             'projection', 'active_row_header',
             'profile', COALESCE(NULLIF(BTRIM(COALESCE(v_out->>'profile', '')), ''), v_profile, 'status_header'),
             'row_key', NULLIF(BTRIM(COALESCE(v_out->>'row_key', '')), ''),
             'timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', '')), ''),
             'current_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'current_timesheet_id', '')), ''),
             'requested_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'requested_timesheet_id', '')), ''),
             'expected_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'expected_timesheet_id', '')), ''),
             'contract_week_id', NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', '')), '')
           ))
      ) AS canonical_patch(row_json)
      WHERE NULLIF(BTRIM(COALESCE(canonical_patch.row_json->>'row_signature', '')), '') IS NOT NULL
      ORDER BY
        CASE
          WHEN NULLIF(BTRIM(COALESCE(v_out->>'row_key', '')), '') IS NOT NULL
           AND canonical_patch.row_json->>'row_key' = v_out->>'row_key'
            THEN 0
          ELSE 1
        END,
        canonical_patch.row_json->>'row_key'
      LIMIT 1;

      IF NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_signature, '')), '') IS NOT NULL THEN
        v_canonical_is_archived := UPPER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'tools_stage', ''))) = 'ARCHIVED';
        v_canonical_retained := LOWER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_can_unprocess := NOT v_canonical_is_archived
          AND NOT v_canonical_retained
          AND LOWER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_unprocess_visible := NOT v_canonical_is_archived
          AND LOWER(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_action_visible',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_action_visible}',
            CASE WHEN v_canonical_retained THEN 'true' ELSE v_canonical_authorise_row_json->>'can_unprocess' END,
            'false'
          ))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_unprocess_block_reason := CASE
          WHEN v_canonical_is_archived THEN 'TIMESHEET_ARCHIVED'
          WHEN v_canonical_retained THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'
          ELSE NULLIF(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_block_reason',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_block_reason}',
            ''
          )), '')
        END;
        v_canonical_unprocess_block_message := CASE
          WHEN v_canonical_is_archived THEN 'Archived timesheets must be Unarchived before lifecycle actions.'
          WHEN v_canonical_retained THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.'
          ELSE NULLIF(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_block_message',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_block_message}',
            ''
          )), '')
        END;

        v_canonical_lifecycle_overlay := jsonb_strip_nulls(
          jsonb_build_object(
            'row_signature', v_canonical_authorise_row_signature,
            'backend_row_signature', COALESCE(NULLIF(BTRIM(v_canonical_authorise_row_json->>'backend_row_signature'), ''), v_canonical_authorise_row_signature),
            'mutation_row_signature', COALESCE(NULLIF(BTRIM(v_canonical_authorise_row_json->>'mutation_row_signature'), ''), v_canonical_authorise_row_signature),
            'summary_stage', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'summary_stage', '')), ''),
            'tools_stage', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'tools_stage', '')), ''),
            'processing_status', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'processing_status', '')), ''),
            'has_retained_financial_history', v_canonical_retained,
            'can_unprocess', v_canonical_can_unprocess,
            'unprocess_action_visible', v_canonical_unprocess_visible,
            'unprocess_block_reason', v_canonical_unprocess_block_reason,
            'unprocess_block_message', v_canonical_unprocess_block_message,
            'permission_state_patch_complete', TRUE,
            'priority_badges_patch_complete', TRUE,
            'lifecycle_authority_complete', TRUE,
            'immediate_lifecycle_patch_available', TRUE,
            'refresh_required', FALSE,
            'requires_affected_row_refresh', FALSE
          )
          || jsonb_build_object(
            'is_archived', v_canonical_is_archived,
            'read_only', v_canonical_is_archived,
            'can_archive', FALSE,
            'can_unarchive', v_canonical_is_archived
          )
        );

        v_canonical_action_flags := COALESCE(v_canonical_authorise_row_json->'action_flags', '{}'::jsonb)
          || jsonb_build_object(
            'has_retained_financial_history', v_canonical_retained,
            'can_unprocess', v_canonical_can_unprocess,
            'unprocess_action_visible', v_canonical_unprocess_visible,
            'unprocess_block_reason', v_canonical_unprocess_block_reason,
            'unprocess_block_message', v_canonical_unprocess_block_message,
            'is_archived', v_canonical_is_archived,
            'read_only', v_canonical_is_archived,
            'can_archive', FALSE,
            'can_unarchive', v_canonical_is_archived,
            'permission_state_patch_complete', TRUE,
            'priority_badges_patch_complete', TRUE,
            'lifecycle_authority_complete', TRUE,
            'immediate_lifecycle_patch_available', TRUE,
            'refresh_required', FALSE,
            'requires_affected_row_refresh', FALSE
          );
        v_canonical_row_patch := COALESCE(v_canonical_authorise_row_json->'row_patch', '{}'::jsonb)
          || v_canonical_lifecycle_overlay;

        v_out := v_out || v_canonical_lifecycle_overlay;
        v_out := JSONB_SET(v_out, '{action_flags}', COALESCE(v_out->'action_flags', '{}'::jsonb) || v_canonical_action_flags, TRUE);
        v_out := JSONB_SET(v_out, '{row_patch}', COALESCE(v_out->'row_patch', '{}'::jsonb) || v_canonical_row_patch, TRUE);

        IF JSONB_TYPEOF(v_out->'row') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{row}',
            COALESCE(v_out->'row', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{row,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{row,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;

        IF JSONB_TYPEOF(v_out->'data_row') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{data_row}',
            COALESCE(v_out->'data_row', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{data_row,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{data_row,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;

        IF JSONB_TYPEOF(v_out->'details') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{details}',
            COALESCE(v_out->'details', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{details,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{details,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;
      END IF;
    END IF;

    RETURN private._candidate_office_context_overlay_v1(v_out);
  END IF;

  IF v_profile = 'evidence' THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (cw_ids.input_contract_week_id IS NOT NULL AND cw0.id = cw_ids.input_contract_week_id)
         OR (cw_ids.input_timesheet_id IS NOT NULL AND cw0.timesheet_id = cw_ids.input_timesheet_id)
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM (
        SELECT input_ids.*
        FROM input_ids
        WHERE input_ids.input_timesheet_id IS NULL
          AND input_ids.input_contract_week_id IS NULL
      ) AS summary_ids
      CROSS JOIN LATERAL public.timesheet_summary_lightweight_rows_v1(
        v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)
      ) AS summary_source
      WHERE (
          summary_ids.input_timesheet_id IS NULL
          OR summary_source.timesheet_id = summary_ids.input_timesheet_id
        )
        AND (
          summary_ids.input_contract_week_id IS NULL
          OR summary_source.contract_week_id = summary_ids.input_contract_week_id
        )
      ORDER BY
        CASE WHEN summary_ids.input_timesheet_id IS NOT NULL AND summary_source.timesheet_id = summary_ids.input_timesheet_id THEN 0 ELSE 1 END,
        CASE WHEN summary_ids.input_contract_week_id IS NOT NULL AND summary_source.contract_week_id = summary_ids.input_contract_week_id THEN 0 ELSE 1 END,
        summary_source.timesheet_id NULLS LAST,
        summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    resolved AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id) AS resolved_contract_id,
        summary_row.candidate_id AS candidate_id,
        summary_row.client_id AS client_id,
        summary_row.candidate_name AS candidate_name,
        summary_row.candidate_display_name AS candidate_display_name,
        summary_row.client_name AS client_name,
        COALESCE(summary_row.week_ending_date, timesheet_row.week_ending_date, contract_week_row.week_ending_date) AS week_ending_date,
        COALESCE(summary_row.route_family, CASE WHEN COALESCE(timesheet_row.qr_status::text, '') <> '' THEN 'QR' WHEN UPPER(COALESCE(timesheet_row.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'ELECTRONIC' THEN 'ELECTRONIC' ELSE 'MANUAL_NON_QR' END) AS route_family,
        COALESCE(summary_row.has_any_evidence, FALSE) AS summary_has_any_evidence,
        COALESCE(summary_row.attached_evidence_count, 0) AS summary_attached_evidence_count,
        summary_row.primary_artifact_storage_key AS summary_primary_artifact_storage_key,
        summary_row.primary_artifact_display_name AS summary_primary_artifact_display_name,
        summary_row.primary_artifact_preview_mode AS summary_primary_artifact_preview_mode,
        timesheet_row.manual_pdf_r2_key AS manual_pdf_r2_key,
        timesheet_row.qr_r2_key AS qr_r2_key,
        timesheet_row.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.uploaded_pdf_r2_key AS uploaded_pdf_r2_key,
        contract_week_row.updated_at AS contract_week_updated_at
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
    ),
    evidence_items AS (
      SELECT
        0::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:manual_pdf:' || resolved.resolved_timesheet_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Timesheet PDF',
          'filename', 'Timesheet PDF',
          'storage_key', resolved.manual_pdf_r2_key,
          'r2_key', resolved.manual_pdf_r2_key,
          'file_key', resolved.manual_pdf_r2_key,
          'download_storage_key', resolved.manual_pdf_r2_key,
          'original_filename', 'Timesheet PDF',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.timesheet_updated_at,
          'rotation_degrees', COALESCE(resolved.manual_pdf_rotation_degrees, 0),
          'last_rotation_deg', COALESCE(resolved.manual_pdf_rotation_degrees, 0),
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.manual_pdf_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_manual_pdf_duplicate
          WHERE te_manual_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_manual_pdf_duplicate.storage_key = resolved.manual_pdf_r2_key
        )
      UNION ALL
      SELECT
        1::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:qr_pdf:' || resolved.resolved_timesheet_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', 'Signed QR Timesheet',
          'filename', 'Signed QR Timesheet',
          'storage_key', resolved.qr_r2_key,
          'r2_key', resolved.qr_r2_key,
          'file_key', resolved.qr_r2_key,
          'download_storage_key', resolved.qr_r2_key,
          'original_filename', 'Signed QR Timesheet',
          'mime_type', 'application/pdf',
          'content_type', 'application/pdf',
          'uploaded_at_utc', resolved.timesheet_updated_at,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', 'PDF',
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.qr_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_qr_pdf_duplicate
          WHERE te_qr_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_qr_pdf_duplicate.storage_key = resolved.qr_r2_key
        )
      UNION ALL
      SELECT
        2::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', 'sys:contract_week_pdf:' || resolved.resolved_contract_week_id::text,
          'evidence_id', NULL::uuid,
          'queue_id', NULL::uuid,
          'timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', 'TIMESHEET',
          'display_name', COALESCE(
            NULLIF(BTRIM(contract_week_queue_file.original_filename), ''),
            NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(resolved.uploaded_pdf_r2_key, ''), '^.*/', '')), ''),
            'Uploaded timesheet'
          ),
          'filename', COALESCE(
            NULLIF(BTRIM(contract_week_queue_file.original_filename), ''),
            NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(resolved.uploaded_pdf_r2_key, ''), '^.*/', '')), ''),
            'Uploaded timesheet'
          ),
          'storage_key', resolved.uploaded_pdf_r2_key,
          'r2_key', resolved.uploaded_pdf_r2_key,
          'file_key', resolved.uploaded_pdf_r2_key,
          'download_storage_key', resolved.uploaded_pdf_r2_key,
          'original_filename', COALESCE(
            NULLIF(BTRIM(contract_week_queue_file.original_filename), ''),
            NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(resolved.uploaded_pdf_r2_key, ''), '^.*/', '')), ''),
            'Uploaded timesheet'
          ),
          'mime_type', COALESCE(
            NULLIF(BTRIM(contract_week_queue_file.mime_type), ''),
            CASE
              WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
              WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
              WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
              WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
              WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
              ELSE 'application/octet-stream'
            END
          ),
          'content_type', COALESCE(
            NULLIF(BTRIM(contract_week_queue_file.mime_type), ''),
            CASE
              WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
              WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
              WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
              WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
              WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
              ELSE 'application/octet-stream'
            END
          ),
          'uploaded_at_utc', COALESCE(contract_week_queue_file.uploaded_at_utc, resolved.contract_week_updated_at),
          'rotation_degrees', COALESCE(contract_week_queue_file.last_rotation_deg::integer, 0),
          'last_rotation_deg', COALESCE(contract_week_queue_file.last_rotation_deg::integer, 0),
          'page_count', CASE
            WHEN COALESCE(contract_week_queue_file.meta_json->>'page_count', '') ~ '^[0-9]+$'
              THEN (contract_week_queue_file.meta_json->>'page_count')::integer
            ELSE NULL::integer
          END,
          'pages', '[]'::jsonb,
          'system', TRUE,
          'is_view_only', TRUE,
          'can_delete', FALSE,
          'can_reclassify', FALSE,
          'can_edit_kind', FALSE,
          'can_edit_type', FALSE,
          'can_return_to_queue', FALSE,
          'preview_mode', CASE
            WHEN NULLIF(BTRIM(contract_week_queue_file.mime_type), '') IS NOT NULL THEN
              CASE
                WHEN LOWER(BTRIM(contract_week_queue_file.mime_type)) = 'application/pdf' THEN 'PDF'
                WHEN LOWER(BTRIM(contract_week_queue_file.mime_type)) LIKE 'image/%' THEN 'IMAGE'
                ELSE 'FILE'
              END
            WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.pdf($|[?#])' THEN 'PDF'
            WHEN LOWER(COALESCE(resolved.uploaded_pdf_r2_key, '')) ~ '\.(png|jpe?g|gif|webp)($|[?#])' THEN 'IMAGE'
            ELSE 'FILE'
          END,
          'source_label', 'System',
          'source_badge', 'System'
        ) AS item_json
      FROM resolved
      LEFT JOIN LATERAL (
        SELECT
          mq_contract_week_file.original_filename,
          mq_contract_week_file.mime_type,
          mq_contract_week_file.uploaded_at_utc,
          mq_contract_week_file.last_rotation_deg,
          mq_contract_week_file.meta_json
        FROM public.manual_timesheet_queue AS mq_contract_week_file
        WHERE UPPER(COALESCE(mq_contract_week_file.status, '')) = 'STAGED'
          AND mq_contract_week_file.r2_key = resolved.uploaded_pdf_r2_key
          AND NULLIF(BTRIM(COALESCE(mq_contract_week_file.meta_json->>'contract_week_id', '')), '') = resolved.resolved_contract_week_id::text
        ORDER BY mq_contract_week_file.uploaded_at_utc DESC NULLS LAST, mq_contract_week_file.id DESC
        LIMIT 1
      ) AS contract_week_queue_file ON TRUE
      WHERE resolved.resolved_contract_week_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(resolved.uploaded_pdf_r2_key, '')), '') IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM public.timesheet_evidence AS te_contract_week_pdf_duplicate
          WHERE te_contract_week_pdf_duplicate.timesheet_id = resolved.resolved_timesheet_id
            AND te_contract_week_pdf_duplicate.storage_key = resolved.uploaded_pdf_r2_key
        )
      UNION ALL
      SELECT
        10::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', te0.id,
          'evidence_id', te0.id,
          'queue_id', NULL::uuid,
          'timesheet_id', te0.timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', UPPER(COALESCE(NULLIF(BTRIM(te0.kind), ''), 'TIMESHEET')),
          'display_name', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'storage_key', te0.storage_key,
          'r2_key', te0.storage_key,
          'file_key', te0.storage_key,
          'download_storage_key', te0.storage_key,
          'original_filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(te0.storage_key, ''), '^.*/', '')), ''), 'Evidence'),
          'mime_type', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
            ELSE NULL::text
          END,
          'content_type', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.png($|[?#])' THEN 'image/png'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.gif($|[?#])' THEN 'image/gif'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.webp($|[?#])' THEN 'image/webp'
            ELSE NULL::text
          END,
          'uploaded_at_utc', te0.created_at,
          'created_at', te0.created_at,
          'created_by', te0.created_by,
          'rotation', 0,
          'rotation_degrees', 0,
          'last_rotation_deg', 0,
          'page_count', NULL::integer,
          'pages', '[]'::jsonb,
          'system', FALSE,
          'is_view_only', FALSE,
          'can_delete', TRUE,
          'can_reclassify', TRUE,
          'can_edit_kind', TRUE,
          'can_edit_type', TRUE,
          'can_return_to_queue', TRUE,
          'preview_mode', CASE
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.pdf($|[?#])' THEN 'PDF'
            WHEN LOWER(COALESCE(te0.storage_key, '')) ~ '\.(png|jpe?g|gif|webp|bmp|svg|tif|tiff|heic|heif|avif)($|[?#])' THEN 'IMAGE'
            ELSE 'FILE'
          END,
          'source_label', 'Attached',
          'source_badge', 'Attached'
        ) AS item_json
      FROM public.timesheet_evidence AS te0
      CROSS JOIN resolved
      WHERE resolved.resolved_timesheet_id IS NOT NULL
        AND te0.timesheet_id = resolved.resolved_timesheet_id
      UNION ALL
      SELECT
        20::integer AS sort_order,
        JSONB_BUILD_OBJECT(
          'id', mq0.id,
          'evidence_id', NULL::uuid,
          'queue_id', mq0.id,
          'timesheet_id', NULL::uuid,
          'contract_week_id', resolved.resolved_contract_week_id,
          'kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
          'staged_kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
          'display_name', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
          'filename', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
          'original_filename', mq0.original_filename,
          'storage_key', mq0.r2_key,
          'r2_key', mq0.r2_key,
          'file_key', mq0.r2_key,
          'download_storage_key', mq0.r2_key,
          'mime_type', mq0.mime_type,
          'content_type', mq0.mime_type,
          'content_hash', mq0.content_hash,
          'uploaded_at_utc', mq0.uploaded_at_utc,
          'staged_at_utc', COALESCE(mq0.meta_json->>'staged_at_utc', mq0.uploaded_at_utc::text),
          'staged_by_user_id', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'staged_by_user_id'), ''), mq0.uploaded_by_user_id::text),
          'rotation_degrees', COALESCE(mq0.last_rotation_deg::integer, 0),
          'last_rotation_deg', COALESCE(mq0.last_rotation_deg::integer, 0),
          'page_count', CASE WHEN COALESCE(mq0.meta_json->>'page_count', '') ~ '^[0-9]+$' THEN (mq0.meta_json->>'page_count')::integer ELSE NULL::integer END,
          'pages', '[]'::jsonb,
          'status', mq0.status,
          'system', FALSE,
          'is_view_only', FALSE,
          'can_delete', TRUE,
          'can_reclassify', TRUE,
          'can_edit_kind', TRUE,
          'can_edit_type', TRUE,
          'can_return_to_queue', TRUE,
          'is_staged_context', TRUE,
          'preview_mode', CASE
            WHEN LOWER(COALESCE(mq0.mime_type, '')) LIKE 'image/%' THEN 'IMAGE'
            WHEN LOWER(COALESCE(mq0.mime_type, '')) = 'application/pdf' THEN 'PDF'
            ELSE 'FILE'
          END,
          'source_label', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged'),
          'source_badge', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged')
        ) AS item_json
      FROM public.manual_timesheet_queue AS mq0
      CROSS JOIN resolved
      WHERE resolved.resolved_timesheet_id IS NULL
        AND resolved.resolved_contract_week_id IS NOT NULL
        AND UPPER(COALESCE(mq0.status, '')) = 'STAGED'
        AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') = resolved.resolved_contract_week_id::text
    ),
    evidence_ranked AS (
      SELECT evidence_items.sort_order, evidence_items.item_json
      FROM evidence_items
      WHERE NULLIF(BTRIM(COALESCE(evidence_items.item_json->>'storage_key', evidence_items.item_json->>'r2_key', '')), '') IS NOT NULL
    ),
    evidence_payload AS (
      SELECT
        COALESCE(JSONB_AGG(evidence_ranked.item_json ORDER BY evidence_ranked.sort_order ASC, evidence_ranked.item_json->>'id' ASC), '[]'::jsonb) AS evidence_json,
        COUNT(*)::integer AS evidence_count,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'TIMESHEET') AS has_timesheet,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'MILEAGE') AS has_mileage,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'TRAVEL') AS has_travel,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'ACCOMMODATION') AS has_accommodation,
        BOOL_OR(UPPER(COALESCE(evidence_ranked.item_json->>'kind', evidence_ranked.item_json->>'staged_kind', '')) = 'OTHER') AS has_other,
        (SELECT er1.item_json FROM evidence_ranked AS er1 ORDER BY er1.sort_order ASC, er1.item_json->>'id' ASC LIMIT 1) AS primary_evidence_json
      FROM evidence_ranked
    ),
    final_payload AS (
      SELECT
        JSONB_BUILD_OBJECT(
          'ok', TRUE,
          'context_kind', 'bulk_authorise_row_context',
          'context_profile', 'evidence',
          'profile', 'evidence',
          'context_type', CASE WHEN 'bulk_authorise_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
          'slim_context', TRUE,
          'header_loaded', FALSE,
          'header_only', FALSE,
          'editor_loaded', FALSE,
          'evidence_loaded', TRUE,
          'compare_loaded', FALSE,
          'full_loaded', FALSE,
          'schedule_pending', TRUE,
          'schedule_authoritative', FALSE,
          'loaded_layers', JSONB_BUILD_ARRAY('evidence'),
          'soft_failure', FALSE,
          'context_degraded', FALSE,
          'degraded_reason', NULL::text,
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'row_signature', MD5(CONCAT_WS('|', COALESCE(resolved.resolved_timesheet_id::text, ''), COALESCE(resolved.resolved_contract_week_id::text, ''), COALESCE(resolved.timesheet_updated_at::text, ''), COALESCE(resolved.contract_week_updated_at::text, ''), 'evidence')),
          'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'attached_evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'attachedRows', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'primary_artifact', COALESCE(evidence_payload.primary_evidence_json, JSONB_BUILD_OBJECT()),
          'preview_storage_key', COALESCE(evidence_payload.primary_evidence_json->>'storage_key', resolved.summary_primary_artifact_storage_key),
          'primary_artifact_storage_key', COALESCE(evidence_payload.primary_evidence_json->>'storage_key', resolved.summary_primary_artifact_storage_key),
          'primary_artifact_preview_mode', COALESCE(evidence_payload.primary_evidence_json->>'preview_mode', resolved.summary_primary_artifact_preview_mode),
          'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
          'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
          'evidence_count', COALESCE(evidence_payload.evidence_count, 0),
          'evidence_badges', JSONB_BUILD_ARRAY(
            JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
            JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
          ),
          'evidence_meta', JSONB_BUILD_OBJECT(
            'evidence_loaded', TRUE,
            'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
            'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
            'evidence_badges', JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
            )
          ),
          'row', JSONB_BUILD_OBJECT(
            'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
            'timesheet_id', resolved.resolved_timesheet_id,
            'current_timesheet_id', resolved.resolved_timesheet_id,
            'requested_timesheet_id', resolved.resolved_timesheet_id,
            'expected_timesheet_id', resolved.resolved_timesheet_id,
            'contract_week_id', resolved.resolved_contract_week_id,
            'contract_id', resolved.resolved_contract_id,
            'candidate_id', resolved.candidate_id,
            'candidate_name', resolved.candidate_name,
            'candidate_display_name', resolved.candidate_display_name,
            'client_id', resolved.client_id,
            'client_name', resolved.client_name,
            'week_ending_date', resolved.week_ending_date,
            'route_family', resolved.route_family,
            'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0),
            'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0),
            'primary_artifact', COALESCE(evidence_payload.primary_evidence_json, JSONB_BUILD_OBJECT()),
            'primary_artifact_storage_key', COALESCE(evidence_payload.primary_evidence_json->>'storage_key', resolved.summary_primary_artifact_storage_key),
            'primary_artifact_preview_mode', COALESCE(evidence_payload.primary_evidence_json->>'preview_mode', resolved.summary_primary_artifact_preview_mode),
            'evidence_badges', JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_payload.has_timesheet, FALSE), 'has_evidence', COALESCE(evidence_payload.has_timesheet, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_payload.has_mileage, FALSE), 'has_evidence', COALESCE(evidence_payload.has_mileage, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_payload.has_travel, FALSE), 'has_evidence', COALESCE(evidence_payload.has_travel, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_payload.has_accommodation, FALSE), 'has_evidence', COALESCE(evidence_payload.has_accommodation, FALSE)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_payload.has_other, FALSE), 'has_evidence', COALESCE(evidence_payload.has_other, FALSE))
            )
          ),
          'row_patch', JSONB_BUILD_OBJECT(),
          'action_flags', JSONB_BUILD_OBJECT('has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0)),
          'details', JSONB_BUILD_OBJECT('evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb), 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE, 'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0))),
          'left_pane', JSONB_BUILD_OBJECT('evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb), 'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', TRUE, 'has_any_evidence', (COALESCE(evidence_payload.evidence_count, 0) > 0), 'attached_evidence_count', COALESCE(evidence_payload.evidence_count, 0))),
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
          'count_deltas', JSONB_BUILD_OBJECT(),
          'filters', v_filters
        ) AS payload_json
      FROM resolved
      LEFT JOIN evidence_payload ON TRUE
      WHERE resolved.resolved_timesheet_id IS NOT NULL OR resolved.resolved_contract_week_id IS NOT NULL
    )
    SELECT final_payload.payload_json || JSONB_BUILD_OBJECT('data_row', final_payload.payload_json->'row')
      INTO v_out
    FROM final_payload;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', 'evidence',
        'profile', 'evidence',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk authorise evidence context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;


    v_canonical_authorise_row_json := NULL;
    v_canonical_authorise_row_signature := NULL;

    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      SELECT canonical_patch.row_json,
             canonical_patch.row_json->>'row_signature'
        INTO v_canonical_authorise_row_json,
             v_canonical_authorise_row_signature
      FROM public.bulk_timesheet_row_patch_v1(
        (
          v_decision_filters
          - 'row_key' - 'rowKey' - 'row_keys' - 'rowKeys'
          - 'timesheet_id' - 'timesheetId' - 'timesheet_ids' - 'timesheetIds'
          - 'current_timesheet_id' - 'currentTimesheetId'
          - 'requested_timesheet_id' - 'requestedTimesheetId'
          - 'expected_timesheet_id' - 'expectedTimesheetId'
          - 'contract_week_id' - 'contractWeekId' - 'contract_week_ids' - 'contractWeekIds'
          - 'week_id' - 'weekId' - 'id' - 'ids'
        )
        || jsonb_strip_nulls(JSONB_BUILD_OBJECT(
             'dataset_mode', 'authorise',
             'projection', 'active_row_header',
             'profile', COALESCE(NULLIF(BTRIM(COALESCE(v_out->>'profile', '')), ''), v_profile, 'status_header'),
             'row_key', NULLIF(BTRIM(COALESCE(v_out->>'row_key', '')), ''),
             'timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', '')), ''),
             'current_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'current_timesheet_id', '')), ''),
             'requested_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'requested_timesheet_id', '')), ''),
             'expected_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'expected_timesheet_id', '')), ''),
             'contract_week_id', NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', '')), '')
           ))
      ) AS canonical_patch(row_json)
      WHERE NULLIF(BTRIM(COALESCE(canonical_patch.row_json->>'row_signature', '')), '') IS NOT NULL
      ORDER BY
        CASE
          WHEN NULLIF(BTRIM(COALESCE(v_out->>'row_key', '')), '') IS NOT NULL
           AND canonical_patch.row_json->>'row_key' = v_out->>'row_key'
            THEN 0
          ELSE 1
        END,
        canonical_patch.row_json->>'row_key'
      LIMIT 1;

      IF NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_signature, '')), '') IS NOT NULL THEN
        v_canonical_is_archived := UPPER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'tools_stage', ''))) = 'ARCHIVED';
        v_canonical_retained := LOWER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_can_unprocess := NOT v_canonical_is_archived
          AND NOT v_canonical_retained
          AND LOWER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_unprocess_visible := NOT v_canonical_is_archived
          AND LOWER(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_action_visible',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_action_visible}',
            CASE WHEN v_canonical_retained THEN 'true' ELSE v_canonical_authorise_row_json->>'can_unprocess' END,
            'false'
          ))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_unprocess_block_reason := CASE
          WHEN v_canonical_is_archived THEN 'TIMESHEET_ARCHIVED'
          WHEN v_canonical_retained THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'
          ELSE NULLIF(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_block_reason',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_block_reason}',
            ''
          )), '')
        END;
        v_canonical_unprocess_block_message := CASE
          WHEN v_canonical_is_archived THEN 'Archived timesheets must be Unarchived before lifecycle actions.'
          WHEN v_canonical_retained THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.'
          ELSE NULLIF(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_block_message',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_block_message}',
            ''
          )), '')
        END;

        v_canonical_lifecycle_overlay := jsonb_strip_nulls(
          jsonb_build_object(
            'row_signature', v_canonical_authorise_row_signature,
            'backend_row_signature', COALESCE(NULLIF(BTRIM(v_canonical_authorise_row_json->>'backend_row_signature'), ''), v_canonical_authorise_row_signature),
            'mutation_row_signature', COALESCE(NULLIF(BTRIM(v_canonical_authorise_row_json->>'mutation_row_signature'), ''), v_canonical_authorise_row_signature),
            'summary_stage', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'summary_stage', '')), ''),
            'tools_stage', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'tools_stage', '')), ''),
            'processing_status', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'processing_status', '')), ''),
            'has_retained_financial_history', v_canonical_retained,
            'can_unprocess', v_canonical_can_unprocess,
            'unprocess_action_visible', v_canonical_unprocess_visible,
            'unprocess_block_reason', v_canonical_unprocess_block_reason,
            'unprocess_block_message', v_canonical_unprocess_block_message,
            'permission_state_patch_complete', TRUE,
            'priority_badges_patch_complete', TRUE,
            'lifecycle_authority_complete', TRUE,
            'immediate_lifecycle_patch_available', TRUE,
            'refresh_required', FALSE,
            'requires_affected_row_refresh', FALSE
          )
          || jsonb_build_object(
            'is_archived', v_canonical_is_archived,
            'read_only', v_canonical_is_archived,
            'can_archive', FALSE,
            'can_unarchive', v_canonical_is_archived
          )
        );

        v_canonical_action_flags := COALESCE(v_canonical_authorise_row_json->'action_flags', '{}'::jsonb)
          || jsonb_build_object(
            'has_retained_financial_history', v_canonical_retained,
            'can_unprocess', v_canonical_can_unprocess,
            'unprocess_action_visible', v_canonical_unprocess_visible,
            'unprocess_block_reason', v_canonical_unprocess_block_reason,
            'unprocess_block_message', v_canonical_unprocess_block_message,
            'is_archived', v_canonical_is_archived,
            'read_only', v_canonical_is_archived,
            'can_archive', FALSE,
            'can_unarchive', v_canonical_is_archived,
            'permission_state_patch_complete', TRUE,
            'priority_badges_patch_complete', TRUE,
            'lifecycle_authority_complete', TRUE,
            'immediate_lifecycle_patch_available', TRUE,
            'refresh_required', FALSE,
            'requires_affected_row_refresh', FALSE
          );
        v_canonical_row_patch := COALESCE(v_canonical_authorise_row_json->'row_patch', '{}'::jsonb)
          || v_canonical_lifecycle_overlay;

        v_out := v_out || v_canonical_lifecycle_overlay;
        v_out := JSONB_SET(v_out, '{action_flags}', COALESCE(v_out->'action_flags', '{}'::jsonb) || v_canonical_action_flags, TRUE);
        v_out := JSONB_SET(v_out, '{row_patch}', COALESCE(v_out->'row_patch', '{}'::jsonb) || v_canonical_row_patch, TRUE);

        IF JSONB_TYPEOF(v_out->'row') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{row}',
            COALESCE(v_out->'row', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{row,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{row,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;

        IF JSONB_TYPEOF(v_out->'data_row') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{data_row}',
            COALESCE(v_out->'data_row', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{data_row,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{data_row,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;

        IF JSONB_TYPEOF(v_out->'details') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{details}',
            COALESCE(v_out->'details', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{details,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{details,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;
      END IF;
    END IF;

    RETURN private._candidate_office_context_overlay_v1(v_out);
  END IF;

  IF v_profile = 'compare_import' THEN
    WITH input_ids AS (
      SELECT
        CASE WHEN v_timesheet_id_text IS NOT NULL AND v_timesheet_id_text ~* v_uuid_re THEN v_timesheet_id_text::uuid ELSE NULL::uuid END AS input_timesheet_id,
        CASE WHEN v_contract_week_id_text IS NOT NULL AND v_contract_week_id_text ~* v_uuid_re THEN v_contract_week_id_text::uuid ELSE NULL::uuid END AS input_contract_week_id
    ),
    contract_week_row AS (
      SELECT cw0.*
      FROM public.contract_weeks AS cw0
      CROSS JOIN input_ids AS cw_ids
      WHERE (cw_ids.input_contract_week_id IS NOT NULL AND cw0.id = cw_ids.input_contract_week_id)
         OR (cw_ids.input_timesheet_id IS NOT NULL AND cw0.timesheet_id = cw_ids.input_timesheet_id)
      ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
      LIMIT 1
    ),
    timesheet_row AS (
      SELECT ts0.*
      FROM public.timesheets AS ts0
      CROSS JOIN input_ids AS ts_ids
      LEFT JOIN contract_week_row AS cw_for_ts ON TRUE
      WHERE ts0.is_current = TRUE
        AND (
          (ts_ids.input_timesheet_id IS NOT NULL AND ts0.timesheet_id = ts_ids.input_timesheet_id)
          OR (ts_ids.input_timesheet_id IS NULL AND cw_for_ts.timesheet_id IS NOT NULL AND ts0.timesheet_id = cw_for_ts.timesheet_id)
        )
      ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
      LIMIT 1
    ),
    tsfin_row AS (
      SELECT tf0.*
      FROM public.timesheets_financials AS tf0
      LEFT JOIN timesheet_row AS ts_for_tf ON TRUE
      WHERE tf0.is_current = TRUE
        AND ts_for_tf.timesheet_id IS NOT NULL
        AND tf0.timesheet_id = ts_for_tf.timesheet_id
      ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      LIMIT 1
    ),
    summary_row AS (
      SELECT summary_source.*
      FROM public.timesheet_summary_lightweight_rows_v1(v_decision_filters || JSONB_BUILD_OBJECT('disable_paging', TRUE, 'limit', 25)) AS summary_source
      CROSS JOIN input_ids AS summary_ids
      WHERE (summary_ids.input_timesheet_id IS NULL OR summary_source.timesheet_id = summary_ids.input_timesheet_id)
        AND (summary_ids.input_contract_week_id IS NULL OR summary_source.contract_week_id = summary_ids.input_contract_week_id)
      ORDER BY summary_source.timesheet_id NULLS LAST, summary_source.contract_week_id NULLS LAST
      LIMIT 1
    ),
    resolved AS (
      SELECT
        COALESCE(summary_row.timesheet_id, timesheet_row.timesheet_id, contract_week_row.timesheet_id) AS resolved_timesheet_id,
        COALESCE(summary_row.contract_week_id, contract_week_row.id) AS resolved_contract_week_id,
        COALESCE(summary_row.contract_id, timesheet_row.contract_id, contract_week_row.contract_id) AS resolved_contract_id,
        summary_row.candidate_id AS candidate_id,
        summary_row.client_id AS client_id,
        summary_row.candidate_name AS candidate_name,
        summary_row.client_name AS client_name,
        summary_row.week_ending_date AS week_ending_date,
        summary_row.route_family AS route_family,
        tsfin_row.external_source_rows_json AS external_source_rows_json,
        tsfin_row.updated_at AS tsfin_updated_at,
        timesheet_row.updated_at AS timesheet_updated_at,
        contract_week_row.updated_at AS contract_week_updated_at
      FROM input_ids
      LEFT JOIN summary_row ON TRUE
      LEFT JOIN timesheet_row ON TRUE
      LEFT JOIN contract_week_row ON TRUE
      LEFT JOIN tsfin_row ON TRUE
    )
    SELECT JSONB_BUILD_OBJECT(
        'ok', TRUE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', 'compare_import',
        'profile', 'compare_import',
        'context_type', CASE WHEN 'bulk_authorise_row_context' = 'bulk_authorise_row_context' THEN 'bulk_authorise' ELSE 'bulk_process' END,
        'slim_context', TRUE,
        'header_loaded', TRUE,
        'header_only', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', TRUE,
        'full_loaded', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', JSONB_BUILD_ARRAY('header', 'compare_import'),
        'soft_failure', FALSE,
        'context_degraded', FALSE,
        'degraded_reason', NULL::text,
        'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
        'timesheet_id', resolved.resolved_timesheet_id,
        'current_timesheet_id', resolved.resolved_timesheet_id,
        'requested_timesheet_id', resolved.resolved_timesheet_id,
        'expected_timesheet_id', resolved.resolved_timesheet_id,
        'contract_week_id', resolved.resolved_contract_week_id,
        'row_signature', MD5(CONCAT_WS('|', COALESCE(resolved.resolved_timesheet_id::text, ''), COALESCE(resolved.resolved_contract_week_id::text, ''), COALESCE(resolved.timesheet_updated_at::text, ''), COALESCE(resolved.contract_week_updated_at::text, ''), COALESCE(resolved.tsfin_updated_at::text, ''), 'compare_import')),
        'row', JSONB_BUILD_OBJECT(
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'contract_id', resolved.resolved_contract_id,
          'candidate_id', resolved.candidate_id,
          'candidate_name', resolved.candidate_name,
          'client_id', resolved.client_id,
          'client_name', resolved.client_name,
          'week_ending_date', resolved.week_ending_date,
          'route_family', resolved.route_family
        ),
        'data_row', JSONB_BUILD_OBJECT(
          'row_key', CASE WHEN resolved.resolved_timesheet_id IS NOT NULL THEN 'timesheet:' || resolved.resolved_timesheet_id::text ELSE 'contract_week:' || resolved.resolved_contract_week_id::text END,
          'timesheet_id', resolved.resolved_timesheet_id,
          'current_timesheet_id', resolved.resolved_timesheet_id,
          'requested_timesheet_id', resolved.resolved_timesheet_id,
          'expected_timesheet_id', resolved.resolved_timesheet_id,
          'contract_week_id', resolved.resolved_contract_week_id,
          'contract_id', resolved.resolved_contract_id,
          'candidate_id', resolved.candidate_id,
          'candidate_name', resolved.candidate_name,
          'client_id', resolved.client_id,
          'client_name', resolved.client_name,
          'week_ending_date', resolved.week_ending_date,
          'route_family', resolved.route_family
        ),
        'row_patch', JSONB_BUILD_OBJECT(),
        'action_flags', JSONB_BUILD_OBJECT(),
        'compare', JSONB_BUILD_OBJECT(
          'source_rows', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'external_source_rows_json', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'include_import_source_rows', v_include_import_source_rows
        ),
        'details', JSONB_BUILD_OBJECT(
          'source_rows', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'external_source_rows_json', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END,
          'evidence', '[]'::jsonb
        ),
        'left_pane', JSONB_BUILD_OBJECT('source_items', CASE WHEN v_include_import_source_rows THEN COALESCE(resolved.external_source_rows_json, '[]'::jsonb) ELSE '[]'::jsonb END, 'evidence', '[]'::jsonb),
        'evidence', '[]'::jsonb,
        'evidence_meta', JSONB_BUILD_OBJECT('evidence_loaded', FALSE),
        'cache_invalidation_hints', JSONB_BUILD_OBJECT(),
        'count_deltas', JSONB_BUILD_OBJECT(),
        'filters', v_filters
      )
      INTO v_out
    FROM resolved
    WHERE resolved.resolved_timesheet_id IS NOT NULL OR resolved.resolved_contract_week_id IS NOT NULL;

    IF v_out IS NULL THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', 'compare_import',
        'profile', 'compare_import',
        'soft_failure', TRUE,
        'context_degraded', TRUE,
        'degraded_reason', 'ROW_NOT_FOUND',
        'header_loaded', FALSE,
        'editor_loaded', FALSE,
        'evidence_loaded', FALSE,
        'compare_loaded', FALSE,
        'full_loaded', FALSE,
        'header_only', FALSE,
        'schedule_pending', TRUE,
        'schedule_authoritative', FALSE,
        'loaded_layers', '[]'::jsonb,
        'error', 'ROW_NOT_FOUND',
        'message', 'No bulk authorise compare/import context was found for the supplied identity',
        'filters', v_filters
      );
    END IF;


    v_canonical_authorise_row_json := NULL;
    v_canonical_authorise_row_signature := NULL;

    IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
      SELECT canonical_patch.row_json,
             canonical_patch.row_json->>'row_signature'
        INTO v_canonical_authorise_row_json,
             v_canonical_authorise_row_signature
      FROM public.bulk_timesheet_row_patch_v1(
        (
          v_decision_filters
          - 'row_key' - 'rowKey' - 'row_keys' - 'rowKeys'
          - 'timesheet_id' - 'timesheetId' - 'timesheet_ids' - 'timesheetIds'
          - 'current_timesheet_id' - 'currentTimesheetId'
          - 'requested_timesheet_id' - 'requestedTimesheetId'
          - 'expected_timesheet_id' - 'expectedTimesheetId'
          - 'contract_week_id' - 'contractWeekId' - 'contract_week_ids' - 'contractWeekIds'
          - 'week_id' - 'weekId' - 'id' - 'ids'
        )
        || jsonb_strip_nulls(JSONB_BUILD_OBJECT(
             'dataset_mode', 'authorise',
             'projection', 'active_row_header',
             'profile', COALESCE(NULLIF(BTRIM(COALESCE(v_out->>'profile', '')), ''), v_profile, 'status_header'),
             'row_key', NULLIF(BTRIM(COALESCE(v_out->>'row_key', '')), ''),
             'timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', '')), ''),
             'current_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'current_timesheet_id', '')), ''),
             'requested_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'requested_timesheet_id', '')), ''),
             'expected_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'expected_timesheet_id', '')), ''),
             'contract_week_id', NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', '')), '')
           ))
      ) AS canonical_patch(row_json)
      WHERE NULLIF(BTRIM(COALESCE(canonical_patch.row_json->>'row_signature', '')), '') IS NOT NULL
      ORDER BY
        CASE
          WHEN NULLIF(BTRIM(COALESCE(v_out->>'row_key', '')), '') IS NOT NULL
           AND canonical_patch.row_json->>'row_key' = v_out->>'row_key'
            THEN 0
          ELSE 1
        END,
        canonical_patch.row_json->>'row_key'
      LIMIT 1;

      IF NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_signature, '')), '') IS NOT NULL THEN
        v_canonical_is_archived := UPPER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'tools_stage', ''))) = 'ARCHIVED';
        v_canonical_retained := LOWER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_can_unprocess := NOT v_canonical_is_archived
          AND NOT v_canonical_retained
          AND LOWER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_unprocess_visible := NOT v_canonical_is_archived
          AND LOWER(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_action_visible',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_action_visible}',
            CASE WHEN v_canonical_retained THEN 'true' ELSE v_canonical_authorise_row_json->>'can_unprocess' END,
            'false'
          ))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_canonical_unprocess_block_reason := CASE
          WHEN v_canonical_is_archived THEN 'TIMESHEET_ARCHIVED'
          WHEN v_canonical_retained THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'
          ELSE NULLIF(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_block_reason',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_block_reason}',
            ''
          )), '')
        END;
        v_canonical_unprocess_block_message := CASE
          WHEN v_canonical_is_archived THEN 'Archived timesheets must be Unarchived before lifecycle actions.'
          WHEN v_canonical_retained THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.'
          ELSE NULLIF(BTRIM(COALESCE(
            v_canonical_authorise_row_json->>'unprocess_block_message',
            v_canonical_authorise_row_json#>>'{action_flags,unprocess_block_message}',
            ''
          )), '')
        END;

        v_canonical_lifecycle_overlay := jsonb_strip_nulls(
          jsonb_build_object(
            'row_signature', v_canonical_authorise_row_signature,
            'backend_row_signature', COALESCE(NULLIF(BTRIM(v_canonical_authorise_row_json->>'backend_row_signature'), ''), v_canonical_authorise_row_signature),
            'mutation_row_signature', COALESCE(NULLIF(BTRIM(v_canonical_authorise_row_json->>'mutation_row_signature'), ''), v_canonical_authorise_row_signature),
            'summary_stage', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'summary_stage', '')), ''),
            'tools_stage', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'tools_stage', '')), ''),
            'processing_status', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'processing_status', '')), ''),
            'has_retained_financial_history', v_canonical_retained,
            'can_unprocess', v_canonical_can_unprocess,
            'unprocess_action_visible', v_canonical_unprocess_visible,
            'unprocess_block_reason', v_canonical_unprocess_block_reason,
            'unprocess_block_message', v_canonical_unprocess_block_message,
            'permission_state_patch_complete', TRUE,
            'priority_badges_patch_complete', TRUE,
            'lifecycle_authority_complete', TRUE,
            'immediate_lifecycle_patch_available', TRUE,
            'refresh_required', FALSE,
            'requires_affected_row_refresh', FALSE
          )
          || jsonb_build_object(
            'is_archived', v_canonical_is_archived,
            'read_only', v_canonical_is_archived,
            'can_archive', FALSE,
            'can_unarchive', v_canonical_is_archived
          )
        );

        v_canonical_action_flags := COALESCE(v_canonical_authorise_row_json->'action_flags', '{}'::jsonb)
          || jsonb_build_object(
            'has_retained_financial_history', v_canonical_retained,
            'can_unprocess', v_canonical_can_unprocess,
            'unprocess_action_visible', v_canonical_unprocess_visible,
            'unprocess_block_reason', v_canonical_unprocess_block_reason,
            'unprocess_block_message', v_canonical_unprocess_block_message,
            'is_archived', v_canonical_is_archived,
            'read_only', v_canonical_is_archived,
            'can_archive', FALSE,
            'can_unarchive', v_canonical_is_archived,
            'permission_state_patch_complete', TRUE,
            'priority_badges_patch_complete', TRUE,
            'lifecycle_authority_complete', TRUE,
            'immediate_lifecycle_patch_available', TRUE,
            'refresh_required', FALSE,
            'requires_affected_row_refresh', FALSE
          );
        v_canonical_row_patch := COALESCE(v_canonical_authorise_row_json->'row_patch', '{}'::jsonb)
          || v_canonical_lifecycle_overlay;

        v_out := v_out || v_canonical_lifecycle_overlay;
        v_out := JSONB_SET(v_out, '{action_flags}', COALESCE(v_out->'action_flags', '{}'::jsonb) || v_canonical_action_flags, TRUE);
        v_out := JSONB_SET(v_out, '{row_patch}', COALESCE(v_out->'row_patch', '{}'::jsonb) || v_canonical_row_patch, TRUE);

        IF JSONB_TYPEOF(v_out->'row') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{row}',
            COALESCE(v_out->'row', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{row,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{row,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;

        IF JSONB_TYPEOF(v_out->'data_row') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{data_row}',
            COALESCE(v_out->'data_row', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{data_row,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{data_row,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;

        IF JSONB_TYPEOF(v_out->'details') = 'object' THEN
          v_out := JSONB_SET(
            v_out,
            '{details}',
            COALESCE(v_out->'details', '{}'::jsonb)
              || v_canonical_lifecycle_overlay
              || jsonb_build_object(
                'action_flags', COALESCE(v_out#>'{details,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
                'row_patch', COALESCE(v_out#>'{details,row_patch}', '{}'::jsonb) || v_canonical_row_patch
              ),
            TRUE
          );
        END IF;
      END IF;
    END IF;

    RETURN private._candidate_office_context_overlay_v1(v_out);
  END IF;

  WITH decision_row AS (
    SELECT patch_result.row_json
    FROM public.bulk_timesheet_row_patch_v1(
      v_decision_filters
      || JSONB_BUILD_OBJECT(
           'dataset_mode', 'authorise',
           'projection', 'active_row_header',
           'profile', v_profile
         )
    ) AS patch_result(row_json)
    ORDER BY patch_result.row_json->>'row_key'
    LIMIT 1
  ),
  row_ids AS (
    SELECT
      decision_row.row_json,
      CASE
        WHEN COALESCE(decision_row.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'timesheet_id')::uuid
        ELSE NULL::uuid
      END AS timesheet_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_week_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_week_id')::uuid
        ELSE NULL::uuid
      END AS contract_week_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'contract_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'contract_id')::uuid
        ELSE NULL::uuid
      END AS contract_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'candidate_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'candidate_id')::uuid
        ELSE NULL::uuid
      END AS candidate_id,
      CASE
        WHEN COALESCE(decision_row.row_json->>'client_id', '') ~* v_uuid_re THEN (decision_row.row_json->>'client_id')::uuid
        ELSE NULL::uuid
      END AS client_id
    FROM decision_row
  ),
  timesheet_row AS (
    SELECT
      ts0.timesheet_id,
      ts0.booking_id,
      ts0.occupant_key_norm,
      ts0.hospital_norm,
      ts0.ward_norm,
      ts0.job_title_norm,
      ts0.shift_label_norm,
      ts0.scheduled_start_iso,
      ts0.scheduled_end_iso,
      ts0.worked_start_iso,
      ts0.worked_end_iso,
      ts0.break_start_iso,
      ts0.break_end_iso,
      ts0.break_minutes,
      ts0.worked_minutes,
      ts0.week_ending_date,
      ts0.auth_name,
      ts0.auth_job_title,
      ts0.authorised_at_server,
      ts0.r2_nurse_key,
      ts0.r2_auth_key,
      ts0.reference_number,
      ts0.reference_set_at,
      ts0.status,
      ts0.created_at,
      ts0.updated_at,
      ts0.version,
      ts0.is_current,
      ts0.contract_id,
      ts0.submission_mode,
      ts0.manual_pdf_r2_key,
      ts0.line_type,
      ts0.sheet_scope,
      ts0.actual_schedule_json,
      ts0.additional_units_week,
      ts0.additional_units_per_day,
      ts0.qr_status,
      ts0.qr_payload_json,
      ts0.qr_generated_at,
      ts0.qr_scanned_at,
      ts0.qr_scan_info_json,
      ts0.qr_r2_key,
      ts0.day_references_json,
      ts0.manual_pdf_rotation_degrees,
      ts0.qr_last_sent_hash,
      ts0.qr_last_sent_at_utc,
      ts0.qr_signed_hash,
      ts0.qr_signed_at_utc,
      ts0.candidate_hint_text,
      ts0.band,
      ts0.generated_pdf_at_utc,
      ts0.generated_pdf_refs_sig,
      ts0.generated_pdf_refs_snapshot_json,
      ts0.generated_pdf_refs_captured_at_utc,
      ts0.qr_sent_refs_sig,
      ts0.qr_sent_refs_snapshot_json,
      ts0.qr_sent_refs_captured_at_utc,
      ts0.is_adjustment,
      ts0.parent_timesheet_id,
      ts0.correction_id,
      ts0.correction_kind,
      ts0.adjustment_origin
    FROM public.timesheets AS ts0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND ts0.timesheet_id = ids.timesheet_id
      AND ts0.is_current = TRUE
    ORDER BY ts0.version DESC NULLS LAST, ts0.updated_at DESC NULLS LAST, ts0.created_at DESC NULLS LAST
    LIMIT 1
  ),
  tsfin_row AS (
    SELECT
      tf0.id,
      tf0.timesheet_id,
      tf0.timesheet_version,
      tf0.basis,
      tf0.is_current,
      tf0.is_stale,
      tf0.stale_reason,
      tf0.worked_start_iso,
      tf0.worked_end_iso,
      tf0.break_start_iso,
      tf0.break_end_iso,
      tf0.break_minutes,
      tf0.candidate_id,
      tf0.client_id,
      tf0.role,
      tf0.band,
      tf0.pay_method,
      tf0.policy_snapshot_json,
      tf0.rate_source_refs_json,
      tf0.hours_day,
      tf0.hours_night,
      tf0.hours_sat,
      tf0.hours_sun,
      tf0.hours_bh,
      tf0.pay_day,
      tf0.pay_night,
      tf0.pay_sat,
      tf0.pay_sun,
      tf0.pay_bh,
      tf0.charge_day,
      tf0.charge_night,
      tf0.charge_sat,
      tf0.charge_sun,
      tf0.charge_bh,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.computed_at_utc,
      tf0.locked_by_invoice_id,
      tf0.locked_at_utc,
      tf0.unlocked_by_credit_note_id,
      tf0.created_at,
      tf0.updated_at,
      tf0.occupant_key_norm,
      tf0.candidate_assignment,
      tf0.processing_status,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_charge_ex_vat,
      tf0.expenses_description,
      tf0.expenses_evidence_r2_key,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.mileage_evidence_r2_key,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.po_number,
      tf0.pay_on_hold,
      tf0.pay_on_hold_reason,
      tf0.pay_on_hold_since_utc,
      tf0.paid_at_utc,
      tf0.paid_by_user_id,
      tf0.payment_reference,
      tf0.remittance_last_sent_at_utc,
      tf0.remittance_send_count,
      tf0.pay_wtr_rate_pct_snapshot,
      tf0.pay_vat_rate_pct_snapshot,
      tf0.pay_vat_amount_snapshot,
      tf0.pay_total_inc_vat_snapshot,
      tf0.processed_by_user_id,
      tf0.processed_at_utc,
      tf0.authorised_by_user_id,
      tf0.authorised_at_utc,
      tf0.expenses_evidence_manifest,
      tf0.mileage_evidence_manifest,
      tf0.actual_schedule_json,
      tf0.actual_minutes_by_day_json,
      tf0.additional_units_json,
      tf0.additional_pay_ex_vat,
      tf0.additional_charge_ex_vat,
      tf0.additional_margin_ex_vat,
      tf0.invoice_breakdown_json,
      tf0.nhsp_import_id,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.external_source_rows_json,
      tf0.mileage_units,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat
    FROM public.timesheets_financials AS tf0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tf0.timesheet_id = ids.timesheet_id
      AND tf0.is_current = TRUE
    ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
    LIMIT 1
  ),
  contract_week_row AS (
    SELECT
      cw0.id,
      cw0.contract_id,
      cw0.week_ending_date,
      cw0.additional_seq,
      cw0.status,
      cw0.submission_mode_snapshot,
      cw0.timesheet_id,
      cw0.uploaded_pdf_r2_key,
      cw0.day_entries_json,
      cw0.totals_json,
      cw0.created_at,
      cw0.updated_at,
      cw0.planned_schedule_json,
      cw0.is_adjustment,
      cw0.enforce_day_partition,
      cw0.allowed_days_mask,
      cw0.split_boundary_date,
      cw0.worker_note,
      cw0.split_group_key
    FROM public.contract_weeks AS cw0
    CROSS JOIN row_ids AS ids
    WHERE (ids.contract_week_id IS NOT NULL AND cw0.id = ids.contract_week_id)
       OR (ids.contract_week_id IS NULL AND ids.timesheet_id IS NOT NULL AND cw0.timesheet_id = ids.timesheet_id)
    ORDER BY cw0.updated_at DESC NULLS LAST, cw0.created_at DESC NULLS LAST, cw0.id DESC
    LIMIT 1
  ),
  contract_row AS (
    SELECT
      ct0.id,
      ct0.candidate_id,
      ct0.client_id,
      ct0.role,
      ct0.band,
      ct0.display_site,
      ct0.ward_hint,
      ct0.start_date,
      ct0.end_date,
      ct0.pay_method_snapshot,
      ct0.rates_json,
      ct0.std_hours_json,
      ct0.default_submission_mode,
      ct0.week_ending_weekday_snapshot,
      ct0.auto_invoice,
      ct0.require_reference_to_pay,
      ct0.require_reference_to_invoice,
      ct0.created_at,
      ct0.updated_at,
      ct0.bucket_labels_json,
      ct0.std_schedule_json,
      ct0.mileage_pay_rate,
      ct0.mileage_charge_rate,
      ct0.additional_rates_json,
      ct0.self_bill,
      ct0.weekly_timesheet_source,
      ct0.no_timesheet_required,
      ct0.daily_calc_of_invoices,
      ct0.group_nightsat_sunbh,
      ct0.is_nhsp,
      ct0.autoprocess_hr,
      ct0.requires_hr,
      ct0.hr_attach_to_invoice,
      ct0.ts_attach_to_invoice,
      ct0.overrideclientsettings,
      ct0.reference_number_required_to_issue_invoice,
      ct0.send_manual_invoices_to_different_email,
      ct0.manual_invoices_alt_email_address,
      ct0.is_ad_hoc
    FROM public.contracts AS ct0
    CROSS JOIN row_ids AS ids
    LEFT JOIN timesheet_row AS ts1 ON TRUE
    LEFT JOIN contract_week_row AS cw1 ON TRUE
    WHERE ct0.id = COALESCE(ids.contract_id, ts1.contract_id, cw1.contract_id)
    ORDER BY ct0.updated_at DESC NULLS LAST, ct0.created_at DESC NULLS LAST, ct0.id DESC
    LIMIT 1
  ),
  candidate_row AS (
    SELECT
      cand0.id,
      cand0.tms_ref,
      cand0.first_name,
      cand0.last_name,
      cand0.display_name,
      cand0.email,
      cand0.phone,
      cand0.pay_method,
      cand0.umbrella_id,
      cand0.active,
      cand0.key_norm,
      cand0.mileage_pay_rate,
      cand0.job_title_id,
      cand0.prof_reg_number,
      cand0.prof_reg_type,
      cand0.ni_number,
      cand0.date_of_birth,
      cand0.gender,
      cand0.title,
      cand0.opt_in_email,
      cand0.opt_in_sms,
      cand0.opt_in_whatsapp,
      cand0.band,
      cand0.tms_ref_num
    FROM public.candidates AS cand0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cand0.id = COALESCE(ids.candidate_id, ct1.candidate_id)
    LIMIT 1
  ),
  client_row AS (
    SELECT
      cli0.id,
      cli0.cli_ref,
      cli0.name,
      cli0.invoice_address,
      cli0.primary_invoice_email,
      cli0.ap_phone,
      cli0.vat_chargeable,
      cli0.payment_terms_days,
      cli0.mileage_charge_rate,
      cli0.ts_queries_email,
      cli0.client_address,
      cli0.contact_title,
      cli0.contact_known_as,
      cli0.contact_forename,
      cli0.contact_surname,
      cli0.contact_job_title,
      cli0.contact_tel,
      cli0.contact_mobile,
      cli0.contact_email,
      cli0.website
    FROM public.clients AS cli0
    CROSS JOIN row_ids AS ids
    LEFT JOIN contract_row AS ct1 ON TRUE
    WHERE cli0.id = COALESCE(ids.client_id, ct1.client_id)
    LIMIT 1
  ),
  validation_rows AS (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.booking_id,
      tv0.status,
      tv0.reason_code,
      tv0.hr_request_id,
      tv0.validated_at_utc,
      tv0.override_requested_at_utc,
      tv0.override_requested_by,
      tv0.override_confirmed_at_utc,
      tv0.override_confirmed_by,
      tv0.override_reason,
      tv0.override_cleared_at_utc,
      tv0.last_source,
      tv0.created_at,
      tv0.updated_at,
      tv0.hr_request_source,
      tv0.hr_request_set_by,
      tv0.hr_request_set_at_utc,
      tv0.pre_validated
    FROM public.timesheet_validations AS tv0
    CROSS JOIN row_ids AS ids
    WHERE ids.timesheet_id IS NOT NULL
      AND tv0.timesheet_id = ids.timesheet_id
    ORDER BY tv0.validated_at_utc DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
  ),
  validation_payload AS (
    SELECT
      COALESCE(JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', validation_rows.id,
          'timesheet_id', validation_rows.timesheet_id,
          'booking_id', validation_rows.booking_id,
          'status', validation_rows.status,
          'reason_code', validation_rows.reason_code,
          'hr_request_id', validation_rows.hr_request_id,
          'validated_at_utc', validation_rows.validated_at_utc,
          'override_requested_at_utc', validation_rows.override_requested_at_utc,
          'override_requested_by', validation_rows.override_requested_by,
          'override_confirmed_at_utc', validation_rows.override_confirmed_at_utc,
          'override_confirmed_by', validation_rows.override_confirmed_by,
          'override_reason', validation_rows.override_reason,
          'override_cleared_at_utc', validation_rows.override_cleared_at_utc,
          'last_source', validation_rows.last_source,
          'created_at', validation_rows.created_at,
          'updated_at', validation_rows.updated_at,
          'hr_request_source', validation_rows.hr_request_source,
          'hr_request_set_by', validation_rows.hr_request_set_by,
          'hr_request_set_at_utc', validation_rows.hr_request_set_at_utc,
          'pre_validated', validation_rows.pre_validated
        ) ORDER BY validation_rows.validated_at_utc DESC NULLS LAST, validation_rows.created_at DESC NULLS LAST, validation_rows.id DESC
      ), '[]'::jsonb) AS validations_json,
      (
        SELECT JSONB_BUILD_OBJECT(
          'id', vr1.id,
          'status', vr1.status,
          'reason_code', vr1.reason_code,
          'hr_request_id', vr1.hr_request_id,
          'validated_at_utc', vr1.validated_at_utc,
          'pre_validated', COALESCE(vr1.pre_validated, FALSE),
          'updated_at', vr1.updated_at
        )
        FROM validation_rows AS vr1
        ORDER BY vr1.validated_at_utc DESC NULLS LAST, vr1.created_at DESC NULLS LAST, vr1.id DESC
        LIMIT 1
      ) AS latest_validation_json
    FROM validation_rows
  ),
  evidence_items AS (
    SELECT
      0::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_id'), ''), 'sys:timesheet:' || COALESCE(row_ids.timesheet_id::text, row_ids.contract_week_id::text)),
        'evidence_id', NULL::uuid,
        'queue_id', NULL::uuid,
        'timesheet_id', row_ids.timesheet_id,
        'contract_week_id', row_ids.contract_week_id,
        'kind', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_kind'), ''), 'TIMESHEET'),
        'display_name', COALESCE(
          NULLIF(BTRIM(primary_queue_file.original_filename), ''),
          NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', ''), '^.*/', '')), ''),
          NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''),
          'Timesheet'
        ),
        'filename', COALESCE(
          NULLIF(BTRIM(primary_queue_file.original_filename), ''),
          NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', ''), '^.*/', '')), ''),
          NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''),
          'Timesheet'
        ),
        'storage_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'r2_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'file_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'download_storage_key', NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''),
        'original_filename', COALESCE(
          NULLIF(BTRIM(primary_queue_file.original_filename), ''),
          NULLIF(BTRIM(REGEXP_REPLACE(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', ''), '^.*/', '')), ''),
          NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_display_name'), ''),
          'Timesheet'
        ),
        'mime_type', COALESCE(
          NULLIF(BTRIM(primary_queue_file.mime_type), ''),
          CASE
            WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
            WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.png($|[?#])' THEN 'image/png'
            WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
            WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.gif($|[?#])' THEN 'image/gif'
            WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.webp($|[?#])' THEN 'image/webp'
            ELSE NULL::text
          END
        ),
        'content_type', COALESCE(
          NULLIF(BTRIM(primary_queue_file.mime_type), ''),
          CASE
            WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.pdf($|[?#])' THEN 'application/pdf'
            WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.png($|[?#])' THEN 'image/png'
            WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.jpe?g($|[?#])' THEN 'image/jpeg'
            WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.gif($|[?#])' THEN 'image/gif'
            WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.webp($|[?#])' THEN 'image/webp'
            ELSE NULL::text
          END
        ),
        'uploaded_at_utc', COALESCE(primary_queue_file.uploaded_at_utc::text, row_ids.row_json->>'updated_at'),
        'rotation_degrees', COALESCE(
          primary_queue_file.last_rotation_deg::integer,
          CASE
            WHEN COALESCE(row_ids.row_json->>'manual_pdf_rotation_degrees', '') ~ '^-?[0-9]+$'
              THEN (row_ids.row_json->>'manual_pdf_rotation_degrees')::integer
            ELSE 0
          END
        ),
        'last_rotation_deg', COALESCE(
          primary_queue_file.last_rotation_deg::integer,
          CASE
            WHEN COALESCE(row_ids.row_json->>'manual_pdf_rotation_degrees', '') ~ '^-?[0-9]+$'
              THEN (row_ids.row_json->>'manual_pdf_rotation_degrees')::integer
            ELSE 0
          END
        ),
        'page_count', CASE
          WHEN COALESCE(primary_queue_file.meta_json->>'page_count', '') ~ '^[0-9]+$'
            THEN (primary_queue_file.meta_json->>'page_count')::integer
          ELSE NULL::integer
        END,
        'pages', '[]'::jsonb,
        'system', TRUE,
        'is_view_only', TRUE,
        'can_delete', FALSE,
        'can_reclassify', FALSE,
        'can_edit_kind', FALSE,
        'can_edit_type', FALSE,
        'can_return_to_queue', FALSE,
        'preview_mode', CASE
          WHEN NULLIF(BTRIM(primary_queue_file.mime_type), '') IS NOT NULL THEN
            CASE
              WHEN LOWER(BTRIM(primary_queue_file.mime_type)) = 'application/pdf' THEN 'PDF'
              WHEN LOWER(BTRIM(primary_queue_file.mime_type)) LIKE 'image/%' THEN 'IMAGE'
              ELSE 'FILE'
            END
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.pdf($|[?#])' THEN 'PDF'
          WHEN LOWER(COALESCE(row_ids.row_json->>'primary_artifact_storage_key', '')) ~ '\.(png|jpe?g|gif|webp)($|[?#])' THEN 'IMAGE'
          ELSE COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_preview_mode'), ''), 'FILE')
        END,
        'source_label', 'System',
        'source_badge', 'System'
      ) AS item_json
      FROM row_ids
      LEFT JOIN LATERAL (
        SELECT
          mq_primary_file.original_filename,
          mq_primary_file.mime_type,
          mq_primary_file.uploaded_at_utc,
          mq_primary_file.last_rotation_deg,
          mq_primary_file.meta_json
        FROM public.manual_timesheet_queue AS mq_primary_file
        WHERE UPPER(COALESCE(mq_primary_file.status, '')) = 'STAGED'
          AND mq_primary_file.r2_key = NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '')
          AND NULLIF(BTRIM(COALESCE(mq_primary_file.meta_json->>'contract_week_id', '')), '') = row_ids.contract_week_id::text
        ORDER BY mq_primary_file.uploaded_at_utc DESC NULLS LAST, mq_primary_file.id DESC
        LIMIT 1
      ) AS primary_queue_file ON TRUE
      WHERE v_include_evidence = TRUE
      AND NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '') IS NOT NULL
      AND NOT (
        COALESCE(NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') LIKE 'evidence:%', FALSE)
        OR (
          row_ids.timesheet_id IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM public.timesheet_evidence AS te_primary_artifact
            WHERE te_primary_artifact.timesheet_id = row_ids.timesheet_id
              AND te_primary_artifact.storage_key = NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), '')
          )
        )
      )
    UNION ALL
    SELECT
      10::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', te0.id,
        'evidence_id', te0.id,
        'timesheet_id', te0.timesheet_id,
        'contract_week_id', NULL::uuid,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(te0.kind), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(te0.display_name), ''), 'Evidence'),
        'filename', COALESCE(NULLIF(BTRIM(te0.display_name), ''), 'Evidence'),
        'storage_key', te0.storage_key,
        'r2_key', te0.storage_key,
        'mime_type', NULL::text,
        'uploaded_at_utc', te0.created_at,
        'created_at', te0.created_at,
        'created_by', te0.created_by,
        'rotation', 0,
        'rotation_degrees', 0,
        'system', FALSE,
        'is_view_only', NOT COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_delete', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_reclassify', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_kind', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_type', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_return_to_queue', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'preview_mode', 'FILE',
        'source_label', 'Attached',
        'source_badge', 'Attached'
      ) AS item_json
    FROM public.timesheet_evidence AS te0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NOT NULL
      AND te0.timesheet_id = ids.timesheet_id
    UNION ALL
    SELECT
      20::integer AS sort_order,
      JSONB_BUILD_OBJECT(
        'id', mq0.id,
        'queue_id', mq0.id,
        'timesheet_id', NULL::uuid,
        'contract_week_id', ids.contract_week_id,
        'kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'staged_kind', UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')),
        'display_name', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'filename', COALESCE(NULLIF(BTRIM(mq0.original_filename), ''), 'Staged timesheet evidence'),
        'original_filename', mq0.original_filename,
        'storage_key', mq0.r2_key,
        'r2_key', mq0.r2_key,
        'mime_type', mq0.mime_type,
        'content_hash', mq0.content_hash,
        'uploaded_at_utc', mq0.uploaded_at_utc,
        'staged_at_utc', COALESCE(mq0.meta_json->>'staged_at_utc', mq0.uploaded_at_utc::text),
        'staged_by_user_id', mq0.uploaded_by_user_id,
        'rotation_degrees', COALESCE(mq0.last_rotation_deg::integer, 0),
        'page_count', CASE WHEN COALESCE(mq0.meta_json->>'page_count', '') ~ '^[0-9]+$' THEN (mq0.meta_json->>'page_count')::integer ELSE NULL::integer END,
        'status', mq0.status,
        'system', FALSE,
        'is_view_only', NOT COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_delete', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_reclassify', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_kind', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_edit_type', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_return_to_queue', COALESCE((ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'is_staged_context', TRUE,
        'preview_mode', 'PDF',
        'source_label', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged'),
        'source_badge', COALESCE(NULLIF(BTRIM(mq0.meta_json->>'source_label'), ''), 'Staged')
      ) AS item_json
    FROM public.manual_timesheet_queue AS mq0
    CROSS JOIN row_ids AS ids
    WHERE v_include_evidence = TRUE
      AND ids.timesheet_id IS NULL
      AND ids.contract_week_id IS NOT NULL
      AND UPPER(COALESCE(mq0.status, '')) = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') = ids.contract_week_id::text
  ),
  evidence_ranked AS (
    SELECT
      evidence_items.sort_order,
      evidence_items.item_json,
      ROW_NUMBER() OVER (ORDER BY evidence_items.sort_order ASC, evidence_items.item_json->>'id' ASC) AS rn
    FROM evidence_items
  ),
  evidence_payload AS (
    SELECT
      COALESCE(JSONB_AGG(evidence_ranked.item_json ORDER BY evidence_ranked.sort_order ASC, evidence_ranked.item_json->>'id' ASC), '[]'::jsonb) AS evidence_json,
      (
        SELECT er1.item_json
        FROM evidence_ranked AS er1
        WHERE er1.rn = 1
      ) AS primary_evidence_json
    FROM evidence_ranked
  ),
  timesheet_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'timesheet_id', timesheet_row.timesheet_id,
        'booking_id', timesheet_row.booking_id,
        'occupant_key_norm', timesheet_row.occupant_key_norm,
        'hospital_norm', timesheet_row.hospital_norm,
        'ward_norm', timesheet_row.ward_norm,
        'job_title_norm', timesheet_row.job_title_norm,
        'shift_label_norm', timesheet_row.shift_label_norm,
        'scheduled_start_iso', timesheet_row.scheduled_start_iso,
        'scheduled_end_iso', timesheet_row.scheduled_end_iso,
        'worked_start_iso', timesheet_row.worked_start_iso,
        'worked_end_iso', timesheet_row.worked_end_iso,
        'break_start_iso', timesheet_row.break_start_iso,
        'break_end_iso', timesheet_row.break_end_iso,
        'break_minutes', timesheet_row.break_minutes,
        'worked_minutes', timesheet_row.worked_minutes,
        'week_ending_date', timesheet_row.week_ending_date,
        'auth_name', timesheet_row.auth_name,
        'auth_job_title', timesheet_row.auth_job_title,
        'authorised_at_server', timesheet_row.authorised_at_server,
        'r2_nurse_key', timesheet_row.r2_nurse_key,
        'r2_auth_key', timesheet_row.r2_auth_key
      )
      || JSONB_BUILD_OBJECT(
        'reference_number', timesheet_row.reference_number,
        'reference_set_at', timesheet_row.reference_set_at,
        'status', timesheet_row.status,
        'created_at', timesheet_row.created_at,
        'updated_at', timesheet_row.updated_at,
        'version', timesheet_row.version,
        'is_current', timesheet_row.is_current,
        'contract_id', timesheet_row.contract_id,
        'submission_mode', timesheet_row.submission_mode,
        'manual_pdf_r2_key', timesheet_row.manual_pdf_r2_key,
        'line_type', timesheet_row.line_type,
        'sheet_scope', timesheet_row.sheet_scope,
        'actual_schedule_json', timesheet_row.actual_schedule_json,
        'additional_units_week', timesheet_row.additional_units_week,
        'additional_units_per_day', timesheet_row.additional_units_per_day,
        'qr_status', timesheet_row.qr_status,
        'qr_payload_json', timesheet_row.qr_payload_json,
        'qr_generated_at', timesheet_row.qr_generated_at,
        'qr_scanned_at', timesheet_row.qr_scanned_at,
        'qr_scan_info_json', timesheet_row.qr_scan_info_json,
        'qr_r2_key', timesheet_row.qr_r2_key,
        'day_references_json', timesheet_row.day_references_json,
        'manual_pdf_rotation_degrees', timesheet_row.manual_pdf_rotation_degrees
      )
      || JSONB_BUILD_OBJECT(
        'qr_last_sent_hash', timesheet_row.qr_last_sent_hash,
        'qr_last_sent_at_utc', timesheet_row.qr_last_sent_at_utc,
        'qr_signed_hash', timesheet_row.qr_signed_hash,
        'qr_signed_at_utc', timesheet_row.qr_signed_at_utc,
        'candidate_hint_text', timesheet_row.candidate_hint_text,
        'band', timesheet_row.band,
        'generated_pdf_at_utc', timesheet_row.generated_pdf_at_utc,
        'generated_pdf_refs_sig', timesheet_row.generated_pdf_refs_sig,
        'generated_pdf_refs_snapshot_json', timesheet_row.generated_pdf_refs_snapshot_json,
        'generated_pdf_refs_captured_at_utc', timesheet_row.generated_pdf_refs_captured_at_utc,
        'qr_sent_refs_sig', timesheet_row.qr_sent_refs_sig,
        'qr_sent_refs_snapshot_json', timesheet_row.qr_sent_refs_snapshot_json,
        'qr_sent_refs_captured_at_utc', timesheet_row.qr_sent_refs_captured_at_utc,
        'is_adjustment', timesheet_row.is_adjustment,
        'parent_timesheet_id', timesheet_row.parent_timesheet_id,
        'correction_id', timesheet_row.correction_id,
        'correction_kind', timesheet_row.correction_kind,
        'adjustment_origin', timesheet_row.adjustment_origin
      ) AS timesheet_json
    FROM timesheet_row
  ),
  tsfin_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', tsfin_row.id,
        'timesheet_id', tsfin_row.timesheet_id,
        'timesheet_version', tsfin_row.timesheet_version,
        'basis', tsfin_row.basis,
        'is_current', tsfin_row.is_current,
        'is_stale', tsfin_row.is_stale,
        'stale_reason', tsfin_row.stale_reason,
        'worked_start_iso', tsfin_row.worked_start_iso,
        'worked_end_iso', tsfin_row.worked_end_iso,
        'break_start_iso', tsfin_row.break_start_iso,
        'break_end_iso', tsfin_row.break_end_iso,
        'break_minutes', tsfin_row.break_minutes,
        'candidate_id', tsfin_row.candidate_id,
        'client_id', tsfin_row.client_id,
        'role', tsfin_row.role,
        'band', tsfin_row.band,
        'pay_method', tsfin_row.pay_method,
        'policy_snapshot_json', tsfin_row.policy_snapshot_json,
        'rate_source_refs_json', tsfin_row.rate_source_refs_json,
        'hours_day', tsfin_row.hours_day,
        'hours_night', tsfin_row.hours_night,
        'hours_sat', tsfin_row.hours_sat,
        'hours_sun', tsfin_row.hours_sun,
        'hours_bh', tsfin_row.hours_bh
      )
      || JSONB_BUILD_OBJECT(
        'pay_day', tsfin_row.pay_day,
        'pay_night', tsfin_row.pay_night,
        'pay_sat', tsfin_row.pay_sat,
        'pay_sun', tsfin_row.pay_sun,
        'pay_bh', tsfin_row.pay_bh,
        'charge_day', tsfin_row.charge_day,
        'charge_night', tsfin_row.charge_night,
        'charge_sat', tsfin_row.charge_sat,
        'charge_sun', tsfin_row.charge_sun,
        'charge_bh', tsfin_row.charge_bh,
        'total_hours', tsfin_row.total_hours,
        'total_pay_ex_vat', tsfin_row.total_pay_ex_vat,
        'total_charge_ex_vat', tsfin_row.total_charge_ex_vat,
        'margin_ex_vat', tsfin_row.margin_ex_vat,
        'computed_at_utc', tsfin_row.computed_at_utc,
        'locked_by_invoice_id', tsfin_row.locked_by_invoice_id,
        'locked_at_utc', tsfin_row.locked_at_utc,
        'unlocked_by_credit_note_id', tsfin_row.unlocked_by_credit_note_id,
        'created_at', tsfin_row.created_at,
        'updated_at', tsfin_row.updated_at,
        'occupant_key_norm', tsfin_row.occupant_key_norm,
        'candidate_assignment', tsfin_row.candidate_assignment,
        'processing_status', tsfin_row.processing_status
      )
      || JSONB_BUILD_OBJECT(
        'expenses_pay_ex_vat', tsfin_row.expenses_pay_ex_vat,
        'expenses_charge_ex_vat', tsfin_row.expenses_charge_ex_vat,
        'expenses_description', tsfin_row.expenses_description,
        'expenses_evidence_r2_key', tsfin_row.expenses_evidence_r2_key,
        'mileage_pay_ex_vat', tsfin_row.mileage_pay_ex_vat,
        'mileage_charge_ex_vat', tsfin_row.mileage_charge_ex_vat,
        'mileage_evidence_r2_key', tsfin_row.mileage_evidence_r2_key,
        'mileage_pay_rate', tsfin_row.mileage_pay_rate,
        'mileage_charge_rate', tsfin_row.mileage_charge_rate,
        'mileage_units', tsfin_row.mileage_units,
        'travel_pay_ex_vat', tsfin_row.travel_pay_ex_vat,
        'travel_charge_ex_vat', tsfin_row.travel_charge_ex_vat,
        'accommodation_pay_ex_vat', tsfin_row.accommodation_pay_ex_vat,
        'accommodation_charge_ex_vat', tsfin_row.accommodation_charge_ex_vat,
        'other_pay_ex_vat', tsfin_row.other_pay_ex_vat,
        'other_charge_ex_vat', tsfin_row.other_charge_ex_vat,
        'po_number', tsfin_row.po_number,
        'pay_on_hold', tsfin_row.pay_on_hold,
        'pay_on_hold_reason', tsfin_row.pay_on_hold_reason,
        'pay_on_hold_since_utc', tsfin_row.pay_on_hold_since_utc,
        'paid_at_utc', tsfin_row.paid_at_utc,
        'paid_by_user_id', tsfin_row.paid_by_user_id,
        'payment_reference', tsfin_row.payment_reference
      )
      || JSONB_BUILD_OBJECT(
        'remittance_last_sent_at_utc', tsfin_row.remittance_last_sent_at_utc,
        'remittance_send_count', tsfin_row.remittance_send_count,
        'pay_wtr_rate_pct_snapshot', tsfin_row.pay_wtr_rate_pct_snapshot,
        'pay_vat_rate_pct_snapshot', tsfin_row.pay_vat_rate_pct_snapshot,
        'pay_vat_amount_snapshot', tsfin_row.pay_vat_amount_snapshot,
        'pay_total_inc_vat_snapshot', tsfin_row.pay_total_inc_vat_snapshot,
        'processed_by_user_id', tsfin_row.processed_by_user_id,
        'processed_at_utc', tsfin_row.processed_at_utc,
        'authorised_by_user_id', tsfin_row.authorised_by_user_id,
        'authorised_at_utc', tsfin_row.authorised_at_utc,
        'expenses_evidence_manifest', tsfin_row.expenses_evidence_manifest,
        'mileage_evidence_manifest', tsfin_row.mileage_evidence_manifest,
        'actual_schedule_json', tsfin_row.actual_schedule_json,
        'actual_minutes_by_day_json', tsfin_row.actual_minutes_by_day_json,
        'additional_units_json', tsfin_row.additional_units_json,
        'additional_pay_ex_vat', tsfin_row.additional_pay_ex_vat,
        'additional_charge_ex_vat', tsfin_row.additional_charge_ex_vat,
        'additional_margin_ex_vat', tsfin_row.additional_margin_ex_vat,
        'invoice_breakdown_json', tsfin_row.invoice_breakdown_json,
        'nhsp_import_id', tsfin_row.nhsp_import_id,
        'has_rate_issue', tsfin_row.has_rate_issue,
        'has_pay_channel_issue', tsfin_row.has_pay_channel_issue,
        'hr_crosscheck_status', tsfin_row.hr_crosscheck_status,
        'hr_crosscheck_issues', tsfin_row.hr_crosscheck_issues,
        'external_source_rows_json', tsfin_row.external_source_rows_json
      ) AS tsfin_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN NULL::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN JSONB_BUILD_OBJECT('mode', 'SEGMENTS', 'segments', tsfin_row.invoice_breakdown_json)
        ELSE NULL::jsonb
      END AS invoice_breakdown_json,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND UPPER(COALESCE(tsfin_row.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS' THEN TRUE
        WHEN tsfin_row.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN TRUE
        ELSE FALSE
      END AS is_segments_mode,
      CASE
        WHEN tsfin_row.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'array' THEN tsfin_row.invoice_breakdown_json
        WHEN jsonb_typeof(tsfin_row.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(tsfin_row.invoice_breakdown_json->'segments') = 'array' THEN tsfin_row.invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END AS segments_json
    FROM tsfin_row
  ),
  contract_week_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_week_row.id,
        'contract_id', contract_week_row.contract_id,
        'week_ending_date', contract_week_row.week_ending_date,
        'additional_seq', contract_week_row.additional_seq,
        'status', contract_week_row.status,
        'submission_mode_snapshot', contract_week_row.submission_mode_snapshot,
        'timesheet_id', contract_week_row.timesheet_id,
        'uploaded_pdf_r2_key', contract_week_row.uploaded_pdf_r2_key,
        'day_entries_json', contract_week_row.day_entries_json,
        'totals_json', contract_week_row.totals_json,
        'created_at', contract_week_row.created_at,
        'updated_at', contract_week_row.updated_at,
        'planned_schedule_json', contract_week_row.planned_schedule_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'is_adjustment', contract_week_row.is_adjustment,
        'enforce_day_partition', contract_week_row.enforce_day_partition,
        'allowed_days_mask', contract_week_row.allowed_days_mask,
        'split_boundary_date', contract_week_row.split_boundary_date,
        'worker_note', contract_week_row.worker_note,
        'split_group_key', contract_week_row.split_group_key,
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display'
      ) AS contract_week_json
    FROM contract_week_row
    CROSS JOIN row_ids
    LEFT JOIN contract_row ON TRUE
  ),
  contract_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'id', contract_row.id,
        'candidate_id', contract_row.candidate_id,
        'client_id', contract_row.client_id,
        'role', contract_row.role,
        'band', contract_row.band,
        'display_site', contract_row.display_site,
        'ward_hint', contract_row.ward_hint,
        'start_date', contract_row.start_date,
        'end_date', contract_row.end_date,
        'pay_method_snapshot', contract_row.pay_method_snapshot,
        'rates_json', contract_row.rates_json,
        'std_hours_json', contract_row.std_hours_json,
        'default_submission_mode', contract_row.default_submission_mode,
        'week_ending_weekday_snapshot', contract_row.week_ending_weekday_snapshot,
        'auto_invoice', contract_row.auto_invoice,
        'require_reference_to_pay', contract_row.require_reference_to_pay,
        'require_reference_to_invoice', contract_row.require_reference_to_invoice,
        'bucket_labels_json', contract_row.bucket_labels_json,
        'std_schedule_json', contract_row.std_schedule_json,
        'mileage_pay_rate', contract_row.mileage_pay_rate,
        'mileage_charge_rate', contract_row.mileage_charge_rate,
        'additional_rates_json', contract_row.additional_rates_json,
        'self_bill', contract_row.self_bill,
        'weekly_timesheet_source', contract_row.weekly_timesheet_source,
        'no_timesheet_required', contract_row.no_timesheet_required,
        'daily_calc_of_invoices', contract_row.daily_calc_of_invoices,
        'group_nightsat_sunbh', contract_row.group_nightsat_sunbh,
        'is_nhsp', contract_row.is_nhsp,
        'autoprocess_hr', contract_row.autoprocess_hr,
        'requires_hr', contract_row.requires_hr,
        'hr_attach_to_invoice', contract_row.hr_attach_to_invoice,
        'ts_attach_to_invoice', contract_row.ts_attach_to_invoice,
        'overrideclientsettings', contract_row.overrideclientsettings,
        'reference_number_required_to_issue_invoice', contract_row.reference_number_required_to_issue_invoice,
        'send_manual_invoices_to_different_email', contract_row.send_manual_invoices_to_different_email,
        'manual_invoices_alt_email_address', contract_row.manual_invoices_alt_email_address,
        'is_ad_hoc', contract_row.is_ad_hoc,
        'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
        'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour'
      ) AS contract_json
    FROM contract_row
    CROSS JOIN row_ids
  ),
  related_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'counts', JSONB_BUILD_OBJECT(),
        'candidate', CASE WHEN candidate_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', candidate_row.id,
          'tms_ref', candidate_row.tms_ref,
          'first_name', candidate_row.first_name,
          'last_name', candidate_row.last_name,
          'display_name', candidate_row.display_name,
          'email', candidate_row.email,
          'phone', candidate_row.phone,
          'pay_method', candidate_row.pay_method,
          'umbrella_id', candidate_row.umbrella_id,
          'active', candidate_row.active,
          'key_norm', candidate_row.key_norm,
          'mileage_pay_rate', candidate_row.mileage_pay_rate,
          'job_title_id', candidate_row.job_title_id,
          'prof_reg_number', candidate_row.prof_reg_number,
          'prof_reg_type', candidate_row.prof_reg_type,
          'ni_number', candidate_row.ni_number,
          'date_of_birth', candidate_row.date_of_birth,
          'gender', candidate_row.gender,
          'title', candidate_row.title,
          'opt_in_email', candidate_row.opt_in_email,
          'opt_in_sms', candidate_row.opt_in_sms,
          'opt_in_whatsapp', candidate_row.opt_in_whatsapp,
          'band', candidate_row.band,
          'tms_ref_num', candidate_row.tms_ref_num
        ) END,
        'client', CASE WHEN client_row.id IS NULL THEN NULL::jsonb ELSE JSONB_BUILD_OBJECT(
          'id', client_row.id,
          'cli_ref', client_row.cli_ref,
          'name', client_row.name,
          'invoice_address', client_row.invoice_address,
          'primary_invoice_email', client_row.primary_invoice_email,
          'ap_phone', client_row.ap_phone,
          'vat_chargeable', client_row.vat_chargeable,
          'payment_terms_days', client_row.payment_terms_days,
          'mileage_charge_rate', client_row.mileage_charge_rate,
          'ts_queries_email', client_row.ts_queries_email,
          'client_address', client_row.client_address,
          'contact_title', client_row.contact_title,
          'contact_known_as', client_row.contact_known_as,
          'contact_forename', client_row.contact_forename,
          'contact_surname', client_row.contact_surname,
          'contact_job_title', client_row.contact_job_title,
          'contact_tel', client_row.contact_tel,
          'contact_mobile', client_row.contact_mobile,
          'contact_email', client_row.contact_email,
          'website', client_row.website
        ) END,
        'contract', COALESCE(contract_payload.contract_json, NULL::jsonb),
        'invoices', '[]'::jsonb,
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT(),
        'invoice', NULL::jsonb,
        'umbrella', NULL::jsonb,
        'series', '[]'::jsonb
      ) AS related_json
    FROM row_ids
    LEFT JOIN candidate_row ON TRUE
    LEFT JOIN client_row ON TRUE
    LEFT JOIN contract_payload ON TRUE
  )
,
  shift_rows AS (
    SELECT
      ns0.id,
      ns0.external_row_key,
      ns0.latest_import_id,
      ns0.candidate_id,
      ns0.client_id,
      ns0.contract_id,
      ns0.timesheet_id,
      ns0.work_date,
      ns0.ward,
      ns0.start_utc,
      ns0.end_utc,
      ns0.break_mins,
      ns0.pay_minutes,
      ns0.pay_amount_snapshot,
      ns0.charge_amount_snapshot,
      ns0.invoice_status,
      ns0.defer_until_run_after,
      ns0.invoice_id,
      ns0.created_at,
      ns0.updated_at,
      ns0.source_system,
      ns0.hr_request_id,
      ns0.held_back_reason,
      ns0.staff_name,
      ns0.staff_norm,
      ns0.ward_norm,
      ns0.assignment_code,
      ns0.ref_num,
      ns0.week_ending_date,
      ns0.cancelled_at_utc,
      ns0.cancelled_by_import_id,
      ns0.cancelled_reason
    FROM public.nhsp_shifts AS ns0
    CROSS JOIN row_ids AS ids
    WHERE (v_include_compare = TRUE OR v_include_import_source_rows = TRUE)
      AND (
        (v_include_compare = TRUE AND COALESCE((ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE)
        OR (v_include_import_source_rows = TRUE AND UPPER(COALESCE(ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE')
      )
      AND (
        (ids.timesheet_id IS NOT NULL AND ns0.timesheet_id = ids.timesheet_id)
        OR (
          ids.timesheet_id IS NULL
          AND ids.contract_id IS NOT NULL
          AND ns0.contract_id = ids.contract_id
          AND ns0.week_ending_date = CASE
            WHEN COALESCE(ids.row_json->>'week_ending_date', '') ~ '^\d{4}-\d{2}-\d{2}$' THEN (ids.row_json->>'week_ending_date')::date
            WHEN COALESCE(ids.row_json->>'contract_week_ending_date', '') ~ '^\d{4}-\d{2}-\d{2}$' THEN (ids.row_json->>'contract_week_ending_date')::date
            ELSE NULL::date
          END
        )
      )
    ORDER BY ns0.work_date ASC NULLS LAST, ns0.start_utc ASC NULLS LAST, ns0.id ASC
  ),
  shifts_payload AS (
    SELECT
      COALESCE(JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', shift_rows.id,
          'external_row_key', shift_rows.external_row_key,
          'latest_import_id', shift_rows.latest_import_id,
          'candidate_id', shift_rows.candidate_id,
          'client_id', shift_rows.client_id,
          'contract_id', shift_rows.contract_id,
          'timesheet_id', shift_rows.timesheet_id,
          'work_date', shift_rows.work_date,
          'ward', shift_rows.ward,
          'start_utc', shift_rows.start_utc,
          'end_utc', shift_rows.end_utc,
          'break_mins', shift_rows.break_mins,
          'pay_minutes', shift_rows.pay_minutes,
          'pay_amount_snapshot', shift_rows.pay_amount_snapshot,
          'charge_amount_snapshot', shift_rows.charge_amount_snapshot,
          'invoice_status', shift_rows.invoice_status,
          'defer_until_run_after', shift_rows.defer_until_run_after,
          'invoice_id', shift_rows.invoice_id,
          'created_at', shift_rows.created_at,
          'updated_at', shift_rows.updated_at,
          'source_system', shift_rows.source_system,
          'hr_request_id', shift_rows.hr_request_id,
          'held_back_reason', shift_rows.held_back_reason,
          'staff_name', shift_rows.staff_name,
          'staff_norm', shift_rows.staff_norm,
          'ward_norm', shift_rows.ward_norm,
          'assignment_code', shift_rows.assignment_code,
          'ref_num', shift_rows.ref_num,
          'week_ending_date', shift_rows.week_ending_date,
          'cancelled_at_utc', shift_rows.cancelled_at_utc,
          'cancelled_by_import_id', shift_rows.cancelled_by_import_id,
          'cancelled_reason', shift_rows.cancelled_reason
        ) ORDER BY shift_rows.work_date ASC NULLS LAST, shift_rows.start_utc ASC NULLS LAST, shift_rows.id ASC
      ), '[]'::jsonb) AS shifts_json,
      JSONB_BUILD_OBJECT(
        'import_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr1.latest_import_id::text)) FROM shift_rows AS sr1 WHERE sr1.latest_import_id IS NOT NULL), '[]'::jsonb),
        'source_systems', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr2.source_system::text)) FROM shift_rows AS sr2 WHERE sr2.source_system IS NOT NULL), '[]'::jsonb),
        'shift_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr3.id::text)) FROM shift_rows AS sr3 WHERE sr3.id IS NOT NULL), '[]'::jsonb),
        'external_row_keys', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr4.external_row_key)) FROM shift_rows AS sr4 WHERE NULLIF(BTRIM(COALESCE(sr4.external_row_key, '')), '') IS NOT NULL), '[]'::jsonb),
        'hr_request_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr5.hr_request_id)) FROM shift_rows AS sr5 WHERE NULLIF(BTRIM(COALESCE(sr5.hr_request_id, '')), '') IS NOT NULL), '[]'::jsonb),
        'work_dates', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr6.work_date::text)) FROM shift_rows AS sr6 WHERE sr6.work_date IS NOT NULL), '[]'::jsonb),
        'ref_nums', COALESCE((SELECT JSONB_AGG(TO_JSONB(sr7.ref_num)) FROM shift_rows AS sr7 WHERE NULLIF(BTRIM(COALESCE(sr7.ref_num, '')), '') IS NOT NULL), '[]'::jsonb)
      ) AS imported_detail_refs_json
    FROM shift_rows
  ),
  base_payload AS (
    SELECT
      JSONB_BUILD_OBJECT(
        'ok', TRUE,
        'context_kind', 'bulk_authorise_row_context',
        'context_profile', v_profile,
        'profile', v_profile,
        'context_type', 'bulk_authorise',
        'slim_context', TRUE,
        'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
        'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
        'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
        'current_version', CASE WHEN COALESCE(row_ids.row_json->>'timesheet_version', timesheet_payload.timesheet_json->>'version', '') ~ '^[0-9]+$' THEN COALESCE(row_ids.row_json->>'timesheet_version', timesheet_payload.timesheet_json->>'version')::integer ELSE NULL::integer END,
        'contract_week_id', row_ids.row_json->>'contract_week_id',
        'row_key', row_ids.row_json->>'row_key',
        'row_signature', row_ids.row_json->>'row_signature',
        'was_stale', COALESCE((row_ids.row_json->>'was_stale')::boolean, FALSE),
        'has_timesheet', COALESCE((row_ids.row_json->>'has_timesheet')::boolean, FALSE),
        'locked', COALESCE((row_ids.row_json->>'locked')::boolean, FALSE),
        'bulk_process_bucket', row_ids.row_json->>'bulk_process_bucket',
        'bulk_authorise_classification', row_ids.row_json->>'bulk_authorise_classification',
        'bulk_authorise_section', row_ids.row_json->>'bulk_authorise_section',
        'route_family', row_ids.row_json->>'route_family',
        'route_subfamily', row_ids.row_json->>'route_subfamily',
        'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
        'is_import_authoritative', COALESCE((row_ids.row_json->>'is_import_authoritative')::boolean, FALSE),
        'compare_block_required', COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE),
        'period_type', row_ids.row_json->>'period_type',
        'timesheet_type_sort_key', CASE WHEN NULLIF(row_ids.row_json->>'timesheet_type_sort_key', '') IS NULL THEN NULL::integer ELSE (row_ids.row_json->>'timesheet_type_sort_key')::integer END,
        'can_save', COALESCE((row_ids.row_json->>'can_save')::boolean, FALSE),
        'can_process', COALESCE((row_ids.row_json->>'can_process')::boolean, FALSE),
        'can_unprocess', COALESCE((row_ids.row_json->>'can_unprocess')::boolean, FALSE),
        'can_edit_timesheet_data', COALESCE((row_ids.row_json->>'can_edit_timesheet_data')::boolean, FALSE),
        'can_manage_evidence', COALESCE((row_ids.row_json->>'can_manage_evidence')::boolean, FALSE),
        'can_add_additional_manual', COALESCE((row_ids.row_json->>'can_add_additional_manual')::boolean, FALSE),
        'review_only', COALESCE((row_ids.row_json->>'review_only')::boolean, FALSE),
        'hr_validation_required_for_invoice', COALESCE((row_ids.row_json->>'hr_validation_required_for_invoice')::boolean, FALSE),
        'validation_status', row_ids.row_json->>'validation_status',
        'validation_pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
        'has_deviation_marker', COALESCE((row_ids.row_json->>'has_deviation_marker')::boolean, FALSE),
        'deviation_marker_reason', row_ids.row_json->>'deviation_marker_reason'
      )
      || JSONB_BUILD_OBJECT(
        'can_bulk_authorise', COALESCE((row_ids.row_json->>'can_bulk_authorise')::boolean, FALSE),
        'can_bulk_unauthorise', COALESCE((row_ids.row_json->>'can_bulk_unauthorise')::boolean, FALSE),
        'requires_authorisation', COALESCE((row_ids.row_json->>'requires_authorisation')::boolean, FALSE),
        'is_authorised', COALESCE((row_ids.row_json->>'is_authorised')::boolean, FALSE),
        'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
        'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
        'qr_unsigned_blocked', COALESCE((row_ids.row_json->>'qr_unsigned_blocked')::boolean, FALSE),
        'qr_signed_returned', COALESCE((row_ids.row_json->>'qr_signed_returned')::boolean, FALSE),
        'row', row_ids.row_json,
        'row_patch', COALESCE(row_ids.row_json->'row_patch', JSONB_BUILD_OBJECT()),
        'details', (
          JSONB_BUILD_OBJECT(
            'requested_timesheet_id', row_ids.row_json->>'requested_timesheet_id',
            'current_timesheet_id', row_ids.row_json->>'current_timesheet_id',
            'expected_timesheet_id', row_ids.row_json->>'expected_timesheet_id',
            'current_version', CASE WHEN COALESCE(row_ids.row_json->>'timesheet_version', timesheet_payload.timesheet_json->>'version', '') ~ '^[0-9]+$' THEN COALESCE(row_ids.row_json->>'timesheet_version', timesheet_payload.timesheet_json->>'version')::integer ELSE NULL::integer END,
            'was_stale', COALESCE((row_ids.row_json->>'was_stale')::boolean, FALSE),
            'booking_id', row_ids.row_json->>'booking_id',
            'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
            'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
            'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
            'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
            'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
            'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT()
          )
          || JSONB_BUILD_OBJECT(
            'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
            'validation_summary', JSONB_BUILD_OBJECT(
              'status', row_ids.row_json->>'validation_status',
              'pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
              'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
              'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
              'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
            ),
            'shifts', CASE
              WHEN v_include_import_source_rows = TRUE
               AND (COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE OR UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE') THEN COALESCE(shifts_payload.shifts_json, '[]'::jsonb)
              ELSE '[]'::jsonb
            END,
            'contract_week_id', row_ids.row_json->>'contract_week_id',
            'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
            'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
            'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb)
          )
          || JSONB_BUILD_OBJECT(
            'policy', (
              CASE
                WHEN tsfin_payload.tsfin_json IS NOT NULL
                 AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
                ELSE JSONB_BUILD_OBJECT()
              END
              || JSONB_BUILD_OBJECT(
                'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
                'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
                'requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
                'autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
                'no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
                'is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE)
              )
            ),
            'effective', JSONB_BUILD_OBJECT(
              'route_type', row_ids.row_json->>'route_type',
              'route_display', row_ids.row_json->>'route_display',
              'summary_stage', row_ids.row_json->>'summary_stage',
              'client_requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
              'client_autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
              'client_no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
              'client_is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE),
              'contract_id', row_ids.row_json->>'contract_id',
              'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
              'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
            ),
            'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
            'summary_stage', row_ids.row_json->>'summary_stage',
            'route_type', row_ids.row_json->>'route_type',
            'route_display', row_ids.row_json->>'route_display',
            'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
            'sheet_scope', row_ids.row_json->>'sheet_scope',
            'period_type', row_ids.row_json->>'period_type'
          )
          || JSONB_BUILD_OBJECT(
            'qr_status', row_ids.row_json->>'qr_status',
            'qr_generated_at', row_ids.row_json->>'qr_generated_at',
            'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
            'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
            'uploaded_pdf_r2_key', row_ids.row_json->>'uploaded_pdf_r2_key',
            'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
            'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees',
            'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
            'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
            'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
            'healthroster_compare', CASE
              WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
                'required', TRUE,
                'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
                'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
              )
              ELSE JSONB_BUILD_OBJECT(
                'required', FALSE,
                'rows', '[]'::jsonb,
                'imported_detail_refs', JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb)
              )
            END,
            'import_source_rows', CASE
              WHEN v_include_import_source_rows = TRUE AND UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE' THEN COALESCE(tsfin_payload.tsfin_json->'external_source_rows_json', '[]'::jsonb)
              ELSE NULL::jsonb
            END
          )
          || JSONB_BUILD_OBJECT(
            'primary_artifact', CASE
              WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
                'id', row_ids.row_json->>'primary_artifact_id',
                'kind', row_ids.row_json->>'primary_artifact_kind',
                'display_name', row_ids.row_json->>'primary_artifact_display_name',
                'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
                'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
              )
              ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
            END,
            'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
            'pay_state', NULL::jsonb,
            'segment_snoozes', '[]'::jsonb
          )
        ),
        'timesheet', COALESCE(timesheet_payload.timesheet_json, NULL::jsonb),
        'tsfin', COALESCE(tsfin_payload.tsfin_json, NULL::jsonb),
        'invoiceBreakdown', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'invoice_breakdown_json', COALESCE(tsfin_payload.invoice_breakdown_json, NULL::jsonb),
        'isSegmentsMode', COALESCE(tsfin_payload.is_segments_mode, FALSE),
        'segments', COALESCE(tsfin_payload.segments_json, '[]'::jsonb),
        'invoice_no_by_invoice_id', JSONB_BUILD_OBJECT()
      )
      || JSONB_BUILD_OBJECT(
        'validations', COALESCE(validation_payload.validations_json, '[]'::jsonb),
        'validation_summary', JSONB_BUILD_OBJECT(
          'status', row_ids.row_json->>'validation_status',
          'pre_validated', COALESCE((row_ids.row_json->>'validation_pre_validated')::boolean, FALSE),
          'hr_validation_satisfied', COALESCE((row_ids.row_json->>'hr_validation_satisfied')::boolean, FALSE),
          'hr_validation_awaiting', COALESCE((row_ids.row_json->>'hr_validation_awaiting')::boolean, FALSE),
          'latest', COALESCE(validation_payload.latest_validation_json, NULL::jsonb)
        ),
        'shifts', CASE
          WHEN v_include_import_source_rows = TRUE
           AND (COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE OR UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE') THEN COALESCE(shifts_payload.shifts_json, '[]'::jsonb)
          ELSE '[]'::jsonb
        END,
        'healthroster_compare', CASE
          WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
            'required', TRUE,
            'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
            'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
          )
          ELSE JSONB_BUILD_OBJECT(
            'required', FALSE,
            'rows', '[]'::jsonb,
            'imported_detail_refs', JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb)
          )
        END,
        'compare_payload', CASE
          WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
            'required', TRUE,
            'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
            'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
          )
          ELSE NULL::jsonb
        END,
        'import_source_rows', CASE
          WHEN v_include_import_source_rows = TRUE AND UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE' THEN COALESCE(tsfin_payload.tsfin_json->'external_source_rows_json', '[]'::jsonb)
          ELSE NULL::jsonb
        END
      )
      || JSONB_BUILD_OBJECT(
        'effective', JSONB_BUILD_OBJECT(
          'route_type', row_ids.row_json->>'route_type',
          'route_display', row_ids.row_json->>'route_display',
          'summary_stage', row_ids.row_json->>'summary_stage',
          'client_requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
          'client_autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
          'client_no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
          'client_is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE),
          'contract_id', row_ids.row_json->>'contract_id',
          'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
          'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb)
        ),
        'ready_to_pay', COALESCE((row_ids.row_json->>'ready_to_pay')::boolean, FALSE),
        'summary_stage', row_ids.row_json->>'summary_stage',
        'route_type', row_ids.row_json->>'route_type',
        'route_display', row_ids.row_json->>'route_display',
        'issue_codes', COALESCE(row_ids.row_json->'issue_codes', '[]'::jsonb),
        'sheet_scope', row_ids.row_json->>'sheet_scope',
        'period_type', row_ids.row_json->>'period_type',
        'qr_status', row_ids.row_json->>'qr_status',
        'qr_generated_at', row_ids.row_json->>'qr_generated_at',
        'qr_scanned_at', row_ids.row_json->>'qr_scanned_at',
        'manual_pdf_r2_key', row_ids.row_json->>'manual_pdf_r2_key',
        'uploaded_pdf_r2_key', row_ids.row_json->>'uploaded_pdf_r2_key',
        'generated_pdf_at_utc', row_ids.row_json->>'generated_pdf_at_utc',
        'manual_pdf_rotation_degrees', row_ids.row_json->>'manual_pdf_rotation_degrees'
      )
      || JSONB_BUILD_OBJECT(
        'contract_week', COALESCE(contract_week_payload.contract_week_json, NULL::jsonb),
        'policy', (
          CASE
            WHEN tsfin_payload.tsfin_json IS NOT NULL
             AND jsonb_typeof(tsfin_payload.tsfin_json->'policy_snapshot_json') = 'object' THEN tsfin_payload.tsfin_json->'policy_snapshot_json'
            ELSE JSONB_BUILD_OBJECT()
          END
          || JSONB_BUILD_OBJECT(
            'weekly_mode', row_ids.row_json->>'contract_weekly_mode',
            'hr_weekly_behaviour', row_ids.row_json->>'contract_hr_weekly_behaviour',
            'requires_hr', COALESCE((row_ids.row_json->>'client_requires_hr')::boolean, FALSE),
            'autoprocess_hr', COALESCE((row_ids.row_json->>'client_autoprocess_hr')::boolean, FALSE),
            'no_timesheet_required', COALESCE((row_ids.row_json->>'client_no_timesheet_required')::boolean, FALSE),
            'is_nhsp', COALESCE((row_ids.row_json->>'client_is_nhsp')::boolean, FALSE)
          )
        ),
        'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
        'evidence_meta', JSONB_BUILD_OBJECT(
          'has_any_evidence', COALESCE((row_ids.row_json->>'has_any_evidence')::boolean, FALSE),
          'evidence_badges', COALESCE(row_ids.row_json->'evidence_badges', '[]'::jsonb),
          'attached_evidence_count', COALESCE(NULLIF(row_ids.row_json->>'attached_evidence_count', '')::integer, 0),
          'queue_staged_count', COALESCE(NULLIF(row_ids.row_json->>'queue_staged_count', '')::integer, 0),
          'evidence_document_locked', COALESCE((row_ids.row_json->>'evidence_document_locked')::boolean, FALSE),
          'evidence_lock_reason', row_ids.row_json->>'evidence_lock_reason'
        ),
        'primary_artifact', CASE
          WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
            'id', row_ids.row_json->>'primary_artifact_id',
            'kind', row_ids.row_json->>'primary_artifact_kind',
            'display_name', row_ids.row_json->>'primary_artifact_display_name',
            'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
            'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
          )
          ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
        END,
        'primary_artifact_id', row_ids.row_json->>'primary_artifact_id',
        'primary_artifact_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_artifact_preview_mode', row_ids.row_json->>'primary_artifact_preview_mode',
        'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
        'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', row_ids.row_json->>'route_family',
          'route_subfamily', row_ids.row_json->>'route_subfamily',
          'underlying_channel_family', row_ids.row_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE((row_ids.row_json->>'is_import_authoritative')::boolean, FALSE),
          'compare_block_required', COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE),
          'primary_artifact', CASE
            WHEN NULLIF(BTRIM(COALESCE(row_ids.row_json->>'primary_artifact_id', '')), '') IS NOT NULL THEN JSONB_BUILD_OBJECT(
              'id', row_ids.row_json->>'primary_artifact_id',
              'kind', row_ids.row_json->>'primary_artifact_kind',
              'display_name', row_ids.row_json->>'primary_artifact_display_name',
              'storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
              'preview_mode', row_ids.row_json->>'primary_artifact_preview_mode'
            )
            ELSE COALESCE(evidence_payload.primary_evidence_json, NULL::jsonb)
          END,
          'preview_storage_key', COALESCE(NULLIF(BTRIM(row_ids.row_json->>'primary_artifact_storage_key'), ''), evidence_payload.primary_evidence_json->>'storage_key'),
          'primary_left_pane_mode', row_ids.row_json->>'primary_left_pane_mode',
          'evidence', COALESCE(evidence_payload.evidence_json, '[]'::jsonb),
          'compare_payload', CASE
            WHEN v_include_compare = TRUE AND COALESCE((row_ids.row_json->>'compare_block_required')::boolean, FALSE) = TRUE THEN JSONB_BUILD_OBJECT(
              'required', TRUE,
              'rows', COALESCE(shifts_payload.shifts_json, '[]'::jsonb),
              'imported_detail_refs', COALESCE(shifts_payload.imported_detail_refs_json, JSONB_BUILD_OBJECT('import_ids', '[]'::jsonb, 'source_systems', '[]'::jsonb, 'shift_ids', '[]'::jsonb, 'external_row_keys', '[]'::jsonb, 'hr_request_ids', '[]'::jsonb, 'work_dates', '[]'::jsonb, 'ref_nums', '[]'::jsonb))
            )
            ELSE NULL::jsonb
          END,
          'source_items', CASE
            WHEN v_include_import_source_rows = TRUE AND UPPER(COALESCE(row_ids.row_json->>'route_family', '')) = 'IMPORT_AUTHORITATIVE' THEN COALESCE(tsfin_payload.tsfin_json->'external_source_rows_json', '[]'::jsonb)
            ELSE '[]'::jsonb
          END
        )
      )
      || JSONB_BUILD_OBJECT(
        'action_flags', COALESCE(row_ids.row_json->'action_flags', JSONB_BUILD_OBJECT()),
        'bulk_authorise', COALESCE(row_ids.row_json->'bulk_authorise', JSONB_BUILD_OBJECT()),
        'artifact_hints', COALESCE(row_ids.row_json->'artifact_hints', JSONB_BUILD_OBJECT()),
        'related', COALESCE(related_payload.related_json, JSONB_BUILD_OBJECT()),
        'pay_state', NULL::jsonb,
        'segment_snoozes', '[]'::jsonb,
        'is_advanced', FALSE,
        'can_unadvance', FALSE,
        'advanced_consumed_by_batch_id', NULL::text,
        'is_snoozed', FALSE,
        'snooze_until_date', NULL::text,
        'snooze_is_indefinite', FALSE,
        'snooze_note', NULL::text
      ) AS payload_json
    FROM decision_row
    CROSS JOIN row_ids
    LEFT JOIN timesheet_payload ON TRUE
    LEFT JOIN tsfin_payload ON TRUE
    LEFT JOIN contract_week_payload ON TRUE
    LEFT JOIN validation_payload ON TRUE
    LEFT JOIN evidence_payload ON TRUE
    LEFT JOIN related_payload ON TRUE
    LEFT JOIN shifts_payload ON TRUE
  ),
  final_payload AS (
    SELECT
      base_payload.payload_json
      || JSONB_BUILD_OBJECT(
        'details', COALESCE(base_payload.payload_json->'details', JSONB_BUILD_OBJECT()),
        'left_pane', JSONB_BUILD_OBJECT(
          'route_family', base_payload.payload_json->>'route_family',
          'route_subfamily', base_payload.payload_json->>'route_subfamily',
          'underlying_channel_family', base_payload.payload_json->>'underlying_channel_family',
          'is_import_authoritative', COALESCE((base_payload.payload_json->>'is_import_authoritative')::boolean, FALSE),
          'compare_block_required', COALESCE((base_payload.payload_json->>'compare_block_required')::boolean, FALSE),
          'primary_artifact', COALESCE(base_payload.payload_json->'primary_artifact', NULL::jsonb),
          'source_items', CASE
            WHEN v_include_import_source_rows = TRUE THEN COALESCE(NULLIF(base_payload.payload_json->'import_source_rows', 'null'::jsonb), base_payload.payload_json->'shifts', '[]'::jsonb)
            ELSE '[]'::jsonb
          END,
          'primary_left_pane_mode', base_payload.payload_json->>'primary_left_pane_mode'
        ),
        'compare_payload', CASE
          WHEN v_include_compare = TRUE THEN COALESCE(base_payload.payload_json->'healthroster_compare', JSONB_BUILD_OBJECT('required', FALSE, 'rows', '[]'::jsonb, 'imported_detail_refs', JSONB_BUILD_OBJECT()))
          ELSE NULL::jsonb
        END,
        'data_row', COALESCE(base_payload.payload_json->'row', JSONB_BUILD_OBJECT())
      ) AS payload_json
    FROM base_payload
  )
  SELECT final_payload.payload_json
    INTO v_out
  FROM final_payload;

  IF v_out IS NOT NULL THEN
    v_out := v_out || JSONB_BUILD_OBJECT(
      'header_loaded', TRUE,
      'header_only', FALSE,
      'editor_loaded', TRUE,
      'evidence_loaded', COALESCE(v_include_evidence, FALSE),
      'compare_loaded', COALESCE(v_include_compare, FALSE),
      'full_loaded', (v_profile = 'full'),
      'schedule_pending', FALSE,
      'schedule_authoritative', TRUE,
      'soft_failure', FALSE,
      'context_degraded', FALSE,
      'degraded_reason', NULL::text,
      'loaded_layers', CASE
        WHEN v_profile = 'full' THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence', 'compare_import', 'full')
        WHEN COALESCE(v_include_evidence, FALSE) = TRUE AND COALESCE(v_include_compare, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence', 'compare_import')
        WHEN COALESCE(v_include_evidence, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'evidence')
        WHEN COALESCE(v_include_compare, FALSE) = TRUE THEN JSONB_BUILD_ARRAY('header', 'editor', 'compare_import')
        ELSE JSONB_BUILD_ARRAY('header', 'editor')
      END
    );
  END IF;


  v_canonical_authorise_row_json := NULL;
  v_canonical_authorise_row_signature := NULL;

  IF v_out IS NOT NULL AND COALESCE(LOWER(NULLIF(BTRIM(COALESCE(v_out->>'ok', '')), '')) IN ('true', '1', 'yes', 'y', 'on'), FALSE) = TRUE THEN
    SELECT canonical_patch.row_json,
           canonical_patch.row_json->>'row_signature'
      INTO v_canonical_authorise_row_json,
           v_canonical_authorise_row_signature
    FROM public.bulk_timesheet_row_patch_v1(
      (
        v_decision_filters
        - 'row_key' - 'rowKey' - 'row_keys' - 'rowKeys'
        - 'timesheet_id' - 'timesheetId' - 'timesheet_ids' - 'timesheetIds'
        - 'current_timesheet_id' - 'currentTimesheetId'
        - 'requested_timesheet_id' - 'requestedTimesheetId'
        - 'expected_timesheet_id' - 'expectedTimesheetId'
        - 'contract_week_id' - 'contractWeekId' - 'contract_week_ids' - 'contractWeekIds'
        - 'week_id' - 'weekId' - 'id' - 'ids'
      )
      || jsonb_strip_nulls(JSONB_BUILD_OBJECT(
           'dataset_mode', 'authorise',
           'projection', 'active_row_header',
           'profile', COALESCE(NULLIF(BTRIM(COALESCE(v_out->>'profile', '')), ''), v_profile, 'status_header'),
           'row_key', NULLIF(BTRIM(COALESCE(v_out->>'row_key', '')), ''),
           'timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'timesheet_id', '')), ''),
           'current_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'current_timesheet_id', '')), ''),
           'requested_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'requested_timesheet_id', '')), ''),
           'expected_timesheet_id', NULLIF(BTRIM(COALESCE(v_out->>'expected_timesheet_id', '')), ''),
           'contract_week_id', NULLIF(BTRIM(COALESCE(v_out->>'contract_week_id', '')), '')
         ))
    ) AS canonical_patch(row_json)
    WHERE NULLIF(BTRIM(COALESCE(canonical_patch.row_json->>'row_signature', '')), '') IS NOT NULL
    ORDER BY
      CASE
        WHEN NULLIF(BTRIM(COALESCE(v_out->>'row_key', '')), '') IS NOT NULL
         AND canonical_patch.row_json->>'row_key' = v_out->>'row_key'
          THEN 0
        ELSE 1
      END,
      canonical_patch.row_json->>'row_key'
    LIMIT 1;

    IF NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_signature, '')), '') IS NOT NULL THEN
      v_canonical_is_archived := UPPER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'tools_stage', ''))) = 'ARCHIVED';
      v_canonical_retained := LOWER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'has_retained_financial_history', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
      v_canonical_can_unprocess := NOT v_canonical_is_archived
        AND NOT v_canonical_retained
        AND LOWER(BTRIM(COALESCE(v_canonical_authorise_row_json->>'can_unprocess', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
      v_canonical_unprocess_visible := NOT v_canonical_is_archived
        AND LOWER(BTRIM(COALESCE(
          v_canonical_authorise_row_json->>'unprocess_action_visible',
          v_canonical_authorise_row_json#>>'{action_flags,unprocess_action_visible}',
          CASE WHEN v_canonical_retained THEN 'true' ELSE v_canonical_authorise_row_json->>'can_unprocess' END,
          'false'
        ))) IN ('true', 't', '1', 'yes', 'y', 'on');
      v_canonical_unprocess_block_reason := CASE
        WHEN v_canonical_is_archived THEN 'TIMESHEET_ARCHIVED'
        WHEN v_canonical_retained THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'
        ELSE NULLIF(BTRIM(COALESCE(
          v_canonical_authorise_row_json->>'unprocess_block_reason',
          v_canonical_authorise_row_json#>>'{action_flags,unprocess_block_reason}',
          ''
        )), '')
      END;
      v_canonical_unprocess_block_message := CASE
        WHEN v_canonical_is_archived THEN 'Archived timesheets must be Unarchived before lifecycle actions.'
        WHEN v_canonical_retained THEN 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.'
        ELSE NULLIF(BTRIM(COALESCE(
          v_canonical_authorise_row_json->>'unprocess_block_message',
          v_canonical_authorise_row_json#>>'{action_flags,unprocess_block_message}',
          ''
        )), '')
      END;

      v_canonical_lifecycle_overlay := jsonb_strip_nulls(
        jsonb_build_object(
          'row_signature', v_canonical_authorise_row_signature,
          'backend_row_signature', COALESCE(NULLIF(BTRIM(v_canonical_authorise_row_json->>'backend_row_signature'), ''), v_canonical_authorise_row_signature),
          'mutation_row_signature', COALESCE(NULLIF(BTRIM(v_canonical_authorise_row_json->>'mutation_row_signature'), ''), v_canonical_authorise_row_signature),
          'summary_stage', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'summary_stage', '')), ''),
          'tools_stage', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'tools_stage', '')), ''),
          'processing_status', NULLIF(BTRIM(COALESCE(v_canonical_authorise_row_json->>'processing_status', '')), ''),
          'has_retained_financial_history', v_canonical_retained,
          'can_unprocess', v_canonical_can_unprocess,
          'unprocess_action_visible', v_canonical_unprocess_visible,
          'unprocess_block_reason', v_canonical_unprocess_block_reason,
          'unprocess_block_message', v_canonical_unprocess_block_message,
          'permission_state_patch_complete', TRUE,
          'priority_badges_patch_complete', TRUE,
          'lifecycle_authority_complete', TRUE,
          'immediate_lifecycle_patch_available', TRUE,
          'refresh_required', FALSE,
          'requires_affected_row_refresh', FALSE
        )
        || jsonb_build_object(
          'is_archived', v_canonical_is_archived,
          'read_only', v_canonical_is_archived,
          'can_archive', FALSE,
          'can_unarchive', v_canonical_is_archived
        )
      );

      v_canonical_action_flags := COALESCE(v_canonical_authorise_row_json->'action_flags', '{}'::jsonb)
        || jsonb_build_object(
          'has_retained_financial_history', v_canonical_retained,
          'can_unprocess', v_canonical_can_unprocess,
          'unprocess_action_visible', v_canonical_unprocess_visible,
          'unprocess_block_reason', v_canonical_unprocess_block_reason,
          'unprocess_block_message', v_canonical_unprocess_block_message,
          'is_archived', v_canonical_is_archived,
          'read_only', v_canonical_is_archived,
          'can_archive', FALSE,
          'can_unarchive', v_canonical_is_archived,
          'permission_state_patch_complete', TRUE,
          'priority_badges_patch_complete', TRUE,
          'lifecycle_authority_complete', TRUE,
          'immediate_lifecycle_patch_available', TRUE,
          'refresh_required', FALSE,
          'requires_affected_row_refresh', FALSE
        );
      v_canonical_row_patch := COALESCE(v_canonical_authorise_row_json->'row_patch', '{}'::jsonb)
        || v_canonical_lifecycle_overlay;

      v_out := v_out || v_canonical_lifecycle_overlay;
      v_out := JSONB_SET(v_out, '{action_flags}', COALESCE(v_out->'action_flags', '{}'::jsonb) || v_canonical_action_flags, TRUE);
      v_out := JSONB_SET(v_out, '{row_patch}', COALESCE(v_out->'row_patch', '{}'::jsonb) || v_canonical_row_patch, TRUE);

      IF JSONB_TYPEOF(v_out->'row') = 'object' THEN
        v_out := JSONB_SET(
          v_out,
          '{row}',
          COALESCE(v_out->'row', '{}'::jsonb)
            || v_canonical_lifecycle_overlay
            || jsonb_build_object(
              'action_flags', COALESCE(v_out#>'{row,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
              'row_patch', COALESCE(v_out#>'{row,row_patch}', '{}'::jsonb) || v_canonical_row_patch
            ),
          TRUE
        );
      END IF;

      IF JSONB_TYPEOF(v_out->'data_row') = 'object' THEN
        v_out := JSONB_SET(
          v_out,
          '{data_row}',
          COALESCE(v_out->'data_row', '{}'::jsonb)
            || v_canonical_lifecycle_overlay
            || jsonb_build_object(
              'action_flags', COALESCE(v_out#>'{data_row,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
              'row_patch', COALESCE(v_out#>'{data_row,row_patch}', '{}'::jsonb) || v_canonical_row_patch
            ),
          TRUE
        );
      END IF;

      IF JSONB_TYPEOF(v_out->'details') = 'object' THEN
        v_out := JSONB_SET(
          v_out,
          '{details}',
          COALESCE(v_out->'details', '{}'::jsonb)
            || v_canonical_lifecycle_overlay
            || jsonb_build_object(
              'action_flags', COALESCE(v_out#>'{details,action_flags}', '{}'::jsonb) || v_canonical_action_flags,
              'row_patch', COALESCE(v_out#>'{details,row_patch}', '{}'::jsonb) || v_canonical_row_patch
            ),
          TRUE
        );
      END IF;
    END IF;
  END IF;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
    'ok', FALSE,
    'context_kind', 'bulk_authorise_row_context',
    'context_profile', v_profile,
    'profile', v_profile,
    'soft_failure', TRUE,
    'context_degraded', TRUE,
    'degraded_reason', 'ROW_NOT_FOUND',
    'header_loaded', FALSE,
    'header_only', FALSE,
    'editor_loaded', FALSE,
    'evidence_loaded', FALSE,
    'compare_loaded', FALSE,
    'full_loaded', FALSE,
    'schedule_pending', TRUE,
    'schedule_authoritative', FALSE,
    'loaded_layers', '[]'::jsonb,
    'error', 'ROW_NOT_FOUND',
    'message', 'No bulk authorise row context was found for the supplied identity',
    'filters', v_filters
  ));
END;
$function$;

CREATE OR REPLACE FUNCTION public.timesheet_qr_send_enqueue_v1(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid DEFAULT NULL,
  p_actor_user_id uuid DEFAULT NULL,
  p_idempotency_key text DEFAULT NULL,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_timesheet_id uuid := NULL;
  v_requested_booking_id text := NULL;
  v_current_timesheet_id uuid := NULL;
  v_current_version integer := NULL;
  v_sheet_scope text := NULL;
  v_submission_mode text := NULL;
  v_contract_id uuid := NULL;
  v_week_ending_date date := NULL;
  v_worked_start_iso timestamp with time zone := NULL;
  v_worked_end_iso timestamp with time zone := NULL;
  v_actual_schedule_json jsonb := NULL;
  v_qr_status text := NULL;
  v_qr_token text := NULL;
  v_effective_qr_token text := NULL;
  v_qr_payload_json jsonb := '{}'::jsonb;
  v_payload_qr_token text := NULL;
  v_qr_generated_at timestamp with time zone := NULL;
  v_qr_scanned_at timestamp with time zone := NULL;
  v_qr_signed_hash text := NULL;
  v_qr_signed_at_utc timestamp with time zone := NULL;
  v_document_revision bigint := NULL;
  v_document_state text := NULL;
  v_updated_at timestamp with time zone := NULL;
  v_tsfin_id uuid := NULL;
  v_tsfin_processing_status text := NULL;
  v_locked_by_invoice_id uuid := NULL;
  v_paid_at_utc timestamp with time zone := NULL;
  v_invoice_breakdown_json jsonb := NULL;
  v_has_segment_invoice_lock boolean := FALSE;
  v_has_hours_for_send boolean := FALSE;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_candidate_email text := NULL;
  v_candidate_name text := NULL;
  v_candidate_opt_in_email boolean := TRUE;
  v_recipient_available boolean := FALSE;
  v_contract_candidate_id uuid := NULL;
  v_contract_client_id uuid := NULL;
  v_idempotency_key text := NULL;
  v_client_idempotency_key text := NULL;
  v_recipient_namespace text := NULL;
  v_mail_held_until_pdf_rendered boolean := TRUE;
  v_mail_hold_reason text := NULL;
  v_mail_scope_json jsonb := '{}'::jsonb;
  v_existing_mail_id uuid := NULL;
  v_existing_mail_status text := NULL;
  v_existing_mail_scope_json jsonb := '{}'::jsonb;
  v_mail_job_id uuid := NULL;
  v_pdf_job_id uuid := NULL;
  v_existing_pdf_job_id uuid := NULL;
  v_job_id uuid := NULL;
  v_send_state text := NULL;
  v_pdf_key text := NULL;
  v_document_idempotency text := NULL;
  v_document_operation_id uuid := NULL;
  v_document_version_id uuid := NULL;
  v_document_version_status text := NULL;
  v_snapshot_json jsonb := '{}'::jsonb;
  v_snapshot_model jsonb := '{}'::jsonb;
  v_snapshot_hash text := NULL;
  v_snapshot_valid boolean := FALSE;
  v_snapshot_error_code text := NULL;
  v_week_period_hash text := NULL;
  v_schedule_hash text := NULL;
  v_reference_signature text := NULL;
  v_additional_units_hash text := NULL;
  v_presentation_settings_hash text := NULL;
  v_qr_payload_hash text := NULL;
  v_complete_printable_content_hash text := NULL;
  v_issue_type text := NULL;
  v_rotate_token boolean := FALSE;
  v_mail_subject text := NULL;
  v_mail_body_text text := NULL;
  v_mail_body_html text := NULL;
  v_mail_reference text := NULL;
  v_mail_scheduled_for_utc timestamp with time zone := NULL;
  v_created_by_user_id uuid := NULL;
  v_post_row jsonb := NULL;
  v_row_key text := NULL;
  v_storage_key text := NULL;
  v_row_signature text := NULL;
  v_candidate_paper_workflow_count integer := 0;
  v_candidate_paper_workflow_id uuid := NULL;
  v_candidate_paper_workflow_generation integer := NULL;
  v_candidate_paper_manifest_sha256 text := NULL;
  v_candidate_paper_binding_json jsonb := '{}'::jsonb;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_ID_REQUIRED',
      'message', 'p_timesheet_id is required.',
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT requested_ts.timesheet_id,
         requested_ts.booking_id
    INTO v_requested_timesheet_id,
         v_requested_booking_id
  FROM public.timesheets AS requested_ts
  WHERE requested_ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_timesheet_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_NOT_FOUND',
      'message', 'Timesheet was not found.',
      'requested_timesheet_id', p_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT current_ts.timesheet_id,
         current_ts.version
    INTO v_current_timesheet_id,
         v_current_version
  FROM public.timesheets AS current_ts
  WHERE current_ts.booking_id = v_requested_booking_id
    AND current_ts.is_current = TRUE
  ORDER BY current_ts.version DESC NULLS LAST,
           current_ts.updated_at DESC NULLS LAST,
           current_ts.created_at DESC NULLS LAST,
           current_ts.timesheet_id DESC
  LIMIT 1;

  IF v_current_timesheet_id IS NULL THEN
    v_current_timesheet_id := v_requested_timesheet_id;
  END IF;

  IF p_expected_timesheet_id IS NOT NULL AND p_expected_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'Timesheet has moved to a newer current row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_requested_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'timesheet_qr_send_enqueue_v1 requires the current timesheet row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', COALESCE(p_expected_timesheet_id, p_timesheet_id),
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT ts_current.sheet_scope::text,
         ts_current.submission_mode::text,
         ts_current.contract_id,
         ts_current.week_ending_date,
         ts_current.worked_start_iso,
         ts_current.worked_end_iso,
         ts_current.actual_schedule_json,
         ts_current.qr_status::text,
         ts_current.qr_token,
         ts_current.qr_payload_json,
         ts_current.qr_generated_at,
         ts_current.qr_scanned_at,
         ts_current.qr_signed_hash,
         ts_current.qr_signed_at_utc,
         ts_current.document_revision,
         ts_current.document_state::text,
         ts_current.version,
         ts_current.updated_at
    INTO v_sheet_scope,
         v_submission_mode,
         v_contract_id,
         v_week_ending_date,
         v_worked_start_iso,
         v_worked_end_iso,
         v_actual_schedule_json,
         v_qr_status,
         v_qr_token,
         v_qr_payload_json,
         v_qr_generated_at,
         v_qr_scanned_at,
         v_qr_signed_hash,
         v_qr_signed_at_utc,
         v_document_revision,
         v_document_state,
         v_current_version,
         v_updated_at
  FROM public.timesheets AS ts_current
  WHERE ts_current.timesheet_id = v_current_timesheet_id
    AND ts_current.is_current = TRUE
  LIMIT 1
  FOR UPDATE;

  IF v_sheet_scope IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CURRENT_TIMESHEET_NOT_FOUND',
      'message', 'Current timesheet was not found.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) NOT IN ('DAILY', 'WEEKLY') THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'UNSUPPORTED_SHEET_SCOPE',
      'message', 'QR send is only supported for DAILY or WEEKLY timesheets.',
      'current_timesheet_id', v_current_timesheet_id,
      'sheet_scope', v_sheet_scope,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_submission_mode, '')) <> 'MANUAL' THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NOT_MANUAL_QR_ROUTE',
      'message', 'QR send requires a MANUAL submission-mode timesheet with QR enabled.',
      'current_timesheet_id', v_current_timesheet_id,
      'submission_mode', v_submission_mode,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_qr_status, '')) <> 'PENDING' THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'QR_NOT_PENDING',
      'message', 'QR send requires qr_status=PENDING.',
      'current_timesheet_id', v_current_timesheet_id,
      'qr_status', v_qr_status,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_qr_scanned_at IS NOT NULL OR NULLIF(BTRIM(COALESCE(v_qr_signed_hash, '')), '') IS NOT NULL OR v_qr_signed_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'QR_ALREADY_SIGNED',
      'message', 'Cannot queue QR send: timesheet already has signed QR markers.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  WITH locked_candidate_paper AS MATERIALIZED (
    SELECT workflow.id,
           workflow.generation,
           workflow.paper_return_manifest_sha256
    FROM public.candidate_submission_workflows AS workflow
    WHERE workflow.route = 'PAPER'
      AND workflow.state = 'AWAITING_PAPER_RETURN'
      AND (
        workflow.target_timesheet_id = v_current_timesheet_id
        OR workflow.anchor_timesheet_id = v_current_timesheet_id
      )
    ORDER BY workflow.id
    FOR UPDATE
  )
  SELECT count(*)::integer,
         (array_agg(locked_candidate_paper.id ORDER BY locked_candidate_paper.id))[1],
         (array_agg(locked_candidate_paper.generation ORDER BY locked_candidate_paper.id))[1],
         (array_agg(encode(locked_candidate_paper.paper_return_manifest_sha256, 'hex')
                    ORDER BY locked_candidate_paper.id))[1]
    INTO v_candidate_paper_workflow_count,
         v_candidate_paper_workflow_id,
         v_candidate_paper_workflow_generation,
         v_candidate_paper_manifest_sha256
  FROM locked_candidate_paper;

  IF v_candidate_paper_workflow_count > 1 THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CANDIDATE_PAPER_WORKFLOW_CONFLICT',
      'message', 'More than one active Candidate PAPER workflow targets this timesheet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_candidate_paper_workflow_count = 1 THEN
    IF v_candidate_paper_workflow_generation IS NULL
       OR v_candidate_paper_workflow_generation < 1
       OR COALESCE(v_candidate_paper_manifest_sha256, '') !~ '^[0-9a-f]{64}$' THEN
      RETURN jsonb_build_object(
        'ok', FALSE,
        'queued', FALSE,
        'operation', 'timesheet_qr_send_enqueue',
        'error_code', 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE',
        'message', 'The Candidate PAPER workflow does not have a valid frozen return manifest.',
        'current_timesheet_id', v_current_timesheet_id,
        'recipient_available', FALSE,
        'send_state', 'REJECTED'
      );
    END IF;
    v_candidate_paper_binding_json := jsonb_build_object(
      'candidate_workflow_id', v_candidate_paper_workflow_id,
      'candidate_workflow_generation', v_candidate_paper_workflow_generation,
      'paper_return_manifest_sha256', v_candidate_paper_manifest_sha256,
      'candidate_paper_pack_ready', FALSE
    );
  END IF;

  SELECT tf_current.id,
         tf_current.processing_status::text,
         tf_current.locked_by_invoice_id,
         tf_current.paid_at_utc,
         tf_current.invoice_breakdown_json,
         tf_current.candidate_id,
         tf_current.client_id
    INTO v_tsfin_id,
         v_tsfin_processing_status,
         v_locked_by_invoice_id,
         v_paid_at_utc,
         v_invoice_breakdown_json,
         v_candidate_id,
         v_client_id
  FROM public.timesheets_financials AS tf_current
  WHERE tf_current.timesheet_id = v_current_timesheet_id
    AND tf_current.is_current = TRUE
  ORDER BY tf_current.computed_at_utc DESC NULLS LAST,
           tf_current.created_at DESC NULLS LAST,
           tf_current.updated_at DESC NULLS LAST,
           tf_current.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_tsfin_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NO_TSFIN',
      'message', 'No current financial snapshot exists for this timesheet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_locked_by_invoice_id IS NOT NULL OR v_paid_at_utc IS NOT NULL OR v_has_segment_invoice_lock = TRUE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_LOCKED_OR_PAID',
      'message', 'Cannot queue QR send: timesheet is locked, invoice-locked, or paid.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(COALESCE(v_actual_schedule_json, '[]'::jsonb)) = 'array' THEN COALESCE(v_actual_schedule_json, '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS schedule_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(schedule_segment.segment_json->>'start', schedule_segment.segment_json->>'start_utc', '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(schedule_segment.segment_json->>'end', schedule_segment.segment_json->>'end_utc', '')), '') IS NOT NULL
    ) OR EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
          WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
           AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
    ) INTO v_has_hours_for_send;
  ELSE
    v_has_hours_for_send := v_worked_start_iso IS NOT NULL AND v_worked_end_iso IS NOT NULL;
  END IF;

  IF COALESCE(v_has_hours_for_send, FALSE) = FALSE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NO_HOURS_RECORDED',
      'message', 'Cannot queue QR send: no hours are recorded yet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_contract_id IS NOT NULL THEN
    SELECT contract_row.candidate_id,
           contract_row.client_id
      INTO v_contract_candidate_id,
           v_contract_client_id
    FROM public.contracts AS contract_row
    WHERE contract_row.id = v_contract_id
    LIMIT 1;
  END IF;

  v_candidate_id := COALESCE(v_contract_candidate_id, v_candidate_id);
  v_client_id := COALESCE(v_contract_client_id, v_client_id);

  IF v_candidate_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CANDIDATE_NOT_FOUND',
      'message', 'Cannot queue QR send: candidate could not be resolved.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT NULLIF(BTRIM(candidate_row.email), ''),
         COALESCE(NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(candidate_row.first_name), ''), NULLIF(BTRIM(candidate_row.last_name), ''))), '')),
         COALESCE(candidate_row.opt_in_email, TRUE)
    INTO v_candidate_email,
         v_candidate_name,
         v_candidate_opt_in_email
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = v_candidate_id
  LIMIT 1;

  v_recipient_available := NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NOT NULL AND COALESCE(v_candidate_opt_in_email, TRUE) = TRUE;

  IF v_recipient_available = FALSE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', CASE WHEN NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NULL THEN 'CANDIDATE_EMAIL_MISSING' ELSE 'CANDIDATE_EMAIL_OPTED_OUT' END,
      'message', CASE WHEN NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NULL THEN 'Cannot queue QR send: candidate email is missing.' ELSE 'Cannot queue QR send: candidate has opted out of email.' END,
      'current_timesheet_id', v_current_timesheet_id,
      'candidate_id', v_candidate_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  v_payload_qr_token := CASE
    WHEN jsonb_typeof(v_qr_payload_json) = 'object' THEN NULLIF(BTRIM(COALESCE(v_qr_payload_json->>'tok', '')), '')
    ELSE NULL
  END;
  v_rotate_token :=
    NULLIF(BTRIM(COALESCE(v_qr_token, '')), '') IS NULL
    OR UPPER(COALESCE(v_document_state, 'STALE')) IN ('STALE', 'FAILED');
  v_effective_qr_token := CASE
    WHEN v_rotate_token THEN gen_random_uuid()::text
    ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_qr_token, '')), ''),
      v_payload_qr_token, gen_random_uuid()::text)
  END;
  v_qr_payload_json := jsonb_build_object('v', 1, 'tok', v_effective_qr_token);
  v_issue_type := CASE
    WHEN NULLIF(BTRIM(COALESCE(v_qr_token, '')), '') IS NULL THEN 'NEW_ISSUE'
    WHEN v_rotate_token THEN 'REISSUED_CHANGED'
    ELSE 'RESENT_UNCHANGED'
  END;

  UPDATE public.timesheets AS ts_qr_update
     SET qr_token = v_effective_qr_token,
         qr_payload_json = v_qr_payload_json,
         qr_generated_at = COALESCE(ts_qr_update.qr_generated_at, v_now),
         qr_r2_key = NULL,
         qr_scanned_at = NULL,
         qr_scan_info_json = NULL,
         updated_at = v_now
   WHERE ts_qr_update.timesheet_id = v_current_timesheet_id
     AND ts_qr_update.is_current = TRUE
  RETURNING ts_qr_update.qr_generated_at,
            ts_qr_update.updated_at,
            ts_qr_update.document_revision
       INTO v_qr_generated_at,
            v_updated_at,
            v_document_revision;

  SELECT t.document_revision
    INTO v_document_revision
  FROM public.timesheets t
  WHERE t.timesheet_id=v_current_timesheet_id AND t.is_current
  LIMIT 1;

  IF UPPER(COALESCE(v_tsfin_processing_status, '')) <> 'AWAITING_MANUAL_SIGNATURE' THEN
    UPDATE public.timesheets_financials AS tf_update
       SET processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum,
           updated_at = v_now
     WHERE tf_update.id = v_tsfin_id
       AND tf_update.is_current = TRUE;
  END IF;

  SELECT tms_user_check.id
    INTO v_created_by_user_id
  FROM public.tms_users AS tms_user_check
  WHERE tms_user_check.id = p_actor_user_id
  LIMIT 1;

  v_client_idempotency_key := NULLIF(
    REGEXP_REPLACE(
      LOWER(BTRIM(COALESCE(p_idempotency_key, ''))),
      '[^a-z0-9._:-]+',
      '-',
      'g'
    ),
    ''
  );

  IF v_client_idempotency_key IS NULL THEN
    v_client_idempotency_key := 'auto:' || md5(CONCAT_WS('|',
      v_current_timesheet_id::text,
      COALESCE(v_current_version::text, ''),
      LOWER(COALESCE(v_candidate_email, ''))
    ));
  END IF;

  v_recipient_namespace := md5(LOWER(COALESCE(v_candidate_email, '')));

  v_idempotency_key := 'timesheet_qr_send:'
    || v_current_timesheet_id::text
    || ':v' || COALESCE(v_current_version::text, '0')
    || ':recipient:' || v_recipient_namespace
    || ':key:' || md5(v_client_idempotency_key);

  PERFORM pg_advisory_xact_lock(hashtext('timesheet_qr_send:' || v_current_timesheet_id::text));
  PERFORM pg_advisory_xact_lock(hashtext(v_idempotency_key));

  v_pdf_key := 'docs-pdf/timesheets/ts_' || v_current_timesheet_id::text || '.pdf';
  v_mail_reference := v_idempotency_key;
  v_mail_scheduled_for_utc := TIMESTAMPTZ '9999-12-31 00:00:00+00';
  v_mail_subject := CASE
    WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Weekly QR timesheet – week ending ' || COALESCE(v_week_ending_date::text, '(unknown)')
    ELSE 'Daily QR timesheet – ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)')
  END;
  v_mail_body_text := CONCAT_WS(E'\n',
    'Please print the attached timesheet, ask the ward manager to sign it,',
    'and then upload the signed copy via the app.',
    CASE
      WHEN private._candidate_feature_enabled_current_v1('candidate_paper_qr')
       AND COALESCE(v_current_version,1)>1
      THEN 'Please remember to sign the replacement timesheet before returning it.'
      ELSE NULL
    END,
    '',
    CASE WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Week ending: ' || COALESCE(v_week_ending_date::text, '(unknown)') ELSE 'Date: ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)') END,
    'Timesheet ID: ' || v_current_timesheet_id::text
  );
  v_mail_body_html := '<p>Please print the attached timesheet, ask the ward manager to sign it, and then upload the signed copy via the app.<br/>'
    || CASE
      WHEN private._candidate_feature_enabled_current_v1('candidate_paper_qr')
       AND COALESCE(v_current_version,1)>1
      THEN 'Please remember to sign the replacement timesheet before returning it.<br/>'
      ELSE ''
    END
    || '<br/>'
    || CASE WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Week ending: ' || COALESCE(v_week_ending_date::text, '(unknown)') ELSE 'Date: ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)') END
    || '<br/>Timesheet ID: ' || v_current_timesheet_id::text || '</p>';

  SELECT p.presentation_model,p.snapshot_json,p.snapshot_hash,p.valid,p.error_code
    INTO v_snapshot_model,v_snapshot_json,v_snapshot_hash,
      v_snapshot_valid,v_snapshot_error_code
  FROM private._invoice_presentation_snapshot_batch(
    jsonb_build_array(jsonb_build_object(
      'request_key','timesheet-qr:'||v_current_timesheet_id::text,
      'entity_type','TIMESHEET',
      'entity_id',v_current_timesheet_id,
      'purpose','TIMESHEET',
      'template_version','timesheet-professional-v2')),
    v_now
  ) p
  LIMIT 1;

  IF NOT COALESCE(v_snapshot_valid,FALSE)
     OR v_snapshot_model->>'schema_version'<>'TIMESHEET_RENDER_MODEL_V2'
     OR v_snapshot_model->>'form_variant'<>'QR_UNSIGNED' THEN
    RETURN jsonb_build_object(
      'ok',FALSE,'queued',FALSE,
      'operation','timesheet_qr_send_enqueue',
      'error_code',coalesce(v_snapshot_error_code,
        'TIMESHEET_PRESENTATION_SNAPSHOT_INVALID'),
      'message','The official QR timesheet could not be frozen safely.',
      'current_timesheet_id',v_current_timesheet_id,
      'recipient_available',TRUE,'send_state','REJECTED');
  END IF;

  v_week_period_hash := coalesce(
    nullif(v_snapshot_model->>'week_period_hash',''),
    encode(digest(coalesce(
      v_snapshot_model->'week_period','{}'::jsonb)::text,'sha256'),'hex'));
  v_schedule_hash := encode(digest(
    coalesce(v_snapshot_model#>'{week_period,days}','[]'::jsonb)::text,
    'sha256'),'hex');
  v_reference_signature := coalesce(
    nullif(v_snapshot_model->>'reference_signature',''),
    encode(digest(coalesce(v_snapshot_model#>'{week_period,days}',
      '[]'::jsonb)::text,'sha256'),'hex'));
  v_additional_units_hash := coalesce(
    nullif(v_snapshot_model->>'additional_units_hash',''),
    encode(digest(coalesce(v_snapshot_model#>'{additional_units_section,rows}',
      '[]'::jsonb)::text,'sha256'),'hex'));
  v_presentation_settings_hash := coalesce(
    nullif(v_snapshot_model->>'presentation_settings_hash',''),
    encode(digest(jsonb_build_object(
      'branding',v_snapshot_model->'branding',
      'wording',v_snapshot_model->'wording')::text,'sha256'),'hex'));
  v_qr_payload_hash := encode(digest(v_qr_payload_json::text,'sha256'),'hex');
  v_complete_printable_content_hash := encode(digest(
    concat_ws('|',v_snapshot_hash,v_week_period_hash,v_schedule_hash,
      v_reference_signature,v_additional_units_hash,
      v_presentation_settings_hash,v_qr_payload_hash,
      'timesheet-professional-v2'),'sha256'),'hex');
  v_document_idempotency := encode(digest(concat_ws('|',
    'BUILD_DOCUMENT','TIMESHEET',v_current_timesheet_id::text,'TIMESHEET',
    v_document_revision::text,'timesheet-professional-v2',
    v_qr_payload_hash,v_complete_printable_content_hash),'sha256'),'hex');

  SELECT v.id,v.operation_id,v.status::text,v.r2_key
    INTO v_document_version_id,v_document_operation_id,
      v_document_version_status,v_pdf_key
  FROM public.invoice_document_versions v
  WHERE v.entity_type='TIMESHEET' AND v.entity_id=v_current_timesheet_id
    AND v.purpose='TIMESHEET'
    AND v.source_revision=v_document_revision::text
    AND v.template_version='timesheet-professional-v2'
    AND v.status IN(
      'PLANNING','WAITING_FOR_INPUTS','RENDERING',
      'ASSEMBLING','VERIFYING','READY')
  ORDER BY (v.status='READY') DESC,v.created_at_utc DESC,v.id DESC
  LIMIT 1;

  IF v_document_version_id IS NULL THEN
    INSERT INTO public.invoice_operations(
      operation_type,entity_type,entity_id,actor_user_id,idempotency_key,
      status,phase,priority,source_revision,template_version,input_json,
      config_json,progress_json,total_units,chunk_count,control_version,
      change_seq,created_at_utc,updated_at_utc
    ) VALUES(
      'BUILD_DOCUMENT','TIMESHEET',v_current_timesheet_id,p_actor_user_id,
      v_document_idempotency,'QUEUED','BUILD_MANIFEST',550,
      v_document_revision::text,'timesheet-professional-v2',
      jsonb_build_object(
        'entity_type','TIMESHEET','entity_id',v_current_timesheet_id,
        'purpose','TIMESHEET','source_revision',v_document_revision,
        'template_version','timesheet-professional-v2',
        'qr_payload_hash',v_qr_payload_hash,
        'printable_content_hash',v_complete_printable_content_hash),
      jsonb_build_object('processor_policy',private._invoice_processor_limits()),
      jsonb_build_object('status_message','Official QR timesheet queued'),
      1,1,1,nextval('public.invoice_operation_change_seq'),v_now,v_now
    )
    ON CONFLICT DO NOTHING
    RETURNING id INTO v_document_operation_id;

    IF v_document_operation_id IS NULL THEN
      SELECT o.id INTO v_document_operation_id
      FROM public.invoice_operations o
      WHERE o.idempotency_key=v_document_idempotency
        AND o.status IN('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      ORDER BY o.created_at_utc DESC,o.id DESC
      LIMIT 1;
    END IF;

    INSERT INTO public.invoice_document_versions(
      entity_type,entity_id,purpose,operation_id,source_revision,
      template_version,status,snapshot_json,snapshot_hash,
      manifest_json,manifest_hash,created_at_utc
    ) VALUES(
      'TIMESHEET',v_current_timesheet_id,'TIMESHEET',
      v_document_operation_id,v_document_revision::text,
      'timesheet-professional-v2','PLANNING',
      v_snapshot_json,v_snapshot_hash,'[]'::jsonb,
      encode(digest('[]','sha256'),'hex'),v_now
    )
    ON CONFLICT(entity_type,entity_id,purpose,source_revision,template_version)
      WHERE purpose IN('DRAFT_PREVIEW','TIMESHEET')
        AND status IN(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING','READY')
    DO NOTHING
    RETURNING id,status::text,r2_key
      INTO v_document_version_id,v_document_version_status,v_pdf_key;

    IF v_document_version_id IS NULL THEN
      SELECT v.id,v.operation_id,v.status::text,v.r2_key
        INTO v_document_version_id,v_document_operation_id,
          v_document_version_status,v_pdf_key
      FROM public.invoice_document_versions v
      WHERE v.entity_type='TIMESHEET' AND v.entity_id=v_current_timesheet_id
        AND v.purpose='TIMESHEET'
        AND v.source_revision=v_document_revision::text
        AND v.template_version='timesheet-professional-v2'
        AND v.status IN(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING','READY')
      ORDER BY (v.status='READY') DESC,v.created_at_utc DESC,v.id DESC
      LIMIT 1;
    END IF;

    INSERT INTO public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,
      operation_control_version,created_at_utc,updated_at_utc
    )
    SELECT v_document_operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',
        v_document_version_id::text,v_document_revision::text,
        'timesheet-professional-v2','1'),'sha256'),'hex'),
      0,'TIMESHEET',v_current_timesheet_id,v_document_version_id,
      'QUEUED',550,v_now,
      jsonb_build_object(
        'purpose','TIMESHEET','source_revision',v_document_revision,
        'template_version','timesheet-professional-v2',
        'qr_payload_hash',v_qr_payload_hash,
        'printable_content_hash',v_complete_printable_content_hash),
      o.control_version,v_now,v_now
    FROM public.invoice_operations o
    WHERE o.id=v_document_operation_id
    ON CONFLICT(operation_id,chunk_type,level_no,sequence_no,work_key)
      DO NOTHING;
  END IF;

  UPDATE public.timesheets t
  SET current_document_version_id=v_document_version_id,
      active_document_operation_id=case
        when v_document_version_status='READY' then null
        else v_document_operation_id end,
      document_state=case
        when v_document_version_status='READY' then 'READY'
        else 'QUEUED' end,
      last_document_error_json=null,updated_at=v_now
  WHERE t.timesheet_id=v_current_timesheet_id AND t.is_current
    AND t.document_revision=v_document_revision;

  v_pdf_job_id := v_document_operation_id;
  v_mail_held_until_pdf_rendered :=
    v_candidate_paper_workflow_count = 1
    OR COALESCE(v_document_version_status,'')<>'READY';
  v_mail_hold_reason := CASE
    WHEN v_candidate_paper_workflow_count = 1 THEN 'CANDIDATE_PAPER_PACK_PENDING'
    WHEN v_mail_held_until_pdf_rendered THEN 'PDF_RENDER_PENDING'
    ELSE NULL
  END;
  v_mail_scheduled_for_utc := CASE
    WHEN v_mail_held_until_pdf_rendered THEN
      TIMESTAMPTZ '9999-12-31 00:00:00+00'
    ELSE v_now
  END;

  v_mail_scope_json := jsonb_build_object(
    'job_kind', 'TIMESHEET_QR_SEND',
    'document_operation_id',v_document_operation_id,
    'document_version_id',v_document_version_id,
    'document_revision',v_document_revision,
    'template_version','timesheet-professional-v2',
    'idempotency_key', v_idempotency_key,
    'client_idempotency_key', v_client_idempotency_key,
    'requires_pdf_render', TRUE,
    'release_mail_after_pdf_render', TRUE,
    'mail_delayed_for_pdf_render',v_mail_held_until_pdf_rendered,
    'mail_held_until_pdf_rendered',v_mail_held_until_pdf_rendered,
    'mail_hold_reason',v_mail_hold_reason,
    'pdf_storage_key', v_pdf_key,
    'current_timesheet_id', v_current_timesheet_id::text,
    'current_version', v_current_version,
    'qr_token_hash',encode(digest(v_effective_qr_token,'sha256'),'hex'),
    'qr_payload_hash',v_qr_payload_hash,
    'week_period_hash',v_week_period_hash,
    'schedule_hash',v_schedule_hash,
    'reference_signature',v_reference_signature,
    'additional_units_hash',v_additional_units_hash,
    'presentation_settings_hash',v_presentation_settings_hash,
    'printable_content_hash',v_complete_printable_content_hash,
    'recipient_email', v_candidate_email
  ) || v_candidate_paper_binding_json;

  SELECT mail_existing.id,
         mail_existing.status::text,
         COALESCE(mail_existing.payment_scope_json, '{}'::jsonb)
    INTO v_existing_mail_id,
         v_existing_mail_status,
         v_existing_mail_scope_json
  FROM public.mail_outbox AS mail_existing
  WHERE mail_existing.type = 'TIMESHEET_QR'
    AND mail_existing.reference = v_mail_reference
    AND mail_existing.context_kind = 'timesheets'
    AND mail_existing.context_id = v_current_timesheet_id
    AND mail_existing."to" = v_candidate_email
  ORDER BY mail_existing.created_at_utc DESC,
           mail_existing.id DESC
  LIMIT 1;

  IF v_existing_mail_id IS NOT NULL THEN
    v_mail_job_id := v_existing_mail_id;

    IF v_candidate_paper_workflow_count = 1
       AND NULLIF(BTRIM(COALESCE(v_existing_mail_scope_json->>'candidate_workflow_id', '')), '') IS NOT NULL
       AND (
         v_existing_mail_scope_json->>'candidate_workflow_id' IS DISTINCT FROM v_candidate_paper_workflow_id::text
         OR COALESCE(v_existing_mail_scope_json->>'candidate_workflow_generation', '') IS DISTINCT FROM v_candidate_paper_workflow_generation::text
         OR LOWER(COALESCE(v_existing_mail_scope_json->>'paper_return_manifest_sha256', ''))
              IS DISTINCT FROM v_candidate_paper_manifest_sha256
       ) THEN
      RETURN jsonb_build_object(
        'ok', FALSE,
        'queued', FALSE,
        'operation', 'timesheet_qr_send_enqueue',
        'error_code', 'CANDIDATE_PAPER_OUTBOX_IDENTITY_CONFLICT',
        'message', 'The existing QR email is bound to a different Candidate PAPER workflow.',
        'current_timesheet_id', v_current_timesheet_id,
        'mail_outbox_id', v_existing_mail_id,
        'recipient_available', FALSE,
        'send_state', 'REJECTED'
      );
    ELSIF UPPER(COALESCE(v_existing_mail_status, '')) = 'SENT' THEN
      v_send_state := 'ALREADY_SENT';
    ELSIF v_candidate_paper_workflow_count = 1
          AND UPPER(COALESCE(v_existing_mail_status, '')) = 'FAILED' THEN
      v_send_state := 'CANDIDATE_PAPER_MAIL_FAILED';
    ELSIF NULLIF(BTRIM(COALESCE(v_existing_mail_scope_json->>'candidate_workflow_id', '')), '') IS NOT NULL
          AND (
            LOWER(COALESCE(v_existing_mail_scope_json->>'candidate_paper_pack_ready', 'false'))
              IN ('true','t','1','yes')
            OR v_candidate_paper_workflow_count = 0
          ) THEN
      -- A Candidate complete pack (or a historical Candidate binding) must never
      -- be replaced by the ordinary one-page QR attachment on enqueue replay.
      v_send_state := 'CANDIDATE_PAPER_OUTBOX_PRESERVED';
      v_mail_held_until_pdf_rendered := LOWER(COALESCE(
        v_existing_mail_scope_json->>'mail_held_until_pdf_rendered', 'false'))
        IN ('true','t','1','yes');
      v_mail_hold_reason := NULLIF(BTRIM(COALESCE(
        v_existing_mail_scope_json->>'mail_hold_reason', '')), '');
    ELSE
      UPDATE public.mail_outbox AS mail_update
         SET status = 'QUEUED'::public.mail_status_enum,
             subject = v_mail_subject,
             body_html = v_mail_body_html,
             body_text = v_mail_body_text,
             attachments = CASE
               WHEN v_mail_held_until_pdf_rendered THEN '[]'::jsonb
               ELSE jsonb_build_array(jsonb_build_object(
                 'r2_key',v_pdf_key,
                 'filename','Timesheet_'||COALESCE(
                   v_week_ending_date::text,v_current_timesheet_id::text)||'.pdf'))
             END,
             last_error = NULL,
             failed_at = NULL,
             scheduled_for_utc = v_mail_scheduled_for_utc,
             next_attempt_at_utc = v_mail_scheduled_for_utc,
             provider_status = NULL,
             provider_message_id = NULL,
             attempt_lease_token = NULL,
             attempt_leased_at_utc = NULL,
             attempt_lease_expires_at_utc = NULL,
             payment_scope_json = v_mail_scope_json
       WHERE mail_update.id = v_existing_mail_id;

      v_send_state := CASE
        WHEN v_candidate_paper_workflow_count = 1 THEN 'CANDIDATE_PAPER_PACK_MAIL_HELD'
        WHEN NOT v_mail_held_until_pdf_rendered THEN 'DOCUMENT_READY_MAIL_QUEUED'
        WHEN UPPER(COALESCE(v_existing_mail_status,''))='FAILED'
          THEN 'DOCUMENT_REQUEUED_MAIL_HELD'
        ELSE 'ALREADY_QUEUED' END;
    END IF;
  ELSE
    INSERT INTO public.mail_outbox AS mail_insert (
      type,
      "to",
      cc,
      bcc,
      reply_to,
      importance,
      email_type,
      subject,
      body_html,
      body_text,
      attachments,
      status,
      last_error,
      created_at_utc,
      sent_at,
      created_by,
      reference,
      recipient_kind,
      recipient_id,
      context_kind,
      context_id,
      mailshot_run_id,
      document_template_id,
      provider_status,
      delivered_at,
      read_at,
      scheduled_for_utc,
      next_attempt_at_utc,
      payment_scope_json
    ) VALUES (
      'TIMESHEET_QR',
      v_candidate_email,
      NULL,
      NULL,
      NULL,
      'Normal',
      'html',
      v_mail_subject,
      v_mail_body_html,
      v_mail_body_text,
      CASE
        WHEN v_mail_held_until_pdf_rendered THEN '[]'::jsonb
        ELSE jsonb_build_array(jsonb_build_object(
          'r2_key',v_pdf_key,
          'filename','Timesheet_'||COALESCE(
            v_week_ending_date::text,v_current_timesheet_id::text)||'.pdf'))
      END,
      'QUEUED'::public.mail_status_enum,
      NULL,
      v_now,
      NULL,
      v_created_by_user_id,
      v_mail_reference,
      'candidate',
      v_candidate_id,
      'timesheets',
      v_current_timesheet_id,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      v_mail_scheduled_for_utc,
      v_mail_scheduled_for_utc,
      v_mail_scope_json
    )
    RETURNING id INTO v_mail_job_id;

    v_send_state := CASE
      WHEN v_candidate_paper_workflow_count = 1 THEN 'CANDIDATE_PAPER_PACK_MAIL_HELD'
      WHEN v_mail_held_until_pdf_rendered THEN 'DOCUMENT_QUEUED_MAIL_HELD'
      ELSE 'DOCUMENT_READY_MAIL_QUEUED' END;
  END IF;

  v_job_id := v_mail_job_id;

  SELECT decision_result.row_json
    INTO v_post_row
  FROM public.bulk_timesheet_row_decision_v1(jsonb_build_object(
    'dataset_mode', 'process',
    'timesheet_id', v_current_timesheet_id::text
  )) AS decision_result(row_json)
  LIMIT 1;

  v_row_key := NULLIF(BTRIM(COALESCE(v_post_row->>'row_key', '')), '');
  v_storage_key := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_storage_key', '')), '');
  v_row_signature := NULLIF(BTRIM(COALESCE(v_post_row->>'row_signature', '')), '');

  INSERT INTO public.audit_events AS audit_insert (
    ts_utc,
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  ) VALUES (
    v_now,
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'TIMESHEET_QR_SEND_QUEUED',
    NULL,
    jsonb_build_object(
      'timesheet_id', v_current_timesheet_id,
      'job_id', v_job_id,
      'mail_outbox_id', v_mail_job_id,
      'pdf_job_id', v_pdf_job_id,
      'idempotency_key', v_idempotency_key,
      'send_state', v_send_state,
      'recipient_email', v_candidate_email,
      'scheduled_for_utc', v_mail_scheduled_for_utc,
      'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
      'mail_delayed_for_pdf_render',v_mail_held_until_pdf_rendered,
      'mail_hold_reason',v_mail_hold_reason,
      'pdf_storage_key', v_pdf_key,
      'document_operation_id',v_document_operation_id,
      'document_version_id',v_document_version_id,
      'issue_type',v_issue_type,
      'client_idempotency_key', v_client_idempotency_key,
      'recipient_namespace', v_recipient_namespace
    ),
    'Bulk QR send enqueue'
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'queued', TRUE,
    'operation', 'timesheet_qr_send_enqueue',
    'job_id', v_job_id,
    'mail_outbox_id', v_mail_job_id,
    'pdf_job_id', v_pdf_job_id,
    'idempotency_key', v_idempotency_key,
    'current_timesheet_id', v_current_timesheet_id,
    'timesheet_id', v_current_timesheet_id,
    'expected_timesheet_id', v_current_timesheet_id,
    'current_version', v_current_version,
    'recipient_available', TRUE,
    'recipient_email', v_candidate_email,
    'recipient_name', v_candidate_name,
    'send_state', v_send_state,
    'scheduled_for_utc', v_mail_scheduled_for_utc,
    'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
    'mail_delayed_for_pdf_render',v_mail_held_until_pdf_rendered,
    'mail_hold_reason',v_mail_hold_reason,
    'candidate_paper_pack_ready',CASE
      WHEN NULLIF(BTRIM(COALESCE(v_existing_mail_scope_json->>'candidate_workflow_id', '')), '') IS NOT NULL
        THEN LOWER(COALESCE(v_existing_mail_scope_json->>'candidate_paper_pack_ready', 'false'))
          IN ('true','t','1','yes')
      WHEN v_candidate_paper_workflow_count = 1 THEN FALSE
      ELSE NULL
    END,
    'pdf_storage_key', v_pdf_key,
    'document_operation_id',v_document_operation_id,
    'document_version_id',v_document_version_id,
    'document_revision',v_document_revision,
    'template_version','timesheet-professional-v2',
    'issue_type',v_issue_type,
    'client_idempotency_key', v_client_idempotency_key,
    'recipient_namespace', v_recipient_namespace,
    'row_patch', COALESCE(v_post_row->'row_patch', jsonb_build_object()),
    'data_row', COALESCE(v_post_row, jsonb_build_object()),
    'row', COALESCE(v_post_row, jsonb_build_object()),
    'cache_invalidation_hints', jsonb_build_object(
      'row_keys', jsonb_build_array(COALESCE(v_row_key, 'timesheet:' || v_current_timesheet_id::text)),
      'timesheet_ids', jsonb_build_array(v_current_timesheet_id),
      'storage_keys', jsonb_build_array(v_storage_key, v_pdf_key),
      'datasets', jsonb_build_array('bulk_process', 'bulk_authorise'),
      'row_signature', v_row_signature,
      'invalidate_context', TRUE,
      'invalidate_preview', TRUE
    ),
    'cache_invalidation', jsonb_build_object(
      'rows', jsonb_build_array(jsonb_build_object(
        'row_key', COALESCE(v_row_key, 'timesheet:' || v_current_timesheet_id::text),
        'timesheet_id', v_current_timesheet_id,
        'new_row_signature', v_row_signature
      )),
      'artifacts', jsonb_build_array(jsonb_build_object(
        'timesheet_id', v_current_timesheet_id,
        'storage_key', COALESCE(v_storage_key, v_pdf_key),
        'pdf_storage_key', v_pdf_key,
        'changed', TRUE
      )),
      'datasets', jsonb_build_array('bulk_process', 'bulk_authorise')
    )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.timesheet_lifecycle_guard_signature_v1(p_timesheet_id uuid DEFAULT NULL::uuid, p_contract_week_id uuid DEFAULT NULL::uuid, p_include_payload boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_week public.contract_weeks%ROWTYPE;
  v_tsfin public.timesheets_financials%ROWTYPE;

  v_resolved_booking_id text := NULL;
  v_signature_payload jsonb := '{}'::jsonb;
  v_component_hashes jsonb := '{}'::jsonb;
  v_component_values jsonb := '{}'::jsonb;
  v_diagnostic_payload jsonb := '{}'::jsonb;
  v_candidate_component jsonb := '{}'::jsonb;
  v_candidate_component_enabled boolean := private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities');
  v_temp_log_enabled boolean := false;
  v_signature text := NULL;
BEGIN
  IF p_timesheet_id IS NULL AND p_contract_week_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'INVALID_PAYLOAD',
      'message', 'timesheet_id or contract_week_id is required',
      'signature', NULL,
      'backend_row_signature', NULL,
      'row_signature', NULL
    );
  END IF;

  IF p_contract_week_id IS NOT NULL THEN
    SELECT cw.*
      INTO v_week
    FROM public.contract_weeks AS cw
    WHERE cw.id = p_contract_week_id
    LIMIT 1;
  END IF;

  IF p_timesheet_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_requested_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = p_timesheet_id
    LIMIT 1;

    IF v_requested_ts.timesheet_id IS NOT NULL THEN
      v_resolved_booking_id := v_requested_ts.booking_id;
    END IF;
  END IF;

  IF v_resolved_booking_id IS NULL AND v_week.timesheet_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_requested_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = v_week.timesheet_id
    LIMIT 1;

    IF v_requested_ts.timesheet_id IS NOT NULL THEN
      v_resolved_booking_id := v_requested_ts.booking_id;
    END IF;
  END IF;

  IF v_resolved_booking_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_resolved_booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1;
  END IF;

  IF v_current_ts.timesheet_id IS NULL AND v_requested_ts.timesheet_id IS NOT NULL THEN
    v_current_ts := v_requested_ts;
  END IF;

  IF v_week.id IS NULL AND v_current_ts.timesheet_id IS NOT NULL THEN
    SELECT cw.*
      INTO v_week
    FROM public.contract_weeks AS cw
    WHERE cw.timesheet_id = v_current_ts.timesheet_id
       OR EXISTS (
         SELECT 1
         FROM public.timesheets AS cw_ts
         WHERE cw_ts.timesheet_id = cw.timesheet_id
           AND cw_ts.booking_id = v_current_ts.booking_id
       )
    ORDER BY
      CASE WHEN cw.timesheet_id = v_current_ts.timesheet_id THEN 0 ELSE 1 END,
      cw.updated_at DESC NULLS LAST,
      cw.created_at DESC NULLS LAST,
      cw.id DESC
    LIMIT 1;
  END IF;

  IF v_current_ts.timesheet_id IS NULL AND v_week.id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'TARGET_NOT_FOUND',
      'message', 'No current timesheet or contract week was found for the supplied identity',
      'timesheet_id', p_timesheet_id,
      'contract_week_id', p_contract_week_id,
      'signature', NULL,
      'backend_row_signature', NULL,
      'row_signature', NULL
    );
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    SELECT tf.*
      INTO v_tsfin
    FROM public.timesheets_financials AS tf
    WHERE tf.timesheet_id = v_current_ts.timesheet_id
      AND tf.is_current = true
    ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
    LIMIT 1;
  END IF;

  IF v_candidate_component_enabled THEN
    v_candidate_component := private._candidate_signature_component_v1(
      CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END
    );
  END IF;

  v_signature_payload := jsonb_strip_nulls(jsonb_build_object(
    'signature_version', 'timesheet_lifecycle_guard_signature_v1',
    'identity', jsonb_build_object(
      'requested_timesheet_id', p_timesheet_id,
      'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
      'booking_id', COALESCE(v_current_ts.booking_id, v_requested_ts.booking_id, v_resolved_booking_id)
    ),
    'timesheet', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_current_ts.booking_id,
      'version', v_current_ts.version,
      'is_current', v_current_ts.is_current,
    'archived_at_utc', v_current_ts.archived_at_utc,
    'archived_by_user_id', v_current_ts.archived_by_user_id,
    'archived_reason_code', v_current_ts.archived_reason_code,
      'status', v_current_ts.status::text,
      'candidate_submission_route_intent', v_current_ts.candidate_submission_route_intent,
      'authorised_at_server', v_current_ts.authorised_at_server,
      'revoked_at', v_current_ts.revoked_at,
      'updated_at', v_current_ts.updated_at
    ) END,
    'timesheets_financials', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE jsonb_build_object(
      'id', v_tsfin.id,
      'timesheet_id', v_tsfin.timesheet_id,
      'is_current', v_tsfin.is_current,
      'processing_status', v_tsfin.processing_status::text,
      'authorised_at_utc', v_tsfin.authorised_at_utc,
      'locked_by_invoice_id', v_tsfin.locked_by_invoice_id,
      'locked_at_utc', v_tsfin.locked_at_utc,
      'paid_at_utc', v_tsfin.paid_at_utc,
      'is_stale', v_tsfin.is_stale,
      'computed_at_utc', v_tsfin.computed_at_utc,
      'updated_at', v_tsfin.updated_at
    ) END,
    'contract_week', CASE WHEN v_week.id IS NULL THEN NULL ELSE jsonb_build_object(
      'id', v_week.id,
      'timesheet_id', v_week.timesheet_id,
      'status', v_week.status::text,
      'updated_at', v_week.updated_at
    ) END
  ));

  IF v_candidate_component_enabled THEN
    v_signature_payload := v_signature_payload || jsonb_build_object('candidate_app', v_candidate_component);
  END IF;

  v_signature := MD5(v_signature_payload::text);

  IF COALESCE(p_include_payload, false) THEN
    BEGIN
      SELECT COALESCE(sd.temp_log, false)
        INTO v_temp_log_enabled
      FROM public.settings_defaults AS sd
      ORDER BY sd.id
      LIMIT 1;
    EXCEPTION
      WHEN undefined_table OR undefined_column THEN
        v_temp_log_enabled := false;
      WHEN OTHERS THEN
        v_temp_log_enabled := false;
    END;

    IF COALESCE(v_temp_log_enabled, false) THEN
      v_component_values := jsonb_strip_nulls(jsonb_build_object(
        'identity', v_signature_payload -> 'identity',
        'timesheet', v_signature_payload -> 'timesheet',
        'timesheets_financials', v_signature_payload -> 'timesheets_financials',
        'contract_week', v_signature_payload -> 'contract_week'
      ));
      v_component_hashes := jsonb_strip_nulls(jsonb_build_object(
        'identity', md5(COALESCE((v_signature_payload -> 'identity')::text, 'null')),
        'timesheet', md5(COALESCE((v_signature_payload -> 'timesheet')::text, 'null')),
        'timesheets_financials', md5(COALESCE((v_signature_payload -> 'timesheets_financials')::text, 'null')),
        'contract_week', md5(COALESCE((v_signature_payload -> 'contract_week')::text, 'null')),
        'full_payload', v_signature
      ));
      IF v_candidate_component_enabled THEN
        v_component_values := v_component_values || jsonb_build_object('candidate_app', v_signature_payload -> 'candidate_app');
        v_component_hashes := v_component_hashes || jsonb_build_object(
          'candidate_app', md5(COALESCE((v_signature_payload -> 'candidate_app')::text, 'null'))
        );
      END IF;
      v_diagnostic_payload := jsonb_strip_nulls(jsonb_build_object(
        'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_PAYLOAD',
        'function_name', 'timesheet_lifecycle_guard_signature_v1',
        'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
        'requested_timesheet_id', p_timesheet_id,
        'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
        'booking_id', COALESCE(v_current_ts.booking_id, v_requested_ts.booking_id, v_resolved_booking_id),
        'signature', v_signature,
        'backend_row_signature', v_signature,
        'component_hashes', v_component_hashes,
        'component_values', v_component_values
      ));
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_PAYLOAD',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(
          CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END,
          p_timesheet_id::text,
          p_contract_week_id::text
        ),
        v_diagnostic_payload
      );
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'signature_version', 'timesheet_lifecycle_guard_signature_v1',
    'signature', v_signature,
    'backend_row_signature', v_signature,
    'row_signature', v_signature,
    'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
    'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
    'requested_timesheet_id', p_timesheet_id,
    'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
    'booking_id', COALESCE(v_current_ts.booking_id, v_requested_ts.booking_id, v_resolved_booking_id),
    'current_version', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.version END,
    'payload', CASE WHEN COALESCE(p_include_payload, false) THEN v_signature_payload ELSE NULL END
  ) || CASE
    WHEN COALESCE(p_include_payload, false) AND COALESCE(v_temp_log_enabled, false) THEN
      jsonb_build_object(
        'component_hashes', v_component_hashes,
        'component_values', v_component_values,
        'diagnostic_payload', v_diagnostic_payload
      )
    ELSE '{}'::jsonb
  END;
END;
$function$;

revoke all on function public.contract_week_manual_draft_upsert_atomic_v1(uuid,text,jsonb,jsonb,boolean,boolean,timestamptz) from public,anon,authenticated;
grant execute on function public.contract_week_manual_draft_upsert_atomic_v1(uuid,text,jsonb,jsonb,boolean,boolean,timestamptz) to service_role;
revoke all on function public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb) from public,anon,authenticated;
grant execute on function public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb) to authenticated,service_role;
revoke all on function public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text) from public,anon,authenticated;
grant execute on function public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text) to public,anon,authenticated,service_role;
revoke all on function public.bulk_process_dataset_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bulk_process_dataset_v1(jsonb) to authenticated,service_role;
revoke all on function public.bulk_authorise_dataset_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bulk_authorise_dataset_v1(jsonb) to authenticated,service_role;
revoke all on function public.bulk_timesheet_row_patch_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bulk_timesheet_row_patch_v1(jsonb) to authenticated,service_role;
revoke all on function public.bulk_process_row_context_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bulk_process_row_context_v1(jsonb) to authenticated,service_role;
revoke all on function public.bulk_authorise_row_context_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bulk_authorise_row_context_v1(jsonb) to authenticated,service_role;
revoke all on function public.timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamptz) from public,anon,authenticated;
grant execute on function public.timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamptz) to authenticated,service_role;
revoke all on function public.timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean) from public,anon,authenticated;
grant execute on function public.timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean) to authenticated,service_role;
