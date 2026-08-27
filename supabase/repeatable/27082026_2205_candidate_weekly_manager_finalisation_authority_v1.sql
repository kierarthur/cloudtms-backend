\set ON_ERROR_STOP on

begin;

-- Candidate electronic finalisation is a trusted server action. The Candidate
-- editing capability remains locked after manager approval, while route-family
-- and lifecycle locks continue to reject unsafe or authoritative mutations.
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
    IF v_candidate_route_guard->>'route_family' IN ('IMPORT_AUTHORITATIVE','MANUAL_NON_QR')
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

revoke all on function public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb) from public,anon,authenticated;
grant execute on function public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb) to service_role;

commit;
