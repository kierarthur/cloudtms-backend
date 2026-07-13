
DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone);
DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone, text);

DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone, text);


DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone);
DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone, text);


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
  );
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





DROP FUNCTION IF EXISTS public.timesheet_daily_manual_process_atomic(uuid, uuid, uuid, jsonb, jsonb, timestamp with time zone);
DROP FUNCTION IF EXISTS public.timesheet_daily_manual_process_atomic(uuid, uuid, uuid, jsonb, jsonb, timestamp with time zone, text);

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
  IF v_current_ts.submission_mode <> 'MANUAL'::public.submission_mode_enum THEN
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





DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_bulk_process_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone);
DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_bulk_process_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone, text);
DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_bulk_process_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone, text, jsonb, jsonb, text);

DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_bulk_process_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone, text, jsonb, jsonb, text);


CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_bulk_process_atomic(p_week_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_timesheet_create_json jsonb DEFAULT NULL::jsonb, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_rotation_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_materialise_staged_evidence boolean DEFAULT true, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text, p_expected_current_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_next_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_response_context text DEFAULT NULL::text, p_queue_timesheet_materialisation_json jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_operation text := CASE WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'contract_week_manual_upsert_bulk_authorise' ELSE 'contract_week_manual_upsert_bulk_process' END;
  v_response_context text := CASE WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'bulk_authorise' ELSE 'bulk_process' END;
  v_result jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_error_detail text := NULL;
  v_detail_json jsonb := NULL;
  v_result_error_code text := NULL;
  v_result_message text := NULL;
  v_attempt integer := 0;
  v_max_attempts constant integer := 6;
  v_retry_wait_ms integer := 0;
  v_total_retry_wait_ms integer := 0;
  v_transient_contention boolean := FALSE;
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_week_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'success', false,
      'operation', v_operation,
      'error_code', 'CONTRACT_WEEK_ID_REQUIRED',
      'message', 'p_week_id is required.'
    );
  END IF;

  LOOP
    v_attempt := v_attempt + 1;
    v_result := '{}'::jsonb;
    v_error_state := NULL;
    v_error_message := NULL;
    v_error_detail := NULL;
    v_detail_json := NULL;
    v_result_error_code := NULL;
    v_result_message := NULL;
    v_transient_contention := FALSE;

    BEGIN
      v_result := public.contract_week_manual_upsert_atomic(
        p_week_id => p_week_id,
        p_expected_timesheet_id => p_expected_timesheet_id,
        p_timesheet_create_json => p_timesheet_create_json,
        p_timesheet_patch_json => p_timesheet_patch_json,
        p_contract_week_patch_json => p_contract_week_patch_json,
        p_tsfin_snapshot_json => COALESCE(p_next_tsfin_snapshot_json, p_tsfin_snapshot_json),
        p_rotation_json => p_rotation_json,
        p_actor_user_id => p_actor_user_id,
        p_materialise_staged_evidence => p_materialise_staged_evidence,
        p_now_utc => v_now,
        p_expected_row_signature => p_expected_row_signature,
        p_queue_timesheet_materialisation_json => p_queue_timesheet_materialisation_json
      );

      v_result_error_code := UPPER(BTRIM(COALESCE(v_result ->> 'error_code', v_result ->> 'error', '')));
      v_result_message := LOWER(BTRIM(COALESCE(v_result ->> 'message', '')));
      v_transient_contention := COALESCE((v_result ->> 'ok')::boolean, true) IS DISTINCT FROM true
        AND (
          v_result_error_code IN ('LOCK_TIMEOUT', '55P03', '40P01', '40001', '57014', 'TRANSIENT_PROCESSING_CONTENTION')
          OR LOWER(v_result_error_code) ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
          OR LOWER(BTRIM(COALESCE(v_result ->> 'sqlstate', ''))) IN ('55p03', '40p01', '40001', '57014')
          OR LOWER(BTRIM(COALESCE(v_result -> 'detail_json' ->> 'error_state', ''))) IN ('55p03', '40p01', '40001', '57014')
          OR v_result_message ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
          OR LOWER(COALESCE(v_result ->> 'detail', '')) ~ '(55p03|40p01|40001|57014|lock[_ ]timeout|deadlock|could not obtain lock|serialization failure)'
        );

      IF v_transient_contention IS DISTINCT FROM true THEN
        EXIT;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_error_state = RETURNED_SQLSTATE,
        v_error_message = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

      BEGIN
        v_detail_json := v_error_detail::jsonb;
      EXCEPTION WHEN OTHERS THEN
        v_detail_json := NULL;
      END;

      v_transient_contention := (
        v_error_state IN ('55P03', '40P01', '40001', '57014')
        OR (
          v_error_state = 'P0001'
          AND UPPER(BTRIM(COALESCE(v_error_message, ''))) IN ('LOCK_TIMEOUT', 'TRANSIENT_PROCESSING_CONTENTION')
        )
        OR LOWER(COALESCE(v_error_message, '')) ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
        OR LOWER(COALESCE(v_error_detail, '')) ~ '(55p03|40p01|40001|lock[_ ]timeout|deadlock|could not obtain lock|serialization failure)'
      );

      IF v_transient_contention IS DISTINCT FROM true THEN
        RETURN jsonb_build_object(
          'ok', false,
          'success', false,
          'operation', v_operation,
          'response_context', v_response_context,
          'bulk_process', v_response_context = 'bulk_process',
          'bulk_authorise', v_response_context = 'bulk_authorise',
          'error_code', CASE WHEN v_error_state = 'P0001' AND NULLIF(BTRIM(v_error_message), '') IS NOT NULL THEN v_error_message ELSE v_error_state END,
          'sqlstate', v_error_state,
          'message', v_error_message,
          'detail', v_error_detail,
          'detail_json', v_detail_json,
          'contract_week_id', p_week_id,
          'expected_timesheet_id', p_expected_timesheet_id,
          'process_retry_count', GREATEST(v_attempt - 1, 0),
          'process_retry_wait_ms', v_total_retry_wait_ms,
          'refresh_required', true,
          'cache_invalidation_hints', jsonb_build_object(
            'changed_domains', jsonb_build_array('timesheet_lifecycle'),
            'contract_week_id', p_week_id,
            'timesheet_id', p_expected_timesheet_id
          )
        );
      END IF;
    END;

    IF v_attempt >= v_max_attempts THEN
      RETURN jsonb_build_object(
        'ok', false,
        'success', false,
        'operation', v_operation,
        'response_context', v_response_context,
        'bulk_process', v_response_context = 'bulk_process',
        'bulk_authorise', v_response_context = 'bulk_authorise',
        'error_code', 'TRANSIENT_PROCESSING_CONTENTION',
        'message', 'The timesheet could not be completed immediately. The row has been safely refreshed.',
        'contract_week_id', p_week_id,
        'expected_timesheet_id', p_expected_timesheet_id,
        'process_retry_count', GREATEST(v_attempt - 1, 0),
        'process_retry_wait_ms', v_total_retry_wait_ms,
        'refresh_required', true,
        'cache_invalidation_hints', jsonb_build_object(
          'changed_domains', jsonb_build_array('timesheet_lifecycle'),
          'contract_week_id', p_week_id,
          'timesheet_id', p_expected_timesheet_id
        )
      );
    END IF;

    v_retry_wait_ms := LEAST(2000, (150::numeric * POWER(2::numeric, GREATEST(v_attempt - 1, 0)::numeric))::integer) + FLOOR(RANDOM() * 125)::integer;
    v_total_retry_wait_ms := v_total_retry_wait_ms + v_retry_wait_ms;
    PERFORM pg_sleep(v_retry_wait_ms::numeric / 1000::numeric);
  END LOOP;

  RETURN COALESCE(v_result, '{}'::jsonb)
    || jsonb_build_object(
      'ok', COALESCE((v_result ->> 'ok')::boolean, true),
      'success', COALESCE((v_result ->> 'success')::boolean, true),
      'operation', v_operation,
      'response_context', v_response_context,
      'bulk_process', v_response_context = 'bulk_process',
      'bulk_authorise', v_response_context = 'bulk_authorise',
      'process_retry_count', GREATEST(v_attempt - 1, 0),
      'process_retry_wait_ms', v_total_retry_wait_ms,
      'transient_contention_recovered', v_attempt > 1,
      'requires_affected_row_refresh', true,
      'refresh_required', true
    );
END;
$function$;



CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_unprocess_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
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
  v_new_status public.ts_fin_processing_status_enum := 'UNPROCESSED'::public.ts_fin_processing_status_enum;
  v_has_segment_invoice_lock boolean := false;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_error_state text := NULL;
  v_error_message text := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_ID_REQUIRED', 'message', 'p_timesheet_id is required.');
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'EXPECTED_TIMESHEET_ID_REQUIRED', 'message', 'p_expected_timesheet_id is required.');
  END IF;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_NOT_FOUND', 'message', 'Timesheet was not found.', 'requested_timesheet_id', p_timesheet_id);
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
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'CURRENT_TIMESHEET_NOT_FOUND', 'message', 'Current timesheet was not found.', 'requested_timesheet_id', p_timesheet_id);
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'Timesheet has moved to a newer current row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id
    );
  END IF;

  IF v_current_ts.sheet_scope IS DISTINCT FROM 'DAILY'::public.timesheet_scope_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'NOT_DAILY', 'message', 'Timesheet is not DAILY; daily manual unprocess only applies to DAILY sheets.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  IF v_current_ts.submission_mode IS DISTINCT FROM 'MANUAL'::public.submission_mode_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'NOT_MANUAL', 'message', 'Timesheet must be MANUAL before unprocessing.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_ARCHIVED', 'message', 'Archived timesheets must be Unarchived before lifecycle actions.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_AUTHORISED_EDIT_BLOCKED', 'message', 'This timesheet is authorised. Unauthorise it before unprocessing.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'NO_TSFIN', 'message', 'No current financial snapshot exists for this timesheet.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, NULL::uuid, false);
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');

  IF COALESCE((v_before_signature_json ->> 'ok')::boolean, false) IS DISTINCT FROM true OR v_current_row_signature IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'ROW_SIGNATURE_UNAVAILABLE',
      'message', 'Unable to compute the current lifecycle signature for this timesheet.',
      'current_timesheet_id', v_current_ts.timesheet_id
    );
  END IF;

  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'message', 'Timesheet changed after it was loaded. Refresh the row and try again.',
      'current_timesheet_id', v_current_ts.timesheet_id,
      'expected_row_signature', v_expected_row_signature,
      'current_row_signature', v_current_row_signature
    );
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
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'TIMESHEET_LOCKED_BY_INVOICE',
      'specific_error_code', 'TIMESHEET_LOCKED_BY_INVOICE',
      'message', 'Cannot unprocess: timesheet is locked by an invoice.',
      'current_timesheet_id', v_current_ts.timesheet_id,
      'current_row_signature', v_current_row_signature
    );
  END IF;

  IF v_previous_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'ALREADY_UNPROCESSED', 'message', 'Timesheet is already UNPROCESSED.', 'current_timesheet_id', v_current_ts.timesheet_id, 'previous_status', v_previous_status::text, 'current_row_signature', v_current_row_signature);
  END IF;

  IF v_previous_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'PROCESSING_STATUS_NOT_UNPROCESSABLE',
      'message', 'Timesheet is not in a processing state that can be moved back to UNPROCESSED.',
      'current_timesheet_id', v_current_ts.timesheet_id,
      'previous_status', v_previous_status::text,
      'current_row_signature', v_current_row_signature
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.timesheet_financial_retention AS retention
    WHERE retention.timesheet_id = v_current_ts.timesheet_id
  ) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'success', false,
      'operation', 'daily_manual_unprocess',
      'error_code', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
      'specific_error_code', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
      'message', 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'timesheet_id', v_current_ts.timesheet_id,
      'current_row_signature', v_current_row_signature,
      'backend_row_signature', v_current_row_signature,
      'row_signature', v_current_row_signature,
      'has_retained_financial_history', true,
      'can_unprocess', false,
      'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
      'unprocess_action_visible', true,
      'row_patch', jsonb_build_object(
        'timesheet_id', v_current_ts.timesheet_id,
        'current_timesheet_id', v_current_ts.timesheet_id,
        'row_key', 'timesheet:' || v_current_ts.timesheet_id::text,
        'row_signature', v_current_row_signature,
        'backend_row_signature', v_current_row_signature,
        'has_retained_financial_history', true,
        'can_unprocess', false,
        'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
        'unprocess_action_visible', true
      ),
      'action_flags', jsonb_build_object(
        'has_retained_financial_history', true,
        'can_unprocess', false,
        'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
        'unprocess_action_visible', true
      )
    );
  END IF;

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         processed_by_user_id = NULL,
         processed_at_utc = NULL,
         authorised_by_user_id = NULL,
         authorised_at_utc = NULL,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  IF v_current_tsfin.id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'TSFIN_UPDATE_FAILED', 'message', 'Failed to move daily timesheet back to UNPROCESSED.', 'current_timesheet_id', v_current_ts.timesheet_id);
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, NULL::uuid, false);
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_DAILY_MANUAL_UNPROCESSED',
    jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'timesheet_financials_id', v_current_tsfin.id,
      'previous_processing_status', v_previous_status::text,
      'previous_row_signature', v_current_row_signature
    ),
    jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'timesheet_financials_id', v_current_tsfin.id,
      'new_processing_status', v_new_status::text,
      'processed_at_utc', NULL::text,
      'processed_by_user_id', NULL::text,
      'authorised_at_utc', NULL::text,
      'authorised_by_user_id', NULL::text,
      'new_row_signature', v_after_row_signature
    ),
    'DAILY_MANUAL_UNPROCESS',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'daily_manual_unprocess',
    'unprocessed', true,
    'requested_timesheet_id', p_timesheet_id,
    'expected_timesheet_id', p_expected_timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'timesheet_id', v_current_ts.timesheet_id,
    'timesheet_financials_id', v_current_tsfin.id,
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_status', v_previous_status::text,
    'previous_processing_status', v_previous_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'status_transition', jsonb_build_object('from', v_previous_status::text, 'to', v_new_status::text, 'processed_at_utc', NULL::text, 'processed_by_user_id', NULL::text),
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'count_deltas', jsonb_build_object('unprocessed', 1, 'processed', -1),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials'), 'timesheet_id', v_current_ts.timesheet_id)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'operation', 'daily_manual_unprocess', 'error_code', 'LOCK_TIMEOUT', 'message', 'The timesheet is currently locked by another operation.', 'timesheet_id', p_timesheet_id);
  END IF;
  RAISE;
END;
$function$;




CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_unprocess_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  RETURN public.timesheet_daily_manual_unprocess_atomic(
    p_timesheet_id => p_timesheet_id,
    p_expected_timesheet_id => p_expected_timesheet_id,
    p_actor_user_id => p_actor_user_id,
    p_now_utc => p_now_utc,
    p_expected_row_signature => NULL::text
  );
END;
$function$;




DROP FUNCTION IF EXISTS public.timesheet_authorise_generic_atomic(uuid, uuid, uuid, timestamp with time zone);
DROP FUNCTION IF EXISTS public.timesheet_authorise_generic_atomic(uuid, uuid, uuid, timestamp with time zone, text);



CREATE OR REPLACE FUNCTION public.timesheet_authorise_generic_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
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
  v_contract_week public.contract_weeks%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := NULL;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_has_segment_invoice_lock boolean := false;
  v_qr_unsigned_blocked boolean := false;
  v_validation_status text := NULL;
  v_validation_pre_validated boolean := false;
  v_validation_ok boolean := false;
  v_contract_requires_hr boolean := false;
  v_client_requires_hr boolean := false;
  v_hr_validation_required_for_invoice boolean := false;
  v_must_hold_for_hr_validation boolean := false;
  v_prevalidated_fast_track boolean := false;
  v_force_ready_for_invoice boolean := false;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_diag_started_at timestamptz := clock_timestamp();
  v_temp_log_enabled boolean := false;
  v_advance_state_refresh_json jsonb := '{}'::jsonb;
  v_has_uncleared_advance_override boolean := false;
BEGIN
  PERFORM set_config('lock_timeout', '300ms', true);

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

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'entry',
      'timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'actor_user_id_present', p_actor_user_id IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
  END IF;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_requested_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'requested_timesheet_resolved',
      'timesheet_id', v_requested_ts.timesheet_id,
      'is_current', COALESCE(v_requested_ts.is_current, false),
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
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
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH', DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'current_timesheet_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'NO_TSFIN')::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_current_tsfin.processing_status::text,
      'old_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_current_ts.contract_id IS NOT NULL THEN
    SELECT COALESCE(c.requires_hr, false)
      INTO v_contract_requires_hr
    FROM public.contracts AS c
    WHERE c.id = v_current_ts.contract_id
    LIMIT 1;
  END IF;

  SELECT
    COALESCE(vts.client_requires_hr, COALESCE(v_contract_requires_hr, false)),
    COALESCE(vts.hr_validation_required_for_invoice, COALESCE(v_contract_requires_hr, false)),
    CASE
      WHEN vts.validation_status IS NULL THEN NULL::text
      ELSE UPPER(vts.validation_status::text)
    END
    INTO v_client_requires_hr,
         v_hr_validation_required_for_invoice,
         v_validation_status
  FROM public.v_timesheets_summary_base AS vts
  WHERE vts.timesheet_id = v_current_ts.timesheet_id
  LIMIT 1;

  v_client_requires_hr := COALESCE(v_client_requires_hr, v_contract_requires_hr, false);
  v_hr_validation_required_for_invoice := COALESCE(v_hr_validation_required_for_invoice, v_contract_requires_hr, false);

  SELECT cw.*
    INTO v_contract_week
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_ts.timesheet_id
     OR EXISTS (
       SELECT 1
       FROM public.timesheets AS cw_ts
       WHERE cw_ts.timesheet_id = cw.timesheet_id
         AND cw_ts.booking_id = v_current_ts.booking_id
     )
  ORDER BY CASE WHEN cw.timesheet_id = v_current_ts.timesheet_id THEN 0 ELSE 1 END,
           cw.updated_at DESC NULLS LAST,
           cw.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND v_contract_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TARGET_NOT_FOUND',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'INVOICED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text, 'lock_scope', 'contract_week_status')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'CANCELLED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_NOT_AUTHORISABLE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text)::text;
  END IF;

  v_prev_status := v_current_tsfin.processing_status;

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

  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'invoice_id', v_current_tsfin.locked_by_invoice_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL OR v_current_tsfin.authorised_at_utc IS NOT NULL OR v_contract_week.status = 'AUTHORISED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  v_qr_unsigned_blocked := COALESCE(
    v_current_tsfin.processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum
    OR (
      v_current_ts.qr_status = 'PENDING'::public.timesheet_qr_status_enum
      AND NULLIF(BTRIM(COALESCE(v_current_ts.qr_token, '')), '') IS NOT NULL
      AND v_current_ts.qr_generated_at IS NOT NULL
      AND v_current_ts.qr_scanned_at IS NULL
    ),
    false
  );
  IF v_qr_unsigned_blocked THEN
    RAISE EXCEPTION USING MESSAGE = 'AWAITING_SIGNED_QR', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        v_current_ts.timesheet_id::text,
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'timesheet_authorise_generic_atomic',
          'stage', 'row_signature_mismatch_before_authorise',
          'action', 'authorise',
          'route_family', 'timesheet_lifecycle',
          'timesheet_id', p_timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json,
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
        ))
      );
    END IF;
    RAISE EXCEPTION USING MESSAGE = 'ROW_SIGNATURE_MISMATCH', DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  SELECT COALESCE(tv.pre_validated, false)
    INTO v_validation_pre_validated
  FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = v_current_ts.timesheet_id
  ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    v_validation_pre_validated := false;
  END IF;

  v_validation_ok := COALESCE(v_validation_status, '') IN ('VALIDATION_OK', 'OVERRIDDEN');
  v_force_ready_for_invoice := v_current_tsfin.basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum);
  v_must_hold_for_hr_validation := COALESCE(v_hr_validation_required_for_invoice, false) AND NOT v_validation_ok;
  v_prevalidated_fast_track := COALESCE(v_validation_pre_validated, false) AND v_validation_ok;

  IF v_prev_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN
    RAISE EXCEPTION USING MESSAGE = 'AUTHORISE_NOT_ALLOWED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'processing_status', v_prev_status::text)::text;
  END IF;

  v_new_status := CASE
    WHEN v_force_ready_for_invoice THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN v_must_hold_for_hr_validation THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    WHEN v_prevalidated_fast_track THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN COALESCE(v_client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
  END;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'before_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'tsfin_id', v_current_tsfin.id,
      'current_row_signature_present', v_current_row_signature IS NOT NULL,
      'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'timesheet_authorise', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);

  UPDATE public.timesheets AS ts
     SET authorised_at_server = v_now,
         revoked_at = NULL,
         revoked_reason = NULL,
         revoked_by = NULL,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'timesheets_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'new_authorised_present', v_current_ts.authorised_at_server IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         authorised_by_user_id = p_actor_user_id,
         authorised_at_utc = v_now,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_prev_status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_contract_week.id IS NOT NULL THEN
    UPDATE public.contract_weeks AS cw
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
     WHERE cw.id = v_contract_week.id
     RETURNING * INTO v_contract_week;
  END IF;

  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'off', true);

  SELECT EXISTS (
    SELECT 1
    FROM public._pay_timesheet_rotation_scope(ARRAY[v_current_ts.timesheet_id]) AS rs
    JOIN public.timesheet_payment_overrides AS payment_override
      ON payment_override.timesheet_id = rs.family_timesheet_id
    WHERE payment_override.cleared_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(payment_override.override_type, ''))) = 'ADVANCE_THIS_PAYMENT'
  ) INTO v_has_uncleared_advance_override;

  IF COALESCE(v_has_uncleared_advance_override, false) THEN
    v_advance_state_refresh_json := public.pay_timesheet_summary_advance_state_refresh(
      p_timesheet_id => v_current_ts.timesheet_id,
      p_actor_user_id => p_actor_user_id
    );

    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_done',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'refresh_result', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  ELSE
    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_skipped',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'reason', 'NO_ACTIVE_ADVANCE_THIS_PAYMENT_OVERRIDE',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'after_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'row_signature_present', v_after_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_AUTHORISED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'previous_processing_status', v_prev_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'new_processing_status', v_new_status::text, 'authorised_at_utc', v_now, 'authorised_by_user_id', p_actor_user_id, 'new_row_signature', v_after_row_signature),
    'TIMESHEET_AUTHORISE',
    p_actor_user_id
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'audit_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'return',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'new_processing_status', v_new_status::text,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'timesheet_authorise',
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'requested_timesheet_id', v_requested_ts.timesheet_id,
    'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
    'booking_id', v_current_ts.booking_id,
    'candidate_id', v_current_tsfin.candidate_id,
    'authorised_at_server', v_current_ts.authorised_at_server,
    'revoked_at', v_current_ts.revoked_at,
    'authorised_at_utc', v_current_tsfin.authorised_at_utc,
    'advance_state_refresh', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_processing_status', v_prev_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'validation_status', v_validation_status,
    'validation_pre_validated', COALESCE(v_validation_pre_validated, false),
    'hr_validation_required_for_invoice', COALESCE(v_hr_validation_required_for_invoice, false),
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'booking_id', v_current_ts.booking_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', CASE WHEN COALESCE(v_has_uncleared_advance_override, false) THEN jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_summary_pay_state_cache') ELSE jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks') END, 'timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'exception',
      'timesheet_id', p_timesheet_id,
      'sqlstate', v_error_state,
      'error_message', v_error_message,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );
  IF v_error_state = '55P03' THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;
  RAISE;
END;
$function$;



DROP FUNCTION IF EXISTS public.timesheet_unauthorise_atomic(uuid, uuid, timestamp with time zone);
DROP FUNCTION IF EXISTS public.timesheet_unauthorise_atomic(uuid, uuid, uuid, timestamp with time zone, text);

CREATE OR REPLACE FUNCTION public.timesheet_unauthorise_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
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
  v_contract_week public.contract_weeks%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := 'PENDING_AUTH'::public.ts_fin_processing_status_enum;
  v_has_segment_invoice_lock boolean := false;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_diag_started_at timestamptz := clock_timestamp();
  v_temp_log_enabled boolean := false;
  v_advance_state_refresh_json jsonb := '{}'::jsonb;
  v_has_uncleared_advance_override boolean := false;
BEGIN
  PERFORM set_config('lock_timeout', '300ms', true);

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

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'entry',
      'timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'actor_user_id_present', p_actor_user_id IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
  END IF;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_requested_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'requested_timesheet_resolved',
      'timesheet_id', v_requested_ts.timesheet_id,
      'is_current', COALESCE(v_requested_ts.is_current, false),
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
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
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH', DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'current_timesheet_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'NO_TSFIN')::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'tsfin_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_current_tsfin.processing_status::text,
      'old_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  SELECT cw.*
    INTO v_contract_week
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_ts.timesheet_id
  ORDER BY cw.updated_at DESC NULLS LAST, cw.id DESC
  LIMIT 1
  FOR UPDATE;

  v_prev_status := v_current_tsfin.processing_status;

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

  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NULL AND v_current_tsfin.authorised_at_utc IS NULL AND COALESCE(v_contract_week.status = 'AUTHORISED'::public.contract_week_status_enum, false) = false THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_UNAUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        v_current_ts.timesheet_id::text,
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'timesheet_unauthorise_atomic',
          'stage', 'row_signature_mismatch_before_unauthorise',
          'action', 'unauthorise',
          'route_family', 'timesheet_lifecycle',
          'timesheet_id', p_timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json,
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
        ))
      );
    END IF;
    RAISE EXCEPTION USING MESSAGE = 'ROW_SIGNATURE_MISMATCH', DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'before_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'tsfin_id', v_current_tsfin.id,
      'current_row_signature_present', v_current_row_signature IS NOT NULL,
      'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'timesheet_unauthorise', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);

  UPDATE public.timesheets AS ts
     SET authorised_at_server = NULL,
         revoked_at = v_now,
         revoked_reason = 'TIMESHEET_UNAUTHORISE',
         revoked_by = p_actor_user_id::text,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'timesheets_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'new_authorised_present', v_current_ts.authorised_at_server IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         authorised_by_user_id = NULL,
         authorised_at_utc = NULL,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'tsfin_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_prev_status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum THEN
    IF v_contract_week.id IS NULL THEN
      SELECT cw.*
        INTO v_contract_week
      FROM public.contract_weeks AS cw
      JOIN public.timesheets AS tw ON tw.timesheet_id = cw.timesheet_id
      WHERE tw.booking_id = v_current_ts.booking_id
      ORDER BY cw.updated_at DESC NULLS LAST, cw.id DESC
      LIMIT 1
      FOR UPDATE OF cw;
    END IF;
    IF v_contract_week.id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET')::text;
    END IF;
    UPDATE public.contract_weeks AS cw
       SET timesheet_id = v_current_ts.timesheet_id,
           status = 'SUBMITTED'::public.contract_week_status_enum,
           updated_at = v_now
     WHERE cw.id = v_contract_week.id
     RETURNING * INTO v_contract_week;
  END IF;

  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'off', true);

  SELECT EXISTS (
    SELECT 1
    FROM public._pay_timesheet_rotation_scope(ARRAY[v_current_ts.timesheet_id]) AS rs
    JOIN public.timesheet_payment_overrides AS payment_override
      ON payment_override.timesheet_id = rs.family_timesheet_id
    WHERE payment_override.cleared_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(payment_override.override_type, ''))) = 'ADVANCE_THIS_PAYMENT'
  ) INTO v_has_uncleared_advance_override;

  IF COALESCE(v_has_uncleared_advance_override, false) THEN
    v_advance_state_refresh_json := public.pay_timesheet_summary_advance_state_refresh(
      p_timesheet_id => v_current_ts.timesheet_id,
      p_actor_user_id => p_actor_user_id
    );

    PERFORM public._temp_diag_log(
      'TEMP_UNAUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_unauthorise_atomic',
        'stage', 'advance_state_refresh_done',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'refresh_result', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  ELSE
    PERFORM public._temp_diag_log(
      'TEMP_UNAUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_unauthorise_atomic',
        'stage', 'advance_state_refresh_skipped',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'reason', 'NO_ACTIVE_ADVANCE_THIS_PAYMENT_OVERRIDE',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'after_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'row_signature_present', v_after_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_UNAUTHORISED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'previous_processing_status', v_prev_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'new_processing_status', v_new_status::text, 'new_row_signature', v_after_row_signature),
    'TIMESHEET_UNAUTHORISE',
    p_actor_user_id
  );

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'audit_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'return',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'new_processing_status', v_new_status::text,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'timesheet_unauthorise',
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'requested_timesheet_id', v_requested_ts.timesheet_id,
    'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
    'booking_id', v_current_ts.booking_id,
    'candidate_id', v_current_tsfin.candidate_id,
    'authorised_at_server', v_current_ts.authorised_at_server,
    'revoked_at', v_current_ts.revoked_at,
    'authorised_at_utc', v_current_tsfin.authorised_at_utc,
    'advance_state_refresh', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_processing_status', v_prev_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'booking_id', v_current_ts.booking_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', CASE WHEN COALESCE(v_has_uncleared_advance_override, false) THEN jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_summary_pay_state_cache') ELSE jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks') END, 'timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  PERFORM public._temp_diag_log(
    'TEMP_UNAUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_unauthorise_atomic',
      'stage', 'exception',
      'timesheet_id', p_timesheet_id,
      'sqlstate', v_error_state,
      'error_message', v_error_message,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );
  IF v_error_state = '55P03' THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;
  RAISE;
END;
$function$;




DROP FUNCTION IF EXISTS public.timesheet_authorise_bulk_atomic(jsonb, uuid, timestamp with time zone);

CREATE OR REPLACE FUNCTION public.timesheet_authorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_items_array jsonb := '[]'::jsonb;
  v_requested_count integer := 0;
  v_success_count integer := 0;
  v_failure_count integer := 0;
  v_uuid_re text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
  v_out jsonb := '{}'::jsonb;
  v_error_state text := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '300ms', true);

  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;
  IF p_items IS NOT NULL AND jsonb_typeof(p_items) NOT IN ('array', 'object') THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;

  v_items_array := CASE
    WHEN p_items IS NULL THEN '[]'::jsonb
    WHEN jsonb_typeof(p_items) = 'array' THEN p_items
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'items') = 'array' THEN p_items -> 'items'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'rows') = 'array' THEN p_items -> 'rows'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selected') = 'array' THEN p_items -> 'selected'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selections') = 'array' THEN p_items -> 'selections'
    WHEN jsonb_typeof(p_items) = 'object' THEN jsonb_build_array(p_items)
    ELSE '[]'::jsonb
  END;
  v_requested_count := jsonb_array_length(v_items_array);
  IF v_requested_count > 100 THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'TOO_MANY_ITEMS', 'requested_count', v_requested_count, 'success_count', 0, 'failure_count', v_requested_count, 'results', '[]'::jsonb);
  END IF;

  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_cw;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_results;

  CREATE TEMP TABLE timesheet_authorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN jsonb_typeof(input_values.item_json) = 'object' THEN input_values.item_json ELSE jsonb_build_object('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'row_key', input_values.item_json ->> 'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'timesheet_id', input_values.item_json ->> 'timesheetId', input_values.item_json ->> 'current_timesheet_id', input_values.item_json ->> 'currentTimesheetId', input_values.item_json ->> 'requested_timesheet_id', input_values.item_json ->> 'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'expected_timesheet_id', input_values.item_json ->> 'expectedTimesheetId', input_values.item_json ->> 'expected_current_timesheet_id', input_values.item_json ->> 'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'backend_row_signature', input_values.item_json ->> 'row_signature', input_values.item_json ->> 'rowSignature', input_values.item_json ->> 'expected_row_signature', input_values.item_json ->> 'expectedRowSignature', '')), '') AS expected_row_signature
  FROM jsonb_array_elements(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_authorise_bulk_state ON COMMIT DROP AS
  SELECT
    item_rows.ordinal,
    item_rows.item_json,
    item_rows.row_key,
    CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END AS requested_timesheet_id,
    CASE WHEN item_rows.expected_timesheet_id_text ~* v_uuid_re THEN item_rows.expected_timesheet_id_text::uuid ELSE NULL::uuid END AS expected_timesheet_id,
    item_rows.expected_row_signature,
    req_ts.timesheet_id AS db_requested_timesheet_id,
    req_ts.booking_id AS requested_booking_id,
    cur_ts.timesheet_id AS current_timesheet_id,
    cur_ts.archived_at_utc AS current_archived_at_utc,
    cur_ts.booking_id AS current_booking_id,
    cur_ts.version AS current_version,
    cur_ts.is_current AS current_is_current,
    cur_ts.authorised_at_server AS current_authorised_at_server,
    cur_ts.qr_status AS current_qr_status,
    cur_ts.qr_token AS current_qr_token,
    cur_ts.qr_generated_at AS current_qr_generated_at,
    cur_ts.qr_scanned_at AS current_qr_scanned_at,
    cur_ts.sheet_scope AS current_sheet_scope,
    cur_ts.contract_id AS current_contract_id,
    tf.id AS tsfin_id,
    tf.processing_status AS tsfin_processing_status,
    tf.basis AS tsfin_basis,
    tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    tf.paid_at_utc AS tsfin_paid_at_utc,
    tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    tf.authorised_at_utc AS tsfin_authorised_at_utc,
    COALESCE(summary_row.client_requires_hr, contract_row.requires_hr, false) AS client_requires_hr,
    COALESCE(summary_row.hr_validation_required_for_invoice, contract_row.requires_hr, false) AS hr_validation_required_for_invoice,
    COALESCE(summary_row.validation_status_text, NULL::text) AS summary_validation_status,
    cw.id AS contract_week_id,
    cw.status AS contract_week_status,
    sig.signature_json AS signature_json,
    sig.signature_text AS current_row_signature,
    COALESCE(segment_state.has_segment_invoice_lock, false) AS has_segment_invoice_lock,
    COALESCE(validation_state.validation_ok, false) AS validation_ok
  FROM pg_temp.timesheet_authorise_bulk_items AS item_rows
  LEFT JOIN LATERAL (
    SELECT ts_req.*
    FROM public.timesheets AS ts_req
    WHERE ts_req.timesheet_id = CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END
    LIMIT 1
    FOR UPDATE
  ) AS req_ts ON true
  LEFT JOIN LATERAL (
    SELECT ts_cur.*
    FROM public.timesheets AS ts_cur
    WHERE req_ts.booking_id IS NOT NULL
      AND ts_cur.booking_id = req_ts.booking_id
    ORDER BY CASE WHEN ts_cur.is_current THEN 0 ELSE 1 END, ts_cur.version DESC NULLS LAST, ts_cur.updated_at DESC NULLS LAST, ts_cur.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS cur_ts ON true
  LEFT JOIN LATERAL (
    SELECT tf_sel.*
    FROM public.timesheets_financials AS tf_sel
    WHERE tf_sel.timesheet_id = cur_ts.timesheet_id
      AND tf_sel.is_current = true
    ORDER BY tf_sel.computed_at_utc DESC NULLS LAST, tf_sel.updated_at DESC NULLS LAST, tf_sel.created_at DESC NULLS LAST, tf_sel.id DESC
    LIMIT 1
    FOR UPDATE
  ) AS tf ON true
  LEFT JOIN LATERAL (
    SELECT c.requires_hr
    FROM public.contracts AS c
    WHERE c.id = cur_ts.contract_id
    LIMIT 1
  ) AS contract_row ON true
  LEFT JOIN LATERAL (
    SELECT
      COALESCE(vts.client_requires_hr, false) AS client_requires_hr,
      COALESCE(vts.hr_validation_required_for_invoice, false) AS hr_validation_required_for_invoice,
      CASE
        WHEN vts.validation_status IS NULL THEN NULL::text
        ELSE UPPER(vts.validation_status::text)
      END AS validation_status_text
    FROM public.v_timesheets_summary_base AS vts
    WHERE vts.timesheet_id = cur_ts.timesheet_id
    LIMIT 1
  ) AS summary_row ON true
  LEFT JOIN LATERAL (
    SELECT cw_sel.*
    FROM public.contract_weeks AS cw_sel
    WHERE cw_sel.timesheet_id = cur_ts.timesheet_id
       OR EXISTS (
         SELECT 1
         FROM public.timesheets AS cw_ts
         WHERE cw_ts.timesheet_id = cw_sel.timesheet_id
           AND cw_ts.booking_id = cur_ts.booking_id
       )
    ORDER BY CASE WHEN cw_sel.timesheet_id = cur_ts.timesheet_id THEN 0 ELSE 1 END,
             cw_sel.updated_at DESC NULLS LAST,
             cw_sel.id DESC
    LIMIT 1
    FOR UPDATE OF cw_sel
  ) AS cw ON true
  LEFT JOIN LATERAL (
    SELECT public.timesheet_lifecycle_signature_v1(cur_ts.timesheet_id, cw.id, false) AS signature_json
  ) AS sig_raw ON true
  LEFT JOIN LATERAL (
    SELECT sig_raw.signature_json AS signature_json,
           NULLIF(BTRIM(COALESCE(sig_raw.signature_json ->> 'backend_row_signature', sig_raw.signature_json ->> 'row_signature', sig_raw.signature_json ->> 'signature', '')), '') AS signature_text
  ) AS sig ON true
  LEFT JOIN LATERAL (
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object' AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments') = 'array' THEN tf.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    ) AS has_segment_invoice_lock
  ) AS segment_state ON true
  LEFT JOIN LATERAL (
    SELECT COALESCE(UPPER(COALESCE(summary_row.validation_status_text, tv.status::text)) IN ('VALIDATION_OK', 'OVERRIDDEN'), false) AS validation_ok
    FROM public.timesheet_validations AS tv
    WHERE tv.timesheet_id = cur_ts.timesheet_id
    ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
    LIMIT 1
  ) AS validation_state ON true;

  CREATE TEMP TABLE timesheet_authorise_bulk_work ON COMMIT DROP AS
  SELECT
    state_rows.*,
    CASE
      WHEN state_rows.requested_timesheet_id IS NULL THEN 'TIMESHEET_ID_REQUIRED'
      WHEN state_rows.expected_timesheet_id IS NULL THEN 'EXPECTED_TIMESHEET_ID_REQUIRED'
      WHEN state_rows.db_requested_timesheet_id IS NULL THEN 'TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_timesheet_id IS NULL THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_is_current IS DISTINCT FROM true THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.expected_timesheet_id IS DISTINCT FROM state_rows.current_timesheet_id THEN 'TIMESHEET_MOVED'
      WHEN state_rows.tsfin_id IS NULL THEN 'NO_TSFIN'
      WHEN state_rows.expected_row_signature IS NOT NULL AND COALESCE(state_rows.current_row_signature, '') IS DISTINCT FROM state_rows.expected_row_signature THEN 'ROW_SIGNATURE_MISMATCH'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_id IS NULL THEN 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'INVOICED'::public.contract_week_status_enum THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'CANCELLED'::public.contract_week_status_enum THEN 'CONTRACT_WEEK_NOT_AUTHORISABLE'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_status = 'AUTHORISED'::public.contract_week_status_enum THEN 'ALREADY_AUTHORISED'
      WHEN state_rows.current_archived_at_utc IS NOT NULL THEN 'TIMESHEET_ARCHIVED'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.has_segment_invoice_lock THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_authorised_at_server IS NOT NULL OR state_rows.tsfin_authorised_at_utc IS NOT NULL THEN 'ALREADY_AUTHORISED'
      WHEN state_rows.tsfin_processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum OR (state_rows.current_qr_status = 'PENDING'::public.timesheet_qr_status_enum AND NULLIF(BTRIM(COALESCE(state_rows.current_qr_token, '')), '') IS NOT NULL AND state_rows.current_qr_generated_at IS NOT NULL AND state_rows.current_qr_scanned_at IS NULL) THEN 'AWAITING_SIGNED_QR'
      WHEN state_rows.tsfin_processing_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN 'AUTHORISE_NOT_ALLOWED'
      ELSE NULL::text
    END AS failure_code,
    CASE
      WHEN state_rows.tsfin_basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.hr_validation_required_for_invoice, false) AND NOT COALESCE(state_rows.validation_ok, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.validation_ok, false) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      WHEN COALESCE(state_rows.client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
      ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    END AS new_processing_status
  FROM pg_temp.timesheet_authorise_bulk_state AS state_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
       SET authorised_at_server = v_now,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
     WHERE work_rows.failure_code IS NULL
       AND ts_upd.timesheet_id = work_rows.current_timesheet_id
       AND ts_upd.is_current = true
     RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
       SET processing_status = work_rows.new_processing_status,
           authorised_by_user_id = p_actor_user_id,
           authorised_at_utc = v_now,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND tf_upd.id = work_rows.tsfin_id
       AND tf_upd.is_current = true
     RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_cw ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.contract_weeks AS cw_upd
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
      FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND cw_upd.id = work_rows.contract_week_id
     RETURNING cw_upd.id, cw_upd.timesheet_id, cw_upd.status, cw_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  PERFORM public._audit_insert(
    'timesheet_batch',
    'bulk_authorise:' || v_now::text,
    'TIMESHEET_BULK_AUTHORISED',
    jsonb_build_object('requested_count', v_requested_count, 'actor_user_id', p_actor_user_id),
    jsonb_build_object(
      'succeeded_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
      'failed_items', COALESCE((SELECT jsonb_agg(jsonb_build_object('item_index', work_rows.ordinal, 'timesheet_id', work_rows.requested_timesheet_id, 'error_code', work_rows.failure_code) ORDER BY work_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_work AS work_rows WHERE work_rows.failure_code IS NOT NULL), '[]'::jsonb)
    ),
    'BULK_AUTHORISE',
    p_actor_user_id
  );

  CREATE TEMP TABLE timesheet_authorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    (work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL) AS success,
    jsonb_build_object(
      'item_index', work_rows.ordinal,
      'success', work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL,
      'action', 'AUTHORISE',
      'error_code', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') END,
      'requested_timesheet_id', work_rows.requested_timesheet_id,
      'expected_timesheet_id', work_rows.expected_timesheet_id,
      'expected_row_signature', work_rows.expected_row_signature,
      'current_row_signature', work_rows.current_row_signature,
      'current_timesheet_id', work_rows.current_timesheet_id,
      'current_version', COALESCE(updated_ts.version, work_rows.current_version),
      'processing_status_before', work_rows.tsfin_processing_status::text,
      'processing_status_after', CASE WHEN updated_tf.processing_status IS NULL THEN NULL ELSE updated_tf.processing_status::text END,
      'contract_week_id', work_rows.contract_week_id,
      'affected_rows', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN jsonb_build_array(jsonb_build_object('timesheet_id', work_rows.current_timesheet_id, 'contract_week_id', work_rows.contract_week_id, 'booking_id', work_rows.current_booking_id, 'row_key', 'timesheet:' || work_rows.current_timesheet_id::text)) ELSE '[]'::jsonb END
    ) AS result_json
  FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id;

  SELECT COUNT(*) FILTER (WHERE result_rows.success)::integer,
         COUNT(*) FILTER (WHERE NOT result_rows.success)::integer
    INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_authorise_bulk_results AS result_rows;

  SELECT jsonb_build_object(
    'ok', true,
    'batch_completed', true,
    'all_success', v_failure_count = 0,
    'action', 'AUTHORISE',
    'requested_count', v_requested_count,
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows), '[]'::jsonb),
    'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
    'failed_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.success = false), '[]'::jsonb),
    'stale_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.result_json ->> 'error_code' = 'ROW_SIGNATURE_MISMATCH'), '[]'::jsonb),
    'count_deltas', jsonb_build_object('processed_eligible', -v_success_count, 'authorised_eligible', v_success_count, 'total', 0),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks'), 'datasets', jsonb_build_array('bulk_authorise'), 'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf), '[]'::jsonb))
  ) INTO v_out;

  RETURN v_out;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'AUTHORISE', 'error_code', 'LOCK_TIMEOUT', 'requested_count', COALESCE(v_requested_count, 0), 'success_count', 0, 'failure_count', COALESCE(v_requested_count, 0), 'results', '[]'::jsonb);
  END IF;
  RAISE;
END;
$function$;




DROP FUNCTION IF EXISTS public.timesheet_unauthorise_bulk_atomic(jsonb, uuid, timestamp with time zone);

CREATE OR REPLACE FUNCTION public.timesheet_unauthorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_items_array jsonb := '[]'::jsonb;
  v_requested_count integer := 0;
  v_success_count integer := 0;
  v_failure_count integer := 0;
  v_uuid_re text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
  v_out jsonb := '{}'::jsonb;
  v_error_state text := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '300ms', true);

  IF p_actor_user_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;
  IF p_items IS NOT NULL AND jsonb_typeof(p_items) NOT IN ('array', 'object') THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'results', '[]'::jsonb);
  END IF;

  v_items_array := CASE
    WHEN p_items IS NULL THEN '[]'::jsonb
    WHEN jsonb_typeof(p_items) = 'array' THEN p_items
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'items') = 'array' THEN p_items -> 'items'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'rows') = 'array' THEN p_items -> 'rows'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selected') = 'array' THEN p_items -> 'selected'
    WHEN jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'selections') = 'array' THEN p_items -> 'selections'
    WHEN jsonb_typeof(p_items) = 'object' THEN jsonb_build_array(p_items)
    ELSE '[]'::jsonb
  END;
  v_requested_count := jsonb_array_length(v_items_array);
  IF v_requested_count > 100 THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'TOO_MANY_ITEMS', 'requested_count', v_requested_count, 'success_count', 0, 'failure_count', v_requested_count, 'results', '[]'::jsonb);
  END IF;

  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_cw;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_results;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN jsonb_typeof(input_values.item_json) = 'object' THEN input_values.item_json ELSE jsonb_build_object('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'row_key', input_values.item_json ->> 'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'timesheet_id', input_values.item_json ->> 'timesheetId', input_values.item_json ->> 'current_timesheet_id', input_values.item_json ->> 'currentTimesheetId', input_values.item_json ->> 'requested_timesheet_id', input_values.item_json ->> 'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'expected_timesheet_id', input_values.item_json ->> 'expectedTimesheetId', input_values.item_json ->> 'expected_current_timesheet_id', input_values.item_json ->> 'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json ->> 'backend_row_signature', input_values.item_json ->> 'row_signature', input_values.item_json ->> 'rowSignature', input_values.item_json ->> 'expected_row_signature', input_values.item_json ->> 'expectedRowSignature', '')), '') AS expected_row_signature
  FROM jsonb_array_elements(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_unauthorise_bulk_state ON COMMIT DROP AS
  SELECT
    item_rows.ordinal,
    item_rows.item_json,
    item_rows.row_key,
    CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END AS requested_timesheet_id,
    CASE WHEN item_rows.expected_timesheet_id_text ~* v_uuid_re THEN item_rows.expected_timesheet_id_text::uuid ELSE NULL::uuid END AS expected_timesheet_id,
    item_rows.expected_row_signature,
    req_ts.timesheet_id AS db_requested_timesheet_id,
    req_ts.booking_id AS requested_booking_id,
    cur_ts.timesheet_id AS current_timesheet_id,
    cur_ts.archived_at_utc AS current_archived_at_utc,
    cur_ts.booking_id AS current_booking_id,
    cur_ts.version AS current_version,
    cur_ts.is_current AS current_is_current,
    cur_ts.authorised_at_server AS current_authorised_at_server,
    cur_ts.sheet_scope AS current_sheet_scope,
    tf.id AS tsfin_id,
    tf.processing_status AS tsfin_processing_status,
    tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    tf.paid_at_utc AS tsfin_paid_at_utc,
    tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    tf.authorised_at_utc AS tsfin_authorised_at_utc,
    cw.id AS contract_week_id,
    sig.signature_json AS signature_json,
    sig.signature_text AS current_row_signature,
    COALESCE(segment_state.has_segment_invoice_lock, false) AS has_segment_invoice_lock
  FROM pg_temp.timesheet_unauthorise_bulk_items AS item_rows
  LEFT JOIN LATERAL (
    SELECT ts_req.*
    FROM public.timesheets AS ts_req
    WHERE ts_req.timesheet_id = CASE WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid ELSE NULL::uuid END
    LIMIT 1
    FOR UPDATE
  ) AS req_ts ON true
  LEFT JOIN LATERAL (
    SELECT ts_cur.*
    FROM public.timesheets AS ts_cur
    WHERE req_ts.booking_id IS NOT NULL
      AND ts_cur.booking_id = req_ts.booking_id
    ORDER BY CASE WHEN ts_cur.is_current THEN 0 ELSE 1 END, ts_cur.version DESC NULLS LAST, ts_cur.updated_at DESC NULLS LAST, ts_cur.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS cur_ts ON true
  LEFT JOIN LATERAL (
    SELECT tf_sel.*
    FROM public.timesheets_financials AS tf_sel
    WHERE tf_sel.timesheet_id = cur_ts.timesheet_id
      AND tf_sel.is_current = true
    ORDER BY tf_sel.computed_at_utc DESC NULLS LAST, tf_sel.updated_at DESC NULLS LAST, tf_sel.created_at DESC NULLS LAST, tf_sel.id DESC
    LIMIT 1
    FOR UPDATE
  ) AS tf ON true
  LEFT JOIN LATERAL (
    SELECT cw_sel.*
    FROM public.contract_weeks AS cw_sel
    WHERE cw_sel.timesheet_id = cur_ts.timesheet_id
       OR EXISTS (SELECT 1 FROM public.timesheets AS cw_ts WHERE cw_ts.timesheet_id = cw_sel.timesheet_id AND cw_ts.booking_id = cur_ts.booking_id)
    ORDER BY CASE WHEN cw_sel.timesheet_id = cur_ts.timesheet_id THEN 0 ELSE 1 END, cw_sel.updated_at DESC NULLS LAST, cw_sel.id DESC
    LIMIT 1
    FOR UPDATE OF cw_sel
  ) AS cw ON cur_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
  LEFT JOIN LATERAL (
    SELECT public.timesheet_lifecycle_signature_v1(cur_ts.timesheet_id, cw.id, false) AS signature_json
  ) AS sig_raw ON true
  LEFT JOIN LATERAL (
    SELECT sig_raw.signature_json AS signature_json,
           NULLIF(BTRIM(COALESCE(sig_raw.signature_json ->> 'backend_row_signature', sig_raw.signature_json ->> 'row_signature', sig_raw.signature_json ->> 'signature', '')), '') AS signature_text
  ) AS sig ON true
  LEFT JOIN LATERAL (
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
          WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object' AND jsonb_typeof(tf.invoice_breakdown_json -> 'segments') = 'array' THEN tf.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    ) AS has_segment_invoice_lock
  ) AS segment_state ON true;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_work ON COMMIT DROP AS
  SELECT
    state_rows.*,
    CASE
      WHEN state_rows.requested_timesheet_id IS NULL THEN 'TIMESHEET_ID_REQUIRED'
      WHEN state_rows.expected_timesheet_id IS NULL THEN 'EXPECTED_TIMESHEET_ID_REQUIRED'
      WHEN state_rows.db_requested_timesheet_id IS NULL THEN 'TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_timesheet_id IS NULL THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_is_current IS DISTINCT FROM true THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.expected_timesheet_id IS DISTINCT FROM state_rows.current_timesheet_id THEN 'TIMESHEET_MOVED'
      WHEN state_rows.tsfin_id IS NULL THEN 'NO_TSFIN'
      WHEN state_rows.expected_row_signature IS NOT NULL AND COALESCE(state_rows.current_row_signature, '') IS DISTINCT FROM state_rows.expected_row_signature THEN 'ROW_SIGNATURE_MISMATCH'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_id IS NULL THEN 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET'
      WHEN state_rows.current_archived_at_utc IS NOT NULL THEN 'TIMESHEET_ARCHIVED'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.has_segment_invoice_lock THEN 'TIMESHEET_LOCKED_BY_INVOICE'
      WHEN state_rows.current_authorised_at_server IS NULL AND state_rows.tsfin_authorised_at_utc IS NULL THEN 'ALREADY_UNAUTHORISED'
      ELSE NULL::text
    END AS failure_code,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum AS new_processing_status
  FROM pg_temp.timesheet_unauthorise_bulk_state AS state_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
       SET authorised_at_server = NULL,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
     WHERE work_rows.failure_code IS NULL
       AND ts_upd.timesheet_id = work_rows.current_timesheet_id
       AND ts_upd.is_current = true
     RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
       SET processing_status = work_rows.new_processing_status,
           authorised_by_user_id = NULL,
           authorised_at_utc = NULL,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_unauthorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND tf_upd.id = work_rows.tsfin_id
       AND tf_upd.is_current = true
     RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_cw ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.contract_weeks AS cw_upd
       SET timesheet_id = work_rows.current_timesheet_id,
           status = 'SUBMITTED'::public.contract_week_status_enum,
           updated_at = v_now
      FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
      JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
     WHERE work_rows.failure_code IS NULL
       AND cw_upd.id = work_rows.contract_week_id
     RETURNING cw_upd.id, cw_upd.timesheet_id, cw_upd.status, cw_upd.updated_at
  )
  SELECT updated_rows.* FROM updated_rows;

  PERFORM public._audit_insert(
    'timesheet_batch',
    'bulk_unauthorise:' || v_now::text,
    'TIMESHEET_BULK_UNAUTHORISED',
    jsonb_build_object('requested_count', v_requested_count, 'actor_user_id', p_actor_user_id),
    jsonb_build_object(
      'succeeded_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
      'failed_items', COALESCE((SELECT jsonb_agg(jsonb_build_object('item_index', work_rows.ordinal, 'timesheet_id', work_rows.requested_timesheet_id, 'error_code', work_rows.failure_code) ORDER BY work_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows WHERE work_rows.failure_code IS NOT NULL), '[]'::jsonb)
    ),
    'BULK_UNAUTHORISE',
    p_actor_user_id
  );

  CREATE TEMP TABLE timesheet_unauthorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    (work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL) AS success,
    jsonb_build_object(
      'item_index', work_rows.ordinal,
      'success', work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL,
      'action', 'UNAUTHORISE',
      'error_code', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') END,
      'requested_timesheet_id', work_rows.requested_timesheet_id,
      'expected_timesheet_id', work_rows.expected_timesheet_id,
      'expected_row_signature', work_rows.expected_row_signature,
      'current_row_signature', work_rows.current_row_signature,
      'current_timesheet_id', work_rows.current_timesheet_id,
      'current_version', COALESCE(updated_ts.version, work_rows.current_version),
      'processing_status_before', work_rows.tsfin_processing_status::text,
      'processing_status_after', CASE WHEN updated_tf.processing_status IS NULL THEN NULL ELSE updated_tf.processing_status::text END,
      'contract_week_id', work_rows.contract_week_id,
      'affected_rows', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN jsonb_build_array(jsonb_build_object('timesheet_id', work_rows.current_timesheet_id, 'contract_week_id', work_rows.contract_week_id, 'booking_id', work_rows.current_booking_id, 'row_key', 'timesheet:' || work_rows.current_timesheet_id::text)) ELSE '[]'::jsonb END
    ) AS result_json
  FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id;

  SELECT COUNT(*) FILTER (WHERE result_rows.success)::integer,
         COUNT(*) FILTER (WHERE NOT result_rows.success)::integer
    INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows;

  SELECT jsonb_build_object(
    'ok', true,
    'batch_completed', true,
    'all_success', v_failure_count = 0,
    'action', 'UNAUTHORISE',
    'requested_count', v_requested_count,
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows), '[]'::jsonb),
    'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb),
    'failed_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows WHERE result_rows.success = false), '[]'::jsonb),
    'stale_items', COALESCE((SELECT jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinal) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows WHERE result_rows.result_json ->> 'error_code' = 'ROW_SIGNATURE_MISMATCH'), '[]'::jsonb),
    'count_deltas', jsonb_build_object('processed_eligible', v_success_count, 'authorised_eligible', -v_success_count, 'total', 0),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks'), 'datasets', jsonb_build_array('bulk_authorise'), 'affected_timesheet_ids', COALESCE((SELECT jsonb_agg(to_jsonb(updated_tf.timesheet_id::text) ORDER BY updated_tf.timesheet_id::text) FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf), '[]'::jsonb))
  ) INTO v_out;

  RETURN v_out;
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE;
  IF v_error_state = '55P03' THEN
    RETURN jsonb_build_object('ok', false, 'batch_completed', false, 'all_success', false, 'action', 'UNAUTHORISE', 'error_code', 'LOCK_TIMEOUT', 'requested_count', COALESCE(v_requested_count, 0), 'success_count', 0, 'failure_count', COALESCE(v_requested_count, 0), 'results', '[]'::jsonb);
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

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
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
  ));
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

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

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
  classified_rows AS MATERIALIZED (
    SELECT
      lightweight_rows.*,
      CASE
        WHEN lightweight_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || lightweight_rows.timesheet_id::text
        WHEN lightweight_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || lightweight_rows.contract_week_id::text
        ELSE NULL::text
      END AS row_key_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
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
      ) AS locked_calc,
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
      ) AS can_bulk_authorise_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = TRUE
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

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
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
  ));
END;
$function$;


-- CloudTMS Retention Marker / Unprocess handover
-- Disposition: latest candidate amended and returned in full.
-- Authoritative baseline: candidate 23, bulk_timesheet_row_patch_v1.
-- Amendment: set-based sticky-marker projection, marker-aware Unprocess contract,
-- and retention fields in top-level, action_flags and row_patch payloads.

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
    (
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
    ) AS row_json
  FROM payload_rows
  ORDER BY
    COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date) ASC NULLS LAST,
    payload_rows.client_name ASC NULLS LAST,
    payload_rows.candidate_name ASC NULLS LAST,
    payload_rows.row_key_calc ASC;
END;
$function$;




REVOKE ALL ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) TO service_role;


CREATE OR REPLACE FUNCTION public.bulk_timesheet_row_decision_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(row_json jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_q text := NULL;
  v_candidate_id_text text := NULL;
  v_client_id_text text := NULL;
  v_q_like text := NULL;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_row_keys text[] := NULL;
  v_dataset_mode text := NULL;
  v_period_filter text := NULL;
  v_date_from_text text := NULL;
  v_date_to_text text := NULL;
  v_week_ending_text text := NULL;
  v_date_from date := NULL;
  v_date_to date := NULL;
  v_week_ending_date date := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
BEGIN
  v_q := NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), '');
  IF v_q IS NOT NULL THEN
    v_q_like := '%' || REPLACE(REPLACE(REPLACE(v_q, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%';
  END IF;

  v_candidate_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '');
  BEGIN
    IF v_candidate_id_text IS NOT NULL THEN
      v_candidate_id := v_candidate_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_candidate_id := NULL;
  END;

  v_client_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '');
  BEGIN
    IF v_client_id_text IS NOT NULL THEN
      v_client_id := v_client_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_client_id := NULL;
  END;

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', '')), '') IS NOT NULL AND (v_filters->>'timesheet_id') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[(v_filters->>'timesheet_id')::uuid];
  END IF;

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', '')), '') IS NOT NULL AND (v_filters->>'contract_week_id') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[(v_filters->>'contract_week_id')::uuid];
  END IF;

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'row_key' AND NULLIF(BTRIM(COALESCE(v_filters->>'row_key', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(v_filters->>'row_key'), '')];
  END IF;

  v_dataset_mode := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'dataset_mode', v_filters->>'datasetMode', '')), ''));
  IF v_dataset_mode NOT IN ('PROCESS', 'AUTHORISE', 'ROW_CONTEXT') THEN
    v_dataset_mode := NULL;
  END IF;

  v_period_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'period_type', v_filters->>'periodType', v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_period_filter NOT IN ('DAILY', 'WEEKLY') THEN
    v_period_filter := NULL;
  END IF;

  v_date_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), '');
  v_date_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), '');
  v_week_ending_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), '');

  BEGIN
    IF v_date_from_text IS NOT NULL THEN
      v_date_from := v_date_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_from := NULL;
  END;

  BEGIN
    IF v_date_to_text IS NOT NULL THEN
      v_date_to := v_date_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_to := NULL;
  END;

  BEGIN
    IF v_week_ending_text IS NOT NULL THEN
      v_week_ending_date := v_week_ending_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_date := NULL;
  END;

  RETURN QUERY
  WITH base_scope AS (
    SELECT vb0.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_filters) AS vb0
  ),
  base_ids AS (
    SELECT DISTINCT
      base_scope.timesheet_id,
      base_scope.contract_week_id
    FROM base_scope
  ),
  retention_unit_members AS MATERIALIZED (
    SELECT
      base_ids.timesheet_id AS row_timesheet_id,
      base_ids.timesheet_id AS member_timesheet_id
    FROM base_ids
    WHERE base_ids.timesheet_id IS NOT NULL

    UNION

    SELECT
      base_ids.timesheet_id AS row_timesheet_id,
      unit_timesheet.timesheet_id AS member_timesheet_id
    FROM base_ids
    JOIN public.timesheets AS anchor_timesheet
      ON anchor_timesheet.timesheet_id = base_ids.timesheet_id
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
  tf_ranked AS (
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
      tf0.processing_status,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.computed_at_utc,
      tf0.locked_by_invoice_id,
      tf0.paid_at_utc,
      tf0.pay_on_hold,
      tf0.pay_on_hold_reason,
      tf0.processed_by_user_id,
      tf0.processed_at_utc,
      tf0.authorised_by_user_id,
      tf0.authorised_at_utc,
      tf0.invoice_breakdown_json,
      tf0.external_source_rows_json,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_charge_ex_vat,
      tf0.expenses_description,
      tf0.expenses_evidence_r2_key,
      tf0.mileage_units,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.mileage_evidence_r2_key,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat,
      tf0.actual_schedule_json,
      tf0.additional_units_json,
      tf0.updated_at,
      tf0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY tf0.timesheet_id
        ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      ) AS rn
    FROM public.timesheets_financials AS tf0
    WHERE tf0.is_current = TRUE
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_tf
        WHERE bs_tf.timesheet_id = tf0.timesheet_id
      )
  ),
  tf_latest AS (
    SELECT
      tf1.id,
      tf1.timesheet_id,
      tf1.timesheet_version,
      tf1.basis,
      tf1.is_stale,
      tf1.stale_reason,
      tf1.worked_start_iso,
      tf1.worked_end_iso,
      tf1.break_start_iso,
      tf1.break_end_iso,
      tf1.break_minutes,
      tf1.candidate_id,
      tf1.client_id,
      tf1.role,
      tf1.band,
      tf1.pay_method,
      tf1.policy_snapshot_json,
      tf1.processing_status,
      tf1.total_hours,
      tf1.total_pay_ex_vat,
      tf1.total_charge_ex_vat,
      tf1.margin_ex_vat,
      tf1.computed_at_utc,
      tf1.locked_by_invoice_id,
      tf1.paid_at_utc,
      tf1.pay_on_hold,
      tf1.pay_on_hold_reason,
      tf1.processed_by_user_id,
      tf1.processed_at_utc,
      tf1.authorised_by_user_id,
      tf1.authorised_at_utc,
      tf1.invoice_breakdown_json,
      tf1.external_source_rows_json,
      tf1.has_rate_issue,
      tf1.has_pay_channel_issue,
      tf1.hr_crosscheck_status,
      tf1.hr_crosscheck_issues,
      tf1.expenses_pay_ex_vat,
      tf1.expenses_charge_ex_vat,
      tf1.expenses_description,
      tf1.expenses_evidence_r2_key,
      tf1.mileage_units,
      tf1.mileage_pay_rate,
      tf1.mileage_charge_rate,
      tf1.mileage_pay_ex_vat,
      tf1.mileage_charge_ex_vat,
      tf1.mileage_evidence_r2_key,
      tf1.travel_pay_ex_vat,
      tf1.travel_charge_ex_vat,
      tf1.accommodation_pay_ex_vat,
      tf1.accommodation_charge_ex_vat,
      tf1.other_pay_ex_vat,
      tf1.other_charge_ex_vat,
      tf1.actual_schedule_json,
      tf1.additional_units_json,
      tf1.updated_at,
      tf1.created_at
    FROM tf_ranked AS tf1
    WHERE tf1.rn = 1
  ),
  tv_ranked AS (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.status,
      tv0.reason_code,
      tv0.pre_validated,
      tv0.validated_at_utc,
      tv0.updated_at,
      tv0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY tv0.timesheet_id
        ORDER BY tv0.updated_at DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
      ) AS rn
    FROM public.timesheet_validations AS tv0
    WHERE tv0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_tv
        WHERE bs_tv.timesheet_id = tv0.timesheet_id
      )
  ),
  tv_latest AS (
    SELECT
      tv1.id,
      tv1.timesheet_id,
      tv1.status,
      tv1.reason_code,
      tv1.pre_validated,
      tv1.validated_at_utc,
      tv1.updated_at,
      tv1.created_at
    FROM tv_ranked AS tv1
    WHERE tv1.rn = 1
  ),
  te_ranked AS (
    SELECT
      te0.id,
      te0.timesheet_id,
      te0.kind,
      te0.display_name,
      te0.storage_key,
      te0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY te0.timesheet_id
        ORDER BY
          CASE
            WHEN UPPER(COALESCE(te0.kind, '')) = 'TIMESHEET' THEN 0
            ELSE 1
          END ASC,
          te0.created_at ASC NULLS LAST,
          te0.id ASC
      ) AS rn
    FROM public.timesheet_evidence AS te0
    WHERE te0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_te
        WHERE bs_te.timesheet_id = te0.timesheet_id
      )
  ),
  te_primary AS (
    SELECT
      te1.id,
      te1.timesheet_id,
      te1.kind,
      te1.display_name,
      te1.storage_key
    FROM te_ranked AS te1
    WHERE te1.rn = 1
  ),
  te_agg AS (
    SELECT
      te2.timesheet_id,
      COUNT(te2.id)::integer AS attached_evidence_count,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'TIMESHEET'), FALSE) AS has_timesheet_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'MILEAGE'), FALSE) AS has_mileage_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'TRAVEL'), FALSE) AS has_travel_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'ACCOMMODATION'), FALSE) AS has_accommodation_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'OTHER'), FALSE) AS has_other_evidence,
      MAX(te2.created_at) AS evidence_updated_at
    FROM public.timesheet_evidence AS te2
    WHERE te2.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_te2
        WHERE bs_te2.timesheet_id = te2.timesheet_id
      )
    GROUP BY te2.timesheet_id
  ),
  mq_ranked AS (
    SELECT
      mq0.id,
      mq0.r2_key,
      mq0.original_filename,
      mq0.mime_type,
      mq0.uploaded_at_utc,
      mq0.last_rotation_deg,
      mq0.meta_json,
      NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') AS contract_week_id_text,
      CASE
        WHEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')) IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER')
          THEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET'))
        ELSE 'OTHER'
      END AS staged_kind_upper,
      ROW_NUMBER() OVER (
        PARTITION BY NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '')
        ORDER BY mq0.uploaded_at_utc ASC NULLS LAST, mq0.id ASC
      ) AS rn
    FROM public.manual_timesheet_queue AS mq0
    WHERE UPPER(COALESCE(mq0.status, '')) = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_mq
        WHERE bs_mq.contract_week_id::text = NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '')
      )
  ),
  mq_primary AS (
    SELECT
      mq1.id,
      mq1.r2_key,
      mq1.original_filename,
      mq1.mime_type,
      mq1.last_rotation_deg,
      mq1.contract_week_id_text,
      mq1.staged_kind_upper
    FROM mq_ranked AS mq1
    WHERE mq1.rn = 1
  ),
  mq_agg AS (
    SELECT
      mq2.contract_week_id_text,
      COUNT(mq2.id)::integer AS queue_staged_count,
      MAX(mq2.uploaded_at_utc) AS queue_updated_at,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'TIMESHEET'), FALSE) AS has_staged_timesheet_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'MILEAGE'), FALSE) AS has_staged_mileage_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'TRAVEL'), FALSE) AS has_staged_travel_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'ACCOMMODATION'), FALSE) AS has_staged_accommodation_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'OTHER'), FALSE) AS has_staged_other_evidence
    FROM mq_ranked AS mq2
    GROUP BY mq2.contract_week_id_text
  ),
  raw_rows AS (
    SELECT
      vb.timesheet_id AS timesheet_id,
      vb.timesheet_status::text AS timesheet_status,
      vb.week_ending_date AS week_ending_date,
      vb.booking_id AS booking_id,
      vb.occupant_key_norm AS occupant_key_norm,
      vb.hospital_norm AS hospital_norm,
      vb.sheet_scope::text AS sheet_scope,
      vb.submission_mode::text AS submission_mode,
      vb.authorised_at_server AS authorised_at_server,
      COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id) AS candidate_id,
      COALESCE(vb.client_id, tf.client_id, ct.client_id) AS client_id,
      COALESCE(vb.pay_method, tf.pay_method, cand.pay_method) AS pay_method,
      COALESCE(tf.processing_status::text, vb.processing_status::text) AS processing_status,
      COALESCE(tf.basis::text, vb.basis::text) AS basis,
      COALESCE(tf.total_hours, vb.total_hours) AS total_hours,
      COALESCE(tf.total_pay_ex_vat, vb.total_pay_ex_vat) AS total_pay_ex_vat,
      COALESCE(tf.total_charge_ex_vat, vb.total_charge_ex_vat) AS total_charge_ex_vat,
      COALESCE(tf.margin_ex_vat, vb.margin_ex_vat) AS margin_ex_vat,
      COALESCE(tf.paid_at_utc, vb.paid_at_utc) AS paid_at_utc,
      (COALESCE(tf.paid_at_utc, vb.paid_at_utc) IS NOT NULL OR vb.pay_paid_at_utc IS NOT NULL) AS is_paid,
      (UPPER(COALESCE(vb.tools_stage, '')) = 'ARCHIVED') AS is_archived,
      (
        COALESCE(tf.locked_by_invoice_id, vb.locked_by_invoice_id) IS NOT NULL
        OR COALESCE(vb.invoice_segments_locked, 0) > 0
        OR COALESCE(vb.invoice_is_paid, FALSE) = TRUE
      ) AS is_invoice_locked,
      COALESCE(retention_unit.has_retained_financial_history, FALSE) AS has_retained_financial_history,
      COALESCE(tf.pay_on_hold, vb.pay_on_hold, FALSE) AS pay_on_hold,
      vb.ready_to_pay AS ready_to_pay,
      COALESCE(tf.locked_by_invoice_id, vb.locked_by_invoice_id) AS locked_by_invoice_id,
      COALESCE(NULLIF(BTRIM(vb.candidate_name), ''), NULLIF(BTRIM(cand.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(cand.first_name), ''), NULLIF(BTRIM(cand.last_name), ''))), ''), vb.occupant_key_norm) AS candidate_name,
      COALESCE(NULLIF(BTRIM(cand.display_name), ''), NULLIF(BTRIM(vb.candidate_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(cand.first_name), ''), NULLIF(BTRIM(cand.last_name), ''))), ''), vb.occupant_key_norm) AS candidate_display_name,
      cand.first_name AS candidate_first_name,
      cand.last_name AS candidate_surname,
      cand.email AS candidate_email,
      COALESCE(cand.opt_in_email, TRUE) AS candidate_opt_in_email,
      COALESCE(NULLIF(BTRIM(vb.client_name), ''), NULLIF(BTRIM(cli.name), ''), vb.hospital_norm) AS client_name,
      COALESCE(NULLIF(BTRIM(cli.name), ''), NULLIF(BTRIM(vb.client_name), ''), vb.hospital_norm) AS client_display_name,
      vb.nhsp_shift_count AS nhsp_shift_count,
      vb.nhsp_shift_included_count AS nhsp_shift_included_count,
      vb.nhsp_shift_deferred_count AS nhsp_shift_deferred_count,
      COALESCE(tv.status::text, vb.validation_status::text) AS validation_status,
      vb.summary_stage AS summary_stage,
      CASE
        WHEN (
          COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
          OR COALESCE(vb.is_adjustment, FALSE) = TRUE
          OR COALESCE(cw.is_adjustment, FALSE) = TRUE
          OR ts.parent_timesheet_id IS NOT NULL
          OR ts.correction_id IS NOT NULL
          OR ts.correction_kind IS NOT NULL
        )
        AND (
          UPPER(COALESCE(vb.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT')
          OR COALESCE(vb.client_is_nhsp, FALSE) = TRUE
          OR COALESCE(ct.is_nhsp, FALSE) = TRUE
          OR UPPER(COALESCE(tf.basis::text, vb.basis::text, '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
        ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN (
          COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
          OR COALESCE(vb.is_adjustment, FALSE) = TRUE
          OR COALESCE(cw.is_adjustment, FALSE) = TRUE
          OR ts.parent_timesheet_id IS NOT NULL
          OR ts.correction_id IS NOT NULL
          OR ts.correction_kind IS NOT NULL
        )
        AND (
          UPPER(COALESCE(vb.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT')
          OR COALESCE(vb.client_autoprocess_hr, FALSE) = TRUE
          OR COALESCE(ct.autoprocess_hr, FALSE) = TRUE
          OR UPPER(COALESCE(tf.basis::text, vb.basis::text, '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
        ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
        WHEN (
          COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
          OR COALESCE(vb.is_adjustment, FALSE) = TRUE
          OR COALESCE(cw.is_adjustment, FALSE) = TRUE
          OR ts.parent_timesheet_id IS NOT NULL
          OR ts.correction_id IS NOT NULL
          OR ts.correction_kind IS NOT NULL
        ) THEN 'WEEKLY_MANUAL_ADJUSTMENT'
        ELSE vb.route_type
      END AS route_type,
      vb.contract_week_id AS contract_week_id,
      vb.contract_week_ending_date AS contract_week_ending_date,
      COALESCE(vb.contract_week_status::text, cw.status::text) AS contract_week_status,
      COALESCE(vb.additional_seq, cw.additional_seq, 0) AS additional_seq,
      (
        COALESCE(ts.is_adjustment, FALSE) = TRUE
        OR COALESCE(vb.is_adjustment, FALSE) = TRUE
        OR COALESCE(cw.is_adjustment, FALSE) = TRUE
        OR COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
      ) AS is_adjustment,
      vb.qr_status::text AS vb_qr_status,
      ts.qr_status::text AS ts_qr_status,
      ts.qr_token AS qr_token,
      ts.qr_generated_at AS qr_generated_at,
      ts.qr_scanned_at AS qr_scanned_at,
      ts.qr_last_sent_hash AS qr_last_sent_hash,
      ts.qr_signed_hash AS qr_signed_hash,
      ts.qr_signed_at_utc AS qr_signed_at_utc,
      vb.qr_token AS vb_qr_token,
      vb.qr_generated_at AS vb_qr_generated_at,
      vb.qr_scanned_at AS vb_qr_scanned_at,
      COALESCE(vb.pay_adjustment_count, 0) AS pay_adjustment_count,
      COALESCE(vb.has_pay_adjustments, FALSE) AS has_pay_adjustments,
      COALESCE(vb.client_autoprocess_hr, FALSE) AS client_autoprocess_hr,
      COALESCE(vb.client_requires_hr, FALSE) AS client_requires_hr,
      COALESCE(vb.client_no_timesheet_required, FALSE) AS client_no_timesheet_required,
      COALESCE(vb.client_pay_reference_required, FALSE) AS client_pay_reference_required,
      COALESCE(vb.client_invoice_reference_required, FALSE) AS client_invoice_reference_required,
      COALESCE(vb.client_hr_validation_required, FALSE) AS client_hr_validation_required,
      COALESCE(vb.client_ts_reference_required, FALSE) AS client_ts_reference_required,
      COALESCE(vb.client_is_nhsp, FALSE) AS client_is_nhsp,
      COALESCE(vb.hr_validation_required_for_invoice, FALSE) AS hr_validation_required_for_invoice,
      COALESCE(tf.has_rate_issue, vb.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(tf.has_pay_channel_issue, vb.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
      COALESCE(tf.hr_crosscheck_status, vb.hr_crosscheck_status) AS hr_crosscheck_status,
      COALESCE(tf.hr_crosscheck_issues, vb.hr_crosscheck_issues) AS hr_crosscheck_issues,
      COALESCE(tf.external_source_rows_json, vb.external_source_rows_json) AS external_source_rows_json,
      COALESCE(vb.issue_codes, ARRAY[]::text[]) AS issue_codes,
      tf.worked_start_iso AS worked_start_iso,
      COALESCE(ts.contract_id, cw.contract_id, ct.id) AS contract_id,
      ct.role AS contract_role,
      ct.band AS contract_band,
      ct.display_site AS contract_display_site,
      ct.ward_hint AS contract_ward_hint,
      ct.std_schedule_json AS contract_std_schedule_json,
      ct.additional_rates_json AS contract_additional_rates_json,
      ct.mileage_pay_rate AS contract_mileage_pay_rate,
      ct.mileage_charge_rate AS contract_mileage_charge_rate,
      ct.default_submission_mode::text AS contract_default_submission_mode,
      ct.weekly_timesheet_source AS contract_weekly_timesheet_source,
      COALESCE(ct.is_nhsp, FALSE) AS contract_is_nhsp,
      COALESCE(ct.autoprocess_hr, FALSE) AS contract_autoprocess_hr,
      COALESCE(ct.requires_hr, FALSE) AS contract_requires_hr,
      COALESCE(ct.no_timesheet_required, FALSE) AS contract_no_timesheet_required,
      COALESCE(ct.daily_calc_of_invoices, FALSE) AS contract_daily_calc_of_invoices,
      COALESCE(ct.group_nightsat_sunbh, FALSE) AS contract_group_nightsat_sunbh,
      COALESCE(ct.is_ad_hoc, FALSE) AS contract_is_ad_hoc,
      vb.candidate_hint_text AS candidate_hint_text,
      COALESCE(tf.expenses_pay_ex_vat, vb.expenses_pay_ex_vat) AS expenses_pay_ex_vat,
      COALESCE(tf.expenses_description, vb.expenses_description) AS expenses_description,
      COALESCE(tf.mileage_units, vb.mileage_units) AS mileage_units,
      COALESCE(tf.mileage_pay_rate, vb.mileage_pay_rate) AS mileage_pay_rate,
      COALESCE(tf.mileage_charge_rate, vb.mileage_charge_rate) AS mileage_charge_rate,
      COALESCE(tf.mileage_pay_ex_vat, vb.mileage_pay_ex_vat) AS mileage_pay_ex_vat,
      COALESCE(tf.travel_pay_ex_vat, vb.travel_pay_ex_vat) AS travel_pay_ex_vat,
      COALESCE(tf.travel_charge_ex_vat, vb.travel_charge_ex_vat) AS travel_charge_ex_vat,
      COALESCE(tf.accommodation_pay_ex_vat, vb.accommodation_pay_ex_vat) AS accommodation_pay_ex_vat,
      COALESCE(tf.accommodation_charge_ex_vat, vb.accommodation_charge_ex_vat) AS accommodation_charge_ex_vat,
      COALESCE(tf.other_pay_ex_vat, vb.other_pay_ex_vat) AS other_pay_ex_vat,
      COALESCE(tf.other_charge_ex_vat, vb.other_charge_ex_vat) AS other_charge_ex_vat,
      vb.invoice_segments_total AS invoice_segments_total,
      vb.invoice_segments_locked AS invoice_segments_locked,
      vb.invoice_segments_unlocked AS invoice_segments_unlocked,
      vb.invoice_segment_stage AS invoice_segment_stage,
      vb.tools_stage AS tools_stage,
      vb.processing_status_display AS processing_status_display,
      vb.invoice_is_paid AS invoice_is_paid,
      vb.refs_block_invoicing AS refs_block_invoicing,
      vb.refs_block_issuing_invoices AS refs_block_issuing_invoices,
      vb.refs_block_invoice_and_issuing AS refs_block_invoice_and_issuing,
      COALESCE(vb.pay_icon_code, 'NONE') AS pay_icon_code,
      vb.pay_status_code AS pay_status_code,
      vb.pay_paid_at_utc AS pay_paid_at_utc,
      COALESCE(vb.net_delta_ex_vat, 0::numeric) AS net_delta_ex_vat,
      ts.version AS timesheet_version,
      ts.is_current AS timesheet_is_current,
      ts.created_at AS timesheet_created_at,
      ts.updated_at AS timesheet_updated_at,
      ts.worked_start_iso AS timesheet_worked_start_iso,
      ts.worked_end_iso AS timesheet_worked_end_iso,
      ts.scheduled_start_iso AS timesheet_scheduled_start_iso,
      ts.scheduled_end_iso AS timesheet_scheduled_end_iso,
      ts.break_start_iso AS timesheet_break_start_iso,
      ts.break_end_iso AS timesheet_break_end_iso,
      ts.break_minutes AS timesheet_break_minutes,
      ts.worked_minutes AS timesheet_worked_minutes,
      ts.reference_number AS reference_number,
      ts.r2_nurse_key AS r2_nurse_key,
      ts.r2_auth_key AS r2_auth_key,
      ts.manual_pdf_r2_key AS manual_pdf_r2_key,
      ts.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
      ts.generated_pdf_at_utc AS generated_pdf_at_utc,
      ts.actual_schedule_json AS timesheet_actual_schedule_json,
      tf.actual_schedule_json AS tsfin_actual_schedule_json,
      ts.additional_units_week AS additional_units_week,
      ts.additional_units_per_day AS additional_units_per_day,
      ts.day_references_json AS day_references_json,
      ts.parent_timesheet_id AS parent_timesheet_id,
      ts.adjustment_origin AS adjustment_origin,
      ts.correction_id AS correction_id,
      ts.correction_kind AS correction_kind,
      cw.id AS cw_id,
      cw.status::text AS cw_status,
      cw.submission_mode_snapshot::text AS cw_submission_mode_snapshot,
      cw.timesheet_id AS cw_timesheet_id,
      cw.uploaded_pdf_r2_key AS uploaded_pdf_r2_key,
      cw.day_entries_json AS contract_week_day_entries_json,
      cw.totals_json AS contract_week_totals_json,
      cw.planned_schedule_json AS contract_week_planned_schedule_json,
      cw.updated_at AS contract_week_updated_at,
      cw.created_at AS contract_week_created_at,
      tf.id AS tsfin_id,
      tf.timesheet_version AS tsfin_timesheet_version,
      tf.is_stale AS tsfin_is_stale,
      tf.stale_reason AS tsfin_stale_reason,
      tf.computed_at_utc AS tsfin_computed_at_utc,
      tf.processed_by_user_id AS processed_by_user_id,
      tf.processed_at_utc AS processed_at_utc,
      pu.display_name AS processed_by_display,
      tf.authorised_by_user_id AS authorised_by_user_id,
      tf.authorised_at_utc AS authorised_at_utc,
      au.display_name AS authorised_by_display,
      tf.invoice_breakdown_json AS invoice_breakdown_json,
      tf.policy_snapshot_json AS policy_snapshot_json,
      tf.updated_at AS tsfin_updated_at,
      tv.updated_at AS validation_updated_at,
      tv.pre_validated AS validation_pre_validated,
      tv.reason_code AS validation_reason_code,
      tv.validated_at_utc AS validation_validated_at_utc,
      te.evidence_updated_at AS evidence_updated_at,
      te.attached_evidence_count AS attached_evidence_count,
      COALESCE(te.has_timesheet_evidence, FALSE) AS ev_timesheet,
      COALESCE(te.has_mileage_evidence, FALSE) AS ev_mileage,
      COALESCE(te.has_travel_evidence, FALSE) AS ev_travel,
      COALESCE(te.has_accommodation_evidence, FALSE) AS ev_accommodation,
      COALESCE(te.has_other_evidence, FALSE) AS ev_other,
      tep.id AS primary_evidence_id,
      tep.kind AS primary_evidence_kind,
      tep.display_name AS primary_evidence_display_name,
      tep.storage_key AS primary_evidence_storage_key,
      COALESCE(mqa.queue_staged_count, 0) AS queue_staged_count,
      mqa.queue_updated_at AS queue_updated_at,
      COALESCE(mqa.has_staged_timesheet_evidence, FALSE) AS staged_ev_timesheet,
      COALESCE(mqa.has_staged_mileage_evidence, FALSE) AS staged_ev_mileage,
      COALESCE(mqa.has_staged_travel_evidence, FALSE) AS staged_ev_travel,
      COALESCE(mqa.has_staged_accommodation_evidence, FALSE) AS staged_ev_accommodation,
      COALESCE(mqa.has_staged_other_evidence, FALSE) AS staged_ev_other,
      mqp.id AS primary_queue_id,
      mqp.r2_key AS primary_queue_r2_key,
      mqp.original_filename AS primary_queue_original_filename,
      mqp.mime_type AS primary_queue_mime_type,
      mqp.last_rotation_deg AS primary_queue_rotation_degrees,
      mqp.staged_kind_upper AS primary_queue_staged_kind,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_electronic
        WHERE hist_electronic.booking_id = vb.booking_id
          AND hist_electronic.is_current = FALSE
          AND UPPER(COALESCE(hist_electronic.submission_mode::text, '')) = 'ELECTRONIC'
      ) AS has_historical_electronic,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_qr_pending
        WHERE hist_qr_pending.booking_id = vb.booking_id
          AND hist_qr_pending.is_current = FALSE
          AND UPPER(COALESCE(hist_qr_pending.qr_status::text, '')) = 'PENDING'
          AND hist_qr_pending.qr_generated_at IS NOT NULL
          AND hist_qr_pending.qr_scanned_at IS NULL
          AND NULLIF(BTRIM(COALESCE(hist_qr_pending.qr_token, '')), '') IS NOT NULL
      ) AS has_historical_qr_pending,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_qr_signed
        WHERE hist_qr_signed.booking_id = vb.booking_id
          AND hist_qr_signed.is_current = FALSE
          AND UPPER(COALESCE(hist_qr_signed.qr_status::text, '')) = 'USED'
          AND hist_qr_signed.qr_scanned_at IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(hist_qr_signed.manual_pdf_r2_key, '')), '') IS NOT NULL
      ) AS has_historical_qr_signed,
      EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
            WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array' THEN tf.invoice_breakdown_json->'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment_value)
        WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_value->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) AS has_segment_invoice_lock
    FROM base_scope AS vb
    LEFT JOIN public.timesheets AS ts
      ON ts.timesheet_id = vb.timesheet_id
     AND ts.is_current = TRUE
    LEFT JOIN retention_by_row AS retention_unit
      ON retention_unit.row_timesheet_id = vb.timesheet_id
    LEFT JOIN public.contract_weeks AS cw
      ON cw.id = vb.contract_week_id
    LEFT JOIN public.contracts AS ct
      ON ct.id = COALESCE(ts.contract_id, cw.contract_id)
    LEFT JOIN tf_latest AS tf
      ON tf.timesheet_id = vb.timesheet_id
    LEFT JOIN tv_latest AS tv
      ON tv.timesheet_id = vb.timesheet_id
    LEFT JOIN te_agg AS te
      ON te.timesheet_id = vb.timesheet_id
    LEFT JOIN te_primary AS tep
      ON tep.timesheet_id = vb.timesheet_id
    LEFT JOIN mq_agg AS mqa
      ON mqa.contract_week_id_text = vb.contract_week_id::text
    LEFT JOIN mq_primary AS mqp
      ON mqp.contract_week_id_text = vb.contract_week_id::text
    LEFT JOIN public.candidates AS cand
      ON cand.id = COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id)
    LEFT JOIN public.clients AS cli
      ON cli.id = COALESCE(vb.client_id, tf.client_id, ct.client_id)
    LEFT JOIN public.tms_users AS pu
      ON pu.id = tf.processed_by_user_id
    LEFT JOIN public.tms_users AS au
      ON au.id = tf.authorised_by_user_id
    WHERE (v_candidate_id IS NULL OR COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id) = v_candidate_id)
      AND (v_client_id IS NULL OR COALESCE(vb.client_id, tf.client_id, ct.client_id) = v_client_id)
      AND (v_timesheet_ids IS NULL OR vb.timesheet_id = ANY(v_timesheet_ids))
      AND (v_contract_week_ids IS NULL OR vb.contract_week_id = ANY(v_contract_week_ids))
      AND (
        v_row_keys IS NULL
        OR CASE
             WHEN vb.timesheet_id IS NOT NULL THEN 'timesheet:' || vb.timesheet_id::text
             WHEN vb.contract_week_id IS NOT NULL THEN 'contract_week:' || vb.contract_week_id::text
             ELSE ''
           END = ANY(v_row_keys)
      )
      AND (
        v_q_like IS NULL
        OR CONCAT_WS(
             ' ',
             vb.candidate_name,
             cand.display_name,
             cand.first_name,
             cand.last_name,
             cand.email,
             vb.client_name,
             cli.name,
             vb.booking_id,
             vb.occupant_key_norm,
             vb.hospital_norm,
             ts.ward_norm,
             ct.display_site,
             ct.ward_hint,
             ts.reference_number,
             vb.route_type,
             vb.summary_stage,
             vb.tools_stage,
             vb.processing_status_display,
             vb.validation_status::text,
             vb.timesheet_id::text,
             vb.contract_week_id::text,
             COALESCE(ts.contract_id, cw.contract_id, ct.id)::text,
             vb.issue_codes::text
           ) ILIKE v_q_like ESCAPE E'\\'
      )
  ),
  classified AS (
    SELECT
      rr.*,
      UPPER(COALESCE(rr.route_type, '')) AS route_type_upper,
      UPPER(COALESCE(rr.sheet_scope, '')) AS sheet_scope_upper,
      UPPER(COALESCE(rr.submission_mode, rr.cw_submission_mode_snapshot, '')) AS submission_mode_upper,
      UPPER(COALESCE(rr.vb_qr_status, rr.ts_qr_status, '')) AS qr_status_upper,
      (
        COALESCE(rr.is_adjustment, FALSE)
        OR COALESCE(rr.additional_seq, 0) > 0
        OR rr.parent_timesheet_id IS NOT NULL
        OR rr.correction_id IS NOT NULL
        OR rr.correction_kind IS NOT NULL
      ) AS is_adjustment_or_additional,
      (
        COALESCE(rr.vb_qr_status, rr.ts_qr_status) IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(rr.qr_token, rr.vb_qr_token, '')), '') IS NOT NULL
        OR rr.qr_generated_at IS NOT NULL
        OR rr.vb_qr_generated_at IS NOT NULL
        OR rr.qr_scanned_at IS NOT NULL
        OR rr.vb_qr_scanned_at IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(rr.qr_last_sent_hash, '')), '') IS NOT NULL
        OR COALESCE(rr.vb_qr_status, rr.ts_qr_status) IN ('PENDING', 'USED', 'EXPIRED', 'CANCELLED')
      ) AS is_qr_route,
      CASE
        WHEN UPPER(COALESCE(rr.sheet_scope, '')) IN ('DAILY', 'WEEKLY') THEN UPPER(COALESCE(rr.sheet_scope, ''))
        WHEN UPPER(COALESCE(rr.route_type, '')) LIKE 'DAILY\_%' ESCAPE '\' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type
    FROM raw_rows AS rr
  ),
  routed AS (
    SELECT
      cl.*,
      CASE
        WHEN cl.is_adjustment_or_additional = TRUE
          AND cl.route_type_upper IN ('WEEKLY_NHSP_ADJUSTMENT', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'WEEKLY_MANUAL_ADJUSTMENT')
          AND cl.submission_mode_upper = 'MANUAL' THEN 'MANUAL_NON_QR'
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
          OR (cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE)
        ) THEN 'IMPORT_AUTHORITATIVE'
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family,
      CASE
        WHEN cl.is_adjustment_or_additional = TRUE
          AND cl.route_type_upper IN ('WEEKLY_NHSP_ADJUSTMENT', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'WEEKLY_MANUAL_ADJUSTMENT')
          AND cl.submission_mode_upper = 'MANUAL' THEN 'MANUAL_NON_QR'
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
        ) THEN 'NHSP'
        WHEN cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN cl.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily,
      CASE
        WHEN cl.is_adjustment_or_additional = TRUE
          AND cl.route_type_upper IN ('WEEKLY_NHSP_ADJUSTMENT', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'WEEKLY_MANUAL_ADJUSTMENT')
          AND cl.submission_mode_upper = 'MANUAL' THEN 'MANUAL_NON_QR'
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
          OR (cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE)
        ) THEN NULL::text
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family,
      (
        cl.route_type_upper = 'WEEKLY_HEALTHROSTER'
        AND COALESCE(cl.client_no_timesheet_required, FALSE) <> TRUE
      ) AS compare_block_required
    FROM classified AS cl
  ),
  decisions AS (
    SELECT
      rt.*,
      CASE
        WHEN rt.route_family = 'IMPORT_AUTHORITATIVE' AND rt.route_subfamily = 'HEALTHROSTER_NO_TIMESHEET' THEN 'HR'
        WHEN rt.route_family = 'IMPORT_AUTHORITATIVE' THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification,
      CASE
        WHEN rt.route_family = 'MANUAL_NON_QR' THEN 1
        WHEN rt.route_family = 'QR' THEN 2
        WHEN rt.route_family = 'ELECTRONIC' THEN 3
        ELSE 99
      END AS timesheet_type_sort_key,
      (
        rt.timesheet_id IS NULL
        AND rt.contract_week_id IS NOT NULL
        AND (
          UPPER(COALESCE(rt.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.contract_week_status, '')) IN ('OPEN', 'PLANNED')
        )
      ) AS is_planned_week_unprocessed,
      (
        rt.timesheet_id IS NOT NULL
        AND UPPER(COALESCE(rt.tools_stage, '')) <> 'ARCHIVED'
        AND (
          UPPER(COALESCE(rt.processing_status, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(rt.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.tools_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.processing_status_display, '')) = 'UNPROCESSED'
        )
      ) AS is_real_row_unprocessed,
      (
        COALESCE(rt.is_archived, FALSE) = TRUE
        OR COALESCE(rt.is_invoice_locked, FALSE) = TRUE
        OR COALESCE(rt.has_segment_invoice_lock, FALSE) = TRUE
      ) AS locked,
      (
        rt.authorised_at_server IS NOT NULL
        OR rt.authorised_at_utc IS NOT NULL
      ) AS is_authorised,
      (
        UPPER(COALESCE(rt.processing_status, '')) = 'PENDING_AUTH'
        OR (
          COALESCE(rt.client_requires_hr, FALSE) = TRUE
          AND COALESCE(rt.client_autoprocess_hr, FALSE) = FALSE
          AND UPPER(COALESCE(rt.processing_status, '')) = 'READY_FOR_HR'
        )
      ) AS requires_authorisation,
      (
        UPPER(COALESCE(rt.validation_status, '')) IN ('VALIDATION_OK', 'OVERRIDDEN')
      ) AS hr_validation_satisfied,
      (
        UPPER(COALESCE(rt.qr_status_upper, '')) = 'PENDING'
        AND (
          (
            NULLIF(BTRIM(COALESCE(rt.qr_token, rt.vb_qr_token, '')), '') IS NOT NULL
            AND COALESCE(rt.qr_generated_at, rt.vb_qr_generated_at) IS NOT NULL
          )
          OR NULLIF(BTRIM(COALESCE(rt.qr_last_sent_hash, '')), '') IS NOT NULL
        )
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NULL
      ) AS qr_pending_awaiting_signature,
      (
        UPPER(COALESCE(rt.qr_status_upper, '')) = 'USED'
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NOT NULL
      ) AS qr_signed_returned,
      (
        UPPER(COALESCE(rt.submission_mode, rt.cw_submission_mode_snapshot, '')) = 'MANUAL'
        AND NULLIF(BTRIM(COALESCE(rt.qr_status_upper, '')), '') IS NULL
        AND NULLIF(BTRIM(COALESCE(rt.qr_token, rt.vb_qr_token, '')), '') IS NULL
        AND COALESCE(rt.qr_generated_at, rt.vb_qr_generated_at) IS NULL
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NULL
      ) AS is_manual_only,
      (
        COALESCE(rt.contract_default_submission_mode, '') = 'ELECTRONIC'
        OR UPPER(COALESCE(rt.policy_snapshot_json->>'default_submission_mode', '')) = 'ELECTRONIC'
      ) AS supports_electronic_submission,
      CASE
        WHEN COALESCE(rt.is_adjustment_or_additional, FALSE) = TRUE THEN
          CASE
            WHEN UPPER(COALESCE(rt.adjustment_origin, '')) = 'MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
            WHEN UPPER(COALESCE(rt.adjustment_origin, '')) LIKE 'IMPORT\_%' ESCAPE '\' OR rt.correction_kind IS NOT NULL OR rt.correction_id IS NOT NULL THEN 'NHSP Adjustment'
            ELSE 'Manual Adjustment'
          END
        WHEN rt.route_type_upper = 'DAILY_ELECTRONIC' THEN 'Daily Electronic'
        WHEN rt.route_type_upper = 'DAILY_MANUAL' THEN 'Daily Manual'
        WHEN rt.route_type_upper = 'WEEKLY_ELECTRONIC' THEN 'Weekly Electronic'
        WHEN rt.route_type_upper = 'WEEKLY_MANUAL' THEN 'Weekly Manual'
        WHEN rt.route_type_upper = 'WEEKLY_NHSP' THEN 'Weekly NHSP'
        WHEN rt.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'Weekly NHSP Adjustment'
        WHEN rt.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'Weekly HealthRoster'
        WHEN rt.route_type_upper = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'Weekly HealthRoster Adjustment'
        WHEN rt.route_type_upper = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Weekly Manual Adjustment'
        ELSE 'Unknown'
      END AS route_display,
      CASE
        WHEN rt.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(rt.client_is_nhsp, FALSE) = TRUE OR COALESCE(rt.contract_is_nhsp, FALSE) = TRUE THEN 'NHSP'
        WHEN rt.route_type_upper IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT') THEN 'HEALTHROSTER'
        WHEN rt.period_type = 'WEEKLY' THEN 'NONE'
        ELSE NULL::text
      END AS contract_weekly_mode,
      CASE
        WHEN rt.route_type_upper IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT') AND COALESCE(rt.client_no_timesheet_required, FALSE) = TRUE THEN 'CREATE'
        WHEN rt.route_type_upper IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT') THEN 'VERIFY'
        ELSE ''
      END AS contract_hr_weekly_behaviour
    FROM routed AS rt
  ),
  final_rows_base AS (
    SELECT
      dc.*,
      (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed) AS is_unprocessed,
      (
        dc.is_adjustment_or_additional = TRUE
        AND dc.contract_week_id IS NOT NULL
        AND dc.route_family = 'MANUAL_NON_QR'
        AND COALESCE(dc.locked, FALSE) = FALSE
        AND COALESCE(dc.is_authorised, FALSE) = FALSE
        AND (dc.timesheet_id IS NULL OR dc.is_real_row_unprocessed = TRUE)
      ) AS supports_unprocessed_expense_draft,
      (
        dc.is_adjustment_or_additional = TRUE
        AND COALESCE(dc.total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(dc.timesheet_actual_schedule_json, dc.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(dc.timesheet_actual_schedule_json, dc.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(dc.timesheet_actual_schedule_json, dc.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          dc.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(dc.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(dc.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
      ) AS keep_additional_manual_adjustment_schedule_empty,
      CASE WHEN (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed) THEN 'UNPROCESSED' ELSE 'PROCESSED' END AS bulk_process_bucket,
      (dc.hr_validation_required_for_invoice = TRUE AND dc.hr_validation_satisfied = FALSE) AS hr_validation_awaiting,
      (
        dc.qr_pending_awaiting_signature = TRUE
        OR UPPER(COALESCE(dc.processing_status, '')) = 'AWAITING_MANUAL_SIGNATURE'
        OR 'Awaiting signed QR timesheet' = ANY(dc.issue_codes)
      ) AS qr_unsigned_blocked,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.requires_authorisation = TRUE
        AND dc.is_authorised = FALSE
        AND (
          dc.qr_pending_awaiting_signature = FALSE
          AND UPPER(COALESCE(dc.processing_status, '')) <> 'AWAITING_MANUAL_SIGNATURE'
          AND NOT ('Awaiting signed QR timesheet' = ANY(dc.issue_codes))
        )
        AND (dc.route_family <> 'QR' OR dc.qr_signed_returned = TRUE)
      ) AS can_bulk_authorise,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_authorised = TRUE
        AND (dc.route_family <> 'QR' OR dc.qr_signed_returned = TRUE)
      ) AS can_bulk_unauthorise,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
        AND NOT (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed)
      ) AS unprocess_action_visible,
      (
        (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
        AND NOT (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed)
      )
        AND COALESCE(dc.has_retained_financial_history, FALSE) = FALSE
      ) AS can_unprocess,
      CASE
        WHEN (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
        AND NOT (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed)
      )
         AND COALESCE(dc.has_retained_financial_history, FALSE) = TRUE
        THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'::text
        ELSE NULL::text
      END AS unprocess_block_reason,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
        AND (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed)
      ) AS can_process,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
      ) AS can_save,
      (
        (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR'))
        AND UPPER(COALESCE(dc.tools_stage, '')) <> 'ARCHIVED'
        AND (dc.locked_by_invoice_id IS NOT NULL OR dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0) = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence,
      (
        (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR'))
        AND (
          UPPER(COALESCE(dc.tools_stage, '')) = 'ARCHIVED'
          OR dc.locked_by_invoice_id IS NOT NULL
          OR dc.has_segment_invoice_lock = TRUE
          OR COALESCE(dc.invoice_segments_locked, 0) > 0
        )
      ) AS evidence_document_locked,
      CASE
        WHEN NOT (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR')) THEN 'INVALID_TIMESHEET_CONTEXT'
        WHEN UPPER(COALESCE(dc.tools_stage, '')) = 'ARCHIVED' THEN 'TIMESHEET_ARCHIVED'
        WHEN dc.locked_by_invoice_id IS NOT NULL THEN 'INVOICE_LOCKED'
        WHEN dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0 THEN 'INVOICE_SEGMENT_LOCKED'
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN 'IMPORT_AUTHORITATIVE_ROUTE'
        ELSE NULL::text
      END AS evidence_lock_reason,
      (
        dc.locked = TRUE
        OR dc.is_authorised = TRUE
        OR dc.route_family <> 'MANUAL_NON_QR'
      ) AS review_only,
      (
        dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND (
          (dc.period_type = 'WEEKLY' AND dc.contract_week_id IS NOT NULL)
          OR (dc.period_type = 'DAILY' AND dc.timesheet_id IS NOT NULL)
        )
        AND (
          dc.route_family = 'IMPORT_AUTHORITATIVE'
          OR (dc.route_family = 'MANUAL_NON_QR' AND dc.submission_mode_upper = 'MANUAL' AND dc.is_qr_route = FALSE)
        )
      ) AS can_add_additional_manual,
      (
        dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND (
          (dc.period_type = 'WEEKLY' AND dc.submission_mode_upper = 'ELECTRONIC' AND (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL))
          OR (dc.period_type = 'DAILY' AND dc.submission_mode_upper = 'ELECTRONIC' AND dc.timesheet_id IS NOT NULL)
        )
      ) AS can_switch_to_manual,
      (
        dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.period_type = 'WEEKLY'
        AND dc.timesheet_id IS NULL
        AND dc.contract_week_id IS NOT NULL
        AND dc.submission_mode_upper = 'MANUAL'
      ) AS can_switch_planned_week_to_electronic,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.has_historical_electronic = TRUE
      ) AS can_revert_to_electronic,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.is_manual_only = TRUE
      ) AS can_allow_qr_again,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.is_manual_only = TRUE
        AND dc.supports_electronic_submission = TRUE
      ) AS can_allow_electronic_again,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.is_qr_route = TRUE
        AND NULLIF(BTRIM(COALESCE(dc.qr_status_upper, '')), '') IS NOT NULL
      ) AS can_convert_qr_to_manual_only,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.has_historical_qr_pending = TRUE
      ) AS can_restore_qr_pending,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.has_historical_qr_signed = TRUE
      ) AS can_restore_qr_signed,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN 'IMPORT_SOURCE_ONLY'
        WHEN dc.route_family = 'ELECTRONIC' AND dc.generated_pdf_at_utc IS NULL AND (NULLIF(BTRIM(COALESCE(dc.r2_nurse_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(dc.r2_auth_key, '')), '') IS NOT NULL) THEN 'SIGNATURES_ONLY'
        WHEN dc.timesheet_id IS NOT NULL AND (dc.manual_pdf_r2_key IS NOT NULL OR dc.generated_pdf_at_utc IS NOT NULL) THEN 'TIMESHEET_ARTIFACT'
        WHEN dc.timesheet_id IS NULL AND dc.contract_week_id IS NOT NULL AND (dc.uploaded_pdf_r2_key IS NOT NULL OR dc.primary_queue_r2_key IS NOT NULL) THEN 'TIMESHEET_ARTIFACT'
        ELSE 'NONE'
      END AS primary_left_pane_mode,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN NULL::text
        WHEN dc.route_family = 'QR'
          AND COALESCE(
            NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
            CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END
          ) IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.route_family = 'ELECTRONIC'
          AND COALESCE(
            CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
            NULLIF(BTRIM(dc.manual_pdf_r2_key), '')
          ) IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.route_family = 'MANUAL_NON_QR'
          AND dc.timesheet_id IS NOT NULL
          AND NULLIF(BTRIM(dc.manual_pdf_r2_key), '') IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.timesheet_id IS NOT NULL
          AND NULLIF(BTRIM(dc.primary_evidence_storage_key), '') IS NOT NULL THEN 'EVIDENCE'
        WHEN dc.timesheet_id IS NULL
          AND dc.contract_week_id IS NOT NULL
          AND NULLIF(BTRIM(dc.uploaded_pdf_r2_key), '') IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.timesheet_id IS NULL
          AND dc.contract_week_id IS NOT NULL
          AND NULLIF(BTRIM(dc.primary_queue_r2_key), '') IS NOT NULL THEN 'QUEUE'
        ELSE NULL::text
      END AS primary_artifact_source,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN NULL::text
        WHEN dc.route_family = 'QR' THEN COALESCE(
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.route_family = 'ELECTRONIC' THEN COALESCE(
          CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.route_family = 'MANUAL_NON_QR' AND dc.timesheet_id IS NOT NULL THEN COALESCE(
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.timesheet_id IS NOT NULL THEN NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        WHEN dc.timesheet_id IS NULL AND dc.contract_week_id IS NOT NULL THEN COALESCE(NULLIF(BTRIM(dc.uploaded_pdf_r2_key), ''), NULLIF(BTRIM(dc.primary_queue_r2_key), ''))
        ELSE NULL::text
      END AS primary_artifact_storage_key,
      CASE
        WHEN dc.timesheet_id IS NOT NULL THEN dc.timesheet_id::text
        WHEN dc.contract_week_id IS NOT NULL THEN dc.contract_week_id::text
        ELSE NULL::text
      END AS stable_row_id_text,
      CASE
        WHEN dc.timesheet_id IS NOT NULL THEN 'timesheet:' || dc.timesheet_id::text
        WHEN dc.contract_week_id IS NOT NULL THEN 'contract_week:' || dc.contract_week_id::text
        ELSE ''
      END AS row_key,
      MD5(CONCAT_WS('|',
        COALESCE(dc.timesheet_id::text, ''),
        COALESCE(dc.contract_week_id::text, ''),
        COALESCE(dc.timesheet_version::text, ''),
        COALESCE(dc.timesheet_updated_at::text, ''),
        COALESCE(dc.contract_week_updated_at::text, ''),
        COALESCE(dc.tsfin_updated_at::text, ''),
        COALESCE(dc.validation_updated_at::text, ''),
        COALESCE(dc.evidence_updated_at::text, ''),
        COALESCE(dc.queue_updated_at::text, ''),
        COALESCE(dc.processing_status, ''),
        COALESCE(dc.summary_stage, ''),
        COALESCE(dc.tools_stage, ''),
        COALESCE(dc.authorised_at_server::text, ''),
        COALESCE(dc.authorised_at_utc::text, ''),
        COALESCE(dc.is_authorised::text, ''),
        COALESCE(dc.locked::text, ''),
        COALESCE(dc.locked_by_invoice_id::text, ''),
        COALESCE(dc.has_segment_invoice_lock::text, ''),
        COALESCE(dc.invoice_segments_locked::text, ''),
        COALESCE(dc.invoice_is_paid::text, ''),
        COALESCE(dc.paid_at_utc::text, ''),
        COALESCE(dc.is_paid::text, 'false'),
        COALESCE(dc.is_invoice_locked::text, 'false'),
        COALESCE(dc.is_archived::text, 'false'),
        COALESCE(dc.has_retained_financial_history::text, 'false'),
        COALESCE(dc.route_family, ''),
        COALESCE(dc.route_subfamily, ''),
        COALESCE(dc.submission_mode, ''),
        COALESCE(dc.cw_submission_mode_snapshot, ''),
        COALESCE(dc.qr_status_upper, ''),
        COALESCE(dc.qr_generated_at::text, ''),
        COALESCE(dc.qr_scanned_at::text, ''),
        COALESCE(dc.qr_signed_at_utc::text, ''),
        COALESCE(dc.validation_status, ''),
        COALESCE(dc.validation_pre_validated::text, ''),
        COALESCE(dc.validation_reason_code, ''),
        COALESCE(dc.validation_validated_at_utc::text, ''),
        COALESCE((dc.hr_validation_required_for_invoice = TRUE AND dc.hr_validation_satisfied = FALSE)::text, ''),
        COALESCE(dc.issue_codes::text, ''),
        COALESCE(dc.attached_evidence_count::text, ''),
        COALESCE(dc.ev_timesheet::text, ''),
        COALESCE(dc.ev_mileage::text, ''),
        COALESCE(dc.ev_travel::text, ''),
        COALESCE(dc.ev_accommodation::text, ''),
        COALESCE(dc.ev_other::text, ''),
        COALESCE(dc.primary_evidence_id::text, ''),
        COALESCE(dc.primary_evidence_kind, ''),
        COALESCE(dc.queue_staged_count::text, ''),
        COALESCE(dc.staged_ev_timesheet::text, ''),
        COALESCE(dc.staged_ev_mileage::text, ''),
        COALESCE(dc.staged_ev_travel::text, ''),
        COALESCE(dc.staged_ev_accommodation::text, ''),
        COALESCE(dc.staged_ev_other::text, ''),
        COALESCE(dc.primary_queue_staged_kind, ''),
        COALESCE(dc.primary_evidence_storage_key, ''),
        COALESCE(dc.manual_pdf_r2_key, ''),
        COALESCE(dc.uploaded_pdf_r2_key, ''),
        COALESCE(dc.generated_pdf_at_utc::text, ''),
        COALESCE(dc.primary_queue_r2_key, ''),
        COALESCE(dc.client_requires_hr::text, ''),
        COALESCE(dc.client_no_timesheet_required::text, ''),
        COALESCE(dc.client_autoprocess_hr::text, ''),
        COALESCE(dc.client_is_nhsp::text, ''),
        COALESCE(dc.total_hours::text, ''),
        COALESCE(dc.total_pay_ex_vat::text, ''),
        COALESCE(dc.total_charge_ex_vat::text, ''),
        COALESCE(dc.margin_ex_vat::text, '')
      )) AS row_signature
    FROM decisions AS dc
  ),
  final_rows AS (
    SELECT
      final_rows_base.*,
      COALESCE(lifecycle_signature.signature_text, final_rows_base.row_signature) AS backend_row_signature,
      COALESCE(lifecycle_signature.signature_text, final_rows_base.row_signature) AS mutation_row_signature
    FROM final_rows_base
    LEFT JOIN LATERAL (
      SELECT NULLIF(BTRIM(COALESCE(
        lifecycle_signature_source.signature_json->>'backend_row_signature',
        lifecycle_signature_source.signature_json->>'row_signature',
        lifecycle_signature_source.signature_json->>'signature',
        ''
      )), '') AS signature_text
      FROM (
        SELECT public.timesheet_lifecycle_signature_v1(final_rows_base.timesheet_id, final_rows_base.contract_week_id, false) AS signature_json
      ) AS lifecycle_signature_source
    ) AS lifecycle_signature ON TRUE
  ),
  artifact_rows AS (
    SELECT
      fr.*,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' AND fr.primary_artifact_storage_key IS NOT NULL AND fr.timesheet_id IS NOT NULL THEN 'timesheet:' || fr.timesheet_id::text
        WHEN fr.primary_artifact_source = 'DOCUMENT' AND fr.primary_artifact_storage_key IS NOT NULL AND fr.contract_week_id IS NOT NULL THEN 'contract_week:' || fr.contract_week_id::text
        WHEN fr.primary_artifact_source = 'EVIDENCE' AND fr.primary_evidence_id IS NOT NULL THEN 'evidence:' || fr.primary_evidence_id::text
        WHEN fr.primary_artifact_source = 'QUEUE' AND fr.primary_queue_id IS NOT NULL THEN 'manual_queue:' || fr.primary_queue_id::text
        WHEN fr.primary_artifact_storage_key IS NOT NULL AND fr.timesheet_id IS NOT NULL THEN 'timesheet:' || fr.timesheet_id::text
        WHEN fr.primary_artifact_storage_key IS NOT NULL AND fr.contract_week_id IS NOT NULL THEN 'contract_week:' || fr.contract_week_id::text
        WHEN fr.primary_evidence_id IS NOT NULL THEN 'evidence:' || fr.primary_evidence_id::text
        WHEN fr.primary_queue_id IS NOT NULL THEN 'manual_queue:' || fr.primary_queue_id::text
        ELSE NULL::text
      END AS primary_artifact_id,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'TIMESHEET'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN UPPER(COALESCE(NULLIF(BTRIM(fr.primary_evidence_kind), ''), 'TIMESHEET'))
        WHEN fr.primary_artifact_source = 'QUEUE' THEN UPPER(COALESCE(NULLIF(BTRIM(fr.primary_queue_staged_kind), ''), 'TIMESHEET'))
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'TIMESHEET'
        WHEN fr.primary_evidence_kind IS NOT NULL THEN UPPER(fr.primary_evidence_kind)
        WHEN fr.primary_queue_id IS NOT NULL THEN UPPER(COALESCE(NULLIF(BTRIM(fr.primary_queue_staged_kind), ''), 'TIMESHEET'))
        ELSE NULL::text
      END AS primary_artifact_kind,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'Timesheet PDF'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN COALESCE(NULLIF(BTRIM(fr.primary_evidence_display_name), ''), 'Evidence')
        WHEN fr.primary_artifact_source = 'QUEUE' THEN COALESCE(NULLIF(BTRIM(fr.primary_queue_original_filename), ''), 'Staged evidence')
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'Timesheet PDF'
        WHEN fr.primary_evidence_display_name IS NOT NULL THEN fr.primary_evidence_display_name
        WHEN fr.primary_queue_original_filename IS NOT NULL THEN fr.primary_queue_original_filename
        ELSE NULL::text
      END AS primary_artifact_display_name,
      CASE
        WHEN fr.primary_left_pane_mode = 'IMPORT_SOURCE_ONLY' THEN 'IMPORT_TABLE'
        WHEN fr.primary_left_pane_mode = 'SIGNATURES_ONLY' THEN 'SIGNATURES'
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'PDF'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN 'FILE'
        WHEN fr.primary_artifact_source = 'QUEUE' THEN 'PDF'
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'PDF'
        WHEN fr.primary_evidence_storage_key IS NOT NULL THEN 'FILE'
        ELSE NULL::text
      END AS primary_artifact_preview_mode,
      (
        COALESCE(fr.ev_timesheet, FALSE)
        OR COALESCE(fr.ev_mileage, FALSE)
        OR COALESCE(fr.ev_travel, FALSE)
        OR COALESCE(fr.ev_accommodation, FALSE)
        OR COALESCE(fr.ev_other, FALSE)
        OR COALESCE(fr.staged_ev_timesheet, FALSE)
        OR COALESCE(fr.staged_ev_mileage, FALSE)
        OR COALESCE(fr.staged_ev_travel, FALSE)
        OR COALESCE(fr.staged_ev_accommodation, FALSE)
        OR COALESCE(fr.staged_ev_other, FALSE)
        OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL
      ) AS has_any_evidence,
      JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', (COALESCE(fr.ev_timesheet, FALSE) OR COALESCE(fr.staged_ev_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(fr.ev_timesheet, FALSE) OR COALESCE(fr.staged_ev_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
        JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', (COALESCE(fr.ev_mileage, FALSE) OR COALESCE(fr.staged_ev_mileage, FALSE)), 'has_evidence', (COALESCE(fr.ev_mileage, FALSE) OR COALESCE(fr.staged_ev_mileage, FALSE))),
        JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', (COALESCE(fr.ev_travel, FALSE) OR COALESCE(fr.staged_ev_travel, FALSE)), 'has_evidence', (COALESCE(fr.ev_travel, FALSE) OR COALESCE(fr.staged_ev_travel, FALSE))),
        JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', (COALESCE(fr.ev_accommodation, FALSE) OR COALESCE(fr.staged_ev_accommodation, FALSE)), 'has_evidence', (COALESCE(fr.ev_accommodation, FALSE) OR COALESCE(fr.staged_ev_accommodation, FALSE))),
        JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', (COALESCE(fr.ev_other, FALSE) OR COALESCE(fr.staged_ev_other, FALSE)), 'has_evidence', (COALESCE(fr.ev_other, FALSE) OR COALESCE(fr.staged_ev_other, FALSE)))
      ) AS evidence_badges
    FROM final_rows AS fr
  )
  SELECT
    (
      JSONB_BUILD_OBJECT(
        'id', COALESCE(ar.timesheet_id::text, ar.contract_week_id::text),
        'row_key', ar.row_key,
        'stable_row_id', ar.stable_row_id_text,
        'timesheet_id', ar.timesheet_id,
        'current_timesheet_id', ar.timesheet_id,
        'requested_timesheet_id', ar.timesheet_id,
        'expected_timesheet_id', ar.timesheet_id,
        'contract_week_id', ar.contract_week_id,
        'contract_id', ar.contract_id,
        'booking_id', ar.booking_id,
        'timesheet_version', ar.timesheet_version,
        'row_signature', ar.row_signature,
        'backend_row_signature', ar.backend_row_signature,
        'mutation_row_signature', ar.mutation_row_signature,
        'updated_at', COALESCE(ar.timesheet_updated_at, ar.contract_week_updated_at, ar.tsfin_updated_at),
        'is_current', COALESCE(ar.timesheet_is_current, TRUE),
        'was_stale', FALSE,
        'candidate_id', ar.candidate_id,
        'candidate_name', ar.candidate_name,
        'candidate_display_name', ar.candidate_display_name,
        'candidate_first_name', ar.candidate_first_name,
        'candidate_surname', ar.candidate_surname,
        'occupant_key_norm', ar.occupant_key_norm,
        'client_id', ar.client_id,
        'client_name', ar.client_name,
        'client_display_name', ar.client_display_name,
        'hospital_name', ar.hospital_norm,
        'hospital_norm', ar.hospital_norm,
        'site_name', ar.contract_display_site,
        'ward_name', COALESCE(ar.contract_ward_hint, ar.contract_display_site),
        'ward_norm', COALESCE(ar.contract_ward_hint, ar.contract_display_site),
        'booking_ref', ar.booking_id,
        'external_ref', ar.reference_number
      )
      || JSONB_BUILD_OBJECT(
        'week_ending_date', ar.week_ending_date,
        'contract_week_ending_date', ar.contract_week_ending_date,
        'work_date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE NULL::date END,
        'date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE ar.week_ending_date END,
        'shift_date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE NULL::date END,
        'period_type', ar.period_type,
        'timesheet_type_sort_key', ar.timesheet_type_sort_key,
        'summary_stage', CASE
          WHEN UPPER(COALESCE(ar.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN ar.is_unprocessed THEN 'UNPROCESSED'
          ELSE ar.summary_stage
        END,
        'tools_stage', CASE
          WHEN UPPER(COALESCE(ar.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN ar.is_unprocessed THEN 'UNPROCESSED'
          ELSE ar.tools_stage
        END,
        'processing_status', ar.processing_status,
        'processing_status_display', CASE WHEN ar.is_unprocessed THEN 'Unprocessed' ELSE ar.processing_status_display END,
        'processed_at_utc', ar.processed_at_utc,
        'processed_by_user_id', ar.processed_by_user_id,
        'processed_by_display', ar.processed_by_display,
        'authorised_at_server', ar.authorised_at_server,
        'authorised_at_utc', ar.authorised_at_utc,
        'authorised_by_user_id', ar.authorised_by_user_id,
        'authorised_by_display', ar.authorised_by_display,
        'locked_by_invoice_id', ar.locked_by_invoice_id,
        'paid_at_utc', ar.paid_at_utc,
        'invoice_segments_total', ar.invoice_segments_total,
        'invoice_segments_locked', ar.invoice_segments_locked,
        'invoice_segments_unlocked', ar.invoice_segments_unlocked,
        'invoice_segment_stage', ar.invoice_segment_stage,
        'invoice_is_paid', ar.invoice_is_paid,
        'ready_to_pay', ar.ready_to_pay,
        'pay_icon_code', ar.pay_icon_code,
        'pay_status_code', ar.pay_status_code,
        'pay_paid_at_utc', ar.pay_paid_at_utc,
        'pay_on_hold', ar.pay_on_hold
      )
      || JSONB_BUILD_OBJECT(
        'total_hours', ar.total_hours,
        'total_pay_ex_vat', ar.total_pay_ex_vat,
        'total_charge_ex_vat', ar.total_charge_ex_vat,
        'margin_ex_vat', ar.margin_ex_vat,
        'net_delta_ex_vat', ar.net_delta_ex_vat,
        'route_type', ar.route_type,
        'route_display', ar.route_display,
        'submission_mode', ar.submission_mode,
        'submission_mode_snapshot', ar.cw_submission_mode_snapshot,
        'sheet_scope', ar.sheet_scope,
        'timesheet_scope', ar.sheet_scope,
        'route_family', ar.route_family,
        'route_subfamily', ar.route_subfamily,
        'underlying_channel_family', ar.underlying_channel_family,
        'is_adjustment', ar.is_adjustment_or_additional,
        'additional_seq', ar.additional_seq,
        'actual_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.timesheet_actual_schedule_json, ar.tsfin_actual_schedule_json, '[]'::jsonb) END,
        'planned_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.contract_week_planned_schedule_json, '[]'::jsonb) END,
        'contract_week_totals_json', COALESCE(ar.contract_week_totals_json, '{}'::jsonb),
        'suppress_standard_schedule_fallback', ar.keep_additional_manual_adjustment_schedule_empty,
        'keep_additional_manual_adjustment_schedule_empty', ar.keep_additional_manual_adjustment_schedule_empty,
        '__suppressStandardScheduleFallback', ar.keep_additional_manual_adjustment_schedule_empty,
        '__keepAdditionalManualAdjustmentScheduleEmpty', ar.keep_additional_manual_adjustment_schedule_empty,
        'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
        'compare_block_required', ar.compare_block_required,
        'bulk_process_bucket', ar.bulk_process_bucket,
        'bulk_authorise_classification', ar.bulk_authorise_classification,
        'bulk_authorise_section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
        'has_timesheet', ar.timesheet_id IS NOT NULL,
        'is_contract_week_only', ar.timesheet_id IS NULL AND ar.contract_week_id IS NOT NULL,
        'is_real_timesheet_row', ar.timesheet_id IS NOT NULL,
        'requires_authorisation', ar.requires_authorisation,
        'is_authorised', ar.is_authorised,
        'is_paid', ar.is_paid,
        'is_invoice_locked', ar.is_invoice_locked,
        'is_archived', ar.is_archived
      )
      || JSONB_BUILD_OBJECT(
        'has_retained_financial_history', ar.has_retained_financial_history,
        'locked', ar.locked,
        'review_only', ar.review_only,
        'can_save', ar.can_save,
        'can_process', ar.can_process,
        'can_unprocess', ar.can_unprocess,
        'unprocess_block_reason', ar.unprocess_block_reason,
        'unprocess_action_visible', ar.unprocess_action_visible,
        'can_edit_timesheet_data', ar.can_edit_timesheet_data,
        'can_manage_evidence', ar.can_manage_evidence,
        'can_bulk_authorise', ar.can_bulk_authorise,
        'can_bulk_unauthorise', ar.can_bulk_unauthorise,
        'can_add_additional_manual', ar.can_add_additional_manual,
        'supportsUnprocessedExpenseDraft', ar.supports_unprocessed_expense_draft,
        'supports_unprocessed_expense_draft', ar.supports_unprocessed_expense_draft,
        'expense_storage_target', CASE
          WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_DRAFT'
          WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TSFIN'
          ELSE NULL::text
        END,
        'expense_evidence_storage_target', CASE
          WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_STAGED_EVIDENCE'
          WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TIMESHEET_EVIDENCE'
          ELSE NULL::text
        END
      )
      || JSONB_BUILD_OBJECT(
        'is_qr', ar.is_qr_route,
        'qr_status', NULLIF(ar.qr_status_upper, ''),
        'qr_generated_at', COALESCE(ar.qr_generated_at, ar.vb_qr_generated_at),
        'qr_scanned_at', COALESCE(ar.qr_scanned_at, ar.vb_qr_scanned_at),
        'qr_signed_at_utc', ar.qr_signed_at_utc,
        'qr_unsigned_blocked', ar.qr_unsigned_blocked,
        'qr_signed_returned', ar.qr_signed_returned,
        'can_allow_qr_again', ar.can_allow_qr_again,
        'can_allow_electronic_again', ar.can_allow_electronic_again,
        'can_switch_to_manual', ar.can_switch_to_manual,
        'can_switch_planned_week_to_electronic', ar.can_switch_planned_week_to_electronic,
        'can_revert_to_electronic', ar.can_revert_to_electronic,
        'can_convert_qr_to_manual_only', ar.can_convert_qr_to_manual_only,
        'can_restore_qr_pending', ar.can_restore_qr_pending,
        'can_restore_qr_signed', ar.can_restore_qr_signed,
        'qr_email_can_send_now', (NOT COALESCE(ar.is_archived, FALSE)) AND (ar.can_allow_qr_again OR NULLIF(ar.qr_status_upper, '') = 'PENDING') AND NULLIF(BTRIM(COALESCE(ar.candidate_email, '')), '') IS NOT NULL AND COALESCE(ar.candidate_opt_in_email, TRUE) = TRUE,
        'qr_email_recipient_available', NULLIF(BTRIM(COALESCE(ar.candidate_email, '')), '') IS NOT NULL,
        'client_requires_hr', ar.client_requires_hr,
        'client_autoprocess_hr', ar.client_autoprocess_hr,
        'client_no_timesheet_required', ar.client_no_timesheet_required,
        'client_is_nhsp', ar.client_is_nhsp,
        'hr_validation_required_for_invoice', ar.hr_validation_required_for_invoice,
        'validation_status', ar.validation_status,
        'validation_pre_validated', COALESCE(ar.validation_pre_validated, FALSE),
        'hr_validation_satisfied', ar.hr_validation_satisfied,
        'hr_validation_awaiting', ar.hr_validation_awaiting,
        'issue_codes', ar.issue_codes,
        'has_deviation_marker', (
          COALESCE(ar.has_rate_issue, FALSE)
          OR COALESCE(ar.has_pay_channel_issue, FALSE)
          OR NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL
          OR COALESCE(array_length(ar.hr_crosscheck_issues, 1), 0) > 0
          OR (
            NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
            AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED')
          )
        ),
        'deviation_marker_reason', CASE
          WHEN COALESCE(ar.has_rate_issue, FALSE) THEN 'RATE_ISSUE'
          WHEN COALESCE(ar.has_pay_channel_issue, FALSE) THEN 'PAY_CHANNEL_ISSUE'
          WHEN NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL THEN ar.validation_reason_code
          WHEN NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
           AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED') THEN ar.hr_crosscheck_status
          WHEN COALESCE(array_length(ar.hr_crosscheck_issues, 1), 0) > 0 THEN 'HR_CROSSCHECK_ISSUE'
          ELSE NULL::text
        END,
        'nhsp_highlight_red', (
          (ar.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(ar.client_is_nhsp, FALSE) = TRUE OR COALESCE(ar.contract_is_nhsp, FALSE) = TRUE)
          AND (
            COALESCE(ar.has_rate_issue, FALSE)
            OR COALESCE(ar.has_pay_channel_issue, FALSE)
            OR COALESCE(ar.nhsp_shift_deferred_count, 0) > 0
            OR NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL
            OR (
              NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
              AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED')
            )
          )
        ),
        'nhsp_highlight_reason', CASE
          WHEN NOT (ar.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(ar.client_is_nhsp, FALSE) = TRUE OR COALESCE(ar.contract_is_nhsp, FALSE) = TRUE) THEN NULL::text
          WHEN COALESCE(ar.nhsp_shift_deferred_count, 0) > 0 THEN 'DEFERRED_NHSP_SHIFTS'
          WHEN COALESCE(ar.has_rate_issue, FALSE) THEN 'RATE_ISSUE'
          WHEN COALESCE(ar.has_pay_channel_issue, FALSE) THEN 'PAY_CHANNEL_ISSUE'
          WHEN NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL THEN ar.validation_reason_code
          WHEN NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
           AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED') THEN ar.hr_crosscheck_status
          ELSE NULL::text
        END,
        'nhsp_deviation_pct', CASE
          WHEN COALESCE(ar.nhsp_shift_count, 0) > 0 THEN ROUND((COALESCE(ar.nhsp_shift_deferred_count, 0)::numeric / NULLIF(ar.nhsp_shift_count::numeric, 0)) * 100, 2)
          ELSE NULL::numeric
        END,
        'nhsp_is_ad_hoc', ar.contract_is_ad_hoc
      )
      || JSONB_BUILD_OBJECT(
        'has_any_evidence', ar.has_any_evidence,
        'evidence_badges', ar.evidence_badges,
        'primary_artifact_id', ar.primary_artifact_id,
        'primary_artifact_kind', ar.primary_artifact_kind,
        'primary_artifact_display_name', ar.primary_artifact_display_name,
        'primary_artifact_storage_key', ar.primary_artifact_storage_key,
        'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
        'primary_artifact_source', ar.primary_artifact_source,
        'primary_left_pane_mode', ar.primary_left_pane_mode,
        'manual_pdf_r2_key', ar.manual_pdf_r2_key,
        'uploaded_pdf_r2_key', ar.uploaded_pdf_r2_key,
        'generated_pdf_at_utc', ar.generated_pdf_at_utc,
        'manual_pdf_rotation_degrees', ar.manual_pdf_rotation_degrees,
        'attached_evidence_count', COALESCE(ar.attached_evidence_count, 0),
        'queue_staged_count', COALESCE(ar.queue_staged_count, 0),
        'evidence_document_locked', ar.evidence_document_locked,
        'evidence_lock_reason', ar.evidence_lock_reason,
        'contract_additional_rates_json', ar.contract_additional_rates_json,
        'contract_std_schedule_json', ar.contract_std_schedule_json,
        'contract_mileage_pay_rate', ar.contract_mileage_pay_rate,
        'contract_mileage_charge_rate', ar.contract_mileage_charge_rate,
        'contract_weekly_mode', ar.contract_weekly_mode,
        'contract_hr_weekly_behaviour', ar.contract_hr_weekly_behaviour,
        'contract_role', ar.contract_role,
        'contract_band', ar.contract_band,
        'reference_number', ar.reference_number,
        'has_pay_adjustments', ar.has_pay_adjustments,
        'pay_adjustment_count', ar.pay_adjustment_count
      )
      || JSONB_BUILD_OBJECT(
        'action_flags', JSONB_BUILD_OBJECT(
          'can_restore_qr_pending', ar.can_restore_qr_pending,
          'can_restore_qr_signed', ar.can_restore_qr_signed,
          'can_revert_to_electronic', ar.can_revert_to_electronic,
          'can_allow_qr_again', ar.can_allow_qr_again,
          'can_allow_electronic_again', ar.can_allow_electronic_again,
          'can_switch_to_manual', ar.can_switch_to_manual,
          'can_switch_planned_week_to_electronic', ar.can_switch_planned_week_to_electronic,
          'can_convert_qr_to_manual_only', ar.can_convert_qr_to_manual_only,
          'supports_electronic_submission', ar.supports_electronic_submission,
          'is_manual_only', ar.is_manual_only,
          'is_adjustment', ar.is_adjustment_or_additional,
          'locked_by_invoice', ar.is_invoice_locked,
          'is_invoice_locked', ar.is_invoice_locked,
          'paid', ar.is_paid,
          'is_paid', ar.is_paid,
          'is_archived', ar.is_archived,
          'has_retained_financial_history', ar.has_retained_financial_history,
          'can_unprocess', ar.can_unprocess,
          'unprocess_block_reason', ar.unprocess_block_reason,
          'unprocess_action_visible', ar.unprocess_action_visible,
          'is_unprocessed', ar.is_unprocessed,
          'route_family', ar.route_family,
          'underlying_channel_family', ar.underlying_channel_family,
          'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
          'can_save', ar.can_save,
          'can_process', ar.can_process,
          'can_edit_timesheet_data', ar.can_edit_timesheet_data,
          'can_manage_evidence', ar.can_manage_evidence,
          'supportsUnprocessedExpenseDraft', ar.supports_unprocessed_expense_draft,
          'supports_unprocessed_expense_draft', ar.supports_unprocessed_expense_draft,
          'expense_storage_target', CASE
            WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_DRAFT'
            WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TSFIN'
            ELSE NULL::text
          END,
          'expense_evidence_storage_target', CASE
            WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_STAGED_EVIDENCE'
            WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TIMESHEET_EVIDENCE'
            ELSE NULL::text
          END,
          'compare_block_required', ar.compare_block_required,
          'has_primary_artifact', ar.primary_artifact_storage_key IS NOT NULL,
          'primary_left_pane_mode', ar.primary_left_pane_mode,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'electronic_artifact_exists', ar.route_family = 'ELECTRONIC' AND ar.primary_artifact_storage_key IS NOT NULL,
          'healthroster_compare_required', ar.compare_block_required
        ),
        'bulk_authorise', JSONB_BUILD_OBJECT(
          'classification', ar.bulk_authorise_classification,
          'section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
          'can_authorise', ar.can_bulk_authorise,
          'can_unauthorise', ar.can_bulk_unauthorise,
          'requires_authorisation', ar.requires_authorisation,
          'is_authorised', ar.is_authorised,
          'hr_validation_awaiting', ar.hr_validation_awaiting,
          'qr_unsigned_blocked', ar.qr_unsigned_blocked,
          'qr_signed_returned', ar.qr_signed_returned
        ),
        'artifact_hints', JSONB_BUILD_OBJECT(
          'route_family', ar.route_family,
          'route_subfamily', ar.route_subfamily,
          'underlying_channel_family', ar.underlying_channel_family,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
          'has_any_evidence', ar.has_any_evidence,
          'evidence_badges', ar.evidence_badges
        ),
        'row_patch', JSONB_BUILD_OBJECT(
          'previous_row_key', NULL::text,
          'row_key', ar.row_key,
          'new_row_key', ar.row_key,
          'stable_row_id', ar.stable_row_id_text,
          'timesheet_id', ar.timesheet_id,
          'current_timesheet_id', ar.timesheet_id,
          'expected_timesheet_id', ar.timesheet_id,
          'contract_week_id', ar.contract_week_id,
          'is_adjustment', ar.is_adjustment_or_additional,
          'additional_seq', ar.additional_seq,
          'route_type', ar.route_type,
          'route_family', ar.route_family,
          'route_subfamily', ar.route_subfamily,
          'underlying_channel_family', ar.underlying_channel_family,
          'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
          'actual_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.timesheet_actual_schedule_json, ar.tsfin_actual_schedule_json, '[]'::jsonb) END,
          'planned_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.contract_week_planned_schedule_json, '[]'::jsonb) END,
          'total_hours', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN 0::numeric ELSE ar.total_hours END,
          'suppress_standard_schedule_fallback', ar.keep_additional_manual_adjustment_schedule_empty,
          'keep_additional_manual_adjustment_schedule_empty', ar.keep_additional_manual_adjustment_schedule_empty,
          '__suppressStandardScheduleFallback', ar.keep_additional_manual_adjustment_schedule_empty,
          '__keepAdditionalManualAdjustmentScheduleEmpty', ar.keep_additional_manual_adjustment_schedule_empty,
          'row_signature', ar.row_signature,
          'backend_row_signature', ar.backend_row_signature,
          'mutation_row_signature', ar.mutation_row_signature,
          'previous_row_signature', NULL::text,
          'bulk_process_bucket', ar.bulk_process_bucket,
          'previous_bulk_process_bucket', NULL::text,
          'bulk_authorise_classification', ar.bulk_authorise_classification,
          'bulk_authorise_section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
          'previous_bulk_authorise_section', NULL::text,
          'processing_status', ar.processing_status,
          'summary_stage', CASE
          WHEN UPPER(COALESCE(ar.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN ar.is_unprocessed THEN 'UNPROCESSED'
          ELSE ar.summary_stage
        END,
          'tools_stage', CASE
          WHEN UPPER(COALESCE(ar.tools_stage, '')) = 'ARCHIVED' THEN 'ARCHIVED'
          WHEN ar.is_unprocessed THEN 'UNPROCESSED'
          ELSE ar.tools_stage
        END,
          'is_authorised', ar.is_authorised,
          'is_paid', ar.is_paid,
          'is_invoice_locked', ar.is_invoice_locked,
          'is_archived', ar.is_archived
        )
        || JSONB_BUILD_OBJECT(
          'has_retained_financial_history', ar.has_retained_financial_history,
          'locked', ar.locked,
          'can_process', ar.can_process,
          'can_unprocess', ar.can_unprocess,
          'unprocess_block_reason', ar.unprocess_block_reason,
          'unprocess_action_visible', ar.unprocess_action_visible,
          'can_bulk_authorise', ar.can_bulk_authorise,
          'can_bulk_unauthorise', ar.can_bulk_unauthorise,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'previous_primary_artifact_storage_key', NULL::text,
          'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
          'evidence_badges', ar.evidence_badges,
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(
            'row_keys', JSONB_BUILD_ARRAY(ar.row_key),
            'timesheet_ids', CASE WHEN ar.timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.timesheet_id) END,
            'contract_week_ids', CASE WHEN ar.contract_week_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.contract_week_id) END,
            'storage_keys', CASE WHEN ar.primary_artifact_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.primary_artifact_storage_key) END,
            'row_signature', ar.row_signature,
            'backend_row_signature', ar.backend_row_signature,
            'mutation_row_signature', ar.mutation_row_signature,
            'invalidate_context', TRUE,
            'invalidate_preview', FALSE
          )
        )
      )
    ) AS row_json
  FROM artifact_rows AS ar
  ORDER BY
    ar.week_ending_date ASC NULLS LAST,
    ar.client_name ASC NULLS LAST,
    ar.candidate_name ASC NULLS LAST,
    ar.row_key ASC;
END;
$function$;

CREATE OR REPLACE FUNCTION public.timesheet_lifecycle_signature_v1(p_timesheet_id uuid DEFAULT NULL::uuid, p_contract_week_id uuid DEFAULT NULL::uuid, p_include_payload boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_week public.contract_weeks%ROWTYPE;
  v_tsfin public.timesheets_financials%ROWTYPE;
  v_validation public.timesheet_validations%ROWTYPE;

  v_resolved_booking_id text := NULL;
  v_target_exists boolean := FALSE;
  v_invoice_segments_locked integer := 0;

  v_evidence_payload jsonb := '{}'::jsonb;
  v_staged_payload jsonb := '{}'::jsonb;
  v_validation_payload jsonb := '{}'::jsonb;
  v_timesheet_payload jsonb := '{}'::jsonb;
  v_contract_week_payload jsonb := '{}'::jsonb;
  v_tsfin_payload jsonb := '{}'::jsonb;
  v_identity_payload jsonb := '{}'::jsonb;
  v_signature_payload jsonb := '{}'::jsonb;
  v_signature text := NULL;
BEGIN
  IF COALESCE(p_include_payload, false) IS NOT TRUE THEN
    RETURN public.timesheet_lifecycle_guard_signature_v1(p_timesheet_id, p_contract_week_id, false);
  END IF;
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
    ORDER BY cw.updated_at DESC NULLS LAST, cw.created_at DESC NULLS LAST, cw.id DESC
    LIMIT 1;
  END IF;

  IF v_week.id IS NOT NULL AND v_resolved_booking_id IS NULL AND v_current_ts.booking_id IS NOT NULL THEN
    v_resolved_booking_id := v_current_ts.booking_id;
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

  IF v_current_ts.timesheet_id IS NOT NULL OR v_resolved_booking_id IS NOT NULL THEN
    SELECT tv.*
      INTO v_validation
    FROM public.timesheet_validations AS tv
    WHERE (
        v_current_ts.timesheet_id IS NOT NULL
        AND tv.timesheet_id = v_current_ts.timesheet_id
      )
       OR (
        v_resolved_booking_id IS NOT NULL
        AND tv.booking_id = v_resolved_booking_id
      )
    ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
    LIMIT 1;
  END IF;

  IF v_tsfin.id IS NOT NULL THEN
    SELECT COUNT(*)::integer
      INTO v_invoice_segments_locked
    FROM jsonb_array_elements(
      CASE
        WHEN v_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_tsfin.invoice_breakdown_json) = 'array' THEN v_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL;
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    WITH evidence_rows AS (
      SELECT
        ev.id,
        ev.kind,
        ev.storage_key,
        ev.created_at,
        ev.display_name
      FROM public.timesheet_evidence AS ev
      WHERE ev.timesheet_id = v_current_ts.timesheet_id
    ), evidence_ordered AS (
      SELECT
        er.id,
        er.kind,
        er.storage_key,
        er.created_at,
        er.display_name,
        ROW_NUMBER() OVER (
          PARTITION BY UPPER(COALESCE(NULLIF(BTRIM(er.kind), ''), 'OTHER'))
          ORDER BY er.created_at ASC NULLS LAST, er.id ASC
        ) AS kind_rank
      FROM evidence_rows AS er
    )
    SELECT jsonb_strip_nulls(
      jsonb_build_object(
        'count', COUNT(*)::integer,
        'updated_at', MAX(eo.created_at),
        'timesheet_count', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) = 'TIMESHEET')::integer,
        'mileage_count', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) = 'MILEAGE')::integer,
        'travel_count', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) = 'TRAVEL')::integer,
        'accommodation_count', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) = 'ACCOMMODATION')::integer,
        'other_count', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION'))::integer,
        'primary_timesheet_storage_key', MIN(eo.storage_key) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(eo.kind), ''), 'OTHER')) = 'TIMESHEET' AND eo.kind_rank = 1),
        'primary_storage_key', MIN(eo.storage_key) FILTER (WHERE eo.kind_rank = 1)
      )
    )
      INTO v_evidence_payload
    FROM evidence_ordered AS eo;
  ELSE
    v_evidence_payload := jsonb_build_object(
      'count', 0,
      'timesheet_count', 0,
      'mileage_count', 0,
      'travel_count', 0,
      'accommodation_count', 0,
      'other_count', 0
    );
  END IF;

  IF v_week.id IS NOT NULL THEN
    WITH staged_rows AS (
      SELECT
        mq.id,
        mq.timesheet_id,
        mq.r2_key,
        mq.status,
        mq.uploaded_at_utc,
        mq.last_rotation_deg,
        mq.meta_json,
        UPPER(COALESCE(
          NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
          NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
          NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
          'TIMESHEET'
        )) AS staged_kind,
        NULLIF(regexp_replace(COALESCE(
          NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
          ''
        ), '^/+', ''), '') AS storage_key
      FROM public.manual_timesheet_queue AS mq
      WHERE mq.status = 'STAGED'
        AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
    ), staged_key_rows AS (
      SELECT DISTINCT sr.storage_key
      FROM staged_rows AS sr
      WHERE sr.staged_kind = 'TIMESHEET'
        AND sr.storage_key IS NOT NULL
      ORDER BY sr.storage_key
    )
    SELECT jsonb_strip_nulls(
      jsonb_build_object(
        'contract_week_id', v_week.id,
        'staged_count', COUNT(*)::integer,
        'staged_updated_at', MAX(sr.uploaded_at_utc),
        'active_staged_timesheet_count', COUNT(*) FILTER (WHERE sr.staged_kind = 'TIMESHEET')::integer,
        'active_staged_timesheet_storage_key_count', (SELECT COUNT(*)::integer FROM staged_key_rows),
        'active_staged_timesheet_storage_keys', COALESCE((SELECT jsonb_agg(skr.storage_key ORDER BY skr.storage_key) FROM staged_key_rows AS skr), '[]'::jsonb),
        'has_staged_timesheet', COUNT(*) FILTER (WHERE sr.staged_kind = 'TIMESHEET') > 0,
        'has_staged_mileage', COUNT(*) FILTER (WHERE sr.staged_kind = 'MILEAGE') > 0,
        'has_staged_travel', COUNT(*) FILTER (WHERE sr.staged_kind = 'TRAVEL') > 0,
        'has_staged_accommodation', COUNT(*) FILTER (WHERE sr.staged_kind = 'ACCOMMODATION') > 0,
        'has_staged_other', COUNT(*) FILTER (WHERE sr.staged_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION')) > 0,
        'primary_staged_timesheet_storage_key', MIN(sr.storage_key) FILTER (WHERE sr.staged_kind = 'TIMESHEET'),
        'primary_staged_timesheet_rotation_degrees', MIN(sr.last_rotation_deg) FILTER (WHERE sr.staged_kind = 'TIMESHEET' AND sr.storage_key = (SELECT MIN(skr.storage_key) FROM staged_key_rows AS skr))
      )
    )
      INTO v_staged_payload
    FROM staged_rows AS sr;
  ELSE
    v_staged_payload := jsonb_build_object(
      'staged_count', 0,
      'active_staged_timesheet_count', 0,
      'active_staged_timesheet_storage_key_count', 0,
      'active_staged_timesheet_storage_keys', '[]'::jsonb,
      'has_staged_timesheet', false,
      'has_staged_mileage', false,
      'has_staged_travel', false,
      'has_staged_accommodation', false,
      'has_staged_other', false
    );
  END IF;

  v_identity_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'requested_timesheet_id', p_timesheet_id,
      'requested_contract_week_id', p_contract_week_id,
      'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
      'booking_id', COALESCE(v_current_ts.booking_id, v_resolved_booking_id),
      'target_exists', (v_current_ts.timesheet_id IS NOT NULL OR v_week.id IS NOT NULL)
    )
  );

  v_timesheet_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'booking_id', NULLIF(v_current_ts.booking_id, ''),
      'version', v_current_ts.version,
      'is_current', v_current_ts.is_current,
    'archived_at_utc', v_current_ts.archived_at_utc,
    'archived_by_user_id', v_current_ts.archived_by_user_id,
    'archived_reason_code', v_current_ts.archived_reason_code,
      'status', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.status::text END,
      'sheet_scope', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.sheet_scope::text END,
      'submission_mode', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.submission_mode::text END,
      'line_type', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.line_type::text END,
      'updated_at', v_current_ts.updated_at,
      'authorised_at_server', v_current_ts.authorised_at_server,
      'manual_pdf_r2_key', NULLIF(v_current_ts.manual_pdf_r2_key, ''),
      'manual_pdf_rotation_degrees', v_current_ts.manual_pdf_rotation_degrees,
      'qr_status', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.qr_status::text END,
      'qr_generated_at', v_current_ts.qr_generated_at,
      'qr_scanned_at', v_current_ts.qr_scanned_at,
      'qr_signed_at_utc', v_current_ts.qr_signed_at_utc,
      'generated_pdf_at_utc', v_current_ts.generated_pdf_at_utc,
      'reference_number', NULLIF(v_current_ts.reference_number, ''),
      'is_adjustment', v_current_ts.is_adjustment,
      'parent_timesheet_id', v_current_ts.parent_timesheet_id
    )
  );

  v_contract_week_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
      'contract_id', v_week.contract_id,
      'week_ending_date', v_week.week_ending_date,
      'additional_seq', v_week.additional_seq,
      'status', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.status::text END,
      'submission_mode_snapshot', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.submission_mode_snapshot::text END,
      'timesheet_id', v_week.timesheet_id,
      'uploaded_pdf_r2_key', NULLIF(v_week.uploaded_pdf_r2_key, ''),
      'updated_at', v_week.updated_at,
      'is_adjustment', v_week.is_adjustment,
      'enforce_day_partition', v_week.enforce_day_partition,
      'allowed_days_mask', NULLIF(v_week.allowed_days_mask, ''),
      'split_boundary_date', v_week.split_boundary_date,
      'split_group_key', NULLIF(v_week.split_group_key, '')
    )
  );

  v_tsfin_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'timesheet_financials_id', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE v_tsfin.id END,
      'timesheet_version', v_tsfin.timesheet_version,
      'is_current', v_tsfin.is_current,
      'is_stale', v_tsfin.is_stale,
      'processing_status', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE v_tsfin.processing_status::text END,
      'candidate_assignment', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE v_tsfin.candidate_assignment::text END,
      'basis', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE v_tsfin.basis::text END,
      'updated_at', v_tsfin.updated_at,
      'computed_at_utc', v_tsfin.computed_at_utc,
      'locked_by_invoice_id', v_tsfin.locked_by_invoice_id,
      'locked_at_utc', v_tsfin.locked_at_utc,
      'invoice_segments_locked', v_invoice_segments_locked,
      'paid_at_utc', v_tsfin.paid_at_utc,
      'pay_on_hold', v_tsfin.pay_on_hold,
      'candidate_id', v_tsfin.candidate_id,
      'client_id', v_tsfin.client_id,
      'pay_method', NULLIF(v_tsfin.pay_method, ''),
      'has_rate_issue', v_tsfin.has_rate_issue,
      'has_pay_channel_issue', v_tsfin.has_pay_channel_issue,
      'hr_crosscheck_status', NULLIF(v_tsfin.hr_crosscheck_status, ''),
      'total_hours', v_tsfin.total_hours,
      'total_pay_ex_vat', v_tsfin.total_pay_ex_vat,
      'total_charge_ex_vat', v_tsfin.total_charge_ex_vat,
      'margin_ex_vat', v_tsfin.margin_ex_vat,
      'expenses_pay_ex_vat', v_tsfin.expenses_pay_ex_vat,
      'expenses_charge_ex_vat', v_tsfin.expenses_charge_ex_vat,
      'mileage_units', v_tsfin.mileage_units,
      'mileage_pay_ex_vat', v_tsfin.mileage_pay_ex_vat,
      'mileage_charge_ex_vat', v_tsfin.mileage_charge_ex_vat
    )
  );

  v_validation_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'validation_id', CASE WHEN v_validation.id IS NULL THEN NULL ELSE v_validation.id END,
      'status', CASE WHEN v_validation.id IS NULL THEN NULL ELSE v_validation.status::text END,
      'reason_code', NULLIF(v_validation.reason_code, ''),
      'pre_validated', v_validation.pre_validated,
      'validated_at_utc', v_validation.validated_at_utc,
      'override_confirmed_at_utc', v_validation.override_confirmed_at_utc,
      'updated_at', v_validation.updated_at,
      'hr_request_source', CASE WHEN v_validation.id IS NULL THEN NULL ELSE v_validation.hr_request_source::text END
    )
  );

  v_signature_payload :=
    jsonb_build_object(
      'identity', v_identity_payload,
      'timesheet', v_timesheet_payload,
      'contract_week', v_contract_week_payload
    )
    || jsonb_build_object(
      'tsfin', v_tsfin_payload,
      'validation', v_validation_payload,
      'evidence', COALESCE(v_evidence_payload, '{}'::jsonb),
      'staged_evidence', COALESCE(v_staged_payload, '{}'::jsonb)
    );

  v_signature := md5(v_signature_payload::text);
  v_target_exists := (v_current_ts.timesheet_id IS NOT NULL OR v_week.id IS NOT NULL);

  RETURN jsonb_strip_nulls(
    jsonb_build_object(
      'ok', v_target_exists,
      'signature', v_signature,
      'backend_row_signature', v_signature,
      'row_signature', v_signature,
      'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
      'booking_id', COALESCE(v_current_ts.booking_id, v_resolved_booking_id),
      'target_exists', v_target_exists
    )
    || CASE
      WHEN COALESCE(p_include_payload, false) THEN jsonb_build_object('signature_payload', v_signature_payload)
      ELSE '{}'::jsonb
    END
  );
END;
$function$;


CREATE OR REPLACE FUNCTION public.contract_week_manual_unprocess_atomic(p_week_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_week public.contract_weeks%ROWTYPE;
  v_pointer_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_existing_queue public.manual_timesheet_queue%ROWTYPE;
  v_contract public.contracts%ROWTYPE;

  v_booking_id text := NULL;
  v_all_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_deleted_timesheet_count integer := 0;
  v_deleted_tsfin_count integer := 0;
  v_deleted_evidence_count integer := 0;
  v_deleted_validation_count integer := 0;
  v_deleted_ts_pdf_outbox_count integer := 0;
  v_deleted_tsfin_outbox_count integer := 0;
  v_cleared_snooze_ids uuid[] := ARRAY[]::uuid[];

  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;

  v_paid_timesheet_id uuid := NULL;
  v_invoice_locked_timesheet_id uuid := NULL;
  v_invoice_segment_locked_timesheet_id uuid := NULL;

  v_seen_storage_keys text[] := ARRAY[]::text[];
  v_stage_items jsonb := '[]'::jsonb;
  v_stage_item jsonb := NULL;
  v_stage_item_kind text := NULL;
  v_stage_item_storage_key text := NULL;
  v_stage_item_display_name text := NULL;
  v_stage_item_created_at timestamp with time zone := NULL;
  v_stage_item_created_by uuid := NULL;
  v_stage_item_rotation integer := 0;
  v_stage_item_timesheet_id uuid := NULL;
  v_stage_item_evidence_id uuid := NULL;
  v_timesheet_stage_keys text[] := ARRAY[]::text[];
  v_active_timesheet_keys text[] := ARRAY[]::text[];
  v_active_timesheet_missing_key_id uuid := NULL;
  v_existing_active_key text := NULL;
  v_repaired_same_key_duplicate_count integer := 0;
  v_dematerialised_primary_timesheet_storage_key text := NULL;
  v_staged_count integer := 0;

  v_evidence_record record;
  v_queue_record record;
  v_queue_storage_key text := NULL;
  v_queue_kind text := NULL;
  v_duplicate_queue_ids uuid[] := ARRAY[]::uuid[];
  v_clean_meta jsonb := '{}'::jsonb;
  v_merged_meta jsonb := '{}'::jsonb;

  v_reopen_snapshot text := 'MANUAL';
  v_reopened_totals_json jsonb := '{}'::jsonb;
  v_reopened_day_entries_json jsonb := '{}'::jsonb;
  v_reopened_planned_schedule_json jsonb := NULL;
  v_additional_units_week_json jsonb := '{}'::jsonb;
  v_additional_units_per_day_json jsonb := '{}'::jsonb;
  v_existing_totals_json jsonb := '{}'::jsonb;
  v_expenses_draft_json jsonb := '{}'::jsonb;

  v_signature_after_text text := NULL;
  v_previous_contract_week_status text := NULL;
  v_previous_processing_status text := NULL;
  v_error_constraint text := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_week_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_week_id')::text;
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
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

  PERFORM pg_advisory_xact_lock(hashtext('contract_week_staged_timesheet:' || v_week.id::text));

  IF v_week.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_UNPROCESSED', DETAIL = jsonb_build_object('contract_week_id', v_week.id)::text;
  END IF;

  SELECT ts.*
    INTO v_pointer_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = v_week.timesheet_id
  FOR UPDATE;

  IF v_pointer_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'timesheet_id', v_week.timesheet_id)::text;
  END IF;

  v_booking_id := v_pointer_ts.booking_id;

  SELECT ts.*
    INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.booking_id = v_booking_id
    AND ts.is_current = true
  ORDER BY ts.version DESC, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('booking_id', v_booking_id, 'reason', 'current_timesheet_not_found')::text;
  END IF;

  IF v_current_ts.timesheet_id IS DISTINCT FROM p_expected_timesheet_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH',
      DETAIL = jsonb_build_object(
        'expected_timesheet_id', p_expected_timesheet_id,
        'current_timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', v_week.id
      )::text;
  END IF;

  IF v_week.status = 'INVOICED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id, 'lock_scope', 'contract_week_status')::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL OR v_week.status = 'AUTHORISED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
  END IF;

  SELECT locked_ids.timesheet_ids
    INTO v_all_timesheet_ids
  FROM (
    SELECT COALESCE(array_agg(locked_ts.timesheet_id ORDER BY locked_ts.version ASC, locked_ts.created_at ASC, locked_ts.timesheet_id ASC), ARRAY[]::uuid[]) AS timesheet_ids
    FROM (
      SELECT ts.timesheet_id, ts.version, ts.created_at
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_booking_id
      FOR UPDATE
    ) AS locked_ts
  ) AS locked_ids;

  IF COALESCE(array_length(v_all_timesheet_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('booking_id', v_booking_id, 'reason', 'timesheet_series_not_found')::text;
  END IF;

  IF EXISTS (
    SELECT 1 FROM public.timesheets AS archived_guard
    WHERE archived_guard.timesheet_id = ANY(v_all_timesheet_ids)
      AND archived_guard.archived_at_utc IS NOT NULL
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_ids', to_jsonb(v_all_timesheet_ids), 'contract_week_id', v_week.id)::text;
  END IF;

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'current_timesheet_financials_not_found')::text;
  END IF;

  v_previous_processing_status := v_current_tsfin.processing_status::text;

  SELECT invoice_guard.timesheet_id
    INTO v_invoice_locked_timesheet_id
  FROM public.timesheets_financials AS invoice_guard
  WHERE invoice_guard.timesheet_id = ANY(v_all_timesheet_ids)
    AND invoice_guard.is_current = true
    AND invoice_guard.locked_by_invoice_id IS NOT NULL
  LIMIT 1
  FOR UPDATE;

  IF v_invoice_locked_timesheet_id IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_invoice_locked_timesheet_id, 'contract_week_id', v_week.id)::text;
  END IF;

  SELECT segment_guard.timesheet_id
    INTO v_invoice_segment_locked_timesheet_id
  FROM public.timesheets_financials AS segment_guard
  WHERE segment_guard.timesheet_id = ANY(v_all_timesheet_ids)
    AND segment_guard.is_current = true
    AND EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN segment_guard.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(segment_guard.invoice_breakdown_json) = 'array' THEN segment_guard.invoice_breakdown_json
          WHEN jsonb_typeof(segment_guard.invoice_breakdown_json) = 'object'
           AND jsonb_typeof(segment_guard.invoice_breakdown_json -> 'segments') = 'array' THEN segment_guard.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    )
  LIMIT 1
  FOR UPDATE;

  IF v_invoice_segment_locked_timesheet_id IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_invoice_segment_locked_timesheet_id, 'contract_week_id', v_week.id, 'lock_scope', 'segment')::text;
  END IF;

  IF v_current_tsfin.authorised_at_utc IS NOT NULL OR v_current_tsfin.authorised_by_user_id IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ALREADY_AUTHORISED',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id, 'lock_scope', 'timesheet_financials')::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, v_week.id, false);
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');

  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object(
        'expected_row_signature', v_expected_row_signature,
        'current_row_signature', v_current_row_signature,
        'current_timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', v_week.id
      )::text;
  END IF;

  SELECT c.*
    INTO v_contract
  FROM public.contracts AS c
  WHERE c.id = v_week.contract_id
  LIMIT 1;

  FOR v_evidence_record IN
    SELECT
      ev.id AS evidence_id,
      ev.timesheet_id,
      ev.kind,
      ev.display_name,
      ev.storage_key,
      ev.created_at,
      ev.created_by,
      ts.manual_pdf_r2_key,
      ts.manual_pdf_rotation_degrees
    FROM public.timesheet_evidence AS ev
    JOIN public.timesheets AS ts ON ts.timesheet_id = ev.timesheet_id
    WHERE ev.timesheet_id = ANY(v_all_timesheet_ids)
    ORDER BY ev.created_at ASC NULLS LAST, ev.id ASC
    FOR UPDATE OF ev
  LOOP
    v_stage_item_storage_key := NULLIF(regexp_replace(BTRIM(COALESCE(v_evidence_record.storage_key, '')), '^/+', ''), '');
    IF v_stage_item_storage_key IS NULL OR v_stage_item_storage_key = ANY(v_seen_storage_keys) THEN
      CONTINUE;
    END IF;
    v_seen_storage_keys := array_append(v_seen_storage_keys, v_stage_item_storage_key);

    v_stage_item_kind := UPPER(COALESCE(NULLIF(BTRIM(v_evidence_record.kind), ''), 'OTHER'));
    IF v_stage_item_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') THEN
      v_stage_item_kind := 'OTHER';
    END IF;

    v_stage_item_rotation := 0;
    IF v_stage_item_kind = 'TIMESHEET'
       AND NULLIF(regexp_replace(BTRIM(COALESCE(v_evidence_record.manual_pdf_r2_key, '')), '^/+', ''), '') = v_stage_item_storage_key THEN
      v_stage_item_rotation := COALESCE(v_evidence_record.manual_pdf_rotation_degrees, 0);
    END IF;

    v_stage_items := v_stage_items || jsonb_build_array(
      jsonb_build_object(
        'source', 'TIMESHEET_EVIDENCE',
        'evidence_id', v_evidence_record.evidence_id,
        'timesheet_id', v_evidence_record.timesheet_id,
        'kind', v_stage_item_kind,
        'storage_key', v_stage_item_storage_key,
        'display_name', COALESCE(NULLIF(BTRIM(v_evidence_record.display_name), ''), regexp_replace(v_stage_item_storage_key, '^.*/', '')),
        'created_at', COALESCE(v_evidence_record.created_at, v_now),
        'created_by', v_evidence_record.created_by,
        'rotation_deg', v_stage_item_rotation
      )
    );
  END LOOP;

  v_stage_item_storage_key := NULLIF(regexp_replace(BTRIM(COALESCE(v_current_ts.manual_pdf_r2_key, '')), '^/+', ''), '');
  IF v_stage_item_storage_key IS NOT NULL AND NOT (v_stage_item_storage_key = ANY(v_seen_storage_keys)) THEN
    v_seen_storage_keys := array_append(v_seen_storage_keys, v_stage_item_storage_key);
    v_stage_items := v_stage_items || jsonb_build_array(
      jsonb_build_object(
        'source', 'LEGACY_MANUAL_PDF_POINTER',
        'evidence_id', NULL,
        'timesheet_id', v_current_ts.timesheet_id,
        'kind', 'TIMESHEET',
        'storage_key', v_stage_item_storage_key,
        'display_name', COALESCE(NULLIF(regexp_replace(v_stage_item_storage_key, '^.*/', ''), ''), 'file'),
        'created_at', v_now,
        'created_by', p_actor_user_id,
        'rotation_deg', COALESCE(v_current_ts.manual_pdf_rotation_degrees, 0)
      )
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT item_rows.storage_key ORDER BY item_rows.storage_key), ARRAY[]::text[])
    INTO v_timesheet_stage_keys
  FROM (
    SELECT NULLIF(BTRIM(item_value.value ->> 'storage_key'), '') AS storage_key
    FROM jsonb_array_elements(v_stage_items) AS item_value(value)
    WHERE UPPER(COALESCE(NULLIF(BTRIM(item_value.value ->> 'kind'), ''), 'OTHER')) = 'TIMESHEET'
  ) AS item_rows
  WHERE item_rows.storage_key IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT active_rows.storage_key ORDER BY active_rows.storage_key), ARRAY[]::text[])
    INTO v_active_timesheet_keys
  FROM (
    SELECT NULLIF(regexp_replace(COALESCE(
      NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
      ''
    ), '^/+', ''), '') AS storage_key
    FROM public.manual_timesheet_queue AS mq
    WHERE mq.status = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
      AND UPPER(COALESCE(
        NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
        'TIMESHEET'
      )) = 'TIMESHEET'
    FOR UPDATE OF mq
  ) AS active_rows
  WHERE active_rows.storage_key IS NOT NULL;

  SELECT mq.id
    INTO v_active_timesheet_missing_key_id
  FROM public.manual_timesheet_queue AS mq
  WHERE mq.status = 'STAGED'
    AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
    AND UPPER(COALESCE(
      NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
      NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
      NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
      'TIMESHEET'
    )) = 'TIMESHEET'
    AND NULLIF(regexp_replace(COALESCE(
      NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
      ''
    ), '^/+', ''), '') IS NULL
  LIMIT 1
  FOR UPDATE;

  IF COALESCE(array_length(v_timesheet_stage_keys, 1), 0) > 0 AND v_active_timesheet_missing_key_id IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_TIMESHEET_EVIDENCE',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_active_timesheet_missing_key_id, 'reason', 'missing_storage_key')::text;
  END IF;

  IF COALESCE(array_length(v_timesheet_stage_keys, 1), 0) > 1 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'dematerialised_storage_keys', to_jsonb(v_timesheet_stage_keys), 'timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF COALESCE(array_length(v_active_timesheet_keys, 1), 0) > 1 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'active_storage_keys', to_jsonb(v_active_timesheet_keys))::text;
  END IF;

  IF COALESCE(array_length(v_active_timesheet_keys, 1), 0) = 1 AND COALESCE(array_length(v_timesheet_stage_keys, 1), 0) = 1 THEN
    v_existing_active_key := v_active_timesheet_keys[1];
    IF v_existing_active_key IS DISTINCT FROM v_timesheet_stage_keys[1] THEN
      RAISE EXCEPTION USING
        MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
        DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'existing_storage_key', v_existing_active_key, 'dematerialised_storage_key', v_timesheet_stage_keys[1])::text;
    END IF;
  END IF;

  FOR v_queue_record IN
    SELECT
      mq.id,
      mq.r2_key,
      mq.meta_json,
      mq.uploaded_at_utc
    FROM public.manual_timesheet_queue AS mq
    WHERE mq.status = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
      AND UPPER(COALESCE(
        NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
        'TIMESHEET'
      )) = 'TIMESHEET'
    ORDER BY mq.uploaded_at_utc ASC NULLS LAST, mq.id ASC
    FOR UPDATE
  LOOP
    v_queue_storage_key := NULLIF(regexp_replace(COALESCE(
      NULLIF(BTRIM(COALESCE(v_queue_record.r2_key, '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'r2_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'storage_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'file_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'canonical_key', '')), ''),
      ''
    ), '^/+', ''), '');

    IF v_queue_storage_key IS NOT NULL
       AND v_queue_storage_key = ANY(v_timesheet_stage_keys)
       AND NOT (v_queue_record.id = ANY(v_duplicate_queue_ids)) THEN
      IF v_existing_active_key IS NULL THEN
        v_existing_active_key := v_queue_storage_key;
      ELSE
        v_duplicate_queue_ids := array_append(v_duplicate_queue_ids, v_queue_record.id);
      END IF;
    END IF;
  END LOOP;

  -- Complete the pre-mutation validation before the retention decision so an
  -- invoice, authorisation, Archived, stale, evidence-conflict, or invalid-actor
  -- blocker is never replaced by the financial-history explanation.
  IF p_actor_user_id IS NULL AND EXISTS (
    SELECT 1
    FROM jsonb_array_elements(v_stage_items) AS staged_item(value)
    WHERE NULLIF(BTRIM(COALESCE(staged_item.value ->> 'storage_key', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(staged_item.value ->> 'created_by', '')), '') IS NULL
  ) THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object(
        'field', 'p_actor_user_id',
        'reason', 'actor required to recreate staged evidence provenance'
      )::text;
  END IF;

  -- The sticky retention marker is authoritative for historical financial linkage.
  -- This check is deliberately after all identity locks and stale-row validation, and
  -- before evidence staging or any destructive mutation.
  IF EXISTS (
    SELECT 1
    FROM public.timesheet_financial_retention AS retention
    WHERE retention.timesheet_id = ANY(v_all_timesheet_ids)
  ) THEN
    RAISE EXCEPTION USING
      MESSAGE = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
      DETAIL = jsonb_build_object(
        'message', 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.',
        'contract_week_id', v_week.id,
        'requested_timesheet_id', p_expected_timesheet_id,
        'current_timesheet_id', v_current_ts.timesheet_id,
        'timesheet_ids', to_jsonb(v_all_timesheet_ids),
        'current_row_signature', v_current_row_signature,
        'backend_row_signature', v_current_row_signature,
        'row_signature', v_current_row_signature,
        'has_retained_financial_history', true,
        'can_unprocess', false,
        'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
        'unprocess_action_visible', true,
        'row_patch', jsonb_build_object(
          'timesheet_id', v_current_ts.timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'row_key', 'timesheet:' || v_current_ts.timesheet_id::text,
          'row_signature', v_current_row_signature,
          'backend_row_signature', v_current_row_signature,
          'has_retained_financial_history', true,
          'can_unprocess', false,
          'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
          'unprocess_action_visible', true
        ),
        'action_flags', jsonb_build_object(
          'has_retained_financial_history', true,
          'can_unprocess', false,
          'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
          'unprocess_action_visible', true
        )
      )::text;
  END IF;


  IF COALESCE(array_length(v_duplicate_queue_ids, 1), 0) > 0 THEN
    UPDATE public.manual_timesheet_queue AS mq
       SET status = 'DISCARDED',
           timesheet_id = NULL,
           meta_json = (
             COALESCE(mq.meta_json, '{}'::jsonb)
               - 'deferred_target_timesheet_id'
               - 'materialised_to_timesheet_id'
               - 'materialisation_deferred_to_backend'
               - 'materialisation_deferred_at_utc'
               - 'materialised_storage_key'
               - 'materialised_at_utc'
               - 'deferred_rotation_degrees'
               - 'duplicate_of_queue_item_id'
               - 'duplicate_timesheet_evidence_identity'
               - 'materialisation_noop_reason'
           ) || jsonb_build_object(
             'contract_week_id', v_week.id::text,
             'staged_kind', 'TIMESHEET',
             'duplicate_timesheet_evidence_identity', true,
             'materialisation_noop_reason', 'same_storage_key_duplicate',
             'same_storage_duplicate_deactivated_at_utc', v_now
           )
     WHERE mq.id = ANY(v_duplicate_queue_ids);

    GET DIAGNOSTICS v_repaired_same_key_duplicate_count = ROW_COUNT;
  END IF;

  FOR v_stage_item IN
    SELECT item_value.value
    FROM jsonb_array_elements(v_stage_items) AS item_value(value)
  LOOP
    v_stage_item_kind := UPPER(COALESCE(NULLIF(BTRIM(v_stage_item ->> 'kind'), ''), 'OTHER'));
    IF v_stage_item_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') THEN
      v_stage_item_kind := 'OTHER';
    END IF;
    v_stage_item_storage_key := NULLIF(regexp_replace(BTRIM(COALESCE(v_stage_item ->> 'storage_key', '')), '^/+', ''), '');
    IF v_stage_item_storage_key IS NULL THEN
      CONTINUE;
    END IF;

    v_stage_item_display_name := COALESCE(NULLIF(BTRIM(v_stage_item ->> 'display_name'), ''), regexp_replace(v_stage_item_storage_key, '^.*/', ''), 'file');
    v_stage_item_created_at := COALESCE(NULLIF(v_stage_item ->> 'created_at', '')::timestamp with time zone, v_now);
    v_stage_item_created_by := NULLIF(v_stage_item ->> 'created_by', '')::uuid;
    v_stage_item_rotation := COALESCE(NULLIF(v_stage_item ->> 'rotation_deg', '')::integer, 0);
    IF v_stage_item_rotation NOT IN (0, 90, 180, 270) THEN
      v_stage_item_rotation := 0;
    END IF;
    v_stage_item_timesheet_id := NULLIF(v_stage_item ->> 'timesheet_id', '')::uuid;
    v_stage_item_evidence_id := NULLIF(v_stage_item ->> 'evidence_id', '')::uuid;

    IF COALESCE(v_stage_item_created_by, p_actor_user_id) IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id', 'reason', 'actor required to recreate staged evidence provenance')::text;
    END IF;

    v_existing_queue := NULL;
    SELECT existing_candidate.id,
           existing_candidate.r2_key,
           existing_candidate.original_filename,
           existing_candidate.mime_type,
           existing_candidate.content_hash,
           existing_candidate.uploaded_by_user_id,
           existing_candidate.uploaded_at_utc,
           existing_candidate.status,
           existing_candidate.timesheet_id,
           existing_candidate.last_rotation_deg,
           existing_candidate.meta_json
      INTO v_existing_queue
    FROM (
      SELECT mq.*, CASE
        WHEN mq.status = 'STAGED'
         AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text THEN 1
        ELSE 0
      END AS prefer_active_staged
      FROM public.manual_timesheet_queue AS mq
      WHERE (
          mq.timesheet_id = ANY(v_all_timesheet_ids)
          OR (
            mq.status = 'STAGED'
            AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
          )
        )
        AND NULLIF(regexp_replace(COALESCE(
          NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
          ''
        ), '^/+', ''), '') = v_stage_item_storage_key
      ORDER BY prefer_active_staged DESC, mq.uploaded_at_utc ASC NULLS LAST, mq.id ASC
      LIMIT 1
      FOR UPDATE
    ) AS existing_candidate;

    v_clean_meta := COALESCE(v_existing_queue.meta_json, '{}'::jsonb)
      - 'deferred_target_timesheet_id'
      - 'materialised_to_timesheet_id'
      - 'materialisation_deferred_to_backend'
      - 'materialisation_deferred_at_utc'
      - 'materialised_storage_key'
      - 'materialised_at_utc'
      - 'deferred_rotation_degrees'
      - 'duplicate_of_queue_item_id'
      - 'duplicate_timesheet_evidence_identity'
      - 'materialisation_noop_reason';

    v_merged_meta := v_clean_meta || jsonb_build_object(
      'contract_week_id', v_week.id::text,
      'staged_kind', v_stage_item_kind,
      'dematerialised_from_timesheet_id', CASE WHEN v_stage_item_timesheet_id IS NULL THEN NULL ELSE v_stage_item_timesheet_id::text END,
      'dematerialised_from_booking_id', v_booking_id,
      'dematerialised_at_utc', v_now
    );

    IF v_existing_queue.id IS NOT NULL THEN
      UPDATE public.manual_timesheet_queue AS mq
         SET status = 'STAGED',
             timesheet_id = NULL,
             r2_key = v_stage_item_storage_key,
             original_filename = v_stage_item_display_name,
             uploaded_by_user_id = COALESCE(v_existing_queue.uploaded_by_user_id, v_stage_item_created_by, p_actor_user_id),
             uploaded_at_utc = COALESCE(v_existing_queue.uploaded_at_utc, v_stage_item_created_at, v_now),
             last_rotation_deg = COALESCE(v_stage_item_rotation, v_existing_queue.last_rotation_deg, 0)::smallint,
             meta_json = v_merged_meta
       WHERE mq.id = v_existing_queue.id;
    ELSE
      INSERT INTO public.manual_timesheet_queue (
        r2_key,
        original_filename,
        mime_type,
        content_hash,
        uploaded_by_user_id,
        uploaded_at_utc,
        status,
        timesheet_id,
        last_rotation_deg,
        meta_json
      )
      VALUES (
        v_stage_item_storage_key,
        v_stage_item_display_name,
        NULL,
        'DEMATERIALISED:' || COALESCE(v_stage_item_evidence_id::text, v_stage_item_storage_key),
        COALESCE(v_stage_item_created_by, p_actor_user_id),
        COALESCE(v_stage_item_created_at, v_now),
        'STAGED',
        NULL,
        v_stage_item_rotation::smallint,
        v_merged_meta
      );
    END IF;

    IF v_stage_item_kind = 'TIMESHEET' AND v_dematerialised_primary_timesheet_storage_key IS NULL THEN
      v_dematerialised_primary_timesheet_storage_key := v_stage_item_storage_key;
    END IF;
  END LOOP;

  v_staged_count := jsonb_array_length(v_stage_items);

  DELETE FROM public.timesheet_evidence AS ev
  WHERE ev.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_evidence_count = ROW_COUNT;

  WITH cleared AS (
    UPDATE public.pay_item_snoozes AS ps
       SET cleared_at_utc = v_now,
           cleared_by_user_id = p_actor_user_id,
           updated_at_utc = v_now,
           updated_by_user_id = p_actor_user_id
     WHERE ps.cleared_at_utc IS NULL
       AND ps.source_ref IS NULL
       AND (
         ps.timesheet_id = ANY(v_all_timesheet_ids)
         OR (v_booking_id IS NOT NULL AND ps.booking_id = v_booking_id)
       )
     RETURNING ps.id
  )
  SELECT COALESCE(array_agg(cleared.id ORDER BY cleared.id), ARRAY[]::uuid[])
    INTO v_cleared_snooze_ids
  FROM cleared;

  DELETE FROM public.ts_pdfs_outbox AS tpo
  WHERE tpo.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_ts_pdf_outbox_count = ROW_COUNT;

  DELETE FROM public.ts_financials_outbox AS tfo
  WHERE tfo.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_tsfin_outbox_count = ROW_COUNT;

  DELETE FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_validation_count = ROW_COUNT;

  DELETE FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_tsfin_count = ROW_COUNT;

  DELETE FROM public.timesheets AS ts
  WHERE ts.booking_id = v_booking_id;
  GET DIAGNOSTICS v_deleted_timesheet_count = ROW_COUNT;

  v_reopen_snapshot := UPPER(COALESCE(NULLIF(BTRIM(v_current_ts.submission_mode::text), ''), NULLIF(BTRIM(v_week.submission_mode_snapshot::text), ''), 'MANUAL'));
  IF v_reopen_snapshot <> 'ELECTRONIC' THEN
    v_reopen_snapshot := 'MANUAL';
  END IF;

  IF v_week.totals_json IS NOT NULL AND jsonb_typeof(v_week.totals_json) = 'object' THEN
    v_existing_totals_json := v_week.totals_json;
  ELSE
    v_existing_totals_json := '{}'::jsonb;
  END IF;

  IF v_current_ts.additional_units_week IS NOT NULL AND jsonb_typeof(v_current_ts.additional_units_week) = 'object' THEN
    SELECT COALESCE(jsonb_object_agg(week_units.key, to_jsonb((week_units.value_text)::numeric)), '{}'::jsonb)
      INTO v_additional_units_week_json
    FROM (
      SELECT UPPER(BTRIM(week_entry.key)) AS key,
             BTRIM(week_entry.value #>> '{}') AS value_text
      FROM jsonb_each(v_current_ts.additional_units_week) AS week_entry(key, value)
      WHERE NULLIF(BTRIM(week_entry.key), '') IS NOT NULL
        AND NULLIF(BTRIM(week_entry.value #>> '{}'), '') ~ '^-?[0-9]+([.][0-9]+)?$'
        AND (week_entry.value #>> '{}')::numeric > 0
    ) AS week_units;
  END IF;

  IF v_current_ts.additional_units_per_day IS NOT NULL AND jsonb_typeof(v_current_ts.additional_units_per_day) = 'object' THEN
    SELECT COALESCE(jsonb_object_agg(per_code.code, per_code.day_values), '{}'::jsonb)
      INTO v_additional_units_per_day_json
    FROM (
      SELECT
        UPPER(BTRIM(code_entry.key)) AS code,
        COALESCE(jsonb_object_agg(SUBSTRING(day_entry.key FROM 1 FOR 10), to_jsonb((day_entry.value #>> '{}')::numeric)), '{}'::jsonb) AS day_values
      FROM jsonb_each(v_current_ts.additional_units_per_day) AS code_entry(key, value)
      JOIN LATERAL jsonb_each(CASE WHEN jsonb_typeof(code_entry.value) = 'object' THEN code_entry.value ELSE '{}'::jsonb END) AS day_entry(key, value) ON true
      WHERE NULLIF(BTRIM(code_entry.key), '') IS NOT NULL
        AND SUBSTRING(day_entry.key FROM 1 FOR 10) ~ '^\d{4}-\d{2}-\d{2}$'
        AND NULLIF(BTRIM(day_entry.value #>> '{}'), '') ~ '^-?[0-9]+([.][0-9]+)?$'
        AND (day_entry.value #>> '{}')::numeric > 0
      GROUP BY UPPER(BTRIM(code_entry.key))
    ) AS per_code
    WHERE jsonb_typeof(per_code.day_values) = 'object'
      AND per_code.day_values <> '{}'::jsonb;
  END IF;

  v_expenses_draft_json := jsonb_build_object(
    'mileage_units', round(COALESCE(v_current_tsfin.mileage_units, 0), 2),
    'travel_pay', round(COALESCE(v_current_tsfin.travel_pay_ex_vat, 0), 2),
    'travel_charge', round(COALESCE(v_current_tsfin.travel_charge_ex_vat, 0), 2),
    'accommodation_pay', round(COALESCE(v_current_tsfin.accommodation_pay_ex_vat, 0), 2),
    'accommodation_charge', round(COALESCE(v_current_tsfin.accommodation_charge_ex_vat, 0), 2),
    'other_pay', round(COALESCE(v_current_tsfin.other_pay_ex_vat, 0), 2),
    'other_charge', round(COALESCE(v_current_tsfin.other_charge_ex_vat, 0), 2),
    'note', COALESCE(NULLIF(BTRIM(v_current_tsfin.expenses_description), ''), NULLIF(BTRIM(v_existing_totals_json #>> '{expenses_draft,note}'), ''), NULLIF(BTRIM(v_existing_totals_json #>> '{expenses_draft,notes}'), ''), '')
  );

  v_reopened_totals_json := v_existing_totals_json
    || jsonb_build_object(
      'hours', jsonb_build_object(
        'day', COALESCE(v_current_tsfin.hours_day, 0),
        'night', COALESCE(v_current_tsfin.hours_night, 0),
        'sat', COALESCE(v_current_tsfin.hours_sat, 0),
        'sun', COALESCE(v_current_tsfin.hours_sun, 0),
        'bh', COALESCE(v_current_tsfin.hours_bh, 0)
      ),
      'additional_units_week', COALESCE(v_additional_units_week_json, '{}'::jsonb),
      'additional_units_per_day', COALESCE(v_additional_units_per_day_json, '{}'::jsonb),
      'expenses_draft', v_expenses_draft_json
    );

  IF v_current_ts.day_references_json IS NOT NULL AND jsonb_typeof(v_current_ts.day_references_json) = 'object' THEN
    v_reopened_day_entries_json := v_current_ts.day_references_json;
  ELSE
    v_reopened_day_entries_json := '{}'::jsonb;
  END IF;

  IF v_current_ts.actual_schedule_json IS NOT NULL AND jsonb_typeof(v_current_ts.actual_schedule_json) = 'array' THEN
    v_reopened_planned_schedule_json := v_current_ts.actual_schedule_json;
  ELSE
    v_reopened_planned_schedule_json := NULL;
  END IF;

  UPDATE public.contract_weeks AS cw
     SET timesheet_id = NULL,
         status = 'OPEN'::public.contract_week_status_enum,
         submission_mode_snapshot = v_reopen_snapshot::public.submission_mode_enum,
         uploaded_pdf_r2_key = v_dematerialised_primary_timesheet_storage_key,
         planned_schedule_json = v_reopened_planned_schedule_json,
         totals_json = v_reopened_totals_json,
         day_entries_json = v_reopened_day_entries_json,
         updated_at = v_now
   WHERE cw.id = v_week.id
   RETURNING cw.* INTO v_week;

  v_after_signature_json := public.timesheet_lifecycle_signature_v1(NULL::uuid, v_week.id, false);
  v_signature_after_text := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', '')), '');

  PERFORM public._audit_insert(
    'contract_week',
    v_week.id::text,
    'CONTRACT_WEEK_MANUAL_TIMESHEET_UNPROCESSED',
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_booking_id,
      'previous_contract_week_status', v_previous_contract_week_status,
      'previous_processing_status', v_previous_processing_status,
      'previous_row_signature', v_current_row_signature
    ),
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'timesheet_id', NULL,
      'booking_id', v_booking_id,
      'new_contract_week_status', v_week.status::text,
      'new_row_signature', v_signature_after_text,
      'staged_count', v_staged_count,
      'primary_timesheet_storage_key', v_dematerialised_primary_timesheet_storage_key,
      'deleted_timesheet_count', v_deleted_timesheet_count,
      'deleted_tsfin_count', v_deleted_tsfin_count,
      'cleared_snooze_count', COALESCE(array_length(v_cleared_snooze_ids, 1), 0)
    ),
    'WEEKLY_MANUAL_UNPROCESS',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'operation', 'weekly_unprocess',
    'contract_week_id', v_week.id,
    'previous_timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', NULL,
    'deleted_timesheet_ids', to_jsonb(v_all_timesheet_ids),
    'deleted_timesheet_count', v_deleted_timesheet_count,
    'previous_contract_week_status', v_previous_contract_week_status,
    'new_contract_week_status', v_week.status::text,
    'previous_processing_status', v_previous_processing_status,
    'new_processing_status', 'UNPROCESSED',
    'backend_row_signature', v_signature_after_text,
    'row_signature', v_signature_after_text,
    'affected_rows', jsonb_build_array(jsonb_build_object(
      'contract_week_id', v_week.id,
      'previous_timesheet_id', v_current_ts.timesheet_id,
      'timesheet_id', NULL,
      'booking_id', v_booking_id,
      'row_key', 'contract_week:' || v_week.id::text
    )),
    'staged_evidence_summary', jsonb_build_object(
      'staged_count', v_staged_count,
      'primary_timesheet_storage_key', v_dematerialised_primary_timesheet_storage_key,
      'repaired_same_key_duplicate_count', v_repaired_same_key_duplicate_count
    ),
    'cleanup_summary', jsonb_build_object(
      'deleted_evidence_count', v_deleted_evidence_count,
      'deleted_tsfin_count', v_deleted_tsfin_count,
      'deleted_validation_count', v_deleted_validation_count,
      'deleted_ts_pdf_outbox_count', v_deleted_ts_pdf_outbox_count,
      'deleted_tsfin_outbox_count', v_deleted_tsfin_outbox_count,
      'cleared_snooze_ids', to_jsonb(v_cleared_snooze_ids)
    ),
    'cache_invalidation_hints', jsonb_build_object(
      'changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_evidence', 'manual_timesheet_queue'),
      'contract_week_id', v_week.id,
      'previous_timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_booking_id
    )
  );
EXCEPTION
  WHEN unique_violation THEN
    GET STACKED DIAGNOSTICS v_error_constraint = CONSTRAINT_NAME;
    IF v_error_constraint = 'uq_manual_timesheet_queue_one_active_staged_timesheet_per_contr' THEN
      RAISE EXCEPTION USING
        MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
        DETAIL = jsonb_build_object(
          'contract_week_id', p_week_id,
          'expected_timesheet_id', p_expected_timesheet_id,
          'constraint_name', v_error_constraint,
          'reason', 'active_staged_timesheet_uniqueness_race'
        )::text;
    END IF;
    RAISE;
  WHEN lock_not_available THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('contract_week_id', p_week_id, 'expected_timesheet_id', p_expected_timesheet_id)::text;
END;
$function$;



CREATE OR REPLACE FUNCTION public.tsfin_write_current_snapshot_single_bounded(p_timesheet_id uuid, p_timesheet_version integer DEFAULT NULL::integer, p_snapshot_json jsonb DEFAULT '{}'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  snap jsonb := COALESCE(p_snapshot_json, '{}'::jsonb);
  prev public.timesheets_financials%ROWTYPE;
  inserted_row public.timesheets_financials%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_prev_id uuid := NULL;
  did_prepare boolean := false;

  v_ib jsonb := '{}'::jsonb;
  v_bad integer := 0;

  v_cat_pay numeric := 0;
  v_cat_charge numeric := 0;
  v_fallback_exp_pay numeric := 0;
  v_fallback_exp_charge numeric := 0;
  v_exp_pay numeric := 0;
  v_exp_charge numeric := 0;

  v_add_units_json jsonb := '{}'::jsonb;
  v_add_pay numeric := 0;
  v_add_charge numeric := 0;
  v_mil_pay numeric := 0;
  v_mil_charge numeric := 0;

  v_nonseg_pay numeric := 0;
  v_nonseg_charge numeric := 0;
  v_nonseg_margin numeric := 0;
  v_core_pay numeric := 0;
  v_core_charge numeric := 0;
  v_total_pay numeric := 0;
  v_total_charge numeric := 0;
  v_margin numeric := 0;

  v_add_obj jsonb := '{}'::jsonb;
  v_tot_obj jsonb := '{}'::jsonb;
  v_mode text := NULL;
  v_seg jsonb := NULL;
  v_exclude boolean := false;

  v_policy jsonb := '{}'::jsonb;
  v_apply_to text := 'PAYE_ONLY';
  v_erni_pct_raw numeric := 0;
  v_erni_mult numeric := 1;
  v_pay_method_u text := NULL;
  v_erni_applies boolean := false;
  v_wage_pay numeric := 0;
  v_wage_pay_cost numeric := 0;
  v_reimb_pay numeric := 0;
  v_pay_cost numeric := 0;
  v_nonseg_wage_pay numeric := 0;
  v_nonseg_wage_pay_cost numeric := 0;
  v_nonseg_reimb_pay numeric := 0;
  v_nonseg_pay_cost numeric := 0;

  v_processing_status public.ts_fin_processing_status_enum := 'UNASSIGNED'::public.ts_fin_processing_status_enum;
  v_candidate_assignment public.candidate_assignment_enum := 'UNASSIGNED'::public.candidate_assignment_enum;
  v_basis public.timesheet_fin_basis_enum := 'SELF_REPORTED'::public.timesheet_fin_basis_enum;
  v_timesheet_version integer := 1;
  v_is_stale boolean := false;
  v_stale_reason text := NULL;
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_pay_method_insert text := NULL;
  v_mileage_units numeric := 0;
  v_error_state text := NULL;
  v_error_message text := NULL;

  v_live_contract_week_id uuid := NULL::uuid;
  v_live_contract_week_status text := NULL::text;
  v_incoming_processing_status text := NULL::text;
  v_incoming_authorised_at_utc timestamptz := NULL::timestamptz;
  v_result_authorised_at_utc timestamptz := NULL::timestamptz;
  v_result_authorised_by_user_id uuid := NULL::uuid;
  v_live_authorised boolean := false;
  v_can_preserve_authorised_at boolean := false;
  v_lifecycle_decision text := 'WRITTEN';
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
  END IF;

  IF p_snapshot_json IS NOT NULL AND jsonb_typeof(p_snapshot_json) <> 'object' THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_snapshot_json')::text;
  END IF;

  SELECT ts.*
    INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(snap ->> 'timesheet_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(snap ->> 'timesheet_id', '')), '')::uuid IS DISTINCT FROM p_timesheet_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH',
      DETAIL = jsonb_build_object(
        'field', 'timesheet_id',
        'expected_value', p_timesheet_id::text,
        'supplied_value', snap ->> 'timesheet_id'
      )::text;
  END IF;

  snap := snap || jsonb_build_object(
    'timesheet_id', p_timesheet_id::text,
    'timesheet_version', COALESCE(p_timesheet_version, NULLIF(snap ->> 'timesheet_version', '')::integer, v_current_ts.version, 1)
  );

  SELECT tf.*
    INTO prev
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = p_timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF prev.id IS NOT NULL AND prev.paid_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_PAID', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id, 'timesheet_financials_id', prev.id)::text;
  END IF;

  IF prev.id IS NOT NULL AND prev.locked_by_invoice_id IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id, 'invoice_id', prev.locked_by_invoice_id)::text;
  END IF;

  v_prev_id := prev.id;

  v_timesheet_version := COALESCE(NULLIF(snap ->> 'timesheet_version', '')::integer, v_current_ts.version, 1);
  v_basis := COALESCE(NULLIF(snap ->> 'basis', '')::public.timesheet_fin_basis_enum, 'SELF_REPORTED'::public.timesheet_fin_basis_enum);
  v_candidate_assignment := COALESCE(NULLIF(snap ->> 'candidate_assignment', '')::public.candidate_assignment_enum, 'UNASSIGNED'::public.candidate_assignment_enum);
  v_processing_status := COALESCE(NULLIF(snap ->> 'processing_status', '')::public.ts_fin_processing_status_enum, 'UNASSIGNED'::public.ts_fin_processing_status_enum);

  v_ib := COALESCE(snap -> 'invoice_breakdown_json', '{}'::jsonb);
  IF v_ib IS NULL OR jsonb_typeof(v_ib) <> 'object' THEN
    v_ib := '{}'::jsonb;
  END IF;

  v_bad := public._tsfin_invalid_segment_count(v_ib);
  IF v_bad > 0 THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH', DETAIL = jsonb_build_object('field', 'invoice_breakdown_json', 'invalid_segment_count', v_bad)::text;
  END IF;

  SELECT cw.id,
         cw.status::text
    INTO v_live_contract_week_id,
         v_live_contract_week_status
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = p_timesheet_id
  ORDER BY cw.updated_at DESC NULLS LAST,
           cw.created_at DESC NULLS LAST,
           cw.id DESC
  LIMIT 1
  FOR UPDATE;

  v_incoming_processing_status := CASE
    WHEN v_processing_status IS NULL THEN NULL::text
    ELSE v_processing_status::text
  END;
  v_incoming_authorised_at_utc := NULLIF(snap ->> 'authorised_at_utc', '')::timestamptz;
  v_result_authorised_at_utc := COALESCE(v_incoming_authorised_at_utc, prev.authorised_at_utc);
  v_result_authorised_by_user_id := COALESCE(
    NULLIF(snap ->> 'authorised_by_user_id', '')::uuid,
    prev.authorised_by_user_id
  );
  v_live_authorised := (
       v_current_ts.authorised_at_server IS NOT NULL
    OR UPPER(BTRIM(COALESCE(v_live_contract_week_status, ''))) = 'AUTHORISED'
    OR prev.authorised_at_utc IS NOT NULL
    OR prev.processing_status IN (
         'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
         'READY_FOR_HR'::public.ts_fin_processing_status_enum
       )
  );

  IF v_live_authorised
     AND (
          v_processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum
       OR v_processing_status NOT IN (
            'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
            'READY_FOR_HR'::public.ts_fin_processing_status_enum
          )
       OR v_result_authorised_at_utc IS NULL
     ) THEN
    v_can_preserve_authorised_at := COALESCE(
      prev.authorised_at_utc,
      v_current_ts.authorised_at_server,
      v_incoming_authorised_at_utc
    ) IS NOT NULL;

    IF NOT v_can_preserve_authorised_at THEN
      v_lifecycle_decision := 'REJECTED';
      PERFORM public._temp_diag_log(
        'TSFIN_SNAPSHOT_LIFECYCLE_DOWNGRADE_REJECTED',
        'TEMP_TIMESHEET_LIFECYCLE',
        p_timesheet_id::text,
        jsonb_strip_nulls(
          jsonb_build_object(
            'function_name', 'tsfin_write_current_snapshot_single_bounded',
            'timesheet_id', p_timesheet_id::text,
            'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
            'incoming_processing_status', v_incoming_processing_status,
            'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
            'live_timesheet_authorised_at_server_present', v_current_ts.authorised_at_server IS NOT NULL,
            'live_contract_week_status', v_live_contract_week_status,
            'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
            'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
            'decision', v_lifecycle_decision,
            'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
            'result_authorised_at_utc_present', false,
            'rejection_reason', 'LIVE_AUTHORISED_STATE_WITHOUT_AUTHORISATION_TIMESTAMP',
            'policy_x_boundary', 'LIFECYCLE_FIELDS_ONLY_NO_ECONOMICS_CHANGE'
          )
        )
      );
      RAISE EXCEPTION USING
        ERRCODE = '40001',
        MESSAGE = 'TSFIN_SNAPSHOT_LIFECYCLE_DOWNGRADE_REJECTED',
        DETAIL = jsonb_build_object(
          'timesheet_id', p_timesheet_id::text,
          'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
          'incoming_processing_status', v_incoming_processing_status,
          'live_contract_week_status', v_live_contract_week_status,
          'reason', 'LIVE_AUTHORISED_STATE_WITHOUT_AUTHORISATION_TIMESTAMP'
        )::text;
    END IF;

    v_processing_status := CASE
      WHEN prev.processing_status IN (
             'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
             'READY_FOR_HR'::public.ts_fin_processing_status_enum
           )
        THEN prev.processing_status
      WHEN v_processing_status IN (
             'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
             'READY_FOR_HR'::public.ts_fin_processing_status_enum
           )
        THEN v_processing_status
      ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    END;
    v_result_authorised_at_utc := COALESCE(
      prev.authorised_at_utc,
      v_current_ts.authorised_at_server,
      v_incoming_authorised_at_utc
    );
    v_result_authorised_by_user_id := COALESCE(
      prev.authorised_by_user_id,
      v_result_authorised_by_user_id,
      p_actor_user_id
    );
    v_lifecycle_decision := 'PRESERVED';
  END IF;

  PERFORM public._temp_diag_log(
    'TSFIN_SNAPSHOT_LIVE_LIFECYCLE_GUARD',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_strip_nulls(
      jsonb_build_object(
        'function_name', 'tsfin_write_current_snapshot_single_bounded',
        'timesheet_id', p_timesheet_id::text,
        'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
        'incoming_processing_status', v_incoming_processing_status,
        'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
        'live_timesheet_authorised_at_server_present', v_current_ts.authorised_at_server IS NOT NULL,
        'live_contract_week_id', CASE WHEN v_live_contract_week_id IS NULL THEN NULL::text ELSE v_live_contract_week_id::text END,
        'live_contract_week_status', v_live_contract_week_status
      )
      || jsonb_build_object(
        'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
        'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
        'decision', v_lifecycle_decision,
        'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
        'result_authorised_at_utc_present', v_result_authorised_at_utc IS NOT NULL,
        'policy_x_boundary', 'LIFECYCLE_FIELDS_ONLY_NO_ECONOMICS_CHANGE'
      )
    )
  );

  IF v_lifecycle_decision = 'PRESERVED' THEN
    PERFORM public._temp_diag_log(
      'TSFIN_SNAPSHOT_LIFECYCLE_STATUS_PRESERVED',
      'TEMP_TIMESHEET_LIFECYCLE',
      p_timesheet_id::text,
      jsonb_strip_nulls(
        jsonb_build_object(
          'function_name', 'tsfin_write_current_snapshot_single_bounded',
          'timesheet_id', p_timesheet_id::text,
          'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
          'incoming_processing_status', v_incoming_processing_status,
          'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
          'live_timesheet_authorised_at_server_present', v_current_ts.authorised_at_server IS NOT NULL,
          'live_contract_week_status', v_live_contract_week_status,
          'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
          'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
          'decision', v_lifecycle_decision,
          'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
          'result_authorised_at_utc_present', v_result_authorised_at_utc IS NOT NULL
        )
      )
    );
  END IF;

  PERFORM public.tsfin_prepare_write(p_timesheet_id);
  did_prepare := true;

  v_cat_pay :=
      COALESCE(NULLIF(snap ->> 'travel_pay_ex_vat', '')::numeric, prev.travel_pay_ex_vat, 0)
    + COALESCE(NULLIF(snap ->> 'accommodation_pay_ex_vat', '')::numeric, prev.accommodation_pay_ex_vat, 0)
    + COALESCE(NULLIF(snap ->> 'other_pay_ex_vat', '')::numeric, prev.other_pay_ex_vat, 0);

  v_cat_charge :=
      COALESCE(NULLIF(snap ->> 'travel_charge_ex_vat', '')::numeric, prev.travel_charge_ex_vat, 0)
    + COALESCE(NULLIF(snap ->> 'accommodation_charge_ex_vat', '')::numeric, prev.accommodation_charge_ex_vat, 0)
    + COALESCE(NULLIF(snap ->> 'other_charge_ex_vat', '')::numeric, prev.other_charge_ex_vat, 0);

  v_fallback_exp_pay := COALESCE(NULLIF(snap ->> 'expenses_pay_ex_vat', '')::numeric, prev.expenses_pay_ex_vat, 0);
  v_fallback_exp_charge := COALESCE(NULLIF(snap ->> 'expenses_charge_ex_vat', '')::numeric, prev.expenses_charge_ex_vat, 0);
  v_exp_pay := CASE WHEN COALESCE(v_cat_pay, 0) <> 0 THEN v_cat_pay ELSE v_fallback_exp_pay END;
  v_exp_charge := CASE WHEN COALESCE(v_cat_charge, 0) <> 0 THEN v_cat_charge ELSE v_fallback_exp_charge END;
  v_exp_pay := round(COALESCE(v_exp_pay, 0), 2);
  v_exp_charge := round(COALESCE(v_exp_charge, 0), 2);

  v_add_units_json := COALESCE(snap -> 'additional_units_json', prev.additional_units_json, '{}'::jsonb);
  v_add_pay := round(COALESCE(NULLIF(snap ->> 'additional_pay_ex_vat', '')::numeric, prev.additional_pay_ex_vat, 0), 2);
  v_add_charge := round(COALESCE(NULLIF(snap ->> 'additional_charge_ex_vat', '')::numeric, prev.additional_charge_ex_vat, 0), 2);
  v_mil_pay := round(COALESCE(NULLIF(snap ->> 'mileage_pay_ex_vat', '')::numeric, prev.mileage_pay_ex_vat, 0), 2);
  v_mil_charge := round(COALESCE(NULLIF(snap ->> 'mileage_charge_ex_vat', '')::numeric, prev.mileage_charge_ex_vat, 0), 2);

  v_nonseg_pay := round(v_add_pay + v_exp_pay + v_mil_pay, 2);
  v_nonseg_charge := round(v_add_charge + v_exp_charge + v_mil_charge, 2);

  v_core_pay := 0;
  v_core_charge := 0;
  v_mode := UPPER(COALESCE(v_ib ->> 'mode', ''));

  IF v_mode = 'SEGMENTS' AND jsonb_typeof(v_ib -> 'segments') = 'array' THEN
    FOR v_seg IN
      SELECT segment_value.value
      FROM jsonb_array_elements(v_ib -> 'segments') AS segment_value(value)
    LOOP
      IF v_seg IS NULL OR jsonb_typeof(v_seg) <> 'object' THEN
        CONTINUE;
      END IF;

      BEGIN
        v_core_charge := v_core_charge + COALESCE(NULLIF(BTRIM(COALESCE(v_seg ->> 'charge_amount', '')), '')::numeric, 0);
      EXCEPTION WHEN OTHERS THEN
        NULL;
      END;

      v_exclude := false;
      BEGIN
        v_exclude := COALESCE(NULLIF(BTRIM(COALESCE(v_seg ->> 'exclude_from_pay', '')), '')::boolean, false);
      EXCEPTION WHEN OTHERS THEN
        v_exclude := false;
      END;

      IF NOT v_exclude THEN
        BEGIN
          v_core_pay := v_core_pay + COALESCE(NULLIF(BTRIM(COALESCE(v_seg ->> 'pay_amount', '')), '')::numeric, 0);
        EXCEPTION WHEN OTHERS THEN
          NULL;
        END;
      END IF;
    END LOOP;
  ELSE
    BEGIN
      v_core_pay := COALESCE(NULLIF(BTRIM(COALESCE(v_ib #>> '{base_hours,pay_ex_vat}', '')), '')::numeric, 0);
    EXCEPTION WHEN OTHERS THEN
      v_core_pay := 0;
    END;

    BEGIN
      v_core_charge := COALESCE(NULLIF(BTRIM(COALESCE(v_ib #>> '{base_hours,charge_ex_vat}', '')), '')::numeric, 0);
    EXCEPTION WHEN OTHERS THEN
      v_core_charge := 0;
    END;
  END IF;

  v_core_pay := round(COALESCE(v_core_pay, 0), 2);
  v_core_charge := round(COALESCE(v_core_charge, 0), 2);
  v_total_pay := round(v_core_pay + v_nonseg_pay, 2);
  v_total_charge := round(v_core_charge + v_nonseg_charge, 2);

  v_policy := COALESCE(snap -> 'policy_snapshot_json', prev.policy_snapshot_json, '{}'::jsonb);
  IF v_policy IS NULL OR jsonb_typeof(v_policy) <> 'object' THEN
    v_policy := '{}'::jsonb;
  END IF;

  v_apply_to := UPPER(COALESCE(NULLIF(BTRIM(COALESCE(v_policy ->> 'apply_erni_to', '')), ''), 'PAYE_ONLY'));
  BEGIN
    v_erni_pct_raw := COALESCE(NULLIF(BTRIM(COALESCE(v_policy ->> 'erni_pct', '')), '')::numeric, 0);
  EXCEPTION WHEN OTHERS THEN
    v_erni_pct_raw := 0;
  END;

  v_erni_mult := 1;
  IF COALESCE(v_erni_pct_raw, 0) > 0 THEN
    IF v_erni_pct_raw > 1 THEN
      v_erni_mult := 1 + (v_erni_pct_raw / 100);
    ELSE
      v_erni_mult := 1 + v_erni_pct_raw;
    END IF;
  END IF;

  v_pay_method_u := UPPER(COALESCE(NULLIF(BTRIM(COALESCE(snap ->> 'pay_method', '')), ''), NULLIF(BTRIM(COALESCE(prev.pay_method::text, '')), ''), ''));
  IF v_pay_method_u IN ('PAYE', 'UMBRELLA') THEN
    v_pay_method_insert := v_pay_method_u;
  ELSE
    v_pay_method_insert := NULL;
  END IF;
  v_erni_applies := (v_pay_method_insert = 'PAYE') AND (v_apply_to = 'ALL' OR v_apply_to = 'PAYE_ONLY');

  v_wage_pay := round(v_core_pay + v_add_pay, 2);
  v_reimb_pay := round(v_exp_pay + v_mil_pay, 2);
  v_wage_pay_cost := v_wage_pay;
  IF v_erni_applies THEN
    v_wage_pay_cost := round(v_wage_pay * v_erni_mult, 2);
  END IF;
  v_pay_cost := round(v_wage_pay_cost + v_reimb_pay, 2);
  v_margin := round(v_total_charge - v_pay_cost, 2);

  v_nonseg_wage_pay := v_add_pay;
  v_nonseg_reimb_pay := round(v_exp_pay + v_mil_pay, 2);
  v_nonseg_wage_pay_cost := v_nonseg_wage_pay;
  IF v_erni_applies THEN
    v_nonseg_wage_pay_cost := round(v_nonseg_wage_pay * v_erni_mult, 2);
  END IF;
  v_nonseg_pay_cost := round(v_nonseg_wage_pay_cost + v_nonseg_reimb_pay, 2);
  v_nonseg_margin := round(v_nonseg_charge - v_nonseg_pay_cost, 2);

  v_add_obj := CASE WHEN v_ib ? 'additional' AND jsonb_typeof(v_ib -> 'additional') = 'object' THEN v_ib -> 'additional' ELSE '{}'::jsonb END;
  IF NOT (v_add_obj ? 'units') OR jsonb_typeof(v_add_obj -> 'units') <> 'object' THEN
    IF v_add_units_json IS NULL OR jsonb_typeof(v_add_units_json) <> 'object' THEN
      v_add_units_json := '{}'::jsonb;
    END IF;
    v_add_obj := jsonb_set(v_add_obj, '{units}', v_add_units_json, true);
  END IF;
  v_add_obj := jsonb_set(v_add_obj, '{pay_ex_vat}', to_jsonb(v_nonseg_pay), true);
  v_add_obj := jsonb_set(v_add_obj, '{charge_ex_vat}', to_jsonb(v_nonseg_charge), true);
  v_add_obj := jsonb_set(v_add_obj, '{margin_ex_vat}', to_jsonb(v_nonseg_margin), true);
  v_ib := jsonb_set(v_ib, '{additional}', v_add_obj, true);

  v_tot_obj := CASE WHEN v_ib ? 'totals' AND jsonb_typeof(v_ib -> 'totals') = 'object' THEN v_ib -> 'totals' ELSE '{}'::jsonb END;
  v_tot_obj := jsonb_set(v_tot_obj, '{total_pay_ex_vat}', to_jsonb(v_total_pay), true);
  v_tot_obj := jsonb_set(v_tot_obj, '{total_charge_ex_vat}', to_jsonb(v_total_charge), true);
  v_tot_obj := jsonb_set(v_tot_obj, '{margin_ex_vat}', to_jsonb(v_margin), true);
  v_ib := jsonb_set(v_ib, '{totals}', v_tot_obj, true);

  v_is_stale := COALESCE(NULLIF(snap ->> 'is_stale', '')::boolean, false);
  v_stale_reason := NULLIF(snap ->> 'stale_reason', '');
  IF v_processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN
    v_is_stale := false;
    v_stale_reason := NULL;
  END IF;

  v_mileage_units := GREATEST(COALESCE(NULLIF(snap ->> 'mileage_units', '')::numeric, prev.mileage_units, 0), 0);

  INSERT INTO public.timesheets_financials (
    timesheet_id,
    timesheet_version,
    basis,
    is_current,
    is_stale,
    stale_reason,
    worked_start_iso,
    worked_end_iso,
    break_start_iso,
    break_end_iso,
    break_minutes,
    candidate_id,
    client_id,
    role,
    band,
    pay_method,
    policy_snapshot_json,
    rate_source_refs_json,
    hours_day,
    hours_night,
    hours_sat,
    hours_sun,
    hours_bh,
    pay_day,
    pay_night,
    pay_sat,
    pay_sun,
    pay_bh,
    charge_day,
    charge_night,
    charge_sat,
    charge_sun,
    charge_bh,
    total_hours,
    total_pay_ex_vat,
    total_charge_ex_vat,
    margin_ex_vat,
    computed_at_utc,
    occupant_key_norm,
    candidate_assignment,
    processing_status,
    expenses_pay_ex_vat,
    expenses_charge_ex_vat,
    expenses_description,
    expenses_evidence_r2_key,
    expenses_evidence_manifest,
    travel_pay_ex_vat,
    travel_charge_ex_vat,
    accommodation_pay_ex_vat,
    accommodation_charge_ex_vat,
    other_pay_ex_vat,
    other_charge_ex_vat,
    mileage_pay_ex_vat,
    mileage_charge_ex_vat,
    mileage_units,
    mileage_evidence_r2_key,
    mileage_evidence_manifest,
    mileage_pay_rate,
    mileage_charge_rate,
    po_number,
    pay_on_hold,
    pay_on_hold_reason,
    pay_on_hold_since_utc,
    pay_wtr_rate_pct_snapshot,
    pay_vat_rate_pct_snapshot,
    pay_vat_amount_snapshot,
    pay_total_inc_vat_snapshot,
    payment_reference,
    remittance_last_sent_at_utc,
    remittance_send_count,
    processed_by_user_id,
    processed_at_utc,
    authorised_by_user_id,
    authorised_at_utc,
    additional_units_json,
    additional_pay_ex_vat,
    additional_charge_ex_vat,
    additional_margin_ex_vat,
    invoice_breakdown_json,
    nhsp_import_id,
    has_rate_issue,
    has_pay_channel_issue,
    hr_crosscheck_status,
    hr_crosscheck_issues,
    external_source_rows_json,
    actual_schedule_json,
    actual_minutes_by_day_json
  )
  VALUES (
    p_timesheet_id,
    v_timesheet_version,
    v_basis,
    true,
    v_is_stale,
    v_stale_reason,
    NULLIF(snap ->> 'worked_start_iso', '')::timestamptz,
    NULLIF(snap ->> 'worked_end_iso', '')::timestamptz,
    NULLIF(snap ->> 'break_start_iso', '')::timestamptz,
    NULLIF(snap ->> 'break_end_iso', '')::timestamptz,
    NULLIF(snap ->> 'break_minutes', '')::integer,
    NULLIF(snap ->> 'candidate_id', '')::uuid,
    NULLIF(snap ->> 'client_id', '')::uuid,
    NULLIF(snap ->> 'role', ''),
    NULLIF(snap ->> 'band', ''),
    v_pay_method_insert,
    COALESCE(snap -> 'policy_snapshot_json', '{}'::jsonb),
    COALESCE(snap -> 'rate_source_refs_json', '{}'::jsonb),
    COALESCE(NULLIF(snap ->> 'hours_day', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'hours_night', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'hours_sat', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'hours_sun', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'hours_bh', '')::numeric, 0),
    NULLIF(snap ->> 'pay_day', '')::numeric,
    NULLIF(snap ->> 'pay_night', '')::numeric,
    NULLIF(snap ->> 'pay_sat', '')::numeric,
    NULLIF(snap ->> 'pay_sun', '')::numeric,
    NULLIF(snap ->> 'pay_bh', '')::numeric,
    NULLIF(snap ->> 'charge_day', '')::numeric,
    NULLIF(snap ->> 'charge_night', '')::numeric,
    NULLIF(snap ->> 'charge_sat', '')::numeric,
    NULLIF(snap ->> 'charge_sun', '')::numeric,
    NULLIF(snap ->> 'charge_bh', '')::numeric,
    COALESCE(NULLIF(snap ->> 'total_hours', '')::numeric, 0),
    COALESCE(v_total_pay, 0),
    COALESCE(v_total_charge, 0),
    COALESCE(v_margin, 0),
    v_now,
    NULLIF(snap ->> 'occupant_key_norm', ''),
    v_candidate_assignment,
    v_processing_status,
    COALESCE(v_exp_pay, 0),
    COALESCE(v_exp_charge, 0),
    NULLIF(snap ->> 'expenses_description', ''),
    NULLIF(snap ->> 'expenses_evidence_r2_key', ''),
    COALESCE(snap -> 'expenses_evidence_manifest', prev.expenses_evidence_manifest),
    COALESCE(NULLIF(snap ->> 'travel_pay_ex_vat', '')::numeric, prev.travel_pay_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'travel_charge_ex_vat', '')::numeric, prev.travel_charge_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'accommodation_pay_ex_vat', '')::numeric, prev.accommodation_pay_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'accommodation_charge_ex_vat', '')::numeric, prev.accommodation_charge_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'other_pay_ex_vat', '')::numeric, prev.other_pay_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'other_charge_ex_vat', '')::numeric, prev.other_charge_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'mileage_pay_ex_vat', '')::numeric, prev.mileage_pay_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'mileage_charge_ex_vat', '')::numeric, prev.mileage_charge_ex_vat, 0),
    v_mileage_units,
    NULLIF(snap ->> 'mileage_evidence_r2_key', ''),
    COALESCE(snap -> 'mileage_evidence_manifest', prev.mileage_evidence_manifest),
    NULLIF(snap ->> 'mileage_pay_rate', '')::numeric,
    NULLIF(snap ->> 'mileage_charge_rate', '')::numeric,
    COALESCE(NULLIF(snap ->> 'po_number', ''), prev.po_number),
    COALESCE(NULLIF(snap ->> 'pay_on_hold', '')::boolean, prev.pay_on_hold, false),
    COALESCE(NULLIF(snap ->> 'pay_on_hold_reason', ''), prev.pay_on_hold_reason),
    COALESCE(NULLIF(snap ->> 'pay_on_hold_since_utc', '')::timestamptz, prev.pay_on_hold_since_utc),
    NULLIF(snap ->> 'pay_wtr_rate_pct_snapshot', '')::numeric,
    NULLIF(snap ->> 'pay_vat_rate_pct_snapshot', '')::numeric,
    COALESCE(NULLIF(snap ->> 'pay_vat_amount_snapshot', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'pay_total_inc_vat_snapshot', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'payment_reference', ''), prev.payment_reference),
    COALESCE(NULLIF(snap ->> 'remittance_last_sent_at_utc', '')::timestamptz, prev.remittance_last_sent_at_utc),
    COALESCE(NULLIF(snap ->> 'remittance_send_count', '')::integer, prev.remittance_send_count, 0),
    COALESCE(NULLIF(snap ->> 'processed_by_user_id', '')::uuid, prev.processed_by_user_id, p_actor_user_id),
    COALESCE(NULLIF(snap ->> 'processed_at_utc', '')::timestamptz, prev.processed_at_utc, v_now),
    v_result_authorised_by_user_id,
    v_result_authorised_at_utc,
    COALESCE(snap -> 'additional_units_json', prev.additional_units_json, '{}'::jsonb),
    COALESCE(NULLIF(snap ->> 'additional_pay_ex_vat', '')::numeric, prev.additional_pay_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'additional_charge_ex_vat', '')::numeric, prev.additional_charge_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'additional_margin_ex_vat', '')::numeric, prev.additional_margin_ex_vat, 0),
    v_ib,
    NULLIF(snap ->> 'nhsp_import_id', '')::uuid,
    COALESCE(NULLIF(snap ->> 'has_rate_issue', '')::boolean, false),
    COALESCE(NULLIF(snap ->> 'has_pay_channel_issue', '')::boolean, false),
    COALESCE(NULLIF(snap ->> 'hr_crosscheck_status', ''), prev.hr_crosscheck_status),
    COALESCE(
      CASE
        WHEN snap ? 'hr_crosscheck_issues' AND jsonb_typeof(snap -> 'hr_crosscheck_issues') = 'array'
          THEN ARRAY(SELECT jsonb_array_elements_text(snap -> 'hr_crosscheck_issues'))
        ELSE NULL
      END,
      prev.hr_crosscheck_issues
    ),
    COALESCE(snap -> 'external_source_rows_json', prev.external_source_rows_json),
    COALESCE(snap -> 'actual_schedule_json', prev.actual_schedule_json),
    COALESCE(snap -> 'actual_minutes_by_day_json', prev.actual_minutes_by_day_json)
  )
  RETURNING * INTO inserted_row;

  RETURN jsonb_build_object(
    'ok', true,
    'timesheet_id', inserted_row.timesheet_id,
    'timesheet_financials_id', inserted_row.id,
    'processing_status', inserted_row.processing_status::text,
    'timesheet_version', inserted_row.timesheet_version,
    'updated_at', inserted_row.updated_at,
    'computed_at_utc', inserted_row.computed_at_utc,
    'version_marker', md5(jsonb_build_object(
      'id', inserted_row.id,
      'timesheet_id', inserted_row.timesheet_id,
      'timesheet_version', inserted_row.timesheet_version,
      'processing_status', inserted_row.processing_status::text,
      'updated_at', inserted_row.updated_at,
      'computed_at_utc', inserted_row.computed_at_utc,
      'total_hours', inserted_row.total_hours,
      'total_pay_ex_vat', inserted_row.total_pay_ex_vat,
      'total_charge_ex_vat', inserted_row.total_charge_ex_vat,
      'margin_ex_vat', inserted_row.margin_ex_vat
    )::text)
  );
EXCEPTION
  WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
      v_error_state = RETURNED_SQLSTATE,
      v_error_message = MESSAGE_TEXT;

    IF did_prepare AND v_prev_id IS NOT NULL THEN
      UPDATE public.timesheets_financials AS restore_tf
         SET is_current = true,
             updated_at = COALESCE(p_now_utc, now())
       WHERE restore_tf.id = v_prev_id
         AND restore_tf.locked_by_invoice_id IS NULL
         AND restore_tf.paid_at_utc IS NULL;
    END IF;

    IF v_error_state IN ('55P03', '57014') THEN
      RAISE EXCEPTION USING
        MESSAGE = 'LOCK_TIMEOUT',
        DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id, 'error_state', v_error_state)::text;
    END IF;

    IF v_error_message = 'TSFIN_LOCKED' THEN
      RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_OR_PAID', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
    END IF;

    IF v_error_state IN ('22P02', '22007', '22008', '22003', '22018') THEN
      RAISE EXCEPTION USING
        MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH',
        DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id, 'error_state', v_error_state, 'error_message', v_error_message)::text;
    END IF;

    RAISE;
END;
$function$;


CREATE OR REPLACE FUNCTION public.timesheet_lifecycle_affected_rows_v1(p_items jsonb DEFAULT '[]'::jsonb, p_context text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_items jsonb := '[]'::jsonb;
  v_item jsonb;
  v_rows jsonb := '[]'::jsonb;
  v_missing jsonb := '[]'::jsonb;
  v_removed jsonb := '[]'::jsonb;
  v_seen_keys text[] := ARRAY[]::text[];
  v_uuid_re text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
  v_item_count integer := 0;

  v_item_context text := NULL;
  v_item_key text := NULL;
  v_timesheet_id uuid := NULL;
  v_contract_week_id uuid := NULL;
  v_previous_timesheet_id uuid := NULL;
  v_booking_id text := NULL;
  v_previous_row_key text := NULL;
  v_previous_bucket text := NULL;

  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_week public.contract_weeks%ROWTYPE;
  v_contract public.contracts%ROWTYPE;
  v_tsfin public.timesheets_financials%ROWTYPE;
  v_candidate public.candidates%ROWTYPE;
  v_client public.clients%ROWTYPE;

  v_signature_json jsonb := '{}'::jsonb;
  v_evidence_summary jsonb := '{}'::jsonb;
  v_staged_summary jsonb := '{}'::jsonb;
  v_invoice_segments_locked integer := 0;
  v_is_paid boolean := false;
  v_is_archived boolean := false;
  v_is_invoice_locked boolean := false;
  v_is_locked boolean := false;
  v_is_authorised boolean := false;
  v_is_unprocessed boolean := false;
  v_has_retained_financial_history boolean := false;
  v_can_process boolean := false;
  v_can_unprocess boolean := false;
  v_unprocess_action_visible boolean := false;
  v_unprocess_block_reason text := NULL;
  v_can_authorise boolean := false;
  v_can_unauthorise boolean := false;
  v_qr_unsigned_blocked boolean := false;
  v_disabled_reasons text[] := ARRAY[]::text[];
  v_bucket text := NULL;
  v_row_key text := NULL;
  v_removed_reason text := NULL;
  v_temp_log_enabled boolean := false;
BEGIN
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

  IF p_items IS NULL THEN
    v_items := '[]'::jsonb;
  ELSIF jsonb_typeof(p_items) = 'array' THEN
    v_items := p_items;
  ELSIF jsonb_typeof(p_items) = 'object' AND jsonb_typeof(p_items -> 'items') = 'array' THEN
    v_items := p_items -> 'items';
  ELSE
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'INVALID_PAYLOAD',
      'message', 'p_items must be a JSON array or an object containing an items array',
      'rows', '[]'::jsonb,
      'missing', '[]'::jsonb,
      'removed', '[]'::jsonb,
      'count_deltas', '{}'::jsonb,
      'cache_invalidation_hints', '{}'::jsonb
    );
  END IF;

  v_item_count := jsonb_array_length(v_items);
  IF v_item_count > 100 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'TOO_MANY_ITEMS',
      'message', 'timesheet_lifecycle_affected_rows_v1 accepts at most 100 affected identities per call',
      'requested_count', v_item_count,
      'rows', '[]'::jsonb,
      'missing', '[]'::jsonb,
      'removed', '[]'::jsonb,
      'count_deltas', '{}'::jsonb,
      'cache_invalidation_hints', jsonb_build_object(
        'changed_domains', jsonb_build_array('timesheet_lifecycle'),
        'context', NULLIF(BTRIM(COALESCE(p_context, '')), '')
      )
    );
  END IF;

  FOR v_item IN
    SELECT item_value.value
    FROM jsonb_array_elements(v_items) AS item_value(value)
  LOOP
    v_item_context := LOWER(NULLIF(BTRIM(COALESCE(v_item ->> 'context', p_context, '')), ''));
    v_timesheet_id := NULL;
    v_contract_week_id := NULL;
    v_previous_timesheet_id := NULL;
    v_booking_id := NULLIF(BTRIM(COALESCE(v_item ->> 'booking_id', v_item ->> 'bookingId', '')), '');
    v_previous_row_key := NULLIF(BTRIM(COALESCE(v_item ->> 'previous_row_key', v_item ->> 'previousRowKey', '')), '');
    v_previous_bucket := NULLIF(BTRIM(COALESCE(v_item ->> 'previous_bucket', v_item ->> 'previousBucket', '')), '');

    IF NULLIF(BTRIM(COALESCE(v_item ->> 'timesheet_id', v_item ->> 'current_timesheet_id', '')), '') ~* v_uuid_re THEN
      v_timesheet_id := NULLIF(BTRIM(COALESCE(v_item ->> 'timesheet_id', v_item ->> 'current_timesheet_id', '')), '')::uuid;
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_item ->> 'contract_week_id', v_item ->> 'week_id', '')), '') ~* v_uuid_re THEN
      v_contract_week_id := NULLIF(BTRIM(COALESCE(v_item ->> 'contract_week_id', v_item ->> 'week_id', '')), '')::uuid;
    END IF;

    IF NULLIF(BTRIM(COALESCE(v_item ->> 'previous_timesheet_id', v_item ->> 'previousTimesheetId', '')), '') ~* v_uuid_re THEN
      v_previous_timesheet_id := NULLIF(BTRIM(COALESCE(v_item ->> 'previous_timesheet_id', v_item ->> 'previousTimesheetId', '')), '')::uuid;
    END IF;

    v_item_key := COALESCE(v_timesheet_id::text, '') || '|' || COALESCE(v_contract_week_id::text, '') || '|' || COALESCE(v_booking_id, '') || '|' || COALESCE(v_previous_timesheet_id::text, '');
    IF v_item_key = '|||' OR v_item_key = '' THEN
      v_missing := v_missing || jsonb_build_array(jsonb_build_object('input', v_item, 'error_code', 'INVALID_IDENTITY'));
      CONTINUE;
    END IF;
    IF v_item_key = ANY(v_seen_keys) THEN
      CONTINUE;
    END IF;
    v_seen_keys := array_append(v_seen_keys, v_item_key);

    v_requested_ts := NULL;
    v_current_ts := NULL;
    v_week := NULL;
    v_contract := NULL;
    v_tsfin := NULL;
    v_candidate := NULL;
    v_client := NULL;
    v_signature_json := '{}'::jsonb;
    v_evidence_summary := '{}'::jsonb;
    v_staged_summary := '{}'::jsonb;
    v_invoice_segments_locked := 0;
    v_is_paid := false;
    v_is_archived := false;
    v_is_invoice_locked := false;
    v_is_locked := false;
    v_is_authorised := false;
    v_is_unprocessed := false;
    v_has_retained_financial_history := false;
    v_can_process := false;
    v_can_unprocess := false;
    v_unprocess_action_visible := false;
    v_unprocess_block_reason := NULL;
    v_can_authorise := false;
    v_can_unauthorise := false;
    v_qr_unsigned_blocked := false;
    v_disabled_reasons := ARRAY[]::text[];
    v_bucket := NULL;
    v_row_key := NULL;
    v_removed_reason := NULL;

    IF v_contract_week_id IS NOT NULL THEN
      SELECT cw.*
        INTO v_week
      FROM public.contract_weeks AS cw
      WHERE cw.id = v_contract_week_id
      LIMIT 1;
    END IF;

    IF v_timesheet_id IS NOT NULL THEN
      SELECT ts.*
        INTO v_requested_ts
      FROM public.timesheets AS ts
      WHERE ts.timesheet_id = v_timesheet_id
      LIMIT 1;

      IF v_requested_ts.timesheet_id IS NOT NULL THEN
        v_booking_id := COALESCE(v_booking_id, v_requested_ts.booking_id);
      END IF;
    END IF;

    IF v_booking_id IS NOT NULL THEN
      SELECT ts.*
        INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_booking_id
        AND ts.is_current = true
      ORDER BY ts.version DESC, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
      LIMIT 1;
    END IF;

    IF v_current_ts.timesheet_id IS NULL AND v_week.timesheet_id IS NOT NULL THEN
      SELECT ts.*
        INTO v_requested_ts
      FROM public.timesheets AS ts
      WHERE ts.timesheet_id = v_week.timesheet_id
      LIMIT 1;

      IF v_requested_ts.timesheet_id IS NOT NULL THEN
        v_booking_id := COALESCE(v_booking_id, v_requested_ts.booking_id);
        SELECT ts.*
          INTO v_current_ts
        FROM public.timesheets AS ts
        WHERE ts.booking_id = v_requested_ts.booking_id
          AND ts.is_current = true
        ORDER BY ts.version DESC, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
        LIMIT 1;
      END IF;
    END IF;

    IF v_current_ts.timesheet_id IS NULL AND v_requested_ts.timesheet_id IS NOT NULL THEN
      v_current_ts := v_requested_ts;
    END IF;

    IF v_week.id IS NULL AND v_current_ts.timesheet_id IS NOT NULL THEN
      SELECT cw.*
        INTO v_week
      FROM public.contract_weeks AS cw
      WHERE cw.timesheet_id = v_current_ts.timesheet_id
      ORDER BY cw.updated_at DESC NULLS LAST, cw.created_at DESC NULLS LAST, cw.id DESC
      LIMIT 1;
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

    IF v_week.id IS NOT NULL THEN
      SELECT c.*
        INTO v_contract
      FROM public.contracts AS c
      WHERE c.id = v_week.contract_id
      LIMIT 1;
    ELSIF v_current_ts.contract_id IS NOT NULL THEN
      SELECT c.*
        INTO v_contract
      FROM public.contracts AS c
      WHERE c.id = v_current_ts.contract_id
      LIMIT 1;
    END IF;

    IF COALESCE(v_tsfin.candidate_id, v_contract.candidate_id) IS NOT NULL THEN
      SELECT cand.*
        INTO v_candidate
      FROM public.candidates AS cand
      WHERE cand.id = COALESCE(v_tsfin.candidate_id, v_contract.candidate_id)
      LIMIT 1;
    END IF;

    IF COALESCE(v_tsfin.client_id, v_contract.client_id) IS NOT NULL THEN
      SELECT cli.*
        INTO v_client
      FROM public.clients AS cli
      WHERE cli.id = COALESCE(v_tsfin.client_id, v_contract.client_id)
      LIMIT 1;
    END IF;

    IF v_current_ts.timesheet_id IS NULL AND v_week.id IS NULL THEN
      v_missing := v_missing || jsonb_build_array(
        jsonb_build_object(
          'input', v_item,
          'timesheet_id', v_timesheet_id,
          'contract_week_id', v_contract_week_id,
          'booking_id', v_booking_id,
          'error_code', 'TARGET_NOT_FOUND'
        )
      );
      CONTINUE;
    END IF;

    IF v_current_ts.timesheet_id IS NULL AND v_week.id IS NOT NULL AND v_week.timesheet_id IS NULL THEN
      v_removed_reason := 'CONTRACT_WEEK_REOPENED';
    END IF;

    IF v_tsfin.id IS NOT NULL THEN
      SELECT COUNT(*)::integer
        INTO v_invoice_segments_locked
      FROM jsonb_array_elements(
        CASE
          WHEN v_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(v_tsfin.invoice_breakdown_json) = 'array' THEN v_tsfin.invoice_breakdown_json
          WHEN jsonb_typeof(v_tsfin.invoice_breakdown_json) = 'object'
           AND jsonb_typeof(v_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_tsfin.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL;
    END IF;

    IF v_current_ts.timesheet_id IS NOT NULL THEN
      SELECT jsonb_strip_nulls(
        jsonb_build_object(
          'count', COUNT(*)::integer,
          'has_timesheet', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(ev.kind), ''), 'OTHER')) = 'TIMESHEET') > 0,
          'has_mileage', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(ev.kind), ''), 'OTHER')) = 'MILEAGE') > 0,
          'has_travel', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(ev.kind), ''), 'OTHER')) = 'TRAVEL') > 0,
          'has_accommodation', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(ev.kind), ''), 'OTHER')) = 'ACCOMMODATION') > 0,
          'has_other', COUNT(*) FILTER (WHERE UPPER(COALESCE(NULLIF(BTRIM(ev.kind), ''), 'OTHER')) NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION')) > 0,
          'updated_at', MAX(ev.created_at),
          'primary_storage_key', MIN(ev.storage_key),
          'manual_pdf_r2_key', NULLIF(v_current_ts.manual_pdf_r2_key, ''),
          'manual_pdf_rotation_degrees', v_current_ts.manual_pdf_rotation_degrees
        )
      )
        INTO v_evidence_summary
      FROM public.timesheet_evidence AS ev
      WHERE ev.timesheet_id = v_current_ts.timesheet_id;
    ELSE
      v_evidence_summary := jsonb_build_object(
        'count', 0,
        'has_timesheet', false,
        'has_mileage', false,
        'has_travel', false,
        'has_accommodation', false,
        'has_other', false
      );
    END IF;

    IF v_week.id IS NOT NULL THEN
      WITH staged_rows AS (
        SELECT
          mq.id,
          mq.uploaded_at_utc,
          mq.last_rotation_deg,
          UPPER(COALESCE(
            NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
            NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
            NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
            'TIMESHEET'
          )) AS staged_kind,
          NULLIF(regexp_replace(COALESCE(
            NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
            NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
            ''
          ), '^/+', ''), '') AS storage_key
        FROM public.manual_timesheet_queue AS mq
        WHERE mq.status = 'STAGED'
          AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
      )
      SELECT jsonb_strip_nulls(
        jsonb_build_object(
          'staged_count', COUNT(*)::integer,
          'has_staged_timesheet', COUNT(*) FILTER (WHERE sr.staged_kind = 'TIMESHEET') > 0,
          'active_staged_timesheet_count', COUNT(*) FILTER (WHERE sr.staged_kind = 'TIMESHEET')::integer,
          'primary_staged_timesheet_storage_key', MIN(sr.storage_key) FILTER (WHERE sr.staged_kind = 'TIMESHEET'),
          'primary_staged_timesheet_rotation_degrees', MIN(sr.last_rotation_deg) FILTER (WHERE sr.staged_kind = 'TIMESHEET'),
          'updated_at', MAX(sr.uploaded_at_utc)
        )
      )
        INTO v_staged_summary
      FROM staged_rows AS sr;
    ELSE
      v_staged_summary := jsonb_build_object('staged_count', 0, 'has_staged_timesheet', false, 'active_staged_timesheet_count', 0);
    END IF;

    v_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, v_week.id, COALESCE(v_temp_log_enabled, false));

    v_is_paid := COALESCE(v_tsfin.paid_at_utc IS NOT NULL, false);
    v_is_archived := COALESCE(v_current_ts.archived_at_utc IS NOT NULL, false);
    v_is_invoice_locked := COALESCE(v_tsfin.locked_by_invoice_id IS NOT NULL, false) OR COALESCE(v_invoice_segments_locked, 0) > 0;
    v_is_locked := COALESCE(v_is_invoice_locked, false);
    v_is_authorised := COALESCE(v_current_ts.authorised_at_server IS NOT NULL, false)
      OR COALESCE(v_tsfin.authorised_at_utc IS NOT NULL, false)
      OR COALESCE(v_week.status = 'AUTHORISED'::public.contract_week_status_enum, false);
    v_is_unprocessed := COALESCE(
      v_current_ts.timesheet_id IS NULL
      OR v_tsfin.id IS NULL
      OR v_tsfin.processing_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum
      OR v_week.status = 'OPEN'::public.contract_week_status_enum,
      false
    );

    IF v_current_ts.timesheet_id IS NOT NULL THEN
      IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum THEN
        SELECT EXISTS (
          SELECT 1
          FROM public.timesheets AS unit_timesheet
          JOIN public.timesheet_financial_retention AS retention
            ON retention.timesheet_id = unit_timesheet.timesheet_id
          WHERE unit_timesheet.booking_id = v_current_ts.booking_id
        )
          INTO v_has_retained_financial_history;
      ELSE
        SELECT EXISTS (
          SELECT 1
          FROM public.timesheet_financial_retention AS retention
          WHERE retention.timesheet_id = v_current_ts.timesheet_id
        )
          INTO v_has_retained_financial_history;
      END IF;
    ELSE
      v_has_retained_financial_history := false;
    END IF;

    v_qr_unsigned_blocked := COALESCE(
      v_current_ts.timesheet_id IS NOT NULL
      AND (
        COALESCE(v_tsfin.processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum, false)
        OR (
          UPPER(COALESCE(v_current_ts.qr_status::text, '')) = 'PENDING'
          AND NULLIF(BTRIM(COALESCE(v_current_ts.qr_token, '')), '') IS NOT NULL
          AND v_current_ts.qr_generated_at IS NOT NULL
          AND v_current_ts.qr_scanned_at IS NULL
        )
      ),
      false
    );

    IF v_is_archived THEN
      v_disabled_reasons := array_append(v_disabled_reasons, 'TIMESHEET_ARCHIVED');
    END IF;
    IF v_is_invoice_locked THEN
      v_disabled_reasons := array_append(v_disabled_reasons, 'TIMESHEET_LOCKED_BY_INVOICE');
    END IF;
    IF v_is_authorised THEN
      v_disabled_reasons := array_append(v_disabled_reasons, 'ALREADY_AUTHORISED');
    END IF;
    IF v_qr_unsigned_blocked THEN
      v_disabled_reasons := array_append(v_disabled_reasons, 'AWAITING_SIGNED_QR');
    END IF;

    v_can_process := COALESCE((NOT v_is_archived) AND (NOT v_is_locked) AND (NOT v_is_authorised) AND v_is_unprocessed, false);
    v_unprocess_action_visible := COALESCE(
      (NOT v_is_archived)
      AND (NOT v_is_locked)
      AND (NOT v_is_authorised)
      AND (NOT v_is_unprocessed)
      AND v_current_ts.timesheet_id IS NOT NULL,
      false
    );
    v_can_unprocess := COALESCE(v_unprocess_action_visible AND (NOT v_has_retained_financial_history), false);
    v_unprocess_block_reason := CASE
      WHEN v_unprocess_action_visible AND v_has_retained_financial_history
      THEN 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS'
      ELSE NULL
    END;
    IF v_unprocess_block_reason IS NOT NULL THEN
      v_disabled_reasons := array_append(v_disabled_reasons, v_unprocess_block_reason);
    END IF;
    v_can_authorise := COALESCE(
      (NOT v_is_archived)
      AND (NOT v_is_locked)
      AND (NOT v_is_authorised)
      AND (NOT v_qr_unsigned_blocked)
      AND v_current_ts.timesheet_id IS NOT NULL
      AND v_tsfin.processing_status IN (
        'PENDING_AUTH'::public.ts_fin_processing_status_enum,
        'READY_FOR_HR'::public.ts_fin_processing_status_enum
      ),
      false
    );
    v_can_unauthorise := COALESCE((NOT v_is_archived) AND (NOT v_is_locked) AND v_is_authorised AND v_current_ts.timesheet_id IS NOT NULL, false);

    v_bucket := CASE
      WHEN v_is_archived THEN 'ARCHIVED'
      WHEN v_is_authorised THEN 'AUTHORISED'
      WHEN v_current_ts.timesheet_id IS NULL THEN 'UNPROCESSED'
      WHEN v_is_unprocessed THEN 'UNPROCESSED'
      ELSE 'PROCESSED'
    END;

    v_row_key := CASE
      WHEN v_current_ts.timesheet_id IS NOT NULL THEN 'timesheet:' || v_current_ts.timesheet_id::text
      WHEN v_week.id IS NOT NULL THEN 'contract_week:' || v_week.id::text
      ELSE NULL
    END;

    IF v_removed_reason IS NOT NULL THEN
      v_removed := v_removed || jsonb_build_array(
        jsonb_strip_nulls(jsonb_build_object(
          'reason', v_removed_reason,
          'previous_timesheet_id', COALESCE(v_previous_timesheet_id, v_timesheet_id),
          'contract_week_id', v_week.id,
          'previous_row_key', v_previous_row_key,
          'current_row_key', v_row_key
        ))
      );
    END IF;

    v_rows := v_rows || jsonb_build_array(
      jsonb_strip_nulls(
        jsonb_build_object(
          'identity', jsonb_strip_nulls(jsonb_build_object(
            'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
            'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
            'previous_timesheet_id', COALESCE(v_previous_timesheet_id, v_timesheet_id),
            'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
            'booking_id', COALESCE(v_current_ts.booking_id, v_booking_id),
            'row_key', v_row_key,
            'previous_row_key', v_previous_row_key
          )),
          'status', jsonb_strip_nulls(jsonb_build_object(
            'bucket', v_bucket,
            'previous_bucket', v_previous_bucket,
            'timesheet_status', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.status::text END,
            'processing_status', CASE WHEN v_tsfin.id IS NULL THEN NULL ELSE v_tsfin.processing_status::text END,
            'contract_week_status', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.status::text END,
            'is_paid', v_is_paid,
            'is_archived', v_is_archived,
            'archived_at_utc', CASE WHEN v_is_archived THEN v_current_ts.archived_at_utc ELSE NULL END,
            'archived_by_user_id', CASE WHEN v_is_archived THEN v_current_ts.archived_by_user_id ELSE NULL END,
            'archived_reason_code', CASE WHEN v_is_archived THEN v_current_ts.archived_reason_code ELSE NULL END,
            'is_invoice_locked', v_is_invoice_locked,
            'is_locked', v_is_locked,
            'is_authorised', v_is_authorised,
            'is_unprocessed', v_is_unprocessed,
            'has_retained_financial_history', v_has_retained_financial_history,
            'can_unprocess', v_can_unprocess,
            'unprocess_block_reason', v_unprocess_block_reason,
            'unprocess_action_visible', v_unprocess_action_visible,
            'qr_unsigned_blocked', v_qr_unsigned_blocked
          )),
          'actions', jsonb_build_object(
            'can_process', v_can_process,
            'can_unprocess', v_can_unprocess,
            'has_retained_financial_history', v_has_retained_financial_history,
            'unprocess_block_reason', v_unprocess_block_reason,
            'unprocess_action_visible', v_unprocess_action_visible,
            'can_authorise', v_can_authorise,
            'can_unauthorise', v_can_unauthorise,
            'disabled_reasons', to_jsonb(v_disabled_reasons)
          ),
          'action_flags', jsonb_build_object(
            'can_process', v_can_process,
            'can_unprocess', v_can_unprocess,
            'has_retained_financial_history', v_has_retained_financial_history,
            'unprocess_block_reason', v_unprocess_block_reason,
            'unprocess_action_visible', v_unprocess_action_visible,
            'can_authorise', v_can_authorise,
            'can_unauthorise', v_can_unauthorise
          ),
          'row_patch', jsonb_strip_nulls(jsonb_build_object(
            'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
            'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
            'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END,
            'row_key', v_row_key,
            'backend_row_signature', v_signature_json ->> 'backend_row_signature',
            'row_signature', v_signature_json ->> 'row_signature',
            'has_retained_financial_history', v_has_retained_financial_history,
            'can_unprocess', v_can_unprocess,
            'unprocess_block_reason', v_unprocess_block_reason,
            'unprocess_action_visible', v_unprocess_action_visible
          ))
        )
        || jsonb_build_object(
          'display', jsonb_strip_nulls(jsonb_build_object(
            'context', v_item_context,
            'week_ending_date', COALESCE(v_current_ts.week_ending_date, v_week.week_ending_date),
            'candidate_id', COALESCE(v_tsfin.candidate_id, v_contract.candidate_id),
            'candidate_display', NULLIF(COALESCE(v_candidate.display_name, BTRIM(CONCAT_WS(' ', v_candidate.first_name, v_candidate.last_name))), ''),
            'client_id', COALESCE(v_tsfin.client_id, v_contract.client_id),
            'client_name', NULLIF(v_client.name, ''),
            'role', COALESCE(NULLIF(v_tsfin.role, ''), NULLIF(v_contract.role, ''), NULLIF(v_current_ts.job_title_norm, '')),
            'band', COALESCE(NULLIF(v_tsfin.band, ''), NULLIF(v_contract.band, ''), NULLIF(v_current_ts.band, '')),
            'total_hours', v_tsfin.total_hours,
            'total_pay_ex_vat', v_tsfin.total_pay_ex_vat,
            'total_charge_ex_vat', v_tsfin.total_charge_ex_vat,
            'margin_ex_vat', v_tsfin.margin_ex_vat
          )),
          'evidence_summary', COALESCE(v_evidence_summary, '{}'::jsonb),
          'staged_evidence_summary', COALESCE(v_staged_summary, '{}'::jsonb),
          'backend_row_signature', v_signature_json ->> 'backend_row_signature',
          'row_signature', v_signature_json ->> 'row_signature',
          'render_signature', v_signature_json ->> 'signature',
          'cache_invalidation_hints', jsonb_build_object(
            'changed_domains', jsonb_build_array('timesheet_lifecycle'),
            'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
            'contract_week_id', CASE WHEN v_week.id IS NULL THEN NULL ELSE v_week.id END
          )
        )
      )
    );
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'rows', v_rows,
    'missing', v_missing,
    'removed', v_removed,
    'count_deltas', '{}'::jsonb,
    'cache_invalidation_hints', jsonb_build_object(
      'changed_domains', jsonb_build_array('timesheet_lifecycle'),
      'context', NULLIF(BTRIM(COALESCE(p_context, '')), '')
    )
  );
END;
$function$;




CREATE OR REPLACE FUNCTION public.manual_timesheet_queue_attach_process_atomic(
  p_queue_id uuid,
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_expected_storage_key text,
  p_kind text DEFAULT 'TIMESHEET'::text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_source_json jsonb DEFAULT '{}'::jsonb,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_queue public.manual_timesheet_queue%ROWTYPE;
  v_ts public.timesheets%ROWTYPE;
  v_kind text := UPPER(BTRIM(COALESCE(p_kind, 'TIMESHEET')));
  v_expected_storage_key text := NULLIF(regexp_replace(COALESCE(BTRIM(p_expected_storage_key), ''), '^/+', ''), '');
  v_queue_storage_key text := NULL;
  v_existing_same public.timesheet_evidence%ROWTYPE;
  v_existing_conflict public.timesheet_evidence%ROWTYPE;
  v_evidence_id uuid := NULL;
  v_display_name text := NULL;
  v_rotation_raw integer := 0;
  v_rotation_degrees integer := 0;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_error_detail text := NULL;
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_queue_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'QUEUE_ID_REQUIRED', 'message', 'p_queue_id is required.');
  END IF;
  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'TIMESHEET_ID_REQUIRED', 'message', 'p_timesheet_id is required.');
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'EXPECTED_TIMESHEET_ID_REQUIRED', 'message', 'p_expected_timesheet_id is required.');
  END IF;
  IF v_expected_storage_key IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'QUEUE_ITEM_STORAGE_REQUIRED', 'message', 'p_expected_storage_key is required.', 'queue_id', p_queue_id);
  END IF;

  IF v_kind = '' OR v_kind = 'TS' THEN
    v_kind := 'TIMESHEET';
  ELSIF v_kind IN ('EXPENSE', 'EXPENSES') THEN
    v_kind := 'TRAVEL';
  ELSIF v_kind IN ('MILES', 'MILE') THEN
    v_kind := 'MILEAGE';
  ELSIF v_kind = 'ACCOM' THEN
    v_kind := 'ACCOMMODATION';
  END IF;

  IF v_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'INVALID_EVIDENCE_KIND', 'message', 'Invalid evidence kind.', 'kind', p_kind);
  END IF;

  SELECT ts.*
    INTO v_ts
    FROM public.timesheets AS ts
   WHERE ts.timesheet_id = p_timesheet_id
     AND ts.is_current = true
   FOR UPDATE;

  IF v_ts.timesheet_id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'TIMESHEET_NOT_FOUND', 'message', 'Timesheet was not found.', 'timesheet_id', p_timesheet_id);
  END IF;

  IF v_ts.timesheet_id IS DISTINCT FROM p_expected_timesheet_id THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'TIMESHEET_MOVED', 'message', 'TIMESHEET_MOVED', 'timesheet_id', p_timesheet_id, 'current_timesheet_id', v_ts.timesheet_id, 'expected_timesheet_id', p_expected_timesheet_id);
  END IF;

  SELECT mq.*
    INTO v_queue
    FROM public.manual_timesheet_queue AS mq
   WHERE mq.id = p_queue_id
   FOR UPDATE;

  IF v_queue.id IS NULL THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'QUEUE_ITEM_NOT_AVAILABLE', 'message', 'Queue item is no longer available.', 'queue_id', p_queue_id, 'expected_storage_key', v_expected_storage_key);
  END IF;

  v_queue_storage_key := NULLIF(regexp_replace(COALESCE(BTRIM(v_queue.r2_key), ''), '^/+', ''), '');
  IF v_queue_storage_key IS NULL OR v_queue_storage_key IS DISTINCT FROM v_expected_storage_key THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'QUEUE_ITEM_STORAGE_MISMATCH', 'message', 'Queue item storage key no longer matches the displayed preview.', 'queue_id', p_queue_id, 'expected_storage_key', v_expected_storage_key, 'storage_key', v_queue_storage_key);
  END IF;

  IF UPPER(COALESCE(v_queue.status, '')) <> 'QUEUED' OR v_queue.timesheet_id IS NOT NULL THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'QUEUE_ITEM_NOT_AVAILABLE', 'message', 'Queue item is no longer available.', 'queue_id', p_queue_id, 'storage_key', v_queue_storage_key, 'status', v_queue.status, 'timesheet_id', v_queue.timesheet_id);
  END IF;

  IF v_kind = 'TIMESHEET' THEN
    SELECT te.*
      INTO v_existing_conflict
      FROM public.timesheet_evidence AS te
     WHERE te.timesheet_id = v_ts.timesheet_id
       AND UPPER(COALESCE(te.kind, '')) = 'TIMESHEET'
       AND NULLIF(regexp_replace(COALESCE(te.storage_key, ''), '^/+', ''), '') IS DISTINCT FROM v_queue_storage_key
     ORDER BY te.created_at DESC NULLS LAST, te.id DESC
     LIMIT 1
     FOR UPDATE;

    IF v_existing_conflict.id IS NOT NULL THEN
      RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'TIMESHEET_EVIDENCE_ALREADY_EXISTS', 'message', 'Only one TIMESHEET evidence file may be attached to a timesheet at a time.', 'timesheet_id', v_ts.timesheet_id, 'existing_evidence_id', v_existing_conflict.id, 'queue_id', p_queue_id, 'storage_key', v_queue_storage_key);
    END IF;
  END IF;

  SELECT te.*
    INTO v_existing_same
    FROM public.timesheet_evidence AS te
   WHERE te.timesheet_id = v_ts.timesheet_id
     AND UPPER(COALESCE(te.kind, '')) = v_kind
     AND NULLIF(regexp_replace(COALESCE(te.storage_key, ''), '^/+', ''), '') = v_queue_storage_key
   ORDER BY te.created_at DESC NULLS LAST, te.id DESC
   LIMIT 1
   FOR UPDATE;

  IF v_existing_same.id IS NOT NULL THEN
    v_evidence_id := v_existing_same.id;
  ELSE
    v_display_name := COALESCE(NULLIF(BTRIM(v_queue.original_filename), ''), split_part(v_queue_storage_key, '/', array_length(string_to_array(v_queue_storage_key, '/'), 1)));
    INSERT INTO public.timesheet_evidence(timesheet_id, kind, display_name, storage_key, created_at, created_by)
    VALUES (v_ts.timesheet_id, v_kind, v_display_name, v_queue_storage_key, v_now, COALESCE(p_actor_user_id, v_queue.uploaded_by_user_id))
    RETURNING id INTO v_evidence_id;
  END IF;

  v_rotation_raw := ((COALESCE(v_queue.last_rotation_deg, 0)::integer % 360) + 360) % 360;
  v_rotation_degrees := CASE
    WHEN v_rotation_raw >= 315 OR v_rotation_raw < 45 THEN 0
    WHEN v_rotation_raw >= 45 AND v_rotation_raw < 135 THEN 90
    WHEN v_rotation_raw >= 135 AND v_rotation_raw < 225 THEN 180
    ELSE 270
  END;

  UPDATE public.manual_timesheet_queue AS mq
     SET status = 'ATTACHED',
         timesheet_id = v_ts.timesheet_id,
         r2_key = v_queue_storage_key,
         meta_json = COALESCE(mq.meta_json, '{}'::jsonb)
           || jsonb_build_object(
             'attached_kind', v_kind,
             'attached_to_timesheet_id', v_ts.timesheet_id::text,
             'attached_at_utc', v_now,
             'attached_storage_key', v_queue_storage_key,
             'attached_from_process', COALESCE((p_source_json ->> 'source') = 'bulk_process_displayed_queue_preview', false),
             'process_claimed_at_utc', v_now,
             'preview_selection_key', COALESCE(p_source_json ->> 'preview_selection_key', p_source_json ->> 'previewSelectionKey'),
             'preview_identity', COALESCE(p_source_json ->> 'preview_identity', p_source_json ->> 'previewIdentity'),
             'active_identity', COALESCE(p_source_json ->> 'active_identity', p_source_json ->> 'activeIdentity')
           )
   WHERE mq.id = p_queue_id
     AND mq.status = 'QUEUED'
     AND mq.timesheet_id IS NULL
     AND NULLIF(regexp_replace(COALESCE(mq.r2_key, ''), '^/+', ''), '') = v_queue_storage_key;

  IF NOT FOUND THEN
    RAISE EXCEPTION USING MESSAGE = 'QUEUE_ITEM_NOT_AVAILABLE', DETAIL = jsonb_build_object('queue_id', p_queue_id, 'storage_key', v_queue_storage_key, 'reason', 'conditional_attach_failed')::text;
  END IF;

  IF v_kind = 'TIMESHEET' THEN
    UPDATE public.timesheets AS ts
       SET manual_pdf_r2_key = v_queue_storage_key,
           manual_pdf_rotation_degrees = v_rotation_degrees,
           updated_at = v_now
     WHERE ts.timesheet_id = v_ts.timesheet_id
       AND ts.is_current = true;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'attached', true,
    'queue_id', p_queue_id,
    'id', p_queue_id,
    'evidence_id', v_evidence_id,
    'timesheet_id', v_ts.timesheet_id,
    'current_timesheet_id', v_ts.timesheet_id,
    'kind', v_kind,
    'storage_key', v_queue_storage_key,
    'queue_item_consumed', true,
    'consumed_queue_item', true,
    'changed_domains', jsonb_build_array('timesheet_evidence', 'manual_timesheet_queue', 'timesheets')
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT, v_error_detail = PG_EXCEPTION_DETAIL;
  IF v_error_state IN ('55P03', '57014') THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', 'LOCK_TIMEOUT', 'message', 'LOCK_TIMEOUT', 'sqlstate', v_error_state, 'detail', v_error_detail, 'queue_id', p_queue_id, 'timesheet_id', p_timesheet_id);
  END IF;
  IF v_error_message IN ('QUEUE_ITEM_NOT_AVAILABLE', 'QUEUE_ITEM_STORAGE_MISMATCH', 'PREVIEW_QUEUE_IMAGE_MISSING') THEN
    RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', v_error_message, 'message', v_error_message, 'sqlstate', v_error_state, 'detail', v_error_detail, 'queue_id', p_queue_id, 'timesheet_id', p_timesheet_id);
  END IF;
  RETURN jsonb_build_object('ok', false, 'success', false, 'error_code', COALESCE(NULLIF(v_error_message, ''), v_error_state), 'message', COALESCE(NULLIF(v_error_message, ''), 'Failed to attach queue item.'), 'sqlstate', v_error_state, 'detail', v_error_detail, 'queue_id', p_queue_id, 'timesheet_id', p_timesheet_id);
END;
$function$;
