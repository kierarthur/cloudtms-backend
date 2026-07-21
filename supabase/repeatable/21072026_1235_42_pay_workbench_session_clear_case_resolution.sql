-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 0d8bbaad604d.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.pay_workbench_session_clear_case_resolution(p_session_id uuid, p_actor_user_id uuid, p_resolution_payload_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_resolution_payload_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_resolution_payload_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_resolution_payload_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_operation text := 'CLEAR';
  v_resolution_family text := '';
  v_candidate_id uuid := NULL::uuid;
  v_candidate_id_text text := '';
  v_case_key text := '';
  v_linked_timesheet_id uuid := NULL::uuid;
  v_linked_timesheet_id_text text := '';
  v_finance_case_id_text text := '';
  v_expected_session_version bigint := NULL::bigint;
  v_expected_session_version_text text := '';
  v_expected_progress_counter_version bigint := NULL::bigint;
  v_expected_progress_counter_version_text text := '';
  v_candidate_in_scope boolean := false;
  v_candidate_has_scope_row boolean := false;
  v_scope_row_inserted integer := 0;
  v_next_scope_ordinal bigint := 0;
  v_matching_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_matching_candidate_count integer := 0;
  v_selected_timesheet_count integer := 0;
  v_matched_selected_timesheet_count integer := 0;
  v_explicit_bulk_request boolean := false;
  v_whole_timesheet_mode boolean := false;
  v_strict_selection_validation boolean := false;
  v_candidate_resolution_row_count integer := 0;
  v_candidate_preview_row_count integer := 0;
  v_eligible_timesheet_count integer := 0;
  v_max_selected_timesheets constant integer := 500;
  v_max_candidate_resolution_rows constant integer := 10000;
  v_max_candidate_preview_rows constant integer := 20000;
  v_max_clearable_timesheets constant integer := 1000;
  v_max_evidence_components_per_row constant integer := 500;
  v_new_session_version bigint := 0;
  v_new_progress_counter_version bigint := 0;
  v_job_json jsonb := '{}'::jsonb;
  v_job_id_text text := '';
  v_job_id uuid := NULL::uuid;
  v_case_resolution_ids jsonb := '[]'::jsonb;
  v_resolution_identity_keys jsonb := '[]'::jsonb;
  v_case_resolution_id_text text := NULL::text;
  v_selected_timesheet_ids_json jsonb := '[]'::jsonb;
  v_deleted_count integer := 0;
  v_clearable_timesheets_json jsonb := '[]'::jsonb;
  v_actor_display text := NULL::text;
  v_actor_role text := NULL::text;
  v_anchor_timesheet_id uuid := NULL::uuid;
  v_anchor_case_key text := '';
  v_clearable_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_clearable_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_excluded_linked_timesheets jsonb := '[]'::jsonb;
  v_excluded_linked_timesheet_count integer := 0;
  v_eligible_linked_timesheet_count integer := 0;
  v_total_affected_timesheet_count integer := 0;
  v_anchor_component_count integer := 0;
  v_finance_case_id uuid := NULL::uuid;
  v_finance_case public.pay_advances%ROWTYPE;
  v_finance_component_before_json jsonb := '[]'::jsonb;
  v_finance_component_after_json jsonb := '[]'::jsonb;
  v_finance_cleared_component_ids jsonb := '[]'::jsonb;
  v_finance_cleared_component_count integer := 0;
  v_stale_batch_record record;
  v_stale_batch_signal_count integer := 0;
  v_stale_batch_ids jsonb := '[]'::jsonb;
  v_stale_batch_item_ids jsonb := '[]'::jsonb;
  v_stale_batch_touch_json jsonb := '{}'::jsonb;
BEGIN
  p_resolution_payload_json:=public._ctms_enrich_correction_resolution_payload_v1(p_session_id,p_resolution_payload_json);
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'actor_user_id is required';
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_user
  WHERE actor_user.id = p_actor_user_id
    AND COALESCE(actor_user.is_active, false) = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'tms_users row % not found or inactive', p_actor_user_id;
  END IF;

  v_operation := UPPER(BTRIM(COALESCE(
    v_resolution_payload_json->>'operation',
    v_resolution_payload_json->>'action',
    'CLEAR'
  )));
  IF v_operation IN ('LIST', 'LIST_CLEARABLE', 'LIST_CLEARABLE_RESOLVED_RATES', 'LIST_RESOLVED_TIMESHEETS') THEN
    v_operation := 'LIST_CLEARABLE';
  ELSIF v_operation IN ('', 'CLEAR', 'CLEAR_RESOLUTION', 'CLEAR_RESOLVED_RATE', 'BULK_CLEAR') THEN
    v_operation := 'CLEAR';
  ELSE
    RAISE EXCEPTION 'unsupported case-resolution clear operation %', v_operation;
  END IF;

  IF v_operation = 'LIST_CLEARABLE' THEN
    SELECT session_row.*
    INTO v_session_row
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = p_session_id;
  ELSE
    SELECT session_row.*
    INTO v_session_row
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = p_session_id
    FOR UPDATE;
  END IF;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_sessions row % not found', p_session_id;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL
     OR v_session_row.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'banking_pay_workbench_session % is not current and OPEN', p_session_id;
  END IF;

  v_expected_session_version_text := BTRIM(COALESCE(
    v_resolution_payload_json->>'expected_session_version',
    v_resolution_payload_json->>'expectedSessionVersion',
    v_resolution_payload_json->>'session_version',
    v_resolution_payload_json->>'sessionVersion',
    ''
  ));
  v_expected_progress_counter_version_text := BTRIM(COALESCE(
    v_resolution_payload_json->>'expected_progress_counter_version',
    v_resolution_payload_json->>'expectedProgressCounterVersion',
    v_resolution_payload_json->>'progress_counter_version',
    v_resolution_payload_json->>'progressCounterVersion',
    ''
  ));

  IF v_operation <> 'LIST_CLEARABLE' THEN
    IF v_expected_session_version_text = '' THEN
      RAISE EXCEPTION 'expected_session_version is required to clear a shared Banking Pay case resolution'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'WORKBENCH_SESSION_VERSION_CONTEXT_REQUIRED', 'session_id', p_session_id::text)::text;
    END IF;
    IF v_expected_progress_counter_version_text = '' THEN
      RAISE EXCEPTION 'expected_progress_counter_version is required to clear a shared Banking Pay case resolution'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'WORKBENCH_SESSION_PROGRESS_CONTEXT_REQUIRED', 'session_id', p_session_id::text)::text;
    END IF;
  END IF;

  IF v_expected_session_version_text <> '' THEN
    IF v_expected_session_version_text !~ '^[0-9]{1,18}$' THEN
      RAISE EXCEPTION 'expected_session_version must be a positive integer'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'WORKBENCH_MODAL_ACTION_INVALID_SESSION_VERSION', 'session_id', p_session_id::text)::text;
    END IF;
    v_expected_session_version := v_expected_session_version_text::bigint;
    IF v_expected_session_version < 1 THEN
      RAISE EXCEPTION 'expected_session_version must be a positive integer'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'WORKBENCH_MODAL_ACTION_INVALID_SESSION_VERSION', 'session_id', p_session_id::text)::text;
    END IF;
    IF COALESCE(v_session_row.version, 0) <> v_expected_session_version THEN
      RAISE EXCEPTION 'workbench session % version changed from % to %', p_session_id, v_expected_session_version, v_session_row.version
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'STALE_SESSION', 'session_id', p_session_id::text, 'expected_session_version', v_expected_session_version, 'current_session_version', v_session_row.version)::text;
    END IF;
  END IF;

  IF v_expected_progress_counter_version_text <> '' THEN
    IF v_expected_progress_counter_version_text !~ '^[0-9]{1,18}$' THEN
      RAISE EXCEPTION 'expected_progress_counter_version must be a non-negative integer'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'WORKBENCH_MODAL_ACTION_INVALID_PROGRESS_COUNTER_VERSION', 'session_id', p_session_id::text)::text;
    END IF;
    v_expected_progress_counter_version := v_expected_progress_counter_version_text::bigint;
    IF v_expected_progress_counter_version < 0 THEN
      RAISE EXCEPTION 'expected_progress_counter_version must be a non-negative integer'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'WORKBENCH_MODAL_ACTION_INVALID_PROGRESS_COUNTER_VERSION', 'session_id', p_session_id::text)::text;
    END IF;
    IF COALESCE(v_session_row.progress_counter_version, 0) <> v_expected_progress_counter_version THEN
      RAISE EXCEPTION 'workbench session % progress counter changed from % to %', p_session_id, v_expected_progress_counter_version, v_session_row.progress_counter_version
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'WORKBENCH_SESSION_PROGRESS_CHANGED', 'session_id', p_session_id::text, 'expected_progress_counter_version', v_expected_progress_counter_version, 'current_progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0))::text;
    END IF;
  END IF;

  v_resolution_family := UPPER(BTRIM(COALESCE(
    v_resolution_payload_json->>'resolution_family',
    v_resolution_payload_json #>> '{case,resolution_family}',
    ''
  )));

  IF v_resolution_family = 'TAXABLE_CHANNEL' THEN
    v_resolution_family := 'TAXABLE_CHANNEL_RESTRUCTURE';
  END IF;

  v_candidate_id_text := BTRIM(COALESCE(
    v_resolution_payload_json->>'candidate_id',
    v_resolution_payload_json #>> '{case,candidate_id}',
    ''
  ));
  IF v_candidate_id_text <> '' THEN
    IF v_candidate_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'candidate_id is invalid';
    END IF;
    v_candidate_id := v_candidate_id_text::uuid;
  END IF;

  v_case_key := BTRIM(COALESCE(
    v_resolution_payload_json->>'case_key',
    v_resolution_payload_json #>> '{case,case_key}',
    ''
  ));

  v_linked_timesheet_id_text := BTRIM(COALESCE(
    v_resolution_payload_json->>'linked_timesheet_id',
    v_resolution_payload_json->>'timesheet_id',
    ''
  ));
  IF v_linked_timesheet_id_text <> '' THEN
    IF v_linked_timesheet_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'timesheet_id is invalid';
    END IF;
    v_linked_timesheet_id := v_linked_timesheet_id_text::uuid;
  END IF;

  v_finance_case_id_text := BTRIM(COALESCE(
    v_resolution_payload_json->>'finance_case_id',
    v_resolution_payload_json #>> '{case,finance_case_id}',
    ''
  ));

  IF v_finance_case_id_text <> ''
     AND v_resolution_family = '' THEN
    RAISE EXCEPTION 'resolution_family is required when clearing a finance case resolution'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'WORKBENCH_FINANCE_RESOLUTION_FAMILY_REQUIRED',
        'session_id', p_session_id::text,
        'finance_case_id', v_finance_case_id_text,
        'message', 'Refresh Banking Pay and try Cancel Resolve again. The finance case resolution family was not supplied.'
      )::text;
  END IF;

  IF v_resolution_family IN ('TAXABLE_CHANNEL_RESTRUCTURE', 'TAXABLE_CHANNEL')
     AND v_finance_case_id_text = '' THEN
    RAISE EXCEPTION 'finance_case_id is required to clear a taxable finance case resolution'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'WORKBENCH_TAXABLE_FINANCE_CASE_ID_REQUIRED',
        'session_id', p_session_id::text,
        'candidate_id', CASE WHEN v_candidate_id IS NULL THEN NULL ELSE v_candidate_id::text END,
        'resolution_family', v_resolution_family,
        'message', 'Refresh Banking Pay and try Cancel Resolve again. The finance case could not be identified.'
      )::text;
  END IF;

  IF v_resolution_family IN ('TAXABLE_CHANNEL_RESTRUCTURE', 'TAXABLE_CHANNEL')
     AND v_finance_case_id_text <> '' THEN
    IF v_finance_case_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'finance_case_id is invalid';
    END IF;

    v_finance_case_id := v_finance_case_id_text::uuid;

    IF v_operation = 'LIST_CLEARABLE' THEN
      SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'finance_case_id', finance_case.id::text,
               'candidate_id', finance_case.candidate_id::text,
               'case_key', COALESCE(NULLIF(v_case_key, ''), 'finance_case:' || finance_case.id::text),
               'resolution_family', v_resolution_family,
               'clearable', true
             )), '[]'::jsonb)
      INTO v_clearable_timesheets_json
      FROM public.pay_advances AS finance_case
      WHERE finance_case.id = v_finance_case_id
        AND EXISTS (
          SELECT 1
          FROM public.pay_finance_case_components AS finance_component
          WHERE finance_component.finance_case_id = finance_case.id
            AND finance_component.closed_at_utc IS NULL
            AND finance_component.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            AND (
              finance_component.saved_target_pay_method IS NOT NULL
              OR finance_component.saved_resolution_mode IS NOT NULL
              OR finance_component.saved_resolution_payload_json IS NOT NULL
              OR finance_component.saved_resolution_result_json IS NOT NULL
              OR finance_component.resolution_fingerprint IS NOT NULL
              OR COALESCE(finance_component.is_resolution_stale, false) = true
            )
        );

      RETURN jsonb_build_object(
        'ok', true,
        'operation', 'LIST_CLEARABLE',
        'session_id', p_session_id::text,
        'candidate_id', CASE WHEN v_candidate_id IS NULL THEN NULL ELSE v_candidate_id::text END,
        'session_version', v_session_row.version,
        'progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
        'clearable_rows', COALESCE(v_clearable_timesheets_json, '[]'::jsonb)
      );
    END IF;

    PERFORM pg_advisory_xact_lock(94201, 1);

    SELECT finance_case.*
    INTO v_finance_case
    FROM public.pay_advances AS finance_case
    WHERE finance_case.id = v_finance_case_id
    FOR UPDATE;

    IF v_finance_case.id IS NULL THEN
      RAISE EXCEPTION 'finance case % not found', v_finance_case_id;
    END IF;

    IF v_candidate_id IS NULL THEN
      v_candidate_id := v_finance_case.candidate_id;
    ELSIF v_candidate_id IS DISTINCT FROM v_finance_case.candidate_id THEN
      RAISE EXCEPTION 'finance case % does not belong to candidate %', v_finance_case_id, v_candidate_id;
    END IF;

    IF COALESCE(v_finance_case.case_type::text, '') NOT IN (
      'OVERPAYMENT',
      'UNDERPAYMENT',
      'MANUAL_DEBT_ADJUSTMENT',
      'MANUAL_CREDIT_ADJUSTMENT'
    ) THEN
      RAISE EXCEPTION 'finance case % is not a taxable finance case-resolution case', v_finance_case_id;
    END IF;

    SELECT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE scope_row.session_id = p_session_id
        AND scope_row.candidate_id = v_candidate_id
    )
    INTO v_candidate_has_scope_row;

    IF NOT COALESCE(v_candidate_has_scope_row, false) THEN
      SELECT COALESCE(MAX(scope_row.scope_ordinal), -1) + 1
      INTO v_next_scope_ordinal
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE scope_row.session_id = p_session_id;

      INSERT INTO public.banking_pay_workbench_session_scope(
        session_id,
        candidate_id,
        scope_ordinal,
        status,
        pending_job_id,
        seeded,
        dirty,
        error_json,
        created_at_utc,
        updated_at_utc
      )
      VALUES (
        p_session_id,
        v_candidate_id,
        v_next_scope_ordinal,
        'READY',
        NULL::uuid,
        true,
        false,
        NULL::jsonb,
        v_now,
        v_now
      )
      ON CONFLICT (session_id, candidate_id) DO NOTHING;
    END IF;

    /*
     * Keep the taxable finance clear path aligned with the apply path and
     * Draft creation by locking the current case components in a deterministic
     * order before any live resolution state is cleared.  Fixed components are
     * locked as part of the case scope but remain untouched by the UPDATE below.
     */
    FOR v_stale_batch_record IN
      SELECT component_lock.id AS finance_component_id
      FROM public.pay_finance_case_components AS component_lock
      WHERE component_lock.finance_case_id = v_finance_case_id
        AND component_lock.closed_at_utc IS NULL
      ORDER BY component_lock.id
      FOR UPDATE
    LOOP
      NULL;
    END LOOP;

    v_finance_component_before_json := COALESCE((
      SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
               'finance_component_id', component_row.id::text,
               'classification', component_row.classification::text,
               'component_key_type', component_row.component_key_type,
               'component_key_value', component_row.component_key_value,
               'source_pay_method', component_row.source_pay_method,
               'source_amount', ROUND(COALESCE(component_row.source_amount, 0), 2),
               'remaining_source_amount', ROUND(COALESCE(component_row.remaining_source_amount, 0), 2),
               'saved_target_pay_method', component_row.saved_target_pay_method,
               'saved_resolution_mode', CASE WHEN component_row.saved_resolution_mode IS NULL THEN NULL ELSE component_row.saved_resolution_mode::text END,
               'saved_resolution_payload_json', component_row.saved_resolution_payload_json,
               'saved_resolution_result_json', component_row.saved_resolution_result_json,
               'resolution_fingerprint', component_row.resolution_fingerprint,
               'is_resolution_stale', COALESCE(component_row.is_resolution_stale, false),
               'stale_reason', component_row.stale_reason
             )) ORDER BY component_row.allocation_priority_group NULLS LAST, component_row.allocation_priority_order NULLS LAST, component_row.id)
      FROM public.pay_finance_case_components AS component_row
      WHERE component_row.finance_case_id = v_finance_case_id
        AND component_row.closed_at_utc IS NULL
    ), '[]'::jsonb);

    v_finance_cleared_component_count := 0;
    v_finance_cleared_component_ids := '[]'::jsonb;
    FOR v_stale_batch_record IN
      UPDATE public.pay_finance_case_components AS component_row
      SET saved_target_pay_method = NULL,
          saved_resolution_mode = NULL,
          saved_resolution_payload_json = NULL,
          saved_resolution_result_json = NULL,
          resolution_fingerprint = NULL,
          is_resolution_stale = false,
          stale_reason = NULL,
          resolved_at_utc = NULL,
          updated_at_utc = v_now
      WHERE component_row.finance_case_id = v_finance_case_id
        AND component_row.closed_at_utc IS NULL
        AND component_row.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
        AND (
          component_row.saved_target_pay_method IS NOT NULL
          OR component_row.saved_resolution_mode IS NOT NULL
          OR component_row.saved_resolution_payload_json IS NOT NULL
          OR component_row.saved_resolution_result_json IS NOT NULL
          OR component_row.resolution_fingerprint IS NOT NULL
          OR COALESCE(component_row.is_resolution_stale, false) = true
        )
      RETURNING component_row.id AS finance_component_id
    LOOP
      v_finance_cleared_component_count := COALESCE(v_finance_cleared_component_count, 0) + 1;
      v_finance_cleared_component_ids := COALESCE(v_finance_cleared_component_ids, '[]'::jsonb)
        || jsonb_build_array(v_stale_batch_record.finance_component_id::text);
    END LOOP;

    v_deleted_count := 0;
    v_case_resolution_ids := '[]'::jsonb;
    v_resolution_identity_keys := '[]'::jsonb;
    v_case_resolution_id_text := NULL::text;
    FOR v_stale_batch_record IN
      DELETE FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
      WHERE resolution_row.session_id = p_session_id
        AND resolution_row.candidate_id = v_candidate_id
        AND (
          v_resolution_family = ''
          OR UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) = v_resolution_family
        )
        AND (
          BTRIM(COALESCE(resolution_row.payload_json->>'finance_case_id', '')) = v_finance_case_id_text
          OR (
            v_case_key <> ''
            AND resolution_row.case_key = v_case_key
          )
        )
      RETURNING resolution_row.id::text AS id_text,
                resolution_row.resolution_identity_key AS identity_key
    LOOP
      v_deleted_count := COALESCE(v_deleted_count, 0) + 1;
      v_case_resolution_ids := COALESCE(v_case_resolution_ids, '[]'::jsonb)
        || jsonb_build_array(v_stale_batch_record.id_text);
      IF v_stale_batch_record.identity_key IS NOT NULL THEN
        v_resolution_identity_keys := COALESCE(v_resolution_identity_keys, '[]'::jsonb)
          || jsonb_build_array(v_stale_batch_record.identity_key);
      END IF;
      IF v_case_resolution_id_text IS NULL OR v_stale_batch_record.id_text < v_case_resolution_id_text THEN
        v_case_resolution_id_text := v_stale_batch_record.id_text;
      END IF;
    END LOOP;

    v_finance_component_after_json := COALESCE((
      SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
               'finance_component_id', component_row.id::text,
               'classification', component_row.classification::text,
               'component_key_type', component_row.component_key_type,
               'component_key_value', component_row.component_key_value,
               'source_pay_method', component_row.source_pay_method,
               'source_amount', ROUND(COALESCE(component_row.source_amount, 0), 2),
               'remaining_source_amount', ROUND(COALESCE(component_row.remaining_source_amount, 0), 2),
               'saved_target_pay_method', component_row.saved_target_pay_method,
               'saved_resolution_mode', CASE WHEN component_row.saved_resolution_mode IS NULL THEN NULL ELSE component_row.saved_resolution_mode::text END,
               'saved_resolution_payload_json', component_row.saved_resolution_payload_json,
               'saved_resolution_result_json', component_row.saved_resolution_result_json,
               'resolution_fingerprint', component_row.resolution_fingerprint,
               'is_resolution_stale', COALESCE(component_row.is_resolution_stale, false),
               'stale_reason', component_row.stale_reason
             )) ORDER BY component_row.allocation_priority_group NULLS LAST, component_row.allocation_priority_order NULLS LAST, component_row.id)
      FROM public.pay_finance_case_components AS component_row
      WHERE component_row.finance_case_id = v_finance_case_id
        AND component_row.closed_at_utc IS NULL
    ), '[]'::jsonb);

    IF COALESCE(v_finance_cleared_component_count, 0) = 0 AND COALESCE(v_deleted_count, 0) = 0 THEN
      RETURN jsonb_build_object(
        'ok', true,
        'operation', 'CLEAR',
        'session_id', p_session_id::text,
        'candidate_id', v_candidate_id::text,
        'finance_case_id', v_finance_case_id::text,
        'session_version', v_session_row.version,
        'progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
        'job_id', NULL::text,
        'case_resolution_id', NULL::text,
        'case_resolution_ids', '[]'::jsonb,
        'resolution_identity_keys', '[]'::jsonb,
        'deleted_count', 0,
        'cleared_component_count', 0,
        'cleared', false,
        'state_changed', false,
        'no_op', true
      );
    END IF;

    INSERT INTO public.pay_finance_case_events(
      finance_case_id,
      finance_component_id,
      event_type,
      event_at_utc,
      actor_user_id,
      pay_batch_id,
      reservation_id,
      before_json,
      after_json,
      reason,
      note
    )
    VALUES (
      v_finance_case_id,
      NULL::uuid,
      'TAXABLE_CHANNEL_RESTRUCTURE_CLEARED',
      v_now,
      p_actor_user_id,
      NULL::uuid,
      NULL::uuid,
      jsonb_build_object(
        'finance_case_id', v_finance_case_id::text,
        'components', v_finance_component_before_json
      ),
      jsonb_build_object(
        'finance_case_id', v_finance_case_id::text,
        'cleared_component_ids', COALESCE(v_finance_cleared_component_ids, '[]'::jsonb),
        'components', v_finance_component_after_json
      ),
      'TAXABLE_CHANNEL_RESTRUCTURE_CLEAR',
      'Cleared current live taxable finance case resolution without changing case balance, reservations, settled history, Snooze, fixed components, or frozen Draft items.'
    );

    FOR v_stale_batch_record IN
      SELECT
        batch_row.id AS pay_batch_id,
        COALESCE(jsonb_agg(DISTINCT batch_item.id::text ORDER BY batch_item.id::text), '[]'::jsonb) AS pay_batch_item_ids,
        COUNT(DISTINCT batch_item.id)::integer AS pay_batch_item_count
      FROM public.pay_batch_items AS batch_item
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = batch_item.pay_batch_candidate_id
      JOIN public.pay_batches AS batch_row
        ON batch_row.id = batch_candidate.pay_batch_id
      WHERE batch_candidate.candidate_id = v_candidate_id
        AND COALESCE(batch_item.is_voided, false) = false
        AND batch_row.cancelled_at_utc IS NULL
        AND public._pay_batch_status_is_active_reservation(batch_row.status)
        AND (
          batch_item.finance_case_id = v_finance_case_id
          OR EXISTS (
            SELECT 1
            FROM public.pay_finance_case_components AS draft_component_scope
            WHERE draft_component_scope.finance_case_id = v_finance_case_id
              AND draft_component_scope.id = batch_item.finance_component_id
          )
        )
      GROUP BY batch_row.id
      ORDER BY batch_row.id
    LOOP
      v_stale_batch_touch_json := public.banking_pay_batch_signal_touch(
        p_pay_batch_id => v_stale_batch_record.pay_batch_id,
        p_change_reason => 'CASE_RESOLUTION_CLEARED',
        p_change_source => 'WORKBENCH_CASE_RESOLUTION_CLEAR',
        p_change_scope_json => jsonb_build_object(
          'stale_hint', true,
          'stale_reason', 'CASE_RESOLUTION_CLEARED',
          'policy_x_authority_scope', 'FROZEN_DRAFT_CURRENT_LIVE_RESOLUTION_CLEARED',
          'finance_case_id', v_finance_case_id::text,
          'candidate_ids', jsonb_build_array(v_candidate_id::text),
          'pay_batch_item_ids', COALESCE(v_stale_batch_record.pay_batch_item_ids, '[]'::jsonb),
          'pay_batch_item_count', COALESCE(v_stale_batch_record.pay_batch_item_count, 0),
          'cleared_component_ids', COALESCE(v_finance_cleared_component_ids, '[]'::jsonb),
          'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE'
        ),
        p_touch_payment_status => false,
        p_touch_correction_progress => false,
        p_touch_alerts => false,
        p_touch_overview => true
      );

      v_stale_batch_signal_count := COALESCE(v_stale_batch_signal_count, 0) + 1;
      v_stale_batch_ids := COALESCE(v_stale_batch_ids, '[]'::jsonb) || jsonb_build_array(v_stale_batch_record.pay_batch_id::text);
      v_stale_batch_item_ids := COALESCE(v_stale_batch_item_ids, '[]'::jsonb) || COALESCE(v_stale_batch_record.pay_batch_item_ids, '[]'::jsonb);
    END LOOP;

    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET version = session_update.version + 1,
        progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_update.id = p_session_id
    RETURNING session_update.version, session_update.progress_counter_version
    INTO v_new_session_version, v_new_progress_counter_version;

    v_job_json := public.pay_workbench_enqueue_session_candidate_refresh(
      p_session_id => p_session_id,
      p_candidate_id => v_candidate_id,
      p_reason => 'SESSION_CASE_RESOLUTION_CLEARED',
      p_actor_user_id => p_actor_user_id,
      p_payload_json => jsonb_build_object(
        'case_key', NULLIF(v_case_key, ''),
        'finance_case_id', v_finance_case_id::text,
        'linked_timesheet_id', CASE WHEN v_finance_case.linked_timesheet_id IS NULL THEN NULL ELSE v_finance_case.linked_timesheet_id::text END,
        'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE',
        'resolution_identity_keys', COALESCE(v_resolution_identity_keys, '[]'::jsonb),
        'deleted_count', v_deleted_count,
        'cleared_component_count', v_finance_cleared_component_count,
        'force_legacy', true,
        'projection_mode', 'LEGACY',
        'projection_class', 'CASE_RESOLUTION',
        'fallback_reason', 'CASE_RESOLUTION_CHANGED',
        'refresh_scope_kind', CASE WHEN v_finance_case.linked_timesheet_id IS NULL THEN 'CANDIDATE_FULL_LIVE' ELSE 'TARGETED_TIMESHEETS' END,
        'targeted_timesheet_ids', CASE WHEN v_finance_case.linked_timesheet_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(v_finance_case.linked_timesheet_id::text) END,
        'linked_timesheet_ids', CASE WHEN v_finance_case.linked_timesheet_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(v_finance_case.linked_timesheet_id::text) END,
        'source_build_required', true,
        'line_work_required', true,
        'delta_refresh_required', false,
        'complex_refresh_required', true,
        'resolved_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      )
    );

    v_job_id_text := BTRIM(COALESCE(
      v_job_json->>'job_id',
      v_job_json #>> '{enqueue_result,job_id}',
      v_job_json #>> '{enqueue_result,job_ids,0}',
      v_job_json #>> '{job_ids,0}',
      v_job_json #>> '{enqueue_result,session_recompute_job_ids,0}',
      v_job_json #>> '{session_recompute_job_ids,0}',
      ''
    ));
    IF v_job_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_job_id := v_job_id_text::uuid;
    END IF;
    IF v_job_id IS NULL THEN
      RAISE EXCEPTION 'candidate refresh enqueue did not return a durable job_id for session % candidate %', p_session_id, v_candidate_id;
    END IF;

    INSERT INTO public.audit_events(
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason,
      actor_user_id
    )
    VALUES (
      'pay_finance_case',
      v_finance_case_id::text,
      'TAXABLE_CHANNEL_RESTRUCTURE_CLEARED',
      jsonb_build_object('finance_case_id', v_finance_case_id::text, 'components', v_finance_component_before_json),
      jsonb_build_object('finance_case_id', v_finance_case_id::text, 'components', v_finance_component_after_json, 'session_version', v_new_session_version, 'progress_counter_version', v_new_progress_counter_version, 'pending_job_id', v_job_id::text),
      'SESSION_CASE_RESOLUTION_CLEARED',
      p_actor_user_id
    );

    RETURN jsonb_build_object(
      'ok', true,
      'operation', 'CLEAR',
      'session_id', p_session_id::text,
      'candidate_id', v_candidate_id::text,
      'finance_case_id', v_finance_case_id::text,
      'session_version', v_new_session_version,
      'progress_counter_version', v_new_progress_counter_version,
      'job_id', v_job_id::text,
      'case_resolution_id', v_case_resolution_id_text,
      'case_resolution_ids', COALESCE(v_case_resolution_ids, '[]'::jsonb),
      'resolution_identity_keys', COALESCE(v_resolution_identity_keys, '[]'::jsonb),
      'deleted_count', v_deleted_count,
      'cleared_component_count', v_finance_cleared_component_count,
      'cleared_component_ids', COALESCE(v_finance_cleared_component_ids, '[]'::jsonb),
      'draft_stale_signal_count', COALESCE(v_stale_batch_signal_count, 0),
      'draft_stale_batch_ids', COALESCE(v_stale_batch_ids, '[]'::jsonb),
      'draft_stale_pay_batch_item_ids', COALESCE(v_stale_batch_item_ids, '[]'::jsonb),
      'cleared', true,
      'state_changed', true,
      'no_op', false,
      'refresh_enqueue', COALESCE(v_job_json, '{}'::jsonb)
    );
  END IF;

  IF v_resolution_family = 'BUCKETED' THEN
    v_anchor_timesheet_id := v_linked_timesheet_id;
    IF v_anchor_timesheet_id IS NULL AND v_case_key ~* '^timesheet:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_anchor_timesheet_id := SUBSTRING(v_case_key FROM 11)::uuid;
    END IF;
    v_anchor_case_key := COALESCE(NULLIF(v_case_key, ''), CASE WHEN v_anchor_timesheet_id IS NULL THEN '' ELSE 'timesheet:' || v_anchor_timesheet_id::text END);

    IF v_candidate_id IS NULL THEN
      RAISE EXCEPTION 'WORKBENCH_RESOLUTION_CANDIDATE_REQUIRED'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'WORKBENCH_RESOLUTION_CANDIDATE_REQUIRED',
          'message', 'Candidate details are required to cancel the resolved rate.'
        )::text;
    END IF;
    IF v_anchor_timesheet_id IS NULL OR v_anchor_case_key = '' THEN
      RAISE EXCEPTION 'WORKBENCH_RESOLUTION_ANCHOR_REQUIRED'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'WORKBENCH_RESOLUTION_ANCHOR_REQUIRED',
          'message', 'The selected timesheet could not be identified. Refresh Banking Pay and try again.'
        )::text;
    END IF;

    IF v_operation = 'CLEAR' THEN
      -- Use the same lock as draft creation, then revalidate the exact anchor family.
      PERFORM pg_advisory_xact_lock(94201, 1);
    END IF;

    CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_clear_anchor_family_existing
    AS
    SELECT resolution_row.*
    FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
    WITH NO DATA;
    TRUNCATE TABLE _tmp_bpay_clear_anchor_family_existing;

    INSERT INTO _tmp_bpay_clear_anchor_family_existing
    SELECT resolution_row.*
    FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
    WHERE resolution_row.session_id = p_session_id
      AND resolution_row.candidate_id = v_candidate_id
      AND UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) = 'BUCKETED'
      AND (
        (
          resolution_row.timesheet_id = v_anchor_timesheet_id
          AND (
            resolution_row.case_key = v_anchor_case_key
            OR BTRIM(COALESCE(resolution_row.payload_json->>'source_anchor_timesheet_id', '')) = v_anchor_timesheet_id::text
          )
        )
        OR (
          resolution_row.timesheet_id IS NOT NULL
          AND resolution_row.timesheet_id <> v_anchor_timesheet_id
          AND BTRIM(COALESCE(resolution_row.payload_json->>'source_anchor_timesheet_id', '')) = v_anchor_timesheet_id::text
          AND BTRIM(COALESCE(resolution_row.payload_json->>'source_anchor_case_key', '')) = v_anchor_case_key
          AND LOWER(BTRIM(COALESCE(resolution_row.payload_json->>'applied_via_linked_scope', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        )
      );

    SELECT COUNT(*)::integer
    INTO v_anchor_component_count
    FROM _tmp_bpay_clear_anchor_family_existing AS family_row
    WHERE family_row.timesheet_id = v_anchor_timesheet_id;

    IF COALESCE(v_anchor_component_count, 0) = 0 THEN
      RAISE EXCEPTION 'WORKBENCH_RESOLUTION_ANCHOR_NOT_CLEARABLE'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'WORKBENCH_RESOLUTION_ANCHOR_NOT_CLEARABLE',
          'session_id', p_session_id::text,
          'candidate_id', v_candidate_id::text,
          'anchor_timesheet_id', v_anchor_timesheet_id::text,
          'message', 'The resolved rate is no longer available to cancel. Refresh Banking Pay and review the latest details.'
        )::text;
    END IF;

    CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_clear_batch_boundary (
      timesheet_id uuid NOT NULL,
      pay_batch_id uuid NOT NULL,
      batch_status text NOT NULL,
      PRIMARY KEY (timesheet_id, pay_batch_id)
    ) ON COMMIT DROP;
    TRUNCATE TABLE _tmp_bpay_clear_batch_boundary;

    INSERT INTO _tmp_bpay_clear_batch_boundary(timesheet_id, pay_batch_id, batch_status)
    SELECT DISTINCT boundary_rows.timesheet_id, boundary_rows.pay_batch_id, boundary_rows.batch_status
    FROM (
      SELECT
        batch_item.timesheet_id,
        batch_row.id AS pay_batch_id,
        UPPER(BTRIM(COALESCE(batch_row.status, ''))) AS batch_status
      FROM public.pay_batch_items AS batch_item
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = batch_item.pay_batch_candidate_id
      JOIN public.pay_batches AS batch_row
        ON batch_row.id = batch_candidate.pay_batch_id
      WHERE batch_item.timesheet_id IS NOT NULL
        AND COALESCE(batch_item.is_voided, false) = false
        AND EXISTS (
          SELECT 1 FROM _tmp_bpay_clear_anchor_family_existing AS family_row
          WHERE family_row.timesheet_id = batch_item.timesheet_id
        )
        AND UPPER(BTRIM(COALESCE(batch_row.status, ''))) NOT IN ('CANCELLED', 'CANCELED')

      UNION ALL

      SELECT
        batch_snapshot.timesheet_id,
        batch_row.id AS pay_batch_id,
        UPPER(BTRIM(COALESCE(batch_row.status, ''))) AS batch_status
      FROM public.pay_batch_timesheet_snapshots AS batch_snapshot
      JOIN public.pay_batches AS batch_row
        ON batch_row.id = batch_snapshot.pay_batch_id
      WHERE EXISTS (
          SELECT 1 FROM _tmp_bpay_clear_anchor_family_existing AS family_row
          WHERE family_row.timesheet_id = batch_snapshot.timesheet_id
        )
        AND UPPER(BTRIM(COALESCE(batch_row.status, ''))) NOT IN ('CANCELLED', 'CANCELED')
    ) AS boundary_rows
    ON CONFLICT (timesheet_id, pay_batch_id) DO UPDATE
      SET batch_status = EXCLUDED.batch_status;

    IF EXISTS (
      SELECT 1 FROM _tmp_bpay_clear_batch_boundary AS boundary_row
      WHERE boundary_row.timesheet_id = v_anchor_timesheet_id
    ) THEN
      RAISE EXCEPTION 'WORKBENCH_RESOLUTION_ANCHOR_FINANCIAL_BOUNDARY'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'WORKBENCH_RESOLUTION_ANCHOR_FINANCIAL_BOUNDARY',
          'session_id', p_session_id::text,
          'candidate_id', v_candidate_id::text,
          'anchor_timesheet_id', v_anchor_timesheet_id::text,
          'message', 'The payment details changed because this timesheet is now included in a payment batch. Refresh Banking Pay and review the latest details.'
        )::text;
    END IF;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'timesheet_id', excluded_scope.timesheet_id::text,
             'reason', 'ALREADY_IN_LIVE_BATCH',
             'batch_status', excluded_scope.batch_status
           ) ORDER BY excluded_scope.timesheet_id::text), '[]'::jsonb)
    INTO v_excluded_linked_timesheets
    FROM (
      SELECT boundary_row.timesheet_id, MIN(boundary_row.batch_status) AS batch_status
      FROM _tmp_bpay_clear_batch_boundary AS boundary_row
      WHERE boundary_row.timesheet_id <> v_anchor_timesheet_id
      GROUP BY boundary_row.timesheet_id
    ) AS excluded_scope;
    v_excluded_linked_timesheet_count := jsonb_array_length(COALESCE(v_excluded_linked_timesheets, '[]'::jsonb));

    DELETE FROM _tmp_bpay_clear_anchor_family_existing AS family_row
    WHERE family_row.timesheet_id <> v_anchor_timesheet_id
      AND EXISTS (
        SELECT 1 FROM _tmp_bpay_clear_batch_boundary AS boundary_row
        WHERE boundary_row.timesheet_id = family_row.timesheet_id
      );

    SELECT COALESCE(array_agg(DISTINCT family_row.timesheet_id ORDER BY family_row.timesheet_id), ARRAY[]::uuid[])
    INTO v_clearable_timesheet_ids
    FROM _tmp_bpay_clear_anchor_family_existing AS family_row
    WHERE family_row.timesheet_id IS NOT NULL;

    SELECT COALESCE(array_agg(DISTINCT family_row.timesheet_id ORDER BY family_row.timesheet_id), ARRAY[]::uuid[])
    INTO v_clearable_linked_timesheet_ids
    FROM _tmp_bpay_clear_anchor_family_existing AS family_row
    WHERE family_row.timesheet_id IS NOT NULL
      AND family_row.timesheet_id <> v_anchor_timesheet_id;

    v_eligible_linked_timesheet_count := COALESCE(array_length(v_clearable_linked_timesheet_ids, 1), 0);
    v_total_affected_timesheet_count := COALESCE(array_length(v_clearable_timesheet_ids, 1), 0);

    SELECT COUNT(*)::integer
    INTO v_deleted_count
    FROM _tmp_bpay_clear_anchor_family_existing;

    IF v_operation = 'LIST_CLEARABLE' THEN
      RETURN jsonb_build_object(
        'ok', true,
        'operation', 'LIST_CLEARABLE',
        'session_id', p_session_id::text,
        'session_version', v_session_row.version,
        'progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
        'candidate_id', v_candidate_id::text,
        'anchor_timesheet_id', v_anchor_timesheet_id::text,
        'anchor_case_key', v_anchor_case_key,
        'clearable_timesheet_ids', COALESCE(to_jsonb(v_clearable_timesheet_ids), '[]'::jsonb),
        'clearable_linked_timesheet_ids', COALESCE(to_jsonb(v_clearable_linked_timesheet_ids), '[]'::jsonb),
        'eligible_linked_timesheet_ids', COALESCE(to_jsonb(v_clearable_linked_timesheet_ids), '[]'::jsonb),
        'eligible_linked_timesheet_count', v_eligible_linked_timesheet_count,
        'total_affected_timesheet_count', v_total_affected_timesheet_count,
        'excluded_linked_timesheets', COALESCE(v_excluded_linked_timesheets, '[]'::jsonb),
        'excluded_linked_timesheet_count', v_excluded_linked_timesheet_count,
        'resolution_component_count', v_deleted_count,
        'state_changed', false,
        'job_id', NULL::text
      );
    END IF;

    IF v_deleted_count <= 0 THEN
      RAISE EXCEPTION 'WORKBENCH_RESOLUTION_ANCHOR_NOT_CLEARABLE'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'WORKBENCH_RESOLUTION_ANCHOR_NOT_CLEARABLE',
          'message', 'The resolved rate is no longer available to cancel. Refresh Banking Pay and review the latest details.'
        )::text;
    END IF;

    SELECT
      COALESCE(JSONB_AGG(family_row.id::text ORDER BY family_row.id::text), '[]'::jsonb),
      COALESCE(JSONB_AGG(family_row.resolution_identity_key ORDER BY family_row.resolution_identity_key), '[]'::jsonb),
      (ARRAY_AGG(family_row.id::text ORDER BY family_row.id))[1]
    INTO v_case_resolution_ids, v_resolution_identity_keys, v_case_resolution_id_text
    FROM _tmp_bpay_clear_anchor_family_existing AS family_row;

    DELETE FROM public.banking_pay_workbench_session_case_resolutions AS delete_resolution
    USING _tmp_bpay_clear_anchor_family_existing AS family_row
    WHERE delete_resolution.id = family_row.id;

    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET version = session_update.version + 1,
        progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_update.id = p_session_id
    RETURNING session_update.version, session_update.progress_counter_version
    INTO v_new_session_version, v_new_progress_counter_version;

    v_job_json := public.pay_workbench_enqueue_session_candidate_refresh(
      p_session_id => p_session_id,
      p_candidate_id => v_candidate_id,
      p_reason => 'SESSION_RESOLVED_RATE_ANCHOR_FAMILY_CLEARED',
      p_actor_user_id => p_actor_user_id,
      p_payload_json => jsonb_build_object(
        'case_key', v_anchor_case_key,
        'anchor_timesheet_id', v_anchor_timesheet_id::text,
        'selected_timesheet_ids', COALESCE(to_jsonb(v_clearable_timesheet_ids), '[]'::jsonb),
        'resolution_family', 'BUCKETED',
        'resolution_identity_keys', v_resolution_identity_keys,
        'deleted_count', v_deleted_count,
        'force_legacy', true,
        'projection_mode', 'LEGACY',
        'projection_class', 'CASE_RESOLUTION',
        'fallback_reason', 'CASE_RESOLUTION_CHANGED',
        'refresh_scope_kind', 'TARGETED_TIMESHEETS',
        'targeted_timesheet_ids', COALESCE(to_jsonb(v_clearable_timesheet_ids), '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(to_jsonb(v_clearable_linked_timesheet_ids), '[]'::jsonb),
        'source_build_required', true,
        'line_work_required', true,
        'delta_refresh_required', false,
        'complex_refresh_required', true,
        'resolved_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      )
    );

    v_job_id_text := BTRIM(COALESCE(
      v_job_json->>'job_id',
      v_job_json #>> '{enqueue_result,job_id}',
      v_job_json #>> '{enqueue_result,job_ids,0}',
      v_job_json #>> '{job_ids,0}',
      ''
    ));
    IF v_job_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_job_id := v_job_id_text::uuid;
    END IF;
    IF v_job_id IS NULL THEN
      RAISE EXCEPTION 'WORKBENCH_REFRESH_JOB_NOT_PROVEN'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
          'code', 'WORKBENCH_REFRESH_JOB_NOT_PROVEN',
          'session_id', p_session_id::text,
          'candidate_id', v_candidate_id::text,
          'message', 'The resolved rate was not cancelled because the required refresh could not be started.'
        )::text;
    END IF;

    SELECT
      NULLIF(BTRIM(COALESCE(actor_user.display_name, actor_user.email, '')), ''),
      NULLIF(BTRIM(COALESCE(actor_user.role, '')), '')
    INTO v_actor_display, v_actor_role
    FROM public.tms_users AS actor_user
    WHERE actor_user.id = p_actor_user_id
    LIMIT 1;

    INSERT INTO public.audit_events(
      object_type, object_id_text, action, before_json, after_json, reason,
      actor_user_id, actor_display, actor_role_at_time
    )
    SELECT
      'banking_pay_workbench_session_case_resolution',
      family_row.id::text,
      'SESSION_CASE_RESOLUTION_CLEARED',
      jsonb_build_object(
        'id', family_row.id::text,
        'session_id', family_row.session_id::text,
        'candidate_id', family_row.candidate_id::text,
        'case_key', family_row.case_key,
        'resolution_family', family_row.resolution_family,
        'resolution_identity_key', family_row.resolution_identity_key,
        'timesheet_id', family_row.timesheet_id::text,
        'payload_json', family_row.payload_json
      ),
      jsonb_build_object(
        'session_version', v_new_session_version,
        'progress_counter_version', v_new_progress_counter_version,
        'pending_job_id', v_job_id::text,
        'cleared_at_utc', v_now,
        'anchor_timesheet_id', v_anchor_timesheet_id::text,
        'anchor_case_key', v_anchor_case_key,
        'targeted_timesheet_ids', COALESCE(to_jsonb(v_clearable_timesheet_ids), '[]'::jsonb),
        'linked_timesheet_ids', COALESCE(to_jsonb(v_clearable_linked_timesheet_ids), '[]'::jsonb)
      ),
      'SESSION_RESOLVED_RATE_ANCHOR_FAMILY_CLEARED',
      p_actor_user_id,
      v_actor_display,
      v_actor_role
    FROM _tmp_bpay_clear_anchor_family_existing AS family_row;

    RETURN jsonb_build_object(
      'ok', true,
      'operation', 'CLEAR',
      'session_id', p_session_id::text,
      'candidate_id', v_candidate_id::text,
      'session_version', v_new_session_version,
      'progress_counter_version', v_new_progress_counter_version,
      'job_id', v_job_id::text,
      'anchor_timesheet_id', v_anchor_timesheet_id::text,
      'anchor_case_key', v_anchor_case_key,
      'eligible_linked_timesheet_ids', COALESCE(to_jsonb(v_clearable_linked_timesheet_ids), '[]'::jsonb),
      'eligible_linked_timesheet_count', v_eligible_linked_timesheet_count,
      'total_affected_timesheet_count', v_total_affected_timesheet_count,
      'excluded_linked_timesheets', COALESCE(v_excluded_linked_timesheets, '[]'::jsonb),
      'excluded_linked_timesheet_count', v_excluded_linked_timesheet_count,
      'case_resolution_ids', v_case_resolution_ids,
      'resolution_identity_keys', v_resolution_identity_keys,
      'deleted_count', v_deleted_count,
      'cleared', true,
      'targeted_refresh_enqueued', true,
      'refresh_scope_kind', 'TARGETED_TIMESHEETS',
      'targeted_timesheet_ids', COALESCE(to_jsonb(v_clearable_timesheet_ids), '[]'::jsonb),
      'linked_timesheet_ids', COALESCE(to_jsonb(v_clearable_linked_timesheet_ids), '[]'::jsonb),
      'enqueue_result', COALESCE(v_job_json, '{}'::jsonb),
      'state_changed', true,
      'no_op', false
    );
  END IF;

  IF v_operation = 'LIST_CLEARABLE' THEN
    IF v_candidate_id IS NULL THEN
      RAISE EXCEPTION 'candidate_id is required to list clearable resolved-rate timesheets';
    END IF;

    SELECT (
      EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_session_scope AS scope_row
        WHERE scope_row.session_id = p_session_id
          AND scope_row.candidate_id = v_candidate_id
      )
      OR EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS preview_row
        WHERE preview_row.session_id = p_session_id
          AND preview_row.candidate_id = v_candidate_id
      )
      OR EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_candidate_line_work AS line_work_row
        WHERE line_work_row.session_id = p_session_id
          AND line_work_row.candidate_id = v_candidate_id
      )
      OR EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
        WHERE resolution_row.session_id = p_session_id
          AND resolution_row.candidate_id = v_candidate_id
      )
      OR v_candidate_id = ANY(COALESCE(v_session_row.scope_candidate_ids, ARRAY[]::uuid[]))
    )
    INTO v_candidate_in_scope;

    IF NOT COALESCE(v_candidate_in_scope, false) THEN
      RAISE EXCEPTION 'candidate % is not in workbench session scope %', v_candidate_id, p_session_id;
    END IF;

    SELECT COUNT(*)::integer
    INTO v_candidate_resolution_row_count
    FROM (
      SELECT 1
      FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
      WHERE resolution_row.session_id = p_session_id
        AND resolution_row.candidate_id = v_candidate_id
        AND UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) = 'BUCKETED'
      LIMIT (v_max_candidate_resolution_rows + 1)
    ) AS bounded_resolution_rows;

    IF COALESCE(v_candidate_resolution_row_count, 0) > v_max_candidate_resolution_rows THEN
      RAISE EXCEPTION 'candidate % has too many resolved-rate rows to list safely in one request', v_candidate_id;
    END IF;

    SELECT COUNT(*)::integer
    INTO v_eligible_timesheet_count
    FROM (
      SELECT DISTINCT resolved_timesheet.resolved_timesheet_id
      FROM (
        SELECT CASE
          WHEN resolution_row.timesheet_id IS NOT NULL THEN resolution_row.timesheet_id
          WHEN BTRIM(COALESCE(resolution_row.payload_json->>'linked_timesheet_id', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN BTRIM(resolution_row.payload_json->>'linked_timesheet_id')::uuid
          WHEN BTRIM(COALESCE(resolution_row.payload_json->>'timesheet_id', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN BTRIM(resolution_row.payload_json->>'timesheet_id')::uuid
          ELSE NULL::uuid
        END AS resolved_timesheet_id
        FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
        WHERE resolution_row.session_id = p_session_id
          AND resolution_row.candidate_id = v_candidate_id
          AND UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) = 'BUCKETED'
      ) AS resolved_timesheet
      WHERE resolved_timesheet.resolved_timesheet_id IS NOT NULL
      LIMIT (v_max_clearable_timesheets + 1)
    ) AS bounded_clearable_timesheets;

    IF COALESCE(v_eligible_timesheet_count, 0) > v_max_clearable_timesheets THEN
      RAISE EXCEPTION 'candidate % has too many resolved-rate timesheets to list safely in one request', v_candidate_id;
    END IF;

    SELECT COUNT(*)::integer
    INTO v_candidate_preview_row_count
    FROM (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id = p_session_id
        AND preview_row.candidate_id = v_candidate_id
        AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
        AND (preview_row.session_version IS NULL OR preview_row.session_version = v_session_row.version)
      LIMIT (v_max_candidate_preview_rows + 1)
    ) AS bounded_preview_rows;

    IF COALESCE(v_candidate_preview_row_count, 0) > v_max_candidate_preview_rows THEN
      RAISE EXCEPTION 'candidate % has too many preview rows to list safely in one request', v_candidate_id;
    END IF;

    IF EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
      WHERE resolution_row.session_id = p_session_id
        AND resolution_row.candidate_id = v_candidate_id
        AND UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) = 'BUCKETED'
        AND jsonb_typeof(resolution_row.payload_json->'bucket_resolutions') = 'array'
        AND jsonb_array_length(resolution_row.payload_json->'bucket_resolutions') > v_max_evidence_components_per_row
    ) THEN
      RAISE EXCEPTION 'candidate % has a resolved-rate evidence row exceeding the safe component limit', v_candidate_id;
    END IF;

    IF EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id = p_session_id
        AND preview_row.candidate_id = v_candidate_id
        AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
        AND (preview_row.session_version IS NULL OR preview_row.session_version = v_session_row.version)
        AND jsonb_typeof(preview_row.row_json->'case_components') = 'array'
        AND jsonb_array_length(preview_row.row_json->'case_components') > v_max_evidence_components_per_row
    ) THEN
      RAISE EXCEPTION 'candidate % has a preview evidence row exceeding the safe component limit', v_candidate_id;
    END IF;

    WITH resolved_rows AS (
      SELECT
        resolution_row.*,
        CASE
          WHEN resolution_row.timesheet_id IS NOT NULL THEN resolution_row.timesheet_id
          WHEN BTRIM(COALESCE(resolution_row.payload_json->>'linked_timesheet_id', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN BTRIM(resolution_row.payload_json->>'linked_timesheet_id')::uuid
          WHEN BTRIM(COALESCE(resolution_row.payload_json->>'timesheet_id', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN BTRIM(resolution_row.payload_json->>'timesheet_id')::uuid
          ELSE NULL::uuid
        END AS resolved_timesheet_id
      FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
      WHERE resolution_row.session_id = p_session_id
        AND resolution_row.candidate_id = v_candidate_id
        AND UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) = 'BUCKETED'
    ),
    eligible_timesheets AS (
      SELECT
        resolved_row.resolved_timesheet_id AS timesheet_id,
        (ARRAY_AGG(resolved_row.case_key ORDER BY resolved_row.updated_at_utc DESC, resolved_row.id DESC))[1] AS case_key,
        (JSONB_AGG(resolved_row.payload_json ORDER BY resolved_row.updated_at_utc DESC, resolved_row.id DESC)->0) AS sample_payload_json,
        COALESCE(JSONB_AGG(resolved_row.id::text ORDER BY resolved_row.id::text), '[]'::jsonb) AS case_resolution_ids,
        COALESCE(JSONB_AGG(resolved_row.resolution_identity_key ORDER BY resolved_row.resolution_identity_key), '[]'::jsonb) AS resolution_identity_keys
      FROM resolved_rows AS resolved_row
      WHERE resolved_row.resolved_timesheet_id IS NOT NULL
      GROUP BY resolved_row.resolved_timesheet_id
    ),
    preview_candidates AS (
      SELECT
        CASE
          WHEN preview_row.timesheet_id IS NOT NULL THEN preview_row.timesheet_id
          WHEN BTRIM(COALESCE(preview_row.row_json->>'resolved_rate_timesheet_id', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN BTRIM(preview_row.row_json->>'resolved_rate_timesheet_id')::uuid
          WHEN BTRIM(COALESCE(preview_row.row_json->>'linked_timesheet_id', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN BTRIM(preview_row.row_json->>'linked_timesheet_id')::uuid
          WHEN BTRIM(COALESCE(preview_row.row_json->>'timesheet_id', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN BTRIM(preview_row.row_json->>'timesheet_id')::uuid
          ELSE NULL::uuid
        END AS timesheet_id,
        preview_row.section,
        preview_row.row_ordinal,
        preview_row.id,
        preview_row.row_json
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id = p_session_id
        AND preview_row.candidate_id = v_candidate_id
        AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
        AND (preview_row.session_version IS NULL OR preview_row.session_version = v_session_row.version)
    ),
    preview_meta AS (
      SELECT DISTINCT ON (preview_candidate.timesheet_id)
        preview_candidate.timesheet_id,
        preview_candidate.row_json
      FROM preview_candidates AS preview_candidate
      WHERE preview_candidate.timesheet_id IS NOT NULL
      ORDER BY
        preview_candidate.timesheet_id,
        CASE WHEN UPPER(BTRIM(COALESCE(preview_candidate.section, ''))) = 'READY_TO_PAY' THEN 0 ELSE 1 END,
        preview_candidate.row_ordinal,
        preview_candidate.id
    ),
    resolution_evidence_source AS (
      SELECT
        resolved_row.resolved_timesheet_id AS timesheet_id,
        COALESCE(NULLIF(BTRIM(bucket_item.value->>'label'), ''), NULLIF(BTRIM(bucket_item.value->>'unit_name'), ''), NULLIF(BTRIM(bucket_item.value->>'bucket_code'), ''), NULLIF(BTRIM(resolved_row.bucket_code), ''), 'Rate unit') AS unit_name,
        COALESCE(NULLIF(BTRIM(bucket_item.value->>'target_units'), ''), NULLIF(BTRIM(bucket_item.value->>'source_units'), ''), NULLIF(BTRIM(bucket_item.value->>'quantity'), ''), NULLIF(BTRIM(bucket_item.value->>'units'), ''), '') AS quantity,
        UPPER(BTRIM(COALESCE(bucket_item.value->>'source_pay_method', resolved_row.payload_json->>'source_pay_method', ''))) AS source_pay_method,
        UPPER(BTRIM(COALESCE(bucket_item.value->>'target_pay_method', bucket_item.value->>'current_pay_method', resolved_row.payload_json->>'target_pay_method', ''))) AS target_pay_method,
        BTRIM(COALESCE(bucket_item.value->>'source_rate', bucket_item.value->>'previous_rate', resolved_row.payload_json->>'source_rate', '')) AS previous_rate,
        BTRIM(COALESCE(bucket_item.value->>'target_rate', bucket_item.value->>'current_resolved_rate', resolved_row.payload_json->>'target_rate', '')) AS current_resolved_rate,
        BTRIM(COALESCE(bucket_item.value->>'source_margin_ex_vat', bucket_item.value->>'old_margin', resolved_row.payload_json->>'source_margin_ex_vat', '')) AS old_margin,
        BTRIM(COALESCE(bucket_item.value->>'target_margin_ex_vat', bucket_item.value->>'new_margin', resolved_row.payload_json->>'target_margin_ex_vat', '')) AS new_margin,
        BTRIM(COALESCE(bucket_item.value#>>'{source_basis_json,work_date}', bucket_item.value->>'work_date', resolved_row.payload_json#>>'{source_basis_json,work_date}', '')) AS work_date,
        BTRIM(COALESCE(bucket_item.value->>'bucket_code', resolved_row.bucket_code, '')) AS bucket_code,
        BTRIM(COALESCE(bucket_item.value->>'component_key_type', resolved_row.component_key_type, '')) AS component_key_type,
        BTRIM(COALESCE(bucket_item.value->>'component_key_value', resolved_row.component_key_value, '')) AS component_key_value
      FROM resolved_rows AS resolved_row
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(resolved_row.payload_json->'bucket_resolutions') = 'array'
               AND jsonb_array_length(resolved_row.payload_json->'bucket_resolutions') > 0
            THEN resolved_row.payload_json->'bucket_resolutions'
          ELSE jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
            'bucket_code', resolved_row.bucket_code,
            'component_key_type', resolved_row.component_key_type,
            'component_key_value', resolved_row.component_key_value,
            'source_units', resolved_row.payload_json->'source_units',
            'target_units', resolved_row.payload_json->'target_units',
            'source_rate', resolved_row.payload_json->'source_rate',
            'target_rate', resolved_row.payload_json->'target_rate',
            'source_pay_method', resolved_row.payload_json->'source_pay_method',
            'target_pay_method', resolved_row.payload_json->'target_pay_method',
            'source_margin_ex_vat', resolved_row.payload_json->'source_margin_ex_vat',
            'target_margin_ex_vat', resolved_row.payload_json->'target_margin_ex_vat',
            'source_basis_json', resolved_row.payload_json->'source_basis_json'
          )))
        END
      ) AS bucket_item(value)
      WHERE resolved_row.resolved_timesheet_id IS NOT NULL
    ),
    preview_component_evidence_source AS (
      SELECT
        preview_candidate.timesheet_id,
        COALESCE(NULLIF(BTRIM(component_item.value->>'label'), ''), NULLIF(BTRIM(component_item.value->>'unit_name'), ''), NULLIF(BTRIM(component_item.value->>'component_label'), ''), NULLIF(BTRIM(component_item.value->>'display_label'), ''), NULLIF(BTRIM(component_item.value->>'bucket_code'), ''), 'Rate unit') AS unit_name,
        COALESCE(NULLIF(BTRIM(component_item.value->>'target_units'), ''), NULLIF(BTRIM(component_item.value->>'source_units'), ''), NULLIF(BTRIM(component_item.value->>'quantity'), ''), NULLIF(BTRIM(component_item.value->>'units'), ''), NULLIF(BTRIM(component_item.value->>'hours'), ''), '') AS quantity,
        UPPER(BTRIM(COALESCE(component_item.value->>'source_pay_method', component_item.value#>>'{source_basis_json,source_pay_method}', ''))) AS source_pay_method,
        UPPER(BTRIM(COALESCE(component_item.value->>'target_pay_method', component_item.value->>'current_target_pay_method', component_item.value->>'saved_target_pay_method', ''))) AS target_pay_method,
        BTRIM(COALESCE(component_item.value->>'source_rate', component_item.value->>'previous_rate', component_item.value#>>'{source_basis_json,source_rate}', component_item.value#>>'{source_basis_json,rate}', '')) AS previous_rate,
        BTRIM(COALESCE(component_item.value->>'target_rate', component_item.value->>'suggested_target_rate', component_item.value->>'current_resolved_rate', component_item.value#>>'{saved_resolution_payload_json,target_rate}', component_item.value#>>'{suggested_resolution_payload_json,suggested_target_rate}', component_item.value#>>'{suggested_resolution_result_json,replacement_rate}', '')) AS current_resolved_rate,
        BTRIM(COALESCE(component_item.value->>'source_margin_ex_vat', component_item.value->>'source_margin', component_item.value->>'old_margin', component_item.value#>>'{suggested_resolution_result_json,source_margin_ex_vat}', '')) AS old_margin,
        BTRIM(COALESCE(component_item.value->>'target_margin_ex_vat', component_item.value->>'suggested_target_margin_ex_vat', component_item.value->>'target_margin', component_item.value->>'new_margin', component_item.value#>>'{suggested_resolution_result_json,target_margin_ex_vat}', '')) AS new_margin,
        BTRIM(COALESCE(component_item.value#>>'{source_basis_json,work_date}', component_item.value->>'work_date', '')) AS work_date,
        BTRIM(COALESCE(component_item.value->>'bucket_code', component_item.value#>>'{source_basis_json,bucket_code}', '')) AS bucket_code,
        BTRIM(COALESCE(component_item.value->>'component_key_type', component_item.value->>'frozen_component_key_type', '')) AS component_key_type,
        BTRIM(COALESCE(component_item.value->>'component_key_value', component_item.value->>'frozen_component_key_value', component_item.value->>'key', '')) AS component_key_value
      FROM preview_candidates AS preview_candidate
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(preview_candidate.row_json->'case_components') = 'array'
               AND jsonb_array_length(preview_candidate.row_json->'case_components') > 0
            THEN preview_candidate.row_json->'case_components'
          ELSE '[]'::jsonb
        END
      ) AS component_item(value)
      WHERE preview_candidate.timesheet_id IS NOT NULL
    ),
    evidence_source AS (
      SELECT resolution_evidence_source.*
      FROM resolution_evidence_source
      UNION ALL
      SELECT preview_component_evidence_source.*
      FROM preview_component_evidence_source
    ),
    evidence_deduped AS (
      SELECT DISTINCT ON (
        evidence_row.timesheet_id,
        evidence_row.unit_name,
        evidence_row.component_key_type,
        evidence_row.component_key_value,
        evidence_row.work_date,
        evidence_row.previous_rate,
        evidence_row.current_resolved_rate,
        evidence_row.quantity
      )
        evidence_row.*,
        CASE
          WHEN evidence_row.source_pay_method <> '' AND evidence_row.target_pay_method <> '' THEN evidence_row.source_pay_method || ' -> ' || evidence_row.target_pay_method
          ELSE ''
        END AS movement
      FROM evidence_source AS evidence_row
      ORDER BY
        evidence_row.timesheet_id,
        evidence_row.unit_name,
        evidence_row.component_key_type,
        evidence_row.component_key_value,
        evidence_row.work_date,
        evidence_row.previous_rate,
        evidence_row.current_resolved_rate,
        evidence_row.quantity
    ),
    evidence_by_timesheet AS (
      SELECT
        evidence_row.timesheet_id,
        COALESCE(JSONB_AGG(jsonb_build_object(
          'unit_name', evidence_row.unit_name,
          'quantity', NULLIF(evidence_row.quantity, ''),
          'movement', NULLIF(evidence_row.movement, ''),
          'source_pay_method', NULLIF(evidence_row.source_pay_method, ''),
          'target_pay_method', NULLIF(evidence_row.target_pay_method, ''),
          'previous_rate', NULLIF(evidence_row.previous_rate, ''),
          'current_resolved_rate', NULLIF(evidence_row.current_resolved_rate, ''),
          'old_margin', NULLIF(evidence_row.old_margin, ''),
          'new_margin', NULLIF(evidence_row.new_margin, ''),
          'work_date', NULLIF(evidence_row.work_date, ''),
          'bucket_code', NULLIF(evidence_row.bucket_code, ''),
          'component_key_type', NULLIF(evidence_row.component_key_type, ''),
          'component_key_value', NULLIF(evidence_row.component_key_value, '')
        ) ORDER BY evidence_row.work_date NULLS LAST, evidence_row.unit_name, evidence_row.component_key_value), '[]'::jsonb) AS evidence_json,
        CASE
          WHEN COUNT(DISTINCT NULLIF(evidence_row.movement, '')) = 0 THEN ''
          WHEN COUNT(DISTINCT NULLIF(evidence_row.movement, '')) = 1 THEN (ARRAY_AGG(DISTINCT NULLIF(evidence_row.movement, '') ORDER BY NULLIF(evidence_row.movement, '')))[1]
          ELSE 'Mixed'
        END AS pay_method_movement
      FROM evidence_deduped AS evidence_row
      GROUP BY evidence_row.timesheet_id
    )
    SELECT COALESCE(JSONB_AGG(jsonb_build_object(
      'timesheet_id', eligible_timesheet.timesheet_id::text,
      'candidate_id', v_candidate_id::text,
      'case_key', eligible_timesheet.case_key,
      'resolution_family', 'BUCKETED',
      'week_ending_date', COALESCE(NULLIF(BTRIM(preview_meta.row_json->>'week_ending_date'), ''), NULLIF(BTRIM(eligible_timesheet.sample_payload_json->>'week_ending_date'), '')),
      'client_name', COALESCE(NULLIF(BTRIM(preview_meta.row_json->>'client_name'), ''), NULLIF(BTRIM(preview_meta.row_json->>'trust_name'), ''), NULLIF(BTRIM(eligible_timesheet.sample_payload_json->>'client_name'), ''), '—'),
      'candidate_name', COALESCE(NULLIF(BTRIM(preview_meta.row_json->>'display_name'), ''), NULLIF(BTRIM(preview_meta.row_json->>'candidate_name'), ''), NULLIF(BTRIM(eligible_timesheet.sample_payload_json->>'candidate_name'), '')),
      'tms_ref', COALESCE(NULLIF(BTRIM(preview_meta.row_json->>'tms_ref'), ''), NULLIF(BTRIM(eligible_timesheet.sample_payload_json->>'tms_ref'), '')),
      'pay_method_movement', COALESCE(NULLIF(evidence_by_timesheet.pay_method_movement, ''), NULLIF(BTRIM(preview_meta.row_json->>'pay_method_movement'), ''), ''),
      'evidence', COALESCE(evidence_by_timesheet.evidence_json, '[]'::jsonb),
      'case_components', CASE WHEN jsonb_typeof(preview_meta.row_json->'case_components') = 'array' THEN preview_meta.row_json->'case_components' ELSE '[]'::jsonb END,
      'case_resolution_ids', eligible_timesheet.case_resolution_ids,
      'resolution_identity_keys', eligible_timesheet.resolution_identity_keys
    ) ORDER BY
      COALESCE(NULLIF(BTRIM(preview_meta.row_json->>'week_ending_date'), ''), NULLIF(BTRIM(eligible_timesheet.sample_payload_json->>'week_ending_date'), ''), '9999-12-31'),
      LOWER(COALESCE(NULLIF(BTRIM(preview_meta.row_json->>'client_name'), ''), NULLIF(BTRIM(preview_meta.row_json->>'trust_name'), ''), NULLIF(BTRIM(eligible_timesheet.sample_payload_json->>'client_name'), ''), '')),
      eligible_timesheet.timesheet_id), '[]'::jsonb)
    INTO v_clearable_timesheets_json
    FROM eligible_timesheets AS eligible_timesheet
    LEFT JOIN preview_meta
      ON preview_meta.timesheet_id = eligible_timesheet.timesheet_id
    LEFT JOIN evidence_by_timesheet
      ON evidence_by_timesheet.timesheet_id = eligible_timesheet.timesheet_id;

    RETURN jsonb_build_object(
      'ok', true,
      'operation', 'LIST_CLEARABLE',
      'session_id', p_session_id::text,
      'session_version', v_session_row.version,
      'progress_counter_version', COALESCE(v_session_row.progress_counter_version, 0),
      'candidate_id', v_candidate_id::text,
      'clearable_timesheets', COALESCE(v_clearable_timesheets_json, '[]'::jsonb),
      'eligible_timesheet_count', COALESCE(v_eligible_timesheet_count, 0),
      'bounded', true,
      'max_clearable_timesheets', v_max_clearable_timesheets,
      'job_id', NULL::text,
      'state_changed', false
    );
  END IF;

  CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_selected_timesheets (
    timesheet_id uuid PRIMARY KEY
  ) ON COMMIT DROP;
  TRUNCATE TABLE _tmp_bpay_selected_timesheets;

  v_explicit_bulk_request := (
    v_resolution_payload_json ? 'selected_timesheet_ids'
    OR v_resolution_payload_json ? 'timesheet_ids'
    OR v_resolution_payload_json ? 'selected_case_identities'
    OR v_resolution_payload_json ? 'selected_case_keys'
  );

  IF v_resolution_payload_json ? 'selected_timesheet_ids' THEN
    IF jsonb_typeof(v_resolution_payload_json->'selected_timesheet_ids') <> 'array' THEN
      RAISE EXCEPTION 'selected_timesheet_ids must be a JSON array';
    END IF;
    IF jsonb_array_length(v_resolution_payload_json->'selected_timesheet_ids') > v_max_selected_timesheets THEN
      RAISE EXCEPTION 'selected_timesheet_ids exceeds the maximum of %', v_max_selected_timesheets;
    END IF;
    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(v_resolution_payload_json->'selected_timesheet_ids') AS selected_element(value)
      WHERE jsonb_typeof(selected_element.value) <> 'string'
         OR BTRIM(selected_element.value #>> '{}') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) THEN
      RAISE EXCEPTION 'selected_timesheet_ids contains an invalid UUID';
    END IF;
    INSERT INTO _tmp_bpay_selected_timesheets(timesheet_id)
    SELECT DISTINCT BTRIM(selected_element.value #>> '{}')::uuid
    FROM jsonb_array_elements(v_resolution_payload_json->'selected_timesheet_ids') AS selected_element(value)
    ON CONFLICT (timesheet_id) DO NOTHING;
  END IF;

  IF v_resolution_payload_json ? 'timesheet_ids' THEN
    IF jsonb_typeof(v_resolution_payload_json->'timesheet_ids') <> 'array' THEN
      RAISE EXCEPTION 'timesheet_ids must be a JSON array';
    END IF;
    IF jsonb_array_length(v_resolution_payload_json->'timesheet_ids') > v_max_selected_timesheets THEN
      RAISE EXCEPTION 'timesheet_ids exceeds the maximum of %', v_max_selected_timesheets;
    END IF;
    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(v_resolution_payload_json->'timesheet_ids') AS selected_element(value)
      WHERE jsonb_typeof(selected_element.value) <> 'string'
         OR BTRIM(selected_element.value #>> '{}') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) THEN
      RAISE EXCEPTION 'timesheet_ids contains an invalid UUID';
    END IF;
    INSERT INTO _tmp_bpay_selected_timesheets(timesheet_id)
    SELECT DISTINCT BTRIM(selected_element.value #>> '{}')::uuid
    FROM jsonb_array_elements(v_resolution_payload_json->'timesheet_ids') AS selected_element(value)
    ON CONFLICT (timesheet_id) DO NOTHING;
  END IF;

  IF v_resolution_payload_json ? 'selected_case_identities' THEN
    IF jsonb_typeof(v_resolution_payload_json->'selected_case_identities') <> 'array' THEN
      RAISE EXCEPTION 'selected_case_identities must be a JSON array';
    END IF;
    IF jsonb_array_length(v_resolution_payload_json->'selected_case_identities') > v_max_selected_timesheets THEN
      RAISE EXCEPTION 'selected_case_identities exceeds the maximum of %', v_max_selected_timesheets;
    END IF;
    IF EXISTS (
      SELECT 1
      FROM (
        SELECT
          selected_element.value,
          BTRIM(COALESCE(
            selected_element.value->>'timesheet_id',
            selected_element.value->>'linked_timesheet_id',
            CASE
              WHEN BTRIM(COALESCE(selected_element.value->>'case_key', '')) ~* '^timesheet:[0-9a-f-]{36}$'
                THEN SUBSTRING(BTRIM(selected_element.value->>'case_key') FROM 11)
              ELSE ''
            END
          )) AS timesheet_id_text
        FROM jsonb_array_elements(v_resolution_payload_json->'selected_case_identities') AS selected_element(value)
      ) AS selected_identity
      WHERE jsonb_typeof(selected_identity.value) <> 'object'
         OR selected_identity.timesheet_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) THEN
      RAISE EXCEPTION 'selected_case_identities contains an invalid timesheet identity';
    END IF;
    INSERT INTO _tmp_bpay_selected_timesheets(timesheet_id)
    SELECT DISTINCT selected_identity.timesheet_id_text::uuid
    FROM (
      SELECT BTRIM(COALESCE(
        selected_element.value->>'timesheet_id',
        selected_element.value->>'linked_timesheet_id',
        CASE
          WHEN BTRIM(COALESCE(selected_element.value->>'case_key', '')) ~* '^timesheet:[0-9a-f-]{36}$'
            THEN SUBSTRING(BTRIM(selected_element.value->>'case_key') FROM 11)
          ELSE ''
        END
      )) AS timesheet_id_text
      FROM jsonb_array_elements(v_resolution_payload_json->'selected_case_identities') AS selected_element(value)
    ) AS selected_identity
    ON CONFLICT (timesheet_id) DO NOTHING;
  END IF;

  IF v_resolution_payload_json ? 'selected_case_keys' THEN
    IF jsonb_typeof(v_resolution_payload_json->'selected_case_keys') <> 'array' THEN
      RAISE EXCEPTION 'selected_case_keys must be a JSON array';
    END IF;
    IF jsonb_array_length(v_resolution_payload_json->'selected_case_keys') > v_max_selected_timesheets THEN
      RAISE EXCEPTION 'selected_case_keys exceeds the maximum of %', v_max_selected_timesheets;
    END IF;
    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(v_resolution_payload_json->'selected_case_keys') AS selected_element(value)
      WHERE jsonb_typeof(selected_element.value) <> 'string'
         OR BTRIM(selected_element.value #>> '{}') !~* '^timesheet:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) THEN
      RAISE EXCEPTION 'selected_case_keys contains an invalid timesheet case key';
    END IF;
    INSERT INTO _tmp_bpay_selected_timesheets(timesheet_id)
    SELECT DISTINCT SUBSTRING(BTRIM(selected_element.value #>> '{}') FROM 11)::uuid
    FROM jsonb_array_elements(v_resolution_payload_json->'selected_case_keys') AS selected_element(value)
    ON CONFLICT (timesheet_id) DO NOTHING;
  END IF;

  IF v_linked_timesheet_id IS NOT NULL THEN
    INSERT INTO _tmp_bpay_selected_timesheets(timesheet_id)
    VALUES (v_linked_timesheet_id)
    ON CONFLICT (timesheet_id) DO NOTHING;
  ELSIF v_case_key ~* '^timesheet:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    INSERT INTO _tmp_bpay_selected_timesheets(timesheet_id)
    VALUES (SUBSTRING(v_case_key FROM 11)::uuid)
    ON CONFLICT (timesheet_id) DO NOTHING;
  END IF;

  SELECT COUNT(*)::integer,
         COALESCE(JSONB_AGG(selected_timesheet.timesheet_id::text ORDER BY selected_timesheet.timesheet_id::text), '[]'::jsonb)
  INTO v_selected_timesheet_count,
       v_selected_timesheet_ids_json
  FROM _tmp_bpay_selected_timesheets AS selected_timesheet;

  IF v_selected_timesheet_count > v_max_selected_timesheets THEN
    RAISE EXCEPTION 'selected timesheet count exceeds the maximum of %', v_max_selected_timesheets;
  END IF;

  v_whole_timesheet_mode := v_selected_timesheet_count > 0 AND (
    v_resolution_family = 'BUCKETED'
    OR v_explicit_bulk_request
    OR (
      v_resolution_family = ''
      AND v_case_key ~* '^timesheet:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )
  );
  v_strict_selection_validation := v_explicit_bulk_request OR v_expected_session_version IS NOT NULL;

  IF v_resolution_family = 'BUCKETED' AND NOT v_whole_timesheet_mode THEN
    RAISE EXCEPTION 'BUCKETED resolved-rate clear requires a whole-timesheet identity';
  END IF;

  IF NOT v_whole_timesheet_mode AND v_case_key = '' THEN
    RAISE EXCEPTION 'case_key is required';
  END IF;

  IF v_candidate_id IS NULL THEN
    IF v_whole_timesheet_mode THEN
      SELECT COALESCE(ARRAY_AGG(DISTINCT resolution_row.candidate_id ORDER BY resolution_row.candidate_id), ARRAY[]::uuid[])
      INTO v_matching_candidate_ids
      FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
      WHERE resolution_row.session_id = p_session_id
        AND UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) = 'BUCKETED'
        AND EXISTS (
          SELECT 1
          FROM _tmp_bpay_selected_timesheets AS selected_timesheet
          WHERE resolution_row.timesheet_id = selected_timesheet.timesheet_id
             OR BTRIM(COALESCE(resolution_row.payload_json->>'linked_timesheet_id', '')) = selected_timesheet.timesheet_id::text
             OR BTRIM(COALESCE(resolution_row.payload_json->>'timesheet_id', '')) = selected_timesheet.timesheet_id::text
        );
    ELSE
      SELECT COALESCE(ARRAY_AGG(DISTINCT resolution_row.candidate_id ORDER BY resolution_row.candidate_id), ARRAY[]::uuid[])
      INTO v_matching_candidate_ids
      FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
      WHERE resolution_row.session_id = p_session_id
        AND resolution_row.case_key = v_case_key
        AND (
          v_linked_timesheet_id IS NULL
          OR resolution_row.timesheet_id = v_linked_timesheet_id
          OR BTRIM(COALESCE(resolution_row.payload_json->>'linked_timesheet_id', '')) = v_linked_timesheet_id::text
          OR BTRIM(COALESCE(resolution_row.payload_json->>'timesheet_id', '')) = v_linked_timesheet_id::text
        )
        AND (
          v_finance_case_id_text = ''
          OR NULLIF(BTRIM(COALESCE(resolution_row.payload_json->>'finance_case_id', '')), '') IS NULL
          OR BTRIM(COALESCE(resolution_row.payload_json->>'finance_case_id', '')) = v_finance_case_id_text
        )
        AND (
          v_resolution_family = ''
          OR UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) = v_resolution_family
        );
    END IF;

    v_matching_candidate_count := COALESCE(ARRAY_LENGTH(v_matching_candidate_ids, 1), 0);

    IF v_matching_candidate_count = 0 THEN
      IF v_strict_selection_validation THEN
        RAISE EXCEPTION 'no clearable resolved-rate rows remain for the selected timesheets in session %', p_session_id;
      END IF;
      RETURN jsonb_build_object(
        'ok', true,
        'operation', 'CLEAR',
        'session_id', p_session_id::text,
        'candidate_id', NULL::text,
        'session_version', v_session_row.version,
        'job_id', NULL::text,
        'case_resolution_id', NULL::text,
        'case_resolution_ids', '[]'::jsonb,
        'resolution_identity_keys', '[]'::jsonb,
        'selected_timesheet_ids', COALESCE(v_selected_timesheet_ids_json, '[]'::jsonb),
        'deleted_count', 0,
        'cleared', false,
        'state_changed', false,
        'no_op', true
      );
    ELSIF v_matching_candidate_count > 1 THEN
      RAISE EXCEPTION 'ambiguous session case-resolution clear target in session %', p_session_id;
    END IF;

    v_candidate_id := v_matching_candidate_ids[1];
  END IF;

  SELECT (
    EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE scope_row.session_id = p_session_id
        AND scope_row.candidate_id = v_candidate_id
    )
    OR EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id = p_session_id
        AND preview_row.candidate_id = v_candidate_id
    )
    OR EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_candidate_line_work AS line_work_row
      WHERE line_work_row.session_id = p_session_id
        AND line_work_row.candidate_id = v_candidate_id
    )
    OR EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
      WHERE resolution_row.session_id = p_session_id
        AND resolution_row.candidate_id = v_candidate_id
    )
    OR v_candidate_id = ANY(COALESCE(v_session_row.scope_candidate_ids, ARRAY[]::uuid[]))
  )
  INTO v_candidate_in_scope;

  IF NOT COALESCE(v_candidate_in_scope, false) THEN
    RAISE EXCEPTION 'candidate % is not in workbench session scope %', v_candidate_id, p_session_id;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_candidate_resolution_row_count
  FROM (
    SELECT 1
    FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
    WHERE resolution_row.session_id = p_session_id
      AND resolution_row.candidate_id = v_candidate_id
    LIMIT (v_max_candidate_resolution_rows + 1)
  ) AS bounded_candidate_resolution_rows;

  IF COALESCE(v_candidate_resolution_row_count, 0) > v_max_candidate_resolution_rows THEN
    RAISE EXCEPTION 'candidate % has too many case-resolution rows to clear safely in one request', v_candidate_id;
  END IF;

  CREATE TEMP TABLE IF NOT EXISTS _tmp_bpay_session_case_resolution_existing
  AS
  SELECT resolution_row.*
  FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
  WITH NO DATA;
  TRUNCATE TABLE _tmp_bpay_session_case_resolution_existing;

  IF v_whole_timesheet_mode THEN
    INSERT INTO _tmp_bpay_session_case_resolution_existing
    SELECT resolution_row.*
    FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
    WHERE resolution_row.session_id = p_session_id
      AND resolution_row.candidate_id = v_candidate_id
      AND UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) = 'BUCKETED'
      AND EXISTS (
        SELECT 1
        FROM _tmp_bpay_selected_timesheets AS selected_timesheet
        WHERE resolution_row.timesheet_id = selected_timesheet.timesheet_id
           OR BTRIM(COALESCE(resolution_row.payload_json->>'linked_timesheet_id', '')) = selected_timesheet.timesheet_id::text
           OR BTRIM(COALESCE(resolution_row.payload_json->>'timesheet_id', '')) = selected_timesheet.timesheet_id::text
      );

    SELECT COUNT(*)::integer
    INTO v_matched_selected_timesheet_count
    FROM _tmp_bpay_selected_timesheets AS selected_timesheet
    WHERE EXISTS (
      SELECT 1
      FROM _tmp_bpay_session_case_resolution_existing AS existing_resolution
      WHERE existing_resolution.timesheet_id = selected_timesheet.timesheet_id
         OR BTRIM(COALESCE(existing_resolution.payload_json->>'linked_timesheet_id', '')) = selected_timesheet.timesheet_id::text
         OR BTRIM(COALESCE(existing_resolution.payload_json->>'timesheet_id', '')) = selected_timesheet.timesheet_id::text
    );

    IF v_matched_selected_timesheet_count <> v_selected_timesheet_count THEN
      RAISE EXCEPTION 'one or more selected timesheets are no longer clearable for candidate % in session %', v_candidate_id, p_session_id;
    END IF;
  ELSE
    INSERT INTO _tmp_bpay_session_case_resolution_existing
    SELECT resolution_row.*
    FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
    WHERE resolution_row.session_id = p_session_id
      AND resolution_row.candidate_id = v_candidate_id
      AND resolution_row.case_key = v_case_key
      AND (
        v_linked_timesheet_id IS NULL
        OR resolution_row.timesheet_id = v_linked_timesheet_id
        OR BTRIM(COALESCE(resolution_row.payload_json->>'linked_timesheet_id', '')) = v_linked_timesheet_id::text
        OR BTRIM(COALESCE(resolution_row.payload_json->>'timesheet_id', '')) = v_linked_timesheet_id::text
      )
      AND (
        v_finance_case_id_text = ''
        OR NULLIF(BTRIM(COALESCE(resolution_row.payload_json->>'finance_case_id', '')), '') IS NULL
        OR BTRIM(COALESCE(resolution_row.payload_json->>'finance_case_id', '')) = v_finance_case_id_text
      )
      AND (
        v_resolution_family = ''
        OR UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) = v_resolution_family
      );
  END IF;

  SELECT COUNT(*)::integer
  INTO v_deleted_count
  FROM _tmp_bpay_session_case_resolution_existing AS existing_resolution;

  IF v_deleted_count > v_max_candidate_resolution_rows THEN
    RAISE EXCEPTION 'clear target exceeds the maximum safe resolved-rate row count of %', v_max_candidate_resolution_rows;
  END IF;

  IF v_deleted_count = 0 THEN
    IF v_strict_selection_validation THEN
      RAISE EXCEPTION 'no clearable resolved-rate rows remain for the selected timesheets in session %', p_session_id;
    END IF;
    RETURN jsonb_build_object(
      'ok', true,
      'operation', 'CLEAR',
      'session_id', p_session_id::text,
      'candidate_id', v_candidate_id::text,
      'session_version', v_session_row.version,
      'job_id', NULL::text,
      'case_resolution_id', NULL::text,
      'case_resolution_ids', '[]'::jsonb,
      'resolution_identity_keys', '[]'::jsonb,
      'selected_timesheet_ids', COALESCE(v_selected_timesheet_ids_json, '[]'::jsonb),
      'deleted_count', 0,
      'cleared', false,
      'state_changed', false,
      'no_op', true
    );
  END IF;

  SELECT
    COALESCE(JSONB_AGG(existing_resolution.id::text ORDER BY existing_resolution.id::text), '[]'::jsonb),
    COALESCE(JSONB_AGG(existing_resolution.resolution_identity_key ORDER BY existing_resolution.resolution_identity_key), '[]'::jsonb),
    (ARRAY_AGG(existing_resolution.id::text ORDER BY existing_resolution.id))[1]
  INTO v_case_resolution_ids,
       v_resolution_identity_keys,
       v_case_resolution_id_text
  FROM _tmp_bpay_session_case_resolution_existing AS existing_resolution;

  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = p_session_id
      AND scope_row.candidate_id = v_candidate_id
  )
  INTO v_candidate_has_scope_row;

  IF NOT COALESCE(v_candidate_has_scope_row, false) THEN
    SELECT COALESCE(MAX(scope_row.scope_ordinal), -1) + 1
    INTO v_next_scope_ordinal
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = p_session_id;

    INSERT INTO public.banking_pay_workbench_session_scope(
      session_id,
      candidate_id,
      scope_ordinal,
      status,
      pending_job_id,
      seeded,
      dirty,
      error_json,
      created_at_utc,
      updated_at_utc
    )
    VALUES (
      p_session_id,
      v_candidate_id,
      v_next_scope_ordinal,
      'READY',
      NULL::uuid,
      true,
      false,
      NULL::jsonb,
      v_now,
      v_now
    )
    ON CONFLICT (session_id, candidate_id) DO NOTHING;

    GET DIAGNOSTICS v_scope_row_inserted = ROW_COUNT;

    IF v_scope_row_inserted > 0 THEN
      UPDATE public.banking_pay_workbench_sessions AS session_update
      SET scope_total_count = GREATEST(
            COALESCE(session_update.scope_total_count, 0),
            (SELECT COUNT(*)::integer FROM public.banking_pay_workbench_session_scope AS scope_count WHERE scope_count.session_id = p_session_id)
          ),
          scope_seeded_count = GREATEST(
            COALESCE(session_update.scope_seeded_count, 0),
            (SELECT COUNT(*) FILTER (WHERE scope_count.seeded)::integer FROM public.banking_pay_workbench_session_scope AS scope_count WHERE scope_count.session_id = p_session_id)
          ),
          scope_ready_count = GREATEST(
            COALESCE(session_update.scope_ready_count, 0),
            (SELECT COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(scope_count.status, ''))) IN ('READY', 'LINE_WORK_READY', 'MATERIALISED', 'MATERIALIZED'))::integer FROM public.banking_pay_workbench_session_scope AS scope_count WHERE scope_count.session_id = p_session_id)
          ),
          updated_at_utc = v_now
      WHERE session_update.id = p_session_id;
    END IF;
  END IF;

  DELETE FROM public.banking_pay_workbench_session_case_resolutions AS delete_resolution
  USING _tmp_bpay_session_case_resolution_existing AS existing_resolution
  WHERE delete_resolution.id = existing_resolution.id;

  UPDATE public.banking_pay_workbench_sessions AS session_update
  SET version = session_update.version + 1,
      progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
      progress_updated_at_utc = v_now,
      updated_at_utc = v_now
  WHERE session_update.id = p_session_id
  RETURNING session_update.version, session_update.progress_counter_version
  INTO v_new_session_version, v_new_progress_counter_version;

  v_job_json := public.pay_workbench_enqueue_session_candidate_refresh(
    p_session_id => p_session_id,
    p_candidate_id => v_candidate_id,
    p_reason => CASE WHEN v_whole_timesheet_mode THEN 'SESSION_RESOLVED_RATE_BULK_CLEARED' ELSE 'SESSION_CASE_RESOLUTION_CLEARED' END,
    p_actor_user_id => p_actor_user_id,
    p_payload_json => jsonb_build_object(
      'case_key', NULLIF(v_case_key, ''),
      'finance_case_id', NULLIF(v_finance_case_id_text, ''),
      'linked_timesheet_id', CASE WHEN v_linked_timesheet_id IS NULL THEN NULL ELSE v_linked_timesheet_id::text END,
      'selected_timesheet_ids', COALESCE(v_selected_timesheet_ids_json, '[]'::jsonb),
      'resolution_family', NULLIF(v_resolution_family, ''),
      'resolution_identity_keys', v_resolution_identity_keys,
      'deleted_count', v_deleted_count,
      'bulk_clear', v_whole_timesheet_mode,
      'force_legacy', true,
      'projection_mode', 'LEGACY',
      'projection_class', 'CASE_RESOLUTION',
      'fallback_reason', 'CASE_RESOLUTION_CHANGED',
      'refresh_scope_kind', CASE
        WHEN COALESCE(v_selected_timesheet_count, 0) > 0 OR v_linked_timesheet_id IS NOT NULL THEN 'TARGETED_TIMESHEETS'
        ELSE 'CANDIDATE_FULL_LIVE'
      END,
      'targeted_timesheet_ids', CASE
        WHEN COALESCE(v_selected_timesheet_count, 0) > 0 THEN COALESCE(v_selected_timesheet_ids_json, '[]'::jsonb)
        WHEN v_linked_timesheet_id IS NOT NULL THEN jsonb_build_array(v_linked_timesheet_id::text)
        ELSE '[]'::jsonb
      END,
      'linked_timesheet_ids', CASE WHEN v_linked_timesheet_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(v_linked_timesheet_id::text) END,
      'source_build_required', true,
      'line_work_required', true,
      'delta_refresh_required', false,
      'complex_refresh_required', true,
      'resolved_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    )
  );

  v_job_id_text := BTRIM(COALESCE(
    v_job_json->>'job_id',
    v_job_json #>> '{enqueue_result,job_id}',
    v_job_json #>> '{enqueue_result,job_ids,0}',
    v_job_json #>> '{job_ids,0}',
    v_job_json #>> '{enqueue_result,session_recompute_job_ids,0}',
    v_job_json #>> '{session_recompute_job_ids,0}',
    ''
  ));
  IF v_job_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_job_id := v_job_id_text::uuid;
  END IF;

  IF v_job_id IS NULL THEN
    RAISE EXCEPTION 'candidate refresh enqueue did not return a durable job_id for session % candidate %', p_session_id, v_candidate_id;
  END IF;

  SELECT
    NULLIF(BTRIM(COALESCE(actor_user.display_name, actor_user.email, '')), ''),
    NULLIF(BTRIM(COALESCE(actor_user.role, '')), '')
  INTO v_actor_display,
       v_actor_role
  FROM public.tms_users AS actor_user
  WHERE actor_user.id = p_actor_user_id
  LIMIT 1;

  INSERT INTO public.audit_events(
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason,
    actor_user_id,
    actor_display,
    actor_role_at_time
  )
  SELECT
    'banking_pay_workbench_session_case_resolution',
    existing_resolution.id::text,
    'SESSION_CASE_RESOLUTION_CLEARED',
    jsonb_build_object(
      'id', existing_resolution.id::text,
      'session_id', existing_resolution.session_id::text,
      'candidate_id', existing_resolution.candidate_id::text,
      'case_key', existing_resolution.case_key,
      'resolution_family', existing_resolution.resolution_family,
      'resolution_identity_key', existing_resolution.resolution_identity_key,
      'timesheet_id', CASE WHEN existing_resolution.timesheet_id IS NULL THEN NULL ELSE existing_resolution.timesheet_id::text END,
      'source_basis_fingerprint', existing_resolution.source_basis_fingerprint,
      'source_family_key', existing_resolution.source_family_key,
      'bucket_code', existing_resolution.bucket_code,
      'component_key_type', existing_resolution.component_key_type,
      'component_key_value', existing_resolution.component_key_value,
      'payload_json', existing_resolution.payload_json
    ),
    jsonb_build_object(
      'id', existing_resolution.id::text,
      'session_id', existing_resolution.session_id::text,
      'candidate_id', existing_resolution.candidate_id::text,
      'case_key', existing_resolution.case_key,
      'resolution_family', existing_resolution.resolution_family,
      'resolution_identity_key', existing_resolution.resolution_identity_key,
      'timesheet_id', CASE WHEN existing_resolution.timesheet_id IS NULL THEN NULL ELSE existing_resolution.timesheet_id::text END,
      'payload_json', existing_resolution.payload_json,
      'session_version', v_new_session_version,
      'progress_counter_version', v_new_progress_counter_version,
      'pending_job_id', v_job_id::text,
      'cleared_at_utc', v_now,
      'bulk_clear', v_whole_timesheet_mode,
      'selected_timesheet_ids', COALESCE(v_selected_timesheet_ids_json, '[]'::jsonb)
    ),
    CASE WHEN v_whole_timesheet_mode THEN 'SESSION_RESOLVED_RATE_BULK_CLEARED' ELSE 'SESSION_CASE_RESOLUTION_CLEARED' END,
    p_actor_user_id,
    v_actor_display,
    v_actor_role
  FROM _tmp_bpay_session_case_resolution_existing AS existing_resolution;

  RETURN jsonb_build_object(
    'ok', true,
    'operation', 'CLEAR',
    'session_id', p_session_id::text,
    'candidate_id', v_candidate_id::text,
    'session_version', v_new_session_version,
    'progress_counter_version', v_new_progress_counter_version,
    'job_id', v_job_id::text,
    'case_resolution_id', v_case_resolution_id_text,
    'case_resolution_ids', v_case_resolution_ids,
    'resolution_identity_keys', v_resolution_identity_keys,
    'selected_timesheet_ids', COALESCE(v_selected_timesheet_ids_json, '[]'::jsonb),
    'selected_timesheet_count', COALESCE(v_selected_timesheet_count, 0),
    'deleted_count', v_deleted_count,
    'cleared', true,
    'bulk_clear', v_whole_timesheet_mode,
    'candidate_refresh_count', 1,
    'refresh_mode', 'LEGACY_TARGETED',
    'targeted_refresh_enqueued', true,
    'force_legacy', true,
    'projection_class', 'CASE_RESOLUTION',
    'fallback_reason', 'CASE_RESOLUTION_CHANGED',
    'refresh_scope_kind', CASE WHEN COALESCE(v_selected_timesheet_count, 0) > 0 OR v_linked_timesheet_id IS NOT NULL THEN 'TARGETED_TIMESHEETS' ELSE 'CANDIDATE_FULL_LIVE' END,
    'targeted_timesheet_ids', CASE WHEN COALESCE(v_selected_timesheet_count, 0) > 0 THEN COALESCE(v_selected_timesheet_ids_json, '[]'::jsonb) WHEN v_linked_timesheet_id IS NOT NULL THEN jsonb_build_array(v_linked_timesheet_id::text) ELSE '[]'::jsonb END,
    'enqueue_result', COALESCE(v_job_json, '{}'::jsonb),
    'state_changed', true,
    'no_op', false
  );
END;
$function$;
