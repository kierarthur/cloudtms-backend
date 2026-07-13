
CREATE OR REPLACE FUNCTION public.candidate_pay_method_change_apply(
  p_candidate_id uuid,
  p_new_method text,
  p_plan jsonb,
  p_actor_user_id uuid,
  p_reason text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  v_new_method text := UPPER(BTRIM(COALESCE(p_new_method, '')));
  v_expected_old_method text;
  v_old_method text;
  v_operation_id uuid;
  v_preview_source_change_seq bigint;
  v_current_source_change_seq bigint := 0;
  v_destination_patch jsonb := '{}'::jsonb;
  v_operation_request_json jsonb := '{}'::jsonb;
  v_unknown_plan_keys text[] := ARRAY[]::text[];
  v_unknown_destination_keys text[] := ARRAY[]::text[];
  v_effective_umbrella_id uuid;
  v_account_holder text;
  v_bank_name text;
  v_sort_code text;
  v_account_number text;
  v_actor_role text;
  v_scope_result jsonb := '{}'::jsonb;
  v_expected_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_enqueued_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_job_id uuid;
  v_job_status text;
  v_job_payload jsonb := '{}'::jsonb;
  v_job_source_change_seq bigint := 0;
  v_before_json jsonb := '{}'::jsonb;
  v_after_json jsonb := '{}'::jsonb;
  v_result jsonb := '{}'::jsonb;
  v_audit_after_json jsonb := '{}'::jsonb;
  v_existing_operation_audit_id uuid := NULL::uuid;
  v_existing_operation_before_json jsonb := '{}'::jsonb;
  v_existing_operation_after_json jsonb := '{}'::jsonb;
  v_existing_operation_result jsonb := '{}'::jsonb;
  v_existing_operation_source_method text := NULL::text;
  v_existing_operation_target_method text := NULL::text;
  v_existing_operation_job_id uuid := NULL::uuid;
  v_existing_operation_job_status text := NULL::text;
  v_existing_operation_source_change_seq bigint := 0;
  v_existing_operation_superseded boolean := false;
BEGIN
  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_CANDIDATE_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF v_new_method NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_TARGET_METHOD_INVALID'
      USING ERRCODE = '22023', DETAIL = COALESCE(p_new_method, '');
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_ACTOR_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  SELECT LOWER(BTRIM(COALESCE(user_row.role, '')))
  INTO v_actor_role
  FROM public.tms_users AS user_row
  WHERE user_row.id = p_actor_user_id
    AND user_row.is_active IS TRUE;

  IF NOT FOUND OR v_actor_role <> 'admin' THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_ADMIN_REQUIRED'
      USING ERRCODE = '42501', DETAIL = p_actor_user_id::text;
  END IF;

  IF p_plan IS NULL OR jsonb_typeof(p_plan) <> 'object' THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_PLAN_OBJECT_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_plan ?| ARRAY[
    'contract_ids',
    'contracts',
    'migrations',
    'successor_start',
    'successor_contract_id',
    'new_contract_id',
    'new_rates_json',
    'new_additional_rates_json',
    'rates_json',
    'timesheet_ids',
    'affected_timesheet_ids',
    'move_contract_weeks',
    'delete_old_contracts'
  ] THEN
    RAISE EXCEPTION 'LEGACY_CONTRACT_MIGRATION_PLAN_REJECTED'
      USING ERRCODE = '22023';
  END IF;

  SELECT COALESCE(array_agg(plan_key ORDER BY plan_key), ARRAY[]::text[])
  INTO v_unknown_plan_keys
  FROM jsonb_object_keys(p_plan) AS plan_keys(plan_key)
  WHERE plan_key NOT IN (
    'operation_id',
    'expected_old_method',
    'preview_source_change_seq',
    'destination_patch'
  );

  IF COALESCE(array_length(v_unknown_plan_keys, 1), 0) > 0 THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_PLAN_FIELDS_UNSUPPORTED'
      USING ERRCODE = '22023', DETAIL = array_to_string(v_unknown_plan_keys, ',');
  END IF;

  IF COALESCE(p_plan->>'operation_id', '') !~* v_uuid_re THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_OPERATION_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;
  v_operation_id := (p_plan->>'operation_id')::uuid;

  v_expected_old_method := UPPER(BTRIM(COALESCE(p_plan->>'expected_old_method', '')));
  IF v_expected_old_method NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_EXPECTED_OLD_METHOD_INVALID'
      USING ERRCODE = '22023';
  END IF;

  IF COALESCE(p_plan->>'preview_source_change_seq', '') !~ '^[0-9]{1,18}$' THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_PREVIEW_SEQUENCE_REQUIRED'
      USING ERRCODE = '22023';
  END IF;
  v_preview_source_change_seq := (p_plan->>'preview_source_change_seq')::bigint;

  IF p_plan ? 'destination_patch' THEN
    IF jsonb_typeof(p_plan->'destination_patch') <> 'object' THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DESTINATION_PATCH_INVALID'
        USING ERRCODE = '22023';
    END IF;
    v_destination_patch := p_plan->'destination_patch';
  END IF;

  SELECT COALESCE(array_agg(destination_key ORDER BY destination_key), ARRAY[]::text[])
  INTO v_unknown_destination_keys
  FROM jsonb_object_keys(v_destination_patch) AS destination_keys(destination_key)
  WHERE destination_key NOT IN (
    'umbrella_id',
    'account_holder',
    'bank_name',
    'sort_code',
    'account_number'
  );

  IF COALESCE(array_length(v_unknown_destination_keys, 1), 0) > 0 THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DESTINATION_FIELDS_UNSUPPORTED'
      USING ERRCODE = '22023', DETAIL = array_to_string(v_unknown_destination_keys, ',');
  END IF;

  v_operation_request_json := jsonb_build_object(
    'candidate_id', p_candidate_id::text,
    'expected_old_method', v_expected_old_method,
    'new_method', v_new_method,
    'preview_source_change_seq', v_preview_source_change_seq,
    'destination_patch', v_destination_patch
  );

  SELECT
    UPPER(BTRIM(COALESCE(candidate_row.pay_method, ''))),
    candidate_row.umbrella_id,
    candidate_row.account_holder,
    candidate_row.bank_name,
    candidate_row.sort_code,
    candidate_row.account_number,
    jsonb_build_object(
      'id', candidate_row.id::text,
      'pay_method', candidate_row.pay_method,
      'umbrella_id', CASE WHEN candidate_row.umbrella_id IS NULL THEN NULL ELSE candidate_row.umbrella_id::text END,
      'account_holder', candidate_row.account_holder,
      'bank_name', candidate_row.bank_name,
      'sort_code', candidate_row.sort_code,
      'account_number', candidate_row.account_number,
      'updated_at', candidate_row.updated_at,
      'rev', candidate_row.rev
    )
  INTO
    v_old_method,
    v_effective_umbrella_id,
    v_account_holder,
    v_bank_name,
    v_sort_code,
    v_account_number,
    v_before_json
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_CANDIDATE_NOT_FOUND'
      USING ERRCODE = 'P0002', DETAIL = p_candidate_id::text;
  END IF;

  SELECT
    audit_row.id,
    COALESCE(audit_row.before_json, '{}'::jsonb),
    COALESCE(audit_row.after_json, '{}'::jsonb)
  INTO
    v_existing_operation_audit_id,
    v_existing_operation_before_json,
    v_existing_operation_after_json
  FROM public.audit_events AS audit_row
  WHERE audit_row.object_type = 'candidate'
    AND audit_row.object_id_text = p_candidate_id::text
    AND audit_row.action = 'PAY_METHOD_CHANGE_PROSPECTIVE_ONLY'
    AND audit_row.after_json->>'operation_id' = v_operation_id::text
  ORDER BY audit_row.ts_utc DESC, audit_row.id DESC
  LIMIT 1;

  IF v_existing_operation_audit_id IS NOT NULL THEN
    v_existing_operation_result := CASE
      WHEN jsonb_typeof(v_existing_operation_after_json->'operation_result') = 'object'
        THEN v_existing_operation_after_json->'operation_result'
      ELSE '{}'::jsonb
    END;

    v_existing_operation_source_method := UPPER(BTRIM(COALESCE(
      v_existing_operation_result->>'original_method',
      v_existing_operation_after_json->>'original_method',
      v_existing_operation_before_json->>'pay_method',
      ''
    )));
    v_existing_operation_target_method := UPPER(BTRIM(COALESCE(
      v_existing_operation_result->>'new_method',
      v_existing_operation_after_json->>'new_method',
      v_existing_operation_after_json->>'pay_method',
      ''
    )));

    IF v_existing_operation_source_method NOT IN ('PAYE', 'UMBRELLA')
       OR v_existing_operation_target_method NOT IN ('PAYE', 'UMBRELLA')
       OR v_existing_operation_source_method = v_existing_operation_target_method THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_OPERATION_AUDIT_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = format(
                'operation=%s audit=%s source=%s target=%s',
                v_operation_id,
                v_existing_operation_audit_id,
                COALESCE(v_existing_operation_source_method, ''),
                COALESCE(v_existing_operation_target_method, '')
              );
    END IF;

    IF v_existing_operation_source_method <> v_expected_old_method
       OR v_existing_operation_target_method <> v_new_method THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_OPERATION_ID_CONFLICT'
        USING ERRCODE = '40001',
              DETAIL = format(
                'operation=%s committed=%s_to_%s requested=%s_to_%s',
                v_operation_id,
                v_existing_operation_source_method,
                v_existing_operation_target_method,
                v_expected_old_method,
                v_new_method
              );
    END IF;

    IF jsonb_typeof(v_existing_operation_after_json->'operation_request') = 'object' THEN
      IF (v_existing_operation_after_json->'operation_request') IS DISTINCT FROM v_operation_request_json THEN
        RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_OPERATION_ID_CONFLICT'
          USING ERRCODE = '40001',
                DETAIL = format('operation=%s request_payload_mismatch', v_operation_id);
      END IF;
    ELSIF v_new_method = 'UMBRELLA' THEN
      IF v_destination_patch ? 'account_holder'
         OR v_destination_patch ? 'bank_name'
         OR v_destination_patch ? 'sort_code'
         OR v_destination_patch ? 'account_number'
         OR (
           v_destination_patch ? 'umbrella_id'
           AND NULLIF(BTRIM(COALESCE(v_destination_patch->>'umbrella_id', '')), '')
             IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_existing_operation_after_json->>'umbrella_id', '')), '')
         ) THEN
        RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_OPERATION_ID_CONFLICT'
          USING ERRCODE = '40001',
                DETAIL = format('operation=%s legacy_destination_payload_mismatch', v_operation_id);
      END IF;
    ELSE
      IF NULLIF(BTRIM(COALESCE(v_destination_patch->>'umbrella_id', '')), '') IS NOT NULL
         OR (
           v_destination_patch ? 'account_holder'
           AND NULLIF(BTRIM(COALESCE(v_destination_patch->>'account_holder', '')), '')
             IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_existing_operation_after_json->>'account_holder', '')), '')
         )
         OR (
           v_destination_patch ? 'bank_name'
           AND NULLIF(BTRIM(COALESCE(v_destination_patch->>'bank_name', '')), '')
             IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_existing_operation_after_json->>'bank_name', '')), '')
         )
         OR (
           v_destination_patch ? 'sort_code'
           AND NULLIF(BTRIM(COALESCE(v_destination_patch->>'sort_code', '')), '')
             IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_existing_operation_after_json->>'sort_code', '')), '')
         )
         OR (
           v_destination_patch ? 'account_number'
           AND NULLIF(BTRIM(COALESCE(v_destination_patch->>'account_number', '')), '')
             IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_existing_operation_after_json->>'account_number', '')), '')
         ) THEN
        RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_OPERATION_ID_CONFLICT'
          USING ERRCODE = '40001',
                DETAIL = format('operation=%s legacy_destination_payload_mismatch', v_operation_id);
      END IF;
    END IF;

    IF COALESCE(
         v_existing_operation_result->>'job_id',
         v_existing_operation_after_json->>'job_id',
         ''
       ) ~* v_uuid_re THEN
      v_existing_operation_job_id := COALESCE(
        v_existing_operation_result->>'job_id',
        v_existing_operation_after_json->>'job_id'
      )::uuid;
    END IF;

    IF v_existing_operation_job_id IS NOT NULL THEN
      SELECT job_row.status
      INTO v_existing_operation_job_status
      FROM public.banking_pay_workbench_jobs AS job_row
      WHERE job_row.id = v_existing_operation_job_id
        AND job_row.candidate_id = p_candidate_id
      LIMIT 1;
    END IF;

    v_existing_operation_job_status := COALESCE(
      v_existing_operation_job_status,
      NULLIF(BTRIM(COALESCE(v_existing_operation_result->>'job_status', '')), ''),
      NULLIF(BTRIM(COALESCE(v_existing_operation_after_json->>'job_status', '')), '')
    );
    v_existing_operation_source_change_seq := COALESCE(
      CASE
        WHEN COALESCE(v_existing_operation_result->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
          THEN (v_existing_operation_result->>'source_change_seq')::bigint
        WHEN COALESCE(v_existing_operation_after_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
          THEN (v_existing_operation_after_json->>'source_change_seq')::bigint
      END,
      0
    );

    IF v_existing_operation_source_change_seq > 0 THEN
      SELECT EXISTS (
        SELECT 1
        FROM public.audit_events AS later_audit
        WHERE later_audit.object_type = 'candidate'
          AND later_audit.object_id_text = p_candidate_id::text
          AND later_audit.action = 'PAY_METHOD_CHANGE_PROSPECTIVE_ONLY'
          AND later_audit.id <> v_existing_operation_audit_id
          AND CASE
            WHEN COALESCE(later_audit.after_json#>>'{operation_result,source_change_seq}', '') ~ '^[0-9]{1,18}$'
              THEN (later_audit.after_json#>>'{operation_result,source_change_seq}')::bigint
            WHEN COALESCE(later_audit.after_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (later_audit.after_json->>'source_change_seq')::bigint
            ELSE 0
          END > v_existing_operation_source_change_seq
      )
      INTO v_existing_operation_superseded;
    END IF;

    v_existing_operation_superseded := COALESCE(v_existing_operation_superseded, false)
      OR v_old_method <> v_existing_operation_target_method;

    v_result := jsonb_strip_nulls(
      COALESCE(v_existing_operation_result, '{}'::jsonb)
      || jsonb_build_object(
        'ok', true,
        'idempotent_replay', true,
        'operation_committed', true,
        'operation_superseded_by_later_change', v_existing_operation_superseded,
        'operation_id', v_operation_id::text,
        'candidate_id', p_candidate_id::text,
        'original_method', v_existing_operation_source_method,
        'new_method', v_existing_operation_target_method,
        'current_method', v_old_method,
        'candidate', v_before_json,
        'job_id', CASE WHEN v_existing_operation_job_id IS NULL THEN NULL ELSE v_existing_operation_job_id::text END,
        'job_status', v_existing_operation_job_status
      )
      || jsonb_build_object(
        'targeted_timesheet_ids', CASE
          WHEN jsonb_typeof(v_existing_operation_result->'targeted_timesheet_ids') = 'array'
            THEN v_existing_operation_result->'targeted_timesheet_ids'
          WHEN jsonb_typeof(v_existing_operation_after_json->'targeted_timesheet_ids') = 'array'
            THEN v_existing_operation_after_json->'targeted_timesheet_ids'
          ELSE '[]'::jsonb
        END,
        'authorised_timesheet_ids', CASE
          WHEN jsonb_typeof(v_existing_operation_result->'authorised_timesheet_ids') = 'array'
            THEN v_existing_operation_result->'authorised_timesheet_ids'
          WHEN jsonb_typeof(v_existing_operation_after_json->'authorised_timesheet_ids') = 'array'
            THEN v_existing_operation_after_json->'authorised_timesheet_ids'
          ELSE '[]'::jsonb
        END,
        'active_advance_timesheet_ids', CASE
          WHEN jsonb_typeof(v_existing_operation_result->'active_advance_timesheet_ids') = 'array'
            THEN v_existing_operation_result->'active_advance_timesheet_ids'
          WHEN jsonb_typeof(v_existing_operation_after_json->'active_advance_timesheet_ids') = 'array'
            THEN v_existing_operation_after_json->'active_advance_timesheet_ids'
          ELSE '[]'::jsonb
        END,
        'authoritative_sessions', CASE
          WHEN jsonb_typeof(v_existing_operation_result->'authoritative_sessions') = 'array'
            THEN v_existing_operation_result->'authoritative_sessions'
          WHEN jsonb_typeof(v_existing_operation_after_json->'authoritative_sessions') = 'array'
            THEN v_existing_operation_after_json->'authoritative_sessions'
          ELSE '[]'::jsonb
        END,
        'replaced_source_session_ids', CASE
          WHEN jsonb_typeof(v_existing_operation_result->'replaced_source_session_ids') = 'array'
            THEN v_existing_operation_result->'replaced_source_session_ids'
          WHEN jsonb_typeof(v_existing_operation_after_json->'replaced_source_session_ids') = 'array'
            THEN v_existing_operation_after_json->'replaced_source_session_ids'
          ELSE '[]'::jsonb
        END,
        'target_details', CASE
          WHEN jsonb_typeof(v_existing_operation_result->'target_details') = 'array'
            THEN v_existing_operation_result->'target_details'
          WHEN jsonb_typeof(v_existing_operation_after_json->'target_details') = 'array'
            THEN v_existing_operation_after_json->'target_details'
          ELSE '[]'::jsonb
        END
      )
      || jsonb_build_object(
        'targeted_timesheet_count', COALESCE(
          CASE
            WHEN COALESCE(v_existing_operation_result->>'targeted_timesheet_count', '') ~ '^[0-9]{1,10}$'
              THEN (v_existing_operation_result->>'targeted_timesheet_count')::integer
            WHEN jsonb_typeof(v_existing_operation_after_json->'targeted_timesheet_ids') = 'array'
              THEN jsonb_array_length(v_existing_operation_after_json->'targeted_timesheet_ids')
          END,
          0
        ),
        'authorised_timesheet_count', COALESCE(
          CASE
            WHEN COALESCE(v_existing_operation_result->>'authorised_timesheet_count', '') ~ '^[0-9]{1,10}$'
              THEN (v_existing_operation_result->>'authorised_timesheet_count')::integer
            WHEN jsonb_typeof(v_existing_operation_after_json->'authorised_timesheet_ids') = 'array'
              THEN jsonb_array_length(v_existing_operation_after_json->'authorised_timesheet_ids')
          END,
          0
        ),
        'active_advance_timesheet_count', COALESCE(
          CASE
            WHEN COALESCE(v_existing_operation_result->>'active_advance_timesheet_count', '') ~ '^[0-9]{1,10}$'
              THEN (v_existing_operation_result->>'active_advance_timesheet_count')::integer
            WHEN jsonb_typeof(v_existing_operation_after_json->'active_advance_timesheet_ids') = 'array'
              THEN jsonb_array_length(v_existing_operation_after_json->'active_advance_timesheet_ids')
          END,
          0
        ),
        'source_target_mismatch_count', COALESCE(
          CASE
            WHEN COALESCE(v_existing_operation_result->>'source_target_mismatch_count', '') ~ '^[0-9]{1,10}$'
              THEN (v_existing_operation_result->>'source_target_mismatch_count')::integer
            WHEN COALESCE(v_existing_operation_after_json->>'source_target_mismatch_count', '') ~ '^[0-9]{1,10}$'
              THEN (v_existing_operation_after_json->>'source_target_mismatch_count')::integer
          END,
          0
        ),
        'source_change_seq', COALESCE(
          CASE
            WHEN COALESCE(v_existing_operation_result->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (v_existing_operation_result->>'source_change_seq')::bigint
            WHEN COALESCE(v_existing_operation_after_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (v_existing_operation_after_json->>'source_change_seq')::bigint
          END,
          0
        ),
        'preview_source_change_seq', COALESCE(
          CASE
            WHEN COALESCE(v_existing_operation_result->>'preview_source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (v_existing_operation_result->>'preview_source_change_seq')::bigint
            WHEN COALESCE(v_existing_operation_after_json->>'preview_source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (v_existing_operation_after_json->>'preview_source_change_seq')::bigint
          END,
          v_preview_source_change_seq
        )
      )
      || jsonb_build_object(
        'contracts_changed', 0,
        'contract_weeks_changed', 0,
        'timesheets_changed', 0,
        'rates_changed', 0,
        'tsfin_repricing_rows', 0,
        'refresh_accepted', true,
        'refresh_completed', CASE
          WHEN v_existing_operation_superseded IS TRUE THEN false
          WHEN UPPER(BTRIM(COALESCE(v_existing_operation_job_status, ''))) = 'SUCCEEDED' THEN true
          ELSE COALESCE((v_existing_operation_result->>'refresh_completed')::boolean, false)
        END,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
        'policy_x_dirtying_only', true,
        'economic_truth_mutation_allowed', false
      )
    );

    RETURN v_result;
  END IF;

  IF v_old_method = v_new_method THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_ALREADY_APPLIED_OR_STALE'
      USING ERRCODE = '40001',
            DETAIL = format('candidate=%s current=%s operation=%s', p_candidate_id, v_old_method, v_operation_id);
  END IF;

  IF v_old_method <> v_expected_old_method THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_STALE_EXPECTED_METHOD'
      USING ERRCODE = '40001', DETAIL = format('expected=%s actual=%s', v_expected_old_method, v_old_method);
  END IF;

  IF v_old_method = v_new_method THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_METHODS_MUST_DIFFER'
      USING ERRCODE = '22023';
  END IF;

  v_scope_result := public.candidate_pay_method_change_refresh_scope_v1(
    p_candidate_id,
    v_old_method,
    v_new_method
  );

  v_current_source_change_seq := COALESCE(
    CASE
      WHEN COALESCE(v_scope_result->>'latest_source_change_seq', '') ~ '^[0-9]{1,18}$'
        THEN (v_scope_result->>'latest_source_change_seq')::bigint
      ELSE 0
    END,
    0
  );

  IF v_preview_source_change_seq <> v_current_source_change_seq THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_PREVIEW_STALE'
      USING ERRCODE = '40001', DETAIL = format('preview_seq=%s current_seq=%s', v_preview_source_change_seq, v_current_source_change_seq);
  END IF;

  SELECT COALESCE(array_agg(target_value::uuid ORDER BY target_value::uuid), ARRAY[]::uuid[])
  INTO v_expected_targeted_timesheet_ids
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(v_scope_result->'targeted_timesheet_ids') = 'array'
        THEN v_scope_result->'targeted_timesheet_ids'
      ELSE '[]'::jsonb
    END
  ) AS target_values(target_value)
  WHERE target_value ~* v_uuid_re;

  IF v_new_method = 'UMBRELLA' THEN
    IF v_destination_patch ? 'account_holder'
       OR v_destination_patch ? 'bank_name'
       OR v_destination_patch ? 'sort_code'
       OR v_destination_patch ? 'account_number' THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_PAYE_BANK_FIELDS_NOT_ALLOWED_FOR_UMBRELLA'
        USING ERRCODE = '22023';
    END IF;

    IF v_destination_patch ? 'umbrella_id' THEN
      IF NULLIF(BTRIM(COALESCE(v_destination_patch->>'umbrella_id', '')), '') IS NULL THEN
        v_effective_umbrella_id := NULL::uuid;
      ELSIF v_destination_patch->>'umbrella_id' ~* v_uuid_re THEN
        v_effective_umbrella_id := (v_destination_patch->>'umbrella_id')::uuid;
      ELSE
        RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_UMBRELLA_ID_INVALID'
          USING ERRCODE = '22023';
      END IF;
    END IF;

    v_account_holder := NULL::text;
    v_bank_name := NULL::text;
    v_sort_code := NULL::text;
    v_account_number := NULL::text;

    IF v_effective_umbrella_id IS NULL THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_UMBRELLA_REQUIRED'
        USING ERRCODE = '23502';
    END IF;

    PERFORM 1
    FROM public.umbrellas AS umbrella_row
    WHERE umbrella_row.id = v_effective_umbrella_id
      AND umbrella_row.enabled IS TRUE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_UMBRELLA_NOT_FOUND_OR_DISABLED'
        USING ERRCODE = '23503', DETAIL = v_effective_umbrella_id::text;
    END IF;
  ELSE
    IF NULLIF(BTRIM(COALESCE(v_destination_patch->>'umbrella_id', '')), '') IS NOT NULL THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_UMBRELLA_ID_NOT_ALLOWED_FOR_PAYE'
        USING ERRCODE = '22023';
    END IF;

    v_effective_umbrella_id := NULL::uuid;

    IF v_destination_patch ? 'account_holder' THEN
      v_account_holder := NULLIF(BTRIM(COALESCE(v_destination_patch->>'account_holder', '')), '');
    END IF;
    IF v_destination_patch ? 'bank_name' THEN
      v_bank_name := NULLIF(BTRIM(COALESCE(v_destination_patch->>'bank_name', '')), '');
    END IF;
    IF v_destination_patch ? 'sort_code' THEN
      v_sort_code := NULLIF(BTRIM(COALESCE(v_destination_patch->>'sort_code', '')), '');
    END IF;
    IF v_destination_patch ? 'account_number' THEN
      v_account_number := NULLIF(BTRIM(COALESCE(v_destination_patch->>'account_number', '')), '');
    END IF;
  END IF;

  PERFORM set_config('cloudtms.candidate_pay_method_change_operation_id', v_operation_id::text, true);
  PERFORM set_config('cloudtms.candidate_pay_method_change_actor_user_id', p_actor_user_id::text, true);
  PERFORM set_config('cloudtms.candidate_pay_method_change_reason', COALESCE(NULLIF(BTRIM(p_reason), ''), 'PAY_METHOD_CHANGE'), true);

  UPDATE public.candidates AS candidate_row
  SET pay_method = v_new_method,
      umbrella_id = v_effective_umbrella_id,
      account_holder = v_account_holder,
      bank_name = v_bank_name,
      sort_code = v_sort_code,
      account_number = v_account_number,
      updated_at = v_now
  WHERE candidate_row.id = p_candidate_id
  RETURNING jsonb_build_object(
    'id', candidate_row.id::text,
    'pay_method', candidate_row.pay_method,
    'umbrella_id', CASE WHEN candidate_row.umbrella_id IS NULL THEN NULL ELSE candidate_row.umbrella_id::text END,
    'account_holder', candidate_row.account_holder,
    'bank_name', candidate_row.bank_name,
    'sort_code', candidate_row.sort_code,
    'account_number', candidate_row.account_number,
    'updated_at', candidate_row.updated_at,
    'rev', candidate_row.rev
  )
  INTO v_after_json;

  SELECT
    job_row.id,
    job_row.status,
    COALESCE(job_row.payload_json, '{}'::jsonb)
  INTO v_job_id, v_job_status, v_job_payload
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.candidate_id = p_candidate_id
    AND UPPER(BTRIM(COALESCE(job_row.job_type, ''))) = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
    AND job_row.payload_json->>'route_change_operation_id' = v_operation_id::text
    AND UPPER(BTRIM(COALESCE(job_row.payload_json->>'route_change_source_method', ''))) = v_old_method
    AND UPPER(BTRIM(COALESCE(job_row.payload_json->>'route_change_target_method', ''))) = v_new_method
  ORDER BY job_row.updated_at_utc DESC, job_row.id DESC
  LIMIT 1;

  IF v_job_id IS NULL THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DURABLE_QUEUE_MISSING'
      USING ERRCODE = 'P0001', DETAIL = v_operation_id::text;
  END IF;

  SELECT COALESCE(array_agg(target_value::uuid ORDER BY target_value::uuid), ARRAY[]::uuid[])
  INTO v_enqueued_targeted_timesheet_ids
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(v_job_payload->'targeted_timesheet_ids') = 'array'
        THEN v_job_payload->'targeted_timesheet_ids'
      ELSE '[]'::jsonb
    END
  ) AS target_values(target_value)
  WHERE target_value ~* v_uuid_re;

  IF v_enqueued_targeted_timesheet_ids IS DISTINCT FROM v_expected_targeted_timesheet_ids THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_QUEUE_SET_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = format(
              'expected=%s enqueued=%s',
              COALESCE(array_to_string(v_expected_targeted_timesheet_ids, ','), ''),
              COALESCE(array_to_string(v_enqueued_targeted_timesheet_ids, ','), '')
            );
  END IF;

  v_job_source_change_seq := COALESCE(
    CASE
      WHEN COALESCE(v_job_payload->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
        THEN (v_job_payload->>'source_change_seq')::bigint
      ELSE 0
    END,
    0
  );

  v_result := jsonb_strip_nulls(
    jsonb_build_object(
      'ok', true,
      'idempotent_replay', false,
      'operation_committed', true,
      'operation_superseded_by_later_change', false,
      'operation_id', v_operation_id::text,
      'candidate_id', p_candidate_id::text,
      'original_method', v_old_method,
      'new_method', v_new_method,
      'current_method', v_new_method,
      'candidate', v_after_json,
      'job_id', v_job_id::text,
      'job_status', v_job_status,
      'source_change_seq', v_job_source_change_seq,
      'preview_source_change_seq', v_preview_source_change_seq
    )
    || jsonb_build_object(
      'targeted_timesheet_ids', to_jsonb(v_enqueued_targeted_timesheet_ids),
      'targeted_timesheet_count', COALESCE(array_length(v_enqueued_targeted_timesheet_ids, 1), 0),
      'authorised_timesheet_ids', CASE
        WHEN jsonb_typeof(v_job_payload->'authorised_timesheet_ids') = 'array'
          THEN v_job_payload->'authorised_timesheet_ids'
        ELSE '[]'::jsonb
      END,
      'authorised_timesheet_count', COALESCE(
        CASE
          WHEN COALESCE(v_job_payload->>'authorised_timesheet_count', '') ~ '^[0-9]{1,10}$'
            THEN (v_job_payload->>'authorised_timesheet_count')::integer
        END,
        0
      ),
      'active_advance_timesheet_ids', CASE
        WHEN jsonb_typeof(v_job_payload->'active_advance_timesheet_ids') = 'array'
          THEN v_job_payload->'active_advance_timesheet_ids'
        ELSE '[]'::jsonb
      END,
      'active_advance_timesheet_count', COALESCE(
        CASE
          WHEN COALESCE(v_job_payload->>'active_advance_timesheet_count', '') ~ '^[0-9]{1,10}$'
            THEN (v_job_payload->>'active_advance_timesheet_count')::integer
        END,
        0
      ),
      'authoritative_sessions', CASE
        WHEN jsonb_typeof(v_job_payload->'authoritative_sessions') = 'array'
          THEN v_job_payload->'authoritative_sessions'
        ELSE '[]'::jsonb
      END,
      'replaced_source_session_ids', CASE
        WHEN jsonb_typeof(v_job_payload->'replaced_source_session_ids') = 'array'
          THEN v_job_payload->'replaced_source_session_ids'
        ELSE '[]'::jsonb
      END,
      'target_details', CASE
        WHEN jsonb_typeof(v_job_payload->'target_details') = 'array'
          THEN v_job_payload->'target_details'
        ELSE '[]'::jsonb
      END,
      'source_target_mismatch_count', COALESCE(
        CASE
          WHEN COALESCE(v_job_payload->>'source_target_mismatch_count', '') ~ '^[0-9]{1,10}$'
            THEN (v_job_payload->>'source_target_mismatch_count')::integer
        END,
        0
      )
    )
    || jsonb_build_object(
      'contracts_changed', 0,
      'contract_weeks_changed', 0,
      'timesheets_changed', 0,
      'rates_changed', 0,
      'tsfin_repricing_rows', 0,
      'refresh_accepted', true,
      'refresh_completed', UPPER(BTRIM(COALESCE(v_job_status, ''))) = 'SUCCEEDED',
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
      'policy_x_dirtying_only', true,
      'economic_truth_mutation_allowed', false
    )
  );

  v_audit_after_json := v_after_json
    || jsonb_build_object(
      'operation_id', v_operation_id::text,
      'original_method', v_old_method,
      'new_method', v_new_method,
      'job_id', v_job_id::text,
      'job_status', v_job_status,
      'source_change_seq', v_job_source_change_seq,
      'preview_source_change_seq', v_preview_source_change_seq,
      'operation_request', v_operation_request_json
    )
    || jsonb_build_object(
      'targeted_timesheet_ids', v_result->'targeted_timesheet_ids',
      'authorised_timesheet_ids', v_result->'authorised_timesheet_ids',
      'active_advance_timesheet_ids', v_result->'active_advance_timesheet_ids',
      'authoritative_sessions', v_result->'authoritative_sessions',
      'replaced_source_session_ids', v_result->'replaced_source_session_ids',
      'target_details', v_result->'target_details',
      'source_target_mismatch_count', v_result->'source_target_mismatch_count'
    )
    || jsonb_build_object(
      'contracts_changed', 0,
      'contract_weeks_changed', 0,
      'timesheets_changed', 0,
      'rates_changed', 0,
      'tsfin_repricing_rows', 0,
      'operation_result', v_result
    );

  PERFORM public._audit_insert(
    'candidate',
    p_candidate_id::text,
    'PAY_METHOD_CHANGE_PROSPECTIVE_ONLY',
    v_before_json,
    v_audit_after_json,
    COALESCE(NULLIF(BTRIM(p_reason), ''), 'PAY_METHOD_CHANGE'),
    p_actor_user_id
  );

  RETURN v_result;
END;
$function$;

REVOKE ALL ON FUNCTION public.candidate_pay_method_change_apply(uuid, text, jsonb, uuid, text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.candidate_pay_method_change_apply(uuid, text, jsonb, uuid, text) TO service_role;

-- =========================================================
-- 3) NEW RPCs: Candidate delete eligibility + apply (tombstones + audit + change bump)
--    Verified:
--      candidates_tombstones(id, deleted_rev, deleted_at)
--      app_change_counters(entity_key, seq, updated_at)
-- =========================================================

CREATE OR REPLACE FUNCTION public.candidate_delete_eligibility(p_candidate_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_contracts int;
  v_contract_weeks int;
  v_timesheets int;
  v_invoice_lines int;
  v_pay_batches int;
  v_reason text := null;
  v_can boolean := true;
BEGIN
  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  SELECT COUNT(*) INTO v_contracts
  FROM public.contracts ct
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_contract_weeks
  FROM public.contract_weeks cw
  JOIN public.contracts ct ON ct.id = cw.contract_id
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_timesheets
  FROM public.timesheets t
  JOIN public.contracts ct ON ct.id = t.contract_id
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_invoice_lines
  FROM public.invoice_lines il
  JOIN public.timesheets t ON t.timesheet_id = il.timesheet_id
  JOIN public.contracts ct ON ct.id = t.contract_id
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_pay_batches
  FROM public.pay_batch_candidates pbc
  WHERE pbc.candidate_id = p_candidate_id;

  IF v_contracts > 0 OR v_contract_weeks > 0 OR v_timesheets > 0 OR v_invoice_lines > 0 OR v_pay_batches > 0 THEN
    v_can := false;
    v_reason := 'Candidate cannot be deleted because related records exist.';
  END IF;

  RETURN jsonb_build_object(
    'candidate_id', p_candidate_id::text,
    'can_delete', v_can,
    'reason', v_reason,
    'blockers', jsonb_build_object(
      'contracts', v_contracts,
      'contract_weeks', v_contract_weeks,
      'timesheets', v_timesheets,
      'invoice_lines', v_invoice_lines,
      'pay_batch_candidates', v_pay_batches
    )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.candidate_delete_apply(p_candidate_id uuid, p_actor_user_id uuid, p_reason text DEFAULT null)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_elig jsonb;
  v_can boolean;
  v_deleted_rev bigint;
  v_seq bigint;
  v_before jsonb;
BEGIN
  v_elig := public.candidate_delete_eligibility(p_candidate_id);
  v_can := COALESCE((v_elig->>'can_delete')::boolean, false);

  IF v_can IS NOT TRUE THEN
    RAISE EXCEPTION '%', COALESCE(v_elig->>'reason', 'Candidate cannot be deleted');
  END IF;

  -- Lock candidate to ensure consistent delete
  SELECT jsonb_build_object(
           'id', c.id::text,
           'tms_ref', c.tms_ref,
           'display_name', c.display_name,
           'active', c.active
         )
    INTO v_before
  FROM public.candidates c
  WHERE c.id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Candidate not found';
  END IF;

  -- Non-blocking dependents (verified tables)
  DELETE FROM public.candidate_job_titles cjt WHERE cjt.candidate_id = p_candidate_id;
  DELETE FROM public.rates_candidate_overrides rco WHERE rco.candidate_id = p_candidate_id;
  DELETE FROM public.legacy_eclipse_candidate_map lem WHERE lem.candidate_id = p_candidate_id;

  -- Delete candidate
  DELETE FROM public.candidates c WHERE c.id = p_candidate_id;

  -- Bump change counter and record deleted_rev
  PERFORM public._change_bump('candidates');
  SELECT acc.seq INTO v_seq
  FROM public.app_change_counters acc
  WHERE acc.entity_key = 'candidates'
  LIMIT 1;

  v_deleted_rev := COALESCE(v_seq, 0);

  INSERT INTO public.candidates_tombstones (id, deleted_rev, deleted_at)
  VALUES (p_candidate_id, v_deleted_rev, now())
  ON CONFLICT (id) DO UPDATE
    SET deleted_rev = EXCLUDED.deleted_rev,
        deleted_at  = EXCLUDED.deleted_at;

  PERFORM public._audit_insert(
    'candidates',
    p_candidate_id::text,
    'DELETE',
    v_before,
    jsonb_build_object('deleted_rev', v_deleted_rev),
    COALESCE(p_reason, 'DELETE'),
    p_actor_user_id
  );

  RETURN jsonb_build_object('deleted', true, 'candidate_id', p_candidate_id::text, 'deleted_rev', v_deleted_rev);
END;
$function$;


-- =========================================================
-- 4) NEW RPCs: Client delete eligibility + apply (tombstones + audit + change bump)
-- =========================================================

CREATE OR REPLACE FUNCTION public.client_delete_eligibility(p_client_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_contracts int;
  v_contract_weeks int;
  v_timesheets int;
  v_invoices int;
  v_reason text := null;
  v_can boolean := true;
BEGIN
  IF p_client_id IS NULL THEN
    RAISE EXCEPTION 'client_id is required';
  END IF;

  SELECT COUNT(*) INTO v_contracts
  FROM public.contracts ct
  WHERE ct.client_id = p_client_id;

  SELECT COUNT(*) INTO v_contract_weeks
  FROM public.contract_weeks cw
  JOIN public.contracts ct ON ct.id = cw.contract_id
  WHERE ct.client_id = p_client_id;

  SELECT COUNT(*) INTO v_timesheets
  FROM public.timesheets t
  JOIN public.contracts ct ON ct.id = t.contract_id
  WHERE ct.client_id = p_client_id;

  SELECT COUNT(*) INTO v_invoices
  FROM public.invoices inv
  WHERE inv.client_id = p_client_id;

  IF v_contracts > 0 OR v_contract_weeks > 0 OR v_timesheets > 0 OR v_invoices > 0 THEN
    v_can := false;
    v_reason := 'Client cannot be deleted because related records exist.';
  END IF;

  RETURN jsonb_build_object(
    'client_id', p_client_id::text,
    'can_delete', v_can,
    'reason', v_reason,
    'blockers', jsonb_build_object(
      'contracts', v_contracts,
      'contract_weeks', v_contract_weeks,
      'timesheets', v_timesheets,
      'invoices', v_invoices
    )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.client_delete_apply(p_client_id uuid, p_actor_user_id uuid, p_reason text DEFAULT null)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_elig jsonb;
  v_can boolean;
  v_deleted_rev bigint;
  v_seq bigint;
  v_before jsonb;
BEGIN
  v_elig := public.client_delete_eligibility(p_client_id);
  v_can := COALESCE((v_elig->>'can_delete')::boolean, false);

  IF v_can IS NOT TRUE THEN
    RAISE EXCEPTION '%', COALESCE(v_elig->>'reason', 'Client cannot be deleted');
  END IF;

  -- Lock client row
  SELECT jsonb_build_object(
           'id', c.id::text,
           'name', c.name,
           'cli_ref', c.cli_ref
         )
    INTO v_before
  FROM public.clients c
  WHERE c.id = p_client_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Client not found';
  END IF;

  -- Non-blocking dependents (verified tables)
  DELETE FROM public.client_hospitals ch WHERE ch.client_id = p_client_id;
  DELETE FROM public.client_settings cs WHERE cs.client_id = p_client_id;
  DELETE FROM public.rates_client_defaults rcd WHERE rcd.client_id = p_client_id;
  DELETE FROM public.legacy_eclipse_client_map lec WHERE lec.client_id = p_client_id;

  -- Delete client
  DELETE FROM public.clients c WHERE c.id = p_client_id;

  -- Bump change counter and record deleted_rev
  PERFORM public._change_bump('clients');
  SELECT acc.seq INTO v_seq
  FROM public.app_change_counters acc
  WHERE acc.entity_key = 'clients'
  LIMIT 1;

  v_deleted_rev := COALESCE(v_seq, 0);

  INSERT INTO public.clients_tombstones (id, deleted_rev, deleted_at)
  VALUES (p_client_id, v_deleted_rev, now())
  ON CONFLICT (id) DO UPDATE
    SET deleted_rev = EXCLUDED.deleted_rev,
        deleted_at  = EXCLUDED.deleted_at;

  PERFORM public._audit_insert(
    'clients',
    p_client_id::text,
    'DELETE',
    v_before,
    jsonb_build_object('deleted_rev', v_deleted_rev),
    COALESCE(p_reason, 'DELETE'),
    p_actor_user_id
  );

  RETURN jsonb_build_object('deleted', true, 'client_id', p_client_id::text, 'deleted_rev', v_deleted_rev);
END;
$function$;


-- =========================================================
-- 5) NEW RPC: Job Titles delete apply (category deletes descendants if safe)
--    Verified tables:
--      default_job_titles(id,label,parent_id,depth,is_role,active,created_at,updated_at,...)
--      candidate_job_titles(candidate_id,job_title_id,is_primary,...)
-- =========================================================

CREATE OR REPLACE FUNCTION public.job_titles_delete_apply(p_job_title_id uuid, p_actor_user_id uuid, p_reason text DEFAULT null)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_is_role boolean;
  v_in_use int;
  v_before jsonb;
BEGIN
  IF p_job_title_id IS NULL THEN
    RAISE EXCEPTION 'job_title_id is required';
  END IF;

  SELECT jsonb_build_object('id', djt.id::text, 'label', djt.label, 'parent_id', CASE WHEN djt.parent_id IS NULL THEN null ELSE djt.parent_id::text END, 'is_role', djt.is_role),
         djt.is_role
    INTO v_before, v_is_role
  FROM public.default_job_titles djt
  WHERE djt.id = p_job_title_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Job title not found';
  END IF;

  IF v_is_role THEN
    SELECT COUNT(*) INTO v_in_use
    FROM public.candidate_job_titles cjt
    WHERE cjt.job_title_id = p_job_title_id;

    IF v_in_use > 0 THEN
      RAISE EXCEPTION 'Candidates have been assigned to this role. You need to reassign the candidates before you can delete this role';
    END IF;

    DELETE FROM public.default_job_titles djt WHERE djt.id = p_job_title_id;

    PERFORM public._audit_insert(
      'job_titles',
      p_job_title_id::text,
      'DELETE_ROLE',
      v_before,
      jsonb_build_object('deleted', true),
      COALESCE(p_reason, 'DELETE_JOB_TITLE'),
      p_actor_user_id
    );

    RETURN jsonb_build_object('deleted', true, 'kind', 'role', 'id', p_job_title_id::text);
  END IF;

  -- Category: gather all descendants
  WITH RECURSIVE tree AS (
    SELECT d1.id, d1.parent_id, d1.is_role
    FROM public.default_job_titles d1
    WHERE d1.id = p_job_title_id
    UNION ALL
    SELECT d2.id, d2.parent_id, d2.is_role
    FROM public.default_job_titles d2
    JOIN tree t ON d2.parent_id = t.id
  ),
  descendants AS (
    SELECT id, is_role
    FROM tree
    WHERE id <> p_job_title_id
  ),
  role_desc AS (
    SELECT id
    FROM descendants
    WHERE is_role = true
  )
  SELECT COUNT(*)
    INTO v_in_use
  FROM public.candidate_job_titles cjt
  WHERE cjt.job_title_id IN (SELECT id FROM role_desc);

  IF v_in_use > 0 THEN
    RAISE EXCEPTION 'Candidates have been assigned to roles in this category. You need to reassign the candidates before you can delete this category';
  END IF;

  -- Delete descendants first, then category
  WITH RECURSIVE tree AS (
    SELECT d1.id, d1.parent_id
    FROM public.default_job_titles d1
    WHERE d1.id = p_job_title_id
    UNION ALL
    SELECT d2.id, d2.parent_id
    FROM public.default_job_titles d2
    JOIN tree t ON d2.parent_id = t.id
  )
  DELETE FROM public.default_job_titles djt
  WHERE djt.id IN (SELECT id FROM tree)
    AND djt.id <> p_job_title_id;

  DELETE FROM public.default_job_titles djt WHERE djt.id = p_job_title_id;

  PERFORM public._audit_insert(
    'job_titles',
    p_job_title_id::text,
    'DELETE_CATEGORY',
    v_before,
    jsonb_build_object('deleted', true),
    COALESCE(p_reason, 'DELETE_JOB_TITLE'),
    p_actor_user_id
  );

  RETURN jsonb_build_object('deleted', true, 'kind', 'category', 'id', p_job_title_id::text);
END;
$function$;

create or replace function public.invoice_quicksearch_ids(
  p_q text,
  p_limit integer default 20000
)
returns table(invoice_id uuid)
language sql
stable
security definer
set search_path = public
as $$
  select s.invoice_id
  from (
    select distinct on (i.id)
      i.id as invoice_id,
      coalesce(i.issued_at_utc, i.created_at) as sort_ts,
      i.invoice_no as sort_no
    from public.invoices as i
    left join public.clients as c
      on c.id = i.client_id
    left join public.invoice_lines as il
      on il.invoice_id = i.id
    left join public.v_timesheets_summary as vts
      on vts.timesheet_id = il.timesheet_id
    where
      p_q is not null
      and btrim(p_q) <> ''
      and (
        i.invoice_no ilike ('%' || p_q || '%')
        or c.name ilike ('%' || p_q || '%')
        or vts.candidate_name ilike ('%' || p_q || '%')
      )
    -- DISTINCT ON requires i.id first in ORDER BY; after that we pick the “best” row per invoice
    order by
      i.id,
      coalesce(i.issued_at_utc, i.created_at) desc nulls last,
      i.invoice_no desc
  ) as s
  order by
    s.sort_ts desc nulls last,
    s.sort_no desc
  limit greatest(1, least(coalesce(p_limit, 20000), 20000));
$$;
