-- Canonical Banking Pay operation lease-release authority.
-- This identity was extracted from the legacy monolithic repeatable so it has
-- exactly one authoritative CREATE OR REPLACE FUNCTION body.




CREATE OR REPLACE FUNCTION public.banking_pay_operation_release_lease(p_operation_id uuid, p_lease_owner text, p_release_state text DEFAULT 'MORE_WORK'::text, p_run_after_delay_seconds integer DEFAULT 0, p_progress_patch_json jsonb DEFAULT '{}'::jsonb, p_result_patch_json jsonb DEFAULT NULL::jsonb, p_error_json jsonb DEFAULT NULL::jsonb, p_resume_reason text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_release_state text := upper(BTRIM(COALESCE(p_release_state, 'MORE_WORK')));
  v_delay_seconds integer := LEAST(GREATEST(COALESCE(p_run_after_delay_seconds, 0), 0), 3600);
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_operation_type text := NULL::text;
  v_next_status text := 'RUNNING';
  v_next_runner_state text := 'RUNNABLE';
  v_next_run_after_utc timestamptz := now();
  v_next_requires_user_action boolean := false;
  v_next_resume_reason text := NULL::text;
  v_completed_at_utc timestamptz := NULL::timestamptz;
  v_failed_at_utc timestamptz := NULL::timestamptz;
  v_retry_failure boolean := false;
  v_next_attempt_count integer := NULL::integer;
  v_attempt_limit_reached boolean := false;
  v_raw_progress_patch_json jsonb := '{}'::jsonb;
  v_progress_patch_json jsonb := '{}'::jsonb;
  v_progress_patch_key text := NULL::text;
  v_progress_patch_value jsonb := NULL::jsonb;
  v_progress_patch_array_count integer := 0;
  v_progress_patch_key_count integer := 0;
  v_result_patch_key_count integer := 0;
  v_progress_patch_bytes integer := 0;
  v_compact_progress_patch_bytes integer := 0;
  v_result_patch_bytes integer := NULL::integer;
  v_error_json_bytes integer := NULL::integer;
  v_existing_progress_json_bytes integer := 0;
  v_existing_result_json_bytes integer := NULL::integer;
  v_release_diag_json jsonb := '{}'::jsonb;
  v_clear_retryable_pre_provider_error boolean := false;
  v_cleared_retryable_pre_provider_progress_error boolean := false;
  v_previous_phase text := NULL::text;
  v_requested_phase text := NULL::text;
  v_phase_for_update text := NULL::text;
  v_execution_overlay_chain_v2 jsonb := NULL::jsonb;
BEGIN
  PERFORM set_config('lock_timeout', '3s', true);

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_OPERATION_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_OPERATION_ID_REQUIRED')::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(p_lease_owner, '')), '') IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_LEASE_OWNER_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_LEASE_OWNER_REQUIRED', 'operation_id', p_operation_id::text)::text;
  END IF;

  v_raw_progress_patch_json := COALESCE(p_progress_patch_json, '{}'::jsonb);

  IF p_progress_patch_json IS NOT NULL AND jsonb_typeof(p_progress_patch_json) <> 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_PROGRESS_PATCH_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_PROGRESS_PATCH_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF p_result_patch_json IS NOT NULL AND jsonb_typeof(p_result_patch_json) <> 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_RESULT_PATCH_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_RESULT_PATCH_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
  END IF;

  SELECT count(*)::integer
  INTO v_progress_patch_key_count
  FROM jsonb_object_keys(v_raw_progress_patch_json) AS progress_patch_keys(progress_key);

  SELECT count(*)::integer
  INTO v_result_patch_key_count
  FROM jsonb_object_keys(COALESCE(p_result_patch_json, '{}'::jsonb)) AS result_patch_keys(result_key);

  v_progress_patch_bytes := pg_column_size(v_raw_progress_patch_json);
  v_result_patch_bytes := CASE WHEN p_result_patch_json IS NULL THEN NULL::integer ELSE pg_column_size(p_result_patch_json) END;
  v_error_json_bytes := CASE WHEN p_error_json IS NULL THEN NULL::integer ELSE pg_column_size(p_error_json) END;

  FOR v_progress_patch_key, v_progress_patch_value IN
    SELECT progress_patch_entry.key, progress_patch_entry.value
    FROM jsonb_each(v_raw_progress_patch_json) AS progress_patch_entry(key, value)
  LOOP
    v_progress_patch_array_count := 0;
    IF v_progress_patch_key IN (
      'candidate_ids',
      'pending_candidate_ids',
      'failed_candidate_ids',
      'scope_ids',
      'transfer_ids',
      'transfer_scope_ids',
      'pay_batch_item_ids',
      'provider_events',
      'rows',
      'row_errors',
      'diagnostic_rows',
      'status_rows',
      'items',
      'proof_rows',
      'transfers',
      'session_progress',
      'recent_jobs',
      'transfer_scope_rollup_proofs',
      'transfer_scope_item_seed_proofs',
      'canonical_preview_lines_json',
      'full_preview_json',
      'provider_payload',
      'provider_response_payload',
      'full_provider_payload',
      'full_payload',
      'raw_payload'
    ) AND jsonb_typeof(v_progress_patch_value) IN ('array', 'object') THEN
      IF jsonb_typeof(v_progress_patch_value) = 'array' THEN
        v_progress_patch_array_count := jsonb_array_length(v_progress_patch_value);
      END IF;

      v_progress_patch_json := v_progress_patch_json || jsonb_build_object(
        v_progress_patch_key,
        jsonb_build_object(
          'omitted_heavy_release_progress_field', true,
          'json_type', jsonb_typeof(v_progress_patch_value),
          'array_count', CASE WHEN jsonb_typeof(v_progress_patch_value) = 'array' THEN v_progress_patch_array_count ELSE NULL END,
          'object_key_count_omitted', CASE WHEN jsonb_typeof(v_progress_patch_value) = 'object' THEN true ELSE NULL END,
          'value_bytes', pg_column_size(v_progress_patch_value)
        )
      );
    ELSIF jsonb_typeof(v_progress_patch_value) = 'array' THEN
      v_progress_patch_array_count := jsonb_array_length(v_progress_patch_value);

      IF v_progress_patch_array_count > 25 THEN
        v_progress_patch_json := v_progress_patch_json || jsonb_build_object(
          v_progress_patch_key,
          jsonb_build_object(
            'omitted_large_release_progress_array', true,
            'array_count', v_progress_patch_array_count,
            'value_bytes', pg_column_size(v_progress_patch_value)
          )
        );
      ELSE
        v_progress_patch_json := v_progress_patch_json || jsonb_build_object(v_progress_patch_key, v_progress_patch_value);
      END IF;
    ELSE
      v_progress_patch_json := v_progress_patch_json || jsonb_build_object(v_progress_patch_key, v_progress_patch_value);
    END IF;
  END LOOP;

  v_compact_progress_patch_bytes := pg_column_size(v_progress_patch_json);

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_OPERATION_NOT_FOUND', 'operation_id', p_operation_id::text)::text;
  END IF;

  v_operation_type := upper(BTRIM(COALESCE(v_operation_row.operation_type, '')));
  v_previous_phase := NULLIF(UPPER(BTRIM(COALESCE(v_operation_row.phase, ''))), '');
  v_existing_progress_json_bytes := pg_column_size(COALESCE(v_operation_row.progress_json, '{}'::jsonb));
  v_existing_result_json_bytes := CASE WHEN v_operation_row.result_json IS NULL THEN NULL::integer ELSE pg_column_size(v_operation_row.result_json) END;
  v_requested_phase := UPPER(BTRIM(COALESCE(
    NULLIF(BTRIM(COALESCE(v_raw_progress_patch_json->>'phase', '')), ''),
    NULLIF(BTRIM(COALESCE(v_raw_progress_patch_json->>'next_phase', '')), ''),
    NULLIF(BTRIM(COALESCE(v_raw_progress_patch_json->>'next_required_phase', '')), ''),
    NULLIF(BTRIM(COALESCE(v_raw_progress_patch_json->>'operation_phase', '')), ''),
    ''
  )));
  v_requested_phase := NULLIF(v_requested_phase, '');

  IF NULLIF(BTRIM(COALESCE(v_operation_row.lease_owner, '')), '') IS NULL
     OR v_operation_row.lease_owner <> p_lease_owner THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_LEASE_OWNER_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_LEASE_OWNER_MISMATCH', 'operation_id', p_operation_id::text, 'expected_lease_owner', v_operation_row.lease_owner, 'actual_lease_owner', p_lease_owner)::text;
  END IF;

  IF v_operation_row.lease_expires_at_utc IS NULL OR v_operation_row.lease_expires_at_utc <= v_now THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_LEASE_EXPIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_LEASE_EXPIRED', 'operation_id', p_operation_id::text, 'lease_expires_at_utc', CASE WHEN v_operation_row.lease_expires_at_utc IS NULL THEN NULL ELSE v_operation_row.lease_expires_at_utc::text END)::text;
  END IF;

  IF v_release_state IN ('MORE_WORK', 'RUNNABLE', 'RUNNING', 'CONTINUE', 'CONTINUING') THEN
    v_next_status := 'RUNNING';
    v_next_runner_state := 'RUNNABLE';
    v_next_run_after_utc := v_now + make_interval(secs => v_delay_seconds);
    v_next_requires_user_action := false;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'MORE_WORK_REMAINS');
  ELSIF v_release_state IN ('WAITING_RETRY', 'RETRYABLE_ERROR', 'RETRYABLE_FAILURE', 'RETRY', 'TRANSIENT_ERROR') THEN
    v_retry_failure := true;
    v_next_attempt_count := COALESCE(v_operation_row.attempt_count, 0) + 1;
    v_attempt_limit_reached := v_next_attempt_count >= COALESCE(v_operation_row.max_attempts, 10);

    IF v_attempt_limit_reached IS TRUE THEN
      v_next_status := 'FAILED';
      v_next_runner_state := 'FAILED';
      v_next_run_after_utc := NULL::timestamptz;
      v_next_requires_user_action := false;
      v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), CASE WHEN v_operation_type = 'DRAFT_CREATE' THEN 'DRAFT_CREATE_ATTEMPT_LIMIT_EXHAUSTED' ELSE 'OPERATION_ATTEMPT_LIMIT_EXHAUSTED' END);
      v_failed_at_utc := v_now;
    ELSE
      v_next_status := 'WAITING';
      v_next_runner_state := 'RUNNABLE';
      v_next_run_after_utc := v_now + make_interval(secs => GREATEST(v_delay_seconds, 1));
      v_next_requires_user_action := false;
      v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'WAITING_RETRY');
    END IF;
  ELSIF v_release_state IN ('WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAIT_AUTHORISATION', 'WAIT_AUTHORIZATION') THEN
    v_next_status := 'WAITING_AUTHORISATION';
    v_next_runner_state := 'WAITING_USER';
    v_next_run_after_utc := NULL::timestamptz;
    v_next_requires_user_action := true;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'AWAITING_PAYMENT_AUTHORISATION');
  ELSIF v_release_state IN ('WAITING_CHILD', 'WAIT_CHILD') THEN
    IF COALESCE(p_progress_patch_json->>'child_operation_id', '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'BANKING_PAY_OPERATION_CHILD_ID_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'BANKING_PAY_OPERATION_CHILD_ID_REQUIRED',
                'operation_id', p_operation_id::text
              )::text;
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_operations AS child_operation
      WHERE child_operation.id = (p_progress_patch_json->>'child_operation_id')::uuid
        AND child_operation.root_operation_id = p_operation_id
    ) THEN
      RAISE EXCEPTION 'BANKING_PAY_OPERATION_CHILD_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'BANKING_PAY_OPERATION_CHILD_MISMATCH',
                'operation_id', p_operation_id::text,
                'child_operation_id', p_progress_patch_json->>'child_operation_id'
              )::text;
    END IF;

    v_next_status := 'WAITING';
    v_next_runner_state := 'WAITING_CHILD';
    v_next_run_after_utc := NULL::timestamptz;
    v_next_requires_user_action := false;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'AWAITING_CHILD_OPERATION');
  ELSIF v_release_state IN ('WAITING_PROVIDER', 'WAIT_PROVIDER') THEN
    v_next_status := 'WAITING_PROVIDER';
    v_next_runner_state := 'WAITING_PROVIDER';
    v_next_run_after_utc := v_now + make_interval(secs => GREATEST(v_delay_seconds, 60));
    v_next_requires_user_action := false;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'AWAITING_PROVIDER_OUTCOME');
  ELSIF v_release_state IN ('REVIEW_REQUIRED', 'UNSAFE', 'AMBIGUOUS') THEN
    v_next_status := 'REVIEW_REQUIRED';
    v_next_runner_state := 'WAITING_USER_REVIEW';
    v_next_run_after_utc := NULL::timestamptz;
    v_next_requires_user_action := true;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'REVIEW_REQUIRED');
  ELSIF v_release_state IN ('COMPLETE', 'COMPLETED', 'DONE') THEN
    v_next_status := 'COMPLETE';
    v_next_runner_state := 'COMPLETE';
    v_next_run_after_utc := NULL::timestamptz;
    v_next_requires_user_action := false;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'OPERATION_COMPLETE');
    v_completed_at_utc := v_now;
  ELSIF v_release_state IN ('FAILED', 'ERROR') THEN
    v_next_status := 'FAILED';
    v_next_runner_state := 'FAILED';
    v_next_run_after_utc := NULL::timestamptz;
    v_next_requires_user_action := CASE WHEN v_operation_type = 'DRAFT_CREATE' THEN false ELSE true END;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), CASE WHEN v_operation_type = 'DRAFT_CREATE' THEN 'DRAFT_CREATE_OPERATION_FAILED' ELSE 'OPERATION_FAILED' END);
    v_failed_at_utc := v_now;
  ELSE
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_STATE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_STATE_INVALID', 'operation_id', p_operation_id::text, 'release_state', p_release_state)::text;
  END IF;

  IF v_requested_phase IS NOT NULL THEN
    v_phase_for_update := v_requested_phase;
  ELSIF v_next_status = 'COMPLETE' THEN
    v_phase_for_update := 'COMPLETE';
  ELSIF v_next_status = 'WAITING_AUTHORISATION' THEN
    v_phase_for_update := 'WAITING_AUTHORISATION';
  ELSE
    v_phase_for_update := NULL::text;
  END IF;

  v_clear_retryable_pre_provider_error := (
    v_release_state IN ('MORE_WORK', 'RUNNABLE', 'RUNNING', 'CONTINUE', 'CONTINUING')
    AND v_next_status IN ('RUNNING', 'WAITING')
    AND v_next_runner_state = 'RUNNABLE'
    AND v_next_requires_user_action IS FALSE
    AND (
      UPPER(BTRIM(COALESCE(v_next_resume_reason, p_resume_reason, ''))) = 'RELEASE_TIMEOUT_BEFORE_PROVIDER_CALL_RETRYABLE'
      OR (
        LOWER(BTRIM(COALESCE(v_raw_progress_patch_json->>'retryable_orchestration_issue', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(v_raw_progress_patch_json->>'release_timeout_before_provider_call', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(v_raw_progress_patch_json->>'provider_ambiguity', 'true'))) IN ('false', 'f', '0', 'no', 'n', 'off')
      )
    )
  );

  v_cleared_retryable_pre_provider_progress_error := (
    v_clear_retryable_pre_provider_error IS TRUE
    AND (
      COALESCE(v_operation_row.progress_json, '{}'::jsonb) ? 'error'
      OR v_progress_patch_json ? 'error'
    )
  );

  -- The canonical PAYMENT_EXECUTE runner terminalises through this lease
  -- release function rather than banking_pay_operation_finish.  Seal and
  -- retain the exact provider-unsubmitted execution-owned dirty chain before
  -- exposing the operation as COMPLETE.  A rejected receipt is diagnostic
  -- evidence only; it never blocks a valid execution and cancellation will
  -- continue through its safe fallback route.
  IF v_operation_type = 'PAYMENT_EXECUTE'
     AND v_next_status = 'COMPLETE'
     AND v_operation_row.pay_batch_id IS NOT NULL THEN
    v_execution_overlay_chain_v2 :=
      private.pay_workbench_execution_unsent_overlay_chain_seal_v2(
        v_operation_row.id,
        v_operation_row.pay_batch_id,
        '{}'::jsonb
      );
  END IF;

  v_release_diag_json := jsonb_build_object(
    'function_name', 'banking_pay_operation_release_lease',
    'operation_id', p_operation_id::text,
    'release_state', v_release_state,
    'previous_phase', v_previous_phase,
    'requested_phase', v_requested_phase,
    'next_phase', COALESCE(v_phase_for_update, v_operation_row.phase),
    'next_status', v_next_status,
    'next_runner_state', v_next_runner_state,
    'run_after_delay_seconds', v_delay_seconds,
    'progress_patch_bytes', v_progress_patch_bytes,
    'compact_progress_patch_bytes', v_compact_progress_patch_bytes,
    'progress_patch_key_count', v_progress_patch_key_count,
    'result_patch_bytes', v_result_patch_bytes,
    'result_patch_key_count', v_result_patch_key_count,
    'error_json_bytes', v_error_json_bytes,
    'existing_progress_json_bytes', v_existing_progress_json_bytes,
    'existing_result_json_bytes', v_existing_result_json_bytes,
    'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
    'cleared_retryable_pre_provider_error', v_clear_retryable_pre_provider_error,
    'cleared_retryable_pre_provider_progress_error', v_cleared_retryable_pre_provider_progress_error,
    'existing_progress_compaction_applied', false,
    'existing_progress_compaction_scope', 'diagnostic_only'
  );

  UPDATE public.banking_pay_operations AS operation_update
  SET status = v_next_status,
      phase = COALESCE(v_phase_for_update, operation_update.phase),
      runner_state = v_next_runner_state,
      run_after_utc = v_next_run_after_utc,
      requires_user_action = v_next_requires_user_action,
      resume_reason = v_next_resume_reason,
      attempt_count = CASE WHEN v_retry_failure IS TRUE THEN COALESCE(v_next_attempt_count, operation_update.attempt_count) ELSE operation_update.attempt_count END,
      lease_owner = NULL::text,
      lease_expires_at_utc = NULL::timestamptz,
      locked_by = NULL::text,
      lock_expires_at_utc = NULL::timestamptz,
      heartbeat_at_utc = v_now,
      last_advanced_at_utc = v_now,
      progress_json = jsonb_strip_nulls(
        CASE
          WHEN v_clear_retryable_pre_provider_error IS TRUE THEN COALESCE(operation_update.progress_json, '{}'::jsonb) - 'error'
          ELSE COALESCE(operation_update.progress_json, '{}'::jsonb)
        END
        || CASE
          WHEN v_clear_retryable_pre_provider_error IS TRUE THEN v_progress_patch_json - 'error'
          ELSE v_progress_patch_json
        END
        || jsonb_build_object(
          'execution_unsent_overlay_chain_v2', v_execution_overlay_chain_v2,
          'last_release', jsonb_build_object(
            'released_at_utc', v_now::text,
            'release_state', v_release_state,
            'next_status', v_next_status,
            'runner_state', v_next_runner_state,
            'run_after_utc', CASE WHEN v_next_run_after_utc IS NULL THEN NULL ELSE v_next_run_after_utc::text END,
            'requires_user_action', v_next_requires_user_action,
            'resume_reason', v_next_resume_reason,
            'previous_phase', v_previous_phase,
            'requested_phase', v_requested_phase,
            'next_phase', COALESCE(v_phase_for_update, operation_update.phase),
            'phase_persisted', v_phase_for_update IS NOT NULL,
            'retry_failure', v_retry_failure,
            'attempt_count', CASE WHEN v_retry_failure IS TRUE THEN v_next_attempt_count ELSE operation_update.attempt_count END,
            'max_attempts', operation_update.max_attempts,
            'attempt_limit_reached', v_attempt_limit_reached,
            'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
            'release_diag', v_release_diag_json
          )
        )
      ),
      result_json = CASE
        WHEN p_result_patch_json IS NULL AND v_execution_overlay_chain_v2 IS NULL
          THEN operation_update.result_json
        ELSE jsonb_strip_nulls(
          COALESCE(operation_update.result_json, '{}'::jsonb)
          || COALESCE(p_result_patch_json, '{}'::jsonb)
          || jsonb_build_object(
            'execution_unsent_overlay_chain_v2', v_execution_overlay_chain_v2
          )
        )
      END,
      error_json = CASE
        WHEN v_clear_retryable_pre_provider_error IS TRUE THEN NULL::jsonb
        WHEN p_error_json IS NULL AND v_attempt_limit_reached IS NOT TRUE THEN operation_update.error_json
        ELSE jsonb_strip_nulls(
          COALESCE(p_error_json, '{}'::jsonb)
          || CASE
            WHEN v_attempt_limit_reached IS TRUE THEN jsonb_build_object(
              'code', v_next_resume_reason,
              'message', 'Operation retry attempt limit was exhausted.',
              'operation_id', p_operation_id::text,
              'attempt_count', v_next_attempt_count,
              'max_attempts', operation_update.max_attempts
            )
            ELSE '{}'::jsonb
          END
        )
      END,
      completed_at_utc = CASE WHEN v_completed_at_utc IS NULL THEN operation_update.completed_at_utc ELSE COALESCE(operation_update.completed_at_utc, v_completed_at_utc) END,
      failed_at_utc = CASE
        WHEN v_clear_retryable_pre_provider_error IS TRUE THEN NULL::timestamptz
        WHEN v_failed_at_utc IS NULL THEN operation_update.failed_at_utc
        ELSE COALESCE(operation_update.failed_at_utc, v_failed_at_utc)
      END,
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'released_lease_owner', p_lease_owner,
    'status', v_next_status,
    'runner_state', v_next_runner_state,
    'phase', COALESCE(v_phase_for_update, v_operation_row.phase),
    'previous_phase', v_previous_phase,
    'phase_persisted', v_phase_for_update IS NOT NULL,
    'run_after_utc', CASE WHEN v_next_run_after_utc IS NULL THEN NULL ELSE v_next_run_after_utc::text END,
    'requires_user_action', v_next_requires_user_action,
    'resume_reason', v_next_resume_reason,
    'released_at_utc', v_now::text,
    'retry_failure', v_retry_failure,
    'attempt_count', CASE WHEN v_retry_failure IS TRUE THEN v_next_attempt_count ELSE v_operation_row.attempt_count END,
    'max_attempts', v_operation_row.max_attempts,
    'attempt_limit_reached', v_attempt_limit_reached,
    'cleared_retryable_pre_provider_error', v_clear_retryable_pre_provider_error,
    'cleared_retryable_pre_provider_progress_error', v_cleared_retryable_pre_provider_progress_error,
    'release_diag', v_release_diag_json
  );
END;
$function$;

ALTER FUNCTION public.banking_pay_operation_release_lease(uuid,text,text,integer,jsonb,jsonb,jsonb,text,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_operation_release_lease(uuid,text,text,integer,jsonb,jsonb,jsonb,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.banking_pay_operation_release_lease(uuid,text,text,integer,jsonb,jsonb,jsonb,text,uuid) FROM anon;
REVOKE ALL ON FUNCTION public.banking_pay_operation_release_lease(uuid,text,text,integer,jsonb,jsonb,jsonb,text,uuid) FROM authenticated;
REVOKE ALL ON FUNCTION public.banking_pay_operation_release_lease(uuid,text,text,integer,jsonb,jsonb,jsonb,text,uuid) FROM service_role;
GRANT EXECUTE ON FUNCTION public.banking_pay_operation_release_lease(uuid,text,text,integer,jsonb,jsonb,jsonb,text,uuid) TO service_role;
