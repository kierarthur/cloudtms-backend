-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Preserves the installed function identity; no overload is added.

CREATE OR REPLACE FUNCTION public.banking_pay_operation_start(p_operation_type text, p_actor_user_id uuid, p_idempotency_key text, p_workbench_session_id uuid DEFAULT NULL::uuid, p_pay_batch_id uuid DEFAULT NULL::uuid, p_root_operation_id uuid DEFAULT NULL::uuid, p_input_json jsonb DEFAULT '{}'::jsonb, p_config_json jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(operation_id uuid, operation_type text, status text, phase text, actor_user_id uuid, workbench_session_id uuid, pay_batch_id uuid, root_operation_id uuid, idempotency_key text, input_json jsonb, config_json jsonb, progress_json jsonb, result_json jsonb, error_json jsonb, total_units integer, completed_units integer, failed_units integer, current_chunk_index integer, chunk_count integer, locked_by text, lock_expires_at_utc timestamp with time zone, created_at_utc timestamp with time zone, started_at_utc timestamp with time zone, updated_at_utc timestamp with time zone, completed_at_utc timestamp with time zone, failed_at_utc timestamp with time zone, is_existing boolean)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO pg_catalog, private, extensions, pg_temp
 SET statement_timeout TO '6000ms'
 SET lock_timeout TO '1000ms'
AS $function$
DECLARE
    v_now timestamptz := now();
    v_operation_type text := upper(NULLIF(BTRIM(COALESCE(p_operation_type, '')), ''));
    v_idempotency_key text := NULLIF(BTRIM(COALESCE(p_idempotency_key, '')), '');
    v_input_json jsonb := COALESCE(p_input_json, '{}'::jsonb);
    v_config_json jsonb := COALESCE(p_config_json, '{}'::jsonb);
    v_compact_input_json jsonb := '{}'::jsonb;
    v_compact_config_json jsonb := '{}'::jsonb;
    v_operation public.banking_pay_operations%ROWTYPE;
    v_existing_by_batch public.banking_pay_operations%ROWTYPE;
    v_allow_restart boolean := false;
    v_initial_status text := 'RUNNING';
    v_initial_phase text := 'INITIALISE';
    v_runner_state text := 'RUNNABLE';
    v_max_attempts integer := 10;
    v_lock_seconds integer := 60;
    v_chunk_config jsonb := '{}'::jsonb;
    v_run_after_utc timestamptz := NULL::timestamptz;
    v_server_runnable boolean := false;
    v_backend_runner_owned boolean := false;
    v_frontend_completion_required boolean := false;
    v_draft_stale_minutes integer := 120;
    v_existing_status text := NULL::text;
    v_existing_draft_is_terminal boolean := false;
    v_existing_draft_is_exhausted boolean := false;
    v_existing_draft_is_stale boolean := false;
    v_terminalise_reason text := NULL::text;
    v_settlement_batch_clean_success boolean := false;
    v_settlement_durable_truth jsonb := '{}'::jsonb;
    v_settlement_batch_status text := NULL::text;
    v_settlement_execution_commit_state text := NULL::text;
    v_settlement_execution_commit_ref text := NULL::text;
    v_settlement_execution_commit_ref_present boolean := false;
    v_settlement_completed_at_utc_present boolean := false;
    v_settlement_freshness_validation_status text := NULL::text;
    v_settlement_freshness_clean boolean := true;
    v_settlement_transfer_count integer := 0;
    v_settlement_terminal_success_transfer_count integer := 0;
    v_settlement_terminal_failed_transfer_count integer := 0;
    v_settlement_pending_or_unknown_transfer_count integer := 0;
    v_settlement_transfer_event_count integer := 0;
    v_settlement_provider_attempt_count integer := 0;
    v_settlement_provider_artifact_count integer := 0;
    v_settlement_candidate_count integer := 0;
    v_settlement_settled_candidate_count integer := 0;
    v_settlement_item_count integer := 0;
    v_settlement_linked_nonvoid_item_count integer := 0;
    v_settlement_covered_nonvoid_item_count integer := 0;
    v_settlement_no_bank_operation_count integer := 0;
    v_settlement_no_bank_scope_count integer := 0;
    v_settlement_settled_no_bank_scope_count integer := 0;
    v_settlement_unresolved_no_bank_scope_count integer := 0;
    v_settlement_authorised_no_bank_scope_count integer := 0;
    v_settlement_unauthorised_no_bank_scope_count integer := 0;
    v_settlement_confirmation_no_bank boolean := false;
    v_settlement_confirmation_mode text := NULL::text;
    v_settlement_confirmation_settlement_mode text := NULL::text;
    v_settlement_confirmation_local_commit_ref text := NULL::text;
    v_settlement_confirmation_auth_authorised boolean := false;
    v_settlement_positive_success boolean := false;
    v_settlement_no_bank_success boolean := false;
    v_settlement_mixed_success boolean := false;
    v_correction_request_id uuid := NULL::uuid;
    v_correction_config public.banking_pay_operation_config%ROWTYPE;
BEGIN
    PERFORM set_config('lock_timeout', '3s', true);

    IF v_operation_type IS NULL THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_OPERATION_TYPE_REQUIRED'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_START_OPERATION_TYPE_REQUIRED')::text;
    END IF;

    IF v_operation_type NOT IN (
        'DRAFT_CREATE',
        'PAYMENT_EXECUTE',
        'PAYMENT_RETRY_BLOCKED_FUNDS',
        'PAYMENT_SETTLEMENT',
        'REMITTANCE_QUEUE',
        'PREVIEW_REFRESH',
        'PAYMENT_CORRECTION'
    ) THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_OPERATION_TYPE_UNSUPPORTED'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_START_OPERATION_TYPE_UNSUPPORTED', 'operation_type', v_operation_type)::text;
    END IF;

    IF v_idempotency_key IS NULL THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_IDEMPOTENCY_KEY_REQUIRED'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_START_IDEMPOTENCY_KEY_REQUIRED')::text;
    END IF;

    IF jsonb_typeof(v_input_json) <> 'object' THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_INPUT_MUST_BE_OBJECT'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_START_INPUT_MUST_BE_OBJECT')::text;
    END IF;

    IF jsonb_typeof(v_config_json) <> 'object' THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_CONFIG_MUST_BE_OBJECT'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_START_CONFIG_MUST_BE_OBJECT')::text;
    END IF;

    IF v_operation_type = 'PAYMENT_CORRECTION' THEN
        IF p_pay_batch_id IS NULL THEN
            RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_CORRECTION_INPUT_INVALID'
              USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
                'code', 'OPERATION_INPUT_INVALID',
                'pay_batch_id_present', p_pay_batch_id IS NOT NULL,
                'actor_user_id_present', p_actor_user_id IS NOT NULL
              )::text;
        END IF;

        BEGIN
            v_correction_request_id := NULLIF(
                BTRIM(COALESCE(v_input_json->>'correction_request_id', '')),
                ''
            )::uuid;
        EXCEPTION WHEN invalid_text_representation THEN
            v_correction_request_id := NULL::uuid;
        END;

        IF v_correction_request_id IS NULL
           OR NOT EXISTS (
               SELECT 1
               FROM public.pay_payment_correction_requests AS correction_request
               WHERE correction_request.id = v_correction_request_id
                 AND correction_request.pay_batch_id = p_pay_batch_id
                  AND (
                    correction_request.requested_by_user_id = p_actor_user_id
                    OR (
                      p_actor_user_id IS NULL
                      AND correction_request.auto_requested IS TRUE
                      AND correction_request.source_bank_event_id IS NOT NULL
                      AND lower(BTRIM(COALESCE(v_input_json->>'auto_requested', 'false'))) IN ('true', 't', '1')
                      AND correction_request.source_bank_event_id::text = NULLIF(
                        BTRIM(COALESCE(v_input_json->>'source_bank_event_id', '')),
                        ''
                      )
                    )
                  )
                 AND correction_request.status IN ('PLANNING', 'PLANNED')
           ) THEN
            RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_CORRECTION_INPUT_INVALID'
              USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
                'code', 'OPERATION_INPUT_INVALID',
                'correction_request_id', v_input_json->>'correction_request_id'
              )::text;
        END IF;

        SELECT config_row.*
        INTO v_correction_config
        FROM public.banking_pay_operation_config AS config_row
        WHERE config_row.operation_type = 'PAYMENT_CORRECTION'
          AND config_row.phase = 'PREPARE_SELECTION'
          AND config_row.chunk_type = 'CANDIDATE_SCOPE'
        LIMIT 1;

        IF NOT FOUND OR COALESCE(v_correction_config.enabled, false) IS NOT TRUE THEN
            RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_CONFIG_DISABLED'
              USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
                'code', 'OPERATION_CONFIG_DISABLED',
                'operation_type', v_operation_type,
                'phase', 'PREPARE_SELECTION'
              )::text;
        END IF;
    END IF;

    v_allow_restart := lower(BTRIM(COALESCE(v_input_json->>'explicit_restart', v_input_json->>'allow_restart', v_config_json->>'explicit_restart', v_config_json->>'allow_restart', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_max_attempts := LEAST(GREATEST(COALESCE(NULLIF(BTRIM(COALESCE(v_config_json->>'max_attempts', '')), '')::integer, 10), 1), 100);
    v_lock_seconds := LEAST(GREATEST(COALESCE(NULLIF(BTRIM(COALESCE(v_config_json->>'lock_seconds', '')), '')::integer, 60), 10), 3600);
    v_server_runnable := v_operation_type IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS', 'PAYMENT_SETTLEMENT', 'REMITTANCE_QUEUE', 'PAYMENT_CORRECTION');
    v_backend_runner_owned := v_server_runnable OR lower(BTRIM(COALESCE(v_input_json->>'backend_runner_owned', v_input_json->>'backendRunnerOwned', v_config_json->>'backend_runner_owned', v_config_json->>'backendRunnerOwned', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
    v_frontend_completion_required := CASE
        WHEN v_operation_type IN ('DRAFT_CREATE', 'PAYMENT_SETTLEMENT', 'REMITTANCE_QUEUE') THEN false
        ELSE lower(BTRIM(COALESCE(v_input_json->>'frontend_completion_required', v_input_json->>'frontendCompletionRequired', v_config_json->>'frontend_completion_required', v_config_json->>'frontendCompletionRequired', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    END;
    v_draft_stale_minutes := LEAST(GREATEST(COALESCE(NULLIF(BTRIM(COALESCE(v_config_json->>'draft_create_stale_minutes', v_config_json->>'draftCreateStaleMinutes', '')), '')::integer, 120), 5), 1440);

    IF NULLIF(BTRIM(COALESCE(v_config_json->>'run_after_utc', v_config_json->>'runAfterUtc', '')), '') IS NOT NULL THEN
        BEGIN
            v_run_after_utc := COALESCE(v_config_json->>'run_after_utc', v_config_json->>'runAfterUtc')::timestamptz;
        EXCEPTION WHEN OTHERS THEN
            v_run_after_utc := NULL::timestamptz;
        END;
    END IF;

    IF v_server_runnable IS TRUE AND (v_run_after_utc IS NULL OR v_run_after_utc <= v_now) THEN
        v_run_after_utc := v_now;
    END IF;

    IF v_server_runnable IS TRUE THEN
        v_runner_state := 'RUNNABLE';
    END IF;

    IF v_operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
        v_initial_status := 'RUNNING';
        v_initial_phase := COALESCE(NULLIF(BTRIM(COALESCE(v_input_json->>'initial_phase', v_config_json->>'initial_phase', '')), ''), 'INITIALISE');
        v_runner_state := 'RUNNABLE';
        v_chunk_config := jsonb_strip_nulls(jsonb_build_object(
            'freshness_limit', LEAST(GREATEST(COALESCE(NULLIF(BTRIM(COALESCE(v_config_json->>'freshness_limit', '')), '')::integer, 100), 1), 100),
            'transfer_scope_limit', LEAST(GREATEST(COALESCE(NULLIF(BTRIM(COALESCE(v_config_json->>'transfer_scope_limit', '')), '')::integer, 100), 1), 100),
            'transfer_prepare_chunk_size', LEAST(GREATEST(COALESCE(NULLIF(BTRIM(COALESCE(v_config_json->>'transfer_prepare_chunk_size', '')), '')::integer, 25), 1), 25),
            'provider_submit_limit', LEAST(GREATEST(COALESCE(NULLIF(BTRIM(COALESCE(v_config_json->>'provider_submit_limit', '')), '')::integer, 25), 1), 25),
            'rail_update_limit', LEAST(GREATEST(COALESCE(NULLIF(BTRIM(COALESCE(v_config_json->>'rail_update_limit', '')), '')::integer, 25), 1), 100),
            'lock_seconds', v_lock_seconds
        ));
    END IF;

    IF v_operation_type = 'PAYMENT_CORRECTION' THEN
        v_initial_status := 'RUNNING';
        v_initial_phase := 'PREPARE_SELECTION';
        v_runner_state := 'RUNNABLE';
        v_backend_runner_owned := true;
        v_frontend_completion_required := false;
        v_max_attempts := 10;
        v_lock_seconds := COALESCE(v_correction_config.lock_seconds, 60);
        v_chunk_config := jsonb_build_object(
            'phase', 'PREPARE_SELECTION',
            'chunk_type', 'CANDIDATE_SCOPE',
            'default_chunk_size', v_correction_config.default_chunk_size,
            'min_chunk_size', v_correction_config.min_chunk_size,
            'max_chunk_size', v_correction_config.max_chunk_size,
            'max_advance_ms', v_correction_config.max_advance_ms,
            'lock_seconds', v_correction_config.lock_seconds
        );
    END IF;

    v_compact_input_json := v_input_json
      - 'scope_candidate_ids'
      - 'candidate_ids'
      - 'pending_candidate_ids'
      - 'pay_batch_item_ids'
      - 'transfer_ids'
      - 'pay_batch_item_ids_json'
      - 'transfer_group_json'
      - 'full_preview_json'
      - 'canonical_preview_lines_json';

    v_compact_config_json := jsonb_strip_nulls((v_config_json
      - 'scope_candidate_ids'
      - 'candidate_ids'
      - 'pending_candidate_ids'
      - 'pay_batch_item_ids'
      - 'transfer_ids'
      - 'pay_batch_item_ids_json'
      - 'transfer_group_json'
      - 'full_preview_json'
      - 'canonical_preview_lines_json') || jsonb_build_object(
        'chunks', v_chunk_config,
        'server_runnable', CASE WHEN v_server_runnable THEN v_server_runnable ELSE NULL::boolean END,
        'backend_runner_owned', CASE WHEN v_server_runnable THEN v_backend_runner_owned ELSE NULL::boolean END,
        'frontend_completion_required', CASE WHEN v_server_runnable THEN v_frontend_completion_required ELSE NULL::boolean END,
        'operation_created_for_backend_runner', CASE WHEN v_server_runnable THEN v_backend_runner_owned ELSE NULL::boolean END,
        'run_after_utc', CASE WHEN v_server_runnable AND v_run_after_utc IS NOT NULL THEN v_run_after_utc::text ELSE NULL::text END
      ));

    PERFORM pg_advisory_xact_lock(pg_catalog.hashtextextended('banking_pay_operation_start:' || v_operation_type || ':' || v_idempotency_key, 0));

    IF v_operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') AND p_pay_batch_id IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock(pg_catalog.hashtextextended('banking_pay_operation_start:PAYMENT_EXECUTION_BATCH:' || p_pay_batch_id::text, 0));
    END IF;

    IF v_operation_type = 'PAYMENT_SETTLEMENT' AND p_pay_batch_id IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock(pg_catalog.hashtextextended('banking_pay_operation_start:PAYMENT_SETTLEMENT_BATCH:' || p_pay_batch_id::text, 0));
    END IF;

    IF v_operation_type = 'DRAFT_CREATE' AND p_workbench_session_id IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock(pg_catalog.hashtextextended('banking_pay_operation_start:DRAFT_CREATE_WORKBENCH_SESSION:' || p_workbench_session_id::text, 0));
    END IF;

    IF v_operation_type = 'PAYMENT_CORRECTION' THEN
        PERFORM pg_advisory_xact_lock(
            pg_catalog.hashtextextended(
                'banking_pay_operation_start:PAYMENT_CORRECTION_REQUEST:'
                || v_correction_request_id::text,
                0
            )
        );
    END IF;

    SELECT existing_operation.*
    INTO v_operation
    FROM public.banking_pay_operations AS existing_operation
    WHERE existing_operation.idempotency_key = v_idempotency_key
    ORDER BY existing_operation.created_at_utc DESC, existing_operation.id DESC
    LIMIT 1
    FOR UPDATE;

    IF FOUND THEN
        IF upper(BTRIM(COALESCE(v_operation.operation_type, ''))) <> v_operation_type THEN
            RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_IDEMPOTENCY_TYPE_MISMATCH'
              USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_START_IDEMPOTENCY_TYPE_MISMATCH', 'operation_id', v_operation.id::text)::text;
        END IF;

        IF v_operation_type <> 'DRAFT_CREATE'
           AND p_actor_user_id IS NOT NULL
           AND v_operation.actor_user_id IS NOT NULL
           AND v_operation.actor_user_id <> p_actor_user_id THEN
            RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_IDEMPOTENCY_ACTOR_MISMATCH'
              USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_START_IDEMPOTENCY_ACTOR_MISMATCH', 'operation_id', v_operation.id::text)::text;
        END IF;

        IF v_operation_type = 'PAYMENT_CORRECTION'
           AND v_operation.input_json->>'correction_request_id'
               IS DISTINCT FROM v_correction_request_id::text THEN
            RAISE EXCEPTION 'BANKING_PAY_OPERATION_START_IDEMPOTENCY_INPUT_MISMATCH'
              USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
                'code', 'OPERATION_IDEMPOTENCY_CONFLICT',
                'operation_id', v_operation.id,
                'correction_request_id', v_correction_request_id
              )::text;
        END IF;

        IF v_operation_type = 'DRAFT_CREATE' THEN
            v_existing_status := upper(BTRIM(COALESCE(v_operation.status, '')));
            v_existing_draft_is_terminal := v_existing_status IN ('COMPLETE', 'COMPLETED', 'FAILED', 'CANCELLED', 'CANCELED', 'ERROR', 'REVIEW_REQUIRED');
            v_existing_draft_is_exhausted := v_existing_status IN ('QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAITING_PROVIDER', 'CONTINUING', 'WAITING_RETRY')
              AND COALESCE(v_operation.attempt_count, 0) >= COALESCE(v_operation.max_attempts, 10);
            v_existing_draft_is_stale := v_existing_status IN ('QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAITING_PROVIDER', 'CONTINUING', 'WAITING_RETRY')
              AND (v_operation.lease_owner IS NULL OR v_operation.lease_expires_at_utc IS NULL OR v_operation.lease_expires_at_utc <= v_now)
              AND COALESCE(v_operation.last_advanced_at_utc, v_operation.updated_at_utc, v_operation.created_at_utc, v_now) <= v_now - make_interval(mins => v_draft_stale_minutes);

            IF v_existing_draft_is_terminal IS TRUE THEN
                NULL;
            ELSIF v_allow_restart IS TRUE OR v_existing_draft_is_exhausted IS TRUE OR v_existing_draft_is_stale IS TRUE THEN
                v_terminalise_reason := CASE
                    WHEN v_allow_restart IS TRUE THEN 'DRAFT_CREATE_EXPLICIT_RESTART_TERMINATED'
                    WHEN v_existing_draft_is_exhausted IS TRUE THEN 'DRAFT_CREATE_ATTEMPT_LIMIT_EXHAUSTED'
                    ELSE 'STALE_DRAFT_CREATE_TERMINATED'
                END;

                UPDATE public.banking_pay_operations AS stale_operation_update
                SET status = 'FAILED',
                    runner_state = 'FAILED',
                    requires_user_action = false,
                    resume_reason = v_terminalise_reason,
                    lease_owner = NULL::text,
                    lease_expires_at_utc = NULL::timestamptz,
                    locked_by = NULL::text,
                    lock_expires_at_utc = NULL::timestamptz,
                    run_after_utc = NULL::timestamptz,
                    failed_at_utc = COALESCE(stale_operation_update.failed_at_utc, v_now),
                    error_json = jsonb_strip_nulls(COALESCE(stale_operation_update.error_json, '{}'::jsonb) || jsonb_build_object(
                        'code', v_terminalise_reason,
                        'message', 'DRAFT_CREATE operation was terminalised before starting a replacement operation.',
                        'operation_id', stale_operation_update.id::text,
                        'attempt_count', stale_operation_update.attempt_count,
                        'max_attempts', stale_operation_update.max_attempts,
                        'terminalised_at_utc', v_now::text
                    )),
                    progress_json = jsonb_strip_nulls(COALESCE(stale_operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
                        'active_operation_guard', 'DRAFT_CREATE_WORKBENCH_SESSION',
                        'terminalised_by_operation_start', true,
                        'terminalise_reason', v_terminalise_reason,
                        'terminalised_at_utc', v_now::text
                    )),
                    updated_at_utc = v_now
                WHERE stale_operation_update.id = v_operation.id;
            ELSE
        RETURN QUERY
        SELECT
            v_operation.id,
            v_operation.operation_type,
            v_operation.status,
            v_operation.phase,
            v_operation.actor_user_id,
            v_operation.workbench_session_id,
            v_operation.pay_batch_id,
            v_operation.root_operation_id,
            v_operation.idempotency_key,
            v_operation.input_json,
            v_operation.config_json,
            v_operation.progress_json,
            v_operation.result_json,
            v_operation.error_json,
            v_operation.total_units,
            v_operation.completed_units,
            v_operation.failed_units,
            v_operation.current_chunk_index,
            v_operation.chunk_count,
            COALESCE(v_operation.lease_owner, v_operation.locked_by),
            COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc),
            v_operation.created_at_utc,
            v_operation.started_at_utc,
            v_operation.updated_at_utc,
            v_operation.completed_at_utc,
            v_operation.failed_at_utc,
            true;
        RETURN;

            END IF;
        ELSIF v_operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
            v_existing_status := upper(BTRIM(COALESCE(v_operation.status, '')));

            IF v_existing_status IN ('QUEUED', 'RUNNING', 'WAITING', 'RUNNABLE', 'CONTINUING', 'WAITING_RETRY', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'AWAITING_AUTHORISATION', 'AWAITING_AUTHORIZATION', 'WAITING_PROVIDER', 'WAITING_FOR_PROVIDER', 'AWAITING_PROVIDER', 'WAITING_USER', 'WAITING_USER_REVIEW', 'REVIEW_REQUIRED') THEN
                RETURN QUERY
                SELECT
                    v_operation.id,
                    v_operation.operation_type,
                    v_operation.status,
                    v_operation.phase,
                    v_operation.actor_user_id,
                    v_operation.workbench_session_id,
                    v_operation.pay_batch_id,
                    v_operation.root_operation_id,
                    v_operation.idempotency_key,
                    v_operation.input_json,
                    v_operation.config_json,
                    v_operation.progress_json,
                    v_operation.result_json,
                    v_operation.error_json,
                    v_operation.total_units,
                    v_operation.completed_units,
                    v_operation.failed_units,
                    v_operation.current_chunk_index,
                    v_operation.chunk_count,
                    COALESCE(v_operation.lease_owner, v_operation.locked_by),
                    COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc),
                    v_operation.created_at_utc,
                    v_operation.started_at_utc,
                    v_operation.updated_at_utc,
                    v_operation.completed_at_utc,
                    v_operation.failed_at_utc,
                    true;
                RETURN;
            END IF;
        ELSE
            v_existing_status := upper(BTRIM(COALESCE(v_operation.status, '')));

            IF v_operation_type = 'PAYMENT_SETTLEMENT'
               AND p_pay_batch_id IS NOT NULL
               AND v_existing_status IN ('FAILED', 'CANCELLED', 'CANCELED', 'ERROR', 'REVIEW_REQUIRED') THEN
                NULL;
            ELSE
                IF v_operation_type IN ('PAYMENT_SETTLEMENT', 'REMITTANCE_QUEUE')
                   AND v_existing_status NOT IN ('COMPLETE', 'COMPLETED', 'FAILED', 'CANCELLED', 'CANCELED', 'ERROR', 'REVIEW_REQUIRED') THEN
                    UPDATE public.banking_pay_operations AS child_operation_update
                    SET config_json = jsonb_strip_nulls(COALESCE(child_operation_update.config_json, '{}'::jsonb) || jsonb_build_object(
                            'server_runnable', true,
                            'backend_runner_owned', true,
                            'frontend_completion_required', false,
                            'operation_created_for_backend_runner', true,
                            'run_after_utc', COALESCE(child_operation_update.run_after_utc, v_now)::text
                        )),
                        progress_json = jsonb_strip_nulls(COALESCE(child_operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
                            'server_runnable', true,
                            'backend_runner_owned', true,
                            'frontend_completion_required', false,
                            'operation_created_for_backend_runner', true,
                            'operation_reused_for_backend_runner', true,
                            'runner_flags_repaired_at_utc', v_now::text,
                            'run_after_utc', COALESCE(child_operation_update.run_after_utc, v_now)::text
                        )),
                        runner_state = 'RUNNABLE',
                        requires_user_action = false,
                        run_after_utc = COALESCE(child_operation_update.run_after_utc, v_now),
                        updated_at_utc = v_now
                    WHERE child_operation_update.id = v_operation.id
                    RETURNING child_operation_update.* INTO v_operation;
                END IF;

                RETURN QUERY
                SELECT
                    v_operation.id,
                    v_operation.operation_type,
                    v_operation.status,
                    v_operation.phase,
                    v_operation.actor_user_id,
                    v_operation.workbench_session_id,
                    v_operation.pay_batch_id,
                    v_operation.root_operation_id,
                    v_operation.idempotency_key,
                    v_operation.input_json,
                    v_operation.config_json,
                    v_operation.progress_json,
                    v_operation.result_json,
                    v_operation.error_json,
                    v_operation.total_units,
                    v_operation.completed_units,
                    v_operation.failed_units,
                    v_operation.current_chunk_index,
                    v_operation.chunk_count,
                    COALESCE(v_operation.lease_owner, v_operation.locked_by),
                    COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc),
                    v_operation.created_at_utc,
                    v_operation.started_at_utc,
                    v_operation.updated_at_utc,
                    v_operation.completed_at_utc,
                    v_operation.failed_at_utc,
                    true;
                RETURN;
            END IF;
        END IF;
    END IF;

    IF v_operation_type = 'PAYMENT_SETTLEMENT' AND p_pay_batch_id IS NOT NULL THEN
        WITH batch_summary AS (
            SELECT
                batch_row.status AS batch_status,
                batch_row.execution_commit_state AS execution_commit_state,
                NULLIF(BTRIM(COALESCE(batch_row.execution_commit_ref, '')), '') AS execution_commit_ref,
                batch_row.completed_at_utc IS NOT NULL AS completed_at_utc_present,
                batch_row.freshness_validation_status AS freshness_validation_status,
                upper(BTRIM(COALESCE(batch_row.freshness_validation_status, ''))) NOT IN ('STALE', 'FAILED', 'BLOCKED', 'CONFLICT') AS freshness_clean,
                COALESCE(batch_row.execution_intent_json, '{}'::jsonb) AS execution_intent_json,
                COALESCE(batch_row.settlement_confirmation_json, '{}'::jsonb) AS settlement_confirmation_json
            FROM public.pay_batches AS batch_row
            WHERE batch_row.id = p_pay_batch_id
        ), confirmation_summary AS (
            SELECT
                lower(BTRIM(COALESCE(batch_summary.settlement_confirmation_json->>'no_bank_payment_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS no_bank_payment_execution,
                upper(BTRIM(COALESCE(batch_summary.settlement_confirmation_json->>'confirmation_mode', ''))) AS confirmation_mode,
                upper(BTRIM(COALESCE(batch_summary.settlement_confirmation_json->>'settlement_mode', ''))) AS settlement_mode,
                NULLIF(BTRIM(COALESCE(batch_summary.settlement_confirmation_json->>'local_commit_reference', '')), '') AS local_commit_reference,
                NULLIF(BTRIM(COALESCE(batch_summary.settlement_confirmation_json->>'paye_net_state_hash', '')), '') AS confirmation_paye_net_state_hash,
                NULLIF(BTRIM(COALESCE(batch_summary.settlement_confirmation_json->>'bank_payment_projection_hash', '')), '') AS confirmation_bank_payment_projection_hash,
                UPPER(NULLIF(BTRIM(COALESCE(batch_summary.settlement_confirmation_json->>'projection_scope', '')), '')) AS confirmation_projection_scope,
                NULLIF(BTRIM(COALESCE(batch_summary.settlement_confirmation_json->>'auth_request_id', '')), '') AS confirmation_auth_request_id_text,
                NULLIF(BTRIM(COALESCE(batch_summary.settlement_confirmation_json->>'execution_operation_id', '')), '') AS confirmation_execution_operation_id_text,
                COALESCE(
                    NULLIF(BTRIM(COALESCE(batch_summary.settlement_confirmation_json->>'settlement_operation_id', '')), ''),
                    NULLIF(BTRIM(COALESCE(batch_summary.execution_intent_json->>'settlement_operation_id', '')), '')
                ) AS confirmation_settlement_operation_id_text
            FROM batch_summary
        ), confirmation_auth_summary AS (
            SELECT EXISTS (
                SELECT 1
                FROM public.pay_batch_auth_requests AS auth_request
                CROSS JOIN batch_summary
                CROSS JOIN confirmation_summary
                WHERE auth_request.pay_batch_id = p_pay_batch_id
                  AND upper(BTRIM(COALESCE(auth_request.state, ''))) = 'AUTHORISED'
                  AND auth_request.id::text = confirmation_summary.confirmation_auth_request_id_text
                  AND NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'operation_id', '')), '') = confirmation_summary.confirmation_execution_operation_id_text
                  AND lower(BTRIM(COALESCE(auth_request.execution_intent_json->>'no_bank_payment_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
                  AND NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'paye_net_state_hash', '')), '') = confirmation_summary.confirmation_paye_net_state_hash
                  AND NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'bank_payment_projection_hash', '')), '') = confirmation_summary.confirmation_bank_payment_projection_hash
                  AND UPPER(NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'pay_channel_scope', '')), '')) = confirmation_summary.confirmation_projection_scope
            ) AS confirmation_auth_authorised
        ), transfer_base AS (
            SELECT
                transfer_row.id AS pay_bank_transfer_id,
                upper(BTRIM(COALESCE(transfer_row.status, ''))) AS status_text,
                upper(BTRIM(COALESCE(transfer_row.rail_state, ''))) AS rail_state_text,
                transfer_row.completed_at_utc AS completed_at_utc
            FROM public.pay_bank_transfers AS transfer_row
            WHERE transfer_row.pay_batch_id = p_pay_batch_id
        ), transfer_flags AS (
            SELECT
                transfer_base.pay_bank_transfer_id,
                (
                    transfer_base.status_text IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'VOIDED', 'RETURNED', 'REVERTED', 'BLOCKED', 'SUBMISSION_FAILED', 'FAILED_BEFORE_COMMIT')
                    OR transfer_base.rail_state_text IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'VOIDED', 'RETURNED', 'REVERTED', 'BLOCKED', 'SUBMISSION_FAILED', 'FAILED_BEFORE_COMMIT')
                ) AS is_failed,
                (
                    transfer_base.status_text IN ('COMPLETED', 'SETTLED', 'PAID', 'CONFIRMED', 'SUCCESS', 'SUCCEEDED')
                    OR transfer_base.rail_state_text IN ('COMPLETED', 'SETTLED', 'PAID', 'CONFIRMED', 'SUCCESS', 'SUCCEEDED')
                    OR transfer_base.completed_at_utc IS NOT NULL
                ) AS has_success_evidence
            FROM transfer_base
        ), transfer_summary AS (
            SELECT
                COUNT(*)::integer AS transfer_count,
                COUNT(*) FILTER (WHERE transfer_flags.has_success_evidence AND transfer_flags.is_failed IS NOT TRUE)::integer AS terminal_success_transfer_count,
                COUNT(*) FILTER (WHERE transfer_flags.is_failed)::integer AS terminal_failed_transfer_count,
                COUNT(*) FILTER (WHERE transfer_flags.has_success_evidence IS NOT TRUE AND transfer_flags.is_failed IS NOT TRUE)::integer AS pending_or_unknown_transfer_count
            FROM transfer_flags
        ), transfer_event_summary AS (
            SELECT COUNT(*)::integer AS transfer_event_count
            FROM public.pay_bank_transfer_events AS transfer_event
            WHERE transfer_event.pay_batch_id = p_pay_batch_id
        ), provider_attempt_summary AS (
            SELECT COUNT(*)::integer AS provider_attempt_count
            FROM public.banking_pay_operation_provider_attempts AS provider_attempt
            LEFT JOIN public.banking_pay_operations AS provider_operation
              ON provider_operation.id = provider_attempt.operation_id
            WHERE provider_attempt.pay_batch_id = p_pay_batch_id
               OR provider_operation.pay_batch_id = p_pay_batch_id
        ), provider_artifact_summary AS (
            SELECT COUNT(*) FILTER (
                WHERE COALESCE(transfer_scope.provider_submit_attempt_count, 0) > 0
                   OR NULLIF(BTRIM(COALESCE(transfer_scope.provider_idempotency_key, '')), '') IS NOT NULL
                   OR NULLIF(BTRIM(COALESCE(transfer_scope.provider_request_id, '')), '') IS NOT NULL
                   OR NULLIF(BTRIM(COALESCE(transfer_scope.provider_transaction_id, '')), '') IS NOT NULL
                   OR transfer_scope.provider_request_prepared_at_utc IS NOT NULL
                   OR transfer_scope.provider_request_sending_at_utc IS NOT NULL
                   OR transfer_scope.provider_request_sent_at_utc IS NOT NULL
                   OR transfer_scope.provider_response_at_utc IS NOT NULL
                   OR upper(BTRIM(COALESCE(transfer_scope.provider_submit_state, 'NOT_READY'))) IN (
                       'CLAIMED',
                       'REQUEST_PREPARING',
                       'REQUEST_SENDING',
                       'REQUEST_SENT_LOCAL',
                       'PROVIDER_ACCEPTED',
                       'PROVIDER_REJECTED',
                       'PROVIDER_UNKNOWN',
                       'CHUNK_FINALISED'
                   )
            )::integer AS provider_artifact_count
            FROM public.banking_pay_operation_transfer_scope AS transfer_scope
            WHERE transfer_scope.pay_batch_id = p_pay_batch_id
        ), candidate_summary AS (
            SELECT
                COUNT(*)::integer AS candidate_count,
                COUNT(*) FILTER (
                    WHERE upper(BTRIM(COALESCE(batch_candidate.settlement_status, ''))) IN ('SETTLED', 'PAID', 'CONFIRMED')
                       OR batch_candidate.settled_at_utc IS NOT NULL
                )::integer AS settled_candidate_count
            FROM public.pay_batch_candidates AS batch_candidate
            WHERE batch_candidate.pay_batch_id = p_pay_batch_id
        ), no_bank_scope_identity AS (
            SELECT
                settlement_scope.id AS settlement_scope_id,
                settlement_scope.operation_id,
                settlement_scope.status,
                settlement_scope.payload_json,
                COALESCE(
                    NULLIF(BTRIM(COALESCE(settlement_scope.payload_json->>'auth_request_id', '')), ''),
                    confirmation_summary.confirmation_auth_request_id_text,
                    NULLIF(BTRIM(COALESCE(batch_summary.execution_intent_json->>'auth_request_id', '')), '')
                ) AS auth_request_id_text,
                COALESCE(
                    NULLIF(BTRIM(COALESCE(settlement_scope.payload_json->>'execution_operation_id', '')), ''),
                    CASE WHEN settlement_operation.root_operation_id IS NULL THEN NULL::text ELSE settlement_operation.root_operation_id::text END,
                    confirmation_summary.confirmation_execution_operation_id_text,
                    NULLIF(BTRIM(COALESCE(batch_summary.execution_intent_json->>'operation_id', '')), '')
                ) AS execution_operation_id_text,
                (
                    confirmation_summary.confirmation_settlement_operation_id_text IS NULL
                    OR settlement_scope.operation_id::text = confirmation_summary.confirmation_settlement_operation_id_text
                ) AS settlement_operation_matches
            FROM public.banking_pay_operation_settlement_scope AS settlement_scope
            JOIN public.banking_pay_operations AS settlement_operation
              ON settlement_operation.id = settlement_scope.operation_id
            CROSS JOIN batch_summary
            CROSS JOIN confirmation_summary
            WHERE settlement_scope.pay_batch_id = p_pay_batch_id
              AND upper(BTRIM(COALESCE(settlement_scope.pay_channel, ''))) = 'PAYE'
              AND upper(BTRIM(COALESCE(settlement_scope.payload_json->>'scope_kind', ''))) = 'NO_BANK_PAYMENT'
              AND upper(BTRIM(COALESCE(settlement_scope.payload_json->>'no_bank_payment_reason', ''))) = 'EXPLICIT_ZERO_PAYE'
              AND (
                  confirmation_summary.confirmation_settlement_operation_id_text IS NULL
                  OR settlement_scope.operation_id::text = confirmation_summary.confirmation_settlement_operation_id_text
              )
        ), no_bank_scope_base AS (
            SELECT
                no_bank_scope_identity.*,
                EXISTS (
                    SELECT 1
                    FROM public.pay_batch_auth_requests AS auth_request
                    WHERE auth_request.pay_batch_id = p_pay_batch_id
                      AND upper(BTRIM(COALESCE(auth_request.state, ''))) = 'AUTHORISED'
                      AND auth_request.id::text = no_bank_scope_identity.auth_request_id_text
                      AND NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'operation_id', '')), '') = no_bank_scope_identity.execution_operation_id_text
                      AND (
                        lower(BTRIM(COALESCE(auth_request.execution_intent_json->>'no_bank_payment_execution', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
                        OR lower(BTRIM(COALESCE(auth_request.execution_intent_json->>'allow_explicit_zero_no_bank_scopes', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
                      )
                      AND NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'paye_net_state_hash', '')), '') = NULLIF(BTRIM(COALESCE(
                        no_bank_scope_identity.payload_json->>'paye_net_state_hash',
                        no_bank_scope_identity.payload_json->>'authorised_paye_net_state_hash',
                        ''
                      )), '')
                      AND NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'bank_payment_projection_hash', '')), '') = NULLIF(BTRIM(COALESCE(
                        no_bank_scope_identity.payload_json->>'bank_payment_projection_hash',
                        no_bank_scope_identity.payload_json->>'authorised_bank_payment_projection_hash',
                        ''
                      )), '')
                      AND UPPER(NULLIF(BTRIM(COALESCE(auth_request.execution_intent_json->>'pay_channel_scope', '')), '')) = UPPER(NULLIF(BTRIM(COALESCE(
                        no_bank_scope_identity.payload_json->>'projection_scope',
                        no_bank_scope_identity.payload_json->>'authorised_scope',
                        no_bank_scope_identity.payload_json->>'scope',
                        ''
                      )), ''))
                ) AND no_bank_scope_identity.settlement_operation_matches AS is_authorised
            FROM no_bank_scope_identity
        ), no_bank_scope_summary AS (
            SELECT
                COUNT(DISTINCT no_bank_scope_base.operation_id) FILTER (WHERE no_bank_scope_base.status <> 'SKIPPED')::integer AS no_bank_operation_count,
                COUNT(*) FILTER (WHERE no_bank_scope_base.status <> 'SKIPPED')::integer AS no_bank_scope_count,
                COUNT(*) FILTER (WHERE no_bank_scope_base.status = 'SETTLED')::integer AS settled_no_bank_scope_count,
                COUNT(*) FILTER (WHERE no_bank_scope_base.status <> 'SKIPPED' AND no_bank_scope_base.status <> 'SETTLED')::integer AS unresolved_no_bank_scope_count,
                COUNT(*) FILTER (WHERE no_bank_scope_base.status = 'SETTLED' AND no_bank_scope_base.is_authorised)::integer AS authorised_no_bank_scope_count,
                COUNT(*) FILTER (WHERE no_bank_scope_base.status <> 'SKIPPED' AND no_bank_scope_base.is_authorised IS NOT TRUE)::integer AS unauthorised_no_bank_scope_count
            FROM no_bank_scope_base
        ), authorised_no_bank_item_ids AS (
            SELECT DISTINCT scope_item.item_id_text::uuid AS pay_batch_item_id
            FROM no_bank_scope_base
            CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(
                CASE
                    WHEN JSONB_TYPEOF(no_bank_scope_base.payload_json->'pay_batch_item_ids') = 'array'
                      THEN no_bank_scope_base.payload_json->'pay_batch_item_ids'
                    ELSE '[]'::jsonb
                END
            ) AS scope_item(item_id_text)
            WHERE no_bank_scope_base.status = 'SETTLED'
              AND no_bank_scope_base.is_authorised
              AND scope_item.item_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        ), item_summary AS (
            SELECT
                COUNT(*) FILTER (
                    WHERE COALESCE(batch_item.is_voided, false) = false
                      AND COALESCE(batch_item.item_type, '') <> 'DEBT_CREATED'
                )::integer AS item_count,
                COUNT(*) FILTER (
                    WHERE COALESCE(batch_item.is_voided, false) = false
                      AND COALESCE(batch_item.item_type, '') <> 'DEBT_CREATED'
                      AND batch_item.pay_bank_transfer_id IS NOT NULL
                )::integer AS linked_nonvoid_item_count,
                COUNT(*) FILTER (
                    WHERE COALESCE(batch_item.is_voided, false) = false
                      AND COALESCE(batch_item.item_type, '') <> 'DEBT_CREATED'
                      AND (
                        batch_item.pay_bank_transfer_id IS NOT NULL
                        OR EXISTS (
                            SELECT 1
                            FROM authorised_no_bank_item_ids AS authorised_item
                            WHERE authorised_item.pay_batch_item_id = batch_item.id
                        )
                      )
                )::integer AS covered_nonvoid_item_count
            FROM public.pay_batch_candidates AS batch_candidate
            JOIN public.pay_batch_items AS batch_item
              ON batch_item.pay_batch_candidate_id = batch_candidate.id
            WHERE batch_candidate.pay_batch_id = p_pay_batch_id
        )
        SELECT
            batch_summary.batch_status,
            batch_summary.execution_commit_state,
            batch_summary.execution_commit_ref,
            batch_summary.execution_commit_ref IS NOT NULL,
            batch_summary.completed_at_utc_present,
            batch_summary.freshness_validation_status,
            batch_summary.freshness_clean,
            COALESCE(transfer_summary.transfer_count, 0),
            COALESCE(transfer_summary.terminal_success_transfer_count, 0),
            COALESCE(transfer_summary.terminal_failed_transfer_count, 0),
            COALESCE(transfer_summary.pending_or_unknown_transfer_count, 0),
            COALESCE(transfer_event_summary.transfer_event_count, 0),
            COALESCE(provider_attempt_summary.provider_attempt_count, 0),
            COALESCE(provider_artifact_summary.provider_artifact_count, 0),
            COALESCE(candidate_summary.candidate_count, 0),
            COALESCE(candidate_summary.settled_candidate_count, 0),
            COALESCE(item_summary.item_count, 0),
            COALESCE(item_summary.linked_nonvoid_item_count, 0),
            COALESCE(item_summary.covered_nonvoid_item_count, 0),
            COALESCE(no_bank_scope_summary.no_bank_operation_count, 0),
            COALESCE(no_bank_scope_summary.no_bank_scope_count, 0),
            COALESCE(no_bank_scope_summary.settled_no_bank_scope_count, 0),
            COALESCE(no_bank_scope_summary.unresolved_no_bank_scope_count, 0),
            COALESCE(no_bank_scope_summary.authorised_no_bank_scope_count, 0),
            COALESCE(no_bank_scope_summary.unauthorised_no_bank_scope_count, 0),
            COALESCE(confirmation_summary.no_bank_payment_execution, false),
            confirmation_summary.confirmation_mode,
            confirmation_summary.settlement_mode,
            confirmation_summary.local_commit_reference,
            COALESCE(confirmation_auth_summary.confirmation_auth_authorised, false)
        INTO
            v_settlement_batch_status,
            v_settlement_execution_commit_state,
            v_settlement_execution_commit_ref,
            v_settlement_execution_commit_ref_present,
            v_settlement_completed_at_utc_present,
            v_settlement_freshness_validation_status,
            v_settlement_freshness_clean,
            v_settlement_transfer_count,
            v_settlement_terminal_success_transfer_count,
            v_settlement_terminal_failed_transfer_count,
            v_settlement_pending_or_unknown_transfer_count,
            v_settlement_transfer_event_count,
            v_settlement_provider_attempt_count,
            v_settlement_provider_artifact_count,
            v_settlement_candidate_count,
            v_settlement_settled_candidate_count,
            v_settlement_item_count,
            v_settlement_linked_nonvoid_item_count,
            v_settlement_covered_nonvoid_item_count,
            v_settlement_no_bank_operation_count,
            v_settlement_no_bank_scope_count,
            v_settlement_settled_no_bank_scope_count,
            v_settlement_unresolved_no_bank_scope_count,
            v_settlement_authorised_no_bank_scope_count,
            v_settlement_unauthorised_no_bank_scope_count,
            v_settlement_confirmation_no_bank,
            v_settlement_confirmation_mode,
            v_settlement_confirmation_settlement_mode,
            v_settlement_confirmation_local_commit_ref,
            v_settlement_confirmation_auth_authorised
        FROM batch_summary
        CROSS JOIN confirmation_summary
        CROSS JOIN confirmation_auth_summary
        CROSS JOIN transfer_summary
        CROSS JOIN transfer_event_summary
        CROSS JOIN provider_attempt_summary
        CROSS JOIN provider_artifact_summary
        CROSS JOIN candidate_summary
        CROSS JOIN item_summary
        CROSS JOIN no_bank_scope_summary;

        v_settlement_positive_success := (
             upper(BTRIM(COALESCE(v_settlement_batch_status, ''))) = 'SETTLED'
         AND upper(BTRIM(COALESCE(v_settlement_execution_commit_state, ''))) = 'COMMITTED'
         AND COALESCE(v_settlement_execution_commit_ref_present, false)
         AND COALESCE(v_settlement_completed_at_utc_present, false)
         AND COALESCE(v_settlement_transfer_count, 0) > 0
         AND COALESCE(v_settlement_terminal_success_transfer_count, 0) > 0
         AND COALESCE(v_settlement_terminal_failed_transfer_count, 0) = 0
         AND COALESCE(v_settlement_pending_or_unknown_transfer_count, 0) = 0
         AND COALESCE(v_settlement_candidate_count, 0) > 0
         AND COALESCE(v_settlement_settled_candidate_count, 0) = COALESCE(v_settlement_candidate_count, 0)
         AND (COALESCE(v_settlement_item_count, 0) = 0 OR COALESCE(v_settlement_linked_nonvoid_item_count, 0) = COALESCE(v_settlement_item_count, 0))
        );

        v_settlement_no_bank_success := (
             upper(BTRIM(COALESCE(v_settlement_batch_status, ''))) = 'SETTLED'
         AND upper(BTRIM(COALESCE(v_settlement_execution_commit_state, ''))) = 'COMMITTED'
         AND COALESCE(v_settlement_execution_commit_ref, '') LIKE 'NO_BANK_PAYMENT:%'
         AND COALESCE(v_settlement_completed_at_utc_present, false)
         AND COALESCE(v_settlement_transfer_count, 0) = 0
         AND COALESCE(v_settlement_transfer_event_count, 0) = 0
         AND COALESCE(v_settlement_provider_attempt_count, 0) = 0
         AND COALESCE(v_settlement_provider_artifact_count, 0) = 0
         AND COALESCE(v_settlement_candidate_count, 0) > 0
         AND COALESCE(v_settlement_settled_candidate_count, 0) = COALESCE(v_settlement_candidate_count, 0)
         AND COALESCE(v_settlement_confirmation_no_bank, false)
         AND v_settlement_confirmation_local_commit_ref IS NOT NULL
         AND v_settlement_confirmation_local_commit_ref = v_settlement_execution_commit_ref
         AND upper(BTRIM(COALESCE(v_settlement_confirmation_mode, ''))) IN ('NO_BANK_PAYMENT_REVIEW', 'NO_BANK_PAYMENT_EXECUTION', 'NO_BANK_PAYMENT_EXTERNAL_CONFIRMATION')
         AND upper(BTRIM(COALESCE(v_settlement_confirmation_settlement_mode, ''))) IN ('STANDARD_BANK', 'CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT')
         AND COALESCE(v_settlement_confirmation_auth_authorised, false)
         AND COALESCE(v_settlement_no_bank_operation_count, 0) = 1
         AND COALESCE(v_settlement_no_bank_scope_count, 0) > 0
         AND COALESCE(v_settlement_settled_no_bank_scope_count, 0) = COALESCE(v_settlement_no_bank_scope_count, 0)
         AND COALESCE(v_settlement_authorised_no_bank_scope_count, 0) = COALESCE(v_settlement_no_bank_scope_count, 0)
         AND COALESCE(v_settlement_unresolved_no_bank_scope_count, 0) = 0
         AND COALESCE(v_settlement_unauthorised_no_bank_scope_count, 0) = 0
         AND (COALESCE(v_settlement_item_count, 0) = 0 OR COALESCE(v_settlement_covered_nonvoid_item_count, 0) = COALESCE(v_settlement_item_count, 0))
        );

        v_settlement_mixed_success := (
             upper(BTRIM(COALESCE(v_settlement_batch_status, ''))) = 'SETTLED'
         AND upper(BTRIM(COALESCE(v_settlement_execution_commit_state, ''))) = 'COMMITTED'
         AND COALESCE(v_settlement_execution_commit_ref_present, false)
         AND COALESCE(v_settlement_execution_commit_ref, '') NOT LIKE 'NO_BANK_PAYMENT:%'
         AND COALESCE(v_settlement_completed_at_utc_present, false)
         AND COALESCE(v_settlement_transfer_count, 0) > 0
         AND COALESCE(v_settlement_terminal_success_transfer_count, 0) > 0
         AND COALESCE(v_settlement_terminal_failed_transfer_count, 0) = 0
         AND COALESCE(v_settlement_pending_or_unknown_transfer_count, 0) = 0
         AND COALESCE(v_settlement_candidate_count, 0) > 0
         AND COALESCE(v_settlement_settled_candidate_count, 0) = COALESCE(v_settlement_candidate_count, 0)
         AND COALESCE(v_settlement_no_bank_operation_count, 0) = 1
         AND COALESCE(v_settlement_no_bank_scope_count, 0) > 0
         AND COALESCE(v_settlement_settled_no_bank_scope_count, 0) = COALESCE(v_settlement_no_bank_scope_count, 0)
         AND COALESCE(v_settlement_authorised_no_bank_scope_count, 0) = COALESCE(v_settlement_no_bank_scope_count, 0)
         AND COALESCE(v_settlement_unresolved_no_bank_scope_count, 0) = 0
         AND COALESCE(v_settlement_unauthorised_no_bank_scope_count, 0) = 0
         AND (COALESCE(v_settlement_item_count, 0) = 0 OR COALESCE(v_settlement_covered_nonvoid_item_count, 0) = COALESCE(v_settlement_item_count, 0))
        );

        v_settlement_batch_clean_success := (
          COALESCE(v_settlement_positive_success, false)
          OR COALESCE(v_settlement_no_bank_success, false)
          OR COALESCE(v_settlement_mixed_success, false)
        );

        v_settlement_durable_truth := jsonb_build_object(
            'batch_status', v_settlement_batch_status,
            'execution_commit_state', v_settlement_execution_commit_state,
            'execution_commit_ref_present', COALESCE(v_settlement_execution_commit_ref_present, false),
            'execution_commit_ref_is_no_bank', COALESCE(v_settlement_execution_commit_ref, '') LIKE 'NO_BANK_PAYMENT:%',
            'completed_at_utc_present', COALESCE(v_settlement_completed_at_utc_present, false),
            'freshness_validation_status', v_settlement_freshness_validation_status,
            'freshness_clean', COALESCE(v_settlement_freshness_clean, true),
            'freshness_is_diagnostic_only', true,
            'transfer_count', COALESCE(v_settlement_transfer_count, 0),
            'terminal_success_transfer_count', COALESCE(v_settlement_terminal_success_transfer_count, 0),
            'terminal_failed_transfer_count', COALESCE(v_settlement_terminal_failed_transfer_count, 0),
            'pending_or_unknown_transfer_count', COALESCE(v_settlement_pending_or_unknown_transfer_count, 0),
            'transfer_event_count', COALESCE(v_settlement_transfer_event_count, 0),
            'provider_attempt_count', COALESCE(v_settlement_provider_attempt_count, 0),
            'provider_artifact_count', COALESCE(v_settlement_provider_artifact_count, 0),
            'candidate_count', COALESCE(v_settlement_candidate_count, 0),
            'settled_candidate_count', COALESCE(v_settlement_settled_candidate_count, 0),
            'unsettled_candidate_count', GREATEST(COALESCE(v_settlement_candidate_count, 0) - COALESCE(v_settlement_settled_candidate_count, 0), 0),
            'item_count', COALESCE(v_settlement_item_count, 0),
            'linked_nonvoid_item_count', COALESCE(v_settlement_linked_nonvoid_item_count, 0),
            'covered_nonvoid_item_count', COALESCE(v_settlement_covered_nonvoid_item_count, 0),
            'no_bank_operation_count', COALESCE(v_settlement_no_bank_operation_count, 0),
            'no_bank_scope_count', COALESCE(v_settlement_no_bank_scope_count, 0),
            'settled_no_bank_scope_count', COALESCE(v_settlement_settled_no_bank_scope_count, 0),
            'unresolved_no_bank_scope_count', COALESCE(v_settlement_unresolved_no_bank_scope_count, 0),
            'authorised_no_bank_scope_count', COALESCE(v_settlement_authorised_no_bank_scope_count, 0),
            'unauthorised_no_bank_scope_count', COALESCE(v_settlement_unauthorised_no_bank_scope_count, 0),
            'no_bank_confirmation_present', COALESCE(v_settlement_confirmation_no_bank, false),
            'no_bank_confirmation_mode', v_settlement_confirmation_mode,
            'settlement_mode', v_settlement_confirmation_settlement_mode,
            'confirmation_local_commit_ref_matches', (v_settlement_confirmation_local_commit_ref IS NOT NULL AND v_settlement_confirmation_local_commit_ref = v_settlement_execution_commit_ref),
            'confirmation_auth_authorised', COALESCE(v_settlement_confirmation_auth_authorised, false),
            'positive_transfer_backed_success', COALESCE(v_settlement_positive_success, false),
            'pure_no_bank_success', COALESCE(v_settlement_no_bank_success, false),
            'mixed_transfer_and_no_bank_success', COALESCE(v_settlement_mixed_success, false),
            'verified_clean_success', COALESCE(v_settlement_batch_clean_success, false)
        );

        SELECT settlement_operation.*
        INTO v_existing_by_batch
        FROM public.banking_pay_operations AS settlement_operation
        WHERE settlement_operation.pay_batch_id = p_pay_batch_id
          AND settlement_operation.operation_type = 'PAYMENT_SETTLEMENT'
          AND (
                upper(BTRIM(COALESCE(settlement_operation.status, ''))) IN ('QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAITING_PROVIDER')
             OR upper(BTRIM(COALESCE(settlement_operation.status, ''))) = 'REVIEW_REQUIRED'
             OR (COALESCE(v_settlement_batch_clean_success, false) = true AND upper(BTRIM(COALESCE(settlement_operation.status, ''))) = 'COMPLETE' AND settlement_operation.phase = 'COMPLETE')
             OR (COALESCE(v_settlement_batch_clean_success, false) = true AND upper(BTRIM(COALESCE(settlement_operation.status, ''))) IN ('FAILED', 'CANCELLED', 'CANCELED'))
          )
        ORDER BY
          CASE
            WHEN upper(BTRIM(COALESCE(settlement_operation.status, ''))) IN ('QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAITING_PROVIDER') THEN 0
            WHEN COALESCE(v_settlement_batch_clean_success, false) = true AND upper(BTRIM(COALESCE(settlement_operation.status, ''))) = 'COMPLETE' THEN 1
            WHEN COALESCE(v_settlement_batch_clean_success, false) = true AND upper(BTRIM(COALESCE(settlement_operation.status, ''))) IN ('FAILED', 'REVIEW_REQUIRED', 'CANCELLED', 'CANCELED') THEN 2
            ELSE 9
          END,
          COALESCE(settlement_operation.completed_at_utc, settlement_operation.updated_at_utc, settlement_operation.created_at_utc) DESC,
          settlement_operation.id DESC
        LIMIT 1
        FOR UPDATE;

        IF FOUND THEN
            v_operation := v_existing_by_batch;
            v_existing_status := upper(BTRIM(COALESCE(v_operation.status, '')));

            IF v_existing_status IN ('QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAITING_PROVIDER') THEN
                UPDATE public.banking_pay_operations AS settlement_operation_update
                SET config_json = jsonb_strip_nulls(COALESCE(settlement_operation_update.config_json, '{}'::jsonb) || jsonb_build_object(
                        'server_runnable', true,
                        'backend_runner_owned', true,
                        'frontend_completion_required', false,
                        'operation_created_for_backend_runner', true,
                        'run_after_utc', COALESCE(settlement_operation_update.run_after_utc, v_now)::text
                    )),
                    progress_json = jsonb_strip_nulls(COALESCE(settlement_operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
                        'server_runnable', true,
                        'backend_runner_owned', true,
                        'frontend_completion_required', false,
                        'operation_created_for_backend_runner', true,
                        'operation_reused_for_backend_runner', true,
                        'payment_settlement_batch_guard_reused', true,
                        'payment_settlement_batch_guard_reason', 'EXISTING_ACTIVE_SETTLEMENT_OPERATION',
                        'durable_settlement_truth', v_settlement_durable_truth,
                        'runner_flags_repaired_at_utc', v_now::text,
                        'run_after_utc', COALESCE(settlement_operation_update.run_after_utc, v_now)::text
                    )),
                    runner_state = 'RUNNABLE',
                    requires_user_action = false,
                    run_after_utc = COALESCE(settlement_operation_update.run_after_utc, v_now),
                    updated_at_utc = v_now
                WHERE settlement_operation_update.id = v_operation.id
                RETURNING settlement_operation_update.* INTO v_operation;
            ELSIF COALESCE(v_settlement_batch_clean_success, false) = true THEN
                UPDATE public.banking_pay_operations AS settlement_operation_update
                SET status = 'COMPLETE',
                    phase = 'COMPLETE',
                    runner_state = 'COMPLETE',
                    requires_user_action = false,
                    resume_reason = CASE
                        WHEN upper(BTRIM(COALESCE(settlement_operation_update.status, ''))) = 'COMPLETE' THEN COALESCE(settlement_operation_update.resume_reason, 'EXISTING_COMPLETED_SETTLEMENT_OPERATION')
                        ELSE 'BATCH_ALREADY_DURABLY_SETTLED'
                    END,
                    lease_owner = NULL::text,
                    lease_expires_at_utc = NULL::timestamptz,
                    locked_by = NULL::text,
                    lock_expires_at_utc = NULL::timestamptz,
                    run_after_utc = NULL::timestamptz,
                    completed_at_utc = COALESCE(settlement_operation_update.completed_at_utc, v_now),
                    progress_json = jsonb_strip_nulls(COALESCE(settlement_operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
                        'payment_settlement_batch_guard_reused', true,
                        'payment_settlement_batch_guard_reason', CASE
                            WHEN upper(BTRIM(COALESCE(settlement_operation_update.status, ''))) = 'COMPLETE' THEN 'EXISTING_COMPLETED_SETTLEMENT_OPERATION'
                            ELSE 'OBSOLETE_TERMINAL_SETTLEMENT_OPERATION_COMPLETED_BY_DURABLE_SUCCESS'
                        END,
                        'idempotent_settlement_complete', true,
                        'already_settled', true,
                        'obsolete_terminal_status_before_idempotent_completion', CASE
                            WHEN upper(BTRIM(COALESCE(settlement_operation_update.status, ''))) = 'COMPLETE' THEN NULL::text
                            ELSE upper(BTRIM(COALESCE(settlement_operation_update.status, '')))
                        END,
                        'durable_settlement_truth', v_settlement_durable_truth,
                        'batch_guard_checked_at_utc', v_now::text
                    )),
                    result_json = jsonb_strip_nulls(COALESCE(settlement_operation_update.result_json, '{}'::jsonb) || jsonb_build_object(
                        'ok', true,
                        'idempotent_settlement_complete', true,
                        'already_settled', true,
                        'reason', 'BATCH_ALREADY_DURABLY_SETTLED',
                        'durable_settlement_truth', v_settlement_durable_truth
                    )),
                    error_json = CASE
                        WHEN upper(BTRIM(COALESCE(settlement_operation_update.status, ''))) IN ('FAILED', 'REVIEW_REQUIRED', 'CANCELLED', 'CANCELED')
                        THEN jsonb_strip_nulls(COALESCE(settlement_operation_update.error_json, '{}'::jsonb) || jsonb_build_object(
                            'obsolete_by_durable_settlement_success', true,
                            'obsolete_terminal_status_before_idempotent_completion', upper(BTRIM(COALESCE(settlement_operation_update.status, ''))),
                            'durable_settlement_truth', v_settlement_durable_truth,
                            'batch_guard_checked_at_utc', v_now::text
                        ))
                        ELSE settlement_operation_update.error_json
                    END,
                    updated_at_utc = v_now
                WHERE settlement_operation_update.id = v_operation.id
                RETURNING settlement_operation_update.* INTO v_operation;
            END IF;

            RETURN QUERY
            SELECT
                v_operation.id,
                v_operation.operation_type,
                v_operation.status,
                v_operation.phase,
                v_operation.actor_user_id,
                v_operation.workbench_session_id,
                v_operation.pay_batch_id,
                v_operation.root_operation_id,
                v_operation.idempotency_key,
                v_operation.input_json,
                v_operation.config_json,
                v_operation.progress_json,
                v_operation.result_json,
                v_operation.error_json,
                v_operation.total_units,
                v_operation.completed_units,
                v_operation.failed_units,
                v_operation.current_chunk_index,
                v_operation.chunk_count,
                COALESCE(v_operation.lease_owner, v_operation.locked_by),
                COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc),
                v_operation.created_at_utc,
                v_operation.started_at_utc,
                v_operation.updated_at_utc,
                v_operation.completed_at_utc,
                v_operation.failed_at_utc,
                true;
            RETURN;
        ELSIF COALESCE(v_settlement_batch_clean_success, false) = true THEN
            INSERT INTO public.banking_pay_operations (
                operation_type,
                status,
                phase,
                actor_user_id,
                workbench_session_id,
                pay_batch_id,
                root_operation_id,
                idempotency_key,
                input_json,
                config_json,
                progress_json,
                result_json,
                error_json,
                total_units,
                completed_units,
                failed_units,
                current_chunk_index,
                chunk_count,
                locked_by,
                lock_expires_at_utc,
                started_at_utc,
                completed_at_utc,
                run_after_utc,
                lease_owner,
                lease_expires_at_utc,
                heartbeat_at_utc,
                last_advanced_at_utc,
                attempt_count,
                max_attempts,
                requires_user_action,
                runner_state,
                resume_reason
            )
            VALUES (
                v_operation_type,
                'COMPLETE',
                'COMPLETE',
                p_actor_user_id,
                p_workbench_session_id,
                p_pay_batch_id,
                p_root_operation_id,
                v_idempotency_key,
                v_compact_input_json,
                jsonb_strip_nulls(v_compact_config_json || jsonb_build_object(
                    'server_runnable', true,
                    'backend_runner_owned', true,
                    'frontend_completion_required', false,
                    'operation_created_for_backend_runner', true,
                    'idempotent_complete_on_start', true
                )),
                jsonb_strip_nulls(jsonb_build_object(
                    'server_runnable', true,
                    'backend_runner_owned', true,
                    'frontend_completion_required', false,
                    'progress_version', 1,
                    'status_text', 'Payment settlement already complete from durable batch truth.',
                    'operation_created_for_backend_runner', true,
                    'started_by_operation_start', true,
                    'payment_settlement_batch_guard_reused', false,
                    'payment_settlement_batch_guard_reason', 'BATCH_ALREADY_DURABLY_SETTLED',
                    'idempotent_settlement_complete', true,
                    'already_settled', true,
                    'durable_settlement_truth', v_settlement_durable_truth,
                    'created_at_utc', v_now::text
                )),
                jsonb_strip_nulls(jsonb_build_object(
                    'ok', true,
                    'idempotent_settlement_complete', true,
                    'already_settled', true,
                    'reason', 'BATCH_ALREADY_DURABLY_SETTLED',
                    'durable_settlement_truth', v_settlement_durable_truth
                )),
                NULL::jsonb,
                0,
                0,
                0,
                0,
                0,
                NULL::text,
                NULL::timestamptz,
                v_now,
                v_now,
                NULL::timestamptz,
                NULL::text,
                NULL::timestamptz,
                v_now,
                v_now,
                0,
                v_max_attempts,
                false,
                'COMPLETE',
                'BATCH_ALREADY_DURABLY_SETTLED'
            )
            RETURNING * INTO v_operation;

            RETURN QUERY
            SELECT
                v_operation.id,
                v_operation.operation_type,
                v_operation.status,
                v_operation.phase,
                v_operation.actor_user_id,
                v_operation.workbench_session_id,
                v_operation.pay_batch_id,
                v_operation.root_operation_id,
                v_operation.idempotency_key,
                v_operation.input_json,
                v_operation.config_json,
                v_operation.progress_json,
                v_operation.result_json,
                v_operation.error_json,
                v_operation.total_units,
                v_operation.completed_units,
                v_operation.failed_units,
                v_operation.current_chunk_index,
                v_operation.chunk_count,
                COALESCE(v_operation.lease_owner, v_operation.locked_by),
                COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc),
                v_operation.created_at_utc,
                v_operation.started_at_utc,
                v_operation.updated_at_utc,
                v_operation.completed_at_utc,
                v_operation.failed_at_utc,
                false;
            RETURN;
        END IF;
    END IF;

    IF v_operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') AND p_pay_batch_id IS NOT NULL THEN
        SELECT active_operation.*
        INTO v_existing_by_batch
        FROM public.banking_pay_operations AS active_operation
        WHERE active_operation.pay_batch_id = p_pay_batch_id
          AND upper(BTRIM(COALESCE(active_operation.operation_type, ''))) IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
          AND NOT (
            upper(BTRIM(COALESCE(active_operation.operation_type, ''))) = 'PAYMENT_EXECUTE'
            AND (
              lower(BTRIM(COALESCE(active_operation.input_json->>'prepare_bank_csv_export_only', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR upper(BTRIM(COALESCE(active_operation.input_json->>'execution_mode', ''))) = 'BANK_CSV_EXPORT_PREPARE'
              OR BTRIM(COALESCE(active_operation.input_json->>'source', '')) = 'handleBankingPayBatchExportCsv'
              OR lower(COALESCE(active_operation.idempotency_key, '')) LIKE 'bank-csv-export-prepare:%'
            )
          )
          AND upper(BTRIM(COALESCE(active_operation.status, ''))) IN ('QUEUED', 'RUNNING', 'WAITING', 'RUNNABLE', 'CONTINUING', 'WAITING_RETRY', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'AWAITING_AUTHORISATION', 'AWAITING_AUTHORIZATION', 'WAITING_PROVIDER', 'WAITING_FOR_PROVIDER', 'AWAITING_PROVIDER', 'WAITING_USER', 'WAITING_USER_REVIEW', 'REVIEW_REQUIRED')
        ORDER BY
          CASE upper(BTRIM(COALESCE(active_operation.status, '')))
            WHEN 'RUNNING' THEN 0
            WHEN 'WAITING' THEN 1
            WHEN 'RUNNABLE' THEN 2
            WHEN 'QUEUED' THEN 3
            WHEN 'CONTINUING' THEN 4
            WHEN 'WAITING_RETRY' THEN 5
            WHEN 'WAITING_AUTHORISATION' THEN 6
            WHEN 'WAITING_AUTHORIZATION' THEN 6
            WHEN 'AWAITING_AUTHORISATION' THEN 6
            WHEN 'AWAITING_AUTHORIZATION' THEN 6
            WHEN 'WAITING_PROVIDER' THEN 7
            WHEN 'WAITING_FOR_PROVIDER' THEN 7
            WHEN 'AWAITING_PROVIDER' THEN 7
            WHEN 'WAITING_USER' THEN 8
            WHEN 'WAITING_USER_REVIEW' THEN 8
            WHEN 'REVIEW_REQUIRED' THEN 9
            ELSE 10
          END,
          active_operation.updated_at_utc DESC NULLS LAST,
          active_operation.created_at_utc DESC NULLS LAST,
          active_operation.id DESC
        LIMIT 1
        FOR UPDATE;

        IF FOUND THEN
            v_operation := v_existing_by_batch;
            RETURN QUERY
            SELECT
                v_operation.id,
                v_operation.operation_type,
                v_operation.status,
                v_operation.phase,
                v_operation.actor_user_id,
                v_operation.workbench_session_id,
                v_operation.pay_batch_id,
                v_operation.root_operation_id,
                v_operation.idempotency_key,
                v_operation.input_json,
                v_operation.config_json,
                v_operation.progress_json,
                v_operation.result_json,
                v_operation.error_json,
                v_operation.total_units,
                v_operation.completed_units,
                v_operation.failed_units,
                v_operation.current_chunk_index,
                v_operation.chunk_count,
                COALESCE(v_operation.lease_owner, v_operation.locked_by),
                COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc),
                v_operation.created_at_utc,
                v_operation.started_at_utc,
                v_operation.updated_at_utc,
                v_operation.completed_at_utc,
                v_operation.failed_at_utc,
                true;
            RETURN;
        END IF;
    END IF;

    IF v_operation_type = 'DRAFT_CREATE' AND p_workbench_session_id IS NOT NULL THEN
        UPDATE public.banking_pay_operations AS stale_draft_update
        SET status = 'FAILED',
            runner_state = 'FAILED',
            requires_user_action = false,
            resume_reason = CASE
                WHEN v_allow_restart IS TRUE THEN 'DRAFT_CREATE_EXPLICIT_RESTART_TERMINATED'
                WHEN COALESCE(stale_draft_update.attempt_count, 0) >= COALESCE(stale_draft_update.max_attempts, 10) THEN 'DRAFT_CREATE_ATTEMPT_LIMIT_EXHAUSTED'
                ELSE 'STALE_DRAFT_CREATE_TERMINATED'
            END,
            lease_owner = NULL::text,
            lease_expires_at_utc = NULL::timestamptz,
            locked_by = NULL::text,
            lock_expires_at_utc = NULL::timestamptz,
            run_after_utc = NULL::timestamptz,
            failed_at_utc = COALESCE(stale_draft_update.failed_at_utc, v_now),
            error_json = jsonb_strip_nulls(COALESCE(stale_draft_update.error_json, '{}'::jsonb) || jsonb_build_object(
                'code', CASE
                    WHEN v_allow_restart IS TRUE THEN 'DRAFT_CREATE_EXPLICIT_RESTART_TERMINATED'
                    WHEN COALESCE(stale_draft_update.attempt_count, 0) >= COALESCE(stale_draft_update.max_attempts, 10) THEN 'DRAFT_CREATE_ATTEMPT_LIMIT_EXHAUSTED'
                    ELSE 'STALE_DRAFT_CREATE_TERMINATED'
                END,
                'message', 'DRAFT_CREATE operation was terminalised by the active workbench-session guard.',
                'operation_id', stale_draft_update.id::text,
                'attempt_count', stale_draft_update.attempt_count,
                'max_attempts', stale_draft_update.max_attempts,
                'terminalised_at_utc', v_now::text
            )),
            progress_json = jsonb_strip_nulls(COALESCE(stale_draft_update.progress_json, '{}'::jsonb) || jsonb_build_object(
                'active_operation_guard', 'DRAFT_CREATE_WORKBENCH_SESSION',
                'terminalised_by_operation_start', true,
                'terminalised_at_utc', v_now::text
            )),
            updated_at_utc = v_now
        WHERE upper(BTRIM(COALESCE(stale_draft_update.operation_type, ''))) = 'DRAFT_CREATE'
          AND (
            stale_draft_update.workbench_session_id = p_workbench_session_id
            OR COALESCE(stale_draft_update.input_json->>'workbench_session_id', '') = p_workbench_session_id::text
            OR COALESCE(stale_draft_update.input_json->>'workbenchSessionId', '') = p_workbench_session_id::text
            OR COALESCE(stale_draft_update.input_json->>'session_id', '') = p_workbench_session_id::text
            OR COALESCE(stale_draft_update.progress_json->>'workbench_session_id', '') = p_workbench_session_id::text
            OR COALESCE(stale_draft_update.progress_json->>'session_id', '') = p_workbench_session_id::text
          )
          AND upper(BTRIM(COALESCE(stale_draft_update.status, ''))) IN ('QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAITING_PROVIDER', 'CONTINUING', 'WAITING_RETRY')
          AND (
            v_allow_restart IS TRUE
            OR COALESCE(stale_draft_update.attempt_count, 0) >= COALESCE(stale_draft_update.max_attempts, 10)
            OR (
              (stale_draft_update.lease_owner IS NULL OR stale_draft_update.lease_expires_at_utc IS NULL OR stale_draft_update.lease_expires_at_utc <= v_now)
              AND COALESCE(stale_draft_update.last_advanced_at_utc, stale_draft_update.updated_at_utc, stale_draft_update.created_at_utc, v_now) <= v_now - make_interval(mins => v_draft_stale_minutes)
            )
          );

        SELECT active_draft_operation.*
        INTO v_operation
        FROM public.banking_pay_operations AS active_draft_operation
        WHERE upper(BTRIM(COALESCE(active_draft_operation.operation_type, ''))) = 'DRAFT_CREATE'
          AND (
            active_draft_operation.workbench_session_id = p_workbench_session_id
            OR COALESCE(active_draft_operation.input_json->>'workbench_session_id', '') = p_workbench_session_id::text
            OR COALESCE(active_draft_operation.input_json->>'workbenchSessionId', '') = p_workbench_session_id::text
            OR COALESCE(active_draft_operation.input_json->>'session_id', '') = p_workbench_session_id::text
            OR COALESCE(active_draft_operation.progress_json->>'workbench_session_id', '') = p_workbench_session_id::text
            OR COALESCE(active_draft_operation.progress_json->>'session_id', '') = p_workbench_session_id::text
          )
          AND upper(BTRIM(COALESCE(active_draft_operation.status, ''))) IN ('QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAITING_PROVIDER', 'CONTINUING', 'WAITING_RETRY')
          AND COALESCE(active_draft_operation.attempt_count, 0) < COALESCE(active_draft_operation.max_attempts, 10)
        ORDER BY active_draft_operation.updated_at_utc DESC NULLS LAST, active_draft_operation.created_at_utc DESC NULLS LAST, active_draft_operation.id DESC
        LIMIT 1
        FOR UPDATE;

        IF FOUND THEN
            RETURN QUERY
            SELECT
                v_operation.id,
                v_operation.operation_type,
                v_operation.status,
                v_operation.phase,
                v_operation.actor_user_id,
                v_operation.workbench_session_id,
                v_operation.pay_batch_id,
                v_operation.root_operation_id,
                v_operation.idempotency_key,
                v_operation.input_json,
                v_operation.config_json,
                v_operation.progress_json,
                v_operation.result_json,
                v_operation.error_json,
                v_operation.total_units,
                v_operation.completed_units,
                v_operation.failed_units,
                v_operation.current_chunk_index,
                v_operation.chunk_count,
                COALESCE(v_operation.lease_owner, v_operation.locked_by),
                COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc),
                v_operation.created_at_utc,
                v_operation.started_at_utc,
                v_operation.updated_at_utc,
                v_operation.completed_at_utc,
                v_operation.failed_at_utc,
                true;
            RETURN;
        END IF;
    END IF;

    INSERT INTO public.banking_pay_operations (
        operation_type,
        status,
        phase,
        actor_user_id,
        workbench_session_id,
        pay_batch_id,
        root_operation_id,
        idempotency_key,
        input_json,
        config_json,
        progress_json,
        result_json,
        error_json,
        total_units,
        completed_units,
        failed_units,
        current_chunk_index,
        chunk_count,
        locked_by,
        lock_expires_at_utc,
        run_after_utc,
        lease_owner,
        lease_expires_at_utc,
        heartbeat_at_utc,
        last_advanced_at_utc,
        attempt_count,
        max_attempts,
        requires_user_action,
        runner_state,
        resume_reason
    )
    VALUES (
        v_operation_type,
        v_initial_status,
        v_initial_phase,
        p_actor_user_id,
        p_workbench_session_id,
        p_pay_batch_id,
        p_root_operation_id,
        v_idempotency_key,
        v_compact_input_json,
        v_compact_config_json,
        jsonb_strip_nulls(jsonb_build_object(
            'server_runnable', v_server_runnable,
            'backend_runner_owned', v_backend_runner_owned,
            'frontend_completion_required', v_frontend_completion_required,
            'progress_version', 1,
            'status_text', CASE
                WHEN v_operation_type = 'DRAFT_CREATE' THEN 'Draft creation operation created for backend runner.'
                WHEN v_operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN 'Payment execution operation created for backend runner.'
                WHEN v_operation_type = 'PAYMENT_SETTLEMENT' THEN 'Payment settlement operation created for backend runner.'
                WHEN v_operation_type = 'REMITTANCE_QUEUE' THEN 'Remittance queue operation created for backend runner.'
                ELSE 'Operation created.'
            END,
            'bounded_chunk_config', v_chunk_config,
            'operation_created_for_backend_runner', v_backend_runner_owned,
            'started_by_operation_start', true,
            'active_operation_guard', CASE WHEN v_operation_type = 'DRAFT_CREATE' THEN 'DRAFT_CREATE_WORKBENCH_SESSION' ELSE NULL::text END,
            'created_at_utc', v_now::text,
            'run_after_utc', CASE WHEN v_run_after_utc IS NULL THEN NULL ELSE v_run_after_utc::text END
        )),
        NULL::jsonb,
        NULL::jsonb,
        0,
        0,
        0,
        0,
        0,
        NULL::text,
        NULL::timestamptz,
        v_run_after_utc,
        NULL::text,
        NULL::timestamptz,
        v_now,
        NULL::timestamptz,
        0,
        v_max_attempts,
        false,
        v_runner_state,
        'OPERATION_STARTED'
    )
    RETURNING * INTO v_operation;

    RETURN QUERY
    SELECT
        v_operation.id,
        v_operation.operation_type,
        v_operation.status,
        v_operation.phase,
        v_operation.actor_user_id,
        v_operation.workbench_session_id,
        v_operation.pay_batch_id,
        v_operation.root_operation_id,
        v_operation.idempotency_key,
        v_operation.input_json,
        v_operation.config_json,
        v_operation.progress_json,
        v_operation.result_json,
        v_operation.error_json,
        v_operation.total_units,
        v_operation.completed_units,
        v_operation.failed_units,
        v_operation.current_chunk_index,
        v_operation.chunk_count,
        COALESCE(v_operation.lease_owner, v_operation.locked_by),
        COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc),
        v_operation.created_at_utc,
        v_operation.started_at_utc,
        v_operation.updated_at_utc,
        v_operation.completed_at_utc,
        v_operation.failed_at_utc,
        false;
END;
$function$;
ALTER FUNCTION public.banking_pay_operation_start(text,uuid,text,uuid,uuid,uuid,jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_operation_start(text,uuid,text,uuid,uuid,uuid,jsonb,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.banking_pay_operation_start(text,uuid,text,uuid,uuid,uuid,jsonb,jsonb) FROM anon;
REVOKE ALL ON FUNCTION public.banking_pay_operation_start(text,uuid,text,uuid,uuid,uuid,jsonb,jsonb) FROM authenticated;
REVOKE ALL ON FUNCTION public.banking_pay_operation_start(text,uuid,text,uuid,uuid,uuid,jsonb,jsonb) FROM service_role;
GRANT EXECUTE ON FUNCTION public.banking_pay_operation_start(text,uuid,text,uuid,uuid,uuid,jsonb,jsonb) TO service_role;
