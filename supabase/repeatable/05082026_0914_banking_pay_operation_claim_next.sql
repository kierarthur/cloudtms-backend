-- Canonical Banking Pay operation claim authority.
-- This identity was extracted from the legacy monolithic repeatable so it has
-- exactly one authoritative CREATE OR REPLACE FUNCTION body.


CREATE OR REPLACE FUNCTION public.banking_pay_operation_claim_next(
    p_operation_id uuid DEFAULT NULL::uuid,
    p_actor_user_id uuid DEFAULT NULL::uuid,
    p_lock_owner text DEFAULT NULL::text,
    p_lock_seconds integer DEFAULT 60,
    p_allow_backend_runner_owned boolean DEFAULT false,
    p_operation_types text[] DEFAULT NULL::text[]
)
RETURNS TABLE (
    claimed boolean,
    not_claimed_reason text,
    operation_id uuid,
    operation_type text,
    status text,
    phase text,
    actor_user_id uuid,
    workbench_session_id uuid,
    pay_batch_id uuid,
    root_operation_id uuid,
    idempotency_key text,
    input_json jsonb,
    config_json jsonb,
    progress_json jsonb,
    result_json jsonb,
    error_json jsonb,
    total_units integer,
    completed_units integer,
    failed_units integer,
    current_chunk_index integer,
    chunk_count integer,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz,
    started_at_utc timestamptz,
    updated_at_utc timestamptz,
    completed_at_utc timestamptz,
    failed_at_utc timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
SET search_path TO 'pg_catalog', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
    v_now timestamptz := now();
    v_operation public.banking_pay_operations%ROWTYPE;
    v_visible public.banking_pay_operations%ROWTYPE;
    v_lock_owner text := COALESCE(NULLIF(BTRIM(COALESCE(p_lock_owner, '')), ''), 'banking-runner:' || pg_backend_pid()::text);
    v_lock_seconds integer := LEAST(GREATEST(COALESCE(p_lock_seconds, 60), 10), 3600);
    v_not_claimed_reason text := NULL::text;
    v_allow_backend_runner_owned boolean := COALESCE(p_allow_backend_runner_owned, false);
    v_operation_types text[] := ARRAY[]::text[];
    v_visible_operation_type text := NULL::text;
    v_visible_backend_runner_claimable boolean := false;
    v_visible_actor_authorised boolean := false;
BEGIN
    PERFORM set_config('lock_timeout', '3s', true);

    SELECT COALESCE(array_agg(DISTINCT supplied_operation_type.normalized_operation_type) FILTER (WHERE supplied_operation_type.normalized_operation_type IS NOT NULL), ARRAY[]::text[])
    INTO v_operation_types
    FROM (
      SELECT NULLIF(upper(BTRIM(COALESCE(operation_type_value, ''))), '') AS normalized_operation_type
      FROM unnest(COALESCE(p_operation_types, ARRAY[]::text[])) AS supplied(operation_type_value)
    ) AS supplied_operation_type;

    IF COALESCE(array_length(v_operation_types, 1), 0) = 0 AND v_allow_backend_runner_owned IS TRUE THEN
      v_operation_types := ARRAY['DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS', 'PAYMENT_SETTLEMENT', 'REMITTANCE_QUEUE', 'PAYMENT_CORRECTION']::text[];
    END IF;

    WITH claimable AS (
      SELECT operation_row.id
      FROM public.banking_pay_operations AS operation_row
      WHERE (p_operation_id IS NULL OR operation_row.id = p_operation_id)
        AND (
          COALESCE(array_length(v_operation_types, 1), 0) = 0
          OR upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = ANY(v_operation_types)
        )
        AND (
          p_actor_user_id IS NULL
          OR operation_row.actor_user_id IS NULL
          OR operation_row.actor_user_id = p_actor_user_id
          OR (
            v_allow_backend_runner_owned IS TRUE
            AND upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = ANY(v_operation_types)
            AND (
              upper(BTRIM(COALESCE(operation_row.input_json->>'backend_runner_owned', operation_row.input_json->>'backendRunnerOwned', operation_row.config_json->>'backend_runner_owned', operation_row.config_json->>'backendRunnerOwned', operation_row.progress_json->>'backend_runner_owned', operation_row.progress_json->>'backendRunnerOwned', operation_row.progress_json->>'server_runnable', operation_row.progress_json->>'serverRunnable', 'false'))) IN ('TRUE', 'T', '1', 'YES', 'Y', 'ON')
              OR (
                operation_row.config_json ? 'frontend_completion_required'
                AND upper(BTRIM(COALESCE(operation_row.config_json->>'frontend_completion_required', ''))) IN ('FALSE', 'F', '0', 'NO', 'N', 'OFF')
              )
              OR (
                operation_row.config_json ? 'frontendCompletionRequired'
                AND upper(BTRIM(COALESCE(operation_row.config_json->>'frontendCompletionRequired', ''))) IN ('FALSE', 'F', '0', 'NO', 'N', 'OFF')
              )
            )
          )
        )
        AND (
          (
            upper(BTRIM(COALESCE(operation_row.status, ''))) = 'RUNNING'
            AND upper(BTRIM(COALESCE(operation_row.runner_state, ''))) IN ('RUNNABLE', 'RUNNING', 'IDLE')
          )
          OR (
            upper(BTRIM(COALESCE(operation_row.status, ''))) = 'WAITING'
            AND upper(BTRIM(COALESCE(operation_row.runner_state, ''))) = 'RUNNABLE'
          )
          OR (
            upper(BTRIM(COALESCE(operation_row.status, ''))) = 'WAITING'
            AND upper(BTRIM(COALESCE(operation_row.runner_state, ''))) = 'WAITING_CHILD'
            AND p_operation_id IS NOT NULL
            AND v_allow_backend_runner_owned IS TRUE
            AND COALESCE(operation_row.progress_json->>'child_operation_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
            AND EXISTS (
              SELECT 1
              FROM public.banking_pay_operations AS child_operation
              WHERE child_operation.id = (operation_row.progress_json->>'child_operation_id')::uuid
                AND child_operation.root_operation_id = operation_row.id
                AND upper(BTRIM(COALESCE(child_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED')
            )
          )
          OR (
            upper(BTRIM(COALESCE(operation_row.status, ''))) = 'WAITING_PROVIDER'
            AND v_allow_backend_runner_owned IS TRUE
            AND upper(BTRIM(COALESCE(operation_row.runner_state, ''))) IN ('WAITING_PROVIDER', 'RUNNABLE')
            AND (
              (
                upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = 'PAYMENT_EXECUTE'
                AND (
                  upper(BTRIM(COALESCE(operation_row.phase, ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
                  OR upper(BTRIM(COALESCE(operation_row.progress_json->>'phase', operation_row.progress_json->>'operation_phase', operation_row.progress_json->>'operationPhase', operation_row.progress_json->>'next_phase', operation_row.progress_json->>'nextPhase', ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
                  OR upper(BTRIM(COALESCE(operation_row.resume_reason, operation_row.progress_json->>'resume_reason', operation_row.progress_json->>'resumeReason', ''))) IN ('AWAITING_PROVIDER_OUTCOME', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT')
                )
              )
              OR (
                upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = 'PAYMENT_SETTLEMENT'
                AND (
                  upper(BTRIM(COALESCE(operation_row.phase, ''))) = 'APPLY_SETTLEMENT_CHUNKS'
                  OR upper(BTRIM(COALESCE(operation_row.progress_json->>'phase', operation_row.progress_json->>'operation_phase', operation_row.progress_json->>'operationPhase', operation_row.progress_json->>'next_phase', operation_row.progress_json->>'nextPhase', ''))) = 'APPLY_SETTLEMENT_CHUNKS'
                )
              )
            )
          )
        )
        AND COALESCE(operation_row.requires_user_action, false) = false
        AND COALESCE(operation_row.run_after_utc, v_now) <= v_now
        AND (
          operation_row.lease_owner IS NULL
          OR operation_row.lease_expires_at_utc IS NULL
          OR operation_row.lease_expires_at_utc <= v_now
        )
        AND COALESCE(operation_row.attempt_count, 0) < COALESCE(operation_row.max_attempts, 10)
      ORDER BY COALESCE(operation_row.run_after_utc, operation_row.created_at_utc, v_now), operation_row.created_at_utc, operation_row.id
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    )
    UPDATE public.banking_pay_operations AS operation_update
    SET status = CASE WHEN upper(BTRIM(COALESCE(operation_update.status, ''))) IN ('WAITING', 'WAITING_PROVIDER') THEN 'RUNNING' ELSE operation_update.status END,
        runner_state = 'RUNNING',
        lease_owner = v_lock_owner,
        lease_expires_at_utc = v_now + make_interval(secs => v_lock_seconds),
        heartbeat_at_utc = v_now,
        last_advanced_at_utc = v_now,
        started_at_utc = COALESCE(operation_update.started_at_utc, v_now),
        locked_by = v_lock_owner,
        lock_expires_at_utc = v_now + make_interval(secs => v_lock_seconds),
        progress_json = jsonb_strip_nulls(COALESCE(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
          'last_claimed_at_utc', v_now::text,
          'last_claim_lease_owner', v_lock_owner,
          'last_claim_runner_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
          'last_claim_backend_runner_owned', v_allow_backend_runner_owned,
          'runner_state', 'RUNNING',
          'claim_count', (
            CASE
              WHEN COALESCE(operation_update.progress_json->>'claim_count', '') ~ '^[0-9]+$'
                THEN (operation_update.progress_json->>'claim_count')::integer
              ELSE 0
            END
          ) + 1,
          'backend_runner_claim', v_allow_backend_runner_owned,
          'runner_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
        )),
        updated_at_utc = v_now
    FROM claimable
    WHERE operation_update.id = claimable.id
    RETURNING operation_update.* INTO v_operation;

    IF FOUND THEN
      RETURN QUERY
      SELECT
        true,
        NULL::text,
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
        jsonb_strip_nulls(COALESCE(v_operation.progress_json, '{}'::jsonb) || jsonb_build_object(
          'runner_state', v_operation.runner_state,
          'run_after_utc', CASE WHEN v_operation.run_after_utc IS NULL THEN NULL ELSE v_operation.run_after_utc::text END,
          'lease_owner', v_operation.lease_owner,
          'lease_expires_at_utc', CASE WHEN v_operation.lease_expires_at_utc IS NULL THEN NULL ELSE v_operation.lease_expires_at_utc::text END,
          'heartbeat_at_utc', CASE WHEN v_operation.heartbeat_at_utc IS NULL THEN NULL ELSE v_operation.heartbeat_at_utc::text END,
          'last_advanced_at_utc', CASE WHEN v_operation.last_advanced_at_utc IS NULL THEN NULL ELSE v_operation.last_advanced_at_utc::text END,
          'requires_user_action', COALESCE(v_operation.requires_user_action, false),
          'resume_reason', v_operation.resume_reason,
          'attempt_count', COALESCE(v_operation.attempt_count, 0),
          'max_attempts', v_operation.max_attempts
        )),
        v_operation.result_json,
        v_operation.error_json,
        v_operation.total_units,
        v_operation.completed_units,
        v_operation.failed_units,
        v_operation.current_chunk_index,
        v_operation.chunk_count,
        v_operation.lease_owner,
        v_operation.lease_expires_at_utc,
        v_operation.created_at_utc,
        v_operation.started_at_utc,
        v_operation.updated_at_utc,
        v_operation.completed_at_utc,
        v_operation.failed_at_utc;
      RETURN;
    END IF;

    IF p_operation_id IS NOT NULL THEN
      SELECT visible_operation.*
      INTO v_visible
      FROM public.banking_pay_operations AS visible_operation
      WHERE visible_operation.id = p_operation_id;

      IF FOUND THEN
        v_visible_operation_type := upper(BTRIM(COALESCE(v_visible.operation_type, '')));
        v_visible_backend_runner_claimable := v_allow_backend_runner_owned IS TRUE
          AND (
            COALESCE(array_length(v_operation_types, 1), 0) = 0
            OR v_visible_operation_type = ANY(v_operation_types)
          )
          AND (
            upper(BTRIM(COALESCE(v_visible.input_json->>'backend_runner_owned', v_visible.input_json->>'backendRunnerOwned', v_visible.config_json->>'backend_runner_owned', v_visible.config_json->>'backendRunnerOwned', v_visible.progress_json->>'backend_runner_owned', v_visible.progress_json->>'backendRunnerOwned', v_visible.progress_json->>'server_runnable', v_visible.progress_json->>'serverRunnable', 'false'))) IN ('TRUE', 'T', '1', 'YES', 'Y', 'ON')
            OR (
              v_visible.config_json ? 'frontend_completion_required'
              AND upper(BTRIM(COALESCE(v_visible.config_json->>'frontend_completion_required', ''))) IN ('FALSE', 'F', '0', 'NO', 'N', 'OFF')
            )
            OR (
              v_visible.config_json ? 'frontendCompletionRequired'
              AND upper(BTRIM(COALESCE(v_visible.config_json->>'frontendCompletionRequired', ''))) IN ('FALSE', 'F', '0', 'NO', 'N', 'OFF')
            )
          );
        v_visible_actor_authorised := p_actor_user_id IS NULL OR v_visible.actor_user_id IS NULL OR v_visible.actor_user_id = p_actor_user_id OR v_visible_backend_runner_claimable IS TRUE;

        IF v_visible_actor_authorised IS NOT TRUE THEN
          RETURN QUERY
          SELECT
            false,
            'ACTOR_MISMATCH'::text,
            p_operation_id,
            NULL::text,
            NULL::text,
            NULL::text,
            NULL::uuid,
            NULL::uuid,
            NULL::uuid,
            NULL::uuid,
            NULL::text,
            NULL::jsonb,
            NULL::jsonb,
            NULL::jsonb,
            NULL::jsonb,
            NULL::jsonb,
            NULL::integer,
            NULL::integer,
            NULL::integer,
            NULL::integer,
            NULL::integer,
            NULL::text,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz;
          RETURN;
        END IF;

        v_not_claimed_reason := CASE
          WHEN COALESCE(array_length(v_operation_types, 1), 0) > 0 AND v_visible_operation_type <> ALL(v_operation_types) THEN 'OPERATION_TYPE_NOT_IN_SCOPE'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) IN ('WAITING_AUTHORISATION', 'REVIEW_REQUIRED', 'COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED') THEN 'NOT_RUNNABLE_STATUS'
          WHEN COALESCE(v_visible.requires_user_action, false) THEN 'REQUIRES_USER_ACTION'
          WHEN COALESCE(v_visible.run_after_utc, v_now) > v_now THEN 'RUN_AFTER_NOT_DUE'
          WHEN v_visible.lease_owner IS NOT NULL AND v_visible.lease_expires_at_utc IS NOT NULL AND v_visible.lease_expires_at_utc > v_now THEN 'LEASE_ACTIVE'
          WHEN COALESCE(v_visible.attempt_count, 0) >= COALESCE(v_visible.max_attempts, 10) THEN 'MAX_ATTEMPTS_REACHED'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'RUNNING'
           AND upper(BTRIM(COALESCE(v_visible.runner_state, ''))) NOT IN ('RUNNABLE', 'RUNNING', 'IDLE') THEN 'RUNNING_NOT_RUNNABLE'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING'
           AND upper(BTRIM(COALESCE(v_visible.runner_state, ''))) = 'WAITING_CHILD' THEN 'WAITING_CHILD_NOT_TERMINAL'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING'
           AND upper(BTRIM(COALESCE(v_visible.runner_state, ''))) <> 'RUNNABLE' THEN 'WAITING_NOT_RUNNABLE'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING_PROVIDER'
           AND v_allow_backend_runner_owned IS NOT TRUE THEN 'WAITING_PROVIDER_BACKEND_RUNNER_REQUIRED'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING_PROVIDER'
           AND upper(BTRIM(COALESCE(v_visible.operation_type, ''))) NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_SETTLEMENT') THEN 'WAITING_PROVIDER_OPERATION_TYPE_NOT_CLAIMABLE'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING_PROVIDER'
           AND upper(BTRIM(COALESCE(v_visible.runner_state, ''))) NOT IN ('WAITING_PROVIDER', 'RUNNABLE') THEN 'WAITING_PROVIDER_NOT_RUNNABLE'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING_PROVIDER'
           AND NOT (
             (
               upper(BTRIM(COALESCE(v_visible.operation_type, ''))) = 'PAYMENT_EXECUTE'
               AND (
                 upper(BTRIM(COALESCE(v_visible.phase, ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
                 OR upper(BTRIM(COALESCE(v_visible.progress_json->>'phase', v_visible.progress_json->>'operation_phase', v_visible.progress_json->>'operationPhase', v_visible.progress_json->>'next_phase', v_visible.progress_json->>'nextPhase', ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
                 OR upper(BTRIM(COALESCE(v_visible.resume_reason, v_visible.progress_json->>'resume_reason', v_visible.progress_json->>'resumeReason', ''))) IN ('AWAITING_PROVIDER_OUTCOME', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT')
               )
             )
             OR (
               upper(BTRIM(COALESCE(v_visible.operation_type, ''))) = 'PAYMENT_SETTLEMENT'
               AND (
                 upper(BTRIM(COALESCE(v_visible.phase, ''))) = 'APPLY_SETTLEMENT_CHUNKS'
                 OR upper(BTRIM(COALESCE(v_visible.progress_json->>'phase', v_visible.progress_json->>'operation_phase', v_visible.progress_json->>'operationPhase', v_visible.progress_json->>'next_phase', v_visible.progress_json->>'nextPhase', ''))) = 'APPLY_SETTLEMENT_CHUNKS'
               )
             )
           ) THEN 'WAITING_PROVIDER_NOT_RECHECK_PHASE'
          ELSE 'NOT_RUNNABLE'
        END;

        RETURN QUERY
        SELECT
          false,
          v_not_claimed_reason,
          v_visible.id,
          v_visible.operation_type,
          v_visible.status,
          v_visible.phase,
          v_visible.actor_user_id,
          v_visible.workbench_session_id,
          v_visible.pay_batch_id,
          v_visible.root_operation_id,
          v_visible.idempotency_key,
          v_visible.input_json,
          v_visible.config_json,
          jsonb_strip_nulls(COALESCE(v_visible.progress_json, '{}'::jsonb) || jsonb_build_object(
            'runner_state', v_visible.runner_state,
            'run_after_utc', CASE WHEN v_visible.run_after_utc IS NULL THEN NULL ELSE v_visible.run_after_utc::text END,
            'lease_owner', v_visible.lease_owner,
            'lease_expires_at_utc', CASE WHEN v_visible.lease_expires_at_utc IS NULL THEN NULL ELSE v_visible.lease_expires_at_utc::text END,
            'requires_user_action', COALESCE(v_visible.requires_user_action, false),
            'resume_reason', v_visible.resume_reason,
            'attempt_count', COALESCE(v_visible.attempt_count, 0),
            'max_attempts', v_visible.max_attempts,
            'backend_runner_claimable', v_visible_backend_runner_claimable
          )),
          v_visible.result_json,
          v_visible.error_json,
          v_visible.total_units,
          v_visible.completed_units,
          v_visible.failed_units,
          v_visible.current_chunk_index,
          v_visible.chunk_count,
          COALESCE(v_visible.lease_owner, v_visible.locked_by),
          COALESCE(v_visible.lease_expires_at_utc, v_visible.lock_expires_at_utc),
          v_visible.created_at_utc,
          v_visible.started_at_utc,
          v_visible.updated_at_utc,
          v_visible.completed_at_utc,
          v_visible.failed_at_utc;
        RETURN;
      END IF;
    END IF;

    RETURN QUERY
    SELECT
      false,
      CASE WHEN p_operation_id IS NULL THEN 'NO_RUNNABLE_OPERATION' ELSE 'NOT_FOUND_OR_NOT_AUTHORISED' END,
      p_operation_id,
      NULL::text,
      NULL::text,
      NULL::text,
      NULL::uuid,
      NULL::uuid,
      NULL::uuid,
      NULL::uuid,
      NULL::text,
      NULL::jsonb,
      NULL::jsonb,
      NULL::jsonb,
      NULL::jsonb,
      NULL::jsonb,
      NULL::integer,
      NULL::integer,
      NULL::integer,
      NULL::integer,
      NULL::integer,
      NULL::text,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz;
END;
$function$;

ALTER FUNCTION public.banking_pay_operation_claim_next(uuid,uuid,text,integer,boolean,text[]) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_operation_claim_next(uuid,uuid,text,integer,boolean,text[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.banking_pay_operation_claim_next(uuid,uuid,text,integer,boolean,text[]) FROM anon;
REVOKE ALL ON FUNCTION public.banking_pay_operation_claim_next(uuid,uuid,text,integer,boolean,text[]) FROM authenticated;
REVOKE ALL ON FUNCTION public.banking_pay_operation_claim_next(uuid,uuid,text,integer,boolean,text[]) FROM service_role;
GRANT EXECUTE ON FUNCTION public.banking_pay_operation_claim_next(uuid,uuid,text,integer,boolean,text[]) TO service_role;