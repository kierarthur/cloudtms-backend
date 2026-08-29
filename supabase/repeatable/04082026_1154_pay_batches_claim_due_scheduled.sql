-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- Canonical bounded due-schedule claim; no direct operation insert.

CREATE OR REPLACE FUNCTION public.pay_batches_claim_due_scheduled(
  p_limit integer DEFAULT 50,
  p_now_utc timestamptz DEFAULT NULL::timestamptz
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path TO pg_catalog, private, extensions, pg_temp
SET statement_timeout TO '6000ms'
SET lock_timeout TO '1000ms'
AS $function$
DECLARE
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_cutoff timestamptz;
  v_limit integer;
  v_batch_record record;
  v_actor_user_id uuid;
  v_idempotency_key text;
  v_input_json jsonb;
  v_operation_id uuid;
  v_operation_created boolean;
  v_guard jsonb;
  v_operations_json jsonb := '[]'::jsonb;
  v_claimed_count integer := 0;
  v_skipped_count integer := 0;
BEGIN
  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 50 THEN
    RAISE EXCEPTION 'PAY_BATCHES_DUE_LIMIT_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'LIMIT_INVALID',
        'limit', p_limit
      )::text;
  END IF;
  v_limit := p_limit;

  -- current_user is always the definer inside this SECURITY DEFINER function.
  -- Only a genuine direct postgres session may use deterministic test time.
  IF p_now_utc IS NOT NULL AND session_user <> 'postgres' THEN
    RAISE EXCEPTION 'PAY_BATCHES_DUE_CALLER_TIME_OVERRIDE_NOT_ALLOWED'
      USING ERRCODE = '42501', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'CALLER_TIME_OVERRIDE_NOT_ALLOWED'
      )::text;
  END IF;
  v_cutoff := COALESCE(p_now_utc, v_now);

  FOR v_batch_record IN
    SELECT
      batch_row.id,
      batch_row.pay_date,
      batch_row.authoritative_payment_date,
      batch_row.schedule_kind,
      batch_row.scheduled_at_utc,
      batch_row.scheduled_by_user_id,
      batch_row.created_by_user_id,
      batch_row.funding_account_ref,
      batch_row.rail_provider_snapshot,
      batch_row.rail_env_snapshot,
      batch_row.execution_intent_json
    FROM public.pay_batches AS batch_row
    CROSS JOIN LATERAL public.pay_batch_submission_evidence(
      batch_row.id,
      true
    ) AS submission_evidence(evidence_json)
    WHERE batch_row.status = 'SCHEDULED'
      AND COALESCE(
        batch_row.execution_commit_state,
        'NOT_SUBMITTED'
      ) = 'NOT_SUBMITTED'
      AND batch_row.scheduled_at_utc IS NOT NULL
      AND batch_row.scheduled_at_utc <= v_cutoff
      AND batch_row.cancelled_at_utc IS NULL
      AND COALESCE(
        (submission_evidence.evidence_json
          ->> 'has_external_provider_submission')::boolean,
        false
      ) IS NOT TRUE
      AND COALESCE(
        (submission_evidence.evidence_json
          ->> 'has_pending_provider_outcome')::boolean,
        false
      ) IS NOT TRUE
      AND COALESCE(
        (submission_evidence.evidence_json
          ->> 'has_unknown_provider_outcome')::boolean,
        false
      ) IS NOT TRUE
      AND COALESCE(
        (submission_evidence.evidence_json
          ->> 'has_terminal_no_money')::boolean,
        false
      ) IS NOT TRUE
      AND COALESCE(
        (submission_evidence.evidence_json
          ->> 'has_final_paid_or_settled')::boolean,
        false
      ) IS NOT TRUE
      AND EXISTS (
        SELECT 1
        FROM public.pay_batch_candidates AS candidate_row
        JOIN public.pay_batch_items AS item_row
          ON item_row.pay_batch_candidate_id = candidate_row.id
        WHERE candidate_row.pay_batch_id = batch_row.id
          AND COALESCE(item_row.is_voided, false) IS NOT TRUE
          AND item_row.item_type <> 'DEBT_CREATED'
      )
      AND EXISTS (
        SELECT 1
        FROM public.pay_batch_auth_requests AS auth_request
        WHERE auth_request.pay_batch_id = batch_row.id
          AND auth_request.state = 'AUTHORISED'
      )
    ORDER BY batch_row.scheduled_at_utc, batch_row.id
    LIMIT v_limit
  LOOP
    v_guard := private.pay_payment_mutation_guard_v1(
      v_batch_record.id,
      NULL::uuid,
      'NEW_PAYMENT_ACTION'
    );

    IF COALESCE((v_guard->>'blocked')::boolean, true) THEN
      v_skipped_count := v_skipped_count + 1;
      v_operations_json := v_operations_json || pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'pay_batch_id', v_batch_record.id,
          'operation_id', NULL,
          'operation_created', false,
          'code', COALESCE(
            v_guard->>'code',
            'PAYMENT_CHANGE_IN_PROGRESS'
          )
        )
      );
      CONTINUE;
    END IF;

    PERFORM 1
    FROM public.pay_batches AS locked_batch
    WHERE locked_batch.id = v_batch_record.id
      AND locked_batch.status = 'SCHEDULED'
      AND locked_batch.scheduled_at_utc IS NOT NULL
      AND locked_batch.scheduled_at_utc <= v_cutoff
      AND COALESCE(
        locked_batch.execution_commit_state,
        'NOT_SUBMITTED'
      ) = 'NOT_SUBMITTED'
      AND locked_batch.cancelled_at_utc IS NULL
    FOR UPDATE SKIP LOCKED;

    IF NOT FOUND THEN
      v_skipped_count := v_skipped_count + 1;
      CONTINUE;
    END IF;

    IF EXISTS (
      SELECT 1
      FROM public.pay_bank_transfers AS transfer_row
      WHERE transfer_row.pay_batch_id = v_batch_record.id
        AND (
          transfer_row.request_id IS NOT NULL
          OR transfer_row.rail_tx_id IS NOT NULL
          OR transfer_row.status IN (
            'SUBMITTED',
            'PROCESSING',
            'COMPLETED',
            'PAID',
            'SETTLED'
          )
        )
    ) OR EXISTS (
      SELECT 1
      FROM public.pay_bank_transfer_events AS event_row
      WHERE event_row.pay_batch_id = v_batch_record.id
        AND event_row.normalised_state IN (
          'UNKNOWN',
          'PENDING',
          'SUBMITTED',
          'PROCESSING',
          'COMPLETED',
          'PAID',
          'SETTLED'
        )
    ) THEN
      v_skipped_count := v_skipped_count + 1;
      v_operations_json := v_operations_json || pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'pay_batch_id', v_batch_record.id,
          'operation_id', NULL,
          'operation_created', false,
          'code', 'PROVIDER_EVIDENCE_PRESENT'
        )
      );
      CONTINUE;
    END IF;

    v_actor_user_id := COALESCE(
      v_batch_record.scheduled_by_user_id,
      v_batch_record.created_by_user_id
    );
    IF v_actor_user_id IS NULL THEN
      v_skipped_count := v_skipped_count + 1;
      CONTINUE;
    END IF;

    v_idempotency_key :=
      'payment-execute:scheduled:batch:' || v_batch_record.id::text
      || ':scheduled_at:' || v_batch_record.scheduled_at_utc::text;

    v_input_json := pg_catalog.jsonb_strip_nulls(
      pg_catalog.jsonb_build_object(
        'source', 'pay_batches_claim_due_scheduled',
        'pay_batch_id', v_batch_record.id,
        'scheduled_at_utc', v_batch_record.scheduled_at_utc,
        'schedule_kind', v_batch_record.schedule_kind,
        'execution_mode', COALESCE(
          v_batch_record.execution_intent_json->>'execution_mode',
          v_batch_record.execution_intent_json->>'mode',
          'STANDARD_BANK'
        ),
        'payment_date', COALESCE(
          v_batch_record.authoritative_payment_date,
          v_batch_record.pay_date
        ),
        'funding_account_ref', v_batch_record.funding_account_ref,
        'rail_provider_snapshot', v_batch_record.rail_provider_snapshot,
        'rail_env_snapshot', v_batch_record.rail_env_snapshot,
        'initial_phase', 'VALIDATE_BATCH',
        'backend_runner_owned', true,
        'claimed_at_utc', v_now
      )
    );

    SELECT operation_row.operation_id,
           operation_row.is_existing IS NOT TRUE
    INTO v_operation_id,
         v_operation_created
    FROM public.banking_pay_operation_start(
      'PAYMENT_EXECUTE',
      v_actor_user_id,
      v_idempotency_key,
      NULL::uuid,
      v_batch_record.id,
      NULL::uuid,
      v_input_json,
      pg_catalog.jsonb_build_object('run_after_utc', v_now)
    ) AS operation_row
    LIMIT 1;

    UPDATE public.pay_batches AS batch_update
    SET status = 'EXECUTING',
        executing_started_at_utc = COALESCE(
          batch_update.executing_started_at_utc,
          v_now
        ),
        execution_intent_json = pg_catalog.jsonb_strip_nulls(
          COALESCE(
            batch_update.execution_intent_json,
            '{}'::jsonb
          ) || pg_catalog.jsonb_build_object(
            'active_operation_id', v_operation_id,
            'scheduled_claimed_at_utc', v_now
          )
        )
    WHERE batch_update.id = v_batch_record.id
      AND batch_update.status = 'SCHEDULED'
      AND COALESCE(
        batch_update.execution_commit_state,
        'NOT_SUBMITTED'
      ) = 'NOT_SUBMITTED';

    v_operations_json := v_operations_json || pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'pay_batch_id', v_batch_record.id,
        'operation_id', v_operation_id,
        'operation_created', v_operation_created,
        'operation_type', 'PAYMENT_EXECUTE',
        'idempotency_key', v_idempotency_key,
        'scheduled_at_utc', v_batch_record.scheduled_at_utc,
        'actor_user_id', v_actor_user_id,
        'code', CASE
          WHEN v_operation_created THEN 'DUE_OPERATION_STARTED'
          ELSE 'DUE_OPERATION_EXISTING'
        END
      )
    );
    v_claimed_count := v_claimed_count + 1;
  END LOOP;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'error_code', NULL,
    'replacement_rpc', 'banking_pay_operation_start',
    'display_message', CASE
      WHEN v_claimed_count = 0 THEN 'No scheduled payments are due.'
      ELSE 'Due scheduled payments were queued for bounded processing.'
    END,
    'server_utc', v_now,
    'cutoff_utc', v_cutoff,
    'limit', v_limit,
    'claimed_count', v_claimed_count,
    'skipped_count', v_skipped_count,
    'operations', v_operations_json,
    'claimed', v_operations_json,
    'continuations', (
      SELECT COALESCE(pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'required', true,
          'operation_id', operation_item.value->>'operation_id',
          'operation_type', 'PAYMENT_EXECUTE',
          'pay_batch_id', operation_item.value->>'pay_batch_id',
          'root_operation_id', NULL,
          'phase', 'VALIDATE_BATCH',
          'run_after_utc', v_now,
          'reason', 'DUE_SCHEDULE_DISCOVERED',
          'successor_relation', 'SELF',
          'requires_user_action', false,
          'terminal', false
        ) ORDER BY operation_item.ordinality
      ), '[]'::jsonb)
      FROM pg_catalog.jsonb_array_elements(v_operations_json) WITH ORDINALITY AS operation_item(value, ordinality)
      WHERE COALESCE(operation_item.value->>'operation_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    ),
    'code', CASE
      WHEN v_claimed_count = 0 THEN 'NO_DUE_BATCH'
      ELSE 'DUE_OPERATION_STARTED'
    END
  );
END;
$function$;

ALTER FUNCTION public.pay_batches_claim_due_scheduled(integer,timestamptz) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batches_claim_due_scheduled(integer,timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_batches_claim_due_scheduled(integer,timestamptz) FROM anon;
REVOKE ALL ON FUNCTION public.pay_batches_claim_due_scheduled(integer,timestamptz) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_batches_claim_due_scheduled(integer,timestamptz) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_batches_claim_due_scheduled(integer,timestamptz) TO service_role;
