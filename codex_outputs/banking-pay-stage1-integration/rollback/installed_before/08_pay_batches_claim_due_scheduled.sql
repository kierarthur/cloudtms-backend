-- Exact installed TEST rollback definition captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: 8c3923a71727eef52b0b28fea7a16cb5

CREATE OR REPLACE FUNCTION public.pay_batches_claim_due_scheduled(p_limit integer DEFAULT 50, p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_cutoff timestamptz := COALESCE(p_now_utc, now());
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 500);
  v_batch_record record;
  v_operation_id uuid := NULL::uuid;
  v_operation_created boolean := false;
  v_actor_user_id uuid := NULL::uuid;
  v_idempotency_key text := NULL::text;
  v_input_json jsonb := '{}'::jsonb;
  v_summary_json jsonb := '{}'::jsonb;
  v_operations_json jsonb := '[]'::jsonb;
  v_claimed_count integer := 0;
  v_operation_config_json jsonb := '{}'::jsonb;
  v_operation_config_snapshot_status text := 'unknown';
BEGIN
  FOR v_batch_record IN
    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.authoritative_payment_date,
      batch_row.schedule_kind,
      batch_row.scheduled_at_utc,
      batch_row.scheduled_by_user_id,
      batch_row.created_by_user_id,
      batch_row.funding_account_ref,
      batch_row.rail_provider_snapshot,
      batch_row.rail_env_snapshot,
      batch_row.execution_intent_json,
      batch_row.execution_commit_state
    FROM public.pay_batches AS batch_row
    CROSS JOIN LATERAL public.pay_batch_submission_evidence(batch_row.id, true) AS submission_evidence(evidence_json)
    WHERE upper(coalesce(batch_row.status, '')) = 'SCHEDULED'
      AND upper(coalesce(batch_row.execution_commit_state, 'NOT_SUBMITTED')) = 'NOT_SUBMITTED'
      AND batch_row.scheduled_at_utc IS NOT NULL
      AND batch_row.scheduled_at_utc <= v_cutoff
      AND batch_row.cancelled_at_utc IS NULL
      AND COALESCE((submission_evidence.evidence_json->>'has_external_provider_submission')::boolean, false) = false
      AND COALESCE((submission_evidence.evidence_json->>'has_pending_provider_outcome')::boolean, false) = false
      AND COALESCE((submission_evidence.evidence_json->>'has_unknown_provider_outcome')::boolean, false) = false
      AND COALESCE((submission_evidence.evidence_json->>'has_terminal_no_money')::boolean, false) = false
      AND COALESCE((submission_evidence.evidence_json->>'has_final_paid_or_settled')::boolean, false) = false
      AND EXISTS (
        SELECT 1
        FROM public.pay_batch_candidates AS active_due_candidates
        JOIN public.pay_batch_items AS active_due_items
          ON active_due_items.pay_batch_candidate_id = active_due_candidates.id
        WHERE active_due_candidates.pay_batch_id = batch_row.id
          AND COALESCE(active_due_items.is_voided, false) = false
          AND active_due_items.item_type <> 'DEBT_CREATED'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_requests AS open_pre_bank_cancel_requests
        WHERE open_pre_bank_cancel_requests.pay_batch_id = batch_row.id
          AND open_pre_bank_cancel_requests.correction_kind = 'PRE_BANK_CANCEL'
          AND open_pre_bank_cancel_requests.status IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'EXPANDED', 'PROCESSING')
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operations AS provider_submit_operations
        WHERE provider_submit_operations.pay_batch_id = batch_row.id
          AND provider_submit_operations.operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
          AND provider_submit_operations.phase IN ('SUBMIT_PROVIDER_TRANSFERS', 'PROVIDER_SUBMIT', 'CLAIM_PROVIDER_SUBMIT', 'EXECUTE_PROVIDER')
          AND provider_submit_operations.status IN ('QUEUED', 'RUNNING', 'PROCESSING', 'CLAIMED')
      )
    ORDER BY batch_row.scheduled_at_utc ASC, batch_row.id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT v_limit
  LOOP
    v_actor_user_id := COALESCE(v_batch_record.scheduled_by_user_id, v_batch_record.created_by_user_id);
    v_idempotency_key := 'payment-execute:scheduled:batch:' || v_batch_record.id::text || ':scheduled_at:' || COALESCE(v_batch_record.scheduled_at_utc::text, 'none');
    v_input_json := jsonb_strip_nulls(jsonb_build_object(
      'source', 'pay_batches_claim_due_scheduled',
      'pay_batch_id', v_batch_record.id::text,
      'scheduled_at_utc', v_batch_record.scheduled_at_utc::text,
      'schedule_kind', v_batch_record.schedule_kind,
      'execution_mode', COALESCE(v_batch_record.execution_intent_json->>'execution_mode', v_batch_record.execution_intent_json->>'mode', 'STANDARD_BANK'),
      'payment_date', COALESCE(v_batch_record.authoritative_payment_date, v_batch_record.pay_date)::text,
      'funding_account_ref', v_batch_record.funding_account_ref,
      'rail_provider_snapshot', v_batch_record.rail_provider_snapshot,
      'rail_env_snapshot', v_batch_record.rail_env_snapshot,
      'claimed_at_utc', v_now::text
    ));

    WITH operation_config_plan AS (
      SELECT *
      FROM (
        VALUES
          ('validate_freshness', 'PAYMENT_EXECUTE', 'VALIDATE_FRESHNESS', 'FRESHNESS_VALIDATE'),
          ('prepare_transfer_scope', 'PAYMENT_EXECUTE', 'PREPARE_TRANSFER_SCOPE', 'TRANSFER_GROUP'),
          ('prepare_transfer_chunks', 'PAYMENT_EXECUTE', 'PREPARE_TRANSFER_CHUNKS', 'TRANSFER_GROUP'),
          ('submit_provider_transfers', 'PAYMENT_EXECUTE', 'SUBMIT_PROVIDER_TRANSFERS', 'TRANSFER_SUBMIT'),
          ('apply_rail_updates', 'PAYMENT_EXECUTE', 'APPLY_RAIL_UPDATES', 'RAIL_UPDATE'),
          ('settlement_apply_chunks', 'PAYMENT_SETTLEMENT', 'APPLY_SETTLEMENT_CHUNKS', 'SETTLEMENT'),
          ('remittance_queue_chunks', 'REMITTANCE_QUEUE', 'QUEUE_REMITTANCE_CHUNKS', 'REMITTANCE'),
          ('payout_notice_queue_chunks', 'REMITTANCE_QUEUE', 'QUEUE_PAYOUT_NOTICE_CHUNKS', 'PAYOUT_NOTICE')
      ) AS config_plan(config_key, operation_type, phase, chunk_type)
    ), operation_config_rows AS (
      SELECT
        operation_config_plan.config_key,
        operation_config_plan.operation_type,
        operation_config_plan.phase,
        operation_config_plan.chunk_type,
        operation_config_get.chunk_size,
        operation_config_get.min_chunk_size,
        operation_config_get.max_chunk_size,
        operation_config_get.max_advance_ms,
        operation_config_get.lock_seconds
      FROM operation_config_plan
      CROSS JOIN LATERAL public.banking_pay_operation_config_get(
        p_operation_type => operation_config_plan.operation_type,
        p_phase => operation_config_plan.phase,
        p_chunk_type => operation_config_plan.chunk_type
      ) AS operation_config_get
    )
    SELECT jsonb_build_object(
      'source', 'pay_batches_claim_due_scheduled',
      'version', 1,
      'operation_type', 'PAYMENT_EXECUTE',
      'snapshotted_at_utc', v_now::text,
      'lock_seconds', COALESCE(max(operation_config_rows.lock_seconds), 60),
      'max_advance_ms', COALESCE(max(operation_config_rows.max_advance_ms), 15000),
      'chunks', COALESCE(jsonb_object_agg(
        operation_config_rows.config_key,
        jsonb_build_object(
          'operation_type', operation_config_rows.operation_type,
          'phase', operation_config_rows.phase,
          'chunk_type', operation_config_rows.chunk_type,
          'chunk_size', operation_config_rows.chunk_size,
          'min_chunk_size', operation_config_rows.min_chunk_size,
          'max_chunk_size', operation_config_rows.max_chunk_size,
          'max_advance_ms', operation_config_rows.max_advance_ms,
          'lock_seconds', operation_config_rows.lock_seconds
        )
        ORDER BY operation_config_rows.config_key
      ), '{}'::jsonb)
    )
    INTO v_operation_config_json
    FROM operation_config_rows;

    v_operation_config_json := COALESCE(v_operation_config_json, '{}'::jsonb) || jsonb_build_object(
      'freshness_chunk_size', COALESCE(NULLIF(v_operation_config_json #>> '{chunks,validate_freshness,chunk_size}', '')::integer, 50),
      'transfer_scope_chunk_size', COALESCE(NULLIF(v_operation_config_json #>> '{chunks,prepare_transfer_scope,chunk_size}', '')::integer, 100),
      'transfer_prepare_chunk_size', COALESCE(NULLIF(v_operation_config_json #>> '{chunks,prepare_transfer_chunks,chunk_size}', '')::integer, 100),
      'provider_submit_chunk_size', COALESCE(NULLIF(v_operation_config_json #>> '{chunks,submit_provider_transfers,chunk_size}', '')::integer, 50),
      'rail_update_chunk_size', COALESCE(NULLIF(v_operation_config_json #>> '{chunks,apply_rail_updates,chunk_size}', '')::integer, 100),
      'settlement_chunk_size', COALESCE(NULLIF(v_operation_config_json #>> '{chunks,settlement_apply_chunks,chunk_size}', '')::integer, 100),
      'remittance_chunk_size', COALESCE(NULLIF(v_operation_config_json #>> '{chunks,remittance_queue_chunks,chunk_size}', '')::integer, 100),
      'payout_notice_chunk_size', COALESCE(NULLIF(v_operation_config_json #>> '{chunks,payout_notice_queue_chunks,chunk_size}', '')::integer, 100)
    );

    SELECT operation_row.id
    INTO v_operation_id
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.idempotency_key = v_idempotency_key
      AND operation_row.operation_type = 'PAYMENT_EXECUTE'
      AND operation_row.pay_batch_id = v_batch_record.id
    ORDER BY operation_row.created_at_utc DESC NULLS LAST, operation_row.id DESC
    LIMIT 1;

    v_operation_created := false;

    IF v_operation_id IS NULL THEN
      INSERT INTO public.banking_pay_operations (
        id,
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
        created_at_utc,
        started_at_utc,
        updated_at_utc,
        completed_at_utc,
        failed_at_utc
      )
      VALUES (
        gen_random_uuid(),
        'PAYMENT_EXECUTE',
        'QUEUED',
        'VALIDATE_BATCH',
        v_actor_user_id,
        NULL::uuid,
        v_batch_record.id,
        NULL::uuid,
        v_idempotency_key,
        v_input_json,
        v_operation_config_json,
        jsonb_build_object(
          'title', 'Scheduled payment queued',
          'status_text', 'Scheduled payment is ready to continue through the scalable execution operation.',
          'source', 'pay_batches_claim_due_scheduled'
        ),
        NULL::jsonb,
        NULL::jsonb,
        0,
        0,
        0,
        0,
        0,
        NULL::text,
        NULL::timestamptz,
        v_now,
        NULL::timestamptz,
        v_now,
        NULL::timestamptz,
        NULL::timestamptz
      )
      RETURNING id INTO v_operation_id;
      v_operation_created := true;
      v_operation_config_snapshot_status := 'created';
    ELSE
      SELECT CASE
        WHEN operation_existing.config_json IS NULL OR operation_existing.config_json = '{}'::jsonb THEN 'repaired'
        ELSE 'reused'
      END
      INTO v_operation_config_snapshot_status
      FROM public.banking_pay_operations AS operation_existing
      WHERE operation_existing.id = v_operation_id;

      IF v_operation_config_snapshot_status = 'repaired' THEN
        UPDATE public.banking_pay_operations AS operation_update
        SET config_json = v_operation_config_json,
            progress_json = COALESCE(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
              'config_snapshot_status', 'repaired',
              'config_snapshot_repaired_at_utc', v_now::text,
              'config_snapshot_source', 'pay_batches_claim_due_scheduled'
            ),
            updated_at_utc = v_now
        WHERE operation_update.id = v_operation_id
          AND (operation_update.config_json IS NULL OR operation_update.config_json = '{}'::jsonb);
      END IF;
    END IF;

    UPDATE public.pay_batches AS batch_update
    SET status = CASE WHEN upper(coalesce(batch_update.status, '')) = 'SCHEDULED' THEN 'EXECUTING' ELSE batch_update.status END,
        executing_started_at_utc = COALESCE(batch_update.executing_started_at_utc, v_now),
        execution_intent_json = jsonb_strip_nulls(COALESCE(batch_update.execution_intent_json, '{}'::jsonb) || jsonb_build_object(
          'active_operation_id', v_operation_id::text,
          'scheduled_claimed_at_utc', v_now::text
        ))
    WHERE batch_update.id = v_batch_record.id
      AND upper(coalesce(batch_update.execution_commit_state, 'NOT_SUBMITTED')) = 'NOT_SUBMITTED';

    v_summary_json := public.pay_batch_execution_summary_get(v_batch_record.id, v_actor_user_id);

    v_operations_json := v_operations_json || jsonb_build_array(jsonb_build_object(
      'pay_batch_id', v_batch_record.id::text,
      'operation_id', v_operation_id::text,
      'operation_created', v_operation_created,
      'operation_config_snapshot_status', v_operation_config_snapshot_status,
      'operation_type', 'PAYMENT_EXECUTE',
      'idempotency_key', v_idempotency_key,
      'scheduled_at_utc', v_batch_record.scheduled_at_utc,
      'actor_user_id', CASE WHEN v_actor_user_id IS NULL THEN NULL ELSE v_actor_user_id::text END,
      'batch_summary', v_summary_json
    ));

    v_claimed_count := v_claimed_count + 1;
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'server_utc', v_now,
    'cutoff_utc', v_cutoff,
    'limit', v_limit,
    'claimed_count', v_claimed_count,
    'operations', v_operations_json,
    'claimed', v_operations_json
  );
END;
$function$;

ALTER FUNCTION pay_batches_claim_due_scheduled(integer,timestamp with time zone) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_batches_claim_due_scheduled(integer,timestamp with time zone) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_batches_claim_due_scheduled(integer,timestamp with time zone) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pay_batches_claim_due_scheduled(integer,timestamp with time zone) TO postgres;
GRANT EXECUTE ON FUNCTION pay_batches_claim_due_scheduled(integer,timestamp with time zone) TO anon;
GRANT EXECUTE ON FUNCTION pay_batches_claim_due_scheduled(integer,timestamp with time zone) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_batches_claim_due_scheduled(integer,timestamp with time zone) TO service_role;
