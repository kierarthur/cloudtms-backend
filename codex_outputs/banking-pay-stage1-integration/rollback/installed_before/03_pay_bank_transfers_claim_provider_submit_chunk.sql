-- Exact installed TEST rollback definition captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: ab1b312f6857054e9475cfe2c57ff9b6

CREATE OR REPLACE FUNCTION public.pay_bank_transfers_claim_provider_submit_chunk(p_operation_id uuid, p_pay_batch_id uuid, p_limit integer DEFAULT 50, p_lock_owner text DEFAULT NULL::text, p_lock_seconds integer DEFAULT 60)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_limit integer := LEAST(GREATEST(coalesce(p_limit, 25), 1), 25);
  v_lock_seconds integer := LEAST(GREATEST(coalesce(p_lock_seconds, 60), 10), 3600);
  v_lock_owner text := coalesce(nullif(btrim(coalesce(p_lock_owner, '')), ''), 'provider-submit:' || p_operation_id::text);
  v_pay_channel_scope text := 'ALL';
  v_chunk_id uuid := NULL::uuid;
  v_sequence_no integer := 1;
  v_transfer_scope_ids jsonb := '[]'::jsonb;
  v_transfer_rows jsonb := '[]'::jsonb;
  v_auth_request_ids jsonb := '[]'::jsonb;
  v_same_operation_authorised_auth_count integer := 0;
  v_claimed_count integer := 0;
  v_remaining_count integer := 0;
BEGIN
  PERFORM set_config('lock_timeout', '3s', true);
  PERFORM public.banking_pay_hot_path_budget_apply('WORKER_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'pay_bank_transfers_claim_provider_submit_chunk: operation_id is required';
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_bank_transfers_claim_provider_submit_chunk: pay_batch_id is required';
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_operations row % not found', p_operation_id;
  END IF;

  IF upper(btrim(coalesce(v_operation_row.operation_type, ''))) NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION 'operation % is not a payment execution operation', p_operation_id;
  END IF;

  IF v_operation_row.pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id <> p_pay_batch_id THEN
    RAISE EXCEPTION 'operation % is for pay batch %, not %', p_operation_id, v_operation_row.pay_batch_id, p_pay_batch_id;
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_batches row % not found', p_pay_batch_id;
  END IF;

  IF upper(btrim(coalesce(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))) <> 'NOT_SUBMITTED'
     OR nullif(btrim(coalesce(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL
     OR v_batch_row.execution_committed_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_PROVIDER_SUBMIT_CLAIM_EXECUTION_BOUNDARY_CROSSED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_PROVIDER_SUBMIT_CLAIM_EXECUTION_BOUNDARY_CROSSED', 'pay_batch_id', p_pay_batch_id::text, 'operation_id', p_operation_id::text)::text;
  END IF;

  v_pay_channel_scope := upper(btrim(coalesce(
    nullif(btrim(coalesce(v_operation_row.input_json->>'pay_channel_scope', '')), ''),
    nullif(btrim(coalesce(v_operation_row.input_json->>'payChannelScope', '')), ''),
    nullif(btrim(coalesce(v_operation_row.config_json->>'pay_channel_scope', '')), ''),
    nullif(btrim(coalesce(v_operation_row.config_json->>'payChannelScope', '')), ''),
    'ALL'
  )));
  IF v_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'LOANS', 'ALL') THEN
    v_pay_channel_scope := 'ALL';
  END IF;

  SELECT count(*)::integer,
         coalesce(jsonb_agg(to_jsonb(auth_request.id::text) ORDER BY auth_request.created_at_utc DESC NULLS LAST, auth_request.id), '[]'::jsonb)
  INTO v_same_operation_authorised_auth_count,
       v_auth_request_ids
  FROM public.pay_batch_auth_requests AS auth_request
  WHERE auth_request.pay_batch_id = p_pay_batch_id
    AND auth_request.state = 'AUTHORISED'
    AND nullif(btrim(coalesce(auth_request.execution_intent_json->>'operation_id', '')), '') = p_operation_id::text;

  IF coalesce(v_same_operation_authorised_auth_count, 0) <= 0 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'claim_ok', false,
      'claim_blocked', true,
      'claim_blocker_code', 'AUTH_REQUEST_NOT_AUTHORISED_FOR_OPERATION',
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'chunk_id', NULL,
      'transfer_scope_ids', '[]'::jsonb,
      'auth_request_ids', '[]'::jsonb,
      'transfers', '[]'::jsonb,
      'claimed_count', 0,
      'remaining_count', 0,
      'provider_submission_status', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL',
      'review_reason_code', 'AUTH_REQUEST_NOT_AUTHORISED_FOR_OPERATION'
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_claimed_scope;
  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_claimed_scope (
    transfer_scope_id uuid PRIMARY KEY,
    pay_bank_transfer_id uuid NOT NULL,
    claim_ordinal integer NOT NULL
  ) ON COMMIT DROP;

  WITH locked_scope AS (
    SELECT scope_row.id AS transfer_scope_id,
           scope_row.pay_bank_transfer_id,
           scope_row.updated_at_utc
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    JOIN public.pay_bank_transfers AS transfer_row
      ON transfer_row.id = scope_row.pay_bank_transfer_id
     AND transfer_row.pay_batch_id = scope_row.pay_batch_id
     AND transfer_row.pay_channel = scope_row.pay_channel
     AND transfer_row.transfer_group_key = scope_row.transfer_group_key
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id
      AND upper(btrim(coalesce(scope_row.status, ''))) = 'PREPARED'
      AND coalesce(scope_row.provider_submit_ready, false) = true
      AND upper(btrim(coalesce(scope_row.provider_submit_state, ''))) = 'READY'
      AND scope_row.pay_bank_transfer_id IS NOT NULL
      AND upper(btrim(coalesce(transfer_row.status, ''))) = 'PENDING'
      AND nullif(btrim(coalesce(transfer_row.rail_tx_id, '')), '') IS NULL
      AND transfer_row.completed_at_utc IS NULL
      AND nullif(btrim(coalesce(transfer_row.failed_reason, '')), '') IS NULL
      AND (v_pay_channel_scope = 'ALL' OR upper(btrim(coalesce(scope_row.pay_channel, ''))) = v_pay_channel_scope)
    ORDER BY scope_row.updated_at_utc NULLS FIRST, scope_row.id
    LIMIT v_limit
    FOR UPDATE OF scope_row, transfer_row SKIP LOCKED
  ), claimable_scope AS (
    SELECT locked_scope.transfer_scope_id,
           locked_scope.pay_bank_transfer_id,
           row_number() OVER (ORDER BY locked_scope.updated_at_utc NULLS FIRST, locked_scope.transfer_scope_id)::integer AS claim_ordinal
    FROM locked_scope
  )
  INSERT INTO pg_temp.tmp_provider_submit_claimed_scope (transfer_scope_id, pay_bank_transfer_id, claim_ordinal)
  SELECT claimable_scope.transfer_scope_id,
         claimable_scope.pay_bank_transfer_id,
         claimable_scope.claim_ordinal
  FROM claimable_scope;

  SELECT count(*)::integer
  INTO v_claimed_count
  FROM pg_temp.tmp_provider_submit_claimed_scope AS claimed_scope;

  IF coalesce(v_claimed_count, 0) <= 0 THEN
    SELECT count(*)::integer
    INTO v_remaining_count
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id
      AND upper(btrim(coalesce(scope_row.status, ''))) = 'PREPARED'
      AND coalesce(scope_row.provider_submit_ready, false) = true
      AND upper(btrim(coalesce(scope_row.provider_submit_state, ''))) = 'READY'
      AND scope_row.pay_bank_transfer_id IS NOT NULL;

    RETURN jsonb_build_object(
      'ok', false,
      'claim_ok', false,
      'claim_blocked', true,
      'claim_blocker_code', 'PROVIDER_SUBMIT_NO_ELIGIBLE_TRANSFERS',
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'chunk_id', NULL,
      'transfer_scope_ids', '[]'::jsonb,
      'auth_request_ids', coalesce(v_auth_request_ids, '[]'::jsonb),
      'transfers', '[]'::jsonb,
      'claimed_count', 0,
      'remaining_count', coalesce(v_remaining_count, 0),
      'provider_submission_status', 'NO_PROVIDER_SUBMISSION_ATTEMPTED'
    );
  END IF;

  SELECT (count(*) + 1)::integer
  INTO v_sequence_no
  FROM public.banking_pay_operation_chunks AS chunk_row
  WHERE chunk_row.operation_id = p_operation_id
    AND chunk_row.phase = 'SUBMIT_PROVIDER_TRANSFERS'
    AND chunk_row.chunk_type = 'TRANSFER_SUBMIT';

  v_chunk_id := gen_random_uuid();

  SELECT coalesce(jsonb_agg(to_jsonb(claimed_scope.transfer_scope_id::text) ORDER BY claimed_scope.claim_ordinal), '[]'::jsonb),
         coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
           'transfer_scope_id', claimed_scope.transfer_scope_id::text,
           'pay_bank_transfer_id', transfer_row.id::text,
           'pay_batch_id', transfer_row.pay_batch_id::text,
           'payee_entity_kind', transfer_row.payee_entity_kind,
           'payee_entity_id', CASE WHEN transfer_row.payee_entity_id IS NULL THEN NULL ELSE transfer_row.payee_entity_id::text END,
           'candidate_id', CASE WHEN transfer_row.candidate_id IS NULL THEN NULL ELSE transfer_row.candidate_id::text END,
           'umbrella_id', CASE WHEN transfer_row.umbrella_id IS NULL THEN NULL ELSE transfer_row.umbrella_id::text END,
           'pay_channel', transfer_row.pay_channel,
           'amount', transfer_row.amount,
           'currency', coalesce(nullif(btrim(coalesce(transfer_row.currency, '')), ''), 'GBP'),
           'payment_reference', transfer_row.payment_reference,
           'payee_name', transfer_row.payee_name,
           'sort_code', transfer_row.sort_code,
           'account_number', transfer_row.account_number,
           'account_type', transfer_row.account_type,
           'bank_details_hash_snapshot', transfer_row.bank_details_hash_snapshot,
           'request_id', transfer_row.request_id,
           'idempotency_key', coalesce(nullif(btrim(coalesce(transfer_row.request_id, '')), ''), 'transfer:' || transfer_row.id::text),
           'funding_account_ref', v_batch_row.funding_account_ref,
           'rail_provider', transfer_row.rail_provider,
           'rail_env', transfer_row.rail_env,
           'transfer_group_key', transfer_row.transfer_group_key
         )) ORDER BY claimed_scope.claim_ordinal), '[]'::jsonb)
  INTO v_transfer_scope_ids,
       v_transfer_rows
  FROM pg_temp.tmp_provider_submit_claimed_scope AS claimed_scope
  JOIN public.pay_bank_transfers AS transfer_row
    ON transfer_row.id = claimed_scope.pay_bank_transfer_id;

  INSERT INTO public.banking_pay_operation_chunks (
    id,
    operation_id,
    phase,
    chunk_type,
    sequence_no,
    status,
    payload_json,
    result_json,
    error_json,
    unit_count,
    completed_count,
    failed_count,
    locked_by,
    lock_expires_at_utc,
    created_at_utc,
    started_at_utc,
    completed_at_utc,
    updated_at_utc
  )
  VALUES (
    v_chunk_id,
    p_operation_id,
    'SUBMIT_PROVIDER_TRANSFERS',
    'TRANSFER_SUBMIT',
    v_sequence_no,
    'RUNNING',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'transfer_scope_ids', coalesce(v_transfer_scope_ids, '[]'::jsonb),
      'transfers', coalesce(v_transfer_rows, '[]'::jsonb),
      'claimed_at_utc', v_now::text,
      'lock_owner', v_lock_owner,
      'row_backed_provider_queue', true
    ),
    jsonb_build_object('provider_submit_stage', 'CLAIMED'),
    NULL::jsonb,
    coalesce(v_claimed_count, 0),
    0,
    0,
    v_lock_owner,
    v_now + make_interval(secs => v_lock_seconds),
    v_now,
    v_now,
    NULL::timestamptz,
    v_now
  );

  UPDATE public.banking_pay_operation_transfer_scope AS scope_update
  SET provider_submit_state = 'CLAIMED',
      provider_submit_ready = false,
      provider_review_required = false,
      provider_unsafe_reason = NULL::text,
      provider_submit_chunk_id = v_chunk_id,
      provider_submit_claimed_at_utc = v_now,
      provider_submit_attempt_count = COALESCE(scope_update.provider_submit_attempt_count, 0) + 1,
      provider_idempotency_key = coalesce(nullif(btrim(scope_update.provider_idempotency_key), ''), coalesce(nullif(btrim(transfer_row.request_id), ''), 'scope:' || scope_update.id::text)),
      provider_request_id = coalesce(nullif(btrim(scope_update.provider_request_id), ''), transfer_row.request_id),
      updated_at_utc = v_now
  FROM pg_temp.tmp_provider_submit_claimed_scope AS claimed_scope
  JOIN public.pay_bank_transfers AS transfer_row
    ON transfer_row.id = claimed_scope.pay_bank_transfer_id
  WHERE scope_update.id = claimed_scope.transfer_scope_id;

  SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id
      AND upper(btrim(coalesce(scope_row.status, ''))) = 'PREPARED'
      AND coalesce(scope_row.provider_submit_ready, false) = true
      AND upper(btrim(coalesce(scope_row.provider_submit_state, ''))) = 'READY'
      AND scope_row.pay_bank_transfer_id IS NOT NULL
    LIMIT 1
  ) THEN 1 ELSE 0 END
  INTO v_remaining_count;

  RETURN jsonb_build_object(
    'ok', true,
    'claim_ok', true,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'chunk_id', v_chunk_id::text,
    'chunk_type', 'TRANSFER_SUBMIT',
    'phase', 'SUBMIT_PROVIDER_TRANSFERS',
    'sequence_no', v_sequence_no,
    'lock_owner', v_lock_owner,
    'lock_expires_at_utc', (v_now + make_interval(secs => v_lock_seconds))::text,
    'transfer_scope_ids', coalesce(v_transfer_scope_ids, '[]'::jsonb),
    'auth_request_ids', coalesce(v_auth_request_ids, '[]'::jsonb),
    'transfers', coalesce(v_transfer_rows, '[]'::jsonb),
    'unit_count', coalesce(v_claimed_count, 0),
    'claimed_count', coalesce(v_claimed_count, 0),
    'remaining_count', coalesce(v_remaining_count, 0),
    'has_more', coalesce(v_remaining_count, 0) > 0,
    'provider_submission_status', 'NO_PROVIDER_SUBMISSION_ATTEMPTED',
    'manual_resolution_required', false
  );
END;
$function$;

ALTER FUNCTION pay_bank_transfers_claim_provider_submit_chunk(uuid,uuid,integer,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_bank_transfers_claim_provider_submit_chunk(uuid,uuid,integer,text,integer) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_bank_transfers_claim_provider_submit_chunk(uuid,uuid,integer,text,integer) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pay_bank_transfers_claim_provider_submit_chunk(uuid,uuid,integer,text,integer) TO postgres;
GRANT EXECUTE ON FUNCTION pay_bank_transfers_claim_provider_submit_chunk(uuid,uuid,integer,text,integer) TO anon;
GRANT EXECUTE ON FUNCTION pay_bank_transfers_claim_provider_submit_chunk(uuid,uuid,integer,text,integer) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_bank_transfers_claim_provider_submit_chunk(uuid,uuid,integer,text,integer) TO service_role;
