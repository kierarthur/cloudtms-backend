-- Canonical transfer-scope execution functions.
-- PAYE uses one frozen candidate destination for the transfer group. Frozen
-- gross-side deductions remain members of that group for accounting and
-- settlement, but they are not standalone payout instructions.
-- Policy X: every destination and membership decision below is derived only
-- from frozen pay-batch candidates/items; no live candidate or finance lookup.

CREATE OR REPLACE FUNCTION public.pay_execute_bank_transfer_scope_seed(p_operation_id uuid, p_pay_batch_id uuid, p_pay_channel_scope text, p_actor_user_id uuid, p_retry_blocked_funds boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_scope text := upper(btrim(coalesce(p_pay_channel_scope, 'ALL')));
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_limit integer := 100;
  v_last_pay_batch_item_id uuid := NULL::uuid;
  v_page_count integer := 0;
  v_group_seeded_count integer := 0;
  v_has_more boolean := false;
  v_next_cursor jsonb := NULL::jsonb;
  v_cached_scope_count integer := 0;
  v_existing_scope_count integer := 0;
  v_unseeded_transfer_scope_count integer := 0;
  v_next_transfer_scope_id uuid := NULL::uuid;
  v_review_scope_count integer := 0;
  v_operation_phase text := NULL::text;
  v_transfer_scope_group_seed_complete boolean := false;
  v_membership_seed_required boolean := false;
  v_next_required_phase text := 'TRANSFER_SCOPE_ITEM_MEMBERSHIP_SEED_PAGE';
  v_global_projection_missing_paye_count integer := 0;
  v_global_projection_explicit_zero_count integer := 0;
  v_global_projection_positive_payment_count integer := 0;
  v_global_projection_positive_payment_total numeric(14,2) := 0;
  v_global_projection_invalid_payment_row_count integer := 0;
  v_global_projection_paye_net_state_hash text := NULL::text;
  v_global_projection_bank_payment_hash text := NULL::text;
  v_scoped_projection_missing_paye_count integer := 0;
  v_scoped_projection_explicit_zero_count integer := 0;
  v_scoped_projection_positive_payment_count integer := 0;
  v_scoped_projection_positive_payment_total numeric(14,2) := 0;
  v_scoped_projection_invalid_payment_row_count integer := 0;
  v_scoped_projection_paye_net_state_hash text := NULL::text;
  v_scoped_projection_bank_payment_hash text := NULL::text;
  v_allow_explicit_zero_no_bank_scopes boolean := false;
  v_scoped_no_transfer_execution boolean := false;
  v_no_bank_payment_execution boolean := false;
  v_no_bank_proof_json jsonb := '{}'::jsonb;
BEGIN
  PERFORM set_config('lock_timeout', '3s', true);
  PERFORM public.banking_pay_hot_path_budget_apply('WORKER_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_OPERATION_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_OPERATION_ID_REQUIRED')::text;
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_BATCH_ID_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_ACTOR_REQUIRED')::text;
  END IF;

  IF v_scope NOT IN ('PAYE', 'UMBRELLA', 'ALL', 'LOANS') THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_SCOPE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_SCOPE_INVALID', 'pay_channel_scope', p_pay_channel_scope)::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_OPERATION_NOT_FOUND', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF upper(btrim(coalesce(v_operation_row.operation_type, ''))) NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_OPERATION_TYPE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_OPERATION_TYPE_INVALID', 'operation_id', p_operation_id::text, 'operation_type', v_operation_row.operation_type)::text;
  END IF;

  IF v_operation_row.pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id <> p_pay_batch_id THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_OPERATION_BATCH_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_OPERATION_BATCH_MISMATCH', 'operation_id', p_operation_id::text, 'operation_pay_batch_id', v_operation_row.pay_batch_id::text, 'pay_batch_id', p_pay_batch_id::text)::text;
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_BATCH_NOT_FOUND', 'pay_batch_id', p_pay_batch_id::text)::text;
  END IF;

  IF upper(btrim(coalesce(v_batch_row.freshness_validation_status, ''))) <> 'PASSED'
     OR nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '') IS NULL
     OR coalesce((v_batch_row.freshness_result_json->>'is_stale')::boolean, false) IS TRUE THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_FRESHNESS_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object(
        'code', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_FRESHNESS_REQUIRED',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'freshness_validation_status', v_batch_row.freshness_validation_status,
        'freshness_result_hash_present', nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '') IS NOT NULL
      )::text;
  END IF;

  IF upper(btrim(coalesce(v_batch_row.status, ''))) = 'BLOCKED_FUNDS' AND coalesce(p_retry_blocked_funds, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_BLOCKED_FUNDS_RETRY_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_BLOCKED_FUNDS_RETRY_REQUIRED', 'pay_batch_id', p_pay_batch_id::text)::text;
  END IF;

  WITH global_projection_rows AS MATERIALIZED (
    SELECT projection_row.*
    FROM public._pay_batch_bank_payment_projection_rows(
      p_pay_batch_id,
      'ALL'
    ) AS projection_row
  ), global_projection_summary AS (
    SELECT
      COUNT(*) FILTER (
        WHERE projection_row.is_paye_net_state_row
          AND projection_row.paye_net_classification = 'MISSING'
      )::integer AS missing_paye_count,
      COUNT(*) FILTER (
        WHERE projection_row.is_paye_net_state_row
          AND projection_row.paye_net_classification = 'ZERO'
      )::integer AS explicit_zero_count,
      COUNT(*) FILTER (WHERE projection_row.is_positive_bank_payment)::integer AS positive_payment_count,
      ROUND(
        COALESCE(
          SUM(projection_row.amount) FILTER (WHERE projection_row.is_positive_bank_payment),
          0
        ),
        2
      )::numeric(14,2) AS positive_payment_total,
      COUNT(*) FILTER (
        WHERE (
          projection_row.paye_net_required IS TRUE
          AND COALESCE(projection_row.paye_net_classification, '') NOT IN ('MISSING', 'ZERO', 'POSITIVE')
        )
        OR (
          COALESCE(projection_row.paye_net_required, false) IS FALSE
          AND (
            projection_row.final_frozen_bank_amount IS NULL
            OR ROUND(projection_row.final_frozen_bank_amount, 2) <= 0
          )
        )
      )::integer AS invalid_payment_row_count,
      MAX(projection_row.paye_net_state_hash) AS paye_net_state_hash,
      MAX(projection_row.bank_payment_projection_hash) AS bank_payment_projection_hash
    FROM global_projection_rows AS projection_row
  ), scoped_projection_rows AS MATERIALIZED (
    SELECT projection_row.*
    FROM public._pay_batch_bank_payment_projection_rows(
      p_pay_batch_id,
      v_scope
    ) AS projection_row
  ), scoped_projection_summary AS (
    SELECT
      COUNT(*) FILTER (
        WHERE projection_row.is_paye_net_state_row
          AND projection_row.paye_net_classification = 'MISSING'
      )::integer AS missing_paye_count,
      COUNT(*) FILTER (
        WHERE projection_row.is_paye_net_state_row
          AND projection_row.paye_net_classification = 'ZERO'
      )::integer AS explicit_zero_count,
      COUNT(*) FILTER (WHERE projection_row.is_positive_bank_payment)::integer AS positive_payment_count,
      ROUND(
        COALESCE(
          SUM(projection_row.amount) FILTER (WHERE projection_row.is_positive_bank_payment),
          0
        ),
        2
      )::numeric(14,2) AS positive_payment_total,
      COUNT(*) FILTER (
        WHERE (
          projection_row.paye_net_required IS TRUE
          AND COALESCE(projection_row.paye_net_classification, '') NOT IN ('MISSING', 'ZERO', 'POSITIVE')
        )
        OR (
          COALESCE(projection_row.paye_net_required, false) IS FALSE
          AND (
            projection_row.final_frozen_bank_amount IS NULL
            OR ROUND(projection_row.final_frozen_bank_amount, 2) <= 0
          )
        )
      )::integer AS invalid_payment_row_count,
      MAX(projection_row.paye_net_state_hash) AS paye_net_state_hash,
      MAX(projection_row.bank_payment_projection_hash) AS bank_payment_projection_hash
    FROM scoped_projection_rows AS projection_row
  )
  SELECT
    COALESCE(global_projection_summary.missing_paye_count, 0),
    COALESCE(global_projection_summary.explicit_zero_count, 0),
    COALESCE(global_projection_summary.positive_payment_count, 0),
    COALESCE(global_projection_summary.positive_payment_total, 0),
    COALESCE(global_projection_summary.invalid_payment_row_count, 0),
    COALESCE(
      global_projection_summary.paye_net_state_hash,
      MD5(JSONB_BUILD_OBJECT(
        'pay_batch_id', p_pay_batch_id::text,
        'scope', 'ALL',
        'rows', '[]'::jsonb
      )::text)
    ),
    COALESCE(
      global_projection_summary.bank_payment_projection_hash,
      MD5(JSONB_BUILD_OBJECT(
        'pay_batch_id', p_pay_batch_id::text,
        'scope', 'ALL',
        'rows', '[]'::jsonb
      )::text)
    ),
    COALESCE(scoped_projection_summary.missing_paye_count, 0),
    COALESCE(scoped_projection_summary.explicit_zero_count, 0),
    COALESCE(scoped_projection_summary.positive_payment_count, 0),
    COALESCE(scoped_projection_summary.positive_payment_total, 0),
    COALESCE(scoped_projection_summary.invalid_payment_row_count, 0),
    COALESCE(
      scoped_projection_summary.paye_net_state_hash,
      MD5(JSONB_BUILD_OBJECT(
        'pay_batch_id', p_pay_batch_id::text,
        'scope', v_scope,
        'rows', '[]'::jsonb
      )::text)
    ),
    COALESCE(
      scoped_projection_summary.bank_payment_projection_hash,
      MD5(JSONB_BUILD_OBJECT(
        'pay_batch_id', p_pay_batch_id::text,
        'scope', v_scope,
        'rows', '[]'::jsonb
      )::text)
    )
  INTO
    v_global_projection_missing_paye_count,
    v_global_projection_explicit_zero_count,
    v_global_projection_positive_payment_count,
    v_global_projection_positive_payment_total,
    v_global_projection_invalid_payment_row_count,
    v_global_projection_paye_net_state_hash,
    v_global_projection_bank_payment_hash,
    v_scoped_projection_missing_paye_count,
    v_scoped_projection_explicit_zero_count,
    v_scoped_projection_positive_payment_count,
    v_scoped_projection_positive_payment_total,
    v_scoped_projection_invalid_payment_row_count,
    v_scoped_projection_paye_net_state_hash,
    v_scoped_projection_bank_payment_hash
  FROM global_projection_summary
  CROSS JOIN scoped_projection_summary;

  IF COALESCE(v_global_projection_missing_paye_count, 0) > 0 THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_SEED_PAYE_NET_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = JSONB_BUILD_OBJECT(
        'code', 'PAYE_NET_REQUIRED_FOR_EXECUTION',
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'pay_channel_scope', v_scope,
        'missing_explicit_paye_input_count', COALESCE(v_global_projection_missing_paye_count, 0)
      )::text;
  END IF;

  v_allow_explicit_zero_no_bank_scopes := (
    COALESCE(v_global_projection_missing_paye_count, 0) = 0
    AND COALESCE(v_scoped_projection_explicit_zero_count, 0) > 0
    AND COALESCE(v_scoped_projection_invalid_payment_row_count, 0) = 0
  );

  v_scoped_no_transfer_execution := (
    v_allow_explicit_zero_no_bank_scopes
    AND COALESCE(v_scoped_projection_positive_payment_count, 0) = 0
  );

  v_no_bank_payment_execution := (
    v_scoped_no_transfer_execution
    AND COALESCE(v_global_projection_positive_payment_count, 0) = 0
    AND COALESCE(v_global_projection_invalid_payment_row_count, 0) = 0
  );

  v_no_bank_proof_json := JSONB_BUILD_OBJECT(
    'proof_source', 'PAY_EXECUTE_BANK_TRANSFER_SCOPE_SEED',
    'proof_generated_at_utc', v_now::text,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'pay_channel_scope', v_scope,
    'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
    'no_bank_payment_execution', v_no_bank_payment_execution,
    'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes,
    'missing_explicit_paye_input_count', COALESCE(v_global_projection_missing_paye_count, 0),
    'global_missing_explicit_paye_input_count', COALESCE(v_global_projection_missing_paye_count, 0),
    'global_explicit_zero_count', COALESCE(v_global_projection_explicit_zero_count, 0),
    'global_positive_bank_payment_count', COALESCE(v_global_projection_positive_payment_count, 0),
    'global_positive_bank_payment_total', ROUND(COALESCE(v_global_projection_positive_payment_total, 0), 2),
    'global_invalid_payment_row_count', COALESCE(v_global_projection_invalid_payment_row_count, 0),
    'scoped_missing_explicit_paye_input_count', COALESCE(v_scoped_projection_missing_paye_count, 0),
    'scoped_explicit_zero_count', COALESCE(v_scoped_projection_explicit_zero_count, 0),
    'scoped_positive_bank_payment_count', COALESCE(v_scoped_projection_positive_payment_count, 0),
    'scoped_positive_bank_payment_total', ROUND(COALESCE(v_scoped_projection_positive_payment_total, 0), 2),
    'scoped_invalid_payment_row_count', COALESCE(v_scoped_projection_invalid_payment_row_count, 0),
    'paye_net_state_hash', v_global_projection_paye_net_state_hash,
    'global_paye_net_state_hash', v_global_projection_paye_net_state_hash,
    'global_bank_payment_projection_hash', v_global_projection_bank_payment_hash,
    'scoped_paye_net_state_hash', v_scoped_projection_paye_net_state_hash,
    'bank_payment_projection_hash', v_scoped_projection_bank_payment_hash,
    'scoped_bank_payment_projection_hash', v_scoped_projection_bank_payment_hash
  );

  v_operation_phase := upper(btrim(coalesce(v_operation_row.phase, '')));
  v_transfer_scope_group_seed_complete :=
    v_operation_phase IN (
      'SEED_TRANSFER_SCOPE_ITEMS',
      'TRANSFER_SCOPE_ITEM_MEMBERSHIP_SEED_PAGE',
      'SEED_TRANSFER_ROLLUP_CHUNKS',
      'ROLLUP_TRANSFER_SCOPE_ITEMS',
      'TRANSFER_SCOPE_ROLLUP_PAGE',
      'PREPARE_TRANSFER_CHUNKS',
      'TRANSFER_CHUNK_PREPARE_PAGE',
      'PREPARE_BATCH_PROOF',
      'PREPARE_BATCH',
      'START_AUTHORISATION_PROOF',
      'START_AUTHORISATION',
      'SUBMIT_PROVIDER_TRANSFERS',
      'SEND_PROVIDER_CHUNK',
      'REQUEST_PROVIDER_SEND',
      'FINALISE_PROVIDER_CHUNK',
      'APPLY_RAIL_UPDATES',
      'QUEUE_REMITTANCES',
      'COMPLETE'
    )
    OR upper(btrim(coalesce(v_operation_row.progress_json->>'transfer_scope_group_seed_complete', ''))) = 'TRUE'
    OR (
      v_operation_row.progress_json ? 'has_more_transfer_groups'
      AND upper(btrim(coalesce(v_operation_row.progress_json->>'has_more_transfer_groups', 'false'))) <> 'TRUE'
      AND v_operation_row.progress_json ? 'last_transfer_scope_seed_at_utc'
    );

  IF coalesce(v_transfer_scope_group_seed_complete, false) THEN
    SELECT count(*)::integer
    INTO v_existing_scope_count
    FROM public.banking_pay_operation_transfer_scope AS existing_scope
    WHERE existing_scope.operation_id = p_operation_id
      AND existing_scope.pay_batch_id = p_pay_batch_id;

    SELECT count(*)::integer
    INTO v_unseeded_transfer_scope_count
    FROM public.banking_pay_operation_transfer_scope AS unseeded_scope
    WHERE unseeded_scope.operation_id = p_operation_id
      AND unseeded_scope.pay_batch_id = p_pay_batch_id
      AND upper(btrim(coalesce(v_operation_row.progress_json #>> ARRAY['transfer_scope_item_seed_proofs', unseeded_scope.id::text, 'seed_complete'], 'false'))) <> 'TRUE'
      AND upper(btrim(coalesce(unseeded_scope.provider_submit_state, 'NOT_READY'))) NOT IN (
        'CLAIMED',
        'REQUEST_PREPARING',
        'REQUEST_SENDING',
        'REQUEST_SENT_LOCAL',
        'PROVIDER_ACCEPTED',
        'PROVIDER_REJECTED',
        'PROVIDER_UNKNOWN',
        'CHUNK_FINALISED'
      );

    SELECT next_scope.id
    INTO v_next_transfer_scope_id
    FROM public.banking_pay_operation_transfer_scope AS next_scope
    WHERE next_scope.operation_id = p_operation_id
      AND next_scope.pay_batch_id = p_pay_batch_id
      AND upper(btrim(coalesce(v_operation_row.progress_json #>> ARRAY['transfer_scope_item_seed_proofs', next_scope.id::text, 'seed_complete'], 'false'))) <> 'TRUE'
      AND upper(btrim(coalesce(next_scope.provider_submit_state, 'NOT_READY'))) NOT IN (
        'CLAIMED',
        'REQUEST_PREPARING',
        'REQUEST_SENDING',
        'REQUEST_SENT_LOCAL',
        'PROVIDER_ACCEPTED',
        'PROVIDER_REJECTED',
        'PROVIDER_UNKNOWN',
        'CHUNK_FINALISED'
      )
    ORDER BY next_scope.created_at_utc NULLS FIRST, next_scope.id
    LIMIT 1;

    SELECT count(*)::integer
    INTO v_review_scope_count
    FROM public.banking_pay_operation_transfer_scope AS review_scope
    WHERE review_scope.operation_id = p_operation_id
      AND review_scope.pay_batch_id = p_pay_batch_id
      AND (
        coalesce(review_scope.provider_review_required, false) IS TRUE
        OR upper(btrim(coalesce(review_scope.provider_submit_state, ''))) = 'REVIEW_REQUIRED'
      );

    IF coalesce(v_existing_scope_count, 0) <= 0 THEN
      IF v_scoped_no_transfer_execution THEN
        UPDATE public.banking_pay_operations AS operation_update
        SET pay_batch_id = p_pay_batch_id,
            status = CASE
              WHEN UPPER(BTRIM(COALESCE(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN 'RUNNING'
              ELSE operation_update.status
            END,
            phase = 'START_AUTHORISATION_PROOF',
            runner_state = 'RUNNABLE',
            requires_user_action = false,
            resume_reason = CASE WHEN v_no_bank_payment_execution THEN 'NO_BANK_PAYMENT_AUTHORISATION_REQUIRED' ELSE 'NO_TRANSFER_SCOPE_AUTHORISATION_REQUIRED' END,
            run_after_utc = v_now,
            progress_json = JSONB_STRIP_NULLS(
              COALESCE(operation_update.progress_json, '{}'::jsonb)
              || JSONB_BUILD_OBJECT(
                'last_transfer_scope_seed_at_utc', v_now::text,
                'last_transfer_scope_seed_phase', 'TRANSFER_GROUP_SEED_PAGE',
                'transfer_scope_group_seed_complete', true,
                'transfer_scope_count', 0,
                'unseeded_transfer_scope_count', 0,
                'membership_seed_required', false,
                'no_transfer_scope', true,
                'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
                'no_bank_payment_execution', v_no_bank_payment_execution,
                'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes,
                'review_required', false,
                'review_reason_code', NULL::text,
                'has_more_transfer_groups', false,
                'transfer_scope_seed_cursor', NULL::jsonb,
                'next_required_phase', 'START_AUTHORISATION_PROOF'
              )
              || v_no_bank_proof_json
              || JSONB_BUILD_OBJECT('no_bank_payment_proof', v_no_bank_proof_json)
            ),
            error_json = CASE
              WHEN UPPER(BTRIM(COALESCE(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN NULL::jsonb
              ELSE operation_update.error_json
            END,
            updated_at_utc = v_now
        WHERE operation_update.id = p_operation_id;

        RETURN JSONB_BUILD_OBJECT(
          'ok', true,
          'code', CASE WHEN v_no_bank_payment_execution THEN 'NO_BANK_PAYMENT_EXECUTION_READY' ELSE 'NO_TRANSFER_EXECUTION_SCOPE_READY' END,
          'message', CASE WHEN v_no_bank_payment_execution THEN 'All required bank-payment groups are explicit zero; no transfer scope is required and authorisation can continue.' ELSE 'The requested scope contains only explicit-zero PAYE payment groups; no transfer scope is required for this scope and authorisation can continue.' END,
          'operation_id', p_operation_id::text,
          'pay_batch_id', p_pay_batch_id::text,
          'pay_channel_scope', v_scope,
          'bounded', true,
          'limit', v_limit,
          'phase_completed', 'TRANSFER_GROUP_SEED_PAGE',
          'source_item_page_count', 0,
          'group_seeded_count', 0,
          'membership_seeded_count', 0,
          'transfer_scope_count', 0,
          'unseeded_transfer_scope_count', 0,
          'review_scope_count', 0,
          'membership_seed_required', false,
          'no_transfer_scope', true,
          'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
          'no_bank_payment_execution', v_no_bank_payment_execution,
          'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes,
          'missing_explicit_paye_input_count', COALESCE(v_global_projection_missing_paye_count, 0),
          'global_explicit_zero_count', COALESCE(v_global_projection_explicit_zero_count, 0),
          'global_positive_bank_payment_count', COALESCE(v_global_projection_positive_payment_count, 0),
          'global_invalid_payment_row_count', COALESCE(v_global_projection_invalid_payment_row_count, 0),
          'scoped_missing_explicit_paye_input_count', COALESCE(v_scoped_projection_missing_paye_count, 0),
          'scoped_explicit_zero_count', COALESCE(v_scoped_projection_explicit_zero_count, 0),
          'scoped_positive_bank_payment_count', COALESCE(v_scoped_projection_positive_payment_count, 0),
          'scoped_positive_bank_payment_total', ROUND(COALESCE(v_scoped_projection_positive_payment_total, 0), 2),
          'scoped_invalid_payment_row_count', COALESCE(v_scoped_projection_invalid_payment_row_count, 0),
          'paye_net_state_hash', v_global_projection_paye_net_state_hash,
          'global_bank_payment_projection_hash', v_global_projection_bank_payment_hash,
          'scoped_paye_net_state_hash', v_scoped_projection_paye_net_state_hash,
          'bank_payment_projection_hash', v_scoped_projection_bank_payment_hash,
          'has_more_transfer_groups', false,
          'has_more_membership_seed', false,
          'next_transfer_scope_id', NULL::text,
          'next_cursor', NULL::jsonb,
          'next_required_phase', 'START_AUTHORISATION_PROOF',
          'server_utc', v_now::text
        );
      END IF;

      UPDATE public.banking_pay_operations AS operation_update
      SET pay_batch_id = p_pay_batch_id,
          phase = 'REVIEW_REQUIRED',
          progress_json = jsonb_strip_nulls(
            coalesce(operation_update.progress_json, '{}'::jsonb)
            || jsonb_build_object(
              'last_transfer_scope_seed_at_utc', v_now::text,
              'last_transfer_scope_seed_phase', 'TRANSFER_GROUP_SEED_PAGE',
              'transfer_scope_group_seed_complete', true,
              'transfer_scope_count', 0,
              'unseeded_transfer_scope_count', 0,
              'membership_seed_required', false,
              'no_transfer_scope', true,
              'next_required_phase', 'REVIEW_REQUIRED'
            )
          ),
          updated_at_utc = v_now
      WHERE operation_update.id = p_operation_id;

      RETURN jsonb_build_object(
        'ok', false,
        'code', 'NO_ELIGIBLE_TRANSFER_SCOPE',
        'message', 'No eligible payable transfer scope was available for this operation.',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text,
        'pay_channel_scope', v_scope,
        'bounded', true,
        'limit', v_limit,
        'phase_completed', 'TRANSFER_GROUP_SEED_PAGE',
        'source_item_page_count', 0,
        'group_seeded_count', 0,
        'membership_seeded_count', 0,
        'transfer_scope_count', 0,
        'unseeded_transfer_scope_count', 0,
        'review_scope_count', coalesce(v_review_scope_count, 0),
        'membership_seed_required', false,
        'no_transfer_scope', true,
        'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
        'no_bank_payment_execution', v_no_bank_payment_execution,
        'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes,
        'global_invalid_payment_row_count', COALESCE(v_global_projection_invalid_payment_row_count, 0),
        'scoped_invalid_payment_row_count', COALESCE(v_scoped_projection_invalid_payment_row_count, 0),
        'has_more_transfer_groups', false,
        'has_more_membership_seed', false,
        'next_transfer_scope_id', NULL::text,
        'next_cursor', NULL::jsonb,
        'next_required_phase', 'REVIEW_REQUIRED',
        'server_utc', v_now::text
      );
    END IF;

    v_membership_seed_required := coalesce(v_unseeded_transfer_scope_count, 0) > 0;
    v_next_required_phase := CASE WHEN coalesce(v_membership_seed_required, false) THEN 'SEED_TRANSFER_SCOPE_ITEMS' ELSE 'SEED_TRANSFER_ROLLUP_CHUNKS' END;

    UPDATE public.banking_pay_operations AS operation_update
    SET pay_batch_id = p_pay_batch_id,
        phase = v_next_required_phase,
        runner_state = CASE WHEN upper(btrim(coalesce(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN operation_update.runner_state ELSE 'RUNNABLE' END,
        run_after_utc = CASE WHEN upper(btrim(coalesce(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN operation_update.run_after_utc ELSE v_now END,
        progress_json = jsonb_strip_nulls(
          coalesce(operation_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'last_transfer_scope_seed_at_utc', v_now::text,
            'last_transfer_scope_seed_phase', 'TRANSFER_GROUP_SEED_PAGE',
            'transfer_scope_group_seed_complete', true,
            'transfer_scope_count', coalesce(v_existing_scope_count, 0),
            'unseeded_transfer_scope_count', coalesce(v_unseeded_transfer_scope_count, 0),
            'membership_seed_required', coalesce(v_membership_seed_required, false),
            'next_transfer_scope_id', CASE WHEN v_next_transfer_scope_id IS NULL THEN NULL ELSE v_next_transfer_scope_id::text END,
            'next_required_phase', v_next_required_phase,
            'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
            'no_bank_payment_execution', v_no_bank_payment_execution,
            'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes
          )
          || v_no_bank_proof_json
          || JSONB_BUILD_OBJECT('no_bank_payment_proof', v_no_bank_proof_json)
        ),
        updated_at_utc = v_now
    WHERE operation_update.id = p_operation_id;

    RETURN jsonb_build_object(
      'ok', true,
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'pay_channel_scope', v_scope,
      'bounded', true,
      'limit', v_limit,
      'phase_completed', 'TRANSFER_GROUP_SEED_PAGE',
      'source_item_page_count', 0,
      'group_seeded_count', 0,
      'membership_seeded_count', 0,
      'transfer_scope_count', coalesce(v_existing_scope_count, 0),
      'unseeded_transfer_scope_count', coalesce(v_unseeded_transfer_scope_count, 0),
      'review_scope_count', coalesce(v_review_scope_count, 0),
      'membership_seed_required', coalesce(v_membership_seed_required, false),
      'no_transfer_scope', false,
      'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
      'no_bank_payment_execution', v_no_bank_payment_execution,
      'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes,
      'global_missing_explicit_paye_input_count', COALESCE(v_global_projection_missing_paye_count, 0),
      'global_explicit_zero_count', COALESCE(v_global_projection_explicit_zero_count, 0),
      'global_positive_bank_payment_count', COALESCE(v_global_projection_positive_payment_count, 0),
      'global_invalid_payment_row_count', COALESCE(v_global_projection_invalid_payment_row_count, 0),
      'scoped_missing_explicit_paye_input_count', COALESCE(v_scoped_projection_missing_paye_count, 0),
      'scoped_explicit_zero_count', COALESCE(v_scoped_projection_explicit_zero_count, 0),
      'scoped_positive_bank_payment_count', COALESCE(v_scoped_projection_positive_payment_count, 0),
      'scoped_invalid_payment_row_count', COALESCE(v_scoped_projection_invalid_payment_row_count, 0),
      'paye_net_state_hash', v_global_projection_paye_net_state_hash,
      'global_bank_payment_projection_hash', v_global_projection_bank_payment_hash,
      'scoped_paye_net_state_hash', v_scoped_projection_paye_net_state_hash,
      'bank_payment_projection_hash', v_scoped_projection_bank_payment_hash,
      'has_more_transfer_groups', false,
      'has_more_membership_seed', coalesce(v_membership_seed_required, false),
      'next_transfer_scope_id', CASE WHEN v_next_transfer_scope_id IS NULL THEN NULL ELSE v_next_transfer_scope_id::text END,
      'next_cursor', NULL::jsonb,
      'next_required_phase', v_next_required_phase,
      'server_utc', v_now::text
    );
  END IF;

  IF nullif(btrim(coalesce(v_operation_row.progress_json #>> '{transfer_scope_seed_cursor,last_pay_batch_item_id}', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_last_pay_batch_item_id := (v_operation_row.progress_json #>> '{transfer_scope_seed_cursor,last_pay_batch_item_id}')::uuid;
  END IF;

  IF nullif(btrim(coalesce(v_operation_row.progress_json->>'transfer_scope_count', '')), '') ~ '^[0-9]+$' THEN
    v_cached_scope_count := (v_operation_row.progress_json->>'transfer_scope_count')::integer;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_bank_transfer_scope_source_page;
  CREATE TEMPORARY TABLE pg_temp.tmp_bank_transfer_scope_source_page ON COMMIT DROP AS
  WITH source_rows AS (
    SELECT
      row_number() OVER (ORDER BY batch_item.id)::integer AS page_ordinal,
      batch_item.id AS pay_batch_item_id,
      batch_candidate.id AS pay_batch_candidate_id,
      batch_candidate.candidate_id,
      CASE WHEN upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) = 'LOANS' THEN 'PAYE' ELSE batch_item.pay_channel END AS pay_channel,
      batch_item.umbrella_id,
      COALESCE(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0)::numeric AS item_amount,
      batch_candidate.net_bank_amount,
      effective_paye_input.id AS effective_paye_net_input_id,
      effective_paye_input.net_amount AS effective_paye_net_input_amount,
      (
        effective_paye_input.id IS NOT NULL
        AND effective_paye_input.net_amount IS NOT NULL
      ) AS has_effective_paye_input,
      batch_item.payout_instruction_snapshot_json,
      upper(nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_kind', '')), '')) AS payee_entity_kind,
      CASE
        WHEN nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (batch_item.payout_instruction_snapshot_json->>'payee_entity_id')::uuid
        ELSE NULL::uuid
      END AS payee_entity_id,
      nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'beneficiary_name', '')), '') AS payee_name,
      nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'sort_code', '')), '') AS sort_code,
      nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_number', '')), '') AS account_number,
      nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_type', '')), '') AS account_type,
      nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), '') AS bank_details_hash_snapshot,
      CASE WHEN coalesce(batch_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$' THEN (batch_item.payout_instruction_snapshot_json->>'week_ending_bucket')::date ELSE NULL::date END AS week_ending_bucket,
      CASE
        WHEN CASE WHEN upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) = 'LOANS' THEN 'PAYE' ELSE batch_item.pay_channel END = 'PAYE'
          THEN batch_candidate.candidate_id::text
               || '|PAYE|'
               || COALESCE(
                    NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''),
                    (
                      SELECT NULLIF(
                               BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'bank_details_hash', '')),
                               ''
                             )
                      FROM public.pay_batch_items AS fallback_item
                      WHERE fallback_item.pay_batch_candidate_id = batch_candidate.id
                        AND COALESCE(fallback_item.is_voided, false) = false
                        AND fallback_item.item_type <> 'DEBT_CREATED'
                        AND fallback_item.pay_channel = 'PAYE'
                        AND NULLIF(
                              BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'bank_details_hash', '')),
                              ''
                            ) IS NOT NULL
                      ORDER BY
                        CASE
                          WHEN NULLIF(
                                 BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'sort_code', '')),
                                 ''
                               ) IS NOT NULL
                           AND NULLIF(
                                 BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'account_number', '')),
                                 ''
                               ) IS NOT NULL
                            THEN 0
                          ELSE 1
                        END,
                        fallback_item.id
                      LIMIT 1
                    ),
                    ''
                  )
        WHEN CASE WHEN upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) = 'LOANS' THEN 'PAYE' ELSE batch_item.pay_channel END = 'UMBRELLA'
          THEN batch_candidate.candidate_id::text || '|UMBRELLA|' || coalesce(CASE WHEN coalesce(batch_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$' THEN (batch_item.payout_instruction_snapshot_json->>'week_ending_bucket') ELSE 'NO_WEEK' END, 'NO_WEEK') || '|' || coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '') || '|' || coalesce(nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''), '')
        ELSE batch_candidate.candidate_id::text || '|OTHER|' || coalesce(nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''), '')
      END AS transfer_group_key
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
    LEFT JOIN LATERAL (
      SELECT
        paye_input_row.id,
        paye_input_row.net_amount
      FROM public.pay_batch_paye_net_inputs AS paye_input_row
      WHERE paye_input_row.pay_batch_candidate_id = batch_candidate.id
      ORDER BY paye_input_row.imported_at_utc DESC,
               paye_input_row.id DESC
      LIMIT 1
    ) AS effective_paye_input
      ON true
    WHERE batch_candidate.pay_batch_id = p_pay_batch_id
      AND coalesce(batch_item.is_voided, false) = false
      AND batch_item.item_type <> 'DEBT_CREATED'
      AND NOT (
        upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) <> 'LOANS'
        AND upper(btrim(coalesce(batch_item.pay_channel, ''))) = 'PAYE'
        AND effective_paye_input.id IS NOT NULL
        AND effective_paye_input.net_amount IS NOT NULL
        AND batch_candidate.net_bank_amount IS NOT NULL
        AND round(batch_candidate.net_bank_amount, 2) = 0
      )
      AND (v_last_pay_batch_item_id IS NULL OR batch_item.id > v_last_pay_batch_item_id)
      AND (
        (v_scope = 'ALL' AND batch_item.pay_channel IN ('PAYE', 'UMBRELLA'))
        OR (v_scope IN ('PAYE', 'UMBRELLA') AND batch_item.pay_channel = v_scope)
        OR (v_scope = 'LOANS' AND batch_item.item_type = 'LOAN_PAYOUT')
        OR (upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) = 'LOANS' AND batch_item.item_type = 'LOAN_PAYOUT')
      )
    ORDER BY batch_item.id
    LIMIT (v_limit + 1)
  )
  SELECT source_rows.*
  FROM source_rows;

  SELECT count(*)::integer
  INTO v_page_count
  FROM pg_temp.tmp_bank_transfer_scope_source_page AS source_page;

  v_has_more := coalesce(v_page_count, 0) > v_limit;

  DROP TABLE IF EXISTS pg_temp.tmp_bank_transfer_scope_group_page;
  CREATE TEMPORARY TABLE pg_temp.tmp_bank_transfer_scope_group_page ON COMMIT DROP AS
  SELECT DISTINCT ON (source_page.pay_channel, source_page.transfer_group_key)
         source_page.pay_channel,
         source_page.transfer_group_key,
         source_page.candidate_id,
         source_page.has_effective_paye_input,
         CASE WHEN source_page.pay_channel = 'UMBRELLA' AND source_page.payee_entity_kind = 'UMBRELLA' THEN source_page.payee_entity_id ELSE NULL::uuid END AS umbrella_id,
         source_page.payee_entity_kind,
         source_page.payee_entity_id,
         'GBP'::text AS currency,
         CASE WHEN source_page.pay_channel = 'PAYE' THEN round(coalesce(source_page.net_bank_amount, 0), 2) ELSE 0::numeric END AS amount,
         CASE WHEN source_page.pay_channel = 'PAYE' THEN 'Pay' ELSE left(coalesce(source_page.payee_name, source_page.candidate_id::text), 18) END AS payment_reference,
         source_page.payee_name,
         source_page.sort_code,
         source_page.account_number,
         source_page.account_type,
         source_page.bank_details_hash_snapshot,
         CASE WHEN source_page.pay_channel = 'PAYE' THEN 'CANDIDATE_DESTINATION' ELSE 'ROW_BACKED_DESTINATION' END AS grouping_mode_used,
         source_page.week_ending_bucket,
         CASE
           WHEN source_page.payout_instruction_snapshot_json IS NULL
             OR source_page.payee_name IS NULL
             OR source_page.bank_details_hash_snapshot IS NULL
             OR length(regexp_replace(coalesce(source_page.sort_code, ''), '[^0-9]', '', 'g')) <> 6
             OR nullif(regexp_replace(coalesce(source_page.account_number, ''), '[^0-9]', '', 'g'), '') IS NULL
             OR (source_page.pay_channel = 'UMBRELLA' AND (source_page.payee_entity_kind IS NULL OR source_page.payee_entity_kind <> 'UMBRELLA' OR source_page.payee_entity_id IS NULL OR source_page.umbrella_id IS NULL OR source_page.payee_entity_id IS DISTINCT FROM source_page.umbrella_id))
             OR (source_page.pay_channel = 'PAYE' AND (source_page.payee_entity_kind IS NULL OR source_page.payee_entity_kind <> 'CANDIDATE' OR source_page.payee_entity_id IS NULL OR source_page.payee_entity_id IS DISTINCT FROM source_page.candidate_id))
             OR (
               source_page.pay_channel = 'PAYE'
               AND upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) <> 'LOANS'
               AND source_page.has_effective_paye_input IS NOT TRUE
             )
             OR (source_page.pay_channel = 'PAYE' AND round(coalesce(source_page.net_bank_amount, 0), 2) <= 0)
           THEN 'FAILED'
           ELSE 'PENDING'
         END AS status,
         CASE
           WHEN source_page.pay_channel = 'PAYE'
            AND upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) <> 'LOANS'
            AND source_page.has_effective_paye_input IS NOT TRUE
             THEN 'PAYE_NET_REQUIRED_FOR_EXECUTION'
           WHEN source_page.pay_channel = 'PAYE'
            AND round(coalesce(source_page.net_bank_amount, 0), 2) <= 0
             THEN 'TRANSFER_SCOPE_NON_POSITIVE_AMOUNT'
           ELSE 'TRANSFER_GROUP_PAYOUT_INSTRUCTION_INVALID'
         END AS failure_reason
  FROM pg_temp.tmp_bank_transfer_scope_source_page AS source_page
  WHERE source_page.page_ordinal <= v_limit
  ORDER BY
    source_page.pay_channel,
    source_page.transfer_group_key,
    CASE
      WHEN source_page.payout_instruction_snapshot_json IS NOT NULL
       AND source_page.payee_name IS NOT NULL
       AND source_page.bank_details_hash_snapshot IS NOT NULL
       AND length(regexp_replace(coalesce(source_page.sort_code, ''), '[^0-9]', '', 'g')) = 6
       AND nullif(regexp_replace(coalesce(source_page.account_number, ''), '[^0-9]', '', 'g'), '') IS NOT NULL
       AND (
         (source_page.pay_channel = 'PAYE'
          AND source_page.payee_entity_kind = 'CANDIDATE'
          AND source_page.payee_entity_id IS NOT NULL
          AND source_page.payee_entity_id IS NOT DISTINCT FROM source_page.candidate_id)
         OR
         (source_page.pay_channel = 'UMBRELLA'
          AND source_page.payee_entity_kind = 'UMBRELLA'
          AND source_page.payee_entity_id IS NOT NULL
          AND source_page.umbrella_id IS NOT NULL
          AND source_page.payee_entity_id IS NOT DISTINCT FROM source_page.umbrella_id)
       )
        THEN 0
      ELSE 1
    END,
    source_page.pay_batch_item_id;

  WITH inserted_scope AS (
    INSERT INTO public.banking_pay_operation_transfer_scope (
      operation_id,
      pay_batch_id,
      pay_channel,
      transfer_group_key,
      candidate_id,
      umbrella_id,
      payee_entity_kind,
      payee_entity_id,
      currency,
      amount,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      bank_details_hash_snapshot,
      grouping_mode_used,
      week_ending_bucket,
      request_id,
      status,
      pay_bank_transfer_id,
      provider_submit_ready,
      provider_submit_state,
      provider_review_required,
      provider_unsafe_reason,
      prepared_item_count,
      prepared_amount_total,
      created_at_utc,
      updated_at_utc
    )
    SELECT p_operation_id,
           p_pay_batch_id,
           group_page.pay_channel,
           group_page.transfer_group_key,
           group_page.candidate_id,
           group_page.umbrella_id,
           group_page.payee_entity_kind,
           group_page.payee_entity_id,
           group_page.currency,
           group_page.amount,
           group_page.payment_reference,
           group_page.payee_name,
           regexp_replace(coalesce(group_page.sort_code, ''), '[^0-9]', '', 'g'),
           regexp_replace(coalesce(group_page.account_number, ''), '[^0-9]', '', 'g'),
           group_page.account_type,
           group_page.bank_details_hash_snapshot,
           group_page.grouping_mode_used,
           group_page.week_ending_bucket,
           'op:' || p_operation_id::text || ':scope:' || md5(group_page.pay_channel || ':' || group_page.transfer_group_key),
           CASE WHEN group_page.status = 'FAILED' THEN 'FAILED' ELSE 'PENDING' END,
           NULL::uuid,
           false,
           CASE WHEN group_page.status = 'FAILED' THEN 'REVIEW_REQUIRED' ELSE 'NOT_READY' END,
           group_page.status = 'FAILED',
           CASE WHEN group_page.status = 'FAILED' THEN group_page.failure_reason ELSE 'TRANSFER_SCOPE_ITEM_SEED_PENDING' END,
           0,
           0,
           v_now,
           v_now
    FROM pg_temp.tmp_bank_transfer_scope_group_page AS group_page
    ON CONFLICT (operation_id, pay_channel, transfer_group_key) DO NOTHING
    RETURNING id
  )
  SELECT count(*)::integer
  INTO v_group_seeded_count
  FROM inserted_scope;

  IF coalesce(v_page_count, 0) > 0 THEN
    SELECT jsonb_build_object('last_pay_batch_item_id', source_page.pay_batch_item_id::text)
    INTO v_next_cursor
    FROM pg_temp.tmp_bank_transfer_scope_source_page AS source_page
    WHERE source_page.page_ordinal <= LEAST(v_limit, v_page_count)
    ORDER BY source_page.page_ordinal DESC
    LIMIT 1;
  ELSE
    v_next_cursor := NULL::jsonb;
  END IF;

  SELECT count(*)::integer
  INTO v_existing_scope_count
  FROM public.banking_pay_operation_transfer_scope AS existing_scope
  WHERE existing_scope.operation_id = p_operation_id
    AND existing_scope.pay_batch_id = p_pay_batch_id;

  SELECT count(*)::integer
  INTO v_unseeded_transfer_scope_count
  FROM public.banking_pay_operation_transfer_scope AS unseeded_scope
  WHERE unseeded_scope.operation_id = p_operation_id
    AND unseeded_scope.pay_batch_id = p_pay_batch_id
    AND upper(btrim(coalesce(v_operation_row.progress_json #>> ARRAY['transfer_scope_item_seed_proofs', unseeded_scope.id::text, 'seed_complete'], 'false'))) <> 'TRUE'
    AND upper(btrim(coalesce(unseeded_scope.provider_submit_state, 'NOT_READY'))) NOT IN (
      'CLAIMED',
      'REQUEST_PREPARING',
      'REQUEST_SENDING',
      'REQUEST_SENT_LOCAL',
      'PROVIDER_ACCEPTED',
      'PROVIDER_REJECTED',
      'PROVIDER_UNKNOWN',
      'CHUNK_FINALISED'
    );

  SELECT next_scope.id
  INTO v_next_transfer_scope_id
  FROM public.banking_pay_operation_transfer_scope AS next_scope
  WHERE next_scope.operation_id = p_operation_id
    AND next_scope.pay_batch_id = p_pay_batch_id
    AND upper(btrim(coalesce(v_operation_row.progress_json #>> ARRAY['transfer_scope_item_seed_proofs', next_scope.id::text, 'seed_complete'], 'false'))) <> 'TRUE'
    AND upper(btrim(coalesce(next_scope.provider_submit_state, 'NOT_READY'))) NOT IN (
      'CLAIMED',
      'REQUEST_PREPARING',
      'REQUEST_SENDING',
      'REQUEST_SENT_LOCAL',
      'PROVIDER_ACCEPTED',
      'PROVIDER_REJECTED',
      'PROVIDER_UNKNOWN',
      'CHUNK_FINALISED'
    )
  ORDER BY next_scope.created_at_utc NULLS FIRST, next_scope.id
  LIMIT 1;

  SELECT count(*)::integer
  INTO v_review_scope_count
  FROM public.banking_pay_operation_transfer_scope AS review_scope
  WHERE review_scope.operation_id = p_operation_id
    AND review_scope.pay_batch_id = p_pay_batch_id
    AND (
      coalesce(review_scope.provider_review_required, false) IS TRUE
      OR upper(btrim(coalesce(review_scope.provider_submit_state, ''))) = 'REVIEW_REQUIRED'
    );

  v_cached_scope_count := coalesce(v_existing_scope_count, 0);

  IF coalesce(v_page_count, 0) = 0 AND coalesce(v_cached_scope_count, 0) <= 0 THEN
      IF v_scoped_no_transfer_execution THEN
        UPDATE public.banking_pay_operations AS operation_update
        SET pay_batch_id = p_pay_batch_id,
            status = CASE
              WHEN UPPER(BTRIM(COALESCE(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN 'RUNNING'
              ELSE operation_update.status
            END,
            phase = 'START_AUTHORISATION_PROOF',
            runner_state = 'RUNNABLE',
            requires_user_action = false,
            resume_reason = CASE WHEN v_no_bank_payment_execution THEN 'NO_BANK_PAYMENT_AUTHORISATION_REQUIRED' ELSE 'NO_TRANSFER_SCOPE_AUTHORISATION_REQUIRED' END,
            run_after_utc = v_now,
            progress_json = JSONB_STRIP_NULLS(
              COALESCE(operation_update.progress_json, '{}'::jsonb)
              || JSONB_BUILD_OBJECT(
                'last_transfer_scope_seed_at_utc', v_now::text,
                'last_transfer_scope_seed_phase', 'TRANSFER_GROUP_SEED_PAGE',
                'transfer_scope_group_seed_complete', true,
                'transfer_scope_count', 0,
                'unseeded_transfer_scope_count', 0,
                'membership_seed_required', false,
                'no_transfer_scope', true,
                'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
                'no_bank_payment_execution', v_no_bank_payment_execution,
                'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes,
                'review_required', false,
                'review_reason_code', NULL::text,
                'has_more_transfer_groups', false,
                'transfer_scope_seed_cursor', NULL::jsonb,
                'next_required_phase', 'START_AUTHORISATION_PROOF'
              )
              || v_no_bank_proof_json
              || JSONB_BUILD_OBJECT('no_bank_payment_proof', v_no_bank_proof_json)
            ),
            error_json = CASE
              WHEN UPPER(BTRIM(COALESCE(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN NULL::jsonb
              ELSE operation_update.error_json
            END,
            updated_at_utc = v_now
        WHERE operation_update.id = p_operation_id;

        RETURN JSONB_BUILD_OBJECT(
          'ok', true,
          'code', CASE WHEN v_no_bank_payment_execution THEN 'NO_BANK_PAYMENT_EXECUTION_READY' ELSE 'NO_TRANSFER_EXECUTION_SCOPE_READY' END,
          'message', CASE WHEN v_no_bank_payment_execution THEN 'All required bank-payment groups are explicit zero; no transfer scope is required and authorisation can continue.' ELSE 'The requested scope contains only explicit-zero PAYE payment groups; no transfer scope is required for this scope and authorisation can continue.' END,
          'operation_id', p_operation_id::text,
          'pay_batch_id', p_pay_batch_id::text,
          'pay_channel_scope', v_scope,
          'bounded', true,
          'limit', v_limit,
          'phase_completed', 'TRANSFER_GROUP_SEED_PAGE',
          'source_item_page_count', 0,
          'group_seeded_count', 0,
          'membership_seeded_count', 0,
          'transfer_scope_count', 0,
          'unseeded_transfer_scope_count', 0,
          'review_scope_count', 0,
          'membership_seed_required', false,
          'no_transfer_scope', true,
          'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
          'no_bank_payment_execution', v_no_bank_payment_execution,
          'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes,
          'missing_explicit_paye_input_count', COALESCE(v_global_projection_missing_paye_count, 0),
          'global_explicit_zero_count', COALESCE(v_global_projection_explicit_zero_count, 0),
          'global_positive_bank_payment_count', COALESCE(v_global_projection_positive_payment_count, 0),
          'global_invalid_payment_row_count', COALESCE(v_global_projection_invalid_payment_row_count, 0),
          'scoped_missing_explicit_paye_input_count', COALESCE(v_scoped_projection_missing_paye_count, 0),
          'scoped_explicit_zero_count', COALESCE(v_scoped_projection_explicit_zero_count, 0),
          'scoped_positive_bank_payment_count', COALESCE(v_scoped_projection_positive_payment_count, 0),
          'scoped_positive_bank_payment_total', ROUND(COALESCE(v_scoped_projection_positive_payment_total, 0), 2),
          'scoped_invalid_payment_row_count', COALESCE(v_scoped_projection_invalid_payment_row_count, 0),
          'paye_net_state_hash', v_global_projection_paye_net_state_hash,
          'global_bank_payment_projection_hash', v_global_projection_bank_payment_hash,
          'scoped_paye_net_state_hash', v_scoped_projection_paye_net_state_hash,
          'bank_payment_projection_hash', v_scoped_projection_bank_payment_hash,
          'has_more_transfer_groups', false,
          'has_more_membership_seed', false,
          'next_transfer_scope_id', NULL::text,
          'next_cursor', NULL::jsonb,
          'next_required_phase', 'START_AUTHORISATION_PROOF',
          'server_utc', v_now::text
        );
      END IF;

      UPDATE public.banking_pay_operations AS operation_update
      SET pay_batch_id = p_pay_batch_id,
          phase = 'REVIEW_REQUIRED',
          progress_json = jsonb_strip_nulls(
            coalesce(operation_update.progress_json, '{}'::jsonb)
            || jsonb_build_object(
              'last_transfer_scope_seed_at_utc', v_now::text,
              'last_transfer_scope_seed_phase', 'TRANSFER_GROUP_SEED_PAGE',
              'transfer_scope_group_seed_complete', true,
              'transfer_scope_count', 0,
              'unseeded_transfer_scope_count', 0,
              'membership_seed_required', false,
              'no_transfer_scope', true,
              'next_required_phase', 'REVIEW_REQUIRED'
            )
          ),
          updated_at_utc = v_now
      WHERE operation_update.id = p_operation_id;

      RETURN jsonb_build_object(
        'ok', false,
        'code', 'NO_ELIGIBLE_TRANSFER_SCOPE',
        'message', 'No eligible payable transfer scope was available for this operation.',
        'operation_id', p_operation_id::text,
        'pay_batch_id', p_pay_batch_id::text,
        'pay_channel_scope', v_scope,
        'bounded', true,
        'limit', v_limit,
        'phase_completed', 'TRANSFER_GROUP_SEED_PAGE',
        'source_item_page_count', 0,
        'group_seeded_count', 0,
        'membership_seeded_count', 0,
        'transfer_scope_count', 0,
        'unseeded_transfer_scope_count', 0,
        'review_scope_count', coalesce(v_review_scope_count, 0),
        'membership_seed_required', false,
        'no_transfer_scope', true,
        'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
        'no_bank_payment_execution', v_no_bank_payment_execution,
        'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes,
        'global_invalid_payment_row_count', COALESCE(v_global_projection_invalid_payment_row_count, 0),
        'scoped_invalid_payment_row_count', COALESCE(v_scoped_projection_invalid_payment_row_count, 0),
        'has_more_transfer_groups', false,
        'has_more_membership_seed', false,
        'next_transfer_scope_id', NULL::text,
        'next_cursor', NULL::jsonb,
        'next_required_phase', 'REVIEW_REQUIRED',
        'server_utc', v_now::text
      );
  END IF;

  v_membership_seed_required := coalesce(v_has_more, false) IS NOT TRUE AND coalesce(v_unseeded_transfer_scope_count, 0) > 0;
  v_next_required_phase := CASE
    WHEN coalesce(v_has_more, false) THEN 'TRANSFER_GROUP_SEED_PAGE'
    WHEN coalesce(v_membership_seed_required, false) THEN 'SEED_TRANSFER_SCOPE_ITEMS'
    ELSE 'SEED_TRANSFER_ROLLUP_CHUNKS'
  END;

  UPDATE public.banking_pay_operations AS operation_update
  SET pay_batch_id = p_pay_batch_id,
      phase = CASE
        WHEN coalesce(v_has_more, false) THEN 'SEED_TRANSFER_SCOPE'
        WHEN coalesce(v_membership_seed_required, false) THEN 'SEED_TRANSFER_SCOPE_ITEMS'
        ELSE 'SEED_TRANSFER_ROLLUP_CHUNKS'
      END,
      runner_state = CASE WHEN upper(btrim(coalesce(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN operation_update.runner_state ELSE 'RUNNABLE' END,
      run_after_utc = CASE WHEN upper(btrim(coalesce(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN operation_update.run_after_utc ELSE v_now END,
      progress_json = jsonb_strip_nulls(
        coalesce(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'last_transfer_scope_seed_at_utc', v_now::text,
          'last_transfer_scope_seed_phase', 'TRANSFER_GROUP_SEED_PAGE',
          'transfer_scope_group_seed_complete', coalesce(v_has_more, false) IS NOT TRUE,
          'transfer_scope_count', coalesce(v_cached_scope_count, 0),
          'unseeded_transfer_scope_count', CASE WHEN coalesce(v_has_more, false) THEN NULL::integer ELSE coalesce(v_unseeded_transfer_scope_count, 0) END,
          'membership_seed_required', coalesce(v_membership_seed_required, false),
          'next_transfer_scope_id', CASE WHEN coalesce(v_membership_seed_required, false) AND v_next_transfer_scope_id IS NOT NULL THEN v_next_transfer_scope_id::text ELSE NULL::text END,
          'has_more_transfer_groups', coalesce(v_has_more, false),
          'transfer_scope_seed_cursor', CASE WHEN coalesce(v_has_more, false) THEN v_next_cursor ELSE NULL::jsonb END,
          'next_required_phase', v_next_required_phase,
          'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
          'no_bank_payment_execution', v_no_bank_payment_execution,
          'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes
        )
        || v_no_bank_proof_json
        || JSONB_BUILD_OBJECT('no_bank_payment_proof', v_no_bank_proof_json)
      ),
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'pay_channel_scope', v_scope,
    'bounded', true,
    'limit', v_limit,
    'phase_completed', 'TRANSFER_GROUP_SEED_PAGE',
    'source_item_page_count', LEAST(coalesce(v_page_count, 0), v_limit),
    'group_seeded_count', coalesce(v_group_seeded_count, 0),
    'membership_seeded_count', 0,
    'transfer_scope_count', coalesce(v_cached_scope_count, 0),
    'unseeded_transfer_scope_count', CASE WHEN coalesce(v_has_more, false) THEN NULL::integer ELSE coalesce(v_unseeded_transfer_scope_count, 0) END,
    'review_scope_count', coalesce(v_review_scope_count, 0),
    'membership_seed_required', coalesce(v_membership_seed_required, false),
    'no_transfer_scope', false,
    'scoped_no_transfer_execution', v_scoped_no_transfer_execution,
    'no_bank_payment_execution', v_no_bank_payment_execution,
    'allow_explicit_zero_no_bank_scopes', v_allow_explicit_zero_no_bank_scopes,
    'global_missing_explicit_paye_input_count', COALESCE(v_global_projection_missing_paye_count, 0),
    'global_explicit_zero_count', COALESCE(v_global_projection_explicit_zero_count, 0),
    'global_positive_bank_payment_count', COALESCE(v_global_projection_positive_payment_count, 0),
    'global_invalid_payment_row_count', COALESCE(v_global_projection_invalid_payment_row_count, 0),
    'scoped_missing_explicit_paye_input_count', COALESCE(v_scoped_projection_missing_paye_count, 0),
    'scoped_explicit_zero_count', COALESCE(v_scoped_projection_explicit_zero_count, 0),
    'scoped_positive_bank_payment_count', COALESCE(v_scoped_projection_positive_payment_count, 0),
    'scoped_invalid_payment_row_count', COALESCE(v_scoped_projection_invalid_payment_row_count, 0),
    'paye_net_state_hash', v_global_projection_paye_net_state_hash,
    'global_bank_payment_projection_hash', v_global_projection_bank_payment_hash,
    'scoped_paye_net_state_hash', v_scoped_projection_paye_net_state_hash,
    'bank_payment_projection_hash', v_scoped_projection_bank_payment_hash,
    'has_more_transfer_groups', coalesce(v_has_more, false),
    'has_more_membership_seed', coalesce(v_membership_seed_required, false),
    'next_transfer_scope_id', CASE WHEN coalesce(v_membership_seed_required, false) AND v_next_transfer_scope_id IS NOT NULL THEN v_next_transfer_scope_id::text ELSE NULL::text END,
    'next_cursor', CASE WHEN coalesce(v_has_more, false) THEN v_next_cursor ELSE NULL::jsonb END,
    'next_required_phase', v_next_required_phase,
    'server_utc', v_now::text
  );
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_execute_bank_transfer_scope_rollup_chunk(
  p_operation_id uuid,
  p_transfer_scope_id uuid,
  p_cursor jsonb DEFAULT NULL::jsonb,
  p_limit integer DEFAULT 100,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_scope_row public.banking_pay_operation_transfer_scope%ROWTYPE;
  v_last_item_ordinal bigint := 0;
  v_page_count integer := 0;
  v_processed_count integer := 0;
  v_page_error_count integer := 0;
  v_page_prepared_count integer := 0;
  v_page_prepared_amount numeric(14,2) := 0;
  v_prepared_item_count integer := 0;
  v_prepared_amount_total numeric(14,2) := 0;
  v_effective_scope_amount numeric(14,2) := 0;
  v_membership_seed_complete boolean := false;
  v_has_more boolean := false;
  v_next_cursor jsonb := NULL::jsonb;
  v_provider_ready boolean := false;
  v_prepared_scope_hash text := NULL::text;
  v_prepared_result_hash text := NULL::text;
BEGIN
  PERFORM set_config('lock_timeout', '3s', true);
  PERFORM public.banking_pay_hot_path_budget_apply('WORKER_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_OPERATION_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_OPERATION_ID_REQUIRED')::text;
  END IF;

  IF p_transfer_scope_id IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_SCOPE_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_SCOPE_ID_REQUIRED')::text;
  END IF;

  IF p_cursor IS NOT NULL AND jsonb_typeof(p_cursor) <> 'object' THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_CURSOR_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_CURSOR_MUST_BE_OBJECT')::text;
  END IF;

  IF nullif(btrim(coalesce(p_cursor->>'last_item_ordinal', '')), '') ~ '^[0-9]+$' THEN
    v_last_item_ordinal := (p_cursor->>'last_item_ordinal')::bigint;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_OPERATION_NOT_FOUND', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF upper(btrim(coalesce(v_operation_row.operation_type, ''))) NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_OPERATION_TYPE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_OPERATION_TYPE_INVALID', 'operation_id', p_operation_id::text, 'operation_type', v_operation_row.operation_type)::text;
  END IF;

  SELECT scope_row.*
  INTO v_scope_row
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.id = p_transfer_scope_id
    AND scope_row.operation_id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_SCOPE_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_SCOPE_NOT_FOUND', 'operation_id', p_operation_id::text, 'transfer_scope_id', p_transfer_scope_id::text)::text;
  END IF;

  IF v_operation_row.pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id <> v_scope_row.pay_batch_id THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_OPERATION_BATCH_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_OPERATION_BATCH_MISMATCH', 'operation_id', p_operation_id::text, 'operation_pay_batch_id', v_operation_row.pay_batch_id::text, 'scope_pay_batch_id', v_scope_row.pay_batch_id::text)::text;
  END IF;

  IF upper(btrim(coalesce(v_scope_row.provider_submit_state, 'NOT_READY'))) NOT IN ('', 'NOT_READY', 'READY', 'REVIEW_REQUIRED') THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_PROVIDER_STATE_ALREADY_STARTED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ROLLUP_PROVIDER_STATE_ALREADY_STARTED', 'transfer_scope_id', p_transfer_scope_id::text, 'provider_submit_state', v_scope_row.provider_submit_state)::text;
  END IF;

  v_membership_seed_complete := COALESCE((v_operation_row.progress_json #>> ARRAY['transfer_scope_item_seed_proofs', p_transfer_scope_id::text, 'seed_complete'])::boolean, false);

  DROP TABLE IF EXISTS pg_temp.tmp_transfer_scope_rollup_page;
  CREATE TEMPORARY TABLE pg_temp.tmp_transfer_scope_rollup_page ON COMMIT DROP AS
  SELECT scope_item.pay_batch_item_id,
         scope_item.item_ordinal,
         scope_item.item_amount,
         CASE
           WHEN item_rows.id IS NULL THEN 'MISSING'
           WHEN COALESCE(item_rows.is_voided, false) THEN 'VOIDED'
           WHEN upper(btrim(coalesce(scope_item.item_status, ''))) IN ('VOIDED', 'PAYOUT_INSTRUCTION_MISSING', 'MISSING', 'ERROR') THEN upper(btrim(coalesce(scope_item.item_status, 'ERROR')))
           WHEN v_scope_row.pay_channel <> 'PAYE'
            AND COALESCE(scope_item.item_amount, 0) <> 0
            AND item_rows.payout_instruction_snapshot_json IS NULL THEN 'PAYOUT_INSTRUCTION_MISSING'
           ELSE 'PREPARED'
         END AS validation_status
  FROM public.banking_pay_operation_transfer_scope_items AS scope_item
  LEFT JOIN public.pay_batch_items AS item_rows
    ON item_rows.id = scope_item.pay_batch_item_id
  WHERE scope_item.operation_id = p_operation_id
    AND scope_item.transfer_scope_id = p_transfer_scope_id
    AND scope_item.item_ordinal > v_last_item_ordinal
    AND upper(btrim(coalesce(scope_item.rollup_status, ''))) = 'PENDING'
  ORDER BY scope_item.item_ordinal, scope_item.pay_batch_item_id
  LIMIT (v_limit + 1);

  SELECT count(*)::integer
  INTO v_page_count
  FROM pg_temp.tmp_transfer_scope_rollup_page AS page_row;

  v_has_more := coalesce(v_page_count, 0) > v_limit;

  WITH page_rows AS (
    SELECT page_row.*
    FROM pg_temp.tmp_transfer_scope_rollup_page AS page_row
    ORDER BY page_row.item_ordinal, page_row.pay_batch_item_id
    LIMIT v_limit
  ), updated AS (
    UPDATE public.banking_pay_operation_transfer_scope_items AS scope_item
    SET item_status = page_rows.validation_status,
        rollup_status = CASE WHEN page_rows.validation_status = 'PREPARED' THEN 'ROLLED_UP' ELSE 'ERROR' END,
        updated_at_utc = v_now
    FROM page_rows
    WHERE scope_item.operation_id = p_operation_id
      AND scope_item.transfer_scope_id = p_transfer_scope_id
      AND scope_item.pay_batch_item_id = page_rows.pay_batch_item_id
    RETURNING scope_item.item_status,
              scope_item.item_amount,
              scope_item.item_ordinal,
              scope_item.pay_batch_item_id
  )
  SELECT count(*)::integer,
         count(*) FILTER (WHERE updated.item_status = 'PREPARED')::integer,
         count(*) FILTER (WHERE updated.item_status <> 'PREPARED')::integer,
         round(coalesce(sum(CASE WHEN updated.item_status = 'PREPARED' THEN coalesce(updated.item_amount, 0) ELSE 0 END), 0), 2)::numeric(14,2),
         md5(coalesce(string_agg(updated.pay_batch_item_id::text || ':' || coalesce(updated.item_amount, 0)::text || ':' || coalesce(updated.item_status, ''), '|' ORDER BY updated.item_ordinal, updated.pay_batch_item_id), 'NO_PAGE_ITEMS'))
  INTO v_processed_count,
       v_page_prepared_count,
       v_page_error_count,
       v_page_prepared_amount,
       v_prepared_scope_hash
  FROM updated;

  v_prepared_item_count := coalesce(v_scope_row.prepared_item_count, 0) + coalesce(v_page_prepared_count, 0);
  v_prepared_amount_total := round(coalesce(v_scope_row.prepared_amount_total, 0) + coalesce(v_page_prepared_amount, 0), 2);
  v_effective_scope_amount := CASE WHEN round(coalesce(v_scope_row.amount, 0), 2) = 0 AND coalesce(v_prepared_amount_total, 0) <> 0 THEN v_prepared_amount_total ELSE round(coalesce(v_scope_row.amount, 0), 2) END;

  v_provider_ready := (
    coalesce(v_membership_seed_complete, false) = true
    AND coalesce(v_has_more, false) = false
    AND coalesce(v_page_error_count, 0) = 0
    AND coalesce(v_scope_row.provider_review_required, false) = false
    AND coalesce(v_prepared_item_count, 0) > 0
    AND round(coalesce(v_prepared_amount_total, 0), 2) = round(coalesce(v_effective_scope_amount, 0), 2)
    AND v_scope_row.pay_bank_transfer_id IS NOT NULL
  );

  v_prepared_result_hash := md5(jsonb_build_object(
    'operation_id', p_operation_id::text,
    'transfer_scope_id', p_transfer_scope_id::text,
    'pay_batch_id', v_scope_row.pay_batch_id::text,
    'prepared_item_count', coalesce(v_prepared_item_count, 0),
    'prepared_amount_total', round(coalesce(v_prepared_amount_total, 0), 2),
    'scope_amount', round(coalesce(v_effective_scope_amount, 0), 2),
    'page_hash', v_prepared_scope_hash,
    'provider_ready', v_provider_ready,
    'has_more', coalesce(v_has_more, false)
  )::text);

  UPDATE public.banking_pay_operation_transfer_scope AS scope_update
  SET prepared_item_count = coalesce(v_prepared_item_count, 0),
      prepared_amount_total = round(coalesce(v_prepared_amount_total, 0), 2),
      amount = CASE WHEN round(coalesce(scope_update.amount, 0), 2) = 0 AND coalesce(v_prepared_amount_total, 0) <> 0 THEN round(coalesce(v_prepared_amount_total, 0), 2) ELSE scope_update.amount END,
      prepared_scope_hash = md5(coalesce(scope_update.prepared_scope_hash, '') || ':' || coalesce(v_prepared_scope_hash, '') || ':' || coalesce(v_processed_count, 0)::text),
      prepared_result_hash = v_prepared_result_hash,
      provider_submit_ready = v_provider_ready,
      provider_submit_state = CASE
        WHEN v_provider_ready THEN 'READY'
        WHEN coalesce(v_page_error_count, 0) > 0 THEN 'REVIEW_REQUIRED'
        ELSE 'NOT_READY'
      END,
      provider_review_required = coalesce(scope_update.provider_review_required, false) OR coalesce(v_page_error_count, 0) > 0,
      provider_unsafe_reason = CASE
        WHEN coalesce(v_page_error_count, 0) > 0 THEN 'TRANSFER_SCOPE_ITEM_ROLLUP_ERROR'
        WHEN coalesce(v_membership_seed_complete, false) IS NOT TRUE THEN 'TRANSFER_SCOPE_ITEM_SEED_INCOMPLETE'
        WHEN coalesce(v_has_more, false) THEN 'TRANSFER_SCOPE_ITEM_ROLLUP_PENDING'
        WHEN scope_update.pay_bank_transfer_id IS NULL THEN 'TRANSFER_PREPARE_PENDING'
        WHEN round(coalesce(v_prepared_amount_total, 0), 2) <> round(coalesce(v_effective_scope_amount, 0), 2) THEN 'TRANSFER_SCOPE_AMOUNT_MISMATCH'
        ELSE NULL::text
      END,
      status = CASE
        WHEN v_provider_ready THEN 'PREPARED'
        WHEN coalesce(v_page_error_count, 0) > 0 THEN 'FAILED'
        WHEN coalesce(v_has_more, false) IS NOT TRUE AND coalesce(v_membership_seed_complete, false) THEN 'ROLLED_UP'
        ELSE scope_update.status
      END,
      updated_at_utc = v_now
  WHERE scope_update.id = p_transfer_scope_id;

  IF v_has_more THEN
    SELECT jsonb_build_object(
      'last_item_ordinal', page_row.item_ordinal,
      'last_pay_batch_item_id', page_row.pay_batch_item_id::text
    )
    INTO v_next_cursor
    FROM pg_temp.tmp_transfer_scope_rollup_page AS page_row
    ORDER BY page_row.item_ordinal, page_row.pay_batch_item_id
    LIMIT 1 OFFSET GREATEST(v_limit - 1, 0);
  ELSE
    v_next_cursor := NULL::jsonb;
  END IF;

  UPDATE public.banking_pay_operations AS operation_update
  SET phase = CASE WHEN coalesce(v_has_more, false) THEN 'ROLLUP_TRANSFER_SCOPE_ITEMS' ELSE 'PREPARE_TRANSFER_CHUNKS' END,
      runner_state = CASE WHEN upper(btrim(coalesce(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN operation_update.runner_state ELSE 'RUNNABLE' END,
      run_after_utc = CASE WHEN upper(btrim(coalesce(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN operation_update.run_after_utc ELSE v_now END,
      progress_json = jsonb_strip_nulls(
        coalesce(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'last_transfer_scope_rollup', jsonb_build_object(
            'transfer_scope_id', p_transfer_scope_id::text,
            'rolled_at_utc', v_now::text,
            'processed_count', coalesce(v_processed_count, 0),
            'membership_seed_complete', coalesce(v_membership_seed_complete, false),
            'provider_submit_ready', v_provider_ready,
            'has_more', coalesce(v_has_more, false),
            'next_cursor', v_next_cursor
          ),
          'transfer_scope_rollup_proofs', CASE
              WHEN jsonb_typeof(operation_update.progress_json->'transfer_scope_rollup_proofs') = 'object'
                THEN coalesce(operation_update.progress_json->'transfer_scope_rollup_proofs', '{}'::jsonb)
              ELSE '{}'::jsonb
            END || jsonb_build_object(
              p_transfer_scope_id::text,
              jsonb_build_object(
                'membership_seed_complete', coalesce(v_membership_seed_complete, false),
                'prepared_item_count', coalesce(v_prepared_item_count, 0),
                'prepared_amount_total', round(coalesce(v_prepared_amount_total, 0), 2),
                'prepared_result_hash', v_prepared_result_hash,
                'provider_submit_ready', v_provider_ready,
                'updated_at_utc', v_now::text
              )
            ),
          'next_required_phase', CASE WHEN coalesce(v_has_more, false) THEN 'TRANSFER_SCOPE_ROLLUP_PAGE' ELSE 'TRANSFER_CHUNK_PREPARE_PAGE' END
        )
      ),
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'pay_batch_id', v_scope_row.pay_batch_id::text,
    'transfer_scope_id', p_transfer_scope_id::text,
    'limit', v_limit,
    'processed_count', coalesce(v_processed_count, 0),
    'page_error_count', coalesce(v_page_error_count, 0),
    'membership_seed_complete', coalesce(v_membership_seed_complete, false),
    'prepared_item_count', coalesce(v_prepared_item_count, 0),
    'prepared_amount_total', round(coalesce(v_prepared_amount_total, 0), 2),
    'scope_amount', round(coalesce(v_effective_scope_amount, 0), 2),
    'prepared_result_hash', v_prepared_result_hash,
    'provider_submit_ready', v_provider_ready,
    'has_more', coalesce(v_has_more, false),
    'next_cursor', v_next_cursor,
    'server_utc', v_now::text
  );
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_execute_bank_transfer_scope_items_seed_chunk(
  p_operation_id uuid,
  p_transfer_scope_id uuid,
  p_cursor jsonb DEFAULT NULL::jsonb,
  p_limit integer DEFAULT 100,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_scope_row public.banking_pay_operation_transfer_scope%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_last_pay_batch_item_id uuid := NULL::uuid;
  v_last_item_ordinal bigint := 0;
  v_page_count integer := 0;
  v_seeded_count integer := 0;
  v_page_error_count integer := 0;
  v_has_more boolean := false;
  v_seed_complete boolean := false;
  v_next_cursor jsonb := NULL::jsonb;
  v_seed_proof_hash text := NULL::text;
  v_membership_count bigint := 0;
  v_no_source boolean := false;
BEGIN
  PERFORM set_config('lock_timeout', '3s', true);
  PERFORM public.banking_pay_hot_path_budget_apply('WORKER_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_OPERATION_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_OPERATION_ID_REQUIRED')::text;
  END IF;

  IF p_transfer_scope_id IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_SCOPE_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_SCOPE_ID_REQUIRED')::text;
  END IF;

  IF p_cursor IS NOT NULL AND jsonb_typeof(p_cursor) <> 'object' THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_CURSOR_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_CURSOR_MUST_BE_OBJECT')::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_OPERATION_NOT_FOUND', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF upper(btrim(coalesce(v_operation_row.operation_type, ''))) NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_OPERATION_TYPE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_OPERATION_TYPE_INVALID', 'operation_id', p_operation_id::text, 'operation_type', v_operation_row.operation_type)::text;
  END IF;

  SELECT scope_row.*
  INTO v_scope_row
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  WHERE scope_row.id = p_transfer_scope_id
    AND scope_row.operation_id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_SCOPE_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_SCOPE_NOT_FOUND', 'operation_id', p_operation_id::text, 'transfer_scope_id', p_transfer_scope_id::text)::text;
  END IF;

  IF v_operation_row.pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id <> v_scope_row.pay_batch_id THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_OPERATION_BATCH_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_OPERATION_BATCH_MISMATCH', 'operation_id', p_operation_id::text, 'operation_pay_batch_id', v_operation_row.pay_batch_id::text, 'scope_pay_batch_id', v_scope_row.pay_batch_id::text)::text;
  END IF;

  IF upper(btrim(coalesce(v_scope_row.provider_submit_state, 'NOT_READY'))) NOT IN ('', 'NOT_READY', 'READY', 'REVIEW_REQUIRED') THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_PROVIDER_STATE_ALREADY_STARTED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_PROVIDER_STATE_ALREADY_STARTED', 'transfer_scope_id', p_transfer_scope_id::text, 'provider_submit_state', v_scope_row.provider_submit_state)::text;
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = v_scope_row.pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_EXECUTE_TRANSFER_SCOPE_ITEMS_SEED_BATCH_NOT_FOUND', 'pay_batch_id', v_scope_row.pay_batch_id::text)::text;
  END IF;

  IF nullif(btrim(coalesce(p_cursor->>'last_pay_batch_item_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_last_pay_batch_item_id := (p_cursor->>'last_pay_batch_item_id')::uuid;
  END IF;
  IF nullif(btrim(coalesce(p_cursor->>'last_item_ordinal', '')), '') ~ '^[0-9]+$' THEN
    v_last_item_ordinal := (p_cursor->>'last_item_ordinal')::bigint;
  END IF;

  IF v_last_pay_batch_item_id IS NULL THEN
    SELECT existing_scope_item.pay_batch_item_id,
           existing_scope_item.item_ordinal
    INTO v_last_pay_batch_item_id,
         v_last_item_ordinal
    FROM public.banking_pay_operation_transfer_scope_items AS existing_scope_item
    WHERE existing_scope_item.operation_id = p_operation_id
      AND existing_scope_item.transfer_scope_id = p_transfer_scope_id
    ORDER BY existing_scope_item.item_ordinal DESC, existing_scope_item.pay_batch_item_id DESC
    LIMIT 1;

    IF NOT FOUND THEN
      v_last_pay_batch_item_id := NULL::uuid;
      v_last_item_ordinal := 0;
    END IF;
  END IF;

  v_last_item_ordinal := COALESCE(v_last_item_ordinal, 0);

  DROP TABLE IF EXISTS pg_temp.tmp_transfer_scope_item_seed_page;
  CREATE TEMPORARY TABLE pg_temp.tmp_transfer_scope_item_seed_page ON COMMIT DROP AS
  WITH candidate_item_page AS (
    SELECT batch_item.id AS pay_batch_item_id,
           batch_item.pay_batch_candidate_id,
           batch_candidate.candidate_id,
           batch_candidate.net_bank_amount,
           COALESCE(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0)::numeric AS source_item_amount,
           batch_item.item_type,
           batch_item.is_voided,
           batch_item.payout_instruction_snapshot_json,
           row_number() OVER (ORDER BY batch_item.id)::bigint AS page_row_no
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
    WHERE batch_candidate.pay_batch_id = v_scope_row.pay_batch_id
      AND batch_candidate.candidate_id = v_scope_row.candidate_id
      AND coalesce(batch_item.is_voided, false) = false
      AND batch_item.item_type <> 'DEBT_CREATED'
      AND (v_last_pay_batch_item_id IS NULL OR batch_item.id > v_last_pay_batch_item_id)
      AND (
        (v_scope_row.pay_channel = 'PAYE' AND (CASE WHEN upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) = 'LOANS' THEN 'PAYE' ELSE batch_item.pay_channel END) = 'PAYE')
        OR (v_scope_row.pay_channel = 'UMBRELLA' AND (CASE WHEN upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) = 'LOANS' THEN 'PAYE' ELSE batch_item.pay_channel END) = 'UMBRELLA')
      )
      AND (
        CASE
          WHEN CASE WHEN upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) = 'LOANS' THEN 'PAYE' ELSE batch_item.pay_channel END = 'PAYE'
            THEN batch_candidate.candidate_id::text
               || '|PAYE|'
               || COALESCE(
                    NULLIF(BTRIM(COALESCE(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''),
                    (
                      SELECT NULLIF(
                               BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'bank_details_hash', '')),
                               ''
                             )
                      FROM public.pay_batch_items AS fallback_item
                      WHERE fallback_item.pay_batch_candidate_id = batch_candidate.id
                        AND COALESCE(fallback_item.is_voided, false) = false
                        AND fallback_item.item_type <> 'DEBT_CREATED'
                        AND fallback_item.pay_channel = 'PAYE'
                        AND NULLIF(
                              BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'bank_details_hash', '')),
                              ''
                            ) IS NOT NULL
                      ORDER BY
                        CASE
                          WHEN NULLIF(
                                 BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'sort_code', '')),
                                 ''
                               ) IS NOT NULL
                           AND NULLIF(
                                 BTRIM(COALESCE(fallback_item.payout_instruction_snapshot_json->>'account_number', '')),
                                 ''
                               ) IS NOT NULL
                            THEN 0
                          ELSE 1
                        END,
                        fallback_item.id
                      LIMIT 1
                    ),
                    ''
                  )
          WHEN CASE WHEN upper(btrim(coalesce(v_batch_row.batch_kind_fixed, ''))) = 'LOANS' THEN 'PAYE' ELSE batch_item.pay_channel END = 'UMBRELLA'
            THEN batch_candidate.candidate_id::text || '|UMBRELLA|' || coalesce(CASE WHEN coalesce(batch_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$' THEN (batch_item.payout_instruction_snapshot_json->>'week_ending_bucket') ELSE 'NO_WEEK' END, 'NO_WEEK') || '|' || coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '') || '|' || coalesce(nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''), '')
          ELSE batch_candidate.candidate_id::text || '|OTHER|' || coalesce(nullif(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''), '')
        END
      ) = v_scope_row.transfer_group_key
    ORDER BY batch_item.id
    LIMIT (v_limit + 1)
  )
  SELECT (COALESCE(v_last_item_ordinal, 0) + candidate_item_page.page_row_no)::bigint AS item_ordinal,
         candidate_item_page.pay_batch_item_id,
         candidate_item_page.pay_batch_candidate_id,
         candidate_item_page.candidate_id,
         round(
           CASE
             WHEN v_scope_row.pay_channel = 'PAYE' THEN CASE WHEN (COALESCE(v_last_item_ordinal, 0) + candidate_item_page.page_row_no) = 1 THEN coalesce(nullif(v_scope_row.amount, 0), candidate_item_page.net_bank_amount, 0) ELSE 0 END
             ELSE coalesce(candidate_item_page.source_item_amount, 0)
           END,
           2
         )::numeric(14,2) AS item_amount,
         CASE
           WHEN COALESCE(candidate_item_page.is_voided, false) THEN 'VOIDED'
           WHEN candidate_item_page.item_type = 'DEBT_CREATED' THEN 'IGNORED_DEBT_CREATED'
           WHEN v_scope_row.pay_channel <> 'PAYE'
            AND COALESCE(candidate_item_page.source_item_amount, 0) <> 0
            AND candidate_item_page.payout_instruction_snapshot_json IS NULL THEN 'PAYOUT_INSTRUCTION_MISSING'
           ELSE 'PENDING'
         END AS seed_status
  FROM candidate_item_page
  ORDER BY candidate_item_page.page_row_no;

  SELECT count(*)::integer
  INTO v_page_count
  FROM pg_temp.tmp_transfer_scope_item_seed_page AS page_row;

  v_has_more := coalesce(v_page_count, 0) > v_limit;

  WITH page_rows AS (
    SELECT page_row.*
    FROM pg_temp.tmp_transfer_scope_item_seed_page AS page_row
    ORDER BY page_row.item_ordinal, page_row.pay_batch_item_id
    LIMIT v_limit
  ), upserted AS (
    INSERT INTO public.banking_pay_operation_transfer_scope_items (
      operation_id,
      pay_batch_id,
      transfer_scope_id,
      pay_batch_item_id,
      pay_batch_candidate_id,
      candidate_id,
      item_amount,
      item_status,
      item_ordinal,
      rollup_status,
      created_at_utc,
      updated_at_utc
    )
    SELECT p_operation_id,
           v_scope_row.pay_batch_id,
           p_transfer_scope_id,
           page_rows.pay_batch_item_id,
           page_rows.pay_batch_candidate_id,
           page_rows.candidate_id,
           page_rows.item_amount,
           page_rows.seed_status,
           page_rows.item_ordinal,
           'PENDING',
           v_now,
           v_now
    FROM page_rows
    ON CONFLICT (operation_id, transfer_scope_id, pay_batch_item_id)
    DO UPDATE
    SET pay_batch_id = EXCLUDED.pay_batch_id,
        pay_batch_candidate_id = EXCLUDED.pay_batch_candidate_id,
        candidate_id = EXCLUDED.candidate_id,
        item_amount = EXCLUDED.item_amount,
        item_status = CASE WHEN public.banking_pay_operation_transfer_scope_items.rollup_status IN ('ROLLED_UP', 'PREPARED', 'READY', 'COMPLETE', 'COMPLETED') THEN public.banking_pay_operation_transfer_scope_items.item_status ELSE EXCLUDED.item_status END,
        item_ordinal = EXCLUDED.item_ordinal,
        rollup_status = CASE WHEN public.banking_pay_operation_transfer_scope_items.rollup_status IN ('ROLLED_UP', 'PREPARED', 'READY', 'COMPLETE', 'COMPLETED') THEN public.banking_pay_operation_transfer_scope_items.rollup_status ELSE 'PENDING' END,
        updated_at_utc = v_now
    RETURNING item_status, item_ordinal, pay_batch_item_id
  )
  SELECT count(*)::integer,
         count(*) FILTER (WHERE upper(btrim(coalesce(upserted.item_status, ''))) IN ('VOIDED', 'PAYOUT_INSTRUCTION_MISSING', 'MISSING', 'ERROR'))::integer,
         max(upserted.item_ordinal)
  INTO v_seeded_count,
       v_page_error_count,
       v_membership_count
  FROM upserted;

  v_no_source := coalesce(v_seeded_count, 0) = 0 AND coalesce(v_last_item_ordinal, 0) = 0;
  v_seed_complete := coalesce(v_has_more, false) IS NOT TRUE;

  IF v_has_more THEN
    SELECT jsonb_build_object(
      'last_item_ordinal', page_row.item_ordinal,
      'last_pay_batch_item_id', page_row.pay_batch_item_id::text
    )
    INTO v_next_cursor
    FROM pg_temp.tmp_transfer_scope_item_seed_page AS page_row
    ORDER BY page_row.item_ordinal, page_row.pay_batch_item_id
    LIMIT 1 OFFSET GREATEST(v_limit - 1, 0);
  ELSE
    v_next_cursor := NULL::jsonb;
  END IF;

  v_seed_proof_hash := md5(jsonb_build_object(
    'operation_id', p_operation_id::text,
    'transfer_scope_id', p_transfer_scope_id::text,
    'pay_batch_id', v_scope_row.pay_batch_id::text,
    'seeded_count', coalesce(v_seeded_count, 0),
    'last_item_ordinal', coalesce(v_membership_count, v_last_item_ordinal, 0),
    'seed_complete', coalesce(v_seed_complete, false),
    'has_more', coalesce(v_has_more, false)
  )::text);

  UPDATE public.banking_pay_operation_transfer_scope AS scope_update
  SET provider_submit_ready = false,
      provider_submit_state = CASE WHEN coalesce(v_page_error_count, 0) > 0 OR v_no_source THEN 'REVIEW_REQUIRED' ELSE 'NOT_READY' END,
      provider_review_required = coalesce(v_page_error_count, 0) > 0 OR v_no_source,
      provider_unsafe_reason = CASE
        WHEN v_no_source THEN 'TRANSFER_SCOPE_ITEM_SOURCE_EMPTY'
        WHEN coalesce(v_page_error_count, 0) > 0 THEN 'TRANSFER_SCOPE_ITEM_SEED_ERROR'
        WHEN coalesce(v_seed_complete, false) IS NOT TRUE THEN 'TRANSFER_SCOPE_ITEM_SEED_INCOMPLETE'
        ELSE 'TRANSFER_SCOPE_ITEM_ROLLUP_PENDING'
      END,
      updated_at_utc = v_now
  WHERE scope_update.id = p_transfer_scope_id;

  UPDATE public.banking_pay_operations AS operation_update
  SET phase = CASE WHEN coalesce(v_has_more, false) THEN 'SEED_TRANSFER_SCOPE_ITEMS' ELSE 'ROLLUP_TRANSFER_SCOPE_ITEMS' END,
      runner_state = CASE WHEN upper(btrim(coalesce(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN operation_update.runner_state ELSE 'RUNNABLE' END,
      run_after_utc = CASE WHEN upper(btrim(coalesce(operation_update.status, ''))) = 'REVIEW_REQUIRED' THEN operation_update.run_after_utc ELSE v_now END,
      progress_json = jsonb_strip_nulls(
        coalesce(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'last_transfer_scope_item_seed', jsonb_build_object(
            'transfer_scope_id', p_transfer_scope_id::text,
            'seeded_at_utc', v_now::text,
            'seeded_count', coalesce(v_seeded_count, 0),
            'last_item_ordinal', coalesce(v_membership_count, v_last_item_ordinal, 0),
            'seed_complete', coalesce(v_seed_complete, false),
            'seed_proof_hash', v_seed_proof_hash,
            'has_more', coalesce(v_has_more, false),
            'next_cursor', v_next_cursor
          ),
          'transfer_scope_item_seed_proofs', CASE
              WHEN jsonb_typeof(operation_update.progress_json->'transfer_scope_item_seed_proofs') = 'object'
                THEN coalesce(operation_update.progress_json->'transfer_scope_item_seed_proofs', '{}'::jsonb)
              ELSE '{}'::jsonb
            END || jsonb_build_object(
              p_transfer_scope_id::text,
              jsonb_build_object(
                'last_item_ordinal', coalesce(v_membership_count, v_last_item_ordinal, 0),
                'seed_complete', coalesce(v_seed_complete, false),
                'seed_proof_hash', v_seed_proof_hash,
                'updated_at_utc', v_now::text
              )
            ),
          'next_required_phase', CASE WHEN coalesce(v_has_more, false) THEN 'TRANSFER_SCOPE_ITEM_MEMBERSHIP_SEED_PAGE' ELSE 'TRANSFER_SCOPE_ROLLUP_PAGE' END
        )
      ),
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'pay_batch_id', v_scope_row.pay_batch_id::text,
    'transfer_scope_id', p_transfer_scope_id::text,
    'source_mode', 'ROW_BACKED_TRANSFER_GROUP',
    'limit', v_limit,
    'seeded_count', coalesce(v_seeded_count, 0),
    'last_item_ordinal', coalesce(v_membership_count, v_last_item_ordinal, 0),
    'seed_complete', coalesce(v_seed_complete, false),
    'seed_proof_hash', v_seed_proof_hash,
    'page_error_count', coalesce(v_page_error_count, 0),
    'has_more', coalesce(v_has_more, false),
    'next_cursor', v_next_cursor,
    'server_utc', v_now::text
  );
END;
$function$;

