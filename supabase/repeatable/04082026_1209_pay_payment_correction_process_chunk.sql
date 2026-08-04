-- CloudTMS Banking Pay cancellation — Stage 1 replacement.
-- One public phase owner; every call advances exactly one bounded phase page.

CREATE OR REPLACE FUNCTION public.pay_payment_correction_process_chunk(
  p_correction_request_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 50,
  p_worker_id text DEFAULT NULL::text,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO pg_catalog, private, extensions, pg_temp
SET statement_timeout TO '6000ms'
SET lock_timeout TO '1000ms'
AS $function$
DECLARE
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_request public.pay_payment_correction_requests%rowtype;
  v_operation public.banking_pay_operations%rowtype;
  v_batch public.pay_batches%rowtype;
  v_phase text;
  v_phase_result jsonb := '{}'::jsonb;
  v_work_result jsonb := '{}'::jsonb;
  v_work record;
  v_claim_limit integer;
  v_claimed_count integer := 0;
  v_applied_count integer := 0;
  v_blocked_count integer := 0;
  v_failed_count integer := 0;
  v_retryable_count integer := 0;
  v_nonterminal_count integer := 0;
  v_summary_cursor bigint := 0;
  v_summary_next bigint := 0;
  v_summary_count integer := 0;
  v_summary_has_more boolean := false;
  v_total_applied integer := 0;
  v_total_blocked integer := 0;
  v_total_failed integer := 0;
  v_total_cancelled integer := 0;
  v_total_terminal integer := 0;
  v_request_result text;
  v_final_result jsonb;
  v_final_result_hash text;
  v_active_item_count integer := 0;
  v_active_candidate_count integer := 0;
  v_active_source_item_amount numeric(14,2) := 0;
  v_active_net_bank_amount numeric(14,2) := 0;
  v_active_paye_schedule_amount numeric(14,2) := 0;
  v_active_transfer_amount numeric(14,2) := 0;
  v_active_paye_candidate_count integer := 0;
  v_unselected_scope_hash_before text;
  v_unselected_scope_hash_after text;
  v_blocked_scope_mismatch_count integer := 0;
  v_requested_action text;
  v_refresh_cursor bigint := 0;
  v_refresh_next bigint := 0;
  v_refresh_candidate_ids jsonb := '[]'::jsonb;
  v_refresh_count integer := 0;
  v_refresh_has_more boolean := false;
  v_refresh_result jsonb := '{}'::jsonb;
  v_refresh_sequence integer := 0;
  v_session_id uuid;
  v_refresh_actor_user_id uuid;
  v_error_message text;
  v_error_state text;
  v_preflight record;
  v_auto_start_result jsonb := '{}'::jsonb;
BEGIN
  IF p_correction_request_id IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'REQUEST_NOT_FOUND')::text;
  END IF;
  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 100 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_PROCESS_LIMIT_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'LIMIT_INVALID')::text;
  END IF;

  -- Automatic, provider-originated terminal no-money requests do not require
  -- requester reauthentication. The public phase owner delegates exactly one
  -- bounded internal START_AUTO transition before taking any row lock, so the
  -- mutation guard keeps its canonical lock order.
  SELECT operation_row.phase,
         request_row.auto_requested,
         request_row.pay_batch_id,
         request_row.reason,
         request_row.source_bank_event_id,
         request_row.accepted_resolution_json
  INTO v_preflight
  FROM public.banking_pay_operations AS operation_row
  JOIN public.pay_payment_correction_requests AS request_row
    ON request_row.id::text = operation_row.input_json ->> 'correction_request_id'
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND request_row.id = p_correction_request_id
  ORDER BY operation_row.created_at_utc
  LIMIT 1;

  IF v_preflight.phase = 'AWAITING_REAUTHENTICATION'
     AND COALESCE(v_preflight.auto_requested, false) THEN
    v_auto_start_result := public.pay_payment_correction_request_start(
      p_pay_batch_id := v_preflight.pay_batch_id,
      p_selection_json := pg_catalog.jsonb_build_object(
        'command', 'START_AUTO',
        'correction_request_id', p_correction_request_id
      ),
      p_reason := v_preflight.reason,
      p_actor_user_id := p_actor_user_id,
      p_source_bank_event_id := v_preflight.source_bank_event_id,
      p_auto_requested := true,
      p_accepted_resolution_json := v_preflight.accepted_resolution_json
    );

    RETURN v_auto_start_result || pg_catalog.jsonb_build_object(
      'phase_owner', 'pay_payment_correction_process_chunk',
      'automatic_provider_no_money', true,
      'processed', 0,
      'applied', 0,
      'skipped', 0,
      'blocked', 0,
      'failed_retryable', 0,
      'failed_final', 0,
      'pending', 0,
      'processing', 0,
      'progress_completed', 0,
      'progress_total', 0,
      'parent_status', v_auto_start_result->>'request_status',
      'totals', '{}'::jsonb,
      'complete', false,
      'requires_user_action', false,
      'processing_continues', true,
      'changed_scope_json', '{}'::jsonb,
      'live_signal_updates', '{}'::jsonb
    );
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND operation_row.input_json->>'correction_request_id' = p_correction_request_id::text
  ORDER BY operation_row.created_at_utc
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'OPERATION_MISMATCH')::text;
  END IF;

  IF p_worker_id IS NULL OR pg_catalog.btrim(p_worker_id) = '' THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_OPERATION_LEASE_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'LEASE_REQUIRED')::text;
  END IF;
  IF COALESCE(v_operation.lease_owner, v_operation.locked_by) IS NULL
     OR COALESCE(v_operation.lease_owner, v_operation.locked_by) IS DISTINCT FROM p_worker_id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_OPERATION_LEASE_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'LEASE_MISMATCH')::text;
  END IF;
  IF COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc) IS NULL THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_OPERATION_LEASE_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'LEASE_REQUIRED')::text;
  END IF;
  IF COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc) <= v_now THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_OPERATION_LEASE_EXPIRED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'LEASE_MISMATCH')::text;
  END IF;

  SELECT request_row.* INTO v_request
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.id = p_correction_request_id
  FOR UPDATE;
  IF NOT FOUND OR v_request.pay_batch_id IS DISTINCT FROM v_operation.pay_batch_id THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_OPERATION_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'OPERATION_MISMATCH')::text;
  END IF;

  SELECT batch_row.* INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = v_request.pay_batch_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'PAY_BATCH_NOT_FOUND')::text;
  END IF;

  v_phase := v_operation.phase;
  v_requested_action := COALESCE(
    v_request.plan_json->>'requested_action', v_request.selection_json->>'requested_action'
  );

  IF v_phase = 'PREPARE_SELECTION' THEN
    v_phase_result := public.pay_payment_correction_selection_prepare_chunk_v1(
      p_correction_request_id,
      v_operation.id,
      CASE WHEN v_operation.progress_json->>'last_pay_batch_candidate_id' IS NULL THEN NULL
           ELSE pg_catalog.jsonb_build_object(
             'last_pay_batch_candidate_id', v_operation.progress_json->>'last_pay_batch_candidate_id'
           ) END,
      LEAST(p_limit, 100),
      p_worker_id,
      p_actor_user_id
    );
    RETURN v_phase_result || pg_catalog.jsonb_build_object(
      'phase_owner', 'pay_payment_correction_process_chunk',
      'processed', COALESCE((v_phase_result->>'page_candidate_count')::integer, 0),
      'applied', 0,
      'skipped', 0,
      'blocked', 0,
      'failed_retryable', 0,
      'failed_final', 0,
      'pending', 0,
      'processing', 0,
      'progress_completed', COALESCE(v_operation.completed_units, 0),
      'progress_total', COALESCE(v_operation.total_units, 0),
      'parent_status', v_request.status,
      'totals', pg_catalog.jsonb_build_object(
        'selected_candidate_count', v_request.plan_json->>'selected_candidate_count',
        'selected_active_item_count', v_request.plan_json->>'selected_active_item_count'
      ),
      'complete', COALESCE((v_phase_result->>'complete')::boolean, false),
      'requires_user_action', COALESCE((v_phase_result->>'complete')::boolean, false),
      'processing_continues', NOT COALESCE((v_phase_result->>'complete')::boolean, false),
      'changed_scope_json', '{}'::jsonb,
      'live_signal_updates', '{}'::jsonb
    );
  END IF;

  IF v_phase = 'EXPAND_WORK' THEN
    v_phase_result := public.pay_payment_correction_expand_work(p_correction_request_id, p_actor_user_id);
    RETURN v_phase_result || pg_catalog.jsonb_build_object(
      'phase_owner', 'pay_payment_correction_process_chunk',
      'processed', COALESCE((v_phase_result->>'page_work_count')::integer, 0),
      'applied', 0,
      'skipped', 0,
      'blocked', 0,
      'failed_retryable', 0,
      'failed_final', 0,
      'pending', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS pending_work
        WHERE pending_work.correction_request_id = p_correction_request_id
          AND pending_work.status = 'PENDING'
      ),
      'processing', 0,
      'progress_completed', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS expanded_work
        WHERE expanded_work.correction_request_id = p_correction_request_id
      ),
      'progress_total', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_request_candidates AS selected_member
        WHERE selected_member.correction_request_id = p_correction_request_id
      ),
      'parent_status', v_request.status,
      'totals', pg_catalog.jsonb_build_object(
        'inserted_count', v_phase_result->>'inserted_count',
        'existing_count', v_phase_result->>'existing_count'
      ),
      'complete', COALESCE((v_phase_result->>'complete')::boolean, false),
      'requires_user_action', false,
      'processing_continues', true,
      'changed_scope_json', '{}'::jsonb,
      'live_signal_updates', '{}'::jsonb
    );
  END IF;

  IF v_phase = 'PROCESS_CHUNKS' THEN
    v_claim_limit := LEAST(p_limit, 25);

    UPDATE public.pay_payment_correction_work_items AS stale_work
    SET status = CASE WHEN stale_work.attempt_count >= 5 THEN 'FAILED_FINAL' ELSE 'FAILED_RETRYABLE' END,
        locked_at_utc = NULL, locked_by = NULL,
        processed_at_utc = CASE WHEN stale_work.attempt_count >= 5 THEN v_now ELSE stale_work.processed_at_utc END,
        last_error = 'STALE_PROCESSING_LEASE_RECOVERED',
        result_json = COALESCE(stale_work.result_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'code', CASE
              WHEN stale_work.attempt_count >= 5 THEN 'FAILED_FINAL'
              ELSE 'ZERO_APPLIED_STALE_LEASE_RECOVERED'
            END,
            'stale_lease_recovered', true
          )
    WHERE stale_work.correction_request_id = p_correction_request_id
      AND stale_work.status = 'PROCESSING'
      AND stale_work.locked_at_utc < v_now - interval '120 seconds';

    FOR v_work IN
      WITH claimable AS (
        SELECT work_row.id
        FROM public.pay_payment_correction_work_items AS work_row
        WHERE work_row.correction_request_id = p_correction_request_id
          AND work_row.status IN ('PENDING', 'FAILED_RETRYABLE')
          AND work_row.attempt_count < 5
        ORDER BY work_row.created_at_utc, work_row.id
        LIMIT v_claim_limit
        FOR UPDATE SKIP LOCKED
      ), claimed AS (
        UPDATE public.pay_payment_correction_work_items AS work_row
        SET status = 'PROCESSING', attempt_count = work_row.attempt_count + 1,
            locked_at_utc = v_now, locked_by = p_worker_id, last_error = NULL
        FROM claimable
        WHERE work_row.id = claimable.id
        RETURNING work_row.*
      )
      SELECT * FROM claimed ORDER BY created_at_utc, id
    LOOP
      v_claimed_count := v_claimed_count + 1;
      BEGIN
        IF v_work.work_kind = 'PRE_BANK_CANCEL' THEN
          v_work_result := public.pay_pre_bank_cancel_apply_work_item(v_work.id, p_actor_user_id);
        ELSIF v_work.work_kind = 'NO_MONEY_UNWIND' THEN
          v_work_result := public.pay_no_money_unwind_apply_work_item(v_work.id, p_actor_user_id);
        ELSE
          v_work_result := pg_catalog.jsonb_build_object(
            'ok', false, 'status', 'BLOCKED',
            'blocker', pg_catalog.jsonb_build_object('code', 'BLOCKED_BY_UNSUPPORTED_SOURCE')
          );
        END IF;

        UPDATE public.pay_payment_correction_work_items AS completed_work
        SET status = CASE
              WHEN completed_work.status <> 'PROCESSING' THEN completed_work.status
              WHEN pg_catalog.upper(COALESCE(v_work_result->>'status', '')) IN ('APPLIED','BLOCKED','SKIPPED','FAILED_FINAL')
                THEN pg_catalog.upper(v_work_result->>'status')
              WHEN COALESCE((v_work_result->>'ok')::boolean, false) THEN 'APPLIED'
              ELSE 'BLOCKED'
            END,
            locked_at_utc = NULL, locked_by = NULL,
            processed_at_utc = COALESCE(completed_work.processed_at_utc, v_now),
            result_json = COALESCE(completed_work.result_json, '{}'::jsonb) || v_work_result
        WHERE completed_work.id = v_work.id;
      EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT, v_error_state = RETURNED_SQLSTATE;
        UPDATE public.pay_payment_correction_work_items AS failed_work
        SET status = CASE WHEN failed_work.attempt_count >= 5 THEN 'FAILED_FINAL' ELSE 'FAILED_RETRYABLE' END,
            locked_at_utc = NULL, locked_by = NULL,
            processed_at_utc = CASE WHEN failed_work.attempt_count >= 5 THEN v_now ELSE NULL END,
            last_error = v_error_message,
            result_json = COALESCE(failed_work.result_json, '{}'::jsonb)
              || pg_catalog.jsonb_build_object(
                'code', CASE WHEN failed_work.attempt_count >= 5 THEN 'FAILED_FINAL' ELSE 'FAILED_RETRYABLE' END,
                'sqlstate', v_error_state, 'message', v_error_message
              )
        WHERE failed_work.id = v_work.id;
      END;
    END LOOP;

    UPDATE public.pay_payment_correction_requests AS processing_request
    SET status = 'PROCESSING', updated_at_utc = v_now
    WHERE processing_request.id = p_correction_request_id
      AND processing_request.status IN ('EXPANDED', 'PROCESSING');

    SELECT
      pg_catalog.count(*) FILTER (WHERE work_row.status = 'APPLIED')::integer,
      pg_catalog.count(*) FILTER (WHERE work_row.status = 'BLOCKED')::integer,
      pg_catalog.count(*) FILTER (WHERE work_row.status = 'FAILED_FINAL')::integer,
      pg_catalog.count(*) FILTER (WHERE work_row.status = 'FAILED_RETRYABLE')::integer,
      pg_catalog.count(*) FILTER (WHERE work_row.status IN ('PENDING','PROCESSING','FAILED_RETRYABLE'))::integer
    INTO v_applied_count, v_blocked_count, v_failed_count, v_retryable_count, v_nonterminal_count
    FROM public.pay_payment_correction_work_items AS work_row
    WHERE work_row.correction_request_id = p_correction_request_id;

    UPDATE public.banking_pay_operations AS processing_operation
    SET phase = CASE WHEN v_nonterminal_count = 0 THEN 'FINALISE' ELSE 'PROCESS_CHUNKS' END,
        status = 'RUNNING', runner_state = 'RUNNABLE', run_after_utc = v_now,
        completed_units = v_applied_count + v_blocked_count + v_failed_count,
        failed_units = v_blocked_count + v_failed_count,
        progress_json = COALESCE(processing_operation.progress_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'applied', v_applied_count, 'blocked', v_blocked_count,
            'failed_final', v_failed_count, 'failed_retryable', v_retryable_count
          ),
        updated_at_utc = v_now
    WHERE processing_operation.id = v_operation.id;

    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'correction_request_id', p_correction_request_id,
      'operation_id', v_operation.id, 'phase', CASE WHEN v_nonterminal_count = 0 THEN 'FINALISE' ELSE 'PROCESS_CHUNKS' END,
      'claimed_count', v_claimed_count, 'applied_count', v_applied_count,
      'blocked_count', v_blocked_count, 'failed_count', v_failed_count,
      'retryable_count', v_retryable_count,
      'processed', v_claimed_count,
      'applied', v_applied_count,
      'skipped', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS skipped_work
        WHERE skipped_work.correction_request_id = p_correction_request_id
          AND skipped_work.status = 'SKIPPED'
      ),
      'blocked', v_blocked_count,
      'failed_retryable', v_retryable_count,
      'failed_final', v_failed_count,
      'pending', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS pending_work
        WHERE pending_work.correction_request_id = p_correction_request_id
          AND pending_work.status = 'PENDING'
      ),
      'processing', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS active_work
        WHERE active_work.correction_request_id = p_correction_request_id
          AND active_work.status = 'PROCESSING'
      ),
      'progress_completed', v_applied_count + v_blocked_count + v_failed_count + (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS skipped_work
        WHERE skipped_work.correction_request_id = p_correction_request_id
          AND skipped_work.status = 'SKIPPED'
      ),
      'progress_total', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS all_work
        WHERE all_work.correction_request_id = p_correction_request_id
      ),
      'parent_status', 'PROCESSING',
      'totals', pg_catalog.jsonb_build_object(
        'applied', v_applied_count,
        'blocked', v_blocked_count,
        'failed_retryable', v_retryable_count,
        'failed_final', v_failed_count,
        'nonterminal', v_nonterminal_count
      ),
      'complete', false,
      'requires_user_action', false,
      'processing_continues', v_nonterminal_count > 0,
      'changed_scope_json', pg_catalog.jsonb_build_object(
        'pay_batch_id', v_request.pay_batch_id,
        'applied_candidate_count', v_applied_count
      ),
      'live_signal_updates', pg_catalog.jsonb_build_object(
        'overview_updated', v_applied_count > 0,
        'payment_status_updated', v_applied_count > 0
      ),
      'code', 'PAYMENT_CORRECTION_PROCESS_PAGE_COMPLETE'
    );
  END IF;

  IF v_phase = 'FINALISE' THEN
    IF EXISTS (
      SELECT 1 FROM public.pay_payment_correction_work_items AS nonterminal_work
      WHERE nonterminal_work.correction_request_id = p_correction_request_id
        AND nonterminal_work.status IN ('PENDING','PROCESSING','FAILED_RETRYABLE')
    ) THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_NONTERMINAL_WORK_REMAINS'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'NONTERMINAL_WORK_REMAINS')::text;
    END IF;

    v_summary_cursor := COALESCE(
      NULLIF(v_operation.progress_json->>'last_finalised_selection_ordinal', '')::bigint, 0
    );

    WITH summary_page AS (
      SELECT member_row.selection_ordinal, work_row.status
      FROM public.pay_payment_correction_request_candidates AS member_row
      JOIN public.pay_payment_correction_work_items AS work_row
        ON work_row.correction_request_id = member_row.correction_request_id
       AND work_row.pay_batch_candidate_id = member_row.pay_batch_candidate_id
       AND work_row.selection_hash = member_row.candidate_scope_hash
      WHERE member_row.correction_request_id = p_correction_request_id
        AND member_row.selection_ordinal > v_summary_cursor
      ORDER BY member_row.selection_ordinal
      LIMIT 100
    ), page_result AS (
      SELECT pg_catalog.count(*)::integer AS terminal_count,
             pg_catalog.count(*) FILTER (WHERE status = 'APPLIED')::integer AS applied_count,
             pg_catalog.count(*) FILTER (WHERE status IN ('BLOCKED','SKIPPED'))::integer AS blocked_count,
             pg_catalog.count(*) FILTER (WHERE status = 'FAILED_FINAL')::integer AS failed_count,
             pg_catalog.count(*) FILTER (WHERE status = 'CANCELLED')::integer AS cancelled_count,
             COALESCE(pg_catalog.max(selection_ordinal), v_summary_cursor) AS next_ordinal
      FROM summary_page
    )
    SELECT terminal_count, applied_count, blocked_count, failed_count, cancelled_count, next_ordinal
    INTO v_summary_count, v_applied_count, v_blocked_count, v_failed_count, v_total_cancelled, v_summary_next
    FROM page_result;

    IF v_summary_count > 0 THEN
      INSERT INTO public.banking_pay_operation_chunks (
        operation_id, phase, chunk_type, sequence_no, status, payload_json, result_json,
        error_json, unit_count, completed_count, failed_count, started_at_utc, completed_at_utc
      ) VALUES (
        v_operation.id, 'FINALISE', 'CANDIDATE_SCOPE', (v_summary_cursor / 100)::integer + 1,
        'COMPLETE', pg_catalog.jsonb_build_object('after_selection_ordinal', v_summary_cursor),
        pg_catalog.jsonb_build_object(
          'last_selection_ordinal', v_summary_next, 'terminal_count', v_summary_count,
          'applied_count', v_applied_count, 'blocked_count', v_blocked_count,
          'failed_count', v_failed_count, 'cancelled_count', v_total_cancelled
        ), NULL, v_summary_count, v_summary_count, v_blocked_count + v_failed_count, v_now, v_now
      ) ON CONFLICT (operation_id, phase, chunk_type, sequence_no) DO NOTHING;
    END IF;

    v_summary_has_more := EXISTS (
      SELECT 1 FROM public.pay_payment_correction_request_candidates AS remaining_member
      WHERE remaining_member.correction_request_id = p_correction_request_id
        AND remaining_member.selection_ordinal > v_summary_next
    );

    IF v_summary_has_more THEN
      UPDATE public.banking_pay_operations AS summary_operation
      SET progress_json = COALESCE(summary_operation.progress_json, '{}'::jsonb)
            || pg_catalog.jsonb_build_object('last_finalised_selection_ordinal', v_summary_next),
          run_after_utc = v_now, updated_at_utc = v_now
      WHERE summary_operation.id = v_operation.id;
      RETURN pg_catalog.jsonb_build_object(
        'ok', true, 'phase', 'FINALISE', 'summary_count', v_summary_count,
        'last_selection_ordinal', v_summary_next, 'complete', false,
        'processed', v_summary_count,
        'applied', v_applied_count,
        'skipped', v_total_cancelled,
        'blocked', v_blocked_count,
        'failed_retryable', 0,
        'failed_final', v_failed_count,
        'pending', 0,
        'processing', 0,
        'progress_completed', v_summary_next,
        'progress_total', (
          SELECT pg_catalog.count(*)::integer
          FROM public.pay_payment_correction_request_candidates AS all_member
          WHERE all_member.correction_request_id = p_correction_request_id
        ),
        'parent_status', v_request.status,
        'totals', pg_catalog.jsonb_build_object(
          'page_terminal', v_summary_count,
          'page_applied', v_applied_count,
          'page_blocked', v_blocked_count,
          'page_failed_final', v_failed_count,
          'page_cancelled', v_total_cancelled
        ),
        'requires_user_action', false,
        'processing_continues', true,
        'changed_scope_json', '{}'::jsonb,
        'live_signal_updates', '{}'::jsonb,
        'code', 'PAYMENT_CORRECTION_FINALISE_SUMMARY_COMPLETE'
      );
    END IF;

    SELECT
      COALESCE(pg_catalog.sum((chunk_row.result_json->>'terminal_count')::integer), 0)::integer,
      COALESCE(pg_catalog.sum((chunk_row.result_json->>'applied_count')::integer), 0)::integer,
      COALESCE(pg_catalog.sum((chunk_row.result_json->>'blocked_count')::integer), 0)::integer,
      COALESCE(pg_catalog.sum((chunk_row.result_json->>'failed_count')::integer), 0)::integer,
      COALESCE(pg_catalog.sum((chunk_row.result_json->>'cancelled_count')::integer), 0)::integer
    INTO v_total_terminal, v_total_applied, v_total_blocked, v_total_failed, v_total_cancelled
    FROM public.banking_pay_operation_chunks AS chunk_row
    WHERE chunk_row.operation_id = v_operation.id
      AND chunk_row.phase = 'FINALISE'
      AND chunk_row.chunk_type = 'CANDIDATE_SCOPE'
      AND chunk_row.status = 'COMPLETE';

    IF v_total_terminal = 0 THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_FINALISE_ZERO_WORK'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'WORK_MEMBERSHIP_MISMATCH')::text;
    END IF;

    v_request_result := CASE
      WHEN v_total_applied = v_total_terminal THEN 'APPLIED'
      WHEN v_total_applied > 0 THEN 'APPLIED_WITH_BLOCKERS'
      WHEN v_total_failed > 0 AND v_total_blocked = 0 AND v_total_cancelled = 0 THEN 'FAILED'
      WHEN v_total_cancelled = v_total_terminal THEN 'CANCELLED'
      ELSE 'BLOCKED'
    END;

    WITH active_items AS (
      SELECT active_item.*
      FROM public.pay_batch_items AS active_item
      JOIN public.pay_batch_candidates AS active_candidate
        ON active_candidate.id = active_item.pay_batch_candidate_id
      WHERE active_candidate.pay_batch_id = v_request.pay_batch_id
        AND COALESCE(active_item.is_voided, false) IS NOT TRUE
        AND active_item.item_type <> 'DEBT_CREATED'
    ), active_candidates AS (
      SELECT DISTINCT active_candidate.id, active_candidate.net_bank_amount
      FROM public.pay_batch_candidates AS active_candidate
      JOIN active_items AS active_item
        ON active_item.pay_batch_candidate_id = active_candidate.id
    ), active_paye_candidates AS (
      SELECT DISTINCT active_candidate.id, active_candidate.net_bank_amount
      FROM active_candidates AS active_candidate
      JOIN active_items AS active_item
        ON active_item.pay_batch_candidate_id = active_candidate.id
      WHERE pg_catalog.upper(COALESCE(active_item.pay_channel, '')) = 'PAYE'
    ), active_transfers AS (
      SELECT DISTINCT transfer_row.id, transfer_row.amount
      FROM public.pay_bank_transfers AS transfer_row
      JOIN active_items AS active_item ON active_item.pay_bank_transfer_id = transfer_row.id
    )
    SELECT
      (SELECT pg_catalog.count(*)::integer FROM active_items),
      (SELECT pg_catalog.count(*)::integer FROM active_candidates),
      (SELECT COALESCE(pg_catalog.sum(active_item.amount_inc_vat), 0)::numeric(14,2) FROM active_items AS active_item),
      (SELECT COALESCE(pg_catalog.sum(active_candidate.net_bank_amount), 0)::numeric(14,2) FROM active_candidates AS active_candidate),
      (SELECT pg_catalog.count(*)::integer FROM active_paye_candidates),
      (SELECT COALESCE(pg_catalog.sum(active_candidate.net_bank_amount), 0)::numeric(14,2) FROM active_paye_candidates AS active_candidate),
      (SELECT COALESCE(pg_catalog.sum(active_transfer.amount), 0)::numeric(14,2) FROM active_transfers AS active_transfer)
    INTO v_active_item_count, v_active_candidate_count,
         v_active_source_item_amount, v_active_net_bank_amount,
         v_active_paye_candidate_count, v_active_paye_schedule_amount,
         v_active_transfer_amount;

    v_unselected_scope_hash_before := v_request.plan_json->>'unselected_scope_hash_before';

    v_unselected_scope_hash_after := private.pay_payment_correction_sha256_v1(
      pg_catalog.jsonb_build_object(
        'version', 1,
        'pay_batch_id', v_request.pay_batch_id,
        'unselected_candidates', COALESCE((
          SELECT pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
              'pay_batch_candidate_id', candidate_scope.id,
              'candidate_id', candidate_scope.candidate_id,
              'net_bank_amount_pence', pg_catalog.round(COALESCE(candidate_scope.net_bank_amount, 0) * 100)::bigint,
              'settlement_status', candidate_scope.settlement_status,
              'items', COALESCE((
                SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                  'id', item_scope.id, 'item_type', item_scope.item_type,
                  'is_voided', COALESCE(item_scope.is_voided, false),
                  'amount_ex_vat_pence', pg_catalog.round(COALESCE(item_scope.amount_ex_vat, 0) * 100)::bigint,
                  'amount_vat_pence', pg_catalog.round(COALESCE(item_scope.amount_vat, 0) * 100)::bigint,
                  'amount_inc_vat_pence', pg_catalog.round(COALESCE(item_scope.amount_inc_vat, 0) * 100)::bigint,
                  'reservation_id', item_scope.reservation_id,
                  'finance_component_id', item_scope.finance_component_id,
                  'pay_bank_transfer_id', item_scope.pay_bank_transfer_id,
                  'operation_source_key', item_scope.operation_source_key
                ) ORDER BY item_scope.id)
                FROM public.pay_batch_items AS item_scope
                WHERE item_scope.pay_batch_candidate_id = candidate_scope.id
              ), '[]'::jsonb),
              'reservations', COALESCE((
                SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                  'id', reservation_scope.id,
                  'pay_batch_item_id', reservation_scope.pay_batch_item_id,
                  'status', reservation_scope.status,
                  'reserved_amount_pence', pg_catalog.round(COALESCE(reservation_scope.reserved_amount, 0) * 100)::bigint,
                  'committed_at_utc', reservation_scope.committed_at_utc,
                  'settled_at_utc', reservation_scope.settled_at_utc,
                  'released_at_utc', reservation_scope.released_at_utc
                ) ORDER BY reservation_scope.id)
                FROM public.pay_advance_reservations AS reservation_scope
                JOIN public.pay_batch_items AS reservation_item ON reservation_item.id = reservation_scope.pay_batch_item_id
                WHERE reservation_item.pay_batch_candidate_id = candidate_scope.id
              ), '[]'::jsonb),
              'transfers', COALESCE((
                SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                  'id', transfer_scope.id, 'status', transfer_scope.status,
                  'rail_state', transfer_scope.rail_state,
                  'request_id', transfer_scope.request_id,
                  'rail_tx_id', transfer_scope.rail_tx_id,
                  'transfer_group_key', transfer_scope.transfer_group_key,
                  'amount_pence', pg_catalog.round(COALESCE(transfer_scope.amount, 0) * 100)::bigint
                ) ORDER BY transfer_scope.id)
                FROM public.pay_bank_transfers AS transfer_scope
                WHERE transfer_scope.id IN (
                  SELECT item_scope.pay_bank_transfer_id
                  FROM public.pay_batch_items AS item_scope
                  WHERE item_scope.pay_batch_candidate_id = candidate_scope.id
                    AND item_scope.pay_bank_transfer_id IS NOT NULL
                )
              ), '[]'::jsonb)
            ) ORDER BY candidate_scope.id
          )
          FROM public.pay_batch_candidates AS candidate_scope
          WHERE candidate_scope.pay_batch_id = v_request.pay_batch_id
            AND NOT EXISTS (
              SELECT 1 FROM public.pay_payment_correction_request_candidates AS selected_scope
              WHERE selected_scope.correction_request_id = p_correction_request_id
                AND selected_scope.pay_batch_candidate_id = candidate_scope.id
            )
        ), '[]'::jsonb)
      )
    );

    SELECT pg_catalog.count(*)::integer
    INTO v_blocked_scope_mismatch_count
    FROM public.pay_payment_correction_request_candidates AS member_scope
    JOIN public.pay_batch_candidates AS candidate_scope
      ON candidate_scope.id = member_scope.pay_batch_candidate_id
    JOIN public.pay_payment_correction_work_items AS work_scope
      ON work_scope.correction_request_id = member_scope.correction_request_id
     AND work_scope.pay_batch_candidate_id = member_scope.pay_batch_candidate_id
    WHERE member_scope.correction_request_id = p_correction_request_id
      AND work_scope.status <> 'APPLIED'
      AND (
        candidate_scope.net_bank_amount IS DISTINCT FROM member_scope.active_amount
        OR EXISTS (
          SELECT 1 FROM public.pay_batch_items AS selected_item
          WHERE selected_item.id = ANY(member_scope.pay_batch_item_ids)
            AND COALESCE(selected_item.is_voided, false)
        )
        OR (
          SELECT pg_catalog.count(*)::integer
          FROM public.pay_batch_items AS selected_item
          WHERE selected_item.id = ANY(member_scope.pay_batch_item_ids)
            AND COALESCE(selected_item.is_voided, false) IS NOT TRUE
        ) <> member_scope.active_item_count
        OR EXISTS (
          SELECT 1 FROM public.pay_payment_correction_items AS applied_item
          WHERE applied_item.correction_request_id = p_correction_request_id
            AND applied_item.pay_batch_candidate_id = member_scope.pay_batch_candidate_id
            AND applied_item.status = 'APPLIED'
        )
        OR private.pay_payment_correction_sha256_v1(
          pg_catalog.jsonb_build_object(
            'version', 1,
            'pay_batch_candidate_id', member_scope.pay_batch_candidate_id,
            'candidate_id', candidate_scope.candidate_id,
            'requested_action', v_requested_action,
            'pay_batch_item_ids', pg_catalog.to_jsonb(member_scope.pay_batch_item_ids),
            'item_count', member_scope.active_item_count,
            'source_row_count', member_scope.source_row_count,
            'active_amount_pence', pg_catalog.round(COALESCE(candidate_scope.net_bank_amount, 0) * 100)::bigint,
            'item_contract', (
              SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                'id', item_scope.id, 'item_type', item_scope.item_type,
                'amount_ex_vat_pence', pg_catalog.round(COALESCE(item_scope.amount_ex_vat, 0) * 100)::bigint,
                'amount_vat_pence', pg_catalog.round(COALESCE(item_scope.amount_vat, 0) * 100)::bigint,
                'amount_inc_vat_pence', pg_catalog.round(COALESCE(item_scope.amount_inc_vat, 0) * 100)::bigint,
                'reservation_id', item_scope.reservation_id,
                'finance_component_id', item_scope.finance_component_id,
                'pay_bank_transfer_id', item_scope.pay_bank_transfer_id,
                'operation_source_key', item_scope.operation_source_key,
                'frozen_component_snapshot_json', item_scope.frozen_component_snapshot_json,
                'frozen_source_basis_json', item_scope.frozen_source_basis_json
              ) ORDER BY item_scope.id)
              FROM public.pay_batch_items AS item_scope
              WHERE item_scope.id = ANY(member_scope.pay_batch_item_ids)
                AND COALESCE(item_scope.is_voided, false) IS NOT TRUE
            ),
            'shared_instruction_scope_hash', member_scope.shared_instruction_scope_hash,
            'eligibility_code', member_scope.eligibility_code_at_plan
          )
        ) IS DISTINCT FROM member_scope.candidate_scope_hash
      );

    IF v_unselected_scope_hash_before IS NULL
       OR v_unselected_scope_hash_after IS DISTINCT FROM v_unselected_scope_hash_before
       OR v_blocked_scope_mismatch_count > 0 THEN
      v_final_result := pg_catalog.jsonb_build_object(
        'request_status', 'FAILED',
        'code', 'PAYMENT_CORRECTION_SCOPE_INTEGRITY_CONFLICT',
        'unselected_scope_hash_before', v_unselected_scope_hash_before,
        'unselected_scope_hash_after', v_unselected_scope_hash_after,
        'unselected_unchanged', v_unselected_scope_hash_before IS NOT NULL
          AND v_unselected_scope_hash_after = v_unselected_scope_hash_before,
        'blocked_scope_mismatch_count', v_blocked_scope_mismatch_count,
        'applied_candidate_count', v_total_applied,
        'blocked_candidate_count', v_total_blocked,
        'failed_candidate_count', v_total_failed,
        'user_message', 'The remaining payment scope changed during cancellation. CloudTMS has stopped normal completion and requires review.'
      );
      v_final_result_hash := private.pay_payment_correction_sha256_v1(v_final_result);

      UPDATE public.pay_batches AS integrity_batch
      SET status = CASE WHEN v_active_item_count = 0 THEN 'CANCELLED' ELSE 'AWAITING_AUTHORISATION' END,
          total_bank_out = v_active_net_bank_amount,
          schedule_kind = NULL, scheduled_at_utc = NULL, scheduled_by_user_id = NULL,
          funding_account_ref = NULL, funds_warning_hours_json = NULL
      WHERE integrity_batch.id = v_request.pay_batch_id;

      UPDATE public.pay_payment_correction_requests AS integrity_request
      SET status = 'FAILED',
          applied_at_utc = CASE WHEN v_total_applied > 0 THEN COALESCE(integrity_request.applied_at_utc, v_now) ELSE integrity_request.applied_at_utc END,
          plan_json = COALESCE(integrity_request.plan_json, '{}'::jsonb)
            || pg_catalog.jsonb_build_object(
              'unselected_scope_hash_after', v_unselected_scope_hash_after,
              'blocked_scope_mismatch_count', v_blocked_scope_mismatch_count,
              'final_result', v_final_result,
              'final_result_hash', v_final_result_hash,
              'finalised_at_utc', v_now
            ),
          updated_at_utc = v_now
      WHERE integrity_request.id = p_correction_request_id;

      UPDATE public.banking_pay_operations AS integrity_operation
      SET phase = 'REFRESH_WORKBENCH', status = 'RUNNING', runner_state = 'RUNNABLE',
          requires_user_action = true, run_after_utc = v_now,
          result_json = COALESCE(integrity_operation.result_json, '{}'::jsonb) || v_final_result,
          error_json = pg_catalog.jsonb_build_object('code', 'PAYMENT_CORRECTION_SCOPE_INTEGRITY_CONFLICT'),
          progress_json = COALESCE(integrity_operation.progress_json, '{}'::jsonb)
            || pg_catalog.jsonb_build_object('financial_finalised', false, 'integrity_review_required', true),
          updated_at_utc = v_now
      WHERE integrity_operation.id = v_operation.id;

      INSERT INTO public.pay_payment_correction_actions (
        correction_request_id, pay_batch_id, actor_kind, actor_user_id, action,
        action_at_utc, note, before_json, after_json, metadata_json
      ) VALUES (
        p_correction_request_id, v_request.pay_batch_id, 'SYSTEM', p_actor_user_id,
        'APPLY', v_now, 'Correction scope integrity conflict.',
        pg_catalog.jsonb_build_object('unselected_scope_hash_before', v_unselected_scope_hash_before),
        v_final_result,
        pg_catalog.jsonb_build_object('code', 'PAYMENT_CORRECTION_SCOPE_INTEGRITY_CONFLICT', 'final_result_hash', v_final_result_hash)
      );

      PERFORM public.pay_batch_display_summary_refresh(v_request.pay_batch_id);
      RETURN pg_catalog.jsonb_build_object(
        'ok', false, 'phase', 'REFRESH_WORKBENCH', 'financial_complete', false,
        'request_status', 'FAILED', 'result', v_final_result,
        'complete', false, 'requires_user_action', true,
        'processing_continues', true,
        'code', 'PAYMENT_CORRECTION_SCOPE_INTEGRITY_CONFLICT'
      );
    END IF;

    UPDATE public.pay_batches AS reconciled_batch
    SET status = CASE
          WHEN v_active_item_count = 0 THEN 'CANCELLED'
          WHEN v_requested_action = 'DRAFT_CANCEL' THEN 'DRAFT'
          ELSE 'AWAITING_AUTHORISATION'
        END,
        cancelled_at_utc = CASE WHEN v_active_item_count = 0 THEN COALESCE(reconciled_batch.cancelled_at_utc, v_now) ELSE reconciled_batch.cancelled_at_utc END,
        cancelled_by_user_id = CASE WHEN v_active_item_count = 0 THEN COALESCE(reconciled_batch.cancelled_by_user_id, p_actor_user_id) ELSE reconciled_batch.cancelled_by_user_id END,
        cancel_reason = CASE WHEN v_active_item_count = 0 THEN COALESCE(reconciled_batch.cancel_reason, v_request.reason) ELSE reconciled_batch.cancel_reason END,
        schedule_kind = CASE WHEN v_requested_action = 'DRAFT_CANCEL' THEN reconciled_batch.schedule_kind ELSE NULL END,
        scheduled_at_utc = CASE WHEN v_requested_action = 'DRAFT_CANCEL' THEN reconciled_batch.scheduled_at_utc ELSE NULL END,
        scheduled_by_user_id = CASE WHEN v_requested_action = 'DRAFT_CANCEL' THEN reconciled_batch.scheduled_by_user_id ELSE NULL END,
        funding_account_ref = CASE WHEN v_requested_action = 'DRAFT_CANCEL' THEN reconciled_batch.funding_account_ref ELSE NULL END,
        funds_warning_hours_json = CASE WHEN v_requested_action = 'DRAFT_CANCEL' THEN reconciled_batch.funds_warning_hours_json ELSE NULL END,
        total_bank_out = v_active_net_bank_amount
    WHERE reconciled_batch.id = v_request.pay_batch_id;

    v_final_result := pg_catalog.jsonb_build_object(
      'request_status', v_request_result, 'selected_candidate_count', v_total_terminal,
      'applied_candidate_count', v_total_applied, 'blocked_candidate_count', v_total_blocked,
      'failed_candidate_count', v_total_failed, 'cancelled_candidate_count', v_total_cancelled,
      'active_overview_candidate_count', v_active_candidate_count,
      'active_overview_amount_pence', pg_catalog.round(v_active_net_bank_amount * 100)::bigint,
      'active_candidate_net_bank_amount_pence', pg_catalog.round(v_active_net_bank_amount * 100)::bigint,
      'active_frozen_source_item_amount_pence', pg_catalog.round(v_active_source_item_amount * 100)::bigint,
      'active_paye_schedule_candidate_count', v_active_paye_candidate_count,
      'active_paye_schedule_amount_pence', pg_catalog.round(v_active_paye_schedule_amount * 100)::bigint,
      'active_transfer_amount_pence', pg_catalog.round(v_active_transfer_amount * 100)::bigint,
      'batch_total_bank_out_pence', pg_catalog.round(v_active_net_bank_amount * 100)::bigint,
      'active_paye_schedule_derived_from_unvoided_frozen_items', true,
      'unselected_scope_hash_before', v_unselected_scope_hash_before,
      'unselected_scope_hash_after', v_unselected_scope_hash_after,
      'unselected_unchanged', true,
      'blocked_scope_mismatch_count', 0,
      'reauthorisation_required', v_active_item_count > 0 AND v_requested_action <> 'DRAFT_CANCEL'
    );
    v_final_result_hash := private.pay_payment_correction_sha256_v1(v_final_result);

    UPDATE public.pay_payment_correction_requests AS final_request
    SET status = v_request_result,
        applied_at_utc = CASE WHEN v_total_applied > 0 THEN COALESCE(final_request.applied_at_utc, v_now) ELSE final_request.applied_at_utc END,
        cancelled_at_utc = CASE WHEN v_request_result = 'CANCELLED' THEN COALESCE(final_request.cancelled_at_utc, v_now) ELSE final_request.cancelled_at_utc END,
        plan_json = COALESCE(final_request.plan_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object('final_result', v_final_result, 'final_result_hash', v_final_result_hash, 'finalised_at_utc', v_now),
        updated_at_utc = v_now
    WHERE final_request.id = p_correction_request_id;

    UPDATE public.banking_pay_operations AS final_operation
    SET phase = 'REFRESH_WORKBENCH', status = 'RUNNING', runner_state = 'RUNNABLE',
        requires_user_action = false, run_after_utc = v_now,
        result_json = COALESCE(final_operation.result_json, '{}'::jsonb) || v_final_result,
        progress_json = COALESCE(final_operation.progress_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object('last_finalised_selection_ordinal', v_summary_next, 'financial_finalised', true),
        updated_at_utc = v_now
    WHERE final_operation.id = v_operation.id;

    PERFORM public.pay_batch_display_summary_refresh(v_request.pay_batch_id);
    PERFORM public.banking_pay_batch_signal_touch(
      p_pay_batch_id := v_request.pay_batch_id,
      p_change_reason := 'PAYMENT_CORRECTION_FINALISED',
      p_change_source := 'pay_payment_correction_process_chunk',
      p_change_scope_json := v_final_result,
      p_touch_payment_status := true,
      p_touch_correction_progress := true,
      p_touch_alerts := false,
      p_touch_overview := true
    );

    INSERT INTO public.pay_payment_correction_actions (
      correction_request_id, pay_batch_id, actor_kind, actor_user_id, action,
      action_at_utc, note, before_json, after_json, metadata_json
    ) VALUES (
      p_correction_request_id, v_request.pay_batch_id, 'SYSTEM', p_actor_user_id,
      'APPLY', v_now, 'Correction financial finalisation complete.', NULL,
      v_final_result, pg_catalog.jsonb_build_object('final_result_hash', v_final_result_hash)
    );

    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'phase', 'REFRESH_WORKBENCH', 'financial_complete', true,
      'request_status', v_request_result, 'result', v_final_result,
      'processed', v_total_terminal,
      'applied', v_total_applied,
      'skipped', v_total_cancelled,
      'blocked', v_total_blocked,
      'failed_retryable', 0,
      'failed_final', v_total_failed,
      'pending', 0,
      'processing', 0,
      'progress_completed', v_total_terminal,
      'progress_total', v_total_terminal,
      'parent_status', v_request_result,
      'totals', v_final_result,
      'complete', false,
      'requires_user_action', v_request_result IN (
        'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED'
      ),
      'processing_continues', true,
      'changed_scope_json', pg_catalog.jsonb_build_object(
        'pay_batch_id', v_request.pay_batch_id,
        'active_candidate_count', v_active_candidate_count,
        'active_item_count', v_active_item_count,
        'active_net_bank_amount', v_active_net_bank_amount,
        'active_frozen_source_item_amount', v_active_source_item_amount,
        'active_paye_schedule_amount', v_active_paye_schedule_amount,
        'active_transfer_amount', v_active_transfer_amount
      ),
      'live_signal_updates', pg_catalog.jsonb_build_object(
        'overview_updated', true,
        'payment_status_updated', true,
        'paye_schedule_updated',
          pg_catalog.upper(COALESCE(v_batch.batch_kind_fixed, '')) = 'PAYE'
      ),
      'code', 'PAYMENT_CORRECTION_FINALISED'
    );
  END IF;

  IF v_phase = 'REFRESH_WORKBENCH' THEN
    v_refresh_cursor := COALESCE(
      NULLIF(v_operation.progress_json->>'last_refreshed_selection_ordinal', '')::bigint, 0
    );

    WITH refresh_page AS (
      SELECT member_row.selection_ordinal, work_row.candidate_id
      FROM public.pay_payment_correction_request_candidates AS member_row
      JOIN public.pay_payment_correction_work_items AS work_row
        ON work_row.correction_request_id = member_row.correction_request_id
       AND work_row.pay_batch_candidate_id = member_row.pay_batch_candidate_id
      WHERE member_row.correction_request_id = p_correction_request_id
        AND member_row.selection_ordinal > v_refresh_cursor
        AND work_row.status = 'APPLIED'
      ORDER BY member_row.selection_ordinal
      LIMIT 100
    )
    SELECT COALESCE(pg_catalog.jsonb_agg(candidate_id ORDER BY selection_ordinal), '[]'::jsonb),
           pg_catalog.count(*)::integer,
           COALESCE(pg_catalog.max(selection_ordinal), v_refresh_cursor)
    INTO v_refresh_candidate_ids, v_refresh_count, v_refresh_next
    FROM refresh_page;

    v_refresh_has_more := EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_request_candidates AS member_row
      JOIN public.pay_payment_correction_work_items AS work_row
        ON work_row.correction_request_id = member_row.correction_request_id
       AND work_row.pay_batch_candidate_id = member_row.pay_batch_candidate_id
      WHERE member_row.correction_request_id = p_correction_request_id
        AND member_row.selection_ordinal > v_refresh_next
        AND work_row.status = 'APPLIED'
    );
    v_refresh_sequence := (v_refresh_cursor / 100)::integer + 1;
    v_session_id := v_batch.source_workbench_session_id;
    IF v_session_id IS NOT NULL THEN
      SELECT session_row.actor_user_id
      INTO v_refresh_actor_user_id
      FROM public.banking_pay_workbench_sessions AS session_row
      WHERE session_row.id = v_session_id
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(session_row.status, ''))) = 'OPEN'
        AND session_row.discarded_at_utc IS NULL;
    END IF;

    IF EXISTS (
      SELECT 1 FROM public.banking_pay_operation_chunks AS existing_refresh
      WHERE existing_refresh.operation_id = v_operation.id
        AND existing_refresh.phase = 'REFRESH_WORKBENCH'
        AND existing_refresh.chunk_type = 'CANDIDATE_SCOPE'
        AND existing_refresh.sequence_no = v_refresh_sequence
        AND existing_refresh.status = 'COMPLETE'
    ) THEN
      SELECT existing_refresh.result_json INTO v_refresh_result
      FROM public.banking_pay_operation_chunks AS existing_refresh
      WHERE existing_refresh.operation_id = v_operation.id
        AND existing_refresh.phase = 'REFRESH_WORKBENCH'
        AND existing_refresh.chunk_type = 'CANDIDATE_SCOPE'
        AND existing_refresh.sequence_no = v_refresh_sequence;
    ELSIF v_refresh_count = 0 THEN
      v_refresh_result := pg_catalog.jsonb_build_object('status', 'NOT_REQUIRED', 'candidate_count', 0);
    ELSIF v_session_id IS NULL THEN
      v_refresh_result := pg_catalog.jsonb_build_object(
        'status', 'NOT_REQUIRED_NO_SOURCE_SESSION',
        'candidate_count', v_refresh_count,
        'scope_signal_recorded', true
      );
    ELSIF v_refresh_actor_user_id IS NULL THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_WORKBENCH_SESSION_NOT_OPEN'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'REFRESH_RETRY',
          'session_id', v_session_id
        )::text;
    ELSIF v_requested_action = 'DRAFT_CANCEL' THEN
      v_refresh_result := public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1(
        v_session_id, v_request.pay_batch_id, 'PAYMENT_CORRECTION_DRAFT_CANCEL', v_refresh_actor_user_id,
        pg_catalog.jsonb_build_object(
          'correction_request_id', p_correction_request_id,
          'candidate_ids', v_refresh_candidate_ids,
          'maximum_candidate_count', 100
        )
      );
    ELSE
      v_refresh_result := public.pay_workbench_enqueue_candidate_refresh_many(
        v_session_id, v_refresh_candidate_ids, 'PAYMENT_CORRECTION_FINALISED', v_refresh_actor_user_id
      );
    END IF;

    INSERT INTO public.banking_pay_operation_chunks (
      operation_id, phase, chunk_type, sequence_no, status, payload_json, result_json,
      error_json, unit_count, completed_count, failed_count, started_at_utc, completed_at_utc
    ) VALUES (
      v_operation.id, 'REFRESH_WORKBENCH', 'CANDIDATE_SCOPE', v_refresh_sequence,
      'COMPLETE', pg_catalog.jsonb_build_object('candidate_ids', v_refresh_candidate_ids),
      COALESCE(v_refresh_result, '{}'::jsonb), NULL,
      v_refresh_count, v_refresh_count, 0, v_now, v_now
    ) ON CONFLICT (operation_id, phase, chunk_type, sequence_no) DO NOTHING;

    UPDATE public.banking_pay_operations AS refresh_operation
    SET progress_json = COALESCE(refresh_operation.progress_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'last_refreshed_selection_ordinal', v_refresh_next,
            'workbench_refresh_status', CASE
              WHEN v_refresh_count = 0
                OR COALESCE(v_refresh_result->>'status', '') LIKE 'NOT_REQUIRED%'
                THEN 'CURRENT'
              ELSE 'STAGED'
            END
          ),
        phase = CASE WHEN v_refresh_has_more THEN 'REFRESH_WORKBENCH' ELSE 'COMPLETE' END,
        status = CASE WHEN v_refresh_has_more THEN 'RUNNING' ELSE 'COMPLETE' END,
        runner_state = CASE WHEN v_refresh_has_more THEN 'RUNNABLE' ELSE 'COMPLETE' END,
        run_after_utc = CASE WHEN v_refresh_has_more THEN v_now ELSE NULL END,
        completed_at_utc = CASE WHEN v_refresh_has_more THEN refresh_operation.completed_at_utc ELSE v_now END,
        updated_at_utc = v_now
    WHERE refresh_operation.id = v_operation.id;

    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'phase', CASE WHEN v_refresh_has_more THEN 'REFRESH_WORKBENCH' ELSE 'COMPLETE' END,
      'candidate_count', v_refresh_count, 'complete', NOT v_refresh_has_more,
      'workbench_refresh', v_refresh_result,
      'processed', v_refresh_count,
      'applied', 0,
      'skipped', 0,
      'blocked', 0,
      'failed_retryable', 0,
      'failed_final', 0,
      'pending', 0,
      'processing', 0,
      'progress_completed', v_refresh_next,
      'progress_total', (
        SELECT pg_catalog.count(*)::integer
        FROM public.pay_payment_correction_work_items AS applied_work
        WHERE applied_work.correction_request_id = p_correction_request_id
          AND applied_work.status = 'APPLIED'
      ),
      'parent_status', v_request.status,
      'totals', pg_catalog.jsonb_build_object(
        'refresh_candidate_count', v_refresh_count,
        'refresh_has_more', v_refresh_has_more
      ),
      'requires_user_action', v_operation.requires_user_action,
      'processing_continues', v_refresh_has_more,
      'changed_scope_json', '{}'::jsonb,
      'live_signal_updates', pg_catalog.jsonb_build_object(
        'workbench_refresh', v_refresh_result
      ),
      'code', CASE WHEN v_refresh_has_more THEN 'PAYMENT_CORRECTION_WORKBENCH_PAGE_STAGED' ELSE 'PAYMENT_CORRECTION_COMPLETE' END
    );
  END IF;

  IF v_phase = 'COMPLETE' OR v_operation.status = 'COMPLETE' THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'correction_request_id', p_correction_request_id,
      'operation_id', v_operation.id, 'phase', 'COMPLETE',
      'result', v_operation.result_json,
      'processed', 0,
      'applied', COALESCE((v_operation.result_json->>'applied_candidate_count')::integer, 0),
      'skipped', 0,
      'blocked', COALESCE((v_operation.result_json->>'blocked_candidate_count')::integer, 0),
      'failed_retryable', 0,
      'failed_final', COALESCE((v_operation.result_json->>'failed_candidate_count')::integer, 0),
      'pending', 0,
      'processing', 0,
      'progress_completed', COALESCE(v_operation.completed_units, 0),
      'progress_total', COALESCE(v_operation.total_units, v_operation.completed_units, 0),
      'parent_status', v_request.status,
      'totals', COALESCE(v_operation.result_json, '{}'::jsonb),
      'complete', true,
      'requires_user_action', v_operation.requires_user_action,
      'processing_continues', false,
      'changed_scope_json', '{}'::jsonb,
      'live_signal_updates', pg_catalog.jsonb_build_object(
        'workbench_refresh_staging_complete', true,
        'workbench_freshness_must_be_read_from_jobs_and_candidate_state', true
      ),
      'code', 'PAYMENT_CORRECTION_ALREADY_COMPLETE'
    );
  END IF;

  RAISE EXCEPTION 'PAYMENT_CORRECTION_PHASE_NOT_RUNNABLE'
    USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
      'code', 'PAYMENT_CORRECTION_PHASE_NOT_RUNNABLE', 'phase', v_phase
    )::text;
END;
$function$;

ALTER FUNCTION public.pay_payment_correction_process_chunk(uuid,integer,text,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_process_chunk(uuid,integer,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_correction_process_chunk(uuid,integer,text,uuid) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_correction_process_chunk(uuid,integer,text,uuid) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_correction_process_chunk(uuid,integer,text,uuid) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_process_chunk(uuid,integer,text,uuid) TO service_role;
