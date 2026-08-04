-- Exact installed TEST rollback definition captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: 44953863f0b5343fd6646ce814b60649

CREATE OR REPLACE FUNCTION public.pay_batch_cancel(p_pay_batch_id uuid, p_actor_user_id uuid, p_reason text, p_correction_request_id uuid DEFAULT NULL::uuid, p_work_item_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_batch public.pay_batches%rowtype;
  v_evidence_json jsonb := '{}'::jsonb;
  v_result_json jsonb := '{}'::jsonb;
  v_progress_json jsonb := '{}'::jsonb;
  v_blockers jsonb := '[]'::jsonb;
  v_reason text := COALESCE(NULLIF(btrim(p_reason), ''), 'Pre-bank cancellation requested');
  v_idempotency_key text := NULL::text;
  v_correction_request_id uuid := p_correction_request_id;
  v_active_payment_item_count integer := 0;
  v_pending_work_count integer := 0;
  v_processing_work_count integer := 0;
  v_applied_work_count integer := 0;
  v_blocked_work_count integer := 0;
  v_failed_work_count integer := 0;
  v_live_signal_result jsonb := '{}'::jsonb;
begin
  if p_pay_batch_id is null then
    raise exception 'PAY_BATCH_CANCEL_PAY_BATCH_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_CANCEL_PAY_BATCH_ID_REQUIRED')::text;
  end if;

  if p_actor_user_id is null then
    raise exception 'PAY_BATCH_CANCEL_ACTOR_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_CANCEL_ACTOR_REQUIRED')::text;
  end if;

  select public.pay_batches.*
  into v_batch
  from public.pay_batches
  where public.pay_batches.id = p_pay_batch_id
  for update;

  if not found then
    raise exception 'PAY_BATCH_CANCEL_BATCH_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_CANCEL_BATCH_NOT_FOUND', 'pay_batch_id', p_pay_batch_id)::text;
  end if;

  v_evidence_json := public.pay_batch_submission_evidence(p_pay_batch_id, true);

  if COALESCE((v_evidence_json->>'has_external_provider_submission')::boolean, false)
     or COALESCE((v_evidence_json->>'has_pending_provider_outcome')::boolean, false)
     or COALESCE((v_evidence_json->>'has_unknown_provider_outcome')::boolean, false)
     or COALESCE((v_evidence_json->>'has_terminal_no_money')::boolean, false)
     or COALESCE((v_evidence_json->>'has_final_paid_or_settled')::boolean, false) then
    v_blockers := jsonb_build_array(jsonb_build_object(
      'code', 'PAY_BATCH_CANCEL_PROVIDER_EVIDENCE_PRESENT',
      'message', 'Whole-batch local cancellation is blocked because provider submission, terminal no-money, unknown outcome, pending provider state, or paid/settled evidence exists.',
      'submission_evidence', v_evidence_json
    ));

    raise exception 'PAY_BATCH_CANCEL_PROVIDER_EVIDENCE_PRESENT'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_CANCEL_PROVIDER_EVIDENCE_PRESENT',
              'pay_batch_id', p_pay_batch_id,
              'blockers', v_blockers,
              'submission_evidence', v_evidence_json
            )::text;
  end if;

  if exists (
    select 1
    from public.banking_pay_operations as provider_operation
    where provider_operation.pay_batch_id = p_pay_batch_id
      and provider_operation.operation_type in ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
      and provider_operation.phase in ('SUBMIT_PROVIDER_TRANSFERS', 'PROVIDER_SUBMIT', 'CLAIM_PROVIDER_SUBMIT', 'EXECUTE_PROVIDER')
      and provider_operation.status in ('QUEUED', 'RUNNING', 'PROCESSING', 'CLAIMED')
    limit 1
  ) then
    raise exception 'PAY_BATCH_CANCEL_PROVIDER_SUBMISSION_IN_PROGRESS'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_CANCEL_PROVIDER_SUBMISSION_IN_PROGRESS',
              'pay_batch_id', p_pay_batch_id
            )::text;
  end if;

  v_idempotency_key := COALESCE(
    CASE WHEN v_correction_request_id IS NULL THEN NULL ELSE 'pay-batch-cancel-existing-request:' || v_correction_request_id::text END,
    'pay-batch-cancel:' || p_pay_batch_id::text
  );

  v_result_json := public.pay_payment_cancel_not_sent_and_recalculate(
    p_pay_batch_id => p_pay_batch_id,
    p_selection_json => jsonb_build_object(
      'scope_type', 'BATCH',
      'source_context', 'pay_batch_cancel',
      'requested_action', 'PRE_PROVIDER_CANCEL_AND_RECALCULATE',
      'legacy_entry_point', true,
      'db_status_rule', 'Only existing pay_batches.status values are written; derived partial lifecycle labels remain payload-only.'
    ),
    p_actor_user_id => p_actor_user_id,
    p_reason => v_reason,
    p_idempotency_key => v_idempotency_key,
    p_confirmation_json => jsonb_build_object(
      'confirmed_pre_provider_cancel', true,
      'legacy_pay_batch_cancel_entry_point', true,
      'called_from_work_item_id', CASE WHEN p_work_item_id IS NULL THEN NULL ELSE p_work_item_id::text END
    )
  );

  v_correction_request_id := COALESCE(
    NULLIF(v_result_json->>'correction_request_id', '')::uuid,
    v_correction_request_id
  );

  if v_correction_request_id is not null then
    select jsonb_build_object(
             'total', count(*)::integer,
             'pending', count(*) filter (where work_items.status = 'PENDING')::integer,
             'processing', count(*) filter (where work_items.status = 'PROCESSING')::integer,
             'applied', count(*) filter (where work_items.status = 'APPLIED')::integer,
             'blocked', count(*) filter (where work_items.status = 'BLOCKED')::integer,
             'failed_retryable', count(*) filter (where work_items.status = 'FAILED_RETRYABLE')::integer,
             'failed_final', count(*) filter (where work_items.status = 'FAILED_FINAL')::integer
           ),
           count(*) filter (where work_items.status = 'PENDING')::integer,
           count(*) filter (where work_items.status = 'PROCESSING')::integer,
           count(*) filter (where work_items.status = 'APPLIED')::integer,
           count(*) filter (where work_items.status = 'BLOCKED')::integer,
           count(*) filter (where work_items.status in ('FAILED_RETRYABLE', 'FAILED_FINAL'))::integer
    into v_progress_json,
         v_pending_work_count,
         v_processing_work_count,
         v_applied_work_count,
         v_blocked_work_count,
         v_failed_work_count
    from public.pay_payment_correction_work_items as work_items
    where work_items.correction_request_id = v_correction_request_id;
  else
    v_progress_json := COALESCE(v_result_json->'progress', '{}'::jsonb);
  end if;

  select count(*)::integer
  into v_active_payment_item_count
  from public.pay_batch_items as active_items
  join public.pay_batch_candidates as active_candidates
    on active_candidates.id = active_items.pay_batch_candidate_id
  where active_candidates.pay_batch_id = p_pay_batch_id
    and coalesce(active_items.is_voided, false) = false
    and active_items.item_type <> 'DEBT_CREATED';

  if coalesce(v_active_payment_item_count, 0) = 0
     and coalesce(v_pending_work_count, 0) = 0
     and coalesce(v_processing_work_count, 0) = 0
     and coalesce(v_blocked_work_count, 0) = 0
     and coalesce(v_failed_work_count, 0) = 0 then
    update public.pay_batches as batch_to_cancel
    set
      status = 'CANCELLED',
      cancelled_at_utc = coalesce(batch_to_cancel.cancelled_at_utc, v_now),
      cancelled_by_user_id = coalesce(batch_to_cancel.cancelled_by_user_id, p_actor_user_id),
      cancel_reason = coalesce(batch_to_cancel.cancel_reason, v_reason),
      schedule_kind = null,
      scheduled_at_utc = null,
      scheduled_by_user_id = null,
      funding_account_ref = null,
      funds_warning_hours_json = null,
      execution_commit_state = case
        when nullif(btrim(coalesce(batch_to_cancel.execution_commit_state, '')), '') is null then 'NOT_SUBMITTED'
        else batch_to_cancel.execution_commit_state
      end
    where batch_to_cancel.id = p_pay_batch_id;
  end if;

  v_live_signal_result := public.banking_pay_batch_signal_touch(
    p_pay_batch_id := p_pay_batch_id,
    p_change_reason := 'PAY_BATCH_CANCEL',
    p_change_source := 'pay_batch_cancel',
    p_change_scope_json := jsonb_strip_nulls(jsonb_build_object(
      'correction_request_id', CASE WHEN v_correction_request_id IS NULL THEN NULL ELSE v_correction_request_id::text END,
      'legacy_entry_point', 'pay_batch_cancel',
      'progress', COALESCE(v_progress_json, '{}'::jsonb),
      'active_payment_item_count_after', COALESCE(v_active_payment_item_count, 0)
    )),
    p_touch_payment_status := true,
    p_touch_correction_progress := true,
    p_touch_alerts := false,
    p_touch_overview := true
  );

  return COALESCE(v_result_json, '{}'::jsonb) || jsonb_build_object(
    'ok', COALESCE(NULLIF(v_result_json->>'ok', '')::boolean, true),
    'pay_batch_id', p_pay_batch_id::text,
    'legacy_entry_point', 'pay_batch_cancel',
    'correction_request_id', CASE WHEN v_correction_request_id IS NULL THEN NULL ELSE v_correction_request_id::text END,
    'progress', COALESCE(v_progress_json, '{}'::jsonb),
    'active_payment_item_count_after', COALESCE(v_active_payment_item_count, 0),
    'batch_status', (select batch_status_after.status from public.pay_batches as batch_status_after where batch_status_after.id = p_pay_batch_id),
    'db_status_rule', 'CANCELLED may be written only after all payment-scope work items have applied; partial cancellation remains derived payload state only.',
    'submission_evidence', v_evidence_json,
    'live_signal', COALESCE(v_live_signal_result, '{}'::jsonb),
    'policy_x_checked', true
  );
end;
$function$;

ALTER FUNCTION pay_batch_cancel(uuid,uuid,text,uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_batch_cancel(uuid,uuid,text,uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_batch_cancel(uuid,uuid,text,uuid,uuid) TO postgres;
GRANT EXECUTE ON FUNCTION pay_batch_cancel(uuid,uuid,text,uuid,uuid) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_batch_cancel(uuid,uuid,text,uuid,uuid) TO service_role;
