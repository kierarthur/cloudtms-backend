\set ON_ERROR_STOP on

-- H2 current-V8 reliability proof.  This uses only synthetic rows inside one
-- outer rollback and invokes no payment, provider, settlement or remittance
-- owner.  It proves that a failed two-channel Draft is durably made unusable
-- without deleting its audit lineage.
BEGIN;
SET LOCAL statement_timeout = '15000ms';
SET LOCAL lock_timeout = '1500ms';
SET LOCAL idle_in_transaction_session_timeout = '30000ms';
SET LOCAL jit = off;

DO $atomic_multibatch_failure$
DECLARE
  v_operation constant uuid := 'a8450000-0000-4000-8000-000000000001';
  v_session constant uuid := 'a8450000-0000-4000-8000-000000000002';
  v_snapshot constant uuid := 'a8450000-0000-4000-8000-000000000003';
  v_actor constant uuid := 'a8450000-0000-4000-8000-000000000004';
  v_candidate_paye constant uuid := 'a8450000-0000-4000-8000-000000000005';
  v_candidate_umbrella constant uuid := 'a8450000-0000-4000-8000-000000000006';
  v_scope_paye constant uuid := 'a8450000-0000-4000-8000-000000000007';
  v_scope_umbrella constant uuid := 'a8450000-0000-4000-8000-000000000008';
  v_batch_paye constant uuid := 'a8450000-0000-4000-8000-000000000009';
  v_batch_umbrella constant uuid := 'a8450000-0000-4000-8000-00000000000a';
  v_batch_candidate_paye constant uuid := 'a8450000-0000-4000-8000-00000000000b';
  v_batch_candidate_umbrella constant uuid := 'a8450000-0000-4000-8000-00000000000c';
  v_item_paye constant uuid := 'a8450000-0000-4000-8000-00000000000d';
  v_item_umbrella constant uuid := 'a8450000-0000-4000-8000-00000000000e';
  v_result jsonb;
  v_finish jsonb;
BEGIN
  INSERT INTO public.tms_users(id,email,password_hash)
  VALUES (v_actor,'h2-atomic-multibatch@example.invalid','LOCAL_ROLLBACK_FIXTURE');

  INSERT INTO public.candidates(id,pay_method)
  VALUES (v_candidate_paye,'PAYE'),(v_candidate_umbrella,'UMBRELLA');

  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,
    eligibility_from_date,eligibility_to_date,status,is_active
  ) VALUES (
    v_snapshot,'2026-09-04','2026-09-06','2026-08-31',
    '2026-08-31','2026-09-06','READY',false
  );

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,status,version,
    progress_counter_version,progress_state,source_snapshot_run_id,
    session_signature,scope_change_generation_target,scope_change_generation_applied,
    scope_change_generation_shadow_checked,authority_fence_generation
  ) VALUES (
    v_session,v_actor,'2026-09-04','2026-09-06','OPEN',1,1,'READY',v_snapshot,
    repeat('a',64),1,1,1,1
  );

  INSERT INTO public.pay_batches(
    id,pay_date,status,banking_system_snapshot,
    external_paye_system_snapshot,rail_provider_snapshot,rail_env_snapshot,
    batch_kind_fixed,source_workbench_session_id,source_snapshot_run_id,
    source_session_version,execution_commit_state
  ) VALUES
    (v_batch_paye,'2026-09-04','DRAFT','MONZO_CSV','SAGE','CSV','PROD',
      'PAYE',v_session,v_snapshot,1,'NOT_SUBMITTED'),
    (v_batch_umbrella,'2026-09-04','DRAFT','MONZO_CSV','SAGE','CSV','PROD',
      'UMBRELLA',v_session,v_snapshot,1,'NOT_SUBMITTED');

  INSERT INTO public.banking_pay_operations(
    id,operation_type,status,phase,actor_user_id,workbench_session_id,pay_batch_id,
    idempotency_key,input_json,progress_json
  ) VALUES (
    v_operation,'DRAFT_CREATE','RUNNING','INSERT_ITEMS',v_actor,v_session,v_batch_paye,
    'h2-v8-atomic-multibatch-failure',
    jsonb_build_object('source_snapshot_run_id',v_snapshot::text,'source_session_version',1),
    jsonb_build_object('created_pay_batch_ids',jsonb_build_array(v_batch_paye,v_batch_umbrella))
  );

  INSERT INTO public.banking_pay_operation_candidate_scope(
    id,operation_id,workbench_session_id,source_snapshot_run_id,source_session_version,
    candidate_id,pay_channel,pay_batch_id,scope_hash,status
  ) VALUES
    (v_scope_paye,v_operation,v_session,v_snapshot,1,v_candidate_paye,'PAYE',v_batch_paye,repeat('1',64),'DRAFTED'),
    (v_scope_umbrella,v_operation,v_session,v_snapshot,1,v_candidate_umbrella,'UMBRELLA',v_batch_umbrella,repeat('2',64),'DRAFTED');

  INSERT INTO public.pay_batch_candidates(id,pay_batch_id,candidate_id,paye_state)
  VALUES
    (v_batch_candidate_paye,v_batch_paye,v_candidate_paye,'READY'),
    (v_batch_candidate_umbrella,v_batch_umbrella,v_candidate_umbrella,NULL);

  INSERT INTO public.pay_batch_items(
    id,pay_batch_candidate_id,item_type,description,amount_ex_vat,amount_vat,
    amount_inc_vat,pay_channel,operation_source_key
  ) VALUES
    (v_item_paye,v_batch_candidate_paye,'SEGMENT_DELTA','Synthetic PAYE partial Draft item',10.00,0.00,10.00,'PAYE',v_operation::text||':PAYE'),
    (v_item_umbrella,v_batch_candidate_umbrella,'SEGMENT_DELTA','Synthetic Umbrella partial Draft item',20.00,4.00,24.00,'UMBRELLA',v_operation::text||':UMBRELLA');

  v_result := public.pay_batch_abort_failed_draft_create_partial(
    v_operation,v_batch_paye,v_actor,'Synthetic two-channel Draft failure',
    jsonb_build_object('code','H2_SYNTHETIC_SECOND_CHANNEL_FAILURE','phase','INSERT_ITEMS',
      'called_from_failure_finalisation',true));
  IF v_result->'ok' IS DISTINCT FROM 'true'::jsonb
     OR v_result->>'status_after' <> 'CANCELLED'
     OR (v_result->>'item_rows_voided')::integer <> 1
     OR (v_result->>'candidate_scopes_failed')::integer <> 1 THEN
    RAISE EXCEPTION 'H2_V8_PAYE_PARTIAL_ABORT_FAILED: %',v_result;
  END IF;

  v_result := public.pay_batch_abort_failed_draft_create_partial(
    v_operation,v_batch_umbrella,v_actor,'Synthetic two-channel Draft failure',
    jsonb_build_object('code','H2_SYNTHETIC_SECOND_CHANNEL_FAILURE','phase','INSERT_ITEMS',
      'called_from_failure_finalisation',true));
  IF v_result->'ok' IS DISTINCT FROM 'true'::jsonb
     OR v_result->>'status_after' <> 'CANCELLED'
     OR (v_result->>'item_rows_voided')::integer <> 1
     OR (v_result->>'candidate_scopes_failed')::integer <> 1 THEN
    RAISE EXCEPTION 'H2_V8_UMBRELLA_PARTIAL_ABORT_FAILED: %',v_result;
  END IF;

  SELECT pg_catalog.to_jsonb(finished) INTO v_finish
  FROM public.banking_pay_operation_finish(
    v_operation,'FAILED',NULL,
    jsonb_build_object(
      'code','H2_SYNTHETIC_SECOND_CHANNEL_FAILURE',
      'cleanup_status','COMPLETE',
      'created_pay_batch_ids',jsonb_build_array(v_batch_paye,v_batch_umbrella),
      'batch_action_blocked',true,
      'normal_draft_actions_blocked',true)
  ) AS finished;
  IF v_finish->>'status' <> 'FAILED' THEN
    RAISE EXCEPTION 'H2_V8_OPERATION_NOT_FAILED_AFTER_CLEANUP: %',v_finish;
  END IF;

  -- Exact replay is harmless: both batches remain cancelled, no item is made
  -- active again, and the audit row is retained rather than deleted.
  v_result := public.pay_batch_abort_failed_draft_create_partial(
    v_operation,v_batch_paye,v_actor,'Synthetic two-channel Draft failure replay',
    jsonb_build_object('code','H2_SYNTHETIC_SECOND_CHANNEL_FAILURE','phase','INSERT_ITEMS'));
  IF v_result->'ok' IS DISTINCT FROM 'true'::jsonb OR v_result->>'status_after' <> 'CANCELLED' THEN
    RAISE EXCEPTION 'H2_V8_ABORT_REPLAY_FAILED: %',v_result;
  END IF;

  IF (SELECT count(*) FROM public.pay_batches WHERE id IN (v_batch_paye,v_batch_umbrella) AND status='CANCELLED') <> 2
     OR (SELECT count(*) FROM public.pay_batch_items WHERE id IN (v_item_paye,v_item_umbrella) AND is_voided) <> 2
     OR (SELECT count(*) FROM public.banking_pay_operation_candidate_scope WHERE operation_id=v_operation AND status='FAILED') <> 2
     OR EXISTS (SELECT 1 FROM public.pay_advance_reservations WHERE pay_batch_id IN (v_batch_paye,v_batch_umbrella) AND released_at_utc IS NULL)
     OR EXISTS (SELECT 1 FROM public.pay_bank_transfers WHERE pay_batch_id IN (v_batch_paye,v_batch_umbrella))
     OR EXISTS (SELECT 1 FROM public.banking_pay_operation_provider_attempts WHERE pay_batch_id IN (v_batch_paye,v_batch_umbrella))
     OR EXISTS (SELECT 1 FROM public.pay_bank_transfer_events WHERE pay_batch_id IN (v_batch_paye,v_batch_umbrella))
     OR EXISTS (SELECT 1 FROM public.banking_pay_operation_remittance_scope WHERE pay_batch_id IN (v_batch_paye,v_batch_umbrella))
     OR EXISTS (SELECT 1 FROM public.banking_pay_operation_settlement_scope WHERE pay_batch_id IN (v_batch_paye,v_batch_umbrella)) THEN
    RAISE EXCEPTION 'H2_V8_ATOMIC_MULTIBATCH_CLEANUP_INVARIANT_FAILED';
  END IF;
END;
$atomic_multibatch_failure$;

ROLLBACK;

DO $post_rollback$
BEGIN
  IF EXISTS (SELECT 1 FROM public.banking_pay_operations WHERE id='a8450000-0000-4000-8000-000000000001'::uuid)
     OR EXISTS (SELECT 1 FROM public.pay_batches WHERE id IN (
       'a8450000-0000-4000-8000-000000000009'::uuid,
       'a8450000-0000-4000-8000-00000000000a'::uuid))
     OR EXISTS (SELECT 1 FROM public.candidates WHERE id IN (
       'a8450000-0000-4000-8000-000000000005'::uuid,
       'a8450000-0000-4000-8000-000000000006'::uuid)) THEN
    RAISE EXCEPTION 'H2_V8_ATOMIC_MULTIBATCH_ROLLBACK_LEAK';
  END IF;
END;
$post_rollback$;
