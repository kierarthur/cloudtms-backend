-- Agency TEST first-use proof. Caller wraps this entire file in BEGIN/ROLLBACK.
-- New synthetic rows only; no contracts, financial rows or provider operations.
DO $verify$
DECLARE
  c uuid:=gen_random_uuid();
  a uuid;
  u uuid;
  op uuid:=gen_random_uuid();
  p jsonb;
  r jsonb;
  replay jsonb;
  plan jsonb;
  seq_before bigint;
  seq_after bigint;
  jobs_before bigint;
  jobs_after bigint;
BEGIN
  IF current_database() IS DISTINCT FROM 'cloudtms_test_clone' THEN
    RAISE EXCEPTION 'TEST_DATABASE_REQUIRED';
  END IF;
  SELECT id INTO a FROM public.tms_users WHERE is_active AND lower(role)='admin' ORDER BY id LIMIT 1;
  SELECT id INTO u FROM public.umbrellas WHERE name='ARDENT PAY PARTNERS LTD' ORDER BY id LIMIT 1;
  IF a IS NULL OR u IS NULL THEN RAISE EXCEPTION 'TEST_ACTOR_AND_UMBRELLA_REQUIRED'; END IF;
  INSERT INTO public.candidates(id,first_name,last_name,email,active,pay_method)
  VALUES(c,'Codex','Pay Method Regression','pay-method-'||c::text||'@example.invalid',true,'PAYE');
  SELECT coalesce((SELECT seq FROM public.app_change_counters WHERE entity_key='pay_candidate:'||c::text),0) INTO seq_before;
  SELECT count(*) INTO jobs_before FROM public.banking_pay_workbench_jobs WHERE candidate_id=c;
  p:=public.candidate_pay_method_change_refresh_scope_v1(c,'PAYE','UMBRELLA');
  p:=public.candidate_pay_method_change_refresh_scope_v1(c,'PAYE','UMBRELLA');
  SELECT coalesce((SELECT seq FROM public.app_change_counters WHERE entity_key='pay_candidate:'||c::text),0) INTO seq_after;
  SELECT count(*) INTO jobs_after FROM public.banking_pay_workbench_jobs WHERE candidate_id=c;
  IF seq_after IS DISTINCT FROM seq_before OR jobs_after IS DISTINCT FROM jobs_before
    OR (p->>'latest_source_change_seq')::bigint IS DISTINCT FROM seq_before
    OR p->'targeted_timesheet_ids' IS DISTINCT FROM '[]'::jsonb THEN
    RAISE EXCEPTION 'PREVIEW_MUTATED_OR_MISREPORTED_SCOPE';
  END IF;
  plan:=jsonb_build_object('operation_id',op,'expected_old_method','PAYE','preview_source_change_seq',seq_before,
    'destination_patch',jsonb_build_object('umbrella_id',u));
  BEGIN
    PERFORM public.candidate_pay_method_change_apply(c,'UMBRELLA',plan||jsonb_build_object('preview_source_change_seq',seq_before+1),a,'Synthetic stale proof');
    RAISE EXCEPTION 'STALE_PREVIEW_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'PT409' THEN
    IF SQLERRM IS DISTINCT FROM 'CANDIDATE_PAY_METHOD_CHANGE_PREVIEW_STALE' THEN RAISE; END IF;
  END;
  r:=public.candidate_pay_method_change_apply(c,'UMBRELLA',plan,a,'Synthetic round-trip proof');
  IF r->>'operation_committed' IS DISTINCT FROM 'true' OR r->>'new_method' IS DISTINCT FROM 'UMBRELLA'
    OR r->'targeted_timesheet_ids' IS DISTINCT FROM '[]'::jsonb THEN RAISE EXCEPTION 'UMBRELLA_APPLY_FAILED'; END IF;
  IF NOT EXISTS(SELECT 1 FROM public.candidates WHERE id=c AND pay_method='UMBRELLA' AND umbrella_id=u)
    OR NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_jobs WHERE id=(r->>'job_id')::uuid AND status='SUCCEEDED') THEN
    RAISE EXCEPTION 'UMBRELLA_CANONICAL_RESULT_MISMATCH';
  END IF;
  SELECT seq INTO seq_before FROM public.app_change_counters WHERE entity_key='pay_candidate:'||c::text;
  replay:=public.candidate_pay_method_change_apply(c,'UMBRELLA',plan,a,'Synthetic replay proof');
  SELECT seq INTO seq_after FROM public.app_change_counters WHERE entity_key='pay_candidate:'||c::text;
  IF replay->>'idempotent_replay' IS DISTINCT FROM 'true' OR replay->>'job_id' IS DISTINCT FROM r->>'job_id'
    OR seq_before IS DISTINCT FROM seq_after THEN RAISE EXCEPTION 'REPLAY_CHANGED_AUTHORITY'; END IF;
  BEGIN
    PERFORM public.candidate_pay_method_change_apply(c,'UMBRELLA',plan||jsonb_build_object('preview_source_change_seq',seq_before+100),a,'Synthetic conflicting replay');
    RAISE EXCEPTION 'CONFLICTING_REPLAY_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'PT409' THEN
    IF SQLERRM IS DISTINCT FROM 'CANDIDATE_PAY_METHOD_CHANGE_OPERATION_ID_CONFLICT' THEN RAISE; END IF;
  END;
  -- Each HTTP request has a new transaction; clear only this test's local context.
  PERFORM set_config('cloudtms.candidate_pay_method_change_operation_id','',true);
  PERFORM set_config('cloudtms.candidate_pay_method_change_actor_user_id','',true);
  p:=public.candidate_pay_method_change_refresh_scope_v1(c,'UMBRELLA','PAYE');
  plan:=jsonb_build_object('operation_id',gen_random_uuid(),'expected_old_method','UMBRELLA','preview_source_change_seq',(p->>'latest_source_change_seq')::bigint,
    'destination_patch',jsonb_build_object('umbrella_id',NULL,'account_holder',NULL,'bank_name',NULL,'sort_code',NULL,'account_number',NULL));
  r:=public.candidate_pay_method_change_apply(c,'PAYE',plan,a,'Synthetic PAYE return proof');
  IF r->>'operation_committed' IS DISTINCT FROM 'true' OR r->>'new_method' IS DISTINCT FROM 'PAYE'
    OR NOT EXISTS(SELECT 1 FROM public.candidates WHERE id=c AND pay_method='PAYE' AND umbrella_id IS NULL
      AND account_holder IS NULL AND bank_name IS NULL AND sort_code IS NULL AND account_number IS NULL)
    OR r->'targeted_timesheet_ids' IS DISTINCT FROM '[]'::jsonb THEN RAISE EXCEPTION 'PAYE_RETURN_FAILED'; END IF;
  IF EXISTS(SELECT 1 FROM public.contracts WHERE candidate_id=c) OR EXISTS(SELECT 1 FROM public.timesheets_financials WHERE candidate_id=c) THEN
    RAISE EXCEPTION 'UNEXPECTED_FINANCIAL_SCOPE';
  END IF;
  RAISE NOTICE 'PASS: preview read-only, stale conflict, PAYE/Umbrella round-trip, idempotent replay, zero financial scope';
END;
$verify$;
