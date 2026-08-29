\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
DO $bank_jobs$
DECLARE v_session public.banking_pay_workbench_sessions%ROWTYPE;
 v_candidate uuid:='10000000-0000-4000-8000-000000000002';v_job uuid:='10000000-0000-4000-8000-000000009951';
 v_next uuid:='10000000-0000-4000-8000-000000009952';v_seq bigint;v_targets jsonb;v_payees jsonb;v_payload jsonb;v_fact jsonb;
 v_saved jsonb;v_many jsonb;v_count integer;v_before text;v_after text;
BEGIN
 SELECT * INTO STRICT v_session FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 SELECT COALESCE(seq,0) INTO v_seq FROM public.app_change_counters WHERE entity_key='pay_candidate:'||v_candidate::text;
 v_targets:=jsonb_build_array(jsonb_build_object('candidate_id',v_candidate,'entity_kind','CANDIDATE','entity_id',v_candidate,'bank_details_hash','fixture-bank-hash'));
 v_payees:=jsonb_build_array(jsonb_build_object('candidate_id',v_candidate,'payee_entity_kind','CANDIDATE','payee_entity_id',v_candidate,'bank_details_hash','fixture-bank-hash'));
 v_payload:=jsonb_build_object('candidate_id',v_candidate,'session_id',v_session.id,'session_version',v_session.version,
   'source_change_seq',v_seq,'payees_json',v_payees,'readiness_fingerprint',md5(v_payees::text),'rail_env','SANDBOX');
 INSERT INTO public.banking_pay_workbench_jobs(id,job_type,status,session_id,candidate_id,snapshot_run_id,dedupe_key,payload_json)
 VALUES(v_job,'PAYEE_READINESS_ENSURE','QUEUED',v_session.id,v_candidate,v_session.source_snapshot_run_id,'fixture-bank-job-generation',v_payload);
 SELECT job_facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'REVOLUT','SANDBOX');
 IF v_fact->>'job_id'<>v_job::text OR v_fact->'can_progress'<>'true'::jsonb OR v_fact->'is_failed'<>'false'::jsonb THEN RAISE EXCEPTION 'CURRENT_BANK_JOB_NOT_FOUND'; END IF;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'CSV','SANDBOX') WHERE job_facts IS NOT NULL;
 IF v_count<>0 THEN RAISE EXCEPTION 'WRONG_PROVIDER_JOB'; END IF;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'REVOLUT','PROD') WHERE job_facts IS NOT NULL;
 IF v_count<>0 THEN RAISE EXCEPTION 'WRONG_ENV_JOB'; END IF;
 -- Exact current source/version and identity; no stale label can fill the gap.
 FOR v_saved IN SELECT value FROM jsonb_array_elements(jsonb_build_array(
  v_payload||jsonb_build_object('session_version',v_session.version+1),
  v_payload||jsonb_build_object('source_change_seq',v_seq-1),
  v_payload||'{"readiness_fingerprint":"wrong-fingerprint"}',
  v_payload||'{"candidate_id":"10000000-0000-4000-8000-000000000003"}',
  v_payload||'{"session_id":"10000000-0000-4000-8000-000000009999"}',
  v_payload||jsonb_build_object('payees_json',jsonb_build_array(v_payees->0||'{"bank_details_hash":"other-bank"}'),
    'readiness_fingerprint',md5(jsonb_build_array(v_payees->0||'{"bank_details_hash":"other-bank"}')::text)),
  v_payload||jsonb_build_object('payees_json',jsonb_build_array(v_payees->0||'{"candidate_id":"10000000-0000-4000-8000-000000000003"}'),
    'readiness_fingerprint',md5(jsonb_build_array(v_payees->0||'{"candidate_id":"10000000-0000-4000-8000-000000000003"}')::text)),
  v_payload||'{"payees_json":false}')) LOOP
   UPDATE public.banking_pay_workbench_jobs SET payload_json=v_saved WHERE id=v_job;
   SELECT count(*) INTO v_count FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'REVOLUT','SANDBOX') WHERE job_facts IS NOT NULL;
   IF v_count<>0 THEN RAISE EXCEPTION 'INVALID_BANK_JOB_CONTEXT_ACCEPTED'; END IF;
 END LOOP;
 UPDATE public.banking_pay_workbench_jobs SET payload_json=v_payload,status='FAILED',failed_at_utc=now() WHERE id=v_job;
 SELECT job_facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'REVOLUT','SANDBOX');
 IF v_fact->'is_failed'<>'true'::jsonb OR v_fact->'can_progress'<>'false'::jsonb THEN RAISE EXCEPTION 'FAILED_JOB_SHOWN_UPDATING'; END IF;
 UPDATE public.banking_pay_workbench_jobs SET status='DEAD' WHERE id=v_job;
 SELECT job_facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'REVOLUT','SANDBOX');
 IF v_fact->'is_failed'<>'true'::jsonb OR v_fact->'can_progress'<>'false'::jsonb THEN RAISE EXCEPTION 'EXHAUSTED_JOB_SHOWN_UPDATING'; END IF;
 INSERT INTO public.banking_pay_workbench_jobs(id,job_type,status,session_id,candidate_id,snapshot_run_id,dedupe_key,payload_json)
 VALUES(v_next,'PAYEE_READINESS_ENSURE','RUNNING',v_session.id,v_candidate,v_session.source_snapshot_run_id,'fixture-bank-job-generation',v_payload);
 SELECT job_facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'REVOLUT','SANDBOX');
 IF v_fact->>'job_id'<>v_next::text OR v_fact->'can_progress'<>'true'::jsonb THEN RAISE EXCEPTION 'ACTIVE_BANK_SUCCESSOR_NOT_ADOPTED'; END IF;
 UPDATE public.banking_pay_workbench_jobs SET status='SUCCEEDED',completed_at_utc=now() WHERE id=v_next;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'REVOLUT','SANDBOX') WHERE job_facts IS NOT NULL;
 IF v_count<>0 THEN RAISE EXCEPTION 'COMPLETED_EQUIVALENT_DID_NOT_SUPPRESS_FAILURE'; END IF;
 DELETE FROM public.banking_pay_workbench_jobs WHERE id=v_next;
 SELECT jsonb_agg(v_payees->0) INTO v_many FROM generate_series(1,26);
 UPDATE public.banking_pay_workbench_jobs SET status='QUEUED',failed_at_utc=NULL,
   payload_json=v_payload||jsonb_build_object('payees_json',v_many,'readiness_fingerprint',md5(v_many::text)) WHERE id=v_job;
 SELECT job_facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'REVOLUT','SANDBOX');
 IF v_fact->'can_progress'<>'false'::jsonb OR v_fact->>'blocked_code'<>'READINESS_JOB_UNIT_TOO_LARGE' THEN RAISE EXCEPTION 'OVERSIZED_JOB_SHOWN_PROGRESSING'; END IF;
 UPDATE public.banking_pay_workbench_jobs SET payload_json=v_payload-'rail_env' WHERE id=v_job;
 SELECT job_facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'REVOLUT','PROD');
 IF v_fact->'can_progress'<>'true'::jsonb THEN RAISE EXCEPTION 'EXECUTOR_EXISTING_DEFAULT_ENV_DRIFT'; END IF;
 SELECT md5(jsonb_agg(to_jsonb(j) ORDER BY j.id)::text) INTO v_before FROM public.banking_pay_workbench_jobs j WHERE j.session_id=v_session.id;
 PERFORM * FROM private.pay_workbench_modal_bank_job_facts_v2(v_session,v_targets,'REVOLUT','PROD');
 SELECT md5(jsonb_agg(to_jsonb(j) ORDER BY j.id)::text) INTO v_after FROM public.banking_pay_workbench_jobs j WHERE j.session_id=v_session.id;
 IF v_before IS DISTINCT FROM v_after THEN RAISE EXCEPTION 'BANK_JOB_READ_MUTATED'; END IF;
 SET LOCAL client_min_messages='notice';
 RAISE NOTICE 'PASS: bank jobs require exact current ownership, fingerprint and target; completed/successor precedence; failed/oversized states never masquerade as progressing.';
END;
$bank_jobs$;
ROLLBACK;
