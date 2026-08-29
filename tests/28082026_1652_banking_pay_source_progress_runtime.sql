\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql

UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true
WHERE id='10000000-0000-4000-8000-000000000005';
INSERT INTO public.candidates(id,display_name,tms_ref)
SELECT ('10000000-0000-4000-8000-' || lpad((6000+n)::text,12,'0'))::uuid,
  'Source fixture ' || n,'SOURCE-' || n FROM generate_series(1,103) n;
INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty)
SELECT '10000000-0000-4000-8000-000000000005'::uuid,
  ('10000000-0000-4000-8000-' || lpad((6000+n)::text,12,'0'))::uuid,n+2,'READY',true,false
FROM generate_series(1,103) n;

CREATE FUNCTION pg_temp.source_fixture_state() RETURNS jsonb LANGUAGE sql AS $test$
  SELECT jsonb_build_object(
    'session',(SELECT to_jsonb(s) FROM public.banking_pay_workbench_sessions s WHERE id='10000000-0000-4000-8000-000000000005'),
    'scope',(SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM public.banking_pay_workbench_session_scope s WHERE session_id='10000000-0000-4000-8000-000000000005'),
    'rows',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id='10000000-0000-4000-8000-000000000005'),
    'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM public.banking_pay_workbench_jobs j WHERE session_id='10000000-0000-4000-8000-000000000005'));
$test$;
CREATE FUNCTION pg_temp.assert_source_facts(p_label text,p_state text,p_reason text DEFAULT NULL)
RETURNS void LANGUAGE plpgsql AS $test$
DECLARE v_id uuid:='10000000-0000-4000-8000-000000000005'; v_owner jsonb; v_totals record; v_row record; v_before jsonb;
BEGIN
  v_before:=pg_temp.source_fixture_state();
  v_owner:=public.pay_workbench_session_recompute_progress_counters(v_id,false,'BANKING_PAY_MODAL_STRUCTURE_V2',false);
  SELECT count(*)::integer AS total,count(DISTINCT candidate_id)::integer AS distinct_candidates,
    count(*) FILTER(WHERE source_ready)::integer AS ready,
    count(*) FILTER(WHERE source_failed)::integer AS failed,
    count(*) FILTER(WHERE recovery_required)::integer AS recovery_required,
    count(*) FILTER(WHERE recovery_scheduled)::integer AS recovery_scheduled,
    count(*) FILTER(WHERE publication_required AND publication_current IS NOT TRUE)::integer AS incomplete
  INTO v_totals FROM private.pay_workbench_modal_source_progress_facts_v2(v_id,1);
  IF v_totals.total<>105 OR v_totals.distinct_candidates<>105 THEN RAISE EXCEPTION 'SOURCE_FACTS_TRUNCATED_OR_DUPLICATED: %',p_label; END IF;
  IF v_totals.total IS DISTINCT FROM (v_owner->>'scope_total_count')::integer
    OR v_totals.ready IS DISTINCT FROM (v_owner->>'scope_ready_count')::integer
    OR LEAST(v_totals.total,v_totals.failed+v_totals.recovery_required) IS DISTINCT FROM (v_owner->>'scope_failed_count')::integer
    OR GREATEST(v_totals.total-v_totals.ready-v_totals.failed-v_totals.recovery_required,0) IS DISTINCT FROM (v_owner->>'scope_pending_count')::integer
    OR v_totals.recovery_required IS DISTINCT FROM (v_owner->>'recovery_required_count')::integer
    OR v_totals.recovery_scheduled IS DISTINCT FROM (v_owner->>'recovery_scheduled_count')::integer
    -- The established compact response omits the incomplete-count field. Its
    -- preserved Draft blocker is the public observation for this predicate.
    OR (v_totals.incomplete>0) IS DISTINCT FROM (v_owner->'draft_blocker_codes' ? 'CURRENT_SOURCE_PREVIEW_PUBLICATION_INCOMPLETE') THEN
    RAISE EXCEPTION 'SOURCE_FACTS_OWNER_PARITY: % facts %, owner %',p_label,to_jsonb(v_totals),
      jsonb_build_object('total',v_owner->'scope_total_count','ready',v_owner->'scope_ready_count',
        'failed',v_owner->'scope_failed_count','pending',v_owner->'scope_pending_count',
        'required',v_owner->'recovery_required_count','scheduled',v_owner->'recovery_scheduled_count',
        'incomplete',v_owner->'certified_publication_incomplete_count');
  END IF;
  SELECT * INTO STRICT v_row FROM private.pay_workbench_modal_source_progress_facts_v2(v_id,1)
  WHERE candidate_id='10000000-0000-4000-8000-000000000002';
  IF v_row.source_state IS DISTINCT FROM p_state OR v_row.owner_failure_reason IS DISTINCT FROM p_reason THEN
    RAISE EXCEPTION 'SOURCE_CLASSIFICATION: % expected %/% got %/%',p_label,p_state,p_reason,v_row.source_state,v_row.owner_failure_reason;
  END IF;
  IF pg_temp.source_fixture_state() IS DISTINCT FROM v_before THEN RAISE EXCEPTION 'SOURCE_FACTS_WROTE_STATE: %',p_label; END IF;
END;
$test$;

DO $states$
DECLARE v_id uuid:='10000000-0000-4000-8000-000000000005'; v_candidate uuid:='10000000-0000-4000-8000-000000000002';
  v_job uuid:='10000000-0000-4000-8000-000000008001'; v_successor uuid:='10000000-0000-4000-8000-000000008002';
  v_status text; v_error text; v_count integer:=0; v_payload jsonb;
BEGIN
  FOREACH v_status IN ARRAY ARRAY['READY','MATERIALISED','MATERIALIZED','SOURCE_EMPTY','FAILED','ERROR','LINE_WORK_ERROR','LINE_WORK_PROCESS_ERROR','SOURCE_BUILD_ERROR','PENDING','SEEDING','SOURCE_BUILD_PENDING'] LOOP
    UPDATE public.banking_pay_workbench_session_scope SET status=v_status WHERE session_id=v_id AND candidate_id=v_candidate;
    PERFORM pg_temp.assert_source_facts(v_status,
      CASE WHEN v_status=ANY(ARRAY['READY','MATERIALISED','MATERIALIZED','SOURCE_EMPTY']) THEN 'CURRENT'
           WHEN v_status=ANY(ARRAY['PENDING','SEEDING']) THEN 'UPDATING' ELSE 'REFRESH_REQUIRED' END,
      CASE WHEN v_status='SOURCE_BUILD_PENDING' THEN 'PENDING_JOB_ID_MISSING' END);
    v_count:=v_count+1;
  END LOOP;
  UPDATE public.banking_pay_workbench_session_scope SET status='READY',dirty=true WHERE session_id=v_id AND candidate_id=v_candidate;
  PERFORM pg_temp.assert_source_facts('Dirty current source','UPDATING');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_session_scope SET dirty=false,error_json='{"code":"FIXTURE_ERROR"}' WHERE session_id=v_id AND candidate_id=v_candidate;
  PERFORM pg_temp.assert_source_facts('Explicit error','REFRESH_REQUIRED');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_session_scope SET error_json=NULL,certified_preview_publication_required=true,certified_preview_publication_parity_ok=false WHERE session_id=v_id AND candidate_id=v_candidate;
  PERFORM pg_temp.assert_source_facts('Incomplete publication','UPDATING');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_session_scope SET certified_preview_publication_required=false,status='SOURCE_BUILD_PENDING' WHERE session_id=v_id AND candidate_id=v_candidate;
  -- A missing non-null owner is fenced by the installed nondeferrable FK. Do
  -- not disable that constraint to manufacture an impossible persisted state.
  BEGIN
    UPDATE public.banking_pay_workbench_session_scope SET pending_job_id=v_job WHERE session_id=v_id AND candidate_id=v_candidate;
    RAISE EXCEPTION 'SOURCE_MISSING_OWNER_FK_BYPASSED';
  EXCEPTION WHEN foreign_key_violation THEN NULL;
  END;
  INSERT INTO public.app_change_counters(entity_key,seq) VALUES('pay_candidate:' || v_candidate::text,10)
  ON CONFLICT(entity_key) DO UPDATE SET seq=EXCLUDED.seq;
  v_payload:=jsonb_build_object('session_version',1,'source_change_seq',10,'source_build_run_id','10000000-0000-4000-8000-000000008999');
  INSERT INTO public.banking_pay_workbench_jobs(id,session_id,candidate_id,job_type,dedupe_key,status,payload_json)
  VALUES(v_job,v_id,v_candidate,'WORKBENCH_CANDIDATE_SOURCE_BUILD','source-facts-owner','QUEUED',v_payload);
  UPDATE public.banking_pay_workbench_session_scope SET pending_job_id=v_job WHERE session_id=v_id AND candidate_id=v_candidate;
  PERFORM pg_temp.assert_source_facts('Exact queued owner','UPDATING');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_jobs SET status='RUNNING' WHERE id=v_job;
  PERFORM pg_temp.assert_source_facts('Exact running owner','UPDATING');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_jobs SET status='FAILED' WHERE id=v_job;
  PERFORM pg_temp.assert_source_facts('Terminal owner','REFRESH_REQUIRED','PENDING_JOB_TERMINAL');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_jobs SET status='QUEUED',candidate_id='10000000-0000-4000-8000-000000000003' WHERE id=v_job;
  PERFORM pg_temp.assert_source_facts('Another candidate cannot own this refresh','REFRESH_REQUIRED','PENDING_JOB_CONTEXT_INVALID');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_jobs SET candidate_id=v_candidate,payload_json=v_payload || '{"session_version":2}' WHERE id=v_job;
  PERFORM pg_temp.assert_source_facts('Wrong session version','REFRESH_REQUIRED','PENDING_JOB_CONTEXT_INVALID');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_jobs SET payload_json=v_payload || '{"source_change_seq":9}' WHERE id=v_job;
  PERFORM pg_temp.assert_source_facts('Stale source change','REFRESH_REQUIRED','PENDING_JOB_CONTEXT_INVALID');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_jobs SET payload_json=v_payload || '{"source_build_run_id":"not-a-uuid"}' WHERE id=v_job;
  PERFORM pg_temp.assert_source_facts('Invalid build identity','REFRESH_REQUIRED','PENDING_JOB_CONTEXT_INVALID');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_jobs SET payload_json=v_payload,job_type='PAYEE_READINESS_ENSURE' WHERE id=v_job;
  PERFORM pg_temp.assert_source_facts('Unrelated readiness job','REFRESH_REQUIRED','PENDING_JOB_CONTEXT_INVALID');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_jobs SET job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD',status='FAILED' WHERE id=v_job;
  INSERT INTO public.banking_pay_workbench_jobs(id,session_id,candidate_id,job_type,dedupe_key,status,payload_json)
  VALUES(v_successor,v_id,v_candidate,'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK','source-facts-successor','QUEUED',v_payload);
  PERFORM pg_temp.assert_source_facts('A valid successor is updating, not another user action','UPDATING','PENDING_JOB_TERMINAL');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_jobs SET payload_json=v_payload || '{"source_change_seq":9}' WHERE id=v_successor;
  PERFORM pg_temp.assert_source_facts('Stale successor cannot hide missing owner','REFRESH_REQUIRED','PENDING_JOB_TERMINAL');v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_jobs SET payload_json=v_payload,status='RUNNING' WHERE id=v_successor;
  PERFORM pg_temp.assert_source_facts('Running successor','UPDATING','PENDING_JOB_TERMINAL');v_count:=v_count+1;
  BEGIN
    PERFORM * FROM private.pay_workbench_modal_source_progress_facts_v2(v_id,2);
    RAISE EXCEPTION 'SOURCE_STALE_VERSION_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_STALE_REVISION' THEN RAISE; END IF;
  END;
  BEGIN
    PERFORM * FROM private.pay_workbench_modal_source_progress_facts_v2('10000000-0000-4000-8000-000000009999',1);
    RAISE EXCEPTION 'SOURCE_MISSING_SESSION_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'WORKBENCH_SESSION_NOT_FOUND' THEN RAISE; END IF;
  END;
  SET LOCAL client_min_messages='notice';
  RAISE NOTICE 'PASS: % progress-owner comparisons across 105 complete scope candidates, valid/invalid owners and successors; no writes from reads.',v_count;
END;
$states$;
ROLLBACK;
