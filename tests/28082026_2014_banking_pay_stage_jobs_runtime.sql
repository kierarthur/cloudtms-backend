\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
DO $stage_jobs$
DECLARE v_session public.banking_pay_workbench_sessions%ROWTYPE;v_reply jsonb;v_facts jsonb;v_payload jsonb;v_bad jsonb;
 v_type text;v_job uuid;v_candidate uuid;v_count integer;v_seq bigint;v_before text;v_after text;
 v_source uuid:='10000000-0000-4000-8000-000000009951';v_successor uuid:='10000000-0000-4000-8000-000000009952';
 v_root uuid:='10000000-0000-4000-8000-000000009953';v_build uuid:='10000000-0000-4000-8000-000000009954';v_cursor jsonb;
BEGIN
 SELECT * INTO STRICT v_session FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 INSERT INTO public.banking_pay_workbench_sessions(id,actor_user_id,pay_date,week_ending_cutoff,session_signature,source_snapshot_run_id,version)
 VALUES(v_source,v_session.actor_user_id,v_session.pay_date,v_session.week_ending_cutoff,'synthetic clone source',v_session.source_snapshot_run_id,1);
 FOREACH v_type IN ARRAY ARRAY['WORKBENCH_CANDIDATE_SOURCE_BUILD','WORKBENCH_CANDIDATE_DELTA_REFRESH','WORKBENCH_CANDIDATE_LINE_WORK_SEED',
   'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS','WORKBENCH_PREVIEW_ROWS_MATERIALISE','WORKBENCH_SESSION_SCOPE_SEED','WORKBENCH_SESSION_CLONE_REBASE'] LOOP
  v_candidate:=CASE WHEN v_type IN ('WORKBENCH_SESSION_SCOPE_SEED','WORKBENCH_SESSION_CLONE_REBASE') THEN NULL::uuid
    ELSE '10000000-0000-4000-8000-000000000002'::uuid END;
  SELECT COALESCE(c.seq,0) INTO v_seq FROM (SELECT 1) a LEFT JOIN public.app_change_counters c ON c.entity_key='pay_candidate:'||v_candidate::text;
  v_cursor:=NULL;
  IF v_type='WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
   INSERT INTO public.banking_pay_workbench_jobs(id,session_id,candidate_id,snapshot_run_id,job_type,dedupe_key,status,payload_json)
   VALUES(v_root,v_session.id,v_candidate,v_session.source_snapshot_run_id,v_type,'synthetic-stage-build-root','QUEUED',
     jsonb_build_object('session_version',v_session.version,'source_change_seq',v_seq,'source_build_run_id','10000000-0000-4000-8000-000000009959'));
   INSERT INTO private.banking_pay_workbench_economic_builds(id,candidate_id,session_id,session_version,source_snapshot_run_id,source_build_run_id,source_job_id,
     captured_candidate_generation,source_change_seq,status,private_stage,stage_version)
   VALUES(v_build,v_candidate,v_session.id,v_session.version,v_session.source_snapshot_run_id,'10000000-0000-4000-8000-000000009959',v_root,
     0,v_seq,'COLLECTING','PREPARE_SCOPE',1);
   v_cursor:=jsonb_build_object('cursor_kind','SCOPE_SELECT','cursor_version',1,'build_id',v_build,'candidate_id',v_candidate,
     'captured_candidate_generation',0,'captured_source_change_seq',v_seq);
   UPDATE public.banking_pay_workbench_jobs SET economic_build_id=v_build,private_stage='PREPARE_SCOPE',
     private_cursor_kind='SCOPE_SELECT',private_stage_version=1,private_cursor_json=v_cursor WHERE id=v_root;
  END IF;
  v_reply:=public.pay_workbench_enqueue_stage_continuation(p_session_id=>v_session.id,p_candidate_id=>v_candidate,p_job_type=>v_type,
    p_source_job_id=>CASE WHEN v_type='WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN v_root END,p_cursor_json=>v_cursor,
    p_actor_user_id=>v_session.actor_user_id,p_reason=>'DISPOSABLE_MODAL_STAGE_PROOF',p_limit=>10,
    p_result_json=>CASE WHEN v_type='WORKBENCH_SESSION_CLONE_REBASE' THEN jsonb_build_object('source_session_id',v_source)
      ELSE jsonb_build_object('source_change_seq',v_seq,'source_build_run_id','10000000-0000-4000-8000-000000009959')
        ||CASE WHEN v_type='WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN '{"next_action":"PREPARE_SCOPE"}'::jsonb ELSE '{}'::jsonb END END);
  IF v_type='WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    UPDATE public.banking_pay_workbench_jobs SET status='SUCCEEDED',completed_at_utc=now() WHERE id=v_root;
  END IF;
  v_job:=(v_reply->>'job_id')::uuid;
  SELECT payload_json INTO STRICT v_payload FROM public.banking_pay_workbench_jobs WHERE id=v_job;
  SELECT job_facts INTO STRICT v_facts FROM private.pay_workbench_modal_stage_job_facts_v2(v_session) WHERE job_type=v_type;
  IF v_facts->>'job_id' IS DISTINCT FROM v_job::text OR v_facts->'can_progress' IS DISTINCT FROM 'true'::jsonb THEN
    RAISE EXCEPTION 'ACTUAL_STAGE_CONTINUATION_NOT_CURRENT: % %',v_type,v_facts; END IF;
  -- A current source-only stage may omit its counter. The actual four-stage
  -- shape probe proves this is existing input, not a new relaxed job contract.
  IF v_type IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS','WORKBENCH_PREVIEW_ROWS_MATERIALISE','WORKBENCH_SESSION_SCOPE_SEED','WORKBENCH_SESSION_CLONE_REBASE') THEN
   IF v_payload ? 'source_change_seq' THEN RAISE EXCEPTION 'UNEXPECTED_STAGE_SEQUENCE_SHAPE: %',v_type;END IF;
  END IF;
  FOR v_bad IN SELECT value FROM jsonb_array_elements(jsonb_build_array(
    v_payload||'{"session_version":2}',v_payload||'{"session_version":"bad"}',
    v_payload||'{"session_id":"10000000-0000-4000-8000-000000009999"}',
    v_payload||'{"candidate_id":"10000000-0000-4000-8000-000000000003"}',
    v_payload||'{"snapshot_run_id":"10000000-0000-4000-8000-000000009999"}',
    v_payload||'{"session_signature":"different-session"}',v_payload||'{"source_change_seq":"stale-or-invalid"}')) LOOP
    UPDATE public.banking_pay_workbench_jobs SET payload_json=v_bad WHERE id=v_job;
    SELECT count(*) INTO v_count FROM private.pay_workbench_modal_stage_job_facts_v2(v_session) WHERE job_type=v_type;
    IF v_count<>0 THEN RAISE EXCEPTION 'STALE_STAGE_BINDING_ACCEPTED: %',v_type;END IF;
  END LOOP;
  UPDATE public.banking_pay_workbench_jobs SET payload_json=v_payload,status='FAILED',failed_at_utc=now() WHERE id=v_job;
  SELECT job_facts INTO STRICT v_facts FROM private.pay_workbench_modal_stage_job_facts_v2(v_session) WHERE job_type=v_type;
  IF v_facts->'is_failed' IS DISTINCT FROM 'true'::jsonb OR v_facts->'can_progress' IS DISTINCT FROM 'false'::jsonb THEN
    RAISE EXCEPTION 'FAILED_STAGE_APPEARS_UPDATING: %',v_type;END IF;
  INSERT INTO public.banking_pay_workbench_jobs(id,session_id,candidate_id,snapshot_run_id,job_type,dedupe_key,status,payload_json,
    economic_build_id,private_stage,private_cursor_kind,private_stage_version,private_cursor_json)
  SELECT v_successor,session_id,candidate_id,snapshot_run_id,job_type,dedupe_key,'QUEUED',payload_json,
    economic_build_id,private_stage,private_cursor_kind,private_stage_version,private_cursor_json FROM public.banking_pay_workbench_jobs WHERE id=v_job;
  SELECT job_facts INTO STRICT v_facts FROM private.pay_workbench_modal_stage_job_facts_v2(v_session) WHERE job_type=v_type;
  IF v_facts->>'job_id' IS DISTINCT FROM v_successor::text OR v_facts->'can_progress' IS DISTINCT FROM 'true'::jsonb THEN
    RAISE EXCEPTION 'CURRENT_STAGE_SUCCESSOR_NOT_ADOPTED: %',v_type;END IF;
  UPDATE public.banking_pay_workbench_jobs SET status='SUCCEEDED',completed_at_utc=now() WHERE id=v_successor;
  SELECT count(*) INTO v_count FROM private.pay_workbench_modal_stage_job_facts_v2(v_session) WHERE job_type=v_type;
  IF v_count<>0 THEN RAISE EXCEPTION 'COMPLETED_STAGE_REPUBLISHED_OLD_FAILURE: %',v_type;END IF;
  DELETE FROM public.banking_pay_workbench_jobs WHERE id=v_successor;
  UPDATE public.banking_pay_workbench_jobs SET status='QUEUED',failed_at_utc=NULL WHERE id=v_job;
  IF v_type='WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN
    UPDATE private.banking_pay_workbench_economic_builds SET private_stage='WORKSPACE_FACT' WHERE id=v_build;
    SELECT job_facts INTO STRICT v_facts FROM private.pay_workbench_modal_stage_job_facts_v2(v_session) WHERE job_type=v_type;
    IF v_facts->'can_progress' IS DISTINCT FROM 'false'::jsonb OR v_facts->>'blocked_code' IS DISTINCT FROM 'STAGE_BUILD_NOT_CURRENT' THEN
      RAISE EXCEPTION 'STALE_BUILD_PHASE_SHOWN_UPDATING';END IF;
    UPDATE private.banking_pay_workbench_economic_builds SET private_stage='PREPARE_SCOPE',captured_candidate_generation=1 WHERE id=v_build;
    SELECT job_facts INTO STRICT v_facts FROM private.pay_workbench_modal_stage_job_facts_v2(v_session) WHERE job_type=v_type;
    IF v_facts->'can_progress' IS DISTINCT FROM 'false'::jsonb THEN RAISE EXCEPTION 'STALE_BUILD_GENERATION_SHOWN_UPDATING';END IF;
    UPDATE private.banking_pay_workbench_economic_builds SET captured_candidate_generation=0,status='FAILED',failed_at_utc=clock_timestamp() WHERE id=v_build;
    SELECT job_facts INTO STRICT v_facts FROM private.pay_workbench_modal_stage_job_facts_v2(v_session) WHERE job_type=v_type;
    IF v_facts->'can_progress' IS DISTINCT FROM 'false'::jsonb THEN RAISE EXCEPTION 'FAILED_BUILD_SHOWN_UPDATING';END IF;
    UPDATE private.banking_pay_workbench_economic_builds SET status='COLLECTING',failed_at_utc=NULL WHERE id=v_build;
  END IF;
  IF v_type='WORKBENCH_SESSION_CLONE_REBASE' THEN
    UPDATE public.banking_pay_workbench_jobs SET payload_json=v_payload-'source_session_id' WHERE id=v_job;
    SELECT job_facts INTO STRICT v_facts FROM private.pay_workbench_modal_stage_job_facts_v2(v_session) WHERE job_type=v_type;
    IF v_facts->'can_progress' IS DISTINCT FROM 'false'::jsonb OR v_facts->>'blocked_code' IS DISTINCT FROM 'STAGE_CLONE_SOURCE_MISSING' THEN
      RAISE EXCEPTION 'CLONE_WITHOUT_SOURCE_APPEARS_UPDATING';END IF;
    UPDATE public.banking_pay_workbench_jobs SET payload_json=v_payload WHERE id=v_job;
  END IF;
 END LOOP;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_stage_job_facts_v2(v_session);
 IF v_count<>7 THEN RAISE EXCEPTION 'STAGE_COVERAGE_INCOMPLETE: %',v_count;END IF;
 SELECT md5(jsonb_agg(to_jsonb(j) ORDER BY j.id)::text) INTO v_before FROM public.banking_pay_workbench_jobs j WHERE session_id=v_session.id;
 PERFORM * FROM private.pay_workbench_modal_stage_job_facts_v2(v_session);
 SELECT md5(jsonb_agg(to_jsonb(j) ORDER BY j.id)::text) INTO v_after FROM public.banking_pay_workbench_jobs j WHERE session_id=v_session.id;
 IF v_before IS DISTINCT FROM v_after THEN RAISE EXCEPTION 'STAGE_READ_MUTATED_JOBS';END IF;
 SET LOCAL client_min_messages='notice';
 RAISE NOTICE 'PASS: seven actual continuation families; optional sequence, 49 stale-binding cases, failed/completed/successor and clone-source boundaries; reads unchanged.';
END;
$stage_jobs$;
ROLLBACK;
