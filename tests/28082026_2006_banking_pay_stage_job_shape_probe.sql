\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
DO $probe$
DECLARE v_type text;v_reply jsonb;v_job public.banking_pay_workbench_jobs%ROWTYPE;
 v_candidate uuid;v_error text;
BEGIN
 FOREACH v_type IN ARRAY ARRAY['WORKBENCH_CANDIDATE_LINE_WORK_PROCESS','WORKBENCH_PREVIEW_ROWS_MATERIALISE','WORKBENCH_SESSION_SCOPE_SEED','WORKBENCH_SESSION_CLONE_REBASE'] LOOP
  v_candidate:=CASE WHEN v_type IN ('WORKBENCH_SESSION_SCOPE_SEED','WORKBENCH_SESSION_CLONE_REBASE') THEN NULL::uuid ELSE '10000000-0000-4000-8000-000000000002'::uuid END;
  v_reply:=public.pay_workbench_enqueue_stage_continuation(
    p_session_id=>'10000000-0000-4000-8000-000000000005',p_candidate_id=>v_candidate,p_job_type=>v_type,
    p_actor_user_id=>'10000000-0000-4000-8000-000000000001',p_reason=>'DISPOSABLE_MODAL_SHAPE_PROOF',p_limit=>10);
  SELECT * INTO STRICT v_job FROM public.banking_pay_workbench_jobs WHERE id=(v_reply->>'job_id')::uuid;
  IF v_job.payload_json->>'session_id' IS DISTINCT FROM v_job.session_id::text
    OR v_job.payload_json->>'session_version' IS DISTINCT FROM '1'
    OR v_job.payload_json->>'session_signature' IS DISTINCT FROM 'synthetic candidate selection only'
    OR v_job.payload_json->>'candidate_id' IS DISTINCT FROM v_candidate::text THEN
    RAISE EXCEPTION 'CONTINUATION_CURRENT_BINDING_SHAPE';END IF;
  SET LOCAL client_min_messages='notice';
  RAISE NOTICE 'Current continuation %: exact context=true; source_sequence_present=%; build_present=%',v_type,
    v_job.payload_json ? 'source_change_seq',v_job.economic_build_id IS NOT NULL;
  SET LOCAL client_min_messages='warning';
 END LOOP;
END;
$probe$;
ROLLBACK;
