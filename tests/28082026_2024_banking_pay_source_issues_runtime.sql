\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
DO $source_issues$
DECLARE v_session public.banking_pay_workbench_sessions%ROWTYPE;v_count integer;v_sources integer;v_keys integer;v_state text;v_status text;
 v_candidate uuid:='10000000-0000-4000-8000-000000000002';v_other uuid:='10000000-0000-4000-8000-000000000003';
 v_sourceonly uuid:='10000000-0000-4000-8000-000000009911';v_reply jsonb;v_job uuid;v_before text;v_after text;
 v_progress jsonb:='{"next_recommended_action":"REFRESH_OR_RETRY"}';v_options jsonb;
BEGIN
 UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
 SELECT * INTO STRICT v_session FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 v_options:=jsonb_build_object('expected_session_version',v_session.version,'expected_progress_counter_version',v_session.progress_counter_version,
   'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'ALL'));
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL',v_progress,true);
 IF v_count<>0 THEN RAISE EXCEPTION 'CURRENT_SOURCE_BECAME_ISSUE';END IF;
 FOREACH v_status IN ARRAY ARRAY['FAILED','ERROR','LINE_WORK_ERROR','LINE_WORK_PROCESS_ERROR','SOURCE_BUILD_ERROR','SOURCE_BUILD_PENDING','PENDING','SEEDING'] LOOP
  UPDATE public.banking_pay_workbench_session_scope SET status=v_status WHERE session_id=v_session.id AND candidate_id=v_candidate;
  SELECT count(*),count(*) FILTER(WHERE source_kind='SOURCE_PROGRESS'),count(DISTINCT task_key),min(task_json->>'state')
    INTO v_count,v_sources,v_keys,v_state FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL',v_progress,true);
  IF v_count<>110 OR v_sources<>1 OR v_keys<>1 OR v_state<>'ACTION_REQUIRED' THEN
    RAISE EXCEPTION 'COMPLETE_SOURCE_ISSUE_FAILED: % % % % %',v_status,v_count,v_sources,v_keys,v_state;END IF;
  IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL',v_progress,true)
    WHERE (source_kind='PREVIEW_ROW' AND (preview_row_id IS NULL OR task_json->'context_only'<>'true'::jsonb))
      OR (source_kind='SOURCE_PROGRESS' AND preview_row_id IS NOT NULL)
      OR task_json->'affected_payment_count_complete' IS DISTINCT FROM 'false'::jsonb) THEN
    RAISE EXCEPTION 'SOURCE_CONTEXT_OR_COUNT_INVENTED';END IF;
  SELECT count(*) INTO v_count FROM private.pay_workbench_modal_ready_members_v2(v_session,'ALL') WHERE candidate_id=v_candidate;
  IF v_count<>0 THEN RAISE EXCEPTION 'NONCURRENT_SOURCE_STILL_READY: %',v_status;END IF;
  SELECT count(*) INTO v_count FROM private.pay_workbench_modal_issue_index_v2(v_session,'ALL','REVOLUT','SANDBOX',v_progress,true,true);
  IF v_count<>2 THEN RAISE EXCEPTION 'SOURCE_INDEX_LOST_PASSIVE_OR_RECOUNTED_STALE_PAYMENT: %',v_count;END IF;
  BEGIN
   PERFORM public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,v_candidate,v_options,v_session.actor_user_id,NULL,100);
   RAISE EXCEPTION 'NONCURRENT_READY_CHILD_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_NOT_READY' THEN RAISE;END IF;END;
  BEGIN
   PERFORM public.pay_workbench_session_set_selected_rows(v_session.id,jsonb_build_object('modal_candidate_intent_v2',jsonb_build_object(
     'candidate_id',v_candidate,'request_id','10000000-0000-4000-8000-000000009919','action','SELECT_ALL_READY','options',v_options)),v_session.actor_user_id);
   RAISE EXCEPTION 'NONCURRENT_CANDIDATE_SELECTION_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_NOT_READY' THEN RAISE;END IF;END;
 END LOOP;
 -- Only the already-published global action is offered. No Retry from a word.
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL',v_progress,false)
   WHERE task_json->>'state'<>'BLOCKED' OR task_json->>'action' IS NOT NULL;
 IF v_count<>0 THEN RAISE EXCEPTION 'SOURCE_GRANTED_UNAVAILABLE_ACTION';END IF;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL','{"next_recommended_action":"WAIT_FOR_WORKER"}',true)
   WHERE task_json->>'state'<>'BLOCKED' OR task_json->>'action' IS NOT NULL;
 IF v_count<>0 THEN RAISE EXCEPTION 'SOURCE_INVENTED_ACTION_FROM_WAIT';END IF;
 v_reply:=public.pay_workbench_enqueue_stage_continuation(p_session_id=>v_session.id,p_candidate_id=>v_candidate,
   p_job_type=>'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',p_actor_user_id=>v_session.actor_user_id,p_reason=>'DISPOSABLE_SOURCE_ISSUE_PROOF',p_limit=>10);
 v_job:=(v_reply->>'job_id')::uuid;
 SELECT * INTO STRICT v_session FROM public.banking_pay_workbench_sessions WHERE id=v_session.id;
 IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL',v_progress,true) WHERE task_json->>'state'<>'UPDATING') THEN
   RAISE EXCEPTION 'VALID_LINE_WORK_NOT_UPDATING';END IF;
 UPDATE public.banking_pay_workbench_jobs SET status='FAILED',failed_at_utc=clock_timestamp() WHERE id=v_job;
 IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL',v_progress,true) WHERE task_json->>'state'<>'ACTION_REQUIRED') THEN
   RAISE EXCEPTION 'FAILED_LINE_WORK_STUCK_UPDATING';END IF;
 -- A shared Refresh action is one task, with every candidate retained.
 UPDATE public.banking_pay_workbench_session_scope SET status='FAILED' WHERE session_id=v_session.id AND candidate_id=v_other;
 SELECT count(DISTINCT task_key),count(DISTINCT candidate_id) INTO v_keys,v_count
   FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL',v_progress,true);
 IF v_keys<>1 OR v_count<>2 THEN RAISE EXCEPTION 'GLOBAL_REFRESH_DUPLICATED_OR_LOST_MEMBER';END IF;
 -- No physical payment: keep the real candidate source, no fake payment.
 INSERT INTO public.candidates(id,display_name,tms_ref,pay_method) VALUES(v_sourceonly,'Source only fixture','SOURCE-ONLY','PAYE');
 INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty)
 VALUES(v_session.id,v_sourceonly,3,'FAILED',true,false);
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'PAYE',v_progress,true)
   WHERE candidate_id=v_sourceonly AND source_kind='SOURCE_PROGRESS' AND preview_row_id IS NULL;
 IF v_count<>1 THEN RAISE EXCEPTION 'SOURCE_ONLY_ISSUE_LOST';END IF;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'UMBRELLA',v_progress,true) WHERE candidate_id=v_sourceonly;
 IF v_count<>0 THEN RAISE EXCEPTION 'SOURCE_ONLY_CHANNEL_LEAK';END IF;
 -- An all-hidden physical candidate must not reappear through source fallback.
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||'{"presentation_role":"HIDDEN_INDEFINITE_SNOOZE"}' WHERE session_id=v_session.id AND candidate_id=v_candidate;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL',v_progress,true) WHERE candidate_id=v_candidate;
 IF v_count<>0 THEN RAISE EXCEPTION 'INDEFINITE_SOURCE_FALLBACK_LEAK';END IF;
 v_session.filters_json:=jsonb_build_object('candidate_id',v_sourceonly);
 SELECT count(*),count(DISTINCT candidate_id) INTO v_count,v_keys FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL',v_progress,true);
 IF v_count<>1 OR v_keys<>1 THEN RAISE EXCEPTION 'SOURCE_CANDIDATE_FILTER_LEAK';END IF;
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO v_before FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id;
 PERFORM * FROM private.pay_workbench_modal_source_issue_members_v2(v_session,'ALL',v_progress,true);
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO v_after FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id;
 IF v_before IS DISTINCT FROM v_after THEN RAISE EXCEPTION 'SOURCE_READ_CHANGED_PAYMENTS';END IF;
 SET LOCAL client_min_messages='notice';
 RAISE NOTICE 'PASS: complete source issues and 110-member details; current work/failed work; eight noncurrent Ready fences; honest source-only count; global Refresh grouping; hidden/channel/candidate filters; no payment write.';
END;
$source_issues$;
ROLLBACK;
