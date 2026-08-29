\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
UPDATE public.banking_pay_workbench_preview_rows SET section='cases_resolutions',selected=false,selection_state='NOT_SELECTABLE',
 row_json=row_json||jsonb_build_object('presentation_section','CASES_RESOLUTIONS','readiness_state','CASES_RESOLUTIONS',
 'case_key','finance:10000000-0000-4000-8000-000000003001','finance_case_id','10000000-0000-4000-8000-000000003001',
 'resolution_family','NON_BUCKET','case_needs_resolution',true,'case_resolution_satisfied_now',false)
WHERE session_id='10000000-0000-4000-8000-000000000005' AND row_ordinal<=105;
DO $tasks$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;t record;n bigint;before_rows text;after_rows text;candidate_reference text;
 p jsonb:='{"next_recommended_action":"REFRESH_OR_RETRY"}';job jsonb;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 SELECT * INTO STRICT t FROM private.pay_workbench_modal_task_summaries_v2(s,'ALL','CSV','PROD',p,true,true);
 SELECT lower(c.tms_ref) INTO STRICT candidate_reference FROM public.candidates c WHERE c.id='10000000-0000-4000-8000-000000000002';
 IF t.issue_state<>'ACTION_REQUIRED' OR t.title<>'Amount decision required' OR t.affected_candidate_count<>1
  OR t.affected_payment_count<>105 OR t.affected_payment_count_complete IS NOT TRUE OR position(candidate_reference IN t.search_text)=0 THEN
  RAISE EXCEPTION 'COMPLETE_CASE_TASK_SUMMARY: state=% title=% candidates=% payments=% complete=% search=%',
   t.issue_state,t.title,t.affected_candidate_count,t.affected_payment_count,t.affected_payment_count_complete,t.search_text;END IF;
 SELECT count(*) INTO n FROM private.pay_workbench_modal_issue_detail_members_v2(s,'ALL','CSV','PROD',p,t.identity,true);
 IF n<>106 THEN RAISE EXCEPTION 'CASE_CONTEXT_LOST';END IF;
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO before_rows FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 PERFORM * FROM private.pay_workbench_modal_task_summaries_v2(s,'ALL','CSV','PROD',p,true,true);
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO after_rows FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 IF before_rows IS DISTINCT FROM after_rows THEN RAISE EXCEPTION 'TASK_SUMMARY_CHANGED_PAYMENT';END IF;
 --105 different unresolved cases must remain105 tasks, not one candidate row.
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||jsonb_build_object(
  'case_key','finance:10000000-0000-4000-8000-'||lpad((30000+row_ordinal)::text,12,'0'),
  'finance_case_id','10000000-0000-4000-8000-'||lpad((30000+row_ordinal)::text,12,'0'))
 WHERE session_id=s.id AND row_ordinal<=105;
 SELECT count(*) INTO n FROM private.pay_workbench_modal_task_summaries_v2(s,'ALL','CSV','PROD',p,true,true)
 WHERE affected_payment_count=1 AND affected_payment_count_complete AND affected_candidate_count=1;
 IF n<>105 THEN RAISE EXCEPTION 'DISTINCT_TASKS_OR_COUNTS_LOST: %',n;END IF;
 s.filters_json:='{"candidate_id":"10000000-0000-4000-8000-000000000003"}';
 IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_task_summaries_v2(s,'ALL','CSV','PROD',p,true,true)) THEN
  RAISE EXCEPTION 'TASK_CANDIDATE_FILTER_LEAK';END IF;
 s.filters_json:='{}';
 UPDATE public.banking_pay_workbench_session_scope SET status='FAILED' WHERE session_id=s.id;
 SELECT * INTO STRICT t FROM private.pay_workbench_modal_task_summaries_v2(s,'ALL','CSV','PROD',p,true,true);
 IF t.task_family<>'SOURCE_PROGRESS' OR t.affected_candidate_count<>2 OR t.affected_payment_count IS NOT NULL
  OR t.affected_payment_count_complete IS NOT FALSE OR t.issue_state<>'ACTION_REQUIRED' THEN RAISE EXCEPTION 'UNKNOWN_SOURCE_COUNT_INVENTED';END IF;
 IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_task_summaries_v2(s,'ALL','CSV','PROD',p,true,false)) THEN
  RAISE EXCEPTION 'UNAVAILABLE_REFRESH_BECAME_ACTION';END IF;
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||'{"presentation_role":"HIDDEN_INDEFINITE_SNOOZE"}'
 WHERE session_id=s.id;
 IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_task_summaries_v2(s,'ALL','CSV','PROD',p,true,true)) THEN
  RAISE EXCEPTION 'HIDDEN_SOURCE_TASK_REAPPEARED';END IF;
 SET LOCAL client_min_messages='notice';
 RAISE NOTICE 'PASS: complete task summaries retain106-member case and105 independent tasks; exact counts and source unknowns; current filter and indefinite exclusion; unchanged payment rows.';
END;
$tasks$;
ROLLBACK;
