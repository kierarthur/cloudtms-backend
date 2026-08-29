\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
UPDATE public.banking_pay_workbench_preview_rows SET section='cases_resolutions',selected=false,selection_state='NOT_SELECTABLE',
 row_json=row_json||jsonb_build_object('presentation_section','CASES_RESOLUTIONS','readiness_state','CASES_RESOLUTIONS',
 'case_key','finance:10000000-0000-4000-8000-000000003001','finance_case_id','10000000-0000-4000-8000-000000003001',
 'resolution_family','NON_BUCKET','case_needs_resolution',true,'case_resolution_satisfied_now',false)
WHERE session_id='10000000-0000-4000-8000-000000000005' AND row_ordinal<=105;
CREATE TEMP TABLE modal_detail_results(kind text,key text,cursor text,limit_value integer,payload jsonb,summary jsonb) ON COMMIT DROP;
DO $details$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;summary jsonb;k text;b text;first_page jsonb;next_page jsonb;
 v_before text;v_after text;v_key text;bad jsonb;v_error text;v_state text;v_kind text;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,'pay_channel_scope','ALL',
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'));
 summary:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id);
 SELECT identity INTO STRICT k FROM private.pay_workbench_modal_issue_index_v2(s,'ALL','CSV','PROD',summary#>'{global,draft}',true,true)
  WHERE issue_state='ACTION_REQUIRED';
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO v_before FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 first_page:=public.pay_workbench_session_get_action_required_detail_v1(s.id,opts,s.actor_user_id,k,NULL,100);
 IF first_page->>'total_count'<>'106' OR first_page->>'affected_payment_count'<>'105'
  OR first_page->'affected_payment_count_complete' IS DISTINCT FROM 'true'::jsonb OR jsonb_array_length(first_page->'rows')<>100
  OR first_page->>'has_more'<>'true' OR first_page->>'page_number'<>'1' THEN RAISE EXCEPTION 'DETAIL_FIRST_PAGE_INCOMPLETE';END IF;
 next_page:=public.pay_workbench_session_get_action_required_detail_v1(s.id,opts,s.actor_user_id,k,first_page->>'next_cursor',100);
 IF next_page->>'total_count'<>'106' OR jsonb_array_length(next_page->'rows')<>6 OR next_page->>'page_number'<>'2'
  OR next_page->>'has_more'<>'false' OR next_page->'previous_cursor'<>'null'::jsonb THEN RAISE EXCEPTION 'DETAIL_NEXT_PAGE_INCOMPLETE';END IF;
 IF (SELECT count(DISTINCT r->>'identity') FROM jsonb_array_elements((first_page->'rows')||(next_page->'rows')) r)<>106 THEN
  RAISE EXCEPTION 'DETAIL_DUPLICATED_OR_LOST_MEMBER';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements((first_page->'rows')||(next_page->'rows')) m
  JOIN public.banking_pay_workbench_preview_rows r ON r.id=(m->>'preview_row_id')::uuid
  WHERE m->'payload' IS DISTINCT FROM private.pay_workbench_modal_row_payload_v2(r)) THEN RAISE EXCEPTION 'DETAIL_CHANGED_ORIGINAL_PAYLOAD';END IF;
 IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements((first_page->'rows')||(next_page->'rows')) m
  WHERE m->>'preview_row_id'='10000000-0000-4000-8000-000000002001' AND m->'context_only'='true'::jsonb) THEN
  RAISE EXCEPTION 'DETAIL_LOST_PASSIVE_CONTEXT';END IF;
 INSERT INTO modal_detail_results VALUES('actions',k,NULL,100,first_page,summary),('actions',k,first_page->>'next_cursor',100,next_page,summary);
 SELECT identity INTO b FROM private.pay_workbench_modal_issue_index_v2(s,'ALL','CSV','PROD',summary#>'{global,draft}',true,true)
  WHERE issue_state='BLOCKED' ORDER BY identity LIMIT 1;
 next_page:=public.pay_workbench_session_get_blocked_detail_v1(s.id,opts,s.actor_user_id,b,NULL,100);
 IF next_page->>'total_count'<>'1' OR next_page->>'affected_payment_count'<>'1' THEN RAISE EXCEPTION 'BLOCKED_DETAIL_WRONG_IDENTITY';END IF;
 INSERT INTO modal_detail_results VALUES('blocked',b,NULL,100,next_page,summary);
 -- Every typed cursor binding is tested against the real reader.
 FOREACH v_key IN ARRAY ARRAY['kind','session_id','session_version','progress_counter_version','scope_hash','issue_identity','limit'] LOOP
  bad:=private.pay_workbench_modal_cursor_decode_v2(first_page->>'next_cursor','{}');
  bad:=jsonb_set(bad,ARRAY[v_key],CASE WHEN v_key IN ('session_version','progress_counter_version','limit') THEN '999'::jsonb ELSE '"other"'::jsonb END);
  BEGIN
   PERFORM public.pay_workbench_session_get_action_required_detail_v1(s.id,opts,s.actor_user_id,k,private.pay_workbench_modal_cursor_encode_v2(bad),100);
   RAISE EXCEPTION 'BAD_DETAIL_BINDING_ACCEPTED: %',v_key;
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
 END LOOP;
 BEGIN
  PERFORM public.pay_workbench_session_get_blocked_detail_v1(s.id,opts,s.actor_user_id,k,NULL,100);
  RAISE EXCEPTION 'ACTION_TASK_ACCEPTED_AS_BLOCKED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_ITEM_NOT_CURRENT' THEN RAISE;END IF;END;
 BEGIN
  PERFORM public.pay_workbench_session_get_action_required_detail_v1(s.id,opts,'10000000-0000-4000-8000-000000009999',k,NULL,100);
  RAISE EXCEPTION 'DETAIL_UNAUTHORISED_ACCEPTED';
 EXCEPTION WHEN insufficient_privilege THEN IF SQLERRM<>'BANKING_PAY_V2_UNAUTHORISED' THEN RAISE;END IF;END;
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO v_after FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 IF v_after IS DISTINCT FROM v_before THEN RAISE EXCEPTION 'DETAIL_READ_MUTATED';END IF;
 -- Three pages prove an exact server-owned Previous cursor, not browser offsets.
 next_page:=public.pay_workbench_session_get_action_required_detail_v1(s.id,opts,s.actor_user_id,k,NULL,40);
 next_page:=public.pay_workbench_session_get_action_required_detail_v1(s.id,opts,s.actor_user_id,k,next_page->>'next_cursor',40);
 v_key:=next_page->>'next_cursor';
 next_page:=public.pay_workbench_session_get_action_required_detail_v1(s.id,opts,s.actor_user_id,k,v_key,40);
 IF next_page->>'page_number'<>'3' OR jsonb_array_length(next_page->'rows')<>26 OR next_page->>'previous_cursor' IS NULL THEN
  RAISE EXCEPTION 'DETAIL_PREVIOUS_CURSOR_MISSING';END IF;
 INSERT INTO modal_detail_results VALUES('actions',k,v_key,40,next_page,summary);
 next_page:=public.pay_workbench_session_get_action_required_detail_v1(s.id,opts,s.actor_user_id,k,next_page->>'previous_cursor',40);
 IF next_page->>'page_number'<>'2' OR jsonb_array_length(next_page->'rows')<>40 THEN RAISE EXCEPTION 'DETAIL_PREVIOUS_CURSOR_WRONG_PAGE';END IF;
 -- Failed source has source-only evidence plus old payment context, never
 -- a fabricated count or a selectable payment in the candidate breakdown.
 UPDATE public.banking_pay_workbench_session_scope SET status='FAILED'
 WHERE session_id=s.id AND candidate_id='10000000-0000-4000-8000-000000000002';
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
 summary:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id);
 SELECT i.identity,i.issue_state INTO STRICT k,v_state
 FROM private.pay_workbench_modal_issue_index_v2(s,'ALL','CSV','PROD',summary#>'{global,draft}',true,true) i
 WHERE i.task_family='SOURCE_PROGRESS';
 v_kind:=CASE WHEN v_state='BLOCKED' THEN 'blocked' ELSE 'actions' END;
 next_page:=CASE WHEN v_kind='blocked' THEN public.pay_workbench_session_get_blocked_detail_v1(s.id,opts,s.actor_user_id,k,NULL,100)
  ELSE public.pay_workbench_session_get_action_required_detail_v1(s.id,opts,s.actor_user_id,k,NULL,100) END;
 IF next_page->>'total_count'<>'110' OR next_page->'affected_payment_count_complete' IS DISTINCT FROM 'false'::jsonb
  OR next_page->'affected_payment_count'<>'null'::jsonb THEN RAISE EXCEPTION 'PUBLIC_SOURCE_DETAIL_INVENTED_CURRENT_PAYMENT_COUNT';END IF;
 INSERT INTO modal_detail_results VALUES(v_kind,k,NULL,100,next_page,summary);
 SET LOCAL client_min_messages='notice';
 RAISE NOTICE 'PASS: public issue detail106 complete members; original payload/action/context retained; strict cursor/auth/current-owner negatives; no writes.';
END;
$details$;
SELECT jsonb_build_object('kind',kind,'key',key,'cursor',cursor,'limit',limit_value,'payload',payload,'summary',summary) FROM modal_detail_results;
ROLLBACK;
