\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
CREATE TEMP TABLE modal_summary_results(label text,payload jsonb) ON COMMIT DROP;
UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
SELECT ('10000000-0000-4000-8000-'||lpad((6000+n)::text,12,'0'))::uuid,'Summary fixture '||lpad(n::text,3,'0'),'SUMMARY-'||n,'PAYE'
FROM generate_series(1,103) n;
INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty,certified_preview_publication_attestation_json)
SELECT '10000000-0000-4000-8000-000000000005'::uuid,('10000000-0000-4000-8000-'||lpad((6000+n)::text,12,'0'))::uuid,n+2,'READY',true,false,
 '{"attestation_version":"CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3","contract_version":"3","semantic_contract_version":"READY_TO_PAY_SEMANTIC_V2"}'::jsonb
FROM generate_series(1,103) n;
INSERT INTO public.banking_pay_workbench_preview_rows(id,session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
SELECT ('10000000-0000-4000-8000-'||lpad((7000+n)::text,12,'0'))::uuid,'10000000-0000-4000-8000-000000000005'::uuid,
 ('10000000-0000-4000-8000-'||lpad((6000+n)::text,12,'0'))::uuid,'canonical_preview_lines','summary-fixture:'||n,300+n,
 '{"pay_channel":"PAYE","amount_display":"10.00","section_amount_display":"10.00","amount_ex_vat":"10.00","presentation_section":"READY_TO_PAY","presentation_role":"ALLOCATION_COMPONENT","line_type":"TIMESHEET_PAYMENT","selection_allowed":true,"draftable":true,"is_ready_for_draft":true}',
 'SOURCE_REF','summary-fixture:'||n,true,'SELECTED','READY',1 FROM generate_series(1,103) n;
DO $summary$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;first_page jsonb;next_page jsonb;again jsonb;g jsonb;
 v_before text;v_after text;v_code text;v_sort text;v_direction text;v_change jsonb;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,'pay_channel_scope','ALL');
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO v_before FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 first_page:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',NULL,100);
 IF first_page->>'total_count'<>'105' OR jsonb_array_length(first_page->'rows')<>100 OR first_page->>'has_more'<>'true'
   OR first_page#>>'{global,selected_ready_display_amount}'<>'2120.00' OR first_page#>>'{global,blocked_count}'<>'3'
   OR first_page#>>'{global,action_required_count}'<>'0' OR first_page#>>'{global,updating_count}'<>'0' THEN
   RAISE EXCEPTION 'SUMMARY_COMPLETE_SCOPE_OR_COUNTERS: total %, rows %, more %, amount %, blocked %, actions %, updating %',
     first_page->>'total_count',jsonb_array_length(first_page->'rows'),first_page->>'has_more',
     first_page#>>'{global,selected_ready_display_amount}',first_page#>>'{global,blocked_count}',
     first_page#>>'{global,action_required_count}',first_page#>>'{global,updating_count}';END IF;
 IF (SELECT sum((r->>'selected_display_amount')::numeric) FROM jsonb_array_elements(first_page->'rows') r)=2120 THEN
   RAISE EXCEPTION 'SUMMARY_FIXTURE_DID_NOT_PROVE_OFF_PAGE_TOTAL';END IF;
 opts:=opts||jsonb_build_object('scope_hash',first_page->>'scope_hash');
 next_page:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',first_page->>'next_cursor',100);
 IF jsonb_array_length(next_page->'rows')<>5 OR next_page->>'page_number'<>'2' OR next_page->>'has_more'<>'false'
   OR next_page->'global' IS DISTINCT FROM first_page->'global' THEN RAISE EXCEPTION 'SUMMARY_NEXT_PAGE_LOST_GLOBAL';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(first_page->'rows') a JOIN jsonb_array_elements(next_page->'rows') b ON a->>'candidate_id'=b->>'candidate_id') THEN
   RAISE EXCEPTION 'SUMMARY_DUPLICATED_PAGE_CANDIDATE';END IF;
 INSERT INTO modal_summary_results VALUES('first',first_page),('next',next_page);
 g:=private.pay_workbench_modal_draft_gate_v2(s.id,212);
 IF first_page#>'{global,draft}' IS DISTINCT FROM g THEN RAISE EXCEPTION 'SUMMARY_CHANGED_DRAFT_OWNER';END IF;
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO v_after FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 IF v_before IS DISTINCT FROM v_after THEN RAISE EXCEPTION 'SUMMARY_READ_CHANGED_SELECTION';END IF;
 BEGIN
  PERFORM public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts||'{"expected_progress_counter_version":3}',s.actor_user_id);
  RAISE EXCEPTION 'SUMMARY_STALE_REQUEST_ACCEPTED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_REVISION' THEN RAISE;END IF;END;
 BEGIN
  PERFORM public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,'10000000-0000-4000-8000-000000009999');
  RAISE EXCEPTION 'SUMMARY_UNAUTHORISED_REQUEST_ACCEPTED';
 EXCEPTION WHEN insufficient_privilege THEN IF SQLERRM<>'BANKING_PAY_V2_UNAUTHORISED' THEN RAISE;END IF;END;
END;
$summary$;
-- Each sort is a separate API operation. The existing PREVIEW_PROGRESS owner
-- sets the subsequent statement budget to3 seconds. Do not override it here.
CREATE FUNCTION pg_temp.verify_summary_sort(p_sort text,p_direction text) RETURNS void LANGUAGE plpgsql AS $proof$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;expected jsonb;actual jsonb;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 SELECT payload->'global' INTO STRICT expected FROM modal_summary_results WHERE label='first';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
  'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 actual:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,p_sort,p_direction,NULL,100);
 IF actual->'global' IS DISTINCT FROM expected THEN RAISE EXCEPTION 'SORT_CHANGED_FINANCIAL_SCOPE';END IF;
 INSERT INTO modal_summary_results VALUES(p_sort||'_'||p_direction,actual);
END;
$proof$;
SELECT pg_temp.verify_summary_sort('CANDIDATE','ASC');
SELECT pg_temp.verify_summary_sort('CANDIDATE','DESC');
SELECT pg_temp.verify_summary_sort('DEDUCTIONS','ASC');
SELECT pg_temp.verify_summary_sort('DEDUCTIONS','DESC');
SELECT pg_temp.verify_summary_sort('READY_TO_PAY','ASC');
SELECT pg_temp.verify_summary_sort('READY_TO_PAY','DESC');
DO $selection_summary$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;v_change jsonb;again jsonb;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
  'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 v_change:=public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('modal_candidate_intent_v2',jsonb_build_object(
   'candidate_id','10000000-0000-4000-8000-000000000002','request_id','10000000-0000-4000-8000-000000009999',
   'action','CLEAR_ALL_READY','options',opts)),s.actor_user_id);
 opts:=jsonb_set(opts,'{expected_progress_counter_version}',v_change->'progress_counter_version');
 again:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id);
 IF again#>>'{global,selected_ready_display_amount}'<>'1050.00'
   OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(again->'rows') r WHERE r->>'candidate_id'='10000000-0000-4000-8000-000000000002'
     AND r->>'selection_state'='NONE' AND r->>'selected_display_amount'='0.00' AND r->'selected_deduction_exists'='false'::jsonb) THEN
   RAISE EXCEPTION 'SUMMARY_LOST_UNCHECKED_CANDIDATE_OR_TOTAL';END IF;
 INSERT INTO modal_summary_results VALUES('after_clear',again);
 SET LOCAL client_min_messages='notice';
 RAISE NOTICE 'PASS: actual public summary105 candidates across two pages; six sorts; complete selected headline; existing Draft owner; stale/auth negatives; actual candidate clear and no read writes.';
END;
$selection_summary$;
DO $navigation_after_selection$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;old_first jsonb;old_next jsonb;current_first jsonb;actual jsonb;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 SELECT payload INTO STRICT old_first FROM modal_summary_results WHERE label='first';
 SELECT payload INTO STRICT old_next FROM modal_summary_results WHERE label='next';
 SELECT payload INTO STRICT current_first FROM modal_summary_results WHERE label='after_clear';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
  'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 BEGIN
  PERFORM public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',old_first->>'next_cursor',100);
  RAISE EXCEPTION 'PUBLIC_NAVIGATION_RENEWED_ORDINARY_CURSOR';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
 actual:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',old_first->>'next_page_anchor',100);
 IF actual->'global' IS DISTINCT FROM current_first->'global' OR actual->>'page_number'<>'2'
  OR (SELECT jsonb_agg(r->'candidate_id' ORDER BY n) FROM jsonb_array_elements(actual->'rows') WITH ORDINALITY q(r,n))
   IS DISTINCT FROM (SELECT jsonb_agg(r->'candidate_id' ORDER BY n) FROM jsonb_array_elements(old_next->'rows') WITH ORDINALITY q(r,n)) THEN
  RAISE EXCEPTION 'PUBLIC_NEXT_ANCHOR_LOST_CURRENT_SELECTION';END IF;
 INSERT INTO modal_summary_results VALUES('next_after_clear',actual);
 actual:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',old_next->>'previous_page_anchor',100);
 IF actual IS DISTINCT FROM current_first THEN RAISE EXCEPTION 'PUBLIC_PREVIOUS_ANCHOR_LOST_CURRENT_SELECTION';END IF;
 INSERT INTO modal_summary_results VALUES('previous_after_clear',actual);
END;
$navigation_after_selection$;
SELECT jsonb_build_object('label',label,'payload',payload) FROM modal_summary_results ORDER BY label;
ROLLBACK;
