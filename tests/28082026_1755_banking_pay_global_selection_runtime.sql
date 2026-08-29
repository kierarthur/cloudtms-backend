\set ON_ERROR_STOP on
BEGIN;
SET LOCAL client_min_messages='warning';
SET LOCAL statement_timeout='45s';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
-- A second full candidate page, each candidate in both pay-channel partitions.
INSERT INTO public.candidates(id,display_name,tms_ref)
SELECT ('20000000-0000-4000-8000-'||lpad(n::text,12,'0'))::uuid,'Global fixture '||n,'GLOBAL-'||n
FROM generate_series(1,105) n;
INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty,certified_preview_publication_attestation_json)
SELECT '10000000-0000-4000-8000-000000000005'::uuid,
 ('20000000-0000-4000-8000-'||lpad(n::text,12,'0'))::uuid,10+n,'READY',true,false,
 '{"attestation_version":"CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3","contract_version":"3","semantic_contract_version":"READY_TO_PAY_SEMANTIC_V2"}'::jsonb
FROM generate_series(1,105) n;
INSERT INTO public.banking_pay_workbench_preview_rows(id,session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
SELECT ('30000000-0000-4000-8000-'||lpad((n*2+ch)::text,12,'0'))::uuid,
 '10000000-0000-4000-8000-000000000005'::uuid,('20000000-0000-4000-8000-'||lpad(n::text,12,'0'))::uuid,
 'canonical_preview_lines','global-fixture:'||n||':'||ch,500+n*2+ch,
 jsonb_build_object('candidate_name','Global fixture '||n,'pay_channel',CASE ch WHEN 0 THEN 'PAYE' ELSE 'UMBRELLA' END,
 'amount_display','10.00','section_amount_display','10.00','amount_ex_vat','10.00','presentation_section','READY_TO_PAY',
 'line_type','TIMESHEET_PAYMENT','presentation_role','ALLOCATION_COMPONENT','selection_allowed',true,'draftable',true,'is_ready_for_draft',true),
 'SOURCE_REF','global-fixture:'||n||':'||ch,true,'SELECTED','READY',1
FROM generate_series(1,105) n CROSS JOIN generate_series(0,1) ch;

DO $global_selection$
DECLARE
 s public.banking_pay_workbench_sessions%ROWTYPE;
 options jsonb; intent jsonb; result jsonb; original_input jsonb; rows_before jsonb; session_before jsonb;
 other_partition jsonb; audit_count bigint; error_text text; bad jsonb; expected text; check_count int:=0;
BEGIN
 SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 options:=jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',4,'pay_channel_scope','PAYE',
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'PAYE'));
 intent:=jsonb_build_object('modal_global_intent_v2',jsonb_build_object('request_id','40000000-0000-4000-8000-000000000001',
   'action','CLEAR_ALL_READY','options',options));
 SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO other_partition FROM public.banking_pay_workbench_preview_rows r
 WHERE session_id=s.id AND row_json->>'pay_channel'='UMBRELLA';
 SELECT count(*) INTO audit_count FROM public.audit_events WHERE object_id_text=s.id::text;
 result:=public.pay_workbench_session_set_selected_rows(s.id,intent,s.actor_user_id);
 IF result->>'ok' IS DISTINCT FROM 'true' OR result->>'progress_counter_version' IS DISTINCT FROM '5'
   OR result->>'final_ready_count' IS DISTINCT FROM '210' OR result->>'recovery_revalidation_count' IS DISTINCT FROM '106' THEN
   RAISE EXCEPTION 'GLOBAL_CLEAR_MUST_COVER_106_CANDIDATES_AND_210_PAYMENTS_ONCE'; END IF;
 SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
 IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_ready_members_v2(s,'PAYE') WHERE selected)
   OR other_partition IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r
     WHERE session_id=s.id AND row_json->>'pay_channel'='UMBRELLA') THEN RAISE EXCEPTION 'GLOBAL_CLEAR_SCOPE_MISMATCH'; END IF;
 IF s.selected_row_count<>109 OR jsonb_array_length(s.progress_json#>'{selection_intent_v1,canonical_preview_lines,selected_economic_identities}')<>109
   OR s.server_selected_preview_row_ids_provided IS NOT FALSE THEN RAISE EXCEPTION 'GLOBAL_CLEAR_LOST_FULL_SESSION_DRAFT_AUTHORITY'; END IF;
 IF (SELECT count(*) FROM public.audit_events WHERE object_id_text=s.id::text)<>audit_count+1 THEN RAISE EXCEPTION 'GLOBAL_CLEAR_AUDIT_COUNT'; END IF;
 original_input:=intent;
 SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO rows_before FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 session_before:=to_jsonb(s);
 IF public.pay_workbench_session_set_selected_rows(s.id,intent,s.actor_user_id) IS DISTINCT FROM result THEN RAISE EXCEPTION 'GLOBAL_RETRY_NOT_IDEMPOTENT'; END IF;
 IF rows_before IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id)
   OR session_before IS DISTINCT FROM (SELECT to_jsonb(x) FROM public.banking_pay_workbench_sessions x WHERE id=s.id) THEN RAISE EXCEPTION 'GLOBAL_RETRY_WROTE'; END IF;
 intent:=jsonb_set(jsonb_set(intent,'{modal_global_intent_v2,request_id}','"40000000-0000-4000-8000-000000000002"'),
   '{modal_global_intent_v2,options,expected_progress_counter_version}','5');
 result:=public.pay_workbench_session_set_selected_rows(s.id,intent,s.actor_user_id);
 IF result->>'state_changed' IS DISTINCT FROM 'false' OR result->>'progress_counter_version' IS DISTINCT FROM '5'
   OR session_before IS DISTINCT FROM (SELECT to_jsonb(x) FROM public.banking_pay_workbench_sessions x WHERE id=s.id) THEN RAISE EXCEPTION 'GLOBAL_NOOP_WROTE'; END IF;

 -- Closed request shape and stale/changed scope must reject without any write.
 FOR bad,expected IN SELECT input,code FROM (VALUES
  (jsonb_set(intent,'{modal_global_intent_v2,candidate_id}','"10000000-0000-4000-8000-000000000002"'),'BANKING_PAY_V2_INVALID_INPUT'),
  (intent||jsonb_build_object('modal_candidate_intent_v2',intent->'modal_global_intent_v2'),'BANKING_PAY_V2_INVALID_INPUT'),
  (jsonb_set(intent,'{modal_global_intent_v2,action}','"TOGGLE"'),'BANKING_PAY_V2_INVALID_INPUT'),
  (jsonb_set(intent,'{modal_global_intent_v2,request_id}','"bad"'),'BANKING_PAY_V2_INVALID_INPUT'),
  (jsonb_set(intent,'{modal_global_intent_v2,options,pay_channel_scope}','"UMBRELLA"'),'BANKING_PAY_V2_SCOPE_MISMATCH'),
  (jsonb_set(intent,'{modal_global_intent_v2,options,expected_progress_counter_version}','4'),'BANKING_PAY_V2_STALE_REVISION'),
  (jsonb_set(original_input,'{modal_global_intent_v2,action}','"SELECT_ALL_READY"'),'BANKING_PAY_V2_REQUEST_CONFLICT')
 ) f(input,code) LOOP
  BEGIN
   PERFORM public.pay_workbench_session_set_selected_rows(s.id,bad,s.actor_user_id);
   RAISE EXCEPTION 'GLOBAL_NEGATIVE_ACCEPTED';
  EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS error_text=MESSAGE_TEXT;
   IF error_text<>expected THEN RAISE EXCEPTION 'GLOBAL_WRONG_FAILURE: expected %, got %',expected,error_text; END IF;
  END;
  check_count:=check_count+1;
 END LOOP;
 IF check_count<>7 OR rows_before IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id)
   OR session_before IS DISTINCT FROM (SELECT to_jsonb(x) FROM public.banking_pay_workbench_sessions x WHERE id=s.id)
   OR (SELECT count(*) FROM public.audit_events WHERE object_id_text=s.id::text)<>audit_count+1 THEN RAISE EXCEPTION 'GLOBAL_NEGATIVES_CHANGED_STATE'; END IF;

 intent:=jsonb_set(jsonb_set(intent,'{modal_global_intent_v2,request_id}','"40000000-0000-4000-8000-000000000003"'),
   '{modal_global_intent_v2,action}','"SELECT_ALL_READY"');
 result:=public.pay_workbench_session_set_selected_rows(s.id,intent,s.actor_user_id);
 SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
 IF s.progress_counter_version<>6 OR result->>'final_ready_count' IS DISTINCT FROM '211'
   OR result->>'recovery_revalidation_count' IS DISTINCT FROM '106'
   OR EXISTS(SELECT 1 FROM private.pay_workbench_modal_ready_members_v2(s,'PAYE') WHERE NOT selected) THEN RAISE EXCEPTION 'GLOBAL_SELECT_OR_PROMOTION_INCOMPLETE'; END IF;
 IF NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_preview_rows WHERE session_id=s.id AND row_key='selection-recovery:1'
   AND selected AND row_json->>'selection_user_override'='SELECTED') THEN RAISE EXCEPTION 'GLOBAL_SELECT_DID_NOT_OVERRIDE_PROMOTED_RECOVERY'; END IF;
 IF other_partition IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r
   WHERE session_id=s.id AND row_json->>'pay_channel'='UMBRELLA') THEN RAISE EXCEPTION 'GLOBAL_SELECT_CHANGED_OTHER_CHANNEL'; END IF;
 IF s.selected_row_count<>320 OR jsonb_array_length(s.progress_json#>'{selection_intent_v1,canonical_preview_lines,selected_economic_identities}')<>320
   OR (SELECT count(*) FROM public.audit_events WHERE object_id_text=s.id::text)<>audit_count+2 THEN RAISE EXCEPTION 'GLOBAL_SELECT_FINAL_CONTRACT'; END IF;

 -- The second affected candidate fails after the first has been revalidated.
 -- The entire top-level SQL call must roll back all earlier work and its audit.
 INSERT INTO public.banking_pay_workbench_preview_rows(id,session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
 SELECT '50000000-0000-4000-8000-000000000001',s.id,'20000000-0000-4000-8000-000000000001','blocked_for_pay',
  'global-broken-recovery',9999,jsonb_set(row_json,'{case_components}','[]'::jsonb),'SOURCE_REF','global-broken-recovery',false,'NOT_SELECTABLE','READY',1
 FROM public.banking_pay_workbench_preview_rows WHERE session_id=s.id AND row_key='selection-recovery:1';
 SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO rows_before FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 session_before:=to_jsonb(s);
 BEGIN
  PERFORM public.pay_workbench_session_set_selected_rows(s.id,
   jsonb_set(jsonb_set(jsonb_set(intent,'{modal_global_intent_v2,request_id}','"40000000-0000-4000-8000-000000000004"'),
    '{modal_global_intent_v2,options,expected_progress_counter_version}','6'),'{modal_global_intent_v2,action}','"CLEAR_ALL_READY"'),s.actor_user_id);
  RAISE EXCEPTION 'GLOBAL_BROKEN_RECOVERY_ACCEPTED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS error_text=MESSAGE_TEXT;
  IF error_text<>'PAY_WORKBENCH_RECOVERY_COMPONENT_AUTHORITY_MISSING' THEN RAISE; END IF;
 END;
 IF rows_before IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id)
   OR session_before IS DISTINCT FROM (SELECT to_jsonb(x) FROM public.banking_pay_workbench_sessions x WHERE id=s.id)
   OR (SELECT count(*) FROM public.audit_events WHERE object_id_text=s.id::text)<>audit_count+2 THEN RAISE EXCEPTION 'GLOBAL_FAILURE_LEFT_PARTIAL_WRITES'; END IF;
END $global_selection$;
SELECT 'GLOBAL_FILTERED_SELECTION_PASS';
ROLLBACK;
