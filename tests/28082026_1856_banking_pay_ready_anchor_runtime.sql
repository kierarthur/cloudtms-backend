-- Appended only to the synthetic local Ready fixture in an open transaction.
-- Actual selection owner is exercised; every fixture change rolls back.
SET LOCAL statement_timeout='30s';
DO $local_guard$ BEGIN
  IF current_database()<>'banking_modal_v2_test' THEN RAISE EXCEPTION 'LOCAL_FIXTURE_ONLY'; END IF;
END $local_guard$;
UPDATE public.banking_pay_workbench_session_scope SET certified_preview_publication_attestation_json=
 '{"attestation_version":"CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3","contract_version":"3","semantic_contract_version":"READY_TO_PAY_SEMANTIC_V2"}'::jsonb
WHERE session_id='00000000-0000-4000-8000-000000000005';
DO $anchor_proof$
DECLARE
 s public.banking_pay_workbench_sessions%ROWTYPE; opts jsonb; first_page jsonb; second_page jsonb; third_page jsonb;
 back_page jsonb; renewed jsonb; change_result jsonb; anchor_data jsonb; bad jsonb; error_code text;
 cid uuid:='00000000-0000-4000-8000-000000000002'; saved_rows jsonb; before_rows text; after_rows text;
BEGIN
 SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='00000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'));
 first_page:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,cid,opts,s.actor_user_id,NULL,40);
 IF first_page->>'page_number' IS DISTINCT FROM '1' OR first_page->>'has_previous' IS DISTINCT FROM 'false'
   OR first_page->>'previous_cursor' IS NOT NULL OR COALESCE(first_page->>'page_anchor','')='' THEN
   RAISE EXCEPTION 'READY_POSITION_METADATA_MISSING';
 END IF;
 second_page:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,cid,opts,s.actor_user_id,first_page->>'next_cursor',40);
 third_page:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,cid,opts,s.actor_user_id,second_page->>'next_cursor',40);
 IF second_page->>'page_number'<>'2' OR second_page->>'has_previous'<>'true' OR second_page->>'previous_cursor' IS NOT NULL
   OR third_page->>'page_number'<>'3' OR jsonb_array_length(third_page->'rows')<>25 THEN
   RAISE EXCEPTION 'READY_CONTINUATION_METADATA_WRONG';
 END IF;
 back_page:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,cid,opts,s.actor_user_id,third_page->>'previous_cursor',40);
 IF back_page->'rows' IS DISTINCT FROM second_page->'rows' THEN RAISE EXCEPTION 'READY_PREVIOUS_ROUNDTRIP_LOST_ROWS'; END IF;
 saved_rows:=(SELECT jsonb_agg(r->'identity' ORDER BY n) FROM jsonb_array_elements(second_page->'rows') WITH ORDINALITY x(r,n));
 change_result:=public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('modal_candidate_intent_v2',
   jsonb_build_object('candidate_id',cid,'request_id','00000000-0000-4000-8000-000000009997',
   'action','SELECT_ALL_READY','options',opts)),s.actor_user_id);
 opts:=jsonb_set(opts,'{expected_progress_counter_version}',change_result->'progress_counter_version');
 BEGIN
   PERFORM public.pay_workbench_session_get_candidate_ready_page_v1(s.id,cid,opts,s.actor_user_id,first_page->>'next_cursor',40);
   RAISE EXCEPTION 'OLD_ORDINARY_READY_CURSOR_ACCEPTED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF; END;
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO before_rows
   FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=s.id;
 renewed:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,cid,opts,s.actor_user_id,second_page->>'page_anchor',40);
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO after_rows
   FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=s.id;
 IF before_rows IS DISTINCT FROM after_rows OR renewed->>'page_number'<>'2' OR renewed->>'total_count'<>'105'
   OR renewed#>>'{candidate,selected_ready_count}'<>'105' OR renewed#>>'{candidate,selected_display_amount}'<>'1050.00'
   OR EXISTS(SELECT 1 FROM jsonb_array_elements(renewed->'rows') r WHERE r->>'selected'<>'true')
   OR (SELECT jsonb_agg(r->'identity' ORDER BY n) FROM jsonb_array_elements(renewed->'rows') WITH ORDINALITY x(r,n)) IS DISTINCT FROM saved_rows THEN
   RAISE EXCEPTION 'READY_ANCHOR_DID_NOT_READ_CURRENT_COMPLETE_PAGE';
 END IF;
 anchor_data:=private.pay_workbench_modal_cursor_decode_v2(second_page->>'page_anchor','{}'::jsonb);
 FOR bad IN SELECT value FROM jsonb_array_elements(jsonb_build_array(
   jsonb_build_object('session_id','00000000-0000-4000-8000-000000000099'),
   jsonb_build_object('candidate_id','00000000-0000-4000-8000-000000000003'),
   jsonb_build_object('session_version',2),jsonb_build_object('scope_hash',repeat('b',64)),
   jsonb_build_object('page_limit',100),jsonb_build_object('progress_counter_version',6),
   jsonb_build_object('progress_counter_version',-1),jsonb_build_object('progress_counter_version',NULL),
   jsonb_build_object('contract','OTHER'),jsonb_build_object('kind','READY'),
   jsonb_build_object('last_id','bad'),jsonb_build_object('last_ordinal',-1))) LOOP
   BEGIN
     PERFORM public.pay_workbench_session_get_candidate_ready_page_v1(s.id,cid,opts,s.actor_user_id,
       private.pay_workbench_modal_cursor_encode_v2(anchor_data||bad),40);
     RAISE EXCEPTION 'INVALID_READY_ANCHOR_ACCEPTED';
   EXCEPTION WHEN SQLSTATE 'P0001' OR invalid_parameter_value THEN
     GET STACKED DIAGNOSTICS error_code=MESSAGE_TEXT;
     IF error_code NOT IN ('BANKING_PAY_V2_STALE_CURSOR','BANKING_PAY_V2_INVALID_CURSOR') THEN RAISE; END IF;
   END;
 END LOOP;
 -- Isolated direct fixture changes exercise section movement only, not policy.
 DELETE FROM public.banking_pay_workbench_preview_rows WHERE id=(second_page#>>'{rows,0,identity}')::uuid;
 UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=6 WHERE id=s.id;
 opts:=jsonb_set(opts,'{expected_progress_counter_version}','6');
 renewed:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,cid,opts,s.actor_user_id,second_page->>'page_anchor',40);
 IF renewed->>'page_number'<>'2' OR renewed#>>'{rows,0,identity}' IS DISTINCT FROM second_page#>>'{rows,1,identity}' THEN
   RAISE EXCEPTION 'REMOVED_READY_ANCHOR_DID_NOT_BACKFILL';
 END IF;
 DELETE FROM public.banking_pay_workbench_preview_rows WHERE session_id=s.id AND candidate_id=cid AND row_ordinal>=83;
 UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=7 WHERE id=s.id;
 opts:=jsonb_set(opts,'{expected_progress_counter_version}','7');
 renewed:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,cid,opts,s.actor_user_id,third_page->>'page_anchor',40);
 IF renewed->>'page_number'<>'2' OR renewed->>'total_count'<>'79' OR jsonb_array_length(renewed->'rows')<>39
   OR renewed->>'has_more'<>'false' THEN RAISE EXCEPTION 'DEPARTED_LAST_READY_PAGE_NOT_BACKFILLED'; END IF;
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||'{"presentation_section":"BLOCKED_FOR_PAY"}'::jsonb
   WHERE session_id=s.id AND candidate_id=cid AND section='canonical_preview_lines';
 UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=8 WHERE id=s.id;
 opts:=jsonb_set(opts,'{expected_progress_counter_version}','8');
 renewed:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,cid,opts,s.actor_user_id,third_page->>'page_anchor',40);
 IF renewed->'candidate' IS DISTINCT FROM 'null'::jsonb OR renewed->'rows'<>'[]'::jsonb OR renewed->>'total_count'<>'0'
   OR renewed->>'page_number'<>'0' OR renewed->>'page_anchor' IS NOT NULL OR renewed->>'has_previous'<>'false' THEN
   RAISE EXCEPTION 'DEPARTED_CANDIDATE_NOT_TRUTHFUL_EMPTY';
 END IF;
 RAISE NOTICE 'PASS: Ready anchor renewal after real selection;12 bindings; removed boundary; departed last page; no read writes';
END $anchor_proof$;
ROLLBACK;
