\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
DO $negative_selection$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_options jsonb; v_input jsonb; v_bad jsonb; v_expected text; v_error text;
  v_rows jsonb; v_session_before jsonb; v_audit_count bigint; v_tests integer:=0; v_result jsonb;
BEGIN
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  v_options:=jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',4,
    'pay_channel_scope','PAYE','scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'PAYE'));
  v_input:=jsonb_build_object('modal_candidate_intent_v2',jsonb_build_object('candidate_id','10000000-0000-4000-8000-000000000002',
    'request_id','10000000-0000-4000-8000-000000000011','action','CLEAR_ALL_READY','options',v_options));
  SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO v_rows FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id;
  v_session_before:=to_jsonb(v_session);
  SELECT count(*) INTO v_audit_count FROM public.audit_events WHERE object_id_text=v_session.id::text;
  FOR v_bad,v_expected IN SELECT input,expected FROM (VALUES
    (v_input || '{"select_preview_row_ids":[]}'::jsonb,'BANKING_PAY_V2_INVALID_INPUT'),
    (jsonb_set(v_input,'{modal_candidate_intent_v2}','[]'::jsonb),'BANKING_PAY_V2_INVALID_INPUT'),
    (jsonb_set(v_input,'{modal_candidate_intent_v2,action}','"TOGGLE"'),'BANKING_PAY_V2_INVALID_INPUT'),
    (jsonb_set(v_input,'{modal_candidate_intent_v2,request_id}','"bad"'),'BANKING_PAY_V2_INVALID_INPUT'),
    (jsonb_set(v_input,'{modal_candidate_intent_v2,candidate_id}','"bad"'),'BANKING_PAY_V2_INVALID_INPUT'),
    (jsonb_set(v_input,'{modal_candidate_intent_v2,options,expected_session_version}','0'),'BANKING_PAY_V2_STALE_REVISION'),
    (jsonb_set(v_input,'{modal_candidate_intent_v2,options,expected_progress_counter_version}','3'),'BANKING_PAY_V2_STALE_REVISION'),
    (jsonb_set(v_input,'{modal_candidate_intent_v2,options,scope_hash}',to_jsonb(repeat('0',64))),'BANKING_PAY_V2_SCOPE_MISMATCH'),
    (jsonb_set(v_input,'{modal_candidate_intent_v2,options,pay_channel_scope}','"ALL"'),'BANKING_PAY_V2_SCOPE_MISMATCH'),
    (jsonb_set(v_input,'{modal_candidate_intent_v2,options,search}','"candidate"'),'BANKING_PAY_V2_INVALID_INPUT'),
    (jsonb_set(v_input,'{modal_candidate_intent_v2,candidate_id}','"10000000-0000-4000-8000-000000000003"'),'BANKING_PAY_V2_CANDIDATE_NOT_CURRENT')
  ) AS fixtures(input,expected) LOOP
    BEGIN
      PERFORM public.pay_workbench_session_set_selected_rows(v_session.id,v_bad,v_session.actor_user_id);
      RAISE EXCEPTION 'NEGATIVE_INTENT_ACCEPTED';
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
      IF v_error<>v_expected THEN RAISE EXCEPTION 'WRONG_TYPED_REJECTION: expected %, got %',v_expected,v_error; END IF;
    END;
    v_tests:=v_tests+1;
  END LOOP;
  IF v_tests<>11 OR v_session_before IS DISTINCT FROM (SELECT to_jsonb(s) FROM public.banking_pay_workbench_sessions s WHERE id=v_session.id)
    OR v_rows IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id)
    OR (SELECT count(*) FROM public.audit_events WHERE object_id_text=v_session.id::text)<>v_audit_count THEN RAISE EXCEPTION 'REJECTED_INTENT_CHANGED_STATE'; END IF;
  -- No actor ID from a browser/inactive user can select candidate payments.
  UPDATE public.tms_users SET is_active=false WHERE id=v_session.actor_user_id;
  BEGIN
    PERFORM public.pay_workbench_session_set_selected_rows(v_session.id,v_input,v_session.actor_user_id);
    RAISE EXCEPTION 'INACTIVE_ACTOR_ACCEPTED';
  EXCEPTION WHEN SQLSTATE '42501' THEN GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_UNAUTHORISED' THEN RAISE; END IF;
  END;
  UPDATE public.tms_users SET is_active=true WHERE id=v_session.actor_user_id;
  -- Excluded/hidden rows are not candidate-checkbox targets.
  -- A current hidden payment is not simultaneously selected/Draftable Ready.
  -- Retain that canonical source contract; do not fabricate selected hidden pay
  -- that would independently fund the unchanged recovery headroom algorithm.
  UPDATE public.banking_pay_workbench_preview_rows SET section='blocked_for_pay',selected=false,selection_state='NOT_SELECTABLE',
    row_json=row_json || '{"hidden_indefinite_snooze":true,"snooze_state":{"state":"SNOOZED"},"draftable":false,"is_ready_for_draft":false,"selection_allowed":false,"presentation_section":"BLOCKED_FOR_PAY"}'::jsonb
    WHERE id='10000000-0000-4000-8000-000000001001';
  SELECT to_jsonb(r) INTO v_rows FROM public.banking_pay_workbench_preview_rows r WHERE id='10000000-0000-4000-8000-000000001001';
  v_result:=public.pay_workbench_session_set_selected_rows(v_session.id,v_input,v_session.actor_user_id);
  IF v_rows IS DISTINCT FROM (SELECT to_jsonb(r) FROM public.banking_pay_workbench_preview_rows r WHERE id='10000000-0000-4000-8000-000000001001') THEN
    RAISE EXCEPTION 'CANDIDATE_INTENT_TOUCHED_HIDDEN_PAYMENT';
  END IF;
  IF v_result->>'final_ready_count'<>'104' THEN RAISE EXCEPTION 'HIDDEN_PAYMENT_WAS_COUNTED'; END IF;
  -- The original global request is still its old owner (not candidate fan-out).
  v_result:=public.pay_workbench_session_set_selected_rows(v_session.id,
    jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',5,'selection_action','CLEAR_SECTION','section','canonical_preview_lines'),v_session.actor_user_id);
  IF v_result->>'ok'<>'true' OR v_result->>'selection_mode'<>'SECTION_CLEAR_CONTRACT_GUARDED'
    OR v_result->>'progress_counter_version'<>'6' THEN RAISE EXCEPTION 'LEGACY_GLOBAL_BRANCH_CHANGED'; END IF;
END $negative_selection$;
ROLLBACK;
