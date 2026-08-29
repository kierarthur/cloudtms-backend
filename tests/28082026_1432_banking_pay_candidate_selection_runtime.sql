\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
UPDATE public.banking_pay_workbench_preview_rows SET selected=false,selection_state='UNSELECTED'
  WHERE session_id='10000000-0000-4000-8000-000000000005' AND row_ordinal<=105;
DO $candidate_selection$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_candidate uuid:='10000000-0000-4000-8000-000000000002';
  v_options jsonb; v_input jsonb; v_result jsonb; v_duplicate jsonb; v_noop jsonb;
  v_other jsonb; v_snapshot jsonb; v_before_session jsonb; v_error text; v_count bigint; v_facts record;
  v_audits bigint; v_input_clear jsonb;
BEGIN
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  SELECT count(*) INTO v_audits FROM public.audit_events WHERE object_id_text=v_session.id::text;
  SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO v_other FROM public.banking_pay_workbench_preview_rows r
    WHERE session_id=v_session.id AND (candidate_id<>v_candidate OR row_json->>'pay_channel'='UMBRELLA');
  v_options:=jsonb_build_object('expected_session_version',v_session.version,'expected_progress_counter_version',v_session.progress_counter_version,
    'pay_channel_scope','PAYE','scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'PAYE'));
  v_input:=jsonb_build_object('modal_candidate_intent_v2',jsonb_build_object('candidate_id',v_candidate,
    'request_id','10000000-0000-4000-8000-000000000011','action','SELECT_ALL_READY','options',v_options));
  v_result:=public.pay_workbench_session_set_selected_rows(v_session.id,v_input,v_session.actor_user_id);
  IF v_result->>'ok'<>'true' OR v_result->>'state_changed'<>'true' OR (v_result->>'progress_counter_version')::bigint<>5
     OR (v_result->>'recovery_revalidation_count')::integer<>1 OR (v_result->>'final_ready_count')::integer<>106 THEN
    RAISE EXCEPTION 'CANDIDATE_SELECT_MUST_SETTLE_ALL_PAGES_AND_PROMOTION_ONCE';
  END IF;
  IF v_other IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r
    WHERE session_id=v_session.id AND (candidate_id<>v_candidate OR row_json->>'pay_channel'='UMBRELLA')) THEN
    RAISE EXCEPTION 'CANDIDATE_SELECT_CHANGED_ANOTHER_SCOPE';
  END IF;
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id=v_session.id;
  SELECT * INTO v_facts FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'PAYE') WHERE candidate_id=v_candidate;
  IF v_facts.selection_state<>'ALL' OR v_facts.selected_ready_count<>106 OR v_facts.selected_display_amount<>1035.00
     OR v_facts.selected_deduction_exists IS NOT TRUE THEN RAISE EXCEPTION 'CANDIDATE_SELECT_FINAL_FACTS_WRONG'; END IF;
  IF NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_preview_rows WHERE row_key='selection-recovery:1'
    AND selected=true AND row_json->>'selection_user_override'='SELECTED') THEN RAISE EXCEPTION 'PROMOTED_OVERRIDE_NOT_REPLACED'; END IF;
  IF v_session.selected_row_count<>110 OR v_session.server_selected_preview_row_ids_provided IS NOT FALSE
    OR v_session.progress_json#>>'{selection_intent_v1,canonical_preview_lines,mode}'<>'IMPLICIT_ALL'
    OR jsonb_array_length(v_session.progress_json#>'{selection_intent_v1,canonical_preview_lines,selected_economic_identities}')<>110 THEN
    RAISE EXCEPTION 'FULL_SESSION_SELECTED_IDENTITY_AUTHORITY_LOST';
  END IF;
  IF (SELECT count(*) FROM public.audit_events WHERE object_id_text=v_session.id::text)<>v_audits+1 THEN RAISE EXCEPTION 'SELECT_MUST_HAVE_ONE_AUDIT'; END IF;
  SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO v_snapshot FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id;
  v_before_session:=to_jsonb(v_session);
  v_duplicate:=public.pay_workbench_session_set_selected_rows(v_session.id,v_input,v_session.actor_user_id);
  IF v_duplicate IS DISTINCT FROM v_result THEN RAISE EXCEPTION 'DUPLICATE_OUTCOME_CHANGED'; END IF;
  IF v_before_session IS DISTINCT FROM (SELECT to_jsonb(s) FROM public.banking_pay_workbench_sessions s WHERE id=v_session.id)
    OR v_snapshot IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id)
    OR (SELECT count(*) FROM public.audit_events WHERE object_id_text=v_session.id::text)<>v_audits+1 THEN RAISE EXCEPTION 'DUPLICATE_MUST_NOT_WRITE'; END IF;
  BEGIN
    PERFORM public.pay_workbench_session_set_selected_rows(v_session.id,jsonb_set(v_input,'{modal_candidate_intent_v2,action}','"CLEAR_ALL_READY"'),v_session.actor_user_id);
    RAISE EXCEPTION 'REUSED_ID_DIFFERENT_INTENT_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_REQUEST_CONFLICT' THEN RAISE; END IF;
  END;
  v_noop:=public.pay_workbench_session_set_selected_rows(v_session.id,
    jsonb_set(jsonb_set(v_input,'{modal_candidate_intent_v2,request_id}','"10000000-0000-4000-8000-000000000012"'),
      '{modal_candidate_intent_v2,options,expected_progress_counter_version}','5'),v_session.actor_user_id);
  IF v_noop->>'state_changed'<>'false' OR v_noop->>'progress_counter_version'<>'5'
    OR v_before_session IS DISTINCT FROM (SELECT to_jsonb(s) FROM public.banking_pay_workbench_sessions s WHERE id=v_session.id)
    OR v_snapshot IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id)
    OR (SELECT count(*) FROM public.audit_events WHERE object_id_text=v_session.id::text)<>v_audits+1 THEN RAISE EXCEPTION 'GENUINE_NOOP_WROTE_STATE'; END IF;
  v_input_clear:=jsonb_set(jsonb_set(jsonb_set(v_input,'{modal_candidate_intent_v2,request_id}','"10000000-0000-4000-8000-000000000013"'),
    '{modal_candidate_intent_v2,action}','"CLEAR_ALL_READY"'),'{modal_candidate_intent_v2,options,expected_progress_counter_version}','5');
  v_result:=public.pay_workbench_session_set_selected_rows(v_session.id,v_input_clear,v_session.actor_user_id);
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id=v_session.id;
  SELECT * INTO v_facts FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'PAYE') WHERE candidate_id=v_candidate;
  IF v_result->>'state_changed'<>'true' OR v_result->>'progress_counter_version'<>'6' OR v_facts.selection_state<>'NONE'
    OR v_facts.selected_ready_count<>0 OR v_facts.selectable_ready_count<>105 OR v_facts.selected_display_amount<>0
    OR v_facts.selected_deduction_exists IS NOT FALSE THEN RAISE EXCEPTION 'CLEAR_ALL_FINAL_AUTHORITY_WRONG'; END IF;
  IF NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_preview_rows WHERE row_key='selection-recovery:1'
    AND selected=false AND private.pay_workbench_preview_effective_section_v1(section,row_json)='blocked_for_pay'
    AND row_json->>'selection_user_override'='UNSELECTED') THEN RAISE EXCEPTION 'CLEAR_ALL_DID_NOT_DEMOTE_RECOVERY'; END IF;
  IF v_other IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r
    WHERE session_id=v_session.id AND (candidate_id<>v_candidate OR row_json->>'pay_channel'='UMBRELLA')) THEN RAISE EXCEPTION 'CLEAR_ALL_CHANGED_ANOTHER_SCOPE'; END IF;
  IF (SELECT count(*) FROM public.audit_events WHERE object_id_text=v_session.id::text)<>v_audits+2 THEN RAISE EXCEPTION 'CLEAR_MUST_HAVE_ONE_AUDIT'; END IF;
  -- An old accepted request cannot be replayed after the later clear intent.
  BEGIN
    PERFORM public.pay_workbench_session_set_selected_rows(v_session.id,v_input,v_session.actor_user_id);
    RAISE EXCEPTION 'OLD_REQUEST_REPLAYED_AFTER_CLEAR';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_STALE_REVISION' THEN RAISE; END IF;
  END;
  -- Select again, then use the original individual-selection request branch.
  v_input:=jsonb_set(jsonb_set(v_input,'{modal_candidate_intent_v2,request_id}','"10000000-0000-4000-8000-000000000014"'),
    '{modal_candidate_intent_v2,options,expected_progress_counter_version}','6');
  v_result:=public.pay_workbench_session_set_selected_rows(v_session.id,v_input,v_session.actor_user_id);
  v_result:=public.pay_workbench_session_set_selected_rows(v_session.id,jsonb_build_object('expected_session_version',1,
    'expected_progress_counter_version',7,'deselect_preview_row_ids',jsonb_build_array('10000000-0000-4000-8000-000000001001')),v_session.actor_user_id);
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id=v_session.id;
  SELECT * INTO v_facts FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'PAYE') WHERE candidate_id=v_candidate;
  IF v_session.progress_counter_version<>8 OR v_facts.selection_state<>'SOME' OR v_facts.selected_ready_count<>105
    OR v_facts.selected_display_amount<>1025.00 THEN RAISE EXCEPTION 'INDIVIDUAL_AFTER_CANDIDATE_ALL_LOST'; END IF;
  -- A real recovery failure must roll back the positive-selection writes too.
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=jsonb_set(row_json,'{case_components}','[]'::jsonb)
    WHERE session_id=v_session.id AND row_key='selection-recovery:1';
  SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO v_snapshot FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id;
  v_before_session:=to_jsonb(v_session);
  SELECT count(*) INTO v_audits FROM public.audit_events WHERE object_id_text=v_session.id::text;
  BEGIN
    PERFORM public.pay_workbench_session_set_selected_rows(v_session.id,
      jsonb_set(jsonb_set(v_input_clear,'{modal_candidate_intent_v2,request_id}','"10000000-0000-4000-8000-000000000015"'),
        '{modal_candidate_intent_v2,options,expected_progress_counter_version}','8'),v_session.actor_user_id);
    RAISE EXCEPTION 'BROKEN_RECOVERY_AUTHORITY_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'PAY_WORKBENCH_RECOVERY_COMPONENT_AUTHORITY_MISSING' THEN RAISE; END IF;
  END;
  IF v_before_session IS DISTINCT FROM (SELECT to_jsonb(s) FROM public.banking_pay_workbench_sessions s WHERE id=v_session.id)
    OR v_snapshot IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id)
    OR (SELECT count(*) FROM public.audit_events WHERE object_id_text=v_session.id::text)<>v_audits THEN RAISE EXCEPTION 'FAILED_CANDIDATE_MUTATION_LEFT_PARTIAL_WRITE'; END IF;
END $candidate_selection$;
ROLLBACK;
