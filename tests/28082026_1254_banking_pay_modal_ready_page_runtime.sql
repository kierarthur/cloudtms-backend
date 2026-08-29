-- Synthetic local fixtures only; no hosted or copied business data. Rollback
-- restores every inserted identity. This proves read contracts, not economics.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout = '15s';
DO $local_guard$
BEGIN
  IF current_database() <> 'banking_modal_v2_test' THEN RAISE EXCEPTION 'LOCAL_FIXTURE_ONLY'; END IF;
END
$local_guard$;
INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
VALUES ('00000000-0000-4000-8000-000000000001','banking-modal-fixture@example.invalid','UNUSABLE_LOCAL_FIXTURE','admin',true);
INSERT INTO public.candidates(id,display_name,tms_ref)
VALUES ('00000000-0000-4000-8000-000000000002','Fixture candidate A','FIXTURE-A'),
       ('00000000-0000-4000-8000-000000000003','Fixture candidate B','FIXTURE-B');
INSERT INTO public.banking_pay_snapshot_runs(id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,eligibility_to_date)
VALUES ('00000000-0000-4000-8000-000000000004','2026-08-28','2026-08-30','2026-08-24','2026-08-01','2026-08-31');
INSERT INTO public.banking_pay_workbench_sessions(id,actor_user_id,pay_date,week_ending_cutoff,session_signature,source_snapshot_run_id,version,progress_counter_version,progress_json)
VALUES ('00000000-0000-4000-8000-000000000005','00000000-0000-4000-8000-000000000001','2026-08-28','2026-08-30',
 'local synthetic fixture only','00000000-0000-4000-8000-000000000004',1,4,'{"ready":true}');
INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty)
VALUES ('00000000-0000-4000-8000-000000000005','00000000-0000-4000-8000-000000000002',1,'READY',true,false),
       ('00000000-0000-4000-8000-000000000005','00000000-0000-4000-8000-000000000003',2,'READY',true,false);
INSERT INTO public.banking_pay_workbench_preview_rows(session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
SELECT '00000000-0000-4000-8000-000000000005'::uuid,
       CASE WHEN n <= 107 THEN '00000000-0000-4000-8000-000000000002'::uuid ELSE '00000000-0000-4000-8000-000000000003'::uuid END,
       CASE n WHEN 1 THEN 'cases_resolutions' WHEN 2 THEN 'blocked_for_pay' ELSE 'canonical_preview_lines' END,
       'local-fixture:' || n::text,n,
       jsonb_build_object('candidate_name','Fixture candidate','pay_channel',CASE WHEN n % 2 = 0 THEN 'PAYE' ELSE 'UMBRELLA' END,
          'amount_display','10.00','section_amount_display','10.00','amount_ex_vat','10.00',
          'presentation_section',CASE n WHEN 1 THEN 'CASES_RESOLUTIONS' WHEN 2 THEN 'BLOCKED_FOR_PAY' ELSE 'READY_TO_PAY' END,
          'line_type','TIMESHEET_PAYMENT','selection_allowed',true,'draftable',true,'is_ready_for_draft',true,
          'linked_timesheet_id','00000000-0000-4000-8000-' || lpad((1000+n)::text,12,'0'),
          'existing_action_metadata',jsonb_build_object('fixture',n)),
       'SOURCE_REF','fixture-' || n::text,n % 3 = 0,CASE WHEN n % 3 = 0 THEN 'SELECTED' ELSE 'UNSELECTED' END,'READY',1
FROM generate_series(1,110) AS n;
DO $ready_read_proof$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_options jsonb; v_first jsonb; v_second jsonb; v_filtered jsonb; v_legacy jsonb; v_error text;
  v_facts record; v_candidate_count bigint; v_total numeric;
  v_candidate_row jsonb; v_binding jsonb; v_timesheets jsonb; v_timesheet_token text;
BEGIN
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id='00000000-0000-4000-8000-000000000005';
  v_options := jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',4,'pay_channel_scope','ALL',
    'scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'ALL'));
  IF private.pay_workbench_modal_summary_context_v2(v_session.id,v_options-'scope_hash',v_session.actor_user_id,NULL)
       IS DISTINCT FROM private.pay_workbench_modal_context_v2(v_session.id,v_options,v_session.actor_user_id) THEN
    RAISE EXCEPTION 'FIRST_SUMMARY_CONTEXT_CHANGED_EXISTING_AUTHORITY';
  END IF;
  BEGIN
    PERFORM private.pay_workbench_modal_summary_context_v2(v_session.id,v_options-'scope_hash',v_session.actor_user_id,'existing_cursor');
    RAISE EXCEPTION 'UNBOUND_CONTINUATION_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_SCOPE_MISMATCH' THEN RAISE; END IF; END;
  BEGIN
    PERFORM private.pay_workbench_modal_summary_context_v2(v_session.id,(v_options-'scope_hash')||'{"expected_progress_counter_version":3}',v_session.actor_user_id,NULL);
    RAISE EXCEPTION 'STALE_FIRST_SUMMARY_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_REVISION' THEN RAISE; END IF; END;
  BEGIN
    PERFORM private.pay_workbench_modal_summary_context_v2(v_session.id,v_options-'scope_hash','00000000-0000-4000-8000-000000099999',NULL);
    RAISE EXCEPTION 'UNAUTHORISED_FIRST_SUMMARY_ACCEPTED';
  EXCEPTION WHEN insufficient_privilege THEN IF SQLERRM<>'BANKING_PAY_V2_UNAUTHORISED' THEN RAISE; END IF; END;
  BEGIN
    PERFORM private.pay_workbench_modal_summary_context_v2(v_session.id,v_options||'{"scope_hash":"0000000000000000000000000000000000000000000000000000000000000000"}',v_session.actor_user_id,NULL);
    RAISE EXCEPTION 'WRONG_FIRST_SUMMARY_SCOPE_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_SCOPE_MISMATCH' THEN RAISE; END IF; END;
  v_first := public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,'00000000-0000-4000-8000-000000000002',v_options,v_session.actor_user_id,NULL,100);
  IF (v_first->>'total_count')::integer <> 105 OR jsonb_array_length(v_first->'rows') <> 100 OR v_first->>'has_more' <> 'true' THEN
    RAISE EXCEPTION 'READY_FILTER_MUST_PRECEDE_PAGINATION';
  END IF;
  IF v_first#>>'{candidate,candidate_id}' IS DISTINCT FROM '00000000-0000-4000-8000-000000000002'
    OR v_first#>>'{candidate,selected_display_amount}' IS DISTINCT FROM '350.00'
    OR v_first#>>'{candidate,selectable_ready_count}' IS DISTINCT FROM '105'
    OR v_first#>>'{candidate,selected_ready_count}' IS DISTINCT FROM '35'
    OR v_first#>>'{candidate,selection_state}' IS DISTINCT FROM 'SOME' THEN
    RAISE EXCEPTION 'READY_CHILD_HEADER_MUST_COVER_UNLOADED_PAYMENTS';
  END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_first->'rows') AS r WHERE r->>'candidate_id' <> '00000000-0000-4000-8000-000000000002'
    OR r->>'effective_section' <> 'canonical_preview_lines' OR r#>>'{existing_action_metadata,fixture}' IS NULL) THEN
    RAISE EXCEPTION 'READY_ROW_SCOPE_OR_METADATA_LOST';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_first->'rows') r WHERE r->>'selected'='false')
     OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(v_first->'rows') r WHERE r->>'selected'='true') THEN
    RAISE EXCEPTION 'SELECTED_AND_UNSELECTED_ROWS_REQUIRED';
  END IF;
  v_legacy := public.pay_workbench_session_get_preview_page(v_session.id,'canonical_preview_lines','{}'::jsonb,100);
  IF (SELECT jsonb_agg(r - ARRAY[
       'identity',
       'selection_group_kind',
       'selection_group_key',
       'selection_group_member_count',
       'selection_group_selected_count',
       'selection_group_state',
       'selection_group_display_amount',
       'selection_group_selected_display_amount'
     ]::text[] ORDER BY ord)
     FROM jsonb_array_elements(v_first->'rows') WITH ORDINALITY AS row_data(r,ord))
     IS DISTINCT FROM v_legacy->'rows' THEN
    RAISE EXCEPTION 'LEGACY_READY_PAYLOAD_PARITY_FAILED';
  END IF;
  v_second := public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,'00000000-0000-4000-8000-000000000002',v_options,v_session.actor_user_id,v_first->>'next_cursor',100);
  IF jsonb_array_length(v_second->'rows') <> 5 OR v_second->>'has_more' <> 'false' THEN RAISE EXCEPTION 'READY_SECOND_PAGE_INVALID'; END IF;
  IF v_second->'candidate' IS DISTINCT FROM v_first->'candidate' THEN
    RAISE EXCEPTION 'READY_HEADER_MUST_NOT_CHANGE_WITH_CHILD_PAGE';
  END IF;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_first->'rows') a JOIN jsonb_array_elements(v_second->'rows') b ON a->>'identity'=b->>'identity') THEN
    RAISE EXCEPTION 'READY_CURSOR_DUPLICATE';
  END IF;
  v_filtered := public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,'00000000-0000-4000-8000-000000000002',
    v_options || jsonb_build_object('pay_channel_scope','PAYE','scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'PAYE')),v_session.actor_user_id,NULL,100);
  IF (v_filtered->>'total_count')::integer <> 52 THEN RAISE EXCEPTION 'PAYE_SCOPE_NOT_APPLIED_BEFORE_PAGING'; END IF;
  -- Identity may be stored on the certified preview row rather than repeated
  -- inside its JSON. Both the main list and child must filter its same payload.
  UPDATE public.banking_pay_workbench_sessions
    SET filters_json='{"candidate_id":"00000000-0000-4000-8000-000000000002"}'::jsonb
    WHERE id=v_session.id RETURNING * INTO v_session;
  v_filtered := public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,'00000000-0000-4000-8000-000000000002',
    v_options || jsonb_build_object('scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'ALL')),v_session.actor_user_id,NULL,100);
  IF (v_filtered->>'total_count')::integer <> 105 THEN RAISE EXCEPTION 'CHILD_FILTER_MUST_USE_CERTIFIED_IDENTITY'; END IF;
  v_filtered := public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,'00000000-0000-4000-8000-000000000003',
    v_options || jsonb_build_object('scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'ALL')),v_session.actor_user_id,NULL,100);
  IF (v_filtered->>'total_count')::integer <> 0 THEN RAISE EXCEPTION 'CHILD_MUST_NOT_WIDEN_CANDIDATE_FILTER'; END IF;
  IF v_filtered->'candidate' IS DISTINCT FROM 'null'::jsonb THEN RAISE EXCEPTION 'FILTERED_CANDIDATE_HEADER_LEAKED'; END IF;
  UPDATE public.banking_pay_workbench_sessions SET filters_json='{}'::jsonb
    WHERE id=v_session.id RETURNING * INTO v_session;
  BEGIN
    PERFORM public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,'00000000-0000-4000-8000-000000000002',
      v_options || '{"expected_progress_counter_version":3}'::jsonb,v_session.actor_user_id,NULL,100);
    RAISE EXCEPTION 'STALE_REVISION_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error <> 'BANKING_PAY_V2_STALE_REVISION' THEN RAISE; END IF;
  END;
  BEGIN
    PERFORM public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,'00000000-0000-4000-8000-000000000003',v_options,v_session.actor_user_id,v_first->>'next_cursor',100);
    RAISE EXCEPTION 'CROSS_CANDIDATE_CURSOR_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error <> 'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF;
  END;
  SELECT count(*),sum(selected_display_amount) INTO v_candidate_count,v_total
  FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'ALL');
  IF v_candidate_count<>2 OR v_total<>360.00 THEN RAISE EXCEPTION 'COMPLETE_CANDIDATE_TOTAL_INVALID'; END IF;
  SELECT * INTO v_facts FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'ALL')
    WHERE candidate_id='00000000-0000-4000-8000-000000000002';
  IF v_facts.selectable_ready_count<>105 OR v_facts.selected_ready_count<>35 OR v_facts.selection_state<>'SOME'
    OR v_facts.selected_display_amount<>350.00 OR v_facts.selected_deduction_exists THEN
    RAISE EXCEPTION 'CANDIDATE_FACTS_MUST_COVER_UNLOADED_READY_PAGE';
  END IF;
  v_binding := jsonb_build_object('contract','BANKING_PAY_MODAL_STRUCTURE_V2','session_id',v_session.id,
    'session_version',v_session.version,'progress_counter_version',v_session.progress_counter_version,'scope_hash',v_options->>'scope_hash');
  v_candidate_row := private.pay_workbench_modal_candidate_row_v2(to_jsonb(v_facts),v_binding);
  v_timesheet_token := v_candidate_row->>'selected_timesheet_scope_token';
  IF (v_candidate_row->>'selected_timesheet_count')::integer<>35
    OR jsonb_array_length(v_candidate_row->'selected_timesheet_ids')<>0 OR v_timesheet_token IS NULL THEN
    RAISE EXCEPTION 'LARGE_SELECTED_TIMESHEET_SCOPE_MUST_USE_EXACT_TOKEN';
  END IF;
  v_timesheets := public.pay_workbench_session_get_selected_ready_timesheets_v1(v_session.id,
    '00000000-0000-4000-8000-000000000002',v_options,v_session.actor_user_id,v_timesheet_token);
  IF (v_timesheets->>'timesheet_count')::integer<>35 OR v_timesheets->'timesheet_ids' IS DISTINCT FROM to_jsonb(v_facts.selected_timesheet_ids) THEN
    RAISE EXCEPTION 'SELECTED_TIMESHEET_TOKEN_LOST_EXACT_SCOPE';
  END IF;
  -- A fixture-only direct change tests the reader, not the mutation owner.
  UPDATE public.banking_pay_workbench_preview_rows SET selected=false,selection_state='UNSELECTED'
    WHERE session_id=v_session.id AND candidate_id='00000000-0000-4000-8000-000000000002';
  SELECT * INTO v_facts FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'ALL')
    WHERE candidate_id='00000000-0000-4000-8000-000000000002';
  IF NOT FOUND OR v_facts.selectable_ready_count<>105 OR v_facts.selected_ready_count<>0 OR v_facts.selection_state<>'NONE'
    OR v_facts.selected_display_amount<>0 OR v_facts.selected_deduction_exists OR cardinality(v_facts.selected_timesheet_ids)<>0 THEN
    RAISE EXCEPTION 'UNTICKED_CANDIDATE_MUST_REMAIN_VISIBLE_WITH_ZERO';
  END IF;
  v_filtered:=public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,
    '00000000-0000-4000-8000-000000000002',v_options,v_session.actor_user_id,NULL,100);
  IF v_filtered#>>'{candidate,selected_display_amount}' IS DISTINCT FROM '0.00'
    OR v_filtered#>>'{candidate,selection_state}' IS DISTINCT FROM 'NONE'
    OR (v_filtered->>'total_count')::integer<>105 THEN RAISE EXCEPTION 'UNCHECKED_CHILD_MUST_REMAIN_INSPECTABLE'; END IF;
  BEGIN
    PERFORM public.pay_workbench_session_get_selected_ready_timesheets_v1(v_session.id,
      '00000000-0000-4000-8000-000000000002',v_options,v_session.actor_user_id,v_timesheet_token);
    RAISE EXCEPTION 'OLD_TIMESHEET_TOKEN_WAS_ACCEPTED_AFTER_SELECTION_CHANGE';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF;
  END;
  UPDATE public.banking_pay_workbench_session_scope SET status='LINE_WORK_PENDING'
    WHERE session_id=v_session.id AND candidate_id='00000000-0000-4000-8000-000000000002';
  BEGIN
    PERFORM public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,'00000000-0000-4000-8000-000000000002',v_options,v_session.actor_user_id,NULL,100);
    RAISE EXCEPTION 'UPDATING_CANDIDATE_WAS_EXPOSED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error <> 'BANKING_PAY_V2_NOT_READY' THEN RAISE; END IF;
  END;
  IF EXISTS (SELECT 1 FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'ALL')
    WHERE candidate_id='00000000-0000-4000-8000-000000000002') THEN
    RAISE EXCEPTION 'UPDATING_CANDIDATE_MUST_NOT_CONTRIBUTE_TO_SUMMARY';
  END IF;
END
$ready_read_proof$;
DO $hidden_scope_proof$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_options jsonb; v_first jsonb; v_second jsonb; v_facts record;
  v_count bigint; v_amount numeric; v_terminal_cursor text; v_error text;
BEGIN
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id='00000000-0000-4000-8000-000000000005';
  UPDATE public.banking_pay_workbench_session_scope SET status='READY',dirty=false WHERE session_id=v_session.id;
  UPDATE public.banking_pay_workbench_preview_rows SET selected=true,selection_state='SELECTED'
    WHERE session_id=v_session.id AND section='canonical_preview_lines';
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || CASE row_ordinal
      WHEN 3 THEN '{"snooze_state":"INDEFINITE"}'::jsonb
      WHEN 4 THEN '{"hidden_indefinite_snooze":true}'::jsonb
      WHEN 5 THEN '{"snooze_state":{"state":"SNOOZED"}}'::jsonb
      WHEN 6 THEN '{"rowJson":{"is_hidden":true}}'::jsonb
      ELSE '{"is_indefinitely_snoozed":true}'::jsonb END
    WHERE session_id=v_session.id AND (row_ordinal BETWEEN 3 AND 6 OR candidate_id='00000000-0000-4000-8000-000000000003');
  v_options:=jsonb_build_object('expected_session_version',v_session.version,'expected_progress_counter_version',v_session.progress_counter_version,
    'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'ALL'));
  v_first:=public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,'00000000-0000-4000-8000-000000000002',v_options,v_session.actor_user_id,NULL,100);
  IF (v_first->>'total_count')::integer<>101 OR jsonb_array_length(v_first->'rows')<>100 OR v_first->>'has_more'<>'true' THEN
    RAISE EXCEPTION 'HIDDEN_ROWS_MUST_BE_EXCLUDED_BEFORE_READY_PAGE';
  END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_first->'rows') r WHERE private.pay_workbench_modal_hidden_v2(r)) THEN
    RAISE EXCEPTION 'HIDDEN_ROW_LEAKED_IN_READY_CHILD';
  END IF;
  v_second:=public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,'00000000-0000-4000-8000-000000000002',v_options,v_session.actor_user_id,v_first->>'next_cursor',100);
  IF jsonb_array_length(v_second->'rows')<>1 THEN RAISE EXCEPTION 'VISIBLE_SECOND_PAGE_LOST'; END IF;
  -- A caller can present the final current row as a cursor, even though the
  -- last-page response never advertises a Next action. Reject that navigation;
  -- do not misrepresent 101 current payments as an empty candidate breakdown.
  SELECT private.pay_workbench_modal_cursor_encode_v2(
    private.pay_workbench_modal_cursor_decode_v2(v_first->>'next_cursor','{}'::jsonb)
      ||jsonb_build_object('last_id',r.id,'last_ordinal',r.row_ordinal)) INTO v_terminal_cursor
  FROM public.banking_pay_workbench_preview_rows r WHERE r.id=(v_second#>>'{rows,0,identity}')::uuid;
  BEGIN
    PERFORM public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,
      '00000000-0000-4000-8000-000000000002',v_options,v_session.actor_user_id,v_terminal_cursor,100);
    RAISE EXCEPTION 'TERMINAL_CURSOR_RETURNED_FALSE_EMPTY';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF;
  END;
  SELECT count(*),sum(selected_display_amount) INTO v_count,v_amount FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'ALL');
  IF v_count<>1 OR v_amount<>1010.00 THEN RAISE EXCEPTION 'HIDDEN_CANDIDATE_OR_AMOUNT_LEAKED'; END IF;
  SELECT * INTO v_facts FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'ALL');
  IF v_facts.selected_ready_count<>101 OR cardinality(v_facts.selected_timesheet_ids)<>101 THEN
    RAISE EXCEPTION 'HIDDEN_TIMESHEETS_OR_SELECTION_COUNT_LEAKED';
  END IF;
  v_second:=public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,
    '00000000-0000-4000-8000-000000000003',v_options,v_session.actor_user_id,NULL,100);
  IF v_second->'candidate' IS DISTINCT FROM 'null'::jsonb OR v_second->'rows'<>'[]'::jsonb
    OR v_second->>'total_count'<>'0' THEN RAISE EXCEPTION 'HIDDEN_ONLY_CANDIDATE_HEADER_LEAKED'; END IF;
  -- A candidate whose surviving Ready rows are presentation-only parents has
  -- no selectable payment. The main and child must both cease to represent it.
  UPDATE public.banking_pay_workbench_preview_rows
    SET row_json=row_json || '{"selection_allowed":false,"draftable":false,"is_ready_for_draft":false,"is_excluded_from_allocation":true,"presentation_role":"PARENT"}'::jsonb
    WHERE session_id=v_session.id AND candidate_id='00000000-0000-4000-8000-000000000002';
  v_second:=public.pay_workbench_session_get_candidate_ready_page_v1(v_session.id,
    '00000000-0000-4000-8000-000000000002',v_options,v_session.actor_user_id,NULL,100);
  IF v_second->'candidate' IS DISTINCT FROM 'null'::jsonb OR v_second->'rows'<>'[]'::jsonb
    OR v_second->>'total_count'<>'0' THEN RAISE EXCEPTION 'CONTEXT_ONLY_CANDIDATE_MUST_BECOME_EMPTY'; END IF;
END;
$hidden_scope_proof$;
ROLLBACK;
