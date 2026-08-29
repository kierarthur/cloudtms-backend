-- Local synthetic reader fixtures. No real Draft, provider, or payment action.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL client_min_messages='warning';
DO $guard$
BEGIN
  IF current_database()<>'banking_modal_v2_test' THEN RAISE EXCEPTION 'LOCAL_FIXTURE_ONLY'; END IF;
END
$guard$;
INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
VALUES ('00000000-0000-4000-8000-000000050001','banking-paging-fixture@example.invalid','UNUSABLE_LOCAL_FIXTURE','admin',true);
INSERT INTO public.candidates(id,display_name,tms_ref)
SELECT ('00000000-0000-4000-9000-' || lpad(n::text,12,'0'))::uuid,
  CASE WHEN n IN (3,4) THEN 'Duplicate fixture' ELSE 'Candidate ' || lpad(n::text,3,'0') END,
  CASE n WHEN 3 THEN 'REF-B' WHEN 4 THEN 'REF-A' ELSE 'FIXTURE-' || n::text END
FROM generate_series(1,105) AS n;
INSERT INTO public.banking_pay_snapshot_runs(id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,eligibility_to_date)
VALUES ('00000000-0000-4000-8000-000000050002','2026-08-28','2026-08-30','2026-08-24','2026-08-01','2026-08-31');
INSERT INTO public.banking_pay_workbench_sessions(id,actor_user_id,pay_date,week_ending_cutoff,session_signature,source_snapshot_run_id,version,progress_counter_version,progress_json)
VALUES ('00000000-0000-4000-8000-000000050003','00000000-0000-4000-8000-000000050001','2026-08-28','2026-08-30',
  'synthetic pagination fixture','00000000-0000-4000-8000-000000050002',1,4,'{"ready":true}');
INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty)
SELECT '00000000-0000-4000-8000-000000050003'::uuid,('00000000-0000-4000-9000-' || lpad(n::text,12,'0'))::uuid,n,'READY',true,false
FROM generate_series(1,105) AS n;
INSERT INTO public.banking_pay_workbench_preview_rows(session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
SELECT '00000000-0000-4000-8000-000000050003'::uuid,('00000000-0000-4000-9000-' || lpad(n::text,12,'0'))::uuid,
  'canonical_preview_lines','paging-fixture:' || n::text,n,
  jsonb_build_object('presentation_section','READY_TO_PAY','presentation_role','CHILD',
    'line_type',CASE WHEN n%10=0 THEN 'LOAN_REPAYMENT' ELSE 'TIMESHEET_PAYMENT' END,
    'finance_case_id',CASE WHEN n%10=0 THEN '00000000-0000-4000-8000-000000050004' ELSE NULL END,
    'amount_ex_vat',CASE WHEN n%10=0 THEN -n ELSE n END,'amount_display',to_char(CASE WHEN n%10=0 THEN -n ELSE n END,'FM999999.00'),
    'section_amount_display',to_char(CASE WHEN n%10=0 THEN -n ELSE n END,'FM999999.00'),
    'pay_channel',CASE WHEN n%2=0 THEN 'PAYE' ELSE 'UMBRELLA' END,
    'selection_allowed',true,'draftable',true,'is_ready_for_draft',true),
  'SOURCE_REF','paging-fixture-' || n::text,n%4<>0,CASE WHEN n%4<>0 THEN 'SELECTED' ELSE 'UNSELECTED' END,'READY',1
FROM generate_series(1,105) AS n;

DO $paging_proof$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_options jsonb; v_key text; v_direction text; v_first jsonb; v_second jsonb; v_back jsonb;
  v_expected jsonb; v_actual jsonb; v_total numeric; v_count integer; v_error text; v_cursor jsonb;
BEGIN
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id='00000000-0000-4000-8000-000000050003';
  v_options:=jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',4,'pay_channel_scope','ALL',
    'scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'ALL'));
  SELECT count(*),sum(selected_display_amount) INTO v_count,v_total FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'ALL');
  IF v_count<>105 THEN RAISE EXCEPTION 'ELIGIBLE_UNSELECTED_CANDIDATES_MUST_REMAIN_PRESENT'; END IF;
  FOREACH v_key IN ARRAY ARRAY['CANDIDATE','DEDUCTIONS','READY_TO_PAY'] LOOP
    FOREACH v_direction IN ARRAY ARRAY['ASC','DESC'] LOOP
      v_first:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,NULL,100);
      v_second:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_first->>'next_cursor',100);
      IF v_first->>'page_anchor' IS NULL OR v_second->>'page_anchor' IS NULL
         OR v_first->'has_previous' IS DISTINCT FROM 'false'::jsonb
         OR v_second->'has_previous' IS DISTINCT FROM 'true'::jsonb THEN
        RAISE EXCEPTION 'CANDIDATE_PAGE_ANCHOR_AND_PREVIOUS_AUTHORITY_REQUIRED';
      END IF;
      v_back:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_second->>'previous_cursor',100);
      IF v_back->'rows' IS DISTINCT FROM v_first->'rows' THEN RAISE EXCEPTION 'SERVER_PREVIOUS_PAGE_LOST_ROWS'; END IF;
      v_back:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_second->>'page_anchor',100);
      IF v_back->'rows' IS DISTINCT FROM v_second->'rows' THEN RAISE EXCEPTION 'SAME_REVISION_ANCHOR_LOST_POSITION'; END IF;
      IF jsonb_array_length(v_first->'rows')<>100 OR jsonb_array_length(v_second->'rows')<>5
         OR v_first->>'has_more'<>'true' OR v_second->>'has_more'<>'false' OR v_second->>'next_cursor' IS NOT NULL THEN
        RAISE EXCEPTION 'CANDIDATE_PAGING_COUNT_FAILED % %',v_key,v_direction;
      END IF;
      IF (v_first#>>'{ready_global,selected_ready_display_amount}')::numeric<>v_total
         OR v_first->'ready_global' IS DISTINCT FROM v_second->'ready_global'
         OR (v_first->>'total_count')::integer<>105 THEN
        RAISE EXCEPTION 'HEADLINE_MUST_USE_COMPLETE_RESULT_NOT_PAGE';
      END IF;
      SELECT jsonb_agg(to_jsonb(f.candidate_id) ORDER BY
        CASE WHEN v_key='DEDUCTIONS' AND v_direction='ASC' THEN f.selected_deduction_exists END ASC,
        CASE WHEN v_key='DEDUCTIONS' AND v_direction='DESC' THEN f.selected_deduction_exists END DESC,
        CASE WHEN v_key='READY_TO_PAY' AND v_direction='ASC' THEN f.selected_display_amount END ASC,
        CASE WHEN v_key='READY_TO_PAY' AND v_direction='DESC' THEN f.selected_display_amount END DESC,
        CASE WHEN v_key='CANDIDATE' AND v_direction='DESC' THEN f.candidate_sort_name END COLLATE "C" DESC,
        CASE WHEN NOT(v_key='CANDIDATE' AND v_direction='DESC') THEN f.candidate_sort_name END COLLATE "C" ASC,
        f.candidate_sort_reference COLLATE "C",f.candidate_id)
      INTO v_expected FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'ALL') AS f;
      SELECT jsonb_agg(row->'candidate_id' ORDER BY ord) INTO v_actual
      FROM jsonb_array_elements((v_first->'rows') || (v_second->'rows')) WITH ORDINALITY AS data(row,ord);
      IF v_actual IS DISTINCT FROM v_expected THEN RAISE EXCEPTION 'GLOBAL_SORT_OR_CURSOR_LOST_ROWS % %',v_key,v_direction; END IF;
      IF (SELECT count(DISTINCT value) FROM jsonb_array_elements_text(v_actual))<>105 THEN RAISE EXCEPTION 'CANDIDATE_DUPLICATED_ACROSS_PAGES'; END IF;
    END LOOP;
  END LOOP;
  v_first:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,'CANDIDATE','ASC',NULL,100);
  FOREACH v_error IN ARRAY ARRAY['next_cursor','page_anchor'] LOOP
    BEGIN
      PERFORM private.pay_workbench_modal_candidate_page_v2(v_session,v_options,'CANDIDATE','ASC',v_first->>v_error,10);
      RAISE EXCEPTION 'CROSS_PAGE_SIZE_CURSOR_ACCEPTED';
    EXCEPTION WHEN SQLSTATE 'P0001' THEN
      IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF;
    END;
  END LOOP;
  BEGIN
    PERFORM private.pay_workbench_modal_candidate_page_v2(v_session,v_options,'READY_TO_PAY','ASC',v_first->>'next_cursor',100);
    RAISE EXCEPTION 'CROSS_SORT_CURSOR_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF;
  END;
  BEGIN
    PERFORM private.pay_workbench_modal_candidate_page_v2(v_session,v_options || jsonb_build_object('scope_hash',repeat('0',64)),
      'CANDIDATE','ASC',v_first->>'next_cursor',100);
    RAISE EXCEPTION 'CROSS_SCOPE_CURSOR_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF;
  END;
  v_cursor:=private.pay_workbench_modal_cursor_decode_v2(v_first->>'next_cursor','{}'::jsonb);
  v_cursor:=v_cursor || jsonb_build_object('last_id','00000000-0000-4000-8000-000000099999');
  BEGIN
    PERFORM private.pay_workbench_modal_candidate_page_v2(v_session,v_options,'CANDIDATE','ASC',private.pay_workbench_modal_cursor_encode_v2(v_cursor),100);
    RAISE EXCEPTION 'NONEXISTENT_CURSOR_ANCHOR_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF;
  END;
END
$paging_proof$;
DO $anchor_proof$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_options jsonb; v_first jsonb; v_second jsonb; v_current jsonb; v_back jsonb; v_next jsonb; v_token jsonb;
  v_anchor text; v_old_cursor text; v_error text; v_key text; v_direction text;
  v_id uuid; v_row_json jsonb; v_before bigint; v_rows jsonb;
BEGIN
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id='00000000-0000-4000-8000-000000050003';
  FOREACH v_key IN ARRAY ARRAY['CANDIDATE','DEDUCTIONS','READY_TO_PAY'] LOOP
    FOREACH v_direction IN ARRAY ARRAY['ASC','DESC'] LOOP
      v_options:=jsonb_build_object('expected_session_version',v_session.version,'expected_progress_counter_version',v_session.progress_counter_version,
        'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'ALL'));
      v_first:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,NULL,10);
      v_second:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_first->>'next_cursor',10);
      v_anchor:=v_second->>'page_anchor';v_old_cursor:=v_first->>'next_cursor';v_before:=v_session.progress_counter_version;
      UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=progress_counter_version+1
        WHERE id=v_session.id RETURNING * INTO v_session;
      v_options:=v_options || jsonb_build_object('expected_progress_counter_version',v_session.progress_counter_version);
      BEGIN
        PERFORM private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_old_cursor,10);
        RAISE EXCEPTION 'ORDINARY_OLD_REVISION_CURSOR_ACCEPTED';
      EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
        IF v_error<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF;
      END;
      v_current:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_anchor,10);
      SELECT jsonb_agg(r-'child_revision' ORDER BY ord) INTO v_rows
        FROM jsonb_array_elements(v_current->'rows') WITH ORDINALITY a(r,ord);
      IF v_rows IS DISTINCT FROM (SELECT jsonb_agg(r-'child_revision' ORDER BY ord)
        FROM jsonb_array_elements(v_second->'rows') WITH ORDINALITY a(r,ord)) THEN
        RAISE EXCEPTION 'ANCHOR_DID_NOT_RENEW_THE_CURRENT_PAGE % %',v_key,v_direction;
      END IF;
      v_back:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_current->>'previous_cursor',10);
      v_next:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_back->>'next_cursor',10);
      IF v_next->'rows' IS DISTINCT FROM v_current->'rows' OR v_current->>'page_number'<>'2' THEN
        RAISE EXCEPTION 'ANCHORED_PREVIOUS_NEXT_ROUND_TRIP_FAILED';
      END IF;
      -- Neither a page anchor nor a cursor may cross scope/sort/session/version
      -- or claim authority from a future selection revision.
      v_token:=private.pay_workbench_modal_cursor_decode_v2(v_anchor,'{}'::jsonb);
      FOREACH v_error IN ARRAY ARRAY['scope_hash','session_id','session_version','progress_counter_version','sort_key','sort_direction'] LOOP
        BEGIN
          PERFORM private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,
            private.pay_workbench_modal_cursor_encode_v2(v_token || jsonb_build_object(v_error,CASE v_error
              WHEN 'scope_hash' THEN to_jsonb(repeat('0',64))
              WHEN 'session_id' THEN to_jsonb('00000000-0000-4000-8000-000000099999'::text)
              WHEN 'session_version' THEN to_jsonb(v_session.version+1)
              WHEN 'progress_counter_version' THEN to_jsonb(v_session.progress_counter_version+1)
              WHEN 'sort_key' THEN to_jsonb('NOT_A_SORT'::text)
              ELSE to_jsonb('NOT_A_DIRECTION'::text) END)),10);
          RAISE EXCEPTION 'CROSS_AUTHORITY_ANCHOR_ACCEPTED';
        EXCEPTION WHEN SQLSTATE 'P0001' THEN
          IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF;
        END;
      END LOOP;
      -- Removing the anchor must backfill its correctly sorted current page.
      v_id:=(v_token->>'last_id')::uuid;
      SELECT row_json INTO v_row_json FROM public.banking_pay_workbench_preview_rows WHERE session_id=v_session.id AND candidate_id=v_id;
      UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || '{"is_hidden":true}'::jsonb
        WHERE session_id=v_session.id AND candidate_id=v_id;
      UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=progress_counter_version+1
        WHERE id=v_session.id RETURNING * INTO v_session;
      v_options:=v_options || jsonb_build_object('expected_progress_counter_version',v_session.progress_counter_version);
      v_current:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_anchor,10);
      IF (v_current->>'total_count')::integer<>104 OR jsonb_array_length(v_current->'rows')<>10
        OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_current->'rows') r WHERE r->>'candidate_id'=v_id::text) THEN
        RAISE EXCEPTION 'DEPARTED_ANCHOR_DID_NOT_BACKFILL';
      END IF;
      v_back:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_current->>'previous_cursor',10);
      v_next:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,v_key,v_direction,v_back->>'next_cursor',10);
      IF v_next->'rows' IS DISTINCT FROM v_current->'rows' THEN RAISE EXCEPTION 'DEPARTED_ANCHOR_ROUND_TRIP_FAILED'; END IF;
      UPDATE public.banking_pay_workbench_preview_rows SET row_json=v_row_json WHERE session_id=v_session.id AND candidate_id=v_id;
    END LOOP;
  END LOOP;
  v_options:=v_options || jsonb_build_object('expected_progress_counter_version',v_session.progress_counter_version);
  v_first:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,'READY_TO_PAY','ASC',NULL,10);
  v_anchor:=v_first->>'page_anchor';v_id:=(v_first#>>'{rows,0,candidate_id}')::uuid;
  -- Change only the synthetic selected amount fixture. The reader must use its
  -- current canonical scalar rather than the amount encoded in the old anchor.
  UPDATE public.banking_pay_workbench_preview_rows SET selected=true,selection_state='SELECTED',
    row_json=row_json || '{"line_type":"TIMESHEET_PAYMENT","amount_display":"99999.00","section_amount_display":"99999.00","amount_ex_vat":"99999.00"}'::jsonb
    WHERE session_id=v_session.id AND candidate_id=v_id;
  UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=progress_counter_version+1
    WHERE id=v_session.id RETURNING * INTO v_session;
  v_options:=v_options || jsonb_build_object('expected_progress_counter_version',v_session.progress_counter_version);
  v_current:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,'READY_TO_PAY','ASC',v_anchor,10);
  IF v_current->>'page_number'<>'11' OR jsonb_array_length(v_current->'rows')<>5
    OR v_current#>>'{rows,4,candidate_id}' IS DISTINCT FROM v_id::text
    OR v_current#>>'{rows,4,selected_display_amount}' IS DISTINCT FROM '99999.00' THEN
    RAISE EXCEPTION 'AMOUNT_ANCHOR_USED_STALE_FINANCIAL_SORT_VALUE';
  END IF;
  -- Removing that final page must not produce an empty list with a positive
  -- total. Retain a complete current last page and a usable Previous control.
  v_anchor:=v_current->>'page_anchor';
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || '{"is_hidden":true}'::jsonb
    WHERE session_id=v_session.id AND candidate_id IN(SELECT (r->>'candidate_id')::uuid FROM jsonb_array_elements(v_current->'rows') r);
  UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=progress_counter_version+1
    WHERE id=v_session.id RETURNING * INTO v_session;
  v_options:=v_options || jsonb_build_object('expected_progress_counter_version',v_session.progress_counter_version);
  v_current:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,'READY_TO_PAY','ASC',v_anchor,10);
  IF v_current->>'total_count'<>'100' OR v_current->>'page_number'<>'10' OR jsonb_array_length(v_current->'rows')<>10
    OR v_current->'has_more'<>'false'::jsonb THEN RAISE EXCEPTION 'DEPARTED_FINAL_PAGE_NOT_RECOVERED'; END IF;
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || '{"is_hidden":true}'::jsonb WHERE session_id=v_session.id;
  v_current:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,'READY_TO_PAY','ASC',v_anchor,10);
  IF v_current->>'total_count'<>'0' OR v_current->'rows'<>'[]'::jsonb OR v_current->'page_anchor'<>'null'::jsonb
    OR v_current->'has_previous'<>'false'::jsonb OR v_current->>'page_number'<>'0' THEN
    RAISE EXCEPTION 'TRUE_EMPTY_ANCHOR_STATE_INCORRECT';
  END IF;
END
$anchor_proof$;
ROLLBACK;
