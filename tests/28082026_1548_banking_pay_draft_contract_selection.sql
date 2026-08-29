\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
-- Valid synthetic pre-Draft row identities; no timesheet/customer is copied.
-- The original preview-page reader publishes the physical FK, so populate it
-- as well as row_json. A JSON-only placeholder would not model real rows.
INSERT INTO public.timesheets(timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date)
SELECT id,'rollback-draft-' || id::text,'rollback candidate','rollback hospital','rollback ward','rollback role','2026-08-23'
FROM public.banking_pay_workbench_preview_rows WHERE session_id='10000000-0000-4000-8000-000000000005';
UPDATE public.banking_pay_workbench_preview_rows
SET timesheet_id=id,key_type='TS_DAY',key_value='2026-08-23',row_json=row_json || jsonb_build_object(
  'candidate_id',candidate_id,'timesheet_id',id,'key_type','TS_DAY','key_value','2026-08-23',
  'selected',selected,'selection_state',selection_state,'status',status,
  'preview_contract',jsonb_build_object('ok',true,'selection_allowed',section='canonical_preview_lines'))
WHERE session_id='10000000-0000-4000-8000-000000000005';
UPDATE public.banking_pay_workbench_preview_rows SET selected=false,selection_state='UNSELECTED',
  row_json=row_json || '{"selected":false,"selection_state":"UNSELECTED"}'::jsonb
WHERE session_id='10000000-0000-4000-8000-000000000005'
  AND candidate_id='10000000-0000-4000-8000-000000000002' AND section='canonical_preview_lines';

CREATE FUNCTION pg_temp.bpay_fixture_selection(p_select boolean,p_legacy boolean) RETURNS void LANGUAGE plpgsql AS $f$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE; ids jsonb; part integer; answer jsonb;
BEGIN
  SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  IF NOT p_legacy THEN
    answer:=public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('modal_candidate_intent_v2',jsonb_build_object(
      'candidate_id','10000000-0000-4000-8000-000000000002','request_id',gen_random_uuid(),
      'action',CASE WHEN p_select THEN 'SELECT_ALL_READY' ELSE 'CLEAR_ALL_READY' END,
      'options',jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
        'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL')))),s.actor_user_id);
    IF answer->>'ok'<>'true' THEN RAISE EXCEPTION 'FIXTURE_V2_SELECTION_FAILED'; END IF;
    RETURN;
  END IF;
  -- TEST ORACLE ONLY: replay the existing individual controls. Production v2
  -- must use the single candidate mutation, never this per-page loop.
  IF NOT p_select THEN
    SELECT jsonb_agg(id::text ORDER BY row_ordinal,id) INTO ids FROM public.banking_pay_workbench_preview_rows
    WHERE session_id=s.id AND candidate_id='10000000-0000-4000-8000-000000000002' AND section='blocked_for_pay';
    PERFORM public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('expected_session_version',s.version,
      'expected_progress_counter_version',s.progress_counter_version,'deselect_preview_row_ids',ids),s.actor_user_id);
  END IF;
  FOR part IN 0..1 LOOP
    SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
    SELECT jsonb_agg(id::text ORDER BY row_ordinal,id) INTO ids FROM (
      SELECT id,row_ordinal FROM public.banking_pay_workbench_preview_rows
      WHERE session_id=s.id AND candidate_id='10000000-0000-4000-8000-000000000002' AND section='canonical_preview_lines'
      ORDER BY row_ordinal,id LIMIT 100 OFFSET part*100) q;
    PERFORM public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('expected_session_version',s.version,
      'expected_progress_counter_version',s.progress_counter_version,
      CASE WHEN p_select THEN 'select_preview_row_ids' ELSE 'deselect_preview_row_ids' END,ids),s.actor_user_id);
  END LOOP;
  IF p_select THEN
    SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
    SELECT jsonb_agg(id::text ORDER BY row_ordinal,id) INTO ids FROM public.banking_pay_workbench_preview_rows
    WHERE session_id=s.id AND candidate_id='10000000-0000-4000-8000-000000000002' AND section='blocked_for_pay';
    PERFORM public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('expected_session_version',s.version,
      'expected_progress_counter_version',s.progress_counter_version,'select_preview_row_ids',ids),s.actor_user_id);
  END IF;
END $f$;

CREATE FUNCTION pg_temp.bpay_fixture_capture(p_phase text) RETURNS jsonb LANGUAGE plpgsql AS $f$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE; rows_json jsonb; pages jsonb:='[]'; page jsonb; cursor_json jsonb;
  section_name text; guard_results jsonb:='[]'; ids jsonb; supplied jsonb; expected_revision bigint; variant text;
  error_text text; expected_error text; before_batches bigint; before_items bigint;
BEGIN
  SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  SELECT jsonb_agg(to_jsonb(r) ORDER BY row_ordinal,id),jsonb_agg(id::text ORDER BY id)
    INTO rows_json,ids FROM public.banking_pay_workbench_preview_rows r
    WHERE session_id=s.id AND session_version=s.version AND selected=true AND status='READY';
  FOREACH section_name IN ARRAY ARRAY['canonical_preview_lines','blocked_for_pay'] LOOP
    cursor_json:='{}';
    FOR page_index IN 1..5 LOOP
      page:=public.pay_workbench_session_get_preview_page(s.id,section_name,cursor_json,100);
      pages:=pages || jsonb_build_array(page);
      EXIT WHEN COALESCE((page->>'has_more')::boolean,false)=false;
      cursor_json:=page->'next_cursor';
      IF page_index=5 OR cursor_json IS NULL THEN RAISE EXCEPTION 'FIXTURE_PAGE_CONTINUATION_FAILED'; END IF;
    END LOOP;
  END LOOP;
  SELECT count(*) INTO before_batches FROM public.pay_batches;
  SELECT count(*) INTO before_items FROM public.pay_batch_items;
  FOREACH variant IN ARRAY ARRAY['EXACT','OMITTED_PAYMENT','CANDIDATE_ID','DUPLICATE','STALE_REVISION'] LOOP
    supplied:=CASE variant WHEN 'OMITTED_PAYMENT' THEN ids-0
      WHEN 'CANDIDATE_ID' THEN jsonb_build_array('10000000-0000-4000-8000-000000000002')
      WHEN 'DUPLICATE' THEN ids || jsonb_build_array(ids->0) ELSE ids END;
    expected_revision:=s.progress_counter_version-CASE WHEN variant='STALE_REVISION' THEN 1 ELSE 0 END;
    expected_error:=CASE WHEN variant='EXACT' THEN 'WORKBENCH_REFRESH_IN_PROGRESS' ELSE 'WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED' END;
    BEGIN
      -- This uncommitted local blocker sits immediately AFTER the real locked
      -- selection guard. EXACT must reach it; every mismatch must stop earlier.
      -- No operation runner is invoked and no batch/financial item is created.
      INSERT INTO public.banking_pay_workbench_jobs(session_id,job_type,status,dedupe_key)
      VALUES(s.id,'WORKBENCH_CANDIDATE_DELTA_REFRESH','QUEUED','rollback-only-draft-guard');
      INSERT INTO public.banking_pay_operations(id,operation_type,status,phase,actor_user_id,workbench_session_id,idempotency_key,input_json)
      VALUES('10000000-0000-4000-8000-000000009001','DRAFT_CREATE','RUNNING','VALIDATE_SESSION',s.actor_user_id,s.id,
        'rollback-only-draft-guard',jsonb_build_object('expected_workbench_progress_counter_version',expected_revision,
          'expected_workbench_selected_preview_row_ids',supplied));
      PERFORM public.pay_workbench_prepare_draft(p_session_id=>s.id,p_actor_user_id=>s.actor_user_id,
        p_operation_id=>'10000000-0000-4000-8000-000000009001',p_operation_mode=>true);
      RAISE EXCEPTION 'FIXTURE_DRAFT_GUARD_DID_NOT_STOP';
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS error_text=MESSAGE_TEXT;
      IF position(expected_error IN error_text)=0 THEN RAISE EXCEPTION 'FIXTURE_DRAFT_GUARD_WRONG_OUTCOME: % / %',variant,error_text; END IF;
      guard_results:=guard_results || jsonb_build_array(jsonb_build_object('variant',variant,'code',expected_error));
    END;
  END LOOP;
  IF before_batches<>(SELECT count(*) FROM public.pay_batches) OR before_items<>(SELECT count(*) FROM public.pay_batch_items)
    OR EXISTS(SELECT 1 FROM public.banking_pay_operations WHERE id='10000000-0000-4000-8000-000000009001')
    OR EXISTS(SELECT 1 FROM public.banking_pay_workbench_jobs WHERE dedupe_key='rollback-only-draft-guard') THEN
    RAISE EXCEPTION 'FIXTURE_DRAFT_GUARD_LEFT_STATE';
  END IF;
  RETURN jsonb_build_object('phase',p_phase,'session',to_jsonb(s),'rows',rows_json,'pages',pages,'guards',guard_results);
END $f$;

\if :legacy
DO $f$ BEGIN PERFORM pg_temp.bpay_fixture_selection(true,true); END $f$;
\else
DO $f$ BEGIN PERFORM pg_temp.bpay_fixture_selection(true,false); END $f$;
\endif
SELECT pg_temp.bpay_fixture_capture('ALL');
DO $f$ DECLARE s public.banking_pay_workbench_sessions%ROWTYPE; BEGIN
  SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  PERFORM public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('expected_session_version',s.version,
    'expected_progress_counter_version',s.progress_counter_version,'deselect_preview_row_ids',
    jsonb_build_array('10000000-0000-4000-8000-000000001001')),s.actor_user_id);
END $f$;
SELECT pg_temp.bpay_fixture_capture('SOME');
\if :legacy
DO $f$ BEGIN PERFORM pg_temp.bpay_fixture_selection(false,true); END $f$;
\else
DO $f$ BEGIN PERFORM pg_temp.bpay_fixture_selection(false,false); END $f$;
\endif
SELECT pg_temp.bpay_fixture_capture('NONE');
ROLLBACK;
