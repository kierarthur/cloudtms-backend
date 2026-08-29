\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='15s';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql

-- Set up a completed SYNTHETIC generation; no hosted jobs or drains are used.
UPDATE public.banking_pay_workbench_jobs SET status='SUCCEEDED'
WHERE session_id IS NULL AND candidate_id IN ('10000000-0000-4000-8000-000000000002','10000000-0000-4000-8000-000000000003')
  AND job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY';
UPDATE public.settings_defaults SET banking_pay_workbench_scope_reconcile_shadow_mode=false
WHERE id=(SELECT id FROM public.settings_defaults ORDER BY id LIMIT 1);
UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true,
  scope_change_generation_applied=public.pay_workbench_scope_current_generation_v1(),preview_row_count=112
WHERE id='10000000-0000-4000-8000-000000000005';

CREATE FUNCTION pg_temp.bpay_assert_draft_gate(p_label text,p_selected bigint,p_expected boolean,p_code text DEFAULT NULL)
RETURNS void LANGUAGE plpgsql AS $test$
DECLARE v_id uuid:='10000000-0000-4000-8000-000000000005'; v_before jsonb; v_after jsonb;
  v_gate jsonb; v_owner jsonb; v_scope jsonb; v_expected_codes jsonb;
BEGIN
  SELECT jsonb_build_object('session',(SELECT to_jsonb(s) FROM public.banking_pay_workbench_sessions s WHERE id=v_id),
    'rows',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_id),
    'scope',(SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM public.banking_pay_workbench_session_scope s WHERE session_id=v_id),
    'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM public.banking_pay_workbench_jobs j),
    'settings',(SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM public.settings_defaults s)) INTO v_before;
  v_owner:=public.pay_workbench_session_recompute_progress_counters(v_id,false,'BANKING_PAY_MODAL_STRUCTURE_V2',false);
  v_scope:=public.pay_workbench_scope_progress_v1(v_id);
  v_gate:=private.pay_workbench_modal_draft_gate_v2(v_id,p_selected);
  IF (v_gate->>'can_create_draft')::boolean IS DISTINCT FROM p_expected THEN
    RAISE EXCEPTION 'DRAFT_GATE_EXPECTATION: % expected % got %',p_label,p_expected,v_gate;
  END IF;
  IF (v_gate->'can_create_draft'='true'::jsonb) IS DISTINCT FROM (v_owner->'can_create_draft'='true'::jsonb
      AND v_owner->'read_only'='false'::jsonb AND v_scope->'draft_safe'='true'::jsonb AND p_selected>0
      AND jsonb_array_length(v_owner->'draft_blocker_codes')=0) THEN
    RAISE EXCEPTION 'DRAFT_OWNER_PARITY: %',p_label;
  END IF;
  v_expected_codes:=v_owner->'draft_blocker_codes';
  IF v_scope->'draft_safe'='false'::jsonb AND NULLIF(v_scope->>'draft_block_reason_code','') IS NOT NULL
      AND NOT v_expected_codes ? (v_scope->>'draft_block_reason_code') THEN
    v_expected_codes:=v_expected_codes||jsonb_build_array(v_scope->'draft_block_reason_code');
  END IF;
  IF p_selected=0 AND NOT v_expected_codes ? 'NO_SELECTED_ROWS' THEN
    v_expected_codes:=v_expected_codes || jsonb_build_array('NO_SELECTED_ROWS');
  END IF;
  IF v_gate->'blocker_codes' IS DISTINCT FROM v_expected_codes THEN RAISE EXCEPTION 'DRAFT_BLOCKERS_PARITY: %',p_label; END IF;
  IF p_code IS NOT NULL AND NOT v_gate->'blocker_codes' ? p_code THEN RAISE EXCEPTION 'DRAFT_BLOCKER_MISSING: % %',p_label,p_code; END IF;
  IF v_gate->'session_ready' IS DISTINCT FROM v_owner->'session_ready'
    OR v_gate->'read_only' IS DISTINCT FROM v_owner->'read_only'
    OR v_gate->'work_queued' IS DISTINCT FROM v_owner->'work_queued'
    OR v_gate->'next_recommended_action' IS DISTINCT FROM v_owner->'next_recommended_action' THEN RAISE EXCEPTION 'DRAFT_STATE_PARITY: %',p_label; END IF;
  IF v_gate->'display_ready' IS DISTINCT FROM v_scope->'display_ready'
    OR v_gate->'draft_safe' IS DISTINCT FROM v_scope->'draft_safe'
    OR v_gate->'draft_block_reason_code' IS DISTINCT FROM v_scope->'draft_block_reason_code'
    OR v_gate->'session_selected_row_count' IS DISTINCT FROM v_owner->'selected_row_count'
    OR v_gate->'session_selected_eligible_ready_row_count' IS DISTINCT FROM v_owner->'selected_eligible_ready_row_count'
    THEN RAISE EXCEPTION 'DRAFT_SCOPE_AND_COMPLETE_SESSION_PARITY: %',p_label; END IF;
  IF octet_length(v_gate::text)>2048 THEN RAISE EXCEPTION 'DRAFT_GATE_PAYLOAD_BUDGET'; END IF;
  SELECT jsonb_build_object('session',(SELECT to_jsonb(s) FROM public.banking_pay_workbench_sessions s WHERE id=v_id),
    'rows',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_id),
    'scope',(SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM public.banking_pay_workbench_session_scope s WHERE session_id=v_id),
    'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM public.banking_pay_workbench_jobs j),
    'settings',(SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM public.settings_defaults s)) INTO v_after;
  IF v_before IS DISTINCT FROM v_after THEN RAISE EXCEPTION 'DRAFT_GATE_WROTE_STATE: %',p_label; END IF;
END;
$test$;

DO $gates$
DECLARE v_id uuid:='10000000-0000-4000-8000-000000000005'; v_candidate uuid:='10000000-0000-4000-8000-000000000002';
  v_status text; v_count integer:=0; v_error text;
BEGIN
  PERFORM pg_temp.bpay_assert_draft_gate('Ready',105,true); v_count:=v_count+1;
  IF private.pay_workbench_modal_draft_gate_v2(v_id,105)->'session_selected_row_count' IS DISTINCT FROM '109'::jsonb THEN
    RAISE EXCEPTION 'FILTERED_COUNT_REPLACED_COMPLETE_SESSION_COUNT';
  END IF;
  UPDATE public.banking_pay_workbench_sessions SET scope_change_generation_applied=0 WHERE id=v_id;
  IF public.pay_workbench_scope_current_generation_v1()<=0 THEN RAISE EXCEPTION 'MISSING_SYNTHETIC_GENERATION'; END IF;
  PERFORM pg_temp.bpay_assert_draft_gate('Background generation not applied',105,false,'SCOPE_RECONCILIATION_REQUIRED'); v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_sessions SET scope_change_generation_applied=public.pay_workbench_scope_current_generation_v1() WHERE id=v_id;
  UPDATE public.settings_defaults SET banking_pay_workbench_scope_reconcile_shadow_mode=true
    WHERE id=(SELECT id FROM public.settings_defaults ORDER BY id LIMIT 1);
  PERFORM pg_temp.bpay_assert_draft_gate('Verification-only scope',105,false,'SCOPE_RECONCILIATION_SHADOW_MODE'); v_count:=v_count+1;
  UPDATE public.settings_defaults SET banking_pay_workbench_scope_reconcile_shadow_mode=false
    WHERE id=(SELECT id FROM public.settings_defaults ORDER BY id LIMIT 1);
  INSERT INTO public.banking_pay_workbench_jobs(id,candidate_id,job_type,dedupe_key,status,scope_change_generation)
    VALUES('10000000-0000-4000-8000-000000005110',v_candidate,'WORKBENCH_CANDIDATE_DIRTY_APPLY',
      'local-draft-gate-upstream','QUEUED',public.pay_workbench_scope_current_generation_v1());
  FOREACH v_status IN ARRAY ARRAY['QUEUED','RUNNING','FAILED','DEAD','SUCCEEDED'] LOOP
    UPDATE public.banking_pay_workbench_jobs SET status=v_status WHERE id='10000000-0000-4000-8000-000000005110';
    PERFORM pg_temp.bpay_assert_draft_gate('Upstream '||v_status,105,v_status='SUCCEEDED',
      CASE WHEN v_status IN ('QUEUED','RUNNING') THEN 'UPSTREAM_SCOPE_EXPANSION_IN_PROGRESS'
        WHEN v_status IN ('FAILED','DEAD') THEN 'UPSTREAM_SCOPE_FAILURE_UNRESOLVED' ELSE NULL END);
    v_count:=v_count+1;
  END LOOP;
  PERFORM pg_temp.bpay_assert_draft_gate('Other channel selected but active filter empty',0,false,'NO_SELECTED_ROWS'); v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_sessions SET progress_json='{"ready":false,"can_create_draft":false}' WHERE id=v_id;
  PERFORM pg_temp.bpay_assert_draft_gate('Stored false does not override current owner',105,true); v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_sessions SET progress_json='{"ready":true,"can_create_draft":true}',scope_seed_complete=false WHERE id=v_id;
  PERFORM pg_temp.bpay_assert_draft_gate('Incomplete scope',105,false,'WORKBENCH_REFRESH_PENDING'); v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true,scope_next_cursor_json='{"fixture":"remaining"}' WHERE id=v_id;
  PERFORM pg_temp.bpay_assert_draft_gate('Scope has another page',105,false,'WORKBENCH_REFRESH_PENDING'); v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_sessions SET scope_next_cursor_json='{}',progress_json='{"replacement_required":true}' WHERE id=v_id;
  PERFORM pg_temp.bpay_assert_draft_gate('Replacement required',105,false,'REPLACEMENT_REQUIRED'); v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_sessions SET progress_json='{"ready":true}' WHERE id=v_id;
  FOREACH v_status IN ARRAY ARRAY['READY','MATERIALISED','MATERIALIZED','SOURCE_EMPTY','FAILED','ERROR','LINE_WORK_ERROR','LINE_WORK_PROCESS_ERROR','SOURCE_BUILD_ERROR','PENDING','SOURCE_BUILD_PENDING'] LOOP
    UPDATE public.banking_pay_workbench_session_scope SET status=v_status WHERE session_id=v_id AND candidate_id=v_candidate;
    PERFORM pg_temp.bpay_assert_draft_gate('Scope ' || v_status,105,v_status=ANY(ARRAY['READY','MATERIALISED','MATERIALIZED','SOURCE_EMPTY']));
    v_count:=v_count+1;
  END LOOP;
  UPDATE public.banking_pay_workbench_session_scope SET status='READY',dirty=true WHERE session_id=v_id AND candidate_id=v_candidate;
  PERFORM pg_temp.bpay_assert_draft_gate('Dirty source with persisted Ready',105,false,'WORKBENCH_REFRESH_PENDING'); v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_session_scope SET dirty=false,error_json='{"code":"FIXTURE_ERROR"}' WHERE session_id=v_id AND candidate_id=v_candidate;
  PERFORM pg_temp.bpay_assert_draft_gate('Source error',105,false,'WORKBENCH_ERRORS_PRESENT'); v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_session_scope SET error_json=NULL,certified_preview_publication_required=true,certified_preview_publication_parity_ok=false WHERE session_id=v_id AND candidate_id=v_candidate;
  PERFORM pg_temp.bpay_assert_draft_gate('Uncertified current publication',105,false,'CURRENT_SOURCE_PREVIEW_PUBLICATION_INCOMPLETE'); v_count:=v_count+1;
  UPDATE public.banking_pay_workbench_session_scope SET certified_preview_publication_required=false WHERE session_id=v_id AND candidate_id=v_candidate;

  INSERT INTO public.banking_pay_workbench_candidate_line_work(session_id,candidate_id,line_key,line_ordinal,status)
  VALUES(v_id,v_candidate,'draft-gate-fixture',1,'PENDING');
  FOREACH v_status IN ARRAY ARRAY['PENDING','ERROR','FAILED','READY','MATERIALISED','SKIPPED','SUPERSEDED','SOURCE_EMPTY','NOT_APPLICABLE','OBSOLETE'] LOOP
    UPDATE public.banking_pay_workbench_candidate_line_work SET status=v_status WHERE session_id=v_id AND line_key='draft-gate-fixture';
    PERFORM pg_temp.bpay_assert_draft_gate('Line work ' || v_status,105,v_status<>ALL(ARRAY['PENDING','ERROR','FAILED']));
    v_count:=v_count+1;
  END LOOP;
  INSERT INTO public.banking_pay_workbench_jobs(id,session_id,candidate_id,job_type,dedupe_key,status)
  VALUES('10000000-0000-4000-8000-000000005100',v_id,v_candidate,'PAYEE_READINESS_ENSURE','local-draft-gate-readiness','QUEUED');
  FOREACH v_status IN ARRAY ARRAY['QUEUED','RUNNING','SUCCEEDED'] LOOP
    UPDATE public.banking_pay_workbench_jobs SET status=v_status WHERE id='10000000-0000-4000-8000-000000005100';
    PERFORM pg_temp.bpay_assert_draft_gate('Readiness job ' || v_status,105,v_status='SUCCEEDED');
    v_count:=v_count+1;
  END LOOP;
  UPDATE public.banking_pay_workbench_preview_rows SET selected=false,selection_state='UNSELECTED' WHERE session_id=v_id;
  PERFORM pg_temp.bpay_assert_draft_gate('No session selection',0,false,'NO_SELECTED_ROWS'); v_count:=v_count+1;
  BEGIN
    PERFORM private.pay_workbench_modal_draft_gate_v2(v_id,-1);
    RAISE EXCEPTION 'NEGATIVE_COUNT_ACCEPTED';
  EXCEPTION WHEN SQLSTATE '22023' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_INVALID_INPUT' THEN RAISE; END IF;
  END;
  BEGIN
    PERFORM private.pay_workbench_modal_draft_gate_v2('10000000-0000-4000-8000-000000009999',1);
    RAISE EXCEPTION 'MISSING_SESSION_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'WORKBENCH_SESSION_NOT_FOUND' THEN RAISE; END IF;
  END;
  RAISE NOTICE 'PASS: % current-owner Draft display states; no state write or Draft creation; negative inputs typed.',v_count;
END;
$gates$;
ROLLBACK;
