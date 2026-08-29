\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
UPDATE public.banking_pay_workbench_preview_rows SET section='cases_resolutions',selected=false,selection_state='NOT_SELECTABLE',
 row_json=row_json || jsonb_build_object('presentation_section','CASES_RESOLUTIONS','readiness_state','CASES_RESOLUTIONS',
 'case_key','finance:10000000-0000-4000-8000-000000003001','finance_case_id','10000000-0000-4000-8000-000000003001',
 'resolution_family','NON_BUCKET','case_needs_resolution',true,'case_resolution_satisfied_now',false)
WHERE session_id='10000000-0000-4000-8000-000000000005' AND row_ordinal<=105;

CREATE FUNCTION pg_temp.finance_task_counts(p_tasks integer,p_members integer,p_channel text DEFAULT 'ALL',p_filters jsonb DEFAULT '{}')
RETURNS void LANGUAGE plpgsql AS $test$
DECLARE v_session public.banking_pay_workbench_sessions%ROWTYPE;v_tasks integer;v_members integer;v_before text;v_after text;
BEGIN
 SELECT * INTO STRICT v_session FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 v_session.filters_json:=p_filters;
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO v_before FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=v_session.id;
 SELECT count(DISTINCT t.task_key),count(*) INTO v_tasks,v_members
 FROM private.pay_workbench_modal_finance_task_members_v2(v_session,p_channel) t;
 IF (v_tasks,v_members) IS DISTINCT FROM (p_tasks,p_members) THEN
   RAISE EXCEPTION 'FINANCE_TASK_COUNT expected %/% got %/%',p_tasks,p_members,v_tasks,v_members;
 END IF;
 IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_finance_task_members_v2(v_session,p_channel) t
   JOIN public.banking_pay_workbench_preview_rows r ON r.id=t.preview_row_id
   WHERE t.row_payload IS DISTINCT FROM private.pay_workbench_modal_row_payload_v2(r)) THEN
   RAISE EXCEPTION 'FINANCE_TASK_PAYLOAD_CHANGED';
 END IF;
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO v_after FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=v_session.id;
 IF v_before IS DISTINCT FROM v_after THEN RAISE EXCEPTION 'FINANCE_TASK_READ_MUTATED'; END IF;
END;
$test$;
DO $tasks$
DECLARE v_id uuid:='10000000-0000-4000-8000-000000000005';v_candidate uuid:='10000000-0000-4000-8000-000000000002';
 v_session public.banking_pay_workbench_sessions%ROWTYPE;v_error text;v_actions integer;v_blocked integer;v_entries integer;v_unique integer;
BEGIN
 -- The pre-existing passive recovery has this same finance-case owner. Keep
 -- it as related detail, without changing its Blocked/selection authority.
 PERFORM pg_temp.finance_task_counts(1,106);
 PERFORM pg_temp.finance_task_counts(1,106,'PAYE');
 SELECT * INTO STRICT v_session FROM public.banking_pay_workbench_sessions WHERE id=v_id;
 IF NOT EXISTS(SELECT 1 FROM private.pay_workbench_modal_finance_task_members_v2(v_session,'ALL') t
   WHERE t.preview_row_id='10000000-0000-4000-8000-000000002001' AND t.task_json->'context_only'='true'::jsonb) THEN
   RAISE EXCEPTION 'PASSIVE_RECOVERY_CONTEXT_WAS_MADE_ACTIONABLE';
 END IF;
 PERFORM pg_temp.finance_task_counts(0,0,'UMBRELLA');
 PERFORM pg_temp.finance_task_counts(0,0,'ALL','{"candidate_id":"10000000-0000-4000-8000-000000000003"}');
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || jsonb_build_object(
  'case_key','finance:10000000-0000-4000-8000-000000003001','finance_case_id','10000000-0000-4000-8000-000000003001',
  'resolution_family','NON_BUCKET','case_needs_resolution',true)
 WHERE id='10000000-0000-4000-8000-000000001106';
 PERFORM pg_temp.finance_task_counts(1,107);
 PERFORM pg_temp.finance_task_counts(1,106,'PAYE');
 PERFORM pg_temp.finance_task_counts(0,0,'UMBRELLA');
 IF NOT EXISTS(SELECT 1 FROM private.pay_workbench_modal_finance_task_members_v2(v_session,'ALL') t
   WHERE t.preview_row_id='10000000-0000-4000-8000-000000001106' AND t.task_json->'context_only'='true'::jsonb) THEN
   RAISE EXCEPTION 'READY_CONTEXT_WAS_MADE_ACTIONABLE';
 END IF;
 -- One row exposes only a component action; another row exposes the primary
 -- case action. It is still one problem, not a case task PLUS a component task.
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || jsonb_build_object(
  'case_needs_resolution',false,'case_components',jsonb_build_array(jsonb_build_object(
    'key','fixture-component','source_basis_fingerprint','fixture-basis','needs_action',true,'show_manual_amount_control',true)))
 WHERE id='10000000-0000-4000-8000-000000001001';
 PERFORM pg_temp.finance_task_counts(1,107);
 -- A resolved presentation still belongs in the complete affected case detail.
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || '{"case_needs_resolution":false}'
 WHERE id='10000000-0000-4000-8000-000000001002';
 PERFORM pg_temp.finance_task_counts(1,107);
 -- Missing UUID on one alias must join the same exact case-key owner.
 SELECT count(*) FILTER(WHERE issue_state='ACTION_REQUIRED'),count(*) FILTER(WHERE issue_state='BLOCKED'),count(*),count(DISTINCT identity)
 INTO v_actions,v_blocked,v_entries,v_unique FROM private.pay_workbench_modal_issue_index_v2(v_session,'ALL','REVOLUT','SANDBOX',
   '{"next_recommended_action":"WAIT_FOR_WORKER"}',true,true);
 IF (v_actions,v_blocked,v_entries,v_unique) IS DISTINCT FROM (1,3,4,4) THEN
   RAISE EXCEPTION 'FINANCE_INDEX_DUPLICATED_CASE_CONTEXT_OR_LOST_PASSIVE: %/%/%/%',v_actions,v_blocked,v_entries,v_unique;END IF;
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json-'finance_case_id'
 WHERE id='10000000-0000-4000-8000-000000001003';
 PERFORM pg_temp.finance_task_counts(1,107);
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || '{"presentation_role":"HIDDEN_INDEFINITE_SNOOZE"}'
 WHERE id='10000000-0000-4000-8000-000000001004';
 PERFORM pg_temp.finance_task_counts(1,106);
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || '{"client_id":"10000000-0000-4000-8000-000000009992"}'
 WHERE session_id=v_id;
 PERFORM pg_temp.finance_task_counts(0,0,'ALL','{"client_id":"10000000-0000-4000-8000-000000009993"}');
 UPDATE public.banking_pay_workbench_session_scope SET dirty=true WHERE session_id=v_id AND candidate_id=v_candidate;
 PERFORM pg_temp.finance_task_counts(0,0);
 UPDATE public.banking_pay_workbench_session_scope SET dirty=false WHERE session_id=v_id AND candidate_id=v_candidate;
 UPDATE public.banking_pay_workbench_preview_rows SET session_version=2 WHERE id='10000000-0000-4000-8000-000000001005';
 PERFORM pg_temp.finance_task_counts(1,105);
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || '{"finance_case_id":"10000000-0000-4000-8000-000000009991"}'
 WHERE id='10000000-0000-4000-8000-000000001006';
 SELECT * INTO STRICT v_session FROM public.banking_pay_workbench_sessions WHERE id=v_id;
 BEGIN
   PERFORM * FROM private.pay_workbench_modal_finance_task_members_v2(v_session,'ALL');
   RAISE EXCEPTION 'CONFLICTING_FINANCE_OWNER_ACCEPTED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN
   GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
   IF v_error<>'BANKING_PAY_V2_CONFLICTING_TASK_OWNER' THEN RAISE; END IF;
 END;
 SET LOCAL client_min_messages='notice';
 RAISE NOTICE 'PASS:105 case presentations grouped into one task; mixed component/resolved rows retained; alias ownership, payload, hidden/filter/version/current-source fences.';
END;
$tasks$;
ROLLBACK;
