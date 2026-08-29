\set ON_ERROR_STOP on
BEGIN;
SET LOCAL client_min_messages='warning';
SET LOCAL statement_timeout='30s';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
-- Synthetic fixtures with explicit client metadata; local rollback only.
UPDATE public.banking_pay_workbench_preview_rows
SET selected=false,selection_state='UNSELECTED',row_json=row_json||jsonb_build_object('client_id',
 CASE WHEN row_ordinal%2=1 THEN '60000000-0000-4000-8000-000000000001' ELSE '60000000-0000-4000-8000-000000000002' END)
WHERE session_id='10000000-0000-4000-8000-000000000005' AND row_key LIKE 'selection-fixture:%';
UPDATE public.banking_pay_workbench_preview_rows SET section='blocked_for_pay',selection_state='NOT_SELECTABLE',
 row_json=row_json||'{"hidden_indefinite_snooze":true,"snooze_state":{"state":"SNOOZED"},"draftable":false,"is_ready_for_draft":false,"selection_allowed":false,"presentation_section":"BLOCKED_FOR_PAY"}'::jsonb
WHERE row_key='selection-fixture:1' AND session_id='10000000-0000-4000-8000-000000000005';
UPDATE public.banking_pay_workbench_preview_rows SET section='cases_resolutions',selection_state='NOT_SELECTABLE',
 row_json=row_json||'{"draftable":false,"is_ready_for_draft":false,"selection_allowed":false,"presentation_section":"CASES_RESOLUTIONS"}'::jsonb
WHERE row_key='selection-fixture:2' AND session_id='10000000-0000-4000-8000-000000000005';
UPDATE public.banking_pay_workbench_preview_rows SET selection_state='NOT_SELECTABLE',
 row_json=row_json||'{"draftable":false,"is_ready_for_draft":false,"selection_allowed":false}'::jsonb
WHERE row_key='selection-fixture:3' AND session_id='10000000-0000-4000-8000-000000000005';

CREATE TEMP TABLE global_filter_checks(channel text,filter_case int) ON COMMIT DROP;
-- One SELECT per scenario keeps the normal per-statement timeout meaningful.
-- Combining 26 mutations inside one DO incorrectly timed the entire matrix as
-- a single application request. No application timeout has been increased.
CREATE FUNCTION pg_temp.check_global_filter(channel text,filter_case int) RETURNS void
LANGUAGE plpgsql AS $filters$
DECLARE
 s public.banking_pay_workbench_sessions%ROWTYPE;
 options jsonb; input jsonb; result jsonb; untouched jsonb; untouched_ids uuid[];
 current_count bigint; audits bigint; err text; original_ids uuid[];
BEGIN
   -- Subtransaction rollback gives every scenario the identical starting data.
   BEGIN
    UPDATE public.banking_pay_workbench_sessions SET filters_json=CASE filter_case
     WHEN 0 THEN '{}'::jsonb
     WHEN 1 THEN '{"candidate_id":"10000000-0000-4000-8000-000000000002"}'::jsonb
     WHEN 2 THEN '{"client_id":"60000000-0000-4000-8000-000000000001"}'::jsonb
     ELSE '{"candidate_id":"10000000-0000-4000-8000-000000000002","client_id":"60000000-0000-4000-8000-000000000002"}'::jsonb END,
     session_signature='global filter fixture '||channel||':'||filter_case
    WHERE id='10000000-0000-4000-8000-000000000005';
    SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
    SELECT array_agg(row_id),count(*) INTO original_ids,current_count FROM private.pay_workbench_modal_ready_members_v2(s,channel);
    IF current_count<1 THEN RAISE EXCEPTION 'GLOBAL_FILTER_FIXTURE_HAS_NO_TARGET'; END IF;
    -- Untouched non-target payments, Cases, hidden snoozes and ineligible rows.
    -- Canonical recovery movement is independently permitted and tested below.
    SELECT array_agg(r.id),jsonb_agg(to_jsonb(r) ORDER BY r.id) INTO untouched_ids,untouched
    FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=s.id
     AND NOT (r.id=ANY(original_ids)) AND row_key LIKE 'selection-fixture:%';
    options:=jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',4,'pay_channel_scope',channel,
      'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,channel));
    input:=jsonb_build_object('modal_global_intent_v2',jsonb_build_object('request_id','70000000-0000-4000-8000-000000000001',
      'action','SELECT_ALL_READY','options',options));
    SELECT count(*) INTO audits FROM public.audit_events WHERE object_id_text=s.id::text;
    result:=public.pay_workbench_session_set_selected_rows(s.id,input,s.actor_user_id);
    SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
    IF s.progress_counter_version<>5 OR EXISTS(SELECT 1 FROM private.pay_workbench_modal_ready_members_v2(s,channel) WHERE NOT selected)
      OR untouched IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id) FROM public.banking_pay_workbench_preview_rows r WHERE r.id=ANY(untouched_ids))
      THEN RAISE EXCEPTION 'GLOBAL_FILTER_SELECT_SCOPE_CHANGED: %:%',channel,filter_case; END IF;
    -- Full-session identity bookkeeping deliberately remains unfiltered: it is
    -- the old Draft owner, not a replacement calculated from visible candidates.
    IF s.selected_row_count IS DISTINCT FROM (SELECT count(*) FROM private.pay_workbench_modal_selection_rows_v2(s.id,s.version) x
      JOIN public.banking_pay_workbench_preview_rows r ON r.id=x.id WHERE x.is_selectable AND r.selected AND r.selection_state='SELECTED')
      THEN RAISE EXCEPTION 'GLOBAL_FILTER_DRAFT_SELECTION_DRIFT'; END IF;
    result:=public.pay_workbench_session_set_selected_rows(s.id,
      jsonb_set(jsonb_set(jsonb_set(input,'{modal_global_intent_v2,action}','"CLEAR_ALL_READY"'),
       '{modal_global_intent_v2,request_id}','"70000000-0000-4000-8000-000000000002"'),
       '{modal_global_intent_v2,options,expected_progress_counter_version}','5'),s.actor_user_id);
    SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
    IF s.progress_counter_version<>6 OR EXISTS(SELECT 1 FROM private.pay_workbench_modal_ready_members_v2(s,channel) WHERE selected)
      OR untouched IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id) FROM public.banking_pay_workbench_preview_rows r WHERE r.id=ANY(untouched_ids))
      OR (SELECT count(*) FROM public.audit_events WHERE object_id_text=s.id::text)<>audits+2
      THEN RAISE EXCEPTION 'GLOBAL_FILTER_CLEAR_SCOPE_CHANGED: %:%',channel,filter_case; END IF;
    RAISE EXCEPTION 'EXPECTED_SCENARIO_ROLLBACK';
   EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS err=MESSAGE_TEXT;
    IF err<>'EXPECTED_SCENARIO_ROLLBACK' THEN RAISE; END IF;
   END;
   INSERT INTO pg_temp.global_filter_checks VALUES(channel,filter_case);
END $filters$;
SELECT format('SELECT pg_temp.check_global_filter(%L,%s);',channel,filter_case)
FROM unnest(ARRAY['ALL','PAYE','UMBRELLA']) channel CROSS JOIN generate_series(0,3) filter_case
\gexec

DO $remaining_filters$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE; untouched jsonb; input jsonb; result jsonb; before_session jsonb;
BEGIN
 IF (SELECT count(*) FROM pg_temp.global_filter_checks)<>12 THEN RAISE EXCEPTION 'GLOBAL_FILTER_CASE_COUNT'; END IF;

 -- Updating candidates must not be targeted or revalidated by the header.
 UPDATE public.banking_pay_workbench_session_scope SET dirty=true,status='SOURCE_BUILD_PENDING'
 WHERE session_id='10000000-0000-4000-8000-000000000005' AND candidate_id='10000000-0000-4000-8000-000000000003';
 SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO untouched FROM public.banking_pay_workbench_preview_rows r
 WHERE session_id=s.id AND candidate_id='10000000-0000-4000-8000-000000000003';
 input:=jsonb_build_object('modal_global_intent_v2',jsonb_build_object('request_id','70000000-0000-4000-8000-000000000003',
  'action','SELECT_ALL_READY','options',jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',4,
   'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'))));
 result:=public.pay_workbench_session_set_selected_rows(s.id,input,s.actor_user_id);
 IF result->>'recovery_revalidation_count'<>'1' OR untouched IS DISTINCT FROM
  (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r
   WHERE session_id=s.id AND candidate_id='10000000-0000-4000-8000-000000000003') THEN RAISE EXCEPTION 'GLOBAL_CHANGED_UPDATING_CANDIDATE'; END IF;

 -- A freshly verified empty scope is a true no-op; stale input is still rejected.
 UPDATE public.banking_pay_workbench_sessions SET filters_json='{"candidate_id":"70000000-0000-4000-8000-000000000099"}',
  session_signature='global empty scope fixture' WHERE id=s.id;
 SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
 before_session:=to_jsonb(s);
 input:=jsonb_build_object('modal_global_intent_v2',jsonb_build_object('request_id','70000000-0000-4000-8000-000000000004',
  'action','CLEAR_ALL_READY','options',jsonb_build_object('expected_session_version',1,'expected_progress_counter_version',s.progress_counter_version,
   'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'))));
 result:=public.pay_workbench_session_set_selected_rows(s.id,input,s.actor_user_id);
 IF result->>'state_changed' IS DISTINCT FROM 'false' OR result->>'recovery_revalidation_count'<>'0'
  OR before_session IS DISTINCT FROM (SELECT to_jsonb(x) FROM public.banking_pay_workbench_sessions x WHERE id=s.id)
  THEN RAISE EXCEPTION 'GLOBAL_EMPTY_SCOPE_WROTE'; END IF;
END $remaining_filters$;
SELECT 'GLOBAL_FILTER_MATRIX_PASS';
ROLLBACK;
