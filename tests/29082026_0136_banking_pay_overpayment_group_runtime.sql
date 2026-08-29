\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql

INSERT INTO public.pay_advances(id,candidate_id,reason,original_amount,outstanding_amount,schedule_json,status,advance_kind,case_type)
VALUES('10000000-0000-4000-8000-000000009200','10000000-0000-4000-8000-000000000002',
  'OVERPAYMENT',10,10,'[]','ACTIVE','OVERPAYMENT','OVERPAYMENT');
UPDATE public.banking_pay_workbench_session_scope
SET status='READY',seeded=true,dirty=false
WHERE session_id='10000000-0000-4000-8000-000000000005' AND candidate_id='10000000-0000-4000-8000-000000000002';
UPDATE public.banking_pay_workbench_preview_rows
SET row_json=row_json||jsonb_build_object(
  'line_type','OVERPAYMENT_RECOVERY','finance_case_id','10000000-0000-4000-8000-000000009200',
  'recovery_residual_contract_version',1,'recovery_source_outstanding_ex_vat',10,
  'recovery_active_reserved_ex_vat',0,'recovery_residual_outstanding_ex_vat',10,
  'nominal_due_amount_ex_vat',10,'case_components',jsonb_build_array(jsonb_build_object(
    'finance_component_id','10000000-0000-4000-8000-000000009202','target_pay_ex_vat','10.00')))
WHERE session_id='10000000-0000-4000-8000-000000000005'
  AND candidate_id='10000000-0000-4000-8000-000000000002' AND row_ordinal=1;

DO $proof$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;before_view jsonb;reply jsonb;v_key text;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 before_view:=private.pay_workbench_modal_candidate_state_v2(s,'ALL','10000000-0000-4000-8000-000000000002');
 v_key:='finance:10000000-0000-4000-8000-000000009200:overpayment_recovery';
 reply:=public.pay_workbench_session_set_ready_group_v1(s.id,'10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,
   'OVERPAYMENT',v_key,false,'10000000-0000-4000-8000-000000009201',before_view->>'view_digest',NULL);
 IF reply->>'selection_scope'<>'COMPLETE_READY_GROUP' OR reply->>'group_member_count'<>'1'
   OR reply->>'owner_call_count'<>'1' OR reply->>'group_kind'<>'OVERPAYMENT' THEN
   RAISE EXCEPTION 'OVERPAYMENT_GROUP_RESPONSE_INVALID %',reply;
 END IF;
 IF EXISTS(SELECT 1 FROM public.banking_pay_workbench_preview_rows r
   WHERE r.session_id=s.id AND r.row_ordinal=1
     AND (r.selected IS DISTINCT FROM false OR r.row_json->>'selection_user_override'<>'UNSELECTED')) THEN
   RAISE EXCEPTION 'OVERPAYMENT_GROUP_DID_NOT_SETTLE';
 END IF;
 IF (SELECT count(*) FROM public.banking_pay_workbench_preview_rows r
   WHERE r.session_id=s.id AND r.candidate_id='10000000-0000-4000-8000-000000000002'
     AND r.row_ordinal BETWEEN 2 AND 107 AND r.selected IS TRUE)<>106 THEN
   RAISE EXCEPTION 'OVERPAYMENT_GROUP_CHANGED_POSITIVE_PAYMENTS';
 END IF;
 RAISE NOTICE 'PASS: real overpayment group settles atomically without changing positive payments.';
END;
$proof$;
ROLLBACK;
