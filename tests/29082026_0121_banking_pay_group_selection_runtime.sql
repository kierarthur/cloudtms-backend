\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
INSERT INTO public.timesheets(timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date)
VALUES('10000000-0000-4000-8000-000000000010','rollback-group-selection','rollback candidate','rollback hospital','rollback ward','rollback role','2026-08-23');
UPDATE public.banking_pay_workbench_preview_rows SET timesheet_id='10000000-0000-4000-8000-000000000010',
 row_json=row_json||jsonb_build_object('timesheet_id','10000000-0000-4000-8000-000000000010')
WHERE session_id='10000000-0000-4000-8000-000000000005' AND candidate_id='10000000-0000-4000-8000-000000000002';
DO $proof$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;before_view jsonb;reply jsonb;
 before_audits bigint;after_audits bigint;before_snapshot text;after_snapshot text;v_error text;v_progress bigint;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 before_view:=private.pay_workbench_modal_candidate_state_v2(s,'ALL','10000000-0000-4000-8000-000000000002');
 SELECT count(*) INTO before_audits FROM public.audit_events WHERE object_id_text=s.id::text;
 reply:=public.pay_workbench_session_set_ready_group_v1(s.id,'10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,
   'TIMESHEET','READY_TO_PAY|10000000-0000-4000-8000-000000000002|10000000-0000-4000-8000-000000000010',false,
   '10000000-0000-4000-8000-000000009100',before_view->>'view_digest',NULL);
 SELECT count(*) INTO after_audits FROM public.audit_events WHERE object_id_text=s.id::text;
 IF reply->>'selection_scope'<>'COMPLETE_READY_GROUP' OR reply->>'group_member_count'<>'107' OR reply->>'owner_call_count'<>'1'
   OR reply->>'group_kind'<>'TIMESHEET' OR reply#>>'{global,selected_ready_count}'<>'2'
   OR reply#>>'{global,selected_ready_display_amount}'<>'20.00'
   OR (reply->>'progress_counter_version')::bigint<=s.progress_counter_version OR after_audits-before_audits<>1 THEN
   RAISE EXCEPTION 'GROUP_SELECTION_RESPONSE_INVALID % audit_delta %',reply,after_audits-before_audits;
 END IF;
 IF EXISTS(SELECT 1 FROM public.banking_pay_workbench_preview_rows WHERE session_id=s.id
   AND candidate_id='10000000-0000-4000-8000-000000000002' AND section='canonical_preview_lines' AND selected) THEN
   RAISE EXCEPTION 'GROUP_SELECTION_LEFT_MEMBER_SELECTED';END IF;
 IF (SELECT count(*) FROM public.banking_pay_workbench_preview_rows WHERE session_id=s.id
   AND candidate_id='10000000-0000-4000-8000-000000000003' AND section='canonical_preview_lines' AND selected)<>2 THEN
   RAISE EXCEPTION 'GROUP_SELECTION_CHANGED_OTHER_CANDIDATE';END IF;
 SELECT md5(jsonb_agg(to_jsonb(r)ORDER BY r.id)::text)INTO before_snapshot FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 SELECT progress_counter_version INTO v_progress FROM public.banking_pay_workbench_sessions WHERE id=s.id;
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
 opts:=opts||jsonb_build_object('expected_progress_counter_version',s.progress_counter_version);
 before_view:=private.pay_workbench_modal_candidate_state_v2(s,'ALL','10000000-0000-4000-8000-000000000002');
 BEGIN
   PERFORM public.pay_workbench_session_set_ready_group_v1(s.id,'10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,
    'TIMESHEET','missing-group',true,'10000000-0000-4000-8000-000000009101',before_view->>'view_digest',NULL);
   RAISE EXCEPTION 'MISSING_GROUP_ACCEPTED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
   IF v_error<>'BANKING_PAY_V2_GROUP_NOT_SELECTABLE' THEN RAISE;END IF;
 END;
 SELECT md5(jsonb_agg(to_jsonb(r)ORDER BY r.id)::text)INTO after_snapshot FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 IF before_snapshot<>after_snapshot OR (SELECT progress_counter_version FROM public.banking_pay_workbench_sessions WHERE id=s.id)<>v_progress THEN
   RAISE EXCEPTION 'MISSING_GROUP_CHANGED_STATE';END IF;
 RAISE NOTICE 'PASS: complete107-row group uses one set-wise original selection-owner call; other candidate and missing-group state preserved.';
END;
$proof$;
ROLLBACK;
