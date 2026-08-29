\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
INSERT INTO public.timesheets(timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date)
VALUES('10000000-0000-4000-8000-000000000010','rollback-ready-group','rollback candidate','rollback hospital','rollback ward','rollback role','2026-08-23');
UPDATE public.banking_pay_workbench_preview_rows SET timesheet_id='10000000-0000-4000-8000-000000000010',
 row_json=row_json||jsonb_build_object('timesheet_id','10000000-0000-4000-8000-000000000010')
WHERE session_id='10000000-0000-4000-8000-000000000005' AND candidate_id='10000000-0000-4000-8000-000000000002';
DO $proof$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;page1 jsonb;page2 jsonb;
 expected_key text:='READY_TO_PAY|10000000-0000-4000-8000-000000000002|10000000-0000-4000-8000-000000000010';
 before_snapshot text;after_snapshot text;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 SELECT md5(jsonb_agg(to_jsonb(r)ORDER BY r.id)::text)INTO before_snapshot
 FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 page1:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,'10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,NULL,100);
 page2:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,'10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,page1->>'next_cursor',100);
 IF page1->>'total_count'<>'107' OR jsonb_array_length(page1->'rows')<>100 OR page1->>'has_more'<>'true'
   OR jsonb_array_length(page2->'rows')<>7 OR page2->>'has_more'<>'false' THEN
   RAISE EXCEPTION 'READY_GROUP_PAGING_INVALID % %',page1,page2;
 END IF;
 IF EXISTS(
   SELECT 1 FROM jsonb_array_elements((page1->'rows')||(page2->'rows')) row
   WHERE row->>'selection_group_kind'<>'TIMESHEET' OR row->>'selection_group_key'<>expected_key
     OR row->>'selection_group_member_count'<>'107' OR row->>'selection_group_selected_count'<>'107'
     OR row->>'selection_group_state'<>'ALL' OR row->>'selection_group_display_amount'<>'1070.00'
     OR row->>'selection_group_selected_display_amount'<>'1070.00'
 ) THEN RAISE EXCEPTION 'READY_GROUP_COMPLETE_FACTS_INVALID';END IF;
 SELECT md5(jsonb_agg(to_jsonb(r)ORDER BY r.id)::text)INTO after_snapshot
 FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
 IF before_snapshot<>after_snapshot OR (SELECT progress_counter_version FROM public.banking_pay_workbench_sessions WHERE id=s.id)<>s.progress_counter_version THEN
   RAISE EXCEPTION 'READY_GROUP_READ_CHANGED_STATE';END IF;
 RAISE NOTICE 'PASS: both physical Ready pages carry the same complete 107-member Timesheet group authority without writes.';
END;
$proof$;
ROLLBACK;
