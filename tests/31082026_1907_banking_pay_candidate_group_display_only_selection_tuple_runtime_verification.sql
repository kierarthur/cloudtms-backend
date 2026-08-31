-- Rollback-contained real first-use proof. A complete visible Timesheet group
-- with no selectable owner must remain readable while carrying the existing
-- null/zero selection tuple expected by the Worker.
\set ON_ERROR_STOP on
begin;
set local statement_timeout='45s';
set local client_min_messages='warning';
set local cloudtms.rollback_fixture_scope='BANKING_PAY_CANDIDATE_GROUP_PAGINATION_V2';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql

delete from public.banking_pay_workbench_preview_rows
where session_id='10000000-0000-4000-8000-000000000005'
  and candidate_id='10000000-0000-4000-8000-000000000002' and row_ordinal>3;

insert into public.timesheets(timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date)
values('10000000-0000-4000-8000-000000006001','display-only-group','display only candidate',
  'display only hospital','display only ward','display only role','2026-08-30');

update public.banking_pay_workbench_preview_rows r
set timesheet_id='10000000-0000-4000-8000-000000006001',
    row_key='timesheet_snapshot:display-only:'||r.id::text,
    row_json=r.row_json||jsonb_build_object(
      'timesheet_id','10000000-0000-4000-8000-000000006001',
      'line_type','TIMESHEET_PAYMENT','presentation_role','PARENT',
      'selection_allowed',false,'draftable',false,'is_ready_for_draft',false)
where r.session_id='10000000-0000-4000-8000-000000000005'
  and r.candidate_id='10000000-0000-4000-8000-000000000002';

do $proof$
declare
  s public.banking_pay_workbench_sessions%rowtype;opts jsonb;reply jsonb;group_key text;
  before_rows text;after_rows text;
begin
  select * into strict s from public.banking_pay_workbench_sessions
  where id='10000000-0000-4000-8000-000000000005';
  opts:=jsonb_build_object('expected_session_version',s.version,
    'expected_progress_counter_version',s.progress_counter_version,
    'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
  select md5(jsonb_agg(to_jsonb(r) order by r.id)::text) into before_rows
  from public.banking_pay_workbench_preview_rows r where r.session_id=s.id;
  select private.pay_workbench_modal_ready_group_key_v2(private.pay_workbench_modal_row_payload_v2(r),'TIMESHEET')
  into strict group_key from public.banking_pay_workbench_preview_rows r
  where r.session_id=s.id order by r.row_ordinal limit 1;

  reply:=public.pay_workbench_session_get_candidate_ready_group_page_v1(s.id,
    '10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,'TIMESHEET',group_key,null,10);
  if reply->>'total_count'<>'3' or jsonb_array_length(reply->'rows')<>3
     or reply->>'page_offset'<>'0' or reply->>'has_more'<>'false' or reply->>'next_cursor' is not null then
    raise exception 'BANKING_PAY_DISPLAY_ONLY_SELECTION_TUPLE: bounded group read failed %',reply;
  end if;
  if exists(select 1 from jsonb_array_elements(reply->'rows') r
    where r->>'presentation_group_kind'<>'TIMESHEET' or r->>'presentation_group_key'<>group_key
      or r->'selection_group_kind'<>'null'::jsonb or r->'selection_group_key'<>'null'::jsonb
      or r->>'selection_group_member_count'<>'0' or r->>'selection_group_selected_count'<>'0'
      or r->'selection_group_display_amount'<>'null'::jsonb
      or r->'selection_group_selected_display_amount'<>'null'::jsonb
      or r->'selection_group_state'<>'null'::jsonb) then
    raise exception 'BANKING_PAY_DISPLAY_ONLY_SELECTION_TUPLE: contradictory selection metadata %',reply;
  end if;
  select md5(jsonb_agg(to_jsonb(r) order by r.id)::text) into after_rows
  from public.banking_pay_workbench_preview_rows r where r.session_id=s.id;
  if before_rows is distinct from after_rows
     or (select progress_counter_version from public.banking_pay_workbench_sessions where id=s.id)<>s.progress_counter_version then
    raise exception 'BANKING_PAY_DISPLAY_ONLY_SELECTION_TUPLE: read changed Workbench state';
  end if;
end;
$proof$;
rollback;
