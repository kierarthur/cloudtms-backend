-- Rollback-contained real first-use proof: 40 Ready rows form 12 complete
-- payment groups (10 + 2), while a 28-row group expands as 10 + 10 + 8.
\set ON_ERROR_STOP on
begin;
set local statement_timeout='45s';
set local client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql

delete from public.banking_pay_workbench_preview_rows
where session_id='10000000-0000-4000-8000-000000000005'
  and candidate_id='10000000-0000-4000-8000-000000000002' and row_ordinal>40;

insert into public.timesheets(timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date)
select ('10000000-0000-4000-8000-'||lpad((5000+n)::text,12,'0'))::uuid,
  'group-page-'||n,'group page candidate','group page hospital','group page ward','group page role','2026-08-30'::date
from generate_series(1,12) n;

update public.banking_pay_workbench_preview_rows r
set timesheet_id=('10000000-0000-4000-8000-'||lpad((5000+case
      when r.row_ordinal<=28 then 1 when r.row_ordinal<=30 then 2 else r.row_ordinal-28 end)::text,12,'0'))::uuid,
    row_json=r.row_json||jsonb_build_object('timesheet_id',
      ('10000000-0000-4000-8000-'||lpad((5000+case
        when r.row_ordinal<=28 then 1 when r.row_ordinal<=30 then 2 else r.row_ordinal-28 end)::text,12,'0'))::uuid)
where r.session_id='10000000-0000-4000-8000-000000000005'
  and r.candidate_id='10000000-0000-4000-8000-000000000002';

do $proof$
declare
  s public.banking_pay_workbench_sessions%rowtype;opts jsonb;main_one jsonb;main_two jsonb;
  detail_one jsonb;detail_two jsonb;detail_three jsonb;group_key text;
  before_rows text;after_rows text;
begin
  select * into strict s from public.banking_pay_workbench_sessions
  where id='10000000-0000-4000-8000-000000000005';
  opts:=jsonb_build_object('expected_session_version',s.version,
    'expected_progress_counter_version',s.progress_counter_version,
    'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
  select md5(jsonb_agg(to_jsonb(r) order by r.id)::text) into before_rows
  from public.banking_pay_workbench_preview_rows r where r.session_id=s.id;

  main_one:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,
    '10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,null,10);
  main_two:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,
    '10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,main_one->>'next_cursor',10);
  if main_one->>'total_count'<>'12' or main_one->>'ready_row_count'<>'40'
     or jsonb_array_length(main_one->'rows')<>10 or main_one->>'has_more'<>'true'
     or main_one->>'page_number'<>'1' or main_two->>'total_count'<>'12'
     or main_two->>'ready_row_count'<>'40' or jsonb_array_length(main_two->'rows')<>2
     or main_two->>'has_more'<>'false' or main_two->>'page_number'<>'2' then
    raise exception 'BANKING_PAY_GROUP_PAGE_VERIFY: main 10/2 group paging failed % %',main_one,main_two;
  end if;
  if (select count(*) from jsonb_array_elements((main_one->'rows')||(main_two->'rows')) r
      where r->>'presentation_group_kind'<>'TIMESHEET')<>0
     or (select count(distinct r->>'presentation_group_key')
       from jsonb_array_elements((main_one->'rows')||(main_two->'rows')) r)<>12
     or (select sum((r->>'presentation_group_row_count')::integer)
       from jsonb_array_elements((main_one->'rows')||(main_two->'rows')) r)<>40 then
    raise exception 'BANKING_PAY_GROUP_PAGE_VERIFY: a group was split, duplicated or omitted';
  end if;

  group_key:=(main_one#>>'{rows,0,presentation_group_key}');
  if main_one#>>'{rows,0,presentation_group_row_count}'<>'28' then
    raise exception 'BANKING_PAY_GROUP_PAGE_VERIFY: large group summary was not retained';
  end if;
  detail_one:=public.pay_workbench_session_get_candidate_ready_group_page_v1(s.id,
    '10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,'TIMESHEET',group_key,null,10);
  detail_two:=public.pay_workbench_session_get_candidate_ready_group_page_v1(s.id,
    '10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,'TIMESHEET',group_key,detail_one->>'next_cursor',10);
  detail_three:=public.pay_workbench_session_get_candidate_ready_group_page_v1(s.id,
    '10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,'TIMESHEET',group_key,detail_two->>'next_cursor',10);
  if detail_one->>'total_count'<>'28' or detail_one->>'page_offset'<>'0'
     or jsonb_array_length(detail_one->'rows')<>10 or detail_one->>'has_more'<>'true'
     or detail_two->>'page_offset'<>'10' or jsonb_array_length(detail_two->'rows')<>10 or detail_two->>'has_more'<>'true'
     or detail_three->>'page_offset'<>'20' or jsonb_array_length(detail_three->'rows')<>8 or detail_three->>'has_more'<>'false'
     or detail_three->>'next_cursor' is not null then
    raise exception 'BANKING_PAY_GROUP_PAGE_VERIFY: detail 10/10/8 paging failed % % %',detail_one,detail_two,detail_three;
  end if;
  if exists(select 1 from jsonb_array_elements((detail_one->'rows')||(detail_two->'rows')||(detail_three->'rows')) r
    where r->>'presentation_group_kind'<>'TIMESHEET' or r->>'presentation_group_key'<>group_key
      or r->>'selection_group_member_count'<>'28') then
    raise exception 'BANKING_PAY_GROUP_PAGE_VERIFY: detail escaped its complete authoritative group';
  end if;
  select md5(jsonb_agg(to_jsonb(r) order by r.id)::text) into after_rows
  from public.banking_pay_workbench_preview_rows r where r.session_id=s.id;
  if before_rows is distinct from after_rows
     or (select progress_counter_version from public.banking_pay_workbench_sessions where id=s.id)<>s.progress_counter_version then
    raise exception 'BANKING_PAY_GROUP_PAGE_VERIFY: read-only paging changed Workbench state';
  end if;
  raise notice 'PASS: 40 Ready rows page as 12 complete groups (10/2); 28-row group expands 10/10/8; no writes.';
end;
$proof$;
rollback;
