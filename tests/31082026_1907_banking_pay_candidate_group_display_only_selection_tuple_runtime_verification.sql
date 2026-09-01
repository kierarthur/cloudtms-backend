-- Rollback-contained first-use proof for the current-payable Candidate Banking
-- contract. A historical parent scalar cannot replace its current eligible
-- child amount, a fully paid Timesheet group is absent, and the Draft input
-- identity/amount set is unchanged by either read.
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
values
 ('10000000-0000-4000-8000-000000006001','current-payable-group','current payable candidate',
  'current payable hospital','current payable ward','current payable role','2026-08-30'),
 ('10000000-0000-4000-8000-000000006002','fully-paid-context','current payable candidate',
  'historical hospital','historical ward','historical role','2026-08-23');

update public.banking_pay_workbench_preview_rows r
set timesheet_id=case when r.row_ordinal<=2 then '10000000-0000-4000-8000-000000006001'::uuid
      else '10000000-0000-4000-8000-000000006002'::uuid end,
    row_key=case when r.row_ordinal=2 then 'current-payable-component:'||r.id::text
      when r.row_ordinal=1 then 'timesheet_snapshot:current-payable:'||r.id::text
      else 'timesheet_snapshot:fully-paid:'||r.id::text end,
    selected=r.row_ordinal=2,
    selection_state=case when r.row_ordinal=2 then 'SELECTED' else 'NOT_SELECTABLE' end,
    row_json=r.row_json||jsonb_build_object(
      'timesheet_id',case when r.row_ordinal<=2 then '10000000-0000-4000-8000-000000006001'::uuid
        else '10000000-0000-4000-8000-000000006002'::uuid end,
      'line_type','TIMESHEET_PAYMENT',
      'presentation_role',case when r.row_ordinal=2 then 'ALLOCATION_COMPONENT' else 'PARENT' end,
      'selection_allowed',r.row_ordinal=2,'draftable',r.row_ordinal=2,'is_ready_for_draft',r.row_ordinal=2,
      'amount_display',case when r.row_ordinal=1 then '4.00' when r.row_ordinal=2 then '1.00' else '17.39' end,
      'section_amount_display',case when r.row_ordinal=1 then '4.00' when r.row_ordinal=2 then '1.00' else '17.39' end,
      'amount_ex_vat',case when r.row_ordinal=1 then '4.00' when r.row_ordinal=2 then '1.00' else '17.39' end)
where r.session_id='10000000-0000-4000-8000-000000000005'
  and r.candidate_id='10000000-0000-4000-8000-000000000002';

do $proof$
declare
  s public.banking_pay_workbench_sessions%rowtype;opts jsonb;outer_reply jsonb;detail_reply jsonb;
  payable_key text;historical_key text;before_rows text;after_rows text;
  draft_before text;draft_after text;draft_amount_before numeric;draft_amount_after numeric;
begin
  select * into strict s from public.banking_pay_workbench_sessions
  where id='10000000-0000-4000-8000-000000000005';
  opts:=jsonb_build_object('expected_session_version',s.version,
    'expected_progress_counter_version',s.progress_counter_version,
    'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
  select md5(jsonb_agg(to_jsonb(r) order by r.id)::text) into before_rows
  from public.banking_pay_workbench_preview_rows r where r.session_id=s.id;
  select md5(jsonb_agg(jsonb_build_array(r.id,r.row_json->>'amount_ex_vat') order by r.id)::text),
    sum((r.row_json->>'amount_ex_vat')::numeric)
  into draft_before,draft_amount_before
  from private.pay_workbench_modal_eligible_rows_v2(s.id,s.version,'canonical_preview_lines') r
  where r.candidate_id='10000000-0000-4000-8000-000000000002' and r.selected and r.status='READY'
    and coalesce((r.row_json->>'selection_allowed')::boolean,false)
    and coalesce((r.row_json->>'draftable')::boolean,false)
    and coalesce((r.row_json->>'is_ready_for_draft')::boolean,false);

  payable_key:='READY_TO_PAY|10000000-0000-4000-8000-000000000002|10000000-0000-4000-8000-000000006001';
  historical_key:='READY_TO_PAY|10000000-0000-4000-8000-000000000002|10000000-0000-4000-8000-000000006002';
  outer_reply:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,
    '10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,null,10);
  if outer_reply->>'total_count'<>'1' or jsonb_array_length(outer_reply->'rows')<>1
     or outer_reply#>>'{rows,0,presentation_group_key}'<>payable_key
     or outer_reply#>>'{rows,0,selection_group_member_count}'<>'1'
     or outer_reply#>>'{rows,0,selection_group_selected_count}'<>'1'
     or outer_reply#>>'{rows,0,selection_group_state}'<>'ALL'
     or outer_reply#>>'{rows,0,selection_group_display_amount}'<>'1.00'
     or outer_reply#>>'{rows,0,selection_group_selected_display_amount}'<>'1.00'
     or outer_reply#>>'{rows,0,amount_display}'<>'4.00' then
    raise exception 'BANKING_PAY_CURRENT_PAYABLE_GROUPS: outer authority incorrect %',outer_reply;
  end if;

  detail_reply:=public.pay_workbench_session_get_candidate_ready_group_page_v1(s.id,
    '10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,'TIMESHEET',payable_key,null,10);
  if detail_reply->>'total_count'<>'1' or jsonb_array_length(detail_reply->'rows')<>1
     or detail_reply#>>'{rows,0,amount_display}'<>'1.00'
     or detail_reply#>>'{rows,0,selection_group_display_amount}'<>'1.00'
     or detail_reply#>>'{rows,0,selection_group_state}'<>'ALL' then
    raise exception 'BANKING_PAY_CURRENT_PAYABLE_GROUPS: eligible breakdown incorrect %',detail_reply;
  end if;

  begin
    perform public.pay_workbench_session_get_candidate_ready_group_page_v1(s.id,
      '10000000-0000-4000-8000-000000000002',opts,s.actor_user_id,'TIMESHEET',historical_key,null,10);
    raise exception 'BANKING_PAY_CURRENT_PAYABLE_GROUPS: fully paid context remained expandable';
  exception when sqlstate 'P0001' then
    if sqlerrm<>'BANKING_PAY_V2_ITEM_NOT_CURRENT' then raise; end if;
  end;

  select md5(jsonb_agg(to_jsonb(r) order by r.id)::text) into after_rows
  from public.banking_pay_workbench_preview_rows r where r.session_id=s.id;
  select md5(jsonb_agg(jsonb_build_array(r.id,r.row_json->>'amount_ex_vat') order by r.id)::text),
    sum((r.row_json->>'amount_ex_vat')::numeric)
  into draft_after,draft_amount_after
  from private.pay_workbench_modal_eligible_rows_v2(s.id,s.version,'canonical_preview_lines') r
  where r.candidate_id='10000000-0000-4000-8000-000000000002' and r.selected and r.status='READY'
    and coalesce((r.row_json->>'selection_allowed')::boolean,false)
    and coalesce((r.row_json->>'draftable')::boolean,false)
    and coalesce((r.row_json->>'is_ready_for_draft')::boolean,false);
  if before_rows is distinct from after_rows or draft_before is distinct from draft_after
     or draft_amount_before is distinct from draft_amount_after or draft_amount_after<>1.00
     or (select progress_counter_version from public.banking_pay_workbench_sessions where id=s.id)<>s.progress_counter_version then
    raise exception 'BANKING_PAY_CURRENT_PAYABLE_GROUPS: read changed Workbench or Draft authority';
  end if;
end;
$proof$;
rollback;
