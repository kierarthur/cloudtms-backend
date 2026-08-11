-- Current/History membership, strict boundaries, ordering, cursor v2 and detail identity.
begin;

update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||jsonb_build_object('candidate_app_reads',true,'candidate_record_role_capabilities',true)
where id=1;

do $current_history$
declare
  v_now timestamptz:='2026-08-11 12:00:00+00';
  v_current date:='2026-08-11';
  v_client uuid:='ac150000-0000-0000-0000-000000000001';
  v_candidate uuid:='ac150000-0000-0000-0000-000000000002';
  v_contract uuid:='ac150000-0000-0000-0000-000000000003';
  v_account uuid:='ac150000-0000-0000-0000-000000000004';
  v_session uuid:='ac150000-0000-0000-0000-000000000005';
  v_current_page jsonb;
  v_history_page jsonb;
  v_detail jsonb;
  v_cursor text;
begin
  insert into public.clients(id,name) values(v_client,'Current History Client');
  insert into public.candidates(id,email,active,key_norm)
  values(v_candidate,'current-history@example.test',true,'CURRENT-HISTORY-CANDIDATE');
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday
  ) values(gen_random_uuid(),v_client,v_current-600,'ELECTRONIC',2);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode
  ) values(v_contract,v_candidate,v_client,v_current-600,v_current+30,2,'ELECTRONIC');

  insert into public.timesheets(
    timesheet_id,contract_id,week_ending_date,line_type,submission_mode
  ) values
    ('ac150000-0000-0000-0000-000000000101',v_contract,v_current,'HOURS','MANUAL'),
    ('ac150000-0000-0000-0000-000000000102',v_contract,v_current-7,'HOURS','MANUAL'),
    ('ac150000-0000-0000-0000-000000000103',v_contract,v_current-105,'HOURS','MANUAL'),
    ('ac150000-0000-0000-0000-000000000104',v_contract,v_current-112,'HOURS','MANUAL'),
    ('ac150000-0000-0000-0000-000000000105',v_contract,v_current-490,'HOURS','MANUAL'),
    ('ac150000-0000-0000-0000-000000000106',v_contract,v_current+7,'HOURS','MANUAL');
  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,timesheet_id
  ) values
    ('ac150000-0000-0000-0000-000000000201',v_contract,v_current,0,'OPEN','ELECTRONIC','ac150000-0000-0000-0000-000000000101'),
    ('ac150000-0000-0000-0000-000000000202',v_contract,v_current-7,0,'OPEN','ELECTRONIC','ac150000-0000-0000-0000-000000000102'),
    ('ac150000-0000-0000-0000-000000000203',v_contract,v_current-105,0,'OPEN','ELECTRONIC','ac150000-0000-0000-0000-000000000103'),
    ('ac150000-0000-0000-0000-000000000204',v_contract,v_current-112,0,'OPEN','ELECTRONIC','ac150000-0000-0000-0000-000000000104'),
    ('ac150000-0000-0000-0000-000000000205',v_contract,v_current-490,0,'OPEN','ELECTRONIC','ac150000-0000-0000-0000-000000000105'),
    ('ac150000-0000-0000-0000-000000000206',v_contract,v_current+7,0,'PLANNED','ELECTRONIC','ac150000-0000-0000-0000-000000000106');
  insert into public.timesheets_financials(
    timesheet_id,candidate_id,client_id,total_hours,processing_status,paid_at_utc
  ) values
    ('ac150000-0000-0000-0000-000000000101',v_candidate,v_client,8,'READY_FOR_INVOICE',v_now-interval '7 days'),
    ('ac150000-0000-0000-0000-000000000102',v_candidate,v_client,8,'READY_FOR_INVOICE',v_now-interval '7 days 0.000001 seconds'),
    ('ac150000-0000-0000-0000-000000000103',v_candidate,v_client,8,'READY_FOR_INVOICE',v_now-interval '8 days'),
    ('ac150000-0000-0000-0000-000000000104',v_candidate,v_client,8,'READY_FOR_INVOICE',v_now-interval '8 days'),
    ('ac150000-0000-0000-0000-000000000105',v_candidate,v_client,8,'UNPROCESSED',null),
    ('ac150000-0000-0000-0000-000000000106',v_candidate,v_client,0,'UNPROCESSED',null);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
  values(v_account,'TEST','current-history@example.test','ACTIVE');
  insert into public.candidate_app_sessions(
    id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
    expires_at_utc,absolute_expires_at_utc
  ) values(v_session,v_account,'TEST',v_candidate,'ACTIVE',decode(repeat('15',32),'hex'),
    v_now+interval '30 days',v_now+interval '90 days');

  v_current_page:=public.candidate_app_timesheet_page_v1(
    v_session,'TEST','CURRENT',null,50,v_now
  );
  if v_current_page->>'view'<>'CURRENT'
     or v_current_page->>'cursor_version'<>'v2'
     or jsonb_array_length(v_current_page->'items')<>2
     or v_current_page#>>'{items,0,contract_week_id}'<>'ac150000-0000-0000-0000-000000000201'
     or v_current_page#>>'{items,1,contract_week_id}'<>'ac150000-0000-0000-0000-000000000205'
     or v_current_page#>>'{items,0,week_ending_label}'<>'Week Ending 11 August 2026'
     or v_current_page#>>'{items,0,tab_bucket}'<>'CURRENT'
     or v_current_page#>>'{items,0,detail_target,identity_kind}'<>'TIMESHEET'
     or jsonb_typeof(v_current_page#>'{items,0,primary_action}') not in ('object','null') then
    raise exception 'Current membership/order/card contract incorrect: %',v_current_page;
  end if;

  v_history_page:=public.candidate_app_timesheet_page_v1(
    v_session,'TEST','HISTORY',null,1,v_now
  );
  v_cursor:=v_history_page->>'next_cursor';
  if v_history_page->>'view'<>'HISTORY'
     or jsonb_array_length(v_history_page->'items')<>1
     or v_history_page#>>'{items,0,contract_week_id}'<>'ac150000-0000-0000-0000-000000000202'
     or v_history_page#>>'{items,0,tab_bucket}'<>'HISTORY'
     or v_cursor not like 'v2|HISTORY|%' then
    raise exception 'History first page or cursor incorrect: %',v_history_page;
  end if;
  v_history_page:=public.candidate_app_timesheet_page_v1(
    v_session,'TEST','HISTORY',v_cursor,50,v_now
  );
  if jsonb_array_length(v_history_page->'items')<>1
     or v_history_page#>>'{items,0,contract_week_id}'<>'ac150000-0000-0000-0000-000000000203'
     or v_history_page->>'next_cursor' is not null then
    raise exception 'History continuation or 16-week boundary incorrect: %',v_history_page;
  end if;

  if exists(
    select 1
    from jsonb_array_elements(v_current_page->'items') current_item
    join jsonb_array_elements(
      public.candidate_app_timesheet_page_v1(v_session,'TEST','HISTORY',null,50,v_now)->'items'
    ) history_item
      on history_item->>'contract_week_id'=current_item->>'contract_week_id'
  ) then raise exception 'Current and History overlapped'; end if;
  begin
    perform public.candidate_app_timesheet_page_v1(
      v_session,'TEST','CURRENT',v_cursor,50,v_now
    );
    raise exception 'History cursor was accepted by Current';
  exception when sqlstate '22023' then
    if sqlerrm<>'CANDIDATE_CURSOR_VIEW_MISMATCH' then raise; end if;
  end;

  v_detail:=public.candidate_app_timesheet_detail_v1(
    v_session,'TEST',null,'ac150000-0000-0000-0000-000000000202',null,v_now
  );
  if v_detail->>'week_ending_label'<>'Week Ending 4 August 2026'
     or v_detail#>>'{list_membership,tab_bucket}'<>'HISTORY'
     or nullif(v_detail->>'candidate_status_code','') is null
     or jsonb_typeof(v_detail->'available_actions')<>'array' then
    raise exception 'Detail list/action contract incorrect: %',v_detail;
  end if;
end;
$current_history$;

rollback;
