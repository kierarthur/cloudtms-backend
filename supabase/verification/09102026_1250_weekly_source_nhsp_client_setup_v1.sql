-- Synthetic rollback proof: configure NHSP before contracts, retaining guards.
\set ON_ERROR_STOP on
begin isolation level repeatable read;
set local lock_timeout='5s';
set local statement_timeout='45s';
lock table public.app_change_counters in share row exclusive mode;
\ir support/06102026_1818_source_verifier_snapshot_guard.sql
\ir support/06102026_1117_source_workbench_fixture_isolation.sql
\ir support/06102026_1410_source_full_row_fingerprints.sql

do $verify$
declare
  actor uuid:='e9100000-0000-4000-8000-000000000001';
  agency uuid:='e9100000-0000-4000-8000-000000000002';
  client uuid:='e9100000-0000-4000-8000-000000000011';
  ordinary uuid:='e9100000-0000-4000-8000-000000000012';
  source_group uuid:='e9100000-0000-4000-8000-000000000021';
  req jsonb;
  shape jsonb;
  saved jsonb;
  original_version text;
  before_contracts text:=pg_temp.ws_verify_full_relation_fingerprint('public.contracts');
  before_timesheets text:=pg_temp.ws_verify_full_relation_fingerprint('public.timesheets');
  before_invoices text:=pg_temp.ws_verify_full_relation_fingerprint('public.invoices');
begin
  perform pg_temp.ws_verify_watch('public.weekly_source_group_clients');
  perform pg_temp.ws_verify_watch('public.weekly_source_client_policies');
  insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
    values(actor,'nhsp-setup-proof@example.invalid','admin','not-a-password','NHSP setup proof',true);
  insert into public.clients(id,name,ts_queries_email) values
    (client,'NHSP setup proof','manager@example.invalid'),(ordinary,'Ordinary setup proof',null);
  insert into public.client_settings(id,client_id,effective_from,is_nhsp,requires_hr,autoprocess_hr,no_timesheet_required)
    values('e9100000-0000-4000-8000-000000000031',client,current_date-1,true,false,false,false),
    ('e9100000-0000-4000-8000-000000000032',ordinary,current_date-1,false,false,false,false);
  insert into public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,
    cutoff_weekday,cutoff_local_time,nhsp_report_heading_name)
    values(source_group,'TEST',agency,'NHSP_SETUP_PROOF','NHSP setup proof','NHSP',3,'15:00','NHSP setup proof');
  req:=jsonb_build_object('actor_user_id',actor,'agency_id',agency,'environment','TEST',
    'client_id',client,'effective_date',current_date);
  shape:=public.weekly_source_client_settings_get_v1(req);
  if shape->>'eligible' is distinct from 'true' or shape->>'configured' is distinct from 'false'
    or shape#>>'{capabilities,source_family}' is distinct from 'NHSP'
    or shape#>>'{capabilities,authority_mode}' is distinct from 'SOURCE_AUTHORITY'
    or shape#>>'{capabilities,document_mode}' is distinct from 'CHECK_ONLY'
    or shape#>>'{capabilities,show_query_settings}' is distinct from 'true'
    or shape#>>'{capabilities,show_rate_settings}' is distinct from 'true' then
    raise exception 'NHSP before-contract settings not visible';
  end if;
  original_version:=shape->>'settings_version';
  req:=(req-'effective_date')||jsonb_build_object('expected_settings_version',original_version,'settings',shape->'settings');
  begin
    perform public.weekly_source_client_settings_save_atomic_v1(jsonb_set(req,'{settings,authority_mode}','"TIMESHEET_AUTHORITY"'));
    raise exception 'Caller overrode NHSP authority';
  exception when invalid_parameter_value then
    if sqlerrm<>'WEEKLY_SOURCE_CLIENT_READ_ONLY_POLICY_MISMATCH' then raise; end if;
  end;
  saved:=public.weekly_source_client_settings_save_atomic_v1(req);
  if saved->>'configured' is distinct from 'true' or saved#>>'{settings,manager_query_recipient}' is distinct from 'manager@example.invalid'
    or not exists(select 1 from public.weekly_source_group_clients m where m.client_id=client and m.valid_from=date '1900-01-01')
    or not exists(select 1 from public.weekly_source_client_policies p where p.client_id=client and p.effective_from=date '1900-01-01') then
    raise exception 'NHSP first save or historical baseline failed';
  end if;
  begin
    perform public.weekly_source_client_settings_save_atomic_v1(req);
    raise exception 'Stale first-save version accepted';
  exception when serialization_failure then
    if sqlerrm<>'WEEKLY_SOURCE_SETTINGS_STALE' then raise; end if;
  end;
  req:=req||jsonb_build_object('expected_settings_version',saved->>'settings_version','settings',saved->'settings');
  -- The client flag must never rescue an inconsistent existing source contract.
  begin
    insert into public.contracts(id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
      self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr,overrideclientsettings)
      values('e9100000-0000-4000-8000-000000000041',client,current_date-1,current_date+30,'PAYE','{}',
        false,'NHSP',false,false,false,true);
    perform public.weekly_source_client_settings_save_atomic_v1(req);
    raise exception 'NHSP flag rescued inconsistent existing contract';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_CLIENT_CONTRACT_POLICY_INCONSISTENT' then raise; end if;
  end;
  update public.clients set ts_queries_email=null where id=client;
  begin
    perform public.weekly_source_client_settings_save_atomic_v1(jsonb_set(req,'{settings,manager_query_recipient}','null'));
    raise exception 'Enabled manager queries accepted without recipient';
  exception when invalid_parameter_value then
    if sqlerrm<>'WEEKLY_SOURCE_MANAGER_QUERY_RECIPIENT_REQUIRED' then raise; end if;
  end;
  update public.clients set ts_queries_email='manager@example.invalid' where id=client;
  saved:=public.weekly_source_client_settings_save_atomic_v1(req);
  if saved->>'configured' is distinct from 'true'
    or (select count(*) from public.weekly_source_group_clients m where m.client_id=client)<>1
    or (select count(*) from public.weekly_source_client_policies p where p.client_id=client and current_date between p.effective_from and coalesce(p.effective_to,'infinity'::date))<>1 then
    raise exception 'NHSP subsequent save without contracts failed';
  end if;
  -- A disabled latest flag must not be rescued by an older true setting.
  insert into public.client_settings(id,client_id,effective_from,is_nhsp,requires_hr,autoprocess_hr,no_timesheet_required)
    values('e9100000-0000-4000-8000-000000000033',client,current_date,false,false,false,false);
  req:=req||jsonb_build_object('expected_settings_version',saved->>'settings_version','settings',saved->'settings');
  begin
    perform public.weekly_source_client_settings_save_atomic_v1(req);
    raise exception 'Disabled effective NHSP mode accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_CLIENT_CONTRACT_POLICY_INCONSISTENT' then raise; end if;
  end;
  shape:=public.weekly_source_client_settings_get_v1(jsonb_build_object('actor_user_id',actor,'agency_id',agency,
    'environment','TEST','client_id',ordinary,'effective_date',current_date));
  if shape->>'eligible' is distinct from 'false' then raise exception 'Ordinary client acquired NHSP eligibility'; end if;
  begin
    perform public.weekly_source_client_settings_save_atomic_v1(jsonb_build_object('actor_user_id',actor,'agency_id',agency,
      'environment','TEST','client_id',ordinary,'expected_settings_version',shape->>'settings_version',
      'settings',shape->'settings'));
    raise exception 'Ordinary client settings accepted without source contract';
  exception when invalid_parameter_value then
    if sqlerrm<>'WEEKLY_SOURCE_CLIENT_SETTINGS_NOT_APPLICABLE' then raise; end if;
  end;
  if before_contracts<>pg_temp.ws_verify_full_relation_fingerprint('public.contracts')
    or before_timesheets<>pg_temp.ws_verify_full_relation_fingerprint('public.timesheets')
    or before_invoices<>pg_temp.ws_verify_full_relation_fingerprint('public.invoices') then
    raise exception 'NHSP setup changed contracts, timesheets or invoices';
  end if;
  if has_function_privilege('authenticated','public.weekly_source_client_settings_save_atomic_v1(jsonb)','EXECUTE')
    or has_function_privilege('service_role','private._weekly_source_settings_client_shape_v1(uuid,text,uuid,date)','EXECUTE')
    or not has_function_privilege('service_role','public.weekly_source_client_settings_save_atomic_v1(jsonb)','EXECUTE') then
    raise exception 'NHSP setup ACL boundary changed';
  end if;
end $verify$;
rollback;
