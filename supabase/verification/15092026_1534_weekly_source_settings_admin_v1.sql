\set ON_ERROR_STOP on

begin;

do $verify_weekly_source_settings_admin$
declare
  v_actor uuid:='9a100000-0000-4000-8000-000000000001';
  v_agency uuid:='9a100000-0000-4000-8000-000000000002';
  v_source_client uuid:='9a100000-0000-4000-8000-000000000011';
  v_timesheet_client uuid:='9a100000-0000-4000-8000-000000000012';
  v_nhsp_client uuid:='9a100000-0000-4000-8000-000000000013';
  v_source_contract uuid:='9a100000-0000-4000-8000-000000000021';
  v_timesheet_contract uuid:='9a100000-0000-4000-8000-000000000022';
  v_nhsp_contract uuid:='9a100000-0000-4000-8000-000000000023';
  v_group uuid;
  v_nhsp_group uuid;
  v_response jsonb;
  v_version text;
  v_global_version bigint;
  v_cycle_version bigint;
begin
  insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
  values (v_actor,'weekly-settings-proof@example.invalid','admin','not-a-real-password','Settings proof',true);
  insert into public.clients(id,name,ts_queries_email) values
    (v_source_client,'Settings source authority','manager-default@example.invalid'),
    (v_timesheet_client,'Settings Timesheet authority','existing-manager-route@example.invalid'),
    (v_nhsp_client,'Settings dedicated NHSP authority','nhsp-manager@example.invalid');
  insert into public.contracts(
    id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    self_bill,weekly_timesheet_source,no_timesheet_required,requires_hr,autoprocess_hr,
    overrideclientsettings
  ) values
    (v_source_contract,v_source_client,current_date-7,current_date+365,'PAYE','{}',
     true,'HEALTHROSTER',true,true,true,true),
    (v_timesheet_contract,v_timesheet_client,current_date-7,current_date+365,'PAYE','{}',
     false,'HEALTHROSTER',false,true,false,true),
    (v_nhsp_contract,v_nhsp_client,current_date-7,current_date+365,'PAYE','{}',
     true,'NHSP',false,false,false,true);

  v_response:=public.weekly_source_source_group_save_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'source_group',pg_catalog.jsonb_build_object(
      'display_name','Settings proof Roster','source_family','ROSTER',
      'cutoff_weekday',3,'cutoff_local_time','15:00','active',true
    )
  ));
  v_group:=(v_response->>'saved_source_group_id')::uuid;
  if v_group is null or not exists(
    select 1 from public.weekly_source_groups source_group
    where source_group.id=v_group and source_group.code like 'SOURCE\_%' escape '\'
      and source_group.agency_id=v_agency and source_group.environment='TEST'
  ) then
    raise exception 'source group server scope/default code proof failed';
  end if;
  if (select pg_catalog.count(*) from public.weekly_source_cycles cycle
      where cycle.source_group_id=v_group)<>1
     or not exists(
       select 1 from public.weekly_source_cycles cycle
       where cycle.source_group_id=v_group and cycle.state='OPEN'
         and cycle.projection_state='NONE' and cycle.version=0
         and extract(dow from cycle.finalisation_week_ending)=0
         and (cycle.cutoff_at_utc at time zone 'Europe/London')::date=
           cycle.finalisation_week_ending+3
         and (cycle.cutoff_at_utc at time zone 'Europe/London')::time='15:00'::time
     ) then
    raise exception 'source group did not create its exact first open cycle';
  end if;
  perform private._weekly_source_settings_ensure_open_cycle_v1(
    v_group,pg_catalog.transaction_timestamp()
  );
  if (select pg_catalog.count(*) from public.weekly_source_cycles cycle
      where cycle.source_group_id=v_group)<>1 then
    raise exception 'first-cycle replay created a duplicate cycle';
  end if;

  v_response:=public.weekly_source_source_group_save_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'source_group',pg_catalog.jsonb_build_object(
      'display_name','Settings proof NHSP','source_family','NHSP',
      'cutoff_weekday',3,'cutoff_local_time','15:00',
      'nhsp_report_heading_name','Settings proof agency','active',true
    )
  ));
  v_nhsp_group:=(v_response->>'saved_source_group_id')::uuid;
  if v_nhsp_group is null or not exists(
    select 1 from public.weekly_source_groups source_group
    where source_group.id=v_nhsp_group and source_group.source_family='NHSP'
      and source_group.agency_id=v_agency and source_group.environment='TEST'
  ) then
    raise exception 'NHSP source group proof failed';
  end if;

  v_response:=public.weekly_source_global_settings_get_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST'
  ));
  v_global_version:=(v_response->>'settings_version')::bigint;
  v_response:=public.weekly_source_global_settings_save_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'expected_settings_version',v_global_version,
    'settings',pg_catalog.jsonb_build_object(
      'candidate_reminder_minutes',360,'candidate_response_deadline_minutes',720,
      'manager_digest_minutes',360,'manager_manual_resend_cooldown_minutes',5,
      'candidate_manual_reminder_cooldown_minutes',60,'manager_secure_link_days',5
    )
  ));
  if v_response->'settings'->>'manager_secure_link_days'<>'5' then
    raise exception 'secure manager-link lifetime save proof failed';
  end if;
  begin
    perform public.weekly_source_global_settings_save_atomic_v1(pg_catalog.jsonb_build_object(
      'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
      'expected_settings_version',v_global_version,'settings',v_response->'settings'
    ));
    raise exception 'stale global save unexpectedly succeeded';
  exception when serialization_failure then null;
  end;

  v_response:=public.weekly_source_client_settings_get_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'client_id',v_source_client,'effective_date',current_date
  ));
  if (v_response->>'eligible')::boolean is not true
     or (v_response->>'configured')::boolean is not false then
    raise exception 'unconfigured eligible Client read proof failed';
  end if;
  v_version:=v_response->>'settings_version';
  v_response:=public.weekly_source_client_settings_save_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'client_id',v_source_client,'expected_settings_version',v_version,
    'settings',pg_catalog.jsonb_build_object(
      'source_group_id',v_group,'effective_from',current_date,
      'authority_mode','SOURCE_AUTHORITY','document_mode','CHECK_ONLY','self_bill_enabled',true,
      'self_bill_correction_presentation','FULL_REVERSAL_REPLACEMENT',
      'source_fixed_expenses_enabled',false,'source_expense_vat_enabled',false,
      'weekly_rate_classification_method','SPLIT_RATE_WINDOWS',
      'duration_break_tie_rule','EARLIEST_LONGEST_PORTION',
      'candidate_queries_enabled',true,'manager_queries_enabled',true,
      'manager_query_recipient',null,'completed_pack_copy_enabled',false,
      'completed_pack_recipient',null
    )
  ));
  if v_response->'settings'->>'manager_query_recipient'<>'manager-default@example.invalid'
     or (v_response->>'configured')::boolean is not true then
    raise exception 'Client settings/default manager recipient proof failed';
  end if;
  if v_response->'settings'->>'effective_from'<>current_date::text
     or not exists(
       select 1 from public.weekly_source_group_clients membership
       where membership.client_id=v_source_client and membership.source_group_id=v_group
         and membership.valid_from=date '1900-01-01'
     )
     or not exists(
       select 1 from public.weekly_source_client_policies policy
       where policy.client_id=v_source_client and policy.source_group_id=v_group
         and policy.effective_from=date '1900-01-01'
     ) then
    raise exception 'first Client settings were not stored from the 1900 baseline';
  end if;
  v_response:=public.weekly_source_client_settings_get_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'client_id',v_source_client,'effective_date',current_date-7
  ));
  if (v_response->>'configured')::boolean is not true
     or v_response->'settings'->>'effective_from'<>(current_date-7)::text then
    raise exception 'first Client settings do not cover a prior-week source date';
  end if;
  v_response:=public.weekly_source_client_settings_get_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'client_id',v_source_client,'effective_date',current_date
  ));

  v_version:=v_response->>'settings_version';
  v_response:=public.weekly_source_client_settings_save_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'client_id',v_source_client,'expected_settings_version',v_version,
    'settings',(v_response->'settings')||pg_catalog.jsonb_build_object(
      'completed_pack_copy_enabled',true,'completed_pack_recipient',null
    )
  ));
  if v_response->'settings'->>'completed_pack_recipient'<>'manager-default@example.invalid' then
    raise exception 'completed-pack recipient default proof failed';
  end if;

  update public.weekly_source_cycles
  set state='OPEN',version=4,projection_state='FAILED'
  where source_group_id=v_group;
  v_version:=v_response->>'settings_version';
  v_response:=public.weekly_source_client_settings_save_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'client_id',v_source_client,'expected_settings_version',v_version,
    'settings',(v_response->'settings')||pg_catalog.jsonb_build_object(
      'weekly_rate_classification_method','WHOLE_SHIFT_START_DAY',
      'duration_break_tie_rule',null
    )
  ));
  select cycle.version into strict v_cycle_version
  from public.weekly_source_cycles cycle where cycle.source_group_id=v_group;
  if v_cycle_version<>5 or exists(
    select 1 from public.weekly_source_cycles cycle
    where cycle.source_group_id=v_group and cycle.projection_state<>'NONE'
  ) or v_response->'settings'->>'duration_break_tie_rule' is not null then
    raise exception 'open preview staleness/whole-shift child clearing proof failed';
  end if;

  v_response:=public.weekly_source_client_settings_get_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'client_id',v_nhsp_client,'effective_date',current_date
  ));
  if (v_response->>'eligible')::boolean is not true
     or v_response->'settings'->>'authority_mode'<>'SOURCE_AUTHORITY'
     or v_response->'settings'->>'document_mode'<>'CHECK_ONLY'
     or (v_response->'settings'->>'self_bill_enabled')::boolean is not true then
    raise exception 'dedicated NHSP source-authority derivation proof failed';
  end if;
  v_version:=v_response->>'settings_version';
  v_response:=public.weekly_source_client_settings_save_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'client_id',v_nhsp_client,'expected_settings_version',v_version,
    'settings',pg_catalog.jsonb_build_object(
      'source_group_id',v_nhsp_group,'effective_from',current_date,
      'authority_mode','SOURCE_AUTHORITY','document_mode','CHECK_ONLY','self_bill_enabled',true,
      'self_bill_correction_presentation',null,
      'source_fixed_expenses_enabled',false,'source_expense_vat_enabled',false,
      'weekly_rate_classification_method','SPLIT_RATE_WINDOWS',
      'duration_break_tie_rule','EARLIEST_LONGEST_PORTION',
      'candidate_queries_enabled',true,'manager_queries_enabled',true,
      'manager_query_recipient',null,'completed_pack_copy_enabled',false,
      'completed_pack_recipient',null
    )
  ));
  if (v_response->>'configured')::boolean is not true
     or v_response->'settings'->>'authority_mode'<>'SOURCE_AUTHORITY'
     or v_response->'settings'->>'manager_query_recipient'<>'nhsp-manager@example.invalid' then
    raise exception 'dedicated NHSP settings save proof failed';
  end if;

  v_response:=public.weekly_source_client_settings_get_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'client_id',v_timesheet_client,'effective_date',current_date
  ));
  v_version:=v_response->>'settings_version';
  begin
    perform public.weekly_source_client_settings_save_atomic_v1(pg_catalog.jsonb_build_object(
      'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
      'client_id',v_timesheet_client,'expected_settings_version',v_version,
      'settings',pg_catalog.jsonb_build_object(
        'source_group_id',v_group,'effective_from',current_date,
        'authority_mode','TIMESHEET_AUTHORITY','document_mode','INVOICE_EVIDENCE_REQUIRED',
        'self_bill_enabled',false,'self_bill_correction_presentation',null,
        'source_fixed_expenses_enabled',false,'source_expense_vat_enabled',false,
        'weekly_rate_classification_method','SPLIT_RATE_WINDOWS',
        'duration_break_tie_rule','EARLIEST_LONGEST_PORTION',
        'candidate_queries_enabled',false,'manager_queries_enabled',true,
        'manager_query_recipient','new-secure-route@example.invalid',
        'completed_pack_copy_enabled',false,'completed_pack_recipient',null
      )
    ));
    raise exception 'Timesheet-authority secure manager route unexpectedly accepted';
  exception when invalid_parameter_value then null;
  end;
  v_response:=public.weekly_source_client_settings_save_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'client_id',v_timesheet_client,'expected_settings_version',v_version,
    'settings',pg_catalog.jsonb_build_object(
      'source_group_id',v_group,'effective_from',current_date,
      'authority_mode','TIMESHEET_AUTHORITY','document_mode','INVOICE_EVIDENCE_REQUIRED',
      'self_bill_enabled',false,'self_bill_correction_presentation',null,
      'source_fixed_expenses_enabled',false,'source_expense_vat_enabled',false,
      'weekly_rate_classification_method','SPLIT_RATE_WINDOWS',
      'duration_break_tie_rule','EARLIEST_LONGEST_PORTION',
      'candidate_queries_enabled',false,'manager_queries_enabled',false,
      'manager_query_recipient',null,'completed_pack_copy_enabled',false,
      'completed_pack_recipient',null
    )
  ));
  if (v_response->'capabilities'->>'show_query_settings')::boolean
     or (v_response->'settings'->>'manager_queries_enabled')::boolean then
    raise exception 'Timesheet-authority existing-manager-route isolation proof failed';
  end if;

  v_response:=public.weekly_source_contract_settings_get_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'contract_id',v_source_contract,'effective_date',current_date
  ));
  v_version:=v_response->>'settings_version';
  v_response:=public.weekly_source_contract_settings_save_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment','TEST',
    'contract_id',v_source_contract,'expected_settings_version',v_version,
    'settings',pg_catalog.jsonb_build_object(
      'effective_from',current_date,
      'weekly_rate_classification_method_override','WHOLE_SHIFT_START_DAY',
      'duration_break_tie_rule_override','LATEST_LONGEST_PORTION',
      'source_fixed_expenses_enabled_override',null,'source_expense_vat_enabled_override',null,
      'candidate_queries_enabled_override',null,'manager_queries_enabled_override',null,
      'manager_query_recipient_override',null,'completed_pack_copy_enabled_override',null,
      'completed_pack_recipient_override',null
    )
  ));
  if v_response->'settings'->>'duration_break_tie_rule_override' is not null
     or v_response->'settings'->'effective'->>'weekly_rate_classification_method'<>'WHOLE_SHIFT_START_DAY' then
    raise exception 'Contract independent override/child clearing proof failed';
  end if;

  if has_function_privilege('anon','public.weekly_source_client_settings_get_v1(jsonb)','execute')
     or has_function_privilege('authenticated','public.weekly_source_client_settings_get_v1(jsonb)','execute')
     or not has_function_privilege('service_role','public.weekly_source_client_settings_get_v1(jsonb)','execute') then
    raise exception 'settings RPC ACL proof failed';
  end if;
end;
$verify_weekly_source_settings_admin$;

rollback;
