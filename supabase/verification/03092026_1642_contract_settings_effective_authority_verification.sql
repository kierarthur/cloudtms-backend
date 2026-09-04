\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for the single dated Client/Contract
-- settings authority.  It proves precedence, planned-only refresh, real-row
-- freezing, Dedicated NHSP and HealthRoster classification, expense separation,
-- invoice VAT stability, Global-only ERNI/holiday dates and browser isolation.
begin;

do $contract_settings_effective_authority_verification$
declare
  v_client uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid();
  v_week_one uuid:=gen_random_uuid();
  v_week_two uuid:=gen_random_uuid();
  v_adjustment_week uuid:=gen_random_uuid();
  v_weekly_timesheet uuid:=gen_random_uuid();
  v_daily_timesheet uuid:=gen_random_uuid();
  v_authority jsonb;
  v_frozen jsonb;
  v_fingerprint text;
  v_import jsonb;
  v_vat jsonb;
  v_review record;
  v_finance_erni numeric;
  v_policy jsonb;
  v_flags jsonb;
  v_pay_impact jsonb;
  v_definition text;
begin
  update public.settings_defaults
  set bh_list='["2026-12-28"]'::jsonb,
      bh_feed_list='["2026-12-25","2026-12-28"]'::jsonb
  where id=1;

  insert into public.clients(
    id,name,primary_invoice_email,vat_chargeable,payment_terms_days
  ) values(
    v_client,'Settings authority verification Client',
    'settings-authority@example.test',true,30
  );
  insert into public.candidates(id,email,display_name,active,key_norm)
  values(
    v_candidate,
    'settings-authority-'||replace(v_candidate::text,'-','')||'@example.test',
    'Settings Authority Candidate',true,
    'SETTINGS-AUTHORITY-'||replace(v_candidate::text,'-','')
  );
  insert into public.client_settings(
    id,client_id,effective_from,default_submission_mode,week_ending_weekday,
    is_nhsp,autoprocess_hr,requires_hr,no_timesheet_required,
    vat_rate_pct,erni_pct,bh_list,timesheet_break_entry_mode,
    candidate_expenses_require_separate_timesheet
  ) values(
    gen_random_uuid(),v_client,'2026-06-01','ELECTRONIC',0,
    false,false,false,false,
    20,99,'["2099-01-01"]'::jsonb,'DURATION_MINUTES',false
  );
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,
    default_submission_mode,pay_method_snapshot,role,overrideclientsettings,
    is_nhsp,autoprocess_hr,requires_hr,no_timesheet_required
  ) values(
    v_contract,v_candidate,v_client,'2026-01-01','2026-12-31',0,
    'ELECTRONIC','PAYE','RMN',false,false,false,false,false
  );

  if (select effective_from from public.client_settings where client_id=v_client)
       is distinct from date '2026-01-01' then
    raise exception 'CLIENT_SETTINGS_ORIGIN_NOT_ALIGNED_TO_EARLIEST_CONTRACT';
  end if;

  insert into public.contract_weeks(
    id,contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,is_adjustment
  ) values
    (v_week_one,v_contract,'2026-05-31',0,'OPEN','ELECTRONIC',false),
    (v_week_two,v_contract,'2026-06-07',0,'OPEN','ELECTRONIC',false),
    (v_adjustment_week,v_contract,'2026-06-07',1,'OPEN','MANUAL',true);

  if (select submission_mode_snapshot from public.contract_weeks
      where id=v_adjustment_week)<>'MANUAL'::public.submission_mode_enum then
    raise exception 'PLANNED_ADJUSTMENT_MANUAL_ROUTE_NOT_PRESERVED_ON_INSERT';
  end if;

  select settings_authority_json into v_authority
  from public.contract_weeks where id=v_week_one;
  select erni_pct into v_finance_erni
  from public.settings_finance_pick(date '2026-05-31') limit 1;
  if v_authority->>'authority_version'<>'CONTRACT_SETTINGS_AUTHORITY_V1'
     or v_authority->>'configured_route'<>'STANDARD_WEEKLY'
     or v_authority#>>'{sources,contract_governed_settings}'<>'CLIENT_SETTINGS'
     or coalesce((v_authority#>>'{values,erni_pct}')::numeric,-1)
        is distinct from coalesce(v_finance_erni,13.8)
     or not (v_authority#>'{values,bh_list}' ? '2026-12-25')
     or not (v_authority#>'{values,bh_list}' ? '2026-12-28')
     or (v_authority#>'{values,bh_list}' ? '2099-01-01') then
    raise exception 'GLOBAL_CLIENT_AUTHORITY_RESOLUTION_INVALID:%',v_authority;
  end if;

  -- A Client change refreshes both remaining planned weeks while Contract
  -- override is off. Dedicated NHSP Weekly is always authoritative.
  update public.client_settings set is_nhsp=true,nhsp_import_auto_authorise=true
  where client_id=v_client;
  if (select submission_mode_snapshot from public.contract_weeks
      where id=v_adjustment_week)<>'MANUAL'::public.submission_mode_enum then
    raise exception 'PLANNED_ADJUSTMENT_MANUAL_ROUTE_REWRITTEN_BY_CLIENT_REFRESH';
  end if;
  select settings_authority_json into v_authority
  from public.contract_weeks where id=v_week_one;
  if v_authority->>'configured_route'<>'DEDICATED_NHSP_WEEKLY'
     or v_authority#>>'{applicability,import_authoritative}'<>'true'
     or v_authority#>>'{applicability,candidate_hours_view_only}'<>'true'
     or v_authority#>>'{values,candidate_expenses_require_separate_timesheet}'<>'true'
     or v_authority#>>'{values,candidate_paper_submission_enabled}'<>'false' then
    raise exception 'DEDICATED_NHSP_WEEKLY_NOT_AUTHORITATIVE:%',v_authority;
  end if;

  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope
  ) values(
    v_weekly_timesheet,'SETTINGS-AUTH-WEEKLY-'||v_weekly_timesheet::text,
    lower(v_candidate::text),'settings authority hospital','ward','rmn',
    v_contract,'2026-05-31','HOURS','MANUAL','WEEKLY'
  );
  update public.contract_weeks set timesheet_id=v_weekly_timesheet
  where id=v_week_one;
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,
    processing_status
  ) values(v_weekly_timesheet,1,v_candidate,v_client,8,'UNPROCESSED');

  select settings_authority_json,settings_authority_fingerprint
  into v_frozen,v_fingerprint
  from public.timesheets where timesheet_id=v_weekly_timesheet;
  if v_frozen#>>'{applicability,import_authoritative}'<>'true'
     or private._timesheet_settings_authority_frozen_v1(v_weekly_timesheet)
        is distinct from v_frozen then
    raise exception 'WEEKLY_REAL_TIMESHEET_NOT_FROZEN:%',v_frozen;
  end if;
  update public.timesheets set
    settings_authority_json=jsonb_set(settings_authority_json,'{values,is_nhsp}','false'::jsonb),
    settings_authority_fingerprint=repeat('0',64)
  where timesheet_id=v_weekly_timesheet;
  if (select settings_authority_json from public.timesheets
      where timesheet_id=v_weekly_timesheet) is distinct from v_frozen
     or (select settings_authority_fingerprint from public.timesheets
         where timesheet_id=v_weekly_timesheet) is distinct from v_fingerprint then
    raise exception 'REAL_TIMESHEET_AUTHORITY_DIRECT_UPDATE_NOT_BLOCKED';
  end if;
  update public.contract_weeks set
    settings_authority_json='{}'::jsonb,
    settings_authority_version=null,
    settings_authority_fingerprint=null,
    settings_authority_resolved_at=null
  where id=v_week_one;
  if (select settings_authority_fingerprint from public.contract_weeks
      where id=v_week_one) is distinct from v_fingerprint then
    raise exception 'LINKED_WEEK_AUTHORITY_DIRECT_UPDATE_NOT_BLOCKED';
  end if;
  update public.contract_weeks set
    settings_authority_json='{}'::jsonb,
    settings_authority_version=null,
    settings_authority_fingerprint=null,
    settings_authority_resolved_at=null
  where id=v_week_two;
  if (select settings_authority_json from public.contract_weeks where id=v_week_two)='{}'::jsonb then
    raise exception 'PLANNED_WEEK_AUTHORITY_DIRECT_UPDATE_NOT_RECALCULATED';
  end if;
  if private._candidate_submission_mode_v1(v_client,v_contract,'2026-05-31')
       <>'ELECTRONIC'::public.submission_mode_enum then
    raise exception 'CANDIDATE_SUBMISSION_MODE_BYPASSED_AUTHORITY';
  end if;
  v_policy:=private._candidate_policy_resolve_v1(v_client,v_contract,'2026-05-31');
  if v_policy->>'import_expense_separation_mandatory'<>'true'
     or v_policy->>'paper_submission_enabled'<>'false'
     or v_policy->>'authority_fingerprint' is distinct from v_fingerprint then
    raise exception 'CANDIDATE_POLICY_BYPASSED_AUTHORITY:%',v_policy;
  end if;
  v_policy:=public.import_auto_authorise_policy_resolve_v2(
    'NHSP'::public.hr_source_enum,v_client,v_contract,v_weekly_timesheet,
    '2026-05-31',false
  );
  if v_policy->>'effective_value'<>'true'
     or v_policy->>'authority_version'<>'CONTRACT_SETTINGS_AUTHORITY_V1'
     or v_policy->>'relevant_date'<>'2026-05-31'
     or v_policy->>'authority_fingerprint' is distinct from v_fingerprint then
    raise exception 'IMPORT_AUTO_AUTHORISE_BYPASSED_AUTHORITY:%',v_policy;
  end if;

  -- The stale caller shape says editable; server authority must still suppress
  -- the NHSP hours/break editor and preserve separate Candidate expenses.
  v_import:=private._candidate_import_authoritative_v1(
    v_client,v_contract,v_weekly_timesheet,null,'2026-05-31'
  );
  if v_import->>'is_import_authoritative'<>'true'
     or v_import->>'candidate_hours_view_only'<>'true'
     or v_import->>'mandatory_expense_separation'<>'true' then
    raise exception 'CANDIDATE_NHSP_READ_AUTHORITY_INVALID:%',v_import;
  end if;
  v_import:=private._candidate_break_entry_context_core_v1(
    v_weekly_timesheet,v_week_one,'2026-05-31',
    jsonb_build_object(
      'can_edit_hours',true,'import_authoritative',false,
      'route_family','ELECTRONIC'
    )
  );
  if v_import->>'applicable'<>'false'
     or v_import->>'reason'<>'IMPORT_AUTHORITATIVE' then
    raise exception 'CANDIDATE_NHSP_BREAK_EDITOR_EXPOSED:%',v_import;
  end if;

  -- Later Client settings refresh only the still-planned week. They cannot
  -- rewrite the real Weekly Timesheet or its invoicing authority.
  update public.client_settings set
    is_nhsp=false,autoprocess_hr=true,requires_hr=true,
    no_timesheet_required=false,vat_rate_pct=5,
    pay_reference_required=true
  where client_id=v_client;
  if (select settings_authority_fingerprint from public.timesheets
     where timesheet_id=v_weekly_timesheet) is distinct from v_fingerprint
     or (select (settings_authority_json#>>'{values,vat_rate_pct}')::numeric
         from public.timesheets where timesheet_id=v_weekly_timesheet)<>20 then
    raise exception 'REAL_WEEKLY_TIMESHEET_REWRITTEN_BY_CLIENT_CHANGE: expected %, actual %, vat %',
      v_fingerprint,
      (select settings_authority_fingerprint from public.timesheets
       where timesheet_id=v_weekly_timesheet),
      (select settings_authority_json#>>'{values,vat_rate_pct}'
       from public.timesheets where timesheet_id=v_weekly_timesheet);
  end if;
  select x.out_policy,x.out_effective_flags into v_policy,v_flags
  from public.tsfin_load_context_batch(array[v_weekly_timesheet]) x;
  if (v_policy->>'vat_rate_pct')::numeric<>20
     or v_policy->>'settings_authority_fingerprint' is distinct from v_fingerprint
     or v_flags->>'client_is_nhsp'<>'true' then
    raise exception 'TSFIN_PROCESSING_DID_NOT_USE_FROZEN_AUTHORITY:%:%',v_policy,v_flags;
  end if;
  v_pay_impact:=public.pay_timesheet_impact_preview(v_weekly_timesheet);
  if v_pay_impact->>'require_reference_to_pay'<>'false' then
    raise exception 'BANKING_PAY_IMPACT_DID_NOT_USE_FROZEN_AUTHORITY:%',v_pay_impact;
  end if;
  select settings_authority_json into v_authority
  from public.contract_weeks where id=v_week_two;
  if v_authority->>'configured_route'<>'HEALTHROSTER_WEEKLY_VALIDATION'
     or v_authority#>>'{applicability,import_authoritative}'<>'false' then
    raise exception 'HEALTHROSTER_VALIDATION_CLASSIFICATION_INVALID:%',v_authority;
  end if;
  select * into v_review
  from public._import_review_effective_authority_core_v1(
    'HR_WEEKLY',v_contract,v_client,'2026-06-07'
  );
  if not v_review.route_eligible or not v_review.validation_eligible
     or v_review.import_authoritative then
    raise exception 'HEALTHROSTER_VALIDATION_REVIEW_AUTHORITY_INVALID';
  end if;

  select to_jsonb(v) into v_vat
  from private._invoice_generation_vat_policy_batch(jsonb_build_array(
    jsonb_build_object(
      'source_member_key','settings-authority-vat',
      'source_type','TIMESHEET','source_id',v_weekly_timesheet,
      'timesheet_id',v_weekly_timesheet,'effective_date','2026-05-31'
    )
  )) v limit 1;
  if v_vat->>'valid'<>'true' or (v_vat->>'vat_rate')::numeric<>20 then
    raise exception 'INVOICE_VAT_DID_NOT_USE_FROZEN_TIMESHEET_AUTHORITY:%',v_vat;
  end if;

  -- A Contract override retains its governed values, while a subsequent
  -- Client change still refreshes Client-owned values on the remaining
  -- planned week.
  update public.contracts set
    overrideclientsettings=true,is_nhsp=false,autoprocess_hr=false,
    requires_hr=false,no_timesheet_required=false
  where id=v_contract;
  update public.client_settings set is_nhsp=true,autoprocess_hr=false,
    requires_hr=false,no_timesheet_required=false,vat_rate_pct=7
  where client_id=v_client;
  select settings_authority_json into v_authority
  from public.contract_weeks where id=v_week_two;
  if v_authority->>'configured_route'<>'STANDARD_WEEKLY'
     or v_authority#>>'{sources,contract_governed_settings}'<>'CONTRACT_OVERRIDE'
     or coalesce((v_authority#>>'{values,vat_rate_pct}')::numeric,-1)<>7 then
    raise exception 'CONTRACT_OVERRIDE_PRECEDENCE_INVALID:%',v_authority;
  end if;
  update public.contracts set is_nhsp=true where id=v_contract;
  if (select settings_authority_json#>>'{applicability,import_authoritative}'
      from public.contract_weeks where id=v_week_two)<>'true' then
    raise exception 'CONTRACT_OVERRIDE_PLANNED_REFRESH_MISSING';
  end if;

  -- HealthRoster authoritative is the exact autoprocess + no-timesheet route;
  -- it does not require or permit the validation-only flag simultaneously.
  update public.contracts set overrideclientsettings=false where id=v_contract;
  update public.client_settings set
    is_nhsp=false,autoprocess_hr=true,requires_hr=false,
    no_timesheet_required=true
  where client_id=v_client;
  select settings_authority_json into v_authority
  from public.contract_weeks where id=v_week_two;
  if v_authority->>'configured_route'<>'HEALTHROSTER_WEEKLY_AUTHORITATIVE'
     or v_authority#>>'{applicability,import_authoritative}'<>'true' then
    raise exception 'HEALTHROSTER_AUTHORITATIVE_CLASSIFICATION_INVALID:%',v_authority;
  end if;
  select * into v_review
  from public._import_review_effective_authority_core_v1(
    'HR_WEEKLY',v_contract,v_client,'2026-06-07'
  );
  if not v_review.route_eligible or v_review.validation_eligible
     or not v_review.import_authoritative then
    raise exception 'HEALTHROSTER_AUTHORITATIVE_REVIEW_AUTHORITY_INVALID';
  end if;

  -- Daily is not real at row creation. It freezes only when Office processing
  -- supplies processed_at_utc, then later settings edits cannot rewrite it.
  insert into public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
    job_title_norm,contract_id,week_ending_date,line_type,submission_mode,
    sheet_scope,settings_authority_json,settings_authority_version,
    settings_authority_fingerprint,settings_authority_resolved_at
  ) values(
    v_daily_timesheet,'SETTINGS-AUTH-DAILY-'||v_daily_timesheet::text,
    lower(v_candidate::text),'settings authority hospital','ward','rmn',
    v_contract,'2026-06-07','HOURS','MANUAL','DAILY',
    v_frozen,'CONTRACT_SETTINGS_AUTHORITY_V1',v_fingerprint,
    (v_frozen->>'resolved_at_utc')::timestamptz
  );
  if (select settings_authority_json from public.timesheets
      where timesheet_id=v_daily_timesheet)<>'{}'::jsonb then
    raise exception 'DAILY_TIMESHEET_CALLER_SNAPSHOT_NOT_CLEARED_ON_INSERT';
  end if;
  update public.timesheets set
    settings_authority_json=v_frozen,
    settings_authority_version='CONTRACT_SETTINGS_AUTHORITY_V1',
    settings_authority_fingerprint=v_fingerprint,
    settings_authority_resolved_at=(v_frozen->>'resolved_at_utc')::timestamptz
  where timesheet_id=v_daily_timesheet;
  if (select settings_authority_json from public.timesheets
      where timesheet_id=v_daily_timesheet)<>'{}'::jsonb then
    raise exception 'UNPROCESSED_DAILY_TIMESHEET_CALLER_SNAPSHOT_NOT_CLEARED';
  end if;
  insert into public.timesheets_financials(
    timesheet_id,timesheet_version,candidate_id,client_id,total_hours,
    processing_status,processed_at_utc
  ) values(
    v_daily_timesheet,1,v_candidate,v_client,8,'READY_FOR_INVOICE',
    '2026-06-08 09:00:00+00'
  );
  select settings_authority_json,settings_authority_fingerprint
  into v_frozen,v_fingerprint
  from public.timesheets where timesheet_id=v_daily_timesheet;
  if v_frozen->>'configured_route'<>'HEALTHROSTER_DAILY_AUTHORITATIVE'
     or v_frozen#>>'{applicability,import_authoritative}'<>'true' then
    raise exception 'DAILY_PROCESSED_TIMESHEET_NOT_FROZEN:%',v_frozen;
  end if;
  update public.client_settings set
    autoprocess_hr=false,requires_hr=false,no_timesheet_required=false
  where client_id=v_client;
  if (select settings_authority_fingerprint from public.timesheets
      where timesheet_id=v_daily_timesheet) is distinct from v_fingerprint then
    raise exception 'REAL_DAILY_TIMESHEET_REWRITTEN_BY_CLIENT_CHANGE';
  end if;

  if not exists(
       select 1 from pg_catalog.pg_trigger
       where tgname='contract_week_settings_authority_before_v1' and tgenabled='O'
     )
     or not exists(
       select 1 from pg_catalog.pg_trigger
       where tgname='daily_timesheet_settings_authority_after_fin_v1' and tgenabled='O'
     ) then
    raise exception 'CONTRACT_SETTINGS_FREEZE_TRIGGERS_NOT_ENABLED';
  end if;
  select pg_catalog.pg_get_functiondef(
    pg_catalog.to_regprocedure(
      'public.pay_preview_candidate_collect_scope(jsonb,uuid,jsonb,integer)'
    )
  ) into v_definition;
  if (length(v_definition)-length(replace(
       v_definition,'private._timesheet_settings_authority_frozen_v1(ts.timesheet_id)',''
     ))) / length('private._timesheet_settings_authority_frozen_v1(ts.timesheet_id)')<>2
     or v_definition like '%con.overrideclientsettings%require_reference_to_pay%'
     or v_definition like '%cs.pay_reference_required%as require_reference_to_pay%' then
    raise exception 'BANKING_PAY_COLLECT_SCOPE_NOT_FROZEN';
  end if;
  select pg_catalog.pg_get_functiondef(
    pg_catalog.to_regprocedure('private._invoice_correction_validate_batch(jsonb,date)')
  ) into v_definition;
  if v_definition not like '%_timesheet_settings_authority_frozen_v1%'
     or v_definition like '%from public.client_settings%' then
    raise exception 'INVOICE_CORRECTION_VALIDATION_NOT_FROZEN';
  end if;
  select pg_catalog.pg_get_functiondef(
    pg_catalog.to_regprocedure(
      'private._invoice_presentation_snapshot_batch(jsonb,timestamp with time zone)'
    )
  ) into v_definition;
  if v_definition not like '%_timesheet_settings_authority_frozen_v1%'
     or v_definition like '%from public.client_settings%' then
    raise exception 'INVOICE_PRESENTATION_NOT_FROZEN';
  end if;
  select pg_catalog.pg_get_functiondef(
    pg_catalog.to_regprocedure('public.timesheet_summary_lightweight_rows_v1(jsonb)')
  ) into v_definition;
  if v_definition like '%BOOL_OR(client_setting.reference_number_required_to_issue_invoice)%'
     or v_definition like '%client_reference_settings%' then
    raise exception 'OFFICE_TIMESHEET_SUMMARY_STILL_READS_CLIENT_HISTORY';
  end if;
  if pg_catalog.has_function_privilege(
       'anon','public.contract_settings_effective_get_v1(uuid,uuid,date,text,uuid)','EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated','public.contract_settings_effective_get_v1(uuid,uuid,date,text,uuid)','EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role','public.contract_settings_effective_get_v1(uuid,uuid,date,text,uuid)','EXECUTE'
     ) then
    raise exception 'CONTRACT_SETTINGS_RESOLVER_ACL_INVALID';
  end if;
end
$contract_settings_effective_authority_verification$;

rollback;

select 'PASS'::text as contract_settings_effective_authority_verification;
