\set ON_ERROR_STOP on

-- Self-billed Candidate expenses must use a separate expense Timesheet,
-- including ordinary (non-import-authoritative) weekly and daily hours.
-- Preserve immutable Timesheet snapshots and all import/hour-entry authority.
-- No business rows, approved claims, finance or payment artifacts are changed.
begin;

create or replace function private._contract_settings_effective_core_v1(
  p_client_id uuid,
  p_contract_id uuid,
  p_relevant_date date,
  p_workflow text,
  p_timesheet_id uuid default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_contract public.contracts%rowtype;
  v_client public.client_settings%rowtype;
  v_client_record public.clients%rowtype;
  v_defaults public.settings_defaults%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_finance jsonb := '{}'::jsonb;
  v_client_id uuid := p_client_id;
  v_client_found boolean := false;
  v_unresolved_daily boolean := false;
  v_workflow text := upper(btrim(coalesce(p_workflow, '')));
  v_override boolean := false;
  v_is_nhsp boolean := false;
  v_requires_hr boolean := false;
  v_autoprocess_hr boolean := false;
  v_no_timesheet_required boolean := false;
  v_import_authoritative boolean := false;
  v_self_bill boolean := false;
  v_configuration_valid boolean := true;
  v_configuration_issue text;
  v_route text := 'STANDARD_WEEKLY';
  v_source text := 'CLIENT_SETTINGS';
  v_holidays jsonb := '[]'::jsonb;
  v_values jsonb;
  v_sources jsonb;
  v_components jsonb;
  v_applicability jsonb;
  v_payload jsonb;
  v_fingerprint text;
begin
  if p_relevant_date is null then
    raise exception 'CONTRACT_SETTINGS_RELEVANT_DATE_REQUIRED' using errcode='22023';
  end if;
  if v_workflow not in ('WEEKLY','DAILY','IMPORT','OFFICE','INVOICE','FINANCE') then
    raise exception 'CONTRACT_SETTINGS_WORKFLOW_INVALID' using errcode='22023';
  end if;

  if p_timesheet_id is not null then
    select * into v_timesheet
    from public.timesheets t
    where t.timesheet_id=p_timesheet_id and t.is_current=true;
    if not found then
      raise exception 'CONTRACT_SETTINGS_CURRENT_TIMESHEET_NOT_FOUND' using errcode='P0002';
    end if;
    if v_timesheet.settings_authority_json <> '{}'::jsonb then
      return private._timesheet_settings_authority_frozen_v1(p_timesheet_id);
    end if;
    if v_timesheet.sheet_scope='WEEKLY'::public.timesheet_scope_enum
       or exists(
         select 1 from public.timesheets_financials tf
         where tf.timesheet_id=v_timesheet.timesheet_id
           and tf.is_current=true
           and tf.processed_at_utc is not null
       ) then
      raise exception 'CONTRACT_SETTINGS_TIMESHEET_AUTHORITY_NOT_FROZEN' using errcode='55000';
    end if;
    if p_contract_id is not null and p_contract_id is distinct from v_timesheet.contract_id then
      raise exception 'CONTRACT_SETTINGS_TIMESHEET_CONTRACT_MISMATCH' using errcode='22023';
    end if;
  end if;

  if p_contract_id is not null then
    select * into v_contract from public.contracts c where c.id=p_contract_id;
    if not found then
      raise exception 'CONTRACT_SETTINGS_CONTRACT_NOT_FOUND' using errcode='P0002';
    end if;
    if v_client_id is not null and v_client_id<>v_contract.client_id then
      raise exception 'CONTRACT_SETTINGS_CLIENT_CONTRACT_MISMATCH' using errcode='22023';
    end if;
    v_client_id:=v_contract.client_id;
    v_override:=coalesce(v_contract.overrideclientsettings,false);
  end if;
  if v_client_id is null then
    v_unresolved_daily:=(
      p_timesheet_id is not null
      and v_workflow='DAILY'
      and v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum
      and v_timesheet.contract_id is null
      and v_timesheet.settings_authority_json='{}'::jsonb
    );
    if not v_unresolved_daily then
      raise exception 'CONTRACT_SETTINGS_CLIENT_REQUIRED' using errcode='22023';
    end if;
  else
    select * into v_client_record from public.clients cl where cl.id=v_client_id;
    if not found then
      raise exception 'CONTRACT_SETTINGS_CLIENT_NOT_FOUND' using errcode='P0002';
    end if;

    select * into v_client
    from public.client_settings cs
    where cs.client_id=v_client_id
      and (cs.effective_from is null or cs.effective_from<=p_relevant_date)
    order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc
    limit 1;
    v_client_found:=found;
    if not v_client_found then
      raise exception 'CONTRACT_SETTINGS_CLIENT_SETTINGS_NOT_FOUND' using errcode='P0002';
    end if;
  end if;
  select * into v_defaults from public.settings_defaults d where d.id=1;
  if not found then
    raise exception 'CONTRACT_SETTINGS_GLOBAL_SETTINGS_NOT_FOUND' using errcode='P0002';
  end if;
  select to_jsonb(fin) into v_finance
  from public.settings_finance_pick(p_relevant_date) fin
  limit 1;
  v_finance:=coalesce(v_finance,'{}'::jsonb);

  v_is_nhsp:=case when v_override then coalesce(v_contract.is_nhsp,false)
    else coalesce(v_client.is_nhsp,false) end;
  v_requires_hr:=case when v_override then coalesce(v_contract.requires_hr,false)
    else coalesce(v_client.requires_hr,false) end;
  v_autoprocess_hr:=case when v_override then coalesce(v_contract.autoprocess_hr,false)
    else coalesce(v_client.autoprocess_hr,false) end;
  v_no_timesheet_required:=case when v_override then coalesce(v_contract.no_timesheet_required,false)
    else coalesce(v_client.no_timesheet_required,false) end;

  if v_is_nhsp and (v_requires_hr or v_autoprocess_hr or v_no_timesheet_required) then
    v_configuration_valid:=false;
    v_configuration_issue:='MULTIPLE_IMPORT_FAMILIES';
  elsif v_is_nhsp and v_workflow='DAILY' then
    v_configuration_valid:=false;
    v_configuration_issue:='NHSP_WEEKLY_WITH_DAILY_WORKFLOW';
  elsif v_no_timesheet_required and not v_autoprocess_hr then
    v_configuration_valid:=false;
    v_configuration_issue:='AUTHORITATIVE_ROSTER_WITHOUT_AUTOPROCESS';
  elsif v_requires_hr and not v_autoprocess_hr then
    v_configuration_valid:=false;
    v_configuration_issue:='ROSTER_VALIDATION_WITHOUT_AUTOPROCESS';
  elsif v_autoprocess_hr and not v_no_timesheet_required and not v_requires_hr then
    v_configuration_valid:=false;
    v_configuration_issue:='ROSTER_MODE_NOT_SELECTED';
  end if;

  v_route:=case
    when v_is_nhsp then 'DEDICATED_NHSP_WEEKLY'
    when v_autoprocess_hr and v_no_timesheet_required and v_workflow='DAILY' then 'HEALTHROSTER_DAILY_AUTHORITATIVE'
    when v_autoprocess_hr and v_no_timesheet_required then 'HEALTHROSTER_WEEKLY_AUTHORITATIVE'
    when v_autoprocess_hr and v_workflow='DAILY' then 'HEALTHROSTER_DAILY_VALIDATION'
    when v_autoprocess_hr then 'HEALTHROSTER_WEEKLY_VALIDATION'
    when v_workflow='DAILY' then 'STANDARD_DAILY'
    else 'STANDARD_WEEKLY' end;
  v_source:=case
    when v_unresolved_daily then 'UNRESOLVED_DAILY_SAFE'
    when v_override then 'CONTRACT_OVERRIDE'
    else 'CLIENT_SETTINGS' end;
  -- Historical contradictory rows fail safely: they can never grant Candidate
  -- hour entry merely because their old flags disagree.
  v_import_authoritative:=case
    when not v_configuration_valid then v_is_nhsp or v_no_timesheet_required
    else v_is_nhsp or (v_autoprocess_hr and v_no_timesheet_required)
  end;

  -- Self-billing controls expense separation, not Candidate hour-entry authority.
  v_self_bill:=case when v_override then coalesce(v_contract.self_bill,false)
    else coalesce(v_client.self_bill_no_invoices_sent,false) end;

  select coalesce(jsonb_agg(to_jsonb(h.date_value) order by h.date_value),'[]'::jsonb)
  into v_holidays
  from (
    select distinct btrim(x.date_value) date_value
    from (
      select jsonb_array_elements_text(
        case when jsonb_typeof(v_defaults.bh_list)='array' then v_defaults.bh_list else '[]'::jsonb end
      ) date_value
      union all
      select jsonb_array_elements_text(
        case when jsonb_typeof(v_defaults.bh_feed_list)='array' then v_defaults.bh_feed_list else '[]'::jsonb end
      ) date_value
    ) x
    where btrim(x.date_value) ~ '^\d{4}-\d{2}-\d{2}$'
  ) h;

  v_values:=jsonb_build_object(
    'timezone_id',coalesce(v_client.timezone_id,v_defaults.timezone_id,'Europe/London'),
    'day_start',coalesce(v_client.day_start,v_defaults.day_start),
    'day_end',coalesce(v_client.day_end,v_defaults.day_end),
    'night_start',coalesce(v_client.night_start,v_defaults.night_start),
    'night_end',coalesce(v_client.night_end,v_defaults.night_end),
    'sat_start',coalesce(v_client.sat_start,v_defaults.sat_start),
    'sat_end',coalesce(v_client.sat_end,v_defaults.sat_end),
    'sun_start',coalesce(v_client.sun_start,v_defaults.sun_start),
    'sun_end',coalesce(v_client.sun_end,v_defaults.sun_end),
    'bh_start',coalesce(v_client.bh_start,v_defaults.bh_start),
    'bh_end',coalesce(v_client.bh_end,v_defaults.bh_end),
    'bh_list',v_holidays,
    'vat_rate_pct',coalesce(v_client.vat_rate_pct,(v_finance->>'vat_rate_pct')::numeric,20),
    'holiday_pay_pct',coalesce(v_client.holiday_pay_pct,(v_finance->>'holiday_pay_pct')::numeric,12.07),
    'erni_pct',coalesce((v_finance->>'erni_pct')::numeric,13.8),
    'apply_holiday_to',coalesce(v_client.apply_holiday_to,v_finance->>'apply_holiday_to','PAYE_ONLY'),
    'apply_erni_to',coalesce(v_finance->>'apply_erni_to','PAYE_ONLY'),
    'margin_includes',coalesce(v_client.margin_includes,v_finance->'margin_includes','{}'::jsonb),
    'hr_validation_required',coalesce(v_client.hr_validation_required,false),
    'ts_reference_required',coalesce(v_client.ts_reference_required,v_defaults.ts_reference_required,false),
    'week_ending_weekday',coalesce(v_client.week_ending_weekday,
      case when p_contract_id is not null then v_contract.week_ending_weekday_snapshot end,0),
    'default_submission_mode',case when v_override then coalesce(v_contract.default_submission_mode,'ELECTRONIC')
      else coalesce(v_client.default_submission_mode,'ELECTRONIC') end,
    'is_nhsp',v_is_nhsp,
    'requires_hr',v_requires_hr,
    'autoprocess_hr',v_autoprocess_hr,
    'no_timesheet_required',v_no_timesheet_required
  )||jsonb_build_object(
    'hr_validation_required_for_invoice',case when v_override
      then coalesce(v_contract.requires_hr,false)
      else coalesce(v_client.hr_validation_required,false) end,
    'daily_calc_of_invoices',case when v_override then coalesce(v_contract.daily_calc_of_invoices,false)
      else coalesce(v_client.daily_calc_of_invoices,false) end,
    'group_nightsat_sunbh',case when v_override then coalesce(v_contract.group_nightsat_sunbh,false)
      else coalesce(v_client.group_nightsat_sunbh,false) end,
    'auto_invoice',case when v_override then coalesce(v_contract.auto_invoice,false)
      else coalesce(v_client.auto_invoice_default,false) end,
    'self_bill',v_self_bill,
    'require_reference_to_pay',case when v_override then coalesce(v_contract.require_reference_to_pay,false)
      else coalesce(v_client.pay_reference_required,false) end,
    'require_reference_to_invoice',case when v_override then coalesce(v_contract.require_reference_to_invoice,false)
      else coalesce(v_client.invoice_reference_required,false) end,
    'reference_number_required_to_issue_invoice',case when v_override
      then coalesce(v_contract.reference_number_required_to_issue_invoice,false)
      else coalesce(v_client.reference_number_required_to_issue_invoice,false) end,
    'hr_attach_to_invoice',case when v_override then coalesce(v_contract.hr_attach_to_invoice,true)
      else coalesce(v_client.hr_attach_to_invoice,v_defaults.hr_attach_to_invoice,true) end,
    'ts_attach_to_invoice',case when v_override then coalesce(v_contract.ts_attach_to_invoice,true)
      else coalesce(v_client.ts_attach_to_invoice,v_defaults.ts_attach_to_invoice,true) end,
    'send_manual_invoices_to_different_email',case when v_override
      then coalesce(v_contract.send_manual_invoices_to_different_email,false)
      else coalesce(v_client.send_manual_invoices_to_different_email,false) end,
    'manual_invoices_alt_email_address',case when v_override then v_contract.manual_invoices_alt_email_address
      else v_client.manual_invoices_alt_email_address end,
    'invoice_consolidation_mode',v_client.invoice_consolidation_mode,
    'healthroster_import_auto_authorise',case when v_unresolved_daily then false else
      coalesce(v_contract.healthroster_import_auto_authorise_override,
        v_client.healthroster_import_auto_authorise,
        v_defaults.healthroster_import_auto_authorise_default,false) end,
    'nhsp_import_auto_authorise',case when v_unresolved_daily then false else
      coalesce(v_contract.nhsp_import_auto_authorise_override,
        v_client.nhsp_import_auto_authorise,v_defaults.nhsp_import_auto_authorise_default,false) end,
    'candidate_electronic_auto_authorise',case when v_unresolved_daily then false else
      coalesce(v_contract.candidate_electronic_auto_authorise_override,
        v_client.candidate_electronic_auto_authorise,
        v_defaults.candidate_electronic_auto_authorise_default,false) end,
    'auto_authorise_on_validation',coalesce(v_defaults.auto_authorise_on_validation,false),
    'candidate_expenses_require_separate_timesheet',case when v_import_authoritative then true
      when v_self_bill then true
      else coalesce(v_contract.candidate_expenses_require_separate_timesheet_override,
        v_client.candidate_expenses_require_separate_timesheet,false) end,
    'candidate_paper_submission_enabled',case when v_import_authoritative then false
      else coalesce(v_contract.candidate_paper_submission_enabled_override,
        v_client.candidate_paper_submission_enabled,false) end,
    'candidate_expense_invoice_email',coalesce(v_contract.candidate_expense_invoice_email_override,
      v_client.candidate_expense_invoice_email),
    'candidate_manager_approval_policy_json',private._candidate_manager_authoriser_effective_v2(
      coalesce(v_client.candidate_manager_approval_policy_json,'{}'::jsonb),
      case when p_contract_id is null then null else v_contract.candidate_manager_approval_policy_json end
    ),
    'allow_daily_manager_authorise_on_phone',coalesce(v_client.allow_daily_manager_authorise_on_phone,true),
    'allow_daily_manager_authorise_by_email',coalesce(v_client.allow_daily_manager_authorise_by_email,false),
    'timesheet_break_entry_mode',case when v_import_authoritative then null
      when v_override then v_contract.timesheet_break_entry_mode else v_client.timesheet_break_entry_mode end,
    'weekly_timesheet_source',case when p_contract_id is null then null else v_contract.weekly_timesheet_source end,
    'rates_json',case when p_contract_id is null then null else v_contract.rates_json end,
    'additional_rates_json',case when p_contract_id is null then null else v_contract.additional_rates_json end,
    'mileage_pay_rate',case when p_contract_id is null then null else v_contract.mileage_pay_rate end,
    'mileage_charge_rate',case when p_contract_id is null then null else v_contract.mileage_charge_rate end,
    'mileage_pay_defaults',v_finance->'mileage_pay_defaults',
    'mileage_charge_defaults',v_finance->'mileage_charge_defaults',
    'bucket_labels_json',case when p_contract_id is null then null else v_contract.bucket_labels_json end,
    'client_vat_chargeable',coalesce(v_client_record.vat_chargeable,true),
    'client_payment_terms_days',v_client_record.payment_terms_days,
    'client_primary_invoice_email',v_client_record.primary_invoice_email
  );

  v_sources:=jsonb_build_object(
    'time_windows',case when v_unresolved_daily then 'GLOBAL' else 'CLIENT_THEN_GLOBAL' end,
    'bank_holiday_dates','GLOBAL_MANUAL_PLUS_GOV_UK_ENGLAND_AND_WALES',
    'bank_holiday_hours',case when v_unresolved_daily then 'GLOBAL' else 'CLIENT_THEN_GLOBAL' end,
    'erni_pct','GLOBAL_FINANCE_WINDOW',
    'apply_erni_to','GLOBAL_FINANCE_WINDOW',
    'finance_window',coalesce(v_finance->>'source','GLOBAL_FINANCE_WINDOW'),
    'contract_governed_settings',v_source,
    'healthroster_import_auto_authorise',case
      when v_unresolved_daily then 'UNRESOLVED_DAILY_SAFE'
      when p_contract_id is not null and v_contract.healthroster_import_auto_authorise_override is not null
        then 'CONTRACT'
      when v_client.healthroster_import_auto_authorise is not null then 'CLIENT'
      else 'GLOBAL' end,
    'nhsp_import_auto_authorise',case
      when v_unresolved_daily then 'UNRESOLVED_DAILY_SAFE'
      when p_contract_id is not null and v_contract.nhsp_import_auto_authorise_override is not null
        then 'CONTRACT'
      when v_client.nhsp_import_auto_authorise is not null then 'CLIENT'
      else 'GLOBAL' end,
    'candidate_electronic_auto_authorise',case
      when v_unresolved_daily then 'UNRESOLVED_DAILY_SAFE'
      when p_contract_id is not null and v_contract.candidate_electronic_auto_authorise_override is not null
        then 'CONTRACT'
      when v_client.candidate_electronic_auto_authorise is not null then 'CLIENT'
      else 'GLOBAL' end,
    'candidate_expenses_require_separate_timesheet',case
      when v_unresolved_daily then 'UNRESOLVED_DAILY_SAFE'
      when v_import_authoritative then 'IMPORT_MANDATORY'
      when v_self_bill then 'SELF_BILL_MANDATORY'
      when p_contract_id is not null
       and v_contract.candidate_expenses_require_separate_timesheet_override is not null then 'CONTRACT'
      else 'CLIENT' end,
    'candidate_paper_submission_enabled',case
      when v_unresolved_daily then 'UNRESOLVED_DAILY_SAFE'
      when v_import_authoritative then 'IMPORT_DISABLED'
      when p_contract_id is not null and v_contract.candidate_paper_submission_enabled_override is not null
        then 'CONTRACT'
      else 'CLIENT' end,
    'candidate_expense_invoice_email',case
      when v_unresolved_daily then 'UNRESOLVED_DAILY_SAFE'
      when p_contract_id is not null and v_contract.candidate_expense_invoice_email_override is not null
        then 'CONTRACT'
      else 'CLIENT' end,
    'candidate_manager_approval_policy_json',case
      when v_unresolved_daily then 'UNRESOLVED_DAILY_SAFE'
      when p_contract_id is not null and v_contract.candidate_manager_approval_policy_json is not null
        then 'CONTRACT'
      else 'CLIENT' end,
    'contract_rates','CONTRACT'
  );
  v_components:=jsonb_build_object(
    'healthroster_import_auto_authorise',jsonb_build_object(
      'global',v_defaults.healthroster_import_auto_authorise_default,
      'client',v_client.healthroster_import_auto_authorise,
      'contract_override',case when p_contract_id is null then null
        else v_contract.healthroster_import_auto_authorise_override end
    ),
    'nhsp_import_auto_authorise',jsonb_build_object(
      'global',v_defaults.nhsp_import_auto_authorise_default,
      'client',v_client.nhsp_import_auto_authorise,
      'contract_override',case when p_contract_id is null then null
        else v_contract.nhsp_import_auto_authorise_override end
    ),
    'candidate_electronic_auto_authorise',jsonb_build_object(
      'global',v_defaults.candidate_electronic_auto_authorise_default,
      'client',v_client.candidate_electronic_auto_authorise,
      'contract_override',case when p_contract_id is null then null
        else v_contract.candidate_electronic_auto_authorise_override end
    )
  );
  v_applicability:=jsonb_build_object(
    'configuration_valid',v_configuration_valid,
    'configuration_issue',v_configuration_issue,
    'import_authoritative',v_import_authoritative,
    'candidate_hours_view_only',v_import_authoritative,
    'candidate_expense_only_carrier_required',v_import_authoritative,
    'candidate_paper_timesheet',v_configuration_valid and not v_import_authoritative and v_workflow<>'DAILY',
    'timesheet_break_entry',v_configuration_valid and not v_import_authoritative and v_workflow<>'IMPORT',
    'invoice_settings',true,
    'finance_settings',true
  );
  v_payload:=jsonb_build_object(
    'authority_version','CONTRACT_SETTINGS_AUTHORITY_V1',
    'client_id',v_client_id,
    'contract_id',p_contract_id,
    'timesheet_id',p_timesheet_id,
    'relevant_date',p_relevant_date,
    'workflow',v_workflow,
    'client_settings_id',v_client.id,
    'client_settings_effective_from',v_client.effective_from,
    'client_settings_updated_at',v_client.updated_at,
    'global_settings_updated_at',v_defaults.updated_at,
    'finance_settings_id',nullif(v_finance->>'id','')::uuid,
    'finance_settings_date_from',nullif(v_finance->>'date_from','')::date,
    'contract_updated_at',case when p_contract_id is null then null else v_contract.updated_at end,
    'override_client_settings',v_override,
    'configured_route',v_route,
    'values',v_values,
    'sources',v_sources,
    'components',v_components,
    'applicability',v_applicability
  );
  v_fingerprint:=encode(digest(convert_to(v_payload::text,'UTF8'),'sha256'),'hex');
  return v_payload||jsonb_build_object(
    'authority_fingerprint',v_fingerprint,
    'resolved_at_utc',statement_timestamp()
  );
end
$function$;

alter function private._contract_settings_effective_core_v1(uuid,uuid,date,text,uuid) owner to postgres;
revoke all on function private._contract_settings_effective_core_v1(uuid,uuid,date,text,uuid)
  from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';
commit;
