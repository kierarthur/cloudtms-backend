\set ON_ERROR_STOP on

begin;

-- Real Timesheet authority belongs to the immutable Timesheet identity, not to
-- whether that identity is the current revision. Invoice correction validation
-- may inspect a historical member of a current correction chain, so the shared
-- frozen-reader verifies either revision without re-resolving live settings.
create or replace function private._timesheet_settings_authority_frozen_v1(
  p_timesheet_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_snapshot jsonb; v_version text; v_fingerprint text; v_expected text;
begin
  select t.settings_authority_json,t.settings_authority_version,
    t.settings_authority_fingerprint
  into v_snapshot,v_version,v_fingerprint
  from public.timesheets t
  where t.timesheet_id=p_timesheet_id;
  if not found then
    raise exception 'CONTRACT_SETTINGS_TIMESHEET_NOT_FOUND' using errcode='P0002';
  end if;
  if coalesce(v_snapshot,'{}'::jsonb)='{}'::jsonb
     or v_version is distinct from 'CONTRACT_SETTINGS_AUTHORITY_V1'
     or coalesce(v_fingerprint,'') !~ '^[0-9a-f]{64}$' then
    raise exception 'CONTRACT_SETTINGS_TIMESHEET_AUTHORITY_NOT_FROZEN' using errcode='55000';
  end if;
  v_expected:=encode(digest(convert_to(
    (v_snapshot-'authority_fingerprint'-'resolved_at_utc')::text,'UTF8'
  ),'sha256'),'hex');
  if v_expected is distinct from v_fingerprint
     or v_snapshot->>'authority_fingerprint' is distinct from v_fingerprint then
    raise exception 'CONTRACT_SETTINGS_TIMESHEET_AUTHORITY_INVALID' using errcode='22023';
  end if;
  return v_snapshot;
end
$function$;

alter function private._timesheet_settings_authority_frozen_v1(uuid) owner to postgres;
revoke all on function private._timesheet_settings_authority_frozen_v1(uuid)
  from public,anon,authenticated,service_role;

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
    'self_bill',case when v_override then coalesce(v_contract.self_bill,false)
      else coalesce(v_client.self_bill_no_invoices_sent,false) end,
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

create or replace function public.contract_settings_effective_get_v1(
  p_client_id uuid,
  p_contract_id uuid,
  p_relevant_date date,
  p_workflow text,
  p_timesheet_id uuid default null
)
returns jsonb
language sql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
  select private._contract_settings_effective_core_v1(
    p_client_id,p_contract_id,p_relevant_date,p_workflow,p_timesheet_id
  )
$function$;

alter function public.contract_settings_effective_get_v1(uuid,uuid,date,text,uuid) owner to postgres;
revoke all on function public.contract_settings_effective_get_v1(uuid,uuid,date,text,uuid)
  from public,anon,authenticated;
grant execute on function public.contract_settings_effective_get_v1(uuid,uuid,date,text,uuid)
  to service_role;

comment on function public.contract_settings_effective_get_v1(uuid,uuid,date,text,uuid) is
  'Service-only dated Client/Contract settings resolver. Derivation authority only: it does not change downstream calculation, invoice, payment or Banking policy.';

commit;

begin;

create or replace function public.settings_bank_holiday_feed_refresh_claim_v1(
  p_now_utc timestamptz default statement_timestamp()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, extensions, pg_temp
as $function$
declare v_token uuid:=gen_random_uuid(); v_row public.settings_defaults%rowtype;
begin
  update public.settings_defaults set
    bh_feed_claim_token=v_token,
    bh_feed_claimed_at=p_now_utc,
    bh_feed_last_checked_at=p_now_utc,
    bh_feed_next_refresh_at=p_now_utc+interval '15 minutes',
    bh_feed_last_error_code=null,
    updated_at=p_now_utc
  where id=1
    and coalesce(bh_feed_next_refresh_at,'-infinity'::timestamptz)<=p_now_utc
    and (bh_feed_claimed_at is null or bh_feed_claimed_at<=p_now_utc-interval '15 minutes')
  returning * into v_row;
  if not found then return jsonb_build_object('claimed',false); end if;
  return jsonb_build_object(
    'claimed',true,'claim_token',v_token,
    'division','england-and-wales',
    'source_url','https://www.gov.uk/bank-holidays.json'
  );
end
$function$;

create or replace function public.settings_bank_holiday_feed_refresh_complete_v1(
  p_claim_token uuid,
  p_dates jsonb,
  p_content_sha256 text,
  p_now_utc timestamptz default statement_timestamp()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, extensions, pg_temp
as $function$
declare v_dates jsonb; v_count integer;
begin
  if p_claim_token is null or jsonb_typeof(p_dates)<>'array'
     or jsonb_array_length(p_dates) not between 1 and 512
     or coalesce(p_content_sha256,'') !~ '^[0-9a-f]{64}$' then
    raise exception 'BANK_HOLIDAY_FEED_RESULT_INVALID' using errcode='22023';
  end if;
  if exists(
    select 1 from jsonb_array_elements_text(p_dates) d(value)
    where btrim(d.value) !~ '^\d{4}-\d{2}-\d{2}$'
       or to_char(to_date(btrim(d.value),'YYYY-MM-DD'),'YYYY-MM-DD')<>btrim(d.value)
  ) then
    raise exception 'BANK_HOLIDAY_FEED_DATE_INVALID' using errcode='22023';
  end if;
  select jsonb_agg(to_jsonb(x.value) order by x.value),count(*)
  into v_dates,v_count
  from (select distinct btrim(d.value) value from jsonb_array_elements_text(p_dates) d(value)) x;
  update public.settings_defaults set
    bh_feed_list=v_dates,
    bh_feed_last_success_at=p_now_utc,
    bh_feed_next_refresh_at=p_now_utc+interval '7 days',
    bh_feed_claim_token=null,
    bh_feed_claimed_at=null,
    bh_feed_content_sha256=p_content_sha256,
    bh_feed_last_error_code=null,
    updated_at=p_now_utc
  where id=1 and bh_feed_claim_token=p_claim_token;
  if not found then
    raise exception 'BANK_HOLIDAY_FEED_CLAIM_STALE' using errcode='40001';
  end if;
  return jsonb_build_object('ok',true,'date_count',v_count,'next_refresh_at',p_now_utc+interval '7 days');
end
$function$;

create or replace function public.settings_bank_holiday_feed_refresh_fail_v1(
  p_claim_token uuid,
  p_error_code text,
  p_now_utc timestamptz default statement_timestamp()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, pg_temp
as $function$
begin
  if p_claim_token is null or nullif(btrim(p_error_code),'') is null
     or char_length(btrim(p_error_code))>80 then
    raise exception 'BANK_HOLIDAY_FEED_FAILURE_INVALID' using errcode='22023';
  end if;
  update public.settings_defaults set
    bh_feed_next_refresh_at=p_now_utc+interval '1 hour',
    bh_feed_claim_token=null,
    bh_feed_claimed_at=null,
    bh_feed_last_error_code=upper(btrim(p_error_code)),
    updated_at=p_now_utc
  where id=1 and bh_feed_claim_token=p_claim_token;
  if not found then return jsonb_build_object('ok',false,'stale_claim',true); end if;
  return jsonb_build_object('ok',true,'retry_at',p_now_utc+interval '1 hour');
end
$function$;

alter function public.settings_bank_holiday_feed_refresh_claim_v1(timestamptz) owner to postgres;
alter function public.settings_bank_holiday_feed_refresh_complete_v1(uuid,jsonb,text,timestamptz) owner to postgres;
alter function public.settings_bank_holiday_feed_refresh_fail_v1(uuid,text,timestamptz) owner to postgres;
revoke all on function public.settings_bank_holiday_feed_refresh_claim_v1(timestamptz) from public,anon,authenticated;
revoke all on function public.settings_bank_holiday_feed_refresh_complete_v1(uuid,jsonb,text,timestamptz) from public,anon,authenticated;
revoke all on function public.settings_bank_holiday_feed_refresh_fail_v1(uuid,text,timestamptz) from public,anon,authenticated;
grant execute on function public.settings_bank_holiday_feed_refresh_claim_v1(timestamptz) to service_role;
grant execute on function public.settings_bank_holiday_feed_refresh_complete_v1(uuid,jsonb,text,timestamptz) to service_role;
grant execute on function public.settings_bank_holiday_feed_refresh_fail_v1(uuid,text,timestamptz) to service_role;

create or replace function private._timesheet_break_entry_precedence_v1(
  p_client_id uuid,
  p_contract_id uuid default null,
  p_as_of_date date default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_authority jsonb; v_date date:=p_as_of_date;
begin
  if v_date is null then
    raise exception 'BREAK_ENTRY_AS_OF_DATE_REQUIRED' using errcode='22023';
  end if;
  if not exists(select 1 from public.clients cl where cl.id=p_client_id)
     or not exists(
       select 1 from public.client_settings cs
       where cs.client_id=p_client_id
         and (cs.effective_from is null or cs.effective_from<=v_date)
     ) then
    raise exception 'CLIENT_OR_SETTINGS_NOT_FOUND' using errcode='P0002';
  end if;
  v_authority:=private._contract_settings_effective_core_v1(
    p_client_id,p_contract_id,v_date,'WEEKLY',null
  );
  if v_authority->>'client_settings_id' is null then
    raise exception 'CLIENT_OR_SETTINGS_NOT_FOUND' using errcode='P0002';
  end if;
  return jsonb_build_object(
    'mode',v_authority#>'{values,timesheet_break_entry_mode}',
    'source',case when coalesce((v_authority->>'override_client_settings')::boolean,false)
      then 'CONTRACT_OVERRIDE' else 'CLIENT_SETTINGS' end,
    'settings_as_of_date',v_date,
    'client_settings_id',v_authority->>'client_settings_id',
    'contract_id',p_contract_id,
    'contract_override_active',coalesce((v_authority->>'override_client_settings')::boolean,false),
    'import_authoritative',coalesce((v_authority#>>'{applicability,import_authoritative}')::boolean,false),
    'is_nhsp',coalesce((v_authority#>>'{values,is_nhsp}')::boolean,false),
    'autoprocess_hr',coalesce((v_authority#>>'{values,autoprocess_hr}')::boolean,false),
    'no_timesheet_required',coalesce((v_authority#>>'{values,no_timesheet_required}')::boolean,false),
    'authority_version',v_authority->>'authority_version',
    'authority_fingerprint',v_authority->>'authority_fingerprint'
  );
end
$function$;

alter function private._timesheet_break_entry_precedence_v1(uuid,uuid,date) owner to postgres;
revoke all on function private._timesheet_break_entry_precedence_v1(uuid,uuid,date)
  from public,anon,authenticated,service_role;

commit;

begin;

create or replace function private._contract_settings_apply_week_snapshot_v1(
  p_week_id uuid
)
returns boolean
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_snapshot jsonb;
  v_fingerprint text;
begin
  select * into v_week from public.contract_weeks where id=p_week_id for update;
  if not found or v_week.timesheet_id is not null then return false; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id;
  if not found then raise exception 'CONTRACT_SETTINGS_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
  v_snapshot:=private._contract_settings_effective_core_v1(
    v_contract.client_id,v_contract.id,v_week.week_ending_date,'WEEKLY',null
  );
  v_fingerprint:=v_snapshot->>'authority_fingerprint';
  if v_week.settings_authority_fingerprint is not distinct from v_fingerprint then
    return false;
  end if;
  update public.contract_weeks set
    submission_mode_snapshot=case
      when coalesce(v_week.is_adjustment,false) or coalesce(v_week.additional_seq,0)>0
        then v_week.submission_mode_snapshot
      else coalesce(
        v_snapshot#>>'{values,default_submission_mode}',
        'ELECTRONIC'
      )::public.submission_mode_enum
    end,
    settings_authority_json=v_snapshot,
    settings_authority_version='CONTRACT_SETTINGS_AUTHORITY_V1',
    settings_authority_fingerprint=v_fingerprint,
    settings_authority_resolved_at=(v_snapshot->>'resolved_at_utc')::timestamptz,
    updated_at=statement_timestamp()
  where id=v_week.id and timesheet_id is null;
  return found;
end
$function$;

alter function private._contract_settings_apply_week_snapshot_v1(uuid) owner to postgres;
revoke all on function private._contract_settings_apply_week_snapshot_v1(uuid)
  from public,anon,authenticated,service_role;

create or replace function private._contract_settings_refresh_planned_for_contract_v1(
  p_contract_id uuid
)
returns integer
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_row record; v_count integer:=0;
begin
  for v_row in
    select cw.id from public.contract_weeks cw
    where cw.contract_id=p_contract_id and cw.timesheet_id is null
    order by cw.week_ending_date,cw.id
  loop
    if private._contract_settings_apply_week_snapshot_v1(v_row.id) then
      v_count:=v_count+1;
    end if;
  end loop;
  return v_count;
end
$function$;

create or replace function private._contract_settings_refresh_planned_for_client_v1(
  p_client_id uuid
)
returns integer
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_row record; v_count integer:=0;
begin
  for v_row in
    select cw.id
    from public.contract_weeks cw
    join public.contracts c on c.id=cw.contract_id
    where c.client_id=p_client_id
      and not coalesce(c.overrideclientsettings,false)
      and cw.timesheet_id is null
    order by c.id,cw.week_ending_date,cw.id
  loop
    if private._contract_settings_apply_week_snapshot_v1(v_row.id) then
      v_count:=v_count+1;
    end if;
  end loop;
  return v_count;
end
$function$;

create or replace function private._contract_settings_refresh_all_planned_v1()
returns integer
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_row record; v_count integer:=0;
begin
  for v_row in
    select cw.id from public.contract_weeks cw
    where cw.timesheet_id is null
    order by cw.contract_id,cw.week_ending_date,cw.id
  loop
    if private._contract_settings_apply_week_snapshot_v1(v_row.id) then
      v_count:=v_count+1;
    end if;
  end loop;
  return v_count;
end
$function$;

alter function private._contract_settings_refresh_planned_for_contract_v1(uuid) owner to postgres;
alter function private._contract_settings_refresh_planned_for_client_v1(uuid) owner to postgres;
alter function private._contract_settings_refresh_all_planned_v1() owner to postgres;
revoke all on function private._contract_settings_refresh_planned_for_contract_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private._contract_settings_refresh_planned_for_client_v1(uuid) from public,anon,authenticated,service_role;
revoke all on function private._contract_settings_refresh_all_planned_v1() from public,anon,authenticated,service_role;

create or replace function private._contract_week_settings_authority_before_v1()
returns trigger
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_contract public.contracts%rowtype; v_snapshot jsonb;
begin
  if tg_op='UPDATE' and old.timesheet_id is not null then
    new.settings_authority_json:=old.settings_authority_json;
    new.settings_authority_version:=old.settings_authority_version;
    new.settings_authority_fingerprint:=old.settings_authority_fingerprint;
    new.settings_authority_resolved_at:=old.settings_authority_resolved_at;
    return new;
  end if;
  if new.timesheet_id is not null and tg_op='UPDATE'
     and old.settings_authority_json<>'{}'::jsonb then
    new.settings_authority_json:=old.settings_authority_json;
    new.settings_authority_version:=old.settings_authority_version;
    new.settings_authority_fingerprint:=old.settings_authority_fingerprint;
    new.settings_authority_resolved_at:=old.settings_authority_resolved_at;
    return new;
  end if;
  select * into v_contract from public.contracts c where c.id=new.contract_id;
  if not found then return new; end if;
  v_snapshot:=private._contract_settings_effective_core_v1(
    v_contract.client_id,v_contract.id,new.week_ending_date,'WEEKLY',null
  );
  if not coalesce(new.is_adjustment,false) and coalesce(new.additional_seq,0)=0 then
    new.submission_mode_snapshot:=coalesce(
      v_snapshot#>>'{values,default_submission_mode}',
      'ELECTRONIC'
    )::public.submission_mode_enum;
  end if;
  new.settings_authority_json:=v_snapshot;
  new.settings_authority_version:='CONTRACT_SETTINGS_AUTHORITY_V1';
  new.settings_authority_fingerprint:=v_snapshot->>'authority_fingerprint';
  new.settings_authority_resolved_at:=(v_snapshot->>'resolved_at_utc')::timestamptz;
  return new;
end
$function$;

create or replace function private._timesheet_settings_authority_before_v1()
returns trigger
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_contract public.contracts%rowtype;
  v_snapshot jsonb;
  v_client_id uuid;
  v_relevant_date date;
begin
  if tg_op='UPDATE' and old.settings_authority_json<>'{}'::jsonb then
    new.settings_authority_json:=old.settings_authority_json;
    new.settings_authority_version:=old.settings_authority_version;
    new.settings_authority_fingerprint:=old.settings_authority_fingerprint;
    new.settings_authority_resolved_at:=old.settings_authority_resolved_at;
    return new;
  end if;
  if new.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum then
    if tg_op='INSERT' then
      new.settings_authority_json:='{}'::jsonb;
      new.settings_authority_version:=null;
      new.settings_authority_fingerprint:=null;
      new.settings_authority_resolved_at:=null;
      return new;
    end if;
    select tf.client_id into v_client_id
    from public.timesheets_financials tf
    where tf.timesheet_id=new.timesheet_id and tf.is_current=true
      and tf.processed_at_utc is not null
    order by tf.computed_at_utc desc nulls last,tf.updated_at desc,tf.id desc
    limit 1;
    if not found then
      new.settings_authority_json:='{}'::jsonb;
      new.settings_authority_version:=null;
      new.settings_authority_fingerprint:=null;
      new.settings_authority_resolved_at:=null;
      return new;
    end if;
    v_relevant_date:=coalesce(
      (new.worked_start_iso at time zone 'Europe/London')::date,
      (new.scheduled_start_iso at time zone 'Europe/London')::date,
      new.week_ending_date
    );
    v_snapshot:=private._contract_settings_effective_core_v1(
      v_client_id,new.contract_id,v_relevant_date,'DAILY',null
    );
    new.settings_authority_json:=v_snapshot;
    new.settings_authority_version:='CONTRACT_SETTINGS_AUTHORITY_V1';
    new.settings_authority_fingerprint:=v_snapshot->>'authority_fingerprint';
    new.settings_authority_resolved_at:=(v_snapshot->>'resolved_at_utc')::timestamptz;
    return new;
  end if;
  if new.contract_id is null then
    new.settings_authority_json:='{}'::jsonb;
    new.settings_authority_version:=null;
    new.settings_authority_fingerprint:=null;
    new.settings_authority_resolved_at:=null;
    return new;
  end if;
  select * into v_contract from public.contracts c where c.id=new.contract_id;
  if not found then return new; end if;
  v_snapshot:=private._contract_settings_effective_core_v1(
    v_contract.client_id,v_contract.id,new.week_ending_date,'WEEKLY',null
  );
  new.settings_authority_json:=v_snapshot;
  new.settings_authority_version:='CONTRACT_SETTINGS_AUTHORITY_V1';
  new.settings_authority_fingerprint:=v_snapshot->>'authority_fingerprint';
  new.settings_authority_resolved_at:=(v_snapshot->>'resolved_at_utc')::timestamptz;
  return new;
end
$function$;

create or replace function private._daily_timesheet_settings_authority_after_fin_v1()
returns trigger
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_timesheet public.timesheets%rowtype; v_snapshot jsonb; v_date date;
begin
  if new.processed_at_utc is null then return new; end if;
  select * into v_timesheet from public.timesheets t
  where t.timesheet_id=new.timesheet_id and t.is_current=true
  for update;
  if not found or v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum
     or v_timesheet.settings_authority_json<>'{}'::jsonb then return new; end if;
  v_date:=coalesce(
    (v_timesheet.worked_start_iso at time zone 'Europe/London')::date,
    (v_timesheet.scheduled_start_iso at time zone 'Europe/London')::date,
    v_timesheet.week_ending_date
  );
  v_snapshot:=private._contract_settings_effective_core_v1(
    new.client_id,v_timesheet.contract_id,v_date,'DAILY',null
  );
  update public.timesheets set
    settings_authority_json=v_snapshot,
    settings_authority_version='CONTRACT_SETTINGS_AUTHORITY_V1',
    settings_authority_fingerprint=v_snapshot->>'authority_fingerprint',
    settings_authority_resolved_at=(v_snapshot->>'resolved_at_utc')::timestamptz
  where timesheet_id=v_timesheet.timesheet_id and settings_authority_json='{}'::jsonb;
  return new;
end
$function$;

create or replace function private._contract_settings_after_contract_v1()
returns trigger language plpgsql volatile security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_first_settings_id uuid;
  v_first_effective_from date;
  v_earliest_contract_date date;
begin
  -- Client creation writes its initial settings row before any Contract exists.
  -- If the first Contract is deliberately backdated, extend that first row
  -- backwards so every Contract date has a real Client-settings authority.
  -- Never move the row forwards and never rewrite any of its setting values.
  select min(c.start_date) into v_earliest_contract_date
  from public.contracts c
  where c.client_id=new.client_id and c.start_date is not null;

  select cs.id,cs.effective_from
  into v_first_settings_id,v_first_effective_from
  from public.client_settings cs
  where cs.client_id=new.client_id
  order by cs.effective_from asc nulls first,cs.created_at asc,cs.id asc
  limit 1;

  if v_first_settings_id is not null
     and v_first_effective_from is not null
     and v_earliest_contract_date is not null
     and v_first_effective_from>v_earliest_contract_date then
    update public.client_settings
    set effective_from=v_earliest_contract_date
    where id=v_first_settings_id and effective_from=v_first_effective_from;
  end if;

  perform private._contract_settings_refresh_planned_for_contract_v1(new.id);
  return new;
end
$function$;

create or replace function private._contract_settings_after_client_v1()
returns trigger language plpgsql volatile security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
begin
  if tg_op='DELETE' then
    perform private._contract_settings_refresh_planned_for_client_v1(old.client_id);
    return old;
  end if;
  perform private._contract_settings_refresh_planned_for_client_v1(new.client_id);
  return new;
end
$function$;

create or replace function private._contract_settings_after_global_v1()
returns trigger language plpgsql volatile security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
begin
  perform private._contract_settings_refresh_all_planned_v1();
  if tg_op='DELETE' then return old; end if;
  return new;
end
$function$;

drop trigger if exists contract_week_settings_authority_before_v1 on public.contract_weeks;
create trigger contract_week_settings_authority_before_v1
before insert or update of contract_id,week_ending_date,timesheet_id,
  settings_authority_json,settings_authority_version,
  settings_authority_fingerprint,settings_authority_resolved_at
on public.contract_weeks
for each row execute function private._contract_week_settings_authority_before_v1();

drop trigger if exists timesheet_settings_authority_before_v1 on public.timesheets;
create trigger timesheet_settings_authority_before_v1
before insert or update of contract_id,week_ending_date,sheet_scope,
  settings_authority_json,settings_authority_version,
  settings_authority_fingerprint,settings_authority_resolved_at
on public.timesheets
for each row execute function private._timesheet_settings_authority_before_v1();

drop trigger if exists daily_timesheet_settings_authority_after_fin_v1 on public.timesheets_financials;
create trigger daily_timesheet_settings_authority_after_fin_v1
after insert or update of processed_at_utc on public.timesheets_financials
for each row execute function private._daily_timesheet_settings_authority_after_fin_v1();

drop trigger if exists contract_settings_authority_after_contract_insert_v1 on public.contracts;
create trigger contract_settings_authority_after_contract_insert_v1
after insert on public.contracts for each row
execute function private._contract_settings_after_contract_v1();
drop trigger if exists contract_settings_authority_after_contract_update_v1 on public.contracts;
create trigger contract_settings_authority_after_contract_update_v1
after update of client_id,start_date,overrideclientsettings,no_timesheet_required,daily_calc_of_invoices,
  group_nightsat_sunbh,is_nhsp,autoprocess_hr,requires_hr,hr_attach_to_invoice,
  ts_attach_to_invoice,reference_number_required_to_issue_invoice,
  send_manual_invoices_to_different_email,manual_invoices_alt_email_address,
  default_submission_mode,timesheet_break_entry_mode,auto_invoice,require_reference_to_pay,
  require_reference_to_invoice,self_bill,healthroster_import_auto_authorise_override,
  nhsp_import_auto_authorise_override,candidate_electronic_auto_authorise_override,
  candidate_expenses_require_separate_timesheet_override,
  candidate_paper_submission_enabled_override,candidate_expense_invoice_email_override,
  candidate_manager_approval_policy_json,rates_json,additional_rates_json,
  mileage_pay_rate,mileage_charge_rate
on public.contracts for each row
execute function private._contract_settings_after_contract_v1();

drop trigger if exists contract_settings_authority_after_client_v1 on public.client_settings;
create trigger contract_settings_authority_after_client_v1
after insert or update or delete on public.client_settings for each row
execute function private._contract_settings_after_client_v1();

drop trigger if exists contract_settings_authority_after_global_defaults_v1 on public.settings_defaults;
create trigger contract_settings_authority_after_global_defaults_v1
after update of timezone_id,day_start,day_end,night_start,night_end,sat_start,sat_end,
  sun_start,sun_end,bh_start,bh_end,bh_list,bh_feed_list,hr_attach_to_invoice,
  ts_attach_to_invoice,ts_reference_required
on public.settings_defaults for each row
execute function private._contract_settings_after_global_v1();

drop trigger if exists contract_settings_authority_after_finance_window_v1 on public.settings_finance_windows;
create trigger contract_settings_authority_after_finance_window_v1
after insert or update or delete on public.settings_finance_windows for each statement
execute function private._contract_settings_after_global_v1();

commit;

begin;

create or replace function private._candidate_import_authoritative_v1(
  p_client_id uuid,
  p_contract_id uuid default null,
  p_timesheet_id uuid default null,
  p_snapshot_json jsonb default null,
  p_evaluation_date date default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_date date:=p_evaluation_date;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_fin jsonb:=coalesce(p_snapshot_json,'{}'::jsonb);
  v_authority jsonb;
  v_config_import boolean:=false;
  v_snapshot_import boolean:=false;
  v_has_external_source_rows boolean:=false;
  v_route_import boolean:=false;
  v_route_type text:='';
  v_route_no_timesheet_required boolean:=false;
  v_source text:='NONE';
  v_workflow text:='WEEKLY';
begin
  if p_client_id is null then
    raise exception 'CANDIDATE_IMPORT_CLIENT_REQUIRED' using errcode='22023';
  end if;
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets t where t.timesheet_id=p_timesheet_id;
    if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
    v_date:=coalesce(v_date,v_timesheet.week_ending_date);
    v_workflow:=case when v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum
      then 'DAILY' else 'WEEKLY' end;
  end if;
  if p_contract_id is not null then
    select * into v_contract from public.contracts c
    where c.id=p_contract_id and c.client_id=p_client_id;
    if not found then raise exception 'CANDIDATE_IMPORT_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
    v_date:=coalesce(v_date,v_contract.start_date);
  end if;
  if v_date is null then
    raise exception 'CANDIDATE_IMPORT_EVALUATION_DATE_REQUIRED' using errcode='22023';
  end if;
  v_authority:=private._contract_settings_effective_core_v1(
    p_client_id,p_contract_id,v_date,v_workflow,p_timesheet_id
  );
  v_config_import:=coalesce((v_authority#>>'{applicability,import_authoritative}')::boolean,false);

  if p_snapshot_json is null and p_timesheet_id is not null then
    select to_jsonb(tf) into v_fin from public.timesheets_financials tf
    where tf.timesheet_id=p_timesheet_id and tf.is_current=true
    order by tf.computed_at_utc desc nulls last,tf.updated_at desc,tf.id desc limit 1;
    v_fin:=coalesce(v_fin,'{}'::jsonb);
  end if;
  if p_timesheet_id is not null then
    begin
      select upper(coalesce(summary.route_type,'')),
        coalesce(summary.client_no_timesheet_required,false)
      into v_route_type,v_route_no_timesheet_required
      from public.v_timesheets_summary summary
      where summary.timesheet_id=p_timesheet_id limit 1;
    exception when others then
      v_route_type:=''; v_route_no_timesheet_required:=false;
    end;
  end if;
  v_route_import:=v_route_type in ('WEEKLY_NHSP','WEEKLY_NHSP_ADJUSTMENT')
    or (v_route_type='WEEKLY_HEALTHROSTER' and v_route_no_timesheet_required);
  v_has_external_source_rows:=case
    when jsonb_typeof(v_fin->'external_source_rows_json')='array'
      then jsonb_array_length(v_fin->'external_source_rows_json')>0
    when jsonb_typeof(v_fin->'external_source_rows_json')='object'
      then v_fin->'external_source_rows_json'<>'{}'::jsonb
    else false end;
  v_snapshot_import:=nullif(v_fin->>'nhsp_import_id','') is not null
    or v_has_external_source_rows
    or upper(coalesce(v_fin->>'basis','')) in (
      'NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_SELF_BILL_ADJUSTMENT',
      'HEALTHROSTER_ADJUSTMENT','HEALTHROSTER_WEEKLY','HEALTHROSTER_WEEKLY_ADJUSTMENT'
    );
  v_source:=case
    when v_config_import then 'CONFIG_'||coalesce(v_authority->>'configured_route','IMPORT_AUTHORITATIVE')
    when v_route_import then 'ROUTE_'||v_route_type
    when nullif(v_fin->>'nhsp_import_id','') is not null then 'NHSP_IMPORT_SNAPSHOT'
    when v_has_external_source_rows then 'EXTERNAL_SOURCE_SNAPSHOT'
    when v_snapshot_import then 'IMPORT_BASIS_SNAPSHOT'
    else 'NONE' end;
  return jsonb_build_object(
    'is_import_authoritative',v_config_import or v_route_import or v_snapshot_import,
    'source_family',v_source,
    'candidate_hours_view_only',v_config_import or v_route_import or v_snapshot_import,
    'mandatory_expense_separation',v_config_import or v_route_import or v_snapshot_import,
    'authority_version',v_authority->>'authority_version',
    'authority_fingerprint',v_authority->>'authority_fingerprint',
    'relevant_date',v_date
  );
end
$function$;

alter function private._candidate_import_authoritative_v1(uuid,uuid,uuid,jsonb,date) owner to postgres;
revoke all on function private._candidate_import_authoritative_v1(uuid,uuid,uuid,jsonb,date)
  from public,anon,authenticated,service_role;

create or replace function public._import_review_effective_authority_core_v1(
  p_source_route text,
  p_contract_id uuid,
  p_client_id uuid,
  p_evidence_date date default null
)
returns table (
  route_eligible boolean,
  validation_eligible boolean,
  import_authoritative boolean,
  authority_mode text,
  authority_basis text,
  effective_is_nhsp boolean,
  effective_autoprocess_hr boolean,
  effective_requires_hr boolean,
  effective_no_timesheet_required boolean,
  settings_as_of_date date,
  client_settings_id uuid,
  client_settings_effective_from date,
  client_settings_updated_at timestamptz,
  contract_updated_at timestamptz,
  authority_fingerprint text
)
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_route text:=upper(btrim(coalesce(p_source_route,'')));
  v_workflow text;
  v_authority jsonb;
  v_is_nhsp boolean;
  v_requires_hr boolean;
  v_autoprocess_hr boolean;
  v_no_timesheet boolean;
  v_route_ok boolean;
  v_validation boolean;
  v_authoritative boolean;
begin
  if p_evidence_date is null then
    raise exception 'IMPORT_AUTHORITY_EVIDENCE_DATE_REQUIRED' using errcode='22023';
  end if;
  v_workflow:=case when v_route in ('HR_DAILY','HEALTHROSTER_DAILY') then 'DAILY' else 'IMPORT' end;
  v_authority:=private._contract_settings_effective_core_v1(
    p_client_id,p_contract_id,p_evidence_date,v_workflow,null
  );
  v_is_nhsp:=coalesce((v_authority#>>'{values,is_nhsp}')::boolean,false);
  v_requires_hr:=coalesce((v_authority#>>'{values,requires_hr}')::boolean,false);
  v_autoprocess_hr:=coalesce((v_authority#>>'{values,autoprocess_hr}')::boolean,false);
  v_no_timesheet:=coalesce((v_authority#>>'{values,no_timesheet_required}')::boolean,false);
  v_route_ok:=case
    when v_route='NHSP' then v_is_nhsp
    when v_route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY','HR_DAILY','HEALTHROSTER_DAILY')
      then v_autoprocess_hr and (v_requires_hr or v_no_timesheet)
    else false end;
  v_validation:=case
    when v_route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY','HR_DAILY','HEALTHROSTER_DAILY')
      then v_requires_hr and v_autoprocess_hr and not v_no_timesheet
    else false end;
  v_authoritative:=case
    when v_route='NHSP' then v_is_nhsp
    when v_route in ('HR_WEEKLY','HEALTHROSTER','HEALTHROSTER_WEEKLY','HR_DAILY','HEALTHROSTER_DAILY')
      then v_autoprocess_hr and v_no_timesheet
    else false end;
  return query select
    v_route_ok,v_validation,v_authoritative,
    case when not v_route_ok then 'OUT_OF_SCOPE'
      when v_authoritative then 'AUTHORITATIVE'
      when v_validation then 'VALIDATION_ONLY' else 'OUT_OF_SCOPE' end,
    case when coalesce((v_authority->>'override_client_settings')::boolean,false)
      then 'CONTRACT_OVERRIDE' else 'CLIENT_SETTINGS' end,
    v_is_nhsp,v_autoprocess_hr,v_requires_hr,v_no_timesheet,
    p_evidence_date,
    nullif(v_authority->>'client_settings_id','')::uuid,
    nullif(v_authority->>'client_settings_effective_from','')::date,
    nullif(v_authority->>'client_settings_updated_at','')::timestamptz,
    nullif(v_authority->>'contract_updated_at','')::timestamptz,
    v_authority->>'authority_fingerprint';
end
$function$;

alter function public._import_review_effective_authority_core_v1(text,uuid,uuid,date) owner to postgres;
revoke all on function public._import_review_effective_authority_core_v1(text,uuid,uuid,date)
  from public,anon,authenticated,service_role;

create or replace function private._candidate_submission_mode_v1(
  p_client_id uuid,
  p_contract_id uuid,
  p_as_of_date date
)
returns public.submission_mode_enum
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_authority jsonb;
begin
  v_authority:=private._contract_settings_effective_core_v1(
    p_client_id,p_contract_id,p_as_of_date,'WEEKLY',null
  );
  return coalesce(
    nullif(v_authority#>>'{values,default_submission_mode}','')::public.submission_mode_enum,
    'ELECTRONIC'::public.submission_mode_enum
  );
end
$function$;

alter function private._candidate_submission_mode_v1(uuid,uuid,date) owner to postgres;
revoke all on function private._candidate_submission_mode_v1(uuid,uuid,date)
  from public,anon,authenticated,service_role;

create or replace function private._candidate_policy_resolve_v1(
  p_client_id uuid,
  p_contract_id uuid default null,
  p_evaluation_date date default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_date date:=coalesce(p_evaluation_date,(transaction_timestamp() at time zone 'Europe/London')::date);
  v_authority jsonb;
  v_values jsonb;
  v_sources jsonb;
  v_global public.settings_defaults%rowtype;
  v_email text;
  v_email_ready boolean;
  v_result jsonb;
begin
  if p_client_id is null then
    raise exception 'CANDIDATE_POLICY_CLIENT_REQUIRED' using errcode='22023';
  end if;
  v_authority:=private._contract_settings_effective_core_v1(
    p_client_id,p_contract_id,v_date,'WEEKLY',null
  );
  v_values:=v_authority->'values';
  v_sources:=v_authority->'sources';
  select * into v_global from public.settings_defaults where id=1;
  if not found then
    raise exception 'CANDIDATE_GLOBAL_SETTINGS_MISSING' using errcode='55000';
  end if;
  v_email:=nullif(btrim(v_values->>'candidate_expense_invoice_email'),'');
  v_email_ready:=v_email is not null and char_length(v_email)<=320
    and v_email~'^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$';
  v_result:=jsonb_build_object(
    'client_id',p_client_id,'contract_id',p_contract_id,'evaluation_date',v_date,
    'candidate_electronic_auto_authorise',coalesce((v_values->>'candidate_electronic_auto_authorise')::boolean,false),
    'candidate_electronic_auto_authorise_source',v_sources->>'candidate_electronic_auto_authorise',
    'expenses_require_separate_timesheet',coalesce((v_values->>'candidate_expenses_require_separate_timesheet')::boolean,false),
    'expenses_require_separate_timesheet_source',v_sources->>'candidate_expenses_require_separate_timesheet',
    'import_expense_separation_mandatory',coalesce((v_authority#>>'{applicability,import_authoritative}')::boolean,false),
    'import_source_family',v_authority->>'configured_route',
    'paper_submission_enabled',coalesce((v_values->>'candidate_paper_submission_enabled')::boolean,false),
    'paper_submission_enabled_source',v_sources->>'candidate_paper_submission_enabled',
    'expense_invoice_email',v_email,'expense_invoice_email_ready',v_email_ready,
    'manager_approval_policy',coalesce(v_values->'candidate_manager_approval_policy_json','{}'::jsonb),
    'allow_daily_manager_authorise_on_phone',coalesce((v_values->>'allow_daily_manager_authorise_on_phone')::boolean,true),
    'allow_daily_manager_authorise_by_email',coalesce((v_values->>'allow_daily_manager_authorise_by_email')::boolean,false),
    'hours_deviation_pct',v_global.candidate_hours_deviation_pct,
    'barred_manager_email_domains',private._candidate_normalize_domain_array_v1(v_global.candidate_barred_manager_email_domains),
    'client_setting_found',true,
    'client_settings_id',nullif(v_authority->>'client_settings_id','')::uuid,
    'contract_found',p_contract_id is not null,
    'global_settings_updated_at',nullif(v_authority->>'global_settings_updated_at','')::timestamptz,
    'authority_version',v_authority->>'authority_version',
    'authority_fingerprint',v_authority->>'authority_fingerprint'
  );
  return v_result||jsonb_build_object(
    'policy_fingerprint',encode(digest(convert_to(v_result::text,'UTF8'),'sha256'),'hex')
  );
end
$function$;

alter function private._candidate_policy_resolve_v1(uuid,uuid,date) owner to postgres;
revoke all on function private._candidate_policy_resolve_v1(uuid,uuid,date)
  from public,anon,authenticated,service_role;

create or replace function public.import_auto_authorise_policy_resolve_v1(
  p_source_system public.hr_source_enum,
  p_client_id uuid,
  p_contract_id uuid default null,
  p_validation_context boolean default false
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_source text:=upper(btrim(coalesce(p_source_system::text,'')));
  v_policy_source text;
  v_date date:=(transaction_timestamp() at time zone 'Europe/London')::date;
  v_authority jsonb;
  v_key text;
  v_source_label text;
  v_components jsonb;
  v_effective boolean;
  v_policy jsonb;
begin
  if p_client_id is null then
    raise exception 'IMPORT_AUTO_AUTHORISE_CLIENT_ID_REQUIRED' using errcode='22023';
  end if;
  if v_source not in ('HEALTHROSTER','HEALTHROSTER_DAILY','NHSP') then
    raise exception 'IMPORT_AUTO_AUTHORISE_SOURCE_SYSTEM_INVALID' using errcode='22023';
  end if;
  v_policy_source:=case when v_source in ('HEALTHROSTER','HEALTHROSTER_DAILY')
    then 'HEALTHROSTER' else 'NHSP' end;
  v_key:=case when v_policy_source='HEALTHROSTER'
    then 'healthroster_import_auto_authorise' else 'nhsp_import_auto_authorise' end;
  v_authority:=private._contract_settings_effective_core_v1(
    p_client_id,p_contract_id,v_date,
    case when v_source='HEALTHROSTER_DAILY' then 'DAILY' else 'IMPORT' end,null
  );
  v_components:=v_authority#>array['components',v_key];
  v_source_label:=v_authority#>>array['sources',v_key];
  v_effective:=coalesce((v_authority#>>array['values',v_key])::boolean,false);
  v_policy:=jsonb_strip_nulls(jsonb_build_object(
    'source_system',v_source,'policy_source_system',v_policy_source,
    'validation_context',coalesce(p_validation_context,false),
    'client_id',p_client_id::text,'contract_id',p_contract_id::text,
    'global_import_value',(v_components->>'global')::boolean,
    'client_import_value',(v_components->>'client')::boolean,
    'contract_override_value',(v_components->>'contract_override')::boolean,
    'effective_import_value',v_effective,
    'global_validation_value',coalesce((v_authority#>>'{values,auto_authorise_on_validation}')::boolean,false),
    'effective_value',v_effective,
    'resolution_source',case v_source_label when 'CONTRACT' then 'CONTRACT_OVERRIDE'
      when 'CLIENT' then 'CLIENT_SETTING' else 'GLOBAL_FALLBACK_CLIENT_SETTING_MISSING' end
      ||case when coalesce(p_validation_context,false) then '_VALIDATION_EXACT_MATCH' else '' end,
    'client_setting_found',true,
    'client_settings_id',v_authority->>'client_settings_id',
    'client_settings_effective_from',v_authority->>'client_settings_effective_from',
    'client_settings_updated_at',v_authority->>'client_settings_updated_at',
    'contract_updated_at',v_authority->>'contract_updated_at',
    'global_settings_updated_at',v_authority->>'global_settings_updated_at',
    'authority_version',v_authority->>'authority_version',
    'authority_fingerprint',v_authority->>'authority_fingerprint'
  ));
  return v_policy||jsonb_build_object(
    'ok',true,
    'policy_fingerprint',encode(digest(convert_to(v_policy::text,'UTF8'),'sha256'),'hex')
  );
end
$function$;

alter function public.import_auto_authorise_policy_resolve_v1(public.hr_source_enum,uuid,uuid,boolean)
  owner to postgres;
revoke all on function public.import_auto_authorise_policy_resolve_v1(public.hr_source_enum,uuid,uuid,boolean)
  from public,anon,authenticated;
grant execute on function public.import_auto_authorise_policy_resolve_v1(public.hr_source_enum,uuid,uuid,boolean)
  to service_role;

create or replace function public.import_auto_authorise_policy_resolve_v2(
  p_source_system public.hr_source_enum,
  p_client_id uuid,
  p_contract_id uuid,
  p_timesheet_id uuid,
  p_relevant_date date,
  p_validation_context boolean default false
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_source text:=upper(btrim(coalesce(p_source_system::text,'')));
  v_policy_source text;
  v_key text;
  v_authority jsonb;
  v_components jsonb;
  v_policy jsonb;
  v_source_label text;
  v_effective boolean;
begin
  if p_client_id is null then
    raise exception 'IMPORT_AUTO_AUTHORISE_CLIENT_ID_REQUIRED' using errcode='22023';
  end if;
  if p_relevant_date is null then
    raise exception 'IMPORT_AUTO_AUTHORISE_RELEVANT_DATE_REQUIRED' using errcode='22023';
  end if;
  if v_source not in ('HEALTHROSTER','HEALTHROSTER_DAILY','NHSP') then
    raise exception 'IMPORT_AUTO_AUTHORISE_SOURCE_SYSTEM_INVALID' using errcode='22023';
  end if;
  v_policy_source:=case when v_source in ('HEALTHROSTER','HEALTHROSTER_DAILY')
    then 'HEALTHROSTER' else 'NHSP' end;
  v_key:=case when v_policy_source='HEALTHROSTER'
    then 'healthroster_import_auto_authorise' else 'nhsp_import_auto_authorise' end;
  v_authority:=private._contract_settings_effective_core_v1(
    p_client_id,p_contract_id,p_relevant_date,
    case when v_source='HEALTHROSTER_DAILY' then 'DAILY' else 'IMPORT' end,
    p_timesheet_id
  );
  v_components:=v_authority#>array['components',v_key];
  v_source_label:=v_authority#>>array['sources',v_key];
  v_effective:=coalesce((v_authority#>>array['values',v_key])::boolean,false);
  v_policy:=jsonb_strip_nulls(jsonb_build_object(
    'source_system',v_source,'policy_source_system',v_policy_source,
    'validation_context',coalesce(p_validation_context,false),
    'client_id',p_client_id::text,'contract_id',p_contract_id::text,
    'global_import_value',(v_components->>'global')::boolean,
    'client_import_value',(v_components->>'client')::boolean,
    'contract_override_value',(v_components->>'contract_override')::boolean,
    'effective_import_value',v_effective,'effective_value',v_effective,
    'global_validation_value',coalesce((v_authority#>>'{values,auto_authorise_on_validation}')::boolean,false),
    'resolution_source',case v_source_label when 'CONTRACT' then 'CONTRACT_OVERRIDE'
      when 'CLIENT' then 'CLIENT_SETTING' else 'GLOBAL_FALLBACK_CLIENT_SETTING_MISSING' end
      ||case when coalesce(p_validation_context,false) then '_VALIDATION_EXACT_MATCH' else '' end,
    'client_setting_found',true,
    'client_settings_id',v_authority->>'client_settings_id',
    'client_settings_effective_from',v_authority->>'client_settings_effective_from',
    'client_settings_updated_at',v_authority->>'client_settings_updated_at',
    'contract_updated_at',v_authority->>'contract_updated_at',
    'global_settings_updated_at',v_authority->>'global_settings_updated_at',
    'authority_version',v_authority->>'authority_version',
    'authority_fingerprint',v_authority->>'authority_fingerprint',
    'relevant_date',p_relevant_date,
    'timesheet_id',p_timesheet_id
  ));
  return v_policy||jsonb_build_object(
    'policy_fingerprint',encode(digest(convert_to(v_policy::text,'UTF8'),'sha256'),'hex')
  );
end
$function$;

alter function public.import_auto_authorise_policy_resolve_v2(
  public.hr_source_enum,uuid,uuid,uuid,date,boolean
) owner to postgres;
revoke all on function public.import_auto_authorise_policy_resolve_v2(
  public.hr_source_enum,uuid,uuid,uuid,date,boolean
) from public,anon,authenticated;
grant execute on function public.import_auto_authorise_policy_resolve_v2(
  public.hr_source_enum,uuid,uuid,uuid,date,boolean
) to service_role;

-- One-time TEST correction and durable compatibility backfill. Planned weeks
-- are recalculated; existing real Timesheets receive a snapshot only when they
-- have never had one. No hours, rates, financials, invoices or payment rows are
-- altered.
do $backfill$
declare v_row record; v_snapshot jsonb; v_date date;
begin
  perform private._contract_settings_refresh_all_planned_v1();
  for v_row in
    select t.timesheet_id,t.contract_id,t.week_ending_date,t.sheet_scope,c.client_id
    from public.timesheets t
    join public.contracts c on c.id=t.contract_id
    where t.settings_authority_json='{}'::jsonb
      and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
    order by t.timesheet_id
  loop
    v_snapshot:=private._contract_settings_effective_core_v1(
      v_row.client_id,v_row.contract_id,v_row.week_ending_date,'WEEKLY',null
    );
    update public.timesheets set
      settings_authority_json=v_snapshot,
      settings_authority_version='CONTRACT_SETTINGS_AUTHORITY_V1',
      settings_authority_fingerprint=v_snapshot->>'authority_fingerprint',
      settings_authority_resolved_at=(v_snapshot->>'resolved_at_utc')::timestamptz
    where timesheet_id=v_row.timesheet_id and settings_authority_json='{}'::jsonb;
  end loop;
  for v_row in
    select distinct on (t.timesheet_id)
      t.timesheet_id,t.contract_id,t.week_ending_date,t.worked_start_iso,
      t.scheduled_start_iso,tf.client_id
    from public.timesheets t
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
    where t.settings_authority_json='{}'::jsonb
      and t.sheet_scope='DAILY'::public.timesheet_scope_enum
      and tf.processed_at_utc is not null
    order by t.timesheet_id,tf.computed_at_utc desc nulls last,tf.updated_at desc,tf.id desc
  loop
    v_date:=coalesce(
      (v_row.worked_start_iso at time zone 'Europe/London')::date,
      (v_row.scheduled_start_iso at time zone 'Europe/London')::date,
      v_row.week_ending_date
    );
    v_snapshot:=private._contract_settings_effective_core_v1(
      v_row.client_id,v_row.contract_id,v_date,'DAILY',null
    );
    update public.timesheets set
      settings_authority_json=v_snapshot,
      settings_authority_version='CONTRACT_SETTINGS_AUTHORITY_V1',
      settings_authority_fingerprint=v_snapshot->>'authority_fingerprint',
      settings_authority_resolved_at=(v_snapshot->>'resolved_at_utc')::timestamptz
    where timesheet_id=v_row.timesheet_id and settings_authority_json='{}'::jsonb;
  end loop;
end
$backfill$;

notify pgrst, 'reload schema';

commit;
