-- WP-F1 immutable rules and dated, default-off group configuration.
-- Existing policy_sha256/NO_CHANGE and all financial owners remain unchanged.

\set ON_ERROR_STOP on

begin;

create or replace function private._weekly_source_rule_values_v1(p_input jsonb)
returns jsonb language plpgsql immutable security invoker
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_values jsonb;
  v_choice record;
  v_choices jsonb:='{"checking_profile_kind":["HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1","HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1","ROSTER_WEEKLY_SUMMARY_ACTUAL_V1"],"self_bill_source":["SAME_FILE","SEPARATE_FINAL_REPORT"],"final_report_profile_kind":["NONE","ACACIUM_SELF_BILL_REPORT_V1"],"invoice_membership":["FINALISED_IN_CYCLE_WINDOW","FOLLOW_FINAL_REPORT_LINES"],"pay_membership":["ALL_FINALISED_IN_FILE","ON_ACCEPTED_REPORT_OR_PROTECTED"],"invoice_amount_basis":["CONTRACT_CALCULATION","SOURCE_AMOUNT"],"source_amount_basis":["NONE","EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY","REPORT_LINE_NET"],"line_vat_method":["ROUND_EACH_LINE_FROM_NET","LINE_GROSS_MINUS_NET_FROM_SOURCE","LINE_VAT_FROM_REPORT","ROUND_TOTAL_FROM_EXACT_NET"],"invoice_vat_method":["SUM_OF_LINES","ONCE_ON_TOTAL_TRUNCATE","ONCE_ON_TOTAL_HALF_UP"],"rate_difference_policy":["NOT_APPLICABLE","FLAG_ALLOW_IGNORE"],"negative_adjustment_policy":["NOT_ALLOWED","NEGATIVE_ADJUSTMENT_LINE"],"lookback_warning":["OFF","WARN_IF_AFTER_OLDEST_PROTECTED"],"identical_rekey":["COMPLETE_AUTOMATICALLY"],"authorised_late_addition":["COMPLETE_AUTOMATICALLY"],"equal_protected_hours":["CLEAR_AUTOMATICALLY"],"signed_comparison":["ON_EVERY_PUBLICATION"],"unfinalised_action":["ASK_CLIENT_TO_FINALISE"],"unfinalised_chase":["ASK_FROM_FIRST_CHECKING_IMPORT_SHOWING_IT"],"changed_query_clocks":["REASK_WITH_NEW_CLOCKS_EXCLUDE_UNCHANGED"],"invoice_sender":["CLIENT_SELF_BILL","AGENCY_INVOICE"]}'::jsonb;
  v_separate boolean; v_unused boolean; v_agency boolean; v_summary boolean;
begin
  perform private._weekly_source_settings_assert_request_v1(p_input,array[
    'checking_profile_kind','self_bill_source','final_report_profile_kind','invoice_membership','pay_membership',
    'invoice_amount_basis','source_amount_basis','line_vat_method','invoice_vat_method','rate_difference_policy',
    'negative_adjustment_policy','lookback_warning','identical_rekey','authorised_late_addition','equal_protected_hours',
    'signed_comparison','unfinalised_action','unfinalised_chase','changed_query_clocks','invoice_sender',
    'earliest_final_weekday','earliest_final_local_time','attach_healthroster_section'
  ],'WEEKLY_SOURCE_RULE_VALUES_INVALID');
  v_values:='{"self_bill_source":"SAME_FILE","final_report_profile_kind":"NONE","invoice_membership":"FINALISED_IN_CYCLE_WINDOW","pay_membership":"ALL_FINALISED_IN_FILE","invoice_amount_basis":"CONTRACT_CALCULATION","source_amount_basis":"NONE","line_vat_method":"ROUND_EACH_LINE_FROM_NET","invoice_vat_method":"SUM_OF_LINES","rate_difference_policy":"NOT_APPLICABLE","negative_adjustment_policy":"NEGATIVE_ADJUSTMENT_LINE","lookback_warning":"WARN_IF_AFTER_OLDEST_PROTECTED","identical_rekey":"COMPLETE_AUTOMATICALLY","authorised_late_addition":"COMPLETE_AUTOMATICALLY","equal_protected_hours":"CLEAR_AUTOMATICALLY","signed_comparison":"ON_EVERY_PUBLICATION","unfinalised_action":"ASK_CLIENT_TO_FINALISE","unfinalised_chase":"ASK_FROM_FIRST_CHECKING_IMPORT_SHOWING_IT","changed_query_clocks":"REASK_WITH_NEW_CLOCKS_EXCLUDE_UNCHANGED","invoice_sender":"CLIENT_SELF_BILL","earliest_final_weekday":4,"earliest_final_local_time":"00:00","attach_healthroster_section":true}'::jsonb||p_input;
  v_agency:=v_values->>'invoice_sender'='AGENCY_INVOICE';
  if v_agency and not p_input ? 'earliest_final_weekday' then
    v_values:=jsonb_set(v_values,'{earliest_final_weekday}','null'::jsonb);
  end if;
  if v_agency and not p_input ? 'earliest_final_local_time' then
    v_values:=jsonb_set(v_values,'{earliest_final_local_time}','null'::jsonb);
  end if;
  for v_choice in select key,value from jsonb_each(v_choices) loop
    if jsonb_typeof(v_values->v_choice.key) is distinct from 'string'
      or not v_choice.value ? (v_values->>v_choice.key) then
      raise exception 'WEEKLY_SOURCE_RULE_VALUE_INVALID' using errcode='22023';
    end if;
  end loop;
  if jsonb_typeof(v_values->'attach_healthroster_section') is distinct from 'boolean' then
    raise exception 'WEEKLY_SOURCE_RULE_VALUE_INVALID' using errcode='22023';
  end if;
  v_unused:=v_values->'earliest_final_weekday'='null'::jsonb and v_values->'earliest_final_local_time'='null'::jsonb;
  v_summary:=v_values->>'checking_profile_kind'='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1';
  if not v_unused and (jsonb_typeof(v_values->'earliest_final_weekday') is distinct from 'number'
      or coalesce(v_values->>'earliest_final_weekday','') !~ '^[0-6]$'
      or jsonb_typeof(v_values->'earliest_final_local_time') is distinct from 'string'
      or coalesce(v_values->>'earliest_final_local_time','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$') then
    raise exception 'WEEKLY_SOURCE_RULE_FINAL_CLOCK_INVALID' using errcode='22023';
  end if;
  if (v_summary or v_agency) is distinct from v_unused then
    raise exception 'WEEKLY_SOURCE_RULE_R9' using errcode='22023';
  end if;
  v_separate:=v_values->>'self_bill_source'='SEPARATE_FINAL_REPORT';
  if not v_separate and (v_values->>'final_report_profile_kind'<>'NONE'
      or v_values->>'invoice_membership'='FOLLOW_FINAL_REPORT_LINES'
      or v_values->>'pay_membership'='ON_ACCEPTED_REPORT_OR_PROTECTED') then
    raise exception 'WEEKLY_SOURCE_RULE_R1' using errcode='22023';
  end if;
  if (v_values->>'invoice_membership'='FOLLOW_FINAL_REPORT_LINES' or v_values->>'pay_membership'='ON_ACCEPTED_REPORT_OR_PROTECTED')
    and (not v_separate or v_values->>'final_report_profile_kind'='NONE') then
    raise exception 'WEEKLY_SOURCE_RULE_R2' using errcode='22023';
  end if;
  if (v_values->>'invoice_amount_basis'='SOURCE_AMOUNT' and v_values->>'source_amount_basis'='NONE')
    or (v_values->>'source_amount_basis'='REPORT_LINE_NET' and not v_separate)
    or (v_values->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY' and v_summary) then
    raise exception 'WEEKLY_SOURCE_RULE_R3' using errcode='22023';
  end if;
  if (v_values->>'line_vat_method'='LINE_VAT_FROM_REPORT' and v_values->>'final_report_profile_kind'='NONE')
    or (v_values->>'line_vat_method'='LINE_GROSS_MINUS_NET_FROM_SOURCE'
      and v_values->>'source_amount_basis'<>'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY')
    or (v_values->>'line_vat_method'='ROUND_TOTAL_FROM_EXACT_NET' and v_values->>'invoice_amount_basis'='SOURCE_AMOUNT') then
    raise exception 'WEEKLY_SOURCE_RULE_R4' using errcode='22023';
  end if;
  if (v_values->>'rate_difference_policy'='FLAG_ALLOW_IGNORE')<>(v_values->>'invoice_amount_basis'='SOURCE_AMOUNT') then
    raise exception 'WEEKLY_SOURCE_RULE_R6' using errcode='22023';
  end if;
  if v_agency and v_values - 'attach_healthroster_section'<>'{"self_bill_source":"SAME_FILE","final_report_profile_kind":"NONE","invoice_membership":"FINALISED_IN_CYCLE_WINDOW","pay_membership":"ALL_FINALISED_IN_FILE","invoice_amount_basis":"CONTRACT_CALCULATION","source_amount_basis":"NONE","line_vat_method":"ROUND_EACH_LINE_FROM_NET","invoice_vat_method":"SUM_OF_LINES","rate_difference_policy":"NOT_APPLICABLE","negative_adjustment_policy":"NEGATIVE_ADJUSTMENT_LINE","lookback_warning":"WARN_IF_AFTER_OLDEST_PROTECTED","identical_rekey":"COMPLETE_AUTOMATICALLY","authorised_late_addition":"COMPLETE_AUTOMATICALLY","equal_protected_hours":"CLEAR_AUTOMATICALLY","signed_comparison":"ON_EVERY_PUBLICATION","unfinalised_action":"ASK_CLIENT_TO_FINALISE","unfinalised_chase":"ASK_FROM_FIRST_CHECKING_IMPORT_SHOWING_IT","changed_query_clocks":"REASK_WITH_NEW_CLOCKS_EXCLUDE_UNCHANGED","invoice_sender":"AGENCY_INVOICE","earliest_final_weekday":null,"earliest_final_local_time":null,"checking_profile_kind":"HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1"}'::jsonb then
    raise exception 'WEEKLY_SOURCE_RULE_R13' using errcode='22023';
  end if;
  return v_values;
end;
$function$;

-- Locked settings owners resolve this context themselves; no browser evidence.
-- require_live additionally requires the configured unsigned agency policy to
-- have been set through its existing Client owner before feature activation.
create or replace function private._weekly_source_rule_group_context_v1(
  p_group_id uuid,p_from date,p_to date,p_values jsonb,p_require_live boolean default false
) returns void language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_group public.weekly_source_groups%rowtype;
  v_row record; v_policy jsonb; v_settings jsonb; v_scope_date date;
  v_contract_context record; v_contract_policy jsonb;
  v_agency boolean:=p_values->>'invoice_sender'='AGENCY_INVOICE';
  v_count integer:=0;
begin
  select * into v_group from public.weekly_source_groups where id=p_group_id and active;
  if not found or v_group.source_family<>'ROSTER' then
    raise exception 'WEEKLY_SOURCE_RULE_R10' using errcode='22023';
  end if;
  if v_agency and exists(select 1 from public.weekly_source_uploads u
    join public.weekly_source_cycles c on c.id=u.source_cycle_id
    join public.weekly_source_format_profiles f on f.id=u.source_format_profile_id
    where c.source_group_id=p_group_id and f.profile_code<>'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1'
      and u.state in ('SEALED','CURRENT','SUPERSEDED','CORRECTION_READY')) then
    raise exception 'WEEKLY_SOURCE_RULE_AGENCY_PROFILE_CONFLICT' using errcode='22023';
  end if;
  if p_values is not null and p_values->>'checking_profile_kind'<>'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' and not v_agency
    and (((p_values->>'earliest_final_weekday')::integer+6)%7*1440
      +extract(hour from (p_values->>'earliest_final_local_time')::time)::integer*60
      +extract(minute from (p_values->>'earliest_final_local_time')::time)::integer
      <(v_group.cutoff_weekday+6)%7*1440+extract(hour from v_group.cutoff_local_time)::integer*60
      +extract(minute from v_group.cutoff_local_time)::integer) then
    raise exception 'WEEKLY_SOURCE_RULE_R9' using errcode='22023';
  end if;
  -- Evaluate every effective policy boundary in the requested dated interval.
  -- Membership and policy tables already exclude overlap; missing coverage is
  -- refused rather than being filled with a guessed policy.
  for v_row in
    select gc.client_id,greatest(gc.valid_from,p_from) as scope_date
    from public.weekly_source_group_clients gc
    where gc.source_group_id=p_group_id and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]')
      && daterange(p_from,coalesce(p_to,'infinity'::date),'[]')
    union
    select gc.client_id,cp.effective_from
    from public.weekly_source_group_clients gc join public.weekly_source_client_policies cp
      on cp.client_id=gc.client_id and cp.source_group_id=gc.source_group_id
    where gc.source_group_id=p_group_id and cp.effective_from>=p_from
      and cp.effective_from<=coalesce(p_to,'infinity'::date)
      and cp.effective_from between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
    union
    select gc.client_id,cp.effective_to+1
    from public.weekly_source_group_clients gc join public.weekly_source_client_policies cp
      on cp.client_id=gc.client_id and cp.source_group_id=gc.source_group_id
    where gc.source_group_id=p_group_id and cp.effective_to is not null and cp.effective_to+1>=p_from
      and cp.effective_to+1<=coalesce(p_to,'infinity'::date)
      and cp.effective_to+1 between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
    order by client_id,scope_date
  loop
    v_count:=v_count+1; v_scope_date:=v_row.scope_date;
    if not exists(select 1 from public.weekly_source_client_policies cp where cp.source_group_id=p_group_id
      and cp.client_id=v_row.client_id and v_scope_date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date)) then
      raise exception 'WEEKLY_SOURCE_RULE_R8' using errcode='22023';
    end if;
    v_policy:=private._weekly_source_effective_policy_v1(v_row.client_id,null,v_scope_date);
    if p_values is null then
      if v_policy->>'authority_mode'<>'TIMESHEET_AUTHORITY' or v_policy->>'document_mode'<>'INVOICE_EVIDENCE_REQUIRED'
        or (v_policy->>'self_bill_enabled')::boolean then
        raise exception 'WEEKLY_SOURCE_RULE_SIGNED_GROUP_REQUIRED' using errcode='22023';
      end if;
      if p_require_live then
        v_settings:=private._contract_settings_effective_core_v1(v_row.client_id,null,v_scope_date,'DAILY',null);
        if not coalesce((v_settings->'applicability'->>'configuration_valid')::boolean,false)
          or not coalesce((v_settings->'values'->>'requires_hr')::boolean,false)
          or not coalesce((v_settings->'values'->>'autoprocess_hr')::boolean,false)
          or coalesce((v_settings->'values'->>'no_timesheet_required')::boolean,true)
          or coalesce((v_settings->'values'->>'is_nhsp')::boolean,true)
          or coalesce((v_settings->'values'->>'self_bill')::boolean,true) then
          raise exception 'WEEKLY_SOURCE_RULE_SIGNED_GROUP_REQUIRED' using errcode='22023'; end if;
      end if;
    elsif v_agency then
      if (v_policy->>'self_bill_enabled')::boolean
        or (p_require_live and (v_policy->>'authority_mode'<>'SOURCE_AUTHORITY' or v_policy->>'document_mode'<>'CHECK_ONLY'))
        or (not p_require_live and not ((v_policy->>'authority_mode'='SOURCE_AUTHORITY' and v_policy->>'document_mode'='CHECK_ONLY')
          or (v_policy->>'authority_mode'='TIMESHEET_AUTHORITY' and v_policy->>'document_mode'='INVOICE_EVIDENCE_REQUIRED'))) then
        raise exception 'WEEKLY_SOURCE_RULE_R14' using errcode='22023';
      end if;
      if exists(select 1 from public.weekly_source_group_rule_assignments a
        where a.source_group_id=p_group_id and a.feature_purpose='SIGNED_CHECK_ONLY'
          and daterange(a.effective_from_week_ending,coalesce(a.effective_to_week_ending,'infinity'::date),'[]')
            && daterange(p_from,coalesce(p_to,'infinity'::date),'[]')) then
        raise exception 'WEEKLY_SOURCE_RULE_R14' using errcode='22023';
      end if;
    elsif v_policy->>'authority_mode'<>'SOURCE_AUTHORITY' or v_policy->>'document_mode'<>'CHECK_ONLY'
      or not (v_policy->>'self_bill_enabled')::boolean then
      raise exception 'WEEKLY_SOURCE_RULE_R15' using errcode='22023';
    end if;
    -- SP-20 has no installed first-use witness in this source stage. Fail closed.
    if p_values is not null and (v_policy->>'source_fixed_expenses_enabled')::boolean then
      raise exception 'WEEKLY_SOURCE_RULE_R7' using errcode='22023';
    end if;
    if v_agency and coalesce((select cs.self_bill_no_invoices_sent from public.client_settings cs
      where cs.client_id=v_row.client_id and (cs.effective_from is null or cs.effective_from<=v_scope_date)
      order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1),false) then
      raise exception 'WEEKLY_SOURCE_RULE_R14' using errcode='22023';
    end if;
    if v_agency and exists(select 1 from public.contracts c where c.client_id=v_row.client_id
      and c.weekly_timesheet_source::text='HEALTHROSTER'
      and daterange(c.start_date,c.end_date,'[]') && daterange(v_scope_date,coalesce(p_to,'infinity'::date),'[]')
      and exists(select 1 from public.weekly_source_group_clients gc where gc.source_group_id=p_group_id
        and gc.client_id=c.client_id and greatest(c.start_date,v_scope_date) between gc.valid_from and coalesce(gc.valid_to,'infinity'::date))
      and (coalesce(c.self_bill,false) or coalesce(c.no_timesheet_required,false) or coalesce(c.is_nhsp,false))) then
      raise exception 'WEEKLY_SOURCE_RULE_R14' using errcode='22023';
    end if;
    -- Contract overrides are resolved by the established dated settings owner.
    for v_contract_context in select c.id,greatest(c.start_date,v_scope_date) as work_date
      from public.contracts c where c.client_id=v_row.client_id and c.weekly_timesheet_source::text='HEALTHROSTER'
        and daterange(c.start_date,c.end_date,'[]') && daterange(v_scope_date,coalesce(p_to,'infinity'::date),'[]')
        and exists(select 1 from public.weekly_source_group_clients gc where gc.source_group_id=p_group_id
          and gc.client_id=c.client_id and greatest(c.start_date,v_scope_date) between gc.valid_from and coalesce(gc.valid_to,'infinity'::date))
    loop
      v_settings:=private._contract_settings_effective_core_v1(v_row.client_id,v_contract_context.id,v_contract_context.work_date,'WEEKLY',null);
      v_contract_policy:=private._weekly_source_effective_policy_v1(v_row.client_id,v_contract_context.id,v_contract_context.work_date);
      if p_values is not null and coalesce((v_contract_policy->>'source_fixed_expenses_enabled')::boolean,false) then
        raise exception 'WEEKLY_SOURCE_RULE_R7' using errcode='22023'; end if;
      if v_agency and (coalesce((v_settings->'values'->>'self_bill')::boolean,true)
        or coalesce((v_settings->'values'->>'no_timesheet_required')::boolean,true)
        or coalesce((v_settings->'values'->>'is_nhsp')::boolean,true)
        or not coalesce((v_settings->'values'->>'requires_hr')::boolean,false)
        or not coalesce((v_settings->'values'->>'autoprocess_hr')::boolean,false)
        or not coalesce((v_settings->'applicability'->>'configuration_valid')::boolean,false)) then
        raise exception 'WEEKLY_SOURCE_RULE_R14' using errcode='22023';
      end if;
    end loop;
  end loop;
  if p_require_live and v_count=0 then
    raise exception 'WEEKLY_SOURCE_RULE_GROUP_MEMBERS_REQUIRED' using errcode='22023';
  end if;
end;
$function$;

create or replace function private.weekly_source_is_import_authoritative_v1(p_client_id uuid,p_contract_id uuid,p_work_date date)
returns boolean language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_policy jsonb;
begin
  if p_client_id is null or p_work_date is null or not exists(select 1
    from public.weekly_source_group_clients gc join public.weekly_source_groups g on g.id=gc.source_group_id
    where gc.client_id=p_client_id and g.active and g.source_family='ROSTER'
      and p_work_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)) then return false; end if;
  if p_contract_id is not null and not exists(select 1 from public.contracts c
    where c.id=p_contract_id and c.client_id=p_client_id) then return false; end if;
  if not exists(select 1 from public.weekly_source_client_policies cp join public.weekly_source_groups g on g.id=cp.source_group_id
    join public.weekly_source_group_clients gc on gc.source_group_id=g.id and gc.client_id=cp.client_id
    where cp.client_id=p_client_id and g.active and g.source_family='ROSTER'
      and p_work_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
      and p_work_date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date)) then return false; end if;
  v_policy:=private._weekly_source_effective_policy_v1(p_client_id,p_contract_id,p_work_date);
  return v_policy->>'source_family'='ROSTER' and v_policy->>'authority_mode'='SOURCE_AUTHORITY'
    and v_policy->>'document_mode'='CHECK_ONLY';
end;
$function$;

create or replace function private.weekly_source_effective_rule_set_v1(p_client_id uuid,p_cycle_id uuid)
returns jsonb language sql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
  select jsonb_build_object('contract','WEEKLY_SOURCE_EFFECTIVE_RULE_SET_V1','source_group_id',g.id,
    'assignment_id',a.id,'assignment_version',a.version,'rule_set_id',r.id,'rule_set_version_id',v.id,
    'version_number',v.version_number,'values_sha256',v.values_sha256,'values',v.values_json)
  from public.weekly_source_cycles c join public.weekly_source_groups g on g.id=c.source_group_id and g.active and g.source_family='ROSTER'
  join public.weekly_source_group_clients gc on gc.source_group_id=g.id and gc.client_id=p_client_id
    and c.finalisation_week_ending between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
  join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id and a.feature_enabled
    and a.feature_purpose='RULE_SET'
    and c.finalisation_week_ending between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
  join public.weekly_source_rule_set_versions v on v.id=a.rule_set_version_id
  join public.weekly_source_rule_sets r on r.id=v.rule_set_id and r.agency_id=g.agency_id and r.environment=g.environment
  where c.id=p_cycle_id
$function$;

create or replace function private.weekly_source_has_self_bill_rules_v1(p_client_id uuid,p_cycle_id uuid)
returns boolean language sql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
  select private.weekly_source_effective_rule_set_v1(p_client_id,p_cycle_id) is not null
$function$;

-- The single admission predicate Credit and ordinary successors must call.
-- Resolve all scope from the physical current Timesheet; never trust a payload.
create or replace function private.weekly_source_agency_invoice_check_only_v1(p_timesheet_id uuid)
returns boolean language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_ts public.timesheets%rowtype; v_contract public.contracts%rowtype;
  v_cycle uuid; v_rules jsonb; v_policy jsonb; v_settings jsonb;
begin
  select * into v_ts from public.timesheets t where t.timesheet_id=p_timesheet_id and t.is_current
    and t.sheet_scope::text='WEEKLY' and t.status::text<>'REVOKED' and t.revoked_at is null and t.archived_at_utc is null;
  if not found or v_ts.contract_id is null or v_ts.week_ending_date is null then return false; end if;
  select * into v_contract from public.contracts c where c.id=v_ts.contract_id
    and c.weekly_timesheet_source::text='HEALTHROSTER';
  if not found or v_contract.self_bill or coalesce(v_contract.no_timesheet_required,false)
    or coalesce(v_contract.is_nhsp,false) then return false; end if;
  if nullif(v_ts.settings_authority_json->>'client_id','') is not null
    and v_ts.settings_authority_json->>'client_id'<>v_contract.client_id::text then return false; end if;
  if v_contract.start_date>v_ts.week_ending_date or v_contract.end_date<v_ts.week_ending_date-6
    or exists(select 1 from public.timesheets_financials f where f.timesheet_id=p_timesheet_id
      and f.is_current and f.timesheet_version=v_ts.version and f.client_id is distinct from v_contract.client_id) then return false; end if;
  select c.id into v_cycle from public.weekly_source_cycles c
    join public.weekly_source_groups g on g.id=c.source_group_id and g.active and g.source_family='ROSTER'
    join public.weekly_source_group_clients gc on gc.source_group_id=g.id and gc.client_id=v_contract.client_id
      and v_ts.week_ending_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
    where c.finalisation_week_ending=v_ts.week_ending_date;
  if v_cycle is null then return false; end if;
  v_rules:=private.weekly_source_effective_rule_set_v1(v_contract.client_id,v_cycle);
  if v_rules is null or v_rules->'values'->>'invoice_sender'<>'AGENCY_INVOICE' then return false; end if;
  if not private.weekly_source_is_import_authoritative_v1(v_contract.client_id,v_contract.id,v_ts.week_ending_date) then return false; end if;
  v_policy:=private._weekly_source_effective_policy_v1(v_contract.client_id,v_contract.id,v_ts.week_ending_date);
  if coalesce((v_policy->>'self_bill_enabled')::boolean,true) then return false; end if;
  if coalesce((select cs.self_bill_no_invoices_sent from public.client_settings cs
    where cs.client_id=v_contract.client_id and (cs.effective_from is null or cs.effective_from<=v_ts.week_ending_date)
    order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1),false) then return false; end if;
  v_settings:=private._contract_settings_effective_core_v1(v_contract.client_id,v_contract.id,v_ts.week_ending_date,'WEEKLY',null);
  return coalesce((v_settings->'applicability'->>'configuration_valid')::boolean,false)
    and not coalesce((v_settings->'values'->>'self_bill')::boolean,true)
    and not coalesce((v_settings->'values'->>'no_timesheet_required')::boolean,true)
    and not coalesce((v_settings->'values'->>'is_nhsp')::boolean,true)
    and coalesce((v_settings->'values'->>'requires_hr')::boolean,false)
    and coalesce((v_settings->'values'->>'autoprocess_hr')::boolean,false);
end;
$function$;

-- Shared signed route scope for D10, credit-origin and correction consumers.
-- Signature completeness is deliberately left to the caller's existing owner.
create or replace function private.weekly_source_signed_check_only_v1(p_timesheet_id uuid)
returns boolean language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_ts public.timesheets%rowtype; v_contract public.contracts%rowtype;
  v_client uuid; v_client_count integer; v_work_date date; v_policy jsonb; v_route record;
begin
  select * into v_ts from public.timesheets t where t.timesheet_id=p_timesheet_id and t.is_current
    and t.sheet_scope::text in ('WEEKLY','DAILY') and t.status::text not in ('REVOKED','ARCHIVED','CANCELLED') and t.revoked_at is null and t.archived_at_utc is null;
  if not found or v_ts.week_ending_date is null then return false; end if;
  if v_ts.contract_id is not null then
    select * into v_contract from public.contracts c where c.id=v_ts.contract_id;
    if not found or v_contract.self_bill or coalesce(v_contract.no_timesheet_required,false)
      or coalesce(v_contract.is_nhsp,false) then return false; end if;
    v_client:=v_contract.client_id;
    if nullif(v_ts.settings_authority_json->>'client_id','') is not null
      and v_ts.settings_authority_json->>'client_id'<>v_client::text then return false; end if;
    if exists(select 1 from public.timesheets_financials f where f.timesheet_id=p_timesheet_id
      and f.is_current and f.timesheet_version=v_ts.version and f.client_id is distinct from v_client) then return false; end if;
  elsif v_ts.sheet_scope::text='DAILY' then
    select count(distinct f.client_id),(array_agg(distinct f.client_id))[1] into v_client_count,v_client
      from public.timesheets_financials f where f.timesheet_id=p_timesheet_id and f.is_current
        and f.timesheet_version=v_ts.version and f.client_id is not null;
    if v_client_count<>1 then return false; end if;
    if nullif(v_ts.settings_authority_json->>'client_id','') is not null
      and v_ts.settings_authority_json->>'client_id'<>v_client::text then return false; end if;
  else return false;
  end if;
  v_work_date:=case when v_ts.sheet_scope::text='DAILY' then (v_ts.worked_start_iso at time zone 'Europe/London')::date
    else v_ts.week_ending_date end;
  if v_work_date is null or v_work_date>v_ts.week_ending_date or v_work_date<v_ts.week_ending_date-6 then return false; end if;
  if not exists(select 1 from public.weekly_source_group_clients gc
    join public.weekly_source_groups g on g.id=gc.source_group_id and g.active and g.source_family='ROSTER'
    join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id and a.feature_enabled
      and a.feature_purpose='SIGNED_CHECK_ONLY' and a.rule_set_version_id is null
      and v_ts.week_ending_date between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
    where gc.client_id=v_client and v_work_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
      and v_ts.week_ending_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)) then return false; end if;
  if not exists(select 1 from public.weekly_source_client_policies cp
    join public.weekly_source_group_clients gc on gc.source_group_id=cp.source_group_id and gc.client_id=cp.client_id
    where cp.client_id=v_client and v_work_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
    and v_work_date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date)) then return false; end if;
  v_policy:=private._weekly_source_effective_policy_v1(v_client,v_ts.contract_id,v_work_date);
  if v_policy->>'source_family'<>'ROSTER' or v_policy->>'authority_mode'<>'TIMESHEET_AUTHORITY'
    or v_policy->>'document_mode'<>'INVOICE_EVIDENCE_REQUIRED' or coalesce((v_policy->>'self_bill_enabled')::boolean,true) then return false; end if;
  select * into v_route from public._import_review_effective_authority_core_v1(
    case when v_ts.sheet_scope::text='DAILY' then 'HR_DAILY' else 'HR_WEEKLY' end,v_ts.contract_id,v_client,v_work_date);
  return coalesce(v_route.route_eligible,false) and coalesce(v_route.validation_eligible,false)
    and not coalesce(v_route.import_authoritative,true) and v_route.authority_mode='VALIDATION_ONLY'
    and not coalesce(v_route.effective_is_nhsp,true) and not coalesce(v_route.effective_no_timesheet_required,true);
end;
$function$;

-- Exact enabled agency witness for the existing Client settings owner. Saved
-- disabled choices never alter derived or persisted policy. Activation invokes
-- the same Client owner in its transaction after enabling this bounded gate.
create or replace function private.weekly_source_configured_agency_invoice_v1(
  p_client_id uuid,p_group_id uuid,p_agency_id uuid,p_environment text,p_date date
) returns boolean language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_settings jsonb; v_group_id uuid:=p_group_id;
begin
  if p_client_id is null or p_date is null then return false; end if;
  if v_group_id is null then
    select gc.source_group_id into v_group_id from public.weekly_source_group_clients gc
      join public.weekly_source_groups g on g.id=gc.source_group_id and g.active and g.source_family='ROSTER'
        and g.agency_id=p_agency_id and g.environment=p_environment
      where gc.client_id=p_client_id and p_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date);
  end if;
  if v_group_id is null or not exists(select 1 from public.weekly_source_groups g
    join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id
    join public.weekly_source_rule_set_versions v on v.id=a.rule_set_version_id
    join public.weekly_source_rule_sets r on r.id=v.rule_set_id and r.agency_id=g.agency_id and r.environment=g.environment
    where g.id=v_group_id and g.active and g.source_family='ROSTER' and g.agency_id=p_agency_id and g.environment=p_environment
      and a.feature_enabled and a.feature_purpose='RULE_SET'
      and p_date between a.effective_from_week_ending-6 and coalesce(a.effective_to_week_ending,'infinity'::date)
      and v.values_json->>'invoice_sender'='AGENCY_INVOICE') then return false; end if;
  if exists(select 1 from public.weekly_source_group_clients gc join public.weekly_source_group_rule_assignments a on a.source_group_id=gc.source_group_id
      where gc.client_id=p_client_id and a.feature_purpose='SIGNED_CHECK_ONLY'
        and p_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
        and p_date between a.effective_from_week_ending-6 and coalesce(a.effective_to_week_ending,'infinity'::date)) then return false; end if;
  if coalesce((select cs.self_bill_no_invoices_sent from public.client_settings cs
      where cs.client_id=p_client_id and (cs.effective_from is null or cs.effective_from<=p_date)
      order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1),false) then return false; end if;
  if not exists(select 1 from public.contracts c where c.client_id=p_client_id
      and c.weekly_timesheet_source::text='HEALTHROSTER' and c.end_date>=p_date)
    or exists(select 1 from public.contracts c where c.client_id=p_client_id
      and c.weekly_timesheet_source::text<>'NONE' and c.end_date>=p_date
      and (c.weekly_timesheet_source::text<>'HEALTHROSTER' or c.self_bill or coalesce(c.no_timesheet_required,false) or coalesce(c.is_nhsp,false))) then return false; end if;
  for v_settings in select private._contract_settings_effective_core_v1(c.client_id,c.id,greatest(c.start_date,p_date),'WEEKLY',null)
    from public.contracts c where c.client_id=p_client_id and c.weekly_timesheet_source::text='HEALTHROSTER' and c.end_date>=p_date
  loop
    if not coalesce((v_settings->'applicability'->>'configuration_valid')::boolean,false)
      or not coalesce((v_settings->'values'->>'requires_hr')::boolean,false)
      or not coalesce((v_settings->'values'->>'autoprocess_hr')::boolean,false)
      or coalesce((v_settings->'values'->>'no_timesheet_required')::boolean,true)
      or coalesce((v_settings->'values'->>'is_nhsp')::boolean,true)
      or coalesce((v_settings->'values'->>'self_bill')::boolean,true) then return false; end if;
  end loop;
  return true;
end;
$function$;

-- NEW installs migrations before repeatables. Reuse the exact shared guard
-- after its earlier central repeatable has installed it, with no new helper.
drop trigger if exists weekly_source_immutable_record_guard on public.weekly_source_rule_set_versions;
create trigger weekly_source_immutable_record_guard before update or delete
  on public.weekly_source_rule_set_versions for each row execute function private._weekly_source_immutable_record_guard_v1();
drop trigger if exists weekly_source_immutable_truncate_guard on public.weekly_source_rule_set_versions;
create trigger weekly_source_immutable_truncate_guard before truncate
  on public.weekly_source_rule_set_versions for each statement execute function private._weekly_source_immutable_record_guard_v1();

-- The existing finalisation insert gets a frozen witness only after activation.
-- UPDATE can never move or backfill it, including NULL -> a later version.
create or replace function private.weekly_source_final_rule_version_guard_v1()
returns trigger language plpgsql security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_version uuid;
begin
  if tg_op='UPDATE' then
    if new.rule_set_version_id is distinct from old.rule_set_version_id then
      raise exception 'WEEKLY_SOURCE_FINAL_RULE_VERSION_IMMUTABLE' using errcode='55000';
    end if;
    return new;
  end if;
  select a.rule_set_version_id into v_version from public.weekly_source_cycles c
    join public.weekly_source_groups g on g.id=c.source_group_id and g.active and g.source_family='ROSTER'
    join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id and a.feature_enabled and a.feature_purpose='RULE_SET'
      and c.finalisation_week_ending between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
    where c.id=new.source_cycle_id;
  if new.rule_set_version_id is not null and new.rule_set_version_id is distinct from v_version then
    raise exception 'WEEKLY_SOURCE_FINAL_RULE_VERSION_STALE' using errcode='40001';
  end if;
  new.rule_set_version_id:=v_version;
  return new;
end;
$function$;
drop trigger if exists weekly_source_final_rule_version_guard on public.weekly_source_final_revisions;
create trigger weekly_source_final_rule_version_guard before insert or update of rule_set_version_id
  on public.weekly_source_final_revisions for each row execute function private.weekly_source_final_rule_version_guard_v1();

create or replace function public.weekly_source_rule_sets_get_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_actor uuid; v_agency uuid; v_environment text;
begin
  perform private._weekly_source_settings_assert_request_v1(p_request,array['actor_user_id','agency_id','environment'],'WEEKLY_SOURCE_RULE_SETS_REQUEST_INVALID');
  begin v_actor:=(p_request->>'actor_user_id')::uuid; v_agency:=(p_request->>'agency_id')::uuid;
  exception when others then raise exception 'WEEKLY_SOURCE_RULE_SETS_REQUEST_INVALID' using errcode='22023'; end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  v_environment:=private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
  return jsonb_build_object('contract','WEEKLY_SOURCE_RULE_SETS_V1','rule_sets',coalesce((select jsonb_agg(
    jsonb_build_object('id',r.id,'code',r.code,'display_name',r.display_name,'version',r.version,
      'versions',coalesce((select jsonb_agg(jsonb_build_object('id',v.id,'version_number',v.version_number,
        'values',v.values_json,'values_sha256',v.values_sha256,'created_by_user_id',v.created_by_user_id,
        'created_at_utc',v.created_at_utc) order by v.version_number desc)
        from public.weekly_source_rule_set_versions v where v.rule_set_id=r.id),'[]'::jsonb)) order by r.code,r.id)
    from public.weekly_source_rule_sets r where r.agency_id=v_agency and r.environment=v_environment),'[]'::jsonb),
    'assignments',coalesce((select jsonb_agg(jsonb_build_object('id',a.id,'source_group_id',g.id,
      'source_group_version',g.version,'rule_set_version_id',a.rule_set_version_id,'version',a.version,
      'effective_from_week_ending',a.effective_from_week_ending,'effective_to_week_ending',a.effective_to_week_ending,
      'feature_enabled',a.feature_enabled,'feature_purpose',a.feature_purpose,'created_by_user_id',a.created_by_user_id,
      'created_at_utc',a.created_at_utc) order by g.id,a.effective_from_week_ending)
    from public.weekly_source_group_rule_assignments a join public.weekly_source_groups g on g.id=a.source_group_id
    where g.agency_id=v_agency and g.environment=v_environment),'[]'::jsonb));
end;
$function$;

create or replace function public.weekly_source_rule_set_version_save_atomic_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_actor uuid; v_agency uuid; v_environment text; v_group_id uuid; v_request_id uuid;
  v_id uuid; v_expected bigint; v_code text; v_name text; v_values jsonb; v_hash text;
  v_group public.weekly_source_groups%rowtype; v_set public.weekly_source_rule_sets%rowtype;
  v_version public.weekly_source_rule_set_versions%rowtype; v_number bigint;
begin
  perform private._weekly_source_settings_assert_request_v1(p_request,array['actor_user_id','agency_id','environment','source_group_id','rule_set','values','request_id'],'WEEKLY_SOURCE_RULE_SAVE_REQUEST_INVALID');
  perform private._weekly_source_settings_assert_request_v1(p_request->'rule_set',array['id','code','display_name','expected_version'],'WEEKLY_SOURCE_RULE_SAVE_REQUEST_INVALID');
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid; v_agency:=(p_request->>'agency_id')::uuid;
    v_group_id:=(p_request->>'source_group_id')::uuid; v_request_id:=(p_request->>'request_id')::uuid;
    v_id:=nullif(p_request->'rule_set'->>'id','')::uuid;
    v_expected:=(p_request->'rule_set'->>'expected_version')::bigint;
    v_code:=p_request->'rule_set'->>'code'; v_name:=btrim(p_request->'rule_set'->>'display_name');
  exception when others then raise exception 'WEEKLY_SOURCE_RULE_SAVE_REQUEST_INVALID' using errcode='22023'; end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  v_environment:=private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
  if v_group_id is null or v_request_id is null or v_expected is null
    or v_code is null or v_code !~ '^[A-Z][A-Z0-9_]{1,79}$' or v_name is null or char_length(v_name) not between 1 and 160
    or (v_id is null and v_expected<>0) or (v_id is not null and v_expected<1) then
    raise exception 'WEEKLY_SOURCE_RULE_SAVE_REQUEST_INVALID' using errcode='22023';
  end if;
  v_values:=private._weekly_source_rule_values_v1(p_request->'values');
  v_hash:=encode(extensions.digest(convert_to((p_request||jsonb_build_object('values',v_values))::text,'UTF8'),'sha256'),'hex');
  perform pg_advisory_xact_lock(hashtextextended('weekly-source-rule-set:'||v_agency::text||':'||v_environment||':'||coalesce(v_id::text,v_code),0));
  select * into v_group from public.weekly_source_groups where id=v_group_id for update;
  if not found or v_group.agency_id<>v_agency or v_group.environment<>v_environment or not v_group.active then
    raise exception 'WEEKLY_SOURCE_RULE_GROUP_NOT_APPLICABLE' using errcode='22023'; end if;
  if v_id is null then
    select * into v_set from public.weekly_source_rule_sets where agency_id=v_agency and environment=v_environment and creation_request_id=v_request_id for update;
    if found then
      if v_set.creation_request_sha256<>v_hash then raise exception 'WEEKLY_SOURCE_RULE_REQUEST_REUSED' using errcode='22023'; end if;
      select * into strict v_version from public.weekly_source_rule_set_versions where rule_set_id=v_set.id and save_request_id=v_request_id;
      return jsonb_build_object('contract','WEEKLY_SOURCE_RULE_SET_SAVED_V1','rule_set_id',v_set.id,'rule_set_version_id',v_version.id,
        'version_number',v_version.version_number,'rule_set_version',v_version.version_number,'values_sha256',v_version.values_sha256,'replayed',true);
    end if;
  else
    select * into v_set from public.weekly_source_rule_sets where id=v_id and agency_id=v_agency and environment=v_environment for update;
    if not found then raise exception 'WEEKLY_SOURCE_RULE_SET_NOT_FOUND' using errcode='22023'; end if;
    select * into v_version from public.weekly_source_rule_set_versions where rule_set_id=v_set.id and save_request_id=v_request_id;
    if found then
      if v_version.save_request_sha256<>v_hash then raise exception 'WEEKLY_SOURCE_RULE_REQUEST_REUSED' using errcode='22023'; end if;
      return jsonb_build_object('contract','WEEKLY_SOURCE_RULE_SET_SAVED_V1','rule_set_id',v_set.id,'rule_set_version_id',v_version.id,
        'version_number',v_version.version_number,'rule_set_version',v_version.version_number,'values_sha256',v_version.values_sha256,'replayed',true);
    end if;
    if v_set.version<>v_expected then raise exception 'WEEKLY_SOURCE_RULE_SET_STALE' using errcode='40001'; end if;
  end if;
  perform private._weekly_source_rule_group_context_v1(v_group_id,(transaction_timestamp() at time zone 'Europe/London')::date,null,v_values,false);
  if v_id is null then
    insert into public.weekly_source_rule_sets(agency_id,environment,code,display_name,creation_request_id,creation_request_sha256,created_by_user_id,updated_by_user_id)
      values(v_agency,v_environment,v_code,v_name,v_request_id,v_hash,v_actor,v_actor) returning * into v_set;
    v_number:=1;
  else
    update public.weekly_source_rule_sets set code=v_code,display_name=v_name,version=version+1,updated_by_user_id=v_actor,updated_at_utc=transaction_timestamp()
      where id=v_set.id returning * into v_set;
    v_number:=v_set.version;
  end if;
  insert into public.weekly_source_rule_set_versions(rule_set_id,version_number,values_json,values_sha256,save_request_id,save_request_sha256,created_by_user_id)
    values(v_set.id,v_number,v_values,encode(extensions.digest(convert_to(v_values::text,'UTF8'),'sha256'),'hex'),v_request_id,v_hash,v_actor) returning * into v_version;
  return jsonb_build_object('contract','WEEKLY_SOURCE_RULE_SET_SAVED_V1','rule_set_id',v_set.id,'rule_set_version_id',v_version.id,
    'version_number',v_number,'rule_set_version',v_set.version,'values_sha256',v_version.values_sha256,'replayed',false);
end;
$function$;

create or replace function public.weekly_source_group_rule_assign_atomic_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_actor uuid; v_agency uuid; v_environment text; v_group_id uuid; v_version_id uuid;
  v_request_id uuid; v_expected bigint; v_from date; v_to date; v_hash text; v_id uuid:=gen_random_uuid(); v_receipt jsonb;
  v_group public.weekly_source_groups%rowtype; v_version public.weekly_source_rule_set_versions%rowtype;
  v_prior public.weekly_source_group_rule_assignments%rowtype;
begin
  perform private._weekly_source_settings_assert_request_v1(p_request,array['actor_user_id','agency_id','environment','source_group_id','rule_set_version_id','effective_from_week_ending','effective_to_week_ending','expected_group_version','request_id'],'WEEKLY_SOURCE_RULE_ASSIGN_REQUEST_INVALID');
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid; v_agency:=(p_request->>'agency_id')::uuid; v_group_id:=(p_request->>'source_group_id')::uuid;
    v_version_id:=nullif(p_request->>'rule_set_version_id','')::uuid; v_request_id:=(p_request->>'request_id')::uuid;
    v_expected:=(p_request->>'expected_group_version')::bigint; v_from:=(p_request->>'effective_from_week_ending')::date;
    v_to:=nullif(p_request->>'effective_to_week_ending','')::date;
  exception when others then raise exception 'WEEKLY_SOURCE_RULE_ASSIGN_REQUEST_INVALID' using errcode='22023'; end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  v_environment:=private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
  if v_group_id is null or v_request_id is null or v_expected is null or v_expected<1 or v_from is null
    or p_request->>'effective_from_week_ending'<>to_char(v_from,'YYYY-MM-DD')
    or (v_to is not null and (p_request->>'effective_to_week_ending'<>to_char(v_to,'YYYY-MM-DD') or v_to<v_from or (v_to-v_from)%7<>0))
    or not p_request ? 'rule_set_version_id' then raise exception 'WEEKLY_SOURCE_RULE_ASSIGN_REQUEST_INVALID' using errcode='22023'; end if;
  v_hash:=encode(extensions.digest(convert_to(p_request::text,'UTF8'),'sha256'),'hex');
  -- Same lifecycle gate as cycle creation; lock cycles too, as finalisation
  -- holds the cycle row even when no group lock is needed by its old owner.
  perform pg_advisory_xact_lock(hashtextextended('weekly-source-cycle-lifecycle:'||v_group_id::text,0));
  select * into v_group from public.weekly_source_groups where id=v_group_id for update;
  if not found or v_group.agency_id<>v_agency or v_group.environment<>v_environment or not v_group.active or v_group.source_family<>'ROSTER' then
    raise exception 'WEEKLY_SOURCE_RULE_GROUP_NOT_APPLICABLE' using errcode='22023'; end if;
  select * into v_prior from public.weekly_source_group_rule_assignments where source_group_id=v_group_id and assignment_request_id=v_request_id;
  if found then
    if v_prior.assignment_request_sha256<>v_hash then raise exception 'WEEKLY_SOURCE_RULE_REQUEST_REUSED' using errcode='22023'; end if;
    return v_prior.assignment_receipt_json||jsonb_build_object('replayed',true);
  end if;
  if v_group.version<>v_expected then raise exception 'WEEKLY_SOURCE_RULE_GROUP_STALE' using errcode='40001'; end if;
  if v_from<(transaction_timestamp() at time zone 'Europe/London')::date then
    raise exception 'WEEKLY_SOURCE_RULE_FUTURE_WEEK_REQUIRED' using errcode='22023'; end if;
  perform 1 from public.weekly_source_cycles c where c.source_group_id=v_group_id and c.finalisation_week_ending>=v_from
    and c.finalisation_week_ending<=coalesce(v_to,'infinity'::date) order by c.id for update;
  if exists(select 1 from public.weekly_source_cycles c where c.source_group_id=v_group_id
    and c.finalisation_week_ending between v_from and coalesce(v_to,'infinity'::date)
    and (c.current_final_revision_id is not null or c.state in ('FINALISING','FINALISED','CORRECTION_IN_PROGRESS')
      or exists(select 1 from public.weekly_source_final_revisions f where f.source_cycle_id=c.id))) then
    raise exception 'WEEKLY_SOURCE_RULE_FINALISED_WEEK_REFUSED' using errcode='55000'; end if;
  if v_version_id is not null then
    select v.* into v_version from public.weekly_source_rule_set_versions v join public.weekly_source_rule_sets r on r.id=v.rule_set_id
      where v.id=v_version_id and r.agency_id=v_agency and r.environment=v_environment;
    if not found then raise exception 'WEEKLY_SOURCE_RULE_VERSION_NOT_FOUND' using errcode='22023'; end if;
    perform private._weekly_source_rule_group_context_v1(v_group_id,v_from,v_to,v_version.values_json,false);
  end if;
  -- A new choice clips only an earlier open assignment. Future overlapping
  -- choices and same-start replacements need an explicit new range decision.
  select * into v_prior from public.weekly_source_group_rule_assignments a where a.source_group_id=v_group_id
    and daterange(a.effective_from_week_ending,coalesce(a.effective_to_week_ending,'infinity'::date),'[]')
      && daterange(v_from,coalesce(v_to,'infinity'::date),'[]') order by a.effective_from_week_ending for update;
  if found then
    if v_prior.feature_enabled and exists(select 1 from public.weekly_source_rule_set_versions v
      where v.id=v_prior.rule_set_version_id and v.values_json->>'invoice_sender'='AGENCY_INVOICE') then
      raise exception 'WEEKLY_SOURCE_RULE_ACTIVE_AGENCY_TRANSITION_REQUIRED' using errcode='55000'; end if;
    if v_prior.effective_from_week_ending>=v_from or v_prior.effective_to_week_ending is not null
      or (v_from-v_prior.effective_from_week_ending)%7<>0
      or exists(select 1 from public.weekly_source_group_rule_assignments a where a.source_group_id=v_group_id
        and a.id<>v_prior.id and daterange(a.effective_from_week_ending,coalesce(a.effective_to_week_ending,'infinity'::date),'[]')
          && daterange(v_from,coalesce(v_to,'infinity'::date),'[]')) then
      raise exception 'WEEKLY_SOURCE_RULE_ASSIGNMENT_OVERLAP' using errcode='22023'; end if;
    update public.weekly_source_group_rule_assignments set effective_to_week_ending=v_from-7,version=version+1 where id=v_prior.id;
  end if;
  update public.weekly_source_groups set version=version+1,updated_by_user_id=v_actor,updated_at_utc=transaction_timestamp()
    where id=v_group_id returning * into v_group;
  v_receipt:=jsonb_build_object('contract','WEEKLY_SOURCE_RULE_ASSIGNED_V1','assignment_id',v_id,'assignment_version',1,
    'source_group_id',v_group_id,'source_group_version',v_group.version,'rule_set_version_id',v_version_id,
    'effective_from_week_ending',v_from,'effective_to_week_ending',v_to,'feature_enabled',false,'replayed',false);
  insert into public.weekly_source_group_rule_assignments(id,source_group_id,rule_set_version_id,effective_from_week_ending,effective_to_week_ending,
    assignment_request_id,assignment_request_sha256,assignment_receipt_json,created_by_user_id)
    values(v_id,v_group_id,v_version_id,v_from,v_to,v_request_id,v_hash,v_receipt,v_actor);
  return v_receipt;
end;
$function$;

-- ROOT setup owner only. There is deliberately no Worker/Office route.
create or replace function public.weekly_source_group_rule_feature_set_atomic_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_actor uuid; v_agency uuid; v_environment text; v_group_id uuid; v_assignment_id uuid; v_version_id uuid;
  v_request_id uuid; v_expected bigint; v_enabled boolean; v_from date; v_release_id text; v_reason text; v_purpose text; v_hash text; v_receipt jsonb;
  v_group public.weekly_source_groups%rowtype; v_assignment public.weekly_source_group_rule_assignments%rowtype;
  v_latest private.cloudtms_database_releases%rowtype; v_values jsonb;
  v_member record; v_manifest jsonb; v_shape jsonb; v_result jsonb; v_changes jsonb:='[]'::jsonb;
  v_entry jsonb; v_apply_values jsonb; v_scope_date date; v_agency_invoice boolean:=false;
begin
  perform private._weekly_source_settings_assert_request_v1(p_request,array['actor_user_id','agency_id','environment','source_group_id','assignment_id',
    'rule_set_version_id','effective_from_week_ending','expected_assignment_version','feature_enabled','feature_purpose','release_id','reason','request_id',
    'client_settings_versions'],'WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID');
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid; v_agency:=(p_request->>'agency_id')::uuid; v_group_id:=(p_request->>'source_group_id')::uuid;
    v_assignment_id:=(p_request->>'assignment_id')::uuid; v_version_id:=nullif(p_request->>'rule_set_version_id','')::uuid;
    v_request_id:=(p_request->>'request_id')::uuid; v_expected:=(p_request->>'expected_assignment_version')::bigint;
    v_enabled:=(p_request->>'feature_enabled')::boolean; v_from:=(p_request->>'effective_from_week_ending')::date;
    v_release_id:=p_request->>'release_id'; v_reason:=btrim(p_request->>'reason'); v_purpose:=p_request->>'feature_purpose';
  exception when others then raise exception 'WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID' using errcode='22023'; end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  v_environment:=private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
  if v_group_id is null or v_assignment_id is null or v_request_id is null or v_expected is null or v_from is null
    or jsonb_typeof(p_request->'feature_enabled') is distinct from 'boolean'
    or not p_request ? 'rule_set_version_id' or v_release_id is null or v_reason is null or char_length(v_reason) not between 1 and 500
    or v_purpose is null or v_purpose not in ('RULE_SET','SIGNED_CHECK_ONLY') or (v_version_id is null)<>(v_purpose='SIGNED_CHECK_ONLY')
    or p_request->>'effective_from_week_ending'<>to_char(v_from,'YYYY-MM-DD') then
    raise exception 'WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID' using errcode='22023'; end if;
  v_hash:=encode(extensions.digest(convert_to(p_request::text,'UTF8'),'sha256'),'hex');
  perform pg_advisory_xact_lock(hashtextextended('weekly-source-cycle-lifecycle:'||v_group_id::text,0));
  -- Match the Client owner's client-before-group lock order. Recheck the exact
  -- dated membership after taking the group/assignment locks below.
  if (select count(distinct gc.client_id) from public.weekly_source_group_clients gc
    where gc.source_group_id=v_group_id and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]')
      && daterange(v_from-6,'infinity'::date,'[]'))>100 then
    raise exception 'WEEKLY_SOURCE_RULE_ACTIVATION_SCOPE_TOO_LARGE' using errcode='22023'; end if;
  for v_member in select distinct gc.client_id from public.weekly_source_group_clients gc
    where gc.source_group_id=v_group_id and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]')
      && daterange(v_from-6,'infinity'::date,'[]') order by gc.client_id
  loop
    perform pg_advisory_xact_lock(hashtextextended('weekly-source-settings-client:'||v_member.client_id::text,0));
  end loop;
  select * into v_group from public.weekly_source_groups where id=v_group_id for update;
  if not found or v_group.agency_id<>v_agency or v_group.environment<>v_environment or not v_group.active or v_group.source_family<>'ROSTER' then
    raise exception 'WEEKLY_SOURCE_RULE_GROUP_NOT_APPLICABLE' using errcode='22023'; end if;
  select * into v_assignment from public.weekly_source_group_rule_assignments where id=v_assignment_id and source_group_id=v_group_id for update;
  if not found or v_assignment.rule_set_version_id is distinct from v_version_id or v_assignment.effective_from_week_ending<>v_from then
    raise exception 'WEEKLY_SOURCE_RULE_ASSIGNMENT_NOT_FOUND' using errcode='22023'; end if;
  -- Latest record, not latest successful record: an APPLYING/FAILED successor holds activation.
  select * into v_latest from private.cloudtms_database_releases order by started_at_utc desc,release_id desc limit 1;
  if not found or v_latest.release_id<>v_release_id or v_latest.status<>'VERIFIED'
    or v_latest.completed_at_utc is null or v_latest.installed_contract_sha256 is distinct from v_latest.repository_contract_sha256 then
    raise exception 'WEEKLY_SOURCE_RULE_VERIFIED_RELEASE_REQUIRED' using errcode='55000'; end if;
  if v_assignment.feature_request_id=v_request_id then
    if v_assignment.feature_request_sha256<>v_hash then raise exception 'WEEKLY_SOURCE_RULE_REQUEST_REUSED' using errcode='22023'; end if;
    return v_assignment.feature_receipt_json||jsonb_build_object('replayed',true);
  end if;
  if v_assignment.version<>v_expected then raise exception 'WEEKLY_SOURCE_RULE_ASSIGNMENT_STALE' using errcode='40001'; end if;
  perform 1 from public.clients c where exists(select 1 from public.weekly_source_group_clients gc
    where gc.client_id=c.id and gc.source_group_id=v_group_id
      and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]') && daterange(v_from-6,coalesce(v_assignment.effective_to_week_ending,'infinity'::date),'[]'))
    order by c.id for update;
  perform 1 from public.client_settings cs where exists(select 1 from public.weekly_source_group_clients gc
    where gc.client_id=cs.client_id and gc.source_group_id=v_group_id
      and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]') && daterange(v_from-6,coalesce(v_assignment.effective_to_week_ending,'infinity'::date),'[]'))
    order by cs.id for update;
  perform 1 from public.contracts c where exists(select 1 from public.weekly_source_group_clients gc
    where gc.client_id=c.client_id and gc.source_group_id=v_group_id
      and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]') && daterange(v_from-6,coalesce(v_assignment.effective_to_week_ending,'infinity'::date),'[]'))
    order by c.id for share;
  if v_assignment.feature_enabled=v_enabled then raise exception 'WEEKLY_SOURCE_RULE_FEATURE_ALREADY_SET' using errcode='22023'; end if;
  select v.values_json into v_values from public.weekly_source_rule_set_versions v join public.weekly_source_rule_sets r on r.id=v.rule_set_id
    where v.id=v_version_id and r.agency_id=v_agency and r.environment=v_environment;
  if v_version_id is not null and not found then raise exception 'WEEKLY_SOURCE_RULE_VERSION_NOT_FOUND' using errcode='22023'; end if;
  v_agency_invoice:=coalesce(v_values->>'invoice_sender'='AGENCY_INVOICE',false);
  if v_agency_invoice then
    v_manifest:=p_request->'client_settings_versions';
    if jsonb_typeof(v_manifest) is distinct from 'array' or jsonb_array_length(v_manifest) not between 1 and 100 then
      raise exception 'WEEKLY_SOURCE_RULE_CLIENT_CAS_MANIFEST_REQUIRED' using errcode='22023'; end if;
    for v_entry in select value from jsonb_array_elements(v_manifest) loop
      perform private._weekly_source_settings_assert_request_v1(v_entry,array['client_id','expected_settings_version'],'WEEKLY_SOURCE_RULE_CLIENT_CAS_MANIFEST_INVALID');
      if jsonb_typeof(v_entry->'client_id') is distinct from 'string' or v_entry->>'client_id' !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
        or jsonb_typeof(v_entry->'expected_settings_version') is distinct from 'string'
        or v_entry->>'expected_settings_version' !~ '^[0-9a-f]{64}$' then
        raise exception 'WEEKLY_SOURCE_RULE_CLIENT_CAS_MANIFEST_INVALID' using errcode='22023'; end if;
    end loop;
    if (select count(distinct value->>'client_id') from jsonb_array_elements(v_manifest))<>jsonb_array_length(v_manifest)
      or (select count(distinct gc.client_id) from public.weekly_source_group_clients gc where gc.source_group_id=v_group_id
        and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]') && daterange(v_from-6,coalesce(v_assignment.effective_to_week_ending,'infinity'::date),'[]'))<>jsonb_array_length(v_manifest)
      or exists(select 1 from jsonb_array_elements(v_manifest) x where not exists(select 1 from public.weekly_source_group_clients gc
        where gc.source_group_id=v_group_id and gc.client_id=(x.value->>'client_id')::uuid
          and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]') && daterange(v_from-6,coalesce(v_assignment.effective_to_week_ending,'infinity'::date),'[]'))) then
      raise exception 'WEEKLY_SOURCE_RULE_CLIENT_CAS_MANIFEST_INVALID' using errcode='22023'; end if;
    for v_member in select gc.client_id,greatest(v_from-6,min(gc.valid_from)) as scope_date
      from public.weekly_source_group_clients gc where gc.source_group_id=v_group_id
        and daterange(gc.valid_from,coalesce(gc.valid_to,'infinity'::date),'[]') && daterange(v_from-6,coalesce(v_assignment.effective_to_week_ending,'infinity'::date),'[]')
      group by gc.client_id order by gc.client_id
    loop
      v_scope_date:=v_member.scope_date;
      if v_scope_date<(transaction_timestamp() at time zone 'Europe/London')::date then
        raise exception 'WEEKLY_SOURCE_RULE_FUTURE_WEEK_REQUIRED' using errcode='22023'; end if;
      v_shape:=private._weekly_source_settings_client_shape_v1(v_agency,v_environment,v_member.client_id,v_scope_date);
      if v_shape->>'settings_version'<>(select value->>'expected_settings_version' from jsonb_array_elements(v_manifest) where (value->>'client_id')::uuid=v_member.client_id) then
        raise exception 'WEEKLY_SOURCE_RULE_CLIENT_SETTINGS_STALE' using errcode='40001'; end if;
      if v_enabled then
        if exists(select 1 from public.weekly_source_client_policies cp where cp.source_group_id=v_group_id and cp.client_id=v_member.client_id
          and cp.effective_from>v_scope_date and cp.effective_from<=coalesce(v_assignment.effective_to_week_ending,'infinity'::date)) then
          raise exception 'WEEKLY_SOURCE_RULE_ACTIVATION_FUTURE_SETTINGS_REFUSED' using errcode='22023'; end if;
        v_changes:=v_changes||jsonb_build_array(jsonb_build_object('client_id',v_member.client_id,'scope_date',v_scope_date,
          'before_values',v_shape->'settings','before_settings_version',v_shape->>'settings_version',
          'before_policy_id',(select cp.id from public.weekly_source_client_policies cp where cp.source_group_id=v_group_id and cp.client_id=v_member.client_id
            and v_scope_date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date))));
      else
        select value into v_entry from jsonb_array_elements(v_assignment.feature_receipt_json->'client_changes')
          where (value->>'client_id')::uuid=v_member.client_id;
        if not found or v_entry->>'after_settings_version' is distinct from v_shape->>'settings_version'
          or (v_entry->>'scope_date')::date<>v_scope_date then
          raise exception 'WEEKLY_SOURCE_RULE_CLIENT_RESTORE_STALE' using errcode='40001'; end if;
        v_changes:=v_changes||jsonb_build_array(v_entry);
      end if;
    end loop;
  elsif p_request ? 'client_settings_versions' and p_request->'client_settings_versions'<>'[]'::jsonb then
    raise exception 'WEEKLY_SOURCE_RULE_CLIENT_CAS_MANIFEST_INVALID' using errcode='22023';
  end if;
  if v_enabled then
    if v_from<(transaction_timestamp() at time zone 'Europe/London')::date then
      raise exception 'WEEKLY_SOURCE_RULE_FUTURE_WEEK_REQUIRED' using errcode='22023'; end if;
    perform 1 from public.weekly_source_cycles c where c.source_group_id=v_group_id
      and c.finalisation_week_ending between v_from and coalesce(v_assignment.effective_to_week_ending,'infinity'::date) order by c.id for update;
    if exists(select 1 from public.weekly_source_cycles c where c.source_group_id=v_group_id
      and c.finalisation_week_ending between v_from and coalesce(v_assignment.effective_to_week_ending,'infinity'::date)
      and (c.current_final_revision_id is not null or c.state in ('FINALISING','FINALISED','CORRECTION_IN_PROGRESS')
        or exists(select 1 from public.weekly_source_final_revisions f where f.source_cycle_id=c.id))) then
      raise exception 'WEEKLY_SOURCE_RULE_FINALISED_WEEK_REFUSED' using errcode='55000'; end if;
    perform private._weekly_source_rule_group_context_v1(v_group_id,v_from,v_assignment.effective_to_week_ending,v_values,not v_agency_invoice);
  end if;
  -- Publish the gate and the approved Client policy transition together. The
  -- feature receipt and Client writes cannot survive any refusal below.
  update public.weekly_source_group_rule_assignments set feature_enabled=v_enabled,feature_purpose=v_purpose,
    feature_changed_by_user_id=v_actor,feature_changed_at_utc=transaction_timestamp(),feature_change_reason=v_reason,feature_release_id=v_release_id,
    feature_request_id=v_request_id,feature_request_sha256=v_hash,feature_receipt_json='{}'::jsonb where id=v_assignment_id;
  if v_agency_invoice then
    v_manifest:='[]'::jsonb;
    for v_entry in select value from jsonb_array_elements(v_changes) order by value->>'client_id' loop
      v_scope_date:=(v_entry->>'scope_date')::date;
      v_shape:=private._weekly_source_settings_client_shape_v1(v_agency,v_environment,(v_entry->>'client_id')::uuid,v_scope_date);
      v_apply_values:=v_entry->'before_values'||jsonb_build_object('effective_from',v_scope_date);
      if v_enabled then v_apply_values:=v_apply_values||jsonb_build_object('authority_mode','SOURCE_AUTHORITY','document_mode','CHECK_ONLY','self_bill_enabled',false); end if;
      v_result:=public.weekly_source_client_settings_save_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'agency_id',v_agency,'environment',v_environment,
        'client_id',v_entry->>'client_id','expected_settings_version',v_shape->>'settings_version','settings',v_apply_values));
      if coalesce((v_result->>'ok')::boolean,false) is not true then raise exception 'WEEKLY_SOURCE_RULE_CLIENT_TRANSITION_FAILED' using errcode='55000'; end if;
      v_shape:=private._weekly_source_settings_client_shape_v1(v_agency,v_environment,(v_entry->>'client_id')::uuid,v_scope_date);
      v_manifest:=v_manifest||jsonb_build_array(v_entry||jsonb_build_object(
        case when v_enabled then 'after_values' else 'restored_values' end,v_shape->'settings',
        case when v_enabled then 'after_settings_version' else 'restored_settings_version' end,v_shape->>'settings_version',
        case when v_enabled then 'after_policy_id' else 'restored_policy_id' end,(select cp.id from public.weekly_source_client_policies cp
          where cp.source_group_id=v_group_id and cp.client_id=(v_entry->>'client_id')::uuid
            and v_scope_date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date))));
    end loop;
    v_changes:=v_manifest;
    if v_enabled then perform private._weekly_source_rule_group_context_v1(v_group_id,v_from,v_assignment.effective_to_week_ending,v_values,true); end if;
  end if;
  v_receipt:=jsonb_build_object('contract','WEEKLY_SOURCE_RULE_FEATURE_RECEIPT_V1','assignment_id',v_assignment_id,
    'assignment_version',v_assignment.version+1,'source_group_id',v_group_id,'rule_set_version_id',v_version_id,
    'effective_from_week_ending',v_from,'feature_enabled',v_enabled,'feature_purpose',v_purpose,'release_id',v_release_id,
    'actor_user_id',v_actor,'changed_at_utc',transaction_timestamp(),'reason',v_reason,'request_id',v_request_id,'client_changes',v_changes,'replayed',false);
  update public.weekly_source_group_rule_assignments set feature_enabled=v_enabled,feature_purpose=v_purpose,
    feature_changed_by_user_id=v_actor,feature_changed_at_utc=transaction_timestamp(),feature_change_reason=v_reason,feature_release_id=v_release_id,
    feature_request_id=v_request_id,feature_request_sha256=v_hash,feature_receipt_json=v_receipt,version=version+1 where id=v_assignment_id;
  return v_receipt;
end;
$function$;

-- Complete current TEST setup owners from 09102026_1250; only F1's narrow
-- configured-agency derivation changes. Existing NHSP and null choices retain
-- their original body, CAS, effective dates, locks and settings version owner.
CREATE OR REPLACE FUNCTION private._weekly_source_settings_client_shape_v1(p_agency_id uuid, p_environment text, p_client_id uuid, p_effective_date date)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_environment text:=private._weekly_source_settings_deployment_v1(p_agency_id,p_environment);
  v_membership public.weekly_source_group_clients%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_policy public.weekly_source_client_policies%rowtype;
  v_scope_date date:=coalesce(p_effective_date,current_date);
  v_membership_count integer;
  v_policy_count integer;
  v_family_count integer;
  v_mode_count integer;
  v_self_bill_count integer;
  v_contract_count integer;
  v_client_nhsp_basis boolean:=false;
  v_client_agency_basis boolean:=false;
  v_derived_family text;
  v_derived_authority text;
  v_derived_document text;
  v_derived_self_bill boolean;
  v_configured boolean:=false;
  v_eligible boolean:=false;
  v_expense_profile_supported boolean:=false;
  v_settings jsonb;
  v_capabilities jsonb;
  v_version text;
  v_default_group_id uuid;
  v_nhsp_group_count integer;
begin
  if p_client_id is null or not exists(select 1 from public.clients client where client.id=p_client_id) then
    raise exception 'WEEKLY_SOURCE_SETTINGS_CLIENT_NOT_FOUND' using errcode='22023';
  end if;

  select pg_catalog.count(*),
         pg_catalog.count(distinct case
           when contract.weekly_timesheet_source::text='NHSP' then 'NHSP'
           when contract.weekly_timesheet_source::text='HEALTHROSTER' then 'ROSTER'
         end),
         pg_catalog.count(distinct case
           when contract.weekly_timesheet_source::text='NHSP'
             or coalesce(contract.no_timesheet_required,false) then 'SOURCE_AUTHORITY'
           else 'TIMESHEET_AUTHORITY'
         end),
         pg_catalog.count(distinct contract.self_bill),
         pg_catalog.min(case
           when contract.weekly_timesheet_source::text='NHSP' then 'NHSP'
           when contract.weekly_timesheet_source::text='HEALTHROSTER' then 'ROSTER'
         end),
         pg_catalog.min(case
           when contract.weekly_timesheet_source::text='NHSP'
             or coalesce(contract.no_timesheet_required,false) then 'SOURCE_AUTHORITY'
           else 'TIMESHEET_AUTHORITY'
         end),
         pg_catalog.bool_and(contract.self_bill)
  into v_contract_count,v_family_count,v_mode_count,v_self_bill_count,
       v_derived_family,v_derived_authority,v_derived_self_bill
  from public.contracts contract
  where contract.client_id=p_client_id
    and contract.weekly_timesheet_source::text<>'NONE'
    and contract.end_date>=v_scope_date;


  -- Client setup precedes contracts. Only the currently effective dedicated
  -- NHSP client mode may supply this pre-contract configuration basis.
  -- Existing source contracts (including conflicting ones) remain authoritative.
  if v_contract_count=0 and coalesce((
    select settings.is_nhsp
      and not coalesce(settings.requires_hr,false)
      and not coalesce(settings.autoprocess_hr,false)
      and not coalesce(settings.no_timesheet_required,false)
    from public.client_settings settings
    where settings.client_id=p_client_id
      and (settings.effective_from is null or settings.effective_from<=v_scope_date)
    order by settings.effective_from desc nulls last,settings.updated_at desc,settings.id desc
    limit 1
  ),false) then
    v_derived_family:='NHSP';
    v_derived_authority:='SOURCE_AUTHORITY';
    v_derived_self_bill:=true;
    v_family_count:=1;
    v_mode_count:=1;
    v_self_bill_count:=1;
    v_client_nhsp_basis:=true;
  end if;

  -- F1: saved disabled agency assignment is configuration evidence only.
  v_client_agency_basis:=private.weekly_source_configured_agency_invoice_v1(
    p_client_id,null,p_agency_id,v_environment,v_scope_date);
  if v_client_agency_basis then
    v_derived_authority:='SOURCE_AUTHORITY';
    v_derived_self_bill:=false;
  end if;

  v_derived_document:=case v_derived_authority
    when 'SOURCE_AUTHORITY' then 'CHECK_ONLY'
    when 'TIMESHEET_AUTHORITY' then 'INVOICE_EVIDENCE_REQUIRED'
  end;
  v_eligible:=(v_contract_count>0 or v_client_nhsp_basis) and v_family_count=1 and v_mode_count=1
    and v_self_bill_count=1
    and ((v_derived_authority='SOURCE_AUTHORITY' and (v_derived_self_bill or v_client_agency_basis))
      or (v_derived_authority='TIMESHEET_AUTHORITY' and not v_derived_self_bill));

  if v_derived_family='NHSP' then
    select pg_catalog.count(*) into v_nhsp_group_count
    from public.weekly_source_groups source_group
    where source_group.agency_id=p_agency_id
      and source_group.environment=v_environment
      and source_group.source_family='NHSP'
      and source_group.active;
    if v_nhsp_group_count>1 then
      raise exception 'WEEKLY_SOURCE_NHSP_GROUP_CARDINALITY_INVALID' using errcode='55000';
    elsif v_nhsp_group_count=1 then
      select source_group.id into strict v_default_group_id
      from public.weekly_source_groups source_group
      where source_group.agency_id=p_agency_id
        and source_group.environment=v_environment
        and source_group.source_family='NHSP'
        and source_group.active;
    end if;
  end if;

  select pg_catalog.count(*) into v_membership_count
  from public.weekly_source_group_clients membership
  join public.weekly_source_groups source_group on source_group.id=membership.source_group_id
  where membership.client_id=p_client_id
    and source_group.agency_id=p_agency_id
    and source_group.environment=v_environment
    and v_scope_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date);
  if v_membership_count>1 then
    raise exception 'WEEKLY_SOURCE_SETTINGS_MEMBERSHIP_CARDINALITY_INVALID' using errcode='55000';
  end if;

  if v_membership_count=1 then
    select membership.*
    into strict v_membership
    from public.weekly_source_group_clients membership
    join public.weekly_source_groups source_group on source_group.id=membership.source_group_id
    where membership.client_id=p_client_id
      and source_group.agency_id=p_agency_id
      and source_group.environment=v_environment
      and v_scope_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date);
    select source_group.* into strict v_group
    from public.weekly_source_groups source_group
    where source_group.id=v_membership.source_group_id;

    select pg_catalog.count(*) into v_policy_count
    from public.weekly_source_client_policies policy
    where policy.source_group_id=v_group.id
      and policy.client_id=p_client_id
      and v_scope_date between policy.effective_from and coalesce(policy.effective_to,'infinity'::date);
    if v_policy_count<>1 then
      raise exception 'WEEKLY_SOURCE_SETTINGS_CLIENT_POLICY_CARDINALITY_INVALID' using errcode='55000';
    end if;
    select * into strict v_policy
    from public.weekly_source_client_policies policy
    where policy.source_group_id=v_group.id
      and policy.client_id=p_client_id
      and v_scope_date between policy.effective_from and coalesce(policy.effective_to,'infinity'::date);
    v_configured:=true;
    v_eligible:=v_eligible or true;
  end if;

  if v_configured then
    select exists(
      select 1
      from public.weekly_source_uploads upload
      join public.weekly_source_cycles cycle on cycle.id=upload.source_cycle_id
      join public.weekly_source_format_profiles profile on profile.id=upload.source_format_profile_id
      where cycle.source_group_id=v_group.id
        and profile.profile_json ? 'expense_column'
        and upload.state in ('SEALED','CURRENT','SUPERSEDED','CORRECTION_READY')
    ) or v_policy.source_fixed_expenses_enabled
    into v_expense_profile_supported;

    v_settings:=pg_catalog.jsonb_build_object(
      'source_group_id',v_group.id,
      -- A first Client policy is deliberately stored from 1900-01-01 so an
      -- existing prior-week source file can be imported immediately.  Present
      -- the requested scope date as the next editable date, however, so a
      -- later settings change creates a new effective-dated row instead of
      -- rewriting that historical baseline.
      'effective_from',case when v_policy.effective_from=date '1900-01-01'
        then v_scope_date else v_policy.effective_from end,
      'authority_mode',case when v_client_agency_basis then v_derived_authority else v_policy.authority_mode end,
      'document_mode',case when v_client_agency_basis then v_derived_document else v_policy.document_mode end,
      'self_bill_enabled',v_policy.self_bill_enabled,
      'self_bill_correction_presentation',v_policy.self_bill_correction_presentation,
      'source_fixed_expenses_enabled',v_policy.source_fixed_expenses_enabled,
      'source_expense_vat_enabled',v_policy.source_expense_vat_enabled,
      'weekly_rate_classification_method',v_policy.weekly_rate_classification_method,
      'duration_break_tie_rule',case when v_policy.weekly_rate_classification_method='SPLIT_RATE_WINDOWS'
        then v_policy.duration_break_tie_rule else null end,
      'candidate_queries_enabled',v_policy.candidate_queries_enabled,
      'manager_queries_enabled',v_policy.manager_queries_enabled,
      'manager_query_recipient',case when v_policy.manager_queries_enabled then v_policy.manager_query_recipient else null end,
      'completed_pack_copy_enabled',v_policy.completed_pack_copy_enabled,
      'completed_pack_recipient',case when v_policy.completed_pack_copy_enabled then v_policy.completed_pack_recipient else null end
    );
    v_version:=pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
      pg_catalog.jsonb_build_object(
        'membership_id',v_membership.id,'membership_from',v_membership.valid_from,
        'membership_to',v_membership.valid_to,'policy_id',v_policy.id,
        'policy_from',v_policy.effective_from,'policy_to',v_policy.effective_to,
        'group_version',v_group.version,'settings',v_settings
      )::text,'UTF8'),'sha256'),'hex');
    v_capabilities:=pg_catalog.jsonb_build_object(
      'source_family',v_group.source_family,
      'authority_mode',case when v_client_agency_basis then v_derived_authority else v_policy.authority_mode end,
      'document_mode',case when v_client_agency_basis then v_derived_document else v_policy.document_mode end,
      'self_bill_enabled',v_policy.self_bill_enabled,
      'show_query_settings',v_client_agency_basis or (v_policy.authority_mode='SOURCE_AUTHORITY' and v_policy.document_mode='CHECK_ONLY'),
      'show_completed_pack',v_policy.document_mode in ('CHECK_ONLY','INVOICE_EVIDENCE_REQUIRED'),
      'show_rate_settings',true,
      'show_source_expenses',v_expense_profile_supported,
      'show_self_bill_correction',v_policy.self_bill_enabled and v_group.source_family='ROSTER'
    );
  else
    v_settings:=pg_catalog.jsonb_build_object(
      'source_group_id',case when v_derived_family='NHSP' then v_default_group_id else null end,
      'effective_from',v_scope_date,
      'authority_mode',v_derived_authority,
      'document_mode',v_derived_document,
      'self_bill_enabled',v_derived_self_bill,
      'self_bill_correction_presentation',case when v_derived_self_bill and v_derived_family='ROSTER'
        then 'FULL_REVERSAL_REPLACEMENT' else null end,
      'source_fixed_expenses_enabled',false,
      'source_expense_vat_enabled',false,
      'weekly_rate_classification_method','SPLIT_RATE_WINDOWS',
      'duration_break_tie_rule','EARLIEST_LONGEST_PORTION',
      'candidate_queries_enabled',v_derived_authority='SOURCE_AUTHORITY' and v_derived_document='CHECK_ONLY',
      'manager_queries_enabled',v_derived_authority='SOURCE_AUTHORITY' and v_derived_document='CHECK_ONLY',
      'manager_query_recipient',case when v_derived_authority='SOURCE_AUTHORITY' and v_derived_document='CHECK_ONLY'
        then (select nullif(pg_catalog.btrim(client.ts_queries_email),'') from public.clients client where client.id=p_client_id) end,
      'completed_pack_copy_enabled',false,
      'completed_pack_recipient',null
    );
    v_version:=pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
      'UNCONFIGURED:'||p_client_id::text||':'||v_scope_date::text||':'||coalesce(v_derived_family,'')||':'||coalesce(v_derived_authority,'')||':'||coalesce(v_default_group_id::text,''),
      'UTF8'),'sha256'),'hex');
    v_capabilities:=pg_catalog.jsonb_build_object(
      'source_family',v_derived_family,
      'authority_mode',v_derived_authority,
      'document_mode',v_derived_document,
      'self_bill_enabled',v_derived_self_bill,
      'show_query_settings',v_derived_authority='SOURCE_AUTHORITY' and v_derived_document='CHECK_ONLY',
      'show_completed_pack',v_derived_document in ('CHECK_ONLY','INVOICE_EVIDENCE_REQUIRED'),
      'show_rate_settings',v_eligible,
      'show_source_expenses',false,
      'show_self_bill_correction',v_derived_self_bill and v_derived_family='ROSTER'
    );
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'eligible',v_eligible,'configured',v_configured,
    'client_id',p_client_id,'effective_date',v_scope_date,
    'settings_version',v_version,'capabilities',v_capabilities,
    'settings',v_settings,
    'source_groups',private._weekly_source_settings_group_list_v1(
      p_agency_id,v_environment,coalesce(v_group.source_family,v_derived_family),true
    )
  );
end;
$function$;

CREATE OR REPLACE FUNCTION public.weekly_source_client_settings_save_atomic_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_actor uuid;
  v_agency uuid;
  v_environment text;
  v_client uuid;
  v_expected text;
  v_input jsonb;
  v_effective_from date;
  v_current_shape jsonb;
  v_group_id uuid;
  v_group public.weekly_source_groups%rowtype;
  v_old_group_id uuid;
  v_membership public.weekly_source_group_clients%rowtype;
  v_policy public.weekly_source_client_policies%rowtype;
  v_policy_found boolean:=false;
  v_contract_count integer;
  v_client_nhsp_basis boolean:=false;
  v_client_agency_basis boolean:=false;
  v_family_count integer;
  v_mode_count integer;
  v_self_bill_count integer;
  v_family text;
  v_authority text;
  v_document text;
  v_self_bill boolean;
  v_correction text;
  v_source_expenses boolean;
  v_source_expense_vat boolean;
  v_rate_method text;
  v_tie_rule text;
  v_candidate_queries boolean;
  v_manager_queries boolean;
  v_manager_recipient text;
  v_completed_copy boolean;
  v_completed_recipient text;
  v_expense_supported boolean;
  v_prior_end date;
  v_nhsp_group_count integer;
  v_membership_history_count integer;
  v_policy_history_count integer;
  v_membership_effective_from date;
  v_policy_effective_from date;
begin
  perform private._weekly_source_settings_assert_request_v1(
    p_request,array[
      'actor_user_id','agency_id','environment','client_id',
      'expected_settings_version','settings'
    ],'WEEKLY_SOURCE_CLIENT_SETTINGS_SAVE_REQUEST_INVALID'
  );
  v_input:=p_request->'settings';
  perform private._weekly_source_settings_assert_request_v1(
    v_input,array[
      'source_group_id','effective_from','authority_mode','document_mode','self_bill_enabled',
      'self_bill_correction_presentation','source_fixed_expenses_enabled','source_expense_vat_enabled',
      'weekly_rate_classification_method','duration_break_tie_rule',
      'candidate_queries_enabled','manager_queries_enabled','manager_query_recipient',
      'completed_pack_copy_enabled','completed_pack_recipient'
    ],'WEEKLY_SOURCE_CLIENT_SETTINGS_VALUES_INVALID'
  );
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_agency:=(p_request->>'agency_id')::uuid;
    v_client:=(p_request->>'client_id')::uuid;
    v_expected:=pg_catalog.btrim(p_request->>'expected_settings_version');
    v_effective_from:=coalesce(nullif(v_input->>'effective_from','')::date,current_date);
    v_group_id:=nullif(v_input->>'source_group_id','')::uuid;
    v_correction:=nullif(pg_catalog.upper(pg_catalog.btrim(v_input->>'self_bill_correction_presentation')),'');
    v_source_expenses:=(v_input->>'source_fixed_expenses_enabled')::boolean;
    v_source_expense_vat:=(v_input->>'source_expense_vat_enabled')::boolean;
    v_rate_method:=pg_catalog.upper(pg_catalog.btrim(v_input->>'weekly_rate_classification_method'));
    v_tie_rule:=nullif(pg_catalog.upper(pg_catalog.btrim(v_input->>'duration_break_tie_rule')),'');
    v_candidate_queries:=(v_input->>'candidate_queries_enabled')::boolean;
    v_manager_queries:=(v_input->>'manager_queries_enabled')::boolean;
    v_manager_recipient:=nullif(pg_catalog.btrim(v_input->>'manager_query_recipient'),'');
    v_completed_copy:=(v_input->>'completed_pack_copy_enabled')::boolean;
    v_completed_recipient:=nullif(pg_catalog.btrim(v_input->>'completed_pack_recipient'),'');
  exception when others then
    raise exception 'WEEKLY_SOURCE_CLIENT_SETTINGS_SAVE_REQUEST_INVALID' using errcode='22023';
  end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  v_environment:=private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
  if v_effective_from<current_date or nullif(v_expected,'') is null then
    raise exception 'WEEKLY_SOURCE_CLIENT_SETTINGS_SAVE_REQUEST_INVALID' using errcode='22023';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly-source-settings-client:'||v_client::text,0
  ));
  v_current_shape:=private._weekly_source_settings_client_shape_v1(
    v_agency,v_environment,v_client,v_effective_from
  );
  if v_current_shape->>'settings_version'<>v_expected then
    raise exception 'WEEKLY_SOURCE_SETTINGS_STALE' using errcode='40001';
  end if;
  if not (v_current_shape->>'eligible')::boolean then
    raise exception 'WEEKLY_SOURCE_CLIENT_SETTINGS_NOT_APPLICABLE' using errcode='22023';
  end if;

  select pg_catalog.count(*),
         pg_catalog.count(distinct case
           when contract.weekly_timesheet_source::text='NHSP' then 'NHSP'
           when contract.weekly_timesheet_source::text='HEALTHROSTER' then 'ROSTER'
         end),
         pg_catalog.count(distinct case
           when contract.weekly_timesheet_source::text='NHSP'
             or coalesce(contract.no_timesheet_required,false) then 'SOURCE_AUTHORITY'
           else 'TIMESHEET_AUTHORITY'
         end),
         pg_catalog.count(distinct contract.self_bill),
         pg_catalog.min(case
           when contract.weekly_timesheet_source::text='NHSP' then 'NHSP'
           when contract.weekly_timesheet_source::text='HEALTHROSTER' then 'ROSTER'
         end),
         pg_catalog.min(case
           when contract.weekly_timesheet_source::text='NHSP'
             or coalesce(contract.no_timesheet_required,false) then 'SOURCE_AUTHORITY'
           else 'TIMESHEET_AUTHORITY'
         end),
         pg_catalog.bool_and(contract.self_bill)
  into v_contract_count,v_family_count,v_mode_count,v_self_bill_count,
       v_family,v_authority,v_self_bill
  from public.contracts contract
  where contract.client_id=v_client
    and contract.weekly_timesheet_source::text<>'NONE'
    and contract.end_date>=v_effective_from;

  -- Client setup precedes contracts. Only the currently effective dedicated
  -- NHSP client mode may supply this pre-contract configuration basis.
  -- Existing source contracts (including conflicting ones) remain authoritative.
  if v_contract_count=0 and coalesce((
    select settings.is_nhsp
      and not coalesce(settings.requires_hr,false)
      and not coalesce(settings.autoprocess_hr,false)
      and not coalesce(settings.no_timesheet_required,false)
    from public.client_settings settings
    where settings.client_id=v_client
      and (settings.effective_from is null or settings.effective_from<=v_effective_from)
    order by settings.effective_from desc nulls last,settings.updated_at desc,settings.id desc
    limit 1
  ),false) then
    v_family:='NHSP';
    v_authority:='SOURCE_AUTHORITY';
    v_self_bill:=true;
    v_family_count:=1;
    v_mode_count:=1;
    v_self_bill_count:=1;
    v_client_nhsp_basis:=true;
  end if;

  -- F1: derive the SOURCE/CHECK_ONLY exception only from an exact saved
  -- dated agency assignment and actual unsigned Client/Contract settings.
  v_client_agency_basis:=private.weekly_source_configured_agency_invoice_v1(
    v_client,v_group_id,v_agency,v_environment,v_effective_from);
  if v_client_agency_basis then
    v_authority:='SOURCE_AUTHORITY';
    v_self_bill:=false;
  end if;
  if (v_contract_count=0 and not v_client_nhsp_basis) or v_family_count<>1 or v_mode_count<>1 or v_self_bill_count<>1
     or (v_authority='SOURCE_AUTHORITY' and not v_self_bill and not v_client_agency_basis)
     or (v_authority='TIMESHEET_AUTHORITY' and v_self_bill) then
    raise exception 'WEEKLY_SOURCE_CLIENT_CONTRACT_POLICY_INCONSISTENT' using errcode='55000';
  end if;
  v_document:=case v_authority when 'SOURCE_AUTHORITY' then 'CHECK_ONLY'
    else 'INVOICE_EVIDENCE_REQUIRED' end;
  if (v_input ? 'authority_mode' and v_input->>'authority_mode'<>v_authority)
     or (v_input ? 'document_mode' and v_input->>'document_mode'<>v_document)
     or (v_input ? 'self_bill_enabled' and (v_input->>'self_bill_enabled')::boolean<>v_self_bill) then
    raise exception 'WEEKLY_SOURCE_CLIENT_READ_ONLY_POLICY_MISMATCH' using errcode='22023';
  end if;

  if v_family='NHSP' then
    select pg_catalog.count(*) into v_nhsp_group_count
    from public.weekly_source_groups source_group
    where source_group.agency_id=v_agency
      and source_group.environment=v_environment
      and source_group.source_family='NHSP'
      and source_group.active;
    if v_nhsp_group_count<>1 then
      raise exception 'WEEKLY_SOURCE_NHSP_GROUP_REQUIRED' using errcode='55000';
    end if;
    select source_group.id into strict v_group_id
    from public.weekly_source_groups source_group
    where source_group.agency_id=v_agency
      and source_group.environment=v_environment
      and source_group.source_family='NHSP'
      and source_group.active;
  end if;

  select * into v_group from public.weekly_source_groups source_group
  where source_group.id=v_group_id for update;
  if not found or v_group.agency_id<>v_agency or v_group.environment<>v_environment
     or not v_group.active or v_group.source_family<>v_family then
    raise exception 'WEEKLY_SOURCE_CLIENT_GROUP_NOT_APPLICABLE' using errcode='22023';
  end if;
  if v_rate_method not in ('SPLIT_RATE_WINDOWS','WHOLE_SHIFT_START_DAY')
     or (v_rate_method='SPLIT_RATE_WINDOWS' and v_tie_rule not in ('EARLIEST_LONGEST_PORTION','LATEST_LONGEST_PORTION')) then
    raise exception 'WEEKLY_SOURCE_CLIENT_RATE_SETTINGS_INVALID' using errcode='22023';
  end if;
  if v_rate_method<>'SPLIT_RATE_WINDOWS' then v_tie_rule:=null; end if;

  if v_self_bill and v_family='ROSTER' then
    if v_correction not in ('FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION') then
      raise exception 'WEEKLY_SOURCE_CLIENT_CORRECTION_SETTING_REQUIRED' using errcode='22023';
    end if;
  elsif v_correction is not null then
    raise exception 'WEEKLY_SOURCE_CLIENT_CORRECTION_SETTING_NOT_APPLICABLE' using errcode='22023';
  end if;

  select exists(
    select 1
    from public.weekly_source_uploads upload
    join public.weekly_source_cycles cycle on cycle.id=upload.source_cycle_id
    join public.weekly_source_format_profiles profile on profile.id=upload.source_format_profile_id
    where cycle.source_group_id=v_group_id
      and profile.profile_json ? 'expense_column'
      and upload.state in ('SEALED','CURRENT','SUPERSEDED','CORRECTION_READY')
  ) or coalesce((v_current_shape->'settings'->>'source_fixed_expenses_enabled')::boolean,false)
  into v_expense_supported;
  if v_client_agency_basis and (v_source_expenses or v_source_expense_vat) then
    raise exception 'WEEKLY_SOURCE_RULE_R13' using errcode='22023';
  end if;
  if v_source_expenses and not v_expense_supported then
    raise exception 'WEEKLY_SOURCE_CLIENT_SOURCE_EXPENSES_NOT_APPLICABLE' using errcode='22023';
  end if;
  if not v_source_expenses then v_source_expense_vat:=false; end if;

  if v_authority<>'SOURCE_AUTHORITY' then
    if v_candidate_queries or v_manager_queries or v_manager_recipient is not null
       or v_completed_copy or v_completed_recipient is not null then
      raise exception 'WEEKLY_SOURCE_CLIENT_QUERY_SETTINGS_NOT_APPLICABLE' using errcode='22023';
    end if;
  else
    if not v_manager_queries then v_manager_recipient:=null; end if;
    if v_manager_queries and v_manager_recipient is null then
      select nullif(pg_catalog.btrim(client.ts_queries_email),'') into v_manager_recipient
      from public.clients client where client.id=v_client;
    end if;
    if v_manager_queries and v_manager_recipient is null then
      raise exception 'WEEKLY_SOURCE_MANAGER_QUERY_RECIPIENT_REQUIRED' using errcode='22023';
    end if;
    if not v_completed_copy then v_completed_recipient:=null; end if;
    if v_completed_copy and v_completed_recipient is null then
      select coalesce(
        v_manager_recipient,
        nullif(pg_catalog.btrim(client.ts_queries_email),'')
      ) into v_completed_recipient
      from public.clients client where client.id=v_client;
    end if;
    if v_completed_copy and v_completed_recipient is null then
      raise exception 'WEEKLY_SOURCE_COMPLETED_PACK_RECIPIENT_REQUIRED' using errcode='22023';
    end if;
  end if;

  select membership.* into v_membership
  from public.weekly_source_group_clients membership
  join public.weekly_source_groups source_group on source_group.id=membership.source_group_id
  where membership.client_id=v_client
    and source_group.agency_id=v_agency and source_group.environment=v_environment
    and v_effective_from between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
  for update of membership;
  if found then v_old_group_id:=v_membership.source_group_id; end if;

  select pg_catalog.count(*) into v_membership_history_count
  from public.weekly_source_group_clients membership
  join public.weekly_source_groups source_group on source_group.id=membership.source_group_id
  where membership.client_id=v_client
    and source_group.agency_id=v_agency and source_group.environment=v_environment;
  v_membership_effective_from:=case when v_membership_history_count=0
    then date '1900-01-01' else v_effective_from end;

  if v_old_group_id is null then
    insert into public.weekly_source_group_clients(
      source_group_id,client_id,valid_from,created_by_user_id
    ) values (v_group_id,v_client,v_membership_effective_from,v_actor)
    returning * into v_membership;
  elsif v_old_group_id<>v_group_id then
    if v_membership.valid_from=v_effective_from then
      update public.weekly_source_group_clients
      set source_group_id=v_group_id
      where id=v_membership.id returning * into v_membership;
    else
      v_prior_end:=v_membership.valid_to;
      update public.weekly_source_group_clients set valid_to=v_effective_from-1
      where id=v_membership.id;
      insert into public.weekly_source_group_clients(
        source_group_id,client_id,valid_from,valid_to,created_by_user_id
      ) values (v_group_id,v_client,v_effective_from,v_prior_end,v_actor)
      returning * into v_membership;
    end if;
  end if;

  if v_old_group_id is not null then
    select * into v_policy
    from public.weekly_source_client_policies policy
    where policy.source_group_id=v_old_group_id and policy.client_id=v_client
      and v_effective_from between policy.effective_from and coalesce(policy.effective_to,'infinity'::date)
    for update;
    v_policy_found:=found;
  end if;
  select pg_catalog.count(*) into v_policy_history_count
  from public.weekly_source_client_policies policy
  join public.weekly_source_groups source_group on source_group.id=policy.source_group_id
  where policy.client_id=v_client
    and source_group.agency_id=v_agency and source_group.environment=v_environment;
  v_policy_effective_from:=case when v_policy_history_count=0
    then date '1900-01-01' else v_effective_from end;
  if v_policy_found and v_policy.effective_from=v_effective_from then
    update public.weekly_source_client_policies
    set source_group_id=v_group_id,authority_mode=v_authority,document_mode=v_document,
        self_bill_enabled=v_self_bill,self_bill_correction_presentation=v_correction,
        source_fixed_expenses_enabled=v_source_expenses,
        source_expense_vat_enabled=v_source_expense_vat,
        weekly_rate_classification_method=v_rate_method,duration_break_tie_rule=v_tie_rule,
        candidate_queries_enabled=v_candidate_queries,manager_queries_enabled=v_manager_queries,
        manager_query_recipient=v_manager_recipient,
        completed_pack_copy_enabled=v_completed_copy,completed_pack_recipient=v_completed_recipient,
        created_by_user_id=v_actor,created_at_utc=pg_catalog.transaction_timestamp()
    where id=v_policy.id;
  else
    if v_policy_found then
      v_prior_end:=v_policy.effective_to;
      update public.weekly_source_client_policies set effective_to=v_effective_from-1
      where id=v_policy.id;
    else
      v_prior_end:=null;
    end if;
    insert into public.weekly_source_client_policies(
      source_group_id,client_id,effective_from,effective_to,
      authority_mode,document_mode,self_bill_enabled,self_bill_correction_presentation,
      source_fixed_expenses_enabled,source_expense_vat_enabled,
      weekly_rate_classification_method,duration_break_tie_rule,
      candidate_queries_enabled,manager_queries_enabled,manager_query_recipient,
      completed_pack_copy_enabled,completed_pack_recipient,created_by_user_id
    ) values (
      v_group_id,v_client,v_policy_effective_from,v_prior_end,
      v_authority,v_document,v_self_bill,v_correction,
      v_source_expenses,v_source_expense_vat,v_rate_method,v_tie_rule,
      v_candidate_queries,v_manager_queries,v_manager_recipient,
      v_completed_copy,v_completed_recipient,v_actor
    );
  end if;

  if v_old_group_id is not null and v_old_group_id<>v_group_id then
    perform private._weekly_source_settings_stale_open_v1(v_old_group_id,v_client);
  end if;
  perform private._weekly_source_settings_stale_open_v1(v_group_id,v_client);
  return private._weekly_source_settings_client_shape_v1(
    v_agency,v_environment,v_client,v_effective_from
  );
exception when exclusion_violation then
  raise exception 'WEEKLY_SOURCE_CLIENT_SETTINGS_EFFECTIVE_RANGE_CONFLICT' using errcode='23P01';
end;
$function$;

alter function private._weekly_source_settings_client_shape_v1(uuid,text,uuid,date) owner to postgres;
revoke all on function private._weekly_source_settings_client_shape_v1(uuid,text,uuid,date) from public,anon,authenticated,service_role;
alter function public.weekly_source_client_settings_save_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_client_settings_save_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_client_settings_save_atomic_v1(jsonb) to service_role;

alter function private._weekly_source_rule_values_v1(jsonb) owner to postgres;
alter function private._weekly_source_rule_group_context_v1(uuid,date,date,jsonb,boolean) owner to postgres;
alter function private.weekly_source_is_import_authoritative_v1(uuid,uuid,date) owner to postgres;
alter function private.weekly_source_effective_rule_set_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_has_self_bill_rules_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_agency_invoice_check_only_v1(uuid) owner to postgres;
alter function private.weekly_source_signed_check_only_v1(uuid) owner to postgres;
alter function private.weekly_source_configured_agency_invoice_v1(uuid,uuid,uuid,text,date) owner to postgres;
alter function private.weekly_source_final_rule_version_guard_v1() owner to postgres;
revoke all on function private._weekly_source_rule_values_v1(jsonb),private._weekly_source_rule_group_context_v1(uuid,date,date,jsonb,boolean),
  private.weekly_source_is_import_authoritative_v1(uuid,uuid,date),private.weekly_source_effective_rule_set_v1(uuid,uuid),
  private.weekly_source_has_self_bill_rules_v1(uuid,uuid),private.weekly_source_agency_invoice_check_only_v1(uuid),
  private.weekly_source_signed_check_only_v1(uuid),
  private.weekly_source_configured_agency_invoice_v1(uuid,uuid,uuid,text,date),
  private.weekly_source_final_rule_version_guard_v1() from public,anon,authenticated,service_role;
alter function public.weekly_source_rule_sets_get_v1(jsonb) owner to postgres;
alter function public.weekly_source_rule_set_version_save_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_group_rule_assign_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_group_rule_feature_set_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_rule_sets_get_v1(jsonb),public.weekly_source_rule_set_version_save_atomic_v1(jsonb),
  public.weekly_source_group_rule_assign_atomic_v1(jsonb),public.weekly_source_group_rule_feature_set_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_rule_sets_get_v1(jsonb),public.weekly_source_rule_set_version_save_atomic_v1(jsonb),
  public.weekly_source_group_rule_assign_atomic_v1(jsonb),public.weekly_source_group_rule_feature_set_atomic_v1(jsonb) to service_role;

-- The explicit owner/revoke/grants above close these definitions themselves.
-- Root's same-commit final ACL gate verifies the updated 102-table inventory
-- and PP service union without replaying an older central definition here.
notify pgrst, 'reload schema';
commit;
