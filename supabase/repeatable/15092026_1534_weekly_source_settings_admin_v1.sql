-- Repeatable CloudTMS authority: weekly_source_settings_admin_v1
-- Service-only administration of the relevant Weekly source settings.  The
-- browser never supplies the Agency or deployment environment; this owner is
-- deliberately limited to the policy fields declared below.

\set ON_ERROR_STOP on

begin;

create or replace function private._weekly_source_settings_assert_request_v1(
  p_request jsonb,
  p_allowed_keys text[],
  p_reason text
) returns void
language plpgsql
immutable
security invoker
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1
       from pg_catalog.jsonb_object_keys(p_request) supplied(key)
       where not (supplied.key=any(p_allowed_keys))
     ) then
    raise exception '%',p_reason using errcode='22023';
  end if;
end;
$function$;

create or replace function private._weekly_source_settings_require_admin_v1(
  p_actor_user_id uuid
) returns void
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if p_actor_user_id is null or not exists(
    select 1
    from public.tms_users office_user
    where office_user.id=p_actor_user_id
      and office_user.is_active
      and pg_catalog.lower(pg_catalog.btrim(coalesce(office_user.role,'')))='admin'
  ) then
    raise exception 'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED' using errcode='42501';
  end if;
end;
$function$;

create or replace function private._weekly_source_settings_deployment_v1(
  p_agency_id uuid,
  p_environment text
) returns text
language plpgsql
immutable
security invoker
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_environment text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_environment,'')));
begin
  if p_agency_id is null or v_environment not in ('TEST','LIVE') then
    raise exception 'WEEKLY_SOURCE_SETTINGS_DEPLOYMENT_INVALID' using errcode='22023';
  end if;
  return v_environment;
end;
$function$;

create or replace function private._weekly_source_settings_group_list_v1(
  p_agency_id uuid,
  p_environment text,
  p_source_family text default null,
  p_active_only boolean default false
) returns jsonb
language sql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'id',source_group.id,
      'code',source_group.code,
      'display_name',source_group.display_name,
      'source_family',source_group.source_family,
      'timezone',source_group.timezone,
      'cutoff_weekday',source_group.cutoff_weekday,
      'cutoff_local_time',source_group.cutoff_local_time,
      'nhsp_report_heading_name',source_group.nhsp_report_heading_name,
      'active',source_group.active,
      'version',source_group.version
    ) order by source_group.display_name,source_group.code,source_group.id
  ),'[]'::jsonb)
  from public.weekly_source_groups source_group
  where source_group.agency_id=p_agency_id
    and source_group.environment=pg_catalog.upper(pg_catalog.btrim(p_environment))
    and (p_source_family is null or source_group.source_family=p_source_family)
    and (not p_active_only or source_group.active)
$function$;

create or replace function private._weekly_source_settings_client_shape_v1(
  p_agency_id uuid,
  p_environment text,
  p_client_id uuid,
  p_effective_date date
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
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

  v_derived_document:=case v_derived_authority
    when 'SOURCE_AUTHORITY' then 'CHECK_ONLY'
    when 'TIMESHEET_AUTHORITY' then 'INVOICE_EVIDENCE_REQUIRED'
  end;
  v_eligible:=v_contract_count>0 and v_family_count=1 and v_mode_count=1
    and v_self_bill_count=1
    and ((v_derived_authority='SOURCE_AUTHORITY' and v_derived_self_bill)
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
      'authority_mode',v_policy.authority_mode,
      'document_mode',v_policy.document_mode,
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
      'authority_mode',v_policy.authority_mode,
      'document_mode',v_policy.document_mode,
      'self_bill_enabled',v_policy.self_bill_enabled,
      'show_query_settings',v_policy.authority_mode='SOURCE_AUTHORITY' and v_policy.document_mode='CHECK_ONLY',
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

create or replace function public.weekly_source_client_settings_get_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_agency uuid;
  v_client uuid;
  v_date date;
begin
  perform private._weekly_source_settings_assert_request_v1(
    p_request,array['actor_user_id','agency_id','environment','client_id','effective_date'],
    'WEEKLY_SOURCE_CLIENT_SETTINGS_GET_REQUEST_INVALID'
  );
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_agency:=(p_request->>'agency_id')::uuid;
    v_client:=(p_request->>'client_id')::uuid;
    v_date:=coalesce(nullif(p_request->>'effective_date','')::date,current_date);
  exception when others then
    raise exception 'WEEKLY_SOURCE_CLIENT_SETTINGS_GET_REQUEST_INVALID' using errcode='22023';
  end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  return private._weekly_source_settings_client_shape_v1(v_agency,p_request->>'environment',v_client,v_date);
end;
$function$;

create or replace function public.weekly_source_global_settings_get_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_agency uuid;
  v_settings public.weekly_source_global_settings%rowtype;
begin
  perform private._weekly_source_settings_assert_request_v1(
    p_request,array['actor_user_id','agency_id','environment'],
    'WEEKLY_SOURCE_GLOBAL_SETTINGS_GET_REQUEST_INVALID'
  );
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_agency:=(p_request->>'agency_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_GLOBAL_SETTINGS_GET_REQUEST_INVALID' using errcode='22023';
  end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  perform private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
  select * into strict v_settings from public.weekly_source_global_settings where singleton;
  return pg_catalog.jsonb_build_object(
    'ok',true,'settings_version',v_settings.version,
    'settings',pg_catalog.jsonb_build_object(
      'candidate_reminder_minutes',extract(epoch from v_settings.candidate_reminder_after)::bigint/60,
      'candidate_response_deadline_minutes',extract(epoch from v_settings.candidate_response_deadline_after)::bigint/60,
      'manager_digest_minutes',extract(epoch from v_settings.manager_partial_digest_after)::bigint/60,
      'manager_manual_resend_cooldown_minutes',extract(epoch from v_settings.manager_manual_send_cooldown)::bigint/60,
      'candidate_manual_reminder_cooldown_minutes',extract(epoch from v_settings.candidate_manual_reminder_cooldown)::bigint/60,
      'manager_secure_link_days',extract(epoch from v_settings.manager_secure_link_lifetime)::bigint/86400
    )
  );
end;
$function$;

create or replace function public.weekly_source_source_groups_get_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_agency uuid;
  v_environment text;
begin
  perform private._weekly_source_settings_assert_request_v1(
    p_request,array['actor_user_id','agency_id','environment'],
    'WEEKLY_SOURCE_GROUP_SETTINGS_GET_REQUEST_INVALID'
  );
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_agency:=(p_request->>'agency_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_GROUP_SETTINGS_GET_REQUEST_INVALID' using errcode='22023';
  end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  v_environment:=private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
  return pg_catalog.jsonb_build_object(
    'ok',true,'source_groups',private._weekly_source_settings_group_list_v1(
      v_agency,v_environment,null,false
    )
  );
end;
$function$;

create or replace function private._weekly_source_settings_contract_shape_v1(
  p_agency_id uuid,
  p_environment text,
  p_contract_id uuid,
  p_effective_date date
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_environment text:=private._weekly_source_settings_deployment_v1(p_agency_id,p_environment);
  v_contract public.contracts%rowtype;
  v_client_shape jsonb;
  v_client_settings jsonb;
  v_client_capabilities jsonb;
  v_policy public.weekly_source_contract_policies%rowtype;
  v_policy_count integer;
  v_scope_date date:=coalesce(p_effective_date,current_date);
  v_configured boolean:=false;
  v_effective jsonb;
  v_settings jsonb;
  v_version text;
begin
  select * into v_contract from public.contracts contract where contract.id=p_contract_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_SETTINGS_CONTRACT_NOT_FOUND' using errcode='22023';
  end if;

  v_client_shape:=private._weekly_source_settings_client_shape_v1(
    p_agency_id,v_environment,v_contract.client_id,v_scope_date
  );
  v_client_settings:=v_client_shape->'settings';
  v_client_capabilities:=v_client_shape->'capabilities';

  select pg_catalog.count(*) into v_policy_count
  from public.weekly_source_contract_policies policy
  where policy.contract_id=p_contract_id
    and v_scope_date between policy.effective_from and coalesce(policy.effective_to,'infinity'::date);
  if v_policy_count>1 then
    raise exception 'WEEKLY_SOURCE_SETTINGS_CONTRACT_POLICY_CARDINALITY_INVALID' using errcode='55000';
  end if;
  if v_policy_count=1 then
    select * into strict v_policy
    from public.weekly_source_contract_policies policy
    where policy.contract_id=p_contract_id
      and v_scope_date between policy.effective_from and coalesce(policy.effective_to,'infinity'::date);
    v_configured:=true;
  end if;

  v_effective:=pg_catalog.jsonb_build_object(
    'weekly_rate_classification_method',coalesce(
      v_policy.weekly_rate_classification_method_override,
      v_client_settings->>'weekly_rate_classification_method'
    ),
    'duration_break_tie_rule',case when coalesce(
      v_policy.weekly_rate_classification_method_override,
      v_client_settings->>'weekly_rate_classification_method'
    )='SPLIT_RATE_WINDOWS' then coalesce(
      v_policy.duration_break_tie_rule_override,
      v_client_settings->>'duration_break_tie_rule'
    ) else null end,
    'source_fixed_expenses_enabled',coalesce(
      v_policy.source_fixed_expenses_enabled_override,
      (v_client_settings->>'source_fixed_expenses_enabled')::boolean
    ),
    'source_expense_vat_enabled',coalesce(
      v_policy.source_fixed_expenses_enabled_override,
      (v_client_settings->>'source_fixed_expenses_enabled')::boolean
    ) and coalesce(
      v_policy.source_expense_vat_enabled_override,
      (v_client_settings->>'source_expense_vat_enabled')::boolean
    ),
    'candidate_queries_enabled',coalesce(
      v_policy.candidate_queries_enabled_override,
      (v_client_settings->>'candidate_queries_enabled')::boolean
    ),
    'manager_queries_enabled',coalesce(
      v_policy.manager_queries_enabled_override,
      (v_client_settings->>'manager_queries_enabled')::boolean
    ),
    'manager_query_recipient',case when coalesce(
      v_policy.manager_queries_enabled_override,
      (v_client_settings->>'manager_queries_enabled')::boolean
    ) then coalesce(v_policy.manager_query_recipient_override,v_client_settings->>'manager_query_recipient') end,
    'completed_pack_copy_enabled',coalesce(
      v_policy.completed_pack_copy_enabled_override,
      (v_client_settings->>'completed_pack_copy_enabled')::boolean
    ),
    'completed_pack_recipient',case when coalesce(
      v_policy.completed_pack_copy_enabled_override,
      (v_client_settings->>'completed_pack_copy_enabled')::boolean
    ) then coalesce(v_policy.completed_pack_recipient_override,v_client_settings->>'completed_pack_recipient') end
  );

  v_settings:=pg_catalog.jsonb_build_object(
    'effective_from',coalesce(v_policy.effective_from,v_scope_date),
    'weekly_rate_classification_method_override',v_policy.weekly_rate_classification_method_override,
    'duration_break_tie_rule_override',v_policy.duration_break_tie_rule_override,
    'source_fixed_expenses_enabled_override',v_policy.source_fixed_expenses_enabled_override,
    'source_expense_vat_enabled_override',v_policy.source_expense_vat_enabled_override,
    'candidate_queries_enabled_override',v_policy.candidate_queries_enabled_override,
    'manager_queries_enabled_override',v_policy.manager_queries_enabled_override,
    'manager_query_recipient_override',v_policy.manager_query_recipient_override,
    'completed_pack_copy_enabled_override',v_policy.completed_pack_copy_enabled_override,
    'completed_pack_recipient_override',v_policy.completed_pack_recipient_override,
    'effective',v_effective,
    'client_settings',v_client_settings
  );
  v_version:=pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
    pg_catalog.jsonb_build_object(
      'contract_id',p_contract_id,
      'contract_updated_at',v_contract.updated_at,
      'client_settings_version',v_client_shape->>'settings_version',
      'contract_policy_id',v_policy.id,
      'contract_policy_from',v_policy.effective_from,
      'contract_policy_to',v_policy.effective_to,
      'settings',v_settings
    )::text,'UTF8'),'sha256'),'hex');

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'eligible',(v_client_shape->>'eligible')::boolean
      and v_contract.weekly_timesheet_source::text<>'NONE'
      and v_contract.end_date>=v_scope_date,
    'configured',v_configured,
    'client_configured',(v_client_shape->>'configured')::boolean,
    'client_id',v_contract.client_id,
    'contract_id',p_contract_id,
    'effective_date',v_scope_date,
    'settings_version',v_version,
    'capabilities',v_client_capabilities,
    'settings',v_settings,
    'source_groups',v_client_shape->'source_groups'
  );
end;
$function$;

create or replace function public.weekly_source_contract_settings_get_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_agency uuid;
  v_contract uuid;
  v_date date;
begin
  perform private._weekly_source_settings_assert_request_v1(
    p_request,array['actor_user_id','agency_id','environment','contract_id','effective_date'],
    'WEEKLY_SOURCE_CONTRACT_SETTINGS_GET_REQUEST_INVALID'
  );
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_agency:=(p_request->>'agency_id')::uuid;
    v_contract:=(p_request->>'contract_id')::uuid;
    v_date:=coalesce(nullif(p_request->>'effective_date','')::date,current_date);
  exception when others then
    raise exception 'WEEKLY_SOURCE_CONTRACT_SETTINGS_GET_REQUEST_INVALID' using errcode='22023';
  end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  return private._weekly_source_settings_contract_shape_v1(
    v_agency,p_request->>'environment',v_contract,v_date
  );
end;
$function$;

create or replace function private._weekly_source_settings_stale_open_v1(
  p_source_group_id uuid,
  p_client_id uuid default null
) returns integer
language plpgsql
volatile
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_family text;
  v_changed integer:=0;
  v_part integer:=0;
begin
  select source_group.source_family into strict v_family
  from public.weekly_source_groups source_group
  where source_group.id=p_source_group_id
  for update;

  if exists(
    select 1
    from public.weekly_source_cycles cycle
    where cycle.source_group_id=p_source_group_id
      and cycle.state in ('FINALISING','CORRECTION_IN_PROGRESS')
  ) or exists(
    select 1
    from public.weekly_source_report_scopes report_scope
    where report_scope.source_group_id=p_source_group_id
      and (p_client_id is null or report_scope.client_id=p_client_id)
      and report_scope.state in ('FINALISING','CORRECTION_IN_PROGRESS')
  ) then
    raise exception 'WEEKLY_SOURCE_SETTINGS_SCOPE_BUSY' using errcode='40001';
  end if;

  if v_family='NHSP' and p_client_id is not null then
    perform 1
    from public.weekly_source_report_scopes report_scope
    where report_scope.source_group_id=p_source_group_id
      and report_scope.client_id=p_client_id
      and report_scope.state in ('OPEN','FINALISABLE')
    for update;
    update public.weekly_source_report_scopes report_scope
    set projection_state='NONE',current_projection_publication_id=null,
        version=report_scope.version+1,updated_at_utc=pg_catalog.transaction_timestamp()
    where report_scope.source_group_id=p_source_group_id
      and report_scope.client_id=p_client_id
      and report_scope.state in ('OPEN','FINALISABLE')
      and (report_scope.projection_state<>'NONE' or report_scope.current_projection_publication_id is not null);
    get diagnostics v_changed=row_count;
  else
    perform 1
    from public.weekly_source_cycles cycle
    where cycle.source_group_id=p_source_group_id
      and cycle.state in ('OPEN','FINALISABLE')
    for update;
    update public.weekly_source_cycles cycle
    set projection_state='NONE',current_projection_publication_id=null,
        version=cycle.version+1
    where cycle.source_group_id=p_source_group_id
      and cycle.state in ('OPEN','FINALISABLE')
      and (cycle.projection_state<>'NONE' or cycle.current_projection_publication_id is not null);
    get diagnostics v_changed=row_count;

    update public.weekly_source_report_scopes report_scope
    set projection_state='NONE',current_projection_publication_id=null,
        version=report_scope.version+1,updated_at_utc=pg_catalog.transaction_timestamp()
    where report_scope.source_group_id=p_source_group_id
      and (p_client_id is null or report_scope.client_id=p_client_id)
      and report_scope.state in ('OPEN','FINALISABLE')
      and (report_scope.projection_state<>'NONE' or report_scope.current_projection_publication_id is not null);
    get diagnostics v_part=row_count;
    v_changed:=v_changed+v_part;
  end if;
  return v_changed;
end;
$function$;

create or replace function public.weekly_source_global_settings_save_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_agency uuid;
  v_expected bigint;
  v_settings_json jsonb;
  v_settings public.weekly_source_global_settings%rowtype;
  v_candidate_reminder integer;
  v_candidate_deadline integer;
  v_manager_digest integer;
  v_manager_cooldown integer;
  v_candidate_cooldown integer;
  v_link_days integer;
begin
  perform private._weekly_source_settings_assert_request_v1(
    p_request,array['actor_user_id','agency_id','environment','expected_settings_version','settings'],
    'WEEKLY_SOURCE_GLOBAL_SETTINGS_SAVE_REQUEST_INVALID'
  );
  v_settings_json:=p_request->'settings';
  perform private._weekly_source_settings_assert_request_v1(
    v_settings_json,array[
      'candidate_reminder_minutes','candidate_response_deadline_minutes',
      'manager_digest_minutes','manager_manual_resend_cooldown_minutes',
      'candidate_manual_reminder_cooldown_minutes','manager_secure_link_days'
    ],'WEEKLY_SOURCE_GLOBAL_SETTINGS_VALUES_INVALID'
  );
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_agency:=(p_request->>'agency_id')::uuid;
    v_expected:=(p_request->>'expected_settings_version')::bigint;
    v_candidate_reminder:=(v_settings_json->>'candidate_reminder_minutes')::integer;
    v_candidate_deadline:=(v_settings_json->>'candidate_response_deadline_minutes')::integer;
    v_manager_digest:=(v_settings_json->>'manager_digest_minutes')::integer;
    v_manager_cooldown:=(v_settings_json->>'manager_manual_resend_cooldown_minutes')::integer;
    v_candidate_cooldown:=(v_settings_json->>'candidate_manual_reminder_cooldown_minutes')::integer;
    v_link_days:=(v_settings_json->>'manager_secure_link_days')::integer;
  exception when others then
    raise exception 'WEEKLY_SOURCE_GLOBAL_SETTINGS_SAVE_REQUEST_INVALID' using errcode='22023';
  end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  perform private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
  if v_candidate_reminder<1 or v_candidate_reminder>=720
     or v_candidate_deadline<=v_candidate_reminder or v_candidate_deadline>10080
     or v_manager_digest<1 or v_manager_digest>10080
     or v_manager_cooldown<5 or v_manager_cooldown>1440
     or v_candidate_cooldown<60 or v_candidate_cooldown>10080
     or v_link_days<1 or v_link_days>30 then
    raise exception 'WEEKLY_SOURCE_GLOBAL_SETTINGS_VALUES_INVALID' using errcode='22023';
  end if;
  select * into strict v_settings
  from public.weekly_source_global_settings where singleton for update;
  if v_settings.version<>v_expected then
    raise exception 'WEEKLY_SOURCE_SETTINGS_STALE' using errcode='40001';
  end if;
  update public.weekly_source_global_settings
  set candidate_reminder_after=pg_catalog.make_interval(mins=>v_candidate_reminder),
      candidate_response_deadline_after=pg_catalog.make_interval(mins=>v_candidate_deadline),
      manager_partial_digest_after=pg_catalog.make_interval(mins=>v_manager_digest),
      manager_manual_send_cooldown=pg_catalog.make_interval(mins=>v_manager_cooldown),
      candidate_manual_reminder_cooldown=pg_catalog.make_interval(mins=>v_candidate_cooldown),
      manager_secure_link_lifetime=pg_catalog.make_interval(days=>v_link_days),
      version=version+1,updated_by_user_id=v_actor,
      updated_at_utc=pg_catalog.transaction_timestamp()
  where singleton;
  return public.weekly_source_global_settings_get_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'agency_id',v_agency,'environment',p_request->>'environment'
  ));
end;
$function$;

-- The source-group settings owner is also the creation boundary for the first
-- usable cycle.  Finalisation and the explicit no-shifts route reuse this
-- helper to open the next cycle only after the prior one is genuinely closed.
-- This keeps ordinary reads stable/read-only and makes cycle creation atomic
-- with the state transition that requires it.
create or replace function private._weekly_source_settings_ensure_open_cycle_v1(
  p_source_group_id uuid,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns uuid
language plpgsql
volatile
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_group public.weekly_source_groups%rowtype;
  v_cycle_id uuid;
  v_open_count integer;
  v_latest_week_ending date;
  v_local_now timestamp;
  v_cutoff_date date;
  v_target_week_ending date;
  v_cutoff_at_utc timestamptz;
  v_days_to_cutoff integer;
begin
  if p_source_group_id is null or p_now_utc is null then
    raise exception 'WEEKLY_SOURCE_CYCLE_ENSURE_REQUEST_INVALID' using errcode='22023';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly-source-cycle-lifecycle:'||p_source_group_id::text,0
  ));
  select * into v_group
  from public.weekly_source_groups source_group
  where source_group.id=p_source_group_id
  for update;
  if not found or not v_group.active then
    raise exception 'WEEKLY_SOURCE_CYCLE_GROUP_NOT_ACTIVE' using errcode='22023';
  end if;

  select pg_catalog.count(*),pg_catalog.min(cycle.id::text)::uuid
  into v_open_count,v_cycle_id
  from public.weekly_source_cycles cycle
  where cycle.source_group_id=v_group.id
    and cycle.state<>'FINALISED';
  if v_open_count>1 then
    raise exception 'WEEKLY_SOURCE_MULTIPLE_OPEN_CYCLES' using errcode='55000';
  end if;
  if v_open_count=1 then
    return v_cycle_id;
  end if;

  select pg_catalog.max(cycle.finalisation_week_ending)
  into v_latest_week_ending
  from public.weekly_source_cycles cycle
  where cycle.source_group_id=v_group.id;
  if v_latest_week_ending is not null then
    v_target_week_ending:=v_latest_week_ending+7;
    v_cutoff_date:=v_target_week_ending+v_group.cutoff_weekday;
  else
    v_local_now:=p_now_utc at time zone v_group.timezone;
    v_days_to_cutoff:=((v_group.cutoff_weekday-
      extract(dow from v_local_now)::integer)+7)%7;
    if v_days_to_cutoff=0 and v_local_now::time>=v_group.cutoff_local_time then
      v_days_to_cutoff:=7;
    end if;
    v_cutoff_date:=v_local_now::date+v_days_to_cutoff;
    v_target_week_ending:=v_cutoff_date-
      extract(dow from v_cutoff_date)::integer;
  end if;
  v_cutoff_at_utc:=(v_cutoff_date+v_group.cutoff_local_time)
    at time zone v_group.timezone;

  insert into public.weekly_source_cycles(
    source_group_id,finalisation_week_ending,cutoff_at_utc,
    state,version,projection_state
  ) values (
    v_group.id,v_target_week_ending,v_cutoff_at_utc,
    'OPEN',0,'NONE'
  )
  on conflict (source_group_id,finalisation_week_ending) do nothing
  returning id into v_cycle_id;
  if v_cycle_id is null then
    select cycle.id into strict v_cycle_id
    from public.weekly_source_cycles cycle
    where cycle.source_group_id=v_group.id
      and cycle.finalisation_week_ending=v_target_week_ending
      and cycle.state<>'FINALISED';
  end if;
  return v_cycle_id;
end;
$function$;

create or replace function public.weekly_source_source_group_save_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_agency uuid;
  v_environment text;
  v_input jsonb;
  v_id uuid;
  v_expected bigint;
  v_existing public.weekly_source_groups%rowtype;
  v_code text;
  v_name text;
  v_family text;
  v_weekday integer;
  v_cutoff time;
  v_heading text;
  v_active boolean;
begin
  perform private._weekly_source_settings_assert_request_v1(
    p_request,array['actor_user_id','agency_id','environment','source_group'],
    'WEEKLY_SOURCE_GROUP_SETTINGS_SAVE_REQUEST_INVALID'
  );
  v_input:=p_request->'source_group';
  perform private._weekly_source_settings_assert_request_v1(
    v_input,array[
      'id','expected_version','code','display_name','source_family','cutoff_weekday',
      'cutoff_local_time','nhsp_report_heading_name','active'
    ],'WEEKLY_SOURCE_GROUP_SETTINGS_VALUES_INVALID'
  );
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_agency:=(p_request->>'agency_id')::uuid;
    v_id:=nullif(v_input->>'id','')::uuid;
    v_expected:=nullif(v_input->>'expected_version','')::bigint;
    v_code:=nullif(pg_catalog.upper(pg_catalog.btrim(v_input->>'code')),'');
    v_name:=pg_catalog.btrim(v_input->>'display_name');
    v_family:=pg_catalog.upper(pg_catalog.btrim(v_input->>'source_family'));
    v_weekday:=(v_input->>'cutoff_weekday')::integer;
    v_cutoff:=(v_input->>'cutoff_local_time')::time;
    v_heading:=nullif(pg_catalog.btrim(v_input->>'nhsp_report_heading_name'),'');
    v_active:=(v_input->>'active')::boolean;
  exception when others then
    raise exception 'WEEKLY_SOURCE_GROUP_SETTINGS_SAVE_REQUEST_INVALID' using errcode='22023';
  end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  v_environment:=private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly-source-settings-nhsp-group:'||v_agency::text||':'||v_environment,0
  ));
  if (v_code is not null and v_code !~ '^[A-Z][A-Z0-9_]{1,79}$')
     or pg_catalog.char_length(v_name) not between 1 and 160
     or v_family not in ('NHSP','ROSTER') or v_weekday not between 0 and 6
     or ((v_family='NHSP')<>(v_heading is not null)) then
    raise exception 'WEEKLY_SOURCE_GROUP_SETTINGS_VALUES_INVALID' using errcode='22023';
  end if;
  if v_family='NHSP' and exists(
    select 1
    from public.weekly_source_groups source_group
    where source_group.agency_id=v_agency
      and source_group.environment=v_environment
      and source_group.source_family='NHSP'
      and (v_id is null or source_group.id<>v_id)
  ) then
    raise exception 'WEEKLY_SOURCE_NHSP_GROUP_ALREADY_EXISTS' using errcode='55000';
  end if;

  if v_id is null then
    if v_expected is not null then
      raise exception 'WEEKLY_SOURCE_GROUP_SETTINGS_STALE' using errcode='40001';
    end if;
    v_id:=pg_catalog.gen_random_uuid();
    v_code:=coalesce(v_code,'SOURCE_'||pg_catalog.upper(pg_catalog.substr(
      pg_catalog.replace(v_id::text,'-',''),1,16
    )));
    insert into public.weekly_source_groups(
      id,
      environment,agency_id,code,display_name,source_family,timezone,
      cutoff_weekday,cutoff_local_time,nhsp_report_heading_name,active,
      version,updated_by_user_id
    ) values (
      v_id,
      v_environment,v_agency,v_code,v_name,v_family,'Europe/London',
      v_weekday,v_cutoff,v_heading,v_active,1,v_actor
    );
  else
    select * into v_existing from public.weekly_source_groups source_group
    where source_group.id=v_id for update;
    if not found or v_existing.agency_id<>v_agency or v_existing.environment<>v_environment then
      raise exception 'WEEKLY_SOURCE_GROUP_SETTINGS_NOT_FOUND' using errcode='22023';
    end if;
    if v_expected is null or v_existing.version<>v_expected then
      raise exception 'WEEKLY_SOURCE_GROUP_SETTINGS_STALE' using errcode='40001';
    end if;
    v_code:=coalesce(v_code,v_existing.code);
    if v_existing.source_family<>v_family and exists(
      select 1 from public.weekly_source_group_clients membership
      where membership.source_group_id=v_id
    ) then
      raise exception 'WEEKLY_SOURCE_GROUP_FAMILY_IN_USE' using errcode='55000';
    end if;
    if not v_active and (
      exists(select 1 from public.weekly_source_group_clients membership
             where membership.source_group_id=v_id and coalesce(membership.valid_to,'infinity'::date)>=current_date)
      or exists(select 1 from public.weekly_source_cycles cycle
                where cycle.source_group_id=v_id and cycle.state not in ('FINALISED'))
    ) then
      raise exception 'WEEKLY_SOURCE_GROUP_ACTIVE_SCOPE_EXISTS' using errcode='55000';
    end if;
    perform private._weekly_source_settings_stale_open_v1(v_id,null);
    update public.weekly_source_groups
    set code=v_code,display_name=v_name,source_family=v_family,
        cutoff_weekday=v_weekday,cutoff_local_time=v_cutoff,
        nhsp_report_heading_name=v_heading,active=v_active,
        version=version+1,updated_at_utc=pg_catalog.transaction_timestamp(),
        updated_by_user_id=v_actor
    where id=v_id;
  end if;

  if v_active then
    perform private._weekly_source_settings_ensure_open_cycle_v1(
      v_id,pg_catalog.transaction_timestamp()
    );
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'saved_source_group_id',v_id,
    'source_groups',private._weekly_source_settings_group_list_v1(
      v_agency,v_environment,null,false
    )
  );
exception when unique_violation then
  raise exception 'WEEKLY_SOURCE_GROUP_CODE_ALREADY_EXISTS' using errcode='23505';
end;
$function$;

create or replace function public.weekly_source_client_settings_save_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
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
  if v_contract_count=0 or v_family_count<>1 or v_mode_count<>1 or v_self_bill_count<>1
     or (v_authority='SOURCE_AUTHORITY' and not v_self_bill)
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

create or replace function public.weekly_source_contract_settings_save_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_agency uuid;
  v_environment text;
  v_contract_id uuid;
  v_contract public.contracts%rowtype;
  v_expected text;
  v_input jsonb;
  v_effective_from date;
  v_current_shape jsonb;
  v_capabilities jsonb;
  v_client_settings jsonb;
  v_source_group_id uuid;
  v_policy public.weekly_source_contract_policies%rowtype;
  v_policy_found boolean:=false;
  v_rate_override text;
  v_tie_override text;
  v_source_expenses_override boolean;
  v_source_expense_vat_override boolean;
  v_candidate_override boolean;
  v_manager_override boolean;
  v_manager_recipient_override text;
  v_completed_override boolean;
  v_completed_recipient_override text;
  v_effective_rate text;
  v_effective_source_expenses boolean;
  v_effective_manager boolean;
  v_effective_completed boolean;
  v_default_recipient text;
  v_prior_end date;
begin
  perform private._weekly_source_settings_assert_request_v1(
    p_request,array[
      'actor_user_id','agency_id','environment','contract_id',
      'expected_settings_version','settings'
    ],'WEEKLY_SOURCE_CONTRACT_SETTINGS_SAVE_REQUEST_INVALID'
  );
  v_input:=p_request->'settings';
  perform private._weekly_source_settings_assert_request_v1(
    v_input,array[
      'effective_from','weekly_rate_classification_method_override',
      'duration_break_tie_rule_override','source_fixed_expenses_enabled_override',
      'source_expense_vat_enabled_override','candidate_queries_enabled_override',
      'manager_queries_enabled_override','manager_query_recipient_override',
      'completed_pack_copy_enabled_override','completed_pack_recipient_override'
    ],'WEEKLY_SOURCE_CONTRACT_SETTINGS_VALUES_INVALID'
  );
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_agency:=(p_request->>'agency_id')::uuid;
    v_contract_id:=(p_request->>'contract_id')::uuid;
    v_expected:=pg_catalog.btrim(p_request->>'expected_settings_version');
    v_effective_from:=coalesce(nullif(v_input->>'effective_from','')::date,current_date);
    v_rate_override:=nullif(pg_catalog.upper(pg_catalog.btrim(v_input->>'weekly_rate_classification_method_override')),'');
    v_tie_override:=nullif(pg_catalog.upper(pg_catalog.btrim(v_input->>'duration_break_tie_rule_override')),'');
    v_source_expenses_override:=case when v_input->'source_fixed_expenses_enabled_override'='null'::jsonb then null
      else (v_input->>'source_fixed_expenses_enabled_override')::boolean end;
    v_source_expense_vat_override:=case when v_input->'source_expense_vat_enabled_override'='null'::jsonb then null
      else (v_input->>'source_expense_vat_enabled_override')::boolean end;
    v_candidate_override:=case when v_input->'candidate_queries_enabled_override'='null'::jsonb then null
      else (v_input->>'candidate_queries_enabled_override')::boolean end;
    v_manager_override:=case when v_input->'manager_queries_enabled_override'='null'::jsonb then null
      else (v_input->>'manager_queries_enabled_override')::boolean end;
    v_manager_recipient_override:=nullif(pg_catalog.btrim(v_input->>'manager_query_recipient_override'),'');
    v_completed_override:=case when v_input->'completed_pack_copy_enabled_override'='null'::jsonb then null
      else (v_input->>'completed_pack_copy_enabled_override')::boolean end;
    v_completed_recipient_override:=nullif(pg_catalog.btrim(v_input->>'completed_pack_recipient_override'),'');
  exception when others then
    raise exception 'WEEKLY_SOURCE_CONTRACT_SETTINGS_SAVE_REQUEST_INVALID' using errcode='22023';
  end;
  perform private._weekly_source_settings_require_admin_v1(v_actor);
  v_environment:=private._weekly_source_settings_deployment_v1(v_agency,p_request->>'environment');
  if v_effective_from<current_date or nullif(v_expected,'') is null then
    raise exception 'WEEKLY_SOURCE_CONTRACT_SETTINGS_SAVE_REQUEST_INVALID' using errcode='22023';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly-source-settings-contract:'||v_contract_id::text,0
  ));
  select * into v_contract from public.contracts contract where contract.id=v_contract_id for update;
  if not found or v_contract.weekly_timesheet_source::text='NONE' then
    raise exception 'WEEKLY_SOURCE_CONTRACT_SETTINGS_NOT_APPLICABLE' using errcode='22023';
  end if;
  v_current_shape:=private._weekly_source_settings_contract_shape_v1(
    v_agency,v_environment,v_contract_id,v_effective_from
  );
  if v_current_shape->>'settings_version'<>v_expected then
    raise exception 'WEEKLY_SOURCE_SETTINGS_STALE' using errcode='40001';
  end if;
  if not (v_current_shape->>'eligible')::boolean
     or not (v_current_shape->>'client_configured')::boolean then
    raise exception 'WEEKLY_SOURCE_CONTRACT_SETTINGS_NOT_APPLICABLE' using errcode='22023';
  end if;
  v_capabilities:=v_current_shape->'capabilities';
  v_client_settings:=v_current_shape->'settings'->'client_settings';
  v_source_group_id:=(v_client_settings->>'source_group_id')::uuid;
  select nullif(pg_catalog.btrim(client.ts_queries_email),'')
  into v_default_recipient
  from public.clients client
  where client.id=v_contract.client_id;

  if v_rate_override is not null and v_rate_override not in ('SPLIT_RATE_WINDOWS','WHOLE_SHIFT_START_DAY') then
    raise exception 'WEEKLY_SOURCE_CONTRACT_RATE_SETTINGS_INVALID' using errcode='22023';
  end if;
  v_effective_rate:=coalesce(v_rate_override,v_client_settings->>'weekly_rate_classification_method');
  if v_effective_rate='SPLIT_RATE_WINDOWS' then
    if v_tie_override is not null and v_tie_override not in ('EARLIEST_LONGEST_PORTION','LATEST_LONGEST_PORTION') then
      raise exception 'WEEKLY_SOURCE_CONTRACT_RATE_SETTINGS_INVALID' using errcode='22023';
    end if;
  else
    v_tie_override:=null;
  end if;

  if not coalesce((v_capabilities->>'show_source_expenses')::boolean,false) then
    if v_source_expenses_override is not null or v_source_expense_vat_override is not null then
      raise exception 'WEEKLY_SOURCE_CONTRACT_SOURCE_EXPENSES_NOT_APPLICABLE' using errcode='22023';
    end if;
  else
    v_effective_source_expenses:=coalesce(
      v_source_expenses_override,
      (v_client_settings->>'source_fixed_expenses_enabled')::boolean
    );
    if not v_effective_source_expenses then v_source_expense_vat_override:=null; end if;
  end if;

  if not coalesce((v_capabilities->>'show_query_settings')::boolean,false) then
    if v_candidate_override is not null or v_manager_override is not null
       or v_manager_recipient_override is not null
       or v_completed_override is not null or v_completed_recipient_override is not null then
      raise exception 'WEEKLY_SOURCE_CONTRACT_QUERY_SETTINGS_NOT_APPLICABLE' using errcode='22023';
    end if;
  else
    v_effective_manager:=coalesce(
      v_manager_override,(v_client_settings->>'manager_queries_enabled')::boolean
    );
    if not v_effective_manager then v_manager_recipient_override:=null; end if;
    if v_effective_manager
       and coalesce(v_manager_recipient_override,v_client_settings->>'manager_query_recipient') is null then
      v_manager_recipient_override:=v_default_recipient;
    end if;
    if v_effective_manager
       and coalesce(v_manager_recipient_override,v_client_settings->>'manager_query_recipient') is null then
      raise exception 'WEEKLY_SOURCE_MANAGER_QUERY_RECIPIENT_REQUIRED' using errcode='22023';
    end if;
    v_effective_completed:=coalesce(
      v_completed_override,(v_client_settings->>'completed_pack_copy_enabled')::boolean
    );
    if not v_effective_completed then v_completed_recipient_override:=null; end if;
    if v_effective_completed
       and coalesce(v_completed_recipient_override,v_client_settings->>'completed_pack_recipient') is null then
      v_completed_recipient_override:=coalesce(
        v_manager_recipient_override,
        v_client_settings->>'manager_query_recipient',
        v_default_recipient
      );
    end if;
    if v_effective_completed
       and coalesce(v_completed_recipient_override,v_client_settings->>'completed_pack_recipient') is null then
      raise exception 'WEEKLY_SOURCE_COMPLETED_PACK_RECIPIENT_REQUIRED' using errcode='22023';
    end if;
  end if;

  select * into v_policy
  from public.weekly_source_contract_policies policy
  where policy.contract_id=v_contract_id
    and v_effective_from between policy.effective_from and coalesce(policy.effective_to,'infinity'::date)
  for update;
  v_policy_found:=found;
  if v_policy_found and v_policy.effective_from=v_effective_from then
    update public.weekly_source_contract_policies
    set weekly_rate_classification_method_override=v_rate_override,
        duration_break_tie_rule_override=v_tie_override,
        source_fixed_expenses_enabled_override=v_source_expenses_override,
        source_expense_vat_enabled_override=v_source_expense_vat_override,
        candidate_queries_enabled_override=v_candidate_override,
        manager_queries_enabled_override=v_manager_override,
        manager_query_recipient_override=v_manager_recipient_override,
        completed_pack_copy_enabled_override=v_completed_override,
        completed_pack_recipient_override=v_completed_recipient_override,
        created_by_user_id=v_actor,created_at_utc=pg_catalog.transaction_timestamp()
    where id=v_policy.id;
  else
    if v_policy_found then
      v_prior_end:=v_policy.effective_to;
      update public.weekly_source_contract_policies set effective_to=v_effective_from-1
      where id=v_policy.id;
    end if;
    insert into public.weekly_source_contract_policies(
      contract_id,effective_from,effective_to,
      weekly_rate_classification_method_override,duration_break_tie_rule_override,
      source_fixed_expenses_enabled_override,source_expense_vat_enabled_override,
      candidate_queries_enabled_override,manager_queries_enabled_override,
      manager_query_recipient_override,completed_pack_copy_enabled_override,
      completed_pack_recipient_override,created_by_user_id
    ) values (
      v_contract_id,v_effective_from,v_prior_end,
      v_rate_override,v_tie_override,v_source_expenses_override,v_source_expense_vat_override,
      v_candidate_override,v_manager_override,v_manager_recipient_override,
      v_completed_override,v_completed_recipient_override,v_actor
    );
  end if;
  perform private._weekly_source_settings_stale_open_v1(v_source_group_id,v_contract.client_id);
  return private._weekly_source_settings_contract_shape_v1(
    v_agency,v_environment,v_contract_id,v_effective_from
  );
exception when exclusion_violation then
  raise exception 'WEEKLY_SOURCE_CONTRACT_SETTINGS_EFFECTIVE_RANGE_CONFLICT' using errcode='23P01';
end;
$function$;

do $weekly_source_settings_admin_acl$
declare
  v_signature text;
begin
  foreach v_signature in array array[
    'public.weekly_source_client_settings_get_v1(jsonb)',
    'public.weekly_source_client_settings_save_atomic_v1(jsonb)',
    'public.weekly_source_contract_settings_get_v1(jsonb)',
    'public.weekly_source_contract_settings_save_atomic_v1(jsonb)',
    'public.weekly_source_global_settings_get_v1(jsonb)',
    'public.weekly_source_global_settings_save_atomic_v1(jsonb)',
    'public.weekly_source_source_groups_get_v1(jsonb)',
    'public.weekly_source_source_group_save_atomic_v1(jsonb)'
  ]
  loop
    -- Miget restores the repository's audited logical postgres owner as the
    -- provider service owner, so dynamic owner statements must use the
    -- current release role rather than attempting SET ROLE postgres.
    execute 'alter function '||v_signature||' owner to current_user';
    execute 'revoke all on function '||v_signature||' from public,anon,authenticated';
    execute 'grant execute on function '||v_signature||' to service_role';
  end loop;
end;
$weekly_source_settings_admin_acl$;

revoke all on function private._weekly_source_settings_assert_request_v1(jsonb,text[],text)
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_settings_require_admin_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_settings_deployment_v1(uuid,text)
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_settings_group_list_v1(uuid,text,text,boolean)
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_settings_client_shape_v1(uuid,text,uuid,date)
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_settings_contract_shape_v1(uuid,text,uuid,date)
  from public,anon,authenticated,service_role;
revoke all on function private._weekly_source_settings_stale_open_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
alter function private._weekly_source_settings_ensure_open_cycle_v1(uuid,timestamptz)
  owner to current_user;
revoke all on function private._weekly_source_settings_ensure_open_cycle_v1(uuid,timestamptz)
  from public,anon,authenticated,service_role;

comment on function public.weekly_source_client_settings_save_atomic_v1(jsonb) is
  'CAS/effective-dated service-only Client settings owner. Inapplicable controls are refused and open projections are made stale.';
comment on function public.weekly_source_contract_settings_save_atomic_v1(jsonb) is
  'CAS/effective-dated service-only independent Contract override owner; null means inherit.';
comment on function public.weekly_source_global_settings_save_atomic_v1(jsonb) is
  'CAS service-only owner for Weekly source query timing and secure-manager-link lifetime.';
comment on function public.weekly_source_source_group_save_atomic_v1(jsonb) is
  'CAS service-only source-group owner scoped by server-supplied Agency and environment; active groups atomically own one current open cycle.';

notify pgrst, 'reload schema';

commit;
