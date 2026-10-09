-- Dedicated NHSP Client setup before its first source contract.
-- Full existing definitions: no contract creation, reconciliation shortcut or
-- financial authority change. Preserve effective dates, CAS and service ACLs.
\set ON_ERROR_STOP on
begin;
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

  v_derived_document:=case v_derived_authority
    when 'SOURCE_AUTHORITY' then 'CHECK_ONLY'
    when 'TIMESHEET_AUTHORITY' then 'INVOICE_EVIDENCE_REQUIRED'
  end;
  v_eligible:=(v_contract_count>0 or v_client_nhsp_basis) and v_family_count=1 and v_mode_count=1
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

  if (v_contract_count=0 and not v_client_nhsp_basis) or v_family_count<>1 or v_mode_count<>1 or v_self_bill_count<>1
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

alter function private._weekly_source_settings_client_shape_v1(uuid,text,uuid,date) owner to current_user;
revoke all on function private._weekly_source_settings_client_shape_v1(uuid,text,uuid,date) from public,anon,authenticated,service_role;
alter function public.weekly_source_client_settings_save_atomic_v1(jsonb) owner to current_user;
revoke all on function public.weekly_source_client_settings_save_atomic_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_client_settings_save_atomic_v1(jsonb) to service_role;
notify pgrst, 'reload schema';
commit;
