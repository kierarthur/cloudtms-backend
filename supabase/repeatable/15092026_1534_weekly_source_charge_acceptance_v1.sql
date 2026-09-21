-- Repeatable CloudTMS authority: weekly_source_charge_acceptance_v1
-- Records the narrow Office decision which admits a current, otherwise valid
-- NHSP source charge warning. It changes no source amount, Candidate pay,
-- Timesheet, TSFIN, Workbench or Banking Pay fact.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_charge_acceptance_policy_fingerprint_v1()
returns bytea
language sql immutable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'NHSP_SOURCE_CHARGE_ACCEPTANCE_POLICY_V1',
    pg_catalog.jsonb_build_object(
      'accepted_results',pg_catalog.jsonb_build_array('MISMATCH','ZERO_SOURCE_CHARGE'),
      'admission_kinds',pg_catalog.jsonb_build_array('ACCEPTED_DISPARITY','ACCEPTED_ZERO'),
      'invoice_authority','EXACT_SOURCE_PENCE','candidate_pay_authority','CLOUDTMS_CALCULATION'
    )
  );
$function$;

create or replace function public.weekly_source_charge_accept_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_check_id uuid;
  v_check public.weekly_source_charge_checks%rowtype;
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;
  v_economic public.weekly_source_row_economic_snapshots%rowtype;
  v_kind text;
  v_policy_fingerprint bytea;
  v_hash bytea;
  v_acceptance_id uuid;
  v_ids uuid[];
  v_selected_keys text[];
  v_eligible_keys text[];
  v_selection_proof text;
  v_expected_proof text;
  v_workspace_version text;
  v_selected_count integer;
  v_mapped_key_count integer;
  v_count integer:=0;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) supplied(key)
       where supplied.key not in (
         'actor_user_id','source_cycle_id','projection_publication_id',
         'warning_keys','selection_proof'
       )
     ) then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_selection_proof:=pg_catalog.lower(pg_catalog.btrim(p_request->>'selection_proof'));
    select coalesce(pg_catalog.array_agg(value order by value),'{}'::text[]),pg_catalog.count(*)::integer
      into v_selected_keys,v_selected_count
    from (
      select distinct pg_catalog.jsonb_array_elements_text(p_request->'warning_keys') value
    ) selected;
  exception when others then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_cycle_id is null or v_publication_id is null
     or v_selection_proof !~ '^[0-9a-f]{64}$'
     or coalesce(pg_catalog.array_length(v_selected_keys,1),0)=0
     or pg_catalog.array_length(v_selected_keys,1)>1000
     or v_selected_count<>pg_catalog.jsonb_array_length(p_request->'warning_keys') then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_REQUEST_INVALID' using errcode='22023';
  end if;
  if not exists(
    select 1 from public.tms_users office_user
    where office_user.id=v_actor and office_user.is_active
      and pg_catalog.lower(pg_catalog.btrim(coalesce(office_user.role,'')))='admin'
  ) then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_ADMIN_REQUIRED' using errcode='42501';
  end if;

  select * into v_publication
  from public.weekly_source_projection_publications publication
  where publication.id=v_publication_id and publication.source_cycle_id=v_cycle_id
  for update;
  if not found or v_publication.state<>'CURRENT'
     or v_publication.authority_scope_kind<>'NHSP_REPORT_SCOPE' then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_PUBLICATION_STALE' using errcode='40001';
  end if;
  select * into strict v_upload from public.weekly_source_uploads where id=v_publication.upload_id;
  select * into strict v_profile from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
  if v_upload.state<>'CURRENT' or v_profile.profile_code<>'NHSP_FINAL_BACKING_V1'
     or v_publication.projection_generation is null then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_PUBLICATION_INVALID' using errcode='55000';
  end if;
  v_policy_fingerprint:=private.weekly_source_charge_acceptance_policy_fingerprint_v1();

  -- The browser selects only the warning keys projected by the current
  -- workspace.  It never supplies charge-check ids or any financial value.
  -- The proof covers the complete current eligible set, so a stale workspace,
  -- Recheck, corrected upload or earlier partial acceptance fails closed.
  select coalesce(pg_catalog.array_agg(eligible.warning_key order by eligible.warning_key),'{}'::text[])
    into v_eligible_keys
  from (
    select 'all-zero-source-charge'::text warning_key
    where exists(
      select 1
      from public.weekly_source_charge_checks charge
      join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
      left join public.weekly_source_charge_acceptances acceptance
        on acceptance.charge_check_id=charge.id
      where source_row.upload_id=v_upload.id
        and charge.generation=v_publication.projection_generation
        and charge.comparison_result='ZERO_SOURCE_CHARGE'
        and charge.phase_severity='PROVISIONAL_WARNING'
        and charge.blocker_code is null
        and acceptance.id is null
    )
    union all
    select 'charge-check:'||charge.id::text
    from public.weekly_source_charge_checks charge
    join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
    left join public.weekly_source_charge_acceptances acceptance
      on acceptance.charge_check_id=charge.id
    where source_row.upload_id=v_upload.id
      and charge.generation=v_publication.projection_generation
      and charge.comparison_result='MISMATCH'
      and charge.phase_severity='PROVISIONAL_WARNING'
      and charge.blocker_code is null
      and acceptance.id is null
  ) eligible;
  v_workspace_version:=private.weekly_source_office_workspace_version_v1(v_cycle_id,v_publication.id);
  v_expected_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'NHSP_RATE_WARNING_SELECTION_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle_id,
      'projection_publication_id',v_publication.id,
      'projection_generation',v_publication.projection_generation,
      'workspace_version',v_workspace_version,
      'eligible_warning_keys',to_jsonb(v_eligible_keys)
    )
  ),'hex');
  if v_selection_proof<>v_expected_proof
     or not v_selected_keys<@v_eligible_keys then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_SELECTION_STALE' using errcode='40001';
  end if;

  select coalesce(pg_catalog.array_agg(charge.id order by charge.id),'{}'::uuid[]),
    pg_catalog.count(distinct case
      when charge.comparison_result='ZERO_SOURCE_CHARGE' then 'all-zero-source-charge'
      else 'charge-check:'||charge.id::text end)::integer
    into v_ids,v_mapped_key_count
  from public.weekly_source_charge_checks charge
  join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
  left join public.weekly_source_charge_acceptances acceptance
    on acceptance.charge_check_id=charge.id
  where source_row.upload_id=v_upload.id
    and charge.generation=v_publication.projection_generation
    and charge.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE')
    and charge.phase_severity='PROVISIONAL_WARNING'
    and charge.blocker_code is null
    and acceptance.id is null
    and (case when charge.comparison_result='ZERO_SOURCE_CHARGE'
      then 'all-zero-source-charge'
      else 'charge-check:'||charge.id::text end)=any(v_selected_keys);
  if coalesce(pg_catalog.array_length(v_ids,1),0)=0
     or v_mapped_key_count<>pg_catalog.array_length(v_selected_keys,1) then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_SELECTION_STALE' using errcode='40001';
  end if;

  foreach v_check_id in array v_ids loop
    select * into v_check from public.weekly_source_charge_checks charge
    where charge.id=v_check_id for update;
    if not found or v_check.generation<>v_publication.projection_generation
       or v_check.comparison_result not in ('MISMATCH','ZERO_SOURCE_CHARGE')
       or v_check.phase_severity<>'PROVISIONAL_WARNING'
       or v_check.blocker_code is not null then
      raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_SELECTION_STALE' using errcode='40001';
    end if;
    select * into strict v_row from public.weekly_source_upload_rows source_row
      where source_row.id=v_check.upload_row_id and source_row.upload_id=v_upload.id;
    select * into strict v_resolution from public.weekly_source_row_resolutions resolution
      where resolution.id=v_check.row_resolution_id
        and resolution.upload_row_id=v_row.id
        and resolution.generation=v_publication.projection_generation;
    if v_resolution.mapping_state<>'RESOLVED' or v_resolution.contract_id is null
       or v_resolution.contract_selection_method is null then
      raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_CONTRACT_UNRESOLVED' using errcode='55000';
    end if;
    select * into strict v_economic from public.weekly_source_row_economic_snapshots economic
      where economic.row_resolution_id=v_resolution.id
        and economic.upload_row_id=v_row.id
        and economic.generation=v_publication.projection_generation
        and economic.contract_id=v_resolution.contract_id;
    if v_check.charge_calculation_fingerprint is distinct from v_economic.calculation_fingerprint
       or v_resolution.contract_and_rate_fingerprint is distinct from v_economic.contract_and_rate_fingerprint
       or v_resolution.effective_policy_fingerprint is distinct from v_economic.effective_policy_fingerprint
       or v_check.source_shift_charge_pence is distinct from v_row.source_shift_charge_pence
       or v_check.calculated_segment_charge_pence is distinct from v_economic.calculated_charge_pence then
      raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_BINDING_STALE' using errcode='40001';
    end if;
    v_kind:=case v_check.comparison_result
      when 'ZERO_SOURCE_CHARGE' then 'ACCEPTED_ZERO' else 'ACCEPTED_DISPARITY' end;
    v_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_CHARGE_ACCEPTANCE_V1',
      pg_catalog.jsonb_build_object(
        'upload_row_id',v_row.id,'row_resolution_id',v_resolution.id,
        'charge_check_id',v_check.id,'source_upload_id',v_upload.id,
        'contract_id',v_resolution.contract_id,'acceptance_kind',v_kind,
        'source_upload_hash',pg_catalog.encode(v_upload.content_sha256,'hex'),
        'source_row_fingerprint',pg_catalog.encode(v_resolution.source_row_fingerprint,'hex'),
        'contract_and_rate_fingerprint',pg_catalog.encode(v_economic.contract_and_rate_fingerprint,'hex'),
        'effective_policy_fingerprint',pg_catalog.encode(v_economic.effective_policy_fingerprint,'hex'),
        'charge_calculation_fingerprint',pg_catalog.encode(v_economic.calculation_fingerprint,'hex'),
        'acceptance_policy_fingerprint',pg_catalog.encode(v_policy_fingerprint,'hex')
      )
    );
    insert into public.weekly_source_charge_acceptances(
      upload_row_id,row_resolution_id,charge_check_id,source_upload_id,contract_id,
      acceptance_kind,source_upload_hash,source_row_fingerprint,
      contract_and_rate_fingerprint,effective_policy_fingerprint,
      charge_calculation_fingerprint,acceptance_policy_fingerprint,
      accepted_by_user_id,acceptance_hash
    ) values (
      v_row.id,v_resolution.id,v_check.id,v_upload.id,v_resolution.contract_id,
      v_kind,v_upload.content_sha256,v_resolution.source_row_fingerprint,
      v_economic.contract_and_rate_fingerprint,v_economic.effective_policy_fingerprint,
      v_economic.calculation_fingerprint,v_policy_fingerprint,v_actor,v_hash
    ) on conflict (charge_check_id) do nothing
    returning id into v_acceptance_id;
    if v_acceptance_id is null then
      select acceptance.id into strict v_acceptance_id
      from public.weekly_source_charge_acceptances acceptance
      where acceptance.charge_check_id=v_check.id and acceptance.acceptance_hash=v_hash;
    end if;
    v_count:=v_count+1;
    v_acceptance_id:=null;
  end loop;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status','ACCEPTED','projection_publication_id',v_publication.id,
    'accepted_count',v_count,'workspace_refresh_required',true
  );
exception
  when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_REQUEST_INVALID' using errcode='22023';
end;
$function$;

alter function private.weekly_source_charge_acceptance_policy_fingerprint_v1() owner to postgres;
alter function public.weekly_source_charge_accept_atomic_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_charge_acceptance_policy_fingerprint_v1()
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_charge_accept_atomic_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_charge_accept_atomic_v1(jsonb) to service_role;

comment on function public.weekly_source_charge_accept_atomic_v1(jsonb) is
  'Records an immutable, fingerprint-bound Office acceptance for selected current NHSP source-charge warnings. It does not alter source pence, Candidate pay, Timesheets, TSFIN, Workbench or Banking Pay.';

commit;
