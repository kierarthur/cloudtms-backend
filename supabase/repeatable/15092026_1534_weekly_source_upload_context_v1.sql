-- Repeatable CloudTMS authority: weekly_source_upload_context_v1
-- Service-only discovery for the Plan 6 upload/publication producer.  It
-- derives scope, exact saved candidate mappings, eligible Contract facts and
-- canonical settings from database authority.  It never accepts money, rates,
-- policy or candidate identities from a browser and creates no business row.

\set ON_ERROR_STOP on

begin;

-- G6-5 / Plan 6.2 `25 §7` and `24 §8`: the eligible-Contract set must carry a
-- compatible-schedule fact, and a separately verified band/role mapping must
-- be available as a tie-breaker for the several-remaining case.  Neither is a
-- band/role text filter: `25 §7` Removed forbids rejecting an otherwise unique
-- safe Contract because band/role text differs, and `24 §8` requires a label
-- filter that leaves nothing to be discarded.  Both facts are returned as
-- per-Contract evidence; the cardinality owner decides.
create or replace function private.weekly_source_schedule_compatible_v1(
  p_std_schedule jsonb,
  p_work_date date
) returns boolean
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_key text;
  v_day jsonb;
begin
  -- A Contract that declares no standard weekly schedule contradicts nothing
  -- the source contains, so it stays compatible.  Only a declared template
  -- that has no entry for the worked weekday is an incompatibility.
  if p_work_date is null then
    return true;
  end if;
  if coalesce(jsonb_typeof(p_std_schedule),'null')<>'object' then
    return true;
  end if;
  if p_std_schedule='{}'::jsonb then
    return true;
  end if;
  v_key:=case extract(dow from p_work_date)::integer
    when 0 then 'sun' when 1 then 'mon' when 2 then 'tue' when 3 then 'wed'
    when 4 then 'thu' when 5 then 'fri' else 'sat' end;
  v_day:=p_std_schedule->v_key;
  return v_day is not null and jsonb_typeof(v_day)='object';
end;
$function$;

-- The established verified band/role authority is public.assignment_band_mappings
-- (`system_type` NHSP or HR_WEEKLY), resolved at the highest available
-- specificity: candidate+client, then candidate, then client, then global.
-- At that specificity an explicit target Contract wins; otherwise a band
-- pattern must appear in the Contract's band.  This reproduces the installed
-- weekly import resolver (repeatable 12122025_weekly_import_phase2.sql) rather
-- than inventing a second, divergent rule.  It never fuzzy-matches Contract
-- names, roles, pay types or pay rates (`24 §8`).
create or replace function private.weekly_source_verified_role_band_match_v1(
  p_system_type text,
  p_incoming_code text,
  p_candidate_id uuid,
  p_client_id uuid,
  p_contract_id uuid,
  p_contract_band text,
  p_work_date date
) returns boolean
language plpgsql stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_code text:=lower(btrim(coalesce(p_incoming_code,'')));
  v_system text:=upper(btrim(coalesce(p_system_type,'')));
  v_spec integer;
  v_target uuid;
  v_patterns text[];
begin
  if v_code='' or v_system not in ('NHSP','HR_WEEKLY')
     or p_candidate_id is null or p_client_id is null or p_contract_id is null then
    return false;
  end if;
  with maps as (
    select mapping.band_match_pattern,mapping.target_contract_id,
      case
        when mapping.candidate_id is not null and mapping.client_id is not null then 3
        when mapping.candidate_id is not null and mapping.client_id is null then 2
        when mapping.candidate_id is null and mapping.client_id is not null then 1
        else 0 end as spec
    from public.assignment_band_mappings mapping
    where mapping.active
      and upper(btrim(mapping.system_type))=v_system
      and lower(btrim(mapping.incoming_code))=v_code
      and (mapping.candidate_id is null or mapping.candidate_id=p_candidate_id)
      and (mapping.client_id is null or mapping.client_id=p_client_id)
  )
  select max(maps.spec) into v_spec from maps;
  if v_spec is null then
    return false;
  end if;
  with maps as (
    select mapping.band_match_pattern,mapping.target_contract_id,
      case
        when mapping.candidate_id is not null and mapping.client_id is not null then 3
        when mapping.candidate_id is not null and mapping.client_id is null then 2
        when mapping.candidate_id is null and mapping.client_id is not null then 1
        else 0 end as spec
    from public.assignment_band_mappings mapping
    where mapping.active
      and upper(btrim(mapping.system_type))=v_system
      and lower(btrim(mapping.incoming_code))=v_code
      and (mapping.candidate_id is null or mapping.candidate_id=p_candidate_id)
      and (mapping.client_id is null or mapping.client_id=p_client_id)
  )
  select
    (select maps.target_contract_id from maps
      where maps.spec=v_spec and maps.target_contract_id is not null
      order by maps.target_contract_id::text limit 1),
    (select array_agg(lower(btrim(maps.band_match_pattern))) from maps where maps.spec=v_spec)
  into v_target,v_patterns;
  if v_target is not null then
    return v_target=p_contract_id;
  end if;
  if v_patterns is null then
    return false;
  end if;
  return exists(
    select 1 from unnest(v_patterns) pattern
    where pattern<>'' and position(pattern in lower(coalesce(p_contract_band,'')))>0
  );
end;
$function$;

create or replace function public.weekly_source_upload_context_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_allowed constant text[]:=array[
    'operation','actor_user_id','source_group_id','source_cycle_id',
    'report_scope_id','client_id','upload_id'
  ]::text[];
  v_operation text;
  v_actor uuid;
  v_group_id uuid;
  v_cycle_id uuid;
  v_report_scope_id uuid;
  v_client_id uuid;
  v_row_client_id uuid;
  v_upload_id uuid;
  v_group public.weekly_source_groups%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_source_row public.weekly_source_upload_rows%rowtype;
  v_source_name text;
  v_source_id text;
  v_source_id_2 text;
  v_source_id_3 text;
  v_source_client text;
  v_norm_name text;
  v_norm_compact text;
  v_candidate_ids uuid[];
  v_candidate_id uuid;
  v_candidate_matches jsonb;
  v_contracts jsonb;
  v_prior_contract_id uuid;
  v_prior_work_event_id uuid;
  -- WP-37: the `24 §9` step-2 compatible-schedule set, counted explicitly.
  v_prior_candidate_count integer;
  v_rows jsonb:='[]'::jsonb;
  v_client_name text;
  v_client_choices jsonb:='[]'::jsonb;
  v_client_choice_count integer:=0;
  v_unknown text;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_CONTEXT_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown
  from pg_catalog.jsonb_object_keys(p_request) key
  where not key=any(v_allowed)
  order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_CONTEXT_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  begin
    v_operation:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'operation','')));
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_group_id:=nullif(p_request->>'source_group_id','')::uuid;
    v_cycle_id:=nullif(p_request->>'source_cycle_id','')::uuid;
    v_report_scope_id:=nullif(p_request->>'report_scope_id','')::uuid;
    v_client_id:=nullif(p_request->>'client_id','')::uuid;
    v_upload_id:=nullif(p_request->>'upload_id','')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_CONTEXT_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_operation not in ('DISCOVER_SCOPE','BUILD_PROJECTION') then
    raise exception 'WEEKLY_SOURCE_CONTEXT_REQUEST_INVALID' using errcode='22023';
  end if;

  if v_operation='BUILD_PROJECTION' then
    if v_upload_id is null then
      raise exception 'WEEKLY_SOURCE_CONTEXT_UPLOAD_REQUIRED' using errcode='22023';
    end if;
    select * into v_upload from public.weekly_source_uploads where id=v_upload_id;
    if not found or v_upload.state not in ('CURRENT','CORRECTION_READY')
       or v_upload.row_manifest_hash is null then
      raise exception 'WEEKLY_SOURCE_CONTEXT_UPLOAD_NOT_SEALED' using errcode='55000';
    end if;
    v_cycle_id:=v_upload.source_cycle_id;
    v_report_scope_id:=v_upload.report_scope_id;
  elsif v_cycle_id is null then
    raise exception 'WEEKLY_SOURCE_CONTEXT_SCOPE_REQUIRED' using errcode='22023';
  end if;

  select * into v_cycle from public.weekly_source_cycles where id=v_cycle_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CONTEXT_CYCLE_NOT_FOUND' using errcode='22023';
  end if;
  select * into v_group from public.weekly_source_groups where id=v_cycle.source_group_id;
  if not found or not v_group.active
     or (v_group_id is not null and v_group.id is distinct from v_group_id) then
    raise exception 'WEEKLY_SOURCE_CONTEXT_GROUP_MISMATCH' using errcode='22023';
  end if;
  v_group_id:=v_group.id;

  if v_report_scope_id is not null then
    select * into v_scope
    from public.weekly_source_report_scopes where id=v_report_scope_id;
    if not found or v_group.source_family<>'NHSP'
       or v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id
       or v_scope.environment is distinct from v_group.environment
       or v_scope.agency_id is distinct from v_group.agency_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc
       or (v_client_id is not null and v_scope.client_id is distinct from v_client_id) then
      raise exception 'WEEKLY_SOURCE_CONTEXT_REPORT_SCOPE_MISMATCH' using errcode='22023';
    end if;
    v_client_id:=v_scope.client_id;
  else
    if v_group.source_family='NHSP' and v_operation='BUILD_PROJECTION'
       and exists(
         select 1 from public.weekly_source_format_profiles profile
         where profile.id=v_upload.source_format_profile_id
           and profile.profile_code='NHSP_FINAL_BACKING_V1'
       ) then
      raise exception 'WEEKLY_SOURCE_CONTEXT_REPORT_SCOPE_REQUIRED' using errcode='22023';
    end if;
    if v_operation='BUILD_PROJECTION' then
      begin
        v_client_id:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
      exception when invalid_text_representation then
        raise exception 'WEEKLY_SOURCE_CONTEXT_CLIENT_INVALID' using errcode='55000';
      end;
    end if;
    if v_client_id is not null and not exists(
      select 1 from public.weekly_source_group_clients membership
      where membership.source_group_id=v_group.id
        and membership.client_id=v_client_id
        and v_cycle.finalisation_week_ending between membership.valid_from
          and coalesce(membership.valid_to,'infinity'::date)
    ) then
      raise exception 'WEEKLY_SOURCE_CONTEXT_CLIENT_SCOPE_MISMATCH' using errcode='22023';
    end if;
  end if;
  if v_client_id is not null then
    select client.name into strict v_client_name
    from public.clients client where client.id=v_client_id;
  elsif v_group.source_family='ROSTER' then
    select pg_catalog.count(*),coalesce(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object('client_id',client.id,'client_name',client.name)
      order by client.name,client.id
    ),'[]'::jsonb)
      into v_client_choice_count,v_client_choices
    from public.weekly_source_group_clients membership
    join public.clients client on client.id=membership.client_id
    where membership.source_group_id=v_group.id
      and v_cycle.finalisation_week_ending between membership.valid_from
        and coalesce(membership.valid_to,'infinity'::date);
    if v_client_choice_count=1 then
      v_client_id:=(v_client_choices->0->>'client_id')::uuid;
      v_client_name:=v_client_choices->0->>'client_name';
    elsif v_client_choice_count=0 then
      raise exception 'WEEKLY_SOURCE_CONTEXT_CLIENT_NOT_FOUND' using errcode='22023';
    end if;
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,
    case when v_operation='DISCOVER_SCOPE' then 'UPLOAD_SOURCE' else 'RECHECK_SOURCE' end,
    v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );

  if v_operation='DISCOVER_SCOPE' then
    return pg_catalog.jsonb_build_object(
      'ok',true,'context_version','WEEKLY_SOURCE_UPLOAD_CONTEXT_V1',
      'environment',v_group.environment,'agency_id',v_group.agency_id,
      'source_group_id',v_group.id,'source_group_code',v_group.code,
      'source_group_name',v_group.display_name,'source_family',v_group.source_family,
      'source_cycle_id',v_cycle.id,'finalisation_week_ending',v_cycle.finalisation_week_ending,
      'cutoff_at_utc',v_cycle.cutoff_at_utc,'cycle_state',v_cycle.state,
      'report_scope_id',v_report_scope_id,'client_id',v_client_id,
      'client_name',v_client_name,'nhsp_report_heading_name',v_group.nhsp_report_heading_name,
      'client_selection_required',v_group.source_family='ROSTER' and v_client_id is null,
      'client_choices',v_client_choices,
      'authority_scope_version',coalesce(v_scope.version,v_cycle.version),
      'authority_scope_kind',case when v_report_scope_id is null then 'CYCLE' else 'NHSP_REPORT_SCOPE' end
    );
  end if;

  select * into strict v_profile
  from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;

  for v_source_row in
    select source_row.*
    from public.weekly_source_upload_rows source_row
    where source_row.upload_id=v_upload.id
    order by source_row.source_row_ordinal,source_row.id
  loop
    v_row_client_id:=v_client_id;
    v_source_name:=nullif(pg_catalog.btrim(coalesce(
      v_source_row.bounded_raw_columns_json->>'worker_name',
      v_source_row.bounded_raw_columns_json->>'candidate',
      v_source_row.source_candidate_identity
    )), '');
    v_source_id:=nullif(pg_catalog.btrim(coalesce(
      v_source_row.bounded_raw_columns_json->>'worker_unique_id',
      v_source_row.bounded_raw_columns_json->>'candidate_id',''
    )), '');
    v_source_id_2:=nullif(pg_catalog.btrim(coalesce(
      v_source_row.bounded_raw_columns_json->>'candidate_uid',''
    )), '');
    v_source_id_3:=nullif(pg_catalog.btrim(coalesce(
      v_source_row.bounded_raw_columns_json->>'payroll_number',''
    )), '');
    v_source_client:=nullif(pg_catalog.btrim(coalesce(
      v_source_row.bounded_raw_columns_json->>'trust',
      v_source_row.source_client_identity
    )), '');
    if v_row_client_id is null and v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1' then
      select case when pg_catalog.count(*)=1 then pg_catalog.min(client.id::text)::uuid end
        into v_row_client_id
      from public.weekly_source_group_clients membership
      join public.clients client on client.id=membership.client_id
      where membership.source_group_id=v_group.id
        and v_source_row.work_date between membership.valid_from
          and coalesce(membership.valid_to,'infinity'::date)
        and pg_catalog.lower(pg_catalog.btrim(client.name))=
          pg_catalog.lower(pg_catalog.btrim(coalesce(v_source_client,'')));
    end if;
    v_norm_name:=pg_catalog.lower(pg_catalog.regexp_replace(
      pg_catalog.btrim(coalesce(v_source_name,'')),'[[:space:]]+',' ','g'
    ));
    v_norm_compact:=pg_catalog.regexp_replace(v_norm_name,'[^a-z0-9]+','','g');

    select coalesce(pg_catalog.array_agg(candidate_id order by candidate_id),'{}'::uuid[])
      into v_candidate_ids
    from (
      select distinct candidate.id as candidate_id
      from public.candidates candidate
      where candidate.active
        and (
          (v_source_id is not null and pg_catalog.lower(pg_catalog.btrim(candidate.tms_ref))=
             pg_catalog.lower(v_source_id))
          or (v_source_id_2 is not null and pg_catalog.lower(pg_catalog.btrim(candidate.tms_ref))=
             pg_catalog.lower(v_source_id_2))
          or (v_source_id_3 is not null and pg_catalog.lower(pg_catalog.btrim(candidate.tms_ref))=
             pg_catalog.lower(v_source_id_3))
          or exists(
            select 1
            from pg_catalog.jsonb_array_elements_text(
              case when pg_catalog.jsonb_typeof(candidate.nhsp_hr_name_aliases)='array'
                then candidate.nhsp_hr_name_aliases else '[]'::jsonb end
            ) alias(value)
            where pg_catalog.lower(pg_catalog.btrim(alias.value))=v_norm_name
               or pg_catalog.regexp_replace(pg_catalog.lower(alias.value),'[^a-z0-9]+','','g')=v_norm_compact
          )
          or exists(
            select 1 from public.hr_name_mappings mapping
            where mapping.active and mapping.candidate_id=candidate.id
              and (
                pg_catalog.lower(pg_catalog.btrim(mapping.hr_name_norm))=v_norm_name
                or pg_catalog.regexp_replace(pg_catalog.lower(mapping.hr_name_norm),'[^a-z0-9]+','','g')=v_norm_compact
              )
              and (nullif(pg_catalog.btrim(coalesce(mapping.hospital_or_trust,'')),'') is null
                or pg_catalog.lower(pg_catalog.btrim(mapping.hospital_or_trust))=
                   pg_catalog.lower(pg_catalog.btrim(coalesce(v_source_client,''))))
          )
        )
    ) exact_saved;
    v_candidate_id:=case when pg_catalog.cardinality(v_candidate_ids)=1 then v_candidate_ids[1] end;
    select coalesce(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'candidate_id',candidate.id,'display_name',coalesce(nullif(candidate.display_name,''),
          pg_catalog.btrim(coalesce(candidate.first_name,'')||' '||coalesce(candidate.last_name,''))),
        'tms_ref',candidate.tms_ref
      ) order by candidate.id
    ),'[]'::jsonb) into v_candidate_matches
    from public.candidates candidate where candidate.id=any(v_candidate_ids);

    v_contracts:='[]'::jsonb;
    v_prior_contract_id:=null;
    v_prior_work_event_id:=null;
    v_prior_candidate_count:=0;
    if v_candidate_id is not null and v_row_client_id is not null then
      select coalesce(pg_catalog.jsonb_agg(contract_item order by contract_item->>'contract_id'),'[]'::jsonb)
        into v_contracts
      from (
        select pg_catalog.jsonb_build_object(
          'contract_id',contract.id,
          'candidate_id',contract.candidate_id,
          'client_id',contract.client_id,
          'valid_from',contract.start_date,
          'valid_to',contract.end_date,
          'display_label',pg_catalog.concat_ws(' · ',nullif(contract.role,''),nullif(contract.band,''),nullif(contract.display_site,'')),
          'role',contract.role,'band',contract.band,
          'pay_type',contract.pay_method_snapshot,
          'rates_json',contract.rates_json,
          'contract_updated_at',contract.updated_at,
          'weekly_source_applicable',true,
          'schedule_compatible',private.weekly_source_schedule_compatible_v1(
            contract.std_schedule_json,v_source_row.work_date
          ),
          'verified_role_band_match',private.weekly_source_verified_role_band_match_v1(
            case when v_profile.profile_code in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1')
              then 'NHSP' else 'HR_WEEKLY' end,
            v_source_row.role_band_source,v_candidate_id,v_row_client_id,
            contract.id,contract.band,v_source_row.work_date
          ),
          'effective_policy',private._weekly_source_effective_policy_v1(
            v_row_client_id,contract.id,v_source_row.work_date
          ),
          'settings_authority',private._contract_settings_effective_core_v1(
            v_row_client_id,contract.id,v_source_row.work_date,'IMPORT',null
          )
        ) contract_item
        from public.contracts contract
        where contract.candidate_id=v_candidate_id
          and contract.client_id=v_row_client_id
          and v_source_row.work_date between contract.start_date and coalesce(contract.end_date,'infinity'::date)
      ) eligible;

      -- G6-13 / XSG-010.  Only the three roster profiles use the source
      -- system's own line identity as the durable work-event key.  An NHSP
      -- Reference Number is evidence, never sole durable identity
      -- (`24 §9`; `25 §7` Removed), so NHSP lineage resolves through the
      -- schedule tuple below, exactly as the publication owner now keys it.
      if v_profile.profile_code in (
        'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
        'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
        'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
      ) and nullif(pg_catalog.btrim(coalesce(v_source_row.external_source_key,'')),'') is not null then
        select event.id,resolution.contract_id
          into v_prior_work_event_id,v_prior_contract_id
        from public.weekly_work_events event
        left join lateral (
          select saved.contract_id
          from public.weekly_source_row_resolutions saved
          where saved.work_event_id=event.id and saved.mapping_state='RESOLVED'
          order by saved.created_at_utc desc,saved.id desc limit 1
        ) resolution on true
        where event.first_source_group_id=v_group.id
          and event.source_format_profile_id=v_profile.id
          and event.identity_kind='PROFILE_EXTERNAL_KEY'
          and event.profile_external_key=v_source_row.external_source_key
          and event.candidate_id=v_candidate_id and event.client_id=v_row_client_id
        order by event.created_at_utc,event.id limit 1;
      else
        -- WP-37, against `24 §9` step 2.  The pack keys identity on "exact
        -- Candidate, actual Client, worked date and **compatible** schedule",
        -- so lineage must NOT demand exact Actual start and end: `24 §6.2`
        -- ("Paid eight hours becomes nine") is a later source change on ONE
        -- root, and an NHSP correction routinely moves the times.  The
        -- cardinality is checked explicitly: none means new work, and more than
        -- one is `24 §9` step 4 (Office confirmation), where nothing is
        -- suggested and the server refuses to guess.
        select pg_catalog.count(*)::integer,
               pg_catalog.min(event.id::text)::uuid
          into v_prior_candidate_count,v_prior_work_event_id
        from public.weekly_work_events event
        where event.first_source_group_id=v_group.id
          and event.source_format_profile_id=v_profile.id
          and event.identity_kind='SCHEDULE_TUPLE'
          and event.candidate_id=v_candidate_id and event.client_id=v_row_client_id
          and event.work_date=v_source_row.work_date
          and private.weekly_source_work_event_schedule_compatible_v1(
                event.id,v_source_row.start_at_local,v_source_row.end_at_local
              );
        if v_prior_candidate_count<>1 then
          v_prior_work_event_id:=null;
        else
          -- The Contract is a SUGGESTION only: `weekly_source_projection_build_v1`
          -- re-proves DURABLE_LINEAGE against a prior RESOLVED resolution for
          -- this same work event before it will accept the selection method.
          select saved.contract_id into v_prior_contract_id
          from public.weekly_source_row_resolutions saved
          where saved.work_event_id=v_prior_work_event_id
            and saved.mapping_state='RESOLVED'
          order by saved.created_at_utc desc,saved.id desc limit 1;
        end if;
      end if;
    end if;

    v_rows:=v_rows||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'upload_row_id',v_source_row.id,
      'source_row_ordinal',v_source_row.source_row_ordinal,
      'external_source_key',v_source_row.external_source_key,
      'source_candidate_identity',v_source_row.source_candidate_identity,
      'source_client_identity',v_source_row.source_client_identity,
      'work_date',v_source_row.work_date,
      'start_at_local',v_source_row.start_at_local,
      'end_at_local',v_source_row.end_at_local,
      'break_minutes',v_source_row.break_minutes,
      'actual_net_minutes',v_source_row.actual_net_minutes,
      'row_finalisation_state',v_source_row.row_finalisation_state,
      'role_band_source',v_source_row.role_band_source,
      'source_commission_pence',v_source_row.source_commission_pence,
      'source_total_cost_pence',v_source_row.source_total_cost_pence,
      'source_shift_charge_pence',v_source_row.source_shift_charge_pence,
      'source_money_parse_state',v_source_row.source_money_parse_state,
      'source_expense_pence',v_source_row.source_expense_pence,
      'source_expense_parse_state',v_source_row.source_expense_parse_state,
      'candidate_match_count',pg_catalog.cardinality(v_candidate_ids),
      'candidate_matches',v_candidate_matches,
      'candidate_id',v_candidate_id,
      'client_id',v_row_client_id,
      'contracts',v_contracts,
      'prior_accepted_contract_id',v_prior_contract_id,
      'prior_work_event_id',v_prior_work_event_id,
      -- `24 §9` step 4.  True means more than one plausible existing work
      -- record remains, so Office must confirm the relationship; the server
      -- refuses to publish the row until it does.
      'prior_work_event_ambiguous',v_prior_candidate_count>1
    ));
  end loop;

  if v_upload.correction_session_id is not null then
    select publication.* into v_publication
    from public.weekly_final_source_correction_sessions correction
    join public.weekly_source_projection_publications publication
      on publication.id=correction.replacement_projection_publication_id
    where correction.id=v_upload.correction_session_id
      and correction.replacement_correction_upload_id=v_upload.id
      and publication.upload_id=v_upload.id
      and publication.correction_session_id=correction.id
      and publication.state='CORRECTION_READY';
  else
    select publication.* into v_publication
    from public.weekly_source_projection_publications publication
    where publication.upload_id=v_upload.id
      and publication.state='CURRENT'
    order by publication.published_at_utc desc nulls last,publication.id desc
    limit 1;
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'context_version','WEEKLY_SOURCE_PROJECTION_CONTEXT_V1',
    'environment',v_group.environment,'agency_id',v_group.agency_id,
    'source_group_id',v_group.id,'source_family',v_group.source_family,
    'source_cycle_id',v_cycle.id,'report_scope_id',v_report_scope_id,
    'client_id',v_client_id,'client_name',v_client_name,
    'profile_id',v_profile.profile_code,'profile_version',v_profile.version,
    'upload_id',v_upload.id,
    'row_manifest_hash',case when v_upload.row_manifest_hash is null then null
      else pg_catalog.encode(v_upload.row_manifest_hash,'hex') end,
    'projection_publication_id',v_publication.id,
    'projection_state',v_publication.state,
    'comparison_manifest_hash',case when v_publication.comparison_manifest_hash is null then null
      else pg_catalog.encode(v_publication.comparison_manifest_hash,'hex') end,
    'issue_set_hash',case when v_publication.issue_set_hash is null then null
      else pg_catalog.encode(v_publication.issue_set_hash,'hex') end,
    'correction_session_id',v_upload.correction_session_id,
    'correction_session_version',case when v_upload.correction_session_id is null then null else (
      select correction.version
      from public.weekly_final_source_correction_sessions correction
      where correction.id=v_upload.correction_session_id
    ) end,
    'authority_scope_kind',case when v_report_scope_id is null then 'CYCLE' else 'NHSP_REPORT_SCOPE' end,
    'authority_scope_version',coalesce(v_scope.version,v_cycle.version),
    'rows',v_rows
  );
end;
$function$;

alter function private.weekly_source_schedule_compatible_v1(jsonb,date) owner to postgres;
alter function private.weekly_source_verified_role_band_match_v1(text,text,uuid,uuid,uuid,text,date)
  owner to postgres;
revoke all on function private.weekly_source_schedule_compatible_v1(jsonb,date)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_verified_role_band_match_v1(text,text,uuid,uuid,uuid,text,date)
  from public,anon,authenticated,service_role;
alter function public.weekly_source_upload_context_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_upload_context_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_upload_context_v1(jsonb) to service_role;

comment on function public.weekly_source_upload_context_v1(jsonb) is
  'Service-only Plan 6 scope and exact saved mapping discovery. Browser financial facts are not accepted.';

commit;
