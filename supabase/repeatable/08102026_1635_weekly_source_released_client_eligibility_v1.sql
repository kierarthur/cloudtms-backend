-- Repeatable CloudTMS authority: released NHSP row-wise client eligibility.
-- Full replacements preserve the established service/Office authority,
-- concurrency, replay, immutable source, contract and finance boundaries.
\set ON_ERROR_STOP on

begin;

-- Client eligibility is derived from the saved profile, never a caller flag.
-- Previously released NHSP checking files may contain any NHSP-enabled client.
-- Use the existing effective-date settings order; older/future true flags must
-- not override the applicable setting. Missing settings fail closed.
-- Final backing reports and every other profile retain their source membership
-- and exact report-client boundaries. Contract/policy eligibility is unchanged.
create or replace function private.weekly_source_upload_client_eligible_v1(
  p_upload_id uuid,p_client_id uuid,p_work_date date
) returns boolean
language sql stable
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select p_work_date is not null and exists(
    select 1
    from public.weekly_source_uploads upload
    join public.weekly_source_format_profiles profile on profile.id=upload.source_format_profile_id
    join public.weekly_source_cycles cycle on cycle.id=upload.source_cycle_id
    join public.weekly_source_groups source_group on source_group.id=cycle.source_group_id
    join public.clients client on client.id=p_client_id
    left join public.weekly_source_report_scopes scope on scope.id=upload.report_scope_id
    where upload.id=p_upload_id
      and case when profile.profile_code='NHSP_PREFINAL_RELEASED_V1' then
        source_group.source_family='NHSP'
        and upload.report_scope_id is null
        and profile.row_finalisation_capability='CHECKING_ONLY'
        and not profile.single_client_required
        and coalesce((
          select settings.is_nhsp
          from public.client_settings settings
          where settings.client_id=client.id
            and (settings.effective_from is null or settings.effective_from<=p_work_date)
          order by settings.effective_from desc nulls last,settings.updated_at desc,settings.id desc
          limit 1
        ),false)
      else
        exists(select 1 from public.weekly_source_group_clients membership
          where membership.source_group_id=cycle.source_group_id
            and membership.client_id=client.id
            and p_work_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date))
        and (upload.report_scope_id is null or (
          scope.client_id=client.id and scope.source_cycle_id=cycle.id
          and scope.source_group_id=cycle.source_group_id))
      end
  );
$function$;
alter function private.weekly_source_upload_client_eligible_v1(uuid,uuid,date) owner to postgres;
revoke all on function private.weekly_source_upload_client_eligible_v1(uuid,uuid,date)
  from public,anon,authenticated,service_role;

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
  v_office_choice private.weekly_source_office_row_choices%rowtype;
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
    if v_operation='BUILD_PROJECTION' and v_cycle.scope_client_id is not null and v_cycle.scope_client_id<>v_client_id then
      raise exception 'WEEKLY_SOURCE_CONTEXT_CLIENT_SCOPE_MISMATCH' using errcode='22023';
    end if;
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
      'previous_coverage',(
        select pg_catalog.jsonb_build_object(
          'start_local_date',prior.confirmed_coverage_start_local_date,
          'end_local_date',prior.confirmed_coverage_end_local_date
        )
        from public.weekly_source_uploads prior
        where prior.id=v_cycle.current_complete_upload_id
          and prior.state='CURRENT'
      ),
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
    -- Released reports identify the client on each row. A legacy upload-level
    -- client hint must not silently assign every row to that one client.
    v_row_client_id:=case when v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1'
      then null else v_client_id end;
    select * into v_office_choice from private.weekly_source_office_row_choices
      where upload_row_id=v_source_row.id order by id desc limit 1;
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
      from public.clients client
      where private.weekly_source_upload_client_eligible_v1(
          v_upload.id,client.id,v_source_row.work_date)
        and pg_catalog.lower(pg_catalog.btrim(client.name))=
          pg_catalog.lower(pg_catalog.btrim(coalesce(v_source_client,'')));
    end if;
    if v_office_choice.client_id is not null then
      select client.id into v_row_client_id
      from public.clients client
      where client.id=v_office_choice.client_id
        and private.weekly_source_upload_client_eligible_v1(
          v_upload.id,client.id,v_source_row.work_date);
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
    if v_office_choice.candidate_id is not null then
      select coalesce(pg_catalog.array_agg(id),'{}'::uuid[]) into v_candidate_ids
        from public.candidates where id=v_office_choice.candidate_id and active;
    end if;
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
      'office_selected_contract_id',v_office_choice.contract_id,
      'office_selected_work_event_id',v_office_choice.work_event_id,
      'office_separate_shift',coalesce(v_office_choice.separate_shift,false),
      'protected_matches',private.weekly_source_protected_match_candidates_v1(v_source_row.id,v_candidate_id,v_row_client_id),
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

alter function public.weekly_source_upload_context_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_upload_context_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_upload_context_v1(jsonb) to service_role;

create or replace function public.weekly_source_office_recheck_begin_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_request_id uuid;
  v_hash bytea;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_prior private.weekly_source_office_rechecks%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;
  v_choice private.weekly_source_office_row_choices%rowtype;
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_candidate uuid;
  v_client uuid;
  v_contract uuid;
  v_work_event uuid;
  v_match_reason text;
  v_separate_shift boolean:=false;
  v_version bigint;
  v_current_publication uuid;
  v_current_upload uuid;
  v_begin jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
    or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key where key not in
      ('actor_user_id','request_id','upload_id','projection_publication_id',
       'expected_authority_scope_version','expected_row_manifest_hash','upload_row_id',
       'candidate_id','client_id','contract_id','work_event_id','match_reason','separate_shift')) then
    raise exception 'WEEKLY_SOURCE_RECHECK_REQUEST_INVALID' using errcode='22023';
  end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  v_request_id:=(p_request->>'request_id')::uuid;
  if v_actor is null or v_request_id is null then
    raise exception 'WEEKLY_SOURCE_RECHECK_REQUEST_INVALID' using errcode='22023';
  end if;
  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFFICE_RECHECK_V1',p_request);
  select * into strict v_upload from public.weekly_source_uploads where id=(p_request->>'upload_id')::uuid;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    pg_catalog.encode(v_upload.declared_scope_fingerprint,'hex'),73241837));
  select * into strict v_cycle from public.weekly_source_cycles where id=v_upload.source_cycle_id for update;
  select * into strict v_upload from public.weekly_source_uploads where id=v_upload.id;
  if v_upload.report_scope_id is null then
    v_version:=v_cycle.version; v_current_upload:=v_cycle.current_complete_upload_id;
    v_current_publication:=v_cycle.current_projection_publication_id;
  else
    select * into strict v_scope from public.weekly_source_report_scopes where id=v_upload.report_scope_id for update;
    v_version:=v_scope.version; v_current_upload:=v_scope.current_complete_upload_id;
    v_current_publication:=v_scope.current_projection_publication_id;
  end if;
  perform private.weekly_source_office_authority_v1(v_actor,'RECHECK_SOURCE',v_cycle.source_group_id,
    coalesce(v_scope.client_id,nullif(v_upload.file_metadata_json->>'client_id','')::uuid),v_cycle.finalisation_week_ending);
  if v_upload.purpose<>'ORDINARY' or v_upload.state<>'CURRENT'
    or v_current_upload is distinct from v_upload.id or v_cycle.state in ('FINALISING','FINALISED')
    or v_scope.state in ('FINALISING','FINALISED') then
    raise exception 'WEEKLY_SOURCE_RECHECK_NOT_CURRENT' using errcode='55000';
  end if;
  select * into v_prior from private.weekly_source_office_rechecks where request_id=v_request_id;
  if found then
    if v_prior.request_hash is distinct from v_hash or v_prior.actor_user_id<>v_actor then
      raise exception 'WEEKLY_SOURCE_RECHECK_REPLAY_CONFLICT' using errcode='23505';
    end if;
    select * into strict v_publication from public.weekly_source_projection_publications where id=v_prior.publication_id;
    if v_publication.authority_scope_version<>v_version or v_publication.state not in ('BUILDING','CURRENT') then
      raise exception 'WEEKLY_SOURCE_RECHECK_NOT_CURRENT' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object('ok',true,'status',v_publication.state,
      'publication_id',v_publication.id,'upload_id',v_upload.id,'authority_scope_version',v_version,'idempotent',true);
  end if;
  if v_version is distinct from (p_request->>'expected_authority_scope_version')::bigint
    or v_current_publication is distinct from (p_request->>'projection_publication_id')::uuid
    or v_upload.row_manifest_hash is distinct from private.weekly_source_hex32_v1(
      p_request->>'expected_row_manifest_hash','WEEKLY_SOURCE_ROW_MANIFEST_HASH_REQUIRED') then
    raise exception 'WEEKLY_SOURCE_PREVIEW_STALE' using errcode='40001';
  end if;
  select * into strict v_publication from public.weekly_source_projection_publications where id=v_current_publication for update;
  if v_publication.state<>'CURRENT' or v_publication.upload_id<>v_upload.id then
    raise exception 'WEEKLY_SOURCE_PREVIEW_STALE' using errcode='40001';
  end if;
  if p_request ?| array['candidate_id','client_id','contract_id','work_event_id','separate_shift'] then
    select * into strict v_row from public.weekly_source_upload_rows
      where id=(p_request->>'upload_row_id')::uuid and upload_id=v_upload.id;
    select * into v_choice from private.weekly_source_office_row_choices
      where upload_row_id=v_row.id order by id desc limit 1;
    select * into v_resolution from public.weekly_source_row_resolutions
      where upload_row_id=v_row.id order by generation desc,id desc limit 1;
    v_candidate:=coalesce((p_request->>'candidate_id')::uuid,v_choice.candidate_id,v_resolution.candidate_id);
    v_client:=coalesce((p_request->>'client_id')::uuid,v_choice.client_id,v_resolution.client_id);
    v_contract:=case when p_request ? 'contract_id' then (p_request->>'contract_id')::uuid
      when p_request ?| array['candidate_id','client_id'] then null else v_choice.contract_id end;
    if v_candidate is not null and not exists(select 1 from public.candidates where id=v_candidate and active) then
      raise exception 'WEEKLY_SOURCE_CANDIDATE_INACTIVE_OR_MISSING' using errcode='22023';
    end if;
    if v_client is not null and not private.weekly_source_upload_client_eligible_v1(
      v_upload.id,v_client,v_row.work_date) then
      raise exception 'WEEKLY_SOURCE_CLIENT_NOT_ELIGIBLE' using errcode='22023';
    end if;
    if v_scope.client_id is not null and v_client is distinct from v_scope.client_id then
      raise exception 'WEEKLY_SOURCE_CLIENT_NOT_ELIGIBLE' using errcode='22023';
    end if;
    if v_contract is not null and not exists(select 1 from public.contracts
      where id=v_contract and candidate_id=v_candidate and client_id=v_client
        and v_row.work_date between start_date and coalesce(end_date,'infinity'::date)) then
      raise exception 'WEEKLY_SOURCE_CONTRACT_NOT_ELIGIBLE' using errcode='22023';
    end if;
    if p_request ? 'separate_shift' then
      if p_request->'separate_shift'<>'true'::jsonb or p_request ? 'work_event_id' then
        raise exception 'WEEKLY_SOURCE_PROTECTED_MATCH_NOT_ELIGIBLE' using errcode='22023';
      end if;
      v_separate_shift:=true;
      v_match_reason:=btrim(p_request->>'match_reason');
      v_contract:=coalesce(v_contract,v_resolution.contract_id);
      if v_contract is null or v_match_reason is null or char_length(v_match_reason) not between 1 and 1000
        or jsonb_array_length(private.weekly_source_protected_match_candidates_v1(v_row.id,v_candidate,v_client))=0
        or exists(select 1 from public.weekly_exceptional_pay_family_events item
          where item.durable_work_event_id=v_resolution.work_event_id) then
        raise exception 'WEEKLY_SOURCE_PROTECTED_MATCH_NOT_ELIGIBLE' using errcode='22023';
      end if;
    elsif p_request ? 'work_event_id' then
      v_work_event:=(p_request->>'work_event_id')::uuid;
      v_match_reason:=btrim(p_request->>'match_reason');
      if v_work_event is null or v_match_reason is null or char_length(v_match_reason) not between 1 and 1000
        or not exists(select 1 from jsonb_array_elements(
          private.weekly_source_protected_match_candidates_v1(v_row.id,v_candidate,v_client)) choice
          where choice->>'work_event_id'=v_work_event::text
            and choice->>'contract_id'=coalesce(v_contract,v_resolution.contract_id)::text) then
        raise exception 'WEEKLY_SOURCE_PROTECTED_MATCH_NOT_ELIGIBLE' using errcode='22023';
      end if;
      v_contract:=coalesce(v_contract,v_resolution.contract_id);
      -- A source identity already included in final authority cannot be moved
      -- to another payable identity by this pre-finalisation choice.
      if exists(select 1 from public.weekly_source_row_resolutions old_resolution
        where old_resolution.upload_row_id=v_row.id and old_resolution.work_event_id<>v_work_event
          and (exists(select 1 from public.weekly_source_final_snapshot_lines final_line
            where final_line.work_event_id=old_resolution.work_event_id)
            or exists(select 1 from public.weekly_source_billing_movements movement
              where movement.work_event_id=old_resolution.work_event_id))) then
        raise exception 'WEEKLY_SOURCE_PROTECTED_MATCH_FINAL_AUTHORITY_CONFLICT' using errcode='55000';
      end if;
    elsif not (p_request ?| array['candidate_id','client_id','contract_id']) then
      v_work_event:=v_choice.work_event_id; v_match_reason:=v_choice.match_reason;
    end if;
    insert into private.weekly_source_office_row_choices(upload_row_id,candidate_id,client_id,contract_id,actor_user_id,work_event_id,match_reason,separate_shift)
      values(v_row.id,v_candidate,v_client,v_contract,v_actor,v_work_event,v_match_reason,v_separate_shift);
  elsif p_request ? 'upload_row_id' then
    raise exception 'WEEKLY_SOURCE_RECHECK_REQUEST_INVALID' using errcode='22023';
  end if;
  update public.weekly_source_projection_publications set state='STALE' where id=v_publication.id;
  if v_upload.report_scope_id is null then
    update public.weekly_source_cycles set version=version+1,projection_state='REBUILDING',current_projection_publication_id=null
      where id=v_cycle.id returning version into v_version;
  else
    update public.weekly_source_report_scopes set version=version+1,projection_state='REBUILDING',current_projection_publication_id=null,
      updated_at_utc=pg_catalog.transaction_timestamp() where id=v_scope.id returning version into v_version;
  end if;
  v_begin:=public.weekly_source_projection_begin_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'upload_id',v_upload.id,'expected_authority_scope_version',v_version));
  if v_begin->>'status'<>'BUILDING' then
    raise exception 'WEEKLY_SOURCE_RECHECK_BEGIN_FAILED' using errcode='55000';
  end if;
  -- This is a new comparison of an existing immutable upload, not its first
  -- publication. Keep earlier resolutions as history and explicitly identify
  -- the new generation; the publisher must census this generation only.
  if v_version>2147483647 then
    raise exception 'WEEKLY_SOURCE_PROJECTION_GENERATION_OVERFLOW' using errcode='22003';
  end if;
  update public.weekly_source_projection_publications
    set projection_generation=v_version::integer
    where id=(v_begin->>'publication_id')::uuid and state='BUILDING';
  v_begin:=v_begin||pg_catalog.jsonb_build_object('projection_generation',v_version::integer);
  insert into private.weekly_source_office_rechecks(request_id,actor_user_id,upload_id,prior_publication_id,publication_id,request_hash,request_json)
    values(v_request_id,v_actor,v_upload.id,v_publication.id,(v_begin->>'publication_id')::uuid,v_hash,p_request);
  return v_begin;
end;
$function$;
alter function public.weekly_source_office_recheck_begin_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_office_recheck_begin_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_office_recheck_begin_v1(jsonb) to service_role;

notify pgrst, 'reload schema';

commit;
