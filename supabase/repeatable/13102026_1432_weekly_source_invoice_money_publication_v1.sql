-- HealthRoster v2 F5/F6 complete controlling successors; adjacent source owners only.
\set ON_ERROR_STOP on
begin;

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
  v_coverage_context jsonb;
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
    v_coverage_context:=private.weekly_source_upload_coverage_context_v1(v_client_id,v_cycle.id);
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
      'effective_rule_set',private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id),
      'client_selection_required',v_group.source_family='ROSTER' and v_client_id is null,
      'client_choices',v_client_choices,
      'authority_scope_version',coalesce(v_scope.version,v_cycle.version),
      'authority_scope_kind',case when v_report_scope_id is null then 'CYCLE' else 'NHSP_REPORT_SCOPE' end
    )||case when v_coverage_context is null then '{}'::jsonb
      else jsonb_build_object('coverage_context',v_coverage_context) end;
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
          'effective_rule_set',private.weekly_source_effective_rule_set_v1(v_row_client_id,v_cycle.id),
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
      'source_gross_pence',v_source_row.source_gross_pence,'source_vat_pence',v_source_row.source_vat_pence,
      'finalised_at_utc',v_source_row.finalised_at_utc,
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

create or replace function public.weekly_source_upload_stage_rows_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_upload_id uuid;
  v_upload public.weekly_source_uploads%rowtype;
  v_group_id uuid;
  v_cycle_date date;
  v_client_id uuid;
  v_item jsonb;
  v_key text;
  v_ordinal integer;
  v_hash bytea;
  v_existing_physical public.weekly_source_physical_rows%rowtype;
  v_existing_row public.weekly_source_upload_rows%rowtype;
  v_existing_money public.weekly_source_money_cell_evidence%rowtype;
  v_existing_expense public.weekly_source_expense_cell_evidence%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;
  v_money public.weekly_source_money_cell_evidence%rowtype;
  v_expense public.weekly_source_expense_cell_evidence%rowtype;
  v_payload jsonb;
  v_physical_added integer:=0;
  v_rows_added integer:=0;
  v_money_added integer:=0;
  v_expenses_added integer:=0;
begin
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists(
       select 1 from pg_catalog.jsonb_object_keys(p_request) key
       where key not in ('actor_user_id','upload_id','physical_rows','normalised_rows',
         'money_evidence','expense_evidence')
     ) then
    raise exception 'WEEKLY_SOURCE_STAGE_ROWS_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_upload_id:=(p_request->>'upload_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_STAGE_ROWS_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_upload_id is null
     or pg_catalog.jsonb_typeof(coalesce(p_request->'physical_rows','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(p_request->'normalised_rows','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(p_request->'money_evidence','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(p_request->'expense_evidence','[]'::jsonb))<>'array' then
    raise exception 'WEEKLY_SOURCE_STAGE_ROWS_REQUEST_INVALID' using errcode='22023';
  end if;
  if pg_catalog.jsonb_array_length(coalesce(p_request->'physical_rows','[]'::jsonb))>10000
     or pg_catalog.jsonb_array_length(coalesce(p_request->'normalised_rows','[]'::jsonb))>10000
     or pg_catalog.jsonb_array_length(coalesce(p_request->'money_evidence','[]'::jsonb))>40001
     or pg_catalog.jsonb_array_length(coalesce(p_request->'expense_evidence','[]'::jsonb))>10000 then
    raise exception 'WEEKLY_SOURCE_STAGE_ROWS_BATCH_TOO_LARGE' using errcode='22023';
  end if;

  select * into v_upload
  from public.weekly_source_uploads where id=v_upload_id for update;
  if not found or v_upload.state<>'STAGING' then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_STAGING' using errcode='55000';
  end if;
  if v_upload.uploaded_by_user_id is distinct from v_actor then
    raise exception 'WEEKLY_SOURCE_UPLOAD_ACTOR_MISMATCH' using errcode='42501';
  end if;
  select cycle.source_group_id,cycle.finalisation_week_ending
    into strict v_group_id,v_cycle_date
  from public.weekly_source_cycles cycle where cycle.id=v_upload.source_cycle_id;
  if v_upload.report_scope_id is null then
    begin
      v_client_id:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
    exception when invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_CLIENT_SCOPE_INVALID' using errcode='55000';
    end;
  else
    select scope.client_id into strict v_client_id
    from public.weekly_source_report_scopes scope
    where scope.id=v_upload.report_scope_id
      and scope.source_cycle_id=v_upload.source_cycle_id;
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'UPLOAD_SOURCE',v_group_id,v_client_id,v_cycle_date
  );

  for v_item in
    select value from pg_catalog.jsonb_array_elements(
      coalesce(p_request->'physical_rows','[]'::jsonb)
    )
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object' then
      raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_INVALID' using errcode='22023';
    end if;
    select key into v_key from pg_catalog.jsonb_object_keys(v_item) key
    where key not in ('source_row_ordinal','bounded_raw_cells_json','classification','row_sha256')
    order by key limit 1;
    if v_key is not null then
      raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_UNKNOWN_FIELD'
        using errcode='22023',detail=v_key;
    end if;
    begin
      v_ordinal:=(v_item->>'source_row_ordinal')::integer;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_ORDINAL_INVALID' using errcode='22023';
    end;
    if v_ordinal is null or v_ordinal<1 or v_ordinal>v_upload.physical_row_count
       or pg_catalog.jsonb_typeof(v_item->'bounded_raw_cells_json')<>'object'
       or coalesce(v_item->>'classification','') not in (
         'HEADER','TRAILER','PROFILE_PROVED_NON_ECONOMIC_CONTINUATION',
         'ACCEPTED_SHIFT','BLOCKING_ECONOMIC_DUPLICATE','BLOCKING_MALFORMED'
       ) then
      raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_INVALID' using errcode='22023';
    end if;
    v_payload:=pg_catalog.jsonb_build_object(
      'source_row_ordinal',v_ordinal,
      'classification',v_item->>'classification',
      'bounded_raw_cells_json',v_item->'bounded_raw_cells_json'
    );
    v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_PHYSICAL_ROW_V1',v_payload);
    if nullif(v_item->>'row_sha256','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'row_sha256','WEEKLY_SOURCE_PHYSICAL_ROW_HASH_INVALID'
       ) is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_HASH_MISMATCH' using errcode='22023';
    end if;
    select * into v_existing_physical
    from public.weekly_source_physical_rows
    where upload_id=v_upload_id and source_row_ordinal=v_ordinal;
    if found then
      if v_existing_physical.classification is distinct from v_item->>'classification'
         or v_existing_physical.bounded_raw_cells_json is distinct from v_item->'bounded_raw_cells_json'
         or v_existing_physical.row_sha256 is distinct from v_hash then
        raise exception 'WEEKLY_SOURCE_PHYSICAL_ROW_REPLAY_CONFLICT' using errcode='55000';
      end if;
    else
      insert into public.weekly_source_physical_rows(
        upload_id,source_row_ordinal,bounded_raw_cells_json,row_sha256,classification
      ) values (
        v_upload_id,v_ordinal,v_item->'bounded_raw_cells_json',v_hash,v_item->>'classification'
      );
      v_physical_added:=v_physical_added+1;
    end if;
  end loop;

  for v_item in
    select value from pg_catalog.jsonb_array_elements(
      coalesce(p_request->'normalised_rows','[]'::jsonb)
    )
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object' then
      raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_INVALID' using errcode='22023';
    end if;
    select key into v_key from pg_catalog.jsonb_object_keys(v_item) key
    where key not in (
      'source_row_ordinal','external_source_key','source_candidate_identity',
      'source_client_identity','work_date','start_at_local','end_at_local',
      'break_minutes','actual_net_minutes','row_finalisation_state','finalised_by',
      'role_band_source','source_commission_pence','source_total_cost_pence',
      'source_shift_charge_pence','source_money_parse_state','source_gross_pence','source_vat_pence','finalised_at_utc',
      'source_qualification_profile_version','source_expense_pence',
      'source_expense_parse_state','bounded_raw_columns_json','normalised_row_hash'
    ) order by key limit 1;
    if v_key is not null then
      raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_UNKNOWN_FIELD'
        using errcode='22023',detail=v_key;
    end if;
    begin
      v_row:=pg_catalog.jsonb_populate_record(null::public.weekly_source_upload_rows,v_item);
    exception when others then
      raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_TYPE_INVALID' using errcode='22023';
    end;
    v_row.row_finalisation_state:=coalesce(v_row.row_finalisation_state,'NOT_APPLICABLE');
    v_row.source_money_parse_state:=coalesce(v_row.source_money_parse_state,'NOT_APPLICABLE');
    v_row.source_expense_parse_state:=coalesce(v_row.source_expense_parse_state,'NOT_APPLICABLE');
    v_row.bounded_raw_columns_json:=coalesce(v_row.bounded_raw_columns_json,'{}'::jsonb);
    if v_row.source_row_ordinal is null or v_row.source_row_ordinal<1
       or v_row.source_row_ordinal>v_upload.physical_row_count
       or nullif(pg_catalog.btrim(coalesce(v_row.source_candidate_identity,'')),'') is null
       or nullif(pg_catalog.btrim(coalesce(v_row.source_client_identity,'')),'') is null
       or v_row.work_date is null
       or pg_catalog.jsonb_typeof(v_row.bounded_raw_columns_json)<>'object'
       or not exists(
         select 1 from public.weekly_source_physical_rows physical
         where physical.upload_id=v_upload_id
           and physical.source_row_ordinal=v_row.source_row_ordinal
           and physical.classification='ACCEPTED_SHIFT'
       ) then
      raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_INVALID' using errcode='22023';
    end if;
    v_payload:=pg_catalog.jsonb_build_object(
      'source_row_ordinal',v_row.source_row_ordinal,
      'external_source_key',v_row.external_source_key,
      'source_candidate_identity',v_row.source_candidate_identity,
      'source_client_identity',v_row.source_client_identity,
      'work_date',v_row.work_date,
      'start_at_local',v_row.start_at_local,
      'end_at_local',v_row.end_at_local,
      'break_minutes',v_row.break_minutes,
      'actual_net_minutes',v_row.actual_net_minutes,
      'row_finalisation_state',v_row.row_finalisation_state,
      'finalised_by',v_row.finalised_by,
      'role_band_source',v_row.role_band_source,
      'source_commission_pence',v_row.source_commission_pence,
      'source_total_cost_pence',v_row.source_total_cost_pence,
      'source_shift_charge_pence',v_row.source_shift_charge_pence,
      'source_money_parse_state',v_row.source_money_parse_state,
      'source_qualification_profile_version',v_row.source_qualification_profile_version,
      'source_expense_pence',v_row.source_expense_pence,
      'source_expense_parse_state',v_row.source_expense_parse_state,
      'bounded_raw_columns_json',v_row.bounded_raw_columns_json
    )||case when v_row.source_gross_pence is not null or v_row.source_vat_pence is not null or v_row.finalised_at_utc is not null
      then jsonb_build_object('source_gross_pence',v_row.source_gross_pence,'source_vat_pence',v_row.source_vat_pence,'finalised_at_utc',v_row.finalised_at_utc) else '{}'::jsonb end;
    v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_NORMALISED_ROW_V1',v_payload);
    if nullif(v_item->>'normalised_row_hash','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'normalised_row_hash','WEEKLY_SOURCE_NORMALISED_ROW_HASH_INVALID'
       ) is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_HASH_MISMATCH' using errcode='22023';
    end if;
    select * into v_existing_row
    from public.weekly_source_upload_rows
    where upload_id=v_upload_id and source_row_ordinal=v_row.source_row_ordinal;
    if found then
      if v_existing_row.normalised_row_hash is distinct from v_hash then
        raise exception 'WEEKLY_SOURCE_NORMALISED_ROW_REPLAY_CONFLICT' using errcode='55000';
      end if;
    else
      -- Two accepted economic rows of one upload may not share an external
      -- source key.  For HealthRoster the key is the `Request Id`, which the
      -- profile uses as durable work identity, so a repeat is a duplicate
      -- economic row (14 s4.2.8).  For NHSP the broker stages the Reference
      -- Number, which 24 s9 says is evidence and never identity; a report that
      -- repeats one is a source shape this build cannot account for, and 14
      -- s4.2.8 blocks it.
      --
      -- Either way the refusal must be STATED.  Until this guard existed the
      -- only thing that stopped it was the unique index
      -- weekly_source_upload_rows_external_key_uq, which reached the caller as
      -- a bare 23505 from a constraint name - an unhandled error, not a
      -- refusal, and the exact shape a positive and its same-Reference full
      -- negative produce.  A unique index must never be the statement of a
      -- rule (standing rule 5); it stays as the backstop behind this one.
      if v_row.external_source_key is not null
         and exists(
           select 1
           from public.weekly_source_upload_rows existing_key
           where existing_key.upload_id=v_upload_id
             and existing_key.external_source_key=v_row.external_source_key
             and existing_key.source_row_ordinal is distinct from v_row.source_row_ordinal
         ) then
        raise exception 'WEEKLY_SOURCE_UPLOAD_DUPLICATE_EXTERNAL_KEY'
          using errcode='55000',
                detail=pg_catalog.jsonb_build_object(
                  'upload_id',v_upload_id,
                  'source_row_ordinal',v_row.source_row_ordinal,
                  'external_source_key',v_row.external_source_key
                )::text;
      end if;
      insert into public.weekly_source_upload_rows(
        upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
        source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
        actual_net_minutes,row_finalisation_state,finalised_by,role_band_source,source_gross_pence,source_vat_pence,finalised_at_utc,
        source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
        source_money_parse_state,source_qualification_profile_version,
        source_expense_pence,source_expense_parse_state,normalised_row_hash,
        bounded_raw_columns_json
      ) values (
        v_upload_id,v_row.source_row_ordinal,v_row.external_source_key,
        v_row.source_candidate_identity,v_row.source_client_identity,v_row.work_date,
        v_row.start_at_local,v_row.end_at_local,v_row.break_minutes,v_row.actual_net_minutes,
        v_row.row_finalisation_state,v_row.finalised_by,v_row.role_band_source,v_row.source_gross_pence,v_row.source_vat_pence,v_row.finalised_at_utc,
        v_row.source_commission_pence,v_row.source_total_cost_pence,
        v_row.source_shift_charge_pence,v_row.source_money_parse_state,
        v_row.source_qualification_profile_version,v_row.source_expense_pence,
        v_row.source_expense_parse_state,v_hash,v_row.bounded_raw_columns_json
      );
      v_rows_added:=v_rows_added+1;
    end if;
  end loop;

  for v_item in
    select value from pg_catalog.jsonb_array_elements(
      coalesce(p_request->'money_evidence','[]'::jsonb)
    )
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object' then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_INVALID' using errcode='22023';
    end if;
    select key into v_key from pg_catalog.jsonb_object_keys(v_item) key
    where key not in (
      'source_row_ordinal','money_field_kind','source_column_index','cell_coordinate',
      'source_kind','original_token','decoded_token','cell_type_marker',
      'formula_present','parse_state','parsed_pence','source_file_sha256','token_sha256'
    ) order by key limit 1;
    if v_key is not null then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_UNKNOWN_FIELD'
        using errcode='22023',detail=v_key;
    end if;
    begin
      v_money:=pg_catalog.jsonb_populate_record(null::public.weekly_source_money_cell_evidence,v_item);
    exception when others then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_TYPE_INVALID' using errcode='22023';
    end;
    v_money.formula_present:=coalesce(v_money.formula_present,false);
    if v_money.source_row_ordinal is null or v_money.source_row_ordinal<1
       or v_money.source_row_ordinal>v_upload.physical_row_count
       or v_money.money_field_kind not in ('COMMISSION','TOTAL_COST','FMC','BOTTOM_TOTAL_COST','SOURCE_NET','SOURCE_GROSS','SOURCE_VAT')
       or v_money.source_column_index is null or v_money.source_column_index<0
       or v_money.source_kind not in (
         'XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN','HTML_DECODED_TEXT','CSV_DECODED_TEXT'
       )
       or v_money.original_token is null or v_money.decoded_token is null
       or v_money.parse_state not in (
         'VALID','MISSING','INVALID','EXCESS_PRECISION','FORMULA',
         'UNSUPPORTED_CELL_TYPE','OVERFLOW'
       ) then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_INVALID' using errcode='22023';
    end if;
    if nullif(v_item->>'source_file_sha256','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'source_file_sha256','WEEKLY_SOURCE_EVIDENCE_FILE_HASH_INVALID'
       ) is distinct from v_upload.content_sha256 then
      raise exception 'WEEKLY_SOURCE_EVIDENCE_FILE_HASH_MISMATCH' using errcode='22023';
    end if;
    v_payload:=pg_catalog.jsonb_build_object(
      'source_row_ordinal',v_money.source_row_ordinal,
      'money_field_kind',v_money.money_field_kind,
      'source_column_index',v_money.source_column_index,
      'cell_coordinate',v_money.cell_coordinate,
      'source_kind',v_money.source_kind,
      'original_token',v_money.original_token,
      'decoded_token',v_money.decoded_token,
      'cell_type_marker',v_money.cell_type_marker,
      'formula_present',v_money.formula_present,
      'parse_state',v_money.parse_state,
      'parsed_pence',v_money.parsed_pence
    );
    v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_MONEY_TOKEN_V1',v_payload);
    if nullif(v_item->>'token_sha256','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'token_sha256','WEEKLY_SOURCE_MONEY_TOKEN_HASH_INVALID'
       ) is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_MONEY_TOKEN_HASH_MISMATCH' using errcode='22023';
    end if;
    select * into v_existing_money
    from public.weekly_source_money_cell_evidence
    where upload_id=v_upload_id and source_row_ordinal=v_money.source_row_ordinal
      and money_field_kind=v_money.money_field_kind;
    if found then
      if v_existing_money.token_sha256 is distinct from v_hash
         or v_existing_money.source_file_sha256 is distinct from v_upload.content_sha256 then
        raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_REPLAY_CONFLICT' using errcode='55000';
      end if;
    else
      insert into public.weekly_source_money_cell_evidence(
        upload_id,source_row_ordinal,money_field_kind,source_column_index,
        cell_coordinate,source_kind,original_token,decoded_token,cell_type_marker,
        formula_present,parse_state,parsed_pence,source_file_sha256,token_sha256
      ) values (
        v_upload_id,v_money.source_row_ordinal,v_money.money_field_kind,
        v_money.source_column_index,v_money.cell_coordinate,v_money.source_kind,
        v_money.original_token,v_money.decoded_token,v_money.cell_type_marker,
        v_money.formula_present,v_money.parse_state,v_money.parsed_pence,
        v_upload.content_sha256,v_hash
      );
      v_money_added:=v_money_added+1;
    end if;
  end loop;

  for v_item in
    select value from pg_catalog.jsonb_array_elements(
      coalesce(p_request->'expense_evidence','[]'::jsonb)
    )
  loop
    if pg_catalog.jsonb_typeof(v_item)<>'object' then
      raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_INVALID' using errcode='22023';
    end if;
    select key into v_key from pg_catalog.jsonb_object_keys(v_item) key
    where key not in (
      'source_row_ordinal','source_column_index','cell_coordinate','source_kind',
      'original_token','decoded_token','cell_type_marker','formula_present',
      'lexical_profile_version','parse_state','parsed_pence',
      'source_file_sha256','token_sha256'
    ) order by key limit 1;
    if v_key is not null then
      raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_UNKNOWN_FIELD'
        using errcode='22023',detail=v_key;
    end if;
    begin
      v_expense:=pg_catalog.jsonb_populate_record(null::public.weekly_source_expense_cell_evidence,v_item);
    exception when others then
      raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_TYPE_INVALID' using errcode='22023';
    end;
    v_expense.formula_present:=coalesce(v_expense.formula_present,false);
    v_expense.lexical_profile_version:=coalesce(
      v_expense.lexical_profile_version,'SOURCE_FIXED_EXPENSE_PENCE_V1'
    );
    if v_expense.source_row_ordinal is null or v_expense.source_row_ordinal<1
       or v_expense.source_row_ordinal>v_upload.physical_row_count
       or v_expense.source_column_index is null or v_expense.source_column_index<0
       or v_expense.source_kind not in (
         'XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN','HTML_DECODED_TEXT','CSV_DECODED_TEXT'
       )
       or v_expense.original_token is null or v_expense.decoded_token is null
       or v_expense.lexical_profile_version<>'SOURCE_FIXED_EXPENSE_PENCE_V1'
       or v_expense.parse_state not in (
         'VALID','OMITTED_ZERO','INVALID','EXCESS_PRECISION','FORMULA',
         'UNSUPPORTED_CELL_TYPE','OVERFLOW'
       ) then
      raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_INVALID' using errcode='22023';
    end if;
    if nullif(v_item->>'source_file_sha256','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'source_file_sha256','WEEKLY_SOURCE_EVIDENCE_FILE_HASH_INVALID'
       ) is distinct from v_upload.content_sha256 then
      raise exception 'WEEKLY_SOURCE_EVIDENCE_FILE_HASH_MISMATCH' using errcode='22023';
    end if;
    v_payload:=pg_catalog.jsonb_build_object(
      'source_row_ordinal',v_expense.source_row_ordinal,
      'source_column_index',v_expense.source_column_index,
      'cell_coordinate',v_expense.cell_coordinate,
      'source_kind',v_expense.source_kind,
      'original_token',v_expense.original_token,
      'decoded_token',v_expense.decoded_token,
      'cell_type_marker',v_expense.cell_type_marker,
      'formula_present',v_expense.formula_present,
      'lexical_profile_version',v_expense.lexical_profile_version,
      'parse_state',v_expense.parse_state,
      'parsed_pence',v_expense.parsed_pence
    );
    v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_EXPENSE_TOKEN_V1',v_payload);
    if nullif(v_item->>'token_sha256','') is not null
       and private.weekly_source_hex32_v1(
         v_item->>'token_sha256','WEEKLY_SOURCE_EXPENSE_TOKEN_HASH_INVALID'
       ) is distinct from v_hash then
      raise exception 'WEEKLY_SOURCE_EXPENSE_TOKEN_HASH_MISMATCH' using errcode='22023';
    end if;
    select * into v_existing_expense
    from public.weekly_source_expense_cell_evidence
    where upload_id=v_upload_id and source_row_ordinal=v_expense.source_row_ordinal;
    if found then
      if v_existing_expense.token_sha256 is distinct from v_hash
         or v_existing_expense.source_file_sha256 is distinct from v_upload.content_sha256 then
        raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_REPLAY_CONFLICT' using errcode='55000';
      end if;
    else
      insert into public.weekly_source_expense_cell_evidence(
        upload_id,source_row_ordinal,source_column_index,cell_coordinate,source_kind,
        original_token,decoded_token,cell_type_marker,formula_present,
        lexical_profile_version,parse_state,parsed_pence,source_file_sha256,token_sha256
      ) values (
        v_upload_id,v_expense.source_row_ordinal,v_expense.source_column_index,
        v_expense.cell_coordinate,v_expense.source_kind,v_expense.original_token,
        v_expense.decoded_token,v_expense.cell_type_marker,v_expense.formula_present,
        v_expense.lexical_profile_version,v_expense.parse_state,v_expense.parsed_pence,
        v_upload.content_sha256,v_hash
      );
      v_expenses_added:=v_expenses_added+1;
    end if;
  end loop;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status','STAGING','upload_id',v_upload_id,
    'inserted',pg_catalog.jsonb_build_object(
      'physical_rows',v_physical_added,'normalised_rows',v_rows_added,
      'money_evidence',v_money_added,'expense_evidence',v_expenses_added
    ),
    'stored',pg_catalog.jsonb_build_object(
      'physical_rows',(select count(*) from public.weekly_source_physical_rows where upload_id=v_upload_id),
      'normalised_rows',(select count(*) from public.weekly_source_upload_rows where upload_id=v_upload_id),
      'money_evidence',(select count(*) from public.weekly_source_money_cell_evidence where upload_id=v_upload_id),
      'expense_evidence',(select count(*) from public.weekly_source_expense_cell_evidence where upload_id=v_upload_id)
    )
  );
end;
$function$;

create or replace function private.weekly_source_upload_seal_core_v1(
  p_upload_id uuid,
  p_actor_user_id uuid
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_prior_upload public.weekly_source_uploads%rowtype;
  v_client_id uuid;
  v_expected_scope bytea;
  v_manifest_hash bytea;
  v_version_before bigint;
  v_version_after bigint;
  v_attempt_id uuid;
  v_actual_physical integer;
  v_actual_header integer;
  v_actual_trailer integer;
  v_actual_continuation integer;
  v_actual_accepted integer;
  v_actual_duplicate integer;
  v_actual_malformed integer;
  v_actual_blocked integer;
  v_normalised_count integer;
  v_money_count integer;
  v_expense_count integer;
  v_actual_coverage_start date;
  v_actual_coverage_end date;
begin
  if p_upload_id is null or p_actor_user_id is null then
    raise exception 'WEEKLY_SOURCE_SEAL_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_upload
  from public.weekly_source_uploads where id=p_upload_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_FOUND' using errcode='22023';
  end if;
  if v_upload.uploaded_by_user_id is distinct from p_actor_user_id then
    raise exception 'WEEKLY_SOURCE_UPLOAD_ACTOR_MISMATCH' using errcode='42501';
  end if;
  if v_upload.state in ('CURRENT','SUPERSEDED','CORRECTION_READY') then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status',v_upload.state,'logical_upload_id',v_upload.id,
      'row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'),
      'current_pointer_moved',false,'idempotent',true
    );
  end if;
  if v_upload.state<>'STAGING' then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_STAGING' using errcode='55000';
  end if;

  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_upload.source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups where id=v_cycle.source_group_id;
  select * into strict v_profile
  from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
  if not v_group.active or not v_profile.active
     or v_profile.profile_code not in (
       'NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1',
       'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
       'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
       'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
     ) or v_profile.version<>1 then
    raise exception 'WEEKLY_SOURCE_RELEASE_PROFILE_STALE' using errcode='55000';
  end if;
  if v_upload.header_coordinate_map_hash is distinct from
     private.weekly_source_sha256_jsonb_v1(
       'WEEKLY_SOURCE_HEADER_COORDINATE_MAP_V1',v_upload.header_coordinate_map_json
     ) then
    raise exception 'WEEKLY_SOURCE_HEADER_MAP_HASH_MISMATCH' using errcode='55000';
  end if;

  if v_upload.report_scope_id is not null then
    if v_profile.profile_code<>'NHSP_FINAL_BACKING_V1' then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_PROFILE_INVALID' using errcode='55000';
    end if;
    select * into strict v_scope
    from public.weekly_source_report_scopes
    where id=v_upload.report_scope_id;
    if v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id
       or v_scope.environment is distinct from v_group.environment
       or v_scope.agency_id is distinct from v_group.agency_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_MISMATCH' using errcode='55000';
    end if;
    v_client_id:=v_scope.client_id;
  else
    begin
      v_client_id:=nullif(v_upload.file_metadata_json->>'client_id','')::uuid;
    exception when invalid_text_representation then
      raise exception 'WEEKLY_SOURCE_CLIENT_SCOPE_INVALID' using errcode='55000';
    end;
  end if;
  v_expected_scope:=private.weekly_source_scope_fingerprint_v1(
    v_group.environment,v_group.agency_id,v_group.id,v_cycle.id,
    v_upload.report_scope_id,v_client_id
  );
  if v_upload.declared_scope_fingerprint is distinct from v_expected_scope then
    raise exception 'WEEKLY_SOURCE_DECLARED_SCOPE_STALE' using errcode='55000';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(pg_catalog.encode(v_expected_scope,'hex'),73241837)
  );
  select * into strict v_upload
  from public.weekly_source_uploads where id=p_upload_id for update;
  if v_upload.uploaded_by_user_id is distinct from p_actor_user_id then
    raise exception 'WEEKLY_SOURCE_UPLOAD_ACTOR_MISMATCH' using errcode='42501';
  end if;
  if v_upload.state in ('CURRENT','SUPERSEDED','CORRECTION_READY') then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status',v_upload.state,'logical_upload_id',v_upload.id,
      'row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'),
      'current_pointer_moved',false,'idempotent',true
    );
  end if;
  if v_upload.state<>'STAGING'
     or v_upload.declared_scope_fingerprint is distinct from v_expected_scope then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_STAGING' using errcode='55000';
  end if;
  if v_upload.report_scope_id is null then
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_upload.source_cycle_id for update;
  else
    -- NHSP authority is per Trust.  Do not serialize independent Trusts by
    -- taking the shared cycle row lock; the exact report-scope row is its head.
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_upload.source_cycle_id;
    select * into strict v_scope
    from public.weekly_source_report_scopes where id=v_upload.report_scope_id for update;
    if v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id
       or v_scope.environment is distinct from v_group.environment
       or v_scope.agency_id is distinct from v_group.agency_id
       or v_scope.client_id is distinct from v_client_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_CHANGED' using errcode='55000';
    end if;
  end if;
  if private.weekly_source_scope_fingerprint_v1(
       v_group.environment,v_group.agency_id,v_group.id,v_cycle.id,
       v_upload.report_scope_id,v_client_id
     ) is distinct from v_expected_scope then
    raise exception 'WEEKLY_SOURCE_SCOPE_CHANGED' using errcode='55000';
  end if;
  perform private.weekly_source_office_authority_v1(
    p_actor_user_id,'SEAL_SOURCE',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );

  select
    count(*)::integer,
    count(*) filter(where classification='HEADER')::integer,
    count(*) filter(where classification='TRAILER')::integer,
    count(*) filter(where classification='PROFILE_PROVED_NON_ECONOMIC_CONTINUATION')::integer,
    count(*) filter(where classification='ACCEPTED_SHIFT')::integer,
    count(*) filter(where classification='BLOCKING_ECONOMIC_DUPLICATE')::integer,
    count(*) filter(where classification='BLOCKING_MALFORMED')::integer
  into v_actual_physical,v_actual_header,v_actual_trailer,v_actual_continuation,
       v_actual_accepted,v_actual_duplicate,v_actual_malformed
  from public.weekly_source_physical_rows where upload_id=v_upload.id;
  select count(*)::integer,
         count(*) filter(where row_finalisation_state in (
           'BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE'
         ))::integer
    into v_normalised_count,v_actual_blocked
  from public.weekly_source_upload_rows where upload_id=v_upload.id;
  v_actual_blocked:=v_actual_blocked+v_actual_duplicate+v_actual_malformed;

  if v_actual_physical<>v_upload.physical_row_count
     or v_actual_header<>v_upload.header_count
     or v_actual_trailer<>v_upload.trailer_count
     or v_actual_continuation<>v_upload.continuation_count
     or v_actual_accepted<>v_upload.accepted_count
     or v_actual_duplicate<>v_upload.blocking_economic_duplicate_count
     or v_actual_malformed<>v_upload.malformed_count
     or v_actual_blocked<>v_upload.blocked_count
     or v_actual_physical<>v_actual_header+v_actual_trailer+v_actual_continuation+
       v_actual_accepted+v_actual_duplicate+v_actual_malformed
     or v_normalised_count<>v_actual_accepted then
    raise exception 'WEEKLY_SOURCE_ROW_COUNTS_MISMATCH' using errcode='55000';
  end if;
  if exists(
    select 1 from public.weekly_source_upload_rows normalised
    left join public.weekly_source_physical_rows physical
      on physical.upload_id=normalised.upload_id
     and physical.source_row_ordinal=normalised.source_row_ordinal
    where normalised.upload_id=v_upload.id
      and (physical.id is null or physical.classification<>'ACCEPTED_SHIFT')
  ) or exists(
    select 1 from public.weekly_source_physical_rows physical
    left join public.weekly_source_upload_rows normalised
      on normalised.upload_id=physical.upload_id
     and normalised.source_row_ordinal=physical.source_row_ordinal
    where physical.upload_id=v_upload.id
      and physical.classification='ACCEPTED_SHIFT'
      and normalised.id is null
  ) then
    raise exception 'WEEKLY_SOURCE_ACCEPTED_ROW_EVIDENCE_MISMATCH' using errcode='55000';
  end if;
  if v_actual_duplicate<>0 or v_actual_malformed<>0 or v_actual_blocked<>0 then
    raise exception 'WEEKLY_SOURCE_UPLOAD_HAS_BLOCKERS' using errcode='55000';
  end if;

  -- The sealed row state is profile authority, not a caller-selected label.
  -- A source-zero row is retained as evidence only by the one released
  -- no-finalisation-column profile which proves that zero means source absence.
  if exists(
    select 1
    from public.weekly_source_upload_rows source_row
    where source_row.upload_id=v_upload.id
      and (
        (v_profile.profile_code in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1')
          and source_row.row_finalisation_state<>'SOURCE_WORKED')
        or (v_profile.profile_code in (
              'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
              'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1'
            ) and source_row.row_finalisation_state not in ('SOURCE_WORKED','SOURCE_UNFINALISED'))
        or (v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
            and source_row.row_finalisation_state not in ('SOURCE_WORKED','SOURCE_ABSENT_ZERO'))
        or (source_row.row_finalisation_state='SOURCE_WORKED' and (
              source_row.start_at_local is null
              or source_row.end_at_local is null
              or source_row.break_minutes is null
              or source_row.actual_net_minutes is null
              or source_row.actual_net_minutes<=0
              or pg_catalog.mod(
                   extract(epoch from (source_row.end_at_local-source_row.start_at_local))::numeric,
                   60::numeric
                 )<>0
              or source_row.actual_net_minutes<>
                 (extract(epoch from (source_row.end_at_local-source_row.start_at_local))/60)::integer
                 - source_row.break_minutes
            ))
        or (source_row.row_finalisation_state='SOURCE_ABSENT_ZERO'
            and source_row.actual_net_minutes is distinct from 0)
      )
  ) then
    raise exception 'WEEKLY_SOURCE_ROW_FINALISATION_STATE_INVALID' using errcode='55000';
  end if;

  -- WP-54.  Two source-row admission rules the pack states and that nothing
  -- enforced.  They are placed HERE, after the state validation immediately
  -- above, because that validation is what guarantees every SOURCE_WORKED row
  -- has a complete, internally consistent Actual tuple; a guard that reads
  -- start and end before that check could silently skip a row whose times were
  -- absent.  Both raise, and public.weekly_source_upload_seal_atomic_v1 turns
  -- the raise into a typed REJECTED attempt with the code as its reason, so a
  -- refused report is refused as a whole -- pack 14 section 4.1.7, "Finalisation
  -- is all or nothing.  A blocker cannot be removed, unticked or ignored".
  --
  -- NHSP-BR-006 (pack 14 section 4.1.3): the confirmed cutoff occurrence may
  -- not predate any row's real Actual finish, overnight finishes included.  The
  -- cutoff handed over is v_cycle.cutoff_at_utc, which for a report scope this
  -- function has already proved equal to v_scope.cutoff_at_utc under the scope
  -- row lock above.
  perform private.weekly_source_cutoff_admission_assert_v1(
    v_upload.id,v_profile.profile_code,v_cycle.cutoff_at_utc
  );
  -- Pack 03 section 7 with pack 14 section 4.2.8: one person cannot work two
  -- intersecting intervals, and two overlapping records are not made valid by
  -- differing references.
  perform private.weekly_source_overlap_admission_assert_v1(v_upload.id);

  if v_actual_accepted=0
     and v_upload.coverage_proof_kind<>'EXPLICIT_EMPTY_CONFIRMATION'
     and coalesce((v_upload.file_metadata_json->>'explicit_empty_attestation')::boolean,false)=false then
    raise exception 'WEEKLY_SOURCE_EMPTY_ATTESTATION_REQUIRED' using errcode='55000';
  end if;

  if v_profile.profile_code='NHSP_FINAL_BACKING_V1' then
    if v_upload.coverage_proof_kind<>'NHSP_TRUST_REPORT_SCOPE'
       or v_upload.confirmed_coverage_start_local_date is not null
       or v_upload.confirmed_coverage_end_local_date is not null
       or private.weekly_source_canonical_report_number_v1(
         v_upload.file_metadata_json->>'nhsp_report_number'
       ) is null
       or v_upload.money_lexical_authority_version<>
          'XLSX_BINARY64_SAME_VALUE_PENCE_V1' then
      raise exception 'WEEKLY_SOURCE_NHSP_FINAL_EVIDENCE_INVALID' using errcode='55000';
    end if;
    select count(*)::integer into v_money_count
    from public.weekly_source_money_cell_evidence where upload_id=v_upload.id;
    if v_money_count not in (v_actual_accepted*3,v_actual_accepted*3+1)
       or (v_money_count=v_actual_accepted*3+1 and not exists(
         select 1 from public.weekly_source_money_cell_evidence money
         join public.weekly_source_physical_rows physical
           on physical.upload_id=money.upload_id
          and physical.source_row_ordinal=money.source_row_ordinal
         where money.upload_id=v_upload.id and money.money_field_kind='BOTTOM_TOTAL_COST'
           and physical.classification='TRAILER'
       ))
       or exists(
         select 1
         from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (
             source_row.source_money_parse_state<>'VALID'
             or source_row.source_commission_pence is null
             or source_row.source_total_cost_pence is null
             or source_row.source_shift_charge_pence is null
             or source_row.source_shift_charge_pence<>
                source_row.source_commission_pence+source_row.source_total_cost_pence
             or sign(source_row.source_commission_pence) not in (0,sign(source_row.source_shift_charge_pence))
             or sign(source_row.source_total_cost_pence) not in (0,sign(source_row.source_shift_charge_pence))
             or (select count(*) from public.weekly_source_money_cell_evidence money
                 where money.upload_id=source_row.upload_id
                   and money.source_row_ordinal=source_row.source_row_ordinal
                    and money.money_field_kind in ('COMMISSION','TOTAL_COST','FMC')
                    and money.parse_state='VALID'
                    and not money.formula_present
                    and (
                      (v_upload.parser_summary_json->>'source_kind'='HTML'
                       and money.source_kind='HTML_DECODED_TEXT')
                      or (coalesce(v_upload.parser_summary_json->>'source_kind','XLSX')='XLSX'
                          and money.source_kind in ('XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN'))
                    )
                    and nullif(money.cell_coordinate,'') is not null)<>3
             or (select money.parsed_pence from public.weekly_source_money_cell_evidence money
                 where money.upload_id=source_row.upload_id
                   and money.source_row_ordinal=source_row.source_row_ordinal
                   and money.money_field_kind='COMMISSION')
                is distinct from source_row.source_commission_pence
             or (select money.parsed_pence from public.weekly_source_money_cell_evidence money
                 where money.upload_id=source_row.upload_id
                   and money.source_row_ordinal=source_row.source_row_ordinal
                   and money.money_field_kind='TOTAL_COST')
                is distinct from source_row.source_total_cost_pence
             or (select money.parsed_pence from public.weekly_source_money_cell_evidence money
                 where money.upload_id=source_row.upload_id
                   and money.source_row_ordinal=source_row.source_row_ordinal
                   and money.money_field_kind='FMC') is distinct from 0
           )
       )
       or exists(
          select 1 from public.weekly_source_money_cell_evidence money
          where money.upload_id=v_upload.id
            and (
              money.source_file_sha256 is distinct from v_upload.content_sha256
              or not (
                (v_upload.parser_summary_json->>'source_kind'='HTML'
                 and money.source_kind='HTML_DECODED_TEXT')
                or (coalesce(v_upload.parser_summary_json->>'source_kind','XLSX')='XLSX'
                    and money.source_kind in ('XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN'))
              )
              or nullif(money.cell_coordinate,'') is null
              or (money.formula_present is distinct from (money.parse_state='FORMULA'))
              or (money.money_field_kind='BOTTOM_TOTAL_COST'
                  and (money.parse_state<>'VALID' or money.formula_present))
            )
        ) then
      raise exception 'WEEKLY_SOURCE_NHSP_MONEY_EVIDENCE_INVALID' using errcode='55000';
    end if;
  elsif v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1' then
    -- Pre-final NHSP money is checking evidence only.  It must be retained
    -- exactly, but an unparseable component remains a non-blocking pricing
    -- issue for the comparison owner rather than rejecting the hours upload.
    if v_upload.money_lexical_authority_version<>
         'XLSX_BINARY64_SAME_VALUE_PENCE_V1' then
      raise exception 'WEEKLY_SOURCE_NHSP_PREFINAL_EVIDENCE_INVALID' using errcode='55000';
    end if;
    select count(*)::integer into v_money_count
    from public.weekly_source_money_cell_evidence where upload_id=v_upload.id;
    if v_money_count<>v_actual_accepted*2
       or exists(
         select 1
         from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (
             (select count(*) from public.weekly_source_money_cell_evidence money
              where money.upload_id=source_row.upload_id
                and money.source_row_ordinal=source_row.source_row_ordinal
                and money.money_field_kind in ('COMMISSION','TOTAL_COST'))<>2
             or (
               source_row.source_money_parse_state='VALID'
               and (
                 source_row.source_commission_pence is null
                 or source_row.source_total_cost_pence is null
                 or source_row.source_shift_charge_pence is null
                 or source_row.source_shift_charge_pence<>
                    source_row.source_commission_pence+source_row.source_total_cost_pence
                 or sign(source_row.source_commission_pence) not in
                    (0,sign(source_row.source_shift_charge_pence))
                 or sign(source_row.source_total_cost_pence) not in
                    (0,sign(source_row.source_shift_charge_pence))
                 or (select count(*) from public.weekly_source_money_cell_evidence money
                     where money.upload_id=source_row.upload_id
                       and money.source_row_ordinal=source_row.source_row_ordinal
                       and money.money_field_kind in ('COMMISSION','TOTAL_COST')
                       and money.parse_state='VALID'
                       and not money.formula_present)<>2
                 or (select money.parsed_pence
                     from public.weekly_source_money_cell_evidence money
                     where money.upload_id=source_row.upload_id
                       and money.source_row_ordinal=source_row.source_row_ordinal
                       and money.money_field_kind='COMMISSION')
                    is distinct from source_row.source_commission_pence
                 or (select money.parsed_pence
                     from public.weekly_source_money_cell_evidence money
                     where money.upload_id=source_row.upload_id
                       and money.source_row_ordinal=source_row.source_row_ordinal
                       and money.money_field_kind='TOTAL_COST')
                    is distinct from source_row.source_total_cost_pence
               )
             )
             or (
               source_row.source_money_parse_state<>'VALID'
               and (
                 source_row.source_money_parse_state not in (
                   'MISSING','INVALID','EXCESS_PRECISION','FORMULA',
                   'UNSUPPORTED_CELL_TYPE','OVERFLOW'
                 )
                 or source_row.source_commission_pence is not null
                 or source_row.source_total_cost_pence is not null
                 or source_row.source_shift_charge_pence is not null
                 or not exists(
                   select 1 from public.weekly_source_money_cell_evidence money
                   where money.upload_id=source_row.upload_id
                     and money.source_row_ordinal=source_row.source_row_ordinal
                     and money.parse_state<>'VALID'
                 )
               )
             )
           )
       )
       or exists(
         select 1 from public.weekly_source_money_cell_evidence money
         where money.upload_id=v_upload.id
           and (
             money.money_field_kind not in ('COMMISSION','TOTAL_COST')
             or money.source_file_sha256 is distinct from v_upload.content_sha256
             or not (
               (v_upload.parser_summary_json->>'source_kind'='HTML'
                and money.source_kind='HTML_DECODED_TEXT')
               or (coalesce(v_upload.parser_summary_json->>'source_kind','XLSX')='XLSX'
                   and money.source_kind in ('XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN'))
             )
             or nullif(money.cell_coordinate,'') is null
             or (money.formula_present is distinct from (money.parse_state='FORMULA'))
           )
       ) then
      raise exception 'WEEKLY_SOURCE_NHSP_PREFINAL_MONEY_EVIDENCE_INVALID'
        using errcode='55000';
    end if;
  elsif (private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id)#>>'{values,invoice_sender}')='CLIENT_SELF_BILL'
    and (private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id)#>>'{values,invoice_amount_basis}')='SOURCE_AMOUNT'
    and (private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id)#>>'{values,source_amount_basis}')='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY' then
    if v_profile.profile_code not in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1')
      or exists(select 1 from public.weekly_source_upload_rows s where s.upload_id=v_upload.id and (
        s.source_commission_pence is not null or s.source_total_cost_pence is not null
        or (s.row_finalisation_state='SOURCE_WORKED' and (s.source_money_parse_state<>'VALID'
          or s.source_qualification_profile_version is distinct from 'SOURCE_INVOICE_MONEY_PENCE_V1'
          or s.source_shift_charge_pence is null or s.source_gross_pence is null or s.source_vat_pence is null
          or s.source_shift_charge_pence<0 or s.source_gross_pence<0 or s.source_shift_charge_pence+s.source_vat_pence<>s.source_gross_pence
          or s.finalised_at_utc is null or nullif(btrim(s.finalised_by),'') is null
          or (select count(*) from public.weekly_source_money_cell_evidence e where e.upload_id=s.upload_id and e.source_row_ordinal=s.source_row_ordinal
            and e.money_field_kind in ('SOURCE_NET','SOURCE_GROSS') and e.parse_state='VALID' and not e.formula_present and e.source_file_sha256=v_upload.content_sha256
            and e.parsed_pence=case e.money_field_kind when 'SOURCE_NET' then s.source_shift_charge_pence else s.source_gross_pence end)<>2))
        or (s.row_finalisation_state<>'SOURCE_WORKED' and (s.source_money_parse_state<>'NOT_APPLICABLE' or s.source_shift_charge_pence is not null
          or s.source_gross_pence is not null or s.source_vat_pence is not null or s.finalised_at_utc is not null))))
      or exists(select 1 from public.weekly_source_money_cell_evidence e where e.upload_id=v_upload.id
        and e.money_field_kind not in ('SOURCE_NET','SOURCE_GROSS')) then
      raise exception 'WEEKLY_SOURCE_INVOICE_MONEY_EVIDENCE_INVALID' using errcode='55000'; end if;
  else
    if exists(select 1 from public.weekly_source_money_cell_evidence where upload_id=v_upload.id)
       or exists(select 1 from public.weekly_source_upload_rows source_row where source_row.upload_id=v_upload.id
         and (source_row.source_money_parse_state<>'NOT_APPLICABLE' or source_row.source_commission_pence is not null
           or source_row.source_total_cost_pence is not null or source_row.source_shift_charge_pence is not null
           or source_row.source_gross_pence is not null or source_row.source_vat_pence is not null)) then
      raise exception 'WEEKLY_SOURCE_MONEY_EVIDENCE_NOT_ALLOWED' using errcode='55000'; end if;
  end if;

  select count(*)::integer into v_expense_count
  from public.weekly_source_expense_cell_evidence where upload_id=v_upload.id;
  if v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' then
    if v_expense_count<>v_actual_accepted
       or exists(
         select 1 from public.weekly_source_upload_rows source_row
         left join public.weekly_source_expense_cell_evidence expense
           on expense.upload_id=source_row.upload_id
          and expense.source_row_ordinal=source_row.source_row_ordinal
         where source_row.upload_id=v_upload.id
           and (
             expense.id is null
             or expense.source_file_sha256 is distinct from v_upload.content_sha256
             or source_row.source_expense_parse_state not in ('VALID','OMITTED_ZERO')
             or expense.parse_state is distinct from source_row.source_expense_parse_state
             or expense.parsed_pence is distinct from source_row.source_expense_pence
           )
       ) then
      raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_INVALID' using errcode='55000';
    end if;
  elsif v_expense_count<>0 or exists(
    select 1 from public.weekly_source_upload_rows source_row
    where source_row.upload_id=v_upload.id
      and (source_row.source_expense_parse_state<>'NOT_APPLICABLE'
           or source_row.source_expense_pence is not null)
  ) then
    raise exception 'WEEKLY_SOURCE_EXPENSE_EVIDENCE_NOT_ALLOWED' using errcode='55000';
  end if;

  if v_profile.profile_code<>'NHSP_FINAL_BACKING_V1' then
    if v_upload.coverage_state<>'COMPLETE'
       or v_upload.confirmed_coverage_start_local_date is null
       or v_upload.confirmed_coverage_end_local_date is null
       or v_upload.confirmed_coverage_start_local_date>v_upload.confirmed_coverage_end_local_date
       or v_upload.coverage_timezone<>'Europe/London'
       or nullif(v_upload.coverage_confirmation_version,'') is null
       or v_upload.coverage_confirmed_by_user_id is null
       or v_upload.coverage_confirmed_at_utc is null then
      raise exception 'WEEKLY_SOURCE_COMPLETE_COVERAGE_INVALID' using errcode='55000';
    end if;
    if v_actual_accepted>0 then
      select pg_catalog.min(source_row.work_date),pg_catalog.max(source_row.work_date)
        into v_actual_coverage_start,v_actual_coverage_end
      from public.weekly_source_upload_rows source_row
      where source_row.upload_id=v_upload.id;
      -- 14 section 5.4.3, first sentence: the SUGGESTION is the file's own
      -- earliest and latest valid work Date, so it is evidence and is still
      -- checked exactly against the rows that were actually staged.  A caller
      -- may not widen the suggestion to manufacture coverage.
      if v_upload.suggested_coverage_start_local_date is distinct from v_actual_coverage_start
         or v_upload.suggested_coverage_end_local_date is distinct from v_actual_coverage_end then
        raise exception 'WEEKLY_SOURCE_COVERAGE_EVIDENCE_MISMATCH' using errcode='55000';
      end if;
      -- 14 section 5.4.3, second sentence: "Office must confirm that the export
      -- is complete for that date range."  The CONFIRMATION is Office's own
      -- attestation and is deliberately independent of the rows present, so
      -- that 5.4.5 bullet 4 -- "absent inside confirmed coverage: create one
      -- full cancellation reversal" -- can reach a shift the source has
      -- stopped sending.  Coverage derived from what arrived can never notice
      -- what stopped arriving.  It must still CONTAIN the evidence, because the
      -- finaliser admits no resolution outside confirmed coverage.
      if v_upload.confirmed_coverage_start_local_date>v_actual_coverage_start
         or v_upload.confirmed_coverage_end_local_date<v_actual_coverage_end then
        raise exception 'WEEKLY_SOURCE_COVERAGE_CONFIRMATION_NARROWER_THAN_EVIDENCE'
          using errcode='55000';
      end if;
    elsif v_upload.suggested_coverage_start_local_date is not null
       or v_upload.suggested_coverage_end_local_date is not null then
      raise exception 'WEEKLY_SOURCE_EMPTY_COVERAGE_SUGGESTION_INVALID' using errcode='55000';
    end if;
  end if;

  v_manifest_hash:=private.weekly_source_upload_manifest_hash_v1(v_upload.id);
  if v_upload.purpose='FINAL_SOURCE_CORRECTION' then
    select * into v_correction
    from public.weekly_final_source_correction_sessions
    where id=v_upload.correction_session_id for update;
    if not found or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.report_scope_id is distinct from v_upload.report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.state<>'STAGING' then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='55000';
    end if;
    update public.weekly_source_uploads
    set row_manifest_hash=v_manifest_hash,state='CORRECTION_READY'
    where id=v_upload.id;
    update public.weekly_final_source_correction_sessions
    set state='READY',version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_correction.id;
    v_attempt_id:=private.weekly_source_upload_attempt_append_v1(
      v_upload.id,p_actor_user_id,'ACCEPTED','CORRECTION_UPLOAD_READY'
    );
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','CORRECTION_READY','logical_upload_id',v_upload.id,
      'row_manifest_hash',pg_catalog.encode(v_manifest_hash,'hex'),
      'attempt_id',v_attempt_id,'current_pointer_moved',false
    );
  end if;

  if v_upload.report_scope_id is null then
    select * into strict v_cycle
    from public.weekly_source_cycles where id=v_upload.source_cycle_id for update;
    if v_cycle.state not in ('OPEN','FINALISABLE') then
      raise exception 'WEEKLY_SOURCE_CYCLE_NOT_OPEN' using errcode='55000';
    end if;
    v_version_before:=v_cycle.version;
    if v_cycle.current_complete_upload_id is not null then
      select * into strict v_prior_upload
      from public.weekly_source_uploads
      where id=v_cycle.current_complete_upload_id for update;
      if v_prior_upload.source_cycle_id is distinct from v_cycle.id
         or v_prior_upload.report_scope_id is not null
         or v_prior_upload.state<>'CURRENT' then
        raise exception 'WEEKLY_SOURCE_CURRENT_POINTER_CORRUPT' using errcode='55000';
      end if;
      -- The longitudinal coverage/omission acknowledgement does not apply to
      -- NHSP previously released shifts. This is provisional checking evidence;
      -- omitted rows never cancel NHSP work or create a financial movement.
      if v_profile.profile_code<>'NHSP_PREFINAL_RELEASED_V1'
         and (v_upload.confirmed_coverage_start_local_date>
            v_prior_upload.confirmed_coverage_start_local_date
          or v_upload.confirmed_coverage_end_local_date<
            v_prior_upload.confirmed_coverage_end_local_date)
         and coalesce(v_upload.coverage_shrink_acknowledged,false)=false then
        raise exception 'WEEKLY_SOURCE_COVERAGE_SHRINK_ACKNOWLEDGEMENT_REQUIRED'
          using errcode='55000';
      end if;
      update public.weekly_source_uploads set state='SUPERSEDED'
      where id=v_prior_upload.id;
    end if;
    update public.weekly_source_projection_publications
    set state='STALE',failure_code='SUPERSEDED_BY_COMPLETE_UPLOAD'
    where source_cycle_id=v_cycle.id and authority_scope_kind='CYCLE'
      and report_scope_id is null and state in ('BUILDING','CURRENT');
    v_version_after:=v_version_before+1;
    update public.weekly_source_uploads
    set row_manifest_hash=v_manifest_hash,state='CURRENT' where id=v_upload.id;
    update public.weekly_source_cycles
    set current_complete_upload_id=v_upload.id,version=v_version_after,
        current_projection_publication_id=null,projection_state='REBUILDING'
    where id=v_cycle.id;
    insert into public.weekly_source_upload_supersessions(
      source_cycle_id,authority_scope_kind,report_scope_id,superseding_upload_id,
      superseded_upload_id,cycle_version_before,cycle_version_after
    ) values (
      v_cycle.id,'CYCLE',null,v_upload.id,v_prior_upload.id,
      v_version_before,v_version_after
    );
  else
    select * into strict v_scope
    from public.weekly_source_report_scopes where id=v_upload.report_scope_id for update;
    if v_scope.state not in ('OPEN','FINALISABLE') then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_NOT_OPEN' using errcode='55000';
    end if;
    v_version_before:=v_scope.version;
    if v_scope.current_complete_upload_id is not null then
      select * into strict v_prior_upload
      from public.weekly_source_uploads
      where id=v_scope.current_complete_upload_id for update;
      if v_prior_upload.source_cycle_id is distinct from v_cycle.id
         or v_prior_upload.report_scope_id is distinct from v_scope.id
         or v_prior_upload.state<>'CURRENT' then
        raise exception 'WEEKLY_SOURCE_CURRENT_POINTER_CORRUPT' using errcode='55000';
      end if;
      update public.weekly_source_uploads set state='SUPERSEDED'
      where id=v_prior_upload.id;
    end if;
    update public.weekly_source_projection_publications
    set state='STALE',failure_code='SUPERSEDED_BY_COMPLETE_UPLOAD'
    where source_cycle_id=v_cycle.id and authority_scope_kind='NHSP_REPORT_SCOPE'
      and report_scope_id=v_scope.id and state in ('BUILDING','CURRENT');
    v_version_after:=v_version_before+1;
    update public.weekly_source_uploads
    set row_manifest_hash=v_manifest_hash,state='CURRENT' where id=v_upload.id;
    update public.weekly_source_report_scopes
    set current_complete_upload_id=v_upload.id,version=v_version_after,
        current_projection_publication_id=null,projection_state='REBUILDING',
        updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_scope.id;
    insert into public.weekly_source_upload_supersessions(
      source_cycle_id,authority_scope_kind,report_scope_id,superseding_upload_id,
      superseded_upload_id,cycle_version_before,cycle_version_after
    ) values (
      v_cycle.id,'NHSP_REPORT_SCOPE',v_scope.id,v_upload.id,v_prior_upload.id,
      v_version_before,v_version_after
    );
  end if;

  v_attempt_id:=private.weekly_source_upload_attempt_append_v1(
    v_upload.id,p_actor_user_id,'ACCEPTED','NEW_COMPLETE_UPLOAD'
  );
  return pg_catalog.jsonb_build_object(
    'ok',true,'status','CURRENT','logical_upload_id',v_upload.id,
    'superseded_upload_id',v_prior_upload.id,
    'authority_scope_kind',case when v_upload.report_scope_id is null
      then 'CYCLE' else 'NHSP_REPORT_SCOPE' end,
    'authority_scope_version',v_version_after,
    'row_manifest_hash',pg_catalog.encode(v_manifest_hash,'hex'),
    'attempt_id',v_attempt_id,'current_pointer_moved',true,
    'projection_state','REBUILDING'
  );
end;
$function$;

create or replace function public.weekly_source_projection_rows_apply_atomic_v1(
  p_actor_user_id uuid,
  p_publication_id uuid,
  p_rows jsonb
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_row_json jsonb;
  v_source_row public.weekly_source_upload_rows%rowtype;
  v_contract public.contracts%rowtype;
  v_work_event public.weekly_work_events%rowtype;
  v_resolution_id uuid;
  v_work_event_id uuid;
  v_mapping_state text;
  v_selection_method text;
  v_candidate_id uuid;
  v_client_id uuid;
  v_contract_id uuid;
  v_prior_work_event_id uuid;
  v_identity_kind text;
  v_link_kind text;
  v_work_event_match_kind text;
  v_work_event_match_fingerprint bytea;
  v_work_event_was_present boolean;
  v_profile_external_key text;
  v_durable_identity_hash bytea;
  -- WP-37: the `24 §9` step-2 candidate set, counted explicitly.  Safety is
  -- never expressed through `limit`/`order by` here: anything other than
  -- exactly one compatible root takes a named branch.
  v_compatible_count integer;
  v_compatible_event_id uuid;
  v_protected_matches jsonb;
  v_protected_choice uuid;
  v_protected_count integer;
  v_protected_separate boolean;
  v_eligible_ids jsonb;
  v_eligible_hash bytea;
  v_generation integer;
  v_applied integer:=0;
  v_expected integer;
  v_existing integer;
  v_qualification jsonb;
  v_charge jsonb;
  v_economic jsonb;
  v_effective_policy jsonb;
  v_invoice_rules jsonb;
  v_source_amount boolean;
  v_contract_settings_authority jsonb;
  v_canonical_economic jsonb;
  v_contract_rate_fingerprint bytea;
  v_effective_policy_fingerprint bytea;
  v_invoice_vat_policy_fingerprint bytea;
  v_calculation_fingerprint bytea;
  v_expense_policy_snapshot_hash bytea;
  v_source_mode text;
  v_row_sign smallint;
  v_invoice_vat_chargeable boolean;
  v_invoice_vat_rate_pct numeric;
  v_source_expense_vat_enabled boolean;
  v_source_authority boolean;
  v_has_worked_tuple boolean;
  v_expected_pay_pence bigint;
  v_expected_charge_pence bigint;
  v_result text;
  v_passed boolean;
  v_charge_source_pence bigint;
  v_charge_calculated_pence bigint;
  v_charge_difference_pence bigint;
  v_charge_row_sign_kind text;
  v_charge_claimed_result text;
  v_charge_expected_result text;
  v_charge_expected_sign_kind text;
  v_mode_a_policy jsonb;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_actor_user_id is null or p_publication_id is null
     or pg_catalog.jsonb_typeof(p_rows)<>'array' then
    raise exception 'WEEKLY_SOURCE_PROJECTION_INPUT_INVALID' using errcode='22023';
  end if;

  -- Read immutable identity first, then enter the same scope lock order as
  -- begin/publish before taking any row lock. Exact retries can therefore race
  -- safely without forming a publication-to-scope lock cycle.
  select * into v_publication
  from public.weekly_source_projection_publications
  where id=p_publication_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_NOT_FOUND' using errcode='22023';
  end if;
  select * into strict v_upload
  from public.weekly_source_uploads
  where id=v_publication.upload_id;
  select * into strict v_cycle
  from public.weekly_source_cycles
  where id=v_publication.source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  perform private.weekly_source_pay_query_admit_v2();
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      pg_catalog.encode(v_upload.declared_scope_fingerprint,'hex'),73241837
    )
  );

  if v_publication.authority_scope_kind='NHSP_REPORT_SCOPE' then
    select * into strict v_cycle
    from public.weekly_source_cycles
    where id=v_publication.source_cycle_id;
    select * into strict v_scope
    from public.weekly_source_report_scopes
    where id=v_publication.report_scope_id
    for update;
  else
    select * into strict v_cycle
    from public.weekly_source_cycles
    where id=v_publication.source_cycle_id
    for update;
  end if;
  select * into strict v_publication
  from public.weekly_source_projection_publications
  where id=p_publication_id
  for update;
  if v_publication.state not in ('BUILDING','CORRECTION_READY')
     or (v_publication.state='CORRECTION_READY'
         and v_publication.correction_session_id is null) then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_NOT_BUILDING' using errcode='55000';
  end if;
  select * into strict v_upload
  from public.weekly_source_uploads
  where id=v_publication.upload_id
  for share;
  select * into strict v_profile
  from public.weekly_source_format_profiles profile
  where profile.id=v_upload.source_format_profile_id and profile.active;

  if v_publication.authority_scope_kind='NHSP_REPORT_SCOPE' then
    if v_scope.version is distinct from v_publication.authority_scope_version
       or (
         v_publication.correction_session_id is null
         and (v_scope.current_complete_upload_id is distinct from v_upload.id
              or v_scope.projection_state<>'REBUILDING')
       ) then
      raise exception 'WEEKLY_SOURCE_PUBLICATION_STALE' using errcode='40001';
    end if;
  elsif v_cycle.version is distinct from v_publication.authority_scope_version
        or (
          v_publication.correction_session_id is null
          and (v_cycle.current_complete_upload_id is distinct from v_upload.id
               or v_cycle.projection_state<>'REBUILDING')
        ) then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_STALE' using errcode='40001';
  end if;

  if v_publication.correction_session_id is not null then
    select * into v_correction
    from public.weekly_final_source_correction_sessions
    where id=v_publication.correction_session_id for update;
    if not found
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from v_publication.authority_scope_kind
       or v_correction.report_scope_id is distinct from v_publication.report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.actor_user_id is distinct from p_actor_user_id
       or v_upload.purpose<>'FINAL_SOURCE_CORRECTION'
       or v_upload.state<>'CORRECTION_READY'
       or (v_publication.state='BUILDING'
         and v_publication.rebuild_idempotency_key is null and (
         v_correction.state<>'READY'
         or v_correction.replacement_projection_publication_id is not null
       ))
       or (v_publication.state='BUILDING'
         and v_publication.rebuild_idempotency_key is not null and (
         v_publication.projection_generation is null
         or v_publication.ready_session_version is null
         or v_correction.state<>'REVIEWED'
         or v_correction.version<>v_publication.ready_session_version
         or v_correction.replacement_projection_publication_id is null
         or v_correction.replacement_projection_publication_id=v_publication.id
       ))
       or (v_publication.state='CORRECTION_READY'
         and v_correction.replacement_projection_publication_id
               is distinct from v_publication.id)
       or coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
            is distinct from v_correction.expected_current_final_revision_id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_PUBLICATION_STALE' using errcode='40001';
    end if;
  elsif v_upload.purpose<>'ORDINARY' or v_upload.state<>'CURRENT' then
    raise exception 'WEEKLY_SOURCE_PUBLICATION_STALE' using errcode='40001';
  end if;

  perform private.weekly_source_office_authority_v1(
    p_actor_user_id,'RECHECK_SOURCE',v_group.id,
    case when v_publication.authority_scope_kind='NHSP_REPORT_SCOPE' then v_scope.client_id else null end,
    v_cycle.finalisation_week_ending
  );

  if v_publication.authority_scope_version>2147483647
     or coalesce(v_publication.projection_generation,0)>2147483647 then
    raise exception 'WEEKLY_SOURCE_PROJECTION_GENERATION_OVERFLOW' using errcode='22003';
  end if;
  v_generation:=coalesce(
    v_publication.projection_generation,
    v_publication.authority_scope_version::integer
  );

  select pg_catalog.count(*)::integer into v_expected
  from public.weekly_source_upload_rows source_row
  where source_row.upload_id=v_upload.id;
  if pg_catalog.jsonb_array_length(p_rows)<>v_expected then
    raise exception 'WEEKLY_SOURCE_PROJECTION_ROW_CENSUS_MISMATCH'
      using errcode='22023',detail=pg_catalog.jsonb_build_object(
        'expected',v_expected,'received',pg_catalog.jsonb_array_length(p_rows)
      )::text;
  end if;

  select pg_catalog.count(*)::integer into v_existing
  from public.weekly_source_row_resolutions resolution
  join public.weekly_source_upload_rows source_row
    on source_row.id=resolution.upload_row_id
  where source_row.upload_id=v_upload.id
    and resolution.generation=v_generation;
  if v_publication.state='CORRECTION_READY' or v_existing>0 then
    if v_existing<>v_expected or exists(
      select 1
      from public.weekly_source_upload_rows source_row
      where source_row.upload_id=v_upload.id
        and not exists(
          select 1
          from public.weekly_source_row_resolutions resolution
          where resolution.upload_row_id=source_row.id
            and resolution.generation=v_generation
            and resolution.source_row_fingerprint=source_row.normalised_row_hash
        )
    ) then
      raise exception 'WEEKLY_SOURCE_PROJECTION_RESOLUTION_CENSUS_INCOMPLETE'
        using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'publication_id',v_publication.id,'upload_id',v_upload.id,
      'authority_scope_version',v_publication.authority_scope_version,
      'generation',v_generation,'applied_row_count',v_existing,'idempotent',true
    );
  end if;

  for v_row_json in select value from pg_catalog.jsonb_array_elements(p_rows)
  loop
    if pg_catalog.jsonb_typeof(v_row_json)<>'object' then
      raise exception 'WEEKLY_SOURCE_PROJECTION_ROW_INVALID' using errcode='22023';
    end if;
    select * into strict v_source_row
    from public.weekly_source_upload_rows source_row
    where source_row.id=(v_row_json->>'upload_row_id')::uuid
      and source_row.upload_id=v_upload.id
    for share;

    if exists(
      select 1 from public.weekly_source_row_resolutions resolution
      where resolution.upload_row_id=v_source_row.id and resolution.generation=v_generation
    ) then
      raise exception 'WEEKLY_SOURCE_PROJECTION_ROW_ALREADY_APPLIED' using errcode='23505';
    end if;

    v_mapping_state:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_row_json->>'mapping_state','')));
    if v_mapping_state not in (
      'RESOLVED','CANDIDATE_NOT_FOUND','CLIENT_NOT_FOUND','NO_ELIGIBLE_CONTRACT',
      'AMBIGUOUS_CONTRACT','CONTRACT_SELECTION_REQUIRED','SOURCE_ROW_BLOCKED'
    ) then
      raise exception 'WEEKLY_SOURCE_MAPPING_STATE_INVALID' using errcode='22023';
    end if;
    v_candidate_id:=nullif(v_row_json->>'candidate_id','')::uuid;
    v_client_id:=nullif(v_row_json->>'client_id','')::uuid;
    v_contract_id:=nullif(v_row_json->>'contract_id','')::uuid;
    v_prior_work_event_id:=nullif(v_row_json->>'prior_work_event_id','')::uuid;
    v_selection_method:=nullif(pg_catalog.upper(pg_catalog.btrim(coalesce(v_row_json->>'contract_selection_method',''))),'');
    v_work_event_id:=null;
    v_link_kind:=null;
    v_work_event_match_kind:=null;
    v_work_event_match_fingerprint:=null;
    v_work_event_was_present:=false;
    v_compatible_count:=0;
    v_compatible_event_id:=null;
    v_economic:=v_row_json->'economic_snapshot';
    v_effective_policy:=null;
    v_invoice_rules:=null; v_source_amount:=false;
    v_contract_settings_authority:=null;
    v_canonical_economic:=null;
    v_contract_rate_fingerprint:=null;
    v_effective_policy_fingerprint:=null;
    v_invoice_vat_policy_fingerprint:=null;
    v_calculation_fingerprint:=null;
    v_expense_policy_snapshot_hash:=null;
    v_source_mode:=null;
    v_row_sign:=null;
    v_invoice_vat_chargeable:=null;
    v_invoice_vat_rate_pct:=null;
    v_source_expense_vat_enabled:=null;
    v_source_authority:=false;
    v_has_worked_tuple:=false;

    v_eligible_ids:=coalesce((
      select pg_catalog.jsonb_agg(contract_id order by contract_id)
      from (
        select distinct value as contract_id
        from pg_catalog.jsonb_array_elements_text(coalesce(v_row_json->'qualifying_contract_ids','[]'::jsonb))
      ) ordered
    ),'[]'::jsonb);
    v_eligible_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_QUALIFYING_CONTRACT_SET_V1',v_eligible_ids
    );

    if v_mapping_state='RESOLVED' then
      if v_candidate_id is null or v_client_id is null or v_contract_id is null
         or v_selection_method not in ('AUTO_UNIQUE','OFFICE_SELECTED','DURABLE_LINEAGE') then
        raise exception 'WEEKLY_SOURCE_RESOLVED_MAPPING_INCOMPLETE' using errcode='22023';
      end if;
      select * into strict v_contract
      from public.contracts contract
      where contract.id=v_contract_id
        and contract.candidate_id=v_candidate_id
        and contract.client_id=v_client_id
        and v_source_row.work_date between contract.start_date and coalesce(contract.end_date,'infinity'::date);
      if not (v_eligible_ids ? v_contract_id::text) then
        raise exception 'WEEKLY_SOURCE_SELECTED_CONTRACT_NOT_QUALIFIED' using errcode='22023';
      end if;
      -- G6-4: the selection method must be provable from server-derived
      -- facts, not asserted by the caller. Authority: 24 §8 ("If one Contract
      -- remains, the system selects it… The system must never offer a chooser
      -- when only one Contract is eligible"), 24 §9 (unique prior lineage),
      -- 25 §7, XSG-011, XSG-012.
      if v_selection_method='AUTO_UNIQUE'
         and pg_catalog.jsonb_array_length(v_eligible_ids)<>1 then
        raise exception 'WEEKLY_SOURCE_AUTO_UNIQUE_NOT_UNIQUE' using errcode='22023',
          detail=pg_catalog.jsonb_build_object(
            'source_row_ordinal',v_source_row.source_row_ordinal,
            'qualifying_contract_count',pg_catalog.jsonb_array_length(v_eligible_ids)
          )::text;
      end if;
      if v_selection_method='OFFICE_SELECTED'
         and pg_catalog.jsonb_array_length(v_eligible_ids)<2 then
        raise exception 'WEEKLY_SOURCE_OFFICE_CHOICE_NOT_WARRANTED' using errcode='22023',
          detail=pg_catalog.jsonb_build_object(
            'source_row_ordinal',v_source_row.source_row_ordinal,
            'qualifying_contract_count',pg_catalog.jsonb_array_length(v_eligible_ids)
          )::text;
      end if;
      if v_selection_method='DURABLE_LINEAGE' then
        if v_prior_work_event_id is null then
          raise exception 'WEEKLY_SOURCE_DURABLE_LINEAGE_UNPROVEN' using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_row_ordinal',v_source_row.source_row_ordinal,
              'reason','PRIOR_WORK_EVENT_ABSENT'
            )::text;
        end if;
        perform 1
        from public.weekly_work_events prior_event
        join public.weekly_source_row_resolutions prior_resolution
          on prior_resolution.work_event_id=prior_event.id
        where prior_event.id=v_prior_work_event_id
          and prior_event.candidate_id=v_candidate_id
          and prior_event.client_id=v_client_id
          and prior_event.first_source_group_id=v_group.id
          and prior_resolution.mapping_state='RESOLVED'
          and prior_resolution.contract_id=v_contract_id
          and (prior_resolution.upload_row_id<>v_source_row.id
            or (prior_resolution.generation<v_generation
              and prior_resolution.source_row_fingerprint=v_source_row.normalised_row_hash
              and exists(select 1 from public.weekly_source_projection_publications prior_publication
                where prior_publication.upload_id=v_upload.id
                  and coalesce(prior_publication.projection_generation,prior_publication.authority_scope_version)=prior_resolution.generation
                  and prior_publication.published_at_utc is not null
                  and prior_publication.state in ('CURRENT','STALE'))));
        if not found then
          raise exception 'WEEKLY_SOURCE_DURABLE_LINEAGE_UNPROVEN' using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_row_ordinal',v_source_row.source_row_ordinal,
              'reason','NO_PRIOR_RESOLVED_CONTRACT_FOR_WORK_EVENT'
            )::text;
        end if;
      end if;

      v_effective_policy:=private._weekly_source_effective_policy_v1(
        v_client_id,v_contract_id,v_source_row.work_date
      );
      v_effective_policy_fingerprint:=private.weekly_source_projection_hex32_v1(
        v_effective_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
      );
      if nullif(v_row_json->>'effective_policy_fingerprint','') is not null
         and private.weekly_source_projection_hex32_v1(
           v_row_json->>'effective_policy_fingerprint','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
         ) is distinct from v_effective_policy_fingerprint then
        raise exception 'WEEKLY_SOURCE_POLICY_STALE' using errcode='40001';
      end if;
      v_invoice_rules:=private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id)->'values';
      v_source_amount:=v_invoice_rules is not null and v_invoice_rules->>'invoice_sender'='CLIENT_SELF_BILL' and v_invoice_rules->>'invoice_amount_basis'='SOURCE_AMOUNT' and v_invoice_rules->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY';
      v_source_authority:=coalesce(v_effective_policy->>'authority_mode','')='SOURCE_AUTHORITY';
      v_source_mode:=nullif(v_effective_policy->>'c1_source_mode','');
      if v_source_authority and (
           coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) is not true
           or v_source_mode not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
         ) then
        raise exception 'WEEKLY_SOURCE_C1_MODE_UNRESOLVED' using errcode='55000';
      end if;
      if not v_source_authority and v_source_mode is not null then
        raise exception 'WEEKLY_SOURCE_TIMESHEET_AUTHORITY_C1_MODE_FORBIDDEN' using errcode='55000';
      end if;
      if (v_profile.profile_code in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1'))
           is distinct from (v_source_mode='NHSP_WEEKLY')
         and v_source_authority then
        raise exception 'WEEKLY_SOURCE_PROFILE_MODE_MISMATCH' using errcode='55000';
      end if;

      v_contract_settings_authority:=private._contract_settings_effective_core_v1(
        v_client_id,v_contract_id,v_source_row.work_date,'IMPORT',null
      );
      v_invoice_vat_chargeable:=coalesce(
        (v_contract_settings_authority#>>'{values,client_vat_chargeable}')::boolean,true
      );
      v_invoice_vat_rate_pct:=case when v_invoice_vat_chargeable then
        coalesce((v_contract_settings_authority#>>'{values,vat_rate_pct}')::numeric,20)
        else 0::numeric end;
      if v_invoice_vat_rate_pct<0 or v_invoice_vat_rate_pct>100 then
        raise exception 'WEEKLY_SOURCE_INVOICE_VAT_RATE_INVALID' using errcode='22023';
      end if;
      v_source_expense_vat_enabled:=coalesce(
        (v_effective_policy->>'source_expense_vat_enabled')::boolean,false
      );
      v_invoice_vat_policy_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_INVOICE_VAT_POLICY_V1',
        pg_catalog.jsonb_build_object(
          'client_id',v_client_id,'contract_id',v_contract_id,'work_date',v_source_row.work_date,
          'client_settings_id',v_contract_settings_authority->>'client_settings_id',
          'client_settings_effective_from',v_contract_settings_authority->>'client_settings_effective_from',
          'finance_settings_id',v_contract_settings_authority->>'finance_settings_id',
          'finance_settings_date_from',v_contract_settings_authority->>'finance_settings_date_from',
          'client_vat_chargeable',v_invoice_vat_chargeable,
          'vat_rate_pct',v_invoice_vat_rate_pct,
          'source_expense_vat_enabled',v_source_expense_vat_enabled,
          'settings_authority_fingerprint',v_contract_settings_authority->>'authority_fingerprint'
        )
      );

      -- G8-1/G8-2 (XSG-009).  `24 §14` and `25 §9`: a signed-Timesheet-authority
      -- row belongs to the established validation-only Mode A journey, never to
      -- source finalisation, and "merely labelling the row as Timesheet evidence
      -- is insufficient".  The unified upload owner therefore records the
      -- resolved Mode A authority as production data on the
      -- `weekly_timesheet_authority_resolutions` relation, which until now had
      -- no writer outside a verification fixture.
      -- `Reference required before pay` and `require_reference_to_invoice` come
      -- from the installed contract-settings authority and default to false;
      -- the auto-authorisation decision is taken from the established resolver
      -- `public.import_auto_authorise_policy_resolve_v2` in validation context,
      -- exactly as `public._import_review_auto_authorise_targets_core_v1` reads
      -- it, so no second policy engine is introduced.
      if not v_source_authority then
        if coalesce(v_effective_policy->>'document_mode','')<>'INVOICE_EVIDENCE_REQUIRED'
           or coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) then
          raise exception 'WEEKLY_SOURCE_TIMESHEET_AUTHORITY_POLICY_INVALID' using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'source_row_ordinal',v_source_row.source_row_ordinal,
              'document_mode',v_effective_policy->>'document_mode',
              'self_bill_enabled',v_effective_policy->>'self_bill_enabled'
            )::text;
        end if;
        v_mode_a_policy:=public.import_auto_authorise_policy_resolve_v2(
          case when v_profile.profile_code in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1')
            then 'NHSP'::public.hr_source_enum
            else 'HEALTHROSTER'::public.hr_source_enum end,
          v_client_id,v_contract_id,null,v_source_row.work_date,true
        );
        insert into public.weekly_timesheet_authority_resolutions(
          source_cycle_id,client_id,contract_id,work_date,authority_mode,document_mode,
          require_reference_to_pay,require_reference_to_invoice,auto_authorise_enabled,
          effective_policy_fingerprint
        ) values (
          v_cycle.id,v_client_id,v_contract_id,v_source_row.work_date,
          'TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',
          coalesce((v_contract_settings_authority#>>'{values,require_reference_to_pay}')::boolean,false),
          coalesce((v_contract_settings_authority#>>'{values,require_reference_to_invoice}')::boolean,false),
          coalesce((v_mode_a_policy->>'effective_value')::boolean,false),
          v_effective_policy_fingerprint
        ) on conflict (source_cycle_id,contract_id,work_date,effective_policy_fingerprint) do nothing;
      end if;

      v_has_worked_tuple:=v_source_row.start_at_local is not null
        and v_source_row.end_at_local is not null
        and v_source_row.break_minutes is not null
        and coalesce(v_source_row.actual_net_minutes,0)>0;

      v_identity_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_row_json->>'identity_kind','')));
      if v_identity_kind not in ('PROFILE_EXTERNAL_KEY','SCHEDULE_TUPLE') then
        raise exception 'WEEKLY_SOURCE_WORK_EVENT_IDENTITY_INVALID' using errcode='22023';
      end if;
      if v_profile.profile_code in (
        'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
        'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
        'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
      ) and v_identity_kind<>'PROFILE_EXTERNAL_KEY' then
        raise exception 'WEEKLY_SOURCE_PROFILE_EXTERNAL_EVENT_KEY_REQUIRED' using errcode='22023';
      end if;
      -- WP-58 / WP-50 finding F7, the mirror image of the guard above.
      -- Pack 24 section 1 supersedes "the NHSP Reference Number is the durable
      -- identity of the real shift", and 24 section 9 states the Reference
      -- Number is "not sole durable work identity": it, the ward and the
      -- location may all change during a correction. An NHSP shift is the same
      -- shift as another by the schedule tuple
      -- (private.weekly_source_work_event_schedule_compatible_v1), never by the
      -- reference. Reference-keyed identity on an NHSP row makes a reversal and
      -- its re-issue three separate work events for one corrected shift, which
      -- is what WP-50 executed. Until this guard existed, the only thing
      -- sending SCHEDULE_TUPLE for NHSP was one ternary in the broker
      -- (broker/src/weekly-source/upload-publication-owner.mjs), so the rule
      -- held on exactly one route. It is enforced here, where no caller can go
      -- round it.
      if v_profile.profile_code in (
        'NHSP_PREFINAL_RELEASED_V1',
        'NHSP_FINAL_BACKING_V1'
      ) and v_identity_kind<>'SCHEDULE_TUPLE' then
        raise exception 'WEEKLY_SOURCE_SCHEDULE_TUPLE_EVENT_KEY_REQUIRED' using errcode='22023';
      end if;
      v_profile_external_key:=case when v_identity_kind='PROFILE_EXTERNAL_KEY'
        then nullif(pg_catalog.btrim(coalesce(v_row_json->>'profile_external_key','')),'') end;
      if v_identity_kind='PROFILE_EXTERNAL_KEY'
         and (v_profile_external_key is null
              or v_profile_external_key is distinct from nullif(pg_catalog.btrim(v_source_row.external_source_key),'')) then
        raise exception 'WEEKLY_SOURCE_EXTERNAL_EVENT_KEY_REQUIRED' using errcode='22023';
      end if;

      if v_source_authority then
        v_protected_matches:=private.weekly_source_protected_match_candidates_v1(v_source_row.id,v_candidate_id,v_client_id);
        select choice.work_event_id,choice.separate_shift into v_protected_choice,v_protected_separate
          from private.weekly_source_office_row_choices choice
          where choice.upload_row_id=v_source_row.id and choice.candidate_id=v_candidate_id
            and choice.client_id=v_client_id and choice.contract_id=v_contract_id
          order by choice.id desc limit 1;
        if v_protected_choice is not null then
          if v_prior_work_event_id is distinct from v_protected_choice
            or not exists(select 1 from jsonb_array_elements(v_protected_matches) match
              where match->>'work_event_id'=v_protected_choice::text and match->>'contract_id'=v_contract_id::text) then
            raise exception 'WEEKLY_SOURCE_PROTECTED_MATCH_NOT_ELIGIBLE' using errcode='40001';
          end if;
        elsif not coalesce(v_protected_separate,false) then
          if exists(select 1 from jsonb_array_elements(v_protected_matches) match
            where match->>'contract_id'=v_contract_id::text and (match->>'retained_source_identity')::boolean) then
            select coalesce(jsonb_agg(match),'[]'::jsonb) into v_protected_matches
              from jsonb_array_elements(v_protected_matches) match
              where match->>'contract_id'=v_contract_id::text and (match->>'retained_source_identity')::boolean;
          elsif exists(select 1 from jsonb_array_elements(v_protected_matches) match
            where match->>'contract_id'=v_contract_id::text and (match->>'schedule_compatible')::boolean) then
            select coalesce(jsonb_agg(match),'[]'::jsonb) into v_protected_matches
              from jsonb_array_elements(v_protected_matches) match
              where match->>'contract_id'=v_contract_id::text and (match->>'schedule_compatible')::boolean;
          end if;
          select count(*) into v_protected_count from jsonb_array_elements(v_protected_matches) match
            where match->>'contract_id'=v_contract_id::text;
          if v_protected_count>0 and (v_protected_count<>1 or not exists(
            select 1 from jsonb_array_elements(v_protected_matches) match
            where match->>'contract_id'=v_contract_id::text
              and ((match->>'schedule_compatible')::boolean or (match->>'retained_source_identity')::boolean)
              and match->>'work_event_id'=v_prior_work_event_id::text)) then
            raise exception 'WEEKLY_SOURCE_PROTECTED_MATCH_REQUIRED' using errcode='55000';
          end if;
        end if;
      end if;
      if v_prior_work_event_id is not null then
        select * into strict v_work_event
        from public.weekly_work_events work_event
        where work_event.id=v_prior_work_event_id
          and work_event.candidate_id=v_candidate_id
          and work_event.client_id=v_client_id
          and work_event.first_source_group_id=v_group.id;
        if v_identity_kind='PROFILE_EXTERNAL_KEY'
           and exists(
             select 1
             from public.weekly_work_events other_event
             where other_event.first_source_group_id=v_group.id
               and other_event.source_format_profile_id=v_upload.source_format_profile_id
               and other_event.identity_kind='PROFILE_EXTERNAL_KEY'
               and other_event.profile_external_key=v_profile_external_key
               and other_event.id<>v_work_event.id
           ) then
          raise exception 'WEEKLY_SOURCE_PROFILE_EVENT_KEY_CONFLICT' using errcode='55000';
        end if;
        v_work_event_id:=v_work_event.id;
        v_work_event_match_kind:='EXACT_DURABLE_LINEAGE';
      else
        if v_identity_kind='PROFILE_EXTERNAL_KEY' then
          v_durable_identity_hash:=private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_WORK_EVENT_PROFILE_KEY_V1',
            pg_catalog.jsonb_build_object(
              'source_group_id',v_group.id,
              'source_format_profile_id',v_upload.source_format_profile_id,
              'profile_external_key',v_profile_external_key
            )
          );
        else
          -- WP-37 / `24 §9` step 2 and step 4.  The guarded set is read only
          -- after the family lock is held, so a concurrent publication cannot
          -- create a second root for the same work between this read and the
          -- insert below.
          perform pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended(
              v_group.id::text||'|'||v_upload.source_format_profile_id::text||'|'
                ||v_candidate_id::text||'|'||v_client_id::text||'|'
                ||v_source_row.work_date::text,
              17092026
            )
          );
          select pg_catalog.count(*)::integer,
                 pg_catalog.min(work_event.id::text)::uuid
            into v_compatible_count,v_compatible_event_id
          from public.weekly_work_events work_event
          where work_event.first_source_group_id=v_group.id
            and work_event.source_format_profile_id=v_upload.source_format_profile_id
            and work_event.identity_kind='SCHEDULE_TUPLE'
            and work_event.candidate_id=v_candidate_id
            and work_event.client_id=v_client_id
            and work_event.work_date=v_source_row.work_date
            and private.weekly_source_work_event_schedule_compatible_v1(
                  work_event.id,v_source_row.start_at_local,v_source_row.end_at_local
                );
          if v_compatible_count>1 then
            -- `24 §9`: "Office confirmation when more than one plausible
            -- relationship remains.  It never guesses."  No root is chosen.
            raise exception 'WEEKLY_SOURCE_WORK_EVENT_IDENTITY_AMBIGUOUS' using errcode='55000',
              detail=pg_catalog.jsonb_build_object(
                'source_row_ordinal',v_source_row.source_row_ordinal,
                'candidate_id',v_candidate_id,'client_id',v_client_id,
                'work_date',v_source_row.work_date,
                'compatible_work_event_count',v_compatible_count
              )::text;
          end if;
          -- The hash still carries this row's own schedule, so a genuinely
          -- different, non-overlapping shift on the same date gets its own
          -- durable identity.  It is a uniqueness token for a NEW root, not
          -- the identity rule: the rule is the compatible-schedule set above.
          v_durable_identity_hash:=private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_WORK_EVENT_SCHEDULE_TUPLE_V1',
            pg_catalog.jsonb_build_object(
              'source_group_id',v_group.id,'source_format_profile_id',v_upload.source_format_profile_id,
              'candidate_id',v_candidate_id,'client_id',v_client_id,'work_date',v_source_row.work_date,
              'start_at_local',v_source_row.start_at_local,'end_at_local',v_source_row.end_at_local
            )
          );
        end if;
        if v_identity_kind='SCHEDULE_TUPLE' and v_compatible_count=1 then
          select * into strict v_work_event
          from public.weekly_work_events work_event
          where work_event.id=v_compatible_event_id;
          v_work_event_was_present:=true;
        else
          select exists(
            select 1 from public.weekly_work_events work_event
            where work_event.durable_identity_hash=v_durable_identity_hash
          ) into v_work_event_was_present;
          insert into public.weekly_work_events(
            candidate_id,client_id,work_date,identity_kind,profile_external_key,
            durable_identity_hash,first_source_group_id,source_format_profile_id
          ) values (
            v_candidate_id,v_client_id,v_source_row.work_date,v_identity_kind,
            v_profile_external_key,v_durable_identity_hash,v_group.id,v_upload.source_format_profile_id
          ) on conflict (durable_identity_hash) do nothing;
          select * into strict v_work_event
          from public.weekly_work_events work_event
          where work_event.durable_identity_hash=v_durable_identity_hash;
        end if;
        if v_work_event.candidate_id is distinct from v_candidate_id
           or v_work_event.client_id is distinct from v_client_id
           or v_work_event.identity_kind is distinct from v_identity_kind
           or v_work_event.profile_external_key is distinct from v_profile_external_key
           or v_work_event.first_source_group_id is distinct from v_group.id
           or v_work_event.source_format_profile_id is distinct from v_upload.source_format_profile_id
           or (v_identity_kind='SCHEDULE_TUPLE' and v_work_event.work_date is distinct from v_source_row.work_date) then
          raise exception 'WEEKLY_SOURCE_WORK_EVENT_IDENTITY_COLLISION' using errcode='55000';
        end if;
        v_work_event_id:=v_work_event.id;
        v_work_event_match_kind:=case
          when v_identity_kind='PROFILE_EXTERNAL_KEY' and v_work_event_was_present then 'REUSED_PROFILE_KEY'
          when v_identity_kind='PROFILE_EXTERNAL_KEY' then 'NEW_PROFILE_KEY'
          when v_work_event_was_present then 'EXACT_DURABLE_LINEAGE'
          else 'NEW_SCHEDULE_TUPLE'
        end;
      end if;
      v_work_event_match_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_WORK_EVENT_MATCH_V1',
        pg_catalog.jsonb_build_object(
          'upload_row_id',v_source_row.id,'work_event_id',v_work_event_id,
          'match_kind',v_work_event_match_kind,'prior_work_event_id',v_prior_work_event_id,
          'source_row_fingerprint',pg_catalog.encode(v_source_row.normalised_row_hash,'hex')
        )
      );
    end if;

    -- Only a resolved, source-authoritative worked row owns a C1 economic
    -- snapshot.  Timesheet-authoritative validation rows, unfinished rows and
    -- explicit source-zero positions must never cross the pay boundary.
    if v_mapping_state='RESOLVED'
       and v_source_authority
       and v_source_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED') then
      if not v_has_worked_tuple then
        raise exception 'WEEKLY_SOURCE_WORKED_TUPLE_REQUIRED' using errcode='22023';
      end if;
      if pg_catalog.jsonb_typeof(v_economic)<>'object'
         or v_economic->>'schema_version' is distinct from 'WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1'
         or v_economic->>'calculator_version' is distinct from 'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1'
         or pg_catalog.upper(coalesce(v_economic->>'source_mode','')) is distinct from v_source_mode
         or pg_catalog.upper(coalesce(v_economic->>'rate_method',''))
              is distinct from v_effective_policy->>'weekly_rate_classification_method'
         or pg_catalog.jsonb_typeof(v_economic->'bucket_minutes')<>'object'
         or pg_catalog.jsonb_typeof(v_economic->'hours')<>'object'
         or pg_catalog.jsonb_typeof(v_economic->'pay_rates')<>'object'
         or pg_catalog.jsonb_typeof(v_economic->'charge_rates')<>'object' then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_INVALID' using errcode='22023';
      end if;

      -- A structurally valid NHSP £0 line is still a worked row.  Source money
      -- decides whether a physical row is a reversal only when it is negative;
      -- zero therefore keeps the positive worked-time/pay sign while the
      -- invoice movement retains the exact £0 source value.
      v_row_sign:=case when v_source_mode='NHSP_WEEKLY' then
        case when coalesce(v_source_row.source_shift_charge_pence,0)<0 then -1 else 1 end
        else 1 end;
      if v_row_sign not in (-1,1)
         or coalesce(v_economic->>'sign','')!~'^[-]?[0-9]+$'
         or (v_economic->>'sign')::smallint is distinct from v_row_sign
         or coalesce(v_economic->>'paid_minutes','')!~'^[0-9]+$'
         or (v_economic->>'paid_minutes')::integer is distinct from v_source_row.actual_net_minutes
         or coalesce(v_economic->>'break_minutes','')!~'^[0-9]+$'
         or (v_economic->>'break_minutes')::integer is distinct from v_source_row.break_minutes then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_SOURCE_TUPLE_MISMATCH' using errcode='22023';
      end if;

      if exists(
        select 1
        from (values ('day'),('night'),('sat'),('sun'),('bh')) bucket(name)
        where coalesce(v_economic#>>array['bucket_minutes',bucket.name],'')!~'^[0-9]+$'
           or coalesce(v_economic#>>array['hours',bucket.name],'')!~'^[+-]?[0-9]+([.][0-9]+)?$'
           or coalesce(v_economic#>>array['pay_rates',bucket.name],'')!~'^[+]?[0-9]+([.][0-9]+)?$'
           or coalesce(v_economic#>>array['charge_rates',bucket.name],'')!~'^[+]?[0-9]+([.][0-9]+)?$'
           or (v_economic#>>array['pay_rates',bucket.name])::numeric<=0
           or (v_economic#>>array['charge_rates',bucket.name])::numeric<=0
      ) then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_BUCKET_INVALID' using errcode='22023';
      end if;
      if (
        (v_economic#>>'{bucket_minutes,day}')::integer+
        (v_economic#>>'{bucket_minutes,night}')::integer+
        (v_economic#>>'{bucket_minutes,sat}')::integer+
        (v_economic#>>'{bucket_minutes,sun}')::integer+
        (v_economic#>>'{bucket_minutes,bh}')::integer
      ) is distinct from v_source_row.actual_net_minutes then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_MINUTES_MISMATCH' using errcode='22023';
      end if;
      if exists(
        select 1
        from (values ('day'),('night'),('sat'),('sun'),('bh')) bucket(name)
        where (v_economic#>>array['hours',bucket.name])::numeric is distinct from
          pg_catalog.round(
            ((v_economic#>>array['bucket_minutes',bucket.name])::numeric/60)*v_row_sign,2
          )
      ) then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_HOURS_MISMATCH' using errcode='22023';
      end if;
      if coalesce(v_economic->>'total_pay_pence','')!~'^[-]?[0-9]+$'
         or coalesce(v_economic->>'calculated_charge_pence','')!~'^[-]?[0-9]+$' then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_TOTAL_INVALID' using errcode='22023';
      end if;

      v_expected_pay_pence:=(pg_catalog.round(
        pg_catalog.round(
          pg_catalog.abs((v_economic#>>'{hours,day}')::numeric)*(v_economic#>>'{pay_rates,day}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,night}')::numeric)*(v_economic#>>'{pay_rates,night}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,sat}')::numeric)*(v_economic#>>'{pay_rates,sat}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,sun}')::numeric)*(v_economic#>>'{pay_rates,sun}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,bh}')::numeric)*(v_economic#>>'{pay_rates,bh}')::numeric,2
        )*100,0
      )::bigint)*v_row_sign;
      v_expected_charge_pence:=(pg_catalog.round(
        pg_catalog.round(
          pg_catalog.abs((v_economic#>>'{hours,day}')::numeric)*(v_economic#>>'{charge_rates,day}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,night}')::numeric)*(v_economic#>>'{charge_rates,night}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,sat}')::numeric)*(v_economic#>>'{charge_rates,sat}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,sun}')::numeric)*(v_economic#>>'{charge_rates,sun}')::numeric+
          pg_catalog.abs((v_economic#>>'{hours,bh}')::numeric)*(v_economic#>>'{charge_rates,bh}')::numeric,2
        )*100,0
      )::bigint)*v_row_sign;
      if (v_economic->>'total_pay_pence')::bigint is distinct from v_expected_pay_pence
         or (v_economic->>'calculated_charge_pence')::bigint is distinct from v_expected_charge_pence then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_TOTAL_MISMATCH' using errcode='22023';
      end if;

      v_contract_rate_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_CONTRACT_RATE_AUTHORITY_V1',
        pg_catalog.jsonb_build_object(
          'contract_id',v_contract.id,'contract_updated_at',v_contract.updated_at,
          'rates_json',v_contract.rates_json,'additional_rates_json',v_contract.additional_rates_json,
          'rate_method',v_effective_policy->>'weekly_rate_classification_method',
          'duration_break_tie_rule',v_effective_policy->>'duration_break_tie_rule',
          'canonical_pay_rates',v_economic->'pay_rates',
          'canonical_charge_rates',v_economic->'charge_rates',
          'settings_authority_fingerprint',v_contract_settings_authority->>'authority_fingerprint'
        )
      );
      v_canonical_economic:=pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode',v_source_mode,
        'rate_method',v_effective_policy->>'weekly_rate_classification_method',
        'sign',v_row_sign,
        'paid_minutes',v_source_row.actual_net_minutes,
        'break_minutes',v_source_row.break_minutes,
        'bucket_minutes',v_economic->'bucket_minutes',
        'hours',v_economic->'hours',
        'pay_rates',v_economic->'pay_rates',
        'charge_rates',v_economic->'charge_rates',
        'total_pay_pence',v_expected_pay_pence::text,
        'calculated_charge_pence',v_expected_charge_pence::text,
        'invoice_vat_chargeable',v_invoice_vat_chargeable,
        'invoice_vat_rate_pct',v_invoice_vat_rate_pct,
        'source_expense_vat_enabled',v_source_expense_vat_enabled,
        'contract_and_rate_fingerprint',pg_catalog.encode(v_contract_rate_fingerprint,'hex'),
        'effective_policy_fingerprint',pg_catalog.encode(v_effective_policy_fingerprint,'hex'),
        'invoice_vat_policy_fingerprint',pg_catalog.encode(v_invoice_vat_policy_fingerprint,'hex'),
        'break_allocation',v_economic->'break_allocation'
      );
      v_calculation_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',v_canonical_economic
      );
    else
      if pg_catalog.jsonb_typeof(v_economic)='object' then
        raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_NOT_APPLICABLE' using errcode='22023';
      end if;
      if v_mapping_state='RESOLVED' and v_source_authority
         and v_source_row.row_finalisation_state='SOURCE_ABSENT_ZERO'
         and coalesce(v_source_row.actual_net_minutes,-1)<>0 then
        raise exception 'WEEKLY_SOURCE_SOURCE_ZERO_INVALID' using errcode='22023';
      end if;
    end if;

    if v_mapping_state='RESOLVED' then
      v_link_kind:=case
        when not v_source_authority then 'TIMESHEET_EVIDENCE'
        when v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1'
          or v_source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'PROVISIONAL_SOURCE'
        when v_source_row.row_finalisation_state='SOURCE_ABSENT_ZERO' then 'ZERO_SOURCE'
        when v_source_mode='NHSP_WEEKLY' and v_row_sign=-1 then 'FULL_NEGATIVE_SOURCE'
        else 'POSITIVE_SOURCE'
      end;
      if nullif(pg_catalog.upper(pg_catalog.btrim(coalesce(v_row_json->>'link_kind',''))),'')
           is distinct from v_link_kind then
        raise exception 'WEEKLY_SOURCE_LINK_KIND_MISMATCH' using errcode='22023';
      end if;
    elsif nullif(pg_catalog.btrim(coalesce(v_row_json->>'link_kind','')),'') is not null then
      raise exception 'WEEKLY_SOURCE_LINK_NOT_APPLICABLE' using errcode='22023';
    end if;

    insert into public.weekly_source_row_resolutions(
      upload_row_id,generation,candidate_id,client_id,contract_id,work_event_id,
      paid_minutes,rate_classifications_json,mapping_state,blocker_code,
      contract_selection_method,work_event_match_kind,work_event_match_fingerprint,
      qualification_profile_fingerprint,
      qualifying_contract_count,qualifying_contract_set_hash,source_row_fingerprint,
      contract_and_rate_fingerprint,effective_policy_fingerprint
    ) values (
      v_source_row.id,v_generation,v_candidate_id,v_client_id,v_contract_id,v_work_event_id,
      v_source_row.actual_net_minutes,v_canonical_economic,
      v_mapping_state,nullif(v_row_json->>'blocker_code',''),v_selection_method,
      v_work_event_match_kind,v_work_event_match_fingerprint,
      private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_QUALIFICATION_PROFILE_V1',
        pg_catalog.jsonb_build_object(
          'upload_row_id',v_source_row.id,'generation',v_generation,
          'qualifying_contract_ids',v_eligible_ids,'mapping_state',v_mapping_state
        )
      ),
      pg_catalog.jsonb_array_length(v_eligible_ids),v_eligible_hash,
      v_source_row.normalised_row_hash,
      case when v_mapping_state='RESOLVED' then coalesce(
        v_contract_rate_fingerprint,
        private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_NON_ECONOMIC_CONTRACT_AUTHORITY_V1',
          pg_catalog.jsonb_build_object(
            'contract_id',v_contract.id,'contract_updated_at',v_contract.updated_at,
            'rates_json',v_contract.rates_json,'additional_rates_json',v_contract.additional_rates_json,
            'source_row_state',v_source_row.row_finalisation_state
          )
        )
      ) end,
      case when v_mapping_state='RESOLVED' then v_effective_policy_fingerprint end
    ) returning id into v_resolution_id;

    if v_canonical_economic is not null then
      insert into public.weekly_source_row_economic_snapshots(
        row_resolution_id,upload_row_id,generation,work_event_id,
        candidate_id,client_id,contract_id,calculator_version,source_mode,rate_method,row_sign,
        paid_minutes,break_minutes,minutes_day,minutes_night,minutes_sat,minutes_sun,minutes_bh,
        hours_day,hours_night,hours_sat,hours_sun,hours_bh,
        pay_day,pay_night,pay_sat,pay_sun,pay_bh,
        charge_day,charge_night,charge_sat,charge_sun,charge_bh,
        total_pay_pence,calculated_charge_pence,
        invoice_vat_chargeable,invoice_vat_rate_pct,source_expense_vat_enabled,
        canonical_result_json,contract_and_rate_fingerprint,effective_policy_fingerprint,
        invoice_vat_policy_fingerprint,calculation_fingerprint
      ) values (
        v_resolution_id,v_source_row.id,v_generation,v_work_event_id,
        v_candidate_id,v_client_id,v_contract_id,'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        v_source_mode,v_effective_policy->>'weekly_rate_classification_method',v_row_sign,
        v_source_row.actual_net_minutes,v_source_row.break_minutes,
        (v_economic#>>'{bucket_minutes,day}')::integer,
        (v_economic#>>'{bucket_minutes,night}')::integer,
        (v_economic#>>'{bucket_minutes,sat}')::integer,
        (v_economic#>>'{bucket_minutes,sun}')::integer,
        (v_economic#>>'{bucket_minutes,bh}')::integer,
        (v_economic#>>'{hours,day}')::numeric,
        (v_economic#>>'{hours,night}')::numeric,
        (v_economic#>>'{hours,sat}')::numeric,
        (v_economic#>>'{hours,sun}')::numeric,
        (v_economic#>>'{hours,bh}')::numeric,
        (v_economic#>>'{pay_rates,day}')::numeric,
        (v_economic#>>'{pay_rates,night}')::numeric,
        (v_economic#>>'{pay_rates,sat}')::numeric,
        (v_economic#>>'{pay_rates,sun}')::numeric,
        (v_economic#>>'{pay_rates,bh}')::numeric,
        (v_economic#>>'{charge_rates,day}')::numeric,
        (v_economic#>>'{charge_rates,night}')::numeric,
        (v_economic#>>'{charge_rates,sat}')::numeric,
        (v_economic#>>'{charge_rates,sun}')::numeric,
        (v_economic#>>'{charge_rates,bh}')::numeric,
        v_expected_pay_pence,v_expected_charge_pence,
        v_invoice_vat_chargeable,v_invoice_vat_rate_pct,v_source_expense_vat_enabled,
        v_canonical_economic,v_contract_rate_fingerprint,v_effective_policy_fingerprint,
        v_invoice_vat_policy_fingerprint,v_calculation_fingerprint
      );
    end if;

    if v_mapping_state='RESOLVED'
       and v_source_authority
       and v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
       and coalesce((v_effective_policy->>'source_fixed_expenses_enabled')::boolean,false) then
      if v_source_row.source_expense_pence is null
         or v_source_row.source_expense_parse_state not in ('VALID','OMITTED_ZERO')
         or (v_source_row.source_expense_parse_state='OMITTED_ZERO'
             and v_source_row.source_expense_pence<>0) then
        raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_INPUT_INVALID'
          using errcode='22023';
      end if;
      if v_source_row.source_expense_pence>0 and (
           v_source_row.start_at_local is null or v_source_row.end_at_local is null
           or v_source_row.end_at_local<=v_source_row.start_at_local
           or v_source_row.break_minutes is null
         ) then
        raise exception 'WEEKLY_SOURCE_EXPENSE_DISPLAY_TUPLE_REQUIRED'
          using errcode='22023';
      end if;
      v_expense_policy_snapshot_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_ROW_EXPENSE_POLICY_SNAPSHOT_V1',
        pg_catalog.jsonb_build_object(
          'row_resolution_id',v_resolution_id,'upload_row_id',v_source_row.id,
          'generation',v_generation,'work_event_id',v_work_event_id,
          'candidate_id',v_candidate_id,'client_id',v_client_id,
          'contract_id',v_contract_id,'row_finalisation_state',v_source_row.row_finalisation_state,
          'source_expense_pence',v_source_row.source_expense_pence,
          'source_expense_parse_state',v_source_row.source_expense_parse_state,
          'source_expense_vat_enabled',v_source_expense_vat_enabled,
          'invoice_vat_chargeable',v_invoice_vat_chargeable,
          'invoice_vat_rate_pct',v_invoice_vat_rate_pct,
          'correction_presentation',v_effective_policy->>'self_bill_correction_presentation',
          'effective_policy_fingerprint',pg_catalog.encode(v_effective_policy_fingerprint,'hex'),
          'invoice_vat_policy_fingerprint',pg_catalog.encode(v_invoice_vat_policy_fingerprint,'hex')
        )
      );
      insert into public.weekly_source_row_expense_policy_snapshots(
        row_resolution_id,upload_row_id,generation,work_event_id,
        candidate_id,client_id,contract_id,source_expense_pence,
        source_expense_parse_state,source_expense_vat_enabled,
        invoice_vat_chargeable,invoice_vat_rate_pct,correction_presentation,effective_policy_fingerprint,
        invoice_vat_policy_fingerprint,snapshot_hash
      ) values (
        v_resolution_id,v_source_row.id,v_generation,v_work_event_id,
        v_candidate_id,v_client_id,v_contract_id,v_source_row.source_expense_pence,
        v_source_row.source_expense_parse_state,v_source_expense_vat_enabled,
        v_invoice_vat_chargeable,v_invoice_vat_rate_pct,
        v_effective_policy->>'self_bill_correction_presentation',v_effective_policy_fingerprint,
        v_invoice_vat_policy_fingerprint,v_expense_policy_snapshot_hash
      );
    end if;

    if v_mapping_state='RESOLVED' then
      insert into public.weekly_work_event_source_links(
        work_event_id,upload_row_id,row_resolution_id,link_kind,link_hash
      ) values (
        v_work_event_id,v_source_row.id,v_resolution_id,
        v_link_kind,
        private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_WORK_EVENT_LINK_V1',
          pg_catalog.jsonb_build_object(
            'work_event_id',v_work_event_id,'upload_row_id',v_source_row.id,
            'resolution_id',v_resolution_id,'link_kind',v_link_kind
          )
        )
      );
    end if;

    for v_qualification in
      select value from pg_catalog.jsonb_array_elements(coalesce(v_row_json->'qualification_observations','[]'::jsonb))
    loop
      if v_profile.profile_code not in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1') then
        raise exception 'WEEKLY_SOURCE_QUALIFICATION_PROFILE_NOT_APPLICABLE' using errcode='22023';
      end if;
      v_result:=pg_catalog.upper(coalesce(v_qualification->>'comparison_result',''));
      -- G6-2/G6-4: a per-Contract qualification verdict is re-derived here
      -- from the immutable source pence and the caller's canonical pence for
      -- that Contract, using the same symmetric NHSP_TWO_COMPONENT_PENCE_V1
      -- rule (25 §6; 24 §8, §13; 14 §4.3 items 1 and 5; NHSP-BR-013). The
      -- eligible set that the selection method is proved against is therefore
      -- the set the server itself would admit. UNVERIFIABLE stays available
      -- as the fail-closed verdict and is never upgraded.
      v_charge_source_pence:=nullif(v_qualification->>'source_shift_charge_pence','')::bigint;
      v_charge_calculated_pence:=nullif(v_qualification->>'canonical_calculated_pence','')::bigint;
      if v_charge_source_pence is distinct from v_source_row.source_shift_charge_pence then
        raise exception 'WEEKLY_SOURCE_QUALIFICATION_SOURCE_PENCE_MISMATCH' using errcode='22023',
          detail=pg_catalog.jsonb_build_object(
            'source_row_ordinal',v_source_row.source_row_ordinal,
            'contract_id',v_qualification->>'contract_id'
          )::text;
      end if;
      v_charge_difference_pence:=case
        when v_charge_source_pence is null or v_charge_calculated_pence is null then null
        else v_charge_source_pence-v_charge_calculated_pence end;
      v_charge_expected_result:=case
        when v_charge_source_pence is null or v_charge_calculated_pence is null then 'UNVERIFIABLE'
        when v_charge_source_pence=0 and v_charge_calculated_pence<>0 then 'ZERO_SOURCE_CHARGE'
        when v_charge_calculated_pence=0 then 'MISMATCH'
        when v_charge_difference_pence=0 then 'EXACT'
        when (v_charge_difference_pence=1 or v_charge_difference_pence=-1)
          and ((v_charge_source_pence>0 and v_charge_calculated_pence>0)
            or (v_charge_source_pence<0 and v_charge_calculated_pence<0))
          then 'SOURCE_ROUNDING_EQUIVALENT'
        else 'MISMATCH' end;
      if v_result<>'UNVERIFIABLE' and v_result is distinct from v_charge_expected_result then
        raise exception 'WEEKLY_SOURCE_QUALIFICATION_RESULT_NOT_REDERIVED' using errcode='22023',
          detail=pg_catalog.jsonb_build_object(
            'source_row_ordinal',v_source_row.source_row_ordinal,
            'contract_id',v_qualification->>'contract_id',
            'claimed_result',v_result,'rederived_result',v_charge_expected_result,
            'signed_difference_pence',v_charge_difference_pence
          )::text;
      end if;
      -- Price does not decide Contract identity. Every safe canonical
      -- observation remains in the reliable base set; warning admission is a
      -- separate Office decision after Contract resolution.
      v_passed:=v_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','MISMATCH','ZERO_SOURCE_CHARGE');
      perform 1
      from public.contracts contract
      where contract.id=(v_qualification->>'contract_id')::uuid
        and contract.candidate_id=v_candidate_id
        and contract.client_id=v_client_id
        and v_source_row.work_date between contract.start_date and coalesce(contract.end_date,'infinity'::date)
        and v_eligible_ids ? contract.id::text;
      if not found then
        raise exception 'WEEKLY_SOURCE_QUALIFICATION_CONTRACT_OUT_OF_SCOPE' using errcode='22023';
      end if;
      insert into public.weekly_source_contract_qualification_observations(
        upload_row_id,generation,candidate_id,client_id,contract_id,
        source_format_profile_id,qualification_profile_version,
        qualification_profile_fingerprint,contract_revision_fingerprint,
        source_shift_charge_pence,canonical_calculated_pence,
        source_charge_difference_pence,comparison_result,qualification_passed,
        ordered_reason_codes,evidence_fingerprint
      ) values (
        v_source_row.id,v_generation,v_candidate_id,v_client_id,
        (v_qualification->>'contract_id')::uuid,v_upload.source_format_profile_id,
        'NHSP_TWO_COMPONENT_PENCE_V1',
        private.weekly_source_sha256_jsonb_v1('NHSP_QUALIFICATION_PROFILE_V1',v_qualification),
        private.weekly_source_projection_hex32_v1(
          v_qualification->>'contract_revision_fingerprint','WEEKLY_SOURCE_CONTRACT_REVISION_FINGERPRINT_INVALID'
        ),
        v_charge_source_pence,
        v_charge_calculated_pence,
        v_charge_difference_pence,
        v_result,v_passed,
        coalesce(array(select pg_catalog.jsonb_array_elements_text(v_qualification->'reason_codes')),'{}'::text[]),
        private.weekly_source_sha256_jsonb_v1('NHSP_CONTRACT_QUALIFICATION_EVIDENCE_V1',v_qualification)
      );
    end loop;

    v_charge:=v_row_json->'charge_check';
    if v_source_amount and v_calculation_fingerprint is not null and jsonb_typeof(v_charge) is distinct from 'object' then
      raise exception 'WEEKLY_SOURCE_INVOICE_SOURCE_PRICE_MISSING' using errcode='55000'; end if;
    if v_charge is not null and pg_catalog.jsonb_typeof(v_charge)='object' then
      if (v_profile.profile_code not in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1') and not coalesce(v_source_amount,false))
         or v_mapping_state<>'RESOLVED' then
        raise exception 'WEEKLY_SOURCE_CHARGE_CHECK_NOT_APPLICABLE' using errcode='22023';
      end if;
      if v_calculation_fingerprint is null then
        raise exception 'WEEKLY_SOURCE_CHARGE_FINGERPRINT_MISSING' using errcode='55000';
      end if;
      if nullif(v_charge->>'charge_calculation_fingerprint','') is not null
         and private.weekly_source_projection_hex32_v1(
               v_charge->>'charge_calculation_fingerprint',
               'WEEKLY_SOURCE_CHARGE_FINGERPRINT_INVALID'
             ) is distinct from v_calculation_fingerprint then
        raise exception 'WEEKLY_SOURCE_CHARGE_FINGERPRINT_MISMATCH' using errcode='22023';
      end if;
      if nullif(v_charge->>'source_commission_pence','')::bigint
            is distinct from v_source_row.source_commission_pence
         or nullif(v_charge->>'source_total_cost_pence','')::bigint
            is distinct from v_source_row.source_total_cost_pence
         or nullif(v_charge->>'source_shift_charge_pence','')::bigint
            is distinct from v_source_row.source_shift_charge_pence
         or nullif(v_charge->>'calculated_segment_charge_pence','')::bigint
            is distinct from v_expected_charge_pence then
        raise exception 'WEEKLY_SOURCE_CHARGE_CHECK_ECONOMIC_MISMATCH' using errcode='22023';
      end if;
      -- G6-2: server-side re-derivation of the NHSP_TWO_COMPONENT_PENCE_V1
      -- verdict. Authority: 25 §6; 24 §13; 14 §4.3 items 1 and 5;
      -- NHSP-BR-013; PRC-006, PRC-007, PRC-020, NHSBR-019, NHSBR-020.
      -- The four inputs are already anchored above to the immutable upload
      -- row and to this server's own canonical calculation, so the verdict
      -- is recomputed here from those facts. A caller may never upgrade a
      -- verdict: anything other than the fail-closed UNVERIFIABLE must equal
      -- the re-derived result, and the sign kind must equal the re-derived
      -- sign kind. EXACT is a zero difference. SOURCE_ROUNDING_EQUIVALENT is
      -- the same non-zero sign on the source and calculated pence with an
      -- absolute difference of exactly one penny in either arithmetic
      -- direction; no directional constraint is permitted.
      v_charge_source_pence:=v_source_row.source_shift_charge_pence;
      v_charge_calculated_pence:=v_expected_charge_pence;
      v_charge_difference_pence:=case
        when v_charge_source_pence is null or v_charge_calculated_pence is null then null
        else v_charge_source_pence-v_charge_calculated_pence end;
      v_charge_expected_sign_kind:=case
        when v_charge_source_pence is null then null
        when v_charge_source_pence>0 then 'POSITIVE'
        when v_charge_source_pence<0 then 'FULL_NEGATIVE'
        when v_charge_calculated_pence>0 then 'POSITIVE'
        when v_charge_calculated_pence<0 then 'FULL_NEGATIVE' end;
      if v_source_amount then
        if v_source_row.source_money_parse_state<>'VALID' or v_charge_source_pence is null or v_charge_source_pence<0 or v_charge_calculated_pence is null or v_charge_calculated_pence<=0 then
          raise exception 'WEEKLY_SOURCE_INVOICE_SOURCE_PRICE_INVALID' using errcode='55000'; end if;
        v_charge_expected_result:=case when v_charge_difference_pence=0 then 'EXACT' when abs(v_charge_difference_pence)=1 then 'SOURCE_ROUNDING_EQUIVALENT' else 'SOURCE_AMOUNT_DIFFERS' end;
        v_charge_expected_sign_kind:='POSITIVE';
      else
      v_charge_expected_result:=case
        when v_source_row.source_commission_pence is null
          or v_source_row.source_total_cost_pence is null
          or v_charge_source_pence is null
          or v_charge_calculated_pence is null then 'UNVERIFIABLE'
        when v_charge_source_pence=0 and v_charge_calculated_pence<>0 then 'ZERO_SOURCE_CHARGE'
        when v_charge_calculated_pence=0 then 'MISMATCH'
        when v_charge_difference_pence=0 then 'EXACT'
        when (v_charge_difference_pence=1 or v_charge_difference_pence=-1)
          and ((v_charge_source_pence>0 and v_charge_calculated_pence>0)
            or (v_charge_source_pence<0 and v_charge_calculated_pence<0))
          then 'SOURCE_ROUNDING_EQUIVALENT'
        else 'MISMATCH' end;
      end if;
      v_charge_claimed_result:=pg_catalog.upper(pg_catalog.btrim(
        coalesce(v_charge->>'comparison_result','')));
      v_charge_row_sign_kind:=pg_catalog.upper(pg_catalog.btrim(
        coalesce(v_charge->>'row_sign_kind','')));
      if (v_source_amount or v_charge_claimed_result<>'UNVERIFIABLE')
         and v_charge_claimed_result is distinct from v_charge_expected_result then
        raise exception 'WEEKLY_SOURCE_CHARGE_CHECK_RESULT_NOT_REDERIVED'
          using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_row_ordinal',v_source_row.source_row_ordinal,
              'claimed_result',v_charge_claimed_result,
              'rederived_result',v_charge_expected_result,
              'signed_difference_pence',v_charge_difference_pence
            )::text;
      end if;
      if v_charge_claimed_result<>'UNVERIFIABLE'
         and v_charge_row_sign_kind is distinct from v_charge_expected_sign_kind then
        raise exception 'WEEKLY_SOURCE_CHARGE_CHECK_SIGN_NOT_REDERIVED'
          using errcode='22023',
            detail=pg_catalog.jsonb_build_object(
              'source_row_ordinal',v_source_row.source_row_ordinal,
              'claimed_row_sign_kind',v_charge_row_sign_kind,
              'rederived_row_sign_kind',v_charge_expected_sign_kind
            )::text;
      end if;
      insert into public.weekly_source_charge_checks(
        upload_row_id,row_resolution_id,generation,row_sign_kind,
        source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
        calculated_segment_charge_pence,source_charge_difference_pence,
        comparison_profile_version,comparison_result,comparison_reason_code,
        phase_severity,blocker_code,charge_calculation_fingerprint
      ) values (
        v_source_row.id,v_resolution_id,v_generation,
        coalesce(v_charge_expected_sign_kind,v_charge_row_sign_kind),
        v_source_row.source_commission_pence,
        v_source_row.source_total_cost_pence,
        v_charge_source_pence,
        v_charge_calculated_pence,
        v_charge_difference_pence,
        case when v_source_amount then 'SOURCE_INVOICE_MONEY_PENCE_V1' else 'NHSP_TWO_COMPONENT_PENCE_V1' end,v_charge_claimed_result,
        coalesce(v_charge->>'comparison_reason_code',''),pg_catalog.upper(v_charge->>'phase_severity'),
        nullif(v_charge->>'blocker_code',''),
        v_calculation_fingerprint
      );
    end if;

    v_applied:=v_applied+1;
  end loop;

  if v_applied<>v_expected or exists(
    select 1 from public.weekly_source_upload_rows source_row
    where source_row.upload_id=v_upload.id
      and not exists(
        select 1 from public.weekly_source_row_resolutions resolution
        where resolution.upload_row_id=source_row.id and resolution.generation=v_generation
      )
  ) then
    raise exception 'WEEKLY_SOURCE_PROJECTION_RESOLUTION_CENSUS_INCOMPLETE' using errcode='55000';
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'publication_id',v_publication.id,'upload_id',v_upload.id,
    'authority_scope_version',v_publication.authority_scope_version,
    'generation',v_generation,'applied_row_count',v_applied
  );
exception
  when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_PROJECTION_VALUE_INVALID' using errcode='22023';
end;
$function$;

create or replace function public.weekly_source_charge_accept_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_invoice_rules jsonb;
  v_roster_source_amount boolean:=false;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_generation bigint;
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
     or v_publication.authority_scope_kind not in ('NHSP_REPORT_SCOPE','CYCLE') then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_PUBLICATION_STALE' using errcode='40001';
  end if;
  select * into strict v_upload from public.weekly_source_uploads where id=v_publication.upload_id;
  select * into strict v_profile from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
  -- Initial publications use the authority-scope version as their mapping
  -- generation; only a rebuilt publication stores an explicit generation.
  -- Match the existing projection builder/publisher, without changing either.
  v_generation:=coalesce(v_publication.projection_generation,v_publication.authority_scope_version);
  v_invoice_rules:=private.weekly_source_effective_rule_set_v1(nullif(v_upload.file_metadata_json->>'client_id','')::uuid,v_cycle_id)->'values';
  v_roster_source_amount:=v_publication.authority_scope_kind='CYCLE' and v_invoice_rules is not null and v_invoice_rules->>'invoice_sender'='CLIENT_SELF_BILL'
    and v_invoice_rules->>'invoice_amount_basis'='SOURCE_AMOUNT' and v_invoice_rules->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'
    and v_profile.final_authority_kind='HEALTHROSTER_ACTUAL_ROWS';
  if v_upload.state<>'CURRENT' or (not coalesce(v_roster_source_amount,false) and v_profile.profile_code<>'NHSP_FINAL_BACKING_V1')
     or v_generation is null then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_PUBLICATION_INVALID' using errcode='55000';
  end if;
  v_policy_fingerprint:=case when v_roster_source_amount then private.weekly_source_source_amount_acceptance_policy_fingerprint_v1() else private.weekly_source_charge_acceptance_policy_fingerprint_v1() end;

  -- The browser selects only the warning keys projected by the current
  -- workspace.  It never supplies charge-check ids or any financial value.
  -- The proof covers the complete current eligible set, so a stale workspace,
  -- Recheck, corrected upload or earlier partial acceptance fails closed.
  select coalesce(pg_catalog.array_agg(eligible.warning_key order by eligible.warning_key),'{}'::text[])
    into v_eligible_keys
  from (
    select 'charge-check:'||charge.id::text warning_key
    from public.weekly_source_charge_checks charge
    join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
    left join public.weekly_source_charge_acceptances acceptance
      on acceptance.charge_check_id=charge.id
    where source_row.upload_id=v_upload.id
      and charge.generation=v_generation
      and ((not v_roster_source_amount and charge.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE')) or (v_roster_source_amount and charge.comparison_result='SOURCE_AMOUNT_DIFFERS'))
      and charge.phase_severity=case when v_roster_source_amount then 'INFORMATION' else 'PROVISIONAL_WARNING' end
      and charge.blocker_code is null
      and acceptance.id is null
  ) eligible;
  v_workspace_version:=private.weekly_source_office_workspace_version_v1(v_cycle_id,v_publication.id);
  v_expected_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    case when v_roster_source_amount then 'SOURCE_AMOUNT_RATE_WARNING_SELECTION_V1' else 'NHSP_RATE_WARNING_SELECTION_V1' end,
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle_id,
      'projection_publication_id',v_publication.id,
      'projection_generation',v_generation,
      'workspace_version',v_workspace_version,
      'eligible_warning_keys',to_jsonb(v_eligible_keys)
    )
  ),'hex');
  if v_selection_proof<>v_expected_proof
     or not v_selected_keys<@v_eligible_keys then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_SELECTION_STALE' using errcode='40001';
  end if;

  select coalesce(pg_catalog.array_agg(charge.id order by charge.id),'{}'::uuid[]),
    pg_catalog.count(distinct 'charge-check:'||charge.id::text)::integer
    into v_ids,v_mapped_key_count
  from public.weekly_source_charge_checks charge
  join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
  left join public.weekly_source_charge_acceptances acceptance
    on acceptance.charge_check_id=charge.id
  where source_row.upload_id=v_upload.id
    and charge.generation=v_generation
    and ((not v_roster_source_amount and charge.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE')) or (v_roster_source_amount and charge.comparison_result='SOURCE_AMOUNT_DIFFERS'))
    and charge.phase_severity=case when v_roster_source_amount then 'INFORMATION' else 'PROVISIONAL_WARNING' end
    and charge.blocker_code is null
    and acceptance.id is null
    and ('charge-check:'||charge.id::text)=any(v_selected_keys);
  if coalesce(pg_catalog.array_length(v_ids,1),0)=0
     or v_mapped_key_count<>pg_catalog.array_length(v_selected_keys,1) then
    raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_SELECTION_STALE' using errcode='40001';
  end if;

  foreach v_check_id in array v_ids loop
    select * into v_check from public.weekly_source_charge_checks charge
    where charge.id=v_check_id for update;
    if not found or v_check.generation<>v_generation
       or (not v_roster_source_amount and v_check.comparison_result not in ('MISMATCH','ZERO_SOURCE_CHARGE'))
       or (v_roster_source_amount and (v_check.comparison_result<>'SOURCE_AMOUNT_DIFFERS' or v_check.comparison_profile_version<>'SOURCE_INVOICE_MONEY_PENCE_V1'))
       or v_check.phase_severity<>case when v_roster_source_amount then 'INFORMATION' else 'PROVISIONAL_WARNING' end
       or v_check.blocker_code is not null then
      raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_SELECTION_STALE' using errcode='40001';
    end if;
    select * into strict v_row from public.weekly_source_upload_rows source_row
      where source_row.id=v_check.upload_row_id and source_row.upload_id=v_upload.id;
    select * into strict v_resolution from public.weekly_source_row_resolutions resolution
      where resolution.id=v_check.row_resolution_id
        and resolution.upload_row_id=v_row.id
        and resolution.generation=v_generation;
    if v_resolution.mapping_state<>'RESOLVED' or v_resolution.contract_id is null
       or v_resolution.contract_selection_method is null then
      raise exception 'WEEKLY_SOURCE_CHARGE_ACCEPT_CONTRACT_UNRESOLVED' using errcode='55000';
    end if;
    select * into strict v_economic from public.weekly_source_row_economic_snapshots economic
      where economic.row_resolution_id=v_resolution.id
        and economic.upload_row_id=v_row.id
        and economic.generation=v_generation
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

alter function public.weekly_source_upload_context_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_upload_context_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_upload_context_v1(jsonb) to service_role;
alter function public.weekly_source_upload_stage_rows_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_upload_stage_rows_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_upload_stage_rows_atomic_v1(jsonb) to service_role;
alter function private.weekly_source_upload_seal_core_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_upload_seal_core_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb) to service_role;
alter function public.weekly_source_charge_accept_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_charge_accept_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_charge_accept_atomic_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
