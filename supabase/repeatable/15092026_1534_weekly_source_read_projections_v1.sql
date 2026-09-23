-- Repeatable CloudTMS authority: weekly_source_read_projections_v1
--
-- Server-owned Office read models and atomic group-selection actions for the
-- Weekly Source workspace. This authority owns presentation and outreach
-- selection only. It never writes Timesheet finance, invoices, Banking Pay,
-- Drafts, payment execution, cancellations, settlement or remittances.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_office_group_key_v1(
  p_source_cycle_id uuid,
  p_candidate_id uuid,
  p_client_id uuid,
  p_manager_recipient_route_key bytea
) returns text
language sql
immutable
strict
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select 'qg_'||pg_catalog.encode(
    private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_OFFICE_QUERY_GROUP_V1',
      pg_catalog.jsonb_build_object(
        'source_cycle_id',p_source_cycle_id,
        'candidate_id',p_candidate_id,
        'client_id',p_client_id,
        'manager_recipient_route_key',pg_catalog.encode(p_manager_recipient_route_key,'hex')
      )
    ),
    'hex'
  );
$function$;

create or replace function private.weekly_source_office_route_key_v1(
  p_source_cycle_id uuid,
  p_candidate_id uuid,
  p_client_id uuid,
  p_contract_id uuid,
  p_work_date date
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_policy jsonb;
  v_recipient text;
  v_route_key bytea;
begin
  select * into strict v_cycle from public.weekly_source_cycles
  where id=p_source_cycle_id;
  select * into strict v_group from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  v_policy:=private._weekly_source_effective_policy_v1(
    p_client_id,p_contract_id,p_work_date
  );
  if (v_policy->>'source_group_id')::uuid<>v_group.id then
    raise exception 'WEEKLY_SOURCE_QUERY_POLICY_GROUP_MISMATCH' using errcode='55000';
  end if;
  v_recipient:=case
    when coalesce((v_policy->>'manager_queries_enabled')::boolean,false)
      then private.weekly_source_query_normalise_recipient_v1(
        v_policy->>'manager_query_recipient'
      )
    else null
  end;
  v_route_key:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_MANAGER_RECIPIENT_ROUTE_V1',
    pg_catalog.jsonb_build_object(
      'environment',v_group.environment,
      'agency_id',v_group.agency_id,
      'source_cycle_id',v_cycle.id,
      'recipient',coalesce(v_recipient,'NO_MANAGER_ROUTE')
    )
  );
  return pg_catalog.jsonb_build_object(
    'route_key',pg_catalog.encode(v_route_key,'hex'),
    'group_key',private.weekly_source_office_group_key_v1(
      v_cycle.id,p_candidate_id,p_client_id,v_route_key
    ),
    'candidate_queries_enabled',coalesce(
      (v_policy->>'candidate_queries_enabled')::boolean,false
    ),
    'manager_queries_enabled',coalesce(
      (v_policy->>'manager_queries_enabled')::boolean,false
    ),
    'manager_recipient_present',v_recipient is not null,
    'authority_mode',v_policy->>'authority_mode',
    'document_mode',v_policy->>'document_mode',
    'source_fixed_expenses_enabled',coalesce(
      (v_policy->>'source_fixed_expenses_enabled')::boolean,false
    ),
    'source_family',v_group.source_family
  );
end;
$function$;

create or replace function private.weekly_source_office_missing_scope_fingerprint_v1(
  p_projection_publication_id uuid,
  p_candidate_id uuid,
  p_client_id uuid,
  p_contract_id uuid,
  p_week_ending date
) returns bytea
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  with publication as (
    select item.* from public.weekly_source_projection_publications item
    where item.id=p_projection_publication_id
  ), resolved as (
    select row.id source_row_id,row.normalised_row_hash,row.work_date,
      row.start_at_local,row.end_at_local,row.break_minutes,row.actual_net_minutes,
      row.row_finalisation_state,
      resolution.id row_resolution_id,resolution.candidate_id,resolution.client_id,
      resolution.contract_id,resolution.work_event_id,resolution.source_row_fingerprint,
      resolution.contract_and_rate_fingerprint,resolution.effective_policy_fingerprint
    from publication
    join public.weekly_source_upload_rows row on row.upload_id=publication.upload_id
    join lateral (
      select candidate.* from public.weekly_source_row_resolutions candidate
      where candidate.upload_row_id=row.id
      order by candidate.generation desc,candidate.id desc limit 1
    ) resolution on resolution.mapping_state='RESOLVED'
    where resolution.candidate_id=p_candidate_id
      and resolution.client_id=p_client_id
      and resolution.contract_id=p_contract_id
      and (date_trunc('week',row.work_date)::date+6)=p_week_ending
  ), canonical as (
    select pg_catalog.jsonb_build_object(
      'projection_publication_id',p_projection_publication_id,
      'candidate_id',p_candidate_id,
      'client_id',p_client_id,
      'contract_id',p_contract_id,
      'week_ending',p_week_ending,
      'source_rows',coalesce(pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'source_row_id',source_row_id,
          'normalised_row_hash',pg_catalog.encode(normalised_row_hash,'hex'),
          'row_resolution_id',row_resolution_id,
          'work_event_id',work_event_id,
          'contract_id',contract_id,
          'source_row_fingerprint',pg_catalog.encode(source_row_fingerprint,'hex'),
          'contract_and_rate_fingerprint',case when contract_and_rate_fingerprint is null then null
            else pg_catalog.encode(contract_and_rate_fingerprint,'hex') end,
          'effective_policy_fingerprint',case when effective_policy_fingerprint is null then null
            else pg_catalog.encode(effective_policy_fingerprint,'hex') end,
          'row_finalisation_state',row_finalisation_state,
          'work_date',work_date,
          'start_at_local',start_at_local,
          'end_at_local',end_at_local,
          'break_minutes',break_minutes,
          'actual_net_minutes',actual_net_minutes
        ) order by work_date,start_at_local,end_at_local,source_row_id
      ),'[]'::jsonb)
    ) value
    from resolved
  )
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_MISSING_TIMESHEET_SCOPE_V1',value
  ) from canonical;
$function$;

-- The single query-group relation is reused by reads and mutations. Group
-- identity is Candidate + actual Client + manager recipient. Missing signed
-- Timesheets are synthesised from the current publication rather than from a
-- discrepancy incident, because a missing Timesheet is its own request route.
create or replace function private.weekly_source_office_query_groups_v1(
  p_source_cycle_id uuid,
  p_projection_publication_id uuid,
  p_filters jsonb default '{}'::jsonb
) returns table(
  group_key text,
  candidate_id uuid,
  client_id uuid,
  manager_recipient_route_key bytea,
  candidate_name text,
  client_name text,
  incident_ids uuid[],
  accept_incident_ids uuid[],
  missing_scopes jsonb,
  children jsonb,
  issue_count integer,
  candidate_asked boolean,
  manager_informed boolean,
  outreach_eligible boolean,
  manager_eligible boolean,
  candidate_app_available boolean,
  first_seen_at_utc timestamptz,
  status_text text,
  status_tone text,
  issue_summary text
)
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  with cycle_context as (
    select cycle.id,cycle.source_group_id,cycle.finalisation_week_ending,
      source_group.environment,source_group.agency_id,source_group.source_family,
      publication.upload_id
    from public.weekly_source_cycles cycle
    join public.weekly_source_groups source_group on source_group.id=cycle.source_group_id
    join public.weekly_source_projection_publications publication
      on publication.id=p_projection_publication_id
     and publication.source_cycle_id=cycle.id
     and publication.state='CURRENT'
    where cycle.id=p_source_cycle_id
  ), incident_facts as (
    select incident.id incident_id,incident.candidate_id,incident.client_id,
      incident.candidate_action_state,incident.manager_action_state,
      incident.manager_potential_state,incident.created_at_utc,
      comparison.issue_family,comparison.contract_id,
      comparison.candidate_timesheet_id,comparison.source_presence,
      comparison.candidate_start_at_local,comparison.candidate_end_at_local,
      comparison.candidate_break_minutes,comparison.system_start_at_local,
      comparison.system_end_at_local,comparison.system_break_minutes,
      work_event.work_date,contract.role job_role,route_context.value route_context
    from cycle_context
    join public.weekly_discrepancy_incidents incident
      on incident.source_cycle_id=cycle_context.id and incident.state='OPEN'
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
     and comparison.projection_publication_id=p_projection_publication_id
    join public.weekly_work_events work_event on work_event.id=incident.work_event_id
    left join public.contracts contract on contract.id=comparison.contract_id
    cross join lateral (
      select private.weekly_source_office_route_key_v1(
        incident.source_cycle_id,incident.candidate_id,incident.client_id,
        comparison.contract_id,work_event.work_date
      ) value
    ) route_context
    where comparison.issue_family<>'CANDIDATE_TIMESHEET_MISSING'
  ), missing_rows as (
    select cycle_context.id source_cycle_id,
      resolution.candidate_id,resolution.client_id,resolution.contract_id,
      row.id source_row_id,row.normalised_row_hash,row.work_date,
      row.start_at_local,row.end_at_local,row.break_minutes,row.actual_net_minutes,
      row.row_finalisation_state,row.created_at_utc,resolution.id row_resolution_id,
      resolution.work_event_id,resolution.source_row_fingerprint,
      resolution.contract_and_rate_fingerprint,resolution.effective_policy_fingerprint,
      (date_trunc('week',row.work_date)::date+6) week_ending,
      coalesce(lineage.timesheet_id,root_contract_week.timesheet_id) timesheet_id,
      route_context.value route_context
    from cycle_context
    join public.weekly_source_upload_rows row on row.upload_id=cycle_context.upload_id
    join lateral (
      select candidate.* from public.weekly_source_row_resolutions candidate
      where candidate.upload_row_id=row.id
      order by candidate.generation desc,candidate.id desc limit 1
    ) resolution on resolution.mapping_state='RESOLVED'
    left join public.weekly_source_row_timesheet_lineages lineage
      on lineage.row_resolution_id=resolution.id
    left join lateral (
      select contract_week.timesheet_id
      from public.contract_weeks contract_week
      where contract_week.contract_id=resolution.contract_id
        and contract_week.week_ending_date=(date_trunc('week',row.work_date)::date+6)
        and contract_week.additional_seq=0
        and not contract_week.is_adjustment
        and contract_week.status<>'CANCELLED'::public.contract_week_status_enum
      order by contract_week.created_at desc,contract_week.id desc
      limit 1
    ) root_contract_week on true
    left join public.timesheets timesheet
      on timesheet.timesheet_id=coalesce(lineage.timesheet_id,root_contract_week.timesheet_id)
     and timesheet.is_current and timesheet.revoked_at is null
     and timesheet.archived_at_utc is null
     and timesheet.sheet_scope='WEEKLY' and timesheet.line_type='HOURS'
    cross join lateral (
      select private.weekly_source_office_route_key_v1(
        cycle_context.id,resolution.candidate_id,resolution.client_id,
        resolution.contract_id,row.work_date
      ) value
    ) route_context
    where row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED')
      and row.actual_net_minutes>0
      and route_context.value->>'authority_mode'='SOURCE_AUTHORITY'
      and route_context.value->>'document_mode'='CHECK_ONLY'
      and (timesheet.timesheet_id is null
        or timesheet.r2_nurse_key is null or timesheet.img_sha256_nurse is null)
  ), missing_scopes as (
    select missing_rows.source_cycle_id,missing_rows.candidate_id,
      missing_rows.client_id,missing_rows.contract_id,missing_rows.week_ending,
      min(missing_rows.work_date) first_work_date,
      (pg_catalog.array_agg(missing_rows.source_row_id order by
        missing_rows.work_date,missing_rows.start_at_local,
        missing_rows.end_at_local,missing_rows.source_row_id))[1] first_source_row_id,
      min(missing_rows.created_at_utc) first_seen_at_utc,
      (min(missing_rows.route_context->>'route_key'))::text route_key_hex,
      bool_and(coalesce((missing_rows.route_context->>'candidate_queries_enabled')::boolean,false))
        candidate_queries_enabled,
      private.weekly_source_office_missing_scope_fingerprint_v1(
        p_projection_publication_id,missing_rows.candidate_id,missing_rows.client_id,
        missing_rows.contract_id,missing_rows.week_ending
      ) expected_source_fingerprint,
      pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'incident_id',null,'row_key','missing-'||missing_rows.source_row_id::text,
        'day_date',to_char(missing_rows.work_date,'Dy FMDD Mon YYYY'),
        'job_role',coalesce(contract.role,''),
        'candidate_hours','Timesheet not submitted',
        'system_hours',case when missing_rows.start_at_local is null then pg_catalog.chr(8212)
          else to_char(missing_rows.start_at_local,'HH24:MI')||'-'||to_char(missing_rows.end_at_local,'HH24:MI') end,
        'issue','Timesheet missing',
        'status',pg_catalog.jsonb_build_object('text','Waiting for Timesheet','tone','warning'),
        'accept_eligible',false,'actions',pg_catalog.jsonb_build_array('View details')
      ) order by missing_rows.work_date,missing_rows.start_at_local,
        missing_rows.end_at_local,missing_rows.source_row_id) children
    from missing_rows
    join public.contracts contract on contract.id=missing_rows.contract_id
    group by missing_rows.source_cycle_id,missing_rows.candidate_id,
      missing_rows.client_id,missing_rows.contract_id,missing_rows.week_ending
  ), incident_grouped as (
    select (incident_facts.route_context->>'group_key')::text group_key,
      incident_facts.candidate_id,incident_facts.client_id,
      pg_catalog.decode(incident_facts.route_context->>'route_key','hex') manager_recipient_route_key,
      pg_catalog.array_agg(incident_facts.incident_id order by incident_facts.incident_id) incident_ids,
      coalesce(pg_catalog.array_agg(incident_facts.incident_id order by incident_facts.incident_id)
        filter (where incident_facts.route_context->>'authority_mode'='SOURCE_AUTHORITY'),'{}'::uuid[])
        accept_incident_ids,
      min(incident_facts.created_at_utc) first_seen_at_utc,
      bool_or(incident_facts.candidate_action_state in ('ASKED','RESPONDED')) candidate_asked,
      bool_or(incident_facts.manager_action_state in ('SENT','RESPONDED')) manager_informed,
      bool_and(coalesce((incident_facts.route_context->>'candidate_queries_enabled')::boolean,false))
        candidate_queries_enabled,
      bool_and(coalesce((incident_facts.route_context->>'manager_queries_enabled')::boolean,false)
        and coalesce((incident_facts.route_context->>'manager_recipient_present')::boolean,false))
        manager_queries_enabled,
      bool_and(incident_facts.candidate_timesheet_id is not null
        and incident_facts.manager_potential_state='AVAILABLE'
        and incident_facts.manager_action_state not in ('RESPONDED','NOT_REQUIRED')) manager_eligible,
      pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'incident_id',incident_facts.incident_id,
        'row_key','incident-'||incident_facts.incident_id::text,
        'day_date',to_char(incident_facts.work_date,'Dy FMDD Mon YYYY'),
        'job_role',coalesce(incident_facts.job_role,''),
        'candidate_hours',case when incident_facts.candidate_start_at_local is null then 'Timesheet not submitted'
          else to_char(incident_facts.candidate_start_at_local,'HH24:MI')||'-'||to_char(incident_facts.candidate_end_at_local,'HH24:MI')
            ||case when incident_facts.candidate_break_minutes is null then '' else ' ('||incident_facts.candidate_break_minutes||' min break)' end end,
        'system_hours',case when incident_facts.source_presence<>'PRESENT' then 'Not included'
          else to_char(incident_facts.system_start_at_local,'HH24:MI')||'-'||to_char(incident_facts.system_end_at_local,'HH24:MI')
            ||case when incident_facts.system_break_minutes is null then '' else ' ('||incident_facts.system_break_minutes||' min break)' end end,
        'issue',case incident_facts.issue_family
          when 'SOURCE_HOURS_DIFFER' then 'Hours differ'
          when 'SOURCE_MISSING_OR_NOT_AUTHORISED' then 'Missing or not yet authorised'
          when 'REFERENCE_MISSING' then 'Reference missing'
          when 'HEALTHROSTER_NOT_FINALISED' then 'Not finalised'
          else 'Needs review' end,
        'status',pg_catalog.jsonb_build_object(
          'text',case when incident_facts.manager_action_state='RESPONDED' then 'Manager responded'
            when incident_facts.manager_action_state='SENT' then 'Manager informed'
            when incident_facts.candidate_action_state='RESPONDED' then 'Candidate responded'
            when incident_facts.candidate_action_state='ASKED' then 'Candidate asked' else 'Needs action' end,
          'tone',case when incident_facts.candidate_action_state='NOT_ASKED' then 'warning' else 'info' end
        ),'accept_eligible',incident_facts.route_context->>'authority_mode'='SOURCE_AUTHORITY',
        'actions',pg_catalog.jsonb_build_array('View details')
      ) order by incident_facts.work_date,
        coalesce(incident_facts.system_start_at_local,incident_facts.candidate_start_at_local),
        incident_facts.incident_id) children,
      pg_catalog.string_agg(distinct case incident_facts.issue_family
        when 'SOURCE_HOURS_DIFFER' then 'Hours differ'
        when 'SOURCE_MISSING_OR_NOT_AUTHORISED' then 'Missing or not yet authorised'
        when 'REFERENCE_MISSING' then 'Reference missing'
        when 'HEALTHROSTER_NOT_FINALISED' then 'Not finalised'
        else 'Needs review' end,', ' order by case incident_facts.issue_family
        when 'SOURCE_HOURS_DIFFER' then 'Hours differ'
        when 'SOURCE_MISSING_OR_NOT_AUTHORISED' then 'Missing or not yet authorised'
        when 'REFERENCE_MISSING' then 'Reference missing'
        when 'HEALTHROSTER_NOT_FINALISED' then 'Not finalised'
        else 'Needs review' end) issue_summary
    from incident_facts
    group by incident_facts.route_context->>'group_key',incident_facts.candidate_id,
      incident_facts.client_id,incident_facts.route_context->>'route_key'
  ), missing_grouped as (
    select private.weekly_source_office_group_key_v1(
        scope.source_cycle_id,scope.candidate_id,scope.client_id,
        pg_catalog.decode(scope.route_key_hex,'hex')
      ) group_key,
      scope.candidate_id,scope.client_id,
      pg_catalog.decode(scope.route_key_hex,'hex') manager_recipient_route_key,
      pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'week_ending',scope.week_ending,'client_id',scope.client_id,
        'contract_id',scope.contract_id,
        'expected_source_fingerprint',pg_catalog.encode(scope.expected_source_fingerprint,'hex')
      ) order by scope.week_ending,scope.client_id,scope.contract_id) scopes,
      min(scope.first_seen_at_utc) first_seen_at_utc,
      bool_and(scope.candidate_queries_enabled) candidate_queries_enabled,
      pg_catalog.jsonb_agg(child.value order by scope.week_ending,scope.contract_id,child.ordinality) children
    from missing_scopes scope
    cross join lateral pg_catalog.jsonb_array_elements(scope.children) with ordinality child(value,ordinality)
    group by scope.source_cycle_id,scope.candidate_id,scope.client_id,scope.route_key_hex
  ), combined as (
    select coalesce(incident.group_key,missing.group_key) group_key,
      coalesce(incident.candidate_id,missing.candidate_id) candidate_id,
      coalesce(incident.client_id,missing.client_id) client_id,
      coalesce(incident.manager_recipient_route_key,missing.manager_recipient_route_key)
        manager_recipient_route_key,
      coalesce(incident.incident_ids,'{}'::uuid[]) incident_ids,
      coalesce(incident.accept_incident_ids,'{}'::uuid[]) accept_incident_ids,
      coalesce(missing.scopes,'[]'::jsonb) missing_scopes,
      coalesce(incident.children,'[]'::jsonb)||coalesce(missing.children,'[]'::jsonb) children,
      coalesce(pg_catalog.array_length(incident.incident_ids,1),0)
        +coalesce(pg_catalog.jsonb_array_length(missing.scopes),0) issue_count,
      coalesce(incident.candidate_asked,false) candidate_asked,
      coalesce(incident.manager_informed,false) manager_informed,
      coalesce(incident.candidate_queries_enabled,true)
        and coalesce(missing.candidate_queries_enabled,true) candidate_queries_enabled,
      coalesce(incident.manager_eligible,false)
        and pg_catalog.jsonb_array_length(coalesce(missing.scopes,'[]'::jsonb))=0 manager_eligible,
      least(incident.first_seen_at_utc,missing.first_seen_at_utc) first_seen_at_utc,
      coalesce(incident.issue_summary,'Timesheet missing') issue_summary
    from incident_grouped incident
    full join missing_grouped missing on missing.group_key=incident.group_key
  ), named as (
    select combined.*,coalesce(nullif(candidate.display_name,''),
        nullif(pg_catalog.concat_ws(' ',candidate.first_name,candidate.last_name),''),
        candidate.tms_ref,'Candidate') candidate_name,
      client.name client_name,context.environment,
      (select count(*)=1
       from public.candidate_app_global_membership_links membership
       join public.candidate_app_accounts account on account.id=membership.account_id
       where membership.candidate_id=combined.candidate_id
         and membership.state='ACTIVE' and account.status='ACTIVE') candidate_app_available
    from combined
    join public.candidates candidate on candidate.id=combined.candidate_id
    join public.clients client on client.id=combined.client_id
    cross join cycle_context context
  )
  select named.group_key,named.candidate_id,named.client_id,
    named.manager_recipient_route_key,named.candidate_name,named.client_name,
    named.incident_ids,named.accept_incident_ids,named.missing_scopes,named.children,named.issue_count,
    named.candidate_asked,named.manager_informed,
    named.candidate_queries_enabled and named.candidate_app_available outreach_eligible,
    named.manager_eligible,named.candidate_app_available,named.first_seen_at_utc,
    case when not named.candidate_app_available then 'Candidate app unavailable'
      when pg_catalog.jsonb_array_length(named.missing_scopes)>0 then 'Waiting for Timesheet'
      when named.manager_informed then 'Manager informed'
      when named.candidate_asked then 'Candidate asked' else 'Needs action' end status_text,
    case when not named.candidate_app_available then 'warning'
      when named.manager_informed or named.candidate_asked then 'info' else 'warning' end status_tone,
    named.issue_summary
  from named
  where coalesce(p_filters->>'status','UNRESOLVED') in ('UNRESOLVED','ALL')
    and (nullif(pg_catalog.btrim(coalesce(p_filters->>'candidate','')),'') is null
      or private.weekly_source_query_ascii_fold_v1(named.candidate_name)
        like '%'||private.weekly_source_query_ascii_fold_v1(p_filters->>'candidate')||'%')
    and (coalesce(p_filters->>'issue','ALL')='ALL'
      or (p_filters->>'issue'='TIMESHEET_MISSING' and pg_catalog.jsonb_array_length(named.missing_scopes)>0)
      or (p_filters->>'issue'='HOURS_DIFFER' and named.issue_summary like '%Hours differ%')
      or (p_filters->>'issue'='MISSING_SHIFT' and named.issue_summary like '%Missing%'));
$function$;

create or replace function private.weekly_source_office_workspace_version_v1(
  p_source_cycle_id uuid,
  p_projection_publication_id uuid
) returns text
language sql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_OFFICE_WORKSPACE_VERSION_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',cycle.id,'cycle_version',cycle.version,
      'cycle_state',cycle.state,'projection_state',cycle.projection_state,
      'projection_publication_id',publication.id,
      'authority_scope_version',publication.authority_scope_version,
      'issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex'),
      'groups',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'group_key',groups.group_key,'incident_ids',groups.incident_ids,
        'accept_incident_ids',groups.accept_incident_ids,
        'missing_scopes',groups.missing_scopes,'candidate_asked',groups.candidate_asked,
        'manager_informed',groups.manager_informed,
        'candidate_app_available',groups.candidate_app_available,
        'outreach_eligible',groups.outreach_eligible,'manager_eligible',groups.manager_eligible
      ) order by groups.group_key)
      from private.weekly_source_office_query_groups_v1(
        cycle.id,publication.id,'{}'::jsonb
      ) groups),'[]'::jsonb)
    )
  ),'hex')
  from public.weekly_source_cycles cycle
  join public.weekly_source_projection_publications publication
    on publication.id=p_projection_publication_id
   and publication.source_cycle_id=cycle.id and publication.state='CURRENT'
  where cycle.id=p_source_cycle_id;
$function$;

create or replace function public.weekly_source_office_workspace_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','tab','source_group_id','source_cycle_id','client_id',
    'report_scope_id','projection_publication_id','cursor','limit','sort_key',
    'sort_direction','status','candidate','issue','cycle_filter'
  ]::text[];
  v_unknown text;
  v_actor uuid;
  v_tab text;
  v_group_id uuid;
  v_cycle_id uuid;
  v_client_id uuid;
  v_report_scope_id uuid;
  v_publication_id uuid;
  v_cursor text;
  v_limit integer;
  v_offset integer:=0;
  v_sort_key text;
  v_sort_direction text;
  v_cycle_filter text;
  v_filters jsonb;
  v_group public.weekly_source_groups%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_guard jsonb;
  v_workspace_version text;
  v_imports jsonb;
  v_queries jsonb;
  v_ready jsonb;
  v_blocked jsonb;
  v_history jsonb;
  v_tracker jsonb;
  v_controls jsonb;
  v_query_count integer:=0;
  v_ask_eligible_count integer:=0;
  v_manager_eligible_count integer:=0;
  v_blocker_count integer:=0;
  v_paid_unresolved_count integer:=0;
  v_total integer:=0;
  v_rows jsonb:='[]'::jsonb;
  v_next_cursor text:='';
  v_cycle_label text;
  v_cycle_state_label text;
  v_cycle_state_tone text;
  v_finalise_payload jsonb:='{}'::jsonb;
  v_finalise_pay_follow_up jsonb;
  v_finalise_enabled boolean:=false;
  v_bulk_actions jsonb:='{}'::jsonb;
  v_all_group_keys jsonb:='[]'::jsonb;
  v_ask_selection_proof text;
  v_manager_selection_proof text;
  v_rate_warnings jsonb:='{}'::jsonb;
  v_rate_warning_rows jsonb:='[]'::jsonb;
  v_rate_warning_keys jsonb:='[]'::jsonb;
  v_rate_warning_selection_proof text;
  v_rate_warning_count integer:=0;
  v_rate_warning_accepted_count integer:=0;
  v_rate_warning_unaccepted_count integer:=0;
  v_rate_warning_zero_count integer:=0;
  v_rate_warning_disparity_count integer:=0;
  v_authority_mode text:='SOURCE_AUTHORITY';
  v_document_mode text:='CHECK_ONLY';
  v_import_journey jsonb:='{}'::jsonb;
  v_import_attention_rows jsonb:='[]'::jsonb;
  v_context_client_name text;
  v_context_cutoff timestamptz;
  v_nhsp_report_number text;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  if pg_catalog.jsonb_typeof(p_request->'actor_user_id')<>'string'
     or exists(
       select 1 from pg_catalog.jsonb_each(p_request) item
       where item.key in ('tab','source_group_id','source_cycle_id','client_id',
         'report_scope_id','projection_publication_id','cursor','sort_key',
         'sort_direction','status','candidate','issue','cycle_filter')
         and pg_catalog.jsonb_typeof(item.value) not in ('string','null')
     )
     or (p_request ? 'limit' and pg_catalog.jsonb_typeof(p_request->'limit') not in ('number','null')) then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_tab:=pg_catalog.lower(coalesce(nullif(pg_catalog.btrim(p_request->>'tab'),''),'imports'));
    v_group_id:=nullif(p_request->>'source_group_id','')::uuid;
    v_cycle_id:=nullif(p_request->>'source_cycle_id','')::uuid;
    v_client_id:=nullif(p_request->>'client_id','')::uuid;
    v_report_scope_id:=nullif(p_request->>'report_scope_id','')::uuid;
    v_publication_id:=nullif(p_request->>'projection_publication_id','')::uuid;
    v_limit:=coalesce(nullif(p_request->>'limit','')::integer,50);
  exception when others then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_tab not in ('imports','queries','finalise','history')
     or v_limit not between 1 and 100 then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';
  end if;
  v_sort_direction:=pg_catalog.lower(coalesce(nullif(pg_catalog.btrim(p_request->>'sort_direction'),''),
    case when v_tab='history' then 'desc' else 'asc' end));
  if v_sort_direction not in ('asc','desc') then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_SORT_INVALID' using errcode='22023';
  end if;
  v_sort_key:=coalesce(nullif(pg_catalog.lower(pg_catalog.btrim(p_request->>'sort_key')),''),
    case v_tab when 'imports' then 'uploaded' when 'history' then 'when' else 'candidate' end);
  if (v_tab='imports' and v_sort_key not in ('file','uploaded','rows','coverage','report','cutoff','status','final_source'))
     or (v_tab='queries' and v_sort_key not in ('candidate','client','issues','candidate_asked','manager_informed','status','age'))
     or (v_tab='finalise' and v_sort_key not in (
       'candidate','day_date','client','system_hours','actual_hours','movement',
       'commission','total_cost','invoice_charge','status','problem','job_role',
       'contract','outcome'
     ))
     or (v_tab='history' and v_sort_key not in ('when','source','event','by','detail')) then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_SORT_INVALID' using errcode='22023';
  end if;
  v_cycle_filter:=pg_catalog.upper(coalesce(nullif(pg_catalog.btrim(p_request->>'cycle_filter'),''),'CURRENT_PAY_CYCLE'));
  if v_cycle_filter not in ('CURRENT_PAY_CYCLE','LAST_4_PAY_CYCLES','LAST_13_PAY_CYCLES') then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_HISTORY_FILTER_INVALID' using errcode='22023';
  end if;
  v_filters:=pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
    'status',nullif(p_request->>'status',''),
    'candidate',nullif(p_request->>'candidate',''),
    'issue',nullif(p_request->>'issue','')
  ));

  if v_group_id is null and v_cycle_id is not null then
    select source_group_id into v_group_id from public.weekly_source_cycles where id=v_cycle_id;
  end if;
  if v_group_id is null then
    select source_group.id into v_group_id
    from public.weekly_source_groups source_group
    where source_group.active
    order by source_group.updated_at_utc desc,source_group.id limit 1;
  end if;
  select * into v_group from public.weekly_source_groups where id=v_group_id and active;
  if not found then raise exception 'WEEKLY_SOURCE_GROUP_NOT_ACTIVE' using errcode='22023'; end if;
  if v_cycle_id is null then
    select cycle.id into v_cycle_id from public.weekly_source_cycles cycle
    where cycle.source_group_id=v_group.id
    order by cycle.finalisation_week_ending desc,cycle.id desc limit 1;
  end if;
  select * into v_cycle from public.weekly_source_cycles
  where id=v_cycle_id and source_group_id=v_group.id;
  if not found then raise exception 'WEEKLY_SOURCE_CYCLE_NOT_FOUND' using errcode='22023'; end if;

  perform private.weekly_source_office_authority_v1(
    v_actor,'VIEW_SOURCE_PROGRESS',v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  if v_report_scope_id is not null then
    select scope.client_id into strict v_client_id
    from public.weekly_source_report_scopes scope
    where scope.id=v_report_scope_id and scope.source_cycle_id=v_cycle.id
      and (v_client_id is null or scope.client_id=v_client_id);
  end if;
  if v_client_id is not null and not exists(
    select 1 from public.weekly_source_group_clients membership
    where membership.source_group_id=v_group.id and membership.client_id=v_client_id
      and v_cycle.finalisation_week_ending between membership.valid_from
        and coalesce(membership.valid_to,'infinity'::date)
  ) then
    raise exception 'WEEKLY_SOURCE_CLIENT_NOT_IN_GROUP' using errcode='22023';
  end if;

  if v_client_id is not null then
    select policy.authority_mode,policy.document_mode
    into v_authority_mode,v_document_mode
    from public.weekly_source_client_policies policy
    where policy.source_group_id=v_group.id and policy.client_id=v_client_id
      and v_cycle.finalisation_week_ending between policy.effective_from
        and coalesce(policy.effective_to,'infinity'::date)
    order by policy.effective_from desc,policy.id desc limit 1;
    if not found then
      v_authority_mode:='SOURCE_AUTHORITY';
      v_document_mode:='CHECK_ONLY';
    end if;
  end if;

  if v_publication_id is null and v_report_scope_id is not null then
    select current_projection_publication_id into v_publication_id
    from public.weekly_source_report_scopes where id=v_report_scope_id;
  end if;
  if v_publication_id is null and v_client_id is not null and v_group.source_family='NHSP' then
    select scope.current_projection_publication_id,scope.id
    into v_publication_id,v_report_scope_id
    from public.weekly_source_report_scopes scope
    where scope.source_cycle_id=v_cycle.id and scope.client_id=v_client_id
    order by scope.cutoff_at_utc desc,scope.id desc limit 1;
  end if;
  if v_publication_id is null then
    v_publication_id:=v_cycle.current_projection_publication_id;
  end if;
  if v_publication_id is not null then
    v_guard:=private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication_id);
    select * into strict v_publication from public.weekly_source_projection_publications
    where id=v_publication_id;
    select * into strict v_upload from public.weekly_source_uploads where id=v_publication.upload_id;
    select profile.* into strict v_profile
    from public.weekly_source_format_profiles profile where profile.id=v_upload.source_format_profile_id;
    v_workspace_version:=private.weekly_source_office_workspace_version_v1(v_cycle.id,v_publication.id);
  else
    v_workspace_version:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_OFFICE_WORKSPACE_VERSION_V1',pg_catalog.jsonb_build_object(
        'source_cycle_id',v_cycle.id,'cycle_version',v_cycle.version,
        'cycle_state',v_cycle.state,'projection_state',v_cycle.projection_state,
        'completions',coalesce((select pg_catalog.jsonb_agg(pg_catalog.encode(completion.completion_hash,'hex')
          order by completion.client_id) from public.weekly_source_client_cycle_completions completion
          where completion.source_cycle_id=v_cycle.id and completion.state='CURRENT'),'[]'::jsonb)
      )),'hex');
  end if;

  v_cursor:=coalesce(nullif(p_request->>'cursor',''),'');
  if v_cursor<>'' then
    if split_part(v_cursor,':',1)<>v_workspace_version
       or split_part(v_cursor,':',2)!~ '^[0-9]+$' then
      raise exception 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE' using errcode='40001';
    end if;
    v_offset:=split_part(v_cursor,':',2)::integer;
  end if;

  if v_publication_id is not null then
    select pg_catalog.count(*)::integer,
      pg_catalog.count(*) filter (where query.outreach_eligible)::integer,
      pg_catalog.count(*) filter (where query.manager_eligible)::integer,
      coalesce(pg_catalog.jsonb_agg(query.group_key order by query.group_key),'[]'::jsonb)
    into v_query_count,v_ask_eligible_count,v_manager_eligible_count,v_all_group_keys
    from private.weekly_source_office_query_groups_v1(v_cycle.id,v_publication.id,v_filters) query;
    v_ask_selection_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_BULK_FILTER_SELECTION_V1',pg_catalog.jsonb_build_object(
        'action','ASK_CANDIDATES','source_cycle_id',v_cycle.id,
        'projection_publication_id',v_publication.id,
        'workspace_version',v_workspace_version,'filters',v_filters,
        'group_keys',v_all_group_keys
      )),'hex');
    v_manager_selection_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_BULK_FILTER_SELECTION_V1',pg_catalog.jsonb_build_object(
        'action','SEND_MANAGER_NOW','source_cycle_id',v_cycle.id,
        'projection_publication_id',v_publication.id,
        'workspace_version',v_workspace_version,'filters',v_filters,
        'group_keys',v_all_group_keys
      )),'hex');
    v_bulk_actions:=pg_catalog.jsonb_build_object(
      'contract','WEEKLY_SOURCE_BULK_FILTER_SELECTION_V1',
      'filtered_group_count',v_query_count,'selection_complete',true,
      'ask_candidates',pg_catalog.jsonb_build_object(
        'action','ASK_CANDIDATES','enabled',v_ask_eligible_count>0,
        'eligible_group_count',v_ask_eligible_count,
        'selection_proof',v_ask_selection_proof,
        'request',pg_catalog.jsonb_build_object(
          'actor_user_id',v_actor,'source_cycle_id',v_cycle.id,
          'projection_publication_id',v_publication.id,
          'expected_workspace_version',v_workspace_version,'action','ASK_CANDIDATES',
          'selection',pg_catalog.jsonb_build_object(
            'mode','ALL_FILTERED','group_keys','[]'::jsonb,
            'excluded_group_keys','[]'::jsonb,'incident_ids','[]'::jsonb,
            'filters',v_filters,'sort_key',v_sort_key,
            'sort_direction',v_sort_direction,'selection_proof',v_ask_selection_proof
          )
        )
      ),
      'send_manager_now',pg_catalog.jsonb_build_object(
        'action','SEND_MANAGER_NOW','enabled',v_manager_eligible_count>0,
        'eligible_group_count',v_manager_eligible_count,
        'selection_proof',v_manager_selection_proof,
        'request',pg_catalog.jsonb_build_object(
          'actor_user_id',v_actor,'source_cycle_id',v_cycle.id,
          'projection_publication_id',v_publication.id,
          'expected_workspace_version',v_workspace_version,'action','SEND_MANAGER_NOW',
          'selection',pg_catalog.jsonb_build_object(
            'mode','ALL_FILTERED','group_keys','[]'::jsonb,
            'excluded_group_keys','[]'::jsonb,'incident_ids','[]'::jsonb,
            'filters',v_filters,'sort_key',v_sort_key,
            'sort_direction',v_sort_direction,'selection_proof',v_manager_selection_proof
          )
        )
      )
    );
  end if;
  select pg_catalog.count(distinct family.id)::integer into v_paid_unresolved_count
  from public.weekly_exceptional_pay_target_families family
  join public.weekly_exceptional_payment_approvals approval on approval.pay_target_family_id=family.id
  where approval.source_cycle_id=v_cycle.id
    and family.current_lifecycle_state in ('PROTECTED','WAITING_SOURCE','READY_TO_RECONCILE','ACTION_REQUIRED');

  select pg_catalog.count(*)::integer into v_blocker_count
  from public.weekly_source_upload_rows source_row
  left join lateral (
    select resolution.* from public.weekly_source_row_resolutions resolution
    where resolution.upload_row_id=source_row.id
    order by resolution.generation desc,resolution.id desc limit 1
  ) resolution on true
  left join lateral (
    select charge.* from public.weekly_source_charge_checks charge
    where charge.upload_row_id=source_row.id
    order by charge.generation desc,charge.id desc limit 1
  ) charge on true
  where v_publication_id is not null and source_row.upload_id=v_upload.id
    and (resolution.mapping_state is distinct from 'RESOLVED'
      or source_row.row_finalisation_state in ('SOURCE_UNFINALISED','BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE')
      or charge.phase_severity='FINALISATION_BLOCKER');

  -- PHD-014..019 / PRC-043..050.  The final NHSP source value remains
  -- authoritative, but a known disparity or structurally valid zero charge is
  -- shown as one concise, server-owned warning decision.  The Trust-wide zero
  -- pattern is grouped into one row to avoid an alert storm.  No source money
  -- from this projection is ever an input to Candidate pay.
  if v_publication_id is not null
     and v_profile.profile_code in ('NHSP_PREFINAL_RELEASED_V1','NHSP_FINAL_BACKING_V1') then
    select pg_catalog.count(*)::integer,
      pg_catalog.count(acceptance.id)::integer,
      pg_catalog.count(*) filter (where acceptance.id is null)::integer,
      pg_catalog.count(*) filter (where charge.comparison_result='ZERO_SOURCE_CHARGE')::integer,
      pg_catalog.count(*) filter (where charge.comparison_result='MISMATCH')::integer
    into v_rate_warning_count,v_rate_warning_accepted_count,
      v_rate_warning_unaccepted_count,v_rate_warning_zero_count,
      v_rate_warning_disparity_count
    from public.weekly_source_charge_checks charge
    join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
    left join public.weekly_source_charge_acceptances acceptance
      on acceptance.charge_check_id=charge.id
    where source_row.upload_id=v_upload.id
      and charge.generation=v_publication.projection_generation
      and charge.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE')
      and charge.phase_severity='PROVISIONAL_WARNING'
      and charge.blocker_code is null;

    if v_rate_warning_count>0 then
      select coalesce(pg_catalog.jsonb_agg(warning.row_json order by warning.sort_order,warning.warning_key),'[]'::jsonb)
      into v_rate_warning_rows
      from (
        select 0 sort_order,'all-zero-source-charge'::text warning_key,
          pg_catalog.jsonb_build_object(
            'warning_key','all-zero-source-charge',
            'candidate',pg_catalog.count(distinct coalesce(resolution.candidate_id::text,source_row.source_candidate_identity))::text
              ||case when pg_catalog.count(distinct coalesce(resolution.candidate_id::text,source_row.source_candidate_identity))=1
                then ' affected candidate' else ' affected candidates' end,
            'day_date',case when pg_catalog.count(*)=1 then pg_catalog.min(to_char(source_row.work_date,'Dy FMDD Mon YYYY')) else 'Multiple shifts' end,
            'source_charge','£0.00','warning','Possible NHSP rate card issue','warning_tone','warning',
            'action_label',case when pg_catalog.count(*) filter (where acceptance.id is null)>0
              then 'View affected shifts' else 'Accepted' end,
            'accept_eligible',v_profile.profile_code='NHSP_FINAL_BACKING_V1'
              and pg_catalog.count(*) filter (where acceptance.id is null)>0,
            'detail_rows',coalesce((
              select pg_catalog.jsonb_agg(detail.row_json order by detail.work_date,detail.candidate_name,detail.charge_check_id)
              from (
                select zero_charge.id charge_check_id,zero_row.work_date,
                  coalesce(zero_candidate.display_name,zero_candidate.tms_ref,zero_row.source_candidate_identity,'Candidate') candidate_name,
                  pg_catalog.jsonb_build_object(
                    'candidate',coalesce(zero_candidate.display_name,zero_candidate.tms_ref,zero_row.source_candidate_identity,'Candidate'),
                    'day_date',to_char(zero_row.work_date,'Dy FMDD Mon YYYY'),
                    'source_charge','£0.00','warning','Possible NHSP rate card issue'
                  ) row_json
                from public.weekly_source_charge_checks zero_charge
                join public.weekly_source_upload_rows zero_row on zero_row.id=zero_charge.upload_row_id
                left join public.weekly_source_row_resolutions zero_resolution on zero_resolution.id=zero_charge.row_resolution_id
                left join public.candidates zero_candidate on zero_candidate.id=zero_resolution.candidate_id
                where zero_row.upload_id=v_upload.id
                  and zero_charge.generation=v_publication.projection_generation
                  and zero_charge.comparison_result='ZERO_SOURCE_CHARGE'
                  and zero_charge.phase_severity='PROVISIONAL_WARNING'
                  and zero_charge.blocker_code is null
                order by zero_row.work_date,candidate_name,zero_charge.id
                limit 100
              ) detail
            ),'[]'::jsonb)
          ) row_json
        from public.weekly_source_charge_checks charge
        join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
        left join public.weekly_source_row_resolutions resolution on resolution.id=charge.row_resolution_id
        left join public.weekly_source_charge_acceptances acceptance on acceptance.charge_check_id=charge.id
        where source_row.upload_id=v_upload.id
          and charge.generation=v_publication.projection_generation
          and charge.comparison_result='ZERO_SOURCE_CHARGE'
          and charge.phase_severity='PROVISIONAL_WARNING'
          and charge.blocker_code is null
        having pg_catalog.count(*)>0
        union all
        select 1 sort_order,'charge-check:'||charge.id::text warning_key,
          pg_catalog.jsonb_build_object(
            'warning_key','charge-check:'||charge.id::text,
            'candidate',coalesce(candidate.display_name,candidate.tms_ref,source_row.source_candidate_identity,'Candidate'),
            'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
            'source_charge',case when charge.source_shift_charge_pence<0 then '-£' else '£' end
              ||to_char(pg_catalog.abs(charge.source_shift_charge_pence)::numeric/100,'FM9999999990.00'),
            'warning','Rate card expired or wrong Contract rate','warning_tone','warning',
            'action_label',case when acceptance.id is null then 'Review rate warning' else 'Accepted' end,
            'accept_eligible',v_profile.profile_code='NHSP_FINAL_BACKING_V1' and acceptance.id is null,
            'detail_rows','[]'::jsonb
          ) row_json
        from public.weekly_source_charge_checks charge
        join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
        left join public.weekly_source_row_resolutions resolution on resolution.id=charge.row_resolution_id
        left join public.candidates candidate on candidate.id=resolution.candidate_id
        left join public.weekly_source_charge_acceptances acceptance on acceptance.charge_check_id=charge.id
        where source_row.upload_id=v_upload.id
          and charge.generation=v_publication.projection_generation
          and charge.comparison_result='MISMATCH'
          and charge.phase_severity='PROVISIONAL_WARNING'
          and charge.blocker_code is null
      ) warning;

      if v_profile.profile_code='NHSP_FINAL_BACKING_V1' and v_rate_warning_unaccepted_count>0 then
        select coalesce(pg_catalog.jsonb_agg(key_value order by key_value),'[]'::jsonb)
        into v_rate_warning_keys
        from (
          select 'all-zero-source-charge'::text key_value
          where exists(
            select 1 from public.weekly_source_charge_checks charge
            join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
            left join public.weekly_source_charge_acceptances acceptance on acceptance.charge_check_id=charge.id
            where source_row.upload_id=v_upload.id
              and charge.generation=v_publication.projection_generation
              and charge.comparison_result='ZERO_SOURCE_CHARGE'
              and charge.phase_severity='PROVISIONAL_WARNING'
              and charge.blocker_code is null and acceptance.id is null
          )
          union all
          select 'charge-check:'||charge.id::text
          from public.weekly_source_charge_checks charge
          join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id
          left join public.weekly_source_charge_acceptances acceptance on acceptance.charge_check_id=charge.id
          where source_row.upload_id=v_upload.id
            and charge.generation=v_publication.projection_generation
            and charge.comparison_result='MISMATCH'
            and charge.phase_severity='PROVISIONAL_WARNING'
            and charge.blocker_code is null and acceptance.id is null
        ) eligible;
        v_rate_warning_selection_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
          'NHSP_RATE_WARNING_SELECTION_V1',pg_catalog.jsonb_build_object(
            'source_cycle_id',v_cycle.id,'projection_publication_id',v_publication.id,
            'projection_generation',v_publication.projection_generation,
            'workspace_version',v_workspace_version,
            'eligible_warning_keys',v_rate_warning_keys
          )),'hex');
      end if;

      v_rate_warnings:=pg_catalog.jsonb_build_object(
        'contract','NHSP_RATE_WARNING_WORKSPACE_V1',
        'phase',case when v_profile.profile_code='NHSP_PREFINAL_RELEASED_V1' then 'PREFINAL'
          when v_rate_warning_unaccepted_count>0 then 'FINAL_AWAITING_ACCEPTANCE' else 'READY' end,
        'rows',v_rate_warning_rows,'total_count',v_rate_warning_count,
        'accepted_count',v_rate_warning_accepted_count,
        'ready_count',v_rate_warning_accepted_count,'hard_blocker_count',0,
        'notice',case when v_rate_warning_zero_count>1 then pg_catalog.jsonb_build_object(
          'title','Possible Trust rate card issue',
          'body',v_rate_warning_zero_count::text||' shifts have a £0 source charge. Check the Trust rate card in NHSP'
            ||case when v_profile.profile_code='NHSP_FINAL_BACKING_V1' then ' before accepting.' else '.' end
            ||case when v_rate_warning_disparity_count=1 then ' One other shift has a different source charge.'
              when v_rate_warning_disparity_count>1 then ' '||v_rate_warning_disparity_count::text||' other shifts have different source charges.' else '' end,
          'tone','warning') else null end,
        'acceptance',case when v_profile.profile_code='NHSP_FINAL_BACKING_V1'
            and v_rate_warning_unaccepted_count>0 then pg_catalog.jsonb_build_object(
          'enabled',true,'action','ACCEPT_NHSP_SOURCE_CHARGES',
          'payload',pg_catalog.jsonb_build_object(
            'source_cycle_id',v_cycle.id,'projection_publication_id',v_publication.id),
          'selection',pg_catalog.jsonb_build_object(
            'key','warning_keys','proof_key','selection_proof','proof',v_rate_warning_selection_proof)
        ) else null end
      );
    end if;
  end if;

  if v_tab='imports' then
    select pg_catalog.count(*)::integer into v_total
    from public.weekly_source_uploads upload where upload.source_cycle_id=v_cycle.id;
    select coalesce(pg_catalog.jsonb_agg(page.row_json order by page.position),'[]'::jsonb)
    into v_rows
    from (
      select pg_catalog.jsonb_build_object(
        'row_key',upload.id,'file',upload.original_filename,
        'uploaded',to_char(upload.uploaded_at_utc at time zone 'Europe/London','DD Mon YYYY HH24:MI'),
        'rows',upload.accepted_count,
        'coverage',case when upload.confirmed_coverage_start_local_date is null then 'Not confirmed'
          else to_char(upload.confirmed_coverage_start_local_date,'DD Mon YYYY')||' to '
            ||to_char(upload.confirmed_coverage_end_local_date,'DD Mon YYYY') end,
        'report',case when v_group.source_family='NHSP' then coalesce(
          nullif(pg_catalog.btrim(upload.file_metadata_json->>'nhsp_report_number'),''),'Not confirmed') else null end,
        'cutoff',case when v_group.source_family='NHSP' then coalesce(
          to_char(scope.cutoff_at_utc at time zone 'Europe/London','FMDD Mon YYYY · HH24:MI'),'Not confirmed') else null end,
        'status',pg_catalog.jsonb_build_object('text',case upload.state
          when 'CURRENT' then 'Current' when 'SUPERSEDED' then 'Superseded'
          when 'SEALED' then 'Ready' else initcap(pg_catalog.replace(upload.state,'_',' ')) end,
          'tone',case when upload.state='CURRENT' then 'positive'
            when upload.state in ('REJECTED') then 'danger' else 'neutral' end),
        'final_source',case when exists(select 1 from public.weekly_source_final_revisions revision
          where revision.upload_id=upload.id and revision.state='CURRENT') then 'Finalised' else pg_catalog.chr(8212) end,
        'actions',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'label','View','enabled',true,'payload',pg_catalog.jsonb_build_object('upload_id',upload.id)
        ))
      ) row_json,
      pg_catalog.row_number() over(order by
        case when v_sort_key='file' and v_sort_direction='asc' then upload.original_filename end asc,
        case when v_sort_key='file' and v_sort_direction='desc' then upload.original_filename end desc,
        case when v_sort_key='rows' and v_sort_direction='asc' then upload.accepted_count end asc,
        case when v_sort_key='rows' and v_sort_direction='desc' then upload.accepted_count end desc,
        case when v_sort_key='report' and v_sort_direction='asc' then upload.file_metadata_json->>'nhsp_report_number' end asc nulls last,
        case when v_sort_key='report' and v_sort_direction='desc' then upload.file_metadata_json->>'nhsp_report_number' end desc nulls last,
        case when v_sort_key='cutoff' and v_sort_direction='asc' then scope.cutoff_at_utc end asc nulls last,
        case when v_sort_key='cutoff' and v_sort_direction='desc' then scope.cutoff_at_utc end desc nulls last,
        case when v_sort_direction='asc' then upload.uploaded_at_utc end asc,
        case when v_sort_direction='desc' then upload.uploaded_at_utc end desc,
        upload.id
      ) position
      from public.weekly_source_uploads upload
      left join public.weekly_source_report_scopes scope on scope.id=upload.report_scope_id
      where upload.source_cycle_id=v_cycle.id
      order by
        case when v_sort_key='file' and v_sort_direction='asc' then upload.original_filename end asc,
        case when v_sort_key='file' and v_sort_direction='desc' then upload.original_filename end desc,
        case when v_sort_key='rows' and v_sort_direction='asc' then upload.accepted_count end asc,
        case when v_sort_key='rows' and v_sort_direction='desc' then upload.accepted_count end desc,
        case when v_sort_key='report' and v_sort_direction='asc' then upload.file_metadata_json->>'nhsp_report_number' end asc nulls last,
        case when v_sort_key='report' and v_sort_direction='desc' then upload.file_metadata_json->>'nhsp_report_number' end desc nulls last,
        case when v_sort_key='cutoff' and v_sort_direction='asc' then scope.cutoff_at_utc end asc nulls last,
        case when v_sort_key='cutoff' and v_sort_direction='desc' then scope.cutoff_at_utc end desc nulls last,
        case when v_sort_direction='asc' then upload.uploaded_at_utc end asc,
        case when v_sort_direction='desc' then upload.uploaded_at_utc end desc,
        upload.id
      offset v_offset limit v_limit
    ) page;
    v_next_cursor:=case when v_offset+v_limit<v_total
      then v_workspace_version||':'||(v_offset+v_limit)::text else '' end;
    v_imports:=pg_catalog.jsonb_build_object('rows',v_rows,'total_count',v_total,
      'next_cursor',v_next_cursor,'has_more',v_next_cursor<>'','record_version',v_workspace_version,'stale',false);
  else
    v_imports:=pg_catalog.jsonb_build_object('rows','[]'::jsonb,'total_count',
      (select pg_catalog.count(*) from public.weekly_source_uploads upload where upload.source_cycle_id=v_cycle.id),
      'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);
  end if;

  if v_authority_mode='TIMESHEET_AUTHORITY' and v_publication_id is not null then
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','authority-'||comparison.id::text,
      'candidate',coalesce(candidate.display_name,candidate.tms_ref,'Candidate'),
      'day_date',to_char(comparison.work_date,'Dy FMDD Mon YYYY'),
      'attention',case comparison.comparison_state
        when 'HOURS_MISMATCH' then 'Hours are different'
        when 'SOURCE_SHIFT_MISSING' then 'Shift is missing or not yet authorised'
        when 'REFERENCE_MISSING' then 'Reference is missing'
        when 'AMBIGUOUS_SOURCE_ROW' then 'More than one source shift matches'
        else 'Check this Timesheet' end,
      'reference',case when nullif(pg_catalog.btrim(comparison.source_reference_number),'') is null
        then 'Not added' else comparison.source_reference_number end,
      'status',pg_catalog.jsonb_build_object(
        'text',case when comparison.comparison_state='REFERENCE_MISSING'
          then 'Reference needed' else 'Manager correction needed' end,
        'tone','warning'),
      'actions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('label','View Timesheet','enabled',true,
          'payload',pg_catalog.jsonb_build_object('timesheet_id',comparison.timesheet_id)),
        pg_catalog.jsonb_build_object('label','Email manager','enabled',true,
          'payload',pg_catalog.jsonb_build_object('timesheet_id',comparison.timesheet_id,
            'comparison_id',comparison.id)))
    ) order by private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,candidate.tms_ref,'')) collate "C",
      comparison.work_date,comparison.id),'[]'::jsonb)
    into v_import_attention_rows
    from public.weekly_timesheet_source_comparisons comparison
    join public.timesheets timesheet_row on timesheet_row.timesheet_id=comparison.timesheet_id
      and timesheet_row.is_current
    left join public.candidates candidate on candidate.id=timesheet_row.candidate_id
    where comparison.projection_publication_id=v_publication_id
      and comparison.comparison_state<>'EXACT_MATCH';
  end if;

  v_import_journey:=pg_catalog.jsonb_build_object(
    'authority_mode',v_authority_mode,
    'document_mode',v_document_mode,
    'title',case when v_authority_mode='TIMESHEET_AUTHORITY'
      then 'Signed Timesheet decides hours' else 'Client system decides hours' end,
    'body',case when v_authority_mode='TIMESHEET_AUTHORITY'
      then 'Timesheet hours are used. The client system is checked so matching references can be added.'
      else 'Client system hours are used after the source is finalised.' end,
    'attention_rows',v_import_attention_rows,
    'attention_count',pg_catalog.jsonb_array_length(v_import_attention_rows)
  );
  v_imports:=v_imports||pg_catalog.jsonb_build_object('journey',v_import_journey);

  if v_tab='queries' and v_publication_id is not null then
    v_total:=v_query_count;
    select coalesce(pg_catalog.jsonb_agg(page.row_json order by page.position),'[]'::jsonb)
    into v_rows
    from (
      select pg_catalog.jsonb_build_object(
        'row_key',query.group_key,'group_key',query.group_key,
        'candidate',query.candidate_name,'client',query.client_name,'issues',query.issue_count,
        'candidate_asked',query.candidate_asked,'manager_informed',query.manager_informed,
        'status',pg_catalog.jsonb_build_object('text',query.status_text,'tone',query.status_tone),
        'age',case when query.first_seen_at_utc is null then pg_catalog.chr(8212)
          when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '1 hour' then 'Less than 1 hour'
          when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '24 hours'
            then floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/3600)::integer||' hours'
          else floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/86400)::integer||' days' end,
        'outreach_eligible',query.outreach_eligible,
        'manager_eligible',query.manager_eligible,
        'candidate_app_available',query.candidate_app_available,
        'children',coalesce((
          select pg_catalog.jsonb_agg(
            (child.value-'actions')||pg_catalog.jsonb_build_object(
              'actions',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
                'label','View details','enabled',true,
                'payload',pg_catalog.jsonb_build_object('detail',pg_catalog.jsonb_build_object(
                  'candidate',query.candidate_name,'client',query.client_name,
                  'status',child.value#>>'{status,text}',
                  'age',case when query.first_seen_at_utc is null then pg_catalog.chr(8212)
                    when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '1 hour' then 'Less than 1 hour'
                    when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '24 hours'
                      then floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/3600)::integer||' hours'
                    else floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/86400)::integer||' days' end,
                  'shifts',pg_catalog.jsonb_build_array(child.value-'actions')
                ))
              ))
            ) order by child.ordinality
          )
          from pg_catalog.jsonb_array_elements(query.children) with ordinality child(value,ordinality)
        ),'[]'::jsonb),
        'actions',pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object(
            'label','Open','enabled',true,
            'payload',pg_catalog.jsonb_build_object(
              'group_key',query.group_key,
              'detail',pg_catalog.jsonb_build_object(
                'candidate',query.candidate_name,'client',query.client_name,
                'status',query.status_text,
                'age',case when query.first_seen_at_utc is null then pg_catalog.chr(8212)
                  when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '1 hour' then 'Less than 1 hour'
                  when pg_catalog.transaction_timestamp()-query.first_seen_at_utc<interval '24 hours'
                    then floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/3600)::integer||' hours'
                  else floor(extract(epoch from (pg_catalog.transaction_timestamp()-query.first_seen_at_utc))/86400)::integer||' days' end,
                'shifts',query.children
              )
            )
          ),
          pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
            'label','Remind candidate','kind','COMMAND','command','REMIND_CANDIDATE',
            'enabled',candidate_reminder.candidate_generation_id is not null
              and (candidate_reminder.manual_reminder_available_at_utc is null
                or pg_catalog.transaction_timestamp()>=candidate_reminder.manual_reminder_available_at_utc),
            'reason',case
              when candidate_reminder.candidate_generation_id is null then 'Ask the candidate before sending a reminder.'
              when candidate_reminder.manual_reminder_available_at_utc is not null
               and pg_catalog.transaction_timestamp()<candidate_reminder.manual_reminder_available_at_utc
                then 'A reminder can be sent after '
                  ||to_char(candidate_reminder.manual_reminder_available_at_utc at time zone 'Europe/London','Dy FMDD Mon YYYY HH24:MI')||'.'
              else null end,
            'available_at_utc',candidate_reminder.manual_reminder_available_at_utc,
            'context',pg_catalog.jsonb_build_object(
              'candidate',query.candidate_name,'client',query.client_name,'status',query.status_text
            ),
            'payload',case when candidate_reminder.candidate_generation_id is not null
              and (candidate_reminder.manual_reminder_available_at_utc is null
                or pg_catalog.transaction_timestamp()>=candidate_reminder.manual_reminder_available_at_utc)
              then pg_catalog.jsonb_build_object(
                'candidate_generation_id',candidate_reminder.candidate_generation_id,
                'projection_publication_id',v_publication.id
              ) else '{}'::jsonb end
          ))
        ),
        'accept_system_hours_action',pg_catalog.jsonb_build_object(
          'label','Accept system hours','kind','COMMAND','command','ACCEPT_SYSTEM_HOURS',
          'enabled',coalesce(pg_catalog.array_length(query.accept_incident_ids,1),0)>0,
          'context',pg_catalog.jsonb_build_object(
            'candidate',query.candidate_name,'client',query.client_name,
            'issue',query.issue_summary
          ),
          'payload',pg_catalog.jsonb_build_object(
            'actor_user_id',v_actor,'source_cycle_id',v_cycle.id,
            'projection_publication_id',v_publication.id,
            'expected_workspace_version',v_workspace_version,'action','ACCEPT_SYSTEM_HOURS',
            'selection',pg_catalog.jsonb_build_object(
              'mode','EXPLICIT','group_keys',pg_catalog.jsonb_build_array(query.group_key),
              'excluded_group_keys','[]'::jsonb,
              'incident_ids',to_jsonb(query.accept_incident_ids),
              'filters',v_filters,'sort_key',v_sort_key,'sort_direction',v_sort_direction,
              'selection_proof',null,
              'group_selection_proofs',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
                'group_key',query.group_key,
                'selection_proof',pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
                  'WEEKLY_SOURCE_ACCEPT_GROUP_SELECTION_V1',pg_catalog.jsonb_build_object(
                    'workspace_version',v_workspace_version,'group_key',query.group_key,
                    'incident_ids',to_jsonb(query.accept_incident_ids)
                  )),'hex')
              ))
            )
          )
        )
      ) row_json,
      pg_catalog.row_number() over(order by
        case when v_sort_key='candidate' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(query.candidate_name) end asc,
        case when v_sort_key='candidate' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(query.candidate_name) end desc,
        case when v_sort_key='client' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(query.client_name) end asc,
        case when v_sort_key='client' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(query.client_name) end desc,
        case when v_sort_key='issues' and v_sort_direction='asc' then query.issue_count end asc,
        case when v_sort_key='issues' and v_sort_direction='desc' then query.issue_count end desc,
        case when v_sort_key='age' and v_sort_direction='asc' then query.first_seen_at_utc end asc,
        case when v_sort_key='age' and v_sort_direction='desc' then query.first_seen_at_utc end desc,
        query.group_key
      ) position
      from private.weekly_source_office_query_groups_v1(v_cycle.id,v_publication.id,v_filters) query
      left join lateral (
        select generation.id candidate_generation_id,
          generation.manual_reminder_available_at_utc
        from public.weekly_candidate_cohorts cohort
        join public.weekly_candidate_outreach_generations generation
          on generation.id=cohort.current_generation_id and generation.state='ACTIVE'
        where cohort.source_cycle_id=v_cycle.id
          and cohort.candidate_id=query.candidate_id
          and cohort.client_id=query.client_id
          and cohort.manager_recipient_route_key=query.manager_recipient_route_key
        limit 1
      ) candidate_reminder on true
      order by
        case when v_sort_key='candidate' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(query.candidate_name) end asc,
        case when v_sort_key='candidate' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(query.candidate_name) end desc,
        case when v_sort_key='client' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(query.client_name) end asc,
        case when v_sort_key='client' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(query.client_name) end desc,
        case when v_sort_key='issues' and v_sort_direction='asc' then query.issue_count end asc,
        case when v_sort_key='issues' and v_sort_direction='desc' then query.issue_count end desc,
        case when v_sort_key='age' and v_sort_direction='asc' then query.first_seen_at_utc end asc,
        case when v_sort_key='age' and v_sort_direction='desc' then query.first_seen_at_utc end desc,
        query.group_key
      offset v_offset limit v_limit
    ) page;
    v_next_cursor:=case when v_offset+v_limit<v_total
      then v_workspace_version||':'||(v_offset+v_limit)::text else '' end;
    v_queries:=pg_catalog.jsonb_build_object('rows',v_rows,'total_count',v_total,
      'next_cursor',v_next_cursor,'has_more',v_next_cursor<>'','record_version',v_workspace_version,'stale',false);
  else
    v_queries:=pg_catalog.jsonb_build_object('rows','[]'::jsonb,'total_count',v_query_count,
      'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);
  end if;
  v_queries:=v_queries||pg_catalog.jsonb_build_object('bulk_actions',v_bulk_actions);

  if v_publication_id is not null then
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key',source_row.id,'candidate',coalesce(candidate.display_name,candidate.tms_ref,'Candidate'),
      'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
      'client',coalesce(client.name,source_row.source_client_identity),
      'system_hours',case when source_row.start_at_local is null then pg_catalog.chr(8212)
        else to_char(source_row.start_at_local,'HH24:MI')||'-'||to_char(source_row.end_at_local,'HH24:MI')
          ||' ('||coalesce(source_row.break_minutes,0)||' min break)' end,
      'actual_hours',case when source_row.start_at_local is null then pg_catalog.chr(8212)
        else to_char(source_row.start_at_local,'HH24:MI')||'-'||to_char(source_row.end_at_local,'HH24:MI')
          ||' ('||coalesce(source_row.break_minutes,0)||' min break)' end,
      'movement',case charge.row_sign_kind
        when 'FULL_NEGATIVE' then 'Reversal'
        when 'POSITIVE' then 'Positive'
        else case when coalesce(source_row.actual_net_minutes,0)<0 then 'Reversal' else 'Positive' end end,
      'commission',case when source_row.source_commission_pence is null then pg_catalog.chr(8212)
        else case when source_row.source_commission_pence<0 then '-£' else '£' end
          ||to_char(pg_catalog.abs(source_row.source_commission_pence)::numeric/100,'FM9999999990.00') end,
      'total_cost',case when source_row.source_total_cost_pence is null then pg_catalog.chr(8212)
        else case when source_row.source_total_cost_pence<0 then '-£' else '£' end
          ||to_char(pg_catalog.abs(source_row.source_total_cost_pence)::numeric/100,'FM9999999990.00') end,
      'invoice_charge',case when source_row.source_shift_charge_pence is null then pg_catalog.chr(8212)
        else case when source_row.source_shift_charge_pence<0 then '-£' else '£' end
          ||to_char(pg_catalog.abs(source_row.source_shift_charge_pence)::numeric/100,'FM9999999990.00') end,
      'status',pg_catalog.jsonb_build_object('text','Ready','tone','positive'),
      'actions','[]'::jsonb
    ) order by
      case when v_sort_key='candidate' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,candidate.tms_ref,'')) end collate "C" asc,
      case when v_sort_key='candidate' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,candidate.tms_ref,'')) end collate "C" desc,
      case when v_sort_key='day_date' and v_sort_direction='asc' then source_row.work_date end asc,
      case when v_sort_key='day_date' and v_sort_direction='desc' then source_row.work_date end desc,
      case when v_sort_key='client' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(client.name,source_row.source_client_identity,'')) end collate "C" asc,
      case when v_sort_key='client' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(client.name,source_row.source_client_identity,'')) end collate "C" desc,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.start_at_local end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.start_at_local end desc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.end_at_local end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.end_at_local end desc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.break_minutes end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.break_minutes end desc nulls last,
      case when v_sort_key='movement' and v_sort_direction='asc' then
        case charge.row_sign_kind when 'FULL_NEGATIVE' then 'Reversal' when 'POSITIVE' then 'Positive'
          else case when coalesce(source_row.actual_net_minutes,0)<0 then 'Reversal' else 'Positive' end end end asc,
      case when v_sort_key='movement' and v_sort_direction='desc' then
        case charge.row_sign_kind when 'FULL_NEGATIVE' then 'Reversal' when 'POSITIVE' then 'Positive'
          else case when coalesce(source_row.actual_net_minutes,0)<0 then 'Reversal' else 'Positive' end end end desc,
      case when v_sort_key='commission' and v_sort_direction='asc' then source_row.source_commission_pence end asc nulls last,
      case when v_sort_key='commission' and v_sort_direction='desc' then source_row.source_commission_pence end desc nulls last,
      case when v_sort_key='total_cost' and v_sort_direction='asc' then source_row.source_total_cost_pence end asc nulls last,
      case when v_sort_key='total_cost' and v_sort_direction='desc' then source_row.source_total_cost_pence end desc nulls last,
      case when v_sort_key='invoice_charge' and v_sort_direction='asc' then source_row.source_shift_charge_pence end asc nulls last,
      case when v_sort_key='invoice_charge' and v_sort_direction='desc' then source_row.source_shift_charge_pence end desc nulls last,
      case when v_sort_key='status' and v_sort_direction='asc' then 'Ready' end asc,
      case when v_sort_key='status' and v_sort_direction='desc' then 'Ready' end desc,
      case when v_sort_key='job_role' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.role_band_source,'')) end collate "C" asc,
      case when v_sort_key='job_role' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.role_band_source,'')) end collate "C" desc,
      case when v_sort_key='contract' and v_sort_direction='asc' then resolution.contract_id end asc nulls last,
      case when v_sort_key='contract' and v_sort_direction='desc' then resolution.contract_id end desc nulls last,
      case when v_sort_key='outcome' and v_sort_direction='asc' then source_row.row_finalisation_state end asc,
      case when v_sort_key='outcome' and v_sort_direction='desc' then source_row.row_finalisation_state end desc,
      private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,candidate.tms_ref,'')) collate "C",
      source_row.work_date,source_row.start_at_local,source_row.id),'[]'::jsonb)
    into v_rows
    from public.weekly_source_upload_rows source_row
    join lateral (select resolution.* from public.weekly_source_row_resolutions resolution
      where resolution.upload_row_id=source_row.id order by resolution.generation desc,resolution.id desc limit 1) resolution
      on resolution.mapping_state='RESOLVED'
    left join public.candidates candidate on candidate.id=resolution.candidate_id
    left join public.clients client on client.id=resolution.client_id
    left join lateral (select charge.* from public.weekly_source_charge_checks charge
      where charge.upload_row_id=source_row.id order by charge.generation desc,charge.id desc limit 1) charge on true
    where source_row.upload_id=v_upload.id
      and source_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO')
      and coalesce(charge.phase_severity,'NONE')<>'FINALISATION_BLOCKER';
    v_ready:=pg_catalog.jsonb_build_object('rows',v_rows,'total_count',pg_catalog.jsonb_array_length(v_rows),
      'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);

    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key',source_row.id,'candidate',coalesce(candidate.display_name,source_row.source_candidate_identity),
      'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
      'system_hours',case
        when source_row.start_at_local is not null and source_row.end_at_local is not null
          then to_char(source_row.start_at_local,'HH24:MI')||pg_catalog.chr(8211)
            ||to_char(source_row.end_at_local,'HH24:MI')
            ||case when source_row.break_minutes is not null
              then ' ('||source_row.break_minutes::text||' min break)' else '' end
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'No confirmed hours'
        else 'Start or finish missing' end,
      'status',pg_catalog.jsonb_build_object(
        'text',case
          when resolution.mapping_state is distinct from 'RESOLVED' then 'Needs correction'
          when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'Not finalised'
          when charge.phase_severity='FINALISATION_BLOCKER' then 'Charge needs checking'
          else 'Needs correction' end,
        'tone',case
          when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'warning'
          else 'danger' end),
      'problem',case
        when resolution.mapping_state is distinct from 'RESOLVED' then 'Link this row before finalising'
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'This shift is not finalised'
        when charge.phase_severity='FINALISATION_BLOCKER' then 'Check the charge for this shift'
        else 'Check the hours for this shift' end,
      'actions',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'label',case when resolution.mapping_state='CANDIDATE_NOT_FOUND' then 'Link candidate'
          when resolution.mapping_state='CLIENT_NOT_FOUND' then 'Link client'
          when resolution.mapping_state in ('NO_ELIGIBLE_CONTRACT','AMBIGUOUS_CONTRACT','CONTRACT_SELECTION_REQUIRED') then 'Choose contract'
          when charge.phase_severity='FINALISATION_BLOCKER' then 'Open charge details'
          else 'View details' end,
        'enabled',true,'payload',pg_catalog.jsonb_build_object('upload_row_id',source_row.id)
      ))
    ) order by
      case when v_sort_key='candidate' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,source_row.source_candidate_identity,'')) end collate "C" asc,
      case when v_sort_key='candidate' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(candidate.display_name,source_row.source_candidate_identity,'')) end collate "C" desc,
      case when v_sort_key='day_date' and v_sort_direction='asc' then source_row.work_date end asc,
      case when v_sort_key='day_date' and v_sort_direction='desc' then source_row.work_date end desc,
      case when v_sort_key='client' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.source_client_identity,'')) end collate "C" asc,
      case when v_sort_key='client' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.source_client_identity,'')) end collate "C" desc,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.start_at_local end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.start_at_local end desc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.end_at_local end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.end_at_local end desc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='asc' then source_row.break_minutes end asc nulls last,
      case when v_sort_key in ('system_hours','actual_hours') and v_sort_direction='desc' then source_row.break_minutes end desc nulls last,
      case when v_sort_key='movement' and v_sort_direction='asc'
        then case when coalesce(source_row.actual_net_minutes,0)<0 then 'Reversal' else 'Positive' end end asc,
      case when v_sort_key='movement' and v_sort_direction='desc'
        then case when coalesce(source_row.actual_net_minutes,0)<0 then 'Reversal' else 'Positive' end end desc,
      case when v_sort_key='commission' and v_sort_direction='asc' then source_row.source_commission_pence end asc nulls last,
      case when v_sort_key='commission' and v_sort_direction='desc' then source_row.source_commission_pence end desc nulls last,
      case when v_sort_key='total_cost' and v_sort_direction='asc' then source_row.source_total_cost_pence end asc nulls last,
      case when v_sort_key='total_cost' and v_sort_direction='desc' then source_row.source_total_cost_pence end desc nulls last,
      case when v_sort_key='invoice_charge' and v_sort_direction='asc' then source_row.source_shift_charge_pence end asc nulls last,
      case when v_sort_key='invoice_charge' and v_sort_direction='desc' then source_row.source_shift_charge_pence end desc nulls last,
      case when v_sort_key='status' and v_sort_direction='asc' then case
        when resolution.mapping_state is distinct from 'RESOLVED' then 'Needs correction'
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'Not finalised'
        when charge.phase_severity='FINALISATION_BLOCKER' then 'Charge needs checking'
        else 'Needs correction' end end asc,
      case when v_sort_key='status' and v_sort_direction='desc' then case
        when resolution.mapping_state is distinct from 'RESOLVED' then 'Needs correction'
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'Not finalised'
        when charge.phase_severity='FINALISATION_BLOCKER' then 'Charge needs checking'
        else 'Needs correction' end end desc,
      case when v_sort_key='problem' and v_sort_direction='asc' then case
        when resolution.mapping_state is distinct from 'RESOLVED' then 'Link this row before finalising'
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'This shift is not finalised'
        when charge.phase_severity='FINALISATION_BLOCKER' then 'Check the charge for this shift'
        else 'Check the hours for this shift' end end asc,
      case when v_sort_key='problem' and v_sort_direction='desc' then case
        when resolution.mapping_state is distinct from 'RESOLVED' then 'Link this row before finalising'
        when source_row.row_finalisation_state='SOURCE_UNFINALISED' then 'This shift is not finalised'
        when charge.phase_severity='FINALISATION_BLOCKER' then 'Check the charge for this shift'
        else 'Check the hours for this shift' end end desc,
      case when v_sort_key='job_role' and v_sort_direction='asc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.role_band_source,'')) end collate "C" asc,
      case when v_sort_key='job_role' and v_sort_direction='desc'
        then private.weekly_source_query_ascii_fold_v1(coalesce(source_row.role_band_source,'')) end collate "C" desc,
      case when v_sort_key='contract' and v_sort_direction='asc' then resolution.contract_id end asc nulls last,
      case when v_sort_key='contract' and v_sort_direction='desc' then resolution.contract_id end desc nulls last,
      case when v_sort_key='outcome' and v_sort_direction='asc' then source_row.row_finalisation_state end asc,
      case when v_sort_key='outcome' and v_sort_direction='desc' then source_row.row_finalisation_state end desc,
      source_row.work_date,private.weekly_source_query_ascii_fold_v1(source_row.source_candidate_identity) collate "C",source_row.id),'[]'::jsonb)
    into v_rows
    from public.weekly_source_upload_rows source_row
    left join lateral (select latest.* from public.weekly_source_row_resolutions latest
      where latest.upload_row_id=source_row.id order by latest.generation desc,latest.id desc limit 1) resolution on true
    left join public.candidates candidate on candidate.id=resolution.candidate_id
    left join lateral (select latest.* from public.weekly_source_charge_checks latest
      where latest.upload_row_id=source_row.id order by latest.generation desc,latest.id desc limit 1) charge on true
    where source_row.upload_id=v_upload.id
      and (resolution.mapping_state is distinct from 'RESOLVED'
        or source_row.row_finalisation_state in ('SOURCE_UNFINALISED','BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE')
        or charge.phase_severity='FINALISATION_BLOCKER');
    v_blocked:=pg_catalog.jsonb_build_object('rows',v_rows,'total_count',pg_catalog.jsonb_array_length(v_rows),
      'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);
    v_finalise_enabled:=v_blocker_count=0
      and (v_profile.profile_code<>'NHSP_FINAL_BACKING_V1' or v_rate_warning_unaccepted_count=0)
      and pg_catalog.statement_timestamp()>=coalesce(
        (select scope.cutoff_at_utc from public.weekly_source_report_scopes scope
          where scope.id=v_report_scope_id),
        v_cycle.cutoff_at_utc
      )
      and v_cycle.state not in ('FINALISING','FINALISED');
    v_finalise_payload:=pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle.id,'authority_scope_kind',v_publication.authority_scope_kind,
      'report_scope_id',v_publication.report_scope_id,'upload_id',v_upload.id,
      'projection_publication_id',v_publication.id,
      'expected_authority_scope_version',v_publication.authority_scope_version,
      'expected_row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'),
      'expected_comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
      'expected_issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex')
    );

    select pg_catalog.jsonb_build_object(
      'state',run_row.state,
      'title',case run_row.state
        when 'ACTION_REQUIRED' then 'Approved hours need attention'
        when 'RECOVERY_REQUIRED' then 'Approved hours update needs checking'
        else 'Approved hours update is not complete' end,
      'body',case run_row.state
        when 'ACTION_REQUIRED' then run_row.action_required_task_count::text
          ||case when run_row.action_required_task_count=1 then ' timesheet needs' else ' timesheets need' end
          ||' review. The source is finalised and invoicing is not delayed.'
        when 'RECOVERY_REQUIRED' then 'The source is finalised. Check the approved-hours update before trying it again.'
        else 'The source is finalised. Continue the approved-hours update.' end,
      'action',case
        when task.state='READY' then pg_catalog.jsonb_build_object(
          'label','Continue approved hours update','command','FINALISE_WEEK',
          'payload',v_finalise_payload
        )
        when task.state in ('SUBMISSION_STARTED','RECOVERY_REQUIRED') then pg_catalog.jsonb_build_object(
          'label','Check approved hours update','command','RECOVER_FINALISED_PAY',
          'payload',pg_catalog.jsonb_build_object(
            'final_revision_id',run_row.final_revision_id,'run_id',run_row.id,
            'task_id',task.id,'expected_task_version',task.version,'confirm_retry',false
          )
        )
        else null end
    ) into v_finalise_pay_follow_up
    from public.weekly_source_final_revisions revision
    join public.weekly_source_finalisation_pay_runs run_row
      on run_row.final_revision_id=revision.id and run_row.state<>'COMPLETE'
    left join lateral (
      select pending.*
      from public.weekly_source_finalisation_pay_tasks pending
      where pending.run_id=run_row.id
        and pending.state in ('READY','SUBMISSION_STARTED','RECOVERY_REQUIRED')
      order by case pending.state
        when 'SUBMISSION_STARTED' then 1 when 'RECOVERY_REQUIRED' then 2 else 3 end,
        pending.task_ordinal,pending.id
      limit 1
    ) task on true
    where revision.source_cycle_id=v_cycle.id and revision.state='CURRENT'
      and revision.authority_scope_kind=v_publication.authority_scope_kind
      and revision.report_scope_id is not distinct from v_publication.report_scope_id
    order by revision.revision_number desc,revision.id desc
    limit 1;
  else
    v_ready:=pg_catalog.jsonb_build_object('rows','[]'::jsonb,'total_count',0,'next_cursor','','has_more',false,'record_version',v_workspace_version,'stale',false);
    v_blocked:=v_ready;
  end if;

  select pg_catalog.jsonb_build_object(
    'title','Finalisation progress','cycle_label','Week ending '||to_char(v_cycle.finalisation_week_ending,'FMDD Mon YYYY'),
    'cycle_id',v_cycle.id,
    'cycle_options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'value',item.id,'label','Week ending '||to_char(item.finalisation_week_ending,'FMDD Mon YYYY'))
      order by item.finalisation_week_ending desc,item.id)
      from public.weekly_source_cycles item where item.source_group_id=v_group.id),'[]'::jsonb),
    'complete',not exists(
      select 1 from public.weekly_source_group_clients membership
      where membership.source_group_id=v_group.id
        and v_cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)
        and not exists(select 1 from public.weekly_source_client_cycle_completions completion
          where completion.source_cycle_id=v_cycle.id and completion.client_id=membership.client_id and completion.state='CURRENT')
    ),
    'rows',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','tracker-'||membership.client_id::text,'source',v_group.display_name,'client',client.name,
      'status',pg_catalog.jsonb_build_object(
        'text',case when completion.id is not null then case completion.completion_kind
          when 'FINAL_SOURCE' then 'Finalised' else 'No shifts this week' end else 'Not finalised' end,
        'tone',case when completion.id is not null then 'positive' else 'warning' end),
      'detail',case when completion.id is not null then to_char(completion.attested_at_utc at time zone 'Europe/London','DD Mon YYYY HH24:MI') else '' end,
      'actions',case
        when completion.id is null
         and not exists(select 1 from public.weekly_source_report_scopes scope
           where scope.source_cycle_id=v_cycle.id and scope.client_id=membership.client_id
             and (scope.current_complete_upload_id is not null or scope.current_projection_publication_id is not null or scope.current_final_revision_id is not null))
         and not exists(select 1 from public.weekly_source_upload_rows source_row
           join public.weekly_source_uploads upload on upload.id=source_row.upload_id and upload.source_cycle_id=v_cycle.id and upload.state='CURRENT'
           join lateral (select resolution.* from public.weekly_source_row_resolutions resolution
             where resolution.upload_row_id=source_row.id order by resolution.generation desc,resolution.id desc limit 1) resolution
             on resolution.mapping_state='RESOLVED' and resolution.client_id=membership.client_id)
         and not exists(select 1 from public.weekly_discrepancy_incidents incident
           where incident.source_cycle_id=v_cycle.id and incident.client_id=membership.client_id and incident.state='OPEN')
        then pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'label','No shifts to import','enabled',true,'kind','COMMAND','command','NO_SHIFTS_TO_IMPORT',
          'context',pg_catalog.jsonb_build_object(
            'trust',client.name,
            'cutoff',to_char(v_cycle.cutoff_at_utc at time zone 'Europe/London','Dy FMDD Mon YYYY HH24:MI')
          ),
          'payload',pg_catalog.jsonb_build_object(
            'actor_user_id',v_actor,
            'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
            'client_id',membership.client_id,'expected_cycle_version',v_cycle.version,
            'attestation_text','No shifts to import'
          )))
        else pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'label','View','enabled',true,'payload',pg_catalog.jsonb_build_object('client_id',membership.client_id)
        )) end
    ) order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",client.id)
    from public.weekly_source_group_clients membership
    join public.clients client on client.id=membership.client_id
    left join public.weekly_source_client_cycle_completions completion
      on completion.source_cycle_id=v_cycle.id and completion.client_id=membership.client_id and completion.state='CURRENT'
    where membership.source_group_id=v_group.id
      and v_cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)),'[]'::jsonb)
  ) into v_tracker;

  if v_tab='history' then
    with ranked_cycles as (
      select item.id,pg_catalog.row_number() over(
        order by item.finalisation_week_ending desc,item.id desc
      ) cycle_ordinal
      from public.weekly_source_cycles item
      where item.source_group_id=v_group.id
        and item.finalisation_week_ending<=v_cycle.finalisation_week_ending
    ), selected_cycles as (
      select id from ranked_cycles
      where (v_cycle_filter='CURRENT_PAY_CYCLE' and id=v_cycle.id)
         or (v_cycle_filter='LAST_4_PAY_CYCLES' and cycle_ordinal<=4)
         or (v_cycle_filter='LAST_13_PAY_CYCLES' and cycle_ordinal<=13)
    ), events as (
      select completion.attested_at_utc when_utc,completion.id::text row_key,
        v_group.display_name source_label,
        case completion.completion_kind when 'FINAL_SOURCE' then 'Finalised' else 'No shifts to import' end event_label,
        coalesce(office_user.display_name,office_user.email,'Office') by_label,
        client.name detail_label
      from public.weekly_source_client_cycle_completions completion
      join selected_cycles selected on selected.id=completion.source_cycle_id
      join public.clients client on client.id=completion.client_id
      left join public.tms_users office_user on office_user.id=completion.attested_by_user_id
      union all
      select upload.uploaded_at_utc,upload.id::text,v_group.display_name,'Source uploaded',
        coalesce(office_user.display_name,office_user.email,'Office'),upload.original_filename
      from public.weekly_source_uploads upload
      join selected_cycles selected on selected.id=upload.source_cycle_id
      left join public.tms_users office_user on office_user.id=upload.uploaded_by_user_id
    )
    select pg_catalog.count(*)::integer into v_total from events;

    with ranked_cycles as (
      select item.id,pg_catalog.row_number() over(
        order by item.finalisation_week_ending desc,item.id desc
      ) cycle_ordinal
      from public.weekly_source_cycles item
      where item.source_group_id=v_group.id
        and item.finalisation_week_ending<=v_cycle.finalisation_week_ending
    ), selected_cycles as (
      select id from ranked_cycles
      where (v_cycle_filter='CURRENT_PAY_CYCLE' and id=v_cycle.id)
         or (v_cycle_filter='LAST_4_PAY_CYCLES' and cycle_ordinal<=4)
         or (v_cycle_filter='LAST_13_PAY_CYCLES' and cycle_ordinal<=13)
    ), events as (
      select completion.attested_at_utc when_utc,completion.id::text row_key,
        v_group.display_name source_label,
        case completion.completion_kind when 'FINAL_SOURCE' then 'Finalised' else 'No shifts to import' end event_label,
        coalesce(office_user.display_name,office_user.email,'Office') by_label,
        client.name detail_label
      from public.weekly_source_client_cycle_completions completion
      join selected_cycles selected on selected.id=completion.source_cycle_id
      join public.clients client on client.id=completion.client_id
      left join public.tms_users office_user on office_user.id=completion.attested_by_user_id
      union all
      select upload.uploaded_at_utc,upload.id::text,v_group.display_name,'Source uploaded',
        coalesce(office_user.display_name,office_user.email,'Office'),upload.original_filename
      from public.weekly_source_uploads upload
      join selected_cycles selected on selected.id=upload.source_cycle_id
      left join public.tms_users office_user on office_user.id=upload.uploaded_by_user_id
    ), page as (
      select pg_catalog.jsonb_build_object(
          'row_key',event.row_key,'when',to_char(event.when_utc at time zone 'Europe/London','DD Mon YYYY HH24:MI'),
          'source',event.source_label,'event',event.event_label,'by',event.by_label,'detail',event.detail_label
        ) row_json,
        pg_catalog.row_number() over(order by
          case when v_sort_key='when' and v_sort_direction='asc' then event.when_utc end asc,
          case when v_sort_key='when' and v_sort_direction='desc' then event.when_utc end desc,
          case when v_sort_key='source' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.source_label) end asc,
          case when v_sort_key='source' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.source_label) end desc,
          case when v_sort_key='event' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.event_label) end asc,
          case when v_sort_key='event' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.event_label) end desc,
          case when v_sort_key='by' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.by_label) end asc,
          case when v_sort_key='by' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.by_label) end desc,
          case when v_sort_key='detail' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.detail_label) end asc,
          case when v_sort_key='detail' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.detail_label) end desc,
          case when v_sort_direction='asc' then event.when_utc end asc,
          case when v_sort_direction='desc' then event.when_utc end desc,
          event.row_key
        ) position
      from events event
      order by
        case when v_sort_key='when' and v_sort_direction='asc' then event.when_utc end asc,
        case when v_sort_key='when' and v_sort_direction='desc' then event.when_utc end desc,
        case when v_sort_key='source' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.source_label) end asc,
        case when v_sort_key='source' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.source_label) end desc,
        case when v_sort_key='event' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.event_label) end asc,
        case when v_sort_key='event' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.event_label) end desc,
        case when v_sort_key='by' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.by_label) end asc,
        case when v_sort_key='by' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.by_label) end desc,
        case when v_sort_key='detail' and v_sort_direction='asc' then private.weekly_source_query_ascii_fold_v1(event.detail_label) end asc,
        case when v_sort_key='detail' and v_sort_direction='desc' then private.weekly_source_query_ascii_fold_v1(event.detail_label) end desc,
        case when v_sort_direction='asc' then event.when_utc end asc,
        case when v_sort_direction='desc' then event.when_utc end desc,
        event.row_key
      offset v_offset limit v_limit
    )
    select coalesce(pg_catalog.jsonb_agg(page.row_json order by page.position),'[]'::jsonb)
    into v_rows from page;
    v_next_cursor:=case when v_offset+v_limit<v_total
      then v_workspace_version||':'||(v_offset+v_limit)::text else '' end;
  else
    v_rows:='[]'::jsonb;
    v_total:=0;
    v_next_cursor:='';
  end if;
  v_history:=pg_catalog.jsonb_build_object('rows',v_rows,
    'total_count',v_total,'next_cursor',v_next_cursor,'has_more',v_next_cursor<>'',
    'record_version',v_workspace_version,'stale',false,'cycle_filter',v_cycle_filter,
    'cycle_options',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('value','CURRENT_PAY_CYCLE','label','Current pay cycle'),
      pg_catalog.jsonb_build_object('value','LAST_4_PAY_CYCLES','label','Last 4 pay cycles'),
      pg_catalog.jsonb_build_object('value','LAST_13_PAY_CYCLES','label','Last 13 pay cycles')
    ));

  if v_client_id is not null then
    select client.name into v_context_client_name from public.clients client where client.id=v_client_id;
  end if;
  v_context_cutoff:=v_cycle.cutoff_at_utc;
  if v_report_scope_id is not null then
    select scope.cutoff_at_utc into v_context_cutoff
    from public.weekly_source_report_scopes scope where scope.id=v_report_scope_id;
  end if;
  if v_group.source_family='NHSP' then
    v_nhsp_report_number:=nullif(pg_catalog.btrim(v_upload.file_metadata_json->>'nhsp_report_number'),'');
    select pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('key','source_group','label','Source','value',v_group.id,
        'options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'value',item.id,'label',case when item.source_family='NHSP' then 'NHSP' else item.display_name end)
          order by private.weekly_source_query_ascii_fold_v1(item.display_name) collate "C",item.id)
          from public.weekly_source_groups item where item.active),'[]'::jsonb)),
      pg_catalog.jsonb_build_object('key','client','label','Trust','value',coalesce(v_client_id::text,''),
        'options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('value',client.id,'label',client.name)
          order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",client.id)
          from public.weekly_source_group_clients membership join public.clients client on client.id=membership.client_id
          where membership.source_group_id=v_group.id and v_cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)),'[]'::jsonb)),
      pg_catalog.jsonb_build_object('key','report','label','Report number','value',coalesce(v_nhsp_report_number,'Not confirmed'),'options','[]'::jsonb),
      pg_catalog.jsonb_build_object('key','cutoff','label','Cutoff','value',to_char(v_context_cutoff at time zone 'Europe/London','DD/MM/YYYY HH24:MI'),'options','[]'::jsonb)
    ) into v_controls;
  else
    select pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('key','journey','label','Journey',
        'value',case when v_authority_mode='TIMESHEET_AUTHORITY'
          then 'Signed Timesheet decides hours' else 'Client system decides hours' end,
        'options','[]'::jsonb),
      pg_catalog.jsonb_build_object('key','source_group','label','Source','value',v_group.id,
        'options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('value',item.id,'label',item.display_name)
          order by private.weekly_source_query_ascii_fold_v1(item.display_name) collate "C",item.id)
          from public.weekly_source_groups item where item.active),'[]'::jsonb)),
      pg_catalog.jsonb_build_object('key','cycle','label','Week','value',v_cycle.id,
        'options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('value',item.id,'label','Week ending '||to_char(item.finalisation_week_ending,'FMDD Mon YYYY'))
          order by item.finalisation_week_ending desc,item.id)
          from public.weekly_source_cycles item where item.source_group_id=v_group.id),'[]'::jsonb)),
      pg_catalog.jsonb_build_object('key','client','label','Client','value',coalesce(v_client_id::text,''),
        'options',coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('value',client.id,'label',client.name)
          order by private.weekly_source_query_ascii_fold_v1(client.name) collate "C",client.id)
          from public.weekly_source_group_clients membership join public.clients client on client.id=membership.client_id
          where membership.source_group_id=v_group.id and v_cycle.finalisation_week_ending between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)),'[]'::jsonb))
    ) into v_controls;
  end if;

  v_cycle_label:='Week ending '||to_char(v_cycle.finalisation_week_ending,'FMDD Mon YYYY');
  v_cycle_state_label:=case v_cycle.state when 'OPEN' then 'Before cutoff'
    when 'FINALISABLE' then 'Ready for finalisation' when 'FINALISED' then 'Finalised'
    when 'CORRECTION_IN_PROGRESS' then 'Correction in progress' else 'Ready for finalisation' end;
  v_cycle_state_tone:=case when v_cycle.state='FINALISED' then 'positive'
    when v_cycle.state='CORRECTION_IN_PROGRESS' then 'danger' else 'warning' end;

  return pg_catalog.jsonb_build_object(
    'contract','WEEKLY_SOURCE_IMPORT_WORKSPACE_V1','workspace_version',v_workspace_version,
    'profile',pg_catalog.jsonb_build_object(
      'id',coalesce(v_profile.profile_code,case when v_group.source_family='NHSP' then 'NHSP_FINAL_BACKING_V1' else 'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1' end),
      'label',case when v_group.source_family='NHSP' then 'NHSP' else v_group.display_name end,
      'finalise_label',case when v_group.source_family='NHSP' then 'Finalise report' else 'Finalise source' end),
    'context',pg_catalog.jsonb_build_object('subtitle',case when v_group.source_family='NHSP' then
        'NHSP'||case when v_context_client_name is null then '' else ' · '||v_context_client_name end
      else v_cycle_label end,'cycle_state',v_cycle_state_label,
      'cycle_tone',v_cycle_state_tone,'controls',v_controls),
    'selected',pg_catalog.jsonb_build_object('source_group_id',v_group.id,'source_cycle_id',v_cycle.id,
      'client_id',v_client_id,'report_scope_id',v_report_scope_id,'projection_publication_id',v_publication_id),
    'counts',pg_catalog.jsonb_build_object('queries',v_query_count,'blockers',v_blocker_count,
      'paid_unresolved',v_paid_unresolved_count),
    'imports',v_imports,'queries',v_queries,
    'finalise',pg_catalog.jsonb_build_object(
      'ready',v_ready,'blocked',v_blocked,'active_list',case when v_blocker_count>0 then 'blocked' else 'ready' end,
      'confirmation_text','I confirm this is the final source for this week.',
      'confirmation_required',true,'finalise_enabled',v_finalise_enabled,
      'finalise_payload',v_finalise_payload,
      'rate_warnings',v_rate_warnings,
      'approved_hours_follow_up',v_finalise_pay_follow_up,
      'source_summary',case when v_publication_id is null then 'Upload a source file to continue.'
        else v_upload.original_filename end,'tracker',v_tracker),
    'history',v_history,'notices','[]'::jsonb,
    -- Gate 9 G9-1, workspace half.  The workspace serves the complete
    -- server-owned lifecycle vocabulary -- all 22 rows, with their exact
    -- headings, statuses, permitted new actions and forbidden inferences -- so
    -- that no Office surface ever hard-codes a lifecycle heading and no browser
    -- has to map a state to one.  The PER-TIMESHEET phase is not computed here:
    -- it is one read per Timesheet from the named owner below, which is what
    -- the Simple Timesheet and the Bulk right pane already call.
    'lifecycle_policy',private.weekly_source_office_lifecycle_policy_v1()
      ||pg_catalog.jsonb_build_object(
        'per_timesheet_owner','public.weekly_source_office_timesheet_presentation_v1',
        'per_timesheet_member','lifecycle',
        'browser_may_infer_phase',false)
  );
exception
  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    raise exception 'WEEKLY_SOURCE_WORKSPACE_REQUEST_INVALID' using errcode='22023';
end;
$function$;

create or replace function private.weekly_source_office_money_text_v1(p_value numeric)
returns text
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  -- chr(163) rather than a literal: a non-ASCII literal in a repeatable is
  -- only loaded correctly when the loading client's encoding is UTF-8, and a
  -- double-encoded currency symbol would put a wrong money label on the four
  -- server-owned Office totals.
  select pg_catalog.chr(163)||to_char(coalesce(p_value,0),'FM999999999990D00');
$function$;

-- ===========================================================================
-- Gate 9 item G9-1: the SERVER-OWNED lifecycle phase.
--
-- `P:\annexes\ui-lifecycle-state-matrix.csv` and `24 section 15` make the
-- display phase, its heading and its permitted new actions server facts.  The
-- browser "must not infer payment state from invoice state, authorisation
-- status or money" (`24 section 15`), so every string below is served, never
-- reconstructed in JavaScript.
--
-- The heading strings are the lifecycle policy's own, character for character.
-- File 24's lifecycle policy CONTROLS: files 17 and 18 still carry the deleted
-- heading `Hours being authorised` (contract erratum E-4) and are not followed.
-- `UI-022`'s heading is `proof/36 section 7`'s DEC-061 Option A wording, with
-- U+00B7 MIDDLE DOT between the two clauses.
--
-- This table is the ONLY place the 22 rows live, so a heading can never drift
-- between the presentation owner, the workspace owner and the verifier.
-- ===========================================================================
create or replace function private.weekly_source_office_lifecycle_policy_v1()
returns jsonb
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'contract','WEEKLY_SOURCE_OFFICE_LIFECYCLE_POLICY_V1',
    'policy_version','4.1.0',
    'source','P:/annexes/ui-lifecycle-state-matrix.csv + 24_TIMESHEET_LIFECYCLE_UI_POLICY',
    'rows',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('ui_state','UI-001','server_phase','FIRST_AUTHORISATION_PENDING',
        'surface','OFFICE','heading','Hours to authorise','heading_source','SERVER',
        'primary_schedule','hours_to_authorise',
        'right_pane_status','Not yet authorised',
        'allowed_new_actions',pg_catalog.jsonb_build_array('AUTHORISE'),
        'forbidden_inference','Do not call source approved before Office acts'),
      pg_catalog.jsonb_build_object('ui_state','UI-002','server_phase','FIRST_AUTHORISATION_PENDING_NO_SUBMISSION',
        'surface','OFFICE','heading','Hours to authorise','heading_source','SERVER',
        'primary_schedule','hours_to_authorise',
        'right_pane_status','Not yet authorised',
        'allowed_new_actions',pg_catalog.jsonb_build_array('AUTHORISE'),
        'forbidden_inference','Do not invent Candidate evidence'),
      pg_catalog.jsonb_build_object('ui_state','UI-003','server_phase','FIRST_AUTHORISATION_PENDING_MISMATCH',
        'surface','OFFICE','heading','Hours to authorise','heading_source','SERVER',
        'primary_schedule','hours_to_authorise',
        'right_pane_status','Mismatch context',
        'allowed_new_actions',pg_catalog.jsonb_build_array('AUTHORISE','PROTECTED_HOURS_REVIEW'),
        'forbidden_inference','Do not show every submitted matching row'),
      pg_catalog.jsonb_build_object('ui_state','UI-004','server_phase','FIRST_ALTERNATE_HOURS_PENDING',
        'surface','OFFICE','heading','Hours to authorise','heading_source','SERVER',
        'primary_schedule','hours_to_authorise',
        'right_pane_status','Office alternate reason available',
        'allowed_new_actions',pg_catalog.jsonb_build_array('AUTHORISE'),
        'forbidden_inference','Do not call alternate hours paid'),
      pg_catalog.jsonb_build_object('ui_state','UI-005','server_phase','AUTHORISED_NOT_PAID',
        'surface','OFFICE','heading','Approved hours','heading_source','SERVER',
        'primary_schedule','approved',
        'right_pane_status','Authorised; not paid',
        'allowed_new_actions',pg_catalog.jsonb_build_array('EXISTING_LIFECYCLE_ACTIONS_ONLY'),
        'forbidden_inference','Do not say Paid'),
      pg_catalog.jsonb_build_object('ui_state','UI-006','server_phase','PAYMENT_PROCESSING',
        'surface','OFFICE','heading','Approved hours','heading_source','SERVER',
        'primary_schedule','processing',
        'right_pane_status','Payment being processed',
        'allowed_new_actions',pg_catalog.jsonb_build_array(),
        'forbidden_inference','Do not infer payment success'),
      pg_catalog.jsonb_build_object('ui_state','UI-007','server_phase','PAID',
        'surface','OFFICE','heading','Hours paid','heading_source','SERVER',
        'primary_schedule','paid_to_date',
        'right_pane_status','Paid date/status',
        'allowed_new_actions',pg_catalog.jsonb_build_array(),
        'forbidden_inference','Do not derive hours from money or invoice movements'),
      pg_catalog.jsonb_build_object('ui_state','UI-008','server_phase','LATER_CHANGE_PENDING_UNPAID',
        'surface','OFFICE','heading','Currently approved hours','heading_source','SERVER',
        'primary_schedule','currently_approved',
        'right_pane_status','Proposal not current',
        'allowed_new_actions',pg_catalog.jsonb_build_array('APPROVE_UPDATED_HOURS','KEEP_CURRENTLY_APPROVED_HOURS'),
        'forbidden_inference','Do not label proposal approved'),
      pg_catalog.jsonb_build_object('ui_state','UI-009','server_phase','LATER_CHANGE_PENDING_PAID',
        'surface','OFFICE','heading','Hours paid to date','heading_source','SERVER',
        'primary_schedule','paid_to_date',
        'right_pane_status','Adjustment decision pending',
        'allowed_new_actions',pg_catalog.jsonb_build_array('APPROVE_UPDATED_HOURS','KEEP_CURRENTLY_APPROVED_HOURS'),
        'forbidden_inference','Do not rewrite earlier paid history'),
      pg_catalog.jsonb_build_object('ui_state','UI-010','server_phase','LATER_CHANGE_APPROVED_FROZEN',
        'surface','OFFICE','heading','Hours paid to date','heading_source','SERVER',
        'primary_schedule','paid_to_date',
        'right_pane_status','Decision saved; publication pending',
        'allowed_new_actions',pg_catalog.jsonb_build_array(),
        'forbidden_inference','Do not publish into active Draft'),
      pg_catalog.jsonb_build_object('ui_state','UI-011','server_phase','LATER_CHANGE_PUBLISHED_UNSETTLED',
        'surface','OFFICE','heading','Approved hours','heading_source','SERVER',
        'primary_schedule','approved',
        'right_pane_status','Adjustment due or recovery being resolved',
        'allowed_new_actions',pg_catalog.jsonb_build_array('EXISTING_BANKING_STATUS_ONLY'),
        'forbidden_inference','Do not create a new Banking UI type'),
      pg_catalog.jsonb_build_object('ui_state','UI-012','server_phase','ADJUSTMENT_SETTLED',
        'surface','OFFICE','heading','Current paid hours','heading_source','SERVER',
        'primary_schedule','current_paid',
        'right_pane_status','Adjustment settled',
        'allowed_new_actions',pg_catalog.jsonb_build_array(),
        'forbidden_inference','Do not collapse audit events'),
      pg_catalog.jsonb_build_object('ui_state','UI-013','server_phase','CROSS_CONTRACT_PENDING',
        'surface','OFFICE','heading','Currently approved hours','heading_source','SERVER',
        'primary_schedule','currently_approved',
        'right_pane_status','One atomic decision',
        'allowed_new_actions',pg_catalog.jsonb_build_array('APPROVE_UPDATED_HOURS','KEEP_CURRENTLY_APPROVED_HOURS'),
        'forbidden_inference','Do not publish one root alone'),
      pg_catalog.jsonb_build_object('ui_state','UI-014','server_phase','TIMESHEET_AUTHORITY_MATCH',
        'surface','OFFICE','heading','Timesheet hours','heading_source','SERVER',
        'primary_schedule','approved',
        'right_pane_status','Reference state',
        'allowed_new_actions',pg_catalog.jsonb_build_array('AUTHORISE'),
        'forbidden_inference','Do not replace pay hours with import hours'),
      pg_catalog.jsonb_build_object('ui_state','UI-015','server_phase','TIMESHEET_AUTHORITY_MISMATCH',
        'surface','OFFICE','heading','Timesheet hours','heading_source','SERVER',
        'primary_schedule','approved',
        'right_pane_status','Reference withheld',
        'allowed_new_actions',pg_catalog.jsonb_build_array('EXISTING_MANAGER_EMAIL_JOURNEY'),
        'forbidden_inference','Do not query Candidate or use secure manager response page'),
      -- UI-016 and UI-017 are the two bypasses.  The Weekly Source component
      -- must not mount at all, so this owner supplies NO heading: the legacy
      -- Weekly and Daily owners keep theirs unchanged.
      pg_catalog.jsonb_build_object('ui_state','UI-016','server_phase','STANDARD_WEEKLY',
        'surface','OFFICE','heading',null,'heading_source','LEGACY_OWNER',
        'primary_schedule',null,
        'right_pane_status',null,
        'allowed_new_actions',pg_catalog.jsonb_build_array('EXISTING_ACTIONS'),
        'forbidden_inference','Weekly Source component must not mount'),
      pg_catalog.jsonb_build_object('ui_state','UI-017','server_phase','DAILY',
        'surface','OFFICE','heading',null,'heading_source','LEGACY_OWNER',
        'primary_schedule',null,
        'right_pane_status',null,
        'allowed_new_actions',pg_catalog.jsonb_build_array('EXISTING_DAILY_ACTIONS'),
        'forbidden_inference','No Contract or Weekly Source assumption'),
      -- UI-018 is an OVERLAY, not a phase of its own: the matrix gives it a
      -- "Context-dependent hours heading" and a "Context-dependent
      -- approved/paid schedule", which is the heading and schedule of whatever
      -- lifecycle phase the week is actually in.  It is therefore reported in
      -- `overlay_states`, never as `ui_state`, and carries no heading.
      pg_catalog.jsonb_build_object('ui_state','UI-018','server_phase','SOURCE_FIXED_EXPENSE',
        'surface','OFFICE_OVERLAY','heading',null,'heading_source','CONTEXT_PHASE',
        'primary_schedule',null,
        'right_pane_status','Included in four server totals',
        'allowed_new_actions',pg_catalog.jsonb_build_array(),
        'forbidden_inference','Do not create a separate zero-hour expense Timesheet'),
      -- UI-019 to UI-021 are the Candidate surface.  The phase is decided here
      -- so that MyTMS infers nothing either; the payload itself is produced by
      -- the Gate 9 G9-6 owner (WP-11a), which this file only reads.
      pg_catalog.jsonb_build_object('ui_state','UI-019','server_phase','MYTMS_SAME',
        'surface','CANDIDATE','heading','Submitted Timesheet','heading_source','SERVER',
        'primary_schedule','submitted',
        'right_pane_status',null,
        'allowed_new_actions',pg_catalog.jsonb_build_array(),
        'forbidden_inference','Do not show money/remittance'),
      pg_catalog.jsonb_build_object('ui_state','UI-020','server_phase','MYTMS_DIFFERENT',
        'surface','CANDIDATE','heading','Submitted Timesheet plus Approved hours to be paid','heading_source','SERVER',
        'primary_schedule','submitted',
        'right_pane_status',null,
        'allowed_new_actions',pg_catalog.jsonb_build_array(),
        'forbidden_inference','Do not show protected/exceptional/reconciliation terms'),
      pg_catalog.jsonb_build_object('ui_state','UI-021','server_phase','MYTMS_NO_SUBMISSION',
        'surface','CANDIDATE','heading','Timesheet (read-only)','heading_source','SERVER',
        'primary_schedule','approved',
        'right_pane_status',null,
        'allowed_new_actions',pg_catalog.jsonb_build_array(),
        'forbidden_inference','Never label the hours as a Candidate submission; do not show money/rates/invoice/remittance/recovery/protected/reconciliation terms; push only when approved hours later change'),
      pg_catalog.jsonb_build_object('ui_state','UI-022','server_phase','OFFICE_WITHDRAWN_INVOICED_FROM_SOURCE',
        -- `proof/36 section 7`, DEC-061 Option A, character for character.  The
        -- separator is U+00B7 MIDDLE DOT with one plain space on each side, and
        -- it is written as `chr(183)` rather than as a literal because a
        -- non-ASCII literal in a repeatable is only loaded correctly when the
        -- loading client's encoding happens to be UTF-8, and the release runner
        -- does not guarantee that.  A double-encoded heading would be a silently
        -- wrong policy string, which is exactly what this owner exists to
        -- prevent.
        'surface','OFFICE',
        'heading','Not authorised for pay '||pg_catalog.chr(183)||' invoiced from source',
        'heading_source','SERVER',
        'primary_schedule',null,
        'right_pane_status','Existing Office lifecycle heading',
        'allowed_new_actions',pg_catalog.jsonb_build_array(),
        'forbidden_inference','Office Weekly detail and Simple Timesheet only; never shown in MyTMS; no invoice object touched')
    )
  );
$function$;

-- One row of the policy by phase.  A phase this owner does not know is a
-- programming error, not a display fallback, so the caller raises.
create or replace function private.weekly_source_office_lifecycle_row_v1(p_server_phase text)
returns jsonb
language sql
immutable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select policy_row.value
  from pg_catalog.jsonb_array_elements(
    private.weekly_source_office_lifecycle_policy_v1()->'rows') as policy_row(value)
  where policy_row.value->>'server_phase'=p_server_phase;
$function$;

-- ---------------------------------------------------------------------------
-- Schedule envelopes.  Every schedule the projection returns has the same
-- shape, so the browser never has to decide whether a missing figure means
-- "zero" or "not known".  An absent schedule is an explicit
-- `available:false` with a machine reason; it is never an empty array.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_office_schedule_absent_v1(
  p_reason text
) returns jsonb
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'available',false,'reason',p_reason,'source',null,
    'row_count',0,'rows','[]'::jsonb);
$function$;

create or replace function private.weekly_source_office_schedule_from_rows_v1(
  p_rows jsonb,
  p_source text
) returns jsonb
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'available',true,'reason',null,'source',p_source,
    'row_count',pg_catalog.jsonb_array_length(coalesce(p_rows,'[]'::jsonb)),
    'rows',coalesce(p_rows,'[]'::jsonb));
$function$;

-- The complete entitlement, hours only, exactly as WP-06's composer emitted it.
-- No rate, no money and no rounding rule is applied here: the six bucket
-- strings are the composer's own, and `total_hours` is their plain sum, which
-- is the one thing a browser must not be asked to do for itself.  Expense
-- components carry no hours and are reported as their own kind.
create or replace function private.weekly_source_office_schedule_from_components_v1(
  p_components jsonb,
  p_source text
) returns jsonb
language sql
stable
set search_path to 'pg_catalog','pg_temp'
as $function$
  with component_row as (
    select
      element.value as component,
      (element.value->>'component_ordinal')::integer as component_ordinal,
      element.value->>'component_kind' as component_kind,
      case when (element.value->>'work_date') ~ '^\d{4}-\d{2}-\d{2}$'
        then (element.value->>'work_date')::date else null end as work_date,
      coalesce((element.value->>'hours_day')::numeric,0)
        +coalesce((element.value->>'hours_night')::numeric,0)
        +coalesce((element.value->>'hours_sat')::numeric,0)
        +coalesce((element.value->>'hours_sun')::numeric,0)
        +coalesce((element.value->>'hours_bh')::numeric,0) as total_hours
    from pg_catalog.jsonb_array_elements(coalesce(p_components,'[]'::jsonb)) as element(value)
  )
  select pg_catalog.jsonb_build_object(
    'available',true,'reason',null,'source',p_source,
    'row_count',(select pg_catalog.count(*)::integer from component_row),
    'rows',coalesce((
      select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'row_key','entitlement-'||component_row.component_ordinal::text,
        'component_id',component_row.component->>'component_id',
        'component_kind',component_row.component_kind,
        'work_event_id',component_row.component->>'component_member_identity',
        'day_date',case when component_row.work_date is null then null
          else pg_catalog.to_char(component_row.work_date,'Dy FMDD Mon YYYY') end,
        'reference_number',component_row.component->>'reference_number',
        'hours_day',component_row.component->>'hours_day',
        'hours_night',component_row.component->>'hours_night',
        'hours_sat',component_row.component->>'hours_sat',
        'hours_sun',component_row.component->>'hours_sun',
        'hours_bh',component_row.component->>'hours_bh',
        'total_hours',case when component_row.component_kind='WORKED_TIME'
          then pg_catalog.to_char(component_row.total_hours,'FM9999999999990.000000') else null end,
        'expense_code',component_row.component->>'expense_code',
        'state',case when component_row.component_kind='WORKED_TIME'
          then 'ENTITLED' else 'ENTITLED_NON_HOURS' end)
        order by component_row.component_ordinal)
      from component_row),'[]'::jsonb));
$function$;

-- The settled allocation, hours only, exactly as the Gate 9 G9-2 reader
-- returned it.  `Hours paid`, `Hours paid to date` and `current paid hours`
-- are the SAME immutable fact under three policy names, so they are read once
-- and never recomputed; currency never enters this function.
create or replace function private.weekly_source_office_schedule_from_allocation_v1(
  p_allocation jsonb,
  p_source text
) returns jsonb
language sql
stable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select case
    when p_allocation is null
      or pg_catalog.jsonb_typeof(p_allocation)<>'object'
      then pg_catalog.jsonb_build_object('available',false,
        'reason','SETTLEMENT_READER_DID_NOT_ANSWER','source',null,
        'row_count',0,'rows','[]'::jsonb)
    -- A paid figure is rendered ONLY on AVAILABLE.  A week that has not been
    -- paid has no paid figure; it does not have zero (WP-11d D4), so no numeric
    -- member appears here at all -- no `total_hours`, no `hours_by_bucket`.
    -- `reason_detail` is the reader's own sentence and is safe to show.
    when coalesce((p_allocation->>'ok')::boolean,false) is not true
      then pg_catalog.jsonb_build_object('available',false,
        'reason',coalesce(p_allocation->>'reason','SETTLEMENT_EVIDENCE_UNAVAILABLE'),
        'reason_detail',p_allocation->>'reason_detail',
        'unavailable_class',p_allocation->>'unavailable_class',
        'settlement_count',p_allocation->'settlement_count',
        'batch_count',p_allocation->'batch_count',
        'source',null,'row_count',0,'rows','[]'::jsonb)
    when p_allocation->>'state'<>'AVAILABLE'
      then pg_catalog.jsonb_build_object('available',false,
        'reason',coalesce(p_allocation->>'reason',p_allocation->>'state'),
        'reason_detail',p_allocation->>'reason_detail',
        'unavailable_class',p_allocation->>'unavailable_class',
        'settlement_count',p_allocation->'settlement_count',
        'batch_count',p_allocation->'batch_count',
        'source',null,'row_count',0,'rows','[]'::jsonb)
    else pg_catalog.jsonb_build_object(
      'available',true,'reason',null,'source',p_source,
      'batch_count',p_allocation->'batch_count',
      'settlement_count',p_allocation->'settlement_count',
      'first_settled_at_utc',p_allocation->'first_settled_at_utc',
      'last_settled_at_utc',p_allocation->'last_settled_at_utc',
      'total_hours',p_allocation->'total_hours',
      'hours_by_bucket',p_allocation->'hours_by_bucket',
      -- Which settlement RESTATED this figure.  The latest settlement restates
      -- the whole position; settlements do not accumulate (WP-11d F1), so a
      -- screen can show the figure's provenance without implying a sum.
      'position_basis',p_allocation->>'position_basis',
      'position_pay_batch_id',p_allocation->>'position_pay_batch_id',
      'position_settled_at_utc',p_allocation->>'position_settled_at_utc',
      'row_count',pg_catalog.jsonb_array_length(coalesce(p_allocation->'shifts','[]'::jsonb)),
      'rows',coalesce((
        select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'row_key','settled-'||shift.ordinality::text,
          'day_date',case when (shift.value->>'date') ~ '^\d{4}-\d{2}-\d{2}$'
            then pg_catalog.to_char((shift.value->>'date')::date,'Dy FMDD Mon YYYY') else null end,
          'segment_id',shift.value->>'segment_id',
          'break_text',coalesce(shift.value->>'break_mins','0')||' min',
          'total_hours',shift.value->>'hours',
          'hours_day',shift.value->>'hours_day',
          'hours_night',shift.value->>'hours_night',
          'hours_sat',shift.value->>'hours_sat',
          'hours_sun',shift.value->>'hours_sun',
          'hours_bh',shift.value->>'hours_bh',
          'settlements',shift.value->'settlements',
          'state','SETTLED')
          order by shift.ordinality)
        from pg_catalog.jsonb_array_elements(coalesce(p_allocation->'shifts','[]'::jsonb))
          with ordinality as shift(value,ordinality)),'[]'::jsonb))
  end;
$function$;

-- ---------------------------------------------------------------------------
-- Is a Candidate payment being processed for this family right now?
--
-- Read-only over Banking Pay evidence, no lock, bounded, no arithmetic on
-- money.  Fail closed in three-valued logic: a NULL `is_voided` counts as NOT
-- voided and a NULL batch status counts as NOT terminal, because both would
-- otherwise silently suppress a real in-flight payment.  `not in` is never
-- used against a nullable column.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_office_payment_progress_v1(
  p_member_timesheet_ids uuid[]
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_live integer:=0;
  v_voided integer:=0;
  v_unknown_status integer:=0;
  v_batches jsonb:='[]'::jsonb;
begin
  if p_member_timesheet_ids is null
     or pg_catalog.cardinality(p_member_timesheet_ids)=0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'state','UNAVAILABLE','reason','FAMILY_UNRESOLVED',
      'live_item_count',0,'voided_item_count',0,'batches','[]'::jsonb);
  end if;

  select
    pg_catalog.count(*) filter (
      where item_row.is_voided is not true
        and coalesce(batch_row.status,'UNKNOWN') not in ('SETTLED','CANCELLED','FAILED'))::integer,
    pg_catalog.count(*) filter (where item_row.is_voided is true)::integer,
    pg_catalog.count(*) filter (where batch_row.status is null)::integer
    into v_live,v_voided,v_unknown_status
  from public.pay_batch_items as item_row
  join public.pay_batch_candidates as candidate_row
    on candidate_row.id=item_row.pay_batch_candidate_id
  join public.pay_batches as batch_row on batch_row.id=candidate_row.pay_batch_id
  where item_row.timesheet_id=any(p_member_timesheet_ids);

  select coalesce(pg_catalog.jsonb_agg(batch_summary order by
           batch_summary->>'pay_batch_id'),'[]'::jsonb)
    into v_batches
  from (
    select distinct pg_catalog.jsonb_build_object(
      'pay_batch_id',batch_row.id,
      'status',coalesce(batch_row.status,'UNKNOWN'),
      'terminal',coalesce(batch_row.status,'UNKNOWN') in ('SETTLED','CANCELLED','FAILED')
    ) as batch_summary
    from public.pay_batch_items as item_row
    join public.pay_batch_candidates as candidate_row
      on candidate_row.id=item_row.pay_batch_candidate_id
    join public.pay_batches as batch_row on batch_row.id=candidate_row.pay_batch_id
    where item_row.timesheet_id=any(p_member_timesheet_ids)
      and item_row.is_voided is not true
  ) as distinct_batches;

  return pg_catalog.jsonb_build_object(
    'ok',true,
    'state',case when v_live>0 then 'IN_FLIGHT' else 'NONE' end,
    'reason',case when v_unknown_status>0 then 'BATCH_STATUS_NULL_TREATED_AS_IN_FLIGHT' else null end,
    'live_item_count',v_live,'voided_item_count',v_voided,
    'batches',v_batches);
exception
  when undefined_table or undefined_column or insufficient_privilege then
    return pg_catalog.jsonb_build_object(
      'ok',false,'state','UNAVAILABLE','reason','BANKING_PAY_EVIDENCE_UNREADABLE',
      'live_item_count',0,'voided_item_count',0,'batches','[]'::jsonb);
end;
$function$;

-- ---------------------------------------------------------------------------
-- Gate 9 item G9-5, first half.  Invoice movement history, SHOWN SEPARATELY.
--
-- `24 section 15`: "Invoice negative/positive lines never appear as Candidate
-- paid hours."  Nothing in this object is a pay schedule, and it is returned
-- under its own top-level key so it can never be mistaken for one.  It also
-- answers the one question `UI-022` needs: is a finalised source movement of
-- this root on a self-bill?
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_office_invoice_movements_v1(
  p_member_timesheet_ids uuid[]
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_movements jsonb:='[]'::jsonb;
  v_count integer:=0;
  v_bound integer:=0;
  v_ordinary_lines integer:=0;
begin
  if p_member_timesheet_ids is null
     or pg_catalog.cardinality(p_member_timesheet_ids)=0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','FAMILY_UNRESOLVED','invoiced_from_source',false,
      'movement_count',0,'bound_line_count',0,'ordinary_invoice_line_count',0,
      'movements','[]'::jsonb);
  end if;

  select pg_catalog.count(*)::integer into v_count
  from public.weekly_source_billing_movements as movement_row
  where movement_row.invoice_timesheet_id=any(p_member_timesheet_ids);

  if v_count>500 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','MOVEMENT_HISTORY_EXCEEDS_BOUND','invoiced_from_source',null,
      'movement_count',v_count,'bound_line_count',0,'ordinary_invoice_line_count',0,
      'movements','[]'::jsonb);
  end if;

  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'billing_movement_id',movement_row.id,
      'work_event_id',movement_row.work_event_id,
      'movement_role',movement_row.movement_role,
      'source_line_kind',movement_row.source_line_kind,
      'source_profile_kind',movement_row.source_profile_kind,
      'final_revision_id',movement_row.final_revision_id,
      'placement_state',movement_row.placement_state,
      'created_at_utc',movement_row.created_at_utc,
      'binding',case when binding_row.id is null then null
        else pg_catalog.jsonb_build_object(
          'invoice_id',binding_row.invoice_id,
          'invoice_line_id',binding_row.invoice_line_id,
          'presentation_line_id',binding_row.presentation_line_id,
          'binding_version',binding_row.binding_version,
          'state',binding_row.state,
          'bound_at_utc',binding_row.bound_at_utc) end,
      'invoice_status',invoice_row.status,
      'note','Invoice movement history. Never Candidate paid hours.')
      order by movement_row.created_at_utc,movement_row.id),'[]'::jsonb),
    pg_catalog.count(*) filter (where binding_row.id is not null)::integer
    into v_movements,v_bound
  from public.weekly_source_billing_movements as movement_row
  left join public.weekly_source_invoice_line_bindings as binding_row
    on binding_row.billing_movement_id=movement_row.id and binding_row.state='CURRENT'
  left join public.invoices as invoice_row on invoice_row.id=binding_row.invoice_id
  where movement_row.invoice_timesheet_id=any(p_member_timesheet_ids);

  select pg_catalog.count(*)::integer into v_ordinary_lines
  from public.invoice_lines as line_row
  where line_row.timesheet_id=any(p_member_timesheet_ids);

  return pg_catalog.jsonb_build_object(
    'ok',true,'reason',null,
    -- DEC-061 Option A: the finalised source movement stays on the exact
    -- self-bill.  "Invoiced from source" is TRUE when a movement of this root
    -- is currently bound to an invoice line, whatever the invoice's own status;
    -- an unissued DRAFT invoice still holds the line.
    'invoiced_from_source',v_bound>0,
    'movement_count',v_count,'bound_line_count',v_bound,
    'ordinary_invoice_line_count',v_ordinary_lines,
    'movements',v_movements);
exception
  when undefined_table or undefined_column or insufficient_privilege then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','INVOICE_EVIDENCE_UNREADABLE','invoiced_from_source',null,
      'movement_count',0,'bound_line_count',0,'ordinary_invoice_line_count',0,
      'movements','[]'::jsonb);
end;
$function$;


-- The Candidate's own submitted evidence, complete and untouched.  The
-- existing `comparison.submitted_rows` member is deliberately narrowed by
-- `24 section 15` ("only the affected submitted dates"), so it cannot serve as
-- the immutable submitted schedule the matrix asks the server to carry.
create or replace function private.weekly_source_office_schedule_from_actual_v1(
  p_actual_schedule jsonb
) returns jsonb
language sql
stable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select case when pg_catalog.jsonb_typeof(p_actual_schedule)<>'array'
    then pg_catalog.jsonb_build_object('available',false,
      'reason','NO_CANDIDATE_SUBMISSION','source',null,'row_count',0,'rows','[]'::jsonb)
    else pg_catalog.jsonb_build_object(
      'available',true,'reason',null,'source','CANDIDATE_SUBMITTED_SCHEDULE',
      'row_count',pg_catalog.jsonb_array_length(p_actual_schedule),
      'rows',coalesce((
        select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'row_key','submitted-all-'||entry.ordinality::text,
          'day_date',case when (entry.value->>'date') ~ '^\d{4}-\d{2}-\d{2}$'
            then pg_catalog.to_char((entry.value->>'date')::date,'Dy FMDD Mon YYYY') else null end,
          'hours',coalesce(entry.value->>'start',pg_catalog.chr(8212))||'-'||coalesce(entry.value->>'end',pg_catalog.chr(8212)),
          'break_text',coalesce(entry.value->>'break_minutes','0')||' min',
          'state','SUBMITTED')
          order by entry.value->>'date',entry.ordinality)
        from pg_catalog.jsonb_array_elements(p_actual_schedule)
          with ordinality as entry(value,ordinality)),'[]'::jsonb))
  end;
$function$;

-- The one shape every lifecycle answer has.  A resolved phase carries the
-- policy's own heading, status and permitted actions; an unresolved one
-- carries `ok:false`, a null heading and a non-empty `errors` array, so a
-- browser that renders headings from this object can only render nothing.
create or replace function private.weekly_source_office_lifecycle_result_v1(
  p_server_phase text,
  p_overlay_states jsonb,
  p_errors jsonb,
  p_note text
) returns jsonb
language plpgsql
stable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
declare
  v_row jsonb;
  v_overlay_rows jsonb:='[]'::jsonb;
begin
  if p_server_phase is null or pg_catalog.jsonb_array_length(coalesce(p_errors,'[]'::jsonb))>0 then
    return pg_catalog.jsonb_build_object(
      'contract','WEEKLY_SOURCE_OFFICE_LIFECYCLE_V1',
      'ok',false,'ui_state',null,'server_phase',p_server_phase,
      'heading',null,'heading_source','NONE',
      'primary_schedule',null,'right_pane_status',null,
      'permitted_actions','[]'::jsonb,
      'forbidden_inference',null,
      'overlay_states',coalesce(p_overlay_states,'[]'::jsonb),
      'errors',coalesce(p_errors,pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('code','LIFECYCLE_PHASE_UNRESOLVED',
          'detail','No lifecycle phase could be resolved from the evidence.'))),
      'note',p_note);
  end if;

  v_row:=private.weekly_source_office_lifecycle_row_v1(p_server_phase);
  if v_row is null then
    return pg_catalog.jsonb_build_object(
      'contract','WEEKLY_SOURCE_OFFICE_LIFECYCLE_V1',
      'ok',false,'ui_state',null,'server_phase',p_server_phase,
      'heading',null,'heading_source','NONE',
      'primary_schedule',null,'right_pane_status',null,
      'permitted_actions','[]'::jsonb,'forbidden_inference',null,
      'overlay_states',coalesce(p_overlay_states,'[]'::jsonb),
      'errors',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'code','LIFECYCLE_PHASE_NOT_IN_POLICY',
        'detail','The resolver produced a phase the lifecycle policy does not define.',
        'server_phase',p_server_phase)),
      'note',p_note);
  end if;

  select coalesce(pg_catalog.jsonb_agg(overlay_row.value order by overlay_row.value->>'ui_state'),'[]'::jsonb)
    into v_overlay_rows
  from pg_catalog.jsonb_array_elements_text(coalesce(p_overlay_states,'[]'::jsonb)) as requested(value)
  cross join lateral (
    select policy_row.value
    from pg_catalog.jsonb_array_elements(
      private.weekly_source_office_lifecycle_policy_v1()->'rows') as policy_row(value)
    where policy_row.value->>'ui_state'=requested.value) as overlay_row(value);

  return pg_catalog.jsonb_build_object(
    'contract','WEEKLY_SOURCE_OFFICE_LIFECYCLE_V1',
    'ok',true,
    'ui_state',v_row->>'ui_state',
    'server_phase',v_row->>'server_phase',
    'surface',v_row->>'surface',
    'heading',v_row->'heading',
    'heading_source',v_row->>'heading_source',
    'primary_schedule',v_row->'primary_schedule',
    'right_pane_status',v_row->'right_pane_status',
    'permitted_actions',v_row->'allowed_new_actions',
    'forbidden_inference',v_row->>'forbidden_inference',
    'overlay_states',coalesce(p_overlay_states,'[]'::jsonb),
    'overlays',v_overlay_rows,
    'errors','[]'::jsonb,
    'note',p_note);
end;
$function$;

-- ---------------------------------------------------------------------------
-- Gate 9 item G9-3, step 1.  Which source revision is a recorded proposal FOR?
--
-- A `PROPOSED` decision bundle stores digests, not its request, so the source
-- revision is not written down anywhere.  It is nevertheless needed twice: to
-- rebuild the complete proposed entitlement for display, and because the two
-- Office decisions take `final_revision_id` in their command payload.
--
-- It is therefore RESOLVED BY PROOF, never guessed and never taken from a sort
-- order: every final revision this root has a movement or a projection receipt
-- for is re-digested with WP-06's own domain-separated encoder, and the answer
-- is accepted only when EXACTLY ONE candidate reproduces the digest the bundle
-- stored.  Zero or two is a contradiction and fails closed.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_office_proposal_revision_v1(
  p_member_timesheet_ids uuid[],
  p_source_revision_digest bytea
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_candidates integer:=0;
  v_matches uuid[];
begin
  if p_member_timesheet_ids is null
     or pg_catalog.cardinality(p_member_timesheet_ids)=0
     or p_source_revision_digest is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','PROPOSAL_REVISION_INPUT_INVALID','final_revision_id',null,
      'candidate_count',0,'match_count',0);
  end if;

  with candidate_revision as (
    select distinct revision_row.*
    from public.weekly_source_final_revisions as revision_row
    where revision_row.id in (
      select movement_row.final_revision_id
      from public.weekly_source_billing_movements as movement_row
      where movement_row.invoice_timesheet_id=any(p_member_timesheet_ids)
      union
      select receipt_row.final_revision_id
      from public.weekly_source_ordinary_pay_projection_receipts as receipt_row
      where receipt_row.root_timesheet_id=any(p_member_timesheet_ids)
    )
  )
  -- The ONE canonical encoder, not a second hash of the same fact (WP-06c
  -- handoff N1).  The recorder used to write this digest with a
  -- domain-separated sha256 while both installed consumers -- the publication
  -- coordinator and the save-pending owner -- recompute it with
  -- `weekly_source_publication_request_digest_v1` and refuse on any difference,
  -- so nothing could publish at all.  The recorder moved onto the canonical
  -- encoder; this resolver was the last reader of the old form.
  --
  -- The two uuids are cast to `text` on purpose: the canonicaliser emits every
  -- uuid as a lower-case JSON string and `revision_number` as a bare integer,
  -- and the encoder refuses a non-integral number, so the object has to be
  -- built in exactly that form to reproduce the stored value.
  select pg_catalog.count(*)::integer,
         pg_catalog.array_agg(candidate_revision.id) filter (
           where private.weekly_source_publication_request_digest_v1(
             pg_catalog.jsonb_build_object(
               'final_revision_id',candidate_revision.id::text,
               'source_cycle_id',candidate_revision.source_cycle_id::text,
               'revision_number',candidate_revision.revision_number,
               'manifest_hash',pg_catalog.encode(candidate_revision.manifest_hash,'hex'),
               'policy_fingerprint',pg_catalog.encode(candidate_revision.policy_fingerprint,'hex')
             ))=p_source_revision_digest)
    into v_candidates,v_matches
  from candidate_revision;

  if v_candidates>200 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','PROPOSAL_REVISION_CANDIDATES_EXCEED_BOUND',
      'final_revision_id',null,'candidate_count',v_candidates,'match_count',0);
  end if;
  if v_matches is null or pg_catalog.cardinality(v_matches)=0 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','PROPOSAL_REVISION_NOT_IDENTIFIED',
      'final_revision_id',null,'candidate_count',v_candidates,'match_count',0);
  end if;
  if pg_catalog.cardinality(v_matches)>1 then
    return pg_catalog.jsonb_build_object(
      'ok',false,'reason','PROPOSAL_REVISION_AMBIGUOUS',
      'final_revision_id',null,'candidate_count',v_candidates,
      'match_count',pg_catalog.cardinality(v_matches));
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'reason',null,'final_revision_id',v_matches[1],
    'candidate_count',v_candidates,'match_count',1);
end;
$function$;

-- ---------------------------------------------------------------------------
-- Gate 9 item G9-3.  The proposal view: the COMPLETE proposed entitlement
-- beside the COMPLETE currently approved one, the pending change, and the
-- decision reason.
--
-- Nothing economic is computed here.  The proposed entitlement comes from
-- WP-06's composer (`weekly_source_ordinary_projection_current_segments_v1` +
-- `…_current_expenses_v1` + `weekly_source_entitlement_components_v1`), the
-- current one from interface I-7 (`weekly_source_effective_inventory_v1`), and
-- the rebuilt request is then re-digested with WP-06's own canonicaliser and
-- encoder and compared with the digest the bundle stored.  A mismatch is a
-- contradictory projection and is reported as such: the browser is shown NO
-- proposal rather than an unproved one.
--
-- Money is deliberately absent from every row: `24 section 15` forbids adding a
-- per-rate financial breakdown, and the four server totals already exist.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_office_proposal_view_v1(
  p_root_timesheet_id uuid,
  p_member_timesheet_ids uuid[]
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_bundle public.weekly_source_entitlement_decision_bundles%rowtype;
  v_bundle_count integer:=0;
  v_pending public.weekly_source_pending_entitlement_bundles%rowtype;
  v_pending_count integer:=0;
  v_conflicting integer:=0;
  v_bundle_found boolean:=false;
  v_cross_contract jsonb;
  v_answer_bundle_id uuid;
  v_answer_bundle_revision bigint;
  v_answer_bundle_kind text:='SINGLE_ROOT';
  v_answer_decision_id uuid;
  v_revision_answer jsonb;
  v_final_revision_id uuid;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_inventory jsonb;
  v_proposed_components jsonb:='[]'::jsonb;
  v_current_components jsonb:='[]'::jsonb;
  v_request jsonb;
  v_digest bytea;
  v_digest_ok boolean:=false;
  v_state text;
  v_reason text;
  v_change jsonb;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_head_found boolean:=false;
  v_rebuild_error text;
  v_members jsonb:='[]'::jsonb;
  v_primary_ordinal integer;
  v_member_count integer:=1;
  v_source_inventory jsonb;
  v_moved_component_ids uuid[];
begin
  if p_member_timesheet_ids is null
     or pg_catalog.cardinality(p_member_timesheet_ids)=0 then
    return pg_catalog.jsonb_build_object(
      'present',false,'state','UNAVAILABLE','reason','FAMILY_UNRESOLVED');
  end if;

  select pg_catalog.count(*)::integer into v_bundle_count
  from public.weekly_source_entitlement_decision_bundles as bundle_row
  where bundle_row.state='PROPOSED'
    and (bundle_row.source_root_timesheet_id=any(p_member_timesheet_ids)
      or bundle_row.target_root_timesheet_id=any(p_member_timesheet_ids));
  select pg_catalog.count(*)::integer into v_pending_count
  from public.weekly_source_pending_entitlement_bundles as pending_row
  where pending_row.state in ('PENDING','RELEASING')
    and pending_row.member_root_ids && p_member_timesheet_ids;

  -- A saved, frozen decision legitimately keeps its decision bundle in
  -- `PROPOSED` until the release owner commits it, so ONE decision in both
  -- places is not a contradiction: the saved decision is the current fact and
  -- wins.  TWO DIFFERENT decisions on one root is a contradiction, because
  -- neither record can be shown to be the one Office made.  Fail closed there
  -- rather than choose.
  if v_bundle_count>0 and v_pending_count>0 then
    select pg_catalog.count(*)::integer into v_conflicting
    from public.weekly_source_entitlement_decision_bundles as bundle_row
    where bundle_row.state='PROPOSED'
      and (bundle_row.source_root_timesheet_id=any(p_member_timesheet_ids)
        or bundle_row.target_root_timesheet_id=any(p_member_timesheet_ids))
      and not exists(
        select 1 from public.weekly_source_pending_entitlement_bundles as pending_row
        where pending_row.state in ('PENDING','RELEASING')
          and pending_row.member_root_ids && p_member_timesheet_ids
          and pending_row.decision_bundle_id=bundle_row.decision_bundle_id);
    if v_conflicting>0 then
      return pg_catalog.jsonb_build_object(
        'present',false,'state','UNAVAILABLE',
        'reason','PROPOSED_AND_PENDING_BUNDLE_DISAGREE',
        'proposed_bundle_count',v_bundle_count,'pending_bundle_count',v_pending_count,
        'conflicting_proposed_bundles',v_conflicting);
    end if;
    -- One decision, saved and frozen: the pending record is authoritative.
    v_bundle_count:=0;
  end if;
  if v_bundle_count>1 then
    return pg_catalog.jsonb_build_object(
      'present',false,'state','UNAVAILABLE','reason','MULTIPLE_PROPOSED_BUNDLES',
      'proposed_bundle_count',v_bundle_count);
  end if;
  if v_pending_count>1 then
    return pg_catalog.jsonb_build_object(
      'present',false,'state','UNAVAILABLE','reason','MULTIPLE_PENDING_BUNDLES',
      'pending_bundle_count',v_pending_count);
  end if;
  if v_bundle_count=0 and v_pending_count=0 then
    return pg_catalog.jsonb_build_object('present',false,'state','NONE','reason',null);
  end if;

  select head_row.* into v_head
  from public.weekly_source_entitlement_heads as head_row
  where head_row.root_timesheet_id=any(p_member_timesheet_ids)
    and head_row.state='COMMITTED_CURRENT';
  v_head_found:=found;

  v_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
  if coalesce((v_inventory->>'ok')::boolean,false) is not true then
    return pg_catalog.jsonb_build_object(
      'present',false,'state','UNAVAILABLE',
      'reason',coalesce(v_inventory->>'code','EFFECTIVE_INVENTORY_UNAVAILABLE'));
  end if;
  v_current_components:=coalesce(v_inventory->'components','[]'::jsonb);

  if v_pending_count=1 then
    -- A saved, frozen decision.  Its complete request IS stored, so nothing is
    -- rebuilt: the proposed entitlement is read straight out of it.
    select pending_row.* into v_pending
    from public.weekly_source_pending_entitlement_bundles as pending_row
    where pending_row.state in ('PENDING','RELEASING')
      and pending_row.member_root_ids && p_member_timesheet_ids;
    v_state:='FROZEN_PENDING';
    v_answer_bundle_id:=v_pending.decision_bundle_id;
    v_answer_bundle_revision:=v_pending.bundle_revision;
    v_answer_decision_id:=v_pending.decision_id;
    -- The saved request is the ONE place a complete multi-root A-to-B decision
    -- is written down.  `24 section 4.5` bounds a bundle at two roots, so the
    -- member set is read whole and every member is reported.
    v_member_count:=pg_catalog.cardinality(v_pending.member_root_ids);
    v_answer_bundle_kind:=case when v_member_count>1
      then 'CROSS_CONTRACT_A_B' else 'SINGLE_ROOT' end;

    -- The digest is taken in the mode and with the pending id the installed
    -- release owner used when it saved the bundle
    -- (`…pending_entitlement_release_v1`: canonical('DEFERRED', pending id)),
    -- not in the acceptance mode of the decision-bundle row.  The two are
    -- different digests of the same decision and must not be crossed.
    v_digest:=private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_request_canonical_v1(
        v_pending.request_json,'DEFERRED',v_pending.id));
    v_digest_ok:=v_digest=v_pending.request_digest;
    v_final_revision_id:=
      (v_pending.request_json#>>'{financial_request,source_revision,final_revision_id}')::uuid;
    if not v_digest_ok then
      v_reason:='PENDING_REQUEST_DIGEST_MISMATCH';
    end if;

    -- Which member is THIS screen's root?  The primary members of the answer
    -- are that root's, so the Office user sees the week in front of them; every
    -- member is also reported in `members`, because the decision is atomic.
    select element.ordinality::integer into v_primary_ordinal
    from pg_catalog.unnest(v_pending.member_root_ids) with ordinality as element(value,ordinality)
    where element.value=any(p_member_timesheet_ids)
    order by element.ordinality
    limit 1;
    if v_primary_ordinal is null then
      v_reason:=coalesce(v_reason,'PENDING_REQUEST_MEMBER_NOT_FOUND_FOR_ROOT');
      v_primary_ordinal:=1;
    end if;
    v_proposed_components:=coalesce((
      select entitlement.value->'components'
      from pg_catalog.jsonb_array_elements(
        v_pending.request_json#>'{financial_request,member_entitlements}') as entitlement(value)
      where (entitlement.value->>'root_ordinal')::text=v_primary_ordinal::text),
      '[]'::jsonb);

    -- Every member, with its own proposed entitlement from the saved request and
    -- its own currently approved entitlement from interface I-7 for that root.
    -- I-7 is per root, so a two-root decision is two I-7 reads, never one
    -- widened read and never a second way of computing a figure.
    select coalesce(pg_catalog.jsonb_agg(member_row order by (member_row->>'root_ordinal')::integer),
                    '[]'::jsonb)
      into v_members
    from (
      select pg_catalog.jsonb_build_object(
        'root_ordinal',element.ordinality::integer,
        'root_timesheet_id',element.value,
        'is_requested_root',element.value=any(p_member_timesheet_ids),
        'family_booking_id',v_pending.member_family_booking_ids[element.ordinality::integer],
        'root_timesheet_version',v_pending.member_root_versions[element.ordinality::integer],
        'contract_id',(
          select choice.value->>'contract_id'
          from pg_catalog.jsonb_array_elements(
            v_pending.request_json#>'{financial_request,contract_choices}') as choice(value)
          where (choice.value->>'root_ordinal')::text=element.ordinality::text),
        'authority_kind',member_entitlement.authority_kind,
        'proposed_component_count',pg_catalog.jsonb_array_length(member_entitlement.components),
        'proposed_certified_zero',pg_catalog.jsonb_array_length(member_entitlement.components)=0,
        'proposed',private.weekly_source_office_schedule_from_components_v1(
          member_entitlement.components,'WEEKLY_SOURCE_ENTITLEMENT_COMPOSER'),
        'currently_approved',case
          when coalesce((member_inventory.value->>'ok')::boolean,false) is not true
            then private.weekly_source_office_schedule_absent_v1(
              coalesce(member_inventory.value->>'code','EFFECTIVE_INVENTORY_UNAVAILABLE'))
          else private.weekly_source_office_schedule_from_components_v1(
            coalesce(member_inventory.value->'components','[]'::jsonb),
            'EFFECTIVE_INVENTORY_'||coalesce(member_inventory.value->>'authority','UNKNOWN')) end,
        'currently_approved_authority',member_inventory.value->>'authority',
        'currently_approved_head_id',member_inventory.value->>'head_id',
        'currently_approved_component_count',member_inventory.value->'component_count'
      ) as member_row
      from pg_catalog.unnest(v_pending.member_root_ids)
        with ordinality as element(value,ordinality)
      cross join lateral (
        select coalesce(entitlement.value->'components','[]'::jsonb) as components,
               entitlement.value->>'authority_kind' as authority_kind
        from pg_catalog.jsonb_array_elements(
          v_pending.request_json#>'{financial_request,member_entitlements}') as entitlement(value)
        where (entitlement.value->>'root_ordinal')::text=element.ordinality::text
      ) as member_entitlement
      cross join lateral (
        select private.weekly_source_effective_inventory_v1(element.value) as value
      ) as member_inventory
    ) as members;

    if v_member_count>1 then
      -- `24 section 4.5`: one atomic A-to-B decision over both roots.  Every
      -- identity here comes from the SAVED REQUEST and the pending bundle, not
      -- from a second reading of the world.
      v_cross_contract:=pg_catalog.jsonb_build_object(
        'source_root_timesheet_id',v_pending.member_root_ids[1],
        'source_root_family_booking_id',v_pending.member_family_booking_ids[1],
        'source_contract_id',v_members->0->>'contract_id',
        'target_root_timesheet_id',v_pending.member_root_ids[2],
        'target_root_family_booking_id',v_pending.member_family_booking_ids[2],
        'target_contract_id',v_members->1->>'contract_id',
        'whole_root_review_required',
          v_pending.request_json#>'{control,whole_root_office_review}' is not null
          and v_pending.request_json#>'{control,whole_root_office_review}'<>'null'::jsonb,
        'whole_root_reviewed_at_utc',
          v_pending.request_json#>>'{control,whole_root_office_review,reviewed_at_utc}',
        'atomic','Both roots are decided by one action; neither publishes alone.');
    end if;
    -- The decision reason is the same fact on both paths: the source revision
    -- the change came from.
    select revision_row.* into v_revision
    from public.weekly_source_final_revisions as revision_row
    where revision_row.id=v_final_revision_id;
  else
    select bundle_row.* into v_bundle
    from public.weekly_source_entitlement_decision_bundles as bundle_row
    where bundle_row.state='PROPOSED'
      and (bundle_row.source_root_timesheet_id=any(p_member_timesheet_ids)
        or bundle_row.target_root_timesheet_id=any(p_member_timesheet_ids));
    v_bundle_found:=found;
    if v_bundle_found then
      v_answer_bundle_id:=v_bundle.decision_bundle_id;
      v_answer_bundle_revision:=v_bundle.bundle_revision;
      v_answer_bundle_kind:=v_bundle.bundle_kind;
      v_answer_decision_id:=v_bundle.decision_id;
    end if;
    -- Captured here, where the record is known to be assigned: an unassigned
    -- PL/pgSQL record raises on ANY field reference, and a CASE guard is not a
    -- reliable way to avoid evaluating one.
    if v_bundle_found and v_bundle.bundle_kind='CROSS_CONTRACT_A_B' then
      v_cross_contract:=pg_catalog.jsonb_build_object(
        'source_root_timesheet_id',v_bundle.source_root_timesheet_id,
        'source_contract_id',v_bundle.source_contract_id,
        'source_root_family_booking_id',v_bundle.source_root_family_booking_id,
        'target_root_timesheet_id',v_bundle.target_root_timesheet_id,
        'target_contract_id',v_bundle.target_contract_id,
        'target_root_family_booking_id',v_bundle.target_root_family_booking_id,
        'whole_root_review_required',v_bundle.whole_root_review_required,
        'whole_root_reviewed_at_utc',v_bundle.whole_root_reviewed_at_utc,
        'atomic','Both roots are decided by one action; neither publishes alone.');
    end if;
    v_state:=case when v_bundle.bundle_kind='CROSS_CONTRACT_A_B'
      then 'PROPOSED_CROSS_CONTRACT' else 'PROPOSED' end;

    -- A cross-Contract A-to-B decision is ONE atomic two-root decision
    -- (`24 section 4.5`), and its complete I-3 request names both members.  It
    -- is rebuilt through WP-06's OWN two-root composer
    -- (`…proposal_cross_contract_request_v1`, WP-06c) exactly as the
    -- single-root branch is rebuilt through WP-06's single-root composer, and
    -- displayed only when the rebuilt request re-digests to the digest the
    -- bundle stored.  This file composes nothing of its own.
    --
    -- One limit, stated on the screen rather than hidden.  `control.
    -- moved_component_ids` is control scope: it is in no column of the decision
    -- bundle and in no digest (WP-06c handoff N3), so a reader has to supply the
    -- move set.  For the WHOLE-entitlement move -- the case `24 section 4.5` is
    -- written for -- the move set is exactly interface I-7's component ids for
    -- the source root, so it is derivable and the digest matches.  For a PARTIAL
    -- move, where some components transfer and others stay, the set is not
    -- recoverable from anything persisted, the digest will not match, and the
    -- proposal is refused with a reason that says so.  A wrong guess here would
    -- put a fabricated entitlement in front of an Office user.
    if v_bundle_found and v_bundle.bundle_kind='CROSS_CONTRACT_A_B' then
      -- The currently approved half is stated first and independently of the
      -- rebuild, because the matrix asks `UI-013` for an "old-root and new-root
      -- complete bundle summary" whether or not the proposal can be stated.
      -- Interface I-7 answers per root, so two roots are two I-7 reads.
      v_member_count:=2;
      select coalesce(pg_catalog.jsonb_agg(member_row order by (member_row->>'root_ordinal')::integer),
                      '[]'::jsonb)
        into v_members
      from (
        select pg_catalog.jsonb_build_object(
          'root_ordinal',member.root_ordinal,
          'root_timesheet_id',member.root_timesheet_id,
          'is_requested_root',member.root_timesheet_id=any(p_member_timesheet_ids),
          'family_booking_id',member.family_booking_id,
          'root_timesheet_version',null,
          'contract_id',member.contract_id,
          'authority_kind',null,
          'proposed_component_count',null,
          'proposed_certified_zero',null,
          'proposed',private.weekly_source_office_schedule_absent_v1(
            'PROPOSAL_NOT_YET_REBUILT'),
          'currently_approved',case
            when coalesce((member_inventory.value->>'ok')::boolean,false) is not true
              then private.weekly_source_office_schedule_absent_v1(
                coalesce(member_inventory.value->>'code','EFFECTIVE_INVENTORY_UNAVAILABLE'))
            else private.weekly_source_office_schedule_from_components_v1(
              coalesce(member_inventory.value->'components','[]'::jsonb),
              'EFFECTIVE_INVENTORY_'||coalesce(member_inventory.value->>'authority','UNKNOWN')) end,
          'currently_approved_authority',member_inventory.value->>'authority',
          'currently_approved_head_id',member_inventory.value->>'head_id',
          'currently_approved_component_count',member_inventory.value->'component_count'
        ) as member_row
        from (values
          (1,v_bundle.source_root_timesheet_id,v_bundle.source_root_family_booking_id,
             v_bundle.source_contract_id),
          (2,v_bundle.target_root_timesheet_id,v_bundle.target_root_family_booking_id,
             v_bundle.target_contract_id)
        ) as member(root_ordinal,root_timesheet_id,family_booking_id,contract_id)
        cross join lateral (
          select private.weekly_source_effective_inventory_v1(member.root_timesheet_id) as value
        ) as member_inventory
      ) as members;

      -- The move set, derived rather than guessed.  A WHOLE-entitlement move is
      -- the source root's complete I-7 inventory, and since the finance approver
      -- ruled PARTIAL Contract-to-Contract moves out of scope for this release
      -- -- WP-06c refuses one at the composer, the recorder and the coordinator,
      -- before anything is written -- it is also the only move set any stored
      -- decision can carry.  Deriving it is therefore not an assumption about
      -- this bundle; it is the release rule.  The digest comparison below is
      -- still what proves it, and still refuses rather than displays.
      v_source_inventory:=private.weekly_source_effective_inventory_v1(
        v_bundle.source_root_timesheet_id);
      if coalesce((v_source_inventory->>'ok')::boolean,false) is not true then
        v_reason:='PROPOSAL_CROSS_CONTRACT_SOURCE_INVENTORY_UNAVAILABLE';
        v_rebuild_error:=coalesce(v_source_inventory->>'code',
          'Interface I-7 could not state the old root current entitlement.');
      else
        select pg_catalog.array_agg((component.value->>'component_id')::uuid
                 order by component.value->>'component_id')
          into v_moved_component_ids
        from pg_catalog.jsonb_array_elements(
          coalesce(v_source_inventory->'components','[]'::jsonb)) as component(value);
        v_moved_component_ids:=coalesce(v_moved_component_ids,array[]::uuid[]);
      end if;

      if v_reason is null then
        v_revision_answer:=private.weekly_source_office_proposal_revision_v1(
          p_member_timesheet_ids,v_bundle.source_revision_digest);
        if coalesce((v_revision_answer->>'ok')::boolean,false) is not true then
          v_reason:=v_revision_answer->>'reason';
        else
          v_final_revision_id:=(v_revision_answer->>'final_revision_id')::uuid;
          select revision_row.* into v_revision
          from public.weekly_source_final_revisions as revision_row
          where revision_row.id=v_final_revision_id;

          -- WP-06's OWN two-root composer.  Every argument comes from the bundle
          -- row; only the move set is derived, and only for the whole-entitlement
          -- case (WP-06c handoff N2 and N3).
          begin
            v_request:=private.weekly_source_entitlement_proposal_cross_contract_request_v1(
              v_bundle.source_root_timesheet_id,
              v_bundle.target_root_timesheet_id,
              v_final_revision_id,
              v_bundle.decision_bundle_id,
              v_bundle.bundle_revision,
              v_bundle.proposed_head_ids[1],
              v_bundle.proposed_head_ids[2],
              v_bundle.decision_id,
              v_moved_component_ids,
              'LOCKED_FINAL_SOURCE','LOCKED_FINAL_SOURCE','UNCHANGED','OFFICE_SELECTED',
              -- I-3 section 5.4: a genuinely new B root is authorised BY this
              -- bundle, and the instruction carries the actor.  The only
              -- defensible source for a read-only rebuild is the accepted
              -- decision's own actor, which `proof/32 section 2` says is copied
              -- once from the immutable accepted Office decision and never taken
              -- from a caller.  It is a derivation from persisted data, not an
              -- invention.
              --
              -- What the digest below does NOT do is prove it.  Only
              -- `financial_request` is canonicalised (I-3 section 0), so `control`
              -- -- this actor included -- is outside the digest, and composing
              -- the same move with a different authorising actor yields the same
              -- digest.  The WP-11b verifier asserts that rather than assuming
              -- it.  The consequence is bounded: this rebuilt
              -- `target_root_authorisation` is never read out of this projection
              -- and never reaches a screen, and everything that DOES reach a
              -- screen -- the source revision, the contract choices and both
              -- roots' entitlement vectors -- is inside the digest and is
              -- proved.  Persisting the actor, or digesting CONTROL scope, is a
              -- decision for WP-06c and I-3, not for a read projection.
              v_bundle.decided_by_user_id,
              -- The whole-root review is fully persisted (WP-06c generalised the
              -- recorder to write both columns), so it is read, not assumed.
              v_bundle.whole_root_reviewed_by_user_id,
              v_bundle.whole_root_reviewed_at_utc);
          exception when others then
            v_request:=null;
            v_rebuild_error:=pg_catalog.concat_ws(': ',sqlstate,sqlerrm);
          end;
          if v_request is null then
            v_reason:='PROPOSAL_REQUEST_REBUILD_FAILED';
          else
            begin
              v_digest:=private.weekly_source_publication_request_digest_v1(
                private.weekly_source_publication_request_canonical_v1(
                  v_request,'IMMEDIATE',null));
              v_digest_ok:=v_digest=v_bundle.request_digest;
            exception when others then
              v_digest_ok:=false;
              v_rebuild_error:=pg_catalog.concat_ws(': ',sqlstate,sqlerrm);
            end;
            if not v_digest_ok then
              -- The rebuilt request does not re-digest to the one stored with
              -- the accepted decision, so the proposed entitlement is refused
              -- BY NAME rather than shown or left blank.
              --
              -- The reason CODE is historical and is kept stable for the
              -- frontend.  What it means today is narrower than its name: a
              -- PARTIAL move is no longer a possible cause, because WP-06c
              -- refuses one at the composer, the recorder and the coordinator
              -- before anything is written (the finance approver's Round-5
              -- ruling, Part E), so no accepted decision can be a partial move.
              --
              -- Only `financial_request` is canonicalised into the digest;
              -- `control` is deliberately outside it (I-3 section 0), so
              -- neither `control.moved_component_ids` nor the target root's
              -- authorisation actor can cause this disagreement.  What is left
              -- is the position itself: the decision was accepted against
              -- entitlement vectors that the roots no longer hold.  The
              -- proposal is stale, and a figure computed from today's position
              -- would be a figure the decision was never taken against.
              v_reason:='PROPOSAL_CROSS_CONTRACT_MOVE_SET_NOT_RECOVERABLE';
              v_rebuild_error:=coalesce(v_rebuild_error,
                'The proposed entitlement is not shown because it cannot be '
                ||'proved. Rebuilding the accepted decision from what is stored '
                ||'today produces a different request from the one that was '
                ||'accepted, so any figure shown here would be a guess. Moving '
                ||'part of an entitlement is not the cause: a partial '
                ||'Contract-to-Contract move is refused when the decision is '
                ||'composed, so no accepted decision can be one. The usual cause '
                ||'is that one of the two Contracts no longer holds the '
                ||'entitlement the decision was taken against, which makes the '
                ||'decision stale and means it should be taken again. Nothing '
                ||'else on this screen is affected: the decision, both Contracts '
                ||'and both currently approved positions are shown as usual.');
            end if;
          end if;
        end if;
      end if;

      -- Whatever the outcome, the members now carry each root's proposed half
      -- from the rebuilt request when it was proved, and an explicit
      -- unavailability with the reason when it was not.
      if v_reason is null then
        select coalesce(pg_catalog.jsonb_agg(
                 member_existing.value||pg_catalog.jsonb_build_object(
                   'authority_kind',member_entitlement.value->>'authority_kind',
                   'proposed_component_count',
                     pg_catalog.jsonb_array_length(
                       coalesce(member_entitlement.value->'components','[]'::jsonb)),
                   'proposed_certified_zero',
                     pg_catalog.jsonb_array_length(
                       coalesce(member_entitlement.value->'components','[]'::jsonb))=0,
                   'proposed',private.weekly_source_office_schedule_from_components_v1(
                     coalesce(member_entitlement.value->'components','[]'::jsonb),
                     'WEEKLY_SOURCE_ENTITLEMENT_COMPOSER'))
                 order by (member_existing.value->>'root_ordinal')::integer),'[]'::jsonb)
          into v_members
        from pg_catalog.jsonb_array_elements(v_members) as member_existing(value)
        join pg_catalog.jsonb_array_elements(
          v_request#>'{financial_request,member_entitlements}') as member_entitlement(value)
          on (member_entitlement.value->>'root_ordinal')::text
             =member_existing.value->>'root_ordinal';
        select (member_row.value->>'root_ordinal')::integer into v_primary_ordinal
        from pg_catalog.jsonb_array_elements(v_members) as member_row(value)
        where coalesce((member_row.value->>'is_requested_root')::boolean,false)
        order by (member_row.value->>'root_ordinal')::integer
        limit 1;
        v_proposed_components:=coalesce((
          select entitlement.value->'components'
          from pg_catalog.jsonb_array_elements(
            v_request#>'{financial_request,member_entitlements}') as entitlement(value)
          where (entitlement.value->>'root_ordinal')::text
                =coalesce(v_primary_ordinal,1)::text),'[]'::jsonb);
      else
        select coalesce(pg_catalog.jsonb_agg(
                 member_existing.value||pg_catalog.jsonb_build_object(
                   'proposed',private.weekly_source_office_schedule_absent_v1(v_reason))
                 order by (member_existing.value->>'root_ordinal')::integer),'[]'::jsonb)
          into v_members
        from pg_catalog.jsonb_array_elements(v_members) as member_existing(value);
      end if;
    end if;

    if v_reason is null and v_answer_bundle_kind<>'CROSS_CONTRACT_A_B' then
    v_revision_answer:=private.weekly_source_office_proposal_revision_v1(
      p_member_timesheet_ids,v_bundle.source_revision_digest);
    if coalesce((v_revision_answer->>'ok')::boolean,false) is not true then
      v_reason:=v_revision_answer->>'reason';
    else
      v_final_revision_id:=(v_revision_answer->>'final_revision_id')::uuid;
      select revision_row.* into v_revision
      from public.weekly_source_final_revisions as revision_row
      where revision_row.id=v_final_revision_id;

      -- WP-06's composer, called exactly as WP-06's own later-change owner
      -- calls it.  This file performs no economic step of its own.
      v_proposed_components:=private.weekly_source_entitlement_components_v1(
        private.weekly_source_ordinary_projection_current_segments_v1(
          v_bundle.source_root_timesheet_id,v_final_revision_id),
        private.weekly_source_ordinary_projection_current_expenses_v1(
          v_bundle.source_root_timesheet_id,v_final_revision_id));

      -- Proof, not assertion: rebuild the request through WP-06's builder and
      -- re-digest it with the one canonical encoder.  Only a byte-equal digest
      -- lets the proposal be displayed.
      --
      -- A builder that REFUSES is reported separately from a digest that
      -- DISAGREES, because the two mean different things: the first says the
      -- server cannot state the proposal at all, the second says the stored
      -- proposal and the current server facts contradict each other.  Collapsing
      -- them would hide an owner defect behind a data message.
      begin
        v_request:=private.weekly_source_entitlement_proposal_request_v1(
          v_bundle.source_root_timesheet_id,v_final_revision_id,'LOCKED_FINAL_SOURCE',
          v_bundle.decision_bundle_id,v_bundle.bundle_revision,
          v_bundle.proposed_head_ids[1],v_bundle.decision_id,v_proposed_components);
      exception when others then
        v_request:=null;
        v_rebuild_error:=pg_catalog.concat_ws(': ',sqlstate,sqlerrm);
      end;
      if v_request is null then
        v_reason:='PROPOSAL_REQUEST_REBUILD_FAILED';
      else
        begin
          v_digest:=private.weekly_source_publication_request_digest_v1(
            private.weekly_source_publication_request_canonical_v1(v_request,'IMMEDIATE',null));
          v_digest_ok:=v_digest=v_bundle.request_digest;
        exception when others then
          v_digest_ok:=false;
          v_rebuild_error:=pg_catalog.concat_ws(': ',sqlstate,sqlerrm);
        end;
        if not v_digest_ok then
          v_reason:='PROPOSAL_REQUEST_DIGEST_MISMATCH';
        end if;
      end if;
    end if;
    end if;
  end if;

  if v_reason is not null then
    -- The bundle exists and the screen must say so, but its content is not
    -- proved, so no entitlement and no decision payload is offered.
    return pg_catalog.jsonb_build_object(
      'present',true,'state','UNAVAILABLE','reason',v_reason,
      'detail',v_rebuild_error,
      'final_revision_id',v_final_revision_id,
      'decision_bundle_id',v_answer_bundle_id,
      'bundle_revision',v_answer_bundle_revision,
      'bundle_kind',v_answer_bundle_kind,
      'pending_bundle_id',v_pending.id,
      'pending_state',v_pending.state,
      'member_count',v_member_count,
      'members',v_members,
      'cross_contract',v_cross_contract,
      'proposed',private.weekly_source_office_schedule_absent_v1(v_reason),
      'currently_approved',private.weekly_source_office_schedule_from_components_v1(
        v_current_components,'EFFECTIVE_INVENTORY_'||coalesce(v_inventory->>'authority','UNKNOWN')),
      'decision',null);
  end if;

  -- One member on the single-root path, so the shape a caller reads is the same
  -- whichever path produced it.
  if pg_catalog.jsonb_array_length(v_members)=0 then
    v_members:=pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'root_ordinal',1,
      'root_timesheet_id',p_root_timesheet_id,
      'is_requested_root',true,
      'family_booking_id',null,
      'root_timesheet_version',null,
      'contract_id',case when v_bundle_found then v_bundle.source_contract_id else null end,
      'authority_kind','LOCKED_FINAL_SOURCE',
      'proposed_component_count',pg_catalog.jsonb_array_length(v_proposed_components),
      'proposed_certified_zero',pg_catalog.jsonb_array_length(v_proposed_components)=0,
      'proposed',private.weekly_source_office_schedule_from_components_v1(
        v_proposed_components,'WEEKLY_SOURCE_ENTITLEMENT_COMPOSER'),
      'currently_approved',private.weekly_source_office_schedule_from_components_v1(
        v_current_components,'EFFECTIVE_INVENTORY_'||coalesce(v_inventory->>'authority','UNKNOWN')),
      'currently_approved_authority',v_inventory->>'authority',
      'currently_approved_head_id',v_inventory->>'head_id',
      'currently_approved_component_count',v_inventory->'component_count'));
    v_member_count:=1;
  end if;

  -- The pending change, by immutable component identity.  This is an identity
  -- comparison of two server-produced inventories; it invents no value.
  select pg_catalog.jsonb_build_object(
    'added',coalesce(added.list,'[]'::jsonb),
    'removed',coalesce(removed.list,'[]'::jsonb),
    'changed',coalesce(changed.list,'[]'::jsonb),
    'unchanged_count',coalesce(unchanged.total,0),
    'added_count',pg_catalog.jsonb_array_length(coalesce(added.list,'[]'::jsonb)),
    'removed_count',pg_catalog.jsonb_array_length(coalesce(removed.list,'[]'::jsonb)),
    'changed_count',pg_catalog.jsonb_array_length(coalesce(changed.list,'[]'::jsonb)))
    into v_change
  from
    (select pg_catalog.jsonb_agg(proposed.value->>'component_id'
       order by proposed.value->>'component_id') as list
     from pg_catalog.jsonb_array_elements(v_proposed_components) as proposed(value)
     where not exists(select 1
       from pg_catalog.jsonb_array_elements(v_current_components) as current_component(value)
       where current_component.value->>'component_id'=proposed.value->>'component_id')) as added,
    (select pg_catalog.jsonb_agg(current_component.value->>'component_id'
       order by current_component.value->>'component_id') as list
     from pg_catalog.jsonb_array_elements(v_current_components) as current_component(value)
     where not exists(select 1
       from pg_catalog.jsonb_array_elements(v_proposed_components) as proposed(value)
       where proposed.value->>'component_id'=current_component.value->>'component_id')) as removed,
    (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
         'component_id',proposed.value->>'component_id',
         'work_date',proposed.value->>'work_date',
         'currently_approved_total_hours',
           coalesce((current_component.value->>'hours_day')::numeric,0)
           +coalesce((current_component.value->>'hours_night')::numeric,0)
           +coalesce((current_component.value->>'hours_sat')::numeric,0)
           +coalesce((current_component.value->>'hours_sun')::numeric,0)
           +coalesce((current_component.value->>'hours_bh')::numeric,0),
         'proposed_total_hours',
           coalesce((proposed.value->>'hours_day')::numeric,0)
           +coalesce((proposed.value->>'hours_night')::numeric,0)
           +coalesce((proposed.value->>'hours_sat')::numeric,0)
           +coalesce((proposed.value->>'hours_sun')::numeric,0)
           +coalesce((proposed.value->>'hours_bh')::numeric,0))
         order by proposed.value->>'component_id') as list
     from pg_catalog.jsonb_array_elements(v_proposed_components) as proposed(value)
     join pg_catalog.jsonb_array_elements(v_current_components) as current_component(value)
       on current_component.value->>'component_id'=proposed.value->>'component_id'
     where current_component.value->'hours_day' is distinct from proposed.value->'hours_day'
        or current_component.value->'hours_night' is distinct from proposed.value->'hours_night'
        or current_component.value->'hours_sat' is distinct from proposed.value->'hours_sat'
        or current_component.value->'hours_sun' is distinct from proposed.value->'hours_sun'
        or current_component.value->'hours_bh' is distinct from proposed.value->'hours_bh') as changed,
    (select pg_catalog.count(*)::integer as total
     from pg_catalog.jsonb_array_elements(v_proposed_components) as proposed(value)
     join pg_catalog.jsonb_array_elements(v_current_components) as current_component(value)
       on current_component.value->>'component_id'=proposed.value->>'component_id'
     where current_component.value->'hours_day' is not distinct from proposed.value->'hours_day'
       and current_component.value->'hours_night' is not distinct from proposed.value->'hours_night'
       and current_component.value->'hours_sat' is not distinct from proposed.value->'hours_sat'
       and current_component.value->'hours_sun' is not distinct from proposed.value->'hours_sun'
       and current_component.value->'hours_bh' is not distinct from proposed.value->'hours_bh') as unchanged;

  return pg_catalog.jsonb_build_object(
    'present',true,'state',v_state,'reason',null,
    'request_digest_verified',true,
    'decision_bundle_id',v_answer_bundle_id,
    'bundle_revision',v_answer_bundle_revision,
    'bundle_kind',v_answer_bundle_kind,
    'decision_id',v_answer_decision_id,
    'final_revision_id',v_final_revision_id,
    'pending_bundle_id',v_pending.id,
    'pending_state',v_pending.state,
    'cross_contract',v_cross_contract,
    'member_count',v_member_count,
    'members',v_members,
    'primary_root_ordinal',coalesce(v_primary_ordinal,1),
    'proposed',private.weekly_source_office_schedule_from_components_v1(
      v_proposed_components,'WEEKLY_SOURCE_ENTITLEMENT_COMPOSER'),
    'proposed_component_count',pg_catalog.jsonb_array_length(v_proposed_components),
    'proposed_certified_zero',pg_catalog.jsonb_array_length(v_proposed_components)=0,
    'currently_approved',private.weekly_source_office_schedule_from_components_v1(
      v_current_components,'EFFECTIVE_INVENTORY_'||coalesce(v_inventory->>'authority','UNKNOWN')),
    'currently_approved_component_count',v_inventory->'component_count',
    'currently_approved_authority',v_inventory->>'authority',
    'currently_approved_head_id',v_inventory->>'head_id',
    'currently_approved_authority_kind',case when v_head_found then v_head.authority_kind else null end,
    'change',v_change,
    'decision_reason',pg_catalog.jsonb_build_object(
      'source_change',case when v_revision.id is null then null
        else pg_catalog.jsonb_build_object(
          'final_revision_id',v_revision.id,
          'revision_number',v_revision.revision_number,
          'reason',v_revision.reason,
          'finalised_at_utc',v_revision.finalised_at_utc,
          'finalised_by_user_id',v_revision.finalised_by_user_id) end,
      'current_position',pg_catalog.jsonb_build_object(
        'authority',v_inventory->>'authority',
        'authority_kind',case when v_head_found then v_head.authority_kind else null end,
        'head_id',v_inventory->>'head_id',
        'decided_by_user_id',case when v_head_found then v_head.decided_by_user_id else null end,
        'committed_at_utc',case when v_head_found then v_head.committed_at_utc else null end)),
    'decision',pg_catalog.jsonb_build_object(
      'endpoint','/api/weekly-source/v1/commands',
      'owner','public.weekly_source_later_change_decide_atomic_v1',
      'schema_version','WEEKLY_SOURCE_LATER_CHANGE_DECISION_V1',
      'actions',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('action','APPROVE_UPDATED_HOURS',
          'label','Approve updated hours','reason_required',false),
        pg_catalog.jsonb_build_object('action','KEEP_CURRENTLY_APPROVED_HOURS',
          'label','Keep currently approved hours','reason_required',false)),
      'command_payload',pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_LATER_CHANGE_DECISION_V1',
        'decision_bundle_id',v_answer_bundle_id,
        'bundle_revision',v_answer_bundle_revision,
        'root_timesheet_id',p_root_timesheet_id,
        'final_revision_id',v_final_revision_id)));
end;
$function$;

-- ---------------------------------------------------------------------------
-- Gate 9 item G9-1.  The resolver.
--
-- One function decides, for one root, which of the 22 matrix rows the week is
-- in.  Every input is a server fact and every contradiction fails closed with
-- a named reason: the browser is never handed a phase that the evidence does
-- not support, and it never has to break a tie itself.
--
-- `p_facts` carries the facts the presentation owner has already established
-- under its own reads, so they are established once:
--   applicable, scope, authority_mode, comparison_state, protected_wait_items,
--   source_fixed_expenses, submitted_available.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_office_lifecycle_phase_v1(
  p_root_timesheet_id uuid,
  p_facts jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_members uuid[];
  v_canonical uuid;
  v_member_count integer:=0;
  v_authorisation_state text;
  v_live_count integer:=0;
  v_withdrawn_count integer:=0;
  v_head_count integer:=0;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_allocation jsonb;
  v_payment jsonb;
  v_invoices jsonb;
  v_proposal jsonb;
  v_phase text;
  v_ui_state text;
  v_overlays jsonb:='[]'::jsonb;
  v_errors jsonb:='[]'::jsonb;
  v_policy_row jsonb;
  v_settled boolean:=false;
  v_position_withheld boolean:=false;
  v_batch_count integer:=0;
  v_settlement_count integer:=0;
  v_comparison text;
  v_authority_mode text;
  v_protected_items integer:=0;
begin
  v_comparison:=p_facts->>'comparison_state';
  v_authority_mode:=p_facts->>'authority_mode';
  v_protected_items:=coalesce((p_facts->>'protected_wait_items')::integer,0);

  -- The two bypasses first: for these the Weekly Source component must not
  -- mount at all and this owner supplies no heading (matrix UI-016, UI-017).
  if coalesce(p_facts->>'scope','')='DAILY' then
    return private.weekly_source_office_lifecycle_result_v1('DAILY','[]'::jsonb,'[]'::jsonb,null);
  end if;
  if coalesce((p_facts->>'applicable')::boolean,false) is not true then
    return private.weekly_source_office_lifecycle_result_v1('STANDARD_WEEKLY','[]'::jsonb,'[]'::jsonb,null);
  end if;

  select pg_catalog.array_agg(distinct scope_row.family_timesheet_id),
         pg_catalog.min(scope_row.canonical_timesheet_id::text)::uuid,
         pg_catalog.count(distinct scope_row.family_timesheet_id)::integer
    into v_members,v_canonical,v_member_count
  from public._pay_timesheet_rotation_scope(array[p_root_timesheet_id]) as scope_row;
  if v_members is null or v_member_count=0 then
    return private.weekly_source_office_lifecycle_result_v1(
      null,'[]'::jsonb,
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'code','FAMILY_UNRESOLVED',
        'detail','The installed rotation resolver returned no member for this root.')),
      null);
  end if;

  -- One live authorisation per root family, or the projection is contradictory.
  select pg_catalog.count(*) filter (where authorisation_row.withdrawn_at_utc is null)::integer,
         pg_catalog.count(*) filter (where authorisation_row.withdrawn_at_utc is not null)::integer
    into v_live_count,v_withdrawn_count
  from public.weekly_source_root_authorisations as authorisation_row
  where authorisation_row.root_timesheet_id=any(v_members);
  v_authorisation_state:=private.weekly_source_office_authorisation_state_v1(p_root_timesheet_id);
  if v_live_count>1 then
    v_errors:=v_errors||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'code','MULTIPLE_LIVE_ROOT_AUTHORISATIONS',
      'detail','More than one not-withdrawn authorisation generation exists for this family.',
      'live_count',v_live_count));
  end if;
  if v_authorisation_state='UNKNOWN' then
    v_errors:=v_errors||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'code','AUTHORISATION_STATE_UNKNOWN',
      'detail','The authorisation-state owner could not answer for this root.'));
  end if;

  select pg_catalog.count(*)::integer into v_head_count
  from public.weekly_source_entitlement_heads as head_row
  where head_row.root_timesheet_id=any(v_members)
    and head_row.state='COMMITTED_CURRENT';
  if v_head_count>1 then
    v_errors:=v_errors||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'code','MULTIPLE_COMMITTED_CURRENT_HEADS',
      'detail','One root has at most one committed current head (24 section 4.3).',
      'head_count',v_head_count));
  elsif v_head_count=1 then
    select head_row.* into v_head
    from public.weekly_source_entitlement_heads as head_row
    where head_row.root_timesheet_id=any(v_members)
      and head_row.state='COMMITTED_CURRENT';
  end if;

  v_allocation:=private.weekly_source_settlement_allocation_v1(p_root_timesheet_id);
  v_payment:=private.weekly_source_office_payment_progress_v1(v_members);
  v_invoices:=private.weekly_source_office_invoice_movements_v1(v_members);
  v_proposal:=private.weekly_source_office_proposal_view_v1(p_root_timesheet_id,v_members);

  v_settled:=coalesce((v_allocation->>'ok')::boolean,false)
    and v_allocation->>'state'='AVAILABLE';
  v_batch_count:=coalesce((v_allocation->>'batch_count')::integer,0);
  -- `settlement_count` and `batch_count` are JSON numbers on AVAILABLE,
  -- NO_SETTLEMENT and BOTH unavailable classes (WP-11d D4), so the phase can be
  -- resolved unconditionally.  They are counts of financial EVENTS and are never
  -- conflated with an amount or an hours figure.
  v_settlement_count:=coalesce((v_allocation->>'settlement_count')::integer,0);
  -- The class is READ, not derived.  WP-11d publishes `unavailable_class` with
  -- exactly the distinction this projection needs, present and null on AVAILABLE
  -- and NO_SETTLEMENT so it can be read unconditionally:
  --
  --   POSITION_WITHHELD  the evidence is sound and only the position cannot be
  --                      stated.  NOT a contradiction: resolve the phase, keep
  --                      the heading, mark the paid schedules unavailable and
  --                      carry the reason and its plain-English detail.
  --   EVIDENCE_DAMAGED   every other reason.  A contradiction; fail closed.
  --
  -- Deriving the class here from a list of reason codes would put the same rule
  -- in two packages and let them drift the first time a reason is added.
  v_position_withheld:=v_allocation->>'unavailable_class'='POSITION_WITHHELD';

  -- Settlement evidence that cannot be read is never guessed at.  It is only
  -- an error when the phase would have had to state a paid figure.
  if coalesce((v_allocation->>'ok')::boolean,false) is not true
     and not v_position_withheld then
    v_errors:=v_errors||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'code','SETTLEMENT_ALLOCATION_UNAVAILABLE',
      -- The detail is the READER's own sentence for this exact reason, which it
      -- guarantees is true of the state and safe to show an Office user.  A
      -- generic sentence written here would drift from the reason beside it.
      'detail',coalesce(v_allocation->>'reason_detail',
        'Hours paid cannot be stated from the immutable settlement allocation.'),
      'reason',v_allocation->>'reason',
      'unavailable_class',v_allocation->>'unavailable_class'));
  end if;
  if coalesce((v_payment->>'ok')::boolean,false) is not true then
    v_errors:=v_errors||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'code','PAYMENT_PROGRESS_UNAVAILABLE','reason',v_payment->>'reason'));
  end if;
  -- Settled money with no authorisation at all is a contradiction: money moved
  -- for a week nobody approved.
  if (v_settled or v_settlement_count>0)
     and v_authorisation_state='NEVER_AUTHORISED' then
    v_errors:=v_errors||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'code','SETTLEMENT_WITHOUT_AUTHORISATION',
      'detail','Settlement evidence exists for a root that was never authorised.'));
  end if;

  if pg_catalog.jsonb_array_length(v_errors)>0 then
    return private.weekly_source_office_lifecycle_result_v1(null,v_overlays,v_errors,null)
      ||pg_catalog.jsonb_build_object(
        'member_timesheet_ids',pg_catalog.to_jsonb(v_members),
        'authorisation_state',v_authorisation_state,
        'settlement',v_allocation,'payment',v_payment,
        'invoice_movements',v_invoices,'proposal',v_proposal);
  end if;

  -- `UI-018` is an overlay on whatever phase the week is really in.
  if coalesce((p_facts->>'source_fixed_expenses')::boolean,false) then
    v_overlays:=v_overlays||pg_catalog.jsonb_build_array('UI-018');
  end if;

  if v_authorisation_state='WITHDRAWN'
     and coalesce((v_invoices->>'invoiced_from_source')::boolean,false) then
    -- DEC-061 Option A (proof/36 section 7): withdrawal stops Candidate payment
    -- only.  The finalised source movement stays on the exact self-bill and the
    -- Office Weekly detail says so until the week is authorised again.
    v_phase:='OFFICE_WITHDRAWN_INVOICED_FROM_SOURCE';
  elsif v_authorisation_state<>'AUTHORISED' then
    -- Never authorised, or withdrawn with no source invoice line: the week is
    -- back in front of Office for a first authorisation (proof/36 section 5.8).
    if v_authority_mode='TIMESHEET_AUTHORITY' then
      v_phase:=case when v_comparison='MATCH' then 'TIMESHEET_AUTHORITY_MATCH'
        else 'TIMESHEET_AUTHORITY_MISMATCH' end;
    elsif v_protected_items>0 then
      v_phase:='FIRST_ALTERNATE_HOURS_PENDING';
    elsif v_comparison='NO_TIMESHEET' then
      v_phase:='FIRST_AUTHORISATION_PENDING_NO_SUBMISSION';
    elsif v_comparison='MISMATCH' then
      v_phase:='FIRST_AUTHORISATION_PENDING_MISMATCH';
    else
      v_phase:='FIRST_AUTHORISATION_PENDING';
    end if;
  -- A decision that is SAVED and frozen outranks an undecided one: `24 section
  -- 4.4` says the previous entitlement stays current until the release
  -- condition is observed, so the screen must say the decision is already made.
  elsif coalesce((v_proposal->>'present')::boolean,false)
        and coalesce(v_proposal->>'pending_state','') in ('PENDING','RELEASING') then
    v_phase:='LATER_CHANGE_APPROVED_FROZEN';
  -- The cross-Contract bundle is told apart by its RECORDED kind, never by
  -- whether its content could be displayed: a bundle whose entitlement cannot
  -- be stated is still one atomic two-root decision.
  elsif coalesce((v_proposal->>'present')::boolean,false)
        and coalesce(v_proposal->>'bundle_kind','SINGLE_ROOT')='CROSS_CONTRACT_A_B' then
    v_phase:='CROSS_CONTRACT_PENDING';
  elsif coalesce((v_proposal->>'present')::boolean,false) then
    v_phase:=case when v_settlement_count>0 then 'LATER_CHANGE_PENDING_PAID'
      else 'LATER_CHANGE_PENDING_UNPAID' end;
  elsif v_head_count=1 and v_head.prior_head_id is not null then
    v_phase:=case when v_settlement_count>0 and v_batch_count>1
      then 'ADJUSTMENT_SETTLED' else 'LATER_CHANGE_PUBLISHED_UNSETTLED' end;
  elsif v_settlement_count>0 then
    v_phase:=case when v_batch_count>1 then 'ADJUSTMENT_SETTLED' else 'PAID' end;
  elsif coalesce(v_payment->>'state','NONE')='IN_FLIGHT' then
    v_phase:='PAYMENT_PROCESSING';
  else
    v_phase:='AUTHORISED_NOT_PAID';
  end if;

  return private.weekly_source_office_lifecycle_result_v1(v_phase,v_overlays,'[]'::jsonb,null)
    ||pg_catalog.jsonb_build_object(
      'member_timesheet_ids',pg_catalog.to_jsonb(v_members),
      'canonical_timesheet_id',v_canonical,
      'authorisation_state',v_authorisation_state,
      'authorisation_generations',pg_catalog.jsonb_build_object(
        'live',v_live_count,'withdrawn',v_withdrawn_count),
      'current_head',case when v_head_count=1 then pg_catalog.jsonb_build_object(
        'head_id',v_head.id,'authority_kind',v_head.authority_kind,
        'head_revision',v_head.head_revision,'prior_head_id',v_head.prior_head_id,
        'certified_zero',v_head.certified_zero,'component_count',v_head.component_count,
        'committed_at_utc',v_head.committed_at_utc) else null end,
      'settlement',v_allocation,'payment',v_payment,
      'invoice_movements',v_invoices,'proposal',v_proposal);
end;
$function$;

-- ---------------------------------------------------------------------------
-- Gate 9 item G9-1, Candidate half.  Matrix rows `UI-019` to `UI-021`.
--
-- The Candidate surface gets its phase from the server too, so MyTMS infers
-- nothing either.  The payload itself belongs to the Gate 9 G9-6 producer; this
-- function only READS it and classifies, so there is exactly one place that
-- decides what a Candidate is shown.  It returns hours facts only: no money, no
-- payment or recovery history, no remittance link and none of the words source,
-- protected, exceptional or reconciliation.
-- ---------------------------------------------------------------------------
create or replace function private.weekly_source_office_candidate_phase_v1(
  p_timesheet_id uuid,
  p_now timestamptz
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_view jsonb;
  v_submitted integer:=0;
  v_approved integer:=0;
  v_differ boolean:=false;
  v_phase text;
begin
  begin
    v_view:=private.weekly_source_candidate_view_v1(p_timesheet_id,p_now);
  exception
    when undefined_function or undefined_table then
      return private.weekly_source_office_lifecycle_result_v1(
        null,'[]'::jsonb,
        pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'code','CANDIDATE_VIEW_PRODUCER_ABSENT',
          'detail','The Gate 9 G9-6 Candidate payload producer is not installed.')),
        null);
    when others then
      return private.weekly_source_office_lifecycle_result_v1(
        null,'[]'::jsonb,
        pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'code','CANDIDATE_VIEW_PRODUCER_FAILED',
          'detail','The Candidate payload producer did not answer for this week.')),
        null);
  end;

  if v_view is null or pg_catalog.jsonb_typeof(v_view)<>'object' then
    return pg_catalog.jsonb_build_object(
      'contract','WEEKLY_SOURCE_OFFICE_LIFECYCLE_V1','ok',true,
      'ui_state',null,'server_phase',null,'surface','CANDIDATE',
      'heading',null,'heading_source','LEGACY_OWNER',
      'primary_schedule',null,'right_pane_status',null,
      'permitted_actions','[]'::jsonb,'forbidden_inference',null,
      'overlay_states','[]'::jsonb,'overlays','[]'::jsonb,'errors','[]'::jsonb,
      'note','CANDIDATE_SURFACE_NOT_APPLICABLE');
  end if;

  v_submitted:=pg_catalog.jsonb_array_length(
    coalesce(v_view->'submitted_timesheet','[]'::jsonb));
  v_approved:=pg_catalog.jsonb_array_length(
    coalesce(v_view->'approved_hours_to_be_paid','[]'::jsonb));
  v_differ:=coalesce((v_view->>'approved_hours_differ')::boolean,false);

  if v_submitted=0 and v_approved=0 then
    -- `NAI-MYT-001`: before Office authorises, a week with no submission has no
    -- Candidate-facing detail at all.  That is not `UI-021`.
    return pg_catalog.jsonb_build_object(
      'contract','WEEKLY_SOURCE_OFFICE_LIFECYCLE_V1','ok',true,
      'ui_state',null,'server_phase',null,'surface','CANDIDATE',
      'heading',null,'heading_source','LEGACY_OWNER',
      'primary_schedule',null,'right_pane_status',null,
      'permitted_actions','[]'::jsonb,'forbidden_inference',null,
      'overlay_states','[]'::jsonb,'overlays','[]'::jsonb,'errors','[]'::jsonb,
      'note','CANDIDATE_DETAIL_NOT_YET_AVAILABLE');
  end if;

  v_phase:=case
    when v_submitted=0 then 'MYTMS_NO_SUBMISSION'
    when v_differ then 'MYTMS_DIFFERENT'
    else 'MYTMS_SAME' end;

  -- The Candidate rows' `forbidden_inference` prose is deliberately NOT echoed
  -- into the Office payload.  It is a prohibition on the Candidate producer,
  -- not a field of this projection, and the Office screen never renders the
  -- Candidate surface; serving the prose here would put the very vocabulary the
  -- rule forbids (`money`, `remittance`) into an Office payload whose own
  -- verifier proves no such word appears.  The complete text stays in the
  -- lifecycle policy, which the workspace serves and the verifier reads.
  return private.weekly_source_office_lifecycle_result_v1(v_phase,'[]'::jsonb,'[]'::jsonb,null)
    ||pg_catalog.jsonb_build_object(
      'forbidden_inference',null,
      'forbidden_inference_ref','annexes/ui-lifecycle-state-matrix.csv',
      'submitted_row_count',v_submitted,
      'approved_hours_row_count',v_approved,
      'approved_hours_differ',v_differ,
      'expense_entry_mode',v_view->>'expense_entry_mode');
end;
$function$;

create or replace function public.weekly_source_office_timesheet_presentation_v1(
  p_request jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array['actor_user_id','timesheet_id']::text[];
  v_unknown text;
  v_actor uuid;
  v_timesheet_id uuid;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_client public.clients%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_policy jsonb;
  v_authority text;
  v_route text;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_tsfin public.timesheets_financials%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_source_rows jsonb:='[]'::jsonb;
  v_submitted_rows jsonb:='[]'::jsonb;
  v_approved_rows jsonb:='[]'::jsonb;
  v_manage_items jsonb:='[]'::jsonb;
  v_comparison_state text;
  v_submitted_available boolean;
  v_submitted_complete boolean;
  v_open_issue_count integer:=0;
  v_unprotected_issue_count integer:=0;
  v_reference_blocker_count integer:=0;
  v_require_reference_to_pay boolean:=false;
  v_authorise_allowed boolean:=false;
  v_blocked_reason text:='';
  v_source_expense_policy text;
  v_manage_allowed boolean:=false;
  v_add_expense_allowed boolean:=false;
  v_record_version text;
  v_pay_ex numeric:=0;
  v_pay_inc numeric:=0;
  v_charge_ex numeric:=0;
  v_charge_inc numeric:=0;
  v_charge_vat_base numeric:=0;
  v_vat_rate numeric:=0;
  v_totals_complete boolean:=false;
  -- Gate 9 items G9-1, G9-3 and G9-5.
  v_lifecycle jsonb;
  v_candidate_lifecycle jsonb;
  v_inventory jsonb;
  v_settlement jsonb;
  v_schedules jsonb;
  v_currently_approved jsonb;
  v_paid jsonb;
  v_phase text;
  v_first_authorisation boolean:=false;
  -- WP-30 (WP-27 sweep finding N5), standing rule 3.  The current source
  -- publication for a week is found through the source-row lineage binding and
  -- the source comparison, both of which are facts about the Timesheet FAMILY.
  -- WP-27 named the lineage limb; the comparison limb beside it is the same
  -- class and was not on its list, so both move together.  EXECUTED on a
  -- rotated family: no publication was found for the current root while the
  -- demoted sibling still found one.
  v_root_family uuid[];
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_PRESENTATION_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_PRESENTATION_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_timesheet_id:=(p_request->>'timesheet_id')::uuid;
  exception when others then
    raise exception 'WEEKLY_SOURCE_PRESENTATION_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_timesheet_id is null then
    raise exception 'WEEKLY_SOURCE_PRESENTATION_REQUEST_INVALID' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'VIEW_SOURCE_PROGRESS',null,null,current_date
  );
  select * into v_timesheet from public.timesheets
  where timesheet_id=v_timesheet_id and is_current and revoked_at is null
    and archived_at_utc is null;
  if not found then raise exception 'WEEKLY_SOURCE_TIMESHEET_NOT_FOUND' using errcode='22023'; end if;
  if v_timesheet.sheet_scope='DAILY' then
    -- Matrix `UI-017`.  The phase is still SERVED, so the browser never has to
    -- decide that a Daily record is a Daily record; the heading stays the
    -- existing Daily owner's and this owner supplies none.
    return pg_catalog.jsonb_build_object('applicable',false,'scope','DAILY')
      ||pg_catalog.jsonb_build_object('lifecycle',
        private.weekly_source_office_lifecycle_phase_v1(
          v_timesheet.timesheet_id,
          pg_catalog.jsonb_build_object('applicable',false,'scope','DAILY')));
  end if;
  if v_timesheet.sheet_scope<>'WEEKLY' or v_timesheet.line_type<>'HOURS'
     or v_timesheet.contract_id is null then
    return pg_catalog.jsonb_build_object('applicable',false,'scope',v_timesheet.sheet_scope)
      ||pg_catalog.jsonb_build_object('lifecycle',
        private.weekly_source_office_lifecycle_phase_v1(
          v_timesheet.timesheet_id,
          pg_catalog.jsonb_build_object('applicable',false,'scope',v_timesheet.sheet_scope)));
  end if;
  select * into strict v_contract from public.contracts where id=v_timesheet.contract_id;
  select * into strict v_client from public.clients where id=v_contract.client_id;
  select case
    when coalesce(v_contract.overrideclientsettings,false)
      then coalesce(v_contract.require_reference_to_pay,false)
    else coalesce((
      select settings.pay_reference_required
      from public.client_settings settings
      where settings.client_id=v_contract.client_id
        and (settings.effective_from is null
          or settings.effective_from<=v_timesheet.week_ending_date)
      order by settings.effective_from desc nulls last,settings.updated_at desc,settings.id desc
      limit 1
    ),false)
  end into v_require_reference_to_pay;
  select source_group.* into v_group
  from public.weekly_source_group_clients membership
  join public.weekly_source_groups source_group on source_group.id=membership.source_group_id
  where membership.client_id=v_contract.client_id and source_group.active
    and v_timesheet.week_ending_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date);
  -- Matrix `UI-016`: an ordinary Weekly record is not in a Weekly Source group,
  -- the Weekly Source component must not mount, and the legacy Weekly owner
  -- keeps its own heading.  The phase is nevertheless served.
  if not found then return pg_catalog.jsonb_build_object('applicable',false,'scope','WEEKLY')
    ||pg_catalog.jsonb_build_object('lifecycle',
      private.weekly_source_office_lifecycle_phase_v1(
        v_timesheet.timesheet_id,
        pg_catalog.jsonb_build_object('applicable',false,'scope','WEEKLY'))); end if;
  v_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_timesheet.week_ending_date
  );
  perform private.weekly_source_office_authority_v1(
    v_actor,'VIEW_SOURCE_PROGRESS',v_group.id,v_contract.client_id,v_timesheet.week_ending_date
  );
  v_authority:=case when v_policy->>'authority_mode'='SOURCE_AUTHORITY'
    then 'CLIENT_SYSTEM' else 'SIGNED_TIMESHEET' end;
  v_route:=case when v_policy->>'authority_mode'='TIMESHEET_AUTHORITY'
    then 'TIMESHEETS_CHECKED_WITH_CLIENT'
    when v_group.source_family='NHSP' then 'NHSP' else 'CLIENT_PROVIDED_HOURS' end;
  v_source_expense_policy:=case
    when coalesce((v_policy->>'source_fixed_expenses_enabled')::boolean,false)
      then 'SOURCE_SUPPLIED' else 'SEPARATE_ADDITIONAL_TIMESHEET' end;
  v_add_expense_allowed:=v_source_expense_policy='SEPARATE_ADDITIONAL_TIMESHEET';

  -- One family resolution, through the one installed adapter, read by both
  -- limbs.  Standing rule 3's fail-closed branch is the adapter's own fallback
  -- plus this explicit cardinality test; it is never a `limit`.  The
  -- `order by … limit 1` below is PRE-EXISTING and is a display choice between
  -- several current publications, not a safety property: nothing is refused on
  -- it, and WP-30 deliberately did not change it.
  v_root_family:=private.weekly_source_invoice_family_timesheet_ids_v1(
    v_timesheet.timesheet_id
  );
  if v_root_family is null or pg_catalog.cardinality(v_root_family)=0 then
    raise exception 'WEEKLY_SOURCE_OFFICE_PRESENTATION_FAMILY_UNRESOLVED'
      using errcode='55000';
  end if;
  select publication.* into v_publication
  from public.weekly_source_projection_publications publication
  where publication.state='CURRENT'
    and (
      exists(select 1 from public.weekly_source_row_timesheet_lineages lineage
        join public.weekly_source_row_resolutions resolution on resolution.id=lineage.row_resolution_id
        join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
        where lineage.timesheet_id=any(v_root_family) and source_row.upload_id=publication.upload_id)
      or exists(select 1 from public.weekly_timesheet_source_comparisons comparison
        where comparison.timesheet_id=any(v_root_family)
          and comparison.projection_publication_id=publication.id)
    )
  order by publication.published_at_utc desc,publication.id desc limit 1;
  if found then
    select * into strict v_cycle from public.weekly_source_cycles where id=v_publication.source_cycle_id;
    perform private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication.id);
  elsif v_policy->>'authority_mode'='SOURCE_AUTHORITY' then
    raise exception 'SOURCE_CHECK_IN_PROGRESS' using errcode='55000';
  end if;

  v_submitted_available:=v_timesheet.r2_nurse_key is not null
    and v_timesheet.img_sha256_nurse is not null;
  v_submitted_complete:=v_submitted_available and (
    v_policy->>'authority_mode'='SOURCE_AUTHORITY'
    or (v_timesheet.r2_auth_key is not null and v_timesheet.img_sha256_auth is not null)
  );

  if v_publication.id is not null then
    select pg_catalog.count(*)::integer into v_open_issue_count
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    where incident.source_cycle_id=v_publication.source_cycle_id
      and incident.candidate_id=v_contract.candidate_id
      and incident.client_id=v_contract.client_id and incident.state='OPEN'
      and comparison.projection_publication_id=v_publication.id
      and (comparison.candidate_timesheet_id is null
        or comparison.candidate_timesheet_id=v_timesheet.timesheet_id);

    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','source-'||source_row.id::text,
      'work_event_id',resolution.work_event_id,
      'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
      'hours',to_char(source_row.start_at_local,'HH24:MI')||'-'||to_char(source_row.end_at_local,'HH24:MI'),
      'break_text',coalesce(source_row.break_minutes,0)||' min',
      'state',case when incident.id is null then case when v_submitted_available then 'MATCH' else 'NO_TIMESHEET' end else 'MISMATCH' end,
      'issue_text',case comparison.issue_family
        when 'SOURCE_HOURS_DIFFER' then 'Hours differ'
        when 'SOURCE_MISSING_OR_NOT_AUTHORISED' then case when v_group.source_family='NHSP'
          then 'Missing or not yet authorised' else 'Not included in system hours' end
        when 'HEALTHROSTER_NOT_FINALISED' then 'Not finalised'
        when 'REFERENCE_MISSING' then 'Reference missing' else null end,
      'additional_units',coalesce((select pg_catalog.jsonb_agg(unit.value order by unit.ordinality)
        from pg_catalog.jsonb_array_elements(pg_catalog.jsonb_build_array(
          case when coalesce(economic.hours_day,0)<>0 then pg_catalog.jsonb_build_object('key','day','label','Day','value',economic.hours_day||' hours','state',case when incident.id is null then 'MATCH' else 'MISMATCH' end) end,
          case when coalesce(economic.hours_night,0)<>0 then pg_catalog.jsonb_build_object('key','night','label','Night','value',economic.hours_night||' hours','state',case when incident.id is null then 'MATCH' else 'MISMATCH' end) end,
          case when coalesce(economic.hours_sat,0)<>0 then pg_catalog.jsonb_build_object('key','sat','label','Saturday','value',economic.hours_sat||' hours','state',case when incident.id is null then 'MATCH' else 'MISMATCH' end) end,
          case when coalesce(economic.hours_sun,0)<>0 then pg_catalog.jsonb_build_object('key','sun','label','Sunday','value',economic.hours_sun||' hours','state',case when incident.id is null then 'MATCH' else 'MISMATCH' end) end,
          case when coalesce(economic.hours_bh,0)<>0 then pg_catalog.jsonb_build_object('key','bh','label','Bank holiday','value',economic.hours_bh||' hours','state',case when incident.id is null then 'MATCH' else 'MISMATCH' end) end
        )) with ordinality unit(value,ordinality) where unit.value<>'null'::jsonb),'[]'::jsonb)
    ) order by source_row.work_date,source_row.start_at_local,source_row.id),'[]'::jsonb)
    into v_source_rows
    from public.weekly_source_upload_rows source_row
    join lateral (select current_resolution.* from public.weekly_source_row_resolutions current_resolution
      where current_resolution.upload_row_id=source_row.id order by current_resolution.generation desc,current_resolution.id desc limit 1) resolution
      on resolution.mapping_state='RESOLVED'
    left join public.weekly_source_row_economic_snapshots economic on economic.row_resolution_id=resolution.id
    left join public.weekly_discrepancy_incidents incident
      on incident.source_cycle_id=v_publication.source_cycle_id
     and incident.work_event_id=resolution.work_event_id and incident.state='OPEN'
    left join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
     and comparison.projection_publication_id=v_publication.id
    where source_row.upload_id=v_publication.upload_id
      and resolution.candidate_id=v_contract.candidate_id
      and resolution.client_id=v_contract.client_id
      and resolution.contract_id=v_contract.id
      and (date_trunc('week',source_row.work_date)::date+6)=v_timesheet.week_ending_date
      and source_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED');
  end if;

  if pg_catalog.jsonb_typeof(v_timesheet.actual_schedule_json)='array' then
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','submitted-'||entry.ordinality,
      'day_date',to_char((entry.value->>'date')::date,'Dy FMDD Mon YYYY'),
      'hours',coalesce(entry.value->>'start',pg_catalog.chr(8212))||'-'||coalesce(entry.value->>'end',pg_catalog.chr(8212)),
      'break_text',coalesce(entry.value->>'break_minutes','0')||' min',
      'state',case when exists(
        select 1 from public.weekly_discrepancy_incidents incident
        join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
        join public.weekly_work_events work_event on work_event.id=incident.work_event_id
        where incident.state='OPEN' and incident.candidate_id=v_contract.candidate_id
          and incident.client_id=v_contract.client_id
          and work_event.work_date=(entry.value->>'date')::date
          and (comparison.candidate_timesheet_id is null or comparison.candidate_timesheet_id=v_timesheet.timesheet_id)
      ) then 'MISMATCH' else 'MATCH' end,
      'affected',exists(
        select 1 from public.weekly_discrepancy_incidents incident
        join public.weekly_issue_comparison_revisions comparison on comparison.id=incident.current_comparison_revision_id
        join public.weekly_work_events work_event on work_event.id=incident.work_event_id
        where incident.state='OPEN' and incident.candidate_id=v_contract.candidate_id
          and incident.client_id=v_contract.client_id
          and work_event.work_date=(entry.value->>'date')::date
          and (comparison.candidate_timesheet_id is null or comparison.candidate_timesheet_id=v_timesheet.timesheet_id)
      ),
      'issue_text',case when exists(
        select 1 from public.weekly_discrepancy_incidents incident
        join public.weekly_work_events work_event on work_event.id=incident.work_event_id
        where incident.state='OPEN' and incident.candidate_id=v_contract.candidate_id
          and incident.client_id=v_contract.client_id and work_event.work_date=(entry.value->>'date')::date
      ) then 'Hours differ' else null end
    ) order by (entry.value->>'date')::date,entry.ordinality),'[]'::jsonb)
    into v_submitted_rows
    from pg_catalog.jsonb_array_elements(v_timesheet.actual_schedule_json) with ordinality entry(value,ordinality)
    where v_policy->>'authority_mode'='TIMESHEET_AUTHORITY'
       or v_open_issue_count>0;
  end if;

  -- Schema change S8 removed `unique (root_timesheet_id)` from
  -- `public.weekly_exceptional_pay_target_families`, so a bare `select … into`
  -- on that column now returns an ARBITRARY row instead of raising, and this
  -- read decides which protected-hours actions the Office screen offers.
  -- The deterministic fail-closed reader resolves the family through the root
  -- identity and raises on an ambiguous or unresolvable family (WP-06 handoff
  -- N2).  A function call does not set FOUND, so the row itself is tested.
  v_family:=private.weekly_source_target_family_for_root_v1(v_timesheet.timesheet_id);
  if v_family.id is not null then
    v_manage_allowed:=v_family.ownership_state='TARGET_MANAGED'
      or v_family.current_lifecycle_state in ('PROTECTED','WAITING_SOURCE','READY_TO_RECONCILE','ACTION_REQUIRED');
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'item_id',event.id,'work_event_id',event.durable_work_event_id,'state',event.state,
      'primary_action','AMEND_PROTECTED_HOURS',
      'available_actions',pg_catalog.jsonb_build_array('AMEND_PROTECTED_HOURS','WITHDRAW_PROTECTED_HOURS','WAIT_FOR_SOURCE','ACCEPT_SOURCE_AND_RECONCILE','RECORD_NOT_WORKED'),
      'command_payload',pg_catalog.jsonb_build_object(
        'source_cycle_id',approval.source_cycle_id,'candidate_id',approval.candidate_id,
        'client_id',approval.client_id,'contract_id',approval.contract_id,
        'week_ending_date',approval.week_ending,'family_id',v_family.id,
        'work_event_id',event.durable_work_event_id,'evidence_timesheet_id',approval.evidence_timesheet_id,
        'work_date',event.work_date,'start_at_local',to_char(event.start_at_local,'HH24:MI'),
        'end_at_local',to_char(event.end_at_local,'HH24:MI'),'break_minutes',event.break_minutes),
      'schedule',pg_catalog.jsonb_build_object('work_date',event.work_date,
        'start_at_local',to_char(event.start_at_local,'HH24:MI'),
        'end_at_local',to_char(event.end_at_local,'HH24:MI'),'break_minutes',event.break_minutes)
    ) order by event.work_date,event.start_at_local,event.id),'[]'::jsonb)
    into v_manage_items
    from public.weekly_exceptional_pay_family_events event
    join lateral (select max(latest.event_sequence) sequence from public.weekly_exceptional_pay_family_events latest
      where latest.family_id=event.family_id and latest.durable_work_event_id=event.durable_work_event_id) latest
      on latest.sequence=event.event_sequence
    join public.weekly_exceptional_payment_approvals approval on approval.id=event.evidence_approval_id
    where event.family_id=v_family.id and event.state='WAIT';
  end if;

  if v_policy->>'authority_mode'='SOURCE_AUTHORITY' and v_publication.id is not null then
    v_manage_allowed:=v_open_issue_count>0 or v_family.id is not null;
    select pg_catalog.count(*)::integer into v_unprotected_issue_count
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
     and comparison.projection_publication_id=v_publication.id
    where incident.source_cycle_id=v_publication.source_cycle_id
      and incident.candidate_id=v_contract.candidate_id
      and incident.client_id=v_contract.client_id
      and incident.state='OPEN'
      and (comparison.candidate_timesheet_id is null
        or comparison.candidate_timesheet_id=v_timesheet.timesheet_id)
      and not exists(
        select 1
        from public.weekly_exceptional_pay_family_events protected_event
        where protected_event.family_id=v_family.id
          and protected_event.durable_work_event_id=incident.work_event_id
          and protected_event.state='WAIT'
          and protected_event.event_sequence=(
            select pg_catalog.max(latest.event_sequence)
            from public.weekly_exceptional_pay_family_events latest
            where latest.family_id=protected_event.family_id
              and latest.durable_work_event_id=protected_event.durable_work_event_id
          )
      );
  end if;

  if v_policy->>'authority_mode'='SOURCE_AUTHORITY' then
    select coalesce(pg_catalog.jsonb_agg(row_value order by work_date,start_at_local,row_key),'[]'::jsonb)
    into v_approved_rows
    from (
      select source_row.work_date,source_row.start_at_local,'approved-'||source_row.id::text row_key,
        pg_catalog.jsonb_build_object('row_key','approved-'||source_row.id::text,
          'day_date',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
          'hours',to_char(source_row.start_at_local,'HH24:MI')||'-'||to_char(source_row.end_at_local,'HH24:MI'),
          'break_text',coalesce(source_row.break_minutes,0)||' min','state',case when incident.id is null then 'READY' else 'MISMATCH' end,
          'status_text',case when incident.id is null then 'Ready' else 'Needs attention' end) row_value
      from public.weekly_source_upload_rows source_row
      join lateral (select current_resolution.* from public.weekly_source_row_resolutions current_resolution
        where current_resolution.upload_row_id=source_row.id order by current_resolution.generation desc,current_resolution.id desc limit 1) resolution
        on resolution.mapping_state='RESOLVED'
      left join public.weekly_discrepancy_incidents incident
        on incident.source_cycle_id=v_publication.source_cycle_id and incident.work_event_id=resolution.work_event_id and incident.state='OPEN'
      where v_publication.id is not null and source_row.upload_id=v_publication.upload_id
        and resolution.candidate_id=v_contract.candidate_id and resolution.client_id=v_contract.client_id
        and resolution.contract_id=v_contract.id
        and (date_trunc('week',source_row.work_date)::date+6)=v_timesheet.week_ending_date
        and source_row.row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED')
        and not exists(select 1 from public.weekly_exceptional_pay_family_events event
          where event.family_id=v_family.id and event.durable_work_event_id=resolution.work_event_id and event.state='WAIT'
            and event.event_sequence=(select max(latest.event_sequence) from public.weekly_exceptional_pay_family_events latest
              where latest.family_id=event.family_id and latest.durable_work_event_id=event.durable_work_event_id))
      union all
      select event.work_date,event.start_at_local,'protected-'||event.id::text,
        pg_catalog.jsonb_build_object('row_key','protected-'||event.id::text,
          'day_date',to_char(event.work_date,'Dy FMDD Mon YYYY'),
          'hours',to_char(event.start_at_local,'HH24:MI')||'-'||to_char(event.end_at_local,'HH24:MI'),
          'break_text',event.break_minutes||' min','state','PROTECTED','status_text','Office-approved hours',
          'context_text','Client system hours are still being checked.')
      from public.weekly_exceptional_pay_family_events event
      where event.family_id=v_family.id and event.state='WAIT'
        and event.event_sequence=(select max(latest.event_sequence) from public.weekly_exceptional_pay_family_events latest
          where latest.family_id=event.family_id and latest.durable_work_event_id=event.durable_work_event_id)
    ) approved;
  else
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'row_key','approved-'||entry.ordinality,'day_date',to_char((entry.value->>'date')::date,'Dy FMDD Mon YYYY'),
      -- `->>` and `||` share a precedence class and associate left to right, so
      -- the unparenthesised form parsed as `entry.value ->> ('start'||'-'||…)`
      -- and raised 42883 for every Timesheet-authority week that had a
      -- submitted schedule, which is exactly matrix rows UI-014 and UI-015.
      'hours',(entry.value->>'start')||'-'||(entry.value->>'end'),
      'break_text',coalesce(entry.value->>'break_minutes','0')||' min',
      'state','READY','status_text','Ready'
    ) order by (entry.value->>'date')::date,entry.ordinality),'[]'::jsonb)
    into v_approved_rows
    from pg_catalog.jsonb_array_elements(case when pg_catalog.jsonb_typeof(v_timesheet.actual_schedule_json)='array'
      then v_timesheet.actual_schedule_json else '[]'::jsonb end) with ordinality entry(value,ordinality);
  end if;

  select * into v_tsfin from public.timesheets_financials financial
  where financial.timesheet_id=v_timesheet.timesheet_id and financial.is_current and not financial.is_stale
  order by financial.timesheet_version desc,financial.computed_at_utc desc,financial.id desc limit 1;
  if found then
    v_pay_ex:=pg_catalog.round(coalesce(v_tsfin.total_pay_ex_vat,0)+coalesce(v_tsfin.additional_pay_ex_vat,0)
      +coalesce(v_tsfin.expenses_pay_ex_vat,0)+coalesce(v_tsfin.mileage_pay_ex_vat,0),2);
    v_charge_ex:=pg_catalog.round(coalesce(v_tsfin.total_charge_ex_vat,0)+coalesce(v_tsfin.additional_charge_ex_vat,0)
      +coalesce(v_tsfin.expenses_charge_ex_vat,0)+coalesce(v_tsfin.mileage_charge_ex_vat,0),2);
    select finance.vat_rate_pct into v_vat_rate from public.settings_finance_windows finance
    where v_timesheet.week_ending_date>=finance.date_from
      and (finance.date_to is null or v_timesheet.week_ending_date<=finance.date_to)
    order by finance.date_from desc,finance.id desc limit 1;
    v_vat_rate:=coalesce(v_vat_rate,0);
    v_pay_inc:=case when v_tsfin.pay_total_inc_vat_snapshot is not null and v_tsfin.pay_total_inc_vat_snapshot<>0
      then v_tsfin.pay_total_inc_vat_snapshot else v_pay_ex+coalesce(v_tsfin.pay_vat_amount_snapshot,0) end;
    v_charge_vat_base:=case when v_client.vat_chargeable then
      coalesce(v_tsfin.total_charge_ex_vat,0)+coalesce(v_tsfin.additional_charge_ex_vat,0)
      +coalesce(v_tsfin.mileage_charge_ex_vat,0)
      +case when v_source_expense_policy<>'SOURCE_SUPPLIED'
          or coalesce((v_policy->>'source_expense_vat_enabled')::boolean,false)
        then coalesce(v_tsfin.expenses_charge_ex_vat,0) else 0 end
      else 0 end;
    v_charge_inc:=pg_catalog.round(v_charge_ex+(v_charge_vat_base*v_vat_rate/100),2);
    v_totals_complete:=true;
  end if;

  if v_policy->>'authority_mode'='TIMESHEET_AUTHORITY' then
    if not v_submitted_complete then
      v_comparison_state:='WAITING_FOR_COMPLETE_TIMESHEET';
      v_authorise_allowed:=false;
      v_blocked_reason:='Waiting for the worker and manager to complete the Timesheet.';
    else
      if v_publication.id is not null then
        select pg_catalog.count(*)::integer into v_reference_blocker_count
        from public.weekly_timesheet_source_comparisons comparison
        where comparison.timesheet_id=v_timesheet.timesheet_id
          and comparison.projection_publication_id=v_publication.id
          and comparison.comparison_state<>'EXACT_MATCH';
      elsif v_require_reference_to_pay then
        v_reference_blocker_count:=1;
      end if;
      v_comparison_state:=case when v_reference_blocker_count>0 then 'MISMATCH' else 'MATCH' end;
      v_authorise_allowed:=not v_require_reference_to_pay or v_reference_blocker_count=0;
      if not v_authorise_allowed then v_blocked_reason:='The required client reference is not ready yet.'; end if;
    end if;
  else
    v_comparison_state:=case when not v_submitted_available then 'NO_TIMESHEET'
      when v_open_issue_count>0 then 'MISMATCH' else 'MATCH' end;
    v_authorise_allowed:=v_unprotected_issue_count=0;
    if not v_authorise_allowed then
      v_blocked_reason:='Resolve the hours needing attention before authorising.';
    end if;
  end if;

  -- =======================================================================
  -- Gate 9 items G9-1, G9-3 and G9-5.
  --
  -- The phase, the heading and the permitted new actions are decided HERE and
  -- served; the browser performs no financial inference (`24 section 15`,
  -- contract section 13).  Every schedule below is either an explicit
  -- `available:true` with the server's own rows, or an explicit
  -- `available:false` with a machine reason.  There is no third case, so a
  -- missing figure can never be read as a zero.
  -- =======================================================================
  v_lifecycle:=private.weekly_source_office_lifecycle_phase_v1(
    v_timesheet.timesheet_id,
    pg_catalog.jsonb_build_object(
      'applicable',true,'scope','WEEKLY',
      'authority_mode',v_policy->>'authority_mode',
      'comparison_state',v_comparison_state,
      'protected_wait_items',pg_catalog.jsonb_array_length(v_manage_items),
      'source_fixed_expenses',v_source_expense_policy='SOURCE_SUPPLIED',
      'submitted_available',v_submitted_available));
  v_candidate_lifecycle:=private.weekly_source_office_candidate_phase_v1(
    v_timesheet.timesheet_id,pg_catalog.transaction_timestamp());
  v_phase:=v_lifecycle->>'server_phase';
  v_settlement:=v_lifecycle->'settlement';
  v_first_authorisation:=v_phase in (
    'FIRST_AUTHORISATION_PENDING','FIRST_AUTHORISATION_PENDING_NO_SUBMISSION',
    'FIRST_AUTHORISATION_PENDING_MISMATCH','FIRST_ALTERNATE_HOURS_PENDING',
    'TIMESHEET_AUTHORITY_MATCH','TIMESHEET_AUTHORITY_MISMATCH');

  -- The current effective entitlement, read once from interface I-7 and never
  -- recomputed here.  `Approved hours` and `Currently approved hours` are the
  -- SAME server fact under two policy names, so they are built from this one
  -- read and cannot disagree.
  v_inventory:=private.weekly_source_effective_inventory_v1(v_timesheet.timesheet_id);
  v_currently_approved:=case
    when coalesce((v_inventory->>'ok')::boolean,false) is not true
      then private.weekly_source_office_schedule_absent_v1(
        coalesce(v_inventory->>'code','EFFECTIVE_INVENTORY_UNAVAILABLE'))
    when coalesce(v_lifecycle->>'authorisation_state','UNKNOWN')<>'AUTHORISED'
      then private.weekly_source_office_schedule_absent_v1('NO_CURRENT_AUTHORISED_ENTITLEMENT')
    else private.weekly_source_office_schedule_from_components_v1(
      coalesce(v_inventory->'components','[]'::jsonb),
      'EFFECTIVE_INVENTORY_'||coalesce(v_inventory->>'authority','UNKNOWN')) end;

  -- `Hours paid`, `Hours paid to date` and `current paid hours` come only from
  -- the immutable per-root, per-shift settlement allocation (Gate 9 G9-2).
  -- Never from a currency-to-hours calculation, never from the last-settled
  -- cache, and never guessed when the evidence is unreadable.
  v_paid:=private.weekly_source_office_schedule_from_allocation_v1(
    v_settlement,'WEEKLY_SOURCE_SETTLEMENT_ALLOCATION');

  v_schedules:=pg_catalog.jsonb_build_object(
    'submitted',private.weekly_source_office_schedule_from_actual_v1(
      v_timesheet.actual_schedule_json),
    'latest_source',case when v_publication.id is null
      then private.weekly_source_office_schedule_absent_v1('NO_CURRENT_SOURCE_PUBLICATION')
      else private.weekly_source_office_schedule_from_rows_v1(
        v_source_rows,'WEEKLY_SOURCE_LATEST_SOURCE') end,
    'hours_to_authorise',case when v_first_authorisation
      then private.weekly_source_office_schedule_from_rows_v1(
        v_approved_rows,'WEEKLY_SOURCE_PROPOSED_FIRST_ENTITLEMENT')
      else private.weekly_source_office_schedule_absent_v1(
        'NOT_IN_A_FIRST_AUTHORISATION_PHASE') end,
    'currently_approved',v_currently_approved,
    'approved',v_currently_approved,
    'processing',case when v_phase='PAYMENT_PROCESSING' then v_currently_approved
      else private.weekly_source_office_schedule_absent_v1('NO_PAYMENT_IN_FLIGHT') end,
    'paid_to_date',v_paid,
    'current_paid',v_paid);

  v_record_version:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_OFFICE_PRESENTATION_VERSION_V1',pg_catalog.jsonb_build_object(
      'timesheet_id',v_timesheet.timesheet_id,'timesheet_version',v_timesheet.version,
      'publication_id',v_publication.id,'publication_issue_hash',case when v_publication.id is null then null else pg_catalog.encode(v_publication.issue_set_hash,'hex') end,
      'timesheet_financial_id',v_tsfin.id,
      'timesheet_financial_evidence_hash',case when v_tsfin.id is null then null
        else pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_OFFICE_TIMESHEET_FINANCIAL_VERSION_V1',to_jsonb(v_tsfin)
        ),'hex') end,
      'protected_family_id',v_family.id,'protected_family_version',v_family.bound_version,
      'comparison_state',v_comparison_state,
      -- Gate 9: the record version must move when the PHASE moves, or a stale
      -- screen could post an action the new phase no longer permits.
      'lifecycle_ok',v_lifecycle->'ok',
      'server_phase',v_lifecycle->'server_phase',
      'lifecycle_errors',v_lifecycle->'errors',
      'proposal_state',v_lifecycle#>'{proposal,state}',
      'proposal_bundle_revision',v_lifecycle#>'{proposal,bundle_revision}',
      'settlement_state',v_settlement->'state',
      'settlement_last_settled_at_utc',v_settlement->'last_settled_at_utc'
    )),'hex');

  return pg_catalog.jsonb_build_object(
    'applicable',true,'contract','WEEKLY_SOURCE_OFFICE_PRESENTATION_V1','scope','WEEKLY',
    'record_version',v_record_version,'freshness','CURRENT','route',v_route,'authority',v_authority,
    'source_expense_policy',v_source_expense_policy,
    'submitted_timesheet_available',v_submitted_available,'submitted_timesheet_complete',v_submitted_complete,
    'comparison',pg_catalog.jsonb_build_object('state',v_comparison_state,
      'source_rows',v_source_rows,'submitted_rows',v_submitted_rows,'rows','[]'::jsonb),
    'approved_rows',v_approved_rows,
    'totals',pg_catalog.jsonb_build_object('complete',v_totals_complete,
      'charge_excluding_vat',private.weekly_source_office_money_text_v1(v_charge_ex),
      'charge_including_vat',private.weekly_source_office_money_text_v1(v_charge_inc),
      'pay_excluding_vat',private.weekly_source_office_money_text_v1(v_pay_ex),
      'pay_including_vat',private.weekly_source_office_money_text_v1(v_pay_inc)),
    'action_state',pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
      'authorise_allowed',v_authorise_allowed,'blocked_reason',v_blocked_reason,
      'manage_approved_hours_allowed',v_manage_allowed,
      'manage_approved_hours',case when v_manage_allowed then pg_catalog.jsonb_build_object(
        'endpoint','/api/weekly-source/v1/commands','expected_record_version',v_record_version,
        'new_item',pg_catalog.jsonb_build_object('allowed',true,'action','APPROVE_PROTECTED_HOURS',
          'command_payload',pg_catalog.jsonb_build_object('source_cycle_id',v_cycle.id,
            'candidate_id',v_contract.candidate_id,'client_id',v_contract.client_id,
            'contract_id',v_contract.id,'week_ending_date',v_timesheet.week_ending_date,
            'evidence_timesheet_id',v_timesheet.timesheet_id)),
        'items',v_manage_items) else null end,
      'add_additional_expense_timesheet_allowed',v_add_expense_allowed
    ))
    -- Gate 9 item G9-4.  `unauthorise_allowed`, the W1 to W9 refusal kind and
    -- its permanence, and the withdrawn state.  None of those checks is
    -- computed here: the bridge calls the withdrawal owner's own read-only
    -- availability function and reports its verdict (`proof/36 section 3` and
    -- section 4).  Merged AFTER `jsonb_strip_nulls` so that every member
    -- survives and so the existing members are byte-identical to before.
    ||private.weekly_source_office_unauthorise_action_state_v1(v_timesheet.timesheet_id)
  )
  -- Gate 9 G9-1: the server-owned phase, its heading, its immutable schedules
  -- and its permitted new actions.  Gate 9 G9-3: the proposal, carried on the
  -- lifecycle object because a proposal IS part of the phase, and repeated at
  -- the top level for the decision surface.  Gate 9 G9-5: `UI-022` arrives as a
  -- phase, and the invoice movement history is returned SEPARATELY so it can
  -- never be mistaken for Candidate paid hours.
  ||pg_catalog.jsonb_build_object(
    'lifecycle',v_lifecycle||pg_catalog.jsonb_build_object('schedules',v_schedules),
    'candidate_lifecycle',v_candidate_lifecycle,
    'proposal',v_lifecycle->'proposal',
    'invoice_movement_history',v_lifecycle->'invoice_movements');
exception
  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    raise exception 'WEEKLY_SOURCE_PRESENTATION_REQUEST_INVALID' using errcode='22023';
end;
$function$;

create or replace function public.weekly_source_office_bulk_query_action_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','source_cycle_id','projection_publication_id',
    'expected_workspace_version','action','selection'
  ]::text[];
  v_selection_allowed constant text[]:=array[
    'mode','group_keys','excluded_group_keys','incident_ids','filters',
    'sort_key','sort_direction','selection_proof','group_selection_proofs'
  ]::text[];
  v_filter_allowed constant text[]:=array['status','candidate','issue']::text[];
  v_unknown text;
  v_actor uuid;
  v_cycle_id uuid;
  v_publication_id uuid;
  v_expected_version text;
  v_current_version text;
  v_action text;
  v_selection jsonb;
  v_mode text;
  v_filters jsonb;
  v_group_selection_proofs jsonb;
  v_selection_proof text;
  v_expected_selection_proof text;
  v_group_keys text[]:='{}'::text[];
  v_excluded_keys text[]:='{}'::text[];
  v_current_keys text[]:='{}'::text[];
  v_selected_keys text[]:='{}'::text[];
  v_incident_ids uuid[]:='{}'::uuid[];
  v_cycle public.weekly_source_cycles%rowtype;
  v_guard jsonb;
  v_group record;
  v_candidate record;
  v_client record;
  v_route record;
  v_scopes jsonb;
  v_exact_incidents uuid[];
  v_owner_result jsonb;
  v_results jsonb:='[]'::jsonb;
  v_exclusions jsonb:='[]'::jsonb;
  v_selected_count integer:=0;
  v_included_count integer:=0;
  v_included_issue_count integer:=0;
  v_excluded_count integer:=0;
  v_candidate_available boolean;
  v_candidate_results jsonb;
  v_candidate_issue_count integer;
  v_candidate_failed boolean;
  v_message text;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_BULK_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_BULK_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_expected_version:=p_request->>'expected_workspace_version';
    v_action:=pg_catalog.upper(pg_catalog.btrim(p_request->>'action'));
    v_selection:=p_request->'selection';
  exception when others then
    raise exception 'WEEKLY_SOURCE_BULK_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_cycle_id is null or v_publication_id is null
     or coalesce(v_expected_version,'')!~ '^[0-9a-f]{64}$'
     or v_action not in ('ASK_CANDIDATES','SEND_MANAGER_NOW','ACCEPT_SYSTEM_HOURS')
     or pg_catalog.jsonb_typeof(v_selection)<>'object' then
    raise exception 'WEEKLY_SOURCE_BULK_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(v_selection) key
  where not (key=any(v_selection_allowed)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  v_mode:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_selection->>'mode','')));
  if v_mode not in ('ALL_FILTERED','EXPLICIT')
     or pg_catalog.jsonb_typeof(coalesce(v_selection->'group_keys','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(v_selection->'excluded_group_keys','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(v_selection->'incident_ids','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(v_selection->'group_selection_proofs','[]'::jsonb))<>'array'
     or pg_catalog.jsonb_typeof(coalesce(v_selection->'filters','{}'::jsonb))<>'object' then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_INVALID' using errcode='22023';
  end if;
  v_filters:=coalesce(v_selection->'filters','{}'::jsonb);
  v_group_selection_proofs:=coalesce(v_selection->'group_selection_proofs','[]'::jsonb);
  v_selection_proof:=nullif(pg_catalog.lower(pg_catalog.btrim(v_selection->>'selection_proof')),'');
  select key into v_unknown from pg_catalog.jsonb_object_keys(v_filters) key
  where not (key=any(v_filter_allowed)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_BULK_FILTER_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  if exists(
    select 1 from pg_catalog.jsonb_each(v_filters) item
    where pg_catalog.jsonb_typeof(item.value)<>'string'
  )
     or coalesce(v_filters->>'status','UNRESOLVED') not in ('UNRESOLVED','ALL')
     or coalesce(v_filters->>'issue','ALL') not in ('ALL','TIMESHEET_MISSING','HOURS_DIFFER','MISSING_SHIFT')
     or pg_catalog.char_length(coalesce(v_filters->>'candidate',''))>120
     or coalesce(v_selection->>'sort_key','candidate') not in (
       'candidate','client','issues','candidate_asked','manager_informed','status','age'
     )
     or pg_catalog.lower(coalesce(v_selection->>'sort_direction','asc')) not in ('asc','desc')
     or (v_selection_proof is not null and v_selection_proof!~ '^[0-9a-f]{64}$') then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_INVALID' using errcode='22023';
  end if;
  begin
    select coalesce(pg_catalog.array_agg(value order by value),'{}'::text[]) into v_group_keys
    from pg_catalog.jsonb_array_elements_text(coalesce(v_selection->'group_keys','[]'::jsonb)) value;
    select coalesce(pg_catalog.array_agg(value order by value),'{}'::text[]) into v_excluded_keys
    from pg_catalog.jsonb_array_elements_text(coalesce(v_selection->'excluded_group_keys','[]'::jsonb)) value;
    select coalesce(pg_catalog.array_agg(value::uuid order by value::uuid),'{}'::uuid[]) into v_incident_ids
    from pg_catalog.jsonb_array_elements_text(coalesce(v_selection->'incident_ids','[]'::jsonb)) value;
  exception when others then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_INVALID' using errcode='22023';
  end;
  if exists(select 1 from pg_catalog.unnest(v_group_keys) key where key!~ '^qg_[0-9a-f]{64}$')
     or exists(select 1 from pg_catalog.jsonb_array_elements(coalesce(v_selection->'group_keys','[]'::jsonb)) item
       where pg_catalog.jsonb_typeof(item)<>'string')
     or exists(select 1 from pg_catalog.jsonb_array_elements(coalesce(v_selection->'excluded_group_keys','[]'::jsonb)) item
       where pg_catalog.jsonb_typeof(item)<>'string')
     or exists(select 1 from pg_catalog.jsonb_array_elements(coalesce(v_selection->'incident_ids','[]'::jsonb)) item
       where pg_catalog.jsonb_typeof(item)<>'string')
     or exists(select 1 from pg_catalog.jsonb_array_elements(v_group_selection_proofs) item
       where pg_catalog.jsonb_typeof(item)<>'object'
          or exists(select 1 from pg_catalog.jsonb_object_keys(item) key
            where key not in ('group_key','selection_proof'))
          or pg_catalog.jsonb_typeof(item->'group_key')<>'string'
          or pg_catalog.jsonb_typeof(item->'selection_proof')<>'string'
          or coalesce(item->>'group_key','')!~ '^qg_[0-9a-f]{64}$'
          or coalesce(item->>'selection_proof','')!~ '^[0-9a-f]{64}$')
     or exists(select 1 from pg_catalog.unnest(v_excluded_keys) key where key!~ '^qg_[0-9a-f]{64}$')
     or pg_catalog.cardinality(v_group_keys)<>(select pg_catalog.count(distinct key) from pg_catalog.unnest(v_group_keys) key)
     or pg_catalog.cardinality(v_excluded_keys)<>(select pg_catalog.count(distinct key) from pg_catalog.unnest(v_excluded_keys) key)
     or pg_catalog.cardinality(v_incident_ids)<>(select pg_catalog.count(distinct id) from pg_catalog.unnest(v_incident_ids) id)
     or (v_mode='ALL_FILTERED' and pg_catalog.cardinality(v_group_keys)<>0)
     or (v_mode='EXPLICIT' and pg_catalog.cardinality(v_group_keys)=0 and pg_catalog.cardinality(v_incident_ids)=0)
     or (v_mode='EXPLICIT' and pg_catalog.cardinality(v_excluded_keys)<>0)
     or (v_mode='ALL_FILTERED' and pg_catalog.cardinality(v_incident_ids)<>0)
     or (v_action<>'ACCEPT_SYSTEM_HOURS' and pg_catalog.cardinality(v_incident_ids)<>0)
     or (v_action<>'ACCEPT_SYSTEM_HOURS' and pg_catalog.jsonb_array_length(v_group_selection_proofs)<>0)
     or (v_action='ACCEPT_SYSTEM_HOURS' and (
       v_mode<>'EXPLICIT' or pg_catalog.cardinality(v_group_keys)=0
       or pg_catalog.cardinality(v_incident_ids)=0
       or pg_catalog.jsonb_array_length(v_group_selection_proofs)<>pg_catalog.cardinality(v_group_keys)
       or (select pg_catalog.count(distinct item->>'group_key')
           from pg_catalog.jsonb_array_elements(v_group_selection_proofs) item)
          <>pg_catalog.cardinality(v_group_keys)
     )) then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_INVALID' using errcode='22023';
  end if;

  v_guard:=private.weekly_source_query_current_publication_v1(v_cycle_id,v_publication_id);
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_cycle_id for update;
  v_current_version:=private.weekly_source_office_workspace_version_v1(v_cycle_id,v_publication_id);
  if v_current_version is distinct from v_expected_version then
    raise exception 'WEEKLY_SOURCE_BULK_WORKSPACE_STALE' using errcode='40001';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,case v_action when 'SEND_MANAGER_NOW' then 'SEND_MANAGER'
      when 'ACCEPT_SYSTEM_HOURS' then 'ACCEPT_SYSTEM_HOURS' else 'ASK_CANDIDATES' end,
    (v_guard->>'source_group_id')::uuid,null,(v_guard->>'finalisation_week_ending')::date
  );
  select coalesce(pg_catalog.array_agg(query.group_key order by query.group_key),'{}'::text[])
  into v_current_keys
  from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query;
  v_expected_selection_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_BULK_FILTER_SELECTION_V1',pg_catalog.jsonb_build_object(
      'action',v_action,'source_cycle_id',v_cycle_id,
      'projection_publication_id',v_publication_id,
      'workspace_version',v_current_version,'filters',v_filters,
      'group_keys',to_jsonb(v_current_keys)
    )),'hex');
  if v_action in ('ASK_CANDIDATES','SEND_MANAGER_NOW')
     and v_selection_proof is distinct from v_expected_selection_proof then
    raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
  end if;
  if v_mode='ALL_FILTERED' then
    if exists(select 1 from pg_catalog.unnest(v_excluded_keys) key where not (key=any(v_current_keys))) then
      raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
    end if;
    select coalesce(pg_catalog.array_agg(key order by key),'{}'::text[]) into v_selected_keys
    from pg_catalog.unnest(v_current_keys) key where not (key=any(v_excluded_keys));
  else
    if exists(select 1 from pg_catalog.unnest(v_group_keys) key where not (key=any(v_current_keys))) then
      raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
    end if;
    v_selected_keys:=v_group_keys;
  end if;
  v_selected_count:=pg_catalog.cardinality(v_selected_keys);

  if v_action='ACCEPT_SYSTEM_HOURS' then
    if exists(
      select 1 from pg_catalog.jsonb_array_elements(v_group_selection_proofs) proof
      where not ((proof->>'group_key')=any(v_selected_keys))
    ) then
      raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
    end if;
    for v_group in
      select query.group_key,query.accept_incident_ids
      from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
      where query.group_key=any(v_selected_keys)
      order by query.group_key
    loop
      if pg_catalog.cardinality(v_group.accept_incident_ids)=0
         or not (v_group.accept_incident_ids && v_incident_ids)
         or not exists(
           select 1 from pg_catalog.jsonb_array_elements(v_group_selection_proofs) proof
           where proof->>'group_key'=v_group.group_key
             and proof->>'selection_proof'=pg_catalog.encode(
               private.weekly_source_sha256_jsonb_v1(
                 'WEEKLY_SOURCE_ACCEPT_GROUP_SELECTION_V1',pg_catalog.jsonb_build_object(
                   'workspace_version',v_current_version,
                   'group_key',v_group.group_key,
                   'incident_ids',to_jsonb(v_group.accept_incident_ids)
                 )
               ),'hex'
             )
         ) then
        raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
      end if;
    end loop;
    if exists(
      select 1
      from pg_catalog.unnest(v_incident_ids) selected_incident(incident_id)
      left join (
        select eligible_group.group_key,eligible_incident.incident_id
        from private.weekly_source_office_query_groups_v1(
          v_cycle_id,v_publication_id,v_filters
        ) eligible_group
        cross join lateral pg_catalog.unnest(
          eligible_group.accept_incident_ids
        ) eligible_incident(incident_id)
        where eligible_group.group_key=any(v_selected_keys)
      ) eligible on eligible.incident_id=selected_incident.incident_id
      group by selected_incident.incident_id
      having pg_catalog.count(eligible.group_key)<>1
    ) then
      raise exception 'WEEKLY_SOURCE_BULK_SELECTION_STALE' using errcode='40001';
    end if;
    v_owner_result:=public.weekly_source_query_accept_system_hours_atomic_v1(
      pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
        'projection_publication_id',v_publication_id,'incident_ids',to_jsonb(v_incident_ids))
    );
    return pg_catalog.jsonb_build_object('ok',true,'action',v_action,'status','COMPLETE',
      'selected_count',v_selected_count,'included_count',v_selected_count,
      'included_issue_count',pg_catalog.cardinality(v_incident_ids),'excluded_count',0,
      'excluded','[]'::jsonb,'result',v_owner_result);
  end if;

  if v_action='ASK_CANDIDATES' then
    for v_group in
      select query.group_key
      from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
      where query.group_key=any(v_selected_keys)
        and not (query.outreach_eligible or not query.candidate_app_available)
      order by query.group_key
    loop
      v_excluded_count:=v_excluded_count+1;
      if pg_catalog.jsonb_array_length(v_exclusions)<100 then
        v_exclusions:=v_exclusions||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'group_key',v_group.group_key,'status','UNAVAILABLE_FOR_CANDIDATE_QUERIES'
        ));
      end if;
    end loop;
    for v_candidate in
      select query.candidate_id,
        bool_and(query.candidate_app_available) candidate_app_available,
        pg_catalog.array_agg(query.group_key order by query.group_key) group_keys
      from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
      where query.group_key=any(v_selected_keys)
        and (query.outreach_eligible or not query.candidate_app_available)
      group by query.candidate_id order by query.candidate_id
    loop
      select pg_catalog.count(*)=1 into v_candidate_available
      from public.candidate_app_global_membership_links membership
      join public.candidate_app_accounts account on account.id=membership.account_id
      where membership.candidate_id=v_candidate.candidate_id
        and membership.state='ACTIVE' and account.status='ACTIVE';
      if not v_candidate_available then
        v_excluded_count:=v_excluded_count+pg_catalog.cardinality(v_candidate.group_keys);
        if pg_catalog.jsonb_array_length(v_exclusions)<100 then
          v_exclusions:=v_exclusions||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'candidate_id',v_candidate.candidate_id,'group_keys',to_jsonb(v_candidate.group_keys),
            'status','UNAVAILABLE_NO_ACTIVE_APP_ACCOUNT'
          ));
        end if;
        continue;
      end if;

      v_candidate_results:='[]'::jsonb;
      v_candidate_issue_count:=0;
      v_candidate_failed:=false;
      begin
        select coalesce(pg_catalog.jsonb_agg(scope.value order by
          (scope.value->>'week_ending')::date,(scope.value->>'client_id')::uuid,(scope.value->>'contract_id')::uuid),'[]'::jsonb)
        into v_scopes
        from (
          select distinct on (value->>'week_ending',value->>'client_id',value->>'contract_id') value
          from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
          cross join lateral pg_catalog.jsonb_array_elements(query.missing_scopes) value
          where query.group_key=any(v_selected_keys) and query.candidate_id=v_candidate.candidate_id
            and (query.outreach_eligible or not query.candidate_app_available)
          order by value->>'week_ending',value->>'client_id',value->>'contract_id',value::text
        ) scope;
        if pg_catalog.jsonb_array_length(v_scopes)>0 then
          v_owner_result:=public.weekly_source_timesheet_submission_request_start_atomic_v1(
            pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
              'projection_publication_id',v_publication_id,'candidate_id',v_candidate.candidate_id,
              'scopes',v_scopes)
          );
          v_candidate_results:=v_candidate_results||pg_catalog.jsonb_build_array(v_owner_result);
          v_candidate_issue_count:=v_candidate_issue_count+pg_catalog.jsonb_array_length(v_scopes);
        end if;

        for v_client in
          select query.client_id,
            pg_catalog.array_agg(query.group_key order by query.group_key) group_keys,
            bool_or(pg_catalog.jsonb_array_length(query.missing_scopes)>0) has_missing_scope
          from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
          where query.group_key=any(v_selected_keys) and query.candidate_id=v_candidate.candidate_id
            and (query.outreach_eligible or not query.candidate_app_available)
          group by query.client_id order by query.client_id
        loop
          if v_client.has_missing_scope then continue; end if;
          select pg_catalog.array_agg(incident.id order by incident.id) into v_exact_incidents
          from public.weekly_discrepancy_incidents incident
          join public.weekly_issue_comparison_revisions comparison
            on comparison.id=incident.current_comparison_revision_id
           and comparison.projection_publication_id=v_publication_id
          where incident.source_cycle_id=v_cycle_id
            and incident.candidate_id=v_candidate.candidate_id
            and incident.client_id=v_client.client_id and incident.state='OPEN'
            and incident.candidate_action_state not in ('RESPONDED','NOT_REQUIRED');
          if coalesce(pg_catalog.cardinality(v_exact_incidents),0)=0 then continue; end if;
          v_owner_result:=public.weekly_source_query_ask_candidate_atomic_v1(
            pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
              'projection_publication_id',v_publication_id,'candidate_id',v_candidate.candidate_id,
              'client_id',v_client.client_id,'incident_ids',to_jsonb(v_exact_incidents))
          );
          v_candidate_results:=v_candidate_results||pg_catalog.jsonb_build_array(v_owner_result);
          v_candidate_issue_count:=v_candidate_issue_count+pg_catalog.cardinality(v_exact_incidents);
        end loop;
      exception when sqlstate '55000' then
        get stacked diagnostics v_message=message_text;
        if v_message='WEEKLY_SOURCE_CANDIDATE_APP_UNAVAILABLE' then
          v_candidate_failed:=true;
        else
          raise;
        end if;
      end;

      if v_candidate_failed then
        v_excluded_count:=v_excluded_count+pg_catalog.cardinality(v_candidate.group_keys);
        if pg_catalog.jsonb_array_length(v_exclusions)<100 then
          v_exclusions:=v_exclusions||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'candidate_id',v_candidate.candidate_id,'group_keys',to_jsonb(v_candidate.group_keys),
            'status','UNAVAILABLE_NO_ACTIVE_APP_ACCOUNT'
          ));
        end if;
      else
        v_results:=v_results||v_candidate_results;
        v_included_count:=v_included_count+pg_catalog.cardinality(v_candidate.group_keys);
        v_included_issue_count:=v_included_issue_count+v_candidate_issue_count;
      end if;
    end loop;
  else
    for v_group in
      select query.*
      from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
      where query.group_key=any(v_selected_keys)
      order by query.group_key
    loop
      if not v_group.manager_eligible or pg_catalog.cardinality(v_group.incident_ids)=0
         or pg_catalog.jsonb_array_length(v_group.missing_scopes)>0 then
        v_excluded_count:=v_excluded_count+1;
        if pg_catalog.jsonb_array_length(v_exclusions)<100 then
          v_exclusions:=v_exclusions||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'group_key',v_group.group_key,'status','UNAVAILABLE_FOR_MANAGER'
          ));
        end if;
      else
        v_included_count:=v_included_count+1;
        v_included_issue_count:=v_included_issue_count+pg_catalog.cardinality(v_group.incident_ids);
      end if;
    end loop;
    for v_route in
      select query.manager_recipient_route_key,
        pg_catalog.array_agg(distinct incident_id order by incident_id) incident_ids
      from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters) query
      cross join lateral pg_catalog.unnest(query.incident_ids) incident_id
      where query.group_key=any(v_selected_keys) and query.manager_eligible
        and pg_catalog.jsonb_array_length(query.missing_scopes)=0
      group by query.manager_recipient_route_key
      order by pg_catalog.encode(query.manager_recipient_route_key,'hex')
    loop
      v_owner_result:=public.weekly_source_query_send_manager_now_atomic_v1(
        pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
          'projection_publication_id',v_publication_id,'incident_ids',to_jsonb(v_route.incident_ids))
      );
      v_results:=v_results||pg_catalog.jsonb_build_array(v_owner_result);
    end loop;
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'action',v_action,
    'status',case when v_included_count=0 then 'NO_ELIGIBLE_SELECTION'
      when v_excluded_count>0 then 'PARTLY_COMPLETE' else 'COMPLETE' end,
    'selected_count',v_selected_count,'included_count',v_included_count,
    'included_issue_count',v_included_issue_count,'excluded_count',v_excluded_count,
    'excluded',v_exclusions,'excluded_omitted_count',greatest(v_excluded_count-pg_catalog.jsonb_array_length(v_exclusions),0),
    'results',v_results,'workspace_version',v_current_version
  );
exception
  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    raise exception 'WEEKLY_SOURCE_BULK_REQUEST_INVALID' using errcode='22023';
end;
$function$;

create or replace function public.weekly_source_no_shifts_attest_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','source_cycle_id','source_group_id','client_id',
    'expected_cycle_version','attestation_text'
  ]::text[];
  v_unknown text;
  v_actor uuid;
  v_cycle_id uuid;
  v_group_id uuid;
  v_client_id uuid;
  v_expected_version bigint;
  v_attestation text;
  v_cycle public.weekly_source_cycles%rowtype;
  v_current public.weekly_source_client_cycle_completions%rowtype;
  v_generation integer;
  v_hash bytea;
  v_completion_id uuid;
  v_now timestamptz:=pg_catalog.transaction_timestamp();
  v_all_complete boolean;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  if pg_catalog.jsonb_typeof(p_request->'actor_user_id')<>'string'
     or pg_catalog.jsonb_typeof(p_request->'source_cycle_id')<>'string'
     or pg_catalog.jsonb_typeof(p_request->'source_group_id')<>'string'
     or pg_catalog.jsonb_typeof(p_request->'client_id')<>'string'
     or pg_catalog.jsonb_typeof(p_request->'expected_cycle_version')<>'number'
     or pg_catalog.jsonb_typeof(p_request->'attestation_text')<>'string' then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_REQUEST_INVALID' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_group_id:=(p_request->>'source_group_id')::uuid;
    v_client_id:=(p_request->>'client_id')::uuid;
    v_expected_version:=(p_request->>'expected_cycle_version')::bigint;
    v_attestation:=pg_catalog.btrim(p_request->>'attestation_text');
  exception when others then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_cycle_id is null or v_group_id is null or v_client_id is null
     or v_expected_version<0
     or pg_catalog.char_length(coalesce(v_attestation,'')) not between 1 and 500 then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_REQUEST_INVALID' using errcode='22023';
  end if;

  select * into v_cycle from public.weekly_source_cycles cycle
  where cycle.id=v_cycle_id for update;
  if not found or v_cycle.source_group_id<>v_group_id then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_CYCLE_NOT_FOUND' using errcode='22023';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'FINALISE_WEEK',v_group_id,v_client_id,v_cycle.finalisation_week_ending
  );

  select * into v_current
  from public.weekly_source_client_cycle_completions completion
  where completion.source_cycle_id=v_cycle.id and completion.client_id=v_client_id
    and completion.state='CURRENT'
  for update;
  if found and v_current.completion_kind='NO_SHIFTS_TO_IMPORT'
     and v_current.attested_by_user_id=v_actor
     and pg_catalog.btrim(coalesce(v_current.attestation_text,''))=v_attestation then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','NO_SHIFTS_ATTESTED','idempotent',true,
      'completion_id',v_current.id,'source_cycle_id',v_cycle.id,
      'source_group_id',v_group_id,'client_id',v_client_id,
      'cycle_state',v_cycle.state,'cycle_version',v_cycle.version
    );
  end if;
  if v_cycle.version<>v_expected_version then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_CYCLE_STALE' using errcode='40001';
  end if;
  if v_cycle.state not in ('OPEN','FINALISABLE') then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_CYCLE_STATE_INVALID' using errcode='55000';
  end if;
  if found and v_current.completion_kind='FINAL_SOURCE' then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_FINAL_SOURCE_EXISTS' using errcode='55000';
  end if;

  if exists(
    select 1 from public.weekly_source_report_scopes scope
    where scope.source_cycle_id=v_cycle.id and scope.client_id=v_client_id
      and (scope.current_complete_upload_id is not null
        or scope.current_projection_publication_id is not null
        or scope.current_final_revision_id is not null)
  ) then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_CLIENT_SOURCE_EXISTS' using errcode='55000';
  end if;
  if exists(
    select 1
    from public.weekly_source_uploads upload
    join public.weekly_source_upload_rows source_row on source_row.upload_id=upload.id
    join lateral (
      select resolution.* from public.weekly_source_row_resolutions resolution
      where resolution.upload_row_id=source_row.id
      order by resolution.generation desc,resolution.id desc limit 1
    ) resolution on resolution.mapping_state='RESOLVED'
    where upload.source_cycle_id=v_cycle.id
      and (upload.id=v_cycle.current_complete_upload_id or upload.state='CURRENT')
      and resolution.client_id=v_client_id
  ) or exists(
    select 1
    from public.weekly_source_final_revisions revision
    join public.weekly_source_final_snapshot_lines snapshot
      on snapshot.final_revision_id=revision.id
    where revision.source_cycle_id=v_cycle.id
      and revision.state in ('PREPARED','CURRENT')
      and snapshot.client_id=v_client_id
  ) then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_CLIENT_SOURCE_EXISTS' using errcode='55000';
  end if;
  if exists(
    select 1
    from public.weekly_source_uploads upload
    join public.weekly_source_upload_rows source_row on source_row.upload_id=upload.id
    left join lateral (
      select resolution.* from public.weekly_source_row_resolutions resolution
      where resolution.upload_row_id=source_row.id
      order by resolution.generation desc,resolution.id desc limit 1
    ) resolution on true
    where upload.source_cycle_id=v_cycle.id
      and (upload.id=v_cycle.current_complete_upload_id or upload.state='CURRENT')
      and resolution.mapping_state is distinct from 'RESOLVED'
  ) then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_UNRESOLVED_SOURCE_ROWS' using errcode='55000';
  end if;
  if exists(
    select 1 from public.weekly_discrepancy_incidents incident
    where incident.source_cycle_id=v_cycle.id and incident.client_id=v_client_id
      and incident.state='OPEN'
  ) then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_OPEN_ISSUE_EXISTS' using errcode='55000';
  end if;

  if v_current.id is not null then
    update public.weekly_source_client_cycle_completions completion
    set state='SUPERSEDED',superseded_at_utc=v_now
    where completion.id=v_current.id and completion.state='CURRENT';
    if not found then
      raise exception 'WEEKLY_SOURCE_NO_SHIFTS_COMPLETION_STALE' using errcode='40001';
    end if;
  end if;
  select coalesce(pg_catalog.max(completion.completion_generation),0)+1
  into v_generation
  from public.weekly_source_client_cycle_completions completion
  where completion.source_cycle_id=v_cycle.id and completion.client_id=v_client_id;
  v_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CLIENT_CYCLE_COMPLETION_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle.id,'source_group_id',v_group_id,
      'client_id',v_client_id,'completion_generation',v_generation,
      'completion_kind','NO_SHIFTS_TO_IMPORT','final_revision_id',null,
      'actor_user_id',v_actor,'attestation_text',v_attestation
    )
  );
  insert into public.weekly_source_client_cycle_completions(
    source_cycle_id,source_group_id,client_id,completion_generation,
    completion_kind,final_revision_id,attested_by_user_id,attested_at_utc,
    attestation_text,completion_hash,state
  ) values (
    v_cycle.id,v_group_id,v_client_id,v_generation,
    'NO_SHIFTS_TO_IMPORT',null,v_actor,v_now,
    v_attestation,v_hash,'CURRENT'
  ) returning id into v_completion_id;

  select not exists(
    select 1 from public.weekly_source_group_clients membership
    where membership.source_group_id=v_group_id
      and v_cycle.finalisation_week_ending between membership.valid_from
        and coalesce(membership.valid_to,'infinity'::date)
      and not exists(
        select 1 from public.weekly_source_client_cycle_completions completion
        where completion.source_cycle_id=v_cycle.id
          and completion.client_id=membership.client_id and completion.state='CURRENT'
      )
  ) into v_all_complete;
  if v_all_complete then
    update public.weekly_source_cycles cycle
    set state='FINALISED',finalised_at_utc=v_now,finalised_by_user_id=v_actor
    where cycle.id=v_cycle.id and cycle.version=v_expected_version
      and cycle.state in ('OPEN','FINALISABLE');
  else
    update public.weekly_source_cycles cycle
    set state='FINALISABLE'
    where cycle.id=v_cycle.id and cycle.version=v_expected_version
      and cycle.state in ('OPEN','FINALISABLE');
  end if;
  if not found then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_CYCLE_STALE' using errcode='40001';
  end if;
  if v_all_complete then
    perform private._weekly_source_settings_ensure_open_cycle_v1(
      v_group_id,v_now
    );
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status','NO_SHIFTS_ATTESTED','idempotent',false,
    'completion_id',v_completion_id,'source_cycle_id',v_cycle.id,
    'source_group_id',v_group_id,'client_id',v_client_id,
    'cycle_state',case when v_all_complete then 'FINALISED' else 'FINALISABLE' end,
    'cycle_version',v_expected_version
  );
exception
  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_REQUEST_INVALID' using errcode='22023';
end;
$function$;

alter function private.weekly_source_office_group_key_v1(uuid,uuid,uuid,bytea) owner to postgres;
alter function private.weekly_source_office_route_key_v1(uuid,uuid,uuid,uuid,date) owner to postgres;
alter function private.weekly_source_office_missing_scope_fingerprint_v1(uuid,uuid,uuid,uuid,date) owner to postgres;
alter function private.weekly_source_office_query_groups_v1(uuid,uuid,jsonb) owner to postgres;
alter function private.weekly_source_office_workspace_version_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_office_money_text_v1(numeric) owner to postgres;
alter function private.weekly_source_office_lifecycle_policy_v1() owner to postgres;
alter function private.weekly_source_office_lifecycle_row_v1(text) owner to postgres;
alter function private.weekly_source_office_lifecycle_result_v1(text,jsonb,jsonb,text) owner to postgres;
alter function private.weekly_source_office_schedule_absent_v1(text) owner to postgres;
alter function private.weekly_source_office_schedule_from_rows_v1(jsonb,text) owner to postgres;
alter function private.weekly_source_office_schedule_from_components_v1(jsonb,text) owner to postgres;
alter function private.weekly_source_office_schedule_from_allocation_v1(jsonb,text) owner to postgres;
alter function private.weekly_source_office_schedule_from_actual_v1(jsonb) owner to postgres;
alter function private.weekly_source_office_payment_progress_v1(uuid[]) owner to postgres;
alter function private.weekly_source_office_invoice_movements_v1(uuid[]) owner to postgres;
alter function private.weekly_source_office_proposal_revision_v1(uuid[],bytea) owner to postgres;
alter function private.weekly_source_office_proposal_view_v1(uuid,uuid[]) owner to postgres;
alter function private.weekly_source_office_lifecycle_phase_v1(uuid,jsonb) owner to postgres;
alter function private.weekly_source_office_candidate_phase_v1(uuid,timestamptz) owner to postgres;
alter function public.weekly_source_office_workspace_v1(jsonb) owner to postgres;
alter function public.weekly_source_office_timesheet_presentation_v1(jsonb) owner to postgres;
alter function public.weekly_source_office_bulk_query_action_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_no_shifts_attest_atomic_v1(jsonb) owner to postgres;

revoke all on function private.weekly_source_office_group_key_v1(uuid,uuid,uuid,bytea)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_route_key_v1(uuid,uuid,uuid,uuid,date)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_missing_scope_fingerprint_v1(uuid,uuid,uuid,uuid,date)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_query_groups_v1(uuid,uuid,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_workspace_version_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_money_text_v1(numeric)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_lifecycle_policy_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_lifecycle_row_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_lifecycle_result_v1(text,jsonb,jsonb,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_schedule_absent_v1(text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_schedule_from_rows_v1(jsonb,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_schedule_from_components_v1(jsonb,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_schedule_from_allocation_v1(jsonb,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_schedule_from_actual_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_payment_progress_v1(uuid[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_invoice_movements_v1(uuid[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_proposal_revision_v1(uuid[],bytea)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_proposal_view_v1(uuid,uuid[])
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_lifecycle_phase_v1(uuid,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_office_candidate_phase_v1(uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_office_workspace_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_office_timesheet_presentation_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_office_bulk_query_action_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_no_shifts_attest_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_office_workspace_v1(jsonb) to service_role;
grant execute on function public.weekly_source_office_timesheet_presentation_v1(jsonb) to service_role;
grant execute on function public.weekly_source_office_bulk_query_action_atomic_v1(jsonb) to service_role;
grant execute on function public.weekly_source_no_shifts_attest_atomic_v1(jsonb) to service_role;

notify pgrst, 'reload schema';

commit;
