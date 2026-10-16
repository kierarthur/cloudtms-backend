create or replace function private.weekly_contact_source_manager_selection_lock_v1(p_incidents uuid[],p_extra_pairs jsonb)
returns void language plpgsql volatile security definer set search_path='pg_catalog','public','private','pg_temp'
as $function$
declare source_group public.weekly_source_groups%rowtype;facts jsonb;pair record;
begin
 if p_incidents is null or cardinality(p_incidents)not between 1 and 500 or jsonb_typeof(p_extra_pairs)is distinct from 'array' then
  raise exception 'CONTACT_MANUAL_ENQUIRY_REQUEST_INVALID' using errcode='22023';end if;
 select groups.* into strict source_group from public.weekly_discrepancy_incidents incident
  join public.weekly_source_groups groups on groups.id=incident.source_group_id where incident.id=p_incidents[1];
 if exists(select 1 from unnest(p_incidents)selected(id) left join public.weekly_discrepancy_incidents incident on incident.id=selected.id
  left join public.weekly_source_groups groups on groups.id=incident.source_group_id where incident.id is null
   or(groups.agency_id,groups.environment)is distinct from(source_group.agency_id,source_group.environment)) then
  raise exception 'CONTACT_SCOPE_NOT_FOUND' using errcode='42501';end if;
 perform private.weekly_contact_deployment_lock_v1(source_group.agency_id,source_group.environment);
 perform private.weekly_contact_context_v1(source_group.agency_id,source_group.environment);
 facts:=private.weekly_contact_source_manager_selection_scope_facts_v1(p_incidents);
 for pair in select distinct(value->>'candidate_id')::uuid candidate_id,(value->>'client_id')::uuid client_id
  from jsonb_array_elements(facts->'pairs'||p_extra_pairs)order by 1,2 loop
  perform private.weekly_contact_scope_lock_v1(source_group.agency_id,source_group.environment,pair.candidate_id,pair.client_id);end loop;
 if private.weekly_contact_source_manager_selection_scope_facts_v1(p_incidents)is distinct from facts then
  raise exception 'CONTACT_MANUAL_ENQUIRY_REVIEW_STALE' using errcode='40001';end if;
end;
$function$;