-- PREPARED_NOT_RUN. Read-only fixture discovery, not eligibility or native PASS.
-- Run catalog readiness first. IDs only: no lease token, provider receipt, email,
-- recipient body, worker identity, signed URL or business-row payload is returned.
select 'ORIGINAL_START_RECOVERY_CANDIDATE'case_kind,a.id attempt_id,t.id target_id,c.agency_id
from public.weekly_message_target_attempts a
join public.weekly_message_dispatch_targets t on t.id=a.dispatch_target_id
join public.weekly_message_dispatch_commands c on c.id=t.dispatch_command_id
where c.environment='TEST' and a.lease_token is not null and nullif(a.lease_owner,'')is not null
order by a.id limit 1;

select 'ORIGINAL_COMPLETED_RESULT_CANDIDATE'case_kind,a.id attempt_id,t.id target_id,c.agency_id
from public.weekly_message_target_attempts a
join public.weekly_message_dispatch_targets t on t.id=a.dispatch_target_id
join public.weekly_message_dispatch_commands c on c.id=t.dispatch_command_id
where c.environment='TEST' and a.completed_at_utc is not null and a.result_hash is not null
order by a.id limit 1;

select 'CURRENT_MANUAL_SCOPE_CANDIDATE_NOT_YET_ELIGIBLE'case_kind,g.agency_id,i.candidate_id,i.client_id,
 i.source_cycle_id,p.id projection_publication_id,min(i.id::text)::uuid incident_id
from public.weekly_discrepancy_incidents i join public.weekly_source_groups g on g.id=i.source_group_id
join public.weekly_source_cycles cycle on cycle.id=i.source_cycle_id
join public.weekly_source_projection_publications p on p.id=cycle.current_projection_publication_id and p.state='CURRENT'
where g.environment='TEST' and g.active and i.state='OPEN'
group by g.agency_id,i.candidate_id,i.client_id,i.source_cycle_id,p.id order by g.agency_id,i.candidate_id,i.client_id limit 5;

select 'CURRENT_ASK_CANDIDATES_ACTOR_CANDIDATE'case_kind,id actor_user_id
from public.tms_users where is_active is true and lower(btrim(coalesce(role,'')))='admin' order by id limit 1;
