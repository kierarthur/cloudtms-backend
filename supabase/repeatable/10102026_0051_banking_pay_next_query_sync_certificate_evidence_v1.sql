-- F-25 immutable QuerySync certificate and original evidence qualification.
-- Design: F-25-query-sync-completion-build.md. True-tail writer remains separate.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_query_sync_certificate_evidence_assert_v1(p_completion_id uuid)
returns void language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare header private.bpay_next_query_root_completion%rowtype;input_row private.bpay_next_query_sync_input_completion%rowtype;
 omitted private.bpay_next_query_sync_omission_completion%rowtype;comparison public.weekly_issue_comparison_revisions%rowtype;
 prior public.weekly_issue_comparison_revisions%rowtype;event public.weekly_discrepancy_events%rowtype;children record;
 work_ids uuid[];issue_hashes bytea[];omitted_ids uuid[];new_count bigint;changed_count bigint;unchanged_count bigint;
begin
 select h.* into strict header from private.bpay_next_query_root_completion h where h.id=p_completion_id and h.owner_kind='QUERY_SYNC' and h.root_variant='QUERY_SYNC';
 select * into strict children from private.bpay_next_query_sync_children_digests_v1(header.id);
 select coalesce(array_agg(r.work_event_id order by r.work_event_id),'{}'::uuid[]),
    coalesce(array_agg(r.input_issue_sha256 order by r.work_event_id),'{}'::bytea[]),
    count(*) filter(where r.outcome='NEW'),count(*) filter(where r.outcome='CHANGED'),count(*) filter(where r.outcome in ('UNCHANGED_OPEN','UNCHANGED_RESOLVED'))
 into work_ids,issue_hashes,new_count,changed_count,unchanged_count from private.bpay_next_query_sync_input_completion r where r.completion_id=header.id;
 select coalesce(array_agg(r.incident_id order by r.incident_id),'{}'::uuid[]) into omitted_ids from private.bpay_next_query_sync_omission_completion r where r.completion_id=header.id;
 if (children.input_count,children.omission_count,children.input_sha256,children.omission_sha256) is distinct from
     (header.input_identity_count,header.omitted_incident_count,header.query_input_completions_sha256,header.query_omission_completions_sha256)
   or (work_ids,omitted_ids,new_count,changed_count,unchanged_count,children.omission_count) is distinct from
     (header.input_identities,header.omitted_incident_ids,header.new_incident_count,header.changed_comparison_count,header.unchanged_incident_count,header.resolved_incident_count)
   or header.request_sha256 is distinct from private.bpay_next_query_sync_request_digest_v1(header.actor_user_id,header.source_cycle_id,header.projection_publication_id,work_ids,issue_hashes)
   or exists(select 1 from private.bpay_next_query_sync_omission_completion o where o.completion_id=header.id and o.work_event_id=any(work_ids)) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_CERTIFICATE_SET_UNBOUND';end if;
 for input_row in select r.* from private.bpay_next_query_sync_input_completion r where r.completion_id=header.id order by r.work_event_id loop
  if input_row.evidence_sha256 is distinct from private.bpay_next_query_sync_input_completion_digest_v1(input_row) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_INPUT_CERTIFICATE_DIGEST_INVALID';end if;
  select c.* into strict comparison from public.weekly_issue_comparison_revisions c where c.id=input_row.comparison_revision_id and c.incident_id=input_row.incident_id;
  if input_row.outcome='NEW' and comparison.revision_number<>1 then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_NEW_COMPARISON_INVALID';end if;
  if input_row.outcome='CHANGED' then
   select c.* into strict prior from public.weekly_issue_comparison_revisions c where c.id=input_row.prior_comparison_revision_id and c.incident_id=input_row.incident_id;
   if prior.material_comparison_fingerprint=comparison.material_comparison_fingerprint or prior.revision_number>=comparison.revision_number then
    raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_CHANGED_COMPARISON_INVALID';end if;
  end if;
  if input_row.outcome in ('NEW','CHANGED') then
   select e.* into strict event from public.weekly_discrepancy_events e where e.id=input_row.recheck_event_id;
   if (event.incident_id,event.issue_episode,event.projection_publication_id,event.expected_comparison_fingerprint,event.event_kind,event.actor_kind,
       event.actor_user_id,event.idempotency_key,event.bounded_payload_json->>'comparison_revision_id',event.bounded_payload_json->>'issue_family',event.bounded_payload_json->>'source_presence')
      is distinct from (input_row.incident_id,input_row.issue_episode,header.projection_publication_id,comparison.material_comparison_fingerprint,
       'SOURCE_RECHECKED'::text,'SYSTEM'::text,null::uuid,
       'SOURCE_RECHECKED:'||input_row.incident_id::text||':'||encode(comparison.material_comparison_fingerprint,'hex'),
       comparison.id::text,comparison.issue_family,comparison.source_presence) then
    raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_RECHECK_EVENT_UNBOUND';end if;
  elsif input_row.outcome='UNCHANGED_RESOLVED' then
   if input_row.matched_facts_sha256 is distinct from private.bpay_next_query_matched_facts_digest_v1(comparison.contract_id,comparison.issue_family,comparison.source_presence,
       comparison.candidate_start_at_local,comparison.candidate_end_at_local,comparison.candidate_break_minutes,
       comparison.system_start_at_local,comparison.system_end_at_local,comparison.system_break_minutes) then
    raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_HISTORICAL_FACTS_UNBOUND';end if;
   select e.* into strict event from public.weekly_discrepancy_events e where e.id=input_row.resolved_decision_event_id and e.issue_episode=input_row.issue_episode;
   perform private.bpay_next_query_resolved_evidence_assert_v1(comparison.id,event.id);
  end if;
 end loop;
 for omitted in select r.* from private.bpay_next_query_sync_omission_completion r where r.completion_id=header.id order by r.incident_id loop
  if omitted.evidence_sha256 is distinct from private.bpay_next_query_sync_omission_completion_digest_v1(omitted) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_OMISSION_CERTIFICATE_DIGEST_INVALID';end if;
  select c.* into strict comparison from public.weekly_issue_comparison_revisions c where c.id=omitted.comparison_revision_id and c.incident_id=omitted.incident_id;
  select e.* into strict event from public.weekly_discrepancy_events e where e.id=omitted.resolved_event_id;
  if (event.incident_id,event.issue_episode,event.projection_publication_id,event.expected_comparison_fingerprint,event.event_kind,event.actor_kind,
      event.actor_user_id,event.idempotency_key,event.bounded_payload_json->>'resolution') is distinct from
     (omitted.incident_id,omitted.issue_episode,header.projection_publication_id,null::bytea,'RESOLVED'::text,'SYSTEM'::text,null::uuid,
       'SOURCE_MATCHED:'||omitted.incident_id::text||':'||header.projection_publication_id::text,'SOURCE_MATCHED'::text) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_OMISSION_EVENT_UNBOUND';end if;
 end loop;
end
$function$;

create or replace function private.bpay_next_query_sync_expected_witnesses_v1(p_completion_id uuid)
returns table(witness_ordinal bigint,witness_kind text,comparison_revision_id uuid,discrepancy_event_id uuid,evidence_sha256 bytea)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
 with manifest as (
  select 'COMPARISON'::text kind,r.comparison_revision_id comparison,null::uuid event from private.bpay_next_query_sync_input_completion r where r.completion_id=p_completion_id
  union select 'COMPARISON',r.prior_comparison_revision_id,null::uuid from private.bpay_next_query_sync_input_completion r where r.completion_id=p_completion_id and r.outcome='CHANGED'
  union select 'COMPARISON',r.comparison_revision_id,null::uuid from private.bpay_next_query_sync_omission_completion r where r.completion_id=p_completion_id
  union select 'SOURCE_RECHECKED',null::uuid,r.recheck_event_id from private.bpay_next_query_sync_input_completion r where r.completion_id=p_completion_id and r.outcome in ('NEW','CHANGED')
  union select 'RESOLVED_DECISION',null::uuid,r.resolved_decision_event_id from private.bpay_next_query_sync_input_completion r where r.completion_id=p_completion_id and r.outcome='UNCHANGED_RESOLVED'
  union select 'SOURCE_MATCHED',null::uuid,r.resolved_event_id from private.bpay_next_query_sync_omission_completion r where r.completion_id=p_completion_id
 ) select row_number() over(order by case m.kind when 'COMPARISON' then 1 when 'SOURCE_RECHECKED' then 2 when 'RESOLVED_DECISION' then 3 when 'SOURCE_MATCHED' then 4 end,
    coalesce(m.comparison,m.event)),m.kind,m.comparison,m.event,private.bpay_next_query_original_evidence_digest_v1(m.kind,m.comparison,m.event)
 from manifest m
$function$;
alter function private.bpay_next_query_sync_certificate_evidence_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_sync_certificate_evidence_assert_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_expected_witnesses_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_sync_expected_witnesses_v1(uuid) from public,anon,authenticated,service_role;
commit;
