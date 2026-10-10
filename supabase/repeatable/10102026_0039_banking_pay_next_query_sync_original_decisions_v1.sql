-- F-25 genuine immutable historical decision qualification.
-- The original resolved-match predicate stays unchanged in its real owner.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_query_resolved_evidence_assert_v1(p_comparison_id uuid,p_resolved_event_id uuid)
returns void language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare comparison public.weekly_issue_comparison_revisions%rowtype;resolved public.weekly_discrepancy_events%rowtype;
 original public.weekly_discrepancy_events%rowtype;resolution text;request_key uuid;request_count integer;
begin
 select c.* into strict comparison from public.weekly_issue_comparison_revisions c where c.id=p_comparison_id;
 select e.* into strict resolved from public.weekly_discrepancy_events e where e.id=p_resolved_event_id;
 resolution:=resolved.bounded_payload_json->>'resolution';
 if resolved.event_kind<>'RESOLVED' or resolved.incident_id<>comparison.incident_id
   or resolved.expected_comparison_fingerprint is distinct from comparison.material_comparison_fingerprint
   or resolved.projection_publication_id is null then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESOLVED_EVIDENCE_UNBOUND';end if;
 if resolution='OFFICE_ACCEPTED_SYSTEM_HOURS' then
  if resolved.actor_kind<>'OFFICE' or resolved.actor_user_id is null or resolved.idempotency_key<>
     'OFFICE_ACCEPTED_RESOLVED:'||comparison.incident_id::text||':'||encode(comparison.material_comparison_fingerprint,'hex') then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESOLVED_OFFICE_KEY_INVALID';end if;
  select e.* into strict original from public.weekly_discrepancy_events e where e.event_kind='OFFICE_ACCEPTED'
    and e.idempotency_key='OFFICE_ACCEPTED:'||comparison.incident_id::text||':'||encode(comparison.material_comparison_fingerprint,'hex');
  if original.bounded_payload_json->>'resolution' is distinct from resolution then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESOLVED_OFFICE_PAIR_INVALID';end if;
 elsif resolution in ('MANAGER_CONFIRMED_SYSTEM_HOURS','CANDIDATE_CORRECTED') then
  if split_part(resolved.idempotency_key,':',1) is distinct from
     (case resolution when 'MANAGER_CONFIRMED_SYSTEM_HOURS' then 'MANAGER_CONFIRMED' else 'CANDIDATE_CORRECTED' end)
     or split_part(resolved.idempotency_key,':',2)!~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or split_part(resolved.idempotency_key,':',3)<>'' then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESOLVED_RESPONSE_KEY_INVALID';end if;
  select e.* into strict original from public.weekly_discrepancy_events e where e.id=split_part(resolved.idempotency_key,':',2)::uuid;
  if resolved.idempotency_key is distinct from
     (case resolution when 'MANAGER_CONFIRMED_SYSTEM_HOURS' then 'MANAGER_CONFIRMED:' else 'CANDIDATE_CORRECTED:' end)||original.id::text then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESOLVED_RESPONSE_KEY_INVALID';end if;
  if original.event_kind is distinct from (case resolution when 'MANAGER_CONFIRMED_SYSTEM_HOURS' then 'MANAGER_RESPONDED' else 'CANDIDATE_RESPONDED' end)
     or original.actor_kind is distinct from (case resolution when 'MANAGER_CONFIRMED_SYSTEM_HOURS' then 'MANAGER' else 'CANDIDATE' end)
     or (resolution='MANAGER_CONFIRMED_SYSTEM_HOURS' and coalesce(original.bounded_payload_json->>'response_kind','') not in ('SYSTEM_CORRECT','CANDIDATE_DID_NOT_WORK'))
     or (resolution='CANDIDATE_CORRECTED' and original.bounded_payload_json->>'choice' is distinct from 'CANDIDATE_WRONG')
     or coalesce(original.bounded_payload_json->>'request_idempotency_key','')!~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or coalesce(original.bounded_payload_json->>'request_item_count','')!~ '^([1-9][0-9]{0,2}|1000)$'
     or coalesce(original.bounded_payload_json->>'request_semantic_hash','')!~ '^[0-9a-f]{64}$'
     or coalesce(original.bounded_payload_json->>'response_fingerprint','')!~ '^[0-9a-f]{64}$' then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESOLVED_RESPONSE_INVALID';end if;
  request_key:=(original.bounded_payload_json->>'request_idempotency_key')::uuid;
  request_count:=(original.bounded_payload_json->>'request_item_count')::integer;
  if (resolution='MANAGER_CONFIRMED_SYSTEM_HOURS' and
       (coalesce(original.bounded_payload_json->>'review_item_id','')!~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or original.idempotency_key is distinct from 'MANAGER_RESPONSE:'||request_key::text||':'||(original.bounded_payload_json->>'review_item_id')))
    or (resolution='CANDIDATE_CORRECTED' and original.idempotency_key is distinct from 'CANDIDATE_RESPONSE:'||request_key::text||':'||original.incident_id::text)
    or (select count(*) from public.weekly_discrepancy_events e where e.event_kind=original.event_kind
      and e.bounded_payload_json->>'request_idempotency_key'=request_key::text)<>request_count
    or exists(select 1 from public.weekly_discrepancy_events e where e.event_kind=original.event_kind
      and e.bounded_payload_json->>'request_idempotency_key'=request_key::text and
       (e.actor_kind<>original.actor_kind or e.bounded_payload_json->>'request_item_count' is distinct from request_count::text
        or e.bounded_payload_json->>'request_semantic_hash' is distinct from original.bounded_payload_json->>'request_semantic_hash')) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESOLVED_RESPONSE_SET_INCOMPLETE';end if;
 else
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESOLVED_KIND_INVALID';
 end if;
 if (original.incident_id,original.issue_episode,original.projection_publication_id,original.expected_comparison_fingerprint,
      original.actor_kind,original.actor_user_id) is distinct from
     (resolved.incident_id,resolved.issue_episode,resolved.projection_publication_id,resolved.expected_comparison_fingerprint,
      resolved.actor_kind,resolved.actor_user_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_RESOLVED_ORIGINAL_PAIR_SCOPE_INVALID';end if;
end
$function$;
alter function private.bpay_next_query_resolved_evidence_assert_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_query_resolved_evidence_assert_v1(uuid,uuid) from public,anon,authenticated,service_role;
commit;
