-- F-25 finite typed completion evidence. No owner finalizer is added here.
-- Design: docs/bpay-next/design/F-25-query-delivery-completion-witnesses.md.
-- Immutable original fields only; raw stages exclude the completion digest.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_query_raw_stages_v1(p_producer_id uuid)
returns table(actual_stage_count bigint,first_stage_ordinal bigint,last_stage_ordinal bigint,raw_stages_sha256 bytea)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select count(*),min(e.transaction_ordinal),max(e.transaction_ordinal),
  pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_RAW_STAGES_V1',p_producer_id,
   coalesce(jsonb_agg(jsonb_build_array(e.id,e.preparing_transaction_id::text,e.module_epoch,e.transaction_ordinal,
     e.owner_relation,e.owner_row_id,e.old_present,e.new_present,pg_catalog.encode(e.old_sha256,'hex'),
     pg_catalog.encode(e.new_sha256,'hex'),q.candidate_id,q.root_timesheet_id,q.old_gate_tuple,q.new_gate_tuple)
    order by e.transaction_ordinal),'[]'::jsonb))::text,'UTF8'))
 from private.bpay_next_query_event_owner eo
 join private.bpay_next_input_version_event e on e.id=eo.event_id
 join private.bpay_next_query_input_version q on q.event_id=e.id
 where eo.producer_id=p_producer_id
$function$;

create or replace function private.bpay_next_query_original_evidence_digest_v1(
 p_witness_kind text,p_comparison_revision_id uuid,p_discrepancy_event_id uuid
) returns bytea language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare comparison public.weekly_issue_comparison_revisions%rowtype;event public.weekly_discrepancy_events%rowtype;
begin
 if p_witness_kind='COMPARISON' and p_comparison_revision_id is not null and p_discrepancy_event_id is null then
  select c.* into strict comparison from public.weekly_issue_comparison_revisions c where c.id=p_comparison_revision_id;
  return pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_ORIGINAL_COMPARISON_V1',
   comparison.id,comparison.incident_id,comparison.revision_number,comparison.projection_publication_id,
   comparison.comparison_upload_id,comparison.final_revision_id,comparison.candidate_timesheet_id,
   comparison.candidate_timesheet_revision,pg_catalog.encode(comparison.candidate_shift_fingerprint,'hex'),
   comparison.source_row_id,comparison.source_work_event_link_id,comparison.contract_id,comparison.issue_family,
   comparison.source_presence,comparison.candidate_start_at_local,comparison.candidate_end_at_local,
   comparison.candidate_break_minutes,comparison.system_start_at_local,comparison.system_end_at_local,
   comparison.system_break_minutes,pg_catalog.encode(comparison.material_comparison_fingerprint,'hex'))::text,'UTF8'));
 elsif p_witness_kind in ('SOURCE_RECHECKED','SOURCE_MATCHED','OFFICE_ACCEPTED','OFFICE_RESOLVED','RESOLVED_DECISION')
   and p_comparison_revision_id is null and p_discrepancy_event_id is not null then
  select e.* into strict event from public.weekly_discrepancy_events e where e.id=p_discrepancy_event_id;
  if (p_witness_kind='SOURCE_RECHECKED' and (event.event_kind<>'SOURCE_RECHECKED' or event.actor_kind<>'SYSTEM'))
   or (p_witness_kind='SOURCE_MATCHED' and (event.event_kind<>'RESOLVED' or event.actor_kind<>'SYSTEM'
     or event.bounded_payload_json->>'resolution' is distinct from 'SOURCE_MATCHED'))
   or (p_witness_kind='OFFICE_ACCEPTED' and (event.event_kind<>'OFFICE_ACCEPTED' or event.actor_kind<>'OFFICE'
     or event.bounded_payload_json->>'resolution' is distinct from 'OFFICE_ACCEPTED_SYSTEM_HOURS'))
   or (p_witness_kind='OFFICE_RESOLVED' and (event.event_kind<>'RESOLVED' or event.actor_kind<>'OFFICE'
     or event.bounded_payload_json->>'resolution' is distinct from 'OFFICE_ACCEPTED_SYSTEM_HOURS'))
   or (p_witness_kind='RESOLVED_DECISION' and (event.event_kind<>'RESOLVED'
     or coalesce(event.bounded_payload_json->>'resolution','') not in
       ('OFFICE_ACCEPTED_SYSTEM_HOURS','MANAGER_CONFIRMED_SYSTEM_HOURS','CANDIDATE_CORRECTED'))) then
   raise exception using errcode='23514',message='BPAY_NEXT_QUERY_ORIGINAL_EVENT_KIND_INVALID';end if;
  return pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_ORIGINAL_EVENT_V1',
   p_witness_kind,event.id,event.incident_id,event.issue_episode,event.projection_publication_id,
   pg_catalog.encode(event.expected_comparison_fingerprint,'hex'),event.event_kind,event.actor_kind,event.actor_user_id,
   event.idempotency_key,event.bounded_payload_json->>'resolution',event.bounded_payload_json->>'comparison_revision_id',
   event.bounded_payload_json->>'issue_family',event.bounded_payload_json->>'source_presence')::text,'UTF8'));
 end if;
 raise exception using errcode='22023',message='BPAY_NEXT_QUERY_ORIGINAL_EVIDENCE_INPUT_INVALID';
end
$function$;

create or replace function private.bpay_next_query_root_completion_digest_v1(p_header private.bpay_next_query_root_completion)
returns bytea language sql immutable security invoker set search_path=pg_catalog,private
as $function$
 select pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_ROOT_COMPLETION_V1',
  p_header.id,p_header.preparing_transaction_id::text,p_header.module_epoch,p_header.owner_kind,p_header.root_variant,
  pg_catalog.encode(p_header.request_sha256,'hex'),pg_catalog.encode(p_header.child_action_sha256,'hex'),
  p_header.source_cycle_id,p_header.projection_publication_id,p_header.actor_user_id,
  p_header.input_identity_count,p_header.input_identities,pg_catalog.encode(p_header.input_identities_sha256,'hex'),
  p_header.omitted_incident_count,p_header.omitted_incident_ids,pg_catalog.encode(p_header.omitted_incidents_sha256,'hex'),
  p_header.new_incident_count,p_header.changed_comparison_count,p_header.unchanged_incident_count,p_header.resolved_incident_count,
  p_header.actual_stage_count,p_header.first_stage_ordinal,p_header.last_stage_ordinal,
  pg_catalog.encode(p_header.raw_stages_sha256,'hex'),p_header.original_witness_count,
  pg_catalog.encode(p_header.original_witnesses_sha256,'hex'))::text,'UTF8'))
$function$;

alter function private.bpay_next_query_raw_stages_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_raw_stages_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_original_evidence_digest_v1(text,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_query_original_evidence_digest_v1(text,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_root_completion_digest_v1(private.bpay_next_query_root_completion) owner to postgres;
revoke all on function private.bpay_next_query_root_completion_digest_v1(private.bpay_next_query_root_completion) from public,anon,authenticated,service_role;
commit;
