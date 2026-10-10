-- F-25 typed QuerySync branch digests; System completion domain preserved.
-- Design: F-25-query-sync-completion-build.md. No owner is finalized here.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_query_root_completion_digest_v1(p_header private.bpay_next_query_root_completion)
returns bytea language sql immutable security invoker set search_path=pg_catalog,private
as $function$
 select case when p_header.owner_kind='QUERY_SYNC' then
  pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_ROOT_COMPLETION_QUERY_V1',
    pg_catalog.encode(base.digest,'hex'),pg_catalog.encode(p_header.query_input_completions_sha256,'hex'),
    pg_catalog.encode(p_header.query_omission_completions_sha256,'hex'))::text,'UTF8'))
 else base.digest end from (select pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array('BPAY_NEXT_QUERY_ROOT_COMPLETION_V1',
  p_header.id,p_header.preparing_transaction_id::text,p_header.module_epoch,p_header.owner_kind,p_header.root_variant,
  pg_catalog.encode(p_header.request_sha256,'hex'),pg_catalog.encode(p_header.child_action_sha256,'hex'),
  p_header.source_cycle_id,p_header.projection_publication_id,p_header.actor_user_id,
  p_header.input_identity_count,p_header.input_identities,pg_catalog.encode(p_header.input_identities_sha256,'hex'),
  p_header.omitted_incident_count,p_header.omitted_incident_ids,pg_catalog.encode(p_header.omitted_incidents_sha256,'hex'),
  p_header.new_incident_count,p_header.changed_comparison_count,p_header.unchanged_incident_count,p_header.resolved_incident_count,
  p_header.actual_stage_count,p_header.first_stage_ordinal,p_header.last_stage_ordinal,
  pg_catalog.encode(p_header.raw_stages_sha256,'hex'),p_header.original_witness_count,
  pg_catalog.encode(p_header.original_witnesses_sha256,'hex'))::text,'UTF8')) as digest) base
$function$;


create or replace function private.bpay_next_query_sync_input_completion_digest_v1(p_row private.bpay_next_query_sync_input_completion)
returns bytea language sql immutable security invoker set search_path=pg_catalog,private
as $function$
 select sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_SYNC_INPUT_COMPLETION_V1',p_row.completion_id,p_row.work_event_id,
  encode(p_row.input_issue_sha256,'hex'),p_row.outcome,p_row.incident_id,p_row.issue_episode,p_row.comparison_revision_id,
  p_row.prior_comparison_revision_id,p_row.recheck_event_id,p_row.resolved_decision_event_id,encode(p_row.matched_facts_sha256,'hex'))::text,'UTF8'))
$function$;

create or replace function private.bpay_next_query_sync_omission_completion_digest_v1(p_row private.bpay_next_query_sync_omission_completion)
returns bytea language sql immutable security invoker set search_path=pg_catalog,private
as $function$
 select sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_SYNC_OMISSION_COMPLETION_V1',p_row.completion_id,p_row.incident_id,
   p_row.work_event_id,p_row.issue_episode,p_row.comparison_revision_id,p_row.resolved_event_id)::text,'UTF8'))
$function$;

create or replace function private.bpay_next_query_sync_children_digests_v1(p_completion_id uuid)
returns table(input_count bigint,omission_count bigint,input_sha256 bytea,omission_sha256 bytea)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select inputs.cnt,omissions.cnt,
  sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_SYNC_INPUT_SET_V1',inputs.cnt,inputs.manifest)::text,'UTF8')),
  sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_SYNC_OMISSION_SET_V1',omissions.cnt,omissions.manifest)::text,'UTF8'))
 from (select count(*) cnt,coalesce(jsonb_agg(jsonb_build_array(r.work_event_id,
   encode(private.bpay_next_query_sync_input_completion_digest_v1(r),'hex')) order by r.work_event_id),'[]'::jsonb) manifest
   from private.bpay_next_query_sync_input_completion r where r.completion_id=p_completion_id) inputs
 cross join (select count(*) cnt,coalesce(jsonb_agg(jsonb_build_array(r.incident_id,
   encode(private.bpay_next_query_sync_omission_completion_digest_v1(r),'hex')) order by r.incident_id),'[]'::jsonb) manifest
   from private.bpay_next_query_sync_omission_completion r where r.completion_id=p_completion_id) omissions
$function$;
alter function private.bpay_next_query_root_completion_digest_v1(private.bpay_next_query_root_completion) owner to postgres;
revoke all on function private.bpay_next_query_root_completion_digest_v1(private.bpay_next_query_root_completion) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_input_completion_digest_v1(private.bpay_next_query_sync_input_completion) owner to postgres;
revoke all on function private.bpay_next_query_sync_input_completion_digest_v1(private.bpay_next_query_sync_input_completion) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_omission_completion_digest_v1(private.bpay_next_query_sync_omission_completion) owner to postgres;
revoke all on function private.bpay_next_query_sync_omission_completion_digest_v1(private.bpay_next_query_sync_omission_completion) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_children_digests_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_sync_children_digests_v1(uuid) from public,anon,authenticated,service_role;
commit;
