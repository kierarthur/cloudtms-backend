-- F-25 explicit true-tail Query protocol completion; not a business reply.
-- Design: F-25-query-delivery-completion-witnesses.md. Original immutable
-- comparison/event IDs only; no raw request, contact, prose or money snapshot.
\set ON_ERROR_STOP on
begin;

create table private.bpay_next_query_root_completion (
 id uuid primary key,
 preparing_transaction_id xid8 not null,
 module_epoch bigint not null check(module_epoch>0),
 owner_kind text not null check(owner_kind in ('SYSTEM_HOURS_ACCEPT','QUERY_SYNC')),
 root_variant text not null check(root_variant in ('DIRECT_SYSTEM_HOURS','BULK_SYSTEM_HOURS','QUERY_SYNC')),
 request_sha256 bytea not null check(pg_catalog.octet_length(request_sha256)=32),
 child_action_sha256 bytea check(pg_catalog.octet_length(child_action_sha256)=32),
 source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
 projection_publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 input_identity_count bigint not null check(input_identity_count between 0 and 1000),
 input_identities uuid[] not null,
 input_identities_sha256 bytea not null check(pg_catalog.octet_length(input_identities_sha256)=32),
 omitted_incident_count bigint not null check(omitted_incident_count between 0 and 1000),
 omitted_incident_ids uuid[] not null,
 omitted_incidents_sha256 bytea not null check(pg_catalog.octet_length(omitted_incidents_sha256)=32),
 new_incident_count bigint not null check(new_incident_count between 0 and 1000),
 changed_comparison_count bigint not null check(changed_comparison_count between 0 and 1000),
 unchanged_incident_count bigint not null check(unchanged_incident_count between 0 and 1000),
 resolved_incident_count bigint not null check(resolved_incident_count between 0 and 1000),
 actual_stage_count bigint not null check(actual_stage_count between 0 and 1000),
 first_stage_ordinal bigint,last_stage_ordinal bigint,
 raw_stages_sha256 bytea not null check(pg_catalog.octet_length(raw_stages_sha256)=32),
 original_witness_count bigint not null check(original_witness_count between 0 and 3000),
 original_witnesses_sha256 bytea not null check(pg_catalog.octet_length(original_witnesses_sha256)=32),
 completion_sha256 bytea not null check(pg_catalog.octet_length(completion_sha256)=32),
 completed_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
 foreign key(id,preparing_transaction_id,module_epoch) references private.bpay_next_query_producer(id,preparing_transaction_id,module_epoch) on delete restrict,
 unique(id,module_epoch),
 check((root_variant='QUERY_SYNC')=(owner_kind='QUERY_SYNC')),
 check((root_variant='QUERY_SYNC')=(child_action_sha256 is null)),
 check((root_variant<>'DIRECT_SYSTEM_HOURS') or child_action_sha256=request_sha256),
 check(pg_catalog.cardinality(input_identities)=input_identity_count and pg_catalog.array_position(input_identities,null) is null
   and (input_identity_count=0 or (pg_catalog.array_ndims(input_identities)=1 and pg_catalog.array_lower(input_identities,1)=1))),
 check(pg_catalog.cardinality(omitted_incident_ids)=omitted_incident_count and pg_catalog.array_position(omitted_incident_ids,null) is null
   and (omitted_incident_count=0 or (pg_catalog.array_ndims(omitted_incident_ids)=1 and pg_catalog.array_lower(omitted_incident_ids,1)=1))),
 check((actual_stage_count=0 and first_stage_ordinal is null and last_stage_ordinal is null)
   or (actual_stage_count>0 and first_stage_ordinal is not null and last_stage_ordinal is not null and first_stage_ordinal between 1 and 1000 and last_stage_ordinal between first_stage_ordinal and 1000
     and actual_stage_count<=last_stage_ordinal-first_stage_ordinal+1)),
 check((owner_kind='QUERY_SYNC' and new_incident_count+changed_comparison_count+unchanged_incident_count=input_identity_count
     and resolved_incident_count=omitted_incident_count)
   or (owner_kind='SYSTEM_HOURS_ACCEPT' and new_incident_count=0 and changed_comparison_count=0 and unchanged_incident_count=0
     and input_identity_count>0 and omitted_incident_count=0 and resolved_incident_count=input_identity_count
     and original_witness_count=2*input_identity_count))
);

create table private.bpay_next_query_root_completion_witness (
 completion_id uuid not null references private.bpay_next_query_root_completion(id) on delete restrict,
 witness_ordinal bigint not null check(witness_ordinal between 1 and 3000),
 witness_kind text not null check(witness_kind in ('COMPARISON','SOURCE_RECHECKED','SOURCE_MATCHED','OFFICE_ACCEPTED','OFFICE_RESOLVED','RESOLVED_DECISION')),
 comparison_revision_id uuid references public.weekly_issue_comparison_revisions(id) on delete restrict,
 discrepancy_event_id uuid references public.weekly_discrepancy_events(id) on delete restrict,
 evidence_sha256 bytea not null check(pg_catalog.octet_length(evidence_sha256)=32),
 primary key(completion_id,witness_ordinal),
 unique(completion_id,witness_kind,comparison_revision_id),
 unique(completion_id,witness_kind,discrepancy_event_id),
 check((witness_kind='COMPARISON' and comparison_revision_id is not null and discrepancy_event_id is null)
   or (witness_kind<>'COMPARISON' and comparison_revision_id is null and discrepancy_event_id is not null))
);

alter table private.bpay_next_query_root_completion owner to postgres;
alter table private.bpay_next_query_root_completion enable row level security;
revoke all on private.bpay_next_query_root_completion from public,anon,authenticated,service_role;
alter table private.bpay_next_query_root_completion_witness owner to postgres;
alter table private.bpay_next_query_root_completion_witness enable row level security;
revoke all on private.bpay_next_query_root_completion_witness from public,anon,authenticated,service_role;

create trigger bpay_next_query_root_completion_immutable before update or delete on private.bpay_next_query_root_completion
 for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_root_completion_no_truncate before truncate on private.bpay_next_query_root_completion
 for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_root_completion_witness_immutable before update or delete on private.bpay_next_query_root_completion_witness
 for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_root_completion_witness_no_truncate before truncate on private.bpay_next_query_root_completion_witness
 for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();

-- Same-xid preclock insertion, typed digests, original evidence qualification
-- and exact complete root/selection checks attach in the owner repeatable.
commit;
