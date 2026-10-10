-- F-25 durable typed per-work QuerySync branch and omission evidence.
-- Design: F-25-query-sync-completion-build.md. No request/financial JSON.
-- True-tail guards/qualification attach in the QuerySync successor.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_query_root_completion
 add column query_input_completions_sha256 bytea,
 add column query_omission_completions_sha256 bytea,
 add constraint bpay_next_query_root_completion_query_children_ck check(
   (owner_kind='QUERY_SYNC' and query_input_completions_sha256 is not null and query_omission_completions_sha256 is not null
     and pg_catalog.octet_length(query_input_completions_sha256)=32 and pg_catalog.octet_length(query_omission_completions_sha256)=32)
   or (owner_kind<>'QUERY_SYNC' and query_input_completions_sha256 is null and query_omission_completions_sha256 is null));
create table private.bpay_next_query_sync_input_completion (
 completion_id uuid not null references private.bpay_next_query_root_completion(id) on delete restrict,
 work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
 outcome text not null check(outcome in ('NEW','CHANGED','UNCHANGED_OPEN','UNCHANGED_RESOLVED')),
 incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
 issue_episode integer not null check(issue_episode>=1),
 comparison_revision_id uuid not null references public.weekly_issue_comparison_revisions(id) on delete restrict,
 prior_comparison_revision_id uuid references public.weekly_issue_comparison_revisions(id) on delete restrict,
 recheck_event_id uuid references public.weekly_discrepancy_events(id) on delete restrict,
 resolved_decision_event_id uuid references public.weekly_discrepancy_events(id) on delete restrict,
 input_issue_sha256 bytea not null check(pg_catalog.octet_length(input_issue_sha256)=32),
 matched_facts_sha256 bytea check(pg_catalog.octet_length(matched_facts_sha256)=32),
 evidence_sha256 bytea not null check(pg_catalog.octet_length(evidence_sha256)=32),
 primary key(completion_id,work_event_id),
 unique(completion_id,incident_id),
 check((outcome='NEW' and prior_comparison_revision_id is null and recheck_event_id is not null and resolved_decision_event_id is null and matched_facts_sha256 is null)
  or (outcome='CHANGED' and prior_comparison_revision_id is not null and prior_comparison_revision_id<>comparison_revision_id
    and recheck_event_id is not null and resolved_decision_event_id is null and matched_facts_sha256 is null)
  or (outcome='UNCHANGED_OPEN' and prior_comparison_revision_id is null and recheck_event_id is null and resolved_decision_event_id is null and matched_facts_sha256 is null)
  or (outcome='UNCHANGED_RESOLVED' and prior_comparison_revision_id is null and recheck_event_id is null and resolved_decision_event_id is not null and matched_facts_sha256 is not null))
);
create table private.bpay_next_query_sync_omission_completion (
 completion_id uuid not null references private.bpay_next_query_root_completion(id) on delete restrict,
 incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
 work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
 issue_episode integer not null check(issue_episode>=1),
 comparison_revision_id uuid not null references public.weekly_issue_comparison_revisions(id) on delete restrict,
 resolved_event_id uuid not null references public.weekly_discrepancy_events(id) on delete restrict,
 evidence_sha256 bytea not null check(pg_catalog.octet_length(evidence_sha256)=32),
 primary key(completion_id,incident_id),
 unique(completion_id,resolved_event_id),
 unique(completion_id,work_event_id)
);
alter table private.bpay_next_query_sync_input_completion owner to postgres;
alter table private.bpay_next_query_sync_input_completion enable row level security;
revoke all on private.bpay_next_query_sync_input_completion from public,anon,authenticated,service_role;
alter table private.bpay_next_query_sync_omission_completion owner to postgres;
alter table private.bpay_next_query_sync_omission_completion enable row level security;
revoke all on private.bpay_next_query_sync_omission_completion from public,anon,authenticated,service_role;
create trigger bpay_next_query_sync_input_completion_immutable before update or delete on private.bpay_next_query_sync_input_completion
 for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_sync_input_completion_no_truncate before truncate on private.bpay_next_query_sync_input_completion
 for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_sync_omission_completion_immutable before update or delete on private.bpay_next_query_sync_omission_completion
 for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_query_sync_omission_completion_no_truncate before truncate on private.bpay_next_query_sync_omission_completion
 for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();
commit;
