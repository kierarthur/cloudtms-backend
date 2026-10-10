-- F-25 Candidate true-root and actual child-call protocol evidence.
-- Design: F-25-candidate-protocol-schema-build.md. No business reply cache,
-- raw submission, contact, credential or financial snapshot is retained.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_candidate_compare_completion (
 id uuid primary key,
 producer_id uuid not null,
 preparing_transaction_id xid8 not null,
 module_epoch bigint not null check(module_epoch>0),
 call_ordinal integer not null check(call_ordinal between 1 and 1000),
 caller_kind text not null check(caller_kind in ('MATERIALISE','PREFINAL')),
 call_request_sha256 bytea not null check(octet_length(call_request_sha256)=32),
 candidate_generation_id uuid references public.weekly_candidate_outreach_generations(id) on delete restrict,
 publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
 membership_id uuid references public.weekly_timesheet_submission_request_memberships(id) on delete restrict,
 timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
 timesheet_revision integer not null check(timesheet_revision>0),
 timesheet_hash bytea not null check(octet_length(timesheet_hash)=32),
 call_now_utc timestamptz,
 candidate_id uuid not null references public.candidates(id) on delete restrict,
 client_id uuid not null references public.clients(id) on delete restrict,
 contract_id uuid not null references public.contracts(id) on delete restrict,
 week_ending_date date not null,
 source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
 source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
 profile_code text not null,
 generated_issue_count integer not null check(generated_issue_count between 0 and 1000),
 generated_work_ids uuid[] not null,
 generated_issues_sha256 bytea not null check(octet_length(generated_issues_sha256)=32),
 omitted_count integer not null check(omitted_count between 0 and 1000),
 omitted_incident_ids uuid[] not null,
 new_count integer not null check(new_count between 0 and 1000),
 changed_count integer not null check(changed_count between 0 and 1000),
 unchanged_count integer not null check(unchanged_count between 0 and 1000),
 resolved_count integer not null check(resolved_count between 0 and 1000),
 open_issue_count integer not null check(open_issue_count>=0),
 outcome text not null check(outcome in ('MATCHED','ISSUES')),
 result_sha256 bytea not null check(octet_length(result_sha256)=32),
 input_completions_sha256 bytea not null check(octet_length(input_completions_sha256)=32),
 omission_completions_sha256 bytea not null check(octet_length(omission_completions_sha256)=32),
 original_witness_count integer not null check(original_witness_count between 0 and 4000),
 original_witnesses_sha256 bytea not null check(octet_length(original_witnesses_sha256)=32),
 begin_event_ordinal integer not null check(begin_event_ordinal between 0 and 1000),
 end_event_ordinal integer not null check(end_event_ordinal between 0 and 1000),
 actual_stage_count integer not null check(actual_stage_count between 0 and 1000),
 raw_stages_sha256 bytea not null check(octet_length(raw_stages_sha256)=32),
 completion_sha256 bytea not null check(octet_length(completion_sha256)=32),
 completed_at_utc timestamptz not null default transaction_timestamp(),
 foreign key(producer_id,preparing_transaction_id,module_epoch)
  references private.bpay_next_query_producer(id,preparing_transaction_id,module_epoch) on delete restrict,
 unique(producer_id,call_ordinal),unique(id,producer_id),
 check((caller_kind='MATERIALISE' and call_ordinal=1 and candidate_generation_id is not null and membership_id is not null)
    or (caller_kind='PREFINAL' and candidate_generation_id is null and membership_id is null)),
 check(new_count+changed_count+unchanged_count=generated_issue_count and resolved_count=omitted_count),
 check((outcome='MATCHED')=(open_issue_count=0)),
 check(end_event_ordinal>=begin_event_ordinal and actual_stage_count=end_event_ordinal-begin_event_ordinal
    and actual_stage_count=3*new_count+2*changed_count+resolved_count),
 check(cardinality(generated_work_ids)=generated_issue_count and array_position(generated_work_ids,null) is null
    and (generated_issue_count=0 or (array_ndims(generated_work_ids)=1 and array_lower(generated_work_ids,1)=1))),
 check(cardinality(omitted_incident_ids)=omitted_count and array_position(omitted_incident_ids,null) is null
    and (omitted_count=0 or (array_ndims(omitted_incident_ids)=1 and array_lower(omitted_incident_ids,1)=1)))
);

create table private.bpay_next_candidate_compare_input_completion (
 compare_completion_id uuid not null,
 producer_id uuid not null,
 work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
 outcome text not null check(outcome in ('NEW','CHANGED','UNCHANGED_OPEN','UNCHANGED_RESOLVED')),
 incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
 issue_episode integer not null check(issue_episode>=1),
 comparison_revision_id uuid not null references public.weekly_issue_comparison_revisions(id) on delete restrict,
 prior_comparison_revision_id uuid references public.weekly_issue_comparison_revisions(id) on delete restrict,
 recheck_event_id uuid references public.weekly_discrepancy_events(id) on delete restrict,
 resolved_decision_event_id uuid references public.weekly_discrepancy_events(id) on delete restrict,
 generated_issue_sha256 bytea not null check(octet_length(generated_issue_sha256)=32),
 matched_facts_sha256 bytea check(octet_length(matched_facts_sha256)=32),
 evidence_sha256 bytea not null check(octet_length(evidence_sha256)=32),
 foreign key(compare_completion_id,producer_id) references private.bpay_next_candidate_compare_completion(id,producer_id) on delete restrict,
 primary key(compare_completion_id,work_event_id),unique(compare_completion_id,incident_id),
 check((outcome='NEW' and prior_comparison_revision_id is null and recheck_event_id is not null and resolved_decision_event_id is null and matched_facts_sha256 is null)
  or (outcome='CHANGED' and prior_comparison_revision_id is not null and prior_comparison_revision_id<>comparison_revision_id
    and recheck_event_id is not null and resolved_decision_event_id is null and matched_facts_sha256 is null)
  or (outcome='UNCHANGED_OPEN' and prior_comparison_revision_id is null and recheck_event_id is null and resolved_decision_event_id is null and matched_facts_sha256 is null)
  or (outcome='UNCHANGED_RESOLVED' and prior_comparison_revision_id is null and recheck_event_id is null and resolved_decision_event_id is not null and matched_facts_sha256 is not null))
);
create table private.bpay_next_candidate_compare_omission_completion (
 compare_completion_id uuid not null,producer_id uuid not null,
 incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
 work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
 issue_episode integer not null check(issue_episode>=1),
 comparison_revision_id uuid not null references public.weekly_issue_comparison_revisions(id) on delete restrict,
 resolved_event_id uuid not null references public.weekly_discrepancy_events(id) on delete restrict,
 evidence_sha256 bytea not null check(octet_length(evidence_sha256)=32),
 foreign key(compare_completion_id,producer_id) references private.bpay_next_candidate_compare_completion(id,producer_id) on delete restrict,
 primary key(compare_completion_id,incident_id),unique(compare_completion_id,work_event_id),unique(compare_completion_id,resolved_event_id)
);

create table private.bpay_next_candidate_materialise_completion (
 producer_id uuid primary key,
 preparing_transaction_id xid8 not null,
 module_epoch bigint not null check(module_epoch>0),
 caller_kind text not null check(caller_kind in ('DIRECT','CANDIDATE_SELF_SUBMIT','CANDIDATE_APP_SUBMIT')),
 variant text not null check(variant in ('FRESH','EXACT_LAST_REPLAY')),
 request_key uuid not null,
 materialise_request_sha256 bytea not null check(octet_length(materialise_request_sha256)=32),
 account_id uuid not null references public.candidate_app_accounts(id) on delete restrict,
 candidate_id uuid not null references public.candidates(id) on delete restrict,
 environment text not null check(environment in ('TEST','LIVE')),
 workflow_id uuid not null references public.candidate_submission_workflows(id) on delete restrict,
 expected_generation integer not null check(expected_generation>0),
 result_generation integer not null check(result_generation>0),
 request_kind text not null check(request_kind in ('CHECK_HOURS','SUBMIT_TIMESHEET','SELF_SUBMIT')),
 contract_id uuid not null references public.contracts(id) on delete restrict,
 contract_week_id uuid not null references public.contract_weeks(id) on delete restrict,
 week_ending_date date not null,
 candidate_generation_id uuid references public.weekly_candidate_outreach_generations(id) on delete restrict,
 projection_publication_id uuid references public.weekly_source_projection_publications(id) on delete restrict,
 request_scope_id uuid,
 submission_membership_id uuid references public.weekly_timesheet_submission_request_memberships(id) on delete restrict,
 timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
 timesheet_revision integer not null check(timesheet_revision>0),
 timesheet_hash bytea not null check(octet_length(timesheet_hash)=32),
 signature_copy_id uuid not null references public.candidate_submission_components(id) on delete restrict,
 signature_content_sha256 bytea check(octet_length(signature_content_sha256)=32),
 immutable_submission_sha256 bytea not null check(octet_length(immutable_submission_sha256)=32),
 result_sha256 bytea not null check(octet_length(result_sha256)=32),
 comparison_completion_id uuid,
 comparison_completion_sha256 bytea,
 begin_event_ordinal integer not null check(begin_event_ordinal between 0 and 1000),
 end_event_ordinal integer not null check(end_event_ordinal between 0 and 1000),
 actual_stage_count integer not null check(actual_stage_count between 0 and 1000),
 raw_stages_sha256 bytea not null check(octet_length(raw_stages_sha256)=32),
 completion_sha256 bytea not null check(octet_length(completion_sha256)=32),
 completed_at_utc timestamptz not null default transaction_timestamp(),
 foreign key(producer_id,preparing_transaction_id,module_epoch)
  references private.bpay_next_query_producer(id,preparing_transaction_id,module_epoch) on delete restrict,
 foreign key(comparison_completion_id,producer_id) references private.bpay_next_candidate_compare_completion(id,producer_id) on delete restrict,
 check(end_event_ordinal>=begin_event_ordinal and actual_stage_count=end_event_ordinal-begin_event_ordinal),
 check((comparison_completion_id is null and comparison_completion_sha256 is null)
    or (comparison_completion_id is not null and comparison_completion_sha256 is not null and octet_length(comparison_completion_sha256)=32)),
 check((variant='FRESH' and request_kind='SUBMIT_TIMESHEET' and comparison_completion_id is not null and submission_membership_id is not null)
    or ((variant='EXACT_LAST_REPLAY' or request_kind in ('CHECK_HOURS','SELF_SUBMIT')) and comparison_completion_id is null)),
 check(request_kind<>'SELF_SUBMIT' or (candidate_generation_id is null and projection_publication_id is null and request_scope_id is null and submission_membership_id is null)),
 check(variant<>'EXACT_LAST_REPLAY' or actual_stage_count=0),
 check(request_kind='SUBMIT_TIMESHEET' or actual_stage_count=0)
);

create table private.bpay_next_candidate_root_completion (
 producer_id uuid primary key,
 preparing_transaction_id xid8 not null,
 module_epoch bigint not null check(module_epoch>0),
 owner_kind text not null check(owner_kind in ('CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT','CANDIDATE_APP_SUBMIT')),
 variant text not null check(variant in ('DIRECT_MATERIALISE','SELF_SUBMIT','APP_RECEIPT_ATTACH')),
 request_key uuid not null,
 root_request_sha256 bytea not null check(octet_length(root_request_sha256)=32),
 candidate_id uuid not null references public.candidates(id) on delete restrict,
 materialise_completion_id uuid references private.bpay_next_candidate_materialise_completion(producer_id) on delete restrict,
 materialise_completion_sha256 bytea,
 app_receipt_id uuid references public.weekly_candidate_app_mutation_receipts(id) on delete restrict,
 app_submission_kind text check(app_submission_kind in ('RESPONSES_ONLY','WHOLE_WEEK_REVISION')),
 app_response_request_key uuid,
 app_response_item_count integer not null check(app_response_item_count between 0 and 1000),
 app_response_set_sha256 bytea not null check(octet_length(app_response_set_sha256)=32),
 comparison_child_count integer not null check(comparison_child_count between 0 and 1000),
 comparison_children_sha256 bytea not null check(octet_length(comparison_children_sha256)=32),
 actual_stage_count integer not null check(actual_stage_count between 0 and 1000),
 first_stage_ordinal integer,
 last_stage_ordinal integer,
 raw_stages_sha256 bytea not null check(octet_length(raw_stages_sha256)=32),
 result_sha256 bytea not null check(octet_length(result_sha256)=32),
 completion_sha256 bytea not null check(octet_length(completion_sha256)=32),
 completed_at_utc timestamptz not null default transaction_timestamp(),
 foreign key(producer_id,preparing_transaction_id,module_epoch)
  references private.bpay_next_query_producer(id,preparing_transaction_id,module_epoch) on delete restrict,
 check((materialise_completion_id is null and materialise_completion_sha256 is null)
    or (materialise_completion_id is not null and materialise_completion_id=producer_id and materialise_completion_sha256 is not null and octet_length(materialise_completion_sha256)=32)),
 check((owner_kind='CANDIDATE_MATERIALISE' and variant='DIRECT_MATERIALISE' and materialise_completion_id is not null
       and app_receipt_id is null and app_submission_kind is null and app_response_request_key is null and app_response_item_count=0)
    or (owner_kind='CANDIDATE_SELF_SUBMIT' and variant='SELF_SUBMIT' and materialise_completion_id is not null
       and app_receipt_id is null and app_submission_kind is null and app_response_request_key is null and app_response_item_count=0
       and comparison_child_count=0 and actual_stage_count=0)
    or (owner_kind='CANDIDATE_APP_SUBMIT' and variant='APP_RECEIPT_ATTACH' and app_receipt_id is not null and app_submission_kind is not null
       and ((app_submission_kind='WHOLE_WEEK_REVISION' and materialise_completion_id is not null)
         or (app_submission_kind='RESPONSES_ONLY' and materialise_completion_id is null))
       and ((app_response_item_count=0 and app_response_request_key is null) or (app_response_item_count>0 and app_response_request_key is not null)))),
 check((actual_stage_count=0 and first_stage_ordinal is null and last_stage_ordinal is null)
    or (actual_stage_count>0 and first_stage_ordinal is not null and last_stage_ordinal is not null
      and first_stage_ordinal between 1 and 1000 and last_stage_ordinal between first_stage_ordinal and 1000
      and actual_stage_count<=last_stage_ordinal-first_stage_ordinal+1))
);

alter table private.bpay_next_candidate_compare_completion owner to postgres;
alter table private.bpay_next_candidate_compare_completion enable row level security;
revoke all on private.bpay_next_candidate_compare_completion from public,anon,authenticated,service_role;
create trigger bpay_next_candidate_compare_completion_immutable before update or delete on private.bpay_next_candidate_compare_completion
 for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_candidate_compare_completion_no_truncate before truncate on private.bpay_next_candidate_compare_completion
 for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();

alter table private.bpay_next_candidate_compare_input_completion owner to postgres;
alter table private.bpay_next_candidate_compare_input_completion enable row level security;
revoke all on private.bpay_next_candidate_compare_input_completion from public,anon,authenticated,service_role;
create trigger bpay_next_candidate_compare_input_completion_immutable before update or delete on private.bpay_next_candidate_compare_input_completion
 for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_candidate_compare_input_completion_no_truncate before truncate on private.bpay_next_candidate_compare_input_completion
 for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();

alter table private.bpay_next_candidate_compare_omission_completion owner to postgres;
alter table private.bpay_next_candidate_compare_omission_completion enable row level security;
revoke all on private.bpay_next_candidate_compare_omission_completion from public,anon,authenticated,service_role;
create trigger bpay_next_candidate_compare_omission_completion_immutable before update or delete on private.bpay_next_candidate_compare_omission_completion
 for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_candidate_compare_omission_completion_no_truncate before truncate on private.bpay_next_candidate_compare_omission_completion
 for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();

alter table private.bpay_next_candidate_materialise_completion owner to postgres;
alter table private.bpay_next_candidate_materialise_completion enable row level security;
revoke all on private.bpay_next_candidate_materialise_completion from public,anon,authenticated,service_role;
create trigger bpay_next_candidate_materialise_completion_immutable before update or delete on private.bpay_next_candidate_materialise_completion
 for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_candidate_materialise_completion_no_truncate before truncate on private.bpay_next_candidate_materialise_completion
 for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();

alter table private.bpay_next_candidate_root_completion owner to postgres;
alter table private.bpay_next_candidate_root_completion enable row level security;
revoke all on private.bpay_next_candidate_root_completion from public,anon,authenticated,service_role;
create trigger bpay_next_candidate_root_completion_immutable before update or delete on private.bpay_next_candidate_root_completion
 for each row execute function private.bpay_next_accepted_evidence_immutable_v1();
create trigger bpay_next_candidate_root_completion_no_truncate before truncate on private.bpay_next_candidate_root_completion
 for each statement execute function private.bpay_next_accepted_evidence_immutable_v1();

-- Same-xid/top-owner INSERT guards and full typed evidence assertions attach
-- before actual Candidate owner successors are installed. No owner is closed
-- by schema alone; PREFINAL still needs its actual SourceUpload root.
commit;
