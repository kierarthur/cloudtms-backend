-- One-time CloudTMS schema/data migration: nhsp_v6_01_physical_source_review_schema
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- HEALTH_V6_ROOT01_BODY_BEGIN
-- Reviewed final V6 content afaf162e59b2e7901ef0dbc3f484ce51c741f0971557f80fdc877bb8b9ecaa88.
-- Physical, invoice and Source immutable source pins:
-- 7d1be386431f5a88b7206d24c9af37b504c36f4e:docs/healthroster-build/proposals/checklist-v6-contract-correction-20261016/CLOSURE.json SHA256 d7fbfb0b71017fcfb512af9ad68e7568bc717f90bbc0409af84ffc0f24eb71b6
-- 7d1be386431f5a88b7206d24c9af37b504c36f4e:docs/healthroster-build/proposals/checklist-v6-contract-correction-20261016/forward/sql/physical-schema.sqlinc SHA256 5b2509ffe94bdb19e52f7bcda77ffd22686bd4b18e63b2ab1f8783a63b6b5115
-- 367c60bb198190be261cde153b9ca92abba6a16c:docs/healthroster-build/proposals/v6-invoice-native-20261016/SOURCE_MANIFEST.json SHA256 3a8048d53a520fad0e4454cd4a6cf9fffddd14de354f62f01fe415d700c4db54
-- 367c60bb198190be261cde153b9ca92abba6a16c:docs/healthroster-build/proposals/v6-invoice-native-20261016/forward/001_billing_schema.sqlinc SHA256 7cf768b713f421248e62dbc23178efbf24adf6096eafe63d284eb316a4d81545
-- 1ace374d453f1ad46d573ce7405f7c1ef9ab5052:docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/one-time/01_complete_health_decision_metadata.sqlinc SHA256 63741add62a1d55c5d486abf9c70c622ffbff3c0462bab2dd378d922bac689f9
-- 1ace374d453f1ad46d573ce7405f7c1ef9ab5052:docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/one-time/02_complete_retained_native_preparations.sqlinc SHA256 5b3d65c49cd5bcbaec306bad9ecb91fe57cfcb1c9f648ace0401b283a9a1f9b7
-- 1ace374d453f1ad46d573ce7405f7c1ef9ab5052:docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/one-time/05_complete_owner_only_rls.sqlinc SHA256 c38472b6ab2645ef3aa966d2820e7c2b950e011f6af2fc3bcc5086366a22d847

-- V6 schema BODY for root-owned scaffold 01. Not an independently installable migration.
-- Reviewed final content afaf162e59b2e7901ef0dbc3f484ce51c741f0971557f80fdc877bb8b9ecaa88.

create table private.timesheet_contract_move_operations (
  id uuid primary key,
  accept_request_id uuid unique,
  accept_request_hash bytea check(accept_request_hash is null or octet_length(accept_request_hash)=32),
  purpose text not null check(purpose in('CONTRACT_CORRECTION','DAILY_RATE_CORRECTION')),
  state text not null check(state in('REVIEWED','COMPLETE','SUPERSEDED')),
  state_revision bigint not null check(state_revision>0),
  family_root_id uuid not null references public.timesheets(timesheet_id),
  from_timesheet_id uuid not null references public.timesheets(timesheet_id),
  to_timesheet_id uuid references public.timesheets(timesheet_id) deferrable initially deferred,
  from_contract_id uuid references public.contracts(id),
  to_contract_id uuid references public.contracts(id),
  candidate_id uuid not null references public.candidates(id),
  actual_client_id uuid not null references public.clients(id),
  -- NULL is allowed only while this preview transaction assembles the genuine sealed review.
  -- The deferred commit guard requires real hashes plus the matching immutable review.
  expected_scope_hash bytea check(expected_scope_hash is null or octet_length(expected_scope_hash)=32),
  before_pay_cut_hash bytea check(before_pay_cut_hash is null or octet_length(before_pay_cut_hash)=32),
  before_invoice_hash bytea check(before_invoice_hash is null or octet_length(before_invoice_hash)=32),
  after_invoice_hash bytea check(after_invoice_hash is null or octet_length(after_invoice_hash)=32),
  invoice_authority text not null check(invoice_authority in('SOURCE_FINANCIAL','CONTRACT_PRICED','MIXED','UNRESOLVED')),
  invoice_move_policy text not null check(invoice_move_policy in('PRESERVE_SOURCE','ORDINARY_CORRECTION','MIXED','REVIEW_REQUIRED')),
  financial_state text not null check(financial_state in('NOT_APPROVED','QUALIFIED','REVIEW_REQUIRED')),
  reason text check(reason is null or length(btrim(reason)) between 1 and 2000),
  created_by_user_id uuid not null references public.tms_users(id),
  created_at_utc timestamptz not null,
  completed_at_utc timestamptz,
  financial_command_id uuid,
  current_financial_cut_id uuid,
  accepted_review_id uuid,
  accepted_review_digest bytea check(accepted_review_digest is null or octet_length(accepted_review_digest)=32),
  billing_revision bigint not null default 0 check(billing_revision>=0),
  unchanged_work_sha256 bytea check(unchanged_work_sha256 is null or octet_length(unchanged_work_sha256)=32),
  placement_receipt_json jsonb,
  accepted_valuation_json jsonb,
  preparation_operation_id uuid,
  receipt_json jsonb check(receipt_json is null or jsonb_typeof(receipt_json)='object'),
  unique(id,family_root_id),
  check((accept_request_id is null)=(accept_request_hash is null)),
  check((state='REVIEWED' and accept_request_id is null
    and completed_at_utc is null and receipt_json is null)
    or (state<>'REVIEWED' and accept_request_id is not null and to_timesheet_id is not null
    and completed_at_utc is not null and receipt_json is not null and reason is not null and after_invoice_hash is not null)),
  check((purpose='CONTRACT_CORRECTION' and from_contract_id is not null and to_contract_id is not null
    and from_contract_id<>to_contract_id and (to_timesheet_id is null or to_timesheet_id<>from_timesheet_id))
    or (purpose='DAILY_RATE_CORRECTION' and from_contract_id is null and to_contract_id is null
      and (to_timesheet_id is null or to_timesheet_id=from_timesheet_id))),
  check(state='REVIEWED' or (accepted_review_id is not null and accepted_review_digest is not null
    and unchanged_work_sha256 is not null and placement_receipt_json is not null and billing_revision>0)),
  check(invoice_authority<>'SOURCE_FINANCIAL' or invoice_move_policy='PRESERVE_SOURCE'),
  check(invoice_authority<>'SOURCE_FINANCIAL' or after_invoice_hash is null or before_invoice_hash=after_invoice_hash)
);

create table private.timesheet_contract_move_requests (
  request_id uuid primary key,
  operation_id uuid not null references private.timesheet_contract_move_operations(id) deferrable initially deferred,
  request_kind text not null check(request_kind in('PREVIEW','ACCEPT','RESUME_FINANCIAL')),
  request_hash bytea not null check(octet_length(request_hash)=32),
  review_id uuid,
  state_revision bigint not null check(state_revision>0),
  receipt_json jsonb check(receipt_json is null or jsonb_typeof(receipt_json)='object'),
  created_at_utc timestamptz not null,
  unique(operation_id,request_id),
  unique(operation_id,request_id,request_hash)
);

create table private.timesheet_contract_move_reviews (
  id uuid primary key,
  operation_id uuid not null references private.timesheet_contract_move_operations(id),
  review_no bigint not null check(review_no>0),
  preview_request_id uuid not null unique,
  preview_request_hash bytea not null check(octet_length(preview_request_hash)=32),
  review_digest bytea not null check(octet_length(review_digest)=32),
  scope_hash bytea not null check(octet_length(scope_hash)=32),
  pay_cut_hash bytea not null check(octet_length(pay_cut_hash)=32),
  invoice_hash bytea not null check(octet_length(invoice_hash)=32),
  manifest_json jsonb not null check(jsonb_typeof(manifest_json)='object'),
  created_at_utc timestamptz not null,
  unique(operation_id,review_no),
  unique(operation_id,id),
  foreign key(operation_id,preview_request_id,preview_request_hash)
    references private.timesheet_contract_move_requests(operation_id,request_id,request_hash) deferrable initially deferred
);
alter table private.timesheet_contract_move_requests add constraint timesheet_contract_move_request_review_fk
  foreign key(operation_id,review_id) references private.timesheet_contract_move_reviews(operation_id,id) deferrable initially deferred;
alter table private.timesheet_contract_move_operations add constraint timesheet_contract_move_accept_request_fk
  foreign key(id,accept_request_id,accept_request_hash)
  references private.timesheet_contract_move_requests(operation_id,request_id,request_hash) deferrable initially deferred;

create table private.timesheet_contract_move_members (
  operation_id uuid not null,
  family_root_id uuid not null references public.timesheets(timesheet_id),
  member_key text not null check(length(member_key) between 1 and 200),
  role text not null check(role in('WORK','EXPENSE_CARRIER','CORRECTION_MEMBER')),
  from_timesheet_id uuid not null references public.timesheets(timesheet_id),
  from_version integer not null check(from_version>0),
  from_financial_id uuid references public.timesheets_financials(id),
  from_contract_id uuid references public.contracts(id),
  to_timesheet_id uuid references public.timesheets(timesheet_id) deferrable initially deferred,
  to_version integer check(to_version>0),
  to_financial_id uuid references public.timesheets_financials(id),
  to_contract_id uuid references public.contracts(id),
  original_booking_id text not null check(length(original_booking_id) between 1 and 128),
  current_booking_id text check(length(current_booking_id) between 1 and 128),
  expense_component_ids uuid[] not null,
  workflow_ids uuid[] not null,
  source_lineage_ids uuid[] not null,
  immutable_evidence_hash bytea not null check(octet_length(immutable_evidence_hash)=32),
  approval_evidence_hash bytea not null check(octet_length(approval_evidence_hash)=32),
  member_hash bytea not null check(octet_length(member_hash)=32),
  primary key(operation_id,member_key),
  unique(operation_id,to_timesheet_id),
  foreign key(operation_id,family_root_id) references private.timesheet_contract_move_operations(id,family_root_id),
  check((to_timesheet_id is null)=(to_version is null)),
  check((to_timesheet_id is null)=(current_booking_id is null))
);

create table private.timesheet_contract_current_placements (
  family_root_id uuid not null references public.timesheets(timesheet_id),
  member_key text not null,
  role text not null check(role in('WORK','EXPENSE_CARRIER','CORRECTION_MEMBER')),
  is_primary boolean not null,
  current_timesheet_id uuid not null unique references public.timesheets(timesheet_id),
  current_contract_id uuid references public.contracts(id),
  operation_id uuid not null,
  placement_revision bigint not null check(placement_revision>0),
  placement_receipt_id uuid not null,
  candidate_id uuid not null references public.candidates(id),
  actual_client_id uuid not null references public.clients(id),
  primary key(family_root_id,member_key),
  foreign key(operation_id,member_key) references private.timesheet_contract_move_members(operation_id,member_key) deferrable initially deferred,
  foreign key(operation_id,family_root_id) references private.timesheet_contract_move_operations(id,family_root_id) deferrable initially deferred
);
create unique index timesheet_contract_one_primary_placement on private.timesheet_contract_current_placements(family_root_id) where is_primary;

create table private.timesheet_contract_family_aliases (
  timesheet_id uuid primary key references public.timesheets(timesheet_id),
  family_root_id uuid not null references public.timesheets(timesheet_id),
  member_key text not null,
  original_timesheet_id uuid not null references public.timesheets(timesheet_id),
  member_role text not null check(member_role in('WORK','EXPENSE_CARRIER','CORRECTION_MEMBER')),
  accepted_operation_id uuid not null,
  accepted_at_utc timestamptz not null,
  foreign key(accepted_operation_id,member_key) references private.timesheet_contract_move_members(operation_id,member_key) deferrable initially deferred,
  foreign key(accepted_operation_id,family_root_id) references private.timesheet_contract_move_operations(id,family_root_id) deferrable initially deferred
);
create index timesheet_contract_family_alias_lookup on private.timesheet_contract_family_aliases(family_root_id,member_key,timesheet_id);

create table private.timesheet_contract_move_execution_contexts (
  id uuid primary key,
  operation_id uuid not null references private.timesheet_contract_move_operations(id),
  backend_pid integer not null,
  transaction_id xid8 not null,
  actor_user_id uuid not null references public.tms_users(id),
  review_id uuid not null,
  manifest_sha256 bytea not null check(octet_length(manifest_sha256)=32),
  billing_revision bigint not null check(billing_revision>0),
  event_no bigint not null default 0 check(event_no>=0),
  phase text not null check(phase in('ACCEPT_PLACEMENT','DAILY_RATE_REVISION','INVOICE_SELECTED_EDIT','INVOICE_SELECTED_RELOCATE')),
  -- Owner-derived complete OLD/NEW guard events from locked native rows; never transport JSON.
  native_manifest_json jsonb not null check(jsonb_typeof(native_manifest_json)='object'),
  created_at_utc timestamptz not null,
  expires_at_utc timestamptz not null,
  unique(operation_id,backend_pid,transaction_id),
  foreign key(operation_id,review_id) references private.timesheet_contract_move_reviews(operation_id,id),
  check(expires_at_utc=created_at_utc+interval '120 seconds'),
  check(backend_pid>0)
);

create table private.timesheet_contract_move_capabilities (
  id uuid primary key,
  execution_context_id uuid not null references private.timesheet_contract_move_execution_contexts(id),
  operation_id uuid not null references private.timesheet_contract_move_operations(id),
  member_key text not null,
  phase text not null,
  billing_revision bigint not null check(billing_revision>0),
  event_no bigint not null check(event_no>0),
  authority_receipt_id uuid not null,
  guard_name text not null,
  relation_oid oid not null,
  row_operation text not null check(row_operation in('INSERT','UPDATE')),
  backend_pid integer not null,
  transaction_id xid8 not null,
  actor_user_id uuid not null references public.tms_users(id),
  manifest_sha256 bytea not null check(octet_length(manifest_sha256)=32),
  scope_receipt_id uuid,
  expected_old_sha256 bytea check(expected_old_sha256 is null or octet_length(expected_old_sha256)=32),
  expected_new_sha256 bytea not null check(octet_length(expected_new_sha256)=32),
  allowed_changed_columns text[] not null,
  created_at_utc timestamptz not null,
  expires_at_utc timestamptz not null,
  unique nulls not distinct(operation_id,member_key,phase,guard_name,relation_oid,row_operation,scope_receipt_id,billing_revision,event_no,expected_new_sha256),
  check((row_operation='INSERT' and expected_old_sha256 is null) or (row_operation='UPDATE' and expected_old_sha256 is not null)),
  check(expires_at_utc<=created_at_utc+interval '120 seconds')
);

create table private.timesheet_contract_move_capability_consumptions (
  execution_context_id uuid not null,
  backend_pid integer not null check(backend_pid>0),
  transaction_id xid8 not null,
  manifest_sha256 bytea not null check(octet_length(manifest_sha256)=32),
  expected_events_json jsonb not null check(jsonb_typeof(expected_events_json)='array'),
  operation_id uuid not null references private.timesheet_contract_move_operations(id),
  member_key text not null,
  phase text not null,
  billing_revision bigint not null check(billing_revision>0),
  event_no bigint not null check(event_no>0),
  authority_receipt_id uuid not null,
  guard_name text not null,
  relation_oid oid not null,
  row_operation text not null check(row_operation in('INSERT','UPDATE')),
  scope_receipt_id uuid,
  row_sha256 bytea not null check(octet_length(row_sha256)=32),
  capability_id uuid not null unique,
  consumed_at_utc timestamptz not null,
  unique nulls not distinct(operation_id,member_key,phase,guard_name,relation_oid,row_operation,scope_receipt_id,billing_revision,event_no,row_sha256)
);

-- Exact private owner policy. service_role receives neither table access nor policy.
do $owner_policy$
declare n text;
begin
  foreach n in array array['timesheet_contract_move_operations','timesheet_contract_move_requests',
    'timesheet_contract_move_reviews','timesheet_contract_move_members','timesheet_contract_current_placements',
    'timesheet_contract_family_aliases','timesheet_contract_move_execution_contexts',
    'timesheet_contract_move_capabilities','timesheet_contract_move_capability_consumptions'] loop
    -- Protected release maps logical owner to the audited provider CURRENT_USER.
    execute format('alter table private.%I owner to %I',n,current_user);
    execute format('alter table private.%I enable row level security',n);
    execute format('alter table private.%I force row level security',n);
    execute format('create policy timesheet_contract_move_owner_only on private.%I for all to current_user using(true) with check(true)',n);
    execute format('revoke all on private.%I from public,anon,authenticated,service_role',n);
  end loop;
end;
$owner_policy$;

alter table private.timesheet_contract_move_operations add constraint timesheet_contract_move_accepted_review_fk
 foreign key(id,accepted_review_id) references private.timesheet_contract_move_reviews(operation_id,id) deferrable initially deferred;

-- Generic Archive still rejects authorised records. Exact correction capability is the sole exception.
alter table public.timesheets drop constraint timesheets_archived_not_authorised_chk;
alter table public.timesheets add constraint timesheets_archived_not_authorised_chk
 check(archived_at_utc is null or authorised_at_server is null or archived_reason_code='CONTRACT_MOVE');

-- Body for the root's managed schema scaffold. No invoice or business row is
-- allocated by this DDL. Physical operation/review/member DDL precedes it.
create table private.timesheet_contract_move_invoice_scopes(
 id uuid primary key,operation_id uuid not null references private.timesheet_contract_move_operations(id),
 accept_request_id uuid unique,accept_request_hash bytea check(accept_request_hash is null or octet_length(accept_request_hash)=32),
 original_invoice_id uuid not null references public.invoices(id),family_root_id uuid not null,
 original_timesheet_id uuid not null references public.timesheets(timesheet_id),current_timesheet_id uuid references public.timesheets(timesheet_id),
 scope_kind text not null check(scope_kind='CONTRACT_MOVE'),original_line_ids uuid[] not null,
 original_lines_sha256 bytea not null check(octet_length(original_lines_sha256)=32),
 original_invoice_state_json jsonb not null,replacement_charge_certificate_json jsonb not null,
 replacement_charge_sha256 bytea not null check(octet_length(replacement_charge_sha256)=32),
 billing_revision bigint not null check(billing_revision>0),required_credit_scope_digest bytea not null check(octet_length(required_credit_scope_digest)=32),
 state text not null check(state in('REVIEWED','PREPARING_DRAFTS','DRAFTS_READY','PARTIALLY_ISSUED','ISSUED','RETRY_WAIT','ACTION_REQUIRED')),
 credit_operation_id uuid references public.invoice_operations(id),credit_note_id uuid references public.invoices(id),
 replacement_invoice_id uuid check(replacement_invoice_id is null),parent_managed boolean not null check(parent_managed),
 state_revision bigint not null check(state_revision>0),created_by_user_id uuid not null references public.tms_users(id),
 created_at_utc timestamptz not null,receipt_json jsonb,
 unique(operation_id,original_invoice_id),unique(id,billing_revision),
 foreign key(operation_id,family_root_id) references private.timesheet_contract_move_operations(id,family_root_id)
);
create table private.timesheet_contract_move_invoice_scope_lines(
 scope_id uuid not null references private.timesheet_contract_move_invoice_scopes(id),
 original_invoice_line_id uuid not null references public.invoice_lines(id),original_timesheet_id uuid not null references public.timesheets(timesheet_id),
 current_timesheet_id uuid references public.timesheets(timesheet_id),original_line_snapshot_json jsonb not null,
 original_line_sha256 bytea not null check(octet_length(original_line_sha256)=32),replacement_component_key text not null,
 primary key(scope_id,original_invoice_line_id)
);
create table private.timesheet_contract_move_billing_slices(
 operation_id uuid not null,member_key text not null,slice_key text not null,scope_kind text not null,
 scope_id uuid not null,authority text not null check(authority in('SOURCE_FINANCIAL','CONTRACT_PRICED')),
 disposition text not null check(disposition in('PRESERVE_SOURCE','NO_INVOICE','EDIT_DRAFT','RELOCATE_DRAFT','REPLACE_ISSUED')),
 original_invoice_id uuid references public.invoices(id),original_financial_id uuid references public.timesheets_financials(id),
 issued_at timestamptz,original_state_json jsonb not null,original_group_json jsonb not null,hold_purpose_json jsonb not null,
 source_sha256 bytea not null check(octet_length(source_sha256)=32),billing_revision bigint not null check(billing_revision>0),
 primary key(operation_id,slice_key),unique(operation_id,billing_revision,slice_key),
 foreign key(operation_id,member_key) references private.timesheet_contract_move_members(operation_id,member_key)
);
create table private.timesheet_contract_move_replacement_groups(
 operation_id uuid not null references private.timesheet_contract_move_operations(id),billing_revision bigint not null check(billing_revision>0),
 group_key text not null,native_group_json jsonb not null,group_sha256 bytea not null check(octet_length(group_sha256)=32),
 planned_invoice_id uuid not null unique,accepted_valuation_json jsonb not null,
 required_credit_scope_digest bytea not null check(octet_length(required_credit_scope_digest)=32),
 generation_operation_id uuid references public.invoice_operations(id),invoice_id uuid references public.invoices(id),
 issue_operation_id uuid references public.invoice_operations(id),document_id uuid references public.invoice_document_versions(id),
 native_state text not null,receipt_json jsonb,supersedes_operation_id uuid,supersedes_billing_revision bigint,
 primary key(operation_id,billing_revision,group_key)
);
create table private.timesheet_contract_move_replacement_members(
 operation_id uuid not null,billing_revision bigint not null,group_key text not null,slice_key text not null,
 current_timesheet_id uuid not null references public.timesheets(timesheet_id),current_financial_id uuid references public.timesheets_financials(id),
 component_key text not null,member_sha256 bytea not null check(octet_length(member_sha256)=32),
 primary key(operation_id,billing_revision,slice_key),
 foreign key(operation_id,billing_revision,group_key) references private.timesheet_contract_move_replacement_groups(operation_id,billing_revision,group_key),
 foreign key(operation_id,billing_revision,slice_key) references private.timesheet_contract_move_billing_slices(operation_id,billing_revision,slice_key)
);
create table private.timesheet_contract_move_scope_fences(
 operation_id uuid not null references private.timesheet_contract_move_operations(id),billing_revision bigint not null check(billing_revision>0),
 scope_key text not null unique,scope_sha256 bytea not null check(octet_length(scope_sha256)=32),
 authority_receipt_id uuid not null,retired_by_native_receipt_id uuid,
 primary key(operation_id,billing_revision,scope_key)
);

-- Exact stores only; no private-schema browser/service grants.
alter table private.timesheet_contract_move_invoice_scopes owner to postgres;
alter table private.timesheet_contract_move_invoice_scope_lines owner to postgres;
alter table private.timesheet_contract_move_billing_slices owner to postgres;
alter table private.timesheet_contract_move_replacement_groups owner to postgres;
alter table private.timesheet_contract_move_replacement_members owner to postgres;
alter table private.timesheet_contract_move_scope_fences owner to postgres;
do $acl$
declare table_name text;
begin
 foreach table_name in array array['timesheet_contract_move_invoice_scopes','timesheet_contract_move_invoice_scope_lines',
  'timesheet_contract_move_billing_slices','timesheet_contract_move_replacement_groups',
  'timesheet_contract_move_replacement_members','timesheet_contract_move_scope_fences'] loop
  execute format('alter table private.%I enable row level security',table_name);
  execute format('alter table private.%I force row level security',table_name);
  execute format('create policy invoice_correction_owner on private.%I to current_user using(true) with check(true)',table_name);
  execute format('revoke all on table private.%I from public,anon,authenticated,service_role',table_name);
 end loop;
end;
$acl$;

create index timesheet_contract_move_billing_slices_invoice_idx on private.timesheet_contract_move_billing_slices(original_invoice_id,operation_id,billing_revision,member_key);
create index timesheet_contract_move_replacement_groups_invoice_idx on private.timesheet_contract_move_replacement_groups((coalesce(invoice_id,planned_invoice_id)));
create index timesheet_contract_move_scopes_credit_idx on private.timesheet_contract_move_invoice_scopes(credit_note_id);

-- Initial Health V5 metadata, once only. No Work, financial effect, contact,
-- native run or invoice is allocated here. Root owns NEW/UPGRADE composition.
create table private.weekly_source_pay_decisions (
 id uuid primary key,
 accept_request_id uuid unique,
 accept_request_hash bytea check(accept_request_hash is null or octet_length(accept_request_hash)=32),
 candidate_id uuid references public.candidates(id) on delete restrict,
 actual_client_id uuid not null references public.clients(id) on delete restrict,
 state_revision bigint not null default 1 check(state_revision>0),
 current_financial_cut_id uuid,
 decision_kind text check(decision_kind in('EXCLUDE_FUTURE_WORK_PAY','ACCEPT_FINAL_SOURCE_PAY')),
 accepted_selection_sha256 bytea check(accepted_selection_sha256 is null or octet_length(accepted_selection_sha256)=32),
 supersedes_decision_id uuid references private.weekly_source_pay_decisions(id) on delete restrict,
 prior_dispositions_sha256 bytea check(prior_dispositions_sha256 is null or octet_length(prior_dispositions_sha256)=32),
 invoice_policy text not null default 'KEEP_SOURCE_INVOICE' check(invoice_policy='KEEP_SOURCE_INVOICE'),
 contact_policy text not null default 'DO_NOT_QUERY_SELECTED_ISSUES' check(contact_policy='DO_NOT_QUERY_SELECTED_ISSUES'),
 reason text check(reason is null or char_length(reason) between 1 and 1000),
 evidence_refs_json jsonb not null default '[]'::jsonb check(jsonb_typeof(evidence_refs_json)='array'),
 invoice_before_hash bytea check(invoice_before_hash is null or octet_length(invoice_before_hash)=32),
 invoice_after_hash bytea check(invoice_after_hash is null or octet_length(invoice_after_hash)=32),
 issue_before_hash bytea check(issue_before_hash is null or octet_length(issue_before_hash)=32),
 created_by_user_id uuid not null references public.tms_users(id) on delete restrict,
 created_at_utc timestamptz not null default transaction_timestamp(),
 receipt_json jsonb check(receipt_json is null or jsonb_typeof(receipt_json)='object'),
 check((accept_request_id is null)=(accept_request_hash is null)),
 check((accept_request_id is null)=(decision_kind is null)),
 check((accept_request_id is null)=(accepted_selection_sha256 is null)),
 check(accept_request_id is null or (reason is not null and prior_dispositions_sha256 is not null
   and invoice_before_hash is not null and invoice_after_hash is not null and issue_before_hash is not null)),
 check(id is distinct from supersedes_decision_id),
 check(invoice_after_hash is null or invoice_before_hash=invoice_after_hash)
);
create table private.weekly_source_pay_decision_requests (
 request_id uuid primary key,
 decision_id uuid not null references private.weekly_source_pay_decisions(id) on delete restrict,
 request_kind text not null check(request_kind in('PREVIEW','ACCEPT','RESUME_FINANCIAL')),
 request_hash bytea not null check(octet_length(request_hash)=32),
 request_json jsonb not null check(jsonb_typeof(request_json)='object'),
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 review_id uuid,
 metadata_state text not null check(metadata_state in('ASSEMBLING','SEALED','CANCELLED','EXPIRED','STALE','CONFIRMED','COMMITTED')),
 receipt_json jsonb check(receipt_json is null or jsonb_typeof(receipt_json)='object'),
 created_at_utc timestamptz not null default transaction_timestamp(),
 unique(decision_id,request_id),
 check((metadata_state='COMMITTED')=(receipt_json is not null) or request_kind='PREVIEW'),
 check(request_kind='PREVIEW' or metadata_state in('CONFIRMED','COMMITTED'))
);
alter table private.weekly_source_pay_decisions add constraint source_pay_decision_accept_request_fk
 foreign key(id,accept_request_id) references private.weekly_source_pay_decision_requests(decision_id,request_id)
 deferrable initially deferred;
create table private.weekly_source_pay_decision_preview_assemblies (
 id uuid primary key,
 decision_id uuid not null references private.weekly_source_pay_decisions(id) on delete restrict,
 preview_request_id uuid not null unique,
 preview_request_hash bytea not null check(octet_length(preview_request_hash)=32),
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 decision_kind text not null check(decision_kind in('EXCLUDE_FUTURE_WORK_PAY','ACCEPT_FINAL_SOURCE_PAY')),
 state text not null check(state in('ASSEMBLING','READY','SEALED','CANCELLED','EXPIRED','STALE')),
 draft_scope_json jsonb not null check(jsonb_typeof(draft_scope_json)='object'
   and jsonb_typeof(draft_scope_json->'selection')='array'),
 draft_scope_sha256 bytea not null check(octet_length(draft_scope_sha256)=32),
 assembly_sha256 bytea check(assembly_sha256 is null or octet_length(assembly_sha256)=32),
 review_id uuid,
 created_at_utc timestamptz not null default transaction_timestamp(),
 expires_at_utc timestamptz not null,
 unique(decision_id,id),
 foreign key(decision_id,preview_request_id) references private.weekly_source_pay_decision_requests(decision_id,request_id) on delete restrict,
 check(expires_at_utc>created_at_utc and expires_at_utc<=created_at_utc+interval '15 minutes'),
 check((state='SEALED')=(review_id is not null)),
 check(state not in('READY','SEALED') or assembly_sha256 is not null)
);
create table private.weekly_source_pay_decision_preview_quotes (
 id uuid primary key,
 assembly_id uuid not null references private.weekly_source_pay_decision_preview_assemblies(id) on delete restrict,
 member_key text not null check(char_length(member_key) between 1 and 512),
 quote_json jsonb not null check(jsonb_typeof(quote_json)='object'),
 quote_sha256 bytea not null check(octet_length(quote_sha256)=32),
 economic_target_sha256 bytea not null check(octet_length(economic_target_sha256)=32),
 unique(assembly_id,member_key)
);
create table private.weekly_source_pay_decision_reviews (
 id uuid primary key,
 decision_id uuid not null references private.weekly_source_pay_decisions(id) on delete restrict,
 preview_assembly_id uuid not null unique,
 review_no bigint not null check(review_no>0),
 preview_request_id uuid not null unique,
 preview_request_hash bytea not null check(octet_length(preview_request_hash)=32),
 review_digest bytea not null check(octet_length(review_digest)=32),
 pay_cut_hash bytea not null check(octet_length(pay_cut_hash)=32),
 source_invoice_hash bytea not null check(octet_length(source_invoice_hash)=32),
 issue_set_hash bytea not null check(octet_length(issue_set_hash)=32),
 private_manifest_json jsonb not null check(jsonb_typeof(private_manifest_json)='object'
   and jsonb_typeof(private_manifest_json->'selection')='array'),
 financial_history_refs_json jsonb not null default '[]'::jsonb check(jsonb_typeof(financial_history_refs_json)='array'),
 financial_history_manifest_sha256 bytea not null check(octet_length(financial_history_manifest_sha256)=32),
 history_qualification_json jsonb not null check(jsonb_typeof(history_qualification_json)='object'),
 created_at_utc timestamptz not null default transaction_timestamp(),
 unique(decision_id,review_no),unique(decision_id,id),unique(decision_id,preview_assembly_id,id),
 foreign key(decision_id,preview_assembly_id) references private.weekly_source_pay_decision_preview_assemblies(decision_id,id) on delete restrict,
 foreign key(decision_id,preview_request_id) references private.weekly_source_pay_decision_requests(decision_id,request_id) on delete restrict
);
alter table private.weekly_source_pay_decision_preview_assemblies add constraint source_pay_assembly_review_triangle_fk
 foreign key(decision_id,id,review_id) references private.weekly_source_pay_decision_reviews(decision_id,preview_assembly_id,id)
 deferrable initially deferred;
alter table private.weekly_source_pay_decision_requests add constraint source_pay_request_review_fk
 foreign key(decision_id,review_id) references private.weekly_source_pay_decision_reviews(decision_id,id)
 deferrable initially deferred;
create table private.weekly_source_pay_decision_members (
 decision_id uuid not null references private.weekly_source_pay_decisions(id) on delete restrict,
 member_key text not null check(char_length(member_key) between 1 and 512),
 scope_kind text not null check(scope_kind in('TIMESHEET_FAMILY','SOURCE_ROW')),
 scope_mode text not null check(scope_mode in('COMPONENT_QUANTITIES','WHOLE_SHIFT')),
 candidate_id uuid not null references public.candidates(id) on delete restrict,
 actual_client_id uuid not null references public.clients(id) on delete restrict,
 family_root_id uuid references public.timesheets(timesheet_id) on delete restrict,
 source_work_event_id uuid references public.weekly_work_events(id) on delete restrict,
 source_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
 component_keys text[] not null,
 supported_quantity_json jsonb not null check(jsonb_typeof(supported_quantity_json)='object'),
 excluded_quantity_json jsonb not null check(jsonb_typeof(excluded_quantity_json)='object'),
 branch text not null check(branch in('FUTURE_UNSIGNED','SIGNED_FINANCIAL','RETAINED_FINANCIAL')),
 source_invoice_certificate_json jsonb not null check(jsonb_typeof(source_invoice_certificate_json)='object'),
 contact_issue_ids uuid[] not null,
 pay_disposition_id uuid,
 primary key(decision_id,member_key),
 check(scope_kind<>'TIMESHEET_FAMILY' or family_root_id is not null),
 check(scope_mode<>'COMPONENT_QUANTITIES' or cardinality(component_keys)>0)
);
create table private.weekly_source_pay_decision_bindings (
 decision_id uuid not null,member_key text not null,
 row_resolution_id uuid not null references public.weekly_source_row_resolutions(id) on delete restrict,
 binding_no bigint not null check(binding_no>0),
 family_root_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
 source_work_event_id uuid references public.weekly_work_events(id) on delete restrict,
 component_keys text[] not null,
 banking_disposition_id uuid,banking_disposition_binding_no bigint check(banking_disposition_binding_no>0),
 binding_certificate_hash bytea not null check(octet_length(binding_certificate_hash)=32),
 created_at_utc timestamptz not null default transaction_timestamp(),
 primary key(decision_id,member_key,binding_no),unique(decision_id,member_key,row_resolution_id),
 foreign key(decision_id,member_key) references private.weekly_source_pay_decision_members(decision_id,member_key) on delete restrict,
 check((banking_disposition_id is null)=(banking_disposition_binding_no is null))
);
-- No FK from preaccept quote/preparation member_key to accepted members: the
-- real member is reviewed JSON until first acceptance. Owner verifies that set.

create table private.weekly_source_pay_decision_unprotect_preparations (
 id uuid primary key,decision_id uuid not null,member_key text not null,
 review_id uuid not null,accept_or_resume_request_id uuid not null,
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 family_id uuid not null references public.weekly_exceptional_pay_target_families(id) on delete restrict,
 orchestration_run_id uuid not null unique references public.weekly_exceptional_orchestration_runs(id) on delete restrict,
 child_idempotency_key text not null unique,
 route text not null check(route in('NEXT','SOURCE_LOCAL')),
 family_bound_version bigint not null check(family_bound_version>0),
 confirmation_request_sha256 bytea not null check(octet_length(confirmation_request_sha256)=32),
 native_prepare_request_json jsonb not null check(jsonb_typeof(native_prepare_request_json)='object'),
 native_prepare_request_sha256 bytea not null check(octet_length(native_prepare_request_sha256)=32),
 native_prepared_json jsonb not null check(jsonb_typeof(native_prepared_json)='object'),
 native_prepared_sha256 bytea not null check(octet_length(native_prepared_sha256)=32),
 source_phase text check(source_phase in('NO_FINAL_REPORT_YET','FINAL_SOURCE_OBSERVED')),
 source_revision uuid,
 native_source_sha256 bytea check(native_source_sha256 is null or octet_length(native_source_sha256)=32),
 source_observation_sha256 bytea check(source_observation_sha256 is null or octet_length(source_observation_sha256)=32),
 placement_certificate_sha256 bytea check(placement_certificate_sha256 is null or octet_length(placement_certificate_sha256)=32),
 context_sha256 bytea check(context_sha256 is null or octet_length(context_sha256)=32),
 reviewed_target_sha256 bytea check(reviewed_target_sha256 is null or octet_length(reviewed_target_sha256)=32),
 payload_sha256 bytea check(payload_sha256 is null or octet_length(payload_sha256)=32),
 payload_json jsonb check(payload_json is null or jsonb_typeof(payload_json)='object'),
 state text not null check(state in('RUN_PREPARED','PREPARED','CONSUMED','STALE')),
 native_receipt_json jsonb check(native_receipt_json is null or jsonb_typeof(native_receipt_json)='object'),
 created_at_utc timestamptz not null default transaction_timestamp(),expires_at_utc timestamptz not null,
 unique(decision_id,member_key,accept_or_resume_request_id),
 foreign key(decision_id,review_id) references private.weekly_source_pay_decision_reviews(decision_id,id) on delete restrict,
 foreign key(decision_id,accept_or_resume_request_id) references private.weekly_source_pay_decision_requests(decision_id,request_id) on delete restrict,
 check(expires_at_utc>created_at_utc and expires_at_utc<=created_at_utc+interval '10 minutes'),
 check((source_revision is null)=(native_source_sha256 is null)),
 check(source_phase is distinct from 'NO_FINAL_REPORT_YET' or source_revision is null),
 check(source_phase is distinct from 'FINAL_SOURCE_OBSERVED' or source_revision is not null),
 check(state not in('PREPARED','CONSUMED') or (source_phase is not null and source_observation_sha256 is not null and placement_certificate_sha256 is not null
   and context_sha256 is not null and reviewed_target_sha256 is not null and payload_sha256 is not null and payload_json is not null)),
 check((state='CONSUMED')=(native_receipt_json is not null))
);
-- Retained ownership fence: no TTL DELETE, even for never-filled RUN_PREPARED.

alter table private.weekly_source_pay_decisions owner to postgres;
alter table private.weekly_source_pay_decision_requests owner to postgres;
alter table private.weekly_source_pay_decision_preview_assemblies owner to postgres;
alter table private.weekly_source_pay_decision_preview_quotes owner to postgres;
alter table private.weekly_source_pay_decision_reviews owner to postgres;
alter table private.weekly_source_pay_decision_members owner to postgres;
alter table private.weekly_source_pay_decision_bindings owner to postgres;
alter table private.weekly_source_pay_decision_unprotect_preparations owner to postgres;
do $source_pay_root01_rls$
declare name text;
begin
 foreach name in array array['weekly_source_pay_decisions','weekly_source_pay_decision_requests','weekly_source_pay_decision_preview_assemblies','weekly_source_pay_decision_preview_quotes','weekly_source_pay_decision_reviews','weekly_source_pay_decision_members','weekly_source_pay_decision_bindings','weekly_source_pay_decision_unprotect_preparations'] loop
  execute format('alter table private.%I enable row level security',name);
  execute format('alter table private.%I force row level security',name);
  execute format('revoke all on table private.%I from public,anon,authenticated,service_role',name);
  execute format('create policy cloudtms_source_pay_decision_owner_only on private.%I for all to %I using(true) with check(true)',name,current_user);
 end loop;
end;
$source_pay_root01_rls$;
-- HEALTH_V6_ROOT01_BODY_END

commit;
