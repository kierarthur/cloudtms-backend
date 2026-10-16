-- One-time CloudTMS schema/data migration: weekly_source_plan6_schema
-- Additive authority for Weekly source evidence, query, finalisation, invoice
-- movement and protected-shift target publication.  These records sit upstream
-- of Workbench and do not alter any Banking Pay, Draft, execution,
-- cancellation, settlement or remittance owner.

\set ON_ERROR_STOP on

begin;

create table public.weekly_source_groups (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  environment text not null default 'TEST' check (environment in ('TEST','LIVE')),
  agency_id uuid not null,
  code text not null check (code ~ '^[A-Z][A-Z0-9_]{1,79}$'),
  display_name text not null check (pg_catalog.char_length(pg_catalog.btrim(display_name)) between 1 and 160),
  source_family text not null check (source_family in ('NHSP','ROSTER')),
  timezone text not null default 'Europe/London' check (timezone='Europe/London'),
  cutoff_weekday smallint not null check (cutoff_weekday between 0 and 6),
  cutoff_local_time time without time zone not null,
  nhsp_report_heading_name text check (nhsp_report_heading_name is null or pg_catalog.char_length(pg_catalog.btrim(nhsp_report_heading_name)) between 1 and 200),
  active boolean not null default true,
  version bigint not null default 1 check (version>=1),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_by_user_id uuid references public.tms_users(id) on delete restrict,
  unique (environment,agency_id,code),
  check ((source_family='NHSP')=(nhsp_report_heading_name is not null))
);
alter table public.weekly_source_groups owner to postgres;

create table public.weekly_source_group_clients (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  valid_from date not null,
  valid_to date,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  created_by_user_id uuid references public.tms_users(id) on delete restrict,
  check (valid_to is null or valid_to>=valid_from),
  unique nulls not distinct (source_group_id,client_id,valid_from,valid_to)
);
alter table public.weekly_source_group_clients owner to postgres;
create index weekly_source_group_clients_lookup_idx
  on public.weekly_source_group_clients(client_id,valid_from,valid_to,source_group_id);
alter table public.weekly_source_group_clients
  add constraint weekly_source_group_clients_no_overlap
  exclude using gist (
    client_id with =,
    daterange(valid_from,coalesce(valid_to,'infinity'::date),'[]') with &&
  );

create table public.weekly_source_client_policies (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  effective_from date not null,
  effective_to date,
  authority_mode text not null default 'SOURCE_AUTHORITY' check (authority_mode in ('SOURCE_AUTHORITY','TIMESHEET_AUTHORITY')),
  document_mode text not null default 'CHECK_ONLY' check (document_mode in ('IMPORT_ONLY','CHECK_ONLY','INVOICE_EVIDENCE_REQUIRED')),
  self_bill_enabled boolean not null default true,
  self_bill_correction_presentation text check (self_bill_correction_presentation is null or self_bill_correction_presentation in ('FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION')),
  source_fixed_expenses_enabled boolean not null default false,
  source_expense_vat_enabled boolean not null default false,
  weekly_rate_classification_method text not null default 'SPLIT_RATE_WINDOWS' check (weekly_rate_classification_method in ('SPLIT_RATE_WINDOWS','WHOLE_SHIFT_START_DAY')),
  duration_break_tie_rule text default 'EARLIEST_LONGEST_PORTION' check (duration_break_tie_rule is null or duration_break_tie_rule in ('EARLIEST_LONGEST_PORTION','LATEST_LONGEST_PORTION')),
  candidate_queries_enabled boolean not null default true,
  manager_queries_enabled boolean not null default true,
  manager_query_recipient text,
  completed_pack_copy_enabled boolean not null default false,
  completed_pack_recipient text,
  created_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (effective_to is null or effective_to>=effective_from),
  check (completed_pack_copy_enabled=false or nullif(pg_catalog.btrim(completed_pack_recipient),'') is not null),
  check (authority_mode<>'TIMESHEET_AUTHORITY' or (not self_bill_enabled and document_mode='INVOICE_EVIDENCE_REQUIRED')),
  check (authority_mode<>'SOURCE_AUTHORITY' or document_mode in ('IMPORT_ONLY','CHECK_ONLY')),
  check ((weekly_rate_classification_method='SPLIT_RATE_WINDOWS')=(duration_break_tie_rule is not null)),
  unique nulls not distinct (source_group_id,client_id,effective_from,effective_to)
);
alter table public.weekly_source_client_policies owner to postgres;
create index weekly_source_client_policies_effective_idx
  on public.weekly_source_client_policies(source_group_id,client_id,effective_from,effective_to);
alter table public.weekly_source_client_policies
  add constraint weekly_source_client_policies_no_overlap
  exclude using gist (
    source_group_id with =,
    client_id with =,
    daterange(effective_from,coalesce(effective_to,'infinity'::date),'[]') with &&
  );

create table public.weekly_source_contract_policies (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  contract_id uuid not null references public.contracts(id) on delete restrict,
  effective_from date not null,
  effective_to date,
  weekly_rate_classification_method_override text check (weekly_rate_classification_method_override is null or weekly_rate_classification_method_override in ('SPLIT_RATE_WINDOWS','WHOLE_SHIFT_START_DAY')),
  duration_break_tie_rule_override text check (duration_break_tie_rule_override is null or duration_break_tie_rule_override in ('EARLIEST_LONGEST_PORTION','LATEST_LONGEST_PORTION')),
  source_fixed_expenses_enabled_override boolean,
  source_expense_vat_enabled_override boolean,
  candidate_queries_enabled_override boolean,
  manager_queries_enabled_override boolean,
  manager_query_recipient_override text,
  completed_pack_copy_enabled_override boolean,
  completed_pack_recipient_override text,
  created_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (effective_to is null or effective_to>=effective_from),
  unique nulls not distinct (contract_id,effective_from,effective_to)
);
alter table public.weekly_source_contract_policies owner to postgres;
create index weekly_source_contract_policies_effective_idx
  on public.weekly_source_contract_policies(contract_id,effective_from,effective_to);
alter table public.weekly_source_contract_policies
  add constraint weekly_source_contract_policies_no_overlap
  exclude using gist (
    contract_id with =,
    daterange(effective_from,coalesce(effective_to,'infinity'::date),'[]') with &&
  );

create table public.weekly_source_format_profiles (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  profile_code text not null,
  version integer not null check (version>=1),
  final_authority_kind text not null check (final_authority_kind in ('GENERIC_COMPLETE_SNAPSHOT','NHSP_TRUST_BACKING_REPORT','HEALTHROSTER_ACTUAL_ROWS')),
  container_kind text not null check (container_kind in ('XLSX','CSV','SELF_CONTAINED_HTML')),
  omission_meaning text not null check (omission_meaning in ('CANCEL_INSIDE_CONFIRMED_COVERAGE','NO_INFERENCE')),
  row_finalisation_capability text not null,
  worked_duration_authority text not null,
  scheduled_hours_fallback boolean not null default false check (scheduled_hours_fallback=false),
  physical_negative_meaning text,
  report_number_required boolean not null default false,
  single_client_required boolean not null default true,
  fmc_must_equal_zero boolean not null default false,
  profile_json jsonb not null check (pg_catalog.jsonb_typeof(profile_json)='object'),
  profile_sha256 bytea not null check (pg_catalog.octet_length(profile_sha256)=32),
  active boolean not null default true,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (profile_code,version),
  unique (profile_sha256),
  check ((final_authority_kind='NHSP_TRUST_BACKING_REPORT')=(omission_meaning='NO_INFERENCE')),
  check (final_authority_kind<>'NHSP_TRUST_BACKING_REPORT' or (report_number_required and fmc_must_equal_zero and physical_negative_meaning='NHSP_FULL_REVERSAL'))
);
alter table public.weekly_source_format_profiles owner to postgres;

create table public.weekly_source_global_settings (
  singleton boolean primary key default true check (singleton),
  candidate_reminder_after interval not null default interval '6 hours' check (candidate_reminder_after>interval '0' and candidate_reminder_after<interval '12 hours'),
  candidate_response_deadline_after interval not null default interval '12 hours' check (candidate_response_deadline_after>candidate_reminder_after),
  manager_partial_digest_after interval not null default interval '6 hours' check (manager_partial_digest_after>interval '0'),
  manager_manual_send_cooldown interval not null default interval '5 minutes' check (manager_manual_send_cooldown>=interval '5 minutes'),
  candidate_manual_reminder_cooldown interval not null default interval '60 minutes' check (candidate_manual_reminder_cooldown>=interval '60 minutes'),
  manager_secure_link_lifetime interval not null default interval '7 days' check (manager_secure_link_lifetime>=interval '1 day' and manager_secure_link_lifetime<=interval '30 days'),
  version bigint not null default 1 check (version>=1),
  updated_by_user_id uuid references public.tms_users(id) on delete restrict,
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);
alter table public.weekly_source_global_settings owner to postgres;
insert into public.weekly_source_global_settings(singleton) values (true);

create table public.weekly_source_cycles (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  finalisation_week_ending date not null,
  cutoff_at_utc timestamptz not null,
  state text not null default 'OPEN' check (state in ('OPEN','FINALISABLE','FINALISING','FINALISED','CORRECTION_IN_PROGRESS')),
  current_complete_upload_id uuid,
  current_final_revision_id uuid,
  version bigint not null default 0 check (version>=0),
  projection_state text not null default 'NONE' check (projection_state in ('NONE','REBUILDING','CURRENT','FAILED')),
  current_projection_publication_id uuid,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  finalised_at_utc timestamptz,
  finalised_by_user_id uuid references public.tms_users(id) on delete restrict,
  unique (source_group_id,finalisation_week_ending),
  check (state<>'FINALISED' or (finalised_at_utc is not null and finalised_by_user_id is not null))
);
alter table public.weekly_source_cycles owner to postgres;

create table public.weekly_source_report_scopes (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  environment text not null default 'TEST' check (environment in ('TEST','LIVE')),
  agency_id uuid not null,
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  cutoff_at_utc timestamptz not null,
  current_complete_upload_id uuid,
  current_final_revision_id uuid,
  version bigint not null default 0 check (version>=0),
  state text not null default 'OPEN' check (state in ('OPEN','FINALISABLE','FINALISING','FINALISED','CORRECTION_IN_PROGRESS')),
  projection_state text not null default 'NONE' check (projection_state in ('NONE','REBUILDING','CURRENT','FAILED')),
  current_projection_publication_id uuid,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (environment,agency_id,source_group_id,client_id,cutoff_at_utc),
  unique (id,source_cycle_id,agency_id,source_group_id,client_id,cutoff_at_utc)
);
alter table public.weekly_source_report_scopes owner to postgres;
create index weekly_source_report_scopes_cycle_idx
  on public.weekly_source_report_scopes(source_cycle_id,client_id,cutoff_at_utc);

create table public.weekly_final_source_correction_sessions (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  authority_scope_kind text not null check (authority_scope_kind in ('CYCLE','NHSP_REPORT_SCOPE')),
  report_scope_id uuid references public.weekly_source_report_scopes(id) on delete restrict,
  expected_current_final_revision_id uuid not null,
  expected_final_manifest_hash bytea not null check (pg_catalog.octet_length(expected_final_manifest_hash)=32),
  replacement_correction_upload_id uuid,
  replacement_projection_publication_id uuid,
  prepared_final_revision_id uuid,
  applied_final_revision_id uuid,
  state text not null default 'DRAFT' check (state in ('DRAFT','STAGING','READY','REVIEWED','PREPARING','PREPARED','COMMITTING','APPLIED','CANCELLED','FAILED')),
  version bigint not null default 1 check (version>=1),
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  reason text not null check (pg_catalog.char_length(pg_catalog.btrim(reason)) between 1 and 1000),
  idempotency_key text not null check (pg_catalog.char_length(pg_catalog.btrim(idempotency_key)) between 1 and 200),
  request_hash bytea not null check (pg_catalog.octet_length(request_hash)=32),
  review_idempotency_key text check (review_idempotency_key is null or pg_catalog.char_length(pg_catalog.btrim(review_idempotency_key)) between 1 and 200),
  review_request_hash bytea check (review_request_hash is null or pg_catalog.octet_length(review_request_hash)=32),
  review_result_json jsonb check (review_result_json is null or pg_catalog.jsonb_typeof(review_result_json)='object'),
  review_result_hash bytea check (review_result_hash is null or pg_catalog.octet_length(review_result_hash)=32),
  prepare_idempotency_key text check (prepare_idempotency_key is null or pg_catalog.char_length(pg_catalog.btrim(prepare_idempotency_key)) between 1 and 200),
  prepare_request_hash bytea check (prepare_request_hash is null or pg_catalog.octet_length(prepare_request_hash)=32),
  prepare_result_json jsonb check (prepare_result_json is null or pg_catalog.jsonb_typeof(prepare_result_json)='object'),
  prepare_result_hash bytea check (prepare_result_hash is null or pg_catalog.octet_length(prepare_result_hash)=32),
  apply_idempotency_key text check (apply_idempotency_key is null or pg_catalog.char_length(pg_catalog.btrim(apply_idempotency_key)) between 1 and 200),
  apply_request_hash bytea check (apply_request_hash is null or pg_catalog.octet_length(apply_request_hash)=32),
  result_json jsonb check (result_json is null or pg_catalog.jsonb_typeof(result_json)='object'),
  result_hash bytea check (result_hash is null or pg_catalog.octet_length(result_hash)=32),
  guard_fingerprint bytea not null check (pg_catalog.octet_length(guard_fingerprint)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  completed_at_utc timestamptz,
  check ((authority_scope_kind='NHSP_REPORT_SCOPE')=(report_scope_id is not null)),
  check ((state in ('APPLIED','CANCELLED','FAILED'))=(completed_at_utc is not null)),
  check (state not in ('REVIEWED','PREPARING','PREPARED','COMMITTING','APPLIED') or (replacement_correction_upload_id is not null and replacement_projection_publication_id is not null and review_idempotency_key is not null and review_request_hash is not null and review_result_json is not null and review_result_hash is not null)),
  check (state not in ('PREPARED','COMMITTING','APPLIED') or (replacement_correction_upload_id is not null and replacement_projection_publication_id is not null and prepared_final_revision_id is not null and prepare_idempotency_key is not null and prepare_request_hash is not null and prepare_result_json is not null and prepare_result_hash is not null)),
  check (state<>'APPLIED' or (applied_final_revision_id=prepared_final_revision_id and apply_idempotency_key is not null and apply_request_hash is not null and result_json is not null and result_hash is not null))
);
alter table public.weekly_final_source_correction_sessions owner to postgres;
create unique index weekly_final_source_correction_sessions_active_uq
  on public.weekly_final_source_correction_sessions(source_cycle_id,authority_scope_kind,coalesce(report_scope_id,'00000000-0000-0000-0000-000000000000'::uuid))
  where state in ('DRAFT','STAGING','READY','REVIEWED','PREPARING','PREPARED','COMMITTING');
create unique index weekly_final_source_correction_sessions_idempotency_uq
  on public.weekly_final_source_correction_sessions(actor_user_id,idempotency_key);
create unique index weekly_final_source_correction_sessions_request_uq
  on public.weekly_final_source_correction_sessions(request_hash);
create unique index weekly_final_source_correction_sessions_review_idempotency_uq
  on public.weekly_final_source_correction_sessions(actor_user_id,review_idempotency_key)
  where review_idempotency_key is not null;
create unique index weekly_final_source_correction_sessions_prepare_idempotency_uq
  on public.weekly_final_source_correction_sessions(actor_user_id,prepare_idempotency_key)
  where prepare_idempotency_key is not null;
create unique index weekly_final_source_correction_sessions_apply_idempotency_uq
  on public.weekly_final_source_correction_sessions(actor_user_id,apply_idempotency_key)
  where apply_idempotency_key is not null;

create table public.weekly_source_uploads (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  report_scope_id uuid references public.weekly_source_report_scopes(id) on delete restrict,
  original_filename text not null check (pg_catalog.char_length(original_filename) between 1 and 255),
  content_sha256 bytea not null check (pg_catalog.octet_length(content_sha256)=32),
  byte_count bigint not null check (byte_count>0),
  source_format_profile_id uuid not null references public.weekly_source_format_profiles(id) on delete restrict,
  parser_version text not null,
  normaliser_version text not null,
  workbook_part_and_sheet_fingerprint bytea check (workbook_part_and_sheet_fingerprint is null or pg_catalog.octet_length(workbook_part_and_sheet_fingerprint)=32),
  header_coordinate_map_json jsonb not null default '{}'::jsonb check (pg_catalog.jsonb_typeof(header_coordinate_map_json)='object'),
  header_coordinate_map_hash bytea not null check (pg_catalog.octet_length(header_coordinate_map_hash)=32),
  money_lexical_authority_version text,
  purpose text not null default 'ORDINARY' check (purpose in ('ORDINARY','FINAL_SOURCE_CORRECTION')),
  correction_session_id uuid references public.weekly_final_source_correction_sessions(id) on delete restrict,
  declared_scope_fingerprint bytea not null check (pg_catalog.octet_length(declared_scope_fingerprint)=32),
  suggested_coverage_start_local_date date,
  suggested_coverage_end_local_date date,
  confirmed_coverage_start_local_date date,
  confirmed_coverage_end_local_date date,
  coverage_timezone text check (coverage_timezone is null or coverage_timezone='Europe/London'),
  coverage_confirmation_version text,
  coverage_confirmed_by_user_id uuid references public.tms_users(id) on delete restrict,
  coverage_confirmed_at_utc timestamptz,
  coverage_shrink_acknowledged boolean,
  coverage_state text check (coverage_state is null or coverage_state in ('COMPLETE','INCOMPLETE','UNKNOWN')),
  coverage_proof_kind text not null check (coverage_proof_kind in ('FORMAT_MANIFEST','OFFICE_COMPLETE_EXPORT_ATTESTATION','EXPLICIT_EMPTY_CONFIRMATION','NHSP_TRUST_REPORT_SCOPE','HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION')),
  physical_row_count integer not null check (physical_row_count>=0),
  header_count integer not null default 0 check (header_count>=0),
  trailer_count integer not null default 0 check (trailer_count>=0),
  continuation_count integer not null default 0 check (continuation_count>=0),
  accepted_count integer not null default 0 check (accepted_count>=0),
  blocking_economic_duplicate_count integer not null default 0 check (blocking_economic_duplicate_count>=0),
  malformed_count integer not null default 0 check (malformed_count>=0),
  blocked_count integer not null default 0 check (blocked_count>=0),
  row_manifest_hash bytea check (row_manifest_hash is null or pg_catalog.octet_length(row_manifest_hash)=32),
  state text not null default 'STAGING' check (state in ('STAGING','SEALED','CURRENT','SUPERSEDED','REJECTED','CORRECTION_READY')),
  uploaded_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  uploaded_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  file_metadata_json jsonb not null default '{}'::jsonb check (pg_catalog.jsonb_typeof(file_metadata_json)='object'),
  parser_summary_json jsonb not null default '{}'::jsonb check (pg_catalog.jsonb_typeof(parser_summary_json)='object'),
  check ((purpose='FINAL_SOURCE_CORRECTION')=(correction_session_id is not null)),
  check (suggested_coverage_end_local_date is null or suggested_coverage_start_local_date<=suggested_coverage_end_local_date),
  check (confirmed_coverage_end_local_date is null or confirmed_coverage_start_local_date<=confirmed_coverage_end_local_date),
  check ((state in ('SEALED','CURRENT','SUPERSEDED','CORRECTION_READY'))=(row_manifest_hash is not null)),
  check (state not in ('SEALED','CURRENT','SUPERSEDED','CORRECTION_READY') or physical_row_count=header_count+trailer_count+continuation_count+accepted_count+blocking_economic_duplicate_count+malformed_count)
);
alter table public.weekly_source_uploads owner to postgres;
create index weekly_source_uploads_scope_idx
  on public.weekly_source_uploads(source_cycle_id,report_scope_id,state,uploaded_at_utc,id);
create index weekly_source_uploads_content_idx
  on public.weekly_source_uploads(content_sha256,byte_count,source_format_profile_id);

alter table public.weekly_final_source_correction_sessions
  add constraint weekly_final_source_correction_sessions_upload_fk
  foreign key (replacement_correction_upload_id) references public.weekly_source_uploads(id) on delete restrict;

create table public.weekly_source_upload_attempts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  environment text not null default 'TEST' check (environment in ('TEST','LIVE')),
  agency_id uuid not null,
  source_group_id uuid references public.weekly_source_groups(id) on delete restrict,
  source_cycle_id uuid references public.weekly_source_cycles(id) on delete restrict,
  report_scope_id uuid references public.weekly_source_report_scopes(id) on delete restrict,
  purpose text not null check (purpose in ('ORDINARY','FINAL_SOURCE_CORRECTION')),
  declared_scope_fingerprint bytea check (declared_scope_fingerprint is null or pg_catalog.octet_length(declared_scope_fingerprint)=32),
  correction_session_id uuid references public.weekly_final_source_correction_sessions(id) on delete restrict,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  attempted_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  original_filename text,
  byte_count bigint check (byte_count is null or byte_count>=0),
  content_sha256 bytea check (content_sha256 is null or pg_catalog.octet_length(content_sha256)=32),
  parser_version text,
  profile_version text,
  normaliser_version text,
  result text not null check (result in ('ACCEPTED','REJECTED','DUPLICATE','PARTIAL','CORRUPT','FAILED','CONFLICT')),
  reason_code text not null check (reason_code ~ '^[A-Z][A-Z0-9_]{2,99}$'),
  logical_upload_id uuid references public.weekly_source_uploads(id) on delete restrict
);
alter table public.weekly_source_upload_attempts owner to postgres;
create index weekly_source_upload_attempts_scope_idx
  on public.weekly_source_upload_attempts(source_cycle_id,report_scope_id,attempted_at_utc,id);

create table public.weekly_source_upload_supersessions (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  authority_scope_kind text not null check (authority_scope_kind in ('CYCLE','NHSP_REPORT_SCOPE')),
  report_scope_id uuid references public.weekly_source_report_scopes(id) on delete restrict,
  superseding_upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  superseded_upload_id uuid references public.weekly_source_uploads(id) on delete restrict,
  cycle_version_before bigint not null check (cycle_version_before>=0),
  cycle_version_after bigint not null check (cycle_version_after=cycle_version_before+1),
  reason text not null default 'NEW_COMPLETE_UPLOAD' check (reason='NEW_COMPLETE_UPLOAD'),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check ((authority_scope_kind='NHSP_REPORT_SCOPE')=(report_scope_id is not null)),
  check (superseded_upload_id is null or superseded_upload_id<>superseding_upload_id),
  unique (superseding_upload_id),
  unique (source_cycle_id,authority_scope_kind,report_scope_id,cycle_version_after)
);
alter table public.weekly_source_upload_supersessions owner to postgres;

create table public.weekly_source_physical_rows (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  source_row_ordinal integer not null check (source_row_ordinal>=1),
  bounded_raw_cells_json jsonb not null check (pg_catalog.jsonb_typeof(bounded_raw_cells_json)='object'),
  row_sha256 bytea not null check (pg_catalog.octet_length(row_sha256)=32),
  classification text not null check (classification in ('HEADER','TRAILER','PROFILE_PROVED_NON_ECONOMIC_CONTINUATION','ACCEPTED_SHIFT','BLOCKING_ECONOMIC_DUPLICATE','BLOCKING_MALFORMED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (upload_id,source_row_ordinal)
);
alter table public.weekly_source_physical_rows owner to postgres;

create table public.weekly_source_upload_rows (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  source_row_ordinal integer not null check (source_row_ordinal>=1),
  external_source_key text,
  source_candidate_identity text not null,
  source_client_identity text not null,
  work_date date not null,
  start_at_local timestamp without time zone,
  end_at_local timestamp without time zone,
  break_minutes integer check (break_minutes is null or break_minutes>=0),
  actual_net_minutes integer check (actual_net_minutes is null or actual_net_minutes>=0),
  row_finalisation_state text not null default 'NOT_APPLICABLE' check (row_finalisation_state in ('NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO','SOURCE_UNFINALISED','BLOCK_FINALISATION_DISAGREEMENT','BLOCK_ACTUAL_TUPLE')),
  finalised_by text,
  role_band_source text,
  source_commission_pence bigint check (source_commission_pence between -999999999999 and 999999999999),
  source_total_cost_pence bigint check (source_total_cost_pence between -999999999999 and 999999999999),
  source_shift_charge_pence bigint check (source_shift_charge_pence between -999999999999 and 999999999999),
  source_money_parse_state text not null default 'NOT_APPLICABLE' check (source_money_parse_state in ('NOT_APPLICABLE','VALID','MISSING','INVALID','EXCESS_PRECISION','FORMULA','UNSUPPORTED_CELL_TYPE','OVERFLOW')),
  source_qualification_profile_version text,
  source_expense_pence bigint check (source_expense_pence between 0 and 999999999999),
  source_expense_parse_state text not null default 'NOT_APPLICABLE' check (source_expense_parse_state in ('NOT_APPLICABLE','VALID','OMITTED_ZERO','INVALID','EXCESS_PRECISION','FORMULA','UNSUPPORTED_CELL_TYPE','OVERFLOW')),
  normalised_row_hash bytea not null check (pg_catalog.octet_length(normalised_row_hash)=32),
  bounded_raw_columns_json jsonb not null default '{}'::jsonb check (pg_catalog.jsonb_typeof(bounded_raw_columns_json)='object'),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (start_at_local is null or end_at_local is null or end_at_local>start_at_local),
  check (break_minutes is null or start_at_local is null or end_at_local is null or break_minutes<=(extract(epoch from (end_at_local-start_at_local))/60)::integer),
  check (row_finalisation_state not in ('NOT_APPLICABLE','SOURCE_WORKED') or (start_at_local is not null and end_at_local is not null and break_minutes is not null and actual_net_minutes is not null and actual_net_minutes>0)),
  check (row_finalisation_state<>'SOURCE_ABSENT_ZERO' or actual_net_minutes=0),
  check (source_shift_charge_pence is not distinct from source_commission_pence+source_total_cost_pence or source_commission_pence is null or source_total_cost_pence is null),
  unique (upload_id,source_row_ordinal)
);
alter table public.weekly_source_upload_rows owner to postgres;
create index weekly_source_upload_rows_match_idx
  on public.weekly_source_upload_rows(upload_id,work_date,source_candidate_identity,external_source_key);
create unique index weekly_source_upload_rows_external_key_uq
  on public.weekly_source_upload_rows(upload_id,external_source_key)
  where external_source_key is not null;

create table public.weekly_source_money_cell_evidence (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  source_row_ordinal integer not null check (source_row_ordinal>=1),
  money_field_kind text not null check (money_field_kind in ('COMMISSION','TOTAL_COST','FMC','BOTTOM_TOTAL_COST')),
  source_column_index integer not null check (source_column_index>=0),
  cell_coordinate text,
  source_kind text not null check (source_kind in ('XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN','HTML_DECODED_TEXT','CSV_DECODED_TEXT')),
  original_token text not null check (pg_catalog.octet_length(pg_catalog.convert_to(original_token,'UTF8'))<=512),
  decoded_token text not null check (pg_catalog.octet_length(pg_catalog.convert_to(decoded_token,'UTF8'))<=512),
  cell_type_marker text,
  formula_present boolean not null default false,
  parse_state text not null check (parse_state in ('VALID','MISSING','INVALID','EXCESS_PRECISION','FORMULA','UNSUPPORTED_CELL_TYPE','OVERFLOW')),
  parsed_pence bigint check (parsed_pence between -999999999999 and 999999999999),
  source_file_sha256 bytea not null check (pg_catalog.octet_length(source_file_sha256)=32),
  token_sha256 bytea not null check (pg_catalog.octet_length(token_sha256)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (upload_id,source_row_ordinal,money_field_kind),
  check ((parse_state='VALID')=(parsed_pence is not null))
);
alter table public.weekly_source_money_cell_evidence owner to postgres;

create table public.weekly_source_expense_cell_evidence (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  source_row_ordinal integer not null check (source_row_ordinal>=1),
  source_column_index integer not null check (source_column_index>=0),
  cell_coordinate text,
  source_kind text not null check (source_kind in ('XLSX_NUMERIC_TOKEN','XLSX_STRING_TOKEN','HTML_DECODED_TEXT','CSV_DECODED_TEXT')),
  original_token text not null check (pg_catalog.octet_length(pg_catalog.convert_to(original_token,'UTF8'))<=512),
  decoded_token text not null check (pg_catalog.octet_length(pg_catalog.convert_to(decoded_token,'UTF8'))<=512),
  cell_type_marker text,
  formula_present boolean not null default false,
  lexical_profile_version text not null default 'SOURCE_FIXED_EXPENSE_PENCE_V1' check (lexical_profile_version='SOURCE_FIXED_EXPENSE_PENCE_V1'),
  parse_state text not null check (parse_state in ('VALID','OMITTED_ZERO','INVALID','EXCESS_PRECISION','FORMULA','UNSUPPORTED_CELL_TYPE','OVERFLOW')),
  parsed_pence bigint check (parsed_pence between 0 and 999999999999),
  source_file_sha256 bytea not null check (pg_catalog.octet_length(source_file_sha256)=32),
  token_sha256 bytea not null check (pg_catalog.octet_length(token_sha256)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (upload_id,source_row_ordinal),
  check ((parse_state in ('VALID','OMITTED_ZERO'))=(parsed_pence is not null))
);
alter table public.weekly_source_expense_cell_evidence owner to postgres;

create table public.weekly_source_projection_publications (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  authority_scope_kind text not null check (authority_scope_kind in ('CYCLE','NHSP_REPORT_SCOPE')),
  report_scope_id uuid references public.weekly_source_report_scopes(id) on delete restrict,
  upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  correction_session_id uuid references public.weekly_final_source_correction_sessions(id) on delete restrict,
  authority_scope_version bigint not null check (authority_scope_version>=1),
  projection_generation integer check (projection_generation is null or projection_generation>=1),
  rebuild_idempotency_key text check (rebuild_idempotency_key is null or pg_catalog.char_length(pg_catalog.btrim(rebuild_idempotency_key)) between 1 and 200),
  rebuild_request_hash bytea check (rebuild_request_hash is null or pg_catalog.octet_length(rebuild_request_hash)=32),
  ready_session_version bigint check (ready_session_version is null or ready_session_version>=1),
  comparison_manifest_hash bytea not null check (pg_catalog.octet_length(comparison_manifest_hash)=32),
  issue_set_hash bytea not null check (pg_catalog.octet_length(issue_set_hash)=32),
  state text not null default 'BUILDING' check (state in ('BUILDING','CURRENT','CORRECTION_READY','STALE','FAILED')),
  failure_code text,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  published_at_utc timestamptz,
  check ((authority_scope_kind='NHSP_REPORT_SCOPE')=(report_scope_id is not null)),
  check (state<>'CORRECTION_READY' or correction_session_id is not null),
  check (state<>'CURRENT' or published_at_utc is not null),
  check ((rebuild_idempotency_key is null)=(rebuild_request_hash is null)),
  check (rebuild_idempotency_key is null or correction_session_id is not null),
  check (ready_session_version is null or correction_session_id is not null)
);
alter table public.weekly_source_projection_publications owner to postgres;
create unique index weekly_source_projection_publications_generation_uq
  on public.weekly_source_projection_publications(
    source_cycle_id,
    authority_scope_kind,
    coalesce(report_scope_id,'00000000-0000-0000-0000-000000000000'::uuid),
    authority_scope_version,
    upload_id,
    coalesce(projection_generation,authority_scope_version::integer)
  );
create unique index weekly_source_projection_publications_current_uq
  on public.weekly_source_projection_publications(
    source_cycle_id,
    authority_scope_kind,
    coalesce(report_scope_id,'00000000-0000-0000-0000-000000000000'::uuid)
  ) where state='CURRENT';
create unique index weekly_source_projection_publications_correction_uq
  on public.weekly_source_projection_publications(
    correction_session_id,
    coalesce(projection_generation,authority_scope_version::integer)
  )
  where correction_session_id is not null;
create unique index weekly_source_projection_publications_rebuild_idempotency_uq
  on public.weekly_source_projection_publications(correction_session_id,rebuild_idempotency_key)
  where correction_session_id is not null and rebuild_idempotency_key is not null;
alter table public.weekly_final_source_correction_sessions
  add constraint weekly_final_source_correction_sessions_publication_fk
  foreign key (replacement_projection_publication_id)
  references public.weekly_source_projection_publications(id) on delete restrict;

create table public.weekly_source_row_qualification_evidence (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  source_format_profile_id uuid not null references public.weekly_source_format_profiles(id) on delete restrict,
  evidence_kind text not null check (evidence_kind in (
    'VERIFIED_SOURCE_ROLE_CODE',
    'VERIFIED_CLIENT_CHARGE_RATE',
    'VERIFIED_CLIENT_SHIFT_CHARGE_RESULT',
    'SHIFT_CHARGE_RESULT_NON_IDENTITY'
  )),
  normalised_text text,
  exact_decimal_value numeric,
  unit text,
  currency text,
  vat_basis text,
  sign_basis text,
  evidence_hash bytea not null check (pg_catalog.octet_length(evidence_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (normalised_text is not null or exact_decimal_value is not null),
  unique (upload_row_id,evidence_kind,evidence_hash)
);
alter table public.weekly_source_row_qualification_evidence owner to postgres;

create table public.weekly_work_events (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  work_date date not null,
  identity_kind text not null check (identity_kind in ('PROFILE_EXTERNAL_KEY','SCHEDULE_TUPLE','OFFICE_PROTECTED_SHIFT')),
  profile_external_key text,
  durable_identity_hash bytea not null check (pg_catalog.octet_length(durable_identity_hash)=32),
  first_source_group_id uuid references public.weekly_source_groups(id) on delete restrict,
  source_format_profile_id uuid references public.weekly_source_format_profiles(id) on delete restrict,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (durable_identity_hash),
  check ((identity_kind='PROFILE_EXTERNAL_KEY')=(profile_external_key is not null)),
  check ((identity_kind in ('PROFILE_EXTERNAL_KEY','SCHEDULE_TUPLE'))=(source_format_profile_id is not null))
);
alter table public.weekly_work_events owner to postgres;
create index weekly_work_events_candidate_week_idx
  on public.weekly_work_events(candidate_id,work_date,client_id,id);
create unique index weekly_work_events_profile_key_uq
  on public.weekly_work_events(first_source_group_id,source_format_profile_id,profile_external_key)
  where identity_kind='PROFILE_EXTERNAL_KEY';

create table public.weekly_source_contract_qualification_observations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  generation integer not null check (generation>=1),
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  source_format_profile_id uuid not null references public.weekly_source_format_profiles(id) on delete restrict,
  qualification_profile_version text not null,
  qualification_profile_fingerprint bytea not null check (pg_catalog.octet_length(qualification_profile_fingerprint)=32),
  contract_revision_fingerprint bytea not null check (pg_catalog.octet_length(contract_revision_fingerprint)=32),
  source_shift_charge_pence bigint not null check (source_shift_charge_pence between -999999999999 and 999999999999),
  canonical_calculated_pence bigint not null check (canonical_calculated_pence between -999999999999 and 999999999999),
  source_charge_difference_pence bigint not null check (source_charge_difference_pence between -999999999999 and 999999999999),
  comparison_result text not null check (comparison_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','MISMATCH','ZERO_SOURCE_CHARGE','UNVERIFIABLE')),
  qualification_passed boolean not null,
  ordered_reason_codes text[] not null default '{}'::text[],
  evidence_fingerprint bytea not null check (pg_catalog.octet_length(evidence_fingerprint)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (source_charge_difference_pence=source_shift_charge_pence-canonical_calculated_pence),
  check (qualification_passed=(comparison_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','MISMATCH','ZERO_SOURCE_CHARGE'))),
  unique (upload_row_id,generation,contract_id)
);
alter table public.weekly_source_contract_qualification_observations owner to postgres;
create index weekly_source_contract_qual_obs_generation_idx
  on public.weekly_source_contract_qualification_observations(upload_row_id,generation,qualification_passed,contract_id);

create table public.weekly_source_row_resolutions (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  generation integer not null check (generation>=1),
  candidate_id uuid references public.candidates(id) on delete restrict,
  client_id uuid references public.clients(id) on delete restrict,
  contract_id uuid references public.contracts(id) on delete restrict,
  work_event_id uuid references public.weekly_work_events(id) on delete restrict,
  paid_minutes integer check (paid_minutes is null or paid_minutes>=0),
  rate_classifications_json jsonb check (rate_classifications_json is null or pg_catalog.jsonb_typeof(rate_classifications_json)='object'),
  mapping_state text not null check (mapping_state in (
    'RESOLVED','CANDIDATE_NOT_FOUND','CLIENT_NOT_FOUND','NO_ELIGIBLE_CONTRACT',
    'AMBIGUOUS_CONTRACT','CONTRACT_SELECTION_REQUIRED','SOURCE_ROW_BLOCKED'
  )),
  blocker_code text,
  contract_selection_method text check (contract_selection_method is null or contract_selection_method in ('AUTO_UNIQUE','OFFICE_SELECTED','DURABLE_LINEAGE')),
  work_event_match_kind text check (work_event_match_kind is null or work_event_match_kind in ('NEW_PROFILE_KEY','REUSED_PROFILE_KEY','NEW_SCHEDULE_TUPLE','EXACT_DURABLE_LINEAGE')),
  work_event_match_fingerprint bytea check (work_event_match_fingerprint is null or pg_catalog.octet_length(work_event_match_fingerprint)=32),
  qualification_profile_fingerprint bytea not null check (pg_catalog.octet_length(qualification_profile_fingerprint)=32),
  qualifying_contract_count integer not null default 0 check (qualifying_contract_count>=0),
  qualifying_contract_set_hash bytea not null check (pg_catalog.octet_length(qualifying_contract_set_hash)=32),
  source_row_fingerprint bytea not null check (pg_catalog.octet_length(source_row_fingerprint)=32),
  contract_and_rate_fingerprint bytea check (contract_and_rate_fingerprint is null or pg_catalog.octet_length(contract_and_rate_fingerprint)=32),
  effective_policy_fingerprint bytea check (effective_policy_fingerprint is null or pg_catalog.octet_length(effective_policy_fingerprint)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check ((mapping_state='RESOLVED')=(candidate_id is not null and client_id is not null and contract_id is not null and work_event_id is not null and blocker_code is null)),
  check ((mapping_state='RESOLVED')=(contract_selection_method is not null)),
  check ((mapping_state='RESOLVED')=(work_event_match_kind is not null and work_event_match_fingerprint is not null)),
  unique (upload_row_id,generation)
);
alter table public.weekly_source_row_resolutions owner to postgres;
create index weekly_source_row_resolutions_current_idx
  on public.weekly_source_row_resolutions(upload_row_id,generation desc,id);

-- The calculation used by preview, Contract qualification, finalisation and
-- source materialisation is frozen once, beside the accepted resolution.  The
-- browser never supplies these values and later owners must not reconstruct
-- them from a live Contract or from imported source money.
create table public.weekly_source_row_economic_snapshots (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  row_resolution_id uuid not null references public.weekly_source_row_resolutions(id) on delete restrict,
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  generation integer not null check (generation>=1),
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  calculator_version text not null check (calculator_version='WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1'),
  source_mode text not null check (source_mode in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')),
  rate_method text not null check (rate_method in ('SPLIT_RATE_WINDOWS','WHOLE_SHIFT_START_DAY')),
  row_sign smallint not null check (row_sign in (-1,1)),
  paid_minutes integer not null check (paid_minutes>0),
  break_minutes integer not null check (break_minutes>=0),
  minutes_day integer not null check (minutes_day>=0),
  minutes_night integer not null check (minutes_night>=0),
  minutes_sat integer not null check (minutes_sat>=0),
  minutes_sun integer not null check (minutes_sun>=0),
  minutes_bh integer not null check (minutes_bh>=0),
  hours_day numeric(7,2) not null,
  hours_night numeric(7,2) not null,
  hours_sat numeric(7,2) not null,
  hours_sun numeric(7,2) not null,
  hours_bh numeric(7,2) not null,
  pay_day numeric(10,2) not null check (pay_day>0),
  pay_night numeric(10,2) not null check (pay_night>0),
  pay_sat numeric(10,2) not null check (pay_sat>0),
  pay_sun numeric(10,2) not null check (pay_sun>0),
  pay_bh numeric(10,2) not null check (pay_bh>0),
  charge_day numeric(10,2) not null check (charge_day>0),
  charge_night numeric(10,2) not null check (charge_night>0),
  charge_sat numeric(10,2) not null check (charge_sat>0),
  charge_sun numeric(10,2) not null check (charge_sun>0),
  charge_bh numeric(10,2) not null check (charge_bh>0),
  total_pay_pence bigint not null check (total_pay_pence between -999999999999 and 999999999999),
  calculated_charge_pence bigint not null check (calculated_charge_pence between -999999999999 and 999999999999),
  total_pay_ex_vat numeric(12,2) generated always as (total_pay_pence::numeric/100) stored,
  calculated_charge_ex_vat numeric(12,2) generated always as (calculated_charge_pence::numeric/100) stored,
  invoice_vat_chargeable boolean not null,
  invoice_vat_rate_pct numeric(5,2) not null check (invoice_vat_rate_pct between 0 and 100),
  source_expense_vat_enabled boolean not null,
  canonical_result_json jsonb not null check (pg_catalog.jsonb_typeof(canonical_result_json)='object'),
  contract_and_rate_fingerprint bytea not null check (pg_catalog.octet_length(contract_and_rate_fingerprint)=32),
  effective_policy_fingerprint bytea not null check (pg_catalog.octet_length(effective_policy_fingerprint)=32),
  invoice_vat_policy_fingerprint bytea not null check (pg_catalog.octet_length(invoice_vat_policy_fingerprint)=32),
  calculation_fingerprint bytea not null check (pg_catalog.octet_length(calculation_fingerprint)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (paid_minutes=minutes_day+minutes_night+minutes_sat+minutes_sun+minutes_bh),
  check (hours_day=pg_catalog.round((minutes_day::numeric/60)*row_sign,2)),
  check (hours_night=pg_catalog.round((minutes_night::numeric/60)*row_sign,2)),
  check (hours_sat=pg_catalog.round((minutes_sat::numeric/60)*row_sign,2)),
  check (hours_sun=pg_catalog.round((minutes_sun::numeric/60)*row_sign,2)),
  check (hours_bh=pg_catalog.round((minutes_bh::numeric/60)*row_sign,2)),
  check (pg_catalog.sign(total_pay_pence)=row_sign),
  check (pg_catalog.sign(calculated_charge_pence)=row_sign),
  check (invoice_vat_chargeable or invoice_vat_rate_pct=0),
  unique (row_resolution_id),
  unique (upload_row_id,generation)
);
alter table public.weekly_source_row_economic_snapshots owner to postgres;
create index weekly_source_row_economic_snapshots_event_idx
  on public.weekly_source_row_economic_snapshots(work_event_id,created_at_utc,id);

-- Source-fixed expenses need the same immutable policy provenance even when a
-- source row is an explicit zero-hours position.  Such a row deliberately has
-- no shift-economic snapshot and can never create a worked segment.
create table public.weekly_source_row_expense_policy_snapshots (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  row_resolution_id uuid not null references public.weekly_source_row_resolutions(id) on delete restrict,
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  generation integer not null check (generation>=1),
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  source_expense_pence bigint not null check (source_expense_pence between 0 and 999999999999),
  source_expense_parse_state text not null check (source_expense_parse_state in ('VALID','OMITTED_ZERO')),
  source_expense_vat_enabled boolean not null,
  invoice_vat_chargeable boolean not null,
  invoice_vat_rate_pct numeric(5,2) not null check (invoice_vat_rate_pct between 0 and 100),
  correction_presentation text not null check (correction_presentation in ('FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION')),
  effective_policy_fingerprint bytea not null check (pg_catalog.octet_length(effective_policy_fingerprint)=32),
  invoice_vat_policy_fingerprint bytea not null check (pg_catalog.octet_length(invoice_vat_policy_fingerprint)=32),
  snapshot_hash bytea not null check (pg_catalog.octet_length(snapshot_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (source_expense_parse_state<>'OMITTED_ZERO' or source_expense_pence=0),
  check (invoice_vat_chargeable or invoice_vat_rate_pct=0),
  unique (row_resolution_id),
  unique (upload_row_id,generation),
  unique (snapshot_hash)
);
alter table public.weekly_source_row_expense_policy_snapshots owner to postgres;
create index weekly_source_row_expense_policy_snapshots_event_idx
  on public.weekly_source_row_expense_policy_snapshots(work_event_id,created_at_utc,id);

-- A source movement retains the ordinary Timesheet relationship required by
-- existing invoice locks and readers.  This immutable binding is produced by
-- the source Timesheet owner; finalisation may read it but never trusts a
-- caller-supplied Timesheet id.
create table public.weekly_source_row_timesheet_lineages (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  row_resolution_id uuid not null references public.weekly_source_row_resolutions(id) on delete restrict,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  contract_week_id uuid not null references public.contract_weeks(id) on delete restrict,
  timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  -- Decision D8: this relation is the per-source-row BINDING, written by the
  -- ensure owner before any Office authorisation.  family_booking_id and
  -- timesheet_version are kept as binding-time facts because stale detection
  -- needs them, but the authorisation record itself is per ROOT and lives in
  -- public.weekly_source_root_authorisations (proof/34 section 4's write-set
  -- table gives the first-authorisation owner as the only writer of a
  -- generation, after the ordinary Authorise succeeds).
  family_booking_id text not null check (pg_catalog.char_length(family_booking_id) between 1 and 200),
  timesheet_version integer not null check (timesheet_version>=1),
  week_ending_date date not null,
  lineage_fingerprint bytea not null check (pg_catalog.octet_length(lineage_fingerprint)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (row_resolution_id),
  unique (lineage_fingerprint)
);
alter table public.weekly_source_row_timesheet_lineages owner to postgres;
create index weekly_source_row_timesheet_lineages_event_idx
  on public.weekly_source_row_timesheet_lineages(work_event_id,contract_id,week_ending_date,id);
create index weekly_source_row_timesheet_lineages_family_idx
  on public.weekly_source_row_timesheet_lineages(family_booking_id,timesheet_version,id);

create table public.weekly_work_event_source_links (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  row_resolution_id uuid not null references public.weekly_source_row_resolutions(id) on delete restrict,
  link_kind text not null check (link_kind in ('PROVISIONAL_SOURCE','POSITIVE_SOURCE','FULL_NEGATIVE_SOURCE','ZERO_SOURCE','TIMESHEET_EVIDENCE','PROTECTED_OFFICE_ENTRY')),
  link_hash bytea not null check (pg_catalog.octet_length(link_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (upload_row_id,row_resolution_id),
  unique (link_hash)
);
alter table public.weekly_work_event_source_links owner to postgres;

create table public.weekly_source_charge_checks (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  row_resolution_id uuid not null references public.weekly_source_row_resolutions(id) on delete restrict,
  generation integer not null check (generation>=1),
  row_sign_kind text not null check (row_sign_kind in ('POSITIVE','FULL_NEGATIVE')),
  source_commission_pence bigint check (source_commission_pence between -999999999999 and 999999999999),
  source_total_cost_pence bigint check (source_total_cost_pence between -999999999999 and 999999999999),
  source_shift_charge_pence bigint check (source_shift_charge_pence between -999999999999 and 999999999999),
  calculated_segment_charge_pence bigint check (calculated_segment_charge_pence between -999999999999 and 999999999999),
  source_charge_difference_pence bigint check (source_charge_difference_pence between -999999999999 and 999999999999),
  source_commission_ex_vat numeric(12,2) generated always as (source_commission_pence::numeric/100) stored,
  source_total_cost_ex_vat numeric(12,2) generated always as (source_total_cost_pence::numeric/100) stored,
  source_shift_charge_ex_vat numeric(12,2) generated always as (source_shift_charge_pence::numeric/100) stored,
  calculated_segment_charge_ex_vat numeric(12,2) generated always as (calculated_segment_charge_pence::numeric/100) stored,
  source_charge_difference_ex_vat numeric(12,2) generated always as (source_charge_difference_pence::numeric/100) stored,
  comparison_profile_version text not null check (comparison_profile_version='NHSP_TWO_COMPONENT_PENCE_V1'),
  comparison_result text not null check (comparison_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','MISMATCH','ZERO_SOURCE_CHARGE','UNVERIFIABLE')),
  comparison_reason_code text not null,
  phase_severity text not null check (phase_severity in ('NONE','INFORMATION','PROVISIONAL_WARNING','FINALISATION_BLOCKER')),
  blocker_code text check (blocker_code is null or blocker_code in ('NHSP_SOURCE_CHARGE_MISMATCH','NHSP_PRICING_METHOD_UNVERIFIABLE','NHSP_SOURCE_CHARGE_INVALID')),
  charge_calculation_fingerprint bytea not null check (pg_catalog.octet_length(charge_calculation_fingerprint)=32),
  calculated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (source_shift_charge_pence is not distinct from source_commission_pence+source_total_cost_pence or source_commission_pence is null or source_total_cost_pence is null),
  check (source_charge_difference_pence is not distinct from source_shift_charge_pence-calculated_segment_charge_pence or source_shift_charge_pence is null or calculated_segment_charge_pence is null),
  check (comparison_result='UNVERIFIABLE' or (source_commission_pence is not null and source_total_cost_pence is not null and source_shift_charge_pence is not null and calculated_segment_charge_pence is not null and source_charge_difference_pence is not null)),
  check (comparison_result<>'EXACT' or source_charge_difference_pence=0),
  -- 25 section 6: "The tolerance is symmetric: exact signed source pence must
  -- have the same non-zero sign as the CloudTMS charge and may differ by no
  -- more than one penny in either direction."  03 section 2 (line 417):
  -- abs(source_charge_difference_pence)=1 with
  -- sign(source_shift_charge_pence)=sign(calculated_segment_charge_pence);
  -- "no directional constraint is permitted" (24 section 13, NHSP-BR-013,
  -- PRC-006, PRC-007, PRC-020, NHSBR-019; gap row XSG-013).
  check (comparison_result<>'SOURCE_ROUNDING_EQUIVALENT' or (source_charge_difference_pence in (1,-1) and ((source_shift_charge_pence>0 and calculated_segment_charge_pence>0) or (source_shift_charge_pence<0 and calculated_segment_charge_pence<0)))),
  check (comparison_result not in ('MISMATCH','ZERO_SOURCE_CHARGE','UNVERIFIABLE') or phase_severity in ('PROVISIONAL_WARNING','FINALISATION_BLOCKER')),
  check (comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE','UNVERIFIABLE') or blocker_code is null),
  check (comparison_result<>'ZERO_SOURCE_CHARGE' or (source_commission_pence=0 and source_total_cost_pence=0 and source_shift_charge_pence=0 and calculated_segment_charge_pence<>0 and blocker_code is null)),
  check (row_sign_kind<>'POSITIVE' or comparison_result='UNVERIFIABLE' or (source_commission_pence>=0 and source_total_cost_pence>=0 and ((source_shift_charge_pence>0 and calculated_segment_charge_pence>0) or (comparison_result='ZERO_SOURCE_CHARGE' and source_shift_charge_pence=0 and calculated_segment_charge_pence>0)))),
  check (row_sign_kind<>'FULL_NEGATIVE' or comparison_result='UNVERIFIABLE' or (source_commission_pence<=0 and source_total_cost_pence<=0 and source_shift_charge_pence<0 and calculated_segment_charge_pence<0)),
  unique (upload_row_id,row_resolution_id,generation)
);
alter table public.weekly_source_charge_checks owner to postgres;

-- An Office acceptance never changes either amount. It only admits the exact
-- current source row after binding every mutable authority used by preview.
-- A new upload, projection generation, Contract/rate/policy change or Recheck
-- creates different bindings, so an older acceptance becomes unusable without
-- being deleted or rewritten.
create table public.weekly_source_charge_acceptances (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  row_resolution_id uuid not null references public.weekly_source_row_resolutions(id) on delete restrict,
  charge_check_id uuid not null references public.weekly_source_charge_checks(id) on delete restrict,
  source_upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  acceptance_kind text not null check (acceptance_kind in ('ACCEPTED_DISPARITY','ACCEPTED_ZERO')),
  source_upload_hash bytea not null check (pg_catalog.octet_length(source_upload_hash)=32),
  source_row_fingerprint bytea not null check (pg_catalog.octet_length(source_row_fingerprint)=32),
  contract_and_rate_fingerprint bytea not null check (pg_catalog.octet_length(contract_and_rate_fingerprint)=32),
  effective_policy_fingerprint bytea not null check (pg_catalog.octet_length(effective_policy_fingerprint)=32),
  charge_calculation_fingerprint bytea not null check (pg_catalog.octet_length(charge_calculation_fingerprint)=32),
  acceptance_policy_fingerprint bytea not null check (pg_catalog.octet_length(acceptance_policy_fingerprint)=32),
  accepted_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  accepted_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  acceptance_hash bytea not null check (pg_catalog.octet_length(acceptance_hash)=32),
  unique (charge_check_id),
  unique (acceptance_hash)
);
alter table public.weekly_source_charge_acceptances owner to postgres;
create index weekly_source_charge_acceptances_row_idx
  on public.weekly_source_charge_acceptances(upload_row_id,row_resolution_id,charge_check_id);

create table public.weekly_source_final_revisions (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  authority_scope_kind text not null check (authority_scope_kind in ('CYCLE','NHSP_REPORT_SCOPE')),
  report_scope_id uuid references public.weekly_source_report_scopes(id) on delete restrict,
  revision_number integer not null check (revision_number>=1),
  upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  predecessor_revision_id uuid,
  coverage_start_local_date date,
  coverage_end_local_date date,
  coverage_timezone text,
  prior_state_cutoff_revision_id uuid,
  reason text not null check (reason in ('INITIAL_FINALISATION','CORRECT_FINAL_SOURCE')),
  finalised_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  finalised_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  manifest_hash bytea not null check (pg_catalog.octet_length(manifest_hash)=32),
  policy_fingerprint bytea not null check (pg_catalog.octet_length(policy_fingerprint)=32),
  state text not null check (state in ('PREPARED','CURRENT','SUPERSEDED')),
  check ((authority_scope_kind='NHSP_REPORT_SCOPE')=(report_scope_id is not null)),
  check ((authority_scope_kind='CYCLE')=(coverage_start_local_date is not null and coverage_end_local_date is not null and coverage_timezone='Europe/London')),
  check (coverage_end_local_date is null or coverage_start_local_date<=coverage_end_local_date),
  unique (source_cycle_id,authority_scope_kind,report_scope_id,revision_number),
  unique (id,source_cycle_id,authority_scope_kind,report_scope_id)
);
alter table public.weekly_source_final_revisions owner to postgres;
alter table public.weekly_source_final_revisions
  add constraint weekly_source_final_revisions_predecessor_fk
  foreign key (predecessor_revision_id) references public.weekly_source_final_revisions(id) on delete restrict;
alter table public.weekly_source_final_revisions
  add constraint weekly_source_final_revisions_prior_cutoff_fk
  foreign key (prior_state_cutoff_revision_id) references public.weekly_source_final_revisions(id) on delete restrict;
create unique index weekly_source_final_revisions_current_uq
  on public.weekly_source_final_revisions(
    source_cycle_id,
    authority_scope_kind,
    coalesce(report_scope_id,'00000000-0000-0000-0000-000000000000'::uuid)
  ) where state='CURRENT';
alter table public.weekly_final_source_correction_sessions
  add constraint weekly_final_source_correction_sessions_expected_revision_fk
  foreign key (expected_current_final_revision_id)
  references public.weekly_source_final_revisions(id) on delete restrict;
alter table public.weekly_final_source_correction_sessions
  add constraint weekly_final_source_correction_sessions_prepared_revision_fk
  foreign key (prepared_final_revision_id)
  references public.weekly_source_final_revisions(id) on delete restrict;
alter table public.weekly_final_source_correction_sessions
  add constraint weekly_final_source_correction_sessions_applied_revision_fk
  foreign key (applied_final_revision_id)
  references public.weekly_source_final_revisions(id) on delete restrict;

create table public.weekly_source_nhsp_backing_reports (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  report_scope_id uuid not null references public.weekly_source_report_scopes(id) on delete restrict,
  final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  backing_report_number text not null check (pg_catalog.char_length(pg_catalog.btrim(backing_report_number)) between 1 and 120),
  cutoff_at_utc timestamptz not null,
  physical_line_count integer not null check (physical_line_count>=0),
  source_total_cost_pence bigint not null check (source_total_cost_pence between -999999999999 and 999999999999),
  source_commission_pence bigint not null check (source_commission_pence between -999999999999 and 999999999999),
  source_invoice_total_pence bigint not null check (source_invoice_total_pence between -999999999999 and 999999999999),
  row_manifest_hash bytea not null check (pg_catalog.octet_length(row_manifest_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (source_invoice_total_pence=source_total_cost_pence+source_commission_pence),
  unique (final_revision_id)
);
alter table public.weekly_source_nhsp_backing_reports owner to postgres;
create index weekly_source_nhsp_backing_reports_number_idx
  on public.weekly_source_nhsp_backing_reports(report_scope_id,backing_report_number,created_at_utc,id);

create table public.weekly_source_client_cycle_completions (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  completion_generation integer not null check (completion_generation>=1),
  completion_kind text not null check (completion_kind in ('FINAL_SOURCE','NO_SHIFTS_TO_IMPORT')),
  final_revision_id uuid references public.weekly_source_final_revisions(id) on delete restrict,
  attested_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  attested_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  attestation_text text,
  completion_hash bytea not null check (pg_catalog.octet_length(completion_hash)=32),
  state text not null check (state in ('CURRENT','SUPERSEDED')),
  superseded_at_utc timestamptz,
  check ((completion_kind='FINAL_SOURCE')=(final_revision_id is not null)),
  check ((state='SUPERSEDED')=(superseded_at_utc is not null)),
  unique (source_cycle_id,client_id,completion_generation),
  unique (completion_hash)
);
alter table public.weekly_source_client_cycle_completions owner to postgres;
create unique index weekly_source_client_cycle_completions_current_uq
  on public.weekly_source_client_cycle_completions(source_cycle_id,client_id)
  where state='CURRENT';

create table public.weekly_source_final_snapshot_lines (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  row_resolution_id uuid not null references public.weekly_source_row_resolutions(id) on delete restrict,
  charge_check_id uuid references public.weekly_source_charge_checks(id) on delete restrict,
  source_profile_kind text not null check (source_profile_kind in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')),
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  external_event_identity text,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  work_date date not null,
  start_at_local timestamp without time zone not null,
  end_at_local timestamp without time zone not null,
  break_minutes integer not null check (break_minutes>=0),
  actual_net_minutes integer not null check (actual_net_minutes>0),
  source_classifications_json jsonb not null default '{}'::jsonb check (pg_catalog.jsonb_typeof(source_classifications_json)='object'),
  pay_vector_json jsonb not null check (pg_catalog.jsonb_typeof(pay_vector_json)='object'),
  charge_vector_json jsonb not null check (pg_catalog.jsonb_typeof(charge_vector_json)='object'),
  source_expense_pence bigint check (source_expense_pence between 0 and 999999999999),
  source_fingerprint bytea not null check (pg_catalog.octet_length(source_fingerprint)=32),
  mapping_fingerprint bytea not null check (pg_catalog.octet_length(mapping_fingerprint)=32),
  rate_fingerprint bytea not null check (pg_catalog.octet_length(rate_fingerprint)=32),
  policy_fingerprint bytea not null check (pg_catalog.octet_length(policy_fingerprint)=32),
  snapshot_line_hash bytea not null check (pg_catalog.octet_length(snapshot_line_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (end_at_local>start_at_local),
  unique (final_revision_id,work_event_id),
  unique (snapshot_line_hash)
);
alter table public.weekly_source_final_snapshot_lines owner to postgres;

create table public.weekly_source_state_transitions (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  finalisation_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  source_profile_kind text not null check (source_profile_kind in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')),
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  previous_present boolean not null,
  new_present boolean not null,
  previous_snapshot_line_id uuid references public.weekly_source_final_snapshot_lines(id) on delete restrict,
  new_snapshot_line_id uuid references public.weekly_source_final_snapshot_lines(id) on delete restrict,
  outcome text not null check (outcome in ('ADD','NO_CHANGE','AMEND','CANCEL')),
  identity_match_kind text not null,
  match_confidence text not null check (match_confidence in ('EXACT_PROFILE_KEY','EXACT_DURABLE_LINEAGE')),
  prior_state_fingerprint bytea check (prior_state_fingerprint is null or pg_catalog.octet_length(prior_state_fingerprint)=32),
  new_state_fingerprint bytea check (new_state_fingerprint is null or pg_catalog.octet_length(new_state_fingerprint)=32),
  transition_fingerprint bytea not null check (pg_catalog.octet_length(transition_fingerprint)=32),
  ordinary_source_entitlement_projection_state text not null default 'PENDING' check (ordinary_source_entitlement_projection_state in ('NOT_APPLICABLE','PENDING','PUBLISHED','FAILED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check ((outcome='ADD')=(not previous_present and new_present)),
  check ((outcome='CANCEL')=(previous_present and not new_present)),
  check ((outcome in ('NO_CHANGE','AMEND'))=(previous_present and new_present)),
  check (previous_present=(previous_snapshot_line_id is not null)),
  check (new_present=(new_snapshot_line_id is not null)),
  unique (final_revision_id,work_event_id),
  unique (transition_fingerprint)
);
alter table public.weekly_source_state_transitions owner to postgres;

-- Immutable, root-scoped evidence that final source authority was either
-- published through the ordinary Weekly Timesheet/TSFIN lifecycle or was
-- deliberately left to the protected-pay owner.  A refused locked attempt is
-- retained without consuming the final root, so a separately approved
-- correction adapter can act later without rewriting history.
create table public.weekly_source_ordinary_pay_projection_receipts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  published_timesheet_financial_id uuid references public.timesheets_financials(id) on delete restrict,
  source_profile_kind text not null check (source_profile_kind in ('GENERIC_COMPLETE_SNAPSHOT','NHSP_TRUST_BACKING_REPORT','HEALTHROSTER_ACTUAL_ROWS')),
  source_mode text not null check (source_mode in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')),
  -- Gate 2 (S9, extended): the projection no longer publishes an entitlement
  -- and no longer refuses a locked root.  'PREPARED_FOR_AUTHORISATION' is a
  -- never-authorised root prepared through the established unauthorised
  -- Timesheet/TSFIN writers (24 section 4.1) and awaiting the ordinary Office
  -- Authorise; 'PROPOSED' is an authorised root for which one complete proposed
  -- entitlement was composed and recorded as a PROPOSED decision bundle
  -- revision (24 section 4.2).  'PUBLISHED' and 'REFUSED_LOCKED' are gone:
  -- nothing here publishes, and a paid or frozen root is no longer refused
  -- because nothing is mutated.
  outcome text not null check (outcome in ('PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED')),
  -- A configured source-fixed expense can be the only economic authority for
  -- an otherwise zero-hours source row.  In that narrow case the root has no
  -- worked source unit and this count is deliberately zero.
  source_unit_count integer not null check (source_unit_count>=0),
  source_unit_outcomes_json jsonb not null check (pg_catalog.jsonb_typeof(source_unit_outcomes_json)='array'),
  source_unit_manifest_hash bytea not null check (pg_catalog.octet_length(source_unit_manifest_hash)=32),
  source_expense_authorities_json jsonb not null default '[]'::jsonb check (pg_catalog.jsonb_typeof(source_expense_authorities_json)='array'),
  source_expense_manifest_hash bytea not null check (pg_catalog.octet_length(source_expense_manifest_hash)=32),
  final_manifest_hash bytea not null check (pg_catalog.octet_length(final_manifest_hash)=32),
  final_policy_fingerprint bytea not null check (pg_catalog.octet_length(final_policy_fingerprint)=32),
  service_snapshot_hash bytea not null check (pg_catalog.octet_length(service_snapshot_hash)=32),
  server_calculation_fingerprint bytea not null check (pg_catalog.octet_length(server_calculation_fingerprint)=32),
  root_before_hash bytea not null check (pg_catalog.octet_length(root_before_hash)=32),
  root_after_hash bytea not null check (pg_catalog.octet_length(root_after_hash)=32),
  idempotency_key text not null check (pg_catalog.char_length(pg_catalog.btrim(idempotency_key)) between 1 and 200),
  request_hash bytea not null check (pg_catalog.octet_length(request_hash)=32),
  receipt_hash bytea not null check (pg_catalog.octet_length(receipt_hash)=32),
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (pg_catalog.jsonb_array_length(source_unit_outcomes_json)=source_unit_count),
  check ((source_profile_kind='NHSP_TRUST_BACKING_REPORT')=(source_mode='NHSP_WEEKLY')),
  -- Only the never-authorised initial branch writes a TSFIN, so only it may
  -- carry a published financial row and only it may move the root hash.  A
  -- PROPOSED receipt is proof that NOTHING was written on the root.
  check ((outcome='PREPARED_FOR_AUTHORISATION')=(published_timesheet_financial_id is not null)),
  check (outcome='PREPARED_FOR_AUTHORISATION' or root_after_hash=root_before_hash),
  unique (idempotency_key),
  unique (request_hash),
  unique (receipt_hash)
);
alter table public.weekly_source_ordinary_pay_projection_receipts owner to postgres;
create unique index weekly_source_ordinary_pay_projection_terminal_uq
  on public.weekly_source_ordinary_pay_projection_receipts(final_revision_id,root_timesheet_id)
  where outcome in ('PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED');
create index weekly_source_ordinary_pay_projection_root_idx
  on public.weekly_source_ordinary_pay_projection_receipts(root_timesheet_id,created_at_utc,id);

-- Immutable evidence for every previously published ordinary root that had to
-- be rebuilt when a same-cycle final source was corrected.  The exact affected
-- root set is derived by the server; the caller may only supply the current
-- service snapshot used by the existing unauthorise/write/reauthorise owner.
create table public.weekly_final_source_correction_root_impacts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  correction_session_id uuid not null references public.weekly_final_source_correction_sessions(id) on delete restrict,
  prior_final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  replacement_final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  prior_projection_receipt_id uuid references public.weekly_source_ordinary_pay_projection_receipts(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  source_profile_kind text not null check (source_profile_kind in ('GENERIC_COMPLETE_SNAPSHOT','NHSP_TRUST_BACKING_REPORT','HEALTHROSTER_ACTUAL_ROWS')),
  source_mode text not null check (source_mode in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')),
  impact_kind text not null check (impact_kind in ('PUBLISHED_REPLACEMENT_ONLY_ROOT','REPROJECTED_WITH_CURRENT_REVISION_MOVEMENTS','REPROJECTED_WITHOUT_CURRENT_REVISION_MOVEMENTS')),
  submitted_service_snapshot_hash bytea not null check (pg_catalog.octet_length(submitted_service_snapshot_hash)=32),
  impact_hash bytea not null check (pg_catalog.octet_length(impact_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check ((source_profile_kind='NHSP_TRUST_BACKING_REPORT')=(source_mode='NHSP_WEEKLY')),
  check ((prior_projection_receipt_id is null)=(impact_kind='PUBLISHED_REPLACEMENT_ONLY_ROOT')),
  unique (correction_session_id,root_timesheet_id),
  unique (replacement_final_revision_id,root_timesheet_id),
  unique (impact_hash)
);
alter table public.weekly_final_source_correction_root_impacts owner to postgres;
create index weekly_final_source_correction_root_impacts_prior_idx
  on public.weekly_final_source_correction_root_impacts(prior_final_revision_id,root_timesheet_id);

create table public.weekly_source_billing_movements (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  transition_id uuid references public.weekly_source_state_transitions(id) on delete restrict,
  nhsp_upload_row_id uuid references public.weekly_source_upload_rows(id) on delete restrict,
  -- The referenced authority table is created below; its FK is installed after
  -- that table exists.  This third origin is invoice-only and never becomes a
  -- Banking Pay or Workbench discriminator.
  expense_authority_generation_id uuid,
  final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  finalisation_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  actual_client_id uuid not null references public.clients(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  movement_role text not null check (movement_role in (
    'POSITIVE','REVERSAL','REPLACEMENT','NET_DIFFERENCE_PRESENTATION',
    'EXPENSE_POSITIVE','EXPENSE_REVERSAL','EXPENSE_REPLACEMENT'
  )),
  correction_unit_id uuid,
  prior_movement_id uuid,
  source_profile_kind text not null check (source_profile_kind in ('GENERIC_COMPLETE_SNAPSHOT','NHSP_TRUST_BACKING_REPORT','HEALTHROSTER_ACTUAL_ROWS')),
  source_line_kind text not null check (source_line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT','GENERATED_HISTORICAL_REVERSAL','NON_NHSP_DIFFERENCE','NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE','SOURCE_FIXED_EXPENSE')),
  source_facts_json jsonb not null check (pg_catalog.jsonb_typeof(source_facts_json)='object'),
  canonical_pay_vector_json jsonb not null check (pg_catalog.jsonb_typeof(canonical_pay_vector_json)='object'),
  canonical_charge_vector_json jsonb not null check (pg_catalog.jsonb_typeof(canonical_charge_vector_json)='object'),
  total_pay_ex_vat numeric(12,2) not null,
  calculated_comparison_charge_pence bigint not null check (calculated_comparison_charge_pence between -999999999999 and 999999999999),
  source_validation_charge_pence bigint check (source_validation_charge_pence between -999999999999 and 999999999999),
  invoice_presentation_charge_pence bigint not null check (invoice_presentation_charge_pence between -999999999999 and 999999999999),
  vat_rate_pct numeric(5,2) not null check (vat_rate_pct between 0 and 100),
  vat_amount numeric(12,2) not null,
  total_inc_vat numeric(12,2) not null,
  price_check_result text not null check (price_check_result in ('NOT_APPLICABLE','EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO')),
  price_check_fingerprint bytea check (price_check_fingerprint is null or pg_catalog.octet_length(price_check_fingerprint)=32),
  charge_acceptance_id uuid references public.weekly_source_charge_acceptances(id) on delete restrict,
  mapping_rate_policy_fingerprint bytea not null check (pg_catalog.octet_length(mapping_rate_policy_fingerprint)=32),
  invoice_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  original_cycle_key text not null,
  movement_economic_hash bytea not null check (pg_catalog.octet_length(movement_economic_hash)=32),
  placement_state text not null default 'UNPLACED' check (placement_state in ('UNPLACED','PLACED','ISSUED','VOIDED_BY_CORRECT_FINAL')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check ((transition_id is not null)::integer
       +(nhsp_upload_row_id is not null)::integer
       +(expense_authority_generation_id is not null)::integer=1),
  check ((source_profile_kind='NHSP_TRUST_BACKING_REPORT')=(nhsp_upload_row_id is not null)),
  check ((expense_authority_generation_id is not null)=(source_line_kind='SOURCE_FIXED_EXPENSE')),
  check ((expense_authority_generation_id is not null)=(movement_role in ('EXPENSE_POSITIVE','EXPENSE_REVERSAL','EXPENSE_REPLACEMENT'))),
  check (source_profile_kind<>'NHSP_TRUST_BACKING_REPORT' or (source_validation_charge_pence is not null and invoice_presentation_charge_pence=source_validation_charge_pence and price_check_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO'))),
  check ((price_check_result in ('ACCEPTED_DISPARITY','ACCEPTED_ZERO'))=(charge_acceptance_id is not null)),
  check (source_line_kind<>'SOURCE_FIXED_EXPENSE' or (source_validation_charge_pence is not null and invoice_presentation_charge_pence=source_validation_charge_pence and price_check_result='NOT_APPLICABLE')),
  check (source_profile_kind='NHSP_TRUST_BACKING_REPORT' or (price_check_result in ('NOT_APPLICABLE','EXACT','SOURCE_ROUNDING_EQUIVALENT') and charge_acceptance_id is null)),
  unique (movement_economic_hash),
  unique (nhsp_upload_row_id)
);
alter table public.weekly_source_billing_movements owner to postgres;
create unique index weekly_source_billing_movements_transition_role_uq
  on public.weekly_source_billing_movements(transition_id,movement_role)
  where transition_id is not null;
create unique index weekly_source_billing_movements_expense_role_uq
  on public.weekly_source_billing_movements(expense_authority_generation_id,movement_role)
  where expense_authority_generation_id is not null;
alter table public.weekly_source_billing_movements
  add constraint weekly_source_billing_movements_prior_fk
  foreign key (prior_movement_id) references public.weekly_source_billing_movements(id) on delete restrict;
create index weekly_source_billing_movements_admission_idx
  on public.weekly_source_billing_movements(final_revision_id,actual_client_id,placement_state,created_at_utc,id);
create index weekly_source_billing_movements_timesheet_lineage_idx
  on public.weekly_source_billing_movements(invoice_timesheet_id,id);

create table public.weekly_source_client_manifests (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  finalisation_week_ending date not null,
  backing_report_number text,
  manifest_hash bytea not null check (pg_catalog.octet_length(manifest_hash)=32),
  movement_count integer not null check (movement_count>=0),
  unchanged_snapshot_count integer not null default 0 check (unchanged_snapshot_count>=0),
  expense_line_count integer not null default 0 check (expense_line_count>=0),
  invoice_state text not null default 'READY' check (invoice_state in ('READY','PARTLY_ADMITTED','ADMITTED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (final_revision_id,client_id),
  unique (manifest_hash)
);
alter table public.weekly_source_client_manifests owner to postgres;
create index weekly_source_client_manifests_batch_idx
  on public.weekly_source_client_manifests(client_id,finalisation_week_ending,invoice_state,id);

create table public.weekly_source_manifest_movements (
  client_manifest_id uuid not null references public.weekly_source_client_manifests(id) on delete restrict,
  billing_movement_id uuid not null references public.weekly_source_billing_movements(id) on delete restrict,
  manifest_ordinal integer not null check (manifest_ordinal>=1),
  movement_hash bytea not null check (pg_catalog.octet_length(movement_hash)=32),
  primary key (client_manifest_id,billing_movement_id),
  unique (client_manifest_id,manifest_ordinal),
  unique (billing_movement_id)
);
alter table public.weekly_source_manifest_movements owner to postgres;

create table public.weekly_source_invoice_presentation_lines (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  billing_movement_id uuid not null references public.weekly_source_billing_movements(id) on delete restrict,
  client_manifest_id uuid not null references public.weekly_source_client_manifests(id) on delete restrict,
  final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  original_finalisation_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  line_kind text not null check (line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT','GENERATED_HISTORICAL_REVERSAL','NON_NHSP_DIFFERENCE','SOURCE_FIXED_EXPENSE')),
  origin_kind text not null check (origin_kind in ('GENERIC_TRANSITION','HEALTHROSTER_TRANSITION','NHSP_PHYSICAL_ROW','SOURCE_FIXED_EXPENSE')),
  correction_root_id uuid,
  correction_role text check (correction_role is null or correction_role in ('REVERSAL','REPLACEMENT','NET_DIFFERENCE')),
  old_internal_movement_id uuid references public.weekly_source_billing_movements(id) on delete restrict,
  new_internal_movement_id uuid references public.weekly_source_billing_movements(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  source_shift_group_id uuid not null,
  role_snapshot text,
  band_snapshot text,
  candidate_display_snapshot text not null,
  client_display_snapshot text not null,
  work_date date not null,
  start_at_local timestamp without time zone not null,
  end_at_local timestamp without time zone not null,
  break_minutes integer not null check (break_minutes>=0),
  booking_reference_snapshot text,
  description_snapshot text not null,
  hours_day numeric(7,2) not null default 0,
  hours_night numeric(7,2) not null default 0,
  hours_sat numeric(7,2) not null default 0,
  hours_sun numeric(7,2) not null default 0,
  hours_bh numeric(7,2) not null default 0,
  pay_day numeric(10,2),
  pay_night numeric(10,2),
  pay_sat numeric(10,2),
  pay_sun numeric(10,2),
  pay_bh numeric(10,2),
  charge_day numeric(10,2),
  charge_night numeric(10,2),
  charge_sat numeric(10,2),
  charge_sun numeric(10,2),
  charge_bh numeric(10,2),
  total_pay_ex_vat numeric(12,2) not null,
  total_charge_ex_vat numeric(12,2) not null,
  calculated_comparison_charge_pence bigint not null check (calculated_comparison_charge_pence between -999999999999 and 999999999999),
  source_validation_charge_pence bigint check (source_validation_charge_pence between -999999999999 and 999999999999),
  invoice_presentation_charge_pence bigint not null check (invoice_presentation_charge_pence between -999999999999 and 999999999999),
  margin_ex_vat numeric(12,2) not null,
  vat_rate_pct numeric(5,2) not null check (vat_rate_pct between 0 and 100),
  vat_amount numeric(12,2) not null,
  total_inc_vat numeric(12,2) not null,
  price_check_result text not null check (price_check_result in ('NOT_APPLICABLE','EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO')),
  price_check_profile text,
  charge_acceptance_id uuid references public.weekly_source_charge_acceptances(id) on delete restrict,
  mapping_rate_policy_fingerprint bytea not null check (pg_catalog.octet_length(mapping_rate_policy_fingerprint)=32),
  source_hash bytea not null check (pg_catalog.octet_length(source_hash)=32),
  amount_authority text not null check (amount_authority in ('CLOUDTMS_CALCULATION','VALIDATED_SOURCE_PENCE')),
  presentation_hash bytea not null check (pg_catalog.octet_length(presentation_hash)=32),
  -- 24 section 12: "a source-fixed expense declared as part of the same source
  -- presentation follows its defined companion relationship; no unrelated
  -- expense or work-event line moves accidentally".  The link lives on the
  -- expense presentation and points at the shift presentation it follows.
  companion_presentation_line_id uuid references public.weekly_source_invoice_presentation_lines(id) on delete restrict,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (companion_presentation_line_id is null or line_kind='SOURCE_FIXED_EXPENSE'),
  check (companion_presentation_line_id is distinct from id),
  check (end_at_local>start_at_local),
  check (invoice_presentation_charge_pence=(total_charge_ex_vat*100)::bigint),
  check (margin_ex_vat=total_charge_ex_vat-total_pay_ex_vat),
  check (origin_kind<>'NHSP_PHYSICAL_ROW' or (amount_authority='VALIDATED_SOURCE_PENCE' and source_validation_charge_pence is not null and invoice_presentation_charge_pence=source_validation_charge_pence and price_check_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO'))),
  check ((price_check_result in ('ACCEPTED_DISPARITY','ACCEPTED_ZERO'))=(charge_acceptance_id is not null)),
  check (origin_kind<>'SOURCE_FIXED_EXPENSE' or (amount_authority='VALIDATED_SOURCE_PENCE' and source_validation_charge_pence is not null and invoice_presentation_charge_pence=source_validation_charge_pence and price_check_result='NOT_APPLICABLE')),
  unique (billing_movement_id),
  unique (presentation_hash)
);
alter table public.weekly_source_invoice_presentation_lines owner to postgres;
create index weekly_source_invoice_presentation_lines_manifest_idx
  on public.weekly_source_invoice_presentation_lines(client_manifest_id,work_date,candidate_display_snapshot,id);
create index weekly_source_invoice_presentation_lines_companion_idx
  on public.weekly_source_invoice_presentation_lines(companion_presentation_line_id)
  where companion_presentation_line_id is not null;

create table public.weekly_source_invoice_line_bindings (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  billing_movement_id uuid not null references public.weekly_source_billing_movements(id) on delete restrict,
  presentation_line_id uuid not null references public.weekly_source_invoice_presentation_lines(id) on delete restrict,
  invoice_line_id uuid not null references public.invoice_lines(id) on delete restrict,
  invoice_id uuid not null references public.invoices(id) on delete restrict,
  original_final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  original_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  correction_root_id uuid,
  correction_role text,
  manifest_hash bytea not null check (pg_catalog.octet_length(manifest_hash)=32),
  materialised_line_hash bytea not null check (pg_catalog.octet_length(materialised_line_hash)=32),
  binding_version integer not null check (binding_version>=1),
  prior_binding_id uuid,
  state text not null check (state in ('CURRENT','SUPERSEDED','VOIDED')),
  bound_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  superseded_at_utc timestamptz,
  check ((state='CURRENT')=(superseded_at_utc is null)),
  unique (billing_movement_id,binding_version)
);
alter table public.weekly_source_invoice_line_bindings owner to postgres;
alter table public.weekly_source_invoice_line_bindings
  add constraint weekly_source_invoice_line_bindings_prior_fk
  foreign key (prior_binding_id) references public.weekly_source_invoice_line_bindings(id) on delete restrict;
create unique index weekly_source_invoice_line_bindings_current_movement_uq
  on public.weekly_source_invoice_line_bindings(billing_movement_id) where state='CURRENT';
create index weekly_source_invoice_line_bindings_line_history_idx
  on public.weekly_source_invoice_line_bindings(invoice_line_id,binding_version,billing_movement_id);
-- A FULL presentation has one movement per line.  The narrowly supported
-- non-NHSP NET presentation has exactly one generated reversal and one
-- replacement movement bound to the same immutable visible line.  The invoice
-- admission owner proves that pair; this lookup index deliberately does not
-- pretend that every current line has a one-movement cardinality.
create index weekly_source_invoice_line_bindings_current_line_idx
  on public.weekly_source_invoice_line_bindings(invoice_line_id) where state='CURRENT';

create table public.weekly_source_invoice_placements (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  billing_movement_id uuid not null references public.weekly_source_billing_movements(id) on delete restrict,
  source_shift_group_id uuid not null,
  invoice_id uuid references public.invoices(id) on delete restrict,
  invoice_line_id uuid references public.invoice_lines(id) on delete restrict,
  placement_revision integer not null check (placement_revision>=1),
  placement_state text not null check (placement_state in ('PLACED','UNPLACED')),
  is_current boolean not null default true,
  original_automatic_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  current_target_cycle_id uuid references public.weekly_source_cycles(id) on delete restrict,
  placement_reason text not null check (placement_reason in ('AUTOMATIC_SOURCE_BATCH','OFFICE_MOVE_BETWEEN_UNISSUED_INVOICES','SOURCE_DRAFT_VOID','CORRECT_FINAL_SOURCE')),
  actor_user_id uuid references public.tms_users(id) on delete restrict,
  prior_placement_id uuid,
  source_invoice_version_fingerprint bytea check (source_invoice_version_fingerprint is null or pg_catalog.octet_length(source_invoice_version_fingerprint)=32),
  destination_invoice_version_fingerprint bytea check (destination_invoice_version_fingerprint is null or pg_catalog.octet_length(destination_invoice_version_fingerprint)=32),
  placement_hash bytea not null check (pg_catalog.octet_length(placement_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check ((placement_state='PLACED')=(invoice_id is not null and invoice_line_id is not null)),
  unique (billing_movement_id,placement_revision),
  unique (placement_hash)
);
alter table public.weekly_source_invoice_placements owner to postgres;
alter table public.weekly_source_invoice_placements
  add constraint weekly_source_invoice_placements_prior_fk
  foreign key (prior_placement_id) references public.weekly_source_invoice_placements(id) on delete restrict;
create unique index weekly_source_invoice_placements_current_uq
  on public.weekly_source_invoice_placements(billing_movement_id)
  where is_current;

create table public.weekly_expense_authority_generations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  row_expense_policy_snapshot_id uuid references public.weekly_source_row_expense_policy_snapshots(id) on delete restrict,
  prior_expense_authority_generation_id uuid,
  generation integer not null check (generation>=1),
  source_observation_kind text not null check (source_observation_kind in ('ROW_PRESENT','OMITTED_IN_COMPLETE_COVERAGE')),
  correction_presentation text not null check (correction_presentation in ('FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION')),
  source_expense_pence bigint not null check (source_expense_pence between 0 and 999999999999),
  source_expense_vat_enabled boolean not null,
  candidate_reimbursement_ex_vat numeric(12,2) not null,
  client_charge_ex_vat numeric(12,2) not null,
  authority_hash bytea not null check (pg_catalog.octet_length(authority_hash)=32),
  state text not null check (state in ('PREPARED','CURRENT','SUPERSEDED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (candidate_reimbursement_ex_vat=source_expense_pence::numeric/100),
  check (client_charge_ex_vat=candidate_reimbursement_ex_vat),
  check ((source_observation_kind='ROW_PRESENT')=(row_expense_policy_snapshot_id is not null)),
  check (source_observation_kind<>'OMITTED_IN_COMPLETE_COVERAGE' or (source_expense_pence=0 and prior_expense_authority_generation_id is not null)),
  unique (work_event_id,generation),
  unique (authority_hash)
);
alter table public.weekly_expense_authority_generations owner to postgres;
alter table public.weekly_expense_authority_generations
  add constraint weekly_expense_authority_generations_prior_fk
  foreign key (prior_expense_authority_generation_id)
  references public.weekly_expense_authority_generations(id) on delete restrict;
create unique index weekly_expense_authority_generations_row_snapshot_uq
  on public.weekly_expense_authority_generations(row_expense_policy_snapshot_id)
  where row_expense_policy_snapshot_id is not null;
alter table public.weekly_source_billing_movements
  add constraint weekly_source_billing_movements_expense_authority_fk
  foreign key (expense_authority_generation_id)
  references public.weekly_expense_authority_generations(id) on delete restrict;
create unique index weekly_expense_authority_generations_current_uq
  on public.weekly_expense_authority_generations(work_event_id) where state='CURRENT';

create table public.weekly_source_expense_materialisations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  expense_authority_generation_id uuid not null references public.weekly_expense_authority_generations(id) on delete restrict,
  billing_movement_id uuid not null references public.weekly_source_billing_movements(id) on delete restrict,
  invoice_presentation_line_id uuid references public.weekly_source_invoice_presentation_lines(id) on delete restrict,
  expense_lineage_hash bytea not null check (pg_catalog.octet_length(expense_lineage_hash)=32),
  state text not null check (state in ('MATERIALISED','SUPERSEDED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (expense_authority_generation_id),
  unique (expense_lineage_hash)
);
alter table public.weekly_source_expense_materialisations owner to postgres;

-- Candidate-pay provenance is deliberately separate from the invoice facet so
-- invoice admission and ordinary TSFIN publication are order-independent.  It
-- records the source-fixed expense on the same ordinary Weekly Timesheet; it
-- is not an expense Timesheet and is not a Banking Pay discriminator.
create table public.weekly_source_expense_pay_materialisations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  expense_authority_generation_id uuid not null
    references public.weekly_expense_authority_generations(id) on delete restrict,
  billing_movement_id uuid not null
    references public.weekly_source_billing_movements(id) on delete restrict,
  root_timesheet_id uuid not null
    references public.timesheets(timesheet_id) on delete restrict,
  candidate_timesheet_financial_id uuid not null
    references public.timesheets_financials(id) on delete restrict,
  pay_lineage_hash bytea not null
    check (pg_catalog.octet_length(pay_lineage_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (expense_authority_generation_id,candidate_timesheet_financial_id),
  unique (pay_lineage_hash)
);
alter table public.weekly_source_expense_pay_materialisations owner to postgres;
create index weekly_source_expense_pay_materialisations_authority_idx
  on public.weekly_source_expense_pay_materialisations(
    expense_authority_generation_id,created_at_utc,id
  );
create index weekly_source_expense_pay_materialisations_root_idx
  on public.weekly_source_expense_pay_materialisations(
    root_timesheet_id,created_at_utc,id
  );

create table public.weekly_discrepancy_incidents (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  episode_number integer not null check (episode_number>=1),
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  state text not null check (state in ('OPEN','RESOLVED','SUPERSEDED','CANCELLED')),
  current_comparison_revision_id uuid,
  reconciliation_state text not null check (reconciliation_state in ('UNRESOLVED','WAITING_FOR_SOURCE','READY_TO_ACCEPT','RECONCILED','NOT_WORKED')),
  candidate_action_state text not null check (candidate_action_state in ('NOT_ASKED','ASKED','RESPONDED','NOT_REQUIRED','WAITING_FOR_TIMESHEET')),
  manager_potential_state text not null check (manager_potential_state in ('NOT_AVAILABLE','AVAILABLE','NOT_REQUIRED')),
  manager_action_state text not null check (manager_action_state in ('NOT_SENT','DUE','SENT','RESPONDED','NOT_REQUIRED')),
  waiting_source_state text not null check (waiting_source_state in ('NOT_WAITING','WAITING_REIMPORT','SOURCE_CHANGED','SOURCE_MATCHED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  resolved_at_utc timestamptz,
  resolution_kind text check (resolution_kind is null or resolution_kind in ('SOURCE_MATCHED','OFFICE_ACCEPTED_SYSTEM_HOURS','CANDIDATE_CORRECTED','MANAGER_CONFIRMED_SYSTEM_HOURS','RECORDED_NOT_WORKED','CORRECT_FINAL_SOURCE')),
  check ((state='RESOLVED')=(resolved_at_utc is not null and resolution_kind is not null)),
  unique (source_group_id,work_event_id,episode_number)
);
alter table public.weekly_discrepancy_incidents owner to postgres;
create unique index weekly_discrepancy_incidents_open_uq
  on public.weekly_discrepancy_incidents(source_group_id,work_event_id)
  where state='OPEN';
create index weekly_discrepancy_incidents_progress_idx
  on public.weekly_discrepancy_incidents(source_cycle_id,state,client_id,candidate_id,id);

create table public.weekly_issue_comparison_revisions (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
  revision_number integer not null check (revision_number>=1),
  projection_publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
  comparison_upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  final_revision_id uuid references public.weekly_source_final_revisions(id) on delete restrict,
  candidate_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  candidate_timesheet_revision integer,
  candidate_shift_fingerprint bytea check (candidate_shift_fingerprint is null or pg_catalog.octet_length(candidate_shift_fingerprint)=32),
  source_row_id uuid references public.weekly_source_upload_rows(id) on delete restrict,
  source_work_event_link_id uuid references public.weekly_work_event_source_links(id) on delete restrict,
  contract_id uuid references public.contracts(id) on delete restrict,
  issue_family text not null check (issue_family in ('SOURCE_MISSING_OR_NOT_AUTHORISED','SOURCE_HOURS_DIFFER','CANDIDATE_TIMESHEET_MISSING','REFERENCE_MISSING','HEALTHROSTER_NOT_FINALISED')),
  source_presence text not null check (source_presence in ('PRESENT','ABSENT','UNFINALISED')),
  candidate_start_at_local timestamp without time zone,
  candidate_end_at_local timestamp without time zone,
  candidate_break_minutes integer,
  system_start_at_local timestamp without time zone,
  system_end_at_local timestamp without time zone,
  system_break_minutes integer,
  material_comparison_fingerprint bytea not null check (pg_catalog.octet_length(material_comparison_fingerprint)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (incident_id,revision_number),
  unique (material_comparison_fingerprint)
);
alter table public.weekly_issue_comparison_revisions owner to postgres;
alter table public.weekly_discrepancy_incidents
  add constraint weekly_discrepancy_incidents_current_comparison_fk
  foreign key (current_comparison_revision_id) references public.weekly_issue_comparison_revisions(id) on delete restrict;

create table public.weekly_route_activations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  audience_route text not null check (audience_route in ('CANDIDATE','MANAGER')),
  route_mode text not null default 'NOT_STARTED' check (route_mode in ('NOT_STARTED','CANDIDATE_FIRST','MANAGER_DIRECT','MANUAL_ONLY','DISABLED')),
  activated_by_user_id uuid references public.tms_users(id) on delete restrict,
  activated_at_utc timestamptz,
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (source_cycle_id,candidate_id,client_id,audience_route),
  check ((route_mode='NOT_STARTED')=(activated_at_utc is null))
);
alter table public.weekly_route_activations owner to postgres;

create table public.weekly_candidate_cohorts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  manager_recipient_route_key bytea not null check (pg_catalog.octet_length(manager_recipient_route_key)=32),
  current_generation_id uuid,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (source_cycle_id,candidate_id,client_id,manager_recipient_route_key)
);
alter table public.weekly_candidate_cohorts owner to postgres;

create table public.weekly_candidate_outreach_generations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  candidate_cohort_id uuid not null references public.weekly_candidate_cohorts(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  generation_number integer not null check (generation_number>=1),
  activation_id uuid not null references public.weekly_route_activations(id) on delete restrict,
  trigger_kind text not null check (trigger_kind in ('OFFICE_ASK','NEW_INCIDENT','REOPENED_INCIDENT')),
  request_kind text not null check (request_kind in ('CHECK_HOURS','SUBMIT_TIMESHEET')),
  route_mode text not null check (route_mode='CANDIDATE_FIRST'),
  started_at_utc timestamptz not null,
  reminder_due_at_utc timestamptz not null,
  deadline_at_utc timestamptz not null,
  manual_reminder_available_at_utc timestamptz,
  state text not null check (state in ('ACTIVE','SUPERSEDED','COMPLETE','CANCELLED')),
  membership_hash bytea not null check (pg_catalog.octet_length(membership_hash)=32),
  superseded_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  constraint weekly_candidate_outreach_generations_clock_ck check (
    reminder_due_at_utc>started_at_utc
    and deadline_at_utc>reminder_due_at_utc
  ),
  check ((state='SUPERSEDED')=(superseded_at_utc is not null)),
  unique (candidate_cohort_id,generation_number)
);
alter table public.weekly_candidate_outreach_generations owner to postgres;
create unique index weekly_candidate_outreach_generations_active_uq
  on public.weekly_candidate_outreach_generations(candidate_cohort_id) where state='ACTIVE';
alter table public.weekly_candidate_cohorts
  add constraint weekly_candidate_cohorts_current_generation_fk
  foreign key (current_generation_id) references public.weekly_candidate_outreach_generations(id) on delete restrict;

create table public.weekly_candidate_outreach_memberships (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  candidate_generation_id uuid not null references public.weekly_candidate_outreach_generations(id) on delete restrict,
  incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
  comparison_revision_id uuid not null references public.weekly_issue_comparison_revisions(id) on delete restrict,
  ordinal integer not null check (ordinal>=1),
  state text not null check (state in ('ACTIONABLE','ANSWERED','RESOLVED','SUPERSEDED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (candidate_generation_id,incident_id),
  unique (candidate_generation_id,ordinal)
);
alter table public.weekly_candidate_outreach_memberships owner to postgres;

create table public.weekly_timesheet_submission_requests (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  environment text not null default 'TEST' check (environment in ('TEST','LIVE')),
  agency_id uuid not null,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  candidate_cohort_id uuid not null references public.weekly_candidate_cohorts(id) on delete restrict,
  request_generation integer not null check (request_generation>=1),
  current_upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  current_projection_publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
  state text not null check (state in ('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED','COMPLETE','SUPERSEDED','CANCELLED')),
  started_at_utc timestamptz,
  reminder_due_at_utc timestamptz,
  deadline_at_utc timestamptz,
  membership_hash bytea not null check (pg_catalog.octet_length(membership_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  constraint weekly_timesheet_submission_requests_clock_ck check (
    (started_at_utc is null and reminder_due_at_utc is null and deadline_at_utc is null)
    or
    (started_at_utc is not null
     and reminder_due_at_utc>started_at_utc
     and deadline_at_utc>reminder_due_at_utc)
  ),
  unique (source_cycle_id,candidate_id,request_generation)
);
alter table public.weekly_timesheet_submission_requests owner to postgres;
create unique index weekly_timesheet_submission_requests_active_uq
  on public.weekly_timesheet_submission_requests(source_cycle_id,candidate_id)
  where state in ('READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED');

create table public.weekly_timesheet_submission_request_memberships (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  submission_request_id uuid not null references public.weekly_timesheet_submission_requests(id) on delete restrict,
  ordinal integer not null check (ordinal>=1),
  week_ending date not null,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  expected_source_fingerprint bytea not null check (pg_catalog.octet_length(expected_source_fingerprint)=32),
  state text not null check (state in ('WAITING','SUBMITTED_MATCHED','SUBMITTED_WITH_ISSUES')),
  submitted_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  submitted_timesheet_revision integer,
  submitted_timesheet_hash bytea check (submitted_timesheet_hash is null or pg_catalog.octet_length(submitted_timesheet_hash)=32),
  completed_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check ((state='WAITING')=(submitted_timesheet_id is null and submitted_timesheet_revision is null and submitted_timesheet_hash is null and completed_at_utc is null)),
  unique (submission_request_id,week_ending,client_id,contract_id),
  unique (submission_request_id,ordinal)
);
alter table public.weekly_timesheet_submission_request_memberships owner to postgres;

create table public.weekly_candidate_response_drafts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  candidate_generation_id uuid not null references public.weekly_candidate_outreach_generations(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  draft_version integer not null check (draft_version>=1),
  current_projection_publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
  state text not null check (state in ('DRAFT','SUBMITTED','SUPERSEDED')),
  draft_hash bytea not null check (pg_catalog.octet_length(draft_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  submitted_at_utc timestamptz,
  unique (candidate_generation_id,draft_version),
  check ((state='SUBMITTED')=(submitted_at_utc is not null))
);
alter table public.weekly_candidate_response_drafts owner to postgres;

create table public.weekly_candidate_response_draft_items (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  response_draft_id uuid not null references public.weekly_candidate_response_drafts(id) on delete restrict,
  incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
  comparison_revision_id uuid not null references public.weekly_issue_comparison_revisions(id) on delete restrict,
  choice text not null check (choice in ('CANDIDATE_WRONG','CANDIDATE_CORRECT','NEITHER_CORRECT')),
  corrected_start_at_local timestamp without time zone,
  corrected_end_at_local timestamp without time zone,
  corrected_break_minutes integer,
  response_fingerprint bytea not null check (pg_catalog.octet_length(response_fingerprint)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (
    (corrected_start_at_local is null and corrected_end_at_local is null and corrected_break_minutes is null)
    or
    (corrected_start_at_local is not null and corrected_end_at_local is not null and corrected_break_minutes is not null)
  ),
  check (
    (choice='CANDIDATE_CORRECT' and corrected_start_at_local is null)
    or choice='CANDIDATE_WRONG'
    or (choice='NEITHER_CORRECT' and corrected_start_at_local is not null)
  ),
  check (corrected_end_at_local is null or corrected_end_at_local>corrected_start_at_local),
  unique (response_draft_id,incident_id)
);
alter table public.weekly_candidate_response_draft_items owner to postgres;

-- Exact private-Worker mutation receipts.  Candidate-facing draft/final
-- retries must return the first committed response before consulting mutable
-- outreach, publication or Timesheet state.  No financial data belongs here.
create table public.weekly_candidate_app_mutation_receipts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  candidate_generation_id uuid not null
    references public.weekly_candidate_outreach_generations(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  mutation_kind text not null check (mutation_kind in ('DRAFT_SAVE','FINAL_SUBMIT')),
  idempotency_key uuid not null unique,
  request_hash bytea not null check (pg_catalog.octet_length(request_hash)=32),
  response_json jsonb not null check (pg_catalog.jsonb_typeof(response_json)='object'),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (candidate_generation_id,mutation_kind,idempotency_key)
);
alter table public.weekly_candidate_app_mutation_receipts owner to postgres;

create table public.weekly_discrepancy_events (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
  issue_episode integer not null check (issue_episode>=1),
  projection_publication_id uuid references public.weekly_source_projection_publications(id) on delete restrict,
  expected_comparison_fingerprint bytea check (expected_comparison_fingerprint is null or pg_catalog.octet_length(expected_comparison_fingerprint)=32),
  event_kind text not null check (event_kind in ('REQUESTED','REMINDER_DUE','REMINDER_SENT','CANDIDATE_RESPONDED','OFFICE_ACCEPTED','MANAGER_RESPONDED','SOURCE_RECHECKED','RESOLVED')),
  actor_kind text not null check (actor_kind in ('SYSTEM','OFFICE','CANDIDATE','MANAGER')),
  actor_user_id uuid references public.tms_users(id) on delete restrict,
  bounded_payload_json jsonb not null default '{}'::jsonb check (pg_catalog.jsonb_typeof(bounded_payload_json)='object'),
  idempotency_key text not null,
  occurred_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (event_kind,idempotency_key)
);
alter table public.weekly_discrepancy_events owner to postgres;

create table public.weekly_manager_recipient_routes (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  environment text not null default 'TEST' check (environment in ('TEST','LIVE')),
  agency_id uuid not null,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  normalised_recipient_hash bytea not null check (pg_catalog.octet_length(normalised_recipient_hash)=32),
  protected_recipient_address text not null,
  current_generation_id uuid,
  manager_send_available_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (environment,agency_id,source_cycle_id,normalised_recipient_hash)
);
alter table public.weekly_manager_recipient_routes owner to postgres;

create table public.weekly_manager_recipient_generations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  recipient_route_id uuid not null references public.weekly_manager_recipient_routes(id) on delete restrict,
  generation_number integer not null check (generation_number>=1),
  trigger_kind text not null check (trigger_kind in ('OFFICE_DIRECT','EARLY_ALL','T6_RESPONDED','T12_REMAINDER','NEW_INCIDENT','MANUAL_RESEND')),
  state text not null check (state in ('ACTIVE','SUPERSEDED','COMPLETE','CANCELLED')),
  membership_hash bytea not null check (pg_catalog.octet_length(membership_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  superseded_at_utc timestamptz,
  check ((state='SUPERSEDED')=(superseded_at_utc is not null)),
  unique (recipient_route_id,generation_number)
);
alter table public.weekly_manager_recipient_generations owner to postgres;
create unique index weekly_manager_recipient_generations_active_uq
  on public.weekly_manager_recipient_generations(recipient_route_id) where state='ACTIVE';
alter table public.weekly_manager_recipient_routes
  add constraint weekly_manager_recipient_routes_current_generation_fk
  foreign key (current_generation_id) references public.weekly_manager_recipient_generations(id) on delete restrict;

create table public.weekly_manager_recipient_memberships (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  recipient_generation_id uuid not null references public.weekly_manager_recipient_generations(id) on delete restrict,
  route_activation_id uuid not null references public.weekly_route_activations(id) on delete restrict,
  candidate_outreach_generation_id uuid references public.weekly_candidate_outreach_generations(id) on delete restrict,
  candidate_cohort_id uuid not null references public.weekly_candidate_cohorts(id) on delete restrict,
  incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
  comparison_revision_id uuid not null references public.weekly_issue_comparison_revisions(id) on delete restrict,
  membership_role text not null check (membership_role in ('MANAGER_ACTIONABLE','WAITING_SOURCE')),
  ordinal integer not null check (ordinal>=1),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (recipient_generation_id,incident_id),
  unique (recipient_generation_id,ordinal)
);
alter table public.weekly_manager_recipient_memberships owner to postgres;

create table public.weekly_manager_cohort_due_events (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  recipient_generation_id uuid not null references public.weekly_manager_recipient_generations(id) on delete restrict,
  candidate_cohort_id uuid not null references public.weekly_candidate_cohorts(id) on delete restrict,
  event_kind text not null check (event_kind in ('EARLY_ALL','T6_RESPONDED','T12_REMAINDER')),
  cohort_started_at_utc timestamptz not null,
  due_at_utc timestamptz not null,
  state text not null check (state in ('PENDING','CLAIMED','CONSUMED','RETIRED')),
  trigger_hash bytea not null check (pg_catalog.octet_length(trigger_hash)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (recipient_generation_id,candidate_cohort_id,event_kind,trigger_hash)
);
alter table public.weekly_manager_cohort_due_events owner to postgres;

create table public.weekly_message_intents (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  environment text not null default 'TEST' check (environment in ('TEST','LIVE')),
  agency_id uuid not null,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  audience_kind text not null check (audience_kind in ('CANDIDATE','MANAGER','PACK_COPY','OFFICE')),
  candidate_cohort_id uuid references public.weekly_candidate_cohorts(id) on delete restrict,
  candidate_generation_id uuid references public.weekly_candidate_outreach_generations(id) on delete restrict,
  recipient_route_id uuid references public.weekly_manager_recipient_routes(id) on delete restrict,
  recipient_generation_id uuid references public.weekly_manager_recipient_generations(id) on delete restrict,
  sorted_due_event_ids uuid[] not null default '{}'::uuid[],
  tranche_kind text not null,
  tranche_sequence integer not null check (tranche_sequence>=1),
  logical_key bytea not null check (pg_catalog.octet_length(logical_key)=32),
  state text not null default 'DUE' check (state in ('DUE','RENDERED','DISPATCHED','RETIRED')),
  due_at_utc timestamptz not null,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check ((audience_kind='CANDIDATE')=(candidate_cohort_id is not null and candidate_generation_id is not null)),
  check ((audience_kind='MANAGER')=(recipient_route_id is not null and recipient_generation_id is not null)),
  unique (logical_key)
);
alter table public.weekly_message_intents owner to postgres;

create table public.weekly_message_renders (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  message_intent_id uuid not null references public.weekly_message_intents(id) on delete restrict,
  render_sequence integer not null check (render_sequence>=1),
  projection_publication_id uuid references public.weekly_source_projection_publications(id) on delete restrict,
  membership_hash bytea not null check (pg_catalog.octet_length(membership_hash)=32),
  policy_version text not null,
  renderer_version text not null,
  subject_text text,
  html_body text,
  plain_body text not null,
  rendered_content_hash bytea not null check (pg_catalog.octet_length(rendered_content_hash)=32),
  provider_idempotency_key text not null,
  state text not null check (state in ('CURRENT','STALE','SUBMISSION_STARTED','DEFINITELY_REJECTED','AMBIGUOUS','ACCEPTED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (message_intent_id,render_sequence),
  unique (rendered_content_hash,provider_idempotency_key)
);
alter table public.weekly_message_renders owner to postgres;

create table public.weekly_message_dispatch_commands (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  message_intent_id uuid not null references public.weekly_message_intents(id) on delete restrict,
  message_render_id uuid not null references public.weekly_message_renders(id) on delete restrict,
  environment text not null default 'TEST' check (environment in ('TEST','LIVE')),
  agency_id uuid not null,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  recipient_route_id uuid references public.weekly_manager_recipient_routes(id) on delete restrict,
  candidate_generation_id uuid references public.weekly_candidate_outreach_generations(id) on delete restrict,
  tranche_kind text not null check (tranche_kind in ('CANDIDATE_INITIAL','CANDIDATE_REMINDER_6H','CANDIDATE_MANUAL_REMINDER','TIMESHEET_SUBMISSION_INITIAL','TIMESHEET_SUBMISSION_REMINDER_6H','MANAGER_EARLY_ALL','MANAGER_T6_RESPONDED','MANAGER_T12_REMAINDER','MANAGER_MANUAL_SELECTED','MANAGER_MANUAL_RESEND','COMPLETED_PACK_COPY')),
  tranche_sequence integer not null check (tranche_sequence>=1),
  sorted_trigger_event_hash bytea not null check (pg_catalog.octet_length(sorted_trigger_event_hash)=32),
  membership_hash bytea not null check (pg_catalog.octet_length(membership_hash)=32),
  policy_version text not null,
  logical_key bytea not null check (pg_catalog.octet_length(logical_key)=32),
  state text not null check (state in ('READY','LEASED','SUBMISSION_STARTED','ACCEPTED','FAILED','AMBIGUOUS','RETIRED')),
  lease_owner text,
  lease_token uuid,
  lease_expires_at_utc timestamptz,
  attempt_count integer not null default 0 check (attempt_count>=0),
  next_attempt_at_utc timestamptz,
  projection_publication_id uuid references public.weekly_source_projection_publications(id) on delete restrict,
  rendered_content_hash bytea not null check (pg_catalog.octet_length(rendered_content_hash)=32),
  provider_message_id text,
  provider_accepted_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (logical_key)
);
alter table public.weekly_message_dispatch_commands owner to postgres;
create index weekly_message_dispatch_commands_due_idx
  on public.weekly_message_dispatch_commands(state,next_attempt_at_utc,id);

create table public.weekly_message_provider_attempts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  dispatch_command_id uuid not null references public.weekly_message_dispatch_commands(id) on delete restrict,
  attempt_number integer not null check (attempt_number>=1),
  channel text not null check (channel in ('PUSH','EMAIL')),
  target_kind text not null check (target_kind in ('CANDIDATE_DEVICE','MANAGER_ADDRESS','PACK_COPY_ADDRESS')),
  keyed_target_fingerprint bytea not null check (pg_catalog.octet_length(keyed_target_fingerprint)=32),
  provider_idempotency_key text not null,
  started_at_utc timestamptz not null,
  completed_at_utc timestamptz,
  outcome text check (outcome is null or outcome in ('ACCEPTED','DEFINITELY_REJECTED','TRANSIENT_FAILURE','AMBIGUOUS')),
  bounded_provider_receipt_json jsonb not null default '{}'::jsonb check (pg_catalog.jsonb_typeof(bounded_provider_receipt_json)='object'),
  bounded_error_json jsonb not null default '{}'::jsonb check (pg_catalog.jsonb_typeof(bounded_error_json)='object'),
  unique (dispatch_command_id,keyed_target_fingerprint,attempt_number),
  unique (provider_idempotency_key)
);
alter table public.weekly_message_provider_attempts owner to postgres;

create table public.weekly_manager_review_batches (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  recipient_generation_id uuid not null references public.weekly_manager_recipient_generations(id) on delete restrict,
  message_render_id uuid not null references public.weekly_message_renders(id) on delete restrict,
  policy_version text not null,
  renderer_version text not null,
  structure_version text not null,
  sent_content_hash bytea not null check (pg_catalog.octet_length(sent_content_hash)=32),
  original_membership_hash bytea not null check (pg_catalog.octet_length(original_membership_hash)=32),
  control_plane_ticket_id uuid not null,
  agency_receipt_id uuid not null,
  credential_generation integer not null check (credential_generation>=1),
  opaque_credential_hash bytea not null check (pg_catalog.octet_length(opaque_credential_hash)=32),
  issued_at_utc timestamptz not null,
  expires_at_utc timestamptz not null,
  state text not null check (state in ('ACTIVE','COMPLETE','EXPIRED','REVOKED')),
  send_sequence integer not null default 1 check (send_sequence>=1),
  lease_owner text,
  lease_token uuid,
  lease_expires_at_utc timestamptz,
  completed_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (expires_at_utc>=issued_at_utc+interval '1 day'
    and expires_at_utc<=issued_at_utc+interval '30 days'),
  unique (recipient_generation_id,credential_generation),
  unique (opaque_credential_hash),
  unique (control_plane_ticket_id),
  unique (agency_receipt_id)
);
alter table public.weekly_manager_review_batches owner to postgres;

create table public.weekly_manager_review_items (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  review_batch_id uuid not null references public.weekly_manager_review_batches(id) on delete restrict,
  incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
  incident_episode integer not null check (incident_episode>=1),
  sent_comparison_revision_id uuid not null references public.weekly_issue_comparison_revisions(id) on delete restrict,
  sent_comparison_fingerprint bytea not null check (pg_catalog.octet_length(sent_comparison_fingerprint)=32),
  client_order integer not null check (client_order>=1),
  candidate_order integer not null check (candidate_order>=1),
  shift_order integer not null check (shift_order>=1),
  response_state text not null check (response_state in ('UNANSWERED','ANSWERED','FILTERED_RESOLVED','OBSOLETE')),
  response_kind text check (response_kind is null or response_kind in ('SYSTEM_CORRECT','CANDIDATE_DID_NOT_WORK','MANAGER_CORRECTED_SOURCE')),
  intended_start_at_local timestamp without time zone,
  intended_end_at_local timestamp without time zone,
  intended_break_minutes integer,
  response_fingerprint bytea,
  answered_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check ((response_state='ANSWERED')=(response_kind is not null and response_fingerprint is not null and answered_at_utc is not null)),
  check (response_kind<>'MANAGER_CORRECTED_SOURCE' or (intended_start_at_local is not null and intended_end_at_local is not null and intended_break_minutes is not null)),
  unique (review_batch_id,incident_id)
);
alter table public.weekly_manager_review_items owner to postgres;

create table public.weekly_manager_route_receipts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  review_batch_id uuid not null references public.weekly_manager_review_batches(id) on delete restrict,
  recipient_generation_id uuid not null references public.weekly_manager_recipient_generations(id) on delete restrict,
  control_plane_ticket_id uuid not null,
  environment text not null default 'TEST' check (environment in ('TEST','LIVE')),
  agency_id uuid not null,
  data_plane_identity text not null,
  route_version text not null,
  credential_version text not null,
  original_membership_hash bytea not null check (pg_catalog.octet_length(original_membership_hash)=32),
  credential_generation integer not null check (credential_generation>=1),
  credential_hash bytea not null check (pg_catalog.octet_length(credential_hash)=32),
  issued_at_utc timestamptz not null,
  expires_at_utc timestamptz not null,
  semantic_hash bytea not null check (pg_catalog.octet_length(semantic_hash)=32),
  state text not null check (state in ('ACTIVE','REVOKED','EXPIRED','COMPLETE')),
  revoked_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (expires_at_utc>=issued_at_utc+interval '1 day'
    and expires_at_utc<=issued_at_utc+interval '30 days'),
  unique (review_batch_id,credential_generation),
  unique (control_plane_ticket_id),
  unique (credential_hash)
);
alter table public.weekly_manager_route_receipts owner to postgres;

-- Reserves the exact secure-review identity before any remote control-plane
-- registration or email rendering occurs.  This closes the otherwise circular
-- dependency where the policy-rendered email needs its review-batch URL before
-- the immutable render (and therefore the review batch) can be committed.
-- The opaque credential itself is never stored here.
create table public.weekly_manager_route_preparations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  message_intent_id uuid not null references public.weekly_message_intents(id) on delete restrict,
  projection_publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
  recipient_generation_id uuid not null references public.weekly_manager_recipient_generations(id) on delete restrict,
  preparation_sequence integer not null check (preparation_sequence>=1),
  review_batch_id uuid not null unique,
  credential_generation integer not null check (credential_generation>=1),
  original_membership_hash bytea not null check (pg_catalog.octet_length(original_membership_hash)=32),
  issued_at_utc timestamptz not null,
  expires_at_utc timestamptz not null,
  state text not null check (state in ('PREPARED','BOUND','RETIRED')),
  bound_at_utc timestamptz,
  retired_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (expires_at_utc>=issued_at_utc+interval '1 day'
    and expires_at_utc<=issued_at_utc+interval '30 days'),
  check ((state='BOUND')=(bound_at_utc is not null)),
  check ((state='RETIRED')=(retired_at_utc is not null)),
  unique (message_intent_id,preparation_sequence),
  unique (recipient_generation_id,credential_generation)
);
alter table public.weekly_manager_route_preparations owner to postgres;
create unique index weekly_manager_route_preparations_current_uq
  on public.weekly_manager_route_preparations(message_intent_id)
  where state in ('PREPARED','BOUND');

create table public.weekly_completed_pack_copy_events (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  document_mode text not null check (document_mode in ('CHECK_ONLY','INVOICE_EVIDENCE_REQUIRED')),
  timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  timesheet_revision integer not null check (timesheet_revision>=1),
  final_document_hash bytea not null check (pg_catalog.octet_length(final_document_hash)=32),
  completion_generation integer not null check (completion_generation>=1),
  recipient_snapshot text not null,
  content_policy_version text not null,
  provider_command_id uuid references public.weekly_message_dispatch_commands(id) on delete restrict,
  state text not null check (state in ('READY','SENT','FAILED','CANCELLED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (timesheet_id,timesheet_revision,completion_generation)
);
alter table public.weekly_completed_pack_copy_events owner to postgres;

create table public.office_action_notifications (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  recipient_user_id uuid not null references public.tms_users(id) on delete restrict,
  event_kind text not null check (event_kind in ('WEEKLY_CANDIDATE_SOURCE_DISPUTED','WEEKLY_MANAGER_SYSTEM_CONFIRMED','WEEKLY_MANAGER_SOURCE_CORRECTED')),
  issue_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
  issue_generation integer not null check (issue_generation>=1),
  response_event_id uuid not null references public.weekly_discrepancy_events(id) on delete restrict,
  payload_json jsonb not null check (pg_catalog.jsonb_typeof(payload_json)='object'),
  dedupe_key text not null,
  operational_state text not null check (operational_state in ('OPEN','RESOLVED')),
  read_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  resolved_at_utc timestamptz,
  check ((operational_state='RESOLVED')=(resolved_at_utc is not null)),
  unique (recipient_user_id,response_event_id),
  unique (recipient_user_id,dedupe_key)
);
alter table public.office_action_notifications owner to postgres;

create table public.weekly_timesheet_authority_resolutions (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  work_date date not null,
  authority_mode text not null check (authority_mode='TIMESHEET_AUTHORITY'),
  document_mode text not null check (document_mode='INVOICE_EVIDENCE_REQUIRED'),
  require_reference_to_pay boolean not null default false,
  require_reference_to_invoice boolean not null,
  auto_authorise_enabled boolean not null,
  effective_policy_fingerprint bytea not null check (pg_catalog.octet_length(effective_policy_fingerprint)=32),
  resolved_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (source_cycle_id,contract_id,work_date,effective_policy_fingerprint)
);
alter table public.weekly_timesheet_authority_resolutions owner to postgres;

create table public.weekly_timesheet_source_comparisons (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  projection_publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
  upload_row_id uuid references public.weekly_source_upload_rows(id) on delete restrict,
  timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  timesheet_revision integer not null check (timesheet_revision>=1),
  -- WP-04 handoff N8: the established validation-only comparison engine
  -- (public.hr_weekly_validation_preview) emits five classes.  UNMATCHED (a
  -- signed Timesheet day with no source row) and AMBIGUOUS (more than one
  -- candidate source row) resolve to no single work event, and HR_ONLY (a
  -- source row with no signed Timesheet day) has no Candidate start or end.
  -- Exactly these three columns are therefore nullable, and the CHECK below
  -- keeps each of them mandatory for the classes that do carry the fact.
  work_event_id uuid references public.weekly_work_events(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  work_date date not null,
  comparison_state text not null check (comparison_state in ('EXACT_MATCH','HOURS_MISMATCH','SOURCE_SHIFT_MISSING','REFERENCE_MISSING','AMBIGUOUS_SOURCE_ROW')),
  candidate_start_at_local timestamp without time zone,
  candidate_end_at_local timestamp without time zone,
  candidate_break_minutes integer not null check (candidate_break_minutes>=0),
  source_start_at_local timestamp without time zone,
  source_end_at_local timestamp without time zone,
  source_break_minutes integer,
  total_break_minutes_match boolean not null,
  source_reference_number text,
  comparison_fingerprint bytea not null check (pg_catalog.octet_length(comparison_fingerprint)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (candidate_end_at_local is null or candidate_end_at_local>candidate_start_at_local),
  check ((candidate_start_at_local is null)=(candidate_end_at_local is null)),
  -- A paired class carries both sides; every row carries at least one side.
  check (comparison_state not in ('EXACT_MATCH','HOURS_MISMATCH') or (work_event_id is not null and candidate_start_at_local is not null)),
  check (work_event_id is not null or candidate_start_at_local is not null),
  check (source_end_at_local is null or source_end_at_local>source_start_at_local),
  check (comparison_state<>'EXACT_MATCH' or (upload_row_id is not null and source_reference_number is not null and total_break_minutes_match)),
  unique (projection_publication_id,timesheet_id,timesheet_revision,work_event_id),
  unique (comparison_fingerprint)
);
alter table public.weekly_timesheet_source_comparisons owner to postgres;

create table public.weekly_timesheet_reference_apply_operations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  projection_publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
  timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  expected_timesheet_revision integer not null check (expected_timesheet_revision>=1),
  expected_item_count integer not null check (expected_item_count>=1),
  expected_item_manifest_hash bytea not null check (pg_catalog.octet_length(expected_item_manifest_hash)=32),
  auto_authorise_requested boolean not null,
  auto_authorise_applied boolean not null default false,
  state text not null check (state in ('READY','APPLIED','REFUSED','FAILED')),
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  idempotency_key text not null,
  operation_hash bytea not null check (pg_catalog.octet_length(operation_hash)=32),
  result_code text,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  completed_at_utc timestamptz,
  unique (idempotency_key),
  unique (operation_hash),
  check ((state in ('APPLIED','REFUSED','FAILED'))=(completed_at_utc is not null))
);
alter table public.weekly_timesheet_reference_apply_operations owner to postgres;

create table public.weekly_timesheet_reference_apply_items (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  operation_id uuid not null references public.weekly_timesheet_reference_apply_operations(id) on delete restrict,
  item_ordinal integer not null check (item_ordinal>=1),
  comparison_id uuid not null references public.weekly_timesheet_source_comparisons(id) on delete restrict,
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  work_date date not null,
  reference_number text not null check (nullif(pg_catalog.btrim(reference_number),'') is not null),
  reference_fact_hash bytea not null check (pg_catalog.octet_length(reference_fact_hash)=32),
  applied_at_utc timestamptz,
  unique (operation_id,item_ordinal),
  unique (operation_id,comparison_id),
  unique (operation_id,upload_row_id)
);
alter table public.weekly_timesheet_reference_apply_items owner to postgres;

-- Protected-hours state is source-owned evidence for the separately installed
-- C1 complete-entitlement producer.  The ordinary Weekly Timesheet remains the
-- sole public Timesheet/economic root: this schema deliberately contains no
-- reversal/replacement Timesheet, Contract Week, TSFIN, Draft, Case, recovery,
-- settlement or payment member table.
create table public.weekly_exceptional_pay_target_families (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  agency_id uuid not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  week_start_date date not null,
  week_ending_date date not null,
  root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  -- S8 (WB-016; handed to WP-06 by WP-03_NEEDS.md N1).  The family used to be
  -- keyed by unique (root_timesheet_id), which binds it to one PHYSICAL
  -- Timesheet id and breaks the moment the root rotates.  The identity is the
  -- Timesheet FAMILY, so the key moves to the trimmed booking id, exactly as
  -- WP-01a keyed weekly_source_entitlement_heads.  root_timesheet_id stays
  -- because eight foreign keys and several existence predicates use it.
  root_family_booking_id text not null check (pg_catalog.char_length(pg_catalog.btrim(root_family_booking_id)) between 1 and 200),
  target_domain text not null default 'WEEKLY_PROTECTED_PAY' check (target_domain='WEEKLY_PROTECTED_PAY'),
  target_domain_version text not null default 'C1_V1' check (target_domain_version='C1_V1'),
  ownership_state text not null check (ownership_state in ('ORDINARY_SOURCE','TARGET_MANAGED')),
  first_signed_evidence_fingerprint bytea not null check (pg_catalog.octet_length(first_signed_evidence_fingerprint)=32),
  current_generation_id uuid,
  current_generation_number integer not null default 0 check (current_generation_number>=0),
  current_complete_target_vector_hash bytea check (current_complete_target_vector_hash is null or pg_catalog.octet_length(current_complete_target_vector_hash)=32),
  current_source_proposal_hash bytea check (current_source_proposal_hash is null or pg_catalog.octet_length(current_source_proposal_hash)=32),
  current_lifecycle_state text not null check (current_lifecycle_state in ('PENDING_APPROVAL','PROTECTED','WAITING_SOURCE','READY_TO_RECONCILE','RECONCILED','NOT_WORKED','ACTION_REQUIRED')),
  c1_publication_state text not null default 'NONE' check (c1_publication_state in ('NONE','PENDING','PUBLISHING','LIVE','FAILED')),
  current_component_count integer not null default 0 check (current_component_count>=0),
  bound_version bigint not null default 1 check (bound_version>=1),
  creation_idempotency_key text not null,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (week_start_date<=week_ending_date),
  check (week_ending_date-week_start_date=6),
  unique (agency_id,candidate_id,contract_id,week_ending_date),
  unique (creation_idempotency_key)
);
alter table public.weekly_exceptional_pay_target_families owner to postgres;
-- S8: the family key.  An expression index, NOT a stored generated column:
-- WP-01a_NEEDS.md N6 records that private._weekly_source_immutable_fact_guard_v1
-- compares to_jsonb(OLD) with to_jsonb(NEW) at BEFORE time, where PostgreSQL has
-- not yet computed generated columns, so a generated key would refuse every
-- lifecycle update on any relation the ACL closure guards.
create unique index weekly_exceptional_pay_target_families_family_uq
  on public.weekly_exceptional_pay_target_families(pg_catalog.btrim(root_family_booking_id));
create index weekly_exceptional_pay_target_families_root_idx
  on public.weekly_exceptional_pay_target_families(root_timesheet_id);

-- The stored family string is free text, so without this guard a caller could
-- attach any booking id to a root and take a different advisory lock from the
-- one protecting the uniqueness index above (proof/32 section 6 step 2).  Same
-- shape as WP-01a's head root-identity guard, with one difference: because S8
-- is additive over installed writers that do not yet name the new column, a
-- NULL is FILLED from the row's own root rather than refused.  A value that is
-- supplied and wrong is still refused, so the guard can only ever tighten.
create function private.weekly_source_target_family_root_identity_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_booking_id text;
begin
  select timesheet_row.booking_id into v_booking_id
  from public.timesheets timesheet_row
  where timesheet_row.timesheet_id=new.root_timesheet_id;

  if found and new.root_family_booking_id is null then
    new.root_family_booking_id:=v_booking_id;
  end if;

  if not found or v_booking_id is distinct from new.root_family_booking_id then
    raise exception 'WEEKLY_SOURCE_TARGET_FAMILY_ROOT_IDENTITY_MISMATCH'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_TARGET_FAMILY_ROOT_IDENTITY_MISMATCH',
              'root_timesheet_id',new.root_timesheet_id,
              'stored_family_booking_id',new.root_family_booking_id
            )::text;
  end if;
  return new;
end;
$function$;
alter function private.weekly_source_target_family_root_identity_v1() owner to postgres;
revoke all on function private.weekly_source_target_family_root_identity_v1()
  from public,anon,authenticated,service_role;
create trigger weekly_exceptional_pay_target_families_root_identity
  before insert or update on public.weekly_exceptional_pay_target_families
  for each row execute function private.weekly_source_target_family_root_identity_v1();

create table public.weekly_exceptional_payment_approvals (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  pay_target_family_id uuid not null references public.weekly_exceptional_pay_target_families(id) on delete restrict,
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  evidence_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  week_ending date not null,
  protected_work_date date not null,
  protected_start_at_local timestamp without time zone not null,
  protected_end_at_local timestamp without time zone not null,
  protected_break_minutes integer not null check (protected_break_minutes>=0),
  signed_submission_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  signed_submission_revision integer,
  signed_submission_hash bytea check (signed_submission_hash is null or pg_catalog.octet_length(signed_submission_hash)=32),
  signed_at_utc timestamptz,
  contributing_issue_episode_ids uuid[] not null default '{}'::uuid[],
  contributing_issue_episode_ids_hash bytea not null check (pg_catalog.octet_length(contributing_issue_episode_ids_hash)=32),
  signed_schedule_fact_hash bytea not null check (pg_catalog.octet_length(signed_schedule_fact_hash)=32),
  contract_rate_policy_source_fingerprint bytea not null check (pg_catalog.octet_length(contract_rate_policy_source_fingerprint)=32),
  approved_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  approval_reason text not null check (pg_catalog.char_length(pg_catalog.btrim(approval_reason)) between 1 and 1000),
  approved_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  comparison_revision_id uuid references public.weekly_issue_comparison_revisions(id) on delete restrict,
  final_revision_id uuid references public.weekly_source_final_revisions(id) on delete restrict,
  approved_target_pay_components_json jsonb not null check (pg_catalog.jsonb_typeof(approved_target_pay_components_json)='object'),
  approved_target_gross numeric(12,2) not null,
  creation_orchestration_run_id uuid,
  approval_hash bytea not null check (pg_catalog.octet_length(approval_hash)=32),
  creation_idempotency_key text not null,
  -- proof/36 section 5 step 7: a first-authorisation withdrawal marks the
  -- approved protected-hours decision withdrawn.  The row is never deleted;
  -- its history stays visible on the Office Weekly detail and in Audit.
  withdrawn_at_utc timestamptz,
  withdrawn_by_user_id uuid references public.tms_users(id) on delete restrict,
  withdrawal_kind text check (withdrawal_kind is null or withdrawal_kind='FIRST_AUTHORISATION_WITHDRAWN'),
  check ((withdrawn_at_utc is null)=(withdrawn_by_user_id is null)),
  check ((withdrawn_at_utc is null)=(withdrawal_kind is null)),
  check (protected_end_at_local>protected_start_at_local),
  check (protected_break_minutes<=(extract(epoch from (protected_end_at_local-protected_start_at_local))/60)::integer),
  check ((signed_submission_timesheet_id is null and signed_submission_revision is null and signed_submission_hash is null and signed_at_utc is null) or (signed_submission_timesheet_id is not null and signed_submission_revision is not null and signed_submission_hash is not null and signed_at_utc is not null)),
  unique (pay_target_family_id,work_event_id,approval_hash),
  unique (creation_idempotency_key)
);
alter table public.weekly_exceptional_payment_approvals owner to postgres;

create table public.weekly_exceptional_pay_family_events (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  family_id uuid not null references public.weekly_exceptional_pay_target_families(id) on delete restrict,
  event_sequence bigint not null check (event_sequence>=1),
  durable_work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  evidence_approval_id uuid not null references public.weekly_exceptional_payment_approvals(id) on delete restrict,
  work_date date not null,
  start_at_local timestamp without time zone not null,
  end_at_local timestamp without time zone not null,
  break_minutes integer not null check (break_minutes>=0),
  rate_classification_json jsonb not null check (pg_catalog.jsonb_typeof(rate_classification_json)='object'),
  source_proposal_snapshot_json jsonb not null check (pg_catalog.jsonb_typeof(source_proposal_snapshot_json)='object'),
  source_proposal_hash bytea not null check (pg_catalog.octet_length(source_proposal_hash)=32),
  fixed_office_target_snapshot_json jsonb not null check (pg_catalog.jsonb_typeof(fixed_office_target_snapshot_json)='object'),
  fixed_office_target_hash bytea not null check (pg_catalog.octet_length(fixed_office_target_hash)=32),
  state text not null check (state in ('WAIT','ACCEPTED_SOURCE','NOT_WORKED')),
  current_comparison_revision_id uuid references public.weekly_issue_comparison_revisions(id) on delete restrict,
  current_final_revision_id uuid references public.weekly_source_final_revisions(id) on delete restrict,
  office_actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  office_reason text not null,
  occurred_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  prior_event_hash bytea check (prior_event_hash is null or pg_catalog.octet_length(prior_event_hash)=32),
  event_hash bytea not null check (pg_catalog.octet_length(event_hash)=32),
  check (end_at_local>start_at_local),
  unique (family_id,event_sequence),
  unique (event_hash)
);
alter table public.weekly_exceptional_pay_family_events owner to postgres;

create table public.weekly_exceptional_pay_generations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  family_id uuid not null references public.weekly_exceptional_pay_target_families(id) on delete restrict,
  generation_number integer not null check (generation_number>=1),
  prior_generation_id uuid,
  prior_generation_hash bytea check (prior_generation_hash is null or pg_catalog.octet_length(prior_generation_hash)=32),
  request_idempotency_key text not null,
  reason text not null check (reason in ('INITIAL_APPROVAL','OFFICE_AMENDMENT','SOURCE_CHANGED','RECONCILE','RECORD_NOT_WORKED','WITHDRAW')),
  complete_prior_vector_json jsonb not null check (pg_catalog.jsonb_typeof(complete_prior_vector_json)='object'),
  complete_prior_vector_hash bytea not null check (pg_catalog.octet_length(complete_prior_vector_hash)=32),
  complete_next_vector_json jsonb not null check (pg_catalog.jsonb_typeof(complete_next_vector_json)='object'),
  complete_next_vector_hash bytea not null check (pg_catalog.octet_length(complete_next_vector_hash)=32),
  fixed_target_source_state_fingerprint bytea not null check (pg_catalog.octet_length(fixed_target_source_state_fingerprint)=32),
  lifecycle_state text not null check (lifecycle_state in ('REQUESTED','READY_TO_STAGE','STAGING','CERTIFIED','PUBLISHED','SUPERSEDED','FAILED','PENDING_C1')),
  superseded_by_generation_id uuid,
  superseded_at_utc timestamptz,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  published_at_utc timestamptz,
  result_hash bytea check (result_hash is null or pg_catalog.octet_length(result_hash)=32),
  check ((generation_number=1)=(prior_generation_id is null and prior_generation_hash is null)),
  check (generation_number=1 or (prior_generation_id is not null and prior_generation_hash is not null)),
  check ((lifecycle_state='PUBLISHED')=(published_at_utc is not null and result_hash is not null)),
  check ((lifecycle_state='SUPERSEDED')=(superseded_by_generation_id is not null and superseded_at_utc is not null)),
  unique (family_id,generation_number),
  unique (request_idempotency_key)
);
alter table public.weekly_exceptional_pay_generations owner to postgres;
alter table public.weekly_exceptional_pay_generations
  add constraint weekly_exceptional_pay_generations_prior_fk
  foreign key (prior_generation_id) references public.weekly_exceptional_pay_generations(id) on delete restrict;
alter table public.weekly_exceptional_pay_generations
  add constraint weekly_exceptional_pay_generations_superseded_by_fk
  foreign key (superseded_by_generation_id) references public.weekly_exceptional_pay_generations(id) on delete restrict;
create unique index weekly_exceptional_pay_generations_published_uq
  on public.weekly_exceptional_pay_generations(family_id) where lifecycle_state='PUBLISHED';
alter table public.weekly_exceptional_pay_target_families
  add constraint weekly_exceptional_pay_target_families_current_generation_fk
  foreign key (current_generation_id) references public.weekly_exceptional_pay_generations(id) on delete restrict;

create table public.weekly_exceptional_pay_target_events (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  family_id uuid not null references public.weekly_exceptional_pay_target_families(id) on delete restrict,
  approval_id uuid not null references public.weekly_exceptional_payment_approvals(id) on delete restrict,
  event_sequence bigint not null check (event_sequence>=1),
  triggering_comparison_revision_id uuid references public.weekly_issue_comparison_revisions(id) on delete restrict,
  triggering_final_revision_id uuid references public.weekly_source_final_revisions(id) on delete restrict,
  prior_event_fingerprint bytea check (prior_event_fingerprint is null or pg_catalog.octet_length(prior_event_fingerprint)=32),
  fixed_target_component_snapshot jsonb not null check (pg_catalog.jsonb_typeof(fixed_target_component_snapshot)='object'),
  current_source_proposal_snapshot jsonb not null check (pg_catalog.jsonb_typeof(current_source_proposal_snapshot)='object'),
  complete_prior_family_vector_fingerprint bytea not null check (pg_catalog.octet_length(complete_prior_family_vector_fingerprint)=32),
  complete_next_family_vector_fingerprint bytea not null check (pg_catalog.octet_length(complete_next_family_vector_fingerprint)=32),
  reason text not null check (reason in ('INITIAL_APPROVAL','OFFICE_AMENDMENT','SOURCE_CHANGED','WAIT','RECONCILE','RECORD_NOT_WORKED','WITHDRAW')),
  resulting_lifecycle_state text not null,
  financial_generation_id uuid references public.weekly_exceptional_pay_generations(id) on delete restrict,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  occurred_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  event_hash bytea not null check (pg_catalog.octet_length(event_hash)=32),
  idempotency_key text not null,
  unique (family_id,event_sequence),
  unique (event_hash),
  unique (idempotency_key),
  check (reason='WAIT' or financial_generation_id is not null)
);
alter table public.weekly_exceptional_pay_target_events owner to postgres;

create table public.weekly_exceptional_orchestration_runs (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  family_id uuid not null references public.weekly_exceptional_pay_target_families(id) on delete restrict,
  request_kind text not null check (request_kind in ('PREVIEW','APPROVE','AMEND','WITHDRAW','WAIT','RECONCILE','RECORD_NOT_WORKED','PUBLISH_C1')),
  idempotency_key text not null,
  requested_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  state text not null check (state in ('RUNNING','COMPLETE','PENDING','FAILED','REFUSED')),
  request_fingerprint bytea check (
    request_fingerprint is null or pg_catalog.octet_length(request_fingerprint)=32
  ),
  before_state_fingerprint bytea not null check (pg_catalog.octet_length(before_state_fingerprint)=32),
  after_state_fingerprint bytea check (after_state_fingerprint is null or pg_catalog.octet_length(after_state_fingerprint)=32),
  started_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  completed_at_utc timestamptz,
  unique (idempotency_key)
);
alter table public.weekly_exceptional_orchestration_runs owner to postgres;
alter table public.weekly_exceptional_payment_approvals
  add constraint weekly_exceptional_payment_approvals_run_fk
  foreign key (creation_orchestration_run_id) references public.weekly_exceptional_orchestration_runs(id) on delete restrict;
alter table public.weekly_exceptional_payment_approvals
  alter column creation_orchestration_run_id set not null;

create table public.weekly_exceptional_orchestration_steps (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  orchestration_run_id uuid not null references public.weekly_exceptional_orchestration_runs(id) on delete restrict,
  sequence integer not null check (sequence>=1),
  step_kind text not null,
  idempotency_key text not null,
  allowlisted_owner_name text,
  allowlisted_owner_signature text,
  bounded_request_hash bytea not null check (pg_catalog.octet_length(bounded_request_hash)=32),
  before_state_fingerprint bytea not null check (pg_catalog.octet_length(before_state_fingerprint)=32),
  bounded_owner_response_json jsonb not null default '{}'::jsonb check (pg_catalog.jsonb_typeof(bounded_owner_response_json)='object'),
  owner_response_hash bytea check (owner_response_hash is null or pg_catalog.octet_length(owner_response_hash)=32),
  after_state_fingerprint bytea check (after_state_fingerprint is null or pg_catalog.octet_length(after_state_fingerprint)=32),
  outcome text not null check (outcome in ('STARTED','COMPLETE','PENDING','FAILED','REFUSED')),
  started_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  completed_at_utc timestamptz,
  unique (orchestration_run_id,sequence),
  unique (idempotency_key)
);
alter table public.weekly_exceptional_orchestration_steps owner to postgres;

create table public.weekly_exceptional_pending_reconciliation_targets (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  family_id uuid not null references public.weekly_exceptional_pay_target_families(id) on delete restrict,
  approval_id uuid not null references public.weekly_exceptional_payment_approvals(id) on delete restrict,
  durable_work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  incident_id uuid references public.weekly_discrepancy_incidents(id) on delete restrict,
  current_final_revision_id uuid references public.weekly_source_final_revisions(id) on delete restrict,
  intended_outcome text not null check (intended_outcome in ('WAIT','ACCEPT_SOURCE','RECORD_NOT_WORKED')),
  source_action_policy_target_fingerprint bytea not null check (pg_catalog.octet_length(source_action_policy_target_fingerprint)=32),
  state text not null check (state in ('ACTIVE','SUPERSEDED','CONSUMED','WITHDRAWN')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  completed_at_utc timestamptz,
  unique (family_id,approval_id,source_action_policy_target_fingerprint)
);
alter table public.weekly_exceptional_pending_reconciliation_targets owner to postgres;
create unique index weekly_exceptional_pending_reconciliation_targets_active_uq
  on public.weekly_exceptional_pending_reconciliation_targets(family_id,approval_id)
  where state='ACTIVE';

create table public.weekly_exceptional_payment_events (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  family_id uuid not null references public.weekly_exceptional_pay_target_families(id) on delete restrict,
  approval_id uuid references public.weekly_exceptional_payment_approvals(id) on delete restrict,
  event_kind text not null check (event_kind in ('APPROVED','PROTECTED_STATE_OBSERVED','SOURCE_MATCH_PROPOSED','WAIT','MATCH_ACCEPTED','RECORDED_NOT_WORKED','RECONCILIATION_RESULT')),
  lifecycle_view text not null,
  bounded_payload_json jsonb not null default '{}'::jsonb check (pg_catalog.jsonb_typeof(bounded_payload_json)='object'),
  idempotency_key text not null,
  occurred_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (idempotency_key)
);
alter table public.weekly_exceptional_payment_events owner to postgres;

create table public.weekly_exceptional_c1_publication_requests (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  family_id uuid not null references public.weekly_exceptional_pay_target_families(id) on delete restrict,
  generation_id uuid not null references public.weekly_exceptional_pay_generations(id) on delete restrict,
  orchestration_run_id uuid not null references public.weekly_exceptional_orchestration_runs(id) on delete restrict,
  agency_id uuid not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  week_ending_date date not null,
  source_mode text not null check (source_mode in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')),
  request_sequence bigint not null check (request_sequence>=1),
  expected_head_revision bigint not null check (expected_head_revision>=0),
  expected_source_count integer not null check (expected_source_count>=1),
  expected_component_count integer not null check (expected_component_count>=0),
  expected_payload_bytes integer not null check (expected_payload_bytes>=1),
  source_manifest_sha256 bytea not null check (pg_catalog.octet_length(source_manifest_sha256)=32),
  entitlement_sha256 bytea not null check (pg_catalog.octet_length(entitlement_sha256)=32),
  approval_sha256 bytea not null check (pg_catalog.octet_length(approval_sha256)=32),
  is_zero_entitlement boolean not null,
  financial_row_id uuid not null references public.timesheets_financials(id) on delete restrict,
  request_sha256 bytea not null check (pg_catalog.octet_length(request_sha256)=32),
  state text not null check (state in ('READY','SUBMITTED','PENDING','RECOVERY_REQUIRED','PUBLISHED','REFUSED','FAILED','RETIRED')),
  c1_operation_id uuid,
  c1_publication_id uuid,
  c1_head_revision bigint,
  c1_receipt_sha256 bytea check (c1_receipt_sha256 is null or pg_catalog.octet_length(c1_receipt_sha256)=32),
  typed_result_json jsonb,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  completed_at_utc timestamptz,
  unique (family_id,generation_id),
  unique (request_sha256),
  unique (family_id,request_sequence)
);
alter table public.weekly_exceptional_c1_publication_requests owner to postgres;

create table public.weekly_exceptional_c1_source_records (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  publication_request_id uuid not null references public.weekly_exceptional_c1_publication_requests(id) on delete restrict,
  source_ordinal integer not null check (source_ordinal>=1),
  -- WB-007, WB-013, 24 section 5 (S7): an adjustment is never copied into a
  -- head.  An ordinary non-advance ts_pay_adjustments occurrence stays
  -- independently owned and is composed exactly once by the Workbench selector,
  -- outside the entitlement.  'NONADVANCE_ADJUSTMENT' is therefore not a legal
  -- authority kind here.
  authority_kind text not null check (authority_kind in ('CANDIDATE_SUBMISSION','CLIENT_SOURCE','OFFICE_APPROVAL','ROOT_FINANCIAL','PROVIDER','SOURCE_EXPENSE','ORDINARY_EXPENSE','APPROVED_COMPONENT')),
  source_system text not null,
  external_identity text not null,
  external_revision text not null,
  source_document_sha256 bytea not null check (pg_catalog.octet_length(source_document_sha256)=32),
  payload_bytes integer not null check (payload_bytes between 1 and 16384),
  part_count integer not null check (part_count between 1 and 6),
  work_date date,
  root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  source_row_sha256 bytea not null check (pg_catalog.octet_length(source_row_sha256)=32),
  unique (publication_request_id,source_ordinal),
  unique (publication_request_id,id)
);
alter table public.weekly_exceptional_c1_source_records owner to postgres;

create table public.weekly_exceptional_c1_source_parts (
  publication_request_id uuid not null references public.weekly_exceptional_c1_publication_requests(id) on delete restrict,
  source_record_id uuid not null references public.weekly_exceptional_c1_source_records(id) on delete restrict,
  source_ordinal integer not null check (source_ordinal>=1),
  part_ordinal integer not null check (part_ordinal between 1 and 6),
  payload_utf8 bytea not null check (pg_catalog.octet_length(payload_utf8) between 1 and 3072),
  fragment_sha256 bytea not null check (pg_catalog.octet_length(fragment_sha256)=32),
  primary key (publication_request_id,source_ordinal,part_ordinal)
);
alter table public.weekly_exceptional_c1_source_parts owner to postgres;

create table public.weekly_exceptional_c1_component_records (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  publication_request_id uuid not null references public.weekly_exceptional_c1_publication_requests(id) on delete restrict,
  component_ordinal integer not null check (component_ordinal>=1),
  component_id uuid not null,
  source_ordinal integer not null check (source_ordinal>=1),
  source_key text not null,
  component_kind text not null,
  economic_key_type text not null,
  economic_key_value text not null,
  component_member_identity text not null,
  segment_id text,
  segment_key text,
  segment_stable_key text,
  work_date date,
  reference_number text,
  hours_day numeric(18,6),
  hours_night numeric(18,6),
  hours_sat numeric(18,6),
  hours_sun numeric(18,6),
  hours_bh numeric(18,6),
  additional_code_raw text,
  unit_count numeric(18,6),
  unit_pay_rate numeric(18,6),
  unit_charge_rate numeric(18,6),
  expense_code text,
  -- S7: adjustment_id is deliberately absent.  WB-007, WB-013 and 24 section 5
  -- forbid copying an adjustment into an immutable head, because an adjustment
  -- created later would make the snapshot stale and create a second owner.
  pay_ex_vat numeric(18,2) not null,
  charge_ex_vat numeric(18,2),
  exclude_from_pay boolean not null,
  origin text not null,
  component_sha256 bytea not null check (pg_catalog.octet_length(component_sha256)=32),
  unique (publication_request_id,component_ordinal),
  unique (publication_request_id,component_id)
);
alter table public.weekly_exceptional_c1_component_records owner to postgres;

-- Every committed C1 cursor is appended before another C1 call is allowed.
-- This makes Worker restarts suffix-safe without trusting in-memory progress.
create table public.weekly_exceptional_c1_publication_checkpoints (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  publication_request_id uuid not null references public.weekly_exceptional_c1_publication_requests(id) on delete restrict,
  checkpoint_sequence bigint not null check (checkpoint_sequence>=1),
  phase text not null check (phase in ('START','STAGE','VALIDATE','CERTIFY','PUBLISH')),
  record_offset integer not null check (record_offset>=0),
  next_record_offset integer not null check (next_record_offset>=record_offset),
  result_json jsonb not null check (pg_catalog.jsonb_typeof(result_json)='object'),
  result_sha256 bytea not null check (pg_catalog.octet_length(result_sha256)=32),
  prior_checkpoint_sha256 bytea check (prior_checkpoint_sha256 is null or pg_catalog.octet_length(prior_checkpoint_sha256)=32),
  checkpoint_sha256 bytea not null check (pg_catalog.octet_length(checkpoint_sha256)=32),
  idempotency_key text not null,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (publication_request_id,checkpoint_sequence),
  unique (publication_request_id,checkpoint_sha256),
  unique (idempotency_key),
  check (next_record_offset>=record_offset)
);
alter table public.weekly_exceptional_c1_publication_checkpoints owner to postgres;

-- An unknown transport outcome is an explicit durable stop, never an excuse
-- for an automatic retry.  Recovery records retain the exact C1 envelope.
create table public.weekly_exceptional_c1_unknown_outcomes (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  publication_request_id uuid not null references public.weekly_exceptional_c1_publication_requests(id) on delete restrict,
  unknown_sequence bigint not null check (unknown_sequence>=1),
  phase text not null check (phase in ('START','STAGE','VALIDATE','CERTIFY','PUBLISH')),
  record_offset integer not null check (record_offset>=0),
  next_record_offset integer not null check (next_record_offset>=record_offset),
  records_submitted integer not null default 0 check (records_submitted>=0),
  error_code text not null,
  recovery_call_json jsonb not null check (pg_catalog.jsonb_typeof(recovery_call_json)='object'),
  recovery_call_sha256 bytea not null check (pg_catalog.octet_length(recovery_call_sha256)=32),
  state text not null check (state in ('RECOVERY_REQUIRED','RECOVERED_COMMITTED','RECOVERED_REPLAYED','REFUSED')),
  recovery_result_json jsonb check (recovery_result_json is null or pg_catalog.jsonb_typeof(recovery_result_json)='object'),
  recovered_at_utc timestamptz,
  idempotency_key text not null,
  recovery_idempotency_key text unique,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (publication_request_id,unknown_sequence),
  unique (publication_request_id,recovery_call_sha256),
  unique (idempotency_key),
  check ((state='RECOVERY_REQUIRED')=(recovery_result_json is null and recovered_at_utc is null)),
  check (next_record_offset>=record_offset),
  check ((phase='STAGE' and records_submitted>=1) or (phase<>'STAGE' and records_submitted=0))
);
alter table public.weekly_exceptional_c1_unknown_outcomes owner to postgres;

-- ---------------------------------------------------------------------------
-- Plan 6.2 Gate 1 - the common current-head interface (24 section 4.3), the
-- bounded decision bundle (24 section 4.5), the pending-publication record
-- (proof/32 section 2) and the frozen publication receipt (proof/32 section 9).
--
-- These relations hold what a worker is owed and the receipt that stops one
-- publication happening twice.  They contain no invoice movement, payment
-- difference, Workbench row, Draft item, correction Timesheet or second
-- settlement ledger (24 section 4.3), and no Banking Pay definition, trigger,
-- column or callback is added anywhere by this migration.
-- ---------------------------------------------------------------------------

-- proof/32 section 9 requires a distinct-array proof inside a CHECK, which
-- cannot contain a subquery.  SELECT DISTINCT treats two NULL elements as
-- equal, so an array holding a NULL is still reported distinct and the
-- separate "no null element" constraint remains the one that fires for it.
create function private.weekly_source_uuid_array_is_distinct_v1(p_values uuid[])
returns boolean
language sql
immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
  select p_values is null
      or pg_catalog.cardinality(p_values) = (
           select pg_catalog.count(*)
           from (select distinct v from pg_catalog.unnest(p_values) as t(v)) d
         );
$function$;
alter function private.weekly_source_uuid_array_is_distinct_v1(uuid[]) owner to postgres;
revoke all on function private.weekly_source_uuid_array_is_distinct_v1(uuid[])
  from public,anon,authenticated,service_role;

-- 24 section 4.5 / H2-024: one immutable bundle identity, one row per bundle
-- revision.  The (decision_bundle_id,bundle_revision) pair is a real key so the
-- post-decision component index below is backed by referential integrity.
create table public.weekly_source_entitlement_decision_bundles (
  decision_bundle_id uuid not null default pg_catalog.gen_random_uuid(),
  bundle_revision bigint not null check (bundle_revision>=1),
  agency_id uuid not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  week_ending_date date not null,
  bundle_kind text not null check (bundle_kind in ('SINGLE_ROOT','CROSS_CONTRACT_A_B')),
  source_root_family_booking_id text not null check (pg_catalog.char_length(source_root_family_booking_id) between 1 and 200),
  source_root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  source_contract_id uuid not null references public.contracts(id) on delete restrict,
  target_root_family_booking_id text check (target_root_family_booking_id is null or pg_catalog.char_length(target_root_family_booking_id) between 1 and 200),
  target_root_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  target_contract_id uuid references public.contracts(id) on delete restrict,
  decision_id uuid not null,
  decided_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  publication_mode text not null check (publication_mode in ('IMMEDIATE','DEFERRED')),
  request_digest bytea not null check (pg_catalog.octet_length(request_digest)=32),
  source_revision_digest bytea not null check (pg_catalog.octet_length(source_revision_digest)=32),
  contract_choice_digest bytea not null check (pg_catalog.octet_length(contract_choice_digest)=32),
  before_inventory_digest bytea not null check (pg_catalog.octet_length(before_inventory_digest)=32),
  proposed_head_ids uuid[] not null,
  -- 24 section 4.5 step 4, file 26 Gate 5 step 6, 27 section 8 step 6, H2-024:
  -- "If B already exists unauthorised and contains unrelated content, the
  -- bundle must either receive explicit whole-root Office review of that
  -- content or block; it must never silently authorise B."
  --
  -- These three are IDENTITY columns, not lifecycle columns: the review is part
  -- of what Office ACCEPTED, so it must be true when the bundle revision is
  -- written and must never change afterwards.  Were they writable later, a
  -- caller could attach the very approval it needs to a bundle it is about to
  -- publish, which is the failure the review exists to prevent, inverted.  A
  -- review taken after a bundle was proposed is a NEW bundle revision, exactly
  -- as a changed request digest is.  The ACL immutable-fact guard therefore
  -- refuses any update to them, and they are deliberately NOT registered as
  -- lifecycle columns in the ACL closure.
  whole_root_review_required boolean not null default false,
  whole_root_reviewed_by_user_id uuid references public.tms_users(id) on delete restrict,
  whole_root_reviewed_at_utc timestamptz,
  state text not null check (state in ('PROPOSED','COMMITTED','SUPERSEDED','ABANDONED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  committed_at_utc timestamptz,
  superseded_at_utc timestamptz,
  -- A recorded review names a reviewer and a time, or it is not a review.
  check (whole_root_review_required=(whole_root_reviewed_by_user_id is not null)),
  check ((whole_root_reviewed_by_user_id is null)=(whole_root_reviewed_at_utc is null)),
  -- The whole-root review is about the TARGET root, so only a cross-Contract
  -- bundle can carry one.
  check (not whole_root_review_required or bundle_kind='CROSS_CONTRACT_A_B'),
  check ((bundle_kind='CROSS_CONTRACT_A_B')=(target_root_family_booking_id is not null and target_contract_id is not null)),
  check (bundle_kind<>'CROSS_CONTRACT_A_B' or target_contract_id is distinct from source_contract_id),
  check (pg_catalog.cardinality(proposed_head_ids)>=1),
  check (pg_catalog.array_position(proposed_head_ids,null::uuid) is null),
  check (private.weekly_source_uuid_array_is_distinct_v1(proposed_head_ids)),
  check ((state='COMMITTED')=(committed_at_utc is not null)),
  check ((state='SUPERSEDED')=(superseded_at_utc is not null)),
  primary key (decision_bundle_id,bundle_revision),
  unique (request_digest)
);
alter table public.weekly_source_entitlement_decision_bundles owner to postgres;
create unique index weekly_source_entitlement_decision_bundles_committed_uq
  on public.weekly_source_entitlement_decision_bundles(decision_bundle_id)
  where state='COMMITTED';
create unique index weekly_source_entitlement_decision_bundles_proposed_uq
  on public.weekly_source_entitlement_decision_bundles(decision_bundle_id)
  where state='PROPOSED';
create index weekly_source_entitlement_decision_bundles_candidate_idx
  on public.weekly_source_entitlement_decision_bundles(candidate_id,week_ending_date,state,decision_bundle_id);
-- Every bundle that needed an explicit whole-root Office review, and who gave
-- it, so the act can be shown after the fact - which is the whole point of
-- persisting it rather than trusting a caller's flag.
create index weekly_source_entitlement_decision_bundles_review_idx
  on public.weekly_source_entitlement_decision_bundles(
       whole_root_reviewed_by_user_id,decision_bundle_id,bundle_revision)
  where whole_root_review_required;

-- 24 section 4.3: one complete current entitlement head for the immutable
-- ordinary root, supporting PROTECTED and LOCKED_FINAL_SOURCE through one
-- common interface; at most one committed current head per root across both
-- authority kinds.  WB-009: certified zero is an explicit head, never an
-- absent one.  27 section 5.2: every fingerprint fact here is fixed size.
create table public.weekly_source_entitlement_heads (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  authority_kind text not null check (authority_kind in ('PROTECTED','LOCKED_FINAL_SOURCE')),
  agency_id uuid not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  week_ending_date date not null,
  root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  root_family_booking_id text not null check (pg_catalog.char_length(root_family_booking_id) between 1 and 200),
  root_timesheet_version integer not null check (root_timesheet_version>=1),
  head_revision bigint not null check (head_revision>=1),
  prior_head_id uuid,
  state text not null check (state in ('STAGED','COMMITTED_CURRENT','SUPERSEDED')),
  certified_zero boolean not null,
  component_count integer not null check (component_count>=0),
  entitlement_digest bytea not null check (pg_catalog.octet_length(entitlement_digest)=32),
  inventory_digest bytea not null check (pg_catalog.octet_length(inventory_digest)=32),
  source_generation_digest bytea not null check (pg_catalog.octet_length(source_generation_digest)=32),
  publication_receipt_digest bytea check (publication_receipt_digest is null or pg_catalog.octet_length(publication_receipt_digest)=32),
  -- Every head originates from an accepted Office decision bundle, immediate or
  -- deferred: proof/32 section 9 makes decision_bundle_id NOT NULL on the
  -- receipt for both modes.  Keeping the pair mandatory here is what lets the
  -- H2-024 index on the component relation bind at all (a nullable tag never
  -- conflicts in a B-tree unique index).
  decision_bundle_id uuid not null,
  bundle_revision bigint not null,
  decision_id uuid not null,
  decided_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  scope_change_tx_token uuid,
  staged_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  committed_at_utc timestamptz,
  superseded_at_utc timestamptz,
  superseded_by_head_id uuid,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (certified_zero=(component_count=0)),
  check ((head_revision=1)=(prior_head_id is null)),
  check ((state='STAGED')=(committed_at_utc is null and superseded_at_utc is null)),
  check ((state='COMMITTED_CURRENT')=(committed_at_utc is not null and superseded_at_utc is null)),
  check ((state='SUPERSEDED')=(superseded_at_utc is not null)),
  check ((superseded_at_utc is null)=(superseded_by_head_id is null)),
  check (superseded_by_head_id is distinct from id),
  check (committed_at_utc is null or committed_at_utc>=staged_at_utc),
  check (superseded_at_utc is null or superseded_at_utc>=staged_at_utc),
  check ((publication_receipt_digest is not null)=(committed_at_utc is not null)),
  -- 24 section 4.5 step 6 and 27 section 4 step 5 require the one aligned
  -- invalidation to have succeeded before the head becomes current, so a
  -- committed head always carries its token and a staged one never does.
  check ((scope_change_tx_token is not null)=(committed_at_utc is not null)),
  unique (id,decision_bundle_id,bundle_revision)
);
alter table public.weekly_source_entitlement_heads owner to postgres;
-- The root identity is the Timesheet FAMILY, keyed on the trimmed booking id:
-- the grouping proof/32 section 6 locks first, so a whitespace-padded
-- booking_id can never present itself as a second root.  It is written as an
-- expression index rather than a stored generated column on purpose: a
-- generated column is NULL in NEW inside a BEFORE trigger, which would make the
-- ACL closure's IMMUTABLE_FACTS_WITH_LIFECYCLE allowlist guard refuse every
-- legitimate activation update.
create unique index weekly_source_entitlement_heads_family_revision_uq
  on public.weekly_source_entitlement_heads(pg_catalog.btrim(root_family_booking_id),head_revision);
alter table public.weekly_source_entitlement_heads
  add constraint weekly_source_entitlement_heads_prior_fk
  foreign key (prior_head_id) references public.weekly_source_entitlement_heads(id) on delete restrict;
alter table public.weekly_source_entitlement_heads
  add constraint weekly_source_entitlement_heads_superseded_by_fk
  foreign key (superseded_by_head_id) references public.weekly_source_entitlement_heads(id) on delete restrict;
alter table public.weekly_source_entitlement_heads
  add constraint weekly_source_entitlement_heads_bundle_fk
  foreign key (decision_bundle_id,bundle_revision)
  references public.weekly_source_entitlement_decision_bundles(decision_bundle_id,bundle_revision) on delete restrict;
-- 24 section 4.3: "One root has at most one committed current head across both
-- authority kinds."  authority_kind is deliberately absent from both keys.
--
-- The rule binds on BOTH identities of the root, because either one alone can
-- be defeated.  The family key catches a whitespace-padded booking_id; the
-- PHYSICAL key catches a head whose family string drifts from the Timesheet it
-- names.  The installed resolver public._pay_timesheet_rotation_scope joins the
-- family with an exact, case-sensitive "=" on the raw booking_id, and
-- timesheets.booking_id is plain text with a case-sensitive unique index, so
-- 'REV-0001' and 'rev-0001' are two DIFFERENT families to the installed owners.
-- Folding case here would merge two real families; the physical key plus the
-- write-time identity guard below is the correct answer instead.
create unique index weekly_source_entitlement_heads_committed_current_uq
  on public.weekly_source_entitlement_heads(pg_catalog.btrim(root_family_booking_id))
  where state='COMMITTED_CURRENT';
create unique index weekly_source_entitlement_heads_committed_root_uq
  on public.weekly_source_entitlement_heads(root_timesheet_id)
  where state='COMMITTED_CURRENT';
create index weekly_source_entitlement_heads_selector_idx
  on public.weekly_source_entitlement_heads(candidate_id,root_timesheet_id,state,id);
create index weekly_source_entitlement_heads_bundle_idx
  on public.weekly_source_entitlement_heads(decision_bundle_id,bundle_revision,id);
-- WP-10 review finding F3.  The Workbench selector's STAGED probe in
-- 04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql reads
--     select 1 from public.weekly_source_entitlement_heads head_row
--      where head_row.root_timesheet_id = <the projected Timesheet>
--        and head_row.state = 'STAGED'
-- for EVERY ordinary Timesheet the Workbench pages.  The committed probe just
-- above it is served by …_committed_root_uq, but nothing led with
-- root_timesheet_id for the staged one: …_selector_idx leads with candidate_id,
-- which that query does not restrict, so the planner had only a sequential scan
-- over a relation that only ever grows.  DO NOT REMOVE THIS AS UNUSED: it is
-- read by that probe, not by any writer in this file.
--
-- It is a PERFORMANCE structure and deliberately NOT unique.  After schema
-- change S8 nothing may be keyed on the physical root id alone for correctness,
-- and more than one staged head per physical root is exactly the state file 26
-- Gate 4's "multiple, staged or broken heads fail closed" exists to catch — a
-- unique index here would turn that detectable condition into a write failure
-- somewhere else.  It is partial on the probe's own predicate, so it holds only
-- the transient staged rows and not the committed history.
create index weekly_source_entitlement_heads_staged_root_idx
  on public.weekly_source_entitlement_heads(root_timesheet_id)
  where state='STAGED';

-- A managed root never rotates after first authorisation (proof/34 section 4,
-- section 6), so the head's stored family identity and version must equal the
-- physical root Timesheet's own booking_id and version at write time.  Without
-- this the family key and the lock key of proof/32 section 6 can name different
-- families and two coordinators can hold disjoint locks for one physical root.
create function private.weekly_source_entitlement_head_root_identity_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_booking_id text;
  v_version integer;
begin
  select timesheet_row.booking_id,timesheet_row.version
    into v_booking_id,v_version
  from public.timesheets timesheet_row
  where timesheet_row.timesheet_id=new.root_timesheet_id;

  if not found
     or v_booking_id is distinct from new.root_family_booking_id
     or v_version is distinct from new.root_timesheet_version then
    raise exception 'WEEKLY_SOURCE_HEAD_ROOT_IDENTITY_MISMATCH'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_HEAD_ROOT_IDENTITY_MISMATCH',
              'root_timesheet_id',new.root_timesheet_id,
              'stored_family_booking_id',new.root_family_booking_id,
              'stored_timesheet_version',new.root_timesheet_version
            )::text;
  end if;
  return new;
end;
$function$;
alter function private.weekly_source_entitlement_head_root_identity_v1() owner to postgres;
revoke all on function private.weekly_source_entitlement_head_root_identity_v1()
  from public,anon,authenticated,service_role;
create trigger weekly_source_entitlement_head_root_identity
  before insert or update on public.weekly_source_entitlement_heads
  for each row execute function private.weekly_source_entitlement_head_root_identity_v1();

-- WB-005: the complete record is built through an explicit schema allowlist.
-- adjustment_id is deliberately absent - ordinary non-advance ts_pay_adjustments
-- occurrences stay independently owned and are never copied into a head
-- (WB-007, WB-013).  No invoice movement, source-cycle, upload, query,
-- response, finalisation or Office-notification field appears here (WB-014).
create table public.weekly_source_entitlement_head_components (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  head_id uuid not null references public.weekly_source_entitlement_heads(id) on delete restrict,
  component_ordinal integer not null check (component_ordinal>=1),
  component_id uuid not null,
  component_kind text not null,
  economic_key_type text not null,
  economic_key_value text not null,
  component_member_identity text not null,
  segment_id text,
  segment_key text,
  segment_stable_key text,
  work_date date,
  reference_number text,
  hours_day numeric(18,6),
  hours_night numeric(18,6),
  hours_sat numeric(18,6),
  hours_sun numeric(18,6),
  hours_bh numeric(18,6),
  additional_code_raw text,
  unit_count numeric(18,6),
  unit_pay_rate numeric(18,6),
  unit_charge_rate numeric(18,6),
  expense_code text,
  pay_ex_vat numeric(18,2) not null,
  charge_ex_vat numeric(18,2),
  exclude_from_pay boolean not null,
  origin text not null,
  -- Mandatory, and mandatory to equal the owning head's own pair: the H2-024
  -- index is a B-tree unique index, and a NULL key never conflicts, so an
  -- optional bundle tag would let the same component be retained in A and added
  -- to B simply by omitting the tag on one of the two rows.
  decision_bundle_id uuid not null,
  bundle_revision bigint not null,
  movement_id uuid,
  movement_group_id uuid,
  component_sha256 bytea not null check (pg_catalog.octet_length(component_sha256)=32),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check (movement_group_id is null or movement_id is not null),
  unique (head_id,component_ordinal),
  unique (head_id,component_id)
);
alter table public.weekly_source_entitlement_head_components owner to postgres;
alter table public.weekly_source_entitlement_head_components
  add constraint weekly_source_entitlement_head_components_bundle_fk
  foreign key (decision_bundle_id,bundle_revision)
  references public.weekly_source_entitlement_decision_bundles(decision_bundle_id,bundle_revision) on delete restrict;
alter table public.weekly_source_entitlement_head_components
  add constraint weekly_source_entitlement_head_components_head_bundle_fk
  foreign key (head_id,decision_bundle_id,bundle_revision)
  references public.weekly_source_entitlement_heads(id,decision_bundle_id,bundle_revision) on delete restrict;
-- 24 section 4.5 step 3 and H2-024, verbatim: a unique index over
-- (decision_bundle_id,bundle_revision,component_id) on the post-decision head
-- components, never a CHECK, because a CHECK cannot express a cross-row rule.
-- It makes it impossible for the same shift or source-fixed expense to be
-- retained in A and added to B within one bundle revision.  It is plain and
-- NON-partial, and NULLS NOT DISTINCT so the rule survives even if a later
-- package ever relaxes the NOT NULL above.
create unique index weekly_source_entitlement_head_components_bundle_component_uq
  on public.weekly_source_entitlement_head_components(decision_bundle_id,bundle_revision,component_id)
  nulls not distinct;
-- Every moved component lands in exactly one destination head.
create unique index weekly_source_entitlement_head_components_bundle_movement_uq
  on public.weekly_source_entitlement_head_components(decision_bundle_id,bundle_revision,movement_id)
  where movement_id is not null;
-- movement_group_id groups components that move together and is deliberately
-- NON-unique (24 section 4.5 step 3).
create index weekly_source_entitlement_head_components_movement_group_idx
  on public.weekly_source_entitlement_head_components(decision_bundle_id,bundle_revision,movement_group_id)
  where movement_group_id is not null;
create index weekly_source_entitlement_head_components_head_idx
  on public.weekly_source_entitlement_head_components(head_id,component_ordinal);

-- WB-009 makes certified zero a selection authority and 27 section 5.2 puts
-- component_count and the certified-zero flag into the Workbench fingerprint,
-- so neither may be a bare writer assertion.  A row CHECK cannot count rows in
-- another relation; a DEFERRABLE INITIALLY DEFERRED constraint trigger can, and
-- it runs at commit, after the head and its complete component inventory have
-- both been written in the one transaction 24 section 4.5 step 5 requires.
create function private.weekly_source_entitlement_head_inventory_assert_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_head_id uuid;
  v_component_count integer;
  v_certified_zero boolean;
  v_actual integer;
begin
  if tg_relid='public.weekly_source_entitlement_heads'::pg_catalog.regclass then
    v_head_id:=new.id;
  else
    v_head_id:=new.head_id;
  end if;

  select head_row.component_count,head_row.certified_zero
    into v_component_count,v_certified_zero
  from public.weekly_source_entitlement_heads head_row
  where head_row.id=v_head_id;
  if not found then
    return null;
  end if;

  select pg_catalog.count(*) into v_actual
  from public.weekly_source_entitlement_head_components component_row
  where component_row.head_id=v_head_id;

  if v_actual<>v_component_count or v_certified_zero<>(v_actual=0) then
    raise exception 'WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH',
              'head_id',v_head_id,
              'declared_component_count',v_component_count,
              'declared_certified_zero',v_certified_zero,
              'actual_component_count',v_actual
            )::text;
  end if;
  return null;
end;
$function$;
alter function private.weekly_source_entitlement_head_inventory_assert_v1() owner to postgres;
revoke all on function private.weekly_source_entitlement_head_inventory_assert_v1()
  from public,anon,authenticated,service_role;
create constraint trigger weekly_source_entitlement_head_inventory_assert
  after insert or update on public.weekly_source_entitlement_heads
  deferrable initially deferred
  for each row execute function private.weekly_source_entitlement_head_inventory_assert_v1();
create constraint trigger weekly_source_entitlement_head_component_inventory_assert
  after insert on public.weekly_source_entitlement_head_components
  deferrable initially deferred
  for each row execute function private.weekly_source_entitlement_head_inventory_assert_v1();

-- Decision D8 (orchestrator, 17 September 2026), from proof/34 section 4's
-- write-set table: the Weekly Source authorisation record is per ROOT, not per
-- source row, and generation 1 is inserted by the first-authorisation owner
-- AFTER the ordinary Authorise succeeds.  The per-source-row binding
-- (public.weekly_source_row_timesheet_lineages) is written by the ensure owner
-- at projection and finalisation time, long before any Office decision, so it
-- cannot carry the generation: a managed-root guard reading it would report a
-- never-authorised Timesheet as managed, refuse the very write that first
-- authorisation makes, and collide with the first-authorisation owner's own
-- generation 1 (WP-03 review F3).
--
-- Immutable once written, except: current_entitlement_head_id, written only by
-- the head-publication coordinator; and withdrawn_at_utc with
-- withdrawn_by_user_id, written ONCE by the withdrawal owner, which clears the
-- head pointer in the same statement.  Re-authorisation appends generation N+1;
-- generation N stays as history.  Deletion is forbidden.
create table public.weekly_source_root_authorisations (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  family_booking_id text not null check (pg_catalog.char_length(family_booking_id) between 1 and 200),
  timesheet_version integer not null check (timesheet_version>=1),
  authorisation_generation integer not null check (authorisation_generation>=1),
  authorised_row_signature text not null check (pg_catalog.char_length(authorised_row_signature) between 1 and 512),
  current_entitlement_head_id uuid,
  authorised_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  authorised_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  withdrawn_at_utc timestamptz,
  withdrawn_by_user_id uuid references public.tms_users(id) on delete restrict,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check ((withdrawn_at_utc is null)=(withdrawn_by_user_id is null)),
  -- proof/36 section 5.6: the withdrawal owner clears the head pointer, so a
  -- withdrawn generation can never keep a live entitlement head.
  check (withdrawn_at_utc is null or current_entitlement_head_id is null),
  check (withdrawn_at_utc is null or withdrawn_at_utc>=authorised_at_utc),
  unique (root_timesheet_id,authorisation_generation)
);
alter table public.weekly_source_root_authorisations owner to postgres;
-- At most one live (not withdrawn) generation per physical root.
create unique index weekly_source_root_authorisations_live_uq
  on public.weekly_source_root_authorisations(root_timesheet_id)
  where withdrawn_at_utc is null;
create index weekly_source_root_authorisations_family_idx
  on public.weekly_source_root_authorisations(family_booking_id,timesheet_version,authorisation_generation,id);
create index weekly_source_root_authorisations_head_idx
  on public.weekly_source_root_authorisations(current_entitlement_head_id)
  where current_entitlement_head_id is not null;

-- The same insert-time identity guard the entitlement head carries: a managed
-- root never rotates after first authorisation (proof/34 section 4, section 6),
-- so the stored family identity and version must be the physical root
-- Timesheet's own booking_id and version at write time.
create function private.weekly_source_root_authorisation_identity_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_booking_id text;
  v_version integer;
begin
  select timesheet_row.booking_id,timesheet_row.version
    into v_booking_id,v_version
  from public.timesheets timesheet_row
  where timesheet_row.timesheet_id=new.root_timesheet_id;

  if not found
     or v_booking_id is distinct from new.family_booking_id
     or v_version is distinct from new.timesheet_version then
    raise exception 'WEEKLY_SOURCE_ROOT_AUTHORISATION_IDENTITY_MISMATCH'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_ROOT_AUTHORISATION_IDENTITY_MISMATCH',
              'root_timesheet_id',new.root_timesheet_id,
              'stored_family_booking_id',new.family_booking_id,
              'stored_timesheet_version',new.timesheet_version
            )::text;
  end if;
  return new;
end;
$function$;
alter function private.weekly_source_root_authorisation_identity_v1() owner to postgres;
revoke all on function private.weekly_source_root_authorisation_identity_v1()
  from public,anon,authenticated,service_role;
create trigger weekly_source_root_authorisation_identity
  before insert or update on public.weekly_source_root_authorisations
  for each row execute function private.weekly_source_root_authorisation_identity_v1();

-- A withdrawal is permanent (proof/36 section 6).  The ACL closure's lifecycle
-- allowlist permits the two withdrawal columns to be written, but it cannot
-- express "once".  Clearing or rewriting them would bring a withdrawn
-- generation back to life and make the root managed again (WP-03 review F12).
create function private.weekly_source_root_authorisation_withdrawal_once_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if old.withdrawn_at_utc is not null
     and (new.withdrawn_at_utc is distinct from old.withdrawn_at_utc
          or new.withdrawn_by_user_id is distinct from old.withdrawn_by_user_id) then
    raise exception 'WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWAL_IMMUTABLE'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWAL_IMMUTABLE',
              'root_authorisation_id',old.id
            )::text;
  end if;
  -- A withdrawn generation never receives an entitlement head again.
  if old.withdrawn_at_utc is not null
     and new.current_entitlement_head_id is not null then
    raise exception 'WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWN'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWN',
              'root_authorisation_id',old.id
            )::text;
  end if;
  return new;
end;
$function$;
alter function private.weekly_source_root_authorisation_withdrawal_once_v1() owner to postgres;
revoke all on function private.weekly_source_root_authorisation_withdrawal_once_v1()
  from public,anon,authenticated,service_role;
create trigger weekly_source_root_authorisation_withdrawal_once
  before update on public.weekly_source_root_authorisations
  for each row execute function private.weekly_source_root_authorisation_withdrawal_once_v1();

-- proof/32 section 2 "Pending bundle record".  Office may record its decision
-- while a Draft or later payment state freezes the root; the decision is saved
-- as pending and the previous effective entitlement stays current until the
-- existing cancellation-safe or complete-settlement release condition is
-- observed by the Weekly-Source-owned release owner (24 section 4.4).
create table public.weekly_source_pending_entitlement_bundles (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  decision_bundle_id uuid not null,
  bundle_revision bigint not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  member_root_ids uuid[] not null,
  member_family_booking_ids text[] not null,
  member_root_versions integer[] not null,
  request_digest bytea not null check (pg_catalog.octet_length(request_digest)=32),
  source_revision_digest bytea not null check (pg_catalog.octet_length(source_revision_digest)=32),
  contract_choice_digest bytea not null check (pg_catalog.octet_length(contract_choice_digest)=32),
  decision_id uuid not null,
  decided_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  proposed_head_ids uuid[] not null,
  -- proof/32 section 8: the deferred release "calls the SAME atomic
  -- head-publication coordinator used for immediate publication ... It does not
  -- reimplement it", and the section 2 apply signature receives no request from
  -- its caller, so the exact request the coordinator will be handed has to live
  -- on the bundle.  The three digests above prove a request has not changed;
  -- they cannot reconstitute one.  Nothing else stores it either: a frozen
  -- decision publishes no head at all (24 section 4.4), so the proposed head
  -- components exist nowhere until the release transaction stages them.  Without
  -- this column a saved bundle could never be released (WP-08b_NEEDS N1).
  --
  -- NOT NULL because a pending bundle without its request is not a deferred
  -- decision, only an unreleasable row: every path that reads the bundle needs
  -- it, and there is no legitimate bundle that lacks it.  No backfill question
  -- arises - decision D3 edits these migrations in place and zero Weekly Source
  -- objects are installed anywhere, so no row exists to carry a default.
  --
  -- This is an IDENTITY column, not a lifecycle one.  It is deliberately absent
  -- from the sixteen lifecycle columns registered in
  -- supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql, so the
  -- IMMUTABLE_FACTS_WITH_LIFECYCLE fact guard refuses every rewrite of it with
  -- WEEKLY_SOURCE_IMMUTABLE_FACT.  A changed request is a new bundle_revision
  -- and a new row, never an edit of this one.
  --
  -- The CHECK is a SHAPE rule and nothing more: it says the stored value is a
  -- JSON object.  Every money predicate over the request - the member set, the
  -- four approval digests, the complete entitlement vector - is proved by the
  -- one canonical encoder and by the uniqueness rules above and on the accepted
  -- decision, never by a CHECK, which cannot express a cross-row rule
  -- (24 section 4.5 step 3; WP-01a's discipline).
  request_json jsonb not null check (pg_catalog.jsonb_typeof(request_json)='object'),
  pending_revision bigint not null check (pending_revision>=1),
  state text not null check (state in ('PENDING','RELEASING','RELEASED','SUPERSEDED','MANUAL_REVIEW')),
  lease_owner text,
  lease_token uuid,
  lease_worker_run_id uuid,
  lease_expires_at_utc timestamptz,
  next_check_at_utc timestamptz,
  -- proof/32 section 10 gives ten consecutive technical failures as the
  -- MANUAL_REVIEW threshold, not as a storage limit.  An upper bound here would
  -- turn an eleventh increment into 23514 instead of a transition, so only the
  -- floor is enforced and the threshold belongs to the release owner.
  technical_failure_count integer not null default 0 check (technical_failure_count>=0),
  manual_review_reason text,
  last_census_json jsonb check (last_census_json is null or pg_catalog.jsonb_typeof(last_census_json)='object'),
  -- proof/32 section 8 step 5 names four facts to record on release: the
  -- receipt id, released_at_utc, released_by_worker_id and the verified
  -- released_by_worker_run_id.  All four have their own column; the digest is
  -- kept as well because section 8 step 1 replays by digest.
  released_receipt_id uuid,
  released_receipt_digest bytea check (released_receipt_digest is null or pg_catalog.octet_length(released_receipt_digest)=32),
  released_by_worker_id text,
  released_by_worker_run_id uuid,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  released_at_utc timestamptz,
  check (pg_catalog.cardinality(member_root_ids)>=1),
  check (pg_catalog.cardinality(member_root_ids)=pg_catalog.cardinality(member_family_booking_ids)),
  check (pg_catalog.cardinality(member_root_ids)=pg_catalog.cardinality(member_root_versions)),
  check (pg_catalog.array_position(member_root_ids,null::uuid) is null),
  check (pg_catalog.array_position(member_family_booking_ids,null::text) is null),
  check (pg_catalog.array_position(member_root_versions,null::integer) is null),
  check (private.weekly_source_uuid_array_is_distinct_v1(member_root_ids)),
  check (pg_catalog.cardinality(proposed_head_ids)>=1),
  check (pg_catalog.array_position(proposed_head_ids,null::uuid) is null),
  check (private.weekly_source_uuid_array_is_distinct_v1(proposed_head_ids)),
  check (state<>'RELEASING' or (lease_owner is not null and lease_token is not null and lease_worker_run_id is not null and lease_expires_at_utc is not null)),
  check (state<>'PENDING' or next_check_at_utc is not null),
  check ((state='MANUAL_REVIEW')=(manual_review_reason is not null)),
  check ((state='RELEASED')=(released_at_utc is not null and released_receipt_id is not null and released_receipt_digest is not null and released_by_worker_id is not null and released_by_worker_run_id is not null)),
  unique (request_digest),
  unique (decision_bundle_id,bundle_revision)
);
alter table public.weekly_source_pending_entitlement_bundles owner to postgres;
alter table public.weekly_source_pending_entitlement_bundles
  add constraint weekly_source_pending_entitlement_bundles_bundle_fk
  foreign key (decision_bundle_id,bundle_revision)
  references public.weekly_source_entitlement_decision_bundles(decision_bundle_id,bundle_revision) on delete restrict;
create unique index weekly_source_pending_entitlement_bundles_live_uq
  on public.weekly_source_pending_entitlement_bundles(decision_bundle_id)
  where state in ('PENDING','RELEASING');
create index weekly_source_pending_entitlement_bundles_claim_idx
  on public.weekly_source_pending_entitlement_bundles(state,next_check_at_utc,id);
create index weekly_source_pending_entitlement_bundles_lease_idx
  on public.weekly_source_pending_entitlement_bundles(state,lease_expires_at_utc,id);

-- proof/34 section 4: the current entitlement head is written on the current
-- ROOT AUTHORISATION generation by the head-publication coordinator only, and
-- cleared by the withdrawal owner (proof/36 section 5.6).  Decision D8 moved
-- this pointer off the per-source-row binding.
alter table public.weekly_source_root_authorisations
  add constraint weekly_source_root_authorisations_current_head_fk
  foreign key (current_entitlement_head_id) references public.weekly_source_entitlement_heads(id) on delete restrict;

do $weekly_source_rls$
declare
  v_table text;
begin
  for v_table in
    select c.relname
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public'
      and c.relkind='r'
      and (
        c.relname like 'weekly\_source\_%' escape '\'
        or c.relname like 'weekly\_exceptional\_%' escape '\'
        or c.relname like 'weekly\_candidate\_%' escape '\'
        or c.relname like 'weekly\_manager\_%' escape '\'
        or c.relname like 'weekly\_timesheet\_%' escape '\'
        or c.relname like 'weekly\_discrepancy\_%' escape '\'
        or c.relname like 'weekly\_issue\_%' escape '\'
        or c.relname like 'weekly\_message\_%' escape '\'
        or c.relname like 'weekly\_completed\_%' escape '\'
        or c.relname like 'weekly\_final\_%' escape '\'
        or c.relname like 'weekly\_route\_%' escape '\'
        or c.relname like 'weekly\_work\_%' escape '\'
        or c.relname like 'weekly\_expense\_%' escape '\'
        or c.relname='office_action_notifications'
      )
  loop
    execute pg_catalog.format('alter table public.%I enable row level security',v_table);
    execute pg_catalog.format('alter table public.%I force row level security',v_table);
    execute pg_catalog.format(
      'create policy cloudtms_miget_service_owner_all on public.%I for all to %I, service_role using (true) with check (true)',
      v_table,current_user
    );
    execute pg_catalog.format('revoke all on public.%I from public,anon,authenticated,service_role',v_table);
    execute pg_catalog.format('grant select,insert,update on public.%I to service_role',v_table);
  end loop;
end;
$weekly_source_rls$;

-- ---------------------------------------------------------------------------
-- proof/32 section 9 (frozen): the one publication receipt for the entitlement
-- head coordinator, immediate and deferred.  It is not the ordinary projection
-- receipt (weekly_source_projection_publications, rejected for this purpose)
-- and not a C1 staging or checkpoint receipt.  Exact replay reads this relation
-- first (proof/32 section 8 step 1), which is what stops a committed release
-- whose response was lost from publishing a second time.
--
-- The private schema is outside the public RLS loop above, so its owner policy,
-- revocations and immutability guards are written explicitly here.  Unlike the
-- public Weekly Source relations, service_role is revoked and is not named in
-- the policy, because proof/32 section 9 requires it; the policy keeps the
-- Miget name cloudtms_miget_service_owner_all.
-- ---------------------------------------------------------------------------
create table private.weekly_source_entitlement_publication_receipts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  decision_bundle_id uuid not null,
  pending_bundle_id uuid,
  bundle_revision bigint not null check (bundle_revision>=1),
  request_digest bytea not null check (pg_catalog.octet_length(request_digest)=32),
  publication_mode text not null check (publication_mode in ('IMMEDIATE','DEFERRED')),
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  member_root_ids uuid[] not null,
  member_family_booking_ids text[] not null,
  member_root_versions integer[] not null,
  head_ids uuid[] not null,
  scope_change_tx_token uuid not null,
  decision_id uuid not null,
  decided_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  released_by_worker_id text,
  released_by_worker_run_id uuid,
  census_json jsonb not null check (pg_catalog.jsonb_typeof(census_json)='object'),
  proof_json jsonb not null check (pg_catalog.jsonb_typeof(proof_json)='object'),
  created_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  check ((publication_mode='DEFERRED')=(pending_bundle_id is not null)),
  check ((publication_mode='DEFERRED')=(released_by_worker_id is not null and released_by_worker_run_id is not null)),
  -- proof/32 section 9: census_json is "the complete per-root census ... not a
  -- summary (an empty object for immediate)" and proof_json is "every section
  -- 5.1/5.2 tuple used ... (an empty object for immediate)".  A deferred
  -- release is the one that had to prove a freeze census, so it may not be
  -- recorded empty.
  check (publication_mode<>'DEFERRED' or (census_json<>'{}'::jsonb and proof_json<>'{}'::jsonb)),
  check (pg_catalog.cardinality(member_root_ids)>=1),
  check (pg_catalog.cardinality(member_root_ids)=pg_catalog.cardinality(member_family_booking_ids)),
  check (pg_catalog.cardinality(member_root_ids)=pg_catalog.cardinality(member_root_versions)),
  check (pg_catalog.cardinality(member_root_ids)=pg_catalog.cardinality(head_ids)),
  check (pg_catalog.array_position(member_root_ids,null::uuid) is null),
  check (pg_catalog.array_position(member_family_booking_ids,null::text) is null),
  check (pg_catalog.array_position(member_root_versions,null::integer) is null),
  check (pg_catalog.array_position(head_ids,null::uuid) is null),
  check (private.weekly_source_uuid_array_is_distinct_v1(member_root_ids)),
  check (private.weekly_source_uuid_array_is_distinct_v1(head_ids)),
  unique (decision_bundle_id,bundle_revision,request_digest),
  unique (request_digest)
);
alter table private.weekly_source_entitlement_publication_receipts owner to postgres;
create index weekly_source_entitlement_publication_receipts_bundle_idx
  on private.weekly_source_entitlement_publication_receipts(decision_bundle_id,bundle_revision,id);
create index weekly_source_entitlement_publication_receipts_pending_idx
  on private.weekly_source_entitlement_publication_receipts(pending_bundle_id,id)
  where pending_bundle_id is not null;

create function private.weekly_source_entitlement_publication_receipt_immutable_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  raise exception 'WEEKLY_SOURCE_PUBLICATION_RECEIPT_IMMUTABLE'
    using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'reason_code','WEEKLY_SOURCE_PUBLICATION_RECEIPT_IMMUTABLE',
            'table_name',tg_table_name,
            'operation',tg_op
          )::text;
end;
$function$;
alter function private.weekly_source_entitlement_publication_receipt_immutable_v1() owner to postgres;
revoke all on function private.weekly_source_entitlement_publication_receipt_immutable_v1()
  from public,anon,authenticated,service_role;

create trigger weekly_source_entitlement_publication_receipt_immutable
  before update or delete on private.weekly_source_entitlement_publication_receipts
  for each row execute function private.weekly_source_entitlement_publication_receipt_immutable_v1();
create trigger weekly_source_entitlement_publication_receipt_truncate_guard
  before truncate on private.weekly_source_entitlement_publication_receipts
  for each statement execute function private.weekly_source_entitlement_publication_receipt_immutable_v1();

alter table private.weekly_source_entitlement_publication_receipts enable row level security;
alter table private.weekly_source_entitlement_publication_receipts force row level security;
revoke all on table private.weekly_source_entitlement_publication_receipts
  from public,anon,authenticated,service_role;
do $weekly_source_receipt_policy$
begin
  execute pg_catalog.format(
    'create policy cloudtms_miget_service_owner_all on private.weekly_source_entitlement_publication_receipts for all to %I using (true) with check (true)',
    current_user
  );
end;
$weekly_source_receipt_policy$;

-- A committed head must be the head a real receipt published, with the same
-- bundle identity and the same single invalidation token (24 section 4.5
-- steps 5 to 7; 27 section 4 steps 4 to 6).  The receipt is appended after the
-- heads activate, inside the same transaction, so this is a DEFERRABLE
-- INITIALLY DEFERRED constraint trigger and not a foreign key.
create function private.weekly_source_entitlement_head_receipt_assert_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if new.publication_receipt_digest is null then
    return null;
  end if;
  if not exists(
    select 1
    from private.weekly_source_entitlement_publication_receipts receipt_row
    where receipt_row.request_digest=new.publication_receipt_digest
      and receipt_row.decision_bundle_id=new.decision_bundle_id
      and receipt_row.bundle_revision=new.bundle_revision
      and receipt_row.scope_change_tx_token=new.scope_change_tx_token
      and new.id=any(receipt_row.head_ids)
  ) then
    raise exception 'WEEKLY_SOURCE_HEAD_RECEIPT_MISSING'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_HEAD_RECEIPT_MISSING',
              'head_id',new.id,
              'decision_bundle_id',new.decision_bundle_id,
              'bundle_revision',new.bundle_revision
            )::text;
  end if;
  return null;
end;
$function$;
alter function private.weekly_source_entitlement_head_receipt_assert_v1() owner to postgres;
revoke all on function private.weekly_source_entitlement_head_receipt_assert_v1()
  from public,anon,authenticated,service_role;
create constraint trigger weekly_source_entitlement_head_receipt_assert
  after insert or update on public.weekly_source_entitlement_heads
  deferrable initially deferred
  for each row execute function private.weekly_source_entitlement_head_receipt_assert_v1();

alter table public.weekly_source_cycles
  add constraint weekly_source_cycles_current_upload_fk
  foreign key (current_complete_upload_id) references public.weekly_source_uploads(id) on delete restrict;
alter table public.weekly_source_cycles
  add constraint weekly_source_cycles_current_final_fk
  foreign key (current_final_revision_id) references public.weekly_source_final_revisions(id) on delete restrict;
alter table public.weekly_source_cycles
  add constraint weekly_source_cycles_current_publication_fk
  foreign key (current_projection_publication_id) references public.weekly_source_projection_publications(id) on delete restrict;
alter table public.weekly_source_report_scopes
  add constraint weekly_source_report_scopes_current_upload_fk
  foreign key (current_complete_upload_id) references public.weekly_source_uploads(id) on delete restrict;
alter table public.weekly_source_report_scopes
  add constraint weekly_source_report_scopes_current_final_fk
  foreign key (current_final_revision_id) references public.weekly_source_final_revisions(id) on delete restrict;
alter table public.weekly_source_report_scopes
  add constraint weekly_source_report_scopes_current_publication_fk
  foreign key (current_projection_publication_id) references public.weekly_source_projection_publications(id) on delete restrict;

commit;
