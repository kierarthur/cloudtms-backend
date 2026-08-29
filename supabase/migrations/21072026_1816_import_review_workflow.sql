-- CloudTMS durable import-review workflow.
-- Additive, rerunnable schema migration. Function bodies are maintained in repeatable SQL.
-- Policy X: this migration does not update timesheets, TSFIN, invoices, pay batches,
-- settlements, remittances, provider records, or any other financial artefact.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '60s';
set local idle_in_transaction_session_timeout = '90s';

-- ---------------------------------------------------------------------------
-- Existing tables: additive workflow and revision metadata.
-- ---------------------------------------------------------------------------

alter table public.hr_imports
  add column if not exists source_file_sha256 text,
  add column if not exists parser_version text,
  add column if not exists revision_group_id uuid,
  add column if not exists revision_no integer,
  add column if not exists supersedes_import_id uuid,
  add column if not exists coverage_mode text,
  add column if not exists coverage_start_date date,
  add column if not exists coverage_end_date date,
  add column if not exists coverage_fingerprint text,
  add column if not exists coverage_locked_at timestamptz,
  add column if not exists coverage_confirmed_by uuid,
  add column if not exists coverage_operation_key text,
  add column if not exists coverage_request_hash text;

alter table public.contracts
  add column if not exists send_ts_queries_to_different_email boolean not null default false,
  add column if not exists ts_queries_alt_email_address text;

alter table public.hr_issue_emails
  add column if not exists contract_id uuid,
  add column if not exists recipient_scope text,
  add column if not exists recipient_scope_key text,
  add column if not exists sent_count integer not null default 0,
  add column if not exists last_successful_delivery_id uuid,
  add column if not exists delivery_history_status text not null default 'LEGACY_UNVERIFIED';

-- New code records PENDING explicitly. Existing rows retain their timestamp and are
-- classified LEGACY_UNVERIFIED; they are never inferred to have provider evidence.
alter table public.hr_issue_emails alter column last_sent_at drop default;
alter table public.hr_issue_emails alter column last_sent_at drop not null;

-- ---------------------------------------------------------------------------
-- Eight new relations approved for the review workflow.
-- ---------------------------------------------------------------------------

create table if not exists public.import_review_scope_clients (
  id uuid primary key default gen_random_uuid(),
  import_id uuid not null references public.hr_imports(id) on delete cascade,
  source_client_key text not null,
  source_display_label text,
  client_id uuid references public.clients(id),
  created_at_utc timestamptz not null default now(),
  created_by_user_id uuid,
  resolved_at_utc timestamptz,
  resolved_by_user_id uuid,
  unique (import_id, source_client_key)
);

create table if not exists public.import_review_scope_candidates (
  id uuid primary key default gen_random_uuid(),
  import_id uuid not null references public.hr_imports(id) on delete cascade,
  source_candidate_key text not null,
  source_display_label text,
  candidate_id uuid references public.candidates(id),
  created_at_utc timestamptz not null default now(),
  created_by_user_id uuid,
  resolved_at_utc timestamptz,
  resolved_by_user_id uuid,
  unique (import_id, source_candidate_key)
);

create table if not exists public.import_review_states (
  import_id uuid primary key references public.hr_imports(id) on delete cascade,
  schema_contract_version text not null default 'IMPORT_REVIEW_DB_V1',
  status text not null default 'STAGED',
  state_version bigint not null default 1,
  preview_generation integer not null default 0,
  preview_fingerprint text,
  ui_state_json jsonb not null default '{}'::jsonb,
  follow_up_status text not null default 'NOT_REQUIRED',
  follow_up_error_code text,
  follow_up_error_message text,
  follow_up_retry_count integer not null default 0,
  last_operation_id uuid,
  created_at_utc timestamptz not null default now(),
  created_by_user_id uuid,
  updated_at_utc timestamptz not null default now(),
  updated_by_user_id uuid,
  last_opened_at_utc timestamptz,
  last_opened_by_user_id uuid,
  abandoned_at_utc timestamptz,
  abandoned_by_user_id uuid,
  abandoned_reason text,
  applied_at_utc timestamptz,
  applied_by_user_id uuid,
  superseded_at_utc timestamptz,
  superseded_by_user_id uuid
);

-- CREATE TABLE IF NOT EXISTS does not add columns to a compatible partial
-- installation.  Keep the contract marker additive and rerunnable as well.
alter table public.import_review_states
  add column if not exists schema_contract_version text not null default 'IMPORT_REVIEW_DB_V1';

create table if not exists public.import_review_decisions (
  action_id text primary key,
  import_id uuid not null references public.import_review_states(import_id) on delete cascade,
  action_kind text not null,
  action_category text not null,
  target_key text not null,
  source_identity text not null,
  hr_row_id uuid references public.hr_rows(id),
  timesheet_id uuid references public.timesheets(timesheet_id),
  shift_id uuid references public.nhsp_shifts(id),
  client_id uuid references public.clients(id),
  candidate_id uuid references public.candidates(id),
  contract_id uuid references public.contracts(id),
  issue_id uuid,
  preview_generation integer not null,
  evidence_fingerprint text not null,
  selectable boolean not null default false,
  default_selected boolean not null default false,
  selected boolean not null default false,
  blocking boolean not null default false,
  requires_reconfirmation boolean not null default false,
  is_current boolean not null default true,
  summary_json jsonb not null default '{}'::jsonb,
  created_at_utc timestamptz not null default now(),
  refreshed_at_utc timestamptz not null default now(),
  selected_at_utc timestamptz,
  selected_by_user_id uuid,
  unique (import_id, action_kind, target_key)
);

create table if not exists public.import_review_events (
  id bigint generated always as identity primary key,
  import_id uuid not null references public.import_review_states(import_id) on delete cascade,
  state_version bigint not null,
  operation_id uuid,
  event_code text not null,
  actor_user_id uuid,
  event_context_json jsonb not null default '{}'::jsonb,
  created_at_utc timestamptz not null default now()
);

create table if not exists public.import_review_daily_timesheet_resolutions (
  id uuid primary key default gen_random_uuid(),
  import_id uuid not null references public.import_review_states(import_id) on delete cascade,
  hr_row_id uuid not null references public.hr_rows(id) on delete cascade,
  resolved_timesheet_id uuid references public.timesheets(timesheet_id),
  resolution_method text not null,
  status text not null default 'CURRENT',
  evidence_fingerprint text not null,
  preview_generation integer not null,
  state_version bigint not null,
  selected_at_utc timestamptz not null default now(),
  selected_by_user_id uuid,
  stale_at_utc timestamptz,
  stale_reason_code text,
  applied_operation_id uuid,
  applied_at_utc timestamptz,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  unique (import_id, hr_row_id)
);

create table if not exists public.hr_issue_email_deliveries (
  id uuid primary key default gen_random_uuid(),
  import_id uuid not null references public.import_review_states(import_id) on delete restrict,
  operation_id uuid not null references public.import_apply_operations(id) on delete restrict,
  recipient_scope text not null,
  recipient_scope_key text not null,
  recipient_route_fingerprint text not null,
  recipient_email text not null,
  reminder_sequence integer not null default 0,
  issue_set_fingerprint text not null,
  deterministic_outbox_key text not null,
  mail_outbox_id uuid references public.mail_outbox(id) on delete restrict,
  status text not null default 'QUEUED',
  provider_message_id text,
  provider_status text,
  accepted_at_utc timestamptz,
  marked_at_utc timestamptz,
  created_at_utc timestamptz not null default now(),
  created_by_user_id uuid,
  updated_at_utc timestamptz not null default now(),
  unique (deterministic_outbox_key),
  unique (operation_id, recipient_scope_key, issue_set_fingerprint, reminder_sequence)
);

create table if not exists public.hr_issue_email_delivery_items (
  id uuid primary key default gen_random_uuid(),
  delivery_id uuid not null references public.hr_issue_email_deliveries(id) on delete cascade,
  issue_id uuid not null references public.hr_issue_emails(id) on delete restrict,
  action_id text not null references public.import_review_decisions(action_id) on delete restrict,
  issue_fingerprint text not null,
  marked_sent_at_utc timestamptz,
  created_at_utc timestamptz not null default now(),
  unique (delivery_id, issue_id),
  unique (delivery_id, action_id)
);

-- ---------------------------------------------------------------------------
-- Idempotent constraints on existing and new relations.
-- ---------------------------------------------------------------------------

do $migration_constraints$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.hr_imports'::regclass
      and conname = 'hr_imports_supersedes_import_id_fkey'
  ) then
    alter table public.hr_imports
      add constraint hr_imports_supersedes_import_id_fkey
      foreign key (supersedes_import_id) references public.hr_imports(id) on delete restrict;
  end if;

  if not exists (select 1 from pg_constraint where conrelid='public.hr_imports'::regclass and conname='hr_imports_revision_no_positive_ck') then
    alter table public.hr_imports add constraint hr_imports_revision_no_positive_ck
      check (revision_no is null or revision_no > 0);
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.hr_imports'::regclass and conname='hr_imports_no_self_supersede_ck') then
    alter table public.hr_imports add constraint hr_imports_no_self_supersede_ck
      check (supersedes_import_id is null or supersedes_import_id <> id);
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.hr_imports'::regclass and conname='hr_imports_coverage_mode_ck') then
    alter table public.hr_imports add constraint hr_imports_coverage_mode_ck
      check (coverage_mode is null or coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES','PARTIAL'));
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.hr_imports'::regclass and conname='hr_imports_coverage_dates_ck') then
    alter table public.hr_imports add constraint hr_imports_coverage_dates_ck
      check (coverage_start_date is null or coverage_end_date is null or coverage_start_date <= coverage_end_date);
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.hr_imports'::regclass and conname='hr_imports_coverage_bundle_ck') then
    alter table public.hr_imports add constraint hr_imports_coverage_bundle_ck check (
      coverage_locked_at is null
      or (coverage_mode is not null and coverage_start_date is not null and coverage_end_date is not null
          and coverage_fingerprint is not null and length(coverage_fingerprint)=64
          and coverage_operation_key is not null and coverage_request_hash is not null)
    );
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.hr_imports'::regclass and conname='hr_imports_source_hash_ck') then
    alter table public.hr_imports add constraint hr_imports_source_hash_ck
      check (source_file_sha256 is null or source_file_sha256 ~ '^[0-9a-f]{64}$');
  end if;

  if not exists (select 1 from pg_constraint where conrelid='public.contracts'::regclass and conname='contracts_ts_query_email_override_ck') then
    alter table public.contracts add constraint contracts_ts_query_email_override_ck check (
      not send_ts_queries_to_different_email
      or (nullif(btrim(ts_queries_alt_email_address),'') is not null and length(btrim(ts_queries_alt_email_address)) <= 320)
    );
  end if;

  if not exists (select 1 from pg_constraint where conrelid='public.hr_issue_emails'::regclass and conname='hr_issue_emails_contract_id_fkey') then
    alter table public.hr_issue_emails add constraint hr_issue_emails_contract_id_fkey
      foreign key (contract_id) references public.contracts(id) on delete restrict;
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.hr_issue_emails'::regclass and conname='hr_issue_emails_sent_count_ck') then
    alter table public.hr_issue_emails add constraint hr_issue_emails_sent_count_ck check (sent_count >= 0);
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.hr_issue_emails'::regclass and conname='hr_issue_emails_history_status_ck') then
    alter table public.hr_issue_emails add constraint hr_issue_emails_history_status_ck
      check (delivery_history_status in ('LEGACY_UNVERIFIED','PENDING','SENT_VERIFIED'));
  end if;

  if not exists (select 1 from pg_constraint where conrelid='public.import_review_states'::regclass and conname='import_review_states_last_operation_id_fkey') then
    alter table public.import_review_states add constraint import_review_states_last_operation_id_fkey
      foreign key (last_operation_id) references public.import_apply_operations(id) on delete restrict;
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.import_review_decisions'::regclass and conname='import_review_decisions_issue_id_fkey') then
    alter table public.import_review_decisions add constraint import_review_decisions_issue_id_fkey
      foreign key (issue_id) references public.hr_issue_emails(id) on delete restrict;
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.import_review_daily_timesheet_resolutions'::regclass and conname='import_review_daily_resolution_applied_operation_id_fkey') then
    alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_resolution_applied_operation_id_fkey
      foreign key (applied_operation_id) references public.import_apply_operations(id) on delete restrict;
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.hr_issue_emails'::regclass and conname='hr_issue_emails_last_successful_delivery_id_fkey') then
    alter table public.hr_issue_emails add constraint hr_issue_emails_last_successful_delivery_id_fkey
      foreign key (last_successful_delivery_id) references public.hr_issue_email_deliveries(id) on delete restrict;
  end if;

  if not exists (select 1 from pg_constraint where conrelid='public.import_review_states'::regclass and conname='import_review_states_status_ck') then
    alter table public.import_review_states add constraint import_review_states_status_ck check (
      status in ('STAGED','IN_REVIEW','BLOCKED','READY','APPLYING','APPLIED','ABANDONED','SUPERSEDED'));
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.import_review_states'::regclass and conname='import_review_states_contract_version_ck') then
    alter table public.import_review_states add constraint import_review_states_contract_version_ck
      check (schema_contract_version = 'IMPORT_REVIEW_DB_V1');
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.import_review_states'::regclass and conname='import_review_states_follow_up_ck') then
    alter table public.import_review_states add constraint import_review_states_follow_up_ck check (
      follow_up_status in ('NOT_REQUIRED','PENDING','COMPLETE','FAILED_RETRYABLE'));
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.import_review_states'::regclass and conname='import_review_states_bounds_ck') then
    alter table public.import_review_states add constraint import_review_states_bounds_ck check (
      state_version > 0 and preview_generation >= 0 and follow_up_retry_count >= 0
      and pg_column_size(ui_state_json) <= 65536
      and coalesce(length(follow_up_error_code),0) <= 128
      and coalesce(length(follow_up_error_message),0) <= 1000
      and coalesce(length(abandoned_reason),0) <= 500);
  end if;

  if not exists (select 1 from pg_constraint where conrelid='public.import_review_decisions'::regclass and conname='import_review_decisions_kind_ck') then
    alter table public.import_review_decisions add constraint import_review_decisions_kind_ck check (
      action_kind in ('INCLUDE_SHIFT','EXCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION',
        'EMAIL_ISSUE','EMAIL_REMINDER','INVALIDATE_REFERENCE','DAILY_TIMESHEET_RESOLUTION',
        'MARK_VALIDATION_ERROR','NO_ACTION','ADVISORY'));
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.import_review_decisions'::regclass and conname='import_review_decisions_category_ck') then
    alter table public.import_review_decisions add constraint import_review_decisions_category_ck check (
      action_category in ('PENDING','READY','NO_ACTION','EMAIL','BLOCKED'));
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.import_review_decisions'::regclass and conname='import_review_decisions_bounds_ck') then
    alter table public.import_review_decisions add constraint import_review_decisions_bounds_ck check (
      length(action_id)=64 and action_id ~ '^[0-9a-f]{64}$'
      and length(evidence_fingerprint)=64 and evidence_fingerprint ~ '^[0-9a-f]{64}$'
      and preview_generation >= 0 and pg_column_size(summary_json) <= 32768
      and (not selected or selectable));
  end if;

  if not exists (select 1 from pg_constraint where conrelid='public.import_review_events'::regclass and conname='import_review_events_bounds_ck') then
    alter table public.import_review_events add constraint import_review_events_bounds_ck check (
      state_version > 0 and length(event_code) between 1 and 128 and pg_column_size(event_context_json) <= 16384);
  end if;

  if not exists (select 1 from pg_constraint where conrelid='public.import_review_daily_timesheet_resolutions'::regclass and conname='import_review_daily_resolution_method_ck') then
    alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_resolution_method_ck
      check (resolution_method in ('AUTO_MATCHED','USER_SELECTED'));
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.import_review_daily_timesheet_resolutions'::regclass and conname='import_review_daily_resolution_status_ck') then
    alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_resolution_status_ck
      check (status in ('CURRENT','STALE','CLEARED','APPLIED'));
  end if;
  if not exists (select 1 from pg_constraint where conrelid='public.import_review_daily_timesheet_resolutions'::regclass and conname='import_review_daily_resolution_bounds_ck') then
    alter table public.import_review_daily_timesheet_resolutions add constraint import_review_daily_resolution_bounds_ck check (
      length(evidence_fingerprint)=64 and evidence_fingerprint ~ '^[0-9a-f]{64}$'
      and preview_generation >= 0 and state_version > 0
      and (status in ('CLEARED','STALE') or resolved_timesheet_id is not null)
      and (status <> 'APPLIED' or (applied_operation_id is not null and applied_at_utc is not null)));
  end if;

  if not exists (select 1 from pg_constraint where conrelid='public.hr_issue_email_deliveries'::regclass and conname='hr_issue_email_deliveries_scope_ck') then
    alter table public.hr_issue_email_deliveries add constraint hr_issue_email_deliveries_scope_ck check (
      recipient_scope in ('CLIENT_DEFAULT','CONTRACT_OVERRIDE') and reminder_sequence >= 0
      and length(recipient_email) between 3 and 320
      and length(recipient_route_fingerprint)=64 and recipient_route_fingerprint ~ '^[0-9a-f]{64}$'
      and length(issue_set_fingerprint)=64 and issue_set_fingerprint ~ '^[0-9a-f]{64}$'
      and status in ('QUEUED','ACCEPTED','SENT','FAILED'));
  end if;
end
$migration_constraints$;

-- ---------------------------------------------------------------------------
-- Indexes (all FK and bounded/keyset access paths used by the RPC suite).
-- ---------------------------------------------------------------------------

create unique index if not exists hr_imports_revision_group_revision_uidx
  on public.hr_imports(revision_group_id, revision_no)
  where revision_group_id is not null and revision_no is not null;
create unique index if not exists hr_imports_coverage_operation_key_uidx
  on public.hr_imports(coverage_operation_key)
  where coverage_operation_key is not null;
create index if not exists hr_imports_supersedes_import_id_idx on public.hr_imports(supersedes_import_id);

create index if not exists import_review_scope_clients_import_idx on public.import_review_scope_clients(import_id);
create index if not exists import_review_scope_clients_client_idx on public.import_review_scope_clients(client_id) where client_id is not null;
create index if not exists import_review_scope_candidates_import_idx on public.import_review_scope_candidates(import_id);
create index if not exists import_review_scope_candidates_candidate_idx on public.import_review_scope_candidates(candidate_id) where candidate_id is not null;
create index if not exists import_review_states_list_idx on public.import_review_states(status, updated_at_utc desc, import_id);
create index if not exists import_review_states_last_operation_idx on public.import_review_states(last_operation_id) where last_operation_id is not null;
create index if not exists import_review_states_follow_up_idx on public.import_review_states(follow_up_status, updated_at_utc, import_id)
  where status='APPLIED' and follow_up_status in ('PENDING','FAILED_RETRYABLE');
create index if not exists import_review_decisions_import_page_idx on public.import_review_decisions(import_id, is_current, action_id);
create index if not exists import_review_decisions_selected_idx on public.import_review_decisions(import_id, action_kind, action_id)
  where is_current and selected;
create index if not exists import_review_decisions_timesheet_idx on public.import_review_decisions(timesheet_id) where timesheet_id is not null;
create index if not exists import_review_decisions_hr_row_idx on public.import_review_decisions(hr_row_id) where hr_row_id is not null;
create index if not exists import_review_decisions_shift_idx on public.import_review_decisions(shift_id) where shift_id is not null;
create index if not exists import_review_decisions_client_idx on public.import_review_decisions(client_id) where client_id is not null;
create index if not exists import_review_decisions_candidate_idx on public.import_review_decisions(candidate_id) where candidate_id is not null;
create index if not exists import_review_decisions_contract_idx on public.import_review_decisions(contract_id) where contract_id is not null;
create index if not exists import_review_events_import_page_idx on public.import_review_events(import_id, id);
create index if not exists import_review_events_operation_idx on public.import_review_events(operation_id) where operation_id is not null;
create index if not exists import_review_daily_resolution_import_idx on public.import_review_daily_timesheet_resolutions(import_id, status, hr_row_id);
create index if not exists import_review_daily_resolution_row_idx on public.import_review_daily_timesheet_resolutions(hr_row_id);
create index if not exists import_review_daily_resolution_timesheet_idx on public.import_review_daily_timesheet_resolutions(resolved_timesheet_id) where resolved_timesheet_id is not null;
create index if not exists import_review_daily_resolution_operation_idx on public.import_review_daily_timesheet_resolutions(applied_operation_id) where applied_operation_id is not null;
create index if not exists hr_issue_email_deliveries_import_idx on public.hr_issue_email_deliveries(import_id, status, id);
create index if not exists hr_issue_email_deliveries_operation_idx on public.hr_issue_email_deliveries(operation_id);
create index if not exists hr_issue_email_deliveries_outbox_idx on public.hr_issue_email_deliveries(mail_outbox_id) where mail_outbox_id is not null;
create index if not exists hr_issue_email_delivery_items_issue_idx on public.hr_issue_email_delivery_items(issue_id);
create index if not exists hr_issue_email_delivery_items_action_idx on public.hr_issue_email_delivery_items(action_id);
create index if not exists hr_issue_emails_contract_idx on public.hr_issue_emails(contract_id) where contract_id is not null;
create index if not exists hr_issue_emails_recipient_scope_idx on public.hr_issue_emails(recipient_scope_key, issue_fingerprint) where recipient_scope_key is not null;

-- ---------------------------------------------------------------------------
-- Narrow trigger helpers required to enforce schema invariants immediately.
-- The same active definitions are included in the repeatable function package.
-- ---------------------------------------------------------------------------

create or replace function public.import_review_prune_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare v_status text;
begin
  if old.pruned_at is null and new.pruned_at is not null then
    select s.status into v_status from public.import_review_states s where s.import_id=new.id;
    if v_status in ('STAGED','IN_REVIEW','BLOCKED','READY','APPLYING') then
      raise exception 'IMPORT_REVIEW_ACTIVE_PRUNE_BLOCKED'
        using errcode='55000', detail=jsonb_build_object('import_id',new.id,'status',v_status)::text;
    end if;
  end if;
  return new;
end
$function$;

create or replace function public._import_review_immutable_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_locked timestamptz;
  v_import_id uuid;
  v_mode text;
  v_parent public.hr_imports%rowtype;
begin
  if tg_table_name='hr_imports' then
    if old.coverage_locked_at is not null and (
      new.coverage_mode is distinct from old.coverage_mode
      or new.coverage_start_date is distinct from old.coverage_start_date
      or new.coverage_end_date is distinct from old.coverage_end_date
      or new.coverage_fingerprint is distinct from old.coverage_fingerprint
      or new.coverage_locked_at is distinct from old.coverage_locked_at
      or new.coverage_operation_key is distinct from old.coverage_operation_key
      or new.coverage_request_hash is distinct from old.coverage_request_hash
      or new.source_file_sha256 is distinct from old.source_file_sha256
      or new.parser_version is distinct from old.parser_version
    ) then
      raise exception 'IMPORT_REVIEW_COVERAGE_IMMUTABLE' using errcode='55000';
    end if;

    if old.coverage_locked_at is not null and (
      new.revision_group_id is distinct from old.revision_group_id
      or new.revision_no is distinct from old.revision_no
      or new.supersedes_import_id is distinct from old.supersedes_import_id
    ) then
      if old.supersedes_import_id is not null
         or new.supersedes_import_id is null then
        raise exception 'IMPORT_REVIEW_REVISION_IDENTITY_IMMUTABLE' using errcode='55000';
      end if;
    end if;

    if new.supersedes_import_id is not null and (
      old.supersedes_import_id is distinct from new.supersedes_import_id
      or old.revision_group_id is distinct from new.revision_group_id
      or old.revision_no is distinct from new.revision_no
    ) then
      select p.* into v_parent from public.hr_imports p where p.id=new.supersedes_import_id;
      if not found
         or v_parent.id=new.id
         or v_parent.source_system is distinct from new.source_system
         or new.revision_group_id is distinct from coalesce(v_parent.revision_group_id,v_parent.id)
         or new.revision_no is null
         or new.revision_no <= coalesce(v_parent.revision_no,0) then
        raise exception 'IMPORT_REVIEW_SUPERSESSION_INCONSISTENT' using errcode='23514';
      end if;
    end if;

    if old.coverage_locked_at is null and new.coverage_locked_at is not null then
      if new.coverage_mode='COMPLETE_SELECTED_CANDIDATES' and not exists (
        select 1 from public.import_review_scope_candidates sc where sc.import_id=new.id
      ) then
        raise exception 'IMPORT_REVIEW_SELECTED_CANDIDATE_SCOPE_REQUIRED' using errcode='23514';
      end if;
      if new.coverage_mode<>'COMPLETE_SELECTED_CANDIDATES' and exists (
        select 1 from public.import_review_scope_candidates sc where sc.import_id=new.id
      ) then
        raise exception 'IMPORT_REVIEW_CANDIDATE_SCOPE_NOT_ALLOWED' using errcode='23514';
      end if;
    end if;
    return new;
  end if;

  if tg_op='DELETE' then v_import_id:=old.import_id; else v_import_id:=new.import_id; end if;
  if tg_table_name='import_review_scope_candidates' and tg_op<>'DELETE' then
    select hi.coverage_mode into v_mode from public.hr_imports hi where hi.id=new.import_id;
    if v_mode is distinct from 'COMPLETE_SELECTED_CANDIDATES' then
      raise exception 'IMPORT_REVIEW_CANDIDATE_SCOPE_NOT_ALLOWED' using errcode='23514';
    end if;
  end if;
  select hi.coverage_locked_at into v_locked
  from public.hr_imports hi where hi.id=v_import_id;
  if v_locked is null then
    if tg_op = 'DELETE' then return old; else return new; end if;
  end if;

  if tg_op in ('INSERT','DELETE') then
    raise exception 'IMPORT_REVIEW_SCOPE_IMMUTABLE' using errcode='55000';
  end if;
  if tg_table_name='import_review_scope_clients' and (
    new.import_id is distinct from old.import_id
    or new.source_client_key is distinct from old.source_client_key
    or new.source_display_label is distinct from old.source_display_label
  ) then raise exception 'IMPORT_REVIEW_SCOPE_IDENTITY_IMMUTABLE' using errcode='55000'; end if;
  if tg_table_name='import_review_scope_candidates' and (
    new.import_id is distinct from old.import_id
    or new.source_candidate_key is distinct from old.source_candidate_key
    or new.source_display_label is distinct from old.source_display_label
  ) then raise exception 'IMPORT_REVIEW_SCOPE_IDENTITY_IMMUTABLE' using errcode='55000'; end if;
  return new;
end
$function$;

create or replace function public._import_review_state_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_transition_allowed boolean:=false;
  v_old_without_allowed jsonb;
  v_new_without_allowed jsonb;
begin
  if tg_op='DELETE' then
    raise exception 'IMPORT_REVIEW_STATE_DELETE_BLOCKED' using errcode='55000';
  end if;
  if new.import_id is distinct from old.import_id
     or new.schema_contract_version is distinct from old.schema_contract_version then
    raise exception 'IMPORT_REVIEW_STATE_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  if new.state_version < old.state_version then
    raise exception 'IMPORT_REVIEW_STATE_VERSION_REGRESSION' using errcode='23514';
  end if;

  if old.status in ('APPLIED','ABANDONED','SUPERSEDED') then
    if old.status='APPLIED' then
      v_old_without_allowed:=to_jsonb(old)-array[
        'state_version','follow_up_status','follow_up_error_code','follow_up_error_message',
        'follow_up_retry_count','last_opened_at_utc','last_opened_by_user_id',
        'updated_at_utc','updated_by_user_id'
      ];
      v_new_without_allowed:=to_jsonb(new)-array[
        'state_version','follow_up_status','follow_up_error_code','follow_up_error_message',
        'follow_up_retry_count','last_opened_at_utc','last_opened_by_user_id',
        'updated_at_utc','updated_by_user_id'
      ];
    else
      v_old_without_allowed:=to_jsonb(old)-array[
        'last_opened_at_utc','last_opened_by_user_id','updated_at_utc','updated_by_user_id'
      ];
      v_new_without_allowed:=to_jsonb(new)-array[
        'last_opened_at_utc','last_opened_by_user_id','updated_at_utc','updated_by_user_id'
      ];
    end if;
    if v_old_without_allowed is distinct from v_new_without_allowed then
      raise exception 'IMPORT_REVIEW_TERMINAL_STATE_IMMUTABLE' using errcode='55000';
    end if;
    return new;
  end if;

  v_transition_allowed:=case old.status
    when 'STAGED' then new.status in ('STAGED','IN_REVIEW','BLOCKED','READY','ABANDONED','SUPERSEDED')
    when 'IN_REVIEW' then new.status in ('IN_REVIEW','BLOCKED','READY','ABANDONED','SUPERSEDED')
    when 'BLOCKED' then new.status in ('IN_REVIEW','BLOCKED','READY','ABANDONED','SUPERSEDED')
    when 'READY' then new.status in ('IN_REVIEW','BLOCKED','READY','APPLYING','ABANDONED','SUPERSEDED')
    when 'APPLYING' then new.status in ('APPLYING','APPLIED')
    else false end;
  if not v_transition_allowed then
    raise exception 'IMPORT_REVIEW_STATUS_TRANSITION_INVALID'
      using errcode='23514',detail=jsonb_build_object('old_status',old.status,'new_status',new.status)::text;
  end if;
  if new.status is distinct from old.status and new.state_version<=old.state_version then
    raise exception 'IMPORT_REVIEW_STATUS_TRANSITION_REQUIRES_VERSION' using errcode='23514';
  end if;
  if new.status='APPLYING' and new.last_operation_id is null then
    raise exception 'IMPORT_REVIEW_APPLYING_OPERATION_REQUIRED' using errcode='23514';
  end if;
  if new.status='APPLIED' and (new.applied_at_utc is null or new.applied_by_user_id is null or new.last_operation_id is null) then
    raise exception 'IMPORT_REVIEW_APPLIED_METADATA_REQUIRED' using errcode='23514';
  end if;
  if new.status='ABANDONED' and (new.abandoned_at_utc is null or new.abandoned_by_user_id is null or nullif(btrim(new.abandoned_reason),'') is null) then
    raise exception 'IMPORT_REVIEW_ABANDONED_METADATA_REQUIRED' using errcode='23514';
  end if;
  if new.status='SUPERSEDED' and (new.superseded_at_utc is null or new.superseded_by_user_id is null) then
    raise exception 'IMPORT_REVIEW_SUPERSEDED_METADATA_REQUIRED' using errcode='23514';
  end if;
  return new;
end
$function$;

create or replace function public._import_review_daily_resolution_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
begin
  if tg_op='DELETE' then
    if old.status='APPLIED' then
      raise exception 'IMPORT_REVIEW_APPLIED_RESOLUTION_IMMUTABLE' using errcode='55000';
    end if;
    return old;
  end if;
  if new.import_id is distinct from old.import_id or new.hr_row_id is distinct from old.hr_row_id then
    raise exception 'IMPORT_REVIEW_RESOLUTION_IDENTITY_IMMUTABLE' using errcode='55000';
  end if;
  if old.status='APPLIED' and to_jsonb(new) is distinct from to_jsonb(old) then
    raise exception 'IMPORT_REVIEW_APPLIED_RESOLUTION_IMMUTABLE' using errcode='55000';
  end if;
  if old.status='APPLIED' then return new; end if;
  if not (case old.status
    when 'CURRENT' then new.status in ('CURRENT','STALE','CLEARED','APPLIED')
    when 'STALE' then new.status in ('STALE','CURRENT','CLEARED')
    when 'CLEARED' then new.status in ('CLEARED','CURRENT','STALE')
    else false end) then
    raise exception 'IMPORT_REVIEW_RESOLUTION_TRANSITION_INVALID'
      using errcode='23514',detail=jsonb_build_object('old_status',old.status,'new_status',new.status)::text;
  end if;
  if new.status='APPLIED' and (new.applied_operation_id is null or new.applied_at_utc is null) then
    raise exception 'IMPORT_REVIEW_APPLIED_RESOLUTION_METADATA_REQUIRED' using errcode='23514';
  end if;
  return new;
end
$function$;

create or replace function public._import_review_events_immutable_guard_v1()
returns trigger
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
begin
  raise exception 'IMPORT_REVIEW_EVENTS_ARE_APPEND_ONLY' using errcode='55000';
end
$function$;

drop trigger if exists trg_import_review_prune_guard on public.hr_imports;
create trigger trg_import_review_prune_guard before update of pruned_at on public.hr_imports
for each row execute function public.import_review_prune_guard_v1();

drop trigger if exists trg_hr_imports_review_immutable on public.hr_imports;
create trigger trg_hr_imports_review_immutable before update on public.hr_imports
for each row execute function public._import_review_immutable_guard_v1();

drop trigger if exists trg_import_review_scope_clients_immutable on public.import_review_scope_clients;
create trigger trg_import_review_scope_clients_immutable before insert or update or delete on public.import_review_scope_clients
for each row execute function public._import_review_immutable_guard_v1();

drop trigger if exists trg_import_review_scope_candidates_immutable on public.import_review_scope_candidates;
create trigger trg_import_review_scope_candidates_immutable before insert or update or delete on public.import_review_scope_candidates
for each row execute function public._import_review_immutable_guard_v1();

drop trigger if exists trg_import_review_states_guard on public.import_review_states;
create trigger trg_import_review_states_guard before update or delete on public.import_review_states
for each row execute function public._import_review_state_guard_v1();

drop trigger if exists trg_import_review_daily_resolution_guard on public.import_review_daily_timesheet_resolutions;
create trigger trg_import_review_daily_resolution_guard before update or delete on public.import_review_daily_timesheet_resolutions
for each row execute function public._import_review_daily_resolution_guard_v1();

drop trigger if exists trg_import_review_events_immutable on public.import_review_events;
create trigger trg_import_review_events_immutable before update or delete on public.import_review_events
for each row execute function public._import_review_events_immutable_guard_v1();

-- ---------------------------------------------------------------------------
-- Shape validation: a same-named incompatible object fails the rerun atomically.
-- ---------------------------------------------------------------------------

do $shape_validation$
declare v_bad text;
begin
  with expected(table_name,column_name,udt_name,is_nullable) as (values
    ('hr_imports','coverage_mode','text','YES'),
    ('hr_imports','coverage_locked_at','timestamptz','YES'),
    ('contracts','send_ts_queries_to_different_email','bool','NO'),
    ('contracts','ts_queries_alt_email_address','text','YES'),
    ('import_review_states','schema_contract_version','text','NO'),
    ('import_review_states','state_version','int8','NO'),
    ('import_review_decisions','action_id','text','NO'),
    ('import_review_daily_timesheet_resolutions','resolved_timesheet_id','uuid','YES'),
    ('hr_issue_email_deliveries','mail_outbox_id','uuid','YES')
  )
  select string_agg(format('%s.%s expected %s/%s got %s/%s',e.table_name,e.column_name,e.udt_name,e.is_nullable,c.udt_name,c.is_nullable),'; ')
  into v_bad
  from expected e
  left join information_schema.columns c
    on c.table_schema='public' and c.table_name=e.table_name and c.column_name=e.column_name
  where c.column_name is null or c.udt_name<>e.udt_name or c.is_nullable<>e.is_nullable;
  if v_bad is not null then
    raise exception 'IMPORT_REVIEW_SCHEMA_INCOMPATIBLE' using errcode='55000',detail=v_bad;
  end if;
end
$shape_validation$;

-- ---------------------------------------------------------------------------
-- Security: service access is through SECURITY DEFINER RPCs, not direct tables.
-- ---------------------------------------------------------------------------

alter table public.import_review_scope_clients enable row level security;
alter table public.import_review_scope_candidates enable row level security;
alter table public.import_review_states enable row level security;
alter table public.import_review_decisions enable row level security;
alter table public.import_review_events enable row level security;
alter table public.import_review_daily_timesheet_resolutions enable row level security;
alter table public.hr_issue_email_deliveries enable row level security;
alter table public.hr_issue_email_delivery_items enable row level security;

revoke all on table public.import_review_scope_clients from public, anon, authenticated, service_role;
revoke all on table public.import_review_scope_candidates from public, anon, authenticated, service_role;
revoke all on table public.import_review_states from public, anon, authenticated, service_role;
revoke all on table public.import_review_decisions from public, anon, authenticated, service_role;
revoke all on table public.import_review_events from public, anon, authenticated, service_role;
revoke all on table public.import_review_daily_timesheet_resolutions from public, anon, authenticated, service_role;
revoke all on table public.hr_issue_email_deliveries from public, anon, authenticated, service_role;
revoke all on table public.hr_issue_email_delivery_items from public, anon, authenticated, service_role;
revoke all on sequence public.import_review_events_id_seq from public, anon, authenticated, service_role;

revoke all on function public.import_review_prune_guard_v1() from public, anon, authenticated, service_role;
revoke all on function public._import_review_immutable_guard_v1() from public, anon, authenticated, service_role;
revoke all on function public._import_review_state_guard_v1() from public, anon, authenticated, service_role;
revoke all on function public._import_review_daily_resolution_guard_v1() from public, anon, authenticated, service_role;
revoke all on function public._import_review_events_immutable_guard_v1() from public, anon, authenticated, service_role;

comment on table public.import_review_states is
  'CloudTMS import review workflow schema v1 (2026-07-21). APPLIED is independent of follow-up status.';
comment on column public.import_review_states.schema_contract_version is
  'Machine-readable hard-cutover contract marker. Only IMPORT_REVIEW_DB_V1 is valid for this release.';
comment on table public.import_review_daily_timesheet_resolutions is
  'Daily HealthRoster evidence associations only. Never an authority to edit timesheet economics or frozen artefacts.';
comment on column public.hr_issue_emails.delivery_history_status is
  'Existing migration-time rows remain LEGACY_UNVERIFIED and are not automatically re-sent.';

commit;
