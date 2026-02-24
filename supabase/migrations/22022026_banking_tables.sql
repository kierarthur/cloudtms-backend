begin;

-- ============================================================
-- DROP Revolut-specific constraint(s) (safe)
-- ============================================================
alter table public.settings_defaults
  drop constraint if exists settings_defaults_revolut_env_check;

-- ============================================================
-- DROP Revolut-specific columns (settings_defaults)
-- ============================================================
alter table public.settings_defaults
  drop column if exists revolut_env,
  drop column if exists revolut_oauth_redirect_uri,
  drop column if exists revolut_client_id,
  drop column if exists revolut_source_account_id_gbp,
  drop column if exists revolut_draft_reference_prefix;

-- ============================================================
-- DROP Revolut-specific columns (candidates / umbrellas)
-- ============================================================
alter table public.candidates
  drop column if exists revolut_counterparty_id,
  drop column if exists revolut_counterparty_account_id;

alter table public.umbrellas
  drop column if exists revolut_counterparty_id,
  drop column if exists revolut_counterparty_account_id;

-- ============================================================
-- DROP Revolut-specific columns (pay_batches / pay_batch_items / pay_bank_transfers)
-- ============================================================
alter table public.pay_batches
  drop column if exists revolut_draft_id;

alter table public.pay_batch_items
  drop column if exists revolut_transaction_id,
  drop column if exists revolut_transaction_state;

alter table public.pay_bank_transfers
  drop column if exists revolut_transaction_id,
  drop column if exists revolut_state,
  drop column if exists revolut_counterparty_id,
  drop column if exists revolut_counterparty_account_id;

-- ============================================================
-- DROP any old Revolut-only mapping table if it exists
-- (you may or may not have created this previously)
-- ============================================================
drop table if exists public.revolut_counterparty_map;

commit;

begin;

-- ============================================================
-- CloudTMS Banking Rails (A1–A10) — DB-first migration
-- Idempotent: safe to run more than once
-- Notes:
-- - timesheets PK is public.timesheets.timesheet_id (NOT "id")
-- - preserves existing banking_system_snapshot flows for now
-- ============================================================

-- ============================================================
-- Prereqs
-- ============================================================
create extension if not exists pgcrypto;

-- ============================================================
-- A1) settings_defaults: generic rail config + scheduling defaults
-- ============================================================
alter table public.settings_defaults
  add column if not exists rail_provider_default text not null default 'CSV',
  add column if not exists rail_env_default text not null default 'PROD',
  add column if not exists rail_supports_scheduling boolean not null default false,
  add column if not exists rail_supports_name_check boolean not null default false,
  add column if not exists rail_supports_auto_execute boolean not null default false,
  add column if not exists default_schedule_umbrella_local text not null default 'Thu 02:00',
  add column if not exists default_schedule_paye_local text not null default 'Fri 02:00',
  add column if not exists funds_warning_hours_json jsonb not null default '[24,12]'::jsonb;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'settings_defaults_rail_provider_default_check'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_rail_provider_default_check
      check (rail_provider_default in ('REVOLUT','CSV'));
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'settings_defaults_rail_env_default_check'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_rail_env_default_check
      check (rail_env_default in ('PROD','SANDBOX'));
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'settings_defaults_funds_warning_hours_json_is_array_chk'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_funds_warning_hours_json_is_array_chk
      check (jsonb_typeof(funds_warning_hours_json) = 'array');
  end if;
end $$;

-- ============================================================
-- A2) bank_payee_map: generic provider payee mapping
-- ============================================================
create table if not exists public.bank_payee_map (
  id uuid primary key default gen_random_uuid(),

  rail_provider text not null, -- REVOLUT | CSV | (future)
  rail_env text not null,      -- PROD | SANDBOX

  entity_kind text not null,   -- CANDIDATE | UMBRELLA
  entity_id uuid not null,

  bank_details_hash text not null,

  payee_id text not null,           -- provider-specific id (e.g. Revolut counterparty_id)
  payee_account_id text null,       -- optional (e.g. counterparty account id)
  meta_json jsonb null,             -- raw provider response

  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),

  constraint bank_payee_map_provider_env_chk
    check (rail_provider in ('REVOLUT','CSV') and rail_env in ('PROD','SANDBOX')),
  constraint bank_payee_map_entity_kind_chk
    check (entity_kind in ('CANDIDATE','UMBRELLA'))
);

create unique index if not exists ux_bank_payee_map_key
  on public.bank_payee_map(rail_provider, rail_env, entity_kind, entity_id, bank_details_hash);

create index if not exists ix_bank_payee_map_entity
  on public.bank_payee_map(entity_kind, entity_id);

create index if not exists ix_bank_payee_map_provider_env
  on public.bank_payee_map(rail_provider, rail_env);

-- ============================================================
-- A3) bank_name_checks: generic "name check" (CoP-like) + override capture
-- ============================================================
create table if not exists public.bank_name_checks (
  id uuid primary key default gen_random_uuid(),

  rail_provider text not null,
  rail_env text not null,

  entity_kind text not null,
  entity_id uuid not null,

  bank_details_hash text not null,

  status text not null, -- UNVERIFIED/PASS/NEAR_MATCH/FAIL/UNAVAILABLE
  checked_at_utc timestamptz null,
  result_json jsonb null,

  override_reason text null,
  override_by_user_id uuid null,
  override_at_utc timestamptz null,
  override_hash text null,

  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),

  constraint bank_name_checks_provider_env_chk
    check (rail_provider in ('REVOLUT','CSV') and rail_env in ('PROD','SANDBOX')),
  constraint bank_name_checks_entity_kind_chk
    check (entity_kind in ('CANDIDATE','UMBRELLA')),
  constraint bank_name_checks_status_chk
    check (status in ('UNVERIFIED','PASS','NEAR_MATCH','FAIL','UNAVAILABLE')),
  constraint bank_name_checks_override_hash_chk
    check (override_reason is null or override_hash = bank_details_hash)
);

create unique index if not exists ux_bank_name_checks_key
  on public.bank_name_checks(rail_provider, rail_env, entity_kind, entity_id, bank_details_hash);

create index if not exists ix_bank_name_checks_entity
  on public.bank_name_checks(entity_kind, entity_id);

-- ============================================================
-- A4) Add bank_details_hash to candidates + umbrellas
-- ============================================================
alter table public.candidates
  add column if not exists bank_details_hash text null;

alter table public.umbrellas
  add column if not exists bank_details_hash text null;

-- ============================================================
-- A5) Hash helper + triggers + backfill
-- ============================================================
create or replace function public._bank_hash(
  p_sort_code text,
  p_account_number text,
  p_account_holder text
) returns text
language plpgsql
as $$
declare
  v_sort text;
  v_acct text;
  v_name text;
  v_raw  text;
begin
  v_sort := regexp_replace(coalesce(p_sort_code,''), '[^0-9]+', '', 'g');
  v_acct := regexp_replace(coalesce(p_account_number,''), '[^0-9]+', '', 'g');
  v_name := upper(regexp_replace(btrim(coalesce(p_account_holder,'')), '\s+', ' ', 'g'));

  if v_sort = '' or v_acct = '' then
    return null;
  end if;

  v_raw := v_sort || '|' || v_acct || '|' || v_name;
  return encode(digest(v_raw, 'sha256'), 'hex');
end $$;

create or replace function public._trg_candidates_set_bank_hash()
returns trigger
language plpgsql
as $$
begin
  new.bank_details_hash := public._bank_hash(new.sort_code, new.account_number, new.account_holder);
  return new;
end $$;

create or replace function public._trg_umbrellas_set_bank_hash()
returns trigger
language plpgsql
as $$
begin
  -- umbrellas schema has no account_holder; use umbrella.name as holder identity
  new.bank_details_hash := public._bank_hash(new.sort_code, new.account_number, new.name);
  return new;
end $$;

do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'trg_candidates_set_bank_hash') then
    create trigger trg_candidates_set_bank_hash
    before insert or update of sort_code, account_number, account_holder
    on public.candidates
    for each row
    execute function public._trg_candidates_set_bank_hash();
  end if;

  if not exists (select 1 from pg_trigger where tgname = 'trg_umbrellas_set_bank_hash') then
    create trigger trg_umbrellas_set_bank_hash
    before insert or update of sort_code, account_number, name
    on public.umbrellas
    for each row
    execute function public._trg_umbrellas_set_bank_hash();
  end if;
end $$;

-- Backfill hashes (idempotent)
update public.candidates
set bank_details_hash = public._bank_hash(sort_code, account_number, account_holder)
where bank_details_hash is null;

update public.umbrellas
set bank_details_hash = public._bank_hash(sort_code, account_number, name)
where bank_details_hash is null;

-- ============================================================
-- A6) pay_batch_timesheet_snapshots (frozen baseline targets)
-- ============================================================
create table if not exists public.pay_batch_timesheet_snapshots (
  id uuid primary key default gen_random_uuid(),

  pay_batch_id uuid not null
    references public.pay_batches(id) on delete cascade,

  -- IMPORTANT: timesheets PK is timesheet_id
  timesheet_id uuid not null
    references public.timesheets(timesheet_id) on delete cascade,

  candidate_id uuid not null
    references public.candidates(id) on delete restrict,

  pay_channel text not null,
  base_snapshot_json jsonb not null,
  target_snapshot_json jsonb not null,
  signature text not null,

  created_at_utc timestamptz not null default now(),

  constraint pay_batch_timesheet_snapshots_pay_channel_chk
    check (pay_channel in ('PAYE','UMBRELLA'))
);

create unique index if not exists ux_pay_batch_timesheet_snapshots_key
  on public.pay_batch_timesheet_snapshots(pay_batch_id, timesheet_id, pay_channel);

create index if not exists ix_pay_batch_timesheet_snapshots_batch
  on public.pay_batch_timesheet_snapshots(pay_batch_id);

create index if not exists ix_pay_batch_timesheet_snapshots_timesheet
  on public.pay_batch_timesheet_snapshots(timesheet_id);

-- ============================================================
-- A7) pay_bank_transfers: generic rail columns + idempotency key
-- ============================================================
alter table public.pay_bank_transfers
  add column if not exists rail_provider text not null default 'CSV',
  add column if not exists rail_env text not null default 'PROD',
  add column if not exists request_id text null,                 -- idempotency key for rail execution
  add column if not exists rail_tx_id text null,                 -- provider tx id (generic)
  add column if not exists rail_state text null,                 -- provider state (generic)
  add column if not exists rail_meta_json jsonb null,            -- raw provider payload
  add column if not exists bank_details_hash_snapshot text null, -- traceability
  add column if not exists payee_entity_kind text not null default 'CANDIDATE',
  add column if not exists payee_entity_id uuid null,
  add column if not exists transfer_group_key text null,
  add column if not exists grouping_mode_used text null,
  add column if not exists week_ending_bucket date null;

do $$
begin
  if not exists (select 1 from pg_constraint where conname='pay_bank_transfers_payee_entity_kind_chk') then
    alter table public.pay_bank_transfers
      add constraint pay_bank_transfers_payee_entity_kind_chk
      check (payee_entity_kind in ('CANDIDATE','UMBRELLA'));
  end if;

  if not exists (select 1 from pg_constraint where conname='pay_bank_transfers_rail_provider_chk') then
    alter table public.pay_bank_transfers
      add constraint pay_bank_transfers_rail_provider_chk
      check (rail_provider in ('REVOLUT','CSV'));
  end if;

  if not exists (select 1 from pg_constraint where conname='pay_bank_transfers_rail_env_chk') then
    alter table public.pay_bank_transfers
      add constraint pay_bank_transfers_rail_env_chk
      check (rail_env in ('PROD','SANDBOX'));
  end if;
end $$;

-- Ensure transfer_group_key is populated (idempotent)
update public.pay_bank_transfers
set transfer_group_key = coalesce(nullif(transfer_group_key,''), id::text)
where transfer_group_key is null or transfer_group_key = '';

-- Ensure request_id is populated for idempotency (idempotent)
update public.pay_bank_transfers
set request_id = coalesce(nullif(request_id,''), id::text)
where request_id is null or request_id = '';

-- Replace status check with a superset (keeps existing values and adds BLOCKED)
do $$
begin
  if exists (select 1 from pg_constraint where conname = 'pay_bank_transfers_status_check') then
    alter table public.pay_bank_transfers drop constraint pay_bank_transfers_status_check;
  end if;

  if not exists (select 1 from pg_constraint where conname = 'pay_bank_transfers_status_chk_v2') then
    alter table public.pay_bank_transfers
      add constraint pay_bank_transfers_status_chk_v2
      check (status in ('PENDING','COMPLETED','FAILED','BLOCKED'));
  end if;
end $$;

create unique index if not exists ux_pay_bank_transfers_group_key
  on public.pay_bank_transfers(pay_batch_id, pay_channel, transfer_group_key);

-- ============================================================
-- A8) pay_batch_items: link items to transfers + repayment week
-- ============================================================
alter table public.pay_batch_items
  add column if not exists pay_bank_transfer_id uuid null
    references public.pay_bank_transfers(id),
  add column if not exists repayment_week_start date null;

create index if not exists ix_pay_batch_items_transfer_id
  on public.pay_batch_items(pay_bank_transfer_id);

-- ============================================================
-- A9) pay_batches: rail snapshot + scheduling + funds-check fields + status
-- ============================================================
alter table public.pay_batches
  add column if not exists rail_provider_snapshot text not null default 'CSV',
  add column if not exists rail_env_snapshot text not null default 'PROD',

  add column if not exists schedule_kind text null,                -- IMMEDIATE | SCHEDULED
  add column if not exists scheduled_at_utc timestamptz null,
  add column if not exists scheduled_by_user_id uuid null
    references public.tms_users(id),

  add column if not exists executing_started_at_utc timestamptz null,
  add column if not exists completed_at_utc timestamptz null,

  add column if not exists cancelled_at_utc timestamptz null,
  add column if not exists cancelled_by_user_id uuid null
    references public.tms_users(id),
  add column if not exists cancel_reason text null,

  add column if not exists funding_account_ref text null,          -- provider-specific funding account reference (generic)
  add column if not exists funds_warning_hours_json jsonb null,
  add column if not exists last_funds_check_at_utc timestamptz null,
  add column if not exists last_funds_check_json jsonb null,
  add column if not exists funds_warning_sent_json jsonb null;

do $$
begin
  if not exists (select 1 from pg_constraint where conname='pay_batches_rail_provider_chk') then
    alter table public.pay_batches
      add constraint pay_batches_rail_provider_chk
      check (rail_provider_snapshot in ('REVOLUT','CSV'));
  end if;

  if not exists (select 1 from pg_constraint where conname='pay_batches_rail_env_chk') then
    alter table public.pay_batches
      add constraint pay_batches_rail_env_chk
      check (rail_env_snapshot in ('PROD','SANDBOX'));
  end if;

  if not exists (select 1 from pg_constraint where conname='pay_batches_schedule_kind_chk') then
    alter table public.pay_batches
      add constraint pay_batches_schedule_kind_chk
      check (schedule_kind is null or schedule_kind in ('IMMEDIATE','SCHEDULED'));
  end if;

  if not exists (select 1 from pg_constraint where conname='pay_batches_funds_warning_hours_is_array_chk') then
    alter table public.pay_batches
      add constraint pay_batches_funds_warning_hours_is_array_chk
      check (funds_warning_hours_json is null or jsonb_typeof(funds_warning_hours_json) = 'array');
  end if;
end $$;

-- Backfill rail_provider_snapshot from existing banking_system_snapshot (idempotent)
update public.pay_batches pb
set rail_provider_snapshot =
  case
    when upper(coalesce(pb.banking_system_snapshot,'')) = 'REVOLUT_API' then 'REVOLUT'
    when upper(coalesce(pb.banking_system_snapshot,'')) like 'REVOLUT_%' then 'REVOLUT'
    else 'CSV'
  end
where pb.rail_provider_snapshot is null
   or pb.rail_provider_snapshot = '';

-- Replace pay_batches status check with a superset that includes existing + new scheduling states
do $$
begin
  if exists (select 1 from pg_constraint where conname = 'pay_batches_status_check') then
    alter table public.pay_batches drop constraint pay_batches_status_check;
  end if;

  if not exists (select 1 from pg_constraint where conname = 'pay_batches_status_chk_v2') then
    alter table public.pay_batches
      add constraint pay_batches_status_chk_v2
      check (status in (
        -- existing (observed in DB functions)
        'DRAFT',
        'WAITING_BANK_CONFIRM',
        'DRAFT_CREATED',
        'PARTIAL',
        'FAILED',
        'SETTLED',

        -- new (for rail scheduling + rollback)
        'READY',
        'SCHEDULED',
        'EXECUTING',
        'CANCELLED',
        'BLOCKED_FUNDS'
      ));
  end if;
end $$;

-- ============================================================
-- A10) Invoice discounting runs: store bank upload code
-- ============================================================
alter table public.id_consolidation_runs
  add column if not exists bank_upload_code text null,
  add column if not exists bank_uploaded_at_utc timestamptz null,
  add column if not exists note text null;

commit;



