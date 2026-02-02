begin;

-- =========================================================
-- A1.1 settings_defaults columns
-- =========================================================
alter table public.settings_defaults
  add column if not exists external_paye_system text not null default 'SAGE',
  add column if not exists banking_system       text not null default 'MONZO_CSV',
  add column if not exists revolut_env          text not null default 'PROD',
  add column if not exists revolut_oauth_redirect_uri      text null,
  add column if not exists revolut_client_id              text null,
  add column if not exists revolut_source_account_id_gbp   text null,
  add column if not exists revolut_draft_reference_prefix  text null;

do $$
begin
  -- external_paye_system: SAGE | CSV
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'settings_defaults'
      and c.conname = 'settings_defaults_external_paye_system_check'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_external_paye_system_check
      check (external_paye_system in ('SAGE','CSV'));
  end if;

  -- banking_system: REVOLUT_API | MONZO_CSV | REVOLUT_CSV
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'settings_defaults'
      and c.conname = 'settings_defaults_banking_system_check'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_banking_system_check
      check (banking_system in ('REVOLUT_API','MONZO_CSV','REVOLUT_CSV'));
  end if;

  -- revolut_env: PROD | SANDBOX
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'settings_defaults'
      and c.conname = 'settings_defaults_revolut_env_check'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_revolut_env_check
      check (revolut_env in ('PROD','SANDBOX'));
  end if;
end$$;

-- =========================================================
-- A1.2 Revolut counterparty fields
-- =========================================================
alter table public.candidates
  add column if not exists revolut_counterparty_id text null,
  add column if not exists revolut_counterparty_account_id text null;

alter table public.umbrellas
  add column if not exists revolut_counterparty_id text null,
  add column if not exists revolut_counterparty_account_id text null;

-- =========================================================
-- A1.3 Weekly cancellation support on nhsp_shifts
-- NOTE: nhsp_shifts uses work_date (date)
-- =========================================================
alter table public.nhsp_shifts
  add column if not exists cancelled_at_utc timestamptz null,
  add column if not exists cancelled_by_import_id uuid null,
  add column if not exists cancelled_reason text null;

do $$
begin
  -- FK to hr_imports only if table exists
  if to_regclass('public.hr_imports') is not null then
    if not exists (
      select 1
      from pg_constraint c
      join pg_class t on t.oid = c.conrelid
      join pg_namespace n on n.oid = t.relnamespace
      where n.nspname = 'public'
        and t.relname = 'nhsp_shifts'
        and c.conname = 'nhsp_shifts_cancelled_by_import_id_fkey'
    ) then
      alter table public.nhsp_shifts
        add constraint nhsp_shifts_cancelled_by_import_id_fkey
        foreign key (cancelled_by_import_id)
        references public.hr_imports(id)
        on delete set null;
    end if;
  end if;
end$$;

create index if not exists idx_nhsp_shifts_missing_shift_lookup
  on public.nhsp_shifts (source_system, client_id, candidate_id, work_date, cancelled_at_utc);

-- =========================================================
-- A1.4 Invoice Discounting ledger tables + sequence
-- =========================================================
create sequence if not exists public.id_ref_seq start 100001;

create table if not exists public.id_invoice_ledger (
  invoice_id uuid primary key,
  invoice_number text,
  invoice_status text,
  invoice_type text, -- 'INVOICE' | 'CREDIT_NOTE' snapshot
  current_ex_vat numeric(12,2) not null default 0,
  current_vat    numeric(12,2) not null default 0,
  current_inc_vat numeric(12,2) not null default 0,
  last_reported_inc_vat numeric(12,2) not null default 0,
  updated_at_utc timestamptz not null default now()
);

create index if not exists idx_id_invoice_ledger_changed
  on public.id_invoice_ledger (updated_at_utc desc);

create table if not exists public.id_consolidation_runs (
  id_ref text primary key,
  created_at_utc timestamptz not null default now(),
  created_by_user_id uuid null,
  total_delta_inc_vat numeric(12,2) not null
);

do $$
begin
  if to_regclass('public.tms_users') is not null then
    if not exists (
      select 1
      from pg_constraint c
      join pg_class t on t.oid = c.conrelid
      join pg_namespace n on n.oid = t.relnamespace
      where n.nspname='public'
        and t.relname='id_consolidation_runs'
        and c.conname='id_consolidation_runs_created_by_user_id_fkey'
    ) then
      alter table public.id_consolidation_runs
        add constraint id_consolidation_runs_created_by_user_id_fkey
        foreign key (created_by_user_id)
        references public.tms_users(id)
        on delete set null;
    end if;
  end if;

  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='id_consolidation_runs'
      and c.conname='id_consolidation_runs_id_ref_6digits_chk'
  ) then
    alter table public.id_consolidation_runs
      add constraint id_consolidation_runs_id_ref_6digits_chk
      check (id_ref ~ '^[0-9]{6}$');
  end if;
end$$;

create table if not exists public.id_consolidation_run_lines (
  id_ref text not null,
  invoice_id uuid not null,
  invoice_number text,
  invoice_status text,
  invoice_type text,
  delta_inc_vat numeric(12,2),
  current_inc_vat numeric(12,2),
  primary key (id_ref, invoice_id)
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='id_consolidation_run_lines'
      and c.conname='id_consolidation_run_lines_id_ref_fkey'
  ) then
    alter table public.id_consolidation_run_lines
      add constraint id_consolidation_run_lines_id_ref_fkey
      foreign key (id_ref)
      references public.id_consolidation_runs(id_ref)
      on delete cascade;
  end if;
end$$;

create index if not exists idx_id_consolidation_run_lines_invoice
  on public.id_consolidation_run_lines (invoice_id);

-- =========================================================
-- A1.5 Payroll ledger tables
-- =========================================================
create table if not exists public.pay_batches (
  id uuid primary key default gen_random_uuid(),
  pay_date date not null,

  created_at_utc timestamptz not null default now(),
  created_by_user_id uuid null,

  status text not null, -- DRAFT, WAITING_BANK_CONFIRM, DRAFT_CREATED, SETTLED, PARTIAL, FAILED, UNPAID

  banking_system_snapshot text not null,
  external_paye_system_snapshot text not null,

  monzo_confirmed_at_utc timestamptz null,
  monzo_confirmed_by_user_id uuid null,

  revolut_draft_id text null,
  last_status_checked_at_utc timestamptz null,

  total_bank_out numeric(12,2) null,
  total_debt_created numeric(12,2) null
);

do $$
begin
  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batches'
      and c.conname='pay_batches_status_check'
  ) then
    alter table public.pay_batches
      add constraint pay_batches_status_check
      check (status in ('DRAFT','WAITING_BANK_CONFIRM','DRAFT_CREATED','SETTLED','PARTIAL','FAILED','UNPAID'));
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batches'
      and c.conname='pay_batches_banking_system_snapshot_check'
  ) then
    alter table public.pay_batches
      add constraint pay_batches_banking_system_snapshot_check
      check (banking_system_snapshot in ('REVOLUT_API','MONZO_CSV','REVOLUT_CSV'));
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batches'
      and c.conname='pay_batches_external_paye_system_snapshot_check'
  ) then
    alter table public.pay_batches
      add constraint pay_batches_external_paye_system_snapshot_check
      check (external_paye_system_snapshot in ('SAGE','CSV'));
  end if;

  if to_regclass('public.tms_users') is not null then
    if not exists (
      select 1 from pg_constraint c
      join pg_class t on t.oid=c.conrelid
      join pg_namespace n on n.oid=t.relnamespace
      where n.nspname='public' and t.relname='pay_batches'
        and c.conname='pay_batches_created_by_user_id_fkey'
    ) then
      alter table public.pay_batches
        add constraint pay_batches_created_by_user_id_fkey
        foreign key (created_by_user_id) references public.tms_users(id)
        on delete set null;
    end if;

    if not exists (
      select 1 from pg_constraint c
      join pg_class t on t.oid=c.conrelid
      join pg_namespace n on n.oid=t.relnamespace
      where n.nspname='public' and t.relname='pay_batches'
        and c.conname='pay_batches_monzo_confirmed_by_user_id_fkey'
    ) then
      alter table public.pay_batches
        add constraint pay_batches_monzo_confirmed_by_user_id_fkey
        foreign key (monzo_confirmed_by_user_id) references public.tms_users(id)
        on delete set null;
    end if;
  end if;
end$$;

create index if not exists idx_pay_batches_pay_date on public.pay_batches (pay_date desc);
create index if not exists idx_pay_batches_status   on public.pay_batches (status);

create table if not exists public.pay_batch_candidates (
  id uuid primary key default gen_random_uuid(),
  pay_batch_id uuid not null,
  candidate_id uuid not null,

  candidate_tms_ref text,
  candidate_display_name text,

  paye_state text null, -- PENDING_NET|READY|SETTLED (null for non-PAYE)
  mismatch_settlement_choice text null, -- SETTLE_VIA_PAYE|SETTLE_VIA_UMBRELLA

  gross_preview numeric(12,2) null,
  net_bank_amount numeric(12,2) null,
  debt_created numeric(12,2) null,
  loan_repayment_taken numeric(12,2) null
);

do $$
begin
  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batch_candidates'
      and c.conname='pay_batch_candidates_pay_batch_id_fkey'
  ) then
    alter table public.pay_batch_candidates
      add constraint pay_batch_candidates_pay_batch_id_fkey
      foreign key (pay_batch_id) references public.pay_batches(id) on delete cascade;
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batch_candidates'
      and c.conname='pay_batch_candidates_candidate_id_fkey'
  ) then
    alter table public.pay_batch_candidates
      add constraint pay_batch_candidates_candidate_id_fkey
      foreign key (candidate_id) references public.candidates(id) on delete restrict;
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batch_candidates'
      and c.conname='pay_batch_candidates_paye_state_check'
  ) then
    alter table public.pay_batch_candidates
      add constraint pay_batch_candidates_paye_state_check
      check (paye_state is null or paye_state in ('PENDING_NET','READY','SETTLED'));
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batch_candidates'
      and c.conname='pay_batch_candidates_mismatch_choice_check'
  ) then
    alter table public.pay_batch_candidates
      add constraint pay_batch_candidates_mismatch_choice_check
      check (mismatch_settlement_choice is null or mismatch_settlement_choice in ('SETTLE_VIA_PAYE','SETTLE_VIA_UMBRELLA'));
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batch_candidates'
      and c.conname='pay_batch_candidates_net_bank_nonneg_chk'
  ) then
    alter table public.pay_batch_candidates
      add constraint pay_batch_candidates_net_bank_nonneg_chk
      check (net_bank_amount is null or net_bank_amount >= 0);
  end if;
end$$;

create index if not exists idx_pay_batch_candidates_batch
  on public.pay_batch_candidates (pay_batch_id);

create index if not exists idx_pay_batch_candidates_candidate
  on public.pay_batch_candidates (candidate_id);

create table if not exists public.pay_batch_items (
  id uuid primary key default gen_random_uuid(),
  pay_batch_candidate_id uuid not null,

  item_type text not null,

  timesheet_id uuid null,
  segment_key text null,
  source_ref text null,

  description text null,

  amount_ex_vat  numeric(12,2) null,
  amount_vat     numeric(12,2) null,
  amount_inc_vat numeric(12,2) null,

  pay_channel text not null, -- PAYE|UMBRELLA
  umbrella_id uuid null,

  bank_reference text null,
  revolut_transaction_id text null,
  revolut_transaction_state text null
);

do $$
begin
  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batch_items'
      and c.conname='pay_batch_items_candidate_fkey'
  ) then
    alter table public.pay_batch_items
      add constraint pay_batch_items_candidate_fkey
      foreign key (pay_batch_candidate_id) references public.pay_batch_candidates(id) on delete cascade;
  end if;

  if to_regclass('public.timesheets') is not null then
    if not exists (
      select 1 from pg_constraint c
      join pg_class t on t.oid=c.conrelid
      join pg_namespace n on n.oid=t.relnamespace
      where n.nspname='public' and t.relname='pay_batch_items'
        and c.conname='pay_batch_items_timesheet_id_fkey'
    ) then
      alter table public.pay_batch_items
        add constraint pay_batch_items_timesheet_id_fkey
        foreign key (timesheet_id) references public.timesheets(timesheet_id) on delete set null;
    end if;
  end if;

  if to_regclass('public.umbrellas') is not null then
    if not exists (
      select 1 from pg_constraint c
      join pg_class t on t.oid=c.conrelid
      join pg_namespace n on n.oid=t.relnamespace
      where n.nspname='public' and t.relname='pay_batch_items'
        and c.conname='pay_batch_items_umbrella_id_fkey'
    ) then
      alter table public.pay_batch_items
        add constraint pay_batch_items_umbrella_id_fkey
        foreign key (umbrella_id) references public.umbrellas(id) on delete set null;
    end if;
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batch_items'
      and c.conname='pay_batch_items_pay_channel_check'
  ) then
    alter table public.pay_batch_items
      add constraint pay_batch_items_pay_channel_check
      check (pay_channel in ('PAYE','UMBRELLA'));
  end if;
end$$;

create index if not exists idx_pay_batch_items_candidate
  on public.pay_batch_items (pay_batch_candidate_id);

create index if not exists idx_pay_batch_items_timesheet
  on public.pay_batch_items (timesheet_id);

create table if not exists public.timesheet_pay_state (
  timesheet_id uuid primary key,
  last_settled_snapshot_json jsonb not null,
  last_settled_signature text not null,
  last_settled_pay_batch_id uuid null,
  last_settled_at_utc timestamptz null
);

do $$
begin
  if to_regclass('public.timesheets') is not null then
    if not exists (
      select 1 from pg_constraint c
      join pg_class t on t.oid=c.conrelid
      join pg_namespace n on n.oid=t.relnamespace
      where n.nspname='public' and t.relname='timesheet_pay_state'
        and c.conname='timesheet_pay_state_timesheet_id_fkey'
    ) then
      alter table public.timesheet_pay_state
        add constraint timesheet_pay_state_timesheet_id_fkey
        foreign key (timesheet_id) references public.timesheets(timesheet_id) on delete cascade;
    end if;
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='timesheet_pay_state'
      and c.conname='timesheet_pay_state_last_batch_fkey'
  ) then
    alter table public.timesheet_pay_state
      add constraint timesheet_pay_state_last_batch_fkey
      foreign key (last_settled_pay_batch_id) references public.pay_batches(id) on delete set null;
  end if;
end$$;

create index if not exists idx_timesheet_pay_state_last_batch
  on public.timesheet_pay_state (last_settled_pay_batch_id);

create table if not exists public.timesheet_pay_state_history (
  id uuid primary key default gen_random_uuid(),
  timesheet_id uuid not null,
  pay_batch_id uuid not null,
  settled_at_utc timestamptz not null default now(),
  snapshot_json jsonb not null,
  signature text not null
);

do $$
begin
  if to_regclass('public.timesheets') is not null then
    if not exists (
      select 1 from pg_constraint c
      join pg_class t on t.oid=c.conrelid
      join pg_namespace n on n.oid=t.relnamespace
      where n.nspname='public' and t.relname='timesheet_pay_state_history'
        and c.conname='timesheet_pay_state_history_timesheet_fkey'
    ) then
      alter table public.timesheet_pay_state_history
        add constraint timesheet_pay_state_history_timesheet_fkey
        foreign key (timesheet_id) references public.timesheets(timesheet_id) on delete cascade;
    end if;
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='timesheet_pay_state_history'
      and c.conname='timesheet_pay_state_history_batch_fkey'
  ) then
    alter table public.timesheet_pay_state_history
      add constraint timesheet_pay_state_history_batch_fkey
      foreign key (pay_batch_id) references public.pay_batches(id) on delete cascade;
    end if;
end$$;

create index if not exists idx_timesheet_pay_state_history_ts
  on public.timesheet_pay_state_history (timesheet_id, settled_at_utc desc);

create table if not exists public.pay_batch_paye_net_inputs (
  id uuid primary key default gen_random_uuid(),
  pay_batch_candidate_id uuid not null,
  source text not null, -- SAGE_IMPORT|MANUAL_ENTRY|CSV_IMPORT
  net_amount numeric(12,2) not null check (net_amount >= 0),
  imported_at_utc timestamptz not null default now(),
  file_name text null,
  file_hash text null
);

do $$
begin
  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batch_paye_net_inputs'
      and c.conname='pay_batch_paye_net_inputs_candidate_fkey'
  ) then
    alter table public.pay_batch_paye_net_inputs
      add constraint pay_batch_paye_net_inputs_candidate_fkey
      foreign key (pay_batch_candidate_id) references public.pay_batch_candidates(id) on delete cascade;
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_batch_paye_net_inputs'
      and c.conname='pay_batch_paye_net_inputs_source_check'
  ) then
    alter table public.pay_batch_paye_net_inputs
      add constraint pay_batch_paye_net_inputs_source_check
      check (source in ('SAGE_IMPORT','MANUAL_ENTRY','CSV_IMPORT'));
  end if;
end$$;

create index if not exists idx_pay_batch_paye_net_inputs_candidate
  on public.pay_batch_paye_net_inputs (pay_batch_candidate_id);

create table if not exists public.pay_bank_transfers (
  id uuid primary key default gen_random_uuid(),
  pay_batch_id uuid not null,

  candidate_id uuid null,
  umbrella_id uuid null,
  pay_channel text not null, -- PAYE|UMBRELLA

  amount numeric(12,2) not null check (amount >= 0),
  currency text not null default 'GBP',

  status text not null default 'PENDING', -- PENDING|COMPLETED|FAILED

  revolut_transaction_id text null,
  revolut_state text null
);

do $$
begin
  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_bank_transfers'
      and c.conname='pay_bank_transfers_batch_fkey'
  ) then
    alter table public.pay_bank_transfers
      add constraint pay_bank_transfers_batch_fkey
      foreign key (pay_batch_id) references public.pay_batches(id) on delete cascade;
  end if;

  if to_regclass('public.candidates') is not null then
    if not exists (
      select 1 from pg_constraint c
      join pg_class t on t.oid=c.conrelid
      join pg_namespace n on n.oid=t.relnamespace
      where n.nspname='public' and t.relname='pay_bank_transfers'
        and c.conname='pay_bank_transfers_candidate_fkey'
    ) then
      alter table public.pay_bank_transfers
        add constraint pay_bank_transfers_candidate_fkey
        foreign key (candidate_id) references public.candidates(id) on delete set null;
    end if;
  end if;

  if to_regclass('public.umbrellas') is not null then
    if not exists (
      select 1 from pg_constraint c
      join pg_class t on t.oid=c.conrelid
      join pg_namespace n on n.oid=t.relnamespace
      where n.nspname='public' and t.relname='pay_bank_transfers'
        and c.conname='pay_bank_transfers_umbrella_fkey'
    ) then
      alter table public.pay_bank_transfers
        add constraint pay_bank_transfers_umbrella_fkey
        foreign key (umbrella_id) references public.umbrellas(id) on delete set null;
    end if;
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_bank_transfers'
      and c.conname='pay_bank_transfers_pay_channel_check'
  ) then
    alter table public.pay_bank_transfers
      add constraint pay_bank_transfers_pay_channel_check
      check (pay_channel in ('PAYE','UMBRELLA'));
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_bank_transfers'
      and c.conname='pay_bank_transfers_status_check'
  ) then
    alter table public.pay_bank_transfers
      add constraint pay_bank_transfers_status_check
      check (status in ('PENDING','COMPLETED','FAILED'));
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_bank_transfers'
      and c.conname='pay_bank_transfers_payee_present_chk'
  ) then
    alter table public.pay_bank_transfers
      add constraint pay_bank_transfers_payee_present_chk
      check (candidate_id is not null or umbrella_id is not null);
  end if;
end$$;

create index if not exists idx_pay_bank_transfers_batch
  on public.pay_bank_transfers (pay_batch_id);

create index if not exists idx_pay_bank_transfers_status
  on public.pay_bank_transfers (status);

create table if not exists public.pay_advance_patches (
  id uuid primary key default gen_random_uuid(),
  advance_id uuid not null,
  pay_batch_id uuid not null,

  old_outstanding_amount numeric(12,2) null,
  new_outstanding_amount numeric(12,2) null,

  old_schedule_json jsonb null,
  new_schedule_json jsonb null,

  old_next_due_week_start date null,
  new_next_due_week_start date null,

  patched_at_utc timestamptz not null default now()
);

do $$
begin
  if to_regclass('public.pay_advances') is not null then
    if not exists (
      select 1 from pg_constraint c
      join pg_class t on t.oid=c.conrelid
      join pg_namespace n on n.oid=t.relnamespace
      where n.nspname='public' and t.relname='pay_advance_patches'
        and c.conname='pay_advance_patches_advance_fkey'
    ) then
      alter table public.pay_advance_patches
        add constraint pay_advance_patches_advance_fkey
        foreign key (advance_id) references public.pay_advances(id) on delete restrict;
    end if;
  end if;

  if not exists (
    select 1 from pg_constraint c
    join pg_class t on t.oid=c.conrelid
    join pg_namespace n on n.oid=t.relnamespace
    where n.nspname='public' and t.relname='pay_advance_patches'
      and c.conname='pay_advance_patches_batch_fkey'
  ) then
    alter table public.pay_advance_patches
      add constraint pay_advance_patches_batch_fkey
      foreign key (pay_batch_id) references public.pay_batches(id) on delete cascade;
  end if;
end$$;

create index if not exists idx_pay_advance_patches_advance
  on public.pay_advance_patches (advance_id, patched_at_utc desc);

create index if not exists idx_pay_advance_patches_batch
  on public.pay_advance_patches (pay_batch_id);

-- =========================================================
-- PostgREST schema reload (Supabase) - safe wrapper
-- =========================================================
do $$
begin
  perform pg_notify('pgrst', 'reload schema');
exception when others then
  -- Ignore notification issues so migration never fails on rerun.
  null;
end$$;

commit;



begin;

-- =========================================================
-- Helpers: Ledger upsert from invoices
-- =========================================================

create or replace function public.id_ledger_upsert_from_invoice_row(
  p_invoice_id uuid,
  p_set_zero boolean default false,
  p_invoice_no text default null,
  p_status_text text default null,
  p_type_text text default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_inv record;
  v_invoice_no text;
  v_status_text text;
  v_type_text text;

  v_ex numeric := 0;
  v_vat numeric := 0;
  v_inc numeric := 0;
begin
  -- Prefer reading the current invoices row when present (normal path).
  select
    i.invoice_no,
    i.status::text as status_text,
    i.type::text as type_text,
    coalesce(i.subtotal_ex_vat,0)::numeric as subtotal_ex_vat,
    coalesce(i.vat_amount,0)::numeric as vat_amount,
    coalesce(i.total_inc_vat,0)::numeric as total_inc_vat
  into v_inv
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  if found then
    v_invoice_no := v_inv.invoice_no;
    v_status_text := v_inv.status_text;
    v_type_text := v_inv.type_text;

    if p_set_zero then
      v_ex := 0; v_vat := 0; v_inc := 0;
    else
      v_ex := coalesce(v_inv.subtotal_ex_vat,0);
      v_vat := coalesce(v_inv.vat_amount,0);
      v_inc := coalesce(v_inv.total_inc_vat,0);
    end if;
  else
    -- Invoice row not found (e.g. already deleted): use provided snapshots and zero totals.
    v_invoice_no := p_invoice_no;
    v_status_text := p_status_text;
    v_type_text := p_type_text;
    v_ex := 0; v_vat := 0; v_inc := 0;
  end if;

  insert into public.id_invoice_ledger (
    invoice_id,
    invoice_number,
    invoice_status,
    invoice_type,
    current_ex_vat,
    current_vat,
    current_inc_vat,
    updated_at_utc
  )
  values (
    p_invoice_id,
    v_invoice_no,
    v_status_text,
    v_type_text,
    round(coalesce(v_ex,0)::numeric,2),
    round(coalesce(v_vat,0)::numeric,2),
    round(coalesce(v_inc,0)::numeric,2),
    now()
  )
  on conflict (invoice_id) do update
  set
    invoice_number   = excluded.invoice_number,
    invoice_status   = excluded.invoice_status,
    invoice_type     = excluded.invoice_type,
    current_ex_vat   = excluded.current_ex_vat,
    current_vat      = excluded.current_vat,
    current_inc_vat  = excluded.current_inc_vat,
    updated_at_utc   = excluded.updated_at_utc;

end;
$$;

-- Recompute totals then upsert ledger.
create or replace function public.id_ledger_recompute_and_sync_invoice(p_invoice_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  -- Canonical totals recompute; this also clears invoice_pdf_r2_key. :contentReference[oaicite:3]{index=3}
  begin
    perform public.invoice_recompute_totals(p_invoice_id);
  exception when others then
    -- If invoice missing or recompute fails, fall back to a safe ledger upsert with zeros.
    -- (We do NOT raise: ledger must not break invoice_lines operations.)
    perform public.id_ledger_upsert_from_invoice_row(p_invoice_id, true, null, null, null);
    return;
  end;

  perform public.id_ledger_upsert_from_invoice_row(p_invoice_id, false, null, null, null);
end;
$$;

-- Metadata-only sync (no recompute). Used for invoices INSERT/UPDATE to keep status/type/invoice_no current.
create or replace function public.id_ledger_sync_invoice_metadata(p_invoice_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.id_ledger_upsert_from_invoice_row(p_invoice_id, false, null, null, null);
end;
$$;

-- =========================================================
-- A2.1 invoice_lines triggers (AFTER INSERT/UPDATE/DELETE)
-- Statement-level triggers with transition tables so we recompute ONCE per invoice per statement.
-- =========================================================

create or replace function public.trg_id_invoice_lines_ai_stmt()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invoice_id uuid;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return null;
  end if;

  for v_invoice_id in
    select distinct nr.invoice_id
    from new_rows nr
    where nr.invoice_id is not null
  loop
    perform public.id_ledger_recompute_and_sync_invoice(v_invoice_id);
  end loop;

  return null;
end;
$$;

create or replace function public.trg_id_invoice_lines_au_stmt()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invoice_id uuid;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return null;
  end if;

  for v_invoice_id in
    with ids as (
      select invoice_id from new_rows where invoice_id is not null
      union
      select invoice_id from old_rows where invoice_id is not null
    )
    select distinct invoice_id from ids
  loop
    perform public.id_ledger_recompute_and_sync_invoice(v_invoice_id);
  end loop;

  return null;
end;
$$;

create or replace function public.trg_id_invoice_lines_ad_stmt()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invoice_id uuid;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return null;
  end if;

  for v_invoice_id in
    select distinct orw.invoice_id
    from old_rows orw
    where orw.invoice_id is not null
  loop
    perform public.id_ledger_recompute_and_sync_invoice(v_invoice_id);
  end loop;

  return null;
end;
$$;

-- Drop+recreate triggers (safe to rerun)
drop trigger if exists trg_id_invoice_lines_ai on public.invoice_lines;
drop trigger if exists trg_id_invoice_lines_au on public.invoice_lines;
drop trigger if exists trg_id_invoice_lines_ad on public.invoice_lines;

-- Create triggers only if invoice_lines exists
do $$
begin
  if to_regclass('public.invoice_lines') is not null then

    create trigger trg_id_invoice_lines_ai
    after insert on public.invoice_lines
    referencing new table as new_rows
    for each statement
    execute function public.trg_id_invoice_lines_ai_stmt();

    create trigger trg_id_invoice_lines_au
    after update on public.invoice_lines
    referencing old table as old_rows new table as new_rows
    for each statement
    execute function public.trg_id_invoice_lines_au_stmt();

    create trigger trg_id_invoice_lines_ad
    after delete on public.invoice_lines
    referencing old table as old_rows
    for each statement
    execute function public.trg_id_invoice_lines_ad_stmt();

  end if;
end$$;

-- =========================================================
-- A2.2 invoices metadata sync trigger (AFTER INSERT/UPDATE)
-- Keeps invoice_no/status/type in ledger even when lines unchanged.
-- =========================================================

create or replace function public.trg_id_invoices_meta_aiu()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return new;
  end if;

  perform public.id_ledger_sync_invoice_metadata(new.id);
  return new;
end;
$$;

drop trigger if exists trg_id_invoices_meta_aiu on public.invoices;

do $$
begin
  if to_regclass('public.invoices') is not null then
    create trigger trg_id_invoices_meta_aiu
    after insert or update on public.invoices
    for each row
    execute function public.trg_id_invoices_meta_aiu();
  end if;
end$$;

-- =========================================================
-- A2.3 invoices delete semantics (AFTER DELETE)
-- Keep ledger row but set current_* = 0 (audit-safe).
-- =========================================================

create or replace function public.trg_id_invoices_after_delete()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if to_regclass('public.id_invoice_ledger') is null then
    return old;
  end if;

  -- Force ledger current_* to zero, keep snapshots from OLD row.
  perform public.id_ledger_upsert_from_invoice_row(
    old.id,
    true,
    old.invoice_no,
    old.status::text,
    old.type::text
  );

  return old;
end;
$$;

drop trigger if exists trg_id_invoices_ad on public.invoices;

do $$
begin
  if to_regclass('public.invoices') is not null then
    create trigger trg_id_invoices_ad
    after delete on public.invoices
    for each row
    execute function public.trg_id_invoices_after_delete();
  end if;
end$$;

-- Optional PostgREST schema reload (safe wrapper)
do $$
begin
  perform pg_notify('pgrst', 'reload schema');
exception when others then
  null;
end$$;

commit;
