-- SQL ADDITION ONLY. Do not alter Simple Modal lifecycle RPCs.
-- Purpose: durable Bulk Authorise/Unauthorise operation state for bounded backend orchestration.
-- Policy X: operational lifecycle orchestration metadata only; no payment economics, VAT, PAYE, Banking Pay economics, settlement, routing, or draft authority changes.

create table if not exists public.timesheet_lifecycle_bulk_operations (
  id uuid primary key default gen_random_uuid(),
  action text not null check (action in ('AUTHORISE', 'UNAUTHORISE')),
  status text not null default 'QUEUED' check (status in ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
  requested_count integer not null default 0 check (requested_count >= 0),
  success_count integer not null default 0 check (success_count >= 0),
  failure_count integer not null default 0 check (failure_count >= 0),
  created_by_user_id uuid null,
  context text not null default 'bulk_authorise',
  request_json jsonb not null default '{}'::jsonb,
  progress_json jsonb not null default '{}'::jsonb,
  result_json jsonb not null default '{}'::jsonb,
  error_json jsonb not null default '{}'::jsonb,
  run_after_utc timestamptz not null default now(),
  started_at_utc timestamptz null,
  completed_at_utc timestamptz null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now()
);

create table if not exists public.timesheet_lifecycle_bulk_operation_items (
  id uuid primary key default gen_random_uuid(),
  operation_id uuid not null references public.timesheet_lifecycle_bulk_operations(id) on delete cascade,
  ordinal integer not null check (ordinal >= 0),
  action text not null check (action in ('AUTHORISE', 'UNAUTHORISE')),
  row_key text null,
  timesheet_id uuid not null,
  current_timesheet_id uuid null,
  requested_timesheet_id uuid null,
  expected_timesheet_id uuid null,
  contract_week_id uuid null,
  expected_row_signature text null,
  status text not null default 'QUEUED' check (status in ('QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED', 'RETRY', 'CANCELLED')),
  attempt_count integer not null default 0 check (attempt_count >= 0),
  result_json jsonb not null default '{}'::jsonb,
  error_json jsonb not null default '{}'::jsonb,
  affected_rows_json jsonb not null default '[]'::jsonb,
  locked_at_utc timestamptz null,
  started_at_utc timestamptz null,
  completed_at_utc timestamptz null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  constraint timesheet_lifecycle_bulk_operation_items_unique_ordinal unique (operation_id, ordinal)
);

create index if not exists idx_tlbo_status_run_after
  on public.timesheet_lifecycle_bulk_operations (status, run_after_utc, created_at_utc);

create index if not exists idx_tlbo_created_by
  on public.timesheet_lifecycle_bulk_operations (created_by_user_id, created_at_utc desc);

create index if not exists idx_tlboi_operation_status_ordinal
  on public.timesheet_lifecycle_bulk_operation_items (operation_id, status, ordinal);

create index if not exists idx_tlboi_timesheet
  on public.timesheet_lifecycle_bulk_operation_items (timesheet_id, created_at_utc desc);

comment on table public.timesheet_lifecycle_bulk_operations is
  'Operational metadata for bounded Bulk Authorise/Unauthorise orchestration. It stores no economics and must call gold lifecycle RPC handlers per item.';

comment on table public.timesheet_lifecycle_bulk_operation_items is
  'Per-timesheet expected identity/signature and result rows for bulk lifecycle orchestration. Expected signatures are captured from the selected user view; workers must not recompute them to bypass stale-state guards.';
