-- CloudTMS Remittances + Pay Batch Breakdown (idempotent / safe to re-run)
-- Adds:
--  - public.pay_batch_item_breakdowns (new table)
--  - settings_defaults: remittances_detailed_breakdown, remittance_receive_when_umbrella_paid
--  - candidates: remittance_overrides_enabled, remittance_receive_enabled, remittances_detailed_breakdown, remittance_receive_when_umbrella_paid
--  - umbrellas: remittance_overrides_enabled, remittances_detailed_breakdown

begin;

-- gen_random_uuid() is already used in your schema, but this is safe to re-run.
create extension if not exists pgcrypto;

-- -------------------------------------------------------------------
-- A1) New table: public.pay_batch_item_breakdowns
-- -------------------------------------------------------------------
create table if not exists public.pay_batch_item_breakdowns (
  id uuid primary key default gen_random_uuid(),
  pay_batch_item_id uuid not null,
  line_kind text not null,
  bucket_code text null,
  unit_name text null,
  units numeric null,
  rate numeric null,
  amount_ex_vat numeric not null,
  amount_vat numeric not null default 0,
  amount_inc_vat numeric not null,
  meta_json jsonb not null default '{}'::jsonb,
  created_at_utc timestamptz not null default now(),
  constraint pay_batch_item_breakdowns_pay_batch_item_id_fkey
    foreign key (pay_batch_item_id)
    references public.pay_batch_items(id)
    on delete cascade
);

-- Ensure columns exist even if the table was created previously with a partial definition.
alter table public.pay_batch_item_breakdowns
  add column if not exists id uuid default gen_random_uuid();

alter table public.pay_batch_item_breakdowns
  add column if not exists pay_batch_item_id uuid;

alter table public.pay_batch_item_breakdowns
  add column if not exists line_kind text;

alter table public.pay_batch_item_breakdowns
  add column if not exists bucket_code text;

alter table public.pay_batch_item_breakdowns
  add column if not exists unit_name text;

alter table public.pay_batch_item_breakdowns
  add column if not exists units numeric;

alter table public.pay_batch_item_breakdowns
  add column if not exists rate numeric;

alter table public.pay_batch_item_breakdowns
  add column if not exists amount_ex_vat numeric;

alter table public.pay_batch_item_breakdowns
  add column if not exists amount_vat numeric;

alter table public.pay_batch_item_breakdowns
  add column if not exists amount_inc_vat numeric;

alter table public.pay_batch_item_breakdowns
  add column if not exists meta_json jsonb default '{}'::jsonb;

alter table public.pay_batch_item_breakdowns
  add column if not exists created_at_utc timestamptz default now();

-- Index
create index if not exists idx_pbib_item
  on public.pay_batch_item_breakdowns (pay_batch_item_id);

-- -------------------------------------------------------------------
-- A2) settings_defaults new columns
-- -------------------------------------------------------------------
alter table public.settings_defaults
  add column if not exists remittances_detailed_breakdown boolean not null default false;

alter table public.settings_defaults
  add column if not exists remittance_receive_when_umbrella_paid boolean not null default false;

-- -------------------------------------------------------------------
-- A3) candidates new columns
-- -------------------------------------------------------------------
alter table public.candidates
  add column if not exists remittance_overrides_enabled boolean not null default false;

alter table public.candidates
  add column if not exists remittance_receive_enabled boolean not null default false;

alter table public.candidates
  add column if not exists remittances_detailed_breakdown boolean not null default false;

alter table public.candidates
  add column if not exists remittance_receive_when_umbrella_paid boolean not null default false;

-- -------------------------------------------------------------------
-- A4) umbrellas new columns
-- -------------------------------------------------------------------
alter table public.umbrellas
  add column if not exists remittance_overrides_enabled boolean not null default false;

alter table public.umbrellas
  add column if not exists remittances_detailed_breakdown boolean not null default false;

commit;
