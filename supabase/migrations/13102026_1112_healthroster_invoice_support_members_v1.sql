-- One-time CloudTMS schema/data migration: healthroster_invoice_support_members_v1
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- Same-store additive membership. No historical invoice is backfilled from
-- mutable Timesheet/import truth. Existing key, grants and RLS are retained.
alter table public.invoice_hr_source_rows
  add column member_rows_json jsonb not null default '{}'::jsonb;
alter table public.invoice_hr_source_rows
  add constraint invoice_hr_source_member_rows_object_v1
  check (jsonb_typeof(member_rows_json)='object');
comment on column public.invoice_hr_source_rows.member_rows_json is
  'Frozen support rows keyed by exact Timesheet UUID; empty object means membership unproved.';

commit;
