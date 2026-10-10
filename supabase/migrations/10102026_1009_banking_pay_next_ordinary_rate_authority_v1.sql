-- One-time CloudTMS schema/data migration: banking_pay_next_ordinary_rate_authority_v1
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- WB-01: unknown unused authority is represented by an actual NULL row.
-- Existing rows, monetary positions and all immutable child guards stay intact.
alter table private.bpay_next_rate_schedule
  drop constraint bpay_next_rate_schedule_check,
  drop constraint bpay_next_rate_schedule_rate_code_check,
  add constraint bpay_next_rate_schedule_rate_code_check
    check (pg_catalog.char_length(rate_code) between 1 and 245),
  add column approved_pay_rate numeric(18,6),
  add column approved_charge_rate numeric(18,6);

-- Exact approved source provenance, captured only by the genuine producer.
-- No historic identity is reconstructed and no target VAT is asserted.
alter table private.bpay_next_work_revision
  add column source_umbrella_id uuid references public.umbrellas(id) on delete restrict,
  add column source_rate_authority_kind text
    check (source_rate_authority_kind is null or
      pg_catalog.char_length(source_rate_authority_kind) between 1 and 128),
  add column source_rate_override_id uuid,
  add column source_rate_default_id uuid;

commit;
