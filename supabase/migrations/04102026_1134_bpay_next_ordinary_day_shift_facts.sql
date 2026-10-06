-- Ordinary SEGMENTS use one stable financial DAY obligation, retaining each
-- original segment as immutable explanatory detail. No history is backfilled.
-- All existing Source/aggregate children keep NULL in these additive columns.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_shift_detail
  -- The effective authorised segment amount: excluded=>0, otherwise the
  -- existing producer's validated exact money. Never read excluded pay_amount.
  add column segment_pay_ex_vat numeric(18,2),
  add column pay_excluded boolean,
  add column hours_day numeric(18,6),
  add column hours_night numeric(18,6),
  add column hours_sat numeric(18,6),
  add column hours_sun numeric(18,6),
  add column hours_bh numeric(18,6),
  add constraint bpay_next_shift_ordinary_bucket_shape check (
    (pay_excluded is null and segment_pay_ex_vat is null
     and hours_day is null and hours_night is null and hours_sat is null
     and hours_sun is null and hours_bh is null)
    or
    (pay_excluded is not null and segment_pay_ex_vat is not null and hours_day is not null
     and hours_night is not null and hours_sat is not null
     and hours_sun is not null and hours_bh is not null
     and hours_day>=0 and hours_night>=0 and hours_sat>=0
     and hours_sun>=0 and hours_bh>=0
     and (not pay_excluded or segment_pay_ex_vat=0))
  );

-- Existing insert-only revision child guard protects every added field after
-- insertion; no new table, money writer, browser privilege or fallback exists.

commit;
