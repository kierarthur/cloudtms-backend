-- One-time CloudTMS schema/data migration: banking_pay_next_shift_query_exclusion
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- SP14 / BD122, owner11Oct: reuse the existing OR47 segment exclusion.
-- Empty additive schema only. Existing immutable snapshot guards remain.
alter table private.bpay_next_run_work_query_snapshot
  add column segment_query_exclusions jsonb,
  add constraint bpay_next_query_segment_exclusions_array
    check(segment_query_exclusions is null or jsonb_typeof(segment_query_exclusions)='array');
alter table private.bpay_next_selected_segment
  add column query_hold_reason text,
  add constraint bpay_next_selected_segment_query_reason
    check(query_hold_reason is null or query_hold_reason in
      ('HEALTHROSTER_NOT_FINALISED','SOURCE_MISSING_OR_NOT_AUTHORISED','SOURCE_HOURS_DIFFER'));

commit;
