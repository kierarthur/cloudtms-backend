-- One-time CloudTMS schema/data migration: banking_pay_next_case_edit_activity_indexes
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- FC09 bounded native Case activity inspection, including older non-ACTIVE
-- holds whose original transfer remains committed. No data or public triggers.
create index bpay_next_case_hold_case_history_idx on private.bpay_next_case_hold(case_id,id);
create index bpay_next_transfer_case_hold_idx on private.bpay_next_transfer_member(case_hold_id,transfer_id)
 where case_hold_id is not null;

commit;
