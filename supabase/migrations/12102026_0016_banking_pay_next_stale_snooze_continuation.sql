-- One-time CloudTMS schema/data migration: banking_pay_next_stale_snooze_continuation
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- WB36/WB44 owner A2 / BD128: original POSITION_APPLY continuation only.
alter table private.bpay_next_publication
  drop constraint bpay_next_publication_phase_check,
  add constraint bpay_next_publication_phase_check
    check (phase in ('NEW','REMOVED','SNOOZE','DONE'));
create index bpay_next_snooze_active_work_uuid_idx
  on private.bpay_next_snooze(work_id,id) where active and work_id is not null;

commit;
