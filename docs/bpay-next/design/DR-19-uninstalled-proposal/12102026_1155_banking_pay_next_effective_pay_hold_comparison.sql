-- DR19/BD143: effective hold identity only. No historical baseline backfill.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_work add column effective_pay_hold_revision bigint not null default 0
 check(effective_pay_hold_revision>=0);
alter table private.bpay_next_work_choice add column effective_pay_hold_revision bigint
 check(effective_pay_hold_revision is null or effective_pay_hold_revision>=0);
commit;
