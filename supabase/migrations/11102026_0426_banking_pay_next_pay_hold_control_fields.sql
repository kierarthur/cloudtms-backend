-- WB40 current control and original Rule B choice evidence only; no backfill.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_work
 add column pay_hold_control_version bigint not null default 0 check(pay_hold_control_version>=0),
 add column pay_hold_changed_at_utc timestamptz,
 add column pay_hold_physical_timesheet_id uuid,
 add column pay_hold_financial_id uuid,
 add column pay_hold boolean not null default false,
 add column pay_hold_reason text;
alter table private.bpay_next_work_choice
 add column pay_hold_control_version bigint not null default 0 check(pay_hold_control_version>=0),
 add column pay_hold_changed_at_utc timestamptz,
 add column pay_hold_physical_timesheet_id uuid,
 add column pay_hold_financial_id uuid,
 add column pay_hold boolean not null default false,
 add column pay_hold_reason text;

commit;
