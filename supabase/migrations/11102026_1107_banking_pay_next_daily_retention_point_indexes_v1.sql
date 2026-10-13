-- DY06/BD104: only three proved-missing point indexes for retained financial classification.
-- No business rows, table/column, public trigger, grant or money owner change.
\set ON_ERROR_STOP on
begin;
create index bpay_next_work_choice_work_run_idx on private.bpay_next_work_choice(work_id,run_id);
create index bpay_next_daily_retention_advance_ts_idx on public.pay_advances(linked_timesheet_id) where linked_timesheet_id is not null;
create index bpay_next_daily_retention_carry_ts_idx on public.pay_manual_adjustment_carry_forwards(timesheet_id) where timesheet_id is not null;
commit;
