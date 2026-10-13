-- WB-29 Ready chosen display/date repair only; four additive native metadata columns.
-- No business backfill, money table, public trigger, protocol or summary change.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_workbench_group
  add column chosen_display_date_sensitive boolean not null default true,
  add column chosen_display_pay_date date,
  add constraint bpay_next_wb_chosen_display_date_finite check(chosen_display_pay_date is null or isfinite(chosen_display_pay_date));
alter table private.bpay_next_workbench_revision
  add column chosen_rollover_seek_date date,
  add column chosen_rollover_seek_work_id uuid,
  add constraint bpay_next_wb_chosen_rollover_seek check(
    (chosen_rollover_seek_work_id is not null or chosen_rollover_seek_date is null)
    and (chosen_rollover_seek_date is null or isfinite(chosen_rollover_seek_date)));
create index bpay_next_wb_chosen_display_due_idx
  on private.bpay_next_workbench_group(chosen_display_pay_date nulls first,work_id)
  where group_kind='WORK' and chosen_display_date_sensitive and complete and not dirty
    and progress_state='CURRENT' and (ready_count>0 or action_count>0);
commit;
