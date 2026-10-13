-- WB-37: native DUE reason, bounded receipt lookup and two fair seek tuples.
-- Additive private metadata only; no rows, finance, public triggers or new tables.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_snooze add column cleared_reason text;
alter table private.bpay_next_snooze add constraint bpay_next_snooze_due_reason_check
  check(cleared_reason is null or (cleared_reason='DUE' and not active and until_date is not null));
create index bpay_next_snooze_event_request_idx
  on private.bpay_next_snooze_event(request_id,id);

-- These are the last visited native keys, not foreign identities or a clock.
-- A failed/busy scope stays due and is retried on the next circular sweep.
alter table private.bpay_next_workbench_revision
  add column snooze_due_seek_date date,
  add column snooze_due_seek_id uuid,
  add column week_rollover_seek_date date,
  add column week_rollover_seek_id uuid,
  add constraint bpay_next_snooze_due_seek_pair
    check((snooze_due_seek_date is null)=(snooze_due_seek_id is null)),
  add constraint bpay_next_week_rollover_seek_pair
    check((week_rollover_seek_date is null)=(week_rollover_seek_id is null));

commit;
