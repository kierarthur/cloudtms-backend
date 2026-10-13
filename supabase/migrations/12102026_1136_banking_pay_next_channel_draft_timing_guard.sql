-- DR27 S02 correction. Prior1125 is locked; successor strengthens NULL truth.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_run_channel_totals
 drop constraint bpay_next_channel_draft_timing_shape,
 add constraint bpay_next_channel_draft_timing_shape check
  (((execution_route is null and execution_pay_date is null and scheduled_at_utc is null)
   or (execution_route in ('BANK_CSV','EXTERNAL') and execution_pay_date is not null and scheduled_at_utc is null)
   or (execution_route='SCHEDULED' and execution_pay_date is not null and scheduled_at_utc is not null)) is true),
 add constraint bpay_next_channel_draft_execution_date_finite check(isfinite(execution_pay_date));
commit;
