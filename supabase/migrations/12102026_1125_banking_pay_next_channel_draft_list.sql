-- DR27 / BD142. Same DR16 money owner; only confirmed directory metadata.
\set ON_ERROR_STOP on
begin;
do $census$
begin
 if exists(select 1 from private.bpay_next_pay_run where confirmed_at_utc is not null) then
  raise exception using errcode='23514',message='BPAY_NEXT_CHANNEL_DRAFT_CENSUS_REQUIRED';
 end if;
end
$census$;
alter table private.bpay_next_run_channel_totals
 add column list_visible boolean not null default false,
 add column created_at_utc timestamptz,
 add column confirmed_at_utc timestamptz,
 add column original_worker_count bigint check(original_worker_count>0),
 add column original_timesheet_count bigint check(original_timesheet_count>=0),
 add column original_gross_ex_vat numeric(38,2),
 add column original_gross_vat numeric(38,2),
 add column original_gross_inc_vat numeric(38,2),
 add column status_group text not null default 'NOT_PAID'
  check(status_group in ('NOT_PAID','WAITING_APPROVAL','PROCESSING','PARTLY_PAID','BANK_UNAVAILABLE','PAID','CANCELLED')),
 add column status_note text check(octet_length(status_note) between 1 and 512),
 add column has_payment_issue boolean not null default false,
 add column execution_route text check(execution_route in ('BANK_CSV','EXTERNAL','SCHEDULED')),
 add column execution_pay_date date,
 add column scheduled_at_utc timestamptz check(isfinite(scheduled_at_utc)),
 add column execution_reference text check(octet_length(execution_reference) between 1 and 256),
 add column change_revision bigint not null default 0 check(change_revision>=0),
 add constraint bpay_next_channel_draft_frozen_shape check
  ((not list_visible and created_at_utc is null and confirmed_at_utc is null
    and original_worker_count is null and original_timesheet_count is null
    and original_gross_ex_vat is null and original_gross_vat is null and original_gross_inc_vat is null)
   or (list_visible and created_at_utc is not null and isfinite(created_at_utc)
    and confirmed_at_utc is not null and isfinite(confirmed_at_utc)
    and original_worker_count is not null and original_timesheet_count is not null
    and original_gross_ex_vat is not null and original_gross_vat is not null
    and original_gross_inc_vat is not null and original_gross_inc_vat=original_gross_ex_vat+original_gross_vat)),
 add constraint bpay_next_channel_draft_timing_shape check
  ((execution_route is null and execution_pay_date is null and scheduled_at_utc is null)
   or (execution_route in ('BANK_CSV','EXTERNAL') and execution_pay_date is not null and scheduled_at_utc is null)
   or (execution_route='SCHEDULED' and execution_pay_date is not null and scheduled_at_utc is not null));
create index bpay_next_channel_draft_created_idx
 on private.bpay_next_run_channel_totals(created_at_utc desc,run_id,pay_channel collate "C") where list_visible;
create index bpay_next_channel_draft_status_idx
 on private.bpay_next_run_channel_totals(status_group,created_at_utc desc,run_id,pay_channel collate "C") where list_visible;
-- Seven counts and a commit-serialized display generation, no money.
create table private.bpay_next_channel_draft_directory (
 id smallint primary key check(id=1),
 generation bigint not null default 0 check(generation>=0),
 not_paid_count bigint not null default 0 check(not_paid_count>=0),
 waiting_approval_count bigint not null default 0 check(waiting_approval_count>=0),
 processing_count bigint not null default 0 check(processing_count>=0),
 partly_paid_count bigint not null default 0 check(partly_paid_count>=0),
 bank_unavailable_count bigint not null default 0 check(bank_unavailable_count>=0),
 paid_count bigint not null default 0 check(paid_count>=0),
 cancelled_count bigint not null default 0 check(cancelled_count>=0)
);
alter table private.bpay_next_channel_draft_directory owner to postgres;
alter table private.bpay_next_channel_draft_directory enable row level security;
revoke all on private.bpay_next_channel_draft_directory from public,anon,authenticated,service_role;
insert into private.bpay_next_channel_draft_directory(id) values(1);
commit;
