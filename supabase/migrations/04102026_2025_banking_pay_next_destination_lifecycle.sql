-- L22 original-leg posting conservation. No copied money, historical sweep,
-- public grants, or change to ordinary single-leg accounting.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_destination_posting (
  anchor_transfer_id uuid primary key references private.bpay_next_destination_group(anchor_transfer_id) on delete restrict,
  posted_leg_count bigint not null default 0 check (posted_leg_count>=0),
  last_posted_transfer_id uuid,
  completed_at_utc timestamptz,
  check ((posted_leg_count=0)=(last_posted_transfer_id is null)),
  check (completed_at_utc is null or pg_catalog.isfinite(completed_at_utc))
);
create table private.bpay_next_destination_posted_leg (
  original_transfer_id uuid primary key references private.bpay_next_destination_group_leg(transfer_id) on delete restrict,
  anchor_transfer_id uuid not null references private.bpay_next_destination_posting(anchor_transfer_id) on delete restrict,
  job_id uuid not null unique references private.bpay_next_job(id) on delete restrict,
  posting_no bigint not null check (posting_no>0),
  posted_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique(anchor_transfer_id,posting_no),
  unique(anchor_transfer_id,original_transfer_id),
  foreign key(anchor_transfer_id,original_transfer_id) references private.bpay_next_destination_group_leg(anchor_transfer_id,transfer_id) on delete restrict
);
alter table private.bpay_next_destination_posting add constraint bpay_next_destination_last_posted_fk
  foreign key(anchor_transfer_id,last_posted_transfer_id)
  references private.bpay_next_destination_posted_leg(anchor_transfer_id,original_transfer_id) on delete restrict;
-- Whole-Candidate cancellation checks every original leg in <=100-row pages
-- before the existing WORK/CASE release owner can run. Finalisation is paged too.
create table private.bpay_next_destination_cancel (
  command_id uuid primary key references private.bpay_next_case_cancel_binding(command_id) on delete restrict,
  anchor_transfer_id uuid not null references private.bpay_next_destination_group(anchor_transfer_id) on delete restrict,
  expected_leg_count bigint not null check (expected_leg_count>1),
  checked_leg_count bigint not null default 0 check (checked_leg_count>=0),
  cancelled_leg_count bigint not null default 0 check (cancelled_leg_count>=0),
  check_cursor integer check (check_cursor>0),
  cancel_cursor integer check (cancel_cursor>0),
  check (checked_leg_count<=expected_leg_count and cancelled_leg_count<=checked_leg_count),
  check ((checked_leg_count=0)=(check_cursor is null)),
  check ((cancelled_leg_count=0)=(cancel_cursor is null))
);
create index bpay_next_destination_cancel_anchor_idx on private.bpay_next_destination_cancel(anchor_transfer_id,command_id);
alter table private.bpay_next_destination_posting owner to postgres;
alter table private.bpay_next_destination_posted_leg owner to postgres;
alter table private.bpay_next_destination_cancel owner to postgres;
revoke all on table private.bpay_next_destination_posting,private.bpay_next_destination_posted_leg,private.bpay_next_destination_cancel
  from public,anon,authenticated,service_role;

commit;
