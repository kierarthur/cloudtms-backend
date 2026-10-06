-- One unpaid returned-cash instruction, never the original payroll group.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_reissue_cancel_request (
  command_id uuid primary key references private.bpay_next_command(id) on delete restrict,
  transfer_id uuid not null references private.bpay_next_transfer(id) on delete restrict,
  return_cash_id uuid not null references private.bpay_next_return_cash(id) on delete restrict,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  amount private.bpay_next_penny_amount not null check(amount>0),
  status text not null check(status in ('REQUESTED','CANCELLED','BLOCKED')),
  blocked_code text check(octet_length(blocked_code) between 1 and 128),
  accepted_at_utc timestamptz not null default transaction_timestamp(),
  completed_at_utc timestamptz,
  foreign key(command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
  foreign key(transfer_id,run_worker_id) references private.bpay_next_transfer(id,run_worker_id) on delete restrict,
  foreign key(run_worker_id,candidate_id) references private.bpay_next_run_worker(id,candidate_id) on delete restrict,
  foreign key(return_cash_id,candidate_id) references private.bpay_next_return_cash(id,candidate_id) on delete restrict,
  check((status='REQUESTED')=(completed_at_utc is null)),
  check((status='BLOCKED')=(blocked_code is not null))
);
create unique index bpay_next_reissue_cancel_pending_idx on private.bpay_next_reissue_cancel_request(transfer_id) where status='REQUESTED';
create index bpay_next_reissue_cancel_cash_idx on private.bpay_next_reissue_cancel_request(return_cash_id,command_id);
create index bpay_next_reissue_cancel_worker_idx on private.bpay_next_reissue_cancel_request(run_worker_id,command_id);
create index bpay_next_reissue_cancel_actor_idx on private.bpay_next_reissue_cancel_request(actor_user_id,command_id);
alter table private.bpay_next_reissue_cancel_request owner to postgres;
revoke all on private.bpay_next_reissue_cancel_request from public,anon,authenticated,service_role;
commit;
