-- PF-04 D01: bounded housekeeping cursor only, no financial authority.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_terminal_cleanup_clock (
  id smallint primary key check (id=1),
  lane text not null default 'SELECTION' check (lane in ('SELECTION','JOB')),
  selection_run_cursor uuid,
  selection_page_cursor bigint,
  job_cursor uuid,
  check ((selection_run_cursor is null)=(selection_page_cursor is null)),
  check (selection_page_cursor is null or selection_page_cursor>0)
);
alter table private.bpay_next_terminal_cleanup_clock owner to postgres;
alter table private.bpay_next_terminal_cleanup_clock enable row level security;
revoke all on private.bpay_next_terminal_cleanup_clock from public,anon,authenticated,service_role;
insert into private.bpay_next_terminal_cleanup_clock(id) values(1);
commit;
