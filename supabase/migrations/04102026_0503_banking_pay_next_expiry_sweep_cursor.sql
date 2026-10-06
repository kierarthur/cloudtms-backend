-- One paired key for bounded expiry discovery, not a Candidate sweep or
-- financial snapshot. A lost dispatch reply is revisited on the next cycle.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_expiry_sweep_cursor (
  id smallint primary key check(id=1),
  after_deadline timestamptz,
  after_run_id uuid,
  sweep_through_utc timestamptz,
  check ((after_deadline is null)=(after_run_id is null)),
  check ((after_deadline is null)=(sweep_through_utc is null)),
  check (after_deadline is null or pg_catalog.isfinite(after_deadline)),
  check (sweep_through_utc is null or
    (pg_catalog.isfinite(sweep_through_utc) and after_deadline<=sweep_through_utc))
);
insert into private.bpay_next_expiry_sweep_cursor(id) values(1);
alter table private.bpay_next_expiry_sweep_cursor owner to postgres;
alter table private.bpay_next_expiry_sweep_cursor enable row level security;
revoke all on table private.bpay_next_expiry_sweep_cursor from public,anon,authenticated,service_role;
commit;
