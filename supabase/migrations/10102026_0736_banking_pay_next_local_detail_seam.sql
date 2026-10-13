-- SP08/SRQ304: private one-use marker for an actual complete staged Local HEAD.
-- No public trigger, backfill, input protocol, money or generic I1 change.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_local_detail_seam(
  head_id uuid primary key references public.weekly_source_entitlement_heads(id) on delete restrict,
  transaction_id xid8 not null,
  consumed boolean not null default false
);
alter table private.bpay_next_local_detail_seam owner to postgres;
alter table private.bpay_next_local_detail_seam enable row level security;
alter table private.bpay_next_local_detail_seam force row level security;
revoke all on private.bpay_next_local_detail_seam from public,anon,authenticated,service_role;
do $policy$
begin
  execute pg_catalog.format('create policy bpay_next_local_detail_seam_owner_all on private.bpay_next_local_detail_seam for all to %I using (true) with check (true)',current_user);
end $policy$;
commit;
