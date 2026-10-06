-- Coordinator-reserved filename. A genuine protected approval, not a C1
-- transport checkpoint, owns this immutable NEXT receipt. No backfill.
\set ON_ERROR_STOP on
begin;
alter table public.weekly_exceptional_pay_target_families
  drop constraint weekly_exceptional_pay_target_famil_target_domain_version_check;
alter table public.weekly_exceptional_pay_target_families
  add constraint bpay_next_protected_family_target_domain_check
  check (target_domain_version in ('C1_V1','NEXT_V1'));

-- PUBLISHED -> SUPERSEDED retains the exact prior publication evidence in
-- both current C1 and NEXT owners. Replace only that audited old predicate;
-- auto-generated constraint names can differ in NEW/rehearsal installs.
do $publication_pair$
declare v_constraint name;
begin
  select c.conname into strict v_constraint from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_exceptional_pay_generations'::regclass and c.contype='c'
      and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid),'[[:space:]()]','','g')
        = 'lifecycle_state=''PUBLISHED''::text=published_at_utcISNOTNULLANDresult_hashISNOTNULL';
  execute pg_catalog.format('alter table public.weekly_exceptional_pay_generations drop constraint %I',v_constraint);
end
$publication_pair$;
alter table public.weekly_exceptional_pay_generations
  add constraint bpay_next_protected_generation_publication_pair_ck check (
    case when lifecycle_state='PUBLISHED' then published_at_utc is not null and result_hash is not null
      when lifecycle_state='SUPERSEDED' then
        (published_at_utc is not null and result_hash is not null)
        or (published_at_utc is null and result_hash is null)
      else published_at_utc is null and result_hash is null end);

-- Exact current canonical-booking lookup, never all physical versions. Do
-- not assert uniqueness over inherited data; the NEXT owner refuses >=2.
create index bpay_next_timesheet_current_trim_booking_idx
  on public.timesheets(pg_catalog.btrim(booking_id))
  where is_current and booking_id is not null and pg_catalog.btrim(booking_id)<>'';

create table private.bpay_next_protected_source_receipt (
  orchestration_run_id uuid primary key references public.weekly_exceptional_orchestration_runs(id) on delete restrict,
  family_id uuid not null references public.weekly_exceptional_pay_target_families(id) on delete restrict,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  approval_id uuid not null unique references public.weekly_exceptional_payment_approvals(id) on delete restrict,
  generation_id uuid not null unique references public.weekly_exceptional_pay_generations(id) on delete restrict,
  work_id uuid not null references private.bpay_next_work(id) on delete restrict,
  revision_id uuid not null unique,
  command_id uuid not null unique references private.bpay_next_command(id) on delete restrict,
  agency_sequence bigint not null check (agency_sequence>0),
  accepted_family_bound_version bigint not null check (accepted_family_bound_version>0),
  prepared_request_sha256 bytea not null check (pg_catalog.octet_length(prepared_request_sha256)=32),
  publication_request_sha256 bytea not null check (pg_catalog.octet_length(publication_request_sha256)=32),
  source_manifest_sha256 bytea not null check (pg_catalog.octet_length(source_manifest_sha256)=32),
  accepted_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  foreign key (work_id,revision_id) references private.bpay_next_work_revision(work_id,id) on delete restrict
);
create index bpay_next_protected_source_receipt_family_idx
  on private.bpay_next_protected_source_receipt(family_id,generation_id);
create index bpay_next_protected_source_receipt_actor_idx
  on private.bpay_next_protected_source_receipt(actor_user_id,orchestration_run_id);
alter table private.bpay_next_protected_source_receipt owner to postgres;
alter table private.bpay_next_protected_source_receipt enable row level security;
revoke all on private.bpay_next_protected_source_receipt from public,anon,authenticated,service_role;

create function private.bpay_next_protected_source_receipt_immutable_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog
as $function$
begin
  raise exception using errcode='55000',message='BPAY_NEXT_PROTECTED_RECEIPT_IMMUTABLE';
end
$function$;
alter function private.bpay_next_protected_source_receipt_immutable_v1() owner to postgres;
revoke all on function private.bpay_next_protected_source_receipt_immutable_v1()
  from public,anon,authenticated,service_role;
create trigger bpay_next_protected_source_receipt_immutable
before update or delete on private.bpay_next_protected_source_receipt
for each row execute function private.bpay_next_protected_source_receipt_immutable_v1();
create trigger bpay_next_protected_source_receipt_no_truncate
before truncate on private.bpay_next_protected_source_receipt
for each statement execute function private.bpay_next_protected_source_receipt_immutable_v1();
commit;
