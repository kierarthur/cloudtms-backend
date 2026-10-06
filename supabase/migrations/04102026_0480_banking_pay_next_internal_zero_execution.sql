-- An INTERNAL_ZERO row is a financial execution envelope, NOT a bank
-- instruction. Reuse exact frozen membership/owned-effect identities without
-- a fictitious CSV, external receipt, bank destination or cash-return balance.
\set ON_ERROR_STOP on
begin;

alter table private.bpay_next_transfer
  add column execution_kind text not null default 'BANK'
    check (execution_kind in ('BANK','INTERNAL_ZERO'));

-- Replace only the two complete audited core predicates. Unknown catalogues
-- fail closed; no prefix/name guessing or removal of other protections.
do $audited_transfer_checks$
declare
  v_name name;v_expected text;v_predicate text;
begin
  foreach v_predicate in array array[
    $status$(status = ANY (ARRAY['BUILDING'::text,'MEMBERS_READY'::text,'DRAFT'::text,'SCHEDULED'::text,
      'ISSUED_CSV'::text,'SUBMITTED'::text,'UNKNOWN'::text,'SETTLED'::text,'RETURNED'::text,'REFUSED'::text,'CANCELLED'::text]))$status$,
    $bank$((status = ANY (ARRAY['BUILDING'::text,'MEMBERS_READY'::text,'CANCELLED'::text])) OR
      ((account_approval_ref IS NOT NULL) AND (destination_rail IS NOT NULL)
       AND (NULLIF(btrim(beneficiary_name_snapshot),''::text) IS NOT NULL)
       AND (sort_code_snapshot ~ '^[0-9]{6}$'::text)
       AND (account_number_snapshot ~ '^[0-9]{8}$'::text)
       AND (NULLIF(btrim(bank_details_hash_snapshot),''::text) IS NOT NULL)))$bank$
  ] loop
    v_expected:=pg_catalog.regexp_replace(v_predicate,'[[:space:]()]','','g');
    select c.conname into strict v_name from pg_catalog.pg_constraint c
      where c.conrelid='private.bpay_next_transfer'::regclass and c.contype='c'
        and pg_catalog.regexp_replace(pg_catalog.pg_get_expr(c.conbin,c.conrelid),'[[:space:]()]','','g')=v_expected;
    execute pg_catalog.format('alter table private.bpay_next_transfer drop constraint %I',v_name);
  end loop;
end
$audited_transfer_checks$;
alter table private.bpay_next_transfer
  add constraint bpay_next_transfer_execution_status_ck check (status in
    ('BUILDING','MEMBERS_READY','DRAFT','SCHEDULED','ISSUED_CSV','SUBMITTED','UNKNOWN','SETTLED',
     'RETURNED','REFUSED','CANCELLED','INTERNAL_PROCESSING','INTERNAL_SETTLED')),
  add constraint bpay_next_transfer_execution_shape_ck check (
    (execution_kind='BANK' and cash_amount>0 and status not in ('INTERNAL_PROCESSING','INTERNAL_SETTLED')
      and (status in ('BUILDING','MEMBERS_READY','CANCELLED') or
        (account_approval_ref is not null and destination_rail is not null
         and nullif(pg_catalog.btrim(beneficiary_name_snapshot),'') is not null
         and sort_code_snapshot ~ '^[0-9]{6}$' and account_number_snapshot ~ '^[0-9]{8}$'
         and nullif(pg_catalog.btrim(bank_details_hash_snapshot),'') is not null)))
    or (execution_kind='INTERNAL_ZERO' and cash_amount=0 and projection_id is not null
      and original_transfer_id is null and return_cash_id is null
      and status in ('BUILDING','MEMBERS_READY','CANCELLED','INTERNAL_PROCESSING','INTERNAL_SETTLED')
      and account_approval_ref is null and destination_rail is null and beneficiary_name_snapshot is null
      and sort_code_snapshot is null and account_number_snapshot is null and bank_details_hash_snapshot is null)),
  add constraint bpay_next_transfer_internal_receipt_key unique
    (id,run_worker_id,candidate_id,projection_id,execution_kind,cash_amount);

-- Core command/job kind and phase are generic NOT NULL TEXT. Assert that
-- exact catalogue fact rather than silently dropping an unknown restriction.
do $audited_kind_shape$
begin
  if exists(select 1 from pg_catalog.pg_constraint c where c.contype='c' and
      ((c.conrelid='private.bpay_next_command'::regclass and
        pg_catalog.pg_get_expr(c.conbin,c.conrelid) ~ '\mcommand_kind\M')
       or (c.conrelid='private.bpay_next_job'::regclass and
        pg_catalog.pg_get_expr(c.conbin,c.conrelid) ~ '\m(job_kind|phase)\M'))) then
    raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_KIND_CATALOGUE_NOT_AUDITED';
  end if;
end
$audited_kind_shape$;
alter table private.bpay_next_command add constraint bpay_next_internal_command_members_ck check
  (command_kind<>'INTERNAL_SETTLEMENT' or status not in ('SEALED','COMPLETE')
    or (expected_member_count is not null and expected_member_count=1));
alter table private.bpay_next_job add constraint bpay_next_internal_job_phase_ck check
  (job_kind<>'INTERNAL_SETTLEMENT' or
    ((status='DONE' and phase='COMPLETE') or (status<>'DONE' and phase='MEMBERS')));

create table private.bpay_next_internal_receipt (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  command_id uuid not null unique references private.bpay_next_command(id) on delete restrict,
  transfer_id uuid not null unique,
  run_worker_id uuid not null,
  candidate_id uuid not null,
  projection_id uuid not null,
  execution_kind text not null default 'INTERNAL_ZERO' check (execution_kind='INTERNAL_ZERO'),
  cash_amount private.bpay_next_penny_amount not null default 0 check (cash_amount=0),
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  occurred_at_utc timestamptz not null default pg_catalog.transaction_timestamp() check (pg_catalog.isfinite(occurred_at_utc)),
  received_at_utc timestamptz not null default pg_catalog.transaction_timestamp() check (pg_catalog.isfinite(received_at_utc)),
  unique (id,command_id,transfer_id,run_worker_id,candidate_id),
  foreign key (transfer_id,run_worker_id,candidate_id,projection_id,execution_kind,cash_amount)
    references private.bpay_next_transfer(id,run_worker_id,candidate_id,projection_id,execution_kind,cash_amount) on delete restrict,
  foreign key (projection_id,run_worker_id) references private.bpay_next_net_projection(id,run_worker_id) on delete restrict,
  foreign key (command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
  check (received_at_utc>=occurred_at_utc)
);
alter table private.bpay_next_outcome_request
  alter column outcome_id drop not null,
  add column internal_receipt_id uuid unique,
  add constraint bpay_next_outcome_receipt_origin_xor_ck check
    ((outcome_id is not null and internal_receipt_id is null) or
     (outcome_id is null and internal_receipt_id is not null)),
  add constraint bpay_next_outcome_internal_receipt_fk foreign key
    (internal_receipt_id,command_id,transfer_id,run_worker_id,candidate_id)
    references private.bpay_next_internal_receipt(id,command_id,transfer_id,run_worker_id,candidate_id) on delete restrict;

alter table private.bpay_next_internal_receipt owner to postgres;
alter table private.bpay_next_internal_receipt enable row level security;
revoke all on table private.bpay_next_internal_receipt from public,anon,authenticated,service_role;
commit;
