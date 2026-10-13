-- One-time CloudTMS schema/data migration: banking_pay_next_input_transaction_v1
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- Private operational ownership of this transaction only. xid8 and ordinal
-- are not business order; every committed version receives agency_sequence.
-- A terminal latch remains retained so a later callback cannot reopen it.
create table private.bpay_next_input_transaction (
  preparing_transaction_id xid8 primary key,
  module_epoch bigint not null check(module_epoch>0),
  phase text not null check(phase in ('OPEN','FINALISING','CLOCK_SEALED')),
  next_event_ordinal bigint not null default 0 check(next_event_ordinal between 0 and 1000),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  clock_sealed_at_utc timestamptz,
  check((phase='CLOCK_SEALED')=(clock_sealed_at_utc is not null))
);
alter table private.bpay_next_input_transaction owner to postgres;
alter table private.bpay_next_input_transaction enable row level security;
revoke all on private.bpay_next_input_transaction from public,anon,authenticated,service_role;

-- First-install only: never invent order for unreceipted historical events.
do $empty_version_precondition$
begin
  if exists(select 1 from private.bpay_next_input_version_event) then
    raise exception using errcode='55000',message='BPAY_NEXT_VERSION_ORDINAL_INSTALL_REQUIRES_EMPTY';
  end if;
end
$empty_version_precondition$;
alter table private.bpay_next_input_version_event
  add column transaction_ordinal bigint not null check(transaction_ordinal between 1 and 1000),
  add constraint bpay_next_input_version_transaction_ordinal_key unique(preparing_transaction_id,transaction_ordinal),
  add constraint bpay_next_input_version_transaction_fk foreign key(preparing_transaction_id)
    references private.bpay_next_input_transaction(preparing_transaction_id) on delete restrict;

commit;
