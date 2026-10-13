-- E-only proposed schema; not adopted, installed, locked or natively qualified.
-- One immutable Source decision certificate, no financial balance or effects.
\set ON_ERROR_STOP on
begin;
create table private.weekly_source_destination_reprice_witness_v1 (
  id uuid primary key,
  decision_bundle_id uuid not null,
  bundle_revision bigint not null check (bundle_revision >= 1),
  idempotency_key text not null unique check (char_length(idempotency_key) between 16 and 200),
  input_sha256 bytea not null check (octet_length(input_sha256)=32),
  actor_user_id uuid not null references public.tms_users(id),
  reason text not null check (char_length(btrim(reason)) between 1 and 2000),
  context_json jsonb not null check (jsonb_typeof(context_json)='object'),
  economic_snapshots_json jsonb not null check (jsonb_typeof(economic_snapshots_json)='array'),
  review_sha256 bytea not null check (octet_length(review_sha256)=32),
  accepted_request_json jsonb not null check (jsonb_typeof(accepted_request_json)='object'),
  accepted_request_sha256 bytea not null check (octet_length(accepted_request_sha256)=32),
  accepted_control_sha256 bytea not null check (octet_length(accepted_control_sha256)=32),
  accepted_at_utc timestamptz not null default transaction_timestamp(),
  unique (decision_bundle_id,bundle_revision),
  foreign key (decision_bundle_id,bundle_revision)
    references public.weekly_source_entitlement_decision_bundles(decision_bundle_id,bundle_revision)
    deferrable initially deferred
);
alter table private.weekly_source_destination_reprice_witness_v1 owner to postgres;
revoke all on private.weekly_source_destination_reprice_witness_v1 from public,anon,authenticated,service_role;
create trigger weekly_source_destination_reprice_immutable
  before update or delete on private.weekly_source_destination_reprice_witness_v1
  for each row execute function private.weekly_source_accepted_publication_action_immutable_v1();
create trigger weekly_source_destination_reprice_truncate_immutable
  before truncate on private.weekly_source_destination_reprice_witness_v1
  for each statement execute function private.weekly_source_accepted_publication_action_immutable_v1();
comment on table private.weekly_source_destination_reprice_witness_v1 is
  'Immutable accepted whole-pair destination rate calculation. Preserves original import economics, charge, invoice and financial facts; not paid comparison or payment authority.';
commit;
