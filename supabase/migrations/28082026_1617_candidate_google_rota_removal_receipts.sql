-- One-time CloudTMS schema/data migration: candidate_google_rota_removal_receipts
-- Exact removal receipts are protocol/audit state, not a new Candidate app projection.
-- Preserve identity across lost responses and later explicit re-enrolment.

\set ON_ERROR_STOP on

begin;

create table private.candidate_google_rota_removal_receipts (
  environment text not null check (environment in ('TEST','LIVE')),
  integration_id uuid not null,
  operation_id uuid not null,
  candidate_id uuid references public.candidates(id) on delete restrict,
  candidate_code_sha256 text not null check (candidate_code_sha256 ~ '^[a-f0-9]{64}$'),
  source_hmac_key_version integer not null check (source_hmac_key_version > 0),
  candidate_source_hmac text not null check (candidate_source_hmac ~ '^[a-f0-9]{64}$'),
  request_sha256 text not null check (request_sha256 ~ '^[a-f0-9]{64}$'),
  outcome text not null check (outcome in ('REMOVED','UNLINKED')),
  removed_at_utc timestamptz not null,
  reenrolled_at_utc timestamptz,
  reenrolment_operation_id uuid,
  correlation_id text not null check (length(btrim(correlation_id)) between 1 and 200),
  primary key (environment,integration_id,operation_id),
  check ((reenrolled_at_utc is null and reenrolment_operation_id is null)
    or (reenrolled_at_utc >= removed_at_utc and reenrolment_operation_id is not null))
);
create index candidate_google_rota_removal_source_idx
  on private.candidate_google_rota_removal_receipts
    (environment,source_hmac_key_version,candidate_source_hmac,removed_at_utc desc);
alter table private.candidate_google_rota_removal_receipts owner to postgres;
alter table private.candidate_google_rota_removal_receipts enable row level security;
revoke all on table private.candidate_google_rota_removal_receipts from public,anon,authenticated,service_role;

commit;
