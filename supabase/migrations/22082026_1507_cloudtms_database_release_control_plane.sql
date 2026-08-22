-- CloudTMS database release control-plane metadata.
-- This stores schema-release evidence only; it never stores business or payment data.

create schema if not exists private;

create table if not exists private.cloudtms_database_identity (
  singleton boolean primary key default true check (singleton),
  environment text not null check (environment in ('TEST','LIVE')),
  customer_key text,
  installation_id uuid not null default extensions.gen_random_uuid(),
  installed_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  constraint cloudtms_database_identity_customer_key_ck check (
    customer_key is null or pg_catalog.btrim(customer_key) <> ''
  )
);

create table if not exists private.cloudtms_database_releases (
  release_id text primary key,
  git_commit text not null check (git_commit ~ '^[0-9a-f]{40}$'),
  repository_contract_sha256 text not null check (repository_contract_sha256 ~ '^[0-9a-f]{64}$'),
  installed_contract_sha256 text not null check (installed_contract_sha256 ~ '^[0-9a-f]{64}$'),
  install_mode text not null check (install_mode in ('NEW','UPGRADE','ADOPT')),
  status text not null check (status in ('APPLYING','VERIFIED','FAILED')),
  started_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
  completed_at_utc timestamptz,
  evidence_json jsonb not null default '{}'::jsonb
);

create table if not exists private.cloudtms_migration_ledger (
  path text primary key,
  content_sha256 text not null check (content_sha256 ~ '^[0-9a-f]{64}$'),
  first_release_id text not null references private.cloudtms_database_releases(release_id),
  applied_at_utc timestamptz not null default pg_catalog.clock_timestamp()
);

create table if not exists private.cloudtms_repeatable_ledger (
  path text primary key,
  closure_sha256 text not null check (closure_sha256 ~ '^[0-9a-f]{64}$'),
  last_release_id text not null references private.cloudtms_database_releases(release_id),
  applied_at_utc timestamptz not null default pg_catalog.clock_timestamp()
);

revoke all on schema private from public, anon, authenticated;
revoke all on table
  private.cloudtms_database_identity,
  private.cloudtms_database_releases,
  private.cloudtms_migration_ledger,
  private.cloudtms_repeatable_ledger
from public, anon, authenticated, service_role;
