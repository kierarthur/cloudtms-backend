-- One-time CloudTMS schema/data migration: weekly_source_import_preparation
-- 30 September Office policy: preparation is an explicit, file-bound decision.
-- Append-only evidence; no backfill, financial writes, or notification changes.

\set ON_ERROR_STOP on

begin;

create table private.weekly_source_import_preparations (
  upload_id uuid primary key references public.weekly_source_uploads(id),
  projection_publication_id uuid not null references public.weekly_source_projection_publications(id),
  authority_scope_version bigint not null check (authority_scope_version>0),
  prepared_by_user_id uuid not null references public.tms_users(id),
  prepared_at_utc timestamptz not null default transaction_timestamp(),
  row_manifest_hash bytea not null check (octet_length(row_manifest_hash)=32)
);
alter table private.weekly_source_import_preparations owner to postgres;
revoke all on private.weekly_source_import_preparations from public,anon,authenticated,service_role;

commit;
