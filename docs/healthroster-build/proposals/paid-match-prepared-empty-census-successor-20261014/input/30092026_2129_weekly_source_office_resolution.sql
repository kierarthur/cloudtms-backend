-- One-time CloudTMS schema/data migration: weekly_source_office_resolution
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- Choices apply to this immutable source row only, never to another file or
-- every person sharing a name. Each replacement choice remains auditable.
create table private.weekly_source_office_row_choices (
  id bigint generated always as identity primary key,
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  candidate_id uuid references public.candidates(id) on delete restrict,
  client_id uuid references public.clients(id) on delete restrict,
  contract_id uuid references public.contracts(id) on delete restrict,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  chosen_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  check(candidate_id is not null or client_id is not null or contract_id is not null)
);
create index weekly_source_office_row_choices_latest_idx
  on private.weekly_source_office_row_choices(upload_row_id,id desc);
create table private.weekly_source_office_rechecks (
  request_id uuid primary key,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  upload_id uuid not null references public.weekly_source_uploads(id) on delete restrict,
  prior_publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
  publication_id uuid not null references public.weekly_source_projection_publications(id) on delete restrict,
  request_hash bytea not null check(pg_catalog.octet_length(request_hash)=32),
  request_json jsonb not null check(pg_catalog.jsonb_typeof(request_json)='object'),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);
alter table private.weekly_source_office_row_choices owner to postgres;
alter table private.weekly_source_office_rechecks owner to postgres;
revoke all on private.weekly_source_office_row_choices,private.weekly_source_office_rechecks from public,anon,authenticated,service_role;
revoke all on sequence private.weekly_source_office_row_choices_id_seq from public,anon,authenticated,service_role;

commit;
