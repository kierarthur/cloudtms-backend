-- An Office review of an existing imported work identity is separate from an
-- automatically detected comparison incident. It cannot create work or pay.
create table private.weekly_source_manual_reviews (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  source_group_id uuid not null references public.weekly_source_groups(id) on delete restrict,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  upload_row_id uuid not null references public.weekly_source_upload_rows(id) on delete restrict,
  row_resolution_id uuid not null references public.weekly_source_row_resolutions(id) on delete restrict,
  source_row_hash bytea not null check (pg_catalog.octet_length(source_row_hash)=32),
  work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  work_date date not null,
  state text not null check (state in ('OPEN','RESOLVED')),
  office_reason text not null check (pg_catalog.char_length(pg_catalog.btrim(office_reason)) between 1 and 1000),
  opened_by_user_id uuid not null references public.tms_users(id) on delete restrict,
  opened_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  resolved_by_user_id uuid references public.tms_users(id) on delete restrict,
  resolved_at_utc timestamptz,
  resolution_kind text check (resolution_kind in ('OFFICE_ACCEPTED_SOURCE','PROTECTED_PAY')),
  check ((state='RESOLVED')=(resolved_by_user_id is not null and resolved_at_utc is not null and resolution_kind is not null))
);
alter table private.weekly_source_manual_reviews owner to postgres;
create unique index weekly_source_manual_reviews_open_uq
  on private.weekly_source_manual_reviews(source_group_id,work_event_id) where state='OPEN';
create index weekly_source_manual_reviews_family_idx
  on private.weekly_source_manual_reviews(contract_id,work_date,state,work_event_id);
revoke all on private.weekly_source_manual_reviews from public,anon,authenticated,service_role;
