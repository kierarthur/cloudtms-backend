-- V5 CONTACT_PAUSE_SPEC: agency-local quiet PUSH authority, not control-plane
-- settings or a second queue/calendar. Bootstrap is a separate exact protected
-- release step after deployment/database verification; these stores start empty.
\set ON_ERROR_STOP on
begin;

create table public.candidate_push_timing_policy (
  environment text not null check (environment in ('TEST','LIVE')),
  agency_id uuid not null,
  deployment_slot boolean not null default true check (deployment_slot),
  revision bigint not null check (revision>=1),
  policy_json jsonb not null check (pg_catalog.jsonb_typeof(policy_json)='object'),
  policy_sha256 bytea not null check (pg_catalog.octet_length(policy_sha256)=32),
  updated_by_user_id uuid references public.tms_users(id) on delete restrict,
  updated_at_utc timestamptz not null check (pg_catalog.isfinite(updated_at_utc)),
  primary key (environment,agency_id),
  unique (deployment_slot)
);
alter table public.candidate_push_timing_policy owner to postgres;

create table public.candidate_push_timing_commands (
  environment text not null,
  agency_id uuid not null,
  request_id uuid not null,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  request_digest bytea not null check (pg_catalog.octet_length(request_digest)=32),
  expected_revision bigint not null check (expected_revision>=1),
  accepted_revision bigint not null check (accepted_revision>=1),
  response_json jsonb not null check (pg_catalog.jsonb_typeof(response_json)='object'),
  created_at_utc timestamptz not null check (pg_catalog.isfinite(created_at_utc)),
  completed_at_utc timestamptz not null check (pg_catalog.isfinite(completed_at_utc)),
  primary key (environment,agency_id,request_id),
  foreign key (environment,agency_id)
    references public.candidate_push_timing_policy(environment,agency_id) on delete restrict,
  check (completed_at_utc>=created_at_utc),
  check (accepted_revision>=expected_revision)
);
alter table public.candidate_push_timing_commands owner to postgres;

alter table public.candidate_push_timing_policy enable row level security;
alter table public.candidate_push_timing_policy force row level security;
alter table public.candidate_push_timing_commands enable row level security;
alter table public.candidate_push_timing_commands force row level security;
create policy cloudtms_miget_service_owner_all on public.candidate_push_timing_policy
  for all to CURRENT_USER,service_role using (true) with check (true);
create policy cloudtms_miget_service_owner_all on public.candidate_push_timing_commands
  for all to CURRENT_USER,service_role using (true) with check (true);
-- Even service_role has no direct table access. Only the explicit SECDEF
-- administration/dispatch owners will receive service EXECUTE.
revoke all on public.candidate_push_timing_policy,public.candidate_push_timing_commands
  from public,anon,authenticated,service_role;

commit;
