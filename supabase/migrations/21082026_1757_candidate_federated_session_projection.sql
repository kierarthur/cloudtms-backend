-- Additive agency-local compatibility projection for centrally authenticated MyTMS sessions.
-- This does not change Candidate business tables or business RPC meanings.

alter table public.candidate_app_sessions
  add column if not exists auth_source text not null default 'LOCAL',
  add column if not exists global_account_identity_hmac bytea,
  add column if not exists global_session_identity_hmac bytea,
  add column if not exists membership_id uuid,
  add column if not exists membership_generation integer,
  add column if not exists route_version integer,
  add column if not exists session_epoch bigint;

do $$
begin
  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid='public.candidate_app_sessions'::regclass
      and conname='candidate_app_sessions_auth_source_ck'
  ) then
    alter table public.candidate_app_sessions add constraint candidate_app_sessions_auth_source_ck
      check (auth_source in ('LOCAL','CONTROL_PLANE'));
  end if;
  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid='public.candidate_app_sessions'::regclass
      and conname='candidate_app_sessions_federated_context_ck'
  ) then
    alter table public.candidate_app_sessions add constraint candidate_app_sessions_federated_context_ck
      check (
        (auth_source='LOCAL'
          and global_account_identity_hmac is null
          and global_session_identity_hmac is null
          and membership_id is null
          and membership_generation is null
          and route_version is null
          and session_epoch is null)
        or
        (auth_source='CONTROL_PLANE'
          and octet_length(global_account_identity_hmac)=32
          and octet_length(global_session_identity_hmac)=32
          and membership_id is not null
          and membership_generation>=1
          and route_version>=1
          and session_epoch>=1)
      );
  end if;
end;
$$;

create unique index if not exists candidate_app_sessions_control_plane_epoch_uq
  on public.candidate_app_sessions(membership_id,session_epoch)
  where auth_source='CONTROL_PLANE' and status='ACTIVE';

create index if not exists candidate_app_sessions_control_plane_context_idx
  on public.candidate_app_sessions(
    membership_id,membership_generation,route_version,session_epoch,expires_at_utc
  ) where auth_source='CONTROL_PLANE';

create table if not exists public.candidate_app_global_membership_links (
  membership_id uuid primary key,
  global_account_identity_hmac bytea not null,
  account_id uuid not null references public.candidate_app_accounts(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  candidate_code text,
  membership_generation integer not null,
  state text not null default 'ACTIVE',
  linked_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  revoked_at_utc timestamptz,
  revoke_reason text,
  constraint candidate_app_global_membership_links_account_hmac_ck
    check (octet_length(global_account_identity_hmac)=32),
  constraint candidate_app_global_membership_links_generation_ck
    check (membership_generation>=1),
  constraint candidate_app_global_membership_links_state_ck
    check (state in ('PENDING','ACTIVE','DISABLED','REVOKED')),
  constraint candidate_app_global_membership_links_state_time_ck
    check ((state='REVOKED' and revoked_at_utc is not null) or state<>'REVOKED'),
  unique (global_account_identity_hmac,candidate_id)
);

create index if not exists candidate_app_global_membership_links_candidate_state_idx
  on public.candidate_app_global_membership_links(candidate_id,state,membership_generation);

alter table public.candidate_app_global_membership_links enable row level security;
alter table public.candidate_app_global_membership_links force row level security;

revoke all on table public.candidate_app_global_membership_links from public,anon,authenticated;
grant select,insert,update,delete on table public.candidate_app_global_membership_links to service_role;

comment on table public.candidate_app_global_membership_links is
  'Agency-local, service-only proof linking one central membership generation to one existing Candidate/account.';
comment on column public.candidate_app_sessions.auth_source is
  'LOCAL preserves the existing credential/session path; CONTROL_PLANE is a short-lived internal projection only.';
