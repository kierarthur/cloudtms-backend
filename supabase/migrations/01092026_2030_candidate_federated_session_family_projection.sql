-- Preserve the control-plane login-family boundary in agency-local Candidate
-- session projections. Separate valid devices may remain active concurrently;
-- monotonic refresh ordering continues to apply within each exact family.

alter table public.candidate_app_sessions
  add column if not exists global_session_family_identity_hmac bytea;

alter table public.candidate_app_sessions
  drop constraint if exists candidate_app_sessions_federated_context_ck;

alter table public.candidate_app_sessions
  add constraint candidate_app_sessions_federated_context_ck
  check (
    (auth_source='LOCAL'
      and global_account_identity_hmac is null
      and global_session_identity_hmac is null
      and global_session_family_identity_hmac is null
      and membership_id is null
      and membership_generation is null
      and route_version is null
      and session_epoch is null)
    or
    (auth_source='CONTROL_PLANE'
      and octet_length(global_account_identity_hmac)=32
      and octet_length(global_session_identity_hmac)=32
      and (
        global_session_family_identity_hmac is null
        or octet_length(global_session_family_identity_hmac)=32
      )
      and membership_id is not null
      and membership_generation>=1
      and route_version>=1
      and session_epoch>=1)
  );

drop index if exists public.candidate_app_sessions_control_plane_epoch_uq;

create unique index if not exists candidate_app_sessions_control_plane_family_epoch_uq
  on public.candidate_app_sessions(
    membership_id,global_session_family_identity_hmac,session_epoch
  )
  where auth_source='CONTROL_PLANE'
    and global_session_family_identity_hmac is not null
    and status='ACTIVE';

create index if not exists candidate_app_sessions_control_plane_family_context_idx
  on public.candidate_app_sessions(
    membership_id,global_session_family_identity_hmac,
    membership_generation,route_version,session_epoch,expires_at_utc
  )
  where auth_source='CONTROL_PLANE'
    and global_session_family_identity_hmac is not null;

comment on column public.candidate_app_sessions.global_session_family_identity_hmac is
  'One-way agency-specific identity for the stable control-plane login family. It scopes session-epoch monotonicity without exposing or coupling to the central family UUID.';
