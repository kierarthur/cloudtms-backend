-- Private, short-lived source transport. Not enrolment or business/payment data.
create table private.candidate_daily_emergency_roster_snapshots (
  environment text not null check (environment in ('TEST','LIVE')),
  candidate_id uuid not null references public.candidates(id) on delete cascade,
  emergency_shift_token text not null check (emergency_shift_token ~ '^[a-f0-9]{64}$'),
  groups_json jsonb not null check (jsonb_typeof(groups_json)='object'),
  observed_at_utc timestamptz not null,
  expires_at_utc timestamptz not null,
  source_sha256 text not null check (source_sha256 ~ '^[a-f0-9]{64}$'),
  primary key (environment,candidate_id,emergency_shift_token),
  check (expires_at_utc=observed_at_utc+interval '5 minutes')
);
alter table private.candidate_daily_emergency_roster_snapshots owner to postgres;
revoke all on private.candidate_daily_emergency_roster_snapshots from public,anon,authenticated,service_role;
