-- JOINT-CONTRACT-V4 I2: immutable acceptance evidence, not financial authority.
-- Only the existing later-change Office owner will populate this fact before
-- genuine publication. No historical approval, current Final or money is
-- backfilled, and no runtime caller is enabled by creating this relation.

\set ON_ERROR_STOP on

begin;

create table private.weekly_source_accepted_publication_actions_v1 (
  action_id uuid primary key,
  decision_bundle_id uuid not null,
  bundle_revision bigint not null check (bundle_revision>=1),
  idempotency_key text not null unique check (char_length(idempotency_key) between 16 and 200),
  request_sha256 bytea not null check (octet_length(request_sha256)=32),
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,
  action text not null check (action='APPROVE_UPDATED_HOURS'),
  root_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
  family_booking_id text not null check (char_length(btrim(family_booking_id)) between 1 and 200),
  root_version bigint not null check (root_version>=1),
  agency_id uuid not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  client_id uuid not null references public.clients(id) on delete restrict,
  contract_id uuid not null references public.contracts(id) on delete restrict,
  week_ending_date date not null,
  source_cycle_id uuid not null references public.weekly_source_cycles(id) on delete restrict,
  final_revision_id uuid not null references public.weekly_source_final_revisions(id) on delete restrict,
  finalisation_week_ending date not null,
  final_revision_number bigint not null check (final_revision_number>=1),
  manifest_hash bytea not null check (octet_length(manifest_hash)=32),
  policy_fingerprint bytea not null check (octet_length(policy_fingerprint)=32),
  expected_prior_origin jsonb not null check (
    jsonb_typeof(expected_prior_origin)='object'
    and coalesce(expected_prior_origin->>'kind','') in
      ('INITIAL_AUTHORISED_TSFIN_V1','COMMITTED_SOURCE_HEAD_V1')
  ),
  before_inventory_sha256 bytea not null check (octet_length(before_inventory_sha256)=32),
  before_entitlement_sha256 bytea not null check (octet_length(before_entitlement_sha256)=32),
  origin_sha256 bytea not null check (octet_length(origin_sha256)=32),
  planned_head_id uuid not null unique,
  accepted_canonical_json jsonb not null check (
    jsonb_typeof(accepted_canonical_json)='object'
    and (accepted_canonical_json->>'publication_mode') is not distinct from 'IMMEDIATE'
    and (accepted_canonical_json->'pending_bundle_id') is not distinct from 'null'::jsonb
  ),
  accepted_canonical_sha256 bytea not null check (octet_length(accepted_canonical_sha256)=32),
  accepted_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (decision_bundle_id,bundle_revision),
  foreign key (decision_bundle_id,bundle_revision)
    references public.weekly_source_entitlement_decision_bundles(decision_bundle_id,bundle_revision)
    on delete restrict
);
alter table private.weekly_source_accepted_publication_actions_v1 owner to postgres;
revoke all on private.weekly_source_accepted_publication_actions_v1
  from public,anon,authenticated,service_role;
comment on table private.weekly_source_accepted_publication_actions_v1 is
  'Exact immutable APP action and normalised complete canonical vector accepted before publication. Not a KEEP/move exception, financial ledger, current-effectiveness certificate or browser capability.';

-- A one-time migration cannot depend on a repeatable definition being present
-- during a fresh installation. This small owner-only guard is self-contained.
create function private.weekly_source_accepted_publication_action_immutable_v1()
returns trigger language plpgsql
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  raise exception 'WEEKLY_SOURCE_ACCEPTED_PUBLICATION_ACTION_IMMUTABLE'
    using errcode='55000';
end;
$function$;
alter function private.weekly_source_accepted_publication_action_immutable_v1() owner to postgres;
revoke all on function private.weekly_source_accepted_publication_action_immutable_v1()
  from public,anon,authenticated,service_role;
create trigger weekly_source_accepted_publication_action_immutable
  before update or delete on private.weekly_source_accepted_publication_actions_v1
  for each row execute function private.weekly_source_accepted_publication_action_immutable_v1();
create trigger weekly_source_accepted_publication_action_truncate_immutable
  before truncate on private.weekly_source_accepted_publication_actions_v1
  for each statement execute function private.weekly_source_accepted_publication_action_immutable_v1();

commit;
