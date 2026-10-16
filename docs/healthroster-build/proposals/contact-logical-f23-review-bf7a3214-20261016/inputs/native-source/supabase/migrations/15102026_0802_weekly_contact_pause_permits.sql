-- Checking contact admission metadata. Existing message, answer, outbox and
-- native attempt owners retain business/send history and provider outcomes.
\set ON_ERROR_STOP on
begin;

create table public.weekly_contact_pause_scopes (
 id uuid primary key default pg_catalog.gen_random_uuid(),
 environment text not null check(environment in ('TEST','LIVE')),
 agency_id uuid not null,
 candidate_id uuid not null references public.candidates(id) on delete restrict,
 client_id uuid not null references public.clients(id) on delete restrict,
 revision bigint not null default 0 check(revision>=0),
 worker_paused_until_utc timestamptz check(worker_paused_until_utc is null or pg_catalog.isfinite(worker_paused_until_utc)),
 manager_paused_until_utc timestamptz check(manager_paused_until_utc is null or pg_catalog.isfinite(manager_paused_until_utc)),
 worker_reason text check(worker_reason is null or pg_catalog.char_length(worker_reason)<=500),
 manager_reason text check(manager_reason is null or pg_catalog.char_length(manager_reason)<=500),
 updated_by_user_id uuid references public.tms_users(id) on delete restrict,
 updated_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
 unique(environment,agency_id,candidate_id,client_id)
);
create table public.weekly_contact_pause_commands (
 request_id uuid primary key,
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 scope_id uuid not null references public.weekly_contact_pause_scopes(id) on delete restrict,
 request_digest bytea not null check(pg_catalog.octet_length(request_digest)=32),
 expected_revision bigint not null check(expected_revision>=0),
 accepted_revision bigint not null check(accepted_revision>=expected_revision),
 operation text not null check(operation in ('PAUSE','RESUME')),
 audience_mask integer not null check(audience_mask in (1,2,3)),
 computed_expiry_utc timestamptz,
 response_json jsonb not null check(pg_catalog.jsonb_typeof(response_json)='object'),
 created_at_utc timestamptz not null,
 completed_at_utc timestamptz not null,
 check(completed_at_utc>=created_at_utc),
 check((operation='PAUSE')=(computed_expiry_utc is not null))
);
create table public.weekly_contact_pause_events (
 id uuid primary key default pg_catalog.gen_random_uuid(),
 scope_id uuid not null references public.weekly_contact_pause_scopes(id) on delete restrict,
 request_id uuid not null references public.weekly_contact_pause_commands(request_id) on delete restrict deferrable initially deferred,
 revision bigint not null check(revision>=1),
 audience text not null check(audience in ('WORKER','MANAGER')),
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 action text not null check(action in ('PAUSE','RESUME')),
 previous_expiry_utc timestamptz,
 new_expiry_utc timestamptz,
 reason text check(reason is null or pg_catalog.char_length(reason)<=500),
 recorded_at_utc timestamptz not null,
 unique(scope_id,revision,audience)
);

create table public.weekly_contact_submission_permits (
 id uuid primary key default pg_catalog.gen_random_uuid(),
 environment text not null check(environment in ('TEST','LIVE')),
 agency_id uuid not null,
 owner_kind text not null check(owner_kind in ('WEEKLY_SOURCE_TARGET','TIMESHEET_QUERY_OUTBOX')),
 logical_delivery_key bytea not null check(pg_catalog.octet_length(logical_delivery_key)=32),
 dispatch_target_id uuid references public.weekly_message_dispatch_targets(id) on delete restrict,
 dispatch_command_id uuid references public.weekly_message_dispatch_commands(id) on delete restrict,
 provider_attempt_id uuid unique references public.weekly_message_target_attempts(id) on delete restrict,
 mail_outbox_id uuid references public.mail_outbox(id) on delete restrict,
 query_delivery_id uuid references public.hr_issue_email_deliveries(id) on delete restrict,
 channel text not null check(channel in ('PUSH','EMAIL')),
 provider text not null check(provider in ('APNS','FCM','POWER_AUTOMATE')),
 content_hash bytea not null check(pg_catalog.octet_length(content_hash)=32),
 recipient_hash bytea not null check(pg_catalog.octet_length(recipient_hash)=32),
 scope_revisions_json jsonb not null check(pg_catalog.jsonb_typeof(scope_revisions_json)='array'
  and pg_catalog.jsonb_array_length(scope_revisions_json)<=500),
 target_snapshot_hash bytea check(target_snapshot_hash is null or pg_catalog.octet_length(target_snapshot_hash)=32),
 safe_target_snapshot_json jsonb,
 policy_revision bigint check(policy_revision is null or policy_revision>=1),
 policy_sha256 bytea check(policy_sha256 is null or pg_catalog.octet_length(policy_sha256)=32),
 calendar_digest bytea check(calendar_digest is null or pg_catalog.octet_length(calendar_digest)=32),
 lease_owner text not null,
 lease_token text not null,
 lease_expires_at_utc timestamptz not null,
 status text not null check(status in ('PREPARED','STARTED','ACCEPTED','FAILED','AMBIGUOUS','RETIRED')),
 admitted_at_utc timestamptz,
 consume_before_utc timestamptz,
 consumed_at_utc timestamptz,
 accepted_at_utc timestamptz,
 provider_idempotency_identity text,
 accepted_result_json jsonb check(accepted_result_json is null or pg_catalog.jsonb_typeof(accepted_result_json)='object'),
 proof_classification text check(proof_classification is null or proof_classification='NOT_SUBMITTED'),
 proved_no_network boolean not null default false,
 created_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
 check((owner_kind='WEEKLY_SOURCE_TARGET' and dispatch_target_id is not null and dispatch_command_id is not null
   and mail_outbox_id is null and query_delivery_id is null)
  or(owner_kind='TIMESHEET_QUERY_OUTBOX' and dispatch_target_id is null and dispatch_command_id is null
   and provider_attempt_id is null and mail_outbox_id is not null and query_delivery_id is not null and channel='EMAIL')),
 check((channel='PUSH')=(provider in ('APNS','FCM'))),
 check(channel<>'PUSH' or(target_snapshot_hash is not null and safe_target_snapshot_json is not null and pg_catalog.jsonb_typeof(safe_target_snapshot_json)='object'
   and policy_revision is not null and policy_sha256 is not null and calendar_digest is not null)),
 check(channel<>'EMAIL' or(policy_revision is null and policy_sha256 is null and calendar_digest is null and consume_before_utc is null)),
 check(status='PREPARED' or admitted_at_utc is not null),
 check(channel<>'PUSH' or status='PREPARED' or consume_before_utc=admitted_at_utc+interval '30 seconds'),
 check(consumed_at_utc is null or admitted_at_utc is not null),
 check(proof_classification is null or(proved_no_network and status='RETIRED')),
 check(not proved_no_network or proof_classification is not distinct from 'NOT_SUBMITTED'),
 check((status='ACCEPTED' and accepted_at_utc is not null and accepted_result_json is not null)
  or(status<>'ACCEPTED' and accepted_at_utc is null and accepted_result_json is null)),
 check(consumed_at_utc is null or(channel='EMAIL' or consumed_at_utc<=consume_before_utc))
);
create unique index weekly_contact_submission_permits_logical_active_uq
 on public.weekly_contact_submission_permits(logical_delivery_key)
 where status in ('PREPARED','STARTED','ACCEPTED','AMBIGUOUS');
create index weekly_contact_submission_permits_target_idx on public.weekly_contact_submission_permits(dispatch_target_id,id);

create table public.weekly_contact_delivery_memberships (
 id uuid primary key default pg_catalog.gen_random_uuid(),
 logical_contact_key bytea not null check(pg_catalog.octet_length(logical_contact_key)=32),
 work_event_id uuid references public.weekly_work_events(id) on delete restrict,
 incident_id uuid references public.weekly_discrepancy_incidents(id) on delete restrict,
 incident_episode integer check(incident_episode is null or incident_episode>=1),
 purpose text not null,
 audience text not null check(audience in ('WORKER','MANAGER')),
 candidate_id uuid not null references public.candidates(id) on delete restrict,
 client_id uuid not null references public.clients(id) on delete restrict,
 scope_id uuid not null references public.weekly_contact_pause_scopes(id) on delete restrict,
 scope_revision bigint not null check(scope_revision>=0),
 delivery_id uuid not null,
 payload_revision bigint not null check(payload_revision>=1),
 content_hash bytea not null check(pg_catalog.octet_length(content_hash)=32),
 accepted_at_utc timestamptz,
 send_ordinal integer not null check(send_ordinal>=1),
 budget_owner_key bytea not null check(pg_catalog.octet_length(budget_owner_key)=32),
 supersedes_unsubmitted_delivery_id uuid,
 created_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
 unique(delivery_id,logical_contact_key,send_ordinal),
 check((incident_id is null)=(incident_episode is null))
);
create unique index weekly_contact_delivery_memberships_accepted_uq
 on public.weekly_contact_delivery_memberships(logical_contact_key,send_ordinal) where accepted_at_utc is not null;

-- Recheck metadata is not another sender queue. A bounded native wake records
-- the existing logical work's disposition; it never grants a provider call.
create table public.weekly_contact_recheck_tasks (
 id uuid primary key default pg_catalog.gen_random_uuid(),
 scope_id uuid not null references public.weekly_contact_pause_scopes(id) on delete restrict,
 request_id uuid not null unique references public.weekly_contact_pause_commands(request_id) on delete restrict deferrable initially deferred,
 scope_revision bigint not null check(scope_revision>=0),
 due_at_utc timestamptz not null,
 state text not null check(state in ('READY','LEASED','DONE')),
 lease_token uuid,
 lease_expires_at_utc timestamptz,
 disposition_json jsonb,
 created_at_utc timestamptz not null default pg_catalog.clock_timestamp(),
 completed_at_utc timestamptz,
 check((state='LEASED' and lease_token is not null and lease_expires_at_utc is not null)
  or(state<>'LEASED' and lease_token is null and lease_expires_at_utc is null)),
 check((state='DONE' and completed_at_utc is not null and disposition_json is not null)
  or(state<>'DONE' and completed_at_utc is null and disposition_json is null))
);
create index weekly_contact_recheck_tasks_due_idx on public.weekly_contact_recheck_tasks(state,due_at_utc,id);

alter table public.weekly_contact_pause_scopes owner to postgres;
alter table public.weekly_contact_pause_commands owner to postgres;
alter table public.weekly_contact_pause_events owner to postgres;
alter table public.weekly_contact_submission_permits owner to postgres;
alter table public.weekly_contact_delivery_memberships owner to postgres;
alter table public.weekly_contact_recheck_tasks owner to postgres;
do $security$
declare v_table text;
begin
 foreach v_table in array array['weekly_contact_pause_scopes','weekly_contact_pause_commands','weekly_contact_pause_events',
  'weekly_contact_submission_permits','weekly_contact_delivery_memberships','weekly_contact_recheck_tasks'] loop
  execute format('alter table public.%I enable row level security',v_table);
  execute format('alter table public.%I force row level security',v_table);
  execute format('create policy cloudtms_miget_service_owner_all on public.%I for all to CURRENT_USER,service_role using(true) with check(true)',v_table);
  execute format('revoke all on public.%I from public,anon,authenticated,service_role',v_table);
 end loop;
end;
$security$;
commit;
