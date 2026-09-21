-- One-time CloudTMS schema migration: Weekly Source per-target delivery.
--
-- Delivery is deliberately modelled below the immutable rendered message.  A
-- command can therefore contain several Candidate devices without the first
-- accepted device closing the remaining targets.  Nothing in this migration
-- belongs to Banking Pay, Workbench, Create Draft, payment execution,
-- cancellation, settlement or remittance.

\set ON_ERROR_STOP on

begin;

alter table public.weekly_message_dispatch_commands
  add column target_set_state text not null default 'NOT_PREPARED'
    check (target_set_state in ('NOT_PREPARED','PREPARED','SUPPRESSED','TERMINAL')),
  add column target_set_hash bytea
    check (target_set_hash is null or pg_catalog.octet_length(target_set_hash)=32),
  add column control_plane_snapshot_id uuid,
  add column target_count integer not null default 0 check (target_count>=0),
  add column terminal_target_count integer not null default 0 check (terminal_target_count>=0),
  add column accepted_target_count integer not null default 0 check (accepted_target_count>=0),
  add column transport_finalised_at_utc timestamptz,
  add column terminal_reason text,
  add constraint weekly_message_dispatch_commands_target_counts_ck check (
    terminal_target_count<=target_count and accepted_target_count<=terminal_target_count
  ),
  add constraint weekly_message_dispatch_commands_target_set_ck check (
    (target_set_state='NOT_PREPARED'
      and target_set_hash is null and target_count=0
      and terminal_target_count=0 and accepted_target_count=0)
    or
    (target_set_state='PREPARED'
      and target_set_hash is not null and target_count>=1
      and transport_finalised_at_utc is null)
    or
    (target_set_state='SUPPRESSED'
      and target_set_hash is not null and target_count=0
      and terminal_target_count=0 and accepted_target_count=0
      and transport_finalised_at_utc is not null and terminal_reason is not null)
    or
    (target_set_state='TERMINAL'
      and target_set_hash is not null and target_count>=1
      and terminal_target_count=target_count
      and transport_finalised_at_utc is not null)
  );

create table public.weekly_message_dispatch_targets (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  dispatch_command_id uuid not null
    references public.weekly_message_dispatch_commands(id) on delete restrict,
  target_ordinal integer not null check (target_ordinal>=1),
  channel text not null check (channel in ('PUSH','EMAIL')),
  target_kind text not null
    check (target_kind in ('CANDIDATE_DEVICE','MANAGER_ADDRESS','PACK_COPY_ADDRESS')),
  provider text not null check (provider in ('APNS','FCM','POWER_AUTOMATE')),
  external_target_id uuid not null,
  keyed_target_fingerprint bytea not null
    check (pg_catalog.octet_length(keyed_target_fingerprint)=32),
  target_snapshot_hash bytea not null
    check (pg_catalog.octet_length(target_snapshot_hash)=32),
  target_version integer not null check (target_version>=1),
  safe_target_snapshot_json jsonb not null
    check (pg_catalog.jsonb_typeof(safe_target_snapshot_json)='object'),
  rendered_content_hash bytea not null
    check (pg_catalog.octet_length(rendered_content_hash)=32),
  provider_idempotency_key text not null
    check (pg_catalog.char_length(provider_idempotency_key) between 1 and 500),
  state text not null default 'READY' check (state in (
    'READY','LEASED','SUBMISSION_STARTED','ACCEPTED','DEFINITELY_REJECTED',
    'TRANSIENT_FAILURE','AMBIGUOUS','RETIRED','SKIPPED'
  )),
  lease_owner text,
  lease_token uuid,
  lease_expires_at_utc timestamptz,
  attempt_count integer not null default 0 check (attempt_count>=0),
  maximum_attempts integer not null default 5 check (maximum_attempts between 1 and 10),
  next_attempt_at_utc timestamptz,
  provider_message_id text,
  provider_accepted_at_utc timestamptz,
  terminal_at_utc timestamptz,
  terminal_reason text,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (dispatch_command_id,target_ordinal),
  unique (dispatch_command_id,keyed_target_fingerprint),
  unique (provider_idempotency_key),
  check ((channel='PUSH')=(target_kind='CANDIDATE_DEVICE')),
  check ((channel='PUSH')=(provider in ('APNS','FCM'))),
  check (
    (state in ('LEASED','SUBMISSION_STARTED')
      and lease_owner is not null and lease_token is not null
      and lease_expires_at_utc is not null)
    or
    (state not in ('LEASED','SUBMISSION_STARTED')
      and lease_owner is null and lease_token is null and lease_expires_at_utc is null)
  ),
  check (
    (state in ('ACCEPTED','DEFINITELY_REJECTED','AMBIGUOUS','RETIRED','SKIPPED')
      and terminal_at_utc is not null)
    or
    (state not in ('ACCEPTED','DEFINITELY_REJECTED','AMBIGUOUS','RETIRED','SKIPPED')
      and terminal_at_utc is null)
  )
);
alter table public.weekly_message_dispatch_targets owner to postgres;
create index weekly_message_dispatch_targets_due_idx
  on public.weekly_message_dispatch_targets(state,next_attempt_at_utc,id);
create index weekly_message_dispatch_targets_command_idx
  on public.weekly_message_dispatch_targets(dispatch_command_id,state,id);

create table public.weekly_message_target_attempts (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  dispatch_target_id uuid not null
    references public.weekly_message_dispatch_targets(id) on delete restrict,
  attempt_number integer not null check (attempt_number>=1),
  provider_idempotency_key text not null
    check (pg_catalog.char_length(provider_idempotency_key) between 1 and 520),
  lease_owner text not null,
  lease_token uuid not null,
  submission_started_at_utc timestamptz not null,
  completed_at_utc timestamptz,
  outcome text check (outcome is null or outcome in (
    'ACCEPTED','DEFINITELY_REJECTED','TRANSIENT_FAILURE','AMBIGUOUS'
  )),
  provider_message_id text,
  bounded_provider_receipt_json jsonb not null default '{}'::jsonb
    check (pg_catalog.jsonb_typeof(bounded_provider_receipt_json)='object'),
  bounded_error_json jsonb not null default '{}'::jsonb
    check (pg_catalog.jsonb_typeof(bounded_error_json)='object'),
  result_hash bytea check (result_hash is null or pg_catalog.octet_length(result_hash)=32),
  unique (dispatch_target_id,attempt_number),
  unique (provider_idempotency_key),
  check ((completed_at_utc is null)=(outcome is null and result_hash is null))
);
alter table public.weekly_message_target_attempts owner to postgres;

create table public.weekly_candidate_message_notifications (
  message_intent_id uuid primary key
    references public.weekly_message_intents(id) on delete restrict,
  candidate_generation_id uuid not null
    references public.weekly_candidate_outreach_generations(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  account_id uuid not null references public.candidate_app_accounts(id) on delete restrict,
  membership_id uuid not null,
  notification_id uuid not null
    references public.candidate_notifications(id) on delete restrict,
  tranche_kind text not null,
  dedupe_key text not null,
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  retired_at_utc timestamptz,
  unique (notification_id),
  unique (dedupe_key)
);
alter table public.weekly_candidate_message_notifications owner to postgres;
create index weekly_candidate_message_notifications_generation_idx
  on public.weekly_candidate_message_notifications(candidate_generation_id,message_intent_id);

create table public.weekly_message_delivery_failures (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  dispatch_command_id uuid not null
    references public.weekly_message_dispatch_commands(id) on delete restrict,
  dispatch_target_id uuid
    references public.weekly_message_dispatch_targets(id) on delete restrict,
  failure_class text not null check (failure_class in (
    'PROVIDER_AMBIGUOUS','RETRIES_EXHAUSTED','PROVIDER_CONFIGURATION',
    'INVALID_TARGET','TARGET_SNAPSHOT_FAILED','TARGET_RETIREMENT_FAILED'
  )),
  safe_failure_code text not null
    check (safe_failure_code ~ '^[A-Z][A-Z0-9_]{1,119}$'),
  safe_context_json jsonb not null default '{}'::jsonb
    check (pg_catalog.jsonb_typeof(safe_context_json)='object'),
  state text not null default 'OPEN' check (state in ('OPEN','ACKNOWLEDGED','RESOLVED')),
  created_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  acknowledged_at_utc timestamptz,
  resolved_at_utc timestamptz,
  unique (dispatch_command_id,dispatch_target_id,failure_class,safe_failure_code)
);
alter table public.weekly_message_delivery_failures owner to postgres;
create index weekly_message_delivery_failures_open_idx
  on public.weekly_message_delivery_failures(state,created_at_utc,id);

do $weekly_source_delivery_rls$
declare
  v_table text;
begin
  foreach v_table in array array[
    'weekly_message_dispatch_targets',
    'weekly_message_target_attempts',
    'weekly_candidate_message_notifications',
    'weekly_message_delivery_failures'
  ]
  loop
    execute pg_catalog.format('alter table public.%I enable row level security',v_table);
    execute pg_catalog.format('alter table public.%I force row level security',v_table);
    execute pg_catalog.format(
      'create policy cloudtms_miget_service_owner_all on public.%I for all to %I, service_role using (true) with check (true)',
      v_table,current_user
    );
    execute pg_catalog.format('revoke all on public.%I from public,anon,authenticated,service_role',v_table);
    execute pg_catalog.format('grant select,insert,update on public.%I to service_role',v_table);
  end loop;
end;
$weekly_source_delivery_rls$;

commit;
