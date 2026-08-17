begin;

create schema if not exists private;

update public.settings_defaults
set candidate_app_feature_flags_json = candidate_app_feature_flags_json
  || '{"candidate_daily_enabled":false}'::jsonb
where id=1
  and not (candidate_app_feature_flags_json ? 'candidate_daily_enabled');

create table if not exists private.candidate_daily_authority_scopes (
  environment text not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  authority_mode text not null default 'GOOGLE_PRIMARY',
  canonical_version bigint not null default 0,
  active_generation_id uuid,
  last_transition_id uuid,
  transition_in_progress boolean not null default false,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  primary key (environment,candidate_id),
  constraint candidate_daily_authority_scopes_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_authority_scopes_mode_ck check (
    authority_mode in ('GOOGLE_PRIMARY','ROLLBACK_PENDING','SUPABASE_PRIMARY')
  ),
  constraint candidate_daily_authority_scopes_version_ck check (canonical_version >= 0)
);

create table if not exists private.candidate_daily_entitlements (
  environment text not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  enabled boolean not null default false,
  valid_from_utc timestamptz,
  valid_to_utc timestamptz,
  actor_user_id uuid,
  reason text not null,
  evidence_sha256 text not null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  primary key (environment,candidate_id),
  constraint candidate_daily_entitlements_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_entitlements_validity_ck check (
    valid_to_utc is null or valid_from_utc is null or valid_to_utc > valid_from_utc
  ),
  constraint candidate_daily_entitlements_reason_ck check (length(btrim(reason)) between 1 and 500),
  constraint candidate_daily_entitlements_evidence_ck check (evidence_sha256 ~ '^[a-f0-9]{64}$')
);

create table if not exists private.candidate_daily_source_links (
  link_id uuid primary key default gen_random_uuid(),
  environment text not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  source_system text not null,
  canonicalization_version text not null,
  link_group_id uuid not null,
  identifier_hmac text not null,
  hmac_key_version integer not null,
  state text not null,
  valid_from_utc timestamptz not null default now(),
  valid_to_utc timestamptz,
  rotation_receipt_id uuid,
  actor_user_id uuid,
  independent_approver_user_id uuid,
  evidence_sha256 text not null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  constraint candidate_daily_source_links_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_source_links_source_ck check (source_system in ('GOOGLE_CREDENTIALLY_PUBLIC_ID')),
  constraint candidate_daily_source_links_canonical_ck check (canonicalization_version='SOURCE_IDENTITY_V1'),
  constraint candidate_daily_source_links_hmac_ck check (identifier_hmac ~ '^[a-f0-9]{64}$'),
  constraint candidate_daily_source_links_key_version_ck check (hmac_key_version > 0),
  constraint candidate_daily_source_links_state_ck check (state in ('PRIMARY','OVERLAP','RETIRED','REJECTED')),
  constraint candidate_daily_source_links_validity_ck check (valid_to_utc is null or valid_to_utc > valid_from_utc),
  constraint candidate_daily_source_links_evidence_ck check (evidence_sha256 ~ '^[a-f0-9]{64}$')
);

create unique index if not exists candidate_daily_source_links_active_hmac_uq
  on private.candidate_daily_source_links(environment,source_system,hmac_key_version,identifier_hmac)
  where state in ('PRIMARY','OVERLAP');
create unique index if not exists candidate_daily_source_links_primary_group_uq
  on private.candidate_daily_source_links(environment,candidate_id,source_system)
  where state='PRIMARY';
create index if not exists candidate_daily_source_links_candidate_group_idx
  on private.candidate_daily_source_links(environment,candidate_id,source_system,link_group_id);

create table if not exists public.candidate_daily_command_receipts (
  command_id uuid primary key default gen_random_uuid(),
  environment text not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  actor_class text not null,
  command_class text not null,
  idempotency_key text not null,
  request_sha256 text not null,
  source_system text,
  source_event_id text,
  source_revision text,
  source_event_time timestamptz,
  item_key text,
  canonical_version_before bigint not null default 0,
  canonical_version_after bigint,
  state text not null default 'IN_PROGRESS',
  lease_owner text,
  lease_token text,
  lease_expires_at_utc timestamptz,
  terminal_http_status integer,
  terminal_body_json jsonb,
  terminal_body_sha256 text,
  correlation_id text not null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  completed_at_utc timestamptz,
  constraint candidate_daily_command_receipts_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_command_receipts_actor_ck check (actor_class in ('CANDIDATE','LEGACY_ADAPTER','SIGNED_SYSTEM')),
  constraint candidate_daily_command_receipts_key_ck check (length(idempotency_key) between 16 and 160),
  constraint candidate_daily_command_receipts_request_ck check (request_sha256 ~ '^[a-f0-9]{64}$'),
  constraint candidate_daily_command_receipts_version_ck check (
    canonical_version_before >= 0 and (canonical_version_after is null or canonical_version_after >= canonical_version_before)
  ),
  constraint candidate_daily_command_receipts_state_ck check (state in ('IN_PROGRESS','COMPLETED','FAILED_FINAL')),
  constraint candidate_daily_command_receipts_terminal_ck check (
    (state='IN_PROGRESS' and terminal_body_json is null and terminal_body_sha256 is null and completed_at_utc is null)
    or
    (state in ('COMPLETED','FAILED_FINAL') and terminal_body_json is not null
      and terminal_body_sha256 ~ '^[a-f0-9]{64}$' and completed_at_utc is not null)
  ),
  constraint candidate_daily_command_receipts_source_ck check (
    source_system is null
    or (nullif(btrim(source_event_id),'') is not null and nullif(btrim(source_revision),'') is not null
      and source_event_time is not null and nullif(btrim(item_key),'') is not null)
  ),
  constraint candidate_daily_command_receipts_scope_uq unique (environment,candidate_id,actor_class,idempotency_key)
);

create unique index if not exists candidate_daily_command_receipts_source_event_uq
  on public.candidate_daily_command_receipts(environment,source_system,source_event_id,item_key)
  where source_system is not null;
create index if not exists candidate_daily_command_receipts_candidate_state_idx
  on public.candidate_daily_command_receipts(environment,candidate_id,state,created_at_utc);

create table if not exists private.candidate_daily_batch_receipts (
  batch_receipt_id uuid primary key default gen_random_uuid(),
  environment text not null,
  actor_class text not null,
  operation_class text not null,
  idempotency_key text not null,
  request_hash text not null,
  item_keys_json jsonb not null,
  item_count integer not null,
  state text not null default 'IN_PROGRESS',
  lease_owner text,
  lease_token text,
  lease_expires_at_utc timestamptz,
  item_receipt_ids_json jsonb not null default '[]'::jsonb,
  claim_request_id uuid,
  claim_target text,
  claim_limit integer,
  claimed_items_json jsonb,
  terminal_http_status integer,
  terminal_response_body jsonb,
  terminal_response_sha256 text,
  correlation_id text not null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  completed_at_utc timestamptz,
  constraint candidate_daily_batch_receipts_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_batch_receipts_actor_ck check (actor_class in ('LEGACY_ADAPTER','SIGNED_SYSTEM','OFFICE_ADMIN')),
  constraint candidate_daily_batch_receipts_operation_ck check (operation_class in (
    'ROTA_GENERATION_PUBLISH','SHEET_EDIT_INGEST','PROJECTION_CLAIM','PROJECTION_COMPLETE',
    'RECONCILIATION','AUTHORITY_TRANSITION'
  )),
  constraint candidate_daily_batch_receipts_key_ck check (length(idempotency_key) between 16 and 160),
  constraint candidate_daily_batch_receipts_hash_ck check (request_hash ~ '^[a-f0-9]{64}$'),
  constraint candidate_daily_batch_receipts_items_ck check (
    jsonb_typeof(item_keys_json)='array' and jsonb_array_length(item_keys_json)=item_count and item_count between 1 and 100
  ),
  constraint candidate_daily_batch_receipts_item_receipts_ck check (jsonb_typeof(item_receipt_ids_json)='array'),
  constraint candidate_daily_batch_receipts_state_ck check (state in ('IN_PROGRESS','COMPLETED','FAILED_FINAL')),
  constraint candidate_daily_batch_receipts_terminal_ck check (
    (state='IN_PROGRESS' and terminal_response_body is null and terminal_response_sha256 is null and completed_at_utc is null)
    or
    (state in ('COMPLETED','FAILED_FINAL') and terminal_response_body is not null
      and terminal_response_sha256 ~ '^[a-f0-9]{64}$' and completed_at_utc is not null)
  ),
  constraint candidate_daily_batch_receipts_scope_uq unique (environment,actor_class,operation_class,idempotency_key)
);

create index if not exists candidate_daily_batch_receipts_state_idx
  on private.candidate_daily_batch_receipts(environment,state,updated_at_utc);

create table if not exists public.candidate_daily_rota_generations (
  generation_id uuid primary key default gen_random_uuid(),
  environment text not null,
  candidate_id uuid not null,
  generation_version bigint not null,
  window_start date not null,
  window_end date not null,
  state text not null default 'BUILDING',
  expected_day_count integer not null default 14,
  actual_day_count integer not null default 0,
  source_system text not null,
  source_event_id text not null,
  source_revision text not null,
  source_event_time timestamptz not null,
  item_key text not null,
  source_hash text not null,
  generation_row_hash text not null,
  batch_receipt_id uuid not null references private.candidate_daily_batch_receipts(batch_receipt_id) on delete restrict,
  correlation_id text not null,
  activated_at_utc timestamptz,
  published_at_utc timestamptz,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  constraint candidate_daily_rota_generations_scope_fk foreign key (environment,candidate_id)
    references private.candidate_daily_authority_scopes(environment,candidate_id) on delete restrict,
  constraint candidate_daily_rota_generations_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_rota_generations_version_ck check (generation_version > 0),
  constraint candidate_daily_rota_generations_window_ck check (window_end-window_start=13),
  constraint candidate_daily_rota_generations_state_ck check (state in ('BUILDING','ACTIVE','SUPERSEDED','REJECTED')),
  constraint candidate_daily_rota_generations_day_count_ck check (
    expected_day_count=14 and actual_day_count between 0 and 14
    and (state<>'ACTIVE' or actual_day_count=14)
  ),
  constraint candidate_daily_rota_generations_hash_ck check (
    source_hash ~ '^[a-f0-9]{64}$' and generation_row_hash ~ '^[a-f0-9]{64}$'
  ),
  constraint candidate_daily_rota_generations_identity_uq unique (generation_id,environment,candidate_id),
  constraint candidate_daily_rota_generations_version_uq unique (environment,candidate_id,generation_version),
  constraint candidate_daily_rota_generations_source_uq unique (environment,source_system,source_event_id,item_key)
);

create unique index if not exists candidate_daily_rota_generations_active_uq
  on public.candidate_daily_rota_generations(environment,candidate_id) where state='ACTIVE';
create index if not exists candidate_daily_rota_generations_candidate_window_idx
  on public.candidate_daily_rota_generations(environment,candidate_id,window_start,window_end,state);

create table if not exists public.candidate_daily_rota_days (
  generation_id uuid not null,
  environment text not null,
  candidate_id uuid not null,
  rota_date date not null,
  booked boolean not null,
  system_blocked boolean not null,
  booking_id text,
  shift_starts_at timestamptz,
  shift_ends_at timestamptz,
  shift_info text,
  hospital text,
  ward text,
  job_title text,
  booking_ref text,
  shift_type text,
  timesheet_authorised boolean,
  timesheet_eligible boolean,
  action_target_kind text,
  action_timesheet_id uuid,
  action_contract_week_id uuid,
  action_workflow_id uuid,
  action_workflow_generation integer,
  action_row_signature text,
  source_row_hash text not null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  primary key (generation_id,rota_date),
  constraint candidate_daily_rota_days_generation_fk foreign key (generation_id,environment,candidate_id)
    references public.candidate_daily_rota_generations(generation_id,environment,candidate_id) on delete cascade,
  constraint candidate_daily_rota_days_identity_uq unique (generation_id,environment,candidate_id,rota_date),
  constraint candidate_daily_rota_days_booking_ck check (
    (booked and nullif(btrim(booking_id),'') is not null and shift_starts_at is not null
      and shift_ends_at is not null and shift_ends_at>shift_starts_at)
    or
    (not booked and booking_id is null and shift_starts_at is null and shift_ends_at is null)
  ),
  constraint candidate_daily_rota_days_text_bounds_ck check (
    length(coalesce(booking_id,''))<=128 and length(coalesce(shift_info,''))<=256
    and length(coalesce(hospital,''))<=160 and length(coalesce(ward,''))<=160
    and length(coalesce(job_title,''))<=160 and length(coalesce(booking_ref,''))<=128
    and length(coalesce(shift_type,''))<=80
  ),
  constraint candidate_daily_rota_days_action_kind_ck check (
    action_target_kind is null or action_target_kind in ('TIMESHEET_DETAIL','CONTRACT_WEEK_DETAIL','WORKFLOW_DETAIL')
  ),
  constraint candidate_daily_rota_days_action_shape_ck check (
    (action_target_kind is null and action_timesheet_id is null and action_contract_week_id is null
      and action_workflow_id is null and action_workflow_generation is null and action_row_signature is null)
    or
    (action_target_kind='TIMESHEET_DETAIL' and action_timesheet_id is not null
      and action_contract_week_id is null and action_workflow_generation is null
      and action_row_signature ~ '^[a-f0-9]{32}$')
    or
    (action_target_kind='CONTRACT_WEEK_DETAIL' and action_contract_week_id is not null
      and action_workflow_generation is null and action_row_signature ~ '^[a-f0-9]{32}$')
    or
    (action_target_kind='WORKFLOW_DETAIL' and action_workflow_id is not null
      and action_workflow_generation>0 and action_timesheet_id is null and action_contract_week_id is null
      and action_row_signature ~ '^[a-f0-9]{32}$')
  ),
  constraint candidate_daily_rota_days_hash_ck check (source_row_hash ~ '^[a-f0-9]{64}$')
);

create index if not exists candidate_daily_rota_days_candidate_date_idx
  on public.candidate_daily_rota_days(environment,candidate_id,rota_date);

alter table private.candidate_daily_authority_scopes
  add constraint candidate_daily_authority_scopes_active_generation_fk
  foreign key (active_generation_id,environment,candidate_id)
  references public.candidate_daily_rota_generations(generation_id,environment,candidate_id)
  deferrable initially deferred;

create table if not exists public.candidate_daily_availability_days (
  environment text not null,
  candidate_id uuid not null,
  availability_date date not null,
  preference text not null,
  availability_version bigint not null,
  source_class text not null,
  source_command_id uuid not null references public.candidate_daily_command_receipts(command_id) on delete restrict,
  changed_at_utc timestamptz not null default now(),
  changed_by_class text not null,
  row_hash text not null,
  primary key (environment,candidate_id,availability_date),
  constraint candidate_daily_availability_days_scope_fk foreign key (environment,candidate_id)
    references private.candidate_daily_authority_scopes(environment,candidate_id) on delete restrict,
  constraint candidate_daily_availability_days_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_availability_days_preference_ck check (
    preference in ('PENDING','NOT_AVAILABLE','LONG_DAY','NIGHT','LONG_DAY_OR_NIGHT')
  ),
  constraint candidate_daily_availability_days_version_ck check (availability_version > 0),
  constraint candidate_daily_availability_days_source_ck check (source_class in ('CANDIDATE','LEGACY_ADAPTER','SIGNED_SYSTEM')),
  constraint candidate_daily_availability_days_changed_by_ck check (changed_by_class in ('CANDIDATE','LEGACY_ADAPTER','SIGNED_SYSTEM')),
  constraint candidate_daily_availability_days_hash_ck check (row_hash ~ '^[a-f0-9]{64}$'),
  constraint candidate_daily_availability_days_evidence_uq unique (
    environment,candidate_id,availability_version,availability_date
  )
);

create index if not exists candidate_daily_availability_days_candidate_version_idx
  on public.candidate_daily_availability_days(environment,candidate_id,availability_version,availability_date);

create table if not exists public.candidate_daily_sheet_projection_outbox (
  outbox_id uuid primary key default gen_random_uuid(),
  environment text not null,
  target text not null default 'MASTER_AVAILABILITY_SHEET',
  candidate_id uuid not null,
  availability_date date not null,
  availability_version bigint not null,
  operation text not null default 'SET_AVAILABILITY',
  preference text not null,
  command_id uuid not null references public.candidate_daily_command_receipts(command_id) on delete restrict,
  state text not null default 'PENDING',
  delivery_attempt_count integer not null default 0,
  deferral_count integer not null default 0,
  next_available_at_utc timestamptz not null default now(),
  overlay_generation_id uuid,
  overlay_generation_version bigint,
  overlay_source_row_hash text,
  lease_owner text,
  lease_token text,
  lease_expires_at_utc timestamptz,
  target_logical_id text,
  observed_sheet_revision text,
  safe_error_code text,
  alerted_at_utc timestamptz,
  completed_at_utc timestamptz,
  correlation_id text not null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  constraint candidate_daily_sheet_projection_outbox_scope_fk foreign key (environment,candidate_id)
    references private.candidate_daily_authority_scopes(environment,candidate_id) on delete restrict,
  constraint candidate_daily_sheet_projection_outbox_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_sheet_projection_outbox_target_ck check (target='MASTER_AVAILABILITY_SHEET'),
  constraint candidate_daily_sheet_projection_outbox_operation_ck check (operation='SET_AVAILABILITY'),
  constraint candidate_daily_sheet_projection_outbox_preference_ck check (
    preference in ('PENDING','NOT_AVAILABLE','LONG_DAY','NIGHT','LONG_DAY_OR_NIGHT')
  ),
  constraint candidate_daily_sheet_projection_outbox_state_ck check (
    state in ('PENDING','CLAIMED','RETRY','DEFERRED_OVERLAY','DELIVERED','TERMINAL')
  ),
  constraint candidate_daily_sheet_projection_outbox_version_ck check (availability_version > 0),
  constraint candidate_daily_sheet_projection_outbox_attempts_ck check (
    delivery_attempt_count between 0 and 12 and deferral_count >= 0
  ),
  constraint candidate_daily_sheet_projection_outbox_lease_ck check (
    (state='CLAIMED' and nullif(btrim(lease_owner),'') is not null and nullif(btrim(lease_token),'') is not null
      and lease_expires_at_utc is not null)
    or state<>'CLAIMED'
  ),
  constraint candidate_daily_sheet_projection_outbox_overlay_ck check (
    (state='DEFERRED_OVERLAY' and overlay_generation_id is not null and overlay_generation_version is not null
      and overlay_source_row_hash ~ '^[a-f0-9]{64}$')
    or state<>'DEFERRED_OVERLAY'
  ),
  constraint candidate_daily_sheet_projection_outbox_dedupe_uq unique (
    environment,target,candidate_id,availability_date,availability_version,operation
  )
);

create index if not exists candidate_daily_projection_claim_ready_idx
  on public.candidate_daily_sheet_projection_outbox(target,state,next_available_at_utc,created_at_utc)
  where state in ('PENDING','RETRY');
create index if not exists candidate_daily_projection_candidate_cursor_idx
  on public.candidate_daily_sheet_projection_outbox(environment,candidate_id,availability_version,state);

create table if not exists private.candidate_daily_sync_state (
  environment text not null,
  candidate_id uuid not null,
  target text not null,
  accepted_canonical_cursor bigint not null default 0,
  required_visible_cursor bigint not null default 0,
  delivered_visible_cursor bigint not null default 0,
  overlay_proof_cursor bigint not null default 0,
  effective_visible_cursor bigint not null default 0,
  observed_source_revision text,
  pending_count integer not null default 0,
  retry_count integer not null default 0,
  deferred_count integer not null default 0,
  terminal_count integer not null default 0,
  last_acknowledged_at_utc timestamptz,
  last_pulled_at_utc timestamptz,
  last_reconciled_at_utc timestamptz,
  state text not null default 'READY',
  safe_error_code text,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  primary key (environment,candidate_id,target),
  constraint candidate_daily_sync_state_scope_fk foreign key (environment,candidate_id)
    references private.candidate_daily_authority_scopes(environment,candidate_id) on delete restrict,
  constraint candidate_daily_sync_state_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_sync_state_target_ck check (target='MASTER_AVAILABILITY_SHEET'),
  constraint candidate_daily_sync_state_cursor_ck check (
    accepted_canonical_cursor >= 0 and required_visible_cursor >= 0 and delivered_visible_cursor >= 0
    and overlay_proof_cursor >= 0 and effective_visible_cursor >= 0
    and effective_visible_cursor <= accepted_canonical_cursor
    and delivered_visible_cursor <= accepted_canonical_cursor
    and overlay_proof_cursor <= accepted_canonical_cursor
  ),
  constraint candidate_daily_sync_state_counts_ck check (
    pending_count>=0 and retry_count>=0 and deferred_count>=0 and terminal_count>=0
  ),
  constraint candidate_daily_sync_state_state_ck check (state in ('READY','LAGGING','BLOCKED','ERROR'))
);

create table if not exists private.candidate_daily_authority_transitions (
  transition_id uuid primary key default gen_random_uuid(),
  batch_receipt_id uuid not null references private.candidate_daily_batch_receipts(batch_receipt_id) on delete restrict,
  environment text not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  prior_authority_mode text not null,
  new_authority_mode text not null,
  effective_at_utc timestamptz not null,
  canonical_version_snapshot bigint not null,
  generation_id_snapshot uuid,
  generation_version_snapshot bigint,
  sync_snapshot_json jsonb not null,
  in_flight_disposition text not null,
  entitlement_before boolean not null,
  entitlement_after boolean not null,
  actor_user_id uuid,
  independent_approver_user_id uuid not null,
  reason text not null,
  evidence_sha256 text not null,
  outcome text not null,
  supersedes_transition_id uuid references private.candidate_daily_authority_transitions(transition_id) on delete restrict,
  correlation_id text not null,
  created_at_utc timestamptz not null default now(),
  constraint candidate_daily_authority_transitions_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_authority_transitions_modes_ck check (
    prior_authority_mode in ('GOOGLE_PRIMARY','ROLLBACK_PENDING','SUPABASE_PRIMARY')
    and new_authority_mode in ('GOOGLE_PRIMARY','ROLLBACK_PENDING','SUPABASE_PRIMARY')
  ),
  constraint candidate_daily_authority_transitions_version_ck check (canonical_version_snapshot>=0),
  constraint candidate_daily_authority_transitions_sync_ck check (jsonb_typeof(sync_snapshot_json)='object'),
  constraint candidate_daily_authority_transitions_inflight_ck check (
    in_flight_disposition in ('DRAINED','CANCELLED','RECONCILED','NONE')
  ),
  constraint candidate_daily_authority_transitions_reason_ck check (length(btrim(reason)) between 1 and 500),
  constraint candidate_daily_authority_transitions_evidence_ck check (evidence_sha256 ~ '^[a-f0-9]{64}$'),
  constraint candidate_daily_authority_transitions_outcome_ck check (outcome in ('COMMITTED','REJECTED')),
  constraint candidate_daily_authority_transitions_batch_candidate_uq unique (batch_receipt_id,candidate_id)
);

alter table private.candidate_daily_authority_scopes
  add constraint candidate_daily_authority_scopes_last_transition_fk
  foreign key (last_transition_id)
  references private.candidate_daily_authority_transitions(transition_id)
  deferrable initially deferred;

create or replace function private._candidate_daily_transition_immutable_v1()
returns trigger language plpgsql set search_path='' as $function$
begin
  raise exception using errcode='55000', message='CANDIDATE_DAILY_TRANSITION_IMMUTABLE';
end;
$function$;

drop trigger if exists candidate_daily_authority_transitions_immutable
  on private.candidate_daily_authority_transitions;
create trigger candidate_daily_authority_transitions_immutable
before update or delete on private.candidate_daily_authority_transitions
for each row execute function private._candidate_daily_transition_immutable_v1();

create table if not exists private.candidate_daily_external_effect_receipts (
  effect_receipt_id uuid primary key default gen_random_uuid(),
  environment text not null,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  effect_key text not null,
  operation text not null,
  shift_identity text,
  source_event_identity text,
  request_hash text not null,
  idempotency_key text not null,
  state text not null default 'IN_PROGRESS',
  first_claimed_at_utc timestamptz not null,
  lease_owner text,
  lease_token text,
  lease_expires_at_utc timestamptz,
  stable_provider_request_id text not null,
  provider_result_identity text,
  provider_reference_hash text,
  attempt_count integer not null default 1,
  safe_evidence_json jsonb not null default '{}'::jsonb,
  terminal_result_json jsonb,
  terminal_body_sha256 text,
  reconciliation_outcome text,
  correlation_id text not null,
  retain_until_utc timestamptz not null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  completed_at_utc timestamptz,
  constraint candidate_daily_external_effects_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_daily_external_effects_key_ck check (length(effect_key) between 16 and 256),
  constraint candidate_daily_external_effects_operation_ck check (operation in (
    'RUNNING_LATE_SEND','CANNOT_ATTEND','LEAVE_EARLY','DNA','MESSAGE_SEEN','ESCALATION_STEP','ACKNOWLEDGEMENT'
  )),
  constraint candidate_daily_external_effects_hash_ck check (request_hash ~ '^[a-f0-9]{64}$'),
  constraint candidate_daily_external_effects_idempotency_ck check (length(idempotency_key) between 16 and 160),
  constraint candidate_daily_external_effects_state_ck check (state in ('IN_PROGRESS','COMPLETED','FAILED_FINAL','UNKNOWN')),
  constraint candidate_daily_external_effects_provider_hash_ck check (
    provider_reference_hash is null or provider_reference_hash ~ '^[a-f0-9]{64}$'
  ),
  constraint candidate_daily_external_effects_attempt_ck check (attempt_count between 1 and 100),
  constraint candidate_daily_external_effects_safe_evidence_ck check (jsonb_typeof(safe_evidence_json)='object'),
  constraint candidate_daily_external_effects_terminal_ck check (
    (state='IN_PROGRESS' and terminal_result_json is null and terminal_body_sha256 is null and completed_at_utc is null)
    or
    (state in ('COMPLETED','FAILED_FINAL','UNKNOWN') and terminal_result_json is not null
      and terminal_body_sha256 ~ '^[a-f0-9]{64}$' and completed_at_utc is not null)
  ),
  constraint candidate_daily_external_effects_effect_uq unique (environment,effect_key),
  constraint candidate_daily_external_effects_idempotency_uq unique (
    environment,candidate_id,operation,idempotency_key
  )
);

create index if not exists candidate_daily_external_effects_lease_idx
  on private.candidate_daily_external_effect_receipts(environment,state,lease_expires_at_utc);
create index if not exists candidate_daily_external_effects_candidate_idx
  on private.candidate_daily_external_effect_receipts(environment,candidate_id,operation,created_at_utc);

alter table public.candidate_daily_availability_days enable row level security;
alter table public.candidate_daily_command_receipts enable row level security;
alter table public.candidate_daily_rota_generations enable row level security;
alter table public.candidate_daily_rota_days enable row level security;
alter table public.candidate_daily_sheet_projection_outbox enable row level security;
alter table private.candidate_daily_source_links enable row level security;
alter table private.candidate_daily_batch_receipts enable row level security;
alter table private.candidate_daily_sync_state enable row level security;
alter table private.candidate_daily_entitlements enable row level security;
alter table private.candidate_daily_authority_scopes enable row level security;
alter table private.candidate_daily_authority_transitions enable row level security;
alter table private.candidate_daily_external_effect_receipts enable row level security;

revoke all on table public.candidate_daily_availability_days from public;
revoke all on table public.candidate_daily_command_receipts from public;
revoke all on table public.candidate_daily_rota_generations from public;
revoke all on table public.candidate_daily_rota_days from public;
revoke all on table public.candidate_daily_sheet_projection_outbox from public;
revoke all on table private.candidate_daily_source_links from public;
revoke all on table private.candidate_daily_batch_receipts from public;
revoke all on table private.candidate_daily_sync_state from public;
revoke all on table private.candidate_daily_entitlements from public;
revoke all on table private.candidate_daily_authority_scopes from public;
revoke all on table private.candidate_daily_authority_transitions from public;
revoke all on table private.candidate_daily_external_effect_receipts from public;
revoke all on function private._candidate_daily_transition_immutable_v1() from public;

do $grant$
begin
  if exists(select 1 from pg_roles where rolname='anon') then
    execute 'revoke all on table public.candidate_daily_availability_days, public.candidate_daily_command_receipts, public.candidate_daily_rota_generations, public.candidate_daily_rota_days, public.candidate_daily_sheet_projection_outbox, private.candidate_daily_source_links, private.candidate_daily_batch_receipts, private.candidate_daily_sync_state, private.candidate_daily_entitlements, private.candidate_daily_authority_scopes, private.candidate_daily_authority_transitions, private.candidate_daily_external_effect_receipts from anon';
  end if;
  if exists(select 1 from pg_roles where rolname='authenticated') then
    execute 'revoke all on table public.candidate_daily_availability_days, public.candidate_daily_command_receipts, public.candidate_daily_rota_generations, public.candidate_daily_rota_days, public.candidate_daily_sheet_projection_outbox, private.candidate_daily_source_links, private.candidate_daily_batch_receipts, private.candidate_daily_sync_state, private.candidate_daily_entitlements, private.candidate_daily_authority_scopes, private.candidate_daily_authority_transitions, private.candidate_daily_external_effect_receipts from authenticated';
  end if;
  if exists(select 1 from pg_roles where rolname='service_role') then
    execute 'grant usage on schema private to service_role';
    execute 'revoke all on table public.candidate_daily_availability_days, public.candidate_daily_command_receipts, public.candidate_daily_rota_generations, public.candidate_daily_rota_days, public.candidate_daily_sheet_projection_outbox, private.candidate_daily_source_links, private.candidate_daily_batch_receipts, private.candidate_daily_sync_state, private.candidate_daily_entitlements, private.candidate_daily_authority_scopes, private.candidate_daily_authority_transitions, private.candidate_daily_external_effect_receipts from service_role';
  end if;
end;
$grant$;

commit;
