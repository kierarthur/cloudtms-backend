-- CloudTMS Candidate App foundation schema.
--
-- Additive and dormant by design:
--   * every Candidate App feature flag is false;
--   * global candidate electronic auto-authorisation is false;
--   * no accounts, challenges, sessions, workflows, evidence or mail are created;
--   * no existing timesheet, TSFIN, invoice, payment or Banking Pay row is changed.
--
-- This migration is written for TEST first.  A future LIVE installation must
-- set settings_defaults.candidate_app_environment to LIVE in its separately
-- approved environment configuration before any Candidate App route is enabled.

alter table public.settings_defaults
  add column if not exists candidate_app_environment text not null default 'TEST',
  add column if not exists candidate_app_feature_flags_json jsonb not null default
    '{"candidate_account_registration":false,"candidate_app_reads":false,"candidate_app_writes":false,"candidate_record_role_capabilities":false,"candidate_expense_atomic_placement":false,"candidate_expense_invoice_routing_v1":false,"candidate_manager_approval":false,"candidate_paper_qr":false,"candidate_notifications":false,"candidate_daily_finalisation":false,"candidate_settings":false,"candidate_route_confirmation":false}'::jsonb,
  add column if not exists candidate_app_system_actor_user_id uuid,
  add column if not exists candidate_electronic_auto_authorise_default boolean not null default false,
  add column if not exists candidate_hours_deviation_pct numeric(6,2) not null default 30,
  add column if not exists candidate_barred_manager_email_domains jsonb not null default '[]'::jsonb;

alter table public.client_settings
  add column if not exists candidate_electronic_auto_authorise boolean,
  add column if not exists candidate_expenses_require_separate_timesheet boolean not null default false,
  add column if not exists candidate_paper_submission_enabled boolean not null default false,
  add column if not exists candidate_expense_invoice_email text,
  add column if not exists candidate_manager_approval_policy_json jsonb not null default
    '{"approved_emails":[],"approved_domains":[],"allow_free_business_email":false}'::jsonb,
  add column if not exists allow_daily_manager_authorise_on_phone boolean not null default true,
  add column if not exists allow_daily_manager_authorise_by_email boolean not null default false;

alter table public.contracts
  add column if not exists candidate_electronic_auto_authorise_override boolean,
  add column if not exists candidate_expenses_require_separate_timesheet_override boolean,
  add column if not exists candidate_paper_submission_enabled_override boolean,
  add column if not exists candidate_expense_invoice_email_override text,
  add column if not exists candidate_manager_approval_policy_json jsonb not null default '{"mode":"INHERIT"}'::jsonb;

-- An unsigned DAILY row cannot physically be ELECTRONIC because the existing
-- two-signature constraint is intentionally unchanged and DAILY has no
-- contract-week submission snapshot.  This constrained server-owned intent is
-- the durable logical route until finalisation writes both signatures.
alter table public.timesheets
  add column if not exists candidate_submission_route_intent text;

do $migration$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.settings_defaults'::regclass
      and conname = 'settings_defaults_candidate_app_system_actor_fk'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_candidate_app_system_actor_fk
      foreign key (candidate_app_system_actor_user_id)
      references public.tms_users(id)
      on delete restrict;
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.settings_defaults'::regclass
      and conname = 'settings_defaults_candidate_app_environment_ck'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_candidate_app_environment_ck
      check (candidate_app_environment in ('TEST','LIVE'));
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.settings_defaults'::regclass
      and conname = 'settings_defaults_candidate_app_flags_object_ck'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_candidate_app_flags_object_ck
      check (jsonb_typeof(candidate_app_feature_flags_json) = 'object');
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.settings_defaults'::regclass
      and conname = 'settings_defaults_candidate_deviation_ck'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_candidate_deviation_ck
      check (candidate_hours_deviation_pct between 0 and 500);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.settings_defaults'::regclass
      and conname = 'settings_defaults_candidate_barred_domains_array_ck'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_candidate_barred_domains_array_ck
      check (jsonb_typeof(candidate_barred_manager_email_domains) = 'array');
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.client_settings'::regclass
      and conname = 'client_settings_candidate_manager_policy_object_ck'
  ) then
    alter table public.client_settings
      add constraint client_settings_candidate_manager_policy_object_ck
      check (jsonb_typeof(candidate_manager_approval_policy_json) = 'object');
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.client_settings'::regclass
      and conname = 'client_settings_candidate_daily_method_ck'
  ) then
    alter table public.client_settings
      add constraint client_settings_candidate_daily_method_ck
      check (allow_daily_manager_authorise_on_phone or allow_daily_manager_authorise_by_email);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.client_settings'::regclass
      and conname = 'client_settings_candidate_expense_email_ck'
  ) then
    alter table public.client_settings
      add constraint client_settings_candidate_expense_email_ck
      check (
        candidate_expense_invoice_email is null
        or btrim(candidate_expense_invoice_email) = ''
        or candidate_expense_invoice_email ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
      );
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.contracts'::regclass
      and conname = 'contracts_candidate_manager_policy_object_ck'
  ) then
    alter table public.contracts
      add constraint contracts_candidate_manager_policy_object_ck
      check (jsonb_typeof(candidate_manager_approval_policy_json) = 'object');
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.contracts'::regclass
      and conname = 'contracts_candidate_expense_email_ck'
  ) then
    alter table public.contracts
      add constraint contracts_candidate_expense_email_ck
      check (
        candidate_expense_invoice_email_override is null
        or btrim(candidate_expense_invoice_email_override) = ''
        or candidate_expense_invoice_email_override ~* '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
      );
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.timesheets'::regclass
      and conname = 'timesheets_candidate_submission_route_intent_ck'
  ) then
    alter table public.timesheets
      add constraint timesheets_candidate_submission_route_intent_ck
      check (
        candidate_submission_route_intent is null
        or (
          candidate_submission_route_intent = 'ELECTRONIC'
          and submission_mode = 'MANUAL'::public.submission_mode_enum
          and sheet_scope = 'DAILY'::public.timesheet_scope_enum
        )
        or (
          candidate_submission_route_intent = 'PAPER'
          and submission_mode = 'MANUAL'::public.submission_mode_enum
          and sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
        )
      );
  end if;
end;
$migration$;

create table if not exists public.candidate_app_accounts (
  id uuid primary key default gen_random_uuid(),
  environment text not null,
  email_normalized text not null,
  status text not null default 'SETUP_REQUIRED',
  password_scheme text,
  password_scheme_version smallint,
  password_salt bytea,
  password_digest bytea,
  password_params_json jsonb not null default '{}'::jsonb,
  password_changed_at_utc timestamptz,
  failed_login_count integer not null default 0,
  locked_until_utc timestamptz,
  notification_preferences_json jsonb not null default
    '{"manager_approval":true,"manager_refusal":true,"authorisation":true,"payment":true,"office_rejection":true,"resubmission_required":true}'::jsonb,
  session_version bigint not null default 1,
  last_login_at_utc timestamptz,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  constraint candidate_app_accounts_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_app_accounts_email_normalized_ck check (
    email_normalized = lower(btrim(email_normalized))
    and email_normalized ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
  ),
  constraint candidate_app_accounts_status_ck check (status in ('SETUP_REQUIRED','ACTIVE','LOCKED','DISABLED')),
  constraint candidate_app_accounts_password_group_ck check (
    (
      password_scheme is null and password_scheme_version is null
      and password_salt is null and password_digest is null
      and password_changed_at_utc is null
    )
    or
    (
      nullif(btrim(password_scheme),'') is not null
      and password_scheme_version is not null and password_scheme_version > 0
      and password_salt is not null and octet_length(password_salt) between 16 and 64
      and password_digest is not null and octet_length(password_digest) between 32 and 128
      and password_changed_at_utc is not null
    )
  ),
  constraint candidate_app_accounts_password_params_ck check (jsonb_typeof(password_params_json) = 'object'),
  constraint candidate_app_accounts_preferences_ck check (jsonb_typeof(notification_preferences_json) = 'object'),
  constraint candidate_app_accounts_failed_login_ck check (failed_login_count between 0 and 1000),
  constraint candidate_app_accounts_session_version_ck check (session_version > 0),
  constraint candidate_app_accounts_environment_email_uq unique (environment,email_normalized)
);

create table if not exists public.candidate_app_sessions (
  id uuid primary key default gen_random_uuid(),
  account_id uuid not null references public.candidate_app_accounts(id) on delete cascade,
  environment text not null,
  selected_candidate_id uuid references public.candidates(id) on delete set null,
  status text not null default 'ACTIVE',
  refresh_token_hash bytea not null,
  token_family_id uuid not null default gen_random_uuid(),
  rotation integer not null default 0,
  issued_at_utc timestamptz not null default now(),
  expires_at_utc timestamptz not null,
  absolute_expires_at_utc timestamptz not null,
  last_used_at_utc timestamptz not null default now(),
  revoked_at_utc timestamptz,
  revoke_reason text,
  replaced_by_session_id uuid,
  device_id_hash bytea,
  platform text,
  push_provider text,
  push_token_ciphertext bytea,
  push_key_version smallint,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  constraint candidate_app_sessions_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_app_sessions_status_ck check (status in ('ACTIVE','ROTATED','REVOKED','EXPIRED')),
  constraint candidate_app_sessions_refresh_hash_ck check (octet_length(refresh_token_hash) = 32),
  constraint candidate_app_sessions_device_hash_ck check (device_id_hash is null or octet_length(device_id_hash) = 32),
  constraint candidate_app_sessions_rotation_ck check (rotation >= 0),
  constraint candidate_app_sessions_expiry_ck check (expires_at_utc <= absolute_expires_at_utc),
  constraint candidate_app_sessions_refresh_hash_uq unique (refresh_token_hash),
  constraint candidate_app_sessions_replaced_by_fk foreign key (replaced_by_session_id)
    references public.candidate_app_sessions(id) on delete set null
);

create table if not exists public.candidate_auth_challenges (
  id uuid primary key default gen_random_uuid(),
  account_id uuid references public.candidate_app_accounts(id) on delete cascade,
  environment text not null,
  email_normalized text not null,
  purpose text not null,
  state text not null default 'PENDING',
  token_hash bytea not null,
  expires_at_utc timestamptz not null,
  attempt_count integer not null default 0,
  resend_count integer not null default 0,
  last_sent_at_utc timestamptz,
  verified_at_utc timestamptz,
  consumed_at_utc timestamptz,
  superseded_at_utc timestamptz,
  superseded_by_id uuid references public.candidate_auth_challenges(id) on delete set null,
  mail_outbox_id uuid references public.mail_outbox(id) on delete set null,
  deterministic_outbox_key text not null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  constraint candidate_auth_challenges_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_auth_challenges_email_ck check (
    email_normalized = lower(btrim(email_normalized))
    and email_normalized ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
  ),
  constraint candidate_auth_challenges_purpose_ck check (purpose in ('ACTIVATE','RESET','RECOVERY')),
  constraint candidate_auth_challenges_state_ck check (state in ('PENDING','VERIFIED','CONSUMED','EXPIRED','SUPERSEDED')),
  constraint candidate_auth_challenges_token_hash_ck check (octet_length(token_hash) = 32),
  constraint candidate_auth_challenges_attempt_ck check (attempt_count between 0 and 5),
  constraint candidate_auth_challenges_resend_ck check (resend_count between 0 and 5),
  constraint candidate_auth_challenges_token_hash_uq unique (token_hash),
  constraint candidate_auth_challenges_outbox_key_uq unique (deterministic_outbox_key)
);

create table if not exists public.candidate_submission_workflows (
  id uuid primary key default gen_random_uuid(),
  environment text not null,
  account_id uuid not null references public.candidate_app_accounts(id) on delete restrict,
  candidate_id uuid not null references public.candidates(id) on delete restrict,
  workflow_kind text not null,
  scope text not null,
  route text not null,
  state text not null default 'CREATED',
  generation integer not null default 1,
  contract_id uuid references public.contracts(id) on delete restrict,
  contract_week_id uuid references public.contract_weeks(id) on delete restrict,
  anchor_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  target_timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  work_date date,
  week_ending_date date,
  policy_snapshot_json jsonb not null default '{}'::jsonb,
  input_snapshot_json jsonb not null default '{}'::jsonb,
  issue_codes jsonb not null default '[]'::jsonb,
  expected_row_signature text,
  capability_hash text,
  rejection_reason text,
  rejection_scope text,
  idempotency_key text not null,
  last_mutation_idempotency_key text,
  last_mutation_response_json jsonb,
  worker_submitted_at_utc timestamptz,
  finalised_at_utc timestamptz,
  cancelled_at_utc timestamptz,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  constraint candidate_submission_workflows_environment_ck check (environment in ('TEST','LIVE')),
  constraint candidate_submission_workflows_kind_ck check (workflow_kind in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED','DAILY')),
  constraint candidate_submission_workflows_scope_ck check (scope in ('WEEKLY','DAILY')),
  constraint candidate_submission_workflows_route_ck check (route in ('ELECTRONIC','PHONE','EMAIL','PAPER')),
  constraint candidate_submission_workflows_kind_scope_route_ck check (
    (
      workflow_kind in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED')
      and scope='WEEKLY'
    )
    or (
      workflow_kind='DAILY'
      and scope='DAILY'
      and route in ('PHONE','EMAIL')
    )
  ),
  constraint candidate_submission_workflows_identity_shape_ck check (
    (
      workflow_kind in ('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED')
      and contract_id is not null
      and contract_week_id is not null
      and week_ending_date is not null
    )
    or (
      workflow_kind='DAILY'
      and target_timesheet_id is not null
      and anchor_timesheet_id=target_timesheet_id
      and work_date is not null
      and contract_week_id is null
      and week_ending_date is null
    )
  ),
  constraint candidate_submission_workflows_state_ck check (state in (
    'CREATED','WORKER_SUBMITTED','AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'AWAITING_PAPER_RETURN','RECEIVED','FINALISED','REFUSED','REJECTED',
    'CANCELLED','EXPIRED','SUPERSEDED'
  )),
  constraint candidate_submission_workflows_generation_ck check (generation > 0),
  constraint candidate_submission_workflows_policy_ck check (jsonb_typeof(policy_snapshot_json) = 'object'),
  constraint candidate_submission_workflows_input_ck check (jsonb_typeof(input_snapshot_json) = 'object'),
  constraint candidate_submission_workflows_issues_ck check (jsonb_typeof(issue_codes) = 'array'),
  constraint candidate_submission_workflows_idempotency_ck check (nullif(btrim(idempotency_key),'') is not null),
  constraint candidate_submission_workflows_account_idempotency_uq unique (account_id,idempotency_key)
);

create table if not exists public.candidate_submission_components (
  id uuid primary key default gen_random_uuid(),
  workflow_id uuid not null references public.candidate_submission_workflows(id) on delete restrict,
  workflow_generation integer not null,
  component_no integer not null,
  timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  component_kind text not null,
  expense_category text,
  document_role text not null,
  state text not null default 'PENDING',
  source_component_id uuid references public.candidate_submission_components(id) on delete restrict,
  storage_key text,
  media_type text,
  byte_size bigint,
  source_content_sha256 bytea,
  upload_idempotency_key text,
  created_at_utc timestamptz not null default now(),
  immutable_at_utc timestamptz,
  superseded_at_utc timestamptz,
  manager_reviewed_at_utc timestamptz,
  manager_approved_at_utc timestamptz,
  constraint candidate_submission_components_generation_ck check (workflow_generation > 0),
  constraint candidate_submission_components_number_ck check (component_no > 0),
  constraint candidate_submission_components_kind_ck check (component_kind in (
    'HOURS_TIMESHEET','EXPENSE_SUMMARY','MILEAGE_FORM','EXPENSE_EVIDENCE',
    'SIGNED_RETURN','MANAGER_SIGNATURE','CANDIDATE_SIGNATURE','PAPER_DOCUMENT'
  )),
  constraint candidate_submission_components_category_ck check (
    (component_kind='EXPENSE_EVIDENCE' and expense_category is not null
      and expense_category in ('TRAVEL','ACCOMMODATION','OTHER','MILEAGE'))
    or (component_kind='MILEAGE_FORM' and expense_category is not null and expense_category='MILEAGE')
    or (component_kind not in ('EXPENSE_EVIDENCE','MILEAGE_FORM') and expense_category is null)
  ),
  constraint candidate_submission_components_role_ck check (document_role in (
    'SOURCE_EVIDENCE','MILEAGE_CLAIM_FORM','EXPENSE_MILEAGE_APPROVAL_SUMMARY',
    'SIGNED_TIMESHEET','MANAGER_SIGNATURE','CANDIDATE_SIGNATURE'
  )),
  constraint candidate_submission_components_state_ck check (state in ('PENDING','UPLOADED','IMMUTABLE','SUPERSEDED','REJECTED','ABANDONED')),
  constraint candidate_submission_components_size_ck check (byte_size is null or byte_size between 1 and 15728640),
  constraint candidate_submission_components_digest_ck check (
    source_content_sha256 is null or octet_length(source_content_sha256) = 32
  ),
  constraint candidate_submission_components_immutable_ck check (
    state <> 'IMMUTABLE'
    or (
      immutable_at_utc is not null
      and (
        component_kind in ('HOURS_TIMESHEET','EXPENSE_SUMMARY')
        or (
          storage_key is not null
          and byte_size is not null
          and source_content_sha256 is not null
        )
        or source_component_id is not null
      )
    )
  ),
  constraint candidate_submission_components_workflow_component_uq unique (workflow_id,workflow_generation,component_no)
);

create table if not exists public.candidate_approval_requests (
  id uuid primary key default gen_random_uuid(),
  workflow_id uuid not null references public.candidate_submission_workflows(id) on delete restrict,
  workflow_generation integer not null,
  method text not null,
  state text not null default 'PENDING',
  manager_email_normalized text,
  manager_name text,
  manager_position text,
  token_hash bytea,
  expires_at_utc timestamptz,
  initial_sent_at_utc timestamptz,
  resend_count integer not null default 0,
  last_sent_at_utc timestamptz,
  next_reminder_at_utc timestamptz,
  renewal_count integer not null default 0,
  refusal_reason text,
  signature_component_id uuid references public.candidate_submission_components(id) on delete restrict,
  approved_at_utc timestamptz,
  refused_at_utc timestamptz,
  cancelled_at_utc timestamptz,
  superseded_at_utc timestamptz,
  idempotency_key text,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  constraint candidate_approval_requests_generation_ck check (workflow_generation > 0),
  constraint candidate_approval_requests_method_ck check (method in ('PHONE','EMAIL')),
  constraint candidate_approval_requests_state_ck check (state in ('PENDING','APPROVED','REFUSED','CANCELLED','EXPIRED','SUPERSEDED')),
  constraint candidate_approval_requests_email_ck check (
    manager_email_normalized is null
    or (
      manager_email_normalized = lower(btrim(manager_email_normalized))
      and manager_email_normalized ~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
    )
  ),
  constraint candidate_approval_requests_token_ck check (token_hash is null or octet_length(token_hash) = 32),
  constraint candidate_approval_requests_email_method_ck check (
    method <> 'EMAIL'
    or (manager_email_normalized is not null and token_hash is not null and expires_at_utc is not null)
  ),
  constraint candidate_approval_requests_resend_ck check (resend_count between 0 and 5),
  constraint candidate_approval_requests_renewal_ck check (renewal_count between 0 and 100)
);

create table if not exists public.candidate_notifications (
  id uuid primary key default gen_random_uuid(),
  account_id uuid not null references public.candidate_app_accounts(id) on delete cascade,
  candidate_id uuid references public.candidates(id) on delete restrict,
  workflow_id uuid references public.candidate_submission_workflows(id) on delete restrict,
  timesheet_id uuid references public.timesheets(timesheet_id) on delete restrict,
  event_type text not null,
  preference_category text not null,
  template_key text not null,
  template_params jsonb not null default '{}'::jsonb,
  deep_link_json jsonb not null default '{}'::jsonb,
  state text not null default 'UNREAD',
  push_state text not null default 'PENDING',
  dedupe_key text not null,
  created_at_utc timestamptz not null default now(),
  read_at_utc timestamptz,
  dismissed_at_utc timestamptz,
  push_claimed_at_utc timestamptz,
  push_sent_at_utc timestamptz,
  push_failed_at_utc timestamptz,
  last_error text,
  constraint candidate_notifications_template_params_ck check (jsonb_typeof(template_params) = 'object'),
  constraint candidate_notifications_deep_link_ck check (jsonb_typeof(deep_link_json) = 'object'),
  constraint candidate_notifications_state_ck check (state in ('UNREAD','READ','DISMISSED')),
  constraint candidate_notifications_push_state_ck check (push_state in ('PENDING','CLAIMED','SENT','SKIPPED','FAILED')),
  constraint candidate_notifications_dedupe_ck check (nullif(btrim(dedupe_key),'') is not null),
  constraint candidate_notifications_dedupe_uq unique (dedupe_key)
);

create index if not exists candidate_app_accounts_status_idx
  on public.candidate_app_accounts(environment,status,email_normalized);
create index if not exists candidate_app_sessions_account_status_idx
  on public.candidate_app_sessions(account_id,status,expires_at_utc);
create index if not exists candidate_app_sessions_family_rotation_idx
  on public.candidate_app_sessions(token_family_id,rotation desc);
create index if not exists candidate_auth_challenges_email_state_idx
  on public.candidate_auth_challenges(environment,email_normalized,purpose,state,created_at_utc desc);
create unique index if not exists candidate_auth_challenges_one_pending_uq
  on public.candidate_auth_challenges(environment,email_normalized,purpose)
  where state in ('PENDING','VERIFIED');
create index if not exists candidate_submission_workflows_candidate_state_idx
  on public.candidate_submission_workflows(candidate_id,state,week_ending_date desc,created_at_utc desc);
create index if not exists candidate_submission_workflows_timesheet_idx
  on public.candidate_submission_workflows(target_timesheet_id,state);
create unique index if not exists candidate_submission_workflows_one_active_expense_uq
  on public.candidate_submission_workflows(candidate_id,contract_id,week_ending_date)
  where workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
    and state in ('CREATED','WORKER_SUBMITTED','AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','AWAITING_PAPER_RETURN','RECEIVED');
create unique index if not exists candidate_submission_components_source_sha256_uq
  on public.candidate_submission_components(source_content_sha256)
  where source_content_sha256 is not null
    and source_component_id is null
    and component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE','SIGNED_RETURN');
create unique index if not exists candidate_submission_components_storage_key_uq
  on public.candidate_submission_components(storage_key)
  where storage_key is not null and source_component_id is null;
create unique index if not exists candidate_submission_components_upload_idempotency_uq
  on public.candidate_submission_components(workflow_id,upload_idempotency_key)
  where upload_idempotency_key is not null;
create unique index if not exists candidate_approval_requests_token_hash_uq
  on public.candidate_approval_requests(token_hash)
  where token_hash is not null;
create unique index if not exists candidate_approval_requests_one_live_uq
  on public.candidate_approval_requests(workflow_id,workflow_generation)
  where state = 'PENDING';
create index if not exists candidate_approval_requests_reminder_idx
  on public.candidate_approval_requests(next_reminder_at_utc,id)
  where state = 'PENDING' and method = 'EMAIL';
create index if not exists candidate_notifications_account_page_idx
  on public.candidate_notifications(account_id,state,created_at_utc desc,id desc);
create index if not exists candidate_notifications_push_claim_idx
  on public.candidate_notifications(push_state,created_at_utc,id)
  where push_state in ('PENDING','FAILED');

alter table public.candidate_app_accounts enable row level security;
alter table public.candidate_app_accounts force row level security;
alter table public.candidate_app_sessions enable row level security;
alter table public.candidate_app_sessions force row level security;
alter table public.candidate_auth_challenges enable row level security;
alter table public.candidate_auth_challenges force row level security;
alter table public.candidate_submission_workflows enable row level security;
alter table public.candidate_submission_workflows force row level security;
alter table public.candidate_submission_components enable row level security;
alter table public.candidate_submission_components force row level security;
alter table public.candidate_approval_requests enable row level security;
alter table public.candidate_approval_requests force row level security;
alter table public.candidate_notifications enable row level security;
alter table public.candidate_notifications force row level security;

revoke all on table public.candidate_app_accounts from public,anon,authenticated;
revoke all on table public.candidate_app_sessions from public,anon,authenticated;
revoke all on table public.candidate_auth_challenges from public,anon,authenticated;
revoke all on table public.candidate_submission_workflows from public,anon,authenticated;
revoke all on table public.candidate_submission_components from public,anon,authenticated;
revoke all on table public.candidate_approval_requests from public,anon,authenticated;
revoke all on table public.candidate_notifications from public,anon,authenticated;

grant select,insert,update,delete on table public.candidate_app_accounts to service_role;
grant select,insert,update,delete on table public.candidate_app_sessions to service_role;
grant select,insert,update,delete on table public.candidate_auth_challenges to service_role;
grant select,insert,update,delete on table public.candidate_submission_workflows to service_role;
grant select,insert,update,delete on table public.candidate_submission_components to service_role;
grant select,insert,update,delete on table public.candidate_approval_requests to service_role;
grant select,insert,update,delete on table public.candidate_notifications to service_role;

comment on table public.candidate_app_accounts is
  'Private Candidate App account identity. Candidate ownership is revalidated from normalized candidates.email; no Google password verifier is stored.';
comment on table public.candidate_app_sessions is
  'Private rotating Candidate App refresh-session registry. Token values are stored only as SHA-256 hashes.';
comment on table public.candidate_auth_challenges is
  'Single-use activation/reset/recovery challenge state. Raw challenge tokens are never stored here.';
comment on table public.candidate_submission_workflows is
  'Versioned Candidate App submission/approval state machine. CloudTMS timesheets and TSFIN remain financial authority.';
comment on table public.candidate_submission_components is
  'Immutable Candidate App upload/document components with global exact-byte SHA-256 duplicate prevention.';
comment on table public.candidate_approval_requests is
  'One-live-request-per-workflow-generation manager approval transaction state.';
comment on table public.candidate_notifications is
  'Candidate in-app/push notification source of truth; provider delivery is outside database transactions.';

comment on column public.settings_defaults.candidate_electronic_auto_authorise_default is
  'Global candidate electronic auto-authorisation fallback. Initial and migration default is false.';
comment on column public.settings_defaults.candidate_app_system_actor_user_id is
  'Required CloudTMS service user for canonical process/authorise/delete/archive audit fields. Candidate auto-authorisation remains unavailable while null.';
comment on column public.client_settings.candidate_electronic_auto_authorise is
  'Nullable candidate electronic auto-authorisation client value; null inherits global.';
comment on column public.contracts.candidate_electronic_auto_authorise_override is
  'Nullable candidate electronic auto-authorisation contract override; non-null wins over client/global.';
comment on column public.settings_defaults.candidate_barred_manager_email_domains is
  'Normalised exact domains barred from free manager-email entry. Initially empty and manager workflow remains feature-disabled until configured.';
