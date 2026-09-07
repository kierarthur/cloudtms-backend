-- Minimal disposable PostgreSQL catalogue used only to compile Candidate App SQL.
-- It is not a production schema and contains no CloudTMS data.

set client_min_messages = warning;
create schema if not exists extensions;
create extension if not exists pgcrypto with schema extensions;
create schema if not exists private;

do $roles$
begin
  if not exists (select 1 from pg_roles where rolname='anon') then create role anon nologin; end if;
  if not exists (select 1 from pg_roles where rolname='authenticated') then create role authenticated nologin; end if;
  if not exists (select 1 from pg_roles where rolname='service_role') then create role service_role nologin bypassrls; end if;
end;
$roles$;

create type public.submission_mode_enum as enum ('ELECTRONIC','MANUAL');
create type public.weekly_timesheet_source_enum as enum ('NONE','NHSP','HEALTHROSTER');
create type public.contract_week_status_enum as enum ('PLANNED','OPEN','SUBMITTED','AUTHORISED','INVOICED','CANCELLED');
create type public.timesheet_status_enum as enum ('DRAFT','OPEN','RECEIVED','SUBMITTED','AUTHORISED','REVOKED','ARCHIVED');
create type public.timesheet_line_type_enum as enum ('HOURS','EXPENSES','MILEAGE');
create type public.timesheet_scope_enum as enum ('DAILY','WEEKLY');
create type public.timesheet_qr_status_enum as enum ('PENDING','USED','CANCELLED','EXPIRED');
create type public.mail_status_enum as enum ('QUEUED','SENT','FAILED');
create type public.invoice_consolidation_mode_enum as enum ('NONE','BY_WEEK','ANY_WEEK');
create type public.correction_financials_date_basis_enum as enum ('PAID_DATE','NOW');
create type public.hr_source_enum as enum ('HEALTHROSTER','NHSP');
create type public.ts_fin_reason_enum as enum ('FIXTURE','CONTEXT_CHANGED','REVOKED');
create type public.timesheet_fin_basis_enum as enum (
  'SELF_REPORTED','HR_VALIDATED','OVERRIDDEN','NHSP','NHSP_ADJUSTMENT',
  'CONTRACT_WEEKLY','HEALTHROSTER_ADJUSTMENT','HEALTHROSTER_SELF_BILL'
);
create type public.validation_status_enum as enum (
  'PENDING','VALIDATION_OK','VALIDATION_ERROR','OVERRIDE_AWAITING_CONFIRM','OVERRIDDEN'
);
create type public.ts_fin_processing_status_enum as enum (
  'UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING','READY_FOR_HR',
  'READY_FOR_INVOICE','PENDING_AUTH','AWAITING_MANUAL_SIGNATURE','UNPROCESSED'
);

create table public.tms_users (
  id uuid primary key default gen_random_uuid(),
  email text not null default 'candidate-runtime@example.invalid',
  is_active boolean not null default true
);

create table public.settings_defaults (
  id integer primary key,
  temp_log boolean default false,
  timezone_id text,
  day_start time,
  day_end time,
  night_start time,
  night_end time,
  sat_start time,
  sat_end time,
  sun_start time,
  sun_end time,
  bh_start time,
  bh_end time,
  bh_source text,
  bh_list jsonb,
  bh_feed_url text,
  ts_reference_required boolean not null default false,
  hr_attach_to_invoice boolean not null default true,
  ts_attach_to_invoice boolean not null default true,
  healthroster_import_auto_authorise_default boolean not null default false,
  nhsp_import_auto_authorise_default boolean not null default false,
  reversal_complete_financials_date public.correction_financials_date_basis_enum not null default 'PAID_DATE',
  reversal_replacement_financials_date public.correction_financials_date_basis_enum not null default 'PAID_DATE',
  updated_at timestamptz not null default now()
);

create table public.clients (
  id uuid primary key default gen_random_uuid(),
  cli_ref text,
  rev bigint not null default 1,
  name text,
  invoice_address text,
  primary_invoice_email text,
  ap_phone text,
  vat_chargeable boolean not null default true,
  payment_terms_days integer not null default 30,
  mileage_charge_rate numeric,
  ts_queries_email text,
  client_address text,
  contact_title text,
  contact_known_as text,
  contact_forename text,
  contact_surname text,
  contact_job_title text,
  contact_tel text,
  contact_mobile text,
  contact_email text,
  website text,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table public.candidates (
  id uuid primary key default gen_random_uuid(),
  tms_ref text,
  email text,
  display_name text,
  first_name text,
  last_name text,
  opt_in_email boolean not null default true,
  active boolean not null default true,
  key_norm text,
  pay_method text
);

create table public.client_settings (
  id uuid primary key default gen_random_uuid(),
  client_id uuid not null references public.clients(id),
  effective_from date,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  timezone_id text,
  day_start time,
  day_end time,
  night_start time,
  night_end time,
  sat_start time,
  sat_end time,
  sun_start time,
  sun_end time,
  bh_start time,
  bh_end time,
  bh_source text,
  bh_list jsonb,
  bh_feed_url text,
  vat_rate_pct numeric,
  holiday_pay_pct numeric,
  erni_pct numeric,
  apply_holiday_to text,
  apply_erni_to text,
  margin_includes text,
  hr_validation_required boolean not null default false,
  ts_reference_required boolean not null default false,
  week_ending_weekday integer,
  autoprocess_hr boolean not null default false,
  pay_reference_required boolean not null default false,
  invoice_reference_required boolean not null default false,
  default_submission_mode public.submission_mode_enum,
  is_nhsp boolean not null default false,
  self_bill_no_invoices_sent boolean not null default false,
  daily_calc_of_invoices boolean not null default false,
  no_timesheet_required boolean not null default false,
  group_nightsat_sunbh boolean not null default false,
  requires_hr boolean not null default false,
  hr_attach_to_invoice boolean not null default true,
  ts_attach_to_invoice boolean not null default true,
  auto_invoice_default boolean not null default false,
  send_manual_invoices_to_different_email boolean not null default false,
  manual_invoices_alt_email_address text,
  invoice_consolidation_mode public.invoice_consolidation_mode_enum not null default 'NONE',
  reference_number_required_to_issue_invoice boolean not null default false,
  opt_in_email boolean not null default true,
  opt_in_sms boolean not null default true,
  opt_in_whatsapp boolean not null default true,
  healthroster_import_auto_authorise boolean not null default false,
  nhsp_import_auto_authorise boolean not null default false,
  reversal_complete_financials_date public.correction_financials_date_basis_enum,
  reversal_replacement_financials_date public.correction_financials_date_basis_enum
);

create table public.contracts (
  id uuid primary key default gen_random_uuid(),
  candidate_id uuid not null references public.candidates(id),
  client_id uuid not null references public.clients(id),
  start_date date not null,
  end_date date not null,
  week_ending_weekday_snapshot integer not null default 0,
  std_schedule_json jsonb not null default '[]'::jsonb,
  role text,
  display_site text,
  band text,
  pay_method_snapshot text not null default 'PAYE',
  default_submission_mode public.submission_mode_enum,
  overrideclientsettings boolean not null default false,
  no_timesheet_required boolean,
  is_nhsp boolean,
  autoprocess_hr boolean,
  weekly_timesheet_source public.weekly_timesheet_source_enum not null default 'NONE',
  self_bill boolean not null default false,
  send_manual_invoices_to_different_email boolean,
  manual_invoices_alt_email_address text
);

create table public.timesheets (
  timesheet_id uuid primary key default gen_random_uuid(),
  version integer not null default 1,
  status public.timesheet_status_enum not null default 'OPEN',
  submission_mode public.submission_mode_enum not null default 'ELECTRONIC',
  line_type public.timesheet_line_type_enum not null default 'HOURS',
  sheet_scope public.timesheet_scope_enum not null default 'WEEKLY',
  is_current boolean not null default true,
  contract_id uuid references public.contracts(id),
  booking_id text,
  week_ending_date date,
  occupant_key_norm text,
  hospital_norm text,
  ward_norm text,
  job_title_norm text,
  shift_label_norm text,
  band text,
  revoked_at timestamptz,
  revoked_reason text,
  revoked_by text,
  is_adjustment boolean not null default false,
  adjustment_origin text,
  parent_timesheet_id uuid,
  correction_id text,
  correction_kind text,
  candidate_hint_text jsonb not null default '{}'::jsonb,
  archived_at_utc timestamptz,
  archived_by_user_id uuid,
  archived_reason_code text,
  authorised_at_server timestamptz,
  auth_name text,
  auth_job_title text,
  r2_nurse_key text,
  r2_auth_key text,
  img_sha256_nurse text,
  img_sha256_auth text,
  scheduled_start_iso timestamptz,
  scheduled_end_iso timestamptz,
  worked_start_iso timestamptz,
  worked_end_iso timestamptz,
  break_start_iso timestamptz,
  break_end_iso timestamptz,
  break_minutes integer,
  worked_minutes integer,
  actual_schedule_json jsonb,
  additional_units_week jsonb not null default '{}'::jsonb,
  additional_units_per_day jsonb not null default '{}'::jsonb,
  manual_pdf_r2_key text,
  manual_pdf_rotation_degrees integer not null default 0,
  reference_number text,
  reference_set_at timestamptz,
  idempotency_key text,
  client_hash text,
  client_ua text,
  day_references_json jsonb,
  qr_token text,
  qr_status public.timesheet_qr_status_enum,
  qr_payload_json jsonb not null default '{}'::jsonb,
  qr_generated_at timestamptz,
  qr_scanned_at timestamptz,
  qr_scan_info_json jsonb,
  qr_r2_key text,
  qr_last_sent_hash text,
  qr_last_sent_at_utc timestamptz,
  qr_signed_hash text,
  qr_signed_at_utc timestamptz,
  generated_pdf_at_utc timestamptz,
  generated_pdf_refs_sig text,
  generated_pdf_refs_snapshot_json jsonb,
  generated_pdf_refs_captured_at_utc timestamptz,
  qr_sent_refs_sig text,
  qr_sent_refs_snapshot_json jsonb,
  qr_sent_refs_captured_at_utc timestamptz,
  manual_document_asset_id uuid,
  document_revision bigint not null default 1,
  document_state text not null default 'NOT_REQUESTED',
  current_document_version_id uuid,
  active_document_operation_id uuid,
  last_document_error_json jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_ts_signatures_for_electronic check (
    submission_mode<>'ELECTRONIC'
    or (r2_nurse_key is not null and r2_auth_key is not null)
  )
);

-- Existing production relation read by the authoritative Timesheet
-- authorisation routine. This disposable catalogue needs the real column
-- shape so first-use Candidate verification exercises the same read path.
create table public.timesheet_payment_overrides (
  id uuid primary key default gen_random_uuid(),
  timesheet_id uuid not null references public.timesheets(timesheet_id),
  candidate_id uuid not null references public.candidates(id),
  override_type text not null default 'ADVANCE_THIS_PAYMENT',
  reason text not null,
  created_at_utc timestamptz not null default now(),
  created_by_user_id uuid references public.tms_users(id),
  consumed_by_pay_batch_id uuid,
  consumed_at_utc timestamptz,
  cleared_at_utc timestamptz,
  cleared_by_user_id uuid references public.tms_users(id),
  clear_reason text
);

-- Existing production helper required by the duplicate-expense first-use verifier.
-- Keep this compile-only fixture definition aligned with the authoritative baseline.
create or replace function public._pay_timesheet_rotation_scope(p_timesheet_ids uuid[])
returns table(
  requested_timesheet_id uuid,
  booking_id text,
  canonical_timesheet_id uuid,
  family_timesheet_id uuid,
  family_is_current boolean,
  family_version integer,
  requested_is_canonical boolean
)
language sql
stable
security definer
set search_path to 'public'
as $function$
with input_timesheets as (
  select distinct
    input_timesheet_values.timesheet_id_value as requested_timesheet_id
  from unnest(coalesce(p_timesheet_ids, array[]::uuid[])) as input_timesheet_values(timesheet_id_value)
  where input_timesheet_values.timesheet_id_value is not null
),
requested_timesheets as (
  select
    input_timesheets.requested_timesheet_id as requested_timesheet_id,
    public.timesheets.timesheet_id as matched_timesheet_id,
    public.timesheets.booking_id as matched_booking_id
  from input_timesheets
  left join public.timesheets
    on public.timesheets.timesheet_id = input_timesheets.requested_timesheet_id
),
requested_bookings as (
  select distinct
    requested_timesheets.matched_booking_id as booking_id
  from requested_timesheets
  where requested_timesheets.matched_timesheet_id is not null
    and requested_timesheets.matched_booking_id is not null
    and btrim(requested_timesheets.matched_booking_id) <> ''
),
canonical_timesheets as (
  select distinct on (current_timesheets.booking_id)
    current_timesheets.booking_id as booking_id,
    current_timesheets.timesheet_id as canonical_timesheet_id
  from public.timesheets as current_timesheets
  join requested_bookings
    on requested_bookings.booking_id = current_timesheets.booking_id
  where current_timesheets.is_current = true
  order by
    current_timesheets.booking_id,
    current_timesheets.version desc,
    current_timesheets.updated_at desc,
    current_timesheets.created_at desc,
    current_timesheets.timesheet_id
),
family_timesheets as (
  select distinct
    family_rows.booking_id as booking_id,
    family_rows.timesheet_id as family_timesheet_id,
    family_rows.is_current as family_is_current,
    family_rows.version as family_version
  from public.timesheets as family_rows
  join requested_bookings
    on requested_bookings.booking_id = family_rows.booking_id
),
resolved_scope_rows as (
  select
    requested_timesheets.requested_timesheet_id as requested_timesheet_id,
    requested_timesheets.matched_booking_id as booking_id,
    canonical_timesheets.canonical_timesheet_id as canonical_timesheet_id,
    family_timesheets.family_timesheet_id as family_timesheet_id,
    family_timesheets.family_is_current as family_is_current,
    family_timesheets.family_version as family_version,
    coalesce(requested_timesheets.requested_timesheet_id = canonical_timesheets.canonical_timesheet_id, false) as requested_is_canonical
  from requested_timesheets
  join family_timesheets
    on family_timesheets.booking_id = requested_timesheets.matched_booking_id
  left join canonical_timesheets
    on canonical_timesheets.booking_id = requested_timesheets.matched_booking_id
  where requested_timesheets.matched_timesheet_id is not null
),
defensive_unresolved_rows as (
  select
    requested_timesheets.requested_timesheet_id as requested_timesheet_id,
    null::text as booking_id,
    requested_timesheets.requested_timesheet_id as canonical_timesheet_id,
    requested_timesheets.requested_timesheet_id as family_timesheet_id,
    null::boolean as family_is_current,
    null::integer as family_version,
    false as requested_is_canonical
  from requested_timesheets
  where requested_timesheets.matched_timesheet_id is null
),
rotation_scope_output as (
  select
    resolved_scope_rows.requested_timesheet_id as requested_timesheet_id,
    resolved_scope_rows.booking_id as booking_id,
    resolved_scope_rows.canonical_timesheet_id as canonical_timesheet_id,
    resolved_scope_rows.family_timesheet_id as family_timesheet_id,
    resolved_scope_rows.family_is_current as family_is_current,
    resolved_scope_rows.family_version as family_version,
    resolved_scope_rows.requested_is_canonical as requested_is_canonical
  from resolved_scope_rows

  union all

  select
    defensive_unresolved_rows.requested_timesheet_id as requested_timesheet_id,
    defensive_unresolved_rows.booking_id as booking_id,
    defensive_unresolved_rows.canonical_timesheet_id as canonical_timesheet_id,
    defensive_unresolved_rows.family_timesheet_id as family_timesheet_id,
    defensive_unresolved_rows.family_is_current as family_is_current,
    defensive_unresolved_rows.family_version as family_version,
    defensive_unresolved_rows.requested_is_canonical as requested_is_canonical
  from defensive_unresolved_rows
)
select
  rotation_scope_output.requested_timesheet_id,
  rotation_scope_output.booking_id,
  rotation_scope_output.canonical_timesheet_id,
  rotation_scope_output.family_timesheet_id,
  rotation_scope_output.family_is_current,
  rotation_scope_output.family_version,
  rotation_scope_output.requested_is_canonical
from rotation_scope_output
order by
  rotation_scope_output.requested_timesheet_id,
  rotation_scope_output.booking_id nulls last,
  rotation_scope_output.family_is_current desc nulls last,
  rotation_scope_output.family_version desc nulls last,
  rotation_scope_output.family_timesheet_id;
$function$;

create table public.contract_weeks (
  id uuid primary key default gen_random_uuid(),
  contract_id uuid not null references public.contracts(id),
  week_ending_date date not null,
  additional_seq integer not null default 0,
  status public.contract_week_status_enum not null default 'PLANNED',
  submission_mode_snapshot public.submission_mode_enum,
  timesheet_id uuid references public.timesheets(timesheet_id),
  uploaded_pdf_r2_key text,
  day_entries_json jsonb not null default '[]'::jsonb,
  totals_json jsonb not null default '{}'::jsonb,
  planned_schedule_json jsonb not null default '[]'::jsonb,
  is_adjustment boolean not null default false,
  enforce_day_partition boolean not null default false,
  allowed_days_mask text,
  split_boundary_date date,
  worker_note text,
  split_group_key text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(contract_id,week_ending_date,additional_seq)
);

create table public.timesheets_financials (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  timesheet_id uuid not null references public.timesheets(timesheet_id),
  candidate_id uuid references public.candidates(id),
  client_id uuid references public.clients(id),
  is_current boolean not null default true,
  timesheet_version integer not null default 1,
  is_stale boolean not null default false,
  basis public.timesheet_fin_basis_enum,
  invoice_breakdown_json jsonb not null default '{}'::jsonb,
  policy_snapshot_json jsonb not null default '{}'::jsonb,
  rate_source_refs_json jsonb not null default '{}'::jsonb,
  processing_status public.ts_fin_processing_status_enum not null default 'UNASSIGNED',
  computed_at_utc timestamptz,
  processed_at_utc timestamptz,
  updated_at timestamptz not null default now(),
  worked_start_iso timestamptz,
  worked_end_iso timestamptz,
  break_start_iso timestamptz,
  break_end_iso timestamptz,
  break_minutes integer,
  role text,
  band text,
  pay_method text,
  has_rate_issue boolean not null default false,
  has_pay_channel_issue boolean not null default false,
  hours_day numeric not null default 0,
  hours_night numeric not null default 0,
  hours_sat numeric not null default 0,
  hours_sun numeric not null default 0,
  hours_bh numeric not null default 0,
  total_hours numeric not null default 0,
  additional_units_json jsonb not null default '{}'::jsonb,
  additional_pay_ex_vat numeric not null default 0,
  additional_charge_ex_vat numeric not null default 0,
  additional_margin_ex_vat numeric not null default 0,
  expenses_pay_ex_vat numeric not null default 0,
  expenses_charge_ex_vat numeric not null default 0,
  expenses_description text,
  expenses_evidence_r2_key text,
  expenses_evidence_manifest jsonb,
  mileage_units numeric not null default 0,
  mileage_pay_ex_vat numeric not null default 0,
  mileage_charge_ex_vat numeric not null default 0,
  mileage_evidence_r2_key text,
  mileage_evidence_manifest jsonb,
  travel_pay_ex_vat numeric not null default 0,
  travel_charge_ex_vat numeric not null default 0,
  accommodation_pay_ex_vat numeric not null default 0,
  accommodation_charge_ex_vat numeric not null default 0,
  other_pay_ex_vat numeric not null default 0,
  other_charge_ex_vat numeric not null default 0,
  actual_schedule_json jsonb,
  actual_minutes_by_day_json jsonb,
  total_pay_ex_vat numeric not null default 0,
  total_charge_ex_vat numeric not null default 0,
  margin_ex_vat numeric not null default 0,
  authorised_at_utc timestamptz,
  authorised_by_user_id uuid,
  paid_at_utc timestamptz,
  paid_by_user_id uuid,
  payment_reference text,
  locked_by_invoice_id uuid,
  locked_at_utc timestamptz,
  nhsp_import_id uuid,
  external_source_rows_json jsonb
);

-- Candidate current/history projection reads the authoritative settlement
-- markers, but the compact compile fixture deliberately omits the wider
-- Banking schema.  Keep the two read-owned tables structurally faithful so
-- Candidate runtime tests cover paid/history precedence without installing
-- the payment mutation system.
create table public.timesheet_pay_state (
  timesheet_id uuid primary key references public.timesheets(timesheet_id),
  last_settled_snapshot_json jsonb not null,
  last_settled_signature text not null,
  last_settled_pay_batch_id uuid,
  last_settled_at_utc timestamptz,
  summary_pay_status_code text,
  summary_pay_icon_code text,
  summary_pay_paid_at_utc timestamptz,
  summary_net_delta_ex_vat numeric(12,2)
);

create table public.timesheet_summary_pay_state_cache (
  timesheet_id uuid primary key references public.timesheets(timesheet_id),
  paid_to_date_ex_vat numeric not null default 0,
  last_paid_at_utc timestamptz,
  reserved_ex_vat numeric not null default 0,
  outstanding_ex_vat numeric not null default 0,
  net_delta_ex_vat numeric not null default 0,
  active_advance boolean not null default false,
  active_processing boolean not null default false,
  summary_state_applies boolean not null default false,
  advance_override_created_at_utc timestamptz,
  advance_authorisation_consumed_at_utc timestamptz,
  summary_pay_status_code text not null default 'UNPAID',
  summary_pay_icon_code text not null default 'NONE',
  summary_badge_codes text[] not null default array[]::text[],
  refreshed_at_utc timestamptz not null default now(),
  refreshed_by_user_id uuid
);

create table public.nhsp_shifts (
  id uuid primary key default gen_random_uuid(),
  timesheet_id uuid references public.timesheets(timesheet_id)
);

create table public.invoices (
  id uuid primary key default gen_random_uuid(),
  client_id uuid references public.clients(id),
  type text not null default 'INVOICE',
  status text not null default 'DRAFT',
  original_invoice_id uuid,
  subtotal_ex_vat numeric not null default 0,
  vat_amount numeric not null default 0,
  total_inc_vat numeric not null default 0,
  header_snapshot_json jsonb not null default '{}'::jsonb,
  do_not_send boolean not null default false,
  document_revision bigint not null default 1,
  issued_document_version_id uuid,
  on_hold_reason text,
  updated_at timestamptz not null default now()
);

create table public.invoice_lines (
  id uuid primary key default gen_random_uuid(),
  invoice_id uuid not null references public.invoices(id),
  timesheet_id uuid references public.timesheets(timesheet_id),
  source_key text,
  vat_rate_pct numeric not null default 0,
  total_charge_ex_vat numeric not null default 0,
  vat_amount numeric not null default 0,
  total_inc_vat numeric not null default 0,
  meta_json jsonb not null default '{}'::jsonb
);

create view public.v_ts_invoice_precheck as
select t.timesheet_id,
  false::boolean require_reference_to_invoice,
  false::boolean issue_missing_reference,
  false::boolean reference_number_required_to_issue_invoice,
  true::boolean effective_ts_attach_to_invoice
from public.timesheets t;

create view public.v_timesheets_summary_base as
select t.timesheet_id,
  false::boolean client_requires_hr,
  false::boolean hr_validation_required_for_invoice,
  null::validation_status_enum validation_status,
  false::boolean client_no_timesheet_required,
  false::boolean client_is_nhsp,
  ''::text route_type
from public.timesheets t;

create view public.v_timesheets_summary as
select t.timesheet_id,
  false::boolean client_hr_validation_required,
  false::boolean client_no_timesheet_required,
  ''::text route_type,
  null::uuid contract_id,
  null::uuid contract_week_id,
  null::text validation_status
from public.timesheets t;

create or replace function private._invoice_source_reference_validate_batch(p_sources jsonb)
returns table(
  source_member_key text,
  blocker_code text,
  reference_hash text,
  current_revision text,
  detail_json jsonb
)
language sql stable
as $function$
  select e.value->>'source_member_key',null::text,null::text,
    e.value->>'row_revision','{}'::jsonb
  from jsonb_array_elements(coalesce(p_sources,'[]'::jsonb)) e(value)
$function$;

create table public.timesheet_evidence (
  id uuid primary key default gen_random_uuid(),
  timesheet_id uuid not null references public.timesheets(timesheet_id),
  kind text not null,
  display_name text,
  storage_key text not null,
  created_at timestamptz not null default now(),
  created_by uuid,
  document_asset_id uuid,
  processing_state text not null default 'DISCOVERED'
);

create sequence public.invoice_operation_change_seq as bigint start with 1;

create or replace function private._invoice_processor_limits()
returns jsonb language sql immutable set search_path='' as $function$
  select '{}'::jsonb
$function$;

create table public.invoice_operations (
  id uuid primary key default gen_random_uuid(),
  parent_operation_id uuid,
  operation_type text not null default 'BUILD_DOCUMENT',
  entity_type text,
  entity_id uuid,
  actor_user_id uuid,
  idempotency_key text not null default gen_random_uuid()::text,
  status text not null default 'QUEUED',
  phase text not null default 'SUBMITTED',
  priority integer not null default 200,
  source_revision text,
  template_version text,
  input_json jsonb not null default '{}'::jsonb,
  config_json jsonb not null default '{}'::jsonb,
  progress_json jsonb not null default '{}'::jsonb,
  result_json jsonb,
  error_json jsonb,
  total_units integer not null default 0,
  completed_units integer not null default 0,
  failed_units integer not null default 0,
  chunk_count integer not null default 0,
  control_version bigint not null default 1,
  change_seq bigint not null default nextval('public.invoice_operation_change_seq'),
  requires_user_action boolean not null default false,
  created_at_utc timestamptz not null default now(),
  started_at_utc timestamptz,
  updated_at_utc timestamptz not null default now(),
  completed_at_utc timestamptz,
  failed_at_utc timestamptz,
  manifest_generation integer not null default 0,
  manifest_committed boolean not null default true,
  release_complete boolean not null default true,
  result_page_revision bigint not null default 0
);

create table public.invoice_document_assets (
  id uuid primary key default gen_random_uuid(),
  status text not null default 'DISCOVERED',
  operation_id uuid references public.invoice_operations(id)
);

create table public.invoice_document_versions (
  id uuid primary key default gen_random_uuid(),
  entity_type text not null,
  entity_id uuid not null,
  purpose text not null,
  source_revision text not null,
  template_version text not null,
  status text not null,
  snapshot_json jsonb not null default '{}'::jsonb,
  snapshot_hash text not null default '',
  manifest_json jsonb not null default '[]'::jsonb,
  manifest_hash text not null default '',
  r2_key text,
  sha256 text,
  size_bytes bigint,
  expected_page_count integer,
  page_count integer,
  core_page_count integer,
  supporting_page_count integer,
  superseded_at_utc timestamptz,
  operation_id uuid references public.invoice_operations(id),
  created_at_utc timestamptz not null default now(),
  ready_at_utc timestamptz,
  verified_at_utc timestamptz,
  error_json jsonb
);

create table public.invoice_hr_source_rows (
  id uuid primary key default gen_random_uuid(),
  invoice_id uuid not null references public.invoices(id),
  source_system text,
  import_id uuid,
  rows_json jsonb not null default '[]'::jsonb
);

create table public.invoice_operation_chunks (
  id uuid primary key default gen_random_uuid(),
  operation_id uuid not null references public.invoice_operations(id),
  chunk_type text not null,
  entity_type text not null,
  entity_id uuid,
  status text not null default 'QUEUED'
);

-- Candidate QR runtime tests exercise the real QR enqueue authority while
-- keeping this compile fixture independent of the full invoice renderer.
create or replace function private._invoice_presentation_snapshot_batch(
  p_requests jsonb,
  p_now_utc timestamptz
) returns table(
  request_key text,
  presentation_model jsonb,
  snapshot_json jsonb,
  snapshot_hash text,
  valid boolean,
  error_code text
)
language sql stable as $$
  select request_row.value->>'request_key',
    jsonb_build_object(
      'schema_version','TIMESHEET_RENDER_MODEL_V2',
      'form_variant','QR_UNSIGNED',
      'week_period',jsonb_build_object('days','[]'::jsonb),
      'additional_units_section',jsonb_build_object('rows','[]'::jsonb),
      'branding','{}'::jsonb,
      'wording','{}'::jsonb
    ),
    jsonb_build_object('fixture',true),
    repeat('f',64),true,null::text
  from jsonb_array_elements(coalesce(p_requests,'[]'::jsonb)) request_row(value)
$$;

create or replace function public.bulk_timesheet_row_decision_v1(p_filter jsonb)
returns table(row_json jsonb)
language sql stable as $$
  select jsonb_build_object(
    'row_key','timesheet:'||coalesce(p_filter->>'timesheet_id','fixture'),
    'row_signature','fixture-row-signature',
    'row_patch','{}'::jsonb
  )
$$;

create or replace function private._invoice_reference_rows_batch(p_invoice_ids uuid[])
returns table(invoice_id uuid,timesheet_id uuid,current_reference text)
language sql stable as $$
  select i.id,null::uuid,null::text from unnest(coalesce(p_invoice_ids,array[]::uuid[])) x(id)
  join public.invoices i on i.id=x.id
$$;

create or replace function private._invoice_correction_validate_batch(
  p_scopes jsonb,
  p_evaluation_date date
) returns table(request_key text,invoice_id uuid,valid boolean,blocker_code text)
language sql stable as $$
  select e.value->>'request_key',
    case when coalesce(e.value->>'invoice_id','')~
      '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
      then (e.value->>'invoice_id')::uuid end,
    true,null::text
  from jsonb_array_elements(coalesce(p_scopes,'[]'::jsonb)) e(value)
$$;

create table public.manual_timesheet_queue (
  id uuid primary key default gen_random_uuid(),
  r2_key text,
  original_filename text,
  status text,
  timesheet_id uuid,
  uploaded_at_utc timestamptz,
  uploaded_by_user_id uuid,
  last_rotation_deg integer not null default 0,
  meta_json jsonb not null default '{}'::jsonb
);

create table public.mail_outbox (
  id uuid primary key default gen_random_uuid(),
  type text not null,
  "to" text not null,
  cc text,
  bcc text,
  reply_to text,
  importance text,
  subject text not null,
  body_html text,
  body_text text,
  attachments jsonb not null default '[]'::jsonb,
  status public.mail_status_enum not null,
  last_error text,
  failed_at timestamptz,
  created_at_utc timestamptz not null default now(),
  created_by uuid,
  reference text,
  recipient_kind text,
  recipient_id uuid,
  context_kind text,
  context_id uuid,
  mailshot_run_id uuid,
  document_template_id uuid,
  email_type text,
  scheduled_for_utc timestamptz,
  next_attempt_at_utc timestamptz,
  deterministic_outbox_key text unique,
  payment_scope_json jsonb not null default '{}'::jsonb,
  sent_at timestamptz,
  delivered_at timestamptz,
  read_at timestamptz,
  provider_status text,
  provider_message_id text,
  attempt_lease_token text,
  attempt_leased_at_utc timestamptz,
  attempt_lease_expires_at_utc timestamptz,
  attachments_ready boolean not null default true,
  waiting_invoice_operation_id uuid
);

create table public.audit_events (
  id uuid primary key default gen_random_uuid(),
  ts_utc timestamptz not null default now(),
  actor_user_id uuid,
  object_type text not null,
  object_id_text text not null,
  action text not null,
  before_json jsonb,
  after_json jsonb,
  reason text,
  correlation_id text
);

create table public.ts_financials_outbox (
  timesheet_id uuid not null references public.timesheets(timesheet_id),
  reason text not null,
  attempt_count integer not null default 0,
  next_attempt_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  constraint uq_tsfin_outbox unique (timesheet_id,reason)
);

create function public._audit_insert(
  p_object_type text,
  p_object_id_text text,
  p_action text,
  p_before_json jsonb,
  p_after_json jsonb,
  p_reason text,
  p_actor_user_id uuid
) returns void language sql as $$
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,reason
  ) values(
    p_actor_user_id,p_object_type,p_object_id_text,p_action,p_before_json,p_after_json,p_reason
  )
$$;

-- Signatures only: the Candidate App SQL composes these existing authorities.
create function public.timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean)
returns jsonb language sql as $$ select '{"row_signature":"fixture"}'::jsonb $$;

create function public.timesheet_qr_restore_version(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_restore_kind text,
  p_actor_user_id uuid
)
returns table(
  timesheet_id uuid,
  restored_version integer,
  sheet_scope text,
  submission_mode text,
  qr_status text,
  qr_token text,
  restored_has_signed_pdf boolean
)
language sql
as $$
  select p_timesheet_id,1,'WEEKLY'::text,'MANUAL'::text,
    upper(p_restore_kind),null::text,upper(p_restore_kind)='SIGNED'
$$;

revoke all on function public.timesheet_qr_restore_version(uuid,uuid,text,uuid)
  from public,anon;
grant execute on function public.timesheet_qr_restore_version(uuid,uuid,text,uuid)
  to authenticated,service_role;

create function public.timesheet_route_version_rotate(
  p_current_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_target_action text,
  p_actor_user_id uuid,
  p_allow_manual_only boolean default false
) returns jsonb language sql security definer set search_path to 'public' as $$
  select jsonb_build_object('ok',true,'legacy_route_rotate',true,
    'current_timesheet_id',p_current_timesheet_id,'new_timesheet_id',p_current_timesheet_id,
    'new_version',1,'action',upper(p_target_action))
$$;
revoke all on function public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean)
  from public,anon;
grant execute on function public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean)
  to authenticated,service_role;

create function public.timesheet_removal_financial_history_v1(
  p_timesheet_ids uuid[],p_booking_ids text[] default null,
  p_contract_week_ids uuid[] default null
) returns jsonb language sql stable security definer set search_path to 'public','pg_temp' as $$
  select jsonb_build_object('ok',true,'blocked',false,'blockers','[]'::jsonb,
    'archive_required',false,'retention_reasons','[]'::jsonb)
$$;
revoke all on function public.timesheet_removal_financial_history_v1(uuid[],text[],uuid[])
  from public,anon,authenticated,service_role;

create function public.contract_week_manual_upsert_atomic(
  p_week_id uuid,
  p_expected_timesheet_id uuid,
  p_timesheet_create_json jsonb,
  p_timesheet_patch_json jsonb,
  p_contract_week_patch_json jsonb,
  p_tsfin_snapshot_json jsonb,
  p_rotation_json jsonb,
  p_actor_user_id uuid,
  p_materialise_staged_evidence boolean,
  p_now_utc timestamptz,
  p_expected_row_signature text,
  p_queue_timesheet_materialisation_json jsonb
) returns jsonb language sql as $$
  with patched as (
    update public.timesheets
    set submission_mode=coalesce((p_timesheet_patch_json->>'submission_mode')::public.submission_mode_enum,submission_mode),
        auth_name=coalesce(p_timesheet_patch_json->>'auth_name',auth_name),
        auth_job_title=coalesce(p_timesheet_patch_json->>'auth_job_title',auth_job_title),
        r2_nurse_key=coalesce(p_timesheet_patch_json->>'r2_nurse_key',r2_nurse_key),
        r2_auth_key=coalesce(p_timesheet_patch_json->>'r2_auth_key',r2_auth_key),
        img_sha256_nurse=coalesce(p_timesheet_patch_json->>'img_sha256_nurse',img_sha256_nurse),
        img_sha256_auth=coalesce(p_timesheet_patch_json->>'img_sha256_auth',img_sha256_auth),
        updated_at=p_now_utc
    where timesheet_id=p_expected_timesheet_id
    returning timesheet_id
  )
  select jsonb_build_object('ok',true,'timesheet_id',p_expected_timesheet_id,'row_signature','fixture')
$$;

create function public.tsfin_write_current_snapshot_single_bounded(
  p_timesheet_id uuid,
  p_timesheet_version integer,
  p_snapshot_json jsonb,
  p_actor_user_id uuid,
  p_now_utc timestamptz
) returns jsonb language plpgsql as $$
declare
  v_snapshot public.timesheets_financials%rowtype;
begin
  v_snapshot:=jsonb_populate_record(null::public.timesheets_financials,coalesce(p_snapshot_json,'{}'::jsonb));
  update public.timesheets_financials
  set timesheet_version=p_timesheet_version,
      candidate_id=coalesce(v_snapshot.candidate_id,candidate_id),
      client_id=coalesce(v_snapshot.client_id,client_id),
      processing_status=coalesce(v_snapshot.processing_status,processing_status),
      hours_day=coalesce(v_snapshot.hours_day,hours_day),
      hours_night=coalesce(v_snapshot.hours_night,hours_night),
      hours_sat=coalesce(v_snapshot.hours_sat,hours_sat),
      hours_sun=coalesce(v_snapshot.hours_sun,hours_sun),
      hours_bh=coalesce(v_snapshot.hours_bh,hours_bh),
      total_hours=coalesce(v_snapshot.total_hours,total_hours),
      expenses_pay_ex_vat=coalesce(v_snapshot.expenses_pay_ex_vat,expenses_pay_ex_vat),
      expenses_charge_ex_vat=coalesce(v_snapshot.expenses_charge_ex_vat,expenses_charge_ex_vat),
      mileage_units=coalesce(v_snapshot.mileage_units,mileage_units),
      mileage_pay_ex_vat=coalesce(v_snapshot.mileage_pay_ex_vat,mileage_pay_ex_vat),
      mileage_charge_ex_vat=coalesce(v_snapshot.mileage_charge_ex_vat,mileage_charge_ex_vat),
      travel_pay_ex_vat=coalesce(v_snapshot.travel_pay_ex_vat,travel_pay_ex_vat),
      travel_charge_ex_vat=coalesce(v_snapshot.travel_charge_ex_vat,travel_charge_ex_vat),
      accommodation_pay_ex_vat=coalesce(v_snapshot.accommodation_pay_ex_vat,accommodation_pay_ex_vat),
      accommodation_charge_ex_vat=coalesce(v_snapshot.accommodation_charge_ex_vat,accommodation_charge_ex_vat),
      other_pay_ex_vat=coalesce(v_snapshot.other_pay_ex_vat,other_pay_ex_vat),
      other_charge_ex_vat=coalesce(v_snapshot.other_charge_ex_vat,other_charge_ex_vat),
      total_pay_ex_vat=coalesce(v_snapshot.total_pay_ex_vat,total_pay_ex_vat),
      total_charge_ex_vat=coalesce(v_snapshot.total_charge_ex_vat,total_charge_ex_vat),
      margin_ex_vat=coalesce(v_snapshot.margin_ex_vat,margin_ex_vat),
      updated_at=p_now_utc,
      computed_at_utc=p_now_utc
  where timesheet_id=p_timesheet_id and is_current;
  if not found then
    insert into public.timesheets_financials(
      timesheet_id,timesheet_version,candidate_id,client_id,processing_status,
      hours_day,hours_night,hours_sat,hours_sun,hours_bh,total_hours,
      expenses_pay_ex_vat,expenses_charge_ex_vat,mileage_units,mileage_pay_ex_vat,
      mileage_charge_ex_vat,travel_pay_ex_vat,travel_charge_ex_vat,
      accommodation_pay_ex_vat,accommodation_charge_ex_vat,other_pay_ex_vat,
      other_charge_ex_vat,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,
      computed_at_utc,updated_at
    ) values(
      p_timesheet_id,p_timesheet_version,v_snapshot.candidate_id,v_snapshot.client_id,
      coalesce(v_snapshot.processing_status,'UNASSIGNED'),
      coalesce(v_snapshot.hours_day,0),coalesce(v_snapshot.hours_night,0),
      coalesce(v_snapshot.hours_sat,0),coalesce(v_snapshot.hours_sun,0),
      coalesce(v_snapshot.hours_bh,0),coalesce(v_snapshot.total_hours,0),
      coalesce(v_snapshot.expenses_pay_ex_vat,0),coalesce(v_snapshot.expenses_charge_ex_vat,0),
      coalesce(v_snapshot.mileage_units,0),coalesce(v_snapshot.mileage_pay_ex_vat,0),
      coalesce(v_snapshot.mileage_charge_ex_vat,0),coalesce(v_snapshot.travel_pay_ex_vat,0),
      coalesce(v_snapshot.travel_charge_ex_vat,0),coalesce(v_snapshot.accommodation_pay_ex_vat,0),
      coalesce(v_snapshot.accommodation_charge_ex_vat,0),coalesce(v_snapshot.other_pay_ex_vat,0),
      coalesce(v_snapshot.other_charge_ex_vat,0),coalesce(v_snapshot.total_pay_ex_vat,0),
      coalesce(v_snapshot.total_charge_ex_vat,0),coalesce(v_snapshot.margin_ex_vat,0),
      p_now_utc,p_now_utc
    );
  end if;
  return jsonb_build_object('ok',true,'timesheet_id',p_timesheet_id);
end;
$$;

create function public.pay_timesheet_summary_pay_state_refresh(
  p_timesheet_ids uuid[],
  p_actor_user_id uuid
) returns jsonb language sql as $$
  select jsonb_build_object('ok',true,'refreshed_count',cardinality(p_timesheet_ids))
$$;

create function public.timesheet_daily_manual_process_atomic(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_actor_user_id uuid,
  p_timesheet_patch_json jsonb,
  p_tsfin_patch_json jsonb,
  p_now_utc timestamptz,
  p_expected_row_signature text
) returns jsonb language sql as $$ select '{"ok":true}'::jsonb $$;

create function public.timesheet_authorise_generic_atomic(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_actor_user_id uuid,
  p_now_utc timestamptz,
  p_expected_row_signature text
) returns jsonb language sql as $$ select '{"ok":true}'::jsonb $$;

create function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)
returns table(
  timesheet_id uuid,
  old_version integer,
  new_version integer,
  sheet_scope text,
  submission_mode text,
  qr_status text,
  qr_token text,
  processing_status text
) language sql as $$
  select $1,1,2,'WEEKLY','MANUAL',null::text,null::text,'UNPROCESSED'
$$;

create function public.contract_week_delete_planned(uuid,uuid)
returns table(ok boolean) language sql as $$ select true $$;

create function public.timesheet_standard_delete_preview_v1(uuid,uuid,uuid,text)
returns jsonb language sql as $$ select '{"decision":"PERMANENT_DELETE"}'::jsonb $$;

create function public.timesheet_standard_delete_apply_v1(uuid,uuid,uuid,text)
returns jsonb language sql as $$ select '{"ok":true}'::jsonb $$;

create function public.timesheet_archive_transition_v1(uuid,text,text,uuid,uuid,text,timestamptz)
returns jsonb language sql as $$ select '{"ok":true}'::jsonb $$;

insert into public.settings_defaults(id) values (1);

-- TEST already provides this canonical TSFIN context authority.  Reproduce
-- its bounded signature in the disposable fixture so issue derivation tests
-- exercise the same dependency instead of falling back to a summary view.
create or replace function public.tsfin_load_context_batch(p_timesheet_ids uuid[])
returns table(
  effective_timesheet_id uuid,
  out_timesheet jsonb,
  out_cur_fin jsonb,
  out_candidate jsonb,
  out_umbrella jsonb,
  out_client_id uuid,
  out_effective_flags jsonb,
  out_policy jsonb
)
language sql
stable
as $function$
  select t.timesheet_id,
    to_jsonb(t),
    to_jsonb(f),
    jsonb_build_object('id',f.candidate_id),
    '{}'::jsonb,
    f.client_id,
    jsonb_build_object(
      'hr_validation_required_for_invoice',false,
      'validation_status',''
    ),
    '{}'::jsonb
  from public.timesheets t
  join public.timesheets_financials f
    on f.timesheet_id=t.timesheet_id and f.is_current=true
  where t.timesheet_id=any(p_timesheet_ids) and t.is_current=true;
$function$;
