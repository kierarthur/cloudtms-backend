-- CloudTMS schema-only baseline generated from the TEST catalogue on 22 August 2026.
-- Contains definitions only. No application rows, sequence values, credentials, or secrets.
-- Do not edit this applied baseline; add a new migration or repeatable authority instead.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

create schema if not exists private;
create extension if not exists pgcrypto with schema extensions;
create extension if not exists "uuid-ossp" with schema extensions;
create extension if not exists pg_trgm with schema public;
create extension if not exists btree_gist with schema public;

-- public.candidate_assignment_enum
create type public.candidate_assignment_enum as enum ('UNASSIGNED', 'ASSIGNED');

-- public.contract_week_status_enum
create type public.contract_week_status_enum as enum ('PLANNED', 'OPEN', 'SUBMITTED', 'AUTHORISED', 'INVOICED', 'CANCELLED');

-- public.correction_financials_date_basis_enum
create type public.correction_financials_date_basis_enum as enum ('PAID_DATE', 'NOW');

-- public.hr_result_status_enum
create type public.hr_result_status_enum as enum ('SUCCESS', 'FAIL');

-- public.hr_source_enum
create type public.hr_source_enum as enum ('HEALTHROSTER', 'NHSP', 'HEALTHROSTER_DAILY');

-- public.invoice_consolidation_mode_enum
create type public.invoice_consolidation_mode_enum as enum ('NONE', 'BY_WEEK', 'ANY_WEEK');

-- public.invoice_pdf_reason_enum
create type public.invoice_pdf_reason_enum as enum ('READY_FOR_RENDER', 'FORCE_REGEN');

-- public.invoice_status_enum
create type public.invoice_status_enum as enum ('DRAFT', 'ISSUED', 'PAID', 'ON_HOLD');

-- public.invoice_type_enum
create type public.invoice_type_enum as enum ('INVOICE', 'CREDIT_NOTE');

-- public.mail_status_enum
create type public.mail_status_enum as enum ('QUEUED', 'SENT', 'FAILED');

-- public.outbox_status_enum
create type public.outbox_status_enum as enum ('PENDING', 'DELIVERED', 'ERROR');

-- public.outbox_target_enum
create type public.outbox_target_enum as enum ('availability', 'rota', 'timesheet_mgmt');

-- public.pay_advance_kind_enum
create type public.pay_advance_kind_enum as enum ('LOAN', 'OVERPAYMENT', 'LEGACY_ADVANCE', 'UNDERPAYMENT');

-- public.pay_advance_payout_status_enum
create type public.pay_advance_payout_status_enum as enum ('PENDING', 'PAID', 'CANCELLED');

-- public.pay_advance_reason_enum
create type public.pay_advance_reason_enum as enum ('MISSING_SHIFT', 'OVERPAY_NHSP', 'OVERPAY_HR', 'MANUAL_ADVANCE', 'LOAN', 'OVERPAYMENT', 'UNDERPAYMENT');

-- public.pay_advance_status_enum
create type public.pay_advance_status_enum as enum ('ACTIVE', 'PAUSED', 'PAID_OFF');

-- public.pay_finance_case_type_enum
create type public.pay_finance_case_type_enum as enum ('PAYMENT_ADVANCE', 'OVERPAYMENT', 'MANUAL_DEBT_ADJUSTMENT', 'MANUAL_CREDIT_ADJUSTMENT', 'UNDERPAYMENT');

-- public.pay_finance_component_classification_enum
create type public.pay_finance_component_classification_enum as enum ('TAXABLE_CHANNEL_SENSITIVE', 'REIMBURSEMENT_GROSS_FIXED', 'NET_PAY_FIXED_RECOVERY');

-- public.pay_finance_component_resolution_mode_enum
create type public.pay_finance_component_resolution_mode_enum as enum ('SUGGESTED_EQUIVALENT_BASIS', 'MANUAL_REPLACEMENT_RATE', 'MANUAL_AMOUNT');

-- public.pay_finance_routing_kind_enum
create type public.pay_finance_routing_kind_enum as enum ('NORMAL_PAY_ROUTE', 'UMBRELLA_COMPANY', 'ONE_OFF_SPECIFIED_BANK_ACCOUNT');

-- public.pay_finance_taxability_enum
create type public.pay_finance_taxability_enum as enum ('TAXABLE', 'NON_TAXABLE');

-- public.pay_override_mode_enum
create type public.pay_override_mode_enum as enum ('NONE', 'TIMESHEET_ADVANCE');

-- public.preset_scope_enum
create type public.preset_scope_enum as enum ('GLOBAL', 'CLIENT');

-- public.professional_reg_type_enum
create type public.professional_reg_type_enum as enum ('NMC', 'GMC', 'HCPC', 'SWE');

-- public.reference_source_enum
create type public.reference_source_enum as enum ('IMPORTED', 'MANUAL', 'OVERRIDDEN');

-- public.role_type_enum
create type public.role_type_enum as enum ('RMN', 'HCA');

-- public.submission_mode_enum
create type public.submission_mode_enum as enum ('ELECTRONIC', 'MANUAL');

-- public.timesheet_break_entry_mode_enum
create type public.timesheet_break_entry_mode_enum as enum ('START_END_TIMES', 'DURATION_MINUTES');

-- public.timesheet_fin_basis_enum
create type public.timesheet_fin_basis_enum as enum ('SELF_REPORTED', 'HR_VALIDATED', 'OVERRIDDEN', 'NHSP', 'NHSP_ADJUSTMENT', 'CONTRACT_WEEKLY', 'HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL');

-- public.timesheet_line_type_enum
create type public.timesheet_line_type_enum as enum ('HOURS', 'EXPENSES', 'MILEAGE');

-- public.timesheet_qr_status_enum
create type public.timesheet_qr_status_enum as enum ('PENDING', 'USED', 'CANCELLED', 'EXPIRED');

-- public.timesheet_scope_enum
create type public.timesheet_scope_enum as enum ('DAILY', 'WEEKLY');

-- public.timesheet_status_enum
create type public.timesheet_status_enum as enum ('RECEIVED', 'STORED', 'SHEETS_PENDING', 'SHEETS_PARTIAL', 'SHEETS_SYNCED', 'ERROR', 'REVOKED');

-- public.ts_fin_processing_status_enum
create type public.ts_fin_processing_status_enum as enum ('UNASSIGNED', 'CLIENT_UNRESOLVED', 'RATE_MISSING', 'PAY_CHANNEL_MISSING', 'READY_FOR_HR', 'READY_FOR_INVOICE', 'PENDING_AUTH', 'AWAITING_MANUAL_SIGNATURE', 'UNPROCESSED');

-- public.ts_fin_reason_enum
create type public.ts_fin_reason_enum as enum ('NEW_AUTHORISED', 'VERSION_ROTATED', 'REVOKED', 'RATE_CHANGED', 'POLICY_CHANGED', 'CONTEXT_CHANGED', 'MANUAL');

-- public.ts_pdf_reason_enum
create type public.ts_pdf_reason_enum as enum ('READY_FOR_INVOICE', 'FORCE_REGEN');

-- public.validation_status_enum
create type public.validation_status_enum as enum ('PENDING', 'VALIDATION_OK', 'VALIDATION_ERROR', 'OVERRIDE_AWAITING_CONFIRM', 'OVERRIDDEN');

-- public.weekly_timesheet_source_enum
create type public.weekly_timesheet_source_enum as enum ('NONE', 'NHSP', 'HEALTHROSTER');

-- private.invoice_generate_candidate_change_seq
create sequence private.invoice_generate_candidate_change_seq as bigint increment by 1 minvalue 1 maxvalue 9223372036854775807 start with 1 cache 1 no cycle;

-- private.invoice_issue_candidate_change_seq
create sequence private.invoice_issue_candidate_change_seq as bigint increment by 1 minvalue 1 maxvalue 9223372036854775807 start with 1 cache 1 no cycle;

-- public.candidate_tmsref_seq
create sequence public.candidate_tmsref_seq as bigint increment by 1 minvalue 1 maxvalue 9223372036854775807 start with 1 cache 1 no cycle;

-- public.client_cliref_seq
create sequence public.client_cliref_seq as bigint increment by 1 minvalue 1 maxvalue 9223372036854775807 start with 1 cache 1 no cycle;

-- public.global_rev_seq
create sequence public.global_rev_seq as bigint increment by 1 minvalue 1 maxvalue 9223372036854775807 start with 1 cache 1 no cycle;

-- public.id_ref_seq
create sequence public.id_ref_seq as bigint increment by 1 minvalue 1 maxvalue 9223372036854775807 start with 100001 cache 1 no cycle;

-- public.invoice_no_seq
create sequence public.invoice_no_seq as bigint increment by 1 minvalue 1 maxvalue 9223372036854775807 start with 1000001 cache 1 no cycle;

-- public.invoice_operation_change_seq
create sequence public.invoice_operation_change_seq as bigint increment by 1 minvalue 1 maxvalue 9223372036854775807 start with 1 cache 1 no cycle;

-- public.migration_smoke_once_only_id_seq
create sequence public.migration_smoke_once_only_id_seq as bigint increment by 1 minvalue 1 maxvalue 9223372036854775807 start with 1 cache 1 no cycle;

-- public.pay_bulk_ref_seq
create sequence public.pay_bulk_ref_seq as bigint increment by 1 minvalue 1000000 maxvalue 9999999 start with 1000000 cache 1 no cycle;

-- private.banking_pay_workbench_candidate_scope_registry
create table private.banking_pay_workbench_candidate_scope_registry (
  candidate_id uuid not null,
  initialisation_status text not null,
  dirty_generation bigint not null,
  evaluated_generation bigint not null,
  current_source_change_seq bigint not null,
  current_build_id uuid,
  bootstrap_id uuid,
  bootstrap_stream text,
  bootstrap_cursor_json jsonb not null,
  bootstrap_rows_seen bigint not null,
  bootstrap_timesheets_registered bigint not null,
  bootstrap_captured_generation bigint,
  bootstrap_captured_source_change_seq bigint,
  last_dirty_reason text not null,
  last_scope_change_tx_token uuid,
  last_dirtied_at_utc timestamp with time zone not null,
  last_evaluated_at_utc timestamp with time zone,
  initialised_at_utc timestamp with time zone,
  failure_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- private.banking_pay_workbench_canonical_stage_lines
create table private.banking_pay_workbench_canonical_stage_lines (
  build_id uuid not null,
  source_ordinal bigint not null,
  session_id uuid not null,
  candidate_id uuid not null,
  session_version bigint not null,
  source_change_seq bigint not null,
  source_build_run_id uuid not null,
  timesheet_id uuid,
  line_key text not null,
  parent_line_key text,
  split_suffix text,
  section text,
  source_row_json jsonb not null,
  economic_key_json jsonb not null,
  contract_json jsonb not null,
  pay_channel_scope text not null,
  refresh_scope_kind text not null,
  row_digest text not null,
  stage_status text not null,
  verified_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null
);

-- private.banking_pay_workbench_economic_build_fact_pages
create table private.banking_pay_workbench_economic_build_fact_pages (
  id uuid not null,
  build_id uuid not null,
  attempt_id uuid not null,
  dependency_unit_key text not null,
  fact_family text not null,
  page_number integer not null,
  cursor_start_json jsonb not null,
  cursor_start_hash text not null,
  cursor_end_json jsonb not null,
  cursor_end_hash text not null,
  expected_source_count integer,
  actual_fact_count integer not null,
  cumulative_fact_count integer not null,
  page_digest text not null,
  cumulative_digest text not null,
  is_family_final boolean not null,
  status text not null,
  completed_at_utc timestamp with time zone not null,
  created_at_utc timestamp with time zone not null
);

-- private.banking_pay_workbench_economic_build_facts
create table private.banking_pay_workbench_economic_build_facts (
  build_id uuid not null,
  fact_family text not null,
  natural_key text not null,
  candidate_id uuid not null,
  timesheet_id uuid,
  subject_timesheet_ids uuid[] not null,
  dependency_unit_key text,
  source_relation text not null,
  source_id uuid,
  source_subkey text,
  economic_key_type text,
  economic_key_value text,
  amount_ex_vat numeric,
  amount_inc_vat numeric,
  truth_ex_vat numeric,
  truth_inc_vat numeric,
  baseline_ex_vat numeric,
  baseline_inc_vat numeric,
  reserved_source_amount numeric,
  finance_case_id uuid,
  finance_component_id uuid,
  reservation_id uuid,
  edge_kind text,
  edge_from_timesheet_id uuid,
  edge_to_timesheet_id uuid,
  edge_source_id uuid,
  payload_schema_version integer not null,
  source_payload_json jsonb not null,
  diagnostic_json jsonb not null,
  financial_digest text not null,
  source_ordinal bigint,
  created_at_utc timestamp with time zone not null
);

-- private.banking_pay_workbench_economic_build_scope
create table private.banking_pay_workbench_economic_build_scope (
  build_id uuid not null,
  timesheet_id uuid not null,
  candidate_id uuid not null,
  stable_ordinal bigint,
  root_timesheet_id uuid,
  dependency_unit_anchor_timesheet_id uuid,
  dependency_unit_key text,
  dependency_unit_digest text,
  seed_reasons text[] not null,
  dependency_reasons text[] not null,
  captured_dirty_generation bigint not null,
  captured_input_fingerprint text,
  closure_status text not null,
  closure_family_ordinal smallint not null,
  closure_last_edge_key text,
  closure_processed_edge_count bigint not null,
  closure_processed_emission_count bigint not null,
  required_fact_families text[] not null,
  completed_fact_families text[] not null,
  fact_row_count integer not null,
  fact_digest text,
  seal_prepared_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- private.banking_pay_workbench_economic_builds
create table private.banking_pay_workbench_economic_builds (
  id uuid not null,
  build_token uuid not null,
  candidate_id uuid not null,
  session_id uuid not null,
  session_version bigint not null,
  source_snapshot_run_id uuid,
  source_build_run_id uuid not null,
  source_job_id uuid,
  captured_candidate_generation bigint not null,
  source_change_seq bigint not null,
  status text not null,
  private_stage text not null,
  stage_version integer not null,
  scope_cursor_json jsonb not null,
  closure_cursor_json jsonb not null,
  fact_cursor_json jsonb not null,
  publication_cursor_json jsonb not null,
  cleanup_cursor_json jsonb not null,
  seed_scope_count integer not null,
  seed_scope_digest text,
  seed_scope_sealed_at_utc timestamp with time zone,
  scope_count integer not null,
  dependency_node_count integer not null,
  dependency_edge_count bigint not null,
  dependency_edge_stream_digest text,
  dependency_edge_stream_terminal_key_json jsonb not null,
  dependency_edge_stream_complete boolean not null,
  tagged_edge_count bigint not null,
  edge_tag_stream_terminal_key_json jsonb not null,
  edge_tag_digest text,
  edge_tag_stream_complete boolean not null,
  unit_count integer not null,
  unit_digest text,
  row_seal_count bigint not null,
  last_stable_ordinal bigint not null,
  sealed_fingerprint_digest text,
  fact_count bigint not null,
  canonical_count integer not null,
  scope_digest text,
  dependency_digest text,
  pre_sync_digest text,
  post_sync_digest text,
  canonical_digest text,
  complexity_vector_json jsonb not null,
  envelope_version integer,
  envelope_snapshot_json jsonb not null,
  envelope_evidence_json jsonb not null,
  attestation_json jsonb not null,
  failure_json jsonb not null,
  dependency_closure_sealed_at_utc timestamp with time zone,
  cleanup_not_before_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  ready_at_utc timestamp with time zone,
  reconciled_at_utc timestamp with time zone,
  completed_at_utc timestamp with time zone,
  obsolete_at_utc timestamp with time zone,
  failed_at_utc timestamp with time zone,
  authority_fingerprint_version smallint,
  authority_fingerprint text
);

-- private.banking_pay_workbench_queue_scan_state
create table private.banking_pay_workbench_queue_scan_state (
  lane_identity text not null,
  scan_kind text not null,
  scan_scope_key text not null,
  cursor_generation bigint,
  cursor_chain_rank smallint,
  cursor_priority integer,
  cursor_due_at timestamp with time zone,
  cursor_created_at timestamp with time zone,
  cursor_object_id uuid,
  sweep_generation bigint not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- private.banking_pay_workbench_stage_attempts
create table private.banking_pay_workbench_stage_attempts (
  id uuid not null,
  job_id uuid not null,
  build_id uuid not null,
  candidate_id uuid not null,
  private_stage text not null,
  attempt_number integer not null,
  attempt_nonce uuid not null,
  worker_id text not null,
  lane_identity text not null,
  captured_candidate_generation bigint not null,
  captured_source_change_seq bigint not null,
  execution_profile_version integer not null,
  attempt_status text not null,
  started_at_utc timestamp with time zone not null,
  lease_expires_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone,
  failed_at_utc timestamp with time zone,
  expired_at_utc timestamp with time zone,
  obsolete_at_utc timestamp with time zone,
  result_code text,
  result_digest text,
  error_class text,
  error_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- private.banking_pay_workbench_timesheet_scope_state
create table private.banking_pay_workbench_timesheet_scope_state (
  timesheet_id uuid not null,
  candidate_id uuid not null,
  economic_state text not null,
  dirty_generation bigint not null,
  evaluated_generation bigint,
  current_input_fingerprint text,
  evaluated_input_fingerprint text,
  last_dirty_reason text not null,
  last_scope_change_tx_token uuid,
  current_build_id uuid,
  registered_at_utc timestamp with time zone not null,
  last_dirtied_at_utc timestamp with time zone not null,
  last_evaluated_at_utc timestamp with time zone,
  closed_at_utc timestamp with time zone,
  updated_at_utc timestamp with time zone not null
);

-- private.candidate_daily_authority_scopes
create table private.candidate_daily_authority_scopes (
  environment text not null,
  candidate_id uuid not null,
  authority_mode text not null,
  canonical_version bigint not null,
  active_generation_id uuid,
  last_transition_id uuid,
  transition_in_progress boolean not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- private.candidate_daily_authority_transitions
create table private.candidate_daily_authority_transitions (
  transition_id uuid not null,
  batch_receipt_id uuid not null,
  environment text not null,
  candidate_id uuid not null,
  prior_authority_mode text not null,
  new_authority_mode text not null,
  effective_at_utc timestamp with time zone not null,
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
  supersedes_transition_id uuid,
  correlation_id text not null,
  created_at_utc timestamp with time zone not null
);

-- private.candidate_daily_batch_receipts
create table private.candidate_daily_batch_receipts (
  batch_receipt_id uuid not null,
  environment text not null,
  actor_class text not null,
  operation_class text not null,
  idempotency_key text not null,
  request_hash text not null,
  item_keys_json jsonb not null,
  item_count integer not null,
  state text not null,
  lease_owner text,
  lease_token text,
  lease_expires_at_utc timestamp with time zone,
  item_receipt_ids_json jsonb not null,
  claim_request_id uuid,
  claim_target text,
  claim_limit integer,
  claimed_items_json jsonb,
  terminal_http_status integer,
  terminal_response_body jsonb,
  terminal_response_sha256 text,
  correlation_id text not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone
);

-- private.candidate_daily_entitlements
create table private.candidate_daily_entitlements (
  environment text not null,
  candidate_id uuid not null,
  enabled boolean not null,
  valid_from_utc timestamp with time zone,
  valid_to_utc timestamp with time zone,
  actor_user_id uuid,
  reason text not null,
  evidence_sha256 text not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- private.candidate_daily_external_effect_receipts
create table private.candidate_daily_external_effect_receipts (
  effect_receipt_id uuid not null,
  environment text not null,
  candidate_id uuid not null,
  effect_key text not null,
  operation text not null,
  shift_identity text,
  source_event_identity text,
  request_hash text not null,
  idempotency_key text not null,
  state text not null,
  first_claimed_at_utc timestamp with time zone not null,
  lease_owner text,
  lease_token text,
  lease_expires_at_utc timestamp with time zone,
  stable_provider_request_id text not null,
  provider_result_identity text,
  provider_reference_hash text,
  attempt_count integer not null,
  safe_evidence_json jsonb not null,
  terminal_result_json jsonb,
  terminal_body_sha256 text,
  reconciliation_outcome text,
  correlation_id text not null,
  retain_until_utc timestamp with time zone not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone
);

-- private.candidate_daily_source_links
create table private.candidate_daily_source_links (
  link_id uuid not null,
  environment text not null,
  candidate_id uuid not null,
  source_system text not null,
  canonicalization_version text not null,
  link_group_id uuid not null,
  identifier_hmac text not null,
  hmac_key_version integer not null,
  state text not null,
  valid_from_utc timestamp with time zone not null,
  valid_to_utc timestamp with time zone,
  rotation_receipt_id uuid,
  actor_user_id uuid,
  independent_approver_user_id uuid,
  evidence_sha256 text not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- private.candidate_daily_sync_state
create table private.candidate_daily_sync_state (
  environment text not null,
  candidate_id uuid not null,
  target text not null,
  accepted_canonical_cursor bigint not null,
  required_visible_cursor bigint not null,
  delivered_visible_cursor bigint not null,
  overlay_proof_cursor bigint not null,
  effective_visible_cursor bigint not null,
  observed_source_revision text,
  pending_count integer not null,
  retry_count integer not null,
  deferred_count integer not null,
  terminal_count integer not null,
  last_acknowledged_at_utc timestamp with time zone,
  last_pulled_at_utc timestamp with time zone,
  last_reconciled_at_utc timestamp with time zone,
  state text not null,
  safe_error_code text,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- private.invoice_async_snapshot_hmac_keys
create table private.invoice_async_snapshot_hmac_keys (
  key_id text not null,
  vault_secret_id uuid not null,
  active_from_utc timestamp with time zone not null,
  active_to_utc timestamp with time zone,
  verify_until_utc timestamp with time zone,
  is_current boolean not null,
  created_at_utc timestamp with time zone not null
);

-- public.app_change_counters
create table public.app_change_counters (
  entity_key text not null,
  seq bigint not null,
  updated_at timestamp with time zone not null,
  scope_change_generation bigint,
  scope_change_tx_token uuid
);

-- public.assignment_band_mappings
create table public.assignment_band_mappings (
  id uuid not null,
  system_type text not null,
  incoming_code text not null,
  candidate_id uuid,
  client_id uuid,
  band_match_pattern text not null,
  active boolean not null,
  notes text,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  target_contract_id uuid
);

-- public.audit_events
create table public.audit_events (
  id uuid not null,
  ts_utc timestamp with time zone not null,
  actor_user_id uuid,
  actor_display text,
  actor_role_at_time text,
  object_type text not null,
  object_id_text text,
  action text not null,
  before_json jsonb,
  after_json jsonb,
  reason text,
  ip text,
  user_agent text,
  correlation_id text
);

-- public.bank_name_checks
create table public.bank_name_checks (
  id uuid not null,
  rail_provider text not null,
  rail_env text not null,
  entity_kind text not null,
  entity_id uuid not null,
  bank_details_hash text not null,
  status text not null,
  checked_at_utc timestamp with time zone,
  result_json jsonb,
  override_reason text,
  override_by_user_id uuid,
  override_at_utc timestamp with time zone,
  override_hash text,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.bank_payee_map
create table public.bank_payee_map (
  id uuid not null,
  rail_provider text not null,
  rail_env text not null,
  entity_kind text not null,
  entity_id uuid not null,
  bank_details_hash text not null,
  payee_id text not null,
  payee_account_id text,
  meta_json jsonb,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.bank_provider_webhook_configs
create table public.bank_provider_webhook_configs (
  id uuid not null,
  provider_key text not null,
  rail_env text not null,
  webhook_public_id text not null,
  provider_webhook_id text,
  webhook_url text not null,
  event_types jsonb not null,
  signing_secret_ref text,
  signing_secret_encrypted text,
  old_signing_secret_encrypted text,
  old_secret_expires_at_utc timestamp with time zone,
  status text not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  last_verified_at_utc timestamp with time zone,
  last_event_at_utc timestamp with time zone,
  last_error_at_utc timestamp with time zone,
  last_error text,
  last_failed_event_sync_at_utc timestamp with time zone,
  meta_json jsonb not null
);

-- public.bank_provider_webhook_receipts
create table public.bank_provider_webhook_receipts (
  id uuid not null,
  provider_key text not null,
  rail_env text not null,
  webhook_config_id uuid,
  provider_webhook_id text,
  provider_event_type text,
  provider_event_id text,
  provider_event_key text not null,
  provider_transaction_id text,
  provider_request_id text,
  signature_valid boolean not null,
  signature_version text,
  signature_header text,
  request_timestamp text,
  request_received_at_utc timestamp with time zone not null,
  event_time_utc timestamp with time zone,
  raw_payload_json jsonb,
  raw_payload_hash text not null,
  raw_headers_redacted jsonb not null,
  normalised_events_json jsonb not null,
  ingest_results_json jsonb not null,
  status text not null,
  error_code text,
  error_message text,
  attempt_count integer not null,
  processed_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_alert_acknowledgements
create table public.banking_alert_acknowledgements (
  id uuid not null,
  alert_fingerprint text not null,
  alert_kind text not null,
  entity_kind text not null,
  entity_id uuid not null,
  acknowledged_by_user_id uuid not null,
  acknowledged_at_utc timestamp with time zone not null,
  acknowledge_scope text not null,
  note text,
  alert_payload_json jsonb not null,
  resolved_at_ack boolean not null
);

-- public.banking_alert_display_summary
create table public.banking_alert_display_summary (
  id uuid not null,
  actor_user_id uuid not null,
  alert_hash text,
  summary_hash text,
  unacknowledged_count integer not null,
  highest_severity text,
  highest_label text,
  summary_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_alert_success_events
create table public.banking_alert_success_events (
  id uuid not null,
  pay_batch_id uuid not null,
  alert_kind text not null,
  event_key text not null,
  payload_json jsonb not null,
  occurred_at_utc timestamp with time zone not null,
  expires_at_utc timestamp with time zone not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_alert_user_preferences
create table public.banking_alert_user_preferences (
  id uuid not null,
  user_id uuid not null,
  enabled boolean not null,
  alert_kind_allowlist jsonb,
  alert_kind_blocklist jsonb not null,
  failure_reason_allowlist jsonb,
  failure_reason_blocklist jsonb not null,
  include_action_required boolean not null,
  include_progress_alerts boolean not null,
  include_informational_alerts boolean not null,
  include_success_alerts boolean not null,
  severity_min text not null,
  muted_provider_keys jsonb not null,
  muted_pay_batch_ids jsonb not null,
  snoozed_until_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_batch_change_signals
create table public.banking_pay_batch_change_signals (
  pay_batch_id uuid not null,
  version bigint not null,
  payment_status_version bigint not null,
  correction_progress_version bigint not null,
  alert_version bigint not null,
  overview_version bigint not null,
  last_changed_at_utc timestamp with time zone not null,
  last_payment_status_changed_at_utc timestamp with time zone,
  last_correction_progress_changed_at_utc timestamp with time zone,
  last_alert_changed_at_utc timestamp with time zone,
  last_change_reason text,
  last_change_source text,
  last_change_scope_json jsonb not null,
  last_changed_transfer_ids jsonb not null,
  last_changed_candidate_ids jsonb not null,
  last_changed_pay_batch_item_ids jsonb not null,
  last_status_hash text,
  last_alert_hash text,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_operation_candidate_allocation_rows
create table public.banking_pay_operation_candidate_allocation_rows (
  id uuid not null,
  operation_id uuid not null,
  candidate_scope_id uuid not null,
  pay_batch_id uuid,
  candidate_id uuid not null,
  pay_channel text not null,
  finance_case_id uuid,
  finance_component_id uuid,
  allocation_type text not null,
  source_ref text,
  operation_source_key text not null,
  allocated_amount numeric not null,
  allocation_basis_json jsonb not null,
  sort_order integer not null,
  status text not null,
  pay_batch_item_id uuid,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_operation_candidate_scope
create table public.banking_pay_operation_candidate_scope (
  id uuid not null,
  operation_id uuid not null,
  workbench_session_id uuid not null,
  source_snapshot_run_id uuid,
  source_session_version bigint,
  candidate_state_id uuid,
  candidate_id uuid not null,
  pay_channel text not null,
  pay_batch_id uuid,
  selected_preview_row_ids_json jsonb not null,
  selected_timesheet_ids_json jsonb not null,
  selected_finance_case_ids_json jsonb not null,
  effective_candidate_fragment_json jsonb not null,
  effective_summary_fragment_json jsonb not null,
  effective_paye_candidate_json jsonb not null,
  effective_non_paye_payee_json jsonb not null,
  effective_payees_json jsonb not null,
  effective_case_resolution_states_json jsonb not null,
  effective_canonical_preview_lines_json jsonb not null,
  selected_canonical_preview_lines_json jsonb not null,
  baseline_component_rows_json jsonb not null,
  hidden_recovery_template_lines_json jsonb not null,
  candidate_totals_json jsonb not null,
  allocation_basis_json jsonb not null,
  scope_hash text not null,
  chunk_sequence integer,
  status text not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_operation_chunks
create table public.banking_pay_operation_chunks (
  id uuid not null,
  operation_id uuid not null,
  phase text not null,
  chunk_type text not null,
  sequence_no integer not null,
  status text not null,
  payload_json jsonb not null,
  result_json jsonb,
  error_json jsonb,
  unit_count integer not null,
  completed_count integer not null,
  failed_count integer not null,
  locked_by text,
  lock_expires_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  started_at_utc timestamp with time zone,
  completed_at_utc timestamp with time zone,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_operation_config
create table public.banking_pay_operation_config (
  id uuid not null,
  operation_type text not null,
  phase text not null,
  chunk_type text not null,
  default_chunk_size integer not null,
  min_chunk_size integer not null,
  max_chunk_size integer not null,
  max_advance_ms integer not null,
  lock_seconds integer not null,
  enabled boolean not null,
  updated_at_utc timestamp with time zone not null,
  updated_by uuid
);

-- public.banking_pay_operation_provider_attempts
create table public.banking_pay_operation_provider_attempts (
  id uuid not null,
  operation_id uuid not null,
  pay_batch_id uuid,
  transfer_scope_id uuid,
  provider_chunk_id uuid,
  provider_idempotency_key text,
  provider_request_id text,
  provider_transaction_id text,
  previous_state text,
  new_state text,
  lease_owner text,
  compact_request_hash text,
  compact_response_hash text,
  compact_error_summary_json jsonb,
  created_at_utc timestamp with time zone not null
);

-- public.banking_pay_operation_remittance_scope
create table public.banking_pay_operation_remittance_scope (
  id uuid not null,
  operation_id uuid not null,
  pay_batch_id uuid not null,
  pay_batch_candidate_id uuid,
  candidate_id uuid,
  recipient_kind text not null,
  recipient_id uuid,
  remittance_type text not null,
  deterministic_outbox_key text not null,
  payload_json jsonb not null,
  status text not null,
  outbox_id uuid,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_operation_scope_units
create table public.banking_pay_operation_scope_units (
  id uuid not null,
  operation_id uuid not null,
  pay_batch_id uuid,
  phase text not null,
  unit_type text not null,
  unit_key text not null,
  unit_payload_json jsonb not null,
  unit_ordinal bigint not null,
  status text not null,
  chunk_id uuid,
  result_hash text,
  error_json jsonb,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_operation_settlement_scope
create table public.banking_pay_operation_settlement_scope (
  id uuid not null,
  operation_id uuid not null,
  pay_batch_id uuid not null,
  pay_batch_candidate_id uuid,
  candidate_id uuid,
  pay_channel text not null,
  settlement_key text not null,
  payload_json jsonb not null,
  status text not null,
  settlement_event_id uuid,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_operation_transfer_scope
create table public.banking_pay_operation_transfer_scope (
  id uuid not null,
  operation_id uuid not null,
  pay_batch_id uuid not null,
  pay_channel text not null,
  transfer_group_key text not null,
  candidate_id uuid,
  umbrella_id uuid,
  payee_entity_kind text,
  payee_entity_id uuid,
  pay_batch_item_ids_json jsonb not null,
  candidate_ids_json jsonb not null,
  currency text not null,
  amount numeric not null,
  payment_reference text,
  payee_name text,
  sort_code text,
  account_number text,
  account_type text,
  bank_details_hash_snapshot text,
  grouping_mode_used text,
  week_ending_bucket date,
  request_id text,
  status text not null,
  pay_bank_transfer_id uuid,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  provider_submit_ready boolean not null,
  provider_submit_state text not null,
  provider_submit_chunk_id uuid,
  provider_submit_claimed_at_utc timestamp with time zone,
  provider_submit_attempt_count integer not null,
  provider_idempotency_key text,
  provider_request_id text,
  provider_transaction_id text,
  provider_request_prepared_at_utc timestamp with time zone,
  provider_request_sending_at_utc timestamp with time zone,
  provider_request_sent_at_utc timestamp with time zone,
  provider_response_at_utc timestamp with time zone,
  provider_submission_status text,
  provider_review_required boolean not null,
  provider_unsafe_reason text,
  prepared_item_count integer not null,
  prepared_amount_total numeric(14,2) not null,
  prepared_scope_hash text,
  prepared_result_hash text
);

-- public.banking_pay_operation_transfer_scope_items
create table public.banking_pay_operation_transfer_scope_items (
  id uuid not null,
  operation_id uuid not null,
  pay_batch_id uuid not null,
  transfer_scope_id uuid not null,
  pay_batch_item_id uuid not null,
  pay_batch_candidate_id uuid,
  candidate_id uuid,
  item_amount numeric(14,2) not null,
  item_status text not null,
  item_ordinal bigint not null,
  rollup_status text not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_operations
create table public.banking_pay_operations (
  id uuid not null,
  operation_type text not null,
  status text not null,
  phase text not null,
  actor_user_id uuid,
  workbench_session_id uuid,
  pay_batch_id uuid,
  root_operation_id uuid,
  idempotency_key text not null,
  input_json jsonb not null,
  config_json jsonb not null,
  progress_json jsonb not null,
  result_json jsonb,
  error_json jsonb,
  total_units integer not null,
  completed_units integer not null,
  failed_units integer not null,
  current_chunk_index integer not null,
  chunk_count integer not null,
  locked_by text,
  lock_expires_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  started_at_utc timestamp with time zone,
  updated_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone,
  failed_at_utc timestamp with time zone,
  run_after_utc timestamp with time zone,
  lease_owner text,
  lease_expires_at_utc timestamp with time zone,
  heartbeat_at_utc timestamp with time zone,
  last_advanced_at_utc timestamp with time zone,
  attempt_count integer not null,
  max_attempts integer not null,
  requires_user_action boolean not null,
  runner_state text not null,
  resume_reason text,
  scope_freeze_status text not null,
  frozen_scope_change_generation bigint,
  scope_frozen_at_utc timestamp with time zone,
  source_scope_seed_complete boolean not null,
  frozen_candidate_scope_count integer,
  frozen_selected_row_count integer,
  frozen_operation_scope_hash text,
  frozen_source_session_version bigint,
  frozen_source_snapshot_run_id uuid,
  post_freeze_scope_status text not null,
  post_freeze_observed_generation bigint,
  post_freeze_relevant_generation bigint,
  post_freeze_scope_checked_at_utc timestamp with time zone
);

-- public.banking_pay_scope_change_transactions
create table public.banking_pay_scope_change_transactions (
  tx_token uuid not null,
  state text not null,
  allocated_generation bigint,
  created_at_utc timestamp with time zone not null,
  finalized_at_utc timestamp with time zone
);

-- public.banking_pay_snapshot_candidate_state
create table public.banking_pay_snapshot_candidate_state (
  id uuid not null,
  snapshot_run_id uuid not null,
  candidate_id uuid not null,
  status text not null,
  candidate_fragment_json jsonb not null,
  summary_fragment_json jsonb not null,
  paye_candidate_json jsonb,
  non_paye_payee_json jsonb,
  payees_json jsonb not null,
  case_resolution_states_json jsonb not null,
  canonical_preview_lines_json jsonb not null,
  source_change_seq bigint not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  last_refreshed_at_utc timestamp with time zone,
  last_error_json jsonb
);

-- public.banking_pay_snapshot_case_component_state
create table public.banking_pay_snapshot_case_component_state (
  id uuid not null,
  snapshot_run_id uuid not null,
  candidate_id uuid not null,
  case_key text not null,
  timesheet_id uuid,
  component_fingerprint text,
  source_basis_fingerprint text,
  source_family_key text,
  bucket_code text,
  component_key_type text,
  component_key_value text,
  component_state_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_snapshot_case_state
create table public.banking_pay_snapshot_case_state (
  id uuid not null,
  snapshot_run_id uuid not null,
  candidate_id uuid not null,
  case_key text not null,
  finance_case_id uuid,
  case_state_json jsonb not null,
  is_blocked boolean not null,
  is_resolved boolean not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_snapshot_line_state
create table public.banking_pay_snapshot_line_state (
  id uuid not null,
  snapshot_run_id uuid not null,
  candidate_id uuid not null,
  preview_row_id text not null,
  timesheet_id uuid,
  case_key text,
  pay_channel text,
  component_key_type text,
  component_key_value text,
  canonical_line_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_snapshot_runs
create table public.banking_pay_snapshot_runs (
  id uuid not null,
  pay_date date not null,
  week_ending_cutoff date not null,
  pay_week_start date not null,
  eligibility_from_date date not null,
  eligibility_to_date date not null,
  status text not null,
  is_active boolean not null,
  summary_json jsonb not null,
  paye_guardrails_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  ready_at_utc timestamp with time zone,
  failed_at_utc timestamp with time zone,
  last_error_json jsonb
);

-- public.banking_pay_workbench_candidate_delta_projection_runs
create table public.banking_pay_workbench_candidate_delta_projection_runs (
  id uuid not null,
  session_id uuid not null,
  candidate_id uuid not null,
  session_version bigint not null,
  source_change_seq bigint,
  source_snapshot_run_id uuid,
  projection_mode text not null,
  projection_class text not null,
  phase text not null,
  cursor_json jsonb not null,
  targeted_timesheet_ids jsonb not null,
  linked_timesheet_ids jsonb not null,
  status text not null,
  fallback_required boolean not null,
  fallback_reason text,
  legacy_compare_status text,
  legacy_compare_json jsonb not null,
  diagnostics_json jsonb not null,
  started_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone,
  write_phase text,
  write_cursor_json jsonb not null,
  projected_row_count integer not null,
  written_source_count integer not null,
  written_line_work_count integer not null,
  written_preview_count integer not null,
  candidate_state_updated boolean not null,
  projection_fingerprint text,
  shadow_compare_required boolean not null,
  shadow_compare_status text,
  shadow_compare_enforced boolean not null,
  admission_seal_version integer,
  admission_seal_json jsonb not null,
  admission_seal_digest text,
  admission_sealed_at_utc timestamp with time zone
);

-- public.banking_pay_workbench_candidate_line_work
create table public.banking_pay_workbench_candidate_line_work (
  id uuid not null,
  session_id uuid not null,
  candidate_id uuid not null,
  timesheet_id uuid,
  line_key text not null,
  line_ordinal bigint not null,
  status text not null,
  work_payload_json jsonb not null,
  result_row_json jsonb,
  error_json jsonb,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_workbench_candidate_source_lines
create table public.banking_pay_workbench_candidate_source_lines (
  id uuid not null,
  session_id uuid not null,
  candidate_id uuid not null,
  session_version bigint not null,
  source_change_seq bigint not null,
  source_build_run_id uuid not null,
  source_ordinal bigint not null,
  line_key text not null,
  parent_line_key text,
  split_suffix text,
  timesheet_id uuid,
  section text,
  source_row_json jsonb not null,
  economic_key_json jsonb not null,
  contract_json jsonb not null,
  pay_channel_scope text not null,
  refresh_scope_kind text not null,
  status text not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  source_publication_id uuid
);

-- public.banking_pay_workbench_case_resolution_carry_registrations
create table public.banking_pay_workbench_case_resolution_carry_registrations (
  id uuid not null,
  target_session_id uuid not null,
  source_session_id uuid not null,
  candidate_id uuid not null,
  source_resolution_id uuid not null,
  source_resolution_identity_key text not null,
  canonical_resolution_key text not null,
  resolution_scope_kind text not null,
  source_economic_fingerprint text not null,
  source_resolution_snapshot_json jsonb not null,
  source_priority integer not null,
  carry_reason text not null,
  status text not null,
  state_reason_code text,
  target_source_build_run_id uuid,
  target_resolution_id uuid,
  target_economic_fingerprint text,
  attempt_count integer not null,
  last_attempt_at_utc timestamp with time zone,
  last_error_json jsonb,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone
);

-- public.banking_pay_workbench_jobs
create table public.banking_pay_workbench_jobs (
  id uuid not null,
  job_type text not null,
  status text not null,
  priority integer not null,
  run_at_utc timestamp with time zone not null,
  attempt_count integer not null,
  max_attempts integer not null,
  dedupe_key text not null,
  snapshot_run_id uuid,
  session_id uuid,
  candidate_id uuid,
  payload_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  started_at_utc timestamp with time zone,
  completed_at_utc timestamp with time zone,
  failed_at_utc timestamp with time zone,
  last_error_json jsonb,
  scope_change_generation bigint,
  scope_change_tx_token uuid,
  economic_build_id uuid,
  private_stage text,
  private_cursor_kind text,
  private_cursor_json jsonb not null,
  private_stage_version integer
);

-- public.banking_pay_workbench_preview_rows
create table public.banking_pay_workbench_preview_rows (
  id uuid not null,
  session_id uuid not null,
  candidate_id uuid not null,
  section text not null,
  row_key text not null,
  row_ordinal bigint not null,
  row_json jsonb not null,
  timesheet_id uuid,
  key_type text,
  key_value text,
  selected boolean not null,
  selection_state text not null,
  status text not null,
  session_version bigint,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_workbench_selection_carry_registrations
create table public.banking_pay_workbench_selection_carry_registrations (
  id uuid not null,
  target_session_id uuid not null,
  source_session_id uuid not null,
  candidate_id uuid not null,
  source_preview_row_id uuid,
  stable_selection_key text not null,
  selected boolean not null,
  selection_state text not null,
  source_priority integer not null,
  carry_reason text not null,
  status text not null,
  state_reason_code text,
  source_row_snapshot_json jsonb not null,
  target_preview_row_id uuid,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone
);

-- public.banking_pay_workbench_session_candidate_state
create table public.banking_pay_workbench_session_candidate_state (
  id uuid not null,
  session_id uuid not null,
  candidate_id uuid not null,
  status text not null,
  effective_candidate_fragment_json jsonb not null,
  effective_summary_fragment_json jsonb not null,
  effective_paye_candidate_json jsonb,
  effective_non_paye_payee_json jsonb,
  effective_payees_json jsonb not null,
  effective_case_resolution_states_json jsonb not null,
  effective_canonical_preview_lines_json jsonb not null,
  source_change_seq bigint not null,
  session_version bigint not null,
  pending_job_id uuid,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  last_recomputed_at_utc timestamp with time zone,
  last_error_json jsonb
);

-- public.banking_pay_workbench_session_case_resolutions
create table public.banking_pay_workbench_session_case_resolutions (
  id uuid not null,
  session_id uuid not null,
  candidate_id uuid not null,
  case_key text not null,
  resolution_family text not null,
  resolution_identity_key text not null,
  timesheet_id uuid,
  source_basis_fingerprint text,
  source_family_key text,
  bucket_code text,
  component_key_type text,
  component_key_value text,
  payload_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  resolution_origin_session_id uuid not null,
  resolution_origin_pay_date date not null,
  resolution_origin_source_basis_fingerprint text
);

-- public.banking_pay_workbench_session_overrides
create table public.banking_pay_workbench_session_overrides (
  id uuid not null,
  session_id uuid not null,
  candidate_id uuid not null,
  override_type text not null,
  override_identity_key text not null,
  timesheet_id uuid,
  payload_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.banking_pay_workbench_session_scope
create table public.banking_pay_workbench_session_scope (
  id uuid not null,
  session_id uuid not null,
  candidate_id uuid not null,
  scope_ordinal bigint not null,
  status text not null,
  pending_job_id uuid,
  seeded boolean not null,
  dirty boolean not null,
  error_json jsonb,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  certified_preview_publication_required boolean not null,
  certified_preview_publication_parity_ok boolean not null,
  certified_preview_publication_session_version bigint,
  certified_preview_publication_source_change_seq bigint,
  certified_preview_publication_source_build_run_id uuid,
  certified_preview_publication_attestation_json jsonb not null,
  certified_preview_publication_attested_at_utc timestamp with time zone,
  certified_preview_publication_source_publication_id uuid
);

-- public.banking_pay_workbench_sessions
create table public.banking_pay_workbench_sessions (
  id uuid not null,
  actor_user_id uuid not null,
  pay_date date not null,
  week_ending_cutoff date not null,
  filters_json jsonb not null,
  __cloudtms_dropped_attnum_6 text,
  session_signature text not null,
  source_snapshot_run_id uuid not null,
  status text not null,
  version bigint not null,
  server_selected_preview_row_ids jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  discarded_at_utc timestamp with time zone,
  server_selected_preview_row_ids_provided boolean not null,
  scope_next_cursor_json jsonb not null,
  scope_seed_complete boolean not null,
  scope_total_count integer not null,
  scope_seeded_count integer not null,
  scope_ready_count integer not null,
  scope_pending_count integer not null,
  scope_failed_count integer not null,
  line_units_total integer not null,
  line_units_ready integer not null,
  line_units_pending integer not null,
  line_units_failed integer not null,
  preview_row_count integer not null,
  selected_row_count integer not null,
  section_counts_json jsonb not null,
  candidate_sample_rows_json jsonb not null,
  progress_state text not null,
  progress_json jsonb not null,
  progress_counter_version bigint not null,
  progress_updated_at_utc timestamp with time zone not null,
  __cloudtms_dropped_attnum_35 text,
  scope_candidate_ids uuid[],
  replacement_session_id uuid,
  replacement_idempotency_key text,
  scope_discovery_checked_at_utc timestamp with time zone,
  scope_change_generation_target bigint not null,
  scope_change_generation_applied bigint not null,
  scope_change_generation_shadow_checked bigint not null
);
alter table public.banking_pay_workbench_sessions drop column __cloudtms_dropped_attnum_6;
alter table public.banking_pay_workbench_sessions drop column __cloudtms_dropped_attnum_35;

-- public.candidate_app_accounts
create table public.candidate_app_accounts (
  id uuid not null,
  environment text not null,
  email_normalized text not null,
  status text not null,
  password_scheme text,
  password_scheme_version smallint,
  password_salt bytea,
  password_digest bytea,
  password_params_json jsonb not null,
  password_changed_at_utc timestamp with time zone,
  failed_login_count integer not null,
  locked_until_utc timestamp with time zone,
  notification_preferences_json jsonb not null,
  session_version bigint not null,
  last_login_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.candidate_app_global_membership_links
create table public.candidate_app_global_membership_links (
  membership_id uuid not null,
  global_account_identity_hmac bytea not null,
  account_id uuid not null,
  candidate_id uuid not null,
  candidate_code text,
  membership_generation integer not null,
  state text not null,
  linked_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  revoked_at_utc timestamp with time zone,
  revoke_reason text
);

-- public.candidate_app_sessions
create table public.candidate_app_sessions (
  id uuid not null,
  account_id uuid not null,
  environment text not null,
  selected_candidate_id uuid,
  status text not null,
  refresh_token_hash bytea not null,
  token_family_id uuid not null,
  rotation integer not null,
  issued_at_utc timestamp with time zone not null,
  expires_at_utc timestamp with time zone not null,
  absolute_expires_at_utc timestamp with time zone not null,
  last_used_at_utc timestamp with time zone not null,
  revoked_at_utc timestamp with time zone,
  revoke_reason text,
  replaced_by_session_id uuid,
  device_id_hash bytea,
  platform text,
  push_provider text,
  push_token_ciphertext bytea,
  push_key_version smallint,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  auth_source text not null,
  global_account_identity_hmac bytea,
  global_session_identity_hmac bytea,
  membership_id uuid,
  membership_generation integer,
  route_version integer,
  session_epoch bigint
);

-- public.candidate_approval_requests
create table public.candidate_approval_requests (
  id uuid not null,
  workflow_id uuid not null,
  workflow_generation integer not null,
  method text not null,
  state text not null,
  manager_email_normalized text,
  manager_name text,
  manager_position text,
  token_hash bytea,
  expires_at_utc timestamp with time zone,
  initial_sent_at_utc timestamp with time zone,
  resend_count integer not null,
  last_sent_at_utc timestamp with time zone,
  next_reminder_at_utc timestamp with time zone,
  renewal_count integer not null,
  refusal_reason text,
  signature_component_id uuid,
  approved_at_utc timestamp with time zone,
  refused_at_utc timestamp with time zone,
  cancelled_at_utc timestamp with time zone,
  superseded_at_utc timestamp with time zone,
  idempotency_key text,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  request_generation integer not null,
  review_manifest_sha256 bytea,
  required_component_ids uuid[] not null,
  required_component_manifest_json jsonb not null,
  manager_review_timesheet_component_id uuid,
  manager_review_timesheet_sha256 bytea,
  review_progress_json jsonb not null,
  review_started_at_utc timestamp with time zone,
  review_completed_at_utc timestamp with time zone
);

-- public.candidate_auth_challenges
create table public.candidate_auth_challenges (
  id uuid not null,
  account_id uuid,
  environment text not null,
  email_normalized text not null,
  purpose text not null,
  state text not null,
  token_hash bytea not null,
  expires_at_utc timestamp with time zone not null,
  attempt_count integer not null,
  resend_count integer not null,
  last_sent_at_utc timestamp with time zone,
  verified_at_utc timestamp with time zone,
  consumed_at_utc timestamp with time zone,
  superseded_at_utc timestamp with time zone,
  superseded_by_id uuid,
  mail_outbox_id uuid,
  deterministic_outbox_key text not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.candidate_daily_availability_days
create table public.candidate_daily_availability_days (
  environment text not null,
  candidate_id uuid not null,
  availability_date date not null,
  preference text not null,
  availability_version bigint not null,
  source_class text not null,
  source_command_id uuid not null,
  changed_at_utc timestamp with time zone not null,
  changed_by_class text not null,
  row_hash text not null
);

-- public.candidate_daily_command_receipts
create table public.candidate_daily_command_receipts (
  command_id uuid not null,
  environment text not null,
  candidate_id uuid not null,
  actor_class text not null,
  command_class text not null,
  idempotency_key text not null,
  request_sha256 text not null,
  source_system text,
  source_event_id text,
  source_revision text,
  source_event_time timestamp with time zone,
  item_key text,
  canonical_version_before bigint not null,
  canonical_version_after bigint,
  state text not null,
  lease_owner text,
  lease_token text,
  lease_expires_at_utc timestamp with time zone,
  terminal_http_status integer,
  terminal_body_json jsonb,
  terminal_body_sha256 text,
  correlation_id text not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone
);

-- public.candidate_daily_rota_days
create table public.candidate_daily_rota_days (
  generation_id uuid not null,
  environment text not null,
  candidate_id uuid not null,
  rota_date date not null,
  booked boolean not null,
  system_blocked boolean not null,
  booking_id text,
  shift_starts_at timestamp with time zone,
  shift_ends_at timestamp with time zone,
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
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.candidate_daily_rota_generations
create table public.candidate_daily_rota_generations (
  generation_id uuid not null,
  environment text not null,
  candidate_id uuid not null,
  generation_version bigint not null,
  window_start date not null,
  window_end date not null,
  state text not null,
  expected_day_count integer not null,
  actual_day_count integer not null,
  source_system text not null,
  source_event_id text not null,
  source_revision text not null,
  source_event_time timestamp with time zone not null,
  item_key text not null,
  source_hash text not null,
  generation_row_hash text not null,
  batch_receipt_id uuid not null,
  correlation_id text not null,
  activated_at_utc timestamp with time zone,
  published_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.candidate_daily_sheet_projection_outbox
create table public.candidate_daily_sheet_projection_outbox (
  outbox_id uuid not null,
  environment text not null,
  target text not null,
  candidate_id uuid not null,
  availability_date date not null,
  availability_version bigint not null,
  operation text not null,
  preference text not null,
  command_id uuid not null,
  state text not null,
  delivery_attempt_count integer not null,
  deferral_count integer not null,
  next_available_at_utc timestamp with time zone not null,
  overlay_generation_id uuid,
  overlay_generation_version bigint,
  overlay_source_row_hash text,
  lease_owner text,
  lease_token text,
  lease_expires_at_utc timestamp with time zone,
  target_logical_id text,
  observed_sheet_revision text,
  safe_error_code text,
  alerted_at_utc timestamp with time zone,
  completed_at_utc timestamp with time zone,
  correlation_id text not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.candidate_job_titles
create table public.candidate_job_titles (
  id uuid not null,
  candidate_id uuid not null,
  job_title_id uuid not null,
  is_primary boolean not null,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null
);

-- public.candidate_notifications
create table public.candidate_notifications (
  id uuid not null,
  account_id uuid not null,
  candidate_id uuid,
  workflow_id uuid,
  timesheet_id uuid,
  event_type text not null,
  preference_category text not null,
  template_key text not null,
  template_params jsonb not null,
  deep_link_json jsonb not null,
  state text not null,
  push_state text not null,
  dedupe_key text not null,
  created_at_utc timestamp with time zone not null,
  read_at_utc timestamp with time zone,
  dismissed_at_utc timestamp with time zone,
  push_claimed_at_utc timestamp with time zone,
  push_sent_at_utc timestamp with time zone,
  push_failed_at_utc timestamp with time zone,
  last_error text
);

-- public.candidate_submission_components
create table public.candidate_submission_components (
  id uuid not null,
  workflow_id uuid not null,
  workflow_generation integer not null,
  component_no integer not null,
  timesheet_id uuid,
  component_kind text not null,
  expense_category text,
  document_role text not null,
  state text not null,
  source_component_id uuid,
  storage_key text,
  media_type text,
  byte_size bigint,
  source_content_sha256 bytea,
  upload_idempotency_key text,
  created_at_utc timestamp with time zone not null,
  immutable_at_utc timestamp with time zone,
  superseded_at_utc timestamp with time zone,
  manager_reviewed_at_utc timestamp with time zone,
  manager_approved_at_utc timestamp with time zone,
  approval_request_id uuid,
  required boolean not null,
  review_ordinal integer,
  review_storage_key text,
  review_content_sha256 bytea,
  review_media_type text,
  review_byte_size bigint,
  review_page_count integer,
  review_render_input_sha256 bytea,
  review_renderer_contract_version text,
  review_renderer_receipt_json jsonb,
  review_generated_at_utc timestamp with time zone,
  review_render_state text not null,
  final_signed_storage_key text,
  final_signed_content_sha256 bytea,
  final_signed_media_type text,
  final_signed_byte_size bigint,
  final_signed_page_count integer,
  final_signed_render_input_sha256 bytea,
  final_signed_renderer_contract_version text,
  final_signed_renderer_receipt_json jsonb,
  final_signed_generated_at_utc timestamp with time zone,
  final_signed_render_state text not null,
  paper_return_page_key text
);

-- public.candidate_submission_workflows
create table public.candidate_submission_workflows (
  id uuid not null,
  environment text not null,
  account_id uuid not null,
  candidate_id uuid not null,
  workflow_kind text not null,
  scope text not null,
  route text not null,
  state text not null,
  generation integer not null,
  contract_id uuid,
  contract_week_id uuid,
  anchor_timesheet_id uuid,
  target_timesheet_id uuid,
  work_date date,
  week_ending_date date,
  policy_snapshot_json jsonb not null,
  input_snapshot_json jsonb not null,
  issue_codes jsonb not null,
  expected_row_signature text,
  capability_hash text,
  rejection_reason text,
  rejection_scope text,
  idempotency_key text not null,
  last_mutation_idempotency_key text,
  last_mutation_response_json jsonb,
  worker_submitted_at_utc timestamp with time zone,
  finalised_at_utc timestamp with time zone,
  cancelled_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  immutable_submission_json jsonb,
  immutable_submission_sha256 bytea,
  policy_snapshot_sha256 bytea,
  candidate_signature_component_id uuid,
  candidate_signature_sha256 bytea,
  candidate_signed_at_utc timestamp with time zone,
  review_manifest_json jsonb,
  review_manifest_sha256 bytea,
  paper_return_manifest_json jsonb,
  paper_return_manifest_sha256 bytea,
  renderer_contract_version text,
  manager_name text,
  manager_position text,
  manager_signature_component_id uuid,
  manager_signature_sha256 bytea,
  manager_approved_at_utc timestamp with time zone,
  daily_context_sha256 bytea,
  canonical_financial_sha256 bytea,
  canonical_save_input_sha256 bytea,
  canonical_save_row_signature text,
  canonical_save_financials_id uuid,
  canonical_save_receipt_json jsonb,
  canonical_saved_at_utc timestamp with time zone,
  replacement_of_workflow_id uuid,
  creation_request_sha256 bytea,
  creation_identity_json jsonb
);

-- public.candidates
create table public.candidates (
  id uuid not null,
  tms_ref text,
  first_name text,
  last_name text,
  display_name text,
  email text,
  phone text,
  pay_method text not null,
  umbrella_id uuid,
  active boolean not null,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  key_norm text,
  mileage_pay_rate numeric,
  account_holder text,
  bank_name text,
  sort_code text,
  account_number text,
  roles jsonb,
  notes text,
  rev bigint,
  job_title_id uuid,
  prof_reg_number text,
  prof_reg_type professional_reg_type_enum,
  ni_number text,
  date_of_birth date,
  gender text,
  address_line1 text,
  address_line2 text,
  address_line3 text,
  town_city text,
  county text,
  postcode text,
  country text,
  nhsp_hr_name_aliases jsonb not null,
  __cloudtms_dropped_attnum_36 text,
  __cloudtms_dropped_attnum_37 text,
  title text,
  opt_in_email boolean not null,
  opt_in_sms boolean not null,
  opt_in_whatsapp boolean not null,
  band smallint,
  tms_ref_num integer generated always as ((NULLIF(regexp_replace(tms_ref, '[^0-9]'::text, ''::text, 'g'::text), ''::text))::integer) stored,
  bank_details_hash text,
  remittance_overrides_enabled boolean not null,
  remittance_receive_enabled boolean not null,
  remittances_detailed_breakdown boolean not null,
  remittance_receive_when_umbrella_paid boolean not null,
  min_take_home_wtd numeric(12,2) not null
);
alter table public.candidates drop column __cloudtms_dropped_attnum_36;
alter table public.candidates drop column __cloudtms_dropped_attnum_37;

-- public.candidates_tombstones
create table public.candidates_tombstones (
  id uuid not null,
  deleted_rev bigint not null,
  deleted_at timestamp with time zone not null
);

-- public.client_hospitals
create table public.client_hospitals (
  id uuid not null,
  client_id uuid not null,
  hospital_name_norm jsonb not null,
  ward_hint jsonb,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null
);

-- public.client_settings
create table public.client_settings (
  id uuid not null,
  client_id uuid not null,
  timezone_id text,
  day_start time without time zone,
  day_end time without time zone,
  night_start time without time zone,
  night_end time without time zone,
  bh_source text,
  bh_list jsonb,
  bh_feed_url text,
  vat_rate_pct numeric(5,2),
  holiday_pay_pct numeric(5,2),
  erni_pct numeric(5,2),
  apply_holiday_to text,
  apply_erni_to text,
  margin_includes jsonb,
  effective_from date,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  hr_validation_required boolean not null,
  ts_reference_required boolean not null,
  week_ending_weekday smallint,
  autoprocess_hr boolean not null,
  pay_reference_required boolean not null,
  invoice_reference_required boolean not null,
  default_submission_mode submission_mode_enum not null,
  sat_start time without time zone,
  sat_end time without time zone,
  sun_start time without time zone,
  sun_end time without time zone,
  is_nhsp boolean not null,
  self_bill_no_invoices_sent boolean not null,
  daily_calc_of_invoices boolean not null,
  no_timesheet_required boolean not null,
  group_nightsat_sunbh boolean not null,
  requires_hr boolean not null,
  hr_attach_to_invoice boolean not null,
  ts_attach_to_invoice boolean not null,
  bh_start time without time zone,
  bh_end time without time zone,
  auto_invoice_default boolean not null,
  send_manual_invoices_to_different_email boolean not null,
  manual_invoices_alt_email_address text,
  invoice_consolidation_mode invoice_consolidation_mode_enum not null,
  reference_number_required_to_issue_invoice boolean not null,
  opt_in_email boolean not null,
  opt_in_sms boolean not null,
  opt_in_whatsapp boolean not null,
  healthroster_import_auto_authorise boolean not null,
  nhsp_import_auto_authorise boolean not null,
  reversal_complete_financials_date correction_financials_date_basis_enum,
  reversal_replacement_financials_date correction_financials_date_basis_enum,
  candidate_electronic_auto_authorise boolean,
  candidate_expenses_require_separate_timesheet boolean not null,
  candidate_paper_submission_enabled boolean not null,
  candidate_expense_invoice_email text,
  candidate_manager_approval_policy_json jsonb not null,
  allow_daily_manager_authorise_on_phone boolean not null,
  allow_daily_manager_authorise_by_email boolean not null,
  timesheet_break_entry_mode timesheet_break_entry_mode_enum not null
);

-- public.clients
create table public.clients (
  id uuid not null,
  cli_ref text,
  name text not null,
  invoice_address text,
  primary_invoice_email text,
  ap_phone text,
  vat_chargeable boolean not null,
  payment_terms_days integer not null,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  mileage_charge_rate numeric,
  ts_queries_email text,
  rev bigint,
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
  notes text
);

-- public.clients_tombstones
create table public.clients_tombstones (
  id uuid not null,
  deleted_rev bigint not null,
  deleted_at timestamp with time zone not null
);

-- public.comms_outbox
create table public.comms_outbox (
  id uuid not null,
  channel text not null,
  status text not null,
  to_address text not null,
  message_text text not null,
  provider_key text not null,
  provider_message_id text,
  provider_payload_json jsonb not null,
  provider_response_json jsonb not null,
  last_error text,
  created_at_utc timestamp with time zone not null,
  sent_at timestamp with time zone,
  delivered_at timestamp with time zone,
  read_at timestamp with time zone,
  failed_at timestamp with time zone,
  created_by uuid,
  recipient_kind text,
  recipient_id uuid,
  context_kind text,
  context_id uuid,
  mailshot_run_id uuid,
  document_template_id uuid,
  scheduled_for_utc timestamp with time zone,
  next_attempt_at_utc timestamp with time zone,
  attempt_lease_token text,
  attempt_leased_at_utc timestamp with time zone,
  attempt_lease_expires_at_utc timestamp with time zone,
  deterministic_outbox_key text
);

-- public.contract_weeks
create table public.contract_weeks (
  id uuid not null,
  contract_id uuid not null,
  week_ending_date date not null,
  additional_seq integer not null,
  status contract_week_status_enum not null,
  submission_mode_snapshot submission_mode_enum not null,
  timesheet_id uuid,
  uploaded_pdf_r2_key text,
  day_entries_json jsonb,
  totals_json jsonb,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  planned_schedule_json jsonb,
  is_adjustment boolean not null,
  enforce_day_partition boolean not null,
  allowed_days_mask text,
  split_boundary_date date,
  worker_note text,
  split_group_key text
);

-- public.contracts
create table public.contracts (
  id uuid not null,
  candidate_id uuid,
  client_id uuid not null,
  role text,
  band text,
  display_site text,
  ward_hint text,
  start_date date not null,
  end_date date not null,
  pay_method_snapshot text not null,
  rates_json jsonb not null,
  std_hours_json jsonb,
  default_submission_mode submission_mode_enum,
  week_ending_weekday_snapshot smallint not null,
  auto_invoice boolean not null,
  require_reference_to_pay boolean not null,
  require_reference_to_invoice boolean not null,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  bucket_labels_json jsonb,
  std_schedule_json jsonb,
  mileage_pay_rate numeric(10,2),
  mileage_charge_rate numeric(10,2),
  additional_rates_json jsonb,
  self_bill boolean not null,
  weekly_timesheet_source weekly_timesheet_source_enum,
  no_timesheet_required boolean,
  daily_calc_of_invoices boolean,
  group_nightsat_sunbh boolean,
  is_nhsp boolean,
  autoprocess_hr boolean,
  requires_hr boolean,
  hr_attach_to_invoice boolean,
  ts_attach_to_invoice boolean,
  overrideclientsettings boolean not null,
  reference_number_required_to_issue_invoice boolean,
  send_manual_invoices_to_different_email boolean,
  manual_invoices_alt_email_address text,
  is_ad_hoc boolean not null,
  healthroster_import_auto_authorise_override boolean,
  nhsp_import_auto_authorise_override boolean,
  send_ts_queries_to_different_email boolean not null,
  ts_queries_alt_email_address text,
  candidate_electronic_auto_authorise_override boolean,
  candidate_expenses_require_separate_timesheet_override boolean,
  candidate_paper_submission_enabled_override boolean,
  candidate_expense_invoice_email_override text,
  candidate_manager_approval_policy_json jsonb not null,
  timesheet_break_entry_mode timesheet_break_entry_mode_enum
);

-- public.default_job_titles
create table public.default_job_titles (
  id uuid not null,
  label text not null,
  parent_id uuid,
  depth smallint not null,
  is_role boolean not null,
  requires_prof_reg boolean not null,
  prof_reg_type professional_reg_type_enum,
  active boolean not null,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null
);

-- public.document_templates
create table public.document_templates (
  id uuid not null,
  entity_type text not null,
  output_type text not null,
  filename text not null,
  description text,
  email_type text,
  selected_field_keys text[] not null,
  template_content_json jsonb not null,
  created_by uuid,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.hr_daily_grade_role_mappings
create table public.hr_daily_grade_role_mappings (
  id uuid not null,
  client_id uuid not null,
  incoming_grade_norm text not null,
  role_code text not null,
  band_norm text,
  active boolean not null,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null
);

-- public.hr_imports
create table public.hr_imports (
  id uuid not null,
  filename text not null,
  uploaded_by uuid,
  uploaded_at_utc timestamp with time zone not null,
  tz_assumption text not null,
  parse_summary_json jsonb,
  created_at timestamp with time zone not null,
  source_system hr_source_enum not null,
  file_r2_key text,
  client_id uuid,
  import_scope text,
  applied_at timestamp with time zone,
  pruned_at timestamp with time zone,
  source_file_sha256 text,
  parser_version text,
  revision_group_id uuid,
  revision_no integer,
  supersedes_import_id uuid,
  coverage_mode text,
  coverage_start_date date,
  coverage_end_date date,
  coverage_fingerprint text,
  coverage_locked_at timestamp with time zone,
  coverage_confirmed_by uuid,
  coverage_operation_key text,
  coverage_request_hash text
);

-- public.hr_issue_email_deliveries
create table public.hr_issue_email_deliveries (
  id uuid not null,
  import_id uuid not null,
  operation_id uuid not null,
  recipient_scope text not null,
  recipient_scope_key text not null,
  recipient_route_fingerprint text not null,
  recipient_email text not null,
  reminder_sequence integer not null,
  issue_set_fingerprint text not null,
  deterministic_outbox_key text not null,
  mail_outbox_id uuid,
  status text not null,
  provider_message_id text,
  provider_status text,
  accepted_at_utc timestamp with time zone,
  marked_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  created_by_user_id uuid,
  updated_at_utc timestamp with time zone not null
);

-- public.hr_issue_email_delivery_items
create table public.hr_issue_email_delivery_items (
  id uuid not null,
  delivery_id uuid not null,
  issue_id uuid not null,
  action_id text not null,
  issue_fingerprint text not null,
  marked_sent_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null
);

-- public.hr_issue_emails
create table public.hr_issue_emails (
  id uuid not null,
  source_system text not null,
  import_id uuid,
  client_id uuid,
  timesheet_id uuid,
  hr_row_id uuid,
  staff_norm text,
  hospital_norm text,
  work_date date,
  reason_code text not null,
  issue_fingerprint text not null,
  last_sent_at timestamp with time zone,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  contract_id uuid,
  recipient_scope text,
  recipient_scope_key text,
  sent_count integer not null,
  last_successful_delivery_id uuid,
  delivery_history_status text not null
);

-- public.hr_name_mappings
create table public.hr_name_mappings (
  id uuid not null,
  hr_name_norm text not null,
  hospital_or_trust text,
  candidate_id uuid not null,
  active boolean not null,
  created_by uuid,
  created_at timestamp with time zone not null,
  last_used_at timestamp with time zone,
  notes text
);

-- public.hr_results
create table public.hr_results (
  id uuid not null,
  row_id uuid not null,
  candidate_id uuid,
  timesheet_id uuid,
  booking_id text,
  status hr_result_status_enum not null,
  reason_code text,
  details_json jsonb,
  created_at_utc timestamp with time zone not null
);

-- public.hr_rows
create table public.hr_rows (
  id uuid not null,
  import_id uuid not null,
  hr_request_id text,
  date_local date not null,
  start_time_local time without time zone not null,
  end_time_local time without time zone not null,
  staff_norm text not null,
  role_type role_type_enum not null,
  unit_raw text,
  unit_hint text,
  agency_raw text,
  created_at timestamp with time zone not null,
  external_row_key text,
  payload_json jsonb,
  staff_raw text,
  assignment_grade_norm text,
  hours_worked numeric
);

-- public.id_consolidation_run_lines
create table public.id_consolidation_run_lines (
  id_ref text not null,
  invoice_id uuid not null,
  invoice_number text,
  invoice_status text,
  invoice_type text,
  delta_inc_vat numeric(12,2),
  current_inc_vat numeric(12,2),
  delta_ex_vat numeric(12,2),
  delta_vat numeric(12,2),
  current_ex_vat numeric(12,2),
  current_vat numeric(12,2)
);

-- public.id_consolidation_runs
create table public.id_consolidation_runs (
  id_ref text not null,
  created_at_utc timestamp with time zone not null,
  created_by_user_id uuid,
  total_delta_inc_vat numeric(12,2) not null,
  total_delta_ex_vat numeric(12,2) not null,
  total_delta_vat numeric(12,2) not null,
  bank_upload_code text,
  bank_uploaded_at_utc timestamp with time zone,
  note text,
  committed_by_user_id uuid
);

-- public.id_invoice_ledger
create table public.id_invoice_ledger (
  invoice_id uuid not null,
  invoice_number text,
  invoice_status text,
  invoice_type text,
  current_ex_vat numeric(12,2) not null,
  current_vat numeric(12,2) not null,
  current_inc_vat numeric(12,2) not null,
  last_reported_inc_vat numeric(12,2) not null,
  updated_at_utc timestamp with time zone not null,
  last_reported_ex_vat numeric(12,2) not null,
  last_reported_vat numeric(12,2) not null
);

-- public.import_apply_operations
create table public.import_apply_operations (
  id uuid not null,
  import_id uuid not null,
  source_system hr_source_enum not null,
  import_revision text not null,
  request_hash text not null,
  actor_user_id uuid not null,
  state text not null,
  response_json jsonb not null,
  committed_at_utc timestamp with time zone,
  financialised_at_utc timestamp with time zone,
  finalised_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.import_column_aliases
create table public.import_column_aliases (
  id uuid not null,
  system_type text not null,
  field_key text not null,
  alias_name text not null,
  active boolean not null,
  notes text,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null
);

-- public.import_review_action_outcomes
create table public.import_review_action_outcomes (
  action_id text not null,
  import_id uuid not null,
  operation_id uuid not null,
  action_kind text not null,
  source_identity text not null,
  candidate_id uuid not null,
  client_id uuid not null,
  contract_id uuid,
  hr_row_id uuid,
  timesheet_id uuid,
  shift_id uuid,
  evidence_fingerprint text not null,
  completed_label text not null,
  summary_json jsonb not null,
  applied_at_utc timestamp with time zone not null,
  applied_by_user_id uuid not null
);

-- public.import_review_daily_timesheet_resolutions
create table public.import_review_daily_timesheet_resolutions (
  id uuid not null,
  import_id uuid not null,
  hr_row_id uuid not null,
  resolved_timesheet_id uuid,
  resolution_method text not null,
  status text not null,
  evidence_fingerprint text not null,
  preview_generation integer not null,
  state_version bigint not null,
  selected_at_utc timestamp with time zone not null,
  selected_by_user_id uuid,
  stale_at_utc timestamp with time zone,
  stale_reason_code text,
  applied_operation_id uuid,
  applied_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.import_review_decisions
create table public.import_review_decisions (
  action_id text not null,
  import_id uuid not null,
  action_kind text not null,
  action_category text not null,
  target_key text not null,
  source_identity text not null,
  hr_row_id uuid,
  timesheet_id uuid,
  shift_id uuid,
  client_id uuid,
  candidate_id uuid,
  contract_id uuid,
  issue_id uuid,
  preview_generation integer not null,
  evidence_fingerprint text not null,
  selectable boolean not null,
  default_selected boolean not null,
  selected boolean not null,
  blocking boolean not null,
  requires_reconfirmation boolean not null,
  is_current boolean not null,
  summary_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  refreshed_at_utc timestamp with time zone not null,
  selected_at_utc timestamp with time zone,
  selected_by_user_id uuid
);

-- public.import_review_events
create table public.import_review_events (
  id bigint generated always as identity not null,
  import_id uuid not null,
  state_version bigint not null,
  operation_id uuid,
  event_code text not null,
  actor_user_id uuid,
  event_context_json jsonb not null,
  created_at_utc timestamp with time zone not null
);

-- public.import_review_scope_candidates
create table public.import_review_scope_candidates (
  id uuid not null,
  import_id uuid not null,
  source_candidate_key text not null,
  source_display_label text,
  candidate_id uuid,
  created_at_utc timestamp with time zone not null,
  created_by_user_id uuid,
  resolved_at_utc timestamp with time zone,
  resolved_by_user_id uuid
);

-- public.import_review_scope_clients
create table public.import_review_scope_clients (
  id uuid not null,
  import_id uuid not null,
  source_client_key text not null,
  source_display_label text,
  client_id uuid,
  created_at_utc timestamp with time zone not null,
  created_by_user_id uuid,
  resolved_at_utc timestamp with time zone,
  resolved_by_user_id uuid
);

-- public.import_review_states
create table public.import_review_states (
  import_id uuid not null,
  schema_contract_version text not null,
  status text not null,
  state_version bigint not null,
  preview_generation integer not null,
  preview_fingerprint text,
  ui_state_json jsonb not null,
  follow_up_status text not null,
  follow_up_error_code text,
  follow_up_error_message text,
  follow_up_retry_count integer not null,
  last_operation_id uuid,
  created_at_utc timestamp with time zone not null,
  created_by_user_id uuid,
  updated_at_utc timestamp with time zone not null,
  updated_by_user_id uuid,
  last_opened_at_utc timestamp with time zone,
  last_opened_by_user_id uuid,
  abandoned_at_utc timestamp with time zone,
  abandoned_by_user_id uuid,
  abandoned_reason text,
  applied_at_utc timestamp with time zone,
  applied_by_user_id uuid,
  superseded_at_utc timestamp with time zone,
  superseded_by_user_id uuid
);

-- public.import_review_weekly_validation_resolutions
create table public.import_review_weekly_validation_resolutions (
  id uuid not null,
  import_id uuid not null,
  hr_row_id uuid not null,
  timesheet_id uuid not null,
  resolution_code text not null,
  status text not null,
  evidence_fingerprint text not null,
  preview_generation integer not null,
  state_version bigint not null,
  selected_at_utc timestamp with time zone not null,
  selected_by_user_id uuid,
  stale_at_utc timestamp with time zone,
  stale_reason_code text,
  applied_operation_id uuid,
  applied_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.invoice_document_assets
create table public.invoice_document_assets (
  id uuid not null,
  source_kind text not null,
  source_id uuid not null,
  source_revision text not null,
  original_r2_key text not null,
  original_filename text,
  declared_media_type text,
  detected_media_type text,
  original_sha256 text,
  original_size_bytes bigint,
  width_pixels integer,
  height_pixels integer,
  orientation_degrees integer,
  source_page_count integer,
  is_encrypted boolean,
  status text not null,
  normalised_manifest_json jsonb not null,
  normalised_manifest_hash text,
  normalised_r2_key text,
  normalised_sha256 text,
  normalised_size_bytes bigint,
  normalised_page_count integer,
  operation_id uuid,
  error_json jsonb,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  ready_at_utc timestamp with time zone
);

-- public.invoice_document_versions
create table public.invoice_document_versions (
  id uuid not null,
  entity_type text not null,
  entity_id uuid not null,
  purpose text not null,
  operation_id uuid not null,
  source_revision text not null,
  template_version text not null,
  status text not null,
  snapshot_json jsonb not null,
  snapshot_hash text not null,
  manifest_json jsonb not null,
  manifest_hash text not null,
  r2_key text,
  sha256 text,
  size_bytes bigint,
  expected_page_count integer,
  page_count integer,
  core_page_count integer,
  supporting_page_count integer,
  created_at_utc timestamp with time zone not null,
  ready_at_utc timestamp with time zone,
  verified_at_utc timestamp with time zone,
  superseded_at_utc timestamp with time zone,
  error_json jsonb
);

-- public.invoice_hr_source_rows
create table public.invoice_hr_source_rows (
  invoice_id uuid not null,
  source_system text not null,
  import_id uuid not null,
  header_columns jsonb,
  rows_json jsonb,
  header_rows jsonb
);

-- public.invoice_jobs_outbox
create table public.invoice_jobs_outbox (
  id uuid not null,
  kind text not null,
  payload jsonb not null,
  attempt_count integer not null,
  next_attempt_at timestamp with time zone,
  last_error text,
  created_at timestamp with time zone not null
);

-- public.invoice_lines
create table public.invoice_lines (
  id uuid not null,
  invoice_id uuid not null,
  timesheet_id uuid,
  booking_id text,
  description text,
  hours_day numeric(7,2) not null,
  hours_night numeric(7,2) not null,
  hours_sat numeric(7,2) not null,
  hours_sun numeric(7,2) not null,
  hours_bh numeric(7,2) not null,
  pay_day numeric(10,2),
  pay_night numeric(10,2),
  pay_sat numeric(10,2),
  pay_sun numeric(10,2),
  pay_bh numeric(10,2),
  charge_day numeric(10,2),
  charge_night numeric(10,2),
  charge_sat numeric(10,2),
  charge_sun numeric(10,2),
  charge_bh numeric(10,2),
  total_pay_ex_vat numeric(12,2) not null,
  total_charge_ex_vat numeric(12,2) not null,
  margin_ex_vat numeric(12,2) not null,
  vat_rate_pct numeric(5,2) not null,
  vat_amount numeric(12,2) not null,
  total_inc_vat numeric(12,2) not null,
  created_at timestamp with time zone not null,
  paper_ts_r2_key text,
  meta_json jsonb not null,
  source_key text
);

-- public.invoice_operation_chunks
create table public.invoice_operation_chunks (
  id uuid not null,
  operation_id uuid not null,
  chunk_type text not null,
  phase text not null,
  sequence_no integer not null,
  level_no integer not null,
  work_key text not null,
  plan_generation integer not null,
  replaced_by_chunk_id uuid,
  replacement_required boolean not null,
  entity_type text,
  entity_id uuid,
  document_version_id uuid,
  document_asset_id uuid,
  input_document_version_id uuid,
  status text not null,
  priority integer not null,
  run_after_utc timestamp with time zone not null,
  payload_json jsonb not null,
  progress_json jsonb not null,
  result_json jsonb,
  error_json jsonb,
  expected_page_count integer,
  actual_page_count integer,
  expected_byte_count bigint,
  actual_byte_count bigint,
  attempt_count integer not null,
  max_attempts integer not null,
  lease_owner text,
  lease_token uuid,
  lease_expires_at_utc timestamp with time zone,
  fence_token bigint not null,
  operation_control_version bigint not null,
  created_at_utc timestamp with time zone not null,
  started_at_utc timestamp with time zone,
  updated_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone,
  failed_at_utc timestamp with time zone,
  manifest_generation integer not null,
  is_manifest_member boolean not null,
  manifest_committed boolean not null,
  result_visible boolean not null,
  selection_key text generated always as (NULLIF((payload_json ->> 'selection_key'::text), ''::text)) stored,
  result_category text generated always as (NULLIF(COALESCE((result_json ->> 'result_category'::text), (payload_json ->> 'result_category'::text)), ''::text)) stored
);

-- public.invoice_operations
create table public.invoice_operations (
  id uuid not null,
  parent_operation_id uuid,
  operation_type text not null,
  entity_type text,
  entity_id uuid,
  actor_user_id uuid,
  idempotency_key text not null,
  status text not null,
  phase text not null,
  priority integer not null,
  source_revision text,
  template_version text,
  input_json jsonb not null,
  config_json jsonb not null,
  progress_json jsonb not null,
  result_json jsonb,
  error_json jsonb,
  total_units integer not null,
  completed_units integer not null,
  failed_units integer not null,
  chunk_count integer not null,
  control_version bigint not null,
  change_seq bigint not null,
  requires_user_action boolean not null,
  created_at_utc timestamp with time zone not null,
  started_at_utc timestamp with time zone,
  updated_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone,
  failed_at_utc timestamp with time zone,
  manifest_generation integer not null,
  manifest_committed boolean not null,
  release_complete boolean not null,
  result_page_revision bigint not null
);

-- public.invoice_pdf_outbox
create table public.invoice_pdf_outbox (
  id uuid not null,
  invoice_id uuid not null,
  reason invoice_pdf_reason_enum not null,
  attempt_count integer not null,
  next_attempt_at timestamp with time zone,
  last_error text,
  force_regen boolean not null,
  created_at timestamp with time zone not null
);

-- public.invoices
create table public.invoices (
  id uuid not null,
  type invoice_type_enum not null,
  invoice_no text,
  client_id uuid not null,
  issued_at_utc timestamp with time zone,
  due_at_utc timestamp with time zone,
  paid_at_utc timestamp with time zone,
  status invoice_status_enum not null,
  status_date_utc timestamp with time zone not null,
  subtotal_ex_vat numeric(12,2) not null,
  vat_amount numeric(12,2) not null,
  total_inc_vat numeric(12,2) not null,
  original_invoice_id uuid,
  notes text,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  invoice_pdf_r2_key text,
  paper_ts_r2_manifest jsonb,
  header_snapshot_json jsonb not null,
  on_hold_reason text,
  invoice_render_manifest jsonb,
  invoice_pdf_generated_at_utc timestamp with time zone,
  do_not_send boolean not null,
  credit_note_created_at_utc timestamp with time zone,
  document_revision bigint not null,
  document_state text not null,
  preview_document_version_id uuid,
  issued_document_version_id uuid,
  active_document_operation_id uuid,
  issue_state text not null,
  active_issue_operation_id uuid,
  last_document_error_json jsonb
);

-- public.legacy_contract_rate_lines
create table public.legacy_contract_rate_lines (
  id uuid not null,
  legacy_contract_id uuid not null,
  line_no smallint not null,
  label text not null,
  pay_rate numeric(12,2),
  margin numeric(12,2),
  charge_rate numeric(12,2),
  created_at timestamp with time zone not null
);

-- public.legacy_contracts
create table public.legacy_contracts (
  id uuid not null,
  source_system text not null,
  source_row_hash text not null,
  legacy_client_ref text not null,
  legacy_candidate_ref text not null,
  client_id uuid not null,
  candidate_id uuid not null,
  start_date date not null,
  end_date date,
  job_title text,
  pay_method text not null,
  raw_payment_method text,
  created_at timestamp with time zone not null
);

-- public.legacy_eclipse_candidate_map
create table public.legacy_eclipse_candidate_map (
  legacy_candidate_ref text not null,
  candidate_id uuid not null,
  imported_at timestamp with time zone not null
);

-- public.legacy_eclipse_client_map
create table public.legacy_eclipse_client_map (
  legacy_client_ref text not null,
  client_id uuid not null,
  imported_at timestamp with time zone not null
);

-- public.mail_outbox
create table public.mail_outbox (
  id uuid not null,
  type text not null,
  "to" text not null,
  cc text,
  subject text not null,
  body_html text,
  body_text text,
  attachments jsonb,
  status mail_status_enum not null,
  last_error text,
  created_at_utc timestamp with time zone not null,
  sent_at timestamp with time zone,
  created_by uuid,
  reference text,
  provider_message_id text,
  failed_at timestamp with time zone,
  recipient_kind text,
  recipient_id uuid,
  context_kind text,
  context_id uuid,
  mailshot_run_id uuid,
  document_template_id uuid,
  provider_status text,
  delivered_at timestamp with time zone,
  read_at timestamp with time zone,
  bcc text,
  reply_to text,
  importance text,
  email_type text,
  scheduled_for_utc timestamp with time zone,
  next_attempt_at_utc timestamp with time zone,
  attempt_lease_token text,
  attempt_leased_at_utc timestamp with time zone,
  attempt_lease_expires_at_utc timestamp with time zone,
  payment_scope_json jsonb not null,
  deterministic_outbox_key text,
  attachments_ready boolean not null,
  waiting_invoice_operation_id uuid,
  attachment_total_bytes bigint,
  attachment_delivery_policy text
);

-- public.mailshot_field_overrides
create table public.mailshot_field_overrides (
  id uuid not null,
  field_id uuid not null,
  entity_type text not null,
  label_override text,
  enabled_local boolean not null
);

-- public.mailshot_fields
create table public.mailshot_fields (
  id uuid not null,
  field_key text not null,
  label_default text not null,
  enabled_global boolean not null,
  allowed_entity_types text[] not null,
  resolver_spec_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.mailshot_runs
create table public.mailshot_runs (
  id uuid not null,
  context_kind text not null,
  output_type text not null,
  document_template_id uuid,
  created_by uuid,
  created_at_utc timestamp with time zone not null,
  selection_json jsonb not null,
  result_json jsonb not null,
  delivery_timing_json jsonb not null
);

-- public.manual_timesheet_queue
create table public.manual_timesheet_queue (
  id uuid not null,
  r2_key text not null,
  original_filename text not null,
  mime_type text,
  content_hash text not null,
  uploaded_by_user_id uuid not null,
  uploaded_at_utc timestamp with time zone not null,
  status text not null,
  timesheet_id uuid,
  last_rotation_deg smallint not null,
  meta_json jsonb not null
);

-- public.migration_smoke_once_only
create table public.migration_smoke_once_only (
  id bigint not null,
  created_at timestamp with time zone not null
);

-- public.nhsp_shifts
create table public.nhsp_shifts (
  id uuid not null,
  external_row_key text not null,
  latest_import_id uuid,
  candidate_id uuid,
  client_id uuid,
  contract_id uuid,
  timesheet_id uuid,
  work_date date not null,
  ward text,
  start_utc timestamp with time zone not null,
  end_utc timestamp with time zone not null,
  break_mins integer not null,
  pay_minutes integer not null,
  pay_amount_snapshot numeric(12,2),
  charge_amount_snapshot numeric(12,2),
  invoice_status text not null,
  defer_until_run_after timestamp with time zone,
  invoice_id uuid,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  source_system hr_source_enum not null,
  __cloudtms_dropped_attnum_22 text,
  __cloudtms_dropped_attnum_23 text,
  hr_request_id text,
  held_back_reason text,
  staff_name text,
  staff_norm text,
  ward_norm text,
  assignment_code text,
  ref_num text,
  week_ending_date date,
  cancelled_at_utc timestamp with time zone,
  cancelled_by_import_id uuid,
  cancelled_reason text
);
alter table public.nhsp_shifts drop column __cloudtms_dropped_attnum_22;
alter table public.nhsp_shifts drop column __cloudtms_dropped_attnum_23;

-- public.pay_advance_patches
create table public.pay_advance_patches (
  id uuid not null,
  advance_id uuid not null,
  pay_batch_id uuid not null,
  old_outstanding_amount numeric(12,2),
  new_outstanding_amount numeric(12,2),
  old_schedule_json jsonb,
  new_schedule_json jsonb,
  old_next_due_week_start date,
  new_next_due_week_start date,
  patched_at_utc timestamp with time zone not null
);

-- public.pay_advance_reservations
create table public.pay_advance_reservations (
  id uuid not null,
  finance_case_id uuid not null,
  pay_batch_id uuid not null,
  pay_batch_candidate_id uuid,
  pay_batch_item_id uuid,
  reserved_amount numeric not null,
  repayment_week_start date,
  status text not null,
  created_at_utc timestamp with time zone not null,
  committed_at_utc timestamp with time zone,
  settled_at_utc timestamp with time zone,
  released_at_utc timestamp with time zone,
  released_reason text,
  created_by_user_id uuid,
  updated_by_user_id uuid,
  finance_component_id uuid,
  frozen_component_snapshot_json jsonb,
  frozen_component_key_type text,
  frozen_component_key_value text,
  frozen_component_classification pay_finance_component_classification_enum,
  frozen_source_basis_json jsonb,
  frozen_source_pay_method text,
  frozen_target_pay_method text,
  frozen_resolution_mode pay_finance_component_resolution_mode_enum,
  frozen_resolution_payload_json jsonb,
  frozen_resolution_result_json jsonb,
  reserved_source_amount numeric(12,2),
  frozen_rounded_target_amount numeric(12,2)
);

-- public.pay_advances
create table public.pay_advances (
  id uuid not null,
  candidate_id uuid not null,
  client_id uuid,
  reason pay_advance_reason_enum not null,
  original_amount numeric(12,2) not null,
  outstanding_amount numeric(12,2) not null,
  linked_shift_date date,
  schedule_json jsonb not null,
  next_due_week_start date,
  status pay_advance_status_enum not null,
  best_guess_hours jsonb,
  notes text,
  created_at timestamp with time zone not null,
  created_by uuid,
  updated_at timestamp with time zone not null,
  advance_kind pay_advance_kind_enum,
  linked_timesheet_id uuid,
  baseline_signature text,
  payout_status pay_advance_payout_status_enum,
  payout_pay_batch_id uuid,
  payout_transfer_id uuid,
  weekly_due numeric(12,2),
  weeks_total integer,
  start_week_start date,
  case_type pay_finance_case_type_enum not null,
  adjustment_comment text,
  source_original_paid_amount numeric,
  source_corrected_paid_amount numeric,
  minimum_earnings_threshold numeric,
  take_home_floor_override numeric,
  written_off_at_utc timestamp with time zone,
  written_off_by_user_id uuid,
  write_off_reason text,
  cleared_at_utc timestamp with time zone,
  cleared_by_user_id uuid,
  taxability pay_finance_taxability_enum,
  routing_kind pay_finance_routing_kind_enum,
  oneoff_bank_details_required boolean not null
);

-- public.pay_bank_transfer_events
create table public.pay_bank_transfer_events (
  id uuid not null,
  pay_batch_id uuid not null,
  pay_bank_transfer_id uuid,
  candidate_id uuid,
  umbrella_id uuid,
  provider_key text,
  provider_event_id text,
  provider_reference text,
  provider_state text,
  normalised_state text not null,
  event_source text not null,
  event_time_utc timestamp with time zone,
  received_at_utc timestamp with time zone not null,
  amount numeric,
  currency text not null,
  mapping_status text not null,
  movement_classification text,
  correction_disposition text,
  raw_payload jsonb not null,
  idempotency_key text not null,
  created_at_utc timestamp with time zone not null,
  mapping_method text,
  provider_webhook_receipt_id uuid,
  provider_event_type text,
  provider_transaction_id text,
  provider_request_id text,
  provider_event_key text,
  provider_signature_valid boolean,
  provider_event_transport text,
  adapter_key text,
  adapter_version text,
  rail_env text,
  provider_failure_reason_code text,
  provider_failure_reason_group text,
  mapping_hints_json jsonb not null
);

-- public.pay_bank_transfers
create table public.pay_bank_transfers (
  id uuid not null,
  pay_batch_id uuid not null,
  candidate_id uuid,
  umbrella_id uuid,
  pay_channel text not null,
  amount numeric(12,2) not null,
  currency text not null,
  status text not null,
  __cloudtms_dropped_attnum_9 text,
  __cloudtms_dropped_attnum_10 text,
  payment_reference text,
  payee_name text,
  sort_code text,
  account_number text,
  account_type text,
  __cloudtms_dropped_attnum_16 text,
  __cloudtms_dropped_attnum_17 text,
  created_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone,
  failed_reason text,
  rail_provider text not null,
  rail_env text not null,
  request_id text,
  rail_tx_id text,
  rail_state text,
  rail_meta_json jsonb,
  bank_details_hash_snapshot text,
  payee_entity_kind text not null,
  payee_entity_id uuid,
  transfer_group_key text,
  grouping_mode_used text,
  week_ending_bucket date
);
alter table public.pay_bank_transfers drop column __cloudtms_dropped_attnum_9;
alter table public.pay_bank_transfers drop column __cloudtms_dropped_attnum_10;
alter table public.pay_bank_transfers drop column __cloudtms_dropped_attnum_16;
alter table public.pay_bank_transfers drop column __cloudtms_dropped_attnum_17;

-- public.pay_batch_auth_actions
create table public.pay_batch_auth_actions (
  id uuid not null,
  auth_request_id uuid not null,
  pay_batch_id uuid not null,
  actor_user_id uuid not null,
  action text not null,
  action_at_utc timestamp with time zone not null,
  note text
);

-- public.pay_batch_auth_requests
create table public.pay_batch_auth_requests (
  id uuid not null,
  pay_batch_id uuid not null,
  requested_by_user_id uuid not null,
  required_quantity integer not null,
  schedule_kind text not null,
  scheduled_at_utc timestamp with time zone not null,
  funding_account_ref text not null,
  funds_warning_hours_json jsonb,
  state text not null,
  golden_key_used boolean not null,
  golden_key_user_id uuid,
  created_at_utc timestamp with time zone not null,
  finalised_at_utc timestamp with time zone,
  finalised_by_user_id uuid,
  execution_intent_json jsonb
);

-- public.pay_batch_auth_tokens
create table public.pay_batch_auth_tokens (
  token text not null,
  auth_request_id uuid not null,
  target_user_id uuid not null,
  expires_at_utc timestamp with time zone not null,
  used_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null
);

-- public.pay_batch_candidates
create table public.pay_batch_candidates (
  id uuid not null,
  pay_batch_id uuid not null,
  candidate_id uuid not null,
  candidate_tms_ref text,
  candidate_display_name text,
  paye_state text,
  mismatch_settlement_choice text,
  gross_preview numeric(12,2),
  net_bank_amount numeric(12,2),
  debt_created numeric(12,2),
  loan_repayment_taken numeric(12,2),
  settlement_status text,
  settled_at_utc timestamp with time zone,
  settled_via text,
  settled_note text,
  overpayment_recovery_taken numeric(12,2) not null,
  awaiting_net_amount boolean not null,
  updated_at timestamp with time zone not null,
  remittance_sent_at_utc timestamp with time zone,
  remittance_sent_by_user_id uuid,
  remittance_trigger_status text,
  last_remittance_error text
);

-- public.pay_batch_display_summary
create table public.pay_batch_display_summary (
  id uuid not null,
  pay_batch_id uuid not null,
  batch_status text,
  batch_kind text,
  candidate_count integer not null,
  item_count integer not null,
  transfer_count integer not null,
  total_payable numeric(14,2) not null,
  total_bank_out numeric(14,2) not null,
  execution_commit_state text,
  rail_provider_label text,
  rail_env_label text,
  issue_summary_counts jsonb not null,
  latest_operation_id uuid,
  latest_operation_status text,
  latest_operation_phase text,
  display_label text,
  stale_summary_json jsonb not null,
  freshness_validation_status text,
  freshness_checked_at_utc timestamp with time zone,
  freshness_result_hash text,
  summary_version bigint not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.pay_batch_item_breakdowns
create table public.pay_batch_item_breakdowns (
  id uuid not null,
  pay_batch_item_id uuid not null,
  line_kind text not null,
  bucket_code text,
  unit_name text,
  units numeric,
  rate numeric,
  amount_ex_vat numeric not null,
  amount_vat numeric not null,
  amount_inc_vat numeric not null,
  meta_json jsonb not null,
  created_at_utc timestamp with time zone not null,
  operation_source_key text
);

-- public.pay_batch_items
create table public.pay_batch_items (
  id uuid not null,
  pay_batch_candidate_id uuid not null,
  item_type text not null,
  timesheet_id uuid,
  segment_key text,
  source_ref text,
  description text,
  amount_ex_vat numeric(12,2),
  amount_vat numeric(12,2),
  amount_inc_vat numeric(12,2),
  pay_channel text not null,
  umbrella_id uuid,
  bank_reference text,
  __cloudtms_dropped_attnum_14 text,
  __cloudtms_dropped_attnum_15 text,
  pay_bank_transfer_id uuid,
  repayment_week_start date,
  is_voided boolean not null,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  is_mismatch boolean not null,
  finance_case_id uuid,
  reservation_id uuid,
  paye_treatment text,
  finance_component_id uuid,
  frozen_component_snapshot_json jsonb,
  frozen_component_key_type text,
  frozen_component_key_value text,
  frozen_component_classification pay_finance_component_classification_enum,
  frozen_source_basis_json jsonb,
  frozen_source_pay_method text,
  frozen_target_pay_method text,
  frozen_resolution_mode pay_finance_component_resolution_mode_enum,
  frozen_resolution_payload_json jsonb,
  frozen_resolution_result_json jsonb,
  frozen_source_amount numeric(12,2),
  frozen_target_amount_ex_vat numeric(12,2),
  frozen_target_amount_vat numeric(12,2),
  frozen_target_amount_inc_vat numeric(12,2),
  payout_instruction_snapshot_json jsonb,
  operation_source_key text
);
alter table public.pay_batch_items drop column __cloudtms_dropped_attnum_14;
alter table public.pay_batch_items drop column __cloudtms_dropped_attnum_15;

-- public.pay_batch_paye_net_inputs
create table public.pay_batch_paye_net_inputs (
  id uuid not null,
  pay_batch_candidate_id uuid not null,
  source text not null,
  net_amount numeric(12,2) not null,
  imported_at_utc timestamp with time zone not null,
  file_name text,
  file_hash text
);

-- public.pay_batch_timesheet_snapshots
create table public.pay_batch_timesheet_snapshots (
  id uuid not null,
  pay_batch_id uuid not null,
  timesheet_id uuid not null,
  candidate_id uuid not null,
  pay_channel text not null,
  base_snapshot_json jsonb not null,
  target_snapshot_json jsonb not null,
  signature text not null,
  created_at_utc timestamp with time zone not null
);

-- public.pay_batches
create table public.pay_batches (
  id uuid not null,
  pay_date date not null,
  created_at_utc timestamp with time zone not null,
  created_by_user_id uuid,
  status text not null,
  banking_system_snapshot text not null,
  external_paye_system_snapshot text not null,
  monzo_confirmed_at_utc timestamp with time zone,
  monzo_confirmed_by_user_id uuid,
  __cloudtms_dropped_attnum_10 text,
  last_status_checked_at_utc timestamp with time zone,
  total_bank_out numeric(12,2),
  total_debt_created numeric(12,2),
  bulk_ref_num integer,
  bulk_ref_date date,
  bulk_reference text,
  rail_provider_snapshot text not null,
  rail_env_snapshot text not null,
  schedule_kind text,
  scheduled_at_utc timestamp with time zone,
  scheduled_by_user_id uuid,
  executing_started_at_utc timestamp with time zone,
  completed_at_utc timestamp with time zone,
  cancelled_at_utc timestamp with time zone,
  cancelled_by_user_id uuid,
  cancel_reason text,
  funding_account_ref text,
  funds_warning_hours_json jsonb,
  last_funds_check_at_utc timestamp with time zone,
  last_funds_check_json jsonb,
  funds_warning_sent_json jsonb,
  batch_kind_fixed text,
  override_mode pay_override_mode_enum,
  override_reason text,
  force_include_timesheet_ids uuid[],
  authoritative_payment_date date,
  authoritative_payment_date_source text,
  same_week_paye_override_used boolean not null,
  same_week_paye_override_reason text,
  same_week_paye_override_verified_at_utc timestamp with time zone,
  same_week_paye_override_verified_by_user_id uuid,
  source_workbench_session_id uuid,
  source_snapshot_run_id uuid,
  source_session_version bigint,
  execution_commit_state text not null,
  execution_commit_ref text,
  execution_committed_at_utc timestamp with time zone,
  bank_csv_export_json jsonb,
  execution_intent_json jsonb,
  settlement_confirmation_json jsonb,
  freshness_operation_id uuid,
  freshness_validation_status text,
  freshness_checked_at_utc timestamp with time zone,
  freshness_result_hash text,
  freshness_scope_hash text,
  freshness_result_json jsonb,
  completion_notice_queued_at_utc timestamp with time zone,
  completion_notice_reference text,
  completion_notice_last_attempt_at_utc timestamp with time zone,
  completion_notice_next_attempt_at_utc timestamp with time zone,
  completion_notice_last_result_json jsonb,
  source_scope_change_generation bigint,
  scope_generation_observed_at_shell bigint
);
alter table public.pay_batches drop column __cloudtms_dropped_attnum_10;

-- public.pay_finance_case_components
create table public.pay_finance_case_components (
  id uuid not null,
  finance_case_id uuid not null,
  candidate_id uuid not null,
  client_id uuid,
  linked_timesheet_id uuid,
  source_family_key text not null,
  component_key_type text not null,
  component_key_value text not null,
  classification pay_finance_component_classification_enum not null,
  source_pay_method text not null,
  source_basis_json jsonb not null,
  source_amount numeric(12,2) not null,
  remaining_source_amount numeric(12,2) not null,
  saved_target_pay_method text,
  saved_resolution_mode pay_finance_component_resolution_mode_enum,
  saved_resolution_payload_json jsonb,
  saved_resolution_result_json jsonb,
  resolution_fingerprint text,
  is_resolution_stale boolean not null,
  stale_reason text,
  allocation_priority_group integer not null,
  allocation_priority_order integer not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  resolved_at_utc timestamp with time zone,
  closed_at_utc timestamp with time zone
);

-- public.pay_finance_case_events
create table public.pay_finance_case_events (
  id uuid not null,
  finance_case_id uuid not null,
  event_type text not null,
  event_at_utc timestamp with time zone not null,
  actor_user_id uuid,
  pay_batch_id uuid,
  reservation_id uuid,
  before_json jsonb,
  after_json jsonb,
  reason text,
  note text,
  finance_component_id uuid
);

-- public.pay_finance_case_oneoff_payout_bank_details
create table public.pay_finance_case_oneoff_payout_bank_details (
  finance_case_id uuid not null,
  candidate_id uuid not null,
  beneficiary_name text not null,
  sort_code text not null,
  account_number text not null,
  bank_details_hash text not null,
  note text,
  created_at_utc timestamp with time zone not null,
  created_by_user_id uuid,
  updated_at_utc timestamp with time zone not null,
  updated_by_user_id uuid
);

-- public.pay_item_snoozes
create table public.pay_item_snoozes (
  id uuid not null,
  candidate_id uuid not null,
  timesheet_id uuid,
  segment_id text,
  source_ref text,
  snooze_kind text not null,
  snooze_until_date date,
  created_at_utc timestamp with time zone not null,
  created_by_user_id uuid,
  note text,
  cleared_at_utc timestamp with time zone,
  cleared_by_user_id uuid,
  updated_at_utc timestamp with time zone,
  updated_by_user_id uuid,
  booking_id text,
  segment_stable_key text,
  schedule_before_snooze_json jsonb,
  next_due_week_start_before_snooze date,
  schedule_after_snooze_json jsonb,
  next_due_week_start_after_snooze date,
  cancelled_at_utc timestamp with time zone,
  cancelled_by_user_id uuid,
  cancel_reason text,
  natural_expiry_source_fingerprint text,
  natural_expiry_checked_fingerprint text,
  natural_expiry_checked_at_utc timestamp with time zone,
  natural_expiry_state_changed boolean,
  natural_expiry_result_code text
);

-- public.pay_manual_adjustment_carry_forwards
create table public.pay_manual_adjustment_carry_forwards (
  id uuid not null,
  source_pay_batch_id uuid not null,
  source_pay_batch_item_id uuid not null,
  source_pay_bank_transfer_id uuid,
  source_pay_batch_candidate_id uuid,
  source_correction_request_id uuid,
  source_correction_work_item_id uuid,
  candidate_id uuid not null,
  umbrella_id uuid,
  client_id uuid,
  timesheet_id uuid,
  pay_channel text not null,
  adjustment_direction text not null,
  amount_ex_vat numeric(12,2),
  amount_vat numeric(12,2),
  amount_inc_vat numeric(12,2) not null,
  amount_basis text,
  paye_treatment text,
  tax_treatment_json jsonb not null,
  description text not null,
  reason text,
  source_ref text,
  source_operation_source_key text,
  source_snapshot_json jsonb not null,
  status text not null,
  target_pay_batch_id uuid,
  target_pay_batch_item_id uuid,
  target_operation_source_key text,
  created_by_user_id uuid,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  reserved_at_utc timestamp with time zone,
  consumed_at_utc timestamp with time zone,
  released_at_utc timestamp with time zone,
  cancelled_at_utc timestamp with time zone,
  status_reason text
);

-- public.pay_payment_correction_actions
create table public.pay_payment_correction_actions (
  id uuid not null,
  correction_request_id uuid not null,
  pay_batch_id uuid not null,
  actor_kind text not null,
  actor_user_id uuid,
  action text not null,
  action_at_utc timestamp with time zone not null,
  note text,
  before_json jsonb,
  after_json jsonb,
  metadata_json jsonb not null
);

-- public.pay_payment_correction_items
create table public.pay_payment_correction_items (
  id uuid not null,
  correction_request_id uuid not null,
  pay_batch_id uuid not null,
  pay_batch_candidate_id uuid,
  candidate_id uuid,
  pay_batch_item_id uuid,
  pay_bank_transfer_id uuid,
  timesheet_id uuid,
  finance_case_id uuid,
  finance_component_id uuid,
  reservation_id uuid,
  item_type text,
  correction_item_kind text not null,
  source_amount numeric,
  amount_ex_vat numeric,
  amount_vat numeric,
  amount_inc_vat numeric,
  economic_key_type text,
  economic_key_value text,
  before_snapshot_json jsonb,
  after_snapshot_json jsonb,
  status text not null,
  created_at_utc timestamp with time zone not null,
  applied_at_utc timestamp with time zone
);

-- public.pay_payment_correction_request_candidates
create table public.pay_payment_correction_request_candidates (
  correction_request_id uuid not null,
  selection_ordinal bigint not null,
  pay_batch_candidate_id uuid not null,
  candidate_scope_hash text not null,
  active_item_count integer not null,
  source_row_count integer not null,
  active_amount numeric(14,2) not null,
  pay_batch_item_ids uuid[] not null,
  shared_instruction_scope_hash text,
  eligibility_code_at_plan text not null,
  created_at_utc timestamp with time zone not null
);

-- public.pay_payment_correction_requests
create table public.pay_payment_correction_requests (
  id uuid not null,
  pay_batch_id uuid not null,
  correction_kind text not null,
  status text not null,
  requested_by_user_id uuid,
  requested_at_utc timestamp with time zone not null,
  required_quantity integer not null,
  approved_count integer not null,
  golden_key_used boolean not null,
  golden_key_user_id uuid,
  reason text,
  selection_json jsonb not null,
  selection_hash text not null,
  plan_json jsonb not null,
  plan_hash text not null,
  accepted_resolution_json jsonb,
  accepted_resolution_hash text,
  source_bank_event_id uuid,
  auto_requested boolean not null,
  created_at_utc timestamp with time zone not null,
  authorised_at_utc timestamp with time zone,
  applied_at_utc timestamp with time zone,
  cancelled_at_utc timestamp with time zone,
  updated_at_utc timestamp with time zone not null,
  reauth_proof_hash text,
  reauth_expires_at_utc timestamp with time zone,
  reauth_consumed_at_utc timestamp with time zone
);

-- public.pay_payment_correction_work_items
create table public.pay_payment_correction_work_items (
  id uuid not null,
  correction_request_id uuid not null,
  pay_batch_id uuid not null,
  pay_batch_candidate_id uuid,
  pay_bank_transfer_id uuid,
  candidate_id uuid,
  umbrella_id uuid,
  work_kind text not null,
  selection_json jsonb not null,
  selection_hash text not null,
  status text not null,
  attempt_count integer not null,
  last_error text,
  locked_at_utc timestamp with time zone,
  locked_by text,
  created_at_utc timestamp with time zone not null,
  processed_at_utc timestamp with time zone,
  result_json jsonb not null,
  processed_by_user_id uuid
);

-- public.pay_payment_return_notice_groups
create table public.pay_payment_return_notice_groups (
  id uuid not null,
  pay_batch_id uuid,
  execution_commit_ref text,
  provider_key text,
  event_source text,
  notice_kind text not null,
  status text not null,
  quiet_until_utc timestamp with time zone not null,
  max_send_at_utc timestamp with time zone not null,
  summary_json jsonb not null,
  mail_outbox_ids jsonb not null,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null,
  sent_at_utc timestamp with time zone,
  alert_fingerprint text
);

-- public.pay_snooze_warning_acknowledgements
create table public.pay_snooze_warning_acknowledgements (
  id uuid not null,
  token_hash text not null,
  actor_user_id uuid not null,
  validation_phase text not null,
  candidate_id uuid not null,
  timesheet_id uuid,
  segment_id text,
  segment_stable_key text,
  source_ref text,
  snooze_kind text not null,
  snooze_until_date date,
  warning_code text not null,
  scope_fingerprint text not null,
  resolution_fingerprint text,
  date_context_fingerprint text not null,
  created_at_utc timestamp with time zone not null,
  expires_at_utc timestamp with time zone not null,
  last_used_at_utc timestamp with time zone
);

-- public.rates_candidate_overrides
create table public.rates_candidate_overrides (
  id uuid not null,
  candidate_id uuid not null,
  client_id uuid not null,
  role text not null,
  band text,
  pay_day numeric(10,2),
  pay_night numeric(10,2),
  pay_sat numeric(10,2),
  pay_sun numeric(10,2),
  pay_bh numeric(10,2),
  date_from date not null,
  date_to date,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  rate_type text not null
);

-- public.rates_client_defaults
create table public.rates_client_defaults (
  id uuid not null,
  client_id uuid not null,
  role text not null,
  band text,
  charge_day numeric(12,2),
  charge_night numeric(12,2),
  charge_sat numeric(12,2),
  charge_sun numeric(12,2),
  charge_bh numeric(12,2),
  __cloudtms_dropped_attnum_10 text,
  __cloudtms_dropped_attnum_11 text,
  __cloudtms_dropped_attnum_12 text,
  __cloudtms_dropped_attnum_13 text,
  __cloudtms_dropped_attnum_14 text,
  date_from date not null,
  date_to date,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  __cloudtms_dropped_attnum_19 text,
  paye_day numeric(12,2),
  paye_night numeric(12,2),
  paye_sat numeric(12,2),
  paye_sun numeric(12,2),
  paye_bh numeric(12,2),
  umb_day numeric(12,2),
  umb_night numeric(12,2),
  umb_sat numeric(12,2),
  umb_sun numeric(12,2),
  umb_bh numeric(12,2),
  disabled_at_utc timestamp with time zone,
  disabled_by uuid
);
alter table public.rates_client_defaults drop column __cloudtms_dropped_attnum_10;
alter table public.rates_client_defaults drop column __cloudtms_dropped_attnum_11;
alter table public.rates_client_defaults drop column __cloudtms_dropped_attnum_12;
alter table public.rates_client_defaults drop column __cloudtms_dropped_attnum_13;
alter table public.rates_client_defaults drop column __cloudtms_dropped_attnum_14;
alter table public.rates_client_defaults drop column __cloudtms_dropped_attnum_19;

-- public.rates_presets
create table public.rates_presets (
  id uuid not null,
  scope preset_scope_enum not null,
  client_id uuid,
  role text not null,
  band text,
  name text not null,
  paye_day numeric,
  paye_night numeric,
  paye_sat numeric,
  paye_sun numeric,
  paye_bh numeric,
  umb_day numeric,
  umb_night numeric,
  umb_sat numeric,
  umb_sun numeric,
  umb_bh numeric,
  charge_day numeric,
  charge_night numeric,
  charge_sat numeric,
  charge_sun numeric,
  charge_bh numeric,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  display_site text,
  bucket_labels_json jsonb,
  std_schedule_json jsonb,
  enable_paye boolean not null,
  enable_umbrella boolean not null,
  mileage_pay_rate numeric(10,2),
  mileage_charge_rate numeric(10,2)
);

-- public.report_presets
create table public.report_presets (
  id uuid not null,
  user_id uuid not null,
  section text not null,
  kind text not null,
  name text not null,
  filters_json jsonb not null,
  is_default boolean not null,
  is_shared boolean not null,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  selection_json jsonb
);

-- public.schema_migrations
create table public.schema_migrations (
  filename text not null,
  applied_at timestamp with time zone not null
);

-- public.schema_repeatables
create table public.schema_repeatables (
  filename text not null,
  content_sha256 text not null,
  applied_at timestamp with time zone not null
);

-- public.settings_defaults
create table public.settings_defaults (
  id smallint not null,
  timezone_id text not null,
  day_start time without time zone not null,
  day_end time without time zone not null,
  night_start time without time zone not null,
  night_end time without time zone not null,
  bh_source text not null,
  bh_list jsonb not null,
  bh_feed_url text,
  __cloudtms_dropped_attnum_10 text,
  __cloudtms_dropped_attnum_11 text,
  __cloudtms_dropped_attnum_12 text,
  __cloudtms_dropped_attnum_13 text,
  __cloudtms_dropped_attnum_14 text,
  __cloudtms_dropped_attnum_15 text,
  __cloudtms_dropped_attnum_16 text,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  bank_name text,
  bank_sort_code text,
  bank_account_number text,
  vat_registration_number text,
  __cloudtms_dropped_attnum_23 text,
  ts_reference_required boolean not null,
  sat_start time without time zone,
  sat_end time without time zone,
  sun_start time without time zone,
  sun_end time without time zone,
  agency_name text,
  agency_logo text,
  bh_start time without time zone not null,
  bh_end time without time zone not null,
  import_config_json jsonb not null,
  timesheet_footer_json jsonb,
  timesheet_header_json jsonb,
  temporary_worker_declaration_json jsonb,
  client_declaration_json jsonb,
  finance_email text,
  finance_email_settings jsonb not null,
  max_attachments_per_email integer not null,
  invoice_debug boolean not null,
  hr_attach_to_invoice boolean not null,
  ts_attach_to_invoice boolean not null,
  registered_address text,
  company_reg_number text,
  external_paye_system text not null,
  banking_system text not null,
  __cloudtms_dropped_attnum_48 text,
  __cloudtms_dropped_attnum_49 text,
  __cloudtms_dropped_attnum_50 text,
  __cloudtms_dropped_attnum_51 text,
  __cloudtms_dropped_attnum_52 text,
  system_emails jsonb not null,
  system_email text,
  rail_provider_default text not null,
  rail_env_default text not null,
  rail_supports_scheduling boolean not null,
  rail_supports_name_check boolean not null,
  rail_supports_auto_execute boolean not null,
  default_schedule_umbrella_local text not null,
  default_schedule_paye_local text not null,
  funds_warning_hours_json jsonb not null,
  rail_default_funding_account_ref text,
  payroll_testing boolean not null,
  paye_remittances_enabled boolean not null,
  remittance_includes_json jsonb,
  remittance_header_message text,
  remittance_footer_message text,
  payment_authoriser_quantity integer not null,
  remittance_test_recipient_email text,
  pay_eligibility_months_back integer not null,
  pay_eligibility_weeks_ahead integer not null,
  pay_export_csv_columns_json jsonb,
  remittances_detailed_breakdown boolean not null,
  remittance_receive_when_umbrella_paid boolean not null,
  comms_adaptors_json jsonb not null,
  pay_export_csv_format_json jsonb not null,
  payment_remittance_send_timing text not null,
  payment_return_auto_reverse_timesheets boolean not null,
  payment_return_admin_recipient_role text not null,
  payment_return_admin_notice_quiet_minutes integer not null,
  payment_return_admin_notice_max_wait_minutes integer not null,
  banking_pay_auto_unwind_terminal_no_money boolean not null,
  banking_pay_workbench_cron_enabled boolean not null,
  banking_pay_workbench_cron_claim_limit integer not null,
  banking_pay_workbench_cron_max_passes integer not null,
  banking_pay_workbench_cron_max_jobs integer not null,
  banking_pay_workbench_cron_max_rows integer not null,
  banking_pay_workbench_cron_max_runtime_ms integer not null,
  banking_pay_workbench_nudge_enabled boolean not null,
  banking_pay_workbench_nudge_claim_limit integer not null,
  banking_pay_workbench_nudge_max_passes integer not null,
  banking_pay_workbench_nudge_max_jobs integer not null,
  banking_pay_workbench_nudge_max_rows integer not null,
  banking_pay_workbench_nudge_max_runtime_ms integer not null,
  banking_pay_workbench_minimum_rpc_budget_ms integer,
  banking_pay_workbench_rpc_safety_buffer_ms integer not null,
  banking_pay_workbench_stage_work_units_per_job integer not null,
  banking_pay_workbench_db_worker_lease_seconds integer,
  banking_pay_workbench_db_worker_max_runtime_ms integer not null,
  banking_pay_workbench_db_worker_min_phase_budget_ms integer not null,
  banking_pay_workbench_auto_continuation_max_bursts integer not null,
  banking_pay_workbench_auto_continuation_max_runtime_ms integer not null,
  banking_pay_workbench_auto_continuation_per_burst_max_runtime_m integer not null,
  banking_pay_workbench_auto_continuation_min_runtime_ms integer not null,
  banking_pay_workbench_auto_continuation_max_passes integer not null,
  banking_pay_workbench_auto_continuation_claim_limit_max integer not null,
  banking_pay_workbench_auto_continuation_max_jobs integer not null,
  banking_pay_workbench_auto_continuation_max_rows integer not null,
  banking_pay_workbench_job_retry_base_seconds integer not null,
  banking_pay_workbench_job_retry_max_seconds integer not null,
  banking_pay_workbench_db_statement_timeout_ms integer not null,
  banking_pay_workbench_db_lock_timeout_ms integer not null,
  banking_pay_workbench_db_idle_tx_timeout_ms integer not null,
  banking_pay_workbench_scope_seed_units_per_job integer not null,
  banking_pay_workbench_source_build_units_per_job integer not null,
  banking_pay_workbench_line_seed_units_per_job integer not null,
  banking_pay_workbench_line_process_units_per_job integer not null,
  banking_pay_workbench_preview_mat_units_per_job integer not null,
  banking_pay_workbench_nudge_scope_seed_units_per_job integer not null,
  banking_pay_workbench_nudge_source_build_units_per_job integer not null,
  banking_pay_workbench_nudge_line_seed_units_per_job integer not null,
  banking_pay_workbench_nudge_line_process_units_per_job integer not null,
  banking_pay_workbench_nudge_preview_mat_units_per_job integer not null,
  banking_pay_workbench_cron_scope_seed_units_per_job integer not null,
  banking_pay_workbench_cron_source_build_units_per_job integer not null,
  banking_pay_workbench_cron_line_seed_units_per_job integer not null,
  banking_pay_workbench_cron_line_process_units_per_job integer not null,
  banking_pay_workbench_cron_preview_mat_units_per_job integer not null,
  banking_pay_workbench_rollover_enabled boolean not null,
  banking_pay_workbench_rollover_max_sessions_per_tick integer not null,
  banking_pay_workbench_rollover_nudge_after_create boolean not null,
  banking_pay_workbench_nudge_source_build_parallelism integer not null,
  banking_pay_workbench_nudge_source_build_parallel_bursts integer not null,
  banking_pay_workbench_nudge_source_build_runtime_floor_ms integer not null,
  banking_pay_workbench_nudge_source_build_lane_claim_limit integer not null,
  banking_pay_workbench_cron_source_build_parallelism integer not null,
  banking_pay_workbench_cron_source_build_parallel_bursts integer not null,
  banking_pay_workbench_cron_source_build_runtime_floor_ms integer not null,
  banking_pay_workbench_cron_source_build_lane_claim_limit integer not null,
  default_workbench_settings jsonb,
  banking_pay_workbench_delta_refresh_enabled boolean not null,
  banking_pay_workbench_delta_shadow_mode boolean not null,
  banking_pay_workbench_delta_enable_normal_timesheet boolean not null,
  banking_pay_workbench_delta_enable_readiness_only boolean not null,
  banking_pay_workbench_delta_enable_reservation_only boolean not null,
  banking_pay_workbench_delta_fallback_on_mismatch boolean not null,
  banking_pay_workbench_delta_units_per_job integer not null,
  banking_pay_workbench_nudge_delta_units_per_job integer not null,
  banking_pay_workbench_cron_delta_units_per_job integer not null,
  banking_pay_workbench_delta_budget_ms integer not null,
  banking_pay_workbench_patch_after_batch_mutation_enabled boolean not null,
  banking_pay_workbench_clone_rebase_enabled boolean not null,
  temp_log boolean not null,
  banking_pay_workbench_clone_rebase_units_per_job integer not null,
  banking_pay_workbench_nudge_clone_rebase_units_per_job integer not null,
  banking_pay_workbench_cron_clone_rebase_units_per_job integer not null,
  banking_pay_workbench_clone_rebase_budget_ms integer not null,
  banking_pay_official_pay_weekday smallint not null,
  healthroster_import_auto_authorise_default boolean not null,
  nhsp_import_auto_authorise_default boolean not null,
  auto_authorise_on_validation boolean not null,
  reversal_complete_financials_date correction_financials_date_basis_enum not null,
  reversal_replacement_financials_date correction_financials_date_basis_enum not null,
  invoice_document_presentation_json jsonb not null,
  banking_pay_workbench_scope_reconcile_enabled boolean not null,
  banking_pay_workbench_scope_reconcile_shadow_mode boolean not null,
  banking_pay_candidate_cancellation_enabled boolean not null,
  banking_pay_correction_max_candidates integer not null,
  banking_pay_correction_max_active_items integer not null,
  banking_pay_correction_max_active_items_per_candidate integer not null,
  banking_pay_correction_max_source_rows_per_candidate integer not null,
  banking_pay_workbench_reconciliation_envelope_version integer not null,
  banking_pay_workbench_reconciliation_envelope_json jsonb not null,
  banking_pay_workbench_reconciliation_envelope_evidence_json jsonb not null,
  banking_pay_workbench_delta_enable_simple_authorise boolean not null,
  banking_pay_workbench_delta_enable_simple_unauthorise boolean not null,
  banking_pay_workbench_delta_enable_exact_import_family boolean not null,
  banking_pay_workbench_source_build_execution_profile_version integer not null,
  banking_pay_workbench_clone_bounded_reuse_v2_enabled boolean not null,
  banking_pay_workbench_clone_source_empty_reuse_enabled boolean not null,
  banking_pay_workbench_reconciliation_optimization_version integer not null,
  banking_pay_workbench_semantic_ready_observe_v2_enabled boolean not null,
  banking_pay_workbench_semantic_ready_publication_v3_enabled boolean not null,
  banking_pay_workbench_semantic_ready_draft_guard_v2_enabled boolean not null,
  banking_pay_cancellation_reversion_observe_v1_enabled boolean not null,
  banking_pay_cancellation_reversion_publish_v1_enabled boolean not null,
  banking_pay_cancellation_reversion_exact_empty_v1_enabled boolean not null,
  banking_pay_draft_overlay_fast_cancel_v1_enabled boolean not null,
  candidate_app_environment text not null,
  candidate_app_feature_flags_json jsonb not null,
  candidate_app_system_actor_user_id uuid,
  candidate_electronic_auto_authorise_default boolean not null,
  candidate_hours_deviation_pct numeric(6,2) not null,
  candidate_barred_manager_email_domains jsonb not null,
  banking_pay_draft_expected_effects_v1_enabled boolean not null,
  banking_pay_draft_self_invalidation_claim_deferral_v1_enabled boolean not null,
  banking_pay_draft_create_adoption_v1_enabled boolean not null,
  banking_pay_source_publication_identity_write_v1_enabled boolean not null,
  banking_pay_source_publication_identity_enforce_v1_enabled boolean not null,
  banking_pay_same_authority_build_election_v1_enabled boolean not null,
  banking_pay_draft_step_rpc_v1_enabled boolean not null,
  banking_pay_selection_intent_identity_v1_enabled boolean not null,
  banking_pay_pre_bank_cancel_set_page_v1_enabled boolean not null,
  banking_pay_correction_request_dirty_deferral_v1_enabled boolean not null,
  banking_pay_scheduled_cancellation_reversion_v2_observe_enabled boolean not null,
  banking_pay_scheduled_cancellation_reversion_v2_publish_enabled boolean not null,
  banking_pay_correction_held_dirty_route_absorption_v1_enabled boolean not null,
  banking_pay_execution_refresh_owner_bridge_v1_observe_enabled boolean not null,
  banking_pay_execution_refresh_owner_bridge_v1_publish_enabled boolean not null
);
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_10;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_11;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_12;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_13;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_14;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_15;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_16;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_23;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_48;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_49;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_50;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_51;
alter table public.settings_defaults drop column __cloudtms_dropped_attnum_52;

-- public.settings_finance_windows
create table public.settings_finance_windows (
  id uuid not null,
  date_from date not null,
  date_to date,
  vat_rate_pct numeric not null,
  erni_pct numeric not null,
  holiday_pay_pct numeric not null,
  apply_holiday_to text,
  apply_erni_to text,
  margin_includes jsonb,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  mileage_pay_defaults numeric,
  mileage_charge_defaults numeric
);

-- public.sheets_outbox
create table public.sheets_outbox (
  id uuid not null,
  booking_id text not null,
  target outbox_target_enum not null,
  payload_json jsonb not null,
  status outbox_status_enum not null,
  attempt_count integer not null,
  last_error text,
  next_attempt_at timestamp with time zone,
  delivered_at timestamp with time zone,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null
);

-- public.timesheet_archive_transition_capability
create table public.timesheet_archive_transition_capability (
  capability_token uuid not null,
  backend_pid integer not null,
  transaction_id bigint not null,
  timesheet_id uuid not null,
  action text not null,
  created_at_utc timestamp with time zone not null
);

-- public.timesheet_evidence
create table public.timesheet_evidence (
  id uuid not null,
  timesheet_id uuid not null,
  kind text not null,
  display_name text,
  storage_key text not null,
  created_at timestamp with time zone not null,
  created_by uuid,
  document_asset_id uuid,
  source_revision text,
  processing_state text not null,
  processing_error_json jsonb,
  document_role text not null,
  candidate_component_id uuid
);

-- public.timesheet_financial_retention
create table public.timesheet_financial_retention (
  timesheet_id uuid not null,
  first_retained_at_utc timestamp with time zone not null
);

-- public.timesheet_lifecycle_bulk_operation_items
create table public.timesheet_lifecycle_bulk_operation_items (
  id uuid not null,
  operation_id uuid not null,
  ordinal integer not null,
  action text not null,
  row_key text,
  timesheet_id uuid not null,
  current_timesheet_id uuid,
  requested_timesheet_id uuid,
  expected_timesheet_id uuid,
  contract_week_id uuid,
  expected_row_signature text,
  status text not null,
  attempt_count integer not null,
  result_json jsonb not null,
  error_json jsonb not null,
  affected_rows_json jsonb not null,
  locked_at_utc timestamp with time zone,
  started_at_utc timestamp with time zone,
  completed_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.timesheet_lifecycle_bulk_operations
create table public.timesheet_lifecycle_bulk_operations (
  id uuid not null,
  action text not null,
  status text not null,
  requested_count integer not null,
  success_count integer not null,
  failure_count integer not null,
  created_by_user_id uuid,
  context text not null,
  request_json jsonb not null,
  progress_json jsonb not null,
  result_json jsonb not null,
  error_json jsonb not null,
  run_after_utc timestamp with time zone not null,
  started_at_utc timestamp with time zone,
  completed_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null,
  updated_at_utc timestamp with time zone not null
);

-- public.timesheet_pay_state
create table public.timesheet_pay_state (
  timesheet_id uuid not null,
  last_settled_snapshot_json jsonb not null,
  last_settled_signature text not null,
  last_settled_pay_batch_id uuid,
  last_settled_at_utc timestamp with time zone,
  summary_pay_status_code text,
  summary_pay_icon_code text,
  summary_pay_paid_at_utc timestamp with time zone,
  summary_net_delta_ex_vat numeric(12,2)
);

-- public.timesheet_pay_state_history
create table public.timesheet_pay_state_history (
  id uuid not null,
  timesheet_id uuid not null,
  pay_batch_id uuid not null,
  settled_at_utc timestamp with time zone not null,
  snapshot_json jsonb not null,
  signature text not null
);

-- public.timesheet_payment_overrides
create table public.timesheet_payment_overrides (
  id uuid not null,
  timesheet_id uuid not null,
  candidate_id uuid not null,
  override_type text not null,
  reason text not null,
  created_at_utc timestamp with time zone not null,
  created_by_user_id uuid,
  consumed_by_pay_batch_id uuid,
  consumed_at_utc timestamp with time zone,
  cleared_at_utc timestamp with time zone,
  cleared_by_user_id uuid,
  clear_reason text
);

-- public.timesheet_r2_cleanup_queue
create table public.timesheet_r2_cleanup_queue (
  delete_operation_id text not null,
  r2_key text not null,
  requested_timesheet_id uuid,
  deleted_timesheet_ids uuid[] not null,
  status text not null,
  attempt_count integer not null,
  last_error text,
  claim_token uuid,
  claimed_at_utc timestamp with time zone,
  next_attempt_at_utc timestamp with time zone not null,
  first_failed_at_utc timestamp with time zone not null,
  last_attempt_at_utc timestamp with time zone not null,
  completed_at_utc timestamp with time zone
);

-- public.timesheet_summary_pay_state_cache
create table public.timesheet_summary_pay_state_cache (
  timesheet_id uuid not null,
  paid_to_date_ex_vat numeric not null,
  last_paid_at_utc timestamp with time zone,
  reserved_ex_vat numeric not null,
  outstanding_ex_vat numeric not null,
  net_delta_ex_vat numeric not null,
  active_advance boolean not null,
  active_processing boolean not null,
  summary_state_applies boolean not null,
  advance_override_created_at_utc timestamp with time zone,
  advance_authorisation_consumed_at_utc timestamp with time zone,
  summary_pay_status_code text not null,
  summary_pay_icon_code text not null,
  summary_badge_codes text[] not null,
  refreshed_at_utc timestamp with time zone not null,
  refreshed_by_user_id uuid
);

-- public.timesheet_validations
create table public.timesheet_validations (
  id uuid not null,
  timesheet_id uuid,
  booking_id text,
  status validation_status_enum not null,
  reason_code text,
  hr_request_id text,
  validated_at_utc timestamp with time zone,
  override_requested_at_utc timestamp with time zone,
  override_requested_by uuid,
  override_confirmed_at_utc timestamp with time zone,
  override_confirmed_by uuid,
  override_reason text,
  override_cleared_at_utc timestamp with time zone,
  last_source uuid,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  hr_request_source reference_source_enum,
  hr_request_set_by uuid,
  hr_request_set_at_utc timestamp with time zone,
  pre_validated boolean not null
);

-- public.timesheets
create table public.timesheets (
  timesheet_id uuid not null,
  booking_id text not null,
  occupant_key_norm text not null,
  hospital_norm text not null,
  ward_norm text not null,
  job_title_norm text not null,
  shift_label_norm text,
  scheduled_start_iso timestamp with time zone,
  scheduled_end_iso timestamp with time zone,
  worked_start_iso timestamp with time zone,
  worked_end_iso timestamp with time zone,
  break_start_iso timestamp with time zone,
  break_end_iso timestamp with time zone,
  break_minutes integer,
  worked_minutes integer,
  week_ending_date date not null,
  auth_name text,
  auth_job_title text,
  authorised_at_server timestamp with time zone,
  r2_nurse_key text,
  r2_auth_key text,
  img_sha256_nurse text,
  img_sha256_auth text,
  reference_number text,
  reference_set_at timestamp with time zone,
  status timesheet_status_enum not null,
  idempotency_key text,
  client_hash text,
  client_ua text,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  version integer not null,
  is_current boolean not null,
  revoked_at timestamp with time zone,
  revoked_reason text,
  revoked_by text,
  contract_id uuid,
  submission_mode submission_mode_enum,
  manual_pdf_r2_key text,
  line_type timesheet_line_type_enum not null,
  sheet_scope timesheet_scope_enum not null,
  actual_schedule_json jsonb,
  additional_units_week jsonb not null,
  additional_units_per_day jsonb not null,
  qr_token text,
  qr_status timesheet_qr_status_enum,
  qr_payload_json jsonb not null,
  qr_generated_at timestamp with time zone,
  qr_scanned_at timestamp with time zone,
  qr_scan_info_json jsonb,
  qr_r2_key text,
  day_references_json jsonb,
  manual_pdf_rotation_degrees integer not null,
  qr_last_sent_hash text,
  qr_last_sent_at_utc timestamp with time zone,
  qr_signed_hash text,
  qr_signed_at_utc timestamp with time zone,
  candidate_hint_text jsonb,
  band text,
  generated_pdf_at_utc timestamp with time zone,
  is_adjustment boolean not null,
  parent_timesheet_id uuid,
  generated_pdf_refs_sig text,
  generated_pdf_refs_snapshot_json jsonb,
  generated_pdf_refs_captured_at_utc timestamp with time zone,
  qr_sent_refs_sig text,
  qr_sent_refs_snapshot_json jsonb,
  qr_sent_refs_captured_at_utc timestamp with time zone,
  correction_id text,
  correction_kind text,
  adjustment_origin text,
  archived_at_utc timestamp with time zone,
  archived_by_user_id uuid,
  archived_reason_code text,
  document_revision bigint not null,
  document_state text not null,
  current_document_version_id uuid,
  active_document_operation_id uuid,
  manual_document_asset_id uuid,
  last_document_error_json jsonb,
  candidate_submission_route_intent text,
  candidate_workflow_id uuid,
  candidate_workflow_generation integer,
  candidate_manager_approved_at_utc timestamp with time zone
);

-- public.timesheets_financials
create table public.timesheets_financials (
  id uuid not null,
  timesheet_id uuid not null,
  timesheet_version integer not null,
  basis timesheet_fin_basis_enum not null,
  is_current boolean not null,
  is_stale boolean not null,
  stale_reason text,
  worked_start_iso timestamp with time zone,
  worked_end_iso timestamp with time zone,
  break_start_iso timestamp with time zone,
  break_end_iso timestamp with time zone,
  break_minutes integer,
  candidate_id uuid,
  client_id uuid,
  role text,
  band text,
  pay_method text,
  policy_snapshot_json jsonb not null,
  rate_source_refs_json jsonb not null,
  hours_day numeric not null,
  hours_night numeric not null,
  hours_sat numeric not null,
  hours_sun numeric not null,
  hours_bh numeric not null,
  pay_day numeric,
  pay_night numeric,
  pay_sat numeric,
  pay_sun numeric,
  pay_bh numeric,
  charge_day numeric,
  charge_night numeric,
  charge_sat numeric,
  charge_sun numeric,
  charge_bh numeric,
  total_hours numeric not null,
  total_pay_ex_vat numeric not null,
  total_charge_ex_vat numeric not null,
  margin_ex_vat numeric not null,
  computed_at_utc timestamp with time zone not null,
  locked_by_invoice_id uuid,
  locked_at_utc timestamp with time zone,
  unlocked_by_credit_note_id uuid,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  occupant_key_norm text,
  candidate_assignment candidate_assignment_enum not null,
  processing_status ts_fin_processing_status_enum not null,
  expenses_pay_ex_vat numeric not null,
  expenses_charge_ex_vat numeric not null,
  expenses_description text,
  expenses_evidence_r2_key text,
  mileage_pay_ex_vat numeric not null,
  mileage_charge_ex_vat numeric not null,
  mileage_evidence_r2_key text,
  mileage_pay_rate numeric,
  mileage_charge_rate numeric,
  po_number text,
  pay_on_hold boolean not null,
  pay_on_hold_reason text,
  pay_on_hold_since_utc timestamp with time zone,
  paid_at_utc timestamp with time zone,
  paid_by_user_id uuid,
  payment_reference text,
  remittance_last_sent_at_utc timestamp with time zone,
  remittance_send_count integer not null,
  pay_wtr_rate_pct_snapshot numeric,
  pay_vat_rate_pct_snapshot numeric,
  pay_vat_amount_snapshot numeric not null,
  pay_total_inc_vat_snapshot numeric not null,
  processed_by_user_id uuid,
  processed_at_utc timestamp with time zone,
  authorised_by_user_id uuid,
  authorised_at_utc timestamp with time zone,
  expenses_evidence_manifest jsonb,
  mileage_evidence_manifest jsonb,
  actual_schedule_json jsonb,
  actual_minutes_by_day_json jsonb,
  additional_units_json jsonb not null,
  additional_pay_ex_vat numeric not null,
  additional_charge_ex_vat numeric not null,
  additional_margin_ex_vat numeric not null,
  invoice_breakdown_json jsonb not null,
  nhsp_import_id uuid,
  has_rate_issue boolean not null,
  has_pay_channel_issue boolean not null,
  hr_crosscheck_status text,
  hr_crosscheck_issues text[],
  external_source_rows_json jsonb,
  mileage_units numeric not null,
  travel_pay_ex_vat numeric not null,
  travel_charge_ex_vat numeric not null,
  accommodation_pay_ex_vat numeric not null,
  accommodation_charge_ex_vat numeric not null,
  other_pay_ex_vat numeric not null,
  other_charge_ex_vat numeric not null
);

-- public.tms_login_2fa_challenges
create table public.tms_login_2fa_challenges (
  id uuid not null,
  user_id uuid not null,
  ip_address text not null,
  code_salt text not null,
  code_hash text not null,
  expires_at_utc timestamp with time zone not null,
  used_at_utc timestamp with time zone,
  attempt_count integer not null,
  resend_count integer not null,
  last_sent_at_utc timestamp with time zone not null,
  created_at timestamp with time zone not null,
  purpose text not null
);

-- public.tms_password_resets
create table public.tms_password_resets (
  id uuid not null,
  user_id uuid not null,
  token text not null,
  expires_at timestamp with time zone not null,
  used_at timestamp with time zone,
  created_at timestamp with time zone not null
);

-- public.tms_user_2fa_trust
create table public.tms_user_2fa_trust (
  id uuid not null,
  user_id uuid not null,
  ip_address text not null,
  verified_at_utc timestamp with time zone not null,
  last_used_at_utc timestamp with time zone,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null
);

-- public.tms_users
create table public.tms_users (
  id uuid not null,
  email text not null,
  role text not null,
  is_active boolean not null,
  password_hash text not null,
  session_version integer not null,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  display_name text,
  grid_prefs_json jsonb not null,
  email_settings jsonb not null,
  last_login_at_utc timestamp with time zone,
  payment_authoriser boolean not null,
  payment_golden_key boolean not null,
  email_signature_html text
);

-- public.ts_financials_outbox
create table public.ts_financials_outbox (
  id uuid not null,
  timesheet_id uuid not null,
  reason ts_fin_reason_enum not null,
  attempt_count integer not null,
  next_attempt_at timestamp with time zone,
  last_error text,
  created_at timestamp with time zone not null
);

-- public.ts_pay_adjustments
create table public.ts_pay_adjustments (
  id uuid not null,
  timesheet_id uuid not null,
  candidate_id uuid not null,
  client_id uuid not null,
  week_ending_date date not null,
  delta_pay_ex_vat numeric(12,2) not null,
  reason text not null,
  created_at timestamp with time zone not null,
  paid_at_utc timestamp with time zone,
  paid_by_user_id uuid,
  payment_reference text,
  note text,
  updated_at timestamp with time zone not null,
  as_advance boolean not null,
  advance_reason pay_advance_reason_enum,
  meta_json jsonb not null
);

-- public.ts_pdfs_outbox
create table public.ts_pdfs_outbox (
  id uuid not null,
  timesheet_id uuid not null,
  reason ts_pdf_reason_enum not null,
  attempt_count integer not null,
  next_attempt_at timestamp with time zone,
  last_error text,
  prefer_generated boolean not null,
  force_regen boolean not null,
  created_at timestamp with time zone not null
);

-- public.umbrellas
create table public.umbrellas (
  id uuid not null,
  name text not null,
  remittance_email text,
  bank_name text,
  sort_code text,
  account_number text,
  vat_chargeable boolean not null,
  enabled boolean not null,
  created_at timestamp with time zone not null,
  updated_at timestamp with time zone not null,
  address_line1 text,
  address_line2 text,
  address_line3 text,
  town_city text,
  county text,
  postcode text,
  country text,
  company_number text,
  __cloudtms_dropped_attnum_19 text,
  __cloudtms_dropped_attnum_20 text,
  bank_details_hash text,
  remittance_overrides_enabled boolean not null,
  remittances_detailed_breakdown boolean not null
);
alter table public.umbrellas drop column __cloudtms_dropped_attnum_19;
alter table public.umbrellas drop column __cloudtms_dropped_attnum_20;
