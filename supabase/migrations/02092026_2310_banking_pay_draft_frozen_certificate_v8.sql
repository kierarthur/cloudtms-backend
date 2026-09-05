-- DRAFT_CERTIFICATE_CONSUMER_V1 transport/orchestration storage.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical only.
-- This migration stores references and receipts only. It does not change eligibility,
-- payment amounts, gross/net treatment, tax, VAT, channel, payee, finance, settlement,
-- cancellation or any other Banking Pay policy.

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_frozen_certificate_scopes_v8 (
  operation_id uuid PRIMARY KEY,
  certificate_uuid uuid NOT NULL,
  pay_channel_scope text NOT NULL,
  constituent_count integer NOT NULL,
  partition_count integer NOT NULL,
  canonical_amount_ex_vat_total text NOT NULL,
  selected_constituents_digest_sha256 text NOT NULL,
  selected_partitions_digest_sha256 text NOT NULL,
  manifest_digest_sha256 text NOT NULL,
  freeze_state text NOT NULL DEFAULT 'STAGING',
  frozen_at_utc timestamptz NULL,
  CONSTRAINT banking_pay_draft_frozen_certificate_scopes_v8_operation_fk
    FOREIGN KEY (operation_id) REFERENCES public.banking_pay_operations(id) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_certificate_scopes_v8_certificate_fk
    FOREIGN KEY (certificate_uuid)
    REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_certificate_scopes_v8_channel_chk
    CHECK (pay_channel_scope IN ('ALL', 'PAYE', 'UMBRELLA')),
  CONSTRAINT banking_pay_draft_frozen_certificate_scopes_v8_count_chk
    CHECK (constituent_count BETWEEN 1 AND 50000 AND partition_count >= 1),
  CONSTRAINT banking_pay_draft_frozen_certificate_scopes_v8_amount_chk
    CHECK (canonical_amount_ex_vat_total ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  CONSTRAINT bp_draft_frozen_cert_v8_constituent_digest_chk
    CHECK (selected_constituents_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT bp_draft_frozen_cert_v8_partition_digest_chk
    CHECK (selected_partitions_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT bp_draft_frozen_cert_v8_manifest_digest_chk
    CHECK (manifest_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_frozen_certificate_scopes_v8_state_chk
    CHECK (freeze_state IN ('STAGING', 'FROZEN', 'ABORTED_BEFORE_FINANCE', 'TERMINAL_COMPLETE', 'TERMINAL_FAILED')),
  CONSTRAINT banking_pay_draft_frozen_certificate_scopes_v8_frozen_shape_chk
    CHECK ((freeze_state = 'STAGING' AND frozen_at_utc IS NULL) OR (freeze_state <> 'STAGING' AND frozen_at_utc IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_frozen_constituent_refs_v8 (
  operation_id uuid NOT NULL,
  certificate_uuid uuid NOT NULL,
  constituent_ordinal integer NOT NULL,
  staged_page_sequence integer NOT NULL,
  CONSTRAINT banking_pay_draft_frozen_constituent_refs_v8_pk PRIMARY KEY (operation_id, constituent_ordinal),
  CONSTRAINT banking_pay_draft_frozen_constituent_refs_v8_scope_fk
    FOREIGN KEY (operation_id) REFERENCES private.banking_pay_draft_frozen_certificate_scopes_v8(operation_id) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_constituent_refs_v8_entry_fk
    FOREIGN KEY (certificate_uuid, constituent_ordinal)
    REFERENCES private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, constituent_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_constituent_refs_v8_ordinal_chk CHECK (constituent_ordinal BETWEEN 0 AND 49999),
  CONSTRAINT banking_pay_draft_frozen_constituent_refs_v8_page_chk CHECK (staged_page_sequence >= 0)
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_frozen_constituent_payloads_v8 (
  operation_id uuid NOT NULL,
  constituent_ordinal integer NOT NULL,
  certificate_uuid uuid NOT NULL,
  preview_row_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  resolved_pay_channel text NOT NULL,
  timesheet_id uuid NULL,
  finance_case_id uuid NULL,
  row_key text NOT NULL,
  row_ordinal bigint NOT NULL,
  payload_json jsonb NOT NULL,
  payload_digest_sha256 text NOT NULL,
  CONSTRAINT banking_pay_draft_frozen_constituent_payloads_v8_pk
    PRIMARY KEY (operation_id, constituent_ordinal),
  CONSTRAINT banking_pay_draft_frozen_constituent_payloads_v8_ref_fk
    FOREIGN KEY (operation_id, constituent_ordinal)
    REFERENCES private.banking_pay_draft_frozen_constituent_refs_v8(operation_id, constituent_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_constituent_payloads_v8_entry_fk
    FOREIGN KEY (certificate_uuid, constituent_ordinal)
    REFERENCES private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, constituent_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_constituent_payloads_v8_ordinal_chk
    CHECK (constituent_ordinal BETWEEN 0 AND 49999 AND row_ordinal >= 0),
  CONSTRAINT banking_pay_draft_frozen_constituent_payloads_v8_channel_chk
    CHECK (resolved_pay_channel IN ('PAYE', 'UMBRELLA')),
  CONSTRAINT banking_pay_draft_frozen_constituent_payloads_v8_key_chk
    CHECK (btrim(row_key) <> ''),
  CONSTRAINT banking_pay_draft_frozen_constituent_payloads_v8_payload_chk
    CHECK (jsonb_typeof(payload_json) = 'object'
      AND pg_catalog.octet_length(payload_json::text) BETWEEN 2 AND 65536),
  CONSTRAINT banking_pay_draft_frozen_constituent_payloads_v8_digest_chk
    CHECK (payload_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_frozen_constituent_payloads_v8_preview_uq
    UNIQUE (operation_id, preview_row_id)
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_frozen_partition_refs_v8 (
  operation_id uuid NOT NULL,
  certificate_uuid uuid NOT NULL,
  partition_ordinal integer NOT NULL,
  staged_page_sequence integer NOT NULL,
  CONSTRAINT banking_pay_draft_frozen_partition_refs_v8_pk PRIMARY KEY (operation_id, partition_ordinal),
  CONSTRAINT banking_pay_draft_frozen_partition_refs_v8_scope_fk
    FOREIGN KEY (operation_id) REFERENCES private.banking_pay_draft_frozen_certificate_scopes_v8(operation_id) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_partition_refs_v8_partition_fk
    FOREIGN KEY (certificate_uuid, partition_ordinal)
    REFERENCES private.banking_pay_workbench_settled_certificate_partitions_v8(certificate_uuid, partition_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_partition_refs_v8_ordinal_chk CHECK (partition_ordinal >= 0),
  CONSTRAINT banking_pay_draft_frozen_partition_refs_v8_page_chk CHECK (staged_page_sequence >= 0)
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_frozen_candidate_inputs_v8 (
  operation_id uuid NOT NULL,
  partition_ordinal integer NOT NULL,
  certificate_uuid uuid NOT NULL,
  candidate_state_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  resolved_pay_channel text NOT NULL,
  source_change_seq bigint NOT NULL,
  source_session_version bigint NOT NULL,
  source_build_run_id uuid NOT NULL,
  source_publication_id uuid NOT NULL,
  source_publication_attestation_json jsonb NOT NULL,
  effective_candidate_fragment_json jsonb NOT NULL,
  effective_summary_fragment_json jsonb NOT NULL,
  effective_paye_candidate_json jsonb NOT NULL,
  effective_non_paye_payee_json jsonb NOT NULL,
  effective_payees_json jsonb NOT NULL,
  effective_case_resolution_states_json jsonb NOT NULL,
  input_digest_sha256 text NOT NULL,
  CONSTRAINT banking_pay_draft_frozen_candidate_inputs_v8_pk
    PRIMARY KEY (operation_id, partition_ordinal),
  CONSTRAINT banking_pay_draft_frozen_candidate_inputs_v8_partition_ref_fk
    FOREIGN KEY (operation_id, partition_ordinal)
    REFERENCES private.banking_pay_draft_frozen_partition_refs_v8(operation_id, partition_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_candidate_inputs_v8_partition_fk
    FOREIGN KEY (certificate_uuid, partition_ordinal)
    REFERENCES private.banking_pay_workbench_settled_certificate_partitions_v8(certificate_uuid, partition_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_candidate_inputs_v8_ordinal_chk
    CHECK (partition_ordinal >= 0 AND source_change_seq >= 0 AND source_session_version >= 1),
  CONSTRAINT banking_pay_draft_frozen_candidate_inputs_v8_channel_chk
    CHECK (resolved_pay_channel IN ('PAYE', 'UMBRELLA')),
  CONSTRAINT banking_pay_draft_frozen_candidate_inputs_v8_json_chk
    CHECK (jsonb_typeof(source_publication_attestation_json) = 'object'
      AND jsonb_typeof(effective_candidate_fragment_json) = 'object'
      AND jsonb_typeof(effective_summary_fragment_json) = 'object'
      AND jsonb_typeof(effective_paye_candidate_json) = 'object'
      AND jsonb_typeof(effective_non_paye_payee_json) = 'object'
      AND jsonb_typeof(effective_payees_json) = 'array'
      AND jsonb_typeof(effective_case_resolution_states_json) = 'object'),
  CONSTRAINT banking_pay_draft_frozen_candidate_inputs_v8_digest_chk
    CHECK (input_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_candidate_inputs_v8_candidate_channel_uq
    UNIQUE (operation_id, candidate_id, resolved_pay_channel)
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_frozen_candidate_scopes_v8 (
  operation_id uuid NOT NULL,
  candidate_scope_ordinal integer NOT NULL,
  certificate_uuid uuid NOT NULL,
  partition_ordinal integer NOT NULL,
  candidate_id uuid NOT NULL,
  resolved_pay_channel text NOT NULL,
  constituent_count integer NOT NULL,
  canonical_amount_ex_vat_total text NOT NULL,
  scope_digest_sha256 text NOT NULL,
  scope_state text NOT NULL DEFAULT 'STAGED',
  pay_batch_id uuid NULL,
  CONSTRAINT banking_pay_draft_frozen_candidate_scopes_v8_pk PRIMARY KEY (operation_id, candidate_scope_ordinal),
  CONSTRAINT banking_pay_draft_frozen_candidate_scopes_v8_operation_fk
    FOREIGN KEY (operation_id) REFERENCES private.banking_pay_draft_frozen_certificate_scopes_v8(operation_id) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_candidate_scopes_v8_partition_fk
    FOREIGN KEY (certificate_uuid, partition_ordinal)
    REFERENCES private.banking_pay_workbench_settled_certificate_partitions_v8(certificate_uuid, partition_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_candidate_scopes_v8_batch_fk
    FOREIGN KEY (pay_batch_id) REFERENCES public.pay_batches(id) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_candidate_scopes_v8_ordinal_chk CHECK (candidate_scope_ordinal >= 0 AND partition_ordinal >= 0),
  CONSTRAINT banking_pay_draft_frozen_candidate_scopes_v8_channel_chk CHECK (resolved_pay_channel IN ('PAYE', 'UMBRELLA')),
  CONSTRAINT banking_pay_draft_frozen_candidate_scopes_v8_count_chk CHECK (constituent_count BETWEEN 1 AND 50000),
  CONSTRAINT banking_pay_draft_frozen_candidate_scopes_v8_amount_chk
    CHECK (canonical_amount_ex_vat_total ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  CONSTRAINT banking_pay_draft_frozen_candidate_scopes_v8_digest_chk CHECK (scope_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_frozen_candidate_scopes_v8_state_chk
    CHECK (scope_state IN ('STAGED', 'FROZEN', 'BATCH_LINKED', 'COMPLETE', 'FAILED')),
  CONSTRAINT bp_draft_frozen_candidate_v8_candidate_channel_uq
    UNIQUE (operation_id, candidate_id, resolved_pay_channel)
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_frozen_candidate_scope_members_v8 (
  operation_id uuid NOT NULL,
  candidate_scope_ordinal integer NOT NULL,
  member_ordinal integer NOT NULL,
  certificate_uuid uuid NOT NULL,
  partition_ordinal integer NOT NULL,
  constituent_ordinal integer NOT NULL,
  stable_identity_digest_sha256 text NOT NULL,
  CONSTRAINT banking_pay_draft_frozen_candidate_scope_members_v8_pk
    PRIMARY KEY (operation_id, candidate_scope_ordinal, member_ordinal),
  CONSTRAINT banking_pay_draft_frozen_candidate_scope_members_v8_scope_fk
    FOREIGN KEY (operation_id, candidate_scope_ordinal)
    REFERENCES private.banking_pay_draft_frozen_candidate_scopes_v8(operation_id, candidate_scope_ordinal) ON DELETE RESTRICT,
  CONSTRAINT bp_draft_frozen_candidate_member_v8_constituent_fk
    FOREIGN KEY (operation_id, constituent_ordinal)
    REFERENCES private.banking_pay_draft_frozen_constituent_refs_v8(operation_id, constituent_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_candidate_scope_members_v8_member_fk
    FOREIGN KEY (certificate_uuid, partition_ordinal, member_ordinal)
    REFERENCES private.banking_pay_workbench_settled_certificate_partition_members_v8(certificate_uuid, partition_ordinal, member_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_candidate_scope_members_v8_ordinal_chk
    CHECK (candidate_scope_ordinal >= 0 AND member_ordinal BETWEEN 0 AND 49999
      AND partition_ordinal >= 0 AND constituent_ordinal BETWEEN 0 AND 49999),
  CONSTRAINT banking_pay_draft_frozen_candidate_scope_members_v8_digest_chk
    CHECK (stable_identity_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT bp_draft_frozen_candidate_member_v8_constituent_uq UNIQUE (operation_id, constituent_ordinal),
  CONSTRAINT banking_pay_draft_frozen_candidate_scope_members_v8_identity_uq
    UNIQUE (operation_id, candidate_scope_ordinal, stable_identity_digest_sha256)
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_frozen_stage_receipts_v8 (
  operation_id uuid NOT NULL,
  stage_kind text NOT NULL,
  page_sequence integer NOT NULL,
  after_ordinal integer NULL,
  requested_limit integer NOT NULL,
  expected_previous_receipt_sha256 text NULL,
  request_preimage_digest_sha256 text NOT NULL,
  row_count integer NOT NULL,
  canonical_byte_count integer NOT NULL,
  next_after_ordinal integer NULL,
  has_more boolean NOT NULL,
  terminal_sentinel_present boolean NOT NULL,
  receipt_digest_sha256 text NOT NULL,
  stage_status text NOT NULL,
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_pk PRIMARY KEY (operation_id, stage_kind, page_sequence),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_operation_fk
    FOREIGN KEY (operation_id) REFERENCES private.banking_pay_draft_frozen_certificate_scopes_v8(operation_id) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_stage_chk CHECK (btrim(stage_kind) <> ''),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_page_chk
    CHECK (page_sequence >= 0 AND (after_ordinal IS NULL OR after_ordinal >= 0) AND requested_limit BETWEEN 1 AND 256),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_previous_chk
    CHECK (expected_previous_receipt_sha256 IS NULL OR expected_previous_receipt_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_request_chk CHECK (request_preimage_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_count_chk
    CHECK (row_count BETWEEN 0 AND 256 AND canonical_byte_count BETWEEN 0 AND 524288),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_cursor_chk CHECK (next_after_ordinal IS NULL OR next_after_ordinal >= 0),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_sentinel_chk
    CHECK (terminal_sentinel_present AND (NOT has_more OR next_after_ordinal IS NOT NULL)),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_receipt_chk CHECK (receipt_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_status_chk CHECK (stage_status IN ('COMMITTED', 'REPLAYED', 'TERMINAL')),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_chain_chk
    CHECK ((page_sequence = 0 AND after_ordinal IS NULL AND expected_previous_receipt_sha256 IS NULL)
      OR (page_sequence > 0 AND after_ordinal IS NOT NULL AND expected_previous_receipt_sha256 IS NOT NULL)),
  CONSTRAINT banking_pay_draft_frozen_stage_receipts_v8_cursor_uq
    UNIQUE NULLS NOT DISTINCT (operation_id, stage_kind, after_ordinal)
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_phase_units_v1 (
  operation_id uuid NOT NULL,
  phase text NOT NULL,
  candidate_scope_ordinal integer NOT NULL,
  unit_state text NOT NULL DEFAULT 'PENDING',
  next_owner_iteration integer NOT NULL DEFAULT 0,
  last_owner_receipt_sha256 text NULL,
  completed_at_utc timestamptz NULL,
  CONSTRAINT banking_pay_draft_phase_units_v1_pk
    PRIMARY KEY (operation_id, phase, candidate_scope_ordinal),
  CONSTRAINT banking_pay_draft_phase_units_v1_scope_fk
    FOREIGN KEY (operation_id, candidate_scope_ordinal)
    REFERENCES private.banking_pay_draft_frozen_candidate_scopes_v8(operation_id, candidate_scope_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_phase_units_v1_phase_chk
    CHECK (phase IN ('SEED_ALLOCATION_ROWS','CREATE_BATCH_SHELLS','INSERT_CANDIDATES','INSERT_ITEMS',
      'APPLY_FINANCE_ADJUSTMENTS','FINALISE_RESERVATIONS','POPULATE_CANDIDATE_SUMMARIES',
      'CREATE_TIMESHEET_SNAPSHOTS','BUILD_ITEM_BREAKDOWNS','ASSERT_INTEGRITY','CONSTITUENT_PARITY',
      'POST_CREATE_REFRESH')),
  CONSTRAINT banking_pay_draft_phase_units_v1_ordinal_chk
    CHECK (candidate_scope_ordinal >= 0 AND next_owner_iteration >= 0),
  CONSTRAINT banking_pay_draft_phase_units_v1_state_chk
    CHECK (unit_state IN ('PENDING','RUNNING','WAITING_CONTINUATION','COMPLETE','FAILED')),
  CONSTRAINT banking_pay_draft_phase_units_v1_receipt_chk
    CHECK (last_owner_receipt_sha256 IS NULL OR last_owner_receipt_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_phase_units_v1_completed_chk
    CHECK ((unit_state = 'COMPLETE' AND completed_at_utc IS NOT NULL)
      OR (unit_state <> 'COMPLETE' AND completed_at_utc IS NULL))
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_owner_receipts_v1 (
  operation_id uuid NOT NULL,
  phase text NOT NULL,
  candidate_scope_ordinal integer NOT NULL,
  owner_iteration integer NOT NULL,
  request_digest_sha256 text NOT NULL,
  previous_receipt_sha256 text NULL,
  delegated_owner_identity text NOT NULL,
  owner_result_json jsonb NOT NULL,
  owner_result_digest_sha256 text NOT NULL,
  owner_has_more boolean NOT NULL,
  owner_processed_count integer NOT NULL,
  owner_after_ordinal integer NULL,
  owner_next_after_ordinal integer NULL,
  owner_remaining_count integer NOT NULL,
  terminal_sentinel_present boolean NOT NULL,
  receipt_digest_sha256 text NOT NULL,
  completed_at_utc timestamptz NOT NULL,
  CONSTRAINT banking_pay_draft_owner_receipts_v1_pk
    PRIMARY KEY (operation_id, phase, candidate_scope_ordinal, owner_iteration),
  CONSTRAINT banking_pay_draft_owner_receipts_v1_unit_fk
    FOREIGN KEY (operation_id, phase, candidate_scope_ordinal)
    REFERENCES private.banking_pay_draft_phase_units_v1(operation_id, phase, candidate_scope_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_owner_receipts_v1_ordinal_chk
    CHECK (candidate_scope_ordinal >= 0 AND owner_iteration >= 0
      AND owner_processed_count BETWEEN 0 AND 100
      AND owner_remaining_count >= 0
      AND (owner_after_ordinal IS NULL OR owner_after_ordinal >= 0)
      AND (owner_next_after_ordinal IS NULL OR owner_next_after_ordinal >= 0)),
  CONSTRAINT banking_pay_draft_owner_receipts_v1_request_chk
    CHECK (request_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_owner_receipts_v1_previous_chk
    CHECK (previous_receipt_sha256 IS NULL OR previous_receipt_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_owner_receipts_v1_owner_chk
    CHECK (btrim(delegated_owner_identity) <> '' AND jsonb_typeof(owner_result_json) = 'object'),
  CONSTRAINT banking_pay_draft_owner_receipts_v1_result_chk
    CHECK (owner_result_digest_sha256 ~ '^[0-9a-f]{64}$'
      AND receipt_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_owner_receipts_v1_progress_chk
    CHECK (terminal_sentinel_present
      AND (NOT owner_has_more OR owner_next_after_ordinal IS NOT NULL)
      AND (owner_has_more OR owner_remaining_count = 0)),
  CONSTRAINT banking_pay_draft_owner_receipts_v1_request_uq
    UNIQUE (operation_id, phase, candidate_scope_ordinal, request_digest_sha256)
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_operation_created_batches_v8 (
  operation_id uuid NOT NULL,
  batch_ordinal integer NOT NULL,
  pay_batch_id uuid NOT NULL,
  candidate_scope_ordinal integer NOT NULL,
  integrity_state text NOT NULL DEFAULT 'PENDING',
  integrity_result_json jsonb NULL,
  integrity_result_digest_sha256 text NULL,
  integrity_checked_at_utc timestamptz NULL,
  post_refresh_state text NOT NULL DEFAULT 'PENDING',
  post_refresh_attempt_count integer NOT NULL DEFAULT 0,
  post_refresh_result_json jsonb NULL,
  post_refresh_result_digest_sha256 text NULL,
  post_refresh_checked_at_utc timestamptz NULL,
  CONSTRAINT banking_pay_draft_operation_created_batches_v8_pk PRIMARY KEY (operation_id, batch_ordinal),
  CONSTRAINT banking_pay_draft_operation_created_batches_v8_scope_fk
    FOREIGN KEY (operation_id, candidate_scope_ordinal)
    REFERENCES private.banking_pay_draft_frozen_candidate_scopes_v8(operation_id, candidate_scope_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_operation_created_batches_v8_batch_fk
    FOREIGN KEY (pay_batch_id) REFERENCES public.pay_batches(id) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_operation_created_batches_v8_ordinal_chk CHECK (batch_ordinal >= 0 AND candidate_scope_ordinal >= 0),
  CONSTRAINT bp_draft_created_batches_v8_integrity_state_chk
    CHECK (integrity_state IN ('PENDING','PASS','SKIPPED_EMPTY_RESERVED','CANCELLED_EMPTY_RESERVED','FAILED')),
  CONSTRAINT bp_draft_created_batches_v8_integrity_json_chk
    CHECK (integrity_result_json IS NULL OR jsonb_typeof(integrity_result_json) = 'object'),
  CONSTRAINT bp_draft_created_batches_v8_integrity_digest_chk
    CHECK (integrity_result_digest_sha256 IS NULL OR integrity_result_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT bp_draft_created_batches_v8_integrity_shape_chk
    CHECK ((integrity_state = 'PENDING' AND integrity_checked_at_utc IS NULL)
      OR (integrity_state <> 'PENDING' AND integrity_result_json IS NOT NULL
        AND integrity_result_digest_sha256 IS NOT NULL AND integrity_checked_at_utc IS NOT NULL)),
  CONSTRAINT bp_draft_created_batches_v8_refresh_state_chk
    CHECK (post_refresh_state IN ('PENDING','RETRY_REQUIRED','REPLACEMENT_REQUIRED','APPLIED','APPLIED_REPLACEMENT','FAILED')),
  CONSTRAINT bp_draft_created_batches_v8_refresh_attempt_chk
    CHECK (post_refresh_attempt_count >= 0),
  CONSTRAINT bp_draft_created_batches_v8_refresh_json_chk
    CHECK (post_refresh_result_json IS NULL OR jsonb_typeof(post_refresh_result_json) = 'object'),
  CONSTRAINT bp_draft_created_batches_v8_refresh_digest_chk
    CHECK (post_refresh_result_digest_sha256 IS NULL OR post_refresh_result_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT bp_draft_created_batches_v8_refresh_shape_chk
    CHECK ((post_refresh_state = 'PENDING' AND post_refresh_attempt_count = 0
          AND post_refresh_result_json IS NULL AND post_refresh_result_digest_sha256 IS NULL
          AND post_refresh_checked_at_utc IS NULL)
      OR (post_refresh_state <> 'PENDING' AND post_refresh_attempt_count >= 1
          AND post_refresh_result_json IS NOT NULL AND post_refresh_result_digest_sha256 IS NOT NULL
          AND post_refresh_checked_at_utc IS NOT NULL)),
  CONSTRAINT banking_pay_draft_operation_created_batches_v8_batch_uq UNIQUE (operation_id, pay_batch_id)
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_operation_terminal_results_v8 (
  operation_id uuid PRIMARY KEY,
  terminal_status text NOT NULL,
  created_pay_batch_count integer NOT NULL,
  created_pay_batch_ids_digest_sha256 text NOT NULL,
  result_digest_sha256 text NOT NULL,
  terminal_error_code text NULL,
  completed_at_utc timestamptz NOT NULL,
  legacy_terminal_result_json jsonb NOT NULL,
  legacy_terminal_result_digest_sha256 text NOT NULL,
  pay_batch_ids uuid[] NOT NULL,
  created_pay_batch_ids uuid[] NOT NULL,
  primary_pay_batch_id uuid NULL,
  pay_batch_id uuid NULL,
  created_batches jsonb NOT NULL,
  additive_certificate_diagnostics_json jsonb NOT NULL,
  CONSTRAINT banking_pay_draft_operation_terminal_results_v8_operation_fk
    FOREIGN KEY (operation_id) REFERENCES private.banking_pay_draft_frozen_certificate_scopes_v8(operation_id) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_operation_terminal_results_v8_status_chk
    CHECK (terminal_status IN ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED')),
  CONSTRAINT banking_pay_draft_operation_terminal_results_v8_count_chk CHECK (created_pay_batch_count >= 0),
  CONSTRAINT bp_draft_terminal_results_v8_created_digest_chk
    CHECK (created_pay_batch_ids_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT bp_draft_terminal_results_v8_result_digest_chk CHECK (result_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT bp_draft_terminal_results_v8_legacy_digest_chk
    CHECK (legacy_terminal_result_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_operation_terminal_results_v8_json_chk
    CHECK (jsonb_typeof(legacy_terminal_result_json) = 'object'
      AND jsonb_typeof(created_batches) = 'array'
      AND jsonb_typeof(additive_certificate_diagnostics_json) = 'object'),
  CONSTRAINT banking_pay_draft_operation_terminal_results_v8_compat_chk
    CHECK (pay_batch_ids = created_pay_batch_ids
      AND (cardinality(pay_batch_ids) = 0 OR primary_pay_batch_id = pay_batch_ids[1])
      AND pay_batch_id IS NOT DISTINCT FROM primary_pay_batch_id
      AND legacy_terminal_result_json->'pay_batch_ids' = to_jsonb(pay_batch_ids)
      AND legacy_terminal_result_json->'created_pay_batch_ids' = to_jsonb(created_pay_batch_ids)
      AND legacy_terminal_result_json->'created_batches' = created_batches)
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_operation_provenance_events_v8 (
  operation_id uuid NOT NULL,
  event_sequence integer NOT NULL,
  event_kind text NOT NULL,
  delegated_owner_identity text NOT NULL,
  owner_receipt_digest_sha256 text NOT NULL,
  event_at_utc timestamptz NOT NULL,
  CONSTRAINT banking_pay_draft_operation_provenance_events_v8_pk PRIMARY KEY (operation_id, event_sequence),
  CONSTRAINT banking_pay_draft_operation_provenance_events_v8_operation_fk
    FOREIGN KEY (operation_id) REFERENCES private.banking_pay_draft_frozen_certificate_scopes_v8(operation_id) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_operation_provenance_events_v8_sequence_chk CHECK (event_sequence >= 0),
  CONSTRAINT banking_pay_draft_operation_provenance_events_v8_kind_chk
    CHECK (event_kind IN ('ADMITTED', 'PAGE_STAGED', 'FROZEN', 'BATCH_LINKED', 'CANCEL_REQUESTED', 'ABORT_APPLIED', 'RESTORE_APPLIED', 'TERMINAL')),
  CONSTRAINT banking_pay_draft_operation_provenance_events_v8_owner_chk CHECK (btrim(delegated_owner_identity) <> ''),
  CONSTRAINT banking_pay_draft_operation_provenance_events_v8_digest_chk CHECK (owner_receipt_digest_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_finalizer_iterations_v8 (
  operation_id uuid NOT NULL,
  candidate_scope_ordinal integer NOT NULL,
  iteration_sequence integer NOT NULL,
  requested_owner_page_size integer NOT NULL,
  expected_previous_receipt_sha256 text NULL,
  request_preimage_digest_sha256 text NOT NULL,
  pre_unreserved_count integer NOT NULL,
  processed_count integer NOT NULL,
  post_unreserved_count integer NOT NULL,
  owner_has_more boolean NOT NULL,
  owner_result_digest_sha256 text NOT NULL,
  iteration_receipt_sha256 text NOT NULL,
  completed_at_utc timestamptz NOT NULL,
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_pk
    PRIMARY KEY (operation_id, candidate_scope_ordinal, iteration_sequence),
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_scope_fk
    FOREIGN KEY (operation_id, candidate_scope_ordinal)
    REFERENCES private.banking_pay_draft_frozen_candidate_scopes_v8(operation_id, candidate_scope_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_ordinal_chk CHECK (candidate_scope_ordinal >= 0 AND iteration_sequence >= 0),
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_page_chk CHECK (requested_owner_page_size = 100),
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_previous_chk
    CHECK (expected_previous_receipt_sha256 IS NULL OR expected_previous_receipt_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_request_chk CHECK (request_preimage_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_count_chk
    CHECK (pre_unreserved_count >= 0 AND processed_count BETWEEN 0 AND 100 AND post_unreserved_count >= 0
      AND post_unreserved_count <= pre_unreserved_count AND processed_count = pre_unreserved_count - post_unreserved_count),
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_progress_chk
    CHECK ((iteration_sequence = 0 AND expected_previous_receipt_sha256 IS NULL)
      OR (iteration_sequence > 0 AND expected_previous_receipt_sha256 IS NOT NULL)),
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_more_chk
    CHECK ((NOT owner_has_more OR (processed_count > 0 AND post_unreserved_count > 0))
      AND (owner_has_more OR post_unreserved_count = 0)),
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_owner_digest_chk CHECK (owner_result_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_finalizer_iterations_v8_receipt_chk CHECK (iteration_receipt_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS private.banking_pay_draft_constituent_parity_results_v8 (
  operation_id uuid NOT NULL,
  constituent_ordinal integer NOT NULL,
  certificate_uuid uuid NOT NULL,
  candidate_scope_ordinal integer NOT NULL,
  expected_constituent_digest_sha256 text NOT NULL,
  expected_pre_draft_facts_digest_sha256 text NOT NULL,
  actual_allocation_row_count integer NOT NULL,
  actual_allocation_digest_sha256 text NOT NULL,
  actual_item_row_count integer NOT NULL,
  actual_item_digest_sha256 text NOT NULL,
  actual_reservation_row_count integer NOT NULL,
  actual_reservation_digest_sha256 text NOT NULL,
  actual_materialisation_digest_sha256 text NOT NULL,
  expected_canonical_byte_count integer NOT NULL,
  actual_canonical_byte_count integer NOT NULL,
  comparison_status text NOT NULL,
  first_mismatch_code text NULL,
  comparison_digest_sha256 text NOT NULL,
  compared_at_utc timestamptz NOT NULL,
  CONSTRAINT banking_pay_draft_constituent_parity_results_v8_pk PRIMARY KEY (operation_id, constituent_ordinal),
  CONSTRAINT banking_pay_draft_constituent_parity_results_v8_constituent_fk
    FOREIGN KEY (operation_id, constituent_ordinal)
    REFERENCES private.banking_pay_draft_frozen_constituent_refs_v8(operation_id, constituent_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_constituent_parity_results_v8_scope_fk
    FOREIGN KEY (operation_id, candidate_scope_ordinal)
    REFERENCES private.banking_pay_draft_frozen_candidate_scopes_v8(operation_id, candidate_scope_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_constituent_parity_results_v8_entry_fk
    FOREIGN KEY (certificate_uuid, constituent_ordinal)
    REFERENCES private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, constituent_ordinal) ON DELETE RESTRICT,
  CONSTRAINT banking_pay_draft_constituent_parity_results_v8_ordinal_chk
    CHECK (constituent_ordinal BETWEEN 0 AND 49999 AND candidate_scope_ordinal >= 0),
  CONSTRAINT banking_pay_draft_constituent_parity_results_v8_count_chk
    CHECK (actual_allocation_row_count >= 0 AND actual_item_row_count >= 0 AND actual_reservation_row_count >= 0),
  CONSTRAINT banking_pay_draft_constituent_parity_results_v8_digest_chk
    CHECK (expected_constituent_digest_sha256 ~ '^[0-9a-f]{64}$'
      AND expected_pre_draft_facts_digest_sha256 ~ '^[0-9a-f]{64}$'
      AND actual_allocation_digest_sha256 ~ '^[0-9a-f]{64}$'
      AND actual_item_digest_sha256 ~ '^[0-9a-f]{64}$'
      AND actual_reservation_digest_sha256 ~ '^[0-9a-f]{64}$'
      AND actual_materialisation_digest_sha256 ~ '^[0-9a-f]{64}$'
      AND comparison_digest_sha256 ~ '^[0-9a-f]{64}$'),
  CONSTRAINT banking_pay_draft_constituent_parity_results_v8_bytes_chk
    CHECK (expected_canonical_byte_count BETWEEN 1 AND 65536 AND actual_canonical_byte_count BETWEEN 1 AND 65536),
  CONSTRAINT banking_pay_draft_constituent_parity_results_v8_status_chk
    CHECK ((comparison_status = 'MATCH' AND first_mismatch_code IS NULL)
      OR (comparison_status = 'MISMATCH' AND first_mismatch_code IS NOT NULL AND btrim(first_mismatch_code) <> ''))
);

CREATE INDEX IF NOT EXISTS banking_pay_draft_frozen_refs_v8_page_idx
  ON private.banking_pay_draft_frozen_constituent_refs_v8(operation_id, constituent_ordinal);
CREATE INDEX IF NOT EXISTS banking_pay_draft_frozen_payloads_v8_scope_page_idx
  ON private.banking_pay_draft_frozen_constituent_payloads_v8(operation_id, candidate_id, resolved_pay_channel, constituent_ordinal);
CREATE INDEX IF NOT EXISTS banking_pay_draft_frozen_payloads_v8_timesheet_page_idx
  ON private.banking_pay_draft_frozen_constituent_payloads_v8(operation_id, timesheet_id, constituent_ordinal)
  WHERE timesheet_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS banking_pay_draft_frozen_partitions_v8_page_idx
  ON private.banking_pay_draft_frozen_partition_refs_v8(operation_id, partition_ordinal);
CREATE INDEX IF NOT EXISTS banking_pay_draft_frozen_candidate_inputs_v8_page_idx
  ON private.banking_pay_draft_frozen_candidate_inputs_v8(operation_id, partition_ordinal);
CREATE INDEX IF NOT EXISTS banking_pay_draft_frozen_scopes_v8_page_idx
  ON private.banking_pay_draft_frozen_candidate_scopes_v8(operation_id, candidate_scope_ordinal);
CREATE INDEX IF NOT EXISTS banking_pay_draft_scope_members_v8_page_idx
  ON private.banking_pay_draft_frozen_candidate_scope_members_v8(operation_id, candidate_scope_ordinal, member_ordinal);
CREATE INDEX IF NOT EXISTS banking_pay_draft_scope_members_v8_constituent_idx
  ON private.banking_pay_draft_frozen_candidate_scope_members_v8(operation_id, constituent_ordinal);
CREATE INDEX IF NOT EXISTS banking_pay_draft_created_batches_v8_page_idx
  ON private.banking_pay_draft_operation_created_batches_v8(operation_id, batch_ordinal);
CREATE INDEX IF NOT EXISTS banking_pay_draft_finalizer_iterations_v8_next_idx
  ON private.banking_pay_draft_finalizer_iterations_v8(operation_id, candidate_scope_ordinal, iteration_sequence);
CREATE INDEX IF NOT EXISTS banking_pay_draft_parity_results_v8_page_idx
  ON private.banking_pay_draft_constituent_parity_results_v8(operation_id, constituent_ordinal);
CREATE INDEX IF NOT EXISTS banking_pay_draft_phase_units_v1_next_idx
  ON private.banking_pay_draft_phase_units_v1(operation_id, phase, unit_state, candidate_scope_ordinal);
CREATE INDEX IF NOT EXISTS banking_pay_draft_owner_receipts_v1_page_idx
  ON private.banking_pay_draft_owner_receipts_v1(operation_id, phase, candidate_scope_ordinal, owner_iteration);
CREATE INDEX IF NOT EXISTS banking_pay_operations_v8_legacy_activation_idx
  ON public.banking_pay_operations(operation_type, status, id)
  WHERE operation_type = 'DRAFT_CREATE' AND status NOT IN ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED');

ALTER TABLE private.banking_pay_draft_frozen_certificate_scopes_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_frozen_constituent_refs_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_frozen_constituent_payloads_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_frozen_partition_refs_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_frozen_candidate_inputs_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_frozen_candidate_scopes_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_frozen_candidate_scope_members_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_frozen_stage_receipts_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_phase_units_v1 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_owner_receipts_v1 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_operation_created_batches_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_operation_terminal_results_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_operation_provenance_events_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_finalizer_iterations_v8 OWNER TO postgres;
ALTER TABLE private.banking_pay_draft_constituent_parity_results_v8 OWNER TO postgres;

REVOKE ALL ON TABLE private.banking_pay_draft_frozen_certificate_scopes_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_frozen_constituent_refs_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_frozen_constituent_payloads_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_frozen_partition_refs_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_frozen_candidate_inputs_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_frozen_candidate_scopes_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_frozen_candidate_scope_members_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_frozen_stage_receipts_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_phase_units_v1 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_owner_receipts_v1 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_operation_created_batches_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_operation_terminal_results_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_operation_provenance_events_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_finalizer_iterations_v8 FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_draft_constituent_parity_results_v8 FROM PUBLIC, anon, authenticated, service_role;

COMMENT ON TABLE private.banking_pay_draft_frozen_certificate_scopes_v8 IS
  'One compact certified Create Draft operation header. Complete constituent data remains normalized and server-owned.';
COMMENT ON TABLE private.banking_pay_draft_frozen_constituent_payloads_v8 IS
  'One immutable certified preview payload per selected constituent. It freezes current pre-Draft authority without any complete selected-set array.';
COMMENT ON TABLE private.banking_pay_draft_frozen_candidate_inputs_v8 IS
  'Small Candidate/channel owner inputs frozen before final scope freeze. Complete preview-line arrays are intentionally excluded.';
COMMENT ON TABLE private.banking_pay_draft_frozen_candidate_scope_members_v8 IS
  'Normalized Candidate/channel membership. A 50,000-member Candidate never becomes one JSON array.';
COMMENT ON TABLE private.banking_pay_draft_phase_units_v1 IS
  'Restartable per-Candidate/channel Create Draft phase units. Unit rows replace the legacy all-scope array handoff.';
COMMENT ON TABLE private.banking_pay_draft_owner_receipts_v1 IS
  'Exact replay-safe receipts for one bounded invocation of an unchanged Draft owner.';
COMMENT ON TABLE private.banking_pay_draft_constituent_parity_results_v8 IS
  'H2 actual-versus-expected parity evidence. It records owner outputs and never decides payment economics.';
