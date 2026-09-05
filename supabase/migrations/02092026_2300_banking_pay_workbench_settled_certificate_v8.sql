-- WORKBENCH_SETTLED_CERTIFICATION_V2 / V8 normalized producer storage.
-- Runtime authority is Miget; the supabase directory name is historical only.
-- This migration stores evidence and identities only. It owns no payment economics.

CREATE SCHEMA IF NOT EXISTS private;

CREATE TABLE "private"."banking_pay_workbench_settled_certificates_v8" (
  "certificate_uuid" uuid NOT NULL DEFAULT gen_random_uuid(),
  "certification_id" text NULL,
  "certificate_contract" text NOT NULL CHECK (certificate_contract = 'WORKBENCH_SETTLED_CERTIFICATION_V2'),
  "certified_at_utc" timestamptz NULL,
  "workbench_session_id" uuid NOT NULL,
  "session_version" bigint NOT NULL CHECK (session_version >= 1),
  "progress_counter_version" bigint NOT NULL CHECK (progress_counter_version >= 1),
  "progress_state" text NOT NULL CHECK (progress_state = 'READY'),
  "source_snapshot_run_id" uuid NOT NULL,
  "session_signature" text NOT NULL CHECK (btrim(session_signature) <> ''),
  "pay_date" date NOT NULL,
  "week_ending_cutoff" date NOT NULL,
  "filters_digest_sha256" text NOT NULL CHECK (filters_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "scope_change_generation_target" bigint NOT NULL CHECK (scope_change_generation_target >= 0),
  "scope_change_generation_applied" bigint NOT NULL CHECK (scope_change_generation_applied >= 0),
  "scope_change_generation_shadow_checked" bigint NOT NULL CHECK (scope_change_generation_shadow_checked >= 0),
  "authority_fence_generation" bigint NOT NULL CHECK (authority_fence_generation >= 1),
  "publication_count" integer NOT NULL CHECK (publication_count >= 0),
  "publications_digest_sha256" text NOT NULL CHECK (publications_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "scope_total_count" integer NOT NULL CHECK (scope_total_count >= 0),
  "scope_seeded_count" integer NOT NULL CHECK (scope_seeded_count >= 0),
  "scope_ready_count" integer NOT NULL CHECK (scope_ready_count >= 0),
  "scope_pending_count" integer NOT NULL CHECK (scope_pending_count = 0),
  "scope_failed_count" integer NOT NULL CHECK (scope_failed_count = 0),
  "line_units_total" integer NOT NULL CHECK (line_units_total >= 0),
  "line_units_ready" integer NOT NULL CHECK (line_units_ready >= 0),
  "line_units_pending" integer NOT NULL CHECK (line_units_pending = 0),
  "line_units_failed" integer NOT NULL CHECK (line_units_failed = 0),
  "selected_row_count" integer NOT NULL CHECK (selected_row_count BETWEEN 1 AND 50000),
  "selected_page_order" text NOT NULL CHECK (selected_page_order = 'row_ordinal asc, preview_row_id asc'),
  "selected_page_size_max" integer NOT NULL CHECK (selected_page_size_max = 256),
  "selected_pages_fetched" integer NOT NULL CHECK (selected_pages_fetched >= 1),
  "selected_terminal_sentinel_seen" boolean NOT NULL CHECK (selected_terminal_sentinel_seen),
  "selected_sentinel_overflow" boolean NOT NULL CHECK (NOT selected_sentinel_overflow),
  "all_selected_rows_loaded" boolean NOT NULL CHECK (all_selected_rows_loaded),
  "server_selected_preview_row_ids_provided" boolean NOT NULL CHECK (server_selected_preview_row_ids_provided),
  "server_selected_ids_equal_materialised_selected_ids" boolean NOT NULL CHECK (server_selected_ids_equal_materialised_selected_ids),
  "ready_action_required_blocked_pairwise_disjoint" boolean NOT NULL CHECK (ready_action_required_blocked_pairwise_disjoint),
  "active_draft_rows_excluded" boolean NOT NULL CHECK (active_draft_rows_excluded),
  "ineligible_rows_excluded" boolean NOT NULL CHECK (ineligible_rows_excluded),
  "snoozed_rows_excluded" boolean NOT NULL CHECK (snoozed_rows_excluded),
  "unloaded_selection_gap_count" integer NOT NULL CHECK (unloaded_selection_gap_count = 0),
  "queued_current_job_count" integer NOT NULL CHECK (queued_current_job_count = 0),
  "running_current_job_count" integer NOT NULL CHECK (running_current_job_count = 0),
  "unresolved_current_job_count" integer NOT NULL CHECK (unresolved_current_job_count = 0),
  "invalid_current_job_pointer_count" integer NOT NULL CHECK (invalid_current_job_pointer_count = 0),
  "historical_terminal_rows_retained" boolean NOT NULL CHECK (historical_terminal_rows_retained),
  "historical_terminal_rows_are_not_current_authority" boolean NOT NULL CHECK (historical_terminal_rows_are_not_current_authority),
  "can_create_draft" boolean NOT NULL CHECK (can_create_draft),
  "selected_eligible_ready_row_count" integer NOT NULL CHECK (selected_eligible_ready_row_count BETWEEN 1 AND 50000),
  "blocking_reason_count" integer NOT NULL CHECK (blocking_reason_count = 0),
  "gate_digest_sha256" text NOT NULL CHECK (gate_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "selected_constituent_count" integer NOT NULL CHECK (selected_constituent_count BETWEEN 1 AND 50000),
  "selected_constituents_digest_sha256" text NULL CHECK (selected_constituents_digest_sha256 IS NULL OR selected_constituents_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "selected_canonical_amount_ex_vat_total" text NOT NULL CHECK (selected_canonical_amount_ex_vat_total ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "selected_partition_count" integer NOT NULL CHECK (selected_partition_count >= 1),
  "selected_partitions_ordering" text NOT NULL CHECK (selected_partitions_ordering = 'minimum constituent ordinal asc, candidate_id asc, resolved_pay_channel asc'),
  "selected_partitions_digest_sha256" text NULL CHECK (selected_partitions_digest_sha256 IS NULL OR selected_partitions_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "manifests_digest_sha256" text NULL CHECK (manifests_digest_sha256 IS NULL OR manifests_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "policy_contract_version" text NOT NULL CHECK (policy_contract_version = 'WORKBENCH_PAYMENT_POLICY_PARITY_V2'),
  "before_policy_projection_digest_sha256" text NOT NULL CHECK (before_policy_projection_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "after_policy_projection_digest_sha256" text NOT NULL CHECK (after_policy_projection_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "policy_digests_equal" boolean NOT NULL CHECK (policy_digests_equal),
  "execution_recovery_delta_only" boolean NOT NULL CHECK (execution_recovery_delta_only),
  "forbidden_policy_delta_count" integer NOT NULL CHECK (forbidden_policy_delta_count = 0),
  "no_payment_policy_change" boolean NOT NULL CHECK (no_payment_policy_change),
  "overall_digest_sha256" text NULL CHECK (overall_digest_sha256 IS NULL OR overall_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "lifecycle" text NOT NULL CHECK (lifecycle IN ('BUILDING','SEALED_CURRENT','SUPERSEDED_BY_NEW_SESSION','REVOKED_CORRUPT_OR_SECURITY','BUILD_FAILED')),
  "build_failure_code" text NULL CHECK (build_failure_code IS NULL OR btrim(build_failure_code) <> ''),
  "build_failure_message" text NULL,
  "build_failed_at_utc" timestamptz NULL,
  "build_id" uuid NOT NULL,
  "build_idempotency_key" text NOT NULL CHECK (btrim(build_idempotency_key) <> ''),
  "build_after_ordinal" integer NULL CHECK (build_after_ordinal IS NULL OR build_after_ordinal >= 0),
  "lease_owner" text NULL,
  "lease_expires_at_utc" timestamptz NULL,
  "sealed_at_utc" timestamptz NULL,
  "superseded_by_certificate_uuid" uuid NULL,
  "revocation_code" text NULL,
  "created_by_user_id" uuid NOT NULL,
  "created_at_utc" timestamptz NOT NULL DEFAULT clock_timestamp(),
  "candidate_filter_id" uuid NULL,
  "client_filter_id" uuid NULL,
  "filter_binding_mode" text NOT NULL CHECK (filter_binding_mode = 'EXACT_CERTIFIED_SELECTED_UNIVERSE'),
  "filter_context_digest_sha256" text NOT NULL CHECK (filter_context_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "filter_scope_manifest_digest_sha256" text NULL CHECK (filter_scope_manifest_digest_sha256 IS NULL OR filter_scope_manifest_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid"),
  UNIQUE (certification_id),
  UNIQUE (build_id),
  UNIQUE (workbench_session_id, session_version, progress_counter_version, authority_fence_generation, build_idempotency_key),
  CHECK (scope_change_generation_target = scope_change_generation_applied AND scope_change_generation_target = scope_change_generation_shadow_checked),
  CHECK (scope_total_count = scope_seeded_count AND scope_total_count = scope_ready_count),
  CHECK (line_units_total = line_units_ready),
  CHECK (selected_row_count = selected_constituent_count AND selected_eligible_ready_row_count = selected_constituent_count),
  CHECK (before_policy_projection_digest_sha256 = after_policy_projection_digest_sha256),
  CHECK ((lifecycle = 'BUILDING' AND certification_id IS NULL AND overall_digest_sha256 IS NULL AND sealed_at_utc IS NULL AND build_failure_code IS NULL AND build_failure_message IS NULL AND build_failed_at_utc IS NULL) OR (lifecycle = 'BUILD_FAILED' AND certification_id IS NULL AND overall_digest_sha256 IS NULL AND sealed_at_utc IS NULL AND build_failure_code IS NOT NULL AND btrim(build_failure_code) <> '' AND build_failed_at_utc IS NOT NULL AND lease_owner IS NULL AND lease_expires_at_utc IS NULL) OR (lifecycle IN ('SEALED_CURRENT','SUPERSEDED_BY_NEW_SESSION','REVOKED_CORRUPT_OR_SECURITY') AND certification_id = 'WORKBENCH_SETTLED_CERTIFICATION_V2:' || overall_digest_sha256 AND sealed_at_utc IS NOT NULL AND build_failure_code IS NULL AND build_failure_message IS NULL AND build_failed_at_utc IS NULL)),
  CHECK (lifecycle <> 'SUPERSEDED_BY_NEW_SESSION' OR superseded_by_certificate_uuid IS NOT NULL),
  CHECK (lifecycle <> 'REVOKED_CORRUPT_OR_SECURITY' OR btrim(revocation_code) <> ''),
  FOREIGN KEY (workbench_session_id) REFERENCES public.banking_pay_workbench_sessions(id) ON DELETE RESTRICT,
  FOREIGN KEY (superseded_by_certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificates_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificates_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificates_v8" TO postgres;

-- Immutable, compact source membership captured under the session authority
-- fence. This is the keyset bridge between Workbench rows and bounded build
-- pages; it contains no Draft-produced facts and no complete JSON arrays.
CREATE TABLE "private"."banking_pay_workbench_settled_certificate_source_members_v8" (
  "certificate_uuid" uuid NOT NULL,
  "constituent_ordinal" integer NOT NULL CHECK (constituent_ordinal BETWEEN 0 AND 49999),
  "preview_row_id" uuid NOT NULL,
  "source_row_ordinal" bigint NOT NULL CHECK (source_row_ordinal >= 0),
  -- Filled by the bounded append page that validates this source row.  NULL is
  -- permitted only while the parent certificate is BUILDING; seal rejects any
  -- uncaptured member, so build_start never hashes 50,000 rows in one call.
  "source_row_digest_sha256" text NULL CHECK (source_row_digest_sha256 IS NULL OR source_row_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid", "constituent_ordinal"),
  UNIQUE (certificate_uuid, preview_row_id),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT,
  FOREIGN KEY (preview_row_id) REFERENCES public.banking_pay_workbench_preview_rows(id) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_source_members_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_source_members_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_source_members_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_publications_v8" (
  "certificate_uuid" uuid NOT NULL,
  "scope_ordinal" integer NOT NULL CHECK (scope_ordinal >= 0),
  "candidate_id" uuid NOT NULL,
  "candidate_state_id" uuid NOT NULL,
  "candidate_state_status" text NOT NULL CHECK (candidate_state_status = 'READY'),
  "source_change_seq" bigint NOT NULL CHECK (source_change_seq >= 0),
  "source_build_run_id" uuid NOT NULL,
  "source_publication_id" uuid NOT NULL,
  "certified_publication_session_version" bigint NOT NULL CHECK (certified_publication_session_version >= 1),
  "publication_attestation_version" bigint NOT NULL CHECK (publication_attestation_version >= 1),
  "publication_attestation_digest_sha256" text NOT NULL CHECK (publication_attestation_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "publication_parity_ok" boolean NOT NULL CHECK (publication_parity_ok),
  "publication_attested_at_utc" timestamptz NOT NULL,
  PRIMARY KEY ("certificate_uuid", "scope_ordinal"),
  UNIQUE (certificate_uuid, candidate_id),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_publications_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_publications_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_publications_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_universes_v8" (
  "certificate_uuid" uuid NOT NULL,
  "universe_kind" text NOT NULL CHECK (universe_kind IN ('READY','ACTION_REQUIRED','BLOCKED','ACTIVE_DRAFT','INELIGIBLE','SNOOZED')),
  "row_count" integer NOT NULL CHECK (row_count >= 0),
  "universe_digest_sha256" text NOT NULL CHECK (universe_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid", "universe_kind"),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_universes_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_universes_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_universes_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_universe_members_v8" (
  "certificate_uuid" uuid NOT NULL,
  "universe_kind" text NOT NULL,
  "member_ordinal" integer NOT NULL CHECK (member_ordinal >= 0),
  "stable_identity_digest_sha256" text NOT NULL CHECK (stable_identity_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid", "universe_kind", "member_ordinal"),
  UNIQUE (certificate_uuid, universe_kind, stable_identity_digest_sha256),
  FOREIGN KEY (certificate_uuid, universe_kind) REFERENCES private.banking_pay_workbench_settled_certificate_universes_v8(certificate_uuid, universe_kind) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_universe_members_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_universe_members_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_universe_members_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_entries_v8" (
  "certificate_uuid" uuid NOT NULL,
  "constituent_ordinal" integer NOT NULL CHECK (constituent_ordinal BETWEEN 0 AND 49999),
  "preview_row_id" uuid NOT NULL,
  "materialised_preview_row_id" uuid NOT NULL,
  "presentation_preview_row_id" text NOT NULL CHECK (btrim(presentation_preview_row_id) <> ''),
  "row_key" text NOT NULL CHECK (btrim(row_key) <> ''),
  "line_id" text NULL,
  "source_kind" text NOT NULL CHECK (btrim(source_kind) <> ''),
  "source_line_id" text NULL,
  "source_row_key" text NOT NULL CHECK (btrim(source_row_key) <> ''),
  "source_publication_id" uuid NOT NULL,
  "source_change_seq" bigint NOT NULL CHECK (source_change_seq >= 0),
  "source_build_run_id" uuid NOT NULL,
  "source_identity_digest_sha256" text NOT NULL CHECK (source_identity_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "candidate_publication_ordinal" integer NOT NULL CHECK (candidate_publication_ordinal >= 0),
  "candidate_id" uuid NOT NULL,
  "client_id" uuid NULL,
  "timesheet_id" uuid NULL,
  "resolved_pay_channel" text NOT NULL CHECK (resolved_pay_channel IN ('PAYE','UMBRELLA')),
  "resolved_payment_method" text NOT NULL CHECK (btrim(resolved_payment_method) <> ''),
  "amount_sign" text NOT NULL CHECK (amount_sign IN ('NEGATIVE','ZERO','POSITIVE')),
  "semantic_kind" text NOT NULL CHECK (btrim(semantic_kind) <> ''),
  "economic_key_timesheet_id" uuid NULL,
  "economic_key_type" text NOT NULL CHECK (economic_key_type IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')),
  "economic_key_value" text NOT NULL CHECK (btrim(economic_key_value) <> ''),
  "canonical_amount_ex_vat" text NOT NULL CHECK (canonical_amount_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "source_reservation_amount_ex_vat" text NULL CHECK (source_reservation_amount_ex_vat IS NULL OR source_reservation_amount_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "prior_payment_treatment" text NOT NULL CHECK (btrim(prior_payment_treatment) <> ''),
  "prior_paid_amount_ex_vat" text NOT NULL CHECK (prior_paid_amount_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "prior_payment_evidence_digest_sha256" text NOT NULL CHECK (prior_payment_evidence_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "supersession_treatment" text NOT NULL CHECK (btrim(supersession_treatment) <> ''),
  "supersession_evidence_digest_sha256" text NOT NULL CHECK (supersession_evidence_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "recovery_contract_version" text NULL,
  "recovery_nominal_due_amount_ex_vat" text NULL CHECK (recovery_nominal_due_amount_ex_vat IS NULL OR recovery_nominal_due_amount_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "recovery_recoverable_this_pay_run_ex_vat" text NULL CHECK (recovery_recoverable_this_pay_run_ex_vat IS NULL OR recovery_recoverable_this_pay_run_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "recovery_headroom_amount_ex_vat" text NULL CHECK (recovery_headroom_amount_ex_vat IS NULL OR recovery_headroom_amount_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "recovery_allocated_amount_ex_vat" text NULL CHECK (recovery_allocated_amount_ex_vat IS NULL OR recovery_allocated_amount_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "recovery_result_kind" text NOT NULL CHECK (btrim(recovery_result_kind) <> ''),
  "recovery_overlay_digest_sha256" text NULL CHECK (recovery_overlay_digest_sha256 IS NULL OR recovery_overlay_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "expected_allocation_basis_kind" text NOT NULL CHECK (btrim(expected_allocation_basis_kind) <> ''),
  "expected_allocated_recovery_amount_ex_vat" text NULL CHECK (expected_allocated_recovery_amount_ex_vat IS NULL OR expected_allocated_recovery_amount_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "expected_allocation_result" text NOT NULL CHECK (btrim(expected_allocation_result) <> ''),
  "expected_allocation_source_digest_sha256" text NULL CHECK (expected_allocation_source_digest_sha256 IS NULL OR expected_allocation_source_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "expected_item_semantic_kind" text NOT NULL CHECK (btrim(expected_item_semantic_kind) <> ''),
  "expected_item_source_identity_digest_sha256" text NOT NULL CHECK (expected_item_source_identity_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "expected_item_amount_ex_vat" text NOT NULL CHECK (expected_item_amount_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "expected_item_source_digest_sha256" text NOT NULL CHECK (expected_item_source_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "expected_reservation_applicability" text NOT NULL CHECK (expected_reservation_applicability IN ('APPLICABLE','NOT_APPLICABLE')),
  "expected_reservation_amount_ex_vat" text NULL CHECK (expected_reservation_amount_ex_vat IS NULL OR expected_reservation_amount_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "expected_reservation_source_digest_sha256" text NULL CHECK (expected_reservation_source_digest_sha256 IS NULL OR expected_reservation_source_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "all_same_key_count" integer NOT NULL CHECK (all_same_key_count >= 0),
  "all_same_key_digest_sha256" text NOT NULL CHECK (all_same_key_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "signed_match_count" integer NOT NULL CHECK (signed_match_count >= 0),
  "signed_match_digest_sha256" text NOT NULL CHECK (signed_match_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "decisive_signed_count" integer NOT NULL CHECK (decisive_signed_count IN (0,1)),
  "decisive_signed_digest_sha256" text NOT NULL CHECK (decisive_signed_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "readiness_class" text NOT NULL CHECK (readiness_class = 'READY'),
  "selection_state" text NOT NULL CHECK (selection_state = 'SELECTED'),
  "selected" boolean NOT NULL CHECK (selected),
  "draftable" boolean NOT NULL CHECK (draftable),
  "is_ready_for_draft" boolean NOT NULL CHECK (is_ready_for_draft),
  "constituent_digest_sha256" text NOT NULL CHECK (constituent_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid", "constituent_ordinal"),
  UNIQUE (certificate_uuid, preview_row_id),
  UNIQUE (certificate_uuid, row_key),
  CHECK (preview_row_id = materialised_preview_row_id),
  CHECK (timesheet_id IS NOT DISTINCT FROM economic_key_timesheet_id),
  CHECK (expected_item_semantic_kind = semantic_kind),
  CHECK (expected_item_source_identity_digest_sha256 = source_identity_digest_sha256),
  CHECK (expected_item_amount_ex_vat = canonical_amount_ex_vat),
  CHECK (expected_reservation_amount_ex_vat IS NOT DISTINCT FROM source_reservation_amount_ex_vat),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT,
  FOREIGN KEY (certificate_uuid, candidate_publication_ordinal) REFERENCES private.banking_pay_workbench_settled_certificate_publications_v8(certificate_uuid, scope_ordinal) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_entries_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_entries_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_entries_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_superseded_sources_v8" (
  "certificate_uuid" uuid NOT NULL,
  "constituent_ordinal" integer NOT NULL CHECK (constituent_ordinal BETWEEN 0 AND 49999),
  "source_ordinal" integer NOT NULL CHECK (source_ordinal >= 0),
  "superseded_source_id" text NOT NULL CHECK (btrim(superseded_source_id) <> ''),
  PRIMARY KEY ("certificate_uuid", "constituent_ordinal", "source_ordinal"),
  FOREIGN KEY (certificate_uuid, constituent_ordinal) REFERENCES private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, constituent_ordinal) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_superseded_sources_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_superseded_sources_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_superseded_sources_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_cert_source_reservations_v8" (
  "certificate_uuid" uuid NOT NULL,
  "constituent_ordinal" integer NOT NULL CHECK (constituent_ordinal BETWEEN 0 AND 49999),
  "reservation_ordinal" integer NOT NULL CHECK (reservation_ordinal >= 0),
  "source_reservation_id" text NOT NULL CHECK (btrim(source_reservation_id) <> ''),
  PRIMARY KEY ("certificate_uuid", "constituent_ordinal", "reservation_ordinal"),
  FOREIGN KEY (certificate_uuid, constituent_ordinal) REFERENCES private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, constituent_ordinal) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_cert_source_reservations_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_cert_source_reservations_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_cert_source_reservations_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_component_evidence_v8" (
  "certificate_uuid" uuid NOT NULL,
  "constituent_ordinal" integer NOT NULL CHECK (constituent_ordinal BETWEEN 0 AND 49999),
  "evidence_kind" text NOT NULL CHECK (evidence_kind IN ('ALL_SAME_ECONOMIC_KEY','FULL_SIGNED_PRE_SIGNATURE','DECISIVE_SIGNED_EVIDENCE')),
  "evidence_ordinal" integer NOT NULL CHECK (evidence_ordinal >= 0),
  "stable_component_id" text NOT NULL CHECK (btrim(stable_component_id) <> ''),
  "frozen_component_ordinal" integer NOT NULL CHECK (frozen_component_ordinal >= 0),
  "source_component_kind" text NOT NULL CHECK (btrim(source_component_kind) <> ''),
  "economic_key_type" text NOT NULL CHECK (economic_key_type IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')),
  "economic_key_value" text NOT NULL CHECK (btrim(economic_key_value) <> ''),
  "component_fallback" text NULL,
  "authoritative_truth_ex_vat" text NULL CHECK (authoritative_truth_ex_vat IS NULL OR authoritative_truth_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "authoritative_baseline_ex_vat" text NULL CHECK (authoritative_baseline_ex_vat IS NULL OR authoritative_baseline_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "authoritative_reserved_ex_vat" text NULL CHECK (authoritative_reserved_ex_vat IS NULL OR authoritative_reserved_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "authoritative_outstanding_ex_vat" text NULL CHECK (authoritative_outstanding_ex_vat IS NULL OR authoritative_outstanding_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "component_amount_ex_vat" text NULL CHECK (component_amount_ex_vat IS NULL OR component_amount_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "source_pay_ex_vat" text NULL CHECK (source_pay_ex_vat IS NULL OR source_pay_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "source_charge_ex_vat" text NULL CHECK (source_charge_ex_vat IS NULL OR source_charge_ex_vat ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "financial_revision_digest" text NULL,
  "target_authority_digest" text NULL,
  "conversion_context_digest" text NULL,
  "physical_bucket_key" text NULL,
  "physical_bucket_digest" text NULL,
  "sealed_evidence_digest" text NULL,
  "source_pay_method" text NULL,
  "target_pay_method" text NULL,
  "full_signed_pre_signature_match" boolean NOT NULL,
  "decisive_signed_evidence" boolean NOT NULL,
  "bound_component_digest_sha256" text NOT NULL CHECK (bound_component_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid", "constituent_ordinal", "evidence_kind", "evidence_ordinal"),
  CHECK (NOT decisive_signed_evidence OR full_signed_pre_signature_match),
  CHECK (evidence_kind <> 'FULL_SIGNED_PRE_SIGNATURE' OR (full_signed_pre_signature_match AND component_fallback = 'WORKED_TIME_AMOUNT' AND authoritative_truth_ex_vat = '0.00' AND authoritative_baseline_ex_vat::numeric < 0)),
  CHECK (evidence_kind <> 'DECISIVE_SIGNED_EVIDENCE' OR (decisive_signed_evidence AND full_signed_pre_signature_match AND component_fallback = 'WORKED_TIME_AMOUNT' AND authoritative_truth_ex_vat = '0.00' AND authoritative_baseline_ex_vat::numeric < 0 AND authoritative_reserved_ex_vat::numeric >= 0 AND authoritative_outstanding_ex_vat::numeric > 0 AND authoritative_outstanding_ex_vat::numeric = authoritative_truth_ex_vat::numeric - authoritative_baseline_ex_vat::numeric - authoritative_reserved_ex_vat::numeric AND component_amount_ex_vat = authoritative_outstanding_ex_vat AND source_pay_ex_vat = authoritative_outstanding_ex_vat AND source_charge_ex_vat = '0.00' AND btrim(financial_revision_digest) <> '' AND btrim(target_authority_digest) <> '' AND btrim(conversion_context_digest) <> '' AND btrim(physical_bucket_key) <> '' AND btrim(physical_bucket_digest) <> '' AND btrim(sealed_evidence_digest) <> '' AND btrim(source_pay_method) <> '' AND btrim(target_pay_method) <> '')),
  FOREIGN KEY (certificate_uuid, constituent_ordinal) REFERENCES private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, constituent_ordinal) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_component_evidence_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_component_evidence_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_component_evidence_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_cert_filter_scope_manifest_v8" (
  "certificate_uuid" uuid NOT NULL,
  "candidate_filter_id" uuid NULL,
  "client_filter_id" uuid NULL,
  "filter_binding_mode" text NOT NULL CHECK (filter_binding_mode = 'EXACT_CERTIFIED_SELECTED_UNIVERSE'),
  "filter_context_digest_sha256" text NOT NULL CHECK (filter_context_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "constituent_count" integer NOT NULL CHECK (constituent_count BETWEEN 1 AND 50000),
  "selected_constituents_digest_sha256" text NOT NULL CHECK (selected_constituents_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "partition_count" integer NOT NULL CHECK (partition_count >= 1),
  "selected_partitions_digest_sha256" text NOT NULL CHECK (selected_partitions_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "canonical_amount_ex_vat_total" text NOT NULL CHECK (canonical_amount_ex_vat_total ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "manifest_digest_sha256" text NOT NULL CHECK (manifest_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid"),
  UNIQUE (certificate_uuid, candidate_filter_id, client_filter_id),
  CHECK (candidate_filter_id IS NOT NULL OR client_filter_id IS NOT NULL OR filter_binding_mode = 'EXACT_CERTIFIED_SELECTED_UNIVERSE'),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_cert_filter_scope_manifest_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_cert_filter_scope_manifest_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_cert_filter_scope_manifest_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_partitions_v8" (
  "certificate_uuid" uuid NOT NULL,
  "partition_ordinal" integer NOT NULL CHECK (partition_ordinal >= 0),
  "candidate_id" uuid NOT NULL,
  "resolved_pay_channel" text NOT NULL CHECK (resolved_pay_channel IN ('PAYE','UMBRELLA')),
  "constituent_count" integer NOT NULL CHECK (constituent_count BETWEEN 1 AND 50000),
  "canonical_amount_ex_vat_total" text NOT NULL CHECK (canonical_amount_ex_vat_total ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "partition_digest_sha256" text NOT NULL CHECK (partition_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid", "partition_ordinal"),
  UNIQUE (certificate_uuid, candidate_id, resolved_pay_channel),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_partitions_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_partitions_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_partitions_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_partition_members_v8" (
  "certificate_uuid" uuid NOT NULL,
  "stream_ordinal" integer NOT NULL CHECK (stream_ordinal BETWEEN 0 AND 49999),
  "partition_ordinal" integer NOT NULL CHECK (partition_ordinal >= 0),
  "member_ordinal" integer NOT NULL CHECK (member_ordinal >= 0),
  "constituent_ordinal" integer NOT NULL CHECK (constituent_ordinal BETWEEN 0 AND 49999),
  "stable_identity_digest_sha256" text NOT NULL CHECK (stable_identity_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid", "partition_ordinal", "member_ordinal"),
  UNIQUE (certificate_uuid, stream_ordinal),
  UNIQUE (certificate_uuid, constituent_ordinal),
  FOREIGN KEY (certificate_uuid, partition_ordinal) REFERENCES private.banking_pay_workbench_settled_certificate_partitions_v8(certificate_uuid, partition_ordinal) ON DELETE RESTRICT,
  FOREIGN KEY (certificate_uuid, constituent_ordinal) REFERENCES private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, constituent_ordinal) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_partition_members_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_partition_members_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_partition_members_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_channel_manifests_v8" (
  "certificate_uuid" uuid NOT NULL,
  "pay_channel_scope" text NOT NULL CHECK (pay_channel_scope IN ('ALL','PAYE','UMBRELLA')),
  "constituent_count" integer NOT NULL CHECK (constituent_count BETWEEN 0 AND 50000),
  "partition_count" integer NOT NULL CHECK (partition_count >= 0),
  "canonical_amount_ex_vat_total" text NOT NULL CHECK (canonical_amount_ex_vat_total ~ '^-?(0|[1-9][0-9]*)\.[0-9]{2}$'),
  "selected_constituents_digest_sha256" text NOT NULL CHECK (selected_constituents_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "selected_partitions_digest_sha256" text NOT NULL CHECK (selected_partitions_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "manifest_digest_sha256" text NOT NULL CHECK (manifest_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid", "pay_channel_scope"),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_channel_manifests_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_channel_manifests_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_channel_manifests_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_policy_owners_v8" (
  "certificate_uuid" uuid NOT NULL,
  "owner_ordinal" integer NOT NULL CHECK (owner_ordinal >= 0),
  "logical_owner_identity" text NOT NULL CHECK (btrim(logical_owner_identity) <> ''),
  PRIMARY KEY ("certificate_uuid", "owner_ordinal"),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_policy_owners_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_policy_owners_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_policy_owners_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_policy_surfaces_v8" (
  "certificate_uuid" uuid NOT NULL,
  "surface_ordinal" integer NOT NULL CHECK (surface_ordinal >= 0),
  "compared_surface" text NOT NULL CHECK (btrim(compared_surface) <> ''),
  PRIMARY KEY ("certificate_uuid", "surface_ordinal"),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_policy_surfaces_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_policy_surfaces_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_policy_surfaces_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_digest_runs_v8" (
  "digest_run_uuid" uuid NOT NULL DEFAULT gen_random_uuid(),
  "certificate_uuid" uuid NOT NULL,
  "stream_kind" text NOT NULL,
  "pay_channel_scope" text NOT NULL CHECK (pay_channel_scope IN ('ALL','PAYE','UMBRELLA')),
  "hash_contract" text NOT NULL CHECK (hash_contract IN ('V1_STABLE_STRINGIFY_CONSTITUENTS','V1_STABLE_STRINGIFY_PARTITIONS','V2_STABLE_STRINGIFY_OVERALL')),
  "status" text NOT NULL CHECK (status IN ('BUILDING','COMPLETE','FAILED')),
  "next_ordinal" integer NOT NULL CHECK (next_ordinal >= 0),
  "stream_phase" text NOT NULL DEFAULT 'OPEN' CHECK (btrim(stream_phase) <> ''),
  "current_partition_ordinal" integer NULL CHECK (current_partition_ordinal IS NULL OR current_partition_ordinal >= 0),
  "current_member_ordinal" integer NULL CHECK (current_member_ordinal IS NULL OR current_member_ordinal >= 0),
  "current_state_json" jsonb NOT NULL CHECK (jsonb_typeof(current_state_json) = 'object'),
  "last_page_receipt_sha256" text NULL CHECK (last_page_receipt_sha256 IS NULL OR last_page_receipt_sha256 ~ '^[0-9a-f]{64}$'),
  "final_digest_sha256" text NULL CHECK (final_digest_sha256 IS NULL OR final_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("digest_run_uuid"),
  UNIQUE (certificate_uuid, stream_kind, pay_channel_scope),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_digest_runs_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_digest_runs_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_digest_runs_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_digest_checkpoints_v8" (
  "digest_run_uuid" uuid NOT NULL,
  "checkpoint_sequence" integer NOT NULL CHECK (checkpoint_sequence >= 0),
  "after_ordinal" integer NULL CHECK (after_ordinal IS NULL OR after_ordinal >= 0),
  "stream_phase" text NOT NULL CHECK (btrim(stream_phase) <> ''),
  "partition_ordinal" integer NULL CHECK (partition_ordinal IS NULL OR partition_ordinal >= 0),
  "nested_array_kind" text NULL CHECK (nested_array_kind IS NULL OR nested_array_kind IN ('ORDERED_CONSTITUENT_ORDINALS','ORDERED_CONSTITUENT_IDENTITY_DIGESTS')),
  "member_ordinal" integer NULL CHECK (member_ordinal IS NULL OR member_ordinal >= 0),
  "state_before_json" jsonb NOT NULL CHECK (jsonb_typeof(state_before_json) = 'object'),
  "state_after_json" jsonb NOT NULL CHECK (jsonb_typeof(state_after_json) = 'object'),
  "page_receipt_sha256" text NOT NULL CHECK (page_receipt_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("digest_run_uuid", "checkpoint_sequence"),
  UNIQUE NULLS NOT DISTINCT (digest_run_uuid, after_ordinal),
  UNIQUE NULLS NOT DISTINCT (digest_run_uuid, partition_ordinal, nested_array_kind, member_ordinal),
  FOREIGN KEY (digest_run_uuid) REFERENCES private.banking_pay_workbench_settled_certificate_digest_runs_v8(digest_run_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_digest_checkpoints_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_digest_checkpoints_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_digest_checkpoints_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_page_receipts_v8" (
  "certificate_uuid" uuid NOT NULL,
  "request_scope_key" text NOT NULL CHECK (btrim(request_scope_key) <> ''),
  "page_kind" text NOT NULL,
  "pay_channel_scope" text NOT NULL CHECK (pay_channel_scope IN ('ALL','PAYE','UMBRELLA')),
  "page_sequence" integer NOT NULL CHECK (page_sequence >= 0),
  "after_ordinal" integer NULL CHECK (after_ordinal IS NULL OR after_ordinal >= 0),
  "requested_limit" integer NOT NULL CHECK (requested_limit BETWEEN 1 AND 256),
  "expected_previous_receipt_sha256" text NULL CHECK (expected_previous_receipt_sha256 IS NULL OR expected_previous_receipt_sha256 ~ '^[0-9a-f]{64}$'),
  "request_preimage_digest_sha256" text NOT NULL CHECK (request_preimage_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "row_count" integer NOT NULL CHECK (row_count BETWEEN 0 AND 256),
  "canonical_byte_count" integer NOT NULL CHECK (canonical_byte_count BETWEEN 0 AND 524288),
  "next_after_ordinal" integer NULL CHECK (next_after_ordinal IS NULL OR next_after_ordinal >= 0),
  "has_more" boolean NOT NULL,
  "terminal_sentinel_present" boolean NOT NULL CHECK (terminal_sentinel_present),
  "page_digest_sha256" text NOT NULL CHECK (page_digest_sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY ("certificate_uuid", "request_scope_key", "page_kind", "pay_channel_scope", "page_sequence"),
  UNIQUE NULLS NOT DISTINCT (certificate_uuid, request_scope_key, page_kind, pay_channel_scope, after_ordinal),
  CHECK ((page_sequence = 0 AND after_ordinal IS NULL AND expected_previous_receipt_sha256 IS NULL) OR (page_sequence > 0 AND after_ordinal IS NOT NULL AND expected_previous_receipt_sha256 IS NOT NULL)),
  CHECK (NOT has_more OR next_after_ordinal IS NOT NULL),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_page_receipts_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_page_receipts_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_page_receipts_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_lifecycle_events_v8" (
  "certificate_uuid" uuid NOT NULL,
  "event_sequence" bigint NOT NULL CHECK (event_sequence >= 0),
  "event_kind" text NOT NULL,
  "reason_code" text NOT NULL,
  "actor_user_id" uuid NULL,
  "related_certificate_uuid" uuid NULL,
  "event_at_utc" timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY ("certificate_uuid", "event_sequence"),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT,
  FOREIGN KEY (related_certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_lifecycle_events_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_lifecycle_events_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_lifecycle_events_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_operation_links_v8" (
  "operation_id" uuid NOT NULL,
  "certificate_uuid" uuid NOT NULL,
  "certification_id" text NOT NULL,
  "overall_digest_sha256" text NOT NULL CHECK (overall_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "pay_channel_scope" text NOT NULL CHECK (pay_channel_scope IN ('ALL','PAYE','UMBRELLA')),
  "idempotency_key" text NOT NULL,
  "admission_request_digest_sha256" text NOT NULL CHECK (admission_request_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "channel_manifest_digest_sha256" text NOT NULL CHECK (channel_manifest_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "link_state" text NOT NULL CHECK (link_state IN ('ADMITTED','STAGING','FROZEN','TERMINAL_FAILED','TERMINAL_COMPLETE')),
  "linked_at_utc" timestamptz NOT NULL DEFAULT clock_timestamp(),
  "candidate_filter_id" uuid NULL,
  "client_filter_id" uuid NULL,
  "filter_context_digest_sha256" text NOT NULL CHECK (filter_context_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "filter_scope_manifest_digest_sha256" text NOT NULL CHECK (filter_scope_manifest_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "same_week_paye_override_reason" text NULL CHECK (same_week_paye_override_reason IS NULL OR (char_length(btrim(same_week_paye_override_reason)) BETWEEN 1 AND 2000)),
  "same_week_paye_override_reauth_purpose" text NULL CHECK (same_week_paye_override_reauth_purpose IS NULL OR same_week_paye_override_reauth_purpose = 'PAYE_SAME_WEEK_OVERRIDE'),
  "same_week_paye_override_continue" boolean NOT NULL DEFAULT false,
  "same_week_paye_override_verified" boolean NOT NULL DEFAULT false,
  "same_week_paye_override_used" boolean NOT NULL DEFAULT false,
  "same_week_paye_override_pay_date" date NOT NULL,
  "same_week_paye_override_pay_week_start" date NOT NULL,
  "same_week_paye_override_pay_week_end" date NOT NULL,
  "same_week_paye_override_guardrail_code" text NULL CHECK (same_week_paye_override_guardrail_code IS NULL OR same_week_paye_override_guardrail_code = 'PAYE_SAME_WEEK_OVERRIDE_REQUIRED'),
  "same_week_paye_override_digest_sha256" text NOT NULL CHECK (same_week_paye_override_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "same_week_paye_override_verified_by_user_id" uuid NULL,
  "same_week_paye_override_verified_at_utc" timestamptz NULL,
  PRIMARY KEY ("operation_id"),
  UNIQUE (certificate_uuid, idempotency_key),
  CHECK (certification_id = 'WORKBENCH_SETTLED_CERTIFICATION_V2:' || overall_digest_sha256),
  CHECK (same_week_paye_override_pay_week_start <= same_week_paye_override_pay_date AND same_week_paye_override_pay_date <= same_week_paye_override_pay_week_end),
  CHECK (
    (same_week_paye_override_used IS FALSE AND same_week_paye_override_verified IS FALSE
      AND same_week_paye_override_reason IS NULL AND same_week_paye_override_reauth_purpose IS NULL
      AND same_week_paye_override_verified_by_user_id IS NULL AND same_week_paye_override_verified_at_utc IS NULL)
    OR
    (same_week_paye_override_used IS TRUE AND same_week_paye_override_continue IS TRUE
      AND same_week_paye_override_verified IS TRUE AND same_week_paye_override_reason IS NOT NULL
      AND same_week_paye_override_reauth_purpose = 'PAYE_SAME_WEEK_OVERRIDE'
      AND same_week_paye_override_verified_by_user_id IS NOT NULL AND same_week_paye_override_verified_at_utc IS NOT NULL)
  ),
  FOREIGN KEY (operation_id) REFERENCES public.banking_pay_operations(id) ON DELETE RESTRICT,
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_operation_links_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_operation_links_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_operation_links_v8" TO postgres;

CREATE TABLE "private"."banking_pay_workbench_settled_certificate_audit_attestations_v8" (
  "attestation_uuid" uuid NOT NULL DEFAULT gen_random_uuid(),
  "certificate_uuid" uuid NOT NULL,
  "attestation_kind" text NOT NULL,
  "non_gating_identity_json" jsonb NOT NULL CHECK (jsonb_typeof(non_gating_identity_json) = 'object'),
  "attestation_digest_sha256" text NOT NULL CHECK (attestation_digest_sha256 ~ '^[0-9a-f]{64}$'),
  "recorded_at_utc" timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY ("attestation_uuid"),
  FOREIGN KEY (certificate_uuid) REFERENCES private.banking_pay_workbench_settled_certificates_v8(certificate_uuid) ON DELETE RESTRICT
);

ALTER TABLE "private"."banking_pay_workbench_settled_certificate_audit_attestations_v8" OWNER TO postgres;
REVOKE ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_audit_attestations_v8" FROM PUBLIC, anon, authenticated, service_role;
GRANT ALL ON TABLE "private"."banking_pay_workbench_settled_certificate_audit_attestations_v8" TO postgres;

CREATE UNIQUE INDEX banking_pay_wb_cert_v8_external_id_uq ON private.banking_pay_workbench_settled_certificates_v8(certification_id) WHERE certification_id IS NOT NULL;

CREATE INDEX banking_pay_wb_cert_v8_current_session_idx ON private.banking_pay_workbench_settled_certificates_v8(workbench_session_id, session_version, progress_counter_version, authority_fence_generation) WHERE lifecycle = 'SEALED_CURRENT';

CREATE INDEX banking_pay_wb_cert_v8_build_claim_idx ON private.banking_pay_workbench_settled_certificates_v8(lifecycle, lease_expires_at_utc, build_id);

CREATE INDEX banking_pay_wb_cert_v8_overall_digest_idx ON private.banking_pay_workbench_settled_certificates_v8(overall_digest_sha256) WHERE overall_digest_sha256 IS NOT NULL;

CREATE INDEX banking_pay_wb_cert_source_members_v8_page_idx ON private.banking_pay_workbench_settled_certificate_source_members_v8(certificate_uuid, constituent_ordinal, preview_row_id);

CREATE INDEX banking_pay_wb_cert_publications_v8_candidate_idx ON private.banking_pay_workbench_settled_certificate_publications_v8(certificate_uuid, candidate_id, scope_ordinal);

CREATE INDEX banking_pay_wb_cert_universe_members_v8_page_idx ON private.banking_pay_workbench_settled_certificate_universe_members_v8(certificate_uuid, universe_kind, member_ordinal);

CREATE INDEX banking_pay_wb_cert_entries_v8_page_idx ON private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, constituent_ordinal);

CREATE INDEX banking_pay_wb_cert_entries_v8_channel_page_idx ON private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, resolved_pay_channel, constituent_ordinal);

CREATE INDEX banking_pay_wb_cert_entries_v8_candidate_channel_page_idx ON private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, candidate_id, resolved_pay_channel, constituent_ordinal);

CREATE INDEX banking_pay_wb_cert_superseded_v8_page_idx ON private.banking_pay_workbench_settled_certificate_superseded_sources_v8(certificate_uuid, constituent_ordinal, source_ordinal);

CREATE INDEX banking_pay_wb_cert_reservations_v8_page_idx ON private.banking_pay_workbench_settled_cert_source_reservations_v8(certificate_uuid, constituent_ordinal, reservation_ordinal);

CREATE INDEX banking_pay_wb_cert_components_v8_page_idx ON private.banking_pay_workbench_settled_certificate_component_evidence_v8(certificate_uuid, constituent_ordinal, evidence_kind, evidence_ordinal);

CREATE INDEX banking_pay_wb_cert_partitions_v8_page_idx ON private.banking_pay_workbench_settled_certificate_partitions_v8(certificate_uuid, partition_ordinal);

CREATE INDEX banking_pay_wb_cert_partitions_v8_channel_page_idx ON private.banking_pay_workbench_settled_certificate_partitions_v8(certificate_uuid, resolved_pay_channel, partition_ordinal);

CREATE INDEX banking_pay_wb_cert_members_v8_page_idx ON private.banking_pay_workbench_settled_certificate_partition_members_v8(certificate_uuid, partition_ordinal, member_ordinal);

CREATE INDEX banking_pay_wb_cert_members_v8_stream_idx ON private.banking_pay_workbench_settled_certificate_partition_members_v8(certificate_uuid, stream_ordinal, partition_ordinal, member_ordinal);

CREATE INDEX banking_pay_wb_cert_manifests_v8_scope_idx ON private.banking_pay_workbench_settled_certificate_channel_manifests_v8(certificate_uuid, pay_channel_scope);

CREATE INDEX banking_pay_wb_cert_links_v8_cert_operation_idx ON private.banking_pay_workbench_settled_certificate_operation_links_v8(certificate_uuid, operation_id);

CREATE UNIQUE INDEX banking_pay_wb_cert_v8_filter_manifest_uq ON private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8(certificate_uuid);

CREATE INDEX banking_pay_wb_cert_v8_filter_lookup_idx ON private.banking_pay_workbench_settled_certificates_v8(candidate_filter_id,client_filter_id,certificate_uuid);

-- The readiness/exclusion universe is captured in bounded keyset pages after
-- build admission.  This index avoids an all-session sort and deliberately
-- leads with the one session authority rather than Candidate locks.
CREATE INDEX banking_pay_wb_preview_cert_v8_universe_page_idx
  ON public.banking_pay_workbench_preview_rows(session_id, session_version, row_ordinal, id);

-- One producer may build or expose one certificate for one exact immutable
-- Workbench authority generation.  This is also the fail-closed guarantee
-- behind current-reference issuance: callers never choose "latest".
CREATE UNIQUE INDEX banking_pay_wb_cert_v8_exact_active_authority_uq
  ON private.banking_pay_workbench_settled_certificates_v8(
    workbench_session_id,
    session_version,
    progress_counter_version,
    authority_fence_generation
  )
  WHERE lifecycle IN ('BUILDING','SEALED_CURRENT');
