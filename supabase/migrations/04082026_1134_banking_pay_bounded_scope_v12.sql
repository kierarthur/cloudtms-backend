-- Banking Pay bounded-scope integration V1.2.4: schema foundation.
--
-- TEST-only implementation artifact. This migration deliberately contains no
-- bootstrap DML and must be installed before the matching repeatable runtime.
-- Policy X boundary: these objects hold pre-draft freshness/orchestration
-- evidence only. Frozen post-draft payment authority remains unchanged.

BEGIN;

-- The shared private schema already exists in TEST. Do not change its owner,
-- ACL, default privileges, or unrelated objects.

CREATE TABLE private.banking_pay_workbench_candidate_scope_registry (
  candidate_id uuid NOT NULL,
  initialisation_status text NOT NULL DEFAULT 'UNINITIALISED',
  dirty_generation bigint NOT NULL DEFAULT 0,
  evaluated_generation bigint NOT NULL DEFAULT 0,
  current_source_change_seq bigint NOT NULL DEFAULT 0,
  current_build_id uuid NULL,
  bootstrap_id uuid NULL,
  bootstrap_stream text NULL,
  bootstrap_cursor_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  bootstrap_rows_seen bigint NOT NULL DEFAULT 0,
  bootstrap_timesheets_registered bigint NOT NULL DEFAULT 0,
  bootstrap_captured_generation bigint NULL,
  bootstrap_captured_source_change_seq bigint NULL,
  last_dirty_reason text NOT NULL DEFAULT 'INITIAL_UNCLASSIFIED',
  last_scope_change_tx_token uuid NULL,
  last_dirtied_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  last_evaluated_at_utc timestamptz NULL,
  initialised_at_utc timestamptz NULL,
  failure_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),

  -- DB-CONSTRAINT-001
  CONSTRAINT bpay_wb_scope_registry_pkey PRIMARY KEY (candidate_id),
  -- DB-CONSTRAINT-002
  CONSTRAINT bpay_wb_scope_registry_candidate_fk
    FOREIGN KEY (candidate_id) REFERENCES public.candidates(id) ON DELETE CASCADE,
  -- DB-CONSTRAINT-004
  CONSTRAINT bpay_wb_scope_registry_status_chk CHECK (
    initialisation_status IN ('UNINITIALISED','DISCOVERING','CLASSIFYING','READY','FAILED')
  ),
  -- DB-CONSTRAINT-005
  CONSTRAINT bpay_wb_scope_registry_counts_chk CHECK (
    dirty_generation >= 0
    AND evaluated_generation >= 0
    AND evaluated_generation <= dirty_generation
    AND current_source_change_seq >= 0
    AND bootstrap_rows_seen >= 0
    AND bootstrap_timesheets_registered >= 0
    AND (bootstrap_captured_generation IS NULL OR bootstrap_captured_generation >= 0)
    AND (bootstrap_captured_source_change_seq IS NULL OR bootstrap_captured_source_change_seq >= 0)
  ),
  -- DB-CONSTRAINT-006
  CONSTRAINT bpay_wb_scope_registry_json_chk CHECK (
    jsonb_typeof(bootstrap_cursor_json) = 'object'
    AND jsonb_typeof(failure_json) = 'object'
    AND btrim(last_dirty_reason) <> ''
  ),
  -- DB-CONSTRAINT-007
  CONSTRAINT bpay_wb_scope_registry_bootstrap_chk CHECK (
    initialisation_status NOT IN ('DISCOVERING','CLASSIFYING')
    OR (
      bootstrap_id IS NOT NULL
      AND NULLIF(btrim(bootstrap_stream),'') IS NOT NULL
      AND bootstrap_captured_generation IS NOT NULL
      AND bootstrap_captured_source_change_seq IS NOT NULL
    )
  ),
  -- DB-CONSTRAINT-008
  CONSTRAINT bpay_wb_scope_registry_terminal_chk CHECK (
    (initialisation_status <> 'READY' OR (
      initialised_at_utc IS NOT NULL
      AND evaluated_generation = dirty_generation
      AND failure_json = '{}'::jsonb
    ))
    AND (initialisation_status <> 'FAILED' OR failure_json <> '{}'::jsonb)
    AND updated_at_utc >= created_at_utc
    AND last_dirtied_at_utc >= created_at_utc
    AND (last_evaluated_at_utc IS NULL OR last_evaluated_at_utc >= created_at_utc)
    AND (initialised_at_utc IS NULL OR initialised_at_utc >= created_at_utc)
  )
);

CREATE TABLE private.banking_pay_workbench_timesheet_scope_state (
  timesheet_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  economic_state text NOT NULL DEFAULT 'DIRTY',
  dirty_generation bigint NOT NULL DEFAULT 0,
  evaluated_generation bigint NULL,
  current_input_fingerprint text NULL,
  evaluated_input_fingerprint text NULL,
  last_dirty_reason text NOT NULL,
  last_scope_change_tx_token uuid NULL,
  current_build_id uuid NULL,
  registered_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  last_dirtied_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  last_evaluated_at_utc timestamptz NULL,
  closed_at_utc timestamptz NULL,
  updated_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),

  -- DB-CONSTRAINT-009
  CONSTRAINT bpay_wb_timesheet_scope_state_pkey PRIMARY KEY (timesheet_id),
  -- DB-CONSTRAINT-010
  CONSTRAINT bpay_wb_timesheet_scope_state_timesheet_fk
    FOREIGN KEY (timesheet_id) REFERENCES public.timesheets(timesheet_id) ON DELETE CASCADE,
  -- DB-CONSTRAINT-011
  CONSTRAINT bpay_wb_timesheet_scope_state_candidate_fk
    FOREIGN KEY (candidate_id) REFERENCES public.candidates(id) ON DELETE CASCADE,
  -- DB-CONSTRAINT-013
  CONSTRAINT bpay_wb_timesheet_scope_state_status_chk CHECK (
    economic_state IN ('DIRTY','LIVE','CLOSED')
  ),
  -- DB-CONSTRAINT-014
  CONSTRAINT bpay_wb_timesheet_scope_state_generation_chk CHECK (
    dirty_generation >= 0
    AND (evaluated_generation IS NULL OR (
      evaluated_generation >= 0 AND evaluated_generation <= dirty_generation
    ))
  ),
  -- DB-CONSTRAINT-015
  CONSTRAINT bpay_wb_timesheet_scope_state_fingerprint_chk CHECK (
    (current_input_fingerprint IS NULL OR current_input_fingerprint ~ '^[0-9a-f]{32}$')
    AND (evaluated_input_fingerprint IS NULL OR evaluated_input_fingerprint ~ '^[0-9a-f]{32}$')
  ),
  -- DB-CONSTRAINT-016
  CONSTRAINT bpay_wb_timesheet_scope_state_evaluated_chk CHECK (
    economic_state = 'DIRTY'
    OR (
      evaluated_generation = dirty_generation
      AND current_input_fingerprint IS NOT NULL
      AND evaluated_input_fingerprint = current_input_fingerprint
      AND last_evaluated_at_utc IS NOT NULL
    )
  ),
  -- DB-CONSTRAINT-017
  CONSTRAINT bpay_wb_timesheet_scope_state_lifecycle_chk CHECK (
    btrim(last_dirty_reason) <> ''
    AND ((economic_state = 'CLOSED') = (closed_at_utc IS NOT NULL))
    AND last_dirtied_at_utc >= registered_at_utc
    AND updated_at_utc >= registered_at_utc
    AND (last_evaluated_at_utc IS NULL OR last_evaluated_at_utc >= registered_at_utc)
    AND (closed_at_utc IS NULL OR closed_at_utc >= registered_at_utc)
  )
);

CREATE TABLE private.banking_pay_workbench_economic_builds (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  build_token uuid NOT NULL DEFAULT gen_random_uuid(),
  candidate_id uuid NOT NULL,
  session_id uuid NOT NULL,
  session_version bigint NOT NULL,
  source_snapshot_run_id uuid NULL,
  source_build_run_id uuid NOT NULL,
  source_job_id uuid NULL,
  captured_candidate_generation bigint NOT NULL,
  source_change_seq bigint NOT NULL,
  status text NOT NULL,
  private_stage text NOT NULL,
  stage_version integer NOT NULL DEFAULT 1,
  scope_cursor_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  closure_cursor_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  fact_cursor_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  publication_cursor_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  cleanup_cursor_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  seed_scope_count integer NOT NULL DEFAULT 0,
  seed_scope_digest text NULL,
  seed_scope_sealed_at_utc timestamptz NULL,
  scope_count integer NOT NULL DEFAULT 0,
  dependency_node_count integer NOT NULL DEFAULT 0,
  dependency_edge_count bigint NOT NULL DEFAULT 0,
  dependency_edge_stream_digest text NULL,
  dependency_edge_stream_terminal_key_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  dependency_edge_stream_complete boolean NOT NULL DEFAULT false,
  tagged_edge_count bigint NOT NULL DEFAULT 0,
  edge_tag_stream_terminal_key_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  edge_tag_digest text NULL,
  edge_tag_stream_complete boolean NOT NULL DEFAULT false,
  unit_count integer NOT NULL DEFAULT 0,
  unit_digest text NULL,
  row_seal_count bigint NOT NULL DEFAULT 0,
  last_stable_ordinal bigint NOT NULL DEFAULT 0,
  sealed_fingerprint_digest text NULL,
  fact_count bigint NOT NULL DEFAULT 0,
  canonical_count integer NOT NULL DEFAULT 0,
  scope_digest text NULL,
  dependency_digest text NULL,
  pre_sync_digest text NULL,
  post_sync_digest text NULL,
  canonical_digest text NULL,
  complexity_vector_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  envelope_version integer NULL,
  envelope_snapshot_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  envelope_evidence_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  attestation_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  failure_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  dependency_closure_sealed_at_utc timestamptz NULL,
  cleanup_not_before_utc timestamptz NULL,
  created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  ready_at_utc timestamptz NULL,
  reconciled_at_utc timestamptz NULL,
  completed_at_utc timestamptz NULL,
  obsolete_at_utc timestamptz NULL,
  failed_at_utc timestamptz NULL,

  -- DB-CONSTRAINT-018
  CONSTRAINT bpay_wb_economic_builds_pkey PRIMARY KEY (id),
  -- DB-CONSTRAINT-019
  CONSTRAINT bpay_wb_economic_builds_token_uq UNIQUE (build_token),
  -- DB-CONSTRAINT-020
  CONSTRAINT bpay_wb_economic_builds_candidate_fk
    FOREIGN KEY (candidate_id) REFERENCES public.candidates(id) ON DELETE CASCADE,
  -- DB-CONSTRAINT-021
  CONSTRAINT bpay_wb_economic_builds_session_fk
    FOREIGN KEY (session_id) REFERENCES public.banking_pay_workbench_sessions(id) ON DELETE RESTRICT,
  -- DB-CONSTRAINT-023
  CONSTRAINT bpay_wb_economic_builds_source_job_fk
    FOREIGN KEY (source_job_id) REFERENCES public.banking_pay_workbench_jobs(id)
    ON DELETE SET NULL DEFERRABLE INITIALLY IMMEDIATE,
  -- DB-CONSTRAINT-024
  CONSTRAINT bpay_wb_economic_builds_status_chk CHECK (status IN (
    'COLLECTING','READY_FOR_RECONCILIATION','RECONCILING','RECONCILED',
    'PUBLISHING','COMPLETE','OBSOLETE','FAILED',
    'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE','CLEANING'
  )),
  -- DB-CONSTRAINT-025
  CONSTRAINT bpay_wb_economic_builds_stage_chk CHECK (private_stage IN (
    'PREPARE_SCOPE','DEPENDENCY_CLOSURE','WORKSPACE_FACT','RECONCILE_EXECUTE',
    'SOURCE_PUBLISH','BOOTSTRAP_DISCOVERY','BUILD_CLEANUP','COMPLETE'
  )),
  -- DB-CONSTRAINT-026
  CONSTRAINT bpay_wb_economic_builds_counts_chk CHECK (
    session_version > 0 AND stage_version > 0
    AND captured_candidate_generation >= 0 AND source_change_seq >= 0
    AND seed_scope_count >= 0 AND scope_count >= 0 AND dependency_node_count >= 0
    AND dependency_edge_count >= 0 AND tagged_edge_count >= 0
    AND unit_count >= 0 AND row_seal_count >= 0 AND last_stable_ordinal >= 0
    AND fact_count >= 0 AND canonical_count >= 0
    AND (envelope_version IS NULL OR envelope_version > 0)
  ),
  -- DB-CONSTRAINT-027
  CONSTRAINT bpay_wb_economic_builds_json_chk CHECK (
    jsonb_typeof(scope_cursor_json) = 'object'
    AND jsonb_typeof(closure_cursor_json) = 'object'
    AND jsonb_typeof(fact_cursor_json) = 'object'
    AND jsonb_typeof(publication_cursor_json) = 'object'
    AND jsonb_typeof(cleanup_cursor_json) = 'object'
    AND jsonb_typeof(dependency_edge_stream_terminal_key_json) = 'object'
    AND jsonb_typeof(edge_tag_stream_terminal_key_json) = 'object'
    AND jsonb_typeof(complexity_vector_json) = 'object'
    AND jsonb_typeof(envelope_snapshot_json) = 'object'
    AND jsonb_typeof(envelope_evidence_json) = 'object'
    AND jsonb_typeof(attestation_json) = 'object'
    AND jsonb_typeof(failure_json) = 'object'
  ),
  -- DB-CONSTRAINT-028
  CONSTRAINT bpay_wb_economic_builds_digest_chk CHECK (
    (seed_scope_digest IS NULL OR seed_scope_digest ~ '^[0-9a-f]{32}$')
    AND (dependency_edge_stream_digest IS NULL OR dependency_edge_stream_digest ~ '^[0-9a-f]{32}$')
    AND (edge_tag_digest IS NULL OR edge_tag_digest ~ '^[0-9a-f]{32}$')
    AND (unit_digest IS NULL OR unit_digest ~ '^[0-9a-f]{32}$')
    AND (sealed_fingerprint_digest IS NULL OR sealed_fingerprint_digest ~ '^[0-9a-f]{32}$')
    AND (scope_digest IS NULL OR scope_digest ~ '^[0-9a-f]{32}$')
    AND (dependency_digest IS NULL OR dependency_digest ~ '^[0-9a-f]{32}$')
    AND (pre_sync_digest IS NULL OR pre_sync_digest ~ '^[0-9a-f]{32}$')
    AND (post_sync_digest IS NULL OR post_sync_digest ~ '^[0-9a-f]{32}$')
    AND (canonical_digest IS NULL OR canonical_digest ~ '^[0-9a-f]{32}$')
  ),
  -- DB-CONSTRAINT-029
  CONSTRAINT bpay_wb_economic_builds_job_identity_chk CHECK (
    status IN ('COMPLETE','OBSOLETE','FAILED','CLEANING') OR source_job_id IS NOT NULL
  ),
  -- DB-CONSTRAINT-030
  CONSTRAINT bpay_wb_economic_builds_lifecycle_chk CHECK (
    updated_at_utc >= created_at_utc
    AND (ready_at_utc IS NULL OR ready_at_utc >= created_at_utc)
    AND (reconciled_at_utc IS NULL OR reconciled_at_utc >= COALESCE(ready_at_utc,created_at_utc))
    AND (completed_at_utc IS NULL OR completed_at_utc >= COALESCE(reconciled_at_utc,created_at_utc))
    AND (obsolete_at_utc IS NULL OR obsolete_at_utc >= created_at_utc)
    AND (failed_at_utc IS NULL OR failed_at_utc >= created_at_utc)
    AND (status <> 'READY_FOR_RECONCILIATION' OR ready_at_utc IS NOT NULL)
    AND (status NOT IN ('RECONCILED','PUBLISHING','COMPLETE') OR reconciled_at_utc IS NOT NULL)
    AND (status <> 'COMPLETE' OR completed_at_utc IS NOT NULL)
    AND (status <> 'OBSOLETE' OR obsolete_at_utc IS NOT NULL)
    AND (status <> 'FAILED' OR failed_at_utc IS NOT NULL)
    AND (status <> 'CLEANING' OR cleanup_not_before_utc IS NOT NULL)
  ),
  -- DB-CONSTRAINT-031
  CONSTRAINT bpay_wb_economic_builds_scale_block_chk CHECK (
    status <> 'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
    OR (
      complexity_vector_json <> '{}'::jsonb
      AND envelope_version IS NOT NULL
      AND envelope_snapshot_json <> '{}'::jsonb
      AND reconciled_at_utc IS NULL
      AND completed_at_utc IS NULL
      AND canonical_count = 0
      AND canonical_digest IS NULL
    )
  ),
  -- DB-CONSTRAINT-032
  CONSTRAINT bpay_wb_economic_builds_complete_chk CHECK (
    status <> 'COMPLETE'
    OR (
      private_stage = 'COMPLETE'
      AND canonical_digest IS NOT NULL
      AND attestation_json <> '{}'::jsonb
      AND completed_at_utc IS NOT NULL
    )
  ),
  -- DB-CONSTRAINT-033
  CONSTRAINT bpay_wb_economic_builds_seed_seal_chk CHECK (
    (seed_scope_sealed_at_utc IS NULL AND seed_scope_digest IS NULL)
    OR (
      seed_scope_sealed_at_utc IS NOT NULL
      AND seed_scope_digest IS NOT NULL
      AND seed_scope_count >= 0
      AND scope_cursor_json @> '{"terminal":true}'::jsonb
    )
  ),
  -- DB-CONSTRAINT-034
  CONSTRAINT bpay_wb_economic_builds_dependency_seal_chk CHECK (
    dependency_closure_sealed_at_utc IS NULL
    OR (
      seed_scope_sealed_at_utc IS NOT NULL
      AND dependency_edge_stream_complete
      AND edge_tag_stream_complete
      AND tagged_edge_count = dependency_edge_count
      AND unit_digest IS NOT NULL
      AND edge_tag_digest IS NOT NULL
      AND scope_digest IS NOT NULL
      AND dependency_digest IS NOT NULL
      AND sealed_fingerprint_digest IS NOT NULL
      AND row_seal_count = scope_count
      AND last_stable_ordinal = row_seal_count
      AND closure_cursor_json @> '{"terminal":true,"seal_phase":"COMPLETE"}'::jsonb
    )
  )
);

-- Circular current-build pointers are added after the build header exists.
ALTER TABLE private.banking_pay_workbench_candidate_scope_registry
  ADD CONSTRAINT bpay_wb_scope_registry_current_build_fk
  FOREIGN KEY (current_build_id)
  REFERENCES private.banking_pay_workbench_economic_builds(id)
  ON DELETE SET NULL DEFERRABLE INITIALLY IMMEDIATE;
-- DB-CONSTRAINT-003
COMMENT ON CONSTRAINT bpay_wb_scope_registry_current_build_fk
  ON private.banking_pay_workbench_candidate_scope_registry IS 'DB-CONSTRAINT-003';

ALTER TABLE private.banking_pay_workbench_timesheet_scope_state
  ADD CONSTRAINT bpay_wb_timesheet_scope_state_current_build_fk
  FOREIGN KEY (current_build_id)
  REFERENCES private.banking_pay_workbench_economic_builds(id)
  ON DELETE SET NULL DEFERRABLE INITIALLY IMMEDIATE;
-- DB-CONSTRAINT-012
COMMENT ON CONSTRAINT bpay_wb_timesheet_scope_state_current_build_fk
  ON private.banking_pay_workbench_timesheet_scope_state IS 'DB-CONSTRAINT-012';

CREATE TABLE private.banking_pay_workbench_economic_build_scope (
  build_id uuid NOT NULL,
  timesheet_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  stable_ordinal bigint NULL,
  root_timesheet_id uuid NULL,
  dependency_unit_anchor_timesheet_id uuid NULL,
  dependency_unit_key text NULL,
  dependency_unit_digest text NULL,
  seed_reasons text[] NOT NULL,
  dependency_reasons text[] NOT NULL,
  captured_dirty_generation bigint NOT NULL,
  captured_input_fingerprint text NULL,
  closure_status text NOT NULL DEFAULT 'PENDING',
  closure_family_ordinal smallint NOT NULL DEFAULT 1,
  closure_last_edge_key text NULL,
  closure_processed_edge_count bigint NOT NULL DEFAULT 0,
  closure_processed_emission_count bigint NOT NULL DEFAULT 0,
  required_fact_families text[] NOT NULL,
  completed_fact_families text[] NOT NULL DEFAULT ARRAY[]::text[],
  fact_row_count integer NOT NULL DEFAULT 0,
  fact_digest text NULL,
  seal_prepared_at_utc timestamptz NULL,
  created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),

  -- DB-CONSTRAINT-035
  CONSTRAINT bpay_wb_economic_build_scope_pkey PRIMARY KEY (build_id,timesheet_id),
  -- DB-CONSTRAINT-036
  CONSTRAINT bpay_wb_economic_build_scope_build_fk
    FOREIGN KEY (build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE CASCADE,
  -- DB-CONSTRAINT-037
  CONSTRAINT bpay_wb_economic_build_scope_status_chk CHECK (
    closure_status IN ('PENDING','EXPANDED','SEALING','SEALED')
  ),
  -- DB-CONSTRAINT-038
  CONSTRAINT bpay_wb_economic_build_scope_counts_chk CHECK (
    captured_dirty_generation >= 0
    AND closure_family_ordinal >= 1
    AND closure_processed_edge_count >= 0
    AND closure_processed_emission_count >= 0
    AND fact_row_count >= 0
  ),
  -- DB-CONSTRAINT-039
  CONSTRAINT bpay_wb_economic_build_scope_arrays_chk CHECK (
    array_position(seed_reasons,NULL) IS NULL
    AND array_position(dependency_reasons,NULL) IS NULL
    AND array_position(required_fact_families,NULL) IS NULL
    AND array_position(completed_fact_families,NULL) IS NULL
  ),
  -- DB-CONSTRAINT-040
  CONSTRAINT bpay_wb_economic_build_scope_family_ordinal_chk CHECK (
    closure_family_ordinal BETWEEN 1 AND 11
  ),
  -- DB-CONSTRAINT-041
  CONSTRAINT bpay_wb_economic_build_scope_pending_chk CHECK (
    closure_status <> 'PENDING'
    OR (
      stable_ordinal IS NULL
      AND dependency_unit_anchor_timesheet_id IS NULL
      AND dependency_unit_key IS NULL
      AND dependency_unit_digest IS NULL
      AND captured_input_fingerprint IS NULL
      AND seal_prepared_at_utc IS NULL
      AND cardinality(completed_fact_families) = 0
      AND fact_row_count = 0
      AND fact_digest IS NULL
    )
  ),
  -- DB-CONSTRAINT-042
  CONSTRAINT bpay_wb_economic_build_scope_expanded_chk CHECK (
    closure_status <> 'EXPANDED'
    OR (
      closure_family_ordinal = 11
      AND stable_ordinal IS NULL
      AND dependency_unit_key IS NULL
      AND dependency_unit_digest IS NULL
      AND captured_input_fingerprint IS NULL
      AND seal_prepared_at_utc IS NULL
      AND cardinality(completed_fact_families) = 0
      AND fact_row_count = 0
      AND fact_digest IS NULL
    )
  ),
  -- DB-CONSTRAINT-043
  CONSTRAINT bpay_wb_economic_build_scope_sealing_chk CHECK (
    closure_status <> 'SEALING'
    OR (
      dependency_unit_anchor_timesheet_id IS NOT NULL
      AND stable_ordinal IS NULL
      AND captured_input_fingerprint IS NULL
      AND seal_prepared_at_utc IS NULL
      AND cardinality(completed_fact_families) = 0
      AND fact_row_count = 0
      AND fact_digest IS NULL
      AND (
        dependency_unit_digest IS NULL
        OR timesheet_id = dependency_unit_anchor_timesheet_id
      )
    )
  ),
  -- DB-CONSTRAINT-044
  CONSTRAINT bpay_wb_economic_build_scope_sealed_chk CHECK (
    closure_status <> 'SEALED'
    OR (
      stable_ordinal > 0
      AND dependency_unit_anchor_timesheet_id IS NOT NULL
      AND NULLIF(btrim(dependency_unit_key),'') IS NOT NULL
      AND dependency_unit_digest ~ '^[0-9a-f]{32}$'
      AND captured_input_fingerprint ~ '^[0-9a-f]{32}$'
      AND seal_prepared_at_utc IS NOT NULL
    )
  ),
  -- DB-CONSTRAINT-045
  CONSTRAINT bpay_wb_economic_build_scope_identity_chk CHECK (
    (dependency_unit_key IS NULL OR (
      btrim(dependency_unit_key) <> '' AND dependency_unit_key <> 'GLOBAL'
    ))
    AND (dependency_unit_digest IS NULL OR dependency_unit_digest ~ '^[0-9a-f]{32}$')
    AND (captured_input_fingerprint IS NULL OR captured_input_fingerprint ~ '^[0-9a-f]{32}$')
    AND (fact_digest IS NULL OR fact_digest ~ '^[0-9a-f]{32}$')
    AND updated_at_utc >= created_at_utc
  ),
  -- DB-CONSTRAINT-046
  CONSTRAINT bpay_wb_economic_build_scope_fact_completion_chk CHECK (
    completed_fact_families <@ required_fact_families
    AND (closure_status = 'SEALED' OR (
      cardinality(completed_fact_families) = 0
      AND fact_row_count = 0
      AND fact_digest IS NULL
    ))
  )
);

CREATE TABLE private.banking_pay_workbench_economic_build_facts (
  build_id uuid NOT NULL,
  fact_family text NOT NULL,
  natural_key text NOT NULL,
  candidate_id uuid NOT NULL,
  timesheet_id uuid NULL,
  subject_timesheet_ids uuid[] NOT NULL,
  dependency_unit_key text NULL,
  source_relation text NOT NULL,
  source_id uuid NULL,
  source_subkey text NULL,
  economic_key_type text NULL,
  economic_key_value text NULL,
  amount_ex_vat numeric NULL,
  amount_inc_vat numeric NULL,
  truth_ex_vat numeric NULL,
  truth_inc_vat numeric NULL,
  baseline_ex_vat numeric NULL,
  baseline_inc_vat numeric NULL,
  reserved_source_amount numeric NULL,
  finance_case_id uuid NULL,
  finance_component_id uuid NULL,
  reservation_id uuid NULL,
  edge_kind text NULL,
  edge_from_timesheet_id uuid NULL,
  edge_to_timesheet_id uuid NULL,
  edge_source_id uuid NULL,
  payload_schema_version integer NOT NULL DEFAULT 1,
  source_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  diagnostic_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  financial_digest text NOT NULL,
  source_ordinal bigint NULL,
  created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),

  -- DB-CONSTRAINT-047
  CONSTRAINT bpay_wb_economic_build_facts_pkey PRIMARY KEY (build_id,fact_family,natural_key),
  -- DB-CONSTRAINT-048
  CONSTRAINT bpay_wb_economic_build_facts_build_fk
    FOREIGN KEY (build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE CASCADE,
  -- DB-CONSTRAINT-049
  CONSTRAINT bpay_wb_economic_build_facts_family_chk CHECK (fact_family IN (
    'DEPENDENCY_EDGE','FROZEN_SETTLED_COMPONENT','LIVE_ENTITLEMENT_INPUT',
    'ENTITLEMENT_COMPONENT','RESERVATION_COMPONENT','PAY_STATE_FALLBACK',
    'FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY','PROTECTION_EVIDENCE',
    'PAYEE_BASELINE_INPUT','ALLOCATION_INPUT','CANONICAL_INPUT'
  )),
  -- DB-CONSTRAINT-050
  CONSTRAINT bpay_wb_economic_build_facts_payload_chk CHECK (
    payload_schema_version > 0
    AND NULLIF(btrim(natural_key),'') IS NOT NULL
    AND NULLIF(btrim(source_relation),'') IS NOT NULL
    AND jsonb_typeof(source_payload_json) = 'object'
    AND jsonb_typeof(diagnostic_json) = 'object'
    AND financial_digest ~ '^[0-9a-f]{32}$'
    AND (source_ordinal IS NULL OR source_ordinal > 0)
  ),
  -- DB-CONSTRAINT-051
  CONSTRAINT bpay_wb_economic_build_facts_subjects_chk CHECK (
    array_position(subject_timesheet_ids,NULL) IS NULL
  ),
  -- DB-CONSTRAINT-052
  CONSTRAINT bpay_wb_economic_build_facts_unit_chk CHECK (
    (fact_family = 'DEPENDENCY_EDGE' AND (
      dependency_unit_key IS NULL
      OR (dependency_unit_key <> 'GLOBAL' AND btrim(dependency_unit_key) <> '')
    ))
    OR (fact_family IN (
      'RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY',
      'PROTECTION_EVIDENCE','ALLOCATION_INPUT'
    ) AND dependency_unit_key = 'GLOBAL')
    OR (fact_family IN (
      'FROZEN_SETTLED_COMPONENT','LIVE_ENTITLEMENT_INPUT','ENTITLEMENT_COMPONENT',
      'PAY_STATE_FALLBACK','PAYEE_BASELINE_INPUT','CANONICAL_INPUT'
    ) AND NULLIF(btrim(dependency_unit_key),'') IS NOT NULL AND dependency_unit_key <> 'GLOBAL')
  ),
  -- DB-CONSTRAINT-053
  CONSTRAINT bpay_wb_economic_build_facts_economic_key_chk CHECK (
    economic_key_type IS NULL
    OR (
      economic_key_type IN (
        'TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE',
        'EXPENSE_CODE','MANUAL_CARRY_FORWARD'
      )
      AND NULLIF(btrim(economic_key_value),'') IS NOT NULL
      AND (economic_key_type <> 'TS_DAY' OR (
        economic_key_value ~ '^\d{4}-\d{2}-\d{2}$'
        AND to_char(economic_key_value::date,'YYYY-MM-DD') = economic_key_value
      ))
    )
  ),
  -- DB-CONSTRAINT-054
  CONSTRAINT bpay_wb_economic_build_facts_edge_chk CHECK (
    fact_family <> 'DEPENDENCY_EDGE'
    OR (
      NULLIF(btrim(edge_kind),'') IS NOT NULL
      AND edge_from_timesheet_id IS NOT NULL
      AND edge_to_timesheet_id IS NOT NULL
      AND edge_source_id IS NOT NULL
      AND cardinality(subject_timesheet_ids) > 0
    )
  ),
  -- DB-CONSTRAINT-055
  CONSTRAINT bpay_wb_economic_build_facts_authority_chk CHECK (
    (fact_family <> 'FROZEN_SETTLED_COMPONENT' OR (
      economic_key_type IS NOT NULL AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL)
    ))
    AND (fact_family <> 'ENTITLEMENT_COMPONENT' OR (
      economic_key_type IS NOT NULL
      AND (truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL)
      AND (baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)
    ))
    AND (fact_family <> 'RESERVATION_COMPONENT' OR (
      reservation_id IS NOT NULL AND economic_key_type IS NOT NULL AND reserved_source_amount IS NOT NULL
    ))
    AND (fact_family <> 'FINANCE_CASE_IDENTITY' OR finance_case_id IS NOT NULL)
    AND (fact_family <> 'FINANCE_COMPONENT_IDENTITY' OR (
      finance_case_id IS NOT NULL AND finance_component_id IS NOT NULL
    ))
    AND (fact_family <> 'PROTECTION_EVIDENCE' OR (
      finance_case_id IS NOT NULL OR finance_component_id IS NOT NULL
      OR reservation_id IS NOT NULL OR source_id IS NOT NULL
    ))
    AND (fact_family <> 'PAYEE_BASELINE_INPUT' OR (
      economic_key_type IS NOT NULL
      AND (truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL
        OR baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)
    ))
    AND (fact_family <> 'ALLOCATION_INPUT' OR (
      economic_key_type IS NOT NULL
      AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL
        OR reserved_source_amount IS NOT NULL)
    ))
    AND (fact_family <> 'CANONICAL_INPUT' OR (
      economic_key_type IS NOT NULL
      AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL
        OR truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL
        OR baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)
    ))
  )
);

CREATE TABLE private.banking_pay_workbench_stage_attempts (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  job_id uuid NOT NULL,
  build_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  private_stage text NOT NULL,
  attempt_number integer NOT NULL,
  attempt_nonce uuid NOT NULL DEFAULT gen_random_uuid(),
  worker_id text NOT NULL,
  lane_identity text NOT NULL,
  captured_candidate_generation bigint NOT NULL,
  captured_source_change_seq bigint NOT NULL,
  execution_profile_version integer NOT NULL,
  attempt_status text NOT NULL DEFAULT 'STARTED',
  started_at_utc timestamptz NOT NULL,
  lease_expires_at_utc timestamptz NOT NULL,
  completed_at_utc timestamptz NULL,
  failed_at_utc timestamptz NULL,
  expired_at_utc timestamptz NULL,
  obsolete_at_utc timestamptz NULL,
  result_code text NULL,
  result_digest text NULL,
  error_class text NULL,
  error_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),

  -- DB-CONSTRAINT-067
  CONSTRAINT bpay_wb_stage_attempts_pkey PRIMARY KEY (id),
  -- DB-CONSTRAINT-068
  CONSTRAINT bpay_wb_stage_attempts_build_fk
    FOREIGN KEY (build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE CASCADE,
  -- DB-CONSTRAINT-069
  CONSTRAINT bpay_wb_stage_attempts_job_fk
    FOREIGN KEY (job_id) REFERENCES public.banking_pay_workbench_jobs(id) ON DELETE RESTRICT,
  -- DB-CONSTRAINT-070
  CONSTRAINT bpay_wb_stage_attempts_nonce_uq UNIQUE (attempt_nonce),
  -- DB-CONSTRAINT-071
  CONSTRAINT bpay_wb_stage_attempts_job_number_uq UNIQUE (job_id,attempt_number),
  -- DB-CONSTRAINT-072
  CONSTRAINT bpay_wb_stage_attempts_status_stage_chk CHECK (
    attempt_status IN ('STARTED','COMPLETED','FAILED','EXPIRED','OBSOLETE')
    AND private_stage IN (
      'PREPARE_SCOPE','DEPENDENCY_CLOSURE','WORKSPACE_FACT','RECONCILE_EXECUTE',
      'SOURCE_PUBLISH','BOOTSTRAP_DISCOVERY','BUILD_CLEANUP','COMPLETE'
    )
    AND btrim(worker_id) <> '' AND btrim(lane_identity) <> ''
  ),
  -- DB-CONSTRAINT-073
  CONSTRAINT bpay_wb_stage_attempts_counts_chk CHECK (
    attempt_number > 0 AND execution_profile_version > 0
    AND captured_candidate_generation >= 0 AND captured_source_change_seq >= 0
  ),
  -- DB-CONSTRAINT-074
  CONSTRAINT bpay_wb_stage_attempts_lease_chk CHECK (
    lease_expires_at_utc > started_at_utc
    AND created_at_utc <= started_at_utc
    AND updated_at_utc >= created_at_utc
  ),
  -- DB-CONSTRAINT-075
  CONSTRAINT bpay_wb_stage_attempts_outcome_chk CHECK (
    (attempt_status = 'STARTED' AND completed_at_utc IS NULL AND failed_at_utc IS NULL
      AND expired_at_utc IS NULL AND obsolete_at_utc IS NULL)
    OR (attempt_status = 'COMPLETED' AND completed_at_utc IS NOT NULL AND failed_at_utc IS NULL
      AND expired_at_utc IS NULL AND obsolete_at_utc IS NULL)
    OR (attempt_status = 'FAILED' AND completed_at_utc IS NULL AND failed_at_utc IS NOT NULL
      AND expired_at_utc IS NULL AND obsolete_at_utc IS NULL)
    OR (attempt_status = 'EXPIRED' AND completed_at_utc IS NULL AND failed_at_utc IS NULL
      AND expired_at_utc IS NOT NULL AND obsolete_at_utc IS NULL)
    OR (attempt_status = 'OBSOLETE' AND completed_at_utc IS NULL AND failed_at_utc IS NULL
      AND expired_at_utc IS NULL AND obsolete_at_utc IS NOT NULL)
  ),
  -- DB-CONSTRAINT-076
  CONSTRAINT bpay_wb_stage_attempts_result_chk CHECK (
    jsonb_typeof(error_json) = 'object'
    AND (result_digest IS NULL OR result_digest ~ '^[0-9a-f]{32}$')
    AND (result_code IS NULL OR NULLIF(btrim(result_code),'') IS NOT NULL)
    AND (error_class IS NULL OR error_class ~ '^[A-Z][A-Z0-9_]*$')
  )
);

CREATE TABLE private.banking_pay_workbench_economic_build_fact_pages (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  build_id uuid NOT NULL,
  attempt_id uuid NOT NULL,
  dependency_unit_key text NOT NULL,
  fact_family text NOT NULL,
  page_number integer NOT NULL,
  cursor_start_json jsonb NOT NULL,
  cursor_start_hash text NOT NULL,
  cursor_end_json jsonb NOT NULL,
  cursor_end_hash text NOT NULL,
  expected_source_count integer NULL,
  actual_fact_count integer NOT NULL,
  cumulative_fact_count integer NOT NULL,
  page_digest text NOT NULL,
  cumulative_digest text NOT NULL,
  is_family_final boolean NOT NULL DEFAULT false,
  status text NOT NULL DEFAULT 'COMPLETED',
  completed_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),

  -- DB-CONSTRAINT-056
  CONSTRAINT bpay_wb_economic_build_fact_pages_pkey PRIMARY KEY (id),
  -- DB-CONSTRAINT-057
  CONSTRAINT bpay_wb_economic_build_fact_pages_build_fk
    FOREIGN KEY (build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE CASCADE,
  -- DB-CONSTRAINT-058
  CONSTRAINT bpay_wb_economic_build_fact_pages_attempt_fk
    FOREIGN KEY (attempt_id) REFERENCES private.banking_pay_workbench_stage_attempts(id)
    ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,
  -- DB-CONSTRAINT-059
  CONSTRAINT bpay_wb_economic_build_fact_pages_number_uq
    UNIQUE (build_id,dependency_unit_key,fact_family,page_number),
  -- DB-CONSTRAINT-060
  CONSTRAINT bpay_wb_economic_build_fact_pages_cursor_uq
    UNIQUE (build_id,dependency_unit_key,fact_family,cursor_start_hash),
  -- DB-CONSTRAINT-061
  CONSTRAINT bpay_wb_economic_build_fact_pages_status_chk CHECK (
    status IN ('COMPLETED','CONFLICTED')
  ),
  -- DB-CONSTRAINT-062
  CONSTRAINT bpay_wb_economic_build_fact_pages_counts_chk CHECK (
    page_number > 0
    AND (expected_source_count IS NULL OR expected_source_count >= 0)
    AND actual_fact_count >= 0
    AND cumulative_fact_count >= actual_fact_count
  ),
  -- DB-CONSTRAINT-063
  CONSTRAINT bpay_wb_economic_build_fact_pages_cursor_digest_chk CHECK (
    jsonb_typeof(cursor_start_json) = 'object'
    AND jsonb_typeof(cursor_end_json) = 'object'
    AND cursor_start_hash ~ '^[0-9a-f]{32}$'
    AND cursor_end_hash ~ '^[0-9a-f]{32}$'
    AND page_digest ~ '^[0-9a-f]{32}$'
    AND cumulative_digest ~ '^[0-9a-f]{32}$'
  ),
  -- DB-CONSTRAINT-064
  CONSTRAINT bpay_wb_economic_build_fact_pages_final_chk CHECK (
    NOT is_family_final OR cursor_end_json @> '{"terminal":true}'::jsonb
  ),
  -- DB-CONSTRAINT-065
  CONSTRAINT bpay_wb_economic_build_fact_pages_completed_chk CHECK (
    completed_at_utc >= created_at_utc
  ),
  -- DB-CONSTRAINT-066
  CONSTRAINT bpay_wb_economic_build_fact_pages_unit_chk CHECK (
    (fact_family IN (
      'RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY',
      'PROTECTION_EVIDENCE','ALLOCATION_INPUT'
    ) AND dependency_unit_key = 'GLOBAL')
    OR (fact_family IN (
      'FROZEN_SETTLED_COMPONENT','LIVE_ENTITLEMENT_INPUT','ENTITLEMENT_COMPONENT',
      'PAY_STATE_FALLBACK','PAYEE_BASELINE_INPUT','CANONICAL_INPUT'
    ) AND btrim(dependency_unit_key) <> '' AND dependency_unit_key <> 'GLOBAL')
  )
);

CREATE TABLE private.banking_pay_workbench_canonical_stage_lines (
  build_id uuid NOT NULL,
  source_ordinal bigint NOT NULL,
  session_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  session_version bigint NOT NULL,
  source_change_seq bigint NOT NULL,
  source_build_run_id uuid NOT NULL,
  timesheet_id uuid NULL,
  line_key text NOT NULL,
  parent_line_key text NULL,
  split_suffix text NULL,
  section text NULL,
  source_row_json jsonb NOT NULL,
  economic_key_json jsonb NOT NULL,
  contract_json jsonb NOT NULL,
  pay_channel_scope text NOT NULL,
  refresh_scope_kind text NOT NULL,
  row_digest text NOT NULL,
  stage_status text NOT NULL DEFAULT 'STAGED',
  verified_at_utc timestamptz NULL,
  created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),

  -- DB-CONSTRAINT-077
  CONSTRAINT bpay_wb_canonical_stage_lines_pkey PRIMARY KEY (build_id,source_ordinal),
  -- DB-CONSTRAINT-078
  CONSTRAINT bpay_wb_canonical_stage_lines_build_fk
    FOREIGN KEY (build_id) REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE CASCADE,
  -- DB-CONSTRAINT-079 / DB-INDEX-035: exact PostgreSQL 17.6 null-aware identity.
  CONSTRAINT bpay_wb_canonical_stage_identity_uq
    UNIQUE NULLS NOT DISTINCT (build_id,timesheet_id,line_key),
  -- DB-CONSTRAINT-080
  CONSTRAINT bpay_wb_canonical_stage_lines_status_chk CHECK (
    stage_status IN ('STAGED','VERIFIED')
  ),
  -- DB-CONSTRAINT-081
  CONSTRAINT bpay_wb_canonical_stage_lines_identity_chk CHECK (
    source_ordinal > 0 AND btrim(line_key) <> ''
  ),
  -- DB-CONSTRAINT-082
  CONSTRAINT bpay_wb_canonical_stage_lines_json_chk CHECK (
    jsonb_typeof(source_row_json) = 'object'
    AND jsonb_typeof(economic_key_json) = 'object'
    AND jsonb_typeof(contract_json) = 'object'
    AND row_digest ~ '^[0-9a-f]{32}$'
  ),
  -- DB-CONSTRAINT-083
  CONSTRAINT bpay_wb_canonical_stage_lines_authority_chk CHECK (
    session_version > 0
    AND source_change_seq >= 0
    AND btrim(pay_channel_scope) <> ''
    AND btrim(refresh_scope_kind) <> ''
    AND ((stage_status = 'VERIFIED') = (verified_at_utc IS NOT NULL))
    AND (verified_at_utc IS NULL OR verified_at_utc >= created_at_utc)
  )
);

ALTER TABLE public.banking_pay_workbench_jobs
  ADD COLUMN economic_build_id uuid NULL,
  ADD COLUMN private_stage text NULL,
  ADD COLUMN private_cursor_kind text NULL,
  ADD COLUMN private_cursor_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN private_stage_version integer NULL;

ALTER TABLE public.banking_pay_workbench_jobs
  -- DB-CONSTRAINT-084
  ADD CONSTRAINT bpay_wb_jobs_economic_build_fk
    FOREIGN KEY (economic_build_id)
    REFERENCES private.banking_pay_workbench_economic_builds(id) ON DELETE SET NULL,
  -- DB-CONSTRAINT-085
  ADD CONSTRAINT bpay_wb_jobs_private_cursor_chk CHECK (
    jsonb_typeof(private_cursor_json) = 'object'
    AND (private_stage_version IS NULL OR private_stage_version > 0)
  ),
  -- DB-CONSTRAINT-086
  ADD CONSTRAINT bpay_wb_jobs_private_stage_cursor_chk CHECK (
    (private_stage IS NULL AND private_cursor_kind IS NULL AND private_stage_version IS NULL)
    OR (private_stage = 'BUILD_INITIALISE' AND private_cursor_kind = 'BUILD_INITIALISE')
    OR (private_stage = 'PREPARE_SCOPE' AND private_cursor_kind IN ('SCOPE_SELECT','SEED_SCOPE_SEAL'))
    OR (private_stage = 'DEPENDENCY_CLOSURE' AND private_cursor_kind IN ('DEPENDENCY_CLOSURE','DEPENDENCY_SCOPE_SEAL'))
    OR (private_stage = 'WORKSPACE_FACT' AND private_cursor_kind = 'WORKSPACE_FACT')
    OR (private_stage = 'RECONCILE_EXECUTE' AND private_cursor_kind = 'RECONCILE_EXECUTE')
    OR (private_stage = 'SOURCE_PUBLISH' AND private_cursor_kind = 'SOURCE_PUBLISH')
    OR (private_stage = 'BOOTSTRAP_DISCOVERY' AND private_cursor_kind = 'BOOTSTRAP_DISCOVERY')
    OR (private_stage = 'BUILD_CLEANUP' AND private_cursor_kind = 'BUILD_CLEANUP')
    OR (private_stage = 'COMPLETE' AND private_cursor_kind = 'COMPLETE')
  ),
  -- DB-CONSTRAINT-087
  ADD CONSTRAINT bpay_wb_jobs_build_identity_chk CHECK (
    (
      private_stage IS NULL
      AND economic_build_id IS NULL
      AND private_cursor_kind IS NULL
      AND private_stage_version IS NULL
      AND private_cursor_json = '{}'::jsonb
    )
    OR (
      economic_build_id IS NULL
      AND job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND status = 'QUEUED'
      AND private_stage = 'BUILD_INITIALISE'
      AND private_cursor_kind = 'BUILD_INITIALISE'
      AND attempt_count = 0
      AND private_stage_version = 1
    )
    OR (
      economic_build_id IS NOT NULL
      AND job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND private_stage IS NOT NULL
      AND private_stage <> 'BUILD_INITIALISE'
      AND private_cursor_kind IS NOT NULL
      AND private_stage_version IS NOT NULL
    )
  );

ALTER TABLE public.settings_defaults
  ADD COLUMN banking_pay_workbench_reconciliation_envelope_version integer NOT NULL DEFAULT 1,
  ADD COLUMN banking_pay_workbench_reconciliation_envelope_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN banking_pay_workbench_reconciliation_envelope_evidence_json jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE public.settings_defaults
  -- DB-CONSTRAINT-088
  ADD CONSTRAINT settings_bpay_wb_reconciliation_envelope_version_chk CHECK (
    banking_pay_workbench_reconciliation_envelope_version > 0
  ),
  -- DB-CONSTRAINT-089
  ADD CONSTRAINT settings_bpay_wb_reconciliation_envelope_json_chk CHECK (
    jsonb_typeof(banking_pay_workbench_reconciliation_envelope_json) = 'object'
    AND jsonb_typeof(banking_pay_workbench_reconciliation_envelope_evidence_json) = 'object'
  );

-- DB-INDEX-002 through 007.
CREATE INDEX bpay_wb_scope_registry_initialisation_idx
  ON private.banking_pay_workbench_candidate_scope_registry
  (initialisation_status,updated_at_utc,candidate_id);
CREATE INDEX bpay_wb_scope_registry_current_build_idx
  ON private.banking_pay_workbench_candidate_scope_registry(current_build_id)
  WHERE current_build_id IS NOT NULL;
CREATE INDEX bpay_wb_timesheet_scope_active_idx
  ON private.banking_pay_workbench_timesheet_scope_state
  (candidate_id,dirty_generation,timesheet_id)
  INCLUDE (economic_state,evaluated_generation,current_input_fingerprint,current_build_id)
  WHERE economic_state IN ('DIRTY','LIVE');
CREATE INDEX bpay_wb_timesheet_scope_dirty_idx
  ON private.banking_pay_workbench_timesheet_scope_state
  (candidate_id,last_dirtied_at_utc,timesheet_id)
  WHERE economic_state = 'DIRTY';
CREATE INDEX bpay_wb_timesheet_scope_build_idx
  ON private.banking_pay_workbench_timesheet_scope_state
  (current_build_id,candidate_id,timesheet_id)
  WHERE current_build_id IS NOT NULL;

-- DB-INDEX-010 through 014.
CREATE UNIQUE INDEX bpay_wb_economic_builds_source_job_uq
  ON private.banking_pay_workbench_economic_builds(source_job_id)
  WHERE source_job_id IS NOT NULL;
CREATE UNIQUE INDEX bpay_wb_economic_builds_candidate_active_uq
  ON private.banking_pay_workbench_economic_builds(candidate_id)
  WHERE status IN (
    'COLLECTING','READY_FOR_RECONCILIATION','RECONCILING','RECONCILED',
    'PUBLISHING','BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
  );
CREATE INDEX bpay_wb_economic_builds_recovery_idx
  ON private.banking_pay_workbench_economic_builds(status,private_stage,updated_at_utc,id)
  WHERE status IN (
    'COLLECTING','READY_FOR_RECONCILIATION','RECONCILING','RECONCILED',
    'PUBLISHING','BLOCKED_UNVALIDATED_RECONCILIATION_SCALE','CLEANING'
  );
CREATE INDEX bpay_wb_economic_builds_session_authority_idx
  ON private.banking_pay_workbench_economic_builds
  (session_id,candidate_id,session_version,source_change_seq,source_build_run_id,id);
CREATE INDEX bpay_wb_economic_builds_cleanup_idx
  ON private.banking_pay_workbench_economic_builds(cleanup_not_before_utc,id)
  WHERE status IN ('COMPLETE','OBSOLETE','FAILED','CLEANING');

-- DB-INDEX-016 through 018.
CREATE UNIQUE INDEX bpay_wb_economic_build_scope_ordinal_uq
  ON private.banking_pay_workbench_economic_build_scope(build_id,stable_ordinal)
  WHERE stable_ordinal IS NOT NULL;
CREATE INDEX bpay_wb_economic_build_scope_unit_idx
  ON private.banking_pay_workbench_economic_build_scope
  (build_id,dependency_unit_key,stable_ordinal,timesheet_id);
CREATE INDEX bpay_wb_economic_build_scope_candidate_idx
  ON private.banking_pay_workbench_economic_build_scope(candidate_id,timesheet_id,build_id);

-- DB-INDEX-020 through 022.
CREATE INDEX bpay_wb_economic_build_facts_unit_idx
  ON private.banking_pay_workbench_economic_build_facts
  (build_id,dependency_unit_key,fact_family,natural_key);
CREATE INDEX bpay_wb_economic_build_facts_timesheet_idx
  ON private.banking_pay_workbench_economic_build_facts
  (build_id,timesheet_id,fact_family,natural_key)
  WHERE timesheet_id IS NOT NULL;
CREATE INDEX bpay_wb_economic_build_facts_source_idx
  ON private.banking_pay_workbench_economic_build_facts
  (source_relation,source_id,build_id,fact_family)
  WHERE source_id IS NOT NULL;

-- DB-INDEX-026 and 027. DB-INDEX-024/025 are constraint-owned.
CREATE INDEX bpay_wb_economic_build_fact_pages_completion_idx
  ON private.banking_pay_workbench_economic_build_fact_pages
  (build_id,fact_family,dependency_unit_key,is_family_final,page_number);
CREATE INDEX bpay_wb_economic_build_fact_pages_attempt_idx
  ON private.banking_pay_workbench_economic_build_fact_pages(attempt_id,id);

-- DB-INDEX-031 through 033. DB-INDEX-029/030 are constraint-owned.
CREATE UNIQUE INDEX bpay_wb_stage_attempts_started_job_uq
  ON private.banking_pay_workbench_stage_attempts(job_id)
  WHERE attempt_status = 'STARTED';
CREATE INDEX bpay_wb_stage_attempts_expiry_idx
  ON private.banking_pay_workbench_stage_attempts(lease_expires_at_utc,id)
  WHERE attempt_status = 'STARTED';
CREATE INDEX bpay_wb_stage_attempts_build_stage_idx
  ON private.banking_pay_workbench_stage_attempts
  (build_id,private_stage,attempt_number DESC,id);

-- DB-INDEX-036 and 037. DB-INDEX-035 is constraint-owned.
CREATE INDEX bpay_wb_canonical_stage_session_candidate_idx
  ON private.banking_pay_workbench_canonical_stage_lines
  (session_id,candidate_id,source_ordinal,line_key);
CREATE INDEX bpay_wb_canonical_stage_timesheet_idx
  ON private.banking_pay_workbench_canonical_stage_lines
  (build_id,timesheet_id,source_ordinal)
  WHERE timesheet_id IS NOT NULL;

-- DB-INDEX-038 through 044.
CREATE INDEX bpay_wb_jobs_source_claim_idx
  ON public.banking_pay_workbench_jobs(run_at_utc,priority,created_at_utc,id)
  INCLUDE (candidate_id,session_id,economic_build_id,private_stage,attempt_count,max_attempts)
  WHERE status = 'QUEUED' AND job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD';
CREATE INDEX bpay_advances_candidate_open_timesheet_idx
  ON public.pay_advances(candidate_id,linked_timesheet_id,id)
  WHERE status = 'ACTIVE' AND linked_timesheet_id IS NOT NULL
    AND cleared_at_utc IS NULL AND written_off_at_utc IS NULL;
CREATE INDEX bpay_finance_components_candidate_open_timesheet_idx
  ON public.pay_finance_case_components(candidate_id,linked_timesheet_id,finance_case_id,id)
  WHERE linked_timesheet_id IS NOT NULL AND closed_at_utc IS NULL;
CREATE INDEX bpay_timesheet_overrides_candidate_active_idx
  ON public.timesheet_payment_overrides(candidate_id,timesheet_id,id)
  WHERE timesheet_id IS NOT NULL AND cleared_at_utc IS NULL
    AND consumed_at_utc IS NULL AND consumed_by_pay_batch_id IS NULL;
CREATE INDEX bpay_ts_adjustments_candidate_unpaid_idx
  ON public.ts_pay_adjustments(candidate_id,timesheet_id,id)
  WHERE timesheet_id IS NOT NULL AND paid_at_utc IS NULL;
CREATE INDEX bpay_wb_timesheet_scope_candidate_fk_idx
  ON private.banking_pay_workbench_timesheet_scope_state(candidate_id,timesheet_id);
CREATE INDEX bpay_wb_economic_builds_candidate_fk_idx
  ON private.banking_pay_workbench_economic_builds(candidate_id,id);

-- DB-INDEX-052 through 054.
CREATE INDEX bpay_wb_jobs_economic_build_idx
  ON public.banking_pay_workbench_jobs(economic_build_id,status,id)
  WHERE economic_build_id IS NOT NULL;
CREATE INDEX bpay_wb_dependency_edges_stream_idx
  ON private.banking_pay_workbench_economic_build_facts
  (build_id,edge_from_timesheet_id,edge_kind,natural_key)
  INCLUDE (edge_to_timesheet_id,dependency_unit_key,financial_digest)
  WHERE fact_family = 'DEPENDENCY_EDGE';
CREATE INDEX bpay_wb_economic_build_scope_incomplete_idx
  ON private.banking_pay_workbench_economic_build_scope
  (build_id,closure_status,dependency_unit_anchor_timesheet_id,timesheet_id)
  WHERE closure_status <> 'SEALED';

ALTER TABLE private.banking_pay_workbench_candidate_scope_registry OWNER TO postgres;
ALTER TABLE private.banking_pay_workbench_timesheet_scope_state OWNER TO postgres;
ALTER TABLE private.banking_pay_workbench_economic_builds OWNER TO postgres;
ALTER TABLE private.banking_pay_workbench_economic_build_scope OWNER TO postgres;
ALTER TABLE private.banking_pay_workbench_economic_build_facts OWNER TO postgres;
ALTER TABLE private.banking_pay_workbench_economic_build_fact_pages OWNER TO postgres;
ALTER TABLE private.banking_pay_workbench_stage_attempts OWNER TO postgres;
ALTER TABLE private.banking_pay_workbench_canonical_stage_lines OWNER TO postgres;

REVOKE ALL ON TABLE private.banking_pay_workbench_candidate_scope_registry FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_workbench_timesheet_scope_state FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_workbench_economic_builds FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_workbench_economic_build_scope FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_workbench_economic_build_facts FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_workbench_economic_build_fact_pages FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_workbench_stage_attempts FROM PUBLIC, anon, authenticated, service_role;
REVOKE ALL ON TABLE private.banking_pay_workbench_canonical_stage_lines FROM PUBLIC, anon, authenticated, service_role;

COMMIT;
