/*
 * Banking Pay certified publication V2, bounded reuse and reconciliation
 * controls. TEST rollout defaults are deliberately disabled.
 */

ALTER TABLE public.banking_pay_workbench_session_scope
  DROP CONSTRAINT bpay_wb_scope_certified_preview_attestation_ck;

ALTER TABLE public.banking_pay_workbench_session_scope
  ADD CONSTRAINT bpay_wb_scope_certified_preview_attestation_ck CHECK (
    certified_preview_publication_parity_ok IS NOT TRUE
    OR (
      certified_preview_publication_required IS TRUE
      AND certified_preview_publication_session_version > 0
      AND certified_preview_publication_source_change_seq >= 0
      AND certified_preview_publication_source_build_run_id IS NOT NULL
      AND certified_preview_publication_attested_at_utc IS NOT NULL
      AND jsonb_typeof(certified_preview_publication_attestation_json)='object'
      AND certified_preview_publication_attestation_json->>'parity_complete'='true'
      AND (
        (
          certified_preview_publication_attestation_json->>'attestation_version'='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1'
          AND certified_preview_publication_attestation_json->>'authority_kind'='BOUNDED_FULL_SOURCE_BUILD'
        )
        OR (
          certified_preview_publication_attestation_json->>'attestation_version'='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2'
          AND certified_preview_publication_attestation_json->>'contract_version'='2'
          AND certified_preview_publication_attestation_json->>'authority_kind'='CERTIFIED_CLONE'
          AND certified_preview_publication_attestation_json->>'final_state' IN ('READY','SOURCE_EMPTY')
        )
        OR (
          certified_preview_publication_attestation_json->>'attestation_version'='CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2'
          AND certified_preview_publication_attestation_json->>'contract_version'='2'
          AND certified_preview_publication_attestation_json->>'authority_kind'='TARGETED_DELTA'
          AND certified_preview_publication_attestation_json->>'final_state' IN ('READY','SOURCE_EMPTY')
        )
      )
    )
  );

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_clone_bounded_reuse_v2_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_clone_source_empty_reuse_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_reconciliation_optimization_version integer NOT NULL DEFAULT 0;

ALTER TABLE public.settings_defaults
  DROP CONSTRAINT IF EXISTS settings_bpay_reconciliation_optimization_version_ck;

ALTER TABLE public.settings_defaults
  ADD CONSTRAINT settings_bpay_reconciliation_optimization_version_ck CHECK (
    banking_pay_workbench_reconciliation_optimization_version IN (0,1)
  );

UPDATE public.settings_defaults
SET banking_pay_workbench_clone_bounded_reuse_v2_enabled=false,
    banking_pay_workbench_clone_source_empty_reuse_enabled=false,
    banking_pay_workbench_reconciliation_optimization_version=0,
    banking_pay_workbench_delta_enable_simple_authorise=false,
    banking_pay_workbench_delta_enable_simple_unauthorise=false,
    banking_pay_workbench_delta_enable_exact_import_family=false
WHERE id=1;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bpay_wb_economic_builds_candidate_complete_recent_v1
ON private.banking_pay_workbench_economic_builds(candidate_id,completed_at_utc DESC,id DESC)
WHERE status='COMPLETE'
  AND private_stage='COMPLETE'
  AND completed_at_utc IS NOT NULL;

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_clone_bounded_reuse_v2_enabled IS
  'Allows certified non-empty historical source reuse after final target-date proof.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_clone_source_empty_reuse_enabled IS
  'Allows positive-proof certified SOURCE_EMPTY reuse; missing rows are never proof.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_reconciliation_optimization_version IS
  'Frozen per build: 0 current reconciliation path; 1 pass-local duplicate-work reduction.';
