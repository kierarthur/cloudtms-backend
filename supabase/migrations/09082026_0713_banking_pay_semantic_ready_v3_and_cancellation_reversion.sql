/*
 * Banking Pay semantic Ready-to-Pay V3 and cancellation-reversion controls.
 *
 * All new behavior is installed disabled.  Cancellation reversion cannot be
 * enabled unless both semantic V3 publication and the server-side Draft guard
 * are enabled, preventing a legacy structurally-valid/recovery-only source
 * from becoming a reversion authority.
 */

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_semantic_ready_observe_v2_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_semantic_ready_publication_v3_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_semantic_ready_draft_guard_v2_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_cancellation_reversion_observe_v1_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_cancellation_reversion_publish_v1_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_cancellation_reversion_exact_empty_v1_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_draft_overlay_fast_cancel_v1_enabled boolean NOT NULL DEFAULT false;

ALTER TABLE public.settings_defaults
  DROP CONSTRAINT IF EXISTS settings_bpay_semantic_cancellation_dependency_ck;

ALTER TABLE public.settings_defaults
  ADD CONSTRAINT settings_bpay_semantic_cancellation_dependency_ck CHECK (
    banking_pay_cancellation_reversion_publish_v1_enabled IS NOT TRUE
    OR (
      banking_pay_workbench_semantic_ready_publication_v3_enabled IS TRUE
      AND banking_pay_workbench_semantic_ready_draft_guard_v2_enabled IS TRUE
    )
  );

UPDATE public.settings_defaults
SET banking_pay_workbench_semantic_ready_observe_v2_enabled = false,
    banking_pay_workbench_semantic_ready_publication_v3_enabled = false,
    banking_pay_workbench_semantic_ready_draft_guard_v2_enabled = false,
    banking_pay_cancellation_reversion_observe_v1_enabled = false,
    banking_pay_cancellation_reversion_publish_v1_enabled = false,
    banking_pay_cancellation_reversion_exact_empty_v1_enabled = false,
    banking_pay_draft_overlay_fast_cancel_v1_enabled = false
WHERE id = 1;

ALTER TABLE public.banking_pay_workbench_session_scope
  DROP CONSTRAINT IF EXISTS bpay_wb_scope_certified_preview_attestation_ck;

ALTER TABLE public.banking_pay_workbench_session_scope
  ADD CONSTRAINT bpay_wb_scope_certified_preview_attestation_ck CHECK (
    certified_preview_publication_parity_ok IS NOT TRUE
    OR (
      certified_preview_publication_required IS TRUE
      AND certified_preview_publication_session_version > 0
      AND certified_preview_publication_source_change_seq >= 0
      AND certified_preview_publication_source_build_run_id IS NOT NULL
      AND certified_preview_publication_attested_at_utc IS NOT NULL
      AND jsonb_typeof(certified_preview_publication_attestation_json) = 'object'
      AND certified_preview_publication_attestation_json->>'parity_complete' = 'true'
      AND (
        (
          certified_preview_publication_attestation_json->>'attestation_version' = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1'
          AND certified_preview_publication_attestation_json->>'authority_kind' = 'BOUNDED_FULL_SOURCE_BUILD'
        )
        OR (
          certified_preview_publication_attestation_json->>'attestation_version' = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2'
          AND certified_preview_publication_attestation_json->>'contract_version' = '2'
          AND certified_preview_publication_attestation_json->>'authority_kind' = 'CERTIFIED_CLONE'
          AND certified_preview_publication_attestation_json->>'final_state' IN ('READY','SOURCE_EMPTY')
        )
        OR (
          certified_preview_publication_attestation_json->>'attestation_version' = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V2'
          AND certified_preview_publication_attestation_json->>'contract_version' = '2'
          AND certified_preview_publication_attestation_json->>'authority_kind' = 'TARGETED_DELTA'
          AND certified_preview_publication_attestation_json->>'final_state' IN ('READY','SOURCE_EMPTY')
        )
        OR (
          certified_preview_publication_attestation_json->>'attestation_version' = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
          AND certified_preview_publication_attestation_json->>'contract_version' = '3'
          AND certified_preview_publication_attestation_json->>'semantic_contract_version' = 'READY_TO_PAY_SEMANTIC_V2'
          AND certified_preview_publication_attestation_json->>'authority_kind' IN (
            'BOUNDED_FULL_SOURCE_BUILD',
            'CERTIFIED_CLONE',
            'TARGETED_DELTA',
            'CERTIFIED_CANCELLATION_REVERSION'
          )
          AND certified_preview_publication_attestation_json->>'final_state' IN ('READY','SOURCE_EMPTY')
          AND certified_preview_publication_attestation_json->>'semantic_ready' = 'true'
          AND COALESCE((certified_preview_publication_attestation_json->>'invalid_selectable_row_count')::integer, -1) = 0
          AND (certified_preview_publication_attestation_json->>'candidate_ready_amount')::numeric >= 0
          AND NULLIF(certified_preview_publication_attestation_json->>'semantic_proof_digest', '') IS NOT NULL
        )
      )
    )
  );

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_semantic_ready_observe_v2_enabled IS
  'Computes READY_TO_PAY_SEMANTIC_V2 diagnostics without changing terminal publication.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_semantic_ready_publication_v3_enabled IS
  'Requires terminal CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3 semantic and structural proof for new publications.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_semantic_ready_draft_guard_v2_enabled IS
  'Rejects recovery-only, cross-candidate-headroom, or negative-result Draft candidate selections.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_cancellation_reversion_observe_v1_enabled IS
  'Runs exact cancellation-reversion admission diagnostics without publishing a reversion source.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_cancellation_reversion_publish_v1_enabled IS
  'Publishes exact certified cancellation reversions; requires semantic V3 publication and Draft guard.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_cancellation_reversion_exact_empty_v1_enabled IS
  'Allows positively proved empty semantic V3 cancellation reversion.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_draft_overlay_fast_cancel_v1_enabled IS
  'Uses the bounded untouched-Draft overlay removal route without PRE_BANK_CANCEL financial work items.';

