-- Banking Pay physical source-publication identity, semantic build ownership,
-- and bounded Draft/cancellation fast-path controls.
--
-- This migration is deliberately additive. Legacy source rows and completed
-- attestations retain NULL publication/fingerprint values and continue through
-- their existing safe paths. New enforcement is enabled only after every
-- writer and reader is installed and executable TEST acceptance has passed.
--
-- Policy X: these columns identify orchestration/source authority only. They do
-- not change post-Draft frozen artifacts, payment economics, VAT, PAYE,
-- settlement, remittance, provider, TSFIN, TS_DAY, or correction formulas.

ALTER TABLE public.banking_pay_workbench_candidate_source_lines
  ADD COLUMN IF NOT EXISTS source_publication_id uuid NULL;

ALTER TABLE public.banking_pay_workbench_session_scope
  ADD COLUMN IF NOT EXISTS certified_preview_publication_source_publication_id uuid NULL;

ALTER TABLE private.banking_pay_workbench_economic_builds
  ADD COLUMN IF NOT EXISTS authority_fingerprint_version smallint NULL,
  ADD COLUMN IF NOT EXISTS authority_fingerprint text NULL;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_source_publication_identity_write_v1_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_source_publication_identity_enforce_v1_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_same_authority_build_election_v1_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_draft_step_rpc_v1_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_selection_intent_identity_v1_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_pre_bank_cancel_set_page_v1_enabled boolean NOT NULL DEFAULT false;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_candidate_source_lines'::regclass
      AND conname = 'bpay_wb_source_publication_identity_ck'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_candidate_source_lines
      ADD CONSTRAINT bpay_wb_source_publication_identity_ck CHECK (
        source_publication_id IS NULL
        OR (
          source_build_run_id IS NOT NULL
          AND session_version > 0
          AND source_change_seq >= 0
          AND source_ordinal > 0
        )
      ) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_session_scope'::regclass
      AND conname = 'bpay_wb_scope_source_publication_identity_ck'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_scope
      ADD CONSTRAINT bpay_wb_scope_source_publication_identity_ck CHECK (
        certified_preview_publication_source_publication_id IS NULL
        OR (
          certified_preview_publication_source_build_run_id IS NOT NULL
          AND certified_preview_publication_session_version > 0
          AND certified_preview_publication_source_change_seq >= 0
        )
      ) NOT VALID;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'private.banking_pay_workbench_economic_builds'::regclass
      AND conname = 'bpay_wb_economic_build_authority_fingerprint_ck'
  ) THEN
    ALTER TABLE private.banking_pay_workbench_economic_builds
      ADD CONSTRAINT bpay_wb_economic_build_authority_fingerprint_ck CHECK (
        (
          authority_fingerprint_version IS NULL
          AND authority_fingerprint IS NULL
        )
        OR (
          authority_fingerprint_version > 0
          AND authority_fingerprint ~ '^[0-9a-f]{64}$'
        )
      ) NOT VALID;
  END IF;
END $$;

-- A physical publication has one ordinal stream and one economic membership
-- set even where its source_build_run_id is shared with historical lineages.
CREATE UNIQUE INDEX IF NOT EXISTS uq_bpay_wb_source_publication_ordinal_v1
  ON public.banking_pay_workbench_candidate_source_lines (
    source_publication_id,
    source_ordinal
  )
  WHERE source_publication_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_bpay_wb_source_publication_line_v1
  ON public.banking_pay_workbench_candidate_source_lines (
    source_publication_id,
    COALESCE(timesheet_id, '00000000-0000-0000-0000-000000000000'::uuid),
    line_key
  )
  WHERE source_publication_id IS NOT NULL;

-- One current-or-complete economic owner per exact semantic authority. Failed
-- and obsolete attempts may be replaced through the existing repair path.
CREATE UNIQUE INDEX IF NOT EXISTS uq_bpay_wb_economic_build_authority_v1
  ON private.banking_pay_workbench_economic_builds (
    candidate_id,
    authority_fingerprint_version,
    authority_fingerprint
  )
  WHERE authority_fingerprint IS NOT NULL
    AND status NOT IN ('FAILED', 'OBSOLETE', 'CLEANING');

COMMENT ON COLUMN public.banking_pay_workbench_candidate_source_lines.source_publication_id IS
  'Immutable identity of one physical candidate source-row cohort. It is distinct from source_build_run_id lineage.';
COMMENT ON COLUMN public.banking_pay_workbench_session_scope.certified_preview_publication_source_publication_id IS
  'Physical source publication proved by the current certified source/preview attestation.';
COMMENT ON COLUMN private.banking_pay_workbench_economic_builds.authority_fingerprint_version IS
  'Version of the semantic build-authority fingerprint contract.';
COMMENT ON COLUMN private.banking_pay_workbench_economic_builds.authority_fingerprint IS
  'Reason-independent SHA-256 identity of the exact accepted current economic build authority.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_source_publication_identity_write_v1_enabled IS
  'Writes physical source_publication_id on every newly created source cohort.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_source_publication_identity_enforce_v1_enabled IS
  'Requires physical source-publication identity for new certified publication and fast-route admission.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_same_authority_build_election_v1_enabled IS
  'Elects or reuses one economic build before dirtying/enqueue side effects for the same accepted authority.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_draft_step_rpc_v1_enabled IS
  'Uses one bounded database transaction/RPC per Draft-create phase step.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_selection_intent_identity_v1_enabled IS
  'Carries explicit selected/unselected intent by stable semantic row identity across republish/recompute.';
COMMENT ON COLUMN public.settings_defaults.banking_pay_pre_bank_cancel_set_page_v1_enabled IS
  'Uses the bounded set-based pre-bank financial cancellation page owner.';

-- Installation is observe-safe. Enablement is a later, explicit TEST gate.
UPDATE public.settings_defaults
SET banking_pay_source_publication_identity_write_v1_enabled = false,
    banking_pay_source_publication_identity_enforce_v1_enabled = false,
    banking_pay_same_authority_build_election_v1_enabled = false,
    banking_pay_draft_step_rpc_v1_enabled = false,
    banking_pay_selection_intent_identity_v1_enabled = false,
    banking_pay_pre_bank_cancel_set_page_v1_enabled = false
WHERE id = 1;
