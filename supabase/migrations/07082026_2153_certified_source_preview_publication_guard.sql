-- Banking Pay Workbench certified CURRENT-source publication guard.
-- TEST rollout only.  This is pre-draft read-model authority and does not
-- alter frozen post-draft payment evidence (Policy X).

ALTER TABLE public.banking_pay_workbench_session_scope
  ADD COLUMN IF NOT EXISTS certified_preview_publication_required boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS certified_preview_publication_parity_ok boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS certified_preview_publication_session_version bigint,
  ADD COLUMN IF NOT EXISTS certified_preview_publication_source_change_seq bigint,
  ADD COLUMN IF NOT EXISTS certified_preview_publication_source_build_run_id uuid,
  ADD COLUMN IF NOT EXISTS certified_preview_publication_attestation_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS certified_preview_publication_attested_at_utc timestamptz;

DO $migration$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.banking_pay_workbench_session_scope'::regclass
      AND conname = 'bpay_wb_scope_certified_preview_attestation_ck'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_session_scope
      ADD CONSTRAINT bpay_wb_scope_certified_preview_attestation_ck
      CHECK (
        certified_preview_publication_parity_ok IS NOT TRUE
        OR (
          certified_preview_publication_required IS TRUE
          AND certified_preview_publication_session_version > 0
          AND certified_preview_publication_source_change_seq >= 0
          AND certified_preview_publication_source_build_run_id IS NOT NULL
          AND certified_preview_publication_attested_at_utc IS NOT NULL
          AND jsonb_typeof(certified_preview_publication_attestation_json) = 'object'
          AND certified_preview_publication_attestation_json->>'attestation_version' = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V1'
          AND certified_preview_publication_attestation_json->>'authority_kind' = 'BOUNDED_FULL_SOURCE_BUILD'
          AND certified_preview_publication_attestation_json->>'parity_complete' = 'true'
        )
      );
  END IF;
END
$migration$;

-- Existing legacy scopes deliberately remain opt-out.  Every future bounded
-- completion and the separately authorised exact repair opt in atomically.
-- This avoids falsely blocking unrelated already-open sessions that have not
-- yet passed through the certified publisher.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bpay_wb_scope_certified_preview_incomplete_v1
  ON public.banking_pay_workbench_session_scope (session_id, scope_ordinal, candidate_id)
  WHERE certified_preview_publication_required IS TRUE
    AND certified_preview_publication_parity_ok IS NOT TRUE;

COMMENT ON COLUMN public.banking_pay_workbench_session_scope.certified_preview_publication_required IS
  'True only after the scope enters the bounded certified-source publication contract.';
COMMENT ON COLUMN public.banking_pay_workbench_session_scope.certified_preview_publication_parity_ok IS
  'Terminal attestation: exact CURRENT source identities and READY preview identities match in both directions.';
COMMENT ON COLUMN public.banking_pay_workbench_session_scope.certified_preview_publication_attestation_json IS
  'Compact certified publication proof; never a replacement for source or preview rows.';
