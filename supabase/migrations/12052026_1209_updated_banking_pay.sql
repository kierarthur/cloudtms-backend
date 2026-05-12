-- 12052026_banking_pay_freshness_metadata_and_scaling_indexes.sql
--
-- Revised after constraint failure:
--   Existing banking_pay_operation_config_chunk_type_chk did not allow
--   FRESHNESS_VALIDATE.
--
-- This migration:
--   1. Adds pay_batches stored freshness-at-execution metadata.
--   2. Extends operation config/chunk chunk_type CHECK constraints to allow
--      FRESHNESS_VALIDATE.
--   3. Seeds the freshness validation chunk config.
--   4. Adds paging/evidence indexes.
--
-- Policy X:
--   This migration stores freshness metadata and adds indexes only.
--   It does not remap, reinterpret, or recalculate batch economics from live
--   TSFIN or live finance-component identity.

BEGIN;

-- ============================================================================
-- 0. Extend allowed operation chunk types to include FRESHNESS_VALIDATE
-- ============================================================================

ALTER TABLE public.banking_pay_operation_config
  DROP CONSTRAINT IF EXISTS banking_pay_operation_config_chunk_type_chk;

ALTER TABLE public.banking_pay_operation_config
  ADD CONSTRAINT banking_pay_operation_config_chunk_type_chk
  CHECK (
    chunk_type IN (
      'CANDIDATE_SCOPE',
      'TSFIN',
      'PAYEE_READINESS',
      'TRANSFER_GROUP',
      'TRANSFER_SUBMIT',
      'RAIL_UPDATE',
      'SETTLEMENT',
      'REMITTANCE',
      'PAYOUT_NOTICE',
      'PREVIEW_PAGE',
      'FRESHNESS_VALIDATE'
    )
  );

ALTER TABLE public.banking_pay_operation_chunks
  DROP CONSTRAINT IF EXISTS banking_pay_operation_chunks_chunk_type_chk;

ALTER TABLE public.banking_pay_operation_chunks
  ADD CONSTRAINT banking_pay_operation_chunks_chunk_type_chk
  CHECK (
    chunk_type IN (
      'CANDIDATE_SCOPE',
      'TSFIN',
      'PAYEE_READINESS',
      'TRANSFER_GROUP',
      'TRANSFER_SUBMIT',
      'RAIL_UPDATE',
      'SETTLEMENT',
      'REMITTANCE',
      'PAYOUT_NOTICE',
      'PREVIEW_PAGE',
      'FRESHNESS_VALIDATE'
    )
  );


-- ============================================================================
-- 1. pay_batches — stored freshness-at-execution metadata
-- ============================================================================

ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS freshness_operation_id uuid,
  ADD COLUMN IF NOT EXISTS freshness_validation_status text,
  ADD COLUMN IF NOT EXISTS freshness_checked_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS freshness_result_hash text,
  ADD COLUMN IF NOT EXISTS freshness_scope_hash text,
  ADD COLUMN IF NOT EXISTS freshness_result_json jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS con
    JOIN pg_class AS cls
      ON cls.oid = con.conrelid
    JOIN pg_namespace AS nsp
      ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = 'public'
      AND cls.relname = 'pay_batches'
      AND con.conname = 'pay_batches_freshness_validation_status_check'
  ) THEN
    ALTER TABLE public.pay_batches
      ADD CONSTRAINT pay_batches_freshness_validation_status_check
      CHECK (
        freshness_validation_status IS NULL
        OR freshness_validation_status IN (
          'PENDING',
          'PASSED',
          'STALE',
          'FAILED',
          'NOT_REQUIRED'
        )
      );
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS con
    JOIN pg_class AS cls
      ON cls.oid = con.conrelid
    JOIN pg_namespace AS nsp
      ON nsp.oid = cls.relnamespace
    WHERE nsp.nspname = 'public'
      AND cls.relname = 'pay_batches'
      AND con.conname = 'pay_batches_freshness_operation_id_fkey'
  ) THEN
    ALTER TABLE public.pay_batches
      ADD CONSTRAINT pay_batches_freshness_operation_id_fkey
      FOREIGN KEY (freshness_operation_id)
      REFERENCES public.banking_pay_operations(id)
      ON DELETE SET NULL;
  END IF;
END
$$;

COMMENT ON COLUMN public.pay_batches.freshness_operation_id IS
  'Operation that produced the stored freshness-at-execution result. Post-draft metadata only; not an economic authority.';

COMMENT ON COLUMN public.pay_batches.freshness_validation_status IS
  'Compact stored freshness-at-execution status: PENDING, PASSED, STALE, FAILED, or NOT_REQUIRED.';

COMMENT ON COLUMN public.pay_batches.freshness_checked_at_utc IS
  'UTC timestamp when the stored freshness-at-execution result was produced.';

COMMENT ON COLUMN public.pay_batches.freshness_result_hash IS
  'Deterministic hash of the aggregate freshness-at-execution result.';

COMMENT ON COLUMN public.pay_batches.freshness_scope_hash IS
  'Deterministic hash of the frozen batch artifact scope used for freshness validation. Must be derived from frozen batch membership/economic keys, not live TSFIN.';

COMMENT ON COLUMN public.pay_batches.freshness_result_json IS
  'Compact capped freshness-at-execution result metadata. Must not store uncapped full diff arrays.';


-- ============================================================================
-- 2. banking_pay_operation_config — seed freshness chunk configuration
-- ============================================================================

INSERT INTO public.banking_pay_operation_config (
  operation_type,
  phase,
  chunk_type,
  default_chunk_size,
  min_chunk_size,
  max_chunk_size,
  max_advance_ms,
  lock_seconds,
  enabled,
  updated_at_utc,
  updated_by
)
VALUES (
  'PAYMENT_EXECUTE',
  'VALIDATE_FRESHNESS',
  'FRESHNESS_VALIDATE',
  50,
  1,
  250,
  15000,
  60,
  true,
  now(),
  NULL
)
ON CONFLICT (operation_type, phase, chunk_type)
DO UPDATE SET
  default_chunk_size = EXCLUDED.default_chunk_size,
  min_chunk_size = EXCLUDED.min_chunk_size,
  max_chunk_size = EXCLUDED.max_chunk_size,
  max_advance_ms = EXCLUDED.max_advance_ms,
  lock_seconds = EXCLUDED.lock_seconds,
  enabled = EXCLUDED.enabled,
  updated_at_utc = now();


-- ============================================================================
-- 3. banking_pay_operation_chunks — freshness-friendly claim index
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_banking_pay_operation_chunks_freshness_claim
ON public.banking_pay_operation_chunks (
  operation_id,
  phase,
  chunk_type,
  status,
  sequence_no
);


-- ============================================================================
-- 4. pay_batch_candidates — stable candidate paging index
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_pay_batch_candidates_batch_id_id
ON public.pay_batch_candidates (
  pay_batch_id,
  id
);


-- ============================================================================
-- 5. pay_batch_items — stable item paging index
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_pay_batch_items_candidate_id_id
ON public.pay_batch_items (
  pay_batch_candidate_id,
  id
);


-- ============================================================================
-- 6. pay_batch_item_breakdowns — stable breakdown paging index
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_pay_batch_item_breakdowns_item_id_id
ON public.pay_batch_item_breakdowns (
  pay_batch_item_id,
  id
);


-- ============================================================================
-- 7. pay_bank_transfers — stable transfer paging index
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_pay_bank_transfers_batch_id_id
ON public.pay_bank_transfers (
  pay_batch_id,
  id
);


-- ============================================================================
-- 8. pay_bank_transfer_events — provider-evidence lookup index
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_pay_bank_transfer_events_provider_evidence_lookup
ON public.pay_bank_transfer_events (
  pay_batch_id,
  event_source,
  normalised_state,
  received_at_utc DESC,
  id
);

COMMIT;
