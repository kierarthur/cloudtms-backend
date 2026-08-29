BEGIN;

-- ============================================================
-- Payment correction DB safety migration
-- Adds durable bank-event mapping strength and safer transfer
-- / correction work-item metadata.
--
-- Policy intent:
-- - Amount-only bank event matches must not be auto-correction eligible.
-- - Deliberately cancelled/voided pre-bank transfers must not be stored as BLOCKED.
-- - Work-item processing can preserve the user who manually processed a chunk.
-- ============================================================


-- ============================================================
-- 1. pay_bank_transfer_events.mapping_method
-- ============================================================

ALTER TABLE public.pay_bank_transfer_events
ADD COLUMN IF NOT EXISTS mapping_method text NULL;

COMMENT ON COLUMN public.pay_bank_transfer_events.mapping_method IS
'Durable bank-event mapping strength. Automatic payment correction may only use strong MATCHED mapping methods; AMOUNT_ONLY_UNIQUE, UNMATCHED, AMBIGUOUS and LEGACY_NO_ARTIFACT require manual review.';

ALTER TABLE public.pay_bank_transfer_events
DROP CONSTRAINT IF EXISTS pay_bank_transfer_events_mapping_method_chk;

ALTER TABLE public.pay_bank_transfer_events
ADD CONSTRAINT pay_bank_transfer_events_mapping_method_chk
CHECK (
  mapping_method IS NULL
  OR mapping_method IN (
    'TRANSFER_ID',
    'PROVIDER_EVENT_ID',
    'PROVIDER_REFERENCE',
    'REQUEST_ID',
    'RAIL_TX_ID',
    'PAYMENT_REFERENCE',
    'MANUAL_TRANSFER_SELECTION',
    'AMOUNT_ONLY_UNIQUE',
    'UNMATCHED',
    'AMBIGUOUS',
    'LEGACY_NO_ARTIFACT'
  )
);

COMMENT ON CONSTRAINT pay_bank_transfer_events_mapping_method_chk
ON public.pay_bank_transfer_events IS
'Allowed durable mapping-strength values for payment correction event classification. Only strong MATCHED methods are auto-correction eligible.';

CREATE INDEX IF NOT EXISTS pay_bank_transfer_events_mapping_method_idx
ON public.pay_bank_transfer_events(mapping_method);


-- ============================================================
-- 2. pay_bank_transfers.status values
-- ============================================================

-- Normalise any existing US spelling before tightening the constraint.
UPDATE public.pay_bank_transfers
SET status = 'CANCELLED'
WHERE status = 'CANCELED';

-- Normalise leading/trailing whitespace and case for existing rows.
UPDATE public.pay_bank_transfers
SET status = upper(btrim(status))
WHERE status IS NOT NULL
  AND status IS DISTINCT FROM upper(btrim(status));

-- DB-side defensive normalisation for future writes.
-- RPCs should still normalise CANCELED -> CANCELLED before storing.
CREATE OR REPLACE FUNCTION public._pay_bank_transfers_normalise_status_biu()
RETURNS trigger
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
BEGIN
  IF NEW.status IS NOT NULL THEN
    NEW.status := upper(btrim(NEW.status));

    IF NEW.status = 'CANCELED' THEN
      NEW.status := 'CANCELLED';
    END IF;
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_pay_bank_transfers_normalise_status_biu
ON public.pay_bank_transfers;

CREATE TRIGGER trg_pay_bank_transfers_normalise_status_biu
BEFORE INSERT OR UPDATE OF status
ON public.pay_bank_transfers
FOR EACH ROW
EXECUTE FUNCTION public._pay_bank_transfers_normalise_status_biu();

-- Replace the existing status check with the expanded correction-aware set.
ALTER TABLE public.pay_bank_transfers
DROP CONSTRAINT IF EXISTS pay_bank_transfers_status_chk_v2;

ALTER TABLE public.pay_bank_transfers
DROP CONSTRAINT IF EXISTS pay_bank_transfers_status_chk_v3;

ALTER TABLE public.pay_bank_transfers
ADD CONSTRAINT pay_bank_transfers_status_chk_v3
CHECK (
  status IN (
    'PENDING',
    'PROCESSING',
    'UNKNOWN',
    'COMPLETED',
    'FAILED',
    'DECLINED',
    'REJECTED',
    'CANCELLED',
    'VOIDED',
    'RETURNED',
    'REVERTED',
    'BLOCKED',
    'SUBMISSION_FAILED',
    'FAILED_BEFORE_COMMIT'
  )
);

COMMENT ON CONSTRAINT pay_bank_transfers_status_chk_v3
ON public.pay_bank_transfers IS
'Correction-aware transfer statuses. Pre-bank zeroed/cancelled transfers should use VOIDED or CANCELLED, not BLOCKED. CANCELED is normalised to CANCELLED by trigger/RPC logic.';


-- ============================================================
-- 3. pay_payment_correction_work_items.processed_by_user_id
-- ============================================================

ALTER TABLE public.pay_payment_correction_work_items
ADD COLUMN IF NOT EXISTS processed_by_user_id uuid NULL;

COMMENT ON COLUMN public.pay_payment_correction_work_items.processed_by_user_id IS
'User who manually processed this correction work item/chunk, when applicable. NULL is allowed for system/cron processing.';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'public.pay_payment_correction_work_items'::regclass
      AND conname = 'pay_payment_correction_work_items_processed_by_user_id_fkey'
  ) THEN
    ALTER TABLE public.pay_payment_correction_work_items
    ADD CONSTRAINT pay_payment_correction_work_items_processed_by_user_id_fkey
    FOREIGN KEY (processed_by_user_id)
    REFERENCES public.tms_users(id)
    ON DELETE SET NULL;
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS pay_payment_correction_work_items_processed_by_user_id_idx
ON public.pay_payment_correction_work_items(processed_by_user_id);


-- ============================================================
-- 4. mail_outbox
-- ============================================================

-- No schema change is required for mail_outbox.
-- The required safety change belongs in the correction/remittance RPCs:
-- queued mail must be cancelled only when tied to the selected correction scope
-- via precise context_kind/context_id/reference/recipient metadata.
-- Do not use broad batch-level cancellation for selected candidate,
-- transfer, finance-case, payout, or umbrella-group correction scopes.

COMMIT;
