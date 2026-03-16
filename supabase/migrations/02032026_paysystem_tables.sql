-- CloudTMS — Loans/Overpayments schema primitives
-- Safe to re-run (idempotent where possible).
--
-- IMPORTANT NOTES (verified from TABLES IN DB):
-- - public.pay_batch_items.item_type is TEXT (not an enum), so you do NOT need to "extend pay_batch_item_type"
--   to allow new values like 'OVERPAYMENT_RECOVERY' / 'LOAN_PAYOUT'. You can insert those strings immediately.
-- - public.pay_advance_reason_enum EXISTS and currently contains: MISSING_SHIFT, OVERPAY_NHSP, OVERPAY_HR, MANUAL_ADVANCE. :contentReference[oaicite:0]{index=0}
--   This migration adds LOAN and OVERPAYMENT to that enum.
-- - pay_batches currently has no stored batch-kind column; existing RPCs derive kind.
--   We add pay_batches.batch_kind_fixed (non-breaking) and later you’ll amend RPCs to respect it.

-- ------------------------------------------------------------
-- A1) Extend public.pay_advance_reason_enum to include LOAN + OVERPAYMENT
-- (Fix: avoid nested $$ quoting; use format(...) in EXECUTE)
-- ------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public' AND t.typname = 'pay_advance_reason_enum'
  ) THEN
    RAISE EXCEPTION 'Expected enum public.pay_advance_reason_enum to exist.';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON t.oid = e.enumtypid
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_advance_reason_enum'
      AND e.enumlabel = 'LOAN'
  ) THEN
    EXECUTE format('ALTER TYPE public.pay_advance_reason_enum ADD VALUE %L', 'LOAN');
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON t.oid = e.enumtypid
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_advance_reason_enum'
      AND e.enumlabel = 'OVERPAYMENT'
  ) THEN
    EXECUTE format('ALTER TYPE public.pay_advance_reason_enum ADD VALUE %L', 'OVERPAYMENT');
  END IF;
END$$;

-- ------------------------------------------------------------
-- A2) New enums for advance_kind + payout_status (only if absent)
-- ------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname='public' AND t.typname='pay_advance_kind_enum'
  ) THEN
    EXECUTE 'CREATE TYPE public.pay_advance_kind_enum AS ENUM (''LOAN'',''OVERPAYMENT'',''LEGACY_ADVANCE'')';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname='public' AND t.typname='pay_advance_payout_status_enum'
  ) THEN
    EXECUTE 'CREATE TYPE public.pay_advance_payout_status_enum AS ENUM (''PENDING'',''PAID'',''CANCELLED'')';
  END IF;
END$$;

-- ------------------------------------------------------------
-- A2) pay_advances: additive columns (null-safe)
-- ------------------------------------------------------------
ALTER TABLE public.pay_advances
  ADD COLUMN IF NOT EXISTS advance_kind public.pay_advance_kind_enum;

ALTER TABLE public.pay_advances
  ADD COLUMN IF NOT EXISTS linked_timesheet_id uuid;

ALTER TABLE public.pay_advances
  ADD COLUMN IF NOT EXISTS baseline_signature text;

ALTER TABLE public.pay_advances
  ADD COLUMN IF NOT EXISTS payout_status public.pay_advance_payout_status_enum;

ALTER TABLE public.pay_advances
  ADD COLUMN IF NOT EXISTS payout_pay_batch_id uuid;

ALTER TABLE public.pay_advances
  ADD COLUMN IF NOT EXISTS payout_transfer_id uuid;

ALTER TABLE public.pay_advances
  ADD COLUMN IF NOT EXISTS weekly_due numeric(12,2);

ALTER TABLE public.pay_advances
  ADD COLUMN IF NOT EXISTS weeks_total integer;

ALTER TABLE public.pay_advances
  ADD COLUMN IF NOT EXISTS start_week_start date;

-- Default/backfill for advance_kind (safe to re-run)
ALTER TABLE public.pay_advances
  ALTER COLUMN advance_kind SET DEFAULT 'LEGACY_ADVANCE'::public.pay_advance_kind_enum;

UPDATE public.pay_advances
SET advance_kind = 'LEGACY_ADVANCE'::public.pay_advance_kind_enum
WHERE advance_kind IS NULL;

-- Optional FK constraints (only if not already present)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_advances_linked_timesheet_id_fkey'
      AND conrelid = 'public.pay_advances'::regclass
  ) THEN
    EXECUTE '
      ALTER TABLE public.pay_advances
      ADD CONSTRAINT pay_advances_linked_timesheet_id_fkey
      FOREIGN KEY (linked_timesheet_id)
      REFERENCES public.timesheets(timesheet_id)
      ON DELETE SET NULL
    ';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_advances_payout_pay_batch_id_fkey'
      AND conrelid = 'public.pay_advances'::regclass
  ) THEN
    EXECUTE '
      ALTER TABLE public.pay_advances
      ADD CONSTRAINT pay_advances_payout_pay_batch_id_fkey
      FOREIGN KEY (payout_pay_batch_id)
      REFERENCES public.pay_batches(id)
      ON DELETE SET NULL
    ';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_advances_payout_transfer_id_fkey'
      AND conrelid = 'public.pay_advances'::regclass
  ) THEN
    EXECUTE '
      ALTER TABLE public.pay_advances
      ADD CONSTRAINT pay_advances_payout_transfer_id_fkey
      FOREIGN KEY (payout_transfer_id)
      REFERENCES public.pay_bank_transfers(id)
      ON DELETE SET NULL
    ';
  END IF;
END$$;

-- ------------------------------------------------------------
-- A2) Candidate-level floor: min_take_home_wtd (global per candidate)
-- ------------------------------------------------------------
ALTER TABLE public.candidates
  ADD COLUMN IF NOT EXISTS min_take_home_wtd numeric(12,2) NOT NULL DEFAULT 0;

-- ------------------------------------------------------------
-- A1/A2) Stored batch kind marker (non-conflicting with derived batch_kind in existing RPCs)
-- ------------------------------------------------------------
ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS batch_kind_fixed text;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'pay_batches_batch_kind_fixed_chk'
      AND conrelid = 'public.pay_batches'::regclass
  ) THEN
    EXECUTE '
      ALTER TABLE public.pay_batches
      ADD CONSTRAINT pay_batches_batch_kind_fixed_chk
      CHECK (
        batch_kind_fixed IS NULL
        OR upper(batch_kind_fixed) IN (''PAYE'',''UMBRELLA'',''MIXED'',''LOANS'')
      )
    ';
  END IF;
END$$;

-- ------------------------------------------------------------
-- A3) Indexes / constraints
-- ------------------------------------------------------------

-- Partial unique index for timesheet overpayment cases:
-- Uniqueness on (candidate_id, linked_timesheet_id, baseline_signature)
-- where advance_kind=OVERPAYMENT and status in (ACTIVE, PAID_OFF).
CREATE UNIQUE INDEX IF NOT EXISTS uq_pay_advances_overpayment_case
  ON public.pay_advances (candidate_id, linked_timesheet_id, baseline_signature)
  WHERE advance_kind = 'OVERPAYMENT'::public.pay_advance_kind_enum
    AND status IN ('ACTIVE'::public.pay_advance_status_enum, 'PAID_OFF'::public.pay_advance_status_enum);

-- WTD computations / batch lookups
CREATE INDEX IF NOT EXISTS idx_pay_batches_paydate_status_kindfixed
  ON public.pay_batches (pay_date, status, batch_kind_fixed);

-- Candidate/week join helper
CREATE INDEX IF NOT EXISTS idx_pay_batch_candidates_candidate_batch
  ON public.pay_batch_candidates (candidate_id, pay_batch_id);

-- Batch item access: candidate + item_type
CREATE INDEX IF NOT EXISTS idx_pay_batch_items_candidate_itemtype
  ON public.pay_batch_items (pay_batch_candidate_id, item_type);

-- If you key loan/overpayment items by advance id in source_ref, this makes lookups fast
CREATE INDEX IF NOT EXISTS idx_pay_batch_items_source_ref
  ON public.pay_batch_items (source_ref)
  WHERE source_ref IS NOT NULL;
