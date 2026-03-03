-- CloudTMS — Policy X / Timesheet Advance schema additions
-- Safe to re-run (idempotent).
-- Source of truth: latest TABLES IN DB.txt you uploaded (pay_batches ends at batch_kind_fixed; pay_batch_candidates lacks overpayment fields).

-- 1) Enum for override mode (NONE / TIMESHEET_ADVANCE)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname = 'public'
      AND t.typname = 'pay_override_mode_enum'
  ) THEN
    EXECUTE 'CREATE TYPE public.pay_override_mode_enum AS ENUM (''NONE'',''TIMESHEET_ADVANCE'')';
  END IF;
END$$;

-- 2) pay_batches: persist Timesheet Advance inputs/audit fields
ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS override_mode public.pay_override_mode_enum;

ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS override_reason text;

ALTER TABLE public.pay_batches
  ADD COLUMN IF NOT EXISTS force_include_timesheet_ids uuid[];

-- (Optional) default override_mode (kept NULL by default unless explicitly set)
-- If you want a default for new rows, uncomment:
-- ALTER TABLE public.pay_batches
--   ALTER COLUMN override_mode SET DEFAULT 'NONE'::public.pay_override_mode_enum;

-- 3) pay_batch_candidates: new summary fields for Policy X deductions + PAYE deferral
ALTER TABLE public.pay_batch_candidates
  ADD COLUMN IF NOT EXISTS overpayment_recovery_taken numeric(12,2) NOT NULL DEFAULT 0;

ALTER TABLE public.pay_batch_candidates
  ADD COLUMN IF NOT EXISTS awaiting_net_amount boolean NOT NULL DEFAULT false;

-- 4) Optional helper indexes (minimal, only if you want them)
-- Fast filter by override_mode (rarely needed; safe to omit if you prefer)
CREATE INDEX IF NOT EXISTS idx_pay_batches_override_mode
  ON public.pay_batches (override_mode);

-- Fast membership queries if you ever need to find batches that force-included a timesheet
-- (GIN is the correct index type for uuid[] array membership)
CREATE INDEX IF NOT EXISTS idx_pay_batches_force_include_ts_gin
  ON public.pay_batches
  USING gin (force_include_timesheet_ids);
