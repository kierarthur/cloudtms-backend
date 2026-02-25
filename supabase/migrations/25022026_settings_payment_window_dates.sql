-- Add Settings Defaults: Pay eligibility window knobs (Option A)
-- Safe to run multiple times.

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS pay_eligibility_months_back integer,
  ADD COLUMN IF NOT EXISTS pay_eligibility_weeks_ahead integer;

-- Backfill + clamp to safe ranges so constraint creation is always re-runnable.
UPDATE public.settings_defaults
SET pay_eligibility_months_back = 6
WHERE pay_eligibility_months_back IS NULL
   OR pay_eligibility_months_back < 0
   OR pay_eligibility_months_back > 120;

UPDATE public.settings_defaults
SET pay_eligibility_weeks_ahead = 2
WHERE pay_eligibility_weeks_ahead IS NULL
   OR pay_eligibility_weeks_ahead < 0
   OR pay_eligibility_weeks_ahead > 52;

-- Defaults + NOT NULL (safe to re-run).
ALTER TABLE public.settings_defaults
  ALTER COLUMN pay_eligibility_months_back SET DEFAULT 6,
  ALTER COLUMN pay_eligibility_months_back SET NOT NULL,
  ALTER COLUMN pay_eligibility_weeks_ahead SET DEFAULT 2,
  ALTER COLUMN pay_eligibility_weeks_ahead SET NOT NULL;

-- CHECK constraints (Postgres has no ADD CONSTRAINT IF NOT EXISTS).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint c
    JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'public'
      AND t.relname = 'settings_defaults'
      AND c.conname = 'settings_defaults_pay_eligibility_months_back_range_chk'
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_pay_eligibility_months_back_range_chk
      CHECK (pay_eligibility_months_back BETWEEN 0 AND 120);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint c
    JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'public'
      AND t.relname = 'settings_defaults'
      AND c.conname = 'settings_defaults_pay_eligibility_weeks_ahead_range_chk'
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_pay_eligibility_weeks_ahead_range_chk
      CHECK (pay_eligibility_weeks_ahead BETWEEN 0 AND 52);
  END IF;
END
$$;
