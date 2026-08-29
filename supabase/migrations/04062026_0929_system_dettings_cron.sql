BEGIN;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_enabled boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_claim_limit integer NOT NULL DEFAULT 50,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_max_passes integer NOT NULL DEFAULT 2,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_max_jobs integer NOT NULL DEFAULT 110,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_max_rows integer NOT NULL DEFAULT 750,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_max_runtime_ms integer NOT NULL DEFAULT 15000,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_enabled boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_claim_limit integer NOT NULL DEFAULT 50,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_max_passes integer NOT NULL DEFAULT 2,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_max_jobs integer NOT NULL DEFAULT 110,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_max_rows integer NOT NULL DEFAULT 750,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_max_runtime_ms integer NOT NULL DEFAULT 15000;

INSERT INTO public.settings_defaults (id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;

UPDATE public.settings_defaults AS settings_row
SET banking_pay_workbench_cron_enabled = true,
    banking_pay_workbench_cron_claim_limit = 50,
    banking_pay_workbench_cron_max_passes = 2,
    banking_pay_workbench_cron_max_jobs = 110,
    banking_pay_workbench_cron_max_rows = 750,
    banking_pay_workbench_cron_max_runtime_ms = 15000,
    banking_pay_workbench_nudge_enabled = true,
    banking_pay_workbench_nudge_claim_limit = 50,
    banking_pay_workbench_nudge_max_passes = 2,
    banking_pay_workbench_nudge_max_jobs = 110,
    banking_pay_workbench_nudge_max_rows = 750,
    banking_pay_workbench_nudge_max_runtime_ms = 15000,
    updated_at = now()
WHERE settings_row.id = 1;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conname = 'settings_defaults_bpw_cron_claim_limit_chk'
      AND constraint_row.conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_cron_claim_limit_chk
      CHECK (banking_pay_workbench_cron_claim_limit BETWEEN 1 AND 100);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conname = 'settings_defaults_bpw_cron_max_passes_chk'
      AND constraint_row.conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_cron_max_passes_chk
      CHECK (banking_pay_workbench_cron_max_passes BETWEEN 1 AND 2);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conname = 'settings_defaults_bpw_cron_max_jobs_chk'
      AND constraint_row.conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_cron_max_jobs_chk
      CHECK (banking_pay_workbench_cron_max_jobs BETWEEN 1 AND 150);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conname = 'settings_defaults_bpw_cron_max_rows_chk'
      AND constraint_row.conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_cron_max_rows_chk
      CHECK (banking_pay_workbench_cron_max_rows BETWEEN 1 AND 5000);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conname = 'settings_defaults_bpw_cron_max_runtime_ms_chk'
      AND constraint_row.conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_cron_max_runtime_ms_chk
      CHECK (banking_pay_workbench_cron_max_runtime_ms BETWEEN 1000 AND 30000);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conname = 'settings_defaults_bpw_nudge_claim_limit_chk'
      AND constraint_row.conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_nudge_claim_limit_chk
      CHECK (banking_pay_workbench_nudge_claim_limit BETWEEN 1 AND 100);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conname = 'settings_defaults_bpw_nudge_max_passes_chk'
      AND constraint_row.conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_nudge_max_passes_chk
      CHECK (banking_pay_workbench_nudge_max_passes BETWEEN 1 AND 2);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conname = 'settings_defaults_bpw_nudge_max_jobs_chk'
      AND constraint_row.conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_nudge_max_jobs_chk
      CHECK (banking_pay_workbench_nudge_max_jobs BETWEEN 1 AND 150);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conname = 'settings_defaults_bpw_nudge_max_rows_chk'
      AND constraint_row.conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_nudge_max_rows_chk
      CHECK (banking_pay_workbench_nudge_max_rows BETWEEN 1 AND 5000);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint AS constraint_row
    WHERE constraint_row.conname = 'settings_defaults_bpw_nudge_max_runtime_ms_chk'
      AND constraint_row.conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_nudge_max_runtime_ms_chk
      CHECK (banking_pay_workbench_nudge_max_runtime_ms BETWEEN 1000 AND 30000);
  END IF;
END $$;

COMMIT;
