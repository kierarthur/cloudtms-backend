BEGIN;

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_source_build_parallelism integer,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_source_build_parallel_bursts integer,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_source_build_runtime_floor_ms integer,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_source_build_lane_claim_limit integer,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_source_build_parallelism integer,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_source_build_parallel_bursts integer,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_source_build_runtime_floor_ms integer,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_source_build_lane_claim_limit integer;

ALTER TABLE public.settings_defaults
  ALTER COLUMN banking_pay_workbench_nudge_source_build_parallelism SET DEFAULT 4,
  ALTER COLUMN banking_pay_workbench_nudge_source_build_parallel_bursts SET DEFAULT 12,
  ALTER COLUMN banking_pay_workbench_nudge_source_build_runtime_floor_ms SET DEFAULT 60000,
  ALTER COLUMN banking_pay_workbench_nudge_source_build_lane_claim_limit SET DEFAULT 1,
  ALTER COLUMN banking_pay_workbench_cron_source_build_parallelism SET DEFAULT 2,
  ALTER COLUMN banking_pay_workbench_cron_source_build_parallel_bursts SET DEFAULT 4,
  ALTER COLUMN banking_pay_workbench_cron_source_build_runtime_floor_ms SET DEFAULT 15000,
  ALTER COLUMN banking_pay_workbench_cron_source_build_lane_claim_limit SET DEFAULT 1;

UPDATE public.settings_defaults
SET
  banking_pay_workbench_nudge_source_build_parallelism =
    COALESCE(banking_pay_workbench_nudge_source_build_parallelism, 4),
  banking_pay_workbench_nudge_source_build_parallel_bursts =
    COALESCE(banking_pay_workbench_nudge_source_build_parallel_bursts, 12),
  banking_pay_workbench_nudge_source_build_runtime_floor_ms =
    COALESCE(banking_pay_workbench_nudge_source_build_runtime_floor_ms, 60000),
  banking_pay_workbench_nudge_source_build_lane_claim_limit =
    COALESCE(banking_pay_workbench_nudge_source_build_lane_claim_limit, 1),

  banking_pay_workbench_cron_source_build_parallelism =
    COALESCE(banking_pay_workbench_cron_source_build_parallelism, 2),
  banking_pay_workbench_cron_source_build_parallel_bursts =
    COALESCE(banking_pay_workbench_cron_source_build_parallel_bursts, 4),
  banking_pay_workbench_cron_source_build_runtime_floor_ms =
    COALESCE(banking_pay_workbench_cron_source_build_runtime_floor_ms, 15000),
  banking_pay_workbench_cron_source_build_lane_claim_limit =
    COALESCE(banking_pay_workbench_cron_source_build_lane_claim_limit, 1);

ALTER TABLE public.settings_defaults
  ALTER COLUMN banking_pay_workbench_nudge_source_build_parallelism SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_nudge_source_build_parallel_bursts SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_nudge_source_build_runtime_floor_ms SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_nudge_source_build_lane_claim_limit SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_cron_source_build_parallelism SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_cron_source_build_parallel_bursts SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_cron_source_build_runtime_floor_ms SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_cron_source_build_lane_claim_limit SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'settings_defaults_bpw_nudge_source_parallelism_chk'
      AND conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_nudge_source_parallelism_chk
      CHECK (banking_pay_workbench_nudge_source_build_parallelism BETWEEN 0 AND 32);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'settings_defaults_bpw_nudge_source_bursts_chk'
      AND conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_nudge_source_bursts_chk
      CHECK (banking_pay_workbench_nudge_source_build_parallel_bursts BETWEEN 0 AND 100);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'settings_defaults_bpw_nudge_source_runtime_floor_chk'
      AND conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_nudge_source_runtime_floor_chk
      CHECK (banking_pay_workbench_nudge_source_build_runtime_floor_ms BETWEEN 0 AND 300000);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'settings_defaults_bpw_nudge_source_lane_claim_chk'
      AND conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_nudge_source_lane_claim_chk
      CHECK (banking_pay_workbench_nudge_source_build_lane_claim_limit BETWEEN 1 AND 10);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'settings_defaults_bpw_cron_source_parallelism_chk'
      AND conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_cron_source_parallelism_chk
      CHECK (banking_pay_workbench_cron_source_build_parallelism BETWEEN 0 AND 32);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'settings_defaults_bpw_cron_source_bursts_chk'
      AND conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_cron_source_bursts_chk
      CHECK (banking_pay_workbench_cron_source_build_parallel_bursts BETWEEN 0 AND 100);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'settings_defaults_bpw_cron_source_runtime_floor_chk'
      AND conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_cron_source_runtime_floor_chk
      CHECK (banking_pay_workbench_cron_source_build_runtime_floor_ms BETWEEN 0 AND 300000);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'settings_defaults_bpw_cron_source_lane_claim_chk'
      AND conrelid = 'public.settings_defaults'::regclass
  ) THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_cron_source_lane_claim_chk
      CHECK (banking_pay_workbench_cron_source_build_lane_claim_limit BETWEEN 1 AND 10);
  END IF;
END $$;

COMMIT;