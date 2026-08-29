BEGIN;

-- Source-build jobs are deliberately claimed by their bounded serial lane.
-- Keep the lane threshold within the configured database worker budget so
-- nudge and cron drains can claim one queued source-build job without
-- increasing parallelism, fan-out, trigger work, or financial scope.
ALTER TABLE public.settings_defaults
  ALTER COLUMN banking_pay_workbench_nudge_source_build_runtime_floor_ms SET DEFAULT 8000,
  ALTER COLUMN banking_pay_workbench_cron_source_build_runtime_floor_ms SET DEFAULT 8000;

UPDATE public.settings_defaults
SET
  banking_pay_workbench_nudge_source_build_runtime_floor_ms = 8000,
  banking_pay_workbench_cron_source_build_runtime_floor_ms = 8000
WHERE id = 1
  AND (
    banking_pay_workbench_nudge_source_build_runtime_floor_ms IS DISTINCT FROM 8000
    OR banking_pay_workbench_cron_source_build_runtime_floor_ms IS DISTINCT FROM 8000
  );

COMMIT;
