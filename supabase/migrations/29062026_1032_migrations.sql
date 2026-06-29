-- Banking Pay Workbench delta settings and read-path indexes
-- Safe to rerun.

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_refresh_enabled boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_shadow_mode boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_enable_normal_timesheet boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_enable_readiness_only boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_enable_reservation_only boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_fallback_on_mismatch boolean NOT NULL DEFAULT true,

  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_units_per_job integer NOT NULL DEFAULT 25,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_nudge_delta_units_per_job integer NOT NULL DEFAULT 25,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_cron_delta_units_per_job integer NOT NULL DEFAULT 50,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_delta_budget_ms integer NOT NULL DEFAULT 3000,

  ADD COLUMN IF NOT EXISTS banking_pay_workbench_patch_after_batch_mutation_enabled boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_clone_rebase_enabled boolean NOT NULL DEFAULT false;

-- Backfill/enforce defaults safely if any column already existed but was nullable.
UPDATE public.settings_defaults
SET
  banking_pay_workbench_delta_refresh_enabled = COALESCE(banking_pay_workbench_delta_refresh_enabled, false),
  banking_pay_workbench_delta_shadow_mode = COALESCE(banking_pay_workbench_delta_shadow_mode, true),
  banking_pay_workbench_delta_enable_normal_timesheet = COALESCE(banking_pay_workbench_delta_enable_normal_timesheet, false),
  banking_pay_workbench_delta_enable_readiness_only = COALESCE(banking_pay_workbench_delta_enable_readiness_only, false),
  banking_pay_workbench_delta_enable_reservation_only = COALESCE(banking_pay_workbench_delta_enable_reservation_only, false),
  banking_pay_workbench_delta_fallback_on_mismatch = COALESCE(banking_pay_workbench_delta_fallback_on_mismatch, true),

  banking_pay_workbench_delta_units_per_job = COALESCE(banking_pay_workbench_delta_units_per_job, 25),
  banking_pay_workbench_nudge_delta_units_per_job = COALESCE(banking_pay_workbench_nudge_delta_units_per_job, 25),
  banking_pay_workbench_cron_delta_units_per_job = COALESCE(banking_pay_workbench_cron_delta_units_per_job, 50),
  banking_pay_workbench_delta_budget_ms = COALESCE(banking_pay_workbench_delta_budget_ms, 3000),

  banking_pay_workbench_patch_after_batch_mutation_enabled = COALESCE(banking_pay_workbench_patch_after_batch_mutation_enabled, true),
  banking_pay_workbench_clone_rebase_enabled = COALESCE(banking_pay_workbench_clone_rebase_enabled, false);

ALTER TABLE public.settings_defaults
  ALTER COLUMN banking_pay_workbench_delta_refresh_enabled SET DEFAULT false,
  ALTER COLUMN banking_pay_workbench_delta_shadow_mode SET DEFAULT true,
  ALTER COLUMN banking_pay_workbench_delta_enable_normal_timesheet SET DEFAULT false,
  ALTER COLUMN banking_pay_workbench_delta_enable_readiness_only SET DEFAULT false,
  ALTER COLUMN banking_pay_workbench_delta_enable_reservation_only SET DEFAULT false,
  ALTER COLUMN banking_pay_workbench_delta_fallback_on_mismatch SET DEFAULT true,

  ALTER COLUMN banking_pay_workbench_delta_units_per_job SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_nudge_delta_units_per_job SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_cron_delta_units_per_job SET DEFAULT 50,
  ALTER COLUMN banking_pay_workbench_delta_budget_ms SET DEFAULT 3000,

  ALTER COLUMN banking_pay_workbench_patch_after_batch_mutation_enabled SET DEFAULT true,
  ALTER COLUMN banking_pay_workbench_clone_rebase_enabled SET DEFAULT false;

ALTER TABLE public.settings_defaults
  ALTER COLUMN banking_pay_workbench_delta_refresh_enabled SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_delta_shadow_mode SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_delta_enable_normal_timesheet SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_delta_enable_readiness_only SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_delta_enable_reservation_only SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_delta_fallback_on_mismatch SET NOT NULL,

  ALTER COLUMN banking_pay_workbench_delta_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_nudge_delta_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_cron_delta_units_per_job SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_delta_budget_ms SET NOT NULL,

  ALTER COLUMN banking_pay_workbench_patch_after_batch_mutation_enabled SET NOT NULL,
  ALTER COLUMN banking_pay_workbench_clone_rebase_enabled SET NOT NULL;

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_delta_refresh_enabled IS
'Feature flag for Banking Pay Workbench candidate delta refresh. Initial deployment default is false.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_delta_shadow_mode IS
'Runs delta refresh in shadow/diagnostic mode where applicable. Initial deployment default is true.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_delta_enable_normal_timesheet IS
'Enables served delta refresh for ordinary targeted timesheet/TSFIN changes after parity is proven. Initial deployment default is false.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_delta_enable_readiness_only IS
'Enables readiness-only delta/patch behaviour where economic rows are unchanged. Initial deployment default is false.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_delta_enable_reservation_only IS
'Enables reservation-only delta/patch behaviour where economic rows are unchanged. Initial deployment default is false.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_delta_fallback_on_mismatch IS
'Forces fallback to legacy source-build path on delta parity, contract, economic-key, or safety mismatch.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_delta_units_per_job IS
'Default delta refresh units per worker job.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_nudge_delta_units_per_job IS
'Delta refresh units per nudge-driven worker job.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_cron_delta_units_per_job IS
'Delta refresh units per scheduled cron worker job.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_delta_budget_ms IS
'Soft SQL chunk budget for candidate delta refresh. The database statement timeout remains only an emergency guard.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_patch_after_batch_mutation_enabled IS
'Allows post-draft/payment/settlement preview patching of affected economic keys instead of replacement-session rebuilds.';

COMMENT ON COLUMN public.settings_defaults.banking_pay_workbench_clone_rebase_enabled IS
'Allows clone/rebase of certified simple rows from compatible previous sessions. Initial deployment default is false.';

-- Preview page index for rewritten pay_workbench_session_get_preview_page.
-- Use after the query is rewritten to filter directly on:
-- session_id, session_version, section, status = 'READY', row_ordinal, id.
CREATE INDEX IF NOT EXISTS idx_bpay_wb_preview_ready_page_v2
ON public.banking_pay_workbench_preview_rows
(session_id, session_version, section, row_ordinal, id)
WHERE status = 'READY';

-- Candidate preview index for rewritten pay_workbench_session_get_candidate_preview.
CREATE INDEX IF NOT EXISTS idx_bpay_wb_preview_ready_candidate_page_v2
ON public.banking_pay_workbench_preview_rows
(session_id, candidate_id, session_version, section, row_ordinal, id)
WHERE status = 'READY';

-- OPTIONAL ONLY AFTER EXPLAIN CONFIRMS JOB-CLAIM SCAN COST:
-- Do not enable this automatically if existing job-claim/dedupe indexes already cover the claim plan.
--
-- CREATE INDEX IF NOT EXISTS idx_bpay_workbench_jobs_due_type_session_v2
-- ON public.banking_pay_workbench_jobs
-- (status, job_type, run_at_utc, priority, id)
-- WHERE status = 'QUEUED';