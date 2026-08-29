-- Targeted Banking Pay workbench runtime/settings alignment.
-- This does not increase worker parallelism, bursts, claim limits, source-build parallelism, or concurrent drains.

UPDATE public.settings_defaults
SET
  banking_pay_workbench_db_worker_lease_seconds = 25,
  banking_pay_workbench_db_worker_max_runtime_ms = 8000,
  banking_pay_workbench_db_worker_min_phase_budget_ms = 2500,
  banking_pay_workbench_minimum_rpc_budget_ms = 10000,
  banking_pay_workbench_rpc_safety_buffer_ms = 1000,
  banking_pay_workbench_db_statement_timeout_ms = 15000,
  banking_pay_workbench_db_lock_timeout_ms = 1500,
  banking_pay_workbench_db_idle_tx_timeout_ms = 30000,
  banking_pay_workbench_auto_continuation_min_runtime_ms = 10000,
  banking_pay_workbench_auto_continuation_per_burst_max_runtime_m = 10000,
  banking_pay_workbench_source_build_units_per_job = 25,
  banking_pay_workbench_nudge_source_build_units_per_job = 25,
  banking_pay_workbench_cron_source_build_units_per_job = 25,
  updated_at = now()
WHERE id = 1
RETURNING
  id,
  banking_pay_workbench_db_worker_lease_seconds,
  banking_pay_workbench_db_worker_max_runtime_ms,
  banking_pay_workbench_db_worker_min_phase_budget_ms,
  banking_pay_workbench_minimum_rpc_budget_ms,
  banking_pay_workbench_rpc_safety_buffer_ms,
  banking_pay_workbench_db_statement_timeout_ms,
  banking_pay_workbench_db_lock_timeout_ms,
  banking_pay_workbench_db_idle_tx_timeout_ms,
  banking_pay_workbench_auto_continuation_min_runtime_ms,
  banking_pay_workbench_auto_continuation_per_burst_max_runtime_m,
  banking_pay_workbench_source_build_units_per_job,
  banking_pay_workbench_nudge_source_build_units_per_job,
  banking_pay_workbench_cron_source_build_units_per_job,
  banking_pay_workbench_nudge_claim_limit,
  banking_pay_workbench_cron_claim_limit,
  banking_pay_workbench_nudge_source_build_parallelism,
  banking_pay_workbench_nudge_source_build_parallel_bursts,
  banking_pay_workbench_cron_source_build_parallelism,
  banking_pay_workbench_cron_source_build_parallel_bursts,
  updated_at;

ALTER TABLE public.settings_defaults
  ALTER COLUMN banking_pay_workbench_db_worker_lease_seconds SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_db_worker_max_runtime_ms SET DEFAULT 8000,
  ALTER COLUMN banking_pay_workbench_db_worker_min_phase_budget_ms SET DEFAULT 2500,
  ALTER COLUMN banking_pay_workbench_minimum_rpc_budget_ms SET DEFAULT 10000,
  ALTER COLUMN banking_pay_workbench_rpc_safety_buffer_ms SET DEFAULT 1000,
  ALTER COLUMN banking_pay_workbench_db_statement_timeout_ms SET DEFAULT 15000,
  ALTER COLUMN banking_pay_workbench_db_lock_timeout_ms SET DEFAULT 1500,
  ALTER COLUMN banking_pay_workbench_db_idle_tx_timeout_ms SET DEFAULT 30000,
  ALTER COLUMN banking_pay_workbench_auto_continuation_min_runtime_ms SET DEFAULT 10000,
  ALTER COLUMN banking_pay_workbench_auto_continuation_per_burst_max_runtime_m SET DEFAULT 10000,
  ALTER COLUMN banking_pay_workbench_source_build_units_per_job SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_nudge_source_build_units_per_job SET DEFAULT 25,
  ALTER COLUMN banking_pay_workbench_cron_source_build_units_per_job SET DEFAULT 25;
