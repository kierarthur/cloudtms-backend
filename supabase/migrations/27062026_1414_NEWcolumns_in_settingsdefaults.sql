-- Migration: settings_defaults Banking Pay Workbench runtime controls
-- Scope: externalise existing runtime/chunk/lease/continuation/retry/timeout controls only.
-- Policy X: no payment economics, TS_DAY keying, preview/draft/remittance/settlement/freshness/export logic is changed here.

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_minimum_rpc_budget_ms integer NULL,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_rpc_safety_buffer_ms integer NOT NULL DEFAULT 750,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_stage_work_units_per_job integer NOT NULL DEFAULT 25,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_db_worker_lease_seconds integer NULL,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_db_worker_max_runtime_ms integer NOT NULL DEFAULT 8000,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_db_worker_min_phase_budget_ms integer NOT NULL DEFAULT 2500,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_auto_continuation_max_bursts integer NOT NULL DEFAULT 3,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_auto_continuation_max_runtime_ms integer NOT NULL DEFAULT 28000,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_auto_continuation_per_burst_max_runtime_ms integer NOT NULL DEFAULT 10000,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_auto_continuation_min_runtime_ms integer NOT NULL DEFAULT 7000,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_auto_continuation_max_passes integer NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_auto_continuation_claim_limit_max integer NOT NULL DEFAULT 10,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_auto_continuation_max_jobs integer NOT NULL DEFAULT 50,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_auto_continuation_max_rows integer NOT NULL DEFAULT 500,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_job_retry_base_seconds integer NOT NULL DEFAULT 30,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_job_retry_max_seconds integer NOT NULL DEFAULT 900,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_db_statement_timeout_ms integer NOT NULL DEFAULT 15000,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_db_lock_timeout_ms integer NOT NULL DEFAULT 1500,
  ADD COLUMN IF NOT EXISTS banking_pay_workbench_db_idle_tx_timeout_ms integer NOT NULL DEFAULT 30000;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_minimum_rpc_budget_ms_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_minimum_rpc_budget_ms_chk
      CHECK (banking_pay_workbench_minimum_rpc_budget_ms IS NULL OR banking_pay_workbench_minimum_rpc_budget_ms BETWEEN 1000 AND 30000);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_rpc_safety_buffer_ms_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_rpc_safety_buffer_ms_chk
      CHECK (banking_pay_workbench_rpc_safety_buffer_ms BETWEEN 100 AND 5000);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_stage_work_units_per_job_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_stage_work_units_per_job_chk
      CHECK (banking_pay_workbench_stage_work_units_per_job BETWEEN 1 AND 100);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_db_worker_lease_seconds_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_db_worker_lease_seconds_chk
      CHECK (banking_pay_workbench_db_worker_lease_seconds IS NULL OR banking_pay_workbench_db_worker_lease_seconds BETWEEN 25 AND 3600);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_db_worker_max_runtime_ms_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_db_worker_max_runtime_ms_chk
      CHECK (banking_pay_workbench_db_worker_max_runtime_ms BETWEEN 1000 AND 30000);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_db_worker_min_phase_budget_ms_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_db_worker_min_phase_budget_ms_chk
      CHECK (
        banking_pay_workbench_db_worker_min_phase_budget_ms BETWEEN 250 AND 15000
        AND banking_pay_workbench_db_worker_min_phase_budget_ms < banking_pay_workbench_db_worker_max_runtime_ms
      );
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_auto_continuation_max_bursts_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_auto_continuation_max_bursts_chk
      CHECK (banking_pay_workbench_auto_continuation_max_bursts BETWEEN 0 AND 8);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_auto_continuation_max_runtime_ms_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_auto_continuation_max_runtime_ms_chk
      CHECK (banking_pay_workbench_auto_continuation_max_runtime_ms BETWEEN 5000 AND 60000);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_auto_continuation_per_burst_max_runtime_ms_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_auto_continuation_per_burst_max_runtime_ms_chk
      CHECK (banking_pay_workbench_auto_continuation_per_burst_max_runtime_ms BETWEEN 1000 AND 15000);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_auto_continuation_min_runtime_ms_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_auto_continuation_min_runtime_ms_chk
      CHECK (
        banking_pay_workbench_auto_continuation_min_runtime_ms BETWEEN 1000 AND 15000
        AND banking_pay_workbench_auto_continuation_min_runtime_ms <= banking_pay_workbench_auto_continuation_per_burst_max_runtime_ms
      );
  END IF;


  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_auto_continuation_max_passes_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_auto_continuation_max_passes_chk
      CHECK (banking_pay_workbench_auto_continuation_max_passes BETWEEN 1 AND 2);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_auto_continuation_claim_limit_max_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_auto_continuation_claim_limit_max_chk
      CHECK (banking_pay_workbench_auto_continuation_claim_limit_max BETWEEN 1 AND 50);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_auto_continuation_max_jobs_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_auto_continuation_max_jobs_chk
      CHECK (banking_pay_workbench_auto_continuation_max_jobs BETWEEN 1 AND 150);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_auto_continuation_max_rows_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_auto_continuation_max_rows_chk
      CHECK (banking_pay_workbench_auto_continuation_max_rows BETWEEN 1 AND 5000);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_job_retry_base_seconds_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_job_retry_base_seconds_chk
      CHECK (banking_pay_workbench_job_retry_base_seconds BETWEEN 5 AND 3600);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_job_retry_max_seconds_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_job_retry_max_seconds_chk
      CHECK (
        banking_pay_workbench_job_retry_max_seconds BETWEEN 5 AND 86400
        AND banking_pay_workbench_job_retry_max_seconds >= banking_pay_workbench_job_retry_base_seconds
      );
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_db_statement_timeout_ms_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_db_statement_timeout_ms_chk
      CHECK (banking_pay_workbench_db_statement_timeout_ms BETWEEN 1000 AND 30000);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_db_lock_timeout_ms_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_db_lock_timeout_ms_chk
      CHECK (banking_pay_workbench_db_lock_timeout_ms BETWEEN 100 AND 5000);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'settings_defaults_bpw_db_idle_tx_timeout_ms_chk') THEN
    ALTER TABLE public.settings_defaults
      ADD CONSTRAINT settings_defaults_bpw_db_idle_tx_timeout_ms_chk
      CHECK (banking_pay_workbench_db_idle_tx_timeout_ms BETWEEN 5000 AND 60000);
  END IF;
END $$;
