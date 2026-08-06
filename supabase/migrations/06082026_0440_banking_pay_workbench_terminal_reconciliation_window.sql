-- Bounded Banking Pay Workbench terminal reconciliation execution window.
--
-- The controlled one-lane gate proved that the terminal reconciliation remains
-- active beyond the former 14-second transport ceiling.  This guarded TEST
-- calibration permits a 23-second RPC hard cap while retaining a 24-second
-- PostgreSQL statement timeout inside the unchanged 25-second attempt lease.
-- The Worker still requires enough outer runtime for RPC 1, RPC 2, result and
-- cancellation reserves before it can claim an attempt.
--
-- Policy X: no economic key, amount, VAT, PAYE/Umbrella, settlement,
-- remittance, frozen-artifact or canonical-publication formula is changed.

DO $migration$
DECLARE
  v_rows_updated integer := 0;
BEGIN
  UPDATE public.settings_defaults
  SET
    banking_pay_workbench_db_worker_max_runtime_ms = 22000,
    banking_pay_workbench_db_statement_timeout_ms = 24000,
    updated_at = clock_timestamp()
  WHERE id = 1
    AND banking_pay_workbench_db_worker_max_runtime_ms = 14000
    AND banking_pay_workbench_db_statement_timeout_ms = 15000
    AND banking_pay_workbench_db_worker_lease_seconds = 25
    AND banking_pay_workbench_rpc_safety_buffer_ms = 1000
    AND banking_pay_workbench_cron_source_build_parallelism = 0
    AND banking_pay_workbench_nudge_source_build_parallelism = 0;

  GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

  IF v_rows_updated = 0 AND NOT EXISTS (
    SELECT 1
    FROM public.settings_defaults AS sd
    WHERE sd.id = 1
      AND sd.banking_pay_workbench_db_worker_max_runtime_ms = 22000
      AND sd.banking_pay_workbench_db_statement_timeout_ms = 24000
      AND sd.banking_pay_workbench_db_worker_lease_seconds = 25
      AND sd.banking_pay_workbench_rpc_safety_buffer_ms = 1000
      AND sd.banking_pay_workbench_cron_source_build_parallelism = 0
      AND sd.banking_pay_workbench_nudge_source_build_parallelism = 0
  ) THEN
    RAISE EXCEPTION
      USING ERRCODE = 'P0001',
            MESSAGE = 'BANKING_PAY_WORKBENCH_TERMINAL_WINDOW_BASELINE_CONFLICT';
  END IF;
END
$migration$;
