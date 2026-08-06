-- Bounded Banking Pay Workbench reconciliation execution window.
--
-- The Worker-side setting loader has always accepted up to 30 seconds, but the
-- drain execution boundary previously imposed an accidental eight-second hard
-- ceiling.  Reconciliation for the controlled bootstrap has demonstrated that
-- eight seconds is too short while remaining below the existing PostgreSQL
-- statement timeout.  This migration changes only the configured Worker
-- runtime window; the 15-second statement timeout, 25-second lease and
-- one-second safety buffer remain unchanged.
--
-- Policy X: no economic key, amount, VAT, PAYE/Umbrella, settlement,
-- remittance, frozen-artifact or canonical-publication formula is changed.

DO $migration$
DECLARE
  v_rows_updated integer := 0;
BEGIN
  UPDATE public.settings_defaults
  SET
    banking_pay_workbench_db_worker_max_runtime_ms = 14000,
    updated_at = clock_timestamp()
  WHERE id = 1
    AND banking_pay_workbench_db_worker_max_runtime_ms = 8000
    AND banking_pay_workbench_db_statement_timeout_ms = 15000
    AND banking_pay_workbench_db_worker_lease_seconds = 25
    AND banking_pay_workbench_rpc_safety_buffer_ms = 1000;

  GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

  IF v_rows_updated = 0 AND NOT EXISTS (
    SELECT 1
    FROM public.settings_defaults AS sd
    WHERE sd.id = 1
      AND sd.banking_pay_workbench_db_worker_max_runtime_ms = 14000
      AND sd.banking_pay_workbench_db_statement_timeout_ms = 15000
      AND sd.banking_pay_workbench_db_worker_lease_seconds = 25
      AND sd.banking_pay_workbench_rpc_safety_buffer_ms = 1000
  ) THEN
    RAISE EXCEPTION
      USING ERRCODE = 'P0001',
            MESSAGE = 'BANKING_PAY_WORKBENCH_RECONCILIATION_WINDOW_BASELINE_CONFLICT';
  END IF;
END
$migration$;
