-- Enable the independently versioned, pass-local reconciliation optimisation
-- for newly created TEST Workbench builds. In-flight builds retain their frozen
-- version. This changes orchestration only: financial formulas, Policy X,
-- timeouts, leases, lane counts, and RPC ownership remain unchanged.
DO $migration$
DECLARE
  v_updated integer := 0;
BEGIN
  UPDATE public.settings_defaults
  SET banking_pay_workbench_reconciliation_optimization_version = 1,
      updated_at = pg_catalog.clock_timestamp()
  WHERE id = 1
    AND banking_pay_workbench_reconciliation_optimization_version = 0;

  GET DIAGNOSTICS v_updated = ROW_COUNT;
  IF v_updated <> 1 THEN
    RAISE EXCEPTION
      'BANKING_PAY_RECONCILIATION_OPTIMISATION_ENABLE_PRECONDITION_FAILED';
  END IF;
END;
$migration$;
