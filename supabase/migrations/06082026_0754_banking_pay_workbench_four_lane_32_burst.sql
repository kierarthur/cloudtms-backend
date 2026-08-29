-- Persist the TEST-accepted Banking Pay bounded-source operating profile.
--
-- This changes outer orchestration only.  It does not change page size,
-- simultaneous work above four lanes, the 23-second RPC hard cap, the
-- 24-second statement timeout, the 25-second lease, attempt limits, finance
-- formulas, canonical publication, triggers or Policy X authority.

DO $migration$
DECLARE
  v_updated_rows integer := 0;
  v_final_rows integer := 0;
BEGIN
  UPDATE public.settings_defaults
  SET banking_pay_workbench_cron_source_build_parallelism = 4,
      banking_pay_workbench_nudge_source_build_parallelism = 4,
      banking_pay_workbench_cron_source_build_parallel_bursts = 32,
      banking_pay_workbench_nudge_source_build_parallel_bursts = 32,
      banking_pay_workbench_cron_source_build_runtime_floor_ms = 45000,
      banking_pay_workbench_nudge_source_build_runtime_floor_ms = 45000,
      updated_at = clock_timestamp()
  WHERE banking_pay_workbench_cron_source_build_parallelism = 1
    AND banking_pay_workbench_nudge_source_build_parallelism = 1
    AND banking_pay_workbench_cron_source_build_parallel_bursts = 32
    AND banking_pay_workbench_nudge_source_build_parallel_bursts = 32
    AND banking_pay_workbench_cron_source_build_runtime_floor_ms = 45000
    AND banking_pay_workbench_nudge_source_build_runtime_floor_ms = 45000
    AND banking_pay_workbench_db_worker_max_runtime_ms = 22000
    AND banking_pay_workbench_db_statement_timeout_ms = 24000
    AND banking_pay_workbench_db_worker_lease_seconds = 25
    AND banking_pay_workbench_rpc_safety_buffer_ms = 1000;

  GET DIAGNOSTICS v_updated_rows = ROW_COUNT;

  SELECT count(*)::integer
  INTO v_final_rows
  FROM public.settings_defaults
  WHERE banking_pay_workbench_cron_source_build_parallelism = 4
    AND banking_pay_workbench_nudge_source_build_parallelism = 4
    AND banking_pay_workbench_cron_source_build_parallel_bursts = 32
    AND banking_pay_workbench_nudge_source_build_parallel_bursts = 32
    AND banking_pay_workbench_cron_source_build_runtime_floor_ms = 45000
    AND banking_pay_workbench_nudge_source_build_runtime_floor_ms = 45000
    AND banking_pay_workbench_db_worker_max_runtime_ms = 22000
    AND banking_pay_workbench_db_statement_timeout_ms = 24000
    AND banking_pay_workbench_db_worker_lease_seconds = 25
    AND banking_pay_workbench_rpc_safety_buffer_ms = 1000;

  IF v_updated_rows > 1 OR v_final_rows <> 1 THEN
    RAISE EXCEPTION
      'BANKING_PAY_WORKBENCH_FOUR_LANE_PROFILE_BASELINE_CONFLICT: updated %, final %',
      v_updated_rows,
      v_final_rows
      USING ERRCODE = '23514';
  END IF;
END;
$migration$;
