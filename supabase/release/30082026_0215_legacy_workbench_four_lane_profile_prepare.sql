-- LEGACY_UPGRADE-only reconstruction of the exact tested predecessor state for
-- 06082026_0754_banking_pay_workbench_four_lane_32_burst.sql.
-- This file is executed atomically with that immutable migration and its legacy
-- ledger insert. It must never run as an ordinary migration or repeatable.

do $legacy_transition$
declare
  v_row_count integer;
  v_updated integer;
begin
  select count(*)::integer into v_row_count from public.settings_defaults;
  if v_row_count <> 1 then
    raise exception 'LEGACY_WORKBENCH_FOUR_LANE_PROFILE_ROW_COUNT_CONFLICT: %', v_row_count;
  end if;

  if exists (
    select 1
    from public.settings_defaults
    where id = 1
      and banking_pay_workbench_db_worker_max_runtime_ms = 22000
      and banking_pay_workbench_db_statement_timeout_ms = 24000
      and banking_pay_workbench_db_worker_lease_seconds = 25
      and banking_pay_workbench_rpc_safety_buffer_ms = 1000
      and banking_pay_workbench_cron_source_build_parallelism = 1
      and banking_pay_workbench_nudge_source_build_parallelism = 1
      and banking_pay_workbench_cron_source_build_parallel_bursts = 32
      and banking_pay_workbench_nudge_source_build_parallel_bursts = 32
      and banking_pay_workbench_cron_source_build_runtime_floor_ms = 45000
      and banking_pay_workbench_nudge_source_build_runtime_floor_ms = 45000
  ) then
    return;
  end if;

  update public.settings_defaults
  set banking_pay_workbench_cron_source_build_parallelism = 1,
      banking_pay_workbench_nudge_source_build_parallelism = 1,
      banking_pay_workbench_cron_source_build_parallel_bursts = 32,
      banking_pay_workbench_nudge_source_build_parallel_bursts = 32,
      banking_pay_workbench_cron_source_build_runtime_floor_ms = 45000,
      banking_pay_workbench_nudge_source_build_runtime_floor_ms = 45000,
      updated_at = clock_timestamp()
  where id = 1
    and banking_pay_workbench_db_worker_max_runtime_ms = 22000
    and banking_pay_workbench_db_statement_timeout_ms = 24000
    and banking_pay_workbench_db_worker_lease_seconds = 25
    and banking_pay_workbench_rpc_safety_buffer_ms = 1000
    and banking_pay_workbench_cron_source_build_parallelism = 0
    and banking_pay_workbench_nudge_source_build_parallelism = 0
    and banking_pay_workbench_cron_source_build_parallel_bursts = 4
    and banking_pay_workbench_nudge_source_build_parallel_bursts = 12
    and banking_pay_workbench_cron_source_build_runtime_floor_ms = 8000
    and banking_pay_workbench_nudge_source_build_runtime_floor_ms = 8000;

  get diagnostics v_updated = row_count;
  if v_updated <> 1 then
    raise exception 'LEGACY_WORKBENCH_FOUR_LANE_PROFILE_BASELINE_CONFLICT';
  end if;
end
$legacy_transition$;
