-- Supabase currently preloads plpgsql_check with fatal errors enabled. The
-- extension can corrupt its internal pldbgapi2 call stack while this nested
-- Banking Pay finaliser runs, aborting otherwise valid PostgreSQL execution.
-- Keep the workaround scoped to this function; it does not change its body or
-- any Banking Pay economic, selection, reservation, or Policy X behaviour.
-- Reassert it on managed Miget TEST upgrades too: an older upgrade installed
-- this setting before the later V8 finalizer replacement, which cleared that
-- function-local setting while leaving the function body unchanged. This
-- repeatable restores parity with a clean NEW replay and its exact writer pin.
ALTER FUNCTION public.pay_batch_finalize_reservations_and_markers(
  uuid,
  text,
  uuid,
  date,
  date,
  uuid,
  jsonb
) SET plpgsql_check.mode TO 'disabled';
