-- Supabase currently preloads plpgsql_check with fatal errors enabled. The
-- extension can corrupt its internal pldbgapi2 call stack while this nested
-- Banking Pay finaliser runs, aborting otherwise valid PostgreSQL execution.
-- Keep the workaround scoped to this function; it does not change its body or
-- any Banking Pay economic, selection, reservation, or Policy X behaviour.
ALTER FUNCTION public.pay_batch_finalize_reservations_and_markers(
  uuid,
  text,
  uuid,
  date,
  date,
  uuid,
  jsonb
) SET plpgsql_check.mode TO 'disabled';
