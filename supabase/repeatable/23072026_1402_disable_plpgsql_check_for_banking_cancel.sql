-- Supabase preloads plpgsql_check with fatal errors enabled. Its passive
-- pldbgapi2 stack tracking can abort deeply nested Banking Pay cancellation
-- and workbench-refresh calls with:
--   cannot find parent statement on pldbgapi2 call stack
--
-- Disable only that platform instrumentation for the bounded entry points
-- involved in draft cancellation and its post-cancel workbench refresh. This
-- changes no cancellation scope, reservation state, frozen batch artefact,
-- finance calculation, remittance behaviour, or Policy X authority.
-- Reasserted after the final retirement of obsolete monolithic Workbench
-- owners so any one-time monolith replay cannot reset this function setting.

ALTER FUNCTION public.pay_payment_cancelability_diagnostic(
  uuid,
  jsonb,
  uuid,
  text
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(
  uuid,
  jsonb,
  uuid,
  text,
  text,
  jsonb
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1(
  uuid,
  uuid,
  text,
  uuid,
  jsonb
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public.pay_workbench_session_clear_all_decisions(
  uuid,
  uuid
) SET plpgsql_check.mode TO 'disabled';
