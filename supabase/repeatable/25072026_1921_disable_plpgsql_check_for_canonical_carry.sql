-- Supabase currently preloads plpgsql_check with fatal errors enabled. The
-- canonical-correction source-build path invokes this bounded carry processor
-- from an already deeply nested Banking Pay worker transaction. In that
-- specific nesting, the extension can corrupt its own pldbgapi2 statement
-- stack and abort otherwise valid PostgreSQL execution.
--
-- Keep the workaround scoped to the new carry processor. This changes no
-- function body, correction identity, economic value, selection, reservation,
-- frozen batch artefact, or Policy X rule.
ALTER FUNCTION public._pay_workbench_case_resolution_carry_process_candidate_v1(
  uuid,
  uuid,
  uuid,
  timestamptz
) SET plpgsql_check.mode TO 'disabled';
