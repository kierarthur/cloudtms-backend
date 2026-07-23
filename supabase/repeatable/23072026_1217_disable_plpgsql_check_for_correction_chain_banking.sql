-- Supabase currently preloads plpgsql_check with fatal errors enabled. During
-- the deeply nested, bounded correction-chain Banking Pay calculation, the
-- extension can corrupt its own pldbgapi2 statement stack and abort otherwise
-- valid PostgreSQL execution.
--
-- Keep the workaround scoped to the five functions that enter or perform this
-- pre-draft calculation plus the two bounded worker entry points that retain
-- the nested call stack. This changes no function body, economic value,
-- component identity, payment state, frozen batch artefact, or Policy X rule.

ALTER FUNCTION public.pay_correction_chain_residual_v1(
  uuid,
  uuid,
  text,
  uuid,
  uuid,
  integer
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public._ctms_candidate_correction_residuals_v1(
  uuid,
  uuid,
  uuid,
  text
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public.pay_workbench_candidate_source_build_chunk(
  uuid,
  uuid,
  jsonb,
  jsonb,
  integer
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public.pay_sync_overpayments_from_preview(
  date,
  date,
  uuid,
  text,
  uuid[],
  jsonb,
  uuid,
  uuid[],
  uuid[]
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public.pay_preview_candidate_collect_scope(
  jsonb,
  uuid,
  jsonb,
  integer
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public.pay_workbench_worker_drain_chunk(
  integer,
  timestamptz,
  uuid,
  uuid,
  text[],
  text,
  integer
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public.pay_workbench_worker_drain_chunk_revalidated_v1(
  integer,
  timestamptz,
  uuid,
  uuid,
  text[],
  text,
  integer
) SET plpgsql_check.mode TO 'disabled';
