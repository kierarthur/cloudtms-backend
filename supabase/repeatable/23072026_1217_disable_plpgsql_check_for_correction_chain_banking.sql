-- Supabase currently preloads plpgsql_check with fatal errors enabled. During
-- the deeply nested, bounded correction-chain Banking Pay calculation, the
-- extension can corrupt its own pldbgapi2 statement stack and abort otherwise
-- valid PostgreSQL execution.
--
-- Keep the workaround scoped to the functions that enter or perform this
-- pre-draft calculation, the two bounded worker entry points that retain
-- the nested call stack, and the bounded finance-case restructure entry point
-- used when a correction chain crosses PAYE/umbrella. This changes no function
-- body, economic value,
-- component identity, payment state, frozen batch artefact, or Policy X rule.

ALTER FUNCTION public.pay_correction_chain_residual_v1(
  uuid,
  uuid,
  text,
  uuid,
  uuid,
  integer
) SET plpgsql_check.mode TO 'disabled';

-- The residual reads frozen settled source authority through this bounded
-- helper.  Keeping the same checker guard on the nested PL/pgSQL frame avoids
-- pldbgapi2 corrupting its parent stack while the outer residual is running.
ALTER FUNCTION public._pay_batch_item_source_reservation_amount_ex_vat(
  uuid
) SET plpgsql_check.mode TO 'disabled';

-- The candidate scan classifies each Timesheet before entering the guarded
-- chain calculation.  Supabase's fatal checker can otherwise lose the parent
-- PL/pgSQL statement while this nested classifier is evaluated in the FOR
-- query, even though the query itself is bounded to 100 current rows.
ALTER FUNCTION public._ctms_import_correction_classify_v1(
  uuid
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public._ctms_candidate_correction_residuals_v1(
  uuid,
  uuid,
  uuid,
  text
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public._ctms_materialise_candidate_correction_residuals_v1(
  uuid,
  uuid,
  uuid,
  timestamptz
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public._ctms_rewrite_sync_correction_cases_v1(
  uuid,
  uuid[],
  uuid[]
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

ALTER FUNCTION public.pay_finance_case_apply_taxable_channel_restructure(
  uuid,
  uuid,
  text,
  text,
  integer,
  numeric,
  numeric,
  date,
  text
) SET plpgsql_check.mode TO 'disabled';
