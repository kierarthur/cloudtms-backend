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

-- The residual reads each correction member's frozen policy envelope through
-- this PL/pgSQL helper.  It is the immediate nested frame at the residual
-- assignment where Supabase has also emitted the pldbgapi2 parent-stack
-- failure, so guard it without changing any policy or financial result.
ALTER FUNCTION public._ctms_correction_policy_leg_read_v1(
  uuid
) SET plpgsql_check.mode TO 'disabled';

-- The residual reads frozen settled source authority through this bounded
-- helper.  Keeping the same checker guard on the nested PL/pgSQL frame avoids
-- pldbgapi2 corrupting its parent stack while the outer residual is running.
ALTER FUNCTION public._pay_batch_item_source_reservation_amount_ex_vat(
  uuid
) SET plpgsql_check.mode TO 'disabled';

-- Candidate refresh may validate the frozen active-draft economic key while
-- the same correction-chain call stack is still open. Guard this frozen-only
-- resolver as well; its Policy X inputs and deterministic result are unchanged.
ALTER FUNCTION public._pay_policy_x_resolve_post_draft_economic_key(
  uuid,
  uuid,
  uuid,
  text,
  text,
  text,
  jsonb,
  jsonb,
  jsonb,
  jsonb
) SET plpgsql_check.mode TO 'disabled';

-- The candidate scan classifies each Timesheet before entering the guarded
-- chain calculation.  Supabase's fatal checker can otherwise lose the parent
-- PL/pgSQL statement while this nested classifier is evaluated in the FOR
-- query, even though the query itself is bounded to 100 current rows.
ALTER FUNCTION public._ctms_import_correction_classify_v1(
  uuid
) SET plpgsql_check.mode TO 'disabled';

-- Payload freshness enters the same bounded correction-chain classifier for
-- every linked member.  The fatal checker has been observed corrupting this
-- parent frame even when the nested residual helpers are already guarded.
ALTER FUNCTION public._ctms_assert_payload_corrections_fresh_v1(
  jsonb,
  text
) SET plpgsql_check.mode TO 'disabled';

-- The correction-chain scope CTE is the first bounded lineage frame used by
-- source build and freshness validation.  Guard that exact frame as well;
-- this changes no lineage result or financial authority.
ALTER FUNCTION public.timesheet_correction_chain_scope_v1(
  uuid,
  boolean,
  integer,
  integer
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public._ctms_candidate_correction_residuals_v1(
  uuid,
  uuid,
  uuid,
  text
) SET plpgsql_check.mode TO 'disabled';

-- Source-build and sync both iterate the residual helper from a bounded
-- PL/pgSQL FOR query. The extension has also been observed losing the parent
-- statement at these immediate frames even when the residual itself is
-- guarded, so keep both entry frames disabled as well.
ALTER FUNCTION public._ctms_rewrite_source_build_correction_negative_components_v1(
  uuid,
  uuid,
  uuid[]
) SET plpgsql_check.mode TO 'disabled';

ALTER FUNCTION public._ctms_rewrite_sync_authoritative_correction_negative_components_v1(
  uuid,
  uuid,
  uuid[]
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

-- Import-review follow-up recalculates a bounded set of correction-pair TSFIN
-- snapshots through this batch writer. The function catches failures per row,
-- so the extension's corrupted parent frame otherwise becomes a retrying
-- ts_financials_outbox error instead of surfacing at the outer RPC boundary.
-- Guarding this exact entry point leaves the per-row validation, snapshot
-- economics, outbox retry contract and ordinary TSFIN body unchanged.
ALTER FUNCTION public.tsfin_write_snapshots_and_complete(
  jsonb
) SET plpgsql_check.mode TO 'disabled';
