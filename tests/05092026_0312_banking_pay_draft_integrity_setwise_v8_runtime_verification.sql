\set ON_ERROR_STOP on

-- Rollback-contained negative runtime proof for the policy-neutral set-wise
-- Draft integrity owner. This consumes only the task-owned eight-row scale
-- fixture created by verify-banking-pay-draft-v8-scale-output-matrix.mjs with
-- --persist-disposable=true. It performs no provider, payment, settlement or
-- remittance action and leaves no database change.

BEGIN;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '1500ms';
SET LOCAL idle_in_transaction_session_timeout = '30s';

DO $guard$
BEGIN
  IF pg_catalog.current_database() <> 'banking_modal_v2_test' THEN
    RAISE EXCEPTION 'LOCAL_FIXTURE_ONLY';
  END IF;
END;
$guard$;

CREATE TEMPORARY TABLE pg_temp.h2_integrity_runtime_context AS
SELECT operation_row.id AS operation_id,
       pg_catalog.min(batch_row.id::text) FILTER (
         WHERE pg_catalog.upper(pg_catalog.btrim(batch_row.batch_kind_fixed)) = 'PAYE'
       )::uuid AS paye_batch_id,
       pg_catalog.min(batch_row.id::text) FILTER (
         WHERE pg_catalog.upper(pg_catalog.btrim(batch_row.batch_kind_fixed)) = 'UMBRELLA'
       )::uuid AS umbrella_batch_id
FROM public.banking_pay_operations AS operation_row
JOIN public.pay_batches AS batch_row
  ON batch_row.source_workbench_session_id = operation_row.workbench_session_id
JOIN public.pay_batch_candidates AS batch_candidate
  ON batch_candidate.pay_batch_id = batch_row.id
JOIN public.candidates AS candidate_row
  ON candidate_row.id = batch_candidate.candidate_id
WHERE operation_row.operation_type = 'DRAFT_CREATE'
  AND candidate_row.display_name LIKE 'H1-V8-SCALE-8:CANDIDATE:%'
GROUP BY operation_row.id;

DO $fixture_guard$
DECLARE
  v_operation_id uuid;
  v_paye_batch_id uuid;
  v_umbrella_batch_id uuid;
BEGIN
  IF (SELECT pg_catalog.count(*) FROM pg_temp.h2_integrity_runtime_context) <> 1 THEN
    RAISE EXCEPTION 'H2_INTEGRITY_RUNTIME_FIXTURE_OPERATION_INVALID';
  END IF;

  SELECT operation_id, paye_batch_id, umbrella_batch_id
  INTO STRICT v_operation_id, v_paye_batch_id, v_umbrella_batch_id
  FROM pg_temp.h2_integrity_runtime_context;

  IF v_paye_batch_id IS NULL OR v_umbrella_batch_id IS NULL
     OR (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_candidate_scope
         WHERE operation_id = v_operation_id) <> 4
     OR (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_candidate_allocation_rows
         WHERE operation_id = v_operation_id) <> 8 THEN
    RAISE EXCEPTION 'H2_INTEGRITY_RUNTIME_FIXTURE_SHAPE_INVALID';
  END IF;
END;
$fixture_guard$;

-- The persisted operation is terminal-complete. Integrity runs immediately
-- before terminal completion in production, so restore only that exact local
-- fixture state inside this rollback transaction.
UPDATE private.banking_pay_draft_frozen_certificate_scopes_v8 AS certificate_scope
SET freeze_state = 'FROZEN'
FROM pg_temp.h2_integrity_runtime_context AS context
WHERE certificate_scope.operation_id = context.operation_id;

DO $baseline$
DECLARE
  v_context record;
  v_result jsonb;
BEGIN
  SELECT * INTO STRICT v_context FROM pg_temp.h2_integrity_runtime_context;
  FOREACH v_result IN ARRAY ARRAY[
    public.pay_batch_assert_integrity(v_context.paye_batch_id, NULL, v_context.operation_id),
    public.pay_batch_assert_integrity(v_context.umbrella_batch_id, NULL, v_context.operation_id)
  ] LOOP
    IF NOT coalesce((v_result->>'pass')::boolean, false)
       OR pg_catalog.jsonb_array_length(coalesce(v_result->'mismatch_details', '[]'::jsonb)) <> 0 THEN
      RAISE EXCEPTION 'H2_INTEGRITY_RUNTIME_BASELINE_FAILED:%', v_result;
    END IF;
  END LOOP;
END;
$baseline$;

SAVEPOINT missing_selected_item;
UPDATE public.banking_pay_operation_candidate_allocation_rows AS allocation_row
SET allocation_basis_json = pg_catalog.jsonb_set(
      allocation_row.allocation_basis_json,
      '{preview_row_id}',
      '"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"'::jsonb
    )
FROM pg_temp.h2_integrity_runtime_context AS context
WHERE allocation_row.operation_id = context.operation_id
  AND allocation_row.pay_batch_id = context.paye_batch_id
  AND allocation_row.id = (
    SELECT target_row.id
    FROM public.banking_pay_operation_candidate_allocation_rows AS target_row
    WHERE target_row.operation_id = context.operation_id
      AND target_row.pay_batch_id = context.paye_batch_id
    ORDER BY target_row.id
    LIMIT 1
  );
DO $missing_selected_item$
DECLARE
  v_context record;
  v_result jsonb;
BEGIN
  SELECT * INTO STRICT v_context FROM pg_temp.h2_integrity_runtime_context;
  v_result := public.pay_batch_assert_integrity(
    v_context.paye_batch_id, NULL, v_context.operation_id
  );
  IF v_result->>'code' <> 'DRAFT_INTEGRITY_FAILED'
     OR NOT EXISTS (
       SELECT 1
       FROM pg_catalog.jsonb_array_elements(v_result->'mismatch_details') AS mismatch(value)
       WHERE mismatch.value->>'check_code' = 'MISSING_SELECTED_PREVIEW_ROW_ITEM'
     ) THEN
    RAISE EXCEPTION 'H2_INTEGRITY_MISSING_ITEM_NOT_REJECTED:%', v_result;
  END IF;
END;
$missing_selected_item$;
ROLLBACK TO SAVEPOINT missing_selected_item;

SAVEPOINT allocation_amount;
UPDATE public.banking_pay_operation_candidate_allocation_rows AS allocation_row
SET allocated_amount = allocation_row.allocated_amount + 0.01
FROM pg_temp.h2_integrity_runtime_context AS context
WHERE allocation_row.operation_id = context.operation_id
  AND allocation_row.pay_batch_id = context.paye_batch_id
  AND allocation_row.id = (
    SELECT target_row.id
    FROM public.banking_pay_operation_candidate_allocation_rows AS target_row
    WHERE target_row.operation_id = context.operation_id
      AND target_row.pay_batch_id = context.paye_batch_id
    ORDER BY target_row.id
    LIMIT 1
  );
DO $allocation_amount$
DECLARE
  v_context record;
  v_result jsonb;
BEGIN
  SELECT * INTO STRICT v_context FROM pg_temp.h2_integrity_runtime_context;
  v_result := public.pay_batch_assert_integrity(
    v_context.paye_batch_id, NULL, v_context.operation_id
  );
  IF v_result->>'code' <> 'DRAFT_INTEGRITY_FAILED'
     OR NOT EXISTS (
       SELECT 1
       FROM pg_catalog.jsonb_array_elements(v_result->'mismatch_details') AS mismatch(value)
       WHERE mismatch.value->>'check_code' = 'ALLOCATION_ITEM_AMOUNT_MISMATCH'
     ) THEN
    RAISE EXCEPTION 'H2_INTEGRITY_ALLOCATION_AMOUNT_NOT_REJECTED:%', v_result;
  END IF;
END;
$allocation_amount$;
ROLLBACK TO SAVEPOINT allocation_amount;

SAVEPOINT scope_totals;
CREATE TEMPORARY TABLE pg_temp.h2_integrity_target_item ON COMMIT DROP AS
SELECT allocation_row.id AS allocation_id,
       allocation_row.pay_batch_item_id AS item_id
FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
JOIN pg_temp.h2_integrity_runtime_context AS context
  ON context.operation_id = allocation_row.operation_id
 AND context.paye_batch_id = allocation_row.pay_batch_id
ORDER BY allocation_row.id
LIMIT 1;

UPDATE public.banking_pay_operation_candidate_allocation_rows AS allocation_row
SET allocated_amount = allocation_row.allocated_amount + 0.01
FROM pg_temp.h2_integrity_target_item AS target_item
WHERE allocation_row.id = target_item.allocation_id;

UPDATE public.pay_batch_items AS item_row
SET amount_ex_vat = item_row.amount_ex_vat + 0.01,
    amount_inc_vat = item_row.amount_inc_vat + 0.01
FROM pg_temp.h2_integrity_target_item AS target_item
WHERE item_row.id = target_item.item_id;

UPDATE public.pay_batch_item_breakdowns AS breakdown_row
SET amount_ex_vat = breakdown_row.amount_ex_vat + 0.01,
    amount_inc_vat = breakdown_row.amount_inc_vat + 0.01
FROM pg_temp.h2_integrity_target_item AS target_item
WHERE breakdown_row.pay_batch_item_id = target_item.item_id;

DO $scope_totals$
DECLARE
  v_context record;
  v_result jsonb;
BEGIN
  SELECT * INTO STRICT v_context FROM pg_temp.h2_integrity_runtime_context;
  v_result := public.pay_batch_assert_integrity(
    v_context.paye_batch_id, NULL, v_context.operation_id
  );
  IF v_result->>'code' <> 'DRAFT_INTEGRITY_FAILED'
     OR NOT EXISTS (
       SELECT 1
       FROM pg_catalog.jsonb_array_elements(v_result->'mismatch_details') AS mismatch(value)
       WHERE mismatch.value->>'check_code' = 'CANDIDATE_SCOPE_TOTAL_MISMATCH'
     )
     OR NOT EXISTS (
       SELECT 1
       FROM pg_catalog.jsonb_array_elements(v_result->'mismatch_details') AS mismatch(value)
       WHERE mismatch.value->>'check_code' = 'PAY_CHANNEL_SCOPE_TOTAL_MISMATCH'
     ) THEN
    RAISE EXCEPTION 'H2_INTEGRITY_SCOPE_TOTALS_NOT_REJECTED:%', v_result;
  END IF;
END;
$scope_totals$;
ROLLBACK TO SAVEPOINT scope_totals;

DO $final_baseline$
DECLARE
  v_context record;
  v_result jsonb;
BEGIN
  SELECT * INTO STRICT v_context FROM pg_temp.h2_integrity_runtime_context;
  v_result := public.pay_batch_assert_integrity(
    v_context.paye_batch_id, NULL, v_context.operation_id
  );
  IF NOT coalesce((v_result->>'pass')::boolean, false) THEN
    RAISE EXCEPTION 'H2_INTEGRITY_RUNTIME_FINAL_BASELINE_FAILED:%', v_result;
  END IF;
  RAISE NOTICE 'H2_DRAFT_INTEGRITY_SETWISE_RUNTIME_PASS={"baseline_batches":2,"typed_negative_checks":4,"transaction_outcome":"ROLLBACK","external_payment_actions":0}';
END;
$final_baseline$;

ROLLBACK;
