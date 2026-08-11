CREATE OR REPLACE FUNCTION private.pay_workbench_draft_finance_item_plan_v1(
  p_operation_id uuid,
  p_candidate_scope_ids jsonb
)
RETURNS TABLE(
  operation_id uuid,
  candidate_scope_id uuid,
  pay_batch_id uuid,
  candidate_id uuid,
  pay_channel text,
  allocation_source_key text,
  planned_item_key text,
  planned_item_type text,
  finance_case_id uuid,
  finance_component_id uuid,
  contribution_amount numeric,
  planned_item_amount numeric,
  contribution_count integer,
  plan_digest text,
  plan_basis_json jsonb
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_scope_count integer := 0;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'pay_workbench_draft_finance_item_plan_v1: p_operation_id is required';
  END IF;

  IF p_candidate_scope_ids IS NULL OR pg_catalog.jsonb_typeof(p_candidate_scope_ids) <> 'array' THEN
    RAISE EXCEPTION 'pay_workbench_draft_finance_item_plan_v1 requires a JSON array of candidate scope ids';
  END IF;

  v_scope_count := pg_catalog.jsonb_array_length(p_candidate_scope_ids);
  IF v_scope_count = 0 OR v_scope_count > 100 THEN
    RAISE EXCEPTION 'pay_workbench_draft_finance_item_plan_v1 candidate scope count must be between 1 and 100, got %', v_scope_count;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.jsonb_array_elements_text(p_candidate_scope_ids) AS supplied(scope_id_text)
    WHERE supplied.scope_id_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) THEN
    RAISE EXCEPTION 'pay_workbench_draft_finance_item_plan_v1 requires candidate scope ids to be UUID strings';
  END IF;

  RETURN QUERY
  WITH scoped_rows AS (
    SELECT allocation_row.*,
           CASE pg_catalog.upper(pg_catalog.btrim(pg_catalog.coalesce(allocation_row.allocation_type, '')))
             WHEN 'OVERPAYMENT_RECOVERY' THEN 'OVERPAYMENT_RECOVERY'
             WHEN 'UNDERPAYMENT_PAYMENT' THEN 'UNDERPAYMENT_PAYMENT'
             WHEN 'MANUAL_DEBT_RECOVERY' THEN 'MANUAL_DEBT_RECOVERY'
             WHEN 'PAYMENT_ADVANCE_REPAYMENT' THEN 'LOAN_REPAYMENT'
             WHEN 'LOAN_REPAYMENT' THEN 'LOAN_REPAYMENT'
             WHEN 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT' THEN 'MANUAL_CREDIT_PAYOUT'
             WHEN 'MANUAL_CREDIT_PAYOUT' THEN 'MANUAL_CREDIT_PAYOUT'
             WHEN 'LOAN_PAYOUT' THEN 'LOAN_PAYOUT'
             ELSE NULL::text
           END AS effective_item_type
    FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    WHERE allocation_row.operation_id = p_operation_id
      AND allocation_row.candidate_scope_id IN (
        SELECT supplied.scope_id_text::uuid
        FROM pg_catalog.jsonb_array_elements_text(p_candidate_scope_ids) AS supplied(scope_id_text)
      )
      AND allocation_row.finance_case_id IS NOT NULL
  ), keyed_rows AS (
    SELECT scoped_row.*,
           'DRAFT_FINANCE_ITEM_V1:' || private.pay_payment_correction_sha256_v1(
             pg_catalog.jsonb_build_object(
               'contract_version', 1,
               'operation_id', scoped_row.operation_id::text,
               'candidate_scope_id', scoped_row.candidate_scope_id::text,
               'pay_batch_id', CASE WHEN scoped_row.pay_batch_id IS NULL THEN NULL ELSE scoped_row.pay_batch_id::text END,
               'candidate_id', scoped_row.candidate_id::text,
               'pay_channel', pg_catalog.upper(pg_catalog.btrim(pg_catalog.coalesce(scoped_row.pay_channel, ''))),
               'planned_item_type', scoped_row.effective_item_type,
               'finance_case_id', scoped_row.finance_case_id::text,
               'finance_component_id', CASE WHEN scoped_row.finance_component_id IS NULL THEN NULL ELSE scoped_row.finance_component_id::text END,
               'direction', CASE WHEN pg_catalog.round(pg_catalog.coalesce(scoped_row.allocated_amount, 0), 2) < 0 THEN 'DEDUCTION' ELSE 'PAYMENT' END,
               'allocation_source_key', scoped_row.operation_source_key
             )
           ) AS effective_planned_item_key
    FROM scoped_rows AS scoped_row
    WHERE scoped_row.effective_item_type = 'OVERPAYMENT_RECOVERY'
      AND pg_catalog.round(pg_catalog.coalesce(scoped_row.allocated_amount, 0), 2) <> 0
  )
  SELECT
    keyed_row.operation_id,
    keyed_row.candidate_scope_id,
    keyed_row.pay_batch_id,
    keyed_row.candidate_id,
    pg_catalog.upper(pg_catalog.btrim(pg_catalog.coalesce(keyed_row.pay_channel, ''))) AS pay_channel,
    keyed_row.operation_source_key AS allocation_source_key,
    keyed_row.effective_planned_item_key AS planned_item_key,
    keyed_row.effective_item_type AS planned_item_type,
    keyed_row.finance_case_id,
    keyed_row.finance_component_id,
    pg_catalog.round(keyed_row.allocated_amount, 2) AS contribution_amount,
    pg_catalog.round(keyed_row.allocated_amount, 2) AS planned_item_amount,
    1::integer AS contribution_count,
    private.pay_payment_correction_sha256_v1(
      pg_catalog.jsonb_build_object(
        'contract_version', 1,
        'planned_item_key', keyed_row.effective_planned_item_key,
        'planned_item_type', keyed_row.effective_item_type,
        'contribution_amount', pg_catalog.round(keyed_row.allocated_amount, 2),
        'allocation_source_key', keyed_row.operation_source_key,
        'allocation_basis_json', pg_catalog.coalesce(keyed_row.allocation_basis_json, '{}'::jsonb) - 'draft_finance_item_plan'
      )
    ) AS plan_digest,
    pg_catalog.coalesce(keyed_row.allocation_basis_json, '{}'::jsonb) - 'draft_finance_item_plan' AS plan_basis_json
  FROM keyed_rows AS keyed_row
  ORDER BY keyed_row.candidate_id, keyed_row.pay_channel, keyed_row.operation_source_key;
END;
$function$;

ALTER FUNCTION private.pay_workbench_draft_finance_item_plan_v1(uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_draft_finance_item_plan_v1(uuid,jsonb) FROM PUBLIC, anon, authenticated, service_role;
