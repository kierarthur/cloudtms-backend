-- Runtime contract checks for James physical rate authority.
-- Run only against TEST after the three repeatables are installed.
\ir ../supabase/verification/13082026_1943_banking_pay_james_rate_authority_readonly.sql

DO $verification$
DECLARE
  v_helper_definition text;
  v_serializer_definition text;
  v_sync_definition text;
BEGIN
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_workbench_sealed_rate_component_projection_v1(uuid,uuid,uuid[])'::regprocedure)
  INTO STRICT v_helper_definition;
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_workbench_unit_economic_occurrence_page_v1(uuid,text,text,uuid,text,integer)'::regprocedure)
  INTO STRICT v_serializer_definition;
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_sync_overpayments_from_workbench_workspace_v1(uuid,uuid,uuid,uuid,date,date,uuid,text,uuid[],jsonb,uuid,uuid[],uuid[])'::regprocedure)
  INTO STRICT v_sync_definition;

  IF v_helper_definition ~* 'public\.(timesheets|timesheets_financials|candidates|umbrellas|settings_finance_windows)'
     OR v_helper_definition ~* 'pay_preview_candidate_build_case_component_rows' THEN
    RAISE EXCEPTION 'JAMES_RATE_HELPER_LIVE_AUTHORITY_READ_DETECTED';
  END IF;

  IF position('rate_authority_version' in v_serializer_definition)=0
     OR position('physical_bucket_digest' in v_serializer_definition)=0
     OR position('builder_comparison_digest' in v_serializer_definition)=0
     OR position('financial_digest_version' in v_serializer_definition)=0 THEN
    RAISE EXCEPTION 'JAMES_RATE_SERIALIZER_CONTRACT_MISSING';
  END IF;

  IF v_sync_definition ~* 'preliminary_outstanding_allocation|preliminary_allocations|final_allocations|preview_truth_weight_total'
     OR v_sync_definition ~ '''source_pay_method''\s*,\s*v_scope'
     OR position('PAY_WORKBENCH_CANONICAL_PHYSICAL_COMPONENT_MISMATCH' in v_sync_definition)=0 THEN
    RAISE EXCEPTION 'JAMES_RATE_SYNCHRONIZER_PHYSICAL_OWNER_FAILED';
  END IF;
END;
$verification$;
