-- Banking Pay bounded-scope controlled reconciliation envelope V2.
--
-- This is a fail-closed TEST calibration envelope for the controlled bootstrap.
-- It does not cap candidate discovery or fact collection: larger candidates are
-- retained durably as BLOCKED_UNVALIDATED_RECONCILIATION_SCALE until a later,
-- measured envelope version admits their complete complexity vector.
-- Policy X, payment economics and frozen post-draft authority are unchanged.

BEGIN;

DO $migration$
DECLARE
  v_updated integer:=0;
BEGIN
  UPDATE public.settings_defaults
  SET banking_pay_workbench_reconciliation_envelope_version=2,
      banking_pay_workbench_reconciliation_envelope_json=jsonb_build_object(
        'relevant_timesheet_count',64,
        'dependency_node_count',64,
        'dependency_edge_count',512,
        'settled_source_row_count',2048,
        'settled_component_count',2048,
        'entitlement_component_count',2048,
        'reservation_component_count',512,
        'finance_case_count',512,
        'finance_component_count',2048,
        'protection_evidence_count',512,
        'expected_case_insert_count',512,
        'expected_case_update_count',512,
        'expected_case_clear_count',512,
        'expected_component_insert_count',2048,
        'expected_component_update_count',2048,
        'expected_component_close_count',2048,
        'canonical_source_row_count',2048,
        'staging_bytes',4194304
      ),
      banking_pay_workbench_reconciliation_envelope_evidence_json=jsonb_build_object(
        'evidence_status','CONTROLLED_TEST_CALIBRATION',
        'envelope_contract','BANKING_PAY_BOUNDED_SCOPE_V2',
        'candidate_discovery_truncated',false,
        'policy_x','UNCHANGED'
      )
  WHERE id=1
    AND banking_pay_workbench_reconciliation_envelope_version=1
    AND banking_pay_workbench_reconciliation_envelope_json='{}'::jsonb
    AND banking_pay_workbench_reconciliation_envelope_evidence_json='{}'::jsonb;

  GET DIAGNOSTICS v_updated=ROW_COUNT;

  IF v_updated=0 AND NOT EXISTS (
    SELECT 1
    FROM public.settings_defaults
    WHERE id=1
      AND banking_pay_workbench_reconciliation_envelope_version=2
      AND banking_pay_workbench_reconciliation_envelope_json=jsonb_build_object(
        'relevant_timesheet_count',64,
        'dependency_node_count',64,
        'dependency_edge_count',512,
        'settled_source_row_count',2048,
        'settled_component_count',2048,
        'entitlement_component_count',2048,
        'reservation_component_count',512,
        'finance_case_count',512,
        'finance_component_count',2048,
        'protection_evidence_count',512,
        'expected_case_insert_count',512,
        'expected_case_update_count',512,
        'expected_case_clear_count',512,
        'expected_component_insert_count',2048,
        'expected_component_update_count',2048,
        'expected_component_close_count',2048,
        'canonical_source_row_count',2048,
        'staging_bytes',4194304
      )
      AND banking_pay_workbench_reconciliation_envelope_evidence_json=jsonb_build_object(
        'evidence_status','CONTROLLED_TEST_CALIBRATION',
        'envelope_contract','BANKING_PAY_BOUNDED_SCOPE_V2',
        'candidate_discovery_truncated',false,
        'policy_x','UNCHANGED'
      )
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_RECONCILIATION_ENVELOPE_BASELINE_CONFLICT'
      USING ERRCODE='55000';
  END IF;
END
$migration$;

COMMIT;
