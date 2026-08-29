-- Banking Pay Version 1.2.9 rollback-safe semantic verification.
-- This file performs no durable DML or DDL.  It raises on any failed invariant.

DO $verification$
DECLARE
  v_cursor jsonb;
  v_result jsonb;
  v_failure text;
BEGIN
  v_cursor:=jsonb_build_object(
    'cursor_kind','WORKSPACE_FACT','cursor_version',2,
    'build_id','11111111-1111-1111-1111-111111111111',
    'candidate_id','22222222-2222-2222-2222-222222222222',
    'captured_candidate_generation',7,'captured_source_change_seq',9,
    'dependency_unit_key','UNIT-1','fact_family','LIVE_ENTITLEMENT_INPUT',
    'page_number',2,'last_source_key','RAW-25','previous_page_digest',md5('page-1'),
    'cumulative_fact_count',25,'cumulative_digest',md5('cumulative'),
    'terminal',false,'raw_physical_source_count',25,
    'resolved_physical_source_count',25,'failed_physical_source_count',0,
    'raw_physical_amount_ex_vat',100,'resolved_physical_amount_ex_vat',100,
    'last_raw_physical_source_key','RAW-25','source_exhausted',false,
    'raw_terminal_source_key',NULL,'raw_page_evidence_digest',md5('evidence'),
    'input_phase','PHYSICAL_SOURCE','input_projection_id',NULL);
  IF private.pay_workbench_fact_cursor_preserve_v2(v_cursor) IS DISTINCT FROM v_cursor THEN
    RAISE EXCEPTION 'V129_CURSOR_NOT_EXACT';
  END IF;

  SELECT resolution_failure INTO v_failure
  FROM private.pay_workbench_financial_source_authority_v2(
    '22222222-2222-2222-2222-222222222222',
    ARRAY['11111111-1111-1111-1111-111111111111'::uuid],
    ARRAY['22222222-2222-2222-2222-222222222222'::uuid],ARRAY[]::uuid[],
    ARRAY['33333333-3333-3333-3333-333333333333'::uuid,
      '44444444-4444-4444-4444-444444444444'::uuid],
    '[{"key_type":"TS_TOTAL","key_value":"TOTAL"}]'::jsonb,
    '{}'::jsonb,'{}'::jsonb,'FINANCE');
  IF v_failure IS DISTINCT FROM 'FINANCE_COMPONENT_CONFLICT' THEN
    RAISE EXCEPTION 'V129_COMPONENT_CONFLICT_NOT_BLOCKED';
  END IF;

  SELECT resolution_failure INTO v_failure
  FROM private.pay_workbench_financial_source_authority_v2(
    '22222222-2222-2222-2222-222222222222',
    ARRAY['11111111-1111-1111-1111-111111111111'::uuid],
    ARRAY['22222222-2222-2222-2222-222222222222'::uuid],ARRAY[]::uuid[],
    ARRAY['33333333-3333-3333-3333-333333333333'::uuid],
    '[{"key_type":"TS_TOTAL","key_value":"TOTAL"},
      {"key_type":"TS_DAY","key_value":"2026-08-05"}]'::jsonb,
    '{}'::jsonb,'{}'::jsonb,'FINANCE');
  IF v_failure IS DISTINCT FROM 'FINANCE_COMPONENT_KEY_CONFLICT' THEN
    RAISE EXCEPTION 'V129_COMPONENT_KEY_CONFLICT_NOT_BLOCKED';
  END IF;

  v_result:=private.pay_workbench_presentation_allocation_expected_v1(
    jsonb_build_object(
      'timesheet_id','11111111-1111-1111-1111-111111111111',
      'candidate_id','22222222-2222-2222-2222-222222222222',
      'candidate_pay_method','PAYE','has_active_timesheet_snooze',false,
      'case_is_blocked',false,'is_ready_for_draft',true,
      'blocked_visible_segment_count',0,'blocked_expense_count',0,
      'ready_segment_count',1,'ready_section_amount_ex_vat',60,
      'amount_ex_vat',100,'non_segment_amount_ex_vat',0,
      'hidden_indefinite_segment_amount_ex_vat',40,'hidden_expense_amount_ex_vat',0,
      'case_resolution_summary_json','{}'::jsonb,'case_components_json','[]'::jsonb,
      'ready_segment_rows_json','[{"amount_ex_vat":60}]'::jsonb,
      'resolved_segment_rows_replace_source_total',true));
  IF jsonb_array_length(v_result)<>1
     OR (v_result->0->>'amount_ex_vat')::numeric<>60
     OR v_result->0->>'presentation_section'<>'READY_TO_PAY'
     OR (v_result->0->>'resolved_segment_rows_replace_source_total')::boolean IS NOT TRUE THEN
    RAISE EXCEPTION 'V129_PARTIAL_HIDDEN_PRESENTATION_INVALID';
  END IF;

  v_result:=private.pay_workbench_presentation_allocation_expected_v1(
    jsonb_build_object(
      'timesheet_id','11111111-1111-1111-1111-111111111111',
      'candidate_id','22222222-2222-2222-2222-222222222222',
      'candidate_pay_method','PAYE','has_active_timesheet_snooze',false,
      'case_is_blocked',false,'is_ready_for_draft',true,
      'blocked_visible_segment_count',1,'blocked_expense_count',0,
      'ready_segment_count',1,'ready_section_amount_ex_vat',80,
      'blocked_section_amount_ex_vat',20,'blocked_expense_amount_ex_vat',0,
      'amount_ex_vat',100,'non_segment_amount_ex_vat',0,
      'case_resolution_summary_json','{}'::jsonb,'case_components_json','[]'::jsonb,
      'ready_segment_rows_json','[{"amount_ex_vat":80}]'::jsonb,
      'blocked_visible_segment_rows_json','[{"amount_ex_vat":20}]'::jsonb));
  IF jsonb_array_length(v_result)<>2
     OR NOT v_result @> '[{"presentation_section":"READY_TO_PAY","amount_ex_vat":80}]'::jsonb
     OR NOT v_result @> '[{"presentation_section":"BLOCKED_FOR_PAY","amount_ex_vat":20}]'::jsonb THEN
    RAISE EXCEPTION 'V129_READY_BLOCKED_PRESENTATION_INVALID';
  END IF;

  v_result:=private.pay_workbench_finance_item_authority_page_bundle_v1(
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',NULL,NULL,25);
  IF (v_result->>'raw_page_count')::integer<>0
     OR (v_result->>'raw_source_exhausted')::boolean IS NOT TRUE
     OR jsonb_array_length(v_result->'rows')<>0 THEN
    RAISE EXCEPTION 'V129_EMPTY_PHYSICAL_BUNDLE_INVALID';
  END IF;
END;
$verification$;
