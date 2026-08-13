-- Banking Pay Version 1.2.10 rollback-safe semantic verification.
-- This file creates no durable relation and performs no financial DML.

DO $verification$
DECLARE
  v_candidate uuid:='00000000-0000-4000-8000-000000000001'::uuid;
  v_other_candidate uuid:='00000000-0000-4000-8000-000000000002'::uuid;
  v_owner_a uuid:='10000000-0000-4000-8000-000000000001'::uuid;
  v_owner_b uuid:='10000000-0000-4000-8000-000000000002'::uuid;
  v_component_a uuid:='20000000-0000-4000-8000-000000000001'::uuid;
  v_component_b uuid:='20000000-0000-4000-8000-000000000002'::uuid;
  v_cursor jsonb;
  v_transition jsonb;
  v_expected jsonb;
  v_authority record;
BEGIN
  v_cursor:=jsonb_build_object(
    'cursor_kind','WORKSPACE_FACT','cursor_version',2,
    'build_id','30000000-0000-4000-8000-000000000001',
    'candidate_id',v_candidate,
    'captured_candidate_generation',7,'captured_source_change_seq',11,
    'dependency_unit_key','UNIT:1','fact_family','CANONICAL_INPUT',
    'page_number',3,'last_source_key','ROW:25',
    'previous_page_digest',md5('previous'),'cumulative_fact_count',25,
    'cumulative_digest',md5('cumulative'),'terminal',true,
    'raw_physical_source_count',25,'resolved_physical_source_count',25,
    'failed_physical_source_count',0,'raw_physical_amount_ex_vat',100,
    'resolved_physical_amount_ex_vat',100,
    'last_raw_physical_source_key','ROW:25','source_exhausted',true,
    'raw_terminal_source_key','ROW:25','raw_page_evidence_digest',md5('evidence'),
    'input_phase','COMPONENTS','input_projection_id',v_owner_a);

  v_transition:=private.pay_workbench_fact_cursor_transition_v3(
    v_cursor,'GLOBAL','RESERVATION_COMPONENT','PHYSICAL_SOURCE',NULL);
  IF v_transition->>'input_phase'<>'PHYSICAL_SOURCE'
     OR v_transition->>'dependency_unit_key'<>'GLOBAL'
     OR v_transition->>'fact_family'<>'RESERVATION_COMPONENT'
     OR v_transition->'input_projection_id'<>'null'::jsonb
     OR (SELECT count(*) FROM jsonb_object_keys(v_transition))<>25
     OR private.pay_workbench_fact_cursor_preserve_v2(v_transition)
        IS DISTINCT FROM v_transition THEN
    RAISE EXCEPTION 'V1210_GLOBAL_CURSOR_TRANSITION_FAILED';
  END IF;

  BEGIN
    PERFORM private.pay_workbench_fact_cursor_transition_v3(
      v_cursor,'GLOBAL','RESERVATION_COMPONENT',NULL,NULL);
    RAISE EXCEPTION 'V1210_NULL_GLOBAL_PHASE_ACCEPTED';
  EXCEPTION WHEN SQLSTATE '22023' THEN
    IF SQLERRM<>'PAY_WORKBENCH_FACT_CURSOR_TRANSITION_INVALID' THEN RAISE; END IF;
  END;

  IF private.pay_workbench_physical_source_continuation_v1(false,true,true)
       IS DISTINCT FROM true
     OR private.pay_workbench_physical_source_continuation_v1(false,true,false)
       IS DISTINCT FROM false
     OR private.pay_workbench_physical_source_continuation_v1(true,false,false)
       IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'V1210_PROJECTION_CONTINUATION_FAILED';
  END IF;
  BEGIN
    PERFORM private.pay_workbench_physical_source_continuation_v1(true,true,true);
    RAISE EXCEPTION 'V1210_INVALID_RAW_EXHAUSTION_ACCEPTED';
  EXCEPTION WHEN SQLSTATE '23514' THEN
    IF SQLERRM<>'PAY_WORKBENCH_PHYSICAL_SOURCE_EVIDENCE_INVALID' THEN RAISE; END IF;
  END;

  SELECT * INTO STRICT v_authority
  FROM private.pay_workbench_financial_source_authority_v3(
    v_candidate,ARRAY[v_owner_b],ARRAY[v_candidate],ARRAY[]::uuid[],
    ARRAY[]::uuid[],jsonb_build_array(jsonb_build_object(
      'source','RESERVATION_COLUMNS','key_type','TS_TOTAL','key_value','TOTAL')),
    jsonb_build_array(
      jsonb_build_object('source','RESERVATION','document','{}'::jsonb),
      jsonb_build_object('source','PAY_BATCH_ITEM','document',
        jsonb_build_object('timesheet_id',v_owner_a))),
    jsonb_build_array(
      jsonb_build_object('source','RESERVATION','document','{}'::jsonb),
      jsonb_build_object('source','PAY_BATCH_ITEM','document','{}'::jsonb)),
    'RESERVATION');
  IF v_authority.resolution_failure<>'RESERVATION_OWNER_CONFLICT' THEN
    RAISE EXCEPTION 'V1210_EMPTY_RESERVATION_MASKED_ITEM_OWNER';
  END IF;

  SELECT * INTO STRICT v_authority
  FROM private.pay_workbench_financial_source_authority_v3(
    v_candidate,ARRAY[v_owner_a],ARRAY[v_candidate],ARRAY[]::uuid[],
    ARRAY[v_component_a],jsonb_build_array(jsonb_build_object(
      'source','RESERVATION_COLUMNS','key_type','TS_TOTAL','key_value','TOTAL')),
    jsonb_build_array(
      jsonb_build_object('source','RESERVATION','document',
        jsonb_build_object('candidate_id',v_candidate)),
      jsonb_build_object('source','PAY_BATCH_ITEM','document',
        jsonb_build_object('timesheet_id',v_owner_a))),
    jsonb_build_array(
      jsonb_build_object('source','RESERVATION','document','{}'::jsonb),
      jsonb_build_object('source','PAY_BATCH_ITEM','document',jsonb_build_object(
        'finance_component_id',v_component_a,
        'component_key_type','TS_TOTAL','component_key_value','TOTAL'))),
    'RESERVATION');
  IF v_authority.resolution_failure IS NOT NULL
     OR v_authority.resolved_timesheet_id IS DISTINCT FROM v_owner_a
     OR v_authority.resolved_finance_component_id IS DISTINCT FROM v_component_a
     OR v_authority.resolved_component_key_type<>'TS_TOTAL'
     OR v_authority.resolved_component_key_value<>'TOTAL' THEN
    RAISE EXCEPTION 'V1210_PARTIAL_DOCUMENT_AGREEMENT_FAILED';
  END IF;

  SELECT * INTO STRICT v_authority
  FROM private.pay_workbench_financial_source_authority_v3(
    v_candidate,ARRAY[v_owner_a],ARRAY[v_candidate],ARRAY[]::uuid[],
    ARRAY[v_component_a],jsonb_build_array(jsonb_build_object(
      'source','RESERVATION_COLUMNS','key_type','TS_TOTAL','key_value','TOTAL')),
    jsonb_build_array(
      jsonb_build_object('source','RESERVATION','document',
        jsonb_build_object('candidate_id',v_other_candidate)),
      jsonb_build_object('source','PAY_BATCH_ITEM','document',
        jsonb_build_object('timesheet_id',v_owner_a))),
    jsonb_build_array(
      jsonb_build_object('source','RESERVATION','document','{}'::jsonb),
      jsonb_build_object('source','PAY_BATCH_ITEM','document','{}'::jsonb)),
    'RESERVATION');
  IF v_authority.resolution_failure<>'RESERVATION_CANDIDATE_CONFLICT' THEN
    RAISE EXCEPTION 'V1210_CANDIDATE_CONFLICT_NOT_DETECTED';
  END IF;

  SELECT * INTO STRICT v_authority
  FROM private.pay_workbench_financial_source_authority_v3(
    v_candidate,ARRAY[v_owner_a],ARRAY[v_candidate],ARRAY[]::uuid[],
    ARRAY[v_component_a],jsonb_build_array(jsonb_build_object(
      'source','RESERVATION_COLUMNS','key_type','TS_TOTAL','key_value','TOTAL')),
    jsonb_build_array(
      jsonb_build_object('source','RESERVATION','document',
        jsonb_build_object('timesheet_id',v_owner_a)),
      jsonb_build_object('source','PAY_BATCH_ITEM','document',
        jsonb_build_object('timesheet_id',v_owner_a))),
    jsonb_build_array(
      jsonb_build_object('source','RESERVATION','document',jsonb_build_object(
        'finance_component_id',v_component_b,
        'component_key_type','TS_DAY','component_key_value','2026-08-05')),
      jsonb_build_object('source','PAY_BATCH_ITEM','document',jsonb_build_object(
        'finance_component_id',v_component_a,
        'component_key_type','TS_TOTAL','component_key_value','TOTAL'))),
    'RESERVATION');
  IF v_authority.resolution_failure NOT IN (
       'RESERVATION_COMPONENT_CONFLICT','RESERVATION_COMPONENT_KEY_CONFLICT') THEN
    RAISE EXCEPTION 'V1210_COMPONENT_OR_KEY_CONFLICT_NOT_DETECTED';
  END IF;

  v_expected:=private.pay_workbench_presentation_allocation_expected_v1(
    jsonb_build_object('timesheet_id',v_owner_a,'candidate_id',v_candidate,
      'candidate_pay_method','PAYE','amount_ex_vat',0,
      'ready_section_amount_ex_vat',0,'ready_segment_count',2,
      'is_ready_for_draft',true));
  IF v_expected<>'[]'::jsonb THEN
    RAISE EXCEPTION 'V1210_ZERO_NET_PUBLIC_ROW_EXPECTED';
  END IF;

  v_expected:=private.pay_workbench_presentation_allocation_expected_v1(
    jsonb_build_object('timesheet_id',v_owner_a,'candidate_id',v_candidate,
      'candidate_pay_method','PAYE','amount_ex_vat',100,
      'ready_section_amount_ex_vat',100,'ready_segment_count',1,
      'has_active_timesheet_snooze',true,'snooze_until_date',NULL,
      'is_ready_for_draft',true));
  IF v_expected<>'[]'::jsonb THEN
    RAISE EXCEPTION 'V1210_INDEFINITE_SNOOZE_PUBLIC_ROW_EXPECTED';
  END IF;

  v_expected:=private.pay_workbench_presentation_allocation_expected_v1(
    jsonb_build_object('timesheet_id',v_owner_a,'candidate_id',v_candidate,
      'candidate_pay_method','PAYE','amount_ex_vat',100,
      'ready_section_amount_ex_vat',100,'ready_segment_count',1,
      'has_active_timesheet_snooze',true,'snooze_until_date','2026-08-12',
      'is_ready_for_draft',true));
  IF jsonb_array_length(v_expected)<>1
     OR v_expected#>>'{0,presentation_section}'<>'BLOCKED_FOR_PAY' THEN
    RAISE EXCEPTION 'V1210_DATED_SNOOZE_BLOCKED_ROW_MISSING';
  END IF;
END;
$verification$;

DO $verification$
DECLARE
  v_sync_definition text;
  v_helper_invocation_count integer;
BEGIN
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_sync_overpayments_from_workbench_workspace_v1(uuid,uuid,uuid,uuid,date,date,uuid,text,uuid[],jsonb,uuid,uuid[],uuid[])'::regprocedure)
  INTO STRICT v_sync_definition;

  SELECT count(*)::integer
  INTO v_helper_invocation_count
  FROM regexp_matches(
    v_sync_definition,
    'private\.pay_workbench_sealed_rate_component_projection_v1\s*\(',
    'g'
  );

  IF v_helper_invocation_count <> 1 THEN
    RAISE EXCEPTION 'V1210_RATE_HELPER_INVOCATION_COUNT_MISMATCH';
  END IF;

  IF v_sync_definition ~ '''source_pay_method''\s*,\s*v_scope'
     OR v_sync_definition ~ '''target_pay_method''\s*,\s*v_scope'
     OR v_sync_definition ~* 'preliminary_outstanding_allocation|preliminary_allocations|final_allocations|preview_truth_weight_total' THEN
    RAISE EXCEPTION 'V1210_BOUNDED_RATE_AUTHORITY_REGRESSION';
  END IF;

  IF position('tmp_sync_builder_physical_components' in v_sync_definition)=0
     OR position('builder_comparison_digest' in v_sync_definition)=0
     OR position('PAY_WORKBENCH_CANONICAL_PHYSICAL_COMPONENT_MISMATCH' in v_sync_definition)=0 THEN
    RAISE EXCEPTION 'V1210_BUILDER_PHYSICAL_OWNER_FENCE_MISSING';
  END IF;
END;
$verification$;
