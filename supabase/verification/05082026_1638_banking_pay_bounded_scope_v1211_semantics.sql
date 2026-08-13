-- Banking Pay Version 1.2.11 rollback-safe semantic verification.
-- Creates no durable objects and performs no application DML.

DO $verification$
DECLARE
  v_candidate uuid := '00000000-0000-4000-8000-000000000001'::uuid;
  v_owner uuid := '10000000-0000-4000-8000-000000000001'::uuid;
  v_authority record;
  v_rollup record;
BEGIN
  SELECT * INTO STRICT v_authority
  FROM private.pay_workbench_financial_source_authority_v3(
    v_candidate, ARRAY[v_owner], ARRAY[v_candidate], ARRAY[]::uuid[],
    ARRAY[]::uuid[], '[]'::jsonb, '[]'::jsonb,
    jsonb_build_array(jsonb_build_object(
      'source', 'PAY_BATCH_ITEM',
      'document', jsonb_build_object(
        'component_key_type', 'TS_TOTAL',
        'component_key_value', 'TOTAL',
        'key_type', 'TS_DAY',
        'key_value', '2026-08-05'))),
    'RESERVATION');
  IF v_authority.resolution_failure <> 'RESERVATION_COMPONENT_KEY_CONFLICT' THEN
    RAISE EXCEPTION 'V1211_RESERVATION_ALIAS_CONFLICT_NOT_DETECTED';
  END IF;

  SELECT * INTO STRICT v_authority
  FROM private.pay_workbench_financial_source_authority_v3(
    v_candidate, ARRAY[v_owner], ARRAY[v_candidate], ARRAY[]::uuid[],
    ARRAY[]::uuid[], '[]'::jsonb,
    jsonb_build_array(jsonb_build_object(
      'source', 'FINANCE_ITEM',
      'document', jsonb_build_object(
        'economic_key', jsonb_build_object(
          'component_key_type', 'TS_TOTAL',
          'component_key_value', 'TOTAL'),
        'component', jsonb_build_object(
          'key_type', 'TS_DAY',
          'key_value', '2026-08-05')))),
    '[]'::jsonb, 'FINANCE');
  IF v_authority.resolution_failure <> 'FINANCE_COMPONENT_KEY_CONFLICT' THEN
    RAISE EXCEPTION 'V1211_FINANCE_NESTED_ALIAS_CONFLICT_NOT_DETECTED';
  END IF;

  SELECT * INTO STRICT v_authority
  FROM private.pay_workbench_financial_source_authority_v3(
    v_candidate, ARRAY[v_owner], ARRAY[v_candidate], ARRAY[]::uuid[],
    ARRAY[]::uuid[], jsonb_build_array(jsonb_build_object(
      'component_key_type', 'TS_TOTAL',
      'component_key_value', 'TOTAL',
      'key_type', 'TS_DAY')),
    '[]'::jsonb, '[]'::jsonb, 'FINANCE');
  IF v_authority.resolution_failure <> 'FINANCE_COMPONENT_KEY_INCOMPLETE' THEN
    RAISE EXCEPTION 'V1211_INCOMPLETE_ALIAS_NOT_DETECTED';
  END IF;

  SELECT * INTO STRICT v_authority
  FROM private.pay_workbench_financial_source_authority_v3(
    v_candidate, ARRAY[v_owner], ARRAY[v_candidate], ARRAY[]::uuid[],
    ARRAY[]::uuid[], '[]'::jsonb, '[]'::jsonb,
    jsonb_build_array(jsonb_build_object(
      'source', 'PAY_BATCH_ITEM',
      'document', jsonb_build_object(
        'component_key_type', 'TS_TOTAL',
        'component_key_value', 'TOTAL',
        'key_type', 'ts_total',
        'key_value', 'TOTAL'))),
    'RESERVATION');
  IF v_authority.resolution_failure IS NOT NULL
     OR v_authority.component_key_pairs <> ARRAY['TS_TOTAL' || E'\x1f' || 'TOTAL']::text[] THEN
    RAISE EXCEPTION 'V1211_AGREEING_ALIASES_DID_NOT_COLLAPSE_BY_VALUE';
  END IF;

  WITH private_state(candidate_id, timesheet_id) AS (
    VALUES
      (v_candidate, '20000000-0000-4000-8000-000000000001'::uuid),
      (v_candidate, '20000000-0000-4000-8000-000000000002'::uuid),
      (v_candidate, '20000000-0000-4000-8000-000000000003'::uuid),
      (v_candidate, '20000000-0000-4000-8000-000000000004'::uuid),
      (v_candidate, '20000000-0000-4000-8000-000000000005'::uuid)
  ), timesheet_canonical_preview_lines(candidate_id, line_json, amount_ex_vat) AS (
    VALUES
      (v_candidate, jsonb_build_object(
        'line_type','TIMESHEET_PAYMENT','real_business_timesheet_id','20000000-0000-4000-8000-000000000001',
        'presentation_section','READY_TO_PAY','draftable',true), 100::numeric),
      (v_candidate, jsonb_build_object(
        'line_type','TIMESHEET_PAYMENT','real_business_timesheet_id','20000000-0000-4000-8000-000000000004',
        'presentation_section','BLOCKED_FOR_PAY','draftable',false), 100::numeric),
      (v_candidate, jsonb_build_object(
        'line_type','TIMESHEET_PAYMENT','real_business_timesheet_id','20000000-0000-4000-8000-000000000005',
        'presentation_section','READY_TO_PAY','draftable',true), 80::numeric),
      (v_candidate, jsonb_build_object(
        'line_type','TIMESHEET_PAYMENT','real_business_timesheet_id','20000000-0000-4000-8000-000000000005',
        'presentation_section','BLOCKED_FOR_PAY','draftable',false), 20::numeric)
  ), emitted_public_timesheets AS (
    SELECT
      tcpl.candidate_id,
      (tcpl.line_json->>'real_business_timesheet_id')::uuid AS timesheet_id,
      bool_or(
        upper(coalesce(tcpl.line_json->>'presentation_section','')) = 'READY_TO_PAY'
        AND coalesce(nullif(tcpl.line_json->>'draftable','')::boolean, false) = true
      ) AS has_ready_public_line,
      bool_or(
        upper(coalesce(tcpl.line_json->>'presentation_section','')) = 'BLOCKED_FOR_PAY'
      ) AS has_blocked_public_line,
      round(coalesce(sum(tcpl.amount_ex_vat) FILTER (
        WHERE upper(coalesce(tcpl.line_json->>'presentation_section','')) = 'READY_TO_PAY'
          AND coalesce(nullif(tcpl.line_json->>'draftable','')::boolean, false) = true
      ), 0), 2) AS ready_public_amount_ex_vat
    FROM timesheet_canonical_preview_lines tcpl
    WHERE upper(coalesce(tcpl.line_json->>'line_type','')) = 'TIMESHEET_PAYMENT'
      AND upper(coalesce(tcpl.line_json->>'presentation_section','')) IN ('READY_TO_PAY','BLOCKED_FOR_PAY')
    GROUP BY tcpl.candidate_id, (tcpl.line_json->>'real_business_timesheet_id')::uuid
  ), rollup AS (
    SELECT
      ps.candidate_id,
      count(*)::integer AS private_state_count,
      count(*) FILTER (WHERE coalesce(ept.has_ready_public_line,false))::integer AS ready_count,
      count(*) FILTER (WHERE coalesce(ept.has_blocked_public_line,false))::integer AS blocked_count,
      round(coalesce(sum(ept.ready_public_amount_ex_vat),0),2) AS ready_total
    FROM private_state ps
    LEFT JOIN emitted_public_timesheets ept
      ON ept.candidate_id = ps.candidate_id
     AND ept.timesheet_id = ps.timesheet_id
    GROUP BY ps.candidate_id
  )
  SELECT * INTO STRICT v_rollup FROM rollup;

  IF v_rollup.private_state_count <> 5
     OR v_rollup.ready_count <> 2
     OR v_rollup.blocked_count <> 2
     OR v_rollup.ready_total <> 180::numeric THEN
    RAISE EXCEPTION 'V1211_PUBLIC_TIMESHEET_ROLLUP_MISMATCH';
  END IF;
END;
$verification$;

DO $verification$
DECLARE
  v_sync_definition text;
BEGIN
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_sync_overpayments_from_workbench_workspace_v1(uuid,uuid,uuid,uuid,date,date,uuid,text,uuid[],jsonb,uuid,uuid[],uuid[])'::regprocedure)
  INTO STRICT v_sync_definition;

  IF position('PAY_SYNC_OVERPAYMENTS_RATE_ECONOMIC_FENCE_MISMATCH' in v_sync_definition)=0
     OR position('PAY_SYNC_OVERPAYMENTS_RATE_BUILDER_COMPONENT_MISSING' in v_sync_definition)=0
     OR position('PAY_SYNC_OVERPAYMENTS_RATE_BUILDER_COMPONENT_EXTRA' in v_sync_definition)=0
     OR position('PAY_SYNC_OVERPAYMENTS_RATE_BUILDER_DIGEST_MISMATCH' in v_sync_definition)=0
     OR position('PAY_SYNC_OVERPAYMENTS_RATE_PHYSICAL_FENCE_MISMATCH' in v_sync_definition)=0 THEN
    RAISE EXCEPTION 'V1211_ECONOMIC_OR_PHYSICAL_RATE_FENCE_MISSING';
  END IF;

  IF position('physical_bucket_key' in v_sync_definition)=0
     OR position('physical_bucket_digest' in v_sync_definition)=0
     OR position('sealed_evidence_digest' in v_sync_definition)=0 THEN
    RAISE EXCEPTION 'V1211_PHYSICAL_RATE_EVIDENCE_MISSING';
  END IF;
END;
$verification$;
