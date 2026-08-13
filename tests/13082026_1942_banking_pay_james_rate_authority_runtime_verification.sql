-- Runtime contract checks for James physical rate authority.
-- Run only against TEST after the three repeatables are installed.
\set ON_ERROR_STOP on
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
     OR position('financial_digest_version' in v_serializer_definition)=0
     OR position('input.source_pay_method IS NULL' in v_serializer_definition)=0
     OR position('input.target_pay_method IS NULL' in v_serializer_definition)=0 THEN
    RAISE EXCEPTION 'JAMES_RATE_SERIALIZER_CONTRACT_MISSING';
  END IF;

  IF v_sync_definition ~* 'preliminary_outstanding_allocation|preliminary_allocations|final_allocations|preview_truth_weight_total'
     OR v_sync_definition ~ '''source_pay_method''\s*,\s*v_scope'
     OR position('PAY_SYNC_OVERPAYMENTS_RATE_PHYSICAL_FENCE_MISMATCH' in v_sync_definition)=0
     OR position('raw.component_member_identity' in v_sync_definition)=0
     OR position('RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING' in v_sync_definition)=0
     OR position('RATE_AUTHORITY_TARGET_PAY_METHOD_MISSING' in v_sync_definition)=0 THEN
    RAISE EXCEPTION 'JAMES_RATE_SYNCHRONIZER_PHYSICAL_OWNER_FAILED';
  END IF;

  IF position('sealed_physical_amount_matches' in v_helper_definition)=0
     OR position('STRUCTURAL_IDENTITY' in v_helper_definition)=0
     OR position('SOLE_BUCKET' in v_helper_definition)=0
     OR position('RATE_AUTHORITY_PHYSICAL_BASELINE_REQUIRED' in v_helper_definition)=0
     OR position('RATE_AUTHORITY_PHYSICAL_RESERVATION_REQUIRED' in v_helper_definition)=0 THEN
    RAISE EXCEPTION 'JAMES_RATE_HELPER_SEALED_AMOUNT_OWNER_FAILED';
  END IF;
END;
$verification$;

-- These fixtures write only private build/scope/fact rows under the fixed IDs
-- below and always roll them back. They do not create a Draft, payment,
-- settlement, remittance, provider action or durable Workbench publication.
BEGIN;

DO $sealed_amount_fixture$
DECLARE
  v_template_build constant uuid := 'b6e2bc38-2bff-4a5f-8b32-675b9a5728af';
  v_build constant uuid := '13082026-1942-4000-8000-000000000001';
  v_timesheet constant uuid := '13082026-1942-4000-8000-000000000002';
  v_candidate constant uuid := '6e8493ae-c207-497e-8d83-0b518753f590';
  v_reservation constant uuid := '13082026-1942-4000-8000-000000000003';
  v_ambiguous_reservation constant uuid := '13082026-1942-4000-8000-000000000004';
  v_family text := 'timesheet:'||v_timesheet::text;
  v_day_key text := 'RATE_BUCKET_V1|'||v_timesheet::text||'|'||
    'timesheet:'||v_timesheet::text||'|TS_DAY|2026-08-13|segment:test|DAY';
  v_night_key text := 'RATE_BUCKET_V1|'||v_timesheet::text||'|'||
    'timesheet:'||v_timesheet::text||'|TS_DAY|2026-08-13|segment:test|NIGHT';
  v_day_canonical jsonb;
  v_night_canonical jsonb;
  v_day_bucket jsonb;
  v_night_bucket jsonb;
  v_physical_digest text;
  v_rate_authority jsonb;
  v_failure text;
  v_count integer;
  v_baseline numeric;
  v_reserved numeric;
BEGIN
  IF EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_builds
      WHERE id=v_build) THEN
    RAISE EXCEPTION 'JAMES_RATE_FIXTURE_ID_ALREADY_EXISTS';
  END IF;

  INSERT INTO private.banking_pay_workbench_economic_builds(
    id,build_token,candidate_id,session_id,session_version,source_build_run_id,
    captured_candidate_generation,source_change_seq,status,private_stage,obsolete_at_utc)
  SELECT v_build,gen_random_uuid(),v_candidate,template.session_id,template.session_version,
    gen_random_uuid(),template.captured_candidate_generation,template.source_change_seq,
    'OBSOLETE','WORKSPACE_FACT',clock_timestamp()
  FROM private.banking_pay_workbench_economic_builds template
  WHERE template.id=v_template_build;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'JAMES_RATE_FIXTURE_TEMPLATE_BUILD_MISSING';
  END IF;

  INSERT INTO private.banking_pay_workbench_economic_build_scope(
    build_id,timesheet_id,candidate_id,seed_reasons,dependency_reasons,
    captured_dirty_generation,required_fact_families)
  VALUES(v_build,v_timesheet,v_candidate,ARRAY['JAMES_RATE_FIXTURE'],ARRAY[]::text[],0,
    ARRAY['LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','RESERVATION_COMPONENT']);

  v_day_canonical:=jsonb_build_object(
    'physical_bucket_version',1,'physical_bucket_key',v_day_key,
    'component_kind','WORKED_TIME','component_member_identity','segment:test',
    'bucket_code','DAY','source_units',round(2::numeric,6),
    'source_rate',round(20::numeric,6),'source_charge_rate',round(40::numeric,6),
    'source_pay_ex_vat',round(40::numeric,2),'source_charge_ex_vat',round(80::numeric,2),
    'baseline_source_pay_ex_vat',NULL::numeric,
    'reserved_source_pay_ex_vat',NULL::numeric,
    'outstanding_source_pay_ex_vat',NULL::numeric,
    'source_pay_method','PAYE','target_pay_method','UMBRELLA');
  v_night_canonical:=jsonb_build_object(
    'physical_bucket_version',1,'physical_bucket_key',v_night_key,
    'component_kind','WORKED_TIME','component_member_identity','segment:test',
    'bucket_code','NIGHT','source_units',round(2::numeric,6),
    'source_rate',round(30::numeric,6),'source_charge_rate',round(60::numeric,6),
    'source_pay_ex_vat',round(60::numeric,2),'source_charge_ex_vat',round(120::numeric,2),
    'baseline_source_pay_ex_vat',NULL::numeric,
    'reserved_source_pay_ex_vat',NULL::numeric,
    'outstanding_source_pay_ex_vat',NULL::numeric,
    'source_pay_method','PAYE','target_pay_method','UMBRELLA');
  v_day_bucket:=v_day_canonical||jsonb_build_object(
    'bucket_sort_ordinal',1,'segment_stable_key','segment:test',
    'physical_bucket_digest',md5(v_day_canonical::text),'is_actionable_candidate',true);
  v_night_bucket:=v_night_canonical||jsonb_build_object(
    'bucket_sort_ordinal',2,'segment_stable_key','segment:test',
    'physical_bucket_digest',md5(v_night_canonical::text),'is_actionable_candidate',true);
  v_physical_digest:=md5(jsonb_build_array(v_day_canonical,v_night_canonical)::text);
  v_rate_authority:=jsonb_build_object(
    'rate_authority_version',1,
    'build',jsonb_build_object('candidate_id',v_candidate::text),
    'source',jsonb_build_object('source_pay_method','PAYE',
      'financial_revision_digest','fixture-financial-revision'),
    'target',jsonb_build_object('target_pay_method','UMBRELLA',
      'target_authority_digest','fixture-target-authority'),
    'conversion',jsonb_build_object('conversion_context_digest','fixture-conversion'),
    'economic',jsonb_build_object('source_family_key',v_family,
      'component_kind','WORKED_TIME','physical_bucket_digest',v_physical_digest,
      'parent_source_charge_ex_vat',round(200::numeric,2)),
    'physical_buckets',jsonb_build_array(v_day_bucket,v_night_bucket),
    'sealed_evidence_digest',md5(jsonb_build_object(
      'sealed_evidence_version',1,
      'financial_revision_digest','fixture-financial-revision',
      'target_authority_digest','fixture-target-authority',
      'conversion_context_digest','fixture-conversion',
      'physical_bucket_digest',v_physical_digest,
      'economic_key_type','TS_DAY','economic_key_value','2026-08-13',
      'parent_source_pay_ex_vat',round(100::numeric,2),
      'parent_source_charge_ex_vat',round(200::numeric,2),
      'source_pay_method','PAYE','target_pay_method','UMBRELLA')::text));

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    truth_ex_vat,source_payload_json,financial_digest,source_ordinal)
  VALUES(v_build,'LIVE_ENTITLEMENT_INPUT','fixture-live',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',
    100,jsonb_build_object('source_key','fixture-live','rate_authority',v_rate_authority),
    md5('fixture-live'),1);

  SELECT count(*),min(failure_code)
  INTO v_count,v_failure
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]);
  IF v_count<>2 OR v_failure IS NOT NULL THEN
    RAISE EXCEPTION 'JAMES_RATE_ZERO_BASELINE_RESERVATION_FIXTURE_FAILED';
  END IF;

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    amount_ex_vat,source_payload_json,financial_digest)
  VALUES(v_build,'FROZEN_SETTLED_COMPONENT','fixture-baseline-exact',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',
    15,jsonb_build_object('frozen_source_basis_json',jsonb_build_object(
      'source_family_key',v_family,'segment_stable_key','segment:test','band','DAY')),
    md5('fixture-baseline-exact'));

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    reserved_source_amount,reservation_id,source_payload_json,financial_digest)
  VALUES(v_build,'RESERVATION_COMPONENT','fixture-reservation-exact',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'GLOBAL','JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',10,v_reservation,
    jsonb_build_object('frozen_source_basis_json',jsonb_build_object(
      'physical_bucket_key',v_night_key)),md5('fixture-reservation-exact'));

  SELECT count(*),min(failure_code),sum(baseline_ex_vat),sum(reserved_ex_vat)
  INTO v_count,v_failure,v_baseline,v_reserved
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]);
  IF v_count<>2 OR v_failure IS NOT NULL
     OR round(v_baseline,2) IS DISTINCT FROM 15::numeric
     OR round(v_reserved,2) IS DISTINCT FROM 10::numeric THEN
    RAISE EXCEPTION 'JAMES_RATE_EXACT_SEALED_AMOUNT_ATTRIBUTION_FAILED';
  END IF;

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    amount_ex_vat,source_payload_json,financial_digest)
  VALUES(v_build,'FROZEN_SETTLED_COMPONENT','fixture-baseline-ambiguous',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',
    5,jsonb_build_object('frozen_source_basis_json',jsonb_build_object(
      'source_family_key',v_family)),md5('fixture-baseline-ambiguous'));

  SELECT min(failure_code)
  INTO v_failure
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]);
  IF v_failure IS DISTINCT FROM 'RATE_AUTHORITY_PHYSICAL_BASELINE_REQUIRED' THEN
    RAISE EXCEPTION 'JAMES_RATE_AMBIGUOUS_BASELINE_DID_NOT_FAIL_CLOSED';
  END IF;

  DELETE FROM private.banking_pay_workbench_economic_build_facts
  WHERE build_id=v_build AND fact_family='FROZEN_SETTLED_COMPONENT'
    AND natural_key='fixture-baseline-ambiguous';

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    reserved_source_amount,reservation_id,source_payload_json,financial_digest)
  VALUES(v_build,'RESERVATION_COMPONENT','fixture-reservation-ambiguous',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'GLOBAL','JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',5,
    v_ambiguous_reservation,'{}'::jsonb,md5('fixture-reservation-ambiguous'));

  SELECT min(failure_code)
  INTO v_failure
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]);
  IF v_failure IS DISTINCT FROM 'RATE_AUTHORITY_PHYSICAL_RESERVATION_REQUIRED' THEN
    RAISE EXCEPTION 'JAMES_RATE_AMBIGUOUS_RESERVATION_DID_NOT_FAIL_CLOSED';
  END IF;
END;
$sealed_amount_fixture$;

CREATE TEMP TABLE tmp_james_rate_projection_raw(
  materialization_ordinal bigint NOT NULL,
  projection_status text NOT NULL,
  timesheet_id uuid,
  economic_key_type text,
  economic_key_value text,
  physical_bucket_key text,
  component_kind text,
  component_member_identity text,
  bucket_code text,
  source_rate numeric,
  source_charge_rate numeric,
  source_pay_method text,
  target_pay_method text
) ON COMMIT DROP;

CREATE TEMP TABLE tmp_james_rate_projection_failures(
  failure_rank integer NOT NULL,
  failure_code text NOT NULL,
  timesheet_id uuid,
  economic_key_type text,
  economic_key_value text,
  physical_bucket_key text,
  detail_json jsonb NOT NULL
) ON COMMIT DROP;

INSERT INTO tmp_james_rate_projection_raw VALUES
  (1,'READY','13082026-1942-4000-8000-000000000010','ADDITIONAL_CODE',
    'OVERTIME_150','additional-key-overtime','ADDITIONAL_UNIT','additional:OVERTIME_150',
    'ADDITIONAL',30,50,'PAYE','UMBRELLA'),
  (2,'READY','13082026-1942-4000-8000-000000000010','ADDITIONAL_CODE',
    'CLIENT_PREMIUM_X7','additional-key-premium','ADDITIONAL_UNIT','additional:CLIENT_PREMIUM_X7',
    'ADDITIONAL',45,70,'PAYE','UMBRELLA'),
  (3,'READY','13082026-1942-4000-8000-000000000011','ADDITIONAL_CODE',
    'CALL_OUT_SPECIAL','additional-key-conflict-a','ADDITIONAL_UNIT','additional:CALL_OUT_SPECIAL',
    'ADDITIONAL',25,40,'PAYE','UMBRELLA'),
  (4,'READY','13082026-1942-4000-8000-000000000011','ADDITIONAL_CODE',
    'CALL_OUT_SPECIAL','additional-key-conflict-b','ADDITIONAL_UNIT','additional:CALL_OUT_SPECIAL',
    'ADDITIONAL',35,55,'PAYE','UMBRELLA'),
  (5,'READY','13082026-1942-4000-8000-000000000012','ADDITIONAL_CODE',
    'NULL_SOURCE_METHOD','additional-key-null-source','ADDITIONAL_UNIT','additional:NULL_SOURCE_METHOD',
    'ADDITIONAL',10,20,NULL,'UMBRELLA'),
  (6,'READY','13082026-1942-4000-8000-000000000013','ADDITIONAL_CODE',
    'NULL_TARGET_METHOD','additional-key-null-target','ADDITIONAL_UNIT','additional:NULL_TARGET_METHOD',
    'ADDITIONAL',10,20,'PAYE',NULL),
  (7,'READY','13082026-1942-4000-8000-000000000014','ADDITIONAL_CODE',
    'DUPLICATE_IDENTITY','additional-key-duplicate','ADDITIONAL_UNIT','additional:DUPLICATE_IDENTITY',
    'ADDITIONAL',10,20,'PAYE','UMBRELLA'),
  (8,'READY','13082026-1942-4000-8000-000000000014','ADDITIONAL_CODE',
    'DUPLICATE_IDENTITY','additional-key-duplicate','ADDITIONAL_UNIT','additional:DUPLICATE_IDENTITY',
    'ADDITIONAL',10,20,'PAYE','UMBRELLA');

INSERT INTO tmp_james_rate_projection_failures
SELECT 21,'RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING',raw.timesheet_id,
  raw.economic_key_type,raw.economic_key_value,raw.physical_bucket_key,
  jsonb_build_object('materialization_ordinal',raw.materialization_ordinal,
    'source_pay_method',raw.source_pay_method)
FROM tmp_james_rate_projection_raw raw
WHERE upper(coalesce(raw.projection_status,''))<>'FAILED'
  AND (nullif(btrim(raw.source_pay_method),'') IS NULL
    OR upper(btrim(raw.source_pay_method)) NOT IN ('PAYE','UMBRELLA'));

INSERT INTO tmp_james_rate_projection_failures
SELECT 22,'RATE_AUTHORITY_TARGET_PAY_METHOD_MISSING',raw.timesheet_id,
  raw.economic_key_type,raw.economic_key_value,raw.physical_bucket_key,
  jsonb_build_object('materialization_ordinal',raw.materialization_ordinal,
    'target_pay_method',raw.target_pay_method)
FROM tmp_james_rate_projection_raw raw
WHERE upper(coalesce(raw.projection_status,''))<>'FAILED'
  AND (nullif(btrim(raw.target_pay_method),'') IS NULL
    OR upper(btrim(raw.target_pay_method)) NOT IN ('PAYE','UMBRELLA'));

INSERT INTO tmp_james_rate_projection_failures
SELECT 30,'RATE_AUTHORITY_DUPLICATE_PHYSICAL_BUCKET',duplicate.timesheet_id,
  duplicate.economic_key_type,duplicate.economic_key_value,duplicate.physical_bucket_key,
  jsonb_build_object('row_count',duplicate.row_count)
FROM (
  SELECT raw.timesheet_id,raw.economic_key_type,raw.economic_key_value,
    raw.physical_bucket_key,count(*)::integer AS row_count
  FROM tmp_james_rate_projection_raw raw
  WHERE upper(coalesce(raw.projection_status,''))<>'FAILED'
  GROUP BY raw.timesheet_id,raw.economic_key_type,raw.economic_key_value,
    raw.physical_bucket_key
  HAVING count(*)>1
) duplicate;

INSERT INTO tmp_james_rate_projection_failures
SELECT 45,'RATE_AUTHORITY_MULTIPLE_RATES_UNSUPPORTED',raw.timesheet_id,
  raw.economic_key_type,raw.economic_key_value,min(raw.physical_bucket_key),
  jsonb_build_object('component_kind',raw.component_kind,
    'component_member_identity',raw.component_member_identity,
    'bucket_code',raw.bucket_code,
    'source_rate_count',count(DISTINCT round(raw.source_rate,6)),
    'source_charge_rate_count',count(DISTINCT round(raw.source_charge_rate,6)))
FROM tmp_james_rate_projection_raw raw
WHERE upper(coalesce(raw.projection_status,'')) IN ('READY','FIXED')
  AND raw.component_kind IN ('WORKED_TIME','ADDITIONAL_UNIT')
GROUP BY raw.timesheet_id,raw.component_kind,raw.component_member_identity,
  raw.economic_key_type,raw.economic_key_value,raw.bucket_code
HAVING count(DISTINCT round(raw.source_rate,6))>1
   OR count(DISTINCT round(raw.source_charge_rate,6))>1;

DO $preconstraint_fixture$
BEGIN
  IF EXISTS(SELECT 1 FROM tmp_james_rate_projection_failures
      WHERE timesheet_id='13082026-1942-4000-8000-000000000010'::uuid
        AND failure_code='RATE_AUTHORITY_MULTIPLE_RATES_UNSUPPORTED') THEN
    RAISE EXCEPTION 'JAMES_RATE_DISTINCT_ADDITIONAL_CODES_WERE_GROUPED_TOGETHER';
  END IF;
  IF (SELECT count(*) FROM tmp_james_rate_projection_failures
      WHERE timesheet_id='13082026-1942-4000-8000-000000000011'::uuid
        AND failure_code='RATE_AUTHORITY_MULTIPLE_RATES_UNSUPPORTED')<>1 THEN
    RAISE EXCEPTION 'JAMES_RATE_IDENTICAL_ADDITIONAL_CONFLICT_NOT_REJECTED';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM tmp_james_rate_projection_failures
      WHERE timesheet_id='13082026-1942-4000-8000-000000000012'::uuid
        AND failure_code='RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING') THEN
    RAISE EXCEPTION 'JAMES_RATE_NULL_SOURCE_METHOD_NOT_TYPED';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM tmp_james_rate_projection_failures
      WHERE timesheet_id='13082026-1942-4000-8000-000000000013'::uuid
        AND failure_code='RATE_AUTHORITY_TARGET_PAY_METHOD_MISSING') THEN
    RAISE EXCEPTION 'JAMES_RATE_NULL_TARGET_METHOD_NOT_TYPED';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM tmp_james_rate_projection_failures
      WHERE timesheet_id='13082026-1942-4000-8000-000000000014'::uuid
        AND failure_code='RATE_AUTHORITY_DUPLICATE_PHYSICAL_BUCKET') THEN
    RAISE EXCEPTION 'JAMES_RATE_DUPLICATE_PHYSICAL_IDENTITY_NOT_TYPED';
  END IF;
END;
$preconstraint_fixture$;

ROLLBACK;
