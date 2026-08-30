-- Deterministic runtime contract checks for James physical rate authority.
-- Run against TEST after the four James runtime repeatables are installed.
-- The live James build diagnostic remains available separately at
-- supabase/verification/13082026_1943_banking_pay_james_rate_authority_readonly.sql.
-- It is intentionally not part of this migration gate because mutable TEST
-- build lifecycle state must not block an unrelated database deployment.
\set ON_ERROR_STOP on

DO $verification$
DECLARE
  v_helper_definition text;
  v_serializer_definition text;
  v_sync_definition text;
  v_source_build_definition text;
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
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_workbench_candidate_source_build_chunk_legacy_v1(uuid,uuid,jsonb,jsonb,integer)'::regprocedure)
  INTO STRICT v_source_build_definition;

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
     OR position('RATE_AUTHORITY_SOURCE_PAY_METHOD_CONFLICT' in v_sync_definition)=0
     OR position('complete_component_method_digest' in v_sync_definition)=0
     OR v_sync_definition ~* 'coalesce\s*\(\s*component\.source_pay_method\s*,\s*(candidate_pay_method|current_target_pay_method)'
     OR v_sync_definition ~* 'min\s*\(\s*sealed\.source_pay_method\s*\)'
     OR position('RATE_AUTHORITY_TARGET_PAY_METHOD_MISSING' in v_sync_definition)=0 THEN
    RAISE EXCEPTION 'JAMES_RATE_SYNCHRONIZER_PHYSICAL_OWNER_FAILED';
  END IF;

  IF position('nested_evidence_raw' in v_helper_definition)=0
     OR position('nested_evidence_normalized' in v_helper_definition)=0
     OR position('exact_allocation_matched' in v_helper_definition)=0
     OR position('sealed_physical_amount_attribution' in v_helper_definition)=0
     OR position('truth_residual_sources' in v_helper_definition)=0
     OR position('source_method_authority_summary' in v_helper_definition)=0
     OR position('source_method_authority_tier' in v_helper_definition)=0
     OR position('selected_authority_priority' in v_helper_definition)=0
     OR position('RATE_AUTHORITY_SOURCE_PAY_METHOD_CONFLICT' in v_helper_definition)=0
     OR position('complete_evidence_digest' in v_helper_definition)=0
     OR position('sealed_finance_case_authority' in v_helper_definition)=0
     OR position('TOP_LEVEL_SOURCE_BASIS' in v_helper_definition)=0
     OR position('CASE_BUCKET_RESOLUTION' in v_helper_definition)=0
     OR v_helper_definition ~* 'min\s*\(\s*(source\.)?source_pay_method\s*\)'
     OR position('RATE_AUTHORITY_NESTED_AMOUNT_OVERCONSUMED' in v_helper_definition)=0
     OR position('RATE_AUTHORITY_PARENT_COMPONENT_RECONCILIATION_MISMATCH' in v_helper_definition)=0 THEN
    RAISE EXCEPTION 'JAMES_RATE_HELPER_SEALED_AMOUNT_OWNER_FAILED';
  END IF;

  IF position('ACTIVE_ITEM_RESERVATION:' in v_source_build_definition)=0
     OR position('pay_batch_items_active_reservation' in v_source_build_definition)=0
     OR position('RESERVATION_ECONOMIC_KEY_MISSING' in v_source_build_definition)=0
     OR position('RESERVATION_ECONOMIC_KEY_CONFLICT' in v_source_build_definition)=0
     OR position('RESERVATION_COMPONENT_SOURCE_KEY_C_V1' in v_source_build_definition)=0
     OR position('source_key COLLATE "C"' in v_source_build_definition)=0
     OR v_source_build_definition !~* 'coalesce\s*\(\s*item\.reservation_id\s*,\s*item\.pay_batch_item_id\s*\)'
     OR position('tmp_sync_sealed_reservation_items' in v_sync_definition)=0 THEN
    RAISE EXCEPTION 'JAMES_RATE_SEALED_RESERVATION_DOMAIN_MISSING';
  END IF;
END;
$verification$;

-- These fixtures write only private build/scope/fact rows under the fixed IDs
-- below and always roll them back. They do not create a Draft, payment,
-- settlement, remittance, provider action or durable Workbench publication.
BEGIN;

DO $sealed_amount_fixture$
DECLARE
  v_snapshot uuid := gen_random_uuid();
  v_actor uuid := gen_random_uuid();
  v_session uuid := gen_random_uuid();
  v_build constant uuid := '13082026-1942-4000-8000-000000000001';
  v_timesheet constant uuid := '13082026-1942-4000-8000-000000000002';
  v_candidate uuid := gen_random_uuid();
  v_reservation constant uuid := '13082026-1942-4000-8000-000000000003';
  v_ambiguous_reservation constant uuid := '13082026-1942-4000-8000-000000000004';
  v_recovery_reservation constant uuid := '13082026-1942-4000-8000-000000000005';
  v_recovery_case constant uuid := '13082026-1942-4000-8000-000000000006';
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
  v_residual_count integer;
  v_fixture_now timestamptz:=clock_timestamp();
BEGIN
  IF EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_builds
      WHERE id=v_build) THEN
    RAISE EXCEPTION 'JAMES_RATE_FIXTURE_ID_ALREADY_EXISTS';
  END IF;

  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active
  ) VALUES (
    v_snapshot,DATE '2099-08-14',DATE '2099-08-09',DATE '2099-08-03',
    DATE '2099-01-01',DATE '2099-08-09','OPEN',false
  );

  INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
  VALUES (
    v_actor,
    'james-rate-rollback-'||replace(v_actor::text,'-','')||'@example.invalid',
    'UNUSABLE_ROLLBACK_VERIFIER','admin',true
  );

  INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
  VALUES (
    v_candidate,
    'James rate rollback verifier',
    'JAMES-RATE-'||replace(v_candidate::text,'-',''),
    'PAYE'
  );

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,status,version
  ) VALUES (
    v_session,v_actor,DATE '2099-08-14',DATE '2099-08-09',
    'JAMES_RATE_ROLLBACK:'||v_session::text,v_snapshot,'OPEN',1
  );

  INSERT INTO private.banking_pay_workbench_economic_builds(
    id,candidate_id,session_id,session_version,source_snapshot_run_id,
    source_build_run_id,source_job_id,captured_candidate_generation,
    source_change_seq,status,private_stage,
    seed_scope_count,seed_scope_digest,seed_scope_sealed_at_utc,
    scope_count,dependency_node_count,dependency_edge_count,tagged_edge_count,
    row_seal_count,last_stable_ordinal,scope_cursor_json,closure_cursor_json,
    dependency_edge_stream_complete,dependency_edge_stream_digest,
    edge_tag_stream_complete,edge_tag_digest,unit_digest,scope_digest,
    dependency_digest,sealed_fingerprint_digest,dependency_closure_sealed_at_utc,
    obsolete_at_utc,
    created_at_utc,updated_at_utc
  ) VALUES (
    v_build,v_candidate,v_session,1,v_snapshot,
    gen_random_uuid(),NULL,0,0,'OBSOLETE','WORKSPACE_FACT',
    1,md5('JAMES_RATE_SEED'),v_fixture_now,
    1,1,0,0,1,1,'{"terminal":true}'::jsonb,
    '{"terminal":true,"seal_phase":"COMPLETE"}'::jsonb,
    true,md5(''),true,md5(''),md5('JAMES_RATE_UNIT'),
    md5('JAMES_RATE_SCOPE'),md5('JAMES_RATE_DEPENDENCY'),
    md5('JAMES_RATE_FINGERPRINT'),v_fixture_now,v_fixture_now,
    v_fixture_now,v_fixture_now
  );

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
    truth_ex_vat,baseline_ex_vat,truth_inc_vat,baseline_inc_vat,financial_digest,source_ordinal)
  VALUES(v_build,'ENTITLEMENT_COMPONENT','fixture-entitlement',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',
    100,0,100,0,md5('fixture-entitlement'),1);

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

  UPDATE private.banking_pay_workbench_economic_build_facts
  SET baseline_ex_vat=15,baseline_inc_vat=15
  WHERE build_id=v_build AND fact_family='ENTITLEMENT_COMPONENT'
    AND natural_key='fixture-entitlement';

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    amount_ex_vat,source_payload_json,financial_digest)
  VALUES(v_build,'FROZEN_SETTLED_COMPONENT','fixture-baseline-exact',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',
    15,jsonb_build_object('frozen_source_basis_json',jsonb_build_object(
      'source_family_key',v_family,'segment_stable_key','segment:test','band','DAY',
      'physical_bucket_key',v_day_key,'source_pay_ex_vat',15,'source_charge_ex_vat',30)),
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
  IF v_count<>3 OR v_failure IS NOT NULL
     OR round(v_baseline,2) IS DISTINCT FROM 15::numeric
     OR round(v_reserved,2) IS DISTINCT FROM 10::numeric THEN
    RAISE EXCEPTION USING
      MESSAGE='JAMES_RATE_EXACT_SEALED_AMOUNT_ATTRIBUTION_FAILED',
      DETAIL=jsonb_build_object(
        'row_count',v_count,'failure_code',v_failure,
        'baseline_ex_vat',v_baseline,'reserved_ex_vat',v_reserved
      )::text;
  END IF;

  -- A bucket-resolution document is a more specific view of the same sealed
  -- physical component. It must replace, rather than add to, the top-level
  -- source basis for that exact physical identity.
  UPDATE private.banking_pay_workbench_economic_build_facts
  SET baseline_ex_vat=20,baseline_inc_vat=20
  WHERE build_id=v_build AND fact_family='ENTITLEMENT_COMPONENT'
    AND natural_key='fixture-entitlement';

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    amount_ex_vat,source_payload_json,financial_digest)
  VALUES(v_build,'FROZEN_SETTLED_COMPONENT','fixture-specific-bucket-precedence',
    v_candidate,v_timesheet,ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,
    'JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',5,
    jsonb_build_object(
      'frozen_source_basis_json',jsonb_build_object(
        'source_family_key',v_family,'segment_stable_key','segment:test','band','DAY',
        'physical_bucket_key',v_day_key,'source_pay_ex_vat',5,'source_charge_ex_vat',10),
      'pay_batch_item',jsonb_build_object(
        'frozen_resolution_payload_json',jsonb_build_object(
          'case_components',jsonb_build_array(jsonb_build_object(
            'source_basis_fingerprint','fixture-specific-bucket-precedence',
            'saved_resolution_payload_json',jsonb_build_object(
              'bucket_resolutions',jsonb_build_array(jsonb_build_object(
                'component_amount_ex_vat',5,
                'source_basis_json',jsonb_build_object(
                  'source_family_key',v_family,'segment_stable_key','segment:test',
                  'band','DAY','physical_bucket_key',v_day_key,
                  'source_pay_ex_vat',5,'source_charge_ex_vat',10))))))))),
    md5('fixture-specific-bucket-precedence'));

  SELECT min(failure_code),sum(baseline_ex_vat)
  INTO v_failure,v_baseline
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]);
  IF v_failure IS NOT NULL OR round(v_baseline,2) IS DISTINCT FROM 20::numeric THEN
    RAISE EXCEPTION 'JAMES_RATE_SPECIFIC_BUCKET_PRECEDENCE_FAILED';
  END IF;

  DELETE FROM private.banking_pay_workbench_economic_build_facts
  WHERE build_id=v_build AND fact_family='FROZEN_SETTLED_COMPONENT'
    AND natural_key='fixture-specific-bucket-precedence';
  UPDATE private.banking_pay_workbench_economic_build_facts
  SET baseline_ex_vat=15,baseline_inc_vat=15
  WHERE build_id=v_build AND fact_family='ENTITLEMENT_COMPONENT'
    AND natural_key='fixture-entitlement';

  -- Direct advance reservations do not repeat pay-batch item classification.
  -- Reuse the exact same-build sealed finance-case identity and remain
  -- fail-closed if that identity is missing or conflicting.
  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,source_id,finance_case_id,
    source_payload_json,financial_digest)
  VALUES(v_build,'FINANCE_CASE_IDENTITY','fixture-recovery-case',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'GLOBAL','JAMES_RATE_FIXTURE',v_recovery_case,v_recovery_case,
    jsonb_build_object('case_type','OVERPAYMENT'),md5('fixture-recovery-case'));

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    reserved_source_amount,finance_case_id,reservation_id,source_payload_json,financial_digest)
  VALUES(v_build,'RESERVATION_COMPONENT','fixture-sealed-case-recovery',v_candidate,
    v_timesheet,ARRAY[v_timesheet],'GLOBAL','JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',
    5,v_recovery_case,v_recovery_reservation,'{}'::jsonb,
    md5('fixture-sealed-case-recovery'));

  SELECT min(failure_code),count(*) FILTER(WHERE COALESCE(
      evidence_json#>'{sealed_physical_attribution,match_authorities}','[]'::jsonb)
      ? 'SIGNED_NON_CHARGE_RECOVERY_V1'),sum(reserved_ex_vat)
  INTO v_failure,v_count,v_reserved
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]);
  IF v_failure IS NOT NULL OR v_count<>1
     OR round(v_reserved,2) IS DISTINCT FROM 5::numeric THEN
    RAISE EXCEPTION 'JAMES_RATE_SEALED_CASE_RECOVERY_AUTHORITY_FAILED';
  END IF;

  DELETE FROM private.banking_pay_workbench_economic_build_facts
  WHERE build_id=v_build AND natural_key IN (
    'fixture-recovery-case','fixture-sealed-case-recovery');

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    amount_ex_vat,source_payload_json,financial_digest)
  VALUES(v_build,'FROZEN_SETTLED_COMPONENT','fixture-baseline-ambiguous',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',
    5,jsonb_build_object('frozen_source_basis_json',jsonb_build_object(
      'source_family_key',v_family,'source_pay_ex_vat',5,'source_charge_ex_vat',10)),
    md5('fixture-baseline-ambiguous'));

  UPDATE private.banking_pay_workbench_economic_build_facts
  SET baseline_ex_vat=20,baseline_inc_vat=20
  WHERE build_id=v_build AND fact_family='ENTITLEMENT_COMPONENT'
    AND natural_key='fixture-entitlement';

  SELECT min(failure_code),count(*) FILTER(WHERE component_kind='WORKED_TIME_RESIDUAL'),
    sum(baseline_ex_vat)
  INTO v_failure,v_residual_count,v_baseline
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]);
  IF v_failure IS NOT NULL OR v_residual_count<>1
     OR round(v_baseline,2) IS DISTINCT FROM 20::numeric THEN
    RAISE EXCEPTION 'JAMES_RATE_UNALLOCATED_BASELINE_RESIDUAL_FAILED';
  END IF;

  DELETE FROM private.banking_pay_workbench_economic_build_facts
  WHERE build_id=v_build AND fact_family='FROZEN_SETTLED_COMPONENT'
    AND natural_key='fixture-baseline-ambiguous';

  UPDATE private.banking_pay_workbench_economic_build_facts
  SET baseline_ex_vat=15,baseline_inc_vat=15
  WHERE build_id=v_build AND fact_family='ENTITLEMENT_COMPONENT'
    AND natural_key='fixture-entitlement';

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    reserved_source_amount,reservation_id,source_payload_json,financial_digest)
  VALUES(v_build,'RESERVATION_COMPONENT','fixture-reservation-ambiguous',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'GLOBAL','JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',5,
    v_ambiguous_reservation,'{}'::jsonb,md5('fixture-reservation-ambiguous'));

  SELECT min(failure_code),count(*) FILTER(WHERE component_kind='WORKED_TIME_RESIDUAL'),
    sum(reserved_ex_vat)
  INTO v_failure,v_residual_count,v_reserved
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]);
  IF v_failure IS NOT NULL OR v_residual_count<>1
     OR round(v_reserved,2) IS DISTINCT FROM 15::numeric THEN
    RAISE EXCEPTION 'JAMES_RATE_UNALLOCATED_RESERVATION_RESIDUAL_FAILED';
  END IF;

  -- Current financial occurrence authority is selected before historical
  -- baseline/reservation evidence for the same current economic key.  The
  -- lower tier remains bound into the complete evidence digest, but cannot
  -- turn a valid current PAYE row into an alphabetical PAYE/UMBRELLA choice.
  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    amount_ex_vat,source_payload_json,financial_digest)
  VALUES(v_build,'FROZEN_SETTLED_COMPONENT','fixture-method-lower-tier',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-13',
    0,jsonb_build_object('frozen_source_pay_method','UMBRELLA'),
    md5('fixture-method-lower-tier'));

  SELECT count(*)::integer
  INTO v_count
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]) projection
  WHERE projection.failure_code IS NOT NULL
     OR projection.source_pay_method IS DISTINCT FROM 'PAYE'
     OR COALESCE((projection.evidence_json#>>
       '{source_method_authority,selected_authority_priority}')::integer,0)<>10;
  IF v_count<>0 THEN
    RAISE EXCEPTION 'JAMES_RATE_CURRENT_OCCURRENCE_METHOD_PRIORITY_FAILED';
  END IF;

  DELETE FROM private.banking_pay_workbench_economic_build_facts
  WHERE build_id=v_build AND fact_family='FROZEN_SETTLED_COMPONENT'
    AND natural_key='fixture-method-lower-tier';

  -- A complete frozen nested component is an authoritative baseline-only
  -- physical row even when no current occurrence bucket exists for the key.
  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    truth_ex_vat,truth_inc_vat,baseline_ex_vat,baseline_inc_vat,financial_digest)
  VALUES(v_build,'ENTITLEMENT_COMPONENT','fixture-baseline-only-entitlement',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-14',
    0,0,11.25,11.25,md5('fixture-baseline-only-entitlement'));

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    amount_ex_vat,source_payload_json,financial_digest)
  VALUES(v_build,'FROZEN_SETTLED_COMPONENT','fixture-baseline-only-parent',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-14',
    11.25,jsonb_build_object('pay_batch_item',jsonb_build_object(
      'frozen_source_pay_method','PAYE',
      'frozen_resolution_payload_json',jsonb_build_object('case_components',jsonb_build_array(
        jsonb_build_object('component_amount_ex_vat',11.25,
          'component_member_identity','segment:baseline-only','bucket_code','DAY',
          'source_units',7.5,'source_rate',1.5,'source_charge_rate',10,
          'source_charge_ex_vat',75,'source_pay_method','PAYE'))))),
    md5('fixture-baseline-only-parent'));

  SELECT count(*)::integer,min(failure_code),sum(baseline_ex_vat)
  INTO v_count,v_failure,v_baseline
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]) projection
  WHERE projection.economic_key_type='TS_DAY'
    AND projection.economic_key_value='2026-08-14';
  IF v_count<>1 OR v_failure IS NOT NULL
     OR round(v_baseline,2) IS DISTINCT FROM 11.25::numeric THEN
    RAISE EXCEPTION 'JAMES_RATE_BASELINE_ONLY_NESTED_COMPONENT_REJECTED';
  END IF;

  -- Conflicts and invalid values still fail closed inside the selected frozen
  -- authority tier when no current occurrence exists for the key.
  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    amount_ex_vat,source_payload_json,financial_digest)
  VALUES(v_build,'FROZEN_SETTLED_COMPONENT','fixture-method-conflict',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-14',
    0,jsonb_build_object('frozen_source_pay_method','UMBRELLA'),
    md5('fixture-method-conflict'));

  SELECT count(*)::integer
  INTO v_count
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]) projection
  WHERE projection.economic_key_value='2026-08-14'
    AND projection.failure_code='RATE_AUTHORITY_SOURCE_PAY_METHOD_CONFLICT'
    AND projection.projection_status='FAILED'
    AND projection.source_pay_method IS NULL
    AND COALESCE((projection.evidence_json#>>
      '{source_method_authority,selected_authority_priority}')::integer,0)=20
    AND COALESCE((projection.evidence_json#>>
      '{source_method_authority,distinct_supported_source_method_count}')::integer,0)=2;
  IF v_count<1 THEN
    RAISE EXCEPTION 'JAMES_RATE_SELECTED_TIER_SOURCE_METHOD_CONFLICT_NOT_TYPED';
  END IF;

  DELETE FROM private.banking_pay_workbench_economic_build_facts
  WHERE build_id=v_build AND fact_family='FROZEN_SETTLED_COMPONENT'
    AND natural_key='fixture-method-conflict';

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,economic_key_type,economic_key_value,
    amount_ex_vat,source_payload_json,financial_digest)
  VALUES(v_build,'FROZEN_SETTLED_COMPONENT','fixture-method-invalid',v_candidate,v_timesheet,
    ARRAY[v_timesheet],'UNIT:'||v_timesheet::text,'JAMES_RATE_FIXTURE','TS_DAY','2026-08-14',
    0,jsonb_build_object('frozen_source_pay_method','PSC'),
    md5('fixture-method-invalid'));

  SELECT count(*)::integer
  INTO v_count
  FROM private.pay_workbench_sealed_rate_component_projection_v1(
    v_build,v_candidate,ARRAY[v_timesheet]) projection
  WHERE projection.economic_key_value='2026-08-14'
    AND projection.failure_code='RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING'
    AND projection.projection_status='FAILED'
    AND projection.source_pay_method IS NULL
    AND COALESCE((projection.evidence_json#>>
      '{source_method_authority,invalid_method_count}')::integer,0)>0;
  IF v_count<1 THEN
    RAISE EXCEPTION 'JAMES_RATE_SELECTED_TIER_INVALID_SOURCE_METHOD_NOT_TYPED';
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

DO $generated_resolution_audit_timestamp_fixture$
DECLARE
  v_before jsonb := jsonb_build_object(
    'id','13082026-1942-4000-8000-000000000099',
    'source_amount',220.00,
    'remaining_source_amount',220.00,
    'saved_resolution_result_json',jsonb_build_object(
      'amount_only_resolution_reused',true,
      'amount_only_resolution_reused_at_utc','2026-08-18T17:00:00Z',
      'source_remaining_amount_ex_vat',220.00,
      'target_remaining_amount_ex_vat',242.50,
      'target_source_ratio',1.102272727273
    )
  );
  v_capture jsonb;
  v_execute jsonb;
  v_economic_change jsonb;
BEGIN
  v_capture := private.pay_workbench_finance_effect_normalise_row_v1(
    'pay_finance_case_components','UPDATE',
    jsonb_set(v_before,
      '{saved_resolution_result_json,amount_only_resolution_reused_at_utc}',
      to_jsonb('2026-08-18T17:01:00Z'::text),false),
    v_before
  );
  v_execute := private.pay_workbench_finance_effect_normalise_row_v1(
    'pay_finance_case_components','UPDATE',
    jsonb_set(v_before,
      '{saved_resolution_result_json,amount_only_resolution_reused_at_utc}',
      to_jsonb('2026-08-18T17:02:00Z'::text),false),
    v_before
  );
  IF v_capture IS DISTINCT FROM v_execute THEN
    RAISE EXCEPTION 'GENERATED_RESOLUTION_AUDIT_TIMESTAMP_NOT_NORMALISED';
  END IF;

  v_economic_change := private.pay_workbench_finance_effect_normalise_row_v1(
    'pay_finance_case_components','UPDATE',
    jsonb_set(
      jsonb_set(v_before,'{remaining_source_amount}','221.00'::jsonb,false),
      '{saved_resolution_result_json,amount_only_resolution_reused_at_utc}',
      to_jsonb('2026-08-18T17:02:00Z'::text),false
    ),
    v_before
  );
  IF v_capture IS NOT DISTINCT FROM v_economic_change THEN
    RAISE EXCEPTION 'GENERATED_RESOLUTION_NORMALISER_MASKED_ECONOMIC_CHANGE';
  END IF;
END;
$generated_resolution_audit_timestamp_fixture$;

ROLLBACK;
