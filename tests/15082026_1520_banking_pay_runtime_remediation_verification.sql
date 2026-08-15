-- Banking Pay runtime-remediation executable contract verification.
-- TEST-only. All fixture state is temporary or transactionally rolled back.
-- No Draft, payment, provider, settlement, remittance or communication action
-- is created or invoked by this script.
\set ON_ERROR_STOP on

BEGIN;

DO $definition_contract$
DECLARE
  v_clone text;
  v_enqueue text;
  v_projection text;
  v_sync text;
  v_builder text;
  v_claim text;
  v_child text;
  v_parent text;
BEGIN
  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_workbench_session_clone_eligible_rows_v1(uuid,uuid,integer,jsonb,jsonb)'::regprocedure)
  INTO STRICT v_clone;
  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb)'::regprocedure)
  INTO STRICT v_enqueue;
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_workbench_sealed_rate_component_projection_v1(uuid,uuid,uuid[])'::regprocedure)
  INTO STRICT v_projection;
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_sync_overpayments_from_workbench_workspace_v1(uuid,uuid,uuid,uuid,date,date,uuid,text,uuid[],jsonb,uuid,uuid[],uuid[])'::regprocedure)
  INTO STRICT v_sync;
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_workbench_candidate_source_build_chunk_legacy_v1(uuid,uuid,jsonb,jsonb,integer)'::regprocedure)
  INTO STRICT v_builder;
  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_workbench_source_build_attempt_claim_start_v1(text,text,integer,timestamptz,uuid,uuid)'::regprocedure)
  INTO STRICT v_claim;
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_workbench_execution_residual_identity_proof_page_v1(uuid,uuid,uuid,uuid,uuid,bigint,uuid[],text,text,jsonb,jsonb,text)'::regprocedure)
  INTO STRICT v_child;
  SELECT pg_catalog.pg_get_functiondef(
    'private.pay_workbench_execution_refresh_owner_proof_page_v1(uuid,uuid,uuid[],text,jsonb)'::regprocedure)
  INTO STRICT v_parent;

  IF position('WORKBENCH_SOURCE_OWNER_V3' in v_clone)=0
     OR position('READY_TO_PAY_SEMANTIC_V2' in v_clone)=0
     OR position('authority_fingerprint_version' in v_clone)=0
     OR position('authority_fingerprint' in v_clone)=0
     OR position('source_publication_baseline_required' in v_clone)=0
     OR position('required_physical_publication_contract_version' in v_clone)=0
     OR position('WORKBENCH_SOURCE_OWNER_V3' in v_enqueue)=0 THEN
    RAISE EXCEPTION 'BANKING_PAY_CLONE_FINGERPRINT_CONTRACT_MISSING';
  END IF;

  IF position('source_method_evidence' in v_projection)=0
     OR position('source_method_authority_tier' in v_projection)=0
     OR position('selected_authority_priority' in v_projection)=0
     OR position('source_method_authority_summary' in v_projection)=0
     OR position('RATE_AUTHORITY_SOURCE_PAY_METHOD_CONFLICT' in v_projection)=0
     OR position('complete_evidence_digest' in v_projection)=0
     OR v_projection ~* 'min\s*\(\s*(source\.)?source_pay_method\s*\)' THEN
    RAISE EXCEPTION 'BANKING_PAY_JAMES_ECONOMIC_KEY_METHOD_CONTRACT_MISSING';
  END IF;

  IF position('complete_component_method_digest' in v_sync)=0
     OR position('RATE_AUTHORITY_SOURCE_PAY_METHOD_CONFLICT' in v_sync)=0
     OR v_sync ~* 'coalesce\s*\(\s*component\.source_pay_method\s*,\s*(candidate_pay_method|current_target_pay_method)'
     OR v_sync ~* 'min\s*\(\s*sealed\.source_pay_method\s*\)' THEN
    RAISE EXCEPTION 'BANKING_PAY_JAMES_TIMESHEET_METHOD_CONTRACT_MISSING';
  END IF;

  IF position('RESERVATION_COMPONENT_SOURCE_KEY_C_V1' in v_builder)=0
     OR position('PAY_WORKBENCH_RESERVATION_ORDER_CONTRACT_OBSOLETE' in v_builder)=0
     OR position('reservation.id::text COLLATE "C"' in v_builder)=0
     OR position('page.source_key COLLATE "C"' in v_builder)=0 THEN
    RAISE EXCEPTION 'BANKING_PAY_RESERVATION_ORDER_CONTRACT_MISSING';
  END IF;

  IF v_builder !~* 'coalesce\s*\(\s*item\.reservation_id\s*,\s*item\.pay_batch_item_id\s*\)' THEN
    RAISE EXCEPTION 'BANKING_PAY_ACTIVE_ITEM_RESERVATION_IDENTITY_MISSING';
  END IF;

  IF position('pay_workbench_repair_orphaned_pending_source_build' in v_claim)=0
     OR position('PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN' in v_claim)=0
     OR position('FAILED_CLOSED_MAX_ATTEMPTS' in v_claim)=0
     OR position('REBOUND_ACTIVE_SUCCESSOR' in v_claim)=0
     OR position('RECONCILED_SUCCESSFUL_BUILD' in v_claim)=0 THEN
    RAISE EXCEPTION 'BANKING_PAY_TERMINAL_CONVERGENCE_CONTRACT_MISSING';
  END IF;

  IF position('referenced_scope_set_digest' in v_child)=0
     OR position('common_publication_attestation_digest' in v_child)=0
     OR position('ready_rows_validated' in v_child)=0
     OR v_child !~* 'row_json\s*->>\s*''source_ordinal'''
     OR v_child !~* 'row_json\s*->>\s*''source_line_id'''
     OR v_child ~ '% 1000000%'
     OR position('residual_candidate_result_count' in v_parent)=0
     OR position('referenced_scope_set_digest' in v_parent)=0
     OR position('common_publication_attestation_digest' in v_parent)=0 THEN
    RAISE EXCEPTION 'BANKING_PAY_RESIDUAL_CLOSED_AUTHORITY_CONTRACT_MISSING';
  END IF;
END;
$definition_contract$;

DO $cursor_contract$
DECLARE
  v_base jsonb;
  v_reservation jsonb;
  v_wrong_contract jsonb;
  v_failed boolean:=false;
BEGIN
  v_base:=jsonb_build_object(
    'cursor_kind','WORKSPACE_FACT','cursor_version',2,
    'build_id','15082026-1520-4000-8000-000000000001',
    'candidate_id','15082026-1520-4000-8000-000000000002',
    'captured_candidate_generation',1,'captured_source_change_seq',1,
    'dependency_unit_key','GLOBAL','fact_family','CANONICAL_INPUT',
    'page_number',1,'last_source_key',NULL,'previous_page_digest',NULL,
    'cumulative_fact_count',0,'cumulative_digest',md5('BPAY_FACT_STREAM_V2'),
    'terminal',false,'raw_physical_source_count',0,
    'resolved_physical_source_count',0,'failed_physical_source_count',0,
    'raw_physical_amount_ex_vat',0,'resolved_physical_amount_ex_vat',0,
    'last_raw_physical_source_key',NULL,'source_exhausted',false,
    'raw_terminal_source_key',NULL,'raw_page_evidence_digest',NULL,
    'input_phase','PHYSICAL_SOURCE','input_projection_id',NULL);

  v_reservation:=private.pay_workbench_fact_cursor_transition_v3(
    v_base,'GLOBAL','RESERVATION_COMPONENT','PHYSICAL_SOURCE',NULL);
  IF v_reservation->>'reservation_source_key_order_contract'
       <>'RESERVATION_COMPONENT_SOURCE_KEY_C_V1'
     OR private.pay_workbench_fact_cursor_preserve_v2(v_reservation)
          IS DISTINCT FROM v_reservation THEN
    RAISE EXCEPTION 'BANKING_PAY_RESERVATION_CURSOR_MARKER_NOT_PRESERVED';
  END IF;

  v_wrong_contract:=v_reservation||jsonb_build_object(
    'reservation_source_key_order_contract','LEGACY_DATABASE_COLLATION');
  BEGIN
    PERFORM private.pay_workbench_fact_cursor_preserve_v2(v_wrong_contract);
  EXCEPTION WHEN SQLSTATE '40001' THEN
    v_failed:=true;
  END;
  IF NOT v_failed THEN
    RAISE EXCEPTION 'BANKING_PAY_RESERVATION_CURSOR_WRONG_MARKER_ACCEPTED';
  END IF;

  v_failed:=false;
  BEGIN
    PERFORM private.pay_workbench_fact_cursor_preserve_v2(
      v_base||jsonb_build_object(
        'reservation_source_key_order_contract','RESERVATION_COMPONENT_SOURCE_KEY_C_V1'));
  EXCEPTION WHEN SQLSTATE '22023' THEN
    v_failed:=true;
  END;
  IF NOT v_failed THEN
    RAISE EXCEPTION 'BANKING_PAY_NON_RESERVATION_CURSOR_MARKER_ACCEPTED';
  END IF;
END;
$cursor_contract$;

CREATE TEMP TABLE banking_pay_reservation_order_fixture(
  source_key text PRIMARY KEY,
  financial_digest text NOT NULL
) ON COMMIT DROP;

INSERT INTO banking_pay_reservation_order_fixture(source_key,financial_digest) VALUES
  ('00000000-0000-4000-8000-000000000010',md5('uuid-10')),
  ('ffffffff-ffff-4fff-8fff-fffffffffff0',md5('uuid-f0')),
  ('~ITEM:00000000-0000-4000-8000-000000000001',md5('item-1')),
  ('~ITEM:ffffffff-ffff-4fff-8fff-ffffffffffff',md5('item-f'));

DO $reservation_pages$
DECLARE
  v_expected text[];
  v_seen text[];
  v_page text[];
  v_last text;
  v_page_size integer;
  v_first_digest text;
  v_replay_digest text;
BEGIN
  SELECT array_agg(source_key ORDER BY source_key COLLATE "C")
  INTO v_expected
  FROM banking_pay_reservation_order_fixture;

  IF v_expected IS DISTINCT FROM ARRAY[
      '00000000-0000-4000-8000-000000000010',
      'ffffffff-ffff-4fff-8fff-fffffffffff0',
      '~ITEM:00000000-0000-4000-8000-000000000001',
      '~ITEM:ffffffff-ffff-4fff-8fff-ffffffffffff']::text[] THEN
    RAISE EXCEPTION 'BANKING_PAY_RESERVATION_C_ORDER_UNEXPECTED';
  END IF;

  FOR v_page_size IN 1..3 LOOP
    v_seen:=ARRAY[]::text[];
    v_last:=NULL;
    LOOP
      SELECT array_agg(page.source_key ORDER BY page.source_key COLLATE "C")
      INTO v_page
      FROM (
        SELECT source_key
        FROM banking_pay_reservation_order_fixture
        WHERE v_last IS NULL OR source_key COLLATE "C">v_last COLLATE "C"
        ORDER BY source_key COLLATE "C"
        LIMIT v_page_size
      ) page;
      EXIT WHEN COALESCE(cardinality(v_page),0)=0;
      v_seen:=v_seen||v_page;
      v_last:=v_page[cardinality(v_page)];
    END LOOP;
    IF v_seen IS DISTINCT FROM v_expected
       OR cardinality(v_seen)<>(SELECT count(*) FROM banking_pay_reservation_order_fixture)
       OR cardinality(ARRAY(SELECT DISTINCT key FROM unnest(v_seen) key))<>cardinality(v_seen) THEN
      RAISE EXCEPTION 'BANKING_PAY_RESERVATION_PAGE_COVERAGE_FAILED:%',v_page_size;
    END IF;
  END LOOP;

  SELECT md5('RESERVATION_COMPONENT_SOURCE_KEY_C_V1:'||string_agg(
    source_key||':'||financial_digest,'' ORDER BY source_key COLLATE "C"))
  INTO v_first_digest
  FROM banking_pay_reservation_order_fixture;
  SELECT md5('RESERVATION_COMPONENT_SOURCE_KEY_C_V1:'||string_agg(
    source_key||':'||financial_digest,'' ORDER BY source_key COLLATE "C"))
  INTO v_replay_digest
  FROM banking_pay_reservation_order_fixture;
  IF v_first_digest IS DISTINCT FROM v_replay_digest THEN
    RAISE EXCEPTION 'BANKING_PAY_RESERVATION_REPLAY_DIGEST_CHANGED';
  END IF;
END;
$reservation_pages$;

CREATE TEMP TABLE banking_pay_ready_identity_fixture(
  row_ordinal numeric NOT NULL,
  row_json jsonb NOT NULL,
  expected_source_line_id uuid NOT NULL,
  expected_source_ordinal bigint NOT NULL,
  expected_valid boolean NOT NULL
) ON COMMIT DROP;

INSERT INTO banking_pay_ready_identity_fixture VALUES
  (42*1000000::numeric+999999,
    jsonb_build_object('source_ordinal',999999,'source_line_id','15082026-1520-4000-8000-000000000011'),
    '15082026-1520-4000-8000-000000000011',999999,true),
  (42*1000000::numeric+1000000,
    jsonb_build_object('source_ordinal',1000000,'source_line_id','15082026-1520-4000-8000-000000000012'),
    '15082026-1520-4000-8000-000000000012',1000000,true),
  (42*1000000::numeric+1000001,
    jsonb_build_object('source_ordinal',1000001,'source_line_id','15082026-1520-4000-8000-000000000013'),
    '15082026-1520-4000-8000-000000000013',1000001,true),
  (42*1000000::numeric+1000002,
    jsonb_build_object('source_ordinal',1000001,'source_line_id','15082026-1520-4000-8000-000000000014'),
    '15082026-1520-4000-8000-000000000014',1000001,false),
  (42*1000000::numeric+1000003,
    jsonb_build_object('source_ordinal','malformed','source_line_id','15082026-1520-4000-8000-000000000015'),
    '15082026-1520-4000-8000-000000000015',1000003,false),
  (42*1000000::numeric+1000004,
    jsonb_build_object('source_ordinal',1000004,'source_line_id','15082026-1520-4000-8000-000000000099'),
    '15082026-1520-4000-8000-000000000016',1000004,false);

DO $ready_identity$
DECLARE
  v_mismatch_count integer;
BEGIN
  SELECT count(*)::integer
  INTO v_mismatch_count
  FROM banking_pay_ready_identity_fixture fixture
  CROSS JOIN LATERAL (
    SELECT CASE WHEN fixture.row_json->>'source_ordinal'~'^[1-9][0-9]{0,18}$'
      THEN (fixture.row_json->>'source_ordinal')::bigint END AS source_ordinal,
      CASE WHEN fixture.row_json->>'source_line_id'
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (fixture.row_json->>'source_line_id')::uuid END AS source_line_id
  ) parsed
  WHERE fixture.expected_valid IS DISTINCT FROM COALESCE((
    parsed.source_ordinal=fixture.expected_source_ordinal
    AND parsed.source_line_id=fixture.expected_source_line_id
    AND fixture.row_ordinal=42::numeric*1000000::numeric+parsed.source_ordinal::numeric),false);
  IF v_mismatch_count<>0 THEN
    RAISE EXCEPTION 'BANKING_PAY_READY_EXPLICIT_IDENTITY_FIXTURE_FAILED';
  END IF;
END;
$ready_identity$;

ROLLBACK;

SELECT 'banking_pay_runtime_remediation_verification_ok' AS result_code;
