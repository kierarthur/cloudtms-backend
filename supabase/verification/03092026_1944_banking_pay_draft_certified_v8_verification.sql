\set ON_ERROR_STOP on

-- Read-only installation and security proof for the row-backed certified
-- Create Draft consumer.  This verifier creates no Draft, payment, provider,
-- settlement or remittance state and does not execute any high-cardinality
-- workload.
DO $verification$
DECLARE
  v_relation text;
  v_identity text;
  v_expected_search_path text;
  v_expected_security_definer boolean;
  v_oid oid;
  v_owner text;
  v_security_definer boolean;
  v_volatility "char";
  v_parallel "char";
  v_config text[];
  v_definition text;
  v_missing_indexes text[];
BEGIN
  FOREACH v_relation IN ARRAY ARRAY[
    'private.banking_pay_draft_frozen_certificate_scopes_v8',
    'private.banking_pay_draft_frozen_constituent_refs_v8',
    'private.banking_pay_draft_frozen_constituent_payloads_v8',
    'private.banking_pay_draft_frozen_partition_refs_v8',
    'private.banking_pay_draft_frozen_candidate_inputs_v8',
    'private.banking_pay_draft_frozen_candidate_scopes_v8',
    'private.banking_pay_draft_frozen_candidate_scope_members_v8',
    'private.banking_pay_draft_frozen_stage_receipts_v8',
    'private.banking_pay_draft_phase_units_v1',
    'private.banking_pay_draft_owner_receipts_v1',
    'private.banking_pay_draft_operation_created_batches_v8',
    'private.banking_pay_draft_operation_terminal_results_v8',
    'private.banking_pay_draft_operation_provenance_events_v8',
    'private.banking_pay_draft_finalizer_iterations_v8',
    'private.banking_pay_draft_constituent_parity_results_v8'
  ] LOOP
    IF pg_catalog.to_regclass(v_relation) IS NULL THEN
      RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_RELATION_MISSING: %',v_relation;
    END IF;
    IF (
      SELECT pg_catalog.pg_get_userbyid(class_row.relowner)
      FROM pg_catalog.pg_class AS class_row
      WHERE class_row.oid=pg_catalog.to_regclass(v_relation)
    ) NOT IN (CURRENT_USER,'postgres') THEN
      RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_RELATION_OWNER_INVALID: %',v_relation;
    END IF;
    IF pg_catalog.has_table_privilege('anon',v_relation,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
       OR pg_catalog.has_table_privilege('authenticated',v_relation,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
       OR pg_catalog.has_table_privilege('service_role',v_relation,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') THEN
      RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_RELATION_ACL_INVALID: %',v_relation;
    END IF;
  END LOOP;

  -- Public entry points are service-only definer functions.  The search path
  -- is declared per current owner and must not be relaxed during release.
  FOR v_identity,v_expected_search_path IN
    SELECT * FROM (VALUES
      ('public.banking_pay_draft_certified_operation_start_v8(jsonb,uuid,text)','search_path=""'),
      ('public.pay_workbench_draft_certificate_constituent_ref_page_v8(uuid,integer,integer,text)','search_path=""'),
      ('public.pay_workbench_draft_certificate_partition_ref_page_v8(uuid,integer,integer,text)','search_path=""'),
      ('public.pay_workbench_draft_certificate_final_freeze_v8(uuid,text)','search_path=""'),
      ('public.pay_workbench_settled_certificate_reference_validate_v8(uuid,text,text,text)','search_path=""'),
      ('public.banking_pay_draft_phase_units_seed_v8(uuid,integer,integer,text)','search_path=""'),
      ('public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(uuid,integer,integer,text)','search_path=""'),
      ('public.pay_workbench_draft_constituent_parity_page_v8(uuid,integer,integer,text)','search_path=""'),
      ('public.banking_pay_draft_advance_bounded_v8(uuid,text,text)','search_path=""'),
      ('public.banking_pay_draft_operation_finish_v8(uuid,text,jsonb,jsonb)','search_path=""'),
      ('public.banking_pay_draft_certificate_stage_advance_v8(uuid,text)','search_path=""'),
      ('public.banking_pay_draft_readiness_page_v8(uuid,text,text,integer,integer)','search_path=""'),
      ('public.pay_workbench_prepare_draft_allocation_rows_seed(uuid,jsonb)','search_path=public, pg_temp'),
      ('public.pay_batch_stage_operation_candidate_chunk_context(uuid,uuid,jsonb,uuid)','search_path=public, pg_temp'),
      ('public.pay_batch_insert_items_from_preview(uuid,uuid,uuid,jsonb)','search_path=public'),
      ('public.pay_batch_apply_finance_adjustments(uuid,text,uuid,numeric,date,uuid,jsonb)','search_path=public'),
      ('public.pay_batch_assert_integrity(uuid,uuid,uuid)','search_path=public')
    ) AS expected(identity,search_path)
  LOOP
    v_oid:=pg_catalog.to_regprocedure(v_identity);
    IF v_oid IS NULL THEN
      RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_FUNCTION_MISSING: %',v_identity;
    END IF;
    SELECT pg_catalog.pg_get_userbyid(proc.proowner),proc.prosecdef,
      proc.provolatile,proc.proparallel,proc.proconfig
    INTO STRICT v_owner,v_security_definer,v_volatility,v_parallel,v_config
    FROM pg_catalog.pg_proc AS proc WHERE proc.oid=v_oid;
    IF v_owner NOT IN (CURRENT_USER,'postgres')
       OR NOT v_security_definer
       OR v_volatility<>'v'
       OR v_parallel<>'u'
       OR NOT (v_expected_search_path=ANY(coalesce(v_config,ARRAY[]::text[])))
       OR pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
       OR pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE')
       OR NOT pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE') THEN
      RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_FUNCTION_SECURITY_INVALID: %',v_identity;
    END IF;
  END LOOP;

  -- Private adapters remain owner-only.  Security-definer status is explicit
  -- rather than inferred, preventing accidental privilege-boundary drift.
  FOR v_identity,v_expected_security_definer IN
    SELECT * FROM (VALUES
      ('private.banking_pay_draft_frozen_certificate_scope_initialise_v8(uuid)',true),
      ('private.pay_workbench_draft_scope_line_rows_v8(uuid,jsonb,jsonb)',false),
      ('private.pay_workbench_prepare_draft_scope_from_certificate_partition_v8(uuid,uuid,integer,text)',false),
      ('private.pay_workbench_draft_constituent_parity_compare_v8(uuid,integer)',false),
      ('private.banking_pay_draft_owner_receipt_record_v8(uuid,text,integer,text,jsonb,boolean,integer,integer)',false),
      ('private.banking_pay_draft_terminal_context_build_v8(uuid)',true),
      ('private.pay_workbench_operation_active_snoozes_v8(uuid,uuid,jsonb)',false),
      ('private.pay_workbench_operation_selected_lines_v8(uuid,uuid)',false),
      ('private.pay_workbench_operation_selected_line_count_v8(uuid,uuid)',false)
    ) AS expected(identity,security_definer)
  LOOP
    v_oid:=pg_catalog.to_regprocedure(v_identity);
    IF v_oid IS NULL THEN
      RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_PRIVATE_FUNCTION_MISSING: %',v_identity;
    END IF;
    SELECT pg_catalog.pg_get_userbyid(proc.proowner),proc.prosecdef,proc.proconfig
    INTO STRICT v_owner,v_security_definer,v_config
    FROM pg_catalog.pg_proc AS proc WHERE proc.oid=v_oid;
    IF v_owner NOT IN (CURRENT_USER,'postgres')
       OR v_security_definer IS DISTINCT FROM v_expected_security_definer
       OR NOT ('search_path=""'=ANY(coalesce(v_config,ARRAY[]::text[])))
       OR pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
       OR pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE')
       OR pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE')
       OR NOT pg_catalog.has_function_privilege(CURRENT_USER,v_oid,'EXECUTE') THEN
      RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_PRIVATE_FUNCTION_SECURITY_INVALID: %',v_identity;
    END IF;
  END LOOP;

  SELECT pg_catalog.array_agg(required.index_name ORDER BY required.index_name)
  INTO v_missing_indexes
  FROM pg_catalog.unnest(ARRAY[
    'banking_pay_draft_frozen_refs_v8_page_idx',
    'banking_pay_draft_frozen_payloads_v8_scope_page_idx',
    'banking_pay_draft_frozen_payloads_v8_timesheet_page_idx',
    'banking_pay_draft_frozen_partitions_v8_page_idx',
    'banking_pay_draft_frozen_candidate_inputs_v8_page_idx',
    'banking_pay_draft_frozen_scopes_v8_page_idx',
    'banking_pay_draft_scope_members_v8_page_idx',
    'banking_pay_draft_scope_members_v8_constituent_idx',
    'banking_pay_draft_created_batches_v8_page_idx',
    'banking_pay_draft_finalizer_iterations_v8_next_idx',
    'banking_pay_draft_parity_results_v8_page_idx',
    'banking_pay_draft_phase_units_v1_next_idx',
    'banking_pay_draft_owner_receipts_v1_page_idx',
    'banking_pay_operations_v8_legacy_activation_idx'
  ]::text[]) AS required(index_name)
  WHERE pg_catalog.to_regclass(
    CASE WHEN required.index_name='banking_pay_operations_v8_legacy_activation_idx'
      THEN 'public.'||required.index_name ELSE 'private.'||required.index_name END
  ) IS NULL;
  IF v_missing_indexes IS NOT NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_INDEXES_MISSING: %',v_missing_indexes;
  END IF;

  -- Preserve the established RPC budgets; passing the release by increasing
  -- either timeout is expressly forbidden.
  SELECT proc.proconfig INTO STRICT v_config
  FROM pg_catalog.pg_proc AS proc
  WHERE proc.oid='public.banking_pay_draft_certified_operation_start_v8(jsonb,uuid,text)'::regprocedure;
  IF NOT (v_config @> ARRAY['statement_timeout=6000ms','lock_timeout=1000ms']::text[]) THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_START_BUDGET_INVALID';
  END IF;
  v_definition:=pg_catalog.pg_get_functiondef(
    'public.banking_pay_draft_operation_finish_v8(uuid,text,jsonb,jsonb)'::regprocedure
  );
  IF v_definition !~* 'set_config\(''statement_timeout'',\s*''15000'',\s*true\)'
     OR v_definition !~* 'set_config\(''lock_timeout'',\s*''1500'',\s*true\)' THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_FINISH_BUDGET_INVALID';
  END IF;

  -- The supported ceiling is 50,000, while runtime load proof is deliberately
  -- capped at 5,000.  This source guard proves the scalar boundary only and
  -- does not construct a 50,000-row fixture.
  v_definition:=pg_catalog.pg_get_functiondef(
    'private.banking_pay_draft_frozen_certificate_scope_initialise_v8(uuid)'::regprocedure
  );
  IF v_definition !~* 'constituent_count\s+NOT\s+BETWEEN\s+1\s+AND\s+50000' THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_CEILING_GUARD_MISSING';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.pg_proc AS proc
    JOIN pg_catalog.pg_namespace AS namespace_row ON namespace_row.oid=proc.pronamespace
    WHERE namespace_row.nspname IN ('public','private')
      AND proc.proname IN (
        'banking_pay_draft_certified_operation_start_v8',
        'pay_workbench_draft_certificate_constituent_ref_page_v8',
        'pay_workbench_draft_certificate_partition_ref_page_v8',
        'pay_workbench_draft_certificate_final_freeze_v8',
        'pay_workbench_settled_certificate_reference_validate_v8',
        'banking_pay_draft_phase_units_seed_v8',
        'pay_workbench_prepare_draft_scope_from_frozen_page_v8',
        'pay_workbench_draft_constituent_parity_page_v8',
        'banking_pay_draft_advance_bounded_v8',
        'banking_pay_draft_operation_finish_v8',
        'banking_pay_draft_certificate_stage_advance_v8',
        'banking_pay_draft_readiness_page_v8'
      )
      AND pg_catalog.pg_get_functiondef(proc.oid) ~* 'pg_catalog\.(coalesce|nullif|least|greatest)\s*\('
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_CERTIFIED_V8_ILLEGAL_CONDITIONAL_PREFIX';
  END IF;
END
$verification$;
