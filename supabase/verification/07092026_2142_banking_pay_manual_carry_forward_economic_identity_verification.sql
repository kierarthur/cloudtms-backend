-- Rollback-contained verification for the manual carry-forward Workbench
-- economic identity bridge. No business or provider row is written.

\set ON_ERROR_STOP on

BEGIN;

DO $verification$
DECLARE
  v_oid oid := pg_catalog.to_regprocedure(
    'public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)'
  );
  v_definition text;
  v_contract jsonb;
  v_test_id uuid := '21402140-2140-4140-8140-214021402140'::uuid;
BEGIN
  IF v_oid IS NULL THEN
    RAISE EXCEPTION 'MANUAL_CARRY_FORWARD_CANONICAL_PRODUCER_MISSING';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_proc AS function_row
    WHERE function_row.oid = v_oid
      AND function_row.prosecdef
      AND function_row.provolatile = 'v'
      AND function_row.proparallel = 'u'
      AND pg_catalog.pg_get_userbyid(function_row.proowner) IN ('postgres', current_user)
      AND (
        function_row.proconfig = ARRAY['search_path=public']::text[]
        OR (
          COALESCE(function_row.proconfig, ARRAY[]::text[]) @> ARRAY[
            'search_path=public',
            'plpgsql_check.mode=disabled',
            'plpgsql_check.profiler=off',
            'plpgsql_check.tracer=off',
            'plpgsql_check.constants_tracing=off',
            'plpgsql_check.cursors_leaks=off',
            'plpgsql_check.strict_cursors_leaks=off',
            'plpgsql_check.fatal_errors=off'
          ]::text[]
          AND COALESCE(function_row.proconfig, ARRAY[]::text[]) <@ ARRAY[
            'search_path=public',
            'plpgsql_check.mode=disabled',
            'plpgsql_check.profiler=off',
            'plpgsql_check.tracer=off',
            'plpgsql_check.constants_tracing=off',
            'plpgsql_check.cursors_leaks=off',
            'plpgsql_check.strict_cursors_leaks=off',
            'plpgsql_check.fatal_errors=off'
          ]::text[]
          AND pg_catalog.cardinality(function_row.proconfig) = 8
        )
      )
  ) THEN
    RAISE EXCEPTION 'MANUAL_CARRY_FORWARD_CANONICAL_PRODUCER_METADATA_MISMATCH';
  END IF;

  IF NOT pg_catalog.has_function_privilege('service_role', v_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('anon', v_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('authenticated', v_oid, 'EXECUTE') THEN
    RAISE EXCEPTION 'MANUAL_CARRY_FORWARD_CANONICAL_PRODUCER_ACL_MISMATCH';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(v_oid) INTO v_definition;
  IF pg_catalog.strpos(v_definition, '''component_key_type'', ''MANUAL_CARRY_FORWARD''') = 0
     OR pg_catalog.strpos(v_definition, '''component_key_value'', cf_lines.manual_adjustment_carry_forward_id::text') = 0
     OR pg_catalog.strpos(v_definition, '''key_type'', ''MANUAL_CARRY_FORWARD''') = 0
     OR pg_catalog.strpos(v_definition, '''key_value'', cf_lines.manual_adjustment_carry_forward_id::text') = 0
     OR pg_catalog.strpos(v_definition, '''economic_key'', jsonb_strip_nulls(jsonb_build_object(') = 0
     OR pg_catalog.strpos(v_definition, '''timesheet_id'', CASE WHEN cf_lines.timesheet_id IS NULL THEN NULL ELSE cf_lines.timesheet_id::text END') = 0 THEN
    RAISE EXCEPTION 'MANUAL_CARRY_FORWARD_CANONICAL_PRODUCER_IDENTITY_MISSING';
  END IF;

  v_contract := public.pay_workbench_preview_line_contract_ok(
    p_line_json => pg_catalog.jsonb_build_object(
      'line_key', 'carry_forward:' || v_test_id::text,
      'line_type', 'MANUAL_ADJUSTMENT_CARRY_FORWARD',
      'case_type', 'MANUAL_ADJUSTMENT_CARRY_FORWARD',
      'case_key', 'carry_forward:' || v_test_id::text,
      'manual_adjustment_carry_forward_id', v_test_id::text,
      'source_ref', 'carry_forward:' || v_test_id::text,
      'item_direction', 'CREDIT',
      'amount_ex_vat', 1.00,
      'draftable', true,
      'is_ready_for_draft', true,
      'is_excluded_from_allocation', false,
      'presentation_section', 'READY_TO_PAY'
    ),
    p_economic_key_json => pg_catalog.jsonb_build_object(
      'key_type', 'MANUAL_CARRY_FORWARD',
      'key_value', v_test_id::text
    ),
    p_target_section => 'canonical_preview_lines'
  );

  IF COALESCE((v_contract->>'ok')::boolean, false) IS NOT TRUE
     OR v_contract->>'key_type' IS DISTINCT FROM 'MANUAL_CARRY_FORWARD'
     OR v_contract->>'key_value' IS DISTINCT FROM v_test_id::text
     OR COALESCE((v_contract->>'is_recognised_manual_carry_forward')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'MANUAL_CARRY_FORWARD_PREVIEW_CONTRACT_REJECTED_IDENTITY: %', v_contract;
  END IF;
END;
$verification$;

ROLLBACK;
