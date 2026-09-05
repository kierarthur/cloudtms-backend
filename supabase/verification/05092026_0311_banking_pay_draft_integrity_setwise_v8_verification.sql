\set ON_ERROR_STOP on

-- Read-only installed-definition proof for the policy-neutral set-wise Draft
-- integrity owner. Runtime authority is Miget TEST; the `supabase` path name
-- is historical. This verifier creates no Draft and performs no payment,
-- provider, settlement, remittance or application-data mutation.
DO $verification$
DECLARE
  v_oid oid := pg_catalog.to_regprocedure('public.pay_batch_assert_integrity(uuid,uuid,uuid)');
  v_owner text;
  v_security_definer boolean;
  v_volatility "char";
  v_parallel "char";
  v_config text[];
  v_public_execute boolean;
  v_definition text;
  v_check_code text;
BEGIN
  IF v_oid IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_INTEGRITY_SETWISE_FUNCTION_MISSING';
  END IF;

  SELECT
    pg_catalog.pg_get_userbyid(proc.proowner),
    proc.prosecdef,
    proc.provolatile,
    proc.proparallel,
    proc.proconfig,
    EXISTS (
      SELECT 1
      FROM pg_catalog.aclexplode(
        coalesce(proc.proacl, pg_catalog.acldefault('f', proc.proowner))
      ) AS acl
      WHERE acl.grantee = 0
        AND acl.privilege_type = 'EXECUTE'
    ),
    pg_catalog.pg_get_functiondef(proc.oid)
  INTO STRICT
    v_owner,
    v_security_definer,
    v_volatility,
    v_parallel,
    v_config,
    v_public_execute,
    v_definition
  FROM pg_catalog.pg_proc AS proc
  WHERE proc.oid = v_oid;

  IF v_owner NOT IN (CURRENT_USER, 'postgres')
     OR NOT v_security_definer
     OR v_volatility <> 'v'
     OR v_parallel <> 'u'
     OR NOT ('search_path=public' = ANY(coalesce(v_config, ARRAY[]::text[])))
     OR v_public_execute
     OR pg_catalog.has_function_privilege('anon', v_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('authenticated', v_oid, 'EXECUTE')
     OR NOT pg_catalog.has_function_privilege('service_role', v_oid, 'EXECUTE') THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_INTEGRITY_SETWISE_SECURITY_INVALID';
  END IF;

  IF pg_catalog.regexp_count(v_definition, 'private\.pay_workbench_operation_selected_lines_v8\(') <> 1
     OR v_definition !~ 'WITH operation_scope_rows AS MATERIALIZED'
     OR v_definition !~ 'operation_reservation_rows AS MATERIALIZED'
     OR v_definition !~ 'operation_first_reservation_rows AS MATERIALIZED'
     OR v_definition !~ 'operation_selected_source_lines AS MATERIALIZED'
     OR v_definition !~ 'operation_selected_lines AS MATERIALIZED'
     OR v_definition !~ 'operation_allocation_rows AS MATERIALIZED'
     OR v_definition !~ 'operation_batch_items AS MATERIALIZED'
     OR v_definition !~ 'allocation_row\.candidate_scope_id = selected_line\.candidate_scope_id'
     OR v_definition !~ 'allocation_row\.bound_preview_row_id = selected_line\.preview_row_id'
     OR v_definition !~ 'item_row\.id = allocation_row\.pay_batch_item_id'
     OR v_definition ~ 'operation_source_key LIKE \(''%'' \|\| selected_line\.preview_row_id \|\| ''%''\)'
     OR v_definition ~* 'SET[[:space:]]+(LOCAL[[:space:]]+)?(statement_timeout|lock_timeout|idle_in_transaction_session_timeout)' THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_INTEGRITY_SETWISE_DEFINITION_INVALID';
  END IF;

  FOREACH v_check_code IN ARRAY ARRAY[
    'MISSING_BATCH_CANDIDATE',
    'UNLINKED_ALLOCATION_ROW',
    'DUPLICATE_BATCH_CANDIDATE',
    'DUPLICATE_OPERATION_ITEM_KEY',
    'DUPLICATE_OPERATION_BREAKDOWN_KEY',
    'DUPLICATE_TIMESHEET_SNAPSHOT',
    'MISSING_TIMESHEET_SNAPSHOT',
    'DRAFT_SCOPE_SELECTED_LINES_MISSING',
    'MISSING_SELECTED_PREVIEW_ROW_ITEM',
    'CANDIDATE_SCOPE_TOTAL_MISMATCH',
    'ALLOCATION_ITEM_AMOUNT_MISMATCH',
    'PAY_CHANNEL_SCOPE_TOTAL_MISMATCH'
  ] LOOP
    IF pg_catalog.strpos(v_definition, quote_literal(v_check_code) || '::text AS check_code') = 0 THEN
      RAISE EXCEPTION 'BANKING_PAY_DRAFT_INTEGRITY_CHECK_MISSING: %', v_check_code;
    END IF;
  END LOOP;
END;
$verification$;
