\set ON_ERROR_STOP on

-- Read-only installed-definition proof for the failed-payment/no-money result
-- envelope. Runtime authority is Miget TEST; the `supabase` directory name is
-- historical. This verifier performs no business or financial mutation.
DO $verification$
DECLARE
  v_oid oid := pg_catalog.to_regprocedure(
    'public.pay_no_money_unwind_apply_work_item(uuid,uuid)'
  );
  v_owner text;
  v_security_definer boolean;
  v_volatility "char";
  v_parallel "char";
  v_config text[];
  v_public_execute boolean;
  v_definition text;
  v_result_section text;
  v_result_start integer;
  v_result_end integer;
  v_result_join_count integer;
  v_required_field text;
BEGIN
  IF v_oid IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_NO_MONEY_UNWIND_OWNER_MISSING';
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
      ) AS acl_row
      WHERE acl_row.grantee = 0
        AND acl_row.privilege_type = 'EXECUTE'
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
     OR v_security_definer IS DISTINCT FROM true
     OR v_volatility <> 'v'
     OR v_parallel <> 'u'
     OR v_config IS DISTINCT FROM ARRAY[
          'search_path=pg_catalog, private, extensions, pg_temp',
          'statement_timeout=6000ms',
          'lock_timeout=1000ms'
        ]::text[]
     OR v_public_execute
     OR pg_catalog.has_function_privilege('anon', v_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('authenticated', v_oid, 'EXECUTE')
     OR NOT pg_catalog.has_function_privilege('service_role', v_oid, 'EXECUTE') THEN
    RAISE EXCEPTION 'BANKING_PAY_NO_MONEY_UNWIND_METADATA_OR_ACL_DRIFT';
  END IF;

  v_result_start := pg_catalog.strpos(v_definition, 'v_result := jsonb_build_object(');
  v_result_end := v_result_start - 1 + pg_catalog.strpos(
    pg_catalog.substr(v_definition, v_result_start),
    'UPDATE public.pay_payment_correction_work_items'
  );

  IF v_result_start = 0 OR v_result_end <= v_result_start THEN
    RAISE EXCEPTION 'BANKING_PAY_NO_MONEY_UNWIND_RESULT_ENVELOPE_MISSING';
  END IF;

  v_result_section := pg_catalog.substr(
    v_definition,
    v_result_start,
    v_result_end - v_result_start
  );
  v_result_join_count := (
    pg_catalog.length(v_result_section)
    - pg_catalog.length(pg_catalog.replace(v_result_section, ') || jsonb_build_object(', ''))
  ) / pg_catalog.length(') || jsonb_build_object(');

  IF v_result_section IS NULL
     OR v_result_join_count <> 2
     OR pg_catalog.strpos(
          v_result_section,
          '''blockers'', ''[]''::jsonb' || chr(10) || '  ) || jsonb_build_object('
        ) = 0
     OR pg_catalog.strpos(
          v_result_section,
          '''rail_state_summary'', COALESCE(v_rail_state_summary_json, ''{}''::jsonb)' || chr(10) || '  ) || jsonb_build_object('
        ) = 0 THEN
    RAISE EXCEPTION 'BANKING_PAY_NO_MONEY_UNWIND_RESULT_ENVELOPE_NOT_BOUNDED';
  END IF;

  FOREACH v_required_field IN ARRAY ARRAY[
    'selected_candidate_count',
    'voided_item_count',
    'released_reservation_count',
    'restored_component_count',
    'active_batch_amount_inc_vat_after',
    'classification_result',
    'provider_evidence_result',
    'rail_state_summary',
    'workbench_refresh'
  ] LOOP
    IF pg_catalog.strpos(v_result_section, quote_literal(v_required_field)) = 0 THEN
      RAISE EXCEPTION 'BANKING_PAY_NO_MONEY_UNWIND_RESULT_FIELD_MISSING: %', v_required_field;
    END IF;
  END LOOP;
END;
$verification$;
