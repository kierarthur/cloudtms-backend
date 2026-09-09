\set ON_ERROR_STOP on

DO $verification$
DECLARE
  v_process_oid oid := pg_catalog.to_regprocedure(
    'public.pay_payment_correction_process_chunk(uuid,integer,text,uuid)'
  );
  v_process_definition text;
  v_expected_gate text :=
    'ELSIF v_requested_action IN (''DRAFT_CANCEL'',''PRE_BANK_CANCEL'',''CANCEL_PAYMENT'',''NO_MONEY_RELEASE'',''NO_MONEY_UNWIND'') THEN';
  v_gate_count integer;
BEGIN
  IF v_process_oid IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_NO_MONEY_WORKBENCH_RETURN_OWNER_MISSING'
      USING ERRCODE = '55000';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(v_process_oid)
  INTO v_process_definition;

  v_gate_count := (
    pg_catalog.length(v_process_definition)
    - pg_catalog.length(pg_catalog.replace(v_process_definition, v_expected_gate, ''))
  ) / pg_catalog.length(v_expected_gate);

  IF v_gate_count IS DISTINCT FROM 1
     OR pg_catalog.strpos(
       v_process_definition,
       'v_correction_kind := CASE WHEN v_action IN (''NO_MONEY_RELEASE'', ''NO_MONEY_UNWIND'')'
     ) > 0
     OR pg_catalog.strpos(
       v_process_definition,
       '''NO_MONEY_RELEASE'',''NO_MONEY_UNWIND'',''SETTLED_REVERSAL'''
     ) > 0 THEN
    RAISE EXCEPTION 'BANKING_PAY_NO_MONEY_WORKBENCH_RETURN_GATE_INVALID'
      USING ERRCODE = '55000';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_proc AS procedure_row
    WHERE procedure_row.oid = v_process_oid
      AND procedure_row.prosecdef
      AND procedure_row.provolatile = 'v'
      AND procedure_row.proparallel = 'u'
      AND procedure_row.proconfig = ARRAY[
        'search_path=pg_catalog, private, extensions, pg_temp',
        'statement_timeout=6000ms',
        'lock_timeout=1000ms'
      ]::text[]
      AND pg_catalog.pg_get_userbyid(procedure_row.proowner) = current_user
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_NO_MONEY_WORKBENCH_RETURN_METADATA_INVALID'
      USING ERRCODE = '55000';
  END IF;

  IF pg_catalog.has_function_privilege('anon', v_process_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('authenticated', v_process_oid, 'EXECUTE')
     OR NOT pg_catalog.has_function_privilege('service_role', v_process_oid, 'EXECUTE') THEN
    RAISE EXCEPTION 'BANKING_PAY_NO_MONEY_WORKBENCH_RETURN_ACL_INVALID'
      USING ERRCODE = '42501';
  END IF;
END;
$verification$;

SELECT 'BANKING_PAY_NO_MONEY_WORKBENCH_RETURN_VERIFIED' AS verification_result;
