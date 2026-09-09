\set ON_ERROR_STOP on

DO $verification$
DECLARE
  v_cancel_oid oid := pg_catalog.to_regprocedure(
    'public.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1(uuid,uuid,text,uuid,jsonb)'
  );
  v_publish_oid oid := pg_catalog.to_regprocedure(
    'private.pay_workbench_publish_certified_source_preview_v1(uuid,uuid,uuid,uuid,bigint,bigint,uuid,text,jsonb,jsonb,jsonb)'
  );
  v_cancel_definition text;
  v_publish_definition text;
BEGIN
  IF v_cancel_oid IS NULL OR v_publish_oid IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_CANCEL_RETURN_SELECTION_OWNER_MISSING'
      USING ERRCODE = '55000';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(v_cancel_oid)
  INTO v_cancel_definition;
  SELECT pg_catalog.pg_get_functiondef(v_publish_oid)
  INTO v_publish_definition;

  IF pg_catalog.strpos(v_cancel_definition, 'POST_CANCEL_RETURN_UNSELECTED') = 0
     OR pg_catalog.strpos(v_cancel_definition, 'banking_pay_workbench_selection_carry_registrations') = 0
     OR pg_catalog.strpos(v_cancel_definition, 'banking_pay_draft_frozen_constituent_payloads_v8') = 0
     OR pg_catalog.strpos(v_cancel_definition, 'PAYMENT_CANCEL_SELECTION_INTENT_IDENTITY_INCOMPLETE') = 0
     OR pg_catalog.strpos(v_cancel_definition, 'PAYMENT_CANCEL_CURRENT_SELECTION_INTENT_NOT_APPLIED') = 0 THEN
    RAISE EXCEPTION 'BANKING_PAY_CANCEL_RETURN_SELECTION_OWNER_INVALID'
      USING ERRCODE = '55000';
  END IF;

  IF pg_catalog.strpos(v_publish_definition, 'banking_pay_workbench_selection_carry_registrations') = 0
     OR pg_catalog.strpos(v_publish_definition, 'v_selected_count := pg_catalog.jsonb_array_length(v_selected_ids)') = 0 THEN
    RAISE EXCEPTION 'BANKING_PAY_CANCEL_RETURN_PUBLISHER_INVALID'
      USING ERRCODE = '55000';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_proc AS procedure_row
    WHERE procedure_row.oid = v_cancel_oid
      AND procedure_row.prosecdef
      AND procedure_row.provolatile = 'v'
      AND procedure_row.proparallel = 'u'
      AND procedure_row.proconfig = ARRAY['search_path=public, pg_temp']::text[]
      AND pg_catalog.pg_get_userbyid(procedure_row.proowner) = current_user
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_CANCEL_RETURN_OWNER_METADATA_INVALID'
      USING ERRCODE = '55000';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_proc AS procedure_row
    WHERE procedure_row.oid = v_publish_oid
      AND procedure_row.prosecdef IS FALSE
      AND procedure_row.provolatile = 'v'
      AND procedure_row.proparallel = 'u'
      AND procedure_row.proconfig = ARRAY['search_path=""']::text[]
      AND pg_catalog.pg_get_userbyid(procedure_row.proowner) = current_user
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_CANCEL_RETURN_PUBLISHER_METADATA_INVALID'
      USING ERRCODE = '55000';
  END IF;

  IF pg_catalog.has_function_privilege('anon', v_cancel_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('authenticated', v_cancel_oid, 'EXECUTE')
     OR NOT pg_catalog.has_function_privilege('service_role', v_cancel_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('anon', v_publish_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('authenticated', v_publish_oid, 'EXECUTE')
     OR pg_catalog.has_function_privilege('service_role', v_publish_oid, 'EXECUTE') THEN
    RAISE EXCEPTION 'BANKING_PAY_CANCEL_RETURN_OWNER_ACL_INVALID'
      USING ERRCODE = '42501';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_class AS index_class
    JOIN pg_catalog.pg_namespace AS index_namespace
      ON index_namespace.oid = index_class.relnamespace
    JOIN pg_catalog.pg_index AS index_catalog
      ON index_catalog.indexrelid = index_class.oid
    WHERE index_namespace.nspname = 'private'
      AND index_class.relname = 'banking_pay_draft_frozen_scopes_v8_batch_candidate_idx'
      AND index_catalog.indisvalid
      AND index_catalog.indisready
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_CANCEL_RETURN_SCOPE_INDEX_MISSING'
      USING ERRCODE = '55000';
  END IF;
END;
$verification$;

SELECT 'BANKING_PAY_CANCEL_RETURN_SELECTION_INTENT_VERIFIED' AS verification_result;
