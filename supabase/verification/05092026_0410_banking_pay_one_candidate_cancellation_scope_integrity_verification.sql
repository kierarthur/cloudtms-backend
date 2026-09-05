\set ON_ERROR_STOP on

-- Banking Pay single-Candidate cancellation integrity and completion.
-- This catalogue verifier proves that the reviewed orchestration owners are
-- installed with their original budgets/security and that single-Candidate
-- cancellation cannot invoke the whole-Draft metadata owner. It performs no
-- business, provider, payment, settlement or remittance mutation.
DO $verification$
DECLARE
  v_selection_signature regprocedure :=
    'public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid)'::regprocedure;
  v_process_signature regprocedure :=
    'public.pay_payment_correction_process_chunk(uuid,integer,text,uuid)'::regprocedure;
  v_selection_definition text;
  v_process_definition text;
  v_selection_owner name;
  v_process_owner name;
  v_selection_security_definer boolean;
  v_process_security_definer boolean;
  v_selection_volatility "char";
  v_process_volatility "char";
  v_selection_parallel "char";
  v_process_parallel "char";
  v_selection_config text[];
  v_process_config text[];
BEGIN
  SELECT pg_get_functiondef(routine.oid), owner_role.rolname,
         routine.prosecdef, routine.provolatile, routine.proparallel,
         routine.proconfig
  INTO STRICT v_selection_definition, v_selection_owner,
              v_selection_security_definer, v_selection_volatility,
              v_selection_parallel, v_selection_config
  FROM pg_proc AS routine
  JOIN pg_roles AS owner_role ON owner_role.oid = routine.proowner
  WHERE routine.oid = v_selection_signature;

  SELECT pg_get_functiondef(routine.oid), owner_role.rolname,
         routine.prosecdef, routine.provolatile, routine.proparallel,
         routine.proconfig
  INTO STRICT v_process_definition, v_process_owner,
              v_process_security_definer, v_process_volatility,
              v_process_parallel, v_process_config
  FROM pg_proc AS routine
  JOIN pg_roles AS owner_role ON owner_role.oid = routine.proowner
  WHERE routine.oid = v_process_signature;

  IF position('same complete frozen batch universe' IN v_selection_definition) = 0
     OR position('IF v_mode = ''EXPLICIT'' AND NOT v_candidate_explicitly_selected THEN' IN v_selection_definition) = 0
     OR position('candidate_hash'', v_candidate_audit_hash' IN v_selection_definition) = 0
     OR position('remaining_candidate.id > v_scan_last_candidate_id' IN v_selection_definition) = 0
     OR position('canonical_explicit_candidate_tokens' IN v_selection_definition) = 0 THEN
    RAISE EXCEPTION 'BANKING_PAY_ONE_CANDIDATE_SELECTION_INTEGRITY_DEFINITION_DRIFT';
  END IF;

  IF position('PERFORM public.pay_payment_cancel_finalise_metadata_v1(' IN v_process_definition) = 0
     OR position('v_requested_action = ''DRAFT_CANCEL''' IN v_process_definition) = 0
     OR position('cancelled_batch.status = ''CANCELLED''' IN v_process_definition) = 0
     OR position('v_refresh_has_more IS NOT TRUE' IN v_process_definition) = 0 THEN
    RAISE EXCEPTION 'BANKING_PAY_CANCELLATION_COMPLETION_SCOPE_DEFINITION_DRIFT';
  END IF;

  IF v_selection_owner <> current_user
     OR v_process_owner <> current_user
     OR v_selection_security_definer IS NOT TRUE
     OR v_process_security_definer IS NOT TRUE
     OR v_selection_volatility <> 'v'
     OR v_process_volatility <> 'v'
     OR v_selection_parallel <> 'u'
     OR v_process_parallel <> 'u'
     OR v_selection_config IS DISTINCT FROM ARRAY[
          'search_path=pg_catalog, private, extensions, pg_temp',
          'statement_timeout=6000ms',
          'lock_timeout=1000ms'
        ]::text[]
     OR v_process_config IS DISTINCT FROM ARRAY[
          'search_path=pg_catalog, private, extensions, pg_temp',
          'statement_timeout=6000ms',
          'lock_timeout=1000ms'
        ]::text[] THEN
    RAISE EXCEPTION 'BANKING_PAY_ONE_CANDIDATE_CANCELLATION_METADATA_DRIFT';
  END IF;

  IF has_function_privilege('anon', v_selection_signature, 'EXECUTE')
     OR has_function_privilege('authenticated', v_selection_signature, 'EXECUTE')
     OR NOT has_function_privilege('service_role', v_selection_signature, 'EXECUTE')
     OR has_function_privilege('anon', v_process_signature, 'EXECUTE')
     OR has_function_privilege('authenticated', v_process_signature, 'EXECUTE')
     OR NOT has_function_privilege('service_role', v_process_signature, 'EXECUTE') THEN
    RAISE EXCEPTION 'BANKING_PAY_ONE_CANDIDATE_CANCELLATION_ACL_DRIFT';
  END IF;

  IF v_selection_definition ~* 'pg_catalog\.(coalesce|nullif|least|greatest)[[:space:]]*\('
     OR v_process_definition ~* 'pg_catalog\.(coalesce|nullif|least|greatest)[[:space:]]*\('
     OR v_selection_definition ~* '(^|[^[:alnum:]_])(min|max)[[:space:]]*\([[:space:]]*[^)]*uuid'
     OR v_process_definition ~* '(^|[^[:alnum:]_])(min|max)[[:space:]]*\([[:space:]]*[^)]*uuid' THEN
    RAISE EXCEPTION 'BANKING_PAY_ONE_CANDIDATE_CANCELLATION_UNSAFE_POSTGRES_CONSTRUCT';
  END IF;
END;
$verification$;
