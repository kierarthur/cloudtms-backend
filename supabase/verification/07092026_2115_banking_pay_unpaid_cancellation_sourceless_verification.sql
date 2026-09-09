-- Rollback-contained release verification for source-less unpaid cancellation.
-- This file is read-only apart from the outer transaction and creates no
-- payment, provider, settlement, remittance, mail or application side effect.

\set ON_ERROR_STOP on

begin;

DO $verification$
DECLARE
  v_row record;
  v_oid oid;
  v_definition text;
  v_index_definition text;
  v_no_transfer_branch_start integer := 0;
  v_terminal_no_money_branch_start integer := 0;
  v_advisory_branch_end integer := 0;
  v_nonterminal_request_count integer := 0;
  v_nonterminal_operation_count integer := 0;
BEGIN
  FOR v_row IN
    SELECT *
    FROM (VALUES
      ('public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text)', 'v', 'u', ARRAY['search_path=public']::text[]),
      ('public._pay_payment_movement_classify(uuid,jsonb)', 'v', 'u', ARRAY['search_path=public']::text[]),
      ('public.pay_payment_correction_plan(uuid,jsonb,uuid,text)', 'v', 'u', ARRAY['search_path=public']::text[]),
      ('public.pay_pre_bank_cancel_apply_work_item(uuid,uuid)', 'v', 'u', ARRAY['search_path=pg_catalog, private, extensions, pg_temp','statement_timeout=6000ms','lock_timeout=1000ms']::text[]),
      ('public.pay_no_money_unwind_apply_work_item(uuid,uuid)', 'v', 'u', ARRAY['search_path=pg_catalog, private, extensions, pg_temp','statement_timeout=6000ms','lock_timeout=1000ms']::text[]),
      ('public.banking_alerts_active_for_user(uuid,text,uuid,boolean,integer,text)', 'v', 'u', ARRAY['search_path=public']::text[]),
      ('public.banking_alert_preferences_get(uuid)', 's', 'u', ARRAY['search_path=public']::text[]),
      ('public.banking_alert_preferences_update(uuid,jsonb)', 'v', 'u', ARRAY['search_path=public']::text[]),
      ('public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid)', 'v', 'u', ARRAY['search_path=pg_catalog, private, extensions, pg_temp','statement_timeout=6000ms','lock_timeout=1000ms']::text[]),
      ('public.pay_payment_correction_expand_work(uuid,uuid)', 'v', 'u', ARRAY['search_path=pg_catalog, private, extensions, pg_temp','statement_timeout=6000ms','lock_timeout=1000ms']::text[]),
      ('public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer)', 's', 'r', ARRAY['search_path=pg_catalog, private, extensions, pg_temp','statement_timeout=5000ms']::text[]),
      ('public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb)', 's', 'r', ARRAY['search_path=pg_catalog, private, extensions, pg_temp','statement_timeout=5000ms']::text[])
    ) AS expected(identity, volatility, parallel_mode, proconfig)
  LOOP
    v_oid := pg_catalog.to_regprocedure(v_row.identity);
    IF v_oid IS NULL THEN
      RAISE EXCEPTION 'SOURCELESS_CANCELLATION_FUNCTION_MISSING: %', v_row.identity;
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_proc AS function_row
      WHERE function_row.oid = v_oid
        AND function_row.prosecdef
        AND function_row.provolatile = v_row.volatility::"char"
        AND function_row.proparallel = v_row.parallel_mode::"char"
        AND coalesce(function_row.proconfig, ARRAY[]::text[]) @> v_row.proconfig
        AND v_row.proconfig @> coalesce(function_row.proconfig, ARRAY[]::text[])
        AND pg_catalog.pg_get_userbyid(function_row.proowner) IN ('postgres', current_user)
    ) THEN
      RAISE EXCEPTION 'SOURCELESS_CANCELLATION_FUNCTION_METADATA_MISMATCH: %', v_row.identity;
    END IF;

    IF NOT pg_catalog.has_function_privilege('service_role', v_oid, 'EXECUTE')
       OR pg_catalog.has_function_privilege('anon', v_oid, 'EXECUTE')
       OR pg_catalog.has_function_privilege('authenticated', v_oid, 'EXECUTE') THEN
      RAISE EXCEPTION 'SOURCELESS_CANCELLATION_FUNCTION_ACL_MISMATCH: %', v_row.identity;
    END IF;
  END LOOP;

  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_payment_correction_selection_prepare_chunk_v1(uuid,uuid,jsonb,integer,text,uuid)'::regprocedure
  ) INTO v_definition;
  IF pg_catalog.strpos(v_definition, 'v_fresh_candidate_scope_assignment := true') = 0
     OR pg_catalog.strpos(v_definition, 'v_communication_cleanup_contract_version := 2') = 0
     OR pg_catalog.strpos(v_definition, 'v_communication_cleanup_contract_raw IN (''1'', ''2'')') = 0
     OR pg_catalog.strpos(v_definition, 'IF v_communication_cleanup_contract_version = 2 THEN') = 0 THEN
    RAISE EXCEPTION 'SOURCELESS_CANCELLATION_FRESH_COMMUNICATION_V2_ASSIGNMENT_MISSING';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_payment_correction_expand_work(uuid,uuid)'::regprocedure
  ) INTO v_definition;
  IF pg_catalog.strpos(v_definition, 'v_communication_cleanup_contract_raw NOT IN (''1'', ''2'')') = 0
     OR pg_catalog.strpos(v_definition, 'IS DISTINCT FROM v_communication_cleanup_contract_version::text') = 0 THEN
    RAISE EXCEPTION 'SOURCELESS_CANCELLATION_COMMUNICATION_VERSION_PROPAGATION_MISSING';
  END IF;

  FOREACH v_oid IN ARRAY ARRAY[
    'public.pay_pre_bank_cancel_apply_work_item(uuid,uuid)'::regprocedure::oid,
    'public.pay_no_money_unwind_apply_work_item(uuid,uuid)'::regprocedure::oid
  ] LOOP
    SELECT pg_catalog.pg_get_functiondef(v_oid) INTO v_definition;
    IF pg_catalog.strpos(v_definition, 'v_communication_cleanup_contract_plan_raw NOT IN (''1'', ''2'')') = 0
       OR pg_catalog.strpos(v_definition, 'IF v_communication_cleanup_contract_version = 1 THEN') = 0
       OR pg_catalog.strpos(v_definition, 'financial_cancellation_independent_of_mail') = 0
       OR pg_catalog.strpos(v_definition, '''follow_up_cancellation_notice_permitted'', false') = 0
       OR pg_catalog.strpos(v_definition, '''follow_up_cancellation_notice_requires_proved_original_sent''') = 0
       OR pg_catalog.strpos(v_definition, 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION') = 0
       OR pg_catalog.strpos(v_definition, 'NEEDS_INVESTIGATION') = 0
       OR pg_catalog.strpos(v_definition, 'PAYMENT_CORRECTION_LEGACY_COMMUNICATION_SCOPE_RESTAGE_REQUIRED') > 0 THEN
      RAISE EXCEPTION 'SOURCELESS_CANCELLATION_APPLY_CONTRACT_MISMATCH: %', v_oid::regprocedure;
    END IF;
  END LOOP;

  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_payment_correction_plan(uuid,jsonb,uuid,text)'::regprocedure
  ) INTO v_definition;
  IF pg_catalog.strpos(v_definition, 'public.mail_outbox') = 0
     OR pg_catalog.strpos(v_definition, 'v_financial_cancellation_communication_v2 :=') = 0
     OR pg_catalog.strpos(v_definition, 'v_recommended_action = ''PRE_PROVIDER_CANCEL_AND_RECALCULATE''') = 0
     OR pg_catalog.strpos(v_definition, 'v_recommended_action = ''NO_MONEY_UNWIND_AND_RECALCULATE''') = 0
     OR pg_catalog.strpos(v_definition, 'resolved_full_payment_scope_json,is_full_scope') = 0
     OR pg_catalog.strpos(v_definition, 'COALESCE(v_draft_removal_requested, false) IS NOT TRUE') = 0
     OR pg_catalog.strpos(v_definition, '''follow_up_cancellation_notice_permitted'', false') = 0
     OR pg_catalog.strpos(v_definition, '''follow_up_cancellation_notice_requires_proved_original_sent'', true') = 0 THEN
    RAISE EXCEPTION 'SOURCELESS_CANCELLATION_PLAN_COMMUNICATION_ADMISSION_MISMATCH';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb)'::regprocedure
  ) INTO v_definition;
  v_no_transfer_branch_start := pg_catalog.strpos(v_definition, 'canonical_provider_state = ''NO_TRANSFER_EVIDENCE''');
  v_terminal_no_money_branch_start := pg_catalog.strpos(v_definition, 'canonical_provider_state = ''TERMINAL_NO_MONEY''');
  v_advisory_branch_end := pg_catalog.strpos(v_definition, ') AS source_less_manual_adjustment_advisory');
  IF v_no_transfer_branch_start = 0
     OR v_terminal_no_money_branch_start <= v_no_transfer_branch_start
     OR v_advisory_branch_end <= v_terminal_no_money_branch_start
     OR pg_catalog.strpos(
          pg_catalog.substring(
            v_definition,
            v_no_transfer_branch_start,
            v_terminal_no_money_branch_start - v_no_transfer_branch_start
          ),
          'provider_outage IS NOT TRUE'
        ) = 0
     OR pg_catalog.strpos(
          pg_catalog.substring(
            v_definition,
            v_terminal_no_money_branch_start,
            v_advisory_branch_end - v_terminal_no_money_branch_start
          ),
          'provider_outage'
        ) > 0 THEN
    RAISE EXCEPTION 'SOURCELESS_CANCELLATION_STATUS_ADMISSION_MISMATCH';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(
    'public.banking_alerts_active_for_user(uuid,text,uuid,boolean,integer,text)'::regprocedure
  ) INTO v_definition;
  IF pg_catalog.strpos(v_definition, 'MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED') = 0
     OR pg_catalog.strpos(v_definition, '''ACTION_REQUIRED''::text AS severity') = 0
     OR pg_catalog.strpos(v_definition, 'investigation_row.pay_batch_item_id AS entity_id') = 0
     OR pg_catalog.strpos(v_definition, '''pay_batch_id'', investigation_row.pay_batch_id::text') = 0 THEN
    RAISE EXCEPTION 'SOURCELESS_CANCELLATION_INVESTIGATION_ALERT_CONTRACT_MISSING';
  END IF;

  SELECT pg_catalog.pg_get_functiondef(
    'public.pay_payment_correction_integrity_check_v1(uuid,uuid,integer)'::regprocedure
  ) INTO v_definition;
  IF pg_catalog.strpos(v_definition, 'SELECTION_CONTRACT_MISMATCH') = 0
     OR pg_catalog.strpos(v_definition, 'SELECTION_PREPARATION_IN_PROGRESS') = 0
     OR pg_catalog.strpos(v_definition, 'v_communication_cleanup_contract_raw IN (''1'', ''2'')') = 0 THEN
    RAISE EXCEPTION 'SOURCELESS_CANCELLATION_INTEGRITY_VERSION_CONTRACT_MISSING';
  END IF;

  IF pg_catalog.to_regclass('public.pay_payment_correction_items_sourceless_investigation_batch_idx') IS NULL
     OR pg_catalog.to_regclass('public.pay_payment_correction_items_sourceless_investigation_item_idx') IS NULL THEN
    RAISE EXCEPTION 'SOURCELESS_CANCELLATION_INVESTIGATION_INDEX_MISSING';
  END IF;

  SELECT pg_catalog.pg_get_indexdef(
    'public.pay_payment_correction_items_sourceless_investigation_batch_idx'::regclass
  ) INTO v_index_definition;
  IF pg_catalog.strpos(v_index_definition, 'NEEDS_INVESTIGATION') = 0
     OR pg_catalog.strpos(v_index_definition, 'PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION') = 0 THEN
    RAISE EXCEPTION 'SOURCELESS_CANCELLATION_INVESTIGATION_INDEX_PREDICATE_MISMATCH';
  END IF;

  -- There is deliberately no old-request migration or compatibility execution.
  -- Final activation requires zero nonterminal correction requests/operations;
  -- terminal history remains immutable audit evidence.
  SELECT pg_catalog.count(*)::integer
  INTO v_nonterminal_request_count
  FROM public.pay_payment_correction_requests AS request_row
  WHERE request_row.status IS NULL
     OR pg_catalog.upper(pg_catalog.btrim(request_row.status)) NOT IN (
       'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED',
       'FAILED', 'REJECTED', 'CANCELLED'
     );

  SELECT pg_catalog.count(*)::integer
  INTO v_nonterminal_operation_count
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type = 'PAYMENT_CORRECTION'
    AND (
      operation_row.status IS NULL
      OR pg_catalog.upper(pg_catalog.btrim(operation_row.status)) NOT IN (
        'COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED'
      )
    );

  IF v_nonterminal_request_count <> 0 OR v_nonterminal_operation_count <> 0 THEN
    RAISE EXCEPTION 'PAYMENT_CORRECTION_NONTERMINAL_CUTOVER_BLOCKED: requests=%, operations=%',
      v_nonterminal_request_count,
      v_nonterminal_operation_count;
  END IF;
END;
$verification$;

rollback;
