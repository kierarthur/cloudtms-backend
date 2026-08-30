-- Supabase Postgres 17 currently preloads plpgsql_check 2.7. During deeply
-- nested, bounded correction-chain and Banking Pay execution, that library can
-- corrupt its own pldbgapi2 statement stack and abort otherwise valid SQL.
--
-- plpgsql_check.mode=disabled is not sufficient on this build: the constant
-- tracer and cursor-leak tracker remain enabled independently. Apply the full
-- runtime-disable set to every function in the affected call chain so the fix
-- takes effect even on an existing pooled database connection.
--
-- This repeatable changes no function body, economic value, component identity,
-- payment state, frozen batch artefact, or Policy X rule. Static source tests
-- and PostgreSQL function compilation remain the validation authorities.

DO $block$
DECLARE
  v_function regprocedure;
  v_extension_present boolean;
  v_setting_count integer;
  v_set_privilege_count integer;
BEGIN
  SELECT EXISTS (
           SELECT 1 FROM pg_catalog.pg_extension WHERE extname='plpgsql_check'
         ),
         (
           SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_settings
           WHERE name IN (
             'plpgsql_check.mode',
             'plpgsql_check.profiler',
             'plpgsql_check.tracer',
             'plpgsql_check.constants_tracing',
             'plpgsql_check.cursors_leaks',
             'plpgsql_check.strict_cursors_leaks',
             'plpgsql_check.fatal_errors'
           )
         ),
         (
           SELECT pg_catalog.count(*)
           FROM (VALUES
             ('plpgsql_check.mode'),
             ('plpgsql_check.profiler'),
             ('plpgsql_check.tracer'),
             ('plpgsql_check.constants_tracing'),
             ('plpgsql_check.cursors_leaks'),
             ('plpgsql_check.strict_cursors_leaks'),
             ('plpgsql_check.fatal_errors')
           ) AS parameter(name)
           WHERE pg_catalog.has_parameter_privilege(current_user,parameter.name,'SET')
         )
  INTO v_extension_present,v_setting_count,v_set_privilege_count;

  IF NOT v_extension_present
     AND v_setting_count=0
     AND v_set_privilege_count=0
  THEN
    RAISE NOTICE 'PLPGSQL_CHECK_RUNTIME_CONFIGURATION_NOT_AVAILABLE_ON_PROVIDER';
    RETURN;
  END IF;

  IF NOT v_extension_present
     OR v_setting_count<>7
     OR v_set_privilege_count<>7
  THEN
    RAISE EXCEPTION USING
      ERRCODE='check_violation',
      MESSAGE=pg_catalog.format(
        'PLPGSQL_CHECK_RUNTIME_CONFIGURATION_PARTIAL_STATE:extension=%s settings=%s privileges=%s',
        v_extension_present,v_setting_count,v_set_privilege_count
      );
  END IF;

  FOREACH v_function IN ARRAY ARRAY[
    'public.pay_correction_chain_residual_v1(uuid,uuid,text,uuid,uuid,integer)'::regprocedure,
    'public._ctms_correction_policy_leg_read_v1(uuid)'::regprocedure,
    'public._pay_batch_item_source_reservation_amount_ex_vat(uuid)'::regprocedure,
    'public._pay_policy_x_resolve_post_draft_economic_key(uuid,uuid,uuid,text,text,text,jsonb,jsonb,jsonb,jsonb)'::regprocedure,
    'public._ctms_import_correction_classify_v1(uuid)'::regprocedure,
    'public._ctms_assert_payload_corrections_fresh_v1(jsonb,text)'::regprocedure,
    'public.timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer)'::regprocedure,
    'public._ctms_candidate_correction_residuals_v1(uuid,uuid,uuid,text)'::regprocedure,
    'public._ctms_rewrite_source_build_correction_negative_components_v1(uuid,uuid,uuid[])'::regprocedure,
    'public._ctms_rewrite_sync_authoritative_correction_negative_components_v1(uuid,uuid,uuid[])'::regprocedure,
    'public._ctms_materialise_candidate_correction_residuals_v1(uuid,uuid,uuid,timestamptz)'::regprocedure,
    'public._ctms_rewrite_sync_correction_cases_v1(uuid,uuid[],uuid[])'::regprocedure,
    'public.pay_workbench_candidate_source_build_chunk(uuid,uuid,jsonb,jsonb,integer)'::regprocedure,
    'public.pay_sync_overpayments_from_preview(date,date,uuid,text,uuid[],jsonb,uuid,uuid[],uuid[])'::regprocedure,
    'public.pay_preview_candidate_collect_scope(jsonb,uuid,jsonb,integer)'::regprocedure,
    'public.pay_preview_candidate_build_timesheet_snapshots(jsonb,uuid,jsonb,integer,text)'::regprocedure,
    'public.pay_preview_candidate_build_payee_baseline(jsonb,uuid)'::regprocedure,
    'public.pay_preview_candidate_build_finance_case_baseline(jsonb,uuid)'::regprocedure,
    'public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)'::regprocedure,
    'public.pay_preview_candidate_build_case_component_rows(jsonb,uuid)'::regprocedure,
    'public.pay_preview_candidate_build_entitlement_rows(jsonb,uuid)'::regprocedure,
    'public.pay_preview_candidate_build_summary_fragment(jsonb,uuid)'::regprocedure,
    'public.banking_pay_hot_path_budget_apply(text)'::regprocedure,
    'public._pay_week_start_monday(date)'::regprocedure,
    'public.pay_paye_guardrails(date,uuid,uuid)'::regprocedure,
    'public.pay_preview_build_context(date,date,uuid,uuid,uuid,jsonb)'::regprocedure,
    'public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid,uuid)'::regprocedure,
    'public.pay_workbench_session_recompute_progress_counters(uuid,boolean,text,boolean)'::regprocedure,
    'public.pay_workbench_session_compact_progress_json(jsonb,boolean)'::regprocedure,
    'public.pay_workbench_preview_section_from_line_json(jsonb)'::regprocedure,
    'public.pay_workbench_preview_line_economic_key(jsonb,uuid,text,jsonb,text,text)'::regprocedure,
    'public.pay_workbench_preview_line_contract_ok(jsonb,jsonb,text)'::regprocedure,
    'public.pay_workbench_worker_drain_chunk(integer,timestamptz,uuid,uuid,text[],text,integer)'::regprocedure,
    'public.pay_workbench_worker_drain_chunk_revalidated_v1(integer,timestamptz,uuid,uuid,text[],text,integer)'::regprocedure,
    'public.pay_finance_case_apply_taxable_channel_restructure(uuid,uuid,text,text,integer,numeric,numeric,date,text)'::regprocedure,
    'public.tsfin_write_snapshots_and_complete(jsonb)'::regprocedure,
    'public.banking_alerts_refresh_for_user(uuid,text,integer)'::regprocedure,
    'public.pay_workbench_session_apply_case_resolution(uuid,uuid,jsonb)'::regprocedure,
    'public.pay_export_bank_csv(uuid,text)'::regprocedure,
    'public._pay_batch_bank_payment_projection_rows(uuid,text)'::regprocedure,
    'public._pay_batch_validate_freshness_base_v1(uuid,uuid,boolean)'::regprocedure,
    'public.pay_batch_validate_freshness(uuid,uuid,boolean)'::regprocedure,
    'public.pay_batch_freshness_scope_seed(uuid,uuid,jsonb,integer)'::regprocedure,
    'public.pay_batch_validate_freshness_chunk(uuid,jsonb,integer)'::regprocedure,
    'public.pay_batch_validate_freshness_chunk(uuid,uuid,uuid,uuid,integer)'::regprocedure,
    'public.pay_batch_freshness_result_get(uuid,uuid)'::regprocedure,
    'public.pay_batch_freshness_result_get(uuid,uuid,uuid)'::regprocedure,
    'public.pay_settle_manual_confirm(uuid,text,text,date,uuid,text,uuid,boolean,text,boolean,uuid,jsonb)'::regprocedure
  ]
  LOOP
    EXECUTE format(
      'ALTER FUNCTION %s SET plpgsql_check.mode TO %L',
      v_function,
      'disabled'
    );
    EXECUTE format(
      'ALTER FUNCTION %s SET plpgsql_check.profiler TO %L',
      v_function,
      'off'
    );
    EXECUTE format(
      'ALTER FUNCTION %s SET plpgsql_check.tracer TO %L',
      v_function,
      'off'
    );
    EXECUTE format(
      'ALTER FUNCTION %s SET plpgsql_check.constants_tracing TO %L',
      v_function,
      'off'
    );
    EXECUTE format(
      'ALTER FUNCTION %s SET plpgsql_check.cursors_leaks TO %L',
      v_function,
      'off'
    );
    EXECUTE format(
      'ALTER FUNCTION %s SET plpgsql_check.strict_cursors_leaks TO %L',
      v_function,
      'off'
    );
    EXECUTE format(
      'ALTER FUNCTION %s SET plpgsql_check.fatal_errors TO %L',
      v_function,
      'off'
    );
  END LOOP;
END;
$block$;
