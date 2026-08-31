\set ON_ERROR_STOP on

-- Read-only installed-state proof for the authority closure used by Banking
-- Pay modal v2.  The first capability-off release stopped after an historical
-- replay had replaced eight current routine bodies.  This verifier proves
-- those bodies, their service-only exposure and the unchanged Draft owner.
DO $verification$
DECLARE
  v_expected record;
  v_actual_sha text;
  v_identity text;
  v_failures text[] := ARRAY[]::text[];
BEGIN
  FOR v_expected IN
    SELECT *
    FROM (VALUES
      ('public.bulk_authorise_dataset_v1(jsonb)', '930d55e60b1599fcdba40ab7b5308ba5991a666f7a92b23f39d8c33a481af5e3'),
      ('public.bulk_process_dataset_v1(jsonb)', 'b8faea3c39aed9d29108bb0dbb9ba8372c50cfff3733796d970b66814020a648'),
      ('public.bulk_timesheet_row_patch_v1(jsonb)', 'adc4f93bb1ef1186bdbb25438566da8f6548a305d61dc229505c075e69128af6'),
      ('public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb)', '89543b82378468b1ae43534f5a4b1a200ffc60ffbef76196398b7f7d6521792f'),
      ('public.pay_workbench_mark_finance_case_dirty()', '50962ed4c2a7acdbf2a9e38d741d5cb017d56bb1501a92b76b8c980837cd7f08'),
      ('public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb)', 'afff514075f85f88642783e6b72db24b64e922b4112274473f67e33c92694d79'),
      ('public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text)', '09e28665f4fde5fac02310592c53de7b1cf0336c01a7cec5b182d8e1346666d8'),
      ('public.timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamptz)', '779097a7e535c9d6e0c44199644034d0f8f0ad195dbbef2c11b6af4b7f03d541'),
      ('public.pay_workbench_prepare_draft(uuid,uuid,jsonb,text,text,boolean,boolean,uuid,timestamptz,uuid,boolean,boolean)', '5a879bc899cff1b1f08a7da0f4cfc1705ef9d3065697f22a20c9b8609ffedfb3')
    ) AS expected(identity, sha256)
  LOOP
    IF pg_catalog.to_regprocedure(v_expected.identity) IS NULL THEN
      v_failures := pg_catalog.array_append(v_failures, v_expected.identity || ':missing');
      CONTINUE;
    END IF;

    SELECT pg_catalog.encode(
      extensions.digest(
        pg_catalog.convert_to(
          pg_catalog.replace(
            pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(v_expected.identity)),
            E'\r\n',
            E'\n'
          ),
          'UTF8'
        ),
        'sha256'
      ),
      'hex'
    )
    INTO v_actual_sha;

    IF v_actual_sha IS DISTINCT FROM v_expected.sha256 THEN
      v_failures := pg_catalog.array_append(
        v_failures,
        v_expected.identity || ':definition:' || COALESCE(v_actual_sha, 'null')
      );
    END IF;
  END LOOP;

  FOREACH v_identity IN ARRAY ARRAY[
    'public._pay_active_settled_components(uuid[])',
    'public.bulk_authorise_dataset_v1(jsonb)',
    'public.bulk_authorise_row_context_v1(jsonb)',
    'public.bulk_process_dataset_v1(jsonb)',
    'public.bulk_process_row_context_v1(jsonb)',
    'public.bulk_timesheet_row_patch_v1(jsonb)',
    'public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)',
    'public.pay_preview_candidate_build_finance_case_baseline(jsonb,uuid)',
    'public.pay_timesheet_summary_pay_state_refresh_trigger()',
    'public.pay_workbench_contract_client_dirty_fanout_chunk(uuid,jsonb,integer)',
    'public.pay_workbench_dirty_apply_jobs_chunk(integer,timestamptz,uuid,uuid,text,integer)',
    'public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb)',
    'public.pay_workbench_enqueue_stage_continuation(uuid,uuid,text,jsonb,uuid,jsonb,uuid,text,integer,integer)',
    'public.pay_workbench_repair_invalid_source_build_poison(uuid,uuid,integer,timestamptz,text)',
    'public.pay_workbench_session_clear_case_resolution(uuid,uuid,jsonb)',
    'public.pay_workbench_session_clone_eligibility_v1(uuid,uuid,uuid,jsonb)',
    'public.pay_workbench_worker_drain_chunk(integer,timestamptz,uuid,uuid,text[],text,integer)',
    'public.timesheet_authorise_bulk_atomic(jsonb,uuid,timestamptz)',
    'public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamptz,text)',
    'public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text)',
    'public.timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean)',
    'public.timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamptz)',
    'public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid,uuid,jsonb)',
    'public.pay_workbench_session_get_action_required_detail_v1(uuid,jsonb,uuid,text,text,integer)',
    'public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text)',
    'public.pay_workbench_session_get_blocked_detail_v1(uuid,jsonb,uuid,text,text,integer)',
    'public.pay_workbench_session_get_blocked_page_v1(uuid,jsonb,uuid,text,text,text,integer,text)',
    'public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer)',
    'public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer)',
    'public.pay_workbench_session_get_selected_ready_timesheets_v1(uuid,uuid,jsonb,uuid,text)',
    'public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb)',
    'public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text)',
    'public.pay_workbench_session_set_ready_group_v1(uuid,uuid,jsonb,uuid,text,text,boolean,uuid,text,jsonb)',
    'public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb)'
  ]
  LOOP
    IF pg_catalog.has_function_privilege('anon', v_identity, 'EXECUTE')
       OR pg_catalog.has_function_privilege('authenticated', v_identity, 'EXECUTE')
       OR NOT pg_catalog.has_function_privilege('service_role', v_identity, 'EXECUTE') THEN
      v_failures := pg_catalog.array_append(v_failures, v_identity || ':acl');
    END IF;
  END LOOP;

  IF COALESCE(pg_catalog.array_length(v_failures, 1), 0) <> 0 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'BANKING_PAY_RELEASE_AUTHORITY_REPAIR_VERIFICATION_FAILED',
      DETAIL = pg_catalog.to_jsonb(v_failures)::text;
  END IF;
END;
$verification$;
