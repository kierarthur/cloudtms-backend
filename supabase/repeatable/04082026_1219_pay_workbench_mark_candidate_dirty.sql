-- Banking Pay bounded-scope V1.2.5: installed TEST authority plus deterministic finance-effect attestation.
CREATE OR REPLACE FUNCTION public.pay_workbench_mark_candidate_dirty()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_started_at timestamptz := clock_timestamp();
  v_now timestamptz := clock_timestamp();
  v_trigger_table text := lower(TG_TABLE_NAME);
  v_new_row jsonb := '{}'::jsonb;
  v_old_row jsonb := '{}'::jsonb;
  v_previous_tsfin_row jsonb := '{}'::jsonb;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_id uuid;
  v_old_timesheet_id uuid := NULL::uuid;
  v_new_timesheet_id uuid := NULL::uuid;
  v_old_contract_id uuid := NULL::uuid;
  v_new_contract_id uuid := NULL::uuid;
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_old_payment_eligible boolean := false;
  v_new_payment_eligible boolean := false;
  v_authorise_boundary_changed boolean := false;
  v_authorise_boundary_is_authorise boolean := false;
  v_authorise_boundary_is_unauthorise boolean := false;
  v_archive_boundary_changed boolean := false;
  v_archive_boundary_is_archive boolean := false;
  v_archive_boundary_is_unarchive boolean := false;
  v_material_live_source_changed boolean := false;
  v_signal_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_signal_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_signal_batch_row record;
  v_signal_reason text := NULL::text;
  v_signal_result jsonb := '{}'::jsonb;
  v_tsfin_payability_state_changed boolean := false;
  v_timesheet_pay_state_settlement_changed boolean := false;
  v_timesheet_pay_state_summary_changed boolean := false;
  v_timesheet_pay_state_bookkeeping_ignored boolean := false;
  v_timesheet_pay_state_noop_ignored boolean := false;
  v_timesheet_pay_state_routing_reason text := NULL::text;
  v_tsfin_insert_authorise_like boolean := false;
  v_tsfin_insert_not_coalesced_reason text := NULL::text;
  v_tsfin_insert_coalesced_job_id uuid := NULL::uuid;
  v_tsfin_insert_coalesced_projection_run_id uuid := NULL::uuid;
  v_tsfin_insert_coalesced_session_id uuid := NULL::uuid;
  v_tsfin_insert_coalesced_source_change_seq bigint := NULL::bigint;
  v_lifecycle_context text := NULLIF(BTRIM(COALESCE(current_setting('cloudtms.lifecycle_mutation_context', true), '')), '');
  v_effective_lifecycle_context text := NULL::text;
  v_lifecycle_dedupe_keys text := '';
  v_lifecycle_dedupe_token text := '';
  v_explicit_banking_pay_action boolean := false;
  v_banking_pay_dirty_required boolean := false;
  v_ordinary_timesheet_edit_save_no_dirty boolean := false;
  v_entity_kind_old text := '';
  v_entity_kind_new text := '';
  v_umbrella_id_old uuid := NULL::uuid;
  v_umbrella_id_new uuid := NULL::uuid;
  v_scope_id text;
  v_reason text;
  v_payload_json jsonb := '{}'::jsonb;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_jobs_queued integer := 0;
  v_candidate_route_change boolean := false;
  v_route_source_method text := NULL::text;
  v_route_target_method text := NULL::text;
  v_route_operation_id text := NULLIF(BTRIM(COALESCE(current_setting('cloudtms.candidate_pay_method_change_operation_id', true), '')), '');
  v_route_actor_user_id text := NULLIF(BTRIM(COALESCE(current_setting('cloudtms.candidate_pay_method_change_actor_user_id', true), '')), '');
  v_route_reason text := NULLIF(BTRIM(COALESCE(current_setting('cloudtms.candidate_pay_method_change_reason', true), '')), '');
  v_route_scope_result jsonb := '{}'::jsonb;
  v_route_authoritative_sessions jsonb := '[]'::jsonb;
  v_route_replaced_source_session_ids jsonb := '[]'::jsonb;
  v_route_target_details jsonb := '[]'::jsonb;
  v_route_authorised_timesheet_ids jsonb := '[]'::jsonb;
  v_route_active_advance_timesheet_ids jsonb := '[]'::jsonb;
  v_route_retained_finance_timesheet_ids jsonb := '[]'::jsonb;
  v_route_authorised_timesheet_count integer := 0;
  v_route_active_advance_timesheet_count integer := 0;
  v_route_retained_finance_timesheet_count integer := 0;
  v_route_source_target_mismatch_count integer := 0;
  v_route_exact_scope boolean := false;
  v_route_coverage_complete boolean := false;
  v_route_targeted_scope_is_empty boolean := false;
  v_route_job_id uuid := NULL::uuid;
  v_route_job_status text := NULL::text;
  v_route_job_source_change_seq bigint := 0;
  v_route_dedupe_key text := NULL::text;
  v_internal_build_token uuid := NULL::uuid;
  v_internal_candidate_id uuid := NULL::uuid;
  v_internal_source_id uuid := NULL::uuid;
  v_internal_timesheet_id uuid := NULL::uuid;
  v_internal_logical_source_id uuid := NULL::uuid;
  v_internal_before_digest text := NULL::text;
  v_internal_after_digest text := NULL::text;
  v_expected_match_count integer := 0;
  v_delete_owner_candidate_id uuid := NULL::uuid;
  v_delete_context regclass := pg_catalog.to_regclass('pg_temp._bpay_candidate_delete_context_v1');
  v_delete_context_suppressed boolean := false;
  v_effect_capture_mode boolean := lower(COALESCE(current_setting('cloudtms.pay_workbench_effect_capture_mode',true),''))='capture';
BEGIN
  PERFORM public._temp_diag_log(
    'TEMP_TRIGGER_DIRTY_STAGE',
    'TEMP_BANKING_PAY_DIRTY',
    NULL::text,
    jsonb_build_object(
      'function_name', 'pay_workbench_mark_candidate_dirty',
      'stage', 'entry',
      'trigger_table', v_trigger_table,
      'trigger_op', TG_OP,
      'mutation_context', v_lifecycle_context,
      'queue_class', 'DIRTY_TRIGGER_PRIORITY',
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)
    )
  );

  IF TG_OP <> 'DELETE' THEN
    v_new_row := to_jsonb(NEW);
  END IF;
  IF TG_OP <> 'INSERT' THEN
    v_old_row := to_jsonb(OLD);
  END IF;


  IF TG_OP='DELETE' AND v_delete_context IS NOT NULL THEN
    IF v_trigger_table='candidates' AND NULLIF(v_old_row->>'id','')~*v_uuid_re THEN
      v_delete_owner_candidate_id:=(v_old_row->>'id')::uuid;
    ELSIF NULLIF(v_old_row->>'candidate_id','')~*v_uuid_re THEN
      v_delete_owner_candidate_id:=(v_old_row->>'candidate_id')::uuid;
    ELSIF NULLIF(COALESCE(v_old_row->>'timesheet_id',v_old_row->>'linked_timesheet_id'),'')~*v_uuid_re THEN
      SELECT COALESCE(financial.candidate_id,contract_row.candidate_id)
      INTO v_delete_owner_candidate_id
      FROM (SELECT COALESCE(NULLIF(v_old_row->>'timesheet_id','')::uuid,
             NULLIF(v_old_row->>'linked_timesheet_id','')::uuid) AS timesheet_id) owner_key
      LEFT JOIN public.timesheets_financials financial
        ON financial.timesheet_id=owner_key.timesheet_id AND financial.is_current
      LEFT JOIN public.timesheets timesheet_row ON timesheet_row.timesheet_id=owner_key.timesheet_id
      LEFT JOIN public.contracts contract_row ON contract_row.id=timesheet_row.contract_id
      LIMIT 1;
    ELSIF NULLIF(v_old_row->>'contract_id','')~*v_uuid_re THEN
      SELECT contract_row.candidate_id INTO v_delete_owner_candidate_id
      FROM public.contracts contract_row WHERE contract_row.id=(v_old_row->>'contract_id')::uuid;
    ELSIF NULLIF(v_old_row->>'finance_case_id','')~*v_uuid_re THEN
      SELECT finance_case.candidate_id INTO v_delete_owner_candidate_id
      FROM public.pay_advances finance_case WHERE finance_case.id=(v_old_row->>'finance_case_id')::uuid;
    ELSIF NULLIF(v_old_row->>'finance_component_id','')~*v_uuid_re THEN
      SELECT component.candidate_id INTO v_delete_owner_candidate_id
      FROM public.pay_finance_case_components component
      WHERE component.id=(v_old_row->>'finance_component_id')::uuid;
    END IF;
  END IF;

  IF TG_OP='DELETE' AND v_delete_context IS NOT NULL
     AND v_delete_owner_candidate_id IS NOT NULL THEN
    IF EXISTS(
         SELECT 1 FROM pg_catalog.pg_class relation
         WHERE relation.oid=v_delete_context
           AND relation.relowner=current_user::regrole::oid AND relation.relpersistence='t'
           AND relation.relnamespace=pg_catalog.pg_my_temp_schema()
       )
       AND (SELECT array_agg(attribute.attname||':'||pg_catalog.format_type(attribute.atttypid,attribute.atttypmod)
              ORDER BY attribute.attnum)
            FROM pg_catalog.pg_attribute attribute
            WHERE attribute.attrelid=v_delete_context
              AND attribute.attnum>0 AND NOT attribute.attisdropped)
         =ARRAY['candidate_id:uuid','delete_operation_id:uuid','candidate_lock_key:bigint',
           'backend_pid:integer','transaction_id:bigint','created_at_utc:timestamp with time zone',
           'suppress:boolean'] THEN
      -- The delete context is optional.  Keep its only row read behind dynamic
      -- SQL so an ordinary finance DELETE cannot fail during statement planning
      -- merely because candidate deletion did not create the temporary table.
      EXECUTE $delete_context$
        SELECT EXISTS(
          SELECT 1
          FROM pg_temp._bpay_candidate_delete_context_v1 context
          WHERE context.candidate_id=$1 AND context.suppress
            AND context.candidate_lock_key=pg_catalog.hashtextextended(
              public._pay_workbench_candidate_serial_key(context.candidate_id),24062027)
            AND context.backend_pid=pg_catalog.pg_backend_pid()
            AND context.transaction_id=pg_catalog.txid_current()
            AND (SELECT count(*) FROM pg_temp._bpay_candidate_delete_context_v1)=1
        )
      $delete_context$ INTO v_delete_context_suppressed USING v_delete_owner_candidate_id;
    END IF;
    IF v_delete_context_suppressed THEN
      RETURN OLD;
    END IF;
  END IF;

  -- The retained pay_advances dirty trigger is recreated as BEFORE ROW.  It
  -- declares the exact full transition before the row mutation and suppresses
  -- only an owner-controlled effect inside the current sealed build scope.
  IF v_trigger_table='pay_advances'
     AND to_regclass('pg_temp._bpay_wb_sync_context_v1') IS NOT NULL
     AND to_regclass('pg_temp._bpay_wb_expected_effects') IS NOT NULL
     AND COALESCE(current_setting('cloudtms.pay_workbench_overpayment_sync_token',true),'')
       ~* v_uuid_re THEN
    v_internal_build_token:=current_setting(
      'cloudtms.pay_workbench_overpayment_sync_token',true)::uuid;
    SELECT build_row.candidate_id INTO v_internal_candidate_id
    FROM pg_temp._bpay_wb_sync_context_v1 sync_context
       JOIN private.banking_pay_workbench_economic_builds build_row
         ON build_row.id=sync_context.build_id AND build_row.build_token=sync_context.build_token
       JOIN private.banking_pay_workbench_stage_attempts attempt
         ON attempt.id=sync_context.attempt_id AND attempt.attempt_nonce=sync_context.attempt_nonce
        AND attempt.build_id=build_row.id AND attempt.attempt_status='STARTED'
    WHERE build_row.status='RECONCILING' AND build_row.private_stage='RECONCILE_EXECUTE'
      AND build_row.build_token=v_internal_build_token;
    IF v_internal_candidate_id IS NOT NULL THEN
      IF TG_WHEN<>'BEFORE' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_CONFLICT' USING ERRCODE='23514';
      END IF;
      v_internal_source_id:=COALESCE(NULLIF(v_new_row->>'id','')::uuid,
        NULLIF(v_old_row->>'id','')::uuid);
      v_internal_timesheet_id:=COALESCE(NULLIF(v_new_row->>'linked_timesheet_id','')::uuid,
        NULLIF(v_old_row->>'linked_timesheet_id','')::uuid);
      v_internal_before_digest:=CASE WHEN TG_OP='INSERT' THEN NULL ELSE md5(
        private.pay_workbench_finance_effect_normalise_row_v1(
          v_trigger_table,TG_OP,v_old_row,v_old_row)::text) END;
      v_internal_after_digest:=CASE WHEN TG_OP='DELETE' THEN NULL ELSE md5(
        private.pay_workbench_finance_effect_normalise_row_v1(
          v_trigger_table,TG_OP,v_new_row,v_old_row)::text) END;
      IF v_internal_source_id IS NULL
         OR COALESCE(NULLIF(v_new_row->>'candidate_id','')::uuid,
              NULLIF(v_old_row->>'candidate_id','')::uuid)
            IS DISTINCT FROM v_internal_candidate_id
         OR (v_internal_timesheet_id IS NOT NULL AND NOT EXISTS(
           SELECT 1 FROM private.banking_pay_workbench_economic_build_scope scope_row
           JOIN pg_temp._bpay_wb_sync_context_v1 sync_context
             ON sync_context.build_id=scope_row.build_id
           WHERE scope_row.timesheet_id=v_internal_timesheet_id
         )) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH' USING ERRCODE='23514';
      END IF;
      IF v_effect_capture_mode THEN
        INSERT INTO pg_temp._bpay_wb_expected_effects(
          build_token,candidate_id,timesheet_id,relation_name,operation,source_id,
          finance_case_id,finance_component_id,economic_key_type,economic_key_value,
          expected_before_digest,expected_after_digest,proposed,observed
        ) VALUES(
          v_internal_build_token,v_internal_candidate_id,v_internal_timesheet_id,
          v_trigger_table,TG_OP,v_internal_source_id,v_internal_source_id,NULL,NULL,NULL,
          v_internal_before_digest,v_internal_after_digest,true,false
        );
      ELSIF TG_OP='INSERT' THEN
        UPDATE pg_temp._bpay_wb_expected_effects expected
        SET actual_source_id=v_internal_source_id,proposed=true
        WHERE expected.ctid=(SELECT candidate.ctid
          FROM pg_temp._bpay_wb_expected_effects candidate
          WHERE candidate.build_token=v_internal_build_token
            AND candidate.candidate_id=v_internal_candidate_id
            AND candidate.timesheet_id IS NOT DISTINCT FROM v_internal_timesheet_id
            AND candidate.relation_name=v_trigger_table AND candidate.operation=TG_OP
            AND candidate.proposed IS NOT TRUE AND candidate.observed IS NOT TRUE
            AND candidate.expected_before_digest IS NULL
            AND candidate.expected_after_digest IS NOT DISTINCT FROM v_internal_after_digest
          ORDER BY candidate.source_id LIMIT 1)
        RETURNING expected.source_id INTO v_internal_logical_source_id;
        GET DIAGNOSTICS v_expected_match_count=ROW_COUNT;
        IF v_expected_match_count<>1 OR v_internal_logical_source_id IS NULL THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH' USING ERRCODE='23514';
        END IF;
        INSERT INTO pg_temp._bpay_wb_effect_identity_map_v1(
          relation_name,logical_source_id,actual_source_id)
        VALUES(v_trigger_table,v_internal_logical_source_id,v_internal_source_id);
      ELSE
        UPDATE pg_temp._bpay_wb_expected_effects expected SET proposed=true
        WHERE expected.build_token=v_internal_build_token
          AND expected.candidate_id=v_internal_candidate_id
          AND expected.timesheet_id IS NOT DISTINCT FROM v_internal_timesheet_id
          AND expected.relation_name=v_trigger_table AND expected.operation=TG_OP
          AND expected.source_id=v_internal_source_id
          AND expected.proposed IS NOT TRUE AND expected.observed IS NOT TRUE
          AND expected.expected_before_digest IS NOT DISTINCT FROM v_internal_before_digest
          AND expected.expected_after_digest IS NOT DISTINCT FROM v_internal_after_digest;
        GET DIAGNOSTICS v_expected_match_count=ROW_COUNT;
        IF v_expected_match_count<>1 THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH' USING ERRCODE='23514';
        END IF;
      END IF;
      IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END IF;
  END IF;

  -- Natural-expiry fields are derived scheduler metadata.  An update confined
  -- to this allowlist must not create another generation or candidate build.
  IF TG_OP = 'UPDATE'
     AND v_trigger_table = 'pay_item_snoozes'
     AND (
       v_new_row - ARRAY[
         'natural_expiry_source_fingerprint',
         'natural_expiry_checked_fingerprint',
         'natural_expiry_checked_at_utc',
         'natural_expiry_state_changed',
         'natural_expiry_result_code'
       ]::text[]
     ) IS NOT DISTINCT FROM (
       v_old_row - ARRAY[
         'natural_expiry_source_fingerprint',
         'natural_expiry_checked_fingerprint',
         'natural_expiry_checked_at_utc',
         'natural_expiry_state_changed',
         'natural_expiry_result_code'
       ]::text[]
     ) THEN
    RETURN NEW;
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '') ~* v_uuid_re THEN
    v_candidate_ids := array_append(v_candidate_ids, NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid);
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '') ~* v_uuid_re THEN
    v_candidate_ids := array_append(v_candidate_ids, NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid);
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_old_row->>'timesheet_id', v_old_row->>'linked_timesheet_id', '')), '') ~* v_uuid_re THEN
    v_old_timesheet_id := NULLIF(BTRIM(COALESCE(v_old_row->>'timesheet_id', v_old_row->>'linked_timesheet_id', '')), '')::uuid;
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_new_row->>'timesheet_id', v_new_row->>'linked_timesheet_id', '')), '') ~* v_uuid_re THEN
    v_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_new_row->>'timesheet_id', v_new_row->>'linked_timesheet_id', '')), '')::uuid;
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_old_row->>'contract_id', '')), '') ~* v_uuid_re THEN
    v_old_contract_id := NULLIF(BTRIM(COALESCE(v_old_row->>'contract_id', '')), '')::uuid;
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_new_row->>'contract_id', '')), '') ~* v_uuid_re THEN
    v_new_contract_id := NULLIF(BTRIM(COALESCE(v_new_row->>'contract_id', '')), '')::uuid;
  END IF;

  v_explicit_banking_pay_action := lower(BTRIM(COALESCE(current_setting('cloudtms.banking_pay_explicit_action', true), 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR lower(BTRIM(COALESCE(current_setting('cloudtms.banking_pay_dirty_allowed', true), 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR lower(BTRIM(COALESCE(v_lifecycle_context, ''))) IN (
      'timesheet_authorise',
      'timesheet_unauthorise',
      'timesheet_archive',
      'timesheet_unarchive',
      'authorise_timesheet',
      'unauthorise_timesheet',
      'archive_timesheet',
      'unarchive_timesheet',
      'banking_pay',
      'banking_pay_refresh',
      'banking_pay_dirty',
      'banking_pay_action',
      'banking_pay_recalculate',
      'banking_pay_decision',
      'banking_pay_case_resolution',
      'banking_pay_timesheet_advance',
      'banking_pay_patch'
    );

  IF TG_OP = 'INSERT'
     AND v_trigger_table = 'timesheets'
     AND v_explicit_banking_pay_action IS NOT TRUE
     AND lower(BTRIM(COALESCE(v_lifecycle_context, ''))) IN ('manual_timesheet_save', 'ordinary_timesheet_save', 'timesheet_manual_save', 'manual_save')
     AND NULLIF(BTRIM(COALESCE(v_new_row->>'authorised_at_server', v_new_row->>'authorised_at_utc', '')), '') IS NULL
     AND NULLIF(BTRIM(COALESCE(v_new_row->>'revoked_at', v_new_row->>'revoked_at_utc', '')), '') IS NULL THEN
    PERFORM public._temp_diag_log(
      'TEMP_TRIGGER_DIRTY_STAGE',
      'TEMP_BANKING_PAY_DIRTY',
      COALESCE(v_new_timesheet_id::text, v_old_timesheet_id::text),
      jsonb_build_object(
        'function_name', 'pay_workbench_mark_candidate_dirty',
        'stage', 'early_return_ordinary_timesheet_insert_save_no_dirty',
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'mutation_context', v_lifecycle_context,
        'authorised_at_server_present', false,
        'revoked_at_present', false,
        'explicit_banking_pay_action', false,
        'banking_pay_dirty_required', false,
        'source_build_required', false,
        'line_work_required', false,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)
      )
    );
    RETURN NEW;
  END IF;

  IF TG_OP = 'UPDATE' AND v_trigger_table = 'timesheets' THEN
    v_authorise_boundary_changed :=
      (
        NULLIF(BTRIM(COALESCE(v_old_row->>'authorised_at_server', '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(v_old_row->>'revoked_at', '')), '') IS NULL
      ) IS DISTINCT FROM (
        NULLIF(BTRIM(COALESCE(v_new_row->>'authorised_at_server', '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(v_new_row->>'revoked_at', '')), '') IS NULL
      );

    v_archive_boundary_changed :=
      (NULLIF(BTRIM(COALESCE(v_old_row->>'archived_at_utc', '')), '') IS NOT NULL)
      IS DISTINCT FROM
      (NULLIF(BTRIM(COALESCE(v_new_row->>'archived_at_utc', '')), '') IS NOT NULL);

    v_material_live_source_changed :=
      (v_old_row->'worked_start_iso') IS DISTINCT FROM (v_new_row->'worked_start_iso')
      OR (v_old_row->'worked_end_iso') IS DISTINCT FROM (v_new_row->'worked_end_iso')
      OR (v_old_row->'break_start_iso') IS DISTINCT FROM (v_new_row->'break_start_iso')
      OR (v_old_row->'break_end_iso') IS DISTINCT FROM (v_new_row->'break_end_iso')
      OR (v_old_row->'break_minutes') IS DISTINCT FROM (v_new_row->'break_minutes')
      OR (v_old_row->'actual_schedule_json') IS DISTINCT FROM (v_new_row->'actual_schedule_json')
      OR (v_old_row->'additional_units_week') IS DISTINCT FROM (v_new_row->'additional_units_week')
      OR (v_old_row->'additional_units_per_day') IS DISTINCT FROM (v_new_row->'additional_units_per_day')
      OR (v_old_row->'week_ending_date') IS DISTINCT FROM (v_new_row->'week_ending_date')
      OR (v_old_row->'booking_id') IS DISTINCT FROM (v_new_row->'booking_id')
      OR (v_old_row->'line_type') IS DISTINCT FROM (v_new_row->'line_type')
      OR (v_old_row->'sheet_scope') IS DISTINCT FROM (v_new_row->'sheet_scope')
      OR (v_old_row->'reference_number') IS DISTINCT FROM (v_new_row->'reference_number')
      OR (v_old_row->'status') IS DISTINCT FROM (v_new_row->'status')
      OR (v_old_row->'submission_mode') IS DISTINCT FROM (v_new_row->'submission_mode')
      OR (v_old_row->'contract_id') IS DISTINCT FROM (v_new_row->'contract_id')
      OR (v_old_row->'is_adjustment') IS DISTINCT FROM (v_new_row->'is_adjustment')
      OR (v_old_row->'parent_timesheet_id') IS DISTINCT FROM (v_new_row->'parent_timesheet_id')
      OR (v_old_row->'correction_id') IS DISTINCT FROM (v_new_row->'correction_id')
      OR (v_old_row->'correction_kind') IS DISTINCT FROM (v_new_row->'correction_kind')
      OR (v_old_row->'adjustment_origin') IS DISTINCT FROM (v_new_row->'adjustment_origin');
  ELSIF TG_OP = 'UPDATE' AND v_trigger_table = 'timesheets_financials' THEN
    v_authorise_boundary_changed :=
      (NULLIF(BTRIM(COALESCE(v_old_row->>'authorised_at_utc', '')), '') IS NOT NULL)
      IS DISTINCT FROM
      (NULLIF(BTRIM(COALESCE(v_new_row->>'authorised_at_utc', '')), '') IS NOT NULL);

    IF LOWER(BTRIM(COALESCE(v_new_row->>'is_current', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      v_material_live_source_changed :=
      (v_old_row->'pay_method') IS DISTINCT FROM (v_new_row->'pay_method')
      OR (v_old_row->'policy_snapshot_json') IS DISTINCT FROM (v_new_row->'policy_snapshot_json')
      OR (v_old_row->'rate_source_refs_json') IS DISTINCT FROM (v_new_row->'rate_source_refs_json')
      OR (v_old_row->'hours_day') IS DISTINCT FROM (v_new_row->'hours_day')
      OR (v_old_row->'hours_night') IS DISTINCT FROM (v_new_row->'hours_night')
      OR (v_old_row->'hours_sat') IS DISTINCT FROM (v_new_row->'hours_sat')
      OR (v_old_row->'hours_sun') IS DISTINCT FROM (v_new_row->'hours_sun')
      OR (v_old_row->'hours_bh') IS DISTINCT FROM (v_new_row->'hours_bh')
      OR (v_old_row->'pay_day') IS DISTINCT FROM (v_new_row->'pay_day')
      OR (v_old_row->'pay_night') IS DISTINCT FROM (v_new_row->'pay_night')
      OR (v_old_row->'pay_sat') IS DISTINCT FROM (v_new_row->'pay_sat')
      OR (v_old_row->'pay_sun') IS DISTINCT FROM (v_new_row->'pay_sun')
      OR (v_old_row->'pay_bh') IS DISTINCT FROM (v_new_row->'pay_bh')
      OR (v_old_row->'total_hours') IS DISTINCT FROM (v_new_row->'total_hours')
      OR (v_old_row->'total_pay_ex_vat') IS DISTINCT FROM (v_new_row->'total_pay_ex_vat')
      OR (v_old_row->'expenses_pay_ex_vat') IS DISTINCT FROM (v_new_row->'expenses_pay_ex_vat')
      OR (v_old_row->'travel_pay_ex_vat') IS DISTINCT FROM (v_new_row->'travel_pay_ex_vat')
      OR (v_old_row->'accommodation_pay_ex_vat') IS DISTINCT FROM (v_new_row->'accommodation_pay_ex_vat')
      OR (v_old_row->'other_pay_ex_vat') IS DISTINCT FROM (v_new_row->'other_pay_ex_vat')
      OR (v_old_row->'mileage_units') IS DISTINCT FROM (v_new_row->'mileage_units')
      OR (v_old_row->'mileage_pay_rate') IS DISTINCT FROM (v_new_row->'mileage_pay_rate')
      OR (v_old_row->'mileage_pay_ex_vat') IS DISTINCT FROM (v_new_row->'mileage_pay_ex_vat')
      OR (v_old_row->'additional_pay_ex_vat') IS DISTINCT FROM (v_new_row->'additional_pay_ex_vat')
      OR (v_old_row->'pay_wtr_rate_pct_snapshot') IS DISTINCT FROM (v_new_row->'pay_wtr_rate_pct_snapshot')
      OR (v_old_row->'pay_vat_rate_pct_snapshot') IS DISTINCT FROM (v_new_row->'pay_vat_rate_pct_snapshot')
      OR (v_old_row->'pay_vat_amount_snapshot') IS DISTINCT FROM (v_new_row->'pay_vat_amount_snapshot')
      OR (v_old_row->'pay_total_inc_vat_snapshot') IS DISTINCT FROM (v_new_row->'pay_total_inc_vat_snapshot')
      OR (v_old_row->'invoice_breakdown_json') IS DISTINCT FROM (v_new_row->'invoice_breakdown_json')
      OR (v_old_row->'additional_units_json') IS DISTINCT FROM (v_new_row->'additional_units_json')
      OR (v_old_row->'actual_schedule_json') IS DISTINCT FROM (v_new_row->'actual_schedule_json')
      OR (v_old_row->'worked_start_iso') IS DISTINCT FROM (v_new_row->'worked_start_iso')
      OR (v_old_row->'worked_end_iso') IS DISTINCT FROM (v_new_row->'worked_end_iso')
      OR (v_old_row->'break_start_iso') IS DISTINCT FROM (v_new_row->'break_start_iso')
      OR (v_old_row->'break_end_iso') IS DISTINCT FROM (v_new_row->'break_end_iso')
      OR (v_old_row->'break_minutes') IS DISTINCT FROM (v_new_row->'break_minutes')
      OR (v_old_row->'pay_on_hold') IS DISTINCT FROM (v_new_row->'pay_on_hold')
      OR (v_old_row->'pay_on_hold_reason') IS DISTINCT FROM (v_new_row->'pay_on_hold_reason')
      OR (v_old_row->'has_rate_issue') IS DISTINCT FROM (v_new_row->'has_rate_issue')
      OR (v_old_row->'has_pay_channel_issue') IS DISTINCT FROM (v_new_row->'has_pay_channel_issue')
      OR (v_old_row->'processing_status') IS DISTINCT FROM (v_new_row->'processing_status')
      OR (v_old_row->'locked_by_invoice_id') IS DISTINCT FROM (v_new_row->'locked_by_invoice_id')
      OR (v_old_row->'unlocked_by_credit_note_id') IS DISTINCT FROM (v_new_row->'unlocked_by_credit_note_id')
      OR (v_old_row->'candidate_id') IS DISTINCT FROM (v_new_row->'candidate_id')
      OR (v_old_row->'client_id') IS DISTINCT FROM (v_new_row->'client_id');
    ELSE
      -- Snapshot rotation demotes the previous row before inserting its
      -- replacement.  The demotion itself is bookkeeping, not a live-source
      -- economic change and must not mark a frozen Draft stale.
      v_material_live_source_changed := false;
    END IF;
  ELSIF TG_OP = 'INSERT' AND v_trigger_table = 'timesheets_financials' THEN
    IF LOWER(BTRIM(COALESCE(v_new_row->>'is_current', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      SELECT COALESCE(to_jsonb(previous_financial_row), '{}'::jsonb)
      INTO v_previous_tsfin_row
      FROM public.timesheets_financials AS previous_financial_row
      WHERE previous_financial_row.timesheet_id = v_new_timesheet_id
        AND previous_financial_row.id IS DISTINCT FROM
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '') ~* v_uuid_re
              THEN NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '')::uuid
            ELSE NULL::uuid
          END
      ORDER BY
        previous_financial_row.timesheet_version DESC NULLS LAST,
        previous_financial_row.computed_at_utc DESC NULLS LAST,
        previous_financial_row.created_at DESC NULLS LAST,
        previous_financial_row.id DESC
      LIMIT 1;

      v_previous_tsfin_row := COALESCE(v_previous_tsfin_row, '{}'::jsonb);
      IF v_previous_tsfin_row = '{}'::jsonb THEN
        v_material_live_source_changed := true;
      ELSE
        v_material_live_source_changed :=
          (v_previous_tsfin_row->'pay_method') IS DISTINCT FROM (v_new_row->'pay_method')
          OR (v_previous_tsfin_row->'policy_snapshot_json') IS DISTINCT FROM (v_new_row->'policy_snapshot_json')
          OR (v_previous_tsfin_row->'rate_source_refs_json') IS DISTINCT FROM (v_new_row->'rate_source_refs_json')
          OR (v_previous_tsfin_row->'hours_day') IS DISTINCT FROM (v_new_row->'hours_day')
          OR (v_previous_tsfin_row->'hours_night') IS DISTINCT FROM (v_new_row->'hours_night')
          OR (v_previous_tsfin_row->'hours_sat') IS DISTINCT FROM (v_new_row->'hours_sat')
          OR (v_previous_tsfin_row->'hours_sun') IS DISTINCT FROM (v_new_row->'hours_sun')
          OR (v_previous_tsfin_row->'hours_bh') IS DISTINCT FROM (v_new_row->'hours_bh')
          OR (v_previous_tsfin_row->'pay_day') IS DISTINCT FROM (v_new_row->'pay_day')
          OR (v_previous_tsfin_row->'pay_night') IS DISTINCT FROM (v_new_row->'pay_night')
          OR (v_previous_tsfin_row->'pay_sat') IS DISTINCT FROM (v_new_row->'pay_sat')
          OR (v_previous_tsfin_row->'pay_sun') IS DISTINCT FROM (v_new_row->'pay_sun')
          OR (v_previous_tsfin_row->'pay_bh') IS DISTINCT FROM (v_new_row->'pay_bh')
          OR (v_previous_tsfin_row->'total_hours') IS DISTINCT FROM (v_new_row->'total_hours')
          OR (v_previous_tsfin_row->'total_pay_ex_vat') IS DISTINCT FROM (v_new_row->'total_pay_ex_vat')
          OR (v_previous_tsfin_row->'expenses_pay_ex_vat') IS DISTINCT FROM (v_new_row->'expenses_pay_ex_vat')
          OR (v_previous_tsfin_row->'travel_pay_ex_vat') IS DISTINCT FROM (v_new_row->'travel_pay_ex_vat')
          OR (v_previous_tsfin_row->'accommodation_pay_ex_vat') IS DISTINCT FROM (v_new_row->'accommodation_pay_ex_vat')
          OR (v_previous_tsfin_row->'other_pay_ex_vat') IS DISTINCT FROM (v_new_row->'other_pay_ex_vat')
          OR (v_previous_tsfin_row->'mileage_units') IS DISTINCT FROM (v_new_row->'mileage_units')
          OR (v_previous_tsfin_row->'mileage_pay_rate') IS DISTINCT FROM (v_new_row->'mileage_pay_rate')
          OR (v_previous_tsfin_row->'mileage_pay_ex_vat') IS DISTINCT FROM (v_new_row->'mileage_pay_ex_vat')
          OR (v_previous_tsfin_row->'additional_pay_ex_vat') IS DISTINCT FROM (v_new_row->'additional_pay_ex_vat')
          OR (v_previous_tsfin_row->'pay_wtr_rate_pct_snapshot') IS DISTINCT FROM (v_new_row->'pay_wtr_rate_pct_snapshot')
          OR (v_previous_tsfin_row->'pay_vat_rate_pct_snapshot') IS DISTINCT FROM (v_new_row->'pay_vat_rate_pct_snapshot')
          OR (v_previous_tsfin_row->'pay_vat_amount_snapshot') IS DISTINCT FROM (v_new_row->'pay_vat_amount_snapshot')
          OR (v_previous_tsfin_row->'pay_total_inc_vat_snapshot') IS DISTINCT FROM (v_new_row->'pay_total_inc_vat_snapshot')
          OR (v_previous_tsfin_row->'invoice_breakdown_json') IS DISTINCT FROM (v_new_row->'invoice_breakdown_json')
          OR (v_previous_tsfin_row->'additional_units_json') IS DISTINCT FROM (v_new_row->'additional_units_json')
          OR (v_previous_tsfin_row->'actual_schedule_json') IS DISTINCT FROM (v_new_row->'actual_schedule_json')
          OR (v_previous_tsfin_row->'worked_start_iso') IS DISTINCT FROM (v_new_row->'worked_start_iso')
          OR (v_previous_tsfin_row->'worked_end_iso') IS DISTINCT FROM (v_new_row->'worked_end_iso')
          OR (v_previous_tsfin_row->'break_start_iso') IS DISTINCT FROM (v_new_row->'break_start_iso')
          OR (v_previous_tsfin_row->'break_end_iso') IS DISTINCT FROM (v_new_row->'break_end_iso')
          OR (v_previous_tsfin_row->'break_minutes') IS DISTINCT FROM (v_new_row->'break_minutes')
          OR (v_previous_tsfin_row->'pay_on_hold') IS DISTINCT FROM (v_new_row->'pay_on_hold')
          OR (v_previous_tsfin_row->'pay_on_hold_reason') IS DISTINCT FROM (v_new_row->'pay_on_hold_reason')
          OR (v_previous_tsfin_row->'has_rate_issue') IS DISTINCT FROM (v_new_row->'has_rate_issue')
          OR (v_previous_tsfin_row->'has_pay_channel_issue') IS DISTINCT FROM (v_new_row->'has_pay_channel_issue')
          OR (v_previous_tsfin_row->'processing_status') IS DISTINCT FROM (v_new_row->'processing_status')
          OR (v_previous_tsfin_row->'locked_by_invoice_id') IS DISTINCT FROM (v_new_row->'locked_by_invoice_id')
          OR (v_previous_tsfin_row->'unlocked_by_credit_note_id') IS DISTINCT FROM (v_new_row->'unlocked_by_credit_note_id')
          OR (v_previous_tsfin_row->'candidate_id') IS DISTINCT FROM (v_new_row->'candidate_id')
          OR (v_previous_tsfin_row->'client_id') IS DISTINCT FROM (v_new_row->'client_id');
      END IF;
    END IF;
  END IF;

  IF (
       (TG_OP = 'UPDATE' AND v_trigger_table = 'timesheets')
       OR (TG_OP IN ('INSERT', 'UPDATE') AND v_trigger_table = 'timesheets_financials')
     )
     AND (v_material_live_source_changed IS TRUE OR v_authorise_boundary_changed IS TRUE OR v_archive_boundary_changed IS TRUE) THEN
    SELECT COALESCE(array_agg(DISTINCT rotation_scope.family_timesheet_id ORDER BY rotation_scope.family_timesheet_id), ARRAY[]::uuid[])
    INTO v_signal_timesheet_ids
    FROM public._pay_timesheet_rotation_scope(
      ARRAY[COALESCE(v_new_timesheet_id, v_old_timesheet_id)]::uuid[]
    ) AS rotation_scope
    WHERE rotation_scope.family_timesheet_id IS NOT NULL;

    IF COALESCE(array_length(v_signal_timesheet_ids, 1), 0) = 0
       AND COALESCE(v_new_timesheet_id, v_old_timesheet_id) IS NOT NULL THEN
      v_signal_timesheet_ids := ARRAY[COALESCE(v_new_timesheet_id, v_old_timesheet_id)]::uuid[];
    END IF;

    SELECT COALESCE(array_agg(DISTINCT candidate_scope.candidate_id ORDER BY candidate_scope.candidate_id), ARRAY[]::uuid[])
    INTO v_signal_candidate_ids
    FROM (
      SELECT CASE
        WHEN NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '') ~* v_uuid_re
          THEN NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS candidate_id
      UNION ALL
      SELECT CASE
        WHEN NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '') ~* v_uuid_re
          THEN NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
        ELSE NULL::uuid
      END
      UNION ALL
      SELECT financial_row.candidate_id
      FROM public.timesheets_financials AS financial_row
      WHERE financial_row.timesheet_id = ANY(COALESCE(v_signal_timesheet_ids, ARRAY[]::uuid[]))
      UNION ALL
      SELECT contract_row.candidate_id
      FROM public.timesheets AS timesheet_row
      JOIN public.contracts AS contract_row
        ON contract_row.id = timesheet_row.contract_id
      WHERE timesheet_row.timesheet_id = ANY(COALESCE(v_signal_timesheet_ids, ARRAY[]::uuid[]))
    ) AS candidate_scope
    WHERE candidate_scope.candidate_id IS NOT NULL;

    v_signal_reason := CASE
      WHEN v_archive_boundary_changed IS TRUE
           AND NULLIF(BTRIM(COALESCE(v_new_row->>'archived_at_utc', '')), '') IS NOT NULL
        THEN 'TIMESHEET_ARCHIVED_LIVE_SOURCE_CHANGED'
      WHEN v_archive_boundary_changed IS TRUE
        THEN 'TIMESHEET_UNARCHIVED_LIVE_SOURCE_CHANGED'
      WHEN v_authorise_boundary_changed IS TRUE
           AND (
             NULLIF(BTRIM(COALESCE(v_new_row->>'authorised_at_server', v_new_row->>'authorised_at_utc', '')), '') IS NULL
             OR NULLIF(BTRIM(COALESCE(v_new_row->>'revoked_at', '')), '') IS NOT NULL
           )
        THEN 'TIMESHEET_UNAUTHORISED_LIVE_SOURCE_CHANGED'
      WHEN v_authorise_boundary_changed IS TRUE
        THEN 'TIMESHEET_AUTHORISED_LIVE_SOURCE_CHANGED'
      ELSE 'TIMESHEET_MATERIAL_LIVE_SOURCE_CHANGED'
    END;

    FOR v_signal_batch_row IN
      SELECT DISTINCT pay_batch.id AS pay_batch_id
      FROM public.pay_batches AS pay_batch
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.pay_batch_id = pay_batch.id
       AND batch_candidate.candidate_id = ANY(COALESCE(v_signal_candidate_ids, ARRAY[]::uuid[]))
      WHERE UPPER(BTRIM(COALESCE(pay_batch.status, ''))) = 'DRAFT'
        AND EXISTS (
          SELECT 1
          FROM public.pay_batch_items AS batch_item
          LEFT JOIN public.pay_advances AS finance_case
            ON finance_case.id = batch_item.finance_case_id
          WHERE batch_item.pay_batch_candidate_id = batch_candidate.id
            AND batch_item.is_voided IS NOT TRUE
            AND (
              batch_item.timesheet_id = ANY(COALESCE(v_signal_timesheet_ids, ARRAY[]::uuid[]))
              OR finance_case.linked_timesheet_id = ANY(COALESCE(v_signal_timesheet_ids, ARRAY[]::uuid[]))
            )
        )
      ORDER BY pay_batch.id
    LOOP
      BEGIN
        v_signal_result := public.banking_pay_batch_signal_touch(
          p_pay_batch_id => v_signal_batch_row.pay_batch_id,
          p_change_reason => v_signal_reason,
          p_change_source => 'pay_workbench_mark_candidate_dirty',
          p_change_scope_json => jsonb_build_object(
            'stale_hint', true,
            'stale_reason', v_signal_reason,
            'candidate_ids', COALESCE(to_jsonb(v_signal_candidate_ids), '[]'::jsonb),
            'timesheet_ids', COALESCE(to_jsonb(v_signal_timesheet_ids), '[]'::jsonb),
            'trigger_table', v_trigger_table,
            'trigger_operation', TG_OP,
            'policy_x_authority_scope', 'FROZEN_DRAFT_INVALIDATION_HINT_ONLY'
          ),
          p_touch_payment_status => false,
          p_touch_correction_progress => false,
          p_touch_alerts => true,
          p_touch_overview => true
        );
      EXCEPTION
        WHEN OTHERS THEN
          v_signal_result := jsonb_build_object(
            'ok', false,
            'error_code', SQLSTATE,
            'error_message', SQLERRM,
            'pay_batch_id', v_signal_batch_row.pay_batch_id::text
          );
          PERFORM public._temp_diag_log(
            'TEMP_TRIGGER_DIRTY_STAGE',
            'TEMP_BANKING_PAY_DIRTY',
            COALESCE(v_new_timesheet_id::text, v_old_timesheet_id::text),
            jsonb_build_object(
              'function_name', 'pay_workbench_mark_candidate_dirty',
              'stage', 'lightweight_draft_stale_signal_failed_non_blocking',
              'pay_batch_id', v_signal_batch_row.pay_batch_id::text,
              'signal_reason', v_signal_reason,
              'error_code', SQLSTATE,
              'error_message', SQLERRM,
              'policy_x_authority_scope', 'FROZEN_DRAFT_INVALIDATION_HINT_ONLY'
            )
          );
      END;
    END LOOP;
  END IF;

  IF TG_OP = 'UPDATE' AND v_trigger_table = 'timesheets' THEN
    IF v_authorise_boundary_changed IS NOT TRUE
       AND v_archive_boundary_changed IS NOT TRUE
       AND v_explicit_banking_pay_action IS NOT TRUE THEN
      PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', COALESCE(v_new_timesheet_id::text, v_old_timesheet_id::text), jsonb_build_object('function_name', 'pay_workbench_mark_candidate_dirty', 'stage', 'early_return_ordinary_timesheet_edit_save_no_dirty', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'material_live_source_changed', v_material_live_source_changed, 'draft_stale_signal_only', v_material_live_source_changed, 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));
      RETURN NEW;
    END IF;
  ELSIF TG_OP = 'UPDATE' AND v_trigger_table = 'timesheets_financials' THEN
    IF v_authorise_boundary_changed IS NOT TRUE AND v_explicit_banking_pay_action IS NOT TRUE THEN
      PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', COALESCE(v_new_timesheet_id::text, v_old_timesheet_id::text), jsonb_build_object('function_name', 'pay_workbench_mark_candidate_dirty', 'stage', 'early_return_ordinary_timesheet_financials_edit_save_no_dirty', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'material_live_source_changed', v_material_live_source_changed, 'draft_stale_signal_only', v_material_live_source_changed, 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));
      RETURN NEW;
    END IF;
  END IF;

  IF TG_OP = 'INSERT'
     AND v_trigger_table = 'timesheets_financials'
     AND v_explicit_banking_pay_action IS NOT TRUE
     AND NULLIF(BTRIM(COALESCE(v_new_row->>'authorised_at_utc', '')), '') IS NULL THEN
    PERFORM public._temp_diag_log(
      'TEMP_TRIGGER_DIRTY_STAGE',
      'TEMP_BANKING_PAY_DIRTY',
      COALESCE(v_new_timesheet_id::text, v_old_timesheet_id::text),
      jsonb_build_object(
        'function_name', 'pay_workbench_mark_candidate_dirty',
        'stage', 'early_return_ordinary_timesheet_financials_insert_edit_save_no_dirty',
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'mutation_context', v_lifecycle_context,
        'authorised_at_utc_present', false,
        'explicit_banking_pay_action', false,
        'banking_pay_dirty_required', false,
        'source_build_required', false,
        'line_work_required', false,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)
      )
    );
    RETURN NEW;
  END IF;

  IF TG_OP = 'UPDATE' AND v_trigger_table = 'timesheet_pay_state' THEN
    v_timesheet_pay_state_settlement_changed :=
      (v_old_row->'last_settled_snapshot_json') IS DISTINCT FROM (v_new_row->'last_settled_snapshot_json')
      OR NULLIF(BTRIM(COALESCE(v_old_row->>'last_settled_signature', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'last_settled_signature', '')), '')
      OR NULLIF(BTRIM(COALESCE(v_old_row->>'last_settled_pay_batch_id', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'last_settled_pay_batch_id', '')), '')
      OR NULLIF(BTRIM(COALESCE(v_old_row->>'last_settled_at_utc', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'last_settled_at_utc', '')), '');

    v_timesheet_pay_state_summary_changed :=
      NULLIF(BTRIM(COALESCE(v_old_row->>'summary_pay_status_code', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'summary_pay_status_code', '')), '')
      OR NULLIF(BTRIM(COALESCE(v_old_row->>'summary_pay_icon_code', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'summary_pay_icon_code', '')), '')
      OR NULLIF(BTRIM(COALESCE(v_old_row->>'summary_pay_paid_at_utc', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'summary_pay_paid_at_utc', '')), '')
      OR NULLIF(BTRIM(COALESCE(v_old_row->>'summary_net_delta_ex_vat', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'summary_net_delta_ex_vat', '')), '');

    IF v_timesheet_pay_state_settlement_changed IS NOT TRUE THEN
      v_timesheet_pay_state_bookkeeping_ignored := COALESCE(v_timesheet_pay_state_summary_changed, false);
      v_timesheet_pay_state_noop_ignored := COALESCE(v_timesheet_pay_state_summary_changed, false) IS NOT TRUE;

      PERFORM public._temp_diag_log(
        'TEMP_TRIGGER_DIRTY_STAGE',
        'TEMP_BANKING_PAY_DIRTY',
        COALESCE(v_new_timesheet_id::text, v_old_timesheet_id::text),
        jsonb_build_object(
          'function_name', 'pay_workbench_mark_candidate_dirty',
          'stage', CASE
            WHEN v_timesheet_pay_state_bookkeeping_ignored IS TRUE THEN 'early_return_timesheet_pay_state_bookkeeping_no_dirty'
            ELSE 'early_return_timesheet_pay_state_noop_no_dirty'
          END,
          'trigger_table', v_trigger_table,
          'trigger_op', TG_OP,
          'dirty_classification', CASE
            WHEN v_timesheet_pay_state_bookkeeping_ignored IS TRUE THEN 'TIMESHEET_PAY_STATE_BOOKKEEPING_IGNORED'
            ELSE 'TIMESHEET_PAY_STATE_NOOP_IGNORED'
          END,
          'timesheet_pay_state_settlement_changed', COALESCE(v_timesheet_pay_state_settlement_changed, false),
          'timesheet_pay_state_summary_changed', COALESCE(v_timesheet_pay_state_summary_changed, false),
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)
        )
      );
      RETURN NEW;
    END IF;

    v_timesheet_pay_state_routing_reason := 'SOURCE_BUILD_REQUIRED_PAY_STATE_SETTLED_BASELINE_CHANGE';
  END IF;

  IF TG_OP = 'UPDATE' AND v_trigger_table = 'timesheets' THEN
    v_old_payment_eligible := NULLIF(BTRIM(COALESCE(v_old_row->>'authorised_at_server', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(v_old_row->>'revoked_at', '')), '') IS NULL
      AND NULLIF(BTRIM(COALESCE(v_old_row->>'archived_at_utc', '')), '') IS NULL;
    v_new_payment_eligible := NULLIF(BTRIM(COALESCE(v_new_row->>'authorised_at_server', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(v_new_row->>'revoked_at', '')), '') IS NULL
      AND NULLIF(BTRIM(COALESCE(v_new_row->>'archived_at_utc', '')), '') IS NULL;
    IF v_old_payment_eligible IS NOT TRUE
       AND v_new_payment_eligible IS NOT TRUE
       AND v_archive_boundary_changed IS NOT TRUE THEN
      PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', COALESCE(v_new_timesheet_id::text, v_old_timesheet_id::text), jsonb_build_object('function_name', 'pay_workbench_mark_candidate_dirty', 'stage', 'early_return_ordinary_unauthorised_edit', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));
      RETURN NEW;
    END IF;
  ELSIF TG_OP = 'UPDATE' AND v_trigger_table = 'timesheets_financials' THEN
    v_old_payment_eligible := NULLIF(BTRIM(COALESCE(v_old_row->>'authorised_at_utc', '')), '') IS NOT NULL;
    v_new_payment_eligible := NULLIF(BTRIM(COALESCE(v_new_row->>'authorised_at_utc', '')), '') IS NOT NULL;
    IF v_old_payment_eligible IS NOT TRUE AND v_new_payment_eligible IS NOT TRUE THEN
      v_tsfin_payability_state_changed :=
        NULLIF(BTRIM(COALESCE(v_old_row->>'paid_at_utc', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'paid_at_utc', '')), '')
        OR NULLIF(BTRIM(COALESCE(v_old_row->>'paid_by_user_id', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'paid_by_user_id', '')), '')
        OR NULLIF(BTRIM(COALESCE(v_old_row->>'payment_reference', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'payment_reference', '')), '')
        OR NULLIF(BTRIM(COALESCE(v_old_row->>'pay_on_hold', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'pay_on_hold', '')), '')
        OR NULLIF(BTRIM(COALESCE(v_old_row->>'pay_on_hold_reason', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'pay_on_hold_reason', '')), '')
        OR NULLIF(BTRIM(COALESCE(v_old_row->>'pay_on_hold_since_utc', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'pay_on_hold_since_utc', '')), '')
        OR NULLIF(BTRIM(COALESCE(v_old_row->>'locked_by_invoice_id', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'locked_by_invoice_id', '')), '')
        OR NULLIF(BTRIM(COALESCE(v_old_row->>'locked_at_utc', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'locked_at_utc', '')), '')
        OR NULLIF(BTRIM(COALESCE(v_old_row->>'unlocked_by_credit_note_id', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'unlocked_by_credit_note_id', '')), '')
        OR NULLIF(BTRIM(COALESCE(v_old_row->>'remittance_last_sent_at_utc', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'remittance_last_sent_at_utc', '')), '')
        OR NULLIF(BTRIM(COALESCE(v_old_row->>'remittance_send_count', '')), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_new_row->>'remittance_send_count', '')), '');
      IF v_tsfin_payability_state_changed IS NOT TRUE THEN
        PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', COALESCE(v_new_timesheet_id::text, v_old_timesheet_id::text), jsonb_build_object('function_name', 'pay_workbench_mark_candidate_dirty', 'stage', 'early_return_ordinary_unauthorised_edit', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'payability_state_changed', false, 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));
        RETURN NEW;
      END IF;
    END IF;
  END IF;

  IF TG_OP = 'UPDATE'
     AND v_trigger_table IN ('timesheets', 'timesheets_financials')
     AND v_authorise_boundary_changed IS TRUE THEN
    IF v_new_payment_eligible IS TRUE AND v_old_payment_eligible IS NOT TRUE THEN
      v_authorise_boundary_is_authorise := true;
    ELSIF v_old_payment_eligible IS TRUE AND v_new_payment_eligible IS NOT TRUE THEN
      v_authorise_boundary_is_unauthorise := true;
    ELSIF lower(BTRIM(COALESCE(v_lifecycle_context, ''))) IN ('timesheet_authorise', 'authorise_timesheet') THEN
      v_authorise_boundary_is_authorise := true;
    ELSIF lower(BTRIM(COALESCE(v_lifecycle_context, ''))) IN ('timesheet_unauthorise', 'unauthorise_timesheet') THEN
      v_authorise_boundary_is_unauthorise := true;
    END IF;
  END IF;

  IF TG_OP = 'UPDATE'
     AND v_trigger_table = 'timesheets'
     AND v_archive_boundary_changed IS TRUE THEN
    IF NULLIF(BTRIM(COALESCE(v_old_row->>'archived_at_utc', '')), '') IS NULL
       AND NULLIF(BTRIM(COALESCE(v_new_row->>'archived_at_utc', '')), '') IS NOT NULL THEN
      v_archive_boundary_is_archive := true;
    ELSIF NULLIF(BTRIM(COALESCE(v_old_row->>'archived_at_utc', '')), '') IS NOT NULL
       AND NULLIF(BTRIM(COALESCE(v_new_row->>'archived_at_utc', '')), '') IS NULL THEN
      v_archive_boundary_is_unarchive := true;
    ELSIF lower(BTRIM(COALESCE(v_lifecycle_context, ''))) IN ('timesheet_archive', 'archive_timesheet') THEN
      v_archive_boundary_is_archive := true;
    ELSIF lower(BTRIM(COALESCE(v_lifecycle_context, ''))) IN ('timesheet_unarchive', 'unarchive_timesheet') THEN
      v_archive_boundary_is_unarchive := true;
    END IF;
  END IF;

  v_effective_lifecycle_context := CASE
    WHEN v_archive_boundary_is_archive IS TRUE THEN 'timesheet_archive'
    WHEN v_archive_boundary_is_unarchive IS TRUE THEN 'timesheet_unarchive'
    WHEN v_authorise_boundary_is_unauthorise IS TRUE THEN 'timesheet_unauthorise'
    WHEN v_authorise_boundary_is_authorise IS TRUE THEN 'timesheet_authorise'
    WHEN lower(BTRIM(COALESCE(v_lifecycle_context, ''))) IN ('timesheet_authorise', 'authorise_timesheet') THEN 'timesheet_authorise'
    WHEN lower(BTRIM(COALESCE(v_lifecycle_context, ''))) IN ('timesheet_unauthorise', 'unauthorise_timesheet') THEN 'timesheet_unauthorise'
    WHEN lower(BTRIM(COALESCE(v_lifecycle_context, ''))) IN ('timesheet_archive', 'archive_timesheet') THEN 'timesheet_archive'
    WHEN lower(BTRIM(COALESCE(v_lifecycle_context, ''))) IN ('timesheet_unarchive', 'unarchive_timesheet') THEN 'timesheet_unarchive'
    ELSE v_lifecycle_context
  END;

  v_banking_pay_dirty_required := v_authorise_boundary_changed IS TRUE
    OR v_archive_boundary_changed IS TRUE
    OR v_explicit_banking_pay_action IS TRUE
    OR (v_trigger_table = 'timesheet_pay_state' AND v_timesheet_pay_state_settlement_changed IS TRUE);
  v_ordinary_timesheet_edit_save_no_dirty := TG_OP = 'UPDATE'
    AND v_trigger_table IN ('timesheets', 'timesheets_financials')
    AND v_banking_pay_dirty_required IS NOT TRUE;

  IF v_old_timesheet_id IS NOT NULL OR v_new_timesheet_id IS NOT NULL THEN
    v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
    SELECT COALESCE(array_agg(DISTINCT timesheet_ids.timesheet_id ORDER BY timesheet_ids.timesheet_id), ARRAY[]::uuid[])
    INTO v_targeted_timesheet_ids
    FROM (
      SELECT v_old_timesheet_id AS timesheet_id
      UNION ALL
      SELECT v_new_timesheet_id AS timesheet_id
    ) AS timesheet_ids
    WHERE timesheet_ids.timesheet_id IS NOT NULL;
  END IF;

  IF v_trigger_table = 'candidates' THEN
    IF NULLIF(BTRIM(COALESCE(v_old_row->>'id', '')), '') ~* v_uuid_re THEN
      v_candidate_ids := array_append(v_candidate_ids, NULLIF(BTRIM(COALESCE(v_old_row->>'id', '')), '')::uuid);
    END IF;
    IF NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '') ~* v_uuid_re THEN
      v_candidate_ids := array_append(v_candidate_ids, NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '')::uuid);
    END IF;

    IF TG_OP = 'UPDATE' THEN
      v_route_source_method := UPPER(BTRIM(COALESCE(v_old_row->>'pay_method', '')));
      v_route_target_method := UPPER(BTRIM(COALESCE(v_new_row->>'pay_method', '')));
      v_candidate_route_change := v_route_source_method IN ('PAYE', 'UMBRELLA')
        AND v_route_target_method IN ('PAYE', 'UMBRELLA')
        AND v_route_source_method <> v_route_target_method;

      IF v_candidate_route_change IS TRUE THEN
        IF v_route_operation_id IS NULL OR v_route_operation_id !~* v_uuid_re THEN
          RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_OPERATION_CONTEXT_REQUIRED'
            USING ERRCODE = '22023';
        END IF;
        IF v_route_actor_user_id IS NULL OR v_route_actor_user_id !~* v_uuid_re THEN
          RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_ACTOR_CONTEXT_REQUIRED'
            USING ERRCODE = '22023';
        END IF;

        v_route_scope_result := public.candidate_pay_method_change_refresh_scope_v1(
          NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '')::uuid,
          v_route_source_method,
          v_route_target_method
        );

        v_route_exact_scope := LOWER(BTRIM(COALESCE(v_route_scope_result->>'exact_scope', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
        v_route_coverage_complete := LOWER(BTRIM(COALESCE(v_route_scope_result->>'coverage_complete', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

        IF COALESCE(v_route_scope_result->>'ok', 'false') <> 'true'
           OR v_route_exact_scope IS NOT TRUE
           OR v_route_coverage_complete IS NOT TRUE
           OR jsonb_typeof(v_route_scope_result->'targeted_timesheet_ids') IS DISTINCT FROM 'array' THEN
          RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_EXACT_SCOPE_UNAVAILABLE'
            USING ERRCODE = 'P0001', DETAIL = COALESCE(v_route_scope_result, '{}'::jsonb)::text;
        END IF;

        IF EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(v_route_scope_result->'targeted_timesheet_ids') AS raw_target(value)
          WHERE raw_target.value !~* v_uuid_re
        ) THEN
          RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_TARGET_SCOPE_INVALID'
            USING ERRCODE = 'P0001';
        END IF;

        SELECT COALESCE(array_agg(DISTINCT target_value::uuid ORDER BY target_value::uuid), ARRAY[]::uuid[])
        INTO v_targeted_timesheet_ids
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(v_route_scope_result->'targeted_timesheet_ids') = 'array'
              THEN v_route_scope_result->'targeted_timesheet_ids'
            ELSE '[]'::jsonb
          END
        ) AS target_values(target_value)
        WHERE target_value ~* v_uuid_re;

        v_refresh_scope_kind := 'TARGETED_TIMESHEETS';
        v_route_targeted_scope_is_empty := COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) = 0;
        v_route_authoritative_sessions := COALESCE(v_route_scope_result->'authoritative_sessions', '[]'::jsonb);
        v_route_replaced_source_session_ids := COALESCE(v_route_scope_result->'replaced_source_session_ids', '[]'::jsonb);
        v_route_target_details := COALESCE(v_route_scope_result->'target_details', '[]'::jsonb);
        v_route_authorised_timesheet_ids := COALESCE(v_route_scope_result->'authorised_timesheet_ids', '[]'::jsonb);
        v_route_active_advance_timesheet_ids := COALESCE(v_route_scope_result->'active_advance_timesheet_ids', '[]'::jsonb);
        v_route_retained_finance_timesheet_ids := COALESCE(v_route_scope_result->'retained_finance_timesheet_ids', '[]'::jsonb);
        v_route_authorised_timesheet_count := COALESCE(
          CASE WHEN COALESCE(v_route_scope_result->>'authorised_timesheet_count', '') ~ '^[0-9]{1,10}$'
            THEN (v_route_scope_result->>'authorised_timesheet_count')::integer END,
          0
        );
        v_route_active_advance_timesheet_count := COALESCE(
          CASE WHEN COALESCE(v_route_scope_result->>'active_advance_timesheet_count', '') ~ '^[0-9]{1,10}$'
            THEN (v_route_scope_result->>'active_advance_timesheet_count')::integer END,
          0
        );
        v_route_retained_finance_timesheet_count := COALESCE(
          CASE WHEN COALESCE(v_route_scope_result->>'retained_finance_timesheet_count', '') ~ '^[0-9]{1,10}$'
            THEN (v_route_scope_result->>'retained_finance_timesheet_count')::integer END,
          0
        );
        v_route_source_target_mismatch_count := COALESCE(
          CASE WHEN COALESCE(v_route_scope_result->>'source_target_mismatch_count', '') ~ '^[0-9]{1,10}$'
            THEN (v_route_scope_result->>'source_target_mismatch_count')::integer END,
          0
        );

        IF COALESCE(v_route_scope_result->>'durable_job_id', '') !~* v_uuid_re
           OR LOWER(BTRIM(COALESCE(v_route_scope_result->>'durable_scope_persisted', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN
          RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DURABLE_QUEUE_MISSING'
            USING ERRCODE = 'P0001', DETAIL = COALESCE(v_route_scope_result, '{}'::jsonb)::text;
        END IF;

        v_route_job_id := (v_route_scope_result->>'durable_job_id')::uuid;
        v_route_job_status := NULLIF(BTRIM(COALESCE(v_route_scope_result->>'durable_job_status', '')), '');
        v_route_dedupe_key := NULLIF(BTRIM(COALESCE(v_route_scope_result->>'durable_job_dedupe_key', '')), '');
        v_route_job_source_change_seq := COALESCE(
          CASE
            WHEN COALESCE(v_route_scope_result->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (v_route_scope_result->>'source_change_seq')::bigint
            ELSE 0
          END,
          0
        );

        IF v_route_job_status NOT IN ('QUEUED', 'RUNNING', 'SUCCEEDED')
           OR v_route_dedupe_key IS NULL
           OR v_route_job_source_change_seq <= 0 THEN
          RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DURABLE_QUEUE_INVALID'
            USING ERRCODE = 'P0001', DETAIL = COALESCE(v_route_scope_result, '{}'::jsonb)::text;
        END IF;

        FOR v_signal_batch_row IN
          SELECT DISTINCT pay_batch.id AS pay_batch_id
          FROM public.pay_batches AS pay_batch
          JOIN public.pay_batch_candidates AS batch_candidate
            ON batch_candidate.pay_batch_id = pay_batch.id
           AND batch_candidate.candidate_id = NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '')::uuid
          WHERE UPPER(BTRIM(COALESCE(pay_batch.status, ''))) = 'DRAFT'
          ORDER BY pay_batch.id
        LOOP
          BEGIN
            v_signal_result := public.banking_pay_batch_signal_touch(
              p_pay_batch_id => v_signal_batch_row.pay_batch_id,
              p_change_reason => 'CANDIDATE_PAY_METHOD_CHANGED',
              p_change_source => 'pay_workbench_mark_candidate_dirty',
              p_change_scope_json => jsonb_build_object(
                'stale_hint', true,
                'stale_reason', 'CANDIDATE_PAY_METHOD_CHANGED',
                'candidate_ids', jsonb_build_array(NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '')),
                'source_pay_method', v_route_source_method,
                'target_pay_method', v_route_target_method,
                'targeted_timesheet_ids', COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb),
                'policy_x_authority_scope', 'FROZEN_DRAFT_INVALIDATION_HINT_ONLY'
              ),
              p_touch_payment_status => false,
              p_touch_correction_progress => false,
              p_touch_alerts => true,
              p_touch_overview => true
            );
          EXCEPTION
            WHEN OTHERS THEN
              v_signal_result := jsonb_build_object(
                'ok', false,
                'error_code', SQLSTATE,
                'error_message', SQLERRM,
                'pay_batch_id', v_signal_batch_row.pay_batch_id::text
              );
              PERFORM public._temp_diag_log(
                'TEMP_TRIGGER_DIRTY_STAGE',
                'TEMP_BANKING_PAY_DIRTY',
                NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), ''),
                jsonb_build_object(
                  'function_name', 'pay_workbench_mark_candidate_dirty',
                  'stage', 'candidate_route_draft_stale_signal_failed_non_blocking',
                  'pay_batch_id', v_signal_batch_row.pay_batch_id::text,
                  'error_code', SQLSTATE,
                  'error_message', SQLERRM,
                  'policy_x_authority_scope', 'FROZEN_DRAFT_INVALIDATION_HINT_ONLY'
                )
              );
          END;
        END LOOP;

        v_explicit_banking_pay_action := true;
        v_banking_pay_dirty_required := true;
        v_ordinary_timesheet_edit_save_no_dirty := false;
        v_effective_lifecycle_context := 'candidate_pay_method_change';
      END IF;
    END IF;
  END IF;

  IF v_trigger_table IN ('bank_name_checks', 'bank_payee_map') THEN
    v_entity_kind_old := UPPER(BTRIM(COALESCE(v_old_row->>'entity_kind', '')));
    v_entity_kind_new := UPPER(BTRIM(COALESCE(v_new_row->>'entity_kind', '')));
    IF v_entity_kind_old = 'CANDIDATE' AND NULLIF(BTRIM(COALESCE(v_old_row->>'entity_id', '')), '') ~* v_uuid_re THEN
      v_candidate_ids := array_append(v_candidate_ids, NULLIF(BTRIM(COALESCE(v_old_row->>'entity_id', '')), '')::uuid);
    ELSIF v_entity_kind_old IN ('UMBRELLA', 'UMBRELLA_COMPANY') AND NULLIF(BTRIM(COALESCE(v_old_row->>'entity_id', '')), '') ~* v_uuid_re THEN
      v_umbrella_id_old := NULLIF(BTRIM(COALESCE(v_old_row->>'entity_id', '')), '')::uuid;
    END IF;
    IF v_entity_kind_new = 'CANDIDATE' AND NULLIF(BTRIM(COALESCE(v_new_row->>'entity_id', '')), '') ~* v_uuid_re THEN
      v_candidate_ids := array_append(v_candidate_ids, NULLIF(BTRIM(COALESCE(v_new_row->>'entity_id', '')), '')::uuid);
    ELSIF v_entity_kind_new IN ('UMBRELLA', 'UMBRELLA_COMPANY') AND NULLIF(BTRIM(COALESCE(v_new_row->>'entity_id', '')), '') ~* v_uuid_re THEN
      v_umbrella_id_new := NULLIF(BTRIM(COALESCE(v_new_row->>'entity_id', '')), '')::uuid;
    END IF;
  END IF;

  IF v_trigger_table = 'umbrellas' THEN
    IF NULLIF(BTRIM(COALESCE(v_old_row->>'id', '')), '') ~* v_uuid_re THEN
      v_umbrella_id_old := NULLIF(BTRIM(COALESCE(v_old_row->>'id', '')), '')::uuid;
    END IF;
    IF NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '') ~* v_uuid_re THEN
      v_umbrella_id_new := NULLIF(BTRIM(COALESCE(v_new_row->>'id', '')), '')::uuid;
    END IF;
  END IF;

  IF v_umbrella_id_old IS NOT NULL OR v_umbrella_id_new IS NOT NULL THEN
    FOR v_scope_id IN
      SELECT DISTINCT umbrella_scope.umbrella_id::text
      FROM (
        SELECT v_umbrella_id_old AS umbrella_id
        UNION ALL
        SELECT v_umbrella_id_new AS umbrella_id
      ) AS umbrella_scope
      WHERE umbrella_scope.umbrella_id IS NOT NULL
    LOOP
      v_reason := 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP;
      v_payload_json := jsonb_build_object(
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'trigger_operation', TG_OP,
        'scope_kind', 'UMBRELLA',
        'scope_id', v_scope_id,
        'reason', v_reason,
        'fallback_reason', 'UMBRELLA_BANK_DETAIL_DIRTY',
        'refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
        'source_build_required', true,
        'line_work_required', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      );
      PERFORM public.pay_workbench_dirty_event_enqueue(
        p_job_type => 'CONTRACT_CLIENT_DIRTY_FANOUT',
        p_scope_kind => 'UMBRELLA',
        p_scope_id => v_scope_id,
        p_candidate_id => NULL::uuid,
        p_targeted_timesheet_ids => ARRAY[]::uuid[],
        p_linked_timesheet_ids => ARRAY[]::uuid[],
        p_payload_json => v_payload_json,
        p_reason => v_reason,
        p_priority => -1000,
        p_run_at_utc => v_now
      );
    END LOOP;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT candidate_candidates.candidate_id ORDER BY candidate_candidates.candidate_id), ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM (
    SELECT unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS candidate_id
    UNION ALL
    SELECT contract_old.candidate_id FROM public.contracts AS contract_old WHERE contract_old.id = v_old_contract_id
    UNION ALL
    SELECT contract_new.candidate_id FROM public.contracts AS contract_new WHERE contract_new.id = v_new_contract_id
    UNION ALL
    SELECT financial_old.candidate_id FROM public.timesheets_financials AS financial_old WHERE financial_old.timesheet_id = v_old_timesheet_id AND financial_old.is_current = true
    UNION ALL
    SELECT financial_new.candidate_id FROM public.timesheets_financials AS financial_new WHERE financial_new.timesheet_id = v_new_timesheet_id AND financial_new.is_current = true
    UNION ALL
    SELECT timesheet_contract_old.candidate_id
    FROM public.timesheets AS timesheet_old
    JOIN public.contracts AS timesheet_contract_old ON timesheet_contract_old.id = timesheet_old.contract_id
    WHERE timesheet_old.timesheet_id = v_old_timesheet_id
    UNION ALL
    SELECT timesheet_contract_new.candidate_id
    FROM public.timesheets AS timesheet_new
    JOIN public.contracts AS timesheet_contract_new ON timesheet_contract_new.id = timesheet_new.contract_id
    WHERE timesheet_new.timesheet_id = v_new_timesheet_id
  ) AS candidate_candidates
  WHERE candidate_candidates.candidate_id IS NOT NULL;

  IF COALESCE(array_length(v_candidate_ids, 1), 0) = 0 THEN
    PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', COALESCE(v_new_timesheet_id::text, v_old_timesheet_id::text), jsonb_build_object('function_name', 'pay_workbench_mark_candidate_dirty', 'stage', 'return_no_candidate', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));
    IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
  END IF;

  v_reason := CASE
    WHEN v_candidate_route_change IS TRUE
      THEN 'CANDIDATE_PAY_METHOD_CHANGE:' || v_route_source_method || '_TO_' || v_route_target_method
    ELSE 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP
  END;

  FOREACH v_candidate_id IN ARRAY v_candidate_ids
  LOOP

    IF TG_OP = 'INSERT' AND v_trigger_table = 'timesheets_financials' THEN
      v_tsfin_insert_not_coalesced_reason := NULL::text;
      v_tsfin_insert_coalesced_job_id := NULL::uuid;
      v_tsfin_insert_coalesced_projection_run_id := NULL::uuid;
      v_tsfin_insert_coalesced_session_id := NULL::uuid;
      v_tsfin_insert_coalesced_source_change_seq := NULL::bigint;
      v_tsfin_insert_authorise_like := v_new_timesheet_id IS NOT NULL
        AND COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) = 1
        AND NULLIF(BTRIM(COALESCE(v_new_row->>'authorised_at_utc', '')), '') IS NOT NULL
        AND COALESCE(LOWER(BTRIM(COALESCE(v_new_row->>'is_current', 'true'))), 'true') NOT IN ('false', 'f', '0', 'no', 'n', 'off');

      IF v_tsfin_insert_authorise_like IS TRUE THEN
        SELECT
          existing_delta.id,
          CASE
            WHEN COALESCE(existing_delta.payload_json->>'projection_run_id', '') ~* v_uuid_re
              THEN (existing_delta.payload_json->>'projection_run_id')::uuid
            ELSE NULL::uuid
          END,
          existing_delta.session_id,
          CASE
            WHEN COALESCE(existing_delta.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (existing_delta.payload_json->>'source_change_seq')::bigint
            WHEN COALESCE(existing_delta.payload_json->>'source_change_sequence', '') ~ '^[0-9]{1,18}$'
              THEN (existing_delta.payload_json->>'source_change_sequence')::bigint
            ELSE NULL::bigint
          END
        INTO
          v_tsfin_insert_coalesced_job_id,
          v_tsfin_insert_coalesced_projection_run_id,
          v_tsfin_insert_coalesced_session_id,
          v_tsfin_insert_coalesced_source_change_seq
        FROM public.banking_pay_workbench_jobs AS existing_delta
        JOIN public.banking_pay_workbench_sessions AS existing_session
          ON existing_session.id = existing_delta.session_id
         AND UPPER(BTRIM(COALESCE(existing_session.status, ''))) = 'OPEN'
         AND existing_session.discarded_at_utc IS NULL
        WHERE existing_delta.candidate_id = v_candidate_id
          AND existing_delta.created_at_utc >= (v_now - INTERVAL '10 minutes')
          AND UPPER(BTRIM(COALESCE(existing_delta.status, ''))) IN ('QUEUED', 'RUNNING', 'SUCCEEDED')
          AND UPPER(BTRIM(COALESCE(existing_delta.job_type, ''))) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH', 'CANDIDATE_DELTA_REFRESH', 'DELTA_REFRESH')
          AND UPPER(BTRIM(COALESCE(existing_delta.payload_json->>'projection_class', ''))) = 'NORMAL_TIMESHEET'
          AND UPPER(BTRIM(COALESCE(existing_delta.payload_json->>'trigger_table', ''))) = 'TIMESHEETS'
          AND UPPER(BTRIM(COALESCE(existing_delta.payload_json->>'trigger_operation', existing_delta.payload_json->>'trigger_op', ''))) = 'UPDATE'
          AND UPPER(BTRIM(COALESCE(existing_delta.payload_json->>'mutation_context', existing_delta.payload_json->>'lifecycle_mutation_context', ''))) IN ('TIMESHEET_AUTHORISE', 'AUTHORISE_TIMESHEET')
          AND LOWER(BTRIM(COALESCE(existing_delta.payload_json->>'authorise_boundary_changed', existing_delta.payload_json#>>'{complexity_flags,authorise_boundary_changed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(
              CASE
                WHEN jsonb_typeof(existing_delta.payload_json->'targeted_timesheet_ids') = 'array'
                  THEN existing_delta.payload_json->'targeted_timesheet_ids'
                ELSE '[]'::jsonb
              END
            ) AS targeted_timesheet_id(value)
            WHERE targeted_timesheet_id.value ~* v_uuid_re
              AND targeted_timesheet_id.value::uuid = v_new_timesheet_id
          )
        ORDER BY existing_delta.created_at_utc DESC, existing_delta.id DESC
        LIMIT 1;

        IF v_tsfin_insert_coalesced_job_id IS NOT NULL THEN
          PERFORM public._temp_diag_log(
            'TEMP_TRIGGER_DIRTY_STAGE',
            'TEMP_BANKING_PAY_DIRTY',
            v_new_timesheet_id::text,
            jsonb_build_object(
              'function_name', 'pay_workbench_mark_candidate_dirty',
              'stage', 'early_return_timesheet_financials_insert_authorise_coalesced_no_dirty',
              'trigger_table', v_trigger_table,
              'trigger_op', TG_OP,
              'coalesced_with_timesheets_authorise_delta', true,
              'coalesced_source_change_seq', v_tsfin_insert_coalesced_source_change_seq,
              'coalesced_job_id', v_tsfin_insert_coalesced_job_id::text,
              'coalesced_projection_run_id', CASE WHEN v_tsfin_insert_coalesced_projection_run_id IS NULL THEN NULL ELSE v_tsfin_insert_coalesced_projection_run_id::text END,
              'coalesced_session_id', CASE WHEN v_tsfin_insert_coalesced_session_id IS NULL THEN NULL ELSE v_tsfin_insert_coalesced_session_id::text END,
              'coalesced_candidate_id', v_candidate_id::text,
              'coalesced_timesheet_id', v_new_timesheet_id::text,
              'banking_pay_dirty_required', false,
              'source_build_required', false,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
              'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)
            )
          );
          CONTINUE;
        END IF;

        v_tsfin_insert_not_coalesced_reason := 'NO_PRIOR_AUTHORISE_DELTA';
      ELSE
        v_tsfin_insert_not_coalesced_reason := CASE
          WHEN v_new_timesheet_id IS NULL THEN 'NO_TARGET_TIMESHEET'
          WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) <> 1 THEN 'MULTI_OR_EMPTY_TARGET_SCOPE'
          WHEN NULLIF(BTRIM(COALESCE(v_new_row->>'authorised_at_utc', '')), '') IS NULL THEN 'UNAUTHORISED_INSERT'
          WHEN COALESCE(LOWER(BTRIM(COALESCE(v_new_row->>'is_current', 'true'))), 'true') IN ('false', 'f', '0', 'no', 'n', 'off') THEN 'NON_CURRENT_INSERT'
          ELSE 'UNKNOWN_SAFE_STATE'
        END;
      END IF;
    END IF;

    IF v_effective_lifecycle_context IN ('timesheet_authorise', 'timesheet_unauthorise', 'timesheet_archive', 'timesheet_unarchive') THEN
      v_lifecycle_dedupe_token := '|LIFECYCLE_DIRTY:' || v_effective_lifecycle_context || ':' || v_candidate_id::text || ':' || COALESCE(array_to_string(v_targeted_timesheet_ids, ','), '') || '|';
      v_lifecycle_dedupe_keys := COALESCE(current_setting('cloudtms.pay_dirty_dedupe_keys', true), '');
      IF POSITION(v_lifecycle_dedupe_token IN v_lifecycle_dedupe_keys) > 0 THEN
        PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0 THEN v_targeted_timesheet_ids[1]::text ELSE NULL::text END, jsonb_build_object('function_name', 'pay_workbench_mark_candidate_dirty', 'stage', 'dedupe_skip_lifecycle_duplicate', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'mutation_context', v_lifecycle_context, 'candidate_id', v_candidate_id::text, 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));
        CONTINUE;
      END IF;
      PERFORM set_config('cloudtms.pay_dirty_dedupe_keys', LEFT(v_lifecycle_dedupe_keys || v_lifecycle_dedupe_token, 60000), true);
    END IF;

    v_payload_json := jsonb_build_object(
      'trigger_table', v_trigger_table,
      'trigger_op', TG_OP,
      'trigger_operation', TG_OP,
      'mutation_context', v_effective_lifecycle_context,
      'lifecycle_mutation_context', v_effective_lifecycle_context,
      'authorise_boundary_changed', COALESCE(v_authorise_boundary_is_authorise, false),
      'unauthorise_boundary_changed', COALESCE(v_authorise_boundary_is_unauthorise, false),
      'archive_boundary_changed', COALESCE(v_archive_boundary_is_archive, false),
      'unarchive_boundary_changed', COALESCE(v_archive_boundary_is_unarchive, false),
      'explicit_banking_pay_action', COALESCE(v_explicit_banking_pay_action, false) OR COALESCE(v_authorise_boundary_changed, false) OR COALESCE(v_archive_boundary_changed, false),
      'banking_pay_dirty_required', COALESCE(v_banking_pay_dirty_required, false),
      'ordinary_timesheet_edit_save_no_dirty', COALESCE(v_ordinary_timesheet_edit_save_no_dirty, false),
      'timesheets_financials_insert_not_coalesced_reason', CASE WHEN v_trigger_table = 'timesheets_financials' AND TG_OP = 'INSERT' THEN v_tsfin_insert_not_coalesced_reason ELSE NULL END,
      'timesheets_financials_insert_coalesced_authorise_delta', false,
      'scope_kind', 'CANDIDATE',
      'scope_id', v_candidate_id::text,
      'candidate_id', v_candidate_id::text,
      'reason', v_reason,
      'dirty_reason', v_reason,
      'timesheet_pay_state_settlement_changed', CASE WHEN v_trigger_table = 'timesheet_pay_state' THEN COALESCE(v_timesheet_pay_state_settlement_changed, false) ELSE NULL END,
      'timesheet_pay_state_summary_changed', CASE WHEN v_trigger_table = 'timesheet_pay_state' THEN COALESCE(v_timesheet_pay_state_summary_changed, false) ELSE NULL END,
      'timesheet_pay_state_bookkeeping_ignored', CASE WHEN v_trigger_table = 'timesheet_pay_state' THEN COALESCE(v_timesheet_pay_state_bookkeeping_ignored, false) ELSE NULL END,
      'timesheet_pay_state_noop_ignored', CASE WHEN v_trigger_table = 'timesheet_pay_state' THEN COALESCE(v_timesheet_pay_state_noop_ignored, false) ELSE NULL END,
      'pay_state_dirty_routing_reason', CASE WHEN v_trigger_table = 'timesheet_pay_state' THEN v_timesheet_pay_state_routing_reason ELSE NULL END,
      'source_build_required_reason', CASE WHEN v_trigger_table = 'timesheet_pay_state' THEN v_timesheet_pay_state_routing_reason ELSE NULL END,
      'refresh_scope_kind', v_refresh_scope_kind,
      'targeted_timesheet_ids', COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb),
      'linked_timesheet_ids', '[]'::jsonb,
      'linked_scope_recompute_required', true,
      'source_build_required', true,
      'line_work_required', true,
      'line_work_only', false,
      'line_work_action', 'SOURCE_BUILD',
      'force_legacy', false,
      'force_broad_legacy', false,
      'projection_class', CASE
        WHEN v_candidate_route_change IS TRUE THEN 'PAYE_UMBRELLA_SWITCH'
        WHEN v_trigger_table IN ('timesheets', 'timesheets_financials')
             AND (v_authorise_boundary_changed IS TRUE OR v_archive_boundary_changed IS TRUE) THEN 'TIMESHEET_LIFECYCLE'
        ELSE 'DIRTY_TRIGGER'
      END,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );

    IF v_candidate_route_change IS TRUE THEN
      v_payload_json := v_payload_json
        || jsonb_strip_nulls(
          jsonb_build_object(
            'candidate_pay_method_change', true,
            'candidate_payment_status_changed', true,
            'pay_method_changed', true,
            'prospective_only', true,
            'old_pay_method', v_route_source_method,
            'new_pay_method', v_route_target_method,
            'route_change_operation_id', v_route_operation_id,
            'route_change_actor_user_id', v_route_actor_user_id,
            'actor_user_id', v_route_actor_user_id,
            'route_change_reason', COALESCE(v_route_reason, 'PAY_METHOD_CHANGE'),
            'route_change_source_method', v_route_source_method,
            'route_change_target_method', v_route_target_method,
            'authorised_timesheet_ids', v_route_authorised_timesheet_ids,
            'authorised_timesheet_count', v_route_authorised_timesheet_count,
            'active_advance_timesheet_ids', v_route_active_advance_timesheet_ids,
            'active_advance_timesheet_count', v_route_active_advance_timesheet_count,
            'retained_finance_timesheet_ids', v_route_retained_finance_timesheet_ids,
            'retained_finance_timesheet_count', v_route_retained_finance_timesheet_count,
            'source_target_mismatch_count', v_route_source_target_mismatch_count,
            'authoritative_sessions', v_route_authoritative_sessions,
            'replaced_source_session_ids', v_route_replaced_source_session_ids,
            'target_details', v_route_target_details,
            'coverage_basis', COALESCE(v_route_scope_result->>'coverage_basis', 'CANONICAL_TIMESHEETS_WITH_RETAINED_FINANCE_AUTHORITY'),
            'coverage_complete', v_route_coverage_complete,
            'exact_target_scope', v_route_exact_scope,
            'targeted_scope_is_empty', v_route_targeted_scope_is_empty
          )
          || jsonb_build_object(
            'contracts_changed', 0,
            'contract_weeks_changed', 0,
            'timesheets_changed', 0,
            'rates_changed', 0,
            'tsfin_repricing_rows', 0,
            'source_records_mutated', false,
            'economic_truth_mutation_allowed', false,
            'refresh_completion_requires_terminal_exact_scope', true,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
            'policy_x_dirtying_only', true
          )
        );
    END IF;

    IF v_candidate_route_change IS TRUE THEN
      v_enqueue_result := jsonb_build_object(
        'ok', true,
        'job_id', v_route_job_id::text,
        'job_type', 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        'status', v_route_job_status,
        'dedupe_key', v_route_dedupe_key,
        'scope_kind', 'CANDIDATE',
        'scope_id', v_candidate_id::text,
        'candidate_id', v_candidate_id::text,
        'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0),
        'latest_source_change_seq', v_route_job_source_change_seq,
        'exact_target_scope', true,
        'targeted_scope_is_empty', v_route_targeted_scope_is_empty,
        'durable_scope_persisted', true
      );
    ELSE
      SELECT public.pay_workbench_dirty_event_enqueue(
        p_job_type => 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        p_scope_kind => 'CANDIDATE',
        p_scope_id => v_candidate_id::text,
        p_candidate_id => v_candidate_id,
        p_targeted_timesheet_ids => v_targeted_timesheet_ids,
        p_linked_timesheet_ids => ARRAY[]::uuid[],
        p_payload_json => v_payload_json,
        p_reason => v_reason,
        p_priority => -1000,
        p_run_at_utc => v_now
      )
      INTO v_enqueue_result;
    END IF;

    v_jobs_queued := v_jobs_queued + 1;
  END LOOP;

  PERFORM public._temp_diag_log(
    'TEMP_TRIGGER_DIRTY_STAGE',
    'TEMP_BANKING_PAY_DIRTY',
    CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0 THEN v_targeted_timesheet_ids[1]::text ELSE NULL::text END,
    jsonb_build_object(
      'function_name', 'pay_workbench_mark_candidate_dirty',
      'stage', CASE
        WHEN v_jobs_queued = 0 AND TG_OP = 'INSERT' AND v_trigger_table = 'timesheets_financials' THEN 'return_no_dirty_after_timesheet_financials_insert_coalesced'
        ELSE 'return_enqueued_dirty_priority'
      END,
      'trigger_table', v_trigger_table,
      'trigger_op', TG_OP,
      'mutation_context', v_lifecycle_context,
      'candidate_count', COALESCE(array_length(v_candidate_ids, 1), 0),
      'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0),
      'jobs_queued', v_jobs_queued,
      'queue_class', 'DIRTY_TRIGGER_PRIORITY',
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)
    )
  );

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$function$;

ALTER FUNCTION public.pay_workbench_mark_candidate_dirty() OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_mark_candidate_dirty() FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_mark_candidate_dirty() TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_mark_candidate_dirty() TO service_role;
