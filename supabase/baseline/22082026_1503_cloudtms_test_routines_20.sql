-- Immutable CloudTMS TEST function snapshot, page 20.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- tsfin_work_success(uuid)
CREATE OR REPLACE FUNCTION public.tsfin_work_success(p_id uuid)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
  DELETE FROM public.ts_financials_outbox t
  WHERE t.id = p_id;
END;
$function$;

-- tsfin_write_current_snapshot_single_bounded(uuid,integer,jsonb,uuid,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.tsfin_write_current_snapshot_single_bounded(p_timesheet_id uuid, p_timesheet_version integer DEFAULT NULL::integer, p_snapshot_json jsonb DEFAULT '{}'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  snap jsonb := COALESCE(p_snapshot_json, '{}'::jsonb);
  prev public.timesheets_financials%ROWTYPE;
  inserted_row public.timesheets_financials%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_prev_id uuid := NULL;
  did_prepare boolean := false;

  v_ib jsonb := '{}'::jsonb;
  v_bad integer := 0;

  v_cat_pay numeric := 0;
  v_cat_charge numeric := 0;
  v_fallback_exp_pay numeric := 0;
  v_fallback_exp_charge numeric := 0;
  v_exp_pay numeric := 0;
  v_exp_charge numeric := 0;

  v_add_units_json jsonb := '{}'::jsonb;
  v_add_pay numeric := 0;
  v_add_charge numeric := 0;
  v_mil_pay numeric := 0;
  v_mil_charge numeric := 0;

  v_nonseg_pay numeric := 0;
  v_nonseg_charge numeric := 0;
  v_nonseg_margin numeric := 0;
  v_core_pay numeric := 0;
  v_core_charge numeric := 0;
  v_total_pay numeric := 0;
  v_total_charge numeric := 0;
  v_margin numeric := 0;

  v_add_obj jsonb := '{}'::jsonb;
  v_tot_obj jsonb := '{}'::jsonb;
  v_mode text := NULL;
  v_seg jsonb := NULL;
  v_exclude boolean := false;

  v_policy jsonb := '{}'::jsonb;
  v_apply_to text := 'PAYE_ONLY';
  v_erni_pct_raw numeric := 0;
  v_erni_mult numeric := 1;
  v_pay_method_u text := NULL;
  v_erni_applies boolean := false;
  v_wage_pay numeric := 0;
  v_wage_pay_cost numeric := 0;
  v_reimb_pay numeric := 0;
  v_pay_cost numeric := 0;
  v_nonseg_wage_pay numeric := 0;
  v_nonseg_wage_pay_cost numeric := 0;
  v_nonseg_reimb_pay numeric := 0;
  v_nonseg_pay_cost numeric := 0;

  v_processing_status public.ts_fin_processing_status_enum := 'UNASSIGNED'::public.ts_fin_processing_status_enum;
  v_candidate_assignment public.candidate_assignment_enum := 'UNASSIGNED'::public.candidate_assignment_enum;
  v_basis public.timesheet_fin_basis_enum := 'SELF_REPORTED'::public.timesheet_fin_basis_enum;
  v_timesheet_version integer := 1;
  v_is_stale boolean := false;
  v_stale_reason text := NULL;
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_pay_method_insert text := NULL;
  v_mileage_units numeric := 0;
  v_error_state text := NULL;
  v_error_message text := NULL;

  v_live_contract_week_id uuid := NULL::uuid;
  v_live_contract_week_status text := NULL::text;
  v_incoming_processing_status text := NULL::text;
  v_incoming_authorised_at_utc timestamptz := NULL::timestamptz;
  v_result_authorised_at_utc timestamptz := NULL::timestamptz;
  v_result_authorised_by_user_id uuid := NULL::uuid;
  v_live_authorised boolean := false;
  v_can_preserve_authorised_at boolean := false;
  v_lifecycle_decision text := 'WRITTEN';
BEGIN
  perform public._ctms_assert_tsfin_snapshot_policy_v1(p_timesheet_id, coalesce(p_snapshot_json, '{}'::jsonb));
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
  END IF;

  IF p_snapshot_json IS NOT NULL AND jsonb_typeof(p_snapshot_json) <> 'object' THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_snapshot_json')::text;
  END IF;

  SELECT ts.*
    INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(snap ->> 'timesheet_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(snap ->> 'timesheet_id', '')), '')::uuid IS DISTINCT FROM p_timesheet_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH',
      DETAIL = jsonb_build_object(
        'field', 'timesheet_id',
        'expected_value', p_timesheet_id::text,
        'supplied_value', snap ->> 'timesheet_id'
      )::text;
  END IF;

  snap := snap || jsonb_build_object(
    'timesheet_id', p_timesheet_id::text,
    'timesheet_version', COALESCE(p_timesheet_version, NULLIF(snap ->> 'timesheet_version', '')::integer, v_current_ts.version, 1)
  );

  SELECT tf.*
    INTO prev
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = p_timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF prev.id IS NOT NULL AND prev.paid_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_PAID', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id, 'timesheet_financials_id', prev.id)::text;
  END IF;

  IF prev.id IS NOT NULL AND prev.locked_by_invoice_id IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id, 'invoice_id', prev.locked_by_invoice_id)::text;
  END IF;

  v_prev_id := prev.id;

  v_timesheet_version := COALESCE(NULLIF(snap ->> 'timesheet_version', '')::integer, v_current_ts.version, 1);
  v_basis := COALESCE(NULLIF(snap ->> 'basis', '')::public.timesheet_fin_basis_enum, 'SELF_REPORTED'::public.timesheet_fin_basis_enum);
  v_candidate_assignment := COALESCE(NULLIF(snap ->> 'candidate_assignment', '')::public.candidate_assignment_enum, 'UNASSIGNED'::public.candidate_assignment_enum);
  v_processing_status := COALESCE(NULLIF(snap ->> 'processing_status', '')::public.ts_fin_processing_status_enum, 'UNASSIGNED'::public.ts_fin_processing_status_enum);

  v_ib := COALESCE(snap -> 'invoice_breakdown_json', '{}'::jsonb);
  IF v_ib IS NULL OR jsonb_typeof(v_ib) <> 'object' THEN
    v_ib := '{}'::jsonb;
  END IF;

  v_bad := public._tsfin_invalid_segment_count(v_ib);
  IF v_bad > 0 THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH', DETAIL = jsonb_build_object('field', 'invoice_breakdown_json', 'invalid_segment_count', v_bad)::text;
  END IF;

  SELECT cw.id,
         cw.status::text
    INTO v_live_contract_week_id,
         v_live_contract_week_status
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = p_timesheet_id
  ORDER BY cw.updated_at DESC NULLS LAST,
           cw.created_at DESC NULLS LAST,
           cw.id DESC
  LIMIT 1
  FOR UPDATE;

  v_incoming_processing_status := CASE
    WHEN v_processing_status IS NULL THEN NULL::text
    ELSE v_processing_status::text
  END;
  v_incoming_authorised_at_utc := NULLIF(snap ->> 'authorised_at_utc', '')::timestamptz;
  v_result_authorised_at_utc := COALESCE(v_incoming_authorised_at_utc, prev.authorised_at_utc);
  v_result_authorised_by_user_id := COALESCE(
    NULLIF(snap ->> 'authorised_by_user_id', '')::uuid,
    prev.authorised_by_user_id
  );
  v_live_authorised := (
       v_current_ts.authorised_at_server IS NOT NULL
    OR UPPER(BTRIM(COALESCE(v_live_contract_week_status, ''))) = 'AUTHORISED'
    OR prev.authorised_at_utc IS NOT NULL
    OR prev.processing_status IN (
         'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
         'READY_FOR_HR'::public.ts_fin_processing_status_enum
       )
  );

  IF v_live_authorised
     AND (
          v_processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum
       OR v_processing_status NOT IN (
            'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
            'READY_FOR_HR'::public.ts_fin_processing_status_enum
          )
       OR v_result_authorised_at_utc IS NULL
     ) THEN
    v_can_preserve_authorised_at := COALESCE(
      prev.authorised_at_utc,
      v_current_ts.authorised_at_server,
      v_incoming_authorised_at_utc
    ) IS NOT NULL;

    IF NOT v_can_preserve_authorised_at THEN
      v_lifecycle_decision := 'REJECTED';
      PERFORM public._temp_diag_log(
        'TSFIN_SNAPSHOT_LIFECYCLE_DOWNGRADE_REJECTED',
        'TEMP_TIMESHEET_LIFECYCLE',
        p_timesheet_id::text,
        jsonb_strip_nulls(
          jsonb_build_object(
            'function_name', 'tsfin_write_current_snapshot_single_bounded',
            'timesheet_id', p_timesheet_id::text,
            'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
            'incoming_processing_status', v_incoming_processing_status,
            'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
            'live_timesheet_authorised_at_server_present', v_current_ts.authorised_at_server IS NOT NULL,
            'live_contract_week_status', v_live_contract_week_status,
            'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
            'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
            'decision', v_lifecycle_decision,
            'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
            'result_authorised_at_utc_present', false,
            'rejection_reason', 'LIVE_AUTHORISED_STATE_WITHOUT_AUTHORISATION_TIMESTAMP',
            'policy_x_boundary', 'LIFECYCLE_FIELDS_ONLY_NO_ECONOMICS_CHANGE'
          )
        )
      );
      RAISE EXCEPTION USING
        ERRCODE = '40001',
        MESSAGE = 'TSFIN_SNAPSHOT_LIFECYCLE_DOWNGRADE_REJECTED',
        DETAIL = jsonb_build_object(
          'timesheet_id', p_timesheet_id::text,
          'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
          'incoming_processing_status', v_incoming_processing_status,
          'live_contract_week_status', v_live_contract_week_status,
          'reason', 'LIVE_AUTHORISED_STATE_WITHOUT_AUTHORISATION_TIMESTAMP'
        )::text;
    END IF;

    v_processing_status := CASE
      WHEN prev.processing_status IN (
             'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
             'READY_FOR_HR'::public.ts_fin_processing_status_enum
           )
        THEN prev.processing_status
      WHEN v_processing_status IN (
             'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
             'READY_FOR_HR'::public.ts_fin_processing_status_enum
           )
        THEN v_processing_status
      ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    END;
    v_result_authorised_at_utc := COALESCE(
      prev.authorised_at_utc,
      v_current_ts.authorised_at_server,
      v_incoming_authorised_at_utc
    );
    v_result_authorised_by_user_id := COALESCE(
      prev.authorised_by_user_id,
      v_result_authorised_by_user_id,
      p_actor_user_id
    );
    v_lifecycle_decision := 'PRESERVED';
  END IF;

  PERFORM public._temp_diag_log(
    'TSFIN_SNAPSHOT_LIVE_LIFECYCLE_GUARD',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_strip_nulls(
      jsonb_build_object(
        'function_name', 'tsfin_write_current_snapshot_single_bounded',
        'timesheet_id', p_timesheet_id::text,
        'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
        'incoming_processing_status', v_incoming_processing_status,
        'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
        'live_timesheet_authorised_at_server_present', v_current_ts.authorised_at_server IS NOT NULL,
        'live_contract_week_id', CASE WHEN v_live_contract_week_id IS NULL THEN NULL::text ELSE v_live_contract_week_id::text END,
        'live_contract_week_status', v_live_contract_week_status
      )
      || jsonb_build_object(
        'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
        'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
        'decision', v_lifecycle_decision,
        'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
        'result_authorised_at_utc_present', v_result_authorised_at_utc IS NOT NULL,
        'policy_x_boundary', 'LIFECYCLE_FIELDS_ONLY_NO_ECONOMICS_CHANGE'
      )
    )
  );

  IF v_lifecycle_decision = 'PRESERVED' THEN
    PERFORM public._temp_diag_log(
      'TSFIN_SNAPSHOT_LIFECYCLE_STATUS_PRESERVED',
      'TEMP_TIMESHEET_LIFECYCLE',
      p_timesheet_id::text,
      jsonb_strip_nulls(
        jsonb_build_object(
          'function_name', 'tsfin_write_current_snapshot_single_bounded',
          'timesheet_id', p_timesheet_id::text,
          'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
          'incoming_processing_status', v_incoming_processing_status,
          'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
          'live_timesheet_authorised_at_server_present', v_current_ts.authorised_at_server IS NOT NULL,
          'live_contract_week_status', v_live_contract_week_status,
          'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
          'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
          'decision', v_lifecycle_decision,
          'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
          'result_authorised_at_utc_present', v_result_authorised_at_utc IS NOT NULL
        )
      )
    );
  END IF;

  PERFORM public.tsfin_prepare_write(p_timesheet_id);
  did_prepare := true;

  v_cat_pay :=
      COALESCE(NULLIF(snap ->> 'travel_pay_ex_vat', '')::numeric, prev.travel_pay_ex_vat, 0)
    + COALESCE(NULLIF(snap ->> 'accommodation_pay_ex_vat', '')::numeric, prev.accommodation_pay_ex_vat, 0)
    + COALESCE(NULLIF(snap ->> 'other_pay_ex_vat', '')::numeric, prev.other_pay_ex_vat, 0);

  v_cat_charge :=
      COALESCE(NULLIF(snap ->> 'travel_charge_ex_vat', '')::numeric, prev.travel_charge_ex_vat, 0)
    + COALESCE(NULLIF(snap ->> 'accommodation_charge_ex_vat', '')::numeric, prev.accommodation_charge_ex_vat, 0)
    + COALESCE(NULLIF(snap ->> 'other_charge_ex_vat', '')::numeric, prev.other_charge_ex_vat, 0);

  v_fallback_exp_pay := COALESCE(NULLIF(snap ->> 'expenses_pay_ex_vat', '')::numeric, prev.expenses_pay_ex_vat, 0);
  v_fallback_exp_charge := COALESCE(NULLIF(snap ->> 'expenses_charge_ex_vat', '')::numeric, prev.expenses_charge_ex_vat, 0);
  v_exp_pay := CASE WHEN COALESCE(v_cat_pay, 0) <> 0 THEN v_cat_pay ELSE v_fallback_exp_pay END;
  v_exp_charge := CASE WHEN COALESCE(v_cat_charge, 0) <> 0 THEN v_cat_charge ELSE v_fallback_exp_charge END;
  v_exp_pay := round(COALESCE(v_exp_pay, 0), 2);
  v_exp_charge := round(COALESCE(v_exp_charge, 0), 2);

  v_add_units_json := COALESCE(snap -> 'additional_units_json', prev.additional_units_json, '{}'::jsonb);
  v_add_pay := round(COALESCE(NULLIF(snap ->> 'additional_pay_ex_vat', '')::numeric, prev.additional_pay_ex_vat, 0), 2);
  v_add_charge := round(COALESCE(NULLIF(snap ->> 'additional_charge_ex_vat', '')::numeric, prev.additional_charge_ex_vat, 0), 2);
  v_mil_pay := round(COALESCE(NULLIF(snap ->> 'mileage_pay_ex_vat', '')::numeric, prev.mileage_pay_ex_vat, 0), 2);
  v_mil_charge := round(COALESCE(NULLIF(snap ->> 'mileage_charge_ex_vat', '')::numeric, prev.mileage_charge_ex_vat, 0), 2);

  v_nonseg_pay := round(v_add_pay + v_exp_pay + v_mil_pay, 2);
  v_nonseg_charge := round(v_add_charge + v_exp_charge + v_mil_charge, 2);

  v_core_pay := 0;
  v_core_charge := 0;
  v_mode := UPPER(COALESCE(v_ib ->> 'mode', ''));

  IF v_mode = 'SEGMENTS' AND jsonb_typeof(v_ib -> 'segments') = 'array' THEN
    FOR v_seg IN
      SELECT segment_value.value
      FROM jsonb_array_elements(v_ib -> 'segments') AS segment_value(value)
    LOOP
      IF v_seg IS NULL OR jsonb_typeof(v_seg) <> 'object' THEN
        CONTINUE;
      END IF;

      BEGIN
        v_core_charge := v_core_charge + COALESCE(NULLIF(BTRIM(COALESCE(v_seg ->> 'charge_amount', '')), '')::numeric, 0);
      EXCEPTION WHEN OTHERS THEN
        NULL;
      END;

      v_exclude := false;
      BEGIN
        v_exclude := COALESCE(NULLIF(BTRIM(COALESCE(v_seg ->> 'exclude_from_pay', '')), '')::boolean, false);
      EXCEPTION WHEN OTHERS THEN
        v_exclude := false;
      END;

      IF NOT v_exclude THEN
        BEGIN
          v_core_pay := v_core_pay + COALESCE(NULLIF(BTRIM(COALESCE(v_seg ->> 'pay_amount', '')), '')::numeric, 0);
        EXCEPTION WHEN OTHERS THEN
          NULL;
        END;
      END IF;
    END LOOP;
  ELSE
    BEGIN
      v_core_pay := COALESCE(NULLIF(BTRIM(COALESCE(v_ib #>> '{base_hours,pay_ex_vat}', '')), '')::numeric, 0);
    EXCEPTION WHEN OTHERS THEN
      v_core_pay := 0;
    END;

    BEGIN
      v_core_charge := COALESCE(NULLIF(BTRIM(COALESCE(v_ib #>> '{base_hours,charge_ex_vat}', '')), '')::numeric, 0);
    EXCEPTION WHEN OTHERS THEN
      v_core_charge := 0;
    END;
  END IF;

  v_core_pay := round(COALESCE(v_core_pay, 0), 2);
  v_core_charge := round(COALESCE(v_core_charge, 0), 2);
  v_total_pay := round(v_core_pay + v_nonseg_pay, 2);
  v_total_charge := round(v_core_charge + v_nonseg_charge, 2);

  v_policy := COALESCE(snap -> 'policy_snapshot_json', prev.policy_snapshot_json, '{}'::jsonb);
  IF v_policy IS NULL OR jsonb_typeof(v_policy) <> 'object' THEN
    v_policy := '{}'::jsonb;
  END IF;

  v_apply_to := UPPER(COALESCE(NULLIF(BTRIM(COALESCE(v_policy ->> 'apply_erni_to', '')), ''), 'PAYE_ONLY'));
  BEGIN
    v_erni_pct_raw := COALESCE(NULLIF(BTRIM(COALESCE(v_policy ->> 'erni_pct', '')), '')::numeric, 0);
  EXCEPTION WHEN OTHERS THEN
    v_erni_pct_raw := 0;
  END;

  v_erni_mult := 1;
  IF COALESCE(v_erni_pct_raw, 0) > 0 THEN
    IF v_erni_pct_raw > 1 THEN
      v_erni_mult := 1 + (v_erni_pct_raw / 100);
    ELSE
      v_erni_mult := 1 + v_erni_pct_raw;
    END IF;
  END IF;

  v_pay_method_u := UPPER(COALESCE(NULLIF(BTRIM(COALESCE(snap ->> 'pay_method', '')), ''), NULLIF(BTRIM(COALESCE(prev.pay_method::text, '')), ''), ''));
  IF v_pay_method_u IN ('PAYE', 'UMBRELLA') THEN
    v_pay_method_insert := v_pay_method_u;
  ELSE
    v_pay_method_insert := NULL;
  END IF;
  v_erni_applies := (v_pay_method_insert = 'PAYE') AND (v_apply_to = 'ALL' OR v_apply_to = 'PAYE_ONLY');

  v_wage_pay := round(v_core_pay + v_add_pay, 2);
  v_reimb_pay := round(v_exp_pay + v_mil_pay, 2);
  v_wage_pay_cost := v_wage_pay;
  IF v_erni_applies THEN
    v_wage_pay_cost := round(v_wage_pay * v_erni_mult, 2);
  END IF;
  v_pay_cost := round(v_wage_pay_cost + v_reimb_pay, 2);
  v_margin := round(v_total_charge - v_pay_cost, 2);

  v_nonseg_wage_pay := v_add_pay;
  v_nonseg_reimb_pay := round(v_exp_pay + v_mil_pay, 2);
  v_nonseg_wage_pay_cost := v_nonseg_wage_pay;
  IF v_erni_applies THEN
    v_nonseg_wage_pay_cost := round(v_nonseg_wage_pay * v_erni_mult, 2);
  END IF;
  v_nonseg_pay_cost := round(v_nonseg_wage_pay_cost + v_nonseg_reimb_pay, 2);
  v_nonseg_margin := round(v_nonseg_charge - v_nonseg_pay_cost, 2);

  v_add_obj := CASE WHEN v_ib ? 'additional' AND jsonb_typeof(v_ib -> 'additional') = 'object' THEN v_ib -> 'additional' ELSE '{}'::jsonb END;
  IF NOT (v_add_obj ? 'units') OR jsonb_typeof(v_add_obj -> 'units') <> 'object' THEN
    IF v_add_units_json IS NULL OR jsonb_typeof(v_add_units_json) <> 'object' THEN
      v_add_units_json := '{}'::jsonb;
    END IF;
    v_add_obj := jsonb_set(v_add_obj, '{units}', v_add_units_json, true);
  END IF;
  v_add_obj := jsonb_set(v_add_obj, '{pay_ex_vat}', to_jsonb(v_nonseg_pay), true);
  v_add_obj := jsonb_set(v_add_obj, '{charge_ex_vat}', to_jsonb(v_nonseg_charge), true);
  v_add_obj := jsonb_set(v_add_obj, '{margin_ex_vat}', to_jsonb(v_nonseg_margin), true);
  v_ib := jsonb_set(v_ib, '{additional}', v_add_obj, true);

  v_tot_obj := CASE WHEN v_ib ? 'totals' AND jsonb_typeof(v_ib -> 'totals') = 'object' THEN v_ib -> 'totals' ELSE '{}'::jsonb END;
  v_tot_obj := jsonb_set(v_tot_obj, '{total_pay_ex_vat}', to_jsonb(v_total_pay), true);
  v_tot_obj := jsonb_set(v_tot_obj, '{total_charge_ex_vat}', to_jsonb(v_total_charge), true);
  v_tot_obj := jsonb_set(v_tot_obj, '{margin_ex_vat}', to_jsonb(v_margin), true);
  v_ib := jsonb_set(v_ib, '{totals}', v_tot_obj, true);

  v_is_stale := COALESCE(NULLIF(snap ->> 'is_stale', '')::boolean, false);
  v_stale_reason := NULLIF(snap ->> 'stale_reason', '');
  IF v_processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum THEN
    v_is_stale := false;
    v_stale_reason := NULL;
  END IF;

  v_mileage_units := GREATEST(COALESCE(NULLIF(snap ->> 'mileage_units', '')::numeric, prev.mileage_units, 0), 0);

  INSERT INTO public.timesheets_financials (
    timesheet_id,
    timesheet_version,
    basis,
    is_current,
    is_stale,
    stale_reason,
    worked_start_iso,
    worked_end_iso,
    break_start_iso,
    break_end_iso,
    break_minutes,
    candidate_id,
    client_id,
    role,
    band,
    pay_method,
    policy_snapshot_json,
    rate_source_refs_json,
    hours_day,
    hours_night,
    hours_sat,
    hours_sun,
    hours_bh,
    pay_day,
    pay_night,
    pay_sat,
    pay_sun,
    pay_bh,
    charge_day,
    charge_night,
    charge_sat,
    charge_sun,
    charge_bh,
    total_hours,
    total_pay_ex_vat,
    total_charge_ex_vat,
    margin_ex_vat,
    computed_at_utc,
    occupant_key_norm,
    candidate_assignment,
    processing_status,
    expenses_pay_ex_vat,
    expenses_charge_ex_vat,
    expenses_description,
    expenses_evidence_r2_key,
    expenses_evidence_manifest,
    travel_pay_ex_vat,
    travel_charge_ex_vat,
    accommodation_pay_ex_vat,
    accommodation_charge_ex_vat,
    other_pay_ex_vat,
    other_charge_ex_vat,
    mileage_pay_ex_vat,
    mileage_charge_ex_vat,
    mileage_units,
    mileage_evidence_r2_key,
    mileage_evidence_manifest,
    mileage_pay_rate,
    mileage_charge_rate,
    po_number,
    pay_on_hold,
    pay_on_hold_reason,
    pay_on_hold_since_utc,
    pay_wtr_rate_pct_snapshot,
    pay_vat_rate_pct_snapshot,
    pay_vat_amount_snapshot,
    pay_total_inc_vat_snapshot,
    payment_reference,
    remittance_last_sent_at_utc,
    remittance_send_count,
    processed_by_user_id,
    processed_at_utc,
    authorised_by_user_id,
    authorised_at_utc,
    additional_units_json,
    additional_pay_ex_vat,
    additional_charge_ex_vat,
    additional_margin_ex_vat,
    invoice_breakdown_json,
    nhsp_import_id,
    has_rate_issue,
    has_pay_channel_issue,
    hr_crosscheck_status,
    hr_crosscheck_issues,
    external_source_rows_json,
    actual_schedule_json,
    actual_minutes_by_day_json
  )
  VALUES (
    p_timesheet_id,
    v_timesheet_version,
    v_basis,
    true,
    v_is_stale,
    v_stale_reason,
    NULLIF(snap ->> 'worked_start_iso', '')::timestamptz,
    NULLIF(snap ->> 'worked_end_iso', '')::timestamptz,
    NULLIF(snap ->> 'break_start_iso', '')::timestamptz,
    NULLIF(snap ->> 'break_end_iso', '')::timestamptz,
    NULLIF(snap ->> 'break_minutes', '')::integer,
    NULLIF(snap ->> 'candidate_id', '')::uuid,
    NULLIF(snap ->> 'client_id', '')::uuid,
    NULLIF(snap ->> 'role', ''),
    NULLIF(snap ->> 'band', ''),
    v_pay_method_insert,
    COALESCE(snap -> 'policy_snapshot_json', '{}'::jsonb),
    COALESCE(snap -> 'rate_source_refs_json', '{}'::jsonb),
    COALESCE(NULLIF(snap ->> 'hours_day', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'hours_night', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'hours_sat', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'hours_sun', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'hours_bh', '')::numeric, 0),
    NULLIF(snap ->> 'pay_day', '')::numeric,
    NULLIF(snap ->> 'pay_night', '')::numeric,
    NULLIF(snap ->> 'pay_sat', '')::numeric,
    NULLIF(snap ->> 'pay_sun', '')::numeric,
    NULLIF(snap ->> 'pay_bh', '')::numeric,
    NULLIF(snap ->> 'charge_day', '')::numeric,
    NULLIF(snap ->> 'charge_night', '')::numeric,
    NULLIF(snap ->> 'charge_sat', '')::numeric,
    NULLIF(snap ->> 'charge_sun', '')::numeric,
    NULLIF(snap ->> 'charge_bh', '')::numeric,
    COALESCE(NULLIF(snap ->> 'total_hours', '')::numeric, 0),
    COALESCE(v_total_pay, 0),
    COALESCE(v_total_charge, 0),
    COALESCE(v_margin, 0),
    v_now,
    NULLIF(snap ->> 'occupant_key_norm', ''),
    v_candidate_assignment,
    v_processing_status,
    COALESCE(v_exp_pay, 0),
    COALESCE(v_exp_charge, 0),
    NULLIF(snap ->> 'expenses_description', ''),
    NULLIF(snap ->> 'expenses_evidence_r2_key', ''),
    COALESCE(snap -> 'expenses_evidence_manifest', prev.expenses_evidence_manifest),
    COALESCE(NULLIF(snap ->> 'travel_pay_ex_vat', '')::numeric, prev.travel_pay_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'travel_charge_ex_vat', '')::numeric, prev.travel_charge_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'accommodation_pay_ex_vat', '')::numeric, prev.accommodation_pay_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'accommodation_charge_ex_vat', '')::numeric, prev.accommodation_charge_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'other_pay_ex_vat', '')::numeric, prev.other_pay_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'other_charge_ex_vat', '')::numeric, prev.other_charge_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'mileage_pay_ex_vat', '')::numeric, prev.mileage_pay_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'mileage_charge_ex_vat', '')::numeric, prev.mileage_charge_ex_vat, 0),
    v_mileage_units,
    NULLIF(snap ->> 'mileage_evidence_r2_key', ''),
    COALESCE(snap -> 'mileage_evidence_manifest', prev.mileage_evidence_manifest),
    NULLIF(snap ->> 'mileage_pay_rate', '')::numeric,
    NULLIF(snap ->> 'mileage_charge_rate', '')::numeric,
    COALESCE(NULLIF(snap ->> 'po_number', ''), prev.po_number),
    COALESCE(NULLIF(snap ->> 'pay_on_hold', '')::boolean, prev.pay_on_hold, false),
    COALESCE(NULLIF(snap ->> 'pay_on_hold_reason', ''), prev.pay_on_hold_reason),
    COALESCE(NULLIF(snap ->> 'pay_on_hold_since_utc', '')::timestamptz, prev.pay_on_hold_since_utc),
    NULLIF(snap ->> 'pay_wtr_rate_pct_snapshot', '')::numeric,
    NULLIF(snap ->> 'pay_vat_rate_pct_snapshot', '')::numeric,
    COALESCE(NULLIF(snap ->> 'pay_vat_amount_snapshot', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'pay_total_inc_vat_snapshot', '')::numeric, 0),
    COALESCE(NULLIF(snap ->> 'payment_reference', ''), prev.payment_reference),
    COALESCE(NULLIF(snap ->> 'remittance_last_sent_at_utc', '')::timestamptz, prev.remittance_last_sent_at_utc),
    COALESCE(NULLIF(snap ->> 'remittance_send_count', '')::integer, prev.remittance_send_count, 0),
    COALESCE(NULLIF(snap ->> 'processed_by_user_id', '')::uuid, prev.processed_by_user_id, p_actor_user_id),
    COALESCE(NULLIF(snap ->> 'processed_at_utc', '')::timestamptz, prev.processed_at_utc, v_now),
    v_result_authorised_by_user_id,
    v_result_authorised_at_utc,
    COALESCE(snap -> 'additional_units_json', prev.additional_units_json, '{}'::jsonb),
    COALESCE(NULLIF(snap ->> 'additional_pay_ex_vat', '')::numeric, prev.additional_pay_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'additional_charge_ex_vat', '')::numeric, prev.additional_charge_ex_vat, 0),
    COALESCE(NULLIF(snap ->> 'additional_margin_ex_vat', '')::numeric, prev.additional_margin_ex_vat, 0),
    v_ib,
    NULLIF(snap ->> 'nhsp_import_id', '')::uuid,
    COALESCE(NULLIF(snap ->> 'has_rate_issue', '')::boolean, false),
    COALESCE(NULLIF(snap ->> 'has_pay_channel_issue', '')::boolean, false),
    COALESCE(NULLIF(snap ->> 'hr_crosscheck_status', ''), prev.hr_crosscheck_status),
    COALESCE(
      CASE
        WHEN snap ? 'hr_crosscheck_issues' AND jsonb_typeof(snap -> 'hr_crosscheck_issues') = 'array'
          THEN ARRAY(SELECT jsonb_array_elements_text(snap -> 'hr_crosscheck_issues'))
        ELSE NULL
      END,
      prev.hr_crosscheck_issues
    ),
    COALESCE(snap -> 'external_source_rows_json', prev.external_source_rows_json),
    COALESCE(snap -> 'actual_schedule_json', prev.actual_schedule_json),
    COALESCE(snap -> 'actual_minutes_by_day_json', prev.actual_minutes_by_day_json)
  )
  RETURNING * INTO inserted_row;

  RETURN jsonb_build_object(
    'ok', true,
    'timesheet_id', inserted_row.timesheet_id,
    'timesheet_financials_id', inserted_row.id,
    'processing_status', inserted_row.processing_status::text,
    'timesheet_version', inserted_row.timesheet_version,
    'updated_at', inserted_row.updated_at,
    'computed_at_utc', inserted_row.computed_at_utc,
    'version_marker', md5(jsonb_build_object(
      'id', inserted_row.id,
      'timesheet_id', inserted_row.timesheet_id,
      'timesheet_version', inserted_row.timesheet_version,
      'processing_status', inserted_row.processing_status::text,
      'updated_at', inserted_row.updated_at,
      'computed_at_utc', inserted_row.computed_at_utc,
      'total_hours', inserted_row.total_hours,
      'total_pay_ex_vat', inserted_row.total_pay_ex_vat,
      'total_charge_ex_vat', inserted_row.total_charge_ex_vat,
      'margin_ex_vat', inserted_row.margin_ex_vat
    )::text)
  );
EXCEPTION
  WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
      v_error_state = RETURNED_SQLSTATE,
      v_error_message = MESSAGE_TEXT;

    IF did_prepare AND v_prev_id IS NOT NULL THEN
      UPDATE public.timesheets_financials AS restore_tf
         SET is_current = true,
             updated_at = COALESCE(p_now_utc, now())
       WHERE restore_tf.id = v_prev_id
         AND restore_tf.locked_by_invoice_id IS NULL
         AND restore_tf.paid_at_utc IS NULL;
    END IF;

    IF v_error_state IN ('55P03', '57014') THEN
      RAISE EXCEPTION USING
        MESSAGE = 'LOCK_TIMEOUT',
        DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id, 'error_state', v_error_state)::text;
    END IF;

    IF v_error_message = 'TSFIN_LOCKED' THEN
      RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_OR_PAID', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
    END IF;

    IF v_error_state IN ('22P02', '22007', '22008', '22003', '22018') THEN
      RAISE EXCEPTION USING
        MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH',
        DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id, 'error_state', v_error_state, 'error_message', v_error_message)::text;
    END IF;

    RAISE;
END;
$function$;

-- tsfin_write_snapshots_and_complete(jsonb)
CREATE OR REPLACE FUNCTION public.tsfin_write_snapshots_and_complete(p_rows jsonb)
 RETURNS TABLE(ok_count integer, fail_count integer, errors jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
 SET "plpgsql_check.profiler" TO 'off'
 SET "plpgsql_check.tracer" TO 'off'
 SET "plpgsql_check.constants_tracing" TO 'off'
 SET "plpgsql_check.cursors_leaks" TO 'off'
 SET "plpgsql_check.strict_cursors_leaks" TO 'off'
 SET "plpgsql_check.fatal_errors" TO 'off'
AS $function$
declare
  v_ok int := 0;
  v_fail int := 0;
  v_errors jsonb := '[]'::jsonb;

  r record;
  snap jsonb;

  prev public.timesheets_financials%rowtype;
  v_prev_id uuid;

  v_outbox_id uuid;
  v_timesheet_id uuid;

  v_err text;
  did_prepare boolean;

  -- ✅ validate segments JSON before writing
  v_ib  jsonb;
  v_bad int;

  -- ✅ expense component rollup (sum components; fallback to expenses_* only if sum == 0)
  v_cat_pay numeric;
  v_cat_charge numeric;
  v_fallback_exp_pay numeric;
  v_fallback_exp_charge numeric;
  v_exp_pay numeric;
  v_exp_charge numeric;

  -- ✅ additional + mileage rollup + patched totals/breakdown
  v_add_units_json jsonb;
  v_add_pay numeric;
  v_add_charge numeric;
  v_mil_pay numeric;
  v_mil_charge numeric;

  v_nonseg_pay numeric;
  v_nonseg_charge numeric;
  v_nonseg_margin numeric;

  v_core_pay numeric;
  v_core_charge numeric;

  v_total_pay numeric;
  v_total_charge numeric;
  v_margin numeric;

  v_add_obj jsonb;
  v_tot_obj jsonb;

  v_mode text;
  v_seg jsonb;
  v_exclude boolean;

  -- ✅ ERNI policy (PAYE only; NEVER applies to expenses/mileage)
  v_policy jsonb;
  v_apply_to text;
  v_erni_pct_raw numeric;
  v_erni_mult numeric;
  v_pay_method_u text;
  v_erni_applies boolean;

  v_wage_pay numeric;
  v_wage_pay_cost numeric;
  v_reimb_pay numeric;
  v_pay_cost numeric;

  v_nonseg_wage_pay numeric;
  v_nonseg_wage_pay_cost numeric;
  v_nonseg_reimb_pay numeric;
  v_nonseg_pay_cost numeric;

  v_processing_status public.ts_fin_processing_status_enum;
  v_candidate_assignment public.candidate_assignment_enum;
  v_basis public.timesheet_fin_basis_enum;
  v_timesheet_version int;
  v_is_stale boolean;
  v_stale_reason text;
  v_temp_log_enabled boolean := false;
  v_signature_diag_json jsonb := '{}'::jsonb;

  v_live_timesheet_authorised_at_server timestamptz := NULL::timestamptz;
  v_live_contract_week_id uuid := NULL::uuid;
  v_live_contract_week_status text := NULL::text;
  v_incoming_processing_status text := NULL::text;
  v_incoming_authorised_at_utc timestamptz := NULL::timestamptz;
  v_result_authorised_at_utc timestamptz := NULL::timestamptz;
  v_result_authorised_by_user_id uuid := NULL::uuid;
  v_live_authorised boolean := false;
  v_can_preserve_authorised_at boolean := false;
  v_lifecycle_decision text := 'WRITTEN';
begin
  perform public._ctms_assert_tsfin_batch_units_v1(coalesce(p_rows, '[]'::jsonb));
  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then
    ok_count := 0;
    fail_count := 0;
    errors := '[]'::jsonb;
    return next;
    return;
  end if;

  for r in
    select
      nullif(elem->>'outbox_id','')::uuid     as outbox_id,
      nullif(elem->>'timesheet_id','')::uuid  as timesheet_id,
      elem->'snapshot'                        as snapshot
    from jsonb_array_elements(p_rows) as elem
  loop
    v_outbox_id := r.outbox_id;
    v_timesheet_id := r.timesheet_id;
    snap := r.snapshot;

    did_prepare := false;
    v_prev_id := null;
    v_live_timesheet_authorised_at_server := NULL::timestamptz;
    v_live_contract_week_id := NULL::uuid;
    v_live_contract_week_status := NULL::text;
    v_incoming_processing_status := NULL::text;
    v_incoming_authorised_at_utc := NULL::timestamptz;
    v_result_authorised_at_utc := NULL::timestamptz;
    v_result_authorised_by_user_id := NULL::uuid;
    v_live_authorised := false;
    v_can_preserve_authorised_at := false;
    v_lifecycle_decision := 'WRITTEN';

    begin
      if v_outbox_id is null or v_timesheet_id is null or snap is null then
        raise exception 'INVALID_BATCH_ROW';
      end if;

      -- SAFETY: do not write if current snapshot is paid
      if exists (
        select 1
        from public.timesheets_financials tf
        where tf.timesheet_id = v_timesheet_id
          and tf.is_current = true
          and tf.paid_at_utc is not null
      ) then
        perform public.tsfin_work_success(v_outbox_id);
        v_ok := v_ok + 1;
        continue;
      end if;

      -- Capture live lifecycle and current snapshot (for restore-on-fail + preserving manual fields).
      select ts.authorised_at_server
      into v_live_timesheet_authorised_at_server
      from public.timesheets AS ts
      where ts.timesheet_id = v_timesheet_id
        and coalesce(ts.is_current, true) = true
      order by ts.updated_at desc nulls last,
               ts.created_at desc nulls last,
               ts.timesheet_id desc
      limit 1
      for update;

      select cw.id,
             cw.status::text
      into v_live_contract_week_id,
           v_live_contract_week_status
      from public.contract_weeks AS cw
      where cw.timesheet_id = v_timesheet_id
      order by cw.updated_at desc nulls last,
               cw.created_at desc nulls last,
               cw.id desc
      limit 1
      for update;

      select *
      into prev
      from public.timesheets_financials tf
      where tf.timesheet_id = v_timesheet_id
        and tf.is_current = true
      order by tf.computed_at_utc desc nulls last,
               tf.created_at desc nulls last,
               tf.updated_at desc nulls last,
               tf.id desc
      limit 1
      for update;

      v_prev_id := prev.id;

      -- Resolve enums/typed fields once (avoid divergence between flags and stored status)
      v_timesheet_version :=
        coalesce(nullif(snap->>'timesheet_version','')::int, 1);

      v_basis :=
        coalesce(
          nullif(snap->>'basis','')::public.timesheet_fin_basis_enum,
          'SELF_REPORTED'::public.timesheet_fin_basis_enum
        );

      v_candidate_assignment :=
        coalesce(
          nullif(snap->>'candidate_assignment','')::public.candidate_assignment_enum,
          'UNASSIGNED'::public.candidate_assignment_enum
        );

      v_processing_status :=
        coalesce(
          nullif(snap->>'processing_status','')::public.ts_fin_processing_status_enum,
          'UNASSIGNED'::public.ts_fin_processing_status_enum
        );

      -- ✅ validate invoice_breakdown_json (prevents persisting null/invalid segments)
      v_ib := coalesce(snap->'invoice_breakdown_json', '{}'::jsonb);
      if v_ib is null or jsonb_typeof(v_ib) <> 'object' then
        v_ib := '{}'::jsonb;
      end if;

      v_bad := public._tsfin_invalid_segment_count(v_ib);
      if v_bad > 0 then
        raise exception 'INVALID_SEGMENTS_JSON:%', v_bad;
      end if;

      v_incoming_processing_status := CASE
        WHEN v_processing_status IS NULL THEN NULL::text
        ELSE v_processing_status::text
      END;
      v_incoming_authorised_at_utc := NULLIF(snap->>'authorised_at_utc','')::timestamptz;
      v_result_authorised_at_utc := coalesce(v_incoming_authorised_at_utc, prev.authorised_at_utc);
      v_result_authorised_by_user_id := coalesce(
        nullif(snap->>'authorised_by_user_id','')::uuid,
        prev.authorised_by_user_id
      );
      v_live_authorised := (
           v_live_timesheet_authorised_at_server IS NOT NULL
        OR upper(btrim(coalesce(v_live_contract_week_status, ''))) = 'AUTHORISED'
        OR prev.authorised_at_utc IS NOT NULL
        OR prev.processing_status IN (
             'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
             'READY_FOR_HR'::public.ts_fin_processing_status_enum
           )
      );

      IF v_live_authorised
         AND (
              v_processing_status = 'PENDING_AUTH'::public.ts_fin_processing_status_enum
           OR v_processing_status NOT IN (
                'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
                'READY_FOR_HR'::public.ts_fin_processing_status_enum
              )
           OR v_result_authorised_at_utc IS NULL
         ) THEN
        v_can_preserve_authorised_at := coalesce(
          prev.authorised_at_utc,
          v_live_timesheet_authorised_at_server,
          v_incoming_authorised_at_utc
        ) IS NOT NULL;

        IF NOT v_can_preserve_authorised_at THEN
          v_lifecycle_decision := 'REJECTED';
          PERFORM public._temp_diag_log(
            'TSFIN_SNAPSHOT_LIFECYCLE_DOWNGRADE_REJECTED',
            'TEMP_TIMESHEET_LIFECYCLE',
            v_timesheet_id::text,
            jsonb_strip_nulls(
              jsonb_build_object(
                'function_name', 'tsfin_write_snapshots_and_complete',
                'outbox_id', CASE WHEN v_outbox_id IS NULL THEN NULL::text ELSE v_outbox_id::text END,
                'timesheet_id', v_timesheet_id::text,
                'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
                'incoming_processing_status', v_incoming_processing_status,
                'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
                'live_timesheet_authorised_at_server_present', v_live_timesheet_authorised_at_server IS NOT NULL,
                'live_contract_week_status', v_live_contract_week_status,
                'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
                'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
                'decision', v_lifecycle_decision,
                'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
                'result_authorised_at_utc_present', false,
                'rejection_reason', 'LIVE_AUTHORISED_STATE_WITHOUT_AUTHORISATION_TIMESTAMP',
                'policy_x_boundary', 'LIFECYCLE_FIELDS_ONLY_NO_ECONOMICS_CHANGE'
              )
            )
          );
          RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = 'TSFIN_SNAPSHOT_LIFECYCLE_DOWNGRADE_REJECTED',
            DETAIL = jsonb_build_object(
              'outbox_id', CASE WHEN v_outbox_id IS NULL THEN NULL::text ELSE v_outbox_id::text END,
              'timesheet_id', v_timesheet_id::text,
              'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
              'incoming_processing_status', v_incoming_processing_status,
              'live_contract_week_status', v_live_contract_week_status,
              'reason', 'LIVE_AUTHORISED_STATE_WITHOUT_AUTHORISATION_TIMESTAMP'
            )::text;
        END IF;

        v_processing_status := CASE
          WHEN prev.processing_status IN (
                 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
                 'READY_FOR_HR'::public.ts_fin_processing_status_enum
               )
            THEN prev.processing_status
          WHEN v_processing_status IN (
                 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum,
                 'READY_FOR_HR'::public.ts_fin_processing_status_enum
               )
            THEN v_processing_status
          ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        END;
        v_result_authorised_at_utc := coalesce(
          prev.authorised_at_utc,
          v_live_timesheet_authorised_at_server,
          v_incoming_authorised_at_utc
        );
        v_result_authorised_by_user_id := coalesce(
          prev.authorised_by_user_id,
          v_result_authorised_by_user_id
        );
        v_lifecycle_decision := 'PRESERVED';
      END IF;

      PERFORM public._temp_diag_log(
        'TSFIN_SNAPSHOT_LIVE_LIFECYCLE_GUARD',
        'TEMP_TIMESHEET_LIFECYCLE',
        v_timesheet_id::text,
        jsonb_strip_nulls(
          jsonb_build_object(
            'function_name', 'tsfin_write_snapshots_and_complete',
            'outbox_id', CASE WHEN v_outbox_id IS NULL THEN NULL::text ELSE v_outbox_id::text END,
            'timesheet_id', v_timesheet_id::text,
            'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
            'incoming_processing_status', v_incoming_processing_status,
            'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
            'live_timesheet_authorised_at_server_present', v_live_timesheet_authorised_at_server IS NOT NULL,
            'live_contract_week_id', CASE WHEN v_live_contract_week_id IS NULL THEN NULL::text ELSE v_live_contract_week_id::text END,
            'live_contract_week_status', v_live_contract_week_status
          )
          || jsonb_build_object(
            'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
            'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
            'decision', v_lifecycle_decision,
            'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
            'result_authorised_at_utc_present', v_result_authorised_at_utc IS NOT NULL,
            'policy_x_boundary', 'LIFECYCLE_FIELDS_ONLY_NO_ECONOMICS_CHANGE'
          )
        )
      );

      IF v_lifecycle_decision = 'PRESERVED' THEN
        PERFORM public._temp_diag_log(
          'TSFIN_SNAPSHOT_LIFECYCLE_STATUS_PRESERVED',
          'TEMP_TIMESHEET_LIFECYCLE',
          v_timesheet_id::text,
          jsonb_strip_nulls(
            jsonb_build_object(
              'function_name', 'tsfin_write_snapshots_and_complete',
              'outbox_id', CASE WHEN v_outbox_id IS NULL THEN NULL::text ELSE v_outbox_id::text END,
              'timesheet_id', v_timesheet_id::text,
              'previous_financials_id', CASE WHEN v_prev_id IS NULL THEN NULL::text ELSE v_prev_id::text END,
              'incoming_processing_status', v_incoming_processing_status,
              'incoming_authorised_at_utc_present', v_incoming_authorised_at_utc IS NOT NULL,
              'live_timesheet_authorised_at_server_present', v_live_timesheet_authorised_at_server IS NOT NULL,
              'live_contract_week_status', v_live_contract_week_status,
              'previous_tsfin_processing_status', CASE WHEN prev.processing_status IS NULL THEN NULL::text ELSE prev.processing_status::text END,
              'previous_tsfin_authorised_at_utc_present', prev.authorised_at_utc IS NOT NULL,
              'decision', v_lifecycle_decision,
              'result_processing_status', CASE WHEN v_processing_status IS NULL THEN NULL::text ELSE v_processing_status::text END,
              'result_authorised_at_utc_present', v_result_authorised_at_utc IS NOT NULL
            )
          )
        );
      END IF;

      -- Guard + rotate current -> history (invoice-lock protected)
      perform public.tsfin_prepare_write(v_timesheet_id);
      did_prepare := true;

      -- ✅ compute expense totals (sum components first; fallback to expenses_* only if sum == 0)
      v_cat_pay :=
          coalesce(nullif(snap->>'travel_pay_ex_vat','')::numeric, prev.travel_pay_ex_vat, 0)
        + coalesce(nullif(snap->>'accommodation_pay_ex_vat','')::numeric, prev.accommodation_pay_ex_vat, 0)
        + coalesce(nullif(snap->>'other_pay_ex_vat','')::numeric, prev.other_pay_ex_vat, 0);

      v_cat_charge :=
          coalesce(nullif(snap->>'travel_charge_ex_vat','')::numeric, prev.travel_charge_ex_vat, 0)
        + coalesce(nullif(snap->>'accommodation_charge_ex_vat','')::numeric, prev.accommodation_charge_ex_vat, 0)
        + coalesce(nullif(snap->>'other_charge_ex_vat','')::numeric, prev.other_charge_ex_vat, 0);

      v_fallback_exp_pay :=
        coalesce(nullif(snap->>'expenses_pay_ex_vat','')::numeric, prev.expenses_pay_ex_vat, 0);

      v_fallback_exp_charge :=
        coalesce(nullif(snap->>'expenses_charge_ex_vat','')::numeric, prev.expenses_charge_ex_vat, 0);

      v_exp_pay :=
        case when coalesce(v_cat_pay, 0) <> 0 then v_cat_pay else v_fallback_exp_pay end;

      v_exp_charge :=
        case when coalesce(v_cat_charge, 0) <> 0 then v_cat_charge else v_fallback_exp_charge end;

      v_exp_pay := round(coalesce(v_exp_pay,0), 2);
      v_exp_charge := round(coalesce(v_exp_charge,0), 2);

      -- ✅ additional + mileage rollup (snap → prev fallback)
      v_add_units_json := coalesce(snap->'additional_units_json', prev.additional_units_json, '{}'::jsonb);

      v_add_pay := coalesce(nullif(snap->>'additional_pay_ex_vat','')::numeric, prev.additional_pay_ex_vat, 0);
      v_add_charge := coalesce(nullif(snap->>'additional_charge_ex_vat','')::numeric, prev.additional_charge_ex_vat, 0);

      v_mil_pay := coalesce(nullif(snap->>'mileage_pay_ex_vat','')::numeric, prev.mileage_pay_ex_vat, 0);
      v_mil_charge := coalesce(nullif(snap->>'mileage_charge_ex_vat','')::numeric, prev.mileage_charge_ex_vat, 0);

      v_add_pay := round(coalesce(v_add_pay,0), 2);
      v_add_charge := round(coalesce(v_add_charge,0), 2);
      v_mil_pay := round(coalesce(v_mil_pay,0), 2);
      v_mil_charge := round(coalesce(v_mil_charge,0), 2);

      -- Non-segment totals (additional + expenses + mileage)
      v_nonseg_pay := round(v_add_pay + v_exp_pay + v_mil_pay, 2);
      v_nonseg_charge := round(v_add_charge + v_exp_charge + v_mil_charge, 2);

      -- ✅ recompute core totals from breakdown (SEGMENTS sums; else base_hours)
      v_core_pay := 0;
      v_core_charge := 0;

      v_mode := upper(coalesce(v_ib->>'mode',''));

      if v_mode = 'SEGMENTS' and jsonb_typeof(v_ib->'segments') = 'array' then
        for v_seg in
          select value from jsonb_array_elements(v_ib->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          -- charge sum includes all segments (negative allowed)
          begin
            v_core_charge := v_core_charge
              + coalesce(nullif(btrim(coalesce(v_seg->>'charge_amount','')), '')::numeric, 0);
          exception when others then
            null;
          end;

          -- pay sum respects exclude_from_pay when present/true
          v_exclude := false;
          begin
            v_exclude := coalesce(nullif(btrim(coalesce(v_seg->>'exclude_from_pay','')), '')::boolean, false);
          exception when others then
            v_exclude := false;
          end;

          if not v_exclude then
            begin
              v_core_pay := v_core_pay
                + coalesce(nullif(btrim(coalesce(v_seg->>'pay_amount','')), '')::numeric, 0);
            exception when others then
              null;
            end;
          end if;
        end loop;
      else
        begin
          v_core_pay := coalesce(nullif(btrim(coalesce(v_ib#>>'{base_hours,pay_ex_vat}','')), '')::numeric, 0);
        exception when others then
          v_core_pay := 0;
        end;

        begin
          v_core_charge := coalesce(nullif(btrim(coalesce(v_ib#>>'{base_hours,charge_ex_vat}','')), '')::numeric, 0);
        exception when others then
          v_core_charge := 0;
        end;
      end if;

      v_core_pay := round(coalesce(v_core_pay,0), 2);
      v_core_charge := round(coalesce(v_core_charge,0), 2);

      -- Totals (pay/charge) are pure ex-vat figures (NO ERNI in totals)
      v_total_pay := round(v_core_pay + v_nonseg_pay, 2);
      v_total_charge := round(v_core_charge + v_nonseg_charge, 2);

      -- ✅ ERNI-aware margin:
      -- - ERNI applies ONLY to PAYE candidates
      -- - ERNI uplifts ONLY wage-like pay (core pay + additional pay)
      -- - ERNI NEVER applies to expenses or mileage
      v_policy := coalesce(snap->'policy_snapshot_json', prev.policy_snapshot_json, '{}'::jsonb);
      if v_policy is null or jsonb_typeof(v_policy) <> 'object' then
        v_policy := '{}'::jsonb;
      end if;

      v_apply_to := upper(coalesce(nullif(btrim(coalesce(v_policy->>'apply_erni_to','')), ''), 'PAYE_ONLY'));

      v_erni_pct_raw := 0;
      begin
        v_erni_pct_raw := coalesce(nullif(btrim(coalesce(v_policy->>'erni_pct','')), '')::numeric, 0);
      exception when others then
        v_erni_pct_raw := 0;
      end;

      v_erni_mult := 1;
      if coalesce(v_erni_pct_raw,0) > 0 then
        if v_erni_pct_raw > 1 then
          v_erni_mult := 1 + (v_erni_pct_raw / 100);
        else
          v_erni_mult := 1 + v_erni_pct_raw;
        end if;
      end if;

      v_pay_method_u :=
        upper(
          coalesce(
            nullif(btrim(coalesce(snap->>'pay_method','')), ''),
            nullif(btrim(coalesce(prev.pay_method::text,'')), ''),
            ''
          )
        );

      -- IMPORTANT: PAYE only. apply_erni_to can be ALL/PAYE_ONLY but never makes it apply to non-PAYE.
      v_erni_applies :=
        (v_pay_method_u = 'PAYE')
        and (v_apply_to = 'ALL' or v_apply_to = 'PAYE_ONLY');

      -- overall margin cost:
      v_wage_pay := round(v_core_pay + v_add_pay, 2);         -- wage-like pay only
      v_reimb_pay := round(v_exp_pay + v_mil_pay, 2);         -- reimbursements (never ERNI)

      v_wage_pay_cost := v_wage_pay;
      if v_erni_applies then
        v_wage_pay_cost := round(v_wage_pay * v_erni_mult, 2);
      end if;

      v_pay_cost := round(v_wage_pay_cost + v_reimb_pay, 2);
      v_margin := round(v_total_charge - v_pay_cost, 2);

      -- non-segment contribution margin (apply ERNI only to additional pay; never to expenses/mileage)
      v_nonseg_wage_pay := v_add_pay;
      v_nonseg_reimb_pay := round(v_exp_pay + v_mil_pay, 2);

      v_nonseg_wage_pay_cost := v_nonseg_wage_pay;
      if v_erni_applies then
        v_nonseg_wage_pay_cost := round(v_nonseg_wage_pay * v_erni_mult, 2);
      end if;

      v_nonseg_pay_cost := round(v_nonseg_wage_pay_cost + v_nonseg_reimb_pay, 2);
      v_nonseg_margin := round(v_nonseg_charge - v_nonseg_pay_cost, 2);

      -- ✅ patch invoice_breakdown_json.additional (preserve units if present; else set from stored additional_units_json)
      v_add_obj := case
        when v_ib ? 'additional' and jsonb_typeof(v_ib->'additional') = 'object' then v_ib->'additional'
        else '{}'::jsonb
      end;

      if not (v_add_obj ? 'units') or jsonb_typeof(v_add_obj->'units') <> 'object' then
        if v_add_units_json is null or jsonb_typeof(v_add_units_json) <> 'object' then
          v_add_units_json := '{}'::jsonb;
        end if;
        v_add_obj := jsonb_set(v_add_obj, '{units}', v_add_units_json, true);
      end if;

      -- additional now represents combined non-segment totals (additional + expenses + mileage)
      v_add_obj := jsonb_set(v_add_obj, '{pay_ex_vat}', to_jsonb(v_nonseg_pay), true);
      v_add_obj := jsonb_set(v_add_obj, '{charge_ex_vat}', to_jsonb(v_nonseg_charge), true);
      v_add_obj := jsonb_set(v_add_obj, '{margin_ex_vat}', to_jsonb(v_nonseg_margin), true);

      v_ib := jsonb_set(v_ib, '{additional}', v_add_obj, true);

      -- ✅ patch invoice_breakdown_json.totals to match computed totals (margin includes PAYE-only ERNI on wage pay)
      v_tot_obj := case
        when v_ib ? 'totals' and jsonb_typeof(v_ib->'totals') = 'object' then v_ib->'totals'
        else '{}'::jsonb
      end;

      v_tot_obj := jsonb_set(v_tot_obj, '{total_pay_ex_vat}', to_jsonb(v_total_pay), true);
      v_tot_obj := jsonb_set(v_tot_obj, '{total_charge_ex_vat}', to_jsonb(v_total_charge), true);
      v_tot_obj := jsonb_set(v_tot_obj, '{margin_ex_vat}', to_jsonb(v_margin), true);

      v_ib := jsonb_set(v_ib, '{totals}', v_tot_obj, true);

      -- ✅ stale flag tie-off (clear stale on a successful READY_FOR_INVOICE snapshot)
      v_is_stale := coalesce(nullif(snap->>'is_stale','')::boolean, false);
      v_stale_reason := nullif(snap->>'stale_reason','');

      if v_processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum then
        v_is_stale := false;
        v_stale_reason := null;
      end if;

      -- Insert new current snapshot (explicit columns)
      insert into public.timesheets_financials (
        timesheet_id,
        timesheet_version,
        basis,
        is_current,
        is_stale,
        stale_reason,

        worked_start_iso,
        worked_end_iso,
        break_start_iso,
        break_end_iso,
        break_minutes,

        candidate_id,
        client_id,
        role,
        band,
        pay_method,

        policy_snapshot_json,
        rate_source_refs_json,

        hours_day,
        hours_night,
        hours_sat,
        hours_sun,
        hours_bh,

        pay_day, pay_night, pay_sat, pay_sun, pay_bh,
        charge_day, charge_night, charge_sat, charge_sun, charge_bh,

        total_hours,
        total_pay_ex_vat,
        total_charge_ex_vat,
        margin_ex_vat,

        computed_at_utc,

        occupant_key_norm,
        candidate_assignment,
        processing_status,

        -- Totals + description + legacy evidence pointers
        expenses_pay_ex_vat,
        expenses_charge_ex_vat,
        expenses_description,
        expenses_evidence_r2_key,
        expenses_evidence_manifest,

        -- category expense columns
        travel_pay_ex_vat,
        travel_charge_ex_vat,
        accommodation_pay_ex_vat,
        accommodation_charge_ex_vat,
        other_pay_ex_vat,
        other_charge_ex_vat,

        mileage_pay_ex_vat,
        mileage_charge_ex_vat,
        mileage_units,
        mileage_evidence_r2_key,
        mileage_evidence_manifest,
        mileage_pay_rate,
        mileage_charge_rate,

        po_number,

        pay_on_hold,
        pay_on_hold_reason,
        pay_on_hold_since_utc,

        pay_wtr_rate_pct_snapshot,
        pay_vat_rate_pct_snapshot,
        pay_vat_amount_snapshot,
        pay_total_inc_vat_snapshot,

        payment_reference,
        remittance_last_sent_at_utc,
        remittance_send_count,

        processed_by_user_id,
        processed_at_utc,
        authorised_by_user_id,
        authorised_at_utc,

        additional_units_json,
        additional_pay_ex_vat,
        additional_charge_ex_vat,
        additional_margin_ex_vat,

        invoice_breakdown_json,

        nhsp_import_id,

        has_rate_issue,
        has_pay_channel_issue,

        hr_crosscheck_status,
        hr_crosscheck_issues,

        external_source_rows_json,

        actual_schedule_json,
        actual_minutes_by_day_json
      )
      values (
        v_timesheet_id,
        v_timesheet_version,
        v_basis,
        true,

        v_is_stale,
        v_stale_reason,

        nullif(snap->>'worked_start_iso','')::timestamptz,
        nullif(snap->>'worked_end_iso','')::timestamptz,
        nullif(snap->>'break_start_iso','')::timestamptz,
        nullif(snap->>'break_end_iso','')::timestamptz,
        nullif(snap->>'break_minutes','')::int,

        nullif(snap->>'candidate_id','')::uuid,
        nullif(snap->>'client_id','')::uuid,
        nullif(snap->>'role',''),
        nullif(snap->>'band',''),
        nullif(snap->>'pay_method',''),

        coalesce(snap->'policy_snapshot_json', '{}'::jsonb),
        coalesce(snap->'rate_source_refs_json', '{}'::jsonb),

        coalesce(nullif(snap->>'hours_day','')::numeric, 0),
        coalesce(nullif(snap->>'hours_night','')::numeric, 0),
        coalesce(nullif(snap->>'hours_sat','')::numeric, 0),
        coalesce(nullif(snap->>'hours_sun','')::numeric, 0),
        coalesce(nullif(snap->>'hours_bh','')::numeric, 0),

        nullif(snap->>'pay_day','')::numeric,
        nullif(snap->>'pay_night','')::numeric,
        nullif(snap->>'pay_sat','')::numeric,
        nullif(snap->>'pay_sun','')::numeric,
        nullif(snap->>'pay_bh','')::numeric,

        nullif(snap->>'charge_day','')::numeric,
        nullif(snap->>'charge_night','')::numeric,
        nullif(snap->>'charge_sat','')::numeric,
        nullif(snap->>'charge_sun','')::numeric,
        nullif(snap->>'charge_bh','')::numeric,

        coalesce(nullif(snap->>'total_hours','')::numeric, 0),

        -- totals are computed from core + (additional + expenses + mileage) (NO ERNI in totals)
        coalesce(v_total_pay, 0),
        coalesce(v_total_charge, 0),

        -- margin includes PAYE-only ERNI on wage pay (core + additional), never on expenses/mileage
        coalesce(v_margin, 0),

        now(),

        nullif(snap->>'occupant_key_norm',''),
        v_candidate_assignment,
        v_processing_status,

        -- expenses_pay_ex_vat: rollup (category sum or fallback)
        coalesce(v_exp_pay, 0),

        -- expenses_charge_ex_vat: rollup (category sum or fallback)
        coalesce(v_exp_charge, 0),

        nullif(snap->>'expenses_description',''),
        nullif(snap->>'expenses_evidence_r2_key',''),
        coalesce(snap->'expenses_evidence_manifest', prev.expenses_evidence_manifest),

        -- category columns (preserved from prev if absent in snapshot)
        coalesce(nullif(snap->>'travel_pay_ex_vat','')::numeric, prev.travel_pay_ex_vat, 0),
        coalesce(nullif(snap->>'travel_charge_ex_vat','')::numeric, prev.travel_charge_ex_vat, 0),
        coalesce(nullif(snap->>'accommodation_pay_ex_vat','')::numeric, prev.accommodation_pay_ex_vat, 0),
        coalesce(nullif(snap->>'accommodation_charge_ex_vat','')::numeric, prev.accommodation_charge_ex_vat, 0),
        coalesce(nullif(snap->>'other_pay_ex_vat','')::numeric, prev.other_pay_ex_vat, 0),
        coalesce(nullif(snap->>'other_charge_ex_vat','')::numeric, prev.other_charge_ex_vat, 0),

        -- preserve mileage pay/charge from prev if absent
        coalesce(nullif(snap->>'mileage_pay_ex_vat','')::numeric, prev.mileage_pay_ex_vat, 0),
        coalesce(nullif(snap->>'mileage_charge_ex_vat','')::numeric, prev.mileage_charge_ex_vat, 0),

        coalesce(nullif(snap->>'mileage_units','')::numeric, prev.mileage_units, 0),
        nullif(snap->>'mileage_evidence_r2_key',''),
        coalesce(snap->'mileage_evidence_manifest', prev.mileage_evidence_manifest),
        nullif(snap->>'mileage_pay_rate','')::numeric,
        nullif(snap->>'mileage_charge_rate','')::numeric,

        coalesce(nullif(snap->>'po_number',''), prev.po_number),

        coalesce(nullif(snap->>'pay_on_hold','')::boolean, prev.pay_on_hold, false),
        coalesce(nullif(snap->>'pay_on_hold_reason',''), prev.pay_on_hold_reason),
        coalesce(nullif(snap->>'pay_on_hold_since_utc','')::timestamptz, prev.pay_on_hold_since_utc),

        nullif(snap->>'pay_wtr_rate_pct_snapshot','')::numeric,
        nullif(snap->>'pay_vat_rate_pct_snapshot','')::numeric,
        coalesce(nullif(snap->>'pay_vat_amount_snapshot','')::numeric, 0),
        coalesce(nullif(snap->>'pay_total_inc_vat_snapshot','')::numeric, 0),

        coalesce(nullif(snap->>'payment_reference',''), prev.payment_reference),
        coalesce(nullif(snap->>'remittance_last_sent_at_utc','')::timestamptz, prev.remittance_last_sent_at_utc),
        coalesce(nullif(snap->>'remittance_send_count','')::int, prev.remittance_send_count, 0),

        coalesce(nullif(snap->>'processed_by_user_id','')::uuid, prev.processed_by_user_id),
        coalesce(nullif(snap->>'processed_at_utc','')::timestamptz, prev.processed_at_utc),
        v_result_authorised_by_user_id,
        v_result_authorised_at_utc,

        -- preserve additional units from prev if absent
        coalesce(snap->'additional_units_json', prev.additional_units_json, '{}'::jsonb),

        -- preserve additional pay/charge/margin from prev if absent
        coalesce(nullif(snap->>'additional_pay_ex_vat','')::numeric, prev.additional_pay_ex_vat, 0),
        coalesce(nullif(snap->>'additional_charge_ex_vat','')::numeric, prev.additional_charge_ex_vat, 0),
        coalesce(nullif(snap->>'additional_margin_ex_vat','')::numeric, prev.additional_margin_ex_vat, 0),

        -- patched breakdown (additional + totals updated)
        v_ib,

        nullif(snap->>'nhsp_import_id','')::uuid,

        coalesce(nullif(snap->>'has_rate_issue','')::boolean, false),
        coalesce(nullif(snap->>'has_pay_channel_issue','')::boolean, false),

        coalesce(nullif(snap->>'hr_crosscheck_status',''), prev.hr_crosscheck_status),
        coalesce(
          case
            when snap ? 'hr_crosscheck_issues' and jsonb_typeof(snap->'hr_crosscheck_issues') = 'array'
              then array(select jsonb_array_elements_text(snap->'hr_crosscheck_issues'))
            else null
          end,
          prev.hr_crosscheck_issues
        ),

        coalesce(snap->'external_source_rows_json', prev.external_source_rows_json),

        coalesce(snap->'actual_schedule_json', prev.actual_schedule_json),
        coalesce(snap->'actual_minutes_by_day_json', prev.actual_minutes_by_day_json)
      );

      perform public.tsfin_work_success(v_outbox_id);
      v_ok := v_ok + 1;

      IF COALESCE(v_temp_log_enabled, false) THEN
        v_signature_diag_json := public.timesheet_lifecycle_guard_signature_v1(v_timesheet_id, NULL::uuid, true);
        PERFORM public._temp_diag_log(
          'TSFIN_SIGNATURE_AFTER_SNAPSHOT',
          'TEMP_TIMESHEET_LIFECYCLE',
          v_timesheet_id::text,
          jsonb_strip_nulls(jsonb_build_object(
            'tag', 'TSFIN_SIGNATURE_AFTER_SNAPSHOT',
            'function_name', 'tsfin_write_snapshots_and_complete',
            'outbox_id', v_outbox_id,
            'timesheet_id', v_timesheet_id,
            'signature', NULLIF(BTRIM(COALESCE(v_signature_diag_json ->> 'backend_row_signature', v_signature_diag_json ->> 'row_signature', v_signature_diag_json ->> 'signature', '')), ''),
            'signature_payload', v_signature_diag_json,
            'processing_status', CASE WHEN v_processing_status IS NULL THEN NULL ELSE v_processing_status::text END,
            'previous_financials_id', v_prev_id
          ))
        );
      END IF;

    exception
      when others then
        v_err := sqlerrm;

        -- Restore previous snapshot if we already rotated current->history and insert failed.
        if did_prepare and v_prev_id is not null then
          update public.timesheets_financials
          set is_current = true,
              updated_at = now()
          where id = v_prev_id
            and locked_by_invoice_id is null
            and paid_at_utc is null;
        end if;

        -- If locked, don't retry forever: delete outbox row
        if v_err = 'TSFIN_LOCKED' then
          perform public.tsfin_work_success(v_outbox_id);
          v_ok := v_ok + 1;
        else
          perform public.tsfin_work_fail(v_outbox_id, v_err);
          v_fail := v_fail + 1;

          v_errors := v_errors || jsonb_build_array(
            jsonb_build_object(
              'outbox_id', v_outbox_id::text,
              'timesheet_id', v_timesheet_id::text,
              'error', v_err
            )
          );
        end if;
    end;
  end loop;

  ok_count := v_ok;
  fail_count := v_fail;
  errors := v_errors;
  return next;
end;
$function$;

-- tspdf_dequeue_batch_ids(integer)
CREATE OR REPLACE FUNCTION public.tspdf_dequeue_batch_ids(p_limit integer DEFAULT 10)
 RETURNS TABLE(outbox_id uuid, timesheet_id uuid, reason ts_pdf_reason_enum, attempt_count integer, next_attempt_at timestamp with time zone, created_at timestamp with time zone, prefer_generated boolean, force_regen boolean)
 LANGUAGE plpgsql
AS $function$
declare
  v_now timestamptz := now();
  v_lim int := greatest(1, least(coalesce(p_limit, 10), 200));
begin
  return query
  with picked as (
    select o.id
    from public.ts_pdfs_outbox o
    where o.next_attempt_at is null or o.next_attempt_at <= v_now
    order by o.next_attempt_at nulls first, o.created_at
    limit v_lim
    for update skip locked
  )
  update public.ts_pdfs_outbox o
  set attempt_count   = o.attempt_count + 1,
      next_attempt_at = v_now + interval '5 minutes'
  where o.id in (select id from picked)
  returning
    o.id as outbox_id,
    o.timesheet_id,
    o.reason,
    o.attempt_count,
    o.next_attempt_at,
    o.created_at,
    o.prefer_generated,
    o.force_regen;
end;
$function$;

-- tspdf_enqueue_many(uuid[],boolean,boolean,ts_pdf_reason_enum,integer)
CREATE OR REPLACE FUNCTION public.tspdf_enqueue_many(p_timesheet_ids uuid[], p_force_regen boolean DEFAULT false, p_prefer_generated boolean DEFAULT true, p_reason ts_pdf_reason_enum DEFAULT NULL::ts_pdf_reason_enum, p_limit integer DEFAULT 500)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 5000));
  v_count int := 0;
  v_i int := 0;
  v_id uuid;
begin
  if p_timesheet_ids is null or coalesce(array_length(p_timesheet_ids, 1), 0) = 0 then
    return 0;
  end if;

  foreach v_id in array p_timesheet_ids loop
    exit when v_i >= v_lim;
    v_i := v_i + 1;

    if v_id is null then
      continue;
    end if;

    v_count := v_count + public.tspdf_enqueue_one(
      p_timesheet_id := v_id,
      p_force_regen := coalesce(p_force_regen, false),
      p_prefer_generated := coalesce(p_prefer_generated, true),
      p_reason := p_reason
    );
  end loop;

  return v_count;
end;
$function$;

-- tspdf_enqueue_one(uuid,boolean,boolean,ts_pdf_reason_enum)
CREATE OR REPLACE FUNCTION public.tspdf_enqueue_one(p_timesheet_id uuid, p_force_regen boolean DEFAULT false, p_prefer_generated boolean DEFAULT true, p_reason ts_pdf_reason_enum DEFAULT NULL::ts_pdf_reason_enum)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_reason public.ts_pdf_reason_enum;
  v_updated int := 0;
  v_inserted int := 0;
begin
  if p_timesheet_id is null then
    return 0;
  end if;

  if coalesce(p_force_regen, false) is true then
    v_reason := 'FORCE_REGEN'::public.ts_pdf_reason_enum;
  else
    v_reason := coalesce(p_reason, 'READY_FOR_INVOICE'::public.ts_pdf_reason_enum);
  end if;

  if coalesce(p_force_regen, false) is true then
    delete from public.ts_pdfs_outbox o
    where o.timesheet_id = p_timesheet_id;

    insert into public.ts_pdfs_outbox(
      timesheet_id,
      reason,
      attempt_count,
      next_attempt_at,
      last_error,
      prefer_generated,
      force_regen,
      created_at
    )
    values (
      p_timesheet_id,
      v_reason,
      0,
      null,
      null,
      coalesce(p_prefer_generated, true),
      true,
      v_now
    )
    on conflict (timesheet_id, reason)
    do update
      set attempt_count    = 0,
          next_attempt_at  = null,
          last_error       = null,
          prefer_generated = public.ts_pdfs_outbox.prefer_generated or excluded.prefer_generated,
          force_regen      = true;

    return 1;
  end if;

  update public.ts_pdfs_outbox o
     set attempt_count    = 0,
         next_attempt_at  = null,
         last_error       = null,
         prefer_generated = o.prefer_generated or coalesce(p_prefer_generated, false),
         force_regen      = o.force_regen
   where o.timesheet_id = p_timesheet_id;

  get diagnostics v_updated = row_count;

  if coalesce(v_updated, 0) > 0 then
    return v_updated;
  end if;

  insert into public.ts_pdfs_outbox(
    timesheet_id,
    reason,
    attempt_count,
    next_attempt_at,
    last_error,
    prefer_generated,
    force_regen,
    created_at
  )
  values (
    p_timesheet_id,
    v_reason,
    0,
    null,
    null,
    coalesce(p_prefer_generated, false),
    false,
    v_now
  )
  on conflict (timesheet_id, reason)
  do update
    set attempt_count    = 0,
        next_attempt_at  = null,
        last_error       = null,
        prefer_generated = public.ts_pdfs_outbox.prefer_generated or excluded.prefer_generated,
        force_regen      = public.ts_pdfs_outbox.force_regen or excluded.force_regen;

  get diagnostics v_inserted = row_count;
  return coalesce(v_inserted, 0);
end;
$function$;

-- tspdf_enqueue_ready_for_invoice(integer)
CREATE OR REPLACE FUNCTION public.tspdf_enqueue_ready_for_invoice(p_limit integer DEFAULT 500)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  v_ins int := 0;
  v_lim int := greatest(1, least(coalesce(p_limit, 500), 2000));
begin
  with eligible as (
    select
      t.timesheet_id,
      t.generated_pdf_at_utc,
      nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') as prev_refs_sig,
      sig.cur_refs_sig,
      o.id as existing_outbox_id,
      coalesce(o.force_regen, false) as existing_force_regen,
      (
        nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') is not null
        and sig.cur_refs_sig is not null
        and nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') <> sig.cur_refs_sig
      ) as refs_sig_mismatch
    from public.timesheets t
    join public.timesheets_financials tf
      on tf.timesheet_id = t.timesheet_id
     and tf.is_current = true
    left join public.ts_pdfs_outbox o
      on o.timesheet_id = t.timesheet_id
     and o.reason = 'READY_FOR_INVOICE'::public.ts_pdf_reason_enum
    left join lateral (
      select
        case
          when nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') is not null
            then public.timesheet_pdf_reference_sig(t.timesheet_id)
          else null::text
        end as cur_refs_sig
    ) sig on true
    where t.is_current = true
      and t.revoked_at is null
      and t.submission_mode::text = 'ELECTRONIC'
      and t.manual_pdf_r2_key is null
      and t.r2_nurse_key is not null
      and t.r2_auth_key  is not null
      and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
      and tf.locked_by_invoice_id is null
      and (
        t.generated_pdf_at_utc is null
        or t.generated_pdf_refs_sig is null
        or (
          nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') is not null
          and sig.cur_refs_sig is not null
          and nullif(btrim(coalesce(t.generated_pdf_refs_sig,'')), '') <> sig.cur_refs_sig
        )
      )
    order by t.updated_at desc nulls last
    limit v_lim
  ),
  to_enqueue as (
    select
      e.timesheet_id,
      (e.refs_sig_mismatch is true) as force_regen
    from eligible e
    where
      (
        -- No existing READY_FOR_INVOICE outbox row: enqueue when dirty/missing OR refs mismatch.
        e.existing_outbox_id is null
      )
      or
      (
        -- Existing outbox row present: only upgrade to force_regen on first detection of mismatch.
        e.existing_outbox_id is not null
        and e.refs_sig_mismatch is true
        and e.existing_force_regen is not true
      )
  ),
  ins as (
    insert into public.ts_pdfs_outbox(
      timesheet_id,
      reason,
      attempt_count,
      next_attempt_at,
      last_error,
      prefer_generated,
      force_regen,
      created_at
    )
    select
      te.timesheet_id,
      'READY_FOR_INVOICE'::public.ts_pdf_reason_enum,
      0,
      null,
      null,
      false,
      te.force_regen,
      now()
    from to_enqueue te
    on conflict (timesheet_id, reason)
    do update
      set attempt_count   = case
                              when public.ts_pdfs_outbox.force_regen is false and excluded.force_regen is true
                                then 0
                              else public.ts_pdfs_outbox.attempt_count
                            end,
          next_attempt_at = case
                              when public.ts_pdfs_outbox.force_regen is false and excluded.force_regen is true
                                then null
                              else public.ts_pdfs_outbox.next_attempt_at
                            end,
          last_error      = case
                              when public.ts_pdfs_outbox.force_regen is false and excluded.force_regen is true
                                then null
                              else public.ts_pdfs_outbox.last_error
                            end,
          prefer_generated = public.ts_pdfs_outbox.prefer_generated or excluded.prefer_generated,
          force_regen      = public.ts_pdfs_outbox.force_regen or excluded.force_regen
      where public.ts_pdfs_outbox.force_regen is false and excluded.force_regen is true
    returning 1
  )
  select count(*) into v_ins from ins;

  return v_ins;
end;
$function$;

-- tspdf_work_fail_bulk(jsonb)
CREATE OR REPLACE FUNCTION public.tspdf_work_fail_bulk(p_rows jsonb)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  v_now timestamptz := now();
  v_count int := 0;
  r record;
begin
  if p_rows is null then return 0; end if;

  for r in
    select
      nullif(elem->>'outbox_id','')::uuid as outbox_id,
      left(coalesce(elem->>'error',''), 4000) as err
    from jsonb_array_elements(p_rows) as elem
  loop
    update public.ts_pdfs_outbox o
    set last_error = r.err,
        next_attempt_at = v_now + interval '30 minutes'
    where o.id = r.outbox_id;

    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$function$;

-- tspdf_work_success_bulk(uuid[])
CREATE OR REPLACE FUNCTION public.tspdf_work_success_bulk(p_ids uuid[])
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  v_count int := 0;
begin
  if p_ids is null or coalesce(array_length(p_ids, 1), 0) = 0 then
    return 0;
  end if;

  -- Atomically:
  -- 1) delete outbox rows (the ACK)
  -- 2) set generated_pdf_at_utc for the corresponding CURRENT timesheet rows
  with gone as (
    delete from public.ts_pdfs_outbox o
    where o.id = any(p_ids)
    returning o.timesheet_id
  ),
  upd as (
    update public.timesheets t
    set generated_pdf_at_utc = now()
    from (select distinct timesheet_id from gone) g
    where t.timesheet_id = g.timesheet_id
      and t.is_current = true
    returning 1
  )
  select count(*) into v_count
  from gone;

  return v_count;
end;
$function$;

-- umbrella_list_ids(jsonb)
CREATE OR REPLACE FUNCTION public.umbrella_list_ids(p_filters jsonb)
 RETURNS TABLE(id uuid)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_q text := null;
  v_bank_name text := null;
  v_sort_code text := null;
  v_account_number text := null;
  v_enabled text := null;
  v_vat_chargeable text := null;
  v_created_from timestamptz := null;
  v_created_to timestamptz := null;
  v_ids uuid[] := null;
begin
  if p_filters is null then
    p_filters := '{}'::jsonb;
  end if;

  v_q := nullif(btrim(coalesce(p_filters->>'q', coalesce(p_filters->>'name', ''))), '');
  v_bank_name := nullif(btrim(coalesce(p_filters->>'bank_name', '')), '');
  v_sort_code := nullif(btrim(coalesce(p_filters->>'sort_code', '')), '');
  v_account_number := nullif(btrim(coalesce(p_filters->>'account_number', '')), '');
  v_enabled := nullif(lower(btrim(coalesce(p_filters->>'enabled', ''))), '');
  v_vat_chargeable := nullif(lower(btrim(coalesce(p_filters->>'vat_chargeable', ''))), '');

  begin
    if nullif(btrim(coalesce(p_filters->>'created_from', '')), '') is not null then
      v_created_from := (p_filters->>'created_from')::timestamptz;
    end if;
  exception when others then
    v_created_from := null;
  end;

  begin
    if nullif(btrim(coalesce(p_filters->>'created_to', '')), '') is not null then
      v_created_to := (p_filters->>'created_to')::timestamptz;
    end if;
  exception when others then
    v_created_to := null;
  end;

  begin
    if p_filters ? 'ids' then
      if jsonb_typeof(p_filters->'ids') = 'array' then
        select array_agg(val::uuid)
        into v_ids
        from (
          select distinct nullif(btrim(e.value), '') as val
          from jsonb_array_elements_text(p_filters->'ids') as e(value)
        ) s
        where s.val is not null;
      elsif nullif(btrim(coalesce(p_filters->>'ids', '')), '') is not null then
        select array_agg(val::uuid)
        into v_ids
        from (
          select distinct nullif(btrim(x), '') as val
          from unnest(regexp_split_to_array(p_filters->>'ids', '\s*,\s*')) as u(x)
        ) s
        where s.val is not null;
      end if;
    end if;
  exception when others then
    v_ids := null;
  end;

  return query
  select u.id
  from public.umbrellas u
  where (v_ids is null or u.id = any(v_ids))
    and (
      v_q is null
      or u.name ilike ('%' || v_q || '%')
      or u.remittance_email ilike ('%' || v_q || '%')
    )
    and (v_bank_name is null or u.bank_name ilike ('%' || v_bank_name || '%'))
    and (v_sort_code is null or u.sort_code ilike ('%' || v_sort_code || '%'))
    and (v_account_number is null or u.account_number ilike ('%' || v_account_number || '%'))
    and (
      v_enabled is null
      or (v_enabled = 'true' and u.enabled = true)
      or (v_enabled = 'false' and u.enabled = false)
    )
    and (
      v_vat_chargeable is null
      or (v_vat_chargeable = 'true' and u.vat_chargeable = true)
      or (v_vat_chargeable = 'false' and u.vat_chargeable = false)
    )
    and (v_created_from is null or u.created_at >= v_created_from)
    and (v_created_to is null or u.created_at <= v_created_to)
  order by u.id;
end;
$function$;

-- weekly_import_apply_cancellations(uuid,jsonb,uuid)
CREATE OR REPLACE FUNCTION public.weekly_import_apply_cancellations(p_import_id uuid, p_actions jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_actions jsonb := coalesce(p_actions, '[]'::jsonb);
  v_cancelled_count int := 0;

  v_import_source_system text;
  v_import_client_id uuid;

  v_idx int;
  v_item jsonb;

  v_shift_id_text text;
  v_shift_id uuid;
  v_reason text;

  -- shift fields
  v_timesheet_id uuid;
  v_shift_invoice_id uuid;

  v_shift_source_system text;
  v_shift_candidate_id uuid;
  v_shift_client_id uuid;
  v_shift_contract_id uuid;
  v_shift_work_date date;
  v_shift_cancelled_at timestamptz;

  v_shift_external_row_key text;
  v_shift_hr_request_id text;
  v_shift_request_norm text;

  v_shift_start_utc timestamptz;
  v_shift_end_utc timestamptz;
  v_shift_break_mins int;
  v_shift_ward text;
  v_shift_week_ending_date date;

  -- ✅ evidence pointer: shift.latest_import_id (may be overridden by anchor evidence)
  v_shift_latest_import_id uuid;

  -- file request-id set
  v_file_request_count int := 0;
  v_present_in_file boolean := false;

  -- invoiced-at-all detection (segment-level)
  v_tf_locked_by_invoice_id uuid;
  v_tf_invoice_breakdown_json jsonb;

  v_seg_json jsonb := null;
  v_seg_invoice_id uuid;
  v_invoice_id_detected uuid := null;
  v_invoiced_detected boolean := false;

  v_branch text := null;

  -- correction timesheet creation
  v_base_ts_week_ending date;
  v_contract_week_ending_weekday_snapshot int := 0;
  v_week_ending_date date;

  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;

  v_client_name text;
  v_candidate_display_name text;
  v_candidate_tms_ref text;

  v_correction_id text;
  v_kind text := 'CANCEL_SHIFT_REVERSAL';

  v_shift_label text;
  v_shift_label_norm text;

  v_booking_base text;
  v_booking_id text;
  v_hash_hex text;

  v_schedule jsonb;
  v_hint jsonb;

  v_base_week_id uuid;

  v_existing_ts_id uuid;
  v_existing_cw_id uuid;

  v_cw_id uuid;
  v_next_additional_seq int;
  v_try int;

  v_ts_id uuid;
  v_correction_ts_id uuid;

  -- fnv1a32 helper vars (deterministic correction_id)
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;

  -- return arrays
  v_timesheet_ids uuid[] := array[]::uuid[];
  v_invoice_ids uuid[] := array[]::uuid[];
  v_credit_note_ids uuid[] := array[]::uuid[];
  v_pdf_jobs_enqueued int := 0;

  -- debug sample
  v_sample jsonb := '[]'::jsonb;
  v_sample_n int := 0;
  v_last_shift_id uuid := null;

  v_sqlstate text;
  v_err text;

  -- ✅ Cancellation anchor (what we reverse)
  v_anchor_start_utc timestamptz := null;
  v_anchor_end_utc timestamptz := null;
  v_anchor_break_mins int := 0;
  v_anchor_import_id uuid := null;

  -- ✅ Detect invoiced replacement (POS) to decide anchor
  v_pos_ts_id uuid := null;
  v_pos_schedule jsonb := null;
  v_pos_tf_locked_by_invoice_id uuid := null;
  v_pos_tf_invoice_breakdown_json jsonb := null;
  v_pos_seg_invoice_id uuid := null;
  v_pos_is_invoiced boolean := false;

  -- ✅ Base evidence import via existing CHANGED_HOURS_REVERSAL schedule (when POS is NOT invoiced)
  v_base_evidence_import_id uuid := null;

  -- ✅ Cleanup: remove uninvoiced CHANGED_HOURS corrections when cancelling (POS not invoiced)
  v_cleanup_ts_ids uuid[] := array[]::uuid[];
  v_cleanup_count int := 0;
  -- Canonical historical correction-chain evidence
  v_chain_scope jsonb := null;
  v_financial_preflight jsonb := null;
  v_correction_financials_policy_envelope jsonb := null;
  v_correction_financials_policy_envelope_fingerprint text := null;
  v_correction_operation_id uuid := null;
  v_root_timesheet_id uuid := null;
  v_latest_positive_timesheet_id uuid := null;
  v_reversal_ts_id uuid := null;
  v_replacement_ts_id uuid := null;
  v_replacement_cw_id uuid := null;
  v_replacement_booking_id text := null;
  v_pair_changed boolean := false;
  v_review_status text;
  v_review_operation_id uuid;
  v_review_input_shift_ids uuid[] := array[]::uuid[];
  v_review_selected_shift_ids uuid[] := array[]::uuid[];

begin
  -- Validate import exists and is HEALTHROSTER + has client_id (Guard B)
  select
    upper(coalesce(hi.source_system::text, '')),
    hi.client_id
  into
    v_import_source_system,
    v_import_client_id
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_import_source_system is null or v_import_source_system = '' then
    raise exception 'weekly_import_apply_cancellations: import % not found in hr_imports.', p_import_id;
  end if;

  if v_import_source_system <> 'HEALTHROSTER' then
    raise exception 'weekly_import_apply_cancellations: import % source_system=%; expected HEALTHROSTER.', p_import_id, v_import_source_system;
  end if;

  if v_import_client_id is null then
    raise exception 'weekly_import_apply_cancellations: import % has null client_id (cannot apply HR cancellations safely).', p_import_id;
  end if;

  -- Validate actions payload
  if jsonb_typeof(v_actions) <> 'array' then
    raise exception 'weekly_import_apply_cancellations: p_actions must be a JSON array.';
  end if;

  select s.status,s.last_operation_id into v_review_status,v_review_operation_id
  from public.import_review_states s where s.import_id=p_import_id;
  if found then
    if v_review_status<>'APPLYING' or v_review_operation_id is null then
      raise exception 'IMPORT_REVIEW_CANCELLATION_CONTEXT_REQUIRED' using errcode='55000';
    end if;
    if jsonb_array_length(v_actions)>500 or exists(
      select 1 from jsonb_array_elements(v_actions) a
      where jsonb_typeof(a)<>'object' or coalesce(a->>'shift_id','') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) then raise exception 'IMPORT_REVIEW_CANCELLATION_ACTIONS_INVALID' using errcode='22023'; end if;
    select coalesce(array_agg(distinct (a->>'shift_id')::uuid order by (a->>'shift_id')::uuid),array[]::uuid[])
      into v_review_input_shift_ids from jsonb_array_elements(v_actions) a;
    select coalesce(array_agg(d.shift_id order by d.shift_id),array[]::uuid[])
      into v_review_selected_shift_ids
    from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.selected and d.action_kind='APPLY_CANCELLATION';
    if v_review_input_shift_ids is distinct from v_review_selected_shift_ids then
      raise exception 'IMPORT_REVIEW_CANCELLATION_ACTION_SET_MISMATCH' using errcode='40001';
    end if;
  end if;

  if jsonb_array_length(v_actions) = 0 then
    return jsonb_build_object(
      'import_id', p_import_id,
      'cancelled_count', 0,
      'affected_timesheet_ids', to_jsonb(array[]::uuid[]),
      'affected_invoice_ids', to_jsonb(array[]::uuid[]),
      'credit_note_ids_created', to_jsonb(array[]::uuid[]),
      'invoice_pdf_jobs_enqueued', 0
    );
  end if;

  -- Build file request-id set (identity key = HR Request ID)
  create temporary table tmp_file_request_set(
    req_norm text primary key,
    req_raw  text
  ) on commit drop;

  insert into tmp_file_request_set(req_norm, req_raw)
  select
    lower(regexp_replace(btrim(src.req_raw), '\s+', ' ', 'g')) as req_norm,
    src.req_raw as req_raw
  from (
    select distinct
      nullif(
        btrim(
          coalesce(
            nullif(r.hr_request_id, ''),
            nullif(r.payload_json->>'request_id','')
          )
        ),
        ''
      ) as req_raw
    from public.hr_rows r
    where r.import_id = p_import_id
  ) as src
  where src.req_raw is not null
  on conflict (req_norm) do nothing;

  select count(*)::int
  into v_file_request_count
  from tmp_file_request_set;

  -- Apply is transactional: any invalid selected row fails whole apply.
  for v_idx, v_item in
    select (e.ord)::int, e.value
    from jsonb_array_elements(v_actions) with ordinality as e(value, ord)
  loop
    -- reset per-item
    v_anchor_start_utc := null;
    v_anchor_end_utc := null;
    v_anchor_break_mins := 0;
    v_anchor_import_id := null;

    v_pos_ts_id := null;
    v_pos_schedule := null;
    v_pos_tf_locked_by_invoice_id := null;
    v_pos_tf_invoice_breakdown_json := null;
    v_pos_seg_invoice_id := null;
    v_pos_is_invoiced := false;

    v_base_evidence_import_id := null;

    v_cleanup_ts_ids := array[]::uuid[];
    v_cleanup_count := 0;
    v_chain_scope := null;
    v_financial_preflight := null;
    v_correction_financials_policy_envelope := null;
    v_correction_financials_policy_envelope_fingerprint := null;
    v_root_timesheet_id := null;
    v_latest_positive_timesheet_id := null;
    v_reversal_ts_id := null;
    v_replacement_ts_id := null;
    v_replacement_cw_id := null;
    v_replacement_booking_id := null;
    v_pair_changed := false;

    v_seg_json := null;
    v_seg_invoice_id := null;

    -- Policy: no force
    if (v_item ? 'force') then
      raise exception 'weekly_import_apply_cancellations: item % contains disallowed field "force" (policy forbids force/override).', v_idx;
    end if;

    v_shift_id_text := nullif(btrim(coalesce(v_item->>'shift_id','')), '');
    v_reason := nullif(btrim(coalesce(v_item->>'reason','')), '');

    if v_shift_id_text is null then
      raise exception 'weekly_import_apply_cancellations: item % missing "shift_id".', v_idx;
    end if;
    if v_reason is null then
      raise exception 'weekly_import_apply_cancellations: item % missing "reason".', v_idx;
    end if;

    begin
      v_shift_id := v_shift_id_text::uuid;
    exception when invalid_text_representation then
      raise exception 'weekly_import_apply_cancellations: item % has invalid shift_id "%".', v_idx, v_shift_id_text;
    end;

    v_last_shift_id := v_shift_id;

    -- Lock shift row + load required fields
    select
      ns.timesheet_id,
      ns.invoice_id,
      upper(coalesce(ns.source_system::text,'')) as shift_source_system,
      ns.candidate_id,
      ns.client_id,
      ns.contract_id,
      ns.work_date,
      ns.cancelled_at_utc,
      ns.external_row_key,
      ns.hr_request_id,
      ns.start_utc,
      ns.end_utc,
      ns.break_mins,
      ns.ward,
      ns.week_ending_date,
      ns.latest_import_id
    into
      v_timesheet_id,
      v_shift_invoice_id,
      v_shift_source_system,
      v_shift_candidate_id,
      v_shift_client_id,
      v_shift_contract_id,
      v_shift_work_date,
      v_shift_cancelled_at,
      v_shift_external_row_key,
      v_shift_hr_request_id,
      v_shift_start_utc,
      v_shift_end_utc,
      v_shift_break_mins,
      v_shift_ward,
      v_shift_week_ending_date,
      v_shift_latest_import_id
    from public.nhsp_shifts ns
    where ns.id = v_shift_id
    for update;

    if not found then
      raise exception 'weekly_import_apply_cancellations: item % shift % not found in nhsp_shifts.', v_idx, v_shift_id;
    end if;

    if v_shift_cancelled_at is not null then
      raise exception 'weekly_import_apply_cancellations: item % shift % is already cancelled (cancelled_at_utc not null).', v_idx, v_shift_id;
    end if;

    -- Guard: cancellations RPC only operates on HEALTHROSTER shifts
    if v_shift_source_system <> 'HEALTHROSTER' then
      raise exception 'weekly_import_apply_cancellations: item % shift % source_system=%; expected HEALTHROSTER.',
        v_idx, v_shift_id, v_shift_source_system;
    end if;

    -- Guard B: shift must belong to the import client
    if v_shift_client_id is null or v_shift_client_id <> v_import_client_id then
      raise exception 'weekly_import_apply_cancellations: item % shift % client_id mismatch (import_client_id=% shift_client_id=%).',
        v_idx, v_shift_id, v_import_client_id, v_shift_client_id;
    end if;

    if v_shift_contract_id is null then
      raise exception 'weekly_import_apply_cancellations: item % shift % missing contract_id.', v_idx, v_shift_id;
    end if;

    if v_shift_candidate_id is null then
      raise exception 'weekly_import_apply_cancellations: item % shift % missing candidate_id.', v_idx, v_shift_id;
    end if;

    if v_shift_work_date is null then
      raise exception 'weekly_import_apply_cancellations: item % shift % missing work_date.', v_idx, v_shift_id;
    end if;

    -- Guard A: require non-empty nhsp_shifts.hr_request_id
    if nullif(btrim(coalesce(v_shift_hr_request_id,'')), '') is null then
      raise exception
        'weekly_import_apply_cancellations: item % shift % has empty hr_request_id; cannot use request-id cancellation identity.',
        v_idx, v_shift_id;
    end if;

    v_shift_request_norm := lower(regexp_replace(btrim(v_shift_hr_request_id), '\s+', ' ', 'g'));

    -- Presence test: if request id is present in file, cancellation is not eligible
    select exists (
      select 1
      from tmp_file_request_set fr
      where fr.req_norm = v_shift_request_norm
    )
    into v_present_in_file;

    if v_present_in_file then
      raise exception
        'weekly_import_apply_cancellations: item % shift % hr_request_id is present in the import file; cancellation rejected (not missing).',
        v_idx, v_shift_id;
    end if;

    -- Derive week_ending_date for cleanup/pos lookup
    v_week_ending_date := v_shift_week_ending_date;
    if v_week_ending_date is null then
      select coalesce(c.week_ending_weekday_snapshot, 0)
      into v_contract_week_ending_weekday_snapshot
      from public.contracts c
      where c.id = v_shift_contract_id
      limit 1;

      v_week_ending_date :=
        (v_shift_work_date + (((v_contract_week_ending_weekday_snapshot - extract(dow from v_shift_work_date)::int + 7) % 7))::int)::date;
    end if;

    if v_week_ending_date is null then
      raise exception 'weekly_import_apply_cancellations: shift % cannot resolve week_ending_date.', v_shift_id;
    end if;

    -- ─────────────────────────────────────────────
    -- Invoiced-at-all detection (segment-level authoritative)
    -- Also capture matched segment JSON for anchor when POS not invoiced
    -- ─────────────────────────────────────────────
    v_tf_locked_by_invoice_id := null;
    v_tf_invoice_breakdown_json := null;
    v_seg_invoice_id := null;
    v_invoice_id_detected := null;
    v_invoiced_detected := false;

    if v_timesheet_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_tf_locked_by_invoice_id,
        v_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_timesheet_id
        and tf.is_current = true
      limit 1;

      begin
        select s2.seg
        into v_seg_json
        from (
          select s2.seg
          from jsonb_array_elements(
            case
              when v_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_tf_invoice_breakdown_json) = 'object'
               and upper(coalesce(v_tf_invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
               and jsonb_typeof(v_tf_invoice_breakdown_json->'segments') = 'array'
              then v_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s2(seg)
          where
            (s2.seg->>'nhsp_shift_id') = v_shift_id::text
            or (
              v_shift_external_row_key is not null
              and (s2.seg->>'external_row_key') = v_shift_external_row_key
            )
          order by
            case when (s2.seg->>'nhsp_shift_id') = v_shift_id::text then 0 else 1 end
          limit 1
        ) as s2;
      exception when others then
        v_seg_json := null;
      end;

      if v_seg_json is not null then
        begin
          v_seg_invoice_id := nullif(btrim(coalesce(v_seg_json->>'invoice_locked_invoice_id','')), '')::uuid;
        exception when others then
          v_seg_invoice_id := null;
        end;
      end if;
    end if;

    v_invoice_id_detected := coalesce(v_seg_invoice_id, v_tf_locked_by_invoice_id, v_shift_invoice_id);
    v_invoiced_detected := (v_invoice_id_detected is not null);

    if v_invoice_id_detected is not null then
      v_invoice_ids := array_append(v_invoice_ids, v_invoice_id_detected);
    end if;

    -- ─────────────────────────────────────────────

    if v_timesheet_id is not null then
      select public.import_timesheet_financial_preflight_v1(
        p_timesheet_ids := array[v_timesheet_id]::uuid[],
        p_action := 'IMPORT_CANCELLATION',
        p_actor_user_id := p_actor_user_id,
        p_expected_state_json := '{}'::jsonb,
        p_lock_rows := true,
        p_max_scope := 100
      ) into v_financial_preflight;

      if coalesce((v_financial_preflight->>'allowed')::boolean,false) is not true then
        raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED', errcode='P0001', detail=v_financial_preflight::text;
      end if;

      if v_financial_preflight->>'required_path' = 'CREATE_OR_UPDATE_CORRECTION_CHAIN'
         and v_invoiced_detected is false then
        select il.invoice_id
        into v_invoice_id_detected
        from public.invoice_lines il
        join public.invoices i on i.id=il.invoice_id
        where il.timesheet_id=v_timesheet_id
        order by coalesce(i.issued_at_utc,i.created_at) desc, il.invoice_id desc
        limit 1;

        if v_invoice_id_detected is null then
          raise exception using message='IMPORT_INVOICE_EVIDENCE_INCOMPLETE',errcode='P0001',
            detail=jsonb_build_object(
              'code','IMPORT_INVOICE_EVIDENCE_INCOMPLETE',
              'timesheet_id',v_timesheet_id,
              'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
            )::text;
        end if;

        v_invoiced_detected := true;
        v_invoice_ids := array_append(v_invoice_ids,v_invoice_id_detected);
      end if;
    end if;

    -- Branch: INPLACE vs CORRECTION
    -- ─────────────────────────────────────────────
    if v_invoiced_detected = false then
      v_branch := 'INPLACE';

      if v_timesheet_id is not null and exists (
        select 1
        from public.timesheets guard_ts
        left join public.timesheets_financials guard_tf
          on guard_tf.timesheet_id=guard_ts.timesheet_id and guard_tf.is_current=true
        where guard_ts.timesheet_id=v_timesheet_id
          and (guard_ts.authorised_at_server is not null or guard_tf.authorised_at_utc is not null)
          and not exists (
            select 1 from public.timesheets_financials paid_guard
            where paid_guard.timesheet_id=guard_ts.timesheet_id
              and paid_guard.paid_at_utc is not null
          )
      ) then
        raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED', errcode='P0001',
          detail=jsonb_build_object(
            'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_timesheet_id,
            'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
            'paid_uninvoiced_rollover_required',false
          )::text;
      end if;

      if v_timesheet_id is not null
         and exists (select 1 from public.timesheets_financials paid_tf where paid_tf.timesheet_id=v_timesheet_id and paid_tf.paid_at_utc is not null)
         and not exists (
           select 1 from public.timesheets_financials current_tf
           where current_tf.timesheet_id=v_timesheet_id and current_tf.is_current=true
             and current_tf.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
             and coalesce((current_tf.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
         ) then
        raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED', errcode='P0001',
          detail=jsonb_build_object(
            'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_timesheet_id,
            'required_path',jsonb_build_array(
              'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
            ),
            'invoice_policy_without_history','NOW',
            'replacement_timesheet_required',false
          )::text;
      end if;


      -- Cancel truth + detach
      update public.nhsp_shifts ns2
      set
        cancelled_at_utc = v_now,
        cancelled_by_import_id = p_import_id,
        cancelled_reason = v_reason,
        timesheet_id = null
      where ns2.id = v_shift_id;

      v_cancelled_count := v_cancelled_count + 1;

      -- TSFIN recompute required for base timesheet
      if v_timesheet_id is not null then
        v_timesheet_ids := array_append(v_timesheet_ids, v_timesheet_id);

        update public.timesheets_financials tfu
        set
          is_stale = true,
          stale_reason = 'IMPORT_CANCEL_DETACH',
          updated_at = v_now
        where tfu.is_current = true
          and tfu.timesheet_id = v_timesheet_id;

        perform public.enqueue_ts_financials_priority(array[v_timesheet_id]::uuid[], 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
      end if;

      v_correction_ts_id := null;

    else
      v_branch := 'CORRECTION';

      if v_timesheet_id is null then
        raise exception using message='CORRECTION_BASE_TIMESHEET_REQUIRED', errcode='P0001',
          detail=jsonb_build_object('code','CORRECTION_BASE_TIMESHEET_REQUIRED','shift_id',v_shift_id)::text;
      end if;

      select public.timesheet_correction_chain_scope_v1(
        v_timesheet_id, true, 32, 100
      ) into v_chain_scope;

      if coalesce((v_chain_scope->>'valid')::boolean,false) is not true then
        raise exception using message='CORRECTION_CHAIN_UNRESOLVED', errcode='P0001', detail=v_chain_scope::text;
      end if;

      v_root_timesheet_id := nullif(v_chain_scope->>'root_timesheet_id','')::uuid;
      v_latest_positive_timesheet_id := coalesce(
        nullif(v_chain_scope->>'latest_positive_timesheet_id','')::uuid,
        v_timesheet_id
      );
      v_correction_operation_id := public._ctms_import_correction_operation_find_v1(
        p_import_id,
        v_root_timesheet_id,
        v_shift_external_row_key,
        'CANCELLATION',
        'REVERSAL_ONLY'
      );
      update public.nhsp_shifts canonical_cancel
      set cancelled_at_utc=coalesce(canonical_cancel.cancelled_at_utc,v_now),
          cancelled_by_import_id=p_import_id
      where canonical_cancel.id=v_shift_id
        and (canonical_cancel.cancelled_by_import_id is null
             or canonical_cancel.cancelled_by_import_id=p_import_id);
      if not found then
        raise exception 'CANCELLATION_IMPORT_EVIDENCE_CONFLICT' using errcode='P0001';
      end if;
      v_correction_financials_policy_envelope := public.correction_financials_policy_resolve_v1(
        v_timesheet_id,
        v_correction_operation_id,
        v_shift_external_row_key,
        'CANCELLATION',
        null::text,
        true,
        32
      );
      v_correction_financials_policy_envelope_fingerprint :=
        v_correction_financials_policy_envelope ->> 'envelope_fingerprint';
      v_kind := 'CANCELLATION_REVERSAL';

      -- ✅ Determine cancellation anchor:
      -- Default anchor = current shift truth (fallback only)
      v_anchor_start_utc := v_shift_start_utc;
      v_anchor_end_utc := v_shift_end_utc;
      v_anchor_break_mins := greatest(0, coalesce(v_shift_break_mins, 0));
      v_anchor_import_id := v_shift_latest_import_id;

      -- Find latest POS (replacement) correction timesheet for this shift/week
      begin
        select
          tpos.timesheet_id,
          tpos.actual_schedule_json
        into
          v_pos_ts_id,
          v_pos_schedule
        from public.timesheets tpos
        where tpos.is_adjustment is true
          and tpos.is_current is true
          and tpos.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
          and tpos.contract_id = v_shift_contract_id
          and tpos.week_ending_date = v_week_ending_date
          and jsonb_typeof(tpos.actual_schedule_json) = 'array'
          and (
            tpos.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id', v_shift_id::text))
            or (
              v_shift_external_row_key is not null
              and tpos.actual_schedule_json @> jsonb_build_array(jsonb_build_object('external_row_key', v_shift_external_row_key))
            )
          )
        order by tpos.updated_at desc nulls last, tpos.created_at desc nulls last
        limit 1
        for update;
      exception when others then
        v_pos_ts_id := null;
        v_pos_schedule := null;
      end;

      if v_pos_ts_id is not null then
        select
          tf.locked_by_invoice_id,
          tf.invoice_breakdown_json
        into
          v_pos_tf_locked_by_invoice_id,
          v_pos_tf_invoice_breakdown_json
        from public.timesheets_financials tf
        where tf.timesheet_id = v_pos_ts_id
          and tf.is_current = true
        order by tf.created_at desc
        limit 1;

        begin
          select
            nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '')::uuid
          into v_pos_seg_invoice_id
          from (
            select s2.seg
            from jsonb_array_elements(
              case
                when v_pos_tf_invoice_breakdown_json is not null
                 and jsonb_typeof(v_pos_tf_invoice_breakdown_json)='object'
                 and jsonb_typeof(v_pos_tf_invoice_breakdown_json->'segments')='array'
                then v_pos_tf_invoice_breakdown_json->'segments'
                else '[]'::jsonb
              end
            ) s2(seg)
            where nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '') is not null
            limit 1
          ) as s2;
        exception when others then
          v_pos_seg_invoice_id := null;
        end;

        v_pos_is_invoiced :=
          (v_pos_tf_locked_by_invoice_id is not null)
          or (v_pos_seg_invoice_id is not null);

        -- If POS is invoiced, reverse POS (anchor = POS schedule)
        if v_pos_is_invoiced is true and v_pos_schedule is not null and jsonb_typeof(v_pos_schedule) = 'array' then
          begin
            v_anchor_start_utc := nullif(btrim(coalesce((v_pos_schedule->0)->>'start_utc','')), '')::timestamptz;
          exception when others then
            v_anchor_start_utc := v_shift_start_utc;
          end;

          begin
            v_anchor_end_utc := nullif(btrim(coalesce((v_pos_schedule->0)->>'end_utc','')), '')::timestamptz;
          exception when others then
            v_anchor_end_utc := v_shift_end_utc;
          end;

          begin
            v_anchor_break_mins := greatest(
              0,
              coalesce(nullif(btrim(coalesce((v_pos_schedule->0)->>'break_mins','')), '')::int, 0)
            );
          exception when others then
            v_anchor_break_mins := greatest(0, coalesce(v_shift_break_mins, 0));
          end;

          begin
            if ((v_pos_schedule->0) ? 'import_id')
              and nullif(btrim(coalesce((v_pos_schedule->0)->>'import_id','')), '') is not null
              and (v_pos_schedule->0)->>'import_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            then
              v_anchor_import_id := ((v_pos_schedule->0)->>'import_id')::uuid;
            end if;
          exception when others then
            null;
          end;
        end if;
      end if;

      -- If POS is NOT invoiced, anchor to base locked segment (and use base evidence import_id when available)
      if v_pos_is_invoiced is not true then
        -- Base evidence import id: from CHANGED_HOURS_REVERSAL schedule if present
        begin
          select
            nullif(btrim(coalesce((tneg.actual_schedule_json->0)->>'import_id','')), '')::uuid
          into v_base_evidence_import_id
          from public.timesheets tneg
          where tneg.is_adjustment is true
            and tneg.is_current is true
            and tneg.correction_kind = 'CHANGED_HOURS_REVERSAL'
            and tneg.contract_id = v_shift_contract_id
            and tneg.week_ending_date = v_week_ending_date
            and jsonb_typeof(tneg.actual_schedule_json)='array'
            and (
              tneg.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id', v_shift_id::text))
              or (
                v_shift_external_row_key is not null
                and tneg.actual_schedule_json @> jsonb_build_array(jsonb_build_object('external_row_key', v_shift_external_row_key))
              )
            )
          order by tneg.updated_at desc nulls last, tneg.created_at desc nulls last
          limit 1;
        exception when others then
          v_base_evidence_import_id := null;
        end;

        if v_base_evidence_import_id is not null then
          v_anchor_import_id := v_base_evidence_import_id;
        end if;

        if v_seg_json is not null then
          begin
            if nullif(btrim(coalesce(v_seg_json->>'start_utc','')), '') is not null then
              v_anchor_start_utc := (v_seg_json->>'start_utc')::timestamptz;
            end if;
          exception when others then
            null;
          end;

          begin
            if nullif(btrim(coalesce(v_seg_json->>'end_utc','')), '') is not null then
              v_anchor_end_utc := (v_seg_json->>'end_utc')::timestamptz;
            end if;
          exception when others then
            null;
          end;

          begin
            if nullif(btrim(coalesce(v_seg_json->>'break_mins','')), '') is not null then
              v_anchor_break_mins := greatest(0, (v_seg_json->>'break_mins')::int);
            end if;
          exception when others then
            null;
          end;
        end if;
      end if;

      if v_anchor_start_utc is null or v_anchor_end_utc is null then
        raise exception 'weekly_import_apply_cancellations: shift % missing anchor start/end; cannot create schedule-driven cancellation correction.', v_shift_id;
      end if;

      -- Deterministic correction id (fnv1a32 over stable string using anchor times)
      v_fnv_s :=
        coalesce(p_import_id::text,'') || '|' ||
        coalesce(v_shift_id::text,'') || '|' ||
        coalesce(v_shift_hr_request_id,'') || '|' ||
        coalesce(coalesce(v_shift_external_row_key,''),'') || '|' ||
        coalesce(coalesce(v_anchor_start_utc::text,''),'') || '|' ||
        coalesce(coalesce(v_anchor_end_utc::text,''),'') || '|' ||
        coalesce(coalesce(v_anchor_break_mins,0)::text,'');

      v_fnv_h := 2166136261;
      for v_fnv_i in 1..char_length(v_fnv_s) loop
        v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
        v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
      end loop;
      v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');

      v_correction_id := 'hrcan:' || p_import_id::text || ':' || v_shift_id::text || ':' || v_fnv_hex;

      -- Load contract display fields (best-effort; may be null)
      select
        c2.display_site,
        c2.ward_hint,
        c2.role
      into
        v_contract_display_site,
        v_contract_ward_hint,
        v_contract_role
      from public.contracts c2
      where c2.id = v_shift_contract_id
      limit 1;

      select cl.name
      into v_client_name
      from public.clients cl
      where cl.id = v_shift_client_id
      limit 1;

      select cand.display_name, cand.tms_ref
      into v_candidate_display_name, v_candidate_tms_ref
      from public.candidates cand
      where cand.id = v_shift_candidate_id
      limit 1;

      -- Schedule entry (anchor-based) + evidence import_id
      v_schedule := jsonb_build_array(
        jsonb_build_object(
          'date', v_shift_work_date::text,
          'ward', nullif(btrim(coalesce(v_shift_ward, v_contract_ward_hint, '')), ''),
          'start_utc', v_anchor_start_utc::text,
          'end_utc', v_anchor_end_utc::text,
          'start', to_char((v_anchor_start_utc at time zone 'Europe/London')::time, 'HH24:MI'),
          'end', to_char((v_anchor_end_utc at time zone 'Europe/London')::time, 'HH24:MI'),
          'break_mins', greatest(0, coalesce(v_anchor_break_mins, 0)),
          'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'shift_id', v_shift_id::text,
          'external_row_key', v_shift_external_row_key,
          'import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end
        )
      );

      v_hint := jsonb_build_object(
        'import_cancellation', jsonb_build_object(
          'import_id', p_import_id::text,
          'trigger_import_id', p_import_id::text,
          'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
          'key_type', 'HR_REQUEST_ID',
          'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'external_row_key', v_shift_external_row_key,
          'shift_id', v_shift_id::text,
          'correction_id', v_correction_id,
          'correction_kind', 'CANCELLATION_REVERSAL_ONLY',
              'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
          'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
          'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end
        )
      );
      v_hint := v_hint || jsonb_build_object(
        'correction_financials_policy_envelope',v_correction_financials_policy_envelope,
        'correction_financials_policy_envelope_fingerprint',v_correction_financials_policy_envelope_fingerprint,
        'root_timesheet_id',v_root_timesheet_id::text,
        'latest_positive_timesheet_id',v_latest_positive_timesheet_id::text
      );

      v_shift_label := 'weekly-hr-cancel-reversal-' || v_correction_id;

      v_shift_label_norm :=
        regexp_replace(
          regexp_replace(lower(trim(v_shift_label)), '\s+', ' ', 'g'),
          '[^\w\s\-@&\/,:]',
          '',
          'g'
        );

      v_booking_base :=
        'scope=WEEKLY' || '|' ||
        'contract_id=' || coalesce(v_shift_contract_id::text,'') || '|' ||
        'candidate_id=' || coalesce(v_shift_candidate_id::text,'') || '|' ||
        'client_id=' || coalesce(v_shift_client_id::text,'') || '|' ||
        'week_ending_date=' || coalesce(v_week_ending_date::text,'') || '|' ||
        'correction_id=' || coalesce(v_correction_id,'') || '|' ||
        'correction_kind=' || v_kind;

      v_hash_hex := substring(encode(extensions.digest(convert_to(v_booking_base, 'utf8'), 'sha256'::text), 'hex') from 1 for 16);
      v_booking_id := 'bk_' || v_hash_hex;

      -- Ensure base contract_week exists (seq=0). Do not overwrite if it exists.
      insert into public.contract_weeks(
        contract_id,
        week_ending_date,
        additional_seq,
        status,
        submission_mode_snapshot,
        timesheet_id,
        planned_schedule_json,
        created_at,
        updated_at,
        is_adjustment
      )
      values (
        v_shift_contract_id,
        v_week_ending_date,
        0,
        'OPEN'::public.contract_week_status_enum,
        'MANUAL'::public.submission_mode_enum,
        null,
        null,
        v_now,
        v_now,
        false
      )
      on conflict (contract_id, week_ending_date, additional_seq) do nothing;

      select cw0.id
      into v_base_week_id
      from public.contract_weeks cw0
      where cw0.contract_id = v_shift_contract_id
        and cw0.week_ending_date = v_week_ending_date
        and cw0.additional_seq = 0
      limit 1
      for update;

      if v_base_week_id is null then
        raise exception 'weekly_import_apply_cancellations: failed to ensure base contract_week exists (contract_id=% week_ending=%).',
          v_shift_contract_id, v_week_ending_date;
      end if;

      -- Idempotency: reuse existing correction timesheet (correction_id+kind)
      v_existing_ts_id := null;

      select t2.timesheet_id
      into v_existing_ts_id
      from public.timesheets t2
      where t2.correction_id = v_correction_id
        and t2.correction_kind in (v_kind, 'CANCEL_SHIFT_REVERSAL')
      order by
        case when t2.correction_kind=v_kind then 0 else 1 end,
        t2.is_current desc,
        t2.version desc,
        t2.timesheet_id
      limit 1
      for update;

      if v_existing_ts_id is not null then
        v_correction_ts_id := v_existing_ts_id;

        -- Ensure adjustment contract_week exists and links to the correction timesheet
        v_existing_cw_id := null;

        select cw2.id
        into v_existing_cw_id
        from public.contract_weeks cw2
        where cw2.timesheet_id = v_existing_ts_id
          and cw2.contract_id = v_shift_contract_id
          and cw2.week_ending_date = v_week_ending_date
        limit 1
        for update;

        if v_existing_cw_id is null then
          perform 1
          from public.contract_weeks cwlock
          where cwlock.contract_id = v_shift_contract_id
            and cwlock.week_ending_date = v_week_ending_date
          for update;

          v_try := 0;
          loop
            v_try := v_try + 1;
            if v_try > 10 then
              raise exception 'weekly_import_apply_cancellations: failed to allocate additional_seq after retries (contract_id=% week_ending=%).',
                v_shift_contract_id, v_week_ending_date;
            end if;

            select coalesce(max(cwmax.additional_seq), 0) + 1
            into v_next_additional_seq
            from public.contract_weeks cwmax
            where cwmax.contract_id = v_shift_contract_id
              and cwmax.week_ending_date = v_week_ending_date;

            begin
              insert into public.contract_weeks(
                contract_id,
                week_ending_date,
                additional_seq,
                is_adjustment,
                submission_mode_snapshot,
                status,
                created_at,
                updated_at,
                timesheet_id
              )
              values (
                v_shift_contract_id,
                v_week_ending_date,
                v_next_additional_seq,
                true,
                'MANUAL'::public.submission_mode_enum,
                'SUBMITTED'::public.contract_week_status_enum,
                v_now,
                v_now,
                v_existing_ts_id
              )
              returning id into v_existing_cw_id;

              exit;
            exception when unique_violation then
              v_existing_cw_id := null;
            end;
          end loop;
        end if;

        if exists (
          select 1
          from public.timesheets desired_reversal
          where desired_reversal.timesheet_id=v_existing_ts_id
            and (
              desired_reversal.actual_schedule_json is distinct from v_schedule
              or desired_reversal.parent_timesheet_id is distinct from v_latest_positive_timesheet_id
              or desired_reversal.contract_id is distinct from v_shift_contract_id
              or desired_reversal.week_ending_date is distinct from v_week_ending_date
              or desired_reversal.correction_id is distinct from v_correction_id
              or desired_reversal.correction_kind is distinct from v_kind
              or coalesce(desired_reversal.candidate_hint_text->>'correction_financials_policy_envelope_fingerprint','')
                   is distinct from coalesce(v_correction_financials_policy_envelope_fingerprint,'')
            )
        ) then
          if exists (
            select 1
            from public.timesheets legacy_reversal
            left join public.timesheets_financials legacy_financial
              on legacy_financial.timesheet_id=legacy_reversal.timesheet_id
             and legacy_financial.is_current=true
            where legacy_reversal.timesheet_id=v_existing_ts_id
              and legacy_reversal.correction_kind='CANCEL_SHIFT_REVERSAL'
              and (
                legacy_reversal.authorised_at_server is not null
                or legacy_financial.authorised_at_utc is not null
                or legacy_financial.paid_at_utc is not null
                or legacy_financial.locked_by_invoice_id is not null
                or exists(select 1 from public.invoice_lines legacy_line where legacy_line.timesheet_id=legacy_reversal.timesheet_id)
              )
          ) then
            raise exception using message='LEGACY_CANCELLATION_CORRECTION_MIGRATION_REQUIRED',errcode='P0001',
              detail=jsonb_build_object('code','LEGACY_CANCELLATION_CORRECTION_MIGRATION_REQUIRED','timesheet_id',v_existing_ts_id,'correction_id',v_correction_id)::text;
          end if;

          if exists (
            select 1
            from public.timesheets guarded_reversal
            left join public.timesheets_financials guarded_reversal_financial
              on guarded_reversal_financial.timesheet_id=guarded_reversal.timesheet_id
             and guarded_reversal_financial.is_current=true
            where guarded_reversal.timesheet_id=v_existing_ts_id
              and (
                guarded_reversal.authorised_at_server is not null
                or guarded_reversal_financial.authorised_at_utc is not null
                or guarded_reversal_financial.paid_at_utc is not null
                or guarded_reversal_financial.locked_by_invoice_id is not null
                or exists(select 1 from public.invoice_lines guarded_line where guarded_line.timesheet_id=guarded_reversal.timesheet_id)
              )
          ) then
            raise exception using message='CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED',errcode='P0001',
              detail=jsonb_build_object('code','CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED','timesheet_id',v_existing_ts_id,'required_path','PAIR_UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE')::text;
          end if;

          update public.timesheets tu
          set
            booking_id = v_booking_id,
            is_current = true,
            status = 'RECEIVED'::public.timesheet_status_enum,
            sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
            submission_mode = 'MANUAL'::public.submission_mode_enum,
            line_type = 'HOURS'::public.timesheet_line_type_enum,
            authorised_at_server = null,
            occupant_key_norm = lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_shift_candidate_id::text)),
            hospital_norm = lower(coalesce(v_contract_display_site, v_client_name, v_shift_client_id::text)),
            ward_norm = lower(coalesce(v_contract_ward_hint, 'contract')),
            job_title_norm = lower(coalesce(v_contract_role, 'weekly')),
            shift_label_norm = v_shift_label_norm,
            week_ending_date = v_week_ending_date,
            contract_id = v_shift_contract_id,
            manual_pdf_r2_key = null,
            actual_schedule_json = v_schedule,
            qr_payload_json = v_hint,
            candidate_hint_text = v_hint,
            is_adjustment = true,
            parent_timesheet_id = v_latest_positive_timesheet_id,
            correction_id = v_correction_id,
            correction_kind = v_kind,
            adjustment_origin = 'IMPORT_CANCELLATION',
            updated_at = v_now
          where tu.timesheet_id = v_existing_ts_id;

          v_pair_changed := true;
        end if;

      else
        -- Create a new adjustment contract_week and new correction timesheet linked to it
        perform 1
        from public.contract_weeks cwlock2
        where cwlock2.contract_id = v_shift_contract_id
          and cwlock2.week_ending_date = v_week_ending_date
        for update;

        v_try := 0;
        loop
          v_try := v_try + 1;
          if v_try > 10 then
            raise exception 'weekly_import_apply_cancellations: failed to allocate additional_seq after retries (contract_id=% week_ending=%).',
              v_shift_contract_id, v_week_ending_date;
          end if;

          select coalesce(max(cwmax2.additional_seq), 0) + 1
          into v_next_additional_seq
          from public.contract_weeks cwmax2
          where cwmax2.contract_id = v_shift_contract_id
            and cwmax2.week_ending_date = v_week_ending_date;

          begin
            insert into public.contract_weeks(
              contract_id,
              week_ending_date,
              additional_seq,
              is_adjustment,
              submission_mode_snapshot,
              status,
              created_at,
              updated_at
            )
            values (
              v_shift_contract_id,
              v_week_ending_date,
              v_next_additional_seq,
              true,
              'MANUAL'::public.submission_mode_enum,
              'SUBMITTED'::public.contract_week_status_enum,
              v_now,
              v_now
            )
            returning id into v_cw_id;

            exit;
          exception when unique_violation then
            v_cw_id := null;
          end;
        end loop;

        v_ts_id := null;

        insert into public.timesheets(
          booking_id,
          version,
          is_current,
          status,
          occupant_key_norm,
          hospital_norm,
          ward_norm,
          job_title_norm,
          shift_label_norm,
          week_ending_date,
          contract_id,
          submission_mode,
          manual_pdf_r2_key,
          line_type,
          sheet_scope,
          actual_schedule_json,
          additional_units_week,
          additional_units_per_day,
          day_references_json,
          authorised_at_server,
          qr_payload_json,
          is_adjustment,
          candidate_hint_text,
          parent_timesheet_id,
          correction_id,
          correction_kind,
          adjustment_origin,
          created_at,
          updated_at
        )
        values (
          v_booking_id,
          1,
          true,
          'RECEIVED'::public.timesheet_status_enum,
          lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_shift_candidate_id::text)),
          lower(coalesce(v_contract_display_site, v_client_name, v_shift_client_id::text)),
          lower(coalesce(v_contract_ward_hint, 'contract')),
          lower(coalesce(v_contract_role, 'weekly')),
          v_shift_label_norm,
          v_week_ending_date,
          v_shift_contract_id,
          'MANUAL'::public.submission_mode_enum,
          null,
          'HOURS'::public.timesheet_line_type_enum,
          'WEEKLY'::public.timesheet_scope_enum,
          v_schedule,
          '{}'::jsonb,
          '{}'::jsonb,
          null,
          null,
          v_hint,
          true,
          v_hint,
          v_latest_positive_timesheet_id,
          v_correction_id,
          v_kind,
          'IMPORT_CANCELLATION',
          v_now,
          v_now
        )
        returning timesheet_id into v_ts_id;

        v_correction_ts_id := v_ts_id;
        v_pair_changed := true;

        update public.contract_weeks cwlink
        set
          timesheet_id = v_correction_ts_id,
          status = 'SUBMITTED'::public.contract_week_status_enum,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          is_adjustment = true,
          updated_at = v_now
        where cwlink.id = v_cw_id;

      end if;


      v_reversal_ts_id := v_correction_ts_id;
      if exists (
        select 1 from public.timesheets legacy_replacement
        where legacy_replacement.correction_id=v_correction_id
          and legacy_replacement.correction_kind in ('CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
      ) then
        raise exception 'LEGACY_ZERO_HOUR_REPLACEMENT_REQUIRES_RECONCILIATION'
          using errcode='P0001',detail=jsonb_build_object('correction_id',v_correction_id)::text;
      end if;
      v_replacement_ts_id := null;
      v_correction_ts_id := v_reversal_ts_id;
      v_timesheet_ids := array_append(v_timesheet_ids,v_reversal_ts_id);
      if v_pair_changed then
        perform public.enqueue_ts_financials_priority(
          array[v_reversal_ts_id]::uuid[],
          'CONTEXT_CHANGED'::public.ts_fin_reason_enum
        );
      end if;

      -- Update truth (cancel) + detach; do NOT recompute base TSFIN here
      update public.nhsp_shifts ns3
      set
        cancelled_at_utc = v_now,
        cancelled_by_import_id = p_import_id,
        cancelled_reason = v_reason,
        timesheet_id = null
      where ns3.id = v_shift_id;

      v_cancelled_count := v_cancelled_count + 1;

      -- Retained financial history is never deleted. Existing changed-hours
      -- corrections stay in the chain and are reconciled by the reversal-only correction unit.
      v_cleanup_ts_ids := array[]::uuid[];
      v_cleanup_count := 0;

      -- ✅ User-facing audit (UNGATED): correction timesheet + invoice history
      begin
        if v_correction_ts_id is not null then
          perform public._audit_insert(
            'timesheets',
            v_correction_ts_id::text,
            'HR_IMPORT_CANCELLATION_CORRECTION_CREATED',
            null,
            jsonb_build_object(
              'trigger_import_id', p_import_id::text,
              'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
              'branch', v_branch,
              'shift_id', v_shift_id::text,
              'work_date', v_shift_work_date::text,
              'external_row_key', v_shift_external_row_key,
              'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'cancel_reason', v_reason,
              'invoice_id', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
              'correction_id', v_correction_id,
              'correction_kind', 'CANCELLATION_REVERSAL_ONLY',
              'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
              'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
              'retained_changed_hours_count', v_cleanup_count,
              'retained_timesheet_ids', to_jsonb(coalesce(v_cleanup_ts_ids, array[]::uuid[]))
            ),
            'IMPORT_CANCELLATION_CORRECTION',
            p_actor_user_id
          );
        end if;

        if v_invoice_id_detected is not null then
          perform public._inv_write_audit(
            p_actor_user_id,
            'HR_IMPORT_CANCELLATION_CORRECTION_CREATED',
            jsonb_build_object(
              'trigger_import_id', p_import_id::text,
              'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
              'shift_id', v_shift_id::text,
              'work_date', v_shift_work_date::text,
              'external_row_key', v_shift_external_row_key,
              'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'invoice_id', v_invoice_id_detected::text,
              'correction_timesheet_id', case when v_correction_ts_id is null then null else v_correction_ts_id::text end,
              'correction_id', v_correction_id,
              'correction_kind', 'CANCELLATION_REVERSAL_ONLY',
              'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
              'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
              'retained_changed_hours_count', v_cleanup_count
            ),
            'invoices',
            v_invoice_id_detected::text,
            null,
            'IMPORT_CANCELLATION_CORRECTION',
            null,
            null,
            null
          );
        end if;
      exception when others then
        null;
      end;

    end if;

    -- Debug sample (cap 30)
    if v_sample_n < 30 then
      v_sample := v_sample || jsonb_build_array(jsonb_build_object(
        'shift_id', v_shift_id::text,
        'key_type', 'HR_REQUEST_ID',
        'hr_request_id', v_shift_hr_request_id,
        'present_in_file', v_present_in_file,
        'timesheet_id', case when v_timesheet_id is null then null else v_timesheet_id::text end,
        'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
        'invoiced_detected', v_invoiced_detected,
        'branch', v_branch,
        'correction_timesheet_id', case when v_correction_ts_id is null then null else v_correction_ts_id::text end,
        'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
        'retained_changed_hours_count', v_cleanup_count
      ));
      v_sample_n := v_sample_n + 1;
    end if;

  end loop;

  -- Deduplicate arrays
  select coalesce(array_agg(distinct x), array[]::uuid[])
  into v_timesheet_ids
  from unnest(v_timesheet_ids) x
  where x is not null;

  select coalesce(array_agg(distinct x), array[]::uuid[])
  into v_invoice_ids
  from unnest(v_invoice_ids) x
  where x is not null;

  -- Debug audit (invoice_debug gated inside _imp_debug_audit)
  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_CANCEL_APPLY_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'key_type', 'HR_REQUEST_ID',
      'selected_count', jsonb_array_length(v_actions),
      'cancelled_count', v_cancelled_count,
      'file_request_count', v_file_request_count,
      'affected_timesheet_ids_count', coalesce(array_length(v_timesheet_ids, 1), 0),
      'affected_invoice_ids_count', coalesce(array_length(v_invoice_ids, 1), 0),
      'sample', v_sample
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  v_credit_note_ids := array[]::uuid[];
  v_pdf_jobs_enqueued := 0;

  return jsonb_build_object(
    'import_id', p_import_id,
    'cancelled_count', v_cancelled_count,
    'affected_timesheet_ids', to_jsonb(v_timesheet_ids),
    'affected_invoice_ids', to_jsonb(v_invoice_ids),
    'credit_note_ids_created', to_jsonb(v_credit_note_ids),
    'invoice_pdf_jobs_enqueued', v_pdf_jobs_enqueued
  );

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'HR_CANCEL_APPLY_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'key_type', 'HR_REQUEST_ID',
        'last_shift_id', case when v_last_shift_id is null then null else v_last_shift_id::text end,
        'selected_count', case when jsonb_typeof(v_actions) = 'array' then jsonb_array_length(v_actions) else null end,
        'cancelled_count', v_cancelled_count,
        'file_request_count', v_file_request_count,
        'sqlstate', v_sqlstate,
        'error', v_err
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

-- weekly_import_apply_phase2(uuid,text)
CREATE OR REPLACE FUNCTION public.weekly_import_apply_phase2(p_import_id uuid, p_system_type text)
 RETURNS TABLE(hr_row_id uuid, external_row_key text, work_date date, incoming_code text, candidate_id uuid, client_id uuid, week_ending_date date, contract_id uuid, action text, reason text, shift_updated boolean)
 LANGUAGE plpgsql
AS $function$
declare
  v_sys text := upper(trim(coalesce(p_system_type,'')));
  v_src public.hr_source_enum;
begin
  if v_sys not in ('NHSP','HR_WEEKLY') then
    raise exception 'weekly_import_apply_phase2: invalid p_system_type=% (expected NHSP or HR_WEEKLY)', p_system_type;
  end if;

  v_src := case
    when v_sys = 'NHSP' then 'NHSP'::public.hr_source_enum
    else 'HEALTHROSTER'::public.hr_source_enum
  end;

  return query
  with p2 as (
    select *
    from public.weekly_import_phase2(p_import_id, v_sys)
  ),

  -- Current shift state + informational finance flags (NOT used to block)
  cur as (
    select
      p2r.hr_row_id,
      p2r.external_row_key,
      p2r.work_date,
      p2r.incoming_code,
      p2r.candidate_id as p2_candidate_id,
      p2r.client_id    as p2_client_id,
      p2r.week_ending_date as p2_week_ending_date,
      p2r.contract_id  as p2_contract_id,
      p2r.action       as p2_action,
      p2r.reason       as p2_reason,

      s.id            as shift_id,
      s.timesheet_id  as existing_timesheet_id,
      s.candidate_id  as existing_candidate_id,
      s.client_id     as existing_client_id,
      s.contract_id   as existing_contract_id,
      s.week_ending_date as existing_week_ending_date,

      (ts.timesheet_id is not null) as existing_timesheet_exists,

      fin.locked_by_invoice_id as fin_locked_by_invoice_id,
      fin.paid_at_utc          as fin_paid_at_utc,

      -- Only detach/reassign if mapping differs vs Phase2
      (
        (s.candidate_id is distinct from p2r.candidate_id)
        or (s.client_id  is distinct from p2r.client_id)
        or (s.contract_id is distinct from p2r.contract_id)
      ) as needs_reassign,

      -- Ensure week_ending_date is persisted (required by ensure+attach stage)
      (
        s.week_ending_date is distinct from p2r.week_ending_date
      ) as needs_week_ending_update,

      -- Also detach if the shift points at a missing/deleted timesheet row
      (
        s.timesheet_id is not null
        and ts.timesheet_id is null
      ) as needs_relink_missing_timesheet,

      (
        coalesce(
          (
            (s.candidate_id is distinct from p2r.candidate_id)
            or (s.client_id  is distinct from p2r.client_id)
            or (s.contract_id is distinct from p2r.contract_id)
          ),
          false
        )
        or
        coalesce(
          (
            s.timesheet_id is not null
            and ts.timesheet_id is null
          ),
          false
        )
      ) as needs_detach

    from p2 p2r
    left join public.nhsp_shifts s
      on s.external_row_key = p2r.external_row_key
     and s.latest_import_id = p_import_id
     and s.source_system    = v_src
    left join public.timesheets ts
      on ts.timesheet_id = s.timesheet_id
    left join public.timesheets_financials fin
      on fin.timesheet_id = s.timesheet_id
     and fin.is_current   = true
  ),

  -- Apply Phase2 mapping into nhsp_shifts:
  -- POLICY: paid/locked does NOT block truth repair. If Phase2 says OK and mapping differs, detach+overwrite.
  upd as (
    update public.nhsp_shifts su
    set
      updated_at = now(),

      -- ✅ Persist week_ending_date computed by Phase2 (client_settings-driven)
      week_ending_date = cur.p2_week_ending_date,

      -- Update mapping keys from Phase2
      contract_id = cur.p2_contract_id,
      candidate_id = coalesce(cur.p2_candidate_id, su.candidate_id),
      client_id    = coalesce(cur.p2_client_id, su.client_id),

      -- Detach if mapping differs OR if timesheet row is missing (deleted), so downstream ensure+attach can relink.
      timesheet_id = case
        when coalesce(cur.needs_detach, false) then null
        else su.timesheet_id
      end

    from cur
    where cur.p2_action = 'OK'
      and cur.p2_contract_id is not null
      and cur.external_row_key is not null
      and cur.p2_week_ending_date is not null
      and su.external_row_key = cur.external_row_key
      and su.latest_import_id = p_import_id
      and su.source_system    = v_src
      and (
        coalesce(cur.needs_detach,false) = true
        or su.contract_id is distinct from cur.p2_contract_id
        or su.candidate_id is distinct from cur.p2_candidate_id
        or su.client_id is distinct from cur.p2_client_id
        or su.week_ending_date is distinct from cur.p2_week_ending_date
      )
    returning su.external_row_key
  )

  select
    cur.hr_row_id,
    cur.external_row_key,
    cur.work_date,
    cur.incoming_code,
    cur.p2_candidate_id as candidate_id,
    cur.p2_client_id    as client_id,
    cur.p2_week_ending_date as week_ending_date,
    cur.p2_contract_id  as contract_id,

    -- No paid/locked blocking in Phase2. Keep Phase2 action as-is.
    cur.p2_action as action,

    -- Informational reason stitching for detach/week-ending update scenarios (no blocking)
    case
      when cur.p2_action = 'OK'
        and (
          coalesce(cur.needs_detach,false) = true
          or coalesce(cur.needs_week_ending_update,false) = true
        )
        and cur.shift_id is not null
      then
        (
          case
            when nullif(btrim(coalesce(cur.p2_reason,'')),'') is null then ''
            else cur.p2_reason || ' '
          end
        )
        ||
        (
          case
            when coalesce(cur.needs_reassign,false) is true and coalesce(cur.needs_relink_missing_timesheet,false) is true
              then 'Shift mapping changed and the shift was linked to a missing/deleted timesheet; truth was updated and shift detached for relink.'
            when coalesce(cur.needs_reassign,false) is true
              then 'Shift mapping changed; truth was updated and shift detached for relink.'
            when coalesce(cur.needs_relink_missing_timesheet,false) is true
              then 'Shift was linked to a missing/deleted timesheet; truth was updated and shift detached for relink.'
            when coalesce(cur.needs_week_ending_update,false) is true
              then 'Shift week_ending_date was updated from Phase2 to keep weekly attachment deterministic.'
            else 'Truth was updated.'
          end
        )
        ||
        (
          case
            when (cur.fin_locked_by_invoice_id is not null or cur.fin_paid_at_utc is not null)
              then ' Issued invoices remain immutable; any financial correction must be represented by standard reversal/adjustment artefacts.'
            else ''
          end
        )
      else cur.p2_reason
    end as reason,

    (u.external_row_key is not null) as shift_updated

  from cur
  left join upd u
    on u.external_row_key = cur.external_row_key;

end;
$function$;

-- weekly_import_changed_hours_phase3(uuid,text)
CREATE OR REPLACE FUNCTION public.weekly_import_changed_hours_phase3(p_import_id uuid, p_system_type text)
 RETURNS TABLE(hr_row_id uuid, external_row_key text, shift_id uuid, source_system text, candidate_id uuid, client_id uuid, contract_id uuid, timesheet_id uuid, contract_self_bill boolean, work_date date, week_ending_date date, old_start_utc timestamp with time zone, old_end_utc timestamp with time zone, old_break_mins integer, new_start_utc timestamp with time zone, new_end_utc timestamp with time zone, new_break_mins integer, old_paid_minutes integer, new_paid_minutes integer, is_changed_hours boolean, is_paid boolean, is_invoiced boolean, invoice_id_detected uuid, old_pay_ex numeric, old_charge_ex numeric, new_pay_ex numeric, new_charge_ex numeric, delta_pay_ex numeric, delta_charge_ex numeric, requires_pay_decision boolean, requires_invoice_decision boolean, requires_any_decision boolean)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_sys public.hr_source_enum;
begin
  v_sys :=
    case
      when upper(coalesce(p_system_type,'')) = 'NHSP' then 'NHSP'::public.hr_source_enum
      when upper(coalesce(p_system_type,'')) = 'HEALTHROSTER' then 'HEALTHROSTER'::public.hr_source_enum
      else null::public.hr_source_enum
    end;

  if v_sys is null then
    raise exception 'weekly_import_changed_hours_phase3: invalid p_system_type "%". Expected NHSP or HEALTHROSTER.', p_system_type;
  end if;

  return query
  with rows_in as (
    select
      r.id as hr_row_id,
      r.external_row_key,

      -- POLICY: shift "date" is derived from start time local date (Europe/London), not date_local.
      ((date_trunc('minute', (r.payload_json->>'start_utc')::timestamptz) at time zone 'Europe/London')::date) as work_date,

      date_trunc('minute', (r.payload_json->>'start_utc')::timestamptz) as new_start_utc,
      date_trunc('minute', (r.payload_json->>'end_utc')::timestamptz)   as new_end_utc,

      -- ✅ FIX: HealthRoster weekly uses Actual Break as authoritative.
      -- Priority: actual_break_mins / actual_break_minutes -> break_mins / break_minutes -> 0
      case
        when (r.payload_json ? 'actual_break_mins') and ((r.payload_json->>'actual_break_mins') ~ '^[0-9]+$')
          then (r.payload_json->>'actual_break_mins')::int
        when (r.payload_json ? 'actual_break_minutes') and ((r.payload_json->>'actual_break_minutes') ~ '^[0-9]+$')
          then (r.payload_json->>'actual_break_minutes')::int
        when (r.payload_json ? 'break_mins') and ((r.payload_json->>'break_mins') ~ '^[0-9]+$')
          then (r.payload_json->>'break_mins')::int
        when (r.payload_json ? 'break_minutes') and ((r.payload_json->>'break_minutes') ~ '^[0-9]+$')
          then (r.payload_json->>'break_minutes')::int
        else 0
      end as new_break_mins
    from public.hr_rows r
    where r.import_id = p_import_id
      and r.external_row_key is not null
      and (r.payload_json->>'start_utc') is not null
      and (r.payload_json->>'end_utc')   is not null
  ),
  matched as (
    select
      ri.*,
      s.id as shift_id,
      s.source_system::text as source_system,
      s.candidate_id,
      s.client_id,
      s.contract_id,
      s.timesheet_id,

      -- week_ending_date resolution (DO NOT assume Sunday):
      -- 1) base timesheet week_ending_date (authoritative) if present
      -- 2) nhsp_shifts.week_ending_date if present
      -- 3) derived from contracts.week_ending_weekday_snapshot (0=Sun) and basis_date (old shift start local date, else import work_date)
      coalesce(
        ts.week_ending_date,
        s.week_ending_date,
        (
          coalesce(
            (date_trunc('minute', s.start_utc) at time zone 'Europe/London')::date,
            ri.work_date
          )
          +
          (
            (
              (
                case
                  when c.week_ending_weekday_snapshot is null then 0
                  when c.week_ending_weekday_snapshot between 0 and 6 then c.week_ending_weekday_snapshot
                  else 0
                end
                -
                extract(dow from coalesce(
                  (date_trunc('minute', s.start_utc) at time zone 'Europe/London')::date,
                  ri.work_date
                ))::int
                + 7
              ) % 7
            )::int
          )
        )::date
      ) as week_ending_date,

      -- old values truncated to minute precision for comparison + output consistency
      date_trunc('minute', s.start_utc) as old_start_utc,
      date_trunc('minute', s.end_utc)   as old_end_utc,
      coalesce(s.break_mins,0) as old_break_mins,
      coalesce(s.pay_minutes,0) as old_paid_minutes,

      s.invoice_id as shift_invoice_id,

      c.self_bill as contract_self_bill
    from rows_in ri
    left join public.nhsp_shifts s
      on s.external_row_key = ri.external_row_key
     and s.source_system = v_sys
     and s.cancelled_at_utc is null
    left join public.contracts c
      on c.id = s.contract_id
    left join public.timesheets ts
      on ts.timesheet_id = s.timesheet_id
     and ts.is_current = true
  ),
  fin as (
    select
      m.*,
      tf.id as tsfin_id,
      tf.paid_at_utc,
      tf.locked_by_invoice_id,
      tf.invoice_breakdown_json,
      tf.policy_snapshot_json,
      tf.pay_day, tf.pay_night, tf.pay_sat, tf.pay_sun, tf.pay_bh,
      tf.charge_day, tf.charge_night, tf.charge_sat, tf.charge_sun, tf.charge_bh,
      (
        tf.paid_at_utc is not null
        or exists (
          select 1
          from public.pay_batch_items settled_item
          join public.pay_batch_candidates settled_candidate
            on settled_candidate.id = settled_item.pay_batch_candidate_id
          where settled_item.timesheet_id = m.timesheet_id
            and coalesce(settled_item.is_voided, false) = false
            and (
              upper(btrim(coalesce(settled_candidate.settlement_status, ''))) = 'SETTLED'
              or settled_candidate.settled_at_utc is not null
            )
        )
      ) as has_paid_evidence,
      invoice_line.invoice_id as invoice_line_invoice_id,
      chain_scope.chain_json as correction_chain_json
    from matched m
    left join public.timesheets_financials tf
      on tf.timesheet_id = m.timesheet_id
     and tf.is_current = true
    left join lateral (
      select il.invoice_id
      from public.invoice_lines il
      join public.invoices i on i.id = il.invoice_id
      where il.timesheet_id = m.timesheet_id
      order by coalesce(i.issued_at_utc, i.created_at) desc, il.invoice_id desc
      limit 1
    ) invoice_line on true
    left join lateral (
      select public.timesheet_correction_chain_scope_v1(
        m.timesheet_id,
        false,
        32,
        100
      ) as chain_json
      where m.timesheet_id is not null
    ) chain_scope on true
  ),
  seg_old as (
    select
      f.*,
      (seg->>'pay_amount')::numeric     as seg_old_pay_ex,
      (seg->>'charge_amount')::numeric  as seg_old_charge_ex,
      nullif(seg->>'invoice_locked_invoice_id','')::uuid as seg_invoice_id
    from fin f
    left join lateral (
      select t.seg
      from jsonb_array_elements(coalesce(f.invoice_breakdown_json->'segments','[]'::jsonb)) as t(seg)
      where (
        (t.seg->>'nhsp_shift_id') = f.shift_id::text
        or (t.seg->>'external_row_key') = f.external_row_key
      )
      order by
        case when (t.seg->>'nhsp_shift_id') = f.shift_id::text then 0 else 1 end
      limit 1
    ) x(seg) on true
  ),
  invline_old as (
    select
      s.*,
      case
        when upper(coalesce(s.source_system,'')) = 'NHSP' then (
          select max(il.total_charge_ex_vat)
          from public.invoice_lines il
          where il.meta_json->>'nhsp_shift_id' = s.shift_id::text
        )
        else null
      end as invline_old_charge_ex
    from seg_old s
  ),
  new_hours as (
    select
      a.*,
      h.hours_day, h.hours_night, h.hours_sat, h.hours_sun, h.hours_bh, h.total_hours,
      greatest(
        0,
        (extract(epoch from (a.new_end_utc - a.new_start_utc))/60)::int - coalesce(a.new_break_mins,0)
      ) as new_paid_minutes
    from invline_old a
    left join lateral public._wkimp_bucket_hours_from_policy(
      coalesce(a.policy_snapshot_json, '{}'::jsonb),
      a.new_start_utc,
      a.new_end_utc,
      a.new_break_mins
    ) h on true
  ),
  amounts as (
    select
      n.*,

      coalesce(n.seg_old_pay_ex, null) as old_pay_ex,
      coalesce(n.seg_old_charge_ex, n.invline_old_charge_ex, null) as old_charge_ex,

      case
        when n.policy_snapshot_json is null then null
        when coalesce((n.correction_chain_json->>'valid')::boolean, false) is false then null
        else round(
          coalesce(n.hours_day,0)   * coalesce(n.pay_day,0) +
          coalesce(n.hours_night,0) * coalesce(n.pay_night,0) +
          coalesce(n.hours_sat,0)   * coalesce(n.pay_sat,0) +
          coalesce(n.hours_sun,0)   * coalesce(n.pay_sun,0) +
          coalesce(n.hours_bh,0)    * coalesce(n.pay_bh,0)
        , 2)
      end as new_pay_ex,

      case
        when n.policy_snapshot_json is null then null
        when coalesce((n.correction_chain_json->>'valid')::boolean, false) is false then null
        else round(
          coalesce(n.hours_day,0)   * coalesce(n.charge_day,0) +
          coalesce(n.hours_night,0) * coalesce(n.charge_night,0) +
          coalesce(n.hours_sat,0)   * coalesce(n.charge_sat,0) +
          coalesce(n.hours_sun,0)   * coalesce(n.charge_sun,0) +
          coalesce(n.hours_bh,0)    * coalesce(n.charge_bh,0)
        , 2)
      end as new_charge_ex
    from new_hours n
  ),
  final_rows as (
    select
      a.hr_row_id,
      a.external_row_key,

      a.shift_id,
      a.source_system,

      a.candidate_id,
      a.client_id,
      a.contract_id,
      a.timesheet_id,

      a.contract_self_bill,

      -- POLICY: shift date is start-date (computed from new_start_utc).
      a.work_date as work_date,
      a.week_ending_date as week_ending_date,

      a.old_start_utc,
      a.old_end_utc,
      a.old_break_mins,

      a.new_start_utc,
      a.new_end_utc,
      a.new_break_mins,

      a.old_paid_minutes,
      a.new_paid_minutes,

      (
        a.shift_id is not null
        and (
          a.old_start_utc is distinct from a.new_start_utc
          or a.old_end_utc is distinct from a.new_end_utc
          or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
        )
      ) as is_changed_hours,

      a.has_paid_evidence as is_paid,

      (
        a.seg_invoice_id is not null
        or a.locked_by_invoice_id is not null
        or a.shift_invoice_id is not null
        or a.invoice_line_invoice_id is not null
      ) as is_invoiced,

      coalesce(
        a.seg_invoice_id,
        a.locked_by_invoice_id,
        a.shift_invoice_id,
        a.invoice_line_invoice_id
      ) as invoice_id_detected,

      a.old_pay_ex,
      a.old_charge_ex,

      a.new_pay_ex,
      a.new_charge_ex,

      case when a.new_pay_ex is null or a.old_pay_ex is null then null else round(a.new_pay_ex - a.old_pay_ex, 2) end as delta_pay_ex,
      case when a.new_charge_ex is null or a.old_charge_ex is null then null else round(a.new_charge_ex - a.old_charge_ex, 2) end as delta_charge_ex,

      (
        a.has_paid_evidence
        and (
          a.shift_id is not null
          and (
            a.old_start_utc is distinct from a.new_start_utc
            or a.old_end_utc is distinct from a.new_end_utc
            or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
          )
        )
      ) as requires_pay_decision,

      (
        (
          a.seg_invoice_id is not null
          or a.locked_by_invoice_id is not null
          or a.shift_invoice_id is not null
          or a.invoice_line_invoice_id is not null
        )
        and (
          a.shift_id is not null
          and (
            a.old_start_utc is distinct from a.new_start_utc
            or a.old_end_utc is distinct from a.new_end_utc
            or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
          )
        )
      ) as requires_invoice_decision,

      (
        (
          (
            a.has_paid_evidence
            or
            (
              a.seg_invoice_id is not null
              or a.locked_by_invoice_id is not null
              or a.shift_invoice_id is not null
              or a.invoice_line_invoice_id is not null
            )
          )
          and (
            a.shift_id is not null
            and (
              a.old_start_utc is distinct from a.new_start_utc
              or a.old_end_utc is distinct from a.new_end_utc
              or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
            )
          )
        )
        or (
          a.shift_id is not null
          and (
            a.old_start_utc is distinct from a.new_start_utc
            or a.old_end_utc is distinct from a.new_end_utc
            or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
          )
          and (
            coalesce((a.correction_chain_json->>'valid')::boolean,false) is false
          )
        )
      ) as requires_any_decision
    from amounts a
  )
  select fr.*
  from final_rows fr
  where fr.is_changed_hours = true
    and fr.timesheet_id is not null
  order by fr.work_date asc, fr.external_row_key asc;

end;
$function$;

-- weekly_import_create_cancellation_corrections(uuid,uuid,uuid)
CREATE OR REPLACE FUNCTION public.weekly_import_create_cancellation_corrections(p_shift_id uuid, p_import_id uuid, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_import_source_system public.hr_source_enum;
  v_import_client_id uuid;

  -- Shift authority
  v_shift_source_system public.hr_source_enum;
  v_shift_candidate_id uuid;
  v_shift_client_id uuid;
  v_shift_contract_id uuid;
  v_shift_timesheet_id uuid;
  v_shift_work_date date;
  v_shift_start_utc timestamptz;
  v_shift_end_utc timestamptz;
  v_shift_break_mins int;
  v_shift_pay_minutes int;
  v_shift_ref_num text;
  v_shift_hr_request_id text;
  v_shift_external_row_key text;
  v_shift_week_ending_date date;
  v_shift_ward text;

  -- Canonical historical chain and preflight authority
  v_chain_scope jsonb;
  v_financial_preflight jsonb;
  v_correction_financials_policy_envelope jsonb;
  v_correction_financials_policy_envelope_fingerprint text;
  v_correction_operation_id uuid;
  v_root_timesheet_id uuid;
  v_latest_positive_timesheet_id uuid;

  -- Historical reversal schedule
  v_anchor_segment jsonb;
  v_reversal_schedule jsonb;
  v_replacement_schedule jsonb := '[]'::jsonb;

  -- Week ending resolution
  v_week_ending_date date;
  v_base_ts_week_ending date;
  v_contract_week_ending_weekday_snapshot int := 0;

  -- Correction identity and member state
  v_correction_id text;
  v_kind text;
  v_member_schedule jsonb;
  v_member_hint jsonb;
  v_member_label_norm text;
  v_member_booking_id text;
  v_member_ts_id uuid;
  v_existing_ts_id uuid;
  v_existing_kind text;
  v_existing_cw_id uuid;
  v_member_cw_id uuid;
  v_base_week_id uuid;
  v_next_additional_seq int;
  v_try int;
  v_pair_changed boolean := false;
  v_pair_count int := 0;
  v_reversal_count int := 0;
  v_replacement_count int := 0;
  v_distinct_parent_count int := 0;

  v_reversal_ts_id uuid;
  v_replacement_ts_id uuid;
  v_created_timesheet_ids uuid[] := array[]::uuid[];

  -- Display / normalized identity
  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;
  v_client_name text;
  v_candidate_display_name text;
  v_candidate_tms_ref text;

  -- Stable identity hash
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;
  v_review_status text;
  v_review_operation_id uuid;

  v_sqlstate text;
  v_err text;
begin
  if p_shift_id is null or p_import_id is null then
    raise exception using message='CANCELLATION_CORRECTION_SCOPE_REQUIRED',errcode='22023',
      detail=jsonb_build_object('code','CANCELLATION_CORRECTION_SCOPE_REQUIRED','shift_id',p_shift_id,'import_id',p_import_id)::text;
  end if;

  if p_actor_user_id is null then
    raise exception using message='CANCELLATION_CORRECTION_ACTOR_REQUIRED',errcode='22023';
  end if;

  select hi.source_system, hi.client_id
  into v_import_source_system, v_import_client_id
  from public.hr_imports hi
  where hi.id=p_import_id
  limit 1;

  if not found then
    raise exception using message='CANCELLATION_CORRECTION_IMPORT_NOT_FOUND',errcode='P0002',
      detail=jsonb_build_object('code','CANCELLATION_CORRECTION_IMPORT_NOT_FOUND','import_id',p_import_id)::text;
  end if;

  select
    ns.source_system,
    ns.candidate_id,
    ns.client_id,
    ns.contract_id,
    ns.timesheet_id,
    ns.work_date,
    ns.start_utc,
    ns.end_utc,
    ns.break_mins,
    ns.pay_minutes,
    ns.ref_num,
    ns.hr_request_id,
    ns.external_row_key,
    ns.week_ending_date,
    ns.ward
  into
    v_shift_source_system,
    v_shift_candidate_id,
    v_shift_client_id,
    v_shift_contract_id,
    v_shift_timesheet_id,
    v_shift_work_date,
    v_shift_start_utc,
    v_shift_end_utc,
    v_shift_break_mins,
    v_shift_pay_minutes,
    v_shift_ref_num,
    v_shift_hr_request_id,
    v_shift_external_row_key,
    v_shift_week_ending_date,
    v_shift_ward
  from public.nhsp_shifts ns
  where ns.id=p_shift_id
  for update;

  if not found then
    raise exception using message='CANCELLATION_CORRECTION_SHIFT_NOT_FOUND',errcode='P0002',
      detail=jsonb_build_object('code','CANCELLATION_CORRECTION_SHIFT_NOT_FOUND','shift_id',p_shift_id)::text;
  end if;

  if v_shift_source_system is distinct from v_import_source_system then
    raise exception using message='CANCELLATION_CORRECTION_SOURCE_MISMATCH',errcode='P0001',
      detail=jsonb_build_object('code','CANCELLATION_CORRECTION_SOURCE_MISMATCH','import_source_system',v_import_source_system,'shift_source_system',v_shift_source_system)::text;
  end if;

  if v_import_client_id is null
     or v_shift_client_id is distinct from v_import_client_id then
    raise exception using message='CANCELLATION_CORRECTION_CLIENT_MISMATCH',errcode='P0001',
      detail=jsonb_build_object('code','CANCELLATION_CORRECTION_CLIENT_MISMATCH','import_client_id',v_import_client_id,'shift_client_id',v_shift_client_id)::text;
  end if;

  if v_shift_timesheet_id is null
     or v_shift_contract_id is null
     or v_shift_candidate_id is null
     or v_shift_client_id is null
     or v_shift_work_date is null then
    raise exception using message='CANCELLATION_CORRECTION_SHIFT_SCOPE_INCOMPLETE',errcode='P0001',
      detail=jsonb_build_object(
        'code','CANCELLATION_CORRECTION_SHIFT_SCOPE_INCOMPLETE',
        'shift_id',p_shift_id,
        'timesheet_id',v_shift_timesheet_id,
        'contract_id',v_shift_contract_id,
        'candidate_id',v_shift_candidate_id,
        'client_id',v_shift_client_id,
        'work_date',v_shift_work_date
      )::text;
  end if;

  select s.status,s.last_operation_id into v_review_status,v_review_operation_id
  from public.import_review_states s where s.import_id=p_import_id;
  if found then
    if v_review_status<>'APPLYING' or v_review_operation_id is null or not exists(
      select 1 from public.import_review_decisions d
      where d.import_id=p_import_id and d.is_current and d.selected
        and d.action_kind='APPLY_CANCELLATION' and d.shift_id=p_shift_id
    ) then raise exception 'IMPORT_REVIEW_CANCELLATION_CONTEXT_REQUIRED' using errcode='55000'; end if;
  end if;

  select public.timesheet_correction_chain_scope_v1(
    v_shift_timesheet_id, true, 32, 100
  ) into v_chain_scope;

  if coalesce((v_chain_scope->>'valid')::boolean,false) is not true then
    raise exception using message='CORRECTION_CHAIN_UNRESOLVED', errcode='P0001', detail=v_chain_scope::text;
  end if;

  v_root_timesheet_id := nullif(v_chain_scope->>'root_timesheet_id','')::uuid;
  v_latest_positive_timesheet_id := coalesce(
    nullif(v_chain_scope->>'latest_positive_timesheet_id','')::uuid,
    v_shift_timesheet_id
  );
  v_correction_operation_id := public._ctms_import_correction_operation_find_v1(
    p_import_id,
    v_root_timesheet_id,
    v_shift_external_row_key,
    'CANCELLATION',
    'REVERSAL_ONLY'
  );
  if not exists (
    select 1 from public.nhsp_shifts canonical_cancel
    where canonical_cancel.id=p_shift_id
      and canonical_cancel.cancelled_by_import_id=p_import_id
  ) then
    raise exception 'CANCELLATION_IMPORT_EVIDENCE_REQUIRED' using errcode='P0001';
  end if;
  v_correction_financials_policy_envelope := public.correction_financials_policy_resolve_v1(
    v_shift_timesheet_id,
    v_correction_operation_id,
    v_shift_external_row_key,
    'CANCELLATION',
    null::text,
    true,
    32
  );
  v_correction_financials_policy_envelope_fingerprint :=
    v_correction_financials_policy_envelope ->> 'envelope_fingerprint';

  v_financial_preflight := public.import_timesheet_financial_preflight_v1(
    p_timesheet_ids := array[v_shift_timesheet_id]::uuid[],
    p_action := 'IMPORT_CANCELLATION_CORRECTION_CREATE',
    p_actor_user_id := p_actor_user_id,
    p_expected_state_json := jsonb_build_object(
      'chain_fingerprints',jsonb_build_object(v_root_timesheet_id::text,v_chain_scope->>'chain_fingerprint')
    ),
    p_lock_rows := true,
    p_max_scope := 100
  );

  if coalesce((v_financial_preflight->>'allowed')::boolean,false) is not true then
    raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_financial_preflight::text;
  end if;

  -- Resolve the exact historical shift schedule from the effective positive
  -- timesheet first. This prevents a later-mutated source row from becoming the
  -- reversal authority.
  select member_segment.segment
  into v_anchor_segment
  from public.timesheets effective_positive
  cross join lateral jsonb_array_elements(
    case
      when jsonb_typeof(effective_positive.actual_schedule_json)='array'
        then effective_positive.actual_schedule_json
      else '[]'::jsonb
    end
  ) member_segment(segment)
  where effective_positive.timesheet_id=v_latest_positive_timesheet_id
    and (
      nullif(member_segment.segment->>'shift_id','')=p_shift_id::text
      or (
        nullif(v_shift_external_row_key,'') is not null
        and nullif(member_segment.segment->>'external_row_key','')=v_shift_external_row_key
      )
    )
  order by member_segment.segment->>'start_utc',member_segment.segment::text
  limit 1;

  if v_anchor_segment is null then
    select financial_segment.segment
    into v_anchor_segment
    from public.timesheets_financials historical_financial
    cross join lateral jsonb_array_elements(
      case
        when jsonb_typeof(historical_financial.invoice_breakdown_json->'segments')='array'
          then historical_financial.invoice_breakdown_json->'segments'
        else '[]'::jsonb
      end
    ) financial_segment(segment)
    where historical_financial.timesheet_id in (v_latest_positive_timesheet_id,v_root_timesheet_id)
      and (
        nullif(financial_segment.segment->>'nhsp_shift_id','')=p_shift_id::text
        or nullif(financial_segment.segment->>'shift_id','')=p_shift_id::text
        or (
          nullif(v_shift_external_row_key,'') is not null
          and nullif(financial_segment.segment->>'external_row_key','')=v_shift_external_row_key
        )
      )
    order by
      case when historical_financial.locked_by_invoice_id is not null then 0 else 1 end,
      case when historical_financial.paid_at_utc is not null then 0 else 1 end,
      historical_financial.computed_at_utc,
      historical_financial.id
    limit 1;
  end if;

  if v_anchor_segment is null
     or nullif(v_anchor_segment->>'start_utc','') is null
     or nullif(v_anchor_segment->>'end_utc','') is null then
    raise exception using message='CANCELLATION_REVERSAL_SCHEDULE_UNRESOLVED',errcode='P0001',
      detail=jsonb_build_object(
        'code','CANCELLATION_REVERSAL_SCHEDULE_UNRESOLVED',
        'shift_id',p_shift_id,
        'root_timesheet_id',v_root_timesheet_id,
        'latest_positive_timesheet_id',v_latest_positive_timesheet_id
      )::text;
  end if;

  v_reversal_schedule := jsonb_build_array(
    v_anchor_segment || jsonb_build_object(
      'date',coalesce(nullif(v_anchor_segment->>'date',''),v_shift_work_date::text),
      'ward',coalesce(nullif(v_anchor_segment->>'ward',''),nullif(v_shift_ward,'')),
      'shift_id',p_shift_id::text,
      'external_row_key',v_shift_external_row_key,
      'ref_num',coalesce(nullif(v_anchor_segment->>'ref_num',''),nullif(v_shift_ref_num,'')),
      'hr_request_id',coalesce(nullif(v_anchor_segment->>'hr_request_id',''),nullif(v_shift_hr_request_id,'')),
      'import_id',p_import_id::text
    )
  );

  -- Resolve week ending without assuming Sunday.
  select base_timesheet.week_ending_date
  into v_base_ts_week_ending
  from public.timesheets base_timesheet
  where base_timesheet.timesheet_id=v_latest_positive_timesheet_id
  limit 1;

  v_week_ending_date := coalesce(v_base_ts_week_ending,v_shift_week_ending_date);
  if v_week_ending_date is null then
    select coalesce(contract_row.week_ending_weekday_snapshot,0)
    into v_contract_week_ending_weekday_snapshot
    from public.contracts contract_row
    where contract_row.id=v_shift_contract_id
    limit 1;

    v_week_ending_date := (
      v_shift_work_date
      + (((v_contract_week_ending_weekday_snapshot-extract(dow from v_shift_work_date)::int+7)%7))::int
    )::date;
  end if;

  if v_week_ending_date is null then
    raise exception using message='CANCELLATION_CORRECTION_WEEK_UNRESOLVED',errcode='P0001';
  end if;

  -- Stable pair identity. Replays of the same import/shift/root use the same pair.
  v_fnv_s :=
    p_import_id::text||'|'||p_shift_id::text||'|'||v_root_timesheet_id::text||'|'||
    v_latest_positive_timesheet_id::text||'|'||v_correction_financials_policy_envelope_fingerprint;
  v_fnv_h := 2166136261;
  for v_fnv_i in 1..char_length(v_fnv_s) loop
    v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
    v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
  end loop;
  v_fnv_hex := lpad(lower(to_hex(v_fnv_h)),8,'0');
  v_correction_id := 'can:'||p_import_id::text||':'||p_shift_id::text||':'||v_fnv_hex;

  select contract_row.display_site,contract_row.ward_hint,contract_row.role
  into v_contract_display_site,v_contract_ward_hint,v_contract_role
  from public.contracts contract_row
  where contract_row.id=v_shift_contract_id
  limit 1;

  select client_row.name into v_client_name
  from public.clients client_row where client_row.id=v_shift_client_id limit 1;

  select candidate_row.display_name,candidate_row.tms_ref
  into v_candidate_display_name,v_candidate_tms_ref
  from public.candidates candidate_row where candidate_row.id=v_shift_candidate_id limit 1;

  insert into public.contract_weeks(contract_id,week_ending_date,additional_seq)
  values(v_shift_contract_id,v_week_ending_date,0)
  on conflict(contract_id,week_ending_date,additional_seq) do nothing;

  select base_week.id into v_base_week_id
  from public.contract_weeks base_week
  where base_week.contract_id=v_shift_contract_id
    and base_week.week_ending_date=v_week_ending_date
    and base_week.additional_seq=0
  limit 1 for update;

  if v_base_week_id is null then
    raise exception using message='CANCELLATION_CORRECTION_BASE_WEEK_UNRESOLVED',errcode='P0001';
  end if;

  foreach v_kind in array array['CANCELLATION_REVERSAL']::text[] loop
    v_member_ts_id := null;
    v_existing_ts_id := null;
    v_existing_kind := null;
    v_existing_cw_id := null;
    v_member_cw_id := null;

    v_member_schedule := case
      when v_kind='CANCELLATION_REVERSAL' then v_reversal_schedule
      else v_replacement_schedule
    end;

    v_member_hint := jsonb_build_object(
      'import_cancellation',jsonb_build_object(
        'import_id',p_import_id::text,
        'shift_id',p_shift_id::text,
        'source_system',v_shift_source_system::text,
        'external_row_key',v_shift_external_row_key,
        'ref_num',nullif(v_shift_ref_num,''),
        'hr_request_id',nullif(v_shift_hr_request_id,''),
        'correction_id',v_correction_id,
        'correction_kind',v_kind,
        'target','HISTORICAL_REVERSAL'
      ),
      'correction_financials_policy_envelope',v_correction_financials_policy_envelope,
      'correction_financials_policy_envelope_fingerprint',v_correction_financials_policy_envelope_fingerprint,
      'root_timesheet_id',v_root_timesheet_id::text,
      'latest_positive_timesheet_id',v_latest_positive_timesheet_id::text
    );

    v_member_label_norm := regexp_replace(
      lower('weekly-cancel-'||case when v_kind='CANCELLATION_REVERSAL' then 'reversal-' else 'replacement-' end||v_correction_id),
      '[^\w\s\-@&/,:.]','','g'
    );

    v_member_booking_id := 'bk_'||substring(
      encode(extensions.digest(convert_to(
        'scope=WEEKLY|contract_id='||v_shift_contract_id::text||'|week_ending_date='||v_week_ending_date::text||
        '|correction_id='||v_correction_id||'|correction_kind='||v_kind,
        'utf8'),'sha256'),'hex') from 1 for 16
    );

    v_existing_ts_id := null;
    v_existing_kind := null;

    select existing_member.timesheet_id,existing_member.correction_kind
    into v_existing_ts_id,v_existing_kind
    from public.timesheets existing_member
    where existing_member.correction_id=v_correction_id
      and (
        existing_member.correction_kind=v_kind
        or (v_kind='CANCELLATION_REVERSAL' and existing_member.correction_kind='CANCEL_SHIFT_REVERSAL')
      )
    order by
      case when existing_member.correction_kind=v_kind then 0 else 1 end,
      existing_member.is_current desc,
      existing_member.version desc,
      existing_member.timesheet_id
    limit 1 for update;

    if v_existing_ts_id is not null then
      v_member_ts_id := v_existing_ts_id;

      if exists (
        select 1 from public.timesheets desired_member
        where desired_member.timesheet_id=v_existing_ts_id
          and (
            desired_member.actual_schedule_json is distinct from v_member_schedule
            or desired_member.parent_timesheet_id is distinct from v_latest_positive_timesheet_id
            or desired_member.contract_id is distinct from v_shift_contract_id
            or desired_member.week_ending_date is distinct from v_week_ending_date
            or desired_member.correction_kind is distinct from v_kind
            or coalesce(desired_member.candidate_hint_text->>'correction_financials_policy_envelope_fingerprint','')
                 is distinct from v_correction_financials_policy_envelope_fingerprint
          )
      ) then
        if exists (
          select 1 from public.timesheets guarded_member
          left join public.timesheets_financials guarded_financial
            on guarded_financial.timesheet_id=guarded_member.timesheet_id and guarded_financial.is_current=true
          where guarded_member.timesheet_id=v_existing_ts_id
            and (
              guarded_member.authorised_at_server is not null
              or guarded_financial.authorised_at_utc is not null
              or guarded_financial.paid_at_utc is not null
              or guarded_financial.locked_by_invoice_id is not null
              or exists(select 1 from public.invoice_lines guarded_line where guarded_line.timesheet_id=guarded_member.timesheet_id)
            )
        ) then
          raise exception using
            message=case when v_existing_kind='CANCEL_SHIFT_REVERSAL' then 'LEGACY_CANCELLATION_CORRECTION_MIGRATION_REQUIRED' else 'CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED' end,
            errcode='P0001',
            detail=jsonb_build_object(
              'code',case when v_existing_kind='CANCEL_SHIFT_REVERSAL' then 'LEGACY_CANCELLATION_CORRECTION_MIGRATION_REQUIRED' else 'CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED' end,
              'timesheet_id',v_existing_ts_id,
              'correction_id',v_correction_id,
              'required_path','PAIR_UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE'
            )::text;
        end if;

        update public.timesheets update_member
        set
          booking_id=v_member_booking_id,
          is_current=true,
          status='RECEIVED'::public.timesheet_status_enum,
          occupant_key_norm=lower(coalesce(v_candidate_tms_ref,v_candidate_display_name,v_shift_candidate_id::text)),
          hospital_norm=lower(coalesce(v_contract_display_site,v_client_name,v_shift_client_id::text)),
          ward_norm=lower(coalesce(v_contract_ward_hint,'contract')),
          job_title_norm=lower(coalesce(v_contract_role,'weekly')),
          shift_label_norm=v_member_label_norm,
          week_ending_date=v_week_ending_date,
          contract_id=v_shift_contract_id,
          sheet_scope='WEEKLY'::public.timesheet_scope_enum,
          submission_mode='MANUAL'::public.submission_mode_enum,
          line_type='HOURS'::public.timesheet_line_type_enum,
          manual_pdf_r2_key=null,
          actual_schedule_json=v_member_schedule,
          additional_units_week='{}'::jsonb,
          additional_units_per_day='{}'::jsonb,
          day_references_json=null,
          authorised_at_server=null,
          qr_payload_json=v_member_hint,
          candidate_hint_text=v_member_hint,
          is_adjustment=true,
          parent_timesheet_id=v_latest_positive_timesheet_id,
          correction_id=v_correction_id,
          correction_kind=v_kind,
          adjustment_origin='IMPORT_CANCELLATION',
          updated_at=v_now
        where update_member.timesheet_id=v_existing_ts_id;

        v_pair_changed := true;
      end if;

      select existing_week.id into v_existing_cw_id
      from public.contract_weeks existing_week
      where existing_week.timesheet_id=v_existing_ts_id
        and existing_week.contract_id=v_shift_contract_id
        and existing_week.week_ending_date=v_week_ending_date
      order by existing_week.additional_seq,existing_week.id
      limit 1 for update;

      if v_existing_cw_id is null then
        if exists (
          select 1 from public.timesheets guarded_member
          left join public.timesheets_financials guarded_financial
            on guarded_financial.timesheet_id=guarded_member.timesheet_id and guarded_financial.is_current=true
          where guarded_member.timesheet_id=v_existing_ts_id
            and (
              guarded_member.authorised_at_server is not null
              or guarded_financial.authorised_at_utc is not null
              or guarded_financial.paid_at_utc is not null
              or guarded_financial.locked_by_invoice_id is not null
              or exists(select 1 from public.invoice_lines guarded_line where guarded_line.timesheet_id=guarded_member.timesheet_id)
            )
        ) then
          raise exception using message='CORRECTION_CONTRACT_WEEK_REPAIR_REQUIRED',errcode='P0001',
            detail=jsonb_build_object('code','CORRECTION_CONTRACT_WEEK_REPAIR_REQUIRED','timesheet_id',v_existing_ts_id)::text;
        end if;
      end if;
    else
      v_member_ts_id := null;
    end if;

    if v_existing_cw_id is null then
      perform 1 from public.contract_weeks week_lock
      where week_lock.contract_id=v_shift_contract_id
        and week_lock.week_ending_date=v_week_ending_date
      order by week_lock.id for update;

      v_try := 0;
      loop
        v_try := v_try+1;
        if v_try>10 then
          raise exception using message='CANCELLATION_CORRECTION_SEQUENCE_ALLOCATION_FAILED',errcode='P0001';
        end if;

        select coalesce(max(existing_seq.additional_seq),0)+1
        into v_next_additional_seq
        from public.contract_weeks existing_seq
        where existing_seq.contract_id=v_shift_contract_id
          and existing_seq.week_ending_date=v_week_ending_date;

        begin
          insert into public.contract_weeks(
            contract_id,week_ending_date,additional_seq,is_adjustment,
            submission_mode_snapshot,status,created_at,updated_at,timesheet_id
          )
          values(
            v_shift_contract_id,v_week_ending_date,v_next_additional_seq,true,
            'MANUAL'::public.submission_mode_enum,'SUBMITTED'::public.contract_week_status_enum,
            v_now,v_now,v_member_ts_id
          )
          returning id into v_member_cw_id;
          exit;
        exception when unique_violation then
          v_member_cw_id := null;
        end;
      end loop;
    else
      v_member_cw_id := v_existing_cw_id;
    end if;

    if v_member_ts_id is null then
      insert into public.timesheets(
        booking_id,version,is_current,status,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
        shift_label_norm,week_ending_date,contract_id,sheet_scope,submission_mode,line_type,
        manual_pdf_r2_key,actual_schedule_json,additional_units_week,additional_units_per_day,
        day_references_json,authorised_at_server,qr_payload_json,created_at,updated_at,is_adjustment,
        parent_timesheet_id,candidate_hint_text,correction_id,correction_kind,adjustment_origin
      )
      values(
        v_member_booking_id,1,true,'RECEIVED'::public.timesheet_status_enum,
        lower(coalesce(v_candidate_tms_ref,v_candidate_display_name,v_shift_candidate_id::text)),
        lower(coalesce(v_contract_display_site,v_client_name,v_shift_client_id::text)),
        lower(coalesce(v_contract_ward_hint,'contract')),
        lower(coalesce(v_contract_role,'weekly')),
        v_member_label_norm,v_week_ending_date,v_shift_contract_id,
        'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
        'HOURS'::public.timesheet_line_type_enum,null,v_member_schedule,'{}'::jsonb,'{}'::jsonb,
        null,null,v_member_hint,v_now,v_now,true,v_latest_positive_timesheet_id,v_member_hint,
        v_correction_id,v_kind,'IMPORT_CANCELLATION'
      )
      returning timesheet_id into v_member_ts_id;

      update public.contract_weeks link_week
      set timesheet_id=v_member_ts_id,
          status='SUBMITTED'::public.contract_week_status_enum,
          submission_mode_snapshot='MANUAL'::public.submission_mode_enum,
          is_adjustment=true,
          updated_at=v_now
      where link_week.id=v_member_cw_id;

      v_pair_changed := true;
    elsif v_existing_cw_id is null then
      update public.contract_weeks link_existing_week
      set timesheet_id=v_member_ts_id,
          status='SUBMITTED'::public.contract_week_status_enum,
          submission_mode_snapshot='MANUAL'::public.submission_mode_enum,
          is_adjustment=true,
          updated_at=v_now
      where link_existing_week.id=v_member_cw_id;
    end if;

    if v_kind='CANCELLATION_REVERSAL' then
      v_reversal_ts_id := v_member_ts_id;
    else
      v_replacement_ts_id := v_member_ts_id;
    end if;
  end loop;

  select count(*)::int,count(distinct unit_member.parent_timesheet_id)::int
  into v_pair_count,v_distinct_parent_count
  from public.timesheets unit_member
  where unit_member.correction_id=v_correction_id
    and unit_member.is_current=true
    and unit_member.correction_kind='CANCELLATION_REVERSAL';

  if v_reversal_ts_id is null or v_pair_count<>1 or v_distinct_parent_count<>1 then
    raise exception using message='CANCELLATION_REVERSAL_UNIT_INCOMPLETE',errcode='P0001',
      detail=jsonb_build_object('correction_id',v_correction_id,'member_count',v_pair_count)::text;
  end if;
  if exists (
    select 1 from public.timesheets legacy_replacement
    where legacy_replacement.correction_id=v_correction_id
      and legacy_replacement.correction_kind in ('CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
  ) then
    raise exception 'LEGACY_ZERO_HOUR_REPLACEMENT_REQUIRES_RECONCILIATION'
      using errcode='P0001',detail=jsonb_build_object('correction_id',v_correction_id)::text;
  end if;
  v_replacement_ts_id:=null;
  v_created_timesheet_ids:=array[v_reversal_ts_id]::uuid[];

  if v_pair_changed then
    perform public.enqueue_ts_financials_priority(
      v_created_timesheet_ids,
      'CONTEXT_CHANGED'::public.ts_fin_reason_enum
    );
  end if;

  perform public._imp_debug_audit(
    p_actor_user_id,
    'WEEKLY_CANCEL_CORRECTION_CREATE_DEBUG',
    jsonb_build_object(
      'import_id',p_import_id::text,
      'shift_id',p_shift_id::text,
      'correction_id',v_correction_id,
      'correction_kind','CANCELLATION_REVERSAL_ONLY',
      'reversal_timesheet_id',v_reversal_ts_id::text,
      'replacement_timesheet_id',v_replacement_ts_id::text,
      'root_timesheet_id',v_root_timesheet_id::text,
      'latest_positive_timesheet_id',v_latest_positive_timesheet_id::text,
      'correction_financials_policy_envelope_fingerprint',v_correction_financials_policy_envelope_fingerprint,
      'week_ending_date',v_week_ending_date::text,
      'pair_changed',v_pair_changed
    ),
    'nhsp_shifts',p_shift_id::text,null,null,null,null
  );

  return jsonb_build_object(
    'import_id',p_import_id,
    'shift_id',p_shift_id,
    'correction_id',v_correction_id,
    'correction_kind','CANCELLATION_REVERSAL_ONLY',
    'reversal_timesheet_id',v_reversal_ts_id,
    'replacement_timesheet_id',v_replacement_ts_id,
    'created_timesheet_ids',to_jsonb(v_created_timesheet_ids),
    'root_timesheet_id',v_root_timesheet_id,
    'latest_positive_timesheet_id',v_latest_positive_timesheet_id,
    'correction_financials_policy_envelope',v_correction_financials_policy_envelope,
    'correction_financials_policy_envelope_fingerprint',v_correction_financials_policy_envelope_fingerprint,
    'chain_fingerprint',v_chain_scope->>'chain_fingerprint',
    'preflight_fingerprint',v_financial_preflight->>'preflight_fingerprint',
    'pair_changed',v_pair_changed
  );

exception when others then
  get stacked diagnostics v_sqlstate=returned_sqlstate,v_err=message_text;
  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'WEEKLY_CANCEL_CORRECTION_CREATE_ERROR',
      jsonb_build_object(
        'import_id',case when p_import_id is null then null else p_import_id::text end,
        'shift_id',case when p_shift_id is null then null else p_shift_id::text end,
        'sqlstate',v_sqlstate,
        'error',v_err
      ),
      'nhsp_shifts',case when p_shift_id is null then null else p_shift_id::text end,
      null,null,null,null
    );
  exception when others then
    null;
  end;
  raise;
end;
$function$;

-- weekly_import_phase2(uuid,text)
CREATE OR REPLACE FUNCTION public.weekly_import_phase2(p_import_id uuid, p_system_type text)
 RETURNS TABLE(hr_row_id uuid, external_row_key text, work_date date, incoming_code text, candidate_id uuid, client_id uuid, week_ending_date date, contract_id uuid, action text, reason text, matched_shift_id uuid, old_start_utc timestamp with time zone, old_end_utc timestamp with time zone, old_break_mins integer, old_paid_minutes integer, old_cancelled_at_utc timestamp with time zone, new_start_utc timestamp with time zone, new_end_utc timestamp with time zone, new_break_mins integer, new_paid_minutes integer, delta_paid_minutes integer, is_new boolean, is_noop boolean, is_changed boolean)
 LANGUAGE plpgsql
AS $function$
declare
  v_sys text := upper(trim(coalesce(p_system_type,'')));
  v_src public.hr_source_enum;

  -- debug (invoice_debug-gated)
  v_invoice_debug boolean := false;
  v_dbg jsonb := null;
  v_rows_out int := 0;

  v_sqlstate text;
  v_err text;
begin
  if v_sys not in ('NHSP','HR_WEEKLY') then
    raise exception 'weekly_import_phase2: invalid p_system_type=% (expected NHSP or HR_WEEKLY)', p_system_type;
  end if;

  v_src :=
    case
      when v_sys = 'NHSP' then 'NHSP'::public.hr_source_enum
      else 'HEALTHROSTER'::public.hr_source_enum
    end;

  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  when others then
    v_invoice_debug := false;
  end;

  return query
  with imp as (
    select
      hi.id,
      hi.source_system,
      hi.client_id as hr_client_id
    from public.hr_imports hi
    where hi.id = p_import_id
    limit 1
  ),
  raw as (
    select
      r.id as hr_row_id,
      r.external_row_key,
      r.date_local as work_date,

      -- staff_name
      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif((r.payload_json ->> 'worker_name'), ''),
        nullif((r.payload_json ->> 'name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      ) as staff_name,

      -- ward/unit (for display / debugging only)
      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif((r.payload_json ->> 'unit'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      ) as ward,

      -- trust_raw (NHSP only)
      coalesce(
        nullif((r.payload_json ->> 'trust'), ''),
        nullif((r.payload_json ->> 'hospital_or_trust'), ''),
        nullif(r.unit_raw, '')
      ) as trust_raw,

      -- ✅ minute-truncated new values (stable comparisons)
      date_trunc('minute', (r.payload_json ->> 'start_utc')::timestamptz) as new_start_utc,
      date_trunc('minute', (r.payload_json ->> 'end_utc')::timestamptz)   as new_end_utc,

      -- ✅ FIX: HealthRoster uses Actual Break as authoritative (priority: actual_break_* then break_* then 0)
      greatest(
        0,
        coalesce(
          nullif(btrim(coalesce(r.payload_json ->> 'actual_break_mins','')), '')::int,
          nullif(btrim(coalesce(r.payload_json ->> 'actual_break_minutes','')), '')::int,
          nullif(btrim(coalesce(r.payload_json ->> 'break_mins','')), '')::int,
          nullif(btrim(coalesce(r.payload_json ->> 'break_minutes','')), '')::int,
          0
        )
      ) as new_break_mins,

      greatest(
        0,
        (floor(extract(epoch from (
          date_trunc('minute', (r.payload_json ->> 'end_utc')::timestamptz)
          -
          date_trunc('minute', (r.payload_json ->> 'start_utc')::timestamptz)
        )) / 60.0))::int
        -
        greatest(
          0,
          coalesce(
            nullif(btrim(coalesce(r.payload_json ->> 'actual_break_mins','')), '')::int,
            nullif(btrim(coalesce(r.payload_json ->> 'actual_break_minutes','')), '')::int,
            nullif(btrim(coalesce(r.payload_json ->> 'break_mins','')), '')::int,
            nullif(btrim(coalesce(r.payload_json ->> 'break_minutes','')), '')::int,
            0
          )
        )
      ) as new_paid_minutes,

      -- incoming_code depends on system_type:
      case
        when v_sys = 'NHSP' then coalesce(
          nullif((r.payload_json ->> 'assignment_code'), ''),
          nullif((r.payload_json ->> 'assignment'), ''),
          nullif((r.payload_json ->> 'Request_Grade'), ''),
          nullif(r.assignment_grade_norm, '')
        )
        else coalesce(
          nullif((r.payload_json ->> 'grade_raw'), ''),
          nullif((r.payload_json ->> 'Grade'), ''),
          nullif((r.payload_json ->> 'Request_Grade'), ''),
          nullif(r.assignment_grade_norm, '')
        )
      end as incoming_code_raw
    from public.hr_rows r
    join imp
      on imp.id = r.import_id
    where r.import_id = p_import_id
      and r.date_local is not null
      and (r.payload_json ->> 'start_utc') is not null
      and (r.payload_json ->> 'end_utc')   is not null
  ),
  resolved_ids as (
    select
      src.*,
      n.staff_lc,
      n.staff_key,
      n.trust_lc,
      n.trust_key,

      -- candidate mapping precedence:
      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.candidate_id
      ) as candidate_id,

      -- client mapping:
      case
        when v_sys = 'NHSP' then coalesce(cli_alias.client_id, cli_name.client_id)
        else imp.hr_client_id
      end as client_id
    from raw src
    join imp on true
    cross join lateral (
      select
        nullif(lower(trim(coalesce(src.staff_name,''))), '') as staff_lc,
        nullif(regexp_replace(lower(coalesce(src.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_key,
        nullif(lower(trim(coalesce(src.trust_raw,''))), '') as trust_lc,
        nullif(regexp_replace(lower(coalesce(src.trust_raw,'')), '[^a-z0-9]+', '', 'g'), '') as trust_key
    ) n

    left join lateral (
      select c.id
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (n.staff_lc  is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_lc]::text[]))
          or
          (n.staff_key is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_key]::text[]))
        )
      limit 1
    ) cand_alias on true

    left join lateral (
      select hm.candidate_id
      from public.hr_name_mappings hm
      where hm.active = true
        and (
          (n.staff_lc  is not null and hm.hr_name_norm = n.staff_lc)
          or
          (n.staff_key is not null and hm.hr_name_norm = n.staff_key)
        )
      order by hm.created_at desc
      limit 1
    ) cand_map on (cand_alias.id is null)

    left join lateral (
      with matches as (
        select c.id as cid
        from public.candidates c
        where c.active = true
          and n.staff_key is not null
          and (
            regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_key
            or
            regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_key
          )
      )
      select
        case when count(*) = 1 then (array_agg(matches.cid order by matches.cid::text))[1] end as candidate_id
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)

    left join lateral (
      select ch.client_id
      from public.client_hospitals ch
      where v_sys = 'NHSP'
        and ch.hospital_name_norm is not null
        and (
          (n.trust_lc  is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_lc]::text[]))
          or
          (n.trust_key is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_key]::text[]))
        )
      limit 1
    ) cli_alias on (v_sys = 'NHSP')

    left join lateral (
      with matches as (
        select cl.id as clid
        from public.clients cl
        where v_sys = 'NHSP'
          and n.trust_key is not null
          and regexp_replace(lower(coalesce(cl.name,'')), '[^a-z0-9]+', '', 'g') = n.trust_key
      )
      select
        case when count(*) = 1 then (array_agg(matches.clid order by matches.clid::text))[1] end as client_id
      from matches
    ) cli_name on (v_sys = 'NHSP' and cli_alias.client_id is null)
  ),
  with_we as (
    select
      r.*,

      coalesce(cs.week_ending_weekday, 0)::int as we_dow,

      (r.work_date
        + (
            (coalesce(cs.week_ending_weekday, 0)::int - extract(dow from r.work_date)::int + 7) % 7
          )
      )::date as week_ending_date,

      lower(trim(coalesce(r.incoming_code_raw,''))) as code_norm
    from resolved_ids r
    left join lateral (
      select cs2.week_ending_weekday
      from public.client_settings cs2
      where cs2.client_id = r.client_id
      order by cs2.effective_from desc, cs2.created_at desc
      limit 1
    ) cs on true
  ),
  in_range_counts as (
    select
      w.*,
      coalesce(cr.in_range_count, 0) as in_range_count
    from with_we w
    left join lateral (
      select count(*)::int as in_range_count
      from public.contracts c
      where c.candidate_id = w.candidate_id
        and c.client_id    = w.client_id
        and c.start_date <= w.work_date
        and (c.end_date is null or c.end_date >= w.work_date)
    ) cr on true
  ),

  -- ✅ UPDATED: mapping precedence now supports candidate+client + target_contract_id
  chosen_maps as (
    select
      w.*,
      m.spec as map_spec,
      m.patterns as band_patterns,
      m.target_contract_id as map_target_contract_id,
      m.has_map as map_has_any
    from in_range_counts w
    left join lateral (
      with maps as (
        -- candidate + client (highest precedence)
        select
          abm.band_match_pattern,
          abm.target_contract_id,
          3 as spec
        from public.assignment_band_mappings abm
        where abm.active = true
          and upper(trim(abm.system_type)) = v_sys
          and lower(trim(abm.incoming_code)) = w.code_norm
          and w.candidate_id is not null
          and w.client_id is not null
          and abm.candidate_id = w.candidate_id
          and abm.client_id = w.client_id

        union all
        -- candidate-only
        select
          abm.band_match_pattern,
          abm.target_contract_id,
          2 as spec
        from public.assignment_band_mappings abm
        where abm.active = true
          and upper(trim(abm.system_type)) = v_sys
          and lower(trim(abm.incoming_code)) = w.code_norm
          and w.candidate_id is not null
          and abm.candidate_id = w.candidate_id
          and abm.client_id is null

        union all
        -- client-only
        select
          abm.band_match_pattern,
          abm.target_contract_id,
          1 as spec
        from public.assignment_band_mappings abm
        where abm.active = true
          and upper(trim(abm.system_type)) = v_sys
          and lower(trim(abm.incoming_code)) = w.code_norm
          and w.client_id is not null
          and abm.candidate_id is null
          and abm.client_id = w.client_id

        union all
        -- global
        select
          abm.band_match_pattern,
          abm.target_contract_id,
          0 as spec
        from public.assignment_band_mappings abm
        where abm.active = true
          and upper(trim(abm.system_type)) = v_sys
          and lower(trim(abm.incoming_code)) = w.code_norm
          and abm.candidate_id is null
          and abm.client_id is null
      ),
      mx as (
        select max(maps.spec) as m
        from maps
      )
      select
        mx.m as spec,

        -- all band patterns at the chosen spec (used when not contract-targeted)
        (select array_agg(lower(trim(m2.band_match_pattern)))
         from maps m2
         where m2.spec = mx.m
        ) as patterns,

        -- choose a single target_contract_id at the chosen spec (deterministic)
        (select m3.target_contract_id
         from maps m3
         where m3.spec = mx.m
           and m3.target_contract_id is not null
         order by m3.target_contract_id::text asc
         limit 1
        ) as target_contract_id,

        (mx.m is not null) as has_map
      from mx
    ) m on true
  ),

  -- ✅ UPDATED: choose contract by target_contract_id when present; otherwise by band patterns
  chosen_contract as (
    select
      w.*,
      coalesce(tc.contract_id, bc.contract_id) as contract_id,
      (w.map_target_contract_id is not null and tc.contract_id is null) as target_contract_invalid
    from chosen_maps w

    -- target contract (only if provided)
    left join lateral (
      select c.id as contract_id
      from public.contracts c
      where w.map_target_contract_id is not null
        and c.id = w.map_target_contract_id
        and c.candidate_id = w.candidate_id
        and c.client_id    = w.client_id
        and c.start_date <= w.work_date
        and (c.end_date is null or c.end_date >= w.work_date)
      limit 1
    ) tc on true

    -- band-pattern match (only if no target contract id)
    left join lateral (
      select c.id as contract_id
      from public.contracts c
      where w.map_target_contract_id is null
        and c.candidate_id = w.candidate_id
        and c.client_id    = w.client_id
        and c.start_date <= w.work_date
        and (c.end_date is null or c.end_date >= w.work_date)
        and w.band_patterns is not null
        and exists (
          select 1
          from unnest(w.band_patterns) p
          where position(lower(p) in lower(coalesce(c.band,''))) > 0
        )
      order by c.start_date desc nulls last, c.id desc
      limit 1
    ) bc on true
  ),

  matched_shift as (
    select
      w.*,

      s.id as matched_shift_id,
      date_trunc('minute', s.start_utc) as old_start_utc,
      date_trunc('minute', s.end_utc)   as old_end_utc,
      coalesce(s.break_mins, 0)::int    as old_break_mins,
      coalesce(s.pay_minutes, 0)::int   as old_paid_minutes,
      s.cancelled_at_utc               as old_cancelled_at_utc,

      s.timesheet_id                   as old_timesheet_id,
      (t.timesheet_id is not null)     as old_timesheet_exists
    from chosen_contract w
    left join public.nhsp_shifts s
      on s.external_row_key = w.external_row_key
     and s.source_system = v_src
    left join public.timesheets t
      on t.timesheet_id = s.timesheet_id
  )
  select
    ms.hr_row_id,
    ms.external_row_key,
    ms.work_date,
    nullif(trim(coalesce(ms.incoming_code_raw,'')),'') as incoming_code,
    ms.candidate_id,
    ms.client_id,
    ms.week_ending_date,
    ms.contract_id,

    case
      when (select count(*) from imp) = 0 then 'REJECT_IMPORT_NOT_FOUND'
      when v_sys = 'NHSP' and (select imp.source_system from imp) <> 'NHSP'::public.hr_source_enum then 'REJECT_SOURCE_SYSTEM_MISMATCH'
      when v_sys = 'HR_WEEKLY' and (select imp.source_system from imp) <> 'HEALTHROSTER'::public.hr_source_enum then 'REJECT_SOURCE_SYSTEM_MISMATCH'
      when ms.candidate_id is null then 'REJECT_NO_CANDIDATE'
      when ms.client_id is null then 'REJECT_NO_CLIENT'
      when ms.code_norm = '' then 'REJECT_BAD_ROW'
      when ms.in_range_count = 0 then 'REJECT_NO_CONTRACT'
      when coalesce(ms.map_has_any,false) is false then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
      when ms.map_target_contract_id is not null and ms.target_contract_invalid is true then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
      when ms.contract_id is null then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
      else 'OK'
    end as action,

    case
      when (select count(*) from imp) = 0 then 'Import not found'
      when v_sys = 'NHSP' and (select imp.source_system from imp) <> 'NHSP'::public.hr_source_enum
        then 'Import source_system is not NHSP'
      when v_sys = 'HR_WEEKLY' and (select imp.source_system from imp) <> 'HEALTHROSTER'::public.hr_source_enum
        then 'Import source_system is not HEALTHROSTER'
      when ms.candidate_id is null then 'No candidate mapping found for staff name'
      when ms.client_id is null then 'No client mapping found'
      when ms.code_norm = '' then 'Missing incoming_code (assignment/grade)'
      when ms.in_range_count = 0 then 'No active contract for candidate/client on this date'
      when coalesce(ms.map_has_any,false) is false
        then 'No band/contract mapping rows exist for this incoming_code at candidate+client/candidate/client/global scope'
      when ms.map_target_contract_id is not null and ms.target_contract_invalid is true
        then 'Mapping points to a contract that is not active for this candidate/client/date (or does not exist)'
      when ms.contract_id is null
        then 'No contract matches mapping rules (target contract invalid or band pattern did not match any in-range contract)'
      else ''
    end as reason,

    -- diff fields
    ms.matched_shift_id,
    ms.old_start_utc,
    ms.old_end_utc,
    ms.old_break_mins,
    ms.old_paid_minutes,
    ms.old_cancelled_at_utc,

    ms.new_start_utc,
    ms.new_end_utc,
    ms.new_break_mins,
    ms.new_paid_minutes,

    case
      when ms.matched_shift_id is null then null::int
      else (ms.new_paid_minutes - ms.old_paid_minutes)
    end as delta_paid_minutes,

    (ms.matched_shift_id is null) as is_new,

    (
      ms.matched_shift_id is not null
      and ms.old_cancelled_at_utc is null
      and ms.old_start_utc = ms.new_start_utc
      and ms.old_end_utc   = ms.new_end_utc
      and ms.old_break_mins = ms.new_break_mins
      and (
        v_sys <> 'NHSP'
        or (ms.old_timesheet_id is not null and ms.old_timesheet_exists is true)
      )
    ) as is_noop,

    (
      ms.matched_shift_id is not null
      and not (
        ms.matched_shift_id is not null
        and ms.old_cancelled_at_utc is null
        and ms.old_start_utc = ms.new_start_utc
        and ms.old_end_utc   = ms.new_end_utc
        and ms.old_break_mins = ms.new_break_mins
        and (
          v_sys <> 'NHSP'
          or (ms.old_timesheet_id is not null and ms.old_timesheet_exists is true)
        )
      )
    ) as is_changed
  from matched_shift ms;

  get diagnostics v_rows_out = row_count;

  -- Debug summary (invoice_debug only). Note: actor_user_id is not available here, so we log with NULL actor.
  if v_invoice_debug then
    begin
      with final_rows as (
        with imp as (
          select
            hi.id,
            hi.source_system,
            hi.client_id as hr_client_id
          from public.hr_imports hi
          where hi.id = p_import_id
          limit 1
        ),
        raw as (
          select
            r.id as hr_row_id,
            r.external_row_key,
            r.date_local as work_date,
            coalesce(
              nullif((r.payload_json ->> 'staff_name'), ''),
              nullif((r.payload_json ->> 'worker_name'), ''),
              nullif((r.payload_json ->> 'name'), ''),
              nullif(r.staff_raw, ''),
              nullif(r.staff_norm, '')
            ) as staff_name,
            coalesce(
              nullif((r.payload_json ->> 'ward'), ''),
              nullif((r.payload_json ->> 'unit'), ''),
              nullif(r.unit_hint, ''),
              nullif(r.unit_raw, '')
            ) as ward,
            coalesce(
              nullif((r.payload_json ->> 'trust'), ''),
              nullif((r.payload_json ->> 'hospital_or_trust'), ''),
              nullif(r.unit_raw, '')
            ) as trust_raw,
            date_trunc('minute', (r.payload_json ->> 'start_utc')::timestamptz) as new_start_utc,
            date_trunc('minute', (r.payload_json ->> 'end_utc')::timestamptz)   as new_end_utc,
            greatest(
              0,
              coalesce(
                nullif(btrim(coalesce(r.payload_json ->> 'actual_break_mins','')), '')::int,
                nullif(btrim(coalesce(r.payload_json ->> 'actual_break_minutes','')), '')::int,
                nullif(btrim(coalesce(r.payload_json ->> 'break_mins','')), '')::int,
                nullif(btrim(coalesce(r.payload_json ->> 'break_minutes','')), '')::int,
                0
              )
            ) as new_break_mins,
            greatest(
              0,
              (floor(extract(epoch from (
                date_trunc('minute', (r.payload_json ->> 'end_utc')::timestamptz)
                -
                date_trunc('minute', (r.payload_json ->> 'start_utc')::timestamptz)
              )) / 60.0))::int
              -
              greatest(
                0,
                coalesce(
                  nullif(btrim(coalesce(r.payload_json ->> 'actual_break_mins','')), '')::int,
                  nullif(btrim(coalesce(r.payload_json ->> 'actual_break_minutes','')), '')::int,
                  nullif(btrim(coalesce(r.payload_json ->> 'break_mins','')), '')::int,
                  nullif(btrim(coalesce(r.payload_json ->> 'break_minutes','')), '')::int,
                  0
                )
              )
            ) as new_paid_minutes,
            case
              when v_sys = 'NHSP' then coalesce(
                nullif((r.payload_json ->> 'assignment_code'), ''),
                nullif((r.payload_json ->> 'assignment'), ''),
                nullif((r.payload_json ->> 'Request_Grade'), ''),
                nullif(r.assignment_grade_norm, '')
              )
              else coalesce(
                nullif((r.payload_json ->> 'grade_raw'), ''),
                nullif((r.payload_json ->> 'Grade'), ''),
                nullif((r.payload_json ->> 'Request_Grade'), ''),
                nullif(r.assignment_grade_norm, '')
              )
            end as incoming_code_raw
          from public.hr_rows r
          join imp
            on imp.id = r.import_id
          where r.import_id = p_import_id
            and r.date_local is not null
            and (r.payload_json ->> 'start_utc') is not null
            and (r.payload_json ->> 'end_utc')   is not null
        ),
        resolved_ids as (
          select
            src.*,
            n.staff_lc,
            n.staff_key,
            n.trust_lc,
            n.trust_key,
            coalesce(
              cand_alias.id,
              cand_map.candidate_id,
              cand_exact_unique.candidate_id
            ) as candidate_id,
            case
              when v_sys = 'NHSP' then coalesce(cli_alias.client_id, cli_name.client_id)
              else imp.hr_client_id
            end as client_id
          from raw src
          join imp on true
          cross join lateral (
            select
              nullif(lower(trim(coalesce(src.staff_name,''))), '') as staff_lc,
              nullif(regexp_replace(lower(coalesce(src.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_key,
              nullif(lower(trim(coalesce(src.trust_raw,''))), '') as trust_lc,
              nullif(regexp_replace(lower(coalesce(src.trust_raw,'')), '[^a-z0-9]+', '', 'g'), '') as trust_key
          ) n
          left join lateral (
            select c.id
            from public.candidates c
            where c.nhsp_hr_name_aliases is not null
              and (
                (n.staff_lc  is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_lc]::text[]))
                or
                (n.staff_key is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_key]::text[]))
              )
            limit 1
          ) cand_alias on true
          left join lateral (
            select hm.candidate_id
            from public.hr_name_mappings hm
            where hm.active = true
              and (
                (n.staff_lc  is not null and hm.hr_name_norm = n.staff_lc)
                or
                (n.staff_key is not null and hm.hr_name_norm = n.staff_key)
              )
            order by hm.created_at desc
            limit 1
          ) cand_map on (cand_alias.id is null)
          left join lateral (
            with matches as (
              select c.id as cid
              from public.candidates c
              where c.active = true
                and n.staff_key is not null
                and (
                  regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_key
                  or
                  regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_key
                )
            )
            select
              case when count(*) = 1 then (array_agg(matches.cid order by matches.cid::text))[1] end as candidate_id
            from matches
          ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)
          left join lateral (
            select ch.client_id
            from public.client_hospitals ch
            where v_sys = 'NHSP'
              and ch.hospital_name_norm is not null
              and (
                (n.trust_lc  is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_lc]::text[]))
                or
                (n.trust_key is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_key]::text[]))
              )
            limit 1
          ) cli_alias on (v_sys = 'NHSP')
          left join lateral (
            with matches as (
              select cl.id as clid
              from public.clients cl
              where v_sys = 'NHSP'
                and n.trust_key is not null
                and regexp_replace(lower(coalesce(cl.name,'')), '[^a-z0-9]+', '', 'g') = n.trust_key
            )
            select
              case when count(*) = 1 then (array_agg(matches.clid order by matches.clid::text))[1] end as client_id
            from matches
          ) cli_name on (v_sys = 'NHSP' and cli_alias.client_id is null)
        ),
        with_we as (
          select
            r.*,
            (r.work_date
              + (
                  (coalesce(cs.week_ending_weekday, 0)::int - extract(dow from r.work_date)::int + 7) % 7
                )
            )::date as week_ending_date,
            lower(trim(coalesce(r.incoming_code_raw,''))) as code_norm
          from resolved_ids r
          left join lateral (
            select cs2.week_ending_weekday
            from public.client_settings cs2
            where cs2.client_id = r.client_id
            order by cs2.effective_from desc, cs2.created_at desc
            limit 1
          ) cs on true
        ),
        in_range_counts as (
          select
            w.*,
            coalesce(cr.in_range_count, 0) as in_range_count
          from with_we w
          left join lateral (
            select count(*)::int as in_range_count
            from public.contracts c
            where c.candidate_id = w.candidate_id
              and c.client_id    = w.client_id
              and c.start_date <= w.work_date
              and (c.end_date is null or c.end_date >= w.work_date)
          ) cr on true
        ),
        chosen_maps as (
          select
            w.*,
            m.spec as map_spec,
            m.patterns as band_patterns,
            m.target_contract_id as map_target_contract_id,
            m.has_map as map_has_any
          from in_range_counts w
          left join lateral (
            with maps as (
              select abm.band_match_pattern, abm.target_contract_id, 3 as spec
              from public.assignment_band_mappings abm
              where abm.active = true
                and upper(trim(abm.system_type)) = v_sys
                and lower(trim(abm.incoming_code)) = w.code_norm
                and w.candidate_id is not null
                and w.client_id is not null
                and abm.candidate_id = w.candidate_id
                and abm.client_id = w.client_id

              union all
              select abm.band_match_pattern, abm.target_contract_id, 2 as spec
              from public.assignment_band_mappings abm
              where abm.active = true
                and upper(trim(abm.system_type)) = v_sys
                and lower(trim(abm.incoming_code)) = w.code_norm
                and w.candidate_id is not null
                and abm.candidate_id = w.candidate_id
                and abm.client_id is null

              union all
              select abm.band_match_pattern, abm.target_contract_id, 1 as spec
              from public.assignment_band_mappings abm
              where abm.active = true
                and upper(trim(abm.system_type)) = v_sys
                and lower(trim(abm.incoming_code)) = w.code_norm
                and w.client_id is not null
                and abm.candidate_id is null
                and abm.client_id = w.client_id

              union all
              select abm.band_match_pattern, abm.target_contract_id, 0 as spec
              from public.assignment_band_mappings abm
              where abm.active = true
                and upper(trim(abm.system_type)) = v_sys
                and lower(trim(abm.incoming_code)) = w.code_norm
                and abm.candidate_id is null
                and abm.client_id is null
            ),
            mx as (select max(maps.spec) as m from maps)
            select
              mx.m as spec,
              (select array_agg(lower(trim(m2.band_match_pattern))) from maps m2 where m2.spec = mx.m) as patterns,
              (select m3.target_contract_id
               from maps m3
               where m3.spec = mx.m and m3.target_contract_id is not null
               order by m3.target_contract_id::text asc
               limit 1
              ) as target_contract_id,
              (mx.m is not null) as has_map
            from mx
          ) m on true
        ),
        chosen_contract as (
          select
            w.*,
            coalesce(tc.contract_id, bc.contract_id) as contract_id,
            (w.map_target_contract_id is not null and tc.contract_id is null) as target_contract_invalid
          from chosen_maps w
          left join lateral (
            select c.id as contract_id
            from public.contracts c
            where w.map_target_contract_id is not null
              and c.id = w.map_target_contract_id
              and c.candidate_id = w.candidate_id
              and c.client_id    = w.client_id
              and c.start_date <= w.work_date
              and (c.end_date is null or c.end_date >= w.work_date)
            limit 1
          ) tc on true
          left join lateral (
            select c.id as contract_id
            from public.contracts c
            where w.map_target_contract_id is null
              and c.candidate_id = w.candidate_id
              and c.client_id    = w.client_id
              and c.start_date <= w.work_date
              and (c.end_date is null or c.end_date >= w.work_date)
              and w.band_patterns is not null
              and exists (
                select 1
                from unnest(w.band_patterns) p
                where position(lower(p) in lower(coalesce(c.band,''))) > 0
              )
            order by c.start_date desc nulls last, c.id desc
            limit 1
          ) bc on true
        ),
        matched_shift as (
          select
            w.*,
            s.id as matched_shift_id,
            s.timesheet_id as old_timesheet_id,
            (t.timesheet_id is not null) as old_timesheet_exists,
            s.cancelled_at_utc as old_cancelled_at_utc,
            date_trunc('minute', s.start_utc) as old_start_utc,
            date_trunc('minute', s.end_utc) as old_end_utc,
            coalesce(s.break_mins,0)::int as old_break_mins
          from chosen_contract w
          left join public.nhsp_shifts s
            on s.external_row_key = w.external_row_key
           and s.source_system = v_src
          left join public.timesheets t
            on t.timesheet_id = s.timesheet_id
        )
        select
          ms.hr_row_id,
          ms.external_row_key,
          ms.work_date,
          ms.candidate_id,
          ms.client_id,
          ms.week_ending_date,
          ms.contract_id,
          case
            when (select count(*) from imp) = 0 then 'REJECT_IMPORT_NOT_FOUND'
            when v_sys = 'NHSP' and (select imp.source_system from imp) <> 'NHSP'::public.hr_source_enum then 'REJECT_SOURCE_SYSTEM_MISMATCH'
            when v_sys = 'HR_WEEKLY' and (select imp.source_system from imp) <> 'HEALTHROSTER'::public.hr_source_enum then 'REJECT_SOURCE_SYSTEM_MISMATCH'
            when ms.candidate_id is null then 'REJECT_NO_CANDIDATE'
            when ms.client_id is null then 'REJECT_NO_CLIENT'
            when ms.code_norm = '' then 'REJECT_BAD_ROW'
            when ms.in_range_count = 0 then 'REJECT_NO_CONTRACT'
            when coalesce(ms.map_has_any,false) is false then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
            when ms.map_target_contract_id is not null and ms.target_contract_invalid is true then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
            when ms.contract_id is null then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
            else 'OK'
          end as action,
          ms.matched_shift_id,
          ms.old_timesheet_id,
          ms.old_timesheet_exists,
          (ms.matched_shift_id is null) as is_new,
          (
            ms.matched_shift_id is not null
            and ms.old_cancelled_at_utc is null
            and ms.old_start_utc = ms.new_start_utc
            and ms.old_end_utc   = ms.new_end_utc
            and ms.old_break_mins = ms.new_break_mins
            and (
              v_sys <> 'NHSP'
              or (ms.old_timesheet_id is not null and ms.old_timesheet_exists is true)
            )
          ) as is_noop,
          (
            ms.matched_shift_id is not null
            and not (
              ms.matched_shift_id is not null
              and ms.old_cancelled_at_utc is null
              and ms.old_start_utc = ms.new_start_utc
              and ms.old_end_utc   = ms.new_end_utc
              and ms.old_break_mins = ms.new_break_mins
              and (
                v_sys <> 'NHSP'
                or (ms.old_timesheet_id is not null and ms.old_timesheet_exists is true)
              )
            )
          ) as is_changed
        from matched_shift ms
      )
      select jsonb_build_object(
        'import_id', p_import_id::text,
        'system_type', v_sys,
        'rows_returned', v_rows_out,

        'rows_total', count(*)::int,
        'ok_rows', count(*) filter (where fr.action = 'OK')::int,
        'reject_rows', count(*) filter (where fr.action <> 'OK')::int,

        'ok_new_rows', count(*) filter (where fr.action = 'OK' and fr.is_new is true)::int,
        'ok_noop_rows', count(*) filter (where fr.action = 'OK' and fr.is_noop is true)::int,
        'ok_changed_rows', count(*) filter (where fr.action = 'OK' and fr.is_changed is true)::int,

        -- attach-needed rows (NHSP) are those that would otherwise be noop but linkage is missing
        'ok_attach_needed_rows',
          count(*) filter (
            where fr.action = 'OK'
              and v_sys = 'NHSP'
              and fr.is_new is false
              and fr.is_noop is false
              and fr.is_changed is true
              and fr.matched_shift_id is not null
              and (
                fr.old_timesheet_id is null
                or fr.old_timesheet_exists is false
              )
          )::int,

        'missing_external_row_key_rows', count(*) filter (where fr.external_row_key is null)::int,
        'missing_candidate_rows', count(*) filter (where fr.candidate_id is null)::int,
        'missing_client_rows', count(*) filter (where fr.client_id is null)::int,
        'missing_contract_rows', count(*) filter (where fr.contract_id is null)::int,

        'sample_ok_changed_external_row_keys',
          coalesce(
            (
              select jsonb_agg(x.external_row_key)
              from (
                select fr2.external_row_key
                from final_rows fr2
                where fr2.action = 'OK'
                  and fr2.is_changed is true
                  and fr2.external_row_key is not null
                order by fr2.external_row_key
                limit 20
              ) as x
            ),
            '[]'::jsonb
          ),

        'sample_ok_new_external_row_keys',
          coalesce(
            (
              select jsonb_agg(x2.external_row_key)
              from (
                select fr3.external_row_key
                from final_rows fr3
                where fr3.action = 'OK'
                  and fr3.is_new is true
                  and fr3.external_row_key is not null
                order by fr3.external_row_key
                limit 20
              ) as x2
            ),
            '[]'::jsonb
          ),

        'reject_action_counts',
          coalesce(
            (
              select jsonb_agg(jsonb_build_object('action', y.action, 'count', y.cnt) order by y.cnt desc, y.action asc)
              from (
                select fr4.action, count(*)::int as cnt
                from final_rows fr4
                where fr4.action <> 'OK'
                group by fr4.action
                order by count(*) desc, fr4.action asc
                limit 30
              ) as y
            ),
            '[]'::jsonb
          )
      )
      into v_dbg
      from final_rows fr;

      perform public._imp_debug_audit(
        null,
        'WEEKLY_IMPORT_PHASE2_DEBUG',
        v_dbg,
        'hr_imports',
        p_import_id::text,
        null,
        null,
        null,
        null
      );
    exception when others then
      null;
    end;
  end if;

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      null,
      'WEEKLY_IMPORT_PHASE2_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'system_type', v_sys,
        'sqlstate', v_sqlstate,
        'error', v_err
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

