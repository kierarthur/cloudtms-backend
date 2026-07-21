-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: b49acc89c802.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
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
