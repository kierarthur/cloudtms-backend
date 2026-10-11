-- DY04: read-only Daily missing-used-bucket refusal in existing Authorise.
-- Original Source/query admission, lifecycle mutations and status refusal retained.
\set ON_ERROR_STOP on
begin;
CREATE OR REPLACE FUNCTION public.timesheet_authorise_generic_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_contract_week public.contract_weeks%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := NULL;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_has_segment_invoice_lock boolean := false;
  v_validation_status text := NULL;
  v_validation_pre_validated boolean := false;
  v_validation_ok boolean := false;
  v_contract_requires_hr boolean := false;
  v_client_requires_hr boolean := false;
  v_hr_validation_required_for_invoice boolean := false;
  v_must_hold_for_hr_validation boolean := false;
  v_prevalidated_fast_track boolean := false;
  v_force_ready_for_invoice boolean := false;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_diag_started_at timestamptz := clock_timestamp();
  v_temp_log_enabled boolean := false;
  v_advance_state_refresh_json jsonb := '{}'::jsonb;
  v_has_uncleared_advance_override boolean := false;
  v_duplicate_expense_review jsonb := '{}'::jsonb;
  v_daily_missing_bucket record;
BEGIN

  if coalesce((public._ctms_import_correction_classify_v1(p_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    return jsonb_build_object(
      'ok', false,
      'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_BULK_LIFECYCLE',
      'timesheet_id', p_timesheet_id,
      'required_rpc', case when 'AUTHORISE' = 'AUTHORISE'
        then 'timesheet_authorise_bulk_atomic' else 'timesheet_unauthorise_bulk_atomic' end
    );
  end if;
  PERFORM set_config('lock_timeout', '300ms', true);

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

  -- SP-12: revoked_at restoration changes the qualified root scope.
  if (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    IF p_timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
    END IF;
    IF p_expected_timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
    END IF;
    IF p_actor_user_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
    END IF;

    perform private.weekly_source_pay_query_admit_v2();
  end if;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'entry',
      'timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'actor_user_id_present', p_actor_user_id IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_id')::text;
  END IF;
  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
  END IF;
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
  END IF;

  SELECT ts.*
    INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF v_requested_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_requested_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'requested_timesheet_resolved',
      'timesheet_id', v_requested_ts.timesheet_id,
      'is_current', COALESCE(v_requested_ts.is_current, false),
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
    SELECT ts.*
      INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_requested_ts.booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;
    IF v_current_ts.timesheet_id IS NULL THEN
      v_current_ts := v_requested_ts;
    END IF;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH', DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  v_duplicate_expense_review:=private._timesheet_duplicate_expense_review_v1(
    v_current_ts.timesheet_id
  );
  IF COALESCE((v_duplicate_expense_review->>'required')::boolean,false)
     AND COALESCE(current_setting('cloudtms.duplicate_expense_reviewed',true),'')<>'true' THEN
    RAISE EXCEPTION 'DUPLICATE_EXPENSE_REVIEW_REQUIRED'
      USING ERRCODE='PT409',DETAIL=jsonb_build_object(
        'code','DUPLICATE_EXPENSE_REVIEW_REQUIRED',
        'timesheet_id',v_current_ts.timesheet_id,
        'categories',COALESCE(v_duplicate_expense_review->'categories','[]'::jsonb)
      )::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'current_timesheet_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'NO_TSFIN')::text;
  END IF;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_locked',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_current_tsfin.processing_status::text,
      'old_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_current_ts.contract_id IS NOT NULL THEN
    SELECT COALESCE(c.requires_hr, false)
      INTO v_contract_requires_hr
    FROM public.contracts AS c
    WHERE c.id = v_current_ts.contract_id
    LIMIT 1;
  END IF;

  IF private._candidate_expense_source_family_v1(v_current_ts.timesheet_id) THEN
SELECT
  COALESCE((authority.settings_json#>>'{values,requires_hr}')::boolean,false) AS client_requires_hr,
  (ts.timesheet_id IS NOT NULL
    AND COALESCE((authority.settings_json#>>'{values,hr_validation_required_for_invoice}')::boolean,false)
    AND NOT COALESCE((authority.settings_json#>>'{values,no_timesheet_required}')::boolean,false)
    AND COALESCE(tf.total_hours,0)>0) AS hr_validation_required_for_invoice,
  UPPER(vl.status::text) AS validation_status
INTO v_client_requires_hr, v_hr_validation_required_for_invoice, v_validation_status
FROM public.timesheets ts
LEFT JOIN public.contract_weeks cw ON cw.timesheet_id=ts.timesheet_id
LEFT JOIN public.contracts ct ON ct.id=COALESCE(ts.contract_id,cw.contract_id)
LEFT JOIN LATERAL (
  SELECT f.client_id,f.total_hours FROM public.timesheets_financials f
  WHERE f.timesheet_id=ts.timesheet_id AND f.is_current=true
  ORDER BY f.created_at DESC LIMIT 1
) tf ON true
CROSS JOIN LATERAL (
  SELECT private._contract_settings_effective_core_v1(
    COALESCE(tf.client_id,ct.client_id),COALESCE(ts.contract_id,cw.contract_id),
    COALESCE((ts.worked_start_iso AT TIME ZONE 'Europe/London')::date,
      (ts.scheduled_start_iso AT TIME ZONE 'Europe/London')::date,ts.week_ending_date),
    CASE WHEN ts.sheet_scope='DAILY'::public.timesheet_scope_enum THEN 'DAILY' ELSE 'FINANCE' END,
    ts.timesheet_id) AS settings_json
) authority
LEFT JOIN LATERAL (
  SELECT tv.status FROM public.timesheet_validations tv
  WHERE tv.timesheet_id=ts.timesheet_id ORDER BY tv.created_at DESC LIMIT 1
) vl ON true
WHERE ts.timesheet_id=v_current_ts.timesheet_id AND ts.is_current=true
LIMIT 1
;
  ELSE
  -- Exact current-root flags only. Do not rebuild the summary of every
  -- Timesheet to authorise one root; retain the same dated/frozen settings,
  -- current-financial selection and latest-validation semantics.
  WITH bpay_authorisation_point_flags AS MATERIALIZED (
    SELECT ts.timesheet_id, COALESCE(tf.total_hours,0) AS total_hours,
      public.contract_settings_effective_get_v1(
        COALESCE(tf.client_id,ct.client_id),COALESCE(ts.contract_id,cw.contract_id),
        COALESCE((ts.worked_start_iso AT TIME ZONE 'Europe/London')::date,
          (ts.scheduled_start_iso AT TIME ZONE 'Europe/London')::date,ts.week_ending_date),
        CASE WHEN ts.sheet_scope='DAILY'::public.timesheet_scope_enum THEN 'DAILY' ELSE 'FINANCE' END,
        ts.timesheet_id) AS settings_json,
      UPPER(vl.status::text) AS validation_status
    FROM public.timesheets ts
    LEFT JOIN public.contract_weeks cw ON cw.timesheet_id=ts.timesheet_id
    LEFT JOIN public.contracts ct ON ct.id=COALESCE(ts.contract_id,cw.contract_id)
    LEFT JOIN LATERAL (
      SELECT f.client_id,f.total_hours FROM public.timesheets_financials f
      WHERE f.timesheet_id=ts.timesheet_id AND f.is_current=true
      ORDER BY f.created_at DESC LIMIT 1
    ) tf ON true
    LEFT JOIN LATERAL (
      SELECT tv.status FROM public.timesheet_validations tv
      WHERE tv.timesheet_id=ts.timesheet_id ORDER BY tv.created_at DESC LIMIT 1
    ) vl ON true
    WHERE ts.timesheet_id=v_current_ts.timesheet_id AND ts.is_current=true
    LIMIT 1
  )
  SELECT COALESCE((point.settings_json#>>'{values,requires_hr}')::boolean,false),
    (point.timesheet_id IS NOT NULL
      AND COALESCE((point.settings_json#>>'{values,hr_validation_required_for_invoice}')::boolean,false)
      AND NOT COALESCE((point.settings_json#>>'{values,no_timesheet_required}')::boolean,false)
      AND point.total_hours>0),
    point.validation_status
  INTO v_client_requires_hr,v_hr_validation_required_for_invoice,v_validation_status
  FROM bpay_authorisation_point_flags point;
  END IF;

  v_client_requires_hr := COALESCE(v_client_requires_hr, v_contract_requires_hr, false);
  v_hr_validation_required_for_invoice := COALESCE(v_hr_validation_required_for_invoice, v_contract_requires_hr, false);

  IF private._candidate_expense_source_family_v1(v_current_ts.timesheet_id) THEN
    SELECT cw.* INTO v_contract_week FROM public.contract_weeks cw
    WHERE cw.timesheet_id=v_current_ts.timesheet_id FOR UPDATE;
  ELSE
  SELECT cw.*
    INTO v_contract_week
  FROM public.contract_weeks AS cw
  WHERE cw.timesheet_id = v_current_ts.timesheet_id
     OR EXISTS (
       SELECT 1
       FROM public.timesheets AS cw_ts
       WHERE cw_ts.timesheet_id = cw.timesheet_id
         AND cw_ts.booking_id = v_current_ts.booking_id
     )
  ORDER BY CASE WHEN cw.timesheet_id = v_current_ts.timesheet_id THEN 0 ELSE 1 END,
           cw.updated_at DESC NULLS LAST,
           cw.id DESC
  LIMIT 1
  FOR UPDATE;
  END IF;

  IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND v_contract_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TARGET_NOT_FOUND',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'INVOICED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text, 'lock_scope', 'contract_week_status')::text;
  END IF;

  IF v_contract_week.id IS NOT NULL AND v_contract_week.status = 'CANCELLED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_NOT_AUTHORISABLE',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_contract_week.id, 'contract_week_status', v_contract_week.status::text)::text;
  END IF;

  v_prev_status := v_current_tsfin.processing_status;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
        WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_current_ts.archived_at_utc IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL OR COALESCE(v_has_segment_invoice_lock, false) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'invoice_id', v_current_tsfin.locked_by_invoice_id)::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL OR v_current_tsfin.authorised_at_utc IS NOT NULL OR v_contract_week.status = 'AUTHORISED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        v_current_ts.timesheet_id::text,
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'timesheet_authorise_generic_atomic',
          'stage', 'row_signature_mismatch_before_authorise',
          'action', 'authorise',
          'route_family', 'timesheet_lifecycle',
          'timesheet_id', p_timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json,
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
        ))
      );
    END IF;
    RAISE EXCEPTION USING MESSAGE = 'ROW_SIGNATURE_MISMATCH', DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)::text;
  END IF;

  SELECT COALESCE(tv.pre_validated, false)
    INTO v_validation_pre_validated
  FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = v_current_ts.timesheet_id
  ORDER BY tv.updated_at DESC NULLS LAST, tv.created_at DESC NULLS LAST, tv.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    v_validation_pre_validated := false;
  END IF;

  v_validation_ok := COALESCE(v_validation_status, '') IN ('VALIDATION_OK', 'OVERRIDDEN');
  v_force_ready_for_invoice := v_current_tsfin.basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum);
  v_must_hold_for_hr_validation := COALESCE(v_hr_validation_required_for_invoice, false) AND NOT v_validation_ok;
  v_prevalidated_fast_track := COALESCE(v_validation_pre_validated, false) AND v_validation_ok;

  -- DY04: report actual missing Daily authority without making it authorisable.
  IF (SELECT active_owner FROM private.bpay_next_module_control WHERE id=1)='NEXT'
     AND v_current_ts.sheet_scope='DAILY'::public.timesheet_scope_enum
     AND v_current_ts.line_type::text='HOURS'
     AND v_current_ts.is_current IS TRUE
     AND v_current_ts.is_adjustment IS DISTINCT FROM TRUE
     AND v_current_ts.parent_timesheet_id IS NULL AND v_current_ts.correction_id IS NULL
     AND NULLIF(BTRIM(v_current_ts.adjustment_origin),'') IS NULL
     AND NULLIF(BTRIM(v_current_ts.correction_kind),'') IS NULL
     AND v_current_ts.worked_start_iso IS NOT NULL
     AND v_current_tsfin.is_current IS TRUE AND v_current_tsfin.is_stale IS FALSE
     AND v_current_tsfin.timesheet_version=v_current_ts.version
     AND v_current_tsfin.basis::text='SELF_REPORTED'
     AND v_current_tsfin.candidate_id IS NOT NULL AND v_current_tsfin.client_id IS NOT NULL
     AND UPPER(v_current_tsfin.pay_method) IN ('PAYE','UMBRELLA')
     AND v_current_tsfin.has_rate_issue IS TRUE
     AND v_prev_status::text IN ('RATE_MISSING','UNPROCESSED')
     AND NOT EXISTS (SELECT 1 FROM public.contracts dc
       WHERE dc.id=v_current_ts.contract_id AND dc.self_bill IS TRUE)
     AND NOT private._candidate_expense_source_family_v1(v_current_ts.timesheet_id) THEN
    FOR v_daily_missing_bucket IN
      SELECT d.* FROM (VALUES
        (1,'DAY'::text,v_current_tsfin.hours_day,v_current_tsfin.pay_day,v_current_tsfin.charge_day),
        (2,'NIGHT',v_current_tsfin.hours_night,v_current_tsfin.pay_night,v_current_tsfin.charge_night),
        (3,'SAT',v_current_tsfin.hours_sat,v_current_tsfin.pay_sat,v_current_tsfin.charge_sat),
        (4,'SUN',v_current_tsfin.hours_sun,v_current_tsfin.pay_sun,v_current_tsfin.charge_sun),
        (5,'BH',v_current_tsfin.hours_bh,v_current_tsfin.pay_bh,v_current_tsfin.charge_bh)
      ) d(ordinal,bucket,hours,pay_rate,charge_rate)
      WHERE d.hours>0 AND (d.pay_rate IS NULL OR d.charge_rate IS NULL) ORDER BY d.ordinal
    LOOP
      RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='BPAY_NEXT_ORDINARY_USED_RATE_MISSING',
        DETAIL=jsonb_build_object('bucket',v_daily_missing_bucket.bucket,
          'rate_field',CASE WHEN v_daily_missing_bucket.pay_rate IS NULL THEN 'PAY' ELSE 'CHARGE' END,
          'rate_authority','DAILY_DATED','sheet_scope','DAILY','line_type','HOURS',
          'timesheet_id',v_current_ts.timesheet_id,'contract_id',v_current_ts.contract_id,
          'candidate_id',v_current_tsfin.candidate_id,'client_id',v_current_tsfin.client_id,
          'work_date',(v_current_ts.worked_start_iso AT TIME ZONE 'Europe/London')::date,
          'pay_channel',UPPER(v_current_tsfin.pay_method))::text;
    END LOOP;
  END IF;

  IF v_prev_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN
    RAISE EXCEPTION USING MESSAGE = 'AUTHORISE_NOT_ALLOWED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'processing_status', v_prev_status::text)::text;
  END IF;

  v_new_status := CASE
    WHEN v_force_ready_for_invoice THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN v_must_hold_for_hr_validation THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    WHEN v_prevalidated_fast_track THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    WHEN COALESCE(v_client_requires_hr, false) THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
    ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
  END;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'before_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'tsfin_id', v_current_tsfin.id,
      'current_row_signature_present', v_current_row_signature IS NOT NULL,
      'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'timesheet_authorise', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);

  UPDATE public.timesheets AS ts
     SET authorised_at_server = v_now,
         revoked_at = NULL,
         revoked_reason = NULL,
         revoked_by = NULL,
         updated_at = v_now
   WHERE ts.timesheet_id = v_current_ts.timesheet_id
     AND ts.is_current = true
   RETURNING * INTO v_current_ts;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'timesheets_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'new_authorised_present', v_current_ts.authorised_at_server IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  UPDATE public.timesheets_financials AS tf
     SET processing_status = v_new_status,
         authorised_by_user_id = p_actor_user_id,
         authorised_at_utc = v_now,
         updated_at = v_now
   WHERE tf.id = v_current_tsfin.id
     AND tf.is_current = true
   RETURNING * INTO v_current_tsfin;

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'tsfin_update_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'tsfin_id', v_current_tsfin.id,
      'old_processing_status', v_prev_status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_authorised_present', v_current_tsfin.authorised_at_utc IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  IF v_contract_week.id IS NOT NULL THEN
    UPDATE public.contract_weeks AS cw
       SET status = 'AUTHORISED'::public.contract_week_status_enum,
           updated_at = v_now
     WHERE cw.id = v_contract_week.id
     RETURNING * INTO v_contract_week;
  END IF;

  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'off', true);

  v_has_uncleared_advance_override := private.bpay_has_uncleared_advance_v1(v_current_ts.timesheet_id);

  IF COALESCE(v_has_uncleared_advance_override, false) THEN
    v_advance_state_refresh_json := public.pay_timesheet_summary_advance_state_refresh(
      p_timesheet_id => v_current_ts.timesheet_id,
      p_actor_user_id => p_actor_user_id
    );

    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_done',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'refresh_result', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  ELSE
    PERFORM public._temp_diag_log(
      'TEMP_AUTHORISE_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_build_object(
        'function_name', 'timesheet_authorise_generic_atomic',
        'stage', 'advance_state_refresh_skipped',
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
        'reason', 'NO_ACTIVE_ADVANCE_THIS_PAYMENT_OVERRIDE',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
      )
    );
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'after_signature_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'row_signature_present', v_after_row_signature IS NOT NULL,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._audit_insert(
    'timesheet',
    v_current_ts.timesheet_id::text,
    'TIMESHEET_AUTHORISED',
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'previous_processing_status', v_prev_status::text, 'previous_row_signature', v_current_row_signature),
    jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'new_processing_status', v_new_status::text, 'authorised_at_utc', v_now, 'authorised_by_user_id', p_actor_user_id, 'new_row_signature', v_after_row_signature),
    'TIMESHEET_AUTHORISE',
    p_actor_user_id
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'audit_done',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    v_current_ts.timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'return',
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
      'new_processing_status', v_new_status::text,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', 'timesheet_authorise',
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'requested_timesheet_id', v_requested_ts.timesheet_id,
    'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END,
    'booking_id', v_current_ts.booking_id,
    'candidate_id', v_current_tsfin.candidate_id,
    'authorised_at_server', v_current_ts.authorised_at_server,
    'revoked_at', v_current_ts.revoked_at,
    'authorised_at_utc', v_current_tsfin.authorised_at_utc,
    'advance_state_refresh', COALESCE(v_advance_state_refresh_json, '{}'::jsonb),
    'current_version', v_current_ts.version,
    'was_stale', v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id,
    'previous_processing_status', v_prev_status::text,
    'processing_status', v_new_status::text,
    'new_processing_status', v_new_status::text,
    'validation_status', v_validation_status,
    'validation_pre_validated', COALESCE(v_validation_pre_validated, false),
    'hr_validation_required_for_invoice', COALESCE(v_hr_validation_required_for_invoice, false),
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'affected_rows', jsonb_build_array(jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END, 'booking_id', v_current_ts.booking_id, 'row_key', 'timesheet:' || v_current_ts.timesheet_id::text)),
    'cache_invalidation_hints', jsonb_build_object('changed_domains', CASE WHEN COALESCE(v_has_uncleared_advance_override, false) THEN jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_summary_pay_state_cache') ELSE jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks') END, 'timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', CASE WHEN v_contract_week.id IS NULL THEN NULL ELSE v_contract_week.id END)
  );
EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;
  -- Exact busy must escape before diagnostic writes or legacy lock translation.
  IF v_error_state = '55P03' AND v_error_message = 'WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY' THEN
    RAISE;
  END IF;
  PERFORM public._temp_diag_log(
    'TEMP_AUTHORISE_STAGE',
    'TEMP_TIMESHEET_LIFECYCLE',
    p_timesheet_id::text,
    jsonb_build_object(
      'function_name', 'timesheet_authorise_generic_atomic',
      'stage', 'exception',
      'timesheet_id', p_timesheet_id,
      'sqlstate', v_error_state,
      'error_message', v_error_message,
      'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 2)
    )
  );
  IF v_error_state = '55P03' THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('timesheet_id', p_timesheet_id)::text;
  END IF;
  RAISE;
END;
$function$;
alter function public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text) owner to postgres;
revoke all on function public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text) from public,anon,authenticated;
grant execute on function public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text) to service_role;
notify pgrst, 'reload schema';
commit;
