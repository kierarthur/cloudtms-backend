-- ============================================================================
-- 02_client_create_with_settings_v1.txt
-- New function. Install after the additive schema migration and function 01.
--
-- The caller supplies p_client_id. This is the idempotency identity:
--   - first call inserts client + client_settings atomically;
--   - an exact retry returns the existing rows;
--   - a different payload for the same UUID is rejected.
--
-- Existing database triggers continue to mint clients.cli_ref and clients.rev.
--
-- Limits:
--   5 arguments.
--   Client JSON <= 64 KiB.
--   Settings JSON <= 128 KiB.
--   No arrays or fanout.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.client_create_with_settings_v1(
  p_client_id uuid,
  p_client_json jsonb,
  p_actor_user_id uuid,
  p_settings_json jsonb DEFAULT '{}'::jsonb,
  p_now_utc timestamptz DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_client_payload jsonb := COALESCE(p_client_json, '{}'::jsonb);
  v_settings_payload jsonb := COALESCE(p_settings_json, '{}'::jsonb);

  v_client_input public.clients%ROWTYPE;
  v_settings_input public.client_settings%ROWTYPE;
  v_global public.settings_defaults%ROWTYPE;
  v_client public.clients%ROWTYPE;
  v_settings public.client_settings%ROWTYPE;
  v_existing_client public.clients%ROWTYPE;
  v_existing_settings public.client_settings%ROWTYPE;

  v_unknown_client_keys text[] := ARRAY[]::text[];
  v_unknown_settings_keys text[] := ARRAY[]::text[];
  v_client_mismatch_keys text[] := ARRAY[]::text[];
  v_settings_mismatch_keys text[] := ARRAY[]::text[];
  v_lock_acquired boolean := false;
  v_replay boolean := false;
BEGIN
  IF p_client_id IS NULL THEN
    RAISE EXCEPTION 'CLIENT_CREATE_ID_REQUIRED'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object('code', 'CLIENT_CREATE_ID_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'CLIENT_CREATE_ACTOR_REQUIRED'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object('code', 'CLIENT_CREATE_ACTOR_REQUIRED')::text;
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_row
  WHERE actor_row.id = p_actor_user_id
    AND COALESCE(actor_row.is_active, false) = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CLIENT_CREATE_ACTOR_INVALID'
      USING ERRCODE = '42501',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_ACTOR_INVALID',
              'actor_user_id', p_actor_user_id::text
            )::text;
  END IF;

  IF jsonb_typeof(v_client_payload) <> 'object' THEN
    RAISE EXCEPTION 'CLIENT_CREATE_PAYLOAD_MUST_BE_OBJECT'
      USING ERRCODE = '22023';
  END IF;

  IF jsonb_typeof(v_settings_payload) <> 'object' THEN
    RAISE EXCEPTION 'CLIENT_SETTINGS_PAYLOAD_MUST_BE_OBJECT'
      USING ERRCODE = '22023';
  END IF;

  IF octet_length(v_client_payload::text) > 65536 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_PAYLOAD_TOO_LARGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_PAYLOAD_TOO_LARGE',
              'max_bytes', 65536
            )::text;
  END IF;

  IF octet_length(v_settings_payload::text) > 131072 THEN
    RAISE EXCEPTION 'CLIENT_SETTINGS_PAYLOAD_TOO_LARGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_SETTINGS_PAYLOAD_TOO_LARGE',
              'max_bytes', 131072
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM jsonb_each(v_client_payload) AS supplied_field(field_name, field_value)
    WHERE supplied_field.field_name IN ('name', 'vat_chargeable', 'payment_terms_days')
      AND supplied_field.field_value = 'null'::jsonb
  ) THEN
    RAISE EXCEPTION 'CLIENT_CREATE_REQUIRED_FIELD_NULL'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_REQUIRED_FIELD_NULL'
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM jsonb_each(v_settings_payload) AS supplied_field(field_name, field_value)
    WHERE supplied_field.field_name IN (
      'effective_from',
      'hr_validation_required',
      'ts_reference_required',
      'autoprocess_hr',
      'pay_reference_required',
      'invoice_reference_required',
      'default_submission_mode',
      'is_nhsp',
      'self_bill_no_invoices_sent',
      'daily_calc_of_invoices',
      'no_timesheet_required',
      'group_nightsat_sunbh',
      'requires_hr',
      'hr_attach_to_invoice',
      'ts_attach_to_invoice',
      'auto_invoice_default',
      'send_manual_invoices_to_different_email',
      'invoice_consolidation_mode',
      'reference_number_required_to_issue_invoice',
      'opt_in_email',
      'opt_in_sms',
      'opt_in_whatsapp',
      'healthroster_import_auto_authorise',
      'nhsp_import_auto_authorise'
    )
      AND supplied_field.field_value = 'null'::jsonb
  ) THEN
    RAISE EXCEPTION 'CLIENT_SETTINGS_REQUIRED_FIELD_NULL'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_SETTINGS_REQUIRED_FIELD_NULL'
            )::text;
  END IF;

  SELECT COALESCE(array_agg(key_name ORDER BY key_name), ARRAY[]::text[])
  INTO v_unknown_client_keys
  FROM jsonb_object_keys(v_client_payload) AS supplied_key(key_name)
  WHERE supplied_key.key_name <> ALL (
    ARRAY[
      'name',
      'invoice_address',
      'primary_invoice_email',
      'ap_phone',
      'vat_chargeable',
      'payment_terms_days',
      'mileage_charge_rate',
      'ts_queries_email',
      'client_address',
      'contact_title',
      'contact_known_as',
      'contact_forename',
      'contact_surname',
      'contact_job_title',
      'contact_tel',
      'contact_mobile',
      'contact_email',
      'website',
      'notes'
    ]::text[]
  );

  IF cardinality(v_unknown_client_keys) > 0 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_UNKNOWN_FIELDS'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_UNKNOWN_FIELDS',
              'unknown_fields', to_jsonb(v_unknown_client_keys)
            )::text;
  END IF;

  SELECT COALESCE(array_agg(key_name ORDER BY key_name), ARRAY[]::text[])
  INTO v_unknown_settings_keys
  FROM jsonb_object_keys(v_settings_payload) AS supplied_key(key_name)
  WHERE supplied_key.key_name <> ALL (
    ARRAY[
      'timezone_id',
      'day_start',
      'day_end',
      'night_start',
      'night_end',
      'bh_source',
      'bh_list',
      'bh_feed_url',
      'vat_rate_pct',
      'holiday_pay_pct',
      'erni_pct',
      'apply_holiday_to',
      'apply_erni_to',
      'margin_includes',
      'effective_from',
      'hr_validation_required',
      'ts_reference_required',
      'week_ending_weekday',
      'autoprocess_hr',
      'pay_reference_required',
      'invoice_reference_required',
      'default_submission_mode',
      'sat_start',
      'sat_end',
      'sun_start',
      'sun_end',
      'is_nhsp',
      'self_bill_no_invoices_sent',
      'daily_calc_of_invoices',
      'no_timesheet_required',
      'group_nightsat_sunbh',
      'requires_hr',
      'hr_attach_to_invoice',
      'ts_attach_to_invoice',
      'bh_start',
      'bh_end',
      'auto_invoice_default',
      'send_manual_invoices_to_different_email',
      'manual_invoices_alt_email_address',
      'invoice_consolidation_mode',
      'reference_number_required_to_issue_invoice',
      'opt_in_email',
      'opt_in_sms',
      'opt_in_whatsapp',
      'healthroster_import_auto_authorise',
      'nhsp_import_auto_authorise',
      'reversal_complete_financials_date',
      'reversal_replacement_financials_date'
    ]::text[]
  );

  IF cardinality(v_unknown_settings_keys) > 0 THEN
    RAISE EXCEPTION 'CLIENT_SETTINGS_UNKNOWN_FIELDS'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_SETTINGS_UNKNOWN_FIELDS',
              'unknown_fields', to_jsonb(v_unknown_settings_keys)
            )::text;
  END IF;

  SELECT *
  INTO v_client_input
  FROM jsonb_populate_record(NULL::public.clients, v_client_payload);

  SELECT *
  INTO v_settings_input
  FROM jsonb_populate_record(NULL::public.client_settings, v_settings_payload);

  PERFORM public._ctms_assert_import_correction_settings_write_v1(
    COALESCE(v_settings_input.is_nhsp, false),
    COALESCE(v_settings_input.autoprocess_hr, false),
    COALESCE(v_settings_input.no_timesheet_required, false),
    CASE WHEN v_settings_payload ? 'reversal_complete_financials_date'
      THEN v_settings_input.reversal_complete_financials_date
      ELSE NULL::public.correction_financials_date_basis_enum
    END,
    CASE WHEN v_settings_payload ? 'reversal_replacement_financials_date'
      THEN v_settings_input.reversal_replacement_financials_date
      ELSE NULL::public.correction_financials_date_basis_enum
    END
  );

  IF NULLIF(BTRIM(COALESCE(v_client_input.name, '')), '') IS NULL THEN
    RAISE EXCEPTION 'CLIENT_CREATE_NAME_REQUIRED'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object('code', 'CLIENT_CREATE_NAME_REQUIRED')::text;
  END IF;

  IF char_length(v_client_input.name) > 500 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_NAME_TOO_LONG'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_NAME_TOO_LONG',
              'max_characters', 500
            )::text;
  END IF;

  IF COALESCE(v_client_input.payment_terms_days, 30) < 0
     OR COALESCE(v_client_input.payment_terms_days, 30) > 365 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_PAYMENT_TERMS_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_PAYMENT_TERMS_OUT_OF_RANGE',
              'min', 0,
              'max', 365
            )::text;
  END IF;

  IF v_client_input.mileage_charge_rate IS NOT NULL
     AND v_client_input.mileage_charge_rate < 0 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_MILEAGE_RATE_NEGATIVE'
      USING ERRCODE = '22023';
  END IF;

  IF v_client_input.primary_invoice_email IS NOT NULL
     AND char_length(v_client_input.primary_invoice_email) > 320 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_PRIMARY_EMAIL_TOO_LONG'
      USING ERRCODE = '22023';
  END IF;

  IF v_client_input.contact_email IS NOT NULL
     AND char_length(v_client_input.contact_email) > 320 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_CONTACT_EMAIL_TOO_LONG'
      USING ERRCODE = '22023';
  END IF;

  IF v_client_input.notes IS NOT NULL
     AND char_length(v_client_input.notes) > 20000 THEN
    RAISE EXCEPTION 'CLIENT_CREATE_NOTES_TOO_LONG'
      USING ERRCODE = '22023';
  END IF;

  SELECT settings_row.*
  INTO v_global
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1
  FOR SHARE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CLIENT_CREATE_GLOBAL_SETTINGS_MISSING'
      USING ERRCODE = 'P0001';
  END IF;

  v_lock_acquired := pg_try_advisory_xact_lock(
    hashtextextended(
      'CLIENT_CREATE_WITH_SETTINGS|' || p_client_id::text,
      24062026
    )
  );

  IF NOT v_lock_acquired THEN
    RAISE EXCEPTION 'CLIENT_CREATE_LOCK_BUSY'
      USING ERRCODE = '55P03',
            DETAIL = jsonb_build_object(
              'code', 'CLIENT_CREATE_LOCK_BUSY',
              'client_id', p_client_id::text,
              'retryable', true
            )::text;
  END IF;

  SELECT client_row.*
  INTO v_existing_client
  FROM public.clients AS client_row
  WHERE client_row.id = p_client_id
  FOR UPDATE;

  IF FOUND THEN
    SELECT settings_row.*
    INTO v_existing_settings
    FROM public.client_settings AS settings_row
    WHERE settings_row.client_id = p_client_id
    ORDER BY
      settings_row.effective_from DESC NULLS LAST,
      settings_row.updated_at DESC,
      settings_row.id DESC
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'CLIENT_CREATE_REPLAY_SETTINGS_MISSING'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'CLIENT_CREATE_REPLAY_SETTINGS_MISSING',
                'client_id', p_client_id::text
              )::text;
    END IF;

    SELECT COALESCE(array_agg(key_name ORDER BY key_name), ARRAY[]::text[])
    INTO v_client_mismatch_keys
    FROM jsonb_object_keys(v_client_payload) AS requested_key(key_name)
    WHERE (to_jsonb(v_existing_client) -> requested_key.key_name)
          IS DISTINCT FROM
          (to_jsonb(v_client_input) -> requested_key.key_name);

    SELECT COALESCE(array_agg(key_name ORDER BY key_name), ARRAY[]::text[])
    INTO v_settings_mismatch_keys
    FROM jsonb_object_keys(v_settings_payload) AS requested_key(key_name)
    WHERE (to_jsonb(v_existing_settings) -> requested_key.key_name)
          IS DISTINCT FROM
          (to_jsonb(v_settings_input) -> requested_key.key_name);

    IF cardinality(v_client_mismatch_keys) > 0
       OR cardinality(v_settings_mismatch_keys) > 0 THEN
      RAISE EXCEPTION 'CLIENT_CREATE_IDEMPOTENCY_CONFLICT'
        USING ERRCODE = '23505',
              DETAIL = jsonb_build_object(
                'code', 'CLIENT_CREATE_IDEMPOTENCY_CONFLICT',
                'client_id', p_client_id::text,
                'client_mismatch_fields', to_jsonb(v_client_mismatch_keys),
                'settings_mismatch_fields', to_jsonb(v_settings_mismatch_keys)
              )::text;
    END IF;

    v_replay := true;
    v_client := v_existing_client;
    v_settings := v_existing_settings;
  ELSE
    INSERT INTO public.clients (
      id,
      name,
      invoice_address,
      primary_invoice_email,
      ap_phone,
      vat_chargeable,
      payment_terms_days,
      created_at,
      updated_at,
      mileage_charge_rate,
      ts_queries_email,
      client_address,
      contact_title,
      contact_known_as,
      contact_forename,
      contact_surname,
      contact_job_title,
      contact_tel,
      contact_mobile,
      contact_email,
      website,
      notes
    )
    VALUES (
      p_client_id,
      BTRIM(v_client_input.name),
      v_client_input.invoice_address,
      v_client_input.primary_invoice_email,
      v_client_input.ap_phone,
      COALESCE(v_client_input.vat_chargeable, true),
      COALESCE(v_client_input.payment_terms_days, 30),
      v_now,
      v_now,
      v_client_input.mileage_charge_rate,
      v_client_input.ts_queries_email,
      v_client_input.client_address,
      v_client_input.contact_title,
      v_client_input.contact_known_as,
      v_client_input.contact_forename,
      v_client_input.contact_surname,
      v_client_input.contact_job_title,
      v_client_input.contact_tel,
      v_client_input.contact_mobile,
      v_client_input.contact_email,
      v_client_input.website,
      v_client_input.notes
    )
    RETURNING *
    INTO v_client;

    INSERT INTO public.client_settings (
      client_id,
      timezone_id,
      day_start,
      day_end,
      night_start,
      night_end,
      bh_source,
      bh_list,
      bh_feed_url,
      vat_rate_pct,
      holiday_pay_pct,
      erni_pct,
      apply_holiday_to,
      apply_erni_to,
      margin_includes,
      effective_from,
      created_at,
      updated_at,
      hr_validation_required,
      ts_reference_required,
      week_ending_weekday,
      autoprocess_hr,
      pay_reference_required,
      invoice_reference_required,
      default_submission_mode,
      sat_start,
      sat_end,
      sun_start,
      sun_end,
      is_nhsp,
      self_bill_no_invoices_sent,
      daily_calc_of_invoices,
      no_timesheet_required,
      group_nightsat_sunbh,
      requires_hr,
      hr_attach_to_invoice,
      ts_attach_to_invoice,
      bh_start,
      bh_end,
      auto_invoice_default,
      send_manual_invoices_to_different_email,
      manual_invoices_alt_email_address,
      invoice_consolidation_mode,
      reference_number_required_to_issue_invoice,
      opt_in_email,
      opt_in_sms,
      opt_in_whatsapp,
      healthroster_import_auto_authorise,
      nhsp_import_auto_authorise,
      reversal_complete_financials_date,
      reversal_replacement_financials_date
    )
    VALUES (
      p_client_id,
      CASE WHEN v_settings_payload ? 'timezone_id'
        THEN v_settings_input.timezone_id ELSE v_global.timezone_id END,
      CASE WHEN v_settings_payload ? 'day_start'
        THEN v_settings_input.day_start ELSE v_global.day_start END,
      CASE WHEN v_settings_payload ? 'day_end'
        THEN v_settings_input.day_end ELSE v_global.day_end END,
      CASE WHEN v_settings_payload ? 'night_start'
        THEN v_settings_input.night_start ELSE v_global.night_start END,
      CASE WHEN v_settings_payload ? 'night_end'
        THEN v_settings_input.night_end ELSE v_global.night_end END,
      CASE WHEN v_settings_payload ? 'bh_source'
        THEN v_settings_input.bh_source ELSE v_global.bh_source END,
      CASE WHEN v_settings_payload ? 'bh_list'
        THEN v_settings_input.bh_list ELSE v_global.bh_list END,
      CASE WHEN v_settings_payload ? 'bh_feed_url'
        THEN v_settings_input.bh_feed_url ELSE v_global.bh_feed_url END,
      v_settings_input.vat_rate_pct,
      v_settings_input.holiday_pay_pct,
      v_settings_input.erni_pct,
      v_settings_input.apply_holiday_to,
      v_settings_input.apply_erni_to,
      v_settings_input.margin_includes,
      COALESCE(
        v_settings_input.effective_from,
        (v_now AT TIME ZONE 'Europe/London')::date
      ),
      v_now,
      v_now,
      COALESCE(v_settings_input.hr_validation_required, false),
      CASE WHEN v_settings_payload ? 'ts_reference_required'
        THEN COALESCE(v_settings_input.ts_reference_required, false)
        ELSE COALESCE(v_global.ts_reference_required, false)
      END,
      v_settings_input.week_ending_weekday,
      COALESCE(v_settings_input.autoprocess_hr, false),
      COALESCE(v_settings_input.pay_reference_required, false),
      COALESCE(v_settings_input.invoice_reference_required, false),
      COALESCE(
        v_settings_input.default_submission_mode,
        'ELECTRONIC'::public.submission_mode_enum
      ),
      CASE WHEN v_settings_payload ? 'sat_start'
        THEN v_settings_input.sat_start ELSE v_global.sat_start END,
      CASE WHEN v_settings_payload ? 'sat_end'
        THEN v_settings_input.sat_end ELSE v_global.sat_end END,
      CASE WHEN v_settings_payload ? 'sun_start'
        THEN v_settings_input.sun_start ELSE v_global.sun_start END,
      CASE WHEN v_settings_payload ? 'sun_end'
        THEN v_settings_input.sun_end ELSE v_global.sun_end END,
      COALESCE(v_settings_input.is_nhsp, false),
      COALESCE(v_settings_input.self_bill_no_invoices_sent, false),
      COALESCE(v_settings_input.daily_calc_of_invoices, false),
      COALESCE(v_settings_input.no_timesheet_required, false),
      COALESCE(v_settings_input.group_nightsat_sunbh, false),
      COALESCE(v_settings_input.requires_hr, false),
      CASE WHEN v_settings_payload ? 'hr_attach_to_invoice'
        THEN COALESCE(v_settings_input.hr_attach_to_invoice, true)
        ELSE COALESCE(v_global.hr_attach_to_invoice, true)
      END,
      CASE WHEN v_settings_payload ? 'ts_attach_to_invoice'
        THEN COALESCE(v_settings_input.ts_attach_to_invoice, true)
        ELSE COALESCE(v_global.ts_attach_to_invoice, true)
      END,
      CASE WHEN v_settings_payload ? 'bh_start'
        THEN v_settings_input.bh_start ELSE v_global.bh_start END,
      CASE WHEN v_settings_payload ? 'bh_end'
        THEN v_settings_input.bh_end ELSE v_global.bh_end END,
      COALESCE(v_settings_input.auto_invoice_default, false),
      COALESCE(
        v_settings_input.send_manual_invoices_to_different_email,
        false
      ),
      v_settings_input.manual_invoices_alt_email_address,
      COALESCE(
        v_settings_input.invoice_consolidation_mode,
        'NONE'::public.invoice_consolidation_mode_enum
      ),
      COALESCE(
        v_settings_input.reference_number_required_to_issue_invoice,
        false
      ),
      COALESCE(v_settings_input.opt_in_email, true),
      COALESCE(v_settings_input.opt_in_sms, true),
      COALESCE(v_settings_input.opt_in_whatsapp, true),
      CASE WHEN v_settings_payload ? 'healthroster_import_auto_authorise'
        THEN COALESCE(
          v_settings_input.healthroster_import_auto_authorise,
          v_global.healthroster_import_auto_authorise_default
        )
        ELSE v_global.healthroster_import_auto_authorise_default
      END,
      CASE WHEN v_settings_payload ? 'nhsp_import_auto_authorise'
        THEN COALESCE(
          v_settings_input.nhsp_import_auto_authorise,
          v_global.nhsp_import_auto_authorise_default
        )
        ELSE v_global.nhsp_import_auto_authorise_default
      END,
      CASE WHEN (
        COALESCE(v_settings_input.is_nhsp, false) = true
        OR (
          COALESCE(v_settings_input.autoprocess_hr, false) = true
          AND COALESCE(v_settings_input.no_timesheet_required, false) = true
        )
      ) AND v_settings_payload ? 'reversal_complete_financials_date'
        THEN v_settings_input.reversal_complete_financials_date
        ELSE NULL::public.correction_financials_date_basis_enum
      END,
      CASE WHEN (
        COALESCE(v_settings_input.is_nhsp, false) = true
        OR (
          COALESCE(v_settings_input.autoprocess_hr, false) = true
          AND COALESCE(v_settings_input.no_timesheet_required, false) = true
        )
      ) AND v_settings_payload ? 'reversal_replacement_financials_date'
        THEN v_settings_input.reversal_replacement_financials_date
        ELSE NULL::public.correction_financials_date_basis_enum
      END
    )
    RETURNING *
    INTO v_settings;

    PERFORM public._inv_write_audit(
      p_actor_user_id,
      'CLIENT_CREATED_WITH_SETTINGS',
      jsonb_build_object(
        'client_id', v_client.id::text,
        'client_ref', v_client.cli_ref,
        'client_name', v_client.name,
        'client_settings_id', v_settings.id::text,
        'healthroster_import_auto_authorise',
          v_settings.healthroster_import_auto_authorise,
        'nhsp_import_auto_authorise',
          v_settings.nhsp_import_auto_authorise,
        'correction_policy_eligible',
          (v_settings.is_nhsp OR (v_settings.autoprocess_hr AND v_settings.no_timesheet_required)),
        'reversal_complete_financials_date_override',
          v_settings.reversal_complete_financials_date,
        'reversal_complete_financials_date_effective',
          COALESCE(v_settings.reversal_complete_financials_date, v_global.reversal_complete_financials_date),
        'reversal_replacement_financials_date_override',
          v_settings.reversal_replacement_financials_date,
        'reversal_replacement_financials_date_effective',
          COALESCE(v_settings.reversal_replacement_financials_date, v_global.reversal_replacement_financials_date),
        'correction_setting_source',
          CASE WHEN v_settings.reversal_complete_financials_date IS NULL
                 AND v_settings.reversal_replacement_financials_date IS NULL
            THEN 'GLOBAL' ELSE 'CLIENT_OR_MIXED' END,
        'copied_global_settings_updated_at', v_global.updated_at
      ),
      'client',
      v_client.id::text,
      NULL::jsonb,
      'Atomic client and initial settings creation',
      NULL::text,
      NULL::text,
      'client-create-with-settings:' || v_client.id::text
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'replay', v_replay,
    'client', to_jsonb(v_client),
    'client_settings', to_jsonb(v_settings),
    'policy', jsonb_build_object(
      'healthroster', public.import_auto_authorise_policy_resolve_v1(
        'HEALTHROSTER'::public.hr_source_enum,
        v_client.id,
        NULL::uuid,
        false
      ),
      'nhsp', public.import_auto_authorise_policy_resolve_v1(
        'NHSP'::public.hr_source_enum,
        v_client.id,
        NULL::uuid,
        false
      )
    )
  );
END;
$function$;

COMMENT ON FUNCTION public.client_create_with_settings_v1(
  uuid,
  jsonb,
  uuid,
  jsonb,
  timestamptz
)
IS 'Atomically creates a client and initial client_settings row. Caller-supplied client UUID provides exact retry identity.';

REVOKE ALL ON FUNCTION public.client_create_with_settings_v1(
  uuid,
  jsonb,
  uuid,
  jsonb,
  timestamptz
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.client_create_with_settings_v1(
  uuid,
  jsonb,
  uuid,
  jsonb,
  timestamptz
) TO service_role;
