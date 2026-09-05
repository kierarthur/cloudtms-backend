-- CloudTMS Banking Pay saved finance-resolution evidence authority.
-- Runtime authority is Miget TEST; the supabase directory name is historical.
-- Policy X: this is pre-Draft evidence compatibility only. It does not alter
-- finance equations, pay method selection, PAYE/Umbrella tax or VAT treatment,
-- frozen Draft authority, payment execution, provider, settlement or remittance.
-- Historical 26052026_2100HRS_NEW_FUNCTIONS.sql remains byte-identical.

CREATE OR REPLACE FUNCTION public.pay_finance_component_resolutions_apply(
  p_candidate_id uuid,
  p_component_resolutions jsonb,
  p_actor_user_id uuid,
  p_finance_case_id uuid DEFAULT NULL::uuid,
  p_reason text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now_utc timestamptz := now();
  v_today_uk date := (now() at time zone 'Europe/London')::date;

  v_candidate record;
  v_finance_settings record;

  v_input_json jsonb := coalesce(p_component_resolutions, '[]'::jsonb);
  v_resolution_json jsonb;
  v_row_seq integer := 0;

  v_component_id_text text;
  v_component_id uuid;
  v_resolution_mode_text text;
  v_target_pay_method text;
  v_resolution_mode public.pay_finance_component_resolution_mode_enum;

  v_component record;

  v_before_json jsonb := null;
  v_after_json jsonb := null;

  v_relevant_erni_pct numeric := null;
  v_vat_rate_pct numeric := null;
  v_umbrella_vat_chargeable boolean := false;

  v_target_units numeric := null;
  v_manual_replacement_rate numeric := null;
  v_manual_amount_ex_vat numeric := null;
  v_requested_target_units numeric := null;

  v_source_units numeric := null;
  v_source_rate numeric := null;
  v_source_charge_rate numeric := null;
  v_bucket_code text := null;
  v_component_semantics text := null;
  v_is_rate_bearing_row boolean := false;
  v_is_amount_led_row boolean := false;

  v_basis_amount_ex_vat numeric(12,2) := 0;
  v_target_amount_ex_vat numeric(12,2) := 0;
  v_target_amount_vat numeric(12,2) := 0;
  v_target_amount_inc_vat numeric(12,2) := 0;
  v_rate_precision numeric := null;

  v_target_amount_ex_vat_per_source numeric(18,10) := 0;
  v_target_amount_vat_per_source numeric(18,10) := 0;
  v_target_amount_inc_vat_per_source numeric(18,10) := 0;
  v_target_units_per_source numeric(18,10) := null;

  v_target_amounts_json jsonb := '{}'::jsonb;
  v_target_basis_json jsonb := '{}'::jsonb;
  v_resolution_payload_json jsonb := '{}'::jsonb;
  v_resolution_result_json jsonb := '{}'::jsonb;
  v_resolution_fingerprint text := null;

  v_applied_components jsonb := '[]'::jsonb;
  v_event_reason text := null;
BEGIN
  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
      'code', 'CANDIDATE_ID_REQUIRED',
      'message', 'pay_finance_component_resolutions_apply: candidate_id is required'
    )::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'pay_finance_component_resolutions_apply: actor_user_id is required'
    )::text;
  END IF;

  IF jsonb_typeof(v_input_json) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
      'code', 'COMPONENT_RESOLUTIONS_ARRAY_REQUIRED',
      'message', 'pay_finance_component_resolutions_apply: component_resolutions must be a JSON array'
    )::text;
  END IF;

  SELECT
    c.id,
    upper(coalesce(c.pay_method, '')) AS pay_method
  INTO v_candidate
  FROM public.candidates c
  WHERE c.id = p_candidate_id
  LIMIT 1;

  IF v_candidate.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
      'code', 'CANDIDATE_NOT_FOUND',
      'message', 'pay_finance_component_resolutions_apply: candidate not found',
      'candidate_id', p_candidate_id::text
    )::text;
  END IF;

  IF v_candidate.pay_method NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
      'code', 'CANDIDATE_PAY_METHOD_INVALID',
      'message', 'pay_finance_component_resolutions_apply: candidate pay_method must be PAYE or UMBRELLA',
      'candidate_id', p_candidate_id::text,
      'candidate_pay_method', v_candidate.pay_method
    )::text;
  END IF;

  SELECT
    sfp.vat_rate_pct,
    sfp.erni_pct
  INTO v_finance_settings
  FROM public.settings_finance_pick(v_today_uk) sfp
  LIMIT 1;

  IF v_finance_settings.vat_rate_pct IS NULL OR v_finance_settings.erni_pct IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
      'code', 'FINANCE_SETTINGS_MISSING',
      'message', 'pay_finance_component_resolutions_apply: finance settings missing vat_rate_pct and/or erni_pct'
    )::text;
  END IF;

  IF jsonb_array_length(v_input_json) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'candidate_id', p_candidate_id::text,
      'finance_case_id', CASE WHEN p_finance_case_id IS NULL THEN NULL ELSE p_finance_case_id::text END,
      'applied_count', 0,
      'components', '[]'::jsonb
    );
  END IF;

  FOR v_resolution_json IN
    SELECT t_resolution.value
    FROM jsonb_array_elements(v_input_json) AS t_resolution(value)
  LOOP
    v_row_seq := v_row_seq + 1;

    IF v_resolution_json IS NULL OR jsonb_typeof(v_resolution_json) <> 'object' THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'INVALID_RESOLUTION_ROW',
        'message', 'pay_finance_component_resolutions_apply: each resolution row must be a JSON object',
        'row_index', v_row_seq
      )::text;
    END IF;

    v_component_id_text := nullif(btrim(coalesce(v_resolution_json->>'finance_component_id', '')), '');
    IF v_component_id_text IS NULL THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'FINANCE_COMPONENT_ID_REQUIRED',
        'message', 'pay_finance_component_resolutions_apply: finance_component_id is required for every resolution row',
        'row_index', v_row_seq
      )::text;
    END IF;

    BEGIN
      v_component_id := v_component_id_text::uuid;
    EXCEPTION WHEN others THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'FINANCE_COMPONENT_ID_INVALID',
        'message', 'pay_finance_component_resolutions_apply: finance_component_id must be a valid UUID',
        'row_index', v_row_seq,
        'finance_component_id', v_component_id_text
      )::text;
    END;

    v_resolution_mode_text := upper(coalesce(nullif(btrim(coalesce(v_resolution_json->>'resolution_mode', '')), ''), ''));
    IF v_resolution_mode_text NOT IN ('SUGGESTED_EQUIVALENT_BASIS', 'MANUAL_REPLACEMENT_RATE', 'MANUAL_AMOUNT') THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'RESOLUTION_MODE_INVALID',
        'message', 'pay_finance_component_resolutions_apply: resolution_mode must be SUGGESTED_EQUIVALENT_BASIS, MANUAL_REPLACEMENT_RATE, or MANUAL_AMOUNT',
        'row_index', v_row_seq,
        'finance_component_id', v_component_id::text,
        'resolution_mode', v_resolution_mode_text
      )::text;
    END IF;
    v_resolution_mode := v_resolution_mode_text::public.pay_finance_component_resolution_mode_enum;

    v_target_pay_method := upper(
      coalesce(
        nullif(btrim(coalesce(v_resolution_json->>'target_pay_method', '')), ''),
        v_candidate.pay_method
      )
    );

    IF v_target_pay_method NOT IN ('PAYE', 'UMBRELLA') THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'TARGET_PAY_METHOD_INVALID',
        'message', 'pay_finance_component_resolutions_apply: target_pay_method must be PAYE or UMBRELLA',
        'row_index', v_row_seq,
        'finance_component_id', v_component_id::text,
        'target_pay_method', v_target_pay_method
      )::text;
    END IF;

    IF v_target_pay_method IS DISTINCT FROM v_candidate.pay_method THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'TARGET_PAY_METHOD_MISMATCH',
        'message', 'pay_finance_component_resolutions_apply: target_pay_method must match the candidate current pay_method',
        'row_index', v_row_seq,
        'finance_component_id', v_component_id::text,
        'target_pay_method', v_target_pay_method,
        'candidate_pay_method', v_candidate.pay_method
      )::text;
    END IF;

    SELECT
      pfc.id,
      pfc.finance_case_id,
      pfc.candidate_id,
      pfc.client_id,
      pfc.linked_timesheet_id,
      pfc.source_family_key,
      pfc.component_key_type,
      pfc.component_key_value,
      pfc.classification,
      upper(coalesce(pfc.source_pay_method, '')) AS source_pay_method,
      pfc.source_basis_json,
      round(coalesce(pfc.source_amount, 0), 2) AS source_amount,
      round(coalesce(pfc.remaining_source_amount, 0), 2) AS remaining_source_amount,
      pfc.saved_target_pay_method,
      pfc.saved_resolution_mode,
      pfc.saved_resolution_payload_json,
      pfc.saved_resolution_result_json,
      pfc.resolution_fingerprint,
      pfc.is_resolution_stale,
      pfc.stale_reason,
      pfc.resolved_at_utc,
      pfc.closed_at_utc,
      pa.case_type,
      pa.status AS finance_case_status
    INTO v_component
    FROM public.pay_finance_case_components pfc
    JOIN public.pay_advances pa
      ON pa.id = pfc.finance_case_id
    WHERE pfc.id = v_component_id
    FOR UPDATE;

    IF v_component.id IS NULL THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'FINANCE_COMPONENT_NOT_FOUND',
        'message', 'pay_finance_component_resolutions_apply: finance component not found',
        'row_index', v_row_seq,
        'finance_component_id', v_component_id::text
      )::text;
    END IF;

    IF v_component.candidate_id IS DISTINCT FROM p_candidate_id THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'FINANCE_COMPONENT_CANDIDATE_MISMATCH',
        'message', 'pay_finance_component_resolutions_apply: finance component does not belong to the specified candidate',
        'row_index', v_row_seq,
        'finance_component_id', v_component_id::text,
        'candidate_id', p_candidate_id::text,
        'component_candidate_id', v_component.candidate_id::text
      )::text;
    END IF;

    IF p_finance_case_id IS NOT NULL AND v_component.finance_case_id IS DISTINCT FROM p_finance_case_id THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'FINANCE_CASE_MISMATCH',
        'message', 'pay_finance_component_resolutions_apply: finance component does not belong to the specified finance case',
        'row_index', v_row_seq,
        'finance_component_id', v_component_id::text,
        'finance_case_id', p_finance_case_id::text,
        'component_finance_case_id', v_component.finance_case_id::text
      )::text;
    END IF;

    IF v_component.closed_at_utc IS NOT NULL OR round(coalesce(v_component.remaining_source_amount, 0), 2) <= 0 THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'FINANCE_COMPONENT_NOT_OPEN',
        'message', 'pay_finance_component_resolutions_apply: finance component must be open with remaining_source_amount > 0',
        'row_index', v_row_seq,
        'finance_component_id', v_component_id::text,
        'closed_at_utc', v_component.closed_at_utc,
        'remaining_source_amount', round(coalesce(v_component.remaining_source_amount, 0), 2)
      )::text;
    END IF;

    IF v_component.classification IS DISTINCT FROM 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'REIMBURSEMENT_RESOLUTION_NOT_ALLOWED',
        'message', 'pay_finance_component_resolutions_apply: reimbursement components cannot be resolved through this path',
        'row_index', v_row_seq,
        'finance_component_id', v_component_id::text,
        'classification', v_component.classification::text
      )::text;
    END IF;

    IF v_component.source_pay_method NOT IN ('PAYE', 'UMBRELLA') THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'SOURCE_PAY_METHOD_INVALID',
        'message', 'pay_finance_component_resolutions_apply: source_pay_method must be PAYE or UMBRELLA on the component',
        'row_index', v_row_seq,
        'finance_component_id', v_component_id::text,
        'source_pay_method', v_component.source_pay_method
      )::text;
    END IF;

    v_source_units := CASE
      WHEN coalesce(v_component.source_basis_json->>'source_units', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_component.source_basis_json->>'source_units')::numeric, 6)
      ELSE NULL
    END;
    v_source_rate := CASE
      WHEN coalesce(v_component.source_basis_json->>'source_rate', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_component.source_basis_json->>'source_rate')::numeric, 6)
      ELSE NULL
    END;
    v_source_charge_rate := CASE
      WHEN coalesce(v_component.source_basis_json->>'source_charge_rate', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_component.source_basis_json->>'source_charge_rate')::numeric, 6)
      ELSE NULL
    END;
    v_bucket_code := nullif(upper(btrim(coalesce(v_component.source_basis_json->>'bucket_code', ''))), '');
    v_is_rate_bearing_row := (
      v_component.classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
      and v_component.component_key_type in ('TS_DAY', 'TS_TOTAL', 'ADDITIONAL_CODE')
      and v_component.component_key_type <> 'ADJUSTMENT_CODE'
      and v_source_units is not null
      and v_source_units > 0
      and v_source_rate is not null
      and v_source_charge_rate is not null
    );
    v_is_amount_led_row := not v_is_rate_bearing_row;
    v_component_semantics := case when v_is_rate_bearing_row then 'RATE_BEARING' else 'AMOUNT_LED' end;

    v_basis_amount_ex_vat := round(coalesce(v_component.remaining_source_amount, 0), 2)::numeric(12,2);

    v_relevant_erni_pct := CASE
      WHEN coalesce(v_resolution_json->>'relevant_erni_pct', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_resolution_json->>'relevant_erni_pct')::numeric, 6)
      WHEN coalesce(v_component.saved_resolution_payload_json->>'relevant_erni_pct', v_component.saved_resolution_result_json->>'relevant_erni_pct', '') ~ '^-?\d+(\.\d+)?$'
        THEN round(coalesce(v_component.saved_resolution_payload_json->>'relevant_erni_pct', v_component.saved_resolution_result_json->>'relevant_erni_pct')::numeric, 6)
      ELSE round(v_finance_settings.erni_pct, 6)
    END;

    v_vat_rate_pct := CASE
      WHEN coalesce(v_resolution_json->>'vat_rate_pct', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_resolution_json->>'vat_rate_pct')::numeric, 6)
      WHEN coalesce(v_component.saved_resolution_payload_json->>'vat_rate_pct', v_component.saved_resolution_result_json->>'vat_rate_pct', '') ~ '^-?\d+(\.\d+)?$'
        THEN round(coalesce(v_component.saved_resolution_payload_json->>'vat_rate_pct', v_component.saved_resolution_result_json->>'vat_rate_pct')::numeric, 6)
      ELSE round(v_finance_settings.vat_rate_pct, 6)
    END;

    v_umbrella_vat_chargeable := coalesce(
      CASE
        WHEN lower(coalesce(v_resolution_json->>'umbrella_vat_chargeable', '')) IN ('true', 'false')
          THEN (v_resolution_json->>'umbrella_vat_chargeable')::boolean
        ELSE NULL
      END,
      CASE
        WHEN lower(coalesce(v_component.saved_resolution_payload_json->>'umbrella_vat_chargeable', '')) IN ('true', 'false')
          THEN (v_component.saved_resolution_payload_json->>'umbrella_vat_chargeable')::boolean
        ELSE NULL
      END,
      CASE
        WHEN lower(coalesce(v_component.saved_resolution_result_json->>'umbrella_vat_chargeable', '')) IN ('true', 'false')
          THEN (v_component.saved_resolution_result_json->>'umbrella_vat_chargeable')::boolean
        ELSE NULL
      END,
      CASE
        WHEN lower(coalesce(v_component.source_basis_json->>'umbrella_vat_chargeable', '')) IN ('true', 'false')
          THEN (v_component.source_basis_json->>'umbrella_vat_chargeable')::boolean
        ELSE NULL
      END,
      CASE
        WHEN lower(coalesce(v_component.source_basis_json->>'vat_chargeable', '')) IN ('true', 'false')
          THEN (v_component.source_basis_json->>'vat_chargeable')::boolean
        ELSE NULL
      END,
      false
    );

    v_target_units := CASE
      WHEN coalesce(v_resolution_json->>'target_units', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_resolution_json->>'target_units')::numeric, 6)
      ELSE NULL
    END;
    v_requested_target_units := v_target_units;

    v_manual_replacement_rate := CASE
      WHEN coalesce(v_resolution_json->>'replacement_rate', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_resolution_json->>'replacement_rate')::numeric, 6)
      WHEN coalesce(v_resolution_json->>'manual_replacement_rate', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_resolution_json->>'manual_replacement_rate')::numeric, 6)
      ELSE NULL
    END;

    v_manual_amount_ex_vat := CASE
      WHEN coalesce(v_resolution_json->>'target_amount_ex_vat', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_resolution_json->>'target_amount_ex_vat')::numeric, 2)
      WHEN coalesce(v_resolution_json->>'target_amount', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_resolution_json->>'target_amount')::numeric, 2)
      WHEN coalesce(v_resolution_json->>'manual_amount_ex_vat', '') ~ '^-?\d+(\.\d+)?$'
        THEN round((v_resolution_json->>'manual_amount_ex_vat')::numeric, 2)
      ELSE NULL
    END;

    IF v_is_rate_bearing_row AND v_resolution_mode = 'MANUAL_AMOUNT'::public.pay_finance_component_resolution_mode_enum THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'MANUAL_AMOUNT_NOT_ALLOWED_FOR_RATE_BEARING_COMPONENT',
        'message', 'pay_finance_component_resolutions_apply: MANUAL_AMOUNT is not allowed for genuine rate-bearing taxable rows',
        'row_index', v_row_seq,
        'finance_component_id', v_component.id::text,
        'component_semantics', v_component_semantics
      )::text;
    END IF;

    IF v_is_amount_led_row AND v_resolution_mode = 'MANUAL_REPLACEMENT_RATE'::public.pay_finance_component_resolution_mode_enum THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'MANUAL_REPLACEMENT_RATE_NOT_ALLOWED_FOR_AMOUNT_LED_COMPONENT',
        'message', 'pay_finance_component_resolutions_apply: MANUAL_REPLACEMENT_RATE is only allowed for genuine rate-bearing taxable rows',
        'row_index', v_row_seq,
        'finance_component_id', v_component.id::text,
        'component_semantics', v_component_semantics
      )::text;
    END IF;

    IF v_is_rate_bearing_row THEN
      IF v_target_units IS NULL THEN
        v_target_units := round(v_source_units, 6);
      END IF;

      IF round(coalesce(v_target_units, 0), 6) <> round(coalesce(v_source_units, 0), 6) THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
          'code', 'RATE_BEARING_TARGET_UNITS_MISMATCH',
          'message', 'pay_finance_component_resolutions_apply: rate-bearing rows must preserve exact source units',
          'row_index', v_row_seq,
          'finance_component_id', v_component.id::text,
          'source_units', v_source_units,
          'target_units', v_target_units
        )::text;
      END IF;
    ELSE
      v_target_units := NULL;
    END IF;

    v_target_amount_ex_vat := null;
    v_target_amount_vat := null;
    v_target_amount_inc_vat := null;
    v_target_amounts_json := '{}'::jsonb;
    v_target_basis_json := '{}'::jsonb;
    v_resolution_payload_json := '{}'::jsonb;
    v_resolution_result_json := '{}'::jsonb;
    v_resolution_fingerprint := null;
    v_target_amount_ex_vat_per_source := 0;
    v_target_amount_vat_per_source := 0;
    v_target_amount_inc_vat_per_source := 0;
    v_target_units_per_source := null;

    v_before_json := jsonb_build_object(
      'finance_component_id', v_component.id::text,
      'finance_case_id', v_component.finance_case_id::text,
      'classification', v_component.classification::text,
      'source_pay_method', v_component.source_pay_method,
      'source_basis_json', v_component.source_basis_json,
      'source_amount', round(coalesce(v_component.source_amount, 0), 2),
      'remaining_source_amount', round(coalesce(v_component.remaining_source_amount, 0), 2),
      'saved_target_pay_method', v_component.saved_target_pay_method,
      'saved_resolution_mode', CASE WHEN v_component.saved_resolution_mode IS NULL THEN NULL ELSE v_component.saved_resolution_mode::text END,
      'saved_resolution_payload_json', v_component.saved_resolution_payload_json,
      'saved_resolution_result_json', v_component.saved_resolution_result_json,
      'source_units', v_source_units,
      'source_rate', v_source_rate,
      'source_charge_rate', v_source_charge_rate,
      'bucket_code', v_bucket_code,
      'component_semantics', v_component_semantics,
      'is_rate_bearing_row', v_is_rate_bearing_row,
      'is_amount_led_row', v_is_amount_led_row,
      'resolution_fingerprint', v_component.resolution_fingerprint,
      'is_resolution_stale', v_component.is_resolution_stale,
      'stale_reason', v_component.stale_reason,
      'resolved_at_utc', v_component.resolved_at_utc
    );

    IF v_resolution_mode = 'SUGGESTED_EQUIVALENT_BASIS'::public.pay_finance_component_resolution_mode_enum THEN
      IF v_component.source_pay_method = 'PAYE' AND v_target_pay_method = 'UMBRELLA' THEN
        v_target_amounts_json := public._pay_convert_paye_to_umbrella(
          v_basis_amount_ex_vat,
          v_relevant_erni_pct,
          v_vat_rate_pct,
          v_umbrella_vat_chargeable
        );
        v_target_amount_ex_vat := round(coalesce((v_target_amounts_json->>'ex')::numeric, 0), 2);
        v_target_amount_vat := round(coalesce((v_target_amounts_json->>'vat')::numeric, 0), 2);
        v_target_amount_inc_vat := round(coalesce((v_target_amounts_json->>'inc')::numeric, 0), 2);
      ELSIF v_component.source_pay_method = 'UMBRELLA' AND v_target_pay_method = 'PAYE' THEN
        v_target_amount_ex_vat := public._pay_convert_umbrella_to_paye_ex(v_basis_amount_ex_vat, v_relevant_erni_pct);
        v_target_amount_vat := 0;
        v_target_amount_inc_vat := v_target_amount_ex_vat;
      ELSIF v_component.source_pay_method = v_target_pay_method AND v_target_pay_method = 'PAYE' THEN
        v_target_amount_ex_vat := v_basis_amount_ex_vat;
        v_target_amount_vat := 0;
        v_target_amount_inc_vat := v_target_amount_ex_vat;
      ELSIF v_component.source_pay_method = v_target_pay_method AND v_target_pay_method = 'UMBRELLA' THEN
        v_target_amounts_json := public._pay_umbrella_vat_calc(
          v_basis_amount_ex_vat,
          v_vat_rate_pct,
          v_umbrella_vat_chargeable
        );
        v_target_amount_ex_vat := round(coalesce((v_target_amounts_json->>'ex')::numeric, 0), 2);
        v_target_amount_vat := round(coalesce((v_target_amounts_json->>'vat')::numeric, 0), 2);
        v_target_amount_inc_vat := round(coalesce((v_target_amounts_json->>'inc')::numeric, 0), 2);
      ELSE
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
          'code', 'UNSUPPORTED_SUGGESTED_CONVERSION',
          'message', 'pay_finance_component_resolutions_apply: unsupported suggested-equivalent conversion pair',
          'row_index', v_row_seq,
          'finance_component_id', v_component.id::text,
          'source_pay_method', v_component.source_pay_method,
          'target_pay_method', v_target_pay_method
        )::text;
      END IF;

      IF v_target_units IS NOT NULL THEN
        IF v_target_units <= 0 THEN
          RAISE EXCEPTION '%', jsonb_build_object(
            'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
            'code', 'TARGET_UNITS_INVALID',
            'message', 'pay_finance_component_resolutions_apply: target_units must be > 0 when provided for suggested equivalent basis',
            'row_index', v_row_seq,
            'finance_component_id', v_component.id::text,
            'target_units', v_target_units
          )::text;
        END IF;
        v_rate_precision := round(v_target_amount_ex_vat / v_target_units, 6);
      ELSE
        v_rate_precision := null;
      END IF;
    ELSIF v_resolution_mode = 'MANUAL_REPLACEMENT_RATE'::public.pay_finance_component_resolution_mode_enum THEN
      IF v_target_units IS NULL OR v_target_units <= 0 THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
          'code', 'TARGET_UNITS_REQUIRED',
          'message', 'pay_finance_component_resolutions_apply: target_units must be > 0 for MANUAL_REPLACEMENT_RATE',
          'row_index', v_row_seq,
          'finance_component_id', v_component.id::text,
          'target_units', v_target_units
        )::text;
      END IF;

      IF v_manual_replacement_rate IS NULL OR v_manual_replacement_rate < 0 THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
          'code', 'REPLACEMENT_RATE_REQUIRED',
          'message', 'pay_finance_component_resolutions_apply: replacement_rate must be >= 0 for MANUAL_REPLACEMENT_RATE',
          'row_index', v_row_seq,
          'finance_component_id', v_component.id::text,
          'replacement_rate', v_manual_replacement_rate
        )::text;
      END IF;

      v_target_amount_ex_vat := round(v_target_units * v_manual_replacement_rate, 2);

      IF v_target_pay_method = 'PAYE' THEN
        v_target_amount_vat := 0;
        v_target_amount_inc_vat := v_target_amount_ex_vat;
      ELSE
        v_target_amounts_json := public._pay_umbrella_vat_calc(
          v_target_amount_ex_vat,
          v_vat_rate_pct,
          v_umbrella_vat_chargeable
        );
        v_target_amount_ex_vat := round(coalesce((v_target_amounts_json->>'ex')::numeric, 0), 2);
        v_target_amount_vat := round(coalesce((v_target_amounts_json->>'vat')::numeric, 0), 2);
        v_target_amount_inc_vat := round(coalesce((v_target_amounts_json->>'inc')::numeric, 0), 2);
      END IF;

      v_rate_precision := round(v_manual_replacement_rate, 6);
    ELSE
      IF v_manual_amount_ex_vat IS NULL OR v_manual_amount_ex_vat < 0 THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
          'code', 'TARGET_AMOUNT_REQUIRED',
          'message', 'pay_finance_component_resolutions_apply: target_amount_ex_vat must be >= 0 for MANUAL_AMOUNT',
          'row_index', v_row_seq,
          'finance_component_id', v_component.id::text,
          'target_amount_ex_vat', v_manual_amount_ex_vat
        )::text;
      END IF;

      v_target_amount_ex_vat := round(v_manual_amount_ex_vat, 2);

      IF v_target_pay_method = 'PAYE' THEN
        v_target_amount_vat := 0;
        v_target_amount_inc_vat := v_target_amount_ex_vat;
      ELSE
        v_target_amounts_json := public._pay_umbrella_vat_calc(
          v_target_amount_ex_vat,
          v_vat_rate_pct,
          v_umbrella_vat_chargeable
        );
        v_target_amount_ex_vat := round(coalesce((v_target_amounts_json->>'ex')::numeric, 0), 2);
        v_target_amount_vat := round(coalesce((v_target_amounts_json->>'vat')::numeric, 0), 2);
        v_target_amount_inc_vat := round(coalesce((v_target_amounts_json->>'inc')::numeric, 0), 2);
      END IF;

      v_rate_precision := null;
    END IF;

    IF v_basis_amount_ex_vat <= 0 THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_COMPONENT_RESOLUTIONS_APPLY',
        'code', 'BASIS_AMOUNT_INVALID',
        'message', 'pay_finance_component_resolutions_apply: basis_source_amount_ex_vat must be > 0',
        'row_index', v_row_seq,
        'finance_component_id', v_component.id::text,
        'basis_source_amount_ex_vat', v_basis_amount_ex_vat
      )::text;
    END IF;

    v_target_amount_ex_vat_per_source := round(v_target_amount_ex_vat / v_basis_amount_ex_vat, 10);
    v_target_amount_vat_per_source := round(v_target_amount_vat / v_basis_amount_ex_vat, 10);
    v_target_amount_inc_vat_per_source := round(v_target_amount_inc_vat / v_basis_amount_ex_vat, 10);
    v_target_units_per_source := CASE
      WHEN v_target_units IS NOT NULL THEN round(v_target_units / v_basis_amount_ex_vat, 10)
      ELSE NULL
    END;

    v_target_basis_json := jsonb_strip_nulls(
      jsonb_build_object(
        'target_pay_method', v_target_pay_method,
        'calculation_kind', v_resolution_mode::text,
        'relevant_erni_pct', round(v_relevant_erni_pct, 6),
        'vat_rate_pct', round(v_vat_rate_pct, 6),
        'umbrella_vat_chargeable', v_umbrella_vat_chargeable,
        'bucket_code', v_bucket_code,
        'source_units', v_source_units,
        'source_rate', v_source_rate,
        'source_charge_rate', v_source_charge_rate,
        'component_semantics', v_component_semantics,
        'is_rate_bearing_row', v_is_rate_bearing_row,
        'is_amount_led_row', v_is_amount_led_row,
        'replacement_rate', CASE WHEN v_resolution_mode = 'MANUAL_REPLACEMENT_RATE'::public.pay_finance_component_resolution_mode_enum THEN round(v_manual_replacement_rate, 6) ELSE NULL END,
        'target_amount_ex_vat_per_source_ex_vat', v_target_amount_ex_vat_per_source,
        'target_amount_vat_per_source_ex_vat', v_target_amount_vat_per_source,
        'target_amount_inc_vat_per_source_ex_vat', v_target_amount_inc_vat_per_source,
        'target_units_per_source_ex_vat', v_target_units_per_source,
        'suggested_target_rate', CASE WHEN v_resolution_mode = 'SUGGESTED_EQUIVALENT_BASIS'::public.pay_finance_component_resolution_mode_enum THEN v_rate_precision ELSE NULL END
      )
    );

    v_resolution_payload_json := jsonb_strip_nulls(
      jsonb_build_object(
        'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE',
        'resolution_mode', v_resolution_mode::text,
        'target_pay_method', v_target_pay_method,
        'applied_basis_source_amount_ex_vat', round(v_basis_amount_ex_vat, 2),
        'relevant_erni_pct', round(v_relevant_erni_pct, 6),
        'vat_rate_pct', round(v_vat_rate_pct, 6),
        'umbrella_vat_chargeable', v_umbrella_vat_chargeable,
        'bucket_code', v_bucket_code,
        'source_units', v_source_units,
        'source_rate', v_source_rate,
        'source_charge_rate', v_source_charge_rate,
        'component_semantics', v_component_semantics,
        'is_rate_bearing_row', v_is_rate_bearing_row,
        'is_amount_led_row', v_is_amount_led_row,
        'requested_target_units', CASE WHEN v_requested_target_units IS NULL THEN NULL ELSE round(v_requested_target_units, 6) END,
        'target_units', CASE WHEN v_target_units IS NULL THEN NULL ELSE round(v_target_units, 6) END,
        'replacement_rate', CASE WHEN v_manual_replacement_rate IS NULL THEN NULL ELSE round(v_manual_replacement_rate, 6) END,
        'manual_amount_ex_vat', CASE WHEN v_manual_amount_ex_vat IS NULL THEN NULL ELSE round(v_manual_amount_ex_vat, 2) END,
        'suggested_target_rate', CASE WHEN v_resolution_mode = 'SUGGESTED_EQUIVALENT_BASIS'::public.pay_finance_component_resolution_mode_enum THEN v_rate_precision ELSE NULL END,
        'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT'
      )
    );

    v_resolution_result_json := jsonb_strip_nulls(
      jsonb_build_object(
        'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE',
        'target_pay_method', v_target_pay_method,
        'target_amount_ex_vat', round(v_target_amount_ex_vat, 2),
        'target_amount_vat', round(v_target_amount_vat, 2),
        'target_amount_inc_vat', round(v_target_amount_inc_vat, 2),
        'basis_source_amount_ex_vat', round(v_basis_amount_ex_vat, 2),
        'applied_basis_source_amount_ex_vat', round(v_basis_amount_ex_vat, 2),
        'relevant_erni_pct', round(v_relevant_erni_pct, 6),
        'vat_rate_pct', round(v_vat_rate_pct, 6),
        'umbrella_vat_chargeable', v_umbrella_vat_chargeable,
        'bucket_code', v_bucket_code,
        'source_units', v_source_units,
        'source_rate', v_source_rate,
        'source_charge_rate', v_source_charge_rate,
        'component_semantics', v_component_semantics,
        'is_rate_bearing_row', v_is_rate_bearing_row,
        'is_amount_led_row', v_is_amount_led_row,
        'requested_target_units', CASE WHEN v_requested_target_units IS NULL THEN NULL ELSE round(v_requested_target_units, 6) END,
        'target_units', CASE WHEN v_target_units IS NULL THEN NULL ELSE round(v_target_units, 6) END,
        'replacement_rate', CASE WHEN v_manual_replacement_rate IS NULL THEN NULL ELSE round(v_manual_replacement_rate, 6) END,
        'target_amount_ex_vat_per_source_ex_vat', v_target_amount_ex_vat_per_source,
        'target_amount_vat_per_source_ex_vat', v_target_amount_vat_per_source,
        'target_amount_inc_vat_per_source_ex_vat', v_target_amount_inc_vat_per_source,
        'target_units_per_source_ex_vat', v_target_units_per_source,
        'reuse_mode', 'PROPORTIONAL_TO_REMAINING_SOURCE_AMOUNT'
      )
    );

    v_resolution_fingerprint := public.pay_finance_component_fingerprint(
      p_source_family_key         => v_component.source_family_key,
      p_component_key_type        => v_component.component_key_type,
      p_component_key_value       => v_component.component_key_value,
      p_classification            => v_component.classification,
      p_source_pay_method         => v_component.source_pay_method,
      p_current_target_pay_method => v_target_pay_method,
      p_source_basis_json         => coalesce(v_component.source_basis_json, '{}'::jsonb),
      p_source_amount             => round(coalesce(v_component.source_amount, 0), 2),
      p_relevant_erni_pct         => round(v_relevant_erni_pct, 6),
      p_target_basis_json         => v_resolution_payload_json
    );

    UPDATE public.pay_finance_case_components pfc
    SET
      saved_target_pay_method = v_target_pay_method,
      saved_resolution_mode = v_resolution_mode,
      saved_resolution_payload_json = v_resolution_payload_json,
      saved_resolution_result_json = v_resolution_result_json,
      resolution_fingerprint = v_resolution_fingerprint,
      resolved_at_utc = v_now_utc,
      is_resolution_stale = false,
      stale_reason = NULL,
      updated_at_utc = v_now_utc
    WHERE pfc.id = v_component.id;

    SELECT jsonb_build_object(
      'finance_component_id', pfc.id::text,
      'finance_case_id', pfc.finance_case_id::text,
      'classification', pfc.classification::text,
      'source_pay_method', pfc.source_pay_method,
      'source_basis_json', pfc.source_basis_json,
      'source_amount', round(coalesce(pfc.source_amount, 0), 2),
      'remaining_source_amount', round(coalesce(pfc.remaining_source_amount, 0), 2),
      'saved_target_pay_method', pfc.saved_target_pay_method,
      'saved_resolution_mode', CASE WHEN pfc.saved_resolution_mode IS NULL THEN NULL ELSE pfc.saved_resolution_mode::text END,
      'saved_resolution_payload_json', pfc.saved_resolution_payload_json,
      'saved_resolution_result_json', pfc.saved_resolution_result_json,
      'source_units', v_source_units,
      'source_rate', v_source_rate,
      'source_charge_rate', v_source_charge_rate,
      'bucket_code', v_bucket_code,
      'component_semantics', v_component_semantics,
      'is_rate_bearing_row', v_is_rate_bearing_row,
      'is_amount_led_row', v_is_amount_led_row,
      'resolution_fingerprint', pfc.resolution_fingerprint,
      'is_resolution_stale', pfc.is_resolution_stale,
      'stale_reason', pfc.stale_reason,
      'resolved_at_utc', pfc.resolved_at_utc
    )
    INTO v_after_json
    FROM public.pay_finance_case_components pfc
    WHERE pfc.id = v_component.id;

    v_event_reason := coalesce(nullif(btrim(coalesce(p_reason, '')), ''), 'COMPONENT_RESOLUTION_APPLIED');

    INSERT INTO public.pay_finance_case_events (
      finance_case_id,
      finance_component_id,
      event_type,
      event_at_utc,
      actor_user_id,
      pay_batch_id,
      reservation_id,
      before_json,
      after_json,
      reason,
      note
    )
    VALUES (
      v_component.finance_case_id,
      v_component.id,
      'COMPONENT_RESOLUTION_APPLIED',
      v_now_utc,
      p_actor_user_id,
      NULL,
      NULL,
      v_before_json,
      v_after_json,
      v_event_reason,
      ('Applied ' || v_resolution_mode::text || ' to taxable finance component')
    );

    v_applied_components := v_applied_components || jsonb_build_array(
      jsonb_build_object(
        'finance_component_id', v_component.id::text,
        'finance_case_id', v_component.finance_case_id::text,
        'saved_target_pay_method', v_target_pay_method,
        'saved_resolution_mode', v_resolution_mode::text,
        'saved_resolution_payload_json', v_resolution_payload_json,
        'saved_resolution_result_json', v_resolution_result_json,
        'source_units', v_source_units,
        'source_rate', v_source_rate,
        'source_charge_rate', v_source_charge_rate,
        'bucket_code', v_bucket_code,
        'component_semantics', v_component_semantics,
        'is_rate_bearing_row', v_is_rate_bearing_row,
        'is_amount_led_row', v_is_amount_led_row,
        'resolution_fingerprint', v_resolution_fingerprint,
        'resolved_at_utc', v_now_utc
      )
    );
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'candidate_id', p_candidate_id::text,
    'finance_case_id', CASE WHEN p_finance_case_id IS NULL THEN NULL ELSE p_finance_case_id::text END,
    'applied_count', jsonb_array_length(v_applied_components),
    'components', v_applied_components
  );
END;
$function$;

ALTER FUNCTION public.pay_finance_component_resolutions_apply(uuid,jsonb,uuid,uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_finance_component_resolutions_apply(uuid,jsonb,uuid,uuid,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_finance_component_resolutions_apply(uuid,jsonb,uuid,uuid,text) TO service_role;

