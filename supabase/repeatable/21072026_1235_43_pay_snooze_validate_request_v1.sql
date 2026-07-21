-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 42d78303df7f.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.pay_snooze_validate_request_v1(
  p_actor_user_id uuid,
  p_candidate_id uuid,
  p_timesheet_id uuid,
  p_segment_id text DEFAULT NULL::text,
  p_segment_stable_key text DEFAULT NULL::text,
  p_source_ref text DEFAULT NULL::text,
  p_snooze_kind text DEFAULT 'DO_NOT_PAY'::text,
  p_snooze_until_date date DEFAULT NULL::date,
  p_validation_phase text DEFAULT 'PRE_SAVE'::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_phase text := UPPER(BTRIM(COALESCE(p_validation_phase, 'PRE_SAVE')));
  v_issue_acknowledgement boolean := true;
  v_kind_input text := UPPER(BTRIM(COALESCE(p_snooze_kind, 'DO_NOT_PAY')));
  v_kind text;
  v_is_payment_scope boolean := false;
  v_scope_kind text;
  v_noun text;
  v_input_segment_id text := NULLIF(BTRIM(COALESCE(p_segment_id, '')), '');
  v_input_segment_stable_key text := NULLIF(BTRIM(COALESCE(p_segment_stable_key, '')), '');
  v_source_ref text := LOWER(NULLIF(BTRIM(COALESCE(p_source_ref, '')), ''));
  v_is_timesheet_expense_scope boolean := false;
  v_expense_source_timesheet_id uuid := NULL::uuid;
  v_expense_code text := NULL::text;
  v_expense_label text := NULL::text;
  v_expense_item_type text := NULL::text;
  v_expense_source_basis_fingerprint text := NULL::text;
  v_expense_source_basis_json jsonb := NULL::jsonb;
  v_expense_amount_ex_vat numeric := NULL::numeric;
  v_expense_amount_display numeric := NULL::numeric;
  v_expense_source_charge_ex_vat numeric := NULL::numeric;
  v_expense_source_row_json jsonb := NULL::jsonb;
  v_finance_case_id uuid := NULL::uuid;
  v_expected_finance_case_type public.pay_finance_case_type_enum;
  v_effective_timesheet_id uuid := NULL::uuid;
  v_booking_id text := NULL::text;
  v_effective_candidate_id uuid := NULL::uuid;
  v_effective_segment_id text := NULL::text;
  v_effective_segment_stable_key text := NULL::text;
  v_invoice_breakdown_json jsonb := '{}'::jsonb;
  v_date_context jsonb;
  v_london_current_date date;
  v_next_official_pay_date date;
  v_date_context_fingerprint text;
  v_has_resolved_rate boolean := false;
  v_resolved_component_count integer := 0;
  v_resolution_fingerprint text := NULL::text;
  v_warning_required boolean := false;
  v_warning_code text := NULL::text;
  v_warning_message text := NULL::text;
  v_date_relation text := 'NOT_APPLICABLE';
  v_scope_fingerprint text;
  v_acknowledgement_token uuid := NULL::uuid;
  v_acknowledgement_expires_at_utc timestamptz := NULL::timestamptz;
BEGIN
  declare v_correction_chain jsonb;
  begin
    if p_timesheet_id is not null and coalesce((public._ctms_import_correction_classify_v1(p_timesheet_id)->>'is_import_authoritative_correction')::boolean,false) then
      v_correction_chain:=public.timesheet_correction_chain_scope_v1(p_timesheet_id,false,32,100);
      if coalesce((v_correction_chain->>'valid')::boolean,false) is not true then raise exception 'CORRECTION_CHAIN_INVALID_FOR_SNOOZE' using errcode='P0001',detail=v_correction_chain::text; end if;
      v_input_segment_stable_key:='correction-chain:'||(v_correction_chain->>'root_timesheet_id');
      v_source_ref:=v_input_segment_stable_key;
    end if;
  end;
  IF v_phase IN ('INTERNAL_PRE_OPEN', 'PRE_OPEN_CHECK') THEN
    v_phase := 'PRE_OPEN';
    v_issue_acknowledgement := false;
  ELSIF v_phase IN ('INTERNAL_PRE_SAVE', 'PRE_SAVE_CHECK') THEN
    v_phase := 'PRE_SAVE';
    v_issue_acknowledgement := false;
  ELSIF v_phase NOT IN ('PRE_OPEN', 'PRE_SAVE') THEN
    RAISE EXCEPTION 'SNOOZE_VALIDATION_PHASE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'SNOOZE_VALIDATION_PHASE_INVALID',
              'validation_phase', p_validation_phase
            )::text;
  END IF;

  IF p_actor_user_id IS NULL OR NOT EXISTS (
    SELECT 1
    FROM public.tms_users AS actor_row
    WHERE actor_row.id = p_actor_user_id
      AND actor_row.is_active IS TRUE
  ) THEN
    RAISE EXCEPTION 'SNOOZE_ACTOR_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'SNOOZE_ACTOR_INVALID')::text;
  END IF;

  IF p_candidate_id IS NULL OR NOT EXISTS (
    SELECT 1
    FROM public.candidates AS candidate_row
    WHERE candidate_row.id = p_candidate_id
  ) THEN
    RAISE EXCEPTION 'SNOOZE_CANDIDATE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'SNOOZE_CANDIDATE_INVALID')::text;
  END IF;

  v_kind := CASE v_kind_input
    WHEN 'BLOCKED' THEN 'BLOCKED_TIMESHEET'
    WHEN 'BLOCKED_TIMESHEET' THEN 'BLOCKED_TIMESHEET'
    WHEN 'DO_NOT_PAY' THEN 'DO_NOT_PAY'
    WHEN 'TIMESHEET_PAYMENT' THEN 'TIMESHEET_PAYMENT'
    WHEN 'OVERPAYMENT_RECOVERY' THEN 'OVERPAYMENT_RECOVERY'
    WHEN 'PAYMENT_ADVANCE_REPAYMENT' THEN 'PAYMENT_ADVANCE_REPAYMENT'
    WHEN 'LOAN_REPAYMENT' THEN 'PAYMENT_ADVANCE_REPAYMENT'
    WHEN 'MANUAL_DEBT_RECOVERY' THEN 'MANUAL_DEBT_RECOVERY'
    ELSE NULL::text
  END;

  IF v_kind IS NULL THEN
    RAISE EXCEPTION 'SNOOZE_KIND_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'SNOOZE_KIND_INVALID', 'snooze_kind', p_snooze_kind)::text;
  END IF;

  v_is_payment_scope := v_kind IN ('DO_NOT_PAY', 'BLOCKED_TIMESHEET', 'TIMESHEET_PAYMENT');
  v_is_timesheet_expense_scope := (
    v_kind = 'DO_NOT_PAY'
    AND v_source_ref IS NOT NULL
    AND v_source_ref ~ '^timesheet-expense:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}:(expenses|travel|accommodation|other|mileage):[0-9a-f]{32}$'
  );

  IF v_source_ref IS NOT NULL
     AND v_is_payment_scope
     AND COALESCE(v_is_timesheet_expense_scope, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'SNOOZE_TIMESHEET_SOURCE_REF_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'SNOOZE_TIMESHEET_SOURCE_REF_INVALID',
              'source_ref', v_source_ref,
              'snooze_kind', v_kind,
              'refresh_required', true
            )::text;
  END IF;

  IF v_is_payment_scope THEN
    IF p_timesheet_id IS NULL THEN
      RAISE EXCEPTION 'SNOOZE_TIMESHEET_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'SNOOZE_TIMESHEET_REQUIRED')::text;
    END IF;

    SELECT input_timesheet.booking_id
    INTO v_booking_id
    FROM public.timesheets AS input_timesheet
    WHERE input_timesheet.timesheet_id = p_timesheet_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'SNOOZE_TIMESHEET_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'SNOOZE_TIMESHEET_INVALID', 'timesheet_id', p_timesheet_id::text)::text;
    END IF;

    SELECT current_timesheet.timesheet_id
    INTO v_effective_timesheet_id
    FROM public.timesheets AS current_timesheet
    WHERE current_timesheet.booking_id = v_booking_id
      AND current_timesheet.is_current IS TRUE
    ORDER BY COALESCE(current_timesheet.version, 0) DESC,
             current_timesheet.updated_at DESC NULLS LAST,
             current_timesheet.created_at DESC NULLS LAST,
             current_timesheet.timesheet_id DESC
    LIMIT 1;

    v_effective_timesheet_id := COALESCE(v_effective_timesheet_id, p_timesheet_id);

    SELECT financial_row.candidate_id,
           COALESCE(financial_row.invoice_breakdown_json, '{}'::jsonb)
    INTO v_effective_candidate_id,
         v_invoice_breakdown_json
    FROM public.timesheets_financials AS financial_row
    WHERE financial_row.timesheet_id = v_effective_timesheet_id
    ORDER BY CASE WHEN financial_row.is_current IS TRUE THEN 0 ELSE 1 END,
             financial_row.updated_at DESC NULLS LAST,
             financial_row.created_at DESC NULLS LAST,
             financial_row.id DESC
    LIMIT 1;

    IF v_effective_candidate_id IS DISTINCT FROM p_candidate_id THEN
      RAISE EXCEPTION 'SNOOZE_TIMESHEET_CANDIDATE_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SNOOZE_TIMESHEET_CANDIDATE_MISMATCH',
                'candidate_id', p_candidate_id::text,
                'timesheet_id', v_effective_timesheet_id::text,
                'resolved_candidate_id', CASE WHEN v_effective_candidate_id IS NULL THEN NULL ELSE v_effective_candidate_id::text END
              )::text;
    END IF;

    IF COALESCE(v_is_timesheet_expense_scope, false) THEN
      IF v_input_segment_id IS NOT NULL OR v_input_segment_stable_key IS NOT NULL THEN
        RAISE EXCEPTION 'SNOOZE_EXPENSE_SCOPE_MUST_NOT_HAVE_SEGMENT'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'SNOOZE_EXPENSE_SCOPE_MUST_NOT_HAVE_SEGMENT',
                  'source_ref', v_source_ref
                )::text;
      END IF;

      v_expense_source_timesheet_id := split_part(v_source_ref, ':', 2)::uuid;
      v_expense_code := UPPER(split_part(v_source_ref, ':', 3));
      v_expense_source_basis_fingerprint := LOWER(split_part(v_source_ref, ':', 4));
      v_expense_label := CASE v_expense_code
        WHEN 'EXPENSES' THEN 'Expenses'
        WHEN 'TRAVEL' THEN 'Travel'
        WHEN 'ACCOMMODATION' THEN 'Accommodation'
        WHEN 'OTHER' THEN 'Other'
        WHEN 'MILEAGE' THEN 'Mileage'
        ELSE NULL::text
      END;
      v_expense_item_type := CASE WHEN v_expense_code = 'MILEAGE' THEN 'MILEAGE_DELTA' ELSE 'EXPENSE_DELTA' END;

      IF v_expense_source_timesheet_id IS DISTINCT FROM v_effective_timesheet_id THEN
        RAISE EXCEPTION 'SNOOZE_EXPENSE_TIMESHEET_IDENTITY_STALE'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'SNOOZE_EXPENSE_TIMESHEET_IDENTITY_STALE',
                  'source_ref', v_source_ref,
                  'source_timesheet_id', v_expense_source_timesheet_id::text,
                  'current_timesheet_id', v_effective_timesheet_id::text,
                  'refresh_required', true
                )::text;
      END IF;

      SELECT
        source_line.source_row_json,
        COALESCE(
          source_line.source_row_json->'expense_source_basis_json',
          source_line.source_row_json->'source_basis_json',
          '{}'::jsonb
        ),
        NULLIF(BTRIM(COALESCE(
          source_line.source_row_json->>'source_basis_fingerprint',
          source_line.source_row_json->>'expense_source_basis_fingerprint',
          ''
        )), ''),
        CASE
          WHEN COALESCE(source_line.source_row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN (source_line.source_row_json->>'amount_ex_vat')::numeric
          WHEN COALESCE(source_line.source_row_json->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN (source_line.source_row_json->>'preview_amount_ex_vat')::numeric
          ELSE NULL::numeric
        END,
        CASE
          WHEN COALESCE(source_line.source_row_json->>'amount_display', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN (source_line.source_row_json->>'amount_display')::numeric
          WHEN COALESCE(source_line.source_row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN (source_line.source_row_json->>'amount_ex_vat')::numeric
          ELSE NULL::numeric
        END,
        CASE
          WHEN COALESCE(source_line.source_row_json#>>'{expense_source_basis_json,source_charge_ex_vat}', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN (source_line.source_row_json#>>'{expense_source_basis_json,source_charge_ex_vat}')::numeric
          WHEN COALESCE(source_line.source_row_json#>>'{source_basis_json,source_charge_ex_vat}', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            THEN (source_line.source_row_json#>>'{source_basis_json,source_charge_ex_vat}')::numeric
          ELSE NULL::numeric
        END
      INTO
        v_expense_source_row_json,
        v_expense_source_basis_json,
        v_expense_source_basis_fingerprint,
        v_expense_amount_ex_vat,
        v_expense_amount_display,
        v_expense_source_charge_ex_vat
      FROM public.banking_pay_workbench_candidate_source_lines AS source_line
      JOIN public.banking_pay_workbench_sessions AS source_session
        ON source_session.id = source_line.session_id
      LEFT JOIN public.app_change_counters AS candidate_counter
        ON candidate_counter.entity_key = 'pay_candidate:' || p_candidate_id::text
      WHERE source_line.candidate_id = p_candidate_id
        AND source_line.timesheet_id = v_effective_timesheet_id
        AND source_line.status = 'CURRENT'
        AND source_line.session_version = COALESCE(source_session.version, 1)
        AND source_line.source_change_seq = COALESCE(candidate_counter.seq, 0)
        AND UPPER(BTRIM(COALESCE(source_session.status, ''))) = 'OPEN'
        AND source_session.discarded_at_utc IS NULL
        AND source_session.replacement_session_id IS NULL
        AND LOWER(BTRIM(COALESCE(source_line.source_row_json->>'source_ref', ''))) = v_source_ref
        AND UPPER(BTRIM(COALESCE(source_line.source_row_json#>>'{snooze_identity,identity_type}', ''))) = 'TIMESHEET_EXPENSE'
        AND UPPER(BTRIM(COALESCE(
              source_line.economic_key_json->>'key_type',
              source_line.source_row_json#>>'{economic_key,key_type}',
              source_line.source_row_json->>'component_key_type',
              ''
            ))) = 'EXPENSE_CODE'
        AND UPPER(BTRIM(COALESCE(
              source_line.economic_key_json->>'key_value',
              source_line.source_row_json#>>'{economic_key,key_value}',
              source_line.source_row_json->>'component_key_value',
              ''
            ))) = v_expense_code
      ORDER BY source_session.updated_at_utc DESC NULLS LAST,
               source_line.updated_at_utc DESC NULLS LAST,
               source_line.id DESC
      LIMIT 1;

      IF v_expense_source_row_json IS NULL
         OR v_expense_source_basis_fingerprint IS DISTINCT FROM LOWER(split_part(v_source_ref, ':', 4))
         OR md5(COALESCE(v_expense_source_basis_json, '{}'::jsonb)::text) IS DISTINCT FROM LOWER(split_part(v_source_ref, ':', 4))
         OR UPPER(BTRIM(COALESCE(v_expense_source_row_json->>'expense_code', v_expense_source_row_json->>'component_key_value', ''))) IS DISTINCT FROM v_expense_code THEN
        RAISE EXCEPTION 'SNOOZE_EXPENSE_SOURCE_IDENTITY_STALE'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'SNOOZE_EXPENSE_SOURCE_IDENTITY_STALE',
                  'candidate_id', p_candidate_id::text,
                  'timesheet_id', v_effective_timesheet_id::text,
                  'expense_code', v_expense_code,
                  'source_ref', v_source_ref,
                  'refresh_required', true
                )::text;
      END IF;

      v_scope_kind := 'TIMESHEET_EXPENSE';
      v_noun := LOWER(v_expense_label || ' expense');
    ELSIF v_kind = 'TIMESHEET_PAYMENT' THEN
      IF v_source_ref IS NOT NULL THEN
        RAISE EXCEPTION 'SNOOZE_TIMESHEET_SCOPE_MUST_NOT_HAVE_SOURCE_REF'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object('code', 'SNOOZE_TIMESHEET_SCOPE_MUST_NOT_HAVE_SOURCE_REF')::text;
      END IF;
      IF v_input_segment_id IS NOT NULL OR v_input_segment_stable_key IS NOT NULL THEN
        RAISE EXCEPTION 'SNOOZE_TIMESHEET_SCOPE_MUST_NOT_HAVE_SEGMENT'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object('code', 'SNOOZE_TIMESHEET_SCOPE_MUST_NOT_HAVE_SEGMENT')::text;
      END IF;
      v_scope_kind := 'TIMESHEET';
      v_noun := 'timesheet';
    ELSE
      IF v_source_ref IS NOT NULL THEN
        RAISE EXCEPTION 'SNOOZE_SEGMENT_SCOPE_MUST_NOT_HAVE_SOURCE_REF'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object('code', 'SNOOZE_SEGMENT_SCOPE_MUST_NOT_HAVE_SOURCE_REF')::text;
      END IF;
      IF v_input_segment_id IS NULL AND v_input_segment_stable_key IS NULL THEN
        RAISE EXCEPTION 'SNOOZE_SEGMENT_IDENTITY_REQUIRED'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object('code', 'SNOOZE_SEGMENT_IDENTITY_REQUIRED')::text;
      END IF;

      SELECT NULLIF(BTRIM(COALESCE(
               segment_row.value->>'segment_id',
               segment_row.value->>'id',
               segment_row.value->>'segment_key',
               ''
             )), ''),
             NULLIF(BTRIM(COALESCE(
               segment_row.value->>'segment_stable_key',
               segment_row.value->>'work_date',
               segment_row.value->>'ref_num',
               segment_row.value->>'segment_key',
               segment_row.value->>'segment_id',
               ''
             )), '')
      INTO v_effective_segment_id,
           v_effective_segment_stable_key
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array'
            THEN v_invoice_breakdown_json->'segments'
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS segment_row(value, ordinal)
      WHERE (
        v_input_segment_stable_key IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(
          segment_row.value->>'segment_stable_key',
          segment_row.value->>'work_date',
          segment_row.value->>'ref_num',
          segment_row.value->>'segment_key',
          segment_row.value->>'segment_id',
          ''
        )), '') = v_input_segment_stable_key
      ) OR (
        v_input_segment_id IS NOT NULL
        AND (
          NULLIF(BTRIM(COALESCE(segment_row.value->>'segment_id', '')), '') = v_input_segment_id
          OR NULLIF(BTRIM(COALESCE(segment_row.value->>'id', '')), '') = v_input_segment_id
          OR NULLIF(BTRIM(COALESCE(segment_row.value->>'segment_key', '')), '') = v_input_segment_id
          OR NULLIF(BTRIM(COALESCE(segment_row.value->>'segment_stable_key', '')), '') = v_input_segment_id
        )
      )
      ORDER BY CASE
        WHEN v_input_segment_stable_key IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(segment_row.value->>'segment_stable_key', '')), '') = v_input_segment_stable_key THEN 0
        WHEN v_input_segment_id IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(segment_row.value->>'segment_id', '')), '') = v_input_segment_id THEN 1
        ELSE 2
      END,
      segment_row.ordinal
      LIMIT 1;

      v_effective_segment_id := COALESCE(v_effective_segment_id, v_input_segment_id);
      v_effective_segment_stable_key := COALESCE(v_effective_segment_stable_key, v_input_segment_stable_key);

      IF v_effective_segment_stable_key IS NULL THEN
        RAISE EXCEPTION 'SNOOZE_SEGMENT_IDENTITY_UNVERIFIABLE'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'SNOOZE_SEGMENT_IDENTITY_UNVERIFIABLE',
                  'timesheet_id', v_effective_timesheet_id::text,
                  'segment_id', v_input_segment_id,
                  'segment_stable_key', v_input_segment_stable_key
                )::text;
      END IF;

      v_scope_kind := 'SEGMENT';
      v_noun := 'shift';
    END IF;
  ELSE
    IF p_timesheet_id IS NOT NULL OR v_input_segment_id IS NOT NULL OR v_input_segment_stable_key IS NOT NULL THEN
      RAISE EXCEPTION 'SNOOZE_FINANCE_SCOPE_IDENTITY_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'SNOOZE_FINANCE_SCOPE_IDENTITY_INVALID')::text;
    END IF;

    IF v_source_ref IS NULL THEN
      RAISE EXCEPTION 'SNOOZE_FINANCE_SOURCE_REF_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'SNOOZE_FINANCE_SOURCE_REF_REQUIRED')::text;
    END IF;

    IF v_source_ref !~* '^advance:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'SNOOZE_FINANCE_SOURCE_REF_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SNOOZE_FINANCE_SOURCE_REF_INVALID',
                'source_ref', v_source_ref
              )::text;
    END IF;

    v_finance_case_id := split_part(v_source_ref, ':', 2)::uuid;
    v_expected_finance_case_type := CASE v_kind
      WHEN 'OVERPAYMENT_RECOVERY' THEN 'OVERPAYMENT'::public.pay_finance_case_type_enum
      WHEN 'PAYMENT_ADVANCE_REPAYMENT' THEN 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
      WHEN 'MANUAL_DEBT_RECOVERY' THEN 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
      ELSE NULL::public.pay_finance_case_type_enum
    END;

    IF NOT EXISTS (
      SELECT 1
      FROM public.pay_advances AS finance_case
      WHERE finance_case.id = v_finance_case_id
        AND finance_case.candidate_id = p_candidate_id
        AND finance_case.case_type = v_expected_finance_case_type
    ) THEN
      RAISE EXCEPTION 'SNOOZE_FINANCE_CASE_AUTHORITY_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SNOOZE_FINANCE_CASE_AUTHORITY_MISMATCH',
                'candidate_id', p_candidate_id::text,
                'source_ref', v_source_ref,
                'snooze_kind', v_kind
              )::text;
    END IF;

    v_scope_kind := 'FINANCE_CASE';
    v_noun := 'payment';
  END IF;

  v_date_context := public.pay_banking_official_date_context_v1(NULL::timestamptz);
  v_london_current_date := (v_date_context->>'business_date')::date;
  v_next_official_pay_date := (v_date_context->>'next_official_pay_date')::date;
  v_date_context_fingerprint := v_date_context->>'configuration_fingerprint';

  IF p_snooze_until_date IS NOT NULL AND p_snooze_until_date < v_london_current_date THEN
    RAISE EXCEPTION 'SNOOZE_DATE_IN_PAST'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'SNOOZE_DATE_IN_PAST',
              'london_current_date', v_london_current_date::text,
              'snooze_until_date', p_snooze_until_date::text
            )::text;
  END IF;

  IF v_is_payment_scope THEN
    SELECT COUNT(*)::integer,
           CASE WHEN COUNT(*) > 0 THEN true ELSE false END,
           CASE
             WHEN COUNT(*) = 0 THEN NULL::text
             ELSE md5(string_agg(
               resolution_row.id::text
               || ':' || resolution_row.resolution_identity_key
               || ':' || COALESCE(resolution_row.updated_at_utc::text, ''),
               '|' ORDER BY resolution_row.id::text
             ))
           END
    INTO v_resolved_component_count,
         v_has_resolved_rate,
         v_resolution_fingerprint
    FROM public.banking_pay_workbench_session_case_resolutions AS resolution_row
    JOIN public.banking_pay_workbench_sessions AS resolution_session
      ON resolution_session.id = resolution_row.session_id
    WHERE resolution_row.candidate_id = p_candidate_id
      AND resolution_row.timesheet_id = v_effective_timesheet_id
      AND UPPER(BTRIM(COALESCE(resolution_row.resolution_family, ''))) IN ('BUCKETED', 'NON_BUCKET')
      AND resolution_session.status = 'OPEN'
      AND resolution_session.discarded_at_utc IS NULL
      AND resolution_session.replacement_session_id IS NULL
      AND (
        v_scope_kind = 'TIMESHEET'
        OR (
          v_scope_kind = 'SEGMENT'
          AND (
            NULLIF(BTRIM(COALESCE(resolution_row.payload_json->>'segment_stable_key', '')), '') = v_effective_segment_stable_key
            OR NULLIF(BTRIM(COALESCE(resolution_row.payload_json#>>'{source_basis_json,segment_stable_key}', '')), '') = v_effective_segment_stable_key
            OR NULLIF(BTRIM(COALESCE(resolution_row.payload_json->>'segment_id', '')), '') = v_effective_segment_id
            OR NULLIF(BTRIM(COALESCE(resolution_row.payload_json#>>'{source_basis_json,segment_id}', '')), '') = v_effective_segment_id
          )
        )
      );
  END IF;

  IF v_phase = 'PRE_OPEN' AND v_has_resolved_rate THEN
    v_warning_required := true;
    v_warning_code := 'RATE_RESOLUTION_REQUIRES_REVIEW_AFTER_UNSNOOZE';
    v_warning_message := CASE
      WHEN v_scope_kind = 'SEGMENT' THEN
        'This shift has a rate resolved to it. When you unsnooze it, you will need to resolve this rate again if it is unsnoozed in a different pay week to this one.'
      ELSE
        'This timesheet has a rate resolved to it. When you unsnooze it, you will need to resolve this rate again if it is unsnoozed in a different pay week to this one.'
    END;
  ELSIF v_phase = 'PRE_SAVE' AND p_snooze_until_date IS NOT NULL THEN
    IF p_snooze_until_date < v_next_official_pay_date THEN
      v_warning_required := true;
      v_warning_code := 'SNOOZE_DATE_BEFORE_NEXT_PAY_RUN';
      v_date_relation := 'BEFORE_NEXT_OFFICIAL_PAY_DATE';
      v_warning_message := 'This snooze date is before the next official pay run. Your payment will be snoozed but it will be eligible to pay in the next pay run once that date has passed.';
    ELSE
      v_warning_required := true;
      v_warning_code := 'SNOOZE_INCLUDES_NEXT_PAY_RUN';
      v_date_relation := CASE
        WHEN p_snooze_until_date = v_next_official_pay_date THEN 'EQUAL_TO_NEXT_OFFICIAL_PAY_DATE'
        ELSE 'AFTER_NEXT_OFFICIAL_PAY_DATE'
      END;
      v_warning_message := 'This snooze includes the next official pay run date, so this payment will not be eligible for that pay run. It may become eligible after the snooze date has passed, subject to the usual payment checks.';
    END IF;
  END IF;

  IF v_phase = 'PRE_SAVE' AND p_snooze_until_date IS NULL THEN
    v_date_relation := CASE WHEN p_snooze_until_date IS NULL THEN 'INDEFINITE' ELSE 'NOT_APPLICABLE' END;
  END IF;

  v_scope_fingerprint := md5(concat_ws('|',
    p_actor_user_id::text,
    p_candidate_id::text,
    COALESCE(v_effective_timesheet_id::text, ''),
    COALESCE(v_effective_segment_id, ''),
    COALESCE(v_effective_segment_stable_key, ''),
    COALESCE(v_source_ref, ''),
    v_kind,
    CASE WHEN v_phase = 'PRE_SAVE' THEN COALESCE(p_snooze_until_date::text, '') ELSE '' END,
    v_phase,
    COALESCE(v_warning_code, ''),
    COALESCE(v_resolution_fingerprint, ''),
    COALESCE(v_date_context_fingerprint, '')
  ));

  IF v_warning_required AND v_issue_acknowledgement THEN
    v_acknowledgement_token := gen_random_uuid();
    v_acknowledgement_expires_at_utc := v_now + interval '30 minutes';

    WITH expired_ack_ids AS (
      SELECT expired_ack.id
      FROM public.pay_snooze_warning_acknowledgements AS expired_ack
      WHERE expired_ack.expires_at_utc < v_now - interval '1 day'
      ORDER BY expired_ack.expires_at_utc, expired_ack.id
      LIMIT 100
    )
    DELETE FROM public.pay_snooze_warning_acknowledgements AS expired_ack
    USING expired_ack_ids
    WHERE expired_ack.id = expired_ack_ids.id;

    INSERT INTO public.pay_snooze_warning_acknowledgements (
      token_hash,
      actor_user_id,
      validation_phase,
      candidate_id,
      timesheet_id,
      segment_id,
      segment_stable_key,
      source_ref,
      snooze_kind,
      snooze_until_date,
      warning_code,
      scope_fingerprint,
      resolution_fingerprint,
      date_context_fingerprint,
      created_at_utc,
      expires_at_utc
    )
    VALUES (
      md5(v_acknowledgement_token::text),
      p_actor_user_id,
      v_phase,
      p_candidate_id,
      v_effective_timesheet_id,
      v_effective_segment_id,
      v_effective_segment_stable_key,
      v_source_ref,
      v_kind,
      p_snooze_until_date,
      v_warning_code,
      v_scope_fingerprint,
      v_resolution_fingerprint,
      v_date_context_fingerprint,
      v_now,
      v_acknowledgement_expires_at_utc
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'validation_phase', v_phase,
    'warning_required', v_warning_required,
    'warning_code', v_warning_code,
    'warning_message', v_warning_message,
    'buttons', CASE WHEN v_warning_required THEN jsonb_build_array('Cancel', 'OK') ELSE '[]'::jsonb END,
    'acknowledgement_token', CASE WHEN v_acknowledgement_token IS NULL THEN NULL ELSE v_acknowledgement_token::text END,
    'acknowledgement_expires_at_utc', v_acknowledgement_expires_at_utc,
    'scope_fingerprint', v_scope_fingerprint,
    'candidate_id', p_candidate_id::text,
    'timesheet_id', CASE WHEN v_effective_timesheet_id IS NULL THEN NULL ELSE v_effective_timesheet_id::text END,
    'booking_id', v_booking_id,
    'segment_id', v_effective_segment_id,
    'segment_stable_key', v_effective_segment_stable_key,
    'source_ref', v_source_ref,
    'scope_kind', v_scope_kind,
    'noun', v_noun,
    'expense_code', v_expense_code,
    'expense_label', v_expense_label,
    'expense_item_type', v_expense_item_type,
    'expense_source_basis_fingerprint', v_expense_source_basis_fingerprint,
    'expense_source_basis_json', v_expense_source_basis_json,
    'expense_amount_ex_vat', v_expense_amount_ex_vat,
    'expense_amount_display', v_expense_amount_display,
    'expense_source_charge_ex_vat', v_expense_source_charge_ex_vat,
    'snooze_kind', v_kind,
    'snooze_until_date', CASE WHEN p_snooze_until_date IS NULL THEN NULL ELSE p_snooze_until_date::text END,
    'london_current_date', v_london_current_date::text,
    'current_official_pay_date', v_date_context->>'current_official_pay_date',
    'next_official_pay_date', v_next_official_pay_date::text,
    'date_relation_to_next_pay_run', v_date_relation,
    'requires_before_next_run_warning', v_warning_code = 'SNOOZE_DATE_BEFORE_NEXT_PAY_RUN',
    'requires_includes_next_run_warning', v_warning_code = 'SNOOZE_INCLUDES_NEXT_PAY_RUN',
    'has_resolved_rate', v_has_resolved_rate,
    'resolved_rate_component_count', v_resolved_component_count,
    'resolution_fingerprint', v_resolution_fingerprint,
    'date_context_configuration_fingerprint', v_date_context_fingerprint
  );
END;
$function$;
