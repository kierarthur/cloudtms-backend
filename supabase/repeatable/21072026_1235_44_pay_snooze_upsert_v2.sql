-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 73059e76f239.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.pay_snooze_upsert_v2(p_candidate_id uuid, p_timesheet_id uuid, p_segment_id text, p_source_ref text, p_snooze_kind text DEFAULT 'DO_NOT_PAY'::text, p_snooze_until_date date DEFAULT NULL::date, p_actor_user_id uuid DEFAULT NULL::uuid, p_note text DEFAULT NULL::text, p_segment_stable_key text DEFAULT NULL::text, p_resolved_rate_acknowledgement_token text DEFAULT NULL::text, p_pay_run_acknowledgement_token text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_date_context jsonb := '{}'::jsonb;
  v_today_uk date;
  v_next_official_pay_date date;
  v_validation_pre_open jsonb := '{}'::jsonb;
  v_validation_pre_save jsonb := '{}'::jsonb;
  v_post_lock_validation jsonb := '{}'::jsonb;
  v_required_scope_fingerprint text := NULL::text;
  v_scope_kind text := NULL::text;
  v_expense_code text := NULL::text;
  v_expense_source_basis_fingerprint text := NULL::text;
  v_expense_source_prefix text := NULL::text;
  v_stale_expense_snoozes_before jsonb := '[]'::jsonb;
  v_stale_expense_snoozes_cancelled integer := 0;
  v_acknowledgement_row public.pay_snooze_warning_acknowledgements%ROWTYPE;
  v_segment_stable_key_hint text := NULLIF(BTRIM(COALESCE(p_segment_stable_key, '')), '');
  v_kind_input text := upper(btrim(coalesce(p_snooze_kind, 'DO_NOT_PAY')));
  v_kind text;
  v_segment_id_input text := nullif(btrim(coalesce(p_segment_id, '')), '');
  v_source_ref text := lower(nullif(btrim(coalesce(p_source_ref, '')), ''));
  v_note text := nullif(btrim(coalesce(p_note, '')), '');

  v_input_booking_id text := NULL;
  v_booking_id text := NULL;
  v_current_timesheet_id uuid := NULL;
  v_effective_timesheet_id uuid := NULL;

  v_input_invoice_breakdown_json jsonb := NULL;
  v_current_invoice_breakdown_json jsonb := NULL;

  v_input_segment_stable_key text := NULL;
  v_segment_stable_key text := NULL;
  v_current_segment_id text := NULL;
  v_effective_segment_id text := NULL;

  v_keeper_id uuid := NULL;
  v_action text := NULL;

  v_before jsonb := NULL;
  v_after jsonb := NULL;

  v_finance_case_id uuid := NULL;
  v_event_type text := NULL;
  v_conflict_snooze_id uuid := NULL;

  v_finance_case_record public.pay_advances%rowtype;
  v_expected_finance_case_type public.pay_finance_case_type_enum;
  v_requested_week_start_date date := NULL;
  v_target_week_start date := NULL;
  v_next_due_week_start_before_snooze_date date := NULL;
  v_next_due_week_start_after_snooze_date date := NULL;

  v_schedule_rebase_result jsonb := NULL;
  v_schedule_before_snooze_json jsonb := NULL;
  v_next_due_week_start_before_snooze text := NULL;
  v_requested_week_start text := NULL;
  v_schedule_after_snooze_json jsonb := NULL;
  v_next_due_week_start_after_snooze text := NULL;
  v_installment_count integer := NULL;
BEGIN
  declare v_correction_chain jsonb;
  begin
    if p_timesheet_id is not null and coalesce((public._ctms_import_correction_classify_v1(p_timesheet_id)->>'is_import_authoritative_correction')::boolean,false) then
      v_correction_chain:=public.timesheet_correction_chain_scope_v1(p_timesheet_id,false,32,100);
      if coalesce((v_correction_chain->>'valid')::boolean,false) is not true then raise exception 'CORRECTION_CHAIN_INVALID_FOR_SNOOZE' using errcode='P0001',detail=v_correction_chain::text; end if;
      v_segment_stable_key_hint:='correction-chain:'||(v_correction_chain->>'root_timesheet_id');
      v_source_ref:=v_segment_stable_key_hint;
    end if;
  end;
  v_date_context := public.pay_banking_official_date_context_v1(NULL::timestamptz);
  v_today_uk := (v_date_context->>'business_date')::date;
  v_next_official_pay_date := (v_date_context->>'next_official_pay_date')::date;

  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
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
    ELSE NULL
  END;

  IF v_kind IS NULL THEN
    RAISE EXCEPTION 'invalid snooze_kind';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'SNOOZE_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'SNOOZE_ACTOR_REQUIRED')::text;
  END IF;

  v_validation_pre_open := public.pay_snooze_validate_request_v1(
    p_actor_user_id => p_actor_user_id,
    p_candidate_id => p_candidate_id,
    p_timesheet_id => p_timesheet_id,
    p_segment_id => p_segment_id,
    p_segment_stable_key => v_segment_stable_key_hint,
    p_source_ref => v_source_ref,
    p_snooze_kind => v_kind,
    p_snooze_until_date => p_snooze_until_date,
    p_validation_phase => 'INTERNAL_PRE_OPEN'
  );

  IF COALESCE((v_validation_pre_open->>'warning_required')::boolean, false) THEN
    v_required_scope_fingerprint := NULLIF(BTRIM(COALESCE(v_validation_pre_open->>'scope_fingerprint', '')), '');
    IF NULLIF(BTRIM(COALESCE(p_resolved_rate_acknowledgement_token, '')), '') IS NULL
       OR NULLIF(BTRIM(COALESCE(p_resolved_rate_acknowledgement_token, '')), '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'SNOOZE_CONFIRMATION_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = (v_validation_pre_open || jsonb_build_object(
                'code', 'SNOOZE_CONFIRMATION_REQUIRED',
                'confirmation_type', 'RESOLVED_RATE',
                'refresh_required', false
              ))::text;
    END IF;

    SELECT ack_row.*
    INTO v_acknowledgement_row
    FROM public.pay_snooze_warning_acknowledgements AS ack_row
    WHERE ack_row.token_hash = md5(BTRIM(p_resolved_rate_acknowledgement_token))
      AND ack_row.actor_user_id = p_actor_user_id
      AND ack_row.validation_phase = 'PRE_OPEN'
      AND ack_row.candidate_id = p_candidate_id
      AND ack_row.source_ref IS NOT DISTINCT FROM v_source_ref
      AND ack_row.warning_code = 'RATE_RESOLUTION_REQUIRES_REVIEW_AFTER_UNSNOOZE'
      AND ack_row.scope_fingerprint = v_required_scope_fingerprint
      AND ack_row.expires_at_utc > clock_timestamp()
      AND ack_row.last_used_at_utc IS NULL
    FOR UPDATE;

    IF v_acknowledgement_row.id IS NULL THEN
      RAISE EXCEPTION 'SNOOZE_CONFIRMATION_STALE'
        USING ERRCODE = 'P0001',
              DETAIL = (v_validation_pre_open || jsonb_build_object(
                'code', 'SNOOZE_CONFIRMATION_STALE',
                'confirmation_type', 'RESOLVED_RATE',
                'refresh_required', true
              ))::text;
    END IF;

    UPDATE public.pay_snooze_warning_acknowledgements AS ack_update
    SET last_used_at_utc = clock_timestamp()
    WHERE ack_update.id = v_acknowledgement_row.id
      AND ack_update.last_used_at_utc IS NULL;
  END IF;

  v_acknowledgement_row := NULL;
  v_validation_pre_save := public.pay_snooze_validate_request_v1(
    p_actor_user_id => p_actor_user_id,
    p_candidate_id => p_candidate_id,
    p_timesheet_id => p_timesheet_id,
    p_segment_id => p_segment_id,
    p_segment_stable_key => v_segment_stable_key_hint,
    p_source_ref => v_source_ref,
    p_snooze_kind => v_kind,
    p_snooze_until_date => p_snooze_until_date,
    p_validation_phase => 'INTERNAL_PRE_SAVE'
  );

  v_scope_kind := UPPER(NULLIF(BTRIM(COALESCE(v_validation_pre_save->>'scope_kind', '')), ''));
  v_expense_code := UPPER(NULLIF(BTRIM(COALESCE(v_validation_pre_save->>'expense_code', '')), ''));
  v_expense_source_basis_fingerprint := LOWER(NULLIF(BTRIM(COALESCE(v_validation_pre_save->>'expense_source_basis_fingerprint', '')), ''));

  IF COALESCE((v_validation_pre_save->>'warning_required')::boolean, false) THEN
    v_required_scope_fingerprint := NULLIF(BTRIM(COALESCE(v_validation_pre_save->>'scope_fingerprint', '')), '');
    IF NULLIF(BTRIM(COALESCE(p_pay_run_acknowledgement_token, '')), '') IS NULL
       OR NULLIF(BTRIM(COALESCE(p_pay_run_acknowledgement_token, '')), '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'SNOOZE_CONFIRMATION_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = (v_validation_pre_save || jsonb_build_object(
                'code', 'SNOOZE_CONFIRMATION_REQUIRED',
                'confirmation_type', 'PAY_RUN_DATE',
                'refresh_required', false
              ))::text;
    END IF;

    SELECT ack_row.*
    INTO v_acknowledgement_row
    FROM public.pay_snooze_warning_acknowledgements AS ack_row
    WHERE ack_row.token_hash = md5(BTRIM(p_pay_run_acknowledgement_token))
      AND ack_row.actor_user_id = p_actor_user_id
      AND ack_row.validation_phase = 'PRE_SAVE'
      AND ack_row.candidate_id = p_candidate_id
      AND ack_row.source_ref IS NOT DISTINCT FROM v_source_ref
      AND ack_row.warning_code = NULLIF(BTRIM(COALESCE(v_validation_pre_save->>'warning_code', '')), '')
      AND ack_row.scope_fingerprint = v_required_scope_fingerprint
      AND ack_row.expires_at_utc > clock_timestamp()
      AND ack_row.last_used_at_utc IS NULL
    FOR UPDATE;

    IF v_acknowledgement_row.id IS NULL THEN
      RAISE EXCEPTION 'SNOOZE_CONFIRMATION_STALE'
        USING ERRCODE = 'P0001',
              DETAIL = (v_validation_pre_save || jsonb_build_object(
                'code', 'SNOOZE_CONFIRMATION_STALE',
                'confirmation_type', 'PAY_RUN_DATE',
                'refresh_required', true
              ))::text;
    END IF;

    UPDATE public.pay_snooze_warning_acknowledgements AS ack_update
    SET last_used_at_utc = clock_timestamp()
    WHERE ack_update.id = v_acknowledgement_row.id
      AND ack_update.last_used_at_utc IS NULL;
  END IF;

  IF p_snooze_until_date IS NOT NULL AND p_snooze_until_date < v_today_uk THEN
    RAISE EXCEPTION 'snooze_until_date must be today or later in Europe/London (or NULL for indefinite)'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'SNOOZE_DATE_IN_PAST',
              'london_current_date', v_today_uk::text,
              'snooze_until_date', p_snooze_until_date::text
            )::text;
  END IF;

  -- Candidate-scoped mutation lock.  The draft guard uses the same lock so a
  -- snooze and a stale Create Draft request cannot pass one another.
  PERFORM pg_advisory_xact_lock(
    pg_catalog.hashtext('public.banking_pay_candidate_snooze_guard'),
    pg_catalog.hashtext(p_candidate_id::text)
  );

  v_post_lock_validation := public.pay_snooze_validate_request_v1(
    p_actor_user_id => p_actor_user_id,
    p_candidate_id => p_candidate_id,
    p_timesheet_id => p_timesheet_id,
    p_segment_id => p_segment_id,
    p_segment_stable_key => v_segment_stable_key_hint,
    p_source_ref => v_source_ref,
    p_snooze_kind => v_kind,
    p_snooze_until_date => p_snooze_until_date,
    p_validation_phase => 'INTERNAL_PRE_SAVE'
  );

  IF NULLIF(BTRIM(COALESCE(v_post_lock_validation->>'scope_fingerprint', '')), '')
       IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_validation_pre_save->>'scope_fingerprint', '')), '')
     OR UPPER(NULLIF(BTRIM(COALESCE(v_post_lock_validation->>'scope_kind', '')), ''))
       IS DISTINCT FROM v_scope_kind
     OR LOWER(NULLIF(BTRIM(COALESCE(v_post_lock_validation->>'source_ref', '')), ''))
       IS DISTINCT FROM v_source_ref THEN
    RAISE EXCEPTION 'SNOOZE_AUTHORITY_CHANGED_DURING_SAVE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'SNOOZE_AUTHORITY_CHANGED_DURING_SAVE',
              'candidate_id', p_candidate_id::text,
              'timesheet_id', CASE WHEN p_timesheet_id IS NULL THEN NULL ELSE p_timesheet_id::text END,
              'source_ref', v_source_ref,
              'refresh_required', true
            )::text;
  END IF;

  v_scope_kind := UPPER(NULLIF(BTRIM(COALESCE(v_post_lock_validation->>'scope_kind', '')), ''));
  v_expense_code := UPPER(NULLIF(BTRIM(COALESCE(v_post_lock_validation->>'expense_code', '')), ''));
  v_expense_source_basis_fingerprint := LOWER(NULLIF(BTRIM(COALESCE(v_post_lock_validation->>'expense_source_basis_fingerprint', '')), ''));

  IF v_scope_kind = 'TIMESHEET_EXPENSE' THEN
    IF v_kind <> 'DO_NOT_PAY'
       OR v_source_ref IS NULL
       OR p_timesheet_id IS NULL
       OR v_segment_id_input IS NOT NULL
       OR v_segment_stable_key_hint IS NOT NULL THEN
      RAISE EXCEPTION 'SNOOZE_EXPENSE_SCOPE_IDENTITY_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SNOOZE_EXPENSE_SCOPE_IDENTITY_INVALID',
                'source_ref', v_source_ref,
                'snooze_kind', v_kind
              )::text;
    END IF;
  ELSIF v_kind IN ('OVERPAYMENT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','MANUAL_DEBT_RECOVERY') THEN
    IF v_source_ref IS NULL THEN
      RAISE EXCEPTION 'source_ref is required for finance-case snoozes';
    END IF;
    IF p_timesheet_id IS NOT NULL OR v_segment_id_input IS NOT NULL THEN
      RAISE EXCEPTION 'timesheet_id/segment_id must not be supplied for finance-case snoozes';
    END IF;
  ELSIF v_kind = 'TIMESHEET_PAYMENT' THEN
    IF p_timesheet_id IS NULL THEN
      RAISE EXCEPTION 'timesheet_id is required for TIMESHEET_PAYMENT snoozes';
    END IF;
    IF v_segment_id_input IS NOT NULL THEN
      RAISE EXCEPTION 'segment_id must be null for TIMESHEET_PAYMENT snoozes';
    END IF;
    IF v_source_ref IS NOT NULL THEN
      RAISE EXCEPTION 'source_ref must be null for TIMESHEET_PAYMENT snoozes';
    END IF;
  ELSE
    IF p_timesheet_id IS NULL THEN
      RAISE EXCEPTION 'timesheet_id is required for timesheet-line snoozes';
    END IF;
    IF v_source_ref IS NOT NULL THEN
      RAISE EXCEPTION 'source_ref must be null for timesheet-line snoozes';
    END IF;
  END IF;

  IF v_source_ref IS NOT NULL
     AND v_source_ref LIKE 'advance:%'
     AND split_part(v_source_ref, ':', 2) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
  THEN
    v_finance_case_id := split_part(v_source_ref, ':', 2)::uuid;
  END IF;

  IF v_kind IN ('OVERPAYMENT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'MANUAL_DEBT_RECOVERY') THEN
    v_expected_finance_case_type := CASE v_kind
      WHEN 'OVERPAYMENT_RECOVERY' THEN 'OVERPAYMENT'::public.pay_finance_case_type_enum
      WHEN 'PAYMENT_ADVANCE_REPAYMENT' THEN 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
      WHEN 'MANUAL_DEBT_RECOVERY' THEN 'MANUAL_DEBT_ADJUSTMENT'::public.pay_finance_case_type_enum
      ELSE NULL::public.pay_finance_case_type_enum
    END;

    IF v_finance_case_id IS NULL THEN
      RAISE EXCEPTION 'SNOOZE_FINANCE_SOURCE_REF_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SNOOZE_FINANCE_SOURCE_REF_INVALID',
                'source_ref', v_source_ref
              )::text;
    END IF;

    SELECT finance_case.*
    INTO v_finance_case_record
    FROM public.pay_advances AS finance_case
    WHERE finance_case.id = v_finance_case_id
    FOR UPDATE;

    IF v_finance_case_record.id IS NULL THEN
      RAISE EXCEPTION 'SNOOZE_FINANCE_CASE_AUTHORITY_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SNOOZE_FINANCE_CASE_AUTHORITY_MISMATCH',
                'candidate_id', p_candidate_id::text,
                'source_ref', v_source_ref,
                'snooze_kind', v_kind,
                'reason', 'FINANCE_CASE_NOT_FOUND'
              )::text;
    END IF;

    IF v_finance_case_record.candidate_id IS DISTINCT FROM p_candidate_id
       OR v_finance_case_record.case_type IS DISTINCT FROM v_expected_finance_case_type THEN
      RAISE EXCEPTION 'SNOOZE_FINANCE_CASE_AUTHORITY_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SNOOZE_FINANCE_CASE_AUTHORITY_MISMATCH',
                'candidate_id', p_candidate_id::text,
                'source_ref', v_source_ref,
                'snooze_kind', v_kind,
                'resolved_candidate_id', CASE WHEN v_finance_case_record.candidate_id IS NULL THEN NULL ELSE v_finance_case_record.candidate_id::text END,
                'resolved_case_type', v_finance_case_record.case_type::text,
                'expected_case_type', v_expected_finance_case_type::text
              )::text;
    END IF;
  END IF;

  IF v_scope_kind = 'TIMESHEET_EXPENSE' THEN
    IF COALESCE(v_post_lock_validation->>'timesheet_id', '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'SNOOZE_EXPENSE_CURRENT_TIMESHEET_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SNOOZE_EXPENSE_CURRENT_TIMESHEET_INVALID',
                'source_ref', v_source_ref,
                'refresh_required', true
              )::text;
    END IF;

    v_effective_timesheet_id := (v_post_lock_validation->>'timesheet_id')::uuid;
    v_current_timesheet_id := v_effective_timesheet_id;
    v_booking_id := NULLIF(BTRIM(COALESCE(v_post_lock_validation->>'booking_id', '')), '');
    v_input_booking_id := v_booking_id;
    v_effective_segment_id := NULL::text;
    v_segment_stable_key := NULL::text;
    v_expense_source_prefix := left(v_source_ref, length(v_source_ref) - 32);

    IF v_expense_code IS NULL
       OR v_expense_source_basis_fingerprint IS NULL
       OR v_expense_source_prefix IS NULL THEN
      RAISE EXCEPTION 'SNOOZE_EXPENSE_CANONICAL_IDENTITY_INCOMPLETE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'SNOOZE_EXPENSE_CANONICAL_IDENTITY_INCOMPLETE',
                'source_ref', v_source_ref,
                'refresh_required', true
              )::text;
    END IF;
  ELSIF v_source_ref IS NULL THEN
    SELECT
      nullif(btrim(coalesce(t.booking_id, '')), '')
    INTO v_input_booking_id
    FROM public.timesheets t
    WHERE t.timesheet_id = p_timesheet_id
    LIMIT 1;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'timesheet_id is invalid or no longer exists';
    END IF;

    v_booking_id := v_input_booking_id;

    IF v_booking_id IS NOT NULL THEN
      SELECT t.timesheet_id
      INTO v_current_timesheet_id
      FROM public.timesheets t
      WHERE t.booking_id = v_booking_id
        AND t.is_current = true
      ORDER BY coalesce(t.version, 0) DESC, t.updated_at DESC NULLS LAST, t.created_at DESC NULLS LAST, t.timesheet_id DESC
      LIMIT 1;
    END IF;

    v_effective_timesheet_id := coalesce(v_current_timesheet_id, p_timesheet_id);

    SELECT tf.invoice_breakdown_json
    INTO v_input_invoice_breakdown_json
    FROM public.timesheets_financials tf
    WHERE tf.timesheet_id = p_timesheet_id
    ORDER BY CASE WHEN tf.is_current THEN 0 ELSE 1 END, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
    LIMIT 1;

    IF v_effective_timesheet_id IS NOT NULL THEN
      SELECT tf.invoice_breakdown_json
      INTO v_current_invoice_breakdown_json
      FROM public.timesheets_financials tf
      WHERE tf.timesheet_id = v_effective_timesheet_id
      ORDER BY CASE WHEN tf.is_current THEN 0 ELSE 1 END, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
      LIMIT 1;
    END IF;

    IF v_segment_id_input IS NOT NULL THEN
      SELECT
        nullif(
          btrim(
            coalesce(
              nullif(btrim(coalesce(seg.value->>'segment_stable_key', '')), ''),
              nullif(btrim(coalesce(seg.value->>'work_date', '')), ''),
              nullif(btrim(coalesce(seg.value->>'ref_num', '')), ''),
              nullif(btrim(coalesce(seg.value->>'segment_key', '')), ''),
              nullif(btrim(coalesce(seg.value->>'segment_id', '')), '')
            )
          ),
          ''
        )
      INTO v_input_segment_stable_key
      FROM jsonb_array_elements(
        CASE
          WHEN v_input_invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(v_input_invoice_breakdown_json) = 'object'
           AND jsonb_typeof(v_input_invoice_breakdown_json->'segments') = 'array'
          THEN v_input_invoice_breakdown_json->'segments'
          ELSE '[]'::jsonb
        END
      ) AS seg(value)
      WHERE jsonb_typeof(seg.value) = 'object'
        AND (
          nullif(btrim(coalesce(seg.value->>'segment_id', '')), '') = v_segment_id_input
          OR nullif(btrim(coalesce(seg.value->>'segment_stable_key', '')), '') = v_segment_id_input
          OR nullif(btrim(coalesce(seg.value->>'segment_key', '')), '') = v_segment_id_input
          OR nullif(btrim(coalesce(seg.value->>'ref_num', '')), '') = v_segment_id_input
          OR nullif(btrim(coalesce(seg.value->>'work_date', '')), '') = v_segment_id_input
        )
      ORDER BY
        CASE
          WHEN nullif(btrim(coalesce(seg.value->>'segment_id', '')), '') = v_segment_id_input THEN 1
          WHEN nullif(btrim(coalesce(seg.value->>'segment_stable_key', '')), '') = v_segment_id_input THEN 2
          WHEN nullif(btrim(coalesce(seg.value->>'segment_key', '')), '') = v_segment_id_input THEN 3
          WHEN nullif(btrim(coalesce(seg.value->>'ref_num', '')), '') = v_segment_id_input THEN 4
          WHEN nullif(btrim(coalesce(seg.value->>'work_date', '')), '') = v_segment_id_input THEN 5
          ELSE 99
        END
      LIMIT 1;

      SELECT
        nullif(btrim(coalesce(seg.value->>'segment_id', '')), ''),
        nullif(
          btrim(
            coalesce(
              nullif(btrim(coalesce(seg.value->>'segment_stable_key', '')), ''),
              nullif(btrim(coalesce(seg.value->>'work_date', '')), ''),
              nullif(btrim(coalesce(seg.value->>'ref_num', '')), ''),
              nullif(btrim(coalesce(seg.value->>'segment_key', '')), ''),
              nullif(btrim(coalesce(seg.value->>'segment_id', '')), '')
            )
          ),
          ''
        )
      INTO v_current_segment_id, v_segment_stable_key
      FROM jsonb_array_elements(
        CASE
          WHEN v_current_invoice_breakdown_json IS NOT NULL
           AND jsonb_typeof(v_current_invoice_breakdown_json) = 'object'
           AND jsonb_typeof(v_current_invoice_breakdown_json->'segments') = 'array'
          THEN v_current_invoice_breakdown_json->'segments'
          ELSE '[]'::jsonb
        END
      ) AS seg(value)
      WHERE jsonb_typeof(seg.value) = 'object'
        AND (
          (
            v_input_segment_stable_key IS NOT NULL
            AND nullif(
                  btrim(
                    coalesce(
                      nullif(btrim(coalesce(seg.value->>'segment_stable_key', '')), ''),
                      nullif(btrim(coalesce(seg.value->>'work_date', '')), ''),
                      nullif(btrim(coalesce(seg.value->>'ref_num', '')), ''),
                      nullif(btrim(coalesce(seg.value->>'segment_key', '')), ''),
                      nullif(btrim(coalesce(seg.value->>'segment_id', '')), '')
                    )
                  ),
                  ''
                ) = v_input_segment_stable_key
          )
          OR nullif(btrim(coalesce(seg.value->>'segment_id', '')), '') = v_segment_id_input
          OR nullif(btrim(coalesce(seg.value->>'segment_stable_key', '')), '') = v_segment_id_input
          OR nullif(btrim(coalesce(seg.value->>'segment_key', '')), '') = v_segment_id_input
          OR nullif(btrim(coalesce(seg.value->>'ref_num', '')), '') = v_segment_id_input
          OR nullif(btrim(coalesce(seg.value->>'work_date', '')), '') = v_segment_id_input
        )
      ORDER BY
        CASE
          WHEN v_input_segment_stable_key IS NOT NULL
           AND nullif(
                 btrim(
                   coalesce(
                     nullif(btrim(coalesce(seg.value->>'segment_stable_key', '')), ''),
                     nullif(btrim(coalesce(seg.value->>'work_date', '')), ''),
                     nullif(btrim(coalesce(seg.value->>'ref_num', '')), ''),
                     nullif(btrim(coalesce(seg.value->>'segment_key', '')), ''),
                     nullif(btrim(coalesce(seg.value->>'segment_id', '')), '')
                   )
                 ),
                 ''
               ) = v_input_segment_stable_key THEN 1
          WHEN nullif(btrim(coalesce(seg.value->>'segment_id', '')), '') = v_segment_id_input THEN 2
          WHEN nullif(btrim(coalesce(seg.value->>'segment_stable_key', '')), '') = v_segment_id_input THEN 3
          WHEN nullif(btrim(coalesce(seg.value->>'segment_key', '')), '') = v_segment_id_input THEN 4
          WHEN nullif(btrim(coalesce(seg.value->>'ref_num', '')), '') = v_segment_id_input THEN 5
          WHEN nullif(btrim(coalesce(seg.value->>'work_date', '')), '') = v_segment_id_input THEN 6
          ELSE 99
        END
      LIMIT 1;

      v_segment_stable_key := coalesce(v_segment_stable_key, v_input_segment_stable_key);
      v_effective_segment_id := v_current_segment_id;

      IF v_segment_stable_key IS NULL THEN
        RAISE EXCEPTION 'segment_id could not be resolved on the timesheet';
      END IF;
    ELSE
      v_segment_stable_key := NULL;
      v_effective_segment_id := NULL;
    END IF;
  END IF;

  IF v_scope_kind = 'TIMESHEET_EXPENSE' THEN
    SELECT COALESCE(
             jsonb_agg(
               jsonb_build_object(
                 'id', stale_snooze.id::text,
                 'source_ref', stale_snooze.source_ref,
                 'snooze_until_date', CASE WHEN stale_snooze.snooze_until_date IS NULL THEN NULL ELSE stale_snooze.snooze_until_date::text END,
                 'note', stale_snooze.note
               )
               ORDER BY stale_snooze.created_at_utc, stale_snooze.id
             ),
             '[]'::jsonb
           )
    INTO v_stale_expense_snoozes_before
    FROM public.pay_item_snoozes AS stale_snooze
    WHERE stale_snooze.candidate_id = p_candidate_id
      AND stale_snooze.timesheet_id = v_effective_timesheet_id
      AND stale_snooze.source_ref LIKE v_expense_source_prefix || '%'
      AND stale_snooze.source_ref IS DISTINCT FROM v_source_ref
      AND stale_snooze.cleared_at_utc IS NULL
      AND stale_snooze.cancelled_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(stale_snooze.snooze_kind, ''))) = 'DO_NOT_PAY';

    UPDATE public.pay_item_snoozes AS stale_snooze
    SET cancelled_at_utc = now(),
        cancelled_by_user_id = p_actor_user_id,
        cancel_reason = 'SUPERSEDED_BY_CURRENT_EXPENSE_SOURCE',
        updated_at_utc = now(),
        updated_by_user_id = p_actor_user_id
    WHERE stale_snooze.candidate_id = p_candidate_id
      AND stale_snooze.timesheet_id = v_effective_timesheet_id
      AND stale_snooze.source_ref LIKE v_expense_source_prefix || '%'
      AND stale_snooze.source_ref IS DISTINCT FROM v_source_ref
      AND stale_snooze.cleared_at_utc IS NULL
      AND stale_snooze.cancelled_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(stale_snooze.snooze_kind, ''))) = 'DO_NOT_PAY';

    GET DIAGNOSTICS v_stale_expense_snoozes_cancelled = ROW_COUNT;

    IF COALESCE(v_stale_expense_snoozes_cancelled, 0) > 0 THEN
      PERFORM public._audit_insert(
        'timesheets',
        v_effective_timesheet_id::text,
        'SNOOZE_STALE_EXPENSE_IDENTITIES_CANCELLED',
        v_stale_expense_snoozes_before,
        jsonb_build_object(
          'current_source_ref', v_source_ref,
          'expense_code', v_expense_code,
          'cancelled_count', v_stale_expense_snoozes_cancelled,
          'cancel_reason', 'SUPERSEDED_BY_CURRENT_EXPENSE_SOURCE'
        ),
        'SUPERSEDED_BY_CURRENT_EXPENSE_SOURCE',
        p_actor_user_id
      );
    END IF;
  END IF;

  IF v_source_ref IS NULL THEN
    IF v_kind = 'TIMESHEET_PAYMENT' THEN
      SELECT s.id
      INTO v_conflict_snooze_id
      FROM public.pay_item_snoozes s
      WHERE s.candidate_id = p_candidate_id
        AND s.source_ref IS NULL
        AND s.cleared_at_utc IS NULL
    AND s.cancelled_at_utc IS NULL
        AND upper(coalesce(s.snooze_kind, '')) IN ('DO_NOT_PAY', 'BLOCKED_TIMESHEET')
        AND coalesce(
              nullif(btrim(coalesce(s.segment_stable_key, '')), ''),
              nullif(btrim(coalesce(s.segment_id, '')), '')
            ) IS NOT NULL
        AND (
          (
            v_booking_id IS NOT NULL
            AND (
              s.booking_id IS NOT DISTINCT FROM v_booking_id
              OR (
                s.booking_id IS NULL
                AND (
                  s.timesheet_id IS NOT DISTINCT FROM p_timesheet_id
                  OR s.timesheet_id IS NOT DISTINCT FROM v_effective_timesheet_id
                )
              )
            )
          )
          OR
          (
            v_booking_id IS NULL
            AND (
              s.timesheet_id IS NOT DISTINCT FROM p_timesheet_id
              OR s.timesheet_id IS NOT DISTINCT FROM v_effective_timesheet_id
            )
          )
        )
      ORDER BY s.updated_at_utc DESC NULLS LAST, s.created_at_utc DESC, s.id DESC
      LIMIT 1;

      IF v_conflict_snooze_id IS NOT NULL THEN
        RAISE EXCEPTION '%', 'SEGMENT_SNOOZES_ALREADY_EXIST'
          USING DETAIL = jsonb_build_object(
            'error_code', 'SEGMENT_SNOOZES_ALREADY_EXIST',
            'candidate_id', p_candidate_id::text,
            'timesheet_id', coalesce(v_effective_timesheet_id, p_timesheet_id)::text,
            'booking_id', v_booking_id,
            'conflict_snooze_id', v_conflict_snooze_id::text
          )::text;
      END IF;
    ELSIF coalesce(v_effective_segment_id, v_segment_stable_key, v_segment_id_input) IS NOT NULL THEN
      SELECT s.id
      INTO v_conflict_snooze_id
      FROM public.pay_item_snoozes s
      WHERE s.candidate_id = p_candidate_id
        AND s.source_ref IS NULL
        AND s.cleared_at_utc IS NULL
    AND s.cancelled_at_utc IS NULL
        AND upper(coalesce(s.snooze_kind, '')) = 'TIMESHEET_PAYMENT'
        AND s.segment_id IS NULL
        AND s.segment_stable_key IS NULL
        AND (
          (
            v_booking_id IS NOT NULL
            AND (
              s.booking_id IS NOT DISTINCT FROM v_booking_id
              OR (
                s.booking_id IS NULL
                AND (
                  s.timesheet_id IS NOT DISTINCT FROM p_timesheet_id
                  OR s.timesheet_id IS NOT DISTINCT FROM v_effective_timesheet_id
                )
              )
            )
          )
          OR
          (
            v_booking_id IS NULL
            AND (
              s.timesheet_id IS NOT DISTINCT FROM p_timesheet_id
              OR s.timesheet_id IS NOT DISTINCT FROM v_effective_timesheet_id
            )
          )
        )
      ORDER BY s.updated_at_utc DESC NULLS LAST, s.created_at_utc DESC, s.id DESC
      LIMIT 1;

      IF v_conflict_snooze_id IS NOT NULL THEN
        RAISE EXCEPTION '%', 'WHOLE_TIMESHEET_ALREADY_SNOOZED'
          USING DETAIL = jsonb_build_object(
            'error_code', 'WHOLE_TIMESHEET_ALREADY_SNOOZED',
            'candidate_id', p_candidate_id::text,
            'timesheet_id', coalesce(v_effective_timesheet_id, p_timesheet_id)::text,
            'booking_id', v_booking_id,
            'segment_id', coalesce(v_effective_segment_id, v_segment_id_input),
            'segment_stable_key', v_segment_stable_key,
            'conflict_snooze_id', v_conflict_snooze_id::text
          )::text;
      END IF;
    END IF;
  END IF;

  SELECT jsonb_build_object(
           'id', s.id::text,
           'candidate_id', s.candidate_id::text,
           'timesheet_id', CASE WHEN s.timesheet_id IS NULL THEN NULL ELSE s.timesheet_id::text END,
           'booking_id', s.booking_id,
           'segment_id', s.segment_id,
           'segment_stable_key', s.segment_stable_key,
           'source_ref', s.source_ref,
           'snooze_kind', s.snooze_kind,
           'snooze_until_date', CASE WHEN s.snooze_until_date IS NULL THEN NULL ELSE s.snooze_until_date::text END,
           'note', s.note
         )
  INTO v_before
  FROM public.pay_item_snoozes s
  WHERE s.candidate_id = p_candidate_id
    AND s.cleared_at_utc IS NULL
    AND s.cancelled_at_utc IS NULL
    AND (
      (v_source_ref IS NOT NULL AND s.source_ref IS NOT DISTINCT FROM v_source_ref)
      OR
      (
        v_source_ref IS NULL
        AND s.source_ref IS NULL
        AND (
          (
            v_booking_id IS NOT NULL
            AND s.booking_id IS NOT DISTINCT FROM v_booking_id
            AND coalesce(s.segment_stable_key, '') IS NOT DISTINCT FROM coalesce(v_segment_stable_key, '')
          )
          OR
          (
            s.booking_id IS NULL
            AND (
              s.timesheet_id IS NOT DISTINCT FROM p_timesheet_id
              OR s.timesheet_id IS NOT DISTINCT FROM v_effective_timesheet_id
            )
            AND s.segment_id IS NOT DISTINCT FROM coalesce(v_effective_segment_id, v_segment_id_input)
          )
        )
      )
    )
  ORDER BY s.updated_at_utc DESC NULLS LAST, s.created_at_utc DESC, s.id DESC
  LIMIT 1;

  SELECT s.id
  INTO v_keeper_id
  FROM public.pay_item_snoozes s
  WHERE s.candidate_id = p_candidate_id
    AND s.cleared_at_utc IS NULL
    AND s.cancelled_at_utc IS NULL
    AND (
      (v_source_ref IS NOT NULL AND s.source_ref IS NOT DISTINCT FROM v_source_ref)
      OR
      (
        v_source_ref IS NULL
        AND s.source_ref IS NULL
        AND (
          (
            v_booking_id IS NOT NULL
            AND s.booking_id IS NOT DISTINCT FROM v_booking_id
            AND coalesce(s.segment_stable_key, '') IS NOT DISTINCT FROM coalesce(v_segment_stable_key, '')
          )
          OR
          (
            s.booking_id IS NULL
            AND (
              s.timesheet_id IS NOT DISTINCT FROM p_timesheet_id
              OR s.timesheet_id IS NOT DISTINCT FROM v_effective_timesheet_id
            )
            AND s.segment_id IS NOT DISTINCT FROM coalesce(v_effective_segment_id, v_segment_id_input)
          )
        )
      )
    )
  ORDER BY s.updated_at_utc DESC NULLS LAST, s.created_at_utc DESC, s.id DESC
  LIMIT 1;

  IF v_scope_kind = 'TIMESHEET_EXPENSE' AND v_keeper_id IS NULL THEN
    SELECT s.id,
           jsonb_build_object(
             'id', s.id::text,
             'candidate_id', s.candidate_id::text,
             'timesheet_id', CASE WHEN s.timesheet_id IS NULL THEN NULL ELSE s.timesheet_id::text END,
             'booking_id', s.booking_id,
             'segment_id', s.segment_id,
             'segment_stable_key', s.segment_stable_key,
             'source_ref', s.source_ref,
             'snooze_kind', s.snooze_kind,
             'snooze_until_date', CASE WHEN s.snooze_until_date IS NULL THEN NULL ELSE s.snooze_until_date::text END,
             'note', s.note,
             'cancelled_at_utc', CASE WHEN s.cancelled_at_utc IS NULL THEN NULL ELSE s.cancelled_at_utc::text END,
             'cancel_reason', s.cancel_reason
           )
    INTO v_keeper_id,
         v_before
    FROM public.pay_item_snoozes AS s
    WHERE s.candidate_id = p_candidate_id
      AND s.timesheet_id = v_effective_timesheet_id
      AND s.source_ref IS NOT DISTINCT FROM v_source_ref
      AND UPPER(BTRIM(COALESCE(s.snooze_kind, ''))) = 'DO_NOT_PAY'
      AND s.cleared_at_utc IS NULL
      AND s.cancelled_at_utc IS NOT NULL
    ORDER BY s.cancelled_at_utc DESC NULLS LAST,
             s.updated_at_utc DESC NULLS LAST,
             s.id DESC
    LIMIT 1
    FOR UPDATE;
  END IF;

  IF v_source_ref IS NULL AND v_segment_id_input IS NOT NULL AND v_effective_segment_id IS NULL THEN
    IF v_keeper_id IS NOT NULL THEN
      UPDATE public.pay_item_snoozes s
      SET
        cleared_at_utc = now(),
        cleared_by_user_id = p_actor_user_id,
        updated_at_utc = now(),
        updated_by_user_id = p_actor_user_id
      WHERE s.id = v_keeper_id;

      SELECT jsonb_build_object(
               'id', s.id::text,
               'candidate_id', s.candidate_id::text,
               'timesheet_id', CASE WHEN s.timesheet_id IS NULL THEN NULL ELSE s.timesheet_id::text END,
               'booking_id', s.booking_id,
               'segment_id', s.segment_id,
               'segment_stable_key', s.segment_stable_key,
               'source_ref', s.source_ref,
               'snooze_kind', s.snooze_kind,
               'snooze_until_date', CASE WHEN s.snooze_until_date IS NULL THEN NULL ELSE s.snooze_until_date::text END,
               'note', s.note,
               'cleared_at_utc', CASE WHEN s.cleared_at_utc IS NULL THEN NULL ELSE s.cleared_at_utc::text END
             )
      INTO v_after
      FROM public.pay_item_snoozes s
      WHERE s.id = v_keeper_id;

      PERFORM public._audit_insert(
        'timesheets',
        coalesce(v_effective_timesheet_id, p_timesheet_id)::text,
        'SNOOZE_CLEARED',
        v_before,
        v_after,
        'SEGMENT_NO_LONGER_MATCHED',
        p_actor_user_id
      );

      RETURN jsonb_build_object(
        'ok', true,
        'london_current_date', v_today_uk::text,
        'active_through_end_date', true,
        'action', 'CLEARED_ORPHANED',
        'id', v_keeper_id::text,
        'candidate_id', p_candidate_id::text,
        'timesheet_id', CASE WHEN v_effective_timesheet_id IS NULL THEN NULL ELSE v_effective_timesheet_id::text END,
        'booking_id', v_booking_id,
        'segment_id', NULL,
        'segment_stable_key', v_segment_stable_key,
        'source_ref', NULL,
        'snooze_kind', v_kind,
        'snooze_until_date', NULL,
        'snooze_is_indefinite', NULL,
        'note', v_note,
        'preview_visibility_hint', 'RELOAD_PREVIEW'
      );
    END IF;

    RAISE EXCEPTION 'segment_id could not be matched on the current timesheet';
  END IF;

  UPDATE public.pay_item_snoozes s
  SET
    cleared_at_utc = now(),
    cleared_by_user_id = p_actor_user_id,
    updated_at_utc = now(),
    updated_by_user_id = p_actor_user_id
  WHERE s.candidate_id = p_candidate_id
    AND s.cleared_at_utc IS NULL
    AND s.cancelled_at_utc IS NULL
    AND s.id <> v_keeper_id
    AND (
      (v_source_ref IS NOT NULL AND s.source_ref IS NOT DISTINCT FROM v_source_ref)
      OR
      (
        v_source_ref IS NULL
        AND s.source_ref IS NULL
        AND (
          (
            v_booking_id IS NOT NULL
            AND s.booking_id IS NOT DISTINCT FROM v_booking_id
            AND coalesce(s.segment_stable_key, '') IS NOT DISTINCT FROM coalesce(v_segment_stable_key, '')
          )
          OR
          (
            s.booking_id IS NULL
            AND (
              s.timesheet_id IS NOT DISTINCT FROM p_timesheet_id
              OR s.timesheet_id IS NOT DISTINCT FROM v_effective_timesheet_id
            )
            AND s.segment_id IS NOT DISTINCT FROM coalesce(v_effective_segment_id, v_segment_id_input)
          )
        )
      )
    );

  IF v_keeper_id IS NOT NULL THEN
    UPDATE public.pay_item_snoozes s
    SET
      timesheet_id = CASE WHEN v_source_ref IS NULL OR v_scope_kind = 'TIMESHEET_EXPENSE' THEN v_effective_timesheet_id ELSE NULL END,
      booking_id = CASE WHEN v_source_ref IS NULL OR v_scope_kind = 'TIMESHEET_EXPENSE' THEN v_booking_id ELSE NULL END,
      segment_id = CASE WHEN v_source_ref IS NULL THEN v_effective_segment_id ELSE NULL END,
      segment_stable_key = CASE WHEN v_source_ref IS NULL THEN v_segment_stable_key ELSE NULL END,
      snooze_kind = v_kind,
      snooze_until_date = p_snooze_until_date,
      note = v_note,
      cancelled_at_utc = CASE WHEN v_scope_kind = 'TIMESHEET_EXPENSE' THEN NULL ELSE s.cancelled_at_utc END,
      cancelled_by_user_id = CASE WHEN v_scope_kind = 'TIMESHEET_EXPENSE' THEN NULL ELSE s.cancelled_by_user_id END,
      cancel_reason = CASE WHEN v_scope_kind = 'TIMESHEET_EXPENSE' THEN NULL ELSE s.cancel_reason END,
      updated_at_utc = now(),
      updated_by_user_id = p_actor_user_id
    WHERE s.id = v_keeper_id;

    v_action := 'UPDATED';
  ELSE
    INSERT INTO public.pay_item_snoozes (
      candidate_id,
      timesheet_id,
      booking_id,
      segment_id,
      segment_stable_key,
      source_ref,
      snooze_kind,
      snooze_until_date,
      created_at_utc,
      created_by_user_id,
      note,
      cleared_at_utc,
      cleared_by_user_id,
      updated_at_utc,
      updated_by_user_id
    )
    VALUES (
      p_candidate_id,
      CASE WHEN v_source_ref IS NULL OR v_scope_kind = 'TIMESHEET_EXPENSE' THEN v_effective_timesheet_id ELSE NULL END,
      CASE WHEN v_source_ref IS NULL OR v_scope_kind = 'TIMESHEET_EXPENSE' THEN v_booking_id ELSE NULL END,
      CASE WHEN v_source_ref IS NULL THEN v_effective_segment_id ELSE NULL END,
      CASE WHEN v_source_ref IS NULL THEN v_segment_stable_key ELSE NULL END,
      v_source_ref,
      v_kind,
      p_snooze_until_date,
      now(),
      p_actor_user_id,
      v_note,
      NULL,
      NULL,
      now(),
      p_actor_user_id
    )
    RETURNING id INTO v_keeper_id;

    v_action := 'CREATED';
  END IF;

  IF v_finance_case_id IS NOT NULL
     AND v_kind IN ('OVERPAYMENT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'MANUAL_DEBT_RECOVERY')
     AND p_snooze_until_date IS NOT NULL
  THEN
    IF v_finance_case_record.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum THEN
      v_schedule_before_snooze_json := coalesce(v_finance_case_record.schedule_json, '[]'::jsonb);
      v_next_due_week_start_before_snooze_date := v_finance_case_record.next_due_week_start;
      v_requested_week_start_date := public._pay_week_start_monday(p_snooze_until_date);

      IF v_next_due_week_start_before_snooze_date IS NULL THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_REPAYMENT_SCHEDULE_REBASE_FOR_SNOOZE',
          'code', 'NEXT_DUE_MISSING',
          'message', '_pay_repayment_schedule_rebase_for_snooze: next_due_week_start is required on the finance case'
        )::text;
      END IF;

      IF v_requested_week_start_date < v_next_due_week_start_before_snooze_date THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_REPAYMENT_SCHEDULE_REBASE_FOR_SNOOZE',
          'code', 'SNOOZE_DATE_BEFORE_NEXT_DUE',
          'message', '_pay_repayment_schedule_rebase_for_snooze: snooze_until_date cannot move the repayment schedule earlier than the current next due week',
          'finance_case_id', v_finance_case_id::text,
          'current_next_due_week_start', v_next_due_week_start_before_snooze_date::text,
          'requested_week_start', v_requested_week_start_date::text
        )::text;
      END IF;

      v_target_week_start := CASE
        WHEN v_requested_week_start_date = v_next_due_week_start_before_snooze_date THEN (v_next_due_week_start_before_snooze_date + 7)
        ELSE v_requested_week_start_date
      END;

      WITH schedule_rows AS (
        SELECT
          row_number() OVER (ORDER BY (elem.value ->> 'week_start')::date ASC, elem.ordinality ASC) AS seq_no,
          round(coalesce((elem.value ->> 'amount')::numeric, 0), 2) AS amount_value
        FROM jsonb_array_elements(v_schedule_before_snooze_json) WITH ORDINALITY AS elem(value, ordinality)
        WHERE coalesce((elem.value ->> 'amount')::numeric, 0) < 0
      )
      SELECT
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'week_start', (v_target_week_start + (((sr.seq_no - 1)::integer) * 7))::date,
              'amount', sr.amount_value
            )
            ORDER BY (v_target_week_start + (((sr.seq_no - 1)::integer) * 7))::date ASC
          ),
          '[]'::jsonb
        ),
        count(*)::integer
      INTO v_schedule_after_snooze_json, v_installment_count
      FROM schedule_rows AS sr;

      v_next_due_week_start_after_snooze_date := CASE
        WHEN v_installment_count > 0 THEN v_target_week_start
        ELSE NULL
      END;

      UPDATE public.pay_advances AS pa
      SET schedule_json = v_schedule_after_snooze_json,
          next_due_week_start = v_next_due_week_start_after_snooze_date,
          updated_at = now()
      WHERE pa.id = v_finance_case_id;

      v_schedule_rebase_result := jsonb_build_object(
        'ok', true,
        'finance_case_id', v_finance_case_id::text,
        'schedule_before_snooze_json', v_schedule_before_snooze_json,
        'next_due_week_start_before_snooze', CASE WHEN v_next_due_week_start_before_snooze_date IS NULL THEN NULL ELSE v_next_due_week_start_before_snooze_date::text END,
        'requested_week_start', CASE WHEN v_requested_week_start_date IS NULL THEN NULL ELSE v_requested_week_start_date::text END,
        'schedule_after_snooze_json', v_schedule_after_snooze_json,
        'next_due_week_start_after_snooze', CASE WHEN v_next_due_week_start_after_snooze_date IS NULL THEN NULL ELSE v_next_due_week_start_after_snooze_date::text END,
        'installment_count', v_installment_count
      );
    ELSE
      v_schedule_rebase_result := public._pay_repayment_schedule_rebase_for_snooze(
        v_finance_case_id,
        p_snooze_until_date
      );
    END IF;

    v_schedule_before_snooze_json := v_schedule_rebase_result -> 'schedule_before_snooze_json';
    v_next_due_week_start_before_snooze_date := CASE
      WHEN nullif(btrim(coalesce(v_schedule_rebase_result ->> 'next_due_week_start_before_snooze', '')), '') IS NULL THEN NULL
      ELSE (v_schedule_rebase_result ->> 'next_due_week_start_before_snooze')::date
    END;
    v_next_due_week_start_before_snooze := CASE
      WHEN v_next_due_week_start_before_snooze_date IS NULL THEN NULL
      ELSE v_next_due_week_start_before_snooze_date::text
    END;
    v_requested_week_start_date := CASE
      WHEN nullif(btrim(coalesce(v_schedule_rebase_result ->> 'requested_week_start', '')), '') IS NULL THEN NULL
      ELSE (v_schedule_rebase_result ->> 'requested_week_start')::date
    END;
    v_requested_week_start := CASE
      WHEN v_requested_week_start_date IS NULL THEN NULL
      ELSE v_requested_week_start_date::text
    END;
    v_schedule_after_snooze_json := v_schedule_rebase_result -> 'schedule_after_snooze_json';
    v_next_due_week_start_after_snooze_date := CASE
      WHEN nullif(btrim(coalesce(v_schedule_rebase_result ->> 'next_due_week_start_after_snooze', '')), '') IS NULL THEN NULL
      ELSE (v_schedule_rebase_result ->> 'next_due_week_start_after_snooze')::date
    END;
    v_next_due_week_start_after_snooze := CASE
      WHEN v_next_due_week_start_after_snooze_date IS NULL THEN NULL
      ELSE v_next_due_week_start_after_snooze_date::text
    END;
    v_installment_count := CASE
      WHEN nullif(btrim(coalesce(v_schedule_rebase_result ->> 'installment_count', '')), '') ~ '^-?[0-9]+$'
      THEN (v_schedule_rebase_result ->> 'installment_count')::integer
      ELSE NULL
    END;

    UPDATE public.pay_item_snoozes AS s
    SET schedule_before_snooze_json = v_schedule_before_snooze_json,
        next_due_week_start_before_snooze = v_next_due_week_start_before_snooze_date,
        schedule_after_snooze_json = v_schedule_after_snooze_json,
        next_due_week_start_after_snooze = v_next_due_week_start_after_snooze_date,
        updated_at_utc = now(),
        updated_by_user_id = p_actor_user_id
    WHERE s.id = v_keeper_id;
  END IF;

  SELECT
    jsonb_build_object(
      'id', s.id::text,
      'candidate_id', s.candidate_id::text,
      'timesheet_id', CASE WHEN s.timesheet_id IS NULL THEN NULL ELSE s.timesheet_id::text END,
      'booking_id', s.booking_id,
      'segment_id', s.segment_id,
      'segment_stable_key', s.segment_stable_key,
      'source_ref', s.source_ref,
      'snooze_kind', s.snooze_kind,
      'snooze_until_date', CASE WHEN s.snooze_until_date IS NULL THEN NULL ELSE s.snooze_until_date::text END,
      'note', s.note
    )
    ||
    CASE
      WHEN v_schedule_rebase_result IS NULL THEN '{}'::jsonb
      ELSE jsonb_build_object('schedule_rebase_result', v_schedule_rebase_result)
    END
  INTO v_after
  FROM public.pay_item_snoozes s
  WHERE s.id = v_keeper_id;

  v_event_type := CASE v_action
    WHEN 'CREATED' THEN 'SNOOZE_APPLIED'
    ELSE 'SNOOZE_UPDATED'
  END;

  IF v_finance_case_id IS NOT NULL THEN
    INSERT INTO public.pay_finance_case_events (
      finance_case_id,
      event_type,
      event_at_utc,
      actor_user_id,
      before_json,
      after_json,
      reason,
      note
    )
    VALUES (
      v_finance_case_id,
      v_event_type,
      now(),
      p_actor_user_id,
      v_before,
      v_after,
      v_kind,
      v_note
    );

    PERFORM public._audit_insert(
      'finance_case',
      v_finance_case_id::text,
      v_event_type,
      v_before,
      v_after,
      v_kind,
      p_actor_user_id
    );
  ELSIF coalesce(v_effective_timesheet_id, p_timesheet_id) IS NOT NULL THEN
    PERFORM public._audit_insert(
      'timesheets',
      coalesce(v_effective_timesheet_id, p_timesheet_id)::text,
      v_event_type,
      v_before,
      v_after,
      v_kind,
      p_actor_user_id
    );
  ELSE
    PERFORM public._audit_insert(
      'pay_item_snoozes',
      v_keeper_id::text,
      v_event_type,
      v_before,
      v_after,
      v_kind,
      p_actor_user_id
    );
  END IF;

  RETURN
    jsonb_build_object(
      'ok', true,
      'london_current_date', v_today_uk::text,
      'active_through_end_date', true,
      'action', v_action,
      'id', v_keeper_id::text,
      'candidate_id', p_candidate_id::text,
      'timesheet_id', CASE WHEN v_effective_timesheet_id IS NULL THEN NULL ELSE v_effective_timesheet_id::text END,
      'booking_id', v_booking_id,
      'segment_id', v_effective_segment_id,
      'segment_stable_key', v_segment_stable_key,
      'source_ref', v_source_ref,
      'scope_kind', v_scope_kind,
      'expense_code', v_expense_code,
      'expense_source_basis_fingerprint', v_expense_source_basis_fingerprint,
      'stale_expense_snoozes_cancelled', COALESCE(v_stale_expense_snoozes_cancelled, 0),
      'finance_case_id', CASE WHEN v_finance_case_id IS NULL THEN NULL ELSE v_finance_case_id::text END,
      'snooze_kind', v_kind,
      'snooze_until_date', CASE WHEN p_snooze_until_date IS NULL THEN NULL ELSE p_snooze_until_date::text END,
      'snooze_is_indefinite', (p_snooze_until_date IS NULL),
      'note', v_note,
      'current_official_pay_date', v_date_context->>'current_official_pay_date',
      'next_official_pay_date', v_next_official_pay_date::text,
      'date_context_configuration_fingerprint', v_date_context->>'configuration_fingerprint',
      'resolved_rate_confirmation_applied', COALESCE((v_validation_pre_open->>'warning_required')::boolean, false),
      'pay_run_confirmation_applied', COALESCE((v_validation_pre_save->>'warning_required')::boolean, false)
    )
    ||
    jsonb_build_object(
      'schedule_before_snooze_json', v_schedule_before_snooze_json,
      'next_due_week_start_before_snooze', v_next_due_week_start_before_snooze,
      'requested_week_start', v_requested_week_start,
      'schedule_after_snooze_json', v_schedule_after_snooze_json,
      'next_due_week_start_after_snooze', v_next_due_week_start_after_snooze,
      'installment_count', v_installment_count
    );
END;
$function$;
