-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 76049e0fe887.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.pay_snooze_clear(p_snooze_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_row record;
  v_candidate_id_hint uuid := NULL::uuid;
  v_before jsonb := NULL;
  v_after jsonb := NULL;
  v_finance_case_id uuid := NULL;

  v_finance_case_record public.pay_advances%rowtype;
  v_effective_from_date date := NULL;
  v_requested_week_start_date date := NULL;
  v_current_operating_week_start_date date := NULL;
  v_target_week_start date := NULL;
  v_next_due_week_start_before_restore_date date := NULL;
  v_next_due_week_start_after_restore_date date := NULL;

  v_schedule_restore_result jsonb := NULL;
  v_schedule_before_restore_json jsonb := NULL;
  v_next_due_week_start_before_restore text := NULL;
  v_requested_week_start text := NULL;
  v_current_operating_week_start text := NULL;
  v_schedule_after_restore_json jsonb := NULL;
  v_next_due_week_start_after_restore text := NULL;
  v_installment_count integer := NULL;
  v_rate_resolution_clear_result jsonb := '{}'::jsonb;
BEGIN
  IF p_snooze_id IS NULL THEN
    RAISE EXCEPTION 'snooze_id is required';
  END IF;

  SELECT s.candidate_id
  INTO v_candidate_id_hint
  FROM public.pay_item_snoozes AS s
  WHERE s.id = p_snooze_id;

  IF v_candidate_id_hint IS NULL THEN
    RAISE EXCEPTION 'SNOOZE_NOT_FOUND';
  END IF;

  -- Use the same candidate-then-row lock order as Snooze upsert, natural
  -- expiry and the final Create Draft guard.  Reading the immutable candidate
  -- identity first avoids a row-lock/advisory-lock inversion with a concurrent
  -- exact expense upsert or clear.
  PERFORM pg_advisory_xact_lock(
    pg_catalog.hashtext('public.banking_pay_candidate_snooze_guard'),
    pg_catalog.hashtext(v_candidate_id_hint::text)
  );

  SELECT
    s.id,
    s.candidate_id,
    s.timesheet_id,
    s.segment_id,
    s.source_ref,
    s.snooze_kind,
    s.snooze_until_date,
    s.note,
    s.created_at_utc,
    s.cleared_at_utc
  INTO v_row
  FROM public.pay_item_snoozes AS s
  WHERE s.id = p_snooze_id
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND OR v_row.candidate_id IS DISTINCT FROM v_candidate_id_hint THEN
    RAISE EXCEPTION 'SNOOZE_AUTHORITY_CHANGED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'SNOOZE_AUTHORITY_CHANGED',
              'snooze_id', p_snooze_id::text
            )::text;
  END IF;

  v_before := jsonb_build_object(
    'id', v_row.id::text,
    'candidate_id', v_row.candidate_id::text,
    'timesheet_id', CASE WHEN v_row.timesheet_id IS NULL THEN NULL ELSE v_row.timesheet_id::text END,
    'segment_id', v_row.segment_id,
    'source_ref', v_row.source_ref,
    'snooze_kind', v_row.snooze_kind,
    'snooze_until_date', CASE WHEN v_row.snooze_until_date IS NULL THEN NULL ELSE v_row.snooze_until_date::text END,
    'note', v_row.note,
    'cleared_at_utc', v_row.cleared_at_utc
  );

  perform public._ctms_clear_correction_chain_snoozes_v1(p_snooze_id,p_actor_user_id);

  IF v_row.cleared_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'london_current_date', (clock_timestamp() AT TIME ZONE 'Europe/London')::date::text,
      'action', 'NOOP_ALREADY_CLEARED',
      'id', v_row.id::text
    );
  END IF;

  IF v_row.source_ref IS NOT NULL
     AND v_row.source_ref LIKE 'advance:%'
     AND split_part(v_row.source_ref, ':', 2) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
  THEN
    v_finance_case_id := split_part(v_row.source_ref, ':', 2)::uuid;
  END IF;

  UPDATE public.pay_item_snoozes s
  SET
    cleared_at_utc = now(),
    cleared_by_user_id = p_actor_user_id,
    updated_at_utc = now(),
    updated_by_user_id = p_actor_user_id
  WHERE s.id = p_snooze_id;

  IF to_regprocedure('public.pay_snooze_clear_cross_pay_week_rate_resolutions_v1(uuid,uuid)') IS NOT NULL THEN
    v_rate_resolution_clear_result := public.pay_snooze_clear_cross_pay_week_rate_resolutions_v1(
      p_snooze_id,
      p_actor_user_id
    );
  ELSE
    RAISE EXCEPTION 'CROSS_PAY_WEEK_UNSNOOZE_HELPER_UNAVAILABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'CROSS_PAY_WEEK_UNSNOOZE_HELPER_UNAVAILABLE',
              'message', 'The snooze was not cleared because rate-resolution safety could not be verified.'
            )::text;
  END IF;

  IF v_finance_case_id IS NOT NULL
     AND upper(coalesce(v_row.snooze_kind, '')) IN ('OVERPAYMENT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'MANUAL_DEBT_RECOVERY')
     AND v_row.snooze_until_date IS NOT NULL
  THEN
    v_effective_from_date := (now() AT TIME ZONE 'Europe/London')::date;

    SELECT pa.*
    INTO v_finance_case_record
    FROM public.pay_advances AS pa
    WHERE pa.id = v_finance_case_id
    LIMIT 1
    FOR UPDATE;

    IF v_finance_case_record.id IS NULL THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_REPAYMENT_SCHEDULE_RESTORE_AFTER_SNOOZE_CLEAR',
        'code', 'FINANCE_CASE_NOT_FOUND',
        'message', '_pay_repayment_schedule_restore_after_snooze_clear: finance case not found',
        'finance_case_id', v_finance_case_id::text
      )::text;
    END IF;

    IF v_finance_case_record.case_type = 'OVERPAYMENT'::public.pay_finance_case_type_enum THEN
      v_schedule_before_restore_json := coalesce(v_finance_case_record.schedule_json, '[]'::jsonb);
      v_next_due_week_start_before_restore_date := v_finance_case_record.next_due_week_start;
      v_requested_week_start_date := public._pay_week_start_monday(v_effective_from_date);
      v_current_operating_week_start_date := public._pay_week_start_monday((now() AT TIME ZONE 'Europe/London')::date);

      IF v_requested_week_start_date < v_current_operating_week_start_date THEN
        RAISE EXCEPTION '%', jsonb_build_object(
          'error', 'PAY_REPAYMENT_SCHEDULE_RESTORE_AFTER_SNOOZE_CLEAR',
          'code', 'EFFECTIVE_FROM_DATE_BEFORE_NOW',
          'message', '_pay_repayment_schedule_restore_after_snooze_clear: effective_from_date cannot restore the repayment schedule earlier than the current operating week',
          'finance_case_id', v_finance_case_id::text,
          'requested_week_start', v_requested_week_start_date::text,
          'current_operating_week_start', v_current_operating_week_start_date::text
        )::text;
      END IF;

      v_target_week_start := v_requested_week_start_date;

      WITH schedule_rows AS (
        SELECT
          row_number() OVER (ORDER BY (elem.value ->> 'week_start')::date ASC, elem.ordinality ASC) AS seq_no,
          round(coalesce((elem.value ->> 'amount')::numeric, 0), 2) AS amount_value
        FROM jsonb_array_elements(v_schedule_before_restore_json) WITH ORDINALITY AS elem(value, ordinality)
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
      INTO v_schedule_after_restore_json, v_installment_count
      FROM schedule_rows AS sr;

      v_next_due_week_start_after_restore_date := CASE
        WHEN v_installment_count > 0 THEN v_target_week_start
        ELSE NULL
      END;

      UPDATE public.pay_advances AS pa
      SET schedule_json = v_schedule_after_restore_json,
          next_due_week_start = v_next_due_week_start_after_restore_date,
          updated_at = now()
      WHERE pa.id = v_finance_case_id;

      v_schedule_restore_result := jsonb_build_object(
        'ok', true,
        'finance_case_id', v_finance_case_id::text,
        'schedule_before_restore_json', v_schedule_before_restore_json,
        'next_due_week_start_before_restore', CASE WHEN v_next_due_week_start_before_restore_date IS NULL THEN NULL ELSE v_next_due_week_start_before_restore_date::text END,
        'requested_week_start', CASE WHEN v_requested_week_start_date IS NULL THEN NULL ELSE v_requested_week_start_date::text END,
        'current_operating_week_start', CASE WHEN v_current_operating_week_start_date IS NULL THEN NULL ELSE v_current_operating_week_start_date::text END,
        'schedule_after_restore_json', v_schedule_after_restore_json,
        'next_due_week_start_after_restore', CASE WHEN v_next_due_week_start_after_restore_date IS NULL THEN NULL ELSE v_next_due_week_start_after_restore_date::text END,
        'installment_count', v_installment_count
      );
    ELSE
      v_schedule_restore_result := public._pay_repayment_schedule_restore_after_snooze_clear(
        v_finance_case_id,
        v_effective_from_date
      );
    END IF;

    v_schedule_before_restore_json := v_schedule_restore_result -> 'schedule_before_restore_json';
    v_next_due_week_start_before_restore_date := CASE
      WHEN nullif(btrim(coalesce(v_schedule_restore_result ->> 'next_due_week_start_before_restore', '')), '') IS NULL THEN NULL
      ELSE (v_schedule_restore_result ->> 'next_due_week_start_before_restore')::date
    END;
    v_next_due_week_start_before_restore := CASE
      WHEN v_next_due_week_start_before_restore_date IS NULL THEN NULL
      ELSE v_next_due_week_start_before_restore_date::text
    END;
    v_requested_week_start_date := CASE
      WHEN nullif(btrim(coalesce(v_schedule_restore_result ->> 'requested_week_start', '')), '') IS NULL THEN NULL
      ELSE (v_schedule_restore_result ->> 'requested_week_start')::date
    END;
    v_requested_week_start := CASE
      WHEN v_requested_week_start_date IS NULL THEN NULL
      ELSE v_requested_week_start_date::text
    END;
    v_current_operating_week_start_date := CASE
      WHEN nullif(btrim(coalesce(v_schedule_restore_result ->> 'current_operating_week_start', '')), '') IS NULL THEN NULL
      ELSE (v_schedule_restore_result ->> 'current_operating_week_start')::date
    END;
    v_current_operating_week_start := CASE
      WHEN v_current_operating_week_start_date IS NULL THEN NULL
      ELSE v_current_operating_week_start_date::text
    END;
    v_schedule_after_restore_json := v_schedule_restore_result -> 'schedule_after_restore_json';
    v_next_due_week_start_after_restore_date := CASE
      WHEN nullif(btrim(coalesce(v_schedule_restore_result ->> 'next_due_week_start_after_restore', '')), '') IS NULL THEN NULL
      ELSE (v_schedule_restore_result ->> 'next_due_week_start_after_restore')::date
    END;
    v_next_due_week_start_after_restore := CASE
      WHEN v_next_due_week_start_after_restore_date IS NULL THEN NULL
      ELSE v_next_due_week_start_after_restore_date::text
    END;
    v_installment_count := CASE
      WHEN nullif(btrim(coalesce(v_schedule_restore_result ->> 'installment_count', '')), '') ~ '^-?[0-9]+$'
      THEN (v_schedule_restore_result ->> 'installment_count')::integer
      ELSE NULL
    END;
  END IF;

  SELECT
    jsonb_build_object(
      'id', s.id::text,
      'candidate_id', s.candidate_id::text,
      'timesheet_id', CASE WHEN s.timesheet_id IS NULL THEN NULL ELSE s.timesheet_id::text END,
      'segment_id', s.segment_id,
      'source_ref', s.source_ref,
      'snooze_kind', s.snooze_kind,
      'snooze_until_date', CASE WHEN s.snooze_until_date IS NULL THEN NULL ELSE s.snooze_until_date::text END,
      'note', s.note,
      'cleared_at_utc', s.cleared_at_utc
    )
    ||
    CASE
      WHEN v_schedule_restore_result IS NULL THEN '{}'::jsonb
      ELSE jsonb_build_object('schedule_restore_result', v_schedule_restore_result)
    END
  INTO v_after
  FROM public.pay_item_snoozes s
  WHERE s.id = p_snooze_id;

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
      'SNOOZE_CLEARED',
      now(),
      p_actor_user_id,
      v_before,
      v_after,
      COALESCE(v_row.snooze_kind, 'SNOOZE_CLEARED'),
      v_row.note
    );

    PERFORM public._audit_insert(
      'finance_case',
      v_finance_case_id::text,
      'SNOOZE_CLEARED',
      v_before,
      v_after,
      COALESCE(v_row.snooze_kind, 'SNOOZE_CLEARED'),
      p_actor_user_id
    );
  ELSIF v_row.timesheet_id IS NOT NULL THEN
    PERFORM public._audit_insert(
      'timesheets',
      v_row.timesheet_id::text,
      'SNOOZE_CLEARED',
      v_before,
      v_after,
      COALESCE(v_row.snooze_kind, 'SNOOZE_CLEARED'),
      p_actor_user_id
    );
  ELSE
    PERFORM public._audit_insert(
      'pay_item_snoozes',
      v_row.id::text,
      'SNOOZE_CLEARED',
      v_before,
      v_after,
      COALESCE(v_row.snooze_kind, 'SNOOZE_CLEARED'),
      p_actor_user_id
    );
  END IF;

  RETURN
    jsonb_build_object(
      'ok', true,
      'london_current_date', (clock_timestamp() AT TIME ZONE 'Europe/London')::date::text,
      'rate_resolution_review_required_after_cross_pay_week_unsnooze',
        COALESCE((v_rate_resolution_clear_result->>'review_required')::boolean, false),
      'rate_resolution_clear_result', COALESCE(v_rate_resolution_clear_result, '{}'::jsonb),
      'action', 'CLEARED',
      'id', v_row.id::text,
      'candidate_id', v_row.candidate_id::text,
      'timesheet_id', CASE WHEN v_row.timesheet_id IS NULL THEN NULL ELSE v_row.timesheet_id::text END,
      'segment_id', v_row.segment_id,
      'source_ref', v_row.source_ref,
      'finance_case_id', CASE WHEN v_finance_case_id IS NULL THEN NULL ELSE v_finance_case_id::text END,
      'snooze_kind', v_row.snooze_kind,
      'preview_visibility_hint', 'RELOAD_PREVIEW'
    )
    ||
    jsonb_build_object(
      'schedule_before_restore_json', v_schedule_before_restore_json,
      'next_due_week_start_before_restore', v_next_due_week_start_before_restore,
      'requested_week_start', v_requested_week_start,
      'current_operating_week_start', v_current_operating_week_start,
      'schedule_after_restore_json', v_schedule_after_restore_json,
      'next_due_week_start_after_restore', v_next_due_week_start_after_restore,
      'installment_count', v_installment_count
    );
END;
$function$;
