DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone);

CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_atomic(p_week_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_timesheet_create_json jsonb DEFAULT NULL::jsonb, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_rotation_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_materialise_staged_evidence boolean DEFAULT true, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS TABLE(contract_week_id uuid, contract_id uuid, timesheet_id uuid, current_timesheet_id uuid, current_timesheet_version integer, was_stale boolean, created_now boolean, processing_status ts_fin_processing_status_enum, contract_week_json jsonb, timesheet_json jsonb, timesheet_financials_json jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());

  v_week public.contract_weeks%ROWTYPE;
  v_contract public.contracts%ROWTYPE;
  v_pointer_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;

  v_create_json jsonb := CASE
    WHEN p_timesheet_create_json IS NULL THEN NULL
    WHEN jsonb_typeof(p_timesheet_create_json) = 'object' THEN p_timesheet_create_json
    ELSE NULL
  END;
  v_patch_json jsonb := CASE
    WHEN p_timesheet_patch_json IS NULL THEN '{}'::jsonb
    WHEN jsonb_typeof(p_timesheet_patch_json) = 'object' THEN p_timesheet_patch_json
    ELSE NULL
  END;
  v_week_patch_json jsonb := CASE
    WHEN p_contract_week_patch_json IS NULL THEN '{}'::jsonb
    WHEN jsonb_typeof(p_contract_week_patch_json) = 'object' THEN p_contract_week_patch_json
    ELSE NULL
  END;
  v_tsfin_snapshot_json jsonb := CASE
    WHEN p_tsfin_snapshot_json IS NULL THEN NULL
    WHEN jsonb_typeof(p_tsfin_snapshot_json) = 'object' THEN p_tsfin_snapshot_json
    ELSE NULL
  END;
  v_rotation_json jsonb := CASE
    WHEN p_rotation_json IS NULL THEN NULL
    WHEN jsonb_typeof(p_rotation_json) = 'object' THEN p_rotation_json
    ELSE NULL
  END;

  v_create_rec public.timesheets%ROWTYPE;
  v_patch_rec public.timesheets%ROWTYPE;
  v_week_patch_rec public.contract_weeks%ROWTYPE;

  v_created_now boolean := false;
  v_was_stale boolean := false;

  v_rotation_action text := NULL;
  v_rotation_revoke_reason text := NULL;
  v_rotation_new_timesheet_id uuid := NULL;
  v_rotation_pending_qr boolean := false;
  v_next_version integer := NULL;
  v_rotated_ts public.timesheets%ROWTYPE;

  v_original_booking_id text := NULL;
  v_candidate_booking_id text := NULL;
  v_booking_retry integer := 0;
  v_existing_booking_ts public.timesheets%ROWTYPE;
  v_existing_booking_cw_id uuid := NULL;
  v_existing_booking_cw_contract_id uuid := NULL;
  v_existing_booking_cw_week_ending_date date := NULL;
  v_existing_booking_same_context boolean := FALSE;

  v_queue_item public.manual_timesheet_queue%ROWTYPE;
  v_queue_kind text := NULL;
  v_queue_storage_key text := NULL;
  v_primary_timesheet_queue_id uuid := NULL;
  v_primary_timesheet_storage_key text := NULL;
  v_primary_timesheet_rotation_raw integer := 0;
  v_primary_timesheet_rotation_deg integer := 0;
  v_timesheet_kind_count integer := 0;

  v_missing jsonb := '[]'::jsonb;
  v_has_evidence boolean := false;
  v_pre_decision_row jsonb := NULL;
  v_post_decision_row jsonb := NULL;
  v_expected_row_signature text := NULL;
  v_current_row_signature text := NULL;
  v_previous_row_signature text := NULL;
  v_new_row_signature text := NULL;
  v_previous_processing_status public.ts_fin_processing_status_enum := NULL;
  v_segment_invoice_lock boolean := FALSE;
  v_compare_key text := NULL;
  -- p_tsfin_snapshot_json is the intended next-write TSFIN snapshot.
  -- It must not be compared to the old/current TSFIN for mutable financial values.

  v_outbox public.ts_financials_outbox%ROWTYPE;
  v_write_result record;

  v_numeric_re text := '^-?[0-9]+([.][0-9]+)?$';
  v_is_additional_manual_adjustment boolean := FALSE;
  v_force_empty_additional_schedule boolean := FALSE;
  v_effective_planned_schedule_json jsonb := NULL;
  v_effective_totals_json jsonb := NULL;
  v_effective_contract_week_hours_zero boolean := TRUE;
  v_expenses_pay_ex_vat numeric := 0;
  v_expenses_charge_ex_vat numeric := 0;
  v_expenses_margin_ex_vat numeric := 0;
  v_mileage_pay_ex_vat numeric := 0;
  v_mileage_charge_ex_vat numeric := 0;
  v_additional_pay_ex_vat numeric := 0;
  v_additional_charge_ex_vat numeric := 0;
  v_additional_margin_ex_vat numeric := 0;
  v_nonsegment_pay_ex_vat numeric := 0;
  v_nonsegment_charge_ex_vat numeric := 0;
  v_nonsegment_margin_ex_vat numeric := 0;
  v_total_pay_ex_vat numeric := 0;
  v_total_charge_ex_vat numeric := 0;
  v_total_margin_ex_vat numeric := 0;
  v_effective_invoice_breakdown_json jsonb := '{}'::jsonb;
  v_additional_units_json jsonb := '{}'::jsonb;
  v_has_additional_unit_rows boolean := FALSE;
  v_existing_planned_schedule_json jsonb := NULL;
  v_existing_totals_json jsonb := NULL;
  v_existing_contract_week_hours_zero boolean := TRUE;
  v_existing_contract_week_schedule_empty boolean := FALSE;
  v_payload_keep_empty_requested boolean := FALSE;
  v_payload_explicit_schedule_edit boolean := FALSE;
  v_paid_direct_expense_change boolean := FALSE;
BEGIN
  IF p_week_id IS NULL THEN
    RAISE EXCEPTION 'p_week_id is required';
  END IF;

  IF p_timesheet_patch_json IS NOT NULL AND v_patch_json IS NULL THEN
    RAISE EXCEPTION 'p_timesheet_patch_json must be a JSON object';
  END IF;

  IF p_contract_week_patch_json IS NOT NULL AND v_week_patch_json IS NULL THEN
    RAISE EXCEPTION 'p_contract_week_patch_json must be a JSON object';
  END IF;

  IF p_timesheet_create_json IS NOT NULL AND v_create_json IS NULL THEN
    RAISE EXCEPTION 'p_timesheet_create_json must be a JSON object';
  END IF;

  IF p_tsfin_snapshot_json IS NOT NULL AND v_tsfin_snapshot_json IS NULL THEN
    RAISE EXCEPTION 'p_tsfin_snapshot_json must be a JSON object';
  END IF;

  IF p_rotation_json IS NOT NULL AND v_rotation_json IS NULL THEN
    RAISE EXCEPTION 'p_rotation_json must be a JSON object';
  END IF;

  SELECT *
  INTO v_week
  FROM public.contract_weeks AS cw
  WHERE cw.id = p_week_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CONTRACT_WEEK_NOT_FOUND';
  END IF;

  -- Serialise weekly manual process/materialisation for the contract-week staged evidence keyspace.
  -- The partial unique index added by the staged TIMESHEET invariant is the hard backstop;
  -- this lock reduces avoidable retry/conflict churn inside this SQL transaction.
  PERFORM pg_advisory_xact_lock(hashtext('contract_week_staged_timesheet:' || v_week.id::text));

  SELECT *
  INTO v_contract
  FROM public.contracts AS ct
  WHERE ct.id = v_week.contract_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CONTRACT_NOT_FOUND';
  END IF;

  v_is_additional_manual_adjustment :=
    COALESCE(v_week.is_adjustment, FALSE) = TRUE
    OR COALESCE(v_week.additional_seq, 0) > 0;

  v_existing_planned_schedule_json := v_week.planned_schedule_json;
  v_existing_totals_json := v_week.totals_json;

  v_existing_contract_week_schedule_empty :=
    v_existing_planned_schedule_json IS NULL
    OR v_existing_planned_schedule_json = 'null'::jsonb
    OR CASE
      WHEN jsonb_typeof(v_existing_planned_schedule_json) = 'array' THEN jsonb_array_length(v_existing_planned_schedule_json) = 0
      ELSE FALSE
    END;

  SELECT COALESCE(SUM(
           CASE
             WHEN NULLIF(BTRIM(COALESCE(existing_hours_value.hour_value, '')), '') ~ v_numeric_re THEN existing_hours_value.hour_value::numeric
             ELSE 0::numeric
           END
         ), 0::numeric) = 0::numeric
    INTO v_existing_contract_week_hours_zero
  FROM jsonb_each_text(
    CASE
      WHEN jsonb_typeof(v_existing_totals_json->'hours') = 'object' THEN v_existing_totals_json->'hours'
      ELSE '{}'::jsonb
    END
  ) AS existing_hours_value(hour_key, hour_value);

  v_effective_planned_schedule_json := CASE
    WHEN v_week_patch_json ? 'planned_schedule_json' THEN v_week_patch_json->'planned_schedule_json'
    ELSE v_week.planned_schedule_json
  END;

  v_effective_totals_json := CASE
    WHEN v_week_patch_json ? 'totals_json' THEN v_week_patch_json->'totals_json'
    ELSE v_week.totals_json
  END;

  SELECT COALESCE(SUM(
           CASE
             WHEN NULLIF(BTRIM(COALESCE(hours_value.value, '')), '') ~ v_numeric_re THEN hours_value.value::numeric
             ELSE 0::numeric
           END
         ), 0::numeric) = 0::numeric
    INTO v_effective_contract_week_hours_zero
  FROM jsonb_each_text(
    CASE
      WHEN jsonb_typeof(v_effective_totals_json->'hours') = 'object' THEN v_effective_totals_json->'hours'
      ELSE '{}'::jsonb
    END
  ) AS hours_value(key, value);

  v_payload_keep_empty_requested :=
    COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'__keepAdditionalManualAdjustmentScheduleEmpty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'__suppressStandardScheduleFallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'__keepAdditionalManualAdjustmentScheduleEmpty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'__suppressStandardScheduleFallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'keep_additional_manual_adjustment_schedule_empty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'__keepAdditionalManualAdjustmentScheduleEmpty', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'suppress_standard_schedule_fallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'__suppressStandardScheduleFallback', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE);

  v_payload_explicit_schedule_edit :=
    COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'explicit_schedule_edit', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'__scheduleUserEdited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'user_entered_schedule', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_week_patch_json->>'manual_schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'explicit_schedule_edit', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'__scheduleUserEdited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'user_entered_schedule', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(UPPER(NULLIF(BTRIM(COALESCE(v_patch_json->>'manual_schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1'), FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'explicit_schedule_edit', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'__scheduleUserEdited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'user_entered_schedule', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE)
    OR COALESCE(CASE WHEN v_create_json IS NULL THEN NULL ELSE UPPER(NULLIF(BTRIM(COALESCE(v_create_json->>'manual_schedule_user_edited', '')), '')) IN ('TRUE', 'T', 'YES', 'Y', '1') END, FALSE);

  v_force_empty_additional_schedule :=
    COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE
    AND COALESCE(v_payload_explicit_schedule_edit, FALSE) = FALSE
    AND (
      COALESCE(v_payload_keep_empty_requested, FALSE) = TRUE
      OR (
        COALESCE(v_existing_contract_week_schedule_empty, FALSE) = TRUE
        AND COALESCE(v_existing_contract_week_hours_zero, TRUE) = TRUE
      )
      OR (
        (
          v_effective_planned_schedule_json IS NULL
          OR v_effective_planned_schedule_json = 'null'::jsonb
          OR CASE
            WHEN jsonb_typeof(v_effective_planned_schedule_json) = 'array' THEN jsonb_array_length(v_effective_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND COALESCE(v_effective_contract_week_hours_zero, TRUE) = TRUE
      )
    );

  IF COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN
    IF v_create_json IS NOT NULL THEN
      v_create_json := v_create_json || jsonb_build_object('is_adjustment', TRUE);
    END IF;

    v_patch_json := COALESCE(v_patch_json, '{}'::jsonb) || jsonb_build_object('is_adjustment', TRUE);
    v_week_patch_json := COALESCE(v_week_patch_json, '{}'::jsonb) || jsonb_build_object('is_adjustment', TRUE);
  END IF;

  IF COALESCE(v_force_empty_additional_schedule, FALSE) = TRUE THEN
    v_effective_totals_json := COALESCE(v_effective_totals_json, '{}'::jsonb) || jsonb_build_object(
      'hours', jsonb_build_object(
        'day', 0,
        'night', 0,
        'sat', 0,
        'sun', 0,
        'bh', 0
      )
    );

    v_week_patch_json := COALESCE(v_week_patch_json, '{}'::jsonb) || jsonb_build_object(
      'planned_schedule_json', '[]'::jsonb,
      'totals_json', v_effective_totals_json
    );

    IF v_create_json IS NOT NULL THEN
      v_create_json := v_create_json || jsonb_build_object('actual_schedule_json', '[]'::jsonb);
    END IF;

    v_patch_json := COALESCE(v_patch_json, '{}'::jsonb) || jsonb_build_object('actual_schedule_json', '[]'::jsonb);

    IF v_tsfin_snapshot_json IS NOT NULL THEN
      -- Empty additional/manual adjustment weeks have no standard hours, but their
      -- expense, mileage, and genuine additional-unit amounts remain separate TSFIN
      -- buckets. Do not copy expense totals into additional_pay_ex_vat.
      v_expenses_pay_ex_vat :=
        COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'travel_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'travel_pay_ex_vat')::numeric ELSE NULL END, 0::numeric)
        + COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'accommodation_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'accommodation_pay_ex_vat')::numeric ELSE NULL END, 0::numeric)
        + COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'other_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'other_pay_ex_vat')::numeric ELSE NULL END, 0::numeric);

      IF v_expenses_pay_ex_vat = 0::numeric THEN
        v_expenses_pay_ex_vat := COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'expenses_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'expenses_pay_ex_vat')::numeric
            ELSE NULL
          END,
          0::numeric
        );
      END IF;

      v_expenses_charge_ex_vat :=
        COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'travel_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'travel_charge_ex_vat')::numeric ELSE NULL END, 0::numeric)
        + COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'accommodation_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'accommodation_charge_ex_vat')::numeric ELSE NULL END, 0::numeric)
        + COALESCE(CASE WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'other_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'other_charge_ex_vat')::numeric ELSE NULL END, 0::numeric);

      IF v_expenses_charge_ex_vat = 0::numeric THEN
        v_expenses_charge_ex_vat := COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'expenses_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'expenses_charge_ex_vat')::numeric
            ELSE NULL
          END,
          0::numeric
        );
      END IF;

      v_mileage_pay_ex_vat := COALESCE(
        CASE
          WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'mileage_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'mileage_pay_ex_vat')::numeric
          ELSE NULL
        END,
        0::numeric
      );

      v_mileage_charge_ex_vat := COALESCE(
        CASE
          WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'mileage_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'mileage_charge_ex_vat')::numeric
          ELSE NULL
        END,
        0::numeric
      );

      v_additional_units_json := CASE
        WHEN jsonb_typeof(v_tsfin_snapshot_json->'additional_units_json') = 'object' THEN v_tsfin_snapshot_json->'additional_units_json'
        ELSE '{}'::jsonb
      END;

      WITH RECURSIVE additional_unit_walk(value_json, key_text) AS (
        SELECT root_json, root_key
        FROM (
          VALUES
            (v_additional_units_json, NULL::text),
            (v_patch_json->'additional_units_week', NULL::text),
            (v_patch_json->'additional_units_per_day', NULL::text),
            (v_create_json->'additional_units_week', NULL::text),
            (v_create_json->'additional_units_per_day', NULL::text),
            (v_effective_totals_json->'additional_units_week', NULL::text),
            (v_effective_totals_json->'additional_units_per_day', NULL::text)
        ) AS additional_unit_roots(root_json, root_key)
        WHERE root_json IS NOT NULL
          AND jsonb_typeof(root_json) IN ('object', 'array', 'number', 'string')

        UNION ALL

        SELECT child_json.value_json, child_json.key_text
        FROM additional_unit_walk AS additional_unit_parent
        CROSS JOIN LATERAL (
          SELECT object_child.value AS value_json,
                 object_child.key::text AS key_text
          FROM jsonb_each(
            CASE
              WHEN jsonb_typeof(additional_unit_parent.value_json) = 'object' THEN additional_unit_parent.value_json
              ELSE '{}'::jsonb
            END
          ) AS object_child(key, value)

          UNION ALL

          SELECT array_child.value AS value_json,
                 additional_unit_parent.key_text AS key_text
          FROM jsonb_array_elements(
            CASE
              WHEN jsonb_typeof(additional_unit_parent.value_json) = 'array' THEN additional_unit_parent.value_json
              ELSE '[]'::jsonb
            END
          ) AS array_child(value)
        ) AS child_json
      )
      SELECT EXISTS (
        SELECT 1
        FROM additional_unit_walk AS additional_unit_value
        WHERE (
          (
            jsonb_typeof(additional_unit_value.value_json) = 'number'
          AND NULLIF(BTRIM(additional_unit_value.value_json #>> '{}'), '') ~ v_numeric_re
          AND (additional_unit_value.value_json #>> '{}')::numeric <> 0::numeric
        )
        OR (
          jsonb_typeof(additional_unit_value.value_json) = 'string'
          AND NULLIF(BTRIM(additional_unit_value.value_json #>> '{}'), '') ~ v_numeric_re
          AND (additional_unit_value.value_json #>> '{}')::numeric <> 0::numeric
          )
        )
        AND LOWER(COALESCE(NULLIF(BTRIM(additional_unit_value.key_text), ''), '')) NOT IN (
          'pay_ex_vat',
          'charge_ex_vat',
          'margin_ex_vat',
          'pay_rate',
          'charge_rate',
          'rate',
          'amount',
          'pay',
          'charge',
          'margin',
          'price',
          'cost',
          'total',
          'total_pay',
          'total_charge',
          'total_margin',
          'total_pay_ex_vat',
          'total_charge_ex_vat',
          'total_margin_ex_vat',
          'vat',
          'vat_rate',
          'inc_vat',
          'code',
          'label',
          'name',
          'id',
          'sort_order',
          'order',
          'seq',
          'sequence',
          'version',
          'day_index',
          'additional_seq',
          'week',
          'date'
        )
      )
      INTO v_has_additional_unit_rows;

      IF COALESCE(v_has_additional_unit_rows, FALSE) THEN
        v_additional_pay_ex_vat := COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'additional_pay_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'additional_pay_ex_vat')::numeric
            ELSE NULL
          END,
          0::numeric
        );

        v_additional_charge_ex_vat := COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'additional_charge_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'additional_charge_ex_vat')::numeric
            ELSE NULL
          END,
          0::numeric
        );

        v_additional_margin_ex_vat := COALESCE(
          CASE
            WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'additional_margin_ex_vat', '')), '') ~ v_numeric_re THEN (v_tsfin_snapshot_json->>'additional_margin_ex_vat')::numeric
            ELSE NULL
          END,
          v_additional_charge_ex_vat - v_additional_pay_ex_vat
        );
      ELSE
        v_additional_pay_ex_vat := 0::numeric;
        v_additional_charge_ex_vat := 0::numeric;
        v_additional_margin_ex_vat := 0::numeric;
      END IF;

      v_expenses_margin_ex_vat := v_expenses_charge_ex_vat - v_expenses_pay_ex_vat;
      v_nonsegment_pay_ex_vat := v_additional_pay_ex_vat + v_expenses_pay_ex_vat + v_mileage_pay_ex_vat;
      v_nonsegment_charge_ex_vat := v_additional_charge_ex_vat + v_expenses_charge_ex_vat + v_mileage_charge_ex_vat;
      v_nonsegment_margin_ex_vat := v_nonsegment_charge_ex_vat - v_nonsegment_pay_ex_vat;
      v_total_pay_ex_vat := v_nonsegment_pay_ex_vat;
      v_total_charge_ex_vat := v_nonsegment_charge_ex_vat;
      v_total_margin_ex_vat := v_total_charge_ex_vat - v_total_pay_ex_vat;

      v_effective_invoice_breakdown_json := COALESCE(
        CASE
          WHEN jsonb_typeof(v_tsfin_snapshot_json->'invoice_breakdown_json') = 'object' THEN v_tsfin_snapshot_json->'invoice_breakdown_json'
          ELSE '{}'::jsonb
        END,
        '{}'::jsonb
      ) || jsonb_build_object(
        'mode', 'SEGMENTS',
        'segments', '[]'::jsonb,
        'additional', jsonb_build_object(
          'units', v_additional_units_json,
          'pay_ex_vat', v_additional_pay_ex_vat,
          'charge_ex_vat', v_additional_charge_ex_vat,
          'margin_ex_vat', v_additional_margin_ex_vat
        ),
        'totals', jsonb_build_object(
          'total_hours', 0,
          'total_pay_ex_vat', v_total_pay_ex_vat,
          'total_charge_ex_vat', v_total_charge_ex_vat,
          'margin_ex_vat', v_total_margin_ex_vat
        )
      );

      v_tsfin_snapshot_json := v_tsfin_snapshot_json || jsonb_build_object(
        'hours_day', 0,
        'hours_night', 0,
        'hours_sat', 0,
        'hours_sun', 0,
        'hours_bh', 0,
        'pay_day', 0,
        'pay_night', 0,
        'pay_sat', 0,
        'pay_sun', 0,
        'pay_bh', 0,
        'charge_day', 0,
        'charge_night', 0,
        'charge_sat', 0,
        'charge_sun', 0,
        'charge_bh', 0,
        'total_hours', 0,
        'expenses_pay_ex_vat', v_expenses_pay_ex_vat,
        'expenses_charge_ex_vat', v_expenses_charge_ex_vat,
        'additional_pay_ex_vat', v_additional_pay_ex_vat,
        'additional_charge_ex_vat', v_additional_charge_ex_vat,
        'additional_margin_ex_vat', v_additional_margin_ex_vat,
        'total_pay_ex_vat', v_total_pay_ex_vat,
        'total_charge_ex_vat', v_total_charge_ex_vat,
        'margin_ex_vat', v_total_margin_ex_vat,
        'actual_schedule_json', '[]'::jsonb,
        'actual_minutes_by_day_json', '{}'::jsonb,
        'invoice_breakdown_json', v_effective_invoice_breakdown_json
      );
    END IF;
  END IF;

  IF v_week.timesheet_id IS NOT NULL THEN
    SELECT *
    INTO v_pointer_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = v_week.timesheet_id
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'TIMESHEET_NOT_FOUND';
    END IF;

    IF COALESCE(v_pointer_ts.is_current, false) THEN
      v_current_ts := v_pointer_ts;
    ELSE
      SELECT *
      INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_pointer_ts.booking_id
        AND ts.is_current = true
      ORDER BY ts.version DESC, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;

      IF NOT FOUND THEN
        SELECT *
        INTO v_current_ts
        FROM public.timesheets AS ts
        WHERE ts.booking_id = v_pointer_ts.booking_id
        ORDER BY ts.version DESC, ts.timesheet_id DESC
        LIMIT 1
        FOR UPDATE;

        IF NOT FOUND THEN
          v_current_ts := v_pointer_ts;
        END IF;
      END IF;
    END IF;

    IF v_current_ts.timesheet_id IS DISTINCT FROM v_week.timesheet_id THEN
      v_was_stale := true;
    END IF;

    IF p_expected_timesheet_id IS NULL THEN
      RAISE EXCEPTION 'expected_timesheet_id is required';
    END IF;

    IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
      RAISE EXCEPTION USING
        MESSAGE = 'TIMESHEET_MOVED',
        DETAIL = jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id::text
        )::text;
    END IF;
  END IF;

  SELECT decision_result.row_json
    INTO v_pre_decision_row
  FROM public.bulk_timesheet_row_decision_v1(
    CASE
      WHEN v_current_ts.timesheet_id IS NOT NULL THEN JSONB_BUILD_OBJECT(
        'dataset_mode', 'process',
        'timesheet_id', v_current_ts.timesheet_id::text,
        'contract_week_id', v_week.id::text
      )
      ELSE JSONB_BUILD_OBJECT(
        'dataset_mode', 'process',
        'contract_week_id', v_week.id::text
      )
    END
  ) AS decision_result(row_json)
  LIMIT 1;

  v_expected_row_signature := NULLIF(BTRIM(COALESCE(
    p_expected_row_signature,
    v_patch_json->>'row_signature',
    v_patch_json->>'rowSignature',
    v_week_patch_json->>'row_signature',
    v_week_patch_json->>'rowSignature',
    ''
  )), '');
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_pre_decision_row->>'row_signature', '')), '');
  v_previous_row_signature := v_current_row_signature;

  IF v_expected_row_signature IS NOT NULL
     AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object(
        'expected_row_signature', v_expected_row_signature,
        'current_row_signature', v_current_row_signature,
        'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END,
        'contract_week_id', v_week.id::text
      )::text;
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    IF v_current_ts.authorised_at_server IS NOT NULL THEN
      RAISE EXCEPTION 'TIMESHEET_ALREADY_AUTHORISED';
    END IF;

    SELECT *
      INTO v_current_tsfin
    FROM public.timesheets_financials AS tsfin_lock
    WHERE tsfin_lock.timesheet_id = v_current_ts.timesheet_id
      AND tsfin_lock.is_current = TRUE
    ORDER BY tsfin_lock.computed_at_utc DESC NULLS LAST, tsfin_lock.created_at DESC NULLS LAST, tsfin_lock.updated_at DESC NULLS LAST, tsfin_lock.id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_current_tsfin.id IS NOT NULL THEN
      v_previous_processing_status := v_current_tsfin.processing_status;

      SELECT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json->'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json->'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment_json)
        WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) INTO v_segment_invoice_lock;

      v_paid_direct_expense_change := FALSE;

      IF v_current_tsfin.paid_at_utc IS NOT NULL
         AND v_tsfin_snapshot_json IS NOT NULL THEN
        SELECT (
          EXISTS (
            SELECT 1
            FROM (VALUES
              ('mileage_units', COALESCE(v_current_tsfin.mileage_units, 0)::numeric),
              ('mileage_pay_rate', COALESCE(v_current_tsfin.mileage_pay_rate, 0)::numeric),
              ('mileage_charge_rate', COALESCE(v_current_tsfin.mileage_charge_rate, 0)::numeric),
              ('mileage_pay_ex_vat', COALESCE(v_current_tsfin.mileage_pay_ex_vat, 0)::numeric),
              ('mileage_charge_ex_vat', COALESCE(v_current_tsfin.mileage_charge_ex_vat, 0)::numeric),
              ('travel_pay_ex_vat', COALESCE(v_current_tsfin.travel_pay_ex_vat, 0)::numeric),
              ('travel_charge_ex_vat', COALESCE(v_current_tsfin.travel_charge_ex_vat, 0)::numeric),
              ('accommodation_pay_ex_vat', COALESCE(v_current_tsfin.accommodation_pay_ex_vat, 0)::numeric),
              ('accommodation_charge_ex_vat', COALESCE(v_current_tsfin.accommodation_charge_ex_vat, 0)::numeric),
              ('other_pay_ex_vat', COALESCE(v_current_tsfin.other_pay_ex_vat, 0)::numeric),
              ('other_charge_ex_vat', COALESCE(v_current_tsfin.other_charge_ex_vat, 0)::numeric),
              ('expenses_pay_ex_vat', COALESCE(v_current_tsfin.expenses_pay_ex_vat, 0)::numeric),
              ('expenses_charge_ex_vat', COALESCE(v_current_tsfin.expenses_charge_ex_vat, 0)::numeric)
            ) AS paid_expense_guard(field_name, current_value)
            WHERE v_tsfin_snapshot_json ? paid_expense_guard.field_name
              AND (
                CASE
                  WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>paid_expense_guard.field_name, '')), '') ~ v_numeric_re
                    THEN (v_tsfin_snapshot_json->>paid_expense_guard.field_name)::numeric
                  ELSE 0::numeric
                END
              ) IS DISTINCT FROM paid_expense_guard.current_value
          )
          OR (
            v_tsfin_snapshot_json ? 'expenses_description'
            AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'expenses_description', '')), '')
                IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_current_tsfin.expenses_description, '')), '')
          )
        )
        INTO v_paid_direct_expense_change;
      END IF;

      IF v_current_tsfin.locked_by_invoice_id IS NOT NULL
         OR COALESCE(v_segment_invoice_lock, FALSE) = TRUE
         OR COALESCE(v_paid_direct_expense_change, FALSE) = TRUE THEN
        RAISE EXCEPTION 'TIMESHEET_LOCKED_OR_PAID';
      END IF;
    END IF;
  END IF;

  IF v_current_ts.timesheet_id IS NULL THEN
    IF v_create_json IS NULL THEN
      RAISE EXCEPTION 'p_timesheet_create_json is required when no current timesheet exists';
    END IF;

    v_create_rec := jsonb_populate_record(NULL::public.timesheets, v_create_json);

    IF NULLIF(BTRIM(COALESCE(v_create_rec.booking_id, '')), '') IS NULL THEN
      RAISE EXCEPTION 'booking_id is required for create';
    END IF;

    IF COALESCE(v_create_rec.week_ending_date, v_week.week_ending_date) IS DISTINCT FROM v_week.week_ending_date THEN
      RAISE EXCEPTION 'week_ending_date must match contract_weeks.week_ending_date';
    END IF;

    IF COALESCE(v_create_rec.contract_id, v_week.contract_id) IS DISTINCT FROM v_week.contract_id THEN
      RAISE EXCEPTION 'contract_id must match contract_weeks.contract_id';
    END IF;

    v_create_rec.booking_id := NULLIF(BTRIM(COALESCE(v_create_rec.booking_id, '')), '');
    v_create_rec.version := COALESCE(v_create_rec.version, 1);
    v_original_booking_id := v_create_rec.booking_id;

    PERFORM pg_advisory_xact_lock(hashtext(v_original_booking_id));

    SELECT ts_existing.*
      INTO v_existing_booking_ts
    FROM public.timesheets AS ts_existing
    WHERE ts_existing.booking_id = v_original_booking_id
    ORDER BY
      CASE WHEN COALESCE(ts_existing.is_current, FALSE) = TRUE THEN 0 ELSE 1 END ASC,
      ts_existing.version DESC NULLS LAST,
      ts_existing.updated_at DESC NULLS LAST,
      ts_existing.created_at DESC NULLS LAST,
      ts_existing.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;

    IF FOUND THEN
      v_existing_booking_cw_id := NULL;
      v_existing_booking_cw_contract_id := NULL;
      v_existing_booking_cw_week_ending_date := NULL;

      SELECT cw_existing.id,
             cw_existing.contract_id,
             cw_existing.week_ending_date
        INTO v_existing_booking_cw_id,
             v_existing_booking_cw_contract_id,
             v_existing_booking_cw_week_ending_date
      FROM public.contract_weeks AS cw_existing
      WHERE cw_existing.timesheet_id = v_existing_booking_ts.timesheet_id
      ORDER BY
        CASE WHEN cw_existing.id = v_week.id THEN 0 ELSE 1 END ASC,
        cw_existing.updated_at DESC NULLS LAST,
        cw_existing.created_at DESC NULLS LAST,
        cw_existing.id DESC
      LIMIT 1
      FOR UPDATE;

      v_existing_booking_same_context :=
        v_existing_booking_ts.contract_id IS NOT DISTINCT FROM v_week.contract_id
        AND v_existing_booking_ts.week_ending_date IS NOT DISTINCT FROM v_week.week_ending_date
        AND v_existing_booking_cw_id IS NOT NULL
        AND v_existing_booking_cw_id IS NOT DISTINCT FROM v_week.id;

      IF v_existing_booking_same_context THEN
        IF COALESCE(v_existing_booking_ts.is_current, FALSE) = TRUE THEN
          v_current_ts := v_existing_booking_ts;
        ELSE
          SELECT ts_current_existing.*
            INTO v_current_ts
          FROM public.timesheets AS ts_current_existing
          WHERE ts_current_existing.booking_id = v_original_booking_id
            AND ts_current_existing.is_current = TRUE
          ORDER BY ts_current_existing.version DESC NULLS LAST,
                   ts_current_existing.updated_at DESC NULLS LAST,
                   ts_current_existing.created_at DESC NULLS LAST,
                   ts_current_existing.timesheet_id DESC
          LIMIT 1
          FOR UPDATE;

          IF NOT FOUND THEN
            v_current_ts := NULL;
            SELECT COALESCE(MAX(ts_version_existing.version), 0) + 1
              INTO v_create_rec.version
            FROM public.timesheets AS ts_version_existing
            WHERE ts_version_existing.booking_id = v_original_booking_id;
            v_create_rec.booking_id := v_original_booking_id;
          END IF;
        END IF;

        IF v_current_ts.timesheet_id IS NOT NULL THEN
          v_was_stale := TRUE;
          v_patch_json :=
            (
              v_create_json
              - 'timesheet_id'
              - 'booking_id'
              - 'version'
              - 'is_current'
              - 'contract_id'
              - 'week_ending_date'
              - 'created_at'
            ) || COALESCE(v_patch_json, '{}'::jsonb);
        END IF;
      ELSE
        v_booking_retry := 0;
        LOOP
          v_booking_retry := v_booking_retry + 1;

          IF v_booking_retry > 25 THEN
            RAISE EXCEPTION USING
              MESSAGE = 'BOOKING_ID_COLLISION',
              DETAIL = jsonb_build_object(
                'reason', 'Unable to derive a collision-free weekly manual booking_id for contract_week.',
                'supplied_booking_id', v_original_booking_id,
                'contract_week_id', v_week.id::text,
                'target_contract_id', v_week.contract_id::text,
                'target_week_ending_date', v_week.week_ending_date::text,
                'existing_timesheet_id', v_existing_booking_ts.timesheet_id::text,
                'existing_contract_id', CASE WHEN v_existing_booking_ts.contract_id IS NULL THEN NULL ELSE v_existing_booking_ts.contract_id::text END,
                'existing_week_ending_date', CASE WHEN v_existing_booking_ts.week_ending_date IS NULL THEN NULL ELSE v_existing_booking_ts.week_ending_date::text END,
                'existing_contract_week_id', CASE WHEN v_existing_booking_cw_id IS NULL THEN NULL ELSE v_existing_booking_cw_id::text END
              )::text;
          END IF;

          IF v_booking_retry = 1 THEN
            v_candidate_booking_id := 'bk_cw_' || REPLACE(v_week.id::text, '-', '');
          ELSE
            v_candidate_booking_id := 'bk_cw_' || REPLACE(v_week.id::text, '-', '') || '_' || SUBSTRING(MD5(
              v_original_booking_id ||
              '|contract_week_id=' || v_week.id::text ||
              '|contract_id=' || v_week.contract_id::text ||
              '|week_ending_date=' || v_week.week_ending_date::text ||
              '|try=' || v_booking_retry::text
            ) FROM 1 FOR 16);
          END IF;

          PERFORM pg_advisory_xact_lock(hashtext(v_candidate_booking_id));

          SELECT ts_candidate.*
            INTO v_existing_booking_ts
          FROM public.timesheets AS ts_candidate
          WHERE ts_candidate.booking_id = v_candidate_booking_id
          ORDER BY
            CASE WHEN COALESCE(ts_candidate.is_current, FALSE) = TRUE THEN 0 ELSE 1 END ASC,
            ts_candidate.version DESC NULLS LAST,
            ts_candidate.updated_at DESC NULLS LAST,
            ts_candidate.created_at DESC NULLS LAST,
            ts_candidate.timesheet_id DESC
          LIMIT 1
          FOR UPDATE;

          IF NOT FOUND THEN
            v_create_rec.booking_id := v_candidate_booking_id;
            EXIT;
          END IF;

          v_existing_booking_cw_id := NULL;
          v_existing_booking_cw_contract_id := NULL;
          v_existing_booking_cw_week_ending_date := NULL;

          SELECT cw_candidate.id,
                 cw_candidate.contract_id,
                 cw_candidate.week_ending_date
            INTO v_existing_booking_cw_id,
                 v_existing_booking_cw_contract_id,
                 v_existing_booking_cw_week_ending_date
          FROM public.contract_weeks AS cw_candidate
          WHERE cw_candidate.timesheet_id = v_existing_booking_ts.timesheet_id
          ORDER BY
            CASE WHEN cw_candidate.id = v_week.id THEN 0 ELSE 1 END ASC,
            cw_candidate.updated_at DESC NULLS LAST,
            cw_candidate.created_at DESC NULLS LAST,
            cw_candidate.id DESC
          LIMIT 1
          FOR UPDATE;

          v_existing_booking_same_context :=
            v_existing_booking_ts.contract_id IS NOT DISTINCT FROM v_week.contract_id
            AND v_existing_booking_ts.week_ending_date IS NOT DISTINCT FROM v_week.week_ending_date
            AND v_existing_booking_cw_id IS NOT NULL
            AND v_existing_booking_cw_id IS NOT DISTINCT FROM v_week.id;

          IF v_existing_booking_same_context THEN
            IF COALESCE(v_existing_booking_ts.is_current, FALSE) = TRUE THEN
              v_current_ts := v_existing_booking_ts;
            ELSE
              SELECT ts_current_candidate.*
                INTO v_current_ts
              FROM public.timesheets AS ts_current_candidate
              WHERE ts_current_candidate.booking_id = v_candidate_booking_id
                AND ts_current_candidate.is_current = TRUE
              ORDER BY ts_current_candidate.version DESC NULLS LAST,
                       ts_current_candidate.updated_at DESC NULLS LAST,
                       ts_current_candidate.created_at DESC NULLS LAST,
                       ts_current_candidate.timesheet_id DESC
              LIMIT 1
              FOR UPDATE;

              IF NOT FOUND THEN
                v_current_ts := NULL;
                SELECT COALESCE(MAX(ts_version_candidate.version), 0) + 1
                  INTO v_create_rec.version
                FROM public.timesheets AS ts_version_candidate
                WHERE ts_version_candidate.booking_id = v_candidate_booking_id;
                v_create_rec.booking_id := v_candidate_booking_id;
              END IF;
            END IF;

            IF v_current_ts.timesheet_id IS NOT NULL THEN
              v_was_stale := TRUE;
              v_patch_json :=
                (
                  v_create_json
                  - 'timesheet_id'
                  - 'booking_id'
                  - 'version'
                  - 'is_current'
                  - 'contract_id'
                  - 'week_ending_date'
                  - 'created_at'
                ) || COALESCE(v_patch_json, '{}'::jsonb);
            END IF;
            EXIT;
          END IF;
        END LOOP;
      END IF;
    END IF;

    IF v_current_ts.timesheet_id IS NULL THEN
      BEGIN
        INSERT INTO public.timesheets (
      timesheet_id,
      booking_id,
      occupant_key_norm,
      hospital_norm,
      ward_norm,
      job_title_norm,
      shift_label_norm,
      week_ending_date,
      authorised_at_server,
      status,
      created_at,
      updated_at,
      version,
      is_current,
      contract_id,
      submission_mode,
      manual_pdf_r2_key,
      line_type,
      sheet_scope,
      actual_schedule_json,
      additional_units_week,
      additional_units_per_day,
      qr_token,
      qr_status,
      qr_payload_json,
      qr_generated_at,
      qr_scanned_at,
      qr_scan_info_json,
      qr_r2_key,
      day_references_json,
      manual_pdf_rotation_degrees,
      qr_last_sent_hash,
      qr_last_sent_at_utc,
      qr_signed_hash,
      qr_signed_at_utc,
      is_adjustment
    )
    VALUES (
      COALESCE(v_create_rec.timesheet_id, gen_random_uuid()),
      v_create_rec.booking_id,
      v_create_rec.occupant_key_norm,
      v_create_rec.hospital_norm,
      v_create_rec.ward_norm,
      v_create_rec.job_title_norm,
      COALESCE(v_create_rec.shift_label_norm, 'weekly'),
      v_week.week_ending_date,
      v_create_rec.authorised_at_server,
      COALESCE(v_create_rec.status, 'RECEIVED'::public.timesheet_status_enum),
      COALESCE(v_create_rec.created_at, v_now),
      COALESCE(v_create_rec.updated_at, v_now),
      COALESCE(v_create_rec.version, 1),
      true,
      v_week.contract_id,
      COALESCE(v_create_rec.submission_mode, 'MANUAL'::public.submission_mode_enum),
      v_create_rec.manual_pdf_r2_key,
      COALESCE(v_create_rec.line_type, 'HOURS'::public.timesheet_line_type_enum),
      COALESCE(v_create_rec.sheet_scope, 'WEEKLY'::public.timesheet_scope_enum),
      COALESCE(v_create_rec.actual_schedule_json, '[]'::jsonb),
      COALESCE(v_create_rec.additional_units_week, '{}'::jsonb),
      COALESCE(v_create_rec.additional_units_per_day, '{}'::jsonb),
      v_create_rec.qr_token,
      v_create_rec.qr_status,
      COALESCE(v_create_rec.qr_payload_json, '{}'::jsonb),
      v_create_rec.qr_generated_at,
      v_create_rec.qr_scanned_at,
      v_create_rec.qr_scan_info_json,
      v_create_rec.qr_r2_key,
      v_create_rec.day_references_json,
      COALESCE(v_create_rec.manual_pdf_rotation_degrees, 0),
      v_create_rec.qr_last_sent_hash,
      v_create_rec.qr_last_sent_at_utc,
      v_create_rec.qr_signed_hash,
      v_create_rec.qr_signed_at_utc,
      CASE
        WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN TRUE
        ELSE COALESCE(v_create_rec.is_adjustment, FALSE)
      END
    )
    RETURNING * INTO v_current_ts;

        v_created_now := true;
      EXCEPTION WHEN unique_violation THEN
        RAISE EXCEPTION USING
          MESSAGE = 'BOOKING_ID_COLLISION',
          DETAIL = jsonb_build_object(
            'reason', 'Unique violation while creating weekly manual timesheet.',
            'constraint', 'timesheets_booking_id_version_uidx',
            'booking_id', v_create_rec.booking_id,
            'version', COALESCE(v_create_rec.version, 1),
            'supplied_booking_id', v_original_booking_id,
            'contract_week_id', v_week.id::text,
            'target_contract_id', v_week.contract_id::text,
            'target_week_ending_date', v_week.week_ending_date::text
          )::text;
      END;
    END IF;
  ELSE
    IF v_rotation_json IS NOT NULL THEN
      IF p_actor_user_id IS NULL THEN
        RAISE EXCEPTION 'p_actor_user_id is required when p_rotation_json is supplied';
      END IF;

      v_rotation_action := upper(NULLIF(BTRIM(COALESCE(v_rotation_json->>'qr_action', '')), ''));
      IF v_rotation_action NOT IN ('INVALIDATE', 'REISSUE', 'REVOKE_TO_MANUAL') THEN
        RAISE EXCEPTION 'p_rotation_json.qr_action must be INVALIDATE, REISSUE, or REVOKE_TO_MANUAL';
      END IF;

      v_rotation_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_rotation_json->>'new_timesheet_id', '')), '')::uuid;
      IF v_rotation_new_timesheet_id IS NULL THEN
        RAISE EXCEPTION 'p_rotation_json.new_timesheet_id is required';
      END IF;

      IF v_current_ts.timesheet_id IS NULL THEN
        RAISE EXCEPTION 'p_rotation_json requires an existing current timesheet';
      END IF;

      IF NULLIF(BTRIM(COALESCE(v_current_ts.booking_id, '')), '') IS NULL THEN
        RAISE EXCEPTION 'Cannot rotate: booking_id missing on current timesheet';
      END IF;

      v_rotation_revoke_reason := NULLIF(BTRIM(COALESCE(v_rotation_json->>'revoke_reason', '')), '');
      v_rotation_pending_qr := v_rotation_action IN ('INVALIDATE', 'REISSUE');

      PERFORM pg_advisory_xact_lock(hashtext(v_current_ts.booking_id));

      SELECT ts.version
      INTO v_next_version
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_current_ts.booking_id
      ORDER BY ts.version DESC, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;

      v_next_version := COALESCE(v_next_version, COALESCE(v_current_ts.version, 1)) + 1;

      UPDATE public.timesheets AS ts
      SET is_current = false,
          status = 'REVOKED'::public.timesheet_status_enum,
          revoked_reason = v_rotation_revoke_reason,
          revoked_by = p_actor_user_id::text,
          updated_at = v_now
      WHERE ts.timesheet_id = v_current_ts.timesheet_id
        AND ts.is_current = true;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'TIMESHEET_ROTATE_REVOKE_FAILED';
      END IF;

      v_rotated_ts := v_current_ts;
      v_rotated_ts.timesheet_id := v_rotation_new_timesheet_id;
      v_rotated_ts.version := v_next_version;
      v_rotated_ts.is_current := true;
      v_rotated_ts.status := 'RECEIVED'::public.timesheet_status_enum;
      v_rotated_ts.revoked_reason := NULL;
      v_rotated_ts.revoked_by := NULL;
      v_rotated_ts.authorised_at_server := NULL;
      v_rotated_ts.qr_token := NULL;
      v_rotated_ts.qr_status := CASE
        WHEN v_rotation_pending_qr THEN 'PENDING'::public.timesheet_qr_status_enum
        ELSE NULL
      END;
      v_rotated_ts.qr_payload_json := '{}'::jsonb;
      v_rotated_ts.qr_generated_at := NULL;
      v_rotated_ts.qr_scanned_at := NULL;
      v_rotated_ts.qr_scan_info_json := NULL;
      v_rotated_ts.qr_r2_key := NULL;
      v_rotated_ts.qr_last_sent_hash := NULL;
      v_rotated_ts.qr_last_sent_at_utc := NULL;
      v_rotated_ts.qr_signed_hash := NULL;
      v_rotated_ts.qr_signed_at_utc := NULL;
      v_rotated_ts.manual_pdf_r2_key := NULL;
      v_rotated_ts.created_at := v_now;
      v_rotated_ts.updated_at := v_now;

      INSERT INTO public.timesheets
      SELECT (v_rotated_ts).*
      RETURNING * INTO v_current_ts;
    END IF;
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL AND COALESCE(v_created_now, FALSE) = FALSE THEN
    IF v_current_ts.authorised_at_server IS NOT NULL THEN
      RAISE EXCEPTION 'TIMESHEET_ALREADY_AUTHORISED';
    END IF;

    SELECT *
      INTO v_current_tsfin
    FROM public.timesheets_financials AS tsfin_reuse_lock
    WHERE tsfin_reuse_lock.timesheet_id = v_current_ts.timesheet_id
      AND tsfin_reuse_lock.is_current = TRUE
    ORDER BY tsfin_reuse_lock.computed_at_utc DESC NULLS LAST,
             tsfin_reuse_lock.created_at DESC NULLS LAST,
             tsfin_reuse_lock.updated_at DESC NULLS LAST,
             tsfin_reuse_lock.id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_current_tsfin.id IS NOT NULL THEN
      v_previous_processing_status := v_current_tsfin.processing_status;

      SELECT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json->'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json->'segments'
            ELSE '[]'::jsonb
          END
        ) AS reuse_invoice_segment(segment_json)
        WHERE NULLIF(BTRIM(COALESCE(reuse_invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) INTO v_segment_invoice_lock;

      v_paid_direct_expense_change := FALSE;

      IF v_current_tsfin.paid_at_utc IS NOT NULL
         AND v_tsfin_snapshot_json IS NOT NULL THEN
        SELECT (
          EXISTS (
            SELECT 1
            FROM (VALUES
              ('mileage_units', COALESCE(v_current_tsfin.mileage_units, 0)::numeric),
              ('mileage_pay_rate', COALESCE(v_current_tsfin.mileage_pay_rate, 0)::numeric),
              ('mileage_charge_rate', COALESCE(v_current_tsfin.mileage_charge_rate, 0)::numeric),
              ('mileage_pay_ex_vat', COALESCE(v_current_tsfin.mileage_pay_ex_vat, 0)::numeric),
              ('mileage_charge_ex_vat', COALESCE(v_current_tsfin.mileage_charge_ex_vat, 0)::numeric),
              ('travel_pay_ex_vat', COALESCE(v_current_tsfin.travel_pay_ex_vat, 0)::numeric),
              ('travel_charge_ex_vat', COALESCE(v_current_tsfin.travel_charge_ex_vat, 0)::numeric),
              ('accommodation_pay_ex_vat', COALESCE(v_current_tsfin.accommodation_pay_ex_vat, 0)::numeric),
              ('accommodation_charge_ex_vat', COALESCE(v_current_tsfin.accommodation_charge_ex_vat, 0)::numeric),
              ('other_pay_ex_vat', COALESCE(v_current_tsfin.other_pay_ex_vat, 0)::numeric),
              ('other_charge_ex_vat', COALESCE(v_current_tsfin.other_charge_ex_vat, 0)::numeric),
              ('expenses_pay_ex_vat', COALESCE(v_current_tsfin.expenses_pay_ex_vat, 0)::numeric),
              ('expenses_charge_ex_vat', COALESCE(v_current_tsfin.expenses_charge_ex_vat, 0)::numeric)
            ) AS paid_expense_guard(field_name, current_value)
            WHERE v_tsfin_snapshot_json ? paid_expense_guard.field_name
              AND (
                CASE
                  WHEN NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>paid_expense_guard.field_name, '')), '') ~ v_numeric_re
                    THEN (v_tsfin_snapshot_json->>paid_expense_guard.field_name)::numeric
                  ELSE 0::numeric
                END
              ) IS DISTINCT FROM paid_expense_guard.current_value
          )
          OR (
            v_tsfin_snapshot_json ? 'expenses_description'
            AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'expenses_description', '')), '')
                IS DISTINCT FROM NULLIF(BTRIM(COALESCE(v_current_tsfin.expenses_description, '')), '')
          )
        )
        INTO v_paid_direct_expense_change;
      END IF;

      IF v_current_tsfin.locked_by_invoice_id IS NOT NULL
         OR COALESCE(v_segment_invoice_lock, FALSE) = TRUE
         OR COALESCE(v_paid_direct_expense_change, FALSE) = TRUE THEN
        RAISE EXCEPTION 'TIMESHEET_LOCKED_OR_PAID';
      END IF;
    END IF;
  END IF;

  IF v_current_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION 'CURRENT_TIMESHEET_NOT_READY';
  END IF;

  IF v_patch_json <> '{}'::jsonb THEN
    v_patch_rec := jsonb_populate_record(v_current_ts, v_patch_json);

    UPDATE public.timesheets AS ts
    SET occupant_key_norm = v_patch_rec.occupant_key_norm,
        hospital_norm = v_patch_rec.hospital_norm,
        ward_norm = v_patch_rec.ward_norm,
        job_title_norm = v_patch_rec.job_title_norm,
        shift_label_norm = v_patch_rec.shift_label_norm,
        authorised_at_server = v_patch_rec.authorised_at_server,
        status = v_patch_rec.status,
        updated_at = CASE
          WHEN v_patch_json ? 'updated_at' THEN COALESCE(v_patch_rec.updated_at, v_now)
          ELSE v_now
        END,
        submission_mode = v_patch_rec.submission_mode,
        manual_pdf_r2_key = v_patch_rec.manual_pdf_r2_key,
        line_type = v_patch_rec.line_type,
        sheet_scope = v_patch_rec.sheet_scope,
        actual_schedule_json = v_patch_rec.actual_schedule_json,
        additional_units_week = COALESCE(v_patch_rec.additional_units_week, '{}'::jsonb),
        additional_units_per_day = COALESCE(v_patch_rec.additional_units_per_day, '{}'::jsonb),
        qr_token = v_patch_rec.qr_token,
        qr_status = v_patch_rec.qr_status,
        qr_payload_json = COALESCE(v_patch_rec.qr_payload_json, '{}'::jsonb),
        qr_generated_at = v_patch_rec.qr_generated_at,
        qr_scanned_at = v_patch_rec.qr_scanned_at,
        qr_scan_info_json = v_patch_rec.qr_scan_info_json,
        qr_r2_key = v_patch_rec.qr_r2_key,
        day_references_json = v_patch_rec.day_references_json,
        manual_pdf_rotation_degrees = COALESCE(v_patch_rec.manual_pdf_rotation_degrees, 0),
        qr_last_sent_hash = v_patch_rec.qr_last_sent_hash,
        qr_last_sent_at_utc = v_patch_rec.qr_last_sent_at_utc,
        qr_signed_hash = v_patch_rec.qr_signed_hash,
        qr_signed_at_utc = v_patch_rec.qr_signed_at_utc,
        is_adjustment = CASE
          WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN TRUE
          ELSE COALESCE(v_patch_rec.is_adjustment, ts.is_adjustment)
        END
    WHERE ts.timesheet_id = v_current_ts.timesheet_id
      AND ts.is_current = true
    RETURNING * INTO v_current_ts;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'TIMESHEET_UPDATE_FAILED';
    END IF;
  END IF;

  v_week_patch_rec := jsonb_populate_record(v_week, v_week_patch_json);

  UPDATE public.contract_weeks AS cw
  SET status = COALESCE(v_week_patch_rec.status, cw.status),
      submission_mode_snapshot = COALESCE(v_week_patch_rec.submission_mode_snapshot, cw.submission_mode_snapshot),
      timesheet_id = v_current_ts.timesheet_id,
      uploaded_pdf_r2_key = COALESCE(v_week_patch_rec.uploaded_pdf_r2_key, cw.uploaded_pdf_r2_key),
      day_entries_json = COALESCE(v_week_patch_rec.day_entries_json, cw.day_entries_json),
      totals_json = COALESCE(v_week_patch_rec.totals_json, cw.totals_json),
      updated_at = CASE
        WHEN v_week_patch_json ? 'updated_at' THEN COALESCE(v_week_patch_rec.updated_at, v_now)
        ELSE v_now
      END,
      planned_schedule_json = COALESCE(v_week_patch_rec.planned_schedule_json, cw.planned_schedule_json),
      is_adjustment = CASE
        WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN TRUE
        ELSE COALESCE(v_week_patch_rec.is_adjustment, cw.is_adjustment)
      END,
      enforce_day_partition = COALESCE(v_week_patch_rec.enforce_day_partition, cw.enforce_day_partition),
      allowed_days_mask = COALESCE(v_week_patch_rec.allowed_days_mask, cw.allowed_days_mask),
      split_boundary_date = COALESCE(v_week_patch_rec.split_boundary_date, cw.split_boundary_date),
      worker_note = COALESCE(v_week_patch_rec.worker_note, cw.worker_note),
      split_group_key = COALESCE(v_week_patch_rec.split_group_key, cw.split_group_key)
  WHERE cw.id = v_week.id
  RETURNING * INTO v_week;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CONTRACT_WEEK_UPDATE_FAILED';
  END IF;

  IF v_created_now AND COALESCE(p_materialise_staged_evidence, true) THEN
    FOR v_queue_item IN
      SELECT *
      FROM public.manual_timesheet_queue AS mq
      WHERE mq.status = 'STAGED'
        AND mq.meta_json->>'contract_week_id' = v_week.id::text
      ORDER BY mq.uploaded_at_utc ASC, mq.id ASC
      FOR UPDATE
    LOOP
      v_queue_kind := upper(
        COALESCE(
          NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'staged_kind', '')), ''),
          NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'kind', '')), ''),
          NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'attached_kind', '')), ''),
          'TIMESHEET'
        )
      );

      IF v_queue_kind NOT IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER') THEN
        v_queue_kind := 'OTHER';
      END IF;

      v_queue_storage_key := NULLIF(
        BTRIM(
          COALESCE(
            NULLIF(BTRIM(COALESCE(v_queue_item.r2_key, '')), ''),
            NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'r2_key', '')), ''),
            NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'storage_key', '')), ''),
            NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'file_key', '')), ''),
            NULLIF(BTRIM(COALESCE(v_queue_item.meta_json->>'canonical_key', '')), ''),
            ''
          )
        ),
        ''
      );
      IF v_queue_storage_key IS NOT NULL THEN
        v_queue_storage_key := NULLIF(regexp_replace(v_queue_storage_key, '^/+', ''), '');
      END IF;

      IF v_queue_kind = 'TIMESHEET' THEN
        IF v_queue_storage_key IS NULL THEN
          RAISE EXCEPTION USING
            MESSAGE = 'INVALID_TIMESHEET_EVIDENCE',
            DETAIL = jsonb_build_object(
              'reason', 'missing_storage_key',
              'queue_id', v_queue_item.id::text,
              'contract_week_id', v_week.id::text
            )::text;
        END IF;

        IF v_primary_timesheet_storage_key IS NULL THEN
          v_primary_timesheet_storage_key := v_queue_storage_key;
          v_primary_timesheet_queue_id := v_queue_item.id;
          v_primary_timesheet_rotation_raw := COALESCE(v_queue_item.last_rotation_deg, 0);
          v_primary_timesheet_rotation_raw := ((v_primary_timesheet_rotation_raw % 360) + 360) % 360;
          v_primary_timesheet_rotation_deg :=
            CASE
              WHEN v_primary_timesheet_rotation_raw >= 315 OR v_primary_timesheet_rotation_raw < 45 THEN 0
              WHEN v_primary_timesheet_rotation_raw >= 45 AND v_primary_timesheet_rotation_raw < 135 THEN 90
              WHEN v_primary_timesheet_rotation_raw >= 135 AND v_primary_timesheet_rotation_raw < 225 THEN 180
              ELSE 270
            END;
        ELSIF v_queue_storage_key IS DISTINCT FROM v_primary_timesheet_storage_key THEN
          RAISE EXCEPTION USING
            MESSAGE = 'MULTIPLE_STAGED_TIMESHEET_FILES',
            DETAIL = jsonb_build_object(
              'reason', 'different_storage_key_conflict',
              'contract_week_id', v_week.id::text,
              'existing_storage_key', v_primary_timesheet_storage_key,
              'conflicting_storage_key', v_queue_storage_key,
              'queue_id', v_queue_item.id::text
            )::text;
        END IF;

        v_timesheet_kind_count := v_timesheet_kind_count + 1;

        IF v_timesheet_kind_count = 1 THEN
          IF NOT EXISTS (
            SELECT 1
            FROM public.timesheet_evidence AS te
            WHERE te.timesheet_id = v_current_ts.timesheet_id
              AND te.kind = 'TIMESHEET'
              AND te.storage_key = v_queue_storage_key
            LIMIT 1
          ) THEN
            INSERT INTO public.timesheet_evidence (
              timesheet_id,
              kind,
              display_name,
              storage_key,
              created_at,
              created_by
            )
            VALUES (
              v_current_ts.timesheet_id,
              'TIMESHEET',
              v_queue_item.original_filename,
              v_queue_storage_key,
              COALESCE(v_queue_item.uploaded_at_utc, v_now),
              COALESCE(v_queue_item.uploaded_by_user_id, p_actor_user_id)
            );
          END IF;
        END IF;

        UPDATE public.manual_timesheet_queue AS mq
        SET status = 'ATTACHED',
            timesheet_id = v_current_ts.timesheet_id,
            r2_key = v_queue_storage_key,
            meta_json = (
              COALESCE(v_queue_item.meta_json, '{}'::jsonb)
                - 'deferred_target_timesheet_id'
                - 'materialisation_deferred_at_utc'
                - 'deferred_rotation_degrees'
                - 'dematerialised_from_timesheet_id'
                - 'dematerialised_from_booking_id'
                - 'dematerialised_at_utc'
            ) || jsonb_build_object(
              'contract_week_id', v_week.id::text,
              'staged_kind', 'TIMESHEET',
              'materialisation_deferred_to_backend', false,
              'materialised_to_timesheet_id', v_current_ts.timesheet_id::text,
              'materialised_at_utc', to_jsonb(v_now),
              'materialised_storage_key', v_queue_storage_key,
              'duplicate_timesheet_evidence_identity', (v_timesheet_kind_count > 1),
              'duplicate_of_queue_item_id', CASE WHEN v_timesheet_kind_count > 1 THEN v_primary_timesheet_queue_id::text ELSE NULL END,
              'materialisation_noop_reason', CASE WHEN v_timesheet_kind_count > 1 THEN 'same_storage_key_duplicate' ELSE NULL END
            )
        WHERE mq.id = v_queue_item.id;
      ELSE
        INSERT INTO public.timesheet_evidence (
          timesheet_id,
          kind,
          display_name,
          storage_key,
          created_at,
          created_by
        )
        VALUES (
          v_current_ts.timesheet_id,
          v_queue_kind,
          v_queue_item.original_filename,
          COALESCE(v_queue_storage_key, v_queue_item.r2_key),
          COALESCE(v_queue_item.uploaded_at_utc, v_now),
          COALESCE(v_queue_item.uploaded_by_user_id, p_actor_user_id)
        );

        UPDATE public.manual_timesheet_queue AS mq
        SET status = 'ATTACHED',
            timesheet_id = v_current_ts.timesheet_id,
            meta_json = (
              COALESCE(v_queue_item.meta_json, '{}'::jsonb)
                - 'deferred_target_timesheet_id'
                - 'materialisation_deferred_at_utc'
                - 'deferred_rotation_degrees'
            ) || jsonb_build_object(
              'contract_week_id', v_week.id::text,
              'staged_kind', v_queue_kind,
              'materialisation_deferred_to_backend', false,
              'materialised_to_timesheet_id', v_current_ts.timesheet_id::text,
              'materialised_at_utc', to_jsonb(v_now),
              'materialised_storage_key', COALESCE(v_queue_storage_key, v_queue_item.r2_key)
            )
        WHERE mq.id = v_queue_item.id;
      END IF;
    END LOOP;
  END IF;

  IF v_primary_timesheet_storage_key IS NOT NULL THEN
    UPDATE public.timesheets AS ts
    SET manual_pdf_r2_key = v_primary_timesheet_storage_key,
        manual_pdf_rotation_degrees = v_primary_timesheet_rotation_deg,
        updated_at = v_now
    WHERE ts.timesheet_id = v_current_ts.timesheet_id
      AND ts.is_current = true
    RETURNING * INTO v_current_ts;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'TIMESHEET_MANUAL_PDF_UPDATE_FAILED';
    END IF;
  END IF;

  IF v_tsfin_snapshot_json IS NULL THEN
    RAISE EXCEPTION 'p_tsfin_snapshot_json is required';
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'client_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'client_id', '')), '')::uuid IS DISTINCT FROM v_contract.client_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH',
      DETAIL = jsonb_build_object(
        'field', 'client_id',
        'expected_value', v_contract.client_id::text,
        'supplied_value', v_tsfin_snapshot_json->>'client_id',
        'contract_week_id', v_week.id::text,
        'timesheet_id', v_current_ts.timesheet_id::text
      )::text;
  END IF;

  IF v_contract.candidate_id IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'candidate_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json->>'candidate_id', '')), '')::uuid IS DISTINCT FROM v_contract.candidate_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH',
      DETAIL = jsonb_build_object(
        'field', 'candidate_id',
        'expected_value', v_contract.candidate_id::text,
        'supplied_value', v_tsfin_snapshot_json->>'candidate_id',
        'contract_week_id', v_week.id::text,
        'timesheet_id', v_current_ts.timesheet_id::text
      )::text;
  END IF;

  -- Do not compare the intended next TSFIN snapshot to the old current TSFIN for
  -- mutable financial values such as hours, rates, totals, expenses, mileage, or margin.
  -- Stale protection for this write is enforced by expected_timesheet_id, backend row
  -- signature, authorisation/invoice/payment locks, and invariant candidate/client checks.

  IF COALESCE(NULLIF(v_tsfin_snapshot_json->>'mileage_units', '')::numeric, 0) > 0
     OR COALESCE(NULLIF(v_tsfin_snapshot_json->>'mileage_pay_ex_vat', '')::numeric, 0) > 0
     OR COALESCE(NULLIF(v_tsfin_snapshot_json->>'mileage_charge_ex_vat', '')::numeric, 0) > 0 THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_evidence AS te
      WHERE te.timesheet_id = v_current_ts.timesheet_id
        AND te.kind = 'MILEAGE'
      LIMIT 1
    ) INTO v_has_evidence;

    IF NOT COALESCE(v_has_evidence, false) THEN
      v_missing := v_missing || jsonb_build_array(
        jsonb_build_object('category', 'mileage', 'required_kind', 'MILEAGE')
      );
    END IF;
  END IF;

  IF COALESCE(NULLIF(v_tsfin_snapshot_json->>'travel_pay_ex_vat', '')::numeric, 0) > 0
     OR COALESCE(NULLIF(v_tsfin_snapshot_json->>'travel_charge_ex_vat', '')::numeric, 0) > 0 THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_evidence AS te
      WHERE te.timesheet_id = v_current_ts.timesheet_id
        AND te.kind = 'TRAVEL'
      LIMIT 1
    ) INTO v_has_evidence;

    IF NOT COALESCE(v_has_evidence, false) THEN
      v_missing := v_missing || jsonb_build_array(
        jsonb_build_object('category', 'travel', 'required_kind', 'TRAVEL')
      );
    END IF;
  END IF;

  IF COALESCE(NULLIF(v_tsfin_snapshot_json->>'accommodation_pay_ex_vat', '')::numeric, 0) > 0
     OR COALESCE(NULLIF(v_tsfin_snapshot_json->>'accommodation_charge_ex_vat', '')::numeric, 0) > 0 THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_evidence AS te
      WHERE te.timesheet_id = v_current_ts.timesheet_id
        AND te.kind = 'ACCOMMODATION'
      LIMIT 1
    ) INTO v_has_evidence;

    IF NOT COALESCE(v_has_evidence, false) THEN
      v_missing := v_missing || jsonb_build_array(
        jsonb_build_object('category', 'accommodation', 'required_kind', 'ACCOMMODATION')
      );
    END IF;
  END IF;

  IF COALESCE(NULLIF(v_tsfin_snapshot_json->>'other_pay_ex_vat', '')::numeric, 0) > 0
     OR COALESCE(NULLIF(v_tsfin_snapshot_json->>'other_charge_ex_vat', '')::numeric, 0) > 0 THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.timesheet_evidence AS te
      WHERE te.timesheet_id = v_current_ts.timesheet_id
        AND te.kind = 'OTHER'
      LIMIT 1
    ) INTO v_has_evidence;

    IF NOT COALESCE(v_has_evidence, false) THEN
      v_missing := v_missing || jsonb_build_array(
        jsonb_build_object('category', 'other', 'required_kind', 'OTHER')
      );
    END IF;
  END IF;

  IF jsonb_array_length(v_missing) > 0 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EVIDENCE_REQUIRED',
      DETAIL = jsonb_build_object('missing', v_missing)::text;
  END IF;

  v_tsfin_snapshot_json :=
    v_tsfin_snapshot_json
    || jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id::text,
      'timesheet_version', v_current_ts.version
    );

  PERFORM public.enqueue_ts_financials_priority(
    ARRAY[v_current_ts.timesheet_id]::uuid[],
    'CONTEXT_CHANGED'::public.ts_fin_reason_enum
  );

  SELECT *
  INTO v_outbox
  FROM public.tsfin_dequeue_specific(
    ARRAY[v_current_ts.timesheet_id]::uuid[],
    1
  )
  LIMIT 1;

  IF v_outbox.id IS NULL THEN
    RAISE EXCEPTION 'TSFIN_OUTBOX_DEQUEUE_FAILED';
  END IF;

  SELECT wr.ok_count, wr.fail_count, wr.errors
  INTO v_write_result
  FROM public.tsfin_write_snapshots_and_complete(
    jsonb_build_array(
      jsonb_build_object(
        'outbox_id', v_outbox.id::text,
        'timesheet_id', v_current_ts.timesheet_id::text,
        'snapshot', v_tsfin_snapshot_json
      )
    )
  ) AS wr;

  IF COALESCE(v_write_result.fail_count, 0) > 0 OR COALESCE(v_write_result.ok_count, 0) <> 1 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TSFIN_WRITE_FAILED',
      DETAIL = COALESCE(v_write_result.errors, '[]'::jsonb)::text;
  END IF;

  SELECT *
  INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = v_current_ts.timesheet_id
    AND ts.is_current = true
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CURRENT_TIMESHEET_NOT_FOUND_AFTER_WRITE';
  END IF;

  SELECT *
  INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_FINANCIALS_CURRENT_NOT_FOUND';
  END IF;

  SELECT decision_result.row_json
    INTO v_post_decision_row
  FROM public.bulk_timesheet_row_decision_v1(JSONB_BUILD_OBJECT(
    'dataset_mode', 'process',
    'timesheet_id', v_current_ts.timesheet_id::text,
    'contract_week_id', v_week.id::text
  )) AS decision_result(row_json)
  LIMIT 1;

  v_new_row_signature := NULLIF(BTRIM(COALESCE(v_post_decision_row->>'row_signature', '')), '');

  PERFORM public._audit_insert(
    'contract_week',
    v_week.id::text,
    CASE
      WHEN v_created_now THEN 'CONTRACT_WEEK_MANUAL_TIMESHEET_CREATED_PROCESSED'
      ELSE 'CONTRACT_WEEK_MANUAL_TIMESHEET_UPDATED_PROCESSED'
    END,
    JSONB_BUILD_OBJECT(
      'actor_user_id', p_actor_user_id,
      'contract_week_id', v_week.id,
      'previous_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END,
      'current_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END,
      'previous_processing_status', v_previous_processing_status,
      'previous_row_signature', v_previous_row_signature,
      'row', COALESCE(v_pre_decision_row, '{}'::jsonb)
    ),
    JSONB_BUILD_OBJECT(
      'actor_user_id', p_actor_user_id,
      'contract_week_id', v_week.id,
      'previous_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'new_processing_status', v_current_tsfin.processing_status,
      'new_row_signature', v_new_row_signature,
      'operation', CASE WHEN v_created_now THEN 'CREATE_PROCESS' ELSE 'UPDATE_PROCESS' END,
      'row', COALESCE(v_post_decision_row, '{}'::jsonb)
    ),
    'WEEKLY_MANUAL_PROCESS',
    p_actor_user_id
  );

  contract_week_id := v_week.id;
  contract_id := v_week.contract_id;
  timesheet_id := v_current_ts.timesheet_id;
  current_timesheet_id := v_current_ts.timesheet_id;
  current_timesheet_version := v_current_ts.version;
  was_stale := v_was_stale;
  created_now := v_created_now;
  processing_status := v_current_tsfin.processing_status;
  contract_week_json := to_jsonb(v_week);
  timesheet_json := to_jsonb(v_current_ts);
  timesheet_financials_json := to_jsonb(v_current_tsfin);

  RETURN NEXT;
END;
$function$;



DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_bulk_process_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone);

DROP FUNCTION IF EXISTS public.contract_week_manual_upsert_bulk_process_atomic(uuid, uuid, jsonb, jsonb, jsonb, jsonb, jsonb, uuid, boolean, timestamp with time zone, text);


CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_bulk_process_atomic(
  p_week_id uuid,
  p_expected_timesheet_id uuid DEFAULT NULL,
  p_timesheet_create_json jsonb DEFAULT NULL,
  p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb,
  p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb,
  p_tsfin_snapshot_json jsonb DEFAULT NULL,
  p_rotation_json jsonb DEFAULT NULL,
  p_actor_user_id uuid DEFAULT NULL,
  p_materialise_staged_evidence boolean DEFAULT true,
  p_now_utc timestamp with time zone DEFAULT now(),
  p_expected_row_signature text DEFAULT NULL,
  p_expected_current_tsfin_snapshot_json jsonb DEFAULT NULL,
  p_next_tsfin_snapshot_json jsonb DEFAULT NULL,
  p_response_context text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_contract_week_id uuid := NULL;
  v_contract_id uuid := NULL;
  v_timesheet_id uuid := NULL;
  v_current_timesheet_id uuid := NULL;
  v_current_timesheet_version integer := NULL;
  v_was_stale boolean := FALSE;
  v_created_now boolean := FALSE;
  v_processing_status public.ts_fin_processing_status_enum := NULL;
  v_contract_week_json jsonb := NULL;
  v_timesheet_json jsonb := NULL;
  v_timesheet_financials_json jsonb := NULL;
  v_pre_row jsonb := NULL;
  v_post_row jsonb := NULL;
  v_row_patch jsonb := '{}'::jsonb;
  v_cache_invalidation_hints jsonb := '{}'::jsonb;
  v_cache_invalidation jsonb := '{}'::jsonb;
  v_pre_bucket text := NULL;
  v_post_bucket text := NULL;
  v_pre_authorise_section text := NULL;
  v_post_authorise_section text := NULL;
  v_unprocessed_delta integer := 0;
  v_processed_delta integer := 0;
  v_total_delta integer := 0;
  v_previous_storage_key text := NULL;
  v_primary_storage_key text := NULL;
  v_primary_artifact_id text := NULL;
  v_primary_artifact_kind text := NULL;
  v_primary_artifact_display_name text := NULL;
  v_primary_artifact_preview_mode text := NULL;
  v_row_key text := NULL;
  v_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_current_row_signature text := NULL;
  v_previous_row_key text := NULL;
  v_preview_changed boolean := FALSE;
  v_error_sqlstate text := NULL;
  v_error_message text := NULL;
  v_error_code text := NULL;
  v_exception_detail text := NULL;
  v_exception_hint text := NULL;
  v_detail_json jsonb := NULL;
  v_missing jsonb := '[]'::jsonb;
  v_error_current_timesheet_id uuid := NULL;
  v_rotation_action text := NULL;
  v_rotation_applied boolean := FALSE;
  v_rotation_old_timesheet_id text := NULL;
  v_rotation_new_timesheet_id text := NULL;
  v_rotation_old_version integer := NULL;
  v_rotation_new_version integer := NULL;
  v_pre_current_timesheet_id_text text := NULL;
  v_pre_current_version_text text := NULL;
  v_patch_dataset_mode text := CASE
    WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'authorise'
    ELSE 'process'
  END;
  v_pre_patch_filters jsonb := '{}'::jsonb;
  v_post_patch_filters jsonb := '{}'::jsonb;
  v_atomic_guard_row jsonb := NULL;
  v_atomic_expected_row_signature text := NULL;
  v_response_context text := CASE
    WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'bulk_authorise'
    ELSE 'bulk_process'
  END;
  v_operation text := CASE
    WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'contract_week_manual_upsert_bulk_authorise'
    ELSE 'contract_week_manual_upsert_bulk_process'
  END;
  v_next_tsfin_snapshot_json jsonb := COALESCE(p_next_tsfin_snapshot_json, p_tsfin_snapshot_json);
  v_is_additional_manual_adjustment boolean := FALSE;
  v_additional_seq integer := 0;
  v_post_route_type text := NULL;
  v_post_route_family text := NULL;
  v_post_route_subfamily text := NULL;
  v_effective_actual_schedule_json jsonb := NULL;
  v_effective_planned_schedule_json jsonb := NULL;
  v_effective_total_hours numeric := NULL;
  v_keep_empty_additional_schedule boolean := FALSE;
  v_defer_post_patch boolean := LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_process_defer_patch';
BEGIN
  IF p_week_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', v_operation,
      'success', FALSE,
      'error_code', 'CONTRACT_WEEK_ID_REQUIRED',
      'message', 'p_week_id is required.'
    );
  END IF;

  IF p_rotation_json IS NOT NULL AND jsonb_typeof(p_rotation_json) = 'object' THEN
    v_rotation_action := UPPER(BTRIM(COALESCE(p_rotation_json->>'qr_action', '')));
    IF v_rotation_action = '' THEN
      v_rotation_action := NULL;
    END IF;
  END IF;

  v_pre_patch_filters := JSONB_BUILD_OBJECT(
    'dataset_mode', v_patch_dataset_mode,
    'contract_week_id', p_week_id::text
  );

  IF p_expected_timesheet_id IS NOT NULL THEN
    v_pre_patch_filters := v_pre_patch_filters || JSONB_BUILD_OBJECT(
      'timesheet_id', p_expected_timesheet_id::text
    );
  END IF;

  SELECT decision_result.row_json
    INTO v_pre_row
  FROM public.bulk_timesheet_row_patch_v1(v_pre_patch_filters) AS decision_result(row_json)
  LIMIT 1;

  v_pre_bucket := NULLIF(BTRIM(COALESCE(v_pre_row->>'bulk_process_bucket', '')), '');
  v_pre_authorise_section := NULLIF(BTRIM(COALESCE(v_pre_row->>'bulk_authorise_section', '')), '');
  v_previous_row_key := NULLIF(BTRIM(COALESCE(v_pre_row->>'row_key', '')), '');
  v_previous_storage_key := NULLIF(BTRIM(COALESCE(v_pre_row->>'primary_artifact_storage_key', '')), '');
  v_pre_current_timesheet_id_text := NULLIF(BTRIM(COALESCE(v_pre_row->>'current_timesheet_id', v_pre_row->>'timesheet_id', p_expected_timesheet_id::text, '')), '');
  v_pre_current_version_text := NULLIF(BTRIM(COALESCE(v_pre_row->>'current_version', v_pre_row->>'current_timesheet_version', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_pre_row->>'row_signature', '')), '');

  IF v_expected_row_signature IS NOT NULL AND v_pre_row IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', v_operation,
      'success', FALSE,
      'error_code', 'ROW_SIGNATURE_LOOKUP_FAILED',
      'message', 'The current row could not be resolved for the supplied timesheet and contract week. Refresh the row and try again.',
      'expected_row_signature', v_expected_row_signature,
      'current_row_signature', NULL,
      'current_timesheet_id', p_expected_timesheet_id,
      'contract_week_id', p_week_id,
      'previous_row_key', CASE WHEN p_expected_timesheet_id IS NULL THEN NULL ELSE 'timesheet:' || p_expected_timesheet_id::text END,
      'refresh_required', TRUE,
      'cache_invalidation_hints', JSONB_BUILD_OBJECT(
        'row_keys', CASE WHEN p_expected_timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY('timesheet:' || p_expected_timesheet_id::text) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
        'timesheet_ids', CASE WHEN p_expected_timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(p_expected_timesheet_id) END,
        'storage_keys', '[]'::jsonb,
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'invalidate_context', TRUE,
        'invalidate_preview', FALSE,
        'refresh_required', TRUE
      ),
      'cache_invalidation', JSONB_BUILD_OBJECT(
        'rows', CASE WHEN p_expected_timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
          'previous_row_key', 'timesheet:' || p_expected_timesheet_id::text,
          'contract_week_id', p_week_id,
          'timesheet_id', p_expected_timesheet_id
        )) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
        'timesheet_ids', CASE WHEN p_expected_timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(p_expected_timesheet_id) END,
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'refresh_required', TRUE
      )
    );
  END IF;

  IF v_expected_row_signature IS NOT NULL
     AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', v_operation,
      'success', FALSE,
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'message', 'Contract week row changed after it was loaded. Refresh the row and try again.',
      'expected_row_signature', v_expected_row_signature,
      'current_row_signature', v_current_row_signature,
      'current_timesheet_id', NULLIF(BTRIM(COALESCE(v_pre_row->>'current_timesheet_id', v_pre_row->>'timesheet_id', '')), '')::uuid,
      'contract_week_id', p_week_id,
      'previous_row_key', v_previous_row_key,
      'refresh_required', TRUE,
      'cache_invalidation_hints', JSONB_BUILD_OBJECT(
        'row_keys', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_row_key) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
        'timesheet_ids', CASE WHEN v_pre_current_timesheet_id_text IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text) END,
        'storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'invalidate_context', TRUE,
        'invalidate_preview', FALSE,
        'refresh_required', TRUE
      ),
      'cache_invalidation', JSONB_BUILD_OBJECT(
        'rows', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
          'previous_row_key', v_previous_row_key,
          'contract_week_id', p_week_id,
          'timesheet_id', CASE WHEN v_pre_current_timesheet_id_text IS NULL THEN NULL ELSE v_pre_current_timesheet_id_text END
        )) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
        'timesheet_ids', CASE WHEN v_pre_current_timesheet_id_text IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text) END,
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'refresh_required', TRUE
      )
    );
  END IF;

  IF v_pre_current_version_text ~ '^[0-9]+$' THEN
    v_rotation_old_version := v_pre_current_version_text::integer;
  END IF;

  SELECT upsert_result.contract_week_id,
         upsert_result.contract_id,
         upsert_result.timesheet_id,
         upsert_result.current_timesheet_id,
         upsert_result.current_timesheet_version,
         upsert_result.was_stale,
         upsert_result.created_now,
         upsert_result.processing_status,
         upsert_result.contract_week_json,
         upsert_result.timesheet_json,
         upsert_result.timesheet_financials_json
    INTO v_contract_week_id,
         v_contract_id,
         v_timesheet_id,
         v_current_timesheet_id,
         v_current_timesheet_version,
         v_was_stale,
         v_created_now,
         v_processing_status,
         v_contract_week_json,
         v_timesheet_json,
         v_timesheet_financials_json
  FROM public.contract_week_manual_upsert_atomic(
    p_week_id => p_week_id,
    p_expected_timesheet_id => p_expected_timesheet_id,
    p_timesheet_create_json => p_timesheet_create_json,
    p_timesheet_patch_json => p_timesheet_patch_json,
    p_contract_week_patch_json => p_contract_week_patch_json,
    p_tsfin_snapshot_json => v_next_tsfin_snapshot_json,
    p_rotation_json => p_rotation_json,
    p_actor_user_id => p_actor_user_id,
    p_materialise_staged_evidence => p_materialise_staged_evidence,
    p_now_utc => v_now,
    p_expected_row_signature => NULL
  ) AS upsert_result;

  IF v_contract_week_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', v_operation,
      'success', FALSE,
      'error_code', 'UPSERT_RETURNED_NO_ROW',
      'message', 'contract_week_manual_upsert_atomic did not return a row.',
      'contract_week_id', p_week_id
    );
  END IF;

  IF v_defer_post_patch THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', TRUE,
      'operation', v_operation,
      'success', TRUE,
      'contract_week_id', v_contract_week_id,
      'contract_id', v_contract_id,
      'timesheet_id', v_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'expected_timesheet_id', v_current_timesheet_id,
      'current_version', v_current_timesheet_version,
      'current_timesheet_version', v_current_timesheet_version,
      'was_stale', COALESCE(v_was_stale, FALSE),
      'created_now', COALESCE(v_created_now, FALSE),
      'processing_status', v_processing_status,
      'contract_week', COALESCE(v_contract_week_json, NULL::jsonb),
      'contract_week_json', COALESCE(v_contract_week_json, NULL::jsonb),
      'timesheet', COALESCE(v_timesheet_json, NULL::jsonb),
      'timesheet_json', COALESCE(v_timesheet_json, NULL::jsonb),
      'tsfin', COALESCE(v_timesheet_financials_json, NULL::jsonb),
      'timesheet_financials_json', COALESCE(v_timesheet_financials_json, NULL::jsonb),
      'previous_row_key', v_previous_row_key,
      'requires_final_patch_refresh', TRUE,
      'deferred_bulk_patch', TRUE,
      'response_context', v_response_context,
      'bulk_authorise', FALSE,
      'bulk_process', TRUE,
      'refresh_required', TRUE,
      'cache_invalidation_hints', JSONB_BUILD_OBJECT(
        'row_keys', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_row_key) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(v_contract_week_id),
        'timesheet_ids', JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text, v_timesheet_id, v_current_timesheet_id),
        'storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'identity_changed', TRUE,
        'manual_changed', TRUE,
        'evidence_changed', FALSE,
        'storage_changed', FALSE,
        'invalidate_context', FALSE,
        'invalidate_row_context', FALSE,
        'invalidate_editor_context', TRUE,
        'invalidate_preview', FALSE,
        'invalidate_evidence', FALSE,
        'refresh_required', TRUE,
        'requires_final_patch_refresh', TRUE,
        'deferred_bulk_patch', TRUE
      ),
      'cache_invalidation', JSONB_BUILD_OBJECT(
        'rows', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
          'previous_row_key', v_previous_row_key,
          'contract_week_id', v_contract_week_id,
          'timesheet_id', v_current_timesheet_id
        )) END,
        'contract_week_ids', JSONB_BUILD_ARRAY(v_contract_week_id),
        'timesheet_ids', JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text, v_timesheet_id, v_current_timesheet_id),
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'refresh_required', TRUE,
        'requires_final_patch_refresh', TRUE,
        'deferred_bulk_patch', TRUE
      )
    );
  END IF;

  v_post_patch_filters := JSONB_BUILD_OBJECT(
    'dataset_mode', v_patch_dataset_mode,
    'contract_week_id', v_contract_week_id::text
  );

  IF v_current_timesheet_id IS NOT NULL THEN
    v_post_patch_filters := v_post_patch_filters || JSONB_BUILD_OBJECT(
      'timesheet_id', v_current_timesheet_id::text
    );
  END IF;

  SELECT decision_result.row_json
    INTO v_post_row
  FROM public.bulk_timesheet_row_patch_v1(v_post_patch_filters) AS decision_result(row_json)
  LIMIT 1;

  IF v_post_row IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', v_operation,
      'success', FALSE,
      'error_code', 'POST_DECISION_ROW_NOT_FOUND',
      'message', 'bulk_timesheet_row_patch_v1 did not return a post-upsert row.',
      'contract_week_id', v_contract_week_id,
      'timesheet_id', v_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'previous_row_key', v_previous_row_key,
      'rotation_applied', FALSE,
      'old_timesheet_id', CASE WHEN p_rotation_json IS NULL THEN NULL ELSE v_pre_current_timesheet_id_text END,
      'refresh_required', TRUE,
      'cache_invalidation_hints', JSONB_BUILD_OBJECT(
        'row_keys', JSONB_BUILD_ARRAY(v_previous_row_key),
        'contract_week_ids', JSONB_BUILD_ARRAY(v_contract_week_id),
        'timesheet_ids', JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text, v_timesheet_id, v_current_timesheet_id),
        'storage_keys', JSONB_BUILD_ARRAY(v_previous_storage_key),
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'invalidate_context', TRUE,
        'invalidate_preview', FALSE,
        'refresh_required', TRUE
      ),
      'cache_invalidation', JSONB_BUILD_OBJECT(
        'rows', JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
          'previous_row_key', v_previous_row_key,
          'contract_week_id', v_contract_week_id,
          'timesheet_id', v_current_timesheet_id
        )),
        'contract_week_ids', JSONB_BUILD_ARRAY(v_contract_week_id),
        'timesheet_ids', JSONB_BUILD_ARRAY(v_pre_current_timesheet_id_text, v_timesheet_id, v_current_timesheet_id),
        'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
        'refresh_required', TRUE
      )
    );
  END IF;

  v_is_additional_manual_adjustment :=
    COALESCE(NULLIF(BTRIM(COALESCE(v_timesheet_json->>'is_adjustment', '')), '')::boolean, FALSE) = TRUE
    OR COALESCE(NULLIF(BTRIM(COALESCE(v_contract_week_json->>'is_adjustment', '')), '')::boolean, FALSE) = TRUE
    OR COALESCE(NULLIF(BTRIM(COALESCE(v_contract_week_json->>'additional_seq', '')), '')::integer, 0) > 0;

  v_additional_seq := COALESCE(NULLIF(BTRIM(COALESCE(v_contract_week_json->>'additional_seq', '')), '')::integer, 0);

  v_effective_actual_schedule_json := CASE
    WHEN jsonb_typeof(v_timesheet_json->'actual_schedule_json') = 'array' THEN v_timesheet_json->'actual_schedule_json'
    WHEN jsonb_typeof(v_timesheet_financials_json->'actual_schedule_json') = 'array' THEN v_timesheet_financials_json->'actual_schedule_json'
    ELSE COALESCE(v_post_row->'actual_schedule_json', '[]'::jsonb)
  END;

  v_effective_planned_schedule_json := CASE
    WHEN jsonb_typeof(v_contract_week_json->'planned_schedule_json') = 'array' THEN v_contract_week_json->'planned_schedule_json'
    ELSE COALESCE(v_post_row->'planned_schedule_json', '[]'::jsonb)
  END;

  v_effective_total_hours := COALESCE(
    NULLIF(BTRIM(COALESCE(v_timesheet_financials_json->>'total_hours', '')), '')::numeric,
    NULLIF(BTRIM(COALESCE(v_post_row->>'total_hours', '')), '')::numeric,
    0::numeric
  );

  v_keep_empty_additional_schedule :=
    COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE
    AND COALESCE(v_effective_total_hours, 0::numeric) = 0::numeric
    AND (
      v_effective_actual_schedule_json IS NULL
      OR CASE
        WHEN jsonb_typeof(v_effective_actual_schedule_json) = 'array' THEN jsonb_array_length(v_effective_actual_schedule_json) = 0
        ELSE FALSE
      END
    )
    AND (
      v_effective_planned_schedule_json IS NULL
      OR CASE
        WHEN jsonb_typeof(v_effective_planned_schedule_json) = 'array' THEN jsonb_array_length(v_effective_planned_schedule_json) = 0
        ELSE FALSE
      END
    );

  IF COALESCE(v_keep_empty_additional_schedule, FALSE) = TRUE THEN
    v_effective_actual_schedule_json := '[]'::jsonb;
    v_effective_planned_schedule_json := '[]'::jsonb;
    v_effective_total_hours := 0::numeric;
  END IF;

  v_post_route_type := CASE
    WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE
      AND (
        UPPER(COALESCE(v_post_row->>'contract_weekly_mode', '')) = 'NHSP'
        OR UPPER(COALESCE(v_post_row->>'route_type', '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT')
        OR UPPER(COALESCE(v_timesheet_financials_json->>'basis', '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
        OR COALESCE(NULLIF(BTRIM(COALESCE(v_post_row->>'client_is_nhsp', '')), '')::boolean, FALSE) = TRUE
      ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
    WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE
      AND (
        UPPER(COALESCE(v_post_row->>'contract_weekly_mode', '')) = 'HEALTHROSTER'
        OR UPPER(COALESCE(v_post_row->>'route_type', '')) IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT')
        OR UPPER(COALESCE(v_timesheet_financials_json->>'basis', '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
        OR COALESCE(NULLIF(BTRIM(COALESCE(v_post_row->>'client_autoprocess_hr', '')), '')::boolean, FALSE) = TRUE
      ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
    ELSE v_post_row->>'route_type'
  END;

  v_post_route_family := CASE
    WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN 'MANUAL_NON_QR'
    ELSE v_post_row->>'route_family'
  END;

  v_post_route_subfamily := CASE
    WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN 'MANUAL_NON_QR'
    ELSE v_post_row->>'route_subfamily'
  END;

  v_post_row := v_post_row || JSONB_BUILD_OBJECT(
    'route_type', v_post_route_type,
    'route_family', v_post_route_family,
    'route_subfamily', v_post_route_subfamily,
    'underlying_channel_family', v_post_route_subfamily,
    'is_import_authoritative', CASE WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN FALSE ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_post_row->>'is_import_authoritative', '')), '')::boolean, FALSE) END,
    'is_adjustment', COALESCE(v_is_additional_manual_adjustment, FALSE),
    'additional_seq', v_additional_seq,
    'actual_schedule_json', COALESCE(v_effective_actual_schedule_json, '[]'::jsonb),
    'planned_schedule_json', COALESCE(v_effective_planned_schedule_json, '[]'::jsonb),
    'total_hours', COALESCE(v_effective_total_hours, 0::numeric),
    'suppress_standard_schedule_fallback', COALESCE(v_keep_empty_additional_schedule, FALSE),
    'keep_additional_manual_adjustment_schedule_empty', COALESCE(v_keep_empty_additional_schedule, FALSE),
    '__suppressStandardScheduleFallback', COALESCE(v_keep_empty_additional_schedule, FALSE),
    '__keepAdditionalManualAdjustmentScheduleEmpty', COALESCE(v_keep_empty_additional_schedule, FALSE)
  );

  v_post_bucket := NULLIF(BTRIM(COALESCE(v_post_row->>'bulk_process_bucket', '')), '');
  v_post_authorise_section := NULLIF(BTRIM(COALESCE(v_post_row->>'bulk_authorise_section', '')), '');
  v_row_key := NULLIF(BTRIM(COALESCE(v_post_row->>'row_key', '')), '');
  v_row_signature := NULLIF(BTRIM(COALESCE(v_post_row->>'row_signature', '')), '');
  v_primary_storage_key := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_storage_key', '')), '');
  v_primary_artifact_id := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_id', '')), '');
  v_primary_artifact_kind := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_kind', '')), '');
  v_primary_artifact_display_name := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_display_name', '')), '');
  v_primary_artifact_preview_mode := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_preview_mode', '')), '');
  v_preview_changed := COALESCE(v_previous_storage_key, '') IS DISTINCT FROM COALESCE(v_primary_storage_key, '');

  v_rotation_applied := p_rotation_json IS NOT NULL
    AND NULLIF(BTRIM(COALESCE(v_pre_current_timesheet_id_text, '')), '') IS NOT NULL
    AND COALESCE(v_pre_current_timesheet_id_text, '') IS DISTINCT FROM COALESCE(v_current_timesheet_id::text, '');

  IF v_rotation_applied THEN
    v_rotation_old_timesheet_id := v_pre_current_timesheet_id_text;
    v_rotation_new_timesheet_id := v_current_timesheet_id::text;
    v_rotation_new_version := v_current_timesheet_version;
  ELSE
    v_rotation_old_timesheet_id := NULL;
    v_rotation_new_timesheet_id := NULL;
    v_rotation_old_version := NULL;
    v_rotation_new_version := NULL;
  END IF;

  v_total_delta := CASE
    WHEN v_pre_row IS NULL AND v_post_row IS NOT NULL THEN 1
    WHEN v_pre_row IS NOT NULL AND v_post_row IS NULL THEN -1
    ELSE 0
  END;

  v_unprocessed_delta := CASE
    WHEN COALESCE(v_pre_bucket, '') = 'UNPROCESSED' AND COALESCE(v_post_bucket, '') <> 'UNPROCESSED' THEN -1
    WHEN COALESCE(v_pre_bucket, '') <> 'UNPROCESSED' AND COALESCE(v_post_bucket, '') = 'UNPROCESSED' THEN 1
    ELSE 0
  END;

  v_processed_delta := CASE
    WHEN COALESCE(v_pre_bucket, '') = 'PROCESSED' AND COALESCE(v_post_bucket, '') <> 'PROCESSED' THEN -1
    WHEN COALESCE(v_pre_bucket, '') <> 'PROCESSED' AND COALESCE(v_post_bucket, '') = 'PROCESSED' THEN 1
    ELSE 0
  END;

  v_row_patch := COALESCE(v_post_row->'row_patch', JSONB_BUILD_OBJECT())
    || JSONB_BUILD_OBJECT(
      'previous_row_key', v_previous_row_key,
      'row_key', v_row_key,
      'new_row_key', v_row_key,
      'stable_row_id', v_post_row->>'stable_row_id',
      'contract_week_id', v_contract_week_id,
      'contract_id', v_contract_id,
      'timesheet_id', v_current_timesheet_id,
      'is_adjustment', COALESCE(v_is_additional_manual_adjustment, FALSE),
      'additional_seq', v_additional_seq,
      'route_type', v_post_route_type,
      'route_family', v_post_route_family,
      'route_subfamily', v_post_route_subfamily,
      'underlying_channel_family', v_post_route_subfamily,
      'is_import_authoritative', CASE WHEN COALESCE(v_is_additional_manual_adjustment, FALSE) = TRUE THEN FALSE ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_post_row->>'is_import_authoritative', '')), '')::boolean, FALSE) END,
      'actual_schedule_json', COALESCE(v_effective_actual_schedule_json, '[]'::jsonb),
      'planned_schedule_json', COALESCE(v_effective_planned_schedule_json, '[]'::jsonb),
      'total_hours', COALESCE(v_effective_total_hours, 0::numeric),
      'suppress_standard_schedule_fallback', COALESCE(v_keep_empty_additional_schedule, FALSE),
      'keep_additional_manual_adjustment_schedule_empty', COALESCE(v_keep_empty_additional_schedule, FALSE),
      '__suppressStandardScheduleFallback', COALESCE(v_keep_empty_additional_schedule, FALSE),
      '__keepAdditionalManualAdjustmentScheduleEmpty', COALESCE(v_keep_empty_additional_schedule, FALSE),
      'current_timesheet_id', v_current_timesheet_id,
      'expected_timesheet_id', v_current_timesheet_id,
      'current_version', v_current_timesheet_version,
      'current_timesheet_version', v_current_timesheet_version,
      'row_signature', v_row_signature,
      'backend_row_signature', v_row_signature,
      'row_backend_signature', v_row_signature,
      'previous_bulk_process_bucket', v_pre_bucket,
      'bulk_process_bucket', v_post_bucket,
      'previous_bulk_authorise_section', v_pre_authorise_section,
      'bulk_authorise_section', v_post_authorise_section,
      'bucket_transition', JSONB_BUILD_OBJECT(
        'from', v_pre_bucket,
        'to', v_post_bucket,
        'changed', COALESCE(v_pre_bucket, '') IS DISTINCT FROM COALESCE(v_post_bucket, '')
      ),
      'primary_artifact_storage_key', v_primary_storage_key,
      'previous_primary_artifact_storage_key', v_previous_storage_key,
      'old_storage_key', v_previous_storage_key,
      'new_storage_key', v_primary_storage_key,
      'old_storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
      'new_storage_keys', CASE WHEN v_primary_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_primary_storage_key) END,
      'primary_artifact_preview_mode', v_primary_artifact_preview_mode,
      'has_any_evidence', COALESCE(NULLIF(v_post_row->>'has_any_evidence', '')::boolean, FALSE),
      'evidence_badges', COALESCE(v_post_row->'evidence_badges', '[]'::jsonb),
      'rotation_applied', v_rotation_applied,
      'old_timesheet_id', v_rotation_old_timesheet_id,
      'new_timesheet_id', v_rotation_new_timesheet_id,
      'qr_action', v_rotation_action
    );

  v_cache_invalidation_hints := JSONB_BUILD_OBJECT(
    'row_keys', JSONB_BUILD_ARRAY(v_previous_row_key, v_row_key),
    'contract_week_ids', JSONB_BUILD_ARRAY(v_contract_week_id),
    'timesheet_ids', JSONB_BUILD_ARRAY(v_rotation_old_timesheet_id, v_timesheet_id, v_current_timesheet_id),
    'storage_keys', CASE WHEN v_preview_changed THEN JSONB_BUILD_ARRAY(v_previous_storage_key, v_primary_storage_key) ELSE '[]'::jsonb END,
    'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
    'row_signature', v_row_signature,
    'backend_row_signature', v_row_signature,
    'row_backend_signature', v_row_signature,
    'identity_changed', TRUE,
    'manual_changed', TRUE,
    'evidence_changed', v_preview_changed,
    'storage_changed', v_preview_changed,
    'invalidate_context', FALSE,
    'invalidate_row_context', FALSE,
    'invalidate_editor_context', TRUE,
    'invalidate_preview', v_preview_changed,
    'invalidate_evidence', v_preview_changed
  );

  v_cache_invalidation := JSONB_BUILD_OBJECT(
    'rows', JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
      'previous_row_key', v_previous_row_key,
      'row_key', v_row_key,
      'new_row_key', v_row_key,
      'contract_week_id', v_contract_week_id,
      'timesheet_id', v_current_timesheet_id,
      'old_timesheet_id', v_rotation_old_timesheet_id,
      'new_row_signature', v_row_signature
    )),
    'artifacts', JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
      'contract_week_id', v_contract_week_id,
      'timesheet_id', v_current_timesheet_id,
      'previous_storage_key', v_previous_storage_key,
      'storage_key', v_primary_storage_key,
      'changed', v_preview_changed
    )),
    'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise')
  );

  RETURN JSONB_BUILD_OBJECT(
    'ok', TRUE,
    'operation', v_operation,
    'success', TRUE,
    'contract_week_id', v_contract_week_id,
    'contract_id', v_contract_id,
    'timesheet_id', v_timesheet_id,
    'current_timesheet_id', v_current_timesheet_id,
    'expected_timesheet_id', v_current_timesheet_id,
    'current_version', v_current_timesheet_version,
    'current_timesheet_version', v_current_timesheet_version,
    'was_stale', COALESCE(v_was_stale, FALSE),
    'created_now', COALESCE(v_created_now, FALSE),
    'rotation_applied', v_rotation_applied,
    'rotation_action', v_rotation_action,
    'old_timesheet_id', v_rotation_old_timesheet_id,
    'new_timesheet_id', v_rotation_new_timesheet_id,
    'old_version', v_rotation_old_version,
    'new_version', v_rotation_new_version,
    'processing_status', v_processing_status,
    'previous_bulk_process_bucket', v_pre_bucket,
    'bulk_process_bucket', v_post_bucket,
    'previous_bulk_authorise_section', v_pre_authorise_section,
    'bulk_authorise_section', v_post_authorise_section,
    'bucket_transition', JSONB_BUILD_OBJECT(
      'from', v_pre_bucket,
      'to', v_post_bucket,
      'changed', COALESCE(v_pre_bucket, '') IS DISTINCT FROM COALESCE(v_post_bucket, '')
    ),
    'contract_week', COALESCE(v_contract_week_json, NULL::jsonb),
    'contract_week_json', COALESCE(v_contract_week_json, NULL::jsonb),
    'timesheet', COALESCE(v_timesheet_json, NULL::jsonb),
    'timesheet_json', COALESCE(v_timesheet_json, NULL::jsonb),
    'tsfin', COALESCE(v_timesheet_financials_json, NULL::jsonb),
    'timesheet_financials_json', COALESCE(v_timesheet_financials_json, NULL::jsonb),
    'row_patch', v_row_patch,
    'row_patches', JSONB_BUILD_ARRAY(v_row_patch),
    'data_row', COALESCE(v_post_row, JSONB_BUILD_OBJECT()),
    'row', COALESCE(v_post_row, JSONB_BUILD_OBJECT()),
    'row_key', v_row_key,
    'new_row_key', v_row_key,
    'previous_row_key', v_previous_row_key,
    'row_signature', v_row_signature,
    'backend_row_signature', v_row_signature,
    'row_backend_signature', v_row_signature,
    'response_context', v_response_context,
    'bulk_authorise', v_response_context = 'bulk_authorise',
    'bulk_process', v_response_context = 'bulk_process',
    'primary_artifact', CASE
      WHEN v_primary_artifact_id IS NOT NULL OR v_primary_storage_key IS NOT NULL THEN JSONB_BUILD_OBJECT(
        'id', v_primary_artifact_id,
        'kind', v_primary_artifact_kind,
        'display_name', v_primary_artifact_display_name,
        'storage_key', v_primary_storage_key,
        'previous_storage_key', v_previous_storage_key,
        'preview_mode', v_primary_artifact_preview_mode,
        'changed', v_preview_changed
      )
      ELSE NULL::jsonb
    END,
    'evidence_hints', JSONB_BUILD_OBJECT(
      'has_any_evidence', COALESCE(NULLIF(v_post_row->>'has_any_evidence', '')::boolean, FALSE),
      'evidence_badges', COALESCE(v_post_row->'evidence_badges', '[]'::jsonb),
      'attached_evidence_count', COALESCE(NULLIF(v_post_row->>'attached_evidence_count', '')::integer, 0),
      'queue_staged_count', COALESCE(NULLIF(v_post_row->>'queue_staged_count', '')::integer, 0),
      'evidence_document_locked', COALESCE(NULLIF(v_post_row->>'evidence_document_locked', '')::boolean, FALSE),
      'evidence_lock_reason', v_post_row->>'evidence_lock_reason'
    ),
    'preview_hints', JSONB_BUILD_OBJECT(
      'primary_artifact_id', v_primary_artifact_id,
      'primary_artifact_kind', v_primary_artifact_kind,
      'primary_artifact_display_name', v_primary_artifact_display_name,
      'primary_artifact_storage_key', v_primary_storage_key,
      'previous_primary_artifact_storage_key', v_previous_storage_key,
      'old_storage_key', v_previous_storage_key,
      'new_storage_key', v_primary_storage_key,
      'old_storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
      'new_storage_keys', CASE WHEN v_primary_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_primary_storage_key) END,
      'primary_artifact_preview_mode', v_primary_artifact_preview_mode,
      'preview_storage_key', v_primary_storage_key,
      'primary_left_pane_mode', v_post_row->>'primary_left_pane_mode',
      'changed', v_preview_changed
    ),
    'artifact_hints', COALESCE(v_post_row->'artifact_hints', JSONB_BUILD_OBJECT(
      'route_family', v_post_row->>'route_family',
      'route_subfamily', v_post_row->>'route_subfamily',
      'underlying_channel_family', v_post_row->>'underlying_channel_family',
      'primary_artifact_storage_key', v_primary_storage_key,
      'previous_primary_artifact_storage_key', v_previous_storage_key,
      'old_storage_key', v_previous_storage_key,
      'new_storage_key', v_primary_storage_key,
      'old_storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
      'new_storage_keys', CASE WHEN v_primary_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_primary_storage_key) END,
      'primary_artifact_preview_mode', v_primary_artifact_preview_mode,
      'has_any_evidence', COALESCE(NULLIF(v_post_row->>'has_any_evidence', '')::boolean, FALSE),
      'evidence_badges', COALESCE(v_post_row->'evidence_badges', '[]'::jsonb),
      'changed', v_preview_changed
    )),
    'count_deltas', JSONB_BUILD_OBJECT(
      'unprocessed', v_unprocessed_delta,
      'processed', v_processed_delta,
      'total', v_total_delta
    ),
    'cache_invalidation_hints', v_cache_invalidation_hints,
    'cache_invalidation', v_cache_invalidation
  );
EXCEPTION WHEN OTHERS THEN
  v_error_sqlstate := SQLSTATE;
  v_error_message := SQLERRM;
  v_error_code := CASE
    WHEN SQLSTATE = 'P0001' AND NULLIF(BTRIM(SQLERRM), '') IS NOT NULL THEN SQLERRM
    ELSE SQLSTATE
  END;

  GET STACKED DIAGNOSTICS
    v_exception_detail = PG_EXCEPTION_DETAIL,
    v_exception_hint = PG_EXCEPTION_HINT;

  IF v_error_sqlstate = '23505'
     AND (
       v_error_message ILIKE '%timesheets_booking_id_version_uidx%'
       OR COALESCE(v_exception_detail, '') ILIKE '%timesheets_booking_id_version_uidx%'
     ) THEN
    v_error_code := 'BOOKING_ID_COLLISION';
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_exception_detail, '')), '') IS NOT NULL THEN
    BEGIN
      v_detail_json := v_exception_detail::jsonb;
      IF jsonb_typeof(v_detail_json->'missing') = 'array' THEN
        v_missing := v_detail_json->'missing';
      END IF;
      IF NULLIF(BTRIM(COALESCE(v_detail_json->>'current_timesheet_id', '')), '') IS NOT NULL THEN
        v_error_current_timesheet_id := (v_detail_json->>'current_timesheet_id')::uuid;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      v_detail_json := NULL;
      v_missing := '[]'::jsonb;
      v_error_current_timesheet_id := NULL;
    END;
  END IF;

  RETURN JSONB_BUILD_OBJECT(
    'ok', FALSE,
    'operation', v_operation,
    'success', FALSE,
    'error_code', v_error_code,
    'sqlstate', v_error_sqlstate,
    'message', v_error_message,
    'detail', v_exception_detail,
    'detail_json', v_detail_json,
    'hint', v_exception_hint,
    'missing', v_missing,
    'supplied_booking_id', v_detail_json->>'supplied_booking_id',
    'booking_id', v_detail_json->>'booking_id',
    'existing_timesheet_id', v_detail_json->>'existing_timesheet_id',
    'existing_contract_id', v_detail_json->>'existing_contract_id',
    'target_contract_id', v_detail_json->>'target_contract_id',
    'target_week_ending_date', v_detail_json->>'target_week_ending_date',
    'contract_week_id', p_week_id,
    'expected_timesheet_id', p_expected_timesheet_id,
    'current_timesheet_id', COALESCE(v_error_current_timesheet_id, p_expected_timesheet_id),
    'rotation_action', v_rotation_action,
    'rotation_applied', FALSE,
    'old_timesheet_id', CASE WHEN p_rotation_json IS NULL THEN NULL ELSE v_pre_current_timesheet_id_text END,
    'previous_row_key', v_previous_row_key,
    'response_context', v_response_context,
    'bulk_authorise', v_response_context = 'bulk_authorise',
    'bulk_process', v_response_context = 'bulk_process',
    'refresh_required', TRUE,
    'cache_invalidation_hints', JSONB_BUILD_OBJECT(
      'row_keys', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_row_key) END,
      'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
      'timesheet_ids', JSONB_BUILD_ARRAY(p_expected_timesheet_id, v_pre_current_timesheet_id_text),
      'storage_keys', CASE WHEN v_previous_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(v_previous_storage_key) END,
      'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
      'invalidate_context', TRUE,
      'invalidate_preview', FALSE,
      'refresh_required', TRUE
    ),
    'cache_invalidation', JSONB_BUILD_OBJECT(
      'rows', CASE WHEN v_previous_row_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT(
        'previous_row_key', v_previous_row_key,
        'contract_week_id', p_week_id,
        'timesheet_id', COALESCE(v_error_current_timesheet_id, p_expected_timesheet_id)
      )) END,
      'contract_week_ids', JSONB_BUILD_ARRAY(p_week_id),
      'timesheet_ids', JSONB_BUILD_ARRAY(p_expected_timesheet_id, v_pre_current_timesheet_id_text),
      'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
      'refresh_required', TRUE
    )
  );
END;
$function$;


DROP FUNCTION IF EXISTS public.timesheet_daily_manual_process_atomic(uuid, uuid, uuid, jsonb, jsonb, timestamp with time zone);
DROP FUNCTION IF EXISTS public.timesheet_daily_manual_process_atomic(uuid, uuid, uuid, jsonb, jsonb, timestamp with time zone, text);


CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_process_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_patch_json jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 VOLATILE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_booking_id text := NULL;
  v_requested_timesheet_id uuid := NULL;
  v_current_timesheet_id uuid := NULL;
  v_current_version integer := NULL;
  v_was_stale boolean := FALSE;
  v_sheet_scope text := NULL;
  v_submission_mode text := NULL;
  v_authorised_at_server timestamp with time zone := NULL;
  v_tsfin_id uuid := NULL;
  v_previous_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := 'PENDING_AUTH'::public.ts_fin_processing_status_enum;
  v_locked_by_invoice_id uuid := NULL;
  v_paid_at_utc timestamp with time zone := NULL;
  v_invoice_breakdown_json jsonb := NULL;
  v_has_segment_invoice_lock boolean := FALSE;
  v_effective_candidate_id uuid := NULL;
  v_effective_client_id uuid := NULL;
  v_effective_pay_method text := NULL;
  v_effective_candidate_assignment text := NULL;
  v_effective_has_rate_issue boolean := FALSE;
  v_effective_has_pay_channel_issue boolean := FALSE;
  v_timesheet_patch jsonb := COALESCE(p_timesheet_patch_json, '{}'::jsonb);
  v_tsfin_patch jsonb := COALESCE(p_tsfin_patch_json, '{}'::jsonb);
  v_timesheet_json jsonb := NULL;
  v_tsfin_json jsonb := NULL;
  v_authoritative_tsfin_json jsonb := NULL;
  v_pre_row jsonb := NULL;
  v_post_row jsonb := NULL;
  v_expected_row_signature text := NULL;
  v_patch_key text := NULL;
  v_supplied_text text := NULL;
  v_authoritative_text text := NULL;
  v_supplied_numeric numeric := NULL;
  v_authoritative_numeric numeric := NULL;
  v_numeric_patch_keys text[] := ARRAY[
    'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh',
    'pay_day', 'pay_night', 'pay_sat', 'pay_sun', 'pay_bh',
    'charge_day', 'charge_night', 'charge_sat', 'charge_sun', 'charge_bh',
    'total_hours', 'total_pay_ex_vat', 'total_charge_ex_vat', 'margin_ex_vat',
    'expenses_pay_ex_vat', 'expenses_charge_ex_vat',
    'mileage_units', 'mileage_pay_rate', 'mileage_charge_rate', 'mileage_pay_ex_vat', 'mileage_charge_ex_vat',
    'travel_pay_ex_vat', 'travel_charge_ex_vat',
    'accommodation_pay_ex_vat', 'accommodation_charge_ex_vat',
    'other_pay_ex_vat', 'other_charge_ex_vat',
    'additional_pay_ex_vat', 'additional_charge_ex_vat', 'additional_margin_ex_vat'
  ];
  v_forbidden_tsfin_patch_keys text[] := ARRAY[
    'candidate_id', 'client_id', 'pay_method', 'candidate_assignment', 'basis',
    'policy_snapshot_json', 'rate_source_refs_json',
    'normal_hours', 'unsocial_hours', 'saturday_hours', 'sunday_hours', 'bank_holiday_hours',
    'sleep_in_units', 'on_call_units', 'mileage_units', 'expenses_units',
    'total_hours', 'total_pay_ex_vat', 'total_charge_ex_vat', 'margin_ex_vat', 'net_delta_ex_vat',
    'normal_pay_rate', 'unsocial_pay_rate', 'saturday_pay_rate', 'sunday_pay_rate', 'bank_holiday_pay_rate',
    'normal_charge_rate', 'unsocial_charge_rate', 'saturday_charge_rate', 'sunday_charge_rate', 'bank_holiday_charge_rate',
    'mileage_pay_rate', 'mileage_charge_rate', 'expenses_pay', 'expenses_charge',
    'has_rate_issue', 'has_pay_channel_issue',
    'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh',
    'pay_day', 'pay_night', 'pay_sat', 'pay_sun', 'pay_bh',
    'charge_day', 'charge_night', 'charge_sat', 'charge_sun', 'charge_bh',
    'expenses_pay_ex_vat', 'expenses_charge_ex_vat',
    'mileage_pay_ex_vat', 'mileage_charge_ex_vat',
    'travel_pay_ex_vat', 'travel_charge_ex_vat',
    'accommodation_pay_ex_vat', 'accommodation_charge_ex_vat',
    'other_pay_ex_vat', 'other_charge_ex_vat',
    'additional_pay_ex_vat', 'additional_charge_ex_vat', 'additional_margin_ex_vat'
  ];
  v_financial_affecting_timesheet_patch_keys text[] := ARRAY[
    'worked_start_iso', 'worked_end_iso', 'break_start_iso', 'break_end_iso',
    'break_minutes', 'worked_minutes', 'actual_schedule_json',
    'additional_units_week', 'additional_units_per_day',
    'scheduled_start_iso', 'scheduled_end_iso', 'week_ending_date', 'worked_date', 'work_date'
  ];
BEGIN
  IF p_timesheet_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_ID_REQUIRED', 'message', 'p_timesheet_id is required.');
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'EXPECTED_TIMESHEET_ID_REQUIRED', 'message', 'p_expected_timesheet_id is required.');
  END IF;

  IF p_actor_user_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'ACTOR_USER_ID_REQUIRED', 'message', 'p_actor_user_id is required.');
  END IF;

  IF jsonb_typeof(v_timesheet_patch) <> 'object' THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_PATCH_MUST_BE_OBJECT', 'message', 'p_timesheet_patch_json must be a JSON object.');
  END IF;

  IF jsonb_typeof(v_tsfin_patch) <> 'object' THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'TSFIN_PATCH_MUST_BE_OBJECT', 'message', 'p_tsfin_patch_json must be a JSON object.');
  END IF;

  FOREACH v_patch_key IN ARRAY v_forbidden_tsfin_patch_keys LOOP
    IF v_tsfin_patch ? v_patch_key THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'success', FALSE,
        'operation', 'daily_manual_process',
        'error_code', 'TSFIN_PATCH_FORBIDDEN_FIELD',
        'message', 'Cannot process: TSFIN patch contains an authoritative or financial field.',
        'field', v_patch_key,
        'timesheet_id', p_timesheet_id
      );
    END IF;
  END LOOP;

  FOREACH v_patch_key IN ARRAY v_financial_affecting_timesheet_patch_keys LOOP
    IF v_timesheet_patch ? v_patch_key THEN
      RETURN JSONB_BUILD_OBJECT(
        'ok', FALSE,
        'success', FALSE,
        'operation', 'daily_manual_process',
        'error_code', 'TIMESHEET_PATCH_REQUIRES_RECALCULATION',
        'message', 'Cannot process while changing worked time, schedule, break, work date, or additional units. Save and recalculate the row before processing.',
        'field', v_patch_key,
        'timesheet_id', p_timesheet_id
      );
    END IF;
  END LOOP;

  SELECT requested_ts.booking_id,
         requested_ts.timesheet_id
    INTO v_requested_booking_id,
         v_requested_timesheet_id
  FROM public.timesheets AS requested_ts
  WHERE requested_ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_timesheet_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_NOT_FOUND', 'message', 'Timesheet was not found.', 'requested_timesheet_id', p_timesheet_id);
  END IF;

  SELECT current_ts.timesheet_id,
         current_ts.version
    INTO v_current_timesheet_id,
         v_current_version
  FROM public.timesheets AS current_ts
  WHERE current_ts.booking_id = v_requested_booking_id
    AND current_ts.is_current = TRUE
  ORDER BY current_ts.version DESC NULLS LAST, current_ts.updated_at DESC NULLS LAST, current_ts.created_at DESC NULLS LAST, current_ts.timesheet_id DESC
  LIMIT 1;

  IF v_current_timesheet_id IS NULL THEN
    v_current_timesheet_id := v_requested_timesheet_id;
  END IF;

  v_was_stale := v_requested_timesheet_id IS DISTINCT FROM v_current_timesheet_id;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', 'daily_manual_process',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'Timesheet has moved to a newer current row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'was_stale', v_was_stale
    );
  END IF;

  SELECT current_ts.sheet_scope::text,
         current_ts.submission_mode::text,
         current_ts.authorised_at_server,
         current_ts.version
    INTO v_sheet_scope,
         v_submission_mode,
         v_authorised_at_server,
         v_current_version
  FROM public.timesheets AS current_ts
  WHERE current_ts.timesheet_id = v_current_timesheet_id
    AND current_ts.is_current = TRUE
  LIMIT 1
  FOR UPDATE;

  IF v_sheet_scope IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'CURRENT_TIMESHEET_NOT_FOUND', 'message', 'Current timesheet was not found.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) <> 'DAILY' THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'NOT_DAILY', 'message', 'Timesheet is not DAILY; daily manual process only applies to DAILY sheets.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF UPPER(COALESCE(v_submission_mode, '')) <> 'MANUAL' THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'NOT_MANUAL', 'message', 'Timesheet must be MANUAL before processing.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF v_authorised_at_server IS NOT NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_ALREADY_AUTHORISED', 'message', 'This timesheet is already authorised.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  SELECT tsfin_current.id,
         tsfin_current.processing_status,
         tsfin_current.locked_by_invoice_id,
         tsfin_current.paid_at_utc,
         tsfin_current.invoice_breakdown_json,
         tsfin_current.candidate_id,
         tsfin_current.client_id,
         tsfin_current.pay_method,
         tsfin_current.candidate_assignment::text,
         COALESCE(tsfin_current.has_rate_issue, FALSE),
         COALESCE(tsfin_current.has_pay_channel_issue, FALSE),
         TO_JSONB(tsfin_current)
    INTO v_tsfin_id,
         v_previous_status,
         v_locked_by_invoice_id,
         v_paid_at_utc,
         v_invoice_breakdown_json,
         v_effective_candidate_id,
         v_effective_client_id,
         v_effective_pay_method,
         v_effective_candidate_assignment,
         v_effective_has_rate_issue,
         v_effective_has_pay_channel_issue,
         v_authoritative_tsfin_json
  FROM public.timesheets_financials AS tsfin_current
  WHERE tsfin_current.timesheet_id = v_current_timesheet_id
    AND tsfin_current.is_current = TRUE
  ORDER BY tsfin_current.computed_at_utc DESC NULLS LAST, tsfin_current.created_at DESC NULLS LAST, tsfin_current.updated_at DESC NULLS LAST, tsfin_current.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_tsfin_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'NO_TSFIN', 'message', 'No current financial snapshot exists for this timesheet.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_locked_by_invoice_id IS NOT NULL OR v_paid_at_utc IS NOT NULL OR v_has_segment_invoice_lock = TRUE THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_LOCKED_OR_PAID', 'message', 'Timesheet already invoiced or paid; cannot process.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF v_previous_status <> 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'NOT_UNPROCESSED', 'message', 'Timesheet is not in UNPROCESSED state.', 'current_timesheet_id', v_current_timesheet_id, 'previous_status', v_previous_status);
  END IF;

  v_expected_row_signature := NULLIF(BTRIM(COALESCE(
    p_expected_row_signature,
    v_timesheet_patch->>'row_signature',
    v_timesheet_patch->>'rowSignature',
    v_tsfin_patch->>'row_signature',
    v_tsfin_patch->>'rowSignature',
    ''
  )), '');

  SELECT decision_result.row_json
    INTO v_pre_row
  FROM public.bulk_timesheet_row_patch_v1(JSONB_BUILD_OBJECT('dataset_mode', 'process', 'timesheet_id', v_current_timesheet_id::text)) AS decision_result(row_json)
  LIMIT 1;

  IF v_expected_row_signature IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_pre_row->>'row_signature', '')), '') IS DISTINCT FROM v_expected_row_signature THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', 'daily_manual_process',
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'message', 'Timesheet changed after it was loaded. Refresh the row and try again.',
      'current_timesheet_id', v_current_timesheet_id,
      'expected_row_signature', v_expected_row_signature,
      'current_row_signature', v_pre_row->>'row_signature'
    );
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_authoritative_tsfin_json->>'total_hours', '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_authoritative_tsfin_json->>'total_pay_ex_vat', '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_authoritative_tsfin_json->>'total_charge_ex_vat', '')), '') IS NULL
     OR NULLIF(BTRIM(COALESCE(v_authoritative_tsfin_json->>'margin_ex_vat', '')), '') IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', 'daily_manual_process',
      'error_code', 'AUTHORITATIVE_TSFIN_TOTALS_MISSING',
      'message', 'Cannot process: authoritative TSFIN totals are missing.',
      'current_timesheet_id', v_current_timesheet_id
    );
  END IF;

  FOREACH v_patch_key IN ARRAY v_numeric_patch_keys LOOP
    IF v_tsfin_patch ? v_patch_key THEN
      v_supplied_text := NULLIF(BTRIM(COALESCE(v_tsfin_patch->>v_patch_key, '')), '');
      v_authoritative_text := NULLIF(BTRIM(COALESCE(v_authoritative_tsfin_json->>v_patch_key, '')), '');

      IF v_supplied_text IS NULL AND v_authoritative_text IS NULL THEN
        CONTINUE;
      END IF;

      IF v_supplied_text IS NULL OR v_authoritative_text IS NULL THEN
        RETURN JSONB_BUILD_OBJECT(
          'ok', FALSE,
          'operation', 'daily_manual_process',
          'error_code', 'TSFIN_PATCH_MISMATCH',
          'message', 'Cannot process: supplied TSFIN value does not match the authoritative DB snapshot.',
          'field', v_patch_key,
          'current_timesheet_id', v_current_timesheet_id
        );
      END IF;

      BEGIN
        v_supplied_numeric := v_supplied_text::numeric;
        v_authoritative_numeric := v_authoritative_text::numeric;
      EXCEPTION WHEN OTHERS THEN
        RETURN JSONB_BUILD_OBJECT(
          'ok', FALSE,
          'operation', 'daily_manual_process',
          'error_code', 'TSFIN_PATCH_NUMERIC_INVALID',
          'message', 'Cannot process: supplied TSFIN numeric value is invalid.',
          'field', v_patch_key,
          'current_timesheet_id', v_current_timesheet_id
        );
      END;

      IF ABS(COALESCE(v_supplied_numeric, 0) - COALESCE(v_authoritative_numeric, 0)) > 0.0001 THEN
        RETURN JSONB_BUILD_OBJECT(
          'ok', FALSE,
          'operation', 'daily_manual_process',
          'error_code', 'TSFIN_PATCH_MISMATCH',
          'message', 'Cannot process: supplied TSFIN totals must match the authoritative DB snapshot.',
          'field', v_patch_key,
          'supplied', v_supplied_numeric,
          'authoritative', v_authoritative_numeric,
          'current_timesheet_id', v_current_timesheet_id
        );
      END IF;
    END IF;
  END LOOP;

  IF v_effective_candidate_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'CANDIDATE_MISSING', 'message', 'Cannot process: candidate is missing from TSFIN context.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF v_effective_client_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'CLIENT_MISSING', 'message', 'Cannot process: client is missing from TSFIN context.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF v_effective_has_rate_issue = TRUE THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'RATE_ISSUE', 'message', 'Cannot process: TSFIN has a rate issue.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF v_effective_has_pay_channel_issue = TRUE THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'PAY_CHANNEL_ISSUE', 'message', 'Cannot process: TSFIN has a pay channel issue.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_effective_pay_method, '')), '') IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'PAY_METHOD_MISSING', 'message', 'Cannot process: pay method is missing from TSFIN context.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_effective_candidate_assignment, '')), '') IS NULL OR UPPER(COALESCE(v_effective_candidate_assignment, '')) = 'UNASSIGNED' THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'CANDIDATE_ASSIGNMENT_UNRESOLVED', 'message', 'Cannot process: candidate assignment is unresolved in TSFIN context.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  UPDATE public.timesheets AS timesheet_update
  SET worked_start_iso = CASE WHEN v_timesheet_patch ? 'worked_start_iso' THEN NULLIF(BTRIM(v_timesheet_patch->>'worked_start_iso'), '')::timestamp with time zone ELSE timesheet_update.worked_start_iso END,
      worked_end_iso = CASE WHEN v_timesheet_patch ? 'worked_end_iso' THEN NULLIF(BTRIM(v_timesheet_patch->>'worked_end_iso'), '')::timestamp with time zone ELSE timesheet_update.worked_end_iso END,
      break_start_iso = CASE WHEN v_timesheet_patch ? 'break_start_iso' THEN NULLIF(BTRIM(v_timesheet_patch->>'break_start_iso'), '')::timestamp with time zone ELSE timesheet_update.break_start_iso END,
      break_end_iso = CASE WHEN v_timesheet_patch ? 'break_end_iso' THEN NULLIF(BTRIM(v_timesheet_patch->>'break_end_iso'), '')::timestamp with time zone ELSE timesheet_update.break_end_iso END,
      break_minutes = CASE WHEN v_timesheet_patch ? 'break_minutes' THEN NULLIF(BTRIM(v_timesheet_patch->>'break_minutes'), '')::integer ELSE timesheet_update.break_minutes END,
      worked_minutes = CASE WHEN v_timesheet_patch ? 'worked_minutes' THEN NULLIF(BTRIM(v_timesheet_patch->>'worked_minutes'), '')::integer ELSE timesheet_update.worked_minutes END,
      actual_schedule_json = CASE WHEN v_timesheet_patch ? 'actual_schedule_json' THEN v_timesheet_patch->'actual_schedule_json' ELSE timesheet_update.actual_schedule_json END,
      additional_units_week = CASE WHEN v_timesheet_patch ? 'additional_units_week' THEN v_timesheet_patch->'additional_units_week' ELSE timesheet_update.additional_units_week END,
      additional_units_per_day = CASE WHEN v_timesheet_patch ? 'additional_units_per_day' THEN v_timesheet_patch->'additional_units_per_day' ELSE timesheet_update.additional_units_per_day END,
      reference_number = CASE WHEN v_timesheet_patch ? 'reference_number' THEN NULLIF(BTRIM(v_timesheet_patch->>'reference_number'), '') ELSE timesheet_update.reference_number END,
      reference_set_at = CASE WHEN v_timesheet_patch ? 'reference_number' THEN CASE WHEN NULLIF(BTRIM(v_timesheet_patch->>'reference_number'), '') IS NULL THEN NULL::timestamp with time zone WHEN NULLIF(BTRIM(v_timesheet_patch->>'reference_number'), '') IS DISTINCT FROM NULLIF(BTRIM(COALESCE(timesheet_update.reference_number, '')), '') THEN v_now ELSE COALESCE(timesheet_update.reference_set_at, v_now) END ELSE timesheet_update.reference_set_at END,
      updated_at = v_now
  WHERE timesheet_update.timesheet_id = v_current_timesheet_id
    AND timesheet_update.is_current = TRUE
  RETURNING TO_JSONB(timesheet_update), timesheet_update.version
  INTO v_timesheet_json, v_current_version;

  IF v_timesheet_json IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'TIMESHEET_UPDATE_FAILED', 'message', 'Failed to update current timesheet.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  UPDATE public.timesheets_financials AS tsfin_update
  SET processing_status = v_new_status,
      processed_by_user_id = p_actor_user_id,
      processed_at_utc = v_now,
      authorised_by_user_id = NULL,
      authorised_at_utc = NULL,
      updated_at = v_now
  WHERE tsfin_update.id = v_tsfin_id
    AND tsfin_update.is_current = TRUE
  RETURNING TO_JSONB(tsfin_update)
  INTO v_tsfin_json;

  IF v_tsfin_json IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_process', 'error_code', 'TSFIN_UPDATE_FAILED', 'message', 'Failed to update current financial snapshot.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  SELECT decision_result.row_json
    INTO v_post_row
  FROM public.bulk_timesheet_row_patch_v1(JSONB_BUILD_OBJECT('dataset_mode', 'process', 'timesheet_id', v_current_timesheet_id::text)) AS decision_result(row_json)
  LIMIT 1;

  PERFORM public._audit_insert(
    'timesheet',
    v_current_timesheet_id::text,
    'TIMESHEET_DAILY_MANUAL_PROCESSED',
    JSONB_BUILD_OBJECT(
      'row', COALESCE(v_pre_row, JSONB_BUILD_OBJECT()),
      'processing_status', v_previous_status,
      'row_signature', v_pre_row->>'row_signature'
    ),
    JSONB_BUILD_OBJECT(
      'row', COALESCE(v_post_row, JSONB_BUILD_OBJECT()),
      'processing_status', v_new_status,
      'processed_at_utc', v_now,
      'processed_by_user_id', p_actor_user_id,
      'row_signature', v_post_row->>'row_signature'
    ),
    'DAILY_MANUAL_PROCESS',
    p_actor_user_id
  );

  RETURN JSONB_BUILD_OBJECT(
    'ok', TRUE,
    'operation', 'daily_manual_process',
    'processed', TRUE,
    'success', TRUE,
    'requested_timesheet_id', p_timesheet_id,
    'expected_timesheet_id', p_expected_timesheet_id,
    'current_timesheet_id', v_current_timesheet_id,
    'timesheet_id', v_current_timesheet_id,
    'current_version', v_current_version,
    'was_stale', v_was_stale,
    'previous_status', v_previous_status,
    'processing_status', v_new_status,
    'status_transition', JSONB_BUILD_OBJECT('from', v_previous_status, 'to', v_new_status, 'processed_at_utc', v_now, 'processed_by_user_id', p_actor_user_id),
    'timesheet', COALESCE(v_timesheet_json, NULL::jsonb),
    'tsfin', COALESCE(v_tsfin_json, NULL::jsonb),
    'row_patch', COALESCE(v_post_row->'row_patch', JSONB_BUILD_OBJECT()),
    'row_patches', JSONB_BUILD_ARRAY(COALESCE(v_post_row->'row_patch', JSONB_BUILD_OBJECT())),
    'row_signature', v_post_row->>'row_signature',
    'data_row', COALESCE(v_post_row, JSONB_BUILD_OBJECT()),
    'count_deltas', JSONB_BUILD_OBJECT('unprocessed', -1, 'processed', 1),
    'cache_invalidation_hints', JSONB_BUILD_OBJECT(
      'row_keys', JSONB_BUILD_ARRAY(COALESCE(v_post_row->>'row_key', 'timesheet:' || v_current_timesheet_id::text)),
      'timesheet_ids', JSONB_BUILD_ARRAY(v_current_timesheet_id),
      'storage_keys', '[]'::jsonb,
      'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
      'row_signature', v_post_row->>'row_signature',
      'manual_changed', TRUE,
      'identity_changed', FALSE,
      'status_only', FALSE,
      'invalidate_context', FALSE,
      'invalidate_row_context', FALSE,
      'invalidate_editor_context', TRUE,
      'invalidate_preview', FALSE,
      'invalidate_evidence', FALSE
    ),
    'cache_invalidation', JSONB_BUILD_OBJECT(
      'row_keys', JSONB_BUILD_ARRAY(COALESCE(v_post_row->>'row_key', 'timesheet:' || v_current_timesheet_id::text)),
      'timesheet_ids', JSONB_BUILD_ARRAY(v_current_timesheet_id),
      'storage_keys', '[]'::jsonb,
      'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
      'row_signature', v_post_row->>'row_signature',
      'manual_changed', TRUE,
      'invalidate_context', FALSE,
      'invalidate_preview', FALSE
    )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_unprocess_atomic(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 VOLATILE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_booking_id text := NULL;
  v_requested_timesheet_id uuid := NULL;
  v_current_timesheet_id uuid := NULL;
  v_current_version integer := NULL;
  v_was_stale boolean := FALSE;
  v_sheet_scope text := NULL;
  v_submission_mode text := NULL;
  v_authorised_at_server timestamp with time zone := NULL;
  v_tsfin_id uuid := NULL;
  v_previous_status public.ts_fin_processing_status_enum := NULL;
  v_new_status public.ts_fin_processing_status_enum := 'UNPROCESSED'::public.ts_fin_processing_status_enum;
  v_locked_by_invoice_id uuid := NULL;
  v_paid_at_utc timestamp with time zone := NULL;
  v_invoice_breakdown_json jsonb := NULL;
  v_has_segment_invoice_lock boolean := FALSE;
  v_tsfin_json jsonb := NULL;
  v_timesheet_json jsonb := NULL;
  v_pre_row jsonb := NULL;
  v_post_row jsonb := NULL;
  v_expected_row_signature text := NULL;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_ID_REQUIRED', 'message', 'p_timesheet_id is required.');
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'EXPECTED_TIMESHEET_ID_REQUIRED', 'message', 'p_expected_timesheet_id is required.');
  END IF;

  SELECT requested_ts.booking_id,
         requested_ts.timesheet_id
    INTO v_requested_booking_id,
         v_requested_timesheet_id
  FROM public.timesheets AS requested_ts
  WHERE requested_ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_timesheet_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_NOT_FOUND', 'message', 'Timesheet was not found.', 'requested_timesheet_id', p_timesheet_id);
  END IF;

  SELECT current_ts.timesheet_id,
         current_ts.version
    INTO v_current_timesheet_id,
         v_current_version
  FROM public.timesheets AS current_ts
  WHERE current_ts.booking_id = v_requested_booking_id
    AND current_ts.is_current = TRUE
  ORDER BY current_ts.version DESC NULLS LAST, current_ts.timesheet_id DESC
  LIMIT 1;

  IF v_current_timesheet_id IS NULL THEN
    v_current_timesheet_id := v_requested_timesheet_id;
  END IF;

  v_was_stale := v_requested_timesheet_id IS DISTINCT FROM v_current_timesheet_id;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', 'daily_manual_unprocess',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'Timesheet has moved to a newer current row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'was_stale', v_was_stale
    );
  END IF;

  SELECT current_ts.sheet_scope::text,
         current_ts.submission_mode::text,
         current_ts.authorised_at_server,
         current_ts.version,
         TO_JSONB(current_ts)
    INTO v_sheet_scope,
         v_submission_mode,
         v_authorised_at_server,
         v_current_version,
         v_timesheet_json
  FROM public.timesheets AS current_ts
  WHERE current_ts.timesheet_id = v_current_timesheet_id
    AND current_ts.is_current = TRUE
  LIMIT 1
  FOR UPDATE;

  IF v_sheet_scope IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'CURRENT_TIMESHEET_NOT_FOUND', 'message', 'Current timesheet was not found.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) <> 'DAILY' THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'NOT_DAILY', 'message', 'Timesheet is not DAILY; daily manual unprocess only applies to DAILY sheets.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF UPPER(COALESCE(v_submission_mode, '')) <> 'MANUAL' THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'NOT_MANUAL', 'message', 'Timesheet must be MANUAL before unprocessing.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF v_authorised_at_server IS NOT NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_AUTHORISED_EDIT_BLOCKED', 'message', 'This timesheet is authorised. Unauthorise it before unprocessing.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  SELECT tsfin_current.id,
         tsfin_current.processing_status,
         tsfin_current.locked_by_invoice_id,
         tsfin_current.paid_at_utc,
         tsfin_current.invoice_breakdown_json
    INTO v_tsfin_id,
         v_previous_status,
         v_locked_by_invoice_id,
         v_paid_at_utc,
         v_invoice_breakdown_json
  FROM public.timesheets_financials AS tsfin_current
  WHERE tsfin_current.timesheet_id = v_current_timesheet_id
    AND tsfin_current.is_current = TRUE
  ORDER BY tsfin_current.computed_at_utc DESC NULLS LAST, tsfin_current.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_tsfin_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'NO_TSFIN', 'message', 'No current financial snapshot exists for this timesheet.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_locked_by_invoice_id IS NOT NULL OR v_paid_at_utc IS NOT NULL OR v_has_segment_invoice_lock = TRUE THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'TIMESHEET_LOCKED_OR_PAID', 'message', 'Cannot unprocess: timesheet is locked or paid.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  IF v_previous_status = 'UNPROCESSED'::public.ts_fin_processing_status_enum THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'ALREADY_UNPROCESSED', 'message', 'Timesheet is already UNPROCESSED.', 'current_timesheet_id', v_current_timesheet_id, 'previous_status', v_previous_status);
  END IF;

  IF v_previous_status NOT IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', 'daily_manual_unprocess',
      'error_code', 'PROCESSING_STATUS_NOT_UNPROCESSABLE',
      'message', 'Timesheet is not in a processing state that can be moved back to UNPROCESSED.',
      'current_timesheet_id', v_current_timesheet_id,
      'previous_status', v_previous_status
    );
  END IF;

  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');

  SELECT decision_result.row_json
    INTO v_pre_row
  FROM public.bulk_timesheet_row_patch_v1(JSONB_BUILD_OBJECT('dataset_mode', 'process', 'timesheet_id', v_current_timesheet_id::text)) AS decision_result(row_json)
  LIMIT 1;

  IF v_expected_row_signature IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_pre_row->>'row_signature', '')), '') IS DISTINCT FROM v_expected_row_signature THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'operation', 'daily_manual_unprocess',
      'error_code', 'ROW_SIGNATURE_MISMATCH',
      'message', 'Timesheet changed after it was loaded. Refresh the row and try again.',
      'current_timesheet_id', v_current_timesheet_id,
      'expected_row_signature', v_expected_row_signature,
      'current_row_signature', v_pre_row->>'row_signature'
    );
  END IF;

  UPDATE public.timesheets_financials AS tsfin_update
  SET processing_status = v_new_status,
      processed_by_user_id = NULL,
      processed_at_utc = NULL,
      authorised_by_user_id = NULL,
      authorised_at_utc = NULL,
      updated_at = v_now
  WHERE tsfin_update.id = v_tsfin_id
    AND tsfin_update.is_current = TRUE
  RETURNING TO_JSONB(tsfin_update)
  INTO v_tsfin_json;

  IF v_tsfin_json IS NULL THEN
    RETURN JSONB_BUILD_OBJECT('ok', FALSE, 'operation', 'daily_manual_unprocess', 'error_code', 'TSFIN_UPDATE_FAILED', 'message', 'Failed to move daily timesheet back to UNPROCESSED.', 'current_timesheet_id', v_current_timesheet_id);
  END IF;

  SELECT TO_JSONB(current_ts)
    INTO v_timesheet_json
  FROM public.timesheets AS current_ts
  WHERE current_ts.timesheet_id = v_current_timesheet_id
    AND current_ts.is_current = TRUE
  LIMIT 1;

  SELECT decision_result.row_json
    INTO v_post_row
  FROM public.bulk_timesheet_row_patch_v1(JSONB_BUILD_OBJECT('dataset_mode', 'process', 'timesheet_id', v_current_timesheet_id::text)) AS decision_result(row_json)
  LIMIT 1;

  PERFORM public._audit_insert(
    'timesheet',
    v_current_timesheet_id::text,
    'TIMESHEET_DAILY_MANUAL_UNPROCESSED',
    JSONB_BUILD_OBJECT(
      'row', COALESCE(v_pre_row, JSONB_BUILD_OBJECT()),
      'processing_status', v_previous_status,
      'row_signature', v_pre_row->>'row_signature'
    ),
    JSONB_BUILD_OBJECT(
      'row', COALESCE(v_post_row, JSONB_BUILD_OBJECT()),
      'processing_status', v_new_status,
      'row_signature', v_post_row->>'row_signature'
    ),
    'DAILY_MANUAL_UNPROCESS',
    p_actor_user_id
  );

  RETURN JSONB_BUILD_OBJECT(
    'ok', TRUE,
    'operation', 'daily_manual_unprocess',
    'unprocessed', TRUE,
    'success', TRUE,
    'requested_timesheet_id', p_timesheet_id,
    'expected_timesheet_id', p_expected_timesheet_id,
    'current_timesheet_id', v_current_timesheet_id,
    'timesheet_id', v_current_timesheet_id,
    'current_version', v_current_version,
    'was_stale', v_was_stale,
    'previous_status', v_previous_status,
    'processing_status', v_new_status,
    'status_transition', JSONB_BUILD_OBJECT('from', v_previous_status, 'to', v_new_status, 'processed_at_utc', NULL::text, 'processed_by_user_id', NULL::text),
    'timesheet', COALESCE(v_timesheet_json, NULL::jsonb),
    'tsfin', COALESCE(v_tsfin_json, NULL::jsonb),
    'row_patch', COALESCE(v_post_row->'row_patch', JSONB_BUILD_OBJECT()),
    'row_patches', JSONB_BUILD_ARRAY(COALESCE(v_post_row->'row_patch', JSONB_BUILD_OBJECT())),
    'row_signature', v_post_row->>'row_signature',
    'data_row', COALESCE(v_post_row, JSONB_BUILD_OBJECT()),
    'count_deltas', JSONB_BUILD_OBJECT('unprocessed', 1, 'processed', -1),
    'cache_invalidation_hints', JSONB_BUILD_OBJECT(
      'row_keys', JSONB_BUILD_ARRAY(COALESCE(v_post_row->>'row_key', 'timesheet:' || v_current_timesheet_id::text)),
      'timesheet_ids', JSONB_BUILD_ARRAY(v_current_timesheet_id),
      'storage_keys', '[]'::jsonb,
      'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
      'row_signature', v_post_row->>'row_signature',
      'status_only', TRUE,
      'identity_changed', FALSE,
      'manual_changed', FALSE,
      'invalidate_context', FALSE,
      'invalidate_row_context', FALSE,
      'invalidate_preview', FALSE,
      'invalidate_evidence', FALSE
    ),
    'cache_invalidation', JSONB_BUILD_OBJECT(
      'rows', JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('row_key', COALESCE(v_post_row->>'row_key', 'timesheet:' || v_current_timesheet_id::text), 'timesheet_id', v_current_timesheet_id, 'new_row_signature', v_post_row->>'row_signature')),
      'artifacts', '[]'::jsonb,
      'datasets', JSONB_BUILD_ARRAY('bulk_process', 'bulk_authorise'),
      'status_only', TRUE,
      'invalidate_context', FALSE,
      'invalidate_preview', FALSE
    )
  );
END;
$function$;


CREATE OR REPLACE FUNCTION public.timesheet_authorise_generic_atomic(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_actor_user_id uuid,
  p_now_utc timestamptz DEFAULT now()
)
RETURNS TABLE(
  booking_id text,
  requested_timesheet_id uuid,
  current_timesheet_id uuid,
  current_version integer,
  was_stale boolean,
  processing_status_before public.ts_fin_processing_status_enum,
  processing_status_after public.ts_fin_processing_status_enum,
  validation_status text,
  validation_pre_validated boolean,
  hr_validation_required_for_invoice boolean,
  timesheet_authorised_before timestamptz,
  timesheet_json jsonb,
  timesheet_financials_json jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO public
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum;
  v_new_status public.ts_fin_processing_status_enum;
  v_force_ready_for_invoice boolean := false;
  v_client_requires_hr boolean := false;
  v_hr_validation_required_for_invoice boolean := false;
  v_validation_status text := NULL;
  v_validation_pre_validated boolean := false;
  v_validation_ok boolean := false;
  v_must_hold_for_hr_validation boolean := false;
  v_prevalidated_fast_track boolean := false;
  v_has_segment_invoice_lock boolean := false;
  v_timesheet_authorised_before timestamptz := NULL;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'p_timesheet_id is required';
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'expected_timesheet_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'p_actor_user_id is required';
  END IF;

  SELECT *
  INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_NOT_FOUND';
  END IF;

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
    SELECT *
    INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_requested_ts.booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      SELECT *
      INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_requested_ts.booking_id
      ORDER BY ts.version DESC, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;

      IF NOT FOUND THEN
        v_current_ts := v_requested_ts;
      END IF;
    END IF;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_MOVED',
      DETAIL = jsonb_build_object(
        'current_timesheet_id', v_current_ts.timesheet_id::text
      )::text;
  END IF;

  SELECT *
  INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'NO_TSFIN';
  END IF;

  v_prev_status := v_current_tsfin.processing_status;
  v_timesheet_authorised_before := v_current_ts.authorised_at_server;

  v_has_segment_invoice_lock := EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_current_tsfin.invoice_breakdown_json IS NOT NULL
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json->'segments') = 'array'
         AND upper(COALESCE(v_current_tsfin.invoice_breakdown_json->>'mode', '')) = 'SEGMENTS'
        THEN v_current_tsfin.invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END
    ) AS seg(value)
    WHERE NULLIF(BTRIM(COALESCE(seg.value->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
  );

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL
     OR v_current_tsfin.paid_at_utc IS NOT NULL
     OR v_has_segment_invoice_lock THEN
    RAISE EXCEPTION 'TIMESHEET_LOCKED_OR_PAID';
  END IF;

  SELECT
    COALESCE(vts.client_requires_hr, false),
    COALESCE(vts.hr_validation_required_for_invoice, false),
    CASE
      WHEN vts.validation_status IS NULL THEN NULL
      ELSE upper(vts.validation_status::text)
    END
  INTO
    v_client_requires_hr,
    v_hr_validation_required_for_invoice,
    v_validation_status
  FROM public.v_timesheets_summary_base AS vts
  WHERE vts.timesheet_id = v_current_ts.timesheet_id
  LIMIT 1;

  SELECT COALESCE(tv.pre_validated, false)
  INTO v_validation_pre_validated
  FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = v_current_ts.timesheet_id
  LIMIT 1;

  IF NOT FOUND THEN
    v_validation_pre_validated := false;
  END IF;

  v_force_ready_for_invoice :=
    v_current_tsfin.basis IN (
      'NHSP'::public.timesheet_fin_basis_enum,
      'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum,
      'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum,
      'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum
    );

  v_validation_ok := COALESCE(v_validation_status, '') IN ('VALIDATION_OK', 'OVERRIDDEN');
  v_must_hold_for_hr_validation := v_hr_validation_required_for_invoice AND NOT v_validation_ok;
  v_prevalidated_fast_track := v_validation_pre_validated AND v_validation_ok;

  v_new_status := v_current_tsfin.processing_status;

  IF v_prev_status IN (
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,
    'READY_FOR_HR'::public.ts_fin_processing_status_enum
  ) THEN
    IF v_must_hold_for_hr_validation THEN
      v_new_status := 'READY_FOR_HR'::public.ts_fin_processing_status_enum;
    ELSE
      v_new_status :=
        CASE
          WHEN v_force_ready_for_invoice THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          WHEN v_prevalidated_fast_track THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          WHEN v_client_requires_hr THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
          ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        END;
    END IF;
  END IF;

  UPDATE public.timesheets AS ts
  SET authorised_at_server = v_now,
      updated_at = v_now
  WHERE ts.timesheet_id = v_current_ts.timesheet_id
    AND ts.is_current = true
  RETURNING * INTO v_current_ts;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_UPDATE_FAILED';
  END IF;

  UPDATE public.timesheets_financials AS tf
  SET processing_status = v_new_status,
      authorised_by_user_id = p_actor_user_id,
      authorised_at_utc = v_now,
      updated_at = v_now
  WHERE tf.id = v_current_tsfin.id
  RETURNING * INTO v_current_tsfin;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TSFIN_UPDATE_FAILED';
  END IF;

  SELECT *
  INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = v_current_ts.timesheet_id
    AND ts.is_current = true
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CURRENT_TIMESHEET_NOT_FOUND_AFTER_WRITE';
  END IF;

  SELECT *
  INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_FINANCIALS_CURRENT_NOT_FOUND';
  END IF;

  booking_id := v_current_ts.booking_id;
  requested_timesheet_id := v_requested_ts.timesheet_id;
  current_timesheet_id := v_current_ts.timesheet_id;
  current_version := v_current_ts.version;
  was_stale := v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id;
  processing_status_before := v_prev_status;
  processing_status_after := v_current_tsfin.processing_status;
  validation_status := v_validation_status;
  validation_pre_validated := v_validation_pre_validated;
  hr_validation_required_for_invoice := v_hr_validation_required_for_invoice;
  timesheet_authorised_before := v_timesheet_authorised_before;
  timesheet_json := to_jsonb(v_current_ts);
  timesheet_financials_json := to_jsonb(v_current_tsfin);

  RETURN NEXT;
END;
$function$;
CREATE OR REPLACE FUNCTION public.timesheet_unauthorise_atomic(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_now_utc timestamptz DEFAULT now()
)
RETURNS TABLE(
  booking_id text,
  requested_timesheet_id uuid,
  current_timesheet_id uuid,
  current_version integer,
  was_stale boolean,
  processing_status_before public.ts_fin_processing_status_enum,
  processing_status_after public.ts_fin_processing_status_enum,
  timesheet_json jsonb,
  timesheet_financials_json jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO public
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_requested_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_contract_week public.contract_weeks%ROWTYPE;
  v_prev_status public.ts_fin_processing_status_enum;
  v_new_status public.ts_fin_processing_status_enum := 'PENDING_AUTH'::public.ts_fin_processing_status_enum;
  v_segment_invoice_locked boolean := false;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'p_timesheet_id is required';
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'expected_timesheet_id is required';
  END IF;

  SELECT *
  INTO v_requested_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = p_timesheet_id
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_NOT_FOUND';
  END IF;

  IF COALESCE(v_requested_ts.is_current, false) THEN
    v_current_ts := v_requested_ts;
  ELSE
    SELECT *
    INTO v_current_ts
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_requested_ts.booking_id
      AND ts.is_current = true
    ORDER BY ts.version DESC, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      SELECT *
      INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_requested_ts.booking_id
      ORDER BY ts.version DESC, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;

      IF NOT FOUND THEN
        v_current_ts := v_requested_ts;
      END IF;
    END IF;
  END IF;

  IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_MOVED',
      DETAIL = jsonb_build_object(
        'current_timesheet_id', v_current_ts.timesheet_id::text
      )::text;
  END IF;

  SELECT *
  INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'NO_TSFIN';
  END IF;

  SELECT (
    COALESCE(upper(v_current_tsfin.invoice_breakdown_json ->> 'mode') = 'SEGMENTS', false)
    AND EXISTS (
      SELECT 1
      FROM jsonb_array_elements(COALESCE(v_current_tsfin.invoice_breakdown_json -> 'segments', '[]'::jsonb)) AS seg(segment_json)
      WHERE NULLIF(btrim(seg.segment_json ->> 'invoice_locked_invoice_id'), '') IS NOT NULL
      LIMIT 1
    )
  )
  INTO v_segment_invoice_locked;

  IF v_current_tsfin.locked_by_invoice_id IS NOT NULL
     OR v_current_tsfin.paid_at_utc IS NOT NULL
     OR COALESCE(v_segment_invoice_locked, false) THEN
    RAISE EXCEPTION 'TIMESHEET_LOCKED_OR_PAID';
  END IF;

  v_prev_status := v_current_tsfin.processing_status;

  UPDATE public.timesheets AS ts
  SET authorised_at_server = NULL,
      updated_at = v_now
  WHERE ts.timesheet_id = v_current_ts.timesheet_id
    AND ts.is_current = true
  RETURNING * INTO v_current_ts;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_UPDATE_FAILED';
  END IF;

  UPDATE public.timesheets_financials AS tf
  SET processing_status = v_new_status,
      authorised_by_user_id = NULL,
      authorised_at_utc = NULL,
      updated_at = v_now
  WHERE tf.id = v_current_tsfin.id
  RETURNING * INTO v_current_tsfin;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TSFIN_UPDATE_FAILED';
  END IF;

  IF v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum THEN
    SELECT *
    INTO v_contract_week
    FROM public.contract_weeks AS cw
    WHERE cw.timesheet_id = v_current_ts.timesheet_id
    LIMIT 1
    FOR UPDATE;

    IF NOT FOUND THEN
      SELECT cw.*
      INTO v_contract_week
      FROM public.contract_weeks AS cw
      JOIN public.timesheets AS tw
        ON tw.timesheet_id = cw.timesheet_id
      WHERE tw.booking_id = v_current_ts.booking_id
      ORDER BY cw.updated_at DESC NULLS LAST, cw.id DESC
      LIMIT 1
      FOR UPDATE OF cw;
    END IF;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET';
    END IF;

    UPDATE public.contract_weeks AS cw
    SET timesheet_id = v_current_ts.timesheet_id,
        status = 'SUBMITTED'::public.contract_week_status_enum,
        updated_at = v_now
    WHERE cw.id = v_contract_week.id
    RETURNING * INTO v_contract_week;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'CONTRACT_WEEK_STATUS_UPDATE_FAILED';
    END IF;
  END IF;

  SELECT *
  INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = v_current_ts.timesheet_id
    AND ts.is_current = true
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CURRENT_TIMESHEET_NOT_FOUND_AFTER_WRITE';
  END IF;

  SELECT *
  INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'TIMESHEET_FINANCIALS_CURRENT_NOT_FOUND';
  END IF;

  booking_id := v_current_ts.booking_id;
  requested_timesheet_id := v_requested_ts.timesheet_id;
  current_timesheet_id := v_current_ts.timesheet_id;
  current_version := v_current_ts.version;
  was_stale := v_requested_ts.timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id;
  processing_status_before := v_prev_status;
  processing_status_after := v_current_tsfin.processing_status;
  timesheet_json := to_jsonb(v_current_ts);
  timesheet_financials_json := to_jsonb(v_current_tsfin);

  RETURN NEXT;
END;
$function$;



CREATE OR REPLACE FUNCTION public.timesheet_authorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 VOLATILE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_items_array jsonb := '[]'::jsonb;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_success_count integer := 0;
  v_failure_count integer := 0;
  v_out jsonb;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'batch_completed', FALSE,
      'all_success', FALSE,
      'action', 'AUTHORISE',
      'error_code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'timesheet_authorise_bulk_atomic requires p_actor_user_id.',
      'requested_count', 0,
      'success_count', 0,
      'failure_count', 0,
      'results', '[]'::jsonb,
      'row_patches', '[]'::jsonb,
      'count_deltas', JSONB_BUILD_OBJECT('processed_eligible', 0, 'authorised_eligible', 0, 'total', 0),
      'cache_invalidation_hints', JSONB_BUILD_OBJECT('row_keys', '[]'::jsonb, 'timesheet_ids', '[]'::jsonb, 'storage_keys', '[]'::jsonb, 'datasets', JSONB_BUILD_ARRAY('bulk_authorise'))
    );
  END IF;

  IF p_items IS NOT NULL AND JSONB_TYPEOF(p_items) NOT IN ('array', 'object') THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'batch_completed', FALSE,
      'all_success', FALSE,
      'action', 'AUTHORISE',
      'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT',
      'message', 'p_items must be a JSON array or an object containing items/rows/selected/selections.',
      'requested_count', 0,
      'success_count', 0,
      'failure_count', 0,
      'results', '[]'::jsonb,
      'row_patches', '[]'::jsonb,
      'count_deltas', JSONB_BUILD_OBJECT('processed_eligible', 0, 'authorised_eligible', 0, 'total', 0),
      'cache_invalidation_hints', JSONB_BUILD_OBJECT('row_keys', '[]'::jsonb, 'timesheet_ids', '[]'::jsonb, 'storage_keys', '[]'::jsonb, 'datasets', JSONB_BUILD_ARRAY('bulk_authorise'))
    );
  END IF;

  v_items_array := CASE
    WHEN p_items IS NULL THEN '[]'::jsonb
    WHEN JSONB_TYPEOF(p_items) = 'array' THEN p_items
    WHEN JSONB_TYPEOF(p_items) = 'object' AND JSONB_TYPEOF(p_items->'items') = 'array' THEN p_items->'items'
    WHEN JSONB_TYPEOF(p_items) = 'object' AND JSONB_TYPEOF(p_items->'rows') = 'array' THEN p_items->'rows'
    WHEN JSONB_TYPEOF(p_items) = 'object' AND JSONB_TYPEOF(p_items->'selected') = 'array' THEN p_items->'selected'
    WHEN JSONB_TYPEOF(p_items) = 'object' AND JSONB_TYPEOF(p_items->'selections') = 'array' THEN p_items->'selections'
    WHEN JSONB_TYPEOF(p_items) = 'object' THEN JSONB_BUILD_ARRAY(p_items)
    ELSE '[]'::jsonb
  END;

  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_decisions;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_post_decisions;
  DROP TABLE IF EXISTS pg_temp.timesheet_authorise_bulk_results;

  CREATE TEMP TABLE timesheet_authorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN JSONB_TYPEOF(input_values.item_json) = 'object' THEN input_values.item_json ELSE JSONB_BUILD_OBJECT('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json->>'row_key', input_values.item_json->>'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json->>'timesheet_id', input_values.item_json->>'timesheetId', input_values.item_json->>'current_timesheet_id', input_values.item_json->>'currentTimesheetId', input_values.item_json->>'requested_timesheet_id', input_values.item_json->>'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json->>'expected_timesheet_id', input_values.item_json->>'expectedTimesheetId', input_values.item_json->>'expected_current_timesheet_id', input_values.item_json->>'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json->>'row_signature', input_values.item_json->>'rowSignature', input_values.item_json->>'expected_row_signature', input_values.item_json->>'expectedRowSignature', '')), '') AS expected_row_signature
  FROM JSONB_ARRAY_ELEMENTS(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_authorise_bulk_state ON COMMIT DROP AS
  SELECT
    item_rows.ordinal,
    item_rows.item_json,
    item_rows.row_key,
    CASE
      WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid
      WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid
      ELSE NULL::uuid
    END AS requested_timesheet_id,
    CASE WHEN item_rows.expected_timesheet_id_text ~* v_uuid_re THEN item_rows.expected_timesheet_id_text::uuid ELSE NULL::uuid END AS expected_timesheet_id,
    item_rows.expected_row_signature AS expected_row_signature,
    requested_ts.timesheet_id AS db_requested_timesheet_id,
    requested_ts.booking_id AS requested_booking_id,
    requested_ts.version AS requested_version,
    requested_ts.is_current AS requested_is_current,
    current_ts.timesheet_id AS current_timesheet_id,
    current_ts.booking_id AS current_booking_id,
    current_ts.version AS current_version,
    current_ts.is_current AS current_is_current,
    current_ts.authorised_at_server AS current_authorised_at_server,
    current_ts.sheet_scope AS current_sheet_scope,
    current_ts.submission_mode AS current_submission_mode,
    current_tf.id AS tsfin_id,
    current_tf.processing_status AS tsfin_processing_status,
    current_tf.basis AS tsfin_basis,
    current_tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    current_tf.paid_at_utc AS tsfin_paid_at_utc,
    current_tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    COALESCE(segment_state.has_segment_invoice_lock, FALSE) AS has_segment_invoice_lock
  FROM pg_temp.timesheet_authorise_bulk_items AS item_rows
  LEFT JOIN LATERAL (
    SELECT ts_req.timesheet_id, ts_req.booking_id, ts_req.version, ts_req.is_current
    FROM public.timesheets AS ts_req
    WHERE ts_req.timesheet_id = CASE
      WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid
      WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid
      ELSE NULL::uuid
    END
    ORDER BY ts_req.version DESC NULLS LAST, ts_req.updated_at DESC NULLS LAST, ts_req.created_at DESC NULLS LAST, ts_req.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS requested_ts ON TRUE
  LEFT JOIN LATERAL (
    SELECT ts_cur.timesheet_id, ts_cur.booking_id, ts_cur.version, ts_cur.is_current, ts_cur.authorised_at_server, ts_cur.sheet_scope, ts_cur.submission_mode
    FROM public.timesheets AS ts_cur
    WHERE requested_ts.booking_id IS NOT NULL
      AND ts_cur.booking_id = requested_ts.booking_id
    ORDER BY CASE WHEN COALESCE(ts_cur.is_current, FALSE) = TRUE THEN 0 ELSE 1 END ASC,
             ts_cur.version DESC NULLS LAST,
             ts_cur.updated_at DESC NULLS LAST,
             ts_cur.created_at DESC NULLS LAST,
             ts_cur.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS current_ts ON TRUE
  LEFT JOIN LATERAL (
    SELECT tf_sel.id, tf_sel.processing_status, tf_sel.basis, tf_sel.locked_by_invoice_id, tf_sel.paid_at_utc, tf_sel.invoice_breakdown_json
    FROM public.timesheets_financials AS tf_sel
    WHERE tf_sel.timesheet_id = current_ts.timesheet_id
      AND tf_sel.is_current = TRUE
    ORDER BY tf_sel.computed_at_utc DESC NULLS LAST, tf_sel.created_at DESC NULLS LAST, tf_sel.updated_at DESC NULLS LAST, tf_sel.id DESC
    LIMIT 1
    FOR UPDATE
  ) AS current_tf ON TRUE
  LEFT JOIN LATERAL (
    SELECT EXISTS (
      SELECT 1
      FROM JSONB_ARRAY_ELEMENTS(
        CASE
          WHEN current_tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN JSONB_TYPEOF(current_tf.invoice_breakdown_json) = 'array' THEN current_tf.invoice_breakdown_json
          WHEN JSONB_TYPEOF(current_tf.invoice_breakdown_json) = 'object' AND JSONB_TYPEOF(current_tf.invoice_breakdown_json->'segments') = 'array' THEN current_tf.invoice_breakdown_json->'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
    ) AS has_segment_invoice_lock
  ) AS segment_state ON TRUE;

  CREATE TEMP TABLE timesheet_authorise_bulk_decisions ON COMMIT DROP AS
  SELECT decision_result.row_json
  FROM (
    SELECT decision_ids.timesheet_ids_json
    FROM (
      SELECT COALESCE(JSONB_AGG(DISTINCT TO_JSONB(state_rows.current_timesheet_id::text)) FILTER (WHERE state_rows.current_timesheet_id IS NOT NULL), '[]'::jsonb) AS timesheet_ids_json
      FROM pg_temp.timesheet_authorise_bulk_state AS state_rows
    ) AS decision_ids
    WHERE JSONB_ARRAY_LENGTH(decision_ids.timesheet_ids_json) > 0
  ) AS decision_filter
  CROSS JOIN LATERAL public.bulk_timesheet_row_patch_v1(JSONB_BUILD_OBJECT('timesheet_ids', decision_filter.timesheet_ids_json, 'dataset_mode', 'authorise', 'projection', 'status_patch', 'changed_domains', JSONB_BUILD_ARRAY('authorise'))) AS decision_result(row_json);

  CREATE TEMP TABLE timesheet_authorise_bulk_work ON COMMIT DROP AS
  SELECT
    state_rows.ordinal,
    state_rows.item_json,
    state_rows.row_key,
    state_rows.requested_timesheet_id,
    state_rows.expected_timesheet_id,
    state_rows.expected_row_signature,
    state_rows.db_requested_timesheet_id,
    state_rows.requested_booking_id,
    state_rows.requested_version,
    state_rows.requested_is_current,
    state_rows.current_timesheet_id,
    state_rows.current_booking_id,
    state_rows.current_version,
    state_rows.current_is_current,
    state_rows.current_authorised_at_server,
    state_rows.current_sheet_scope,
    state_rows.current_submission_mode,
    state_rows.tsfin_id,
    state_rows.tsfin_processing_status,
    state_rows.tsfin_basis,
    state_rows.tsfin_locked_by_invoice_id,
    state_rows.tsfin_paid_at_utc,
    state_rows.has_segment_invoice_lock,
    decision_rows.row_json AS pre_row_json,
    COALESCE((decision_rows.row_json->>'can_bulk_authorise')::boolean, FALSE) AS eligible_flag,
    CASE
      WHEN state_rows.requested_timesheet_id IS NULL THEN 'TIMESHEET_ID_REQUIRED'
      WHEN state_rows.expected_timesheet_id IS NULL THEN 'EXPECTED_TIMESHEET_ID_REQUIRED'
      WHEN state_rows.db_requested_timesheet_id IS NULL THEN 'TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_timesheet_id IS NULL THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_is_current IS DISTINCT FROM TRUE THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.expected_timesheet_id IS DISTINCT FROM state_rows.current_timesheet_id THEN 'TIMESHEET_MOVED'
      WHEN state_rows.tsfin_id IS NULL THEN 'NO_TSFIN'
      WHEN decision_rows.row_json IS NULL THEN 'DECISION_ROW_NOT_FOUND'
      WHEN state_rows.expected_row_signature IS NOT NULL AND NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'row_signature', '')), '') IS DISTINCT FROM state_rows.expected_row_signature THEN 'ROW_SIGNATURE_MISMATCH'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.tsfin_paid_at_utc IS NOT NULL OR state_rows.has_segment_invoice_lock = TRUE OR COALESCE((decision_rows.row_json->>'locked')::boolean, FALSE) = TRUE THEN 'TIMESHEET_LOCKED_OR_PAID'
      WHEN COALESCE((decision_rows.row_json->>'qr_unsigned_blocked')::boolean, FALSE) = TRUE THEN 'QR_UNSIGNED_BLOCKED'
      WHEN COALESCE((decision_rows.row_json->>'is_authorised')::boolean, FALSE) = TRUE THEN 'ALREADY_AUTHORISED'
      WHEN COALESCE((decision_rows.row_json->>'requires_authorisation')::boolean, FALSE) = FALSE THEN 'AUTHORISATION_NOT_REQUIRED'
      WHEN COALESCE((decision_rows.row_json->>'can_bulk_authorise')::boolean, FALSE) = FALSE THEN 'AUTHORISE_NOT_ALLOWED'
      ELSE NULL::text
    END AS failure_code,
    CASE
      WHEN state_rows.tsfin_processing_status IN ('PENDING_AUTH'::public.ts_fin_processing_status_enum, 'READY_FOR_HR'::public.ts_fin_processing_status_enum) THEN
        CASE
          WHEN COALESCE((decision_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
          WHEN state_rows.tsfin_basis IN ('NHSP'::public.timesheet_fin_basis_enum, 'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_SELF_BILL'::public.timesheet_fin_basis_enum, 'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum) THEN 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          WHEN COALESCE((decision_rows.row_json->>'client_requires_hr')::boolean, FALSE) = TRUE THEN 'READY_FOR_HR'::public.ts_fin_processing_status_enum
          ELSE 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        END
      ELSE state_rows.tsfin_processing_status
    END AS new_processing_status
  FROM pg_temp.timesheet_authorise_bulk_state AS state_rows
  LEFT JOIN pg_temp.timesheet_authorise_bulk_decisions AS decision_rows
    ON CASE WHEN COALESCE(decision_rows.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (decision_rows.row_json->>'timesheet_id')::uuid ELSE NULL::uuid END = state_rows.current_timesheet_id;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
    SET authorised_at_server = v_now,
        updated_at = v_now
    FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
    WHERE work_rows.failure_code IS NULL
      AND ts_upd.timesheet_id = work_rows.current_timesheet_id
      AND ts_upd.is_current = TRUE
    RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.timesheet_id, updated_rows.version, updated_rows.updated_at
  FROM updated_rows;

  CREATE TEMP TABLE timesheet_authorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
    SET processing_status = work_rows.new_processing_status,
        authorised_by_user_id = p_actor_user_id,
        authorised_at_utc = v_now,
        updated_at = v_now
    FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
    JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts
      ON updated_ts.timesheet_id = work_rows.current_timesheet_id
    WHERE work_rows.failure_code IS NULL
      AND tf_upd.id = work_rows.tsfin_id
      AND tf_upd.is_current = TRUE
    RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.timesheet_id, updated_rows.processing_status, updated_rows.updated_at
  FROM updated_rows;


  CREATE TEMP TABLE timesheet_authorise_bulk_post_decisions ON COMMIT DROP AS
  SELECT post_decision_result.row_json
  FROM (
    SELECT post_ids.timesheet_ids_json
    FROM (
      SELECT COALESCE(JSONB_AGG(DISTINCT TO_JSONB(updated_tf.timesheet_id::text)) FILTER (WHERE updated_tf.timesheet_id IS NOT NULL), '[]'::jsonb) AS timesheet_ids_json
      FROM pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf
    ) AS post_ids
    WHERE JSONB_ARRAY_LENGTH(post_ids.timesheet_ids_json) > 0
  ) AS post_filter
  CROSS JOIN LATERAL public.bulk_timesheet_row_patch_v1(JSONB_BUILD_OBJECT('timesheet_ids', post_filter.timesheet_ids_json, 'dataset_mode', 'authorise', 'projection', 'status_patch', 'changed_domains', JSONB_BUILD_ARRAY('authorise'))) AS post_decision_result(row_json);



  PERFORM public._audit_insert(
    'timesheet',
    audit_work.current_timesheet_id::text,
    'TIMESHEET_BULK_AUTHORISED',
    JSONB_BUILD_OBJECT(
      'row', COALESCE(audit_work.pre_row_json, JSONB_BUILD_OBJECT()),
      'processing_status', audit_work.tsfin_processing_status,
      'authorised_at_server', audit_work.current_authorised_at_server,
      'row_signature', audit_work.pre_row_json->>'row_signature'
    ),
    JSONB_BUILD_OBJECT(
      'row', COALESCE(audit_post.row_json, JSONB_BUILD_OBJECT()),
      'processing_status', audit_updated.processing_status,
      'authorised_at_server', CASE WHEN 'AUTHORISE' = 'AUTHORISE' THEN v_now ELSE NULL::timestamp with time zone END,
      'row_signature', audit_post.row_json->>'row_signature'
    ),
    'BULK_AUTHORISE',
    p_actor_user_id
  )
  FROM pg_temp.timesheet_authorise_bulk_work AS audit_work
  JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS audit_updated
    ON audit_updated.timesheet_id = audit_work.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_authorise_bulk_post_decisions AS audit_post
    ON CASE WHEN COALESCE(audit_post.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (audit_post.row_json->>'timesheet_id')::uuid ELSE NULL::uuid END = audit_work.current_timesheet_id
  WHERE audit_work.failure_code IS NULL;

  CREATE TEMP TABLE timesheet_authorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN TRUE ELSE FALSE END AS success,
    JSONB_BUILD_OBJECT(
      'item_index', work_rows.ordinal,
      'success', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN TRUE ELSE FALSE END,
      'action', 'AUTHORISE',
      'error_code', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL::text ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') END,
      'message', CASE
        WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL::text
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'TIMESHEET_MOVED' THEN 'Timesheet has moved to a different current row.'
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'TIMESHEET_LOCKED_OR_PAID' THEN 'Timesheet is locked, invoice-locked, or paid.'
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'ROW_SIGNATURE_MISMATCH' THEN 'Timesheet changed after it was loaded. Refresh the row and try again.'
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'QR_UNSIGNED_BLOCKED' THEN 'QR timesheet has not been signed and returned.'
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'AUTHORISE_NOT_ALLOWED' THEN 'Timesheet is not eligible for bulk authorisation.'
        ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED')
      END,
      'requested_timesheet_id', work_rows.requested_timesheet_id,
      'expected_timesheet_id', work_rows.expected_timesheet_id,
      'expected_row_signature', work_rows.expected_row_signature,
      'current_row_signature', work_rows.pre_row_json->>'row_signature',
      'current_timesheet_id', work_rows.current_timesheet_id,
      'current_version', COALESCE(updated_ts.version, work_rows.current_version),
      'was_stale', work_rows.requested_timesheet_id IS DISTINCT FROM work_rows.current_timesheet_id,
      'processing_status_before', work_rows.tsfin_processing_status,
      'processing_status_after', updated_tf.processing_status,
      'row_key_before', COALESCE(work_rows.row_key, work_rows.pre_row_json->>'row_key'),
      'row_key_after', COALESCE(post_rows.row_json->>'row_key', work_rows.pre_row_json->>'row_key'),
      'row', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN COALESCE(post_rows.row_json, JSONB_BUILD_OBJECT()) ELSE COALESCE(work_rows.pre_row_json, JSONB_BUILD_OBJECT()) END,
      'row_patch', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN COALESCE(post_rows.row_json->'row_patch', JSONB_BUILD_OBJECT()) ELSE JSONB_BUILD_OBJECT() END,
      'cache_invalidation_hints', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN JSONB_BUILD_OBJECT(
        'row_keys', JSONB_BUILD_ARRAY(COALESCE(work_rows.row_key, work_rows.pre_row_json->>'row_key'), post_rows.row_json->>'row_key'),
        'timesheet_ids', JSONB_BUILD_ARRAY(work_rows.requested_timesheet_id, work_rows.current_timesheet_id),
        'storage_keys', '[]'::jsonb,
        'row_signature', post_rows.row_json->>'row_signature',
        'status_only', TRUE,
        'invalidate_context', FALSE,
        'invalidate_row_context', FALSE,
        'invalidate_preview', FALSE,
        'invalidate_evidence', FALSE
      ) ELSE JSONB_BUILD_OBJECT() END
    ) AS result_json
  FROM pg_temp.timesheet_authorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_authorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_authorise_bulk_post_decisions AS post_rows ON CASE WHEN COALESCE(post_rows.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (post_rows.row_json->>'timesheet_id')::uuid ELSE NULL::uuid END = work_rows.current_timesheet_id;

  SELECT
    COUNT(result_rows.result_json) FILTER (WHERE result_rows.success = TRUE)::integer,
    COUNT(result_rows.result_json) FILTER (WHERE result_rows.success = FALSE)::integer
  INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_authorise_bulk_results AS result_rows;

  SELECT JSONB_BUILD_OBJECT(
    'ok', TRUE,
    'batch_completed', TRUE,
    'all_success', v_failure_count = 0,
    'action', 'AUTHORISE',
    'requested_count', JSONB_ARRAY_LENGTH(v_items_array),
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT JSONB_AGG(result_rows.result_json ORDER BY result_rows.ordinal ASC) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows), '[]'::jsonb),
    'row_patches', COALESCE((SELECT JSONB_AGG(result_rows.result_json->'row_patch' ORDER BY result_rows.ordinal ASC) FROM pg_temp.timesheet_authorise_bulk_results AS result_rows WHERE result_rows.success = TRUE), '[]'::jsonb),
    'count_deltas', JSONB_BUILD_OBJECT('processed_eligible', -v_success_count, 'authorised_eligible', v_success_count, 'total', 0),
    'cache_invalidation_hints', JSONB_BUILD_OBJECT(
      'row_keys', COALESCE((SELECT JSONB_AGG(TO_JSONB(distinct_row_keys.row_key)) FROM (SELECT DISTINCT cache_key_values.cache_key AS row_key FROM pg_temp.timesheet_authorise_bulk_results AS result_rows CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(result_rows.result_json->'cache_invalidation_hints'->'row_keys', '[]'::jsonb)) AS cache_key_values(cache_key) WHERE result_rows.success = TRUE AND NULLIF(BTRIM(COALESCE(cache_key_values.cache_key, '')), '') IS NOT NULL) AS distinct_row_keys), '[]'::jsonb),
      'timesheet_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(distinct_timesheet_ids.timesheet_id_text)) FROM (SELECT DISTINCT cache_timesheet_values.timesheet_id_text FROM pg_temp.timesheet_authorise_bulk_results AS result_rows CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(result_rows.result_json->'cache_invalidation_hints'->'timesheet_ids', '[]'::jsonb)) AS cache_timesheet_values(timesheet_id_text) WHERE result_rows.success = TRUE AND NULLIF(BTRIM(COALESCE(cache_timesheet_values.timesheet_id_text, '')), '') IS NOT NULL) AS distinct_timesheet_ids), '[]'::jsonb),
      'storage_keys', COALESCE((SELECT JSONB_AGG(TO_JSONB(distinct_storage_keys.storage_key)) FROM (SELECT DISTINCT cache_storage_values.storage_key FROM pg_temp.timesheet_authorise_bulk_results AS result_rows CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(result_rows.result_json->'cache_invalidation_hints'->'storage_keys', '[]'::jsonb)) AS cache_storage_values(storage_key) WHERE result_rows.success = TRUE AND NULLIF(BTRIM(COALESCE(cache_storage_values.storage_key, '')), '') IS NOT NULL) AS distinct_storage_keys), '[]'::jsonb),
      'datasets', JSONB_BUILD_ARRAY('bulk_authorise')
    )
  ) INTO v_out;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT('ok', TRUE, 'batch_completed', TRUE, 'all_success', TRUE, 'action', 'AUTHORISE', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'has_failures', FALSE, 'results', '[]'::jsonb, 'row_patches', '[]'::jsonb, 'count_deltas', JSONB_BUILD_OBJECT('processed_eligible', 0, 'authorised_eligible', 0, 'total', 0), 'cache_invalidation_hints', JSONB_BUILD_OBJECT('row_keys', '[]'::jsonb, 'timesheet_ids', '[]'::jsonb, 'storage_keys', '[]'::jsonb, 'datasets', JSONB_BUILD_ARRAY('bulk_authorise'))));
END;
$function$;




CREATE OR REPLACE FUNCTION public.timesheet_unauthorise_bulk_atomic(p_items jsonb DEFAULT '[]'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 VOLATILE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_items_array jsonb := '[]'::jsonb;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
  v_success_count integer := 0;
  v_failure_count integer := 0;
  v_out jsonb;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'batch_completed', FALSE,
      'all_success', FALSE,
      'action', 'UNAUTHORISE',
      'error_code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'timesheet_unauthorise_bulk_atomic requires p_actor_user_id.',
      'requested_count', 0,
      'success_count', 0,
      'failure_count', 0,
      'results', '[]'::jsonb,
      'row_patches', '[]'::jsonb,
      'count_deltas', JSONB_BUILD_OBJECT('processed_eligible', 0, 'authorised_eligible', 0, 'total', 0),
      'cache_invalidation_hints', JSONB_BUILD_OBJECT('row_keys', '[]'::jsonb, 'timesheet_ids', '[]'::jsonb, 'storage_keys', '[]'::jsonb, 'datasets', JSONB_BUILD_ARRAY('bulk_authorise'))
    );
  END IF;

  IF p_items IS NOT NULL AND JSONB_TYPEOF(p_items) NOT IN ('array', 'object') THEN
    RETURN JSONB_BUILD_OBJECT(
      'ok', FALSE,
      'batch_completed', FALSE,
      'all_success', FALSE,
      'action', 'UNAUTHORISE',
      'error_code', 'ITEMS_JSON_MUST_BE_ARRAY_OR_OBJECT',
      'message', 'p_items must be a JSON array or an object containing items/rows/selected/selections.',
      'requested_count', 0,
      'success_count', 0,
      'failure_count', 0,
      'results', '[]'::jsonb,
      'row_patches', '[]'::jsonb,
      'count_deltas', JSONB_BUILD_OBJECT('processed_eligible', 0, 'authorised_eligible', 0, 'total', 0),
      'cache_invalidation_hints', JSONB_BUILD_OBJECT('row_keys', '[]'::jsonb, 'timesheet_ids', '[]'::jsonb, 'storage_keys', '[]'::jsonb, 'datasets', JSONB_BUILD_ARRAY('bulk_authorise'))
    );
  END IF;

  v_items_array := CASE
    WHEN p_items IS NULL THEN '[]'::jsonb
    WHEN JSONB_TYPEOF(p_items) = 'array' THEN p_items
    WHEN JSONB_TYPEOF(p_items) = 'object' AND JSONB_TYPEOF(p_items->'items') = 'array' THEN p_items->'items'
    WHEN JSONB_TYPEOF(p_items) = 'object' AND JSONB_TYPEOF(p_items->'rows') = 'array' THEN p_items->'rows'
    WHEN JSONB_TYPEOF(p_items) = 'object' AND JSONB_TYPEOF(p_items->'selected') = 'array' THEN p_items->'selected'
    WHEN JSONB_TYPEOF(p_items) = 'object' AND JSONB_TYPEOF(p_items->'selections') = 'array' THEN p_items->'selections'
    WHEN JSONB_TYPEOF(p_items) = 'object' THEN JSONB_BUILD_ARRAY(p_items)
    ELSE '[]'::jsonb
  END;

  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_items;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_state;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_decisions;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_work;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_ts;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_tf;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_updated_cw;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_post_decisions;
  DROP TABLE IF EXISTS pg_temp.timesheet_unauthorise_bulk_results;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_items ON COMMIT DROP AS
  SELECT
    input_values.ordinality::integer AS ordinal,
    CASE WHEN JSONB_TYPEOF(input_values.item_json) = 'object' THEN input_values.item_json ELSE JSONB_BUILD_OBJECT('value', input_values.item_json) END AS item_json,
    NULLIF(BTRIM(COALESCE(input_values.item_json->>'row_key', input_values.item_json->>'rowKey', '')), '') AS row_key,
    NULLIF(BTRIM(COALESCE(input_values.item_json->>'timesheet_id', input_values.item_json->>'timesheetId', input_values.item_json->>'current_timesheet_id', input_values.item_json->>'currentTimesheetId', input_values.item_json->>'requested_timesheet_id', input_values.item_json->>'requestedTimesheetId', '')), '') AS timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json->>'expected_timesheet_id', input_values.item_json->>'expectedTimesheetId', input_values.item_json->>'expected_current_timesheet_id', input_values.item_json->>'expectedCurrentTimesheetId', '')), '') AS expected_timesheet_id_text,
    NULLIF(BTRIM(COALESCE(input_values.item_json->>'row_signature', input_values.item_json->>'rowSignature', input_values.item_json->>'expected_row_signature', input_values.item_json->>'expectedRowSignature', '')), '') AS expected_row_signature
  FROM JSONB_ARRAY_ELEMENTS(v_items_array) WITH ORDINALITY AS input_values(item_json, ordinality);

  CREATE TEMP TABLE timesheet_unauthorise_bulk_state ON COMMIT DROP AS
  SELECT
    item_rows.ordinal,
    item_rows.item_json,
    item_rows.row_key,
    CASE
      WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid
      WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid
      ELSE NULL::uuid
    END AS requested_timesheet_id,
    CASE WHEN item_rows.expected_timesheet_id_text ~* v_uuid_re THEN item_rows.expected_timesheet_id_text::uuid ELSE NULL::uuid END AS expected_timesheet_id,
    item_rows.expected_row_signature AS expected_row_signature,
    requested_ts.timesheet_id AS db_requested_timesheet_id,
    requested_ts.booking_id AS requested_booking_id,
    requested_ts.version AS requested_version,
    requested_ts.is_current AS requested_is_current,
    current_ts.timesheet_id AS current_timesheet_id,
    current_ts.booking_id AS current_booking_id,
    current_ts.version AS current_version,
    current_ts.is_current AS current_is_current,
    current_ts.authorised_at_server AS current_authorised_at_server,
    current_ts.sheet_scope AS current_sheet_scope,
    current_ts.submission_mode AS current_submission_mode,
    current_tf.id AS tsfin_id,
    current_tf.processing_status AS tsfin_processing_status,
    current_tf.basis AS tsfin_basis,
    current_tf.locked_by_invoice_id AS tsfin_locked_by_invoice_id,
    current_tf.paid_at_utc AS tsfin_paid_at_utc,
    current_tf.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
    COALESCE(segment_state.has_segment_invoice_lock, FALSE) AS has_segment_invoice_lock,
    contract_week_target.contract_week_id AS contract_week_id_for_update
  FROM pg_temp.timesheet_unauthorise_bulk_items AS item_rows
  LEFT JOIN LATERAL (
    SELECT ts_req.timesheet_id, ts_req.booking_id, ts_req.version, ts_req.is_current
    FROM public.timesheets AS ts_req
    WHERE ts_req.timesheet_id = CASE
      WHEN item_rows.timesheet_id_text ~* v_uuid_re THEN item_rows.timesheet_id_text::uuid
      WHEN item_rows.row_key LIKE 'timesheet:%' AND SUBSTRING(item_rows.row_key FROM 11) ~* v_uuid_re THEN SUBSTRING(item_rows.row_key FROM 11)::uuid
      ELSE NULL::uuid
    END
    ORDER BY ts_req.version DESC NULLS LAST, ts_req.updated_at DESC NULLS LAST, ts_req.created_at DESC NULLS LAST, ts_req.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS requested_ts ON TRUE
  LEFT JOIN LATERAL (
    SELECT ts_cur.timesheet_id, ts_cur.booking_id, ts_cur.version, ts_cur.is_current, ts_cur.authorised_at_server, ts_cur.sheet_scope, ts_cur.submission_mode
    FROM public.timesheets AS ts_cur
    WHERE requested_ts.booking_id IS NOT NULL
      AND ts_cur.booking_id = requested_ts.booking_id
    ORDER BY CASE WHEN COALESCE(ts_cur.is_current, FALSE) = TRUE THEN 0 ELSE 1 END ASC,
             ts_cur.version DESC NULLS LAST,
             ts_cur.updated_at DESC NULLS LAST,
             ts_cur.created_at DESC NULLS LAST,
             ts_cur.timesheet_id DESC
    LIMIT 1
    FOR UPDATE
  ) AS current_ts ON TRUE
  LEFT JOIN LATERAL (
    SELECT tf_sel.id, tf_sel.processing_status, tf_sel.basis, tf_sel.locked_by_invoice_id, tf_sel.paid_at_utc, tf_sel.invoice_breakdown_json
    FROM public.timesheets_financials AS tf_sel
    WHERE tf_sel.timesheet_id = current_ts.timesheet_id
      AND tf_sel.is_current = TRUE
    ORDER BY tf_sel.computed_at_utc DESC NULLS LAST, tf_sel.created_at DESC NULLS LAST, tf_sel.updated_at DESC NULLS LAST, tf_sel.id DESC
    LIMIT 1
    FOR UPDATE
  ) AS current_tf ON TRUE
  LEFT JOIN LATERAL (
    SELECT EXISTS (
      SELECT 1
      FROM JSONB_ARRAY_ELEMENTS(
        CASE
          WHEN current_tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN JSONB_TYPEOF(current_tf.invoice_breakdown_json) = 'array' THEN current_tf.invoice_breakdown_json
          WHEN JSONB_TYPEOF(current_tf.invoice_breakdown_json) = 'object' AND JSONB_TYPEOF(current_tf.invoice_breakdown_json->'segments') = 'array' THEN current_tf.invoice_breakdown_json->'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
    ) AS has_segment_invoice_lock
  ) AS segment_state ON TRUE
  LEFT JOIN LATERAL (
    SELECT cw_match.id AS contract_week_id
    FROM public.contract_weeks AS cw_match
    WHERE current_ts.timesheet_id IS NOT NULL
      AND current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
      AND (
        cw_match.timesheet_id = current_ts.timesheet_id
        OR EXISTS (
          SELECT 1
          FROM public.timesheets AS cw_timesheet
          WHERE cw_timesheet.timesheet_id = cw_match.timesheet_id
            AND cw_timesheet.booking_id = current_ts.booking_id
        )
      )
    ORDER BY
      CASE WHEN cw_match.timesheet_id = current_ts.timesheet_id THEN 0 ELSE 1 END ASC,
      cw_match.updated_at DESC NULLS LAST,
      cw_match.id DESC
    LIMIT 1
    FOR UPDATE OF cw_match
  ) AS contract_week_target ON TRUE;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_decisions ON COMMIT DROP AS
  SELECT decision_result.row_json
  FROM (
    SELECT decision_ids.timesheet_ids_json
    FROM (
      SELECT COALESCE(JSONB_AGG(DISTINCT TO_JSONB(state_rows.current_timesheet_id::text)) FILTER (WHERE state_rows.current_timesheet_id IS NOT NULL), '[]'::jsonb) AS timesheet_ids_json
      FROM pg_temp.timesheet_unauthorise_bulk_state AS state_rows
    ) AS decision_ids
    WHERE JSONB_ARRAY_LENGTH(decision_ids.timesheet_ids_json) > 0
  ) AS decision_filter
  CROSS JOIN LATERAL public.bulk_timesheet_row_patch_v1(JSONB_BUILD_OBJECT('timesheet_ids', decision_filter.timesheet_ids_json, 'dataset_mode', 'authorise', 'projection', 'status_patch', 'changed_domains', JSONB_BUILD_ARRAY('unauthorise'))) AS decision_result(row_json);

  CREATE TEMP TABLE timesheet_unauthorise_bulk_work ON COMMIT DROP AS
  SELECT
    state_rows.ordinal,
    state_rows.item_json,
    state_rows.row_key,
    state_rows.requested_timesheet_id,
    state_rows.expected_timesheet_id,
    state_rows.expected_row_signature,
    state_rows.db_requested_timesheet_id,
    state_rows.requested_booking_id,
    state_rows.requested_version,
    state_rows.requested_is_current,
    state_rows.current_timesheet_id,
    state_rows.current_booking_id,
    state_rows.current_version,
    state_rows.current_is_current,
    state_rows.current_authorised_at_server,
    state_rows.current_sheet_scope,
    state_rows.current_submission_mode,
    state_rows.tsfin_id,
    state_rows.tsfin_processing_status,
    state_rows.tsfin_basis,
    state_rows.tsfin_locked_by_invoice_id,
    state_rows.tsfin_paid_at_utc,
    state_rows.has_segment_invoice_lock,
    state_rows.contract_week_id_for_update,
    decision_rows.row_json AS pre_row_json,
    COALESCE((decision_rows.row_json->>'can_bulk_unauthorise')::boolean, FALSE) AS eligible_flag,
    CASE
      WHEN state_rows.requested_timesheet_id IS NULL THEN 'TIMESHEET_ID_REQUIRED'
      WHEN state_rows.expected_timesheet_id IS NULL THEN 'EXPECTED_TIMESHEET_ID_REQUIRED'
      WHEN state_rows.db_requested_timesheet_id IS NULL THEN 'TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_timesheet_id IS NULL THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.current_is_current IS DISTINCT FROM TRUE THEN 'CURRENT_TIMESHEET_NOT_FOUND'
      WHEN state_rows.expected_timesheet_id IS DISTINCT FROM state_rows.current_timesheet_id THEN 'TIMESHEET_MOVED'
      WHEN state_rows.tsfin_id IS NULL THEN 'NO_TSFIN'
      WHEN decision_rows.row_json IS NULL THEN 'DECISION_ROW_NOT_FOUND'
      WHEN state_rows.expected_row_signature IS NOT NULL AND NULLIF(BTRIM(COALESCE(decision_rows.row_json->>'row_signature', '')), '') IS DISTINCT FROM state_rows.expected_row_signature THEN 'ROW_SIGNATURE_MISMATCH'
      WHEN state_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum AND state_rows.contract_week_id_for_update IS NULL THEN 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET'
      WHEN state_rows.tsfin_locked_by_invoice_id IS NOT NULL OR state_rows.tsfin_paid_at_utc IS NOT NULL OR state_rows.has_segment_invoice_lock = TRUE OR COALESCE((decision_rows.row_json->>'locked')::boolean, FALSE) = TRUE THEN 'TIMESHEET_LOCKED_OR_PAID'
      WHEN COALESCE((decision_rows.row_json->>'is_authorised')::boolean, FALSE) = FALSE THEN 'NOT_AUTHORISED'
      WHEN COALESCE((decision_rows.row_json->>'can_bulk_unauthorise')::boolean, FALSE) = FALSE THEN 'UNAUTHORISE_NOT_ALLOWED'
      ELSE NULL::text
    END AS failure_code,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum AS new_processing_status
  FROM pg_temp.timesheet_unauthorise_bulk_state AS state_rows
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_decisions AS decision_rows
    ON CASE WHEN COALESCE(decision_rows.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (decision_rows.row_json->>'timesheet_id')::uuid ELSE NULL::uuid END = state_rows.current_timesheet_id;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_ts ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets AS ts_upd
    SET authorised_at_server = NULL,
        updated_at = v_now
    FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
    WHERE work_rows.failure_code IS NULL
      AND ts_upd.timesheet_id = work_rows.current_timesheet_id
      AND ts_upd.is_current = TRUE
    RETURNING ts_upd.timesheet_id, ts_upd.version, ts_upd.updated_at
  )
  SELECT updated_rows.timesheet_id, updated_rows.version, updated_rows.updated_at
  FROM updated_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_tf ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.timesheets_financials AS tf_upd
    SET processing_status = work_rows.new_processing_status,
        authorised_by_user_id = NULL,
        authorised_at_utc = NULL,
        updated_at = v_now
    FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
    JOIN pg_temp.timesheet_unauthorise_bulk_updated_ts AS updated_ts
      ON updated_ts.timesheet_id = work_rows.current_timesheet_id
    WHERE work_rows.failure_code IS NULL
      AND tf_upd.id = work_rows.tsfin_id
      AND tf_upd.is_current = TRUE
    RETURNING tf_upd.timesheet_id, tf_upd.processing_status, tf_upd.updated_at
  )
  SELECT updated_rows.timesheet_id, updated_rows.processing_status, updated_rows.updated_at
  FROM updated_rows;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_updated_cw ON COMMIT DROP AS
  WITH updated_rows AS (
    UPDATE public.contract_weeks AS cw_upd
    SET timesheet_id = work_rows.current_timesheet_id,
        status = 'SUBMITTED'::public.contract_week_status_enum,
        updated_at = v_now
    FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
    JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf
      ON updated_tf.timesheet_id = work_rows.current_timesheet_id
    WHERE work_rows.failure_code IS NULL
      AND work_rows.current_sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
      AND cw_upd.id = work_rows.contract_week_id_for_update
    RETURNING cw_upd.id, cw_upd.timesheet_id, cw_upd.status, cw_upd.updated_at
  )
  SELECT updated_rows.id, updated_rows.timesheet_id, updated_rows.status, updated_rows.updated_at
  FROM updated_rows;


  CREATE TEMP TABLE timesheet_unauthorise_bulk_post_decisions ON COMMIT DROP AS
  SELECT post_decision_result.row_json
  FROM (
    SELECT post_ids.timesheet_ids_json
    FROM (
      SELECT COALESCE(JSONB_AGG(DISTINCT TO_JSONB(updated_tf.timesheet_id::text)) FILTER (WHERE updated_tf.timesheet_id IS NOT NULL), '[]'::jsonb) AS timesheet_ids_json
      FROM pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf
    ) AS post_ids
    WHERE JSONB_ARRAY_LENGTH(post_ids.timesheet_ids_json) > 0
  ) AS post_filter
  CROSS JOIN LATERAL public.bulk_timesheet_row_patch_v1(JSONB_BUILD_OBJECT('timesheet_ids', post_filter.timesheet_ids_json, 'dataset_mode', 'authorise', 'projection', 'status_patch', 'changed_domains', JSONB_BUILD_ARRAY('unauthorise'))) AS post_decision_result(row_json);



  PERFORM public._audit_insert(
    'timesheet',
    audit_work.current_timesheet_id::text,
    'TIMESHEET_BULK_UNAUTHORISED',
    JSONB_BUILD_OBJECT(
      'row', COALESCE(audit_work.pre_row_json, JSONB_BUILD_OBJECT()),
      'processing_status', audit_work.tsfin_processing_status,
      'authorised_at_server', audit_work.current_authorised_at_server,
      'row_signature', audit_work.pre_row_json->>'row_signature'
    ),
    JSONB_BUILD_OBJECT(
      'row', COALESCE(audit_post.row_json, JSONB_BUILD_OBJECT()),
      'processing_status', audit_updated.processing_status,
      'authorised_at_server', CASE WHEN 'UNAUTHORISE' = 'AUTHORISE' THEN v_now ELSE NULL::timestamp with time zone END,
      'row_signature', audit_post.row_json->>'row_signature'
    ),
    'BULK_UNAUTHORISE',
    p_actor_user_id
  )
  FROM pg_temp.timesheet_unauthorise_bulk_work AS audit_work
  JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS audit_updated
    ON audit_updated.timesheet_id = audit_work.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_post_decisions AS audit_post
    ON CASE WHEN COALESCE(audit_post.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (audit_post.row_json->>'timesheet_id')::uuid ELSE NULL::uuid END = audit_work.current_timesheet_id
  WHERE audit_work.failure_code IS NULL;

  CREATE TEMP TABLE timesheet_unauthorise_bulk_results ON COMMIT DROP AS
  SELECT
    work_rows.ordinal,
    CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN TRUE ELSE FALSE END AS success,
    JSONB_BUILD_OBJECT(
      'item_index', work_rows.ordinal,
      'success', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN TRUE ELSE FALSE END,
      'action', 'UNAUTHORISE',
      'error_code', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL::text ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') END,
      'message', CASE
        WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN NULL::text
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'TIMESHEET_MOVED' THEN 'Timesheet has moved to a different current row.'
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'TIMESHEET_LOCKED_OR_PAID' THEN 'Timesheet is locked, invoice-locked, or paid.'
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'ROW_SIGNATURE_MISMATCH' THEN 'Timesheet changed after it was loaded. Refresh the row and try again.'
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'NOT_AUTHORISED' THEN 'Timesheet is not currently authorised.'
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'UNAUTHORISE_NOT_ALLOWED' THEN 'Timesheet is not eligible for bulk unauthorisation.'
        WHEN COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED') = 'CONTRACT_WEEK_NOT_FOUND_FOR_WEEKLY_TIMESHEET' THEN 'Contract week was not found for this weekly timesheet.'
        ELSE COALESCE(work_rows.failure_code, 'MUTATION_UPDATE_FAILED')
      END,
      'requested_timesheet_id', work_rows.requested_timesheet_id,
      'expected_timesheet_id', work_rows.expected_timesheet_id,
      'expected_row_signature', work_rows.expected_row_signature,
      'current_row_signature', work_rows.pre_row_json->>'row_signature',
      'current_timesheet_id', work_rows.current_timesheet_id,
      'current_version', COALESCE(updated_ts.version, work_rows.current_version),
      'was_stale', work_rows.requested_timesheet_id IS DISTINCT FROM work_rows.current_timesheet_id,
      'processing_status_before', work_rows.tsfin_processing_status,
      'processing_status_after', updated_tf.processing_status,
      'row_key_before', COALESCE(work_rows.row_key, work_rows.pre_row_json->>'row_key'),
      'row_key_after', COALESCE(post_rows.row_json->>'row_key', work_rows.pre_row_json->>'row_key'),
      'row', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN COALESCE(post_rows.row_json, JSONB_BUILD_OBJECT()) ELSE COALESCE(work_rows.pre_row_json, JSONB_BUILD_OBJECT()) END,
      'row_patch', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN COALESCE(post_rows.row_json->'row_patch', JSONB_BUILD_OBJECT()) ELSE JSONB_BUILD_OBJECT() END,
      'cache_invalidation_hints', CASE WHEN work_rows.failure_code IS NULL AND updated_tf.timesheet_id IS NOT NULL THEN JSONB_BUILD_OBJECT(
        'row_keys', JSONB_BUILD_ARRAY(COALESCE(work_rows.row_key, work_rows.pre_row_json->>'row_key'), post_rows.row_json->>'row_key'),
        'timesheet_ids', JSONB_BUILD_ARRAY(work_rows.requested_timesheet_id, work_rows.current_timesheet_id),
        'storage_keys', '[]'::jsonb,
        'row_signature', post_rows.row_json->>'row_signature',
        'status_only', TRUE,
        'invalidate_context', FALSE,
        'invalidate_row_context', FALSE,
        'invalidate_preview', FALSE,
        'invalidate_evidence', FALSE
      ) ELSE JSONB_BUILD_OBJECT() END
    ) AS result_json
  FROM pg_temp.timesheet_unauthorise_bulk_work AS work_rows
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_updated_ts AS updated_ts ON updated_ts.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_updated_tf AS updated_tf ON updated_tf.timesheet_id = work_rows.current_timesheet_id
  LEFT JOIN pg_temp.timesheet_unauthorise_bulk_post_decisions AS post_rows ON CASE WHEN COALESCE(post_rows.row_json->>'timesheet_id', '') ~* v_uuid_re THEN (post_rows.row_json->>'timesheet_id')::uuid ELSE NULL::uuid END = work_rows.current_timesheet_id;

  SELECT
    COUNT(result_rows.result_json) FILTER (WHERE result_rows.success = TRUE)::integer,
    COUNT(result_rows.result_json) FILTER (WHERE result_rows.success = FALSE)::integer
  INTO v_success_count, v_failure_count
  FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows;

  SELECT JSONB_BUILD_OBJECT(
    'ok', TRUE,
    'batch_completed', TRUE,
    'all_success', v_failure_count = 0,
    'action', 'UNAUTHORISE',
    'requested_count', JSONB_ARRAY_LENGTH(v_items_array),
    'success_count', v_success_count,
    'failure_count', v_failure_count,
    'has_failures', v_failure_count > 0,
    'results', COALESCE((SELECT JSONB_AGG(result_rows.result_json ORDER BY result_rows.ordinal ASC) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows), '[]'::jsonb),
    'row_patches', COALESCE((SELECT JSONB_AGG(result_rows.result_json->'row_patch' ORDER BY result_rows.ordinal ASC) FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows WHERE result_rows.success = TRUE), '[]'::jsonb),
    'count_deltas', JSONB_BUILD_OBJECT('processed_eligible', v_success_count, 'authorised_eligible', -v_success_count, 'total', 0),
    'cache_invalidation_hints', JSONB_BUILD_OBJECT(
      'row_keys', COALESCE((SELECT JSONB_AGG(TO_JSONB(distinct_row_keys.row_key)) FROM (SELECT DISTINCT cache_key_values.cache_key AS row_key FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(result_rows.result_json->'cache_invalidation_hints'->'row_keys', '[]'::jsonb)) AS cache_key_values(cache_key) WHERE result_rows.success = TRUE AND NULLIF(BTRIM(COALESCE(cache_key_values.cache_key, '')), '') IS NOT NULL) AS distinct_row_keys), '[]'::jsonb),
      'timesheet_ids', COALESCE((SELECT JSONB_AGG(TO_JSONB(distinct_timesheet_ids.timesheet_id_text)) FROM (SELECT DISTINCT cache_timesheet_values.timesheet_id_text FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(result_rows.result_json->'cache_invalidation_hints'->'timesheet_ids', '[]'::jsonb)) AS cache_timesheet_values(timesheet_id_text) WHERE result_rows.success = TRUE AND NULLIF(BTRIM(COALESCE(cache_timesheet_values.timesheet_id_text, '')), '') IS NOT NULL) AS distinct_timesheet_ids), '[]'::jsonb),
      'storage_keys', COALESCE((SELECT JSONB_AGG(TO_JSONB(distinct_storage_keys.storage_key)) FROM (SELECT DISTINCT cache_storage_values.storage_key FROM pg_temp.timesheet_unauthorise_bulk_results AS result_rows CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(result_rows.result_json->'cache_invalidation_hints'->'storage_keys', '[]'::jsonb)) AS cache_storage_values(storage_key) WHERE result_rows.success = TRUE AND NULLIF(BTRIM(COALESCE(cache_storage_values.storage_key, '')), '') IS NOT NULL) AS distinct_storage_keys), '[]'::jsonb),
      'datasets', JSONB_BUILD_ARRAY('bulk_authorise')
    )
  ) INTO v_out;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT('ok', TRUE, 'batch_completed', TRUE, 'all_success', TRUE, 'action', 'UNAUTHORISE', 'requested_count', 0, 'success_count', 0, 'failure_count', 0, 'has_failures', FALSE, 'results', '[]'::jsonb, 'row_patches', '[]'::jsonb, 'count_deltas', JSONB_BUILD_OBJECT('processed_eligible', 0, 'authorised_eligible', 0, 'total', 0), 'cache_invalidation_hints', JSONB_BUILD_OBJECT('row_keys', '[]'::jsonb, 'timesheet_ids', '[]'::jsonb, 'storage_keys', '[]'::jsonb, 'datasets', JSONB_BUILD_ARRAY('bulk_authorise'))));
END;
$function$;





CREATE OR REPLACE FUNCTION public.bulk_process_dataset_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_show_weekly_manual boolean := TRUE;
  v_show_daily_manual boolean := TRUE;
  v_bucket text := NULL;
  v_limit_text text := NULL;
  v_offset_text text := NULL;
  v_week_ending_from_text text := NULL;
  v_week_ending_to_text text := NULL;
  v_week_ending_from date := NULL;
  v_week_ending_to date := NULL;
  v_limit integer := NULL;
  v_offset integer := 0;
  v_out jsonb;
BEGIN
  v_show_weekly_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_weekly_manual', v_filters->>'showWeeklyManual', v_filters->>'show_weekly', v_filters->>'showWeekly', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_daily_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_daily_manual', v_filters->>'showDailyManual', v_filters->>'show_daily', v_filters->>'showDaily', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_bucket := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'bucket', v_filters->>'bulk_process_bucket', v_filters->>'bulkProcessBucket', '')), ''));
  IF v_bucket NOT IN ('UNPROCESSED', 'PROCESSED') THEN
    v_bucket := NULL;
  END IF;

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

  v_week_ending_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_from', v_filters->>'weekEndingFrom', '')), '');
  v_week_ending_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_to', v_filters->>'weekEndingTo', '')), '');

  BEGIN
    IF v_week_ending_from_text IS NOT NULL THEN
      v_week_ending_from := v_week_ending_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_from := NULL;
  END;

  BEGIN
    IF v_week_ending_to_text IS NOT NULL THEN
      v_week_ending_to := v_week_ending_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_to := NULL;
  END;

  WITH lightweight_rows AS MATERIALIZED (
    SELECT summary_row.*
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters || JSONB_BUILD_OBJECT(
        'disable_paging', TRUE,
        'disablePaging', TRUE,
        'apply_paging', FALSE,
        'applyPaging', FALSE,
        'profile', 'list',
        'context_profile', 'list',
        'include_evidence', FALSE,
        'include_compare', FALSE,
        'include_import_source_rows', FALSE
      )
    ) AS summary_row
    WHERE summary_row.timesheet_id IS NOT NULL
       OR summary_row.contract_week_id IS NOT NULL
  ),
  source_rows AS MATERIALIZED (
    SELECT
      lightweight_rows.*,
      current_timesheet.is_adjustment AS current_timesheet_is_adjustment,
      current_timesheet.parent_timesheet_id AS current_timesheet_parent_timesheet_id,
      current_timesheet.adjustment_origin AS current_timesheet_adjustment_origin,
      current_timesheet.correction_id AS current_timesheet_correction_id,
      current_timesheet.correction_kind AS current_timesheet_correction_kind,
      (
        COALESCE(current_timesheet.is_adjustment, FALSE) = TRUE
        AND (
          LEFT(UPPER(COALESCE(NULLIF(BTRIM(current_timesheet.adjustment_origin::text), ''), '')), 7) = 'IMPORT_'
          OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_id::text, '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_kind::text, '')), '') IS NOT NULL
        )
      ) AS is_import_derived_adjustment_calc,
      (
        (
          COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE
          OR COALESCE(current_timesheet.is_adjustment, FALSE) = TRUE
          OR current_timesheet.parent_timesheet_id IS NOT NULL
        )
        AND (
          UPPER(COALESCE(lightweight_rows.route_family, '')) IN ('MANUAL', 'MANUAL_NON_QR', 'QR')
          OR UPPER(COALESCE(lightweight_rows.submission_mode, lightweight_rows.submission_mode_snapshot, '')) = 'MANUAL'
          OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE
          OR UPPER(COALESCE(lightweight_rows.qr_status, '')) IN ('PENDING', 'SIGNED', 'SCANNED', 'USED')
        )
        AND NOT (
          COALESCE(current_timesheet.is_adjustment, FALSE) = TRUE
          AND (
            LEFT(UPPER(COALESCE(NULLIF(BTRIM(current_timesheet.adjustment_origin::text), ''), '')), 7) = 'IMPORT_'
            OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_id::text, '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(current_timesheet.correction_kind::text, '')), '') IS NOT NULL
          )
        )
      ) AS is_user_created_manual_qr_adjustment_calc
    FROM lightweight_rows
    LEFT JOIN public.timesheets AS current_timesheet
      ON current_timesheet.timesheet_id = lightweight_rows.timesheet_id
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      source_rows.*,
      CASE
        WHEN source_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || source_rows.timesheet_id::text
        WHEN source_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || source_rows.contract_week_id::text
        ELSE NULL::text
      END AS row_key_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'QR' OR COALESCE(source_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(source_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') THEN 'NHSP'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'QR' OR COALESCE(source_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(source_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'IMPORT'
        WHEN UPPER(COALESCE(source_rows.underlying_channel_family, source_rows.route_family, '')) = 'QR' OR COALESCE(source_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(source_rows.underlying_channel_family, source_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(source_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      CASE
        WHEN UPPER(COALESCE(source_rows.sheet_scope, source_rows.route_subfamily, '')) = 'DAILY' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type_calc,
      CASE
        WHEN source_rows.timesheet_id IS NULL
          OR UPPER(COALESCE(source_rows.processing_status, source_rows.tools_stage, source_rows.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(source_rows.summary_stage, '')) = 'UNPROCESSED' THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(source_rows.paid_at_utc, source_rows.pay_paid_at_utc) IS NOT NULL
        OR COALESCE(source_rows.invoice_is_paid, FALSE) = TRUE
        OR COALESCE(source_rows.invoice_segments_locked, 0) > 0
      ) AS locked_calc,
      COALESCE(source_rows.is_authorised, FALSE) AS authorised_calc,
      (
        UPPER(COALESCE(source_rows.processing_status, '')) = 'PENDING_AUTH'
        OR UPPER(COALESCE(source_rows.processing_status, '')) = 'READY_FOR_HR'
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(source_rows.qr_status, '')) = 'PENDING'
        AND source_rows.timesheet_id IS NOT NULL
      ) AS qr_unsigned_blocked_calc,
      (
        COALESCE(source_rows.validation_status, '') <> ''
        AND UPPER(COALESCE(source_rows.validation_status, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN', 'OK', 'VALID')
      ) AS hr_validation_awaiting_calc,
      CASE
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HR'
        WHEN source_rows.is_import_derived_adjustment_calc = TRUE
          AND UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP') THEN 'NHSP'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(source_rows.client_no_timesheet_required, FALSE) = TRUE
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'HR'
        WHEN UPPER(COALESCE(source_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND source_rows.is_user_created_manual_qr_adjustment_calc = FALSE THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      MD5(CONCAT_WS('|',
        COALESCE(source_rows.timesheet_id::text, ''),
        COALESCE(source_rows.contract_week_id::text, ''),
        COALESCE(source_rows.processing_status, ''),
        COALESCE(source_rows.summary_stage, ''),
        COALESCE(source_rows.tools_stage, ''),
        COALESCE(source_rows.authorised_at_utc::text, ''),
        COALESCE(source_rows.authorised_at_server::text, ''),
        COALESCE(source_rows.processed_at_utc::text, ''),
        COALESCE(source_rows.paid_at_utc::text, ''),
        COALESCE(source_rows.invoice_segments_locked::text, ''),
        COALESCE(source_rows.route_family, ''),
        COALESCE(source_rows.route_subfamily, ''),
        COALESCE(source_rows.validation_status, ''),
        COALESCE(source_rows.attached_evidence_count::text, ''),
        COALESCE(source_rows.primary_artifact_storage_key, ''),
        COALESCE(source_rows.is_import_derived_adjustment_calc::text, ''),
        COALESCE(source_rows.is_user_created_manual_qr_adjustment_calc::text, ''),
        COALESCE(source_rows.current_timesheet_adjustment_origin::text, ''),
        COALESCE(source_rows.current_timesheet_correction_id::text, ''),
        COALESCE(source_rows.current_timesheet_correction_kind::text, '')
      )) AS row_signature_calc
    FROM source_rows
    WHERE (v_week_ending_from IS NULL OR source_rows.week_ending_date >= v_week_ending_from)
      AND (v_week_ending_to IS NULL OR source_rows.week_ending_date <= v_week_ending_to)
  ),
  eligibility_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      (
        (
          classified_rows.route_family_calc = 'MANUAL_NON_QR'
          OR COALESCE(classified_rows.is_user_created_manual_qr_adjustment_calc, FALSE) = TRUE
        )
        AND COALESCE(classified_rows.is_import_derived_adjustment_calc, FALSE) = FALSE
        AND classified_rows.route_family_calc NOT IN ('IMPORT_AUTHORITATIVE', 'ELECTRONIC')
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
      ) AS bulk_process_user_adjustment_eligible_calc
    FROM classified_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      eligibility_rows.*,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      ) AS can_save_calc,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      ) AS can_edit_timesheet_data_calc,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
        AND eligibility_rows.bulk_process_bucket_calc = 'UNPROCESSED'
      ) AS can_process_calc,
      (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
        AND eligibility_rows.bulk_process_bucket_calc = 'PROCESSED'
      ) AS can_unprocess_calc,
      (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.requires_authorisation_calc = TRUE
        AND eligibility_rows.authorised_calc = FALSE
        AND eligibility_rows.qr_unsigned_blocked_calc = FALSE
      ) AS can_bulk_authorise_calc,
      (
        eligibility_rows.timesheet_id IS NOT NULL
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = TRUE
      ) AS can_bulk_unauthorise_calc,
      (
        (eligibility_rows.timesheet_id IS NOT NULL OR eligibility_rows.contract_week_id IS NOT NULL)
        AND eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      ) AS can_manage_evidence_calc,
      (
        eligibility_rows.locked_calc = FALSE
        AND eligibility_rows.authorised_calc = FALSE
        AND COALESCE(eligibility_rows.is_adjusted, FALSE) = FALSE
      ) AS can_add_additional_manual_calc,
      (
        eligibility_rows.locked_calc = TRUE
        OR eligibility_rows.authorised_calc = TRUE
        OR eligibility_rows.bulk_process_user_adjustment_eligible_calc = FALSE
      ) AS review_only_calc
    FROM eligibility_rows
  ),
  evidence_target_rows AS MATERIALIZED (
    SELECT DISTINCT
      decision_rows.row_key_calc AS row_key_calc,
      decision_rows.timesheet_id AS timesheet_id,
      decision_rows.contract_week_id AS contract_week_id
    FROM decision_rows
    WHERE decision_rows.row_key_calc IS NOT NULL
      AND decision_rows.bulk_process_user_adjustment_eligible_calc = TRUE
      AND (
        (UPPER(COALESCE(decision_rows.period_type_calc, decision_rows.sheet_scope, '')) = 'WEEKLY' AND v_show_weekly_manual = TRUE)
        OR (UPPER(COALESCE(decision_rows.period_type_calc, decision_rows.sheet_scope, '')) = 'DAILY' AND v_show_daily_manual = TRUE)
        OR UPPER(COALESCE(decision_rows.period_type_calc, decision_rows.sheet_scope, '')) NOT IN ('WEEKLY', 'DAILY')
      )
      AND (
        (
          decision_rows.bulk_process_bucket_calc = 'UNPROCESSED'
          AND (v_bucket IS NULL OR v_bucket = 'UNPROCESSED')
          AND decision_rows.can_process_calc = TRUE
        )
        OR (
          decision_rows.bulk_process_bucket_calc = 'PROCESSED'
          AND (v_bucket IS NULL OR v_bucket = 'PROCESSED')
          AND decision_rows.timesheet_id IS NOT NULL
          AND decision_rows.can_unprocess_calc = TRUE
        )
      )
  ),
  evidence_kind_rows AS MATERIALIZED (
    SELECT
      evidence_target_rows.row_key_calc AS row_key_calc,
      evidence_target_rows.timesheet_id AS timesheet_id,
      evidence_target_rows.contract_week_id AS contract_week_id,
      timesheet_evidence.id AS evidence_id,
      CASE
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('TIMESHEET', 'TS') THEN 'TIMESHEET'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('MILEAGE', 'MILES', 'MILE') THEN 'MILEAGE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) = 'TRAVEL' THEN 'TRAVEL'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('ACCOMMODATION', 'ACCOM') THEN 'ACCOMMODATION'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) = 'OTHER' THEN 'OTHER'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IN ('EXPENSE', 'EXPENSES') THEN 'EXPENSE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), '')) IS NOT NULL THEN UPPER(NULLIF(BTRIM(COALESCE(timesheet_evidence.kind::text, '')), ''))
        ELSE 'OTHER'
      END AS evidence_kind_norm,
      NULLIF(BTRIM(COALESCE(timesheet_evidence.display_name, '')), '') AS evidence_display_name,
      NULLIF(BTRIM(COALESCE(timesheet_evidence.storage_key, '')), '') AS evidence_storage_key,
      timesheet_evidence.created_at AS evidence_created_at
    FROM evidence_target_rows AS evidence_target_rows
    JOIN public.timesheet_evidence AS timesheet_evidence
      ON timesheet_evidence.timesheet_id = evidence_target_rows.timesheet_id
    WHERE evidence_target_rows.timesheet_id IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(timesheet_evidence.storage_key, '')), '') IS NOT NULL

    UNION ALL

    SELECT
      evidence_target_rows.row_key_calc AS row_key_calc,
      NULL::uuid AS timesheet_id,
      evidence_target_rows.contract_week_id AS contract_week_id,
      manual_timesheet_queue.id AS evidence_id,
      CASE
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('TIMESHEET', 'TS') THEN 'TIMESHEET'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('MILEAGE', 'MILES', 'MILE') THEN 'MILEAGE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) = 'TRAVEL' THEN 'TRAVEL'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('ACCOMMODATION', 'ACCOM') THEN 'ACCOMMODATION'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) = 'OTHER' THEN 'OTHER'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IN ('EXPENSE', 'EXPENSES') THEN 'EXPENSE'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), '')) IS NOT NULL THEN UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'staged_kind', manual_timesheet_queue.meta_json->>'kind', '')), ''))
        ELSE 'OTHER'
      END AS evidence_kind_norm,
      NULLIF(BTRIM(COALESCE(manual_timesheet_queue.original_filename, '')), '') AS evidence_display_name,
      NULLIF(BTRIM(COALESCE(manual_timesheet_queue.r2_key, '')), '') AS evidence_storage_key,
      manual_timesheet_queue.uploaded_at_utc AS evidence_created_at
    FROM evidence_target_rows AS evidence_target_rows
    JOIN public.manual_timesheet_queue AS manual_timesheet_queue
      ON evidence_target_rows.timesheet_id IS NULL
     AND evidence_target_rows.contract_week_id IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(manual_timesheet_queue.meta_json->>'contract_week_id', '')), '') = evidence_target_rows.contract_week_id::text
    WHERE UPPER(NULLIF(BTRIM(COALESCE(manual_timesheet_queue.status, '')), '')) = 'STAGED'
      AND manual_timesheet_queue.timesheet_id IS NULL
      AND NULLIF(BTRIM(COALESCE(manual_timesheet_queue.r2_key, '')), '') IS NOT NULL
  ),
  evidence_kind_ranked AS MATERIALIZED (
    SELECT
      evidence_kind_rows.*,
      ROW_NUMBER() OVER (
        PARTITION BY evidence_kind_rows.row_key_calc
        ORDER BY
          CASE evidence_kind_rows.evidence_kind_norm
            WHEN 'TIMESHEET' THEN 1
            WHEN 'MILEAGE' THEN 2
            WHEN 'TRAVEL' THEN 3
            WHEN 'ACCOMMODATION' THEN 4
            WHEN 'OTHER' THEN 5
            ELSE 6
          END,
          evidence_kind_rows.evidence_kind_norm ASC,
          evidence_kind_rows.evidence_created_at ASC NULLS LAST,
          evidence_kind_rows.evidence_id ASC
      ) AS evidence_rank
    FROM evidence_kind_rows
  ),
  evidence_kind_counts AS MATERIALIZED (
    SELECT
      evidence_kind_ranked.row_key_calc AS row_key_calc,
      evidence_kind_ranked.evidence_kind_norm AS evidence_kind_norm,
      COUNT(*)::integer AS evidence_kind_count,
      CASE evidence_kind_ranked.evidence_kind_norm
        WHEN 'TIMESHEET' THEN 1
        WHEN 'MILEAGE' THEN 2
        WHEN 'TRAVEL' THEN 3
        WHEN 'ACCOMMODATION' THEN 4
        WHEN 'OTHER' THEN 5
        ELSE 6
      END AS evidence_kind_sort
    FROM evidence_kind_ranked
    GROUP BY evidence_kind_ranked.row_key_calc, evidence_kind_ranked.evidence_kind_norm
  ),
  evidence_kind_extra_badges AS MATERIALIZED (
    SELECT
      evidence_kind_counts.row_key_calc AS row_key_calc,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'kind', evidence_kind_counts.evidence_kind_norm,
          'present', evidence_kind_counts.evidence_kind_count > 0,
          'has_evidence', evidence_kind_counts.evidence_kind_count > 0,
          'count', evidence_kind_counts.evidence_kind_count
        )
        ORDER BY evidence_kind_counts.evidence_kind_sort, evidence_kind_counts.evidence_kind_norm
      ) AS extra_evidence_badges
    FROM evidence_kind_counts
    WHERE evidence_kind_counts.evidence_kind_norm NOT IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER')
    GROUP BY evidence_kind_counts.row_key_calc
  ),
  evidence_kind_summary AS MATERIALIZED (
    SELECT
      evidence_kind_ranked.row_key_calc AS row_key_calc,
      NULLIF(MAX(evidence_kind_ranked.timesheet_id::text), '')::uuid AS timesheet_id,
      NULLIF(MAX(evidence_kind_ranked.contract_week_id::text), '')::uuid AS contract_week_id,
      COUNT(*)::integer AS total_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'TIMESHEET')::integer AS timesheet_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'MILEAGE')::integer AS mileage_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'TRAVEL')::integer AS travel_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'ACCOMMODATION')::integer AS accommodation_evidence_count,
      COUNT(*) FILTER (WHERE evidence_kind_ranked.evidence_kind_norm = 'OTHER')::integer AS other_evidence_count,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN evidence_kind_ranked.evidence_id::text ELSE NULL::text END) AS primary_artifact_id,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN evidence_kind_ranked.evidence_kind_norm ELSE NULL::text END) AS primary_artifact_kind,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN evidence_kind_ranked.evidence_storage_key ELSE NULL::text END) AS primary_artifact_storage_key,
      MAX(CASE WHEN evidence_kind_ranked.evidence_rank = 1 THEN COALESCE(evidence_kind_ranked.evidence_display_name, REGEXP_REPLACE(evidence_kind_ranked.evidence_storage_key, '^.*/', '')) ELSE NULL::text END) AS primary_artifact_display_name
    FROM evidence_kind_ranked
    GROUP BY evidence_kind_ranked.row_key_calc
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      (JSONB_BUILD_OBJECT(
        'row_key',
        decision_rows.row_key_calc,
        'stable_row_id',
        COALESCE(decision_rows.timesheet_id::text, decision_rows.contract_week_id::text),
        'row_type',
        CASE WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet' ELSE 'contract_week' END,
        'timesheet_id',
        decision_rows.timesheet_id,
        'current_timesheet_id',
        decision_rows.timesheet_id,
        'requested_timesheet_id',
        decision_rows.timesheet_id,
        'expected_timesheet_id',
        decision_rows.timesheet_id,
        'contract_week_id',
        decision_rows.contract_week_id,
        'contract_id',
        decision_rows.contract_id,
        'candidate_id',
        decision_rows.candidate_id,
        'candidate_name',
        decision_rows.candidate_name,
        'candidate_display_name',
        decision_rows.candidate_display_name,
        'candidate_first_name',
        NULL::text,
        'candidate_surname',
        NULL::text,
        'client_id',
        decision_rows.client_id,
        'client_name',
        decision_rows.client_name,
        'booking_id',
        decision_rows.booking_id,
        'external_ref',
        decision_rows.booking_id,
        'occupant_key_norm',
        decision_rows.occupant_key_norm,
        'hospital_norm',
        decision_rows.hospital_norm,
        'candidate_hint_text',
        COALESCE(decision_rows.candidate_hint_text, '{}'::jsonb),
        'week_ending_date',
        decision_rows.week_ending_date,
        'contract_week_ending_date',
        decision_rows.week_ending_date,
        'work_date',
        decision_rows.work_date,
        'date',
        COALESCE(decision_rows.work_date, decision_rows.week_ending_date),
        'shift_date',
        decision_rows.work_date,
        'period_type',
        decision_rows.period_type_calc,
        'sheet_scope',
        decision_rows.sheet_scope,
        'submission_mode',
        decision_rows.submission_mode,
        'submission_mode_snapshot',
        decision_rows.submission_mode_snapshot,
        'basis',
        decision_rows.basis,
        'route_type',
        decision_rows.route_type,
        'route_display',
        decision_rows.route_display,
        'route_family',
        decision_rows.route_family_calc,
        'route_subfamily',
        decision_rows.route_subfamily_calc,
        'underlying_channel_family',
        decision_rows.underlying_channel_family_calc,
        'bulk_process_user_adjustment_eligible',
        decision_rows.bulk_process_user_adjustment_eligible_calc,
        'is_import_authoritative',
        decision_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
        'is_import_derived_adjustment',
        COALESCE(decision_rows.is_import_derived_adjustment_calc, FALSE),
        'is_user_created_manual_qr_adjustment',
        COALESCE(decision_rows.is_user_created_manual_qr_adjustment_calc, FALSE),
        'adjustment_source',
        CASE
          WHEN decision_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_DERIVED'
          WHEN decision_rows.is_user_created_manual_qr_adjustment_calc = TRUE THEN 'USER_CREATED_MANUAL_QR'
          ELSE NULL::text
        END,
        'bulk_process_excluded_reason',
        CASE
          WHEN decision_rows.is_import_derived_adjustment_calc = TRUE THEN 'IMPORT_AUTHORITATIVE_ADJUSTED_HOURS'
          ELSE NULL::text
        END,
        'current_timesheet_is_adjustment',
        decision_rows.current_timesheet_is_adjustment,
        'parent_timesheet_id',
        decision_rows.current_timesheet_parent_timesheet_id,
        'adjustment_origin',
        decision_rows.current_timesheet_adjustment_origin,
        'correction_id',
        decision_rows.current_timesheet_correction_id,
        'correction_kind',
        decision_rows.current_timesheet_correction_kind,
        'summary_stage',
        decision_rows.summary_stage,
        'tools_stage',
        decision_rows.tools_stage,
        'processing_status',
        decision_rows.processing_status
      )
      || JSONB_BUILD_OBJECT(
        'processing_status_display',
        decision_rows.processing_status_display
      )
      || JSONB_BUILD_OBJECT(
        'bulk_process_bucket',
        decision_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification',
        decision_rows.bulk_authorise_classification_calc,
        'bulk_authorise_section',
        CASE WHEN decision_rows.can_bulk_authorise_calc THEN 'processed_eligible' WHEN decision_rows.can_bulk_unauthorise_calc THEN 'authorised_eligible' ELSE NULL::text END,
        'authorised_at_utc',
        decision_rows.authorised_at_utc,
        'authorised_at_server',
        decision_rows.authorised_at_server,
        'processed_at_utc',
        decision_rows.processed_at_utc,
        'is_authorised',
        decision_rows.authorised_calc,
        'locked',
        decision_rows.locked_calc,
        'total_hours',
        decision_rows.total_hours,
        'total_pay_ex_vat',
        decision_rows.total_pay_ex_vat,
        'total_charge_ex_vat',
        decision_rows.total_charge_ex_vat,
        'margin_ex_vat',
        decision_rows.margin_ex_vat,
        'net_delta_ex_vat',
        decision_rows.net_delta_ex_vat,
        'paid_at_utc',
        decision_rows.paid_at_utc,
        'pay_icon_code',
        decision_rows.pay_icon_code,
        'pay_status_code',
        decision_rows.pay_status_code,
        'pay_paid_at_utc',
        decision_rows.pay_paid_at_utc,
        'ready_to_pay',
        FALSE,
        'pay_on_hold',
        FALSE,
        'invoice_is_paid',
        decision_rows.invoice_is_paid,
        'invoice_issue_stage',
        decision_rows.invoice_issue_stage,
        'invoice_segment_stage',
        decision_rows.invoice_segment_stage,
        'invoice_segments_total',
        COALESCE(decision_rows.invoice_segments_total, 0),
        'invoice_segments_locked',
        COALESCE(decision_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked',
        COALESCE(decision_rows.invoice_segments_unlocked, 0),
        'issue_codes',
        COALESCE(TO_JSONB(decision_rows.issue_codes), '[]'::jsonb),
        'validation_status',
        decision_rows.validation_status,
        'validation_summary',
        decision_rows.validation_summary,
        'validation_pre_validated',
        FALSE,
        'hr_validation_awaiting',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_required_for_invoice',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_satisfied',
        NOT decision_rows.hr_validation_awaiting_calc,
        'hr_crosscheck_status',
        decision_rows.hr_crosscheck_status,
        'hr_crosscheck_issues',
        COALESCE(TO_JSONB(decision_rows.hr_crosscheck_issues), '[]'::jsonb),
        'qr_status',
        decision_rows.qr_status,
        'is_qr',
        decision_rows.is_qr,
        'qr_signed_at_utc',
        NULL::text,
        'can_allow_qr_again',
        FALSE,
        'can_allow_electronic_again',
        FALSE,
        'can_switch_to_manual',
        FALSE
      )
      || JSONB_BUILD_OBJECT(
        'can_revert_to_electronic',
        FALSE,
        'can_convert_qr_to_manual_only',
        FALSE,
        'qr_email_can_send_now',
        FALSE,
        'qr_email_recipient_available',
        FALSE,
        'is_adjusted',
        (COALESCE(decision_rows.current_timesheet_is_adjustment, FALSE) OR COALESCE(decision_rows.is_adjusted, FALSE)),
        'is_adjustment',
        (COALESCE(decision_rows.current_timesheet_is_adjustment, FALSE) OR COALESCE(decision_rows.is_adjusted, FALSE)),
        'needs_attention',
        decision_rows.needs_attention,
        'has_rate_issue',
        decision_rows.has_rate_issue,
        'has_pay_channel_issue',
        decision_rows.has_pay_channel_issue,
        'client_no_timesheet_required',
        decision_rows.client_no_timesheet_required,
        'client_autoprocess_hr',
        decision_rows.client_autoprocess_hr,
        'client_is_nhsp',
        decision_rows.client_is_nhsp,
        'has_deviation_marker',
        FALSE,
        'deviation_marker_reason',
        NULL::text,
        'nhsp_highlight_red',
        FALSE,
        'nhsp_highlight_reason',
        NULL::text,
        'nhsp_deviation_pct',
        NULL::numeric,
        'nhsp_is_ad_hoc',
        FALSE,
        'has_any_evidence',
        COALESCE(evidence_kind_summary.total_evidence_count > 0, decision_rows.has_any_evidence),
        'attached_evidence_count',
        COALESCE(evidence_kind_summary.total_evidence_count, decision_rows.attached_evidence_count, 0),
        'evidence_count',
        COALESCE(evidence_kind_summary.total_evidence_count, decision_rows.attached_evidence_count, 0),
        'primary_artifact_id',
        evidence_kind_summary.primary_artifact_id,
        'primary_artifact_kind',
        evidence_kind_summary.primary_artifact_kind,
        'primary_artifact_storage_key',
        COALESCE(evidence_kind_summary.primary_artifact_storage_key, decision_rows.primary_artifact_storage_key),
        'primary_artifact_display_name',
        COALESCE(evidence_kind_summary.primary_artifact_display_name, decision_rows.primary_artifact_display_name),
        'primary_artifact_preview_mode',
        decision_rows.primary_artifact_preview_mode,
        'manual_pdf_r2_key',
        NULL::text,
        'uploaded_pdf_r2_key',
        NULL::text,
        'generated_pdf_at_utc',
        NULL::text,
        'manual_pdf_rotation_degrees',
        0,
        'queue_staged_count',
        0,
        'evidence_document_locked',
        decision_rows.locked_calc,
        'evidence_lock_reason',
        CASE WHEN decision_rows.locked_calc THEN 'locked_or_paid' ELSE NULL::text END,
        'has_timesheet',
        decision_rows.timesheet_id IS NOT NULL,
        'is_contract_week_only',
        decision_rows.timesheet_id IS NULL AND decision_rows.contract_week_id IS NOT NULL,
        'timesheet_version',
        NULL::integer,
        'updated_at',
        NULL::text,
        'is_current',
        TRUE,
        'was_stale',
        FALSE,
        'timesheet_type_sort_key',
        NULL::text
      )
      || JSONB_BUILD_OBJECT(
        'processed_by_user_id',
        NULL::text,
        'processed_by_display',
        NULL::text,
        'authorised_by_user_id',
        NULL::text,
        'authorised_by_display',
        NULL::text,
        'can_save',
        decision_rows.can_save_calc,
        'can_process',
        decision_rows.can_process_calc,
        'can_unprocess',
        decision_rows.can_unprocess_calc,
        'can_bulk_authorise',
        decision_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise',
        decision_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data',
        decision_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence',
        decision_rows.can_manage_evidence_calc,
        'can_add_additional_manual',
        decision_rows.can_add_additional_manual_calc,
        'review_only',
        decision_rows.review_only_calc,
        'row_signature',
        MD5(CONCAT_WS('|',
          decision_rows.row_signature_calc,
          COALESCE(decision_rows.bulk_process_user_adjustment_eligible_calc::text, ''),
          COALESCE(evidence_kind_summary.primary_artifact_kind, ''),
          COALESCE(evidence_kind_summary.timesheet_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.mileage_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.travel_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.accommodation_evidence_count::text, '0'),
          COALESCE(evidence_kind_summary.other_evidence_count::text, '0'),
          COALESCE(evidence_kind_extra_badges.extra_evidence_badges::text, '[]')
        )),
        'evidence_badges',
        (
          JSONB_BUILD_ARRAY(
            JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.mileage_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.travel_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0)),
            JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.other_evidence_count, 0))
          )
          || COALESCE(evidence_kind_extra_badges.extra_evidence_badges, '[]'::jsonb)
        ),
        'artifact_hints',
        JSONB_BUILD_OBJECT(
          'has_any_evidence', COALESCE(evidence_kind_summary.total_evidence_count > 0, decision_rows.has_any_evidence),
          'attached_evidence_count', COALESCE(evidence_kind_summary.total_evidence_count, decision_rows.attached_evidence_count, 0),
          'primary_artifact_id', evidence_kind_summary.primary_artifact_id,
          'primary_artifact_kind', evidence_kind_summary.primary_artifact_kind,
          'primary_artifact_storage_key', COALESCE(evidence_kind_summary.primary_artifact_storage_key, decision_rows.primary_artifact_storage_key),
          'primary_artifact_display_name', COALESCE(evidence_kind_summary.primary_artifact_display_name, decision_rows.primary_artifact_display_name),
          'primary_artifact_preview_mode', decision_rows.primary_artifact_preview_mode,
          'evidence_badges', (
            JSONB_BUILD_ARRAY(
              JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.timesheet_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.mileage_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.mileage_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.travel_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.travel_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.accommodation_evidence_count, 0)),
              JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'has_evidence', COALESCE(evidence_kind_summary.other_evidence_count, 0) > 0, 'count', COALESCE(evidence_kind_summary.other_evidence_count, 0))
            )
            || COALESCE(evidence_kind_extra_badges.extra_evidence_badges, '[]'::jsonb)
          )
        ),
        'action_flags',
        JSONB_BUILD_OBJECT(
          'can_save', decision_rows.can_save_calc,
          'can_process', decision_rows.can_process_calc,
          'can_unprocess', decision_rows.can_unprocess_calc,
          'can_bulk_authorise', decision_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', decision_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', decision_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', decision_rows.can_manage_evidence_calc,
          'can_add_additional_manual', decision_rows.can_add_additional_manual_calc,
          'review_only', decision_rows.review_only_calc
        ),
        'row_patch',
        JSONB_BUILD_OBJECT(),
        'cache_invalidation_hints',
        JSONB_BUILD_OBJECT(),
        'count_deltas',
        JSONB_BUILD_OBJECT(),
        'header_loaded',
        TRUE,
        'header_only',
        TRUE,
        'editor_loaded',
        FALSE,
        'evidence_loaded',
        FALSE,
        'compare_loaded',
        FALSE,
        'full_loaded',
        FALSE,
        'schedule_pending',
        TRUE,
        'schedule_authoritative',
        FALSE,
        'loaded_layers',
        JSONB_BUILD_ARRAY('dataset_row'),
        'dataset_source',
        'timesheet_summary_lightweight_rows_v1'
      )) AS row_json
    FROM decision_rows
    LEFT JOIN evidence_kind_summary AS evidence_kind_summary
      ON evidence_kind_summary.row_key_calc = decision_rows.row_key_calc
    LEFT JOIN evidence_kind_extra_badges AS evidence_kind_extra_badges
      ON evidence_kind_extra_badges.row_key_calc = decision_rows.row_key_calc
  ),
  manual_rows AS MATERIALIZED (
    SELECT payload_rows.row_json
    FROM payload_rows
    WHERE COALESCE((payload_rows.row_json->>'bulk_process_user_adjustment_eligible')::boolean, FALSE) = TRUE
      AND COALESCE((payload_rows.row_json->>'is_import_authoritative')::boolean, FALSE) = FALSE
      AND COALESCE(payload_rows.row_json->>'adjustment_source', '') <> 'IMPORT_DERIVED'
      AND (
        (UPPER(COALESCE(payload_rows.row_json->>'period_type', payload_rows.row_json->>'sheet_scope', '')) = 'WEEKLY' AND v_show_weekly_manual = TRUE)
        OR (UPPER(COALESCE(payload_rows.row_json->>'period_type', payload_rows.row_json->>'sheet_scope', '')) = 'DAILY' AND v_show_daily_manual = TRUE)
        OR UPPER(COALESCE(payload_rows.row_json->>'period_type', payload_rows.row_json->>'sheet_scope', '')) NOT IN ('WEEKLY', 'DAILY')
      )
  ),
  unprocessed_rows AS MATERIALIZED (
    SELECT manual_rows.row_json
    FROM manual_rows
    WHERE UPPER(COALESCE(manual_rows.row_json->>'bulk_process_bucket', '')) = 'UNPROCESSED'
      AND (v_bucket IS NULL OR v_bucket = 'UNPROCESSED')
      AND COALESCE((manual_rows.row_json->>'can_process')::boolean, FALSE) = TRUE
  ),
  processed_rows AS MATERIALIZED (
    SELECT manual_rows.row_json
    FROM manual_rows
    WHERE UPPER(COALESCE(manual_rows.row_json->>'bulk_process_bucket', '')) = 'PROCESSED'
      AND (v_bucket IS NULL OR v_bucket = 'PROCESSED')
      AND NULLIF(BTRIM(COALESCE(manual_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND COALESCE((manual_rows.row_json->>'can_unprocess')::boolean, FALSE) = TRUE
  ),
  paged_unprocessed_rows AS MATERIALIZED (
    SELECT unprocessed_rows.row_json
    FROM unprocessed_rows
    ORDER BY unprocessed_rows.row_json->>'week_ending_date', unprocessed_rows.row_json->>'client_name', unprocessed_rows.row_json->>'candidate_name', unprocessed_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  paged_processed_rows AS MATERIALIZED (
    SELECT processed_rows.row_json
    FROM processed_rows
    ORDER BY processed_rows.row_json->>'week_ending_date', processed_rows.row_json->>'client_name', processed_rows.row_json->>'candidate_name', processed_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  counts AS (
    SELECT
      (SELECT COUNT(*)::integer FROM unprocessed_rows) AS unprocessed_count,
      (SELECT COUNT(*)::integer FROM processed_rows) AS processed_count
  )
  SELECT JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'show_weekly_manual', v_show_weekly_manual,
      'show_daily_manual', v_show_daily_manual,
      'bucket', v_bucket,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'week_ending_from', v_week_ending_from,
      'week_ending_to', v_week_ending_to,
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'unprocessed', counts.unprocessed_count,
      'processed', counts.processed_count,
      'total', counts.unprocessed_count + counts.processed_count
    ),
    'unprocessed_rows', COALESCE((
      SELECT JSONB_AGG(paged_unprocessed_rows.row_json ORDER BY paged_unprocessed_rows.row_json->>'week_ending_date', paged_unprocessed_rows.row_json->>'client_name', paged_unprocessed_rows.row_json->>'candidate_name', paged_unprocessed_rows.row_json->>'row_key')
      FROM paged_unprocessed_rows
    ), '[]'::jsonb),
    'processed_rows', COALESCE((
      SELECT JSONB_AGG(paged_processed_rows.row_json ORDER BY paged_processed_rows.row_json->>'week_ending_date', paged_processed_rows.row_json->>'client_name', paged_processed_rows.row_json->>'candidate_name', paged_processed_rows.row_json->>'row_key')
      FROM paged_processed_rows
    ), '[]'::jsonb)
  )
  INTO v_out
  FROM counts;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'show_weekly_manual', v_show_weekly_manual,
      'show_daily_manual', v_show_daily_manual,
      'bucket', v_bucket,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'week_ending_from', v_week_ending_from,
      'week_ending_to', v_week_ending_to,
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT('unprocessed', 0, 'processed', 0, 'total', 0),
    'unprocessed_rows', '[]'::jsonb,
    'processed_rows', '[]'::jsonb
  ));
END;
$function$;


CREATE OR REPLACE FUNCTION public.bulk_authorise_dataset_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_classification text := NULL;
  v_show_daily boolean := TRUE;
  v_show_weekly boolean := TRUE;
  v_show_manual boolean := TRUE;
  v_show_qr boolean := TRUE;
  v_show_electronic boolean := TRUE;
  v_validation_already boolean := TRUE;
  v_validation_awaiting boolean := TRUE;
  v_limit_text text := NULL;
  v_offset_text text := NULL;
  v_limit integer := NULL;
  v_offset integer := 0;
  v_out jsonb;
BEGIN
  v_classification := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'classification', v_filters->>'classificationRaw', '')), ''));
  IF v_classification NOT IN ('TIMESHEETS', 'NHSP', 'HR') THEN
    v_classification := NULL;
  END IF;

  v_show_daily := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_daily', v_filters->>'showDaily', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_weekly := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_weekly', v_filters->>'showWeekly', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_manual := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_manual', v_filters->>'showManual', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_qr := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_qr', v_filters->>'showQr', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_show_electronic := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'show_electronic', v_filters->>'showElectronic', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_validation_already := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'validation_already', v_filters->>'validationAlready', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_validation_awaiting := CASE
    WHEN LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'validation_awaiting', v_filters->>'validationAwaiting', '')), '')) IN ('false', '0', 'no', 'n', 'off') THEN FALSE
    ELSE TRUE
  END;

  v_limit_text := NULLIF(BTRIM(COALESCE(v_filters->>'limit', v_filters->>'page_size', v_filters->>'pageSize', '')), '');
  IF v_limit_text ~ '^[0-9]+$' THEN
    v_limit := GREATEST(1, LEAST(v_limit_text::integer, 1000));
  END IF;

  v_offset_text := NULLIF(BTRIM(COALESCE(v_filters->>'offset', v_filters->>'page_offset', v_filters->>'pageOffset', '')), '');
  IF v_offset_text ~ '^[0-9]+$' THEN
    v_offset := GREATEST(v_offset_text::integer, 0);
  END IF;

  WITH lightweight_rows AS MATERIALIZED (
    SELECT summary_row.*
    FROM public.timesheet_summary_lightweight_rows_v1(
      v_filters || JSONB_BUILD_OBJECT(
        'disable_paging', TRUE,
        'disablePaging', TRUE,
        'apply_paging', FALSE,
        'applyPaging', FALSE,
        'profile', 'list',
        'context_profile', 'list',
        'include_evidence', FALSE,
        'include_compare', FALSE,
        'include_import_source_rows', FALSE
      )
    ) AS summary_row
    WHERE summary_row.timesheet_id IS NOT NULL
       OR summary_row.contract_week_id IS NOT NULL
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      lightweight_rows.*,
      CASE
        WHEN lightweight_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || lightweight_rows.timesheet_id::text
        WHEN lightweight_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || lightweight_rows.contract_week_id::text
        ELSE NULL::text
      END AS row_key_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT_AUTHORITATIVE'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY') THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'NHSP'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'IMPORT'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'IMPORT'
        WHEN UPPER(COALESCE(lightweight_rows.underlying_channel_family, lightweight_rows.route_family, '')) = 'QR' OR COALESCE(lightweight_rows.is_qr, FALSE) = TRUE THEN 'QR'
        WHEN UPPER(COALESCE(lightweight_rows.underlying_channel_family, lightweight_rows.route_family, '')) = 'ELECTRONIC' OR UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.sheet_scope, lightweight_rows.route_subfamily, '')) = 'DAILY' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type_calc,
      CASE
        WHEN lightweight_rows.timesheet_id IS NULL
          OR UPPER(COALESCE(lightweight_rows.processing_status, lightweight_rows.tools_stage, lightweight_rows.summary_stage, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(lightweight_rows.summary_stage, '')) = 'UNPROCESSED' THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(lightweight_rows.paid_at_utc, lightweight_rows.pay_paid_at_utc) IS NOT NULL
        OR COALESCE(lightweight_rows.invoice_is_paid, FALSE) = TRUE
        OR COALESCE(lightweight_rows.invoice_segments_locked, 0) > 0
      ) AS locked_calc,
      COALESCE(lightweight_rows.is_authorised, FALSE) AS authorised_calc,
      (
        UPPER(COALESCE(lightweight_rows.processing_status, '')) = 'PENDING_AUTH'
        OR UPPER(COALESCE(lightweight_rows.processing_status, '')) = 'READY_FOR_HR'
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(lightweight_rows.qr_status, '')) = 'PENDING'
        AND lightweight_rows.timesheet_id IS NOT NULL
      ) AS qr_unsigned_blocked_calc,
      (
        COALESCE(lightweight_rows.validation_status, '') <> ''
        AND UPPER(COALESCE(lightweight_rows.validation_status, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN', 'OK', 'VALID')
      ) AS hr_validation_awaiting_calc,
      CASE
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
          AND COALESCE(lightweight_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HR'
        WHEN UPPER(COALESCE(lightweight_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
          AND NOT (COALESCE(lightweight_rows.is_adjusted, FALSE) = TRUE AND UPPER(COALESCE(lightweight_rows.submission_mode, '')) = 'MANUAL') THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      MD5(CONCAT_WS('|',
        COALESCE(lightweight_rows.timesheet_id::text, ''),
        COALESCE(lightweight_rows.contract_week_id::text, ''),
        COALESCE(lightweight_rows.processing_status, ''),
        COALESCE(lightweight_rows.summary_stage, ''),
        COALESCE(lightweight_rows.tools_stage, ''),
        COALESCE(lightweight_rows.authorised_at_utc::text, ''),
        COALESCE(lightweight_rows.authorised_at_server::text, ''),
        COALESCE(lightweight_rows.processed_at_utc::text, ''),
        COALESCE(lightweight_rows.paid_at_utc::text, ''),
        COALESCE(lightweight_rows.invoice_segments_locked::text, ''),
        COALESCE(lightweight_rows.route_family, ''),
        COALESCE(lightweight_rows.route_subfamily, ''),
        COALESCE(lightweight_rows.validation_status, ''),
        COALESCE(lightweight_rows.attached_evidence_count::text, ''),
        COALESCE(lightweight_rows.primary_artifact_storage_key, '')
      )) AS row_signature_calc
    FROM lightweight_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.requires_authorisation_calc = TRUE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.qr_unsigned_blocked_calc = FALSE
      ) AS can_bulk_authorise_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = TRUE
      ) AS can_bulk_unauthorise_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_save_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR classified_rows.contract_week_id IS NOT NULL)
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
        AND classified_rows.bulk_process_bucket_calc = 'UNPROCESSED'
      ) AS can_process_calc,
      (
        classified_rows.timesheet_id IS NOT NULL
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND classified_rows.route_family_calc = 'MANUAL_NON_QR'
        AND classified_rows.bulk_process_bucket_calc = 'PROCESSED'
      ) AS can_unprocess_calc,
      (
        (classified_rows.timesheet_id IS NOT NULL OR (classified_rows.contract_week_id IS NOT NULL AND classified_rows.route_family_calc = 'MANUAL_NON_QR'))
        AND classified_rows.locked_calc = FALSE
        AND classified_rows.route_family_calc <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence_calc,
      (
        classified_rows.locked_calc = FALSE
        AND classified_rows.authorised_calc = FALSE
        AND COALESCE(classified_rows.is_adjusted, FALSE) = FALSE
      ) AS can_add_additional_manual_calc,
      (
        classified_rows.locked_calc = TRUE
        OR classified_rows.authorised_calc = TRUE
        OR classified_rows.route_family_calc <> 'MANUAL_NON_QR'
      ) AS review_only_calc
    FROM classified_rows
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      (JSONB_BUILD_OBJECT(
        'row_key',
        decision_rows.row_key_calc,
        'stable_row_id',
        COALESCE(decision_rows.timesheet_id::text, decision_rows.contract_week_id::text),
        'row_type',
        CASE WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet' ELSE 'contract_week' END,
        'timesheet_id',
        decision_rows.timesheet_id,
        'current_timesheet_id',
        decision_rows.timesheet_id,
        'requested_timesheet_id',
        decision_rows.timesheet_id,
        'expected_timesheet_id',
        decision_rows.timesheet_id,
        'contract_week_id',
        decision_rows.contract_week_id,
        'contract_id',
        decision_rows.contract_id,
        'candidate_id',
        decision_rows.candidate_id,
        'candidate_name',
        decision_rows.candidate_name,
        'candidate_display_name',
        decision_rows.candidate_display_name,
        'candidate_first_name',
        NULL::text,
        'candidate_surname',
        NULL::text,
        'client_id',
        decision_rows.client_id,
        'client_name',
        decision_rows.client_name,
        'booking_id',
        decision_rows.booking_id,
        'external_ref',
        decision_rows.booking_id,
        'occupant_key_norm',
        decision_rows.occupant_key_norm,
        'hospital_norm',
        decision_rows.hospital_norm,
        'candidate_hint_text',
        COALESCE(decision_rows.candidate_hint_text, '{}'::jsonb),
        'week_ending_date',
        decision_rows.week_ending_date,
        'contract_week_ending_date',
        decision_rows.week_ending_date,
        'work_date',
        decision_rows.work_date,
        'date',
        COALESCE(decision_rows.work_date, decision_rows.week_ending_date),
        'shift_date',
        decision_rows.work_date,
        'period_type',
        decision_rows.period_type_calc,
        'sheet_scope',
        decision_rows.sheet_scope,
        'submission_mode',
        decision_rows.submission_mode,
        'submission_mode_snapshot',
        decision_rows.submission_mode_snapshot,
        'basis',
        decision_rows.basis,
        'route_type',
        decision_rows.route_type,
        'route_display',
        decision_rows.route_display,
        'route_family',
        decision_rows.route_family_calc,
        'route_subfamily',
        decision_rows.route_subfamily_calc,
        'underlying_channel_family',
        decision_rows.underlying_channel_family_calc,
        'summary_stage',
        decision_rows.summary_stage,
        'tools_stage',
        decision_rows.tools_stage,
        'processing_status',
        decision_rows.processing_status,
        'processing_status_display',
        decision_rows.processing_status_display
      )
      || JSONB_BUILD_OBJECT(
        'bulk_process_bucket',
        decision_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification',
        decision_rows.bulk_authorise_classification_calc,
        'bulk_authorise_section',
        CASE WHEN decision_rows.can_bulk_authorise_calc THEN 'processed_eligible' WHEN decision_rows.can_bulk_unauthorise_calc THEN 'authorised_eligible' ELSE NULL::text END,
        'authorised_at_utc',
        decision_rows.authorised_at_utc,
        'authorised_at_server',
        decision_rows.authorised_at_server,
        'processed_at_utc',
        decision_rows.processed_at_utc,
        'is_authorised',
        decision_rows.authorised_calc,
        'locked',
        decision_rows.locked_calc,
        'total_hours',
        decision_rows.total_hours,
        'total_pay_ex_vat',
        decision_rows.total_pay_ex_vat,
        'total_charge_ex_vat',
        decision_rows.total_charge_ex_vat,
        'margin_ex_vat',
        decision_rows.margin_ex_vat,
        'net_delta_ex_vat',
        decision_rows.net_delta_ex_vat,
        'paid_at_utc',
        decision_rows.paid_at_utc,
        'pay_icon_code',
        decision_rows.pay_icon_code,
        'pay_status_code',
        decision_rows.pay_status_code,
        'pay_paid_at_utc',
        decision_rows.pay_paid_at_utc,
        'ready_to_pay',
        FALSE,
        'pay_on_hold',
        FALSE,
        'invoice_is_paid',
        decision_rows.invoice_is_paid,
        'invoice_issue_stage',
        decision_rows.invoice_issue_stage,
        'invoice_segment_stage',
        decision_rows.invoice_segment_stage,
        'invoice_segments_total',
        COALESCE(decision_rows.invoice_segments_total, 0),
        'invoice_segments_locked',
        COALESCE(decision_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked',
        COALESCE(decision_rows.invoice_segments_unlocked, 0),
        'issue_codes',
        COALESCE(TO_JSONB(decision_rows.issue_codes), '[]'::jsonb),
        'validation_status',
        decision_rows.validation_status,
        'validation_summary',
        decision_rows.validation_summary,
        'validation_pre_validated',
        FALSE,
        'hr_validation_awaiting',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_required_for_invoice',
        decision_rows.hr_validation_awaiting_calc,
        'hr_validation_satisfied',
        NOT decision_rows.hr_validation_awaiting_calc,
        'hr_crosscheck_status',
        decision_rows.hr_crosscheck_status,
        'hr_crosscheck_issues',
        COALESCE(TO_JSONB(decision_rows.hr_crosscheck_issues), '[]'::jsonb),
        'qr_status',
        decision_rows.qr_status,
        'is_qr',
        decision_rows.is_qr,
        'qr_signed_at_utc',
        NULL::text,
        'can_allow_qr_again',
        FALSE,
        'can_allow_electronic_again',
        FALSE,
        'can_switch_to_manual',
        FALSE
      )
      || JSONB_BUILD_OBJECT(
        'can_revert_to_electronic',
        FALSE,
        'can_convert_qr_to_manual_only',
        FALSE,
        'qr_email_can_send_now',
        FALSE,
        'qr_email_recipient_available',
        FALSE,
        'is_adjusted',
        decision_rows.is_adjusted,
        'is_adjustment',
        decision_rows.is_adjusted,
        'needs_attention',
        decision_rows.needs_attention,
        'has_rate_issue',
        decision_rows.has_rate_issue,
        'has_pay_channel_issue',
        decision_rows.has_pay_channel_issue,
        'client_no_timesheet_required',
        decision_rows.client_no_timesheet_required,
        'client_autoprocess_hr',
        decision_rows.client_autoprocess_hr,
        'client_is_nhsp',
        decision_rows.client_is_nhsp,
        'has_deviation_marker',
        FALSE,
        'deviation_marker_reason',
        NULL::text,
        'nhsp_highlight_red',
        FALSE,
        'nhsp_highlight_reason',
        NULL::text,
        'nhsp_deviation_pct',
        NULL::numeric,
        'nhsp_is_ad_hoc',
        FALSE,
        'has_any_evidence',
        decision_rows.has_any_evidence,
        'attached_evidence_count',
        decision_rows.attached_evidence_count,
        'evidence_count',
        decision_rows.attached_evidence_count,
        'primary_artifact_id',
        NULL::text,
        'primary_artifact_kind',
        CASE WHEN decision_rows.primary_artifact_storage_key IS NOT NULL THEN 'TIMESHEET' ELSE NULL::text END,
        'primary_artifact_storage_key',
        decision_rows.primary_artifact_storage_key,
        'primary_artifact_display_name',
        decision_rows.primary_artifact_display_name,
        'primary_artifact_preview_mode',
        decision_rows.primary_artifact_preview_mode,
        'manual_pdf_r2_key',
        NULL::text,
        'uploaded_pdf_r2_key',
        NULL::text,
        'generated_pdf_at_utc',
        NULL::text,
        'manual_pdf_rotation_degrees',
        0,
        'queue_staged_count',
        0,
        'evidence_document_locked',
        decision_rows.locked_calc,
        'evidence_lock_reason',
        CASE WHEN decision_rows.locked_calc THEN 'locked_or_paid' ELSE NULL::text END,
        'has_timesheet',
        decision_rows.timesheet_id IS NOT NULL,
        'is_contract_week_only',
        decision_rows.timesheet_id IS NULL AND decision_rows.contract_week_id IS NOT NULL,
        'timesheet_version',
        NULL::integer,
        'updated_at',
        NULL::text,
        'is_current',
        TRUE,
        'was_stale',
        FALSE,
        'timesheet_type_sort_key',
        NULL::text
      )
      || JSONB_BUILD_OBJECT(
        'processed_by_user_id',
        NULL::text,
        'processed_by_display',
        NULL::text,
        'authorised_by_user_id',
        NULL::text,
        'authorised_by_display',
        NULL::text,
        'can_save',
        decision_rows.can_save_calc,
        'can_process',
        decision_rows.can_process_calc,
        'can_unprocess',
        decision_rows.can_unprocess_calc,
        'can_bulk_authorise',
        decision_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise',
        decision_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data',
        decision_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence',
        decision_rows.can_manage_evidence_calc,
        'can_add_additional_manual',
        decision_rows.can_add_additional_manual_calc,
        'review_only',
        decision_rows.review_only_calc,
        'row_signature',
        decision_rows.row_signature_calc,
        'evidence_badges',
        JSONB_BUILD_ARRAY(
          JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', COALESCE(decision_rows.has_any_evidence, FALSE), 'has_evidence', COALESCE(decision_rows.has_any_evidence, FALSE)),
          JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', FALSE, 'has_evidence', FALSE),
          JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', FALSE, 'has_evidence', FALSE)
        ),
        'artifact_hints',
        JSONB_BUILD_OBJECT(
          'has_any_evidence', decision_rows.has_any_evidence,
          'attached_evidence_count', decision_rows.attached_evidence_count,
          'primary_artifact_storage_key', decision_rows.primary_artifact_storage_key,
          'primary_artifact_display_name', decision_rows.primary_artifact_display_name,
          'primary_artifact_preview_mode', decision_rows.primary_artifact_preview_mode
        ),
        'action_flags',
        JSONB_BUILD_OBJECT(
          'can_save', decision_rows.can_save_calc,
          'can_process', decision_rows.can_process_calc,
          'can_unprocess', decision_rows.can_unprocess_calc,
          'can_bulk_authorise', decision_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', decision_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', decision_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', decision_rows.can_manage_evidence_calc,
          'can_add_additional_manual', decision_rows.can_add_additional_manual_calc,
          'review_only', decision_rows.review_only_calc
        ),
        'row_patch',
        JSONB_BUILD_OBJECT(),
        'cache_invalidation_hints',
        JSONB_BUILD_OBJECT(),
        'count_deltas',
        JSONB_BUILD_OBJECT(),
        'header_loaded',
        TRUE,
        'header_only',
        TRUE,
        'editor_loaded',
        FALSE,
        'evidence_loaded',
        FALSE,
        'compare_loaded',
        FALSE,
        'full_loaded',
        FALSE,
        'schedule_pending',
        TRUE,
        'schedule_authoritative',
        FALSE,
        'loaded_layers',
        JSONB_BUILD_ARRAY('dataset_row'),
        'dataset_source',
        'timesheet_summary_lightweight_rows_v1'
      )) AS row_json
    FROM decision_rows
  ),
  canonical_authorise_signature_rows AS MATERIALIZED (
    SELECT canonical_patch.row_json
    FROM public.bulk_timesheet_row_patch_v1(
      JSONB_BUILD_OBJECT(
        'dataset_mode', 'authorise',
        'projection', 'dataset_row',
        'row_keys', COALESCE((
          SELECT JSONB_AGG(payload_rows.row_json->>'row_key' ORDER BY payload_rows.row_json->>'row_key')
          FROM payload_rows
          WHERE NULLIF(BTRIM(COALESCE(payload_rows.row_json->>'row_key', '')), '') IS NOT NULL
        ), '[]'::jsonb)
      )
    ) AS canonical_patch(row_json)
  ),
  canonical_payload_rows AS MATERIALIZED (
    SELECT
      CASE
        WHEN NULLIF(BTRIM(COALESCE(canonical_authorise_signature_rows.row_json->>'row_signature', '')), '') IS NOT NULL THEN
          JSONB_SET(
            payload_rows.row_json,
            '{row_signature}',
            TO_JSONB(canonical_authorise_signature_rows.row_json->>'row_signature'),
            TRUE
          )
        ELSE payload_rows.row_json
      END AS row_json
    FROM payload_rows
    LEFT JOIN canonical_authorise_signature_rows
      ON canonical_authorise_signature_rows.row_json->>'row_key' = payload_rows.row_json->>'row_key'
  ),
  eligible_rows_before_classification AS MATERIALIZED (
    SELECT canonical_payload_rows.row_json
    FROM canonical_payload_rows
    WHERE NULLIF(BTRIM(COALESCE(canonical_payload_rows.row_json->>'timesheet_id', '')), '') IS NOT NULL
      AND UPPER(COALESCE(canonical_payload_rows.row_json->>'bulk_process_bucket', '')) <> 'UNPROCESSED'
      AND NULLIF(BTRIM(COALESCE(canonical_payload_rows.row_json->>'bulk_authorise_section', '')), '') IS NOT NULL
  ),
  classification_filtered_rows AS MATERIALIZED (
    SELECT eligible_rows_before_classification.row_json
    FROM eligible_rows_before_classification
    WHERE v_classification IS NULL
       OR UPPER(COALESCE(eligible_rows_before_classification.row_json->>'bulk_authorise_classification', '')) = v_classification
  ),
  visible_rows AS MATERIALIZED (
    SELECT classification_filtered_rows.row_json
    FROM classification_filtered_rows
    WHERE (
        UPPER(COALESCE(classification_filtered_rows.row_json->>'bulk_authorise_classification', '')) <> 'TIMESHEETS'
        OR (
          CASE
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'period_type', classification_filtered_rows.row_json->>'sheet_scope', '')) = 'DAILY' THEN v_show_daily
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'period_type', classification_filtered_rows.row_json->>'sheet_scope', '')) = 'WEEKLY' THEN v_show_weekly
            ELSE TRUE
          END
          AND CASE
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'MANUAL_NON_QR' THEN v_show_manual
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'QR' THEN v_show_qr
            WHEN UPPER(COALESCE(classification_filtered_rows.row_json->>'route_family', classification_filtered_rows.row_json->>'underlying_channel_family', '')) = 'ELECTRONIC' THEN v_show_electronic
            ELSE v_show_manual AND v_show_qr AND v_show_electronic
          END
          AND CASE
            WHEN COALESCE((classification_filtered_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE THEN v_validation_awaiting
            ELSE v_validation_already
          END
        )
      )
  ),
  paged_visible_rows AS MATERIALIZED (
    SELECT visible_rows.row_json
    FROM visible_rows
    ORDER BY visible_rows.row_json->>'week_ending_date', visible_rows.row_json->>'client_name', visible_rows.row_json->>'candidate_name', visible_rows.row_json->>'row_key'
    OFFSET v_offset
    LIMIT COALESCE(v_limit, 2147483647)
  ),
  visible_counts AS (
    SELECT
      COUNT(*)::integer AS total_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_section' = 'processed_eligible')::integer AS processed_eligible_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_section' = 'authorised_eligible')::integer AS authorised_eligible_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'MANUAL_NON_QR')::integer AS manual_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'QR')::integer AS qr_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND visible_rows.row_json->>'route_family' = 'ELECTRONIC')::integer AS electronic_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = FALSE)::integer AS already_validated_count,
      COUNT(*) FILTER (WHERE visible_rows.row_json->>'bulk_authorise_classification' = 'TIMESHEETS' AND COALESCE((visible_rows.row_json->>'hr_validation_awaiting')::boolean, FALSE) = TRUE)::integer AS awaiting_validation_count
    FROM visible_rows
  ),
  eligible_counts AS (
    SELECT
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'TIMESHEETS')::integer AS timesheets_count,
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'NHSP')::integer AS nhsp_count,
      COUNT(*) FILTER (WHERE eligible_rows_before_classification.row_json->>'bulk_authorise_classification' = 'HR')::integer AS hr_count
    FROM eligible_rows_before_classification
  ),
  rows_payload AS (
    SELECT COALESCE(
      JSONB_AGG(
        paged_visible_rows.row_json
        ORDER BY paged_visible_rows.row_json->>'week_ending_date', paged_visible_rows.row_json->>'client_name', paged_visible_rows.row_json->>'candidate_name', paged_visible_rows.row_json->>'row_key'
      ),
      '[]'::jsonb
    ) AS rows_json
    FROM paged_visible_rows
  )
  SELECT JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'classification', v_classification,
      'show_daily', v_show_daily,
      'show_weekly', v_show_weekly,
      'show_manual', v_show_manual,
      'show_qr', v_show_qr,
      'show_electronic', v_show_electronic,
      'validation_already', v_validation_already,
      'validation_awaiting', v_validation_awaiting,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', COALESCE(visible_counts.total_count, 0),
      'processed_eligible', COALESCE(visible_counts.processed_eligible_count, 0),
      'authorised_eligible', COALESCE(visible_counts.authorised_eligible_count, 0),
      'by_classification', JSONB_BUILD_OBJECT(
        'TIMESHEETS', COALESCE(eligible_counts.timesheets_count, 0),
        'NHSP', COALESCE(eligible_counts.nhsp_count, 0),
        'HR', COALESCE(eligible_counts.hr_count, 0)
      ),
      'timesheets_by_type', JSONB_BUILD_OBJECT(
        'manual', COALESCE(visible_counts.manual_count, 0),
        'qr', COALESCE(visible_counts.qr_count, 0),
        'electronic', COALESCE(visible_counts.electronic_count, 0)
      ),
      'validation', JSONB_BUILD_OBJECT(
        'already_validated', COALESCE(visible_counts.already_validated_count, 0),
        'awaiting_validation', COALESCE(visible_counts.awaiting_validation_count, 0),
        'scope', 'visible_rows_after_classification_and_toggle_filters'
      ),
      'scope', JSONB_BUILD_OBJECT(
        'total', 'visible_rows_after_classification_and_toggle_filters',
        'by_classification', 'eligible_rows_before_classification_filter',
        'timesheets_by_type', 'visible_rows_after_classification_and_toggle_filters',
        'validation', 'visible_rows_after_classification_and_toggle_filters'
      )
    ),
    'rows', COALESCE(rows_payload.rows_json, '[]'::jsonb)
  )
  INTO v_out
  FROM visible_counts
  CROSS JOIN eligible_counts
  CROSS JOIN rows_payload;

  RETURN COALESCE(v_out, JSONB_BUILD_OBJECT(
    'filters', JSONB_BUILD_OBJECT(
      'q', NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), ''),
      'candidate_id', NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), ''),
      'client_id', NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), ''),
      'classification', v_classification,
      'show_daily', v_show_daily,
      'show_weekly', v_show_weekly,
      'show_manual', v_show_manual,
      'show_qr', v_show_qr,
      'show_electronic', v_show_electronic,
      'validation_already', v_validation_already,
      'validation_awaiting', v_validation_awaiting,
      'date_from', NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), ''),
      'date_to', NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), ''),
      'week_ending_date', NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), ''),
      'limit', v_limit,
      'offset', v_offset,
      'dataset_source', 'timesheet_summary_lightweight_rows_v1'
    ),
    'counts', JSONB_BUILD_OBJECT(
      'total', 0,
      'processed_eligible', 0,
      'authorised_eligible', 0,
      'by_classification', JSONB_BUILD_OBJECT('TIMESHEETS', 0, 'NHSP', 0, 'HR', 0),
      'timesheets_by_type', JSONB_BUILD_OBJECT('manual', 0, 'qr', 0, 'electronic', 0),
      'validation', JSONB_BUILD_OBJECT('already_validated', 0, 'awaiting_validation', 0)
    ),
    'rows', '[]'::jsonb
  ));
END;
$function$;




CREATE OR REPLACE FUNCTION public.bulk_timesheet_row_patch_v1(
  p_filters jsonb DEFAULT '{}'::jsonb
)
RETURNS TABLE(row_json jsonb)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO public
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_source_filters jsonb := COALESCE(p_filters, '{}'::jsonb);

  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

  v_dataset_mode text := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'dataset_mode', v_filters->>'datasetMode', '')), ''));
  v_projection text := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'projection', v_filters->>'profile', 'status_patch')), ''));

  v_row_keys text[] := NULL;
  v_previous_row_key text := NULL;
  v_actor_user_id uuid := NULL;

  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_has_timesheet_filter boolean := FALSE;
  v_has_contract_week_filter boolean := FALSE;
  v_has_row_key_filter boolean := FALSE;

  v_changed_domains text[] := ARRAY[]::text[];
  v_status_only_hint boolean := FALSE;
  v_manual_changed_hint boolean := FALSE;
  v_evidence_changed_hint boolean := FALSE;
  v_storage_changed_hint boolean := FALSE;
  v_identity_changed_hint boolean := FALSE;
BEGIN
  IF v_dataset_mode NOT IN ('process', 'authorise') THEN
    v_dataset_mode := LOWER(NULLIF(BTRIM(COALESCE(v_filters->>'mode', '')), ''));
  END IF;

  IF v_dataset_mode NOT IN ('process', 'authorise') THEN
    v_dataset_mode := NULL;
  END IF;

  IF v_projection NOT IN ('status_patch', 'dataset_row', 'summary_row', 'active_row_header') THEN
    v_projection := 'status_patch';
  END IF;

  v_previous_row_key := NULLIF(BTRIM(COALESCE(v_filters->>'previous_row_key', v_filters->>'previousRowKey', '')), '');

  BEGIN
    IF NULLIF(BTRIM(COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId', '')), '') IS NOT NULL
       AND COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId') ~* v_uuid_re THEN
      v_actor_user_id := COALESCE(v_filters->>'actor_user_id', v_filters->>'actorUserId')::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_actor_user_id := NULL;
  END;

  v_has_row_key_filter :=
    (v_filters ? 'row_key')
    OR (v_filters ? 'rowKey')
    OR (v_filters ? 'row_keys')
    OR (v_filters ? 'rowKeys');

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value ORDER BY row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'rowKeys' AND jsonb_typeof(v_filters->'rowKeys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value ORDER BY row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'rowKeys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(COALESCE(v_filters->>'row_key', v_filters->>'rowKey')), '')];
  END IF;

  IF v_has_row_key_filter AND COALESCE(ARRAY_LENGTH(v_row_keys, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_row_keys, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('row_keys', to_jsonb(v_row_keys));
  END IF;

  v_has_timesheet_filter :=
    (v_filters ? 'timesheet_id')
    OR (v_filters ? 'timesheetId')
    OR (v_filters ? 'timesheet_ids')
    OR (v_filters ? 'timesheetIds');

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheetIds' AND jsonb_typeof(v_filters->'timesheetIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheetIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId', '')), '') IS NOT NULL
        AND COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[COALESCE(v_filters->>'timesheet_id', v_filters->>'timesheetId')::uuid];
  END IF;

  IF v_has_timesheet_filter AND COALESCE(ARRAY_LENGTH(v_timesheet_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_timesheet_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('timesheet_ids', to_jsonb(v_timesheet_ids));
  END IF;

  v_has_contract_week_filter :=
    (v_filters ? 'contract_week_id')
    OR (v_filters ? 'contractWeekId')
    OR (v_filters ? 'contract_week_ids')
    OR (v_filters ? 'contractWeekIds');

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contractWeekIds' AND jsonb_typeof(v_filters->'contractWeekIds') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value ORDER BY uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contractWeekIds') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId', '')), '') IS NOT NULL
        AND COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[COALESCE(v_filters->>'contract_week_id', v_filters->>'contractWeekId')::uuid];
  END IF;

  IF v_has_contract_week_filter AND COALESCE(ARRAY_LENGTH(v_contract_week_ids, 1), 0) = 0 THEN
    RETURN;
  END IF;

  IF COALESCE(ARRAY_LENGTH(v_contract_week_ids, 1), 0) > 0 THEN
    v_source_filters :=
      v_source_filters
      || jsonb_build_object('contract_week_ids', to_jsonb(v_contract_week_ids));
  END IF;

  IF v_filters ? 'changed_domains' AND jsonb_typeof(v_filters->'changed_domains') = 'array' THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(input_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM jsonb_array_elements_text(v_filters->'changed_domains') AS input_values(value)
    WHERE NULLIF(BTRIM(input_values.value), '') IS NOT NULL;
  ELSIF v_filters ? 'changedDomains' AND jsonb_typeof(v_filters->'changedDomains') = 'array' THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(input_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM jsonb_array_elements_text(v_filters->'changedDomains') AS input_values(value)
    WHERE NULLIF(BTRIM(input_values.value), '') IS NOT NULL;
  ELSIF NULLIF(BTRIM(COALESCE(v_filters->>'changed_domains', v_filters->>'changedDomains', '')), '') IS NOT NULL THEN
    SELECT COALESCE(ARRAY_AGG(DISTINCT LOWER(NULLIF(BTRIM(split_values.value), ''))), ARRAY[]::text[])
      INTO v_changed_domains
    FROM unnest(regexp_split_to_array(COALESCE(v_filters->>'changed_domains', v_filters->>'changedDomains'), '\s*,\s*')) AS split_values(value)
    WHERE NULLIF(BTRIM(split_values.value), '') IS NOT NULL;
  END IF;

  v_identity_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['identity','adoption','row_identity','contract_week_to_timesheet','contract-week-to-timesheet','created_timesheet']::text[], FALSE);

  v_evidence_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['evidence','attached_evidence','queue_evidence','document','documents']::text[], FALSE);

  v_storage_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['storage','storage_key','storage-key','preview','artifact','primary_artifact']::text[], FALSE);

  v_manual_changed_hint :=
    COALESCE(v_changed_domains && ARRAY['manual','schedule','editor','save','process','unprocess','financials','tsfin','additional_units','expenses']::text[], FALSE);

  -- Process-mode mutation callers may request a lightweight status_patch without
  -- explicit changed_domains. Treat those as manual/editor-affecting so the
  -- frontend fetches one fresh active_row_visible context instead of preserving
  -- stale schedule/expense/editor state as if this were a pure status patch.
  IF NOT v_manual_changed_hint
     AND COALESCE(ARRAY_LENGTH(v_changed_domains, 1), 0) = 0
     AND v_dataset_mode = 'process'
     AND v_projection = 'status_patch' THEN
    v_manual_changed_hint := TRUE;
  END IF;

  v_status_only_hint :=
    (
      COALESCE(v_changed_domains && ARRAY['status','authorise','authorize','unauthorise','unauthorize','processing_status','authorisation','authorization']::text[], FALSE)
      OR (
        COALESCE(ARRAY_LENGTH(v_changed_domains, 1), 0) = 0
        AND v_dataset_mode = 'authorise'
        AND v_projection = 'status_patch'
      )
    )
    AND NOT v_identity_changed_hint
    AND NOT v_evidence_changed_hint
    AND NOT v_storage_changed_hint
    AND NOT v_manual_changed_hint;

  RETURN QUERY
  WITH source_rows AS MATERIALIZED (
    SELECT
      source_row.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_source_filters) AS source_row
  ),
  enriched_rows AS MATERIALIZED (
    SELECT
      source_rows.*,
      timesheet_row.version AS timesheet_version,
      timesheet_row.updated_at AS timesheet_updated_at,
      timesheet_row.is_current AS timesheet_is_current,
      timesheet_row.manual_pdf_r2_key,
      timesheet_row.qr_r2_key,
      timesheet_row.generated_pdf_at_utc,
      timesheet_row.manual_pdf_rotation_degrees,
      timesheet_row.qr_token AS timesheet_qr_token,
      timesheet_row.qr_generated_at AS timesheet_qr_generated_at,
      timesheet_row.qr_scanned_at AS timesheet_qr_scanned_at,
      contract_week_row.updated_at AS contract_week_updated_at,
      contract_week_row.uploaded_pdf_r2_key,
      contract_week_row.planned_schedule_json AS contract_week_planned_schedule_json,
      contract_week_row.totals_json AS contract_week_totals_json,
      contract_week_row.is_adjustment AS contract_week_is_adjustment,
      contract_week_row.additional_seq AS contract_week_additional_seq,
      timesheet_row.actual_schedule_json AS timesheet_actual_schedule_json,
      timesheet_row.is_adjustment AS timesheet_is_adjustment,
      timesheet_row.parent_timesheet_id AS timesheet_parent_timesheet_id,
      timesheet_row.correction_id AS timesheet_correction_id,
      timesheet_row.correction_kind AS timesheet_correction_kind,
      timesheet_row.adjustment_origin AS timesheet_adjustment_origin,
      financial_row.updated_at AS tsfin_updated_at,
      financial_row.actual_schedule_json AS tsfin_actual_schedule_json,
      financial_row.invoice_breakdown_json AS tsfin_invoice_breakdown_json,
      financial_row.total_hours AS tsfin_total_hours,
      financial_row.expenses_pay_ex_vat AS tsfin_expenses_pay_ex_vat,
      financial_row.expenses_charge_ex_vat AS tsfin_expenses_charge_ex_vat,
      financial_row.mileage_units AS tsfin_mileage_units,
      financial_row.mileage_pay_ex_vat AS tsfin_mileage_pay_ex_vat,
      financial_row.mileage_charge_ex_vat AS tsfin_mileage_charge_ex_vat,
      financial_row.travel_pay_ex_vat AS tsfin_travel_pay_ex_vat,
      financial_row.travel_charge_ex_vat AS tsfin_travel_charge_ex_vat,
      financial_row.accommodation_pay_ex_vat AS tsfin_accommodation_pay_ex_vat,
      financial_row.accommodation_charge_ex_vat AS tsfin_accommodation_charge_ex_vat,
      financial_row.other_pay_ex_vat AS tsfin_other_pay_ex_vat,
      financial_row.other_charge_ex_vat AS tsfin_other_charge_ex_vat,
      financial_row.authorised_at_utc AS tsfin_authorised_at_utc,
      financial_row.processed_at_utc AS tsfin_processed_at_utc,
      COALESCE(evidence_summary.attached_evidence_count, 0)::integer AS evidence_attached_count,
      evidence_summary.primary_storage_key AS evidence_primary_storage_key,
      evidence_summary.primary_display_name AS evidence_primary_display_name,
      CASE
        WHEN evidence_summary.evidence_updated_at IS NOT NULL
         AND staged_evidence_summary.staged_evidence_updated_at IS NOT NULL
          THEN GREATEST(evidence_summary.evidence_updated_at, staged_evidence_summary.staged_evidence_updated_at)
        ELSE COALESCE(evidence_summary.evidence_updated_at, staged_evidence_summary.staged_evidence_updated_at)
      END AS evidence_updated_at,
      COALESCE(evidence_summary.has_attached_timesheet_evidence, FALSE) AS evidence_has_attached_timesheet,
      COALESCE(evidence_summary.has_attached_mileage_evidence, FALSE) AS evidence_has_attached_mileage,
      COALESCE(evidence_summary.has_attached_travel_evidence, FALSE) AS evidence_has_attached_travel,
      COALESCE(evidence_summary.has_attached_accommodation_evidence, FALSE) AS evidence_has_attached_accommodation,
      COALESCE(evidence_summary.has_attached_other_evidence, FALSE) AS evidence_has_attached_other,
      COALESCE(staged_evidence_summary.staged_evidence_count, 0)::integer AS evidence_staged_count,
      staged_evidence_summary.primary_staged_storage_key AS evidence_primary_staged_storage_key,
      staged_evidence_summary.primary_staged_display_name AS evidence_primary_staged_display_name,
      staged_evidence_summary.primary_staged_kind AS evidence_primary_staged_kind,
      COALESCE(staged_evidence_summary.has_staged_timesheet_evidence, FALSE) AS evidence_has_staged_timesheet,
      COALESCE(staged_evidence_summary.has_staged_mileage_evidence, FALSE) AS evidence_has_staged_mileage,
      COALESCE(staged_evidence_summary.has_staged_travel_evidence, FALSE) AS evidence_has_staged_travel,
      COALESCE(staged_evidence_summary.has_staged_accommodation_evidence, FALSE) AS evidence_has_staged_accommodation,
      COALESCE(staged_evidence_summary.has_staged_other_evidence, FALSE) AS evidence_has_staged_other,
      UPPER(COALESCE(source_rows.route_type, '')) AS route_type_upper,
      UPPER(COALESCE(source_rows.submission_mode::text, '')) AS submission_mode_upper,
      CASE
        WHEN UPPER(COALESCE(source_rows.sheet_scope::text, '')) IN ('DAILY', 'WEEKLY') THEN UPPER(COALESCE(source_rows.sheet_scope::text, ''))
        WHEN UPPER(COALESCE(source_rows.route_type, '')) LIKE 'DAILY\_%' ESCAPE '\' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type,
      (
        COALESCE(source_rows.is_qr, FALSE)
        OR source_rows.qr_status IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(timesheet_row.qr_token, '')), '') IS NOT NULL
        OR timesheet_row.qr_generated_at IS NOT NULL
      ) AS is_qr_route,
      (
        UPPER(COALESCE(source_rows.submission_mode::text, contract_week_row.submission_mode_snapshot::text, '')) = 'MANUAL'
        AND (
          COALESCE(timesheet_row.is_adjustment, FALSE) = TRUE
          OR COALESCE(contract_week_row.is_adjustment, FALSE) = TRUE
          OR COALESCE(contract_week_row.additional_seq, source_rows.additional_seq, 0) > 0
          OR COALESCE(source_rows.is_adjustment, FALSE) = TRUE
          OR timesheet_row.parent_timesheet_id IS NOT NULL
        )
        AND NOT (
          UPPER(COALESCE(timesheet_row.adjustment_origin, '')) IN ('IMPORT_CORRECTION', 'IMPORT_CANCELLATION')
          OR NULLIF(BTRIM(COALESCE(timesheet_row.correction_kind, '')), '') IS NOT NULL
          OR timesheet_row.correction_id IS NOT NULL
        )
      ) AS is_manual_additional_adjustment_calc
    FROM source_rows
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = source_rows.timesheet_id
     AND timesheet_row.is_current = TRUE
    LEFT JOIN public.contract_weeks AS contract_week_row
      ON contract_week_row.id = source_rows.contract_week_id
    LEFT JOIN public.timesheets_financials AS financial_row
      ON financial_row.timesheet_id = source_rows.timesheet_id
     AND financial_row.is_current = TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(timesheet_evidence_row.id)::integer AS attached_evidence_count,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET'), FALSE) AS has_attached_timesheet_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'MILEAGE'), FALSE) AS has_attached_mileage_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TRAVEL'), FALSE) AS has_attached_travel_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'ACCOMMODATION'), FALSE) AS has_attached_accommodation_evidence,
        COALESCE(BOOL_OR(UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'OTHER'), FALSE) AS has_attached_other_evidence,
        (ARRAY_AGG(
          timesheet_evidence_row.storage_key
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_storage_key,
        (ARRAY_AGG(
          COALESCE(NULLIF(timesheet_evidence_row.display_name, ''), timesheet_evidence_row.kind, 'Evidence')
          ORDER BY
            (UPPER(COALESCE(timesheet_evidence_row.kind, '')) = 'TIMESHEET') DESC,
            timesheet_evidence_row.created_at DESC,
            timesheet_evidence_row.id DESC
        ))[1] AS primary_display_name,
        MAX(timesheet_evidence_row.created_at) AS evidence_updated_at
      FROM public.timesheet_evidence AS timesheet_evidence_row
      WHERE timesheet_evidence_row.timesheet_id = source_rows.timesheet_id
    ) AS evidence_summary ON TRUE
    LEFT JOIN LATERAL (
      SELECT
        COUNT(staged_queue_row.id)::integer AS staged_evidence_count,
        MAX(staged_queue_row.uploaded_at_utc) AS staged_evidence_updated_at,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'TIMESHEET'), FALSE) AS has_staged_timesheet_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'MILEAGE'), FALSE) AS has_staged_mileage_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'TRAVEL'), FALSE) AS has_staged_travel_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'ACCOMMODATION'), FALSE) AS has_staged_accommodation_evidence,
        COALESCE(BOOL_OR(staged_queue_row.staged_kind_upper = 'OTHER'), FALSE) AS has_staged_other_evidence,
        (ARRAY_AGG(
          staged_queue_row.r2_key
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_storage_key,
        (ARRAY_AGG(
          staged_queue_row.display_name
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_display_name,
        (ARRAY_AGG(
          staged_queue_row.staged_kind_upper
          ORDER BY
            (staged_queue_row.staged_kind_upper = 'TIMESHEET') DESC,
            staged_queue_row.uploaded_at_utc DESC NULLS LAST,
            staged_queue_row.id DESC
        ) FILTER (
          WHERE NULLIF(BTRIM(COALESCE(staged_queue_row.r2_key, '')), '') IS NOT NULL
            AND NULLIF(BTRIM(COALESCE(staged_queue_row.display_name, '')), '') IS NOT NULL
        ))[1] AS primary_staged_kind
      FROM (
        SELECT
          manual_queue_row.id,
          manual_queue_row.r2_key,
          manual_queue_row.uploaded_at_utc,
          COALESCE(
            NULLIF(BTRIM(COALESCE(manual_queue_row.original_filename, '')), ''),
            NULLIF(BTRIM(COALESCE(manual_queue_row.meta_json->>'display_name', manual_queue_row.meta_json->>'displayName', manual_queue_row.meta_json->>'filename', manual_queue_row.meta_json->>'original_filename', manual_queue_row.meta_json->>'originalFilename', '')), '')
          ) AS display_name,
          CASE
            WHEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(
              manual_queue_row.meta_json->>'staged_kind',
              manual_queue_row.meta_json->>'stagedKind',
              manual_queue_row.meta_json->>'evidence_kind',
              manual_queue_row.meta_json->>'evidenceKind',
              manual_queue_row.meta_json->>'kind',
              manual_queue_row.meta_json->>'type',
              manual_queue_row.meta_json->>'evidence_type',
              manual_queue_row.meta_json->>'evidenceType',
              ''
            )), ''), 'TIMESHEET')) IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER')
              THEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(
                manual_queue_row.meta_json->>'staged_kind',
                manual_queue_row.meta_json->>'stagedKind',
                manual_queue_row.meta_json->>'evidence_kind',
                manual_queue_row.meta_json->>'evidenceKind',
                manual_queue_row.meta_json->>'kind',
                manual_queue_row.meta_json->>'type',
                manual_queue_row.meta_json->>'evidence_type',
                manual_queue_row.meta_json->>'evidenceType',
                ''
              )), ''), 'TIMESHEET'))
            ELSE 'OTHER'
          END AS staged_kind_upper
        FROM public.manual_timesheet_queue AS manual_queue_row
        WHERE source_rows.timesheet_id IS NULL
          AND source_rows.contract_week_id IS NOT NULL
          AND UPPER(COALESCE(manual_queue_row.status, '')) = 'STAGED'
          AND NULLIF(BTRIM(COALESCE(manual_queue_row.meta_json->>'contract_week_id', '')), '') = source_rows.contract_week_id::text
      ) AS staged_queue_row
    ) AS staged_evidence_summary ON TRUE
  ),
  classified_rows AS MATERIALIZED (
    SELECT
      enriched_rows.*,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
            AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE
          )
        ) THEN 'IMPORT_AUTHORITATIVE'
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family_calc,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
        ) THEN 'NHSP'
        WHEN enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
         AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily_calc,
      CASE
        WHEN COALESCE(enriched_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'MANUAL_NON_QR'
        WHEN (
          enriched_rows.route_type_upper = 'WEEKLY_NHSP'
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT'
            AND NOT (COALESCE(enriched_rows.is_adjusted, FALSE) AND enriched_rows.submission_mode_upper = 'MANUAL')
          )
          OR (
            enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
            AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) = TRUE
          )
        ) THEN NULL::text
        WHEN enriched_rows.is_qr_route THEN 'QR'
        WHEN enriched_rows.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family_calc,
      (
        enriched_rows.route_type_upper = 'WEEKLY_HEALTHROSTER'
        AND COALESCE(enriched_rows.client_no_timesheet_required, FALSE) <> TRUE
      ) AS compare_block_required_calc,
      (
        enriched_rows.timesheet_id IS NULL
        AND enriched_rows.contract_week_id IS NOT NULL
        AND (
          UPPER(COALESCE(enriched_rows.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.contract_week_status::text, '')) IN ('OPEN', 'PLANNED')
        )
      ) AS is_planned_week_unprocessed_calc,
      (
        enriched_rows.timesheet_id IS NOT NULL
        AND (
          UPPER(COALESCE(enriched_rows.processing_status::text, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(enriched_rows.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.tools_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(enriched_rows.processing_status_display, '')) = 'UNPROCESSED'
        )
      ) AS is_real_row_unprocessed_calc,
      (
        COALESCE(enriched_rows.locked_by_invoice_id, NULL) IS NOT NULL
        OR COALESCE(enriched_rows.paid_at_utc, NULL) IS NOT NULL
        OR COALESCE(enriched_rows.invoice_segments_locked, 0) > 0
        OR COALESCE(enriched_rows.invoice_is_paid, FALSE) = TRUE
      ) AS locked_calc,
      (
        enriched_rows.authorised_at_server IS NOT NULL
        OR enriched_rows.tsfin_authorised_at_utc IS NOT NULL
      ) AS is_authorised_calc,
      (
        UPPER(COALESCE(enriched_rows.processing_status::text, '')) = 'PENDING_AUTH'
        OR (
          COALESCE(enriched_rows.client_requires_hr, FALSE) = TRUE
          AND COALESCE(enriched_rows.client_autoprocess_hr, FALSE) = FALSE
          AND UPPER(COALESCE(enriched_rows.processing_status::text, '')) = 'READY_FOR_HR'
        )
      ) AS requires_authorisation_calc,
      (
        UPPER(COALESCE(enriched_rows.qr_status::text, '')) = 'PENDING'
        AND (
          NULLIF(BTRIM(COALESCE(enriched_rows.timesheet_qr_token, '')), '') IS NOT NULL
          OR enriched_rows.timesheet_qr_generated_at IS NOT NULL
        )
        AND enriched_rows.timesheet_qr_scanned_at IS NULL
      ) AS qr_pending_awaiting_signature_calc,
      (
        UPPER(COALESCE(enriched_rows.qr_status::text, '')) = 'USED'
        AND enriched_rows.timesheet_qr_scanned_at IS NOT NULL
      ) AS qr_signed_returned_calc,
      COALESCE(
        enriched_rows.evidence_primary_storage_key,
        enriched_rows.evidence_primary_staged_storage_key,
        NULLIF(enriched_rows.manual_pdf_r2_key, ''),
        NULLIF(enriched_rows.qr_r2_key, ''),
        NULLIF(enriched_rows.uploaded_pdf_r2_key, '')
      ) AS primary_artifact_storage_key_calc,
      COALESCE(
        enriched_rows.evidence_primary_display_name,
        enriched_rows.evidence_primary_staged_display_name,
        CASE WHEN NULLIF(enriched_rows.manual_pdf_r2_key, '') IS NOT NULL THEN 'Manual timesheet PDF' END,
        CASE WHEN NULLIF(enriched_rows.qr_r2_key, '') IS NOT NULL THEN 'QR timesheet' END,
        CASE WHEN NULLIF(enriched_rows.uploaded_pdf_r2_key, '') IS NOT NULL THEN 'Uploaded weekly PDF' END
      ) AS primary_artifact_display_name_calc
    FROM enriched_rows
  ),
  decision_rows AS MATERIALIZED (
    SELECT
      classified_rows.*,
      CASE
        WHEN classified_rows.route_family_calc = 'IMPORT_AUTHORITATIVE'
         AND classified_rows.route_subfamily_calc = 'HEALTHROSTER_NO_TIMESHEET' THEN 'HR'
        WHEN classified_rows.route_family_calc = 'IMPORT_AUTHORITATIVE' THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification_calc,
      (classified_rows.is_planned_week_unprocessed_calc OR classified_rows.is_real_row_unprocessed_calc) AS is_unprocessed_calc,
      CASE
        WHEN (classified_rows.is_planned_week_unprocessed_calc OR classified_rows.is_real_row_unprocessed_calc) THEN 'UNPROCESSED'
        ELSE 'PROCESSED'
      END AS bulk_process_bucket_calc,
      (
        COALESCE(classified_rows.client_hr_validation_required, FALSE) = TRUE
        AND UPPER(COALESCE(classified_rows.validation_status::text, '')) NOT IN ('VALIDATION_OK', 'OVERRIDDEN')
      ) AS hr_validation_awaiting_calc,
      (
        classified_rows.qr_pending_awaiting_signature_calc = TRUE
        OR UPPER(COALESCE(classified_rows.processing_status::text, '')) = 'AWAITING_MANUAL_SIGNATURE'
        OR 'Awaiting signed QR timesheet' = ANY(COALESCE(classified_rows.issue_codes, ARRAY[]::text[]))
      ) AS qr_unsigned_blocked_calc,
      CASE
        WHEN classified_rows.primary_artifact_storage_key_calc IS NOT NULL THEN 'document'
        ELSE NULL::text
      END AS primary_artifact_preview_mode_calc
    FROM classified_rows
  ),
  final_rows AS MATERIALIZED (
    SELECT
      decision_rows.*,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL THEN 'timesheet:' || decision_rows.timesheet_id::text
        WHEN decision_rows.contract_week_id IS NOT NULL THEN 'contract_week:' || decision_rows.contract_week_id::text
        ELSE ''::text
      END AS row_key_calc,
      CASE
        WHEN decision_rows.timesheet_id IS NOT NULL THEN decision_rows.timesheet_id::text
        WHEN decision_rows.contract_week_id IS NOT NULL THEN decision_rows.contract_week_id::text
        ELSE NULL::text
      END AS stable_row_id_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.requires_authorisation_calc = TRUE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.qr_unsigned_blocked_calc = FALSE
        AND (decision_rows.route_family_calc <> 'QR' OR decision_rows.qr_signed_returned_calc = TRUE)
      ) AS can_bulk_authorise_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = TRUE
        AND (decision_rows.route_family_calc <> 'QR' OR decision_rows.qr_signed_returned_calc = TRUE)
      ) AS can_bulk_unauthorise_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_save_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data_calc,
      (
        decision_rows.timesheet_id IS NOT NULL
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = FALSE
      ) AS can_unprocess_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR decision_rows.contract_week_id IS NOT NULL)
        AND decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND decision_rows.route_family_calc = 'MANUAL_NON_QR'
        AND decision_rows.is_unprocessed_calc = TRUE
      ) AS can_process_calc,
      (
        (decision_rows.timesheet_id IS NOT NULL OR (decision_rows.contract_week_id IS NOT NULL AND decision_rows.route_family_calc = 'MANUAL_NON_QR'))
        AND (decision_rows.locked_by_invoice_id IS NOT NULL OR COALESCE(decision_rows.invoice_segments_locked, 0) > 0) = FALSE
        AND decision_rows.route_family_calc <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence_calc,
      (
        decision_rows.locked_calc = TRUE
        OR decision_rows.is_authorised_calc = TRUE
        OR decision_rows.route_family_calc <> 'MANUAL_NON_QR'
      ) AS review_only_calc,
      (
        decision_rows.locked_calc = FALSE
        AND decision_rows.is_authorised_calc = FALSE
        AND COALESCE(decision_rows.is_adjusted, FALSE) = FALSE
        AND (
          (decision_rows.period_type = 'WEEKLY' AND decision_rows.contract_week_id IS NOT NULL)
          OR (decision_rows.period_type = 'DAILY' AND decision_rows.timesheet_id IS NOT NULL)
        )
        AND (
          decision_rows.route_family_calc = 'IMPORT_AUTHORITATIVE'
          OR (decision_rows.route_family_calc = 'MANUAL_NON_QR' AND decision_rows.submission_mode_upper = 'MANUAL' AND decision_rows.is_qr_route = FALSE)
        )
      ) AS can_add_additional_manual_calc
    FROM decision_rows
  ),
  signed_rows AS MATERIALIZED (
    SELECT
      final_rows.*,
      md5(concat_ws('|',
        COALESCE(final_rows.timesheet_id::text, ''),
        COALESCE(final_rows.contract_week_id::text, ''),
        COALESCE(final_rows.timesheet_version::text, final_rows.timesheet_version::text, ''),
        COALESCE(final_rows.timesheet_updated_at::text, ''),
        COALESCE(final_rows.contract_week_updated_at::text, ''),
        COALESCE(final_rows.tsfin_updated_at::text, ''),
        COALESCE(final_rows.evidence_updated_at::text, ''),
        COALESCE(final_rows.evidence_staged_count::text, ''),
        COALESCE(final_rows.evidence_has_staged_timesheet::text, ''),
        COALESCE(final_rows.evidence_has_staged_mileage::text, ''),
        COALESCE(final_rows.evidence_has_staged_travel::text, ''),
        COALESCE(final_rows.evidence_has_staged_accommodation::text, ''),
        COALESCE(final_rows.evidence_has_staged_other::text, ''),
        COALESCE(final_rows.processing_status::text, ''),
        COALESCE(final_rows.summary_stage, ''),
        COALESCE(final_rows.tools_stage, ''),
        COALESCE(final_rows.authorised_at_server::text, ''),
        COALESCE(final_rows.tsfin_authorised_at_utc::text, ''),
        COALESCE(final_rows.is_authorised_calc::text, ''),
        COALESCE(final_rows.locked_calc::text, ''),
        COALESCE(final_rows.route_family_calc, ''),
        COALESCE(final_rows.route_subfamily_calc, ''),
        COALESCE(final_rows.bulk_process_bucket_calc, ''),
        COALESCE(final_rows.bulk_authorise_classification_calc, ''),
        COALESCE(final_rows.primary_artifact_storage_key_calc, ''),
        COALESCE(final_rows.total_hours::text, ''),
        COALESCE(final_rows.total_pay_ex_vat::text, ''),
        COALESCE(final_rows.total_charge_ex_vat::text, ''),
        COALESCE(final_rows.margin_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_expenses_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_expenses_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_mileage_units::text, ''),
        COALESCE(final_rows.tsfin_mileage_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_mileage_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_travel_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_travel_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_accommodation_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_accommodation_charge_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_other_pay_ex_vat::text, ''),
        COALESCE(final_rows.tsfin_other_charge_ex_vat::text, ''),
        COALESCE(final_rows.issue_codes::text, '')
      )) AS row_signature_calc
    FROM final_rows
  ),
  payload_rows AS MATERIALIZED (
    SELECT
      signed_rows.*,
      CASE
        WHEN signed_rows.can_bulk_authorise_calc THEN 'processed_eligible'
        WHEN signed_rows.can_bulk_unauthorise_calc THEN 'authorised_eligible'
        ELSE NULL::text
      END AS bulk_authorise_section_calc,
      (
        v_identity_changed_hint
        OR (
          v_previous_row_key IS NOT NULL
          AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
        )
      ) AS identity_changed_calc,
      (
        v_status_only_hint
        AND NOT (
          v_identity_changed_hint
          OR (
            v_previous_row_key IS NOT NULL
            AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
          )
        )
      ) AS status_only_calc,
      jsonb_build_object(
        'status_only', (
          v_status_only_hint
          AND NOT (
            v_identity_changed_hint
            OR (
              v_previous_row_key IS NOT NULL
              AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
            )
          )
        ),
        'manual_changed', v_manual_changed_hint,
        'evidence_changed', v_evidence_changed_hint,
        'storage_changed', v_storage_changed_hint,
        'identity_changed', (
          v_identity_changed_hint
          OR (
            v_previous_row_key IS NOT NULL
            AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
          )
        ),
        'invalidate_context', false,
        'invalidate_row_context', false,
        'invalidate_preview', CASE WHEN v_storage_changed_hint OR v_evidence_changed_hint THEN true ELSE false END,
        'invalidate_evidence', CASE WHEN v_evidence_changed_hint OR v_storage_changed_hint THEN true ELSE false END,
        'invalidate_editor_context', CASE WHEN v_manual_changed_hint THEN true ELSE false END,
        'row_keys', CASE
          WHEN v_previous_row_key IS NOT NULL
           AND v_previous_row_key IS DISTINCT FROM signed_rows.row_key_calc
            THEN jsonb_build_array(v_previous_row_key, signed_rows.row_key_calc)
          ELSE jsonb_build_array(signed_rows.row_key_calc)
        END,
        'timesheet_ids', CASE WHEN signed_rows.timesheet_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.timesheet_id) END,
        'contract_week_ids', CASE WHEN signed_rows.contract_week_id IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.contract_week_id) END,
        'storage_keys', CASE WHEN signed_rows.primary_artifact_storage_key_calc IS NULL THEN '[]'::jsonb ELSE jsonb_build_array(signed_rows.primary_artifact_storage_key_calc) END,
        'row_signature', signed_rows.row_signature_calc,
        'datasets', CASE
          WHEN v_dataset_mode = 'authorise' THEN jsonb_build_array('bulk_authorise')
          WHEN v_dataset_mode = 'process' THEN jsonb_build_array('bulk_process')
          ELSE jsonb_build_array('bulk_process', 'bulk_authorise')
        END
      ) AS cache_hints_json,
      jsonb_build_object(
        'processed_eligible', 0,
        'authorised_eligible', 0,
        'unprocessed', 0,
        'processed', 0,
        'total', 0
      ) AS count_deltas_json,
      (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) AS keep_additional_manual_adjustment_schedule_empty_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN '[]'::jsonb
        ELSE COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json, '[]'::jsonb)
      END AS actual_schedule_json_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN '[]'::jsonb
        ELSE COALESCE(signed_rows.contract_week_planned_schedule_json, '[]'::jsonb)
      END AS planned_schedule_json_calc,
      CASE
        WHEN (
        COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
        AND COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(signed_rows.timesheet_actual_schedule_json, signed_rows.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          signed_rows.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(signed_rows.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(signed_rows.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(
            CASE
              WHEN signed_rows.tsfin_invoice_breakdown_json IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'array' THEN signed_rows.tsfin_invoice_breakdown_json
              WHEN jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json) = 'object'
               AND jsonb_typeof(signed_rows.tsfin_invoice_breakdown_json->'segments') = 'array' THEN signed_rows.tsfin_invoice_breakdown_json->'segments'
              ELSE '[]'::jsonb
            END
          ) AS keep_empty_segment(segment_json)
        )
      ) THEN 0::numeric
        ELSE COALESCE(signed_rows.total_hours, signed_rows.tsfin_total_hours, 0::numeric)
      END AS total_hours_calc,
      CASE
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
         AND (
           UPPER(COALESCE(signed_rows.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT', 'NHSP')
           OR UPPER(COALESCE(signed_rows.basis::text, '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
           OR COALESCE(signed_rows.client_is_nhsp, FALSE) = TRUE
         ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE
         AND (
           UPPER(COALESCE(signed_rows.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER', 'HEALTHROSTER_DAILY')
           OR UPPER(COALESCE(signed_rows.basis::text, '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
           OR COALESCE(signed_rows.client_autoprocess_hr, FALSE) = TRUE
         ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
        WHEN signed_rows.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
         AND COALESCE(signed_rows.is_manual_additional_adjustment_calc, FALSE) = TRUE THEN 'WEEKLY_MANUAL_ADJUSTMENT'
        ELSE signed_rows.route_type
      END AS effective_route_type_calc,
      (
        COALESCE(signed_rows.timesheet_is_adjustment, FALSE) = TRUE
        OR COALESCE(signed_rows.contract_week_is_adjustment, FALSE) = TRUE
        OR COALESCE(signed_rows.contract_week_additional_seq, signed_rows.additional_seq, 0) > 0
        OR COALESCE(signed_rows.is_adjustment, FALSE) = TRUE
        OR signed_rows.timesheet_parent_timesheet_id IS NOT NULL
        OR signed_rows.timesheet_correction_id IS NOT NULL
        OR signed_rows.timesheet_correction_kind IS NOT NULL
      ) AS effective_is_adjustment_calc
    FROM signed_rows
  )
  SELECT
    (
      jsonb_build_object(
        'id', COALESCE(payload_rows.timesheet_id::text, payload_rows.contract_week_id::text),
        'row_key', payload_rows.row_key_calc,
        'previous_row_key', v_previous_row_key,
        'new_row_key', payload_rows.row_key_calc,
        'stable_row_id', payload_rows.stable_row_id_calc,
        'timesheet_id', payload_rows.timesheet_id,
        'current_timesheet_id', payload_rows.timesheet_id,
        'requested_timesheet_id', payload_rows.timesheet_id,
        'expected_timesheet_id', payload_rows.timesheet_id,
        'contract_week_id', payload_rows.contract_week_id,
        'contract_id', COALESCE((SELECT contract_lookup.contract_id FROM public.timesheets AS contract_lookup WHERE contract_lookup.timesheet_id = payload_rows.timesheet_id AND contract_lookup.is_current = TRUE LIMIT 1), (SELECT contract_week_lookup.contract_id FROM public.contract_weeks AS contract_week_lookup WHERE contract_week_lookup.id = payload_rows.contract_week_id LIMIT 1)),
        'row_signature', payload_rows.row_signature_calc,
        'backend_row_signature', payload_rows.row_signature_calc,
        'render_signature', payload_rows.row_signature_calc,
        'previous_row_signature', NULL::text,
        'timesheet_version', payload_rows.timesheet_version,
        'current_version', payload_rows.timesheet_version,
        'updated_at', COALESCE(payload_rows.timesheet_updated_at, payload_rows.contract_week_updated_at, payload_rows.tsfin_updated_at),
        'is_current', COALESCE(payload_rows.timesheet_is_current, TRUE),
        'was_stale', FALSE,
        'actor_user_id', v_actor_user_id,
        'projection', v_projection,
        'dataset_mode', v_dataset_mode
      )
      || jsonb_build_object(
        'candidate_id', payload_rows.candidate_id,
        'candidate_name', payload_rows.candidate_name,
        'candidate_display_name', payload_rows.candidate_name,
        'client_id', payload_rows.client_id,
        'client_name', payload_rows.client_name,
        'client_display_name', payload_rows.client_name,
        'booking_id', payload_rows.booking_id,
        'booking_ref', payload_rows.booking_id,
        'occupant_key_norm', payload_rows.occupant_key_norm,
        'hospital_name', payload_rows.hospital_norm,
        'hospital_norm', payload_rows.hospital_norm,
        'week_ending_date', COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date),
        'work_date', CASE WHEN payload_rows.period_type = 'DAILY' THEN payload_rows.week_ending_date ELSE NULL::date END,
        'period_type', payload_rows.period_type,
        'sheet_scope', payload_rows.sheet_scope,
        'timesheet_scope', payload_rows.sheet_scope,
        'submission_mode', payload_rows.submission_mode,
        'submission_mode_snapshot', payload_rows.submission_mode,
        'basis', payload_rows.basis,
        'route_type', payload_rows.effective_route_type_calc,
        'route_display', CASE
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('NHSP', 'WEEKLY_NHSP') THEN 'NHSP'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'NHSP Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('HEALTHROSTER', 'WEEKLY_HEALTHROSTER') THEN 'HealthRoster'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'HealthRoster Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'QR' THEN 'QR'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
          WHEN COALESCE(payload_rows.effective_route_type_calc, '') <> '' THEN initcap(replace(payload_rows.effective_route_type_calc, '_', ' '))
          ELSE 'Manual'
        END,
        'route_family', payload_rows.route_family_calc,
        'route_subfamily', payload_rows.route_subfamily_calc,
        'underlying_channel_family', payload_rows.underlying_channel_family_calc,
        'is_import_authoritative', payload_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
        'compare_block_required', payload_rows.compare_block_required_calc,
        'is_adjustment', payload_rows.effective_is_adjustment_calc,
        'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
        'actual_schedule_json', payload_rows.actual_schedule_json_calc,
        'planned_schedule_json', payload_rows.planned_schedule_json_calc,
        'contract_week_totals_json', COALESCE(payload_rows.contract_week_totals_json, '{}'::jsonb),
        'suppress_standard_schedule_fallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        'keep_additional_manual_adjustment_schedule_empty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        '__suppressStandardScheduleFallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
        '__keepAdditionalManualAdjustmentScheduleEmpty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc
      )
      || jsonb_build_object(
        'processing_status', payload_rows.processing_status::text,
        'processing_status_display', payload_rows.processing_status_display,
        'summary_stage', CASE WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED' ELSE payload_rows.summary_stage END,
        'tools_stage', CASE WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED' ELSE payload_rows.tools_stage END,
        'bulk_process_bucket', payload_rows.bulk_process_bucket_calc,
        'bulk_authorise_classification', payload_rows.bulk_authorise_classification_calc,
        'bulk_authorise_section', payload_rows.bulk_authorise_section_calc,
        'is_authorised', payload_rows.is_authorised_calc,
        'authorised_at_utc', COALESCE(payload_rows.tsfin_authorised_at_utc, payload_rows.authorised_at_server),
        'authorised_at_server', payload_rows.authorised_at_server,
        'processed_at_utc', COALESCE(payload_rows.tsfin_processed_at_utc, NULL::timestamp with time zone),
        'requires_authorisation', payload_rows.requires_authorisation_calc,
        'locked', payload_rows.locked_calc,
        'locked_by_invoice_id', payload_rows.locked_by_invoice_id,
        'paid_at_utc', payload_rows.paid_at_utc,
        'review_only', payload_rows.review_only_calc,
        'can_save', payload_rows.can_save_calc,
        'can_process', payload_rows.can_process_calc,
        'can_unprocess', payload_rows.can_unprocess_calc,
        'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
        'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
        'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
        'can_manage_evidence', payload_rows.can_manage_evidence_calc,
        'can_add_additional_manual', payload_rows.can_add_additional_manual_calc
      )
      || jsonb_build_object(
        'total_hours', COALESCE(payload_rows.total_hours_calc, 0::numeric),
        'total_pay_ex_vat', COALESCE(payload_rows.total_pay_ex_vat, 0::numeric),
        'total_charge_ex_vat', COALESCE(payload_rows.total_charge_ex_vat, 0::numeric),
        'margin_ex_vat', COALESCE(payload_rows.margin_ex_vat, 0::numeric),
        'expenses_pay_ex_vat', payload_rows.tsfin_expenses_pay_ex_vat,
        'expenses_charge_ex_vat', payload_rows.tsfin_expenses_charge_ex_vat,
        'mileage_units', payload_rows.tsfin_mileage_units,
        'mileage_pay_ex_vat', payload_rows.tsfin_mileage_pay_ex_vat,
        'mileage_charge_ex_vat', payload_rows.tsfin_mileage_charge_ex_vat,
        'travel_pay_ex_vat', payload_rows.tsfin_travel_pay_ex_vat,
        'travel_charge_ex_vat', payload_rows.tsfin_travel_charge_ex_vat,
        'accommodation_pay_ex_vat', payload_rows.tsfin_accommodation_pay_ex_vat,
        'accommodation_charge_ex_vat', payload_rows.tsfin_accommodation_charge_ex_vat,
        'other_pay_ex_vat', payload_rows.tsfin_other_pay_ex_vat,
        'other_charge_ex_vat', payload_rows.tsfin_other_charge_ex_vat,
        'net_delta_ex_vat', COALESCE(payload_rows.net_delta_ex_vat, COALESCE(payload_rows.total_charge_ex_vat, 0::numeric) - COALESCE(payload_rows.total_pay_ex_vat, 0::numeric)),
        'invoice_is_paid', COALESCE(payload_rows.invoice_is_paid, FALSE),
        'invoice_segments_total', COALESCE(payload_rows.invoice_segments_total, 0),
        'invoice_segments_locked', COALESCE(payload_rows.invoice_segments_locked, 0),
        'invoice_segments_unlocked', COALESCE(payload_rows.invoice_segments_unlocked, 0),
        'invoice_segment_stage', payload_rows.invoice_segment_stage,
        'pay_icon_code', payload_rows.pay_icon_code,
        'pay_status_code', payload_rows.pay_status_code,
        'pay_paid_at_utc', payload_rows.pay_paid_at_utc,
        'issue_codes', COALESCE(to_jsonb(payload_rows.issue_codes), '[]'::jsonb),
        'validation_status', payload_rows.validation_status::text,
        'hr_crosscheck_status', payload_rows.hr_crosscheck_status,
        'hr_crosscheck_issues', COALESCE(to_jsonb(payload_rows.hr_crosscheck_issues), '[]'::jsonb),
        'hr_validation_awaiting', payload_rows.hr_validation_awaiting_calc,
        'qr_unsigned_blocked', payload_rows.qr_unsigned_blocked_calc,
        'qr_signed_returned', payload_rows.qr_signed_returned_calc
      )
      || jsonb_build_object(
        'is_qr', payload_rows.is_qr_route,
        'qr_status', payload_rows.qr_status::text,
        'qr_generated_at', payload_rows.timesheet_qr_generated_at,
        'qr_scanned_at', payload_rows.timesheet_qr_scanned_at,
        'is_adjusted', COALESCE(payload_rows.is_adjusted, FALSE),
        'needs_attention', COALESCE(payload_rows.needs_attention, FALSE),
        'has_rate_issue', COALESCE(payload_rows.has_rate_issue, FALSE),
        'has_pay_channel_issue', COALESCE(payload_rows.has_pay_channel_issue, FALSE),
        'client_requires_hr', COALESCE(payload_rows.client_requires_hr, FALSE),
        'client_no_timesheet_required', COALESCE(payload_rows.client_no_timesheet_required, FALSE),
        'client_autoprocess_hr', COALESCE(payload_rows.client_autoprocess_hr, FALSE),
        'client_is_nhsp', COALESCE(payload_rows.client_is_nhsp, FALSE),
        'has_any_evidence', (
          COALESCE(payload_rows.evidence_attached_count, 0) > 0
          OR COALESCE(payload_rows.evidence_staged_count, 0) > 0
          OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL
        ),
        'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
        'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0),
        'evidence_count', COALESCE(payload_rows.evidence_attached_count, 0) + COALESCE(payload_rows.evidence_staged_count, 0),
        'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
        'primary_artifact_display_name', payload_rows.primary_artifact_display_name_calc,
        'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
        'evidence_badges', jsonb_build_array(
          jsonb_build_object('kind', 'TIMESHEET', 'present', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
          jsonb_build_object('kind', 'MILEAGE', 'present', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE))),
          jsonb_build_object('kind', 'TRAVEL', 'present', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE))),
          jsonb_build_object('kind', 'ACCOMMODATION', 'present', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE))),
          jsonb_build_object('kind', 'OTHER', 'present', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)))
        )
      )
      || jsonb_build_object(
        'count_deltas', payload_rows.count_deltas_json,
        'cache_invalidation_hints', payload_rows.cache_hints_json,
        'action_flags', jsonb_build_object(
          'can_save', payload_rows.can_save_calc,
          'can_process', payload_rows.can_process_calc,
          'can_unprocess', payload_rows.can_unprocess_calc,
          'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
          'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
          'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
          'can_manage_evidence', payload_rows.can_manage_evidence_calc,
          'can_add_additional_manual', payload_rows.can_add_additional_manual_calc,
          'review_only', payload_rows.review_only_calc,
          'is_adjustment', payload_rows.effective_is_adjustment_calc,
          'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
          'supportsUnprocessedExpenseDraft', (
            payload_rows.is_manual_additional_adjustment_calc = TRUE
            AND payload_rows.contract_week_id IS NOT NULL
            AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
            AND payload_rows.locked_calc = FALSE
            AND payload_rows.is_authorised_calc = FALSE
            AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE)
          ),
          'supports_unprocessed_expense_draft', (
            payload_rows.is_manual_additional_adjustment_calc = TRUE
            AND payload_rows.contract_week_id IS NOT NULL
            AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
            AND payload_rows.locked_calc = FALSE
            AND payload_rows.is_authorised_calc = FALSE
            AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE)
          ),
          'expense_storage_target', CASE
            WHEN payload_rows.is_manual_additional_adjustment_calc = TRUE
             AND payload_rows.contract_week_id IS NOT NULL
             AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
             AND payload_rows.locked_calc = FALSE
             AND payload_rows.is_authorised_calc = FALSE
             AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE) THEN 'CONTRACT_WEEK_DRAFT'
            WHEN payload_rows.timesheet_id IS NOT NULL AND payload_rows.route_family_calc = 'MANUAL_NON_QR' THEN 'TSFIN'
            ELSE NULL::text
          END,
          'expense_evidence_storage_target', CASE
            WHEN payload_rows.is_manual_additional_adjustment_calc = TRUE
             AND payload_rows.contract_week_id IS NOT NULL
             AND payload_rows.route_family_calc = 'MANUAL_NON_QR'
             AND payload_rows.locked_calc = FALSE
             AND payload_rows.is_authorised_calc = FALSE
             AND (payload_rows.timesheet_id IS NULL OR payload_rows.is_unprocessed_calc = TRUE) THEN 'CONTRACT_WEEK_STAGED_EVIDENCE'
            WHEN payload_rows.timesheet_id IS NOT NULL AND payload_rows.route_family_calc = 'MANUAL_NON_QR' THEN 'TIMESHEET_EVIDENCE'
            ELSE NULL::text
          END
        ),
        'artifact_hints', jsonb_build_object(
          'route_family', payload_rows.route_family_calc,
          'route_subfamily', payload_rows.route_subfamily_calc,
          'underlying_channel_family', payload_rows.underlying_channel_family_calc,
          'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
          'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
          'has_any_evidence', (COALESCE(payload_rows.evidence_attached_count, 0) > 0 OR COALESCE(payload_rows.evidence_staged_count, 0) > 0 OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL),
          'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
          'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0)
        ),
        'row_patch', (
          jsonb_build_object(
            'previous_row_key', v_previous_row_key,
            'row_key', payload_rows.row_key_calc,
            'new_row_key', payload_rows.row_key_calc,
            'stable_row_id', payload_rows.stable_row_id_calc,
            'timesheet_id', payload_rows.timesheet_id,
            'current_timesheet_id', payload_rows.timesheet_id,
            'requested_timesheet_id', payload_rows.timesheet_id,
            'expected_timesheet_id', payload_rows.timesheet_id,
            'contract_week_id', payload_rows.contract_week_id,
            'row_signature', payload_rows.row_signature_calc,
            'backend_row_signature', payload_rows.row_signature_calc,
            'render_signature', payload_rows.row_signature_calc,
            'previous_row_signature', NULL::text,
            'timesheet_version', payload_rows.timesheet_version,
            'current_version', payload_rows.timesheet_version,
            'updated_at', COALESCE(payload_rows.timesheet_updated_at, payload_rows.contract_week_updated_at, payload_rows.tsfin_updated_at),
            'is_current', COALESCE(payload_rows.timesheet_is_current, TRUE),
            'was_stale', FALSE,
            'candidate_id', payload_rows.candidate_id,
            'candidate_name', payload_rows.candidate_name,
            'candidate_display_name', payload_rows.candidate_name,
            'client_id', payload_rows.client_id,
            'client_name', payload_rows.client_name,
            'client_display_name', payload_rows.client_name,
            'booking_id', payload_rows.booking_id,
            'booking_ref', payload_rows.booking_id,
            'occupant_key_norm', payload_rows.occupant_key_norm,
            'hospital_name', payload_rows.hospital_norm,
            'hospital_norm', payload_rows.hospital_norm,
            'week_ending_date', COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date),
            'work_date', CASE WHEN payload_rows.period_type = 'DAILY' THEN payload_rows.week_ending_date ELSE NULL::date END,
            'period_type', payload_rows.period_type,
            'sheet_scope', payload_rows.sheet_scope,
            'timesheet_scope', payload_rows.sheet_scope,
            'submission_mode', payload_rows.submission_mode,
            'submission_mode_snapshot', payload_rows.submission_mode,
            'basis', payload_rows.basis,
            'route_type', payload_rows.effective_route_type_calc,
            'route_display', CASE
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('NHSP', 'WEEKLY_NHSP') THEN 'NHSP'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'NHSP Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) IN ('HEALTHROSTER', 'WEEKLY_HEALTHROSTER') THEN 'HealthRoster'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'HealthRoster Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'HEALTHROSTER_DAILY' THEN 'HealthRoster Daily'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'QR' THEN 'QR'
          WHEN UPPER(COALESCE(payload_rows.effective_route_type_calc, '')) = 'NO_TIMESHEET_REQUIRED' THEN 'No timesheet required'
          WHEN COALESCE(payload_rows.effective_route_type_calc, '') <> '' THEN initcap(replace(payload_rows.effective_route_type_calc, '_', ' '))
          ELSE 'Manual'
        END,
            'route_family', payload_rows.route_family_calc,
            'route_subfamily', payload_rows.route_subfamily_calc
          )
          || jsonb_build_object(
            'underlying_channel_family', payload_rows.underlying_channel_family_calc,
            'is_import_authoritative', payload_rows.route_family_calc = 'IMPORT_AUTHORITATIVE',
            'compare_block_required', payload_rows.compare_block_required_calc,
            'is_adjustment', payload_rows.effective_is_adjustment_calc,
            'additional_seq', COALESCE(payload_rows.contract_week_additional_seq, payload_rows.additional_seq, 0),
            'actual_schedule_json', payload_rows.actual_schedule_json_calc,
            'planned_schedule_json', payload_rows.planned_schedule_json_calc,
            'contract_week_totals_json', COALESCE(payload_rows.contract_week_totals_json, '{}'::jsonb),
            'suppress_standard_schedule_fallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            'keep_additional_manual_adjustment_schedule_empty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            '__suppressStandardScheduleFallback', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            '__keepAdditionalManualAdjustmentScheduleEmpty', payload_rows.keep_additional_manual_adjustment_schedule_empty_calc,
            'processing_status', payload_rows.processing_status::text,
            'processing_status_display', payload_rows.processing_status_display,
            'summary_stage', CASE WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED' ELSE payload_rows.summary_stage END,
            'tools_stage', CASE WHEN payload_rows.is_unprocessed_calc THEN 'UNPROCESSED' ELSE payload_rows.tools_stage END,
            'bulk_process_bucket', payload_rows.bulk_process_bucket_calc,
            'previous_bulk_process_bucket', NULL::text,
            'bulk_authorise_classification', payload_rows.bulk_authorise_classification_calc,
            'bulk_authorise_section', payload_rows.bulk_authorise_section_calc,
            'previous_bulk_authorise_section', NULL::text,
            'is_authorised', payload_rows.is_authorised_calc,
            'authorised_at_utc', COALESCE(payload_rows.tsfin_authorised_at_utc, payload_rows.authorised_at_server),
            'authorised_at_server', payload_rows.authorised_at_server,
            'processed_at_utc', COALESCE(payload_rows.tsfin_processed_at_utc, NULL::timestamp with time zone),
            'requires_authorisation', payload_rows.requires_authorisation_calc,
            'locked', payload_rows.locked_calc,
            'locked_by_invoice_id', payload_rows.locked_by_invoice_id,
            'paid_at_utc', payload_rows.paid_at_utc,
            'review_only', payload_rows.review_only_calc,
            'can_save', payload_rows.can_save_calc,
            'can_process', payload_rows.can_process_calc,
            'can_unprocess', payload_rows.can_unprocess_calc,
            'can_bulk_authorise', payload_rows.can_bulk_authorise_calc,
            'can_bulk_unauthorise', payload_rows.can_bulk_unauthorise_calc,
            'can_edit_timesheet_data', payload_rows.can_edit_timesheet_data_calc,
            'can_manage_evidence', payload_rows.can_manage_evidence_calc,
            'can_add_additional_manual', payload_rows.can_add_additional_manual_calc
          )
          || jsonb_build_object(
            'total_hours', COALESCE(payload_rows.total_hours_calc, 0::numeric),
            'total_pay_ex_vat', COALESCE(payload_rows.total_pay_ex_vat, 0::numeric),
            'total_charge_ex_vat', COALESCE(payload_rows.total_charge_ex_vat, 0::numeric),
            'margin_ex_vat', COALESCE(payload_rows.margin_ex_vat, 0::numeric),
            'net_delta_ex_vat', COALESCE(payload_rows.net_delta_ex_vat, COALESCE(payload_rows.total_charge_ex_vat, 0::numeric) - COALESCE(payload_rows.total_pay_ex_vat, 0::numeric)),
            'invoice_is_paid', COALESCE(payload_rows.invoice_is_paid, FALSE),
            'invoice_segments_total', COALESCE(payload_rows.invoice_segments_total, 0),
            'invoice_segments_locked', COALESCE(payload_rows.invoice_segments_locked, 0),
            'invoice_segments_unlocked', COALESCE(payload_rows.invoice_segments_unlocked, 0),
            'invoice_segment_stage', payload_rows.invoice_segment_stage,
            'pay_icon_code', payload_rows.pay_icon_code,
            'pay_status_code', payload_rows.pay_status_code,
            'pay_paid_at_utc', payload_rows.pay_paid_at_utc,
            'issue_codes', COALESCE(to_jsonb(payload_rows.issue_codes), '[]'::jsonb),
            'validation_status', payload_rows.validation_status::text,
            'hr_crosscheck_status', payload_rows.hr_crosscheck_status,
            'hr_crosscheck_issues', COALESCE(to_jsonb(payload_rows.hr_crosscheck_issues), '[]'::jsonb),
            'hr_validation_awaiting', payload_rows.hr_validation_awaiting_calc,
            'qr_unsigned_blocked', payload_rows.qr_unsigned_blocked_calc,
            'qr_signed_returned', payload_rows.qr_signed_returned_calc,
            'is_qr', payload_rows.is_qr_route,
            'qr_status', payload_rows.qr_status::text,
            'qr_generated_at', payload_rows.timesheet_qr_generated_at,
            'qr_scanned_at', payload_rows.timesheet_qr_scanned_at,
            'is_adjusted', COALESCE(payload_rows.is_adjusted, FALSE),
            'needs_attention', COALESCE(payload_rows.needs_attention, FALSE),
            'has_rate_issue', COALESCE(payload_rows.has_rate_issue, FALSE),
            'has_pay_channel_issue', COALESCE(payload_rows.has_pay_channel_issue, FALSE)
          )
          || jsonb_build_object(
            'client_requires_hr', COALESCE(payload_rows.client_requires_hr, FALSE),
            'client_no_timesheet_required', COALESCE(payload_rows.client_no_timesheet_required, FALSE),
            'client_autoprocess_hr', COALESCE(payload_rows.client_autoprocess_hr, FALSE),
            'client_is_nhsp', COALESCE(payload_rows.client_is_nhsp, FALSE),
            'has_any_evidence', (
              COALESCE(payload_rows.evidence_attached_count, 0) > 0
              OR COALESCE(payload_rows.evidence_staged_count, 0) > 0
              OR payload_rows.primary_artifact_storage_key_calc IS NOT NULL
            ),
            'attached_evidence_count', COALESCE(payload_rows.evidence_attached_count, 0),
            'queue_staged_count', COALESCE(payload_rows.evidence_staged_count, 0),
            'evidence_count', COALESCE(payload_rows.evidence_attached_count, 0) + COALESCE(payload_rows.evidence_staged_count, 0),
            'primary_artifact_storage_key', payload_rows.primary_artifact_storage_key_calc,
            'previous_primary_artifact_storage_key', NULL::text,
            'primary_artifact_display_name', payload_rows.primary_artifact_display_name_calc,
            'primary_artifact_preview_mode', payload_rows.primary_artifact_preview_mode_calc,
            'evidence_badges', jsonb_build_array(
              jsonb_build_object('kind', 'TIMESHEET', 'present', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_timesheet, FALSE) OR COALESCE(payload_rows.evidence_has_staged_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(payload_rows.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.qr_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(payload_rows.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
              jsonb_build_object('kind', 'MILEAGE', 'present', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_mileage, FALSE) OR COALESCE(payload_rows.evidence_has_staged_mileage, FALSE))),
              jsonb_build_object('kind', 'TRAVEL', 'present', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_travel, FALSE) OR COALESCE(payload_rows.evidence_has_staged_travel, FALSE))),
              jsonb_build_object('kind', 'ACCOMMODATION', 'present', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_accommodation, FALSE) OR COALESCE(payload_rows.evidence_has_staged_accommodation, FALSE))),
              jsonb_build_object('kind', 'OTHER', 'present', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)), 'has_evidence', (COALESCE(payload_rows.evidence_has_attached_other, FALSE) OR COALESCE(payload_rows.evidence_has_staged_other, FALSE)))
            ),
            'count_deltas', payload_rows.count_deltas_json,
            'cache_invalidation_hints', payload_rows.cache_hints_json
          )
        )
      )
    ) AS row_json
  FROM payload_rows
  ORDER BY
    COALESCE(payload_rows.contract_week_ending_date, payload_rows.week_ending_date) ASC NULLS LAST,
    payload_rows.client_name ASC NULLS LAST,
    payload_rows.candidate_name ASC NULLS LAST,
    payload_rows.row_key_calc ASC;
END;
$function$;



REVOKE ALL ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) TO service_role;





CREATE OR REPLACE FUNCTION public.bulk_timesheet_row_decision_v1(p_filters jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(row_json jsonb)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_filters jsonb := COALESCE(p_filters, '{}'::jsonb);
  v_q text := NULL;
  v_candidate_id_text text := NULL;
  v_client_id_text text := NULL;
  v_q_like text := NULL;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_timesheet_ids uuid[] := NULL;
  v_contract_week_ids uuid[] := NULL;
  v_row_keys text[] := NULL;
  v_dataset_mode text := NULL;
  v_period_filter text := NULL;
  v_date_from_text text := NULL;
  v_date_to_text text := NULL;
  v_week_ending_text text := NULL;
  v_date_from date := NULL;
  v_date_to date := NULL;
  v_week_ending_date date := NULL;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';
BEGIN
  v_q := NULLIF(BTRIM(COALESCE(v_filters->>'q', v_filters->>'candidate_text', v_filters->>'candidateText', v_filters->>'name', '')), '');
  IF v_q IS NOT NULL THEN
    v_q_like := '%' || REPLACE(REPLACE(REPLACE(v_q, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%';
  END IF;

  v_candidate_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'candidate_id', v_filters->>'candidateId', '')), '');
  BEGIN
    IF v_candidate_id_text IS NOT NULL THEN
      v_candidate_id := v_candidate_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_candidate_id := NULL;
  END;

  v_client_id_text := NULLIF(BTRIM(COALESCE(v_filters->>'client_id', v_filters->>'clientId', '')), '');
  BEGIN
    IF v_client_id_text IS NOT NULL THEN
      v_client_id := v_client_id_text::uuid;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_client_id := NULL;
  END;

  IF v_filters ? 'timesheet_ids' AND jsonb_typeof(v_filters->'timesheet_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_timesheet_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'timesheet_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'timesheet_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'timesheet_id', '')), '') IS NOT NULL AND (v_filters->>'timesheet_id') ~* v_uuid_re THEN
    v_timesheet_ids := ARRAY[(v_filters->>'timesheet_id')::uuid];
  END IF;

  IF v_filters ? 'contract_week_ids' AND jsonb_typeof(v_filters->'contract_week_ids') = 'array' THEN
    SELECT ARRAY_AGG(uuid_values.uuid_value)
      INTO v_contract_week_ids
    FROM (
      SELECT DISTINCT input_values.value::uuid AS uuid_value
      FROM jsonb_array_elements_text(v_filters->'contract_week_ids') AS input_values(value)
      WHERE input_values.value ~* v_uuid_re
    ) AS uuid_values;
  ELSIF v_filters ? 'contract_week_id' AND NULLIF(BTRIM(COALESCE(v_filters->>'contract_week_id', '')), '') IS NOT NULL AND (v_filters->>'contract_week_id') ~* v_uuid_re THEN
    v_contract_week_ids := ARRAY[(v_filters->>'contract_week_id')::uuid];
  END IF;

  IF v_filters ? 'row_keys' AND jsonb_typeof(v_filters->'row_keys') = 'array' THEN
    SELECT ARRAY_AGG(row_key_values.row_key_value)
      INTO v_row_keys
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(input_values.value), '') AS row_key_value
      FROM jsonb_array_elements_text(v_filters->'row_keys') AS input_values(value)
    ) AS row_key_values
    WHERE row_key_values.row_key_value IS NOT NULL;
  ELSIF v_filters ? 'row_key' AND NULLIF(BTRIM(COALESCE(v_filters->>'row_key', '')), '') IS NOT NULL THEN
    v_row_keys := ARRAY[NULLIF(BTRIM(v_filters->>'row_key'), '')];
  END IF;

  v_dataset_mode := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'dataset_mode', v_filters->>'datasetMode', '')), ''));
  IF v_dataset_mode NOT IN ('PROCESS', 'AUTHORISE', 'ROW_CONTEXT') THEN
    v_dataset_mode := NULL;
  END IF;

  v_period_filter := UPPER(NULLIF(BTRIM(COALESCE(v_filters->>'period_type', v_filters->>'periodType', v_filters->>'sheet_scope', v_filters->>'sheetScope', '')), ''));
  IF v_period_filter NOT IN ('DAILY', 'WEEKLY') THEN
    v_period_filter := NULL;
  END IF;

  v_date_from_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_from', v_filters->>'dateFrom', v_filters->>'from_date', v_filters->>'fromDate', '')), '');
  v_date_to_text := NULLIF(BTRIM(COALESCE(v_filters->>'date_to', v_filters->>'dateTo', v_filters->>'to_date', v_filters->>'toDate', '')), '');
  v_week_ending_text := NULLIF(BTRIM(COALESCE(v_filters->>'week_ending_date', v_filters->>'weekEndingDate', v_filters->>'week_ending', v_filters->>'weekEnding', '')), '');

  BEGIN
    IF v_date_from_text IS NOT NULL THEN
      v_date_from := v_date_from_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_from := NULL;
  END;

  BEGIN
    IF v_date_to_text IS NOT NULL THEN
      v_date_to := v_date_to_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_date_to := NULL;
  END;

  BEGIN
    IF v_week_ending_text IS NOT NULL THEN
      v_week_ending_date := v_week_ending_text::date;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    v_week_ending_date := NULL;
  END;

  RETURN QUERY
  WITH base_scope AS (
    SELECT vb0.*
    FROM public.bulk_timesheet_workbench_row_source_v1(v_filters) AS vb0
  ),
  base_ids AS (
    SELECT DISTINCT
      base_scope.timesheet_id,
      base_scope.contract_week_id
    FROM base_scope
  ),
  tf_ranked AS (
    SELECT
      tf0.id,
      tf0.timesheet_id,
      tf0.timesheet_version,
      tf0.basis,
      tf0.is_current,
      tf0.is_stale,
      tf0.stale_reason,
      tf0.worked_start_iso,
      tf0.worked_end_iso,
      tf0.break_start_iso,
      tf0.break_end_iso,
      tf0.break_minutes,
      tf0.candidate_id,
      tf0.client_id,
      tf0.role,
      tf0.band,
      tf0.pay_method,
      tf0.policy_snapshot_json,
      tf0.processing_status,
      tf0.total_hours,
      tf0.total_pay_ex_vat,
      tf0.total_charge_ex_vat,
      tf0.margin_ex_vat,
      tf0.computed_at_utc,
      tf0.locked_by_invoice_id,
      tf0.paid_at_utc,
      tf0.pay_on_hold,
      tf0.pay_on_hold_reason,
      tf0.processed_by_user_id,
      tf0.processed_at_utc,
      tf0.authorised_by_user_id,
      tf0.authorised_at_utc,
      tf0.invoice_breakdown_json,
      tf0.external_source_rows_json,
      tf0.has_rate_issue,
      tf0.has_pay_channel_issue,
      tf0.hr_crosscheck_status,
      tf0.hr_crosscheck_issues,
      tf0.expenses_pay_ex_vat,
      tf0.expenses_charge_ex_vat,
      tf0.expenses_description,
      tf0.expenses_evidence_r2_key,
      tf0.mileage_units,
      tf0.mileage_pay_rate,
      tf0.mileage_charge_rate,
      tf0.mileage_pay_ex_vat,
      tf0.mileage_charge_ex_vat,
      tf0.mileage_evidence_r2_key,
      tf0.travel_pay_ex_vat,
      tf0.travel_charge_ex_vat,
      tf0.accommodation_pay_ex_vat,
      tf0.accommodation_charge_ex_vat,
      tf0.other_pay_ex_vat,
      tf0.other_charge_ex_vat,
      tf0.actual_schedule_json,
      tf0.additional_units_json,
      tf0.updated_at,
      tf0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY tf0.timesheet_id
        ORDER BY tf0.created_at DESC NULLS LAST, tf0.updated_at DESC NULLS LAST, tf0.id DESC
      ) AS rn
    FROM public.timesheets_financials AS tf0
    WHERE tf0.is_current = TRUE
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_tf
        WHERE bs_tf.timesheet_id = tf0.timesheet_id
      )
  ),
  tf_latest AS (
    SELECT
      tf1.id,
      tf1.timesheet_id,
      tf1.timesheet_version,
      tf1.basis,
      tf1.is_stale,
      tf1.stale_reason,
      tf1.worked_start_iso,
      tf1.worked_end_iso,
      tf1.break_start_iso,
      tf1.break_end_iso,
      tf1.break_minutes,
      tf1.candidate_id,
      tf1.client_id,
      tf1.role,
      tf1.band,
      tf1.pay_method,
      tf1.policy_snapshot_json,
      tf1.processing_status,
      tf1.total_hours,
      tf1.total_pay_ex_vat,
      tf1.total_charge_ex_vat,
      tf1.margin_ex_vat,
      tf1.computed_at_utc,
      tf1.locked_by_invoice_id,
      tf1.paid_at_utc,
      tf1.pay_on_hold,
      tf1.pay_on_hold_reason,
      tf1.processed_by_user_id,
      tf1.processed_at_utc,
      tf1.authorised_by_user_id,
      tf1.authorised_at_utc,
      tf1.invoice_breakdown_json,
      tf1.external_source_rows_json,
      tf1.has_rate_issue,
      tf1.has_pay_channel_issue,
      tf1.hr_crosscheck_status,
      tf1.hr_crosscheck_issues,
      tf1.expenses_pay_ex_vat,
      tf1.expenses_charge_ex_vat,
      tf1.expenses_description,
      tf1.expenses_evidence_r2_key,
      tf1.mileage_units,
      tf1.mileage_pay_rate,
      tf1.mileage_charge_rate,
      tf1.mileage_pay_ex_vat,
      tf1.mileage_charge_ex_vat,
      tf1.mileage_evidence_r2_key,
      tf1.travel_pay_ex_vat,
      tf1.travel_charge_ex_vat,
      tf1.accommodation_pay_ex_vat,
      tf1.accommodation_charge_ex_vat,
      tf1.other_pay_ex_vat,
      tf1.other_charge_ex_vat,
      tf1.actual_schedule_json,
      tf1.additional_units_json,
      tf1.updated_at,
      tf1.created_at
    FROM tf_ranked AS tf1
    WHERE tf1.rn = 1
  ),
  tv_ranked AS (
    SELECT
      tv0.id,
      tv0.timesheet_id,
      tv0.status,
      tv0.reason_code,
      tv0.pre_validated,
      tv0.validated_at_utc,
      tv0.updated_at,
      tv0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY tv0.timesheet_id
        ORDER BY tv0.updated_at DESC NULLS LAST, tv0.created_at DESC NULLS LAST, tv0.id DESC
      ) AS rn
    FROM public.timesheet_validations AS tv0
    WHERE tv0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_tv
        WHERE bs_tv.timesheet_id = tv0.timesheet_id
      )
  ),
  tv_latest AS (
    SELECT
      tv1.id,
      tv1.timesheet_id,
      tv1.status,
      tv1.reason_code,
      tv1.pre_validated,
      tv1.validated_at_utc,
      tv1.updated_at,
      tv1.created_at
    FROM tv_ranked AS tv1
    WHERE tv1.rn = 1
  ),
  te_ranked AS (
    SELECT
      te0.id,
      te0.timesheet_id,
      te0.kind,
      te0.display_name,
      te0.storage_key,
      te0.created_at,
      ROW_NUMBER() OVER (
        PARTITION BY te0.timesheet_id
        ORDER BY
          CASE
            WHEN UPPER(COALESCE(te0.kind, '')) = 'TIMESHEET' THEN 0
            ELSE 1
          END ASC,
          te0.created_at ASC NULLS LAST,
          te0.id ASC
      ) AS rn
    FROM public.timesheet_evidence AS te0
    WHERE te0.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_te
        WHERE bs_te.timesheet_id = te0.timesheet_id
      )
  ),
  te_primary AS (
    SELECT
      te1.id,
      te1.timesheet_id,
      te1.kind,
      te1.display_name,
      te1.storage_key
    FROM te_ranked AS te1
    WHERE te1.rn = 1
  ),
  te_agg AS (
    SELECT
      te2.timesheet_id,
      COUNT(te2.id)::integer AS attached_evidence_count,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'TIMESHEET'), FALSE) AS has_timesheet_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'MILEAGE'), FALSE) AS has_mileage_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'TRAVEL'), FALSE) AS has_travel_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'ACCOMMODATION'), FALSE) AS has_accommodation_evidence,
      COALESCE(BOOL_OR(UPPER(COALESCE(te2.kind, '')) = 'OTHER'), FALSE) AS has_other_evidence,
      MAX(te2.created_at) AS evidence_updated_at
    FROM public.timesheet_evidence AS te2
    WHERE te2.timesheet_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_te2
        WHERE bs_te2.timesheet_id = te2.timesheet_id
      )
    GROUP BY te2.timesheet_id
  ),
  mq_ranked AS (
    SELECT
      mq0.id,
      mq0.r2_key,
      mq0.original_filename,
      mq0.mime_type,
      mq0.uploaded_at_utc,
      mq0.last_rotation_deg,
      mq0.meta_json,
      NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') AS contract_week_id_text,
      CASE
        WHEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET')) IN ('TIMESHEET', 'MILEAGE', 'TRAVEL', 'ACCOMMODATION', 'OTHER')
          THEN UPPER(COALESCE(NULLIF(BTRIM(COALESCE(mq0.meta_json->>'staged_kind', mq0.meta_json->>'kind', '')), ''), 'TIMESHEET'))
        ELSE 'OTHER'
      END AS staged_kind_upper,
      ROW_NUMBER() OVER (
        PARTITION BY NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '')
        ORDER BY mq0.uploaded_at_utc ASC NULLS LAST, mq0.id ASC
      ) AS rn
    FROM public.manual_timesheet_queue AS mq0
    WHERE UPPER(COALESCE(mq0.status, '')) = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '') IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM base_scope AS bs_mq
        WHERE bs_mq.contract_week_id::text = NULLIF(BTRIM(COALESCE(mq0.meta_json->>'contract_week_id', '')), '')
      )
  ),
  mq_primary AS (
    SELECT
      mq1.id,
      mq1.r2_key,
      mq1.original_filename,
      mq1.mime_type,
      mq1.last_rotation_deg,
      mq1.contract_week_id_text,
      mq1.staged_kind_upper
    FROM mq_ranked AS mq1
    WHERE mq1.rn = 1
  ),
  mq_agg AS (
    SELECT
      mq2.contract_week_id_text,
      COUNT(mq2.id)::integer AS queue_staged_count,
      MAX(mq2.uploaded_at_utc) AS queue_updated_at,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'TIMESHEET'), FALSE) AS has_staged_timesheet_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'MILEAGE'), FALSE) AS has_staged_mileage_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'TRAVEL'), FALSE) AS has_staged_travel_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'ACCOMMODATION'), FALSE) AS has_staged_accommodation_evidence,
      COALESCE(BOOL_OR(mq2.staged_kind_upper = 'OTHER'), FALSE) AS has_staged_other_evidence
    FROM mq_ranked AS mq2
    GROUP BY mq2.contract_week_id_text
  ),
  raw_rows AS (
    SELECT
      vb.timesheet_id AS timesheet_id,
      vb.timesheet_status::text AS timesheet_status,
      vb.week_ending_date AS week_ending_date,
      vb.booking_id AS booking_id,
      vb.occupant_key_norm AS occupant_key_norm,
      vb.hospital_norm AS hospital_norm,
      vb.sheet_scope::text AS sheet_scope,
      vb.submission_mode::text AS submission_mode,
      vb.authorised_at_server AS authorised_at_server,
      COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id) AS candidate_id,
      COALESCE(vb.client_id, tf.client_id, ct.client_id) AS client_id,
      COALESCE(vb.pay_method, tf.pay_method, cand.pay_method) AS pay_method,
      COALESCE(tf.processing_status::text, vb.processing_status::text) AS processing_status,
      COALESCE(tf.basis::text, vb.basis::text) AS basis,
      COALESCE(tf.total_hours, vb.total_hours) AS total_hours,
      COALESCE(tf.total_pay_ex_vat, vb.total_pay_ex_vat) AS total_pay_ex_vat,
      COALESCE(tf.total_charge_ex_vat, vb.total_charge_ex_vat) AS total_charge_ex_vat,
      COALESCE(tf.margin_ex_vat, vb.margin_ex_vat) AS margin_ex_vat,
      COALESCE(tf.paid_at_utc, vb.paid_at_utc) AS paid_at_utc,
      COALESCE(tf.pay_on_hold, vb.pay_on_hold, FALSE) AS pay_on_hold,
      vb.ready_to_pay AS ready_to_pay,
      COALESCE(tf.locked_by_invoice_id, vb.locked_by_invoice_id) AS locked_by_invoice_id,
      COALESCE(NULLIF(BTRIM(vb.candidate_name), ''), NULLIF(BTRIM(cand.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(cand.first_name), ''), NULLIF(BTRIM(cand.last_name), ''))), ''), vb.occupant_key_norm) AS candidate_name,
      COALESCE(NULLIF(BTRIM(cand.display_name), ''), NULLIF(BTRIM(vb.candidate_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(cand.first_name), ''), NULLIF(BTRIM(cand.last_name), ''))), ''), vb.occupant_key_norm) AS candidate_display_name,
      cand.first_name AS candidate_first_name,
      cand.last_name AS candidate_surname,
      cand.email AS candidate_email,
      COALESCE(cand.opt_in_email, TRUE) AS candidate_opt_in_email,
      COALESCE(NULLIF(BTRIM(vb.client_name), ''), NULLIF(BTRIM(cli.name), ''), vb.hospital_norm) AS client_name,
      COALESCE(NULLIF(BTRIM(cli.name), ''), NULLIF(BTRIM(vb.client_name), ''), vb.hospital_norm) AS client_display_name,
      vb.nhsp_shift_count AS nhsp_shift_count,
      vb.nhsp_shift_included_count AS nhsp_shift_included_count,
      vb.nhsp_shift_deferred_count AS nhsp_shift_deferred_count,
      COALESCE(tv.status::text, vb.validation_status::text) AS validation_status,
      vb.summary_stage AS summary_stage,
      CASE
        WHEN (
          COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
          OR COALESCE(vb.is_adjustment, FALSE) = TRUE
          OR COALESCE(cw.is_adjustment, FALSE) = TRUE
          OR ts.parent_timesheet_id IS NOT NULL
          OR ts.correction_id IS NOT NULL
          OR ts.correction_kind IS NOT NULL
        )
        AND (
          UPPER(COALESCE(vb.route_type, '')) IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT')
          OR COALESCE(vb.client_is_nhsp, FALSE) = TRUE
          OR COALESCE(ct.is_nhsp, FALSE) = TRUE
          OR UPPER(COALESCE(tf.basis::text, vb.basis::text, '')) IN ('NHSP', 'NHSP_ADJUSTMENT')
        ) THEN 'WEEKLY_NHSP_ADJUSTMENT'
        WHEN (
          COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
          OR COALESCE(vb.is_adjustment, FALSE) = TRUE
          OR COALESCE(cw.is_adjustment, FALSE) = TRUE
          OR ts.parent_timesheet_id IS NOT NULL
          OR ts.correction_id IS NOT NULL
          OR ts.correction_kind IS NOT NULL
        )
        AND (
          UPPER(COALESCE(vb.route_type, '')) IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT')
          OR COALESCE(vb.client_autoprocess_hr, FALSE) = TRUE
          OR COALESCE(ct.autoprocess_hr, FALSE) = TRUE
          OR UPPER(COALESCE(tf.basis::text, vb.basis::text, '')) IN ('HEALTHROSTER_ADJUSTMENT', 'HEALTHROSTER_SELF_BILL')
        ) THEN 'WEEKLY_HEALTHROSTER_ADJUSTMENT'
        WHEN (
          COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
          OR COALESCE(vb.is_adjustment, FALSE) = TRUE
          OR COALESCE(cw.is_adjustment, FALSE) = TRUE
          OR ts.parent_timesheet_id IS NOT NULL
          OR ts.correction_id IS NOT NULL
          OR ts.correction_kind IS NOT NULL
        ) THEN 'WEEKLY_MANUAL_ADJUSTMENT'
        ELSE vb.route_type
      END AS route_type,
      vb.contract_week_id AS contract_week_id,
      vb.contract_week_ending_date AS contract_week_ending_date,
      COALESCE(vb.contract_week_status::text, cw.status::text) AS contract_week_status,
      COALESCE(vb.additional_seq, cw.additional_seq, 0) AS additional_seq,
      (
        COALESCE(ts.is_adjustment, FALSE) = TRUE
        OR COALESCE(vb.is_adjustment, FALSE) = TRUE
        OR COALESCE(cw.is_adjustment, FALSE) = TRUE
        OR COALESCE(vb.additional_seq, cw.additional_seq, 0) > 0
      ) AS is_adjustment,
      vb.qr_status::text AS vb_qr_status,
      ts.qr_status::text AS ts_qr_status,
      ts.qr_token AS qr_token,
      ts.qr_generated_at AS qr_generated_at,
      ts.qr_scanned_at AS qr_scanned_at,
      ts.qr_last_sent_hash AS qr_last_sent_hash,
      ts.qr_signed_hash AS qr_signed_hash,
      ts.qr_signed_at_utc AS qr_signed_at_utc,
      vb.qr_token AS vb_qr_token,
      vb.qr_generated_at AS vb_qr_generated_at,
      vb.qr_scanned_at AS vb_qr_scanned_at,
      COALESCE(vb.pay_adjustment_count, 0) AS pay_adjustment_count,
      COALESCE(vb.has_pay_adjustments, FALSE) AS has_pay_adjustments,
      COALESCE(vb.client_autoprocess_hr, FALSE) AS client_autoprocess_hr,
      COALESCE(vb.client_requires_hr, FALSE) AS client_requires_hr,
      COALESCE(vb.client_no_timesheet_required, FALSE) AS client_no_timesheet_required,
      COALESCE(vb.client_pay_reference_required, FALSE) AS client_pay_reference_required,
      COALESCE(vb.client_invoice_reference_required, FALSE) AS client_invoice_reference_required,
      COALESCE(vb.client_hr_validation_required, FALSE) AS client_hr_validation_required,
      COALESCE(vb.client_ts_reference_required, FALSE) AS client_ts_reference_required,
      COALESCE(vb.client_is_nhsp, FALSE) AS client_is_nhsp,
      COALESCE(vb.hr_validation_required_for_invoice, FALSE) AS hr_validation_required_for_invoice,
      COALESCE(tf.has_rate_issue, vb.has_rate_issue, FALSE) AS has_rate_issue,
      COALESCE(tf.has_pay_channel_issue, vb.has_pay_channel_issue, FALSE) AS has_pay_channel_issue,
      COALESCE(tf.hr_crosscheck_status, vb.hr_crosscheck_status) AS hr_crosscheck_status,
      COALESCE(tf.hr_crosscheck_issues, vb.hr_crosscheck_issues) AS hr_crosscheck_issues,
      COALESCE(tf.external_source_rows_json, vb.external_source_rows_json) AS external_source_rows_json,
      COALESCE(vb.issue_codes, ARRAY[]::text[]) AS issue_codes,
      tf.worked_start_iso AS worked_start_iso,
      COALESCE(ts.contract_id, cw.contract_id, ct.id) AS contract_id,
      ct.role AS contract_role,
      ct.band AS contract_band,
      ct.display_site AS contract_display_site,
      ct.ward_hint AS contract_ward_hint,
      ct.std_schedule_json AS contract_std_schedule_json,
      ct.additional_rates_json AS contract_additional_rates_json,
      ct.mileage_pay_rate AS contract_mileage_pay_rate,
      ct.mileage_charge_rate AS contract_mileage_charge_rate,
      ct.default_submission_mode::text AS contract_default_submission_mode,
      ct.weekly_timesheet_source AS contract_weekly_timesheet_source,
      COALESCE(ct.is_nhsp, FALSE) AS contract_is_nhsp,
      COALESCE(ct.autoprocess_hr, FALSE) AS contract_autoprocess_hr,
      COALESCE(ct.requires_hr, FALSE) AS contract_requires_hr,
      COALESCE(ct.no_timesheet_required, FALSE) AS contract_no_timesheet_required,
      COALESCE(ct.daily_calc_of_invoices, FALSE) AS contract_daily_calc_of_invoices,
      COALESCE(ct.group_nightsat_sunbh, FALSE) AS contract_group_nightsat_sunbh,
      COALESCE(ct.is_ad_hoc, FALSE) AS contract_is_ad_hoc,
      vb.candidate_hint_text AS candidate_hint_text,
      COALESCE(tf.expenses_pay_ex_vat, vb.expenses_pay_ex_vat) AS expenses_pay_ex_vat,
      COALESCE(tf.expenses_description, vb.expenses_description) AS expenses_description,
      COALESCE(tf.mileage_units, vb.mileage_units) AS mileage_units,
      COALESCE(tf.mileage_pay_rate, vb.mileage_pay_rate) AS mileage_pay_rate,
      COALESCE(tf.mileage_charge_rate, vb.mileage_charge_rate) AS mileage_charge_rate,
      COALESCE(tf.mileage_pay_ex_vat, vb.mileage_pay_ex_vat) AS mileage_pay_ex_vat,
      COALESCE(tf.travel_pay_ex_vat, vb.travel_pay_ex_vat) AS travel_pay_ex_vat,
      COALESCE(tf.travel_charge_ex_vat, vb.travel_charge_ex_vat) AS travel_charge_ex_vat,
      COALESCE(tf.accommodation_pay_ex_vat, vb.accommodation_pay_ex_vat) AS accommodation_pay_ex_vat,
      COALESCE(tf.accommodation_charge_ex_vat, vb.accommodation_charge_ex_vat) AS accommodation_charge_ex_vat,
      COALESCE(tf.other_pay_ex_vat, vb.other_pay_ex_vat) AS other_pay_ex_vat,
      COALESCE(tf.other_charge_ex_vat, vb.other_charge_ex_vat) AS other_charge_ex_vat,
      vb.invoice_segments_total AS invoice_segments_total,
      vb.invoice_segments_locked AS invoice_segments_locked,
      vb.invoice_segments_unlocked AS invoice_segments_unlocked,
      vb.invoice_segment_stage AS invoice_segment_stage,
      vb.tools_stage AS tools_stage,
      vb.processing_status_display AS processing_status_display,
      vb.invoice_is_paid AS invoice_is_paid,
      vb.refs_block_invoicing AS refs_block_invoicing,
      vb.refs_block_issuing_invoices AS refs_block_issuing_invoices,
      vb.refs_block_invoice_and_issuing AS refs_block_invoice_and_issuing,
      COALESCE(vb.pay_icon_code, 'NONE') AS pay_icon_code,
      vb.pay_status_code AS pay_status_code,
      vb.pay_paid_at_utc AS pay_paid_at_utc,
      COALESCE(vb.net_delta_ex_vat, 0::numeric) AS net_delta_ex_vat,
      ts.version AS timesheet_version,
      ts.is_current AS timesheet_is_current,
      ts.created_at AS timesheet_created_at,
      ts.updated_at AS timesheet_updated_at,
      ts.worked_start_iso AS timesheet_worked_start_iso,
      ts.worked_end_iso AS timesheet_worked_end_iso,
      ts.scheduled_start_iso AS timesheet_scheduled_start_iso,
      ts.scheduled_end_iso AS timesheet_scheduled_end_iso,
      ts.break_start_iso AS timesheet_break_start_iso,
      ts.break_end_iso AS timesheet_break_end_iso,
      ts.break_minutes AS timesheet_break_minutes,
      ts.worked_minutes AS timesheet_worked_minutes,
      ts.reference_number AS reference_number,
      ts.r2_nurse_key AS r2_nurse_key,
      ts.r2_auth_key AS r2_auth_key,
      ts.manual_pdf_r2_key AS manual_pdf_r2_key,
      ts.manual_pdf_rotation_degrees AS manual_pdf_rotation_degrees,
      ts.generated_pdf_at_utc AS generated_pdf_at_utc,
      ts.actual_schedule_json AS timesheet_actual_schedule_json,
      tf.actual_schedule_json AS tsfin_actual_schedule_json,
      ts.additional_units_week AS additional_units_week,
      ts.additional_units_per_day AS additional_units_per_day,
      ts.day_references_json AS day_references_json,
      ts.parent_timesheet_id AS parent_timesheet_id,
      ts.adjustment_origin AS adjustment_origin,
      ts.correction_id AS correction_id,
      ts.correction_kind AS correction_kind,
      cw.id AS cw_id,
      cw.status::text AS cw_status,
      cw.submission_mode_snapshot::text AS cw_submission_mode_snapshot,
      cw.timesheet_id AS cw_timesheet_id,
      cw.uploaded_pdf_r2_key AS uploaded_pdf_r2_key,
      cw.day_entries_json AS contract_week_day_entries_json,
      cw.totals_json AS contract_week_totals_json,
      cw.planned_schedule_json AS contract_week_planned_schedule_json,
      cw.updated_at AS contract_week_updated_at,
      cw.created_at AS contract_week_created_at,
      tf.id AS tsfin_id,
      tf.timesheet_version AS tsfin_timesheet_version,
      tf.is_stale AS tsfin_is_stale,
      tf.stale_reason AS tsfin_stale_reason,
      tf.computed_at_utc AS tsfin_computed_at_utc,
      tf.processed_by_user_id AS processed_by_user_id,
      tf.processed_at_utc AS processed_at_utc,
      pu.display_name AS processed_by_display,
      tf.authorised_by_user_id AS authorised_by_user_id,
      tf.authorised_at_utc AS authorised_at_utc,
      au.display_name AS authorised_by_display,
      tf.invoice_breakdown_json AS invoice_breakdown_json,
      tf.policy_snapshot_json AS policy_snapshot_json,
      tf.updated_at AS tsfin_updated_at,
      tv.updated_at AS validation_updated_at,
      tv.pre_validated AS validation_pre_validated,
      tv.reason_code AS validation_reason_code,
      tv.validated_at_utc AS validation_validated_at_utc,
      te.evidence_updated_at AS evidence_updated_at,
      te.attached_evidence_count AS attached_evidence_count,
      COALESCE(te.has_timesheet_evidence, FALSE) AS ev_timesheet,
      COALESCE(te.has_mileage_evidence, FALSE) AS ev_mileage,
      COALESCE(te.has_travel_evidence, FALSE) AS ev_travel,
      COALESCE(te.has_accommodation_evidence, FALSE) AS ev_accommodation,
      COALESCE(te.has_other_evidence, FALSE) AS ev_other,
      tep.id AS primary_evidence_id,
      tep.kind AS primary_evidence_kind,
      tep.display_name AS primary_evidence_display_name,
      tep.storage_key AS primary_evidence_storage_key,
      COALESCE(mqa.queue_staged_count, 0) AS queue_staged_count,
      mqa.queue_updated_at AS queue_updated_at,
      COALESCE(mqa.has_staged_timesheet_evidence, FALSE) AS staged_ev_timesheet,
      COALESCE(mqa.has_staged_mileage_evidence, FALSE) AS staged_ev_mileage,
      COALESCE(mqa.has_staged_travel_evidence, FALSE) AS staged_ev_travel,
      COALESCE(mqa.has_staged_accommodation_evidence, FALSE) AS staged_ev_accommodation,
      COALESCE(mqa.has_staged_other_evidence, FALSE) AS staged_ev_other,
      mqp.id AS primary_queue_id,
      mqp.r2_key AS primary_queue_r2_key,
      mqp.original_filename AS primary_queue_original_filename,
      mqp.mime_type AS primary_queue_mime_type,
      mqp.last_rotation_deg AS primary_queue_rotation_degrees,
      mqp.staged_kind_upper AS primary_queue_staged_kind,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_electronic
        WHERE hist_electronic.booking_id = vb.booking_id
          AND hist_electronic.is_current = FALSE
          AND UPPER(COALESCE(hist_electronic.submission_mode::text, '')) = 'ELECTRONIC'
      ) AS has_historical_electronic,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_qr_pending
        WHERE hist_qr_pending.booking_id = vb.booking_id
          AND hist_qr_pending.is_current = FALSE
          AND UPPER(COALESCE(hist_qr_pending.qr_status::text, '')) = 'PENDING'
          AND hist_qr_pending.qr_generated_at IS NOT NULL
          AND hist_qr_pending.qr_scanned_at IS NULL
          AND NULLIF(BTRIM(COALESCE(hist_qr_pending.qr_token, '')), '') IS NOT NULL
      ) AS has_historical_qr_pending,
      EXISTS (
        SELECT 1
        FROM public.timesheets AS hist_qr_signed
        WHERE hist_qr_signed.booking_id = vb.booking_id
          AND hist_qr_signed.is_current = FALSE
          AND UPPER(COALESCE(hist_qr_signed.qr_status::text, '')) = 'USED'
          AND hist_qr_signed.qr_scanned_at IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(hist_qr_signed.manual_pdf_r2_key, '')), '') IS NOT NULL
      ) AS has_historical_qr_signed,
      EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN tf.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'array' THEN tf.invoice_breakdown_json
            WHEN jsonb_typeof(tf.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array' THEN tf.invoice_breakdown_json->'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment_value)
        WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_value->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) AS has_segment_invoice_lock
    FROM base_scope AS vb
    LEFT JOIN public.timesheets AS ts
      ON ts.timesheet_id = vb.timesheet_id
     AND ts.is_current = TRUE
    LEFT JOIN public.contract_weeks AS cw
      ON cw.id = vb.contract_week_id
    LEFT JOIN public.contracts AS ct
      ON ct.id = COALESCE(ts.contract_id, cw.contract_id)
    LEFT JOIN tf_latest AS tf
      ON tf.timesheet_id = vb.timesheet_id
    LEFT JOIN tv_latest AS tv
      ON tv.timesheet_id = vb.timesheet_id
    LEFT JOIN te_agg AS te
      ON te.timesheet_id = vb.timesheet_id
    LEFT JOIN te_primary AS tep
      ON tep.timesheet_id = vb.timesheet_id
    LEFT JOIN mq_agg AS mqa
      ON mqa.contract_week_id_text = vb.contract_week_id::text
    LEFT JOIN mq_primary AS mqp
      ON mqp.contract_week_id_text = vb.contract_week_id::text
    LEFT JOIN public.candidates AS cand
      ON cand.id = COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id)
    LEFT JOIN public.clients AS cli
      ON cli.id = COALESCE(vb.client_id, tf.client_id, ct.client_id)
    LEFT JOIN public.tms_users AS pu
      ON pu.id = tf.processed_by_user_id
    LEFT JOIN public.tms_users AS au
      ON au.id = tf.authorised_by_user_id
    WHERE (v_candidate_id IS NULL OR COALESCE(vb.candidate_id, tf.candidate_id, ct.candidate_id) = v_candidate_id)
      AND (v_client_id IS NULL OR COALESCE(vb.client_id, tf.client_id, ct.client_id) = v_client_id)
      AND (v_timesheet_ids IS NULL OR vb.timesheet_id = ANY(v_timesheet_ids))
      AND (v_contract_week_ids IS NULL OR vb.contract_week_id = ANY(v_contract_week_ids))
      AND (
        v_row_keys IS NULL
        OR CASE
             WHEN vb.timesheet_id IS NOT NULL THEN 'timesheet:' || vb.timesheet_id::text
             WHEN vb.contract_week_id IS NOT NULL THEN 'contract_week:' || vb.contract_week_id::text
             ELSE ''
           END = ANY(v_row_keys)
      )
      AND (
        v_q_like IS NULL
        OR CONCAT_WS(
             ' ',
             vb.candidate_name,
             cand.display_name,
             cand.first_name,
             cand.last_name,
             cand.email,
             vb.client_name,
             cli.name,
             vb.booking_id,
             vb.occupant_key_norm,
             vb.hospital_norm,
             ts.ward_norm,
             ct.display_site,
             ct.ward_hint,
             ts.reference_number,
             vb.route_type,
             vb.summary_stage,
             vb.tools_stage,
             vb.processing_status_display,
             vb.validation_status::text,
             vb.timesheet_id::text,
             vb.contract_week_id::text,
             COALESCE(ts.contract_id, cw.contract_id, ct.id)::text,
             vb.issue_codes::text
           ) ILIKE v_q_like ESCAPE E'\\'
      )
  ),
  classified AS (
    SELECT
      rr.*,
      UPPER(COALESCE(rr.route_type, '')) AS route_type_upper,
      UPPER(COALESCE(rr.sheet_scope, '')) AS sheet_scope_upper,
      UPPER(COALESCE(rr.submission_mode, rr.cw_submission_mode_snapshot, '')) AS submission_mode_upper,
      UPPER(COALESCE(rr.vb_qr_status, rr.ts_qr_status, '')) AS qr_status_upper,
      (
        COALESCE(rr.is_adjustment, FALSE)
        OR COALESCE(rr.additional_seq, 0) > 0
        OR rr.parent_timesheet_id IS NOT NULL
        OR rr.correction_id IS NOT NULL
        OR rr.correction_kind IS NOT NULL
      ) AS is_adjustment_or_additional,
      (
        COALESCE(rr.vb_qr_status, rr.ts_qr_status) IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(rr.qr_token, rr.vb_qr_token, '')), '') IS NOT NULL
        OR rr.qr_generated_at IS NOT NULL
        OR rr.vb_qr_generated_at IS NOT NULL
        OR rr.qr_scanned_at IS NOT NULL
        OR rr.vb_qr_scanned_at IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(rr.qr_last_sent_hash, '')), '') IS NOT NULL
        OR COALESCE(rr.vb_qr_status, rr.ts_qr_status) IN ('PENDING', 'USED', 'EXPIRED', 'CANCELLED')
      ) AS is_qr_route,
      CASE
        WHEN UPPER(COALESCE(rr.sheet_scope, '')) IN ('DAILY', 'WEEKLY') THEN UPPER(COALESCE(rr.sheet_scope, ''))
        WHEN UPPER(COALESCE(rr.route_type, '')) LIKE 'DAILY\_%' ESCAPE '\' THEN 'DAILY'
        ELSE 'WEEKLY'
      END AS period_type
    FROM raw_rows AS rr
  ),
  routed AS (
    SELECT
      cl.*,
      CASE
        WHEN cl.is_adjustment_or_additional = TRUE
          AND cl.route_type_upper IN ('WEEKLY_NHSP_ADJUSTMENT', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'WEEKLY_MANUAL_ADJUSTMENT')
          AND cl.submission_mode_upper = 'MANUAL' THEN 'MANUAL_NON_QR'
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
          OR (cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE)
        ) THEN 'IMPORT_AUTHORITATIVE'
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_family,
      CASE
        WHEN cl.is_adjustment_or_additional = TRUE
          AND cl.route_type_upper IN ('WEEKLY_NHSP_ADJUSTMENT', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'WEEKLY_MANUAL_ADJUSTMENT')
          AND cl.submission_mode_upper = 'MANUAL' THEN 'MANUAL_NON_QR'
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
        ) THEN 'NHSP'
        WHEN cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE THEN 'HEALTHROSTER_NO_TIMESHEET'
        WHEN cl.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'HEALTHROSTER_TIMESHEET_REQUIRED'
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS route_subfamily,
      CASE
        WHEN cl.is_adjustment_or_additional = TRUE
          AND cl.route_type_upper IN ('WEEKLY_NHSP_ADJUSTMENT', 'WEEKLY_HEALTHROSTER_ADJUSTMENT', 'WEEKLY_MANUAL_ADJUSTMENT')
          AND cl.submission_mode_upper = 'MANUAL' THEN 'MANUAL_NON_QR'
        WHEN (
          cl.route_type_upper = 'WEEKLY_NHSP'
          OR (cl.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' AND NOT (cl.is_adjustment_or_additional AND cl.submission_mode_upper = 'MANUAL'))
          OR (cl.route_type_upper = 'WEEKLY_HEALTHROSTER' AND COALESCE(cl.client_no_timesheet_required, FALSE) = TRUE)
        ) THEN NULL::text
        WHEN cl.is_qr_route THEN 'QR'
        WHEN cl.submission_mode_upper = 'ELECTRONIC' THEN 'ELECTRONIC'
        ELSE 'MANUAL_NON_QR'
      END AS underlying_channel_family,
      (
        cl.route_type_upper = 'WEEKLY_HEALTHROSTER'
        AND COALESCE(cl.client_no_timesheet_required, FALSE) <> TRUE
      ) AS compare_block_required
    FROM classified AS cl
  ),
  decisions AS (
    SELECT
      rt.*,
      CASE
        WHEN rt.route_family = 'IMPORT_AUTHORITATIVE' AND rt.route_subfamily = 'HEALTHROSTER_NO_TIMESHEET' THEN 'HR'
        WHEN rt.route_family = 'IMPORT_AUTHORITATIVE' THEN 'NHSP'
        ELSE 'TIMESHEETS'
      END AS bulk_authorise_classification,
      CASE
        WHEN rt.route_family = 'MANUAL_NON_QR' THEN 1
        WHEN rt.route_family = 'QR' THEN 2
        WHEN rt.route_family = 'ELECTRONIC' THEN 3
        ELSE 99
      END AS timesheet_type_sort_key,
      (
        rt.timesheet_id IS NULL
        AND rt.contract_week_id IS NOT NULL
        AND (
          UPPER(COALESCE(rt.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.contract_week_status, '')) IN ('OPEN', 'PLANNED')
        )
      ) AS is_planned_week_unprocessed,
      (
        rt.timesheet_id IS NOT NULL
        AND (
          UPPER(COALESCE(rt.processing_status, '')) IN ('UNPROCESSED', 'UNASSIGNED')
          OR UPPER(COALESCE(rt.summary_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.tools_stage, '')) = 'UNPROCESSED'
          OR UPPER(COALESCE(rt.processing_status_display, '')) = 'UNPROCESSED'
        )
      ) AS is_real_row_unprocessed,
      (
        COALESCE(rt.locked_by_invoice_id, NULL) IS NOT NULL
        OR COALESCE(rt.paid_at_utc, NULL) IS NOT NULL
        OR COALESCE(rt.has_segment_invoice_lock, FALSE) = TRUE
        OR COALESCE(rt.invoice_segments_locked, 0) > 0
        OR COALESCE(rt.invoice_is_paid, FALSE) = TRUE
      ) AS locked,
      (
        rt.authorised_at_server IS NOT NULL
        OR rt.authorised_at_utc IS NOT NULL
      ) AS is_authorised,
      (
        UPPER(COALESCE(rt.processing_status, '')) = 'PENDING_AUTH'
        OR (
          COALESCE(rt.client_requires_hr, FALSE) = TRUE
          AND COALESCE(rt.client_autoprocess_hr, FALSE) = FALSE
          AND UPPER(COALESCE(rt.processing_status, '')) = 'READY_FOR_HR'
        )
      ) AS requires_authorisation,
      (
        UPPER(COALESCE(rt.validation_status, '')) IN ('VALIDATION_OK', 'OVERRIDDEN')
      ) AS hr_validation_satisfied,
      (
        UPPER(COALESCE(rt.qr_status_upper, '')) = 'PENDING'
        AND (
          (
            NULLIF(BTRIM(COALESCE(rt.qr_token, rt.vb_qr_token, '')), '') IS NOT NULL
            AND COALESCE(rt.qr_generated_at, rt.vb_qr_generated_at) IS NOT NULL
          )
          OR NULLIF(BTRIM(COALESCE(rt.qr_last_sent_hash, '')), '') IS NOT NULL
        )
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NULL
      ) AS qr_pending_awaiting_signature,
      (
        UPPER(COALESCE(rt.qr_status_upper, '')) = 'USED'
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NOT NULL
      ) AS qr_signed_returned,
      (
        UPPER(COALESCE(rt.submission_mode, rt.cw_submission_mode_snapshot, '')) = 'MANUAL'
        AND NULLIF(BTRIM(COALESCE(rt.qr_status_upper, '')), '') IS NULL
        AND NULLIF(BTRIM(COALESCE(rt.qr_token, rt.vb_qr_token, '')), '') IS NULL
        AND COALESCE(rt.qr_generated_at, rt.vb_qr_generated_at) IS NULL
        AND COALESCE(rt.qr_scanned_at, rt.vb_qr_scanned_at) IS NULL
      ) AS is_manual_only,
      (
        COALESCE(rt.contract_default_submission_mode, '') = 'ELECTRONIC'
        OR UPPER(COALESCE(rt.policy_snapshot_json->>'default_submission_mode', '')) = 'ELECTRONIC'
      ) AS supports_electronic_submission,
      CASE
        WHEN COALESCE(rt.is_adjustment_or_additional, FALSE) = TRUE THEN
          CASE
            WHEN UPPER(COALESCE(rt.adjustment_origin, '')) = 'MANUAL_ADJUSTMENT' THEN 'Manual Adjustment'
            WHEN UPPER(COALESCE(rt.adjustment_origin, '')) LIKE 'IMPORT\_%' ESCAPE '\' OR rt.correction_kind IS NOT NULL OR rt.correction_id IS NOT NULL THEN 'NHSP Adjustment'
            ELSE 'Manual Adjustment'
          END
        WHEN rt.route_type_upper = 'DAILY_ELECTRONIC' THEN 'Daily Electronic'
        WHEN rt.route_type_upper = 'DAILY_MANUAL' THEN 'Daily Manual'
        WHEN rt.route_type_upper = 'WEEKLY_ELECTRONIC' THEN 'Weekly Electronic'
        WHEN rt.route_type_upper = 'WEEKLY_MANUAL' THEN 'Weekly Manual'
        WHEN rt.route_type_upper = 'WEEKLY_NHSP' THEN 'Weekly NHSP'
        WHEN rt.route_type_upper = 'WEEKLY_NHSP_ADJUSTMENT' THEN 'Weekly NHSP Adjustment'
        WHEN rt.route_type_upper = 'WEEKLY_HEALTHROSTER' THEN 'Weekly HealthRoster'
        WHEN rt.route_type_upper = 'WEEKLY_HEALTHROSTER_ADJUSTMENT' THEN 'Weekly HealthRoster Adjustment'
        WHEN rt.route_type_upper = 'WEEKLY_MANUAL_ADJUSTMENT' THEN 'Weekly Manual Adjustment'
        ELSE 'Unknown'
      END AS route_display,
      CASE
        WHEN rt.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(rt.client_is_nhsp, FALSE) = TRUE OR COALESCE(rt.contract_is_nhsp, FALSE) = TRUE THEN 'NHSP'
        WHEN rt.route_type_upper IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT') THEN 'HEALTHROSTER'
        WHEN rt.period_type = 'WEEKLY' THEN 'NONE'
        ELSE NULL::text
      END AS contract_weekly_mode,
      CASE
        WHEN rt.route_type_upper IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT') AND COALESCE(rt.client_no_timesheet_required, FALSE) = TRUE THEN 'CREATE'
        WHEN rt.route_type_upper IN ('WEEKLY_HEALTHROSTER', 'WEEKLY_HEALTHROSTER_ADJUSTMENT') THEN 'VERIFY'
        ELSE ''
      END AS contract_hr_weekly_behaviour
    FROM routed AS rt
  ),
  final_rows AS (
    SELECT
      dc.*,
      (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed) AS is_unprocessed,
      (
        dc.is_adjustment_or_additional = TRUE
        AND dc.contract_week_id IS NOT NULL
        AND dc.route_family = 'MANUAL_NON_QR'
        AND COALESCE(dc.locked, FALSE) = FALSE
        AND COALESCE(dc.is_authorised, FALSE) = FALSE
        AND (dc.timesheet_id IS NULL OR dc.is_real_row_unprocessed = TRUE)
      ) AS supports_unprocessed_expense_draft,
      (
        dc.is_adjustment_or_additional = TRUE
        AND COALESCE(dc.total_hours, 0::numeric) = 0::numeric
        AND (
          COALESCE(dc.timesheet_actual_schedule_json, dc.tsfin_actual_schedule_json) IS NULL
          OR CASE
            WHEN jsonb_typeof(COALESCE(dc.timesheet_actual_schedule_json, dc.tsfin_actual_schedule_json)) = 'array' THEN jsonb_array_length(COALESCE(dc.timesheet_actual_schedule_json, dc.tsfin_actual_schedule_json)) = 0
            ELSE FALSE
          END
        )
        AND (
          dc.contract_week_planned_schedule_json IS NULL
          OR CASE
            WHEN jsonb_typeof(dc.contract_week_planned_schedule_json) = 'array' THEN jsonb_array_length(dc.contract_week_planned_schedule_json) = 0
            ELSE FALSE
          END
        )
      ) AS keep_additional_manual_adjustment_schedule_empty,
      CASE WHEN (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed) THEN 'UNPROCESSED' ELSE 'PROCESSED' END AS bulk_process_bucket,
      (dc.hr_validation_required_for_invoice = TRUE AND dc.hr_validation_satisfied = FALSE) AS hr_validation_awaiting,
      (
        dc.qr_pending_awaiting_signature = TRUE
        OR UPPER(COALESCE(dc.processing_status, '')) = 'AWAITING_MANUAL_SIGNATURE'
        OR 'Awaiting signed QR timesheet' = ANY(dc.issue_codes)
      ) AS qr_unsigned_blocked,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.requires_authorisation = TRUE
        AND dc.is_authorised = FALSE
        AND (
          dc.qr_pending_awaiting_signature = FALSE
          AND UPPER(COALESCE(dc.processing_status, '')) <> 'AWAITING_MANUAL_SIGNATURE'
          AND NOT ('Awaiting signed QR timesheet' = ANY(dc.issue_codes))
        )
        AND (dc.route_family <> 'QR' OR dc.qr_signed_returned = TRUE)
      ) AS can_bulk_authorise,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_authorised = TRUE
        AND (dc.route_family <> 'QR' OR dc.qr_signed_returned = TRUE)
      ) AS can_bulk_unauthorise,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
      ) AS can_edit_timesheet_data,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
        AND NOT (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed)
      ) AS can_unprocess,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
        AND (dc.is_planned_week_unprocessed OR dc.is_real_row_unprocessed)
      ) AS can_process,
      (
        (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL)
        AND dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.route_family = 'MANUAL_NON_QR'
      ) AS can_save,
      (
        (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR'))
        AND (dc.locked_by_invoice_id IS NOT NULL OR dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0) = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
      ) AS can_manage_evidence,
      (
        (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR'))
        AND (dc.locked_by_invoice_id IS NOT NULL OR dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0) = TRUE
      ) AS evidence_document_locked,
      CASE
        WHEN NOT (dc.timesheet_id IS NOT NULL OR (dc.contract_week_id IS NOT NULL AND dc.route_family = 'MANUAL_NON_QR')) THEN 'INVALID_TIMESHEET_CONTEXT'
        WHEN dc.locked_by_invoice_id IS NOT NULL THEN 'INVOICE_LOCKED'
        WHEN dc.has_segment_invoice_lock = TRUE OR COALESCE(dc.invoice_segments_locked, 0) > 0 THEN 'INVOICE_SEGMENT_LOCKED'
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN 'IMPORT_AUTHORITATIVE_ROUTE'
        ELSE NULL::text
      END AS evidence_lock_reason,
      (
        dc.locked = TRUE
        OR dc.is_authorised = TRUE
        OR dc.route_family <> 'MANUAL_NON_QR'
      ) AS review_only,
      (
        dc.locked = FALSE
        AND dc.is_authorised = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND (
          (dc.period_type = 'WEEKLY' AND dc.contract_week_id IS NOT NULL)
          OR (dc.period_type = 'DAILY' AND dc.timesheet_id IS NOT NULL)
        )
        AND (
          dc.route_family = 'IMPORT_AUTHORITATIVE'
          OR (dc.route_family = 'MANUAL_NON_QR' AND dc.submission_mode_upper = 'MANUAL' AND dc.is_qr_route = FALSE)
        )
      ) AS can_add_additional_manual,
      (
        dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND (
          (dc.period_type = 'WEEKLY' AND dc.submission_mode_upper = 'ELECTRONIC' AND (dc.timesheet_id IS NOT NULL OR dc.contract_week_id IS NOT NULL))
          OR (dc.period_type = 'DAILY' AND dc.submission_mode_upper = 'ELECTRONIC' AND dc.timesheet_id IS NOT NULL)
        )
      ) AS can_switch_to_manual,
      (
        dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.period_type = 'WEEKLY'
        AND dc.timesheet_id IS NULL
        AND dc.contract_week_id IS NOT NULL
        AND dc.submission_mode_upper = 'MANUAL'
      ) AS can_switch_planned_week_to_electronic,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.has_historical_electronic = TRUE
      ) AS can_revert_to_electronic,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.is_manual_only = TRUE
      ) AS can_allow_qr_again,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.route_family <> 'IMPORT_AUTHORITATIVE'
        AND dc.is_manual_only = TRUE
        AND dc.supports_electronic_submission = TRUE
      ) AS can_allow_electronic_again,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.is_qr_route = TRUE
        AND NULLIF(BTRIM(COALESCE(dc.qr_status_upper, '')), '') IS NOT NULL
      ) AS can_convert_qr_to_manual_only,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.has_historical_qr_pending = TRUE
      ) AS can_restore_qr_pending,
      (
        dc.timesheet_id IS NOT NULL
        AND dc.locked = FALSE
        AND dc.is_adjustment_or_additional = FALSE
        AND dc.has_historical_qr_signed = TRUE
      ) AS can_restore_qr_signed,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN 'IMPORT_SOURCE_ONLY'
        WHEN dc.route_family = 'ELECTRONIC' AND dc.generated_pdf_at_utc IS NULL AND (NULLIF(BTRIM(COALESCE(dc.r2_nurse_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(dc.r2_auth_key, '')), '') IS NOT NULL) THEN 'SIGNATURES_ONLY'
        WHEN dc.timesheet_id IS NOT NULL AND (dc.manual_pdf_r2_key IS NOT NULL OR dc.generated_pdf_at_utc IS NOT NULL) THEN 'TIMESHEET_ARTIFACT'
        WHEN dc.timesheet_id IS NULL AND dc.contract_week_id IS NOT NULL AND (dc.uploaded_pdf_r2_key IS NOT NULL OR dc.primary_queue_r2_key IS NOT NULL) THEN 'TIMESHEET_ARTIFACT'
        ELSE 'NONE'
      END AS primary_left_pane_mode,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN NULL::text
        WHEN dc.route_family = 'QR'
          AND COALESCE(
            NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
            CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END
          ) IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.route_family = 'ELECTRONIC'
          AND COALESCE(
            CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
            NULLIF(BTRIM(dc.manual_pdf_r2_key), '')
          ) IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.route_family = 'MANUAL_NON_QR'
          AND dc.timesheet_id IS NOT NULL
          AND NULLIF(BTRIM(dc.manual_pdf_r2_key), '') IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.timesheet_id IS NOT NULL
          AND NULLIF(BTRIM(dc.primary_evidence_storage_key), '') IS NOT NULL THEN 'EVIDENCE'
        WHEN dc.timesheet_id IS NULL
          AND dc.contract_week_id IS NOT NULL
          AND NULLIF(BTRIM(dc.uploaded_pdf_r2_key), '') IS NOT NULL THEN 'DOCUMENT'
        WHEN dc.timesheet_id IS NULL
          AND dc.contract_week_id IS NOT NULL
          AND NULLIF(BTRIM(dc.primary_queue_r2_key), '') IS NOT NULL THEN 'QUEUE'
        ELSE NULL::text
      END AS primary_artifact_source,
      CASE
        WHEN dc.route_family = 'IMPORT_AUTHORITATIVE' THEN NULL::text
        WHEN dc.route_family = 'QR' THEN COALESCE(
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.route_family = 'ELECTRONIC' THEN COALESCE(
          CASE WHEN dc.generated_pdf_at_utc IS NOT NULL AND dc.timesheet_id IS NOT NULL THEN 'docs-pdf/timesheets/ts_' || dc.timesheet_id::text || '.pdf' ELSE NULL END,
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.route_family = 'MANUAL_NON_QR' AND dc.timesheet_id IS NOT NULL THEN COALESCE(
          NULLIF(BTRIM(dc.manual_pdf_r2_key), ''),
          NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        )
        WHEN dc.timesheet_id IS NOT NULL THEN NULLIF(BTRIM(dc.primary_evidence_storage_key), '')
        WHEN dc.timesheet_id IS NULL AND dc.contract_week_id IS NOT NULL THEN COALESCE(NULLIF(BTRIM(dc.uploaded_pdf_r2_key), ''), NULLIF(BTRIM(dc.primary_queue_r2_key), ''))
        ELSE NULL::text
      END AS primary_artifact_storage_key,
      CASE
        WHEN dc.timesheet_id IS NOT NULL THEN dc.timesheet_id::text
        WHEN dc.contract_week_id IS NOT NULL THEN dc.contract_week_id::text
        ELSE NULL::text
      END AS stable_row_id_text,
      CASE
        WHEN dc.timesheet_id IS NOT NULL THEN 'timesheet:' || dc.timesheet_id::text
        WHEN dc.contract_week_id IS NOT NULL THEN 'contract_week:' || dc.contract_week_id::text
        ELSE ''
      END AS row_key,
      MD5(CONCAT_WS('|',
        COALESCE(dc.timesheet_id::text, ''),
        COALESCE(dc.contract_week_id::text, ''),
        COALESCE(dc.timesheet_version::text, ''),
        COALESCE(dc.timesheet_updated_at::text, ''),
        COALESCE(dc.contract_week_updated_at::text, ''),
        COALESCE(dc.tsfin_updated_at::text, ''),
        COALESCE(dc.validation_updated_at::text, ''),
        COALESCE(dc.evidence_updated_at::text, ''),
        COALESCE(dc.queue_updated_at::text, ''),
        COALESCE(dc.processing_status, ''),
        COALESCE(dc.summary_stage, ''),
        COALESCE(dc.tools_stage, ''),
        COALESCE(dc.authorised_at_server::text, ''),
        COALESCE(dc.authorised_at_utc::text, ''),
        COALESCE(dc.is_authorised::text, ''),
        COALESCE(dc.locked::text, ''),
        COALESCE(dc.locked_by_invoice_id::text, ''),
        COALESCE(dc.has_segment_invoice_lock::text, ''),
        COALESCE(dc.invoice_segments_locked::text, ''),
        COALESCE(dc.invoice_is_paid::text, ''),
        COALESCE(dc.paid_at_utc::text, ''),
        COALESCE(dc.route_family, ''),
        COALESCE(dc.route_subfamily, ''),
        COALESCE(dc.submission_mode, ''),
        COALESCE(dc.cw_submission_mode_snapshot, ''),
        COALESCE(dc.qr_status_upper, ''),
        COALESCE(dc.qr_generated_at::text, ''),
        COALESCE(dc.qr_scanned_at::text, ''),
        COALESCE(dc.qr_signed_at_utc::text, ''),
        COALESCE(dc.validation_status, ''),
        COALESCE(dc.validation_pre_validated::text, ''),
        COALESCE(dc.validation_reason_code, ''),
        COALESCE(dc.validation_validated_at_utc::text, ''),
        COALESCE((dc.hr_validation_required_for_invoice = TRUE AND dc.hr_validation_satisfied = FALSE)::text, ''),
        COALESCE(dc.issue_codes::text, ''),
        COALESCE(dc.attached_evidence_count::text, ''),
        COALESCE(dc.ev_timesheet::text, ''),
        COALESCE(dc.ev_mileage::text, ''),
        COALESCE(dc.ev_travel::text, ''),
        COALESCE(dc.ev_accommodation::text, ''),
        COALESCE(dc.ev_other::text, ''),
        COALESCE(dc.primary_evidence_id::text, ''),
        COALESCE(dc.primary_evidence_kind, ''),
        COALESCE(dc.queue_staged_count::text, ''),
        COALESCE(dc.staged_ev_timesheet::text, ''),
        COALESCE(dc.staged_ev_mileage::text, ''),
        COALESCE(dc.staged_ev_travel::text, ''),
        COALESCE(dc.staged_ev_accommodation::text, ''),
        COALESCE(dc.staged_ev_other::text, ''),
        COALESCE(dc.primary_queue_staged_kind, ''),
        COALESCE(dc.primary_evidence_storage_key, ''),
        COALESCE(dc.manual_pdf_r2_key, ''),
        COALESCE(dc.uploaded_pdf_r2_key, ''),
        COALESCE(dc.generated_pdf_at_utc::text, ''),
        COALESCE(dc.primary_queue_r2_key, ''),
        COALESCE(dc.client_requires_hr::text, ''),
        COALESCE(dc.client_no_timesheet_required::text, ''),
        COALESCE(dc.client_autoprocess_hr::text, ''),
        COALESCE(dc.client_is_nhsp::text, ''),
        COALESCE(dc.total_hours::text, ''),
        COALESCE(dc.total_pay_ex_vat::text, ''),
        COALESCE(dc.total_charge_ex_vat::text, ''),
        COALESCE(dc.margin_ex_vat::text, '')
      )) AS row_signature
    FROM decisions AS dc
  ),
  artifact_rows AS (
    SELECT
      fr.*,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' AND fr.primary_artifact_storage_key IS NOT NULL AND fr.timesheet_id IS NOT NULL THEN 'timesheet:' || fr.timesheet_id::text
        WHEN fr.primary_artifact_source = 'DOCUMENT' AND fr.primary_artifact_storage_key IS NOT NULL AND fr.contract_week_id IS NOT NULL THEN 'contract_week:' || fr.contract_week_id::text
        WHEN fr.primary_artifact_source = 'EVIDENCE' AND fr.primary_evidence_id IS NOT NULL THEN 'evidence:' || fr.primary_evidence_id::text
        WHEN fr.primary_artifact_source = 'QUEUE' AND fr.primary_queue_id IS NOT NULL THEN 'manual_queue:' || fr.primary_queue_id::text
        WHEN fr.primary_artifact_storage_key IS NOT NULL AND fr.timesheet_id IS NOT NULL THEN 'timesheet:' || fr.timesheet_id::text
        WHEN fr.primary_artifact_storage_key IS NOT NULL AND fr.contract_week_id IS NOT NULL THEN 'contract_week:' || fr.contract_week_id::text
        WHEN fr.primary_evidence_id IS NOT NULL THEN 'evidence:' || fr.primary_evidence_id::text
        WHEN fr.primary_queue_id IS NOT NULL THEN 'manual_queue:' || fr.primary_queue_id::text
        ELSE NULL::text
      END AS primary_artifact_id,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'TIMESHEET'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN UPPER(COALESCE(NULLIF(BTRIM(fr.primary_evidence_kind), ''), 'TIMESHEET'))
        WHEN fr.primary_artifact_source = 'QUEUE' THEN UPPER(COALESCE(NULLIF(BTRIM(fr.primary_queue_staged_kind), ''), 'TIMESHEET'))
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'TIMESHEET'
        WHEN fr.primary_evidence_kind IS NOT NULL THEN UPPER(fr.primary_evidence_kind)
        WHEN fr.primary_queue_id IS NOT NULL THEN UPPER(COALESCE(NULLIF(BTRIM(fr.primary_queue_staged_kind), ''), 'TIMESHEET'))
        ELSE NULL::text
      END AS primary_artifact_kind,
      CASE
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'Timesheet PDF'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN COALESCE(NULLIF(BTRIM(fr.primary_evidence_display_name), ''), 'Evidence')
        WHEN fr.primary_artifact_source = 'QUEUE' THEN COALESCE(NULLIF(BTRIM(fr.primary_queue_original_filename), ''), 'Staged evidence')
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'Timesheet PDF'
        WHEN fr.primary_evidence_display_name IS NOT NULL THEN fr.primary_evidence_display_name
        WHEN fr.primary_queue_original_filename IS NOT NULL THEN fr.primary_queue_original_filename
        ELSE NULL::text
      END AS primary_artifact_display_name,
      CASE
        WHEN fr.primary_left_pane_mode = 'IMPORT_SOURCE_ONLY' THEN 'IMPORT_TABLE'
        WHEN fr.primary_left_pane_mode = 'SIGNATURES_ONLY' THEN 'SIGNATURES'
        WHEN fr.primary_artifact_source = 'DOCUMENT' THEN 'PDF'
        WHEN fr.primary_artifact_source = 'EVIDENCE' THEN 'FILE'
        WHEN fr.primary_artifact_source = 'QUEUE' THEN 'PDF'
        WHEN fr.primary_artifact_storage_key IS NOT NULL THEN 'PDF'
        WHEN fr.primary_evidence_storage_key IS NOT NULL THEN 'FILE'
        ELSE NULL::text
      END AS primary_artifact_preview_mode,
      (
        COALESCE(fr.ev_timesheet, FALSE)
        OR COALESCE(fr.ev_mileage, FALSE)
        OR COALESCE(fr.ev_travel, FALSE)
        OR COALESCE(fr.ev_accommodation, FALSE)
        OR COALESCE(fr.ev_other, FALSE)
        OR COALESCE(fr.staged_ev_timesheet, FALSE)
        OR COALESCE(fr.staged_ev_mileage, FALSE)
        OR COALESCE(fr.staged_ev_travel, FALSE)
        OR COALESCE(fr.staged_ev_accommodation, FALSE)
        OR COALESCE(fr.staged_ev_other, FALSE)
        OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL
      ) AS has_any_evidence,
      JSONB_BUILD_ARRAY(
        JSONB_BUILD_OBJECT('kind', 'TIMESHEET', 'present', (COALESCE(fr.ev_timesheet, FALSE) OR COALESCE(fr.staged_ev_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL), 'has_evidence', (COALESCE(fr.ev_timesheet, FALSE) OR COALESCE(fr.staged_ev_timesheet, FALSE) OR NULLIF(BTRIM(COALESCE(fr.manual_pdf_r2_key, '')), '') IS NOT NULL OR NULLIF(BTRIM(COALESCE(fr.uploaded_pdf_r2_key, '')), '') IS NOT NULL)),
        JSONB_BUILD_OBJECT('kind', 'MILEAGE', 'present', (COALESCE(fr.ev_mileage, FALSE) OR COALESCE(fr.staged_ev_mileage, FALSE)), 'has_evidence', (COALESCE(fr.ev_mileage, FALSE) OR COALESCE(fr.staged_ev_mileage, FALSE))),
        JSONB_BUILD_OBJECT('kind', 'TRAVEL', 'present', (COALESCE(fr.ev_travel, FALSE) OR COALESCE(fr.staged_ev_travel, FALSE)), 'has_evidence', (COALESCE(fr.ev_travel, FALSE) OR COALESCE(fr.staged_ev_travel, FALSE))),
        JSONB_BUILD_OBJECT('kind', 'ACCOMMODATION', 'present', (COALESCE(fr.ev_accommodation, FALSE) OR COALESCE(fr.staged_ev_accommodation, FALSE)), 'has_evidence', (COALESCE(fr.ev_accommodation, FALSE) OR COALESCE(fr.staged_ev_accommodation, FALSE))),
        JSONB_BUILD_OBJECT('kind', 'OTHER', 'present', (COALESCE(fr.ev_other, FALSE) OR COALESCE(fr.staged_ev_other, FALSE)), 'has_evidence', (COALESCE(fr.ev_other, FALSE) OR COALESCE(fr.staged_ev_other, FALSE)))
      ) AS evidence_badges
    FROM final_rows AS fr
  )
  SELECT
    (
      JSONB_BUILD_OBJECT(
        'id', COALESCE(ar.timesheet_id::text, ar.contract_week_id::text),
        'row_key', ar.row_key,
        'stable_row_id', ar.stable_row_id_text,
        'timesheet_id', ar.timesheet_id,
        'current_timesheet_id', ar.timesheet_id,
        'requested_timesheet_id', ar.timesheet_id,
        'expected_timesheet_id', ar.timesheet_id,
        'contract_week_id', ar.contract_week_id,
        'contract_id', ar.contract_id,
        'booking_id', ar.booking_id,
        'timesheet_version', ar.timesheet_version,
        'row_signature', ar.row_signature,
        'updated_at', COALESCE(ar.timesheet_updated_at, ar.contract_week_updated_at, ar.tsfin_updated_at),
        'is_current', COALESCE(ar.timesheet_is_current, TRUE),
        'was_stale', FALSE,
        'candidate_id', ar.candidate_id,
        'candidate_name', ar.candidate_name,
        'candidate_display_name', ar.candidate_display_name,
        'candidate_first_name', ar.candidate_first_name,
        'candidate_surname', ar.candidate_surname,
        'occupant_key_norm', ar.occupant_key_norm,
        'client_id', ar.client_id,
        'client_name', ar.client_name,
        'client_display_name', ar.client_display_name,
        'hospital_name', ar.hospital_norm,
        'hospital_norm', ar.hospital_norm,
        'site_name', ar.contract_display_site,
        'ward_name', COALESCE(ar.contract_ward_hint, ar.contract_display_site),
        'ward_norm', COALESCE(ar.contract_ward_hint, ar.contract_display_site),
        'booking_ref', ar.booking_id,
        'external_ref', ar.reference_number
      )
      || JSONB_BUILD_OBJECT(
        'week_ending_date', ar.week_ending_date,
        'contract_week_ending_date', ar.contract_week_ending_date,
        'work_date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE NULL::date END,
        'date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE ar.week_ending_date END,
        'shift_date', CASE WHEN ar.period_type = 'DAILY' THEN (COALESCE(ar.timesheet_worked_start_iso, ar.timesheet_scheduled_start_iso, ar.worked_start_iso) AT TIME ZONE 'Europe/London')::date ELSE NULL::date END,
        'period_type', ar.period_type,
        'timesheet_type_sort_key', ar.timesheet_type_sort_key,
        'summary_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.summary_stage END,
        'tools_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.tools_stage END,
        'processing_status', ar.processing_status,
        'processing_status_display', CASE WHEN ar.is_unprocessed THEN 'Unprocessed' ELSE ar.processing_status_display END,
        'processed_at_utc', ar.processed_at_utc,
        'processed_by_user_id', ar.processed_by_user_id,
        'processed_by_display', ar.processed_by_display,
        'authorised_at_server', ar.authorised_at_server,
        'authorised_at_utc', ar.authorised_at_utc,
        'authorised_by_user_id', ar.authorised_by_user_id,
        'authorised_by_display', ar.authorised_by_display,
        'locked_by_invoice_id', ar.locked_by_invoice_id,
        'paid_at_utc', ar.paid_at_utc,
        'invoice_segments_total', ar.invoice_segments_total,
        'invoice_segments_locked', ar.invoice_segments_locked,
        'invoice_segments_unlocked', ar.invoice_segments_unlocked,
        'invoice_segment_stage', ar.invoice_segment_stage,
        'invoice_is_paid', ar.invoice_is_paid,
        'ready_to_pay', ar.ready_to_pay,
        'pay_icon_code', ar.pay_icon_code,
        'pay_status_code', ar.pay_status_code,
        'pay_paid_at_utc', ar.pay_paid_at_utc,
        'pay_on_hold', ar.pay_on_hold
      )
      || JSONB_BUILD_OBJECT(
        'total_hours', ar.total_hours,
        'total_pay_ex_vat', ar.total_pay_ex_vat,
        'total_charge_ex_vat', ar.total_charge_ex_vat,
        'margin_ex_vat', ar.margin_ex_vat,
        'net_delta_ex_vat', ar.net_delta_ex_vat,
        'route_type', ar.route_type,
        'route_display', ar.route_display,
        'submission_mode', ar.submission_mode,
        'submission_mode_snapshot', ar.cw_submission_mode_snapshot,
        'sheet_scope', ar.sheet_scope,
        'timesheet_scope', ar.sheet_scope,
        'route_family', ar.route_family,
        'route_subfamily', ar.route_subfamily,
        'underlying_channel_family', ar.underlying_channel_family,
        'is_adjustment', ar.is_adjustment_or_additional,
        'additional_seq', ar.additional_seq,
        'actual_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.timesheet_actual_schedule_json, ar.tsfin_actual_schedule_json, '[]'::jsonb) END,
        'planned_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.contract_week_planned_schedule_json, '[]'::jsonb) END,
        'contract_week_totals_json', COALESCE(ar.contract_week_totals_json, '{}'::jsonb),
        'suppress_standard_schedule_fallback', ar.keep_additional_manual_adjustment_schedule_empty,
        'keep_additional_manual_adjustment_schedule_empty', ar.keep_additional_manual_adjustment_schedule_empty,
        '__suppressStandardScheduleFallback', ar.keep_additional_manual_adjustment_schedule_empty,
        '__keepAdditionalManualAdjustmentScheduleEmpty', ar.keep_additional_manual_adjustment_schedule_empty,
        'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
        'compare_block_required', ar.compare_block_required,
        'bulk_process_bucket', ar.bulk_process_bucket,
        'bulk_authorise_classification', ar.bulk_authorise_classification,
        'bulk_authorise_section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
        'has_timesheet', ar.timesheet_id IS NOT NULL,
        'is_contract_week_only', ar.timesheet_id IS NULL AND ar.contract_week_id IS NOT NULL,
        'is_real_timesheet_row', ar.timesheet_id IS NOT NULL,
        'requires_authorisation', ar.requires_authorisation,
        'is_authorised', ar.is_authorised,
        'locked', ar.locked,
        'review_only', ar.review_only,
        'can_save', ar.can_save,
        'can_process', ar.can_process,
        'can_unprocess', ar.can_unprocess,
        'can_edit_timesheet_data', ar.can_edit_timesheet_data,
        'can_manage_evidence', ar.can_manage_evidence,
        'can_bulk_authorise', ar.can_bulk_authorise,
        'can_bulk_unauthorise', ar.can_bulk_unauthorise,
        'can_add_additional_manual', ar.can_add_additional_manual,
        'supportsUnprocessedExpenseDraft', ar.supports_unprocessed_expense_draft,
        'supports_unprocessed_expense_draft', ar.supports_unprocessed_expense_draft,
        'expense_storage_target', CASE
          WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_DRAFT'
          WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TSFIN'
          ELSE NULL::text
        END,
        'expense_evidence_storage_target', CASE
          WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_STAGED_EVIDENCE'
          WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TIMESHEET_EVIDENCE'
          ELSE NULL::text
        END
      )
      || JSONB_BUILD_OBJECT(
        'is_qr', ar.is_qr_route,
        'qr_status', NULLIF(ar.qr_status_upper, ''),
        'qr_generated_at', COALESCE(ar.qr_generated_at, ar.vb_qr_generated_at),
        'qr_scanned_at', COALESCE(ar.qr_scanned_at, ar.vb_qr_scanned_at),
        'qr_signed_at_utc', ar.qr_signed_at_utc,
        'qr_unsigned_blocked', ar.qr_unsigned_blocked,
        'qr_signed_returned', ar.qr_signed_returned,
        'can_allow_qr_again', ar.can_allow_qr_again,
        'can_allow_electronic_again', ar.can_allow_electronic_again,
        'can_switch_to_manual', ar.can_switch_to_manual,
        'can_switch_planned_week_to_electronic', ar.can_switch_planned_week_to_electronic,
        'can_revert_to_electronic', ar.can_revert_to_electronic,
        'can_convert_qr_to_manual_only', ar.can_convert_qr_to_manual_only,
        'can_restore_qr_pending', ar.can_restore_qr_pending,
        'can_restore_qr_signed', ar.can_restore_qr_signed,
        'qr_email_can_send_now', (ar.can_allow_qr_again OR NULLIF(ar.qr_status_upper, '') = 'PENDING') AND NULLIF(BTRIM(COALESCE(ar.candidate_email, '')), '') IS NOT NULL AND COALESCE(ar.candidate_opt_in_email, TRUE) = TRUE,
        'qr_email_recipient_available', NULLIF(BTRIM(COALESCE(ar.candidate_email, '')), '') IS NOT NULL,
        'client_requires_hr', ar.client_requires_hr,
        'client_autoprocess_hr', ar.client_autoprocess_hr,
        'client_no_timesheet_required', ar.client_no_timesheet_required,
        'client_is_nhsp', ar.client_is_nhsp,
        'hr_validation_required_for_invoice', ar.hr_validation_required_for_invoice,
        'validation_status', ar.validation_status,
        'validation_pre_validated', COALESCE(ar.validation_pre_validated, FALSE),
        'hr_validation_satisfied', ar.hr_validation_satisfied,
        'hr_validation_awaiting', ar.hr_validation_awaiting,
        'issue_codes', ar.issue_codes,
        'has_deviation_marker', (
          COALESCE(ar.has_rate_issue, FALSE)
          OR COALESCE(ar.has_pay_channel_issue, FALSE)
          OR NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL
          OR COALESCE(array_length(ar.hr_crosscheck_issues, 1), 0) > 0
          OR (
            NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
            AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED')
          )
        ),
        'deviation_marker_reason', CASE
          WHEN COALESCE(ar.has_rate_issue, FALSE) THEN 'RATE_ISSUE'
          WHEN COALESCE(ar.has_pay_channel_issue, FALSE) THEN 'PAY_CHANNEL_ISSUE'
          WHEN NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL THEN ar.validation_reason_code
          WHEN NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
           AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED') THEN ar.hr_crosscheck_status
          WHEN COALESCE(array_length(ar.hr_crosscheck_issues, 1), 0) > 0 THEN 'HR_CROSSCHECK_ISSUE'
          ELSE NULL::text
        END,
        'nhsp_highlight_red', (
          (ar.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(ar.client_is_nhsp, FALSE) = TRUE OR COALESCE(ar.contract_is_nhsp, FALSE) = TRUE)
          AND (
            COALESCE(ar.has_rate_issue, FALSE)
            OR COALESCE(ar.has_pay_channel_issue, FALSE)
            OR COALESCE(ar.nhsp_shift_deferred_count, 0) > 0
            OR NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL
            OR (
              NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
              AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED')
            )
          )
        ),
        'nhsp_highlight_reason', CASE
          WHEN NOT (ar.route_type_upper IN ('WEEKLY_NHSP', 'WEEKLY_NHSP_ADJUSTMENT') OR COALESCE(ar.client_is_nhsp, FALSE) = TRUE OR COALESCE(ar.contract_is_nhsp, FALSE) = TRUE) THEN NULL::text
          WHEN COALESCE(ar.nhsp_shift_deferred_count, 0) > 0 THEN 'DEFERRED_NHSP_SHIFTS'
          WHEN COALESCE(ar.has_rate_issue, FALSE) THEN 'RATE_ISSUE'
          WHEN COALESCE(ar.has_pay_channel_issue, FALSE) THEN 'PAY_CHANNEL_ISSUE'
          WHEN NULLIF(BTRIM(COALESCE(ar.validation_reason_code, '')), '') IS NOT NULL THEN ar.validation_reason_code
          WHEN NULLIF(BTRIM(COALESCE(ar.hr_crosscheck_status, '')), '') IS NOT NULL
           AND UPPER(COALESCE(ar.hr_crosscheck_status, '')) NOT IN ('OK', 'VALID', 'VALIDATION_OK', 'MATCHED', 'PASS', 'PASSED') THEN ar.hr_crosscheck_status
          ELSE NULL::text
        END,
        'nhsp_deviation_pct', CASE
          WHEN COALESCE(ar.nhsp_shift_count, 0) > 0 THEN ROUND((COALESCE(ar.nhsp_shift_deferred_count, 0)::numeric / NULLIF(ar.nhsp_shift_count::numeric, 0)) * 100, 2)
          ELSE NULL::numeric
        END,
        'nhsp_is_ad_hoc', ar.contract_is_ad_hoc
      )
      || JSONB_BUILD_OBJECT(
        'has_any_evidence', ar.has_any_evidence,
        'evidence_badges', ar.evidence_badges,
        'primary_artifact_id', ar.primary_artifact_id,
        'primary_artifact_kind', ar.primary_artifact_kind,
        'primary_artifact_display_name', ar.primary_artifact_display_name,
        'primary_artifact_storage_key', ar.primary_artifact_storage_key,
        'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
        'primary_artifact_source', ar.primary_artifact_source,
        'primary_left_pane_mode', ar.primary_left_pane_mode,
        'manual_pdf_r2_key', ar.manual_pdf_r2_key,
        'uploaded_pdf_r2_key', ar.uploaded_pdf_r2_key,
        'generated_pdf_at_utc', ar.generated_pdf_at_utc,
        'manual_pdf_rotation_degrees', ar.manual_pdf_rotation_degrees,
        'attached_evidence_count', COALESCE(ar.attached_evidence_count, 0),
        'queue_staged_count', COALESCE(ar.queue_staged_count, 0),
        'evidence_document_locked', ar.evidence_document_locked,
        'evidence_lock_reason', ar.evidence_lock_reason,
        'contract_additional_rates_json', ar.contract_additional_rates_json,
        'contract_std_schedule_json', ar.contract_std_schedule_json,
        'contract_mileage_pay_rate', ar.contract_mileage_pay_rate,
        'contract_mileage_charge_rate', ar.contract_mileage_charge_rate,
        'contract_weekly_mode', ar.contract_weekly_mode,
        'contract_hr_weekly_behaviour', ar.contract_hr_weekly_behaviour,
        'contract_role', ar.contract_role,
        'contract_band', ar.contract_band,
        'reference_number', ar.reference_number,
        'has_pay_adjustments', ar.has_pay_adjustments,
        'pay_adjustment_count', ar.pay_adjustment_count
      )
      || JSONB_BUILD_OBJECT(
        'action_flags', JSONB_BUILD_OBJECT(
          'can_restore_qr_pending', ar.can_restore_qr_pending,
          'can_restore_qr_signed', ar.can_restore_qr_signed,
          'can_revert_to_electronic', ar.can_revert_to_electronic,
          'can_allow_qr_again', ar.can_allow_qr_again,
          'can_allow_electronic_again', ar.can_allow_electronic_again,
          'can_switch_to_manual', ar.can_switch_to_manual,
          'can_switch_planned_week_to_electronic', ar.can_switch_planned_week_to_electronic,
          'can_convert_qr_to_manual_only', ar.can_convert_qr_to_manual_only,
          'supports_electronic_submission', ar.supports_electronic_submission,
          'is_manual_only', ar.is_manual_only,
          'is_adjustment', ar.is_adjustment_or_additional,
          'locked_by_invoice', ar.locked_by_invoice_id IS NOT NULL,
          'paid', ar.paid_at_utc IS NOT NULL,
          'is_unprocessed', ar.is_unprocessed,
          'route_family', ar.route_family,
          'underlying_channel_family', ar.underlying_channel_family,
          'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
          'can_save', ar.can_save,
          'can_process', ar.can_process,
          'can_edit_timesheet_data', ar.can_edit_timesheet_data,
          'can_manage_evidence', ar.can_manage_evidence,
          'supportsUnprocessedExpenseDraft', ar.supports_unprocessed_expense_draft,
          'supports_unprocessed_expense_draft', ar.supports_unprocessed_expense_draft,
          'expense_storage_target', CASE
            WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_DRAFT'
            WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TSFIN'
            ELSE NULL::text
          END,
          'expense_evidence_storage_target', CASE
            WHEN ar.supports_unprocessed_expense_draft THEN 'CONTRACT_WEEK_STAGED_EVIDENCE'
            WHEN ar.timesheet_id IS NOT NULL AND ar.route_family = 'MANUAL_NON_QR' THEN 'TIMESHEET_EVIDENCE'
            ELSE NULL::text
          END,
          'compare_block_required', ar.compare_block_required,
          'has_primary_artifact', ar.primary_artifact_storage_key IS NOT NULL,
          'primary_left_pane_mode', ar.primary_left_pane_mode,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'electronic_artifact_exists', ar.route_family = 'ELECTRONIC' AND ar.primary_artifact_storage_key IS NOT NULL,
          'healthroster_compare_required', ar.compare_block_required
        ),
        'bulk_authorise', JSONB_BUILD_OBJECT(
          'classification', ar.bulk_authorise_classification,
          'section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
          'can_authorise', ar.can_bulk_authorise,
          'can_unauthorise', ar.can_bulk_unauthorise,
          'requires_authorisation', ar.requires_authorisation,
          'is_authorised', ar.is_authorised,
          'hr_validation_awaiting', ar.hr_validation_awaiting,
          'qr_unsigned_blocked', ar.qr_unsigned_blocked,
          'qr_signed_returned', ar.qr_signed_returned
        ),
        'artifact_hints', JSONB_BUILD_OBJECT(
          'route_family', ar.route_family,
          'route_subfamily', ar.route_subfamily,
          'underlying_channel_family', ar.underlying_channel_family,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
          'has_any_evidence', ar.has_any_evidence,
          'evidence_badges', ar.evidence_badges
        ),
        'row_patch', JSONB_BUILD_OBJECT(
          'previous_row_key', NULL::text,
          'row_key', ar.row_key,
          'new_row_key', ar.row_key,
          'stable_row_id', ar.stable_row_id_text,
          'timesheet_id', ar.timesheet_id,
          'current_timesheet_id', ar.timesheet_id,
          'expected_timesheet_id', ar.timesheet_id,
          'contract_week_id', ar.contract_week_id,
          'is_adjustment', ar.is_adjustment_or_additional,
          'additional_seq', ar.additional_seq,
          'route_type', ar.route_type,
          'route_family', ar.route_family,
          'route_subfamily', ar.route_subfamily,
          'underlying_channel_family', ar.underlying_channel_family,
          'is_import_authoritative', ar.route_family = 'IMPORT_AUTHORITATIVE',
          'actual_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.timesheet_actual_schedule_json, ar.tsfin_actual_schedule_json, '[]'::jsonb) END,
          'planned_schedule_json', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN '[]'::jsonb ELSE COALESCE(ar.contract_week_planned_schedule_json, '[]'::jsonb) END,
          'total_hours', CASE WHEN ar.keep_additional_manual_adjustment_schedule_empty THEN 0::numeric ELSE ar.total_hours END,
          'suppress_standard_schedule_fallback', ar.keep_additional_manual_adjustment_schedule_empty,
          'keep_additional_manual_adjustment_schedule_empty', ar.keep_additional_manual_adjustment_schedule_empty,
          '__suppressStandardScheduleFallback', ar.keep_additional_manual_adjustment_schedule_empty,
          '__keepAdditionalManualAdjustmentScheduleEmpty', ar.keep_additional_manual_adjustment_schedule_empty,
          'row_signature', ar.row_signature,
          'previous_row_signature', NULL::text,
          'bulk_process_bucket', ar.bulk_process_bucket,
          'previous_bulk_process_bucket', NULL::text,
          'bulk_authorise_classification', ar.bulk_authorise_classification,
          'bulk_authorise_section', CASE WHEN ar.can_bulk_authorise THEN 'processed_eligible' WHEN ar.can_bulk_unauthorise THEN 'authorised_eligible' ELSE NULL::text END,
          'previous_bulk_authorise_section', NULL::text,
          'processing_status', ar.processing_status,
          'summary_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.summary_stage END,
          'tools_stage', CASE WHEN ar.is_unprocessed THEN 'UNPROCESSED' ELSE ar.tools_stage END,
          'is_authorised', ar.is_authorised,
          'locked', ar.locked,
          'can_process', ar.can_process,
          'can_unprocess', ar.can_unprocess,
          'can_bulk_authorise', ar.can_bulk_authorise,
          'can_bulk_unauthorise', ar.can_bulk_unauthorise,
          'primary_artifact_storage_key', ar.primary_artifact_storage_key,
          'previous_primary_artifact_storage_key', NULL::text,
          'primary_artifact_preview_mode', ar.primary_artifact_preview_mode,
          'evidence_badges', ar.evidence_badges,
          'cache_invalidation_hints', JSONB_BUILD_OBJECT(
            'row_keys', JSONB_BUILD_ARRAY(ar.row_key),
            'timesheet_ids', CASE WHEN ar.timesheet_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.timesheet_id) END,
            'contract_week_ids', CASE WHEN ar.contract_week_id IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.contract_week_id) END,
            'storage_keys', CASE WHEN ar.primary_artifact_storage_key IS NULL THEN '[]'::jsonb ELSE JSONB_BUILD_ARRAY(ar.primary_artifact_storage_key) END,
            'row_signature', ar.row_signature,
            'invalidate_context', TRUE,
            'invalidate_preview', FALSE
          )
        )
      )
    ) AS row_json
  FROM artifact_rows AS ar
  ORDER BY
    ar.week_ending_date ASC NULLS LAST,
    ar.client_name ASC NULLS LAST,
    ar.candidate_name ASC NULLS LAST,
    ar.row_key ASC;
END;
$function$;

