CREATE OR REPLACE FUNCTION public.contract_week_manual_draft_upsert_atomic_v1(
  p_week_id uuid,
  p_expected_row_signature text,
  p_totals_json jsonb,
  p_planned_schedule_json jsonb DEFAULT NULL::jsonb,
  p_replace_planned_schedule boolean DEFAULT false,
  p_force_adjustment boolean DEFAULT false,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_week public.contract_weeks%ROWTYPE;
  v_before_signature_json jsonb;
  v_after_signature_json jsonb;
  v_current_row_signature text;
  v_after_row_signature text;
  v_expected_row_signature text := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  v_status text;
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
BEGIN
  IF p_week_id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object('field', 'p_week_id')::text;
  END IF;

  IF v_expected_row_signature IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  IF p_totals_json IS NULL OR jsonb_typeof(p_totals_json) <> 'object' THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object('field', 'p_totals_json', 'reason', 'object_required')::text;
  END IF;

  IF COALESCE(p_replace_planned_schedule, false)
     AND (p_planned_schedule_json IS NULL OR jsonb_typeof(p_planned_schedule_json) <> 'array') THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object('field', 'p_planned_schedule_json', 'reason', 'array_required')::text;
  END IF;

  SELECT cw.*
    INTO v_week
  FROM public.contract_weeks AS cw
  WHERE cw.id = p_week_id
  FOR UPDATE;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_NOT_FOUND',
      DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  IF v_week.timesheet_id IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_MOVED',
      DETAIL = jsonb_build_object(
        'contract_week_id', v_week.id,
        'current_timesheet_id', v_week.timesheet_id
      )::text;
  END IF;

  v_status := UPPER(BTRIM(COALESCE(v_week.status::text, '')));
  IF v_status NOT IN ('PLANNED', 'OPEN') THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_DRAFT_NOT_EDITABLE',
      DETAIL = jsonb_build_object(
        'contract_week_id', v_week.id,
        'status', v_status
      )::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(
    NULL::uuid,
    v_week.id,
    false
  );
  v_current_row_signature := NULLIF(BTRIM(COALESCE(
    v_before_signature_json ->> 'backend_row_signature',
    v_before_signature_json ->> 'row_signature',
    v_before_signature_json ->> 'signature',
    ''
  )), '');

  IF v_current_row_signature IS NULL
     OR v_current_row_signature IS DISTINCT FROM v_expected_row_signature THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object(
        'contract_week_id', v_week.id,
        'expected_row_signature', v_expected_row_signature,
        'current_row_signature', v_current_row_signature
      )::text;
  END IF;

  UPDATE public.contract_weeks AS cw
  SET totals_json = p_totals_json,
      planned_schedule_json = CASE
        WHEN COALESCE(p_replace_planned_schedule, false) THEN p_planned_schedule_json
        ELSE cw.planned_schedule_json
      END,
      is_adjustment = CASE
        WHEN COALESCE(p_force_adjustment, false) THEN true
        ELSE cw.is_adjustment
      END,
      updated_at = v_now
  WHERE cw.id = v_week.id
    AND cw.timesheet_id IS NULL
  RETURNING cw.* INTO v_week;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_MOVED',
      DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(
    NULL::uuid,
    v_week.id,
    false
  );
  v_after_row_signature := NULLIF(BTRIM(COALESCE(
    v_after_signature_json ->> 'backend_row_signature',
    v_after_signature_json ->> 'row_signature',
    v_after_signature_json ->> 'signature',
    ''
  )), '');

  IF v_after_row_signature IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'PLANNED_CONTRACT_WEEK_SIGNATURE_MISSING',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id)::text;
  END IF;

  RETURN to_jsonb(v_week) || jsonb_build_object(
    'ok', true,
    'updated', true,
    'contract_week_id', v_week.id,
    'current_timesheet_id', NULL,
    'backend_row_signature', v_after_row_signature,
    'mutation_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'expected_row_signature', v_after_row_signature,
    'planned_contract_week_authority_complete', true,
    'planned_contract_week_authority_contract_week_id', v_week.id,
    'refresh_required', false,
    'affected_rows', jsonb_build_array(jsonb_build_object(
      'row_key', 'contract_week:' || v_week.id::text,
      'contract_week_id', v_week.id,
      'timesheet_id', NULL,
      'backend_row_signature', v_after_row_signature,
      'row_signature', v_after_row_signature,
      'planned_contract_week_authority_complete', true
    ))
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.contract_week_manual_draft_upsert_atomic_v1(
  uuid,
  text,
  jsonb,
  jsonb,
  boolean,
  boolean,
  timestamp with time zone
) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.contract_week_manual_draft_upsert_atomic_v1(
  uuid,
  text,
  jsonb,
  jsonb,
  boolean,
  boolean,
  timestamp with time zone
) FROM anon;
REVOKE ALL ON FUNCTION public.contract_week_manual_draft_upsert_atomic_v1(
  uuid,
  text,
  jsonb,
  jsonb,
  boolean,
  boolean,
  timestamp with time zone
) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.contract_week_manual_draft_upsert_atomic_v1(
  uuid,
  text,
  jsonb,
  jsonb,
  boolean,
  boolean,
  timestamp with time zone
) TO service_role;
