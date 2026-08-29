-- Keep the established financially-clean weekly-chain deletion authority intact,
-- then reconcile only the parent Contract boundary after that authority succeeds.

DO $do$
BEGIN
  IF to_regprocedure('private.contract_week_delete_planned_base_v1(uuid,uuid)') IS NULL THEN
    IF to_regprocedure('public.contract_week_delete_planned(uuid,uuid)') IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'CONTRACT_WEEK_DELETE_PLANNED_AUTHORITY_MISSING';
    END IF;

    EXECUTE 'ALTER FUNCTION public.contract_week_delete_planned(uuid, uuid) SET SCHEMA private';
    EXECUTE 'ALTER FUNCTION private.contract_week_delete_planned(uuid, uuid) RENAME TO contract_week_delete_planned_base_v1';
  END IF;
END
$do$;

CREATE OR REPLACE FUNCTION public.contract_week_delete_planned(
  p_contract_week_id uuid,
  p_actor_user_id uuid
)
RETURNS TABLE (
  deleted boolean,
  contract_week_id uuid
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_deleted boolean;
  v_deleted_contract_week_id uuid;
  v_contract_id uuid;
  v_contract_start date;
  v_contract_end date;
  v_week_ending_weekday integer;
  v_first_remaining_week date;
  v_last_remaining_week date;
  v_start_week date;
  v_end_week date;
  v_first_remaining_planned_date date;
  v_last_remaining_planned_date date;
  v_new_start date;
  v_new_end date;
BEGIN
  SELECT cw.contract_id
  INTO v_contract_id
  FROM public.contract_weeks cw
  WHERE cw.id = p_contract_week_id;

  SELECT result.deleted, result.contract_week_id
  INTO v_deleted, v_deleted_contract_week_id
  FROM private.contract_week_delete_planned_base_v1(
    p_contract_week_id,
    p_actor_user_id
  ) AS result;

  IF v_deleted IS NOT TRUE OR v_contract_id IS NULL THEN
    deleted := v_deleted;
    contract_week_id := v_deleted_contract_week_id;
    RETURN NEXT;
    RETURN;
  END IF;

  SELECT c.start_date, c.end_date, COALESCE(c.week_ending_weekday_snapshot, 0)
  INTO v_contract_start, v_contract_end, v_week_ending_weekday
  FROM public.contracts c
  WHERE c.id = v_contract_id
  FOR UPDATE;

  IF FOUND THEN
    SELECT min(cw.week_ending_date), max(cw.week_ending_date)
    INTO v_first_remaining_week, v_last_remaining_week
    FROM public.contract_weeks cw
    WHERE cw.contract_id = v_contract_id;

    v_new_start := v_contract_start;
    v_new_end := v_contract_end;

    IF v_first_remaining_week IS NOT NULL THEN
      v_start_week := v_contract_start
        + mod(v_week_ending_weekday - extract(dow from v_contract_start)::integer + 7, 7);
      v_end_week := v_contract_end
        + mod(v_week_ending_weekday - extract(dow from v_contract_end)::integer + 7, 7);

      IF v_first_remaining_week > v_start_week THEN
        SELECT min((entry.item ->> 'date')::date)
        INTO v_first_remaining_planned_date
        FROM public.contract_weeks remaining
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(remaining.planned_schedule_json, '[]'::jsonb)) entry(item)
        WHERE remaining.contract_id = v_contract_id
          AND remaining.week_ending_date = v_first_remaining_week
          AND jsonb_typeof(entry.item) = 'object'
          AND COALESCE(entry.item ->> 'date', '') ~ '^\d{4}-\d{2}-\d{2}$';

        v_new_start := COALESCE(v_first_remaining_planned_date, v_first_remaining_week - 6);
      END IF;

      IF v_last_remaining_week < v_end_week THEN
        SELECT max((entry.item ->> 'date')::date)
        INTO v_last_remaining_planned_date
        FROM public.contract_weeks remaining
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(remaining.planned_schedule_json, '[]'::jsonb)) entry(item)
        WHERE remaining.contract_id = v_contract_id
          AND remaining.week_ending_date = v_last_remaining_week
          AND jsonb_typeof(entry.item) = 'object'
          AND COALESCE(entry.item ->> 'date', '') ~ '^\d{4}-\d{2}-\d{2}$';

        v_new_end := COALESCE(v_last_remaining_planned_date, v_last_remaining_week);
      END IF;

      IF v_new_start IS DISTINCT FROM v_contract_start
         OR v_new_end IS DISTINCT FROM v_contract_end THEN
        UPDATE public.contracts
        SET start_date = v_new_start,
            end_date = v_new_end
        WHERE id = v_contract_id;

        INSERT INTO public.audit_events(
          actor_user_id,
          object_type,
          object_id_text,
          action,
          before_json,
          after_json,
          reason
        )
        VALUES (
          p_actor_user_id,
          'contract',
          v_contract_id::text,
          'CONTRACT_DATES_RECONCILED_AFTER_WEEK_DELETE',
          jsonb_build_object('start_date', v_contract_start, 'end_date', v_contract_end),
          jsonb_build_object('start_date', v_new_start, 'end_date', v_new_end),
          'PLANNED_CONTRACT_WEEK_DELETED'
        );
      END IF;
    END IF;
  END IF;

  deleted := v_deleted;
  contract_week_id := v_deleted_contract_week_id;
  RETURN NEXT;
END;
$function$;

REVOKE ALL ON FUNCTION public.contract_week_delete_planned(uuid, uuid) FROM PUBLIC;

DO $do$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'anon') THEN
    EXECUTE 'REVOKE ALL ON FUNCTION public.contract_week_delete_planned(uuid, uuid) FROM anon';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'authenticated') THEN
    EXECUTE 'REVOKE ALL ON FUNCTION public.contract_week_delete_planned(uuid, uuid) FROM authenticated';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'service_role') THEN
    EXECUTE 'GRANT EXECUTE ON FUNCTION public.contract_week_delete_planned(uuid, uuid) TO service_role';
  END IF;
END
$do$;

DO $do$
BEGIN
  IF to_regprocedure('private.timesheet_weekly_chain_delete_apply_base_v1(uuid,uuid,uuid[],uuid[],uuid[],text)') IS NULL THEN
    IF to_regprocedure('public.timesheet_weekly_chain_delete_apply(uuid,uuid,uuid[],uuid[],uuid[],text)') IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_WEEKLY_CHAIN_DELETE_AUTHORITY_MISSING';
    END IF;

    EXECUTE 'ALTER FUNCTION public.timesheet_weekly_chain_delete_apply(uuid, uuid, uuid[], uuid[], uuid[], text) SET SCHEMA private';
    EXECUTE 'ALTER FUNCTION private.timesheet_weekly_chain_delete_apply(uuid, uuid, uuid[], uuid[], uuid[], text) RENAME TO timesheet_weekly_chain_delete_apply_base_v1';
  END IF;
END
$do$;

CREATE OR REPLACE FUNCTION public.timesheet_weekly_chain_delete_apply(
  p_timesheet_id uuid,
  p_actor_user_id uuid,
  p_expected_timesheet_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_contract_week_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_nhsp_shift_ids uuid[] DEFAULT NULL::uuid[],
  p_expected_row_signature text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_preview jsonb;
  v_result jsonb;
  v_contract_id uuid;
  v_contract_start date;
  v_contract_end date;
  v_week_ending_weekday integer;
  v_first_remaining_week date;
  v_last_remaining_week date;
  v_start_week date;
  v_end_week date;
  v_first_remaining_planned_date date;
  v_last_remaining_planned_date date;
  v_new_start date;
  v_new_end date;
BEGIN
  v_preview := public.timesheet_weekly_chain_delete_preview(p_timesheet_id, p_actor_user_id);
  v_contract_id := NULLIF(v_preview ->> 'contract_id', '')::uuid;

  v_result := private.timesheet_weekly_chain_delete_apply_base_v1(
    p_timesheet_id,
    p_actor_user_id,
    p_expected_timesheet_ids,
    p_expected_contract_week_ids,
    p_expected_nhsp_shift_ids,
    p_expected_row_signature
  );

  IF COALESCE((v_result ->> 'ok')::boolean, false) IS NOT TRUE
     OR COALESCE((v_result ->> 'apply_performed')::boolean, false) IS NOT TRUE THEN
    RETURN v_result;
  END IF;

  IF v_contract_id IS NULL THEN
    RETURN v_result;
  END IF;

  SELECT c.start_date, c.end_date, COALESCE(c.week_ending_weekday_snapshot, 0)
  INTO v_contract_start, v_contract_end, v_week_ending_weekday
  FROM public.contracts c
  WHERE c.id = v_contract_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN v_result;
  END IF;

  SELECT min(cw.week_ending_date), max(cw.week_ending_date)
  INTO v_first_remaining_week, v_last_remaining_week
  FROM public.contract_weeks cw
  WHERE cw.contract_id = v_contract_id;

  v_new_start := v_contract_start;
  v_new_end := v_contract_end;

  IF v_first_remaining_week IS NOT NULL THEN
    v_start_week := v_contract_start
      + mod(v_week_ending_weekday - extract(dow from v_contract_start)::integer + 7, 7);
    v_end_week := v_contract_end
      + mod(v_week_ending_weekday - extract(dow from v_contract_end)::integer + 7, 7);

    IF v_first_remaining_week > v_start_week THEN
      SELECT min((entry.item ->> 'date')::date)
      INTO v_first_remaining_planned_date
      FROM public.contract_weeks remaining
      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(remaining.planned_schedule_json, '[]'::jsonb)) entry(item)
      WHERE remaining.contract_id = v_contract_id
        AND remaining.week_ending_date = v_first_remaining_week
        AND jsonb_typeof(entry.item) = 'object'
        AND COALESCE(entry.item ->> 'date', '') ~ '^\d{4}-\d{2}-\d{2}$';

      v_new_start := COALESCE(v_first_remaining_planned_date, v_first_remaining_week - 6);
    END IF;

    IF v_last_remaining_week < v_end_week THEN
      SELECT max((entry.item ->> 'date')::date)
      INTO v_last_remaining_planned_date
      FROM public.contract_weeks remaining
      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(remaining.planned_schedule_json, '[]'::jsonb)) entry(item)
      WHERE remaining.contract_id = v_contract_id
        AND remaining.week_ending_date = v_last_remaining_week
        AND jsonb_typeof(entry.item) = 'object'
        AND COALESCE(entry.item ->> 'date', '') ~ '^\d{4}-\d{2}-\d{2}$';

      v_new_end := COALESCE(v_last_remaining_planned_date, v_last_remaining_week);
    END IF;

    IF v_new_start IS DISTINCT FROM v_contract_start
       OR v_new_end IS DISTINCT FROM v_contract_end THEN
      UPDATE public.contracts
      SET start_date = v_new_start,
          end_date = v_new_end
      WHERE id = v_contract_id;

      INSERT INTO public.audit_events(
        actor_user_id,
        object_type,
        object_id_text,
        action,
        before_json,
        after_json,
        reason
      )
      VALUES (
        p_actor_user_id,
        'contract',
        v_contract_id::text,
        'CONTRACT_DATES_RECONCILED_AFTER_WEEK_DELETE',
        jsonb_build_object('start_date', v_contract_start, 'end_date', v_contract_end),
        jsonb_build_object('start_date', v_new_start, 'end_date', v_new_end),
        'WEEKLY_TIMESHEET_CHAIN_DELETED'
      );
    END IF;
  END IF;

  RETURN v_result || jsonb_build_object(
    'contract_dates', jsonb_build_object(
      'start_date', v_new_start,
      'end_date', v_new_end,
      'changed', v_new_start IS DISTINCT FROM v_contract_start
        OR v_new_end IS DISTINCT FROM v_contract_end
    )
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.timesheet_weekly_chain_delete_apply(uuid, uuid, uuid[], uuid[], uuid[], text) FROM PUBLIC;

DO $do$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'anon') THEN
    EXECUTE 'REVOKE ALL ON FUNCTION public.timesheet_weekly_chain_delete_apply(uuid, uuid, uuid[], uuid[], uuid[], text) FROM anon';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'authenticated') THEN
    EXECUTE 'REVOKE ALL ON FUNCTION public.timesheet_weekly_chain_delete_apply(uuid, uuid, uuid[], uuid[], uuid[], text) FROM authenticated';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'service_role') THEN
    EXECUTE 'GRANT EXECUTE ON FUNCTION public.timesheet_weekly_chain_delete_apply(uuid, uuid, uuid[], uuid[], uuid[], text) TO service_role';
  END IF;
END
$do$;
