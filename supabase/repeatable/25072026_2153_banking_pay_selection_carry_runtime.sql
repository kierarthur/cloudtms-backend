-- Banking Pay selection carry is pre-draft intent only.
-- It never derives, changes or freezes financial amounts.

CREATE OR REPLACE FUNCTION public._pay_workbench_preview_selection_key_v1(
  p_candidate_id uuid,
  p_section text,
  p_timesheet_id uuid,
  p_key_type text,
  p_key_value text,
  p_row_key text,
  p_row_json jsonb
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
SECURITY INVOKER
SET search_path TO 'pg_catalog'
AS $function$
DECLARE
  v_row jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_row_json, '{}'::jsonb)) = 'object'
      THEN COALESCE(p_row_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_canonical_key text;
  v_economic_key text;
  v_key_type text := upper(btrim(COALESCE(p_key_type, '')));
  v_key_value text := btrim(COALESCE(p_key_value, ''));
BEGIN
  IF p_candidate_id IS NULL THEN
    RETURN NULL;
  END IF;

  v_canonical_key := NULLIF(btrim(COALESCE(
    v_row ->> 'canonical_correction_key',
    v_row #>> '{source_basis_json,canonical_correction_key}',
    v_row #>> '{frozen_source_basis_json,canonical_correction_key}',
    v_row #>> '{resolution_payload_json,canonical_correction_key}',
    v_row #>> '{saved_resolution_payload_json,canonical_correction_key}',
    v_row #>> '{frozen_resolution_payload_json,canonical_correction_key}',
    v_row #>> '{case_resolution_summary,canonical_correction_key}',
    ''
  )), '');

  IF v_canonical_key IS NOT NULL THEN
    RETURN concat_ws('|', 'CORRECTION', p_candidate_id::text, v_canonical_key);
  END IF;

  v_economic_key := NULLIF(btrim(COALESCE(
    v_row ->> 'economic_key',
    v_row ->> 'stable_economic_key',
    v_row #>> '{economic_key_json,canonical_key}',
    ''
  )), '');

  IF v_economic_key IS NOT NULL THEN
    RETURN concat_ws('|', 'ECONOMIC', p_candidate_id::text, v_economic_key);
  END IF;

  IF p_timesheet_id IS NOT NULL
     AND v_key_type <> ''
     AND v_key_value <> '' THEN
    RETURN concat_ws(
      '|',
      'TIMESHEET_COMPONENT',
      p_candidate_id::text,
      p_timesheet_id::text,
      v_key_type,
      v_key_value
    );
  END IF;

  IF p_timesheet_id IS NOT NULL
     AND NULLIF(btrim(COALESCE(p_row_key, '')), '') IS NOT NULL THEN
    RETURN concat_ws(
      '|',
      'TIMESHEET_ROW',
      p_candidate_id::text,
      p_timesheet_id::text,
      upper(btrim(COALESCE(p_section, ''))),
      btrim(p_row_key)
    );
  END IF;

  RETURN NULL;
END;
$function$;

ALTER FUNCTION public._pay_workbench_preview_selection_key_v1(
  uuid, text, uuid, text, text, text, jsonb
) OWNER TO postgres;

REVOKE ALL
ON FUNCTION public._pay_workbench_preview_selection_key_v1(
  uuid, text, uuid, text, text, text, jsonb
)
FROM PUBLIC, anon, authenticated, service_role;


CREATE OR REPLACE FUNCTION public.pay_workbench_session_carry_forward_preview_selections_v1(
  p_source_session_id uuid,
  p_target_session_id uuid,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_options jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_options_json, '{}'::jsonb)) = 'object'
      THEN COALESCE(p_options_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_source record;
  v_target record;
  v_actor_user_id uuid;
  v_allow_same_pay_date boolean :=
    lower(btrim(COALESCE(
      v_options ->> 'allow_same_pay_date_duplicate',
      v_options ->> 'allowSamePayDateDuplicate',
      'false'
    ))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_source_priority integer := CASE
    WHEN COALESCE(
      v_options ->> 'source_priority',
      v_options ->> 'sourcePriority',
      ''
    ) ~ '^-?[0-9]{1,7}$'
      THEN COALESCE(
        v_options ->> 'source_priority',
        v_options ->> 'sourcePriority'
      )::integer
    ELSE 0
  END;
  v_carry_reason text := left(upper(COALESCE(
    NULLIF(btrim(COALESCE(
      v_options ->> 'carry_forward_reason',
      v_options ->> 'carryForwardReason',
      ''
    )), ''),
    'SESSION_REPLACEMENT'
  )), 128);
  v_registered integer := 0;
  v_existing integer := 0;
  v_ambiguous integer := 0;
BEGIN
  IF p_source_session_id IS NULL
     OR p_target_session_id IS NULL
     OR p_source_session_id = p_target_session_id THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CARRY_SESSION_INVALID'
      USING ERRCODE = '22023';
  END IF;

  PERFORM 1
  FROM public.banking_pay_workbench_sessions session_lock
  WHERE session_lock.id IN (p_source_session_id, p_target_session_id)
  ORDER BY session_lock.id
  FOR UPDATE;

  SELECT * INTO v_source
  FROM public.banking_pay_workbench_sessions
  WHERE id = p_source_session_id;

  SELECT * INTO v_target
  FROM public.banking_pay_workbench_sessions
  WHERE id = p_target_session_id;

  IF v_source.id IS NULL OR v_target.id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CARRY_SESSION_NOT_FOUND'
      USING ERRCODE = 'P0002';
  END IF;

  IF v_target.status <> 'OPEN'
     OR v_target.discarded_at_utc IS NOT NULL
     OR v_target.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CARRY_TARGET_OBSOLETE'
      USING ERRCODE = '55000';
  END IF;

  IF v_source.week_ending_cutoff IS DISTINCT FROM v_target.week_ending_cutoff
     OR v_source.pay_date > v_target.pay_date
     OR (v_source.pay_date = v_target.pay_date AND v_allow_same_pay_date IS NOT TRUE) THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CARRY_SCOPE_MISMATCH'
      USING ERRCODE = '55000';
  END IF;

  IF NOT (
    (v_source.status = 'OPEN' AND v_source.discarded_at_utc IS NULL)
    OR
    (v_source.status = 'DISCARDED'
      AND v_source.discarded_at_utc IS NOT NULL
      AND v_source.replacement_session_id = p_target_session_id)
  ) THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CARRY_SOURCE_LIFECYCLE_INVALID'
      USING ERRCODE = '55000';
  END IF;

  v_actor_user_id := CASE
    WHEN COALESCE(
      v_options ->> 'actor_user_id',
      v_options ->> 'actorUserId',
      ''
    ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      THEN COALESCE(
        v_options ->> 'actor_user_id',
        v_options ->> 'actorUserId'
      )::uuid
    ELSE v_target.actor_user_id
  END;

  IF NOT EXISTS (
    SELECT 1
    FROM public.tms_users actor_row
    WHERE actor_row.id = v_actor_user_id
      AND actor_row.is_active = true
  ) THEN
    RAISE EXCEPTION 'WORKBENCH_SELECTION_CARRY_ACTOR_INVALID'
      USING ERRCODE = '42501';
  END IF;

  WITH source_rows AS (
    SELECT
      source_preview.id AS source_preview_row_id,
      source_preview.candidate_id,
      source_preview.selected,
      upper(btrim(source_preview.selection_state)) AS selection_state,
      source_preview.row_key,
      source_preview.section,
      source_preview.timesheet_id,
      source_preview.key_type,
      source_preview.key_value,
      source_preview.row_json,
      public._pay_workbench_preview_selection_key_v1(
        source_preview.candidate_id,
        source_preview.section,
        source_preview.timesheet_id,
        source_preview.key_type,
        source_preview.key_value,
        source_preview.row_key,
        source_preview.row_json
      ) AS stable_selection_key
    FROM public.banking_pay_workbench_preview_rows source_preview
    WHERE source_preview.session_id = p_source_session_id
      AND source_preview.session_version = v_source.version
      AND source_preview.status = 'READY'
      AND upper(btrim(COALESCE(source_preview.selection_state, '')))
        IN ('SELECTED', 'UNSELECTED')
  ),
  grouped_keys AS (
    SELECT
      stable_selection_key,
      candidate_id,
      count(*) AS row_count,
      count(DISTINCT selection_state) AS state_count
    FROM source_rows
    WHERE stable_selection_key IS NOT NULL
    GROUP BY stable_selection_key, candidate_id
  ),
  inserted AS (
    INSERT INTO public.banking_pay_workbench_selection_carry_registrations (
      target_session_id,
      source_session_id,
      candidate_id,
      source_preview_row_id,
      stable_selection_key,
      selected,
      selection_state,
      source_priority,
      carry_reason,
      status,
      state_reason_code,
      source_row_snapshot_json,
      created_at_utc,
      updated_at_utc,
      completed_at_utc
    )
    SELECT
      p_target_session_id,
      p_source_session_id,
      source_row.candidate_id,
      source_row.source_preview_row_id,
      source_row.stable_selection_key,
      source_row.selection_state = 'SELECTED',
      source_row.selection_state,
      v_source_priority,
      v_carry_reason,
      CASE WHEN grouped_key.state_count > 1 THEN 'AMBIGUOUS' ELSE 'PENDING' END,
      CASE WHEN grouped_key.state_count > 1 THEN 'SOURCE_SELECTION_STATES_CONFLICT' END,
      jsonb_strip_nulls(jsonb_build_object(
        'source_preview_row_id', source_row.source_preview_row_id,
        'source_session_id', p_source_session_id,
        'candidate_id', source_row.candidate_id,
        'stable_selection_key', source_row.stable_selection_key,
        'selected', source_row.selected,
        'selection_state', source_row.selection_state,
        'row_key', source_row.row_key,
        'section', source_row.section,
        'timesheet_id', source_row.timesheet_id,
        'key_type', source_row.key_type,
        'key_value', source_row.key_value,
        'policy_x_authority_scope', 'PRE_DRAFT_SELECTION_INTENT_ONLY'
      )),
      clock_timestamp(),
      clock_timestamp(),
      CASE WHEN grouped_key.state_count > 1 THEN clock_timestamp() END
    FROM source_rows source_row
    JOIN grouped_keys grouped_key
      ON grouped_key.stable_selection_key = source_row.stable_selection_key
     AND grouped_key.candidate_id = source_row.candidate_id
    ON CONFLICT (target_session_id, source_preview_row_id)
    DO NOTHING
    RETURNING status
  )
  SELECT
    count(*) FILTER (WHERE status = 'PENDING')::integer,
    count(*) FILTER (WHERE status = 'AMBIGUOUS')::integer
  INTO v_registered, v_ambiguous
  FROM inserted;

  SELECT count(*)::integer
  INTO v_existing
  FROM public.banking_pay_workbench_selection_carry_registrations
  WHERE target_session_id = p_target_session_id
    AND source_session_id = p_source_session_id;

  RETURN jsonb_build_object(
    'ok', true,
    'source_session_id', p_source_session_id,
    'target_session_id', p_target_session_id,
    'registered_count', COALESCE(v_registered, 0),
    'ambiguous_count', COALESCE(v_ambiguous, 0),
    'total_registered_or_existing_count', COALESCE(v_existing, 0),
    'policy_x_authority_scope', 'PRE_DRAFT_SELECTION_INTENT_ONLY'
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_session_carry_forward_preview_selections_v1(
  uuid, uuid, jsonb
) OWNER TO postgres;

REVOKE ALL
ON FUNCTION public.pay_workbench_session_carry_forward_preview_selections_v1(
  uuid, uuid, jsonb
)
FROM PUBLIC, anon, authenticated, service_role;


CREATE OR REPLACE FUNCTION public.trg_banking_pay_preview_selection_carry_apply()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_stable_selection_key text;
  v_registration record;
  v_selected_ids jsonb;
BEGIN
  IF pg_trigger_depth() > 1
     OR NEW.status <> 'READY'
     OR NEW.candidate_id IS NULL THEN
    RETURN NEW;
  END IF;

  v_stable_selection_key := public._pay_workbench_preview_selection_key_v1(
    NEW.candidate_id,
    NEW.section,
    NEW.timesheet_id,
    NEW.key_type,
    NEW.key_value,
    NEW.row_key,
    NEW.row_json
  );

  IF v_stable_selection_key IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT registration_row.*
  INTO v_registration
  FROM public.banking_pay_workbench_selection_carry_registrations registration_row
  WHERE registration_row.target_session_id = NEW.session_id
    AND registration_row.candidate_id = NEW.candidate_id
    AND registration_row.stable_selection_key = v_stable_selection_key
    AND registration_row.status = 'PENDING'
  ORDER BY
    registration_row.source_priority,
    registration_row.created_at_utc,
    registration_row.id
  LIMIT 1
  FOR UPDATE;

  IF v_registration.id IS NULL THEN
    RETURN NEW;
  END IF;

  UPDATE public.banking_pay_workbench_preview_rows target_preview
  SET selected = v_registration.selected,
      selection_state = v_registration.selection_state,
      row_json = jsonb_strip_nulls(
        COALESCE(target_preview.row_json, '{}'::jsonb)
        || jsonb_build_object(
          'selected', v_registration.selected,
          'selection_state', v_registration.selection_state,
          'selection_origin', 'SESSION_REPLACEMENT_CARRY',
          'selection_carry_registration_id', v_registration.id,
          'selection_carried_at_utc', clock_timestamp(),
          'policy_x_selection_authority_scope', 'PRE_DRAFT_SELECTION_INTENT_ONLY'
        )
      ),
      updated_at_utc = clock_timestamp()
  WHERE target_preview.id = NEW.id;

  UPDATE public.banking_pay_workbench_selection_carry_registrations applied
  SET status = 'APPLIED',
      target_preview_row_id = NEW.id,
      state_reason_code = 'MATCHED_STABLE_SELECTION_KEY',
      updated_at_utc = clock_timestamp(),
      completed_at_utc = clock_timestamp()
  WHERE applied.id = v_registration.id;

  UPDATE public.banking_pay_workbench_selection_carry_registrations superseded
  SET status = 'SUPERSEDED',
      state_reason_code = 'HIGHER_PRIORITY_SELECTION_CARRIED',
      updated_at_utc = clock_timestamp(),
      completed_at_utc = clock_timestamp()
  WHERE superseded.target_session_id = NEW.session_id
    AND superseded.candidate_id = NEW.candidate_id
    AND superseded.stable_selection_key = v_stable_selection_key
    AND superseded.status = 'PENDING'
    AND superseded.id <> v_registration.id;

  SELECT COALESCE(
    jsonb_agg(selected_preview.id::text ORDER BY selected_preview.row_ordinal, selected_preview.id),
    '[]'::jsonb
  )
  INTO v_selected_ids
  FROM public.banking_pay_workbench_preview_rows selected_preview
  WHERE selected_preview.session_id = NEW.session_id
    AND selected_preview.status = 'READY'
    AND selected_preview.selected IS TRUE
    AND selected_preview.selection_state = 'SELECTED';

  UPDATE public.banking_pay_workbench_sessions target_session
  SET server_selected_preview_row_ids = v_selected_ids,
      server_selected_preview_row_ids_provided = true,
      selected_row_count = jsonb_array_length(v_selected_ids),
      progress_json = COALESCE(target_session.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'selection_intent_v1',
          COALESCE(target_session.progress_json->'selection_intent_v1', '{}'::jsonb)
          || jsonb_build_object(
            'canonical_preview_lines',
            jsonb_build_object(
              'mode', 'EXPLICIT_INCLUDE',
              'server_selected_preview_row_ids_provided', true,
              'selected_preview_row_ids', v_selected_ids,
              'updated_at_utc', clock_timestamp(),
              'source', 'SESSION_REPLACEMENT_CARRY'
            )
          )
        ),
      updated_at_utc = clock_timestamp()
  WHERE target_session.id = NEW.session_id
    AND target_session.status = 'OPEN'
    AND target_session.discarded_at_utc IS NULL;

  RETURN NEW;
END;
$function$;

ALTER FUNCTION public.trg_banking_pay_preview_selection_carry_apply()
  OWNER TO postgres;

REVOKE ALL
ON FUNCTION public.trg_banking_pay_preview_selection_carry_apply()
FROM PUBLIC, anon, authenticated, service_role;
