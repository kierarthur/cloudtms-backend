-- Current single-function owner for the Banking Pay Workbench session-open wrapper.
-- The function body is intentionally unchanged; this repeatable prevents a historical
-- multi-function monolith or an ACL-only fence from being treated as its current owner.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_open(p_actor_user_id uuid, p_pay_date date, p_week_ending_cutoff date, p_filters_json jsonb, p_session_signature text, p_force_new_session boolean DEFAULT false, p_discard_source_session boolean DEFAULT false, p_source_session_id uuid DEFAULT NULL::uuid, p_obsolete_session_ids jsonb DEFAULT '[]'::jsonb, p_mutation_context text DEFAULT NULL::text, p_created_pay_batch_ids jsonb DEFAULT '[]'::jsonb, p_dirty_candidate_ids jsonb DEFAULT '[]'::jsonb, p_refresh_job_ids jsonb DEFAULT '[]'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
-- Exact current-owner marker only; established session-open decisions are unchanged.
DECLARE
  v_now timestamptz := now();
  v_filters_json jsonb := CASE WHEN jsonb_typeof(COALESCE(p_filters_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_filters_json, '{}'::jsonb) ELSE '{}'::jsonb END;
  v_input_session_signature text := NULLIF(BTRIM(COALESCE(p_session_signature, '')), '');
  v_session_signature text := NULL::text;
  v_canonical_session_signature text := NULL::text;
  v_sanitised_filters_json jsonb := '{}'::jsonb;
  v_filter_strip_keys text[] := ARRAY[
    'preview_context_summary',
    'paye_guardrails',
    'preview_decisions_json',
    'preview_decisions',
    'open_options',
    'options',
    'progress_json',
    'candidate_sample_rows_json',
    'scope_next_cursor_json',
    'pay_date',
    'payDate',
    'pay_date_text',
    'payDateText',
    'source_pay_date',
    'sourcePayDate',
    'target_pay_date',
    'targetPayDate',
    'pay_week',
    'payWeek',
    'pay_week_label',
    'payWeekLabel',
    'pay_week_start',
    'payWeekStart',
    'pay_week_end',
    'payWeekEnd',
    'pay_period_start',
    'payPeriodStart',
    'pay_period_end',
    'payPeriodEnd',
    'week_start',
    'weekStart',
    'week_end',
    'weekEnd',
    'week_ending_cutoff',
    'weekEndingCutoff',
    'source_snapshot_run_id',
    'snapshot_run_id',
    'source_session_id',
    'sourceSessionId',
    'target_session_id',
    'targetSessionId',
    'session_id',
    'sessionId',
    'session_version',
    'sessionVersion',
    'session_signature',
    'sessionSignature',
    'clone_from_session_id',
    'cloneFromSessionId',
    'existing_paye_draft',
    'eligibility',
    'today_uk',
    'worker_metadata',
    'job_metadata',
    'source_build',
    'clone_rebase',
    'clone_rebase_result',
    'cursor',
    'cursor_json',
    'page_cursor',
    'candidate_cursor',
    'last_cursor',
    'active_jobs',
    'pending_job_ids_json',
    'replacement_session_id',
    'replacement_idempotency_key'
  ]::text[];
  v_signature_nested_filters jsonb := '{}'::jsonb;
  v_signature_nested_filter jsonb := '{}'::jsonb;
  v_signature_candidate_filter_id text := NULL::text;
  v_signature_client_filter_id text := NULL::text;
  v_signature_candidate_ids jsonb := '[]'::jsonb;
  v_effective_week_ending_cutoff date := COALESCE(p_week_ending_cutoff, DATE '9999-12-31');
  v_filter_candidate_id uuid := NULL::uuid;
  v_filter_client_id uuid := NULL::uuid;
  v_snapshot_info_json jsonb := '{}'::jsonb;
  v_snapshot_run_id uuid := NULL::uuid;
  v_summary_context_json jsonb := '{}'::jsonb;
  v_existing_session_id uuid := NULL::uuid;
  v_session_id uuid := NULL::uuid;
  v_session_version bigint := 1;
  v_action text := 'WORKBENCH_SESSION_OPENED';
  v_force_new_session boolean := COALESCE(p_force_new_session, false) OR COALESCE(p_discard_source_session, false);
  v_mutation_context text := NULLIF(BTRIM(COALESCE(p_mutation_context, '')), '');
  v_post_mutation_requires_refresh boolean := false;
  v_seed_result jsonb := '{}'::jsonb;
  v_scope_first_page_count integer := 0;
  v_scope_has_more boolean := false;
  v_scope_next_cursor jsonb := NULL::jsonb;
  v_scope_seed_complete boolean := false;
  v_scope_seeded_count integer := 0;
  v_scope_pending_count integer := 0;
  v_scope_ready_count integer := 0;
  v_scope_failed_count integer := 0;
  v_ready_flag boolean := false;
  v_ready_empty boolean := false;
  v_actor_is_active boolean := false;
  v_open_options_json jsonb := '{}'::jsonb;
  v_allow_session_rebase boolean := false;
  v_force_discarded_session_count integer := 0;
  v_force_discarded_session_ids jsonb := '[]'::jsonb;
  v_force_discarded_session_link_count integer := 0;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('BOOTSTRAP');

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SESSION_OPEN_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_WORKBENCH_SESSION_OPEN_ACTOR_REQUIRED')::text;
  END IF;

  IF p_pay_date IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SESSION_OPEN_PAY_DATE_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_WORKBENCH_SESSION_OPEN_PAY_DATE_REQUIRED')::text;
  END IF;

  v_sanitised_filters_json := v_filters_json - v_filter_strip_keys;

  IF jsonb_typeof(v_sanitised_filters_json->'filters') = 'object' THEN
    v_sanitised_filters_json := jsonb_set(
      v_sanitised_filters_json,
      '{filters}',
      COALESCE(v_sanitised_filters_json->'filters', '{}'::jsonb) - v_filter_strip_keys,
      true
    );
  END IF;

  IF jsonb_typeof(v_sanitised_filters_json->'filter') = 'object' THEN
    v_sanitised_filters_json := jsonb_set(
      v_sanitised_filters_json,
      '{filter}',
      COALESCE(v_sanitised_filters_json->'filter', '{}'::jsonb) - v_filter_strip_keys,
      true
    );
  END IF;

  v_sanitised_filters_json := jsonb_strip_nulls(v_sanitised_filters_json);
  v_signature_nested_filters := CASE
    WHEN jsonb_typeof(v_sanitised_filters_json->'filters') = 'object'
      THEN COALESCE(v_sanitised_filters_json->'filters', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_signature_nested_filter := CASE
    WHEN jsonb_typeof(v_sanitised_filters_json->'filter') = 'object'
      THEN COALESCE(v_sanitised_filters_json->'filter', '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  v_signature_candidate_filter_id := NULLIF(BTRIM(COALESCE(
    v_sanitised_filters_json->>'candidate_filter_id',
    v_sanitised_filters_json->>'candidateFilterId',
    v_sanitised_filters_json->>'filter_candidate_id',
    v_signature_nested_filters->>'candidate_filter_id',
    v_signature_nested_filters->>'candidateFilterId',
    v_signature_nested_filters->>'filter_candidate_id',
    v_signature_nested_filter->>'candidate_filter_id',
    v_signature_nested_filter->>'candidateFilterId',
    v_signature_nested_filter->>'filter_candidate_id',
    ''
  )), '');

  v_signature_client_filter_id := NULLIF(BTRIM(COALESCE(
    v_sanitised_filters_json->>'client_filter_id',
    v_sanitised_filters_json->>'clientFilterId',
    v_sanitised_filters_json->>'filter_client_id',
    v_sanitised_filters_json->>'client_id',
    v_sanitised_filters_json->>'clientId',
    v_signature_nested_filters->>'client_filter_id',
    v_signature_nested_filters->>'clientFilterId',
    v_signature_nested_filters->>'filter_client_id',
    v_signature_nested_filters->>'client_id',
    v_signature_nested_filters->>'clientId',
    v_signature_nested_filter->>'client_filter_id',
    v_signature_nested_filter->>'clientFilterId',
    v_signature_nested_filter->>'filter_client_id',
    v_signature_nested_filter->>'client_id',
    v_signature_nested_filter->>'clientId',
    ''
  )), '');

  WITH raw_candidate_ids(candidate_id_text) AS (
    SELECT NULLIF(BTRIM(v_sanitised_filters_json->>'candidate_id'), '')
    UNION ALL SELECT NULLIF(BTRIM(v_sanitised_filters_json->>'candidateId'), '')
    UNION ALL SELECT NULLIF(BTRIM(v_signature_nested_filters->>'candidate_id'), '')
    UNION ALL SELECT NULLIF(BTRIM(v_signature_nested_filters->>'candidateId'), '')
    UNION ALL SELECT NULLIF(BTRIM(v_signature_nested_filter->>'candidate_id'), '')
    UNION ALL SELECT NULLIF(BTRIM(v_signature_nested_filter->>'candidateId'), '')
    UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
    FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_sanitised_filters_json->'candidate_ids') = 'array' THEN v_sanitised_filters_json->'candidate_ids' ELSE '[]'::jsonb END) AS candidate_id_value(value)
    UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
    FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_sanitised_filters_json->'candidateIds') = 'array' THEN v_sanitised_filters_json->'candidateIds' ELSE '[]'::jsonb END) AS candidate_id_value(value)
    UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
    FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_signature_nested_filters->'candidate_ids') = 'array' THEN v_signature_nested_filters->'candidate_ids' ELSE '[]'::jsonb END) AS candidate_id_value(value)
    UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
    FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_signature_nested_filters->'candidateIds') = 'array' THEN v_signature_nested_filters->'candidateIds' ELSE '[]'::jsonb END) AS candidate_id_value(value)
    UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
    FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_signature_nested_filter->'candidate_ids') = 'array' THEN v_signature_nested_filter->'candidate_ids' ELSE '[]'::jsonb END) AS candidate_id_value(value)
    UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
    FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_signature_nested_filter->'candidateIds') = 'array' THEN v_signature_nested_filter->'candidateIds' ELSE '[]'::jsonb END) AS candidate_id_value(value)
  ), valid_candidate_ids AS (
    SELECT DISTINCT LOWER(raw_candidate_ids.candidate_id_text) AS candidate_id_text
    FROM raw_candidate_ids
    WHERE raw_candidate_ids.candidate_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  )
  SELECT COALESCE(jsonb_agg(valid_candidate_ids.candidate_id_text ORDER BY valid_candidate_ids.candidate_id_text), '[]'::jsonb)
  INTO v_signature_candidate_ids
  FROM valid_candidate_ids;

  v_canonical_session_signature := jsonb_build_object(
    'actor_user_id', p_actor_user_id::text,
    'candidate_filter_id', COALESCE(v_signature_candidate_filter_id, ''),
    'candidate_ids', COALESCE(v_signature_candidate_ids, '[]'::jsonb),
    'client_filter_id', COALESCE(v_signature_client_filter_id, ''),
    'kind', 'BANKING_PAY_WORKBENCH',
    'pay_date', p_pay_date::text,
    'signature_version', 4,
    'week_ending_cutoff', v_effective_week_ending_cutoff::text
  )::text;
  v_session_signature := NULLIF(BTRIM(COALESCE(v_canonical_session_signature, '')), '');

  IF v_session_signature IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SESSION_OPEN_SIGNATURE_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_SESSION_OPEN_SIGNATURE_REQUIRED',
              'input_session_signature_present', v_input_session_signature IS NOT NULL
            )::text;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.tms_users AS actor_user
    WHERE actor_user.id = p_actor_user_id
      AND COALESCE(actor_user.is_active, false) = true
  )
  INTO v_actor_is_active;

  IF COALESCE(v_actor_is_active, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SESSION_OPEN_ACTOR_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_WORKBENCH_SESSION_OPEN_ACTOR_NOT_ALLOWED', 'actor_user_id', p_actor_user_id::text)::text;
  END IF;

  v_open_options_json := COALESCE(v_filters_json, '{}'::jsonb)
    || CASE
      WHEN jsonb_typeof(v_filters_json->'options') = 'object' THEN COALESCE(v_filters_json->'options', '{}'::jsonb)
      WHEN jsonb_typeof(v_filters_json->'open_options') = 'object' THEN COALESCE(v_filters_json->'open_options', '{}'::jsonb)
      ELSE '{}'::jsonb
    END;
  v_allow_session_rebase := LOWER(BTRIM(COALESCE(
    v_open_options_json->>'allow_session_rebase',
    v_open_options_json->>'allowSessionRebase',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF COALESCE(p_force_new_session, false) IS NOT TRUE
     AND COALESCE(p_discard_source_session, false) IS NOT TRUE
     AND v_mutation_context IS NULL
     AND to_regprocedure('public.pay_workbench_session_open_shared_v2(uuid,date,date,jsonb,text)') IS NOT NULL THEN
    RETURN public.pay_workbench_session_open_shared_v2(
      p_actor_user_id => p_actor_user_id,
      p_pay_date => p_pay_date,
      p_week_ending_cutoff => p_week_ending_cutoff,
      p_filters_json => v_filters_json,
      p_session_signature => v_session_signature
    ) || jsonb_build_object(
      'open_wrapper_passed_clone_rebase_options', COALESCE(v_allow_session_rebase, false),
      'open_wrapper_function', 'pay_workbench_session_open'
    );
  END IF;

  IF BTRIM(COALESCE(v_filters_json->>'candidate_id', v_filters_json->>'candidateId', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_filter_candidate_id := COALESCE(v_filters_json->>'candidate_id', v_filters_json->>'candidateId')::uuid;
  END IF;

  IF BTRIM(COALESCE(v_filters_json->>'client_id', v_filters_json->>'clientId', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_filter_client_id := COALESCE(v_filters_json->>'client_id', v_filters_json->>'clientId')::uuid;
  END IF;

  v_post_mutation_requires_refresh := (
    v_mutation_context IS NOT NULL
    OR (jsonb_typeof(COALESCE(p_created_pay_batch_ids, '[]'::jsonb)) = 'array' AND jsonb_array_length(COALESCE(p_created_pay_batch_ids, '[]'::jsonb)) > 0)
    OR (jsonb_typeof(COALESCE(p_dirty_candidate_ids, '[]'::jsonb)) = 'array' AND jsonb_array_length(COALESCE(p_dirty_candidate_ids, '[]'::jsonb)) > 0)
    OR (jsonb_typeof(COALESCE(p_refresh_job_ids, '[]'::jsonb)) = 'array' AND jsonb_array_length(COALESCE(p_refresh_job_ids, '[]'::jsonb)) > 0)
  );

  IF v_post_mutation_requires_refresh THEN
    v_force_new_session := true;
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('public.pay_workbench_session_open:' || p_actor_user_id::text || ':' || v_session_signature));

  v_snapshot_info_json := public.pay_workbench_snapshot_ensure_run(
    p_pay_date => p_pay_date,
    p_week_ending_cutoff => v_effective_week_ending_cutoff,
    p_actor_user_id => p_actor_user_id
  );

  IF BTRIM(COALESCE(v_snapshot_info_json->>'snapshot_run_id', '')) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'pay_workbench_snapshot_ensure_run did not return a valid snapshot_run_id';
  END IF;

  v_snapshot_run_id := (v_snapshot_info_json->>'snapshot_run_id')::uuid;

  v_summary_context_json := public.pay_preview_build_context(
    p_pay_date => p_pay_date,
    p_week_ending_cutoff => v_effective_week_ending_cutoff,
    p_actor_user_id => p_actor_user_id,
    p_candidate_id => v_filter_candidate_id,
    p_client_id => v_filter_client_id,
    p_preview_decisions_json => v_sanitised_filters_json || jsonb_build_object('preview_context_mode', 'SUMMARY', 'scope_limit', 100)
  );

  IF v_force_new_session IS TRUE THEN
    WITH force_discarded_sessions AS (
      UPDATE public.banking_pay_workbench_sessions AS existing_session
      SET status = 'DISCARDED',
          discarded_at_utc = COALESCE(existing_session.discarded_at_utc, v_now),
          progress_state = 'DISCARDED',
          progress_json = jsonb_strip_nulls(
            COALESCE(existing_session.progress_json, '{}'::jsonb)
            || jsonb_build_object(
              'discarded_reason', COALESCE(v_mutation_context, 'FORCE_NEW_SESSION'),
              'discarded_by_function', 'pay_workbench_session_open',
              'discarded_at_utc', v_now::text,
              'canonical_scope_signature', v_session_signature
            )
          ),
          progress_counter_version = COALESCE(existing_session.progress_counter_version, 0) + 1,
          progress_updated_at_utc = v_now,
          updated_at_utc = v_now
      WHERE existing_session.actor_user_id = p_actor_user_id
        AND existing_session.status = 'OPEN'
        AND existing_session.discarded_at_utc IS NULL
        AND (
          (
            existing_session.pay_date = p_pay_date
            AND existing_session.week_ending_cutoff = v_effective_week_ending_cutoff
            AND EXISTS (
          SELECT 1
          FROM LATERAL (
            SELECT COALESCE(existing_session.filters_json, '{}'::jsonb) AS filtered_json
          ) AS session_filter
          CROSS JOIN LATERAL (
            SELECT
              CASE WHEN jsonb_typeof(session_filter.filtered_json->'filters') = 'object' THEN COALESCE(session_filter.filtered_json->'filters', '{}'::jsonb) ELSE '{}'::jsonb END AS nested_filters_json,
              CASE WHEN jsonb_typeof(session_filter.filtered_json->'filter') = 'object' THEN COALESCE(session_filter.filtered_json->'filter', '{}'::jsonb) ELSE '{}'::jsonb END AS nested_filter_json
          ) AS session_nested
          CROSS JOIN LATERAL (
            WITH raw_candidate_ids(candidate_id_text) AS (
              SELECT NULLIF(BTRIM(session_filter.filtered_json->>'candidate_id'), '')
              UNION ALL SELECT NULLIF(BTRIM(session_filter.filtered_json->>'candidateId'), '')
              UNION ALL SELECT NULLIF(BTRIM(session_nested.nested_filters_json->>'candidate_id'), '')
              UNION ALL SELECT NULLIF(BTRIM(session_nested.nested_filters_json->>'candidateId'), '')
              UNION ALL SELECT NULLIF(BTRIM(session_nested.nested_filter_json->>'candidate_id'), '')
              UNION ALL SELECT NULLIF(BTRIM(session_nested.nested_filter_json->>'candidateId'), '')
              UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
              FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_filter.filtered_json->'candidate_ids') = 'array' THEN session_filter.filtered_json->'candidate_ids' ELSE '[]'::jsonb END) AS candidate_id_value(value)
              UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
              FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_filter.filtered_json->'candidateIds') = 'array' THEN session_filter.filtered_json->'candidateIds' ELSE '[]'::jsonb END) AS candidate_id_value(value)
              UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
              FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_nested.nested_filters_json->'candidate_ids') = 'array' THEN session_nested.nested_filters_json->'candidate_ids' ELSE '[]'::jsonb END) AS candidate_id_value(value)
              UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
              FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_nested.nested_filters_json->'candidateIds') = 'array' THEN session_nested.nested_filters_json->'candidateIds' ELSE '[]'::jsonb END) AS candidate_id_value(value)
              UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
              FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_nested.nested_filter_json->'candidate_ids') = 'array' THEN session_nested.nested_filter_json->'candidate_ids' ELSE '[]'::jsonb END) AS candidate_id_value(value)
              UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
              FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_nested.nested_filter_json->'candidateIds') = 'array' THEN session_nested.nested_filter_json->'candidateIds' ELSE '[]'::jsonb END) AS candidate_id_value(value)
            ), valid_candidate_ids AS (
              SELECT DISTINCT LOWER(raw_candidate_ids.candidate_id_text) AS candidate_id_text
              FROM raw_candidate_ids
              WHERE raw_candidate_ids.candidate_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            )
            SELECT COALESCE(jsonb_agg(valid_candidate_ids.candidate_id_text ORDER BY valid_candidate_ids.candidate_id_text), '[]'::jsonb) AS candidate_ids_json
            FROM valid_candidate_ids
          ) AS session_candidate_ids
          WHERE COALESCE(NULLIF(BTRIM(COALESCE(
                  session_filter.filtered_json->>'candidate_filter_id',
                  session_filter.filtered_json->>'candidateFilterId',
                  session_filter.filtered_json->>'filter_candidate_id',
                  session_nested.nested_filters_json->>'candidate_filter_id',
                  session_nested.nested_filters_json->>'candidateFilterId',
                  session_nested.nested_filters_json->>'filter_candidate_id',
                  session_nested.nested_filter_json->>'candidate_filter_id',
                  session_nested.nested_filter_json->>'candidateFilterId',
                  session_nested.nested_filter_json->>'filter_candidate_id',
                  ''
                )), ''), '') IS NOT DISTINCT FROM COALESCE(v_signature_candidate_filter_id, '')
            AND COALESCE(NULLIF(BTRIM(COALESCE(
                  session_filter.filtered_json->>'client_filter_id',
                  session_filter.filtered_json->>'clientFilterId',
                  session_filter.filtered_json->>'filter_client_id',
                  session_filter.filtered_json->>'client_id',
                  session_filter.filtered_json->>'clientId',
                  session_nested.nested_filters_json->>'client_filter_id',
                  session_nested.nested_filters_json->>'clientFilterId',
                  session_nested.nested_filters_json->>'filter_client_id',
                  session_nested.nested_filters_json->>'client_id',
                  session_nested.nested_filters_json->>'clientId',
                  session_nested.nested_filter_json->>'client_filter_id',
                  session_nested.nested_filter_json->>'clientFilterId',
                  session_nested.nested_filter_json->>'filter_client_id',
                  session_nested.nested_filter_json->>'client_id',
                  session_nested.nested_filter_json->>'clientId',
                  ''
                )), ''), '') IS NOT DISTINCT FROM COALESCE(v_signature_client_filter_id, '')
            AND COALESCE(session_candidate_ids.candidate_ids_json, '[]'::jsonb) IS NOT DISTINCT FROM COALESCE(v_signature_candidate_ids, '[]'::jsonb)
            )
          )
          OR existing_session.id IS NOT DISTINCT FROM p_source_session_id
          OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(COALESCE(p_obsolete_session_ids, '[]'::jsonb)) = 'array' THEN COALESCE(p_obsolete_session_ids, '[]'::jsonb) ELSE '[]'::jsonb END) AS obsolete_session_id(value)
            WHERE NULLIF(BTRIM(obsolete_session_id.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
              AND NULLIF(BTRIM(obsolete_session_id.value), '')::uuid = existing_session.id
          )
        )
      RETURNING existing_session.id
    )
    SELECT COUNT(*)::integer,
           COALESCE(jsonb_agg(force_discarded_sessions.id::text ORDER BY force_discarded_sessions.id::text), '[]'::jsonb)
    INTO v_force_discarded_session_count,
         v_force_discarded_session_ids
    FROM force_discarded_sessions;
  ELSE
    SELECT existing_session.id
    INTO v_existing_session_id
    FROM public.banking_pay_workbench_sessions AS existing_session
    WHERE existing_session.actor_user_id = p_actor_user_id
      AND existing_session.status = 'OPEN'
      AND existing_session.discarded_at_utc IS NULL
      AND existing_session.pay_date IS NOT DISTINCT FROM p_pay_date
      AND existing_session.week_ending_cutoff IS NOT DISTINCT FROM v_effective_week_ending_cutoff
      AND EXISTS (
        SELECT 1
        FROM LATERAL (
          SELECT COALESCE(existing_session.filters_json, '{}'::jsonb) AS filtered_json
        ) AS session_filter
        CROSS JOIN LATERAL (
          SELECT
            CASE WHEN jsonb_typeof(session_filter.filtered_json->'filters') = 'object' THEN COALESCE(session_filter.filtered_json->'filters', '{}'::jsonb) ELSE '{}'::jsonb END AS nested_filters_json,
            CASE WHEN jsonb_typeof(session_filter.filtered_json->'filter') = 'object' THEN COALESCE(session_filter.filtered_json->'filter', '{}'::jsonb) ELSE '{}'::jsonb END AS nested_filter_json
        ) AS session_nested
        CROSS JOIN LATERAL (
          WITH raw_candidate_ids(candidate_id_text) AS (
            SELECT NULLIF(BTRIM(session_filter.filtered_json->>'candidate_id'), '')
            UNION ALL SELECT NULLIF(BTRIM(session_filter.filtered_json->>'candidateId'), '')
            UNION ALL SELECT NULLIF(BTRIM(session_nested.nested_filters_json->>'candidate_id'), '')
            UNION ALL SELECT NULLIF(BTRIM(session_nested.nested_filters_json->>'candidateId'), '')
            UNION ALL SELECT NULLIF(BTRIM(session_nested.nested_filter_json->>'candidate_id'), '')
            UNION ALL SELECT NULLIF(BTRIM(session_nested.nested_filter_json->>'candidateId'), '')
            UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
            FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_filter.filtered_json->'candidate_ids') = 'array' THEN session_filter.filtered_json->'candidate_ids' ELSE '[]'::jsonb END) AS candidate_id_value(value)
            UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
            FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_filter.filtered_json->'candidateIds') = 'array' THEN session_filter.filtered_json->'candidateIds' ELSE '[]'::jsonb END) AS candidate_id_value(value)
            UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
            FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_nested.nested_filters_json->'candidate_ids') = 'array' THEN session_nested.nested_filters_json->'candidate_ids' ELSE '[]'::jsonb END) AS candidate_id_value(value)
            UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
            FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_nested.nested_filters_json->'candidateIds') = 'array' THEN session_nested.nested_filters_json->'candidateIds' ELSE '[]'::jsonb END) AS candidate_id_value(value)
            UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
            FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_nested.nested_filter_json->'candidate_ids') = 'array' THEN session_nested.nested_filter_json->'candidate_ids' ELSE '[]'::jsonb END) AS candidate_id_value(value)
            UNION ALL SELECT NULLIF(BTRIM(candidate_id_value.value), '')
            FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(session_nested.nested_filter_json->'candidateIds') = 'array' THEN session_nested.nested_filter_json->'candidateIds' ELSE '[]'::jsonb END) AS candidate_id_value(value)
          ), valid_candidate_ids AS (
            SELECT DISTINCT LOWER(raw_candidate_ids.candidate_id_text) AS candidate_id_text
            FROM raw_candidate_ids
            WHERE raw_candidate_ids.candidate_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          )
          SELECT COALESCE(jsonb_agg(valid_candidate_ids.candidate_id_text ORDER BY valid_candidate_ids.candidate_id_text), '[]'::jsonb) AS candidate_ids_json
          FROM valid_candidate_ids
        ) AS session_candidate_ids
        WHERE COALESCE(NULLIF(BTRIM(COALESCE(
                session_filter.filtered_json->>'candidate_filter_id',
                session_filter.filtered_json->>'candidateFilterId',
                session_filter.filtered_json->>'filter_candidate_id',
                session_nested.nested_filters_json->>'candidate_filter_id',
                session_nested.nested_filters_json->>'candidateFilterId',
                session_nested.nested_filters_json->>'filter_candidate_id',
                session_nested.nested_filter_json->>'candidate_filter_id',
                session_nested.nested_filter_json->>'candidateFilterId',
                session_nested.nested_filter_json->>'filter_candidate_id',
                ''
              )), ''), '') IS NOT DISTINCT FROM COALESCE(v_signature_candidate_filter_id, '')
          AND COALESCE(NULLIF(BTRIM(COALESCE(
                session_filter.filtered_json->>'client_filter_id',
                session_filter.filtered_json->>'clientFilterId',
                session_filter.filtered_json->>'filter_client_id',
                session_filter.filtered_json->>'client_id',
                session_filter.filtered_json->>'clientId',
                session_nested.nested_filters_json->>'client_filter_id',
                session_nested.nested_filters_json->>'clientFilterId',
                session_nested.nested_filters_json->>'filter_client_id',
                session_nested.nested_filters_json->>'client_id',
                session_nested.nested_filters_json->>'clientId',
                session_nested.nested_filter_json->>'client_filter_id',
                session_nested.nested_filter_json->>'clientFilterId',
                session_nested.nested_filter_json->>'filter_client_id',
                session_nested.nested_filter_json->>'client_id',
                session_nested.nested_filter_json->>'clientId',
                ''
              )), ''), '') IS NOT DISTINCT FROM COALESCE(v_signature_client_filter_id, '')
          AND COALESCE(session_candidate_ids.candidate_ids_json, '[]'::jsonb) IS NOT DISTINCT FROM COALESCE(v_signature_candidate_ids, '[]'::jsonb)
      )
    ORDER BY existing_session.updated_at_utc DESC NULLS LAST,
             existing_session.created_at_utc DESC NULLS LAST,
             existing_session.id DESC
    LIMIT 1;
  END IF;

  IF v_existing_session_id IS NULL THEN
    INSERT INTO public.banking_pay_workbench_sessions (
      actor_user_id,
      pay_date,
      week_ending_cutoff,
      filters_json,
      session_signature,
      source_snapshot_run_id,
      status,
      version,
      server_selected_preview_row_ids,
      server_selected_preview_row_ids_provided,
      scope_next_cursor_json,
      scope_seed_complete,
      scope_total_count,
      scope_seeded_count,
      scope_ready_count,
      scope_pending_count,
      scope_failed_count,
      line_units_total,
      line_units_ready,
      line_units_pending,
      line_units_failed,
      preview_row_count,
      selected_row_count,
      section_counts_json,
      candidate_sample_rows_json,
      progress_state,
      progress_json,
      progress_counter_version,
      progress_updated_at_utc,
      created_at_utc,
      updated_at_utc,
      discarded_at_utc
    )
    VALUES (
      p_actor_user_id,
      p_pay_date,
      v_effective_week_ending_cutoff,
      v_sanitised_filters_json || jsonb_build_object(
        'scope_is_row_backed', true,
        'scope_seed_source', 'pay_preview_build_context.PAGE',
        'scope_count_unknown', true,
        'canonical_scope_signature_version', 4
      ),
      v_session_signature,
      v_snapshot_run_id,
      'OPEN',
      1,
      '[]'::jsonb,
      false,
      '{}'::jsonb,
      false,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      '{}'::jsonb,
      '[]'::jsonb,
      'OPENING',
      jsonb_build_object('phase', 'OPENING', 'status_text', 'Preparing payment preview.', 'count_unknown', true),
      1,
      v_now,
      v_now,
      v_now,
      NULL::timestamptz
    )
    RETURNING public.banking_pay_workbench_sessions.id,
              public.banking_pay_workbench_sessions.version
    INTO v_session_id, v_session_version;

    v_action := 'WORKBENCH_SESSION_CREATED';
  ELSE
    UPDATE public.banking_pay_workbench_sessions AS workbench_session
    SET actor_user_id = p_actor_user_id,
        pay_date = p_pay_date,
        week_ending_cutoff = v_effective_week_ending_cutoff,
        filters_json = v_sanitised_filters_json || jsonb_build_object(
          'scope_is_row_backed', true,
          'scope_seed_source', 'pay_preview_build_context.PAGE',
          'scope_count_unknown', true,
          'canonical_scope_signature_version', 4
        ),
        source_snapshot_run_id = v_snapshot_run_id,
        version = workbench_session.version + 1,
        server_selected_preview_row_ids = '[]'::jsonb,
        server_selected_preview_row_ids_provided = false,
        scope_next_cursor_json = '{}'::jsonb,
        scope_seed_complete = false,
        scope_total_count = 0,
        scope_seeded_count = 0,
        scope_ready_count = 0,
        scope_pending_count = 0,
        scope_failed_count = 0,
        line_units_total = 0,
        line_units_ready = 0,
        line_units_pending = 0,
        line_units_failed = 0,
        preview_row_count = 0,
        selected_row_count = 0,
        section_counts_json = '{}'::jsonb,
        candidate_sample_rows_json = '[]'::jsonb,
        progress_state = 'OPENING',
        progress_json = jsonb_build_object('phase', 'OPENING', 'status_text', 'Preparing payment preview.', 'count_unknown', true),
        progress_counter_version = workbench_session.progress_counter_version + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE workbench_session.id = v_existing_session_id
    RETURNING workbench_session.id,
              workbench_session.version
    INTO v_session_id, v_session_version;

    DELETE FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = v_session_id;

    DELETE FROM public.banking_pay_workbench_preview_rows AS preview_row
    WHERE preview_row.session_id = v_session_id;

    DELETE FROM public.banking_pay_workbench_candidate_line_work AS line_work
    WHERE line_work.session_id = v_session_id;

    v_action := 'WORKBENCH_SESSION_RESUMED';
  END IF;

  IF COALESCE(v_force_discarded_session_count, 0) > 0 AND v_session_id IS NOT NULL THEN
    WITH force_discarded_session_ids AS (
      SELECT NULLIF(BTRIM(discarded_session_id.value), '')::uuid AS session_id
      FROM jsonb_array_elements_text(COALESCE(v_force_discarded_session_ids, '[]'::jsonb)) AS discarded_session_id(value)
      WHERE NULLIF(BTRIM(discarded_session_id.value), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ), linked_force_discarded_sessions AS (
      UPDATE public.banking_pay_workbench_sessions AS discarded_session
      SET replacement_session_id = v_session_id,
          replacement_idempotency_key = COALESCE(
            discarded_session.replacement_idempotency_key,
            'force-new:' || discarded_session.id::text || ':replacement:' || v_session_id::text
          ),
          progress_json = jsonb_strip_nulls(
            COALESCE(discarded_session.progress_json, '{}'::jsonb)
            || jsonb_build_object(
              'replacement_session_id', v_session_id::text,
              'replacement_linked_by_function', 'pay_workbench_session_open',
              'replacement_linked_at_utc', v_now::text,
              'replacement_reason', COALESCE(v_mutation_context, 'FORCE_NEW_SESSION')
            )
          ),
          progress_counter_version = COALESCE(discarded_session.progress_counter_version, 0) + 1,
          progress_updated_at_utc = v_now,
          updated_at_utc = v_now
      FROM force_discarded_session_ids AS force_discarded_session_id
      WHERE discarded_session.id = force_discarded_session_id.session_id
        AND discarded_session.id IS DISTINCT FROM v_session_id
        AND discarded_session.status = 'DISCARDED'
        AND discarded_session.replacement_session_id IS NULL
      RETURNING discarded_session.id
    )
    SELECT COUNT(*)::integer
    INTO v_force_discarded_session_link_count
    FROM linked_force_discarded_sessions;
  END IF;

  v_seed_result := public.pay_workbench_session_seed_scope_chunk(
    p_session_id => v_session_id,
    p_cursor_json => NULL::jsonb,
    p_limit => 100
  );

  v_scope_first_page_count := COALESCE(NULLIF(BTRIM(COALESCE(v_seed_result->>'seeded_count', '')), '')::integer, 0);
  v_scope_has_more := COALESCE(NULLIF(BTRIM(COALESCE(v_seed_result->>'has_more', '')), '')::boolean, false);
  v_scope_next_cursor := COALESCE(v_seed_result->'next_cursor', NULL::jsonb);
  v_scope_seed_complete := COALESCE(NULLIF(BTRIM(COALESCE(v_seed_result->>'scope_seed_complete', '')), '')::boolean, false);
  v_scope_seeded_count := COALESCE(NULLIF(BTRIM(COALESCE(v_seed_result#>>'{progress,scope_rows_seeded}', v_seed_result->>'scope_rows_seeded', '')), '')::integer, v_scope_first_page_count, 0);
  v_scope_pending_count := COALESCE(NULLIF(BTRIM(COALESCE(v_seed_result#>>'{progress,scope_rows_pending}', v_seed_result->>'scope_rows_pending', '')), '')::integer, v_scope_first_page_count, 0);
  v_scope_ready_count := COALESCE(NULLIF(BTRIM(COALESCE(v_seed_result#>>'{progress,scope_rows_ready}', v_seed_result->>'scope_rows_ready', '')), '')::integer, 0);
  v_scope_failed_count := COALESCE(NULLIF(BTRIM(COALESCE(v_seed_result#>>'{progress,scope_rows_error}', v_seed_result->>'scope_rows_error', '')), '')::integer, 0);
  v_ready_flag := v_scope_seed_complete AND v_scope_seeded_count = 0;
  v_ready_empty := v_ready_flag;

  PERFORM public._audit_insert(
    'banking_pay_workbench_session',
    v_session_id::text,
    v_action,
    NULL::jsonb,
    jsonb_build_object(
      'id', v_session_id::text,
      'actor_user_id', p_actor_user_id::text,
      'pay_date', p_pay_date::text,
      'week_ending_cutoff', v_effective_week_ending_cutoff::text,
      'session_signature', v_session_signature,
      'source_snapshot_run_id', v_snapshot_run_id::text,
      'row_backed_scope', true,
      'force_discarded_session_count', COALESCE(v_force_discarded_session_count, 0),
      'force_discarded_session_ids', COALESCE(v_force_discarded_session_ids, '[]'::jsonb),
      'force_discarded_session_link_count', COALESCE(v_force_discarded_session_link_count, 0),
      'scope_first_page_count', v_scope_first_page_count,
      'scope_has_more', v_scope_has_more,
      'scope_seed_result', v_seed_result
    ),
    'SESSION_OPEN',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'session_id', v_session_id::text,
    'snapshot_run_id', v_snapshot_run_id::text,
    'session_signature', v_session_signature,
    'pay_date', p_pay_date::text,
    'week_ending_cutoff', v_effective_week_ending_cutoff::text,
    'action', v_action,
    'session_version', v_session_version,
    'row_backed_scope', true,
    'candidate_ids_returned', false,
    'requires_paging', true,
    'force_discarded_session_count', COALESCE(v_force_discarded_session_count, 0),
    'force_discarded_session_ids', COALESCE(v_force_discarded_session_ids, '[]'::jsonb),
    'force_discarded_session_link_count', COALESCE(v_force_discarded_session_link_count, 0)
  )
  || jsonb_build_object(
    'scope_first_page_count', COALESCE(v_scope_first_page_count, 0),
    'scope_seeded_count', COALESCE(v_scope_seeded_count, 0),
    'scope_pending_count', COALESCE(v_scope_pending_count, 0),
    'scope_ready_count', COALESCE(v_scope_ready_count, 0),
    'scope_failed_count', COALESCE(v_scope_failed_count, 0),
    'scope_has_more', COALESCE(v_scope_has_more, false),
    'scope_seed_complete', COALESCE(v_scope_seed_complete, false),
    'scope_next_cursor', v_scope_next_cursor,
    'scope_seed_result', COALESCE(v_seed_result, '{}'::jsonb)
  )
  || jsonb_build_object(
    'ready', v_ready_flag,
    'ready_flag', v_ready_flag,
    'ready_empty', v_ready_empty,
    'total_count', COALESCE(v_scope_seeded_count, 0),
    'completed_count', COALESCE(v_scope_ready_count, 0),
    'pending_count', COALESCE(v_scope_pending_count, 0),
    'failed_count', COALESCE(v_scope_failed_count, 0),
    'candidate_counts', jsonb_build_object(
      'total', COALESCE(v_scope_seeded_count, 0),
      'ready', COALESCE(v_scope_ready_count, 0),
      'pending', COALESCE(v_scope_pending_count, 0),
      'failed', COALESCE(v_scope_failed_count, 0),
      'count_unknown', COALESCE(v_scope_has_more, false)
    ),
    'progress_summary', jsonb_build_object(
      'phase', CASE WHEN v_ready_flag THEN 'READY' ELSE 'REFRESHING_CANDIDATES' END,
      'status_text', CASE WHEN v_ready_flag THEN 'Payment preview is ready.' ELSE 'Preparing payment preview.' END,
      'ready_flag', v_ready_flag,
      'ready_empty', v_ready_empty,
      'count_unknown', COALESCE(v_scope_has_more, false)
    ),
    'opened_at_utc', v_now::text
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_session_open(uuid,date,date,jsonb,text,boolean,boolean,uuid,jsonb,text,jsonb,jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_open(uuid,date,date,jsonb,text,boolean,boolean,uuid,jsonb,text,jsonb,jsonb,jsonb) FROM PUBLIC, anon, authenticated, service_role, authenticator, supabase_admin;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_open(uuid,date,date,jsonb,text,boolean,boolean,uuid,jsonb,text,jsonb,jsonb,jsonb) TO postgres;

commit;
