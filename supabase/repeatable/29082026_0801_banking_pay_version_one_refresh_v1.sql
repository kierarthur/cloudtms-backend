-- Banking Pay current-authority refresh for a first-version Workbench session.
-- A version-one session has no prior session version to rebase. It must use
-- the existing canonical enqueue path without calling the version rebase
-- helper with the impossible version zero.
-- Policy X remains pre-Draft live truth; no financial owner is changed.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_refresh_current_authority_v1(
  p_session_id uuid,
  p_actor_user_id uuid,
  p_cursor_json jsonb DEFAULT '{}'::jsonb,
  p_limit integer DEFAULT 100
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_after_scope_ordinal bigint := COALESCE(NULLIF(pg_catalog.btrim(COALESCE(p_cursor_json->>'last_scope_ordinal', '')), '')::bigint, -1);
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_last_scope_ordinal bigint := NULL::bigint;
  v_has_more boolean := false;
  v_currentness jsonb := '{}'::jsonb;
  v_candidate_result jsonb := '{}'::jsonb;
  v_candidate_id uuid := NULL::uuid;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_rebase_result jsonb := '{}'::jsonb;
  v_terminal_current_count integer := 0;
  v_active_owner_count integer := 0;
  v_version_rebased_count integer := 0;
  v_enqueued_candidate_count integer := 0;
  v_no_job_count integer := 0;
  v_route_results jsonb := '[]'::jsonb;
BEGIN
  IF p_session_id IS NULL OR p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_ARGUMENT_INVALID'
      USING ERRCODE = 'P0001';
  END IF;
  IF pg_catalog.jsonb_typeof(COALESCE(p_cursor_json, '{}'::jsonb)) <> 'object' THEN
    RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_CURSOR_INVALID'
      USING ERRCODE = 'P0001';
  END IF;

  PERFORM 1 FROM public.tms_users AS actor_row WHERE actor_row.id = p_actor_user_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_ACTOR_INVALID'
      USING ERRCODE = 'P0001';
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_SESSION_MISSING'
      USING ERRCODE = 'P0001';
  END IF;
  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_session.status, ''))) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001';
  END IF;

  WITH candidate_page AS (
    SELECT scope_row.candidate_id, scope_row.scope_ordinal
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = p_session_id
      AND scope_row.scope_ordinal > v_after_scope_ordinal
    ORDER BY scope_row.scope_ordinal, scope_row.candidate_id
    LIMIT (v_limit + 1)
  ), bounded_page AS (
    SELECT candidate_page.candidate_id, candidate_page.scope_ordinal
    FROM candidate_page
    ORDER BY candidate_page.scope_ordinal, candidate_page.candidate_id
    LIMIT v_limit
  )
  SELECT
    COALESCE(pg_catalog.array_agg(bounded_page.candidate_id ORDER BY bounded_page.scope_ordinal, bounded_page.candidate_id), ARRAY[]::uuid[]),
    pg_catalog.max(bounded_page.scope_ordinal),
    (SELECT pg_catalog.count(*) > v_limit FROM candidate_page)
  INTO v_candidate_ids, v_last_scope_ordinal, v_has_more
  FROM bounded_page;

  IF pg_catalog.cardinality(v_candidate_ids) = 0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'contract_version', 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_V1',
      'session_id', p_session_id,
      'candidate_count', 0,
      'terminal_current_count', 0,
      'active_owner_count', 0,
      'version_rebased_count', 0,
      'enqueued_candidate_count', 0,
      'work_candidate_count', 0,
      'has_more', false,
      'next_cursor', NULL::jsonb,
      'no_change', true,
      'policy_x_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  END IF;

  v_currentness := private.pay_workbench_candidate_physical_currentness_page_v1(
    p_session_id,
    v_candidate_ids,
    'OBSERVE_ONLY',
    pg_catalog.jsonb_build_object('contract_version', '1', 'allow_active_owner', true)
  );

  FOR v_candidate_result IN
    SELECT result_row.value
    FROM pg_catalog.jsonb_array_elements(COALESCE(v_currentness->'candidate_results', '[]'::jsonb)) AS result_row(value)
    ORDER BY result_row.value->>'candidate_id'
  LOOP
    IF COALESCE(v_candidate_result->>'candidate_id', '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_RESULT_INVALID'
        USING ERRCODE = 'P0001';
    END IF;
    v_candidate_id := (v_candidate_result->>'candidate_id')::uuid;

    IF COALESCE((v_candidate_result->>'terminal_current')::boolean, false) THEN
      v_terminal_current_count := v_terminal_current_count + 1;
      v_route_results := v_route_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'candidate_id', v_candidate_id,
        'route', 'CURRENT_NO_CHANGE',
        'currentness_reason', v_candidate_result->>'currentness_reason',
        'proof_digest', v_candidate_result->>'proof_digest'
      ));
    ELSIF COALESCE((v_candidate_result->>'current_or_active_owner')::boolean, false)
          AND COALESCE(v_candidate_result->>'currentness_reason', '') = 'ACTIVE_OWNER' THEN
      v_active_owner_count := v_active_owner_count + 1;
      v_route_results := v_route_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'candidate_id', v_candidate_id,
        'route', 'ACTIVE_OWNER',
        'owner_job_id', v_candidate_result->>'active_owner_job_id',
        'proof_digest', v_candidate_result->>'proof_digest'
      ));
    ELSE
      IF v_session.version = 1 THEN
        v_rebase_result := pg_catalog.jsonb_build_object(
          'ok', true,
          'rebased', false,
          'reason', 'SESSION_VERSION_ONE_HAS_NO_PREVIOUS_VERSION'
        );
      ELSE
        v_rebase_result := private.pay_workbench_candidate_session_version_rebase_v1(
          p_session_id,
          v_candidate_id,
          v_session.version - 1,
          v_session.version,
          p_actor_user_id
        );
      END IF;

      IF COALESCE((v_rebase_result->>'rebased')::boolean, false) THEN
        v_version_rebased_count := v_version_rebased_count + 1;
        v_terminal_current_count := v_terminal_current_count + 1;
        v_route_results := v_route_results || pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object(
            'candidate_id', v_candidate_id,
            'route', 'SESSION_VERSION_REBASE',
            'currentness_reason', v_candidate_result->>'currentness_reason',
            'rebase_result', v_rebase_result
          )
        );
      ELSE
        v_enqueue_result := public.pay_workbench_enqueue_session_candidate_refresh(
        p_session_id => p_session_id,
        p_candidate_id => v_candidate_id,
        p_reason => 'USER_REQUESTED_CURRENT_AUTHORITY_REFRESH',
        p_actor_user_id => p_actor_user_id,
        p_payload_json => pg_catalog.jsonb_build_object(
          'force_refresh', false,
          'user_requested_refresh', false,
          'refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
          'physical_currentness_reason', v_candidate_result->>'currentness_reason',
          'physical_currentness_proof_digest', v_candidate_result->>'proof_digest'
        )
        );
        IF COALESCE((v_enqueue_result->>'enqueued_candidate_count')::integer, 0) > 0
           OR COALESCE(v_enqueue_result->>'job_type', '') <> '' THEN
          v_enqueued_candidate_count := v_enqueued_candidate_count + 1;
        ELSE
          v_no_job_count := v_no_job_count + 1;
        END IF;
        v_route_results := v_route_results || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
          'candidate_id', v_candidate_id,
          'route', COALESCE(NULLIF(v_enqueue_result->>'job_type', ''), 'CANONICAL_ENQUEUE_NO_JOB'),
          'currentness_reason', v_candidate_result->>'currentness_reason',
          'version_rebase_result', v_rebase_result,
          'enqueue_result', v_enqueue_result
        ));
      END IF;
    END IF;
  END LOOP;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'contract_version', 'WORKBENCH_CURRENT_AUTHORITY_REFRESH_V1',
    'session_id', p_session_id,
    'candidate_count', pg_catalog.cardinality(v_candidate_ids),
    'terminal_current_count', v_terminal_current_count,
    'active_owner_count', v_active_owner_count,
    'version_rebased_count', v_version_rebased_count,
    'enqueued_candidate_count', v_enqueued_candidate_count,
    'no_job_count', v_no_job_count,
    'work_candidate_count', v_active_owner_count + v_enqueued_candidate_count,
    'route_results', v_route_results,
    'has_more', v_has_more,
    'next_cursor', CASE WHEN v_has_more THEN pg_catalog.jsonb_build_object('last_scope_ordinal', v_last_scope_ordinal) ELSE NULL::jsonb END,
    'no_change', v_terminal_current_count = pg_catalog.cardinality(v_candidate_ids)
      AND v_version_rebased_count = 0,
    'policy_x_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer) OWNER TO postgres;
ALTER FUNCTION public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer) SET plpgsql_check.mode TO 'disabled';
REVOKE ALL ON FUNCTION public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_refresh_current_authority_v1(uuid,uuid,jsonb,integer) TO service_role;

NOTIFY pgrst, 'reload schema';

commit;
