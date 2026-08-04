-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.pay_workbench_contract_client_dirty_fanout_chunk(p_job_id uuid, p_cursor_json jsonb, p_limit integer)
-- Installed pg_get_functiondef MD5: 4230c3cd60c64f3c767d5332220681b7
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.pay_workbench_contract_client_dirty_fanout_chunk(p_job_id uuid, p_cursor_json jsonb DEFAULT NULL::jsonb, p_limit integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_job_row public.banking_pay_workbench_jobs%ROWTYPE;
  v_payload_json jsonb := '{}'::jsonb;
  v_cursor_json jsonb := '{}'::jsonb;
  v_scope_kind text := '';
  v_scope_id_text text := NULL::text;
  v_scope_id uuid := NULL::uuid;
  v_specific_scope_id_text text := NULL::text;
  v_specific_scope_id uuid := NULL::uuid;
  v_payload_candidate_id_text text := NULL::text;
  v_payload_candidate_id uuid := NULL::uuid;
  v_payload_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_payload_candidate_value jsonb := NULL::jsonb;
  v_payload_candidate_value_text text := NULL::text;
  v_payload_candidate_index integer := 0;
  v_cursor_candidate_id_text text := NULL::text;
  v_cursor_candidate_id uuid := NULL::uuid;
  v_reason text := NULL::text;
  v_trigger_table text := NULL::text;
  v_trigger_op text := NULL::text;
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_ids_json jsonb := '[]'::jsonb;
  v_candidate_count integer := 0;
  v_has_more boolean := false;
  v_last_candidate_id uuid := NULL::uuid;
  v_next_cursor_json jsonb := NULL::jsonb;
  v_affected_scope_count integer := 0;
  v_affected_session_count integer := 0;
  v_jobs_queued_or_reused integer := 0;
  v_source_rows_marked_dirty_count integer := 0;
  v_gate_candidate_id uuid := NULL::uuid;
  v_gate_result jsonb := '{}'::jsonb;
  v_gate_allowed_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_gate_blocked boolean := false;
  v_gate_blocked_candidate_id uuid := NULL::uuid;
  v_gate_last_allowed_candidate_id uuid := NULL::uuid;
  v_gate_allowed_count integer := 0;
  v_invalidation_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_invalidation_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_scope_change_tx_token uuid := NULL::uuid;
  v_scope_invalidation_result jsonb := '{}'::jsonb;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_job_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_JOB_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_DIRTY_FANOUT_JOB_ID_REQUIRED'
            )::text;
  END IF;

  SELECT fanout_job.*
  INTO v_job_row
  FROM public.banking_pay_workbench_jobs AS fanout_job
  WHERE fanout_job.id = p_job_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_JOB_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_DIRTY_FANOUT_JOB_NOT_FOUND',
              'job_id', p_job_id::text
            )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_job_row.job_type, ''))) <> 'CONTRACT_CLIENT_DIRTY_FANOUT' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_JOB_TYPE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_DIRTY_FANOUT_JOB_TYPE_INVALID',
              'job_id', v_job_row.id::text,
              'job_type', v_job_row.job_type
            )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_job_row.status, ''))) <> 'RUNNING' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_JOB_NOT_RUNNING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_DIRTY_FANOUT_JOB_NOT_RUNNING',
              'job_id', v_job_row.id::text,
              'status', v_job_row.status
            )::text;
  END IF;

  IF v_job_row.scope_change_generation IS NOT NULL THEN
    PERFORM set_config(
      'cloudtms.banking_pay_inherited_scope_generation',
      v_job_row.scope_change_generation::text,
      true
    );
  END IF;

  v_payload_json := CASE
    WHEN jsonb_typeof(COALESCE(v_job_row.payload_json, '{}'::jsonb)) = 'object'
      THEN COALESCE(v_job_row.payload_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;

  v_cursor_json := CASE
    WHEN p_cursor_json IS NOT NULL
         AND jsonb_typeof(p_cursor_json) = 'object'
      THEN p_cursor_json
    WHEN jsonb_typeof(v_payload_json->'cursor_json') = 'object'
      THEN v_payload_json->'cursor_json'
    WHEN jsonb_typeof(v_payload_json->'cursor') = 'object'
      THEN v_payload_json->'cursor'
    ELSE '{}'::jsonb
  END;

  v_scope_kind := UPPER(BTRIM(COALESCE(v_payload_json->>'scope_kind', '')));

  IF v_scope_kind NOT IN ('CONTRACT', 'CLIENT', 'UMBRELLA') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_SCOPE_KIND_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_DIRTY_FANOUT_SCOPE_KIND_INVALID',
              'job_id', v_job_row.id::text,
              'scope_kind', v_scope_kind
            )::text;
  END IF;

  v_scope_id_text := COALESCE(
    NULLIF(BTRIM(COALESCE(v_payload_json->>'scope_id', '')), ''),
    CASE
      WHEN v_scope_kind = 'CONTRACT'
        THEN NULLIF(BTRIM(COALESCE(v_payload_json->>'contract_id', '')), '')
      WHEN v_scope_kind = 'CLIENT'
        THEN NULLIF(BTRIM(COALESCE(v_payload_json->>'client_id', '')), '')
      WHEN v_scope_kind = 'UMBRELLA'
        THEN NULLIF(BTRIM(COALESCE(v_payload_json->>'umbrella_id', '')), '')
      ELSE NULL::text
    END
  );

  IF COALESCE(v_scope_id_text, '')
     !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_SCOPE_ID_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_DIRTY_FANOUT_SCOPE_ID_INVALID',
              'job_id', v_job_row.id::text,
              'scope_kind', v_scope_kind,
              'scope_id', v_scope_id_text
            )::text;
  END IF;

  v_scope_id := v_scope_id_text::uuid;

  v_specific_scope_id_text := CASE
    WHEN v_scope_kind = 'CONTRACT'
      THEN NULLIF(BTRIM(COALESCE(v_payload_json->>'contract_id', '')), '')
    WHEN v_scope_kind = 'CLIENT'
      THEN NULLIF(BTRIM(COALESCE(v_payload_json->>'client_id', '')), '')
    WHEN v_scope_kind = 'UMBRELLA'
      THEN NULLIF(BTRIM(COALESCE(v_payload_json->>'umbrella_id', '')), '')
    ELSE NULL::text
  END;

  IF v_specific_scope_id_text IS NOT NULL THEN
    IF v_specific_scope_id_text
       !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_SPECIFIC_SCOPE_ID_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_DIRTY_FANOUT_SPECIFIC_SCOPE_ID_INVALID',
                'job_id', v_job_row.id::text,
                'scope_kind', v_scope_kind,
                'scope_id', v_scope_id::text,
                'specific_scope_id', v_specific_scope_id_text
              )::text;
    END IF;

    v_specific_scope_id := v_specific_scope_id_text::uuid;

    IF v_specific_scope_id IS DISTINCT FROM v_scope_id THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_SCOPE_PAYLOAD_CONFLICT'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_DIRTY_FANOUT_SCOPE_PAYLOAD_CONFLICT',
                'job_id', v_job_row.id::text,
                'scope_kind', v_scope_kind,
                'scope_id', v_scope_id::text,
                'specific_scope_id', v_specific_scope_id::text
              )::text;
    END IF;
  END IF;

  IF v_payload_json ? 'candidate_ids' THEN
    IF jsonb_typeof(v_payload_json->'candidate_ids') IS DISTINCT FROM 'array' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_CANDIDATE_IDS_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_DIRTY_FANOUT_CANDIDATE_IDS_INVALID',
                'job_id', v_job_row.id::text,
                'candidate_ids', v_payload_json->'candidate_ids'
              )::text;
    END IF;

    v_payload_candidate_index := 0;

    FOR v_payload_candidate_value IN
      SELECT payload_candidate.value
      FROM jsonb_array_elements(v_payload_json->'candidate_ids') AS payload_candidate(value)
    LOOP
      v_payload_candidate_index := v_payload_candidate_index + 1;
      v_payload_candidate_value_text := CASE
        WHEN jsonb_typeof(v_payload_candidate_value) = 'string'
          THEN NULLIF(BTRIM(v_payload_candidate_value #>> '{}'), '')
        ELSE NULL::text
      END;

      IF COALESCE(v_payload_candidate_value_text, '')
         !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_CANDIDATE_IDS_INVALID'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_WORKBENCH_DIRTY_FANOUT_CANDIDATE_IDS_INVALID',
                  'job_id', v_job_row.id::text,
                  'candidate_index', v_payload_candidate_index,
                  'candidate_value', v_payload_candidate_value
                )::text;
      END IF;

      v_payload_candidate_ids := array_append(
        v_payload_candidate_ids,
        v_payload_candidate_value_text::uuid
      );
    END LOOP;

    SELECT COALESCE(
      array_agg(DISTINCT payload_candidate.candidate_id ORDER BY payload_candidate.candidate_id),
      ARRAY[]::uuid[]
    )
    INTO v_payload_candidate_ids
    FROM unnest(v_payload_candidate_ids) AS payload_candidate(candidate_id);
  END IF;

  v_payload_candidate_id_text := NULLIF(
    BTRIM(COALESCE(v_payload_json->>'candidate_id', '')),
    ''
  );

  IF v_payload_candidate_id_text IS NOT NULL THEN
    IF v_payload_candidate_id_text
       !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_CANDIDATE_ID_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_DIRTY_FANOUT_CANDIDATE_ID_INVALID',
                'job_id', v_job_row.id::text,
                'candidate_id', v_payload_candidate_id_text
              )::text;
    END IF;

    v_payload_candidate_id := v_payload_candidate_id_text::uuid;
  END IF;

  v_cursor_candidate_id_text := COALESCE(
    NULLIF(BTRIM(COALESCE(v_cursor_json->>'last_candidate_id', '')), ''),
    NULLIF(BTRIM(COALESCE(v_cursor_json->>'after_candidate_id', '')), ''),
    NULLIF(BTRIM(COALESCE(v_cursor_json->>'candidate_id', '')), '')
  );

  IF v_cursor_candidate_id_text IS NOT NULL THEN
    IF v_cursor_candidate_id_text
       !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_FANOUT_CURSOR_INVALID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_DIRTY_FANOUT_CURSOR_INVALID',
                'job_id', v_job_row.id::text,
                'cursor_json', v_cursor_json
              )::text;
    END IF;

    v_cursor_candidate_id := v_cursor_candidate_id_text::uuid;
  END IF;

  v_reason := COALESCE(
    NULLIF(BTRIM(COALESCE(v_payload_json->>'reason', '')), ''),
    'DIRTY_FANOUT:' || v_scope_kind
  );
  v_trigger_table := NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_table', '')), '');
  v_trigger_op := NULLIF(BTRIM(COALESCE(v_payload_json->>'trigger_op', '')), '');

  WITH candidate_pool AS (
    SELECT historical_candidate.candidate_id
    FROM unnest(v_payload_candidate_ids) AS historical_candidate(candidate_id)
    WHERE historical_candidate.candidate_id IS NOT NULL

    UNION

    SELECT v_payload_candidate_id AS candidate_id
    WHERE v_payload_candidate_id IS NOT NULL

    UNION

    SELECT derived_payload_candidate.candidate_id
    FROM public._pay_workbench_candidate_serial_candidate_ids(v_job_row.candidate_id, v_payload_json) AS derived_payload_candidate(candidate_id)
    WHERE derived_payload_candidate.candidate_id IS NOT NULL

    UNION

    SELECT contract_scope.candidate_id
    FROM public.contracts AS contract_scope
    WHERE v_scope_kind = 'CONTRACT'
      AND contract_scope.id = v_scope_id
      AND contract_scope.candidate_id IS NOT NULL

    UNION

    SELECT client_contract.candidate_id
    FROM public.contracts AS client_contract
    WHERE v_scope_kind = 'CLIENT'
      AND client_contract.client_id = v_scope_id
      AND client_contract.candidate_id IS NOT NULL

    UNION

    SELECT umbrella_candidate.id
    FROM public.candidates AS umbrella_candidate
    WHERE v_scope_kind = 'UMBRELLA'
      AND umbrella_candidate.umbrella_id = v_scope_id
  ),
  ordered_candidates AS (
    SELECT candidate_pool.candidate_id,
           ROW_NUMBER() OVER (ORDER BY candidate_pool.candidate_id) AS candidate_ordinal
    FROM candidate_pool
    WHERE candidate_pool.candidate_id IS NOT NULL
      AND (
        v_cursor_candidate_id IS NULL
        OR candidate_pool.candidate_id > v_cursor_candidate_id
      )
    ORDER BY candidate_pool.candidate_id
    LIMIT (v_limit + 1)
  ),
  selected_candidates AS (
    SELECT ordered_candidates.candidate_id,
           ordered_candidates.candidate_ordinal
    FROM ordered_candidates
    WHERE ordered_candidates.candidate_ordinal <= v_limit
  )
  SELECT
    COALESCE(
      array_agg(
        selected_candidates.candidate_id
        ORDER BY selected_candidates.candidate_id
      ),
      ARRAY[]::uuid[]
    ),
    COALESCE(COUNT(*)::integer, 0),
    EXISTS (
      SELECT 1
      FROM ordered_candidates AS extra_candidate
      WHERE extra_candidate.candidate_ordinal > v_limit
    ),
    (
      SELECT last_selected.candidate_id
      FROM selected_candidates AS last_selected
      ORDER BY last_selected.candidate_id DESC
      LIMIT 1
    )
  INTO
    v_candidate_ids,
    v_candidate_count,
    v_has_more,
    v_last_candidate_id
  FROM selected_candidates;

  v_candidate_ids_json := COALESCE(to_jsonb(v_candidate_ids), '[]'::jsonb);

  IF v_has_more AND v_last_candidate_id IS NOT NULL THEN
    v_next_cursor_json := jsonb_build_object(
      'last_candidate_id', v_last_candidate_id::text,
      'scope_kind', v_scope_kind,
      'scope_id', v_scope_id::text
    );
  ELSE
    v_next_cursor_json := NULL::jsonb;
  END IF;

  IF COALESCE(v_candidate_count, 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'job_id', v_job_row.id::text,
      'job_type', v_job_row.job_type,
      'scope_kind', v_scope_kind,
      'scope_id', v_scope_id::text,
      'reason', v_reason,
      'limit', v_limit,
      'cursor_json', v_cursor_json,
      'candidate_ids', '[]'::jsonb,
      'candidate_count', 0,
      'processed_count', 0,
      'affected_scope_count', 0,
      'affected_session_count', 0,
      'enqueued_count', 0,
      'has_more', false,
      'next_cursor', NULL::jsonb,
      'next_cursor_json', NULL::jsonb,
      'candidate_line_calculation_performed', false
    );
  END IF;

  v_gate_allowed_candidate_ids := ARRAY[]::uuid[];
  v_gate_blocked := false;
  v_gate_blocked_candidate_id := NULL::uuid;
  v_gate_last_allowed_candidate_id := NULL::uuid;

  FOREACH v_gate_candidate_id IN ARRAY v_candidate_ids
  LOOP
    v_gate_result := public._pay_workbench_candidate_serial_try_gate(
      p_job_id => v_job_row.id,
      p_candidate_id => v_gate_candidate_id,
      p_job_type => v_job_row.job_type,
      p_payload_json => v_payload_json || jsonb_build_object(
        'candidate_id', v_gate_candidate_id::text,
        'candidate_serial_candidate_id', v_gate_candidate_id::text,
        'candidate_serial_key', public._pay_workbench_candidate_serial_key(v_gate_candidate_id),
        'fanout_scope_kind', v_scope_kind,
        'fanout_scope_id', v_scope_id::text,
        'cursor_json', v_cursor_json
      ),
      p_reason => 'CANDIDATE_SERIAL_CONTRACT_CLIENT_FANOUT_GATE'
    );

    IF lower(BTRIM(COALESCE(v_gate_result->>'blocked', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
       OR lower(BTRIM(COALESCE(v_gate_result->>'allowed', 'true'))) IN ('false', 'f', '0', 'no', 'n', 'off') THEN
      v_gate_blocked := true;
      v_gate_blocked_candidate_id := v_gate_candidate_id;
      EXIT;
    END IF;

    v_gate_allowed_candidate_ids := array_append(v_gate_allowed_candidate_ids, v_gate_candidate_id);
    v_gate_last_allowed_candidate_id := v_gate_candidate_id;
  END LOOP;

  IF v_gate_blocked IS TRUE THEN
    SELECT COALESCE(array_agg(DISTINCT allowed_candidate.candidate_id ORDER BY allowed_candidate.candidate_id), ARRAY[]::uuid[])
    INTO v_candidate_ids
    FROM unnest(COALESCE(v_gate_allowed_candidate_ids, ARRAY[]::uuid[])) AS allowed_candidate(candidate_id)
    WHERE allowed_candidate.candidate_id IS NOT NULL;

    v_candidate_count := COALESCE(array_length(v_candidate_ids, 1), 0);
    v_candidate_ids_json := COALESCE(to_jsonb(v_candidate_ids), '[]'::jsonb);
    v_has_more := true;
    v_last_candidate_id := v_gate_last_allowed_candidate_id;

    v_next_cursor_json := jsonb_strip_nulls(
      CASE
        WHEN v_gate_last_allowed_candidate_id IS NOT NULL THEN
          jsonb_build_object(
            'last_candidate_id', v_gate_last_allowed_candidate_id::text,
            'scope_kind', v_scope_kind,
            'scope_id', v_scope_id::text
          )
        ELSE
          COALESCE(v_cursor_json, '{}'::jsonb)
          || jsonb_build_object(
            'scope_kind', v_scope_kind,
            'scope_id', v_scope_id::text
          )
      END
      || jsonb_build_object(
        'candidate_serial_blocked_candidate_id', v_gate_blocked_candidate_id::text,
        'candidate_serial_blocked_at_utc', v_now::text,
        'candidate_serial_wait_reason', COALESCE(v_gate_result->>'reason', 'CANDIDATE_SERIAL_CONTRACT_CLIENT_FANOUT_DELAYED'),
        'candidate_serial_delayed', true
      )
    );

    PERFORM public._pay_workbench_candidate_serial_audit(
      'CANDIDATE_SERIAL_CONTRACT_CLIENT_FANOUT_DELAYED',
      v_job_row.id,
      v_gate_blocked_candidate_id,
      COALESCE(v_gate_result, '{}'::jsonb) || jsonb_build_object(
        'job_type', v_job_row.job_type,
        'scope_kind', v_scope_kind,
        'scope_id', v_scope_id::text,
        'cursor_json', v_cursor_json,
        'next_cursor_json', v_next_cursor_json,
        'allowed_before_block_count', COALESCE(v_candidate_count, 0),
        'blocked_candidate_preserved', true,
        'candidate_mutation_skipped_for_blocked_candidate', true
      ),
      'CANDIDATE_SERIAL_CONTRACT_CLIENT_FANOUT_DELAYED',
      NULL::uuid
    );

    IF COALESCE(v_candidate_count, 0) = 0 THEN
      UPDATE public.banking_pay_workbench_jobs AS delayed_job
      SET payload_json = public._pay_workbench_dirty_payload_merge(
            COALESCE(delayed_job.payload_json, '{}'::jsonb),
            jsonb_build_object(
              'candidate_serial_delayed', true,
              'candidate_serial_blocked_candidate_id', v_gate_blocked_candidate_id::text,
              'candidate_serial_wait_reason', COALESCE(v_gate_result->>'reason', 'CANDIDATE_SERIAL_CONTRACT_CLIENT_FANOUT_DELAYED'),
              'next_cursor_json', v_next_cursor_json,
              'cursor_json', v_next_cursor_json,
              'has_more', true,
              'rerun_required', true,
              'candidate_mutation_skipped_for_blocked_candidate', true,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            )
          ),
          updated_at_utc = v_now
      WHERE delayed_job.id = v_job_row.id;

      RETURN jsonb_build_object(
        'ok', true,
        'job_id', v_job_row.id::text,
        'job_type', v_job_row.job_type,
        'scope_kind', v_scope_kind,
        'scope_id', v_scope_id::text,
        'reason', v_reason,
        'limit', v_limit,
        'cursor_json', v_cursor_json,
        'candidate_ids', '[]'::jsonb,
        'candidate_count', 0,
        'processed_count', 0,
        'affected_scope_count', 0,
        'affected_session_count', 0,
        'enqueued_count', 0,
        'has_more', true,
        'next_cursor', v_next_cursor_json,
        'next_cursor_json', v_next_cursor_json,
        'candidate_serial_delayed', true,
        'candidate_serial_blocked_candidate_id', v_gate_blocked_candidate_id::text,
        'candidate_mutation_skipped_for_blocked_candidate', true,
        'candidate_line_calculation_performed', false
      );
    END IF;
  END IF;

  CREATE TEMPORARY TABLE IF NOT EXISTS pay_workbench_dirty_fanout_candidates (
    candidate_id uuid PRIMARY KEY,
    source_change_seq bigint NOT NULL DEFAULT 0
  ) ON COMMIT DROP;

  CREATE TEMPORARY TABLE IF NOT EXISTS pay_workbench_dirty_fanout_affected_scope (
    session_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    PRIMARY KEY (session_id, candidate_id)
  ) ON COMMIT DROP;

  TRUNCATE TABLE pg_temp.pay_workbench_dirty_fanout_candidates;
  TRUNCATE TABLE pg_temp.pay_workbench_dirty_fanout_affected_scope;

  INSERT INTO pg_temp.pay_workbench_dirty_fanout_candidates (
    candidate_id,
    source_change_seq
  )
  SELECT page_candidate.candidate_id,
         0::bigint
  FROM unnest(v_candidate_ids) AS page_candidate(candidate_id)
  ON CONFLICT (candidate_id) DO NOTHING;

  WITH exact_pairs AS (
    SELECT active_state.candidate_id,active_state.timesheet_id
    FROM private.banking_pay_workbench_timesheet_scope_state active_state
    JOIN pg_temp.pay_workbench_dirty_fanout_candidates fanout_candidate
      ON fanout_candidate.candidate_id=active_state.candidate_id
    WHERE active_state.economic_state IN ('DIRTY','LIVE')
    UNION
    SELECT source_owner.candidate_id,source_owner.timesheet_id
    FROM public.banking_pay_workbench_candidate_source_lines source_owner
    JOIN pg_temp.pay_workbench_dirty_fanout_candidates fanout_candidate
      ON fanout_candidate.candidate_id=source_owner.candidate_id
    WHERE source_owner.status IN ('CURRENT','DIRTY') AND source_owner.timesheet_id IS NOT NULL
  ), complete_pairs AS (
    SELECT candidate_id,timesheet_id FROM exact_pairs
    UNION ALL
    SELECT fanout_candidate.candidate_id,NULL::uuid
    FROM pg_temp.pay_workbench_dirty_fanout_candidates fanout_candidate
    WHERE NOT EXISTS(SELECT 1 FROM exact_pairs WHERE exact_pairs.candidate_id=fanout_candidate.candidate_id)
  )
  SELECT array_agg(candidate_id ORDER BY candidate_id,timesheet_id NULLS FIRST),
         array_agg(timesheet_id ORDER BY candidate_id,timesheet_id NULLS FIRST)
  INTO v_invalidation_candidate_ids,v_invalidation_timesheet_ids
  FROM complete_pairs;

  IF COALESCE(v_job_row.payload_json->>'scope_change_tx_token','')
     ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_scope_change_tx_token:=(v_job_row.payload_json->>'scope_change_tx_token')::uuid;
  END IF;
  v_scope_invalidation_result:=private.pay_workbench_scope_invalidate_v1(
    COALESCE(v_invalidation_candidate_ids,ARRAY[]::uuid[]),
    COALESCE(v_invalidation_timesheet_ids,ARRAY[]::uuid[]),
    v_reason,v_scope_change_tx_token,
    COALESCE(v_job_row.payload_json,'{}'::jsonb)||jsonb_build_object(
      'skip_candidate_job_enqueue',true,'inherited_scope_generation',v_job_row.scope_change_generation)
  );

  WITH bumped_counters AS (
    INSERT INTO public.app_change_counters AS candidate_counter (
      entity_key,
      seq,
      updated_at
    )
    SELECT
      'pay_candidate:' || fanout_candidate.candidate_id::text,
      1::bigint,
      v_now
    FROM pg_temp.pay_workbench_dirty_fanout_candidates AS fanout_candidate
    ON CONFLICT (entity_key)
    DO UPDATE
    SET seq = candidate_counter.seq + 1,
        updated_at = v_now
    RETURNING candidate_counter.entity_key,
              candidate_counter.seq
  )
  UPDATE pg_temp.pay_workbench_dirty_fanout_candidates AS fanout_candidate
  SET source_change_seq = bumped_counter.seq
  FROM bumped_counters AS bumped_counter
  WHERE bumped_counter.entity_key = 'pay_candidate:' || fanout_candidate.candidate_id::text;

  WITH candidate_open_scope AS (
    SELECT
      scope_row.session_id,
      scope_row.candidate_id,
      open_session.actor_user_id,
      open_session.pay_date,
      MAX(open_session.pay_date) OVER (
        PARTITION BY open_session.actor_user_id
      ) AS latest_actor_open_pay_date
    FROM public.banking_pay_workbench_session_scope AS scope_row
    JOIN public.banking_pay_workbench_sessions AS open_session
      ON open_session.id = scope_row.session_id
    JOIN pg_temp.pay_workbench_dirty_fanout_candidates AS fanout_candidate
      ON fanout_candidate.candidate_id = scope_row.candidate_id
    WHERE open_session.status = 'OPEN'
      AND open_session.discarded_at_utc IS NULL
  ),
  affected_scope AS (
    UPDATE public.banking_pay_workbench_session_scope AS scope_update
    SET status = 'SOURCE_BUILD_PENDING',
        dirty = true,
        error_json = NULL::jsonb,
        updated_at_utc = v_now
    FROM candidate_open_scope AS target_scope
    WHERE target_scope.session_id = scope_update.session_id
      AND target_scope.candidate_id = scope_update.candidate_id
      AND target_scope.pay_date = target_scope.latest_actor_open_pay_date
    RETURNING scope_update.session_id,
              scope_update.candidate_id
  )
  INSERT INTO pg_temp.pay_workbench_dirty_fanout_affected_scope (
    session_id,
    candidate_id
  )
  SELECT affected_scope.session_id,
         affected_scope.candidate_id
  FROM affected_scope
  ON CONFLICT (session_id, candidate_id) DO NOTHING;

  GET DIAGNOSTICS v_affected_scope_count = ROW_COUNT;

  WITH affected_session_summary AS (
    SELECT
      affected_scope.session_id,
      COUNT(*)::integer AS affected_candidate_count,
      (array_agg(
        affected_scope.candidate_id
        ORDER BY affected_scope.candidate_id DESC
      ))[1] AS last_candidate_id
    FROM pg_temp.pay_workbench_dirty_fanout_affected_scope AS affected_scope
    GROUP BY affected_scope.session_id
  ),
  updated_sessions AS (
    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET progress_state = 'DIRTY',
        scope_pending_count = GREATEST(
          COALESCE(session_update.scope_pending_count, 0),
          1
        ),
        candidate_sample_rows_json = jsonb_build_array(
          jsonb_build_object(
            'candidate_id', affected_summary.last_candidate_id::text,
            'status', 'SOURCE_BUILD_PENDING',
            'reason', v_reason
          )
        ),
        progress_json = jsonb_strip_nulls(
          COALESCE(session_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'last_dirty_candidate_id', affected_summary.last_candidate_id::text,
            'last_dirty_reason', v_reason,
            'last_dirty_at_utc', v_now::text,
            'last_dirty_fanout_job_id', v_job_row.id::text,
            'last_dirty_fanout_scope_kind', v_scope_kind,
            'last_dirty_fanout_scope_id', v_scope_id::text,
            'last_dirty_fanout_candidate_count', affected_summary.affected_candidate_count
          )
        ),
        progress_counter_version = COALESCE(session_update.progress_counter_version, 0)
          + affected_summary.affected_candidate_count,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    FROM affected_session_summary AS affected_summary
    WHERE session_update.id = affected_summary.session_id
    RETURNING session_update.id
  )
  SELECT COUNT(*)::integer
  INTO v_affected_session_count
  FROM updated_sessions;

  WITH dirtied_source_rows AS (
    UPDATE public.banking_pay_workbench_candidate_source_lines AS source_line
    SET status = 'DIRTY',
        source_row_json = jsonb_strip_nulls(
          COALESCE(source_line.source_row_json, '{}'::jsonb)
          || jsonb_build_object(
            'dirty_reason', v_reason,
            'dirty_fanout_job_id', v_job_row.id::text,
            'dirty_fanout_scope_kind', v_scope_kind,
            'dirty_fanout_scope_id', v_scope_id::text,
            'dirty_trigger_table', v_trigger_table,
            'dirty_trigger_operation', v_trigger_op,
            'dirty_at_utc', v_now::text
          )
        ),
        updated_at_utc = v_now
    FROM pg_temp.pay_workbench_dirty_fanout_affected_scope AS affected_scope
    WHERE source_line.session_id = affected_scope.session_id
      AND source_line.candidate_id = affected_scope.candidate_id
      AND source_line.status = 'CURRENT'
    RETURNING source_line.id
  )
  SELECT COUNT(*)::integer
  INTO v_source_rows_marked_dirty_count
  FROM dirtied_source_rows;

  WITH upserted_jobs AS (
    INSERT INTO public.banking_pay_workbench_jobs AS refresh_job (
      job_type,
      status,
      priority,
      run_at_utc,
      attempt_count,
      max_attempts,
      dedupe_key,
      snapshot_run_id,
      session_id,
      candidate_id,
      payload_json,
      economic_build_id,
      private_stage,
      private_cursor_kind,
      private_cursor_json,
      private_stage_version,
      created_at_utc,
      updated_at_utc,
      started_at_utc,
      completed_at_utc,
      failed_at_utc,
      last_error_json
    )
    SELECT
      'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'QUEUED',
      44,
      v_now,
      0,
      8,
      'WORKBENCH_CANDIDATE_SOURCE_BUILD:'
        || open_session.id::text
        || ':'
        || affected_scope.candidate_id::text
        || ':v'
        || COALESCE(open_session.version, 0)::text
        || ':s'
        || COALESCE(fanout_candidate.source_change_seq, 0)::text
        || ':snapshot:'
        || open_session.source_snapshot_run_id::text
        || ':signature:'
        || md5(COALESCE(open_session.session_signature, ''))
        || ':source_build:'
        || source_build_identity.source_build_run_id::text
        || ':cursor:START',
      open_session.source_snapshot_run_id,
      open_session.id,
      affected_scope.candidate_id,
      jsonb_build_object(
        'trigger_table', v_trigger_table,
        'trigger_operation', v_trigger_op,
        'trigger_op', v_trigger_op,
        'scope_kind', 'CANDIDATE',
        'scope_id', affected_scope.candidate_id::text,
        'candidate_id', affected_scope.candidate_id::text,
        'reason', v_reason,
        'source_change_seq', fanout_candidate.source_change_seq,
        'source_change_sequence', fanout_candidate.source_change_seq,
        'refresh_scope_kind', 'CANDIDATE_FULL_LIVE',
        'candidate_serial_key', public._pay_workbench_candidate_serial_key(affected_scope.candidate_id),
        'candidate_serial_candidate_id', affected_scope.candidate_id::text,
        'candidate_serial_active_chain_id', COALESCE(NULLIF(BTRIM(COALESCE(v_job_row.payload_json->>'candidate_serial_active_chain_id', '')), ''), v_job_row.id::text),
        'candidate_serial_source_job_id', v_job_row.id::text,
        'candidate_serial_reason', 'CANDIDATE_SERIAL_FANOUT_SOURCE_BUILD_QUEUED'
      )
      || jsonb_build_object(
        'targeted_timesheet_ids', '[]'::jsonb,
        'linked_timesheet_ids', '[]'::jsonb,
        'session_id', open_session.id::text,
        'source_session_id', open_session.id::text,
        'session_version', COALESCE(open_session.version, 0),
        'session_signature', open_session.session_signature,
        'snapshot_run_id', open_session.source_snapshot_run_id::text,
        'source_snapshot_run_id', open_session.source_snapshot_run_id::text,
        'source_build_run_id', source_build_identity.source_build_run_id::text,
        'source_fanout_job_id', v_job_row.id::text
      )
      || jsonb_build_object(
        'fanout_scope_kind', v_scope_kind,
        'fanout_scope_id', v_scope_id::text,
        'job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'canonical_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'source_build_required', true,
        'line_work_required', true,
        'line_work_only', false,
        'source_build_action', 'BUILD_SOURCE',
        'line_work_action', 'SOURCE_BUILD',
        'pay_channel_scope', COALESCE(NULLIF(UPPER(BTRIM(COALESCE(open_session.filters_json->>'pay_channel_scope', open_session.filters_json#>>'{filters,pay_channel_scope}', ''))), ''), 'ALL'),
        'limit', v_limit,
        'source_build_limit', v_limit,
        'source_rows_marked_dirty_count', COALESCE(v_source_rows_marked_dirty_count, 0)
      ),
      NULL::uuid,
      'BUILD_INITIALISE',
      'BUILD_INITIALISE',
      '{}'::jsonb,
      1,
      v_now,
      v_now,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::jsonb
    FROM pg_temp.pay_workbench_dirty_fanout_affected_scope AS affected_scope
    JOIN public.banking_pay_workbench_sessions AS open_session
      ON open_session.id = affected_scope.session_id
     AND open_session.status = 'OPEN'
     AND open_session.discarded_at_utc IS NULL
    JOIN pg_temp.pay_workbench_dirty_fanout_candidates AS fanout_candidate
      ON fanout_candidate.candidate_id = affected_scope.candidate_id
    CROSS JOIN LATERAL (
      SELECT md5(concat_ws(':',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        open_session.source_snapshot_run_id::text,
        open_session.id::text,
        affected_scope.candidate_id::text,
        COALESCE(open_session.version, 0)::text,
        COALESCE(fanout_candidate.source_change_seq, 0)::text,
        'CANDIDATE_FULL_LIVE',
        v_scope_kind,
        v_scope_id::text,
        v_reason
      )) AS source_build_hash
    ) AS source_build_hash
    CROSS JOIN LATERAL (
      SELECT (
        substr(source_build_hash.source_build_hash, 1, 8) || '-' ||
        substr(source_build_hash.source_build_hash, 9, 4) || '-' ||
        substr(source_build_hash.source_build_hash, 13, 4) || '-' ||
        substr(source_build_hash.source_build_hash, 17, 4) || '-' ||
        substr(source_build_hash.source_build_hash, 21, 12)
      )::uuid AS source_build_run_id
    ) AS source_build_identity
    ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
    DO UPDATE
    SET priority = LEAST(refresh_job.priority, EXCLUDED.priority),
        run_at_utc = LEAST(refresh_job.run_at_utc, EXCLUDED.run_at_utc),
        payload_json = public._pay_workbench_merge_targeted_scope_payload(
          COALESCE(refresh_job.payload_json, '{}'::jsonb),
          COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
        ),
        updated_at_utc = v_now
    RETURNING refresh_job.id
  )
  SELECT COUNT(*)::integer
  INTO v_jobs_queued_or_reused
  FROM upserted_jobs;

  RETURN jsonb_build_object(
      'ok', true,
      'job_id', v_job_row.id::text,
      'job_type', v_job_row.job_type,
      'scope_kind', v_scope_kind,
      'scope_id', v_scope_id::text,
      'reason', v_reason,
      'trigger_table', v_trigger_table,
      'trigger_op', v_trigger_op,
      'limit', v_limit,
      'cursor_json', v_cursor_json,
      'candidate_ids', v_candidate_ids_json,
      'candidate_count', COALESCE(v_candidate_count, 0),
      'processed_count', COALESCE(v_candidate_count, 0),
      'affected_scope_count', COALESCE(v_affected_scope_count, 0),
      'affected_session_count', COALESCE(v_affected_session_count, 0),
      'enqueued_count', COALESCE(v_jobs_queued_or_reused, 0),
      'jobs_queued_or_reused', COALESCE(v_jobs_queued_or_reused, 0),
      'source_rows_marked_dirty_count', COALESCE(v_source_rows_marked_dirty_count, 0),
      'first_stage_job_type', 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
    )
    || jsonb_build_object(
      'has_more', COALESCE(v_has_more, false),
      'candidate_serial_delayed', COALESCE(v_gate_blocked, false),
      'candidate_serial_blocked_candidate_id', CASE WHEN v_gate_blocked_candidate_id IS NULL THEN NULL::text ELSE v_gate_blocked_candidate_id::text END,
      'next_cursor', v_next_cursor_json,
      'next_cursor_json', v_next_cursor_json,
      'last_candidate_id', CASE
        WHEN v_last_candidate_id IS NULL THEN NULL::text
        ELSE v_last_candidate_id::text
      END,
      'candidate_line_calculation_performed', false,
      'session_created', false,
      'rpc_fanout_performed', false
    );
END;
$function$;

ALTER FUNCTION public.pay_workbench_contract_client_dirty_fanout_chunk(p_job_id uuid, p_cursor_json jsonb, p_limit integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_contract_client_dirty_fanout_chunk(p_job_id uuid, p_cursor_json jsonb, p_limit integer) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_contract_client_dirty_fanout_chunk(p_job_id uuid, p_cursor_json jsonb, p_limit integer) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_contract_client_dirty_fanout_chunk(p_job_id uuid, p_cursor_json jsonb, p_limit integer) TO authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_contract_client_dirty_fanout_chunk(p_job_id uuid, p_cursor_json jsonb, p_limit integer) TO service_role;
