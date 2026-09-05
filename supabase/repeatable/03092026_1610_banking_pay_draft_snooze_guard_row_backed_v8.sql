-- Row-backed transport adapter for the existing live Draft snooze authority.
-- Runtime authority is Miget TEST.  The `supabase` directory name is historical.
--
-- Policy is deliberately unchanged: an active snooze matching any selected
-- constituent still blocks item creation, the same per-Candidate advisory lock
-- is used by snooze upsert/clear, and legacy array-backed operations continue to
-- call the established public owner byte-for-byte.  V8 changes only how the
-- already-frozen selected identities are read: normalized rows replace the
-- removed candidate-scope JSON arrays and the common no-snooze path never
-- materialises the complete selected set.

CREATE OR REPLACE FUNCTION private.pay_workbench_operation_active_snoozes_v8(
  p_operation_id uuid,
  p_workbench_session_id uuid,
  p_candidate_scope_ids jsonb DEFAULT NULL::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
VOLATILE
SET search_path TO ''
AS $function$
DECLARE
  v_today_uk date := (pg_catalog.clock_timestamp() AT TIME ZONE 'Europe/London')::date;
  v_scope_ids jsonb := CASE
    WHEN pg_catalog.jsonb_typeof(COALESCE(p_candidate_scope_ids, '[]'::jsonb)) = 'array'
      THEN COALESCE(p_candidate_scope_ids, '[]'::jsonb)
    ELSE '[]'::jsonb
  END;
  v_scope_filter_count integer := 0;
  v_is_v8_operation boolean := false;
  v_v8_scope_count integer := 0;
  v_selected_line_count integer := 0;
  v_active_snooze_count integer := 0;
  v_candidate_id uuid;
  v_snoozes jsonb := '[]'::jsonb;
  v_preview_row_ids jsonb := '[]'::jsonb;
  v_timesheet_ids jsonb := '[]'::jsonb;
  v_segment_stable_keys jsonb := '[]'::jsonb;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_SNOOZE_GUARD_OPERATION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'BANKING_PAY_DRAFT_SNOOZE_GUARD_OPERATION_REQUIRED')::text;
  END IF;
  IF p_workbench_session_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_SNOOZE_GUARD_SESSION_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'BANKING_PAY_DRAFT_SNOOZE_GUARD_SESSION_REQUIRED')::text;
  END IF;
  IF NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
      AND operation_row.workbench_session_id = p_workbench_session_id
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(operation_row.operation_type, ''))) = 'DRAFT_CREATE'
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_SNOOZE_GUARD_OPERATION_SESSION_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'BANKING_PAY_DRAFT_SNOOZE_GUARD_OPERATION_SESSION_MISMATCH',
              'operation_id', p_operation_id,
              'workbench_session_id', p_workbench_session_id)::text;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS certificate_scope
    WHERE certificate_scope.operation_id = p_operation_id
      AND certificate_scope.freeze_state = 'FROZEN'
  )
  INTO v_is_v8_operation;

  IF NOT v_is_v8_operation THEN
    -- The adapter is intentionally invisible to every pre-V8 operation.
    RETURN public.pay_workbench_operation_active_snoozes_v1(
      p_operation_id, p_workbench_session_id, v_scope_ids);
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_scope_filter_count
  FROM pg_catalog.jsonb_array_elements_text(v_scope_ids) AS scope_id(value)
  WHERE NULLIF(pg_catalog.btrim(scope_id.value), '') IS NOT NULL;

  IF v_scope_filter_count NOT BETWEEN 1 AND 100
     OR EXISTS (
       SELECT 1
       FROM pg_catalog.jsonb_array_elements_text(v_scope_ids) AS scope_id(value)
       WHERE NULLIF(pg_catalog.btrim(scope_id.value), '') IS NULL
          OR pg_catalog.btrim(scope_id.value) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     )
     OR v_scope_filter_count IS DISTINCT FROM (
       SELECT pg_catalog.count(DISTINCT pg_catalog.btrim(scope_id.value))::integer
       FROM pg_catalog.jsonb_array_elements_text(v_scope_ids) AS scope_id(value)
     ) THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_SNOOZE_GUARD_SCOPE_INVALID'
      USING ERRCODE = '22023',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'BANKING_PAY_DRAFT_SNOOZE_GUARD_SCOPE_INVALID',
              'scope_count', v_scope_filter_count,
              'maximum_scope_count', 100)::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_v8_draft_snooze_candidate_scopes;
  CREATE TEMPORARY TABLE _bpay_v8_draft_snooze_candidate_scopes
  ON COMMIT DROP AS
  SELECT public_scope.id AS candidate_scope_id,
         public_scope.candidate_id,
         frozen_scope.candidate_scope_ordinal,
         frozen_scope.constituent_count
  FROM public.banking_pay_operation_candidate_scope AS public_scope
  JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
    ON frozen_scope.operation_id = public_scope.operation_id
   AND frozen_scope.candidate_id = public_scope.candidate_id
   AND frozen_scope.resolved_pay_channel = public_scope.pay_channel
   AND frozen_scope.scope_digest_sha256 = public_scope.scope_hash
   AND frozen_scope.scope_state IN ('FROZEN', 'BATCH_LINKED', 'COMPLETE')
  JOIN private.banking_pay_draft_frozen_certificate_scopes_v8 AS certificate_scope
    ON certificate_scope.operation_id = frozen_scope.operation_id
   AND certificate_scope.freeze_state = 'FROZEN'
  WHERE public_scope.operation_id = p_operation_id
    AND public_scope.workbench_session_id = p_workbench_session_id
    AND public_scope.id IN (
      SELECT pg_catalog.btrim(scope_id.value)::uuid
      FROM pg_catalog.jsonb_array_elements_text(v_scope_ids) AS scope_id(value)
    );

  SELECT pg_catalog.count(*)::integer,
         COALESCE(pg_catalog.sum(candidate_scope.constituent_count), 0)::integer
  INTO v_v8_scope_count, v_selected_line_count
  FROM pg_temp._bpay_v8_draft_snooze_candidate_scopes AS candidate_scope;

  IF v_v8_scope_count IS DISTINCT FROM v_scope_filter_count THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_SNOOZE_GUARD_SCOPE_MIXED_OR_INCOMPLETE'
      USING ERRCODE = '55000',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'BANKING_PAY_DRAFT_SNOOZE_GUARD_SCOPE_MIXED_OR_INCOMPLETE',
              'expected_scope_count', v_scope_filter_count,
              'row_backed_scope_count', v_v8_scope_count)::text;
  END IF;
  IF v_selected_line_count NOT BETWEEN 1 AND 50000
     OR EXISTS (
       SELECT 1
       FROM pg_temp._bpay_v8_draft_snooze_candidate_scopes AS candidate_scope
       LEFT JOIN LATERAL (
         SELECT pg_catalog.count(*)::integer AS member_count
         FROM private.banking_pay_draft_frozen_candidate_scope_members_v8 AS member
         WHERE member.operation_id = p_operation_id
           AND member.candidate_scope_ordinal = candidate_scope.candidate_scope_ordinal
       ) AS member_count ON true
       WHERE member_count.member_count IS DISTINCT FROM candidate_scope.constituent_count
     ) THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_SNOOZE_GUARD_FROZEN_SCOPE_INCOMPLETE'
      USING ERRCODE = '55000',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'BANKING_PAY_DRAFT_SNOOZE_GUARD_FROZEN_SCOPE_INCOMPLETE',
              'selected_line_count', v_selected_line_count)::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_draft_snooze_guard_scope;
  CREATE TEMPORARY TABLE _bpay_draft_snooze_guard_scope (
    candidate_scope_id uuid NOT NULL,
    preview_row_id uuid,
    candidate_id uuid NOT NULL,
    timesheet_id uuid,
    booking_id text,
    segment_id text,
    segment_stable_key text,
    source_ref text
  ) ON COMMIT DROP;

  -- One row per frozen Candidate scope is sufficient for the item owner's
  -- exact scope-coverage assertion.  Selected identities are consulted below
  -- only when an active snooze exists for that Candidate.
  INSERT INTO pg_temp._bpay_draft_snooze_guard_scope(
    candidate_scope_id, candidate_id
  )
  SELECT candidate_scope.candidate_scope_id, candidate_scope.candidate_id
  FROM pg_temp._bpay_v8_draft_snooze_candidate_scopes AS candidate_scope
  ORDER BY candidate_scope.candidate_scope_id;

  FOR v_candidate_id IN
    SELECT DISTINCT candidate_scope.candidate_id
    FROM pg_temp._bpay_v8_draft_snooze_candidate_scopes AS candidate_scope
    ORDER BY candidate_scope.candidate_id
  LOOP
    -- This is the exact advisory lock used by the established snooze upsert
    -- and clear owners.  No new lock class or wider lock scope is introduced.
    PERFORM pg_catalog.pg_advisory_xact_lock(
      pg_catalog.hashtext('public.banking_pay_candidate_snooze_guard'),
      pg_catalog.hashtext(v_candidate_id::text)
    );
  END LOOP;

  DROP TABLE IF EXISTS pg_temp._bpay_draft_active_snoozes;
  CREATE TEMPORARY TABLE _bpay_draft_active_snoozes
  ON COMMIT DROP AS
  WITH active_snooze AS MATERIALIZED (
    SELECT snooze_row.*
    FROM public.pay_item_snoozes AS snooze_row
    WHERE snooze_row.candidate_id IN (
      SELECT candidate_scope.candidate_id
      FROM pg_temp._bpay_v8_draft_snooze_candidate_scopes AS candidate_scope
    )
      AND snooze_row.cleared_at_utc IS NULL
      AND snooze_row.cancelled_at_utc IS NULL
      AND (snooze_row.snooze_until_date IS NULL OR snooze_row.snooze_until_date >= v_today_uk)
  ), selected_identity AS MATERIALIZED (
    SELECT candidate_scope.candidate_scope_id,
           payload.preview_row_id,
           payload.candidate_id,
           COALESCE(payload.timesheet_id,
             CASE WHEN NULLIF(pg_catalog.btrim(COALESCE(
               payload.payload_json->>'timesheet_id', payload.payload_json->>'timesheetId',
               payload.payload_json->>'source_timesheet_id', payload.payload_json->>'sourceTimesheetId', '')), '')
                 ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
               THEN NULLIF(pg_catalog.btrim(COALESCE(
                 payload.payload_json->>'timesheet_id', payload.payload_json->>'timesheetId',
                 payload.payload_json->>'source_timesheet_id', payload.payload_json->>'sourceTimesheetId', '')), '')::uuid
               ELSE NULL::uuid END) AS timesheet_id,
           COALESCE(NULLIF(pg_catalog.btrim(COALESCE(
             payload.payload_json->>'booking_id', payload.payload_json->>'bookingId',
             payload.payload_json#>>'{timesheet,booking_id}', '')), ''),
             NULLIF(pg_catalog.btrim(COALESCE(timesheet_row.booking_id, '')), '')) AS booking_id,
           NULLIF(pg_catalog.btrim(COALESCE(
             payload.payload_json->>'segment_id', payload.payload_json->>'segmentId',
             payload.payload_json->>'segment_identity', payload.payload_json->>'seg_identity',
             payload.payload_json#>>'{segment,id}', '')), '') AS segment_id,
           NULLIF(pg_catalog.btrim(COALESCE(
             payload.payload_json->>'segment_stable_key', payload.payload_json->>'segmentStableKey',
             payload.payload_json#>>'{segment,stable_key}',
             payload.payload_json#>>'{snooze_identity,segment_stable_key}', '')), '') AS segment_stable_key,
           NULLIF(pg_catalog.btrim(COALESCE(
             payload.payload_json->>'source_ref', payload.payload_json->>'sourceRef',
             CASE WHEN payload.finance_case_id IS NOT NULL
               THEN 'advance:' || payload.finance_case_id::text ELSE NULL END, '')), '') AS source_ref
    FROM pg_temp._bpay_v8_draft_snooze_candidate_scopes AS candidate_scope
    JOIN private.banking_pay_draft_frozen_candidate_scope_members_v8 AS member
      ON member.operation_id = p_operation_id
     AND member.candidate_scope_ordinal = candidate_scope.candidate_scope_ordinal
    JOIN private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
      ON payload.operation_id = member.operation_id
     AND payload.constituent_ordinal = member.constituent_ordinal
     AND payload.candidate_id = candidate_scope.candidate_id
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id = payload.timesheet_id
    WHERE EXISTS (SELECT 1 FROM active_snooze)
  )
  SELECT DISTINCT
    selected_identity.candidate_scope_id,
    selected_identity.preview_row_id,
    selected_identity.candidate_id,
    selected_identity.timesheet_id,
    selected_identity.segment_stable_key AS selected_segment_stable_key,
    selected_identity.source_ref AS selected_source_ref,
    snooze_row.id AS snooze_id,
    snooze_row.snooze_kind,
    snooze_row.snooze_until_date,
    snooze_row.timesheet_id AS snooze_timesheet_id,
    snooze_row.booking_id AS snooze_booking_id,
    snooze_row.segment_id AS snooze_segment_id,
    snooze_row.segment_stable_key AS snooze_segment_stable_key,
    snooze_row.source_ref AS snooze_source_ref,
    CASE WHEN selected_identity.source_ref IS NOT NULL
      THEN 'EXACT_SOURCE_REF' ELSE 'LEGACY_TIMESHEET_SEGMENT' END AS match_scope
  FROM selected_identity
  JOIN active_snooze AS snooze_row
    ON snooze_row.candidate_id = selected_identity.candidate_id
   AND (
     (selected_identity.source_ref IS NOT NULL
       AND pg_catalog.lower(NULLIF(pg_catalog.btrim(COALESCE(snooze_row.source_ref, '')), ''))
           = pg_catalog.lower(selected_identity.source_ref))
     OR (
       selected_identity.source_ref IS NULL
       AND snooze_row.source_ref IS NULL
       AND (
         (selected_identity.timesheet_id IS NOT NULL AND snooze_row.timesheet_id = selected_identity.timesheet_id)
         OR (selected_identity.booking_id IS NOT NULL
           AND NULLIF(pg_catalog.btrim(COALESCE(snooze_row.booking_id, '')), '') = selected_identity.booking_id)
       )
       AND (
         (snooze_row.segment_stable_key IS NULL AND snooze_row.segment_id IS NULL)
         OR (selected_identity.segment_stable_key IS NOT NULL
           AND NULLIF(pg_catalog.btrim(COALESCE(snooze_row.segment_stable_key, '')), '') = selected_identity.segment_stable_key)
         OR (selected_identity.segment_stable_key IS NULL AND selected_identity.segment_id IS NOT NULL
           AND NULLIF(pg_catalog.btrim(COALESCE(snooze_row.segment_id, '')), '') = selected_identity.segment_id)
       )
     )
   );

  SELECT pg_catalog.count(*)::integer
  INTO v_active_snooze_count
  FROM pg_temp._bpay_draft_active_snoozes;

  SELECT COALESCE(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
      'snooze_id', active_snooze.snooze_id,
      'candidate_scope_id', active_snooze.candidate_scope_id,
      'preview_row_id', active_snooze.preview_row_id,
      'candidate_id', active_snooze.candidate_id,
      'timesheet_id', active_snooze.timesheet_id,
      'segment_stable_key', active_snooze.selected_segment_stable_key,
      'snooze_kind', active_snooze.snooze_kind,
      'snooze_until_date', active_snooze.snooze_until_date,
      'indefinite', active_snooze.snooze_until_date IS NULL,
      'source_ref', active_snooze.selected_source_ref,
      'snooze_source_ref', active_snooze.snooze_source_ref,
      'source_match_scope', active_snooze.match_scope,
      'identity_type', CASE
        WHEN active_snooze.selected_source_ref ~* '^timesheet-expense:' THEN 'TIMESHEET_EXPENSE'
        WHEN active_snooze.selected_source_ref IS NOT NULL THEN 'EXACT_SOURCE'
        WHEN active_snooze.selected_segment_stable_key IS NOT NULL THEN 'TIMESHEET_SEGMENT'
        ELSE 'TIMESHEET' END
    )) ORDER BY active_snooze.candidate_id, active_snooze.timesheet_id, active_snooze.snooze_id), '[]'::jsonb)
  INTO v_snoozes
  FROM pg_temp._bpay_draft_active_snoozes AS active_snooze;

  SELECT COALESCE(pg_catalog.jsonb_agg(value ORDER BY value), '[]'::jsonb)
  INTO v_preview_row_ids
  FROM (SELECT DISTINCT active_snooze.preview_row_id::text AS value
        FROM pg_temp._bpay_draft_active_snoozes AS active_snooze
        WHERE active_snooze.preview_row_id IS NOT NULL) AS preview_values;
  SELECT COALESCE(pg_catalog.jsonb_agg(value ORDER BY value), '[]'::jsonb)
  INTO v_timesheet_ids
  FROM (SELECT DISTINCT active_snooze.timesheet_id::text AS value
        FROM pg_temp._bpay_draft_active_snoozes AS active_snooze
        WHERE active_snooze.timesheet_id IS NOT NULL) AS timesheet_values;
  SELECT COALESCE(pg_catalog.jsonb_agg(value ORDER BY value), '[]'::jsonb)
  INTO v_segment_stable_keys
  FROM (SELECT DISTINCT active_snooze.selected_segment_stable_key AS value
        FROM pg_temp._bpay_draft_active_snoozes AS active_snooze
        WHERE active_snooze.selected_segment_stable_key IS NOT NULL) AS segment_values;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id,
    'workbench_session_id', p_workbench_session_id,
    'london_current_date', v_today_uk,
    'selected_line_count', v_selected_line_count,
    'active_snooze_count', v_active_snooze_count,
    'affected_preview_row_ids', v_preview_row_ids,
    'affected_timesheet_ids', v_timesheet_ids,
    'affected_segment_stable_keys', v_segment_stable_keys,
    'snoozes', v_snoozes,
    'active_snoozes', v_snoozes,
    'refresh_required', v_active_snooze_count > 0,
    'next_action', CASE WHEN v_active_snooze_count > 0 THEN 'REFRESH_WORKBENCH' ELSE 'CONTINUE_DRAFT' END,
    'row_backed_transport_v8', true
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_operation_active_snoozes_v8(uuid,uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_operation_active_snoozes_v8(uuid,uuid,jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
