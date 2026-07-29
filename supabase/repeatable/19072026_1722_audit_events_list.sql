-- Keep the filtered set and its page in one SQL statement. PostgreSQL CTEs are
-- statement-scoped; the previous definition attempted to read page_rows from a
-- second statement and therefore failed every Audit screen request.

CREATE OR REPLACE FUNCTION public.audit_events_list(
  p_search text DEFAULT NULL,
  p_action text DEFAULT NULL,
  p_object_type text DEFAULT NULL,
  p_actor_display text DEFAULT NULL,
  p_limit integer DEFAULT 50,
  p_offset integer DEFAULT 0,
  p_sort_by text DEFAULT 'ts_utc',
  p_sort_dir text DEFAULT 'desc'
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_search text := NULLIF(BTRIM(p_search), '');
  v_action text := NULLIF(BTRIM(p_action), '');
  v_object_type text := NULLIF(BTRIM(p_object_type), '');
  v_actor_display text := NULLIF(BTRIM(p_actor_display), '');
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 500);
  v_offset integer := GREATEST(COALESCE(p_offset, 0), 0);
  v_sort_by text := LOWER(COALESCE(NULLIF(BTRIM(p_sort_by), ''), 'ts_utc'));
  v_sort_dir text := LOWER(COALESCE(NULLIF(BTRIM(p_sort_dir), ''), 'desc'));
  v_items jsonb := '[]'::jsonb;
  v_total bigint := 0;
BEGIN
  -- Hash-ledger deployment probe: behaviour intentionally unchanged.
  IF v_sort_by NOT IN ('ts_utc', 'action', 'actor_display', 'object_type', 'object_id_text', 'correlation_id') THEN
    v_sort_by := 'ts_utc';
  END IF;

  IF v_sort_dir NOT IN ('asc', 'desc') THEN
    v_sort_dir := 'desc';
  END IF;

  WITH filtered AS (
    SELECT
      audit_row.id,
      audit_row.ts_utc,
      audit_row.actor_user_id,
      audit_row.actor_display,
      audit_row.actor_role_at_time,
      audit_row.object_type,
      audit_row.object_id_text,
      audit_row.action,
      audit_row.before_json,
      audit_row.after_json,
      audit_row.reason,
      audit_row.ip,
      audit_row.user_agent,
      audit_row.correlation_id
    FROM public.audit_events AS audit_row
    WHERE (
      v_search IS NULL
      OR CONCAT_WS(
        ' ',
        COALESCE(audit_row.actor_display, ''),
        COALESCE(audit_row.actor_role_at_time, ''),
        COALESCE(audit_row.object_type, ''),
        COALESCE(audit_row.object_id_text, ''),
        COALESCE(audit_row.action, ''),
        COALESCE(audit_row.reason, ''),
        COALESCE(audit_row.correlation_id, '')
      ) ILIKE ('%' || v_search || '%')
    )
      AND (v_action IS NULL OR UPPER(COALESCE(audit_row.action, '')) = UPPER(v_action))
      AND (v_object_type IS NULL OR UPPER(COALESCE(audit_row.object_type, '')) = UPPER(v_object_type))
      AND (v_actor_display IS NULL OR COALESCE(audit_row.actor_display, '') ILIKE ('%' || v_actor_display || '%'))
  ), ordered_rows AS (
    SELECT
      filtered_row.*,
      ROW_NUMBER() OVER (
        ORDER BY
          CASE WHEN v_sort_by = 'ts_utc' AND v_sort_dir = 'asc' THEN filtered_row.ts_utc END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'ts_utc' AND v_sort_dir = 'desc' THEN filtered_row.ts_utc END DESC NULLS LAST,
          CASE WHEN v_sort_by = 'action' AND v_sort_dir = 'asc' THEN LOWER(filtered_row.action) END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'action' AND v_sort_dir = 'desc' THEN LOWER(filtered_row.action) END DESC NULLS LAST,
          CASE WHEN v_sort_by = 'actor_display' AND v_sort_dir = 'asc' THEN LOWER(filtered_row.actor_display) END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'actor_display' AND v_sort_dir = 'desc' THEN LOWER(filtered_row.actor_display) END DESC NULLS LAST,
          CASE WHEN v_sort_by = 'object_type' AND v_sort_dir = 'asc' THEN LOWER(filtered_row.object_type) END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'object_type' AND v_sort_dir = 'desc' THEN LOWER(filtered_row.object_type) END DESC NULLS LAST,
          CASE WHEN v_sort_by = 'object_id_text' AND v_sort_dir = 'asc' THEN LOWER(filtered_row.object_id_text) END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'object_id_text' AND v_sort_dir = 'desc' THEN LOWER(filtered_row.object_id_text) END DESC NULLS LAST,
          CASE WHEN v_sort_by = 'correlation_id' AND v_sort_dir = 'asc' THEN LOWER(filtered_row.correlation_id) END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'correlation_id' AND v_sort_dir = 'desc' THEN LOWER(filtered_row.correlation_id) END DESC NULLS LAST,
          filtered_row.ts_utc DESC,
          filtered_row.id DESC
      ) AS sort_ordinal
    FROM filtered AS filtered_row
  ), page_rows AS (
    SELECT ordered_row.*
    FROM ordered_rows AS ordered_row
    WHERE ordered_row.sort_ordinal > v_offset
      AND ordered_row.sort_ordinal <= v_offset + v_limit
  ), page_aggregate AS (
    SELECT COALESCE(
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', page_row.id,
          'ts_utc', page_row.ts_utc,
          'actor_user_id', page_row.actor_user_id,
          'actor_display', page_row.actor_display,
          'actor_role_at_time', page_row.actor_role_at_time,
          'object_type', page_row.object_type,
          'object_id_text', page_row.object_id_text,
          'action', page_row.action,
          'before_json', page_row.before_json,
          'after_json', page_row.after_json,
          'reason', page_row.reason,
          'ip', page_row.ip,
          'user_agent', page_row.user_agent,
          'correlation_id', page_row.correlation_id
        )
        ORDER BY page_row.sort_ordinal
      ),
      '[]'::jsonb
    ) AS items
    FROM page_rows AS page_row
  )
  SELECT page_aggregate.items,
         (SELECT COUNT(*)::bigint FROM filtered)
  INTO v_items, v_total
  FROM page_aggregate;

  RETURN JSONB_BUILD_OBJECT(
    'ok', true,
    'items', COALESCE(v_items, '[]'::jsonb),
    'total_count', COALESCE(v_total, 0),
    'limit', v_limit,
    'offset', v_offset,
    'sort_by', v_sort_by,
    'sort_dir', v_sort_dir
  );
END;
$function$;

ALTER FUNCTION public.audit_events_list(text, text, text, text, integer, integer, text, text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.audit_events_list(text, text, text, text, integer, integer, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.audit_events_list(text, text, text, text, integer, integer, text, text) FROM anon;
REVOKE ALL ON FUNCTION public.audit_events_list(text, text, text, text, integer, integer, text, text) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.audit_events_list(text, text, text, text, integer, integer, text, text) TO service_role;
