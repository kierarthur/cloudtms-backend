-- Banking finance audit actors and refresh-loop guard.
--
-- Policy X: these functions affect pre-draft refresh orchestration and audit
-- presentation only. They do not alter post-draft frozen batch authority,
-- economic keys, payment execution, settlement, or remittance behaviour.

CREATE OR REPLACE FUNCTION public.pay_finance_case_audit_actor_index(
  p_finance_case_id uuid
)
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
WITH case_ref AS (
  SELECT advance_row.linked_timesheet_id
  FROM public.pay_advances AS advance_row
  WHERE advance_row.id = p_finance_case_id
),
actor_events AS (
  SELECT
    finance_event.id,
    'FINANCE_CASE_EVENT'::text AS source,
    finance_event.event_type,
    finance_event.event_at_utc AS at_utc,
    finance_event.actor_user_id,
    COALESCE(
      NULLIF(BTRIM(COALESCE(actor_user.display_name, '')), ''),
      NULLIF(BTRIM(COALESCE(actor_user.email, '')), ''),
      CASE WHEN finance_event.actor_user_id IS NULL THEN 'System' ELSE 'Former user' END
    ) AS actor_display_name
  FROM public.pay_finance_case_events AS finance_event
  LEFT JOIN public.tms_users AS actor_user
    ON actor_user.id = finance_event.actor_user_id
  WHERE finance_event.finance_case_id = p_finance_case_id

  UNION ALL

  SELECT
    audit_event.id,
    'AUDIT'::text AS source,
    audit_event.action AS event_type,
    audit_event.ts_utc AS at_utc,
    audit_event.actor_user_id,
    COALESCE(
      NULLIF(BTRIM(COALESCE(audit_event.actor_display, '')), ''),
      NULLIF(BTRIM(COALESCE(actor_user.display_name, '')), ''),
      NULLIF(BTRIM(COALESCE(actor_user.email, '')), ''),
      CASE WHEN audit_event.actor_user_id IS NULL THEN 'System' ELSE 'Former user' END
    ) AS actor_display_name
  FROM public.audit_events AS audit_event
  LEFT JOIN public.tms_users AS actor_user
    ON actor_user.id = audit_event.actor_user_id
  CROSS JOIN case_ref
  WHERE (
    audit_event.object_type IN ('pay_advances', 'finance_cases')
    AND audit_event.object_id_text = p_finance_case_id::text
  )
  OR (
    case_ref.linked_timesheet_id IS NOT NULL
    AND audit_event.object_type = 'timesheets'
    AND audit_event.object_id_text = case_ref.linked_timesheet_id::text
  )

  UNION ALL

  SELECT
    snooze_row.id,
    'SNOOZE'::text AS source,
    'SNOOZE_APPLIED'::text AS event_type,
    snooze_row.created_at_utc AS at_utc,
    snooze_row.created_by_user_id AS actor_user_id,
    COALESCE(
      NULLIF(BTRIM(COALESCE(actor_user.display_name, '')), ''),
      NULLIF(BTRIM(COALESCE(actor_user.email, '')), ''),
      CASE WHEN snooze_row.created_by_user_id IS NULL THEN 'System' ELSE 'Former user' END
    ) AS actor_display_name
  FROM public.pay_item_snoozes AS snooze_row
  LEFT JOIN public.tms_users AS actor_user
    ON actor_user.id = snooze_row.created_by_user_id
  WHERE snooze_row.source_ref = ('advance:' || p_finance_case_id::text)
    AND snooze_row.created_at_utc IS NOT NULL

  UNION ALL

  SELECT
    snooze_row.id,
    'SNOOZE'::text AS source,
    'SNOOZE_CLEARED'::text AS event_type,
    snooze_row.cleared_at_utc AS at_utc,
    snooze_row.cleared_by_user_id AS actor_user_id,
    COALESCE(
      NULLIF(BTRIM(COALESCE(actor_user.display_name, '')), ''),
      NULLIF(BTRIM(COALESCE(actor_user.email, '')), ''),
      CASE WHEN snooze_row.cleared_by_user_id IS NULL THEN 'System' ELSE 'Former user' END
    ) AS actor_display_name
  FROM public.pay_item_snoozes AS snooze_row
  LEFT JOIN public.tms_users AS actor_user
    ON actor_user.id = snooze_row.cleared_by_user_id
  WHERE snooze_row.source_ref = ('advance:' || p_finance_case_id::text)
    AND snooze_row.cleared_at_utc IS NOT NULL
)
SELECT jsonb_build_object(
  'events',
  COALESCE(
    jsonb_agg(
      jsonb_strip_nulls(
        jsonb_build_object(
          'event_id', actor_event.id::text,
          'source', actor_event.source,
          'event_type', actor_event.event_type,
          'at_utc', actor_event.at_utc,
          'actor_user_id', CASE
            WHEN actor_event.actor_user_id IS NULL THEN NULL
            ELSE actor_event.actor_user_id::text
          END,
          'actor_display_name', actor_event.actor_display_name
        )
      )
      ORDER BY actor_event.at_utc, actor_event.source, actor_event.event_type, actor_event.id
    ),
    '[]'::jsonb
  )
)
FROM actor_events AS actor_event;
$function$;

ALTER FUNCTION public.pay_finance_case_audit_actor_index(uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_finance_case_audit_actor_index(uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_finance_case_audit_actor_index(uuid) TO service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_mark_finance_case_dirty()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_started_at timestamptz := clock_timestamp();
  v_now timestamptz := clock_timestamp();
  v_trigger_table text := lower(TG_TABLE_NAME);
  v_new_row jsonb := '{}'::jsonb;
  v_old_row jsonb := '{}'::jsonb;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  v_old_finance_case_id uuid := NULL::uuid;
  v_new_finance_case_id uuid := NULL::uuid;
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_id uuid;
  v_candidate_id uuid;
  v_scope_id text;
  v_reason text;
  v_payload_json jsonb := '{}'::jsonb;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_jobs_queued integer := 0;
BEGIN
  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', NULL::text, jsonb_build_object('function_name', 'pay_workbench_mark_finance_case_dirty', 'stage', 'entry', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'queue_class', 'DIRTY_TRIGGER_PRIORITY'));

  IF TG_OP <> 'DELETE' THEN v_new_row := to_jsonb(NEW); END IF;
  IF TG_OP <> 'INSERT' THEN v_old_row := to_jsonb(OLD); END IF;

  -- SYNC_SKIPPED is an informational audit result produced by a refresh. It is
  -- not a new finance mutation and must not enqueue another refresh of itself.
  IF v_trigger_table = 'pay_finance_case_events'
     AND UPPER(BTRIM(COALESCE(v_new_row->>'event_type', v_old_row->>'event_type', ''))) = 'SYNC_SKIPPED' THEN
    IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_old_row->>'finance_case_id', v_old_row->>'id', '')), '') ~* v_uuid_re THEN
    v_old_finance_case_id := NULLIF(BTRIM(COALESCE(v_old_row->>'finance_case_id', v_old_row->>'id', '')), '')::uuid;
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_new_row->>'finance_case_id', v_new_row->>'id', '')), '') ~* v_uuid_re THEN
    v_new_finance_case_id := NULLIF(BTRIM(COALESCE(v_new_row->>'finance_case_id', v_new_row->>'id', '')), '')::uuid;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT finance_cases.finance_case_id ORDER BY finance_cases.finance_case_id), ARRAY[]::uuid[])
  INTO v_finance_case_ids
  FROM (
    SELECT v_old_finance_case_id AS finance_case_id
    UNION ALL
    SELECT v_new_finance_case_id AS finance_case_id
  ) AS finance_cases
  WHERE finance_cases.finance_case_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT candidates.candidate_id ORDER BY candidates.candidate_id), ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM (
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid ELSE NULL::uuid END AS candidate_id
    UNION ALL
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid ELSE NULL::uuid END AS candidate_id
    UNION ALL
    SELECT component_old.candidate_id FROM public.pay_finance_case_components AS component_old WHERE component_old.finance_case_id = v_old_finance_case_id
    UNION ALL
    SELECT component_new.candidate_id FROM public.pay_finance_case_components AS component_new WHERE component_new.finance_case_id = v_new_finance_case_id
    UNION ALL
    SELECT advance_old.candidate_id FROM public.pay_advances AS advance_old WHERE advance_old.id = v_old_finance_case_id
    UNION ALL
    SELECT advance_new.candidate_id FROM public.pay_advances AS advance_new WHERE advance_new.id = v_new_finance_case_id
  ) AS candidates
  WHERE candidates.candidate_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT timesheets.timesheet_id ORDER BY timesheets.timesheet_id), ARRAY[]::uuid[])
  INTO v_targeted_timesheet_ids
  FROM (
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_old_row->>'linked_timesheet_id', v_old_row->>'timesheet_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_old_row->>'linked_timesheet_id', v_old_row->>'timesheet_id', '')), '')::uuid ELSE NULL::uuid END AS timesheet_id
    UNION ALL
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_new_row->>'linked_timesheet_id', v_new_row->>'timesheet_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_new_row->>'linked_timesheet_id', v_new_row->>'timesheet_id', '')), '')::uuid ELSE NULL::uuid END AS timesheet_id
  ) AS timesheets
  WHERE timesheets.timesheet_id IS NOT NULL;

  v_reason := 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP;

  IF COALESCE(array_length(v_finance_case_ids, 1), 0) > 0 THEN
    FOREACH v_finance_case_id IN ARRAY v_finance_case_ids
    LOOP
      v_payload_json := jsonb_build_object(
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'trigger_operation', TG_OP,
        'scope_kind', 'FINANCE_CASE',
        'scope_id', v_finance_case_id::text,
        'finance_case_id', v_finance_case_id::text,
        'finance_case_ids', to_jsonb(v_finance_case_ids),
        'candidate_ids', to_jsonb(v_candidate_ids),
        'targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
        'reason', v_reason,
        'dirty_reason', v_reason,
        'refresh_scope_kind', CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0 THEN 'TARGETED_TIMESHEETS' ELSE 'CANDIDATE_FULL_LIVE' END,
        'force_legacy', true,
        'projection_class', 'FINANCE_CASE',
        'fallback_reason', 'FINANCE_CASE_DIRTY_TRIGGER',
        'source_build_required', true,
        'line_work_required', true,
        'row_backed_scope_required', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      );

      SELECT public.pay_workbench_dirty_event_enqueue(
        p_job_type => 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
        p_scope_kind => 'FINANCE_CASE',
        p_scope_id => v_finance_case_id::text,
        p_candidate_id => NULL::uuid,
        p_targeted_timesheet_ids => v_targeted_timesheet_ids,
        p_linked_timesheet_ids => ARRAY[]::uuid[],
        p_payload_json => v_payload_json,
        p_reason => v_reason,
        p_priority => -1000,
        p_run_at_utc => v_now
      )
      INTO v_enqueue_result;
      v_jobs_queued := v_jobs_queued + 1;
    END LOOP;
  ELSE
    FOREACH v_candidate_id IN ARRAY v_candidate_ids
    LOOP
      v_scope_id := v_candidate_id::text;
      v_payload_json := jsonb_build_object(
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'trigger_operation', TG_OP,
        'scope_kind', 'CANDIDATE',
        'scope_id', v_scope_id,
        'candidate_id', v_scope_id,
        'candidate_ids', to_jsonb(ARRAY[v_candidate_id]),
        'targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
        'reason', v_reason,
        'dirty_reason', v_reason,
        'refresh_scope_kind', CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0 THEN 'TARGETED_TIMESHEETS' ELSE 'CANDIDATE_FULL_LIVE' END,
        'projection_class', 'FINANCE_CASE',
        'fallback_reason', 'FINANCE_CASE_DIRTY_TRIGGER_NO_CASE_ID',
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      );

      SELECT public.pay_workbench_dirty_event_enqueue(
        p_job_type => 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        p_scope_kind => 'CANDIDATE',
        p_scope_id => v_scope_id,
        p_candidate_id => v_candidate_id,
        p_targeted_timesheet_ids => v_targeted_timesheet_ids,
        p_linked_timesheet_ids => ARRAY[]::uuid[],
        p_payload_json => v_payload_json,
        p_reason => v_reason,
        p_priority => -1000,
        p_run_at_utc => v_now
      )
      INTO v_enqueue_result;
      v_jobs_queued := v_jobs_queued + 1;
    END LOOP;
  END IF;

  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', NULL::text, jsonb_build_object('function_name', 'pay_workbench_mark_finance_case_dirty', 'stage', 'return_enqueued_dirty_priority', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'finance_case_count', COALESCE(array_length(v_finance_case_ids, 1), 0), 'candidate_count', COALESCE(array_length(v_candidate_ids, 1), 0), 'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0), 'jobs_queued', v_jobs_queued, 'queue_class', 'DIRTY_TRIGGER_PRIORITY', 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));

  IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$function$;

ALTER FUNCTION public.pay_workbench_mark_finance_case_dirty() OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_mark_finance_case_dirty() FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_mark_finance_case_dirty() TO service_role;
