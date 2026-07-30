CREATE OR REPLACE FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(
  p_session_id uuid DEFAULT NULL::uuid,
  p_candidate_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 10,
  p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone,
  p_reason text DEFAULT 'PENDING_SCOPE_OWNER_REPAIR'::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 10), 1), 25);
  v_reason text := COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), 'PENDING_SCOPE_OWNER_REPAIR');
  v_candidate record;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_owner public.banking_pay_workbench_jobs%ROWTYPE;
  v_success public.banking_pay_workbench_jobs%ROWTYPE;
  v_success_run_id uuid := NULL::uuid;
  v_success_source_change_seq bigint := 0;
  v_active public.banking_pay_workbench_jobs%ROWTYPE;
  v_live_change_seq bigint := 0;
  v_owner_canonical_type text := NULL::text;
  v_owner_valid boolean := false;
  v_owner_reason text := NULL::text;
  v_enqueue_payload jsonb := '{}'::jsonb;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_success_result jsonb := '{}'::jsonb;
  v_successor_job_id uuid := NULL::uuid;
  v_successor_run_id_text text := NULL::text;
  v_successor_valid boolean := false;
  v_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_targeted_timesheet_ids jsonb := '[]'::jsonb;
  v_linked_timesheet_ids jsonb := '[]'::jsonb;
  v_pay_channel_scope text := 'ALL';
  v_repaired_count integer := 0;
  v_reconciled_count integer := 0;
  v_rebound_count integer := 0;
  v_enqueued_count integer := 0;
  v_failed_closed_count integer := 0;
  v_skipped_count integer := 0;
  v_result_rows jsonb := '[]'::jsonb;
  v_action text := NULL::text;
  v_safe_error_code text := NULL::text;
  v_safe_error_message text := NULL::text;
  v_audit_failed boolean := false;
BEGIN
  FOR v_candidate IN
    SELECT scope_row.session_id, scope_row.candidate_id
    FROM public.banking_pay_workbench_session_scope AS scope_row
    JOIN public.banking_pay_workbench_sessions AS session_row
      ON session_row.id = scope_row.session_id
    LEFT JOIN public.banking_pay_workbench_jobs AS owner_job
      ON owner_job.id = scope_row.pending_job_id
    WHERE UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
      AND session_row.discarded_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) = 'SOURCE_BUILD_PENDING'
      AND (p_session_id IS NULL OR scope_row.session_id = p_session_id)
      AND (p_candidate_id IS NULL OR scope_row.candidate_id = p_candidate_id)
      AND (
        scope_row.pending_job_id IS NULL
        OR owner_job.id IS NULL
        OR UPPER(BTRIM(COALESCE(owner_job.status, ''))) NOT IN ('QUEUED', 'RUNNING')
        OR owner_job.session_id IS DISTINCT FROM scope_row.session_id
        OR owner_job.candidate_id IS DISTINCT FROM scope_row.candidate_id
        OR (
          CASE
            WHEN UPPER(BTRIM(COALESCE(owner_job.job_type, ''))) IN (
              'WORKBENCH_CANDIDATE_SOURCE_BUILD',
              'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
              'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
              'CANDIDATE_SOURCE_BUILD',
              'CANDIDATE_SOURCE_BUILD_CHUNK',
              'SOURCE_BUILD',
              'SOURCE_BUILD_PAGE'
            ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
            ELSE UPPER(BTRIM(COALESCE(owner_job.job_type, '')))
          END
        ) <> 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        OR CASE
             WHEN COALESCE(owner_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
               THEN (owner_job.payload_json->>'session_version')::bigint
             ELSE NULL::bigint
           END IS DISTINCT FROM session_row.version
        OR CASE
             WHEN COALESCE(owner_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
               THEN (owner_job.payload_json->>'source_change_seq')::bigint
             ELSE NULL::bigint
           END IS NULL
        OR COALESCE(owner_job.payload_json->>'source_build_run_id', '') !~*
           '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      )
    ORDER BY scope_row.updated_at_utc ASC NULLS FIRST, scope_row.session_id, scope_row.candidate_id
    LIMIT v_limit
  LOOP
    v_action := NULL::text;
    v_safe_error_code := NULL::text;
    v_safe_error_message := NULL::text;
    v_successor_job_id := NULL::uuid;
    v_successor_run_id_text := NULL::text;
    v_successor_valid := false;
    v_audit_failed := false;

    SELECT session_row.*
    INTO v_session
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_candidate.session_id
    FOR UPDATE;

    IF NOT FOUND
       OR UPPER(BTRIM(COALESCE(v_session.status, ''))) <> 'OPEN'
       OR v_session.discarded_at_utc IS NOT NULL THEN
      v_skipped_count := v_skipped_count + 1;
      CONTINUE;
    END IF;

    SELECT scope_row.*
    INTO v_scope
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = v_candidate.session_id
      AND scope_row.candidate_id = v_candidate.candidate_id
    FOR UPDATE;

    IF NOT FOUND OR UPPER(BTRIM(COALESCE(v_scope.status, ''))) <> 'SOURCE_BUILD_PENDING' THEN
      v_skipped_count := v_skipped_count + 1;
      CONTINUE;
    END IF;

    v_live_change_seq := 0;
    SELECT COALESCE(change_counter.seq, 0)
    INTO v_live_change_seq
    FROM public.app_change_counters AS change_counter
    WHERE change_counter.entity_key = 'pay_candidate:' || v_scope.candidate_id::text;
    v_live_change_seq := COALESCE(v_live_change_seq, 0);

    v_owner := NULL::public.banking_pay_workbench_jobs;
    IF v_scope.pending_job_id IS NOT NULL THEN
      SELECT owner_job.*
      INTO v_owner
      FROM public.banking_pay_workbench_jobs AS owner_job
      WHERE owner_job.id = v_scope.pending_job_id
      FOR UPDATE;
    END IF;

    v_owner_canonical_type := CASE
      WHEN UPPER(BTRIM(COALESCE(v_owner.job_type, ''))) IN (
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
        'CANDIDATE_SOURCE_BUILD',
        'CANDIDATE_SOURCE_BUILD_CHUNK',
        'SOURCE_BUILD',
        'SOURCE_BUILD_PAGE'
      ) THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      ELSE UPPER(BTRIM(COALESCE(v_owner.job_type, '')))
    END;

    v_owner_valid := v_owner.id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(v_owner.status, ''))) IN ('QUEUED', 'RUNNING')
      AND v_owner.session_id = v_scope.session_id
      AND v_owner.candidate_id = v_scope.candidate_id
      AND v_owner_canonical_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND CASE
            WHEN COALESCE(v_owner.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
              THEN (v_owner.payload_json->>'session_version')::bigint
            ELSE NULL::bigint
          END = v_session.version
      AND CASE
            WHEN COALESCE(v_owner.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (v_owner.payload_json->>'source_change_seq')::bigint
            ELSE NULL::bigint
          END >= v_live_change_seq
      AND COALESCE(v_owner.payload_json->>'source_build_run_id', '') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    IF v_owner_valid THEN
      v_skipped_count := v_skipped_count + 1;
      CONTINUE;
    END IF;

    v_owner_reason := CASE
      WHEN v_scope.pending_job_id IS NULL THEN 'PENDING_JOB_ID_MISSING'
      WHEN v_owner.id IS NULL THEN 'PENDING_JOB_ROW_MISSING'
      WHEN UPPER(BTRIM(COALESCE(v_owner.status, ''))) NOT IN ('QUEUED', 'RUNNING') THEN 'PENDING_JOB_TERMINAL'
      WHEN v_owner.session_id IS DISTINCT FROM v_scope.session_id
        OR v_owner.candidate_id IS DISTINCT FROM v_scope.candidate_id THEN 'PENDING_JOB_SCOPE_MISMATCH'
      WHEN v_owner_canonical_type <> 'WORKBENCH_CANDIDATE_SOURCE_BUILD' THEN 'PENDING_JOB_TYPE_MISMATCH'
      WHEN COALESCE(v_owner.payload_json->>'source_build_run_id', '') !~*
           '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN 'PENDING_JOB_RUN_ID_INVALID'
      WHEN COALESCE(v_owner.payload_json->>'session_version', '') !~ '^[0-9]{1,18}$'
        OR (v_owner.payload_json->>'session_version')::bigint IS DISTINCT FROM v_session.version
        THEN 'PENDING_JOB_SESSION_VERSION_STALE'
      ELSE 'PENDING_JOB_SOURCE_CHANGE_SEQ_STALE'
    END;

    v_success := NULL::public.banking_pay_workbench_jobs;
    SELECT successful_job.*
    INTO v_success
    FROM public.banking_pay_workbench_jobs AS successful_job
    WHERE successful_job.session_id = v_scope.session_id
      AND successful_job.candidate_id = v_scope.candidate_id
      AND UPPER(BTRIM(COALESCE(successful_job.status, ''))) = 'SUCCEEDED'
      AND UPPER(BTRIM(COALESCE(successful_job.job_type, ''))) IN (
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
        'CANDIDATE_SOURCE_BUILD',
        'CANDIDATE_SOURCE_BUILD_CHUNK',
        'SOURCE_BUILD',
        'SOURCE_BUILD_PAGE'
      )
      AND CASE
            WHEN COALESCE(successful_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
              THEN (successful_job.payload_json->>'session_version')::bigint
            ELSE NULL::bigint
          END = v_session.version
      AND CASE
            WHEN COALESCE(successful_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (successful_job.payload_json->>'source_change_seq')::bigint
            ELSE NULL::bigint
          END >= v_live_change_seq
      AND COALESCE(successful_job.payload_json->>'source_build_run_id', '') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_candidate_source_lines AS successful_source
        WHERE successful_source.session_id = v_scope.session_id
          AND successful_source.candidate_id = v_scope.candidate_id
          AND successful_source.source_build_run_id::text = successful_job.payload_json->>'source_build_run_id'
          AND successful_source.session_version = v_session.version
          AND successful_source.source_change_seq >= v_live_change_seq
          AND UPPER(BTRIM(COALESCE(successful_source.status, ''))) IN ('CURRENT', 'DIRTY')
      )
    ORDER BY (successful_job.payload_json->>'source_change_seq')::bigint DESC,
             successful_job.completed_at_utc DESC NULLS LAST,
             successful_job.id DESC
    LIMIT 1;

    IF FOUND THEN
      v_success_run_id := (v_success.payload_json->>'source_build_run_id')::uuid;
      v_success_source_change_seq := (v_success.payload_json->>'source_change_seq')::bigint;
      v_success_result := public.pay_workbench_reconcile_successful_source_build(
        p_session_id => v_scope.session_id,
        p_candidate_id => v_scope.candidate_id,
        p_source_build_run_id => v_success_run_id,
        p_source_change_seq => v_success_source_change_seq,
        p_session_version => v_session.version,
        p_success_job_id => v_success.id,
        p_refresh_scope_kind => COALESCE(NULLIF(BTRIM(v_success.payload_json->>'refresh_scope_kind'), ''), 'CANDIDATE_FULL_LIVE'),
        p_targeted_timesheet_ids => CASE WHEN jsonb_typeof(v_success.payload_json->'targeted_timesheet_ids') = 'array' THEN v_success.payload_json->'targeted_timesheet_ids' ELSE '[]'::jsonb END,
        p_linked_timesheet_ids => CASE WHEN jsonb_typeof(v_success.payload_json->'linked_timesheet_ids') = 'array' THEN v_success.payload_json->'linked_timesheet_ids' ELSE '[]'::jsonb END,
        p_recompute_session_progress => true
      );
      v_action := 'RECONCILED_SUCCESSFUL_BUILD';
      v_reconciled_count := v_reconciled_count + 1;
      v_repaired_count := v_repaired_count + 1;
      v_successor_job_id := v_success.id;
    ELSE
      v_active := NULL::public.banking_pay_workbench_jobs;
      SELECT active_job.*
      INTO v_active
      FROM public.banking_pay_workbench_jobs AS active_job
      WHERE active_job.id IS DISTINCT FROM v_scope.pending_job_id
        AND active_job.session_id = v_scope.session_id
        AND active_job.candidate_id = v_scope.candidate_id
        AND UPPER(BTRIM(COALESCE(active_job.status, ''))) IN ('QUEUED', 'RUNNING')
        AND UPPER(BTRIM(COALESCE(active_job.job_type, ''))) IN (
          'WORKBENCH_CANDIDATE_SOURCE_BUILD',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
          'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
          'CANDIDATE_SOURCE_BUILD',
          'CANDIDATE_SOURCE_BUILD_CHUNK',
          'SOURCE_BUILD',
          'SOURCE_BUILD_PAGE'
        )
        AND CASE
              WHEN COALESCE(active_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
                THEN (active_job.payload_json->>'session_version')::bigint
              ELSE NULL::bigint
            END = v_session.version
        AND CASE
              WHEN COALESCE(active_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                THEN (active_job.payload_json->>'source_change_seq')::bigint
              ELSE NULL::bigint
            END >= v_live_change_seq
        AND COALESCE(active_job.payload_json->>'source_build_run_id', '') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ORDER BY CASE WHEN UPPER(BTRIM(COALESCE(active_job.status, ''))) = 'RUNNING' THEN 0 ELSE 1 END,
               (active_job.payload_json->>'source_change_seq')::bigint DESC,
               active_job.created_at_utc ASC,
               active_job.id ASC
      LIMIT 1
      FOR UPDATE;

      IF FOUND THEN
        UPDATE public.banking_pay_workbench_session_scope AS rebound_scope
        SET pending_job_id = v_active.id,
            status = 'SOURCE_BUILD_PENDING',
            dirty = true,
            error_json = NULL::jsonb,
            updated_at_utc = v_now
        WHERE rebound_scope.id = v_scope.id
          AND (
            rebound_scope.pending_job_id IS NOT DISTINCT FROM v_scope.pending_job_id
            OR rebound_scope.pending_job_id IS NULL
          );
        v_action := 'REBOUND_ACTIVE_SUCCESSOR';
        v_rebound_count := v_rebound_count + 1;
        v_repaired_count := v_repaired_count + 1;
        v_successor_job_id := v_active.id;
      ELSIF v_owner.id IS NOT NULL
            AND COALESCE(v_owner.attempt_count, 0) >= COALESCE(v_owner.max_attempts, 8) THEN
        v_action := 'FAILED_CLOSED_MAX_ATTEMPTS';
        v_safe_error_code := 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB';
        v_safe_error_message := 'Candidate refresh could not be recovered because all job attempts were used.';
        UPDATE public.banking_pay_workbench_session_scope AS failed_scope
        SET status = 'SOURCE_BUILD_ERROR',
            pending_job_id = NULL::uuid,
            dirty = true,
            error_json = jsonb_build_object(
              'code', v_safe_error_code,
              'message', v_safe_error_message,
              'job_id', v_owner.id::text,
              'attempt_count', COALESCE(v_owner.attempt_count, 0),
              'max_attempts', COALESCE(v_owner.max_attempts, 8),
              'automatic_recovery_scheduled', false,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            ),
            updated_at_utc = v_now
        WHERE failed_scope.id = v_scope.id
          AND failed_scope.pending_job_id IS NOT DISTINCT FROM v_scope.pending_job_id;
        v_failed_closed_count := v_failed_closed_count + 1;
        v_repaired_count := v_repaired_count + 1;
      ELSE
        v_refresh_scope_kind := COALESCE(NULLIF(UPPER(BTRIM(COALESCE(v_owner.payload_json->>'refresh_scope_kind', ''))), ''), 'CANDIDATE_FULL_LIVE');
        IF v_refresh_scope_kind NOT IN ('TARGETED_TIMESHEETS', 'CANDIDATE_FULL_LIVE') THEN
          v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
        END IF;
        v_targeted_timesheet_ids := CASE
          WHEN jsonb_typeof(v_owner.payload_json->'targeted_timesheet_ids') = 'array'
            THEN v_owner.payload_json->'targeted_timesheet_ids'
          ELSE '[]'::jsonb
        END;
        v_linked_timesheet_ids := CASE
          WHEN jsonb_typeof(v_owner.payload_json->'linked_timesheet_ids') = 'array'
            THEN v_owner.payload_json->'linked_timesheet_ids'
          ELSE '[]'::jsonb
        END;
        v_pay_channel_scope := COALESCE(NULLIF(UPPER(BTRIM(COALESCE(v_owner.payload_json->>'pay_channel_scope', ''))), ''), 'ALL');
        IF v_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'ALL', 'LOANS') THEN
          v_pay_channel_scope := 'ALL';
        END IF;

        BEGIN
          v_enqueue_payload := jsonb_build_object(
            'session_id', v_session.id::text,
            'source_session_id', v_session.id::text,
            'candidate_id', v_scope.candidate_id::text,
            'session_version', v_session.version,
            'source_change_seq', v_live_change_seq,
            'refresh_scope_kind', v_refresh_scope_kind,
            'targeted_timesheet_ids', v_targeted_timesheet_ids,
            'linked_timesheet_ids', v_linked_timesheet_ids,
            'pay_channel_scope', v_pay_channel_scope,
            'force_legacy', true,
            'force_broad_legacy', v_refresh_scope_kind = 'CANDIDATE_FULL_LIVE',
            'owner_repair', true,
            'owner_repair_reason', v_owner_reason,
            'replaces_job_id', CASE WHEN v_owner.id IS NULL THEN NULL ELSE v_owner.id::text END,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          );
          v_enqueue_result := public.pay_workbench_enqueue_candidate_refresh(
            p_snapshot_run_id => v_session.source_snapshot_run_id,
            p_candidate_id => v_scope.candidate_id,
            p_reason => v_reason,
            p_actor_user_id => v_session.actor_user_id,
            p_payload_json => v_enqueue_payload
          );

          IF COALESCE(v_enqueue_result->>'job_id', '') ~*
             '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
            v_successor_job_id := (v_enqueue_result->>'job_id')::uuid;
          END IF;

          SELECT successor_job.payload_json->>'source_build_run_id'
          INTO v_successor_run_id_text
          FROM public.banking_pay_workbench_jobs AS successor_job
          JOIN public.banking_pay_workbench_session_scope AS successor_scope
            ON successor_scope.session_id = successor_job.session_id
           AND successor_scope.candidate_id = successor_job.candidate_id
          WHERE successor_job.id = v_successor_job_id
            AND successor_job.session_id = v_session.id
            AND successor_job.candidate_id = v_scope.candidate_id
            AND UPPER(BTRIM(COALESCE(successor_job.status, ''))) IN ('QUEUED', 'RUNNING')
            AND UPPER(BTRIM(COALESCE(successor_job.job_type, ''))) = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
            AND successor_scope.pending_job_id = successor_job.id
            AND UPPER(BTRIM(COALESCE(successor_scope.status, ''))) = 'SOURCE_BUILD_PENDING'
            AND CASE
                  WHEN COALESCE(successor_job.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
                    THEN (successor_job.payload_json->>'session_version')::bigint
                  ELSE NULL::bigint
                END = v_session.version
            AND CASE
                  WHEN COALESCE(successor_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                    THEN (successor_job.payload_json->>'source_change_seq')::bigint
                  ELSE NULL::bigint
                END >= v_live_change_seq
            AND COALESCE(successor_job.payload_json->>'source_build_run_id', '') ~*
                '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
          v_successor_valid := FOUND;

          IF v_successor_valid IS NOT TRUE THEN
            RAISE EXCEPTION 'PAY_WORKBENCH_OWNER_REPAIR_SUCCESSOR_INVALID'
              USING ERRCODE = 'P0001';
          END IF;

          IF v_owner.id IS NOT NULL THEN
            UPDATE public.banking_pay_workbench_jobs AS repaired_old_job
            SET payload_json = COALESCE(repaired_old_job.payload_json, '{}'::jsonb)
              || jsonb_build_object(
                'owner_repair_applied', true,
                'owner_repair_reason', v_owner_reason,
                'owner_repair_successor_job_id', v_successor_job_id::text,
                'owner_repair_successor_source_build_run_id', v_successor_run_id_text,
                'owner_repair_at_utc', v_now::text
              ),
                updated_at_utc = v_now
            WHERE repaired_old_job.id = v_owner.id;
          END IF;

          v_action := 'ENQUEUED_CANONICAL_SUCCESSOR';
          v_enqueued_count := v_enqueued_count + 1;
          v_repaired_count := v_repaired_count + 1;
        EXCEPTION WHEN OTHERS THEN
          v_action := 'FAILED_CLOSED_REPAIR_ERROR';
          v_safe_error_code := 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB';
          v_safe_error_message := 'Candidate refresh could not be scheduled automatically. Refresh Banking Pay or retry the candidate.';
          UPDATE public.banking_pay_workbench_session_scope AS failed_repair_scope
          SET status = 'SOURCE_BUILD_ERROR',
              pending_job_id = NULL::uuid,
              dirty = true,
              error_json = jsonb_build_object(
                'code', v_safe_error_code,
                'message', v_safe_error_message,
                'job_id', CASE WHEN v_owner.id IS NULL THEN NULL ELSE v_owner.id::text END,
                'automatic_recovery_scheduled', false,
                'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
              ),
              updated_at_utc = v_now
          WHERE failed_repair_scope.id = v_scope.id
            AND failed_repair_scope.pending_job_id IS NOT DISTINCT FROM v_scope.pending_job_id;
          v_failed_closed_count := v_failed_closed_count + 1;
          v_repaired_count := v_repaired_count + 1;
        END;
      END IF;
    END IF;

    BEGIN
      PERFORM public._audit_insert(
        'banking_pay_workbench_session_scope',
        v_scope.candidate_id::text,
        'PENDING_SOURCE_BUILD_OWNER_REPAIRED',
        jsonb_build_object(
          'session_id', v_scope.session_id::text,
          'candidate_id', v_scope.candidate_id::text,
          'old_pending_job_id', CASE WHEN v_scope.pending_job_id IS NULL THEN NULL ELSE v_scope.pending_job_id::text END,
          'owner_failure_reason', v_owner_reason
        ),
        jsonb_build_object(
          'session_id', v_scope.session_id::text,
          'candidate_id', v_scope.candidate_id::text,
          'action', v_action,
          'successor_job_id', CASE WHEN v_successor_job_id IS NULL THEN NULL ELSE v_successor_job_id::text END,
          'automatic_recovery_scheduled', v_action IN ('REBOUND_ACTIVE_SUCCESSOR', 'ENQUEUED_CANONICAL_SUCCESSOR'),
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        ),
        'PAY_WORKBENCH_PENDING_SOURCE_BUILD_OWNER_REPAIR',
        v_session.actor_user_id
      );
    EXCEPTION WHEN OTHERS THEN
      v_audit_failed := true;
    END;

    v_result_rows := v_result_rows || jsonb_build_array(
      jsonb_strip_nulls(jsonb_build_object(
        'session_id', v_scope.session_id::text,
        'candidate_id', v_scope.candidate_id::text,
        'old_pending_job_id', CASE WHEN v_scope.pending_job_id IS NULL THEN NULL ELSE v_scope.pending_job_id::text END,
        'owner_failure_reason', v_owner_reason,
        'action', v_action,
        'successor_job_id', CASE WHEN v_successor_job_id IS NULL THEN NULL ELSE v_successor_job_id::text END,
        'automatic_recovery_scheduled', v_action IN ('REBOUND_ACTIVE_SUCCESSOR', 'ENQUEUED_CANONICAL_SUCCESSOR'),
        'failure_code', v_safe_error_code,
        'message', v_safe_error_message,
        'audit_failed', v_audit_failed
      ))
    );
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'repair_code', 'PAY_WORKBENCH_PENDING_SOURCE_BUILD_OWNER_REPAIR',
    'examined_count', jsonb_array_length(v_result_rows) + v_skipped_count,
    'repaired_count', v_repaired_count,
    'reconciled_count', v_reconciled_count,
    'rebound_count', v_rebound_count,
    'enqueued_count', v_enqueued_count,
    'failed_closed_count', v_failed_closed_count,
    'skipped_count', v_skipped_count,
    'automatic_recovery_scheduled', (v_rebound_count + v_enqueued_count) > 0,
    'results', v_result_rows,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text) FROM anon;
REVOKE ALL ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(uuid, uuid, integer, timestamp with time zone, text) TO service_role;
