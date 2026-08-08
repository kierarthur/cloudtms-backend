-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.pay_workbench_repair_orphaned_pending_source_build(p_session_id uuid, p_candidate_id uuid, p_limit integer, p_now_utc timestamp with time zone, p_reason text)
-- Installed pg_get_functiondef MD5: 78d2a4ac9dd7b8309ed5c77112d981f0
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(p_session_id uuid DEFAULT NULL::uuid, p_candidate_id uuid DEFAULT NULL::uuid, p_limit integer DEFAULT 10, p_now_utc timestamp with time zone DEFAULT NULL::timestamp with time zone, p_reason text DEFAULT 'PENDING_SCOPE_OWNER_REPAIR'::text)
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
  v_post_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_owner public.banking_pay_workbench_jobs%ROWTYPE;
  v_post_owner public.banking_pay_workbench_jobs%ROWTYPE;
  v_success public.banking_pay_workbench_jobs%ROWTYPE;
  v_active public.banking_pay_workbench_jobs%ROWTYPE;
  v_success_run_id uuid := NULL::uuid;
  v_success_source_change_seq bigint := 0;
  v_live_change_seq bigint := 0;
  v_post_live_change_seq bigint := 0;
  v_owner_canonical_type text := NULL::text;
  v_owner_valid boolean := false;
  v_owner_generation_obsolete boolean := false;
  v_owner_reason text := NULL::text;
  v_enqueue_payload jsonb := '{}'::jsonb;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_success_result jsonb := '{}'::jsonb;
  v_progress_recompute_result jsonb := '{}'::jsonb;
  v_successor_job_id uuid := NULL::uuid;
  v_successor_run_id_text text := NULL::text;
  v_successor_valid boolean := false;
  v_refresh_scope_kind text := 'CANDIDATE_FULL_LIVE';
  v_targeted_timesheet_ids jsonb := '[]'::jsonb;
  v_linked_timesheet_ids jsonb := '[]'::jsonb;
  v_pay_channel_scope text := 'ALL';
  v_scope_transition_row_count integer := 0;
  v_reconciliation_attempted boolean := false;
  v_reconciliation_applied boolean := false;
  v_reconciliation_error_code text := NULL::text;
  v_reconciliation_postcondition_proven boolean := false;
  v_post_scope_found boolean := false;
  v_exact_current_source_exists boolean := false;
  v_unresolved_current_source_exists boolean := false;
  v_remaining_owner_valid boolean := false;
  v_candidate_action text := NULL::text;
  v_candidate_branch text := NULL::text;
  v_candidate_transition_proven boolean := false;
  v_candidate_unresolved boolean := false;
  v_candidate_skipped boolean := false;
  v_candidate_failure_code text := NULL::text;
  v_candidate_failure_message text := NULL::text;
  v_candidate_unresolved_reason text := NULL::text;
  v_candidate_old_pending_job_id uuid := NULL::uuid;
  v_progress_recomputed boolean := false;
  v_progress_recompute_error_code text := NULL::text;
  v_audit_failed boolean := false;
  v_repaired_count integer := 0;
  v_reconciled_count integer := 0;
  v_rebound_count integer := 0;
  v_enqueued_count integer := 0;
  v_failed_closed_count integer := 0;
  v_skipped_count integer := 0;
  v_unresolved_count integer := 0;
  v_progress_recomputed_count integer := 0;
  v_progress_recompute_failed_count integer := 0;
  v_result_rows jsonb := '[]'::jsonb;
  v_expired_attempt record;
  v_expired_attempt_count integer:=0;
  v_expired_requeued_count integer:=0;
  v_expired_exhausted_count integer:=0;
BEGIN
  -- Recover delivered-but-uncompleted material attempts first.  Each recovery
  -- uses the candidate lock and registry/build/job/attempt lock order.
  FOR v_expired_attempt IN
    SELECT attempt.id attempt_id,attempt.job_id,attempt.build_id,attempt.candidate_id,
           attempt.private_stage,job.attempt_count,job.max_attempts
    FROM private.banking_pay_workbench_stage_attempts attempt
    JOIN public.banking_pay_workbench_jobs job ON job.id=attempt.job_id
    WHERE attempt.attempt_status='STARTED'
      AND clock_timestamp()>=attempt.lease_expires_at_utc+interval '15 seconds'
      AND job.status='RUNNING' AND job.economic_build_id=attempt.build_id
      AND job.private_stage=attempt.private_stage
      AND (p_session_id IS NULL OR job.session_id=p_session_id)
      AND (p_candidate_id IS NULL OR attempt.candidate_id=p_candidate_id)
    ORDER BY attempt.lease_expires_at_utc,attempt.id
    LIMIT v_limit
  LOOP
    IF pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(
      public._pay_workbench_candidate_serial_key(v_expired_attempt.candidate_id),24062027)) THEN
      PERFORM 1 FROM private.banking_pay_workbench_candidate_scope_registry
      WHERE candidate_id=v_expired_attempt.candidate_id FOR UPDATE;
      PERFORM 1 FROM private.banking_pay_workbench_economic_builds
      WHERE id=v_expired_attempt.build_id FOR UPDATE;
      PERFORM 1 FROM public.banking_pay_workbench_jobs
      WHERE id=v_expired_attempt.job_id FOR UPDATE;
      PERFORM 1 FROM private.banking_pay_workbench_stage_attempts
      WHERE id=v_expired_attempt.attempt_id FOR UPDATE;
      UPDATE private.banking_pay_workbench_stage_attempts SET attempt_status='EXPIRED',
        expired_at_utc=clock_timestamp(),result_code='LEASE_EXPIRED_AFTER_CANCELLATION_GRACE',
        error_class='DELIVERED_ATTEMPT_EXPIRED',updated_at_utc=clock_timestamp()
      WHERE id=v_expired_attempt.attempt_id AND attempt_status='STARTED'
        AND clock_timestamp()>=lease_expires_at_utc+interval '15 seconds';
      IF FOUND THEN
        v_expired_attempt_count:=v_expired_attempt_count+1;
        IF v_expired_attempt.attempt_count<v_expired_attempt.max_attempts THEN
          UPDATE public.banking_pay_workbench_jobs SET status='QUEUED',started_at_utc=NULL,
            run_at_utc=clock_timestamp()+make_interval(secs=>LEAST(300,
              GREATEST(1,power(2,LEAST(v_expired_attempt.attempt_count,8))::integer))),
            last_error_json=jsonb_build_object('code','DELIVERED_ATTEMPT_EXPIRED'),
            updated_at_utc=clock_timestamp()
          WHERE id=v_expired_attempt.job_id AND status='RUNNING';
          v_expired_requeued_count:=v_expired_requeued_count+1;
        ELSE
          UPDATE public.banking_pay_workbench_jobs SET status='FAILED',
            failed_at_utc=clock_timestamp(),last_error_json=jsonb_build_object(
              'code','DELIVERED_ATTEMPT_EXHAUSTED'),updated_at_utc=clock_timestamp()
          WHERE id=v_expired_attempt.job_id AND status='RUNNING';
          UPDATE private.banking_pay_workbench_economic_builds SET status='FAILED',
            failed_at_utc=clock_timestamp(),failure_json=jsonb_build_object(
              'code','DELIVERED_ATTEMPT_EXHAUSTED'),updated_at_utc=clock_timestamp()
          WHERE id=v_expired_attempt.build_id AND status NOT IN ('COMPLETE','OBSOLETE');
          v_expired_exhausted_count:=v_expired_exhausted_count+1;
        END IF;
      END IF;
    END IF;
  END LOOP;

  FOR v_candidate IN
    SELECT scope_row.session_id, scope_row.candidate_id
    FROM public.banking_pay_workbench_session_scope AS scope_row
    JOIN public.banking_pay_workbench_sessions AS session_row
      ON session_row.id = scope_row.session_id
    LEFT JOIN public.banking_pay_workbench_jobs AS owner_job
      ON owner_job.id = scope_row.pending_job_id
    LEFT JOIN public.app_change_counters AS owner_change_counter
      ON owner_change_counter.entity_key = 'pay_candidate:' || scope_row.candidate_id::text
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
        OR (
          CASE
            WHEN COALESCE(owner_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
              THEN (owner_job.payload_json->>'source_change_seq')::bigint
            ELSE NULL::bigint
          END
          < COALESCE(owner_change_counter.seq, 0)
        )
        OR COALESCE(owner_job.payload_json->>'source_build_run_id', '') !~*
           '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      )
    ORDER BY scope_row.updated_at_utc ASC NULLS FIRST, scope_row.session_id, scope_row.candidate_id
    LIMIT v_limit
  LOOP
    v_session := NULL::public.banking_pay_workbench_sessions;
    v_scope := NULL::public.banking_pay_workbench_session_scope;
    v_post_scope := NULL::public.banking_pay_workbench_session_scope;
    v_owner := NULL::public.banking_pay_workbench_jobs;
    v_post_owner := NULL::public.banking_pay_workbench_jobs;
    v_success := NULL::public.banking_pay_workbench_jobs;
    v_active := NULL::public.banking_pay_workbench_jobs;
    v_success_run_id := NULL::uuid;
    v_success_source_change_seq := 0;
    v_live_change_seq := 0;
    v_post_live_change_seq := 0;
    v_owner_canonical_type := NULL::text;
    v_owner_valid := false;
    v_owner_generation_obsolete := false;
    v_owner_reason := NULL::text;
    v_enqueue_payload := '{}'::jsonb;
    v_enqueue_result := '{}'::jsonb;
    v_success_result := '{}'::jsonb;
    v_progress_recompute_result := '{}'::jsonb;
    v_successor_job_id := NULL::uuid;
    v_successor_run_id_text := NULL::text;
    v_successor_valid := false;
    v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';
    v_targeted_timesheet_ids := '[]'::jsonb;
    v_linked_timesheet_ids := '[]'::jsonb;
    v_pay_channel_scope := 'ALL';
    v_scope_transition_row_count := 0;
    v_reconciliation_attempted := false;
    v_reconciliation_applied := false;
    v_reconciliation_error_code := NULL::text;
    v_reconciliation_postcondition_proven := false;
    v_post_scope_found := false;
    v_exact_current_source_exists := false;
    v_unresolved_current_source_exists := false;
    v_remaining_owner_valid := false;
    v_candidate_action := NULL::text;
    v_candidate_branch := NULL::text;
    v_candidate_transition_proven := false;
    v_candidate_unresolved := false;
    v_candidate_skipped := false;
    v_candidate_failure_code := NULL::text;
    v_candidate_failure_message := NULL::text;
    v_candidate_unresolved_reason := NULL::text;
    v_candidate_old_pending_job_id := NULL::uuid;
    v_progress_recomputed := false;
    v_progress_recompute_error_code := NULL::text;
    v_audit_failed := false;

    BEGIN
      IF NOT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(
        public._pay_workbench_candidate_serial_key(v_candidate.candidate_id),24062027
      )) THEN
        v_candidate_skipped := true;
        v_candidate_action := 'SKIPPED_CANDIDATE_SERIAL_BUSY';
      ELSE
        PERFORM 1
        FROM private.banking_pay_workbench_candidate_scope_registry AS registry
        WHERE registry.candidate_id=v_candidate.candidate_id
        FOR UPDATE;

        SELECT session_row.*
        INTO v_session
        FROM public.banking_pay_workbench_sessions AS session_row
        WHERE session_row.id = v_candidate.session_id
        FOR UPDATE;

        IF NOT FOUND
           OR UPPER(BTRIM(COALESCE(v_session.status, ''))) <> 'OPEN'
           OR v_session.discarded_at_utc IS NOT NULL THEN
          v_candidate_skipped := true;
          v_candidate_action := 'SKIPPED_AFTER_RECHECK';
        ELSE
          SELECT scope_row.*
          INTO v_scope
          FROM public.banking_pay_workbench_session_scope AS scope_row
          WHERE scope_row.session_id = v_candidate.session_id
            AND scope_row.candidate_id = v_candidate.candidate_id
          FOR UPDATE;

        IF NOT FOUND
           OR UPPER(BTRIM(COALESCE(v_scope.status, ''))) <> 'SOURCE_BUILD_PENDING' THEN
          v_candidate_skipped := true;
          v_candidate_action := 'SKIPPED_AFTER_RECHECK';
        ELSE
          v_candidate_old_pending_job_id := v_scope.pending_job_id;

          SELECT COALESCE((
            SELECT change_counter.seq
            FROM public.app_change_counters AS change_counter
            WHERE change_counter.entity_key = 'pay_candidate:' || v_scope.candidate_id::text
          ), 0)
          INTO v_live_change_seq;

          IF v_scope.pending_job_id IS NOT NULL THEN
            SELECT owner_job.*
            INTO v_owner
            FROM public.banking_pay_workbench_jobs AS owner_job
            WHERE owner_job.id = v_scope.pending_job_id
            FOR UPDATE;
          END IF;

          v_owner_generation_obsolete := UPPER(BTRIM(COALESCE(
            v_owner.last_error_json->>'code',''
          )))='ATTEMPT_GENERATION_OBSOLETE';

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
            v_candidate_skipped := true;
            v_candidate_action := 'SKIPPED_AFTER_RECHECK';
          ELSE
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

            SELECT successful_job.*
            INTO v_success
            FROM public.banking_pay_workbench_jobs AS successful_job
            WHERE successful_job.session_id = v_scope.session_id
              AND successful_job.candidate_id = v_scope.candidate_id
              AND UPPER(BTRIM(COALESCE(successful_job.status, ''))) = 'SUCCEEDED'
              AND successful_job.completed_at_utc IS NOT NULL
              AND successful_job.failed_at_utc IS NULL
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
                  AND successful_source.source_change_seq = CASE
                        WHEN COALESCE(successful_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                          THEN (successful_job.payload_json->>'source_change_seq')::bigint
                        ELSE NULL::bigint
                      END
                  AND UPPER(BTRIM(COALESCE(successful_source.status, ''))) = 'CURRENT'
              )
              AND NOT EXISTS (
                SELECT 1
                FROM public.banking_pay_workbench_candidate_source_lines AS unresolved_successful_source
                WHERE unresolved_successful_source.session_id = v_scope.session_id
                  AND unresolved_successful_source.candidate_id = v_scope.candidate_id
                  AND unresolved_successful_source.session_version = v_session.version
                  AND UPPER(BTRIM(COALESCE(unresolved_successful_source.status, ''))) IN ('DIRTY', 'ERROR')
              )
            ORDER BY
              CASE
                WHEN COALESCE(successful_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                  THEN (successful_job.payload_json->>'source_change_seq')::bigint
                ELSE NULL::bigint
              END DESC,
              successful_job.completed_at_utc DESC,
              successful_job.id DESC
            LIMIT 1
            FOR UPDATE OF successful_job;

            IF FOUND THEN
              v_success_run_id := (v_success.payload_json->>'source_build_run_id')::uuid;
              v_success_source_change_seq := (v_success.payload_json->>'source_change_seq')::bigint;

              BEGIN
                v_reconciliation_attempted := true;
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
                  p_recompute_session_progress => false
                );

                IF jsonb_typeof(v_success_result) IS DISTINCT FROM 'object'
                   OR LOWER(BTRIM(COALESCE(v_success_result->>'ok', ''))) <> 'true'
                   OR LOWER(BTRIM(COALESCE(v_success_result->>'skipped', ''))) <> 'false' THEN
                  RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_RESULT_NOT_CONFIRMED'
                    USING ERRCODE = 'P0001';
                END IF;

                SELECT reconciled_scope.*
                INTO v_post_scope
                FROM public.banking_pay_workbench_session_scope AS reconciled_scope
                WHERE reconciled_scope.id = v_scope.id
                FOR UPDATE;
                v_post_scope_found := FOUND;

                PERFORM 1
                FROM public.banking_pay_workbench_candidate_source_lines AS exact_current_source
                WHERE exact_current_source.session_id = v_scope.session_id
                  AND exact_current_source.candidate_id = v_scope.candidate_id
                  AND exact_current_source.session_version = v_session.version
                  AND exact_current_source.source_build_run_id = v_success_run_id
                  AND exact_current_source.source_change_seq = v_success_source_change_seq
                  AND UPPER(BTRIM(COALESCE(exact_current_source.status, ''))) = 'CURRENT'
                FOR UPDATE;
                v_exact_current_source_exists := FOUND;

                PERFORM 1
                FROM public.banking_pay_workbench_candidate_source_lines AS unresolved_current_source
                WHERE unresolved_current_source.session_id = v_scope.session_id
                  AND unresolved_current_source.candidate_id = v_scope.candidate_id
                  AND unresolved_current_source.session_version = v_session.version
                  AND UPPER(BTRIM(COALESCE(unresolved_current_source.status, ''))) IN ('DIRTY', 'ERROR')
                FOR UPDATE;
                v_unresolved_current_source_exists := FOUND;

                v_remaining_owner_valid := v_post_scope.pending_job_id IS NULL;
                IF v_post_scope.pending_job_id IS NOT NULL THEN
                  SELECT downstream_owner.*
                  INTO v_post_owner
                  FROM public.banking_pay_workbench_jobs AS downstream_owner
                  WHERE downstream_owner.id = v_post_scope.pending_job_id
                  FOR UPDATE;

                  v_remaining_owner_valid := FOUND
                    AND v_post_owner.session_id = v_scope.session_id
                    AND v_post_owner.candidate_id = v_scope.candidate_id
                    AND UPPER(BTRIM(COALESCE(v_post_owner.status, ''))) IN ('QUEUED', 'RUNNING')
                    AND UPPER(BTRIM(COALESCE(v_post_owner.job_type, ''))) IN (
                      'WORKBENCH_CANDIDATE_SOURCE_BUILD',
                      'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
                      'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
                      'CANDIDATE_SOURCE_BUILD',
                      'CANDIDATE_SOURCE_BUILD_CHUNK',
                      'SOURCE_BUILD',
                      'SOURCE_BUILD_PAGE',
                      'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
                      'WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE',
                      'CANDIDATE_LINE_WORK_SEED',
                      'CANDIDATE_LINE_WORK_SEED_PAGE',
                      'LINE_WORK_SEED_PAGE',
                      'SNAPSHOT_CANDIDATE_REFRESH',
                      'CANDIDATE_REFRESH',
                      'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
                      'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK',
                      'CANDIDATE_LINE_WORK_PROCESS',
                      'CANDIDATE_LINE_WORK_PROCESS_CHUNK',
                      'LINE_WORK_PROCESS',
                      'LINE_WORK_PROCESS_CHUNK',
                      'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
                      'WORKBENCH_PREVIEW_ROWS_MATERIALIZE',
                      'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK',
                      'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK',
                      'PREVIEW_ROWS_MATERIALISE',
                      'PREVIEW_ROWS_MATERIALIZE',
                      'PREVIEW_ROWS_MATERIALISE_CHUNK',
                      'PREVIEW_ROWS_MATERIALIZE_CHUNK'
                    )
                    AND CASE
                          WHEN COALESCE(v_post_owner.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
                            THEN (v_post_owner.payload_json->>'session_version')::bigint
                          ELSE NULL::bigint
                        END = v_session.version;
                END IF;

                v_reconciliation_postcondition_proven := v_post_scope_found
                  AND UPPER(BTRIM(COALESCE(v_post_scope.status, ''))) IN (
                    'SOURCE_READY',
                    'LINE_WORK_PENDING',
                    'MATERIALISED',
                    'MATERIALIZED',
                    'READY',
                    'SOURCE_EMPTY'
                  )
                  AND UPPER(BTRIM(COALESCE(v_post_scope.status, ''))) <> 'SOURCE_BUILD_PENDING'
                  AND (
                    v_scope.pending_job_id IS NULL
                    OR v_post_scope.pending_job_id IS DISTINCT FROM v_scope.pending_job_id
                  )
                  AND COALESCE(v_post_scope.dirty, false) IS NOT TRUE
                  AND v_exact_current_source_exists
                  AND v_unresolved_current_source_exists IS NOT TRUE
                  AND v_remaining_owner_valid;

                IF v_reconciliation_postcondition_proven IS NOT TRUE THEN
                  RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_POSTCONDITION_NOT_PROVEN'
                    USING ERRCODE = 'P0001';
                END IF;

                v_reconciliation_applied := true;
              EXCEPTION WHEN OTHERS THEN
                v_reconciliation_applied := false;
                v_reconciliation_error_code := SQLSTATE;
                v_reconciliation_postcondition_proven := false;
              END;
            END IF;

            IF v_reconciliation_applied THEN
              v_candidate_action := 'RECONCILED_SUCCESSFUL_BUILD';
              v_candidate_branch := 'RECONCILED';
              v_candidate_transition_proven := true;
              v_successor_job_id := v_success.id;
            ELSE
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
              ORDER BY
                CASE WHEN UPPER(BTRIM(COALESCE(active_job.status, ''))) = 'RUNNING' THEN 0 ELSE 1 END,
                CASE
                  WHEN COALESCE(active_job.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                    THEN (active_job.payload_json->>'source_change_seq')::bigint
                  ELSE NULL::bigint
                END DESC,
                active_job.created_at_utc ASC,
                active_job.id ASC
              LIMIT 1
              FOR UPDATE OF active_job;

              IF FOUND THEN
                v_successor_job_id := v_active.id;

                UPDATE public.banking_pay_workbench_session_scope AS rebound_scope
                SET pending_job_id = v_active.id,
                    status = 'SOURCE_BUILD_PENDING',
                    dirty = true,
                    error_json = NULL::jsonb,
                    updated_at_utc = v_now
                WHERE rebound_scope.id = v_scope.id
                  AND rebound_scope.pending_job_id IS NOT DISTINCT FROM v_scope.pending_job_id;

                GET DIAGNOSTICS v_scope_transition_row_count = ROW_COUNT;

                SELECT rebound_post_scope.*
                INTO v_post_scope
                FROM public.banking_pay_workbench_session_scope AS rebound_post_scope
                WHERE rebound_post_scope.id = v_scope.id
                FOR UPDATE;

                SELECT COALESCE((
                  SELECT change_counter.seq
                  FROM public.app_change_counters AS change_counter
                  WHERE change_counter.entity_key = 'pay_candidate:' || v_scope.candidate_id::text
                ), 0)
                INTO v_post_live_change_seq;

                SELECT EXISTS (
                  SELECT 1
                  FROM public.banking_pay_workbench_jobs AS rebound_owner
                  WHERE rebound_owner.id = v_active.id
                    AND rebound_owner.session_id = v_scope.session_id
                    AND rebound_owner.candidate_id = v_scope.candidate_id
                    AND UPPER(BTRIM(COALESCE(rebound_owner.status, ''))) IN ('QUEUED', 'RUNNING')
                    AND UPPER(BTRIM(COALESCE(rebound_owner.job_type, ''))) IN (
                      'WORKBENCH_CANDIDATE_SOURCE_BUILD',
                      'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
                      'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
                      'CANDIDATE_SOURCE_BUILD',
                      'CANDIDATE_SOURCE_BUILD_CHUNK',
                      'SOURCE_BUILD',
                      'SOURCE_BUILD_PAGE'
                    )
                    AND CASE
                          WHEN COALESCE(rebound_owner.payload_json->>'session_version', '') ~ '^[0-9]{1,18}$'
                            THEN (rebound_owner.payload_json->>'session_version')::bigint
                          ELSE NULL::bigint
                        END = v_session.version
                    AND CASE
                          WHEN COALESCE(rebound_owner.payload_json->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
                            THEN (rebound_owner.payload_json->>'source_change_seq')::bigint
                          ELSE NULL::bigint
                        END >= v_post_live_change_seq
                    AND COALESCE(rebound_owner.payload_json->>'source_build_run_id', '') ~*
                        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                )
                INTO v_successor_valid;

                IF v_scope_transition_row_count = 1
                   AND UPPER(BTRIM(COALESCE(v_post_scope.status, ''))) = 'SOURCE_BUILD_PENDING'
                   AND v_post_scope.pending_job_id = v_active.id
                   AND COALESCE(v_post_scope.dirty, false) IS TRUE
                   AND v_post_scope.error_json IS NULL
                   AND v_successor_valid THEN
                  v_candidate_action := 'REBOUND_ACTIVE_SUCCESSOR';
                  v_candidate_branch := 'REBOUND';
                  v_candidate_transition_proven := true;
                ELSIF v_scope_transition_row_count = 0
                      AND UPPER(BTRIM(COALESCE(v_post_scope.status, ''))) = 'SOURCE_BUILD_PENDING'
                      AND COALESCE(v_post_scope.dirty, false) IS TRUE
                      AND v_post_scope.error_json IS NULL
                      AND v_post_scope.pending_job_id = v_active.id
                      AND v_successor_valid THEN
                  v_candidate_skipped := true;
                  v_candidate_action := 'SKIPPED_AFTER_RECHECK';
                  v_successor_job_id := v_post_scope.pending_job_id;
                ELSE
                  RAISE EXCEPTION 'PAY_WORKBENCH_OWNER_REPAIR_POSTCONDITION_NOT_PROVEN'
                    USING ERRCODE = 'P0001';
                END IF;
              ELSIF v_owner.id IS NOT NULL
                    AND v_owner_generation_obsolete IS NOT TRUE
                    AND COALESCE(v_owner.attempt_count, 0) >= COALESCE(v_owner.max_attempts, 8) THEN
                v_candidate_failure_code := 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB';
                v_candidate_failure_message := 'Candidate refresh could not be recovered because all job attempts were used.';

                UPDATE public.banking_pay_workbench_session_scope AS failed_scope
                SET status = 'SOURCE_BUILD_ERROR',
                    pending_job_id = NULL::uuid,
                    dirty = true,
                    error_json = jsonb_build_object(
                      'code', v_candidate_failure_code,
                      'message', v_candidate_failure_message,
                      'job_id', v_owner.id::text,
                      'attempt_count', COALESCE(v_owner.attempt_count, 0),
                      'max_attempts', COALESCE(v_owner.max_attempts, 8),
                      'automatic_recovery_scheduled', false,
                      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                    ),
                    updated_at_utc = v_now
                WHERE failed_scope.id = v_scope.id
                  AND failed_scope.pending_job_id IS NOT DISTINCT FROM v_scope.pending_job_id;

                GET DIAGNOSTICS v_scope_transition_row_count = ROW_COUNT;

                SELECT failed_post_scope.*
                INTO v_post_scope
                FROM public.banking_pay_workbench_session_scope AS failed_post_scope
                WHERE failed_post_scope.id = v_scope.id
                FOR UPDATE;

                IF UPPER(BTRIM(COALESCE(v_post_scope.status, ''))) = 'SOURCE_BUILD_ERROR'
                   AND v_post_scope.pending_job_id IS NULL
                   AND COALESCE(v_post_scope.dirty, false) IS TRUE
                   AND v_post_scope.error_json->>'code' = 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB'
                   AND LOWER(BTRIM(COALESCE(v_post_scope.error_json->>'automatic_recovery_scheduled', ''))) = 'false' THEN
                  IF v_scope_transition_row_count = 1 THEN
                    v_candidate_action := 'FAILED_CLOSED_MAX_ATTEMPTS';
                    v_candidate_branch := 'FAILED_CLOSED';
                    v_candidate_transition_proven := true;
                  ELSE
                    v_candidate_skipped := true;
                    v_candidate_action := 'SKIPPED_AFTER_RECHECK';
                  END IF;
                ELSE
                  RAISE EXCEPTION 'PAY_WORKBENCH_OWNER_REPAIR_POSTCONDITION_NOT_PROVEN'
                    USING ERRCODE = 'P0001';
                END IF;
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

                  IF v_owner_generation_obsolete
                     AND lower(BTRIM(COALESCE(
                       v_owner.payload_json->>'bounded_scope_state_precedes_job','false'
                     ))) IN ('true','t','1','yes','y','on')
                     AND COALESCE(v_owner.payload_json->>'scope_change_tx_token','') ~*
                       '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                     AND COALESCE(v_owner.payload_json->>'scope_change_generation','') ~ '^\d+$' THEN
                    v_enqueue_payload := v_enqueue_payload || jsonb_build_object(
                      'bounded_scope_state_precedes_job',true,
                      'scope_change_tx_token',v_owner.payload_json->>'scope_change_tx_token',
                      'scope_change_generation',(v_owner.payload_json->>'scope_change_generation')::bigint
                    );
                  END IF;
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
                    AND COALESCE(successor_scope.dirty, false) IS TRUE
                    AND successor_scope.error_json IS NULL
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
                        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                  FOR UPDATE OF successor_job, successor_scope;
                  v_successor_valid := FOUND;

                  SELECT final_successor_scope.*
                  INTO v_post_scope
                  FROM public.banking_pay_workbench_session_scope AS final_successor_scope
                  WHERE final_successor_scope.id = v_scope.id
                  FOR UPDATE;

                  IF v_successor_valid IS NOT TRUE
                     OR UPPER(BTRIM(COALESCE(v_post_scope.status, ''))) <> 'SOURCE_BUILD_PENDING'
                     OR v_post_scope.pending_job_id IS DISTINCT FROM v_successor_job_id
                     OR COALESCE(v_post_scope.dirty, false) IS NOT TRUE
                     OR v_post_scope.error_json IS NOT NULL THEN
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

                  v_candidate_action := 'ENQUEUED_CANONICAL_SUCCESSOR';
                  v_candidate_branch := 'ENQUEUED';
                  v_candidate_transition_proven := true;
                EXCEPTION WHEN OTHERS THEN
                  v_successor_job_id := NULL::uuid;
                  v_successor_run_id_text := NULL::text;
                  v_successor_valid := false;
                  v_candidate_failure_code := 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB';
                  v_candidate_failure_message := 'Candidate refresh could not be scheduled automatically. Refresh Banking Pay or retry the candidate.';

                  UPDATE public.banking_pay_workbench_session_scope AS failed_repair_scope
                  SET status = 'SOURCE_BUILD_ERROR',
                      pending_job_id = NULL::uuid,
                      dirty = true,
                      error_json = jsonb_build_object(
                        'code', v_candidate_failure_code,
                        'message', v_candidate_failure_message,
                        'job_id', CASE WHEN v_owner.id IS NULL THEN NULL ELSE v_owner.id::text END,
                        'automatic_recovery_scheduled', false,
                        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                      ),
                      updated_at_utc = v_now
                  WHERE failed_repair_scope.id = v_scope.id
                    AND failed_repair_scope.pending_job_id IS NOT DISTINCT FROM v_scope.pending_job_id;

                  GET DIAGNOSTICS v_scope_transition_row_count = ROW_COUNT;

                  SELECT failed_repair_post_scope.*
                  INTO v_post_scope
                  FROM public.banking_pay_workbench_session_scope AS failed_repair_post_scope
                  WHERE failed_repair_post_scope.id = v_scope.id
                  FOR UPDATE;

                  IF UPPER(BTRIM(COALESCE(v_post_scope.status, ''))) = 'SOURCE_BUILD_ERROR'
                     AND v_post_scope.pending_job_id IS NULL
                     AND COALESCE(v_post_scope.dirty, false) IS TRUE
                     AND v_post_scope.error_json->>'code' = 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB'
                     AND LOWER(BTRIM(COALESCE(v_post_scope.error_json->>'automatic_recovery_scheduled', ''))) = 'false' THEN
                    IF v_scope_transition_row_count = 1 THEN
                      v_candidate_action := 'FAILED_CLOSED_REPAIR_ERROR';
                      v_candidate_branch := 'FAILED_CLOSED';
                      v_candidate_transition_proven := true;
                    ELSE
                      v_candidate_skipped := true;
                      v_candidate_action := 'SKIPPED_AFTER_RECHECK';
                    END IF;
                  ELSE
                    RAISE EXCEPTION 'PAY_WORKBENCH_OWNER_REPAIR_POSTCONDITION_NOT_PROVEN'
                      USING ERRCODE = 'P0001';
                  END IF;
                END;
              END IF;
            END IF;

            IF v_candidate_skipped IS NOT TRUE
               AND v_candidate_transition_proven IS NOT TRUE THEN
              RAISE EXCEPTION 'PAY_WORKBENCH_OWNER_REPAIR_POSTCONDITION_NOT_PROVEN'
                USING ERRCODE = 'P0001';
            END IF;

            IF v_candidate_transition_proven THEN
              BEGIN
                v_progress_recompute_result := public.pay_workbench_session_recompute_progress_counters(
                  p_session_id => v_scope.session_id,
                  p_apply => true,
                  p_reason => CASE v_candidate_action
                    WHEN 'RECONCILED_SUCCESSFUL_BUILD' THEN 'OWNER_REPAIR_RECONCILED_SUCCESSFUL_BUILD'
                    WHEN 'REBOUND_ACTIVE_SUCCESSOR' THEN 'OWNER_REPAIR_REBOUND_ACTIVE_SUCCESSOR'
                    WHEN 'ENQUEUED_CANONICAL_SUCCESSOR' THEN 'OWNER_REPAIR_ENQUEUED_CANONICAL_SUCCESSOR'
                    WHEN 'FAILED_CLOSED_MAX_ATTEMPTS' THEN 'OWNER_REPAIR_FAILED_CLOSED_MAX_ATTEMPTS'
                    ELSE 'OWNER_REPAIR_FAILED_CLOSED_REPAIR_ERROR'
                  END,
                  p_write_progress_json => true
                );

                v_progress_recomputed := jsonb_typeof(v_progress_recompute_result) = 'object'
                  AND LOWER(BTRIM(COALESCE(v_progress_recompute_result->>'ok', ''))) = 'true';

                IF v_progress_recomputed IS NOT TRUE THEN
                  RAISE EXCEPTION 'PAY_WORKBENCH_OWNER_REPAIR_PROGRESS_RECOMPUTE_NOT_CONFIRMED'
                    USING ERRCODE = 'P0001';
                END IF;
              EXCEPTION WHEN OTHERS THEN
                v_progress_recomputed := false;
                v_progress_recompute_error_code := SQLSTATE;
              END;

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
                    'action', v_candidate_action,
                    'successor_job_id', CASE WHEN v_successor_job_id IS NULL THEN NULL ELSE v_successor_job_id::text END,
                    'automatic_recovery_scheduled', v_candidate_action IN ('REBOUND_ACTIVE_SUCCESSOR', 'ENQUEUED_CANONICAL_SUCCESSOR'),
                    'state_transition_proven', true,
                    'progress_recomputed', v_progress_recomputed,
                    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
                  ),
                  'PAY_WORKBENCH_PENDING_SOURCE_BUILD_OWNER_REPAIR',
                  v_session.actor_user_id
                );
              EXCEPTION WHEN OTHERS THEN
                v_audit_failed := true;
              END;
            END IF;
          END IF;
        END IF;
      END IF;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      v_candidate_transition_proven := false;
      v_candidate_unresolved := true;
      v_candidate_skipped := false;
      v_candidate_action := 'UNRESOLVED_POSTCONDITION_NOT_PROVEN';
      v_candidate_branch := NULL::text;
      v_candidate_failure_code := 'WORKBENCH_PENDING_SCOPE_WITHOUT_ACTIVE_JOB';
      v_candidate_failure_message := NULL::text;
      v_candidate_unresolved_reason := 'POSTCONDITION_NOT_PROVEN';
      v_successor_job_id := NULL::uuid;
      v_progress_recomputed := false;
      v_progress_recompute_error_code := NULL::text;
      v_audit_failed := false;
    END;

    IF v_candidate_skipped THEN
      v_skipped_count := v_skipped_count + 1;
    ELSIF v_candidate_unresolved THEN
      v_unresolved_count := v_unresolved_count + 1;
      v_result_rows := v_result_rows || jsonb_build_array(
        jsonb_strip_nulls(jsonb_build_object(
          'session_id', v_candidate.session_id::text,
          'candidate_id', v_candidate.candidate_id::text,
          'old_pending_job_id', CASE WHEN v_candidate_old_pending_job_id IS NULL THEN NULL ELSE v_candidate_old_pending_job_id::text END,
          'owner_failure_reason', COALESCE(v_owner_reason, 'PENDING_OWNER_REVALIDATION_FAILED'),
          'action', v_candidate_action,
          'automatic_recovery_scheduled', false,
          'failure_code', v_candidate_failure_code,
          'unresolved_reason', v_candidate_unresolved_reason,
          'retry_safe', true,
          'audit_failed', v_audit_failed
        )) || jsonb_build_object(
          'state_transition_proven', false,
          'repaired', false,
          'progress_recomputed', false,
          'progress_recompute_error_code', NULL::text
        )
      );
    ELSIF v_candidate_transition_proven THEN
      v_repaired_count := v_repaired_count + 1;

      CASE v_candidate_branch
        WHEN 'RECONCILED' THEN
          v_reconciled_count := v_reconciled_count + 1;
        WHEN 'REBOUND' THEN
          v_rebound_count := v_rebound_count + 1;
        WHEN 'ENQUEUED' THEN
          v_enqueued_count := v_enqueued_count + 1;
        WHEN 'FAILED_CLOSED' THEN
          v_failed_closed_count := v_failed_closed_count + 1;
        ELSE
          NULL;
      END CASE;

      IF v_progress_recomputed THEN
        v_progress_recomputed_count := v_progress_recomputed_count + 1;
      ELSE
        v_progress_recompute_failed_count := v_progress_recompute_failed_count + 1;
      END IF;

      v_result_rows := v_result_rows || jsonb_build_array(
        jsonb_strip_nulls(jsonb_build_object(
          'session_id', v_scope.session_id::text,
          'candidate_id', v_scope.candidate_id::text,
          'old_pending_job_id', CASE WHEN v_scope.pending_job_id IS NULL THEN NULL ELSE v_scope.pending_job_id::text END,
          'owner_failure_reason', v_owner_reason,
          'action', v_candidate_action,
          'successor_job_id', CASE WHEN v_successor_job_id IS NULL THEN NULL ELSE v_successor_job_id::text END,
          'automatic_recovery_scheduled', v_candidate_action IN ('REBOUND_ACTIVE_SUCCESSOR', 'ENQUEUED_CANONICAL_SUCCESSOR'),
          'failure_code', v_candidate_failure_code,
          'message', v_candidate_failure_message,
          'audit_failed', v_audit_failed
        )) || jsonb_build_object(
          'state_transition_proven', true,
          'repaired', true,
          'progress_recomputed', v_progress_recomputed,
          'progress_recompute_error_code', v_progress_recompute_error_code
        )
      );
    END IF;
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'repair_code', 'PAY_WORKBENCH_PENDING_SOURCE_BUILD_OWNER_REPAIR',
    'expired_attempt_count',v_expired_attempt_count,
    'expired_attempt_requeued_count',v_expired_requeued_count,
    'expired_attempt_exhausted_count',v_expired_exhausted_count,
    'examined_count', jsonb_array_length(v_result_rows) + v_skipped_count,
    'repaired_count', v_repaired_count,
    'reconciled_count', v_reconciled_count,
    'rebound_count', v_rebound_count,
    'enqueued_count', v_enqueued_count,
    'failed_closed_count', v_failed_closed_count,
    'skipped_count', v_skipped_count,
    'unresolved_count', v_unresolved_count,
    'progress_recomputed_count', v_progress_recomputed_count,
    'progress_recompute_failed_count', v_progress_recompute_failed_count,
    'all_state_transitions_proven', v_unresolved_count = 0,
    'all_progress_recomputed', v_progress_recompute_failed_count = 0,
    'partial', v_unresolved_count > 0 OR v_progress_recompute_failed_count > 0,
    'automatic_recovery_scheduled', (v_rebound_count + v_enqueued_count) > 0,
    'results', v_result_rows,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(p_session_id uuid, p_candidate_id uuid, p_limit integer, p_now_utc timestamp with time zone, p_reason text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(p_session_id uuid, p_candidate_id uuid, p_limit integer, p_now_utc timestamp with time zone, p_reason text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(p_session_id uuid, p_candidate_id uuid, p_limit integer, p_now_utc timestamp with time zone, p_reason text) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_repair_orphaned_pending_source_build(p_session_id uuid, p_candidate_id uuid, p_limit integer, p_now_utc timestamp with time zone, p_reason text) TO service_role;
