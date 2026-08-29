-- Banking Pay discarded-session source-build blocker repair.
-- Policy X: pre-Draft orchestration only; no payment economics, selection,
-- Draft, provider, settlement, remittance, cancellation, or communications.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_repair_discarded_session_blockers_v1(
  p_session_id uuid,
  p_candidate_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 10,
  p_reason text DEFAULT 'DISCARDED_SESSION_SOURCE_BUILD_BLOCKER_REPAIR'::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_now timestamptz:=clock_timestamp();
  v_limit integer:=LEAST(GREATEST(COALESCE(p_limit,10),1),25);
  v_reason text:=COALESCE(NULLIF(BTRIM(COALESCE(p_reason,'')),''),
    'DISCARDED_SESSION_SOURCE_BUILD_BLOCKER_REPAIR');
  v_candidate record;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_attempt_count integer:=0;
  v_job_count integer:=0;
  v_build_count integer:=0;
  v_total_attempt_count integer:=0;
  v_total_job_count integer:=0;
  v_total_build_count integer:=0;
  v_repaired_count integer:=0;
  v_skipped_count integer:=0;
  v_failed_count integer:=0;
  v_owner_repair_result jsonb:='{}'::jsonb;
  v_progress_result jsonb:='{}'::jsonb;
  v_results jsonb:='[]'::jsonb;
BEGIN
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DISCARDED_BLOCKER_REPAIR_SESSION_REQUIRED'
      USING ERRCODE='22023';
  END IF;

  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  FOR v_candidate IN
    SELECT current_scope.session_id,current_scope.candidate_id
    FROM public.banking_pay_workbench_session_scope AS current_scope
    JOIN public.banking_pay_workbench_sessions AS current_session
      ON current_session.id=current_scope.session_id
    WHERE current_scope.session_id=p_session_id
      AND UPPER(BTRIM(COALESCE(current_session.status,'')))='OPEN'
      AND current_session.discarded_at_utc IS NULL
      AND UPPER(BTRIM(COALESCE(current_scope.status,'')))='SOURCE_BUILD_PENDING'
      AND (p_candidate_id IS NULL OR current_scope.candidate_id=p_candidate_id)
      AND EXISTS(
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS stale_job
        JOIN public.banking_pay_workbench_sessions AS stale_session
          ON stale_session.id=stale_job.session_id
        WHERE stale_job.candidate_id=current_scope.candidate_id
          AND stale_job.session_id IS DISTINCT FROM current_scope.session_id
          AND UPPER(BTRIM(COALESCE(stale_job.job_type,'')))=
            'WORKBENCH_CANDIDATE_SOURCE_BUILD'
          AND UPPER(BTRIM(COALESCE(stale_job.status,''))) IN ('QUEUED','RUNNING')
          AND (UPPER(BTRIM(COALESCE(stale_session.status,'')))='DISCARDED'
            OR stale_session.discarded_at_utc IS NOT NULL)
      )
    ORDER BY current_scope.updated_at_utc ASC NULLS FIRST,current_scope.candidate_id
    LIMIT v_limit
  LOOP
    v_attempt_count:=0;
    v_job_count:=0;
    v_build_count:=0;
    v_owner_repair_result:='{}'::jsonb;
    v_progress_result:='{}'::jsonb;

    BEGIN
      IF NOT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(
        public._pay_workbench_candidate_serial_key(v_candidate.candidate_id),24062027
      )) THEN
        v_skipped_count:=v_skipped_count+1;
        v_results:=v_results || jsonb_build_array(jsonb_build_object(
          'session_id',v_candidate.session_id::text,
          'candidate_id',v_candidate.candidate_id::text,
          'action','SKIPPED_CANDIDATE_SERIAL_BUSY'));
        CONTINUE;
      END IF;

      PERFORM 1
      FROM private.banking_pay_workbench_candidate_scope_registry AS registry
      WHERE registry.candidate_id=v_candidate.candidate_id
      FOR UPDATE;

      SELECT session_row.* INTO STRICT v_session
      FROM public.banking_pay_workbench_sessions AS session_row
      WHERE session_row.id=v_candidate.session_id
        AND UPPER(BTRIM(COALESCE(session_row.status,'')))='OPEN'
        AND session_row.discarded_at_utc IS NULL
      FOR UPDATE;

      SELECT scope_row.* INTO STRICT v_scope
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE scope_row.session_id=v_candidate.session_id
        AND scope_row.candidate_id=v_candidate.candidate_id
        AND UPPER(BTRIM(COALESCE(scope_row.status,'')))='SOURCE_BUILD_PENDING'
      FOR UPDATE;

      -- Established lock order: candidate registry, build, job, attempt.
      PERFORM 1
      FROM private.banking_pay_workbench_economic_builds AS stale_build
      WHERE stale_build.candidate_id=v_candidate.candidate_id
        AND stale_build.session_id IS DISTINCT FROM v_candidate.session_id
        AND EXISTS(
          SELECT 1
          FROM public.banking_pay_workbench_jobs AS stale_job
          JOIN public.banking_pay_workbench_sessions AS stale_session
            ON stale_session.id=stale_job.session_id
          WHERE stale_job.economic_build_id=stale_build.id
            AND stale_job.candidate_id=v_candidate.candidate_id
            AND stale_job.session_id=stale_build.session_id
            AND UPPER(BTRIM(COALESCE(stale_job.job_type,'')))=
              'WORKBENCH_CANDIDATE_SOURCE_BUILD'
            AND UPPER(BTRIM(COALESCE(stale_job.status,''))) IN ('QUEUED','RUNNING')
            AND (UPPER(BTRIM(COALESCE(stale_session.status,'')))='DISCARDED'
              OR stale_session.discarded_at_utc IS NOT NULL)
        )
      ORDER BY stale_build.id
      FOR UPDATE;

      PERFORM 1
      FROM public.banking_pay_workbench_jobs AS stale_job
      JOIN public.banking_pay_workbench_sessions AS stale_session
        ON stale_session.id=stale_job.session_id
      WHERE stale_job.candidate_id=v_candidate.candidate_id
        AND stale_job.session_id IS DISTINCT FROM v_candidate.session_id
        AND UPPER(BTRIM(COALESCE(stale_job.job_type,'')))=
          'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND UPPER(BTRIM(COALESCE(stale_job.status,''))) IN ('QUEUED','RUNNING')
        AND (UPPER(BTRIM(COALESCE(stale_session.status,'')))='DISCARDED'
          OR stale_session.discarded_at_utc IS NOT NULL)
      ORDER BY stale_job.id
      FOR UPDATE OF stale_job;

      PERFORM 1
      FROM private.banking_pay_workbench_stage_attempts AS stale_attempt
      JOIN public.banking_pay_workbench_jobs AS stale_job
        ON stale_job.id=stale_attempt.job_id
      JOIN public.banking_pay_workbench_sessions AS stale_session
        ON stale_session.id=stale_job.session_id
      WHERE stale_attempt.candidate_id=v_candidate.candidate_id
        AND stale_attempt.attempt_status='STARTED'
        AND stale_job.candidate_id=v_candidate.candidate_id
        AND stale_job.session_id IS DISTINCT FROM v_candidate.session_id
        AND UPPER(BTRIM(COALESCE(stale_job.job_type,'')))=
          'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND UPPER(BTRIM(COALESCE(stale_job.status,''))) IN ('QUEUED','RUNNING')
        AND (UPPER(BTRIM(COALESCE(stale_session.status,'')))='DISCARDED'
          OR stale_session.discarded_at_utc IS NOT NULL)
      ORDER BY stale_attempt.id
      FOR UPDATE OF stale_attempt;

      UPDATE private.banking_pay_workbench_stage_attempts AS stale_attempt
      SET attempt_status='OBSOLETE',obsolete_at_utc=v_now,
        result_code='DISCARDED_SESSION_SOURCE_BUILD_TERMINALISED',
        error_class='DISCARDED_SESSION_SOURCE_BUILD',
        error_json=jsonb_build_object(
          'code','DISCARDED_SESSION_SOURCE_BUILD_TERMINALISED',
          'message','Obsolete source-build work from a discarded Banking Pay session was terminated.',
          'repair_reason',v_reason,'current_session_id',v_candidate.session_id::text,
          'repaired_at_utc',v_now::text),
        updated_at_utc=v_now
      FROM public.banking_pay_workbench_jobs AS stale_job,
        public.banking_pay_workbench_sessions AS stale_session
      WHERE stale_attempt.job_id=stale_job.id
        AND stale_session.id=stale_job.session_id
        AND stale_attempt.candidate_id=v_candidate.candidate_id
        AND stale_attempt.attempt_status='STARTED'
        AND stale_job.candidate_id=v_candidate.candidate_id
        AND stale_job.session_id IS DISTINCT FROM v_candidate.session_id
        AND UPPER(BTRIM(COALESCE(stale_job.job_type,'')))=
          'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND UPPER(BTRIM(COALESCE(stale_job.status,''))) IN ('QUEUED','RUNNING')
        AND (UPPER(BTRIM(COALESCE(stale_session.status,'')))='DISCARDED'
          OR stale_session.discarded_at_utc IS NOT NULL);
      GET DIAGNOSTICS v_attempt_count=ROW_COUNT;

      UPDATE public.banking_pay_workbench_jobs AS stale_job
      SET status='DEAD',failed_at_utc=COALESCE(stale_job.failed_at_utc,v_now),
        updated_at_utc=v_now,
        private_stage=CASE WHEN stale_job.economic_build_id IS NULL
          AND stale_job.private_stage='BUILD_INITIALISE' THEN NULL::text
          ELSE stale_job.private_stage END,
        private_cursor_kind=CASE WHEN stale_job.economic_build_id IS NULL
          AND stale_job.private_stage='BUILD_INITIALISE' THEN NULL::text
          ELSE stale_job.private_cursor_kind END,
        private_cursor_json=CASE WHEN stale_job.economic_build_id IS NULL
          AND stale_job.private_stage='BUILD_INITIALISE' THEN '{}'::jsonb
          ELSE stale_job.private_cursor_json END,
        private_stage_version=CASE WHEN stale_job.economic_build_id IS NULL
          AND stale_job.private_stage='BUILD_INITIALISE' THEN NULL::integer
          ELSE stale_job.private_stage_version END,
        last_error_json=jsonb_build_object(
          'code','DISCARDED_SESSION_SOURCE_BUILD_TERMINALISED',
          'message','Obsolete source-build work from a discarded Banking Pay session was terminated.',
          'repair_reason',v_reason,'current_session_id',v_candidate.session_id::text,
          'repaired_at_utc',v_now::text),
        payload_json=COALESCE(stale_job.payload_json,'{}'::jsonb)
          || jsonb_build_object(
            'discarded_session_blocker_terminalised',true,
            'discarded_session_blocker_terminalised_at_utc',v_now::text,
            'discarded_session_blocker_repair_reason',v_reason,
            'current_session_id',v_candidate.session_id::text)
      FROM public.banking_pay_workbench_sessions AS stale_session
      WHERE stale_session.id=stale_job.session_id
        AND stale_job.candidate_id=v_candidate.candidate_id
        AND stale_job.session_id IS DISTINCT FROM v_candidate.session_id
        AND UPPER(BTRIM(COALESCE(stale_job.job_type,'')))=
          'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND UPPER(BTRIM(COALESCE(stale_job.status,''))) IN ('QUEUED','RUNNING')
        AND (UPPER(BTRIM(COALESCE(stale_session.status,'')))='DISCARDED'
          OR stale_session.discarded_at_utc IS NOT NULL);
      GET DIAGNOSTICS v_job_count=ROW_COUNT;

      IF v_job_count=0 THEN
        v_skipped_count:=v_skipped_count+1;
        v_results:=v_results || jsonb_build_array(jsonb_build_object(
          'session_id',v_candidate.session_id::text,
          'candidate_id',v_candidate.candidate_id::text,
          'action','SKIPPED_AFTER_RECHECK'));
        CONTINUE;
      END IF;

      UPDATE private.banking_pay_workbench_economic_builds AS stale_build
      SET status='OBSOLETE',obsolete_at_utc=COALESCE(stale_build.obsolete_at_utc,v_now),
        updated_at_utc=v_now,
        failure_json=jsonb_build_object(
          'code','DISCARDED_SESSION_SOURCE_BUILD_TERMINALISED',
          'message','Obsolete source-build work from a discarded Banking Pay session was terminated.',
          'repair_reason',v_reason,'current_session_id',v_candidate.session_id::text,
          'repaired_at_utc',v_now::text)
      WHERE stale_build.candidate_id=v_candidate.candidate_id
        AND stale_build.session_id IS DISTINCT FROM v_candidate.session_id
        AND stale_build.status NOT IN ('COMPLETE','OBSOLETE','FAILED')
        AND EXISTS(
          SELECT 1
          FROM public.banking_pay_workbench_jobs AS terminal_job
          JOIN public.banking_pay_workbench_sessions AS terminal_session
            ON terminal_session.id=terminal_job.session_id
          WHERE terminal_job.economic_build_id=stale_build.id
            AND terminal_job.candidate_id=v_candidate.candidate_id
            AND terminal_job.session_id=stale_build.session_id
            AND UPPER(BTRIM(COALESCE(terminal_job.job_type,'')))=
              'WORKBENCH_CANDIDATE_SOURCE_BUILD'
            AND terminal_job.status='DEAD'
            AND LOWER(BTRIM(COALESCE(terminal_job.payload_json
              ->>'discarded_session_blocker_terminalised','false')))
              IN ('true','t','1','yes','y','on')
            AND (UPPER(BTRIM(COALESCE(terminal_session.status,'')))='DISCARDED'
              OR terminal_session.discarded_at_utc IS NOT NULL));
      GET DIAGNOSTICS v_build_count=ROW_COUNT;

      IF EXISTS(
        SELECT 1
        FROM public.banking_pay_workbench_jobs AS remaining_job
        JOIN public.banking_pay_workbench_sessions AS remaining_session
          ON remaining_session.id=remaining_job.session_id
        WHERE remaining_job.candidate_id=v_candidate.candidate_id
          AND remaining_job.session_id IS DISTINCT FROM v_candidate.session_id
          AND UPPER(BTRIM(COALESCE(remaining_job.job_type,'')))=
            'WORKBENCH_CANDIDATE_SOURCE_BUILD'
          AND UPPER(BTRIM(COALESCE(remaining_job.status,''))) IN ('QUEUED','RUNNING')
          AND (UPPER(BTRIM(COALESCE(remaining_session.status,'')))='DISCARDED'
            OR remaining_session.discarded_at_utc IS NOT NULL)
      ) OR EXISTS(
        SELECT 1
        FROM private.banking_pay_workbench_stage_attempts AS remaining_attempt
        JOIN public.banking_pay_workbench_jobs AS remaining_job
          ON remaining_job.id=remaining_attempt.job_id
        JOIN public.banking_pay_workbench_sessions AS remaining_session
          ON remaining_session.id=remaining_job.session_id
        WHERE remaining_attempt.candidate_id=v_candidate.candidate_id
          AND remaining_attempt.attempt_status='STARTED'
          AND remaining_job.session_id IS DISTINCT FROM v_candidate.session_id
          AND UPPER(BTRIM(COALESCE(remaining_job.job_type,'')))=
            'WORKBENCH_CANDIDATE_SOURCE_BUILD'
          AND (UPPER(BTRIM(COALESCE(remaining_session.status,'')))='DISCARDED'
            OR remaining_session.discarded_at_utc IS NOT NULL)
      ) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_DISCARDED_BLOCKER_REPAIR_POSTCONDITION_FAILED'
          USING ERRCODE='P0001';
      END IF;

      v_owner_repair_result:=public.pay_workbench_repair_orphaned_pending_source_build(
        v_candidate.session_id,v_candidate.candidate_id,1,v_now,
        'DISCARDED_SESSION_BLOCKER_CURRENT_OWNER_REVALIDATION');

      v_progress_result:=public.pay_workbench_session_recompute_progress_counters(
        p_session_id=>v_candidate.session_id,p_apply=>true,
        p_reason=>'DISCARDED_SESSION_SOURCE_BUILD_BLOCKER_REPAIRED',
        p_write_progress_json=>true);
      IF jsonb_typeof(v_progress_result) IS DISTINCT FROM 'object'
        OR LOWER(BTRIM(COALESCE(v_progress_result->>'ok','false')))
          NOT IN ('true','t','1','yes','y','on') THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_DISCARDED_BLOCKER_PROGRESS_RECOMPUTE_FAILED'
          USING ERRCODE='P0001';
      END IF;

      BEGIN
        PERFORM public._audit_insert(
          'banking_pay_workbench_session_scope',v_candidate.candidate_id::text,
          'DISCARDED_SESSION_SOURCE_BUILD_BLOCKER_REPAIRED',
          jsonb_build_object('session_id',v_candidate.session_id::text,
            'candidate_id',v_candidate.candidate_id::text),
          jsonb_build_object('session_id',v_candidate.session_id::text,
            'candidate_id',v_candidate.candidate_id::text,
            'terminalised_attempt_count',v_attempt_count,
            'terminalised_job_count',v_job_count,
            'obsoleted_build_count',v_build_count,
            'policy_x_authority_scope','PRE_DRAFT_WORKBENCH_REPAIR_ONLY'),
          v_reason,v_session.actor_user_id);
      EXCEPTION WHEN OTHERS THEN
        NULL;
      END;

      v_total_attempt_count:=v_total_attempt_count+v_attempt_count;
      v_total_job_count:=v_total_job_count+v_job_count;
      v_total_build_count:=v_total_build_count+v_build_count;
      v_repaired_count:=v_repaired_count+1;
      v_results:=v_results || jsonb_build_array(jsonb_build_object(
        'session_id',v_candidate.session_id::text,
        'candidate_id',v_candidate.candidate_id::text,
        'action','REPAIRED_DISCARDED_SESSION_BLOCKER',
        'terminalised_attempt_count',v_attempt_count,
        'terminalised_job_count',v_job_count,
        'obsoleted_build_count',v_build_count,
        'current_owner_repair_result',v_owner_repair_result,
        'progress_recomputed',true));
    EXCEPTION WHEN OTHERS THEN
      v_failed_count:=v_failed_count+1;
      v_results:=v_results || jsonb_build_array(jsonb_build_object(
        'session_id',v_candidate.session_id::text,
        'candidate_id',v_candidate.candidate_id::text,
        'action','REPAIR_FAILED_NO_PARTIAL_ADOPTION',
        'code',SQLSTATE,'message',SQLERRM));
    END;
  END LOOP;

  RETURN jsonb_build_object(
    'ok',v_failed_count=0,
    'partial',v_failed_count>0 AND v_repaired_count>0,
    'examined_count',v_repaired_count+v_skipped_count+v_failed_count,
    'repaired_candidate_count',v_repaired_count,
    'terminalised_attempt_count',v_total_attempt_count,
    'terminalised_job_count',v_total_job_count,
    'obsoleted_build_count',v_total_build_count,
    'skipped_count',v_skipped_count,'failed_count',v_failed_count,
    'automatic_recovery_scheduled',v_repaired_count>0,
    'results',v_results,
    'policy_x_authority_scope','PRE_DRAFT_WORKBENCH_REPAIR_ONLY');
END;
$function$;

ALTER FUNCTION public.pay_workbench_repair_discarded_session_blockers_v1(
  uuid,uuid,integer,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_repair_discarded_session_blockers_v1(
  uuid,uuid,integer,text) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_repair_discarded_session_blockers_v1(
  uuid,uuid,integer,text) TO postgres,service_role;

NOTIFY pgrst, 'reload schema';

commit;
