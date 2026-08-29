-- Read-only stage-job observations for the modal. The existing enqueue,
-- executor, source-publication and progress owners are not replaced.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_stage_job_facts_v2(
  p_session public.banking_pay_workbench_sessions
) RETURNS TABLE(candidate_id uuid,job_type text,job_facts jsonb)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
BEGIN
  IF p_session.id IS NULL OR p_session.status<>'OPEN' OR p_session.discarded_at_utc IS NOT NULL
    OR p_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'OBSOLETE_SESSION' USING ERRCODE='P0001';
  END IF;
  RETURN QUERY
  WITH jobs AS MATERIALIZED (
    SELECT j.*,UPPER(BTRIM(COALESCE(j.status,''))) AS status_key,
      CASE
        WHEN UPPER(BTRIM(j.job_type)) IN ('WORKBENCH_SESSION_SCOPE_SEED','SESSION_SCOPE_SEED','WORKBENCH_SCOPE_SEED','WORKBENCH_SCOPE_SEED_PAGE','SCOPE_SEED_PAGE') THEN 'WORKBENCH_SESSION_SCOPE_SEED'
        WHEN UPPER(BTRIM(j.job_type)) IN ('WORKBENCH_CANDIDATE_DELTA_REFRESH','CANDIDATE_DELTA_REFRESH','DELTA_REFRESH') THEN 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
        WHEN UPPER(BTRIM(j.job_type)) IN ('WORKBENCH_SESSION_CLONE_REBASE','SESSION_CLONE_REBASE','CLONE_REBASE') THEN 'WORKBENCH_SESSION_CLONE_REBASE'
        WHEN UPPER(BTRIM(j.job_type)) IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD','WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK','WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE','CANDIDATE_SOURCE_BUILD','CANDIDATE_SOURCE_BUILD_CHUNK','SOURCE_BUILD','SOURCE_BUILD_PAGE') THEN 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
        WHEN UPPER(BTRIM(j.job_type)) IN ('WORKBENCH_CANDIDATE_LINE_WORK_SEED','WORKBENCH_CANDIDATE_LINE_WORK_SEED_PAGE','CANDIDATE_LINE_WORK_SEED','CANDIDATE_LINE_WORK_SEED_PAGE','LINE_WORK_SEED_PAGE','SNAPSHOT_CANDIDATE_REFRESH','CANDIDATE_REFRESH') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_SEED'
        WHEN UPPER(BTRIM(j.job_type)) IN ('WORKBENCH_CANDIDATE_LINE_WORK_PROCESS','WORKBENCH_CANDIDATE_LINE_WORK_PROCESS_CHUNK','CANDIDATE_LINE_WORK_PROCESS','CANDIDATE_LINE_WORK_PROCESS_CHUNK','LINE_WORK_PROCESS','LINE_WORK_PROCESS_CHUNK') THEN 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS'
        WHEN UPPER(BTRIM(j.job_type)) IN ('WORKBENCH_PREVIEW_ROWS_MATERIALISE','WORKBENCH_PREVIEW_ROWS_MATERIALIZE','WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK','WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK','PREVIEW_ROWS_MATERIALISE','PREVIEW_ROWS_MATERIALIZE','PREVIEW_ROWS_MATERIALISE_CHUNK','PREVIEW_ROWS_MATERIALIZE_CHUNK','PREVIEW_ROW_MATERIALISE_CHUNK','PREVIEW_ROW_MATERIALIZE_CHUNK') THEN 'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
      END AS family,
      CASE WHEN COALESCE(j.payload_json->>'source_change_seq','')~'^[0-9]{1,18}$'
        THEN (j.payload_json->>'source_change_seq')::bigint END AS source_seq,
      CASE WHEN COALESCE(NULLIF(j.payload_json->>'source_session_id',''),NULLIF(j.payload_json->>'clone_from_session_id',''),
        NULLIF(j.payload_json->>'sourceSessionId',''),NULLIF(j.payload_json->>'cloneFromSessionId',''),
        NULLIF(j.payload_json->>'replacement_source_session_id','')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN COALESCE(NULLIF(j.payload_json->>'source_session_id',''),NULLIF(j.payload_json->>'clone_from_session_id',''),
          NULLIF(j.payload_json->>'sourceSessionId',''),NULLIF(j.payload_json->>'cloneFromSessionId',''),
          NULLIF(j.payload_json->>'replacement_source_session_id',''))::uuid END AS clone_source_id
    FROM public.banking_pay_workbench_jobs j
    WHERE j.session_id=p_session.id AND UPPER(BTRIM(COALESCE(j.status,''))) IN ('QUEUED','RUNNING','FAILED','DEAD')
      AND j.completed_at_utc IS NULL
      AND CASE WHEN COALESCE(j.payload_json->>'session_version','')~'^[0-9]{1,18}$'
        THEN (j.payload_json->>'session_version')::bigint END=p_session.version
      -- Table ownership is primary. Legacy optional mirrors must not contradict
      -- it, but are not invented when absent from an established stage payload.
      AND (j.payload_json->>'session_id' IS NULL OR j.payload_json->>'session_id'=p_session.id::text)
      AND (j.payload_json->>'candidate_id' IS NULL OR j.payload_json->>'candidate_id'=j.candidate_id::text)
      AND (j.snapshot_run_id IS NULL OR j.snapshot_run_id=p_session.source_snapshot_run_id)
      AND (j.payload_json->>'snapshot_run_id' IS NULL OR j.payload_json->>'snapshot_run_id'=p_session.source_snapshot_run_id::text)
      AND (j.payload_json->>'session_signature' IS NULL OR j.payload_json->>'session_signature'=p_session.session_signature)
      AND (j.candidate_id IS NULL OR EXISTS(SELECT 1 FROM public.banking_pay_workbench_session_scope s
        WHERE s.session_id=p_session.id AND s.candidate_id=j.candidate_id))
  ), current_jobs AS MATERIALIZED (
    SELECT j.*,
      (j.economic_build_id IS NULL OR (b.id IS NOT NULL AND b.session_id=j.session_id
        AND b.candidate_id=j.candidate_id AND b.session_version=p_session.version AND b.private_stage=j.private_stage
        AND j.private_cursor_json->>'build_id'=b.id::text AND j.private_cursor_json->>'candidate_id'=b.candidate_id::text
        AND (j.private_cursor_json->>'captured_candidate_generation' IS NULL
          OR j.private_cursor_json->>'captured_candidate_generation'=b.captured_candidate_generation::text)
        AND (j.private_cursor_json->>'captured_source_change_seq' IS NULL
          OR j.private_cursor_json->>'captured_source_change_seq'=b.source_change_seq::text)
        AND b.source_change_seq>=COALESCE(c.seq,0)
        AND b.status IN ('COLLECTING','READY_FOR_RECONCILIATION','RECONCILING','RECONCILED','PUBLISHING'))) AS build_current,
      (j.family<>'WORKBENCH_SESSION_CLONE_REBASE' OR clone_source.id IS NOT NULL
        -- The existing clone owner may choose a reusable source for one exact
        -- candidate. Do not require a source ID before that existing selection.
        OR (j.candidate_id IS NOT NULL AND COALESCE(NULLIF(j.payload_json->>'direct_candidate_id',''),
          NULLIF(j.payload_json->>'candidate_id',''))=j.candidate_id::text)) AS clone_context_valid
    FROM jobs j
    LEFT JOIN public.app_change_counters c ON c.entity_key='pay_candidate:'||j.candidate_id::text
    LEFT JOIN private.banking_pay_workbench_economic_builds b ON b.id=j.economic_build_id
    LEFT JOIN public.banking_pay_workbench_sessions clone_source ON clone_source.id=j.clone_source_id
      AND clone_source.id<>p_session.id
      AND ((clone_source.status='OPEN' AND clone_source.discarded_at_utc IS NULL)
        OR (clone_source.status IN ('DISCARDED','REPLACED') AND clone_source.discarded_at_utc IS NOT NULL))
    WHERE j.family IS NOT NULL
      AND (j.family NOT IN ('WORKBENCH_CANDIDATE_SOURCE_BUILD','WORKBENCH_CANDIDATE_DELTA_REFRESH','WORKBENCH_CANDIDATE_LINE_WORK_SEED') OR j.candidate_id IS NOT NULL)
      -- Only SOURCE_BUILD has the original mandatory source/run predicate.
      -- Other current continuation owners may legitimately omit the sequence.
      AND (j.family<>'WORKBENCH_CANDIDATE_SOURCE_BUILD' OR (j.source_seq>=COALESCE(c.seq,0)
        AND COALESCE(j.payload_json->>'source_build_run_id','')~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'))
      AND (j.payload_json->>'source_change_seq' IS NULL OR j.source_seq>=COALESCE(c.seq,0))
      AND NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_jobs done
        WHERE done.id<>j.id AND done.session_id=j.session_id AND done.candidate_id IS NOT DISTINCT FROM j.candidate_id
          AND done.job_type=j.job_type AND done.dedupe_key=j.dedupe_key
          AND done.completed_at_utc IS NOT NULL AND done.failed_at_utc IS NULL)
  ), effective AS (
    SELECT DISTINCT ON(j.candidate_id,j.family) j.* FROM current_jobs j
    ORDER BY j.candidate_id,j.family,
      CASE WHEN j.status_key IN ('QUEUED','RUNNING') AND j.failed_at_utc IS NULL AND j.build_current AND j.clone_context_valid THEN 0
        WHEN j.status_key IN ('QUEUED','RUNNING') THEN 1 ELSE 2 END,
      j.source_seq DESC NULLS LAST,j.updated_at_utc DESC,j.created_at_utc DESC,j.id DESC
  )
  SELECT e.candidate_id,e.family,jsonb_build_object(
    'job_id',e.id,'job_type',e.family,'status',e.status_key,'source_change_seq',e.source_seq,
    'session_version',p_session.version,'is_failed',e.status_key IN ('FAILED','DEAD'),
    'can_progress',e.status_key IN ('QUEUED','RUNNING') AND e.failed_at_utc IS NULL AND e.build_current AND e.clone_context_valid,
    'blocked_code',CASE WHEN NOT e.build_current THEN 'STAGE_BUILD_NOT_CURRENT' WHEN NOT e.clone_context_valid THEN 'STAGE_CLONE_SOURCE_MISSING' END,
    'job_generation',encode(extensions.digest(convert_to(jsonb_build_array(e.id,e.family,e.status_key,
      p_session.version,e.source_seq,e.updated_at_utc,e.failed_at_utc,e.attempt_count,e.build_current,e.clone_context_valid)::text,'UTF8'),'sha256'),'hex'))
  FROM effective e;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_stage_job_facts_v2(public.banking_pay_workbench_sessions) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_stage_job_facts_v2(public.banking_pay_workbench_sessions) FROM PUBLIC, anon, authenticated, service_role;

commit;
