-- Banking Pay replacement-session candidate-owner correction.
-- Candidate work is always re-established through the canonical candidate
-- refresh owner.  Session-level scope work retains the established replay.
-- Payment economics, selection, Draft, provider and settlement authorities are
-- unchanged.

CREATE OR REPLACE FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(
  p_source_session_id uuid,
  p_target_session_id uuid,
  p_reason text DEFAULT 'REPLACED_SESSION_QUEUE_REPLAY'::text,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_reason text := COALESCE(NULLIF(BTRIM(p_reason), ''), 'REPLACED_SESSION_QUEUE_REPLAY');
  v_options_json jsonb := CASE
    WHEN jsonb_typeof(COALESCE(p_options_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_options_json, '{}'::jsonb)
    ELSE '{}'::jsonb
  END;
  v_source_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_target_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_candidate record;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_canonical_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_live_change_seq bigint := 0;
  v_source_active_job_count integer := 0;
  v_source_queued_job_count integer := 0;
  v_source_running_job_count integer := 0;
  v_source_candidate_job_count integer := 0;
  v_candidate_scope_missing_count integer := 0;
  v_candidate_refresh_enqueued_count integer := 0;
  v_candidate_refresh_deferred_count integer := 0;
  v_session_job_replayed_count integer := 0;
  v_replayed_job_count integer := 0;
  v_source_queued_terminalised_count integer := 0;
  v_replay_job_ids jsonb := '[]'::jsonb;
  v_source_job_ids jsonb := '[]'::jsonb;
  v_candidate_source_job_ids jsonb := '[]'::jsonb;
  v_terminalise_source_queued boolean := true;
  v_target_root_active boolean := false;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_source_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_SOURCE_SESSION_REQUIRED'
      USING ERRCODE='P0001',DETAIL=jsonb_build_object('code','PAY_WORKBENCH_QUEUE_REPLAY_SOURCE_SESSION_REQUIRED')::text;
  END IF;
  IF p_target_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SESSION_REQUIRED'
      USING ERRCODE='P0001',DETAIL=jsonb_build_object('code','PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SESSION_REQUIRED')::text;
  END IF;

  SELECT source_session.* INTO v_source_session
  FROM public.banking_pay_workbench_sessions AS source_session
  WHERE source_session.id=p_source_session_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_SOURCE_SESSION_NOT_FOUND'
      USING ERRCODE='P0001',DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_QUEUE_REPLAY_SOURCE_SESSION_NOT_FOUND',
        'source_session_id',p_source_session_id::text)::text;
  END IF;

  SELECT target_session.* INTO v_target_session
  FROM public.banking_pay_workbench_sessions AS target_session
  WHERE target_session.id=p_target_session_id
    AND UPPER(BTRIM(COALESCE(target_session.status,'')))='OPEN'
    AND target_session.discarded_at_utc IS NULL;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SESSION_NOT_OPEN'
      USING ERRCODE='P0001',DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SESSION_NOT_OPEN',
        'target_session_id',p_target_session_id::text)::text;
  END IF;

  IF p_source_session_id=p_target_session_id THEN
    RETURN jsonb_build_object(
      'ok',true,'source_session_id',p_source_session_id::text,
      'target_session_id',p_target_session_id::text,'skipped',true,
      'skip_reason','SOURCE_AND_TARGET_IDENTICAL','replayed_job_count',0,
      'source_queued_terminalised_count',0,'source_running_job_count',0,
      'candidate_refresh_enqueued_count',0,'candidate_refresh_deferred_count',0);
  END IF;

  v_terminalise_source_queued := LOWER(BTRIM(COALESCE(
    v_options_json->>'terminalise_source_queued','true'
  ))) NOT IN ('false','f','0','no','n','off');

  DROP TABLE IF EXISTS pg_temp._bpay_replaced_session_queue_replay_jobs;
  CREATE TEMP TABLE _bpay_replaced_session_queue_replay_jobs ON COMMIT DROP AS
  SELECT source_job.id AS source_job_id,source_job.job_type,
    UPPER(BTRIM(COALESCE(source_job.status,''))) AS source_status,
    source_job.priority,source_job.run_at_utc,source_job.max_attempts,
    source_job.snapshot_run_id,source_job.session_id,source_job.candidate_id,
    COALESCE(source_job.payload_json,'{}'::jsonb) AS payload_json,
    ('REPLAY_REPLACED_SESSION:' || p_target_session_id::text ||
      ':source_job:' || source_job.id::text) AS replay_dedupe_key
  FROM public.banking_pay_workbench_jobs AS source_job
  WHERE source_job.session_id=p_source_session_id
    AND UPPER(BTRIM(COALESCE(source_job.status,''))) IN ('QUEUED','RUNNING')
    AND UPPER(BTRIM(COALESCE(source_job.job_type,''))) IN (
      'WORKBENCH_SESSION_SCOPE_SEED','WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK','WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE',
      'WORKBENCH_CANDIDATE_DELTA_REFRESH','WORKBENCH_CANDIDATE_LINE_WORK_SEED',
      'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS','WORKBENCH_PREVIEW_ROWS_MATERIALISE',
      'CANDIDATE_SOURCE_BUILD','CANDIDATE_SOURCE_BUILD_CHUNK','SOURCE_BUILD','SOURCE_BUILD_PAGE'
    );

  SELECT COUNT(*)::integer,
    COUNT(*) FILTER(WHERE source_status='QUEUED')::integer,
    COUNT(*) FILTER(WHERE source_status='RUNNING')::integer,
    COUNT(*) FILTER(WHERE candidate_id IS NOT NULL)::integer,
    COALESCE(jsonb_agg(source_job_id::text ORDER BY source_job_id::text),'[]'::jsonb),
    COALESCE(jsonb_agg(source_job_id::text ORDER BY source_job_id::text)
      FILTER(WHERE candidate_id IS NOT NULL),'[]'::jsonb)
  INTO v_source_active_job_count,v_source_queued_job_count,
    v_source_running_job_count,v_source_candidate_job_count,
    v_source_job_ids,v_candidate_source_job_ids
  FROM pg_temp._bpay_replaced_session_queue_replay_jobs;

  IF COALESCE(v_source_active_job_count,0)=0 THEN
    RETURN jsonb_build_object(
      'ok',true,'source_session_id',p_source_session_id::text,
      'target_session_id',p_target_session_id::text,'reason',v_reason,
      'source_active_job_count',0,'source_queued_job_count',0,
      'source_running_job_count',0,'source_candidate_job_count',0,
      'replayed_job_count',0,'session_job_replayed_count',0,
      'candidate_refresh_enqueued_count',0,'candidate_refresh_deferred_count',0,
      'source_queued_terminalised_count',0,'replay_job_ids','[]'::jsonb,
      'source_job_ids','[]'::jsonb);
  END IF;

  SELECT COUNT(*)::integer INTO v_candidate_scope_missing_count
  FROM pg_temp._bpay_replaced_session_queue_replay_jobs AS replay_job
  WHERE replay_job.candidate_id IS NOT NULL
    AND NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_session_scope AS target_scope
      WHERE target_scope.session_id=p_target_session_id
        AND target_scope.candidate_id=replay_job.candidate_id);

  SELECT EXISTS(
    SELECT 1 FROM public.banking_pay_workbench_jobs AS target_root
    WHERE target_root.session_id=p_target_session_id
      AND target_root.candidate_id IS NULL
      AND UPPER(BTRIM(COALESCE(target_root.status,''))) IN ('QUEUED','RUNNING')
      AND UPPER(BTRIM(COALESCE(target_root.job_type,''))) IN (
        'WORKBENCH_SESSION_SCOPE_SEED','WORKBENCH_SESSION_CLONE_REBASE'
      )
  ) INTO v_target_root_active;

  FOR v_candidate IN
    SELECT replay_job.candidate_id,
      COALESCE(jsonb_agg(replay_job.source_job_id::text ORDER BY replay_job.source_job_id::text),'[]'::jsonb) AS source_job_ids,
      EXISTS(SELECT 1 FROM public.banking_pay_workbench_session_scope AS target_scope
        WHERE target_scope.session_id=p_target_session_id
          AND target_scope.candidate_id=replay_job.candidate_id) AS target_scope_exists
    FROM pg_temp._bpay_replaced_session_queue_replay_jobs AS replay_job
    WHERE replay_job.candidate_id IS NOT NULL
    GROUP BY replay_job.candidate_id
    ORDER BY replay_job.candidate_id
  LOOP
    IF v_candidate.target_scope_exists IS NOT TRUE THEN
      IF v_target_root_active IS NOT TRUE THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SCOPE_OWNER_MISSING'
          USING ERRCODE='P0001',DETAIL=jsonb_build_object(
            'code','PAY_WORKBENCH_QUEUE_REPLAY_TARGET_SCOPE_OWNER_MISSING',
            'target_session_id',p_target_session_id::text,
            'candidate_id',v_candidate.candidate_id::text,
            'source_job_ids',v_candidate.source_job_ids)::text;
      END IF;
      v_candidate_refresh_deferred_count:=v_candidate_refresh_deferred_count+1;
      CONTINUE;
    END IF;

    v_enqueue_result:=public.pay_workbench_enqueue_candidate_refresh(
      p_snapshot_run_id=>v_target_session.source_snapshot_run_id,
      p_candidate_id=>v_candidate.candidate_id,
      p_reason=>'REPLACED_SESSION_CANONICAL_CANDIDATE_REFRESH',
      p_actor_user_id=>v_target_session.actor_user_id,
      p_payload_json=>jsonb_build_object(
        'session_id',p_target_session_id::text,
        'source_session_id',p_target_session_id::text,
        'target_session_id',p_target_session_id::text,
        'session_version',v_target_session.version,
        'refresh_scope_kind','CANDIDATE_FULL_LIVE',
        'targeted_timesheet_ids','[]'::jsonb,
        'linked_timesheet_ids','[]'::jsonb,
        'force_legacy',true,'force_broad_legacy',true,
        'canonical_refresh_from_replaced_session',true,
        'canonical_refresh_source_session_id',p_source_session_id::text,
        'replayed_from_source_job_ids',v_candidate.source_job_ids,
        'replay_reason',v_reason,
        'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
      )
    );

    IF jsonb_typeof(v_enqueue_result) IS DISTINCT FROM 'object'
       OR LOWER(BTRIM(COALESCE(v_enqueue_result->>'ok',''))) <> 'true' THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_CANONICAL_REFRESH_FAILED'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_QUEUE_REPLAY_CANONICAL_REFRESH_FAILED',
          'target_session_id',p_target_session_id::text,
          'candidate_id',v_candidate.candidate_id::text)::text;
    END IF;

    IF LOWER(BTRIM(COALESCE(v_enqueue_result->>'source_build_required','false')))
       IN ('true','t','1','yes','y','on') THEN
      IF COALESCE(v_enqueue_result->>'job_id','') !~*
         '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_CANONICAL_JOB_REQUIRED'
          USING ERRCODE='P0001';
      END IF;
      v_canonical_job:=NULL;
      SELECT canonical_job.* INTO v_canonical_job
      FROM public.banking_pay_workbench_jobs AS canonical_job
      WHERE canonical_job.id=(v_enqueue_result->>'job_id')::uuid
        AND canonical_job.session_id=p_target_session_id
        AND canonical_job.candidate_id=v_candidate.candidate_id
        AND UPPER(BTRIM(COALESCE(canonical_job.status,''))) IN ('QUEUED','RUNNING')
        AND UPPER(BTRIM(COALESCE(canonical_job.job_type,'')))='WORKBENCH_CANDIDATE_SOURCE_BUILD';
      SELECT COALESCE(change_counter.seq,0) INTO v_live_change_seq
      FROM public.app_change_counters AS change_counter
      WHERE change_counter.entity_key='pay_candidate:' || v_candidate.candidate_id::text;
      v_live_change_seq:=COALESCE(v_live_change_seq,0);
      IF v_canonical_job.id IS NULL
         OR (CASE WHEN COALESCE(v_canonical_job.payload_json->>'session_version','') ~ '^[0-9]{1,18}$'
              THEN (v_canonical_job.payload_json->>'session_version')::bigint ELSE NULL::bigint END
              ) IS DISTINCT FROM v_target_session.version
         OR (CASE WHEN COALESCE(v_canonical_job.payload_json->>'source_change_seq','') ~ '^[0-9]{1,18}$'
              THEN (v_canonical_job.payload_json->>'source_change_seq')::bigint ELSE NULL::bigint END
              ) < v_live_change_seq
         OR COALESCE(v_canonical_job.payload_json->>'source_build_run_id','') !~*
              '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_QUEUE_REPLAY_CANONICAL_JOB_INVALID'
          USING ERRCODE='P0001';
      END IF;
    END IF;
    v_candidate_refresh_enqueued_count:=v_candidate_refresh_enqueued_count+1;
  END LOOP;

  WITH replayed_jobs AS (
    INSERT INTO public.banking_pay_workbench_jobs(
      job_type,status,priority,run_at_utc,attempt_count,max_attempts,dedupe_key,
      snapshot_run_id,session_id,candidate_id,payload_json,economic_build_id,
      private_stage,private_cursor_kind,private_cursor_json,private_stage_version,
      created_at_utc,updated_at_utc
    )
    SELECT 'WORKBENCH_SESSION_SCOPE_SEED','QUEUED',replay_job.priority,
      LEAST(COALESCE(replay_job.run_at_utc,v_now),v_now),0,
      GREATEST(COALESCE(replay_job.max_attempts,8),1),replay_job.replay_dedupe_key,
      v_target_session.source_snapshot_run_id,p_target_session_id,NULL::uuid,
      jsonb_strip_nulls((replay_job.payload_json - ARRAY[
        'worker_id','worker_claimed_at_utc','worker_lease_seconds',
        'worker_lease_expires_at_utc','worker_function','result_json','completion_json',
        'session_id','sessionId','source_session_id','sourceSessionId',
        'target_session_id','targetSessionId','snapshot_run_id','source_snapshot_run_id',
        'session_version','sessionVersion'
      ]::text[]) || jsonb_build_object(
        'session_id',p_target_session_id::text,
        'target_session_id',p_target_session_id::text,
        'snapshot_run_id',v_target_session.source_snapshot_run_id::text,
        'source_snapshot_run_id',v_target_session.source_snapshot_run_id::text,
        'session_version',v_target_session.version,
        'replayed_from_replaced_session',true,
        'replayed_from_session_id',p_source_session_id::text,
        'replayed_from_job_id',replay_job.source_job_id::text,
        'replayed_from_job_status',replay_job.source_status,
        'replay_reason',v_reason,'replayed_at_utc',v_now::text
      )),NULL::uuid,NULL::text,NULL::text,'{}'::jsonb,NULL::integer,v_now,v_now
    FROM pg_temp._bpay_replaced_session_queue_replay_jobs AS replay_job
    WHERE replay_job.candidate_id IS NULL
      AND UPPER(BTRIM(COALESCE(replay_job.job_type,'')))='WORKBENCH_SESSION_SCOPE_SEED'
    ON CONFLICT(dedupe_key) WHERE status IN ('QUEUED','RUNNING')
    DO UPDATE SET priority=LEAST(public.banking_pay_workbench_jobs.priority,EXCLUDED.priority),
      run_at_utc=LEAST(public.banking_pay_workbench_jobs.run_at_utc,EXCLUDED.run_at_utc),
      payload_json=COALESCE(public.banking_pay_workbench_jobs.payload_json,'{}'::jsonb)
        || EXCLUDED.payload_json || jsonb_build_object(
          'queue_replay_reused',true,'queue_replay_reused_at_utc',v_now::text),
      updated_at_utc=v_now
    RETURNING id
  )
  SELECT COUNT(*)::integer,
    COALESCE(jsonb_agg(replayed_jobs.id::text ORDER BY replayed_jobs.id::text),'[]'::jsonb)
  INTO v_session_job_replayed_count,v_replay_job_ids FROM replayed_jobs;

  v_replayed_job_count:=COALESCE(v_session_job_replayed_count,0)
    + COALESCE(v_candidate_refresh_enqueued_count,0);

  IF v_terminalise_source_queued THEN
    UPDATE public.banking_pay_workbench_jobs AS old_job
    SET status='DEAD',failed_at_utc=COALESCE(old_job.failed_at_utc,v_now),updated_at_utc=v_now,
      private_stage=CASE WHEN UPPER(BTRIM(COALESCE(old_job.job_type,'')))='WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND old_job.economic_build_id IS NULL AND old_job.private_stage='BUILD_INITIALISE'
        THEN NULL::text ELSE old_job.private_stage END,
      private_cursor_kind=CASE WHEN UPPER(BTRIM(COALESCE(old_job.job_type,'')))='WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND old_job.economic_build_id IS NULL AND old_job.private_stage='BUILD_INITIALISE'
        THEN NULL::text ELSE old_job.private_cursor_kind END,
      private_cursor_json=CASE WHEN UPPER(BTRIM(COALESCE(old_job.job_type,'')))='WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND old_job.economic_build_id IS NULL AND old_job.private_stage='BUILD_INITIALISE'
        THEN '{}'::jsonb ELSE old_job.private_cursor_json END,
      private_stage_version=CASE WHEN UPPER(BTRIM(COALESCE(old_job.job_type,'')))='WORKBENCH_CANDIDATE_SOURCE_BUILD'
        AND old_job.economic_build_id IS NULL AND old_job.private_stage='BUILD_INITIALISE'
        THEN NULL::integer ELSE old_job.private_stage_version END,
      last_error_json=jsonb_build_object(
        'code','REPLACED_SESSION_QUEUE_REPLAYED',
        'message','Queued work was replayed against the replacement Banking Pay workbench session.',
        'source_session_id',p_source_session_id::text,
        'replacement_session_id',p_target_session_id::text,
        'replay_reason',v_reason,'replayed_at_utc',v_now::text),
      payload_json=COALESCE(old_job.payload_json,'{}'::jsonb) || jsonb_build_object(
        'terminalised_as_replaced_session_queue_replayed',true,
        'replacement_session_id',p_target_session_id::text,
        'terminalised_at_utc',v_now::text)
    FROM pg_temp._bpay_replaced_session_queue_replay_jobs AS replay_job
    WHERE old_job.id=replay_job.source_job_id AND old_job.status='QUEUED';
    GET DIAGNOSTICS v_source_queued_terminalised_count=ROW_COUNT;
  END IF;

  PERFORM public._audit_insert(
    'banking_pay_workbench_session',p_source_session_id::text,
    'WORKBENCH_REPLACED_SESSION_QUEUE_REPLAYED',
    jsonb_build_object(
      'source_session_id',p_source_session_id::text,
      'target_session_id',p_target_session_id::text,
      'source_active_job_count',v_source_active_job_count,
      'source_queued_job_count',v_source_queued_job_count,
      'source_running_job_count',v_source_running_job_count,
      'source_candidate_job_count',v_source_candidate_job_count,
      'source_job_ids',v_source_job_ids),
    jsonb_build_object(
      'replayed_job_count',v_replayed_job_count,
      'session_job_replayed_count',v_session_job_replayed_count,
      'candidate_refresh_enqueued_count',v_candidate_refresh_enqueued_count,
      'candidate_refresh_deferred_count',v_candidate_refresh_deferred_count,
      'source_queued_terminalised_count',v_source_queued_terminalised_count,
      'candidate_scope_missing_count',v_candidate_scope_missing_count,
      'candidate_source_job_ids',v_candidate_source_job_ids,
      'replay_job_ids',v_replay_job_ids),v_reason,NULL::uuid);

  RETURN jsonb_build_object(
    'ok',true,'source_session_id',p_source_session_id::text,
    'target_session_id',p_target_session_id::text,'reason',v_reason,
    'source_active_job_count',v_source_active_job_count,
    'source_queued_job_count',v_source_queued_job_count,
    'source_running_job_count',v_source_running_job_count,
    'source_candidate_job_count',v_source_candidate_job_count,
    'replayed_job_count',v_replayed_job_count,
    'session_job_replayed_count',v_session_job_replayed_count,
    'candidate_refresh_enqueued_count',v_candidate_refresh_enqueued_count,
    'candidate_refresh_deferred_count',v_candidate_refresh_deferred_count,
    'source_queued_terminalised_count',v_source_queued_terminalised_count,
    'candidate_scope_missing_count',v_candidate_scope_missing_count,
    'source_job_ids',v_source_job_ids,'candidate_source_job_ids',v_candidate_source_job_ids,
    'replay_job_ids',v_replay_job_ids,
    'running_source_jobs_left_to_complete_stale',v_source_running_job_count);
END;
$function$;

ALTER FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(uuid,uuid,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(uuid,uuid,text,jsonb) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(uuid,uuid,text,jsonb) TO postgres,service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_repair_replayed_candidate_jobs_v1(
  p_session_id uuid DEFAULT NULL::uuid,
  p_candidate_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 10,
  p_reason text DEFAULT 'REPLACED_SESSION_CANDIDATE_OWNER_REPAIR'::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_now timestamptz:=clock_timestamp();
  v_limit integer:=LEAST(GREATEST(COALESCE(p_limit,10),1),25);
  v_reason text:=COALESCE(NULLIF(BTRIM(p_reason),''),'REPLACED_SESSION_CANDIDATE_OWNER_REPAIR');
  v_candidate record;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_owner public.banking_pay_workbench_jobs%ROWTYPE;
  v_repair_result jsonb:='{}'::jsonb;
  v_terminalised integer:=0;
  v_total_terminalised integer:=0;
  v_repaired integer:=0;
  v_skipped integer:=0;
  v_failed integer:=0;
  v_results jsonb:='[]'::jsonb;
  v_owner_valid boolean:=false;
  v_live_change_seq bigint:=0;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  FOR v_candidate IN
    SELECT DISTINCT replay_job.session_id,replay_job.candidate_id
    FROM public.banking_pay_workbench_jobs AS replay_job
    JOIN public.banking_pay_workbench_sessions AS target_session
      ON target_session.id=replay_job.session_id
    JOIN public.banking_pay_workbench_session_scope AS target_scope
      ON target_scope.session_id=replay_job.session_id
     AND target_scope.candidate_id=replay_job.candidate_id
    WHERE replay_job.candidate_id IS NOT NULL
      AND replay_job.status IN ('QUEUED','RUNNING')
      AND LOWER(BTRIM(COALESCE(replay_job.payload_json->>'replayed_from_replaced_session','false')))
          IN ('true','t','1','yes','y','on')
      AND COALESCE(replay_job.payload_json->>'replayed_from_session_id','') ~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND replay_job.dedupe_key LIKE
          ('REPLAY_REPLACED_SESSION:' || replay_job.session_id::text || ':source_job:%')
      AND UPPER(BTRIM(COALESCE(target_session.status,'')))='OPEN'
      AND target_session.discarded_at_utc IS NULL
      AND (p_session_id IS NULL OR replay_job.session_id=p_session_id)
      AND (p_candidate_id IS NULL OR replay_job.candidate_id=p_candidate_id)
    ORDER BY replay_job.session_id,replay_job.candidate_id
    LIMIT v_limit
  LOOP
    BEGIN
      IF NOT pg_catalog.pg_try_advisory_xact_lock(pg_catalog.hashtextextended(
        public._pay_workbench_candidate_serial_key(v_candidate.candidate_id),24062027
      )) THEN
        v_skipped:=v_skipped+1;
        v_results:=v_results || jsonb_build_array(jsonb_build_object(
          'session_id',v_candidate.session_id::text,
          'candidate_id',v_candidate.candidate_id::text,
          'action','SKIPPED_CANDIDATE_SERIAL_BUSY'));
        CONTINUE;
      END IF;

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
      FOR UPDATE;

      IF EXISTS(
        SELECT 1 FROM public.banking_pay_workbench_jobs AS running_replay
        WHERE running_replay.session_id=v_candidate.session_id
          AND running_replay.candidate_id=v_candidate.candidate_id
          AND running_replay.status='RUNNING'
          AND LOWER(BTRIM(COALESCE(running_replay.payload_json->>'replayed_from_replaced_session','false')))
              IN ('true','t','1','yes','y','on')
          AND running_replay.dedupe_key LIKE
              ('REPLAY_REPLACED_SESSION:' || v_candidate.session_id::text || ':source_job:%')
      ) THEN
        v_skipped:=v_skipped+1;
        v_results:=v_results || jsonb_build_array(jsonb_build_object(
          'session_id',v_candidate.session_id::text,
          'candidate_id',v_candidate.candidate_id::text,
          'action','SKIPPED_RUNNING_REPLAY_OWNER'));
        CONTINUE;
      END IF;

      UPDATE public.banking_pay_workbench_jobs AS replay_job
      SET status='DEAD',failed_at_utc=COALESCE(replay_job.failed_at_utc,v_now),
        updated_at_utc=v_now,
        private_stage=CASE WHEN replay_job.economic_build_id IS NULL
          AND replay_job.private_stage='BUILD_INITIALISE' THEN NULL::text ELSE replay_job.private_stage END,
        private_cursor_kind=CASE WHEN replay_job.economic_build_id IS NULL
          AND replay_job.private_stage='BUILD_INITIALISE' THEN NULL::text ELSE replay_job.private_cursor_kind END,
        private_cursor_json=CASE WHEN replay_job.economic_build_id IS NULL
          AND replay_job.private_stage='BUILD_INITIALISE' THEN '{}'::jsonb ELSE replay_job.private_cursor_json END,
        private_stage_version=CASE WHEN replay_job.economic_build_id IS NULL
          AND replay_job.private_stage='BUILD_INITIALISE' THEN NULL::integer ELSE replay_job.private_stage_version END,
        last_error_json=jsonb_build_object(
          'code','REPLACED_SESSION_CANDIDATE_JOB_REQUIRES_CANONICAL_OWNER',
          'message','Candidate refresh was replaced by the current canonical workbench owner.',
          'repair_reason',v_reason,'repaired_at_utc',v_now::text),
        payload_json=COALESCE(replay_job.payload_json,'{}'::jsonb) || jsonb_build_object(
          'replayed_candidate_owner_terminalised',true,
          'replayed_candidate_owner_terminalised_at_utc',v_now::text,
          'replayed_candidate_owner_repair_reason',v_reason)
      WHERE replay_job.session_id=v_candidate.session_id
        AND replay_job.candidate_id=v_candidate.candidate_id
        AND replay_job.status='QUEUED'
        AND LOWER(BTRIM(COALESCE(replay_job.payload_json->>'replayed_from_replaced_session','false')))
            IN ('true','t','1','yes','y','on')
        AND COALESCE(replay_job.payload_json->>'replayed_from_session_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND replay_job.dedupe_key LIKE
            ('REPLAY_REPLACED_SESSION:' || v_candidate.session_id::text || ':source_job:%');
      GET DIAGNOSTICS v_terminalised=ROW_COUNT;

      IF v_terminalised=0 THEN
        v_skipped:=v_skipped+1;
        CONTINUE;
      END IF;

      IF v_scope.pending_job_id IS NOT NULL AND EXISTS(
        SELECT 1 FROM public.banking_pay_workbench_jobs AS old_owner
        WHERE old_owner.id=v_scope.pending_job_id
          AND old_owner.status='DEAD'
          AND LOWER(BTRIM(COALESCE(old_owner.payload_json->>'replayed_candidate_owner_terminalised','false')))
              IN ('true','t','1','yes','y','on')
      ) THEN
        v_repair_result:=public.pay_workbench_repair_orphaned_pending_source_build(
          v_candidate.session_id,v_candidate.candidate_id,1,v_now,
          'REPLACED_SESSION_CANDIDATE_OWNER_REPAIR'
        );
      ELSE
        v_repair_result:=jsonb_build_object('ok',true,'repair_not_required',true);
      END IF;

      SELECT scope_row.* INTO STRICT v_scope
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE scope_row.session_id=v_candidate.session_id
        AND scope_row.candidate_id=v_candidate.candidate_id
      FOR UPDATE;

      v_owner_valid:=UPPER(BTRIM(COALESCE(v_scope.status,''))) IN (
        'SOURCE_READY','LINE_WORK_PENDING','MATERIALISED','MATERIALIZED','READY','SOURCE_EMPTY'
      ) AND v_scope.pending_job_id IS NULL;
      IF NOT v_owner_valid AND v_scope.pending_job_id IS NOT NULL THEN
        v_owner:=NULL;
        SELECT owner_job.* INTO v_owner
        FROM public.banking_pay_workbench_jobs AS owner_job
        WHERE owner_job.id=v_scope.pending_job_id
          AND owner_job.session_id=v_candidate.session_id
          AND owner_job.candidate_id=v_candidate.candidate_id
          AND owner_job.status IN ('QUEUED','RUNNING')
          AND UPPER(BTRIM(COALESCE(owner_job.job_type,''))) IN (
            'WORKBENCH_CANDIDATE_SOURCE_BUILD','WORKBENCH_CANDIDATE_SOURCE_BUILD_CHUNK',
            'WORKBENCH_CANDIDATE_SOURCE_BUILD_PAGE','CANDIDATE_SOURCE_BUILD',
            'CANDIDATE_SOURCE_BUILD_CHUNK','SOURCE_BUILD','SOURCE_BUILD_PAGE'
          );
        SELECT COALESCE(change_counter.seq,0) INTO v_live_change_seq
        FROM public.app_change_counters AS change_counter
        WHERE change_counter.entity_key='pay_candidate:' || v_candidate.candidate_id::text;
        v_live_change_seq:=COALESCE(v_live_change_seq,0);
        v_owner_valid:=v_owner.id IS NOT NULL
          AND (CASE WHEN COALESCE(v_owner.payload_json->>'session_version','') ~ '^[0-9]{1,18}$'
            THEN (v_owner.payload_json->>'session_version')::bigint ELSE NULL::bigint END)=v_session.version
          AND (CASE WHEN COALESCE(v_owner.payload_json->>'source_change_seq','') ~ '^[0-9]{1,18}$'
            THEN (v_owner.payload_json->>'source_change_seq')::bigint ELSE NULL::bigint END)>=v_live_change_seq
          AND COALESCE(v_owner.payload_json->>'source_build_run_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND LOWER(BTRIM(COALESCE(v_owner.payload_json->>'replayed_from_replaced_session','false')))
            NOT IN ('true','t','1','yes','y','on');
      END IF;

      IF v_owner_valid IS NOT TRUE OR EXISTS(
        SELECT 1 FROM public.banking_pay_workbench_jobs AS remaining_replay
        WHERE remaining_replay.session_id=v_candidate.session_id
          AND remaining_replay.candidate_id=v_candidate.candidate_id
          AND remaining_replay.status IN ('QUEUED','RUNNING')
          AND LOWER(BTRIM(COALESCE(remaining_replay.payload_json->>'replayed_from_replaced_session','false')))
              IN ('true','t','1','yes','y','on')
          AND remaining_replay.dedupe_key LIKE
              ('REPLAY_REPLACED_SESSION:' || v_candidate.session_id::text || ':source_job:%')
      ) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_REPLAYED_CANDIDATE_OWNER_REPAIR_POSTCONDITION_FAILED'
          USING ERRCODE='P0001';
      END IF;

      v_total_terminalised:=v_total_terminalised+v_terminalised;
      v_repaired:=v_repaired+1;
      v_results:=v_results || jsonb_build_array(jsonb_build_object(
        'session_id',v_candidate.session_id::text,
        'candidate_id',v_candidate.candidate_id::text,
        'action','REPAIRED_CANONICAL_OWNER',
        'terminalised_replay_job_count',v_terminalised,
        'canonical_pending_job_id',CASE WHEN v_scope.pending_job_id IS NULL THEN NULL ELSE v_scope.pending_job_id::text END,
        'repair_result',v_repair_result));
    EXCEPTION WHEN OTHERS THEN
      v_failed:=v_failed+1;
      v_results:=v_results || jsonb_build_array(jsonb_build_object(
        'session_id',v_candidate.session_id::text,
        'candidate_id',v_candidate.candidate_id::text,
        'action','REPAIR_FAILED_NO_PARTIAL_ADOPTION',
        'code',SQLSTATE,'message',SQLERRM));
    END;
  END LOOP;

  RETURN jsonb_build_object(
    'ok',v_failed=0,'partial',v_failed>0 AND v_repaired>0,
    'examined_count',v_repaired+v_skipped+v_failed,
    'repaired_candidate_count',v_repaired,
    'terminalised_replay_job_count',v_total_terminalised,
    'skipped_count',v_skipped,'failed_count',v_failed,
    'automatic_recovery_scheduled',v_repaired>0,
    'results',v_results,
    'policy_x_authority_scope','PRE_DRAFT_WORKBENCH_REPAIR_ONLY');
END;
$function$;

ALTER FUNCTION public.pay_workbench_repair_replayed_candidate_jobs_v1(uuid,uuid,integer,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_repair_replayed_candidate_jobs_v1(uuid,uuid,integer,text) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_repair_replayed_candidate_jobs_v1(uuid,uuid,integer,text) TO postgres,service_role;
