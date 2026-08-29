-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.pay_workbench_dirty_event_enqueue(p_job_type text, p_scope_kind text, p_scope_id text, p_candidate_id uuid, p_targeted_timesheet_ids uuid[], p_linked_timesheet_ids uuid[], p_payload_json jsonb, p_reason text, p_priority integer, p_run_at_utc timestamp with time zone)
-- Installed pg_get_functiondef MD5: e13fd99e709b5461d558647e1f6263cc
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.pay_workbench_dirty_event_enqueue(p_job_type text, p_scope_kind text, p_scope_id text, p_candidate_id uuid DEFAULT NULL::uuid, p_targeted_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[], p_linked_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[], p_payload_json jsonb DEFAULT '{}'::jsonb, p_reason text DEFAULT NULL::text, p_priority integer DEFAULT '-1000'::integer, p_run_at_utc timestamp with time zone DEFAULT NULL::timestamp with time zone)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_job_type text := UPPER(BTRIM(COALESCE(p_job_type, '')));
  v_scope_kind text := UPPER(BTRIM(COALESCE(p_scope_kind, '')));
  v_scope_id text := NULLIF(BTRIM(COALESCE(p_scope_id, '')), '');
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_linked_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_targeted_key_part text := 'ALL';
  v_dedupe_key text;
  v_reason text := NULLIF(BTRIM(COALESCE(p_reason, '')), '');
  v_payload_json jsonb := '{}'::jsonb;
  v_incoming_payload jsonb := '{}'::jsonb;
  v_latest_source_change_seq bigint := 0;
  v_job_id uuid;
  v_job_status text;
  v_job_payload jsonb;
  v_priority integer := COALESCE(p_priority, -1000);
  v_run_at_utc timestamptz := COALESCE(p_run_at_utc, v_now);
  v_scope_uuid uuid := NULL::uuid;
  v_scope_invalidation_result jsonb := '{}'::jsonb;
  v_scope_change_tx_token uuid := NULL::uuid;
BEGIN
  IF v_job_type NOT IN (
    'WORKBENCH_CANDIDATE_DIRTY_APPLY',
    'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
    'CONTRACT_CLIENT_DIRTY_FANOUT'
  ) THEN
    RAISE EXCEPTION 'Unsupported Banking Pay dirty event job_type: %', p_job_type
      USING ERRCODE = '22023';
  END IF;

  IF v_scope_kind = '' OR v_scope_id IS NULL THEN
    RAISE EXCEPTION 'Banking Pay dirty event requires scope_kind and scope_id'
      USING ERRCODE = '22023';
  END IF;

  IF v_scope_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_scope_uuid := v_scope_id::uuid;
  END IF;

  IF v_job_type IN ('WORKBENCH_FINANCE_CASE_DIRTY_APPLY', 'CONTRACT_CLIENT_DIRTY_FANOUT')
     AND v_scope_uuid IS NULL THEN
    RAISE EXCEPTION 'Banking Pay dirty event scope_id must be a UUID for job_type %, scope_kind %, scope_id %', v_job_type, v_scope_kind, v_scope_id
      USING ERRCODE = '22023';
  END IF;

  SELECT COALESCE(array_agg(DISTINCT target_id ORDER BY target_id), ARRAY[]::uuid[])
  INTO v_targeted_timesheet_ids
  FROM unnest(COALESCE(p_targeted_timesheet_ids, ARRAY[]::uuid[])) AS target_id
  WHERE target_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT linked_id ORDER BY linked_id), ARRAY[]::uuid[])
  INTO v_linked_timesheet_ids
  FROM unnest(COALESCE(p_linked_timesheet_ids, ARRAY[]::uuid[])) AS linked_id
  WHERE linked_id IS NOT NULL;

  -- The central invalidator owns durable candidate/timesheet economic state.
  -- Its guarded callback into this function preserves the installed queue
  -- envelope without recursion or a second queue authority.
  IF v_job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
     AND p_candidate_id IS NOT NULL
     AND COALESCE(current_setting('cloudtms.bpay_scope_invalidator_active',true),'')<>'true' THEN
    IF COALESCE(p_payload_json->>'scope_change_tx_token','')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      v_scope_change_tx_token:=(p_payload_json->>'scope_change_tx_token')::uuid;
    END IF;
    v_scope_invalidation_result:=private.pay_workbench_scope_invalidate_v1(
      CASE WHEN cardinality(v_targeted_timesheet_ids)=0 THEN ARRAY[p_candidate_id]
        ELSE array_fill(p_candidate_id,ARRAY[cardinality(v_targeted_timesheet_ids)]) END,
      CASE WHEN cardinality(v_targeted_timesheet_ids)=0 THEN ARRAY[NULL::uuid]
        ELSE v_targeted_timesheet_ids END,
      COALESCE(v_reason,'DIRTY_TRIGGER:'||v_scope_kind),
      v_scope_change_tx_token,
      COALESCE(p_payload_json,'{}'::jsonb)||jsonb_build_object('skip_candidate_job_enqueue',true)
    );
  END IF;

  IF COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0 THEN
    v_targeted_key_part := array_to_string(v_targeted_timesheet_ids, ',');
  END IF;

  IF p_candidate_id IS NOT NULL THEN
    PERFORM public._change_bump('pay_candidate:' || p_candidate_id::text);

    SELECT COALESCE(change_counter.seq, 0)
    INTO v_latest_source_change_seq
    FROM public.app_change_counters AS change_counter
    WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;
  END IF;

  v_latest_source_change_seq := GREATEST(
    COALESCE(v_latest_source_change_seq, 0),
    COALESCE(CASE WHEN COALESCE(p_payload_json->>'latest_source_change_seq', '') ~ '^\d+$' THEN (p_payload_json->>'latest_source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(p_payload_json->>'source_change_seq', '') ~ '^\d+$' THEN (p_payload_json->>'source_change_seq')::bigint END, 0),
    COALESCE(CASE WHEN COALESCE(p_payload_json->>'source_change_sequence', '') ~ '^\d+$' THEN (p_payload_json->>'source_change_sequence')::bigint END, 0)
  );

  IF v_reason IS NULL THEN
    v_reason := 'DIRTY_TRIGGER:' || v_scope_kind;
  END IF;

  IF v_job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY' THEN
    IF p_candidate_id IS NULL THEN
      RAISE EXCEPTION 'Candidate dirty apply job requires p_candidate_id'
        USING ERRCODE = '22023';
    END IF;
    v_dedupe_key := 'DIRTY_TRIGGER:WORKBENCH_CANDIDATE_DIRTY_APPLY:CANDIDATE:'
      || p_candidate_id::text
      || ':TIMESHEETS:' || v_targeted_key_part;
  ELSIF v_job_type = 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY' THEN
    v_dedupe_key := 'DIRTY_TRIGGER:WORKBENCH_FINANCE_CASE_DIRTY_APPLY:' || v_scope_kind || ':' || v_scope_id;
  ELSE
    v_dedupe_key := 'DIRTY_TRIGGER:CONTRACT_CLIENT_DIRTY_FANOUT:' || v_scope_kind || ':' || v_scope_id;
  END IF;

  v_incoming_payload := jsonb_strip_nulls(
    COALESCE(p_payload_json, '{}'::jsonb)
    || jsonb_build_object(
      'job_type', v_job_type,
      'scope_kind', v_scope_kind,
      'scope_id', v_scope_id,
      'queue_class', 'DIRTY_TRIGGER_PRIORITY',
      'priority_class', 'DIRTY_TRIGGER_PRIORITY',
      'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
      'finance_case_id', CASE WHEN v_job_type = 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY' AND v_scope_uuid IS NOT NULL THEN v_scope_uuid::text ELSE NULL::text END,
      'contract_id', CASE WHEN v_job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT' AND v_scope_kind = 'CONTRACT' AND v_scope_uuid IS NOT NULL THEN v_scope_uuid::text ELSE NULL::text END,
      'client_id', CASE WHEN v_job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT' AND v_scope_kind = 'CLIENT' AND v_scope_uuid IS NOT NULL THEN v_scope_uuid::text ELSE NULL::text END,
      'umbrella_id', CASE WHEN v_job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT' AND v_scope_kind = 'UMBRELLA' AND v_scope_uuid IS NOT NULL THEN v_scope_uuid::text ELSE NULL::text END,
      'targeted_timesheet_ids', COALESCE(to_jsonb(v_targeted_timesheet_ids), '[]'::jsonb),
      'linked_timesheet_ids', COALESCE(to_jsonb(v_linked_timesheet_ids), '[]'::jsonb),
      'reason', v_reason,
      'reason_latest', v_reason,
      'reason_count', 1,
      'trigger_source', COALESCE(p_payload_json->>'trigger_source', p_payload_json->>'trigger_table', v_scope_kind),
      'latest_source_change_seq', v_latest_source_change_seq,
      'source_change_seq', v_latest_source_change_seq,
      'source_change_sequence', v_latest_source_change_seq,
      'latest_event_at_utc', v_now::text,
      'event_at_utc', v_now::text,
      'rerun_required', false,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
      'policy_x_dirtying_only', true,
      'economic_truth_mutation_allowed', false
    )
  );

  v_payload_json := public._pay_workbench_dirty_payload_merge('{}'::jsonb, v_incoming_payload);

  INSERT INTO public.banking_pay_workbench_jobs AS queued_job (
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
    created_at_utc,
    updated_at_utc,
    started_at_utc,
    completed_at_utc,
    failed_at_utc,
    last_error_json
  )
  VALUES (
    v_job_type,
    'QUEUED',
    v_priority,
    v_run_at_utc,
    0,
    8,
    v_dedupe_key,
    NULL,
    NULL,
    CASE WHEN v_job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY' THEN p_candidate_id ELSE NULL::uuid END,
    v_payload_json,
    v_now,
    v_now,
    NULL,
    NULL,
    NULL,
    NULL
  )
  ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
  DO UPDATE
  SET priority = LEAST(queued_job.priority, EXCLUDED.priority),
      run_at_utc = LEAST(queued_job.run_at_utc, EXCLUDED.run_at_utc),
      updated_at_utc = v_now,
      failed_at_utc = CASE WHEN queued_job.status = 'QUEUED' THEN NULL ELSE queued_job.failed_at_utc END,
      last_error_json = CASE WHEN queued_job.status = 'QUEUED' THEN NULL::jsonb ELSE queued_job.last_error_json END,
      payload_json = public._pay_workbench_dirty_payload_merge(
        COALESCE(queued_job.payload_json, '{}'::jsonb),
        COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
        || CASE
             WHEN queued_job.status = 'RUNNING' THEN jsonb_build_object('rerun_required', true)
             ELSE '{}'::jsonb
           END
      )
  RETURNING queued_job.id, queued_job.status, queued_job.payload_json
  INTO v_job_id, v_job_status, v_job_payload;

  PERFORM public._temp_diag_log(
    'TEMP_TRIGGER_DIRTY_STAGE',
    'TEMP_BANKING_PAY_DIRTY',
    v_scope_id,
    jsonb_build_object(
      'function_name', 'pay_workbench_dirty_event_enqueue',
      'stage', CASE WHEN v_job_status = 'RUNNING' THEN 'dirty_enqueue_coalesced_existing_running' ELSE 'dirty_enqueue_inserted_or_coalesced_queued' END,
      'job_id', v_job_id::text,
      'job_type', v_job_type,
      'dedupe_key', v_dedupe_key,
      'queue_class', 'DIRTY_TRIGGER_PRIORITY',
      'priority', v_priority,
      'scope_kind', v_scope_kind,
      'scope_id', v_scope_id,
      'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
      'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0),
      'latest_source_change_seq', v_latest_source_change_seq,
      'rerun_required', COALESCE((v_job_payload->>'rerun_required')::boolean, false)
    )
  );

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', v_job_id::text,
    'job_type', v_job_type,
    'status', v_job_status,
    'dedupe_key', v_dedupe_key,
    'queue_class', 'DIRTY_TRIGGER_PRIORITY',
    'priority', v_priority,
    'scope_kind', v_scope_kind,
    'scope_id', v_scope_id,
    'candidate_id', CASE WHEN p_candidate_id IS NULL THEN NULL ELSE p_candidate_id::text END,
    'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0),
    'latest_source_change_seq', v_latest_source_change_seq,
    'rerun_required', COALESCE((v_job_payload->>'rerun_required')::boolean, false)
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_dirty_event_enqueue(p_job_type text, p_scope_kind text, p_scope_id text, p_candidate_id uuid, p_targeted_timesheet_ids uuid[], p_linked_timesheet_ids uuid[], p_payload_json jsonb, p_reason text, p_priority integer, p_run_at_utc timestamp with time zone) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_dirty_event_enqueue(p_job_type text, p_scope_kind text, p_scope_id text, p_candidate_id uuid, p_targeted_timesheet_ids uuid[], p_linked_timesheet_ids uuid[], p_payload_json jsonb, p_reason text, p_priority integer, p_run_at_utc timestamp with time zone) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_dirty_event_enqueue(p_job_type text, p_scope_kind text, p_scope_id text, p_candidate_id uuid, p_targeted_timesheet_ids uuid[], p_linked_timesheet_ids uuid[], p_payload_json jsonb, p_reason text, p_priority integer, p_run_at_utc timestamp with time zone) TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_dirty_event_enqueue(p_job_type text, p_scope_kind text, p_scope_id text, p_candidate_id uuid, p_targeted_timesheet_ids uuid[], p_linked_timesheet_ids uuid[], p_payload_json jsonb, p_reason text, p_priority integer, p_run_at_utc timestamp with time zone) TO service_role;
