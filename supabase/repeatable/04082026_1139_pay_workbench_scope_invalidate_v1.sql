-- Banking Pay bounded-scope V1.2.4: central set-based invalidator.
-- Policy X: dirtying/freshness metadata only; no financial calculation or DML.

CREATE OR REPLACE FUNCTION private.pay_workbench_scope_invalidate_v1(
  p_candidate_ids uuid[],
  p_timesheet_ids uuid[],
  p_reason text,
  p_scope_change_tx_token uuid DEFAULT NULL::uuid,
  p_payload_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_reason text := NULLIF(btrim(COALESCE(p_reason,'')), '');
  v_payload jsonb := COALESCE(p_payload_json,'{}'::jsonb);
  v_tx_token uuid;
  v_pair_count integer := 0;
  v_candidate_count integer := 0;
  v_timesheet_count integer := 0;
  v_registry_existing integer := 0;
  v_state_existing integer := 0;
  v_registry_affected integer := 0;
  v_state_affected integer := 0;
  v_job_inserted integer := 0;
  v_job_coalesced integer := 0;
  v_job_result jsonb;
  v_candidate record;
  v_source_change_seq bigint := 0;
BEGIN
  IF jsonb_typeof(v_payload) <> 'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_INVALIDATION_INPUT_INVALID'
      USING ERRCODE='22023', DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_SCOPE_INVALIDATION_INPUT_INVALID',
        'field','p_payload_json'
      )::text;
  END IF;

  IF v_reason IS NULL
     OR cardinality(COALESCE(p_candidate_ids,ARRAY[]::uuid[]))
        <> cardinality(COALESCE(p_timesheet_ids,ARRAY[]::uuid[])) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_INVALIDATION_INPUT_INVALID'
      USING ERRCODE='22023', DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_SCOPE_INVALIDATION_INPUT_INVALID',
        'candidate_count',cardinality(COALESCE(p_candidate_ids,ARRAY[]::uuid[])),
        'timesheet_count',cardinality(COALESCE(p_timesheet_ids,ARRAY[]::uuid[]))
      )::text;
  END IF;

  CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_invalidation_pairs_v1(
    candidate_id uuid NOT NULL,
    timesheet_id uuid NULL,
    UNIQUE NULLS NOT DISTINCT(candidate_id,timesheet_id)
  ) ON COMMIT DROP;
  TRUNCATE pg_temp._bpay_wb_invalidation_pairs_v1;

  INSERT INTO pg_temp._bpay_wb_invalidation_pairs_v1(candidate_id,timesheet_id)
  SELECT candidate_id,timesheet_id
  FROM unnest(
    COALESCE(p_candidate_ids,ARRAY[]::uuid[]),
    COALESCE(p_timesheet_ids,ARRAY[]::uuid[])
  ) AS input_pair(candidate_id,timesheet_id)
  WHERE candidate_id IS NOT NULL
  ON CONFLICT DO NOTHING;

  GET DIAGNOSTICS v_pair_count = ROW_COUNT;
  IF v_pair_count = 0 THEN
    RETURN jsonb_build_object(
      'ok',true,'candidate_count',0,'timesheet_count',0,
      'registry_inserted_count',0,'registry_updated_count',0,
      'state_inserted_count',0,'state_updated_count',0,
      'job_inserted_count',0,'job_coalesced_count',0,
      'scope_change_tx_token',NULL,'reason',v_reason
    );
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._bpay_wb_invalidation_pairs_v1 AS invalidation_pair
    LEFT JOIN public.candidates AS candidate_row
      ON candidate_row.id=invalidation_pair.candidate_id
    WHERE candidate_row.id IS NULL
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_INVALIDATION_OWNERSHIP_MISMATCH'
      USING ERRCODE='23503', DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_SCOPE_INVALIDATION_OWNERSHIP_MISMATCH',
        'kind','CANDIDATE_NOT_FOUND'
      )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp._bpay_wb_invalidation_pairs_v1 AS invalidation_pair
    LEFT JOIN public.timesheets AS timesheet_row
      ON timesheet_row.timesheet_id=invalidation_pair.timesheet_id
    WHERE invalidation_pair.timesheet_id IS NOT NULL
      AND (
        timesheet_row.timesheet_id IS NULL
        OR EXISTS (
          SELECT 1
          FROM public.timesheets_financials AS ownership_financial
          WHERE ownership_financial.timesheet_id=invalidation_pair.timesheet_id
            AND ownership_financial.is_current
            AND ownership_financial.candidate_id IS DISTINCT FROM invalidation_pair.candidate_id
        )
      )
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_INVALIDATION_OWNERSHIP_MISMATCH'
      USING ERRCODE='23503', DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_SCOPE_INVALIDATION_OWNERSHIP_MISMATCH',
        'kind','TIMESHEET_CANDIDATE_MISMATCH'
      )::text;
  END IF;

  v_tx_token := COALESCE(p_scope_change_tx_token,public.pay_workbench_scope_change_tx_token_v1());
  IF NOT EXISTS (
    SELECT 1 FROM public.banking_pay_scope_change_transactions AS scope_tx
    WHERE scope_tx.tx_token=v_tx_token AND scope_tx.state='PENDING'
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_TRANSACTION_TOKEN_INVALID'
      USING ERRCODE='22023', DETAIL=jsonb_build_object(
        'code','PAY_WORKBENCH_SCOPE_TRANSACTION_TOKEN_INVALID',
        'scope_change_tx_token',v_tx_token
      )::text;
  END IF;

  SELECT count(DISTINCT candidate_id)::integer,
         count(DISTINCT timesheet_id) FILTER (WHERE timesheet_id IS NOT NULL)::integer
  INTO v_candidate_count,v_timesheet_count
  FROM pg_temp._bpay_wb_invalidation_pairs_v1;

  SELECT count(*)::integer INTO v_registry_existing
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry
  WHERE registry.candidate_id IN (
    SELECT DISTINCT candidate_id FROM pg_temp._bpay_wb_invalidation_pairs_v1
  );

  INSERT INTO private.banking_pay_workbench_candidate_scope_registry AS registry(
    candidate_id,last_dirty_reason,last_scope_change_tx_token,
    last_dirtied_at_utc,updated_at_utc
  )
  SELECT DISTINCT candidate_id,v_reason,v_tx_token,clock_timestamp(),clock_timestamp()
  FROM pg_temp._bpay_wb_invalidation_pairs_v1
  ORDER BY candidate_id
  ON CONFLICT(candidate_id) DO UPDATE
  SET last_dirty_reason=EXCLUDED.last_dirty_reason,
      last_scope_change_tx_token=EXCLUDED.last_scope_change_tx_token,
      last_dirtied_at_utc=EXCLUDED.last_dirtied_at_utc,
      updated_at_utc=EXCLUDED.updated_at_utc;
  GET DIAGNOSTICS v_registry_affected = ROW_COUNT;

  SELECT count(*)::integer INTO v_state_existing
  FROM private.banking_pay_workbench_timesheet_scope_state AS scope_state
  WHERE scope_state.timesheet_id IN (
    SELECT timesheet_id FROM pg_temp._bpay_wb_invalidation_pairs_v1
    WHERE timesheet_id IS NOT NULL
  );

  INSERT INTO private.banking_pay_workbench_timesheet_scope_state AS scope_state(
    timesheet_id,candidate_id,economic_state,dirty_generation,
    last_dirty_reason,last_scope_change_tx_token,registered_at_utc,
    last_dirtied_at_utc,updated_at_utc
  )
  SELECT timesheet_id,candidate_id,'DIRTY',0,v_reason,v_tx_token,
         clock_timestamp(),clock_timestamp(),clock_timestamp()
  FROM pg_temp._bpay_wb_invalidation_pairs_v1
  WHERE timesheet_id IS NOT NULL
  ORDER BY candidate_id,timesheet_id
  ON CONFLICT(timesheet_id) DO UPDATE
  SET candidate_id=EXCLUDED.candidate_id,
      economic_state='DIRTY',
      last_dirty_reason=EXCLUDED.last_dirty_reason,
      last_scope_change_tx_token=EXCLUDED.last_scope_change_tx_token,
      last_dirtied_at_utc=EXCLUDED.last_dirtied_at_utc,
      closed_at_utc=NULL,
      updated_at_utc=EXCLUDED.updated_at_utc;
  GET DIAGNOSTICS v_state_affected = ROW_COUNT;

  IF COALESCE((v_payload->>'skip_candidate_job_enqueue')::boolean,false) IS NOT TRUE THEN
    PERFORM set_config('cloudtms.bpay_scope_invalidator_active','true',true);
    FOR v_candidate IN
      SELECT candidate_id,
             COALESCE(array_agg(timesheet_id ORDER BY timesheet_id)
               FILTER (WHERE timesheet_id IS NOT NULL),ARRAY[]::uuid[]) AS timesheet_ids
      FROM pg_temp._bpay_wb_invalidation_pairs_v1
      GROUP BY candidate_id
      ORDER BY candidate_id
    LOOP
      v_job_result := public.pay_workbench_dirty_event_enqueue(
        p_job_type=>'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        p_scope_kind=>'CANDIDATE',
        p_scope_id=>v_candidate.candidate_id::text,
        p_candidate_id=>v_candidate.candidate_id,
        p_targeted_timesheet_ids=>v_candidate.timesheet_ids,
        p_linked_timesheet_ids=>ARRAY[]::uuid[],
        p_payload_json=>v_payload || jsonb_build_object(
          'scope_change_tx_token',v_tx_token,
          'bounded_scope_state_precedes_job',true
        ),
        p_reason=>v_reason,
        p_priority=>COALESCE((v_payload->>'priority')::integer,-1000),
        p_run_at_utc=>clock_timestamp()
      );
      IF COALESCE(v_job_result->>'status','')='QUEUED' THEN
        v_job_inserted := v_job_inserted+1;
      ELSE
        v_job_coalesced := v_job_coalesced+1;
      END IF;
      IF COALESCE(v_job_result->>'latest_source_change_seq','') ~ '^\d+$' THEN
        v_source_change_seq := (v_job_result->>'latest_source_change_seq')::bigint;
        UPDATE private.banking_pay_workbench_candidate_scope_registry
        SET current_source_change_seq=GREATEST(current_source_change_seq,v_source_change_seq),
            updated_at_utc=clock_timestamp()
        WHERE candidate_id=v_candidate.candidate_id;
      END IF;
    END LOOP;
  END IF;

  RETURN jsonb_build_object(
    'ok',true,
    'candidate_count',v_candidate_count,
    'timesheet_count',v_timesheet_count,
    'registry_inserted_count',GREATEST(v_registry_affected-v_registry_existing,0),
    'registry_updated_count',LEAST(v_registry_existing,v_registry_affected),
    'state_inserted_count',GREATEST(v_state_affected-v_state_existing,0),
    'state_updated_count',LEAST(v_state_existing,v_state_affected),
    'job_inserted_count',v_job_inserted,
    'job_coalesced_count',v_job_coalesced,
    'scope_change_tx_token',v_tx_token,
    'reason',v_reason
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_scope_invalidate_v1(uuid[],uuid[],text,uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_scope_invalidate_v1(uuid[],uuid[],text,uuid,jsonb) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_scope_invalidate_v1(uuid[],uuid[],text,uuid,jsonb) TO postgres;
