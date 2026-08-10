-- Banking Pay physical source-publication identity and bounded Draft step RPC.
--
-- Policy X: the helper below identifies an already-authoritative pre-Draft
-- source cohort. It does not calculate or alter payment economics.

CREATE OR REPLACE FUNCTION private.pay_workbench_source_publication_identity_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_session_version bigint,
  p_source_change_seq bigint,
  p_source_build_run_id uuid
)
RETURNS uuid
LANGUAGE plpgsql
IMMUTABLE
STRICT
PARALLEL SAFE
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_hash text;
BEGIN
  IF p_session_version <= 0 OR p_source_change_seq < 0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SOURCE_PUBLICATION_AUTHORITY_INVALID'
      USING ERRCODE='22023', DETAIL=pg_catalog.jsonb_build_object(
        'code','PAY_WORKBENCH_SOURCE_PUBLICATION_AUTHORITY_INVALID',
        'reason','VERSION_OR_SEQUENCE_INVALID'
      )::text;
  END IF;

  v_hash := pg_catalog.md5(
    'BANKING_PAY_SOURCE_PUBLICATION_V1|' ||
    p_session_id::text || '|' || p_candidate_id::text || '|' ||
    p_session_version::text || '|' || p_source_change_seq::text || '|' ||
    p_source_build_run_id::text
  );

  RETURN (
    pg_catalog.substr(v_hash,1,8) || '-' ||
    pg_catalog.substr(v_hash,9,4) || '-' ||
    pg_catalog.substr(v_hash,13,4) || '-' ||
    pg_catalog.substr(v_hash,17,4) || '-' ||
    pg_catalog.substr(v_hash,21,12)
  )::uuid;
END;
$function$;

ALTER FUNCTION private.pay_workbench_source_publication_identity_v1(
  uuid,uuid,bigint,bigint,uuid
) OWNER TO postgres;

REVOKE ALL ON FUNCTION private.pay_workbench_source_publication_identity_v1(
  uuid,uuid,bigint,bigint,uuid
) FROM PUBLIC,anon,authenticated,service_role;

GRANT EXECUTE ON FUNCTION private.pay_workbench_source_publication_identity_v1(
  uuid,uuid,bigint,bigint,uuid
) TO postgres;

COMMENT ON FUNCTION private.pay_workbench_source_publication_identity_v1(
  uuid,uuid,bigint,bigint,uuid
) IS 'Deterministic immutable identity for one physical Banking Pay candidate source cohort.';

-- Executes one row-backed Draft-create phase unit in one database transaction.
-- Existing economic owners remain the only formula authorities; this function
-- only removes HTTP round trips around claim, business work, finish and save.
CREATE OR REPLACE FUNCTION public.banking_pay_draft_create_step_v1(
  p_operation_id uuid,
  p_worker_id text,
  p_expected_phase text,
  p_request_budget_ms integer DEFAULT 25000
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path TO 'pg_catalog','public','private','pg_temp'
AS $function$
DECLARE
  v_started_at timestamptz:=pg_catalog.clock_timestamp();
  v_business_started_at timestamptz;
  v_now timestamptz:=pg_catalog.clock_timestamp();
  v_operation public.banking_pay_operations%ROWTYPE;
  v_chunk record;
  v_group record;
  v_phase text:=pg_catalog.upper(pg_catalog.btrim(pg_catalog.coalesce(p_expected_phase,'')));
  v_next_phase text;
  v_worker_id text:=pg_catalog.nullif(pg_catalog.btrim(pg_catalog.coalesce(p_worker_id,'')),'');
  v_scope_ids jsonb:='[]'::jsonb;
  v_result jsonb:='{}'::jsonb;
  v_results jsonb:='[]'::jsonb;
  v_saved jsonb:='{}'::jsonb;
  v_finished jsonb:='{}'::jsonb;
  v_business_ms integer:=0;
  v_scope_count integer:=0;
  v_pay_date date;
  v_week_start date;
  v_enabled boolean:=false;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  SELECT pg_catalog.coalesce(setting.banking_pay_draft_step_rpc_v1_enabled,false)
  INTO v_enabled
  FROM public.settings_defaults AS setting
  WHERE setting.id=1;
  IF v_enabled IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',false,'code','DRAFT_CREATE_STEP_RPC_DISABLED'
    );
  END IF;

  IF p_operation_id IS NULL OR v_worker_id IS NULL
     OR v_phase NOT IN (
       'SEED_ALLOCATION_ROWS','INSERT_CANDIDATES','INSERT_ITEMS',
       'APPLY_FINANCE_ADJUSTMENTS','FINALISE_RESERVATIONS',
       'POPULATE_CANDIDATE_SUMMARIES','CREATE_TIMESHEET_SNAPSHOTS',
       'BUILD_ITEM_BREAKDOWNS'
     ) OR pg_catalog.coalesce(p_request_budget_ms,25000) NOT BETWEEN 1000 AND 25000 THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_CREATE_STEP_INPUT_INVALID'
      USING ERRCODE='22023',DETAIL=pg_catalog.jsonb_build_object(
        'code','BANKING_PAY_DRAFT_CREATE_STEP_INPUT_INVALID',
        'operation_id',p_operation_id,
        'expected_phase',v_phase
      )::text;
  END IF;

  SELECT operation_row.* INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id=p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object('ok',false,'handled',true,'code','DRAFT_CREATE_OPERATION_NOT_FOUND');
  END IF;
  IF pg_catalog.upper(pg_catalog.btrim(pg_catalog.coalesce(v_operation.operation_type,'')))<>'DRAFT_CREATE'
     OR pg_catalog.upper(pg_catalog.btrim(pg_catalog.coalesce(v_operation.status,''))) NOT IN ('RUNNING','CONTINUING','WAITING_RETRY') THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',false,'code','DRAFT_CREATE_OPERATION_NOT_RUNNABLE',
      'operation',pg_catalog.to_jsonb(v_operation)
    );
  END IF;
  IF pg_catalog.upper(pg_catalog.btrim(pg_catalog.coalesce(v_operation.phase,'')))<>v_phase THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',false,'code','DRAFT_CREATE_PHASE_MOVED',
      'operation',pg_catalog.to_jsonb(v_operation)
    );
  END IF;
  IF pg_catalog.coalesce(v_operation.lease_owner,v_operation.locked_by) IS NOT NULL
     AND pg_catalog.coalesce(v_operation.lease_owner,v_operation.locked_by)<>v_worker_id THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',false,'code','DRAFT_CREATE_LEASE_OWNER_MISMATCH',
      'operation',pg_catalog.to_jsonb(v_operation)
    );
  END IF;
  IF pg_catalog.coalesce(v_operation.lease_expires_at_utc,v_operation.lock_expires_at_utc) IS NOT NULL
     AND pg_catalog.coalesce(v_operation.lease_expires_at_utc,v_operation.lock_expires_at_utc)<=v_now THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',false,'code','DRAFT_CREATE_LEASE_EXPIRED',
      'operation',pg_catalog.to_jsonb(v_operation)
    );
  END IF;

  v_next_phase:=CASE v_phase
    WHEN 'SEED_ALLOCATION_ROWS' THEN 'CREATE_BATCH_SHELLS'
    WHEN 'INSERT_CANDIDATES' THEN 'INSERT_ITEMS'
    WHEN 'INSERT_ITEMS' THEN 'APPLY_FINANCE_ADJUSTMENTS'
    WHEN 'APPLY_FINANCE_ADJUSTMENTS' THEN 'FINALISE_RESERVATIONS'
    WHEN 'FINALISE_RESERVATIONS' THEN 'POPULATE_CANDIDATE_SUMMARIES'
    WHEN 'POPULATE_CANDIDATE_SUMMARIES' THEN 'CREATE_TIMESHEET_SNAPSHOTS'
    WHEN 'CREATE_TIMESHEET_SNAPSHOTS' THEN 'BUILD_ITEM_BREAKDOWNS'
    WHEN 'BUILD_ITEM_BREAKDOWNS' THEN 'ASSERT_INTEGRITY'
  END;

  SELECT claimed_chunk.* INTO v_chunk
  FROM public.banking_pay_operation_claim_chunk(
    p_operation_id,v_phase,'CANDIDATE_SCOPE',v_worker_id,
    LEAST(GREATEST(COALESCE(v_operation.config_json->>'lock_seconds','60')::integer,5),3600)
  ) AS claimed_chunk
  LIMIT 1;

  IF NOT FOUND OR v_chunk.chunk_id IS NULL THEN
    SELECT pg_catalog.to_jsonb(saved_row) INTO v_saved
    FROM public.banking_pay_operation_save_progress(
      p_operation_id,'RUNNING',v_next_phase,NULL,NULL,NULL,NULL,NULL,
      pg_catalog.jsonb_build_object(
        'status_text',v_phase||' chunks complete.',
        'draft_step_rpc',true,
        'draft_step_phase_complete',true,
        'draft_step_round_trip_count',1,
        'draft_step_elapsed_ms',pg_catalog.floor(extract(epoch FROM (pg_catalog.clock_timestamp()-v_started_at))*1000)::integer
      ),NULL
    ) AS saved_row
    LIMIT 1;
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',true,'phase_complete',true,
      'phase',v_phase,'next_phase',v_next_phase,'operation',v_saved,
      'round_trip_count',1,
      'elapsed_ms',pg_catalog.floor(extract(epoch FROM (pg_catalog.clock_timestamp()-v_started_at))*1000)::integer
    );
  END IF;

  IF pg_catalog.coalesce((v_chunk.payload_json->>'row_backed')::boolean,false) IS NOT TRUE
     OR pg_catalog.coalesce((v_chunk.payload_json->>'legacy_tiny_compat')::boolean,false) IS TRUE
     OR pg_catalog.coalesce(v_chunk.payload_json->>'source_table','')='diagnostic_legacy_units' THEN
    RAISE EXCEPTION 'DRAFT_CREATE_STEP_CHUNK_NOT_ROW_BACKED'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','DRAFT_CREATE_STEP_CHUNK_NOT_ROW_BACKED','chunk_id',v_chunk.chunk_id,'phase',v_phase
      )::text;
  END IF;

  SELECT pg_catalog.coalesce(pg_catalog.jsonb_agg(scope_id ORDER BY scope_id),'[]'::jsonb)
  INTO v_scope_ids
  FROM (
    SELECT DISTINCT value::uuid AS scope_id
    FROM pg_catalog.jsonb_array_elements_text(
      CASE
        WHEN pg_catalog.jsonb_typeof(v_chunk.payload_json->'units')='array' THEN v_chunk.payload_json->'units'
        WHEN pg_catalog.jsonb_typeof(v_chunk.payload_json->'candidate_scope_ids')='array' THEN v_chunk.payload_json->'candidate_scope_ids'
        WHEN pg_catalog.jsonb_typeof(v_chunk.payload_json->'scope_unit_ids')='array' THEN v_chunk.payload_json->'scope_unit_ids'
        ELSE '[]'::jsonb
      END
    ) AS unit(value)
    WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS scope_ids;
  v_scope_count:=pg_catalog.jsonb_array_length(v_scope_ids);
  IF pg_catalog.coalesce(v_chunk.unit_count,0)>0 AND v_scope_count=0 THEN
    RAISE EXCEPTION 'DRAFT_CREATE_STEP_SCOPE_IDS_MISSING'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','DRAFT_CREATE_STEP_SCOPE_IDS_MISSING','chunk_id',v_chunk.chunk_id,'phase',v_phase
      )::text;
  END IF;

  v_pay_date:=NULLIF(v_operation.input_json->>'pay_date','')::date;
  v_week_start:=NULLIF(v_operation.input_json->>'week_start','')::date;
  v_business_started_at:=pg_catalog.clock_timestamp();

  IF v_phase='SEED_ALLOCATION_ROWS' THEN
    SELECT pg_catalog.to_jsonb(seed_row) INTO v_result
    FROM public.pay_workbench_prepare_draft_allocation_rows_seed(p_operation_id,v_scope_ids) AS seed_row
    LIMIT 1;
    v_result:=pg_catalog.coalesce(v_result,'{}'::jsonb);
    v_results:=pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'candidate_scope_ids',v_scope_ids,'result',v_result
    ));
  ELSE
    FOR v_group IN
      SELECT scope.pay_batch_id,scope.pay_channel,
             pg_catalog.jsonb_agg(scope.id ORDER BY scope.id) AS scope_ids
      FROM public.banking_pay_operation_candidate_scope AS scope
      WHERE scope.operation_id=p_operation_id
        AND scope.id IN (
          SELECT value::uuid FROM pg_catalog.jsonb_array_elements_text(v_scope_ids) AS unit(value)
        )
      GROUP BY scope.pay_batch_id,scope.pay_channel
      ORDER BY scope.pay_batch_id,scope.pay_channel
    LOOP
      IF v_group.pay_batch_id IS NULL THEN
        RAISE EXCEPTION 'DRAFT_CREATE_STEP_BATCH_ID_MISSING' USING ERRCODE='P0001';
      END IF;
      v_result:=CASE v_phase
        WHEN 'INSERT_CANDIDATES' THEN public.pay_batch_insert_candidates_from_preview(
          v_group.pay_batch_id,v_operation.actor_user_id,p_operation_id,v_group.scope_ids)
        WHEN 'INSERT_ITEMS' THEN public.pay_batch_insert_items_from_preview(
          v_group.pay_batch_id,v_operation.actor_user_id,p_operation_id,v_group.scope_ids)
        WHEN 'APPLY_FINANCE_ADJUSTMENTS' THEN public.pay_batch_apply_finance_adjustments(
          v_group.pay_batch_id,v_group.pay_channel,v_operation.actor_user_id,NULL,NULL,p_operation_id,v_group.scope_ids)
        WHEN 'FINALISE_RESERVATIONS' THEN public.pay_batch_finalize_reservations_and_markers(
          v_group.pay_batch_id,v_group.pay_channel,v_operation.actor_user_id,v_pay_date,v_week_start,p_operation_id,v_group.scope_ids)
        WHEN 'POPULATE_CANDIDATE_SUMMARIES' THEN public.pay_batch_populate_candidate_summaries(
          v_group.pay_batch_id,v_group.pay_channel,v_operation.actor_user_id,p_operation_id,v_group.scope_ids)
        WHEN 'CREATE_TIMESHEET_SNAPSHOTS' THEN public.pay_batch_create_timesheet_snapshots(
          v_group.pay_batch_id,v_operation.actor_user_id,p_operation_id,v_group.scope_ids)
        WHEN 'BUILD_ITEM_BREAKDOWNS' THEN public.pay_batch_build_item_breakdowns(
          v_group.pay_batch_id,v_operation.actor_user_id,p_operation_id,v_group.scope_ids)
      END;
      IF pg_catalog.coalesce((v_result->>'ok')::boolean,true) IS NOT TRUE THEN
        RAISE EXCEPTION 'DRAFT_CREATE_STEP_BUSINESS_OWNER_REJECTED'
          USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
            'code','DRAFT_CREATE_STEP_BUSINESS_OWNER_REJECTED','phase',v_phase,
            'chunk_id',v_chunk.chunk_id,'pay_batch_id',v_group.pay_batch_id,
            'result',v_result
          )::text;
      END IF;
      v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'pay_batch_id',v_group.pay_batch_id,'pay_channel',v_group.pay_channel,
        'candidate_scope_ids',v_group.scope_ids,'result',v_result
      ));
    END LOOP;
  END IF;

  v_business_ms:=pg_catalog.floor(extract(epoch FROM (pg_catalog.clock_timestamp()-v_business_started_at))*1000)::integer;

  SELECT pg_catalog.to_jsonb(finished_row) INTO v_finished
  FROM public.banking_pay_operation_finish_chunk(
    v_chunk.chunk_id,'COMPLETE',v_scope_count,0,
    pg_catalog.jsonb_build_object('phase',v_phase,'scope_ids',v_scope_ids,'results',v_results),NULL
  ) AS finished_row
  LIMIT 1;

  SELECT pg_catalog.to_jsonb(saved_row) INTO v_saved
  FROM public.banking_pay_operation_save_progress(
    p_operation_id,'RUNNING',v_phase,NULL,v_scope_count,0,v_chunk.sequence_no,NULL,
    pg_catalog.jsonb_build_object(
      'status_text','Processed one '||v_phase||' Draft chunk.',
      'draft_step_rpc',true,
      'draft_step_phase',v_phase,
      'draft_step_chunk_id',v_chunk.chunk_id,
      'draft_step_business_ms',v_business_ms,
      'draft_step_round_trip_count',1,
      'draft_step_elapsed_ms',pg_catalog.floor(extract(epoch FROM (pg_catalog.clock_timestamp()-v_started_at))*1000)::integer
    ),NULL
  ) AS saved_row
  LIMIT 1;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'handled',true,'phase_complete',false,
    'phase',v_phase,'next_phase',v_phase,'chunk_id',v_chunk.chunk_id,
    'scope_count',v_scope_count,'phase_result',v_results,
    'chunk',v_finished,'operation',v_saved,'business_ms',v_business_ms,
    'round_trip_count',1,
    'elapsed_ms',pg_catalog.floor(extract(epoch FROM (pg_catalog.clock_timestamp()-v_started_at))*1000)::integer
  );
END;
$function$;

ALTER FUNCTION public.banking_pay_draft_create_step_v1(uuid,text,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_draft_create_step_v1(uuid,text,text,integer)
  FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.banking_pay_draft_create_step_v1(uuid,text,text,integer)
  TO postgres,service_role;

COMMENT ON FUNCTION public.banking_pay_draft_create_step_v1(uuid,text,text,integer)
IS 'Service-only single-transaction claim/business/finish/progress step for one row-backed Draft-create phase.';
