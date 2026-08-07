/*
 * Banking Pay targeted lifecycle delta authority.
 *
 * These helpers deliberately contain no payment formula, settlement,
 * remittance or post-draft fallback.  They only establish session scope,
 * seal a pre-draft targeted-delta proof, and atomically promote a previously
 * sealed staging result.
 */

CREATE OR REPLACE FUNCTION private.pay_workbench_session_candidate_scope_ensure_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_dirty_job_id uuid,
  p_source_change_seq bigint,
  p_reason text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_context jsonb := '{}'::jsonb;
  v_candidate_ids jsonb := '[]'::jsonb;
  v_filter_candidate_id uuid;
  v_filter_client_id uuid;
  v_eligible boolean := false;
  v_inserted boolean := false;
  v_scope_id uuid;
  v_scope_ordinal bigint;
BEGIN
  IF p_session_id IS NULL OR p_candidate_id IS NULL OR p_dirty_job_id IS NULL
     OR p_source_change_seq IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ENSURE_ARGUMENT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code','PAY_WORKBENCH_SCOPE_ENSURE_ARGUMENT_REQUIRED')::text;
  END IF;

  IF NOT pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtextextended(
      public._pay_workbench_candidate_serial_key(p_candidate_id),
      24062027
    )
  ) THEN
    RETURN jsonb_build_object(
      'ok', true,
      'eligible', false,
      'inserted', false,
      'reused', false,
      'reason', 'CANDIDATE_SERIAL_BUSY'
    );
  END IF;

  SELECT job_row.*
  INTO v_job
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.id = p_dirty_job_id
  FOR UPDATE;

  IF NOT FOUND
     OR v_job.job_type <> 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
     OR v_job.status <> 'RUNNING'
     OR v_job.candidate_id IS DISTINCT FROM p_candidate_id THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ENSURE_DIRTY_JOB_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code','PAY_WORKBENCH_SCOPE_ENSURE_DIRTY_JOB_INVALID',
              'job_id',p_dirty_job_id,
              'candidate_id',p_candidate_id
            )::text;
  END IF;

  SELECT registry_row.*
  INTO v_registry
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry_row
  WHERE registry_row.candidate_id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND
     OR COALESCE(v_registry.current_source_change_seq, 0) <> p_source_change_seq THEN
    RETURN jsonb_build_object(
      'ok', true,
      'eligible', false,
      'inserted', false,
      'reused', false,
      'reason', 'SOURCE_CHANGE_SEQUENCE_STALE'
    );
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
  FOR UPDATE;

  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_session.status,''))) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'eligible', false,
      'inserted', false,
      'reused', false,
      'reason', 'SESSION_NOT_OPEN'
    );
  END IF;

  IF pg_catalog.btrim(COALESCE(
       v_session.filters_json->>'candidate_id',
       v_session.filters_json->>'candidateId',''
     )) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_filter_candidate_id := COALESCE(
      v_session.filters_json->>'candidate_id',
      v_session.filters_json->>'candidateId'
    )::uuid;
  END IF;

  IF v_filter_candidate_id IS NOT NULL
     AND v_filter_candidate_id IS DISTINCT FROM p_candidate_id THEN
    RETURN jsonb_build_object(
      'ok', true,
      'eligible', false,
      'inserted', false,
      'reused', false,
      'reason', 'CANDIDATE_FILTER_EXCLUDED'
    );
  END IF;

  IF pg_catalog.btrim(COALESCE(
       v_session.filters_json->>'client_id',
       v_session.filters_json->>'clientId',''
     )) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_filter_client_id := COALESCE(
      v_session.filters_json->>'client_id',
      v_session.filters_json->>'clientId'
    )::uuid;
  END IF;

  v_context := public.pay_preview_build_context(
    p_pay_date => v_session.pay_date,
    p_week_ending_cutoff => v_session.week_ending_cutoff,
    p_actor_user_id => v_session.actor_user_id,
    p_candidate_id => p_candidate_id,
    p_client_id => v_filter_client_id,
    p_preview_decisions_json => COALESCE(v_session.filters_json,'{}'::jsonb)
      || jsonb_build_object(
        'preview_context_mode','PAGE',
        'scope_limit',1,
        'scope_cursor',NULL::jsonb,
        'scope_candidate_ensure',true
      )
  );

  v_candidate_ids := CASE
    WHEN jsonb_typeof(v_context->'candidate_ids') = 'array'
      THEN COALESCE(v_context->'candidate_ids','[]'::jsonb)
    WHEN jsonb_typeof(v_context->'scope_candidate_ids') = 'array'
      THEN COALESCE(v_context->'scope_candidate_ids','[]'::jsonb)
    ELSE '[]'::jsonb
  END;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements_text(v_candidate_ids) AS candidate_value(value)
    WHERE pg_catalog.btrim(candidate_value.value) ~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND pg_catalog.btrim(candidate_value.value)::uuid = p_candidate_id
  ) INTO v_eligible;

  IF NOT v_eligible THEN
    RETURN jsonb_build_object(
      'ok', true,
      'eligible', false,
      'inserted', false,
      'reused', false,
      'reason', 'CANDIDATE_NOT_SESSION_ELIGIBLE'
    );
  END IF;

  SELECT scope_row.id, scope_row.scope_ordinal
  INTO v_scope_id, v_scope_ordinal
  FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id = p_session_id
    AND scope_row.candidate_id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND THEN
    SELECT COALESCE(pg_catalog.max(scope_row.scope_ordinal),0) + 1
    INTO v_scope_ordinal
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = p_session_id;

    INSERT INTO public.banking_pay_workbench_session_scope(
      session_id,candidate_id,scope_ordinal,status,pending_job_id,
      seeded,dirty,error_json,created_at_utc,updated_at_utc
    )
    VALUES(
      p_session_id,p_candidate_id,v_scope_ordinal,'PENDING',NULL,
      true,true,NULL,v_now,v_now
    )
    ON CONFLICT (session_id,candidate_id) DO NOTHING
    RETURNING id INTO v_scope_id;

    v_inserted := v_scope_id IS NOT NULL;

    IF NOT v_inserted THEN
      SELECT scope_row.id, scope_row.scope_ordinal
      INTO v_scope_id, v_scope_ordinal
      FROM public.banking_pay_workbench_session_scope AS scope_row
      WHERE scope_row.session_id = p_session_id
        AND scope_row.candidate_id = p_candidate_id
      FOR UPDATE;
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'eligible', true,
    'inserted', v_inserted,
    'reused', NOT v_inserted,
    'baseline_required', v_inserted,
    'scope_id', v_scope_id,
    'scope_ordinal', v_scope_ordinal,
    'source_change_seq', p_source_change_seq,
    'reason', CASE WHEN v_inserted THEN 'NEW_SCOPE_BASELINE_REQUIRED' ELSE COALESCE(NULLIF(p_reason,''),'SCOPE_REUSED') END
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_session_candidate_scope_ensure_v1(uuid,uuid,uuid,bigint,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_session_candidate_scope_ensure_v1(uuid,uuid,uuid,bigint,text) FROM PUBLIC, anon, authenticated, service_role;


CREATE OR REPLACE FUNCTION private.pay_workbench_targeted_delta_admission_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_projection_run_id uuid,
  p_event_class text,
  p_targeted_timesheet_ids uuid[],
  p_linked_timesheet_ids uuid[],
  p_source_change_seq bigint
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_event_class text := pg_catalog.upper(pg_catalog.btrim(COALESCE(p_event_class,'')));
  v_targets uuid[] := ARRAY[]::uuid[];
  v_linked uuid[] := ARRAY[]::uuid[];
  v_affected uuid[] := ARRAY[]::uuid[];
  v_closure jsonb := '{}'::jsonb;
  v_owner_count integer := 0;
  v_finance_count integer := 0;
  v_settled_count integer := 0;
  v_connected_count integer := 0;
  v_current_count integer := 0;
  v_current_build_count integer := 0;
  v_unaffected_count integer := 0;
  v_target_before_count integer := 0;
  v_unaffected_digest text;
  v_target_before_digest text;
  v_input_digest text;
  v_preview_digest text;
  v_existing_seal_json jsonb := '{}'::jsonb;
  v_current_build_id uuid;
  v_current_source_change_seq bigint;
  v_accepted_full_build_id uuid;
  v_source_run_digest text;
  v_seal jsonb;
  v_seal_digest text;
BEGIN
  IF p_session_id IS NULL OR p_candidate_id IS NULL OR p_source_change_seq IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_TARGETED_DELTA_ADMISSION_ARGUMENT_REQUIRED'
      USING ERRCODE='P0001', DETAIL=jsonb_build_object('code','PAY_WORKBENCH_TARGETED_DELTA_ADMISSION_ARGUMENT_REQUIRED')::text;
  END IF;

  IF v_event_class NOT IN ('AUTHORISE','UNAUTHORISE') THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_EVENT_NOT_SUPPORTED');
  END IF;

  SELECT COALESCE(array_agg(DISTINCT value ORDER BY value),ARRAY[]::uuid[])
  INTO v_targets
  FROM unnest(COALESCE(p_targeted_timesheet_ids,ARRAY[]::uuid[])) AS target(value)
  WHERE value IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT value ORDER BY value),ARRAY[]::uuid[])
  INTO v_linked
  FROM unnest(COALESCE(p_linked_timesheet_ids,ARRAY[]::uuid[])) AS linked(value)
  WHERE value IS NOT NULL;

  IF COALESCE(array_length(v_targets,1),0) = 0 OR array_length(v_targets,1) > 250 THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_TARGET_INVALID');
  END IF;

  SELECT session_row.* INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id=p_session_id;
  IF NOT FOUND OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_session.status,''))) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_SESSION_STALE');
  END IF;

  SELECT scope_row.* INTO v_scope
  FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id=p_session_id AND scope_row.candidate_id=p_candidate_id;
  IF NOT FOUND OR pg_catalog.upper(COALESCE(v_scope.status,'')) NOT IN ('READY','MATERIALISED','SOURCE_EMPTY','READY_EMPTY')
     OR COALESCE(v_scope.dirty,false) THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_BASELINE_NOT_ESTABLISHED');
  END IF;

  SELECT registry_row.* INTO v_registry
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry_row
  WHERE registry_row.candidate_id=p_candidate_id;
  IF NOT FOUND OR COALESCE(v_registry.current_source_change_seq,0) <> p_source_change_seq
     OR COALESCE(v_registry.evaluated_generation,-1) <> COALESCE(v_registry.dirty_generation,0) THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_GENERATION_STALE');
  END IF;

  SELECT completed_build.id
  INTO v_accepted_full_build_id
  FROM private.banking_pay_workbench_economic_builds AS completed_build
  WHERE completed_build.session_id=p_session_id
    AND completed_build.candidate_id=p_candidate_id
    AND completed_build.session_version=v_session.version
    AND completed_build.status='COMPLETE'
    AND completed_build.private_stage='COMPLETE'
    AND completed_build.completed_at_utc IS NOT NULL
    AND completed_build.captured_candidate_generation<=v_registry.dirty_generation
    AND completed_build.source_change_seq<=p_source_change_seq
  ORDER BY completed_build.source_change_seq DESC,
           completed_build.completed_at_utc DESC,
           completed_build.id DESC
  LIMIT 1;
  IF v_accepted_full_build_id IS NULL THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_BASELINE_NOT_ESTABLISHED');
  END IF;

  SELECT count(DISTINCT target.value)::integer
  INTO v_owner_count
  FROM unnest(v_targets) AS target(value)
  JOIN public.timesheets AS timesheet_row ON timesheet_row.timesheet_id=target.value
  LEFT JOIN public.contracts AS contract_row ON contract_row.id=timesheet_row.contract_id
  WHERE contract_row.candidate_id=p_candidate_id
     OR EXISTS (
       SELECT 1 FROM public.timesheets_financials AS tf
       WHERE tf.timesheet_id=target.value AND tf.candidate_id=p_candidate_id
     );
  IF v_owner_count <> array_length(v_targets,1) THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_TARGET_OWNERSHIP_MISMATCH');
  END IF;

  v_closure := public._pay_workbench_refresh_dependency_closure_v1(
    p_candidate_id,v_targets,v_linked,ARRAY[]::uuid[],250,100
  );
  IF COALESCE((v_closure->>'coverage_complete')::boolean,false) IS NOT TRUE
     OR COALESCE((v_closure->>'requires_full_candidate')::boolean,true) THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,
      'reason',COALESCE(v_closure->>'fallback_reason','TARGETED_DELTA_DEPENDENCY_INCOMPLETE'));
  END IF;

  SELECT COALESCE(array_agg(DISTINCT value::uuid ORDER BY value::uuid),ARRAY[]::uuid[])
  INTO v_affected
  FROM jsonb_array_elements_text(COALESCE(v_closure->'effective_targeted_timesheet_ids','[]'::jsonb)) AS affected(value)
  WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  IF v_affected IS DISTINCT FROM v_targets THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_CONNECTED_SCOPE_PRESENT');
  END IF;

  SELECT count(*)::integer INTO v_connected_count
  FROM public.timesheets AS t
  WHERE t.timesheet_id=ANY(v_affected)
    AND (t.correction_id IS NOT NULL OR COALESCE(t.is_adjustment,false)
         OR t.parent_timesheet_id IS NOT NULL);
  SELECT v_connected_count + count(*)::integer INTO v_connected_count
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id=ANY(v_affected) AND tf.is_current
    AND tf.nhsp_import_id IS NOT NULL;
  IF v_connected_count > 0 THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_CONNECTED_AUTHORITY_PRESENT');
  END IF;

  SELECT count(*)::integer INTO v_finance_count
  FROM public.pay_finance_case_components AS c
  WHERE c.candidate_id=p_candidate_id
    AND (c.linked_timesheet_id IS NULL OR c.linked_timesheet_id=ANY(v_affected));
  SELECT v_finance_count + count(*)::integer INTO v_finance_count
  FROM public.pay_advances AS a
  WHERE a.candidate_id=p_candidate_id
    AND pg_catalog.upper(COALESCE(a.status::text,'')) IN ('ACTIVE','PAUSED')
    AND a.cleared_at_utc IS NULL AND a.written_off_at_utc IS NULL
    AND (a.linked_timesheet_id IS NULL OR a.linked_timesheet_id=ANY(v_affected));
  SELECT v_finance_count + count(*)::integer INTO v_finance_count
  FROM public.ts_pay_adjustments AS a
  WHERE a.candidate_id=p_candidate_id AND a.paid_at_utc IS NULL
    AND a.timesheet_id=ANY(v_affected);
  SELECT v_finance_count + count(*)::integer INTO v_finance_count
  FROM public.timesheet_payment_overrides AS o
  WHERE o.candidate_id=p_candidate_id AND o.timesheet_id=ANY(v_affected)
    AND o.consumed_at_utc IS NULL AND o.cleared_at_utc IS NULL;
  IF v_finance_count > 0 THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_FINANCE_AUTHORITY_PRESENT');
  END IF;

  SELECT count(*)::integer INTO v_settled_count
  FROM public.timesheet_pay_state AS ps
  WHERE ps.timesheet_id=ANY(v_affected)
    AND (ps.last_settled_signature IS NOT NULL OR ps.last_settled_pay_batch_id IS NOT NULL);
  SELECT v_settled_count + count(*)::integer INTO v_settled_count
  FROM public.pay_batch_items AS bi
  WHERE bi.timesheet_id=ANY(v_affected) AND NOT COALESCE(bi.is_voided,false);
  SELECT v_settled_count + count(*)::integer INTO v_settled_count
  FROM public.pay_payment_correction_items AS ci
  WHERE ci.candidate_id=p_candidate_id AND ci.timesheet_id=ANY(v_affected);
  IF v_settled_count > 0 THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_SETTLED_AUTHORITY_PRESENT');
  END IF;

  SELECT count(*)::integer,
         count(DISTINCT source_build_run_id)::integer,
         pg_catalog.min(source_build_run_id),
         pg_catalog.min(source_change_seq)
  INTO v_current_count,v_current_build_count,v_current_build_id,v_current_source_change_seq
  FROM public.banking_pay_workbench_candidate_source_lines AS source_row
  WHERE source_row.session_id=p_session_id AND source_row.candidate_id=p_candidate_id
    AND source_row.status='CURRENT';
  SELECT pg_catalog.encode(extensions.digest(pg_catalog.convert_to(COALESCE(
    pg_catalog.string_agg(run_id::text,'|' ORDER BY run_id::text),'EMPTY'),'UTF8'),'sha256'),'hex')
  INTO v_source_run_digest
  FROM (
    SELECT DISTINCT source_row.source_build_run_id AS run_id
    FROM public.banking_pay_workbench_candidate_source_lines AS source_row
    WHERE source_row.session_id=p_session_id
      AND source_row.candidate_id=p_candidate_id
      AND source_row.status='CURRENT'
      AND source_row.source_build_run_id IS NOT NULL
  ) AS current_runs;

  IF EXISTS (
        SELECT 1 FROM public.banking_pay_workbench_candidate_source_lines AS source_row
        WHERE source_row.session_id=p_session_id AND source_row.candidate_id=p_candidate_id
         AND (
           source_row.status = 'ERROR'
           OR (
             source_row.status = 'DIRTY'
             AND (
               p_projection_run_id IS NULL
               OR source_row.source_build_run_id IS DISTINCT FROM p_projection_run_id
             )
           )
         )
     )
     OR EXISTS (
       SELECT 1
       FROM public.banking_pay_workbench_candidate_source_lines AS source_row
       WHERE source_row.session_id=p_session_id
         AND source_row.candidate_id=p_candidate_id
         AND source_row.status='CURRENT'
         AND (
           source_row.session_version IS DISTINCT FROM v_session.version
           OR source_row.source_change_seq>p_source_change_seq
           OR source_row.source_build_run_id IS NULL
           OR NOT (
             EXISTS (
               SELECT 1
               FROM private.banking_pay_workbench_economic_builds AS source_build
               WHERE source_build.source_build_run_id=source_row.source_build_run_id
                 AND source_build.session_id=p_session_id
                 AND source_build.candidate_id=p_candidate_id
                 AND source_build.status='COMPLETE'
                 AND source_build.private_stage='COMPLETE'
                 AND source_build.completed_at_utc IS NOT NULL
             )
             OR EXISTS (
               SELECT 1
               FROM public.banking_pay_workbench_candidate_delta_projection_runs AS source_delta
               WHERE source_delta.id=source_row.source_build_run_id
                 AND source_delta.session_id=p_session_id
                 AND source_delta.candidate_id=p_candidate_id
                 AND source_delta.status='COMPLETED'
                 AND source_delta.completed_at_utc IS NOT NULL
             )
           )
         )
     ) THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_SOURCE_NOT_CURRENT');
  END IF;
  IF v_current_count=0 AND pg_catalog.upper(COALESCE(v_scope.status,'')) NOT IN ('SOURCE_EMPTY','READY_EMPTY') THEN
    RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_SOURCE_NOT_CURRENT');
  END IF;

  SELECT count(*)::integer,
         pg_catalog.encode(extensions.digest(pg_catalog.convert_to(COALESCE(jsonb_agg(
           jsonb_build_object('id',s.id,'ordinal',s.source_ordinal,'line_key',s.line_key,
             'timesheet_id',s.timesheet_id,'row',s.source_row_json,'key',s.economic_key_json)
           ORDER BY s.source_ordinal,s.id)::text,'[]'),'UTF8'),'sha256'),'hex')
  INTO v_unaffected_count,v_unaffected_digest
  FROM public.banking_pay_workbench_candidate_source_lines AS s
  WHERE s.session_id=p_session_id AND s.candidate_id=p_candidate_id
    AND s.status='CURRENT'
    AND (s.timesheet_id IS NULL OR NOT (s.timesheet_id=ANY(v_affected)));

  SELECT count(*)::integer,
         pg_catalog.encode(extensions.digest(pg_catalog.convert_to(COALESCE(jsonb_agg(
           jsonb_build_object('id',s.id,'ordinal',s.source_ordinal,'line_key',s.line_key,
             'timesheet_id',s.timesheet_id,'row',s.source_row_json,'key',s.economic_key_json)
           ORDER BY s.source_ordinal,s.id)::text,'[]'),'UTF8'),'sha256'),'hex')
  INTO v_target_before_count,v_target_before_digest
  FROM public.banking_pay_workbench_candidate_source_lines AS s
  WHERE s.session_id=p_session_id AND s.candidate_id=p_candidate_id
    AND s.status='CURRENT' AND s.timesheet_id=ANY(v_affected);

  SELECT pg_catalog.encode(extensions.digest(pg_catalog.convert_to(COALESCE(jsonb_agg(
    jsonb_build_object('timesheet',to_jsonb(t),'financial',COALESCE(tf.rows,'[]'::jsonb),'pay_state',to_jsonb(ps))
    ORDER BY t.timesheet_id)::text,'[]'),'UTF8'),'sha256'),'hex')
  INTO v_input_digest
  FROM public.timesheets AS t
  LEFT JOIN LATERAL (
    SELECT jsonb_agg(to_jsonb(f) ORDER BY f.id) AS rows
    FROM public.timesheets_financials AS f
    WHERE f.timesheet_id=t.timesheet_id AND f.is_current
  ) AS tf ON true
  LEFT JOIN public.timesheet_pay_state AS ps ON ps.timesheet_id=t.timesheet_id
  WHERE t.timesheet_id=ANY(v_affected);

  IF p_projection_run_id IS NOT NULL THEN
    SELECT projection_run.admission_seal_json
    INTO v_existing_seal_json
    FROM public.banking_pay_workbench_candidate_delta_projection_runs projection_run
    WHERE projection_run.id=p_projection_run_id
      AND projection_run.session_id=p_session_id
      AND projection_run.candidate_id=p_candidate_id
      AND projection_run.admission_seal_version=1;
  END IF;

  IF COALESCE(v_existing_seal_json,'{}'::jsonb) <> '{}'::jsonb THEN
    IF EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_preview_rows p
      WHERE p.session_id=p_session_id AND p.candidate_id=p_candidate_id
        AND p.timesheet_id=ANY(v_affected)
        AND p.row_json ? 'targeted_delta_stage'
        AND p.row_json#>>'{targeted_delta_stage,projection_run_id}' IS DISTINCT FROM p_projection_run_id::text
    ) THEN
      RETURN jsonb_build_object('ok',true,'admitted',false,'full_route_required',true,'reason','TARGETED_DELTA_FOREIGN_STAGING_PRESENT');
    END IF;
    v_preview_digest:=v_existing_seal_json->>'preview_selection_digest';
  ELSE
    SELECT pg_catalog.encode(extensions.digest(pg_catalog.convert_to(COALESCE(jsonb_agg(
      jsonb_build_object('id',p.id,'row_key',p.row_key,'selected',p.selected,
        'selection_state',p.selection_state,'status',p.status)
      ORDER BY p.section,p.row_ordinal,p.id)::text,'[]'),'UTF8'),'sha256'),'hex')
    INTO v_preview_digest
    FROM public.banking_pay_workbench_preview_rows AS p
    WHERE p.session_id=p_session_id AND p.candidate_id=p_candidate_id
      AND p.timesheet_id=ANY(v_affected);
  END IF;

  v_seal := jsonb_build_object(
    'seal_version',1,
    'projection_run_id',p_projection_run_id,
    'session_id',p_session_id,
    'candidate_id',p_candidate_id,
    'event_class',v_event_class,
    'session_version',v_session.version,
    'source_snapshot_run_id',v_session.source_snapshot_run_id,
    'source_change_seq',p_source_change_seq,
    'candidate_dirty_generation',v_registry.dirty_generation,
    'candidate_evaluated_generation',v_registry.evaluated_generation,
    'accepted_build_id',v_accepted_full_build_id,
    'accepted_source_build_run_id',CASE WHEN v_current_build_count=1 THEN v_current_build_id ELSE NULL::uuid END,
    'accepted_source_run_count',v_current_build_count,
    'accepted_source_run_digest',v_source_run_digest,
    'accepted_source_change_seq',v_current_source_change_seq,
    'affected_timesheet_ids',to_jsonb(v_affected),
    'unaffected_source_count',v_unaffected_count,
    'unaffected_source_digest',v_unaffected_digest,
    'target_before_count',v_target_before_count,
    'target_before_digest',v_target_before_digest,
    'input_digest',v_input_digest,
    'preview_selection_digest',v_preview_digest,
    'current_source_count',v_current_count,
    'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
  );
  v_seal_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_seal::text,'UTF8'),'sha256'),'hex'
  );

  RETURN jsonb_build_object(
    'ok',true,'admitted',true,'full_route_required',false,'seal_version',1,
    'admission_seal_json',v_seal,'admission_seal_digest',v_seal_digest,
    'reason','TARGETED_DELTA_ADMITTED'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_targeted_delta_admission_v1(uuid,uuid,uuid,text,uuid[],uuid[],bigint) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_targeted_delta_admission_v1(uuid,uuid,uuid,text,uuid[],uuid[],bigint) FROM PUBLIC, anon, authenticated, service_role;


CREATE OR REPLACE FUNCTION private.pay_workbench_targeted_delta_scope_finalize_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_projection_run_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_run public.banking_pay_workbench_candidate_delta_projection_runs%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_seal jsonb;
  v_seal_digest text;
  v_admission jsonb;
  v_targets uuid[] := ARRAY[]::uuid[];
  v_event_class text;
  v_source_promoted integer := 0;
  v_source_retired integer := 0;
  v_line_promoted integer := 0;
  v_line_retired integer := 0;
  v_preview_promoted integer := 0;
  v_preview_retired integer := 0;
  v_state jsonb;
BEGIN
  IF p_session_id IS NULL OR p_candidate_id IS NULL OR p_projection_run_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_TARGETED_DELTA_FINALIZE_ARGUMENT_REQUIRED' USING ERRCODE='P0001';
  END IF;

  SELECT run_row.* INTO v_run
  FROM public.banking_pay_workbench_candidate_delta_projection_runs AS run_row
  WHERE run_row.id=p_projection_run_id
    AND run_row.session_id=p_session_id
    AND run_row.candidate_id=p_candidate_id
  FOR UPDATE;
  IF NOT FOUND OR v_run.status <> 'RUNNING' OR COALESCE(v_run.admission_seal_version,0) <> 1
     OR jsonb_typeof(v_run.admission_seal_json) <> 'object'
     OR v_run.admission_seal_digest IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_TARGETED_DELTA_FINALIZE_RUN_INVALID' USING ERRCODE='P0001';
  END IF;

  v_seal := v_run.admission_seal_json;
  v_seal_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_seal::text,'UTF8'),'sha256'),'hex'
  );
  IF v_seal_digest IS DISTINCT FROM v_run.admission_seal_digest
     OR v_seal->>'projection_run_id' IS DISTINCT FROM p_projection_run_id::text
     OR v_seal->>'session_id' IS DISTINCT FROM p_session_id::text
     OR v_seal->>'candidate_id' IS DISTINCT FROM p_candidate_id::text THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_TARGETED_DELTA_FINALIZE_SEAL_INVALID' USING ERRCODE='P0001';
  END IF;

  SELECT COALESCE(array_agg(value::uuid ORDER BY value::uuid),ARRAY[]::uuid[])
  INTO v_targets
  FROM jsonb_array_elements_text(COALESCE(v_seal->'affected_timesheet_ids','[]'::jsonb)) AS target(value)
  WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  v_event_class := v_seal->>'event_class';

  SELECT session_row.* INTO v_session FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id=p_session_id FOR UPDATE;
  SELECT scope_row.* INTO v_scope FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id=p_session_id AND scope_row.candidate_id=p_candidate_id FOR UPDATE;
  SELECT registry_row.* INTO v_registry FROM private.banking_pay_workbench_candidate_scope_registry AS registry_row
  WHERE registry_row.candidate_id=p_candidate_id FOR UPDATE;
  IF v_session.id IS NULL OR v_scope.id IS NULL OR v_registry.candidate_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_TARGETED_DELTA_FINALIZE_AUTHORITY_MISSING' USING ERRCODE='P0001';
  END IF;

  v_admission := private.pay_workbench_targeted_delta_admission_v1(
    p_session_id,p_candidate_id,p_projection_run_id,v_event_class,v_targets,ARRAY[]::uuid[],v_run.source_change_seq
  );
  IF COALESCE((v_admission->>'admitted')::boolean,false) IS NOT TRUE
     OR v_admission->>'admission_seal_digest' IS DISTINCT FROM v_run.admission_seal_digest THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_TARGETED_DELTA_FINALIZE_PROOF_STALE'
      USING ERRCODE='P0001', DETAIL=jsonb_build_object('code','PAY_WORKBENCH_TARGETED_DELTA_FINALIZE_PROOF_STALE','reason',v_admission->>'reason')::text;
  END IF;

  UPDATE public.banking_pay_workbench_candidate_source_lines AS old_source
  SET status='SUPERSEDED',updated_at_utc=v_now
  WHERE old_source.session_id=p_session_id AND old_source.candidate_id=p_candidate_id
    AND old_source.status='CURRENT' AND old_source.timesheet_id=ANY(v_targets);
  GET DIAGNOSTICS v_source_retired = ROW_COUNT;

  UPDATE public.banking_pay_workbench_candidate_source_lines AS staged_source
  SET status='CURRENT',updated_at_utc=v_now
  WHERE staged_source.session_id=p_session_id AND staged_source.candidate_id=p_candidate_id
    AND staged_source.source_build_run_id=p_projection_run_id
    AND staged_source.status='DIRTY' AND staged_source.timesheet_id=ANY(v_targets);
  GET DIAGNOSTICS v_source_promoted = ROW_COUNT;

  UPDATE public.banking_pay_workbench_candidate_line_work AS line_row
  SET status=COALESCE(NULLIF(line_row.work_payload_json#>>'{targeted_delta_stage,final_status}',''),'READY'),
      work_payload_json=line_row.work_payload_json-'targeted_delta_stage',
      updated_at_utc=v_now
  WHERE line_row.session_id=p_session_id AND line_row.candidate_id=p_candidate_id
    AND line_row.timesheet_id=ANY(v_targets)
    AND line_row.work_payload_json#>>'{targeted_delta_stage,projection_run_id}'=p_projection_run_id::text;
  GET DIAGNOSTICS v_line_promoted = ROW_COUNT;

  UPDATE public.banking_pay_workbench_candidate_line_work AS line_row
  SET status='SKIPPED',updated_at_utc=v_now
  WHERE line_row.session_id=p_session_id AND line_row.candidate_id=p_candidate_id
    AND line_row.timesheet_id=ANY(v_targets)
    AND line_row.work_payload_json#>>'{targeted_delta_stage,projection_run_id}' IS DISTINCT FROM p_projection_run_id::text
    AND NOT EXISTS (
      SELECT 1 FROM public.banking_pay_workbench_candidate_source_lines AS current_source
      WHERE current_source.session_id=p_session_id AND current_source.candidate_id=p_candidate_id
        AND current_source.status='CURRENT' AND current_source.timesheet_id=line_row.timesheet_id
        AND current_source.line_key=line_row.line_key
    );
  GET DIAGNOSTICS v_line_retired = ROW_COUNT;

  UPDATE public.banking_pay_workbench_preview_rows AS preview_row
  SET selected=COALESCE((preview_row.row_json#>>'{targeted_delta_stage,final_selected}')::boolean,false),
      selection_state=COALESCE(NULLIF(preview_row.row_json#>>'{targeted_delta_stage,final_selection_state}',''),'NOT_SELECTED'),
      status=COALESCE(NULLIF(preview_row.row_json#>>'{targeted_delta_stage,final_status}',''),'READY'),
      row_json=preview_row.row_json-'targeted_delta_stage',
      updated_at_utc=v_now
  WHERE preview_row.session_id=p_session_id AND preview_row.candidate_id=p_candidate_id
    AND preview_row.timesheet_id=ANY(v_targets)
    AND preview_row.row_json#>>'{targeted_delta_stage,projection_run_id}'=p_projection_run_id::text;
  GET DIAGNOSTICS v_preview_promoted = ROW_COUNT;

  UPDATE public.banking_pay_workbench_preview_rows AS preview_row
  SET selected=false,selection_state='NOT_SELECTABLE',status='SUPERSEDED',updated_at_utc=v_now
  WHERE preview_row.session_id=p_session_id AND preview_row.candidate_id=p_candidate_id
    AND preview_row.timesheet_id=ANY(v_targets)
    AND preview_row.row_json#>>'{targeted_delta_stage,projection_run_id}' IS DISTINCT FROM p_projection_run_id::text
    AND NOT EXISTS (
      SELECT 1 FROM public.banking_pay_workbench_candidate_source_lines AS current_source
      WHERE current_source.session_id=p_session_id AND current_source.candidate_id=p_candidate_id
        AND current_source.status='CURRENT' AND current_source.timesheet_id=preview_row.timesheet_id
    );
  GET DIAGNOSTICS v_preview_retired = ROW_COUNT;

  v_state := public.pay_workbench_delta_update_candidate_state_v1(
    p_session_id,p_candidate_id,p_projection_run_id,
    jsonb_build_object('context','TARGETED_LIFECYCLE_DELTA_FINALIZE','source_change_seq',v_run.source_change_seq)
  );

  UPDATE private.banking_pay_workbench_timesheet_scope_state AS state_row
  SET economic_state=CASE WHEN EXISTS (
        SELECT 1 FROM public.banking_pay_workbench_candidate_source_lines AS s
        WHERE s.session_id=p_session_id AND s.candidate_id=p_candidate_id
          AND s.timesheet_id=state_row.timesheet_id AND s.status='CURRENT'
      ) THEN 'LIVE' ELSE 'CLOSED' END,
      evaluated_generation=state_row.dirty_generation,
      evaluated_input_fingerprint=state_row.current_input_fingerprint,
      last_evaluated_at_utc=v_now,
      closed_at_utc=CASE WHEN EXISTS (
        SELECT 1 FROM public.banking_pay_workbench_candidate_source_lines AS s
        WHERE s.session_id=p_session_id AND s.candidate_id=p_candidate_id
          AND s.timesheet_id=state_row.timesheet_id AND s.status='CURRENT'
      ) THEN NULL ELSE v_now END,
      updated_at_utc=v_now
  WHERE state_row.candidate_id=p_candidate_id AND state_row.timesheet_id=ANY(v_targets);

  UPDATE public.banking_pay_workbench_candidate_delta_projection_runs AS run_update
  SET status='COMPLETED',phase='FINALISE',fallback_required=false,fallback_reason=NULL,
      candidate_state_updated=true,completed_at_utc=v_now,updated_at_utc=v_now,
      diagnostics_json=COALESCE(run_update.diagnostics_json,'{}'::jsonb)||jsonb_build_object(
        'atomic_promotion',true,'source_promoted',v_source_promoted,'source_retired',v_source_retired,
        'line_promoted',v_line_promoted,'line_retired',v_line_retired,
        'preview_promoted',v_preview_promoted,'preview_retired',v_preview_retired
      )
  WHERE run_update.id=p_projection_run_id;

  PERFORM public.pay_workbench_session_recompute_progress_counters(
    p_session_id,true,'TARGETED_LIFECYCLE_DELTA_FINALISE',true
  );

  RETURN jsonb_build_object(
    'ok',true,'status','COMPLETED','projection_run_id',p_projection_run_id,
    'source_promoted',v_source_promoted,'source_retired',v_source_retired,
    'line_promoted',v_line_promoted,'line_retired',v_line_retired,
    'preview_promoted',v_preview_promoted,'preview_retired',v_preview_retired,
    'candidate_state',v_state
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_targeted_delta_scope_finalize_v1(uuid,uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_targeted_delta_scope_finalize_v1(uuid,uuid,uuid) FROM PUBLIC, anon, authenticated, service_role;
