CREATE OR REPLACE FUNCTION public.pay_workbench_mark_candidate_dirty()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_trigger_table text := lower(TG_TABLE_NAME);
  v_new_row jsonb := '{}'::jsonb;
  v_old_row jsonb := '{}'::jsonb;
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_id uuid;
  v_live_change_seq bigint := 0;
  v_reason text;
  v_payload_json jsonb;
  v_dedupe_key text;
BEGIN
  IF TG_OP <> 'DELETE' THEN
    v_new_row := to_jsonb(NEW);
  END IF;

  IF TG_OP <> 'INSERT' THEN
    v_old_row := to_jsonb(OLD);
  END IF;

  IF v_trigger_table = 'candidates' THEN
    v_candidate_ids := array_cat(
      v_candidate_ids,
      ARRAY[
        nullif(btrim(COALESCE(v_old_row->>'id', '')), '')::uuid,
        nullif(btrim(COALESCE(v_new_row->>'id', '')), '')::uuid
      ]
    );

  ELSIF v_trigger_table = 'umbrellas' THEN
    SELECT COALESCE(array_agg(DISTINCT c.id), ARRAY[]::uuid[])
    INTO v_candidate_ids
    FROM public.candidates c
    WHERE c.umbrella_id IN (
      nullif(btrim(COALESCE(v_old_row->>'id', '')), '')::uuid,
      nullif(btrim(COALESCE(v_new_row->>'id', '')), '')::uuid
    );

  ELSIF v_trigger_table = 'timesheets_financials' THEN
    v_candidate_ids := array_cat(
      v_candidate_ids,
      ARRAY[
        nullif(btrim(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
        nullif(btrim(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
      ]
    );

  ELSIF v_trigger_table = 'timesheets' THEN
    SELECT array_cat(
             array_cat(
               ARRAY[
                 (
                   SELECT ct.candidate_id
                   FROM public.contracts ct
                   WHERE ct.id = nullif(btrim(COALESCE(v_old_row->>'contract_id', '')), '')::uuid
                   LIMIT 1
                 ),
                 (
                   SELECT ct.candidate_id
                   FROM public.contracts ct
                   WHERE ct.id = nullif(btrim(COALESCE(v_new_row->>'contract_id', '')), '')::uuid
                   LIMIT 1
                 )
               ]::uuid[],
               ARRAY[
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials tf
                   WHERE tf.timesheet_id = nullif(btrim(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 ),
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials tf
                   WHERE tf.timesheet_id = nullif(btrim(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 )
               ]::uuid[]
             ),
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;

  ELSIF v_trigger_table = 'timesheet_pay_state' THEN
    SELECT array_cat(
             array_cat(
               ARRAY[
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials tf
                   WHERE tf.timesheet_id = nullif(btrim(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 ),
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials tf
                   WHERE tf.timesheet_id = nullif(btrim(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 )
               ]::uuid[],
               ARRAY[
                 (
                   SELECT ct.candidate_id
                   FROM public.timesheets ts
                   JOIN public.contracts ct
                     ON ct.id = ts.contract_id
                   WHERE ts.timesheet_id = nullif(btrim(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid
                   LIMIT 1
                 ),
                 (
                   SELECT ct.candidate_id
                   FROM public.timesheets ts
                   JOIN public.contracts ct
                     ON ct.id = ts.contract_id
                   WHERE ts.timesheet_id = nullif(btrim(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid
                   LIMIT 1
                 )
               ]::uuid[]
             ),
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;

  ELSIF v_trigger_table = 'timesheet_payment_overrides' THEN
    SELECT array_cat(
             array_cat(
               ARRAY[
                 nullif(btrim(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
                 nullif(btrim(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
               ]::uuid[],
               ARRAY[
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials tf
                   WHERE tf.timesheet_id = nullif(btrim(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 ),
                 (
                   SELECT tf.candidate_id
                   FROM public.timesheets_financials tf
                   WHERE tf.timesheet_id = nullif(btrim(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid
                     AND tf.is_current = true
                   ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                   LIMIT 1
                 )
               ]::uuid[]
             ),
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;

  ELSIF v_trigger_table = 'pay_item_snoozes' THEN
    SELECT array_cat(
             array_cat(
               array_cat(
                 ARRAY[
                   nullif(btrim(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
                   nullif(btrim(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
                 ]::uuid[],
                 ARRAY[
                   (
                     SELECT tf.candidate_id
                     FROM public.timesheets_financials tf
                     WHERE tf.timesheet_id = nullif(btrim(COALESCE(v_old_row->>'timesheet_id', '')), '')::uuid
                       AND tf.is_current = true
                     ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                     LIMIT 1
                   ),
                   (
                     SELECT tf.candidate_id
                     FROM public.timesheets_financials tf
                     WHERE tf.timesheet_id = nullif(btrim(COALESCE(v_new_row->>'timesheet_id', '')), '')::uuid
                       AND tf.is_current = true
                     ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.id DESC
                     LIMIT 1
                   )
                 ]::uuid[]
               ),
               ARRAY[
                 (
                   SELECT COALESCE(tf.candidate_id, ct.candidate_id)
                   FROM public.timesheets ts
                   LEFT JOIN public.timesheets_financials tf
                     ON tf.timesheet_id = ts.timesheet_id
                    AND tf.is_current = true
                   LEFT JOIN public.contracts ct
                     ON ct.id = ts.contract_id
                   WHERE ts.booking_id = nullif(btrim(COALESCE(v_old_row->>'booking_id', '')), '')::uuid
                   ORDER BY ts.updated_at DESC NULLS LAST, ts.timesheet_id DESC
                   LIMIT 1
                 ),
                 (
                   SELECT COALESCE(tf.candidate_id, ct.candidate_id)
                   FROM public.timesheets ts
                   LEFT JOIN public.timesheets_financials tf
                     ON tf.timesheet_id = ts.timesheet_id
                    AND tf.is_current = true
                   LEFT JOIN public.contracts ct
                     ON ct.id = ts.contract_id
                   WHERE ts.booking_id = nullif(btrim(COALESCE(v_new_row->>'booking_id', '')), '')::uuid
                   ORDER BY ts.updated_at DESC NULLS LAST, ts.timesheet_id DESC
                   LIMIT 1
                 )
               ]::uuid[]
             ),
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;

  ELSIF v_trigger_table = 'ts_pay_adjustments' THEN
    v_candidate_ids := array_cat(
      v_candidate_ids,
      ARRAY[
        nullif(btrim(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
        nullif(btrim(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
      ]
    );

  ELSIF v_trigger_table = 'pay_advances' THEN
    v_candidate_ids := array_cat(
      v_candidate_ids,
      ARRAY[
        nullif(btrim(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
        nullif(btrim(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid
      ]
    );

  ELSIF v_trigger_table IN ('bank_name_checks', 'bank_payee_map') THEN
    SELECT array_cat(
             CASE
               WHEN upper(COALESCE(v_old_row->>'entity_kind', '')) = 'CANDIDATE'
                 THEN ARRAY[nullif(btrim(COALESCE(v_old_row->>'entity_id', '')), '')::uuid]
               WHEN upper(COALESCE(v_old_row->>'entity_kind', '')) = 'UMBRELLA'
                 THEN COALESCE(
                   (
                     SELECT array_agg(DISTINCT c.id)
                     FROM public.candidates c
                     WHERE c.umbrella_id = nullif(btrim(COALESCE(v_old_row->>'entity_id', '')), '')::uuid
                   ),
                   ARRAY[]::uuid[]
                 )
               ELSE ARRAY[]::uuid[]
             END,
             CASE
               WHEN upper(COALESCE(v_new_row->>'entity_kind', '')) = 'CANDIDATE'
                 THEN ARRAY[nullif(btrim(COALESCE(v_new_row->>'entity_id', '')), '')::uuid]
               WHEN upper(COALESCE(v_new_row->>'entity_kind', '')) = 'UMBRELLA'
                 THEN COALESCE(
                   (
                     SELECT array_agg(DISTINCT c.id)
                     FROM public.candidates c
                     WHERE c.umbrella_id = nullif(btrim(COALESCE(v_new_row->>'entity_id', '')), '')::uuid
                   ),
                   ARRAY[]::uuid[]
                 )
               ELSE ARRAY[]::uuid[]
             END
           )
    INTO v_candidate_ids;

  END IF;

  SELECT COALESCE(array_agg(DISTINCT x.candidate_id), ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS x(candidate_id)
  WHERE x.candidate_id IS NOT NULL;

  IF COALESCE(array_length(v_candidate_ids, 1), 0) = 0 THEN
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    ELSE
      RETURN NEW;
    END IF;
  END IF;

  FOREACH v_candidate_id IN ARRAY v_candidate_ids
  LOOP
    PERFORM public._change_bump('pay_candidate:' || v_candidate_id::text);

    SELECT COALESCE(acc.seq, 0)
    INTO v_live_change_seq
    FROM public.app_change_counters acc
    WHERE acc.entity_key = 'pay_candidate:' || v_candidate_id::text;

    v_reason := 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP;
    v_payload_json := jsonb_build_object(
      'trigger_table', v_trigger_table,
      'trigger_op', TG_OP,
      'scope_kind', 'CANDIDATE',
      'scope_id', v_candidate_id::text,
      'candidate_id', v_candidate_id::text,
      'reason', v_reason,
      'source_change_seq', v_live_change_seq,
      'old_timesheet_id', nullif(btrim(COALESCE(v_old_row->>'timesheet_id', '')), ''),
      'new_timesheet_id', nullif(btrim(COALESCE(v_new_row->>'timesheet_id', '')), ''),
      'old_contract_id', nullif(btrim(COALESCE(v_old_row->>'contract_id', '')), ''),
      'new_contract_id', nullif(btrim(COALESCE(v_new_row->>'contract_id', '')), ''),
      'old_entity_kind', nullif(btrim(COALESCE(v_old_row->>'entity_kind', '')), ''),
      'new_entity_kind', nullif(btrim(COALESCE(v_new_row->>'entity_kind', '')), ''),
      'old_entity_id', nullif(btrim(COALESCE(v_old_row->>'entity_id', '')), ''),
      'new_entity_id', nullif(btrim(COALESCE(v_new_row->>'entity_id', '')), '')
    );
    v_dedupe_key := 'CONTRACT_CLIENT_DIRTY_FANOUT:CANDIDATE:' || v_candidate_id::text;

    INSERT INTO public.banking_pay_workbench_jobs (
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
      'CONTRACT_CLIENT_DIRTY_FANOUT',
      'QUEUED',
      200,
      v_now,
      0,
      8,
      v_dedupe_key,
      NULL,
      NULL,
      v_candidate_id,
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
    SET priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
        run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
        candidate_id = COALESCE(public.banking_pay_workbench_jobs.candidate_id, EXCLUDED.candidate_id),
        payload_json = COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || COALESCE(EXCLUDED.payload_json, '{}'::jsonb),
        updated_at_utc = v_now;

    UPDATE public.banking_pay_workbench_session_candidate_state scs
    SET status = 'PENDING',
        source_change_seq = GREATEST(COALESCE(scs.source_change_seq, 0), v_live_change_seq),
        updated_at_utc = v_now,
        last_error_json = NULL
    FROM public.banking_pay_workbench_sessions ws
    WHERE ws.id = scs.session_id
      AND ws.status = 'OPEN'
      AND scs.candidate_id = v_candidate_id
      AND ws.scope_candidate_ids @> ARRAY[v_candidate_id]::uuid[];
  END LOOP;

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_mark_finance_case_dirty()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_trigger_table text := lower(TG_TABLE_NAME);
  v_new_row jsonb := '{}'::jsonb;
  v_old_row jsonb := '{}'::jsonb;
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_id uuid;
  v_live_change_seq bigint := 0;
  v_reason text;
  v_payload_json jsonb;
  v_dedupe_key text;
BEGIN
  IF TG_OP <> 'DELETE' THEN
    v_new_row := to_jsonb(NEW);
  END IF;

  IF TG_OP <> 'INSERT' THEN
    v_old_row := to_jsonb(OLD);
  END IF;

  IF v_trigger_table = 'pay_finance_case_components' THEN
    SELECT array_cat(
             ARRAY[
               nullif(btrim(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid,
               nullif(btrim(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid,
               (
                 SELECT pa.candidate_id
                 FROM public.pay_advances pa
                 WHERE pa.id = nullif(btrim(COALESCE(v_old_row->>'finance_case_id', '')), '')::uuid
                 LIMIT 1
               ),
               (
                 SELECT pa.candidate_id
                 FROM public.pay_advances pa
                 WHERE pa.id = nullif(btrim(COALESCE(v_new_row->>'finance_case_id', '')), '')::uuid
                 LIMIT 1
               )
             ]::uuid[],
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;

  ELSIF v_trigger_table = 'pay_finance_case_events' THEN
    SELECT array_cat(
             ARRAY[
               (
                 SELECT pa.candidate_id
                 FROM public.pay_advances pa
                 WHERE pa.id = nullif(btrim(COALESCE(v_old_row->>'finance_case_id', '')), '')::uuid
                 LIMIT 1
               ),
               (
                 SELECT pa.candidate_id
                 FROM public.pay_advances pa
                 WHERE pa.id = nullif(btrim(COALESCE(v_new_row->>'finance_case_id', '')), '')::uuid
                 LIMIT 1
               )
             ]::uuid[],
             ARRAY[]::uuid[]
           )
    INTO v_candidate_ids;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT x.candidate_id), ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM unnest(COALESCE(v_candidate_ids, ARRAY[]::uuid[])) AS x(candidate_id)
  WHERE x.candidate_id IS NOT NULL;

  IF COALESCE(array_length(v_candidate_ids, 1), 0) = 0 THEN
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    ELSE
      RETURN NEW;
    END IF;
  END IF;

  FOREACH v_candidate_id IN ARRAY v_candidate_ids
  LOOP
    PERFORM public._change_bump('pay_candidate:' || v_candidate_id::text);

    SELECT COALESCE(acc.seq, 0)
    INTO v_live_change_seq
    FROM public.app_change_counters acc
    WHERE acc.entity_key = 'pay_candidate:' || v_candidate_id::text;

    v_reason := 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP;
    v_payload_json := jsonb_build_object(
      'trigger_table', v_trigger_table,
      'trigger_op', TG_OP,
      'scope_kind', 'CANDIDATE',
      'scope_id', v_candidate_id::text,
      'candidate_id', v_candidate_id::text,
      'reason', v_reason,
      'source_change_seq', v_live_change_seq,
      'old_finance_case_id', nullif(btrim(COALESCE(v_old_row->>'finance_case_id', '')), ''),
      'new_finance_case_id', nullif(btrim(COALESCE(v_new_row->>'finance_case_id', '')), ''),
      'old_row_id', nullif(btrim(COALESCE(v_old_row->>'id', '')), ''),
      'new_row_id', nullif(btrim(COALESCE(v_new_row->>'id', '')), '')
    );
    v_dedupe_key := 'CONTRACT_CLIENT_DIRTY_FANOUT:CANDIDATE:' || v_candidate_id::text;

    INSERT INTO public.banking_pay_workbench_jobs (
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
      'CONTRACT_CLIENT_DIRTY_FANOUT',
      'QUEUED',
      200,
      v_now,
      0,
      8,
      v_dedupe_key,
      NULL,
      NULL,
      v_candidate_id,
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
    SET priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
        run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
        candidate_id = COALESCE(public.banking_pay_workbench_jobs.candidate_id, EXCLUDED.candidate_id),
        payload_json = COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || COALESCE(EXCLUDED.payload_json, '{}'::jsonb),
        updated_at_utc = v_now;

    UPDATE public.banking_pay_workbench_session_candidate_state scs
    SET status = 'PENDING',
        source_change_seq = GREATEST(COALESCE(scs.source_change_seq, 0), v_live_change_seq),
        updated_at_utc = v_now,
        last_error_json = NULL
    FROM public.banking_pay_workbench_sessions ws
    WHERE ws.id = scs.session_id
      AND ws.status = 'OPEN'
      AND scs.candidate_id = v_candidate_id
      AND ws.scope_candidate_ids @> ARRAY[v_candidate_id]::uuid[];
  END LOOP;

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_candidate_refresh(
  p_snapshot_run_id uuid,
  p_candidate_id uuid,
  p_reason text DEFAULT NULL::text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_payload_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_live_change_seq bigint := 0;
  v_dedupe_key text;
  v_job_id uuid;
  v_job_status text;
  v_job_was_inserted boolean := false;
  v_existing_state_status text := NULL;
  v_existing_state_source_change_seq bigint := 0;
  v_existing_job_id uuid := NULL;
  v_existing_job_status text := NULL;
  v_existing_job_source_change_seq bigint := 0;
  v_existing_job_payload jsonb := '{}'::jsonb;
  v_force_refresh boolean := false;
  v_reuse_source_change_seq bigint := 0;
BEGIN
  IF p_snapshot_run_id IS NULL THEN
    RAISE EXCEPTION 'snapshot_run_id is required';
  END IF;

  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  PERFORM 1
  FROM public.banking_pay_snapshot_runs sr
  WHERE sr.id = p_snapshot_run_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_snapshot_runs row % not found', p_snapshot_run_id;
  END IF;

  PERFORM 1
  FROM public.candidates c
  WHERE c.id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'candidates row % not found', p_candidate_id;
  END IF;

  v_force_refresh := (
    CASE
      WHEN COALESCE(p_payload_json, '{}'::jsonb) ? 'force_refresh' THEN
        CASE jsonb_typeof(COALESCE(p_payload_json, '{}'::jsonb)->'force_refresh')
          WHEN 'boolean' THEN COALESCE((COALESCE(p_payload_json, '{}'::jsonb)->>'force_refresh')::boolean, false)
          WHEN 'string' THEN lower(btrim(COALESCE(COALESCE(p_payload_json, '{}'::jsonb)->>'force_refresh', ''))) IN ('true', 't', '1', 'yes', 'y', 'force')
          WHEN 'number' THEN COALESCE((COALESCE(p_payload_json, '{}'::jsonb)->>'force_refresh')::numeric, 0) <> 0
          ELSE false
        END
      ELSE false
    END
  ) OR upper(COALESCE(btrim(p_reason), '')) IN ('FORCE_REFRESH', 'FORCED_REFRESH', 'FORCE');

  SELECT COALESCE(acc.seq, 0)
  INTO v_live_change_seq
  FROM public.app_change_counters acc
  WHERE acc.entity_key = 'pay_candidate:' || p_candidate_id::text;

  SELECT COALESCE(scs.status, NULL), COALESCE(scs.source_change_seq, 0)
  INTO v_existing_state_status, v_existing_state_source_change_seq
  FROM public.banking_pay_snapshot_candidate_state scs
  WHERE scs.snapshot_run_id = p_snapshot_run_id
    AND scs.candidate_id = p_candidate_id
  LIMIT 1;

  SELECT j.id,
         j.status,
         COALESCE(
           CASE
             WHEN btrim(COALESCE(j.payload_json->>'source_change_seq', '')) ~ '^[0-9]+$'
               THEN (j.payload_json->>'source_change_seq')::bigint
             ELSE 0
           END,
           0
         ),
         COALESCE(j.payload_json, '{}'::jsonb)
  INTO v_existing_job_id,
       v_existing_job_status,
       v_existing_job_source_change_seq,
       v_existing_job_payload
  FROM public.banking_pay_workbench_jobs j
  WHERE j.job_type = 'SNAPSHOT_CANDIDATE_REFRESH'
    AND j.snapshot_run_id = p_snapshot_run_id
    AND j.candidate_id = p_candidate_id
    AND j.status IN ('QUEUED', 'RUNNING')
  ORDER BY COALESCE(
             CASE
               WHEN btrim(COALESCE(j.payload_json->>'source_change_seq', '')) ~ '^[0-9]+$'
                 THEN (j.payload_json->>'source_change_seq')::bigint
               ELSE 0
             END,
             0
           ) DESC,
           j.updated_at_utc DESC NULLS LAST,
           j.created_at_utc DESC NULLS LAST,
           j.id DESC
  LIMIT 1;

  IF NOT v_force_refresh
     AND upper(COALESCE(v_existing_state_status, '')) = 'READY'
     AND v_existing_state_source_change_seq >= v_live_change_seq THEN
    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      NULL,
      'NO_OP',
      NULL,
      jsonb_build_object(
        'job_type', 'SNAPSHOT_CANDIDATE_REFRESH',
        'snapshot_run_id', p_snapshot_run_id::text,
        'candidate_id', p_candidate_id::text,
        'source_change_seq', v_live_change_seq,
        'existing_state_status', v_existing_state_status,
        'existing_state_source_change_seq', v_existing_state_source_change_seq,
        'reason', p_reason,
        'force_refresh', v_force_refresh
      ),
      'WORKBENCH_JOB_ENQUEUE',
      p_actor_user_id
    );

    RETURN jsonb_build_object(
      'ok', true,
      'job_id', NULL,
      'job_type', 'SNAPSHOT_CANDIDATE_REFRESH',
      'snapshot_run_id', p_snapshot_run_id::text,
      'candidate_id', p_candidate_id::text,
      'source_change_seq', v_live_change_seq,
      'dedupe_key', NULL,
      'reason', p_reason,
      'reused', false,
      'no_op', true,
      'state_status', v_existing_state_status,
      'state_source_change_seq', v_existing_state_source_change_seq,
      'force_refresh', v_force_refresh
    );
  END IF;

  v_reuse_source_change_seq := GREATEST(v_live_change_seq, v_existing_state_source_change_seq, v_existing_job_source_change_seq);

  IF NOT v_force_refresh
     AND v_existing_job_id IS NOT NULL
     AND (
       v_existing_job_source_change_seq >= v_live_change_seq
       OR (
         upper(COALESCE(v_existing_state_status, '')) = 'PENDING'
         AND v_existing_state_source_change_seq >= v_live_change_seq
       )
     ) THEN
    INSERT INTO public.banking_pay_snapshot_candidate_state (
      snapshot_run_id,
      candidate_id,
      status,
      candidate_fragment_json,
      summary_fragment_json,
      paye_candidate_json,
      non_paye_payee_json,
      payees_json,
      case_resolution_states_json,
      canonical_preview_lines_json,
      source_change_seq,
      created_at_utc,
      updated_at_utc,
      last_refreshed_at_utc,
      last_error_json
    )
    VALUES (
      p_snapshot_run_id,
      p_candidate_id,
      'PENDING',
      '{}'::jsonb,
      '{}'::jsonb,
      NULL,
      NULL,
      '[]'::jsonb,
      '[]'::jsonb,
      '[]'::jsonb,
      v_reuse_source_change_seq,
      v_now,
      v_now,
      NULL,
      NULL
    )
    ON CONFLICT (snapshot_run_id, candidate_id)
    DO UPDATE
    SET status = 'PENDING',
        source_change_seq = GREATEST(public.banking_pay_snapshot_candidate_state.source_change_seq, EXCLUDED.source_change_seq),
        updated_at_utc = v_now,
        last_error_json = NULL;

    UPDATE public.banking_pay_workbench_session_candidate_state scs
    SET status = 'PENDING',
        source_change_seq = GREATEST(COALESCE(scs.source_change_seq, 0), v_reuse_source_change_seq),
        pending_job_id = v_existing_job_id,
        updated_at_utc = v_now,
        last_error_json = NULL
    FROM public.banking_pay_workbench_sessions ws
    WHERE ws.id = scs.session_id
      AND ws.status = 'OPEN'
      AND ws.source_snapshot_run_id = p_snapshot_run_id
      AND scs.candidate_id = p_candidate_id
      AND ws.scope_candidate_ids @> ARRAY[p_candidate_id]::uuid[];

    PERFORM public._audit_insert(
      'banking_pay_workbench_job',
      v_existing_job_id::text,
      'REUSED',
      NULL,
      jsonb_build_object(
        'id', v_existing_job_id::text,
        'job_type', 'SNAPSHOT_CANDIDATE_REFRESH',
        'status', v_existing_job_status,
        'snapshot_run_id', p_snapshot_run_id::text,
        'candidate_id', p_candidate_id::text,
        'source_change_seq', v_live_change_seq,
        'existing_state_status', v_existing_state_status,
        'existing_state_source_change_seq', v_existing_state_source_change_seq,
        'existing_job_source_change_seq', v_existing_job_source_change_seq,
        'reason', p_reason,
        'force_refresh', v_force_refresh
      ),
      'WORKBENCH_JOB_ENQUEUE',
      p_actor_user_id
    );

    RETURN jsonb_build_object(
      'ok', true,
      'job_id', v_existing_job_id::text,
      'job_type', 'SNAPSHOT_CANDIDATE_REFRESH',
      'snapshot_run_id', p_snapshot_run_id::text,
      'candidate_id', p_candidate_id::text,
      'source_change_seq', v_live_change_seq,
      'dedupe_key', NULL,
      'reason', p_reason,
      'reused', true,
      'no_op', false,
      'status', v_existing_job_status,
      'existing_state_status', v_existing_state_status,
      'existing_state_source_change_seq', v_existing_state_source_change_seq,
      'existing_job_source_change_seq', v_existing_job_source_change_seq,
      'payload_json', v_existing_job_payload,
      'force_refresh', v_force_refresh
    );
  END IF;

  v_dedupe_key := 'SNAPSHOT_CANDIDATE_REFRESH:' || p_snapshot_run_id::text || ':' || p_candidate_id::text || ':s' || v_live_change_seq::text;

  INSERT INTO public.banking_pay_workbench_jobs (
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
    'SNAPSHOT_CANDIDATE_REFRESH',
    'QUEUED',
    50,
    v_now,
    0,
    8,
    v_dedupe_key,
    p_snapshot_run_id,
    NULL,
    p_candidate_id,
    COALESCE(p_payload_json, '{}'::jsonb)
      || jsonb_build_object(
        'reason', p_reason,
        'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
        'snapshot_run_id', p_snapshot_run_id::text,
        'candidate_id', p_candidate_id::text,
        'source_change_seq', v_live_change_seq,
        'job_type', 'SNAPSHOT_CANDIDATE_REFRESH',
        'force_refresh', v_force_refresh
      ),
    v_now,
    v_now,
    NULL,
    NULL,
    NULL,
    NULL
  )
  ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
  DO UPDATE
  SET priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
      run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
      payload_json = COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || COALESCE(EXCLUDED.payload_json, '{}'::jsonb),
      updated_at_utc = v_now
  RETURNING public.banking_pay_workbench_jobs.id, public.banking_pay_workbench_jobs.status, (xmax = 0)
  INTO v_job_id, v_job_status, v_job_was_inserted;

  INSERT INTO public.banking_pay_snapshot_candidate_state (
    snapshot_run_id,
    candidate_id,
    status,
    candidate_fragment_json,
    summary_fragment_json,
    paye_candidate_json,
    non_paye_payee_json,
    payees_json,
    case_resolution_states_json,
    canonical_preview_lines_json,
    source_change_seq,
    created_at_utc,
    updated_at_utc,
    last_refreshed_at_utc,
    last_error_json
  )
  VALUES (
    p_snapshot_run_id,
    p_candidate_id,
    'PENDING',
    '{}'::jsonb,
    '{}'::jsonb,
    NULL,
    NULL,
    '[]'::jsonb,
    '[]'::jsonb,
    '[]'::jsonb,
    v_live_change_seq,
    v_now,
    v_now,
    NULL,
    NULL
  )
  ON CONFLICT (snapshot_run_id, candidate_id)
  DO UPDATE
  SET status = 'PENDING',
      source_change_seq = GREATEST(public.banking_pay_snapshot_candidate_state.source_change_seq, EXCLUDED.source_change_seq),
      updated_at_utc = v_now,
      last_error_json = NULL;

  UPDATE public.banking_pay_workbench_session_candidate_state scs
  SET status = 'PENDING',
      source_change_seq = GREATEST(COALESCE(scs.source_change_seq, 0), v_live_change_seq),
      pending_job_id = v_job_id,
      updated_at_utc = v_now,
      last_error_json = NULL
  FROM public.banking_pay_workbench_sessions ws
  WHERE ws.id = scs.session_id
    AND ws.status = 'OPEN'
    AND ws.source_snapshot_run_id = p_snapshot_run_id
    AND scs.candidate_id = p_candidate_id
    AND ws.scope_candidate_ids @> ARRAY[p_candidate_id]::uuid[];

  PERFORM public._audit_insert(
    'banking_pay_workbench_job',
    v_job_id::text,
    CASE WHEN v_job_was_inserted THEN 'QUEUED' ELSE 'REUSED' END,
    NULL,
    jsonb_build_object(
      'id', v_job_id::text,
      'job_type', 'SNAPSHOT_CANDIDATE_REFRESH',
      'status', v_job_status,
      'snapshot_run_id', p_snapshot_run_id::text,
      'candidate_id', p_candidate_id::text,
      'dedupe_key', v_dedupe_key,
      'source_change_seq', v_live_change_seq,
      'force_refresh', v_force_refresh
    ),
    'WORKBENCH_JOB_ENQUEUE',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'job_id', v_job_id::text,
    'job_type', 'SNAPSHOT_CANDIDATE_REFRESH',
    'snapshot_run_id', p_snapshot_run_id::text,
    'candidate_id', p_candidate_id::text,
    'source_change_seq', v_live_change_seq,
    'dedupe_key', v_dedupe_key,
    'reason', p_reason,
    'reused', (NOT v_job_was_inserted),
    'no_op', false,
    'force_refresh', v_force_refresh
  );
END;
$function$;

BEGIN;
DROP TRIGGER IF EXISTS trg_pay_workbench_mark_candidate_dirty__timesheet_pay_state ON public.timesheet_pay_state;
COMMIT;


