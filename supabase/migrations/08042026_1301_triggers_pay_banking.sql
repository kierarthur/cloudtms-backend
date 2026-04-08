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
  v_snapshot_run_id uuid;
  v_reason text;
  v_payload_json jsonb;
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
      'candidate_id', v_candidate_id::text,
      'old_timesheet_id', nullif(btrim(COALESCE(v_old_row->>'timesheet_id', '')), ''),
      'new_timesheet_id', nullif(btrim(COALESCE(v_new_row->>'timesheet_id', '')), ''),
      'old_contract_id', nullif(btrim(COALESCE(v_old_row->>'contract_id', '')), ''),
      'new_contract_id', nullif(btrim(COALESCE(v_new_row->>'contract_id', '')), ''),
      'old_entity_kind', nullif(btrim(COALESCE(v_old_row->>'entity_kind', '')), ''),
      'new_entity_kind', nullif(btrim(COALESCE(v_new_row->>'entity_kind', '')), ''),
      'old_entity_id', nullif(btrim(COALESCE(v_old_row->>'entity_id', '')), ''),
      'new_entity_id', nullif(btrim(COALESCE(v_new_row->>'entity_id', '')), '')
    );

    FOR v_snapshot_run_id IN
      SELECT sr.id
      FROM public.banking_pay_snapshot_runs sr
      WHERE sr.is_active = true
      ORDER BY sr.created_at_utc ASC, sr.id ASC
    LOOP
      PERFORM public.pay_workbench_enqueue_candidate_refresh(
        p_snapshot_run_id => v_snapshot_run_id,
        p_candidate_id => v_candidate_id,
        p_reason => v_reason,
        p_actor_user_id => NULL,
        p_payload_json => v_payload_json
      );
    END LOOP;

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
  v_snapshot_run_id uuid;
  v_reason text;
  v_payload_json jsonb;
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
      'candidate_id', v_candidate_id::text,
      'old_finance_case_id', nullif(btrim(COALESCE(v_old_row->>'finance_case_id', '')), ''),
      'new_finance_case_id', nullif(btrim(COALESCE(v_new_row->>'finance_case_id', '')), ''),
      'old_row_id', nullif(btrim(COALESCE(v_old_row->>'id', '')), ''),
      'new_row_id', nullif(btrim(COALESCE(v_new_row->>'id', '')), '')
    );

    FOR v_snapshot_run_id IN
      SELECT sr.id
      FROM public.banking_pay_snapshot_runs sr
      WHERE sr.is_active = true
      ORDER BY sr.created_at_utc ASC, sr.id ASC
    LOOP
      PERFORM public.pay_workbench_enqueue_candidate_refresh(
        p_snapshot_run_id => v_snapshot_run_id,
        p_candidate_id => v_candidate_id,
        p_reason => v_reason,
        p_actor_user_id => NULL,
        p_payload_json => v_payload_json
      );
    END LOOP;

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

CREATE OR REPLACE FUNCTION public.pay_workbench_mark_contract_client_dirty()
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
  v_contract_id_old uuid := NULL;
  v_contract_id_new uuid := NULL;
  v_client_id_old uuid := NULL;
  v_client_id_new uuid := NULL;
  v_candidate_id_old uuid := NULL;
  v_candidate_id_new uuid := NULL;
  v_row_id_old uuid := NULL;
  v_row_id_new uuid := NULL;
  v_scope record;
  v_dedupe_key text;
  v_payload_json jsonb;
BEGIN
  IF TG_OP <> 'DELETE' THEN
    v_new_row := to_jsonb(NEW);
  END IF;

  IF TG_OP <> 'INSERT' THEN
    v_old_row := to_jsonb(OLD);
  END IF;

  IF v_trigger_table = 'contracts' THEN
    v_contract_id_old := nullif(btrim(COALESCE(v_old_row->>'id', '')), '')::uuid;
    v_contract_id_new := nullif(btrim(COALESCE(v_new_row->>'id', '')), '')::uuid;
    v_client_id_old := nullif(btrim(COALESCE(v_old_row->>'client_id', '')), '')::uuid;
    v_client_id_new := nullif(btrim(COALESCE(v_new_row->>'client_id', '')), '')::uuid;
    v_candidate_id_old := nullif(btrim(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid;
    v_candidate_id_new := nullif(btrim(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid;
    v_row_id_old := v_contract_id_old;
    v_row_id_new := v_contract_id_new;
  ELSIF v_trigger_table = 'client_settings' THEN
    v_client_id_old := nullif(btrim(COALESCE(v_old_row->>'client_id', '')), '')::uuid;
    v_client_id_new := nullif(btrim(COALESCE(v_new_row->>'client_id', '')), '')::uuid;
    v_row_id_old := nullif(btrim(COALESCE(v_old_row->>'id', '')), '')::uuid;
    v_row_id_new := nullif(btrim(COALESCE(v_new_row->>'id', '')), '')::uuid;
  ELSE
    v_contract_id_old := nullif(btrim(COALESCE(v_old_row->>'contract_id', '')), '')::uuid;
    v_contract_id_new := nullif(btrim(COALESCE(v_new_row->>'contract_id', '')), '')::uuid;
    v_client_id_old := nullif(btrim(COALESCE(v_old_row->>'client_id', '')), '')::uuid;
    v_client_id_new := nullif(btrim(COALESCE(v_new_row->>'client_id', '')), '')::uuid;
    v_candidate_id_old := nullif(btrim(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid;
    v_candidate_id_new := nullif(btrim(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid;
    v_row_id_old := nullif(btrim(COALESCE(v_old_row->>'id', '')), '')::uuid;
    v_row_id_new := nullif(btrim(COALESCE(v_new_row->>'id', '')), '')::uuid;
  END IF;

  IF v_trigger_table = 'contracts' OR v_contract_id_old IS NOT NULL OR v_contract_id_new IS NOT NULL THEN
    FOR v_scope IN
      WITH scope_rows AS (
        SELECT
          'CONTRACT'::text AS scope_kind,
          v_contract_id_old AS scope_id,
          v_contract_id_old AS contract_id,
          v_client_id_old AS client_id,
          v_candidate_id_old AS candidate_id,
          v_row_id_old AS row_id
        UNION ALL
        SELECT
          'CONTRACT'::text AS scope_kind,
          v_contract_id_new AS scope_id,
          v_contract_id_new AS contract_id,
          v_client_id_new AS client_id,
          v_candidate_id_new AS candidate_id,
          v_row_id_new AS row_id
      )
      SELECT DISTINCT
        sr.scope_kind,
        sr.scope_id,
        sr.contract_id,
        sr.client_id,
        sr.candidate_id,
        sr.row_id
      FROM scope_rows sr
      WHERE sr.scope_id IS NOT NULL
      ORDER BY sr.scope_kind ASC, sr.scope_id ASC
    LOOP
      v_dedupe_key := 'CONTRACT_CLIENT_DIRTY_FANOUT:' || v_scope.scope_kind || ':' || v_scope.scope_id::text;
      v_payload_json := jsonb_build_object(
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'scope_kind', v_scope.scope_kind,
        'scope_id', v_scope.scope_id::text,
        'contract_id', CASE WHEN v_scope.contract_id IS NULL THEN NULL ELSE v_scope.contract_id::text END,
        'client_id', CASE WHEN v_scope.client_id IS NULL THEN NULL ELSE v_scope.client_id::text END,
        'candidate_id', CASE WHEN v_scope.candidate_id IS NULL THEN NULL ELSE v_scope.candidate_id::text END,
        'row_id', CASE WHEN v_scope.row_id IS NULL THEN NULL ELSE v_scope.row_id::text END,
        'reason', 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP
      );

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
        NULL,
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
          payload_json = COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || COALESCE(EXCLUDED.payload_json, '{}'::jsonb),
          updated_at_utc = v_now;
    END LOOP;

  ELSIF v_trigger_table = 'client_settings' OR v_client_id_old IS NOT NULL OR v_client_id_new IS NOT NULL THEN
    FOR v_scope IN
      WITH scope_rows AS (
        SELECT
          'CLIENT'::text AS scope_kind,
          v_client_id_old AS scope_id,
          NULL::uuid AS contract_id,
          v_client_id_old AS client_id,
          v_candidate_id_old AS candidate_id,
          v_row_id_old AS row_id
        UNION ALL
        SELECT
          'CLIENT'::text AS scope_kind,
          v_client_id_new AS scope_id,
          NULL::uuid AS contract_id,
          v_client_id_new AS client_id,
          v_candidate_id_new AS candidate_id,
          v_row_id_new AS row_id
      )
      SELECT DISTINCT
        sr.scope_kind,
        sr.scope_id,
        sr.contract_id,
        sr.client_id,
        sr.candidate_id,
        sr.row_id
      FROM scope_rows sr
      WHERE sr.scope_id IS NOT NULL
      ORDER BY sr.scope_kind ASC, sr.scope_id ASC
    LOOP
      v_dedupe_key := 'CONTRACT_CLIENT_DIRTY_FANOUT:' || v_scope.scope_kind || ':' || v_scope.scope_id::text;
      v_payload_json := jsonb_build_object(
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'scope_kind', v_scope.scope_kind,
        'scope_id', v_scope.scope_id::text,
        'contract_id', CASE WHEN v_scope.contract_id IS NULL THEN NULL ELSE v_scope.contract_id::text END,
        'client_id', CASE WHEN v_scope.client_id IS NULL THEN NULL ELSE v_scope.client_id::text END,
        'candidate_id', CASE WHEN v_scope.candidate_id IS NULL THEN NULL ELSE v_scope.candidate_id::text END,
        'row_id', CASE WHEN v_scope.row_id IS NULL THEN NULL ELSE v_scope.row_id::text END,
        'reason', 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP
      );

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
        NULL,
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
          payload_json = COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb) || COALESCE(EXCLUDED.payload_json, '{}'::jsonb),
          updated_at_utc = v_now;
    END LOOP;
  END IF;

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$function$;
