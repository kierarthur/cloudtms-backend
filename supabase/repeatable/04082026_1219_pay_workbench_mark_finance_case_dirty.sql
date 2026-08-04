-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.pay_workbench_mark_finance_case_dirty()
-- Installed pg_get_functiondef MD5: 2d76c236024895498470a05da0532a58
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.pay_workbench_mark_finance_case_dirty()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_started_at timestamptz := clock_timestamp();
  v_now timestamptz := clock_timestamp();
  v_trigger_table text := lower(TG_TABLE_NAME);
  v_new_row jsonb := '{}'::jsonb;
  v_old_row jsonb := '{}'::jsonb;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  v_old_finance_case_id uuid := NULL::uuid;
  v_new_finance_case_id uuid := NULL::uuid;
  v_finance_case_ids uuid[] := ARRAY[]::uuid[];
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_finance_case_id uuid;
  v_candidate_id uuid;
  v_scope_id text;
  v_reason text;
  v_payload_json jsonb := '{}'::jsonb;
  v_enqueue_result jsonb := '{}'::jsonb;
  v_jobs_queued integer := 0;
  v_internal_build_token uuid := NULL::uuid;
  v_internal_candidate_id uuid := NULL::uuid;
  v_internal_source_id uuid := NULL::uuid;
  v_internal_timesheet_id uuid := NULL::uuid;
  v_internal_finance_case_id uuid := NULL::uuid;
  v_internal_finance_component_id uuid := NULL::uuid;
  v_internal_before_digest text := NULL::text;
  v_internal_after_digest text := NULL::text;
  v_expected_match_count integer := 0;
  v_internal_economic_key_type text := NULL::text;
  v_internal_economic_key_value text := NULL::text;
  v_effect_capture_mode boolean := lower(COALESCE(current_setting('cloudtms.pay_workbench_effect_capture_mode',true),''))='capture';
BEGIN
  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', NULL::text, jsonb_build_object('function_name', 'pay_workbench_mark_finance_case_dirty', 'stage', 'entry', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'queue_class', 'DIRTY_TRIGGER_PRIORITY'));

  IF TG_OP <> 'DELETE' THEN v_new_row := to_jsonb(NEW); END IF;
  IF TG_OP <> 'INSERT' THEN v_old_row := to_jsonb(OLD); END IF;

  -- These three retained Workbench triggers are recreated as BEFORE ROW
  -- triggers by the V1.2.4 trigger artifact.  The exact transition identity and
  -- full before/after digests are therefore declared before the finance DML is
  -- applied.  The current build token and durable RPC-2 context are required;
  -- an effect outside the sealed build scope fails the reconciliation.
  IF to_regclass('pg_temp._bpay_wb_sync_context_v1') IS NOT NULL
     AND to_regclass('pg_temp._bpay_wb_expected_effects') IS NOT NULL
     AND COALESCE(current_setting('cloudtms.pay_workbench_overpayment_sync_token',true),'')
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_internal_build_token:=current_setting('cloudtms.pay_workbench_overpayment_sync_token',true)::uuid;
    SELECT build_row.candidate_id INTO v_internal_candidate_id
    FROM private.banking_pay_workbench_economic_builds build_row
    JOIN pg_temp._bpay_wb_sync_context_v1 sync_context
      ON sync_context.build_id=build_row.id AND sync_context.build_token=build_row.build_token
    JOIN private.banking_pay_workbench_stage_attempts attempt
      ON attempt.id=sync_context.attempt_id AND attempt.attempt_nonce=sync_context.attempt_nonce
      AND attempt.build_id=build_row.id AND attempt.attempt_status='STARTED'
    WHERE build_row.build_token=v_internal_build_token
      AND build_row.status='RECONCILING' AND build_row.private_stage='RECONCILE_EXECUTE';
    IF v_internal_candidate_id IS NOT NULL THEN
      IF TG_WHEN<>'BEFORE' THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_CONFLICT' USING ERRCODE='23514';
      END IF;
      v_internal_source_id:=COALESCE(NULLIF(v_new_row->>'id','')::uuid,
        NULLIF(v_old_row->>'id','')::uuid,NULLIF(v_new_row->>'finance_case_id','')::uuid,
        NULLIF(v_old_row->>'finance_case_id','')::uuid);
      v_internal_timesheet_id:=COALESCE(NULLIF(v_new_row->>'linked_timesheet_id','')::uuid,
        NULLIF(v_old_row->>'linked_timesheet_id','')::uuid,NULLIF(v_new_row->>'timesheet_id','')::uuid,
        NULLIF(v_old_row->>'timesheet_id','')::uuid);
      IF v_internal_source_id IS NULL THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_CONFLICT' USING ERRCODE='23514';
      END IF;
      v_internal_finance_case_id:=COALESCE(NULLIF(v_new_row->>'finance_case_id','')::uuid,
        NULLIF(v_old_row->>'finance_case_id','')::uuid,
        CASE WHEN v_trigger_table='pay_advances' THEN v_internal_source_id END);
      v_internal_finance_component_id:=COALESCE(
        NULLIF(v_new_row->>'finance_component_id','')::uuid,
        NULLIF(v_old_row->>'finance_component_id','')::uuid,
        CASE WHEN v_trigger_table='pay_finance_case_components' THEN v_internal_source_id END);
      v_internal_economic_key_type:=COALESCE(NULLIF(btrim(v_new_row->>'component_key_type'),''),
        NULLIF(btrim(v_old_row->>'component_key_type'),''));
      v_internal_economic_key_value:=COALESCE(NULLIF(btrim(v_new_row->>'component_key_value'),''),
        NULLIF(btrim(v_old_row->>'component_key_value'),''));
      v_internal_before_digest:=CASE WHEN TG_OP='INSERT' THEN NULL ELSE md5((
        v_old_row-ARRAY['created_at','created_at_utc','updated_at','updated_at_utc',
          'event_at_utc']::text[])::text) END;
      v_internal_after_digest:=CASE
        WHEN TG_OP='DELETE' THEN NULL
        WHEN TG_OP='INSERT' THEN md5((v_new_row-ARRAY['id','finance_case_id',
          'finance_component_id','created_at','created_at_utc','updated_at',
          'updated_at_utc','event_at_utc']::text[])::text)
        ELSE md5((v_new_row-ARRAY['created_at','created_at_utc','updated_at',
          'updated_at_utc','event_at_utc']::text[])::text)
      END;

      IF COALESCE(NULLIF(v_new_row->>'candidate_id','')::uuid,
           NULLIF(v_old_row->>'candidate_id','')::uuid,
           (SELECT finance_case.candidate_id FROM public.pay_advances finance_case
             WHERE finance_case.id=v_internal_finance_case_id))
           IS DISTINCT FROM v_internal_candidate_id
         OR (v_internal_timesheet_id IS NOT NULL AND NOT EXISTS(
           SELECT 1 FROM private.banking_pay_workbench_economic_build_scope scope_row
           JOIN pg_temp._bpay_wb_sync_context_v1 sync_context
             ON sync_context.build_id=scope_row.build_id
           WHERE scope_row.timesheet_id=v_internal_timesheet_id
         ))
         OR (v_trigger_table='pay_finance_case_components'
           AND v_internal_economic_key_type IS NOT NULL
           AND NOT EXISTS(
             SELECT 1 FROM private.banking_pay_workbench_economic_build_facts fact
             JOIN pg_temp._bpay_wb_sync_context_v1 sync_context
               ON sync_context.build_id=fact.build_id
             WHERE fact.economic_key_type=v_internal_economic_key_type
               AND fact.economic_key_value=v_internal_economic_key_value
           )
           AND NOT EXISTS(
             SELECT 1 FROM private.banking_pay_workbench_economic_build_facts fact
             JOIN pg_temp._bpay_wb_sync_context_v1 sync_context
               ON sync_context.build_id=fact.build_id
             WHERE fact.finance_component_id=v_internal_finance_component_id
           )) THEN
        RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH' USING ERRCODE='23514';
      END IF;
      IF v_effect_capture_mode THEN
        INSERT INTO pg_temp._bpay_wb_expected_effects(
          build_token,candidate_id,timesheet_id,relation_name,operation,source_id,
          finance_case_id,finance_component_id,economic_key_type,economic_key_value,
          expected_before_digest,expected_after_digest,proposed,observed
        ) VALUES(
          v_internal_build_token,v_internal_candidate_id,v_internal_timesheet_id,v_trigger_table,TG_OP,
          v_internal_source_id,v_internal_finance_case_id,v_internal_finance_component_id,
          v_internal_economic_key_type,v_internal_economic_key_value,
          v_internal_before_digest,v_internal_after_digest,true,false
        );
      ELSIF TG_OP='INSERT' THEN
        UPDATE pg_temp._bpay_wb_expected_effects expected
        SET source_id=v_internal_source_id,finance_case_id=v_internal_finance_case_id,
            finance_component_id=v_internal_finance_component_id,proposed=true
        WHERE expected.ctid=(SELECT candidate.ctid
          FROM pg_temp._bpay_wb_expected_effects candidate
          WHERE candidate.build_token=v_internal_build_token
            AND candidate.candidate_id=v_internal_candidate_id
            AND candidate.timesheet_id IS NOT DISTINCT FROM v_internal_timesheet_id
            AND candidate.relation_name=v_trigger_table AND candidate.operation=TG_OP
            AND candidate.proposed IS NOT TRUE AND candidate.observed IS NOT TRUE
            AND candidate.economic_key_type IS NOT DISTINCT FROM v_internal_economic_key_type
            AND candidate.economic_key_value IS NOT DISTINCT FROM v_internal_economic_key_value
            AND candidate.expected_before_digest IS NULL
            AND candidate.expected_after_digest IS NOT DISTINCT FROM v_internal_after_digest
          ORDER BY candidate.source_id LIMIT 1);
        GET DIAGNOSTICS v_expected_match_count=ROW_COUNT;
        IF v_expected_match_count<>1 THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH' USING ERRCODE='23514';
        END IF;
      ELSE
        UPDATE pg_temp._bpay_wb_expected_effects expected SET proposed=true
        WHERE expected.build_token=v_internal_build_token
          AND expected.candidate_id=v_internal_candidate_id
          AND expected.timesheet_id IS NOT DISTINCT FROM v_internal_timesheet_id
          AND expected.relation_name=v_trigger_table AND expected.operation=TG_OP
          AND expected.source_id=v_internal_source_id
          AND expected.finance_case_id IS NOT DISTINCT FROM v_internal_finance_case_id
          AND expected.finance_component_id IS NOT DISTINCT FROM v_internal_finance_component_id
          AND expected.economic_key_type IS NOT DISTINCT FROM v_internal_economic_key_type
          AND expected.economic_key_value IS NOT DISTINCT FROM v_internal_economic_key_value
          AND expected.proposed IS NOT TRUE AND expected.observed IS NOT TRUE
          AND expected.expected_before_digest IS NOT DISTINCT FROM v_internal_before_digest
          AND expected.expected_after_digest IS NOT DISTINCT FROM v_internal_after_digest;
        GET DIAGNOSTICS v_expected_match_count=ROW_COUNT;
        IF v_expected_match_count<>1 THEN
          RAISE EXCEPTION 'PAY_WORKBENCH_EXPECTED_EFFECT_MISMATCH' USING ERRCODE='23514';
        END IF;
      END IF;
      IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END IF;
  END IF;

  -- A protected preview synchronisation deliberately records SYNC_SKIPPED as
  -- audit evidence.  That informational event must not dirty Banking Pay and
  -- enqueue the same protected synchronisation again.
  IF v_trigger_table = 'pay_finance_case_events'
     AND TG_OP <> 'DELETE'
     AND UPPER(BTRIM(COALESCE(v_new_row->>'event_type', ''))) = 'SYNC_SKIPPED' THEN
    RETURN NEW;
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_old_row->>'finance_case_id', v_old_row->>'id', '')), '') ~* v_uuid_re THEN
    v_old_finance_case_id := NULLIF(BTRIM(COALESCE(v_old_row->>'finance_case_id', v_old_row->>'id', '')), '')::uuid;
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_new_row->>'finance_case_id', v_new_row->>'id', '')), '') ~* v_uuid_re THEN
    v_new_finance_case_id := NULLIF(BTRIM(COALESCE(v_new_row->>'finance_case_id', v_new_row->>'id', '')), '')::uuid;
  END IF;

  SELECT COALESCE(array_agg(DISTINCT finance_cases.finance_case_id ORDER BY finance_cases.finance_case_id), ARRAY[]::uuid[])
  INTO v_finance_case_ids
  FROM (
    SELECT v_old_finance_case_id AS finance_case_id
    UNION ALL
    SELECT v_new_finance_case_id AS finance_case_id
  ) AS finance_cases
  WHERE finance_cases.finance_case_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT candidates.candidate_id ORDER BY candidates.candidate_id), ARRAY[]::uuid[])
  INTO v_candidate_ids
  FROM (
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_old_row->>'candidate_id', '')), '')::uuid ELSE NULL::uuid END AS candidate_id
    UNION ALL
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_new_row->>'candidate_id', '')), '')::uuid ELSE NULL::uuid END AS candidate_id
    UNION ALL
    SELECT component_old.candidate_id FROM public.pay_finance_case_components AS component_old WHERE component_old.finance_case_id = v_old_finance_case_id
    UNION ALL
    SELECT component_new.candidate_id FROM public.pay_finance_case_components AS component_new WHERE component_new.finance_case_id = v_new_finance_case_id
    UNION ALL
    SELECT advance_old.candidate_id FROM public.pay_advances AS advance_old WHERE advance_old.id = v_old_finance_case_id
    UNION ALL
    SELECT advance_new.candidate_id FROM public.pay_advances AS advance_new WHERE advance_new.id = v_new_finance_case_id
  ) AS candidates
  WHERE candidates.candidate_id IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT timesheets.timesheet_id ORDER BY timesheets.timesheet_id), ARRAY[]::uuid[])
  INTO v_targeted_timesheet_ids
  FROM (
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_old_row->>'linked_timesheet_id', v_old_row->>'timesheet_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_old_row->>'linked_timesheet_id', v_old_row->>'timesheet_id', '')), '')::uuid ELSE NULL::uuid END AS timesheet_id
    UNION ALL
    SELECT CASE WHEN NULLIF(BTRIM(COALESCE(v_new_row->>'linked_timesheet_id', v_new_row->>'timesheet_id', '')), '') ~* v_uuid_re THEN NULLIF(BTRIM(COALESCE(v_new_row->>'linked_timesheet_id', v_new_row->>'timesheet_id', '')), '')::uuid ELSE NULL::uuid END AS timesheet_id
  ) AS timesheets
  WHERE timesheets.timesheet_id IS NOT NULL;

  v_reason := 'DIRTY_TRIGGER:' || upper(v_trigger_table) || ':' || TG_OP;

  IF COALESCE(array_length(v_finance_case_ids, 1), 0) > 0 THEN
    FOREACH v_finance_case_id IN ARRAY v_finance_case_ids
    LOOP
      v_payload_json := jsonb_build_object(
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'trigger_operation', TG_OP,
        'scope_kind', 'FINANCE_CASE',
        'scope_id', v_finance_case_id::text,
        'finance_case_id', v_finance_case_id::text,
        'finance_case_ids', to_jsonb(v_finance_case_ids),
        'candidate_ids', to_jsonb(v_candidate_ids),
        'targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
        'reason', v_reason,
        'dirty_reason', v_reason,
        'refresh_scope_kind', CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0 THEN 'TARGETED_TIMESHEETS' ELSE 'CANDIDATE_FULL_LIVE' END,
        'force_legacy', true,
        'projection_class', 'FINANCE_CASE',
        'fallback_reason', 'FINANCE_CASE_DIRTY_TRIGGER',
        'source_build_required', true,
        'line_work_required', true,
        'row_backed_scope_required', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      );

      SELECT public.pay_workbench_dirty_event_enqueue(
        p_job_type => 'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
        p_scope_kind => 'FINANCE_CASE',
        p_scope_id => v_finance_case_id::text,
        p_candidate_id => NULL::uuid,
        p_targeted_timesheet_ids => v_targeted_timesheet_ids,
        p_linked_timesheet_ids => ARRAY[]::uuid[],
        p_payload_json => v_payload_json,
        p_reason => v_reason,
        p_priority => -1000,
        p_run_at_utc => v_now
      )
      INTO v_enqueue_result;
      v_jobs_queued := v_jobs_queued + 1;
    END LOOP;
  ELSE
    FOREACH v_candidate_id IN ARRAY v_candidate_ids
    LOOP
      v_scope_id := v_candidate_id::text;
      v_payload_json := jsonb_build_object(
        'trigger_table', v_trigger_table,
        'trigger_op', TG_OP,
        'trigger_operation', TG_OP,
        'scope_kind', 'CANDIDATE',
        'scope_id', v_scope_id,
        'candidate_id', v_scope_id,
        'candidate_ids', to_jsonb(ARRAY[v_candidate_id]),
        'targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
        'reason', v_reason,
        'dirty_reason', v_reason,
        'refresh_scope_kind', CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0 THEN 'TARGETED_TIMESHEETS' ELSE 'CANDIDATE_FULL_LIVE' END,
        'projection_class', 'FINANCE_CASE',
        'fallback_reason', 'FINANCE_CASE_DIRTY_TRIGGER_NO_CASE_ID',
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      );

      SELECT public.pay_workbench_dirty_event_enqueue(
        p_job_type => 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        p_scope_kind => 'CANDIDATE',
        p_scope_id => v_scope_id,
        p_candidate_id => v_candidate_id,
        p_targeted_timesheet_ids => v_targeted_timesheet_ids,
        p_linked_timesheet_ids => ARRAY[]::uuid[],
        p_payload_json => v_payload_json,
        p_reason => v_reason,
        p_priority => -1000,
        p_run_at_utc => v_now
      )
      INTO v_enqueue_result;
      v_jobs_queued := v_jobs_queued + 1;
    END LOOP;
  END IF;

  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', NULL::text, jsonb_build_object('function_name', 'pay_workbench_mark_finance_case_dirty', 'stage', 'return_enqueued_dirty_priority', 'trigger_table', v_trigger_table, 'trigger_op', TG_OP, 'finance_case_count', COALESCE(array_length(v_finance_case_ids, 1), 0), 'candidate_count', COALESCE(array_length(v_candidate_ids, 1), 0), 'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0), 'jobs_queued', v_jobs_queued, 'queue_class', 'DIRTY_TRIGGER_PRIORITY', 'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_started_at)) * 1000)::numeric, 2)));

  IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
$function$;

ALTER FUNCTION public.pay_workbench_mark_finance_case_dirty() OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_mark_finance_case_dirty() FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_mark_finance_case_dirty() TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_mark_finance_case_dirty() TO service_role;
