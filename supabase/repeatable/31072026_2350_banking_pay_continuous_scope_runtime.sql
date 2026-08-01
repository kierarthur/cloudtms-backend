-- Banking Pay continuous candidate-scope maintenance runtime.
-- Policy X boundary: everything in this file operates on pre-draft live scope
-- or records immutable generation provenance. It does not recalculate or
-- mutate frozen economic artefacts, payment execution, settlement or remittance.

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_current_generation_v1()
RETURNS bigint
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
  SELECT COALESCE((
    SELECT change_counter.seq
    FROM public.app_change_counters AS change_counter
    WHERE change_counter.entity_key = 'pay_candidate_scope_generation'
  ), 0)::bigint;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_change_tx_token_v1()
RETURNS uuid
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_token uuid;
  v_state text;
BEGIN
  IF LOWER(BTRIM(COALESCE(current_setting('cloudtms.scope_generation_finalising', true), 'false')))
       IN ('true', 't', '1', 'yes', 'y', 'on') THEN
    RETURN NULL::uuid;
  END IF;

  BEGIN
    v_token := NULLIF(BTRIM(COALESCE(
      current_setting('cloudtms.banking_pay_scope_tx_token', true),
      ''
    )), '')::uuid;
  EXCEPTION WHEN invalid_text_representation THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_TRANSACTION_TOKEN_INVALID'
      USING ERRCODE = 'P0001';
  END;

  IF v_token IS NULL THEN
    v_token := gen_random_uuid();
    PERFORM set_config('cloudtms.banking_pay_scope_tx_token', v_token::text, true);
    INSERT INTO public.banking_pay_scope_change_transactions(
      tx_token, state, created_at_utc
    ) VALUES (
      v_token, 'PENDING', clock_timestamp()
    );
    RETURN v_token;
  END IF;

  SELECT scope_tx.state
  INTO v_state
  FROM public.banking_pay_scope_change_transactions AS scope_tx
  WHERE scope_tx.tx_token = v_token;

  IF v_state IN ('FINALIZED', 'NOOP') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_GENERATION_FINALIZED_TOO_EARLY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_SCOPE_GENERATION_FINALIZED_TOO_EARLY',
              'state', v_state
            )::text;
  END IF;

  INSERT INTO public.banking_pay_scope_change_transactions(
    tx_token, state, created_at_utc
  ) VALUES (
    v_token, 'PENDING', clock_timestamp()
  ) ON CONFLICT (tx_token) DO NOTHING;

  RETURN v_token;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_counter_stage_trg_v1()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_inherited_generation bigint;
BEGIN
  IF NEW.entity_key !~* '^pay_candidate:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RETURN NEW;
  END IF;

  IF TG_OP = 'UPDATE' AND NEW.seq IS NOT DISTINCT FROM OLD.seq THEN
    RETURN NEW;
  END IF;

  IF LOWER(BTRIM(COALESCE(current_setting('cloudtms.scope_generation_finalising', true), 'false')))
       IN ('true', 't', '1', 'yes', 'y', 'on') THEN
    RETURN NEW;
  END IF;

  BEGIN
    v_inherited_generation := NULLIF(BTRIM(COALESCE(
      current_setting('cloudtms.banking_pay_inherited_scope_generation', true),
      ''
    )), '')::bigint;
  EXCEPTION WHEN invalid_text_representation THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_INHERITED_SCOPE_GENERATION_INVALID'
      USING ERRCODE = 'P0001';
  END;

  IF v_inherited_generation IS NOT NULL THEN
    IF TG_OP = 'UPDATE'
       AND v_inherited_generation <= COALESCE(OLD.scope_change_generation, 0) THEN
      -- A retried/equal or older broad-fanout child is already represented.
      -- Preserve the ordinary candidate source sequence as well as generation.
      NEW.seq := OLD.seq;
      NEW.updated_at := OLD.updated_at;
      NEW.scope_change_generation := OLD.scope_change_generation;
      NEW.scope_change_tx_token := NULL::uuid;
      RETURN NEW;
    END IF;

    NEW.scope_change_generation := GREATEST(
      COALESCE(NEW.scope_change_generation, 0),
      v_inherited_generation
    );
    NEW.scope_change_tx_token := NULL::uuid;
  ELSE
    NEW.scope_change_tx_token := public.pay_workbench_scope_change_tx_token_v1();
  END IF;

  RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_job_stage_trg_v1()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_job_type text := UPPER(BTRIM(COALESCE(NEW.job_type, '')));
  v_inherited_generation bigint;
  v_is_source_enqueue boolean := false;
BEGIN
  IF v_job_type = 'WORKBENCH_SCOPE_RECONCILE' THEN
    IF NEW.scope_change_tx_token IS NOT NULL THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_COORDINATOR_SOURCE_TOKEN_FORBIDDEN'
        USING ERRCODE = 'P0001';
    END IF;
    RETURN NEW;
  END IF;

  IF v_job_type NOT IN (
    'WORKBENCH_CANDIDATE_DIRTY_APPLY',
    'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
    'CONTRACT_CLIENT_DIRTY_FANOUT'
  ) THEN
    RETURN NEW;
  END IF;

  v_is_source_enqueue := TG_OP = 'INSERT'
    OR COALESCE(NEW.payload_json->>'latest_event_at_utc', '')
       IS DISTINCT FROM COALESCE(OLD.payload_json->>'latest_event_at_utc', '');

  IF NOT v_is_source_enqueue THEN
    RETURN NEW;
  END IF;

  BEGIN
    v_inherited_generation := NULLIF(BTRIM(COALESCE(
      current_setting('cloudtms.banking_pay_inherited_scope_generation', true),
      ''
    )), '')::bigint;
  EXCEPTION WHEN invalid_text_representation THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_INHERITED_SCOPE_GENERATION_INVALID'
      USING ERRCODE = 'P0001';
  END;

  IF v_inherited_generation IS NOT NULL THEN
    NEW.scope_change_generation := GREATEST(
      COALESCE(NEW.scope_change_generation, 0),
      v_inherited_generation
    );
    NEW.scope_change_tx_token := NULL::uuid;
  ELSE
    NEW.scope_change_tx_token := public.pay_workbench_scope_change_tx_token_v1();
  END IF;

  RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_change_finalize_trg_v1()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_state text;
  v_candidate_count integer := 0;
  v_job_count integer := 0;
  v_generation bigint;
BEGIN
  SELECT scope_tx.state
  INTO v_state
  FROM public.banking_pay_scope_change_transactions AS scope_tx
  WHERE scope_tx.tx_token = NEW.tx_token
  FOR UPDATE;

  IF NOT FOUND OR v_state <> 'PENDING' THEN
    RETURN NEW;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_jobs AS coordinator_job
    WHERE coordinator_job.scope_change_tx_token = NEW.tx_token
      AND UPPER(BTRIM(COALESCE(coordinator_job.job_type, ''))) = 'WORKBENCH_SCOPE_RECONCILE'
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_COORDINATOR_SOURCE_TOKEN_FORBIDDEN'
      USING ERRCODE = 'P0001';
  END IF;

  SELECT COUNT(*)::integer
  INTO v_candidate_count
  FROM public.app_change_counters AS candidate_counter
  WHERE candidate_counter.scope_change_tx_token = NEW.tx_token;

  SELECT COUNT(*)::integer
  INTO v_job_count
  FROM public.banking_pay_workbench_jobs AS staged_job
  WHERE staged_job.scope_change_tx_token = NEW.tx_token
    AND UPPER(BTRIM(COALESCE(staged_job.job_type, ''))) <> 'WORKBENCH_SCOPE_RECONCILE';

  IF v_candidate_count = 0 AND v_job_count = 0 THEN
    UPDATE public.banking_pay_scope_change_transactions AS scope_tx
    SET state = 'NOOP',
        finalized_at_utc = clock_timestamp()
    WHERE scope_tx.tx_token = NEW.tx_token;
    RETURN NEW;
  END IF;

  PERFORM set_config('cloudtms.scope_generation_finalising', 'true', true);

  -- These rows were staged earlier by this transaction. The deterministic
  -- lock pass documents and enforces the finaliser's lock order.
  PERFORM 1
  FROM public.app_change_counters AS staged_counter
  WHERE staged_counter.scope_change_tx_token = NEW.tx_token
  ORDER BY staged_counter.entity_key
  FOR UPDATE;

  PERFORM 1
  FROM public.banking_pay_workbench_jobs AS staged_job
  WHERE staged_job.scope_change_tx_token = NEW.tx_token
  ORDER BY staged_job.id
  FOR UPDATE;

  INSERT INTO public.app_change_counters AS generation_counter(
    entity_key, seq, updated_at
  ) VALUES (
    'pay_candidate_scope_generation', 1, clock_timestamp()
  )
  ON CONFLICT (entity_key) DO UPDATE
  SET seq = generation_counter.seq + 1,
      updated_at = clock_timestamp()
  RETURNING seq INTO v_generation;

  UPDATE public.app_change_counters AS candidate_counter
  SET scope_change_generation = GREATEST(
        COALESCE(candidate_counter.scope_change_generation, 0),
        v_generation
      ),
      scope_change_tx_token = NULL::uuid
  WHERE candidate_counter.scope_change_tx_token = NEW.tx_token;

  UPDATE public.banking_pay_workbench_jobs AS staged_job
  SET scope_change_generation = GREATEST(
        COALESCE(staged_job.scope_change_generation, 0),
        v_generation
      ),
      scope_change_tx_token = NULL::uuid,
      payload_json = COALESCE(staged_job.payload_json, '{}'::jsonb)
        || jsonb_build_object('scope_change_generation', v_generation),
      updated_at_utc = clock_timestamp()
  WHERE staged_job.scope_change_tx_token = NEW.tx_token
    AND UPPER(BTRIM(COALESCE(staged_job.job_type, ''))) <> 'WORKBENCH_SCOPE_RECONCILE';

  UPDATE public.banking_pay_scope_change_transactions AS scope_tx
  SET state = 'FINALIZED',
      allocated_generation = v_generation,
      finalized_at_utc = clock_timestamp()
  WHERE scope_tx.tx_token = NEW.tx_token;

  RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS trg_pay_workbench_scope_counter_stage_v1
  ON public.app_change_counters;
CREATE TRIGGER trg_pay_workbench_scope_counter_stage_v1
BEFORE INSERT OR UPDATE ON public.app_change_counters
FOR EACH ROW
EXECUTE FUNCTION public.pay_workbench_scope_counter_stage_trg_v1();

DROP TRIGGER IF EXISTS trg_pay_workbench_scope_job_stage_v1
  ON public.banking_pay_workbench_jobs;
CREATE TRIGGER trg_pay_workbench_scope_job_stage_v1
BEFORE INSERT OR UPDATE ON public.banking_pay_workbench_jobs
FOR EACH ROW
EXECUTE FUNCTION public.pay_workbench_scope_job_stage_trg_v1();

DROP TRIGGER IF EXISTS trg_pay_workbench_scope_change_finalize_v1
  ON public.banking_pay_scope_change_transactions;
CREATE CONSTRAINT TRIGGER trg_pay_workbench_scope_change_finalize_v1
AFTER INSERT ON public.banking_pay_scope_change_transactions
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION public.pay_workbench_scope_change_finalize_trg_v1();

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_admission_candidates_v1(
  p_session_id uuid,
  p_candidate_ids jsonb,
  p_target_generation bigint
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_candidate_ids jsonb := '[]'::jsonb;
  v_context jsonb := '{}'::jsonb;
  v_filter_candidate_id uuid;
  v_filter_client_id uuid;
BEGIN
  IF p_session_id IS NULL OR jsonb_typeof(COALESCE(p_candidate_ids, 'null'::jsonb)) <> 'array' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_INPUT_INVALID'
      USING ERRCODE = '22023';
  END IF;

  IF jsonb_array_length(p_candidate_ids) > 50 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_INPUT_TOO_LARGE'
      USING ERRCODE = '22023';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM jsonb_array_elements_text(p_candidate_ids) AS candidate_value(value)
    WHERE NULLIF(BTRIM(candidate_value.value), '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_CANDIDATE_ID_INVALID'
      USING ERRCODE = '22023';
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND
     OR UPPER(BTRIM(COALESCE(v_session.status, ''))) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL
     OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_SESSION_NOT_CURRENT'
      USING ERRCODE = 'P0001';
  END IF;

  IF p_target_generation IS NULL OR p_target_generation < v_session.scope_change_generation_applied THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_GENERATION_INVALID'
      USING ERRCODE = '22023';
  END IF;

  IF BTRIM(COALESCE(v_session.filters_json->>'candidate_id', v_session.filters_json->>'candidateId', ''))
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_filter_candidate_id := COALESCE(
      v_session.filters_json->>'candidate_id',
      v_session.filters_json->>'candidateId'
    )::uuid;
  END IF;

  IF BTRIM(COALESCE(v_session.filters_json->>'client_id', v_session.filters_json->>'clientId', ''))
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_filter_client_id := COALESCE(
      v_session.filters_json->>'client_id',
      v_session.filters_json->>'clientId'
    )::uuid;
  END IF;

  PERFORM set_config('cloudtms.banking_pay_scope_admission', 'true', true);
  v_context := public.pay_preview_build_context(
    p_pay_date => v_session.pay_date,
    p_week_ending_cutoff => v_session.week_ending_cutoff,
    p_actor_user_id => v_session.actor_user_id,
    p_candidate_id => v_filter_candidate_id,
    p_client_id => v_filter_client_id,
    p_preview_decisions_json => COALESCE(v_session.filters_json, '{}'::jsonb)
      || jsonb_build_object(
        'preview_context_mode', 'PAGE',
        'scope_limit', GREATEST(jsonb_array_length(p_candidate_ids), 1),
        'scope_admission_candidate_ids', p_candidate_ids
      )
  );

  SELECT COALESCE(jsonb_agg(to_jsonb(candidate_value.value) ORDER BY candidate_value.value), '[]'::jsonb)
  INTO v_candidate_ids
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(v_context->'candidate_ids') = 'array' THEN v_context->'candidate_ids'
      ELSE '[]'::jsonb
    END
  ) AS candidate_value(value);

  RETURN jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'target_generation', p_target_generation,
    'candidate_ids', v_candidate_ids,
    'eligible_count', jsonb_array_length(v_candidate_ids),
    'normal_eligibility_authority_used', v_filter_candidate_id IS NULL,
    'explicit_candidate_filter_preserved', v_filter_candidate_id IS NOT NULL,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_reconcile_ensure_v1()
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_enabled boolean := true;
  v_shadow boolean := false;
  v_generation bigint := 0;
  v_lag_exists boolean := false;
  v_job_id uuid;
  v_created boolean := false;
  v_repair_count integer := 0;
  v_cleanup_count integer := 0;
BEGIN
  SELECT
    COALESCE(settings_row.banking_pay_workbench_scope_reconcile_enabled, true),
    COALESCE(settings_row.banking_pay_workbench_scope_reconcile_shadow_mode, false)
  INTO v_enabled, v_shadow
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  IF NOT COALESCE(v_enabled, true) THEN
    RETURN jsonb_build_object('ok', true, 'enabled', false, 'created', false);
  END IF;

  IF NOT pg_try_advisory_xact_lock(94231, 1) THEN
    RETURN jsonb_build_object('ok', true, 'enabled', true, 'advisory_lock_acquired', false, 'created', false);
  END IF;

  -- Bounded rollout/repair pass: legacy candidate counters predate the global
  -- generation column. Advancing their existing source sequence once lets the
  -- ordinary deferred finaliser assign one coalesced generation without a
  -- full eligibility scan.
  WITH repair_page AS (
    SELECT candidate_counter.entity_key
    FROM public.app_change_counters AS candidate_counter
    WHERE candidate_counter.entity_key LIKE 'pay_candidate:%'
      AND candidate_counter.entity_key
            ~* '^pay_candidate:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND candidate_counter.scope_change_generation IS NULL
    ORDER BY candidate_counter.updated_at, candidate_counter.entity_key
    LIMIT 100
    FOR UPDATE SKIP LOCKED
  )
  UPDATE public.app_change_counters AS candidate_counter
  SET seq = candidate_counter.seq + 1,
      updated_at = clock_timestamp()
  FROM repair_page
  WHERE candidate_counter.entity_key = repair_page.entity_key;
  GET DIAGNOSTICS v_repair_count = ROW_COUNT;

  WITH cleanup_page AS (
    SELECT scope_tx.tx_token
    FROM public.banking_pay_scope_change_transactions AS scope_tx
    WHERE scope_tx.state IN ('FINALIZED', 'NOOP')
      AND scope_tx.finalized_at_utc < clock_timestamp() - interval '7 days'
    ORDER BY scope_tx.finalized_at_utc, scope_tx.tx_token
    LIMIT 100
    FOR UPDATE SKIP LOCKED
  )
  DELETE FROM public.banking_pay_scope_change_transactions AS scope_tx
  USING cleanup_page
  WHERE scope_tx.tx_token = cleanup_page.tx_token;
  GET DIAGNOSTICS v_cleanup_count = ROW_COUNT;

  v_generation := public.pay_workbench_scope_current_generation_v1();

  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
      AND session_row.discarded_at_utc IS NULL
      AND session_row.replacement_session_id IS NULL
      AND COALESCE(session_row.scope_seed_complete, false)
      AND COALESCE(session_row.scope_change_generation_applied, 0) < v_generation
  ) INTO v_lag_exists;

  IF NOT v_lag_exists THEN
    RETURN jsonb_build_object(
      'ok', true,
      'enabled', true,
      'shadow_mode', v_shadow,
      'current_generation', v_generation,
      'lag_exists', false,
      'legacy_counter_repair_count', v_repair_count,
      'staging_cleanup_count', v_cleanup_count,
      'created', false
    );
  END IF;

  INSERT INTO public.banking_pay_workbench_jobs AS coordinator_job(
    job_type, status, priority, run_at_utc, attempt_count, max_attempts,
    dedupe_key, payload_json, created_at_utc, updated_at_utc,
    scope_change_generation, scope_change_tx_token
  ) VALUES (
    'WORKBENCH_SCOPE_RECONCILE', 'QUEUED', 40, clock_timestamp(), 0, 8,
    'WORKBENCH_SCOPE_RECONCILE:GLOBAL',
    jsonb_build_object(
      'run_mode', 'CONTINUOUS_SCOPE_RECONCILE',
      'target_generation', v_generation,
      'candidate_limit', 25,
      'session_limit', 1,
      'shadow_mode', v_shadow,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ),
    clock_timestamp(), clock_timestamp(), v_generation, NULL::uuid
  )
  ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
  DO NOTHING
  RETURNING coordinator_job.id INTO v_job_id;

  v_created := v_job_id IS NOT NULL;
  IF NOT v_created THEN
    SELECT active_job.id
    INTO v_job_id
    FROM public.banking_pay_workbench_jobs AS active_job
    WHERE active_job.dedupe_key = 'WORKBENCH_SCOPE_RECONCILE:GLOBAL'
      AND active_job.status IN ('QUEUED', 'RUNNING')
    ORDER BY active_job.created_at_utc
    LIMIT 1;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'enabled', true,
    'shadow_mode', v_shadow,
    'current_generation', v_generation,
    'lag_exists', true,
    'legacy_counter_repair_count', v_repair_count,
    'staging_cleanup_count', v_cleanup_count,
    'created', v_created,
    'job_id', CASE WHEN v_job_id IS NULL THEN NULL ELSE v_job_id::text END
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_reconcile_drain_one_v1(
  p_candidate_limit integer DEFAULT 25,
  p_worker_id text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_limit integer := LEAST(GREATEST(COALESCE(p_candidate_limit, 25), 1), 50);
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_target bigint := 0;
  v_current_generation bigint := 0;
  v_base_generation bigint := 0;
  v_cursor_generation bigint := 0;
  v_cursor_entity_key text := '';
  v_page_count integer := 0;
  v_has_more boolean := false;
  v_last_generation bigint := 0;
  v_last_entity_key text := '';
  v_shortlist jsonb := '[]'::jsonb;
  v_admission jsonb := '{}'::jsonb;
  v_eligible jsonb := '[]'::jsonb;
  v_inserted jsonb := '[]'::jsonb;
  v_inserted_count integer := 0;
  v_enqueue jsonb := '{}'::jsonb;
  v_upstream_active integer := 0;
  v_upstream_failed integer := 0;
  v_more_sessions boolean := false;
  v_error_message text;
  v_error_detail text;
  v_error_state text;
BEGIN
  SELECT queued_job.*
  INTO v_job
  FROM public.banking_pay_workbench_jobs AS queued_job
  WHERE UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) = 'WORKBENCH_SCOPE_RECONCILE'
    AND queued_job.status = 'QUEUED'
    AND queued_job.run_at_utc <= v_now
  ORDER BY queued_job.priority, queued_job.run_at_utc, queued_job.created_at_utc, queued_job.id
  FOR UPDATE SKIP LOCKED
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', true, 'processed', false, 'reason', 'NO_DUE_COORDINATOR');
  END IF;

  UPDATE public.banking_pay_workbench_jobs AS claimed_job
  SET status = 'RUNNING',
      started_at_utc = v_now,
      updated_at_utc = v_now,
      payload_json = COALESCE(claimed_job.payload_json, '{}'::jsonb)
        || jsonb_build_object('worker_id', COALESCE(NULLIF(BTRIM(p_worker_id), ''), 'SCOPE_RECONCILE'))
  WHERE claimed_job.id = v_job.id
  RETURNING claimed_job.* INTO v_job;

  BEGIN
    v_current_generation := public.pay_workbench_scope_current_generation_v1();
    v_target := CASE
      WHEN COALESCE(v_job.payload_json->>'target_generation', '') ~ '^\d+$'
        THEN (v_job.payload_json->>'target_generation')::bigint
      ELSE v_current_generation
    END;
    v_target := LEAST(GREATEST(v_target, 0), v_current_generation);

    IF COALESCE(v_job.payload_json->>'active_session_id', '')
         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      SELECT session_row.*
      INTO v_session
      FROM public.banking_pay_workbench_sessions AS session_row
      WHERE session_row.id = (v_job.payload_json->>'active_session_id')::uuid
        AND UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
        AND session_row.discarded_at_utc IS NULL
        AND session_row.replacement_session_id IS NULL
        AND COALESCE(session_row.scope_seed_complete, false)
        AND COALESCE(session_row.scope_change_generation_applied, 0) < v_target
      FOR UPDATE;
    END IF;

    IF v_session.id IS NULL THEN
      SELECT session_row.*
      INTO v_session
      FROM public.banking_pay_workbench_sessions AS session_row
      WHERE UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
        AND session_row.discarded_at_utc IS NULL
        AND session_row.replacement_session_id IS NULL
        AND COALESCE(session_row.scope_seed_complete, false)
        AND COALESCE(session_row.scope_change_generation_applied, 0) < v_target
      ORDER BY session_row.session_signature, session_row.id
      FOR UPDATE SKIP LOCKED
      LIMIT 1;
    END IF;

    IF v_session.id IS NULL THEN
      SELECT generation_counter.seq
      INTO v_current_generation
      FROM public.app_change_counters AS generation_counter
      WHERE generation_counter.entity_key = 'pay_candidate_scope_generation'
      FOR UPDATE;

      SELECT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_sessions AS lagging_session
        WHERE UPPER(BTRIM(COALESCE(lagging_session.status, ''))) = 'OPEN'
          AND lagging_session.discarded_at_utc IS NULL
          AND lagging_session.replacement_session_id IS NULL
          AND COALESCE(lagging_session.scope_seed_complete, false)
          AND COALESCE(lagging_session.scope_change_generation_applied, 0) < v_current_generation
      ) INTO v_more_sessions;

      IF v_current_generation > v_target OR v_more_sessions THEN
        UPDATE public.banking_pay_workbench_jobs AS coordinator_job
        SET status = 'QUEUED',
            run_at_utc = v_now,
            started_at_utc = NULL::timestamptz,
            payload_json = (COALESCE(coordinator_job.payload_json, '{}'::jsonb)
              - 'active_session_id' - 'cursor_generation' - 'cursor_entity_key')
              || jsonb_build_object('target_generation', v_current_generation),
            scope_change_generation = v_current_generation,
            updated_at_utc = v_now
        WHERE coordinator_job.id = v_job.id;
        RETURN jsonb_build_object('ok', true, 'processed', true, 'status', 'QUEUED', 'reason', 'NEW_GENERATION_OR_SESSION_LAG');
      END IF;

      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'SUCCEEDED',
          completed_at_utc = v_now,
          started_at_utc = NULL::timestamptz,
          payload_json = COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            || jsonb_build_object('completed_generation', v_target),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
      RETURN jsonb_build_object('ok', true, 'processed', true, 'status', 'SUCCEEDED', 'target_generation', v_target);
    END IF;

    v_base_generation := COALESCE(v_session.scope_change_generation_applied, 0);
    v_cursor_generation := CASE
      WHEN COALESCE(v_job.payload_json->>'cursor_generation', '') ~ '^\d+$'
        THEN (v_job.payload_json->>'cursor_generation')::bigint
      ELSE v_base_generation
    END;
    v_cursor_entity_key := COALESCE(v_job.payload_json->>'cursor_entity_key', '');

    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET scope_change_generation_target = v_target,
        progress_json = COALESCE(session_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'scope_reconcile_status', 'RUNNING',
            'scope_change_generation_target', v_target,
            'scope_change_generation_applied', v_base_generation
          ),
        updated_at_utc = v_now
    WHERE session_update.id = v_session.id;

    SELECT
      COUNT(*) FILTER (WHERE upstream_job.status IN ('QUEUED', 'RUNNING'))::integer,
      COUNT(*) FILTER (WHERE upstream_job.status = 'FAILED')::integer
    INTO v_upstream_active, v_upstream_failed
    FROM public.banking_pay_workbench_jobs AS upstream_job
    WHERE upstream_job.job_type IN (
      'WORKBENCH_CANDIDATE_DIRTY_APPLY',
      'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
      'CONTRACT_CLIENT_DIRTY_FANOUT'
    )
      AND upstream_job.scope_change_generation IS NOT NULL
      AND upstream_job.scope_change_generation <= v_target
      AND upstream_job.status IN ('QUEUED', 'RUNNING', 'FAILED');

    IF v_upstream_failed > 0 THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_RECONCILIATION_UPSTREAM_FAILED'
        USING ERRCODE = 'P0001';
    END IF;

    IF v_upstream_active > 0 THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'QUEUED',
          run_at_utc = v_now + interval '1 second',
          started_at_utc = NULL::timestamptz,
          payload_json = COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'target_generation', v_target,
              'active_session_id', v_session.id::text,
              'upstream_wait_count', v_upstream_active
            ),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
      RETURN jsonb_build_object('ok', true, 'processed', true, 'status', 'QUEUED', 'reason', 'WAITING_FOR_UPSTREAM', 'upstream_count', v_upstream_active);
    END IF;

    DROP TABLE IF EXISTS pg_temp._bpay_scope_reconcile_page;
    CREATE TEMP TABLE _bpay_scope_reconcile_page ON COMMIT DROP AS
    SELECT candidate_change.entity_key,
           candidate_change.scope_change_generation,
           SUBSTRING(candidate_change.entity_key FROM 15)::uuid AS candidate_id,
           ROW_NUMBER() OVER (
             ORDER BY candidate_change.scope_change_generation, candidate_change.entity_key
           ) AS page_ordinal
    FROM public.app_change_counters AS candidate_change
    WHERE candidate_change.entity_key LIKE 'pay_candidate:%'
      AND candidate_change.entity_key
            ~* '^pay_candidate:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND candidate_change.scope_change_generation > v_base_generation
      AND candidate_change.scope_change_generation <= v_target
      AND (
        candidate_change.scope_change_generation > v_cursor_generation
        OR (
          candidate_change.scope_change_generation = v_cursor_generation
          AND candidate_change.entity_key > v_cursor_entity_key
        )
      )
    ORDER BY candidate_change.scope_change_generation, candidate_change.entity_key
    LIMIT (v_limit + 1);

    SELECT COUNT(*)::integer,
           EXISTS (SELECT 1 FROM pg_temp._bpay_scope_reconcile_page WHERE page_ordinal > v_limit)
    INTO v_page_count, v_has_more
    FROM pg_temp._bpay_scope_reconcile_page
    WHERE page_ordinal <= v_limit;

    SELECT COALESCE(jsonb_agg(to_jsonb(page_row.candidate_id::text) ORDER BY page_row.page_ordinal), '[]'::jsonb)
    INTO v_shortlist
    FROM pg_temp._bpay_scope_reconcile_page AS page_row
    WHERE page_row.page_ordinal <= v_limit
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_session_scope AS existing_scope
        WHERE existing_scope.session_id = v_session.id
          AND existing_scope.candidate_id = page_row.candidate_id
      );

    IF jsonb_array_length(v_shortlist) > 0 THEN
      v_admission := public.pay_workbench_scope_admission_candidates_v1(
        v_session.id,
        v_shortlist,
        v_target
      );
      v_eligible := CASE
        WHEN jsonb_typeof(v_admission->'candidate_ids') = 'array' THEN v_admission->'candidate_ids'
        ELSE '[]'::jsonb
      END;
    END IF;

    IF jsonb_array_length(v_eligible) > 0 THEN
      WITH input_candidates AS (
        SELECT DISTINCT candidate_value.value::uuid AS candidate_id
        FROM jsonb_array_elements_text(v_eligible) AS candidate_value(value)
      ), base_ordinal AS (
        SELECT COALESCE(MAX(scope_row.scope_ordinal), 0)::bigint AS value
        FROM public.banking_pay_workbench_session_scope AS scope_row
        WHERE scope_row.session_id = v_session.id
      ), inserted_scope AS (
        INSERT INTO public.banking_pay_workbench_session_scope(
          session_id, candidate_id, scope_ordinal, status, pending_job_id,
          seeded, dirty, error_json, created_at_utc, updated_at_utc
        )
        SELECT v_session.id,
               input_candidates.candidate_id,
               base_ordinal.value + ROW_NUMBER() OVER (ORDER BY input_candidates.candidate_id),
               'PENDING', NULL::uuid, true, false, NULL::jsonb, v_now, v_now
        FROM input_candidates
        CROSS JOIN base_ordinal
        ON CONFLICT (session_id, candidate_id) DO NOTHING
        RETURNING candidate_id
      )
      SELECT COUNT(*)::integer,
             COALESCE(jsonb_agg(to_jsonb(inserted_scope.candidate_id::text) ORDER BY inserted_scope.candidate_id), '[]'::jsonb)
      INTO v_inserted_count, v_inserted
      FROM inserted_scope;

      IF v_inserted_count > 0 THEN
        v_enqueue := public.pay_workbench_enqueue_candidate_refresh_many(
          p_session_id => v_session.id,
          p_candidate_ids => v_inserted,
          p_reason => 'CONTINUOUS_SCOPE_RECONCILE_ADMISSION',
          p_actor_user_id => v_session.actor_user_id
        );
      END IF;
    END IF;

    SELECT page_row.scope_change_generation, page_row.entity_key
    INTO v_last_generation, v_last_entity_key
    FROM pg_temp._bpay_scope_reconcile_page AS page_row
    WHERE page_row.page_ordinal <= v_limit
    ORDER BY page_row.page_ordinal DESC
    LIMIT 1;

    IF v_has_more THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'QUEUED',
          run_at_utc = v_now,
          started_at_utc = NULL::timestamptz,
          payload_json = COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'target_generation', v_target,
              'active_session_id', v_session.id::text,
              'cursor_generation', v_last_generation,
              'cursor_entity_key', v_last_entity_key,
              'candidate_limit', v_limit
            ),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;

      RETURN jsonb_build_object(
        'ok', true, 'processed', true, 'status', 'QUEUED',
        'session_id', v_session.id::text, 'target_generation', v_target,
        'page_count', v_page_count, 'admitted_count', v_inserted_count,
        'has_more', true
      );
    END IF;

    -- The upstream barrier is repeated immediately before advancing the
    -- session watermark. A broad root can never be treated as irrelevant
    -- before its bounded expansion has finished.
    SELECT
      COUNT(*) FILTER (WHERE upstream_job.status IN ('QUEUED', 'RUNNING'))::integer,
      COUNT(*) FILTER (WHERE upstream_job.status = 'FAILED')::integer
    INTO v_upstream_active, v_upstream_failed
    FROM public.banking_pay_workbench_jobs AS upstream_job
    WHERE upstream_job.job_type IN (
      'WORKBENCH_CANDIDATE_DIRTY_APPLY',
      'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
      'CONTRACT_CLIENT_DIRTY_FANOUT'
    )
      AND upstream_job.scope_change_generation IS NOT NULL
      AND upstream_job.scope_change_generation <= v_target
      AND upstream_job.status IN ('QUEUED', 'RUNNING', 'FAILED');

    IF v_upstream_failed > 0 THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_RECONCILIATION_UPSTREAM_FAILED'
        USING ERRCODE = 'P0001';
    END IF;

    IF v_upstream_active > 0 THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'QUEUED', run_at_utc = v_now + interval '1 second',
          started_at_utc = NULL::timestamptz,
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
      RETURN jsonb_build_object('ok', true, 'processed', true, 'status', 'QUEUED', 'reason', 'UPSTREAM_APPEARED_BEFORE_ADVANCE');
    END IF;

    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET scope_change_generation_applied = v_target,
        scope_change_generation_target = v_target,
        progress_json = COALESCE(session_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'scope_reconcile_status', 'SUCCEEDED',
            'scope_change_generation_target', v_target,
            'scope_change_generation_applied', v_target,
            'scope_reconciled_at_utc', v_now::text
          ),
        progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
        progress_updated_at_utc = v_now,
        updated_at_utc = v_now
    WHERE session_update.id = v_session.id;

    SELECT generation_counter.seq
    INTO v_current_generation
    FROM public.app_change_counters AS generation_counter
    WHERE generation_counter.entity_key = 'pay_candidate_scope_generation'
    FOR UPDATE;

    SELECT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_sessions AS lagging_session
      WHERE UPPER(BTRIM(COALESCE(lagging_session.status, ''))) = 'OPEN'
        AND lagging_session.discarded_at_utc IS NULL
        AND lagging_session.replacement_session_id IS NULL
        AND COALESCE(lagging_session.scope_seed_complete, false)
        AND COALESCE(lagging_session.scope_change_generation_applied, 0) < v_current_generation
    ) INTO v_more_sessions;

    IF v_current_generation > v_target OR v_more_sessions THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'QUEUED',
          run_at_utc = v_now,
          started_at_utc = NULL::timestamptz,
          payload_json = (COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            - 'active_session_id' - 'cursor_generation' - 'cursor_entity_key')
            || jsonb_build_object('target_generation', v_current_generation),
          scope_change_generation = v_current_generation,
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
    ELSE
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'SUCCEEDED',
          completed_at_utc = v_now,
          started_at_utc = NULL::timestamptz,
          payload_json = COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            || jsonb_build_object('completed_generation', v_target),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
    END IF;

    RETURN jsonb_build_object(
      'ok', true, 'processed', true,
      'status', CASE WHEN v_current_generation > v_target OR v_more_sessions THEN 'QUEUED' ELSE 'SUCCEEDED' END,
      'session_id', v_session.id::text,
      'target_generation', v_target,
      'current_generation', v_current_generation,
      'page_count', v_page_count,
      'admitted_count', v_inserted_count,
      'has_more', false,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
      v_error_message = MESSAGE_TEXT,
      v_error_detail = PG_EXCEPTION_DETAIL,
      v_error_state = RETURNED_SQLSTATE;

    UPDATE public.banking_pay_workbench_jobs AS failed_coordinator
    SET attempt_count = COALESCE(failed_coordinator.attempt_count, 0) + 1,
        status = CASE
          WHEN COALESCE(failed_coordinator.attempt_count, 0) + 1 >= COALESCE(failed_coordinator.max_attempts, 8)
            THEN 'FAILED'
          ELSE 'QUEUED'
        END,
        run_at_utc = v_now + make_interval(
          secs => LEAST(900, 30 * GREATEST(COALESCE(failed_coordinator.attempt_count, 0) + 1, 1))
        ),
        started_at_utc = NULL::timestamptz,
        failed_at_utc = CASE
          WHEN COALESCE(failed_coordinator.attempt_count, 0) + 1 >= COALESCE(failed_coordinator.max_attempts, 8)
            THEN v_now
          ELSE NULL::timestamptz
        END,
        last_error_json = jsonb_strip_nulls(jsonb_build_object(
          'code', COALESCE(NULLIF(v_error_state, ''), 'PAY_WORKBENCH_SCOPE_RECONCILE_FAILED'),
          'message', v_error_message,
          'detail', v_error_detail,
          'failed_at_utc', v_now::text
        )),
        updated_at_utc = v_now
    WHERE failed_coordinator.id = v_job.id;

    RETURN jsonb_build_object(
      'ok', false,
      'processed', true,
      'job_id', v_job.id::text,
      'code', COALESCE(NULLIF(v_error_state, ''), 'PAY_WORKBENCH_SCOPE_RECONCILE_FAILED'),
      'message', v_error_message
    );
  END;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_progress_v1(p_session_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_current bigint := 0;
  v_active_session_jobs integer := 0;
  v_failed_session_jobs integer := 0;
  v_upstream_active integer := 0;
  v_upstream_failed integer := 0;
  v_display_ready boolean := false;
  v_draft_safe boolean := false;
  v_reason text;
BEGIN
  SELECT session_row.* INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'code', 'WORKBENCH_SESSION_NOT_FOUND');
  END IF;

  v_current := public.pay_workbench_scope_current_generation_v1();

  SELECT
    COUNT(*) FILTER (WHERE job_row.status IN ('QUEUED', 'RUNNING'))::integer,
    COUNT(*) FILTER (WHERE job_row.status = 'FAILED')::integer
  INTO v_active_session_jobs, v_failed_session_jobs
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.session_id = p_session_id
    AND job_row.job_type IN (
      'WORKBENCH_SESSION_SCOPE_SEED', 'WORKBENCH_CANDIDATE_SOURCE_BUILD',
      'WORKBENCH_CANDIDATE_DELTA_REFRESH', 'WORKBENCH_SESSION_CLONE_REBASE',
      'WORKBENCH_CANDIDATE_LINE_WORK_SEED', 'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
      'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
    )
    AND job_row.status IN ('QUEUED', 'RUNNING', 'FAILED');

  SELECT
    COUNT(*) FILTER (WHERE job_row.status IN ('QUEUED', 'RUNNING'))::integer,
    COUNT(*) FILTER (WHERE job_row.status = 'FAILED')::integer
  INTO v_upstream_active, v_upstream_failed
  FROM public.banking_pay_workbench_jobs AS job_row
  WHERE job_row.job_type IN (
      'WORKBENCH_CANDIDATE_DIRTY_APPLY',
      'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
      'CONTRACT_CLIENT_DIRTY_FANOUT'
    )
    AND job_row.scope_change_generation IS NOT NULL
    AND job_row.scope_change_generation <= v_current
    AND job_row.status IN ('QUEUED', 'RUNNING', 'FAILED');

  v_display_ready := UPPER(BTRIM(COALESCE(v_session.status, ''))) = 'OPEN'
    AND v_session.discarded_at_utc IS NULL
    AND v_session.replacement_session_id IS NULL
    AND COALESCE(v_session.scope_seed_complete, false)
    AND (
      COALESCE(v_session.preview_row_count, 0) > 0
      OR (
        v_active_session_jobs = 0
        AND COALESCE(v_session.scope_pending_count, 0) = 0
        AND COALESCE(v_session.line_units_pending, 0) = 0
      )
    );

  v_draft_safe := v_display_ready
    AND COALESCE(v_session.scope_change_generation_applied, 0) = v_current
    AND v_active_session_jobs = 0
    AND v_failed_session_jobs = 0
    AND v_upstream_active = 0
    AND v_upstream_failed = 0;

  v_reason := CASE
    WHEN NOT v_display_ready THEN 'INITIAL_SCOPE_NOT_READY'
    WHEN v_upstream_failed > 0 OR v_failed_session_jobs > 0 THEN 'REFRESH_FAILED'
    WHEN COALESCE(v_session.scope_change_generation_applied, 0) < v_current THEN 'SCOPE_RECONCILIATION_REQUIRED'
    WHEN v_upstream_active > 0 THEN 'UPSTREAM_SCOPE_EXPANSION_IN_PROGRESS'
    WHEN v_active_session_jobs > 0 THEN 'CANDIDATE_REFRESH_IN_PROGRESS'
    ELSE NULL::text
  END;

  RETURN jsonb_build_object(
    'ok', true,
    'display_ready', v_display_ready,
    'draft_safe', v_draft_safe,
    'draft_block_reason_code', v_reason,
    'scope_change_generation_current', v_current,
    'scope_change_generation_target', COALESCE(v_session.scope_change_generation_target, 0),
    'scope_change_generation_applied', COALESCE(v_session.scope_change_generation_applied, 0),
    'scope_change_generation_lag', GREATEST(v_current - COALESCE(v_session.scope_change_generation_applied, 0), 0),
    'scope_reconcile_status', COALESCE(v_session.progress_json->>'scope_reconcile_status', 'IDLE'),
    'scope_reconcile_error', CASE
      WHEN v_upstream_failed > 0 OR v_failed_session_jobs > 0 THEN 'A Banking Pay refresh failed.'
      ELSE NULL::text
    END
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_expired_snooze_refreshes_v1(
  p_session_id uuid DEFAULT NULL::uuid,
  p_session_limit integer DEFAULT 25,
  p_candidate_limit integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_date_context jsonb := public.pay_banking_official_date_context_v1(NULL::timestamptz);
  v_today_uk date;
  v_current_official_pay_date date;
  v_limit integer := LEAST(GREATEST(COALESCE(p_candidate_limit, 100), 1), 100);
  v_actor_user_id uuid;
  v_due record;
  v_transition jsonb := '{}'::jsonb;
  v_enqueue jsonb := '{}'::jsonb;
  v_processed integer := 0;
  v_changed integer := 0;
  v_enqueued integer := 0;
  v_more_due boolean := false;
  v_results jsonb := '[]'::jsonb;
BEGIN
  v_today_uk := (v_date_context->>'business_date')::date;
  v_current_official_pay_date := (v_date_context->>'current_official_pay_date')::date;

  SELECT session_row.actor_user_id
  INTO v_actor_user_id
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
    AND session_row.discarded_at_utc IS NULL
    AND session_row.replacement_session_id IS NULL
    AND (p_session_id IS NULL OR session_row.id = p_session_id)
  ORDER BY session_row.updated_at_utc DESC, session_row.id
  LIMIT 1;

  IF v_actor_user_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'processed_session_count', 0,
      'processed_snooze_count', 0,
      'transition_changed_count', 0,
      'enqueued_count', 0,
      'more_due', false,
      'reason', 'NO_CURRENT_OPEN_SESSION'
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_global_due_snoozes;
  CREATE TEMP TABLE _bpay_global_due_snoozes ON COMMIT DROP AS
  SELECT snooze_row.id,
         snooze_row.candidate_id,
         snooze_row.timesheet_id,
         snooze_row.snooze_until_date,
         ROW_NUMBER() OVER (
           ORDER BY snooze_row.snooze_until_date, snooze_row.candidate_id, snooze_row.id
         ) AS page_ordinal
  FROM public.pay_item_snoozes AS snooze_row
  WHERE snooze_row.cleared_at_utc IS NULL
    AND snooze_row.cancelled_at_utc IS NULL
    AND snooze_row.snooze_until_date IS NOT NULL
    AND snooze_row.snooze_until_date < v_today_uk
  ORDER BY snooze_row.snooze_until_date, snooze_row.candidate_id, snooze_row.id
  LIMIT (v_limit + 1);

  v_more_due := EXISTS (
    SELECT 1 FROM pg_temp._bpay_global_due_snoozes WHERE page_ordinal > v_limit
  );

  FOR v_due IN
    SELECT due_snooze.*
    FROM pg_temp._bpay_global_due_snoozes AS due_snooze
    WHERE due_snooze.page_ordinal <= v_limit
    ORDER BY due_snooze.page_ordinal
  LOOP
    v_transition := public.pay_snooze_resolution_transition_v1(
      p_snooze_id => v_due.id,
      p_actor_user_id => v_actor_user_id,
      p_transition_reason => 'NATURAL_EXPIRY'
    );

    v_processed := v_processed + 1;
    IF LOWER(BTRIM(COALESCE(v_transition->>'state_changed', 'false')))
         IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      v_changed := v_changed + 1;
      v_enqueue := public.pay_workbench_dirty_event_enqueue(
        p_job_type => 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        p_scope_kind => 'CANDIDATE',
        p_scope_id => v_due.candidate_id::text,
        p_candidate_id => v_due.candidate_id,
        p_targeted_timesheet_ids => CASE
          WHEN v_due.timesheet_id IS NULL THEN ARRAY[]::uuid[]
          ELSE ARRAY[v_due.timesheet_id]::uuid[]
        END,
        p_linked_timesheet_ids => ARRAY[]::uuid[],
        p_payload_json => jsonb_build_object(
          'refresh_scope_kind', CASE
            WHEN v_due.timesheet_id IS NULL THEN 'CANDIDATE_FULL_LIVE'
            ELSE 'TARGETED_TIMESHEETS'
          END,
          'snooze_id', v_due.id::text,
          'london_current_date', v_today_uk::text,
          'current_official_pay_date', v_current_official_pay_date::text,
          'reason', 'SNOOZE_EXPIRED_LONDON_DATE',
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
        ),
        p_reason => 'SNOOZE_EXPIRED_LONDON_DATE',
        p_priority => -1000,
        p_run_at_utc => v_now
      );

      v_enqueued := v_enqueued + 1;
      v_results := v_results || jsonb_build_array(jsonb_build_object(
        'candidate_id', v_due.candidate_id::text,
        'snooze_id', v_due.id::text,
        'job_id', v_enqueue->>'job_id'
      ));
    END IF;
  END LOOP;

  RETURN jsonb_build_object(
    'ok', true,
    'global_due_scan', true,
    'processed_session_count', 1,
    'processed_snooze_count', v_processed,
    'transition_changed_count', v_changed,
    'unchanged_due_count', GREATEST(v_processed - v_changed, 0),
    'enqueued_count', v_enqueued,
    'more_due', v_more_due,
    'work_limit', v_limit,
    'results', v_results
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_workbench_scope_current_generation_v1() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_change_tx_token_v1() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_counter_stage_trg_v1() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_job_stage_trg_v1() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_change_finalize_trg_v1() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_admission_candidates_v1(uuid, jsonb, bigint) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_reconcile_ensure_v1() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_reconcile_drain_one_v1(integer, text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_progress_v1(uuid) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_enqueue_expired_snooze_refreshes_v1(uuid, integer, integer) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_current_generation_v1() TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_change_tx_token_v1() TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_counter_stage_trg_v1() TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_job_stage_trg_v1() TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_change_finalize_trg_v1() TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_admission_candidates_v1(uuid, jsonb, bigint) TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_reconcile_ensure_v1() TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_reconcile_drain_one_v1(integer, text) TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_progress_v1(uuid) TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_enqueue_expired_snooze_refreshes_v1(uuid, integer, integer) TO service_role;

DO $verification$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgrelid = 'public.banking_pay_scope_change_transactions'::regclass
      AND tgname = 'trg_pay_workbench_scope_change_finalize_v1'
      AND tgdeferrable
      AND tginitdeferred
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_FINALISER_TRIGGER_NOT_DEFERRED';
  END IF;

  IF has_function_privilege('anon', 'public.pay_workbench_scope_reconcile_ensure_v1()', 'EXECUTE')
     OR has_function_privilege('authenticated', 'public.pay_workbench_scope_reconcile_ensure_v1()', 'EXECUTE') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_INTERNAL_HELPER_EXPOSED';
  END IF;
END
$verification$;
-- ---------------------------------------------------------------------------
-- Banking Pay continuous-scope completion overrides (1 August 2026)
-- ---------------------------------------------------------------------------
-- These are deliberately the final effective definitions in this late
-- repeatable. They complete recovery-aware blocking, genuine shadow mode,
-- shortlist isolation and durable snooze-expiry fairness without adding a
-- queue, Worker RPC, financial calculation path or Policy X fallback.

-- The deployment workflow installs changed repeatables before one-time
-- migrations. Keep the additive columns idempotently available here; the
-- migration remains the one-time schema ledger authority.
ALTER TABLE public.banking_pay_workbench_sessions
  ADD COLUMN IF NOT EXISTS scope_change_generation_shadow_checked bigint NOT NULL DEFAULT 0;

ALTER TABLE public.pay_item_snoozes
  ADD COLUMN IF NOT EXISTS natural_expiry_source_fingerprint text,
  ADD COLUMN IF NOT EXISTS natural_expiry_checked_fingerprint text,
  ADD COLUMN IF NOT EXISTS natural_expiry_checked_at_utc timestamptz,
  ADD COLUMN IF NOT EXISTS natural_expiry_state_changed boolean,
  ADD COLUMN IF NOT EXISTS natural_expiry_result_code text;

CREATE OR REPLACE FUNCTION public.pay_item_snoozes_sync_identity_fields()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_booking_id text;
BEGIN
  NEW.source_ref := NULLIF(BTRIM(COALESCE(NEW.source_ref, '')), '');
  NEW.segment_id := NULLIF(BTRIM(COALESCE(NEW.segment_id, '')), '');
  NEW.booking_id := NULLIF(BTRIM(COALESCE(NEW.booking_id, '')), '');
  NEW.segment_stable_key := NULLIF(BTRIM(COALESCE(NEW.segment_stable_key, '')), '');
  NEW.note := NULLIF(BTRIM(COALESCE(NEW.note, '')), '');

  NEW.snooze_kind := UPPER(BTRIM(COALESCE(NEW.snooze_kind, 'DO_NOT_PAY')));
  IF NEW.snooze_kind = 'BLOCKED' THEN
    NEW.snooze_kind := 'BLOCKED_TIMESHEET';
  ELSIF NEW.snooze_kind = 'LOAN_REPAYMENT' THEN
    NEW.snooze_kind := 'PAYMENT_ADVANCE_REPAYMENT';
  END IF;

  IF NEW.timesheet_id IS NOT NULL AND NEW.booking_id IS NULL THEN
    SELECT NULLIF(BTRIM(COALESCE(timesheet_row.booking_id, '')), '')
    INTO v_booking_id
    FROM public.timesheets AS timesheet_row
    WHERE timesheet_row.timesheet_id = NEW.timesheet_id
    LIMIT 1;

    IF v_booking_id IS NOT NULL THEN
      NEW.booking_id := v_booking_id;
    END IF;
  END IF;

  IF NEW.segment_stable_key IS NULL AND NEW.segment_id IS NOT NULL THEN
    NEW.segment_stable_key := NEW.segment_id;
  END IF;

  NEW.natural_expiry_source_fingerprint := ENCODE(
    extensions.digest(
      convert_to(
        jsonb_build_array(
          COALESCE(NEW.id::text, ''),
          COALESCE(NEW.candidate_id::text, ''),
          COALESCE(NEW.timesheet_id::text, ''),
          COALESCE(NEW.booking_id, ''),
          COALESCE(NEW.segment_id, ''),
          COALESCE(NEW.segment_stable_key, ''),
          COALESCE(NEW.source_ref, ''),
          COALESCE(NEW.snooze_kind, ''),
          COALESCE(NEW.snooze_until_date::text, '')
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS trg_pay_item_snoozes_sync_identity_fields_biu
  ON public.pay_item_snoozes;
CREATE TRIGGER trg_pay_item_snoozes_sync_identity_fields_biu
BEFORE INSERT OR UPDATE OF
  candidate_id,
  timesheet_id,
  booking_id,
  segment_id,
  segment_stable_key,
  source_ref,
  snooze_kind,
  snooze_until_date,
  note
ON public.pay_item_snoozes
FOR EACH ROW
EXECUTE FUNCTION public.pay_item_snoozes_sync_identity_fields();

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_blocker_state_v1(
  p_session_id uuid,
  p_target_generation bigint,
  p_operation_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_operation public.banking_pay_operations%ROWTYPE;
  v_filter_candidate_id uuid;
  v_filter_client_id uuid;
  v_upstream_active integer := 0;
  v_upstream_failed integer := 0;
  v_session_active integer := 0;
  v_session_failed integer := 0;
  v_broad_pending integer := 0;
  v_unknown_relevance integer := 0;
  v_active_sample jsonb := '[]'::jsonb;
  v_failure_sample jsonb := '[]'::jsonb;
  v_session_active_sample jsonb := '[]'::jsonb;
  v_session_failure_sample jsonb := '[]'::jsonb;
BEGIN
  IF p_session_id IS NULL OR p_target_generation IS NULL OR p_target_generation < 0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_BLOCKER_INPUT_INVALID'
      USING ERRCODE = '22023';
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_BLOCKER_SESSION_NOT_FOUND'
      USING ERRCODE = 'P0001';
  END IF;

  IF p_operation_id IS NOT NULL THEN
    SELECT operation_row.*
    INTO v_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
      AND operation_row.workbench_session_id = p_session_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_BLOCKER_OPERATION_MISMATCH'
        USING ERRCODE = 'P0001';
    END IF;
  END IF;

  IF BTRIM(COALESCE(v_session.filters_json->>'candidate_id', v_session.filters_json->>'candidateId', ''))
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_filter_candidate_id := COALESCE(
      v_session.filters_json->>'candidate_id',
      v_session.filters_json->>'candidateId'
    )::uuid;
  END IF;

  IF BTRIM(COALESCE(v_session.filters_json->>'client_id', v_session.filters_json->>'clientId', ''))
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_filter_client_id := COALESCE(
      v_session.filters_json->>'client_id',
      v_session.filters_json->>'clientId'
    )::uuid;
  END IF;

  WITH generated_raw AS MATERIALIZED (
    SELECT
      job_row.*,
      COALESCE(NULLIF(BTRIM(job_row.dedupe_key), ''), 'JOB:' || job_row.id::text) AS work_identity,
      UPPER(BTRIM(COALESCE(job_row.payload_json->>'scope_kind', ''))) AS scope_kind,
      COALESCE(
        NULLIF(BTRIM(job_row.payload_json->>'scope_id'), ''),
        NULLIF(BTRIM(job_row.payload_json->>'client_id'), ''),
        NULLIF(BTRIM(job_row.payload_json->>'contract_id'), '')
      ) AS direct_scope_id,
      evidence.candidate_ids,
      CASE
        WHEN UPPER(BTRIM(COALESCE(job_row.payload_json->>'scope_kind', ''))) = 'CLIENT'
         AND COALESCE(job_row.payload_json->>'scope_id', job_row.payload_json->>'client_id', '')
               ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN COALESCE(job_row.payload_json->>'scope_id', job_row.payload_json->>'client_id')::uuid
        ELSE NULL::uuid
      END AS direct_client_id,
      CASE
        WHEN UPPER(BTRIM(COALESCE(job_row.payload_json->>'scope_kind', ''))) = 'CONTRACT'
         AND COALESCE(job_row.payload_json->>'scope_id', job_row.payload_json->>'contract_id', '')
               ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN COALESCE(job_row.payload_json->>'scope_id', job_row.payload_json->>'contract_id')::uuid
        ELSE NULL::uuid
      END AS direct_contract_id
    FROM public.banking_pay_workbench_jobs AS job_row
    CROSS JOIN LATERAL (
      SELECT COALESCE(array_agg(DISTINCT candidate_source.candidate_id ORDER BY candidate_source.candidate_id), ARRAY[]::uuid[]) AS candidate_ids
      FROM (
        SELECT job_row.candidate_id
        WHERE job_row.candidate_id IS NOT NULL
        UNION ALL
        SELECT (job_row.payload_json->>'candidate_id')::uuid
        WHERE COALESCE(job_row.payload_json->>'candidate_id', '')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        UNION ALL
        SELECT (job_row.payload_json->>'processed_candidate_id')::uuid
        WHERE COALESCE(job_row.payload_json->>'processed_candidate_id', '')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        UNION ALL
        SELECT candidate_value.value::uuid
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(job_row.payload_json->'candidate_ids') = 'array'
              THEN job_row.payload_json->'candidate_ids'
            ELSE '[]'::jsonb
          END
        ) AS candidate_value(value)
        WHERE candidate_value.value
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) AS candidate_source
    ) AS evidence
    WHERE job_row.job_type IN (
      'WORKBENCH_CANDIDATE_DIRTY_APPLY',
      'WORKBENCH_FINANCE_CASE_DIRTY_APPLY',
      'CONTRACT_CLIENT_DIRTY_FANOUT'
    )
      AND job_row.scope_change_generation IS NOT NULL
      AND job_row.scope_change_generation <= p_target_generation
      AND job_row.status IN ('QUEUED', 'RUNNING', 'FAILED', 'DEAD', 'SUCCEEDED')
  ), generated_classified AS MATERIALIZED (
    SELECT
      generated_raw.*,
      CASE
        WHEN p_operation_id IS NOT NULL THEN
          CASE
            WHEN generated_raw.job_type <> 'CONTRACT_CLIENT_DIRTY_FANOUT' THEN
              CASE
                WHEN COALESCE(array_length(generated_raw.candidate_ids, 1), 0) = 0 THEN 'UNKNOWN'
                WHEN EXISTS (
                  SELECT 1
                  FROM public.banking_pay_operation_candidate_scope AS operation_scope
                  WHERE operation_scope.operation_id = p_operation_id
                    AND operation_scope.candidate_id = ANY(generated_raw.candidate_ids)
                ) THEN 'RELEVANT'
                ELSE 'IRRELEVANT'
              END
            WHEN generated_raw.scope_kind = 'CANDIDATE'
             AND COALESCE(array_length(generated_raw.candidate_ids, 1), 0) > 0 THEN
              CASE
                WHEN EXISTS (
                  SELECT 1
                  FROM public.banking_pay_operation_candidate_scope AS operation_scope
                  WHERE operation_scope.operation_id = p_operation_id
                    AND operation_scope.candidate_id = ANY(generated_raw.candidate_ids)
                ) THEN 'RELEVANT'
                ELSE 'IRRELEVANT'
              END
            ELSE 'UNKNOWN'
          END
        WHEN v_filter_candidate_id IS NOT NULL THEN
          CASE
            WHEN generated_raw.job_type <> 'CONTRACT_CLIENT_DIRTY_FANOUT' THEN
              CASE
                WHEN COALESCE(array_length(generated_raw.candidate_ids, 1), 0) = 0 THEN 'UNKNOWN'
                WHEN v_filter_candidate_id = ANY(generated_raw.candidate_ids) THEN 'RELEVANT'
                ELSE 'IRRELEVANT'
              END
            WHEN generated_raw.scope_kind = 'CANDIDATE'
             AND COALESCE(array_length(generated_raw.candidate_ids, 1), 0) > 0 THEN
              CASE WHEN v_filter_candidate_id = ANY(generated_raw.candidate_ids) THEN 'RELEVANT' ELSE 'IRRELEVANT' END
            ELSE 'UNKNOWN'
          END
        WHEN v_filter_client_id IS NOT NULL THEN
          CASE
            WHEN generated_raw.job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT'
             AND generated_raw.scope_kind = 'CLIENT'
             AND generated_raw.direct_client_id IS NOT NULL
              THEN CASE WHEN generated_raw.direct_client_id = v_filter_client_id THEN 'RELEVANT' ELSE 'IRRELEVANT' END
            WHEN generated_raw.job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT'
             AND generated_raw.scope_kind = 'CONTRACT'
             AND generated_raw.direct_contract_id IS NOT NULL
              THEN CASE
                WHEN EXISTS (
                  SELECT 1
                  FROM public.contracts AS contract_row
                  WHERE contract_row.id = generated_raw.direct_contract_id
                    AND contract_row.client_id = v_filter_client_id
                ) THEN 'RELEVANT'
                ELSE 'IRRELEVANT'
              END
            ELSE 'UNKNOWN'
          END
        ELSE 'RELEVANT'
      END AS relevance_decision
    FROM generated_raw
  ), generated_ranked AS MATERIALIZED (
    SELECT
      generated_classified.*,
      ROW_NUMBER() OVER (
        PARTITION BY generated_classified.work_identity
        ORDER BY generated_classified.scope_change_generation DESC,
                 generated_classified.created_at_utc DESC,
                 generated_classified.id DESC
      ) AS authority_ordinal
    FROM generated_classified
    WHERE generated_classified.relevance_decision <> 'IRRELEVANT'
  ), generated_effective AS MATERIALIZED (
    SELECT *
    FROM generated_ranked
    WHERE authority_ordinal = 1
  )
  SELECT
    (SELECT COUNT(*)::integer FROM generated_effective WHERE status IN ('QUEUED', 'RUNNING')),
    (SELECT COUNT(*)::integer FROM generated_effective WHERE status IN ('FAILED', 'DEAD')),
    (SELECT COUNT(*)::integer FROM generated_effective
      WHERE job_type = 'CONTRACT_CLIENT_DIRTY_FANOUT'
        AND status IN ('QUEUED', 'RUNNING', 'FAILED', 'DEAD')),
    (SELECT COUNT(*)::integer FROM generated_effective WHERE relevance_decision = 'UNKNOWN'),
    COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'job_id', limited_job.id::text,
        'job_type', limited_job.job_type,
        'status', limited_job.status,
        'scope_change_generation', limited_job.scope_change_generation,
        'dedupe_key', limited_job.work_identity,
        'relevance', limited_job.relevance_decision
      ) ORDER BY limited_job.scope_change_generation, limited_job.id)
      FROM (
        SELECT * FROM generated_effective
        WHERE status IN ('QUEUED', 'RUNNING')
        ORDER BY scope_change_generation, id
        LIMIT 25
      ) AS limited_job
    ), '[]'::jsonb),
    COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'job_id', limited_job.id::text,
        'job_type', limited_job.job_type,
        'status', limited_job.status,
        'scope_change_generation', limited_job.scope_change_generation,
        'dedupe_key', limited_job.work_identity,
        'relevance', limited_job.relevance_decision
      ) ORDER BY limited_job.scope_change_generation, limited_job.id)
      FROM (
        SELECT * FROM generated_effective
        WHERE status IN ('FAILED', 'DEAD')
        ORDER BY scope_change_generation, id
        LIMIT 25
      ) AS limited_job
    ), '[]'::jsonb)
  INTO
    v_upstream_active,
    v_upstream_failed,
    v_broad_pending,
    v_unknown_relevance,
    v_active_sample,
    v_failure_sample;

  WITH session_ranked AS MATERIALIZED (
    SELECT
      session_job.*,
      COALESCE(NULLIF(BTRIM(session_job.dedupe_key), ''), 'JOB:' || session_job.id::text) AS work_identity,
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(NULLIF(BTRIM(session_job.dedupe_key), ''), 'JOB:' || session_job.id::text)
        ORDER BY session_job.created_at_utc DESC, session_job.id DESC
      ) AS authority_ordinal
    FROM public.banking_pay_workbench_jobs AS session_job
    WHERE session_job.session_id = p_session_id
      AND session_job.job_type IN (
        'WORKBENCH_SESSION_SCOPE_SEED',
        'WORKBENCH_CANDIDATE_SOURCE_BUILD',
        'WORKBENCH_CANDIDATE_DELTA_REFRESH',
        'WORKBENCH_SESSION_CLONE_REBASE',
        'WORKBENCH_CANDIDATE_LINE_WORK_SEED',
        'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',
        'WORKBENCH_PREVIEW_ROWS_MATERIALISE'
      )
      AND session_job.status IN ('QUEUED', 'RUNNING', 'FAILED', 'DEAD', 'SUCCEEDED')
  ), session_effective AS MATERIALIZED (
    SELECT *
    FROM session_ranked
    WHERE authority_ordinal = 1
  )
  SELECT
    (SELECT COUNT(*)::integer FROM session_effective WHERE status IN ('QUEUED', 'RUNNING')),
    (SELECT COUNT(*)::integer FROM session_effective WHERE status IN ('FAILED', 'DEAD')),
    COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'job_id', limited_job.id::text,
        'job_type', limited_job.job_type,
        'status', limited_job.status,
        'dedupe_key', limited_job.work_identity
      ) ORDER BY limited_job.created_at_utc, limited_job.id)
      FROM (
        SELECT * FROM session_effective
        WHERE status IN ('QUEUED', 'RUNNING')
        ORDER BY created_at_utc, id
        LIMIT 25
      ) AS limited_job
    ), '[]'::jsonb),
    COALESCE((
      SELECT jsonb_agg(jsonb_build_object(
        'job_id', limited_job.id::text,
        'job_type', limited_job.job_type,
        'status', limited_job.status,
        'dedupe_key', limited_job.work_identity
      ) ORDER BY limited_job.created_at_utc, limited_job.id)
      FROM (
        SELECT * FROM session_effective
        WHERE status IN ('FAILED', 'DEAD')
        ORDER BY created_at_utc, id
        LIMIT 25
      ) AS limited_job
    ), '[]'::jsonb)
  INTO
    v_session_active,
    v_session_failed,
    v_session_active_sample,
    v_session_failure_sample;

  SELECT COALESCE(jsonb_agg(sample_row.value ORDER BY sample_row.ordinality), '[]'::jsonb)
  INTO v_active_sample
  FROM jsonb_array_elements(COALESCE(v_active_sample, '[]'::jsonb) || COALESCE(v_session_active_sample, '[]'::jsonb))
       WITH ORDINALITY AS sample_row(value, ordinality)
  WHERE sample_row.ordinality <= 25;

  SELECT COALESCE(jsonb_agg(sample_row.value ORDER BY sample_row.ordinality), '[]'::jsonb)
  INTO v_failure_sample
  FROM jsonb_array_elements(COALESCE(v_failure_sample, '[]'::jsonb) || COALESCE(v_session_failure_sample, '[]'::jsonb))
       WITH ORDINALITY AS sample_row(value, ordinality)
  WHERE sample_row.ordinality <= 25;

  RETURN jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'operation_id', CASE WHEN p_operation_id IS NULL THEN NULL ELSE p_operation_id::text END,
    'target_generation', p_target_generation,
    'upstream_active_count', COALESCE(v_upstream_active, 0),
    'upstream_unresolved_failure_count', COALESCE(v_upstream_failed, 0),
    'session_active_count', COALESCE(v_session_active, 0),
    'session_unresolved_failure_count', COALESCE(v_session_failed, 0),
    'session_scope_failed_count', COALESCE(v_session.scope_failed_count, 0),
    'session_line_failed_count', COALESCE(v_session.line_units_failed, 0),
    'broad_root_pending_count', COALESCE(v_broad_pending, 0),
    'unknown_relevance_count', COALESCE(v_unknown_relevance, 0),
    'active_sample', COALESCE(v_active_sample, '[]'::jsonb),
    'failure_sample', COALESCE(v_failure_sample, '[]'::jsonb),
    'all_clear',
      COALESCE(v_upstream_active, 0) = 0
      AND COALESCE(v_upstream_failed, 0) = 0
      AND COALESCE(v_session_active, 0) = 0
      AND COALESCE(v_session_failed, 0) = 0
      AND COALESCE(v_session.scope_failed_count, 0) = 0
      AND COALESCE(v_session.line_units_failed, 0) = 0
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_admission_candidates_v1(
  p_session_id uuid,
  p_candidate_ids jsonb,
  p_target_generation bigint
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_input_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_shortlist jsonb := '[]'::jsonb;
  v_candidate_ids jsonb := '[]'::jsonb;
  v_context jsonb := '{}'::jsonb;
  v_filter_candidate_id uuid;
  v_filter_client_id uuid;
  v_current_generation bigint := 0;
BEGIN
  IF p_session_id IS NULL OR jsonb_typeof(COALESCE(p_candidate_ids, 'null'::jsonb)) <> 'array' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_INPUT_INVALID'
      USING ERRCODE = '22023';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM jsonb_array_elements_text(p_candidate_ids) AS candidate_value(value)
    WHERE NULLIF(BTRIM(candidate_value.value), '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_CANDIDATE_ID_INVALID'
      USING ERRCODE = '22023';
  END IF;

  SELECT COALESCE(array_agg(DISTINCT candidate_value.value::uuid ORDER BY candidate_value.value::uuid), ARRAY[]::uuid[])
  INTO v_input_candidate_ids
  FROM jsonb_array_elements_text(p_candidate_ids) AS candidate_value(value);

  IF COALESCE(array_length(v_input_candidate_ids, 1), 0) > 50 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_INPUT_TOO_LARGE'
      USING ERRCODE = '22023';
  END IF;

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND
     OR UPPER(BTRIM(COALESCE(v_session.status, ''))) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL
     OR v_session.replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_SESSION_NOT_CURRENT'
      USING ERRCODE = 'P0001';
  END IF;

  v_current_generation := public.pay_workbench_scope_current_generation_v1();
  IF p_target_generation IS NULL
     OR p_target_generation < v_session.scope_change_generation_applied
     OR p_target_generation > v_current_generation THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_GENERATION_INVALID'
      USING ERRCODE = '22023';
  END IF;

  IF COALESCE(array_length(v_input_candidate_ids, 1), 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'session_id', p_session_id::text,
      'target_generation', p_target_generation,
      'candidate_ids', '[]'::jsonb,
      'eligible_count', 0,
      'shortlist_empty', true,
      'normal_eligibility_authority_used', true,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  END IF;

  SELECT COALESCE(jsonb_agg(to_jsonb(candidate_id::text) ORDER BY candidate_id), '[]'::jsonb)
  INTO v_shortlist
  FROM unnest(v_input_candidate_ids) AS candidate_id;

  IF BTRIM(COALESCE(v_session.filters_json->>'candidate_id', v_session.filters_json->>'candidateId', ''))
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_filter_candidate_id := COALESCE(
      v_session.filters_json->>'candidate_id',
      v_session.filters_json->>'candidateId'
    )::uuid;

    IF NOT (v_filter_candidate_id = ANY(v_input_candidate_ids)) THEN
      RETURN jsonb_build_object(
        'ok', true,
        'session_id', p_session_id::text,
        'target_generation', p_target_generation,
        'candidate_ids', '[]'::jsonb,
        'eligible_count', 0,
        'shortlist_empty', false,
        'explicit_candidate_filter_intersection_empty', true,
        'normal_eligibility_authority_used', false,
        'explicit_candidate_filter_preserved', true,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
      );
    END IF;
  END IF;

  IF BTRIM(COALESCE(v_session.filters_json->>'client_id', v_session.filters_json->>'clientId', ''))
       ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_filter_client_id := COALESCE(
      v_session.filters_json->>'client_id',
      v_session.filters_json->>'clientId'
    )::uuid;
  END IF;

  PERFORM set_config('cloudtms.banking_pay_scope_admission', 'true', true);
  v_context := public.pay_preview_build_context(
    p_pay_date => v_session.pay_date,
    p_week_ending_cutoff => v_session.week_ending_cutoff,
    p_actor_user_id => v_session.actor_user_id,
    p_candidate_id => v_filter_candidate_id,
    p_client_id => v_filter_client_id,
    p_preview_decisions_json => COALESCE(v_session.filters_json, '{}'::jsonb)
      || jsonb_build_object(
        'preview_context_mode', 'PAGE',
        'scope_limit', COALESCE(array_length(v_input_candidate_ids, 1), 0),
        'scope_admission_candidate_ids', v_shortlist
      )
  );

  SELECT COALESCE(jsonb_agg(to_jsonb(candidate_value.value) ORDER BY candidate_value.value), '[]'::jsonb)
  INTO v_candidate_ids
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(v_context->'candidate_ids') = 'array' THEN v_context->'candidate_ids'
      ELSE '[]'::jsonb
    END
  ) AS candidate_value(value);

  IF EXISTS (
    SELECT 1
    FROM jsonb_array_elements_text(v_candidate_ids) AS output_candidate(value)
    WHERE output_candidate.value::uuid <> ALL(v_input_candidate_ids)
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_ADMISSION_OUTPUT_OUTSIDE_SHORTLIST'
      USING ERRCODE = 'P0001';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'target_generation', p_target_generation,
    'candidate_ids', v_candidate_ids,
    'eligible_count', jsonb_array_length(v_candidate_ids),
    'shortlist_empty', false,
    'normal_eligibility_authority_used', v_filter_candidate_id IS NULL,
    'explicit_candidate_filter_preserved', v_filter_candidate_id IS NOT NULL,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_reconcile_ensure_v1()
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_enabled boolean := true;
  v_shadow boolean := false;
  v_generation bigint := 0;
  v_lag_exists boolean := false;
  v_job_id uuid;
  v_created boolean := false;
  v_repair_count integer := 0;
  v_cleanup_count integer := 0;
  v_mode text := 'AUTHORITATIVE';
BEGIN
  SELECT
    COALESCE(settings_row.banking_pay_workbench_scope_reconcile_enabled, true),
    COALESCE(settings_row.banking_pay_workbench_scope_reconcile_shadow_mode, false)
  INTO v_enabled, v_shadow
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  IF NOT COALESCE(v_enabled, true) THEN
    RETURN jsonb_build_object('ok', true, 'enabled', false, 'created', false);
  END IF;

  IF NOT pg_try_advisory_xact_lock(94231, 1) THEN
    RETURN jsonb_build_object(
      'ok', true,
      'enabled', true,
      'advisory_lock_acquired', false,
      'created', false
    );
  END IF;

  WITH repair_page AS (
    SELECT candidate_counter.entity_key
    FROM public.app_change_counters AS candidate_counter
    WHERE candidate_counter.entity_key LIKE 'pay_candidate:%'
      AND candidate_counter.entity_key
            ~* '^pay_candidate:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND candidate_counter.scope_change_generation IS NULL
    ORDER BY candidate_counter.updated_at, candidate_counter.entity_key
    LIMIT 100
    FOR UPDATE SKIP LOCKED
  )
  UPDATE public.app_change_counters AS candidate_counter
  SET seq = candidate_counter.seq + 1,
      updated_at = clock_timestamp()
  FROM repair_page
  WHERE candidate_counter.entity_key = repair_page.entity_key;
  GET DIAGNOSTICS v_repair_count = ROW_COUNT;

  WITH cleanup_page AS (
    SELECT scope_tx.tx_token
    FROM public.banking_pay_scope_change_transactions AS scope_tx
    WHERE scope_tx.state IN ('FINALIZED', 'NOOP')
      AND scope_tx.finalized_at_utc < clock_timestamp() - interval '7 days'
    ORDER BY scope_tx.finalized_at_utc, scope_tx.tx_token
    LIMIT 100
    FOR UPDATE SKIP LOCKED
  )
  DELETE FROM public.banking_pay_scope_change_transactions AS scope_tx
  USING cleanup_page
  WHERE scope_tx.tx_token = cleanup_page.tx_token;
  GET DIAGNOSTICS v_cleanup_count = ROW_COUNT;

  v_generation := public.pay_workbench_scope_current_generation_v1();
  v_mode := CASE WHEN v_shadow THEN 'SHADOW' ELSE 'AUTHORITATIVE' END;

  SELECT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
      AND session_row.discarded_at_utc IS NULL
      AND session_row.replacement_session_id IS NULL
      AND COALESCE(session_row.scope_seed_complete, false)
      AND (
        (v_shadow AND COALESCE(session_row.scope_change_generation_shadow_checked, 0) < v_generation)
        OR
        (NOT v_shadow AND COALESCE(session_row.scope_change_generation_applied, 0) < v_generation)
      )
  ) INTO v_lag_exists;

  IF NOT v_lag_exists THEN
    RETURN jsonb_build_object(
      'ok', true,
      'enabled', true,
      'shadow_mode', v_shadow,
      'reconcile_mode', v_mode,
      'current_generation', v_generation,
      'lag_exists', false,
      'legacy_counter_repair_count', v_repair_count,
      'staging_cleanup_count', v_cleanup_count,
      'created', false
    );
  END IF;

  INSERT INTO public.banking_pay_workbench_jobs AS coordinator_job(
    job_type, status, priority, run_at_utc, attempt_count, max_attempts,
    dedupe_key, payload_json, created_at_utc, updated_at_utc,
    scope_change_generation, scope_change_tx_token
  ) VALUES (
    'WORKBENCH_SCOPE_RECONCILE', 'QUEUED', 40, clock_timestamp(), 0, 8,
    'WORKBENCH_SCOPE_RECONCILE:GLOBAL',
    jsonb_build_object(
      'run_mode', 'CONTINUOUS_SCOPE_RECONCILE',
      'reconcile_mode', v_mode,
      'target_generation', v_generation,
      'candidate_limit', 25,
      'session_limit', 1,
      'shadow_mode', v_shadow,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ),
    clock_timestamp(), clock_timestamp(), v_generation, NULL::uuid
  )
  ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
  DO NOTHING
  RETURNING coordinator_job.id INTO v_job_id;

  v_created := v_job_id IS NOT NULL;
  IF NOT v_created THEN
    SELECT active_job.id
    INTO v_job_id
    FROM public.banking_pay_workbench_jobs AS active_job
    WHERE active_job.dedupe_key = 'WORKBENCH_SCOPE_RECONCILE:GLOBAL'
      AND active_job.status IN ('QUEUED', 'RUNNING')
    ORDER BY active_job.created_at_utc
    LIMIT 1;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'enabled', true,
    'shadow_mode', v_shadow,
    'reconcile_mode', v_mode,
    'current_generation', v_generation,
    'lag_exists', true,
    'legacy_counter_repair_count', v_repair_count,
    'staging_cleanup_count', v_cleanup_count,
    'created', v_created,
    'job_id', CASE WHEN v_job_id IS NULL THEN NULL ELSE v_job_id::text END
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_item_snoozes_sync_identity_fields() FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_blocker_state_v1(uuid, bigint, uuid) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_admission_candidates_v1(uuid, jsonb, bigint) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_reconcile_ensure_v1() FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.pay_item_snoozes_sync_identity_fields() TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_blocker_state_v1(uuid, bigint, uuid) TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_admission_candidates_v1(uuid, jsonb, bigint) TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_reconcile_ensure_v1() TO service_role;
-- ---------------------------------------------------------------------------
-- Final coordinator, progress and snooze-fairness authorities.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_reconcile_drain_one_v1(
  p_candidate_limit integer DEFAULT 25,
  p_worker_id text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_limit integer := LEAST(GREATEST(COALESCE(p_candidate_limit, 25), 1), 50);
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_enabled boolean := true;
  v_config_shadow boolean := false;
  v_mode text := 'AUTHORITATIVE';
  v_target bigint := 0;
  v_current_generation bigint := 0;
  v_base_generation bigint := 0;
  v_cursor_generation bigint := 0;
  v_cursor_entity_key text := '';
  v_page_count integer := 0;
  v_has_more boolean := false;
  v_last_generation bigint := 0;
  v_last_entity_key text := '';
  v_shortlist jsonb := '[]'::jsonb;
  v_admission jsonb := '{}'::jsonb;
  v_eligible jsonb := '[]'::jsonb;
  v_inserted jsonb := '[]'::jsonb;
  v_inserted_count integer := 0;
  v_enqueue jsonb := '{}'::jsonb;
  v_blocker jsonb := '{}'::jsonb;
  v_upstream_active integer := 0;
  v_upstream_failed integer := 0;
  v_more_sessions boolean := false;
  v_already_in_scope integer := 0;
  v_would_admit integer := 0;
  v_would_reject integer := 0;
  v_shadow_summary jsonb := '{}'::jsonb;
  v_shadow_previous jsonb := '{}'::jsonb;
  v_shadow_sample jsonb := '[]'::jsonb;
  v_error_message text;
  v_error_detail text;
  v_error_state text;
BEGIN
  SELECT queued_job.*
  INTO v_job
  FROM public.banking_pay_workbench_jobs AS queued_job
  WHERE UPPER(BTRIM(COALESCE(queued_job.job_type, ''))) = 'WORKBENCH_SCOPE_RECONCILE'
    AND queued_job.status = 'QUEUED'
    AND queued_job.run_at_utc <= v_now
  ORDER BY queued_job.priority, queued_job.run_at_utc, queued_job.created_at_utc, queued_job.id
  FOR UPDATE SKIP LOCKED
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', true, 'processed', false, 'reason', 'NO_DUE_COORDINATOR');
  END IF;

  UPDATE public.banking_pay_workbench_jobs AS claimed_job
  SET status = 'RUNNING',
      started_at_utc = v_now,
      updated_at_utc = v_now,
      payload_json = COALESCE(claimed_job.payload_json, '{}'::jsonb)
        || jsonb_build_object('worker_id', COALESCE(NULLIF(BTRIM(p_worker_id), ''), 'SCOPE_RECONCILE'))
  WHERE claimed_job.id = v_job.id
  RETURNING claimed_job.* INTO v_job;

  BEGIN
    SELECT
      COALESCE(settings_row.banking_pay_workbench_scope_reconcile_enabled, true),
      COALESCE(settings_row.banking_pay_workbench_scope_reconcile_shadow_mode, false)
    INTO v_enabled, v_config_shadow
    FROM public.settings_defaults AS settings_row
    ORDER BY settings_row.id
    LIMIT 1;

    IF NOT COALESCE(v_enabled, true) THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'SUCCEEDED',
          completed_at_utc = v_now,
          started_at_utc = NULL::timestamptz,
          payload_json = COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            || jsonb_build_object('completion_reason', 'SCOPE_RECONCILIATION_DISABLED'),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
      RETURN jsonb_build_object('ok', true, 'processed', true, 'status', 'SUCCEEDED', 'reason', 'SCOPE_RECONCILIATION_DISABLED');
    END IF;

    v_mode := UPPER(BTRIM(COALESCE(
      v_job.payload_json->>'reconcile_mode',
      CASE WHEN v_config_shadow THEN 'SHADOW' ELSE 'AUTHORITATIVE' END
    )));
    IF v_mode NOT IN ('SHADOW', 'AUTHORITATIVE') THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_RECONCILE_MODE_INVALID'
        USING ERRCODE = '22023';
    END IF;

    IF v_config_shadow AND v_mode = 'AUTHORITATIVE' THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'QUEUED',
          run_at_utc = v_now,
          started_at_utc = NULL::timestamptz,
          payload_json = (COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            - 'active_session_id' - 'cursor_generation' - 'cursor_entity_key')
            || jsonb_build_object('reconcile_mode', 'SHADOW', 'shadow_mode', true),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
      RETURN jsonb_build_object('ok', true, 'processed', true, 'status', 'QUEUED', 'reason', 'CONVERTED_TO_SHADOW');
    END IF;

    v_current_generation := public.pay_workbench_scope_current_generation_v1();
    v_target := CASE
      WHEN COALESCE(v_job.payload_json->>'target_generation', '') ~ '^\d+$'
        THEN (v_job.payload_json->>'target_generation')::bigint
      ELSE 0
    END;
    v_target := GREATEST(v_target, v_current_generation);

    UPDATE public.banking_pay_workbench_jobs AS coordinator_job
    SET scope_change_generation = v_target,
        payload_json = COALESCE(coordinator_job.payload_json, '{}'::jsonb)
          || jsonb_build_object('target_generation', v_target, 'reconcile_mode', v_mode),
        updated_at_utc = v_now
    WHERE coordinator_job.id = v_job.id;

    IF COALESCE(v_job.payload_json->>'active_session_id', '')
         ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      SELECT session_row.*
      INTO v_session
      FROM public.banking_pay_workbench_sessions AS session_row
      WHERE session_row.id = (v_job.payload_json->>'active_session_id')::uuid
        AND UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
        AND session_row.discarded_at_utc IS NULL
        AND session_row.replacement_session_id IS NULL
        AND COALESCE(session_row.scope_seed_complete, false)
        AND (
          (v_mode = 'SHADOW' AND COALESCE(session_row.scope_change_generation_shadow_checked, 0) < v_target)
          OR
          (v_mode = 'AUTHORITATIVE' AND COALESCE(session_row.scope_change_generation_applied, 0) < v_target)
        )
      FOR UPDATE;
    END IF;

    IF v_session.id IS NULL THEN
      SELECT session_row.*
      INTO v_session
      FROM public.banking_pay_workbench_sessions AS session_row
      WHERE UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
        AND session_row.discarded_at_utc IS NULL
        AND session_row.replacement_session_id IS NULL
        AND COALESCE(session_row.scope_seed_complete, false)
        AND (
          (v_mode = 'SHADOW' AND COALESCE(session_row.scope_change_generation_shadow_checked, 0) < v_target)
          OR
          (v_mode = 'AUTHORITATIVE' AND COALESCE(session_row.scope_change_generation_applied, 0) < v_target)
        )
      ORDER BY session_row.session_signature, session_row.id
      FOR UPDATE SKIP LOCKED
      LIMIT 1;
    END IF;
    IF v_session.id IS NULL THEN
      SELECT generation_counter.seq
      INTO v_current_generation
      FROM public.app_change_counters AS generation_counter
      WHERE generation_counter.entity_key = 'pay_candidate_scope_generation'
      FOR UPDATE;

      SELECT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_sessions AS lagging_session
        WHERE UPPER(BTRIM(COALESCE(lagging_session.status, ''))) = 'OPEN'
          AND lagging_session.discarded_at_utc IS NULL
          AND lagging_session.replacement_session_id IS NULL
          AND COALESCE(lagging_session.scope_seed_complete, false)
          AND (
            (v_mode = 'SHADOW' AND COALESCE(lagging_session.scope_change_generation_shadow_checked, 0) < v_current_generation)
            OR
            (v_mode = 'AUTHORITATIVE' AND COALESCE(lagging_session.scope_change_generation_applied, 0) < v_current_generation)
          )
      ) INTO v_more_sessions;

      IF v_current_generation > v_target OR v_more_sessions THEN
        UPDATE public.banking_pay_workbench_jobs AS coordinator_job
        SET status = 'QUEUED',
            run_at_utc = v_now,
            started_at_utc = NULL::timestamptz,
            payload_json = (COALESCE(coordinator_job.payload_json, '{}'::jsonb)
              - 'active_session_id' - 'cursor_generation' - 'cursor_entity_key')
              || jsonb_build_object('target_generation', v_current_generation, 'reconcile_mode', v_mode),
            scope_change_generation = v_current_generation,
            updated_at_utc = v_now
        WHERE coordinator_job.id = v_job.id;
        RETURN jsonb_build_object('ok', true, 'processed', true, 'status', 'QUEUED', 'reason', 'NEW_GENERATION_OR_SESSION_LAG');
      END IF;

      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'SUCCEEDED',
          completed_at_utc = v_now,
          started_at_utc = NULL::timestamptz,
          payload_json = COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            || jsonb_build_object('completed_generation', v_target, 'completed_mode', v_mode),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
      RETURN jsonb_build_object(
        'ok', true, 'processed', true, 'status', 'SUCCEEDED',
        'target_generation', v_target, 'reconcile_mode', v_mode
      );
    END IF;

    v_base_generation := CASE
      WHEN v_mode = 'SHADOW' THEN COALESCE(v_session.scope_change_generation_shadow_checked, 0)
      ELSE COALESCE(v_session.scope_change_generation_applied, 0)
    END;
    v_cursor_generation := CASE
      WHEN COALESCE(v_job.payload_json->>'cursor_generation', '') ~ '^\d+$'
        THEN (v_job.payload_json->>'cursor_generation')::bigint
      ELSE v_base_generation
    END;
    v_cursor_entity_key := COALESCE(v_job.payload_json->>'cursor_entity_key', '');

    UPDATE public.banking_pay_workbench_sessions AS session_update
    SET scope_change_generation_target = CASE
          WHEN v_mode = 'AUTHORITATIVE' THEN v_target
          ELSE session_update.scope_change_generation_target
        END,
        progress_json = COALESCE(session_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'scope_reconcile_status', CASE WHEN v_mode = 'SHADOW' THEN 'SHADOW_RUNNING' ELSE 'RUNNING' END,
            'scope_reconcile_mode', v_mode,
            'scope_change_generation_target', v_target,
            'scope_change_generation_applied', COALESCE(session_update.scope_change_generation_applied, 0),
            'scope_change_generation_shadow_checked', COALESCE(session_update.scope_change_generation_shadow_checked, 0)
          ),
        updated_at_utc = v_now
    WHERE session_update.id = v_session.id;

    v_blocker := public.pay_workbench_scope_blocker_state_v1(v_session.id, v_target, NULL::uuid);
    v_upstream_active := COALESCE((v_blocker->>'upstream_active_count')::integer, 0);
    v_upstream_failed := COALESCE((v_blocker->>'upstream_unresolved_failure_count')::integer, 0);

    IF v_upstream_failed > 0 THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'QUEUED',
          run_at_utc = v_now + interval '30 seconds',
          started_at_utc = NULL::timestamptz,
          last_error_json = jsonb_build_object(
            'code', 'PAY_WORKBENCH_SCOPE_RECONCILIATION_UPSTREAM_FAILED',
            'failure_sample', COALESCE(v_blocker->'failure_sample', '[]'::jsonb),
            'target_generation', v_target
          ),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
      RETURN jsonb_build_object(
        'ok', false, 'processed', true, 'status', 'QUEUED',
        'reason', 'UPSTREAM_SCOPE_FAILURE_UNRESOLVED',
        'target_generation', v_target,
        'failure_sample', COALESCE(v_blocker->'failure_sample', '[]'::jsonb)
      );
    END IF;

    IF v_upstream_active > 0 THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'QUEUED',
          run_at_utc = v_now + interval '1 second',
          started_at_utc = NULL::timestamptz,
          payload_json = COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'target_generation', v_target,
              'active_session_id', v_session.id::text,
              'upstream_wait_count', v_upstream_active
            ),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
      RETURN jsonb_build_object(
        'ok', true, 'processed', true, 'status', 'QUEUED',
        'reason', 'WAITING_FOR_UPSTREAM', 'upstream_count', v_upstream_active
      );
    END IF;

    DROP TABLE IF EXISTS pg_temp._bpay_scope_reconcile_page;
    CREATE TEMP TABLE _bpay_scope_reconcile_page ON COMMIT DROP AS
    SELECT candidate_change.entity_key,
           candidate_change.scope_change_generation,
           SUBSTRING(candidate_change.entity_key FROM 15)::uuid AS candidate_id,
           ROW_NUMBER() OVER (
             ORDER BY candidate_change.scope_change_generation, candidate_change.entity_key
           ) AS page_ordinal
    FROM public.app_change_counters AS candidate_change
    WHERE candidate_change.entity_key LIKE 'pay_candidate:%'
      AND candidate_change.entity_key
            ~* '^pay_candidate:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND candidate_change.scope_change_generation > v_base_generation
      AND candidate_change.scope_change_generation <= v_target
      AND (
        candidate_change.scope_change_generation > v_cursor_generation
        OR (
          candidate_change.scope_change_generation = v_cursor_generation
          AND candidate_change.entity_key > v_cursor_entity_key
        )
      )
    ORDER BY candidate_change.scope_change_generation, candidate_change.entity_key
    LIMIT (v_limit + 1);

    SELECT COUNT(*)::integer,
           EXISTS (SELECT 1 FROM pg_temp._bpay_scope_reconcile_page WHERE page_ordinal > v_limit)
    INTO v_page_count, v_has_more
    FROM pg_temp._bpay_scope_reconcile_page
    WHERE page_ordinal <= v_limit;

    SELECT COUNT(*)::integer
    INTO v_already_in_scope
    FROM pg_temp._bpay_scope_reconcile_page AS page_row
    WHERE page_row.page_ordinal <= v_limit
      AND EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_session_scope AS existing_scope
        WHERE existing_scope.session_id = v_session.id
          AND existing_scope.candidate_id = page_row.candidate_id
      );

    SELECT COALESCE(
             jsonb_agg(to_jsonb(page_row.candidate_id::text) ORDER BY page_row.page_ordinal),
             '[]'::jsonb
           )
    INTO v_shortlist
    FROM pg_temp._bpay_scope_reconcile_page AS page_row
    WHERE page_row.page_ordinal <= v_limit
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_session_scope AS existing_scope
        WHERE existing_scope.session_id = v_session.id
          AND existing_scope.candidate_id = page_row.candidate_id
      );

    IF jsonb_array_length(v_shortlist) > 0 THEN
      v_admission := public.pay_workbench_scope_admission_candidates_v1(
        v_session.id,
        v_shortlist,
        v_target
      );
      v_eligible := CASE
        WHEN jsonb_typeof(v_admission->'candidate_ids') = 'array' THEN v_admission->'candidate_ids'
        ELSE '[]'::jsonb
      END;
    ELSE
      v_eligible := '[]'::jsonb;
    END IF;

    v_would_admit := jsonb_array_length(v_eligible);
    v_would_reject := GREATEST(jsonb_array_length(v_shortlist) - v_would_admit, 0);
    IF v_mode = 'AUTHORITATIVE' AND v_would_admit > 0 THEN
      WITH input_candidates AS (
        SELECT DISTINCT candidate_value.value::uuid AS candidate_id
        FROM jsonb_array_elements_text(v_eligible) AS candidate_value(value)
      ), base_ordinal AS (
        SELECT COALESCE(MAX(scope_row.scope_ordinal), 0)::bigint AS value
        FROM public.banking_pay_workbench_session_scope AS scope_row
        WHERE scope_row.session_id = v_session.id
      ), inserted_scope AS (
        INSERT INTO public.banking_pay_workbench_session_scope(
          session_id, candidate_id, scope_ordinal, status, pending_job_id,
          seeded, dirty, error_json, created_at_utc, updated_at_utc
        )
        SELECT v_session.id,
               input_candidates.candidate_id,
               base_ordinal.value + ROW_NUMBER() OVER (ORDER BY input_candidates.candidate_id),
               'PENDING', NULL::uuid, true, false, NULL::jsonb, v_now, v_now
        FROM input_candidates
        CROSS JOIN base_ordinal
        ON CONFLICT (session_id, candidate_id) DO NOTHING
        RETURNING candidate_id
      )
      SELECT COUNT(*)::integer,
             COALESCE(
               jsonb_agg(to_jsonb(inserted_scope.candidate_id::text) ORDER BY inserted_scope.candidate_id),
               '[]'::jsonb
             )
      INTO v_inserted_count, v_inserted
      FROM inserted_scope;

      IF v_inserted_count > 0 THEN
        v_enqueue := public.pay_workbench_enqueue_candidate_refresh_many(
          p_session_id => v_session.id,
          p_candidate_ids => v_inserted,
          p_reason => 'CONTINUOUS_SCOPE_RECONCILE_ADMISSION',
          p_actor_user_id => v_session.actor_user_id
        );
      END IF;
    END IF;

    SELECT page_row.scope_change_generation, page_row.entity_key
    INTO v_last_generation, v_last_entity_key
    FROM pg_temp._bpay_scope_reconcile_page AS page_row
    WHERE page_row.page_ordinal <= v_limit
    ORDER BY page_row.page_ordinal DESC
    LIMIT 1;

    v_shadow_previous := CASE
      WHEN jsonb_typeof(v_job.payload_json->'shadow_accumulator') = 'object'
        THEN v_job.payload_json->'shadow_accumulator'
      ELSE '{}'::jsonb
    END;

    SELECT COALESCE(jsonb_agg(sample_row.value ORDER BY sample_row.ordinality), '[]'::jsonb)
    INTO v_shadow_sample
    FROM jsonb_array_elements(
      COALESCE(v_shadow_previous->'candidate_sample', '[]'::jsonb)
      || COALESCE((
        SELECT jsonb_agg(to_jsonb(sample_candidate.value) ORDER BY sample_candidate.ordinality)
        FROM jsonb_array_elements_text(v_shortlist)
             WITH ORDINALITY AS sample_candidate(value, ordinality)
        WHERE sample_candidate.ordinality <= 25
      ), '[]'::jsonb)
    ) WITH ORDINALITY AS sample_row(value, ordinality)
    WHERE sample_row.ordinality <= 25;

    v_shadow_summary := jsonb_build_object(
      'target_generation', v_target,
      'candidates_considered', COALESCE((v_shadow_previous->>'candidates_considered')::integer, 0) + v_page_count,
      'already_in_scope', COALESCE((v_shadow_previous->>'already_in_scope')::integer, 0) + v_already_in_scope,
      'would_admit', COALESCE((v_shadow_previous->>'would_admit')::integer, 0) + v_would_admit,
      'would_reject', COALESCE((v_shadow_previous->>'would_reject')::integer, 0) + v_would_reject,
      'would_enqueue_refresh', COALESCE((v_shadow_previous->>'would_enqueue_refresh')::integer, 0) + v_would_admit,
      'candidate_sample', v_shadow_sample,
      'decision_hash', encode(
        extensions.digest(
          convert_to(v_shortlist::text || '|' || v_eligible::text || '|' || v_target::text, 'UTF8'),
          'sha256'
        ),
        'hex'
      )
    );

    IF v_has_more THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'QUEUED',
          run_at_utc = v_now,
          started_at_utc = NULL::timestamptz,
          payload_json = COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            || jsonb_build_object(
              'target_generation', v_target,
              'active_session_id', v_session.id::text,
              'cursor_generation', v_last_generation,
              'cursor_entity_key', v_last_entity_key,
              'candidate_limit', v_limit,
              'reconcile_mode', v_mode,
              'shadow_accumulator', CASE
                WHEN v_mode = 'SHADOW' THEN v_shadow_summary
                ELSE NULL::jsonb
              END,
              'last_shadow_page', CASE
                WHEN v_mode = 'SHADOW' THEN v_shadow_summary
                ELSE NULL::jsonb
              END
            ),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;

      RETURN jsonb_build_object(
        'ok', true,
        'processed', true,
        'status', 'QUEUED',
        'reconcile_mode', v_mode,
        'session_id', v_session.id::text,
        'target_generation', v_target,
        'page_count', v_page_count,
        'admitted_count', CASE WHEN v_mode = 'AUTHORITATIVE' THEN v_inserted_count ELSE 0 END,
        'shadow_summary', CASE WHEN v_mode = 'SHADOW' THEN v_shadow_summary ELSE NULL::jsonb END,
        'has_more', true
      );
    END IF;

    v_blocker := public.pay_workbench_scope_blocker_state_v1(v_session.id, v_target, NULL::uuid);
    v_upstream_active := COALESCE((v_blocker->>'upstream_active_count')::integer, 0);
    v_upstream_failed := COALESCE((v_blocker->>'upstream_unresolved_failure_count')::integer, 0);

    IF v_upstream_failed > 0 OR v_upstream_active > 0 THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'QUEUED',
          run_at_utc = v_now + CASE
            WHEN v_upstream_failed > 0 THEN interval '30 seconds'
            ELSE interval '1 second'
          END,
          started_at_utc = NULL::timestamptz,
          last_error_json = CASE
            WHEN v_upstream_failed > 0 THEN jsonb_build_object(
              'code', 'PAY_WORKBENCH_SCOPE_RECONCILIATION_UPSTREAM_FAILED',
              'failure_sample', COALESCE(v_blocker->'failure_sample', '[]'::jsonb),
              'target_generation', v_target
            )
            ELSE coordinator_job.last_error_json
          END,
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
      RETURN jsonb_build_object(
        'ok', v_upstream_failed = 0,
        'processed', true,
        'status', 'QUEUED',
        'reason', CASE
          WHEN v_upstream_failed > 0 THEN 'UPSTREAM_SCOPE_FAILURE_UNRESOLVED'
          ELSE 'UPSTREAM_APPEARED_BEFORE_ADVANCE'
        END
      );
    END IF;

    IF v_mode = 'SHADOW' THEN
      UPDATE public.banking_pay_workbench_sessions AS session_update
      SET scope_change_generation_shadow_checked = v_target,
          progress_json = COALESCE(session_update.progress_json, '{}'::jsonb)
            || jsonb_build_object(
              'scope_reconcile_status', 'SHADOW_SUCCEEDED',
              'scope_reconcile_mode', 'SHADOW',
              'scope_change_generation_shadow_checked', v_target,
              'scope_shadow_summary', v_shadow_summary,
              'scope_shadow_checked_at_utc', v_now::text
            ),
          progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
          progress_updated_at_utc = v_now,
          updated_at_utc = v_now
      WHERE session_update.id = v_session.id;
    ELSE
      UPDATE public.banking_pay_workbench_sessions AS session_update
      SET scope_change_generation_applied = v_target,
          scope_change_generation_target = v_target,
          progress_json = COALESCE(session_update.progress_json, '{}'::jsonb)
            || jsonb_build_object(
              'scope_reconcile_status', 'SUCCEEDED',
              'scope_reconcile_mode', 'AUTHORITATIVE',
              'scope_change_generation_target', v_target,
              'scope_change_generation_applied', v_target,
              'scope_reconciled_at_utc', v_now::text
            ),
          progress_counter_version = COALESCE(session_update.progress_counter_version, 0) + 1,
          progress_updated_at_utc = v_now,
          updated_at_utc = v_now
      WHERE session_update.id = v_session.id;
    END IF;

    SELECT generation_counter.seq
    INTO v_current_generation
    FROM public.app_change_counters AS generation_counter
    WHERE generation_counter.entity_key = 'pay_candidate_scope_generation'
    FOR UPDATE;

    SELECT EXISTS (
      SELECT 1
      FROM public.banking_pay_workbench_sessions AS lagging_session
      WHERE UPPER(BTRIM(COALESCE(lagging_session.status, ''))) = 'OPEN'
        AND lagging_session.discarded_at_utc IS NULL
        AND lagging_session.replacement_session_id IS NULL
        AND COALESCE(lagging_session.scope_seed_complete, false)
        AND (
          (v_mode = 'SHADOW' AND COALESCE(lagging_session.scope_change_generation_shadow_checked, 0) < v_current_generation)
          OR
          (v_mode = 'AUTHORITATIVE' AND COALESCE(lagging_session.scope_change_generation_applied, 0) < v_current_generation)
        )
    ) INTO v_more_sessions;

    IF v_current_generation > v_target OR v_more_sessions THEN
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'QUEUED',
          run_at_utc = v_now,
          started_at_utc = NULL::timestamptz,
          payload_json = (COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            - 'active_session_id' - 'cursor_generation' - 'cursor_entity_key')
            || jsonb_build_object('target_generation', v_current_generation, 'reconcile_mode', v_mode),
          scope_change_generation = v_current_generation,
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
    ELSE
      UPDATE public.banking_pay_workbench_jobs AS coordinator_job
      SET status = 'SUCCEEDED',
          completed_at_utc = v_now,
          started_at_utc = NULL::timestamptz,
          payload_json = COALESCE(coordinator_job.payload_json, '{}'::jsonb)
            || jsonb_build_object('completed_generation', v_target, 'completed_mode', v_mode),
          updated_at_utc = v_now
      WHERE coordinator_job.id = v_job.id;
    END IF;
    RETURN jsonb_build_object(
      'ok', true,
      'processed', true,
      'status', CASE
        WHEN v_current_generation > v_target OR v_more_sessions THEN 'QUEUED'
        ELSE 'SUCCEEDED'
      END,
      'reconcile_mode', v_mode,
      'session_id', v_session.id::text,
      'target_generation', v_target,
      'current_generation', v_current_generation,
      'page_count', v_page_count,
      'admitted_count', CASE WHEN v_mode = 'AUTHORITATIVE' THEN v_inserted_count ELSE 0 END,
      'shadow_summary', CASE WHEN v_mode = 'SHADOW' THEN v_shadow_summary ELSE NULL::jsonb END,
      'has_more', false,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
      v_error_message = MESSAGE_TEXT,
      v_error_detail = PG_EXCEPTION_DETAIL,
      v_error_state = RETURNED_SQLSTATE;

    UPDATE public.banking_pay_workbench_jobs AS failed_coordinator
    SET attempt_count = COALESCE(failed_coordinator.attempt_count, 0) + 1,
        status = CASE
          WHEN COALESCE(failed_coordinator.attempt_count, 0) + 1 >= COALESCE(failed_coordinator.max_attempts, 8)
            THEN 'FAILED'
          ELSE 'QUEUED'
        END,
        run_at_utc = v_now + make_interval(
          secs => LEAST(900, 30 * GREATEST(COALESCE(failed_coordinator.attempt_count, 0) + 1, 1))
        ),
        started_at_utc = NULL::timestamptz,
        failed_at_utc = CASE
          WHEN COALESCE(failed_coordinator.attempt_count, 0) + 1 >= COALESCE(failed_coordinator.max_attempts, 8)
            THEN v_now
          ELSE NULL::timestamptz
        END,
        last_error_json = jsonb_strip_nulls(jsonb_build_object(
          'code', COALESCE(NULLIF(v_error_state, ''), 'PAY_WORKBENCH_SCOPE_RECONCILE_FAILED'),
          'message', v_error_message,
          'detail', v_error_detail,
          'failed_at_utc', v_now::text
        )),
        updated_at_utc = v_now
    WHERE failed_coordinator.id = v_job.id;

    RETURN jsonb_build_object(
      'ok', false,
      'processed', true,
      'job_id', v_job.id::text,
      'code', COALESCE(NULLIF(v_error_state, ''), 'PAY_WORKBENCH_SCOPE_RECONCILE_FAILED'),
      'message', v_error_message
    );
  END;
END;
$function$;

CREATE OR REPLACE FUNCTION public.pay_workbench_scope_progress_v1(p_session_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_current bigint := 0;
  v_shadow boolean := false;
  v_blocker jsonb := '{}'::jsonb;
  v_session_active integer := 0;
  v_session_failed integer := 0;
  v_upstream_active integer := 0;
  v_upstream_failed integer := 0;
  v_display_ready boolean := false;
  v_draft_safe boolean := false;
  v_reason text;
BEGIN
  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object('ok', false, 'code', 'WORKBENCH_SESSION_NOT_FOUND');
  END IF;

  SELECT COALESCE(settings_row.banking_pay_workbench_scope_reconcile_shadow_mode, false)
  INTO v_shadow
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  v_current := public.pay_workbench_scope_current_generation_v1();
  v_blocker := public.pay_workbench_scope_blocker_state_v1(p_session_id, v_current, NULL::uuid);
  v_session_active := COALESCE((v_blocker->>'session_active_count')::integer, 0);
  v_session_failed := COALESCE((v_blocker->>'session_unresolved_failure_count')::integer, 0)
    + COALESCE((v_blocker->>'session_scope_failed_count')::integer, 0)
    + COALESCE((v_blocker->>'session_line_failed_count')::integer, 0);
  v_upstream_active := COALESCE((v_blocker->>'upstream_active_count')::integer, 0);
  v_upstream_failed := COALESCE((v_blocker->>'upstream_unresolved_failure_count')::integer, 0);

  v_display_ready := UPPER(BTRIM(COALESCE(v_session.status, ''))) = 'OPEN'
    AND v_session.discarded_at_utc IS NULL
    AND v_session.replacement_session_id IS NULL
    AND COALESCE(v_session.scope_seed_complete, false)
    AND (
      COALESCE(v_session.preview_row_count, 0) > 0
      OR (
        v_session_active = 0
        AND COALESCE(v_session.scope_pending_count, 0) = 0
        AND COALESCE(v_session.scope_failed_count, 0) = 0
        AND COALESCE(v_session.line_units_pending, 0) = 0
        AND COALESCE(v_session.line_units_failed, 0) = 0
      )
    );

  v_draft_safe := v_display_ready
    AND NOT v_shadow
    AND COALESCE(v_session.scope_change_generation_applied, 0) = v_current
    AND v_session_active = 0
    AND v_session_failed = 0
    AND v_upstream_active = 0
    AND v_upstream_failed = 0;

  v_reason := CASE
    WHEN NOT v_display_ready THEN 'INITIAL_SCOPE_NOT_READY'
    WHEN v_shadow THEN 'SCOPE_RECONCILIATION_SHADOW_MODE'
    WHEN v_upstream_failed > 0 THEN 'UPSTREAM_SCOPE_FAILURE_UNRESOLVED'
    WHEN v_session_failed > 0 THEN 'CANDIDATE_REFRESH_FAILED'
    WHEN COALESCE(v_session.scope_change_generation_applied, 0) < v_current THEN 'SCOPE_RECONCILIATION_REQUIRED'
    WHEN v_upstream_active > 0 THEN 'UPSTREAM_SCOPE_EXPANSION_IN_PROGRESS'
    WHEN v_session_active > 0 THEN 'CANDIDATE_REFRESH_IN_PROGRESS'
    ELSE NULL::text
  END;

  RETURN jsonb_build_object(
    'ok', true,
    'display_ready', v_display_ready,
    'draft_safe', v_draft_safe,
    'draft_block_reason_code', v_reason,
    'scope_change_generation_current', v_current,
    'scope_change_generation_target', COALESCE(v_session.scope_change_generation_target, 0),
    'scope_change_generation_applied', COALESCE(v_session.scope_change_generation_applied, 0),
    'scope_change_generation_shadow_checked', COALESCE(v_session.scope_change_generation_shadow_checked, 0),
    'scope_change_generation_lag', GREATEST(v_current - COALESCE(v_session.scope_change_generation_applied, 0), 0),
    'scope_reconcile_status', COALESCE(v_session.progress_json->>'scope_reconcile_status', 'IDLE'),
    'scope_reconcile_mode', CASE WHEN v_shadow THEN 'SHADOW' ELSE 'AUTHORITATIVE' END,
    'scope_reconcile_error', CASE
      WHEN v_upstream_failed > 0 THEN 'A background Banking Pay scope update failed and has not yet been recovered.'
      WHEN v_session_failed > 0 THEN 'A candidate payment refresh failed.'
      ELSE NULL::text
    END,
    'scope_blocker_active_sample', COALESCE(v_blocker->'active_sample', '[]'::jsonb),
    'scope_blocker_failure_sample', COALESCE(v_blocker->'failure_sample', '[]'::jsonb)
  );
END;
$function$;
CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_expired_snooze_refreshes_v1(
  p_session_id uuid DEFAULT NULL::uuid,
  p_session_limit integer DEFAULT 25,
  p_candidate_limit integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public', 'pg_catalog', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_date_context jsonb := public.pay_banking_official_date_context_v1(NULL::timestamptz);
  v_today_uk date;
  v_current_official_pay_date date;
  v_limit integer := LEAST(GREATEST(COALESCE(p_candidate_limit, 100), 1), 100);
  v_actor_user_id uuid;
  v_due record;
  v_transition jsonb := '{}'::jsonb;
  v_enqueue jsonb := '{}'::jsonb;
  v_examined integer := 0;
  v_changed integer := 0;
  v_unchanged integer := 0;
  v_marked integer := 0;
  v_marker_rows integer := 0;
  v_failed integer := 0;
  v_enqueued integer := 0;
  v_more_due boolean := false;
  v_results jsonb := '[]'::jsonb;
BEGIN
  v_today_uk := (v_date_context->>'business_date')::date;
  v_current_official_pay_date := (v_date_context->>'current_official_pay_date')::date;

  SELECT session_row.actor_user_id
  INTO v_actor_user_id
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
    AND session_row.discarded_at_utc IS NULL
    AND session_row.replacement_session_id IS NULL
    AND (p_session_id IS NULL OR session_row.id = p_session_id)
  ORDER BY session_row.updated_at_utc DESC, session_row.id
  LIMIT 1;

  IF v_actor_user_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', true,
      'processed_session_count', 0,
      'due_examined_count', 0,
      'state_changed_count', 0,
      'unchanged_due_count', 0,
      'check_marker_updated_count', 0,
      'transition_failed_count', 0,
      'has_more_unchecked_due', false,
      'reason', 'NO_CURRENT_OPEN_SESSION'
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_unchecked_due_snoozes;
  CREATE TEMP TABLE _bpay_unchecked_due_snoozes ON COMMIT DROP AS
  WITH locked_due AS (
    SELECT snooze_row.id
    FROM public.pay_item_snoozes AS snooze_row
    WHERE snooze_row.cleared_at_utc IS NULL
      AND snooze_row.cancelled_at_utc IS NULL
      AND snooze_row.snooze_until_date IS NOT NULL
      AND snooze_row.snooze_until_date < v_today_uk
      AND snooze_row.natural_expiry_source_fingerprint IS NOT NULL
      AND snooze_row.natural_expiry_checked_fingerprint
            IS DISTINCT FROM snooze_row.natural_expiry_source_fingerprint
    ORDER BY snooze_row.snooze_until_date, snooze_row.candidate_id, snooze_row.id
    FOR UPDATE SKIP LOCKED
    LIMIT v_limit
  )
  SELECT snooze_row.id,
         snooze_row.candidate_id,
         snooze_row.timesheet_id,
         snooze_row.snooze_until_date,
         snooze_row.natural_expiry_source_fingerprint
  FROM public.pay_item_snoozes AS snooze_row
  JOIN locked_due ON locked_due.id = snooze_row.id
  ORDER BY snooze_row.snooze_until_date, snooze_row.candidate_id, snooze_row.id;

  FOR v_due IN
    SELECT due_snooze.*
    FROM pg_temp._bpay_unchecked_due_snoozes AS due_snooze
    ORDER BY due_snooze.snooze_until_date, due_snooze.candidate_id, due_snooze.id
  LOOP
    BEGIN
      v_examined := v_examined + 1;
      v_transition := public.pay_snooze_resolution_transition_v1(
        p_snooze_id => v_due.id,
        p_actor_user_id => v_actor_user_id,
        p_transition_reason => 'NATURAL_EXPIRY'
      );

      UPDATE public.pay_item_snoozes AS snooze_update
      SET natural_expiry_checked_fingerprint = v_due.natural_expiry_source_fingerprint,
          natural_expiry_checked_at_utc = v_now,
          natural_expiry_state_changed = LOWER(BTRIM(COALESCE(v_transition->>'state_changed', 'false')))
            IN ('true', 't', '1', 'yes', 'y', 'on'),
          natural_expiry_result_code = LEFT(COALESCE(
            NULLIF(BTRIM(v_transition->>'code'), ''),
            NULLIF(BTRIM(v_transition->>'reason'), ''),
            'NATURAL_EXPIRY_EVALUATED'
          ), 200)
      WHERE snooze_update.id = v_due.id
        AND snooze_update.natural_expiry_source_fingerprint = v_due.natural_expiry_source_fingerprint;
      GET DIAGNOSTICS v_marker_rows = ROW_COUNT;
      v_marked := v_marked + v_marker_rows;

      IF LOWER(BTRIM(COALESCE(v_transition->>'state_changed', 'false')))
           IN ('true', 't', '1', 'yes', 'y', 'on') THEN
        v_changed := v_changed + 1;
        v_enqueue := public.pay_workbench_dirty_event_enqueue(
          p_job_type => 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
          p_scope_kind => 'CANDIDATE',
          p_scope_id => v_due.candidate_id::text,
          p_candidate_id => v_due.candidate_id,
          p_targeted_timesheet_ids => CASE
            WHEN v_due.timesheet_id IS NULL THEN ARRAY[]::uuid[]
            ELSE ARRAY[v_due.timesheet_id]::uuid[]
          END,
          p_linked_timesheet_ids => ARRAY[]::uuid[],
          p_payload_json => jsonb_build_object(
            'refresh_scope_kind', CASE
              WHEN v_due.timesheet_id IS NULL THEN 'CANDIDATE_FULL_LIVE'
              ELSE 'TARGETED_TIMESHEETS'
            END,
            'snooze_id', v_due.id::text,
            'london_current_date', v_today_uk::text,
            'current_official_pay_date', v_current_official_pay_date::text,
            'reason', 'SNOOZE_EXPIRED_LONDON_DATE',
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
          ),
          p_reason => 'SNOOZE_EXPIRED_LONDON_DATE',
          p_priority => -1000,
          p_run_at_utc => v_now
        );
        v_enqueued := v_enqueued + 1;
        v_results := v_results || jsonb_build_array(jsonb_build_object(
          'candidate_id', v_due.candidate_id::text,
          'snooze_id', v_due.id::text,
          'job_id', v_enqueue->>'job_id'
        ));
      ELSE
        v_unchanged := v_unchanged + 1;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      v_failed := v_failed + 1;
      v_results := v_results || jsonb_build_array(jsonb_build_object(
        'snooze_id', v_due.id::text,
        'transition_failed', true,
        'sqlstate', SQLSTATE
      ));
    END;
  END LOOP;

  SELECT EXISTS (
    SELECT 1
    FROM public.pay_item_snoozes AS snooze_row
    WHERE snooze_row.cleared_at_utc IS NULL
      AND snooze_row.cancelled_at_utc IS NULL
      AND snooze_row.snooze_until_date IS NOT NULL
      AND snooze_row.snooze_until_date < v_today_uk
      AND snooze_row.natural_expiry_source_fingerprint IS NOT NULL
      AND snooze_row.natural_expiry_checked_fingerprint
            IS DISTINCT FROM snooze_row.natural_expiry_source_fingerprint
  ) INTO v_more_due;

  RETURN jsonb_build_object(
    'ok', v_failed = 0,
    'global_due_scan', true,
    'processed_session_count', 1,
    'due_examined_count', v_examined,
    'state_changed_count', v_changed,
    'unchanged_due_count', v_unchanged,
    'check_marker_updated_count', v_marked,
    'transition_failed_count', v_failed,
    'enqueued_count', v_enqueued,
    'has_more_unchecked_due', v_more_due,
    'work_limit', v_limit,
    'results', v_results
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_workbench_scope_reconcile_drain_one_v1(integer, text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_progress_v1(uuid) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.pay_workbench_enqueue_expired_snooze_refreshes_v1(uuid, integer, integer) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_reconcile_drain_one_v1(integer, text) TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_progress_v1(uuid) TO service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_enqueue_expired_snooze_refreshes_v1(uuid, integer, integer) TO service_role;
DO $snooze_backfill$
DECLARE
  v_rows integer := 0;
BEGIN
  LOOP
    WITH backfill_page AS (
      SELECT snooze_row.id,
             encode(
               extensions.digest(
                 convert_to(
                   jsonb_build_array(
                     snooze_row.id::text,
                     COALESCE(snooze_row.candidate_id::text, ''),
                     COALESCE(snooze_row.timesheet_id::text, ''),
                     COALESCE(snooze_row.booking_id::text, ''),
                     COALESCE(snooze_row.segment_id::text, ''),
                     COALESCE(BTRIM(snooze_row.segment_stable_key), ''),
                     COALESCE(BTRIM(snooze_row.source_ref), ''),
                     UPPER(BTRIM(COALESCE(snooze_row.snooze_kind, ''))),
                     COALESCE(snooze_row.snooze_until_date::text, '')
                   )::text,
                   'UTF8'
                 ),
                 'sha256'
               ),
               'hex'
             ) AS canonical_fingerprint
      FROM public.pay_item_snoozes AS snooze_row
      WHERE snooze_row.natural_expiry_source_fingerprint IS NULL
         OR snooze_row.natural_expiry_source_fingerprint IS DISTINCT FROM
              encode(
                extensions.digest(
                  convert_to(
                    jsonb_build_array(
                      snooze_row.id::text,
                      COALESCE(snooze_row.candidate_id::text, ''),
                      COALESCE(snooze_row.timesheet_id::text, ''),
                      COALESCE(snooze_row.booking_id::text, ''),
                      COALESCE(snooze_row.segment_id::text, ''),
                      COALESCE(BTRIM(snooze_row.segment_stable_key), ''),
                      COALESCE(BTRIM(snooze_row.source_ref), ''),
                      UPPER(BTRIM(COALESCE(snooze_row.snooze_kind, ''))),
                      COALESCE(snooze_row.snooze_until_date::text, '')
                    )::text,
                    'UTF8'
                  ),
                  'sha256'
                ),
                'hex'
              )
      ORDER BY snooze_row.id
      LIMIT 100
      FOR UPDATE SKIP LOCKED
    )
    UPDATE public.pay_item_snoozes AS snooze_update
    SET natural_expiry_source_fingerprint = backfill_page.canonical_fingerprint
    FROM backfill_page
    WHERE snooze_update.id = backfill_page.id;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    EXIT WHEN v_rows = 0;
  END LOOP;
END
$snooze_backfill$;

CREATE INDEX IF NOT EXISTS idx_pay_item_snoozes_unchecked_natural_expiry
ON public.pay_item_snoozes (
  snooze_until_date,
  candidate_id,
  id
)
WHERE snooze_until_date IS NOT NULL
  AND cleared_at_utc IS NULL
  AND cancelled_at_utc IS NULL
  AND natural_expiry_source_fingerprint IS NOT NULL
  AND natural_expiry_checked_fingerprint IS DISTINCT FROM natural_expiry_source_fingerprint;

DO $completion_verification$
DECLARE
  v_definition text;
BEGIN
  SELECT pg_get_functiondef(
    'public.pay_workbench_scope_reconcile_drain_one_v1(integer,text)'::regprocedure
  ) INTO v_definition;
  IF POSITION('scope_change_generation_shadow_checked' IN v_definition) = 0
     OR POSITION('pay_workbench_scope_blocker_state_v1' IN v_definition) = 0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_COORDINATOR_COMPLETION_NOT_EFFECTIVE';
  END IF;

  SELECT pg_get_functiondef(
    'public.pay_workbench_enqueue_expired_snooze_refreshes_v1(uuid,integer,integer)'::regprocedure
  ) INTO v_definition;
  IF POSITION('natural_expiry_checked_fingerprint' IN v_definition) = 0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SNOOZE_FAIRNESS_NOT_EFFECTIVE';
  END IF;
END
$completion_verification$;
