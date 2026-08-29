-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.pay_workbench_scope_change_finalize_trg_v1()
-- Installed pg_get_functiondef MD5: eae95763e85538f8f32bfb4e25a1582d
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.pay_workbench_scope_change_finalize_trg_v1()
 RETURNS trigger
 LANGUAGE plpgsql
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

  -- Bounded-scope metadata is locked candidate-first, then timesheet, before
  -- queue/counter finalisation.  This is the canonical invalidation lock order.
  PERFORM 1
  FROM private.banking_pay_workbench_candidate_scope_registry AS registry
  WHERE registry.last_scope_change_tx_token=NEW.tx_token
  ORDER BY registry.candidate_id
  FOR UPDATE;

  PERFORM 1
  FROM private.banking_pay_workbench_timesheet_scope_state AS scope_state
  WHERE scope_state.last_scope_change_tx_token=NEW.tx_token
  ORDER BY scope_state.candidate_id,scope_state.timesheet_id
  FOR UPDATE;

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

  UPDATE private.banking_pay_workbench_candidate_scope_registry AS registry
  SET dirty_generation=GREATEST(registry.dirty_generation,v_generation),
      last_scope_change_tx_token=NULL,updated_at_utc=clock_timestamp()
  WHERE registry.last_scope_change_tx_token=NEW.tx_token;

  UPDATE private.banking_pay_workbench_timesheet_scope_state AS scope_state
  SET economic_state='DIRTY',dirty_generation=GREATEST(scope_state.dirty_generation,v_generation),
      last_scope_change_tx_token=NULL,closed_at_utc=NULL,updated_at_utc=clock_timestamp()
  WHERE scope_state.last_scope_change_tx_token=NEW.tx_token;

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

  IF EXISTS(SELECT 1 FROM private.banking_pay_workbench_candidate_scope_registry
      WHERE last_scope_change_tx_token=NEW.tx_token)
     OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_timesheet_scope_state
      WHERE last_scope_change_tx_token=NEW.tx_token)
     OR EXISTS(SELECT 1 FROM public.app_change_counters
      WHERE scope_change_tx_token=NEW.tx_token)
     OR EXISTS(SELECT 1 FROM public.banking_pay_workbench_jobs
      WHERE scope_change_tx_token=NEW.tx_token
        AND UPPER(BTRIM(COALESCE(job_type,'')))<>'WORKBENCH_SCOPE_RECONCILE') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_TRANSACTION_TOKEN_UNRESOLVED'
      USING ERRCODE='P0001';
  END IF;

  UPDATE public.banking_pay_scope_change_transactions AS scope_tx
  SET state = 'FINALIZED',
      allocated_generation = v_generation,
      finalized_at_utc = clock_timestamp()
  WHERE scope_tx.tx_token = NEW.tx_token;

  RETURN NEW;
END;
$function$;

ALTER FUNCTION public.pay_workbench_scope_change_finalize_trg_v1() OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_scope_change_finalize_trg_v1() FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_change_finalize_trg_v1() TO postgres;
GRANT EXECUTE ON FUNCTION public.pay_workbench_scope_change_finalize_trg_v1() TO service_role;
