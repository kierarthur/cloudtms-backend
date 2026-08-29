-- Repair the one TEST Workbench invalidation staged while the historical
-- continuous-scope finaliser had temporarily overwritten the bounded-scope
-- finaliser. The exact durable job and transaction evidence are required;
-- this migration never creates, completes, or requeues work.
-- Policy X: pre-draft freshness metadata only. No economic, payment,
-- settlement, provider, remittance, or frozen artefact is changed.

DO $migration$
DECLARE
  v_job_id constant uuid := '8d441fff-4153-413c-a522-72b6903a754f'::uuid;
  v_candidate_id constant uuid := 'bfdc14ec-82a6-566c-b6d5-bf760ecaf030'::uuid;
  v_expected_generation constant bigint := 96;
  v_expected_target_count constant integer := 9;
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_tx_token uuid;
  v_target_count integer;
  v_owned_state_count integer;
BEGIN
  SELECT *
  INTO v_job
  FROM public.banking_pay_workbench_jobs
  WHERE id = v_job_id
  FOR UPDATE;

  IF NOT FOUND
     OR v_job.job_type <> 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
     OR v_job.candidate_id IS DISTINCT FROM v_candidate_id
     OR v_job.scope_change_generation IS DISTINCT FROM v_expected_generation
     OR COALESCE((v_job.payload_json->>'bounded_scope_state_precedes_job')::boolean, false) IS NOT TRUE
     OR v_job.status NOT IN ('QUEUED', 'RUNNING') THEN
    RAISE EXCEPTION 'BANKING_PAY_SCOPE_GENERATION_GAP_JOB_PROOF_FAILED'
      USING ERRCODE = 'P0001';
  END IF;

  BEGIN
    v_tx_token := NULLIF(BTRIM(COALESCE(
      v_job.payload_json->>'scope_change_tx_token',
      ''
    )), '')::uuid;
  EXCEPTION WHEN invalid_text_representation THEN
    RAISE EXCEPTION 'BANKING_PAY_SCOPE_GENERATION_GAP_TOKEN_INVALID'
      USING ERRCODE = 'P0001';
  END;

  IF v_tx_token IS NULL OR NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_scope_change_transactions AS scope_tx
    WHERE scope_tx.tx_token = v_tx_token
      AND scope_tx.state = 'FINALIZED'
      AND scope_tx.allocated_generation = v_expected_generation
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_SCOPE_GENERATION_GAP_TRANSACTION_PROOF_FAILED'
      USING ERRCODE = 'P0001';
  END IF;

  CREATE TEMP TABLE pg_temp._bpay_scope_generation_gap_targets(
    timesheet_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  INSERT INTO pg_temp._bpay_scope_generation_gap_targets(timesheet_id)
  SELECT DISTINCT target_value::uuid
  FROM (
    SELECT jsonb_array_elements_text(
      CASE WHEN jsonb_typeof(v_job.payload_json->'targeted_timesheet_ids') = 'array'
           THEN v_job.payload_json->'targeted_timesheet_ids'
           ELSE '[]'::jsonb END
    ) AS target_value
    UNION ALL
    SELECT jsonb_array_elements_text(
      CASE WHEN jsonb_typeof(v_job.payload_json->'linked_timesheet_ids') = 'array'
           THEN v_job.payload_json->'linked_timesheet_ids'
           ELSE '[]'::jsonb END
    ) AS target_value
  ) AS target_values
  WHERE target_value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ON CONFLICT DO NOTHING;

  SELECT COUNT(*)::integer
  INTO v_target_count
  FROM pg_temp._bpay_scope_generation_gap_targets;

  SELECT COUNT(*)::integer
  INTO v_owned_state_count
  FROM pg_temp._bpay_scope_generation_gap_targets AS target
  JOIN private.banking_pay_workbench_timesheet_scope_state AS scope_state
    ON scope_state.timesheet_id = target.timesheet_id
   AND scope_state.candidate_id = v_candidate_id;

  IF v_target_count <> v_expected_target_count
     OR v_owned_state_count <> v_expected_target_count THEN
    RAISE EXCEPTION 'BANKING_PAY_SCOPE_GENERATION_GAP_TARGET_PROOF_FAILED'
      USING ERRCODE = 'P0001';
  END IF;

  UPDATE private.banking_pay_workbench_candidate_scope_registry AS registry
  SET dirty_generation = GREATEST(registry.dirty_generation, v_expected_generation),
      last_scope_change_tx_token = CASE
        WHEN registry.last_scope_change_tx_token = v_tx_token THEN NULL::uuid
        ELSE registry.last_scope_change_tx_token
      END,
      updated_at_utc = clock_timestamp()
  WHERE registry.candidate_id = v_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANKING_PAY_SCOPE_GENERATION_GAP_REGISTRY_NOT_FOUND'
      USING ERRCODE = 'P0001';
  END IF;

  UPDATE private.banking_pay_workbench_timesheet_scope_state AS scope_state
  SET economic_state = 'DIRTY',
      dirty_generation = GREATEST(scope_state.dirty_generation, v_expected_generation),
      last_scope_change_tx_token = CASE
        WHEN scope_state.last_scope_change_tx_token = v_tx_token THEN NULL::uuid
        ELSE scope_state.last_scope_change_tx_token
      END,
      closed_at_utc = NULL,
      updated_at_utc = clock_timestamp()
  FROM pg_temp._bpay_scope_generation_gap_targets AS target
  WHERE scope_state.timesheet_id = target.timesheet_id
    AND scope_state.candidate_id = v_candidate_id;

  IF EXISTS (
    SELECT 1
    FROM private.banking_pay_workbench_candidate_scope_registry AS registry
    WHERE registry.candidate_id = v_candidate_id
      AND registry.dirty_generation < v_expected_generation
  ) OR EXISTS (
    SELECT 1
    FROM pg_temp._bpay_scope_generation_gap_targets AS target
    JOIN private.banking_pay_workbench_timesheet_scope_state AS scope_state
      ON scope_state.timesheet_id = target.timesheet_id
     AND scope_state.candidate_id = v_candidate_id
    WHERE scope_state.dirty_generation < v_expected_generation
       OR scope_state.economic_state <> 'DIRTY'
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_SCOPE_GENERATION_GAP_REPAIR_NOT_PROVED'
      USING ERRCODE = 'P0001';
  END IF;
END;
$migration$;
