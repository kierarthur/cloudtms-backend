\set ON_ERROR_STOP on

-- Rollback-contained state-machine proof for Candidate DIRTY_APPLY cohorting.
-- A separate 0521 Node harness owns the true simultaneous-worker assertion.
BEGIN;
SET LOCAL statement_timeout = '120s';
SET LOCAL lock_timeout = '5s';

SET LOCAL ROLE service_role;

CREATE TEMP TABLE pg_temp._bpay_dirty_cohort_poison_observed(
  marker text NOT NULL
) ON COMMIT DROP;

CREATE OR REPLACE FUNCTION pg_temp._bpay_dirty_cohort_poison_trg()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO pg_temp._bpay_dirty_cohort_poison_observed(marker)
  VALUES (TG_TABLE_NAME);
  RETURN NEW;
END;
$function$;

-- HOSTILE_TEMP_TABLE_PRECREATION: the prior IF NOT EXISTS/TRUNCATE pattern
-- would either inherit these triggers or fail against these hostile schemas.
CREATE TEMP TABLE pg_temp._bpay_candidate_dirty_cohort_members_v1(
  hostile_payload text
) ON COMMIT DROP;
CREATE TRIGGER hostile_members_insert
BEFORE INSERT ON pg_temp._bpay_candidate_dirty_cohort_members_v1
FOR EACH ROW EXECUTE FUNCTION pg_temp._bpay_dirty_cohort_poison_trg();

CREATE TEMP TABLE pg_temp._bpay_candidate_dirty_cohort_roots_v1(
  hostile_payload text
) ON COMMIT DROP;
CREATE TRIGGER hostile_roots_insert
BEFORE INSERT ON pg_temp._bpay_candidate_dirty_cohort_roots_v1
FOR EACH ROW EXECUTE FUNCTION pg_temp._bpay_dirty_cohort_poison_trg();

RESET ROLE;

DO $verification$
DECLARE
  v_prefix text := 'BPAY-DIRTY-COHORT-0521:' || pg_catalog.gen_random_uuid()::text;
  v_all_candidate uuid := pg_catalog.gen_random_uuid();
  v_disjoint_candidate uuid := pg_catalog.gen_random_uuid();
  v_scale_candidate uuid := pg_catalog.gen_random_uuid();
  v_drift_candidate uuid := pg_catalog.gen_random_uuid();
  v_correction_candidate uuid := pg_catalog.gen_random_uuid();
  v_late_candidate uuid := pg_catalog.gen_random_uuid();
  v_all_timesheet uuid := pg_catalog.gen_random_uuid();
  v_disjoint_x uuid := pg_catalog.gen_random_uuid();
  v_disjoint_y uuid := pg_catalog.gen_random_uuid();
  v_drift_timesheet uuid := pg_catalog.gen_random_uuid();
  v_correction_regular_timesheet uuid := pg_catalog.gen_random_uuid();
  v_correction_owned_timesheet uuid := pg_catalog.gen_random_uuid();
  v_late_x uuid := pg_catalog.gen_random_uuid();
  v_late_y uuid := pg_catalog.gen_random_uuid();
  v_scale_timesheets uuid[] := ARRAY[
    pg_catalog.gen_random_uuid(), pg_catalog.gen_random_uuid(),
    pg_catalog.gen_random_uuid(), pg_catalog.gen_random_uuid(),
    pg_catalog.gen_random_uuid(), pg_catalog.gen_random_uuid(),
    pg_catalog.gen_random_uuid()
  ];
  v_all_job uuid := pg_catalog.gen_random_uuid();
  v_target_job uuid := pg_catalog.gen_random_uuid();
  v_disjoint_x_job uuid := pg_catalog.gen_random_uuid();
  v_disjoint_y_job uuid := pg_catalog.gen_random_uuid();
  v_drift_job uuid := pg_catalog.gen_random_uuid();
  v_correction_regular_job uuid := pg_catalog.gen_random_uuid();
  v_correction_owned_job uuid := pg_catalog.gen_random_uuid();
  v_late_x_job uuid := pg_catalog.gen_random_uuid();
  v_late_y_job uuid := pg_catalog.gen_random_uuid();
  v_batch_id uuid := pg_catalog.gen_random_uuid();
  v_batch_candidate_id uuid := pg_catalog.gen_random_uuid();
  v_correction_request_id uuid := pg_catalog.gen_random_uuid();
  v_correction_operation_id uuid := pg_catalog.gen_random_uuid();
  v_correction_token uuid;
  v_correction_context jsonb;
  v_correction_context_digest text;
  v_result jsonb := '{}'::jsonb;
  v_second_result jsonb := '{}'::jsonb;
  v_job_before jsonb;
  v_job_after jsonb;
  v_batch_before_json jsonb;
  v_batch_candidate_before_json jsonb;
  v_correction_request_before_json jsonb;
  v_correction_operation_before_json jsonb;
  v_token uuid;
  v_generation bigint;
  v_seq bigint;
  v_generation_before bigint;
  v_seq_before bigint;
  v_drift_token uuid;
  v_drift_generation bigint;
  v_drift_seq bigint;
  v_scale_first_job uuid;
  v_scale_elapsed_ms numeric;
  v_member record;
  v_subset uuid[];
  v_plan jsonb;
  v_operations_before bigint;
  v_batches_before bigint;
  v_batch_items_before bigint;
  v_provider_before bigint;
  v_settlement_before bigint;
  v_remittance_before bigint;
  v_poison_members_oid oid;
  v_poison_roots_oid oid;
BEGIN
  SELECT pg_catalog.to_regclass(
           'pg_temp._bpay_candidate_dirty_cohort_members_v1'
         )::oid,
         pg_catalog.to_regclass(
           'pg_temp._bpay_candidate_dirty_cohort_roots_v1'
         )::oid
  INTO STRICT v_poison_members_oid, v_poison_roots_oid;

  IF pg_catalog.pg_get_userbyid((
       SELECT relation.relowner
       FROM pg_catalog.pg_class AS relation
       WHERE relation.oid = v_poison_members_oid
     )) IS DISTINCT FROM 'service_role'
     OR pg_catalog.pg_get_userbyid((
       SELECT relation.relowner
       FROM pg_catalog.pg_class AS relation
       WHERE relation.oid = v_poison_roots_oid
     )) IS DISTINCT FROM 'service_role' THEN
    RAISE EXCEPTION 'HOSTILE_TEMP_TABLE_CROSS_ROLE_SETUP_FAILED';
  END IF;

  UPDATE public.settings_defaults
  SET banking_pay_correction_request_dirty_deferral_v1_enabled = true;

  INSERT INTO public.candidates(id, display_name, tms_ref, pay_method)
  VALUES
    (v_all_candidate, v_prefix || ':ALL', v_prefix || ':ALL', 'PAYE'),
    (v_disjoint_candidate, v_prefix || ':DISJOINT', v_prefix || ':DISJOINT', 'PAYE'),
    (v_scale_candidate, v_prefix || ':SCALE', v_prefix || ':SCALE', 'PAYE'),
    (v_drift_candidate, v_prefix || ':DRIFT', v_prefix || ':DRIFT', 'PAYE'),
    (v_correction_candidate, v_prefix || ':CORRECTION', v_prefix || ':CORRECTION', 'PAYE'),
    (v_late_candidate, v_prefix || ':LATE', v_prefix || ':LATE', 'PAYE');

  INSERT INTO public.timesheets(
    timesheet_id, booking_id, occupant_key_norm, hospital_norm, ward_norm,
    job_title_norm, week_ending_date, status, is_current, version
  )
  SELECT fixture.timesheet_id,
         v_prefix || ':' || fixture.booking_suffix,
         v_prefix, 'VERIFY', 'VERIFY', 'VERIFY', DATE '2099-09-06',
         'RECEIVED', true, 1
  FROM (
    VALUES
      (v_all_timesheet, 'ALL'),
      (v_disjoint_x, 'DISJOINT_X'),
      (v_disjoint_y, 'DISJOINT_Y'),
      (v_drift_timesheet, 'DRIFT'),
      (v_correction_regular_timesheet, 'CORRECTION_REGULAR'),
      (v_correction_owned_timesheet, 'CORRECTION_OWNED'),
      (v_late_x, 'LATE_X'),
      (v_late_y, 'LATE_Y'),
      (v_scale_timesheets[1], 'SCALE_1'),
      (v_scale_timesheets[2], 'SCALE_2'),
      (v_scale_timesheets[3], 'SCALE_3'),
      (v_scale_timesheets[4], 'SCALE_4'),
      (v_scale_timesheets[5], 'SCALE_5'),
      (v_scale_timesheets[6], 'SCALE_6'),
      (v_scale_timesheets[7], 'SCALE_7')
  ) AS fixture(timesheet_id, booking_suffix);

  INSERT INTO public.timesheets_financials(
    timesheet_id, timesheet_version, is_current, candidate_id,
    candidate_assignment, processing_status
  )
  SELECT fixture.timesheet_id, 1, true, fixture.candidate_id,
         'ASSIGNED', 'READY_FOR_HR'
  FROM (
    VALUES
      (v_all_timesheet, v_all_candidate),
      (v_disjoint_x, v_disjoint_candidate),
      (v_disjoint_y, v_disjoint_candidate),
      (v_drift_timesheet, v_drift_candidate),
      (v_correction_regular_timesheet, v_correction_candidate),
      (v_correction_owned_timesheet, v_correction_candidate),
      (v_late_x, v_late_candidate),
      (v_late_y, v_late_candidate),
      (v_scale_timesheets[1], v_scale_candidate),
      (v_scale_timesheets[2], v_scale_candidate),
      (v_scale_timesheets[3], v_scale_candidate),
      (v_scale_timesheets[4], v_scale_candidate),
      (v_scale_timesheets[5], v_scale_candidate),
      (v_scale_timesheets[6], v_scale_candidate),
      (v_scale_timesheets[7], v_scale_candidate)
  ) AS fixture(timesheet_id, candidate_id);

  -- Direct fixture jobs below model already-emitted production events. Keep
  -- their payload sequences below a legitimate live Candidate source counter;
  -- the cohort helper must never manufacture a counter jump to trust payload.
  INSERT INTO public.app_change_counters(entity_key, seq)
  SELECT 'pay_candidate:' || candidate_id::text, 1000
  FROM pg_catalog.unnest(ARRAY[
    v_all_candidate, v_disjoint_candidate, v_scale_candidate, v_drift_candidate,
    v_correction_candidate, v_late_candidate
  ]) AS candidate_id
  ON CONFLICT (entity_key) DO UPDATE
  SET seq = GREATEST(public.app_change_counters.seq, EXCLUDED.seq);

  -- Finish fixture-trigger transactions and retire their canonical jobs. Every
  -- asserted cohort below is then identified by an exact fixture job id.
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  UPDATE public.banking_pay_workbench_jobs
  SET status = 'SUCCEEDED', completed_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE candidate_id = ANY(ARRAY[
    v_all_candidate, v_disjoint_candidate, v_scale_candidate, v_drift_candidate,
    v_correction_candidate, v_late_candidate
  ])
    AND status IN ('QUEUED', 'RUNNING');

  SELECT count(*) INTO v_operations_before FROM public.banking_pay_operations;
  SELECT count(*) INTO v_batches_before FROM public.pay_batches;
  SELECT count(*) INTO v_batch_items_before FROM public.pay_batch_items;
  SELECT count(*) INTO v_provider_before
  FROM public.banking_pay_operation_provider_attempts;
  SELECT count(*) INTO v_settlement_before
  FROM public.banking_pay_operation_settlement_scope;
  SELECT count(*) INTO v_remittance_before
  FROM public.banking_pay_operation_remittance_scope;

  --------------------------------------------------------------------------
  -- ALL_PLUS_TARGETED: ALL dominates without changing either durable scope.
  --------------------------------------------------------------------------
  INSERT INTO public.banking_pay_workbench_jobs(
    id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
    dedupe_key, candidate_id, payload_json, created_at_utc, updated_at_utc
  ) VALUES
    (
      v_all_job, 'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'QUEUED', -1000,
      pg_catalog.clock_timestamp(), 0, 8, v_prefix || ':ALL_JOB',
      v_all_candidate,
      pg_catalog.jsonb_build_object(
        'candidate_id', v_all_candidate, 'targeted_timesheet_ids', '[]'::jsonb,
        'linked_timesheet_ids', '[]'::jsonb, 'finance_case_ids', '[]'::jsonb,
        'reason', 'DIRTY_TRIGGER:PAY_BATCH_ITEMS:UPDATE',
        'reason_latest', 'DIRTY_TRIGGER:PAY_BATCH_ITEMS:UPDATE',
        'reasons', pg_catalog.jsonb_build_array('DIRTY_TRIGGER:PAY_BATCH_ITEMS:UPDATE'),
        'latest_source_change_seq', 1, 'source_change_seq', 1,
        'source_change_sequence', 1, 'latest_event_at_utc',
        pg_catalog.clock_timestamp()
      ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()
    ),
    (
      v_target_job, 'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'QUEUED', -1000,
      pg_catalog.clock_timestamp(), 0, 8, v_prefix || ':TARGET_JOB',
      v_all_candidate,
      pg_catalog.jsonb_build_object(
        'candidate_id', v_all_candidate,
        'targeted_timesheet_ids', pg_catalog.to_jsonb(ARRAY[v_all_timesheet]),
        'linked_timesheet_ids', '[]'::jsonb, 'finance_case_ids', '[]'::jsonb,
        'reason', 'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE',
        'reason_latest', 'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE',
        'reasons', pg_catalog.jsonb_build_array(
          'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE'
        ),
        'latest_source_change_seq', 2, 'source_change_seq', 2,
        'source_change_sequence', 2, 'latest_event_at_utc',
        pg_catalog.clock_timestamp()
      ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()
    );

  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  UPDATE public.banking_pay_workbench_jobs
  SET status = 'RUNNING', attempt_count = 1,
      started_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_all_job;

  -- Exercise the SECURITY DEFINER entry as the lower-privileged caller that
  -- owns both hostile fixed-name temporary relations.
  EXECUTE 'SET LOCAL ROLE service_role';
  v_result := public.pay_workbench_candidate_dirty_apply_job_process(v_all_job, 100);
  EXECUTE 'RESET ROLE';
  IF v_result->>'dirty_apply_cohort_action'
       IS DISTINCT FROM 'COHORT_REISSUED_PENDING_FINALIZATION'
     OR (v_result->>'dirty_apply_cohort_member_count')::integer <> 2
     OR v_result->>'dirty_apply_cohort_authority_scope'
          IS DISTINCT FROM 'CANDIDATE_FULL_LIVE'
     OR (SELECT status FROM public.banking_pay_workbench_jobs
         WHERE id = v_all_job) IS DISTINCT FROM 'QUEUED'
     OR (SELECT payload_json->'targeted_timesheet_ids'
         FROM public.banking_pay_workbench_jobs WHERE id = v_all_job)
          IS DISTINCT FROM '[]'::jsonb
     OR (SELECT payload_json->'targeted_timesheet_ids'
         FROM public.banking_pay_workbench_jobs WHERE id = v_target_job)
          IS DISTINCT FROM pg_catalog.to_jsonb(ARRAY[v_all_timesheet])
     OR (SELECT count(DISTINCT payload_json->>'scope_change_tx_token')
         FROM public.banking_pay_workbench_jobs
         WHERE id IN (v_all_job, v_target_job)) <> 1 THEN
    RAISE EXCEPTION 'ALL_PLUS_TARGETED_STAGE_FAILED'
      USING DETAIL = v_result::text;
  END IF;

  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  SELECT (payload_json->>'scope_change_tx_token')::uuid,
         scope_change_generation
  INTO STRICT v_token, v_generation
  FROM public.banking_pay_workbench_jobs
  WHERE id = v_all_job;
  SELECT seq INTO STRICT v_seq
  FROM public.app_change_counters
  WHERE entity_key = 'pay_candidate:' || v_all_candidate::text;

  IF (SELECT count(*) FROM public.banking_pay_workbench_jobs
      WHERE id IN (v_all_job, v_target_job)
        AND payload_json->>'scope_change_tx_token' = v_token::text
        AND scope_change_generation = v_generation
        AND payload_json->>'scope_change_generation' = v_generation::text
        AND payload_json->>'dirty_apply_cohort_authority_scope' =
              'CANDIDATE_FULL_LIVE') <> 2
     OR (SELECT dirty_generation
         FROM private.banking_pay_workbench_candidate_scope_registry
         WHERE candidate_id = v_all_candidate) IS DISTINCT FROM v_generation
     OR (SELECT scope_change_generation FROM public.app_change_counters
         WHERE entity_key = 'pay_candidate:' || v_all_candidate::text)
          IS DISTINCT FROM v_generation THEN
    RAISE EXCEPTION 'ALL_PLUS_TARGETED_FINAL_AUTHORITY_FAILED';
  END IF;

  -- The originally targeted member must execute the stronger full path, and
  -- the stable historical preinvalidated flag must not cause a second issue.
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'RUNNING', attempt_count = attempt_count + 1,
      started_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_target_job;
  v_second_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_target_job, 100
  );
  IF v_second_result->>'refresh_scope_kind'
       IS DISTINCT FROM 'CANDIDATE_FULL_LIVE'
     OR COALESCE((v_second_result->>'preinvalidated_scope_reissued')::boolean, false)
          IS NOT TRUE
     OR (v_second_result->>'effective_scope_change_generation')::bigint
          IS DISTINCT FROM v_generation
     OR (SELECT seq FROM public.app_change_counters
         WHERE entity_key = 'pay_candidate:' || v_all_candidate::text)
          IS DISTINCT FROM v_seq
     OR (SELECT scope_change_generation FROM public.app_change_counters
         WHERE entity_key = 'pay_candidate:' || v_all_candidate::text)
          IS DISTINCT FROM v_generation THEN
    RAISE EXCEPTION 'ALL_PLUS_TARGETED_FULL_FAST_PATH_FAILED'
      USING DETAIL = v_second_result::text;
  END IF;

  --------------------------------------------------------------------------
  -- DISJOINT_TARGETED: X and Y keep their job scopes but share union proof.
  --------------------------------------------------------------------------
  INSERT INTO public.banking_pay_workbench_jobs(
    id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
    dedupe_key, candidate_id, payload_json, created_at_utc, updated_at_utc
  ) VALUES
    (v_disjoint_x_job, 'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'QUEUED', -1000,
     pg_catalog.clock_timestamp(), 0, 8, v_prefix || ':DISJOINT_X_JOB',
     v_disjoint_candidate,
     pg_catalog.jsonb_build_object(
       'candidate_id', v_disjoint_candidate,
       'targeted_timesheet_ids', pg_catalog.to_jsonb(ARRAY[v_disjoint_x]),
       'linked_timesheet_ids', '[]'::jsonb, 'finance_case_ids', '[]'::jsonb,
       'reason_latest', 'DIRTY_TRIGGER:TIMESHEETS:UPDATE',
       'reasons', pg_catalog.jsonb_build_array('DIRTY_TRIGGER:TIMESHEETS:UPDATE'),
       'latest_source_change_seq', 3, 'source_change_seq', 3,
       'source_change_sequence', 3,
       'latest_event_at_utc', pg_catalog.clock_timestamp()
     ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()),
    (v_disjoint_y_job, 'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'QUEUED', -1000,
     pg_catalog.clock_timestamp(), 0, 8, v_prefix || ':DISJOINT_Y_JOB',
     v_disjoint_candidate,
     pg_catalog.jsonb_build_object(
       'candidate_id', v_disjoint_candidate,
       'targeted_timesheet_ids', pg_catalog.to_jsonb(ARRAY[v_disjoint_y]),
       'linked_timesheet_ids', '[]'::jsonb, 'finance_case_ids', '[]'::jsonb,
       'reason_latest', 'DIRTY_TRIGGER:TIMESHEET_SUMMARY:UPDATE',
       'reasons', pg_catalog.jsonb_build_array('DIRTY_TRIGGER:TIMESHEET_SUMMARY:UPDATE'),
       'latest_source_change_seq', 4, 'source_change_seq', 4,
       'source_change_sequence', 4,
       'latest_event_at_utc', pg_catalog.clock_timestamp()
     ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp());
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'RUNNING', attempt_count = 1,
      started_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_disjoint_x_job;
  v_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_disjoint_x_job, 100
  );
  IF v_result->>'dirty_apply_cohort_authority_scope'
       IS DISTINCT FROM 'TARGETED_UNION'
     OR (v_result->>'dirty_apply_cohort_member_count')::integer <> 2 THEN
    RAISE EXCEPTION 'DISJOINT_TARGETED_STAGE_FAILED'
      USING DETAIL = v_result::text;
  END IF;
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  SELECT scope_change_generation INTO STRICT v_generation
  FROM public.banking_pay_workbench_jobs WHERE id = v_disjoint_x_job;
  IF (SELECT count(*) FROM private.banking_pay_workbench_timesheet_scope_state
      WHERE candidate_id = v_disjoint_candidate
        AND timesheet_id IN (v_disjoint_x, v_disjoint_y)
        AND dirty_generation = v_generation) <> 2
     OR (SELECT count(DISTINCT scope_change_generation)
         FROM public.banking_pay_workbench_jobs
         WHERE id IN (v_disjoint_x_job, v_disjoint_y_job)) <> 1
     OR (SELECT payload_json->'targeted_timesheet_ids'
         FROM public.banking_pay_workbench_jobs WHERE id = v_disjoint_x_job)
          IS DISTINCT FROM pg_catalog.to_jsonb(ARRAY[v_disjoint_x])
     OR (SELECT payload_json->'targeted_timesheet_ids'
         FROM public.banking_pay_workbench_jobs WHERE id = v_disjoint_y_job)
          IS DISTINCT FROM pg_catalog.to_jsonb(ARRAY[v_disjoint_y]) THEN
    RAISE EXCEPTION 'DISJOINT_TARGETED_UNION_FINAL_AUTHORITY_FAILED';
  END IF;

  --------------------------------------------------------------------------
  -- ACTIVE_MEMBER_COUNT_101: no cohort-member capacity failure and all later
  -- members take their own finalized marker fast path without generation churn.
  --------------------------------------------------------------------------
  FOR v_member IN SELECT generate_series(1, 101) AS member_number LOOP
    SELECT COALESCE(pg_catalog.array_agg(scope_id ORDER BY ordinal), ARRAY[]::uuid[])
    INTO v_subset
    FROM pg_catalog.unnest(v_scale_timesheets) WITH ORDINALITY
      AS scale_scope(scope_id, ordinal)
    WHERE (v_member.member_number & (1 << (ordinal - 1)::integer)) <> 0;

    INSERT INTO public.banking_pay_workbench_jobs(
      job_type, status, priority, run_at_utc, attempt_count, max_attempts,
      dedupe_key, candidate_id, payload_json, created_at_utc, updated_at_utc
    ) VALUES (
      'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'QUEUED', -1000,
      pg_catalog.clock_timestamp(), 0, 8,
      v_prefix || ':SCALE:' || v_member.member_number::text,
      v_scale_candidate,
      pg_catalog.jsonb_build_object(
        'candidate_id', v_scale_candidate,
        'targeted_timesheet_ids', pg_catalog.to_jsonb(v_subset),
        'linked_timesheet_ids', '[]'::jsonb,
        'finance_case_ids', '[]'::jsonb,
        'reason_latest', 'DIRTY_TRIGGER:TIMESHEETS:UPDATE',
        'reasons', pg_catalog.jsonb_build_array('DIRTY_TRIGGER:TIMESHEETS:UPDATE'),
        'latest_source_change_seq', v_member.member_number,
        'source_change_seq', v_member.member_number,
        'source_change_sequence', v_member.member_number,
        'latest_event_at_utc', pg_catalog.clock_timestamp()
      ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()
    ) RETURNING id INTO v_scale_first_job;
    IF v_member.member_number = 1 THEN
      -- Preserve the actual first member; later INSERT ... RETURNING values are
      -- irrelevant to the cohort leader selection.
      SELECT id INTO STRICT v_scale_first_job
      FROM public.banking_pay_workbench_jobs
      WHERE dedupe_key = v_prefix || ':SCALE:1';
    END IF;
  END LOOP;
  SELECT id INTO STRICT v_scale_first_job
  FROM public.banking_pay_workbench_jobs
  WHERE dedupe_key = v_prefix || ':SCALE:1';
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'RUNNING', attempt_count = 1,
      started_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_scale_first_job;
  v_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_scale_first_job, 100
  );
  IF (v_result->>'dirty_apply_cohort_member_count')::integer <> 101
     OR v_result->>'dirty_apply_cohort_action'
          IS DISTINCT FROM 'COHORT_REISSUED_PENDING_FINALIZATION'
     OR v_result::text ~* 'CAPACITY_EXHAUSTED|TOO_MANY_(MEMBERS|JOBS)' THEN
    RAISE EXCEPTION 'ACTIVE_MEMBER_COUNT_101_STAGE_FAILED'
      USING DETAIL = v_result::text;
  END IF;
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  SELECT scope_change_generation INTO STRICT v_generation
  FROM public.banking_pay_workbench_jobs WHERE id = v_scale_first_job;
  SELECT seq INTO STRICT v_seq
  FROM public.app_change_counters
  WHERE entity_key = 'pay_candidate:' || v_scale_candidate::text;
  PERFORM pg_catalog.set_config('enable_seqscan', 'off', true);
  EXECUTE pg_catalog.format(
    $plan$EXPLAIN (FORMAT JSON, COSTS OFF)
      SELECT candidate_job.id
      FROM public.banking_pay_workbench_jobs AS candidate_job
      WHERE candidate_job.candidate_id = %L::uuid
        AND candidate_job.job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND candidate_job.status IN ('QUEUED', 'RUNNING')
      ORDER BY candidate_job.id$plan$,
    v_scale_candidate::text
  ) INTO v_plan;
  IF v_plan::text !~ 'idx_bpay_wb_jobs_candidate_dirty_active_cohort_v1' THEN
    RAISE EXCEPTION 'DIRECT_INDEX_PLAN_NOT_USED'
      USING DETAIL = v_plan::text;
  END IF;
  PERFORM pg_catalog.set_config('enable_seqscan', 'on', true);
  v_generation_before := public.pay_workbench_scope_current_generation_v1();
  v_scale_elapsed_ms := -1000 * EXTRACT(
    epoch FROM pg_catalog.clock_timestamp()
  );
  FOR v_member IN
    SELECT id
    FROM public.banking_pay_workbench_jobs
    WHERE candidate_id = v_scale_candidate
      AND job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
      AND dedupe_key LIKE v_prefix || ':SCALE:%'
    ORDER BY id
  LOOP
    UPDATE public.banking_pay_workbench_jobs
    SET status = 'RUNNING', attempt_count = attempt_count + 1,
        started_at_utc = pg_catalog.clock_timestamp(),
        updated_at_utc = pg_catalog.clock_timestamp()
    WHERE id = v_member.id;
    v_second_result := public.pay_workbench_candidate_dirty_apply_job_process(
      v_member.id, 1
    );
    IF (v_second_result->>'effective_scope_change_generation')::bigint
         IS DISTINCT FROM v_generation
       OR v_second_result->>'refresh_scope_kind'
            IS DISTINCT FROM 'TARGETED_TIMESHEETS' THEN
      RAISE EXCEPTION 'ACTIVE_MEMBER_COUNT_101_FAST_PATH_FAILED'
        USING DETAIL = v_second_result::text;
    END IF;
    UPDATE public.banking_pay_workbench_jobs
    SET status = 'SUCCEEDED', completed_at_utc = pg_catalog.clock_timestamp(),
        updated_at_utc = pg_catalog.clock_timestamp()
    WHERE id = v_member.id;
  END LOOP;
  v_scale_elapsed_ms := v_scale_elapsed_ms + 1000 * EXTRACT(
    epoch FROM pg_catalog.clock_timestamp()
  );
  IF v_scale_elapsed_ms > 30000 THEN
    RAISE EXCEPTION 'ACTIVE_MEMBER_COUNT_101_FAST_PATH_BUDGET_EXCEEDED'
      USING DETAIL = pg_catalog.jsonb_build_object(
        'elapsed_ms', v_scale_elapsed_ms,
        'budget_ms', 30000
      )::text;
  END IF;
  IF (SELECT seq FROM public.app_change_counters
      WHERE entity_key = 'pay_candidate:' || v_scale_candidate::text)
       IS DISTINCT FROM v_seq
     OR public.pay_workbench_scope_current_generation_v1()
          IS DISTINCT FROM v_generation_before THEN
    RAISE EXCEPTION 'ACTIVE_MEMBER_COUNT_101_GENERATION_CHURNED';
  END IF;

  --------------------------------------------------------------------------
  -- TARGETED_FINALIZED_TO_FULL_DRIFT: a preserved targeted cohort token must
  -- not pass the vacuous zero-row state proof after ownership closure becomes
  -- Candidate-full. Both public fast path and singleton helper reuse must fail.
  --------------------------------------------------------------------------
  INSERT INTO public.banking_pay_workbench_jobs(
    id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
    dedupe_key, candidate_id, payload_json, created_at_utc, updated_at_utc
  ) VALUES (
    v_drift_job, 'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'RUNNING', -1000,
    pg_catalog.clock_timestamp(), 1, 8, v_prefix || ':DRIFT_JOB',
    v_drift_candidate,
    pg_catalog.jsonb_build_object(
      'candidate_id', v_drift_candidate,
      'targeted_timesheet_ids', pg_catalog.to_jsonb(ARRAY[v_drift_timesheet]),
      'linked_timesheet_ids', '[]'::jsonb,
      'finance_case_ids', '[]'::jsonb,
      'reason_latest', 'DIRTY_TRIGGER:TIMESHEETS:UPDATE',
      'reasons', pg_catalog.jsonb_build_array('DIRTY_TRIGGER:TIMESHEETS:UPDATE'),
      'latest_source_change_seq', 1,
      'source_change_seq', 1,
      'source_change_sequence', 1,
      'latest_event_at_utc', pg_catalog.clock_timestamp()
    ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()
  );
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  v_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_drift_job, 100
  );
  IF v_result->>'dirty_apply_cohort_action'
       IS DISTINCT FROM 'COHORT_REISSUED_PENDING_FINALIZATION'
     OR v_result->>'dirty_apply_cohort_authority_scope'
          IS DISTINCT FROM 'TARGETED_UNION' THEN
    RAISE EXCEPTION 'TARGETED_FINALIZED_TO_FULL_INITIAL_STAGE_FAILED'
      USING DETAIL = v_result::text;
  END IF;
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  SELECT (payload_json->>'scope_change_tx_token')::uuid,
         scope_change_generation
  INTO STRICT v_drift_token, v_drift_generation
  FROM public.banking_pay_workbench_jobs
  WHERE id = v_drift_job;
  SELECT seq INTO STRICT v_drift_seq
  FROM public.app_change_counters
  WHERE entity_key = 'pay_candidate:' || v_drift_candidate::text;

  -- Fixture-only ownership withdrawal forces the existing targeted payload's
  -- dependency closure to full without replacing its finalized cohort keys.
  PERFORM pg_catalog.set_config('session_replication_role', 'replica', true);
  UPDATE public.timesheets_financials
  SET candidate_id = v_all_candidate
  WHERE timesheet_id = v_drift_timesheet
    AND candidate_id = v_drift_candidate;
  PERFORM pg_catalog.set_config('session_replication_role', 'origin', true);
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'RUNNING', attempt_count = attempt_count + 1,
      started_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp(),
      payload_json = payload_json || pg_catalog.jsonb_build_object(
        'latest_event_at_utc', pg_catalog.clock_timestamp()
      )
  WHERE id = v_drift_job;

  v_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_drift_job, 100
  );
  IF v_result->>'dirty_apply_cohort_action'
       IS DISTINCT FROM 'COHORT_REISSUED_PENDING_FINALIZATION'
     OR v_result->>'dirty_apply_cohort_authority_scope'
          IS DISTINCT FROM 'CANDIDATE_FULL_LIVE'
     OR (v_result->>'effective_scope_change_tx_token')::uuid
          IS NOT DISTINCT FROM v_drift_token THEN
    RAISE EXCEPTION 'TARGETED_FINALIZED_TO_FULL_STALE_REUSE'
      USING DETAIL = v_result::text;
  END IF;
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  IF (SELECT scope_change_generation
      FROM public.banking_pay_workbench_jobs
      WHERE id = v_drift_job) <= v_drift_generation
     OR (SELECT seq
         FROM public.app_change_counters
         WHERE entity_key = 'pay_candidate:' || v_drift_candidate::text)
          <= v_drift_seq
     OR (SELECT payload_json->>'dirty_apply_cohort_authority_scope'
         FROM public.banking_pay_workbench_jobs
         WHERE id = v_drift_job) IS DISTINCT FROM 'CANDIDATE_FULL_LIVE' THEN
    RAISE EXCEPTION 'TARGETED_FINALIZED_TO_FULL_NEW_AUTHORITY_FAILED';
  END IF;

  SELECT (payload_json->>'scope_change_tx_token')::uuid,
         scope_change_generation
  INTO STRICT v_token, v_generation
  FROM public.banking_pay_workbench_jobs
  WHERE id = v_drift_job;
  SELECT seq INTO STRICT v_seq
  FROM public.app_change_counters
  WHERE entity_key = 'pay_candidate:' || v_drift_candidate::text;
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'RUNNING', attempt_count = attempt_count + 1,
      started_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_drift_job;
  v_second_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_drift_job, 100
  );
  IF v_second_result->>'refresh_scope_kind'
       IS DISTINCT FROM 'CANDIDATE_FULL_LIVE'
     OR (v_second_result->>'effective_scope_change_tx_token')::uuid
          IS DISTINCT FROM v_token
     OR (v_second_result->>'effective_scope_change_generation')::bigint
          IS DISTINCT FROM v_generation
     OR (SELECT seq
         FROM public.app_change_counters
         WHERE entity_key = 'pay_candidate:' || v_drift_candidate::text)
          IS DISTINCT FROM v_seq THEN
    RAISE EXCEPTION 'TARGETED_FINALIZED_TO_FULL_EXACT_FULL_REPLAY_FAILED'
      USING DETAIL = v_second_result::text;
  END IF;
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'SUCCEEDED', completed_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_drift_job;

  --------------------------------------------------------------------------
  -- LEGACY_INDEX_PLAN: the expression is byte-equivalent to helper lookup.
  --------------------------------------------------------------------------
  INSERT INTO public.banking_pay_workbench_jobs(
    job_type, status, priority, run_at_utc, attempt_count, max_attempts,
    dedupe_key, candidate_id, payload_json, created_at_utc, updated_at_utc
  )
  SELECT 'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'QUEUED', -1000,
         pg_catalog.clock_timestamp(), 0, 8,
         v_prefix || ':LEGACY:' || legacy_number::text,
         NULL::uuid,
         pg_catalog.jsonb_build_object(
           'candidate_id', v_scale_candidate,
           'targeted_timesheet_ids', pg_catalog.to_jsonb(ARRAY[v_scale_timesheets[1]]),
           'linked_timesheet_ids', '[]'::jsonb,
           'finance_case_ids', '[]'::jsonb,
           'latest_source_change_seq', 101 + legacy_number,
           'latest_event_at_utc', pg_catalog.clock_timestamp()
         ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()
  FROM generate_series(1, 101) AS legacy_number;
  PERFORM pg_catalog.set_config('enable_seqscan', 'off', true);
  EXECUTE pg_catalog.format(
    $plan$EXPLAIN (FORMAT JSON, COSTS OFF)
      SELECT legacy_job.id
      FROM public.banking_pay_workbench_jobs AS legacy_job
      WHERE legacy_job.candidate_id IS NULL
        AND legacy_job.job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND legacy_job.status IN ('QUEUED', 'RUNNING')
        AND pg_catalog.lower(pg_catalog.btrim(COALESCE(
              legacy_job.payload_json->>'candidate_id', ''
            ))) = %L
      ORDER BY legacy_job.id$plan$,
    v_scale_candidate::text
  ) INTO v_plan;
  IF v_plan::text !~ 'idx_bpay_wb_jobs_legacy_candidate_dirty_active_cohort_v1' THEN
    RAISE EXCEPTION 'LEGACY_INDEX_PLAN_NOT_USED'
      USING DETAIL = v_plan::text;
  END IF;
  PERFORM pg_catalog.set_config('enable_seqscan', 'on', true);

  IF (SELECT count(*) FROM public.banking_pay_operations) <> v_operations_before
     OR (SELECT count(*) FROM public.pay_batches) <> v_batches_before
     OR (SELECT count(*) FROM public.pay_batch_items) <> v_batch_items_before
     OR (SELECT count(*) FROM public.banking_pay_operation_provider_attempts)
          <> v_provider_before
     OR (SELECT count(*) FROM public.banking_pay_operation_settlement_scope)
          <> v_settlement_before
     OR (SELECT count(*) FROM public.banking_pay_operation_remittance_scope)
          <> v_remittance_before THEN
    RAISE EXCEPTION 'POLICY_X_PRE_CORRECTION_WINDOW_CHANGED';
  END IF;

  --------------------------------------------------------------------------
  -- CORRECTION_OWNED_EXCLUSION: exact unfinished request authority is not
  -- restamped; after terminal lifecycle it is admitted by the next cohort.
  --------------------------------------------------------------------------
  INSERT INTO public.pay_batches(
    id, pay_date, status, banking_system_snapshot,
    external_paye_system_snapshot
  ) VALUES (
    v_batch_id, DATE '2099-09-08', 'DRAFT', 'MONZO_CSV', 'CSV'
  );
  INSERT INTO public.pay_batch_candidates(
    id, pay_batch_id, candidate_id, candidate_tms_ref,
    candidate_display_name
  ) VALUES (
    v_batch_candidate_id, v_batch_id, v_correction_candidate,
    v_prefix || ':CORRECTION', v_prefix || ':CORRECTION'
  );
  INSERT INTO public.pay_payment_correction_requests(
    id, pay_batch_id, correction_kind, status, selection_hash, plan_hash,
    reason
  ) VALUES (
    v_correction_request_id, v_batch_id, 'PRE_BANK_CANCEL', 'PROCESSING',
    repeat('1', 64), repeat('2', 64), v_prefix
  );
  INSERT INTO public.pay_payment_correction_request_candidates(
    correction_request_id, selection_ordinal, pay_batch_candidate_id,
    candidate_scope_hash, active_item_count, source_row_count, active_amount,
    pay_batch_item_ids, eligibility_code_at_plan
  ) VALUES (
    v_correction_request_id, 1, v_batch_candidate_id, repeat('3', 64),
    1, 1, 0, ARRAY[pg_catalog.gen_random_uuid()], 'ELIGIBLE'
  );
  INSERT INTO public.banking_pay_operations(
    id, operation_type, status, phase, pay_batch_id, idempotency_key,
    input_json
  ) VALUES (
    v_correction_operation_id, 'PAYMENT_CORRECTION', 'RUNNING',
    'FINANCIAL_APPLY', v_batch_id, v_prefix || ':CORRECTION_OPERATION',
    pg_catalog.jsonb_build_object(
      'correction_request_id', v_correction_request_id
    )
  );

  -- Snapshot after every explicit correction fixture row exists and before
  -- either correction-cohort processor call.
  SELECT count(*) INTO v_operations_before FROM public.banking_pay_operations;
  SELECT count(*) INTO v_batches_before FROM public.pay_batches;
  SELECT count(*) INTO v_batch_items_before FROM public.pay_batch_items;
  SELECT count(*) INTO v_provider_before
  FROM public.banking_pay_operation_provider_attempts;
  SELECT count(*) INTO v_settlement_before
  FROM public.banking_pay_operation_settlement_scope;
  SELECT count(*) INTO v_remittance_before
  FROM public.banking_pay_operation_remittance_scope;
  SELECT pg_catalog.to_jsonb(batch_row) INTO STRICT v_batch_before_json
  FROM public.pay_batches AS batch_row WHERE id = v_batch_id;
  SELECT pg_catalog.to_jsonb(candidate_row)
  INTO STRICT v_batch_candidate_before_json
  FROM public.pay_batch_candidates AS candidate_row
  WHERE id = v_batch_candidate_id;
  SELECT pg_catalog.to_jsonb(request_row)
  INTO STRICT v_correction_request_before_json
  FROM public.pay_payment_correction_requests AS request_row
  WHERE id = v_correction_request_id;
  SELECT pg_catalog.to_jsonb(operation_row)
  INTO STRICT v_correction_operation_before_json
  FROM public.banking_pay_operations AS operation_row
  WHERE id = v_correction_operation_id;

  v_correction_token := public.pay_workbench_scope_change_tx_token_v1();
  v_correction_context_digest := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to(
      'CORRECTION_OWNED_DIRTY_CAUSAL_V1' || '|' ||
      v_correction_request_id::text || '|' ||
      v_correction_operation_id::text || '|' || '' || '|' ||
      v_batch_id::text || '|' || v_correction_candidate::text || '|' ||
      'FINANCIAL_PAGE_APPLIED' || '|' || 'PRE_DRAFT_LIVE_TRUTH' || '|' ||
      '1' || '|' || '1' || '|' || '',
      'UTF8'
    ), 'sha256'
  ), 'hex');
  v_correction_context := pg_catalog.jsonb_build_object(
    'contract_version', 'CORRECTION_OWNED_DIRTY_CAUSAL_V1',
    'correction_request_id', v_correction_request_id,
    'correction_operation_id', v_correction_operation_id,
    'correction_work_item_id', '',
    'pay_batch_id', v_batch_id,
    'candidate_id', v_correction_candidate,
    'lifecycle_phase', 'FINANCIAL_PAGE_APPLIED',
    'policy_x_boundary', 'PRE_DRAFT_LIVE_TRUTH',
    'pre_request_source_change_seq', '1',
    'pre_request_dirty_generation', '1',
    'pre_request_fence_digest', '',
    'context_digest', v_correction_context_digest
  );

  INSERT INTO public.banking_pay_workbench_jobs(
    id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
    dedupe_key, candidate_id, payload_json, created_at_utc, updated_at_utc
  ) VALUES
    (v_correction_regular_job, 'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'QUEUED',
     -1000, pg_catalog.clock_timestamp(), 0, 8,
     v_prefix || ':CORRECTION_REGULAR_JOB', v_correction_candidate,
     pg_catalog.jsonb_build_object(
       'candidate_id', v_correction_candidate,
       'targeted_timesheet_ids', pg_catalog.to_jsonb(ARRAY[v_correction_regular_timesheet]),
       'linked_timesheet_ids', '[]'::jsonb, 'finance_case_ids', '[]'::jsonb,
       'reason_latest', 'DIRTY_TRIGGER:TIMESHEETS:UPDATE',
       'reasons', pg_catalog.jsonb_build_array('DIRTY_TRIGGER:TIMESHEETS:UPDATE'),
       'latest_source_change_seq', 1,
       'latest_event_at_utc', pg_catalog.clock_timestamp()
     ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()),
    (v_correction_owned_job, 'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'QUEUED',
     -1000, pg_catalog.clock_timestamp(), 0, 8,
     v_prefix || ':CORRECTION_OWNED_JOB', v_correction_candidate,
     pg_catalog.jsonb_build_object(
       'candidate_id', v_correction_candidate,
       'targeted_timesheet_ids', pg_catalog.to_jsonb(ARRAY[v_correction_owned_timesheet]),
       'linked_timesheet_ids', '[]'::jsonb, 'finance_case_ids', '[]'::jsonb,
       'reason_latest', 'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE',
       'reasons', pg_catalog.jsonb_build_array(
         'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE'
       ),
       'trigger_table', 'pay_payment_correction_requests',
       'policy_x_dirtying_only', true,
       'economic_truth_mutation_allowed', false,
       'correction_dirty_contexts', pg_catalog.jsonb_build_object(
         v_correction_candidate::text, v_correction_context
       ),
       'request_owned_scope_change_tx_token', v_correction_token,
       'scope_change_tx_token', v_correction_token,
       'latest_source_change_seq', 2,
       'latest_event_at_utc', pg_catalog.clock_timestamp()
     ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp());
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  IF (SELECT count(*) FROM public.banking_pay_operations) <> v_operations_before
     OR (SELECT count(*) FROM public.pay_batches) <> v_batches_before
     OR (SELECT count(*) FROM public.pay_batch_items) <> v_batch_items_before
     OR (SELECT count(*) FROM public.banking_pay_operation_provider_attempts)
          <> v_provider_before
     OR (SELECT count(*) FROM public.banking_pay_operation_settlement_scope)
          <> v_settlement_before
     OR (SELECT count(*) FROM public.banking_pay_operation_remittance_scope)
          <> v_remittance_before
     OR (SELECT pg_catalog.to_jsonb(batch_row)
         FROM public.pay_batches AS batch_row WHERE id = v_batch_id)
          IS DISTINCT FROM v_batch_before_json
     OR (SELECT pg_catalog.to_jsonb(candidate_row)
         FROM public.pay_batch_candidates AS candidate_row
         WHERE id = v_batch_candidate_id)
          IS DISTINCT FROM v_batch_candidate_before_json
     OR (SELECT pg_catalog.to_jsonb(request_row)
         FROM public.pay_payment_correction_requests AS request_row
         WHERE id = v_correction_request_id)
          IS DISTINCT FROM v_correction_request_before_json
     OR (SELECT pg_catalog.to_jsonb(operation_row)
         FROM public.banking_pay_operations AS operation_row
         WHERE id = v_correction_operation_id)
          IS DISTINCT FROM v_correction_operation_before_json THEN
    RAISE EXCEPTION 'POLICY_X_CORRECTION_COHORT_WINDOW_CHANGED';
  END IF;
  SELECT pg_catalog.to_jsonb(batch_row) INTO STRICT v_batch_before_json
  FROM public.pay_batches AS batch_row WHERE id = v_batch_id;
  SELECT pg_catalog.to_jsonb(candidate_row)
  INTO STRICT v_batch_candidate_before_json
  FROM public.pay_batch_candidates AS candidate_row
  WHERE id = v_batch_candidate_id;
  SELECT pg_catalog.to_jsonb(request_row)
  INTO STRICT v_correction_request_before_json
  FROM public.pay_payment_correction_requests AS request_row
  WHERE id = v_correction_request_id;
  SELECT pg_catalog.to_jsonb(operation_row)
  INTO STRICT v_correction_operation_before_json
  FROM public.banking_pay_operations AS operation_row
  WHERE id = v_correction_operation_id;
  SELECT pg_catalog.to_jsonb(job) INTO STRICT v_job_before
  FROM public.banking_pay_workbench_jobs AS job
  WHERE id = v_correction_owned_job;
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'RUNNING', attempt_count = 1,
      started_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_correction_regular_job;
  v_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_correction_regular_job, 100
  );
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  IF (SELECT pg_catalog.to_jsonb(batch_row)
      FROM public.pay_batches AS batch_row WHERE id = v_batch_id)
       IS DISTINCT FROM v_batch_before_json
     OR (SELECT pg_catalog.to_jsonb(candidate_row)
         FROM public.pay_batch_candidates AS candidate_row
         WHERE id = v_batch_candidate_id)
          IS DISTINCT FROM v_batch_candidate_before_json
     OR (SELECT pg_catalog.to_jsonb(request_row)
         FROM public.pay_payment_correction_requests AS request_row
         WHERE id = v_correction_request_id)
          IS DISTINCT FROM v_correction_request_before_json
     OR (SELECT pg_catalog.to_jsonb(operation_row)
         FROM public.banking_pay_operations AS operation_row
         WHERE id = v_correction_operation_id)
          IS DISTINCT FROM v_correction_operation_before_json THEN
    RAISE EXCEPTION 'POLICY_X_CORRECTION_FIRST_PROCESSOR_ROW_CHANGED';
  END IF;
  SELECT pg_catalog.to_jsonb(job) INTO STRICT v_job_after
  FROM public.banking_pay_workbench_jobs AS job
  WHERE id = v_correction_owned_job;
  IF (v_result->>'dirty_apply_cohort_member_count')::integer <> 1
     OR (v_result->>'dirty_apply_cohort_excluded_request_owned_count')::integer <> 1
     OR v_job_after IS DISTINCT FROM v_job_before THEN
    RAISE EXCEPTION 'CORRECTION_OWNED_EXCLUSION_FAILED'
      USING DETAIL = pg_catalog.jsonb_build_object(
        'result', v_result, 'owned_before', v_job_before,
        'owned_after', v_job_after
      )::text;
  END IF;

  UPDATE public.pay_payment_correction_requests
  SET status = 'APPLIED', applied_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_correction_request_id;
  UPDATE public.banking_pay_operations
  SET status = 'COMPLETE', phase = 'COMPLETE',
      completed_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_correction_operation_id;
  SELECT pg_catalog.to_jsonb(batch_row) INTO STRICT v_batch_before_json
  FROM public.pay_batches AS batch_row WHERE id = v_batch_id;
  SELECT pg_catalog.to_jsonb(candidate_row)
  INTO STRICT v_batch_candidate_before_json
  FROM public.pay_batch_candidates AS candidate_row
  WHERE id = v_batch_candidate_id;
  SELECT pg_catalog.to_jsonb(request_row)
  INTO STRICT v_correction_request_before_json
  FROM public.pay_payment_correction_requests AS request_row
  WHERE id = v_correction_request_id;
  SELECT pg_catalog.to_jsonb(operation_row)
  INTO STRICT v_correction_operation_before_json
  FROM public.banking_pay_operations AS operation_row
  WHERE id = v_correction_operation_id;
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'SUCCEEDED', completed_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_correction_regular_job;
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'RUNNING', attempt_count = attempt_count + 1,
      started_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_correction_owned_job;
  v_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_correction_owned_job, 100
  );
  IF v_result->>'dirty_apply_cohort_action'
       IS DISTINCT FROM 'COHORT_REISSUED_PENDING_FINALIZATION'
     OR (v_result->>'dirty_apply_cohort_member_count')::integer <> 1
     OR (v_result->>'dirty_apply_cohort_excluded_request_owned_count')::integer <> 0 THEN
    RAISE EXCEPTION 'CORRECTION_OWNED_TERMINAL_ADMISSION_FAILED'
      USING DETAIL = v_result::text;
  END IF;
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';

  IF (SELECT pg_catalog.to_jsonb(batch_row)
      FROM public.pay_batches AS batch_row WHERE id = v_batch_id)
       IS DISTINCT FROM v_batch_before_json
     OR (SELECT pg_catalog.to_jsonb(candidate_row)
         FROM public.pay_batch_candidates AS candidate_row
         WHERE id = v_batch_candidate_id)
          IS DISTINCT FROM v_batch_candidate_before_json
     OR (SELECT pg_catalog.to_jsonb(request_row)
         FROM public.pay_payment_correction_requests AS request_row
         WHERE id = v_correction_request_id)
          IS DISTINCT FROM v_correction_request_before_json
     OR (SELECT pg_catalog.to_jsonb(operation_row)
         FROM public.banking_pay_operations AS operation_row
         WHERE id = v_correction_operation_id)
          IS DISTINCT FROM v_correction_operation_before_json THEN
    RAISE EXCEPTION 'POLICY_X_CORRECTION_SECOND_PROCESSOR_ROW_CHANGED';
  END IF;

  IF (SELECT count(*) FROM public.banking_pay_operations) <> v_operations_before
     OR (SELECT count(*) FROM public.pay_batches) <> v_batches_before
     OR (SELECT count(*) FROM public.pay_batch_items) <> v_batch_items_before
     OR (SELECT count(*) FROM public.banking_pay_operation_provider_attempts)
          <> v_provider_before
     OR (SELECT count(*) FROM public.banking_pay_operation_settlement_scope)
          <> v_settlement_before
     OR (SELECT count(*) FROM public.banking_pay_operation_remittance_scope)
          <> v_remittance_before THEN
    RAISE EXCEPTION 'POLICY_X_CORRECTION_PROCESSOR_WINDOW_CHANGED';
  END IF;

  -- Establish a fresh before-image for the late-arrival/replay processor
  -- window; the final assertion below compares against this exact snapshot.
  SELECT count(*) INTO v_operations_before FROM public.banking_pay_operations;
  SELECT count(*) INTO v_batches_before FROM public.pay_batches;
  SELECT count(*) INTO v_batch_items_before FROM public.pay_batch_items;
  SELECT count(*) INTO v_provider_before
  FROM public.banking_pay_operation_provider_attempts;
  SELECT count(*) INTO v_settlement_before
  FROM public.banking_pay_operation_settlement_scope;
  SELECT count(*) INTO v_remittance_before
  FROM public.banking_pay_operation_remittance_scope;

  --------------------------------------------------------------------------
  -- LATE_ARRIVAL + RESPONSE_LOSS_REPLAY: a later distinct key forms one next
  -- cohort; replay of the committed first response never issues again.
  --------------------------------------------------------------------------
  INSERT INTO public.banking_pay_workbench_jobs(
    id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
    dedupe_key, candidate_id, payload_json, created_at_utc, updated_at_utc
  ) VALUES (
    v_late_x_job, 'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'RUNNING', -1000,
    pg_catalog.clock_timestamp(), 1, 8, v_prefix || ':LATE_X_JOB',
    v_late_candidate,
    pg_catalog.jsonb_build_object(
      'candidate_id', v_late_candidate,
      'targeted_timesheet_ids', pg_catalog.to_jsonb(ARRAY[v_late_x]),
      'linked_timesheet_ids', '[]'::jsonb, 'finance_case_ids', '[]'::jsonb,
      'reason_latest', 'DIRTY_TRIGGER:TIMESHEETS:UPDATE',
      'latest_source_change_seq', 1,
      'latest_event_at_utc', pg_catalog.clock_timestamp()
    ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()
  );
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  v_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_late_x_job, 100
  );
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  SELECT scope_change_generation INTO STRICT v_generation
  FROM public.banking_pay_workbench_jobs WHERE id = v_late_x_job;
  SELECT seq INTO STRICT v_seq
  FROM public.app_change_counters
  WHERE entity_key = 'pay_candidate:' || v_late_candidate::text;

  -- The caller loses the committed response. Reclaiming the same job proves
  -- exact finalized authority and does not increment token/generation/seq.
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'RUNNING', attempt_count = attempt_count + 1,
      started_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_late_x_job;
  v_second_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_late_x_job, 100
  );
  IF (v_second_result->>'effective_scope_change_generation')::bigint
       IS DISTINCT FROM v_generation
     OR (SELECT seq FROM public.app_change_counters
         WHERE entity_key = 'pay_candidate:' || v_late_candidate::text)
          IS DISTINCT FROM v_seq THEN
    RAISE EXCEPTION 'RESPONSE_LOSS_REPLAY_CHURNED_AUTHORITY'
      USING DETAIL = v_second_result::text;
  END IF;
  UPDATE public.banking_pay_workbench_jobs
  SET status = 'SUCCEEDED', completed_at_utc = pg_catalog.clock_timestamp(),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE id = v_late_x_job;

  -- This distinct-key event was outside the first ID snapshot and therefore
  -- must form exactly one later cohort rather than inherit a stale marker.
  INSERT INTO public.banking_pay_workbench_jobs(
    id, job_type, status, priority, run_at_utc, attempt_count, max_attempts,
    dedupe_key, candidate_id, payload_json, created_at_utc, updated_at_utc
  ) VALUES (
    v_late_y_job, 'WORKBENCH_CANDIDATE_DIRTY_APPLY', 'RUNNING', -1000,
    pg_catalog.clock_timestamp(), 1, 8, v_prefix || ':LATE_Y_JOB',
    v_late_candidate,
    pg_catalog.jsonb_build_object(
      'candidate_id', v_late_candidate,
      'targeted_timesheet_ids', pg_catalog.to_jsonb(ARRAY[v_late_y]),
      'linked_timesheet_ids', '[]'::jsonb, 'finance_case_ids', '[]'::jsonb,
      'reason_latest', 'DIRTY_TRIGGER:TIMESHEET_SUMMARY:UPDATE',
      'latest_source_change_seq', v_seq + 1,
      'latest_event_at_utc', pg_catalog.clock_timestamp()
    ), pg_catalog.clock_timestamp(), pg_catalog.clock_timestamp()
  );
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  v_result := public.pay_workbench_candidate_dirty_apply_job_process(
    v_late_y_job, 100
  );
  IF v_result->>'dirty_apply_cohort_action'
       IS DISTINCT FROM 'COHORT_REISSUED_PENDING_FINALIZATION'
     OR (v_result->>'dirty_apply_cohort_member_count')::integer <> 1 THEN
    RAISE EXCEPTION 'LATE_ARRIVAL_NEXT_COHORT_FAILED'
      USING DETAIL = v_result::text;
  END IF;
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE';
  PERFORM pg_catalog.set_config('cloudtms.scope_generation_finalising', 'false', true);
  PERFORM pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token', '', true);
  EXECUTE 'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED';
  IF (SELECT scope_change_generation FROM public.banking_pay_workbench_jobs
      WHERE id = v_late_y_job) <= v_generation THEN
    RAISE EXCEPTION 'LATE_ARRIVAL_GENERATION_NOT_ADVANCED';
  END IF;

  --------------------------------------------------------------------------
  -- POLICY_X_NO_MONEY_EFFECT: processors above may dirty pre-draft metadata;
  -- they never create economic/payment/provider/settlement/remittance rows.
  --------------------------------------------------------------------------
  IF (SELECT count(*) FROM public.banking_pay_operations) <> v_operations_before
     OR (SELECT count(*) FROM public.pay_batches) <> v_batches_before
     OR (SELECT count(*) FROM public.pay_batch_items) <> v_batch_items_before
     OR (SELECT count(*) FROM public.banking_pay_operation_provider_attempts)
          <> v_provider_before
     OR (SELECT count(*) FROM public.banking_pay_operation_settlement_scope)
          <> v_settlement_before
     OR (SELECT count(*) FROM public.banking_pay_operation_remittance_scope)
          <> v_remittance_before
     OR (SELECT pg_catalog.to_jsonb(batch_row)
         FROM public.pay_batches AS batch_row WHERE id = v_batch_id)
          IS DISTINCT FROM v_batch_before_json
     OR (SELECT pg_catalog.to_jsonb(candidate_row)
         FROM public.pay_batch_candidates AS candidate_row
         WHERE id = v_batch_candidate_id)
          IS DISTINCT FROM v_batch_candidate_before_json
     OR (SELECT pg_catalog.to_jsonb(request_row)
         FROM public.pay_payment_correction_requests AS request_row
         WHERE id = v_correction_request_id)
          IS DISTINCT FROM v_correction_request_before_json
     OR (SELECT pg_catalog.to_jsonb(operation_row)
         FROM public.banking_pay_operations AS operation_row
         WHERE id = v_correction_operation_id)
          IS DISTINCT FROM v_correction_operation_before_json THEN
    RAISE EXCEPTION 'POLICY_X_NO_MONEY_EFFECT_FAILED';
  END IF;

  IF EXISTS (SELECT 1 FROM pg_temp._bpay_dirty_cohort_poison_observed) THEN
    RAISE EXCEPTION 'HOSTILE_TEMP_TABLE_TRIGGER_EXECUTED';
  END IF;

  IF pg_catalog.to_regclass(
       'pg_temp._bpay_candidate_dirty_cohort_members_v1'
     )::oid IS DISTINCT FROM v_poison_members_oid
     OR pg_catalog.to_regclass(
       'pg_temp._bpay_candidate_dirty_cohort_roots_v1'
     )::oid IS DISTINCT FROM v_poison_roots_oid
     OR (SELECT count(*)
         FROM pg_catalog.pg_trigger AS trigger_row
         WHERE trigger_row.tgrelid IN (
           v_poison_members_oid, v_poison_roots_oid
         )
           AND NOT trigger_row.tgisinternal) <> 2 THEN
    RAISE EXCEPTION 'HOSTILE_TEMP_TABLE_CROSS_ROLE_IDENTITY_CHANGED';
  END IF;

  RAISE NOTICE
    'PASS: ALL_PLUS_TARGETED, DISJOINT_TARGETED, ACTIVE_MEMBER_COUNT_101 (% ms), TARGETED_FINALIZED_TO_FULL_DRIFT, EXACT_FULL_REPLAY, CORRECTION_OWNED_EXCLUSION, LATE_ARRIVAL, RESPONSE_LOSS_REPLAY, DIRECT_INDEX_PLAN, LEGACY_INDEX_PLAN, HOSTILE_TEMP_TABLE_CROSS_ROLE and POLICY_X_NO_MONEY_EFFECT.',
    pg_catalog.round(v_scale_elapsed_ms, 2);
END;
$verification$;

ROLLBACK;
