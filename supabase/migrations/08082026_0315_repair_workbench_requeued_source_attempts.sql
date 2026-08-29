-- One-time TEST-safe repair for source attempts stranded by the obsolete
-- pay_workbench_fail_job body. The public jobs were already requeued with a
-- deterministic stage error, but their private STARTED attempts were not made
-- terminal. The current focused fail owner prevents any recurrence.

DO $migration$
DECLARE
  v_target_count integer:=0;
  v_updated_count integer:=0;
BEGIN
  SELECT count(*)::integer INTO v_target_count
  FROM private.banking_pay_workbench_stage_attempts attempt
  JOIN public.banking_pay_workbench_jobs job ON job.id=attempt.job_id
  WHERE attempt.attempt_status='STARTED'
    AND attempt.lease_expires_at_utc+interval '15 seconds'<clock_timestamp()
    AND job.status='QUEUED'
    AND job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
    AND job.economic_build_id=attempt.build_id
    AND job.private_stage=attempt.private_stage
    AND job.last_error_json->>'code'='23514';

  IF v_target_count>10 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_REQUEUED_SOURCE_ATTEMPT_REPAIR_BOUND_EXCEEDED'
      USING ERRCODE='23514';
  END IF;

  IF EXISTS(
    SELECT 1
    FROM private.banking_pay_workbench_stage_attempts attempt
    JOIN public.banking_pay_workbench_jobs job ON job.id=attempt.job_id
    WHERE attempt.attempt_status='STARTED'
      AND attempt.lease_expires_at_utc+interval '15 seconds'<clock_timestamp()
      AND job.status='QUEUED'
      AND job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND job.economic_build_id=attempt.build_id
      AND job.private_stage=attempt.private_stage
      AND job.last_error_json->>'code'='23514'
    GROUP BY attempt.job_id
    HAVING count(*)<>1
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_REQUEUED_SOURCE_ATTEMPT_REPAIR_PROOF_FAILED'
      USING ERRCODE='23514';
  END IF;

  UPDATE private.banking_pay_workbench_stage_attempts attempt
  SET attempt_status='FAILED',
      failed_at_utc=clock_timestamp(),
      result_code='REQUEUED_STAGE_ERROR_ATTEMPT_REPAIRED',
      error_class='DETERMINISTIC_STAGE_ERROR',
      error_json=jsonb_build_object(
        'code','REQUEUED_STAGE_ERROR_ATTEMPT_REPAIRED',
        'job_error_code','23514'
      ),
      updated_at_utc=clock_timestamp()
  FROM public.banking_pay_workbench_jobs job
  WHERE job.id=attempt.job_id
    AND attempt.attempt_status='STARTED'
    AND attempt.lease_expires_at_utc+interval '15 seconds'<clock_timestamp()
    AND job.status='QUEUED'
    AND job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
    AND job.economic_build_id=attempt.build_id
    AND job.private_stage=attempt.private_stage
    AND job.last_error_json->>'code'='23514';
  GET DIAGNOSTICS v_updated_count=ROW_COUNT;

  IF v_updated_count<>v_target_count THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_REQUEUED_SOURCE_ATTEMPT_REPAIR_UPDATE_MISMATCH'
      USING ERRCODE='23514';
  END IF;

  IF EXISTS(
    SELECT 1
    FROM private.banking_pay_workbench_stage_attempts attempt
    JOIN public.banking_pay_workbench_jobs job ON job.id=attempt.job_id
    WHERE attempt.attempt_status='STARTED'
      AND attempt.lease_expires_at_utc+interval '15 seconds'<clock_timestamp()
      AND job.status='QUEUED'
      AND job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
      AND job.economic_build_id=attempt.build_id
      AND job.private_stage=attempt.private_stage
      AND job.last_error_json->>'code'='23514'
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_REQUEUED_SOURCE_ATTEMPT_REPAIR_INCOMPLETE'
      USING ERRCODE='23514';
  END IF;
END;
$migration$;
