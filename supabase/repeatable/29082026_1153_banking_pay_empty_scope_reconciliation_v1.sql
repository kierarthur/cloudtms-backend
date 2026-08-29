-- Banking Pay bounded-source correction for a candidate with no current
-- economic Timesheet scope. The legacy empty-scope projection deliberately
-- performs no finance mutation, but it predates the durable bounded-build
-- reconciliation envelope. This adapter completes that existing zero-line
-- authority so SOURCE_PUBLISH can safely supersede any older current lines.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_reconcile_empty_scope_v1(
  p_job_id uuid,
  p_build_id uuid,
  p_attempt_id uuid,
  p_attempt_nonce uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
DECLARE
  v_build private.banking_pay_workbench_economic_builds%ROWTYPE;
  v_registry private.banking_pay_workbench_candidate_scope_registry%ROWTYPE;
  v_job public.banking_pay_workbench_jobs%ROWTYPE;
  v_attempt private.banking_pay_workbench_stage_attempts%ROWTYPE;
  v_fact_count bigint:=0;
  v_fact_digest text:=md5('');
  v_post_sync_digest text:=md5('');
  v_canonical_digest text:=md5('');
  v_effect_digest text:=md5('[]');
  v_observed_effect_digest text:=md5('');
  v_now timestamptz:=clock_timestamp();
BEGIN
  SELECT * INTO v_build
  FROM private.banking_pay_workbench_economic_builds
  WHERE id=p_build_id
  FOR UPDATE;
  SELECT * INTO v_job
  FROM public.banking_pay_workbench_jobs
  WHERE id=p_job_id
  FOR UPDATE;
  SELECT * INTO v_attempt
  FROM private.banking_pay_workbench_stage_attempts
  WHERE id=p_attempt_id
  FOR UPDATE;
  SELECT * INTO v_registry
  FROM private.banking_pay_workbench_candidate_scope_registry
  WHERE candidate_id=v_build.candidate_id
  FOR UPDATE;

  IF v_build.id IS NULL OR v_job.id IS NULL OR v_attempt.id IS NULL
     OR v_registry.candidate_id IS NULL
     OR v_job.economic_build_id IS DISTINCT FROM p_build_id
     OR v_job.session_id IS DISTINCT FROM v_build.session_id
     OR v_job.candidate_id IS DISTINCT FROM v_build.candidate_id
     OR v_job.status IS DISTINCT FROM 'RUNNING'
     OR v_job.private_stage IS DISTINCT FROM 'RECONCILE_EXECUTE'
     OR v_build.source_job_id IS DISTINCT FROM p_job_id
     OR v_build.status IS DISTINCT FROM 'RECONCILING'
     OR v_build.private_stage IS DISTINCT FROM 'RECONCILE_EXECUTE'
     OR v_registry.current_build_id IS DISTINCT FROM p_build_id
     OR v_registry.dirty_generation IS DISTINCT FROM v_build.captured_candidate_generation
     OR v_registry.current_source_change_seq IS DISTINCT FROM v_build.source_change_seq
     OR v_attempt.job_id IS DISTINCT FROM p_job_id
     OR v_attempt.build_id IS DISTINCT FROM p_build_id
     OR v_attempt.candidate_id IS DISTINCT FROM v_build.candidate_id
     OR v_attempt.private_stage IS DISTINCT FROM 'RECONCILE_EXECUTE'
     OR v_attempt.attempt_nonce IS DISTINCT FROM p_attempt_nonce
     OR v_attempt.attempt_status IS DISTINCT FROM 'STARTED'
     OR clock_timestamp()>=v_attempt.lease_expires_at_utc THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_ATTEMPT_STALE'
      USING ERRCODE='40001';
  END IF;

  IF v_build.scope_count<>0
     OR v_build.row_seal_count<>0
     OR v_build.dependency_node_count<>0
     OR EXISTS(
       SELECT 1
       FROM private.banking_pay_workbench_economic_build_scope AS scope_row
       WHERE scope_row.build_id=p_build_id
       LIMIT 1
     )
     OR v_build.dependency_closure_sealed_at_utc IS NULL
     OR v_build.dependency_edge_stream_complete IS NOT TRUE
     OR v_build.edge_tag_stream_complete IS NOT TRUE
     OR COALESCE((v_build.attestation_json->>'effect_plan_sealed')::boolean,false) IS NOT TRUE
     OR COALESCE((v_build.attestation_json->>'effect_plan_count')::integer,-1)<>0
     OR v_build.attestation_json->>'effect_plan_digest' IS DISTINCT FROM v_effect_digest
     OR EXISTS(
       SELECT 1
       FROM private.banking_pay_workbench_economic_build_facts AS effect_row
       WHERE effect_row.build_id=p_build_id
         AND effect_row.fact_family='EXPECTED_FINANCE_EFFECT'
       LIMIT 1
     ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_EMPTY_SCOPE_RECONCILIATION_INVALID'
      USING ERRCODE='23514';
  END IF;

  SELECT count(*)::bigint,
         md5(COALESCE(string_agg(
           fact_family||':'||natural_key||':'||financial_digest,''
           ORDER BY fact_family,natural_key
         ),''))
  INTO v_fact_count,v_fact_digest
  FROM private.banking_pay_workbench_economic_build_facts
  WHERE build_id=p_build_id
    AND fact_family NOT IN ('DEPENDENCY_EDGE','EXPECTED_FINANCE_EFFECT');

  SELECT md5(jsonb_build_object(
    'candidate_id',v_build.candidate_id,
    'case_count',count(*),
    'case_digest',md5(COALESCE(string_agg(
      md5(to_jsonb(case_row)::text),'' ORDER BY case_row.id
    ),''))
  )::text)
  INTO v_post_sync_digest
  FROM public.pay_advances AS case_row
  WHERE case_row.candidate_id=v_build.candidate_id;

  DELETE FROM private.banking_pay_workbench_canonical_stage_lines
  WHERE build_id=p_build_id;

  UPDATE private.banking_pay_workbench_economic_builds
  SET status='RECONCILED',
      private_stage='SOURCE_PUBLISH',
      fact_count=v_fact_count,
      pre_sync_digest=v_fact_digest,
      post_sync_digest=v_post_sync_digest,
      canonical_count=0,
      canonical_digest=v_canonical_digest,
      attestation_json=COALESCE(attestation_json,'{}'::jsonb)||jsonb_build_object(
        'version',1,
        'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH',
        'build_id',p_build_id,
        'attempt_id',p_attempt_id,
        'scope_count',0,
        'fact_count',v_fact_count,
        'canonical_count',0,
        'effect_plan_sealed',true,
        'effect_plan_count',0,
        'effect_plan_digest',v_effect_digest,
        'observed_finance_effect_count',0,
        'observed_finance_effect_digest',v_observed_effect_digest,
        'economic_component_digest',md5(''),
        'presentation_allocation_digest',md5(''),
        'pre_sync_digest',v_fact_digest,
        'post_sync_digest',v_post_sync_digest,
        'canonical_digest',v_canonical_digest,
        'empty_scope_reconciliation_version',1
      ),
      reconciled_at_utc=v_now,
      publication_cursor_json=jsonb_build_object(
        'cursor_kind','SOURCE_PUBLISH',
        'cursor_version',1,
        'build_id',p_build_id,
        'candidate_id',v_build.candidate_id,
        'canonical_count',0,
        'canonical_digest',v_canonical_digest
      ),
      updated_at_utc=v_now
  WHERE id=p_build_id
    AND status='RECONCILING'
    AND private_stage='RECONCILE_EXECUTE';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_ATTEMPT_STALE'
      USING ERRCODE='40001';
  END IF;

  RETURN jsonb_build_object(
    'ok',true,
    'build_id',p_build_id,
    'attempt_id',p_attempt_id,
    'scope_timesheet_count',0,
    'fact_count',v_fact_count,
    'canonical_stage_count',0,
    'pre_sync_digest',v_fact_digest,
    'post_sync_digest',v_post_sync_digest,
    'canonical_digest',v_canonical_digest,
    'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_reconcile_empty_scope_v1(
  uuid,uuid,uuid,uuid
) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_reconcile_empty_scope_v1(
  uuid,uuid,uuid,uuid
) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_reconcile_empty_scope_v1(
  uuid,uuid,uuid,uuid
) TO postgres;

\ir 04082026_1210_pay_sync_overpayments_from_preview.sql

NOTIFY pgrst, 'reload schema';

commit;
