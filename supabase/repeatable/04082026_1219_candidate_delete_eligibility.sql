-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.candidate_delete_eligibility(p_candidate_id uuid)
-- Installed pg_get_functiondef MD5: 9bf31e97ea39e80783f47a7782ef4ff7
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.candidate_delete_eligibility(p_candidate_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_contracts int;
  v_contract_weeks int;
  v_timesheets int;
  v_invoice_lines int;
  v_pay_batches int;
  v_reason text := null;
  v_can boolean := true;
  v_active_builds int:=0;
  v_current_scale_blocks int:=0;
  v_started_attempts int:=0;
  v_active_workbench_jobs int:=0;
BEGIN
  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'candidate_id is required';
  END IF;

  SELECT COUNT(*) INTO v_contracts
  FROM public.contracts ct
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_contract_weeks
  FROM public.contract_weeks cw
  JOIN public.contracts ct ON ct.id = cw.contract_id
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_timesheets
  FROM public.timesheets t
  JOIN public.contracts ct ON ct.id = t.contract_id
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_invoice_lines
  FROM public.invoice_lines il
  JOIN public.timesheets t ON t.timesheet_id = il.timesheet_id
  JOIN public.contracts ct ON ct.id = t.contract_id
  WHERE ct.candidate_id = p_candidate_id;

  SELECT COUNT(*) INTO v_pay_batches
  FROM public.pay_batch_candidates pbc
  WHERE pbc.candidate_id = p_candidate_id;

  SELECT count(*)::integer INTO v_active_builds
  FROM private.banking_pay_workbench_economic_builds build_row
  WHERE build_row.candidate_id=p_candidate_id
    AND build_row.status IN ('COLLECTING','READY_FOR_RECONCILIATION','RECONCILING',
      'RECONCILED','PUBLISHING','CLEANING');
  SELECT count(*)::integer INTO v_current_scale_blocks
  FROM private.banking_pay_workbench_candidate_scope_registry registry
  JOIN private.banking_pay_workbench_economic_builds build_row ON build_row.id=registry.current_build_id
  WHERE registry.candidate_id=p_candidate_id
    AND build_row.status='BLOCKED_UNVALIDATED_RECONCILIATION_SCALE';
  SELECT count(*)::integer INTO v_started_attempts
  FROM private.banking_pay_workbench_stage_attempts attempt
  WHERE attempt.candidate_id=p_candidate_id AND attempt.attempt_status='STARTED';
  SELECT count(*)::integer INTO v_active_workbench_jobs
  FROM public.banking_pay_workbench_jobs job
  WHERE job.candidate_id=p_candidate_id AND job.status NOT IN ('SUCCEEDED','FAILED','DEAD');

  IF v_contracts > 0 OR v_contract_weeks > 0 OR v_timesheets > 0 OR v_invoice_lines > 0 OR v_pay_batches > 0
     OR v_active_builds>0 OR v_current_scale_blocks>0 OR v_started_attempts>0 OR v_active_workbench_jobs>0 THEN
    v_can := false;
    v_reason := 'Candidate cannot be deleted because related records exist.';
  END IF;

  RETURN jsonb_build_object(
    'candidate_id', p_candidate_id::text,
    'can_delete', v_can,
    'reason', v_reason,
    'blockers', jsonb_build_object(
      'contracts', v_contracts,
      'contract_weeks', v_contract_weeks,
      'timesheets', v_timesheets,
      'invoice_lines', v_invoice_lines,
      'pay_batch_candidates', v_pay_batches,
      'active_banking_pay_builds',v_active_builds,
      'current_scale_blocked_builds',v_current_scale_blocks,
      'started_banking_pay_attempts',v_started_attempts,
      'active_workbench_jobs',v_active_workbench_jobs
    )
  );
END;
$function$;

ALTER FUNCTION public.candidate_delete_eligibility(p_candidate_id uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.candidate_delete_eligibility(p_candidate_id uuid) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.candidate_delete_eligibility(p_candidate_id uuid) TO postgres;
GRANT EXECUTE ON FUNCTION public.candidate_delete_eligibility(p_candidate_id uuid) TO authenticated;
GRANT EXECUTE ON FUNCTION public.candidate_delete_eligibility(p_candidate_id uuid) TO service_role;
