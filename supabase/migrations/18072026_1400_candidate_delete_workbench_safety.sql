-- Candidate deletion must preserve terminal Banking Pay workbench history without
-- leaving a live foreign-key dependency, and it must never enqueue work for a
-- candidate after that candidate has been deleted.

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
  v_workbench_active int;
  v_workbench_terminal int;
  v_reason text := null;
  v_can boolean := true;
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

  SELECT
    COUNT(*) FILTER (
      WHERE UPPER(BTRIM(COALESCE(j.status, ''))) IN ('QUEUED', 'RUNNING')
    ),
    COUNT(*) FILTER (
      WHERE UPPER(BTRIM(COALESCE(j.status, ''))) IN ('SUCCEEDED', 'FAILED', 'DEAD')
    )
  INTO v_workbench_active, v_workbench_terminal
  FROM public.banking_pay_workbench_jobs j
  WHERE j.candidate_id = p_candidate_id;

  IF v_contracts > 0
     OR v_contract_weeks > 0
     OR v_timesheets > 0
     OR v_invoice_lines > 0
     OR v_pay_batches > 0
     OR v_workbench_active > 0 THEN
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
      'banking_pay_workbench_jobs_active', v_workbench_active,
      'banking_pay_workbench_jobs_terminal', v_workbench_terminal
    )
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.candidate_delete_apply(
  p_candidate_id uuid,
  p_actor_user_id uuid,
  p_reason text DEFAULT NULL::text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_elig jsonb;
  v_can boolean;
  v_deleted_rev bigint;
  v_seq bigint;
  v_before jsonb;
  v_detached_terminal_workbench_jobs int := 0;
BEGIN
  v_elig := public.candidate_delete_eligibility(p_candidate_id);
  v_can := COALESCE((v_elig->>'can_delete')::boolean, false);

  IF v_can IS NOT TRUE THEN
    RAISE EXCEPTION '%', COALESCE(v_elig->>'reason', 'Candidate cannot be deleted');
  END IF;

  -- Lock candidate to ensure consistent delete.
  SELECT jsonb_build_object(
           'id', c.id::text,
           'tms_ref', c.tms_ref,
           'display_name', c.display_name,
           'active', c.active
         )
    INTO v_before
  FROM public.candidates c
  WHERE c.id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Candidate not found';
  END IF;

  -- Non-blocking dependents (verified tables).
  DELETE FROM public.candidate_job_titles cjt WHERE cjt.candidate_id = p_candidate_id;
  DELETE FROM public.rates_candidate_overrides rco WHERE rco.candidate_id = p_candidate_id;
  DELETE FROM public.legacy_eclipse_candidate_map lem WHERE lem.candidate_id = p_candidate_id;

  -- Terminal workbench jobs are immutable history. Keep every job and payload,
  -- but remove the live candidate foreign-key edge before deleting the candidate.
  UPDATE public.banking_pay_workbench_jobs j
  SET candidate_id = NULL,
      updated_at_utc = now()
  WHERE j.candidate_id = p_candidate_id
    AND UPPER(BTRIM(COALESCE(j.status, ''))) IN ('SUCCEEDED', 'FAILED', 'DEAD');

  GET DIAGNOSTICS v_detached_terminal_workbench_jobs = ROW_COUNT;

  -- Delete candidate. Active workbench jobs remain a foreign-key backstop if a
  -- new active job appears after the eligibility check.
  DELETE FROM public.candidates c WHERE c.id = p_candidate_id;

  -- Bump change counter and record deleted_rev.
  PERFORM public._change_bump('candidates');
  SELECT acc.seq INTO v_seq
  FROM public.app_change_counters acc
  WHERE acc.entity_key = 'candidates'
  LIMIT 1;

  v_deleted_rev := COALESCE(v_seq, 0);

  INSERT INTO public.candidates_tombstones (id, deleted_rev, deleted_at)
  VALUES (p_candidate_id, v_deleted_rev, now())
  ON CONFLICT (id) DO UPDATE
    SET deleted_rev = EXCLUDED.deleted_rev,
        deleted_at  = EXCLUDED.deleted_at;

  PERFORM public._audit_insert(
    'candidates',
    p_candidate_id::text,
    'DELETE',
    v_before,
    jsonb_build_object(
      'deleted_rev', v_deleted_rev,
      'detached_terminal_workbench_jobs', v_detached_terminal_workbench_jobs
    ),
    COALESCE(p_reason, 'DELETE'),
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'deleted', true,
    'candidate_id', p_candidate_id::text,
    'deleted_rev', v_deleted_rev,
    'detached_terminal_workbench_jobs', v_detached_terminal_workbench_jobs
  );
END;
$function$;

-- Candidate INSERT/UPDATE still dirties pre-draft Banking Pay live truth.
-- Candidate DELETE cannot produce a valid candidate-scoped refresh and must not
-- enqueue a foreign-key reference to the row that has just been deleted.
DROP TRIGGER IF EXISTS trg_pay_workbench_mark_candidate_dirty__candidates
ON public.candidates;

CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__candidates
AFTER INSERT OR UPDATE ON public.candidates
FOR EACH ROW
EXECUTE FUNCTION public.pay_workbench_mark_candidate_dirty();
