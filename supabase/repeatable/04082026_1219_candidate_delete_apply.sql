-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.candidate_delete_apply(p_candidate_id uuid, p_actor_user_id uuid, p_reason text)
-- Installed pg_get_functiondef MD5: c2e7f4d13b4f4268527717ec9c529bf8
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.candidate_delete_apply(p_candidate_id uuid, p_actor_user_id uuid, p_reason text DEFAULT NULL::text)
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
  v_delete_operation_id uuid:=gen_random_uuid();
  v_candidate_lock_key bigint;
BEGIN
  IF p_candidate_id IS NULL THEN RAISE EXCEPTION 'candidate_id is required'; END IF;
  v_candidate_lock_key:=pg_catalog.hashtextextended(
    public._pay_workbench_candidate_serial_key(p_candidate_id),24062027);
  PERFORM pg_catalog.pg_advisory_xact_lock(v_candidate_lock_key);

  -- Lock candidate to ensure consistent delete
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

  PERFORM 1 FROM private.banking_pay_workbench_candidate_scope_registry
  WHERE candidate_id=p_candidate_id FOR UPDATE;
  PERFORM 1 FROM private.banking_pay_workbench_economic_builds
  WHERE candidate_id=p_candidate_id ORDER BY id FOR UPDATE;
  PERFORM 1 FROM public.banking_pay_workbench_jobs
  WHERE candidate_id=p_candidate_id ORDER BY id FOR UPDATE;
  PERFORM 1 FROM private.banking_pay_workbench_stage_attempts
  WHERE candidate_id=p_candidate_id ORDER BY id FOR UPDATE;

  v_elig := public.candidate_delete_eligibility(p_candidate_id);
  v_can := COALESCE((v_elig->>'can_delete')::boolean, false);
  IF v_can IS NOT TRUE THEN
    RAISE EXCEPTION '%', COALESCE(v_elig->>'reason', 'Candidate cannot be deleted');
  END IF;

  IF pg_catalog.to_regclass('pg_temp._bpay_candidate_delete_context_v1') IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_DELETE_CONTEXT_CONFLICT' USING ERRCODE='23514';
  END IF;
  CREATE TEMP TABLE pg_temp._bpay_candidate_delete_context_v1(
    candidate_id uuid PRIMARY KEY,
    delete_operation_id uuid UNIQUE NOT NULL,
    candidate_lock_key bigint NOT NULL,
    backend_pid integer NOT NULL,
    transaction_id bigint NOT NULL,
    created_at_utc timestamptz NOT NULL,
    suppress boolean NOT NULL CHECK(suppress)
  ) ON COMMIT DROP;
  INSERT INTO pg_temp._bpay_candidate_delete_context_v1 VALUES(
    p_candidate_id,v_delete_operation_id,v_candidate_lock_key,pg_catalog.pg_backend_pid(),
    pg_catalog.txid_current(),clock_timestamp(),true);

  -- Preserve terminal public job diagnostics while severing candidate/build
  -- foreign-key authority before the private graph cascades.
  UPDATE public.banking_pay_workbench_jobs SET candidate_id=NULL,economic_build_id=NULL,
    private_stage=NULL,private_cursor_kind=NULL,private_cursor_json='{}'::jsonb,
    private_stage_version=NULL,
    updated_at_utc=clock_timestamp(),payload_json=COALESCE(payload_json,'{}'::jsonb)
      ||jsonb_build_object('deleted_candidate_id',p_candidate_id,
        'candidate_delete_operation_id',v_delete_operation_id,
        'detached_private_stage',private_stage,
        'detached_private_cursor_kind',private_cursor_kind)
  WHERE candidate_id=p_candidate_id AND status IN ('SUCCEEDED','FAILED');

  -- Non-blocking dependents (verified tables)
  DELETE FROM public.candidate_job_titles cjt WHERE cjt.candidate_id = p_candidate_id;
  DELETE FROM public.rates_candidate_overrides rco WHERE rco.candidate_id = p_candidate_id;
  DELETE FROM public.legacy_eclipse_candidate_map lem WHERE lem.candidate_id = p_candidate_id;

  -- Delete candidate
  DELETE FROM public.candidates c WHERE c.id = p_candidate_id;

  IF EXISTS(SELECT 1 FROM private.banking_pay_workbench_candidate_scope_registry WHERE candidate_id=p_candidate_id)
     OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_timesheet_scope_state WHERE candidate_id=p_candidate_id)
     OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_economic_builds WHERE candidate_id=p_candidate_id)
     OR EXISTS(SELECT 1 FROM private.banking_pay_workbench_stage_attempts WHERE candidate_id=p_candidate_id) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_DELETE_PRIVATE_GRAPH_REMAINS' USING ERRCODE='23514';
  END IF;

  -- Bump change counter and record deleted_rev
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
    jsonb_build_object('deleted_rev', v_deleted_rev),
    COALESCE(p_reason, 'DELETE'),
    p_actor_user_id
  );

  RETURN jsonb_build_object('deleted', true, 'candidate_id', p_candidate_id::text, 'deleted_rev', v_deleted_rev);
END;
$function$;

ALTER FUNCTION public.candidate_delete_apply(p_candidate_id uuid, p_actor_user_id uuid, p_reason text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.candidate_delete_apply(p_candidate_id uuid, p_actor_user_id uuid, p_reason text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.candidate_delete_apply(p_candidate_id uuid, p_actor_user_id uuid, p_reason text) TO postgres;
GRANT EXECUTE ON FUNCTION public.candidate_delete_apply(p_candidate_id uuid, p_actor_user_id uuid, p_reason text) TO authenticated;
GRANT EXECUTE ON FUNCTION public.candidate_delete_apply(p_candidate_id uuid, p_actor_user_id uuid, p_reason text) TO service_role;
