-- Read-only content evidence over the complete existing canonical candidate
-- facts. Digests exclude navigation revisions, not payment or Timesheet facts.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_state_v2(
  p_session public.banking_pay_workbench_sessions,p_channel text,p_candidate_id uuid
) RETURNS jsonb LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_scope text;v_binding jsonb;v_result jsonb;
BEGIN
  IF p_session.id IS NULL OR p_session.version IS NULL OR p_session.progress_counter_version IS NULL
     OR p_session.version<0 OR p_session.progress_counter_version<0
     OR p_channel IS NULL OR p_channel NOT IN ('ALL','PAYE','UMBRELLA') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  v_scope:=private.pay_workbench_modal_scope_hash_v2(p_session,p_channel);
  IF COALESCE(v_scope,'') !~ '^[a-f0-9]{64}$' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  v_binding:=jsonb_build_object('contract','BANKING_PAY_MODAL_STRUCTURE_V2','session_id',p_session.id,
    'session_version',p_session.version,'progress_counter_version',p_session.progress_counter_version,'scope_hash',v_scope);
  WITH facts AS MATERIALIZED (
    SELECT f.*,encode(extensions.digest(convert_to(to_jsonb(f)::text,'UTF8'),'sha256'),'hex') AS fact_hash
    FROM private.pay_workbench_modal_candidate_facts_v2(p_session,p_channel) f
  )
  SELECT jsonb_build_object(
    'candidate_count',count(*),'other_candidate_count',count(*) FILTER(WHERE f.candidate_id IS DISTINCT FROM p_candidate_id),
    'candidate',(SELECT private.pay_workbench_modal_candidate_row_v2(to_jsonb(target)-'fact_hash',v_binding)
      FROM facts target WHERE target.candidate_id=p_candidate_id),
    'view_digest',encode(extensions.digest(convert_to(jsonb_build_object('scope',v_scope,'facts',
      COALESCE(jsonb_agg(jsonb_build_array(f.candidate_id,f.fact_hash) ORDER BY f.candidate_id),'[]'::jsonb))::text,'UTF8'),'sha256'),'hex'),
    'other_candidates_digest',encode(extensions.digest(convert_to(jsonb_build_object('scope',v_scope,'excluded_candidate',p_candidate_id,'facts',
      COALESCE(jsonb_agg(jsonb_build_array(f.candidate_id,f.fact_hash) ORDER BY f.candidate_id)
        FILTER(WHERE f.candidate_id IS DISTINCT FROM p_candidate_id),'[]'::jsonb))::text,'UTF8'),'sha256'),'hex'),
    'membership_digest',encode(extensions.digest(convert_to(jsonb_build_object('scope',v_scope,'candidates',
      COALESCE(jsonb_agg(f.candidate_id ORDER BY f.candidate_id),'[]'::jsonb))::text,'UTF8'),'sha256'),'hex')
  ) INTO v_result FROM facts f;
  RETURN v_result;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_candidate_state_v2(public.banking_pay_workbench_sessions,text,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_candidate_state_v2(public.banking_pay_workbench_sessions,text,uuid) FROM PUBLIC, anon, authenticated, service_role;

commit;
