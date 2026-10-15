-- Source-only proposed verifier. Not executed and not canonically registered.
-- Existing named Candidate v3 inventory remains required after the final
-- closure. These are replacements of three existing identities; no public
-- signature, owner, grant, route enum, table or column is added here.
do $worker_only_candidate_security_verification$
declare
  v_target record;
  v_oid oid;
  v_seen integer:=0;
begin
  for v_target in select * from (values
    ('public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamp with time zone)',true),
    ('public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)',true),
    ('public.candidate_submission_finalize_single_flight_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)',true),
    ('private._candidate_component_render_contract_v1(uuid,integer,uuid,text)',false)
  ) targets(signature,service_executable)
  loop
    v_oid:=pg_catalog.to_regprocedure(v_target.signature);
    if v_oid is null then
      raise exception 'CANDIDATE_WORKER_ONLY_SECURITY_OWNER_MISSING:%',v_target.signature;
    end if;
    if not exists(select 1 from pg_catalog.pg_proc routine where routine.oid=v_oid
      and routine.prosecdef
      and exists(select 1 from pg_catalog.unnest(routine.proconfig) setting where setting like 'search_path=%'))
      or pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE') is distinct from v_target.service_executable
      or pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
      or pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE')
      or exists(select 1 from pg_catalog.pg_proc routine
        cross join lateral pg_catalog.aclexplode(coalesce(routine.proacl,pg_catalog.acldefault('f',routine.proowner))) acl
        where routine.oid=v_oid and acl.grantee=0 and acl.privilege_type='EXECUTE') then
      raise exception 'CANDIDATE_WORKER_ONLY_SECURITY_BOUNDARY_INVALID:%',v_target.signature;
    end if;
    v_seen:=v_seen+1;
  end loop;
  if v_seen<>4 then raise exception 'CANDIDATE_WORKER_ONLY_SECURITY_INVENTORY_INVALID';end if;
end;
$worker_only_candidate_security_verification$;
