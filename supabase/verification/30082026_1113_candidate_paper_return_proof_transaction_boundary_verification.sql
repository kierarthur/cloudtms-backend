-- Installed transaction and ACL authority for returned-paper QR proof.

do $candidate_paper_return_proof_transaction_boundary_verification$
declare
  v_oid oid;
  v_security_definer boolean;
  v_volatility "char";
  v_config text[];
  v_owner text;
  v_definition text;
begin
  select p.oid,p.prosecdef,p.provolatile,p.proconfig,owner_role.rolname
    into v_oid,v_security_definer,v_volatility,v_config,v_owner
  from pg_catalog.pg_proc p
  join pg_catalog.pg_roles owner_role on owner_role.oid=p.proowner
  where p.oid=pg_catalog.to_regprocedure(
    'public.candidate_paper_return_proof_validate_v1(uuid,text,uuid,integer,text,text,text,text,timestamp with time zone)'
  );

  if not found
     or not v_security_definer
     or v_volatility<>'v'
     or v_owner<>current_user
     or v_config is distinct from
        array['search_path=pg_catalog, public, private, extensions, pg_temp']::text[]
     or not pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE')
     or pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE') then
    raise exception 'CANDIDATE_PAPER_RETURN_PROOF_TRANSACTION_BOUNDARY_INVALID';
  end if;

  v_definition:=pg_catalog.pg_get_functiondef(v_oid);
  if v_definition not like '%_candidate_session_context_v1(%true%'
     or v_definition not like '%CANDIDATE_PAPER_QR_PROOF_MISMATCH%'
     or v_definition not like '%CANDIDATE_PAPER_QR_PROOF_FORBIDDEN%' then
    raise exception 'CANDIDATE_PAPER_RETURN_PROOF_LOCK_OR_FAIL_CLOSED_AUTHORITY_MISSING';
  end if;
end
$candidate_paper_return_proof_transaction_boundary_verification$;

select 'PASS'::text as candidate_paper_return_proof_transaction_boundary_verification;
