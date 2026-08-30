\set ON_ERROR_STOP on

-- Installed catalogue guard for the QR-only Office evidence label authority.
-- The full rollback-contained PAPER first-use remains
-- tests/30082026_2207_candidate_paper_evidence_manifest_label_runtime_verification.sql.

do $candidate_paper_evidence_manifest_label_verification$
declare
  v_oid oid;
  v_definition text;
  v_owner name;
  v_security_definer boolean;
  v_volatility "char";
  v_config text[];
begin
  v_oid:=pg_catalog.to_regprocedure(
    'public.timesheet_expense_apply_atomic_v1(uuid,text,uuid,uuid,integer,text,jsonb,uuid[],text,timestamp with time zone)'
  );
  if v_oid is null
     or not pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE')
     or pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE') then
    raise exception 'CANDIDATE_PAPER_EVIDENCE_MANIFEST_LABEL_ACL_INVALID';
  end if;

  select
    pg_catalog.pg_get_functiondef(p.oid),
    owner_role.rolname,
    p.prosecdef,
    p.provolatile,
    p.proconfig
  into v_definition,v_owner,v_security_definer,v_volatility,v_config
  from pg_catalog.pg_proc p
  join pg_catalog.pg_roles owner_role on owner_role.oid=p.proowner
  where p.oid=v_oid;

  if v_owner<>current_user
     or v_security_definer is not true
     or v_volatility<>'v'
     or v_config is distinct from array['search_path=pg_catalog, public, private, pg_temp']::text[] then
    raise exception 'CANDIDATE_PAPER_EVIDENCE_MANIFEST_LABEL_SECURITY_INVALID';
  end if;

  if position(
       'case when v_is_paper then nullif(v_paper_page->>''display_name'','''') end'
       in lower(regexp_replace(v_definition,'\s+',' ','g'))
     )=0
     or position(
       'nullif(v_claim->>''evidence_display_name'','''')'
       in lower(regexp_replace(v_definition,'\s+',' ','g'))
     )=0
     or v_definition~*'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
    raise exception 'CANDIDATE_PAPER_EVIDENCE_MANIFEST_LABEL_AUTHORITY_INVALID';
  end if;
end
$candidate_paper_evidence_manifest_label_verification$;

select 'PASS'::text as candidate_paper_evidence_manifest_label_verification;
