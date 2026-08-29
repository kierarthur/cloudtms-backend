-- Current Candidate-named relation/RPC security authority.
-- Supersedes the execution of the protected 22082026_1402 verifier without
-- rewriting that historical boundary. The manager-email and final Candidate
-- functions added on 23 August are provider-owner-normalised; browser execution
-- must remain zero.

do $candidate_named_security_verification_v2$
declare
  v_count integer;
  v_service_missing integer;
  v_browser_executable integer;
  v_hash text;
begin
  with targets as (
    select
      c.relname,c.relrowsecurity,
      pg_catalog.has_table_privilege('service_role',c.oid,'SELECT') as svc_select,
      pg_catalog.has_table_privilege('service_role',c.oid,'INSERT') as svc_insert,
      pg_catalog.has_table_privilege('service_role',c.oid,'UPDATE') as svc_update,
      pg_catalog.has_table_privilege('service_role',c.oid,'DELETE') as svc_delete,
      pg_catalog.has_table_privilege('anon',c.oid,'SELECT,INSERT,UPDATE,DELETE') as anon_access,
      pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT,INSERT,UPDATE,DELETE') as auth_access
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind in ('r','p')
      and c.relname in (
        'banking_pay_snapshot_candidate_state',
        'banking_pay_workbench_session_candidate_state',
        'legacy_eclipse_candidate_map',
        'pay_batch_candidates',
        'rates_candidate_overrides',
        'candidates_tombstones'
      )
  )
  select pg_catalog.count(*),
         pg_catalog.md5(pg_catalog.string_agg(
           relname||'|'||relrowsecurity::text||'|'||
           svc_select::text||svc_insert::text||svc_update::text||svc_delete::text||'|'||
           anon_access::text||'|'||auth_access::text,
           E'\n' order by relname
         ))
  into v_count,v_hash from targets;

  if v_count<>6 or v_hash<>'bc44e32bc6dc29e429d3555177cba049' then
    raise exception 'CANDIDATE_NAMED_TABLE_ISOLATION_FAILED:count=% hash=%',v_count,v_hash;
  end if;

  with targets as (
    select
      c.relname,
      ('security_invoker=true'=any(coalesce(c.reloptions,array[]::text[]))) as invoker,
      pg_catalog.has_table_privilege('service_role',c.oid,'SELECT') as svc_select,
      pg_catalog.has_table_privilege('anon',c.oid,'SELECT') as anon_select,
      pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT') as auth_select
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='v'
      and c.relname in (
        'v_legacy_candidate_contract_summary',
        'v_legacy_client_candidates',
        'v_legacy_contracts_by_candidate',
        'candidates_summary',
        'candidates_summary_activity',
        'v_mailshot_src_candidate',
        'vw_picker_candidates'
      )
  )
  select pg_catalog.count(*),
         pg_catalog.md5(pg_catalog.string_agg(
           relname||'|'||invoker::text||'|'||svc_select::text||'|'||
           anon_select::text||'|'||auth_select::text,
           E'\n' order by relname
         ))
  into v_count,v_hash from targets;

  if v_count<>7 or v_hash<>'e91248c342b81c4220fe257b2d76941a' then
    raise exception 'CANDIDATE_NAMED_VIEW_ISOLATION_FAILED:count=% hash=%',v_count,v_hash;
  end if;

  with targets as (
    select
      n.nspname||'.'||p.proname||'('||coalesce((
        select pg_catalog.string_agg(
          type_namespace.nspname||'.'||argument_type.typname,
          ',' order by argument.argument_ordinal
        )
        from pg_catalog.unnest(p.proargtypes::oid[]) with ordinality
          as argument(type_oid,argument_ordinal)
        join pg_catalog.pg_type argument_type on argument_type.oid=argument.type_oid
        join pg_catalog.pg_namespace type_namespace on type_namespace.oid=argument_type.typnamespace
      ),'')||')' as signature,
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute,
      pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
      pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE') as auth_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public' and p.prosecdef and p.proname ilike '%candidate%'
  )
  select
    pg_catalog.count(*),
    pg_catalog.count(*) filter (where not svc_execute),
    pg_catalog.count(*) filter (where anon_execute or auth_execute),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text||'|'||anon_execute::text||'|'||auth_execute::text,
      E'\n' order by signature
    ),''))
  into v_count,v_service_missing,v_browser_executable,v_hash
  from targets;

  if v_count<>105 or v_service_missing<>8 or v_browser_executable<>0
     or v_hash<>'4166c08c7abd5e9ed638091c182ce2e5' then
    raise exception 'CANDIDATE_NAMED_RPC_ISOLATION_FAILED:count=% service_missing=% browser_executable=% hash=%',
      v_count,v_service_missing,v_browser_executable,v_hash;
  end if;

  if not pg_catalog.has_function_privilege(
    'anon','public.cloudtms_data_api_mfa_gate()','EXECUTE'
  ) or not pg_catalog.has_function_privilege(
    'authenticated','public.cloudtms_data_api_mfa_gate()','EXECUTE'
  ) then
    raise exception 'CANDIDATE_NAMED_MFA_PRE_REQUEST_EXECUTE_CONTRACT_CHANGED';
  end if;
end
$candidate_named_security_verification_v2$;
