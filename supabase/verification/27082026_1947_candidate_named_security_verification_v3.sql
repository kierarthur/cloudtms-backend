-- Supersedes the execution of the historical v2 verifier without rewriting it.
-- Adds the service-only Candidate Timesheet Summary cursor, automatic manager
-- finalisation recovery, reject-before-delete guard RPCs, and the two durable
-- Candidate document-render recovery readers to the exact public
-- Candidate RPC inventory; browser execution remains zero.

do $candidate_named_security_verification_v3$
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

  -- Banking Pay Modal Structure v2, its bounded candidate Ready-group detail,
  -- its replaced-session candidate-owner
  -- repair, the three service-only MyTMS Places and contacts functions, the
  -- weekly paper-target preparation functions, the four QR signed-pack V2
  -- manifest/proof/component/whole-pack functions, the exact service-only
  -- legacy one-page cancellation wrapper, manager-finalisation single-flight
  -- wrapper, two reject-before-delete guard functions, the target-less
  -- standalone expense PAPER pack helper, and the closed Advanced Expense
  -- component/summary/Office authorities are present. The inner Office
  -- category-rejection atomic remains deliberately inaccessible to service_role
  -- because only its guarded Office adapter may invoke it. Plan 6 adds nine
  -- service-only Candidate Weekly Source functions. Browser execution remains
  -- exactly zero and the unrelated service-missing count remains unchanged.
  --
  -- MOVED 162 -> 163 at the Plan 6.2 final seals pass (WP-15d), 18 September
  -- 2026. This is a NEW seal movement: HANDOVER 2 round-5 Part C does not cover
  -- it, because the routine that causes it did not exist when that ruling was
  -- written. It is reported to the approver as a new movement rather than under
  -- the Part C approved list. Part C's governing reason for the sibling
  -- browser-isolation RPC seal applies identically here: a stale inventory seal
  -- may not ship even when the security property holds.
  --
  -- Previous seal: count=162 service_missing=9 browser_executable=0
  --                hash=9a8763756da0c15664eea8e8f29874f7
  -- Measured on a full NEW build from empty (241 migrations, 645 repeatables):
  --                count=163 service_missing=9 browser_executable=0
  --                hash=30ab67eac33606d24d9832e5f79b1681
  --
  -- Proof: the same inventory query run against ws62_wp15d_base, a clone of the
  -- pre-Plan-6.2 ws62_template, reproduces the previous seal exactly, which is
  -- what makes the comparison trustworthy. The set difference against it is
  -- 1 row present in NEW, 0 absent from NEW and 0 changed:
  --
  --   public.weekly_source_candidate_hours_push_v1(pg_catalog.jsonb)|true|false|false
  --
  -- That single routine is WP-14's Gate 11 Candidate hours push. It moves this
  -- seal rather than the general browser-isolation RPC seal only because its
  -- name contains 'candidate', which the general verifier filters out and this
  -- one selects for. It is SECURITY DEFINER, owned by postgres, carries a fixed
  -- search_path and holds exactly one foreign grant, service_role EXECUTE; it is
  -- registered in private._weekly_source_acl_service_rpc_contract_v1().
  --
  -- THE SECURITY PROPERTY IS UNCHANGED: anon and authenticated hold EXECUTE on
  -- none of the 163, so browser_executable stays 0, and service_missing stays 9
  -- because the added routine is granted. Full record and the both-direction
  -- proof: IMPL\reports\WP-15d_REPORT.md.
  -- 24 September: the exact service-only Office provisional expense reader
  -- adds one public routine. Measured on PostgreSQL 17 after the isolated
  -- carrier closure: 164 / 9 / 0; no existing routine grants were changed.
  -- 26 September: candidate-initiated signed CHECK_ONLY hours adds exactly one
  -- service-only public SECURITY DEFINER entry; browser access remains zero.
  -- Measured on the complete PostgreSQL 17 NEW catalogue: 165 / 9 / 0.
  if v_count<>165 or v_service_missing<>9 or v_browser_executable<>0
     or v_hash<>'5fe9d64913a1eb75837ea0b98a16e752' then
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
$candidate_named_security_verification_v3$;
