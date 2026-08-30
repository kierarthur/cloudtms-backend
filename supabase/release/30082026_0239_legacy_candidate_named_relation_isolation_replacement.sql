-- LEGACY_UPGRADE-only provider-neutral execution of the immutable, protected
-- Candidate-named relation isolation boundary. The protected migration remains
-- byte-for-byte unchanged. This replacement performs its exact table/view
-- operations after structural name-manifest checks while avoiding provider ACL
-- fingerprints. It changes no rows, policies, definitions, functions, finance,
-- Banking Pay economics, or Policy X authority.

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $legacy_candidate_named_relation_isolation$
declare
  v_count integer;
  v_hash text;
  v_service_hash_before text;
  v_service_hash_after text;
  v_target record;
begin
  with targets as (
    select
      c.oid,c.relname,
      has_table_privilege('service_role',c.oid,'SELECT') svc_select,
      has_table_privilege('service_role',c.oid,'INSERT') svc_insert,
      has_table_privilege('service_role',c.oid,'UPDATE') svc_update,
      has_table_privilege('service_role',c.oid,'DELETE') svc_delete,
      has_table_privilege('service_role',c.oid,'TRUNCATE') svc_truncate,
      has_table_privilege('service_role',c.oid,'REFERENCES') svc_references,
      has_table_privilege('service_role',c.oid,'TRIGGER') svc_trigger
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind in('r','p')
      and c.relname in (
        'banking_pay_snapshot_candidate_state',
        'banking_pay_workbench_session_candidate_state',
        'legacy_eclipse_candidate_map','pay_batch_candidates',
        'rates_candidate_overrides','candidates_tombstones'
      )
  )
  select
    count(*)::integer,
    md5(coalesce(string_agg(relname,E'\n' order by relname),'')),
    md5(coalesce(string_agg(
      relname||'|'||svc_select::text||svc_insert::text||svc_update::text||
      svc_delete::text||svc_truncate::text||svc_references::text||svc_trigger::text,
      E'\n' order by relname),''))
  into v_count,v_hash,v_service_hash_before
  from targets;

  if v_count<>6 or v_hash<>'56579812da56cf68a033dc225936531f' then
    raise exception 'LEGACY_CANDIDATE_NAMED_TABLE_MANIFEST_DRIFT:count=% hash=%',
      v_count,v_hash;
  end if;

  for v_target in
    select c.relname,
      has_table_privilege('service_role',c.oid,'SELECT') svc_select,
      has_table_privilege('service_role',c.oid,'INSERT') svc_insert,
      has_table_privilege('service_role',c.oid,'UPDATE') svc_update,
      has_table_privilege('service_role',c.oid,'DELETE') svc_delete,
      has_table_privilege('service_role',c.oid,'TRUNCATE') svc_truncate,
      has_table_privilege('service_role',c.oid,'REFERENCES') svc_references,
      has_table_privilege('service_role',c.oid,'TRIGGER') svc_trigger
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind in('r','p')
      and c.relname in (
        'banking_pay_snapshot_candidate_state',
        'banking_pay_workbench_session_candidate_state',
        'legacy_eclipse_candidate_map','pay_batch_candidates',
        'rates_candidate_overrides','candidates_tombstones'
      )
    order by c.relname
  loop
    execute format('alter table %I.%I enable row level security','public',v_target.relname);
    execute format(
      'revoke all privileges on table %I.%I from PUBLIC, anon, authenticated',
      'public',v_target.relname
    );
    if v_target.svc_select then execute format('grant select on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_insert then execute format('grant insert on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_update then execute format('grant update on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_delete then execute format('grant delete on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_truncate then execute format('grant truncate on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_references then execute format('grant references on table %I.%I to service_role','public',v_target.relname); end if;
    if v_target.svc_trigger then execute format('grant trigger on table %I.%I to service_role','public',v_target.relname); end if;
  end loop;

  with targets as (
    select c.oid,c.relname,c.relrowsecurity,
      has_table_privilege('service_role',c.oid,'SELECT') svc_select,
      has_table_privilege('service_role',c.oid,'INSERT') svc_insert,
      has_table_privilege('service_role',c.oid,'UPDATE') svc_update,
      has_table_privilege('service_role',c.oid,'DELETE') svc_delete,
      has_table_privilege('service_role',c.oid,'TRUNCATE') svc_truncate,
      has_table_privilege('service_role',c.oid,'REFERENCES') svc_references,
      has_table_privilege('service_role',c.oid,'TRIGGER') svc_trigger,
      has_table_privilege('anon',c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') anon_access,
      has_table_privilege('authenticated',c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') auth_access
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind in('r','p')
      and c.relname in (
        'banking_pay_snapshot_candidate_state',
        'banking_pay_workbench_session_candidate_state',
        'legacy_eclipse_candidate_map','pay_batch_candidates',
        'rates_candidate_overrides','candidates_tombstones'
      )
  )
  select
    md5(coalesce(string_agg(
      relname||'|'||svc_select::text||svc_insert::text||svc_update::text||
      svc_delete::text||svc_truncate::text||svc_references::text||svc_trigger::text,
      E'\n' order by relname),'')),
    count(*) filter (where not relrowsecurity or anon_access or auth_access)::integer
  into v_service_hash_after,v_count
  from targets;

  if v_service_hash_after<>v_service_hash_before then
    raise exception 'LEGACY_CANDIDATE_NAMED_TABLE_SERVICE_ACL_CHANGED';
  end if;
  if v_count<>0 then
    raise exception 'LEGACY_CANDIDATE_NAMED_TABLE_ISOLATION_FAILED:count=%',v_count;
  end if;

  select count(*)::integer,md5(coalesce(string_agg(c.relname,E'\n' order by c.relname),''))
  into v_count,v_hash
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='v'
    and c.relname in (
      'v_legacy_candidate_contract_summary','v_legacy_client_candidates',
      'v_legacy_contracts_by_candidate','candidates_summary',
      'candidates_summary_activity','v_mailshot_src_candidate','vw_picker_candidates'
    );
  if v_count<>7 or v_hash<>'e3517fd0dc706d587a2c7275f370bca5' then
    raise exception 'LEGACY_CANDIDATE_NAMED_VIEW_MANIFEST_DRIFT:count=% hash=%',v_count,v_hash;
  end if;

  for v_target in
    select c.relname
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='v'
      and c.relname in (
        'v_legacy_candidate_contract_summary','v_legacy_client_candidates',
        'v_legacy_contracts_by_candidate','candidates_summary',
        'candidates_summary_activity','v_mailshot_src_candidate','vw_picker_candidates'
      )
    order by c.relname
  loop
    execute format('alter view %I.%I set (security_invoker=true)','public',v_target.relname);
    execute format(
      'revoke all privileges on table %I.%I from PUBLIC, anon, authenticated',
      'public',v_target.relname
    );
    execute format('grant select on table %I.%I to service_role','public',v_target.relname);
  end loop;

  select count(*) filter (
    where not ('security_invoker=true'=any(coalesce(c.reloptions,array[]::text[])))
      or not has_table_privilege('service_role',c.oid,'SELECT')
      or has_table_privilege('anon',c.oid,'SELECT')
      or has_table_privilege('authenticated',c.oid,'SELECT')
  )::integer
  into v_count
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='v'
    and c.relname in (
      'v_legacy_candidate_contract_summary','v_legacy_client_candidates',
      'v_legacy_contracts_by_candidate','candidates_summary',
      'candidates_summary_activity','v_mailshot_src_candidate','vw_picker_candidates'
    );
  if v_count<>0 then
    raise exception 'LEGACY_CANDIDATE_NAMED_VIEW_ISOLATION_FAILED:count=%',v_count;
  end if;
end
$legacy_candidate_named_relation_isolation$;
