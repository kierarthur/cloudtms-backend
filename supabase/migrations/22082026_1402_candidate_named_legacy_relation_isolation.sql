-- TEST-first closure for the thirteen Candidate-named legacy relations left
-- outside the protected Candidate/MyTMS migration and the non-Candidate general
-- isolation phase.
--
-- This migration changes no rows, policies, view definitions, function bodies,
-- financial calculation, Banking Pay economic rule, or Policy X authority. It
-- enables RLS on six exact tables, changes seven exact views to invoker rights,
-- and removes direct browser-role relation authority while preserving the
-- existing service_role privileges.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $candidate_named_relation_isolation$
declare
  v_count integer;
  v_hash text;
  v_apply_tables boolean := false;
  v_apply_views boolean := false;
  v_target record;
begin
  with targets as (
    select
      c.relname,
      c.relrowsecurity,
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
  into v_count,v_hash
  from targets;

  if v_count=6 and v_hash='f6ac2c10d6ba7f4ceb84a34846184e0c' then
    v_apply_tables := true;
  elsif v_count=6 and v_hash='bc44e32bc6dc29e429d3555177cba049' then
    v_apply_tables := false;
  else
    raise exception 'CANDIDATE_NAMED_TABLE_MANIFEST_DRIFT:count=% hash=%',v_count,v_hash;
  end if;

  if v_apply_tables then
    for v_target in
      select c.relname
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
      order by c.relname
    loop
      execute pg_catalog.format('alter table %I.%I enable row level security','public',v_target.relname);
      execute pg_catalog.format(
        'revoke all privileges on table %I.%I from PUBLIC, anon, authenticated',
        'public',v_target.relname
      );
    end loop;
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
  into v_count,v_hash
  from targets;

  if v_count=7 and v_hash='a0f86bbd602a7845d5f61b02a0d006c2' then
    v_apply_views := true;
  elsif v_count=7 and v_hash='e91248c342b81c4220fe257b2d76941a' then
    v_apply_views := false;
  else
    raise exception 'CANDIDATE_NAMED_VIEW_MANIFEST_DRIFT:count=% hash=%',v_count,v_hash;
  end if;

  if v_apply_views then
    for v_target in
      select c.relname
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
      order by c.relname
    loop
      execute pg_catalog.format('alter view %I.%I set (security_invoker=true)','public',v_target.relname);
      execute pg_catalog.format(
        'revoke all privileges on table %I.%I from PUBLIC, anon, authenticated',
        'public',v_target.relname
      );
      execute pg_catalog.format('grant select on table %I.%I to service_role','public',v_target.relname);
    end loop;
  end if;
end
$candidate_named_relation_isolation$;
