begin;
do $managed_writer$ begin
  if not exists(select 1 from pg_catalog.pg_locks where locktype='advisory'
    and pid=pg_catalog.pg_backend_pid() and granted and mode='ExclusiveLock' and objsubid=1
    and classid=((pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)>>32)&4294967295)::oid
    and objid=(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)&4294967295)::oid)
  then raise exception 'CLOUDTMS_MANAGED_WRITER_GUARD_LOST'; end if;
end $managed_writer$;
do $health_coverage_ledger$ begin
 if not exists(select 1 from private.cloudtms_migration_ledger where path='supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql' and content_sha256='d46481d0cd53880041b126c554d34dccc39854ff3bf31ac98883f85949cc1d82') then raise exception 'HEALTH_COVERAGE_CONSTRUCTOR_LEDGER_REQUIRED' using errcode='55000';end if;
 end $health_coverage_ledger$;
lock table private.weekly_source_pay_decisions,private.weekly_source_pay_decision_requests,private.weekly_source_pay_decision_preview_assemblies,private.weekly_source_pay_decision_preview_quotes,private.weekly_source_pay_decision_reviews,private.weekly_source_pay_decision_members,private.weekly_source_pay_decision_bindings,private.weekly_source_pay_decision_unprotect_preparations,private.weekly_source_pay_decision_acceptance_applications,private.weekly_source_pay_decision_native_adoptions,private.weekly_source_pay_decision_publication_adoptions in access exclusive mode;
do $health_covered_security$
declare relation_name text; relation_oid oid; owner_oid oid:=current_user::regrole::oid;
begin
 if current_user<>session_user or pg_catalog.current_setting('row_security')<>'on' or not exists(select 1 from pg_catalog.pg_roles where oid=owner_oid and not rolsuper and not rolbypassrls) then
  raise exception 'HEALTH_COVERAGE_OWNER_REQUIRED' using errcode='42501'; end if;
 foreach relation_name in array array['weekly_source_pay_decisions','weekly_source_pay_decision_requests','weekly_source_pay_decision_preview_assemblies','weekly_source_pay_decision_preview_quotes','weekly_source_pay_decision_reviews','weekly_source_pay_decision_members','weekly_source_pay_decision_bindings','weekly_source_pay_decision_unprotect_preparations']::text[] loop
  relation_oid:=pg_catalog.to_regclass('private.'||relation_name);
  if relation_oid is null or not exists(select 1 from pg_catalog.pg_class c where c.oid=relation_oid and c.relkind='r' and c.relpersistence='p' and not c.relispartition and c.relam=(select oid from pg_catalog.pg_am where amname='heap') and c.relreplident='d' and c.relowner=owner_oid and c.relrowsecurity and c.relforcerowsecurity and c.reloptions is null)
   or exists(select 1 from pg_catalog.pg_inherits where inhrelid=relation_oid or inhparent=relation_oid)
   or exists(select 1 from pg_catalog.pg_rewrite where ev_class=relation_oid)
   or exists(select 1 from pg_catalog.pg_attribute where attrelid=relation_oid and attnum>0 and (attisdropped or attacl is not null))
   or (select coalesce(jsonb_agg(to_jsonb(a) order by a.privilege_type),'[]'::jsonb) from pg_catalog.aclexplode(coalesce((select relacl from pg_catalog.pg_class where oid=relation_oid),pg_catalog.acldefault('r',owner_oid))) a) is distinct from (select jsonb_agg(to_jsonb(a) order by a.privilege_type) from pg_catalog.aclexplode(pg_catalog.acldefault('r',owner_oid)) a)
   or (select count(*) from pg_catalog.pg_policy where polrelid=relation_oid)<>1
   or not exists(select 1 from pg_catalog.pg_policy p where p.polrelid=relation_oid and p.polname='cloudtms_source_pay_decision_owner_only' and p.polcmd='*' and p.polpermissive and p.polroles=array[owner_oid]::oid[] and pg_catalog.pg_get_expr(p.polqual,p.polrelid,false)='true' and pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid,false)='true') then
    raise exception 'HEALTH_COVERAGE_SECURITY_MISMATCH: %',relation_name using errcode='55000'; end if;
 end loop;
end $health_covered_security$;
-- One-time CloudTMS schema/data migration: nhsp_v6_source_owner_only_rls
-- Source-only Health assembly; actual cross-owner dependencies remain mandatory.





-- Exact d863577918ce7d439206296e91ad78de298e98b2:docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/one-time/05_complete_owner_only_rls.sqlinc
alter table private.weekly_source_pay_decisions owner to postgres;
alter table private.weekly_source_pay_decision_requests owner to postgres;
alter table private.weekly_source_pay_decision_preview_assemblies owner to postgres;
alter table private.weekly_source_pay_decision_preview_quotes owner to postgres;
alter table private.weekly_source_pay_decision_reviews owner to postgres;
alter table private.weekly_source_pay_decision_members owner to postgres;
alter table private.weekly_source_pay_decision_bindings owner to postgres;
alter table private.weekly_source_pay_decision_unprotect_preparations owner to postgres;
alter table private.weekly_source_pay_decision_acceptance_applications owner to postgres;
alter table private.weekly_source_pay_decision_native_adoptions owner to postgres;
alter table private.weekly_source_pay_decision_publication_adoptions owner to postgres;
do $source_pay_private_rls$
declare name text;
begin
 foreach name in array array['weekly_source_pay_decision_acceptance_applications','weekly_source_pay_decision_native_adoptions','weekly_source_pay_decision_publication_adoptions'] loop
  execute format('alter table private.%I enable row level security',name);
  execute format('alter table private.%I force row level security',name);
  execute format('revoke all on table private.%I from public,anon,authenticated,service_role',name);
  execute format('create policy cloudtms_source_pay_decision_owner_only on private.%I for all to %I using(true) with check(true)',name,current_user);
 end loop;
end;
$source_pay_private_rls$;



do $health_covered_security$
declare relation_name text; relation_oid oid; owner_oid oid:=current_user::regrole::oid;
begin
 if current_user<>session_user or pg_catalog.current_setting('row_security')<>'on' or not exists(select 1 from pg_catalog.pg_roles where oid=owner_oid and not rolsuper and not rolbypassrls) then
  raise exception 'HEALTH_COVERAGE_OWNER_REQUIRED' using errcode='42501'; end if;
 foreach relation_name in array array['weekly_source_pay_decisions','weekly_source_pay_decision_requests','weekly_source_pay_decision_preview_assemblies','weekly_source_pay_decision_preview_quotes','weekly_source_pay_decision_reviews','weekly_source_pay_decision_members','weekly_source_pay_decision_bindings','weekly_source_pay_decision_unprotect_preparations','weekly_source_pay_decision_acceptance_applications','weekly_source_pay_decision_native_adoptions','weekly_source_pay_decision_publication_adoptions']::text[] loop
  relation_oid:=pg_catalog.to_regclass('private.'||relation_name);
  if relation_oid is null or not exists(select 1 from pg_catalog.pg_class c where c.oid=relation_oid and c.relkind='r' and c.relpersistence='p' and not c.relispartition and c.relam=(select oid from pg_catalog.pg_am where amname='heap') and c.relreplident='d' and c.relowner=owner_oid and c.relrowsecurity and c.relforcerowsecurity and c.reloptions is null)
   or exists(select 1 from pg_catalog.pg_inherits where inhrelid=relation_oid or inhparent=relation_oid)
   or exists(select 1 from pg_catalog.pg_rewrite where ev_class=relation_oid)
   or exists(select 1 from pg_catalog.pg_attribute where attrelid=relation_oid and attnum>0 and (attisdropped or attacl is not null))
   or (select coalesce(jsonb_agg(to_jsonb(a) order by a.privilege_type),'[]'::jsonb) from pg_catalog.aclexplode(coalesce((select relacl from pg_catalog.pg_class where oid=relation_oid),pg_catalog.acldefault('r',owner_oid))) a) is distinct from (select jsonb_agg(to_jsonb(a) order by a.privilege_type) from pg_catalog.aclexplode(pg_catalog.acldefault('r',owner_oid)) a)
   or (select count(*) from pg_catalog.pg_policy where polrelid=relation_oid)<>1
   or not exists(select 1 from pg_catalog.pg_policy p where p.polrelid=relation_oid and p.polname='cloudtms_source_pay_decision_owner_only' and p.polcmd='*' and p.polpermissive and p.polroles=array[owner_oid]::oid[] and pg_catalog.pg_get_expr(p.polqual,p.polrelid,false)='true' and pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid,false)='true') then
    raise exception 'HEALTH_COVERAGE_SECURITY_MISMATCH: %',relation_name using errcode='55000'; end if;
 end loop;
end $health_covered_security$;
do $health_coverage_receipt$ begin update private.cloudtms_database_releases set evidence_json=jsonb_set(evidence_json,array['health_source_constructor_coverage'],coalesce(evidence_json->'health_source_constructor_coverage','{}'::jsonb)||jsonb_build_object('supabase/migrations/16102026_1104_nhsp_v6_source_owner_only_rls.sql','{"version":"HEALTH_SOURCE_EXACT_ONCE_V1","constructor":"supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql","constructorSha256":"d46481d0cd53880041b126c554d34dccc39854ff3bf31ac98883f85949cc1d82","consumer":"supabase/migrations/16102026_1104_nhsp_v6_source_owner_only_rls.sql","consumerSha256":"195b80edb9f161ca770e50a1cca146f1d931def449ba769fc8f8d4687b41c1c0","authoritySha256":"551879c37e7f7f24dd6e1f7bb3241c7fcda034ed05fea4125cf0644722749302","coverage":"EIGHT_SECURITY_VERIFIED_THREE_APPLIED","businessRowsRead":0}'::jsonb),true) where release_id='SYNTHETIC_SOURCE_ONLY_RELEASE';if not found then raise exception 'HEALTH_COVERAGE_RELEASE_REQUIRED' using errcode='55000';end if;end $health_coverage_receipt$;
do $managed_writer$ begin
  if not exists(select 1 from pg_catalog.pg_locks where locktype='advisory'
    and pid=pg_catalog.pg_backend_pid() and granted and mode='ExclusiveLock' and objsubid=1
    and classid=((pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)>>32)&4294967295)::oid
    and objid=(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)&4294967295)::oid)
  then raise exception 'CLOUDTMS_MANAGED_WRITER_GUARD_LOST'; end if;
end $managed_writer$;
insert into private.cloudtms_migration_ledger(path,content_sha256,first_release_id)
    values('supabase/migrations/16102026_1104_nhsp_v6_source_owner_only_rls.sql','195b80edb9f161ca770e50a1cca146f1d931def449ba769fc8f8d4687b41c1c0','SYNTHETIC_SOURCE_ONLY_RELEASE');
commit;