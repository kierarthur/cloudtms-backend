with expected(signature)as(values ('private.weekly_source_query_require_service_v1()'),('public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)'),('public.weekly_source_message_dispatch_result_atomic_v1(jsonb)'),('private.weekly_source_protected_qualification_owner_v1()')),functions as(
 select e.signature,case when p.oid is null then null else md5(to_jsonb(p)::text)end fingerprint
 from expected e left join pg_proc p on p.oid=to_regprocedure(e.signature))
 select jsonb_build_object('database',current_database(),'postmaster_epoch',extract(epoch from pg_postmaster_start_time())::text,'server_version_num',current_setting('server_version_num'),
 'functions',(select jsonb_object_agg(signature,fingerprint)from functions),
 'schemas',(select jsonb_object_agg(nspname,md5(jsonb_build_array(nspowner,nspacl)::text))from pg_namespace where nspname in('public','private')),
 'default_acl',(select md5(coalesce(jsonb_agg(jsonb_build_array(defaclrole,defaclnamespace,defaclobjtype,defaclacl)order by defaclrole,defaclnamespace,defaclobjtype),'[]'::jsonb)::text)from pg_default_acl),
 'migration_ledger',(select jsonb_build_object('count',count(*),'md5',md5(coalesce(jsonb_agg(to_jsonb(l)order by to_jsonb(l)::text),'[]'::jsonb)::text))from private.cloudtms_migration_ledger l),
 'repeatable_ledger',(select jsonb_build_object('count',count(*),'md5',md5(coalesce(jsonb_agg(to_jsonb(l)order by to_jsonb(l)::text),'[]'::jsonb)::text))from private.cloudtms_repeatable_ledger l),
 'roles',(select jsonb_object_agg(rolname,jsonb_build_array(rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls))from pg_roles where rolname in('anon','authenticated','service_role')),
 'identity_relation_present',to_regclass('private.cloudtms_database_identity')is not null,
 'identity_test',exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST'),
 'module',(select active_owner||':'||owner_epoch::text from private.bpay_next_module_control where id=1),
 'marker_absent',to_regprocedure('private.hr_contact_closed_4bf03ef2c9057f1c3f7db169f6c529dc()')is null)as contact_component_snapshot;
