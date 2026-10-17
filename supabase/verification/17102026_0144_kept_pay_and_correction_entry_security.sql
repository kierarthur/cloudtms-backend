\set ON_ERROR_STOP on
begin;
-- Read-only catalogue assertions. This does not claim behavioural acceptance.
do $verify$
declare expected record; actual record; role_name text; fn oid; relation oid;
begin
 for expected in select * from (values
  ('private.weekly_source_identical_rekey_prior_v1(uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_kept_as_paid_read_admit_v1(uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_kept_as_paid_facts_v1(uuid,text)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_kept_as_paid_context_v1(uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_invoice_credited_targets_admit_v1(uuid,jsonb,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_keep_ack_native_guard_v1()',false,'pg_catalog,pg_temp'),
  ('private.timesheet_correction_entry_subject_v1(uuid,uuid)',false,'pg_catalog,public,private,pg_temp'),
  ('private.timesheet_correction_entry_v1(uuid,uuid)',false,'pg_catalog,public,private,pg_temp'),
  ('public.weekly_source_keep_as_paid_decide_atomic_v1(jsonb)',true,'pg_catalog,pg_temp'),
  ('public.timesheet_correction_entry_v1(jsonb)',true,'pg_catalog,public,private,pg_temp')
 ) names(signature,service_execute,search_path) loop
  fn:=pg_catalog.to_regprocedure(expected.signature);
  if fn is null then raise exception 'HEALTH_KEPT_ENTRY_ROUTINE_MISSING: %',expected.signature;end if;
  select p.* into strict actual from pg_catalog.pg_proc p where p.oid=fn;
  if not actual.prosecdef or actual.proowner<>(current_user::regrole)::oid
   or not exists(select 1 from pg_catalog.unnest(actual.proconfig) setting
    where pg_catalog.replace(setting,' ','')='search_path='||expected.search_path)
   or exists(select 1 from pg_catalog.aclexplode(coalesce(actual.proacl,pg_catalog.acldefault('f',actual.proowner))) acl
    where acl.privilege_type='EXECUTE' and acl.grantee<>actual.proowner
     and (not expected.service_execute or acl.grantee<>('service_role'::regrole)::oid)) then
   raise exception 'HEALTH_KEPT_ENTRY_SECURITY_INVALID: %',expected.signature;end if;
  foreach role_name in array array['anon','authenticated'] loop
   if pg_catalog.has_function_privilege(role_name,fn,'EXECUTE') then
    raise exception 'HEALTH_KEPT_ENTRY_BROWSER_EXECUTE: %',expected.signature;end if;
  end loop;
  if pg_catalog.has_function_privilege('service_role',fn,'EXECUTE') is distinct from expected.service_execute then
   raise exception 'HEALTH_KEPT_ENTRY_SERVICE_EXECUTE_INVALID: %',expected.signature;end if;
 end loop;
 relation:=pg_catalog.to_regclass('public.weekly_source_kept_as_paid_decisions');
 if relation is null then raise exception 'HEALTH_KEPT_DECISION_RELATION_MISSING';end if;
 select c.* into strict actual from pg_catalog.pg_class c where c.oid=relation;
 if actual.relkind<>'r' or actual.relowner<>(current_user::regrole)::oid
  or not actual.relrowsecurity or not actual.relforcerowsecurity
  or exists(select 1 from pg_catalog.aclexplode(coalesce(actual.relacl,pg_catalog.acldefault('r',actual.relowner))) acl
   where acl.grantee<>actual.relowner) then raise exception 'HEALTH_KEPT_DECISION_SECURITY_INVALID';end if;
 foreach role_name in array array['anon','authenticated','service_role'] loop
  if pg_catalog.has_table_privilege(role_name,relation,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
   or pg_catalog.has_any_column_privilege(role_name,relation,'SELECT,INSERT,UPDATE,REFERENCES') then
   raise exception 'HEALTH_KEPT_DECISION_DIRECT_ACCESS: %',role_name;end if;
 end loop;
 for expected in select * from (values
  ('public.audit_events','zzzz_weekly_source_keep_ack_native_guard',7,'private.weekly_source_keep_ack_native_guard_v1()'),
  ('public.audit_events','zzzz_weekly_source_keep_ack_update_guard',19,'private.weekly_source_keep_ack_native_guard_v1()'),
  ('public.audit_events','zzzz_weekly_source_keep_ack_delete_guard',11,'private.weekly_source_keep_ack_native_guard_v1()'),
  ('public.audit_events','zzzz_weekly_source_keep_ack_truncate_guard',34,'private.weekly_source_keep_ack_native_guard_v1()'),
  ('public.weekly_source_kept_as_paid_decisions','weekly_source_kept_as_paid_decisions_immutable',27,'private._weekly_source_immutable_record_guard_v1()'),
  ('public.weekly_source_kept_as_paid_decisions','weekly_source_kept_as_paid_decisions_truncate_immutable',34,'private._weekly_source_immutable_record_guard_v1()')
 ) triggers(relation_name,trigger_name,type_bits,function_signature) loop
  if not exists(select 1 from pg_catalog.pg_trigger t
   where t.tgrelid=pg_catalog.to_regclass(expected.relation_name) and t.tgname=expected.trigger_name
    and not t.tgisinternal and t.tgenabled='O' and t.tgtype=expected.type_bits
    and t.tgfoid=pg_catalog.to_regprocedure(expected.function_signature)) then
   raise exception 'HEALTH_KEPT_NATIVE_TRIGGER_INVALID: %',expected.trigger_name;end if;
 end loop;
 for expected in select * from (values
  ('public.weekly_source_kept_as_paid_decisions_identity','public.weekly_source_kept_as_paid_decisions',false),
  ('public.weekly_source_keep_ack_request_identity','public.audit_events',true),
  ('public.weekly_source_keep_ack_event_sequence','public.audit_events',true)
 ) indexes(index_name,relation_name,partial) loop
  if not exists(select 1 from pg_catalog.pg_index i
   where i.indexrelid=pg_catalog.to_regclass(expected.index_name)
    and i.indrelid=pg_catalog.to_regclass(expected.relation_name)
    and i.indisunique and i.indisvalid and i.indisready
    and (i.indpred is not null)=expected.partial) then
   raise exception 'HEALTH_KEPT_IDEMPOTENCY_INDEX_INVALID: %',expected.index_name;end if;
 end loop;
end;
$verify$;
rollback;
