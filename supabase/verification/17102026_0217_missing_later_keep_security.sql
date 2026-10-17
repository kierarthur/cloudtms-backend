\set ON_ERROR_STOP on
begin;
-- Read-only catalogue assertions. This does not claim behavioural acceptance.
do $verify$
declare expected record; actual record; role_name text; fn oid; relation oid;
begin
 for expected in select * from (values
  ('private.weekly_source_kept_as_paid_facts_for_publication_v1(uuid,text,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_kept_as_paid_context_for_publication_v1(uuid,uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_snapshot_movement_v1(uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_cancel_basis_v1(uuid,uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_finalisation_roots_v1(uuid,integer,uuid,uuid,uuid,date,date)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_cancel_transition_assert_v1(uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_cancel_native_insert_guard_v1()',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_current_selection_v1(uuid,uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_prepared_census_recheck_v1(uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_kept_as_paid_read_for_publication_v1(uuid,uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_accept_admission_v1(uuid,uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_accept_head_chain_v1(uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_accept_current_removal_v1(uuid,uuid,uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_accept_inventory_inputs_v1(uuid,uuid,uuid,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_accept_proposal_v1(uuid,bigint,uuid)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_accept_prepare_v1(jsonb)',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_missing_accept_proof_guard_v1()',false,'pg_catalog,pg_temp'),
  ('private.weekly_source_keep_as_paid_scope_rows_v1(uuid,uuid,uuid,uuid,uuid,date)',false,'pg_catalog,pg_temp'),
  ('public.weekly_source_missing_accept_prepare_atomic_v1(jsonb)',true,'pg_catalog,pg_temp'),
  ('public.weekly_source_missing_accept_context_v1(jsonb)',true,'pg_catalog,pg_temp'),
  ('public.weekly_source_keep_as_paid_context_v1(jsonb)',true,'pg_catalog,pg_temp')
 ) names(signature,service_execute,search_path) loop
  fn:=pg_catalog.to_regprocedure(expected.signature);
  if fn is null then raise exception 'HEALTH_MISSING_LATER_KEEP_ROUTINE_MISSING: %',expected.signature;end if;
  select p.* into strict actual from pg_catalog.pg_proc p where p.oid=fn;
  if not actual.prosecdef or actual.proowner<>(current_user::regrole)::oid
   or not exists(select 1 from pg_catalog.unnest(actual.proconfig) setting
    where pg_catalog.replace(setting,' ','')='search_path='||expected.search_path)
   or exists(select 1 from pg_catalog.aclexplode(coalesce(actual.proacl,pg_catalog.acldefault('f',actual.proowner))) acl
    where acl.privilege_type='EXECUTE' and acl.grantee<>actual.proowner
     and (not expected.service_execute or acl.grantee<>('service_role'::regrole)::oid)) then
   raise exception 'HEALTH_MISSING_LATER_KEEP_SECURITY_INVALID: %',expected.signature;end if;
  foreach role_name in array array['anon','authenticated'] loop
   if pg_catalog.has_function_privilege(role_name,fn,'EXECUTE') then
    raise exception 'HEALTH_MISSING_LATER_KEEP_BROWSER_EXECUTE: %',expected.signature;end if;
  end loop;
  if pg_catalog.has_function_privilege('service_role',fn,'EXECUTE') is distinct from expected.service_execute then
   raise exception 'HEALTH_MISSING_LATER_KEEP_SERVICE_EXECUTE_INVALID: %',expected.signature;end if;
 end loop;
 for expected in select * from (values
  ('public.weekly_source_state_transitions','exclude_cancel','boolean',true),
  ('public.weekly_source_state_transitions','cancel_exclusion_basis','jsonb',false),
  ('public.weekly_source_final_revisions','missing_cancel_basis_census','jsonb',true)
 ) columns(relation_name,column_name,column_type,not_null) loop
  if not exists(select 1 from pg_catalog.pg_attribute a where a.attrelid=pg_catalog.to_regclass(expected.relation_name)
   and a.attname=expected.column_name and a.attnum>0 and not a.attisdropped
   and a.atttypid=pg_catalog.to_regtype(expected.column_type) and a.attnotnull=expected.not_null) then
   raise exception 'HEALTH_MISSING_COLUMN_INVALID: %',expected.column_name;end if;
 end loop;
 for expected in select * from (values
  ('public.weekly_source_state_transitions','weekly_source_cancel_exclusion_shape'),
  ('public.weekly_source_final_revisions','weekly_source_missing_cancel_census_shape')
 ) constraints(relation_name,constraint_name) loop
  if not exists(select 1 from pg_catalog.pg_constraint c where c.conrelid=pg_catalog.to_regclass(expected.relation_name)
   and c.conname=expected.constraint_name and c.contype='c' and c.convalidated) then
   raise exception 'HEALTH_MISSING_CONSTRAINT_INVALID: %',expected.constraint_name;end if;
 end loop;
 for expected in select * from (values
  ('public.weekly_source_state_transitions','zzzz_weekly_source_missing_cancel_native_insert_guard',5,'private.weekly_source_missing_cancel_native_insert_guard_v1()'),
  ('public.audit_events','weekly_source_missing_accept_proof_insert_guard',7,'private.weekly_source_missing_accept_proof_guard_v1()'),
  ('public.audit_events','weekly_source_missing_accept_proof_update_guard',19,'private.weekly_source_missing_accept_proof_guard_v1()'),
  ('public.audit_events','weekly_source_missing_accept_proof_delete_guard',11,'private.weekly_source_missing_accept_proof_guard_v1()'),
  ('public.audit_events','weekly_source_missing_accept_proof_truncate_guard',34,'private.weekly_source_missing_accept_proof_guard_v1()')
 ) triggers(relation_name,trigger_name,type_bits,function_signature) loop
  if not exists(select 1 from pg_catalog.pg_trigger t where t.tgrelid=pg_catalog.to_regclass(expected.relation_name)
   and t.tgname=expected.trigger_name and not t.tgisinternal and t.tgenabled='O' and t.tgtype=expected.type_bits
   and t.tgfoid=pg_catalog.to_regprocedure(expected.function_signature)) then
   raise exception 'HEALTH_MISSING_NATIVE_TRIGGER_INVALID: %',expected.trigger_name;end if;
 end loop;
 if not exists(select 1 from pg_catalog.pg_index i where i.indexrelid=pg_catalog.to_regclass('public.weekly_source_missing_accept_prepare_key_uq')
  and i.indrelid=pg_catalog.to_regclass('public.audit_events') and i.indisunique and i.indisvalid and i.indisready and i.indpred is not null) then
  raise exception 'HEALTH_MISSING_NATIVE_REQUEST_INDEX_INVALID';end if;
end;
$verify$;
rollback;
