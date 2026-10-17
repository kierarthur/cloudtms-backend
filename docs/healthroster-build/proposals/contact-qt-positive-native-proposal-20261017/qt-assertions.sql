-- Root-only server-owned rollback B. These are native assertions, not a result fixture.
create temporary table hr_contact_qt_result(value jsonb not null) on commit drop;
do $qt_audit_admission$ begin
 if (
with expected_columns(ord,name,type_oid,identity) as(values (1,'id',2950,''),(2,'ts_utc',1184,''),(3,'actor_user_id',2950,''),(4,'actor_display',25,''),(5,'actor_role_at_time',25,''),(6,'object_type',25,''),(7,'object_id_text',25,''),(8,'action',25,''),(9,'before_json',3802,''),(10,'after_json',3802,''),(11,'reason',25,''),(12,'ip',25,''),(13,'user_agent',25,''),(14,'correlation_id',25,''),(15,'event_sequence_is_authoritative',16,''),(16,'event_sequence',20,'a')),
expected_types(oid,send_symbol,out_symbol,send_vol,out_vol) as(values (2950,'uuid_send','uuid_out','i','i'),(1184,'timestamptz_send','timestamptz_out','i','s'),(25,'textsend','textout','s','i'),(3802,'jsonb_send','jsonb_out','i','i'),(16,'boolsend','boolout','i','i'),(20,'int8send','int8out','i','i')),
expected_functions(identity,source_symbol,arg_oid,return_oid,vol) as(values ('pg_catalog.record_send(record)','record_send',2249,17,'s'),('pg_catalog.sha256(bytea)','sha256_bytea',17,17,'i'),('pg_catalog.current_setting(text)','show_config_by_name',25,25,'s')),
r as(select c.* from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='audit_events')
select
 (select count(*)=1 and coalesce(bool_and(r.relkind='r' and r.relpersistence='p' and r.relrowsecurity and not r.relforcerowsecurity and r.relowner=(select oid from pg_catalog.pg_roles where rolname='dee50tht') and r.relam=(select oid from pg_catalog.pg_am where amname='heap' and amtype='t')),false) from r)
 and (select count(*)=16 from pg_catalog.pg_attribute a join r on r.oid=a.attrelid where a.attnum>0 and not a.attisdropped)
 and not exists(select 1 from expected_columns e where not exists(select 1 from pg_catalog.pg_attribute a join r on r.oid=a.attrelid where a.attnum=e.ord and a.attname=e.name and a.atttypid=e.type_oid and not a.attisdropped and a.attidentity::text=e.identity and a.attgenerated=''))
 and not exists(select 1 from pg_catalog.pg_inherits i join r on r.oid in(i.inhrelid,i.inhparent))
 and not exists(select 1 from pg_catalog.pg_rewrite w join r on r.oid=w.ev_class)
 and (select count(*)=1 from pg_catalog.pg_policy p join r on r.oid=p.polrelid)
 and (select count(*)=2 from pg_catalog.pg_roles where rolname in('dee50tht','service_role'))
 and exists(select 1 from pg_catalog.pg_policy p join r on r.oid=p.polrelid where p.polname='cloudtms_miget_service_owner_all' and p.polcmd='*' and p.polpermissive and pg_catalog.pg_get_expr(p.polqual,p.polrelid)='true' and pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid)='true'
 and array(select unnest(p.polroles) order by 1)=(select array_agg(oid order by oid) from pg_catalog.pg_roles where rolname in('dee50tht','service_role')))
 and not exists(select 1 from expected_types e where not exists(select 1 from pg_catalog.pg_type t join pg_catalog.pg_namespace n on n.oid=t.typnamespace join pg_catalog.pg_proc snd on snd.oid=t.typsend join pg_catalog.pg_language sl on sl.oid=snd.prolang join pg_catalog.pg_proc outp on outp.oid=t.typoutput join pg_catalog.pg_language ol on ol.oid=outp.prolang
 where t.oid=e.oid and n.nspname='pg_catalog' and t.typtype='b' and t.typbasetype=0 and t.typisdefined
 and snd.pronargs=1 and snd.proargtypes[0]=t.oid and snd.prorettype=17 and not snd.proretset and snd.provariadic=0 and outp.pronargs=1 and outp.proargtypes[0]=t.oid and outp.prorettype=2275 and not outp.proretset and outp.provariadic=0
 and snd.pronamespace=n.oid and snd.proowner=10 and snd.prokind='f' and sl.lanname='internal' and snd.prosrc=e.send_symbol and snd.probin is null and snd.proconfig is null and not snd.prosecdef and snd.provolatile=e.send_vol and snd.proisstrict and snd.proparallel='s' and pg_catalog.has_function_privilege(current_user,snd.oid,'EXECUTE')
 and outp.pronamespace=n.oid and outp.proowner=10 and outp.prokind='f' and ol.lanname='internal' and outp.prosrc=e.out_symbol and outp.probin is null and outp.proconfig is null and not outp.prosecdef and outp.provolatile=e.out_vol and outp.proisstrict and outp.proparallel='s' and pg_catalog.has_function_privilege(current_user,outp.oid,'EXECUTE')))
 and not exists(select 1 from expected_functions e where not exists(select 1 from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace join pg_catalog.pg_language l on l.oid=p.prolang
 where p.oid=pg_catalog.to_regprocedure(e.identity) and p.pronargs=1 and p.proargtypes[0]=e.arg_oid and p.prorettype=e.return_oid and not p.proretset and p.provariadic=0 and n.nspname='pg_catalog' and p.proowner=10 and p.prokind='f' and l.lanname='internal' and p.prosrc=e.source_symbol and p.probin is null and p.proconfig is null and not p.prosecdef and p.provolatile=e.vol and p.proisstrict and p.proparallel='s' and pg_catalog.has_function_privilege(current_user,p.oid,'EXECUTE')))
 and pg_catalog.current_setting('server_version_num')='170011' and pg_catalog.current_setting('server_encoding')='UTF8' and pg_catalog.current_setting('client_encoding')='UTF8'
 and exists(select 1 from r join pg_catalog.pg_type audit_row_type on audit_row_type.oid=r.reltype join pg_catalog.pg_namespace audit_row_namespace on audit_row_namespace.oid=audit_row_type.typnamespace where audit_row_type.typtype='c' and audit_row_type.typisdefined and audit_row_type.typbasetype=0 and audit_row_type.typrelid=r.oid and audit_row_type.typowner=r.relowner and audit_row_namespace.nspname='public' and audit_row_type.typname='audit_events' and audit_row_type.typsend=pg_catalog.to_regprocedure('pg_catalog.record_send(record)')) as allowed
 ) is not true then raise exception 'QT_AUDIT_RECORD_ENCODER_CATALOGUE_UNAVAILABLE';end if;
 -- Canonical local digest order requires the exact existing unique native sequence index.
 -- This is metadata admission, not an assumption that GENERATED ALWAYS implies uniqueness.
 if not exists(
  select 1 from pg_catalog.pg_index audit_order_index
  join pg_catalog.pg_class audit_order_relation on audit_order_relation.oid=audit_order_index.indexrelid
  join pg_catalog.pg_namespace audit_order_namespace on audit_order_namespace.oid=audit_order_relation.relnamespace
  join pg_catalog.pg_am audit_order_method on audit_order_method.oid=audit_order_relation.relam
  join pg_catalog.pg_attribute audit_order_column on audit_order_column.attrelid=audit_order_index.indrelid
   and audit_order_column.attname='event_sequence' and audit_order_column.attnum>0 and not audit_order_column.attisdropped
  where audit_order_namespace.nspname='public' and audit_order_relation.relname='audit_events_event_sequence_uq'
   and audit_order_relation.relkind='i' and audit_order_method.amname='btree'
   and audit_order_index.indrelid='public.audit_events'::regclass
   and audit_order_index.indisunique and audit_order_index.indimmediate and audit_order_index.indisvalid and audit_order_index.indisready and audit_order_index.indislive
   and audit_order_index.indnkeyatts=1 and audit_order_index.indnatts=1
   and audit_order_index.indexprs is null and audit_order_index.indpred is null
   and audit_order_index.indkey[0]=audit_order_column.attnum and audit_order_index.indoption[0]=0
   and audit_order_column.attnotnull and audit_order_column.atttypid='pg_catalog.int8'::regtype and audit_order_column.attidentity='a'
   and pg_catalog.pg_get_indexdef(audit_order_relation.oid)='CREATE UNIQUE INDEX audit_events_event_sequence_uq ON public.audit_events USING btree (event_sequence)'
 ) then raise exception 'QT_AUDIT_EVENT_SEQUENCE_INDEX_UNAVAILABLE';end if;
 if to_regclass('pg_temp.hr_contact_qt_audit_before') is not null or to_regclass('pg_temp.hr_contact_qt_audit_after') is not null then
  raise exception 'QT_AUDIT_DIGEST_TEMP_NAMESPACE_UNAVAILABLE';end if;
end;$qt_audit_admission$;
create temporary table hr_contact_qt_audit_before on commit drop as select a.event_sequence,pg_catalog.sha256(pg_catalog.record_send(ROW(a,pg_catalog.current_setting('TimeZone')))) as row_digest from public.audit_events a limit 1500001;
do $qt_audit_before_bound$ begin
 if (select count(*) from pg_temp.hr_contact_qt_audit_before)>1500000 then
  raise exception 'QT_AUDIT_WHOLE_TABLE_ROW_BOUND_EXCEEDED';end if;
 if exists(select 1 from pg_temp.hr_contact_qt_audit_before where event_sequence is null or row_digest is null or pg_catalog.octet_length(row_digest)<>32) then
  raise exception 'QT_AUDIT_DIGEST_NATIVE_FRAME_INVALID';end if;
end;$qt_audit_before_bound$;
do $qt_controls$
declare
 ids uuid[]:=array['f717d041-0000-4000-8000-000000000001','f717d041-0000-4000-8000-000000000002',
 'f717d041-0000-4000-8000-000000000003','f717d041-0000-4000-8000-000000000004',
 'f717d041-0000-4000-8000-000000000005','f717d041-0000-4000-8000-000000000006']::uuid[];
 agency uuid:='f717c051-0000-4000-8000-000000000019';
 request1 uuid:='f717c051-0000-4000-8000-000000000002';
 request2 uuid:='f717c051-0000-4000-8000-000000000003';
 request3 uuid:='f717c051-0000-4000-8000-000000000004';
 policy1 jsonb:=jsonb_build_object('version','PUSH_TIMING_POLICY_V1','enabled',true,
 'timezone','Europe/London','normal_quiet_start','00:00','normal_quiet_end','07:00',
 'day_rules',jsonb_build_object('monday',jsonb_build_object('mode','INHERIT'),
 'tuesday',jsonb_build_object('mode','INHERIT'),'wednesday',jsonb_build_object('mode','INHERIT'),
 'thursday',jsonb_build_object('mode','INHERIT'),'friday',jsonb_build_object('mode','INHERIT'),
 'saturday',jsonb_build_object('mode','INHERIT'),'sunday',jsonb_build_object('mode','INHERIT')),
 'bank_holiday_rule',jsonb_build_object('mode','INHERIT'));
 policy2 jsonb; get_request jsonb; save1 jsonb;save2 jsonb;first_receipt jsonb;second_receipt jsonb;
 result jsonb;before_journal jsonb;actor uuid;api text;role_name text;denials integer:=0;rejected boolean;
 global_before jsonb;defaults_before jsonb;audits_before text;constraint_name text;acl_checks integer:=0;
begin
 global_before:=(select to_jsonb(s) from public.weekly_source_global_settings s where singleton);
 defaults_before:=(select jsonb_agg(to_jsonb(s) order by s.id) from public.settings_defaults s);
 insert into public.tms_users(id,email,role,is_active,password_hash,payment_authoriser,payment_golden_key) values
 (ids[1],'daily-health-actor-admin-20261017@example.invalid','admin',true,'!DAILY_HEALTH_ACTOR_ROLLBACK_ONLY!',false,false),
 (ids[2],'daily-health-actor-authoriser-20261017@example.invalid','user',true,'!DAILY_HEALTH_ACTOR_ROLLBACK_ONLY!',true,false),
 (ids[3],'daily-health-actor-golden-20261017@example.invalid','user',true,'!DAILY_HEALTH_ACTOR_ROLLBACK_ONLY!',true,true),
 (ids[4],'daily-health-actor-ordinary-20261017@example.invalid','user',true,'!DAILY_HEALTH_ACTOR_ROLLBACK_ONLY!',false,false),
 (ids[5],'daily-health-actor-inactive-admin-20261017@example.invalid','admin',false,'!DAILY_HEALTH_ACTOR_ROLLBACK_ONLY!',false,false),
 (ids[6],'daily-health-actor-inactive-authoriser-20261017@example.invalid','user',false,'!DAILY_HEALTH_ACTOR_ROLLBACK_ONLY!',true,false);
 get_request:=jsonb_build_object('version','PUSH_TIMING_GET_V1','actor_user_id',ids[1],'agency_id',agency,'environment','TEST');
 rejected:=false;begin perform public.candidate_push_timing_get_v1(get_request);
 exception when sqlstate 'P0001' then if sqlerrm<>'PUSH_TIMING_DEPLOYMENT_UNAVAILABLE' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'QT_MISSING_LOCAL_POLICY_NOT_REFUSED';end if;
 insert into public.candidate_push_timing_policy(environment,agency_id,revision,policy_json,policy_sha256,updated_at_utc)
 values('TEST',agency,1,policy1,private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_POLICY_V1',policy1),clock_timestamp());
 result:=public.candidate_push_timing_get_v1(get_request);
 if result->>'version'<>'PUSH_TIMING_STATE_V1' or result->>'revision'<>'1'
  or result->'policy' is distinct from policy1 or result->>'can_edit'<>'true'
  or result->>'policy_sha256'<>encode(private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_POLICY_V1',policy1),'hex') then
  raise exception 'QT_GENUINE_FIRST_GET_MISMATCH';end if;
 save1:=(get_request-'version')||jsonb_build_object('version','PUSH_TIMING_SAVE_V1','request_id',request1,'expected_revision','1','policy',policy1);
 first_receipt:=public.candidate_push_timing_save_v1(save1);
 if first_receipt->>'revision'<>'1' or public.candidate_push_timing_save_v1(save1) is distinct from first_receipt then
  raise exception 'QT_UNCHANGED_SAVE_OR_REPLAY_MISMATCH';end if;
 rejected:=false;begin perform public.candidate_push_timing_save_v1(jsonb_set(save1,'{policy,normal_quiet_end}','"08:00"'));
 exception when sqlstate 'P0001' then if sqlerrm<>'PUSH_TIMING_IDEMPOTENCY_CONFLICT' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'QT_CHANGED_REPLAY_NOT_REFUSED';end if;
 policy2:=jsonb_set(policy1,'{normal_quiet_end}','"08:00"');
 save2:=jsonb_set(jsonb_set(save1,'{request_id}',to_jsonb(request2)),'{policy}',policy2);
 second_receipt:=public.candidate_push_timing_save_v1(save2);
 if second_receipt->>'revision'<>'2' or public.candidate_push_timing_save_v1(save2) is distinct from second_receipt
  or public.candidate_push_timing_save_v1(save1) is distinct from first_receipt then raise exception 'QT_NEW_SAVE_OR_HISTORICAL_REPLAY_MISMATCH';end if;
 rejected:=false;begin perform public.candidate_push_timing_save_v1(jsonb_set(save2,'{request_id}',to_jsonb(request3)));
 exception when sqlstate 'P0001' then if sqlerrm<>'PUSH_TIMING_REVISION_CONFLICT' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'QT_STALE_NEW_CAS_NOT_REFUSED';end if;
 before_journal:=(select jsonb_agg(to_jsonb(c) order by c.request_id) from public.candidate_push_timing_commands c);
 foreach actor in array ids[2:6] loop
  foreach api in array array['GET','SAVE'] loop
   rejected:=false;begin
    if api='GET' then perform public.candidate_push_timing_get_v1(jsonb_set(get_request,'{actor_user_id}',to_jsonb(actor)));
    else perform public.candidate_push_timing_save_v1(jsonb_set(save1,'{actor_user_id}',to_jsonb(actor)));end if;
   exception when insufficient_privilege then if sqlerrm<>'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED' then raise;end if;rejected:=true;end;
   if not rejected then raise exception 'QT_ORDINARY_PAY_ONLY_OR_INACTIVE_ADMITTED';end if;denials:=denials+1;
  end loop;
 end loop;
 update public.tms_users set is_active=false where id=ids[1];
 rejected:=false;begin perform public.candidate_push_timing_save_v1(save1);
 exception when insufficient_privilege then if sqlerrm<>'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'QT_REVOKED_ADMIN_REPLAY_DISCLOSED';end if;
 update public.tms_users set is_active=true,role='user',payment_authoriser=true where id=ids[1];
 rejected:=false;begin perform public.candidate_push_timing_save_v1(save1);
 exception when insufficient_privilege then if sqlerrm<>'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'QT_PAY_ONLY_TRANSITION_REPLAY_DISCLOSED';end if;
 update public.tms_users set role='admin',payment_authoriser=false where id=ids[1];
 if public.candidate_push_timing_save_v1(save1) is distinct from first_receipt then raise exception 'QT_RESTORED_ADMIN_REPLAY_CHANGED';end if;
 if (select jsonb_agg(to_jsonb(c) order by c.request_id) from public.candidate_push_timing_commands c) is distinct from before_journal then
  raise exception 'QT_REFUSAL_OR_REPLAY_CHANGED_JOURNAL';end if;
 if (select count(*) from public.candidate_push_timing_commands)<>2 or not exists(
  select 1 from public.candidate_push_timing_commands c where c.request_id=request1 and c.agency_id=agency and c.environment='TEST'
   and c.actor_user_id=ids[1] and c.expected_revision=1 and c.accepted_revision=1 and c.response_json=first_receipt
   and c.request_digest=private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_SAVE_REQUEST_V1',save1)) or not exists(
  select 1 from public.candidate_push_timing_commands c where c.request_id=request2 and c.agency_id=agency and c.environment='TEST'
   and c.actor_user_id=ids[1] and c.expected_revision=1 and c.accepted_revision=2 and c.response_json=second_receipt
   and c.request_digest=private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_SAVE_REQUEST_V1',save2)) then
  raise exception 'QT_EXACT_TWO_COMMAND_AUDIT_MISMATCH';end if;
 if (select count(*) from public.candidate_push_timing_policy)<>1 or not exists(select 1 from public.candidate_push_timing_policy p
  where p.environment='TEST' and p.agency_id=agency and p.revision=2 and p.policy_json=policy2 and p.updated_by_user_id=ids[1]
   and p.policy_sha256=private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_POLICY_V1',policy2)) then raise exception 'QT_EXACT_FINAL_POLICY_MISMATCH';end if;
 rejected:=false;begin update public.candidate_push_timing_commands set response_json='{}' where request_id=request1;
 exception when check_violation then if sqlerrm<>'PUSH_TIMING_IMMUTABLE_AUTHORITY' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'QT_COMMAND_AUDIT_MUTABLE';end if;
 rejected:=false;begin update public.tms_users set payment_authoriser=false where id=ids[3];
 exception when check_violation then get stacked diagnostics constraint_name=CONSTRAINT_NAME;
  if constraint_name<>'tms_users_payment_golden_implies_authoriser_chk' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'QT_GOLDEN_NATIVE_INVARIANT_NOT_PRESERVED';end if;
 if (select to_jsonb(s) from public.weekly_source_global_settings s where singleton) is distinct from global_before
  or (select jsonb_agg(to_jsonb(s) order by s.id) from public.settings_defaults s) is distinct from defaults_before then
  raise exception 'QT_UNRELATED_SETTINGS_OR_GLOBAL_AUDIT_CHANGED';end if;
 foreach role_name in array array['anon','authenticated','service_role'] loop
  if has_table_privilege(role_name,'public.candidate_push_timing_policy','SELECT,INSERT,UPDATE,DELETE')
   or has_table_privilege(role_name,'public.candidate_push_timing_commands','SELECT,INSERT,UPDATE,DELETE') then raise exception 'QT_DIRECT_TABLE_EXPOSED';end if;
  if has_function_privilege(role_name,'public.candidate_push_timing_get_v1(jsonb)','EXECUTE') is distinct from (role_name='service_role')
   or has_function_privilege(role_name,'public.candidate_push_timing_save_v1(jsonb)','EXECUTE') is distinct from (role_name='service_role') then
   raise exception 'QT_EFFECTIVE_RPC_ACL_MISMATCH';end if;acl_checks:=acl_checks+1;
 end loop;
 if current_user<>session_user then raise exception 'QT_ROLE_SWITCH_NOT_ALLOWED';end if;
 if (select count(*) from public.tms_users where id=any(ids))<>6
  or exists(select 1 from public.tms_users u where u.id=any(ids) and (
   u.role is distinct from case when u.id in(ids[1],ids[5]) then 'admin' else 'user' end
   or u.is_active is distinct from (u.id=any(ids[1:4]))
   or u.payment_authoriser is distinct from (u.id in(ids[2],ids[3],ids[6]))
   or u.payment_golden_key is distinct from (u.id=ids[3])
   or u.password_hash is distinct from '!DAILY_HEALTH_ACTOR_ROLLBACK_ONLY!'
   or u.created_at is distinct from transaction_timestamp() or u.updated_at is distinct from transaction_timestamp()
   or u.display_name is not null or u.email_signature_html is not null or u.last_login_at_utc is not null
   or u.email_settings is distinct from '{}'::jsonb or u.grid_prefs_json is distinct from '{}'::jsonb or u.session_version is distinct from 1)) then
  raise exception 'QT_EXACT_SIX_USER_AFTER_STATE_MISMATCH';end if;
 insert into hr_contact_qt_result values(jsonb_build_object('programme','QT_ADMIN_CONTROLS_V1','fixtureUsers',6,
 'policyRows',1,'commandAuditRows',2,'ordinaryPayInactiveDenials',denials,'sameTransactionRevocationRefusals',2,
 'changedBodyRefusals',1,'staleCasRefusals',1,'effectiveRoleAclChecks',acl_checks,'directRoleApi','NOT_RUN',
 'crossAgencyProof','NOT_RUN','postWaitRace','NOT_RUN','outerIsolation',current_setting('transaction_isolation')));
end;$qt_controls$;
create temporary table hr_contact_qt_audit_after on commit drop as select a.event_sequence,pg_catalog.sha256(pg_catalog.record_send(ROW(a,pg_catalog.current_setting('TimeZone')))) as row_digest from public.audit_events a limit 1500001;
do $qt_audit_equality$
declare audit_before_count bigint;audit_after_count bigint;audit_original_hashagg text;
begin
 if (
with expected_columns(ord,name,type_oid,identity) as(values (1,'id',2950,''),(2,'ts_utc',1184,''),(3,'actor_user_id',2950,''),(4,'actor_display',25,''),(5,'actor_role_at_time',25,''),(6,'object_type',25,''),(7,'object_id_text',25,''),(8,'action',25,''),(9,'before_json',3802,''),(10,'after_json',3802,''),(11,'reason',25,''),(12,'ip',25,''),(13,'user_agent',25,''),(14,'correlation_id',25,''),(15,'event_sequence_is_authoritative',16,''),(16,'event_sequence',20,'a')),
expected_types(oid,send_symbol,out_symbol,send_vol,out_vol) as(values (2950,'uuid_send','uuid_out','i','i'),(1184,'timestamptz_send','timestamptz_out','i','s'),(25,'textsend','textout','s','i'),(3802,'jsonb_send','jsonb_out','i','i'),(16,'boolsend','boolout','i','i'),(20,'int8send','int8out','i','i')),
expected_functions(identity,source_symbol,arg_oid,return_oid,vol) as(values ('pg_catalog.record_send(record)','record_send',2249,17,'s'),('pg_catalog.sha256(bytea)','sha256_bytea',17,17,'i'),('pg_catalog.current_setting(text)','show_config_by_name',25,25,'s')),
r as(select c.* from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='audit_events')
select
 (select count(*)=1 and coalesce(bool_and(r.relkind='r' and r.relpersistence='p' and r.relrowsecurity and not r.relforcerowsecurity and r.relowner=(select oid from pg_catalog.pg_roles where rolname='dee50tht') and r.relam=(select oid from pg_catalog.pg_am where amname='heap' and amtype='t')),false) from r)
 and (select count(*)=16 from pg_catalog.pg_attribute a join r on r.oid=a.attrelid where a.attnum>0 and not a.attisdropped)
 and not exists(select 1 from expected_columns e where not exists(select 1 from pg_catalog.pg_attribute a join r on r.oid=a.attrelid where a.attnum=e.ord and a.attname=e.name and a.atttypid=e.type_oid and not a.attisdropped and a.attidentity::text=e.identity and a.attgenerated=''))
 and not exists(select 1 from pg_catalog.pg_inherits i join r on r.oid in(i.inhrelid,i.inhparent))
 and not exists(select 1 from pg_catalog.pg_rewrite w join r on r.oid=w.ev_class)
 and (select count(*)=1 from pg_catalog.pg_policy p join r on r.oid=p.polrelid)
 and (select count(*)=2 from pg_catalog.pg_roles where rolname in('dee50tht','service_role'))
 and exists(select 1 from pg_catalog.pg_policy p join r on r.oid=p.polrelid where p.polname='cloudtms_miget_service_owner_all' and p.polcmd='*' and p.polpermissive and pg_catalog.pg_get_expr(p.polqual,p.polrelid)='true' and pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid)='true'
 and array(select unnest(p.polroles) order by 1)=(select array_agg(oid order by oid) from pg_catalog.pg_roles where rolname in('dee50tht','service_role')))
 and not exists(select 1 from expected_types e where not exists(select 1 from pg_catalog.pg_type t join pg_catalog.pg_namespace n on n.oid=t.typnamespace join pg_catalog.pg_proc snd on snd.oid=t.typsend join pg_catalog.pg_language sl on sl.oid=snd.prolang join pg_catalog.pg_proc outp on outp.oid=t.typoutput join pg_catalog.pg_language ol on ol.oid=outp.prolang
 where t.oid=e.oid and n.nspname='pg_catalog' and t.typtype='b' and t.typbasetype=0 and t.typisdefined
 and snd.pronargs=1 and snd.proargtypes[0]=t.oid and snd.prorettype=17 and not snd.proretset and snd.provariadic=0 and outp.pronargs=1 and outp.proargtypes[0]=t.oid and outp.prorettype=2275 and not outp.proretset and outp.provariadic=0
 and snd.pronamespace=n.oid and snd.proowner=10 and snd.prokind='f' and sl.lanname='internal' and snd.prosrc=e.send_symbol and snd.probin is null and snd.proconfig is null and not snd.prosecdef and snd.provolatile=e.send_vol and snd.proisstrict and snd.proparallel='s' and pg_catalog.has_function_privilege(current_user,snd.oid,'EXECUTE')
 and outp.pronamespace=n.oid and outp.proowner=10 and outp.prokind='f' and ol.lanname='internal' and outp.prosrc=e.out_symbol and outp.probin is null and outp.proconfig is null and not outp.prosecdef and outp.provolatile=e.out_vol and outp.proisstrict and outp.proparallel='s' and pg_catalog.has_function_privilege(current_user,outp.oid,'EXECUTE')))
 and not exists(select 1 from expected_functions e where not exists(select 1 from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace join pg_catalog.pg_language l on l.oid=p.prolang
 where p.oid=pg_catalog.to_regprocedure(e.identity) and p.pronargs=1 and p.proargtypes[0]=e.arg_oid and p.prorettype=e.return_oid and not p.proretset and p.provariadic=0 and n.nspname='pg_catalog' and p.proowner=10 and p.prokind='f' and l.lanname='internal' and p.prosrc=e.source_symbol and p.probin is null and p.proconfig is null and not p.prosecdef and p.provolatile=e.vol and p.proisstrict and p.proparallel='s' and pg_catalog.has_function_privilege(current_user,p.oid,'EXECUTE')))
 and pg_catalog.current_setting('server_version_num')='170011' and pg_catalog.current_setting('server_encoding')='UTF8' and pg_catalog.current_setting('client_encoding')='UTF8'
 and exists(select 1 from r join pg_catalog.pg_type audit_row_type on audit_row_type.oid=r.reltype join pg_catalog.pg_namespace audit_row_namespace on audit_row_namespace.oid=audit_row_type.typnamespace where audit_row_type.typtype='c' and audit_row_type.typisdefined and audit_row_type.typbasetype=0 and audit_row_type.typrelid=r.oid and audit_row_type.typowner=r.relowner and audit_row_namespace.nspname='public' and audit_row_type.typname='audit_events' and audit_row_type.typsend=pg_catalog.to_regprocedure('pg_catalog.record_send(record)')) as allowed
 ) is not true then raise exception 'QT_AUDIT_RECORD_ENCODER_CATALOGUE_UNAVAILABLE';end if;
 select count(*) into audit_before_count from pg_temp.hr_contact_qt_audit_before;
 select count(*) into audit_after_count from pg_temp.hr_contact_qt_audit_after;
 if audit_before_count>1500000 or audit_after_count>1500000 then
  raise exception 'QT_AUDIT_WHOLE_TABLE_ROW_BOUND_EXCEEDED';end if;
 if audit_before_count is distinct from audit_after_count then
  raise exception 'QT_UNRELATED_SETTINGS_OR_GLOBAL_AUDIT_CHANGED';end if;
 if exists(select 1 from pg_temp.hr_contact_qt_audit_before where event_sequence is null or row_digest is null or pg_catalog.octet_length(row_digest)<>32)
  or exists(select 1 from pg_temp.hr_contact_qt_audit_after where event_sequence is null or row_digest is null or pg_catalog.octet_length(row_digest)<>32) then
  raise exception 'QT_AUDIT_DIGEST_NATIVE_FRAME_INVALID';end if;
 audit_original_hashagg:=pg_catalog.current_setting('enable_hashagg');
 perform pg_catalog.set_config('enable_hashagg','off',true);
 if exists(select event_sequence,row_digest from pg_temp.hr_contact_qt_audit_before
   except all select event_sequence,row_digest from pg_temp.hr_contact_qt_audit_after)
  or exists(select event_sequence,row_digest from pg_temp.hr_contact_qt_audit_after
   except all select event_sequence,row_digest from pg_temp.hr_contact_qt_audit_before) then
  raise exception 'QT_UNRELATED_SETTINGS_OR_GLOBAL_AUDIT_CHANGED';end if;
 perform pg_catalog.set_config('enable_hashagg',audit_original_hashagg,true);
end;$qt_audit_equality$;
select value as programme_result from hr_contact_qt_result;
