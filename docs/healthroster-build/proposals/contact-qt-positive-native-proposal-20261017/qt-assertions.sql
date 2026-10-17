-- Root-only server-owned rollback B. These are native assertions, not a result fixture.
create temporary table hr_contact_qt_result(value jsonb not null) on commit drop;
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
 audits_before:=(select md5(coalesce(string_agg(md5(to_jsonb(a)::text),'|' order by md5(to_jsonb(a)::text)),'')) from public.audit_events a);
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
  or (select jsonb_agg(to_jsonb(s) order by s.id) from public.settings_defaults s) is distinct from defaults_before
  or (select md5(coalesce(string_agg(md5(to_jsonb(a)::text),'|' order by md5(to_jsonb(a)::text)),'')) from public.audit_events a) is distinct from audits_before then
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
select value as programme_result from hr_contact_qt_result;
