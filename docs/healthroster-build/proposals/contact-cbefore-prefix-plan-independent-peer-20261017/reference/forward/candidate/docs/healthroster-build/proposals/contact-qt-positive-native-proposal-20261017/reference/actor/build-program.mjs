// Exact single-transaction actor programme; no connection or arbitrary SQL input.
import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';import {pathToFileURL,fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=s=>createHash('sha256').update(s).digest('hex'),literal=s=>"'"+String(s).replaceAll("'","''")+"'";
const check=(v,c)=>{if(!v)throw Error(c);};
const fixtureIDs=Object.freeze([1,2,3,4,5,6].map(n=>'f717d041-0000-4000-8000-'+String(n).padStart(12,'0')));
const fixtureEmails=Object.freeze(['admin','authoriser','golden','ordinary','inactive-admin','inactive-authoriser'].map(n=>'daily-health-actor-'+n+'-20261017@example.invalid'));
export function fixturePredicate(pins,alias='u'){
 check(alias==='u','ACTOR_FIXED_ALIAS_REQUIRED');
 check(JSON.stringify(pins.fixtureIDs)===JSON.stringify(fixtureIDs)&&JSON.stringify(pins.fixtureEmails)===JSON.stringify(fixtureEmails),'ACTOR_EXACT_RESERVED_NAMESPACE_REQUIRED');
 return `(${alias}.id=any(array[${pins.fixtureIDs.map(v=>literal(v)+'::uuid').join(',')}]::uuid[]) or ${alias}.email=any(array[${pins.fixtureEmails.map(literal).join(',')}]::text[]))`;
}
// Inspect the complete genuine saved diagnostic locally. The emitted native
// SQL compares its exact raw pg_get_functiondef hash, without normalization.
export function verifyNativeTouchDependency(pins,bytes){
 check(pins.nativeDependencyProof&&sha(bytes)===pins.nativeDependencyProof.sha256,'ACTOR_NATIVE_DIAGNOSTIC_BYTES_REQUIRED');
 const proof=JSON.parse(bytes),blocks=proof.definitions.filter(d=>d.definitions?.length===1&&d.definitions[0].schema_name==='public'&&d.definitions[0].function_name==='tms_touch_updated_at');
 check(blocks.length===1,'ACTOR_NATIVE_TOUCH_COMPLETE_PROOF_REQUIRED');const block=blocks[0],row=block.definitions[0];
 check(block.authenticated===true&&block.read_only===true&&block.database==='agency_test'&&block.matched===1&&row.identity_arguments==='','ACTOR_NATIVE_TOUCH_READONLY_PROOF_REQUIRED');
 check(sha(row.definition)===row.rawSha256&&row.rawSha256==='d5d7808078fd3fa302415a450a4e8255e6a2519f0a5f29b89416583637499b63'
  &&sha(row.definition.replaceAll('\r\n','\n'))===row.completeLfSha256
  &&row.completeLfSha256==='f6ea6fb5bdd1a6db720dd669ebd8da61f86651674a74ce7df23cdd4e630eefe6'
  &&pins.userStore.touch.definition_sha256===row.completeLfSha256&&pins.userStore.touch.nativeRawDefinitionSha256===row.rawSha256,'ACTOR_EXACT_NATIVE_TOUCH_SOURCE_REQUIRED');
 return row.rawSha256;
}
export async function buildActorProgram({root}){
 const pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json')),page='supabase/repeatable/17102026_0248_nhsp_v6_daily_health_support/01_complete_helpers.sqlinc';
 check(typeof root==='string'&&fs.realpathSync.native(root).replaceAll('\\','/')==='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026','ACTOR_REGISTERED_PHYSICAL_ROOT_REQUIRED');
 const bytes=fs.readFileSync(root+'/'+page),pin=pins.rootInputs.find(r=>r.file===page);
 check(pin&&sha(bytes)===pin.sha256&&bytes.length===pin.bytes,'ACTOR_EXACT_CURRENT_HELPER_REQUIRED');
 const contract=JSON.parse(fs.readFileSync(root+'/supabase/release/current-contract.json'));
 check(JSON.stringify(contract.relations.find(r=>r.schema==='public'&&r.name==='tms_users'))===JSON.stringify(pins.userStore.relation),'ACTOR_USER_STORE_SOURCE_CHANGED');
 const {scanManagedSql}=await import(pathToFileURL(root+'/scripts/cloudtms-managed-release-sql.mjs').href);
 const src=fs.readFileSync(root+'/'+page,'utf8'),parsed=scanManagedSql(src),signature='private.daily_rate_correction_health_actor_require_v1(uuid,boolean)';
 const selected=parsed.statements.map(s=>parsed.source.slice(s.start,s.end).trim()).filter(s=>s.startsWith('create or replace function private.daily_rate_correction_health_actor_require_v1('));check(selected.length===1,'ACTOR_EXACT_COMPLETE_OWNER_REQUIRED');
 const body=selected[0],store=pins.userStore,ids=pins.fixtureIDs,emails=pins.fixtureEmails;
 const touchRawHash=verifyNativeTouchDependency(pins,fs.readFileSync(here+'/'+pins.nativeDependencyProof.file));
 check(ids.length===6&&emails.length===6&&new Set(ids).size===6&&new Set(emails).size===6,'ACTOR_EXACT_FIXTURES_REQUIRED');
 const expectedColumns=store.relation.columns.map(c=>Object.fromEntries(['name','type','default','not_null','identity','generated'].map(k=>[k,c[k]])));
 const expectedConstraints=store.relation.constraints;
 const preflight=`set local search_path=pg_catalog,public;
do $actor_store$ begin
 if current_database()<>'cloudtms_test_clone' or current_setting('transaction_isolation')<>'read committed'
  or current_setting('transaction_read_only')<>'off' or current_user<>session_user
  or current_setting('server_version_num')::integer/10000<>17 then raise exception 'DAILY_ACTOR_NATIVE_RC_TEST_REQUIRED' using errcode='55000';end if;
 if to_regprocedure('private.weekly_source_pay_actor_allowed_v1(uuid)') is null
  or encode(sha256(convert_to(pg_get_functiondef('private.weekly_source_pay_actor_allowed_v1(uuid)'::regprocedure),'UTF8')),'hex')<>${literal(store.wp.definition_sha256)}
  or to_regprocedure('public.tms_touch_updated_at()') is null
  or encode(sha256(convert_to(pg_get_functiondef('public.tms_touch_updated_at()'::regprocedure),'UTF8')),'hex')<>${literal(touchRawHash)} then
  raise exception 'DAILY_ACTOR_NATIVE_WPPP_OR_TOUCH_SOURCE_CHANGED' using errcode='55000';end if;
 if exists(select 1 from pg_proc p where p.oid=any(array[
   'private.weekly_source_pay_actor_allowed_v1(uuid)'::regprocedure,'public.tms_touch_updated_at()'::regprocedure])
   and (pg_get_userbyid(p.proowner)<>current_user or not p.prosecdef or p.prokind<>'f')) then
  raise exception 'DAILY_ACTOR_NATIVE_OWNER_BOUNDARY_CHANGED' using errcode='55000';end if;
 if not exists(select 1 from pg_class c where c.oid='public.tms_users'::regclass and c.relkind='r' and c.relrowsecurity
    and not c.relforcerowsecurity and pg_get_userbyid(c.relowner)=current_user)
  or exists(select 1 from pg_inherits where inhrelid='public.tms_users'::regclass or inhparent='public.tms_users'::regclass)
  or exists(select 1 from pg_rewrite where ev_class='public.tms_users'::regclass) then
  raise exception 'DAILY_ACTOR_NATIVE_STORE_BOUNDARY_CHANGED' using errcode='55000';end if;
 if (select jsonb_agg(jsonb_build_object('name',a.attname,'type',format_type(a.atttypid,a.atttypmod),
   'default',pg_get_expr(d.adbin,d.adrelid),'not_null',a.attnotnull,'identity',a.attidentity::text,'generated',a.attgenerated::text) order by a.attname)
   from pg_attribute a left join pg_attrdef d on d.adrelid=a.attrelid and d.adnum=a.attnum
   where a.attrelid='public.tms_users'::regclass and a.attnum>0 and not a.attisdropped) is distinct from ${literal(JSON.stringify(expectedColumns))}::jsonb
  or (select jsonb_agg(jsonb_build_object('name',c.conname,'type',c.contype::text,'deferred',c.condeferred,
   'validated',c.convalidated,'deferrable',c.condeferrable,'definition',pg_get_constraintdef(c.oid,true)) order by c.conname)
   from pg_constraint c where c.conrelid='public.tms_users'::regclass) is distinct from ${literal(JSON.stringify(expectedConstraints))}::jsonb
  or (select coalesce(jsonb_agg(jsonb_build_object('name',t.tgname,'table','tms_users','schema','public',
   'enabled',t.tgenabled::text,'definition',pg_get_triggerdef(t.oid,true)) order by t.tgname),'[]'::jsonb)
   from pg_trigger t where t.tgrelid='public.tms_users'::regclass and not t.tgisinternal) is distinct from ${literal(JSON.stringify(store.triggers))}::jsonb then
  raise exception 'DAILY_ACTOR_NATIVE_STORE_INVARIANT_OR_TRIGGER_CHANGED' using errcode='55000';end if;
 if exists(select 1 from public.tms_users u where ${fixturePredicate(pins)}) then
  raise exception 'DAILY_ACTOR_FIXTURE_NAMESPACE_NOT_VACANT' using errcode='55000';end if;
end;$actor_store$;`;
 const acl=`do $actor_acl$ begin
 alter function ${signature} owner to current_user;
 revoke all on function ${signature} from public,anon,authenticated,service_role;
 if exists(select 1 from pg_proc p cross join lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
  where p.oid=${literal(signature)}::regprocedure and (a.grantee<>p.proowner or a.privilege_type<>'EXECUTE' or a.is_grantable))
  or has_function_privilege('anon',${literal(signature)},'EXECUTE')
  or has_function_privilege('authenticated',${literal(signature)},'EXECUTE')
  or has_function_privilege('service_role',${literal(signature)},'EXECUTE') then
  raise exception 'DAILY_ACTOR_NATIVE_PRIVATE_ACL_REQUIRED' using errcode='55000';end if;
end;$actor_acl$;`;
 const states=[['admin',true,false,false],['user',true,true,false],['user',true,true,true],['user',true,false,false],['admin',false,false,false],['user',false,true,false]];
 const expectedAfter=states.map((s,i)=>[ids[i],emails[i],s[0],s[1],'!DAILY_HEALTH_ACTOR_ROLLBACK_ONLY!',s[2],s[3]]);
 const rows=states.map((s,i)=>`(${literal(ids[i])}::uuid,${literal(emails[i])},${literal(s[0])},${s[1]},'!DAILY_HEALTH_ACTOR_ROLLBACK_ONLY!',${s[2]},${s[3]})`).join(',\n  ');
 const roles=`create temporary table daily_health_actor_native_result(value jsonb not null) on commit drop;
do $actor_native$
declare actor uuid;mode boolean;rejected boolean;allowed boolean;expected boolean;
 violated_constraint text;role_name text;null_refusals integer:=0;checks integer:=0;denials integer:=0;transitions integer:=0;constraint_refusals integer:=0;effective_acl_checks integer:=0;
begin
 insert into public.tms_users(id,email,role,is_active,password_hash,payment_authoriser,payment_golden_key) values
  ${rows};
 if (select count(*) from public.tms_users u where ${fixturePredicate(pins)})<>6 then raise exception 'DAILY_ACTOR_NATIVE_FIXTURE_COUNT_MISMATCH';end if;
 foreach actor in array array[${ids.map(v=>literal(v)+'::uuid').join(',')}] loop
  expected:=actor=${literal(ids[0])}::uuid;
  if private.weekly_source_pay_actor_allowed_v1(actor) is distinct from (actor=any(array[${ids.slice(0,3).map(v=>literal(v)+'::uuid').join(',')}]::uuid[])) then
   raise exception 'DAILY_ACTOR_GENUINE_WPPP_MATRIX_MISMATCH';end if;
  foreach mode in array array[false,true] loop
   rejected:=false;
   begin perform private.daily_rate_correction_health_actor_require_v1(actor,mode);
   exception when insufficient_privilege then if sqlerrm<>'DAILY_RATE_ACTOR_NOT_ALLOWED' then raise;end if;rejected:=true;end;
   if rejected=expected then raise exception 'DAILY_ACTOR_CURRENT_NATIVE_ROLE_MATRIX_MISMATCH';end if;
   checks:=checks+1;if rejected then denials:=denials+1;end if;
  end loop;
 end loop;
 -- Only this new native fixture is changed; these are same-transaction reads,
 -- not a cross-session revocation or advisory-wait proof.
 update public.tms_users set is_active=false where id=${literal(ids[0])}::uuid;
 rejected:=false;begin perform private.daily_rate_correction_health_actor_require_v1(${literal(ids[0])}::uuid,true);
 exception when insufficient_privilege then if sqlerrm<>'DAILY_RATE_ACTOR_NOT_ALLOWED' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'DAILY_ACTOR_OWN_INACTIVE_CHANGE_NOT_OBSERVED';end if;transitions:=transitions+1;
 update public.tms_users set is_active=true,role='user',payment_authoriser=true where id=${literal(ids[0])}::uuid;
 if private.weekly_source_pay_actor_allowed_v1(${literal(ids[0])}::uuid) is distinct from true then raise exception 'DAILY_ACTOR_REAL_PAY_FLAG_NOT_OBSERVED';end if;
 rejected:=false;begin perform private.daily_rate_correction_health_actor_require_v1(${literal(ids[0])}::uuid,true);
 exception when insufficient_privilege then if sqlerrm<>'DAILY_RATE_ACTOR_NOT_ALLOWED' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'DAILY_ACTOR_PAY_ONLY_CHANGE_WAS_ADMITTED';end if;transitions:=transitions+1;
 update public.tms_users set role='admin',payment_authoriser=false where id=${literal(ids[0])}::uuid;
 perform private.daily_rate_correction_health_actor_require_v1(${literal(ids[0])}::uuid,true);transitions:=transitions+1;
 rejected:=false;begin
  update public.tms_users set payment_authoriser=false where id=${literal(ids[2])}::uuid;
 exception when check_violation then get stacked diagnostics violated_constraint=CONSTRAINT_NAME;
  if violated_constraint<>'tms_users_payment_golden_implies_authoriser_chk' then raise;end if;rejected:=true;end;
 if not rejected or not exists(select 1 from public.tms_users where id=${literal(ids[2])}::uuid and payment_authoriser and payment_golden_key) then
  raise exception 'DAILY_ACTOR_GOLDEN_NATIVE_STORE_INVARIANT_NOT_PRESERVED';end if;constraint_refusals:=constraint_refusals+1;
 rejected:=false;begin perform private.daily_rate_correction_health_actor_require_v1(null,false);
 exception when insufficient_privilege then if sqlerrm<>'DAILY_RATE_ACTOR_NOT_ALLOWED' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'DAILY_ACTOR_NULL_ID_WAS_ADMITTED';end if;null_refusals:=null_refusals+1;
 rejected:=false;begin perform private.daily_rate_correction_health_actor_require_v1(${literal(ids[0])}::uuid,null);
 exception when insufficient_privilege then if sqlerrm<>'DAILY_RATE_ACTOR_NOT_ALLOWED' then raise;end if;rejected:=true;end;
 if not rejected then raise exception 'DAILY_ACTOR_NULL_LOCK_WAS_ADMITTED';end if;null_refusals:=null_refusals+1;
 -- Miget's generated owner cannot SET ROLE service_role. These are actual
 -- effective catalogue checks, not runtime execution/refusal under that role.
 foreach role_name in array array['anon','authenticated','service_role'] loop
  if has_function_privilege(role_name,${literal(signature)},'EXECUTE') then
   raise exception 'DAILY_ACTOR_PRIVATE_EFFECTIVE_ROLE_EXECUTE_WAS_GRANTED';end if;
  effective_acl_checks:=effective_acl_checks+1;
 end loop;
 if current_user<>session_user then raise exception 'DAILY_ACTOR_ROLE_RESET_REQUIRED';end if;
 if (select jsonb_agg(jsonb_build_array(u.id,u.email,u.role,u.is_active,u.password_hash,u.payment_authoriser,u.payment_golden_key) order by u.id)
   from public.tms_users u where ${fixturePredicate(pins)}) is distinct from ${literal(JSON.stringify(expectedAfter))}::jsonb
  or exists(select 1 from public.tms_users u where ${fixturePredicate(pins)} and (
   u.created_at is distinct from transaction_timestamp() or u.updated_at is distinct from transaction_timestamp()
   or u.display_name is not null or u.email_signature_html is not null or u.last_login_at_utc is not null
   or u.email_settings is distinct from '{}'::jsonb or u.grid_prefs_json is distinct from '{}'::jsonb or u.session_version is distinct from 1)) then
  raise exception 'DAILY_ACTOR_NATIVE_EXACT_AFTER_STATE_MISMATCH';end if;
 insert into daily_health_actor_native_result values(jsonb_build_object('programme','DAILY_HEALTH_ACTOR_NATIVE_V1',
  'fixtureRows',(select count(*) from public.tms_users u where ${fixturePredicate(pins)}),
  'matrixChecks',checks,'matrixDenials',denials,'sameTransactionChanges',transitions,'constraintRefusals',constraint_refusals,
  'nullGuardRefusals',null_refusals,'effectiveRoleAclChecks',effective_acl_checks,'directRoleExecution','NOT_RUN','nativeWPPP',true,'dailyBasis','NOT_RUN','postWaitRace','NOT_RUN','outerIsolation','read committed'));
end;$actor_native$;
select value as actor_result from daily_health_actor_native_result;`;
 const sql=[preflight,body,acl,roles].join('\n'),statements=scanManagedSql(sql);
 check(statements.meta.length===0&&statements.statements.length<1000&&Buffer.byteLength(sql)<8000000,'ACTOR_NATIVE_TRANSPORT_BOUND');
 return {sql,sqlSha256:sha(sql),sqlBytes:Buffer.byteLength(sql),definitionCount:1,businessFixtureRows:6,signature,
  matrixChecks:12,matrixDenials:10,sameTransactionChanges:3,constraintRefusals:1,effectiveRoleAclChecks:3,directRoleExecution:'NOT_RUN',nullGuardRefusals:2,
  directBusinessRelations:['public.tms_users'],snapshotRelations:['private.candidate_daily_batch_receipts','public.tms_users','public.ts_financials_outbox'],
  native:'NOT_RUN',dailyBasis:'NOT_RUN',isolation:'RC_ONLY',bankBody:'NOT_INCLUDED'};
}
