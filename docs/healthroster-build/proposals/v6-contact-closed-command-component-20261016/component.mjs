import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{createHash,randomBytes}from'node:crypto';
import{isDeepStrictEqual}from'node:util';
export const here=path.dirname(fileURLToPath(import.meta.url));
export const sourceInputsSha256='24ef002dd779b88b21bbf648dfb677be58bfa6d4960fedd7367eb5788bf0b6d0';
export const hash=value=>createHash('sha256').update(value).digest('hex');
const fail=code=>{throw Error('CONTACT_CLOSED_COMPONENT_'+code);},quote=s=>"'"+s.replaceAll("'","''")+"'",same=isDeepStrictEqual;
export function loadInputs(read=p=>fs.readFileSync(path.join(here,p))){
 const bytes=read('SOURCE_INPUTS.json');if(hash(bytes)!==sourceInputsSha256)fail('SOURCE_INPUTS_CHANGED');const data=JSON.parse(bytes);
 if(data.version!=='CONTACT_CLOSED_COMMAND_COMPONENT_INPUTS_V1'||data.functions.length!==4||data.sourceCheckpoint!=='fae51b0e0bb33e3e3d62cce47bb6aa96da9b3fbe')fail('SOURCE_SCOPE_CHANGED');
 for(const p of data.files){const b=read(p.path);if(b.length!==p.bytes||hash(b)!==p.sha256)fail('COMPLETE_SOURCE_CHANGED');}
 const functions=data.functions.map(p=>{const sql=read(p.file).toString().trim();if(hash(sql)!==p.completeStatementSha256)fail('COMPLETE_DECLARATION_CHANGED');return{...p,sql};});
 if(functions[0].signature!=='private.weekly_source_query_require_service_v1()'||functions[1].signature!=='public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)'||functions[2].signature!=='public.weekly_source_message_dispatch_result_atomic_v1(jsonb)'||functions[3].signature!=='private.weekly_source_protected_qualification_owner_v1()')fail('SOURCE_SIGNATURE_CHANGED');
 return{data,functions};
}
function fingerprintQuery(functions,marker){
 const values=functions.map(p=>'('+quote(p.signature)+')').join(',');
 return `with expected(signature)as(values ${values}),functions as(
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
 'marker_absent',to_regprocedure(${quote('private.'+marker+'()')})is null)as contact_component_snapshot`;
}
function metadataCondition(p,alias='p'){
 return `coalesce((${alias}.oid is not null and md5(${alias}.prosrc)=${quote(p.rawProsrcMd5)} and ${alias}.prosecdef=${p.securityDefiner} and ${alias}.provolatile=${quote(p.volatility)} and (select lanname from pg_language where oid=${alias}.prolang)='plpgsql' and cardinality(${alias}.proconfig)=1 and replace(replace(${alias}.proconfig[1],' ',''),'"','')=${quote('search_path='+p.searchPath)}),false)`;
}
const heldCodes=new Set(['CONTACT_COMPONENT_IDENTITY_RELATION_MISSING','CONTACT_COMPONENT_TEST_IDENTITY_REQUIRED','CONTACT_COMPONENT_PROTECTED_OWNER_HELPER_MISSING','CONTACT_COMPONENT_PROTECTED_OWNER_HELPER_CHANGED','CONTACT_COMPONENT_SERVICE_HELPER_MISSING','CONTACT_COMPONENT_SERVICE_HELPER_CHANGED','CONTACT_COMPONENT_ACTUAL_LOGIN_OWNER_REQUIRED','CONTACT_COMPONENT_REAL_ROLES_REQUIRED','CONTACT_COMPONENT_SCHEMA_PRIVILEGES_REQUIRED','CONTACT_COMPONENT_EXISTING_SERVICE_CONTEXT_REQUIRED','CONTACT_COMPONENT_SERVICE_HELPER_OWNER_REQUIRED','CONTACT_COMPONENT_EXISTING_COMMAND_OWNER_REQUIRED']);
export function buildPacket({mode='session-context',marker='hr_contact_closed_'+randomBytes(16).toString('hex'),read}={}){
 if(!['session-context','protected-owner-request'].includes(mode))fail('MODE_REFUSED');if(!/^hr_contact_closed_[a-f0-9]{32}$/.test(marker)||marker.length>63)fail('MARKER_REFUSED');
 const{data,functions}=loadInputs(read),service=functions[0],start=functions[1],result=functions[2],protectedOwner=functions[3],snapshot=fingerprintQuery(functions,marker),harness='private.'+marker;
 const guard=`do $contact_preflight$
 declare actual_owner name; p pg_proc%rowtype; begin
 if to_regclass('private.cloudtms_database_identity')is null then raise exception 'CONTACT_COMPONENT_IDENTITY_RELATION_MISSING' using errcode='55000';end if;
 if current_database()<>'cloudtms_test_clone' or (select count(*)from private.cloudtms_database_identity where singleton and environment='TEST')<>1 then raise exception 'CONTACT_COMPONENT_TEST_IDENTITY_REQUIRED' using errcode='42501';end if;
 select * into p from pg_proc where oid=to_regprocedure(${quote(protectedOwner.signature)});
 if not found then raise exception 'CONTACT_COMPONENT_PROTECTED_OWNER_HELPER_MISSING' using errcode='55000';end if;
 if not(${metadataCondition(protectedOwner)})then raise exception 'CONTACT_COMPONENT_PROTECTED_OWNER_HELPER_CHANGED' using errcode='55000';end if;
 actual_owner:=pg_get_userbyid(p.proowner);
 if session_user is distinct from actual_owner or current_user is distinct from actual_owner then raise exception 'CONTACT_COMPONENT_ACTUAL_LOGIN_OWNER_REQUIRED' using errcode='42501';end if;
 select * into p from pg_proc where oid=to_regprocedure(${quote(service.signature)});
 if not found then raise exception 'CONTACT_COMPONENT_SERVICE_HELPER_MISSING' using errcode='55000';end if;
 if not(${metadataCondition(service)})then raise exception 'CONTACT_COMPONENT_SERVICE_HELPER_CHANGED' using errcode='55000';end if;
 if pg_get_userbyid(p.proowner)is distinct from actual_owner then raise exception 'CONTACT_COMPONENT_SERVICE_HELPER_OWNER_REQUIRED' using errcode='42501';end if;
 if exists(select 1 from pg_proc q where q.oid in(to_regprocedure(${quote(start.signature)}),to_regprocedure(${quote(result.signature)}))and pg_get_userbyid(q.proowner)is distinct from actual_owner)then raise exception 'CONTACT_COMPONENT_EXISTING_COMMAND_OWNER_REQUIRED' using errcode='42501';end if;
 if (select count(*)from pg_roles where rolname in('anon','authenticated','service_role'))<>3 then raise exception 'CONTACT_COMPONENT_REAL_ROLES_REQUIRED' using errcode='55000';end if;
 if not has_schema_privilege(current_user,'private','USAGE')or not has_schema_privilege(current_user,'private','CREATE')or not has_schema_privilege(current_user,'public','USAGE')or not has_schema_privilege(current_user,'public','CREATE')or not has_schema_privilege('service_role','public','USAGE')then raise exception 'CONTACT_COMPONENT_SCHEMA_PRIVILEGES_REQUIRED' using errcode='42501';end if;
${mode==='session-context'?`if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'then raise exception 'CONTACT_COMPONENT_EXISTING_SERVICE_CONTEXT_REQUIRED' using errcode='55000';end if;`:''}
 if to_regprocedure(${quote(harness+'()')})is not null then raise exception 'CONTACT_COMPONENT_MARKER_ALREADY_PRESENT' using errcode='55000';end if;
 end $contact_preflight$;`;
 const parts=[snapshot+';',guard];
 if(mode==='protected-owner-request')parts.push(`do $contact_owner_request$ begin
 -- Existing reviewed owner request dialect, transaction-local component test only.
 -- The preceding guard proved actual session_user/current_user equals the unchanged
 -- installed protected owner. No role, actor, writer or capability is manufactured.
 perform set_config('request.jwt.claim.role','service_role',true);
 perform private.weekly_source_protected_qualification_owner_v1();
 end $contact_owner_request$;`);
 parts.push(service.sql,...data.acl.projectedStatements.slice(0,2),start.sql,data.acl.soleMappedResult,...data.acl.startStatements.slice(1),result.sql,...data.acl.projectedStatements.slice(2));
 parts.push(`do $contact_catalogue$ declare p pg_proc%rowtype; begin
 ${functions.slice(0,3).map(p=>`select * into strict p from pg_proc where oid=${quote(p.signature)}::regprocedure;
 if not(${metadataCondition(p)})or pg_get_userbyid(p.proowner)is distinct from current_user then raise exception 'CONTACT_COMPONENT_EXACT_NATIVE_DECLARATION_REQUIRED' using errcode='55000';end if;
 if has_function_privilege('anon',p.oid,'EXECUTE')or has_function_privilege('authenticated',p.oid,'EXECUTE')or has_function_privilege('service_role',p.oid,'EXECUTE')is distinct from ${p.signature.startsWith('public.')} then raise exception 'CONTACT_COMPONENT_NATIVE_ACL_REQUIRED' using errcode='42501';end if;`).join('\n')}
 end $contact_catalogue$;`);
 parts.push(`create function ${harness}()returns jsonb language plpgsql security invoker set search_path='pg_catalog','pg_temp'
 as $contact_native_assertions$ declare message text;closed_count integer:=0; begin
 perform private.weekly_source_protected_qualification_owner_v1();
 perform private.weekly_source_query_require_service_v1();
 begin perform public.weekly_source_message_dispatch_submission_start_atomic_v1('{}'::jsonb);
 raise exception 'CONTACT_COMPONENT_COMMAND_START_REOPENED';
 exception when object_not_in_prerequisite_state then get stacked diagnostics message=message_text;
 if message<>'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED'then raise;end if;closed_count:=closed_count+1;end;
 begin perform public.weekly_source_message_dispatch_result_atomic_v1('{}'::jsonb);
 raise exception 'CONTACT_COMPONENT_COMMAND_RESULT_REOPENED';
 exception when object_not_in_prerequisite_state then get stacked diagnostics message=message_text;
 if message<>'WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED'then raise;end if;closed_count:=closed_count+1;end;
 if closed_count<>2 then raise exception 'CONTACT_COMPONENT_NATIVE_ASSERTION_COUNT_REQUIRED';end if;
 return jsonb_build_object('closed_count',closed_count,'service_helper_first_use',true,'protected_owner_first_use',true,'native_acl_checked',true,'marker',${quote(marker)},'mode',${quote(mode)},'scope','CLOSED_COMMAND_COMPONENT_ONLY','api_role_proof','NOT_RUN','provider_called',false);
 end $contact_native_assertions$;`);
 parts.push(`revoke all on function ${harness}()from public,anon,authenticated,service_role;`);
 parts.push(`select jsonb_build_object('status','CLOSED_COMMAND_COMPONENT_ASSERTIONS_REACHED','database',current_database(),'marker',${quote(marker)},'mode',${quote(mode)},'proof',${harness}())as contact_component_result;`);
 const sql=parts.join('\n\n')+'\n',verification_sql=snapshot+';\n';if(Buffer.byteLength(sql)>2_000_000||parts.length>100)fail('BOUNDED_TOOL_ENVELOPE_REQUIRED');
 const expected={version:'CONTACT_CLOSED_COMMAND_COMPONENT_EXPECTED_V1',sourceInputsSha256,sourceCheckpoint:data.sourceCheckpoint,mode,marker,sqlSha256:hash(sql),verificationSqlSha256:hash(verification_sql),statementCount:parts.length,sourceFunctions:3,protectedHelperPreexistingRequired:true,heldBeforeOverlayStatement:2,beforeSnapshotStatement:1,proofStatement:parts.length,scope:'CLOSED_COMMAND_COMPONENT_ONLY',apiRoleProof:'NOT_RUN',native:'NOT_RUN'};
 return{request:{database:'agency_test',sql,verification_sql,continue_on_error:false},expected};
}
export function preflightPacket(){
 const{functions}=loadInputs(),protectedOwner=functions[3],service=functions[0];
 const sql=`select jsonb_build_object('database',current_database(),'private_schema_present',to_regnamespace('private')is not null,'identity_relation_present',to_regclass('private.cloudtms_database_identity')is not null,'migration_ledger_present',to_regclass('private.cloudtms_migration_ledger')is not null,'repeatable_ledger_present',to_regclass('private.cloudtms_repeatable_ledger')is not null,
 'current_user',current_user,'session_user',session_user,'protected_owner',(select pg_get_userbyid(proowner)from pg_proc where oid=to_regprocedure(${quote(protectedOwner.signature)})),
 'protected_owner_helper_present',to_regprocedure(${quote(protectedOwner.signature)})is not null,
 'protected_owner_helper_body_matches',coalesce((select ${metadataCondition(protectedOwner)}from pg_proc p where oid=to_regprocedure(${quote(protectedOwner.signature)})),false),
 'service_helper_present',to_regprocedure(${quote(service.signature)})is not null,
 'service_helper_body_matches',coalesce((select ${metadataCondition(service)}from pg_proc p where oid=to_regprocedure(${quote(service.signature)})),false),
 'existing_service_claim_context',coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')='service_role',
 'roles',(select jsonb_agg(jsonb_build_object('role',rolname,'superuser',rolsuper,'bypass_rls',rolbypassrls)order by rolname)from pg_roles where rolname in('anon','authenticated','service_role')),
 'existing_acl',(select jsonb_agg(jsonb_build_object('signature',e.signature,'present',p.oid is not null,'owner',pg_get_userbyid(p.proowner),
 'anon_execute',case when exists(select 1 from pg_roles where rolname='anon')then has_function_privilege('anon',p.oid,'EXECUTE')end,
 'authenticated_execute',case when exists(select 1 from pg_roles where rolname='authenticated')then has_function_privilege('authenticated',p.oid,'EXECUTE')end,
 'service_execute',case when exists(select 1 from pg_roles where rolname='service_role')then has_function_privilege('service_role',p.oid,'EXECUTE')end)order by e.signature)
 from(values ${functions.map(p=>'('+quote(p.signature)+')').join(',')})e(signature)left join pg_proc p on p.oid=to_regprocedure(e.signature)),
 'scope','CATALOGUE_AND_EXISTING_CONTEXT_ONLY','api_role_proof','NOT_RUN')as contact_component_preflight;\n`;
 return{database:'agency_test',sql,verification_sql:"select current_database()as database;\n",continue_on_error:false};
}
const errorCode=row=>typeof row.error==='string'?row.error:row.error?.message;
function validSnapshot(s,functions){
 const md5=x=>typeof x==='string'&&/^[a-f0-9]{32}$/.test(x),keys=(o,k)=>o&&same(Object.keys(o).sort(),k.slice().sort()),ledger=x=>keys(x,['count','md5'])&&Number.isSafeInteger(x.count)&&x.count>=0&&md5(x.md5);
 return keys(s,['database','postmaster_epoch','server_version_num','functions','schemas','default_acl','migration_ledger','repeatable_ledger','roles','identity_relation_present','marker_absent'])&&s.database==='cloudtms_test_clone'&&typeof s.postmaster_epoch==='string'&&/^\d+(?:\.\d+)?$/.test(s.postmaster_epoch)&&/^\d+$/.test(s.server_version_num)&&keys(s.functions,functions.map(f=>f.signature))&&Object.values(s.functions).every(x=>x===null||md5(x))&&keys(s.schemas,['public','private'])&&Object.values(s.schemas).every(md5)&&md5(s.default_acl)&&ledger(s.migration_ledger)&&ledger(s.repeatable_ledger)&&s.roles&&Object.keys(s.roles).every(k=>['anon','authenticated','service_role'].includes(k))&&Object.values(s.roles).every(a=>Array.isArray(a)&&a.length===7&&a.every(x=>typeof x==='boolean'))&&typeof s.identity_relation_present==='boolean'&&s.marker_absent===true;
}
export function validateNativeResult(result,expected){
 if(expected?.version!=='CONTACT_CLOSED_COMMAND_COMPONENT_EXPECTED_V1'||expected.sourceInputsSha256!==sourceInputsSha256||!['session-context','protected-owner-request'].includes(expected.mode)||!/^hr_contact_closed_[a-f0-9]{32}$/.test(expected.marker))fail('EXPECTED_IDENTITY_REFUSED');
 if(!same(expected,buildPacket({mode:expected.mode,marker:expected.marker}).expected))fail('EXPECTED_IDENTITY_REFUSED');
 if(result?.accepted!==true||result.database!=='cloudtms_test_clone'||result.sqlSha256!==expected.sqlSha256||result.statementCount!==expected.statementCount||result.rolledBack!==true||!Array.isArray(result.statements))fail('NATIVE_ENVELOPE_REFUSED');
 const rows=result.statements;
 for(const[row,i]of rows.map((r,i)=>[r,i]))if(row.statement!==i+1||row.rowsTruncated!==false)fail('NATIVE_STATEMENT_FRAME_REFUSED');
 const before=rows[0]?.rows?.[0]?.contact_component_snapshot;if(rows[0]?.rows?.length!==1||!validSnapshot(before,loadInputs().functions))fail('BEFORE_SNAPSHOT_REFUSED');
 const fresh=result.rollbackVerification;if(fresh?.connectedFresh!==true||fresh.database!=='cloudtms_test_clone'||!Array.isArray(fresh.statements)||fresh.statements.length!==1||fresh.statements[0].statement!==1||fresh.statements[0].rowsTruncated!==false||Object.hasOwn(fresh.statements[0],'error')||Object.hasOwn(fresh.statements[0],'errorCode'))fail('FRESH_CONNECTION_REFUSED');
 const restored=fresh.statements[0].rows?.[0]?.contact_component_snapshot;if(fresh.statements[0].rows?.length!==1||!same(before,restored)||restored.marker_absent!==true)fail('FRESH_RESTORATION_REFUSED');
 const errors=rows.filter(r=>Object.hasOwn(r,'error')||Object.hasOwn(r,'errorCode'));
 if(errors.length){const row=errors[0],message=errorCode(row);if(errors.length===1&&rows.length===2&&row.statement===2&&heldCodes.has(message))return{status:'HELD_BEFORE_OVERLAY',reason:message,restorationVerified:true,nativeClosedCommandProof:'NOT_RUN',apiRoleProof:'NOT_RUN'};fail('NATIVE_STATEMENT_ERROR');}
 if(rows.length!==expected.statementCount||rows.at(-1).rows?.length!==1||rows.slice(1,-1).some(r=>!Array.isArray(r.rows)||r.rows.length!==0))fail('NATIVE_STATEMENT_CENSUS_REFUSED');
 const final=rows.at(-1).rows?.[0]?.contact_component_result,proof=final?.proof;
 if(final?.status!=='CLOSED_COMMAND_COMPONENT_ASSERTIONS_REACHED'||final.database!=='cloudtms_test_clone'||final.marker!==expected.marker||final.mode!==expected.mode||!same(proof,{closed_count:2,service_helper_first_use:true,protected_owner_first_use:true,native_acl_checked:true,marker:expected.marker,mode:expected.mode,scope:'CLOSED_COMMAND_COMPONENT_ONLY',api_role_proof:'NOT_RUN',provider_called:false}))fail('NATIVE_ASSERTION_REFUSED');
 return{status:'NATIVE_CLOSED_COMMAND_COMPONENT_AND_FRESH_RESTORATION_VALIDATED',mode:expected.mode,sourceFunctions:3,closedCommands:2,restorationVerified:true,scope:'CLOSED_COMMAND_COMPONENT_ONLY',apiRoleProof:'NOT_RUN',providerCalled:false};
}
