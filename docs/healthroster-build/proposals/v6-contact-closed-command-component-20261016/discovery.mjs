// Bounded SELECT-only genuine fixture discovery. No row payload/capability output.
import{hash}from'./component.mjs';
const required=[['tms_users','id'],['tms_users','is_active'],['tms_users','role'],['weekly_message_target_attempts','id'],['weekly_message_target_attempts','dispatch_target_id'],['weekly_message_target_attempts','lease_token'],['weekly_message_target_attempts','lease_owner'],['weekly_message_target_attempts','completed_at_utc'],['weekly_message_target_attempts','attempt_number'],['weekly_message_target_attempts','outcome'],['weekly_message_target_attempts','provider_message_id'],['weekly_message_target_attempts','bounded_provider_receipt_json'],['weekly_message_target_attempts','bounded_error_json'],['weekly_message_target_attempts','result_hash'],['weekly_message_dispatch_targets','id'],['weekly_message_dispatch_targets','dispatch_command_id'],['weekly_message_dispatch_commands','id'],['weekly_message_dispatch_commands','agency_id'],['weekly_message_dispatch_commands','environment']];
export function buildDiscoveryPackets(){
 const catalogue=`with required(relation_name,column_name)as(values ${required.map(([r,c])=>"('"+r+"','"+c+"')").join(',')}),missing as(select e.* from required e left join pg_namespace n on n.nspname='public'left join pg_class c on c.relnamespace=n.oid and c.relname=e.relation_name left join pg_attribute a on a.attrelid=c.oid and a.attname=e.column_name and a.attnum>0 and not a.attisdropped where a.attname is null)
 select jsonb_build_object('database',current_database(),'missing_required_columns',(select coalesce(jsonb_agg(to_jsonb(m)order by relation_name,column_name),'[]')from missing m),'digest_available',to_regprocedure('extensions.digest(bytea,text)')is not null,'scope','CATALOGUE_ONLY_NOT_FIXTURE_OR_ROLE_PROOF')as contact_discovery_preflight;\n`;
 const actor=`select 'ACTOR_CANDIDATE_NOT_ROLE_PROOF'case_kind,category,id actor_user_id,
 md5(jsonb_build_array(id,is_active,lower(btrim(coalesce(role,''))))::text)authority_fields_md5
 from(select * from(select 'ACTIVE_ADMIN'category,id,is_active,role from public.tms_users where is_active is true and lower(btrim(coalesce(role,'')))='admin'order by id limit 1)a
 union all select * from(select 'ACTIVE_NONADMIN'category,id,is_active,role from public.tms_users where is_active is true and lower(btrim(coalesce(role,'')))<>'admin'order by id limit 1)b
 union all select * from(select 'INACTIVE'category,id,is_active,role from public.tms_users where is_active is false order by id limit 1)c)s order by category;`;
 const recovery=`select 'ORIGINAL_START_RECOVERY_CANDIDATE_ONLY'case_kind,a.id attempt_id,t.id target_id,c.agency_id,
 a.completed_at_utc is null unknown_attempt,md5(jsonb_build_array(a.id,a.dispatch_target_id,a.attempt_number,a.completed_at_utc)::text)attempt_state_md5
 from public.weekly_message_target_attempts a join public.weekly_message_dispatch_targets t on t.id=a.dispatch_target_id
 join public.weekly_message_dispatch_commands c on c.id=t.dispatch_command_id where c.environment='TEST'
 and a.lease_token is not null and nullif(a.lease_owner,'')is not null order by a.id limit 5;`;
 const completed=`select 'COMPLETED_ORIGINAL_RESULT_CANDIDATE_ONLY'case_kind,a.id attempt_id,t.id target_id,c.agency_id,
 md5(jsonb_build_array(a.id,a.dispatch_target_id,a.attempt_number,a.completed_at_utc)::text)attempt_state_md5
 from public.weekly_message_target_attempts a join public.weekly_message_dispatch_targets t on t.id=a.dispatch_target_id
 join public.weekly_message_dispatch_commands c on c.id=t.dispatch_command_id where c.environment='TEST'
 and a.completed_at_utc is not null and a.result_hash is not null and a.outcome in('ACCEPTED','DEFINITELY_REJECTED','TRANSIENT_FAILURE','AMBIGUOUS')
 and a.result_hash=extensions.digest(convert_to(jsonb_build_object('provider_attempt_id',a.id,'outcome',upper(btrim(a.outcome)),
 'provider_message_id',nullif(btrim(a.provider_message_id),''),'bounded_provider_receipt',coalesce(a.bounded_provider_receipt_json,'{}'::jsonb),
 'bounded_error',coalesce(a.bounded_error_json,'{}'::jsonb))::text,'UTF8'),'sha256')order by a.id limit 5;`;
 const sql=[actor,recovery,completed].join('\n\n')+'\n',verify="select current_database()as database;\n";
 return{preflight:{database:'agency_test',sql:catalogue,verification_sql:verify,continue_on_error:false},discovery:{database:'agency_test',sql,verification_sql:verify,continue_on_error:false},expected:{version:'CONTACT_BOUNDED_READONLY_DISCOVERY_EXPECTED_V1',preflightSqlSha256:hash(catalogue),sqlSha256:hash(sql),statementCount:3,maxRows:[3,5,5],native:'NOT_RUN',qualification:'CANDIDATE_IDENTIFIERS_ONLY_NOT_ELIGIBILITY_OR_ROLE_PROOF',permittedOutputKeys:[['case_kind','category','actor_user_id','authority_fields_md5'],['case_kind','attempt_id','target_id','agency_id','unknown_attempt','attempt_state_md5'],['case_kind','attempt_id','target_id','agency_id','attempt_state_md5']]}};
}
export function validateDiscoveryResult(result){
 const{expected}=buildDiscoveryPackets(),fail=()=>{throw Error('CONTACT_DISCOVERY_NATIVE_RESULT_REFUSED');};
 if(result?.accepted!==true||result.database!=='cloudtms_test_clone'||result.sqlSha256!==expected.sqlSha256||result.statementCount!==3||result.rolledBack!==true||result.statements?.length!==3||result.rollbackVerification?.connectedFresh!==true||result.rollbackVerification.database!=='cloudtms_test_clone')fail();
 for(let i=0;i<3;i++){const s=result.statements[i];if(s.statement!==i+1||s.rowsTruncated!==false||Object.hasOwn(s,'error')||Object.hasOwn(s,'errorCode')||!Array.isArray(s.rows)||s.rows.length>expected.maxRows[i]||s.rows.some(row=>Object.keys(row).sort().join('|')!==expected.permittedOutputKeys[i].slice().sort().join('|')))fail();}
 const uuid=x=>typeof x==='string'&&/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(x),md5=x=>typeof x==='string'&&/^[a-f0-9]{32}$/.test(x);
 for(const r of result.statements[0].rows)if(r.case_kind!=='ACTOR_CANDIDATE_NOT_ROLE_PROOF'||!['ACTIVE_ADMIN','ACTIVE_NONADMIN','INACTIVE'].includes(r.category)||!uuid(r.actor_user_id)||!md5(r.authority_fields_md5))fail();
 if(new Set(result.statements[0].rows.map(r=>r.category)).size!==result.statements[0].rows.length)fail();
 for(let i=1;i<3;i++)for(const r of result.statements[i].rows)if(r.case_kind!==(i===1?'ORIGINAL_START_RECOVERY_CANDIDATE_ONLY':'COMPLETED_ORIGINAL_RESULT_CANDIDATE_ONLY')||!uuid(r.attempt_id)||!uuid(r.target_id)||!uuid(r.agency_id)||!md5(r.attempt_state_md5)||(i===1&&typeof r.unknown_attempt!=='boolean'))fail();
 const fresh=result.rollbackVerification.statements;if(fresh?.length!==1||fresh[0].statement!==1||fresh[0].rowsTruncated!==false||Object.hasOwn(fresh[0],'error')||Object.hasOwn(fresh[0],'errorCode')||fresh[0].rows?.length!==1||fresh[0].rows[0].database!=='cloudtms_test_clone')fail();
 return{status:'BOUNDED_NATIVE_CANDIDATE_DISCOVERY_VALIDATED',counts:result.statements.map(s=>s.rows.length),eligibilityProof:'NOT_RUN',apiRoleProof:'NOT_RUN'};
}
