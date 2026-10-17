// Only two sealed programmes and genuine original turn identities are accepted.
import fs from'node:fs';
import {source,requireThat,literal,sha,ROUTE,SCOPE,markerFor}from'./source.mjs';
import {fixturePredicate}from'./build-program.mjs';
import {NATIVE_BOUNDARY_SQL}from'./native-boundary.mjs';
export {source,requireThat,literal,sha,ROUTE,SCOPE,markerFor};
export const heldSql=identity=>`exists(select 1 from pg_catalog.pg_locks where locktype='advisory' and database=${identity.databaseOid} and pid=${identity.backendPid} and classid=20261013 and objid=1 and objsubid=2 and mode='ExclusiveLock' and granted)`;
export const holderProofSql=identity=>`select jsonb_build_object('physicalDatabase',current_database(),'databaseOid',(select oid::bigint from pg_catalog.pg_database where datname=current_database()),'holderPid',${identity.backendPid},'backendPid',pg_backend_pid(),'holderGranted',(${heldSql(identity)}),'readOnly',current_setting('transaction_read_only')::boolean,'noRoleSwitch',current_user=session_user,'transactionStartedAt',transaction_timestamp(),'statementStartedAt',statement_timestamp(),'observedAt',clock_timestamp());`;
const guardClock=(identity,phase)=>`select jsonb_build_object('phase',${literal(phase)},'physicalDatabase',current_database(),'databaseOid',(select oid::bigint from pg_catalog.pg_database where datname=current_database()),'backendPid',pg_backend_pid(),'holderPid',${identity.backendPid},'holderGranted',(${heldSql(identity)}),'readOnly',current_setting('transaction_read_only')::boolean,'transactionStartedAt',transaction_timestamp(),'statementStartedAt',statement_timestamp(),'observedAt',clock_timestamp()) as hr_guard_clock;`;
export const FIXED_AUDIT_CTAS_SQL=Object.freeze(["create temporary table hr_contact_qt_audit_before on commit drop as select a.event_sequence,pg_catalog.sha256(pg_catalog.record_send(ROW(a,pg_catalog.current_setting('TimeZone')))) as row_digest from public.audit_events a limit 1500001;","create temporary table hr_contact_qt_audit_after on commit drop as select a.event_sequence,pg_catalog.sha256(pg_catalog.record_send(ROW(a,pg_catalog.current_setting('TimeZone')))) as row_digest from public.audit_events a limit 1500001;"]);
export function responseExpectations(text,scan){
 const parsed=scan(text);requireThat(parsed.meta.length===0,'CONTACT_QT_EXECUTABLE_SQL_ONLY');
 return parsed.statements.map((s,i)=>{const command=s.tokens[0],statementSql=parsed.source.slice(s.start,s.end).trim(),fixedAuditCtas=FIXED_AUDIT_CTAS_SQL.includes(statementSql);
  requireThat(['SELECT','SET','DO','CREATE','ALTER','REVOKE','GRANT','DROP','LOCK','RESET','NOTIFY'].includes(command),'CONTACT_QT_RESPONSE_COMMAND_REFUSED');
  return {statement:i+1,command:fixedAuditCtas?'SELECT':command,kind:fixedAuditCtas?'FIXED_AUDIT_CTAS_BOUNDED_STORED_ROWS':command==='SELECT'?'ONE_ROW':'EMPTY_COMMAND',sqlSha256:sha(parsed.source.slice(s.start,s.end).trim()),
   abiQualification:fixedAuditCtas?'PG17_SELECT_0_TO_1500000_EMPTY_FIELDS_ROWS_SOURCE_EXPECTATION_NATIVE_CTAS_UNOBSERVED':command==='SELECT'?'STRICT_NATIVE_INTEGER_ONE_ROW_REQUIRED':'CLOSED_EXACT_STATEMENT_AND_PROVIDER_COMMAND_TAG_EXPECTATION_NATIVE_CASE_NOT_RUN'};
 });
}
export async function buildInput({root,identity,caseId}){
 const {programme,rehearsal,live,sql,control,pins}=await source(root,caseId);control.assertTurnIdentity(identity);
 const marker=markerFor(identity);requireThat(/^hr_install2_[a-f0-9]{32}$/.test(marker),'CONTACT_QT_REAL_MARKER_IDENTITY_REQUIRED');
 const chunks=[],indices={};const append=(text,key)=>{const start=sql.scanManagedSql(chunks.join('\n')).statements.length+1;chunks.push(text);if(key)indices[key]=start+sql.scanManagedSql(text).statements.length-1;return start;};
 append("set local jit=off;set local pg_show_plans.is_enabled=off;set local work_mem='4MB';set local maintenance_work_mem='32MB';");
 append(guardClock(identity,'B_GUARD_BEFORE'),'guardBefore');
 append(`do $contact_qt_guard$ begin if current_database()<>'cloudtms_test_clone'
  or current_setting('transaction_isolation')<>'read committed' or current_setting('transaction_read_only')<>'off' or current_user<>session_user
  or current_setting('server_version_num')::integer/10000<>17
  or (select oid from pg_catalog.pg_database where datname=current_database())<>${identity.databaseOid} or not (${heldSql(identity)})
  or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null)
  or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1)
  or exists(select 1 from private.cloudtms_database_releases where status='APPLYING') or to_regclass(${literal('private.'+marker)}) is not null
  or ${NATIVE_BOUNDARY_SQL}
  or exists(select 1 from public.tms_users u where ${fixturePredicate(pins)}) then raise exception 'CONTACT_QT_TURN_TEST_OR_VACANCY_REFUSED' using errcode='55000';end if;end;$contact_qt_guard$;`,'guard');
 append(guardClock(identity,'B_GUARD_AFTER'),'guardAfter');append(`create table private.${marker}(proof boolean not null);`,'markerCreation');append(live.transactionIdentitySql(),'transaction');
 const programmeStart=append(programme.sql,'programmeResult');append(live.fixtureObservationSql(0),'fixtureObservation');
 append(`select jsonb_build_object('physicalDatabase',current_database(),'databaseOid',(select oid::bigint from pg_catalog.pg_database where datname=current_database()),'backendPid',pg_backend_pid(),'xid',pg_current_xact_id()::text,'marker',${literal(marker)},'markerPresent',to_regclass(${literal('private.'+marker)}) is not null,'markerRows',(select count(*) from private.${marker}),'holderPid',${identity.backendPid},'holderGranted',(${heldSql(identity)}),'noRoleSwitch',current_user=session_user,'transactionStartedAt',transaction_timestamp(),'statementStartedAt',statement_timestamp(),'observedAt',clock_timestamp());`,'completion');
 const text=chunks.join('\n');requireThat(text.split(programme.sql).length===2,'CONTACT_QT_PROGRAMME_EMBED_ONCE');
 const fresh=`select jsonb_build_object('userFixtureRowsRemaining',(select count(*) from public.tms_users u where ${fixturePredicate(pins)}),'readOnly',current_setting('transaction_read_only')::boolean,'backendPid',pg_backend_pid()${caseId==='QT_ADMIN_CONTROLS_V1'?",'qtObjectsAbsent',to_regclass('public.candidate_push_timing_policy') is null and to_regclass('public.candidate_push_timing_commands') is null":''});`;
 const verification="select pg_catalog.set_config('search_path','pg_catalog, public',true);\n"+rehearsal.identityProbeSql(marker)+'\n'+holderProofSql(identity)+'\n'+fresh;
 const relations=pins.relationNames.map(name=>({schema:name.split('.')[0],name:name.split('.')[1],kind:'r'}));
 const canonicalSql=rehearsal.restorationSnapshotSql({identity,marker,relations,exportContractSql:fs.readFileSync(root+'/supabase/release/export_contract.sql','utf8'),observeLiveBusiness:true});
 const input={database:'agency_test',label:'HR_CONTACT_QT_'+caseId+'_'+identity.turnId.slice(8).toUpperCase(),sql:text,verification_sql:verification,continue_on_error:false,max_rows_per_statement:1,statement_timeout_ms:30000,lock_timeout_ms:2000};
 const receipt={version:'CONTACT_QT_POSITIVE_ROLLBACK_INPUT_V1',caseId,scope:SCOPE,route:ROUTE,holderIdentity:identity,marker,label:input.label,
  sqlSha256:sha(text),verificationSqlSha256:sha(verification),programmeSha256:programme.sqlSha256,programmeBytes:programme.sqlBytes,programmeStart,expectedProgramme:programme.expected,
  statements:sql.scanManagedSql(text).statements.length,indices,canonicalSqlSha256:sha(canonicalSql),snapshotRelations:pins.relationNames,
  responses:responseExpectations(text,sql.scanManagedSql),verificationResponses:responseExpectations(verification,sql.scanManagedSql),businessFixtureRows:programme.businessFixtureRows,
  markerControlRelationCreated:true,bankingBody:'NOT_INCLUDED',native:'NOT_RUN'};
 requireThat(Buffer.byteLength(text)<=8000000&&receipt.statements<=1000,'CONTACT_QT_MCP_ENVELOPE_REQUIRED');return {input,receipt};
}
