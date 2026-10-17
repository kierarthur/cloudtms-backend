// This builder accepts an original turn identity, never caller SQL or a case alias.
import fs from 'node:fs';
import path from 'node:path';
import {source,requireThat,literal,sha,ROUTE,SCOPE,markerFor} from './source.mjs';
export {source,requireThat,literal,sha,ROUTE,SCOPE,markerFor};
export function heldSql(identity){return `exists(select 1 from pg_catalog.pg_locks where locktype='advisory' and database=${identity.databaseOid}
 and pid=${identity.backendPid} and classid=20261013 and objid=1 and objsubid=2 and mode='ExclusiveLock' and granted)`;}
export function holderProofSql(identity){return `select jsonb_build_object('physicalDatabase',current_database(),
 'databaseOid',(select oid::bigint from pg_catalog.pg_database where datname=current_database()),
 'holderPid',${identity.backendPid},'backendPid',pg_backend_pid(),'holderGranted',(${heldSql(identity)}),
 'readOnly',current_setting('transaction_read_only')::boolean,'noRoleSwitch',current_user=session_user,
 'transactionStartedAt',transaction_timestamp(),'statementStartedAt',statement_timestamp(),'observedAt',clock_timestamp());`;}
function guardClockSql(identity,phase){
 requireThat(['B_GUARD_BEFORE','B_GUARD_AFTER'].includes(phase),'PURE_CLOCK_PHASE_REFUSED');
 return `select jsonb_build_object('phase',${literal(phase)},'physicalDatabase',current_database(),
  'databaseOid',(select oid::bigint from pg_catalog.pg_database where datname=current_database()),'backendPid',pg_backend_pid(),
  'holderPid',${identity.backendPid},'holderGranted',(${heldSql(identity)}),'readOnly',current_setting('transaction_read_only')::boolean,
  'transactionStartedAt',transaction_timestamp(),'statementStartedAt',statement_timestamp(),'observedAt',clock_timestamp()) as hr_guard_clock;`;
}
export function responseExpectations(source,scanManagedSql){
 const parsed=scanManagedSql(source);requireThat(parsed.meta.length===0,'PURE_RESPONSE_EXECUTABLE_SQL_ONLY');
 return parsed.statements.map((s,i)=>{
  const t=s.tokens;let command;
  if(t[0]==='SELECT')command='SELECT';
  else if(t[0]==='SET'&&t[1]==='LOCAL')command='SET';
  else if(t[0]==='DO')command='DO';
  else if(t[0]==='CREATE'&&(t[1]==='TABLE'||t.slice(1,4).join(' ')==='OR REPLACE FUNCTION'))command='CREATE';
  else if(t[0]==='RESET'&&(t[1]==='ROLE'||t.slice(1,3).join(' ')==='SESSION AUTHORIZATION'))command='RESET';
  else throw Error('PURE_RESPONSE_COMMAND_REFUSED');
  return {statement:i+1,command,kind:command==='SELECT'?'ONE_ROW':'EMPTY_COMMAND',
   sqlSha256:sha(parsed.source.slice(s.start,s.end).trim()),
   abiQualification:command==='SET'?'OBSERVED_GENUINE_R2':command==='SELECT'?'INTEGER_ROW_COUNT_REQUIRED':'SOURCE_EXPECTATION_NATIVE_NOT_PROVED'};
 });
}
export async function buildInput({root,identity}){
 const {programme,rehearsal,live,sql,control}=await source(root);control.assertTurnIdentity(identity);
 const marker=markerFor(identity);requireThat(/^hr_install2_[a-f0-9]{32}$/.test(marker),'PURE_MARKER_IDENTITY_REQUIRED');
 const chunks=[],indices={};
 const append=(text,key)=>{const start=sql.scanManagedSql(chunks.join('\n')).statements.length+1;chunks.push(text);
  if(key)indices[key]=start+sql.scanManagedSql(text).statements.length-1;return start;};
 append("set local jit=off; set local pg_show_plans.is_enabled=off; set local work_mem='4MB'; set local maintenance_work_mem='32MB';");
 append(guardClockSql(identity,'B_GUARD_BEFORE'),'guardBefore');
 append(`do $pure_guard$ begin if current_database()<>'cloudtms_test_clone'
  or current_setting('transaction_isolation')<>'read committed' or current_setting('transaction_read_only')<>'off'
  or current_user<>session_user or (select oid from pg_catalog.pg_database where datname=current_database())<>${identity.databaseOid}
  or not (${heldSql(identity)})
  or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null)
  or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1)
  or exists(select 1 from private.cloudtms_database_releases where status='APPLYING')
  or to_regclass(${literal('private.'+marker)}) is not null
  then raise exception 'DAILY_PURE_TURN_OR_TEST_IDENTITY_REFUSED' using errcode='55000';end if;end;$pure_guard$;`,'guard');
 append(guardClockSql(identity,'B_GUARD_AFTER'),'guardAfter');
 // This real relation is created inside server-owned B and must disappear after rollback.
 append(`create table private.${marker}(proof boolean not null);`,'markerCreation');
 append(live.transactionIdentitySql(),'transaction');
 const programmeStart=append(programme.sql,'programmeResult');
 append(live.fixtureObservationSql(0),'fixtureObservation');
 append(`select jsonb_build_object('physicalDatabase',current_database(),'databaseOid',(select oid::bigint from pg_catalog.pg_database where datname=current_database()),
  'backendPid',pg_backend_pid(),'xid',pg_current_xact_id()::text,'marker',${literal(marker)},'markerPresent',to_regclass(${literal('private.'+marker)}) is not null,
  'markerRows',(select count(*) from private.${marker}),'holderPid',${identity.backendPid},'holderGranted',(${heldSql(identity)}),'noRoleSwitch',current_user=session_user,
  'transactionStartedAt',transaction_timestamp(),'statementStartedAt',statement_timestamp(),'observedAt',clock_timestamp());`,'completion');
 const text=chunks.join('\n');requireThat(text.split(programme.sql).length===2,'PURE_PROGRAMME_EMBED_ONCE_REQUIRED');
 const verification="select pg_catalog.set_config('search_path','pg_catalog, public',true);\n"+rehearsal.identityProbeSql(marker)+'\n'+holderProofSql(identity);
 const input={database:'agency_test',label:'HR_DAILY_HEALTH_PURE_'+identity.turnId.slice(8).toUpperCase(),sql:text,verification_sql:verification,
  continue_on_error:false,max_rows_per_statement:1,statement_timeout_ms:30000,lock_timeout_ms:2000};
 const relations=live.backgroundRelations.map(name=>({schema:name.split('.')[0],name:name.split('.')[1],kind:'r'}));
 const canonicalSql=rehearsal.restorationSnapshotSql({identity,marker,relations,
  exportContractSql:fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8'),observeLiveBusiness:true});
 const receipt={version:'DAILY_HEALTH_PURE_ROLLBACK_INPUT_V1',scope:SCOPE,route:ROUTE,holderIdentity:identity,marker,
  sqlSha256:sha(text),verificationSqlSha256:sha(verification),programmeSha256:programme.sqlSha256,programmeBytes:8029,
  programmeStatements:sql.scanManagedSql(programme.sql).statements.length,programmeStart,
  statements:sql.scanManagedSql(text).statements.length,indices,canonicalSqlSha256:sha(canonicalSql),
  responses:responseExpectations(text,sql.scanManagedSql),verificationResponses:responseExpectations(verification,sql.scanManagedSql),
  businessFixtureRows:0,markerControlRelationCreated:true,publicComposer:'NOT_RUN',bankingBody:'NOT_INCLUDED',native:'NOT_RUN'};
 return {input,receipt};
}
