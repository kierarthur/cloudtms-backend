// Source-only builder. B is ONLY the server-owned Kier MCP rollback route.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {sha,assertSourcePins,buildPlan} from '../v6-contact-six-lock-contention-20261016/prepare.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
export const previous=path.join(here,'../v6-contact-six-lock-contention-20261016');
export const requireThat=(ok,code)=>{if(!ok)throw Error(code);};
export const literal=v=>`'${String(v).replaceAll("'","''")}'`;
export const modeName=mode=>({'share':'ShareLock','access exclusive':'AccessExclusiveLock','access share':'AccessShareLock','row exclusive':'RowExclusiveLock'})[mode];
const writerPattern=/^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(/;
// Exact owner-reviewed storage migration; every other source byte stays pinned.
export function reviewedPathRepair(root){
 const file='scripts/healthroster-rehearsal-turn.mjs',bytes=fs.readFileSync(path.join(root,file)),text=bytes.toString('utf8');
 const before="const workspace='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub';",after="const workspace='D:/Development/GitHub';";
 requireThat(text.split(after).length===2&&!text.includes(before),'EXACT_REVIEWED_D_WORKSPACE_REQUIRED');
 const reversed=text.replace(after,before),currentSha256=sha(bytes),currentLfSha256=sha(text.replaceAll('\r\n','\n'));
 requireThat(currentSha256==='2a4078b744545a4b4c15a9842947e222ffae75dcadf9b087f49b3c8a30cd36aa'
  &&sha(reversed)==='b7e16cdb12e8b5c6d11894e53296e4d20aeeaceb8dd968ed32936884f3ab3610','EXACT_PATH_ONLY_SOURCE_REVERSE_REQUIRED');
 const plan=JSON.parse(fs.readFileSync(path.join(previous,'PLAN.json'),'utf8'));
 requireThat(plan.inputs.find(r=>r.file===file)?.sha256===sha(reversed.replaceAll('\r\n','\n')),'HISTORICAL_PATH_SOURCE_PIN_REQUIRED');
 return {file,currentSha256,currentLfSha256,beforeSha256:sha(reversed),wholeSourceReverse:true};
}
export async function source(root){
 const plan=JSON.parse(fs.readFileSync(path.join(previous,'PLAN.json'),'utf8'));
 const repair=reviewedPathRepair(root);
 assertSourcePins({...plan,inputs:plan.inputs.map(r=>r.file===repair.file?{...r,sha256:repair.currentLfSha256}:r)},root);
 const reconstructed=await buildPlan(root);requireThat(JSON.stringify(plan.cases)===JSON.stringify(reconstructed.cases),'CANONICAL_LOCK_SOURCE_CHANGED');
 const expected=JSON.parse(fs.readFileSync(path.join(here,'../v6-contact-six-lock-mcp-successor-20261016/AUTHORITY.json'),'utf8'));
 requireThat(sha(fs.readFileSync(path.join(previous,'PLAN.json')))===expected.historicalPlanSha256,'HISTORICAL_FROZEN_PLAN_CHANGED');
 for(const row of expected.helperInputs)requireThat(sha(fs.readFileSync(path.join(root,row.file)))===(row.file===repair.file?repair.currentSha256:row.sha256),'MCP_SUCCESSOR_HELPER_SOURCE_CHANGED');
 const writers=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8')).routines.filter(row=>writerPattern.test(row.identity));
 requireThat(JSON.stringify(sortWriters(writers))===JSON.stringify(expected.expectedWriters),'BACKGROUND_WRITER_SOURCE_CHANGED');
 const load=file=>import(pathToFileURL(path.join(root,file)).href);
 const rehearsal=await load('scripts/healthroster-install1-full-rehearsal.mjs'),live=await load('scripts/healthroster-live-test-restoration.mjs'),sql=await load('scripts/cloudtms-managed-release-sql.mjs');
 return {plan,expected,rehearsal,live,sql};
}
export const sortWriters=rows=>[...rows].sort((a,b)=>(a.schema+'|'+a.identity).localeCompare(b.schema+'|'+b.identity));
export function holderProofSql(row,names){return `select jsonb_build_object('backendPid',pg_backend_pid(),'databaseOid',(select oid::bigint from pg_database where datname=current_database()),
 'physicalDatabase',current_database(),'readOnly',current_setting('transaction_read_only')::boolean,
 'targetGranted',exists(select 1 from pg_locks where pid=pg_backend_pid() and locktype='relation' and relation=${literal(row.targetTable)}::regclass and mode=${literal(modeName(row.holderMode))} and granted),
 'onlyTargetWitness',(select count(*)=1 from pg_locks where pid=pg_backend_pid() and locktype='relation' and relation in (${names.map(n=>literal(n)+'::regclass').join(',')})));`;}
export function holderGuard(row,identity){return `exists(select 1 from pg_catalog.pg_locks where locktype='advisory' and database=${identity.databaseOid} and pid=${identity.backendPid} and classid=20261013 and objid=1 and objsubid=2 and mode='ExclusiveLock' and granted)
 and exists(select 1 from pg_catalog.pg_locks where locktype='relation' and database=${identity.databaseOid} and pid=${identity.backendPid} and relation=${literal(row.targetTable)}::regclass and mode=${literal(modeName(row.holderMode))} and granted)`;}
export function proofSql(row,identity,count){return `select jsonb_build_object('holderPid',${identity.backendPid},'writerPid',pg_backend_pid(),'holderGranted',(${holderGuard(row,identity)}),'precedingCount',${count},'precedingGranted',${count?row.preceding.slice(0,count).map(p=>`exists(select 1 from pg_locks where pid=pg_backend_pid() and locktype='relation' and relation=${literal(p.table)}::regclass and mode=${literal(modeName(p.mode))} and granted)`).join(' and '):'true'});`;}
export async function buildInput({root,caseId,identity}){
 const {plan,rehearsal,live,sql}=await source(root);
 const turn=await import(pathToFileURL(path.join(root,'scripts/healthroster-turn-control.mjs')).href);turn.assertTurnIdentity(identity);
 const row=plan.cases.find(c=>c.id===caseId);requireThat(row,'EXACT_CASE_REQUIRED');
 const names=[...plan.relationNames,...live.backgroundRelations],relations=names.map(name=>({schema:name.split('.')[0],name:name.split('.')[1],kind:'r'}));
 const snapshot=rehearsal.restorationSnapshotSql({identity,marker:plan.marker,relations,exportContractSql:fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8'),observeLiveBusiness:true});
 const chunks=[],indices={proofs:[]};
 const append=(text,key)=>{const start=sql.scanManagedSql(chunks.join('\n')).statements.length+1;chunks.push(text);if(key)indices[key]=start+sql.scanManagedSql(text).statements.length-1;return start;};
 append("set local jit=off; set local pg_show_plans.is_enabled=off; set local work_mem='4MB'; set local maintenance_work_mem='32MB';");
 append(`do $contact_guard$ begin if current_database()<>'cloudtms_test_clone' or current_setting('transaction_isolation')<>'read committed'
  or not (${holderGuard(row,identity)}) or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null)
  or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='LEGACY' and owner_epoch=1)
  or exists(select 1 from private.cloudtms_database_releases where status='APPLYING')
  or to_regclass(${literal('private.'+plan.marker)}) is not null
  then raise exception 'CONTACT_MCP_TURN_HOLDER_OR_IDENTITY_LOST' using errcode='55000'; end if; end; $contact_guard$;`);
 append(snapshot,'beforeSnapshot');append(live.transactionIdentitySql(),'transaction');append(live.fixtureObservationSql(0),'fixtureObservation');
 for(const [index,command]of row.commands.slice(2,-1).entries()){append(command);indices.proofs.push(append(proofSql(row,identity,index+1)));}
 append(proofSql(row,identity,row.preceding.length),'targetReady');append(row.commands.at(-1),'expectedFailedStatement');
 const text=chunks.join('\n'),query=snapshot.slice(snapshot.indexOf('with identity_proof'));
 const verification="select pg_catalog.set_config('search_path','pg_catalog, public',true);\n"+query+'\n'+proofSql(row,identity,0);
 const input={database:'agency_test',label:'HR_CONTACT_MCP_'+caseId.replaceAll('-','_').toUpperCase(),sql:text,verification_sql:verification,
  continue_on_error:false,max_rows_per_statement:1,statement_timeout_ms:30000,lock_timeout_ms:2000};
 const receipt={version:'CONTACT_SIX_LOCK_KIER_MCP_INPUT_V1',caseId,canonicalPacketSha256:row.canonicalPacketSha256,
  exactLockCommands:row.commands.slice(2),holderIdentity:identity,marker:plan.marker,sqlSha256:sha(text),verificationSqlSha256:sha(verification),
  statements:sql.scanManagedSql(text).statements.length,indices,targetTable:row.targetTable,targetMode:row.targetMode,holderMode:row.holderMode,
  expectedError:'55P03',administrativeEnvelope:'SERVER_OWNS_TRANSACTION_NO_FAKE_MANAGED_WRITER_CONTEXT',originalAssertionsBodyLedgerSent:false,
  observeLiveBusiness:true,backgroundJobsPaused:false,fixtureDmlDdl:0,route:'mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback',native:'NOT_RUN'};
 requireThat(text.trimEnd().endsWith(row.commands.at(-1)),'TARGET_MUST_BE_FINAL_STATEMENT');return {input,receipt};
}
export function saveInput(out,{input,receipt}){fs.writeFileSync(path.join(out,'INPUT.json'),JSON.stringify(input,null,2)+'\n');fs.writeFileSync(path.join(out,'INPUT_RECEIPT.json'),JSON.stringify(receipt,null,2)+'\n');}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const [root,out,caseId]=process.argv.slice(2),identity=JSON.parse(fs.readFileSync(path.join(out,'TURN_IDENTITY.json'),'utf8'));
 saveInput(out,await buildInput({root,caseId,identity}));console.log(JSON.stringify({status:'MCP_INPUT_BUILT_NOT_EXECUTED',caseId}));
}
