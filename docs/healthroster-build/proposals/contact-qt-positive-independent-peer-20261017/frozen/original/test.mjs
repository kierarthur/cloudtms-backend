// All protocol/native-shaped data in this file is SYNTHETIC boundary test data.
// It is never persisted as native evidence and never opens a connection.
import test from'node:test';import assert from'node:assert/strict';import fs from'node:fs';import path from'node:path';import {pathToFileURL,fileURLToPath}from'node:url';
import {buildProgram,CASES}from'./build-program.mjs';import {source,ROOT,sha,SCOPE}from'./source.mjs';import{buildInput}from'./input-builder.mjs';
import {readStage,bindSnapshots,statementResponses}from'./result-validator.mjs';import {createFixedMetadataReader,BoundedMemoryCapture,LIMITS}from'./fixed-metadata-reader.mjs';
import {holdCase}from'./hold-case.mjs';import {BUDGET,assertNoMaps}from'./control.mjs';import{rootCallTiming}from'./timing.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),plans=JSON.parse(fs.readFileSync(here+'/PROGRAMMES.json')),
 pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json')),identity={physicalDatabase:'cloudtms_test_clone',databaseOid:17,backendPid:101,turnId:'hr_turn_'+'1'.repeat(32),key:[20261013,1]},
 now='2026-10-17T10:00:00.000Z';
const built={};for(const caseId of CASES)built[caseId]=await buildInput({root:ROOT,caseId,identity});
function synthetic(caseId){
 const receipt=structuredClone(built[caseId].receipt),at=(key)=>receipt.indices[key]-1;
 const responses=receipt.responses.map(e=>({statement:e.statement,command:e.command,rowCount:e.kind==='ONE_ROW'?1:null,fields:e.kind==='ONE_ROW'?['value']:[],rows:e.kind==='ONE_ROW'?[{value:{}}]:[],rowsTruncated:false}));
 const put=(key,value)=>responses[at(key)].rows=[{value}];
 const clock=phase=>({phase,physicalDatabase:'cloudtms_test_clone',databaseOid:17,backendPid:102,holderPid:101,holderGranted:true,readOnly:false,transactionStartedAt:now,statementStartedAt:now,observedAt:now});
 put('guardBefore',clock('B_GUARD_BEFORE'));put('guardAfter',clock('B_GUARD_AFTER'));
 const transaction={database:'cloudtms_test_clone',backendPid:102,xid:'1001',epochZero:true};
 put('transaction','HR_REHEARSAL_TRANSACTION_JSON '+JSON.stringify(transaction));
 put('fixtureObservation','HR_FIXTURE_OBSERVATION_JSON '+JSON.stringify({...transaction,unit:0,rows:[]}));
 put('programmeResult',plans[caseId].expected);put('completion',{physicalDatabase:'cloudtms_test_clone',databaseOid:17,backendPid:102,xid:'1001',marker:receipt.marker,markerPresent:true,markerRows:0,holderPid:101,holderGranted:true,noRoleSwitch:true,observedAt:now});
 const fresh=receipt.verificationResponses.map(e=>({statement:e.statement,command:e.command,rowCount:1,fields:['value'],rows:[{value:{}}],rowsTruncated:false}));
 fresh[0].rows=[{value:'pg_catalog, public'}];fresh[1].rows=[{value:{physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',applyingCount:0,markerAbsent:true}}];
 fresh[2].rows=[{value:{physicalDatabase:'cloudtms_test_clone',databaseOid:17,backendPid:102,holderPid:101,holderGranted:true,readOnly:true,noRoleSwitch:true,transactionStartedAt:now,statementStartedAt:now,observedAt:now}}];
 fresh[3].rows=[{value:{userFixtureRowsRemaining:0,readOnly:true,backendPid:102,...caseId==='QT_ADMIN_CONTROLS_V1'?{qtObjectsAbsent:true}:{}}}];
 const result={database:'cloudtms_test_clone',databaseTarget:'agency_test',label:receipt.label,sqlSha256:receipt.sqlSha256,statementCount:receipt.statements,accepted:true,rolledBack:true,statements:responses,rollbackVerification:{connectedFresh:true,database:'cloudtms_test_clone',statements:fresh}};
 return {receipt,result};
}
function packets(receipt){
 const make=(phase,readerPid)=>({childClosed:true,peer:{database:'cloudtms_test_clone',databaseOid:17,holderPid:101,readerPid,readOnly:true,isolation:'repeatable read',noRoleSwitch:true},
  provenance:{phase,scope:SCOPE,canonicalSqlSha256:receipt.canonicalSqlSha256,observeLiveBusiness:true,...phase==='AFTER'?{fixtureSource:'UNCHANGED_CANONICAL_FRESH_FIXTURE_PROOF_C_AFTER_THIRD_FRAME'}:{}},
  snapshot:{businessRelations:pins.relationNames.map(relation=>({relation,count:0,hash:'0'.repeat(64)})),marker:receipt.marker,markerAbsent:true},
  ...phase==='AFTER'?{fixtureProof:{backendPid:readerPid,connectedFresh:true}}:{}});
 return {beforePacket:make('BEFORE',103),afterPacket:make('AFTER',104)};
}
test('two exact complete programmes render within unchanged caps',async()=>{
 for(const caseId of CASES){const p=await buildProgram({root:ROOT,caseId});assert.equal(p.sqlSha256,plans[caseId].sqlSha256);assert.equal(p.statements,caseId==='CONTACT_CONSTRUCTORS_V1'?21:51);assert.equal(built[caseId].receipt.statements,caseId==='CONTACT_CONSTRUCTORS_V1'?34:64);assert.ok(Buffer.byteLength(built[caseId].input.sql)<8000000);assert.equal(built[caseId].input.max_rows_per_statement,1);}
 assert.equal(BUDGET.workMilliseconds,120000);assert.equal(BUDGET.wholeMilliseconds,180000);assert.equal(LIMITS.stdoutBytes,64*1024*1024);
});
test('unprovided case, fake Contact lock alias, wrong physical root refuse before connection',async()=>{
 for(const caseId of ['contact20-lock1','ARBITRARY_SQL'])await assert.rejects(buildProgram({root:ROOT,caseId}),/EXACT_CASE/);
 await assert.rejects(source(path.dirname(ROOT),CASES[0]),/PHYSICAL_ROOT/);
 await assert.rejects(holdCase({root:ROOT,out:'unused',caseId:CASES[0]}),/ROOT_AUTHORIZATION/);
});
test('full constructor bodies and complete old assertions are retained',async()=>{
 const{sql}=await source(ROOT,CASES[0]);for(const [copy,filename]of[['contact20.sql','16102026_1205_weekly_contact_20_automatic_followup_tranche.sql'],['contact30.sql','16102026_1205_weekly_contact_30_submission_cohort_identity.sql']]){
  const raw=fs.readFileSync(here+'/reference/'+copy,'utf8'),body=sql.exactAtomicBody(raw);assert.equal(plans[CASES[0]].sql.split(body).length,2);assert.equal(sha(raw),plans[CASES[0]].transfers.find(t=>t.file.endsWith(filename)).completeOriginalSha256);}
 const s=plans[CASES[0]].sql;assert.ok(s.indexOf('lock table public.weekly_manager_recipient_generations')<s.indexOf('CONTACT_SOURCE_AUTOMATIC_TRANCHE_PRIOR_SCHEMA_MISMATCH'));assert.ok(s.includes('CONTACT_SUBMISSION_RETAINED_IDENTITY_UNRESOLVED'));
});
test('QT actual return tag and command/domain audit checks bind current owner',()=>{
 const actual=fs.readFileSync(here+'/reference/qt-authority.sql','utf8'),trial=fs.readFileSync(here+'/qt-assertions.sql','utf8');
 assert.ok(actual.includes("'version','PUSH_TIMING_STATE_V1'"));assert.ok(trial.includes("result->>'version'<>'PUSH_TIMING_STATE_V1'"));
 for(const token of ['QT_EXACT_TWO_COMMAND_AUDIT_MISMATCH','QT_REFUSAL_OR_REPLAY_CHANGED_JOURNAL','QT_EXACT_FINAL_POLICY_MISMATCH','QT_GOLDEN_NATIVE_INVARIANT_NOT_PRESERVED','QT_UNRELATED_SETTINGS_OR_GLOBAL_AUDIT_CHANGED'])assert.ok(trial.includes(token));
 assert.equal(plans.QT_ADMIN_CONTROLS_V1.expected.ordinaryPayInactiveDenials,10);assert.equal(plans.QT_ADMIN_CONTROLS_V1.expected.postWaitRace,'NOT_RUN');assert.equal(plans.QT_ADMIN_CONTROLS_V1.expected.directRoleApi,'NOT_RUN');
});
test('unchanged native actor preflight precedes INSERT; no role switching',()=>{
 const p=plans.QT_ADMIN_CONTROLS_V1;assert.ok(p.sql.indexOf('DAILY_ACTOR_NATIVE_STORE_INVARIANT_OR_TRIGGER_CHANGED')<p.sql.indexOf('insert into public.tms_users'));assert.ok(p.sql.includes('d5d7808078fd3fa302415a450a4e8255e6a2519f0a5f29b89416583637499b63'));
 assert.doesNotMatch(p.sql,/\bset\s+(?:local\s+)?role\s+(?:service_role|anon|authenticated)/i);
});
test('fixed C uses original canonical query and all twelve complete relations without connecting',async()=>{
 const turn={identity,signal:new AbortController().signal,assertHeld:async()=>{throw Error('NO_CONNECTION_ALLOWED');},runOwnedChild:()=>{throw Error('NO_CONNECTION_ALLOWED');}};
 for(const caseId of CASES){const reader=await createFixedMetadataReader({root:ROOT,turn,caseId}),view=reader.inspectSource();assert.equal(sha(view.canonicalSql),built[caseId].receipt.canonicalSqlSha256);assert.equal(view.relations.length,12);assert.match(view.input,/begin isolation level repeatable read read only/);for(const name of pins.relationNames)assert.ok(view.canonicalSql.includes(name));}
});
test('reader parent full raw inverse and original control/timing exact bytes',()=>{
 const reverse=JSON.parse(fs.readFileSync(here+'/READER_PARENT_REVERSE.json'));let value=fs.readFileSync(here+'/fixed-metadata-reader.mjs','utf8');for(const h of [...reverse.hunks].reverse())value=value.replace(h.after,()=>h.before);assert.equal(value,reverse.before);
 for(const n of ['control.mjs','timing.mjs'])assert.deepEqual(fs.readFileSync(here+'/'+n),fs.readFileSync(here+'/reference/adapter/'+n));
});
test('all four original SQL parents fully reverse including exact owner and framing spans',()=>{
 const parents=JSON.parse(fs.readFileSync(here+'/PROGRAMME_PARENT_REVERSES.json'));assert.equal(parents.inverses.length,4);
 for(const row of parents.inverses){let value=row.selectedExecutable;for(const h of [...row.ownerEdits].reverse())value=value.slice(0,h.offset)+h.before+value.slice(h.offset+h.after.length);for(const h of [...row.originalPackagingSpans].reverse())value=value.slice(0,h.bodyOffset)+h.raw+value.slice(h.bodyOffset);assert.equal(value,row.beforeRaw);assert.equal(sha(value),row.wholeOriginalSha256);}
});
test('native metadata guard refuses unreviewed policy/owner/rule/inheritance/DDL hooks before business reads',()=>{
 const text=fs.readFileSync(here+'/native-boundary.mjs','utf8');for(const t of ['pg_event_trigger','pg_inherits','pg_rewrite',"policy_row.polcmd='*'",'policy_row.polpermissive',"pg_catalog.pg_get_expr(policy_row.polqual,policy_row.polrelid)='true'",'pg_catalog.pg_get_userbyid(relation_row.relowner)<>current_user'])assert.ok(text.includes(t));
 for(const caseId of CASES)assert.ok(built[caseId].input.sql.indexOf('pg_event_trigger')<built[caseId].input.sql.indexOf('create table private.hr_install2_'));
});
test('synthetic response boundaries accept exact two shapes, not native qualification',()=>{
 for(const caseId of CASES){const{receipt,result}=synthetic(caseId),stage=readStage(result,receipt);assert.equal(stage.transaction.backendPid,102);assert.equal(stage.freshPeerPid,102);assert.equal(stage.nativeQualification,undefined);}
});
test('synthetic SET/DDL null is per-index; SELECT null, wrong tag and extra fields refuse',()=>{
 const{receipt,result}=synthetic(CASES[1]);assert.equal(statementResponses(result.statements,receipt.responses),true);
 for(const alter of [r=>r.statements[receipt.indices.programmeResult-1].rowCount=null,r=>r.statements[0].command='SELECT',r=>r.statements[0].invented=true]){const x=structuredClone(result);alter(x);assert.throws(()=>readStage(x,receipt),/SEQUENCE/);}
});
test('synthetic wrong programme counters/accepted/rollback/freshness refuse',()=>{
 const{receipt,result}=synthetic(CASES[1]);for(const alter of [r=>r.accepted=false,r=>r.rolledBack=false,r=>r.rollbackVerification.connectedFresh=false,r=>r.statements[receipt.indices.programmeResult-1].rows[0].value.commandAuditRows=3]){const x=structuredClone(result);alter(x);assert.throws(()=>readStage(x,receipt));}
});
test('pooled MCP PID equal B is permitted only with independent C-after and full scope',()=>{
 const{receipt,result}=synthetic(CASES[1]),stage=readStage(result,receipt),p=packets(receipt);assert.equal(stage.freshPeerPid,stage.transaction.backendPid);assert.doesNotThrow(()=>bindSnapshots({receipt,stage,...p}));
 const bad=structuredClone(p);bad.afterPacket.peer.readerPid=102;bad.afterPacket.fixtureProof.backendPid=102;assert.throws(()=>bindSnapshots({receipt,stage,...bad}),/INDEPENDENT_C_AFTER/);
 const narrow=structuredClone(p);narrow.beforePacket.snapshot.businessRelations.pop();assert.throws(()=>bindSnapshots({receipt,stage,...narrow}),/COMPLETE_SCOPE/);
});
test('original WeakMap refuses invented proof or stale copied qualification',async()=>{
 const {live}=await source(ROOT,CASES[0]);assert.throws(()=>live.assertQualifiedLiveRestoration({},{},{status:'PASS'}),/UNBRANDED_OR_STALE/);
 const text=fs.readFileSync(ROOT+'/scripts/healthroster-live-test-restoration.mjs','utf8');assert.ok(text.includes('const qualifiedFrames=new WeakMap()'));
});
test('bounded capture and persistence refuse oversized/native raw maps',()=>{
 const c=new BoundedMemoryCapture();c.stdout(Buffer.alloc(LIMITS.stdoutBytes+1));assert.throws(()=>c.finish(),/STDOUT_BOUND/);assert.throws(()=>assertNoMaps({backgroundRows:{}}),/MAP_PERSISTENCE/);
});
test('root timing is actual bounded metadata only; no fabricated latency or SQL result',()=>{
 const value={version:'ROOT_OBSERVED_MCP_CALL_TIMING_V1',route:built[CASES[0]].receipt.route,sqlSha256:'a'.repeat(64),callStartedAt:now,callReturnedAt:now};
 assert.equal(rootCallTiming(value,{sqlSha256:'a'.repeat(64),readyAt:now,resultReadAt:now}).rootObservedCallMilliseconds,0);assert.throws(()=>rootCallTiming({...value,accepted:true},{sqlSha256:'a'.repeat(64),readyAt:now,resultReadAt:now}),/TIMING_BINDING/);
});
