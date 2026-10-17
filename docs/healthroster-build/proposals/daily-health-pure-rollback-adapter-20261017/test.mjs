// All native-shaped objects below are SYNTHETIC offline fixtures, never receipts.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {EventEmitter} from 'node:events';
import {execFileSync} from 'node:child_process';
import {createRequire} from 'node:module';
import {source,ROOT,here,sha,PROGRAMME_SHA,SCOPE,ROUTE} from './source.mjs';
import {buildInput} from './input-builder.mjs';
import {createFixedMetadataReader,BoundedMemoryCapture,LIMITS} from './fixed-metadata-reader.mjs';
import {readStage,bindSnapshots,validateRestoration,statementResponses,nativeClockFrames} from './result-validator.mjs';
import {rootCallTiming,WORK_DEADLINE_CODE} from './timing.mjs';
import {FiniteControls,assertElapsed,assertNoMaps,BUDGET} from './control.mjs';
import {holdPure} from './hold-pure.mjs';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:100,turnId:'hr_turn_'+'a'.repeat(32),key:[20261013,1]};
const src=await source(ROOT),built=await buildInput({root:ROOT,identity});
const row=(value,prefix='')=>({command:'SELECT',rowCount:1,fields:['result'],rowsTruncated:false,rows:[{result:prefix?prefix+JSON.stringify(value):value}]});
const empty=command=>({command,rowCount:null,fields:[],rowsTruncated:false,rows:[]});
const clock=observedAt=>({transactionStartedAt:'2026-10-17T10:00:00.000Z',statementStartedAt:observedAt,observedAt});
const transaction={database:'cloudtms_test_clone',backendPid:200,xid:'50',epochZero:true};
const observation={...transaction,unit:0,rows:[]};
function nativeFixture(){
 const result={database:'cloudtms_test_clone',databaseTarget:'agency_test',label:built.input.label,sqlSha256:built.receipt.sqlSha256,
  statementCount:built.receipt.statements,accepted:true,rolledBack:true,statements:built.receipt.responses.map(e=>({statement:e.statement,...e.kind==='ONE_ROW'?row(null):empty(e.command)}))};
 const set=(key,value)=>Object.assign(result.statements[built.receipt.indices[key]-1],value);
 set('transaction',row(transaction,'HR_REHEARSAL_TRANSACTION_JSON '));set('fixtureObservation',row(observation,'HR_FIXTURE_OBSERVATION_JSON '));
 set('programmeResult',row('DAILY_HEALTH_NATIVE_UNICODE_REASON_ROUNDTRIP_ASSERTED'));
 for(const [key,phase,time]of [['guardBefore','B_GUARD_BEFORE','2026-10-17T10:00:00.001Z'],['guardAfter','B_GUARD_AFTER','2026-10-17T10:00:00.002Z']])
  set(key,row({phase,physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:200,holderPid:100,holderGranted:true,readOnly:false,...clock(time)}));
 set('completion',row({physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:200,xid:'50',marker:built.receipt.marker,
  markerPresent:true,markerRows:0,holderPid:100,holderGranted:true,noRoleSwitch:true,...clock('2026-10-17T10:00:00.003Z')}));
 result.rollbackVerification={connectedFresh:true,database:'cloudtms_test_clone',statements:[row('pg_catalog, public'),row({physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',applyingCount:0,markerAbsent:true}),
  row({physicalDatabase:'cloudtms_test_clone',databaseOid:123,holderPid:100,backendPid:300,holderGranted:true,readOnly:true,noRoleSwitch:true,...clock('2026-10-17T10:00:00.004Z')})].map((s,i)=>({statement:i+1,...s}))};
 return result;
}
function snapshots(){
 const relationHash=sha(''),relations=src.live.backgroundRelations.map(relation=>({relation,count:0,hash:relationHash}));
 const base={physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',applyingCount:0,
  markerAbsent:true,marker:built.receipt.marker,ledgerFingerprint:sha('ledger'),catalogueFingerprint:sha('catalogue'),relationCensus:sha('census'),
  businessCountsFingerprint:sha('counts'),businessRowsFingerprint:sha('rows'),businessRelations:relations,otherActivityCount:1,
  backgroundRows:Object.fromEntries(src.live.backgroundRelations.map(r=>[r,[]])),backgroundWriterRoutines:src.expectedWriters,
  backgroundSnapshotEpochZero:true,backgroundSnapshotXmax:'100',backgroundSnapshotXip:[],
  backgroundSnapshotStartedAt:'2026-10-17T10:00:00Z',backgroundObservedAt:'2026-10-17T10:00:01Z'};
 const packet=(phase,pid)=>({childClosed:true,provenance:{phase,scope:SCOPE,canonicalSqlSha256:built.receipt.canonicalSqlSha256,observeLiveBusiness:true,
   fixtureSource:'UNCHANGED_CANONICAL_FRESH_FIXTURE_PROOF_C_AFTER_THIRD_FRAME'},transport:{frameCount:phase==='AFTER'?3:2},
  peer:{database:'cloudtms_test_clone',databaseOid:123,holderPid:100,readOnly:true,isolation:'repeatable read',noRoleSwitch:true,readerPid:pid},
  snapshot:structuredClone(base)});
 const beforePacket=packet('BEFORE',400),afterPacket=packet('AFTER',500);afterPacket.snapshot.connectedFresh=true;
 afterPacket.fixtureProof={database:'cloudtms_test_clone',backendPid:500,connectedFresh:true,rehearsalBackendPid:200,rehearsalXid:'50',parentStatus:'aborted',
  relations:src.live.backgroundRelations.map(relation=>({relation,observed_fixture_keys:0,remaining:0}))};
 return {beforePacket,afterPacket};
}
test('committed root source and exact original programme, no business relation closure',()=>{
 assert.equal(src.programme.sqlBytes,8029);assert.equal(src.programme.sqlSha256,PROGRAMME_SHA);assert.equal(src.programme.definitionCount,5);
 assert.equal(src.programme.positiveUnicodeVectors,6);assert.equal(src.programme.malformedStoredReasonRefusals,8);
 const declarations=src.programme.sql.slice(0,src.programme.sql.indexOf('-- REQUIRED_NOT_RUN.'));
 assert.doesNotMatch(declarations,/\b(?:from|join)\s+(?:private|public)\.|\b(?:insert|update|delete|merge|execute|nextval|setval)\b/i);
 assert.equal((declarations.match(/create or replace function /g)||[]).length,5);
 assert.ok(src.pins.rootInputs.length>=14);assert.equal(src.expectedWriters.length,4);
 const names=new Set(src.pins.rootInputs.map(r=>r.file));
 for(const f of src.pins.rootInputs.filter(r=>r.file.endsWith('.mjs'))){
  const text=fs.readFileSync(path.join(ROOT,f.file),'utf8');
  for(const m of text.matchAll(/(?:\bfrom\s*|\bimport\s*)['"](\.[^'"]+\.mjs)['"]/g))assert.ok(names.has(path.posix.normalize(path.posix.join(path.posix.dirname(f.file),m[1]))));
 }
});
test('input is exact programme plus finite marker/control envelope, genuine original observation',()=>{
 assert.equal(built.receipt.route,ROUTE);assert.equal(built.receipt.programmeBytes,8029);assert.equal(built.input.sql.split(src.programme.sql).length,2);
 assert.equal(built.receipt.programmeStatements,7);assert.match(built.input.sql,/create table private\.hr_install2_a{32}\(proof boolean not null\);/);
 assert.ok(built.input.sql.includes(src.live.fixtureObservationSql(0)));
 assert.ok(built.input.sql.indexOf(src.live.fixtureObservationSql(0))>built.input.sql.indexOf(src.programme.sql));
 assert.doesNotMatch(built.input.sql,/\b(?:commit|rollback)\s*;|\block\s+table\b|Contact|CONTACT/);
 assert.equal(built.input.database,'agency_test');assert.equal(built.input.continue_on_error,false);assert.equal(built.input.max_rows_per_statement,1);
});
test('reader retains whole canonical exporter and complete BOTH background scope, with fresh proof third frame',async()=>{
 const turn={identity,signal:new AbortController().signal,assertHeld:async()=>{},runOwnedChild:async()=>{throw Error('OFFLINE_ONLY');}};
 const reader=await createFixedMetadataReader({root:ROOT,turn});
 const proof=reader.inspectSource({transaction,observations:[observation],before:snapshots().beforePacket.snapshot});
 assert.equal(proof.relations.length,2);assert.deepEqual(proof.relations.map(r=>r.schema+'.'+r.name),src.live.backgroundRelations);
 const original=src.rehearsal.restorationSnapshotSql({identity,marker:built.receipt.marker,relations:proof.relations,
  exportContractSql:fs.readFileSync(path.join(ROOT,'supabase/release/export_contract.sql'),'utf8'),observeLiveBusiness:true});
 assert.equal(proof.canonicalSql,original);assert.equal(sha(original),built.receipt.canonicalSqlSha256);
 assert.equal(proof.after.fixtureSql,src.live.freshFixtureProofSql(transaction,[observation],snapshots().beforePacket.snapshot));
 assert.match(proof.after.fixtureSql,/pg_xact_status\('50'::xid8\)/);assert.match(proof.input,/repeatable read read only/);
 assert.ok(proof.after.input.indexOf(proof.after.fixtureSql)>proof.after.input.indexOf(original));
 assert.equal(proof.after.input.split(original).length,2);
});
test('finite source transfer reverses entire exact Report parents using literal callbacks',()=>{
 const transfers=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_TRANSFERS.json'),'utf8'));
 for(const r of transfers){let text=fs.readFileSync(path.join(here,r.file),'utf8');assert.equal(sha(text),r.afterSha256);
  for(const h of [...r.hunks].reverse()){assert.equal(text.split(h.after).length,2);text=text.replace(h.after,()=>h.before);}
  const bytes=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',ROOT,'show',r.ownerCommit+':'+r.ownerFile]);
  assert.ok(Buffer.from(text).equals(bytes));assert.equal(sha(bytes),r.beforeSha256);
 }
 const before="select '$' as endanchor, '$&' as literal, '$`' as token;";
 assert.equal('replacement'.replace('replacement',()=>before),before);
});
test('native-shaped success requires real B marker, original assertion and genuine observations',()=>{
 assert.equal(readStage(nativeFixture(),built.receipt).transaction.backendPid,200);
 for(const mutate of [r=>r.rolledBack=false,r=>r.accepted=false,r=>r.sqlSha256=sha('wrong'),r=>r.database='LIVE',r=>r.statements.pop(),
  r=>r.statements[0].rowsTruncated=true,r=>r.statements[0].statement=2,r=>r.statements[built.receipt.indices.programmeResult-1].rows=[{result:'PASS'}],
  r=>r.statements[built.receipt.indices.completion-1].rows[0].result.markerPresent=false,
  r=>r.statements[built.receipt.indices.completion-1].rows[0].result.markerRows=1,
  r=>r.statements[built.receipt.indices.completion-1].rows[0].result.xid='51',
  r=>r.rollbackVerification.connectedFresh=false,r=>r.rollbackVerification.statements[2].rows[0].result.readOnly=false,
  r=>r.rollbackVerification.statements[1].rows[0].result.markerAbsent=false]){
  const r=nativeFixture();mutate(r);assert.throws(()=>readStage(r,built.receipt));
 }
});
test('genuine R2 SET null responses pass exact command ABI, but actual guard failure never becomes success',()=>{
 const e=JSON.parse(fs.readFileSync(path.join(here,'GENUINE_R2_COMMAND_EVIDENCE.json'),'utf8'));
 assert.equal(e.accepted,false);assert.equal(e.failure.statement,5);assert.equal(e.nativeProgrammeRan,false);
 assert.ok(statementResponses(e.observedCommandRows,built.receipt.responses.slice(0,4)));
 const failed=nativeFixture();failed.accepted=e.accepted;failed.statements=[...e.observedCommandRows,{statement:5,error:{code:'55000'}}];
 assert.throws(()=>readStage(failed,built.receipt),/PURE_MCP_ROLLBACK_ENVELOPE_REFUSED/);
 for(const mutate of [r=>r[0].command='DO',r=>r[0].rowCount=0,r=>r[0].rows=[{}],r=>r[0].fields=['unexpected'],r=>r[0].rowsTruncated=true]){
  const rows=structuredClone(e.observedCommandRows);mutate(rows);assert.equal(statementResponses(rows,built.receipt.responses.slice(0,4)),false);
 }
});
test('unobserved DO CREATE RESET ABI stays synthetic closed expectation; SELECT never accepts null counts',()=>{
 const result=nativeFixture();assert.ok(statementResponses(result.statements,built.receipt.responses));
 for(const command of ['DO','CREATE','RESET']){
  const index=built.receipt.responses.findIndex(e=>e.command===command);assert.ok(index>=0);
  assert.equal(built.receipt.responses[index].abiQualification,'SOURCE_EXPECTATION_NATIVE_NOT_PROVED');
  for(const mutate of [s=>s.rowCount=0,s=>s.command='SET',s=>s.fields=['unexpected'],s=>s.rows=[{}]]){
   const r=nativeFixture();mutate(r.statements[index]);assert.equal(statementResponses(r.statements,built.receipt.responses),false);
  }
 }
 const selected=nativeFixture();selected.statements[built.receipt.indices.transaction-1].rowCount=null;
 assert.equal(statementResponses(selected.statements,built.receipt.responses),false);
 const wrongFields=nativeFixture();wrongFields.statements[built.receipt.indices.transaction-1].fields=['other'];
 assert.equal(statementResponses(wrongFields.statements,built.receipt.responses),false);
});
test('actual local pg8.23 Result parser derives exact no-row tags from synthetic messages without a connection',()=>{
 const evidence=JSON.parse(fs.readFileSync(path.join(here,'PG_RESULT_PARSER_EVIDENCE.json'),'utf8'));
 assert.equal(evidence.pgVersion,'8.23.0');assert.equal(sha(fs.readFileSync(evidence.physicalSource)),evidence.sourceSha256);
 assert.equal(sha(fs.readFileSync(evidence.physicalPackage)),evidence.packageSha256);
 assert.match(fs.realpathSync.native(evidence.physicalSource),/^D:[\\/]Development[\\/]GitHub[\\/]/);
 const Result=createRequire(import.meta.url)(evidence.physicalSource);
 for(const sample of evidence.samples){const result=new Result();result.addCommandComplete({text:sample.syntheticCommandCompleteTag});
  assert.equal(result.command,sample.command);assert.equal(result.rowCount,sample.rowCount);assert.deepEqual(result.fields,sample.fields);assert.deepEqual(result.rows,sample.rows);
 }
 assert.equal(evidence.deploymentIdentity,'NOT_PROVED_FROM_LOCAL_DEPENDENCY');
});
test('native guard clocks are actual frame observations and require same B identity and ordered timestamps',()=>{
 const r=nativeFixture();assert.equal(nativeClockFrames(r,built.receipt).length,2);
 for(const mutate of [r=>r.statements[built.receipt.indices.guardBefore-1].rows[0].result.observedAt='invalid',
  r=>r.statements[built.receipt.indices.guardAfter-1].rows[0].result.backendPid=999,
  r=>r.statements[built.receipt.indices.guardAfter-1].rows[0].result.holderGranted=false,
  r=>r.statements[built.receipt.indices.guardAfter-1].rows[0].result.observedAt='2026-10-17T09:00:00.000Z']){
  const r=nativeFixture();mutate(r);assert.throws(()=>readStage(r,built.receipt));
 }
});
test('root timing is exact observed envelope, bound to input and local READY/result-read order',()=>{
 const value={version:'ROOT_OBSERVED_MCP_CALL_TIMING_V1',route:ROUTE,sqlSha256:built.receipt.sqlSha256,
  callStartedAt:'2026-10-17T10:00:00.200Z',callReturnedAt:'2026-10-17T10:00:00.300Z'},
 context={sqlSha256:built.receipt.sqlSha256,readyAt:'2026-10-17T10:00:00.100Z',resultReadAt:'2026-10-17T10:00:00.400Z'};
 assert.equal(rootCallTiming(value,context).rootObservedCallMilliseconds,100);
 for(const mutate of [v=>v.sqlSha256=sha('other'),v=>v.route='other',v=>v.callStartedAt='2026-10-17T10:00:00.000Z',
  v=>v.callReturnedAt='2026-10-17T10:00:00.500Z',v=>v.extra=true]){const v=structuredClone(value);mutate(v);assert.throws(()=>rootCallTiming(v,context));}
});
test('distinct C-after bound to original fixture proof; no aliases or omitted relation scope',()=>{
 const stage=readStage(nativeFixture(),built.receipt),p=snapshots();assert.ok(bindSnapshots({receipt:built.receipt,stage,...p}));
 for(const pid of [100,200,300,400]){const q=snapshots();q.afterPacket.peer.readerPid=pid;assert.throws(()=>bindSnapshots({receipt:built.receipt,stage,...q}));}
 const q=snapshots();q.afterPacket.fixtureProof.backendPid=999;assert.throws(()=>bindSnapshots({receipt:built.receipt,stage,...q}));
 const omitted=snapshots();omitted.beforePacket.snapshot.businessRelations.pop();assert.throws(()=>bindSnapshots({receipt:built.receipt,stage,...omitted}));
});
test('fresh MCP pooled B or closed C-before PID qualifies only with independent C-after and original restoration',async()=>{
 for(const pooledPid of [200,400]){
  const result=nativeFixture();result.rollbackVerification.statements[2].rows[0].result.backendPid=pooledPid;
  const stage=readStage(result,built.receipt);assert.equal(stage.freshPeerPid,pooledPid);
  const p=snapshots(),summary=await validateRestoration({root:ROOT,result,receipt:built.receipt,...p});
  assert.equal(summary.readerPids.after,500);assert.equal(summary.restoration.liveRestoration.parentRollback.status,'aborted');
  const originalProof=p.afterPacket.snapshot.liveRestoration;
  assert.throws(()=>src.rehearsal.assertFreshRestoration(p.beforePacket.snapshot,{...p.afterPacket.snapshot,liveRestoration:structuredClone(originalProof)},built.receipt.marker),/REHEARSAL_LIVE_RESTORATION_UNBRANDED_OR_STALE/);
  p.beforePacket.snapshot.backgroundSnapshotXmax='99';
  assert.throws(()=>src.rehearsal.assertFreshRestoration(p.beforePacket.snapshot,p.afterPacket.snapshot,built.receipt.marker),/REHEARSAL_LIVE_RESTORATION_UNBRANDED_OR_STALE/);
  for(const mutate of [q=>{q.afterPacket.peer.readerPid=200;q.afterPacket.fixtureProof.backendPid=200;},
   q=>{q.afterPacket.peer.readerPid=400;q.afterPacket.fixtureProof.backendPid=400;},q=>q.afterPacket.fixtureProof.backendPid=pooledPid,
   q=>q.afterPacket.fixtureProof.parentStatus='committed',q=>q.afterPacket.fixtureProof.rehearsalXid='49',
   q=>q.afterPacket.fixtureProof.relations[0].remaining=1,q=>q.beforePacket.peer.readerPid=200]){
   const q=snapshots();mutate(q);await assert.rejects(()=>validateRestoration({root:ROOT,result,receipt:built.receipt,...q}));
  }
 }
});
test('original qualifier succeeds only on complete synthetic native-shaped proof and mints an uncopyable brand',async()=>{
 const p=snapshots(),summary=await validateRestoration({root:ROOT,result:nativeFixture(),receipt:built.receipt,...p});
 assert.equal(summary.restoration.liveRestoration.status,'LIVE_TEST_RESTORATION_QUALIFIED');assert.equal(summary.restoration.liveRestoration.relationCount,2);
 assert.throws(()=>src.rehearsal.assertFreshRestoration(p.beforePacket.snapshot,{...p.afterPacket.snapshot,liveRestoration:structuredClone(p.afterPacket.snapshot.liveRestoration)},built.receipt.marker),/REHEARSAL_LIVE_RESTORATION_UNBRANDED_OR_STALE/);
 assertNoMaps(summary);
});
test('original qualifier refuses committed parent, surviving fixture, incomplete rows, or native writer drift',async()=>{
 for(const mutate of [p=>p.afterPacket.fixtureProof.parentStatus='committed',p=>p.afterPacket.fixtureProof.relations[0].remaining=1,
  p=>p.afterPacket.snapshot.backgroundRows[src.live.backgroundRelations[0]].push({keyHash:sha('key'),rowHash:sha('row')}),
  p=>p.afterPacket.snapshot.backgroundWriterRoutines.pop(),p=>p.afterPacket.snapshot.ledgerFingerprint=sha('drift')]){
  const p=snapshots();mutate(p);await assert.rejects(()=>validateRestoration({root:ROOT,result:nativeFixture(),receipt:built.receipt,...p}));
 }
});
test('bounded transport rejects diagnostics, malformed UTF8, too many frames and oversize capture',()=>{
 const good=new BoundedMemoryCapture('AFTER');good.stdout(Buffer.from('{}\n{}\n{}\n'));assert.equal(good.finish().transport.frameCount,3);
 for(const chunk of [Buffer.from('{}\n{}\n{}\n{}\n'),Buffer.from([255])]){const c=new BoundedMemoryCapture();c.stdout(chunk);assert.throws(()=>c.finish());}
 const diagnostic=new BoundedMemoryCapture();diagnostic.stderr('credential-like raw text');assert.throws(()=>diagnostic.finish(),/LOCAL_METADATA_DIAGNOSTIC_REFUSED/);assert.equal(diagnostic.chunks.length,0);
 const cap=new BoundedMemoryCapture();cap.stdout(Buffer.alloc(LIMITS.stdoutBytes+1));assert.throws(()=>cap.finish(),/LOCAL_METADATA_STDOUT_BOUND/);
 assert.equal(cap.chunks.length,0);assert.equal(LIMITS.childMilliseconds,70000);assert.equal(LIMITS.statementMilliseconds,45000);
});
test('control protocol refuses unsolicited, duplicate, oversized and EOF signals; cleanup budget remains bounded',async()=>{
 const input=new EventEmitter(),proc=new EventEmitter(),controls=new FiniteControls(input,new AbortController().signal,proc);
 const first=controls.wait('PREPARE');input.emit('data','PREPARE\n');await first;
 const second=controls.wait('RELEASE');input.emit('data','PREPARE\n');await assert.rejects(second,/CONTROL_PHASE_REFUSED/);controls.dispose();
 for(const message of ['anything\n','x'.repeat(BUDGET.controlBytes+1),'ABORT\n']){const c=new FiniteControls(new EventEmitter(),new AbortController().signal,new EventEmitter());const p=c.wait('PREPARE');c.feed(message);await assert.rejects(p);c.dispose();}
 const c=new FiniteControls(new EventEmitter(),new AbortController().signal,new EventEmitter()),p=c.wait('PREPARE');c.input.emit('end');await assert.rejects(p,/CONTROL_EOF_ABORT/);c.dispose();
 assert.throws(()=>assertElapsed(0,180001));assert.equal(BUDGET.workMilliseconds+BUDGET.cleanupReserveMilliseconds,BUDGET.wholeMilliseconds);
});
test('work timer cause survives finite controls; genuine turn loss remains distinct with unchanged budgets',async()=>{
 for(const [reason,expected]of [[WORK_DEADLINE_CODE,WORK_DEADLINE_CODE],['TURN_OWNERSHIP_LOST','CONTROL_OWNED_TURN_LOST']]){
  const external=new AbortController(),c=new FiniteControls(new EventEmitter(),external.signal,new EventEmitter()),pending=c.wait('PREPARE');
  external.abort(Error(reason));await assert.rejects(pending,e=>e.message===expected);assert.equal(c.signal.reason.message,expected);c.dispose();
 }
 assert.equal(BUDGET.workMilliseconds,120000);assert.equal(BUDGET.wholeMilliseconds,180000);
});
test('source/authority boundaries refuse another root and unauthorised execution before a connection',async()=>{
 await assert.rejects(()=>source(path.resolve(here,'../../../..')),/PURE_REGISTERED_PHYSICAL_ROOT_REQUIRED/);
 await assert.rejects(()=>holdPure({root:ROOT,out:'unused'}),/PURE_ROOT_HOLDER_AUTHORIZATION_REQUIRED/);
 for(const value of [{backgroundRows:{}},{x:{keyHash:sha('sensitive')}},{fixtureObservations:[]}])assert.throws(()=>assertNoMaps(value),/RAW_CANONICAL_MAP_PERSISTENCE_REFUSED/);
 assert.doesNotMatch(fs.readFileSync(path.join(here,'hold-pure.mjs'),'utf8'),/caseId|cleanupWitness|LOCK TABLE|qualificationInput/);
});
