// SYNTHETIC offline envelopes below are boundary tests, never native receipts.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {EventEmitter} from 'node:events';
import {source,ROOT,here,sha,PROGRAMME_SHA,SCOPE,assertManifestedFixtureEntry} from './source.mjs';
import {fixturePredicate,buildActorProgram} from './build-program.mjs';
import {buildInput} from './input-builder.mjs';import {readStage,bindSnapshots,validateRestoration,statementResponses} from './result-validator.mjs';
import {createFixedMetadataReader,BoundedMemoryCapture,LIMITS} from './fixed-metadata-reader.mjs';
import {holdActor} from './hold-actor.mjs';import {FiniteControls,BUDGET,assertNoMaps} from './control.mjs';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:100,turnId:'hr_turn_'+'a'.repeat(32),key:[20261013,1]};
const src=await source(ROOT),built=await buildInput({root:ROOT,identity});
const row=(value,prefix='')=>({command:'SELECT',rowCount:1,fields:['result'],rowsTruncated:false,rows:[{result:prefix?prefix+JSON.stringify(value):value}]}),empty=command=>({command,rowCount:null,fields:[],rows:[],rowsTruncated:false});
const clock=t=>({transactionStartedAt:'2026-10-17T10:00:00.000Z',statementStartedAt:t,observedAt:t});
const transaction={database:'cloudtms_test_clone',backendPid:200,xid:'50',epochZero:true},observation={...transaction,unit:0,rows:[]};
const actorResult={programme:'DAILY_HEALTH_ACTOR_NATIVE_V1',fixtureRows:6,matrixChecks:12,matrixDenials:10,sameTransactionChanges:3,constraintRefusals:1,nullGuardRefusals:2,effectiveRoleAclChecks:3,directRoleExecution:'NOT_RUN',nativeWPPP:true,dailyBasis:'NOT_RUN',postWaitRace:'NOT_RUN',outerIsolation:'read committed'};
function nativeFixture(){
 const result={database:'cloudtms_test_clone',databaseTarget:'agency_test',label:built.input.label,sqlSha256:built.receipt.sqlSha256,
 statementCount:built.receipt.statements,accepted:true,rolledBack:true,statements:built.receipt.responses.map(e=>({statement:e.statement,...e.kind==='ONE_ROW'?row(null):empty(e.command)}))};
 const set=(key,value)=>Object.assign(result.statements[built.receipt.indices[key]-1],value);
 set('transaction',row(transaction,'HR_REHEARSAL_TRANSACTION_JSON '));set('fixtureObservation',row(observation,'HR_FIXTURE_OBSERVATION_JSON '));set('programmeResult',row(structuredClone(actorResult)));
 for(const[key,phase,t]of [['guardBefore','B_GUARD_BEFORE','2026-10-17T10:00:00.001Z'],['guardAfter','B_GUARD_AFTER','2026-10-17T10:00:00.002Z']])set(key,row({phase,physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:200,holderPid:100,holderGranted:true,readOnly:false,...clock(t)}));
 set('completion',row({physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:200,xid:'50',marker:built.receipt.marker,markerPresent:true,markerRows:0,holderPid:100,holderGranted:true,noRoleSwitch:true,...clock('2026-10-17T10:00:00.003Z')}));
 result.rollbackVerification={connectedFresh:true,database:'cloudtms_test_clone',statements:[row('pg_catalog, public'),row({physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',applyingCount:0,markerAbsent:true}),
 row({physicalDatabase:'cloudtms_test_clone',databaseOid:123,holderPid:100,backendPid:200,holderGranted:true,readOnly:true,noRoleSwitch:true,...clock('2026-10-17T10:00:00.004Z')}),
 row({userFixtureRowsRemaining:0,readOnly:true,backendPid:200})].map((s,i)=>({statement:i+1,...s}))};return result;
}
function snapshots(){
 const relations=['private.candidate_daily_batch_receipts','public.tms_users','public.ts_financials_outbox'].map(relation=>({relation,count:0,hash:sha('')}));
 const base={physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',applyingCount:0,markerAbsent:true,marker:built.receipt.marker,
 ledgerFingerprint:sha('ledger'),catalogueFingerprint:sha('catalogue'),relationCensus:sha('census'),businessCountsFingerprint:sha('counts'),businessRowsFingerprint:sha('rows'),businessRelations:relations,otherActivityCount:1,
 backgroundRows:Object.fromEntries(src.live.backgroundRelations.map(r=>[r,[]])),backgroundWriterRoutines:src.expectedWriters,backgroundSnapshotEpochZero:true,backgroundSnapshotXmax:'100',backgroundSnapshotXip:[],
 backgroundSnapshotStartedAt:'2026-10-17T10:00:00Z',backgroundObservedAt:'2026-10-17T10:00:01Z'};
 const packet=(phase,pid)=>({childClosed:true,provenance:{phase,scope:SCOPE,canonicalSqlSha256:built.receipt.canonicalSqlSha256,observeLiveBusiness:true,fixtureSource:'UNCHANGED_CANONICAL_FRESH_FIXTURE_PROOF_C_AFTER_THIRD_FRAME'},transport:{frameCount:phase==='AFTER'?3:2},
 peer:{database:'cloudtms_test_clone',databaseOid:123,holderPid:100,readOnly:true,isolation:'repeatable read',noRoleSwitch:true,readerPid:pid},snapshot:structuredClone(base)});
 const beforePacket=packet('BEFORE',400),afterPacket=packet('AFTER',500);afterPacket.snapshot.connectedFresh=true;
 afterPacket.fixtureProof={database:'cloudtms_test_clone',backendPid:500,connectedFresh:true,rehearsalBackendPid:200,rehearsalXid:'50',parentStatus:'aborted',relations:src.live.backgroundRelations.map(relation=>({relation,observed_fixture_keys:0,remaining:0}))};return{beforePacket,afterPacket};
}
test('exact complete one-helper actor programme, six native users and closed store boundary',()=>{
 assert.equal(src.programme.sqlSha256,PROGRAMME_SHA);assert.equal(src.programme.sqlBytes,19447);assert.equal(src.programme.definitionCount,1);assert.equal(src.programme.businessFixtureRows,6);
 assert.match(src.programme.sql,/pg_get_constraintdef\(c.oid,true\)/);assert.match(src.programme.sql,/pg_get_triggerdef\(t.oid,true\)/);assert.match(src.programme.sql,/get stacked diagnostics violated_constraint=CONSTRAINT_NAME/);
 assert.match(src.programme.sql,/revoke all on function private.daily_rate_correction_health_actor_require_v1\(uuid,boolean\) from public,anon,authenticated,service_role/);
 assert.doesNotMatch(src.programme.sql,/grant\s|insert into (?:private|public)\.(?!tms_users)|update (?:private|public)\.(?!tms_users)|\b(?:commit|rollback)\s*;|daily_rate_correction_health_(?:basis|review_prepare|accept_basis|result_basis)_v1/);
 assert.doesNotMatch(src.programme.sql,/\bset\s+(?:(?:local|session)\s+)?(?:transaction|transaction_isolation)\b|\bset_config\s*\(\s*'transaction_isolation'/i);
});
test('namespace and physical input are closed, no arbitrary SQL or user fixture selection',async()=>{
 assert.throws(()=>fixturePredicate({...src.pins,fixtureIDs:['00000000-0000-0000-0000-000000000001']}),/EXACT_RESERVED_NAMESPACE/);
 assert.throws(()=>fixturePredicate(src.pins,'malicious'),/FIXED_ALIAS/);await assert.rejects(()=>buildActorProgram({root:path.dirname(ROOT)}),/REGISTERED_PHYSICAL_ROOT/);
});
test('entire existing actor body copied byte-for-byte in gate canonical form',()=>{
 const page=fs.readFileSync(ROOT+'/supabase/repeatable/17102026_0248_nhsp_v6_daily_health_support/01_complete_helpers.sqlinc','utf8');const p=src.sql.scanManagedSql(page);
 const exact=p.statements.map(s=>p.source.slice(s.start,s.end).trim()).find(s=>s.startsWith('create or replace function private.daily_rate_correction_health_actor_require_v1('));assert.equal(src.programme.sql.split(exact).length,2);
});
test('20-statement finite B includes native observations, real marker and four fresh checks',()=>{
 assert.equal(built.receipt.statements,20);assert.equal(built.receipt.programmeStatements,7);assert.equal(built.receipt.verificationResponses.length,4);assert.equal(built.receipt.businessFixtureRows,6);
 assert.equal(built.input.sql.split(src.programme.sql).length,2);assert.ok(built.input.sql.includes(src.live.fixtureObservationSql(0)));assert.match(built.input.verification_sql,/userFixtureRowsRemaining/);
 assert.equal(built.input.database,'agency_test');assert.equal(built.input.continue_on_error,false);assert.equal(built.input.max_rows_per_statement,1);assert(built.input.sql.length<8000000);
});
test('closed response admits genuine pooled MCP PID shape and actual asserted counts only',()=>{
 const r=nativeFixture();assert(statementResponses(r.statements,built.receipt.responses));const s=readStage(r,built.receipt);assert.equal(s.freshPeerPid,transaction.backendPid);
 for(const key of ['fixtureRows','matrixChecks','matrixDenials','sameTransactionChanges','constraintRefusals','nullGuardRefusals','effectiveRoleAclChecks']){const bad=nativeFixture();bad.statements[built.receipt.indices.programmeResult-1].rows[0].result={...actorResult,[key]:0};assert.throws(()=>readStage(bad,built.receipt),/ACTUAL_ASSERTION_COUNTS/);}
 const claimed=nativeFixture();claimed.statements[built.receipt.indices.programmeResult-1].rows[0].result.directRoleExecution='PASS';assert.throws(()=>readStage(claimed,built.receipt),/ACTUAL_ASSERTION_COUNTS/);
 const fake=nativeFixture();fake.statements[built.receipt.indices.programmeResult-1].rows[0].result={...actorResult,dailyBasis:'PASS'};assert.throws(()=>readStage(fake,built.receipt),/ACTUAL_ASSERTION_COUNTS/);
});
test('strict SELECT counts and closed per-index command/null ABI refuse invented success',()=>{
 const bad=nativeFixture();bad.statements[0].rowCount=0;assert(!statementResponses(bad.statements,built.receipt.responses));const other=nativeFixture();other.statements[built.receipt.indices.programmeResult-1].rowCount=null;assert.throws(()=>readStage(other,built.receipt),/STATEMENT_SEQUENCE/);
 const missed=nativeFixture();missed.rollbackVerification.statements[3].rows[0].result.userFixtureRowsRemaining=1;assert.throws(()=>readStage(missed,built.receipt),/FRESH_USER_FIXTURES_ABSENT/);
 const extra=nativeFixture();extra.rollbackVerification.statements[3].rows[0].result.unknown='unreviewed';assert.throws(()=>readStage(extra,built.receipt),/FRESH_USER_FIXTURES_ABSENT/);
});
test('whole original canonical SQL includes complete users plus both backgrounds and genuine third C-after proof',async()=>{
 const reader=await createFixedMetadataReader({root:ROOT,turn:{identity,signal:new AbortController().signal,assertHeld:async()=>{},runOwnedChild:async()=>{throw Error('OFFLINE_ONLY');}}});
 const x=reader.inspectSource({transaction,observations:[observation],before:snapshots().beforePacket.snapshot});assert.deepEqual(x.relations.map(r=>r.schema+'.'+r.name),['private.candidate_daily_batch_receipts','public.tms_users','public.ts_financials_outbox']);
 assert.equal(x.canonicalSql,src.rehearsal.restorationSnapshotSql({identity,marker:built.receipt.marker,relations:x.relations,exportContractSql:fs.readFileSync(ROOT+'/supabase/release/export_contract.sql','utf8'),observeLiveBusiness:true}));
 assert.equal(x.after.fixtureSql,src.live.freshFixtureProofSql(transaction,[observation],snapshots().beforePacket.snapshot));assert.equal(sha(x.canonicalSql),built.receipt.canonicalSqlSha256);
});
test('distinct C-after remains mandatory; missing users scope and reused C-after refuse',()=>{
 const r=nativeFixture(),s=readStage(r,built.receipt);const a=snapshots();a.afterPacket.peer.readerPid=transaction.backendPid;assert.throws(()=>bindSnapshots({receipt:built.receipt,stage:s,...a}),/DISTINCT_C_AFTER/);
 const b=snapshots();b.beforePacket.snapshot.businessRelations=b.beforePacket.snapshot.businessRelations.filter(r=>r.relation!=='public.tms_users');assert.throws(()=>bindSnapshots({receipt:built.receipt,stage:s,...b}),/COMPLETE_BACKGROUND_SCOPE/);
});
test('original WeakMap qualification succeeds only for its exact bound synthetic snapshots',async()=>{
 const frames=snapshots(),out=await validateRestoration({root:ROOT,result:nativeFixture(),receipt:built.receipt,...frames});assert.equal(out.businessFixtureRows,6);assert.equal(out.dailyBasis,'NOT_RUN');
 const changed=snapshots();changed.afterPacket.snapshot.businessRelations.find(r=>r.relation==='public.tms_users').hash=sha('unrestored user');await assert.rejects(()=>validateRestoration({root:ROOT,result:nativeFixture(),receipt:built.receipt,...changed}),/UNATTRIBUTED_BUSINESS_CHANGE/);
 const forged=snapshots();forged.afterPacket.snapshot.businessRowsFingerprint=sha('changed');forged.afterPacket.snapshot.liveRestoration={status:'LIVE_TEST_RESTORATION_QUALIFIED'};assert.throws(()=>src.rehearsal.assertFreshRestoration(forged.beforePacket.snapshot,forged.afterPacket.snapshot,built.receipt.marker),/UNBRANDED_OR_STALE/);
});
test('bounded capture, protocol and deadlines retain original limits and reject raw-map output',()=>{
 assert.equal(BUDGET.workMilliseconds,120000);assert.equal(BUDGET.wholeMilliseconds,180000);assert.equal(LIMITS.stdoutBytes,64*1024*1024);
 const capture=new BoundedMemoryCapture();capture.stderr(Buffer.from('diagnostic'));assert.throws(()=>capture.finish(),/DIAGNOSTIC_REFUSED/);
 const input=new EventEmitter();input.setEncoding=()=>{};input.resume=()=>{};input.pause=()=>{};const ctrl=new FiniteControls(input,new AbortController().signal,new EventEmitter());ctrl.feed('UNREVIEWED\n');assert(ctrl.signal.aborted);ctrl.dispose();assert.throws(()=>assertNoMaps({backgroundRows:{}}),/MAP_PERSISTENCE/);
});
test('holder rejects authorization before connecting and wrong root independent of mount location',async()=>{
 await assert.rejects(()=>holdActor({root:ROOT,out:'unused'}),/ROOT_HOLDER_AUTHORIZATION/);await assert.rejects(()=>source(path.dirname(ROOT)),/REGISTERED_PHYSICAL_ROOT/);
 // Only a committed exact root manifest entry may authorize business fixtures.
 if(!fs.readFileSync(ROOT+'/docs/healthroster-build/TEST_SETUP_MANIFEST.md','utf8').includes('### DAILY-HEALTH-ACTOR-RB-R2-20261017'))assert.throws(()=>assertManifestedFixtureEntry(ROOT),/EXACT_COMMITTED_SETUP_ENTRY/);
});
