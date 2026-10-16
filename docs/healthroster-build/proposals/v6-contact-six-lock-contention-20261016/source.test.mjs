import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {sha,assertSourcePins} from './prepare.mjs';
import {qualifyCase,lockMode,runNative} from './run-contention.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const plan=JSON.parse(fs.readFileSync(path.join(here,'PLAN.json'),'utf8'));
test('six exact canonical acquisitions have distinct target holders and no original assertion/body/ledger in transmitted prefixes',()=>{
 assert.equal(plan.cases.length,6);assert.equal(new Set(plan.cases.map(row=>row.targetTable)).size,6);
 for(const row of plan.cases){
  assert.equal(sha(row.prefix),row.prefixSha256);assert.equal(row.commands.length,row.canonicalTargetOrdinal);
  const packet=plan.packets.find(p=>p.path===row.migrationPath);assert.equal(packet.packet.slice(0,row.targetEnd),row.prefix);
  assert.ok(row.targetEnd<packet.firstOriginalOffset);assert.equal(row.commands.at(-1),`lock table ${row.targetTable} in ${row.targetMode} mode nowait;`);
  assert.ok(!/\b(?:create|alter|drop|insert|update|delete|truncate)\b/i.test(row.commands.join('\n')));
  assert.equal(row.preceding.some(p=>p.table===row.targetTable),false);
  assert.equal(row.holderMode,row.targetMode==='share'?'row exclusive':'access share');
  assert.ok(lockMode(row.holderMode));
 }
 assert.equal(plan.cases.filter(row=>row.holderReadOnly).length,3);
});
test('saved actual source pins match the complete frozen source evidence',()=>{
 assert.equal(assertSourcePins(plan,path.join(here,'inputs/root')),true);
});
test('positive pair retains both complete canonical bodies and original assertions/DDL in order, with no ledger/release row',()=>{
 const bodies=plan.packets.map(row=>row.body);
 assert.equal(sha(plan.positive.sql),plan.positive.sha256);
 for(const [index,packet]of plan.packets.entries()){
  assert.equal(sha(packet.body),packet.bodySha256);
  assert.equal(fs.readFileSync(path.join(here,`positive/contact${index===0?'20':'30'}-COMPLETE_CANONICAL_BODY.sql`),'utf8'),packet.body);
 }
 assert.ok(plan.positive.sql.indexOf(bodies[0])<plan.positive.sql.indexOf(bodies[1]));
 assert.ok(plan.positive.sql.startsWith('begin isolation level read committed;'));
 assert.ok(plan.positive.sql.endsWith('rollback;'));
 assert.ok(!/insert into private\.cloudtms_(?:migration_ledger|database_releases)/i.test(plan.positive.sql));
 assert.ok(plan.positive.sql.includes('CONTACT_SOURCE_AUTOMATIC_TRANCHE_PRIOR_ENUM_MISMATCH'));
 assert.ok(plan.positive.sql.includes('CONTACT_SUBMISSION_RETAINED_IDENTITY_UNRESOLVED'));
});
function fake(testCase,{targetState='55P03',earlyState,drift=false}={}){
 const events=[],receipts=[];let snapshots=0,holder=false;
 const turn={identity:{backendPid:100,databaseOid:20},async assertHeld(){events.push('assertHeld');},turnConnection:{async query(sql){
  events.push({holderSql:sql});
  if(sql.includes("'valid'"))return[JSON.stringify({physicalDatabase:'cloudtms_test_clone',valid:true})];
  if(sql.startsWith('begin ')){holder=true;return[];}
  if(sql==='rollback;'){holder=false;return[];}
  if(sql.includes("'onlyOneWitnessLock'"))return[JSON.stringify({backendPid:100,physicalDatabase:'cloudtms_test_clone',readOnly:testCase.holderReadOnly,targetGranted:holder,onlyOneWitnessLock:true})];
  if(sql.includes("'writerGone'"))return[JSON.stringify({writerGone:true,writerLocksGone:true})];
  if(sql.includes("'witnessLocksGone'"))return[JSON.stringify({witnessLocksGone:!holder})];
  throw Error('UNEXPECTED_HOLDER_SQL');
 }}};
 const writer={failure:null,finished:false,async execute(sql){
  events.push({writerSql:sql});
  if(sql.includes("'serverMajor'"))return JSON.stringify({backendPid:202,physicalDatabase:'cloudtms_test_clone',serverMajor:17,testIdentity:true,legacy:true,noApplying:true});
  if(sql==='ACQUIRE')return't';
  if(sql.includes("'precedingGranted'"))return JSON.stringify({writerPid:202,holderAlive:holder,holderGranted:holder,precedingGranted:true});
  if(earlyState&&testCase.commands.slice(2,-1).includes(sql)){const error=Object.assign(Error('closed'),{sqlState:earlyState});this.failure=error;throw error;}
  if(sql===testCase.commands.at(-1)&&targetState){const error=Object.assign(Error('closed'),{sqlState:targetState});this.failure=error;throw error;}
  return'';
 },async close(){this.finished=true;events.push('writerClosed');}};
 const snapshot=async()=>{assert.equal(holder,false);assert.ok(!snapshots||writer.finished);events.push('freshSnapshot');return{
  physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',markerAbsent:true,
  applyingCount:0,otherActivityCount:0,observerPid:++snapshots===1?201:203,connectedFresh:true,marker:plan.marker,
  ledgerFingerprint:drift&&snapshots===2?'b':'a',catalogueFingerprint:'a',businessCountsFingerprint:'a',businessRowsFingerprint:'a',relationCensus:'a'};};
 const assertRestoration=(before,after,marker)=>{assert.equal(marker,plan.marker);for(const key of['ledgerFingerprint','catalogueFingerprint','businessCountsFingerprint','businessRowsFingerprint','relationCensus'])if(before[key]!==after[key])throw Object.assign(Error('drift'),{code:'RESTORATION_MISMATCH'});return{connectedFresh:true,markerAbsent:true};};
 return {events,receipts,writer,options:{plan,testCase,turn,openWriter:async()=>writer,snapshot,assertRestoration,acquireWriterSql:'ACQUIRE',record:r=>receipts.push(r)}};
}
for(const testCase of plan.cases)test(testCase.id+' reaches exact target after all earlier locks and restores both owned sessions',async()=>{
 const f=fake(testCase);const result=await qualifyCase(f.options);
 assert.equal(result.status,'PASS_EXPECTED_LOCK_REFUSAL_FRESH_RESTORATION');assert.equal(result.precedingSuccessful,testCase.preceding.length);
 assert.equal(result.actualSqlState,'55P03');assert.equal(result.holderWitnessLocksGone,true);assert.equal(result.writerShutdown.writerLocksGone,true);
 const sent=f.events.filter(e=>e.writerSql).map(e=>e.writerSql);assert.ok(!sent.some(sql=>sql.includes('CONTACT_SUBMISSION_')||sql.includes('CONTACT_SOURCE_AUTOMATIC_')));
 assert.equal(f.receipts.length,1);assert.equal(f.events.filter(e=>e==='freshSnapshot').length,2);
});
test('missed target contention aborts before any original body, explicitly rolls back and still freshly proves restoration',async()=>{
 const f=fake(plan.cases[5],{targetState:null});await assert.rejects(qualifyCase(f.options),/CONTACT_EXPECTED_CONTENTION_MISSING/);
 assert.ok(f.events.some(e=>e.writerSql==='rollback;'));assert.equal(f.receipts[0].holderRolledBack,true);assert.equal(f.receipts[0].after.connectedFresh,true);
});
test('wrong target SQLSTATE cannot count as expected refusal but cleanup/fresh proof still run',async()=>{
 const f=fake(plan.cases[0],{targetState:'42501'});await assert.rejects(qualifyCase(f.options),/CONTACT_UNEXPECTED_TARGET_SQLSTATE/);
 assert.equal(f.receipts[0].actualSqlState,'42501');assert.equal(f.receipts[0].after.connectedFresh,true);
});
test('earlier-lock conflict does not receive target success credit',async()=>{
 const f=fake(plan.cases[5],{earlyState:'55P03'});await assert.rejects(qualifyCase(f.options));
 assert.equal(f.receipts[0].targetDispatched,undefined);assert.equal(f.receipts[0].after.connectedFresh,true);
});
test('restoration mismatch holds a successful target refusal',async()=>{
 const f=fake(plan.cases[0],{drift:true});await assert.rejects(qualifyCase(f.options),/drift/);
 assert.equal(f.receipts[0].cleanupCode,'RESTORATION_MISMATCH');assert.equal(f.receipts[0].status,'HOLD_INCOMPLETE_OR_RESTORATION_FAILED');
});
test('import and unauthorized native entry cannot read credentials or open sessions',async()=>{
 await assert.rejects(runNative({root:'absent',output:'absent'}),/ROOT_NATIVE_AUTHORIZATION_REQUIRED/);
});
