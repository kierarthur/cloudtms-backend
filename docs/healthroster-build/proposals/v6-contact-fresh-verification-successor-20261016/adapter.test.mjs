// Synthetic offline evidence only. No DB/tool call, credential or native claim.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {EventEmitter} from 'node:events';
import {buildInput,source,sha} from '../v6-contact-local-metadata-mcp-adapter-20261016/input-builder.mjs';
import {readStageOne,prepareQualification,validatePair,oneRow} from './result-validator.mjs';
import {prepareQualification as oldPrepare,validatePair as oldValidate} from '../v6-contact-local-metadata-mcp-adapter-20261016/result-validator.mjs';
import {BUDGET,FiniteControls,assertElapsed,assertNoMaps} from '../v6-contact-local-metadata-mcp-adapter-20261016/control.mjs';
import {holdCase,cleanupWitness} from './hold-case.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:20,backendPid:100,turnId:'hr_turn_'+'0'.repeat(32),key:[20261013,1]};
const loaded=await source(root);
for(const row of loaded.plan.cases)test(row.id+' preserves every original B statement except complete large snapshot and ends at exact target',async()=>{
 const e=await buildInput({root,caseId:row.id,identity}),scan=loaded.sql.scanManagedSql(e.input.sql),old=loaded.sql.scanManagedSql(e.reverse.originalSql);
 assert.equal(e.receipt.statements,old.statements.length-1);assert.equal(scan.length,undefined);
 assert.equal(e.input.sql.slice(scan.statements.at(-1).start),row.commands.at(-1));
 assert.deepEqual(scan.statements.filter(s=>s.tokens[0]==='LOCK').map(s=>e.input.sql.slice(s.start,s.end)),row.commands.slice(2));
 assert.ok(!e.input.sql.includes('backgroundRows'));assert.ok(!e.input.verification_sql.includes('backgroundRows'));
 assert.ok(e.input.verification_sql.includes("'markerAbsent'"));assert.ok(e.input.verification_sql.includes("'module'"));
 assert.ok(e.receipt.original.wholeRawReverse);assert.ok(e.receipt.original.everyOtherStatementUnchanged);assert.ok(e.receipt.original.freshOtherStatementBytesUnchanged);
 assert.equal(e.input.max_rows_per_statement,1);assert.equal(e.input.continue_on_error,false);
});
const row=(value,prefix='')=>({command:'SELECT',rowCount:1,rowsTruncated:false,fields:['frame'],rows:[{frame:prefix?prefix+JSON.stringify(value):value}]});
async function evidence(){
 const {input,receipt}=await buildInput({root,caseId:'contact30-lock4',identity});
 const businessRelations=[...loaded.plan.relationNames,...loaded.live.backgroundRelations].map(relation=>({relation,count:0,hash:sha('')}));
 const base={physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',applyingCount:0,markerAbsent:true,marker:receipt.marker,
  ledgerFingerprint:sha('ledger'),catalogueFingerprint:sha('catalogue'),businessCountsFingerprint:sha('counts'),businessRowsFingerprint:sha('rows'),relationCensus:sha('relations'),businessRelations,
  backgroundRows:Object.fromEntries(loaded.live.backgroundRelations.map(r=>[r,[]])),backgroundSnapshotEpochZero:true,backgroundSnapshotXmax:'100',backgroundSnapshotXip:[],
  backgroundSnapshotStartedAt:'2026-10-16T12:00:00Z',backgroundObservedAt:'2026-10-16T12:00:00Z',backgroundWriterRoutines:loaded.expected.expectedWriters,otherActivityCount:7};
 const packet=(snapshot,phase,pid)=>({snapshot,childClosed:true,peer:{database:'cloudtms_test_clone',databaseOid:20,holderPid:100,readerPid:pid,readOnly:true,isolation:'repeatable read',noRoleSwitch:true},
  provenance:{caseId:receipt.caseId,phase,canonicalSqlSha256:receipt.beforeCanonicalSqlSha256,observeLiveBusiness:true},transport:{stdoutBytes:1000,snapshotUtf8Bytes:900,snapshotSha256:sha(phase)}});
 const beforePacket=packet(structuredClone(base),'BEFORE',201),afterPacket=packet({...structuredClone(base),connectedFresh:true,otherActivityCount:9,backgroundSnapshotXmax:'110',backgroundObservedAt:'2026-10-16T12:00:02Z'},'AFTER',204);
 const transaction={database:'cloudtms_test_clone',backendPid:202,xid:'105',epochZero:true},observation={unit:0,...transaction,rows:[]};
 const proof={holderPid:100,writerPid:202,holderGranted:true,precedingCount:3,precedingGranted:true};
 const statements=Array.from({length:receipt.statements},(_,i)=>({statement:i+1,command:'SET',rowCount:null,fields:[],rows:[],rowsTruncated:false}));
 const put=(i,s)=>statements[i-1]={statement:i,...s};put(receipt.indices.transaction,row(transaction,'HR_REHEARSAL_TRANSACTION_JSON '));put(receipt.indices.fixtureObservation,row(observation,'HR_FIXTURE_OBSERVATION_JSON '));
 for(const [n,i]of receipt.indices.proofs.entries())put(i,row({...proof,precedingCount:n+1}));put(receipt.indices.targetReady,row(proof));put(receipt.indices.expectedFailedStatement,{error:{code:'55P03',message:'could not obtain lock on relation "unit_witness"',routine:'LockTableRecurse'}});
 const result={accepted:false,databaseTarget:'agency_test',database:'cloudtms_test_clone',label:input.label,sqlSha256:receipt.sqlSha256,statementCount:receipt.statements,statements,rolledBack:true,fatalError:structuredClone(statements.at(-1).error),
  rollbackVerification:{connectedFresh:true,database:'cloudtms_test_clone',statements:[{statement:1,...row('pg_catalog, public')},{statement:2,...row({physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',applyingCount:0,markerAbsent:true})},{statement:3,...row({...proof,writerPid:203,precedingCount:0})}]}};
 const args={root,input,receipt,result,beforePacket,afterPacket},qualificationInput=await prepareQualification(args);
 const fixtureProof={database:'cloudtms_test_clone',backendPid:205,connectedFresh:true,rehearsalBackendPid:202,rehearsalXid:'105',parentStatus:'aborted',relations:loaded.live.backgroundRelations.map(relation=>({relation,observed_fixture_keys:0,remaining:0}))};
 const qualificationResult={accepted:true,rolledBack:true,statementCount:2,databaseTarget:'agency_test',database:'cloudtms_test_clone',label:qualificationInput.label,sqlSha256:sha(qualificationInput.sql),
  statements:[{statement:1,...row(fixtureProof)},{statement:2,...row({...proof,writerPid:205,precedingCount:0})}],rollbackVerification:{connectedFresh:true,database:'cloudtms_test_clone',statements:[{statement:1,...row({...fixtureProof,backendPid:206})},{statement:2,...row({...proof,writerPid:206,precedingCount:0})}]}};
 return {...args,qualificationInput,qualificationResult};
}
test('complete canonical WeakMap qualifier permits unchanged fingerprints with real-shaped active-session evidence',async()=>{
 const e=await evidence(),s=await validatePair(e);assert.equal(s.status,'EXPECTED_EXACT_LOCK_AND_FULL_CANONICAL_RESTORATION_QUALIFIED_PENDING_OWNED_CLOSE');
 assert.equal(s.restoration.liveRestoration.status,'LIVE_TEST_RESTORATION_QUALIFIED');assert.equal(s.after.otherActivityCount,9);assert.doesNotThrow(()=>assertNoMaps(s));
 assert.ok(!JSON.stringify(s).includes('backgroundRows'));assert.ok(!JSON.stringify(s).includes('backgroundSnapshotXip'));
});
test('changed witness fingerprint cannot be waived by active sessions',async()=>{const e=await evidence();e.afterPacket.snapshot.businessRelations[0].hash=sha('changed');await assert.rejects(validatePair(e),/REHEARSAL_UNATTRIBUTED_BUSINESS_CHANGE/);});
test('complete background maps are still mandatory even unchanged',async()=>{const e=await evidence();e.afterPacket.snapshot.backgroundRows[loaded.live.backgroundRelations[0]]=[{keyHash:sha('k'),rowHash:sha('r')}];await assert.rejects(validatePair(e),/REHEARSAL_BACKGROUND_MAP_INCOMPLETE/);});
test('full genuine-shaped Daily claim attribution uses unchanged canonical validator and retains no row map in summary',async()=>{
 const e=await evidence(),relation=loaded.live.backgroundRelations[0],after=e.afterPacket.snapshot;
 const claim={keyHash:sha('new-claim'),rowHash:sha('complete-native-row'),xmin:'108',xactStatus:'committed',nativeClaimProved:true,
  createdAt:'2026-10-16T12:00:01Z',updatedAt:'2026-10-16T12:00:01Z',completedAt:'2026-10-16T12:00:01Z'};
 after.backgroundRows[relation]=[claim];const aggregate=after.businessRelations.find(r=>r.relation===relation);aggregate.count=1;aggregate.hash=sha(claim.rowHash);
 after.businessCountsFingerprint=sha('new-counts');after.businessRowsFingerprint=sha('new-rows');
 const s=await validatePair(e);assert.equal(s.restoration.liveRestoration.qualifiedBusinessChanges[0].created,1);assert.doesNotThrow(()=>assertNoMaps(s));
});
test('plain copied live status cannot forge the private canonical WeakMap brand',async()=>{const e=await evidence();e.afterPacket.snapshot.liveRestoration={status:'LIVE_TEST_RESTORATION_QUALIFIED'};assert.throws(()=>loaded.rehearsal.assertFreshRestoration(e.beforePacket.snapshot,e.afterPacket.snapshot,e.receipt.marker),/REHEARSAL_LIVE_RESTORATION_UNBRANDED_OR_STALE/);});
test('native four-writer source drift is refused',async()=>{const e=await evidence();e.afterPacket.snapshot.backgroundWriterRoutines[0].definition_sha256=sha('drift');await assert.rejects(validatePair(e),/MCP_ACTUAL_BACKGROUND_WRITER_DEFINITIONS_CHANGED/);});
test('wrong/earlier SQLSTATE cannot receive expected target credit',async()=>{const e=await evidence();e.result.statements.at(-1).error.code='42501';assert.throws(()=>readStageOne(e.result,e.receipt),/MCP_EXACT_FINAL_LOCK_REFUSAL_REQUIRED/);});
test('actual closed failed statement/error shape is accepted without inventing payload flags',async()=>{const e=await evidence();const failed=e.result.statements.at(-1);assert.deepEqual(Object.keys(failed),['statement','error']);assert.doesNotThrow(()=>readStageOne(e.result,e.receipt));assert.equal(Object.hasOwn(failed,'rowsTruncated'),false);});
test('error rows with any partial payload/flag/unknown data hold rather than normalize',async()=>{
 for(const extra of [{rows:[]},{rowsTruncated:true},{rowsTruncated:false},{rowCount:0},{unknown:'data'}]){const e=await evidence();Object.assign(e.result.statements.at(-1),extra);assert.throws(()=>readStageOne(e.result,e.receipt),/MCP_STATEMENT_PREFIX_INCOMPLETE/);}
 const e=await evidence();e.result.statements.at(-1).error.extra='data';assert.throws(()=>readStageOne(e.result,e.receipt),/MCP_STATEMENT_PREFIX_INCOMPLETE/);
});
test('successful native statement missing rowsTruncated remains HOLD',async()=>{const e=await evidence();delete e.result.statements[0].rowsTruncated;assert.throws(()=>readStageOne(e.result,e.receipt),/MCP_STATEMENT_PREFIX_INCOMPLETE/);});
test('only actual matching expected55P03 fatal error is admitted for accepted=false',async()=>{for(const fatal of [null,{code:'55P03',message:'different',routine:'LockTableRecurse'}]){const e=await evidence();e.result.fatalError=fatal;assert.throws(()=>readStageOne(e.result,e.receipt),/MCP_EXACT_EXPECTED_FATAL_ERROR_REQUIRED/);}});
test('fresh small identity/LEGACY/marker cannot be substituted or omitted',async()=>{for(const [field,value]of [['module','NEXT:2'],['markerAbsent',false],['identityTest',false]]){const e=await evidence();e.result.rollbackVerification.statements[1].rows[0].frame[field]=value;assert.throws(()=>readStageOne(e.result,e.receipt),/MCP_FRESH_TEST_LEGACY_MARKER_REQUIRED/);}});
test('C-after cannot reuse A/B/before or MCP-fresh PID',async()=>{for(const pid of [100,201,202,203]){const e=await evidence();e.afterPacket.peer.readerPid=pid;await assert.rejects(validatePair(e),/LOCAL_(AFTER_NOT_FRESH|CANONICAL_NATIVE_PEER_REQUIRED)/);}});
test('canonical local source/query drift is refused',async()=>{const e=await evidence();e.afterPacket.provenance.canonicalSqlSha256=sha('different');await assert.rejects(validatePair(e),/LOCAL_CANONICAL_SOURCE_PEER_REQUIRED/);});
test('second MCP fresh rollback flag and aborted genuine parent remain required',async()=>{const e=await evidence();e.qualificationResult.rollbackVerification.connectedFresh=false;await assert.rejects(validatePair(e),/MCP_SECOND_FRESH_ROLLBACK_REQUIRED/);});
test('control parse/truncation faults expose no raw text',()=>{const s=row('{"private":"unterminated}');assert.throws(()=>oneRow(s),/^Error: MCP_CONTROL_JSON_REFUSED$/);s.rows[0].frame='X'.repeat(20001);assert.throws(()=>oneRow(s),/MCP_SMALL_CONTROL_FRAME_REQUIRED/);});
function fakeControls(){const input=new EventEmitter(),processLike=new EventEmitter(),controller=new AbortController();return {input,controller,controls:new FiniteControls(input,controller.signal,processLike)};}
test('finite PREPARE then RELEASE protocol accepts split lines only in expected phase',async()=>{const {controls}=fakeControls();let w=controls.wait('PREPARE');controls.feed('PRE');controls.feed('PARE\r\n');assert.equal(await w,'PREPARE');w=controls.wait('RELEASE');controls.feed('RELEASE\n');assert.equal(await w,'RELEASE');controls.dispose();});
test('wrong/duplicate/oversized control refuses and never admits qualification',async()=>{for(const line of ['RELEASE\n','PREPARE\nPREPARE\n','UNKNOWN\n','X'.repeat(257)]){const {controls}=fakeControls();const w=controls.wait('PREPARE');controls.feed(line);if(line.startsWith('PREPARE'))await w;else await assert.rejects(w,/CONTROL_/);assert.equal(controls.signal.aborted,true);controls.dispose();}});
test('ABORT/EOF/turn-loss stop pending protocol',async()=>{for(const how of ['ABORT','EOF','LOST']){const {controls,input,controller}=fakeControls(),w=controls.wait('PREPARE');if(how==='ABORT')controls.feed('ABORT\n');if(how==='EOF')input.emit('end');if(how==='LOST')controller.abort();await assert.rejects(w,/CONTROL_/);controls.dispose();}});
test('whole sequence budget is180s and work leaves60s cleanup reserve',()=>{assert.equal(BUDGET.workMilliseconds+BUDGET.cleanupReserveMilliseconds,180000);assert.equal(assertElapsed(0,180000),180000);assert.throws(()=>assertElapsed(0,180001),/WHOLE_SEQUENCE_BUDGET_HELD/);assert.throws(()=>assertElapsed(0,120001,BUDGET.workMilliseconds),/WHOLE_SEQUENCE_BUDGET_HELD/);});
test('raw maps/row identities cannot reach any small file even when nested and tiny',()=>{for(const v of [{backgroundRows:{}},{x:{backgroundSnapshotXip:[]}},{x:[{keyHash:'private'}]}])assert.throws(()=>assertNoMaps(v),/RAW_CANONICAL_MAP_PERSISTENCE_REFUSED/);assert.doesNotThrow(()=>assertNoMaps({sql:"select 'keyHash';",businessRelations:[{relation:'public.x',count:0,hash:sha('')}]}));});
test('owned cleanup performs explicit rollback and exact witness proof in one small query',async()=>{let sql;const p=await cleanupWitness({query:async s=>{sql=s;return [JSON.stringify({physicalDatabase:'cloudtms_test_clone',backendPid:100,witnessLocksGone:true})];}},loaded.plan);assert.ok(sql.startsWith('rollback;'));assert.ok(sql.includes("pid=pg_backend_pid()"));assert.equal(p.witnessLocksGone,true);assert.ok(!sql.includes('pg_terminate_backend'));});
test('failed cleanup cannot return proved release',async()=>{await assert.rejects(cleanupWitness({query:async()=>[JSON.stringify({physicalDatabase:'cloudtms_test_clone',backendPid:100,witnessLocksGone:false})]},loaded.plan),/OWNED_HOLDER_LOCK_RELEASE_UNPROVED/);});
test('unauthorized entry refuses before any path/connection',async()=>{await assert.rejects(holdCase({root:'absent',out:'absent',caseId:'contact20-lock1'}),/ROOT_HOLDER_AUTHORIZATION_REQUIRED/);});
test('fixed-reader and whole canonical authority are imported unchanged, never a direct B runner',()=>{const s=fs.readFileSync(path.join(here,'hold-case.mjs'),'utf8');assert.ok(s.includes("../v6-contact-large-frame-feasibility-20261016/fixed-metadata-reader.mjs"));assert.ok(!s.includes('directManagedPsqlSession'));assert.ok(!s.includes("saveSmall(out,'BEFORE"));assert.ok(s.indexOf('beforePacket=await reader.before()')<s.indexOf('await turn.turnConnection.query(`begin'));assert.ok(s.indexOf('cleanupProof=await cleanupWitness')<s.indexOf("status:'PASS_EXPECTED"));});
test('second main may reuse original B PID; only actual fresh verification qualifies',async()=>{
 const e=await evidence();e.qualificationResult.statements[0].rows[0].frame.backendPid=202;
 // This field is actually true in the historical canonical main row. The
 // physical-PID comparison independently refuses its original-B reuse.
 e.qualificationResult.statements[0].rows[0].frame.connectedFresh=true;
 e.qualificationResult.statements[1].rows[0].frame.writerPid=202;
 const oldInput=await oldPrepare(e);
 await assert.rejects(oldValidate({...e,qualificationInput:oldInput}),/REHEARSAL_FIXTURE_ABSENCE_UNPROVED/);
 const s=await validatePair(e);assert.equal(s.readerPids.qualificationMain,202);assert.equal(s.readerPids.qualificationFresh,206);
 assert.equal(s.fixtureQualificationSource,'SECOND_MCP_FRESH_READ_ONLY_VERIFICATION_ONLY');
});
test('both second-tool SQL strings are byte-exact original canonical fixture/held proof',async()=>{
 const e=await evidence(),old=await oldPrepare(e);assert.equal(e.qualificationInput.sql,old.sql);
 assert.equal(e.qualificationInput.verification_sql,old.sql);
 assert.equal(e.qualificationResult.rollbackVerification.statements.length,2);
});
test('main success cannot substitute for absent/empty/error/truncated fresh verification',async()=>{
 for(const mutate of [e=>delete e.qualificationResult.rollbackVerification,
  e=>e.qualificationResult.rollbackVerification.statements=[],
  e=>e.qualificationResult.rollbackVerification.statements[0].error={code:'XX000'},
  e=>e.qualificationResult.rollbackVerification.statements[0].rowsTruncated=true,
  e=>delete e.qualificationResult.rollbackVerification.statements[0].rowsTruncated]){
  const e=await evidence();mutate(e);await assert.rejects(validatePair(e),/MCP_SECOND_FRESH_ROLLBACK_REQUIRED/);
 }
});
test('fresh fixture/held row pairing and all original PID exclusions remain strict',async()=>{
 for(const pid of [100,201,202,204,205]){const e=await evidence();
  e.qualificationResult.rollbackVerification.statements[0].rows[0].frame.backendPid=pid;
  e.qualificationResult.rollbackVerification.statements[1].rows[0].frame.writerPid=pid;
  await assert.rejects(validatePair(e),/MCP_SECOND_VERIFICATION_PID_NOT_FRESH/);
 }
 const e=await evidence();e.qualificationResult.rollbackVerification.statements[1].rows[0].frame.writerPid=207;
 await assert.rejects(validatePair(e),/MCP_FIXTURE_PROOF_HOLDER_REQUIRED/);
});
test('an already closed first-MCP verification physical PID may serve the genuine fresh current verification transaction',async()=>{
 const e=await evidence();e.qualificationResult.rollbackVerification.statements[0].rows[0].frame.backendPid=203;
 e.qualificationResult.rollbackVerification.statements[1].rows[0].frame.writerPid=203;
 const s=await validatePair(e);assert.equal(s.readerPids.qualificationFresh,203);
});
test('fresh physical database, parent XID/status, connectedFresh and fixture absence cannot be waived',async()=>{
 for(const [key,value]of [['database','wrong'],['parentStatus','committed'],['rehearsalXid','106'],['rehearsalBackendPid',999],['connectedFresh',false]]){
  const e=await evidence();e.qualificationResult.rollbackVerification.statements[0].rows[0].frame[key]=value;
  await assert.rejects(validatePair(e),/MCP_SECOND_VERIFICATION_PID_NOT_FRESH|REHEARSAL_FIXTURE_ABSENCE_UNPROVED/);
 }
 const e=await evidence();e.qualificationResult.rollbackVerification.statements[0].rows[0].frame.relations[0].remaining=1;
 await assert.rejects(validatePair(e),/REHEARSAL_FIXTURE_ABSENCE_UNPROVED/);
});
test('second-tool SQL hash, statement count, native flags and source-verification input are pinned',async()=>{
 for(const mutate of [e=>e.qualificationResult.sqlSha256=sha('wrong'),e=>e.qualificationResult.statementCount=1,
  e=>e.qualificationResult.accepted=false,e=>e.qualificationResult.rolledBack=false,
  e=>e.qualificationInput.verification_sql+='\nselect 1;']){
  const e=await evidence();mutate(e);await assert.rejects(validatePair(e),/MCP_FRESH_FIXTURE_RESULT_REQUIRED|MCP_FRESH_INPUT_CHANGED/);
 }
});
test('complete holder has only three import changes and validator has an exact whole-predecessor reverse',()=>{
 const delta=JSON.parse(fs.readFileSync(path.join(here,'DELTA.json'),'utf8'));
 for(const c of delta.changes){const before=fs.readFileSync(path.join(here,c.predecessor),'utf8'),after=fs.readFileSync(path.join(here,c.file),'utf8');
  assert.equal(sha(before),c.beforeSha256);assert.equal(sha(after),c.afterSha256);let s=before;
  for(const h of c.hunks){assert.equal(s.split(h.before).length,2);s=s.replace(h.before,h.after);}assert.equal(s,after);
  for(const h of c.hunks.toReversed()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,h.before);}assert.equal(s,before);
 }
 assert.equal(delta.changes[1].hunks.length,3);assert.equal(delta.unchangedHolderLifecycleAndCleanup,true);
});
test('historical observed pooled-main failure shape is pinned without copying raw frames or treating it as PASS',()=>{
 const p=JSON.parse(fs.readFileSync(path.join(here,'NATIVE_POOL_SHAPE.json'),'utf8'));
 assert.equal(p.qualificationResultSha256,'5c4aa770a0d34f15a1cfbcb22f9747e41f04a8545ae5041c609d6100578d9b39');
 assert.equal(p.mainPidReusedOriginalB,true);assert.equal(p.mainFixtureConnectedFresh,true);assert.equal(p.verificationStatements,0);
 assert.equal(p.qualification,'HELD_NOT_PASS');assert.equal(p.rawPidsRowsOrFramesRetained,false);
 assert.deepEqual(p.mainFixtureKeys,['database','relations','backendPid','parentStatus','rehearsalXid','connectedFresh','rehearsalBackendPid']);
});
