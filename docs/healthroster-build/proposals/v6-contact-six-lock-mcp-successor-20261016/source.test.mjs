import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {buildInput,source} from './input-builder.mjs';
import {readStageOne,prepareQualification,validatePair} from './result-validator.mjs';
import {holdCase} from './hold-case.mjs';
import {sha} from '../v6-contact-six-lock-contention-20261016/prepare.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root=JSON.parse(fs.readFileSync(path.join(here,'../v6-contact-six-lock-contention-20261016/PLAN.json'),'utf8')).root;
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:20,backendPid:100,turnId:'hr_turn_'+'0'.repeat(32),key:[20261013,1]};
const loaded=await source(root);
for(const row of loaded.plan.cases)test(row.id+' builds only exact locks on server rollback route and final target is last',async()=>{
 const {input,receipt}=await buildInput({root,caseId:row.id,identity});
 const scan=loaded.sql.scanManagedSql(input.sql);assert.equal(scan.statements.length,receipt.statements);
 assert.equal(scan.statements.at(-1).start,input.sql.lastIndexOf(row.commands.at(-1)));
 assert.equal(input.sql.slice(scan.statements.at(-1).start),row.commands.at(-1));
 assert.ok(scan.statements.every(s=>['SET','DO','SELECT','WITH','RESET','LOCK'].includes(s.tokens[0])));
 assert.ok(!input.sql.includes('CLOUDTMS_MANAGED_WRITER_GUARD_LOST'));
 assert.deepEqual(scan.statements.filter(s=>s.tokens[0]==='LOCK').map(s=>input.sql.slice(s.start,s.end)),row.commands.slice(2));
 assert.equal(receipt.route,'mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback');
 assert.equal(input.continue_on_error,false);assert.equal(receipt.observeLiveBusiness,true);
});
function nativeRow(value,prefix=''){return{command:'SELECT',rowCount:1,rowsTruncated:false,fields:['frame'],rows:[{frame:prefix?prefix+JSON.stringify(value):value}]};}
async function nativeEvidence(){
 const {input,receipt}=await buildInput({root,caseId:'contact30-lock4',identity});
 const businessRelations=[...loaded.plan.relationNames,...loaded.live.backgroundRelations].map(relation=>({relation,count:0,hash:sha('')}));
 const base={physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',applyingCount:0,markerAbsent:true,
  ledgerFingerprint:sha('ledger'),catalogueFingerprint:sha('catalogue'),businessCountsFingerprint:sha('counts'),businessRowsFingerprint:sha('rows'),relationCensus:sha('relations'),
  businessRelations,backgroundRows:Object.fromEntries(loaded.live.backgroundRelations.map(r=>[r,[]])),backgroundSnapshotEpochZero:true,
  backgroundSnapshotXmax:'100',backgroundSnapshotXip:[],backgroundSnapshotStartedAt:'2026-10-16T12:00:00Z',backgroundObservedAt:'2026-10-16T12:00:00Z',
  backgroundWriterRoutines:loaded.expected.expectedWriters,otherActivityCount:7};
 const before=structuredClone(base),after={...structuredClone(base),otherActivityCount:9,backgroundSnapshotXmax:'110',backgroundObservedAt:'2026-10-16T12:00:02Z'};
 const transaction={database:'cloudtms_test_clone',backendPid:202,xid:'105',epochZero:true};
 const observation={unit:0,...transaction,rows:[]};
 const proof={holderPid:100,writerPid:202,holderGranted:true,precedingCount:3,precedingGranted:true};
 const statements=Array.from({length:receipt.statements},(_,i)=>({statement:i+1,command:'SET',rowCount:null,fields:[],rows:[],rowsTruncated:false}));
 const put=(index,row)=>statements[index-1]={statement:index,...row};
 put(receipt.indices.beforeSnapshot,nativeRow(before));put(receipt.indices.transaction,nativeRow(transaction,'HR_REHEARSAL_TRANSACTION_JSON '));
 put(receipt.indices.fixtureObservation,nativeRow(observation,'HR_FIXTURE_OBSERVATION_JSON '));
 for(const [index,i] of receipt.indices.proofs.entries())put(i,nativeRow({...proof,precedingCount:index+1}));
 put(receipt.indices.targetReady,nativeRow(proof));
 put(receipt.indices.expectedFailedStatement,{error:{code:'55P03'}});
 const result={accepted:false,databaseTarget:'agency_test',database:'cloudtms_test_clone',label:input.label,sqlSha256:receipt.sqlSha256,statementCount:receipt.statements,statements,rolledBack:true,
  rollbackVerification:{connectedFresh:true,database:'cloudtms_test_clone',statements:[{statement:1,...nativeRow('pg_catalog, public')},{statement:2,...nativeRow(after)},{statement:3,...nativeRow(proof)}]}};
 const qualificationInput=await prepareQualification({root,result,receipt});
 const fixtureProof={database:'cloudtms_test_clone',backendPid:203,connectedFresh:true,rehearsalBackendPid:202,rehearsalXid:'105',parentStatus:'aborted',
  relations:loaded.live.backgroundRelations.map(relation=>({relation,observed_fixture_keys:0,remaining:0}))};
 const qualificationResult={accepted:true,rolledBack:true,databaseTarget:'agency_test',database:'cloudtms_test_clone',label:qualificationInput.label,sqlSha256:sha(qualificationInput.sql),
  statements:[{statement:1,...nativeRow(fixtureProof)},{statement:2,...nativeRow(proof)}]};
 return{root,input,result,receipt,qualificationInput,qualificationResult};
}
test('canonical branded live proof admits unchanged fingerprints with active background sessions',async()=>{
 const e=await nativeEvidence(),summary=await validatePair(e);assert.equal(summary.status,'PASS_EXPECTED_EXACT_LOCK_REFUSAL_AND_CANONICAL_LIVE_RESTORATION');
 assert.deepEqual(summary.otherActivityObserved,{before:7,after:9});assert.equal(summary.backgroundPaused,false);
 assert.equal(summary.restoration.liveRestoration.status,'LIVE_TEST_RESTORATION_QUALIFIED');
});
test('changed witness data is not waived by background activity',async()=>{
 const e=await nativeEvidence();e.result.rollbackVerification.statements[1].rows[0].frame.businessRelations[0].hash=sha('changed');
 await assert.rejects(validatePair(e),/REHEARSAL_UNATTRIBUTED_BUSINESS_CHANGE/);
});
test('native background writer drift is refused',async()=>{
 const e=await nativeEvidence();e.result.rollbackVerification.statements[1].rows[0].frame.backgroundWriterRoutines[0].definition_sha256=sha('drift');
 await assert.rejects(validatePair(e),/MCP_ACTUAL_BACKGROUND_WRITER_DEFINITIONS_CHANGED/);
});
test('missing target error or earlier refusal cannot receive target credit',async()=>{
 const e=await nativeEvidence();e.result.statements.at(-1).error.code='42501';assert.throws(()=>readStageOne(e.result,e.receipt),/MCP_EXACT_FINAL_LOCK_REFUSAL_REQUIRED/);
});
test('fresh physical rollback proof cannot be replaced by a saved status',async()=>{
 const e=await nativeEvidence();e.result.rollbackVerification.connectedFresh=false;assert.throws(()=>readStageOne(e.result,e.receipt),/MCP_FRESH_VERIFICATION_REQUIRED/);
});
test('forged live status cannot mint existing canonical WeakMap qualification',async()=>{
 const e=await nativeEvidence(),stage=readStageOne(e.result,e.receipt);stage.after.liveRestoration={status:'LIVE_TEST_RESTORATION_QUALIFIED'};
 assert.throws(()=>loaded.rehearsal.assertFreshRestoration(stage.before,stage.after,e.receipt.marker),/REHEARSAL_LIVE_RESTORATION_UNBRANDED_OR_STALE/);
});
test('holder-only coordinator rejects unauthorized entry before opening any route',async()=>{
 await assert.rejects(holdCase({root:'absent',out:'absent',caseId:'contact20-lock1'}),/ROOT_HOLDER_AUTHORIZATION_REQUIRED/);
});
