// Validates returned native MCP evidence only. This module opens no connection.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {source,sortWriters,proofSql,requireThat} from './input-builder.mjs';
import {sha} from '../v6-contact-six-lock-contention-20261016/prepare.mjs';
export function oneRow(statement,prefix=''){
 requireThat(statement&&!statement.error&&statement.rowCount===1&&statement.rowsTruncated===false&&statement.rows?.length===1,'MCP_ONE_NATIVE_ROW_REQUIRED');
 const values=Object.values(statement.rows[0]);requireThat(values.length===1,'MCP_ONE_NATIVE_FIELD_REQUIRED');let value=values[0];
 if(typeof value==='string'){requireThat(!prefix||value.startsWith(prefix),'MCP_FRAME_PREFIX_REQUIRED');value=JSON.parse(prefix?value.slice(prefix.length):value);}
 requireThat(value&&typeof value==='object','MCP_NATIVE_OBJECT_REQUIRED');return value;
}
export function readStageOne(result,receipt){
 requireThat(result.database==='cloudtms_test_clone'&&result.databaseTarget==='agency_test'&&result.label==='HR_CONTACT_MCP_'+receipt.caseId.replaceAll('-','_').toUpperCase()
  &&result.sqlSha256===receipt.sqlSha256&&result.statementCount===receipt.statements&&result.accepted===false&&result.rolledBack===true,'MCP_ROLLBACK_ENVELOPE_REFUSED');
 const failed=result.statements?.filter(s=>s.error)??[];requireThat(failed.length===1&&failed[0].statement===receipt.indices.expectedFailedStatement&&failed[0].error.code==='55P03','MCP_EXACT_FINAL_LOCK_REFUSAL_REQUIRED');
 requireThat(result.statements.length===receipt.indices.expectedFailedStatement&&result.statements.every((s,i)=>s.statement===i+1),'MCP_STATEMENT_PREFIX_INCOMPLETE');
 const statement=index=>result.statements[index-1],before=oneRow(statement(receipt.indices.beforeSnapshot)),
  transaction=oneRow(statement(receipt.indices.transaction),'HR_REHEARSAL_TRANSACTION_JSON '),
  observation=oneRow(statement(receipt.indices.fixtureObservation),'HR_FIXTURE_OBSERVATION_JSON ');
 requireThat(transaction.backendPid!==receipt.holderIdentity.backendPid&&observation.rows?.length===0,'MCP_NO_FIXTURE_OBSERVATION_REQUIRED');
 for(const [count,index] of [...receipt.indices.proofs,receipt.indices.targetReady].entries()){
  const proof=oneRow(statement(index));requireThat(proof.writerPid===transaction.backendPid&&proof.holderPid===receipt.holderIdentity.backendPid
   &&proof.holderGranted===true&&proof.precedingGranted===true&&proof.precedingCount===(index===receipt.indices.targetReady?receipt.exactLockCommands.length-1:count+1),'MCP_NATIVE_PRECEDING_LOCK_PROOF_REFUSED');
 }
 const verification=result.rollbackVerification;requireThat(verification?.connectedFresh===true&&verification.database==='cloudtms_test_clone'
  &&verification.statements?.length===3&&verification.statements.every(s=>!s.error),'MCP_FRESH_VERIFICATION_REQUIRED');
 const freshHolder=oneRow(verification.statements[2]);requireThat(freshHolder.holderGranted===true&&freshHolder.holderPid===receipt.holderIdentity.backendPid,'MCP_FRESH_HELD_HOLDER_REQUIRED');
 const after={...oneRow(verification.statements[1]),marker:receipt.marker,connectedFresh:true};
 return {before:{...before,marker:receipt.marker},after,transaction,observations:[observation],expectedUnits:[0]};
}
export async function prepareQualification({root,result,receipt}){
 const {live,plan}=await source(root),stage=readStageOne(result,receipt),row=plan.cases.find(c=>c.id===receipt.caseId);
 const sql=live.freshFixtureProofSql(stage.transaction,stage.observations,stage.before)+'\n'+proofSql(row,receipt.holderIdentity,0);
 return {database:'agency_test',label:'HR_CONTACT_MCP_FRESH_'+receipt.caseId.replaceAll('-','_').toUpperCase(),sql,
  continue_on_error:false,max_rows_per_statement:1,statement_timeout_ms:30000,lock_timeout_ms:2000};
}
export async function validatePair({root,result,receipt,qualificationInput,qualificationResult}){
 const {expected,live,rehearsal}=await source(root),stage=readStageOne(result,receipt);
 const rebuilt=await prepareQualification({root,result,receipt});requireThat(JSON.stringify(rebuilt)===JSON.stringify(qualificationInput),'MCP_FRESH_INPUT_CHANGED');
 requireThat(qualificationResult.accepted===true&&qualificationResult.rolledBack===true&&qualificationResult.database==='cloudtms_test_clone'
  &&qualificationResult.databaseTarget==='agency_test'&&qualificationResult.label===qualificationInput.label
  &&qualificationResult.sqlSha256===sha(qualificationInput.sql)&&qualificationResult.statements?.length===2&&qualificationResult.statements.every(s=>!s.error),'MCP_FRESH_FIXTURE_RESULT_REQUIRED');
 const fixtureProof=oneRow(qualificationResult.statements[0]);
 const held=oneRow(qualificationResult.statements[1]);requireThat(held.holderGranted===true&&held.holderPid===receipt.holderIdentity.backendPid,'MCP_FIXTURE_PROOF_HOLDER_REQUIRED');
 requireThat(JSON.stringify(sortWriters(stage.before.backgroundWriterRoutines))===JSON.stringify(expected.expectedWriters)
  &&JSON.stringify(sortWriters(stage.after.backgroundWriterRoutines))===JSON.stringify(expected.expectedWriters),'MCP_ACTUAL_BACKGROUND_WRITER_DEFINITIONS_CHANGED');
 const writerProof={matched:true,routines:expected.expectedWriters.map(r=>({schema:r.schema,identity:r.identity,definitionSha256:r.definition_sha256})),
  authority:'Exact reviewed source contract writer records equal genuine before/fresh native catalogue; unchanged actual native claim/dequeue/fail owners'};
 stage.after.liveRestoration=live.qualifyLiveRestoration(stage.before,stage.after,{transaction:stage.transaction,observations:stage.observations,
  expectedUnits:stage.expectedUnits,fixtureProof,writerProof});
 const restoration=rehearsal.assertFreshRestoration(stage.before,stage.after,receipt.marker);
 return {version:'CONTACT_EXACT_NOWAIT_MCP_NATIVE_QUALIFICATION_V1',status:'PASS_EXPECTED_EXACT_LOCK_REFUSAL_AND_CANONICAL_LIVE_RESTORATION',
  caseId:receipt.caseId,sqlSha256:receipt.sqlSha256,expectedFailedStatement:receipt.indices.expectedFailedStatement,errorCode:'55P03',
  rolledBack:true,connectedFresh:true,markerAbsent:true,legacy:'LEGACY:1',fixtures:0,backgroundPaused:false,
  otherActivityObserved:{before:stage.before.otherActivityCount,after:stage.after.otherActivityCount},
  restoration,before:stage.before,after:stage.after,originalAssertionsBodyLedgerSent:false,
  route:receipt.route,outerRepeatableReadSnapshotCurrentness:'NOT_QUALIFIED',positiveMigrationBodies:'NOT_RUN',PartB:'NOT_RUN'};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const [mode,root,out]=process.argv.slice(2),read=name=>JSON.parse(fs.readFileSync(path.join(out,name),'utf8'));
 const args={root,result:read('NATIVE_RESULT.json'),receipt:read('INPUT_RECEIPT.json')};
 if(mode==='prepare'){fs.writeFileSync(path.join(out,'QUALIFICATION_INPUT.json'),JSON.stringify(await prepareQualification(args),null,2)+'\n');console.log('FRESH_READ_ONLY_MCP_INPUT_BUILT_NOT_EXECUTED');}
 else if(mode==='validate'){const summary=await validatePair({...args,qualificationInput:read('QUALIFICATION_INPUT.json'),qualificationResult:read('QUALIFICATION_RESULT.json')});fs.writeFileSync(path.join(out,'SUMMARY.json'),JSON.stringify(summary,null,2)+'\n');console.log(JSON.stringify({status:summary.status,caseId:summary.caseId}));}
 else throw Error('EXPLICIT_PREPARE_OR_VALIDATE_REQUIRED');
}
