// Pure native-evidence validator; no connection/file/capability substitute.
import {source,proofSql,sortWriters,sha,requireThat} from './input-builder.mjs';
export function oneRow(statement,prefix=''){
 requireThat(statement&&!statement.error&&statement.rowCount===1&&statement.rowsTruncated===false&&statement.rows?.length===1,'MCP_ONE_NATIVE_ROW_REQUIRED');
 const fields=Object.values(statement.rows[0]);requireThat(fields.length===1,'MCP_ONE_NATIVE_FIELD_REQUIRED');let value=fields[0];
 if(typeof value==='string'){
  requireThat(Buffer.byteLength(value)<=16384&&(!prefix||value.startsWith(prefix)),'MCP_SMALL_CONTROL_FRAME_REQUIRED');
  try{value=JSON.parse(prefix?value.slice(prefix.length):value);}catch{throw Error('MCP_CONTROL_JSON_REFUSED');}
 }
 requireThat(value&&typeof value==='object'&&!Array.isArray(value),'MCP_NATIVE_OBJECT_REQUIRED');return value;
}
function closedFinalError(s){
 // Actual server error rows contain ONLY statement/error, not a result payload
 // and not an invented rowsTruncated=false. Pin the observed error vocabulary.
 if(JSON.stringify(Object.keys(s).sort())!==JSON.stringify(['error','statement']))return false;
 const e=s.error;
 return e&&typeof e==='object'&&!Array.isArray(e)&&JSON.stringify(Object.keys(e).sort())===JSON.stringify(['code','message','routine'])
  &&typeof e.code==='string'&&/^[A-Z0-9]{5}$/.test(e.code)&&typeof e.message==='string'&&Buffer.byteLength(e.message)<=2048
  &&typeof e.routine==='string'&&/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(e.routine);
}
function statementSequence(rows,count,finalError=false){return Array.isArray(rows)&&rows.length===count&&rows.every((s,i)=>s&&typeof s==='object'&&!Array.isArray(s)&&s.statement===i+1
 &&(finalError&&i===count-1?closedFinalError(s):!s.error&&s.rowsTruncated===false));}
export function readStageOne(result,receipt){
 requireThat(result.database==='cloudtms_test_clone'&&result.databaseTarget==='agency_test'&&result.label==='HR_CONTACT_MCP_'+receipt.caseId.replaceAll('-','_').toUpperCase()
  &&result.sqlSha256===receipt.sqlSha256&&result.statementCount===receipt.statements&&result.accepted===false&&result.rolledBack===true,'MCP_ROLLBACK_ENVELOPE_REFUSED');
 requireThat(statementSequence(result.statements,receipt.indices.expectedFailedStatement,true),'MCP_STATEMENT_PREFIX_INCOMPLETE');
 const failed=result.statements.filter(s=>s.error);requireThat(failed.length===1&&failed[0].statement===receipt.indices.expectedFailedStatement&&failed[0].error.code==='55P03','MCP_EXACT_FINAL_LOCK_REFUSAL_REQUIRED');
 requireThat(closedFinalError({statement:failed[0].statement,error:result.fatalError})
  &&['code','message','routine'].every(k=>result.fatalError[k]===failed[0].error[k]),'MCP_EXACT_EXPECTED_FATAL_ERROR_REQUIRED');
 const at=i=>result.statements[i-1],transaction=oneRow(at(receipt.indices.transaction),'HR_REHEARSAL_TRANSACTION_JSON '),
  observation=oneRow(at(receipt.indices.fixtureObservation),'HR_FIXTURE_OBSERVATION_JSON ');
 requireThat(transaction.backendPid!==receipt.holderIdentity.backendPid&&observation.rows?.length===0,'MCP_NO_FIXTURE_OBSERVATION_REQUIRED');
 for(const [count,index]of [...receipt.indices.proofs,receipt.indices.targetReady].entries()){
  const proof=oneRow(at(index));requireThat(proof.writerPid===transaction.backendPid&&proof.holderPid===receipt.holderIdentity.backendPid
   &&proof.holderGranted===true&&proof.precedingGranted===true&&proof.precedingCount===(index===receipt.indices.targetReady?receipt.exactLockCommands.length-1:count+1),'MCP_NATIVE_PRECEDING_LOCK_PROOF_REFUSED');
 }
 const fresh=result.rollbackVerification;requireThat(fresh?.connectedFresh===true&&fresh.database==='cloudtms_test_clone'
  &&statementSequence(fresh.statements,3)&&fresh.statements.every(s=>!s.error),'MCP_FRESH_VERIFICATION_REQUIRED');
 const identity=oneRow(fresh.statements[1]),held=oneRow(fresh.statements[2]);
 requireThat(identity.physicalDatabase==='cloudtms_test_clone'&&identity.identityTest===true&&identity.customerKeyNull===true
  &&identity.module==='LEGACY:1'&&identity.applyingCount===0&&identity.markerAbsent===true,'MCP_FRESH_TEST_LEGACY_MARKER_REQUIRED');
 requireThat(held.holderGranted===true&&held.holderPid===receipt.holderIdentity.backendPid&&held.precedingCount===0&&held.precedingGranted===true
  &&Number.isSafeInteger(held.writerPid)&&held.writerPid>0&&held.writerPid!==transaction.backendPid&&held.writerPid!==receipt.holderIdentity.backendPid,'MCP_FRESH_HELD_HOLDER_REQUIRED');
 return {transaction,observations:[observation],expectedUnits:[0],freshPeerPid:held.writerPid};
}
export function bindLocalSnapshots({receipt,stage,beforePacket,afterPacket}){
 for(const [packet,phase]of [[beforePacket,'BEFORE'],[afterPacket,'AFTER']]){
  requireThat(packet?.childClosed===true&&packet.provenance?.phase===phase&&packet.provenance.caseId===receipt.caseId
   &&packet.provenance.canonicalSqlSha256===receipt.beforeCanonicalSqlSha256&&packet.provenance.observeLiveBusiness===true,'LOCAL_CANONICAL_SOURCE_PEER_REQUIRED');
  const p=packet.peer;requireThat(p.database==='cloudtms_test_clone'&&p.databaseOid===receipt.holderIdentity.databaseOid&&p.holderPid===receipt.holderIdentity.backendPid
   &&p.readOnly===true&&p.isolation==='repeatable read'&&p.noRoleSwitch===true&&Number.isSafeInteger(p.readerPid)&&p.readerPid>0
   &&p.readerPid!==receipt.holderIdentity.backendPid,'LOCAL_CANONICAL_NATIVE_PEER_REQUIRED');
 }
 requireThat(afterPacket.peer.readerPid!==beforePacket.peer.readerPid&&afterPacket.peer.readerPid!==stage.transaction.backendPid
  &&afterPacket.peer.readerPid!==stage.freshPeerPid,'LOCAL_AFTER_NOT_FRESH');
 return {before:beforePacket.snapshot,after:afterPacket.snapshot};
}
export async function prepareQualification({root,result,receipt,beforePacket,afterPacket}){
 const {live,plan}=await source(root),stage=readStageOne(result,receipt),local=bindLocalSnapshots({receipt,stage,beforePacket,afterPacket});
 live.validateFixtureObservations(stage.transaction,stage.observations,stage.expectedUnits);
 const row=plan.cases.find(c=>c.id===receipt.caseId),text=live.freshFixtureProofSql(stage.transaction,stage.observations,local.before)+'\n'+proofSql(row,receipt.holderIdentity,0);
 requireThat(Buffer.byteLength(text)<65536,'SMALL_GENUINE_FIXTURE_PROOF_REQUIRED');
 return {database:'agency_test',label:'HR_CONTACT_MCP_FRESH_'+receipt.caseId.replaceAll('-','_').toUpperCase(),sql:text,
  continue_on_error:false,max_rows_per_statement:1,statement_timeout_ms:30000,lock_timeout_ms:2000};
}
export async function validatePair({root,result,receipt,beforePacket,afterPacket,qualificationInput,qualificationResult}){
 const {live,rehearsal,expected}=await source(root),stage=readStageOne(result,receipt),local=bindLocalSnapshots({receipt,stage,beforePacket,afterPacket});
 const rebuilt=await prepareQualification({root,result,receipt,beforePacket,afterPacket});
 requireThat(JSON.stringify(rebuilt)===JSON.stringify(qualificationInput),'MCP_FRESH_INPUT_CHANGED');
 requireThat(qualificationResult.accepted===true&&qualificationResult.rolledBack===true&&qualificationResult.database==='cloudtms_test_clone'
  &&qualificationResult.databaseTarget==='agency_test'&&qualificationResult.label===qualificationInput.label&&qualificationResult.sqlSha256===sha(qualificationInput.sql)
  &&!qualificationResult.fatalError&&statementSequence(qualificationResult.statements,2)&&qualificationResult.statements.every(s=>!s.error),'MCP_FRESH_FIXTURE_RESULT_REQUIRED');
 requireThat(qualificationResult.rollbackVerification?.connectedFresh===true&&qualificationResult.rollbackVerification.database==='cloudtms_test_clone','MCP_SECOND_FRESH_ROLLBACK_REQUIRED');
 const fixtureProof=oneRow(qualificationResult.statements[0]),held=oneRow(qualificationResult.statements[1]);
 requireThat(held.holderGranted===true&&held.holderPid===receipt.holderIdentity.backendPid&&held.precedingGranted===true&&held.precedingCount===0
  &&held.writerPid===fixtureProof.backendPid,'MCP_FIXTURE_PROOF_HOLDER_REQUIRED');
 requireThat(JSON.stringify(sortWriters(local.before.backgroundWriterRoutines))===JSON.stringify(expected.expectedWriters)
  &&JSON.stringify(sortWriters(local.after.backgroundWriterRoutines))===JSON.stringify(expected.expectedWriters),'MCP_ACTUAL_BACKGROUND_WRITER_DEFINITIONS_CHANGED');
 const writerProof={matched:true,routines:expected.expectedWriters.map(r=>({schema:r.schema,identity:r.identity,definitionSha256:r.definition_sha256})),
  authority:'Exact reviewed four source records equal complete genuine native C-before and fresh C-after catalogue records'};
 local.after.liveRestoration=live.qualifyLiveRestoration(local.before,local.after,{transaction:stage.transaction,observations:stage.observations,
  expectedUnits:stage.expectedUnits,fixtureProof,writerProof});
 const restoration=rehearsal.assertFreshRestoration(local.before,local.after,receipt.marker);
 const safe=s=>({ledgerFingerprint:s.ledgerFingerprint,catalogueFingerprint:s.catalogueFingerprint,relationCensus:s.relationCensus,
  businessCountsFingerprint:s.businessCountsFingerprint,businessRowsFingerprint:s.businessRowsFingerprint,businessRelations:s.businessRelations,
  otherActivityCount:s.otherActivityCount});
 return {version:'CONTACT_FIXED_LOCAL_METADATA_MCP_QUALIFICATION_V1',status:'EXPECTED_EXACT_LOCK_AND_FULL_CANONICAL_RESTORATION_QUALIFIED_PENDING_OWNED_CLOSE',
  caseId:receipt.caseId,sqlSha256:receipt.sqlSha256,expectedFailedStatement:receipt.indices.expectedFailedStatement,errorCode:'55P03',
  rolledBack:true,connectedFresh:true,markerAbsent:true,legacy:'LEGACY:1',fixtures:0,backgroundPaused:false,
  readerPids:{before:beforePacket.peer.readerPid,after:afterPacket.peer.readerPid,nativeB:stage.transaction.backendPid,holder:receipt.holderIdentity.backendPid},
  canonicalSqlSha256:receipt.beforeCanonicalSqlSha256,localTransport:{before:beforePacket.transport,after:afterPacket.transport},
  before:safe(local.before),after:safe(local.after),restoration,originalAssertionsBodyLedgerSent:false,route:receipt.route,
  outerRepeatableReadSnapshotCurrentness:'NOT_QUALIFIED',positiveMigrationBodies:'NOT_RUN',PartB:'NOT_RUN'};
}
