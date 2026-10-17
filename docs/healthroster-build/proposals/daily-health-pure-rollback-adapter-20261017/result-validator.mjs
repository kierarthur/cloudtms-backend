// Closed native-shaped evidence validation. No database, file or substitute qualifier.
import {source,requireThat,sha,SCOPE,sortWriters} from './source.mjs';
const positivePid=value=>Number.isSafeInteger(value)&&value>0;
function statements(rows,count){return Array.isArray(rows)&&rows.length===count&&rows.every((s,i)=>s&&s.statement===i+1
 &&!s.error&&s.rowsTruncated===false&&Number.isSafeInteger(s.rowCount)&&s.rowCount>=0&&Array.isArray(s.rows)&&s.rows.length===s.rowCount);}
export function oneField(statement){
 requireThat(statement&&!statement.error&&statement.rowCount===1&&statement.rowsTruncated===false&&statement.rows?.length===1,'PURE_MCP_ONE_NATIVE_ROW_REQUIRED');
 const fields=Object.values(statement.rows[0]);requireThat(fields.length===1,'PURE_MCP_ONE_NATIVE_FIELD_REQUIRED');return fields[0];
}
export function oneRow(statement,prefix=''){
 let value=oneField(statement);
 if(typeof value==='string'){
  requireThat(Buffer.byteLength(value)<=16384&&(!prefix||value.startsWith(prefix)),'PURE_MCP_SMALL_FRAME_REQUIRED');
  try{value=JSON.parse(prefix?value.slice(prefix.length):value);}catch{throw Error('PURE_MCP_JSON_REFUSED');}
 }
 requireThat(value&&typeof value==='object'&&!Array.isArray(value),'PURE_MCP_OBJECT_REQUIRED');return value;
}
export function readStage(result,receipt){
 requireThat(result?.database==='cloudtms_test_clone'&&result.databaseTarget==='agency_test'
  &&result.label==='HR_DAILY_HEALTH_PURE_'+receipt.holderIdentity.turnId.slice(8).toUpperCase()
  &&result.sqlSha256===receipt.sqlSha256&&result.statementCount===receipt.statements
  &&result.accepted===true&&result.rolledBack===true&&!result.fatalError,'PURE_MCP_ROLLBACK_ENVELOPE_REFUSED');
 requireThat(statements(result.statements,receipt.statements),'PURE_MCP_STATEMENT_SEQUENCE_REFUSED');
 const at=key=>result.statements[receipt.indices[key]-1];
 requireThat(at('markerCreation').rowCount===0,'PURE_REAL_MARKER_DDL_REQUIRED');
 const transaction=oneRow(at('transaction'),'HR_REHEARSAL_TRANSACTION_JSON '),observation=oneRow(at('fixtureObservation'),'HR_FIXTURE_OBSERVATION_JSON ');
 requireThat(positivePid(transaction.backendPid)&&transaction.backendPid!==receipt.holderIdentity.backendPid
  &&observation.rows?.length===0,'PURE_ACTUAL_EMPTY_B_OBSERVATION_REQUIRED');
 requireThat(oneField(at('programmeResult'))==='DAILY_HEALTH_NATIVE_UNICODE_REASON_ROUNDTRIP_ASSERTED','PURE_ORIGINAL_PROGRAMME_ASSERTION_REQUIRED');
 const completed=oneRow(at('completion'));
 requireThat(completed.physicalDatabase==='cloudtms_test_clone'&&completed.databaseOid===receipt.holderIdentity.databaseOid
  &&completed.backendPid===transaction.backendPid&&completed.xid===transaction.xid&&completed.marker===receipt.marker
  &&completed.markerPresent===true&&completed.markerRows===0&&completed.holderGranted===true
  &&completed.holderPid===receipt.holderIdentity.backendPid&&completed.noRoleSwitch===true,'PURE_REAL_B_MARKER_AND_TURN_REQUIRED');
 const fresh=result.rollbackVerification;
 requireThat(fresh?.connectedFresh===true&&fresh.database==='cloudtms_test_clone'&&statements(fresh.statements,3),'PURE_MCP_FRESH_VERIFICATION_REQUIRED');
 const identity=oneRow(fresh.statements[1]),held=oneRow(fresh.statements[2]);
 requireThat(identity.physicalDatabase==='cloudtms_test_clone'&&identity.identityTest===true&&identity.customerKeyNull===true
  &&identity.module==='LEGACY:1'&&identity.applyingCount===0&&identity.markerAbsent===true,'PURE_MCP_FRESH_MARKER_ABSENCE_REQUIRED');
 requireThat(held.physicalDatabase==='cloudtms_test_clone'&&held.databaseOid===receipt.holderIdentity.databaseOid
  &&held.holderGranted===true&&held.holderPid===receipt.holderIdentity.backendPid&&held.noRoleSwitch===true
  &&positivePid(held.backendPid)&&held.backendPid!==transaction.backendPid&&held.backendPid!==receipt.holderIdentity.backendPid,'PURE_MCP_FRESH_HELD_TURN_REQUIRED');
 return {transaction,observations:[observation],expectedUnits:[0],freshPeerPid:held.backendPid};
}
export function bindSnapshots({receipt,stage,beforePacket,afterPacket}){
 for(const [packet,phase]of [[beforePacket,'BEFORE'],[afterPacket,'AFTER']]){
  requireThat(packet?.childClosed===true&&packet.provenance?.phase===phase&&packet.provenance.scope===SCOPE
   &&packet.provenance.canonicalSqlSha256===receipt.canonicalSqlSha256&&packet.provenance.observeLiveBusiness===true,'PURE_CANONICAL_SOURCE_PEER_REQUIRED');
  const p=packet.peer;
  requireThat(p.database==='cloudtms_test_clone'&&p.databaseOid===receipt.holderIdentity.databaseOid
   &&p.holderPid===receipt.holderIdentity.backendPid&&p.readOnly===true&&p.isolation==='repeatable read'&&p.noRoleSwitch===true
   &&positivePid(p.readerPid)&&p.readerPid!==receipt.holderIdentity.backendPid,'PURE_CANONICAL_NATIVE_PEER_REQUIRED');
  const names=packet.snapshot?.businessRelations?.map(r=>r.relation).sort();
  requireThat(JSON.stringify(names)===JSON.stringify(['private.candidate_daily_batch_receipts','public.ts_financials_outbox'])
   &&packet.snapshot.marker===receipt.marker&&packet.snapshot.markerAbsent===true,'PURE_BOTH_COMPLETE_BACKGROUND_SCOPE_REQUIRED');
 }
 const afterPid=afterPacket.peer.readerPid;
 requireThat(new Set([receipt.holderIdentity.backendPid,stage.transaction.backendPid,stage.freshPeerPid,beforePacket.peer.readerPid,afterPid]).size===5,'PURE_DISTINCT_C_AFTER_REQUIRED');
 requireThat(afterPacket.fixtureProof?.backendPid===afterPid&&afterPacket.fixtureProof.connectedFresh===true
  &&afterPacket.provenance.fixtureSource==='UNCHANGED_CANONICAL_FRESH_FIXTURE_PROOF_C_AFTER_THIRD_FRAME','PURE_GENUINE_C_AFTER_FIXTURE_PROOF_REQUIRED');
 return {before:beforePacket.snapshot,after:afterPacket.snapshot};
}
export async function validateRestoration({root,result,receipt,beforePacket,afterPacket}){
 const {live,rehearsal,expectedWriters}=await source(root),stage=readStage(result,receipt),local=bindSnapshots({receipt,stage,beforePacket,afterPacket});
 live.validateFixtureObservations(stage.transaction,stage.observations,stage.expectedUnits);
 requireThat(JSON.stringify(sortWriters(local.before.backgroundWriterRoutines))===JSON.stringify(expectedWriters)
  &&JSON.stringify(sortWriters(local.after.backgroundWriterRoutines))===JSON.stringify(expectedWriters),'PURE_ACTUAL_BACKGROUND_WRITER_DRIFT');
 const writerProof={matched:true,routines:expectedWriters.map(r=>({schema:r.schema,identity:r.identity,definitionSha256:r.definition_sha256})),
  authority:'Exact original sealed four writer records equal genuine complete C-before and distinct C-after catalogue records'};
 // Use the original live module instance imported by the original rehearsal module.
 // No proof copying or label-based substitute can pass its private WeakMap brand.
 local.after.liveRestoration=live.qualifyLiveRestoration(local.before,local.after,{transaction:stage.transaction,observations:stage.observations,
  expectedUnits:stage.expectedUnits,fixtureProof:afterPacket.fixtureProof,writerProof});
 const restoration=rehearsal.assertFreshRestoration(local.before,local.after,receipt.marker);
 const safe=s=>({ledgerFingerprint:s.ledgerFingerprint,catalogueFingerprint:s.catalogueFingerprint,relationCensus:s.relationCensus,
  businessCountsFingerprint:s.businessCountsFingerprint,businessRowsFingerprint:s.businessRowsFingerprint,businessRelations:s.businessRelations,otherActivityCount:s.otherActivityCount});
 return {status:'PURE_PROGRAMME_AND_ORIGINAL_RESTORATION_QUALIFIED_PENDING_SHARED_TURN_CLOSE',scope:SCOPE,route:receipt.route,
  programmeSha256:receipt.programmeSha256,programmeBytes:8029,definitions:5,positiveUnicodeVectors:6,malformedStoredReasonRefusals:8,
  sqlSha256:receipt.sqlSha256,rolledBack:true,connectedFresh:true,markerAbsent:true,realMarkerCreatedInB:true,businessFixtureRows:0,
  readerPids:{holder:receipt.holderIdentity.backendPid,before:beforePacket.peer.readerPid,nativeB:stage.transaction.backendPid,mcpVerification:stage.freshPeerPid,after:afterPacket.peer.readerPid},
  localTransport:{before:beforePacket.transport,after:afterPacket.transport},before:safe(local.before),after:safe(local.after),restoration,
  fullInstallRehearsal:'NOT_RUN',dailyComposers:'NOT_RUN',businessCurrentness:'NOT_QUALIFIED'};
}
