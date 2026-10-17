// Closed native-shaped evidence validation. No database, file or substitute qualifier.
import {source,requireThat,sha,SCOPE,sortWriters} from './source.mjs';
import {observedInstant} from './timing.mjs';
const positivePid=value=>Number.isSafeInteger(value)&&value>0;
function statementResponse(s,e){
  if(!s||!e||!Number.isSafeInteger(e.statement)||e.statement<1||s.statement!==e.statement||s.error||s.rowsTruncated!==false||s.command!==e.command
   ||JSON.stringify(Object.keys(s).sort())!==JSON.stringify(['command','fields','rowCount','rows','rowsTruncated','statement'])
   ||!Array.isArray(s.rows)||!Array.isArray(s.fields))return false;
  if(e.kind==='EMPTY_COMMAND')return ['SET','DO','CREATE','RESET'].includes(e.command)&&s.rowCount===null&&s.rows.length===0&&s.fields.length===0;
  return e.kind==='ONE_ROW'&&e.command==='SELECT'&&s.rowCount===1&&s.rows.length===1&&s.fields.length===1
   &&typeof s.fields[0]==='string'&&s.fields[0].length>0&&s.fields[0].length<=128
   &&s.rows[0]&&typeof s.rows[0]==='object'&&!Array.isArray(s.rows[0])&&JSON.stringify(Object.keys(s.rows[0]))===JSON.stringify(s.fields);
}
export function statementResponses(rows,expected){
 return Array.isArray(expected)&&Array.isArray(rows)&&rows.length===expected.length
  &&rows.every((s,i)=>expected[i]?.statement===i+1&&statementResponse(s,expected[i]));
}
export function oneField(statement){
 requireThat(statement&&!statement.error&&statement.rowCount===1&&statement.rowsTruncated===false&&statement.rows?.length===1,'ACTOR_MCP_ONE_NATIVE_ROW_REQUIRED');
 const fields=Object.values(statement.rows[0]);requireThat(fields.length===1,'ACTOR_MCP_ONE_NATIVE_FIELD_REQUIRED');return fields[0];
}
export function oneRow(statement,prefix=''){
 let value=oneField(statement);
 if(typeof value==='string'){
  requireThat(Buffer.byteLength(value)<=16384&&(!prefix||value.startsWith(prefix)),'ACTOR_MCP_SMALL_FRAME_REQUIRED');
  try{value=JSON.parse(prefix?value.slice(prefix.length):value);}catch{throw Error('ACTOR_MCP_JSON_REFUSED');}
 }
 requireThat(value&&typeof value==='object'&&!Array.isArray(value),'ACTOR_MCP_OBJECT_REQUIRED');return value;
}
export function nativeClockFrames(result,receipt){
 const frames=[];
 for(const[key,phase]of [['guardBefore','B_GUARD_BEFORE'],['guardAfter','B_GUARD_AFTER']]){
  const index=receipt.indices[key],s=result?.statements?.find(row=>row.statement===index);if(!s||s.error)continue;
  const expected=receipt.responses[index-1];
  requireThat(statementResponse(s,expected),'ACTOR_NATIVE_CLOCK_RESPONSE_REFUSED');
  const value=oneRow(s);requireThat(value.phase===phase&&value.physicalDatabase==='cloudtms_test_clone'
   &&value.databaseOid===receipt.holderIdentity.databaseOid&&positivePid(value.backendPid)
   &&value.holderPid===receipt.holderIdentity.backendPid&&typeof value.holderGranted==='boolean'&&value.readOnly===false,'ACTOR_NATIVE_CLOCK_IDENTITY_REFUSED');
  const times=['transactionStartedAt','statementStartedAt','observedAt'].map(key=>observedInstant(value[key]));
  requireThat(times[0]<=times[1]&&times[1]<=times[2],'ACTOR_NATIVE_CLOCK_ORDER_REFUSED');frames.push(value);
 }
 return frames;
}
export function readStage(result,receipt){
 requireThat(result?.database==='cloudtms_test_clone'&&result.databaseTarget==='agency_test'
  &&result.label==='HR_DAILY_HEALTH_ACTOR_'+receipt.holderIdentity.turnId.slice(8).toUpperCase()
  &&result.sqlSha256===receipt.sqlSha256&&result.statementCount===receipt.statements
  &&result.accepted===true&&result.rolledBack===true&&!result.fatalError,'ACTOR_MCP_ROLLBACK_ENVELOPE_REFUSED');
 requireThat(statementResponses(result.statements,receipt.responses)&&receipt.responses.length===receipt.statements,'ACTOR_MCP_STATEMENT_SEQUENCE_REFUSED');
 const at=key=>result.statements[receipt.indices[key]-1];
 requireThat(at('markerCreation').command==='CREATE'&&at('markerCreation').rowCount===null,'ACTOR_REAL_MARKER_DDL_REQUIRED');
 const transaction=oneRow(at('transaction'),'HR_REHEARSAL_TRANSACTION_JSON '),observation=oneRow(at('fixtureObservation'),'HR_FIXTURE_OBSERVATION_JSON ');
 requireThat(positivePid(transaction.backendPid)&&transaction.backendPid!==receipt.holderIdentity.backendPid
  &&observation.rows?.length===0,'ACTOR_ACTUAL_EMPTY_B_OBSERVATION_REQUIRED');
 const actorResult=oneRow(at('programmeResult'));
 requireThat(JSON.stringify(Object.keys(actorResult).sort())===JSON.stringify(['programme','fixtureRows','matrixChecks','matrixDenials','sameTransactionChanges','constraintRefusals','nullGuardRefusals','directRoleRefusals','nativeWPPP','dailyBasis','postWaitRace','outerIsolation'].sort())
  &&actorResult.programme==='DAILY_HEALTH_ACTOR_NATIVE_V1'&&actorResult.fixtureRows===6&&actorResult.matrixChecks===12&&actorResult.matrixDenials===10
  &&actorResult.sameTransactionChanges===3&&actorResult.constraintRefusals===1&&actorResult.nullGuardRefusals===2&&actorResult.directRoleRefusals===3
  &&actorResult.nativeWPPP===true&&actorResult.dailyBasis==='NOT_RUN'&&actorResult.postWaitRace==='NOT_RUN'&&actorResult.outerIsolation==='read committed','ACTOR_ACTUAL_ASSERTION_COUNTS_REQUIRED');
 const completed=oneRow(at('completion'));
 requireThat(completed.physicalDatabase==='cloudtms_test_clone'&&completed.databaseOid===receipt.holderIdentity.databaseOid
  &&completed.backendPid===transaction.backendPid&&completed.xid===transaction.xid&&completed.marker===receipt.marker
  &&completed.markerPresent===true&&completed.markerRows===0&&completed.holderGranted===true
  &&completed.holderPid===receipt.holderIdentity.backendPid&&completed.noRoleSwitch===true,'ACTOR_REAL_B_MARKER_AND_TURN_REQUIRED');
 const clocks=nativeClockFrames(result,receipt);
 requireThat(clocks.length===2&&clocks.every(c=>c.backendPid===transaction.backendPid&&c.holderGranted===true)
  &&clocks[0].transactionStartedAt===clocks[1].transactionStartedAt
  &&observedInstant(clocks[1].observedAt)>=observedInstant(clocks[0].observedAt)
  &&observedInstant(completed.observedAt)>=observedInstant(clocks[1].observedAt),'ACTOR_NATIVE_GUARD_CLOCKS_REQUIRED');
 const fresh=result.rollbackVerification;
 requireThat(fresh?.connectedFresh===true&&fresh.database==='cloudtms_test_clone'&&receipt.verificationResponses.length===4
  &&statementResponses(fresh.statements,receipt.verificationResponses),'ACTOR_MCP_FRESH_VERIFICATION_REQUIRED');
 const identity=oneRow(fresh.statements[1]),held=oneRow(fresh.statements[2]),users=oneRow(fresh.statements[3]);
 requireThat(JSON.stringify(Object.keys(users).sort())===JSON.stringify(['backendPid','readOnly','userFixtureRowsRemaining'])
  &&users.userFixtureRowsRemaining===0&&users.readOnly===true&&users.backendPid===held.backendPid,'ACTOR_FRESH_USER_FIXTURES_ABSENT_REQUIRED');
 requireThat(identity.physicalDatabase==='cloudtms_test_clone'&&identity.identityTest===true&&identity.customerKeyNull===true
  &&identity.module==='LEGACY:1'&&identity.applyingCount===0&&identity.markerAbsent===true,'ACTOR_MCP_FRESH_MARKER_ABSENCE_REQUIRED');
 requireThat(held.physicalDatabase==='cloudtms_test_clone'&&held.databaseOid===receipt.holderIdentity.databaseOid
  &&held.holderGranted===true&&held.holderPid===receipt.holderIdentity.backendPid&&held.readOnly===true&&held.noRoleSwitch===true
  &&positivePid(held.backendPid)&&held.backendPid!==receipt.holderIdentity.backendPid,'ACTOR_MCP_FRESH_HELD_TURN_REQUIRED');
 for(const field of ['transactionStartedAt','statementStartedAt','observedAt'])observedInstant(held[field]);
 return {transaction,observations:[observation],expectedUnits:[0],freshPeerPid:held.backendPid,nativeClocks:clocks,
  nativeCompletionObservedAt:completed.observedAt,mcpVerificationObservedAt:held.observedAt};
}
export function bindSnapshots({receipt,stage,beforePacket,afterPacket}){
 for(const [packet,phase]of [[beforePacket,'BEFORE'],[afterPacket,'AFTER']]){
  requireThat(packet?.childClosed===true&&packet.provenance?.phase===phase&&packet.provenance.scope===SCOPE
   &&packet.provenance.canonicalSqlSha256===receipt.canonicalSqlSha256&&packet.provenance.observeLiveBusiness===true,'ACTOR_CANONICAL_SOURCE_PEER_REQUIRED');
  const p=packet.peer;
  requireThat(p.database==='cloudtms_test_clone'&&p.databaseOid===receipt.holderIdentity.databaseOid
   &&p.holderPid===receipt.holderIdentity.backendPid&&p.readOnly===true&&p.isolation==='repeatable read'&&p.noRoleSwitch===true
   &&positivePid(p.readerPid)&&p.readerPid!==receipt.holderIdentity.backendPid,'ACTOR_CANONICAL_NATIVE_PEER_REQUIRED');
  const names=packet.snapshot?.businessRelations?.map(r=>r.relation).sort();
  requireThat(JSON.stringify(names)===JSON.stringify(['private.candidate_daily_batch_receipts','public.tms_users','public.ts_financials_outbox'])
   &&packet.snapshot.marker===receipt.marker&&packet.snapshot.markerAbsent===true,'ACTOR_BOTH_COMPLETE_BACKGROUND_SCOPE_REQUIRED');
 }
 const afterPid=afterPacket.peer.readerPid;
 // A fresh MCP client can reuse B's (or the already closed C-before's)
 // physical pooled backend. The independent C-after must still be distinct.
 requireThat(new Set([receipt.holderIdentity.backendPid,stage.transaction.backendPid,beforePacket.peer.readerPid]).size===3,'ACTOR_GENUINE_BEFORE_HOLDER_B_REQUIRED');
 requireThat(![receipt.holderIdentity.backendPid,stage.transaction.backendPid,beforePacket.peer.readerPid,stage.freshPeerPid].includes(afterPid),'ACTOR_DISTINCT_C_AFTER_REQUIRED');
 requireThat(afterPacket.fixtureProof?.backendPid===afterPid&&afterPacket.fixtureProof.connectedFresh===true
  &&afterPacket.provenance.fixtureSource==='UNCHANGED_CANONICAL_FRESH_FIXTURE_PROOF_C_AFTER_THIRD_FRAME','ACTOR_GENUINE_C_AFTER_FIXTURE_PROOF_REQUIRED');
 return {before:beforePacket.snapshot,after:afterPacket.snapshot};
}
export async function validateRestoration({root,result,receipt,beforePacket,afterPacket}){
 const {live,rehearsal,expectedWriters}=await source(root),stage=readStage(result,receipt),local=bindSnapshots({receipt,stage,beforePacket,afterPacket});
 live.validateFixtureObservations(stage.transaction,stage.observations,stage.expectedUnits);
 requireThat(JSON.stringify(sortWriters(local.before.backgroundWriterRoutines))===JSON.stringify(expectedWriters)
  &&JSON.stringify(sortWriters(local.after.backgroundWriterRoutines))===JSON.stringify(expectedWriters),'ACTOR_ACTUAL_BACKGROUND_WRITER_DRIFT');
 const writerProof={matched:true,routines:expectedWriters.map(r=>({schema:r.schema,identity:r.identity,definitionSha256:r.definition_sha256})),
  authority:'Exact original sealed four writer records equal genuine complete C-before and distinct C-after catalogue records'};
 // Use the original live module instance imported by the original rehearsal module.
 // No proof copying or label-based substitute can pass its private WeakMap brand.
 local.after.liveRestoration=live.qualifyLiveRestoration(local.before,local.after,{transaction:stage.transaction,observations:stage.observations,
  expectedUnits:stage.expectedUnits,fixtureProof:afterPacket.fixtureProof,writerProof});
 const restoration=rehearsal.assertFreshRestoration(local.before,local.after,receipt.marker);
 const safe=s=>({ledgerFingerprint:s.ledgerFingerprint,catalogueFingerprint:s.catalogueFingerprint,relationCensus:s.relationCensus,
  businessCountsFingerprint:s.businessCountsFingerprint,businessRowsFingerprint:s.businessRowsFingerprint,businessRelations:s.businessRelations,otherActivityCount:s.otherActivityCount});
 return {status:'ACTOR_PROGRAMME_AND_ORIGINAL_RESTORATION_QUALIFIED_PENDING_SHARED_TURN_CLOSE',scope:SCOPE,route:receipt.route,
  programmeSha256:receipt.programmeSha256,programmeBytes:receipt.programmeBytes,definitions:1,actorMatrixChecks:12,matrixDenials:10,sameTransactionChanges:3,directRoleRefusals:3,nullGuardRefusals:2,
  sqlSha256:receipt.sqlSha256,rolledBack:true,connectedFresh:true,markerAbsent:true,realMarkerCreatedInB:true,businessFixtureRows:6,
  readerPids:{holder:receipt.holderIdentity.backendPid,before:beforePacket.peer.readerPid,nativeB:stage.transaction.backendPid,mcpVerification:stage.freshPeerPid,after:afterPacket.peer.readerPid},
  localTransport:{before:beforePacket.transport,after:afterPacket.transport},before:safe(local.before),after:safe(local.after),restoration,
  nativeClockFrames:stage.nativeClocks,nativeCompletionObservedAt:stage.nativeCompletionObservedAt,mcpVerificationObservedAt:stage.mcpVerificationObservedAt,
  fullInstallRehearsal:'NOT_RUN',dailyComposers:'NOT_RUN',businessCurrentness:'NOT_QUALIFIED',dailyBasis:'NOT_RUN',incompleteReview:'NOT_RUN',isolationRefusal:'NOT_RUN',postWaitRace:'NOT_RUN'};
}
