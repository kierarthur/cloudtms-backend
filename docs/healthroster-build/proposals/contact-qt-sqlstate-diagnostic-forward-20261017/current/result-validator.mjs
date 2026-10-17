// Finite observed result validation; the original live WeakMap qualifier is reused.
import fs from'node:fs';import {fileURLToPath}from'node:url';import path from'node:path';
import {source,requireThat,SCOPE,sortWriters}from'./source.mjs';import {observedInstant}from'./timing.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),plans=JSON.parse(fs.readFileSync(here+'/PROGRAMMES.json')),
 relations=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json')).relationNames;
const pid=v=>Number.isSafeInteger(v)&&v>0;
const ordered=value=>JSON.stringify(Object.fromEntries(Object.entries(value).sort(([a],[b])=>a.localeCompare(b))));
function statementResponse(s,e){
 if(!s||!e||s.statement!==e.statement||s.error||s.rowsTruncated!==false||s.command!==e.command
  ||JSON.stringify(Object.keys(s).sort())!==JSON.stringify(['command','fields','rowCount','rows','rowsTruncated','statement'])||!Array.isArray(s.rows)||!Array.isArray(s.fields))return false;
 if(e.kind==='EMPTY_COMMAND')return ['SET','DO','CREATE','ALTER','REVOKE','GRANT','DROP','LOCK','RESET','NOTIFY'].includes(e.command)&&s.rowCount===null&&s.rows.length===0&&s.fields.length===0;
 return e.kind==='ONE_ROW'&&e.command==='SELECT'&&s.rowCount===1&&s.rows.length===1&&s.fields.length===1&&typeof s.fields[0]==='string'
  &&s.fields[0].length>0&&s.fields[0].length<=128&&s.rows[0]&&typeof s.rows[0]==='object'&&!Array.isArray(s.rows[0])&&JSON.stringify(Object.keys(s.rows[0]))===JSON.stringify(s.fields);
}
export const statementResponses=(rows,expected)=>Array.isArray(rows)&&Array.isArray(expected)&&rows.length===expected.length&&rows.every((s,i)=>expected[i]?.statement===i+1&&statementResponse(s,expected[i]));
function oneRow(s,prefix=''){
 requireThat(s&&!s.error&&s.rowCount===1&&s.rowsTruncated===false&&s.rows?.length===1&&Object.keys(s.rows[0]).length===1,'CONTACT_QT_ONE_NATIVE_FIELD_REQUIRED');let v=Object.values(s.rows[0])[0];
 if(typeof v==='string'){requireThat(Buffer.byteLength(v)<=16384&&(!prefix||v.startsWith(prefix)),'CONTACT_QT_SMALL_NATIVE_FRAME_REQUIRED');try{v=JSON.parse(prefix?v.slice(prefix.length):v);}catch{throw Error('CONTACT_QT_NATIVE_JSON_REFUSED');}}
 requireThat(v&&typeof v==='object'&&!Array.isArray(v),'CONTACT_QT_NATIVE_OBJECT_REQUIRED');return v;
}
export function nativeClockFrames(result,receipt){
 const frames=[];for(const[key,phase]of[['guardBefore','B_GUARD_BEFORE'],['guardAfter','B_GUARD_AFTER']]){
  const index=receipt.indices[key],s=result?.statements?.find(x=>x.statement===index);if(!s||s.error)continue;
  requireThat(statementResponse(s,receipt.responses[index-1]),'CONTACT_QT_CLOCK_RESPONSE_REFUSED');const v=oneRow(s);
  requireThat(v.phase===phase&&v.physicalDatabase==='cloudtms_test_clone'&&v.databaseOid===receipt.holderIdentity.databaseOid&&pid(v.backendPid)
   &&v.holderPid===receipt.holderIdentity.backendPid&&typeof v.holderGranted==='boolean'&&v.readOnly===false,'CONTACT_QT_CLOCK_IDENTITY_REFUSED');
  const times=['transactionStartedAt','statementStartedAt','observedAt'].map(k=>observedInstant(v[k]));requireThat(times[0]<=times[1]&&times[1]<=times[2],'CONTACT_QT_NATIVE_CLOCK_ORDER_REFUSED');frames.push(v);
 }return frames;
}
export function readStage(result,receipt){
 const plan=plans[receipt.caseId];requireThat(plan&&receipt.programmeSha256===plan.sqlSha256&&receipt.programmeBytes===plan.sqlBytes&&ordered(receipt.expectedProgramme)===ordered(plan.expected),'CONTACT_QT_EXACT_SEALED_CASE_REQUIRED');
 requireThat(result?.database==='cloudtms_test_clone'&&result.databaseTarget==='agency_test'&&result.label===receipt.label&&result.sqlSha256===receipt.sqlSha256
  &&result.statementCount===receipt.statements&&result.accepted===true&&result.rolledBack===true&&!result.fatalError,'CONTACT_QT_MCP_ROLLBACK_ENVELOPE_REFUSED');
 requireThat(statementResponses(result.statements,receipt.responses)&&receipt.responses.length===receipt.statements,'CONTACT_QT_MCP_SEQUENCE_REFUSED');
 const at=key=>result.statements[receipt.indices[key]-1];requireThat(at('markerCreation').command==='CREATE'&&at('markerCreation').rowCount===null,'CONTACT_QT_REAL_MARKER_CREATION_REQUIRED');
 const transaction=oneRow(at('transaction'),'HR_REHEARSAL_TRANSACTION_JSON '),observation=oneRow(at('fixtureObservation'),'HR_FIXTURE_OBSERVATION_JSON ');
 requireThat(pid(transaction.backendPid)&&transaction.backendPid!==receipt.holderIdentity.backendPid&&observation.rows?.length===0,'CONTACT_QT_GENUINE_B_EMPTY_BACKGROUND_OBSERVATION_REQUIRED');
 requireThat(ordered(oneRow(at('programmeResult')))===ordered(plan.expected),'CONTACT_QT_ACTUAL_PROGRAMME_ASSERTIONS_REQUIRED');
 const completed=oneRow(at('completion'));requireThat(completed.physicalDatabase==='cloudtms_test_clone'&&completed.databaseOid===receipt.holderIdentity.databaseOid
  &&completed.backendPid===transaction.backendPid&&completed.xid===transaction.xid&&completed.marker===receipt.marker&&completed.markerPresent===true&&completed.markerRows===0
  &&completed.holderGranted===true&&completed.holderPid===receipt.holderIdentity.backendPid&&completed.noRoleSwitch===true,'CONTACT_QT_REAL_MARKER_AND_HELD_TURN_REQUIRED');
 const clocks=nativeClockFrames(result,receipt);requireThat(clocks.length===2&&clocks.every(c=>c.backendPid===transaction.backendPid&&c.holderGranted===true)
  &&clocks[0].transactionStartedAt===clocks[1].transactionStartedAt&&observedInstant(clocks[1].observedAt)>=observedInstant(clocks[0].observedAt)
  &&observedInstant(completed.observedAt)>=observedInstant(clocks[1].observedAt),'CONTACT_QT_NATIVE_GUARD_CLOCKS_REQUIRED');
 const fresh=result.rollbackVerification;requireThat(fresh?.connectedFresh===true&&fresh.database==='cloudtms_test_clone'&&receipt.verificationResponses.length===4
  &&statementResponses(fresh.statements,receipt.verificationResponses),'CONTACT_QT_FRESH_VERIFICATION_REQUIRED');
 const identity=oneRow(fresh.statements[1]),held=oneRow(fresh.statements[2]),fixtures=oneRow(fresh.statements[3]);
 const expectedKeys=['backendPid','readOnly','userFixtureRowsRemaining',...receipt.caseId==='QT_ADMIN_CONTROLS_V1'?['qtObjectsAbsent']:[]].sort();
 requireThat(JSON.stringify(Object.keys(fixtures).sort())===JSON.stringify(expectedKeys)&&fixtures.userFixtureRowsRemaining===0&&fixtures.readOnly===true
  &&fixtures.backendPid===held.backendPid&&(receipt.caseId!=='QT_ADMIN_CONTROLS_V1'||fixtures.qtObjectsAbsent===true),'CONTACT_QT_FRESH_FIXTURE_ABSENCE_REQUIRED');
 requireThat(identity.physicalDatabase==='cloudtms_test_clone'&&identity.identityTest===true&&identity.customerKeyNull===true&&identity.module==='LEGACY:1'
  &&identity.applyingCount===0&&identity.markerAbsent===true,'CONTACT_QT_FRESH_MARKER_ABSENCE_REQUIRED');
 requireThat(held.physicalDatabase==='cloudtms_test_clone'&&held.databaseOid===receipt.holderIdentity.databaseOid&&held.holderGranted===true&&held.holderPid===receipt.holderIdentity.backendPid
  &&held.readOnly===true&&held.noRoleSwitch===true&&pid(held.backendPid)&&held.backendPid!==receipt.holderIdentity.backendPid,'CONTACT_QT_FRESH_HELD_TURN_REQUIRED');
 for(const key of['transactionStartedAt','statementStartedAt','observedAt'])observedInstant(held[key]);
 return{transaction,observations:[observation],expectedUnits:[0],freshPeerPid:held.backendPid,nativeClocks:clocks,nativeCompletionObservedAt:completed.observedAt,mcpVerificationObservedAt:held.observedAt};
}
export function bindSnapshots({receipt,stage,beforePacket,afterPacket}){
 requireThat(JSON.stringify(receipt.snapshotRelations)===JSON.stringify(relations),'CONTACT_QT_EXACT_TWELVE_RELATIONS_REQUIRED');
 for(const[p,phase]of[[beforePacket,'BEFORE'],[afterPacket,'AFTER']]){
  requireThat(p?.childClosed===true&&p.provenance?.phase===phase&&p.provenance.scope===SCOPE&&p.provenance.canonicalSqlSha256===receipt.canonicalSqlSha256&&p.provenance.observeLiveBusiness===true,'CONTACT_QT_ORIGINAL_CANONICAL_SOURCE_REQUIRED');
  const v=p.peer;requireThat(v.database==='cloudtms_test_clone'&&v.databaseOid===receipt.holderIdentity.databaseOid&&v.holderPid===receipt.holderIdentity.backendPid
   &&v.readOnly===true&&v.isolation==='repeatable read'&&v.noRoleSwitch===true&&pid(v.readerPid)&&v.readerPid!==receipt.holderIdentity.backendPid,'CONTACT_QT_CANONICAL_PEER_REQUIRED');
  requireThat(JSON.stringify(p.snapshot.businessRelations.map(r=>r.relation).sort())===JSON.stringify(relations)&&p.snapshot.marker===receipt.marker&&p.snapshot.markerAbsent===true,'CONTACT_QT_COMPLETE_SCOPE_REQUIRED');
 }
 requireThat(new Set([receipt.holderIdentity.backendPid,stage.transaction.backendPid,beforePacket.peer.readerPid]).size===3,'CONTACT_QT_DISTINCT_BEFORE_HOLDER_B_REQUIRED');
 const afterPid=afterPacket.peer.readerPid;requireThat(![receipt.holderIdentity.backendPid,stage.transaction.backendPid,beforePacket.peer.readerPid,stage.freshPeerPid].includes(afterPid),'CONTACT_QT_INDEPENDENT_C_AFTER_REQUIRED');
 requireThat(afterPacket.fixtureProof?.backendPid===afterPid&&afterPacket.fixtureProof.connectedFresh===true&&afterPacket.provenance.fixtureSource==='UNCHANGED_CANONICAL_FRESH_FIXTURE_PROOF_C_AFTER_THIRD_FRAME','CONTACT_QT_ORIGINAL_C_AFTER_FIXTURE_PROOF_REQUIRED');
 return{before:beforePacket.snapshot,after:afterPacket.snapshot};
}
export async function validateRestoration({root,result,receipt,beforePacket,afterPacket}){
 const{live,rehearsal,expectedWriters}=await source(root,receipt.caseId),stage=readStage(result,receipt),local=bindSnapshots({receipt,stage,beforePacket,afterPacket});
 live.validateFixtureObservations(stage.transaction,stage.observations,stage.expectedUnits);
 requireThat(JSON.stringify(sortWriters(local.before.backgroundWriterRoutines))===JSON.stringify(expectedWriters)&&JSON.stringify(sortWriters(local.after.backgroundWriterRoutines))===JSON.stringify(expectedWriters),'CONTACT_QT_ORIGINAL_BACKGROUND_WRITER_DRIFT');
 const writerProof={matched:true,routines:expectedWriters.map(r=>({schema:r.schema,identity:r.identity,definitionSha256:r.definition_sha256})),authority:'Original sealed four writers equal genuine complete C-before and independent C-after catalogue records'};
 local.after.liveRestoration=live.qualifyLiveRestoration(local.before,local.after,{transaction:stage.transaction,observations:stage.observations,expectedUnits:stage.expectedUnits,fixtureProof:afterPacket.fixtureProof,writerProof});
 // Same original module instance and private WeakMap brand; no copied qualifier.
 const restoration=rehearsal.assertFreshRestoration(local.before,local.after,receipt.marker);
 const safe=s=>({ledgerFingerprint:s.ledgerFingerprint,catalogueFingerprint:s.catalogueFingerprint,relationCensus:s.relationCensus,businessCountsFingerprint:s.businessCountsFingerprint,businessRowsFingerprint:s.businessRowsFingerprint,businessRelations:s.businessRelations,otherActivityCount:s.otherActivityCount});
 return{status:'CONTACT_QT_ACTUAL_CASE_AND_ORIGINAL_RESTORATION_QUALIFIED_PENDING_SHARED_TURN_CLOSE',caseId:receipt.caseId,scope:SCOPE,route:receipt.route,
  programmeSha256:receipt.programmeSha256,programmeBytes:receipt.programmeBytes,sqlSha256:receipt.sqlSha256,rolledBack:true,connectedFresh:true,markerAbsent:true,realMarkerCreatedInB:true,
  actualProgrammeAssertions:plans[receipt.caseId].expected,readerPids:{holder:receipt.holderIdentity.backendPid,before:beforePacket.peer.readerPid,nativeB:stage.transaction.backendPid,mcpVerification:stage.freshPeerPid,after:afterPacket.peer.readerPid},
  localTransport:{before:beforePacket.transport,after:afterPacket.transport},before:safe(local.before),after:safe(local.after),restoration,nativeClockFrames:stage.nativeClocks,
  nativeCompletionObservedAt:stage.nativeCompletionObservedAt,mcpVerificationObservedAt:stage.mcpVerificationObservedAt,providerEffects:'NONE_IN_SCOPE',directRoleApi:'NOT_RUN',crossAgencyProof:'NOT_RUN',postWaitRace:'NOT_RUN',fullInstallRehearsal:'NOT_RUN'};
}
