// Pure synthetic boundary vectors against the exact frozen exported validator.
// No native result, database, socket, holder, marker or fixture is produced.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
import {bindSnapshots} from './frozen/forward/current/result-validator.mjs';
import {SCOPE} from './frozen/forward/current/source.mjs';
import {BUDGET} from './frozen/forward/current/control.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),pins=JSON.parse(fs.readFileSync(path.join(here,'frozen/forward/current/SOURCE_PINS.json'))),relations=pins.relationNames;
assert.equal(relations.length,12);assert.equal(BUDGET.workMilliseconds,120000);assert.equal(BUDGET.cleanupReserveMilliseconds,60000);assert.equal(BUDGET.wholeMilliseconds,180000);
const receipt={snapshotRelations:relations,holderIdentity:{databaseOid:17,backendPid:101},canonicalSqlSha256:'f'.repeat(64),marker:'synthetic-only-not-created'};
const stage={transaction:{backendPid:102},freshPeerPid:105};
const packet=(phase,readerPid)=>({childClosed:true,provenance:{phase,scope:SCOPE,canonicalSqlSha256:receipt.canonicalSqlSha256,observeLiveBusiness:true,...phase==='AFTER'?{fixtureSource:'UNCHANGED_CANONICAL_FRESH_FIXTURE_PROOF_C_AFTER_THIRD_FRAME'}:{}},peer:{database:'cloudtms_test_clone',databaseOid:17,holderPid:101,readerPid,readOnly:true,isolation:'repeatable read',noRoleSwitch:true},snapshot:{businessRelations:relations.map(relation=>({relation})),marker:receipt.marker,markerAbsent:true},...phase==='AFTER'?{fixtureProof:{backendPid:readerPid,connectedFresh:true}}:{}});
const input=()=>({receipt:structuredClone(receipt),stage:structuredClone(stage),beforePacket:packet('BEFORE',103),afterPacket:packet('AFTER',104)}),cases=[];
bindSnapshots(input());cases.push({case:'closed independent physical identities',expected:'ACCEPT_SYNTHETIC_FRAME_ONLY'});
function refusal(name,edit){const v=input();edit(v);assert.throws(()=>bindSnapshots(v));cases.push({case:name,expected:'REFUSE'});}
for(const [label,pid]of [['holder A',101],['native B',102],['C-before',103],['MCP verification',105]])refusal('C-after equals '+label,v=>{v.afterPacket.peer.readerPid=pid;v.afterPacket.fixtureProof.backendPid=pid;});
for(const [label,pid]of [['holder A',101],['native B',102]])refusal('C-before equals '+label,v=>v.beforePacket.peer.readerPid=pid);
refusal('after fixture proof belongs to another peer',v=>v.afterPacket.fixtureProof.backendPid=106);
refusal('after fixture proof is not fresh',v=>v.afterPacket.fixtureProof.connectedFresh=false);
refusal('copied or unclosed C-before',v=>v.beforePacket.childClosed=false);
refusal('copied or unclosed C-after',v=>v.afterPacket.childClosed=false);
refusal('missing complete scope',v=>v.afterPacket.snapshot.businessRelations.pop());
refusal('duplicate relation instead of genuine twelfth',v=>v.afterPacket.snapshot.businessRelations[11]=v.afterPacket.snapshot.businessRelations[0]);
refusal('altered canonical query hash',v=>v.afterPacket.provenance.canonicalSqlSha256='0'.repeat(64));
refusal('wrong snapshot scope',v=>v.beforePacket.provenance.scope='OTHER');
refusal('writable C-after',v=>v.afterPacket.peer.readOnly=false);
refusal('role-switched C-after',v=>v.afterPacket.peer.noRoleSwitch=false);
refusal('not repeatable-read C-after',v=>v.afterPacket.peer.isolation='read committed');
refusal('stale marker present',v=>v.afterPacket.snapshot.markerAbsent=false);
refusal('copied fixture proof provenance',v=>v.afterPacket.provenance.fixtureSource='COPIED');
const report={version:'CONTACT_QT_INDEPENDENT_PHYSICAL_FRAME_BOUNDARIES_V1',cases,vectorCount:cases.length,actualExport:'bindSnapshots',syntheticOnly:true,native:'NOT_RUN',noRestorationQualificationClaim:true};
fs.writeFileSync(path.join(here,'INDEPENDENT_BOUNDARY_RESULTS.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({syntheticBoundaryVectors:cases.length,native:'NOT_RUN'}));
