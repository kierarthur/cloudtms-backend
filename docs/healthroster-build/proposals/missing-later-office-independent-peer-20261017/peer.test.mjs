import fs from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import assert from 'node:assert/strict';
const sources=JSON.parse(fs.readFileSync(new URL('./COUNTER_SOURCES.json',import.meta.url)));
const original=vm.runInNewContext(sources.originalAccepted+'\naccepted;');
const decode=s=>vm.runInNewContext(s+'\n({bundleRevision,nextBundleRevision,isLaterChangeDecisionResult});');
const held=decode(sources.heldCad4),fixed=decode(sources.corrected353);
const id='10000000-0000-0000-0000-000000000001';
const payload=(decision,revision)=>({decision,decision_bundle_id:id,bundle_revision:revision});
const result=(p,revision)=>({ok:true,idempotent_replay:false,decision:p.decision,decision_bundle_id:id,
 retained:false,published:true,reviewed_bundle_revision:p.bundle_revision,bundle_revision:revision});
test('preserved finding: original accepts numeric KEEP successor, cad4 refuses, exact353 restores it',()=>{
 const p=payload('KEEP_CURRENTLY_APPROVED_HOURS',3),r=result(p,4);
 assert.equal(original(r,{command:{body:{payload:p}}}),true);
 assert.equal(held.isLaterChangeDecisionResult(r,p),false);
 assert.equal(fixed.isLaterChangeDecisionResult(r,p),true);
});
test('both native successor decisions preserve genuine pending/published and replay outcomes at int63 extremes',()=>{
 for(const decision of ['KEEP_CURRENTLY_APPROVED_HOURS','APPROVE_UPDATED_HOURS'])
 for(const [a,b] of [[3,4],['9007199254740993','9007199254740994'],['9223372036854775806','9223372036854775807']])
 for(const replay of [false,true])for(const published of [false,true]){
  const p=payload(decision,a),r={...result(p,b),published,idempotent_replay:replay};
  assert.equal(fixed.isLaterChangeDecisionResult(r,p),true);
 }
});
test('foreign decisions/IDs, unsafe counters and false native outcomes never settle a retained intent',()=>{
 const p=payload('KEEP_CURRENTLY_APPROVED_HOURS','9007199254740993'),r=result(p,'9007199254740994');
 for(const delta of [{ok:false},{retained:true},{published:'true'},{idempotent_replay:1},
  {decision:'APPROVE_UPDATED_HOURS'},{decision_bundle_id:'foreign'},
  {reviewed_bundle_revision:'9007199254740992'},{reviewed_bundle_revision:9007199254740992},
  {bundle_revision:9007199254740994},{bundle_revision:'09007199254740994'},
  {bundle_revision:'9007199254740993'},{bundle_revision:'9223372036854775808'}])
  assert.equal(fixed.isLaterChangeDecisionResult({...r,...delta},p),false);
 assert.equal(fixed.isLaterChangeDecisionResult(result(payload('UNKNOWN','3'),'4'),payload('UNKNOWN','3')),false);
});
test('retained max KEEP stays representable; successor max overflow and APPROVE retained outcome refuse',()=>{
 const p=payload('KEEP_CURRENTLY_APPROVED_HOURS','9223372036854775807');
 const r={...result(p,p.bundle_revision),retained:true,published:false};
 assert.equal(fixed.isLaterChangeDecisionResult(r,p),true);
 assert.equal(fixed.nextBundleRevision(p.bundle_revision),null);
 assert.equal(fixed.isLaterChangeDecisionResult(result(p,'9223372036854775808'),p),false);
 const q=payload('APPROVE_UPDATED_HOURS',p.bundle_revision);
 assert.equal(fixed.isLaterChangeDecisionResult({...r,decision:q.decision},q),false);
});
test('comparison never mutates exact numeric or string retained request/hash preimage',()=>{
 for(const revision of [3,'3','9007199254740993']){
  const p=Object.freeze(payload('KEEP_CURRENTLY_APPROVED_HOURS',revision));
  const request=Object.freeze({action:'LATER_CHANGE_DECISION',payload:p}),bytes=JSON.stringify(request);
  const r=Object.freeze(result(p,(BigInt(revision)+1n).toString()));
  assert.equal(fixed.isLaterChangeDecisionResult(r,p),true);
  assert.equal(JSON.stringify(request),bytes);assert.equal(typeof p.bundle_revision,typeof revision);
 }
});
