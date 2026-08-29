import assert from 'node:assert/strict';import test from 'node:test';import * as worker from '../broker/src/banking-pay-modal-v2.js';
const candidate='00000000-0000-4000-8000-000000000002';
const movement=()=>({identity:'00000000-0000-4000-8000-000000000001',candidate_id:candidate,row_key:'original',
 from:'canonical_preview_lines',to:'blocked_lines',selected:false});
const envelope=()=>({state_changed:true,movements:[movement()],movements_complete:true,movement_count:1,movement_digest:'a'.repeat(64),
 invalidations:{scope:'ALL_PREVIOUS_DETAILS',ready:true,actions:true,updating:true,blocked:true}});
test('Worker accepts only explicit complete or whole-cache-invalidating movement evidence',()=>{
 assert.equal(typeof worker.validateBankingPayMovementEnvelope,'function');
 const p=envelope();assert.equal(worker.validateBankingPayMovementEnvelope(p,candidate),p);
 Object.assign(p,{movements:[],movements_complete:false,movement_count:5000});
 assert.equal(worker.validateBankingPayMovementEnvelope(p,candidate),p);
});
for(const [label,change] of Object.entries({
 missingFlag:p=>delete p.movements_complete,wrongFlag:p=>p.movements_complete='true',missingCount:p=>delete p.movement_count,
 missingDigest:p=>delete p.movement_digest,badDigest:p=>p.movement_digest='bad',countDisagrees:p=>p.movement_count=2,
 incompletePretendsEmpty:p=>Object.assign(p,{movements:[],movements_complete:false,movement_count:0}),
 partialArray:p=>Object.assign(p,{movements_complete:false,movement_count:5000}),
 missingInvalidation:p=>delete p.invalidations.blocked,retainedReady:p=>p.invalidations.ready=false,
 scopedInvalidation:p=>p.invalidations.scope='SOME_DETAILS',duplicate:p=>{p.movements.push({...p.movements[0]});p.movement_count=2;},
 otherCandidate:p=>p.movements[0].candidate_id='00000000-0000-4000-8000-000000000099',missingPhysical:p=>delete p.movements[0].identity,
 unchangedSection:p=>p.movements[0].to=p.movements[0].from,stringSelected:p=>p.movements[0].selected='false',
 noopWithMovement:p=>p.state_changed=false
}))test(`Worker rejects movement ambiguity ${label}`,()=>{
 const p=envelope();change(p);assert.throws(()=>worker.validateBankingPayMovementEnvelope(p,candidate),/INVALID_RESPONSE/);
});
