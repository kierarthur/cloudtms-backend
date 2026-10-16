import test from 'node:test';
import assert from 'node:assert/strict';
import {decodeContactManualEnquiryReview as review,decodeContactManualEnquiryReceipt as receipt,decodeContactManualEnquiryRow as rowContext} from '../broker/src/weekly-source/contact-manual-enquiry-contract.mjs';
import {id,hex,time,copy,fixture} from './fixtures/contact-manual-enquiry-contract-fixture.mjs';
const refuses=fn=>assert.throws(fn,e=>e.code==='CONTACT_MANUAL_ENQUIRY_RESULT_INVALID'&&e.status===502);
test('new read context binds actual pair/revisions/full issue set without changing pause DTO',()=>{
 const pair={candidate_id:id(2),client_id:id(3)};
 const value={version:'CONTACT_MANUAL_ENQUIRY_ROW_V1',...pair,scope_revision:'9007199254740993',checking_revision:'7',checking_facts_sha256:hex(1),available_issue_ids:[id(4)],
  actions:['WORKER','MANAGER'].map(audience=>({operation:'CONTACT_MANUAL_ENQUIRY_PREVIEW',audience,label:'New enquiry',enabled:true,disabled_reason:null}))};
 assert.equal(rowContext(value,pair),value);
 refuses(()=>rowContext(value,{...pair,client_id:id(90)}));
 value.available_issue_ids.push(id(4));refuses(()=>rowContext(value,pair));
 value.available_issue_ids=[];for(const a of value.actions)Object.assign(a,{enabled:false,disabled_reason:'NO_OPEN_SOURCE_ISSUES'});
 assert.equal(rowContext(value,pair),value);value.scope_revision=9007199254740992;refuses(()=>rowContext(value,pair));
});
test('partial exclusions retain full scope coverage, exact huge revisions and a routine-only manual exception',()=>{
 const f=fixture();assert.equal(review(f.result,f.request),f.result);
 assert.equal(f.result.scope_results[0].scope_revision,'9007199254740993');
 assert.equal(f.result.scope_results[0].members[0].cutoff.routine.policy_revision,'9007199254740995');
 assert.equal(receipt(f.saved,f.accept,f.result),f.saved);
});
test('empty genuinely ineligible review has no confirmation identity',()=>{
 const f=fixture();f.result.scope_results[0].members[0].eligible=false;f.result.scope_results[0].members[0].exclusion_reason='PAUSED';
 Object.assign(f.result,{groups:[],can_accept:false,review_id:null,review_digest:null,expires_at_utc:null});
 assert.equal(review(f.result,f.request),f.result);
});
test('first manager preview can seal a genuine recipient before native route allocation',()=>{
 const f=fixture();f.result.groups[0].recipient_route_id=null;
 assert.equal(review(f.result,f.request),f.result);
 f.result.groups[0].recipient.manager_address=null;refuses(()=>review(f.result,f.request));
});
test('remaining resend wait is a sealed server sample and never a browser countdown',()=>{
 const f=fixture(),member=f.result.scope_results[0].members[1];
 member.cooldown_until_utc='2026-10-16T10:00:00.000001Z';member.cooldown_remaining_seconds=371;
 assert.equal(review(f.result,f.request),f.result);assert.equal(member.cooldown_remaining_seconds,371);
 member.cooldown_remaining_seconds='371';refuses(()=>review(f.result,f.request));
 member.cooldown_remaining_seconds=371;member.cooldown_until_utc=null;refuses(()=>review(f.result,f.request));
});
test('changed checking revision cannot produce an acceptable preview',()=>{
 const f=fixture();f.result.scope_results[0].checking_revision='8';refuses(()=>review(f.result,f.request));
 Object.assign(f.result,{can_accept:false,review_id:null,review_digest:null,expires_at_utc:null});assert.equal(review(f.result,f.request),f.result);
});
for(const [name,mutate] of [
 ['foreign worker row',f=>{f.result.groups[0].preview.rows[0].candidateId=id(90);}],
 ['foreign client cutoff',f=>{f.result.scope_results[0].members[0].cutoff.client_id=id(90);}],
 ['omitted excluded issue',f=>{f.result.scope_results[0].members.pop();}],
 ['excluded issue sent',f=>{f.result.groups[0].selected_issue_ids.push(id(5));}],
 ['eligible issue omitted',f=>{f.result.groups=[];}],
 ['duplicate issue in another group',f=>{f.result.groups.push({...copy(f.result.groups[0]),group_key:hex(6)});}],
 ['extra manager row',f=>{f.result.groups[0].preview.rows.push({...copy(f.result.groups[0].preview.rows[0]),issueId:id(90)});}],
 ['numeric bigint',f=>{f.result.scope_results[0].scope_revision=9007199254740992;}],
 ['leading zero revision',f=>{f.result.scope_results[0].checking_revision='07';}],
 ['undeclared provider fact',f=>{f.result.provider_accepted=true;}],
 ['fabricated future provider URL',f=>{f.result.groups[0].preview.review_url='https://invalid.example';}],
 ['coerced break minutes',f=>{f.result.groups[0].preview.rows[0].breakMinutes='30';}],
 ['invalid calendar date',f=>{f.result.groups[0].preview.rows[0].workDate='2026-02-30';}],
 ['invented hard prohibition classification',f=>{f.result.scope_results[0].members[0].cutoff.hard_restriction.present=true;}],
 ['arbitrary worker model',f=>{f.result.groups[0].preview.kind='OTHER';}],
 ['false confirmation flag',f=>{f.result.can_accept=false;}],
 ])test('refuses '+name,()=>{const f=fixture();mutate(f);refuses(()=>review(f.result,f.request));});
test('actual worker check and submission messages retain distinct native purpose',()=>{
 for(const plain of ['Open MyTMS to check your Timesheet hours.','Open MyTMS to submit your Timesheet hours.']){
  const f=fixture();f.request.audience=f.result.audience='WORKER';const group=f.result.groups[0];
  Object.assign(group,{audience:'WORKER',channel:'PUSH',recipient_route_id:null,candidate_generation_id:id(12),recipient:{candidate_id:id(2),manager_address:null},
   preview:{kind:'WORKER_QUERY_MODEL_V1',policy_version:'weekly-source-candidate-push-v1',renderer_version:'1.0.0',structure_version:null,rows:[],plain_body:plain,attachment_count:0}});
  assert.equal(review(f.result,f.request),f.result);group.recipient.candidate_id=id(90);refuses(()=>review(f.result,f.request));
 }
});
test('aliased issues share one genuine saved member ordinal, never two budget reservations',()=>{
 const f=fixture();for(const scope of [f.result.scope_results[0],f.saved.scope_results[0]])Object.assign(scope.members[1],{eligible:true,exclusion_reason:null});
 f.result.groups[0].selected_issue_ids.push(id(5));f.result.groups[0].preview.rows.push({...copy(f.result.groups[0].preview.rows[0]),issueId:id(5)});
 f.saved.groups[0].members.push({...copy(f.saved.groups[0].members[0]),issue_id:id(5)});
 assert.equal(receipt(f.saved,f.accept,f.result),f.saved);f.saved.groups[0].members[1].send_ordinal++;
 refuses(()=>receipt(f.saved,f.accept,f.result));
});
test('exact immutable receipt replay stays valid after a later browser clock',()=>{
 const f=fixture();f.saved.accepted_at_utc='2020-01-01T00:00:00.000001Z';const before=JSON.stringify(f.saved);
 assert.equal(receipt(f.saved,f.accept),f.saved);assert.equal(JSON.stringify(f.saved),before);
 f.saved.provider_submission_started=true;refuses(()=>receipt(f.saved,f.accept));
});
test('receipt cannot shrink eligibility or substitute a reviewed group',()=>{
 const f=fixture();f.saved.groups[0].group_key=hex(6);refuses(()=>receipt(f.saved,f.accept,f.result));
 const g=fixture();g.saved.groups[0].members=[];refuses(()=>receipt(g.saved,g.accept));
});
