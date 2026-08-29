import assert from 'node:assert/strict';
import test from 'node:test';
import {validateBankingPayModalEnvelope} from '../broker/src/banking-pay-modal-v2.js';
const id=n=>`10000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const envelope={ok:true,contract:'BANKING_PAY_MODAL_STRUCTURE_V2',contract_version:1,session_id:id(1),session_version:1,progress_counter_version:2,scope_hash:'a'.repeat(64)};
const baseArgs={p_session_id:id(1),p_options_json:{expected_session_version:1,expected_progress_counter_version:2,scope_hash:envelope.scope_hash,pay_channel_scope:'ALL'},p_limit:100,p_cursor:null};
const counts={affected_candidate_count:1,affected_payment_count:105,affected_payment_count_complete:true};
function detail(kind='actionDetail',pageNumber=1,total=105){
 const start=(pageNumber-1)*100;
 return {...envelope,...counts,[kind==='actionDetail'?'task_key':'blocker_key']:'current_issue',total_count:total,
 page_number:pageNumber,has_previous:pageNumber>1,previous_cursor:pageNumber>2?'previous_page':null,
 has_more:start+100<total,next_cursor:start+100<total?'next_page':null,
 rows:Array.from({length:Math.min(100,total-start)},(_,i)=>({identity:`member_${start+i}`,candidate_id:id(2),
  preview_row_id:id(1000+start+i),source_kind:'PREVIEW_ROW',context_only:false,payload:{preview_row_id:id(1000+start+i)}}))};
}
const args=(kind,cursor=null)=>({...baseArgs,p_cursor:cursor,[kind==='actionDetail'?'p_task_key':'p_blocker_key']:'current_issue'});
for(const kind of ['actionDetail','blockedDetail']){
 for(const n of [1,2])test(`${kind} preserves bounded page${n} of105 current members`,()=>{
  const value=detail(kind,n);assert.equal(validateBankingPayModalEnvelope(value,kind,args(kind,n===1?null:'cursor')),value);
 });
 for(const [name,change] of Object.entries({
  wrongKey:v=>v[kind==='actionDetail'?'task_key':'blocker_key']='another_issue',empty:v=>v.rows=[],
  shortPage:v=>v.rows.pop(),lastPage:v=>{v.total_count=90;v.has_more=false;v.next_cursor=null;},
  wrongPage:v=>v.page_number=2,wrongPrevious:v=>v.has_previous=true,
  missingCompleteness:v=>delete v.affected_payment_count_complete,unknownZero:v=>v.affected_payment_count_complete=false,
  invalidCandidate:v=>v.rows[0].candidate_id='invalid',missingContext:v=>delete v.rows[0].context_only,
  missingPayload:v=>delete v.rows[0].payload,invalidPayment:v=>v.rows[0].preview_row_id='invalid',
  wrongPayloadPayment:v=>v.rows[0].payload.preview_row_id=id(999),
  wrongPayloadCandidate:v=>v.rows[0].payload.candidate_id=id(999),
  inventedSourcePayment:v=>v.rows[0].source_kind='SOURCE_PROGRESS',hidden:v=>v.rows[0].indefinite_snooze=true,
  oversized:v=>v.rows[0].payload.large='x'.repeat(256*1024)
 }))test(`${kind} rejects ${name} before output`,()=>{
  const value=detail(kind);change(value);assert.throws(()=>validateBankingPayModalEnvelope(value,kind,args(kind)),/INVALID_RESPONSE/);
 });
}
function actionRow(){return {identity:'bank_issue',issue_state:'ACTION_REQUIRED',title:'Candidate bank details are missing.',...counts};}
function list(row){return {...envelope,rows:[row],total_count:1,scope_count:1,has_more:false,next_cursor:null,
 page_number:1,has_previous:false,previous_cursor:null,search:'',sort_key:'TITLE',sort_direction:'ASC',view:'ACTION_REQUIRED',
 updating_count:0,updating:[],updating_has_more:false,updating_next_cursor:null};}
const listArgs={...baseArgs,p_search:'',p_sort_key:'TITLE',p_sort_direction:'ASC',p_view:'ACTION_REQUIRED'};
const blockedRow=()=>({identity:'current_blocker',candidate_id:id(2),candidate_name:'Synthetic candidate',candidate_reference:'TEST',
 reason:'Insufficient funds to deduct',affected_display_amount:'15.00'});
const blockedList=row=>({...list(row),sort_key:'CANDIDATE'});
const blockedArgs={...baseArgs,p_search:'',p_sort_key:'CANDIDATE',p_sort_direction:'ASC'};
for(const amount of ['15.00','-10.50','0.00',null])test('Blocked list accepts exact current display amount '+amount,()=>{
 const value=blockedList({...blockedRow(),affected_display_amount:amount});
 assert.equal(validateBankingPayModalEnvelope(value,'blocked',blockedArgs),value);
});
for(const [label,change] of Object.entries({
 missingCandidate:r=>delete r.candidate_id,badCandidate:r=>r.candidate_id='not-a-candidate',emptyName:r=>r.candidate_name=' ',
 missingReference:r=>delete r.candidate_reference,emptyReason:r=>r.reason=' ',missingAmount:r=>delete r.affected_display_amount,
 numericAmount:r=>r.affected_display_amount=15,nonDecimal:r=>r.affected_display_amount='1e3',
 paddedDecimal:r=>r.affected_display_amount='015.00',invalidAmount:r=>r.affected_display_amount='NaN',
 negativeZero:r=>r.affected_display_amount='-0.00',
 wideAmount:r=>r.affected_display_amount='1'.repeat(17)+'.00',badIdentity:r=>r.identity='with whitespace',
 sourceInventsPayment:r=>Object.assign(r,{source_kind:'SOURCE_PROGRESS',preview_row_id:id(3)}),
 wrongPhysicalReference:r=>Object.assign(r,{source_kind:'PREVIEW_ROW',preview_row_id:null})
}))test('Blocked list rejects incomplete or corrupt display: '+label,()=>{
 const row=blockedRow();change(row);
 assert.throws(()=>validateBankingPayModalEnvelope(blockedList(row),'blocked',blockedArgs),/INVALID_RESPONSE/);
});
test('Action list rejects a silently truncated first page of101 tasks',()=>{
 const value={...list(actionRow()),total_count:101,scope_count:101,has_more:true,next_cursor:'next_page'};
 assert.throws(()=>validateBankingPayModalEnvelope(value,'actions',listArgs),/INVALID_RESPONSE/);
});
test('Action list accepts exact100/1 pages and rejects an unexpected first-request page',()=>{
 const value={...list(actionRow()),total_count:101,scope_count:101,has_more:true,next_cursor:'next_page',
  rows:Array.from({length:100},(_,n)=>({...actionRow(),identity:'task_'+n}))};
 assert.equal(validateBankingPayModalEnvelope(value,'actions',listArgs),value);
 value.rows=[value.rows[0]];value.page_number=2;value.has_previous=true;value.has_more=false;value.next_cursor=null;
 assert.equal(validateBankingPayModalEnvelope(value,'actions',{...listArgs,p_cursor:'boundary'}),value);
 assert.throws(()=>validateBankingPayModalEnvelope(value,'actions',listArgs),/INVALID_RESPONSE/);
});
for(const n of [0,105,null])test(`Action list accepts truthful payment count ${n}`,()=>{
 const value=list({...actionRow(),affected_payment_count:n,affected_payment_count_complete:n!==null});
 assert.equal(validateBankingPayModalEnvelope(value,'actions',listArgs),value);
});
for(const [label,change] of Object.entries({missingCompleteness:r=>delete r.affected_payment_count_complete,
 unknownZero:r=>{r.affected_payment_count=0;r.affected_payment_count_complete=false;},
 knownNull:r=>r.affected_payment_count=null,negative:r=>r.affected_payment_count=-1,
 noCandidates:r=>r.affected_candidate_count=0}))test(`Action list rejects ${label}`,()=>{
 const r=actionRow();change(r);assert.throws(()=>validateBankingPayModalEnvelope(list(r),'actions',listArgs),/INVALID_RESPONSE/);
});
test('source-only task remains visible with no fabricated payment identity',()=>{
 const value=detail('actionDetail',1,1);Object.assign(value,{affected_payment_count:null,affected_payment_count_complete:false});
 Object.assign(value.rows[0],{source_kind:'STORED_PAYEE',preview_row_id:null,payload:{bank_issue:true}});
 assert.equal(validateBankingPayModalEnvelope(value,'actionDetail',args('actionDetail')),value);
});
const updatingRow=n=>({...actionRow(),identity:'updating_'+n,issue_state:'UPDATING',title:'Refreshing…',
 affected_payment_count:null,affected_payment_count_complete:false});
test('Action list retains100 inline Updating tasks and explicit continuation for105',()=>{
 const value={...list(actionRow()),updating_count:105,updating:Array.from({length:100},(_,n)=>updatingRow(n)),
  updating_has_more:true,updating_next_cursor:'updating_next'};
 assert.equal(validateBankingPayModalEnvelope(value,'actions',listArgs),value);
});
for(const [label,change] of Object.entries({
 missingView:v=>delete v.view,wrongView:v=>v.view='UPDATING',wrongState:v=>v.rows[0].issue_state='UPDATING',
 hiddenUpdating:v=>v.updating=[],incompleteUpdating:v=>v.updating.pop(),duplicateUpdating:v=>v.updating[1]=v.updating[0],
 missingContinuation:v=>v.updating_next_cursor=null,falseComplete:v=>v.updating_has_more=false,
 updatingUnknownZero:v=>v.updating[0].affected_payment_count=0,updatingIsAction:v=>v.updating[0].issue_state='ACTION_REQUIRED',
 crossSectionDuplicate:v=>v.updating[0].identity=v.rows[0].identity
}))test(`Action list rejects broken Updating contract: ${label}`,()=>{
 const value={...list(actionRow()),updating_count:105,updating:Array.from({length:100},(_,n)=>updatingRow(n)),
  updating_has_more:true,updating_next_cursor:'updating_next'};change(value);
 assert.throws(()=>validateBankingPayModalEnvelope(value,'actions',listArgs),/INVALID_RESPONSE/);
});
