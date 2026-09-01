import assert from 'node:assert/strict';
import test from 'node:test';
import { dispatchBankingPayModalV2Request } from '../broker/src/banking-pay-modal-v2.js';

const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const session=id(1),candidate=id(2),actor=id(3),requestId=id(4);
const options={expected_session_version:2,expected_progress_counter_version:3,scope_hash:'a'.repeat(64),pay_channel_scope:'ALL'};
const envelope=()=>({ok:true,contract:'BANKING_PAY_MODAL_STRUCTURE_V2',contract_version:1,
  session_id:session,session_version:2,progress_counter_version:4,scope_hash:options.scope_hash});
const row=()=>({candidate_id:candidate,candidate_name:'Synthetic candidate',candidate_reference:'SYNTHETIC',
  candidate_sort_name:'synthetic candidate',candidate_sort_reference:'synthetic',child_revision:'2:4:scope',facts_digest:'b'.repeat(64),
  selectable_ready_count:2,selected_ready_count:0,selection_state:'NONE',selected_display_amount:'0.00',
  selected_deduction_exists:false,selected_timesheet_count:0,selected_timesheet_ids:[],selected_timesheet_scope_token:null});
const ready=()=>({...envelope(),candidate_id:candidate,candidate:row(),total_count:2,ready_row_count:2,has_more:false,next_cursor:null,
  page_number:1,has_previous:false,previous_cursor:null,page_anchor:'current_anchor',
  rows:[10,11].map(n=>({identity:id(n),candidate_id:candidate,effective_section:'canonical_preview_lines',selected:false,
    presentation_group_kind:'ROW',presentation_group_key:`ROW|${id(n)}`,presentation_group_row_count:1,
    selection_group_kind:null,selection_group_key:null,selection_group_member_count:0,selection_group_selected_count:0,
    selection_group_state:null,selection_group_display_amount:null,selection_group_selected_display_amount:null}))});
const selection=()=>({...envelope(),request_id:requestId,state_changed:true,candidate_absent:false,candidate:row(),
  view_digest:'d'.repeat(64),retention:{before_view_digest:'c'.repeat(64),other_candidates_unchanged:true,
    membership_unchanged:true,candidate_sort_unchanged:true,amount_sort_unchanged:false,deduction_sort_unchanged:true},
  movements:[],movements_complete:true,movement_count:0,movement_digest:'b'.repeat(64),
  invalidations:{scope:'ALL_PREVIOUS_DETAILS',ready:true,actions:true,updating:true,blocked:true},
  global:{candidate_count:1,selected_candidate_count:0,selected_ready_count:0,selectable_ready_count:2,selection_state:'NONE',
    selected_ready_display_amount:'0.00',action_required_count:0,updating_count:0,blocked_count:0,
    draft:{can_create_draft:false,blocker_codes:['NO_SELECTED_ROWS'],session_ready:true,read_only:false,work_queued:false,
      display_ready:true,draft_safe:true,draft_block_reason_code:null,session_selected_row_count:0,session_selected_eligible_ready_row_count:0}}});
const bytes=value=>Buffer.byteLength(JSON.stringify(value),'utf8');
function padding(value,target){value.synthetic_padding='';value.synthetic_padding='x'.repeat(target-bytes(value));assert.equal(bytes(value),target);return value;}
async function invoke(payload,{open,omitOpen=false}={}){
  let calls=0,args;
  const body={...options,action:'CLEAR_ALL_READY',request_id:requestId,expected_view_digest:'c'.repeat(64)};
  if(!omitOpen&&open!==undefined)body.open_ready=open;
  const response=await dispatchBankingPayModalV2Request({
    req:new Request(`https://test.invalid/api/banking/pay/workbench/v2/session/${session}/candidate/${candidate}/selection`,
      {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)}),
    user:{id:actor},rpc:async(name,value)=>{calls++;args=value;assert.equal(name,'pay_workbench_session_set_candidate_ready_selection_v1');return payload;}
  });
  return {status:response.status,payload:await response.json(),calls,args};
}
test('complete open Ready page travels in the single candidate mutation reply',async()=>{
  const reply={...selection(),ready_page:ready()};
  const result=await invoke(reply,{open:{cursor:null,limit:100}});
  assert.equal(result.status,200);assert.equal(result.calls,1);
  assert.deepEqual(result.args.p_open_ready_json,{cursor:null,limit:100});
  assert.deepEqual(result.payload,reply);
});
test('main-only selection keeps its existing small response and has no additional child request',async()=>{
  const result=await invoke(selection());assert.equal(result.status,200);assert.equal(result.calls,1);
});
test('the approved exception accommodates a complete100-row synthetic breakdown, not all child pages',async()=>{
  const child=ready();child.total_count=105;child.ready_row_count=105;child.has_more=true;child.next_cursor='next_current';
  child.rows=Array.from({length:100},(_,n)=>({identity:id(100+n),candidate_id:candidate,
    effective_section:'canonical_preview_lines',selected:false,synthetic_detail:'x'.repeat(1650),
    presentation_group_kind:'ROW',presentation_group_key:`ROW|${id(100+n)}`,presentation_group_row_count:1,
    selection_group_kind:null,selection_group_key:null,selection_group_member_count:0,selection_group_selected_count:0,
    selection_group_state:null,selection_group_display_amount:null,selection_group_selected_display_amount:null}));
  const reply={...selection(),ready_page:child};assert.ok(bytes(reply)>32*1024);
  const result=await invoke(reply,{open:{cursor:null,limit:100}});
  assert.equal(result.status,200);assert.equal(result.calls,1);assert.equal(result.payload.ready_page.rows.length,100);
});
for(const [name,open] of Object.entries({array:[],string:'cursor',extra:{cursor:null,limit:100,candidate_id:id(6)},
  missingCursor:{limit:100},missingLimit:{cursor:null},tooMany:{cursor:null,limit:101},badLimit:{cursor:null,limit:'100'},
  badCursor:{cursor:'../other',limit:100}}))test(`invalid open_ready ${name} is rejected before mutation`,async()=>{
  const result=await invoke(selection(),{open});assert.equal(result.status,400);assert.equal(result.calls,0);
  assert.equal(result.payload.outcome,'NOT_SUBMITTED');
});
for(const [name,modify] of Object.entries({
  missing:reply=>{delete reply.ready_page;},empty:reply=>{reply.ready_page.rows=[];},
  wrongCandidate:reply=>{reply.ready_page.candidate_id=id(7);},oldRevision:reply=>{reply.ready_page.progress_counter_version=3;},
  futureRevision:reply=>{reply.ready_page.progress_counter_version=5;},wrongScope:reply=>{reply.ready_page.scope_hash='b'.repeat(64);},
  inconsistentHeader:reply=>{reply.ready_page.candidate.candidate_name='Different';},
  duplicate:reply=>{reply.ready_page.rows[1]={...reply.ready_page.rows[0]};},
  incompleteFirst:reply=>{reply.ready_page.rows.pop();},
  overLimit:reply=>{reply.ready_page.rows=Array.from({length:101},(_,n)=>({...reply.ready_page.rows[0],identity:id(100+n)}));reply.ready_page.total_count=101;},
  blockedRow:reply=>{reply.ready_page.rows[0].effective_section='blocked_lines';}
}))test(`malformed open Ready ${name} cannot publish a partial mutation result`,async()=>{
  const reply={...selection(),ready_page:ready()};modify(reply);
  const result=await invoke(reply,{open:{cursor:null,limit:100}});
  assert.equal(result.status,502);assert.equal(result.calls,1);assert.equal(result.payload.outcome,'UNCERTAIN');
  assert.equal(result.payload.read_back_required,true);assert.equal(result.payload.retry_mutation,false);
});
test('a smaller last continuation retains its complete candidate totals',async()=>{
  const reply={...selection(),ready_page:ready()};reply.ready_page.rows.pop();
  Object.assign(reply.ready_page,{page_number:2,has_previous:true});
  const result=await invoke(reply,{open:{cursor:'prior_boundary',limit:1}});
  assert.equal(result.status,200);assert.equal(result.payload.ready_page.total_count,2);
});
test('an unsolicited child is rejected instead of bypassing the main-only budget',async()=>{
  const result=await invoke({...selection(),ready_page:ready()});assert.equal(result.status,502);assert.equal(result.calls,1);
});
test('base selection byte limit accepts exact boundary and rejects one extra byte',async()=>{
  const reply=padding(selection(),32*1024);
  assert.equal((await invoke(reply)).status,200);reply.synthetic_padding+='x';
  const result=await invoke(reply);assert.equal(result.status,502);assert.equal(result.payload.outcome,'UNCERTAIN');
});
test('an open child cannot hide an oversized base or exceed its own512KB limit',async()=>{
  const child=padding(ready(),512*1024);
  const reply={...selection(),ready_page:child};
  assert.equal((await invoke(reply,{open:{cursor:null,limit:100}})).status,200);
  child.synthetic_padding+='x';assert.equal((await invoke(reply,{open:{cursor:null,limit:100}})).status,502);
  reply.ready_page=ready();reply.synthetic_padding='x'.repeat(32*1024);
  assert.equal((await invoke(reply,{open:{cursor:null,limit:100}})).status,502);
});
test('payload limits measure UTF8 bytes rather than JavaScript string length',async()=>{
  const reply=selection();reply.synthetic_padding='£'.repeat(17*1024);
  assert.ok(JSON.stringify(reply).length<32*1024);assert.ok(bytes(reply)>32*1024);
  assert.equal((await invoke(reply)).status,502);
});
for(const [name,change] of Object.entries({jump:reply=>{reply.progress_counter_version=5;},
  noOpWithAdvance:reply=>{reply.state_changed=false;},changedWithoutAdvance:reply=>{reply.progress_counter_version=3;}}))
test(`selection revision rejects ${name}`,async()=>{
  const reply=selection();change(reply);assert.equal((await invoke(reply)).status,502);
});
