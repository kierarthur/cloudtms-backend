// In-memory boundary proof only: real frontend request builder -> real Worker
// router/validator -> explicit RPC fixture. No network, credential or DB write.
import assert from 'node:assert/strict';
import test from 'node:test';
import path from 'node:path';
import {createRequire} from 'node:module';
import {dispatchBankingPayModalV2Request} from '../broker/src/banking-pay-modal-v2.js';
const require=createRequire(import.meta.url);
const frontend=process.env.BANKING_MODAL_FRONTEND_ROOT;
const enabled=Boolean(frontend);
const id=value=>`00000000-0000-4000-8000-${String(value).padStart(12,'0')}`;
const ctx={session_id:id(1000),expected_session_version:2,expected_progress_counter_version:3,scope_hash:'a'.repeat(64),pay_channel_scope:'ALL'};
const envelope=extra=>({ok:true,contract:'BANKING_PAY_MODAL_STRUCTURE_V2',contract_version:1,session_id:ctx.session_id,
  session_version:2,progress_counter_version:3,scope_hash:ctx.scope_hash,...extra});
const candidate={candidate_id:id(1),candidate_name:'Synthetic boundary candidate',candidate_reference:'TEST-ONLY',
  candidate_sort_name:'synthetic boundary candidate',candidate_sort_reference:'test-only',child_revision:'2:3:fixture',facts_digest:'b'.repeat(64),
  selectable_ready_count:1,selected_ready_count:0,selection_state:'NONE',selected_display_amount:'0.00',
  selected_deduction_exists:false,selected_timesheet_count:0,selected_timesheet_ids:[],selected_timesheet_scope_token:null};
const global={candidate_count:1,selected_candidate_count:0,selected_ready_count:0,selectable_ready_count:1,
  selected_ready_display_amount:'0.00',selection_state:'NONE',action_required_count:0,blocked_count:0,updating_count:0,
  draft:{can_create_draft:false,blocker_codes:['NO_SELECTED_ROWS'],session_ready:true,read_only:false,work_queued:false,
    display_ready:true,draft_safe:true,draft_block_reason_code:null,session_selected_row_count:0,session_selected_eligible_ready_row_count:0}};
const readyRow={identity:id(1100),candidate_id:id(1),effective_section:'canonical_preview_lines',selected:false,
  selection_group_kind:null,selection_group_key:null,selection_group_member_count:0,selection_group_selected_count:0,
  selection_group_state:null,selection_group_display_amount:null,selection_group_selected_display_amount:null};
function bridge(reply){
  const {createTransport}=require(path.join(frontend,'js/banking-pay-modal-v2.js'));const calls=[];
  const transport=createTransport({API:route=>`https://test-boundary.example.invalid${route}`,
    authFetch:(url,options)=>dispatchBankingPayModalV2Request({req:new Request(url,options),user:{id:id(9000)},
      rpc:async(name,args,policy)=>{calls.push({name,args,policy});return reply;}})});
  return {transport,calls};
}
test('current frontend initial-summary builder reaches the exact Worker RPC with server-discovered scope',{skip:!enabled},async()=>{
  const reply=envelope({view_digest:'c'.repeat(64),rows:[candidate],global,total_count:1,has_more:false,next_cursor:null,sort_key:'CANDIDATE',sort_direction:'ASC',
    page_number:1,has_previous:false,previous_cursor:null,page_anchor:'page_anchor',next_page_anchor:null,previous_page_anchor:null});
  const {transport,calls}=bridge(reply);const args={...ctx,sort_key:'CANDIDATE',sort_direction:'ASC',limit:100};delete args.scope_hash;
  assert.deepEqual(await transport.readPage('summary',args),reply);
  assert.equal(calls.length,1);assert.equal(calls[0].name,'pay_workbench_session_get_candidate_summary_page_v1');
  assert.equal(calls[0].args.p_options_json.scope_hash,null);assert.equal(calls[0].args.p_limit,100);
});
for(const [kind,name,params,extra] of [
  ['ready','pay_workbench_session_get_candidate_ready_page_v1',{candidate_id:id(1),cursor:null,limit:100},
    {candidate_id:id(1),candidate,page_number:1,has_previous:false,previous_cursor:null,page_anchor:'fixture_ready_anchor'}],
  ['actions','pay_workbench_session_get_action_required_page_v1',{search:'bank details',sort_key:'PAYMENTS',sort_direction:'DESC',cursor:null,limit:100},
    {search:'bank details',sort_key:'PAYMENTS',sort_direction:'DESC',scope_count:0,view:'ACTION_REQUIRED',
      updating_count:0,updating:[],updating_has_more:false,updating_next_cursor:null}],
  ['blocked','pay_workbench_session_get_blocked_page_v1',{search:'on hold',sort_key:'AMOUNT',sort_direction:'ASC',cursor:null,limit:100},
    {search:'on hold',sort_key:'AMOUNT',sort_direction:'ASC',scope_count:0}],
  ['actionDetail','pay_workbench_session_get_action_required_detail_v1',{identity:'exact_task',cursor:null,limit:100},
    {task_key:'exact_task'}],
  ['blockedDetail','pay_workbench_session_get_blocked_detail_v1',{identity:'exact_blocker',cursor:null,limit:100},
    {blocker_key:'exact_blocker'}]
]){
  test(`current frontend ${kind} request and response cross the exact Worker contract`,{skip:!enabled},async()=>{
    const isDetail=['actionDetail','blockedDetail'].includes(kind);
    const reply=envelope({rows:kind==='ready'?[readyRow]
      :isDetail?[{identity:'source_issue',candidate_id:id(1),preview_row_id:null,source_kind:'SOURCE_PROGRESS',context_only:false,payload:{current_source_issue:true}}]:[],
      total_count:kind==='ready'||isDetail?1:0,has_more:false,next_cursor:null,
      ...(['actions','blocked'].includes(kind)?{page_number:0,has_previous:false,previous_cursor:null}:{}),
      ...(isDetail?{page_number:1,has_previous:false,previous_cursor:null,affected_candidate_count:1,
        affected_payment_count:null,affected_payment_count_complete:false}:{}),...extra});
    const {transport,calls}=bridge(reply);assert.deepEqual(await transport.readPage(kind,{...ctx,...params}),reply);
    assert.equal(calls.length,1);assert.equal(calls[0].name,name);assert.equal(calls[0].args.p_actor_user_id,id(9000));
    assert.equal(calls[0].policy.routeClass,'PREVIEW_PROGRESS');assert.equal(calls[0].args.p_limit,100);
    if(params.identity)assert.equal(calls[0].args[kind==='actionDetail'?'p_task_key':'p_blocker_key'],params.identity);
    if(params.search)assert.equal(calls[0].args.p_search,params.search);
    if(kind==='actions')assert.equal(calls[0].args.p_view,'ACTION_REQUIRED');
  });
}
test('real selected-Timesheet transport preserves the exact candidate and opaque selected scope',{skip:!enabled},async()=>{
  const reply=envelope({candidate_id:id(1),timesheet_count:1,timesheet_ids:[id(2001)]});
  const {transport,calls}=bridge(reply);
  assert.deepEqual(await transport.readPage('timesheets',{...ctx,candidate_id:id(1),scope_token:'exact_selected_scope'}),reply);
  assert.equal(calls.length,1);assert.equal(calls[0].name,'pay_workbench_session_get_selected_ready_timesheets_v1');
  assert.equal(calls[0].args.p_scope_token,'exact_selected_scope');
});
test('real candidate POST reaches one current mutation RPC and keeps Draft input out',{skip:!enabled},async()=>{
  const reply=envelope({progress_counter_version:4,request_id:id(999),state_changed:true,global,candidate,
    view_digest:'d'.repeat(64),retention:{before_view_digest:'c'.repeat(64),other_candidates_unchanged:true,membership_unchanged:true,
      candidate_sort_unchanged:true,amount_sort_unchanged:false,deduction_sort_unchanged:true},
    candidate_absent:false,movements:[],movements_complete:true,movement_count:0,movement_digest:'b'.repeat(64),
    invalidations:{scope:'ALL_PREVIOUS_DETAILS',ready:true,actions:true,updating:true,blocked:true}});
  const {transport,calls}=bridge(reply);
  assert.deepEqual(await transport.mutateCandidate({...ctx,candidate_id:id(1),action:'CLEAR_ALL_READY',request_id:id(999),expected_view_digest:'c'.repeat(64)}),reply);
  assert.equal(calls.length,1);assert.equal(calls[0].name,'pay_workbench_session_set_candidate_ready_selection_v1');
  assert.equal(calls[0].policy.routeClass,'CONTROL');
  assert.deepEqual(Object.keys(calls[0].args).sort(),['p_action','p_actor_user_id','p_candidate_id','p_expected_view_digest','p_options_json','p_request_id','p_session_id']);
});

test('current frontend open-child selection crosses the Worker once with complete same-revision rows',{skip:!enabled},async()=>{
  const currentCandidate={...candidate,child_revision:'2:4:fixture'};
  const reply=envelope({progress_counter_version:4,request_id:id(999),state_changed:true,global,candidate:currentCandidate,
    view_digest:'d'.repeat(64),retention:{before_view_digest:'c'.repeat(64),other_candidates_unchanged:true,membership_unchanged:true,
      candidate_sort_unchanged:true,amount_sort_unchanged:false,deduction_sort_unchanged:true},
    candidate_absent:false,movements:[],movements_complete:true,movement_count:0,movement_digest:'b'.repeat(64),
    invalidations:{scope:'ALL_PREVIOUS_DETAILS',ready:true,actions:true,updating:true,blocked:true},ready_page:envelope({progress_counter_version:4,
      candidate_id:id(1),candidate:currentCandidate,total_count:1,has_more:false,next_cursor:null,
      page_number:1,has_previous:false,previous_cursor:null,page_anchor:'fixture_ready_anchor',
      rows:[readyRow]})});
  const {transport,calls}=bridge(reply);
  assert.deepEqual(await transport.mutateCandidate({...ctx,candidate_id:id(1),action:'CLEAR_ALL_READY',request_id:id(999),
    expected_view_digest:'c'.repeat(64),
    open_ready:{cursor:null,limit:100}}),reply);
  assert.equal(calls.length,1);assert.deepEqual(calls[0].args.p_open_ready_json,{cursor:null,limit:100});
  assert.equal(calls[0].policy.routeClass,'CONTROL');
});
