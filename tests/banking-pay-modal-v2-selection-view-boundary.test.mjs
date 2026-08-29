import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { dispatchBankingPayModalV2Request, validateBankingPayModalEnvelope } from '../broker/src/banking-pay-modal-v2.js';
const require=createRequire(import.meta.url);
assert.ok(process.env.BANKING_MODAL_FRONTEND_ROOT,'Use the actual saved frontend source');
const frontend=process.env.BANKING_MODAL_FRONTEND_ROOT;
const mutation=require(path.join(frontend,'js/banking-pay-modal-v2-mutation.js'));
const {createTransport}=require(path.join(frontend,'js/banking-pay-modal-v2.js'));
const {id,candidate}=require(path.join(frontend,'tests/fixtures/banking-pay-v2-table-page.cjs'));
const digest='c'.repeat(64);
const options={expected_session_version:2,expected_progress_counter_version:3,scope_hash:'a'.repeat(64),pay_channel_scope:'ALL'};
const request=()=>({session_id:id(1000),candidate_id:id(1),...options,action:'CLEAR_ALL_READY',request_id:id(9000),expected_view_digest:digest});
const args=()=>({p_session_id:id(1000),p_candidate_id:id(1),p_options_json:options,p_actor_user_id:id(9001),
 p_action:'CLEAR_ALL_READY',p_request_id:id(9000),p_expected_view_digest:digest});
const reply=()=>({ok:true,contract:'BANKING_PAY_MODAL_STRUCTURE_V2',contract_version:1,session_id:id(1000),
 session_version:2,progress_counter_version:4,scope_hash:options.scope_hash,request_id:id(9000),state_changed:true,
 candidate_absent:false,candidate:candidate(1),view_digest:'d'.repeat(64),
 retention:{before_view_digest:digest,other_candidates_unchanged:true,membership_unchanged:true,
  candidate_sort_unchanged:true,amount_sort_unchanged:false,deduction_sort_unchanged:false},
 global:{candidate_count:1,selected_candidate_count:0,selected_ready_count:0,selectable_ready_count:1,
  selection_state:'NONE',selected_ready_display_amount:'0.00',action_required_count:0,updating_count:0,blocked_count:0,
  draft:{can_create_draft:false,blocker_codes:['NO_SELECTED_ROWS'],session_ready:true,read_only:false,work_queued:false,
    display_ready:true,draft_safe:true,draft_block_reason_code:null,session_selected_row_count:0,session_selected_eligible_ready_row_count:0}},
 movements:[],movements_complete:true,movement_count:0,movement_digest:'b'.repeat(64),
 invalidations:{scope:'ALL_PREVIOUS_DETAILS',ready:true,actions:true,updating:true,blocked:true}});
function bridge(payload=reply(),rpcError=null){
 const calls=[];
 const transport=createTransport({API:p=>'https://test.invalid'+p,authFetch:(url,opts)=>
  dispatchBankingPayModalV2Request({req:new Request(url,opts),user:{id:id(9001)},rpc:async(name,value)=>{
   calls.push({name,args:value});if(rpcError)throw rpcError;return payload;
  }})});
 return {transport,calls};
}
test('public candidate adapter is service-only and calls exactly the proved private response owner',()=>{
 const sql=fs.readFileSync(new URL('../supabase/repeatable/28082026_2313_banking_pay_modal_candidate_selection_response.sql',import.meta.url),'utf8');
 const publicPart=sql.slice(sql.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_candidate_ready_selection_v1'));
 assert.match(publicPart,/p_expected_view_digest text/);
 assert.match(publicPart,/SECURITY DEFINER SET search_path TO ''/);
 assert.equal((publicPart.match(/private\.pay_workbench_modal_candidate_selection_response_v2\(/g)||[]).length,1);
 assert.match(publicPart,/REVOKE ALL ON FUNCTION[\s\S]*FROM PUBLIC, anon, authenticated/);
 assert.match(publicPart,/GRANT EXECUTE ON FUNCTION[\s\S]*TO service_role/);
 assert.doesNotMatch(publicPart,/\b(?:UPDATE|INSERT|DELETE|SUM|ROUND|ABS)\b/);
});
test('separate expected view digest crosses both boundaries without altering original four options',async()=>{
 const {transport,calls}=bridge();assert.deepEqual(await transport.mutateCandidate(request()),reply());
 assert.equal(calls.length,1);assert.equal(calls[0].args.p_expected_view_digest,digest);
 assert.deepEqual(calls[0].args.p_options_json,options);
 assert.equal(mutation.validateRetention(reply(),digest).view_digest,'d'.repeat(64));
});
for(const invalid of [undefined,null,'','c'.repeat(63),'C'.repeat(64),12,{},['c'.repeat(64)]])
 test('invalid expected view cannot reach a mutation: '+JSON.stringify(invalid),async()=>{
  const {transport,calls}=bridge();await assert.rejects(()=>transport.mutateCandidate({...request(),expected_view_digest:invalid}));
  assert.equal(calls.length,0);
  // Independently exercise the Worker, bypassing browser validation.
  const body={...request(),expected_view_digest:invalid};delete body.session_id;delete body.candidate_id;
  let submitted=0;
  const result=await dispatchBankingPayModalV2Request({req:new Request('https://test.invalid/api/banking/pay/workbench/v2/session/'+id(1000)+'/candidate/'+id(1)+'/selection',
   {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)}),user:{id:id(9001)},rpc:async()=>{submitted++;return reply();}});
  assert.equal(result.status,400);assert.equal(submitted,0);
 });
for(const [name,change] of Object.entries({
 missing:p=>delete p.retention,extra:p=>p.retention.assume_safe=true,wrongBefore:p=>p.retention.before_view_digest='e'.repeat(64),
 stringBoolean:p=>p.retention.other_candidates_unchanged='true',missingBoolean:p=>delete p.retention.membership_unchanged,
 nullProof:p=>p.retention=null,arrayProof:p=>p.retention=[],nullBoolean:p=>p.retention.amount_sort_unchanged=null,
 missingView:p=>delete p.view_digest,badView:p=>p.view_digest='D'.repeat(64)
}))test('both response boundaries reject '+name+' and never claim no write',async()=>{
 const p=reply();change(p);
 assert.throws(()=>validateBankingPayModalEnvelope(p,'selection',args()),/INVALID_RESPONSE/);
 assert.throws(()=>mutation.validateRetention(p,digest),/INVALID_RESPONSE/);
 const {transport,calls}=bridge(p);
 await assert.rejects(()=>transport.mutateCandidate(request()),e=>e.outcome==='UNCERTAIN');
 assert.equal(calls.length,1);
 // Browser must also reject malformed proof even if an upstream returns200.
 const browser=createTransport({API:p=>p,authFetch:async()=>new Response(JSON.stringify(p))});
 await assert.rejects(()=>browser.mutateCandidate(request()),e=>e.outcome==='UNCERTAIN');
});
for(const code of ['BANKING_PAY_V2_STALE_VIEW','BANKING_PAY_V2_SELECTION_TOO_LARGE','BANKING_PAY_V2_READY_TOO_LARGE'])
 test('confirmed SQL rollback '+code+' stays typed and is not retried',async()=>{
  const {transport,calls}=bridge(null,{code:'P0001',message:code});
  await assert.rejects(()=>transport.mutateCandidate(request()),e=>e.code===code&&e.outcome==='REJECTED');
  assert.equal(calls.length,1);
 });

const globalRequest=()=>{const r=request();delete r.candidate_id;return r;};

const rowRequest=()=>{const r=request();delete r.action;return {...r,preview_row_ids:[id(500)],selected:false};};
const rowReply=()=>({...reply(),candidate_id:id(1),selection_scope:'EXACT_READY_ROWS'});
test('individual exact-row patch crosses both boundaries once without changing its original options',async()=>{
 const {transport,calls}=bridge(rowReply());assert.deepEqual(await transport.mutateRows(rowRequest()),rowReply());
 assert.equal(calls.length,1);assert.equal(calls[0].name,'pay_workbench_session_set_ready_rows_v1');
 assert.deepEqual(calls[0].args,{p_session_id:id(1000),p_candidate_id:id(1),p_options_json:options,p_actor_user_id:id(9001),
  p_preview_row_ids:[id(500)],p_selected:false,p_request_id:id(9000),p_expected_view_digest:digest});
});
for(const [label,extra] of Object.entries({empty:{preview_row_ids:[]},missing:{preview_row_ids:undefined},
 duplicate:{preview_row_ids:[id(500),id(500).toUpperCase()]},tooMany:{preview_row_ids:Array.from({length:101},(_,n)=>id(n+500))},
 invalidId:{preview_row_ids:['not-an-id']},nonArray:{preview_row_ids:id(500)},wrongBoolean:{selected:'false'},missingBoolean:{selected:undefined},
 action:{action:'CLEAR_ALL_READY'},search:{search:'wrong'},sort:{sort_key:'CANDIDATE'},badView:{expected_view_digest:null}}))
 test('exact-row request rejects '+label+' before dispatch at both boundaries',async()=>{
  const {transport,calls}=bridge(rowReply());await assert.rejects(()=>transport.mutateRows({...rowRequest(),...extra}));assert.equal(calls.length,0);
  const body={...rowRequest(),...extra};delete body.session_id;delete body.candidate_id;let submitted=0;
  const r=await dispatchBankingPayModalV2Request({req:new Request('https://test.invalid/api/banking/pay/workbench/v2/session/'+id(1000)+'/candidate/'+id(1)+'/ready-selection',
   {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)}),user:{id:id(9001)},rpc:async()=>{submitted++;return rowReply();}});
  assert.equal(r.status,400);assert.equal(submitted,0);
 });
for(const [label,change] of Object.entries({missingScope:p=>delete p.selection_scope,globalScope:p=>p.selection_scope='FILTERED_READY',
 noChange:p=>p.state_changed=false,staleProgress:p=>p.progress_counter_version=3,wrongView:p=>p.retention.before_view_digest='e'.repeat(64)}))
 test('row mutation '+label+' cannot be adopted or mistaken for a rejection',async()=>{
  const p=rowReply();change(p);const {transport,calls}=bridge(p);
  await assert.rejects(()=>transport.mutateRows(rowRequest()),e=>e.outcome==='UNCERTAIN');assert.equal(calls.length,1);
  const browser=createTransport({API:p=>p,authFetch:async()=>new Response(JSON.stringify(p))});
  await assert.rejects(()=>browser.mutateRows(rowRequest()),e=>e.outcome==='UNCERTAIN');
 });
test('a nonselectable exact row is a confirmed typed rejection with no blind retry',async()=>{
 const {transport,calls}=bridge(null,{code:'P0001',message:'BANKING_PAY_V2_ROW_NOT_SELECTABLE'});
 await assert.rejects(()=>transport.mutateRows(rowRequest()),e=>e.code==='BANKING_PAY_V2_ROW_NOT_SELECTABLE'&&e.outcome==='REJECTED');
 assert.equal(calls.length,1);
});
const groupRequest=()=>{const r=request();delete r.action;return {...r,group_kind:'TIMESHEET',group_key:'READY_TO_PAY|candidate|timesheet',selected:false};};
const groupReply=()=>({...reply(),candidate_id:id(1),selection_scope:'COMPLETE_READY_GROUP',group_kind:'TIMESHEET',
 group_key:'READY_TO_PAY|candidate|timesheet',group_member_count:107,owner_call_count:1});
test('complete Ready group crosses browser and Worker exactly once without loaded row IDs',async()=>{
 const {transport,calls}=bridge(groupReply());assert.deepEqual(await transport.mutateGroup(groupRequest()),groupReply());
 assert.equal(calls.length,1);assert.equal(calls[0].name,'pay_workbench_session_set_ready_group_v1');
 assert.deepEqual(calls[0].args,{p_session_id:id(1000),p_candidate_id:id(1),p_options_json:options,p_actor_user_id:id(9001),
  p_group_kind:'TIMESHEET',p_group_key:'READY_TO_PAY|candidate|timesheet',p_selected:false,
  p_request_id:id(9000),p_expected_view_digest:digest});
 assert.equal(Object.hasOwn(calls[0].args,'p_preview_row_ids'),false);
});
for(const [label,extra] of Object.entries({missingKind:{group_kind:undefined},badKind:{group_kind:'ROW'},missingKey:{group_key:undefined},
 emptyKey:{group_key:''},longKey:{group_key:'x'.repeat(513)},wrongBoolean:{selected:'false'},ids:{preview_row_ids:[id(500)]},action:{action:'CLEAR_ALL_READY'}}))
 test('complete-group request rejects '+label+' before dispatch at both boundaries',async()=>{
  const {transport,calls}=bridge(groupReply());await assert.rejects(()=>transport.mutateGroup({...groupRequest(),...extra}));assert.equal(calls.length,0);
  const body={...groupRequest(),...extra};delete body.session_id;delete body.candidate_id;let submitted=0;
  const r=await dispatchBankingPayModalV2Request({req:new Request('https://test.invalid/api/banking/pay/workbench/v2/session/'+id(1000)+'/candidate/'+id(1)+'/group-selection',
   {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)}),user:{id:id(9001)},rpc:async()=>{submitted++;return groupReply();}});
  assert.equal(r.status,400);assert.equal(submitted,0);
 });
for(const [label,change] of Object.entries({missingScope:p=>delete p.selection_scope,rowScope:p=>p.selection_scope='EXACT_READY_ROWS',
 wrongKind:p=>p.group_kind='OVERPAYMENT',wrongKey:p=>p.group_key='other',zeroMembers:p=>p.group_member_count=0,
 wrongCalls:p=>p.owner_call_count=2,noChange:p=>p.state_changed=false,staleProgress:p=>p.progress_counter_version=3}))
 test('complete-group result '+label+' cannot publish or pretend no write',async()=>{
  const p=groupReply();change(p);const {transport,calls}=bridge(p);
  await assert.rejects(()=>transport.mutateGroup(groupRequest()),e=>e.outcome==='UNCERTAIN');assert.equal(calls.length,1);
  const browser=createTransport({API:p=>p,authFetch:async()=>new Response(JSON.stringify(p))});
  await assert.rejects(()=>browser.mutateGroup(groupRequest()),e=>e.outcome==='UNCERTAIN');
 });
test('a nonselectable complete group is a confirmed typed rejection with no blind retry',async()=>{
 const {transport,calls}=bridge(null,{code:'P0001',message:'BANKING_PAY_V2_GROUP_NOT_SELECTABLE'});
 await assert.rejects(()=>transport.mutateGroup(groupRequest()),e=>e.code==='BANKING_PAY_V2_GROUP_NOT_SELECTABLE'&&e.outcome==='REJECTED');
 assert.equal(calls.length,1);
});
const globalReply=()=>{const p=reply();delete p.candidate;delete p.candidate_absent;delete p.retention;
 return {...p,before_view_digest:digest,selection_scope:'FILTERED_READY',requires_summary_refresh:true};};
test('global header crosses the existing authenticated transport once with no loaded candidate IDs',async()=>{
 const {transport,calls}=bridge(globalReply());
 assert.deepEqual(await transport.mutateGlobal(globalRequest()),globalReply());
 assert.equal(calls.length,1);assert.equal(calls[0].name,'pay_workbench_session_set_filtered_ready_selection_v1');
 assert.deepEqual(calls[0].args,{p_session_id:id(1000),p_options_json:options,p_actor_user_id:id(9001),
  p_action:'CLEAR_ALL_READY',p_request_id:id(9000),p_expected_view_digest:digest});
});
for(const [label,extra] of Object.entries({candidate:{candidate_id:id(1)},child:{open_ready:{cursor:null,limit:100}},
 visibleIds:{selected_preview_row_ids:[id(5)]},sort:{sort_key:'CANDIDATE'},search:{search:'no new filter'}}))
 test('global header rejects '+label+' at both request boundaries',async()=>{
  const {transport,calls}=bridge(globalReply());await assert.rejects(()=>transport.mutateGlobal({...globalRequest(),...extra}));
  assert.equal(calls.length,0);
  const body={...globalRequest(),...extra};delete body.session_id;let submitted=0;
  const r=await dispatchBankingPayModalV2Request({req:new Request('https://test.invalid/api/banking/pay/workbench/v2/session/'+id(1000)+'/selection',
   {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)}),user:{id:id(9001)},rpc:async()=>{submitted++;return globalReply();}});
  assert.equal(r.status,400);assert.equal(submitted,0);
 });
for(const [label,change] of Object.entries({staleView:p=>p.before_view_digest='e'.repeat(64),
 missingRefresh:p=>delete p.requires_summary_refresh,falseRefresh:p=>p.requires_summary_refresh=false,
 wrongScope:p=>p.selection_scope='CANDIDATE_READY',unsolicitedCandidate:p=>p.candidate=candidate(1),
 unsolicitedChild:p=>p.ready_page={},futureProgress:p=>p.progress_counter_version=5}))
 test('global result '+label+' cannot publish or pretend no write',async()=>{
  const p=globalReply();change(p);const {transport,calls}=bridge(p);
  await assert.rejects(()=>transport.mutateGlobal(globalRequest()),e=>e.outcome==='UNCERTAIN');assert.equal(calls.length,1);
  const a=args();delete a.p_candidate_id;
  assert.throws(()=>validateBankingPayModalEnvelope(p,'globalSelection',a),/INVALID_RESPONSE/);
 });
