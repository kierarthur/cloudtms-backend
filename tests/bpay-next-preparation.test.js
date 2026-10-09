import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {handleBpayNextPreparationCommand,handleBpayNextPreparationPage,BPAY_NEXT_PREPARATION_LIMITS} from '../broker/src/bpay-next-preparation.js';

const id=n=>`b8570000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const ACTOR=id(1),RUN=id(2),WORK=id(3),REV=id(4),LINE=id(5),CANDIDATE=id(6),COMMAND=id(7),WORKER=id(8),SHIFT=id(9),DETAIL=id(10);
const SEQ='9007199254740993';
const INPUT={
  CREATE:{run_id:RUN,pay_date:'2026-10-09'},
  WORK_PAGE:{run_id:RUN,page_no:'1',choices:[{work_id:WORK,expected_revision_id:REV,mode:'SUBSET'}]},
  COMPONENT_PAGE:{run_id:RUN,work_id:WORK,expected_revision_id:REV,page_no:'1',approved_line_ids:[LINE]},
  COMPONENT_SEAL:{run_id:RUN,work_id:WORK,expected_revision_id:REV,expected_pages:'1',expected_count:'1'},
  CASE_PAGE:{run_id:RUN,candidate_id:CANDIDATE,selection_revision:'1',page_no:'1',request_id:COMMAND,component_ids:[LINE],selected:[false]},
  CASE_SEAL:{run_id:RUN,candidate_id:CANDIDATE,selection_revision:'1',expected_pages:'1',expected_items:'1'},
  SEAL_ENQUEUE:{run_id:RUN,command_id:COMMAND,expected_pages:'1',expected_count:'1'},
  REVIEW:{run_id:RUN},CONFIRM:{run_id:RUN,review_revision:SEQ}
};
const OUTPUT={
  CREATE:{run_id:RUN,status:'PREPARING',selection_state:'OPEN',selection_count:'0',selected_candidate_count:'0'},
  WORK_PAGE:{run_id:RUN,page_no:'1',item_count:'1',selection_count:'1',selected_candidate_count:'1',replay:false},
  COMPONENT_PAGE:{run_id:RUN,work_id:WORK,expected_revision_id:REV,page_no:'1',item_count:'1',component_count:'1',replay:false},
  COMPONENT_SEAL:{run_id:RUN,work_id:WORK,expected_revision_id:REV,sealed:true,component_count:'1',replay:false},
  CASE_PAGE:{run_id:RUN,candidate_id:CANDIDATE,selection_revision:'1',page_no:'1',item_count:'1',replay:false},
  CASE_SEAL:{run_id:RUN,candidate_id:CANDIDATE,selection_revision:'1',sealed:true,selected_count:'0',excluded_count:'1',replay:false},
  SEAL_ENQUEUE:{run_id:RUN,command_id:COMMAND,agency_sequence:SEQ,replay:false},
  REVIEW:{run_id:RUN,phase:'REVIEW',reason:null,review_revision:SEQ,all_ready:true,replay:false},
  CONFIRM:{run_id:RUN,phase:'DRAFT',review_revision:SEQ,replay:false,drafts:[{run_id:RUN,pay_channel:'PAYE'},{run_id:RUN,pay_channel:'UMBRELLA'}]}
};
const command=(action='WORK_PAGE',change={})=>new Request('https://broker.test.invalid/api/banking/pay/next/preparation',{
  method:'POST',body:JSON.stringify({action,...INPUT[action],...change})});
const page=(kind,args={})=>{
  const params=new URLSearchParams({kind,...Object.fromEntries(Object.entries(args).map(([k,v])=>[k,String(v)]))});
  return new Request(`https://broker.test.invalid/api/banking/pay/next/preparation-page?${params}`);
};
function deps(payload,raw){
  const calls=[];
  return {calls,async requireOfficeUser(req,roles){assert.ok(req instanceof Request);assert.deepEqual(roles,['admin']);return {id:ACTOR,role:'admin'};},
    async preparationRpc(name,args,options){calls.push({name,args,options});return raw??Response.json(payload);},
    async preparationPageRpc(name,args,options){calls.push({name,args,options});return raw??Response.json(payload);}};
}
async function failed(req,d,code,status=400,unknown=false,read=false){
  const response=await (read?handleBpayNextPreparationPage:handleBpayNextPreparationCommand)(req,d);
  assert.equal(response.status,status);const body=await response.json();assert.equal(body.error_code,`BPAY_NEXT_PREPARATION_${code}`);
  assert.equal(body.outcome_unknown,unknown);assert.equal(response.headers.get('x-bpay-next-preparation-wake'),null);return body;
}
for(const action of Object.keys(INPUT))test(`${action} authenticates before one closed actor-bound call and preserves exact receipt`,async()=>{
  const d=deps(OUTPUT[action]);const response=await handleBpayNextPreparationCommand(command(action),d);
  assert.equal(response.status,200);assert.deepEqual(await response.json(),{ok:true,action,result:OUTPUT[action]});
  assert.equal(response.headers.get('cache-control'),'no-store');assert.equal(d.calls.length,1);
  assert.equal(d.calls[0].name,action);assert.deepEqual(d.calls[0].args,INPUT[action]);
  assert.equal(d.calls[0].options.actorUserId,ACTOR);assert.ok(d.calls[0].options.signal instanceof AbortSignal);
  assert.equal(d.calls[0].options.maxResponseBytes,128*1024);assert.equal(d.calls[0].options.timeoutMs,8000);
  assert.equal(d.calls[0].options.routeClass,'OPERATION_NUDGE');
  assert.equal(response.headers.get('x-bpay-next-preparation-wake'),action==='SEAL_ENQUEUE'?'ENROLL':null);
});
test('exact successful enqueue replay still carries wake; other receipts never do',async()=>{
  const response=await handleBpayNextPreparationCommand(command('SEAL_ENQUEUE'),deps({...OUTPUT.SEAL_ENQUEUE,replay:true}));
  assert.equal(response.status,200);assert.equal(response.headers.get('x-bpay-next-preparation-wake'),'ENROLL');
  const response2=await handleBpayNextPreparationCommand(command('COMPONENT_PAGE'),deps({...OUTPUT.COMPONENT_PAGE,replay:true,component_count:SEQ}));
  assert.equal((await response2.json()).result.component_count,SEQ);assert.equal(response2.headers.get('x-bpay-next-preparation-wake'),null);
});

test('CONFIRM preserves one or both frozen channel addresses on the same run, never Source/ordinary partitions',async()=>{
  for(const drafts of [[{run_id:RUN,pay_channel:'PAYE'}],[{run_id:RUN,pay_channel:'UMBRELLA'}],OUTPUT.CONFIRM.drafts]){
    const receipt={...OUTPUT.CONFIRM,drafts},d=deps(receipt);
    const response=await handleBpayNextPreparationCommand(command('CONFIRM'),d);
    assert.equal(response.status,200);assert.deepEqual((await response.json()).result.drafts,drafts);assert.equal(d.calls.length,1);
  }
  for(const drafts of [[],[{run_id:WORK,pay_channel:'PAYE'}],[{run_id:RUN,pay_channel:'SOURCE'}],
    [{run_id:RUN,pay_channel:'PAYE'},{run_id:RUN,pay_channel:'PAYE'}],
    [...OUTPUT.CONFIRM.drafts].reverse(),[{run_id:RUN,pay_channel:'PAYE',source_kind:'ORDINARY'}]]){
    await failed(command('CONFIRM'),deps({...OUTPUT.CONFIRM,drafts}),'RESPONSE_INVALID',503,true);
  }
  const {drafts,...missing}=OUTPUT.CONFIRM;
  await failed(command('CONFIRM'),deps(missing),'RESPONSE_INVALID',503,true);
});

test('channel address read is closed to exactly the confirmed run and channel worker pages retain frozen zero amounts',async()=>{
  const addresses=deps({rows:OUTPUT.CONFIRM.drafts,complete:true});
  const response=await handleBpayNextPreparationPage(page('CHANNEL_DRAFTS',{run_id:RUN}),addresses);
  assert.equal(response.status,200);assert.deepEqual((await response.json()).rows,OUTPUT.CONFIRM.drafts);
  assert.equal(addresses.calls.length,1);assert.deepEqual(addresses.calls[0].args,{run_id:RUN});
  await failed(page('CHANNEL_DRAFTS',{run_id:RUN}),deps({rows:[{run_id:WORK,pay_channel:'PAYE'}],complete:true}),'RESPONSE_INVALID',503,false,true);
  const args={run_id:RUN,pay_channel:'UMBRELLA',limit:1};
  const row={run_worker_id:WORKER,candidate_id:CANDIDATE,pay_channel:'UMBRELLA',worker_status:'CANCELLED',frozen_ex_vat:'0.00',frozen_vat:'0.00',frozen_inc_vat:'0.00',review_issue_code:null};
  const d=deps({rows:[row],complete:false}),workers=await handleBpayNextPreparationPage(page('CHANNEL_WORKERS',args),d);
  assert.equal(workers.status,200);const body=await workers.json();assert.deepEqual(body.rows,[row]);assert.deepEqual(body.next_cursor,{after_candidate_id:CANDIDATE});
  await failed(page('CHANNEL_WORKERS',args),deps({rows:[{...row,pay_channel:'PAYE'}],complete:true}),'RESPONSE_INVALID',503,false,true);
});
test('auth precedes malformed request parsing and no unauthorized dependency runs',async()=>{
  const d=deps(OUTPUT.CREATE);d.requireOfficeUser=async()=>null;
  await failed(new Request('https://test.invalid',{method:'POST',body:'not-json'}),d,'UNAUTHORIZED',401);assert.equal(d.calls.length,0);
  d.requireOfficeUser=async()=>({id:ACTOR,role:'finance'});await failed(command('CREATE'),d,'FORBIDDEN',403);assert.equal(d.calls.length,0);
});
test('caller actor, money, function, worker and fallback state are never accepted',async()=>{
  for(const action of Object.keys(INPUT))for(const extra of [{actor_user_id:ACTOR},{p_actor_user_id:ACTOR},{amount:'1.00'},
    {function_name:'private.owner'},{candidate_history:true},{selection_mode:'ALL'},{owner_epoch:SEQ}]){
    const d=deps(OUTPUT[action]);await failed(command(action,extra),d,'REQUEST_INVALID');assert.equal(d.calls.length,0);
  }
  await failed(command('SQL'),deps(),'REQUEST_INVALID');
});
test('WORK ALL and SUBSET are explicit revision-bound tuples, not default missing-metadata behavior',async()=>{
  for(const mode of ['ALL','SUBSET']){
    const input={choices:[{work_id:WORK,expected_revision_id:REV,mode}]};const d=deps(OUTPUT.WORK_PAGE);
    assert.equal((await handleBpayNextPreparationCommand(command('WORK_PAGE',input),d)).status,200);
    assert.equal(d.calls[0].args.choices[0].mode,mode);assert.ok(Object.isFrozen(d.calls[0].args.choices[0]));
  }
  for(const choices of [[{work_id:WORK,mode:'ALL'}],[{work_id:WORK,expected_revision_id:null,mode:'ALL'}],
    [{work_id:WORK,expected_revision_id:REV,mode:'ALL',component_key:'DAY:2026-10-01'}],
    [{work_id:WORK,expected_revision_id:REV,mode:'ALL'},{work_id:WORK.toUpperCase(),expected_revision_id:REV,mode:'SUBSET'}],[],
    Array.from({length:101},(_,n)=>({work_id:id(n+100),expected_revision_id:REV,mode:'SUBSET'}))]){
    const d=deps();await failed(command('WORK_PAGE',{choices}),d,'REQUEST_INVALID');assert.equal(d.calls.length,0);
  }
});
test('component pages are up to100 original line IDs, never caller dates/keys/amounts',async()=>{
  const approved_line_ids=Array.from({length:100},(_,n)=>id(n+100));const d=deps({...OUTPUT.COMPONENT_PAGE,item_count:'100',component_count:'100'});
  assert.equal((await handleBpayNextPreparationCommand(command('COMPONENT_PAGE',{approved_line_ids}),d)).status,200);
  assert.deepEqual(d.calls[0].args.approved_line_ids,approved_line_ids);
  for(const ids of [[],[LINE,LINE.toUpperCase()],Array.from({length:101},(_,n)=>id(n+100)),['DAY:2026-10-01']]){
    const bad=deps();await failed(command('COMPONENT_PAGE',{approved_line_ids:ids}),bad,'REQUEST_INVALID');assert.equal(bad.calls.length,0);
  }
  for(const values of [{page_no:1},{page_no:'01'},{page_no:'0'},{page_no:'9223372036854775808'},
    {component_keys:['DAY:2026-10-01']},{work_date:'2026-10-01'}])await failed(command('COMPONENT_PAGE',values),deps(),'REQUEST_INVALID');
  const precise=deps({...OUTPUT.COMPONENT_PAGE,page_no:SEQ});assert.equal((await handleBpayNextPreparationCommand(command('COMPONENT_PAGE',{page_no:SEQ}),precise)).status,200);
  assert.equal(precise.calls[0].args.page_no,SEQ);
});
test('case-only zero-WORK seal and explicit excluded case selection remain permitted',async()=>{
  const zero={expected_pages:'0',expected_count:'0'};const d=deps(OUTPUT.SEAL_ENQUEUE);
  assert.equal((await handleBpayNextPreparationCommand(command('SEAL_ENQUEUE',zero),d)).status,200);assert.deepEqual(d.calls[0].args,{...INPUT.SEAL_ENQUEUE,...zero});
  for(const selected of [[false,true],[],[0]])await failed(command('CASE_PAGE',{selected}),deps(),'REQUEST_INVALID');
  await failed(command('COMPONENT_SEAL',{expected_count:'0'}),deps(),'REQUEST_INVALID');
});
test('wrong scope/count, JS numeric bigint, free result fields and invalid review phases close mutation outcome',async()=>{
  for(const[action,change]of [['WORK_PAGE',{run_id:id(99)}],['WORK_PAGE',{item_count:'2'}],['COMPONENT_PAGE',{expected_revision_id:id(99)}],
    ['COMPONENT_SEAL',{component_count:'2'}],['SEAL_ENQUEUE',{agency_sequence:9007199254740992}],['REVIEW',{phase:'PREPARING'}],
    ['REVIEW',{reason:'UNKNOWN'}],['CONFIRM',{review_revision:'1'}],['CREATE',{money:'0'}]]){
    await failed(command(action),deps({...OUTPUT[action],...change}),'RESPONSE_INVALID',503,true);
  }
  for(const output of [{...OUTPUT.REVIEW,phase:'PREPARING',reason:'WORKERS_INCOMPLETE',all_ready:null,review_revision:'0'},
    {...OUTPUT.REVIEW,all_ready:null,replay:true}])assert.equal((await handleBpayNextPreparationCommand(command('REVIEW'),deps(output))).status,200);
});
test('definite owner refusals stay known; transport/invalid receipt retry exact original body with unknown outcome',async()=>{
  for(const[db,code,status]of [[{code:'42501'},'FORBIDDEN',403],[{code:'P0002'},'SCOPE_NOT_FOUND',404],
    [{code:'23514'},'CONFLICT',409],[{code:'55000',message:'BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT'},'NOT_ELIGIBLE',409],
    [{code:'55000',message:'BPAY_NEXT_MODULE_NOT_ACTIVE'},'MODULE_NOT_ACTIVE',503],[{code:'PGRST202'},'DEPENDENCY_UNAVAILABLE',503]]){
    const d=deps(null,Response.json(db,{status:400}));await failed(command(),d,code,status);assert.equal(d.calls.length,1);
  }
  const d=deps();d.preparationRpc=async(name,args)=>{d.calls.push({name,args});throw new Error('synthetic secret not exposed');};
  const body=await failed(command(),d,'DEPENDENCY_UNAVAILABLE',503,true);assert.equal(body.retryable,true);assert.equal(d.calls.length,1);
  assert.deepEqual(d.calls[0].args,INPUT.WORK_PAGE);assert.equal(JSON.stringify(body).includes('synthetic'),false);
});
test('request and response stream budgets cancel overflow; successful read never adopts numeric money',async()=>{
  const d=deps();const request=new Request('https://test.invalid',{method:'POST',body:' '.repeat(BPAY_NEXT_PREPARATION_LIMITS.requestBytes+1)});
  await failed(request,d,'REQUEST_TOO_LARGE',413);assert.equal(d.calls.length,0);
  let cancelled=false;const raw=new Response(new ReadableStream({start(c){c.enqueue(new Uint8Array(128*1024+1));},cancel(){cancelled=true;}}));
  await failed(command(),deps(null,raw),'RESPONSE_TOO_LARGE',503,true);assert.equal(cancelled,true);
  const malformed=deps(null,new Response(new Uint8Array([0xff])));await failed(command(),malformed,'RESPONSE_INVALID',503,true);
});
const component=(n='1')=>({approved_line_id:LINE,line_no:n,component_key:'DAY:2026-10-01',component_kind:'WORK',work_date:'2026-10-01',source_pay_channel:'PAYE',
  approved_source_ex_vat:'9007199254740993.01',realised_source_ex_vat:'0.00',held_source_ex_vat:'0.00',residual_source_ex_vat:'9007199254740993.01',position_ready:true});
test('component keyset page retains exact money and immutable original IDs with explicit continuation',async()=>{
  const d=deps({rows:[component(SEQ)],complete:false});const response=await handleBpayNextPreparationPage(page('COMPONENTS',{
    work_id:WORK,expected_revision_id:REV,after_line_no:'0',limit:1}),d);
  assert.equal(response.status,200);assert.deepEqual(await response.json(),{ok:true,kind:'COMPONENTS',rows:[component(SEQ)],complete:false,next_cursor:{after_line_no:SEQ}});
  assert.deepEqual(d.calls[0].args,{work_id:WORK,expected_revision_id:REV,after_line_no:'0',limit:1});assert.equal(d.calls[0].options.routeClass,'READ');
  assert.equal(response.headers.get('x-bpay-next-preparation-wake'),null);
  for(const row of [{...component(),approved_source_ex_vat:1},{...component(),residual_source_ex_vat:'1e2'},
    {...component(),component_key:''},{...component(),money:'extra'}])await failed(page('COMPONENTS',{work_id:WORK,expected_revision_id:REV}),deps({rows:[row],complete:true}),'RESPONSE_INVALID',503,false,true);
});
test('selected zero, signed approved liabilities, nullable missing position and zero clock child facts are not erased',async()=>{
  for(const row of [{...component(),approved_source_ex_vat:'0.00',residual_source_ex_vat:'0.00'},
    {...component(),approved_source_ex_vat:'-10.00',residual_source_ex_vat:'-10.00'},
    {...component(),realised_source_ex_vat:null,held_source_ex_vat:null,residual_source_ex_vat:null,position_ready:false}]){
    const response=await handleBpayNextPreparationPage(page('COMPONENTS',{work_id:WORK,expected_revision_id:REV}),deps({rows:[row],complete:true}));
    assert.deepEqual((await response.json()).rows,[row]);
  }
  const row={shift_detail_id:SHIFT,detail_no:'4',work_date:'2026-10-01',shift_start_at:null,shift_end_at:null,shift_start_local:null,shift_end_local:null,
    shift_overnight:null,submitted_minutes:'0',approved_minutes:'0',approved_hours:'0.000000',detail_label:'Excluded zero child',segment_pay_ex_vat:'0.00',pay_excluded:true,
    hours_day:'0.000000',hours_night:null,hours_sat:'0.000000',hours_sun:'0.000000',hours_bh:'0.000000'};
  const req=()=>page('SHIFTS',{work_id:WORK,expected_revision_id:REV,approved_line_id:LINE,after_detail_no:'3',limit:1});
  assert.deepEqual((await(await handleBpayNextPreparationPage(req(),deps({rows:[row],complete:true}))).json()).rows,[row]);assert.equal(Object.keys(row).length,19);
  await failed(req(),deps({rows:[{...row,segment_pay_ex_vat:'1.00'}],complete:true}),'RESPONSE_INVALID',503,false,true);
});
const PAGES={
  WORK_CHOICES:[{run_id:RUN},{work_id:WORK,expected_revision_id:null,selection_mode:'ALL',selection_state:'SEALED',page_count:'0',component_count:'0'}],
  SELECTED_COMPONENTS:[{run_id:RUN,work_id:WORK},{selection_no:'1',approved_line_id:LINE,expected_revision_id:REV,component_key:'EXPENSE:TRAVEL'}],
  WORKERS:[{run_id:RUN},{run_worker_id:WORKER,candidate_id:CANDIDATE,worker_status:'READY',frozen_ex_vat:'10.00',frozen_vat:'0.00',frozen_inc_vat:'10.00',review_issue_code:null}],
  REVISION_COMPARISON:[{run_worker_id:WORKER},{run_work_id:DETAIL,work_id:WORK,original_timesheet_id:LINE,captured_revision_id:REV,current_revision_id:null,newer_approval_available:true}],
  LINES:[{run_worker_id:WORKER},{run_line_id:DETAIL,line_no:'1',original_timesheet_id:LINE,captured_revision_id:REV,component_key:'EXPENSE:TRAVEL',component_kind:'EXPENSE',
    work_date:null,source_pay_channel:'PAYE',target_pay_channel:'PAYE',source_consumed_ex_vat:'10.00',frozen_ex_vat:'10.00',frozen_vat:'0.00',frozen_inc_vat:'10.00'}],
  BREAKS:[{work_id:WORK,expected_revision_id:REV,approved_line_id:LINE,shift_detail_id:SHIFT},{break_detail_id:DETAIL,break_no:'1',break_start_at:null,break_end_at:null,
    break_start_local:'09:30',break_end_local:'09:45',break_minutes:'15'}],
  RATES:[{work_id:WORK,expected_revision_id:REV,approved_line_id:LINE},{rate_detail_id:DETAIL,bucket:'BH',approved_hours:'0.000000',source_pay_rate:null}]
};
for(const[kind,[args,row]]of Object.entries(PAGES))test(`${kind} uses one closed typed page, honest nulls and exact text`,async()=>{
  const d=deps({rows:[row],complete:true});const response=await handleBpayNextPreparationPage(page(kind,args),d);
  assert.equal(response.status,200);assert.deepEqual(await response.json(),{ok:true,kind,rows:[row],complete:true,next_cursor:null});
  assert.equal(d.calls.length,1);assert.equal(d.calls[0].name,kind);assert.equal(d.calls[0].args.limit,50);
  assert.equal(response.headers.get('x-bpay-next-preparation-wake'),null);
});
test('closed GET input, duplicate parameters, wrong kinds/scopes and cursor order are rejected',async()=>{
  for(const req of [page('SQL'),page('COMPONENTS',{work_id:WORK,expected_revision_id:REV,actor_user_id:ACTOR}),
    page('COMPONENTS',{work_id:WORK,expected_revision_id:REV,limit:101}),page('COMPONENTS',{work_id:WORK,expected_revision_id:REV,after_line_no:'-1'}),
    new Request(`https://test.invalid?kind=WORKERS&run_id=${RUN}&run_id=${RUN}`)]){
    const d=deps({rows:[],complete:true});await failed(req,d,'REQUEST_INVALID',400,false,true);assert.equal(d.calls.length,0);
  }
  for(const rows of [[component('1'),component('1')],[component('3'),component('2')]])await failed(
    page('COMPONENTS',{work_id:WORK,expected_revision_id:REV}),deps({rows,complete:true}),'RESPONSE_INVALID',503,false,true);
  await failed(page('COMPONENTS',{work_id:WORK,expected_revision_id:REV,after_line_no:'1'}),deps({rows:[component('1')],complete:true}),'RESPONSE_INVALID',503,false,true);
  await failed(page('WORKERS',{run_id:RUN}),deps({rows:[],complete:false}),'RESPONSE_INVALID',503,false,true);
});
const currentWork=(n=3,week='2026-10-04')=>({work_id:id(n),candidate_id:CANDIDATE,original_timesheet_id:LINE,work_kind:'ORDINARY',week_ending_date:week,
 approval_state:'APPROVED',current_revision_id:REV,applied_revision_id:REV,position_ready:true,source_pay_channel:'PAYE',timesheet_reference:'TEST-ONLY',
 client_display_name:'TEST CLIENT',approved_source_ex_vat:'120.00'});
test('CURRENT_WORK is a factual candidate-scoped bounded composite cursor, not payability or a reconstructed older approval',async()=>{
 const rows=[currentWork(3,'2026-09-27'),currentWork(11,'2026-09-27')];const d=deps({rows,complete:false});
 const response=await handleBpayNextPreparationPage(page('CURRENT_WORK',{candidate_id:CANDIDATE,limit:2}),d);
 assert.equal(response.status,200);assert.deepEqual(await response.json(),{ok:true,kind:'CURRENT_WORK',rows,complete:false,
  next_cursor:{after_week:'2026-09-27',after_work_id:id(11)}});
 assert.deepEqual(d.calls[0].args,{candidate_id:CANDIDATE,after_week:null,after_work_id:null,limit:2});
 assert.equal(response.headers.get('x-bpay-next-preparation-wake'),null);
 const withdrawn={...currentWork(),approval_state:'WITHDRAWN',current_revision_id:null,position_ready:false,source_pay_channel:null,
  timesheet_reference:null,client_display_name:null,approved_source_ex_vat:null};
 const actual=await handleBpayNextPreparationPage(page('CURRENT_WORK',{candidate_id:CANDIDATE}),deps({rows:[withdrawn],complete:true}));
 assert.deepEqual((await actual.json()).rows,[withdrawn]);
 const first=await handleBpayNextPreparationPage(page('CURRENT_WORK',{candidate_id:CANDIDATE,after_week:'2026-09-27',after_work_id:id(11)}),
  deps({rows:[currentWork(3,'2026-10-04')],complete:true}));assert.equal(first.status,200);
 for(const args of [{candidate_id:CANDIDATE,after_week:'2026-09-27'},{candidate_id:CANDIDATE,after_work_id:WORK},
  {candidate_id:CANDIDATE,after_week:'2026-02-30',after_work_id:WORK}])await failed(page('CURRENT_WORK',args),deps(),'REQUEST_INVALID',400,false,true);
 for(const rows of [[currentWork(),currentWork()],[currentWork(11),currentWork(3)],[{...currentWork(),candidate_id:ACTOR}],
  [{...withdrawn,position_ready:true}],[{...currentWork(),approved_source_ex_vat:120}],[{...currentWork(),payable:true}]])
  await failed(page('CURRENT_WORK',{candidate_id:CANDIDATE}),deps({rows,complete:true}),'RESPONSE_INVALID',503,false,true);
});
const statusHeader=()=>({run_id:RUN,run_status:'DRAFT',selection_state:'SEALED',review_revision:SEQ,run_worker_id:WORKER,candidate_id:CANDIDATE,worker_status:'READY',
 target_pay_channel:'PAYE',umbrella_id:null,preparation_revision:'1',case_selection_revision:'0',net_request_revision:'2',net_projection_revision:'1',net_pending:true,
 review_issue_code:null,review_issue_work_id:null,frozen_gross_ex_vat:'120.00',frozen_gross_vat:'0.00',frozen_gross_inc_vat:'120.00',
 projection_id:REV,projection_input_kind:'PAYE_MANUAL',entered_paye_net:'85.00',accepted_recoveries:'0.00',accepted_net_additions:'0.00',cash_amount:'85.00'});
const storedCredit=(n=11)=>({legacy_case_id:id(n),candidate_id:CANDIDATE,principal_source_ex_vat:'9007199254740993.01',
 original_source_pay_channel:'UMBRELLA',original_tax_treatment:'NON_TAXABLE',original_routing_kind:'ONE_OFF_SPECIFIED_BANK_ACCOUNT',
 original_created_at_utc:'2026-09-28T09:00:00Z',bank_version_at_utc:'2026-09-28T09:01:00+00:00',
 bank_details_hash:'retained-hash',beneficiary_name:'Existing one-off payee',account_last4:'0042'});
test('CURRENT_STORED_CREDITS is one Candidate-scoped factual masked origin page with exact cursor and money text',async()=>{
 const rows=[storedCredit(11),storedCredit(12)],d=deps({rows,complete:false,scan_cursor:id(14)});
 const response=await handleBpayNextPreparationPage(page('CURRENT_STORED_CREDITS',{candidate_id:CANDIDATE,after_legacy_case_id:id(10),limit:2}),d);
 assert.equal(response.status,200);assert.deepEqual(await response.json(),{ok:true,kind:'CURRENT_STORED_CREDITS',rows,complete:false,next_cursor:{after_legacy_case_id:id(14)}});
 assert.equal(d.calls.length,1);assert.deepEqual(d.calls[0].args,{candidate_id:CANDIDATE,after_legacy_case_id:id(10),limit:2});
 assert.equal(d.calls[0].options.actorUserId,ACTOR);assert.equal(d.calls[0].options.routeClass,'READ');
 assert.equal(response.headers.get('x-bpay-next-preparation-wake'),null);
 const empty=await handleBpayNextPreparationPage(page('CURRENT_STORED_CREDITS',{candidate_id:CANDIDATE}),deps({rows:[],complete:true,scan_cursor:null}));
 assert.deepEqual(await empty.json(),{ok:true,kind:'CURRENT_STORED_CREDITS',rows:[],complete:true,next_cursor:null});
});
test('CURRENT_STORED_CREDITS rejects foreign Candidate, wrong origin, numeric money, full bank data and forged eligibility',async()=>{
 const request=()=>page('CURRENT_STORED_CREDITS',{candidate_id:CANDIDATE});
 for(const change of [{candidate_id:ACTOR},{original_source_pay_channel:'PAYE'},{original_tax_treatment:'TAXABLE'},
   {original_routing_kind:'CANDIDATE_BANK_ACCOUNT'},{principal_source_ex_vat:40},{principal_source_ex_vat:'0.00'},
   {principal_source_ex_vat:'-1.00'},{principal_source_ex_vat:'1e2'},{account_last4:'12345678'},
   {account_last4:42},{account_number:'12345678'},{sort_code:'123456'},{eligible:true},{bound:false},
   {bank_details_hash:''},{bank_details_hash:'é'.repeat(129)},{beneficiary_name:' '},{bank_version_at_utc:'infinity'}]){
   await failed(request(),deps({rows:[{...storedCredit(),...change}],complete:true,scan_cursor:id(11)}),'RESPONSE_INVALID',503,false,true);
 }
 for(const rows of [[storedCredit(12),storedCredit(11)],[storedCredit(),storedCredit()]])
   await failed(request(),deps({rows,complete:true,scan_cursor:id(12)}),'RESPONSE_INVALID',503,false,true);
 await failed(page('CURRENT_STORED_CREDITS',{candidate_id:CANDIDATE,after_legacy_case_id:id(11)}),
   deps({rows:[storedCredit(11)],complete:true,scan_cursor:id(11)}),'RESPONSE_INVALID',503,false,true);
 await failed(request(),deps({rows:[],complete:true,kind:'CURRENT_STORED_CREDITS'}),'RESPONSE_INVALID',503,false,true);
});
test('CURRENT_STORED_CREDITS accepts bounded empty scan progress only with a strictly advancing certified cursor',async()=>{
 const request=()=>page('CURRENT_STORED_CREDITS',{candidate_id:CANDIDATE,after_legacy_case_id:id(10)});
 const response=await handleBpayNextPreparationPage(request(),deps({rows:[],complete:false,scan_cursor:id(12)}));
 assert.equal(response.status,200);assert.deepEqual(await response.json(),{ok:true,kind:'CURRENT_STORED_CREDITS',rows:[],complete:false,next_cursor:{after_legacy_case_id:id(12)}});
 for(const payload of [{rows:[],complete:false,scan_cursor:null},{rows:[],complete:false,scan_cursor:id(10)},
  {rows:[],complete:false,scan_cursor:id(9)},{rows:[],complete:false,scan_cursor:'bad'},
  {rows:[],complete:false},{rows:[storedCredit(12)],complete:false,scan_cursor:id(11)},
  {rows:[storedCredit()],complete:true,scan_cursor:null},{rows:[],complete:false,scan_cursor:id(12),eligible:true}])
  await failed(request(),deps(payload),'RESPONSE_INVALID',503,false,true);
 const terminal=await handleBpayNextPreparationPage(request(),deps({rows:[],complete:true,scan_cursor:id(12)}));
 assert.equal(terminal.status,200);assert.equal((await terminal.json()).next_cursor,null);
 // No relaxation of the ordinary fixed page protocol.
 await failed(page('CURRENT_CASES',{candidate_id:CANDIDATE}),deps({rows:[],complete:false,scan_cursor:id(12)}),'RESPONSE_INVALID',503,false,true);
});
test('CURRENT_STORED_CREDITS rejects caller actor/account/value and oversized pages before any read',async()=>{
 for(const args of [{candidate_id:CANDIDATE,actor_user_id:ACTOR},{candidate_id:CANDIDATE,amount:'40.00'},
   {candidate_id:CANDIDATE,account_last4:'0042'},{candidate_id:CANDIDATE,after_legacy_case_id:'not-an-id'},
   {candidate_id:CANDIDATE,limit:101}]){
   const d=deps({rows:[],complete:true});await failed(page('CURRENT_STORED_CREDITS',args),d,'REQUEST_INVALID',400,false,true);assert.equal(d.calls.length,0);
 }
});
const transfer=(n='1')=>({transfer_id:DETAIL,transfer_no:n,projection_id:REV,execution_kind:'BANK',status:'RETURNED',cash_amount:'85.00',original_transfer_id:null});
test('STATUS preserves accepted projection while explicitly pending, one original transfer cursor and no bank fields',async()=>{
 const header=statusHeader(),rows=[transfer()];const d=deps({header,rows,complete:false});
 const response=await handleBpayNextPreparationPage(page('STATUS',{run_id:RUN,run_worker_id:WORKER,limit:1}),d);
 assert.equal(response.status,200);assert.deepEqual(await response.json(),{ok:true,kind:'STATUS',header,rows,complete:false,next_cursor:{after_transfer_no:'1'}});
 assert.equal(d.calls[0].args.after_transfer_no,null);assert.equal(d.calls.length,1);
 assert.equal(response.headers.get('x-bpay-next-preparation-wake'),null);
 const empty={...header,projection_id:null,projection_input_kind:null,entered_paye_net:null,accepted_recoveries:null,accepted_net_additions:null,cash_amount:null};
 const read=await handleBpayNextPreparationPage(page('STATUS',{run_id:RUN,run_worker_id:WORKER}),deps({header:empty,rows:[],complete:true}));
 assert.equal(read.status,200);assert.equal((await read.json()).header.cash_amount,null);
 const payout={...header,projection_input_kind:'CASE_PAYOUT',entered_paye_net:null,accepted_net_additions:'35.00',cash_amount:'35.00'};
 assert.equal((await handleBpayNextPreparationPage(page('STATUS',{run_id:RUN,run_worker_id:WORKER}),deps({header:payout,rows:[],complete:true}))).status,200);
 const zero={...transfer(),execution_kind:'INTERNAL_ZERO',cash_amount:'0.00',status:'INTERNAL_SETTLED'};
 assert.equal((await handleBpayNextPreparationPage(page('STATUS',{run_id:RUN,run_worker_id:WORKER}),deps({header,rows:[zero],complete:true}))).status,200);
 for(const after_transfer_no of ['0','2147483648','01','1e2'])await failed(page('STATUS',{run_id:RUN,run_worker_id:WORKER,after_transfer_no}),deps(),'REQUEST_INVALID',400,false,true);
});
test('STATUS rejects foreign scope, false pending interpretation, invented money and free execution/account tree',async()=>{
 const req=()=>page('STATUS',{run_id:RUN,run_worker_id:WORKER});
 for(const header of [{...statusHeader(),run_id:ACTOR},{...statusHeader(),run_worker_id:ACTOR},{...statusHeader(),net_pending:false},
  {...statusHeader(),cash_amount:85},{...statusHeader(),entered_paye_net:'85e0'},{...statusHeader(),projection_id:null},
  {...statusHeader(),accepted_recoveries:null},{...statusHeader(),projection_input_kind:'CASE_PAYOUT'},
  {...statusHeader(),bank_account:{number:'not-accepted'}}])await failed(req(),deps({header,rows:[],complete:true}),'RESPONSE_INVALID',503,false,true);
 for(const rows of [[{...transfer(),transfer_no:1}],[{...transfer(),transfer_no:'2147483648'}],[{...transfer(),sort_code:'123456'}],
  [transfer('2'),transfer('1')]])await failed(req(),deps({header:statusHeader(),rows,complete:true}),'RESPONSE_INVALID',503,false,true);
});
test('timeout aborts one dependency without auto-repeat, mutation unknown and read known',async(t)=>{
  t.mock.timers.enable({apis:['setTimeout']});
  for(const read of [false,true]){
    const d=deps();let signal,calls=0;
    const hanging=async(name,args,options)=>{signal=options.signal;calls++;return new Promise(()=>{});};
    d.preparationRpc=hanging;d.preparationPageRpc=hanging;
    const pending=(read?handleBpayNextPreparationPage(page('WORKERS',{run_id:RUN}),d):handleBpayNextPreparationCommand(command(),d));
    for(let n=0;n<20&&!signal;n++)await new Promise(resolve=>setImmediate(resolve));
    assert.ok(signal);t.mock.timers.tick(8000);const response=await pending;const body=await response.json();
    assert.equal(body.error_code,'BPAY_NEXT_PREPARATION_DEPENDENCY_TIMEOUT');assert.equal(body.outcome_unknown,!read);assert.equal(body.retryable,true);
    assert.equal(signal.aborted,true);assert.equal(calls,1);
  }
});
test('saved selection schema/owners preserve nullable ALL evidence, exact FK, typed bounded page conservation and no financial fallback',()=>{
  const source=p=>readFileSync(new URL(p,import.meta.url),'utf8');
  const schema=source('../supabase/migrations/04102026_1241_bpay_next_component_selection.sql');
  const owners=source('../supabase/repeatable/04102026_0570_banking_pay_next_component_selection_v1.sql');
  const service=source('../supabase/repeatable/04102026_0575_banking_pay_next_preparation_service_v1.sql');
  assert.match(schema,/expected_revision_id uuid,/);assert.match(schema,/selection_mode='SUBSET' and expected_revision_id is not null/);
  assert.match(schema,/references private\.bpay_next_approved_line\(revision_id,id,component_key\)/);
  assert.match(schema,/expected_selected_component_count/);assert.match(schema,/captured_selected_component_count/);
  assert.match(owners,/return; -- original evidence, never re-read a newer revision/);
  assert.match(owners,/v_count<>v_page\.item_count/);assert.match(owners,/BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION/);
  assert.match(service,/request\.jwt\.claim\.role/);assert.match(service,/u\.is_active is true and u\.role::text='admin'/);
  assert.match(service,/grant execute on function public\.bpay_next_preparation_intake_v1\(uuid,text,jsonb\),\s*public\.bpay_next_preparation_page_v1\(uuid,text,jsonb\) to service_role/);
  assert.doesNotMatch(service,/grant[^;]*(?:anon|authenticated)/i);assert.doesNotMatch(owners+service,/timesheets_financials|pay_session|execute format|EXECUTE\s+v_/i);
  assert.match(service,/select case p_kind[\s\S]*order by l\.line_no,l\.id limit v_limit/);
  assert.match(service,/A byte boundary ends/);
  assert.match(service,/return private\.bpay_next_current_work_page_v1/);assert.match(service,/return private\.bpay_next_worker_status_page_v1/);
  assert.match(service,/2147483647/);
});
