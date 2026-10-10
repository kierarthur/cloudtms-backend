import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {
  BPAY_NEXT_HISTORY_SECTIONS as SECTIONS, handleBpayNextHistoryRead,
  createBpayNextHistoryRpc, validateBpayNextHistoryRequest, validateBpayNextHistoryResponse, projectBpayNextMemberOrigin
} from '../broker/src/bpay-next-history.js';
const actor = 'b8100000-0000-4000-8000-000000000001';
const candidate = 'b8100000-0000-4000-8000-000000000002';
const timesheet = 'b8100000-0000-4000-8000-000000000003';
const work = 'b8100000-0000-4000-8000-000000000004';
const consistency = 'LIVE_KEYSET_REFRESH_FOR_LATE_COMMIT';
const unavailable = ['MOVED_ROOT_FLAT_PAID_BINDING','CANONICAL_MULTI_CHANNEL_VECTOR','MOVE_COMPARISON_RECEIPT','GENERAL_APPROVAL_ACTOR_REASON_NOT_CAPTURED_IN_NEXT_ROWS'];
const call = (extra={}) => ({version:'PAYMENT_HISTORY_V1',candidate_id:candidate,timesheet_id:null,section:'MANIFEST',parent_id:null,context_id:null,component_key:null,after:null,upper:null,limit:50,...extra});
const manifest = (request=call()) => ({
  version:'PAYMENT_HISTORY_V1',candidate_id:candidate,work_id:request.timesheet_id ? work : null,
  sections:Object.entries(SECTIONS).map(([section,definition])=>({section,...definition,
    applicability:request.timesheet_id && ['CASES','COMMANDS'].includes(section) ? 'CANDIDATE_ONLY_USE_REFERENCED_PARENT':'SCOPE_QUALIFIED'})),
  terminal:true,next_cursor:null,consistency,scalar_encoding:'STRING_OR_NULL',
  unavailable:unavailable.concat(request.timesheet_id ? ['TIMESHEET_COMMAND_WITHOUT_WORK_OR_CASE_ANCESTRY']:[])
});
const request = value => new Request('https://office.invalid/api/banking/pay/next/history-page',{method:'POST',body:JSON.stringify(value)});
const deps = payload => ({requireOfficeUser:async()=>({id:actor,role:'admin'}),historyRpc:async()=>Response.json(payload)});
const page = (request, rows, terminal=true, upper=rows.at(-1)?.key ?? null) => ({
  version:'PAYMENT_HISTORY_V1',candidate_id:candidate,work_id:request.timesheet_id ? work : null,
  section:request.section,parent_id:request.parent_id,context_id:request.context_id,component_key:request.component_key,
  amount_scope:SECTIONS[request.section].amount_scope,consistency,rows,upper,terminal,
  next_cursor:terminal ? null : {version:'PAYMENT_HISTORY_CURSOR_V1',scope_kind:request.timesheet_id ? 'TIMESHEET':'CANDIDATE',
    scope_id:request.timesheet_id || request.candidate_id,section:request.section,parent_id:request.parent_id,
    context_id:request.context_id,component_key:request.component_key,upper,key:rows.at(-1).key}
});
function row(section, key) {
  const data=Object.fromEntries(SECTIONS[section].fields.map(field=>[field,null]));
  SECTIONS[section].keys.forEach(([field],index)=>{data[field]=key[index];});
  if(section==='POSITIONS') data.current_residual_source_ex_vat='-2.00';
  return {key,data};
}
test('all 52 allowlists, key/parent/state and manifest equal the frozen SQL registry',()=>{
  const registry=JSON.parse(fs.readFileSync(new URL('../docs/bpay-next/final-definer-closures.json',import.meta.url),'utf8'));
  const owner=registry.chains.find(row=>row.signature==='public.bpay_next_payment_history_page_v1(jsonb)');assert.ok(owner);
  const sql=fs.readFileSync(new URL('../'+owner.finalPhysical,import.meta.url),'utf8');
  const original=JSON.parse(sql.split('$history$')[1]);
  assert.equal(Object.keys(SECTIONS).length,52);
  for(const [name,value] of Object.entries(original)) assert.deepEqual(SECTIONS[name],{
    parent:value.parent,keys:value.keys,fields:value.fields.split(','),amount_scope:value.state
  });
  assert.equal(validateBpayNextHistoryResponse(manifest(),call()).sections.length,52);
  const ts=call({candidate_id:null,timesheet_id:timesheet});
  assert.equal(validateBpayNextHistoryResponse(manifest(ts),ts).work_id,work);
});
test('actual fixed RPC injects authenticated actor, abort and service headers without browser actor',async()=>{
  let fetched=0;
  const rpc=createBpayNextHistoryRpc({baseUrl:'https://agency-test.invalid/',headers:{Authorization:'Bearer controlled-test-only'},fetchImpl:async(url,init)=>{
    fetched++;assert.equal(url,'https://agency-test.invalid/rest/v1/rpc/bpay_next_payment_history_page_v1');
    assert.equal(init.redirect,'error');assert.ok(init.signal instanceof AbortSignal);
    assert.equal(new Headers(init.headers).get('authorization'),'Bearer controlled-test-only');
    assert.deepEqual(JSON.parse(init.body),{p_request:{...call(),actor_user_id:actor}});
    return Response.json(manifest());
  }});
  const result=await handleBpayNextHistoryRead(request(call()),{...deps(null),historyRpc:rpc});
  assert.equal(result.status,200);assert.equal((await result.json()).page.sections.length,52);assert.equal(fetched,1);
  await assert.rejects(rpc({...call(),actor_user_id:work},{actorUserId:actor,signal:new AbortController().signal}));
  assert.throws(()=>createBpayNextHistoryRpc({baseUrl:'https://username:password@agency.invalid'}));
});
test('unauthenticated/nonadmin/method and arbitrary fields refuse before any RPC',async()=>{
  let count=0;const historyRpc=async()=>{count++;throw Error('must not execute');};
  for(const user of [null,{id:actor,role:'viewer'}]) {
    const result=await handleBpayNextHistoryRead(request(call()),{historyRpc,requireOfficeUser:async()=>user});
    assert.ok([401,403].includes(result.status));
  }
  for(const changed of [{actor_user_id:actor},{rpc:'other'},{section:'toString'},{limit:101},{candidate_id:null},{upper:[work]},{section:'WORKS',parent_id:work}]) {
    const result=await handleBpayNextHistoryRead(request({...call(),...changed}),{historyRpc,requireOfficeUser:async()=>({id:actor,role:'admin'})});
    assert.equal(result.status,400);
  }
  assert.equal((await handleBpayNextHistoryRead(new Request('https://office.invalid'),deps(null))).status,405);
  assert.equal(count,0);
});
test('scope-bound continuation preserves complete cursor, NULL and decimal/64-bit text',()=>{
  const req=call({section:'REVISIONS',parent_id:work,limit:1});
  const r=row('REVISIONS',['9007199254740993']);
  r.data.approved_source_ex_vat='1234567890.123456'; r.data.source_pay_channel=null;
  const p=page(req,[r],false,['9007199254740995']);
  assert.equal(validateBpayNextHistoryResponse(p,req).rows[0].data.approved_source_ex_vat,'1234567890.123456');
  const next={...req,after:p.next_cursor,upper:p.upper};
  assert.equal(validateBpayNextHistoryRequest(next),next);
  for(const changed of [{candidate_id:work},{parent_id:candidate},{context_id:actor},{section:'SCHEDULE'},{upper:['9007199254740996']}])
    assert.throws(()=>validateBpayNextHistoryRequest({...next,...changed}));
  for(const changed of [{rows:[{...r,data:{...r.data,bank_account:'secret'}}]},{rows:[{...r,data:{...r.data,revision_no:9007199254740993}}]},
    {next_cursor:{...p.next_cursor,key:['9007199254740992']}},{terminal:true},{amount_scope:'WHOLE_TRANSFER_CASH'}])
    assert.throws(()=>validateBpayNextHistoryResponse({...p,...changed},req));
});
test('every section supports bounded empty response and manifest cannot expand privacy fields',()=>{
  for(const [section,definition] of Object.entries(SECTIONS)) {
    const req=call({section,parent_id:definition.parent==='SCOPE' ? null:work,component_key:section==='EFFECTS'?'component':null});
    validateBpayNextHistoryRequest(req);validateBpayNextHistoryResponse(page(req,[]),req);
  }
  const m=manifest();m.sections[0].fields=[...m.sections[0].fields,'raw_payload'];
  assert.throws(()=>validateBpayNextHistoryResponse(m,call()));
});
test('stream byte/UTF8 guards cancel and return only safe static error',async()=>{
  let cancelled=false;
  const body=new ReadableStream({start(controller){controller.enqueue(new Uint8Array(132000));},cancel(){cancelled=true;}});
  const result=await handleBpayNextHistoryRead(request(call()),{...deps(null),historyRpc:async()=>new Response(body)});
  assert.equal(result.status,502);assert.equal(cancelled,true);
  assert.deepEqual(await result.json(),{ok:false,error_code:'BPAY_NEXT_HISTORY_PAGE_TOO_LARGE',retryable:false});
  const bad=await handleBpayNextHistoryRead(request(call()),{...deps(null),historyRpc:async()=>new Response(new Uint8Array([255]))});
  assert.equal(bad.status,502);
  const denied=await handleBpayNextHistoryRead(request(call()),deps({code:'42501',message:'business secret ignored'}));
  assert.equal(denied.status,502); // successful malformed response never leaks its message
});
test('SQL refusals retain closed status without payload/logging and read stays activation-independent',async()=>{
  for(const [code,status] of [['42501',403],['22023',400],['54000',503],['PGRST202',503]]) {
    const result=await handleBpayNextHistoryRead(request(call()),{...deps(null),historyRpc:async()=>Response.json({code,message:'private-data',details:'secret'},{status:400})});
    assert.equal(result.status,status);assert.ok(!(await result.text()).includes('private-data'));
  }
  const broker=fs.readFileSync(new URL('../broker/src/index.js',import.meta.url),'utf8');
  const route=broker.indexOf("if (req.method === 'POST' && nextPath === '/api/banking/pay/next/history-page')");
  const nextGate=broker.indexOf('if (nextOfficeRoute) {');
  const oldGate=broker.indexOf("if (p.startsWith('/api/banking/'))");
  assert.ok(nextGate>0&&nextGate<route&&route<oldGate);
  const admission=broker.slice(nextGate,route);
  assert.match(admission,/const user = await requireUser\(env, req\);/);
  assert.match(admission,/if \(!user\) return withCORS\(env, req, unauthorized\(\)\)/);
  assert.match(admission,/if \(user\.role !== 'admin'\)/);
  assert.match(admission,/status: 403/);
  assert.doesNotMatch(admission,/sbRpc\(|sbHeaders\(env\)/);
  assert.ok(route<broker.indexOf("if (req.method === 'POST' && nextPath === '/api/banking/pay/next/frozen-detail')"));
  assert.match(broker.slice(route,route+480),/requireOfficeUser: async \(\) => user/);
  const owner=fs.readFileSync(new URL('../broker/src/bpay-next-history.js',import.meta.url),'utf8');
  assert.doesNotMatch(owner,/console\.|module_status|NEXT.*enabled|waitUntil|queue\.send/);
});

test('PS03 typed member projection keeps genuine zero Case and refuses wrong shapes/parent without money arithmetic',()=>{
  const data={...row('MEMBERS',['1']).data,transfer_id:work,run_worker_id:candidate,subject_kind:'WORK',run_line_id:actor,signed_cash_contribution:'12.34'};
  assert.equal(projectBpayNextMemberOrigin(data).origin.id,actor);
  assert.throws(()=>projectBpayNextMemberOrigin({...data,case_hold_id:timesheet}));
  const zero={...data,subject_kind:'CASE',run_line_id:null,case_instruction_id:actor,case_allocation_result_id:timesheet,signed_cash_contribution:'0.00'};
  assert.equal(projectBpayNextMemberOrigin(zero).origin.hold_id,null);
  assert.throws(()=>projectBpayNextMemberOrigin({...zero,signed_cash_contribution:'1.00'}));
  for(const subject_kind of ['NET_ADJUSTMENT','CASH_REISSUE']) {
    const member={...data,subject_kind,run_line_id:null};
    assert.equal(projectBpayNextMemberOrigin(member).origin.id,work);
    assert.throws(()=>projectBpayNextMemberOrigin({...member,run_line_id:actor}));
  }
  assert.throws(()=>projectBpayNextMemberOrigin({...data,signed_cash_contribution:12.34}));
  const own=call({section:'MEMBERS',parent_id:work});
  assert.equal(validateBpayNextHistoryResponse(page(own,[{key:['1'],data}]),own).rows[0].data.signed_cash_contribution,'12.34');
  assert.throws(()=>validateBpayNextHistoryResponse(page(own,[{key:['1'],data:{...data,transfer_id:actor}}]),own));
});
