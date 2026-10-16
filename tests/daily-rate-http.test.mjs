// Transport/calculator unit tests only; mocked RPC replies are not native proof.
import test from 'node:test';
import assert from 'node:assert/strict';
import {handleDailyRateCorrectionPreview,handleDailyRateCorrectionAccept,handleDailyRateCorrectionGet,dispatchDailyRateCorrectionRequest,registerDailyRateCorrectionRoutes,createDailyRateCorrectionRpc,DAILY_RATE_CORRECTION_LIMITS} from '../broker/src/daily-rate-correction.js';
const id=n=>`00000000-0000-0000-0000-${String(n).padStart(12,'0')}`,sha='a'.repeat(64),version='DAILY_RATE_CORRECTION_V1';
const response=(value,status=200)=>new Response(JSON.stringify(value),{status,headers:{'content-type':'application/json'}});
const preview=()=>({version,operation_id:id(7),review_id:id(8),review_digest:sha,state_revision:'1',ready_to_confirm:true,current_timesheet_id:id(1),summary:{worked_facts_preserved:true,pay_effect:{visibility:'FULL',text:'Native payment comparison'},invoice_effect:{visibility:'FULL',text:'Native invoice comparison'}},known_blockers:[],confirm:{request_version:version,review_id:id(8),review_digest:sha,expected_state_revision:'1'},links:[]});
const result=()=>({version,purpose:'DAILY_RATE_CORRECTION',operation_id:id(7),state:'COMPLETE',state_revision:'4',placement:{status:'COMMITTED',original_timesheet_id:id(1),current_timesheet_id:id(1),placement_revision:'1',receipt_id:id(9)},financial:{status:'REGISTERED',command_id:id(10),cut_id:id(11),summary:{visibility:'FULL',text:'Native revision registered'}},billing:{status:'NOT_REQUIRED',revision:'1',group_id:null,visible_summary:null},next_actions:[],links:[]});
const browserPreview=()=>({version,request_id:id(12),timesheet_id:id(1),reason:' Updated native rates '});
const browserAccept=()=>({version,request_id:id(13),operation_id:id(7),review_id:id(8),review_digest:sha,expected_state_revision:'1'});
const request=(kind,body)=>new Request(`https://unit.invalid/api/timesheets/${id(1)}/daily-rate-correction/${kind}`,{method:'POST',headers:{'content-type':'application/json'},body:typeof body==='string'?body:JSON.stringify(body)});
function fixture(){
 const timesheet={timesheet_id:id(1),version:2,job_title_norm:'RN',band:null,worked_start_iso:'2026-10-01T08:00:00Z',worked_end_iso:'2026-10-01T14:00:00Z',authorised_at_server:'2026-10-02T10:00:00Z'};
 const candidate={id:id(3),pay_method:'PAYE',roles:['RN'],mileage_pay_rate:null};
 const rates={k:id(1),source_kind:'CANDIDATE_OVERRIDE',override_id:id(4),default_id:id(5),rate_type:'PAYE',pay_day:10,pay_night:null,pay_sat:null,pay_sun:null,pay_bh:null,charge_day:20,charge_night:null,charge_sat:null,charge_sun:null,charge_bh:null};
 const selection={k:id(1),candidate_id:id(3),client_id:id(6),role:'RN',band:null,date:'2026-10-01',rate_type:'PAYE'};
 const context={version:'DAILY_RATE_CONTEXT_V1',identity:{timesheet_id:id(1),timesheet_version:'2',candidate_id:id(3),client_id:id(6),contract_id:null,origin:'DAILY'},current_financial_id:id(2),context_digest:sha,unchanged_work_sha256:sha,
  authority_selection_args:selection,rate_authority:{selected_rates:rates,rate_provenance:{version:'DAILY_RATE_AUTHORITY_V1',pay:{kind:'CANDIDATE_OVERRIDE'},charge:{kind:'CLIENT_DEFAULT'}}},
  native_calculation_input:{out_timesheet:timesheet,out_cur_fin:{id:id(2),additional_units_json:{}},out_candidate:candidate,out_client_id:id(6),out_umbrella:null,out_policy:{},out_effective_flags:{}},native_claim_charge_authority:{mileage_charge_rate:null}};
 const calls=[],deps={requireUser:async()=>({id:id(20),is_active:true,role:'admin'}),toLocalParts:()=>({ymd:'2026-10-01'}),parseCandidateRoleCodes:()=>['RN'],normaliseRole:v=>v,normaliseBand:v=>v,
  subtractBreak:s=>s,classifyMinutes:()=>({hours_day:6,hours_night:0,hours_sat:0,hours_sun:0,hours_bh:0}),resolveEffectivePayChannel:()=>({ok:true}),
  rpc:async(name,args)=>{calls.push({name,args});if(name.endsWith('_context_v1'))return response(context);if(name==='tsfin_resolve_rates_batch')return response([rates]);if(name.endsWith('_preview_v1'))return response(preview());return response(result());}};
 return {context,rates,calls,deps};
}
test('Daily preview uses exact native selector and canonical builder; private context never becomes public DTO',async()=>{
 const {calls,deps}=fixture(),r=await handleDailyRateCorrectionPreview(request('preview',browserPreview()),{},deps);
 assert.equal(r.status,200);assert.deepEqual(await r.json(),preview());assert.equal(calls.length,3);
 assert.deepEqual(calls[0].args,{p_args:{timesheet_id:id(1),actor_user_id:id(20)}});
 assert.equal(calls[2].args.p_args.calculation_snapshot.total_pay_ex_vat,60);assert.equal(calls[2].args.p_args.calculation_snapshot.total_charge_ex_vat,120);
 assert.equal(calls[2].args.p_args.actor_user_id,id(20));assert.equal(calls[2].args.p_args.rate_provenance.worked_facts_sha256,sha);
 assert.ok(!('to_contract_id' in calls[2].args.p_args));
});
test('browser-supplied actor, money, Contract, duplicate escaped keys and path mismatch are rejected before RPC',async()=>{
 for(const body of [{...browserPreview(),actor_user_id:id(99)},{...browserPreview(),calculation_snapshot:{}},{...browserPreview(),to_contract_id:id(99)},{...browserPreview(),timesheet_id:id(2)},JSON.stringify(browserPreview()).slice(0,-1)+',"re\\u0061son":"other"}']){
  const {deps,calls}=fixture(),r=await handleDailyRateCorrectionPreview(request('preview',body),{},deps);assert.equal(r.status,400);assert.equal(calls.length,0);
 }
});
test('fresh active Office administrator is required before preview or acceptance',async()=>{
 for(const actor of [{id:id(20),is_active:false,role:'admin'},{id:id(20),is_active:true,role:'viewer'}]){
  const {deps,calls}=fixture();deps.requireUser=async()=>actor;
  const r=await handleDailyRateCorrectionAccept(request('accept',browserAccept()),{},deps);assert.ok([401,403].includes(r.status));assert.equal(calls.length,0);
 }
});
test('selected rates changing between native context and resolver do not produce a fresh calculation',async()=>{
 const {deps,context,calls}=fixture();context.rate_authority.selected_rates={...context.rate_authority.selected_rates,pay_day:99};
 deps.rpc=async(name,args)=>{calls.push({name,args});if(name.endsWith('_context_v1'))return response(context);if(name==='tsfin_resolve_rates_batch')return response([{...context.rate_authority.selected_rates,pay_day:10}]);return response({code:'22023'},400);};
 const r=await handleDailyRateCorrectionPreview(request('preview',browserPreview()),{},deps);assert.equal(r.status,409);assert.equal((await r.json()).error_code,'REVIEW_STALE');
 assert.equal(calls.at(-1).args.p_args.calculation_snapshot,null);
});
test('saved preview replay can return before a now-missing used rate is freshly admitted',async()=>{
 const {deps,context,calls}=fixture();context.rate_authority.selected_rates.pay_day=null;
 const r=await handleDailyRateCorrectionPreview(request('preview',browserPreview()),{},deps);assert.equal(r.status,200);assert.deepEqual(await r.json(),preview());
 assert.equal(calls.at(-1).args.p_args.context_digest,null);assert.equal(calls.at(-1).args.p_args.calculation_snapshot,null);
});
test('Accept repeats the exact browser UUID/body and performs no calculator, context, queue or automatic retry',async()=>{
 const {deps,calls}=fixture(),body=browserAccept();
 for(let i=0;i<2;i++){const r=await handleDailyRateCorrectionAccept(request('accept',body),{},deps);assert.equal(r.status,200);}
 assert.equal(calls.length,2);assert.equal(calls[0].name,'timesheet_daily_rate_correction_accept_v1');assert.deepEqual(calls[0],calls[1]);
 assert.deepEqual(calls[0].args.p_args,{...body,actor_user_id:id(20)});
});
test('malformed success or lost native Accept reply remains uncertain and retains request identity',async()=>{
 for(const reply of [async()=>new Response('{'),async()=>{throw new Error('private SQL/credential detail');},async()=>response({...result(),state:'invented'})]){
  const {deps}=fixture();let attempts=0;deps.rpc=async()=>{attempts++;return reply();};
  const r=await handleDailyRateCorrectionAccept(request('accept',browserAccept()),{},deps);assert.equal(r.status,503);assert.deepEqual(await r.json(),{ok:false,error_code:'DAILY_RATE_RESULT_UNCERTAIN',retryable:true});assert.equal(attempts,1);
 }
});
test('definitive SQL stale/permission failure is not reported as successful or uncertain acceptance',async()=>{
 for(const [code,status,expected] of [['40001',409,'REVIEW_STALE'],['42501',403,'DAILY_RATE_ACTOR_NOT_ALLOWED']]){
  const {deps}=fixture();deps.rpc=async()=>response({code,message:'sensitive native detail'},status);
  const r=await handleDailyRateCorrectionAccept(request('accept',browserAccept()),{},deps);assert.equal(r.status,status);assert.equal((await r.json()).error_code,expected);
 }
});
test('GET uses only actual operation and current actor and rejects Contract or unrelated results',async()=>{
 const {deps,calls}=fixture(),url=`https://unit.invalid/api/timesheets/daily-rate-correction/operations/${id(7)}`;
 let r=await handleDailyRateCorrectionGet(new Request(url),{},deps);assert.equal(r.status,200);assert.deepEqual(calls[0].args,{p_args:{version,operation_id:id(7),actor_user_id:id(20)}});
 deps.rpc=async()=>response({...result(),purpose:'CONTRACT_CORRECTION'});r=await handleDailyRateCorrectionGet(new Request(url),{},deps);assert.equal(r.status,502);
});
test('oversized browser data and invalid native cache return truthful dependency errors',async()=>{
 const {deps,calls}=fixture();let r=await handleDailyRateCorrectionPreview(request('preview',' '.repeat(DAILY_RATE_CORRECTION_LIMITS.requestBytes+1)),{},deps);assert.equal(r.status,413);assert.equal(calls.length,0);
 deps.rpc=async()=>response({code:'PGRST202',message:'native unavailable'},404);
 r=await handleDailyRateCorrectionGet(new Request(`https://unit.invalid/api/timesheets/daily-rate-correction/operations/${id(7)}`),{},deps);assert.equal(r.status,503);assert.equal((await r.json()).error_code,'DAILY_RATE_NATIVE_DEPENDENCY_UNAVAILABLE');
});
test('RPC transport allows only specified native functions and uses the supplied abort signal',async()=>{
 const controller=new AbortController();let sent;
 const rpc=createDailyRateCorrectionRpc({baseUrl:'https://unit.invalid',headers:{'x-unit':'trusted'},fetchImpl:async(url,options)=>{sent={url:String(url),options};return response({});}});
 await rpc('timesheet_daily_rate_correction_context_v1',{p_args:{timesheet_id:id(1),actor_user_id:id(20)}},{signal:controller.signal});
 assert.equal(sent.url,'https://unit.invalid/rest/v1/rpc/timesheet_daily_rate_correction_context_v1');assert.equal(sent.options.signal,controller.signal);assert.equal(sent.options.redirect,'error');
 await assert.rejects(()=>rpc('tsfin_write_snapshots_and_complete',{}, {signal:controller.signal}),/TRANSPORT_INVALID/);
});
test('specified route registration leaves ordinary Daily processing outside this module',()=>{
 const mounted=[];registerDailyRateCorrectionRoutes({post:(path)=>mounted.push(path),get:(path)=>mounted.push(path)},{});assert.equal(mounted.length,3);
 assert.equal(dispatchDailyRateCorrectionRequest(new Request('https://unit.invalid/api/timesheets/x/process-daily-manual'),{},null,{}),null);
});
