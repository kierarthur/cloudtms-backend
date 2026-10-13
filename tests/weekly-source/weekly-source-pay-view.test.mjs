import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
import { dispatchWeeklySourceRequest, WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT } from '../../broker/src/weekly-source/routes.js';
import { isActivePaymentActor } from '../../broker/src/payment-actor-permission.js';
import { weeklySourceAuthoriseRouting } from '../../broker/src/weekly-source/authorise-routing.mjs';
import { weeklySourceOfficePresentationInternals } from '../../broker/src/index.js';

const id='f7100000-0000-4000-8000-000000000002';
const root='f7100000-0000-4000-8000-000000000030';
const authoriser={id,is_active:true,role:'user',payment_authoriser:true,payment_golden_key:false};
const request=(path,body,method=body===undefined?'GET':'POST')=>new Request(`https://test.invalid/api/weekly-source/v1/${path}`,
  {method,...(body===undefined?{}:{headers:{'content-type':'application/json'},body:JSON.stringify(body)})});
function deps(user=authoriser){
  const calls=[];
  return {calls,requireUser:async(_env,_req,roles)=>{assert.deepEqual(roles,[]);return user;},
    rpc:async(name,args)=>{calls.push({name,args});return {ok:true};},
    orchestrateProtectedAction:async(input)=>{calls.push(input);return {ok:true};}};
}

test('non-admin pay actors receive the server-selected Queries landing and bound real actor',async()=>{
  for(const user of [authoriser,{...authoriser,payment_authoriser:false,payment_golden_key:true}]){
    const dependencies=deps(user);
    const response=await dispatchWeeklySourceRequest(request('workspace'),{},{},dependencies);
    assert.equal(response.status,200);
    assert.deepEqual(dependencies.calls,[{name:'weekly_source_office_workspace_v1',args:{p_request:{actor_user_id:id,tab:'queries'}}}]);
  }
});

test('pay view admits exact existing pay actions, retaining server actor and all owner proofs',async()=>{
  for(const action of ['APPROVE_PROTECTED_HOURS','AMEND_PROTECTED_HOURS','WITHDRAW_PROTECTED_HOURS','WAIT_FOR_SOURCE',
    'ACCEPT_SOURCE_AND_RECONCILE','RECORD_NOT_WORKED','ACCEPT_SYSTEM_HOURS','OPEN_MANUAL_REVIEW','RESOLVE_MANUAL_REVIEW']){
    const dependencies=deps();
    const payload={actor_user_id:root,expected_current_row_hash:'a'.repeat(64)};
    const response=await dispatchWeeklySourceRequest(request('commands',{action,payload}),{},{},dependencies);
    assert.equal(response.status,200,action);
    assert.equal(dependencies.calls.length,1,action);
    const call=dependencies.calls[0];
    const actual=call.request??call.args.p_request;
    assert.equal(actual.actor_user_id,id);
    assert.equal(actual.expected_current_row_hash,payload.expected_current_row_hash);
  }
});

test('non-admin direct admin/communications/invoice routes are refused before their owners run',async()=>{
  for(const path of ['uploads/preview','uploads/accept','settings/global','settings/source-groups',`timesheets/${root}/presentation`]){
    const dependencies=deps();
    const response=await dispatchWeeklySourceRequest(request(path,{},'POST'),{},{},dependencies);
    assert.equal(response.status,403,path);assert.equal(dependencies.calls.length,0);
    assert.equal((await response.json()).error_code,'WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED');
  }
  for(const action of ['ASK_CANDIDATES','SEND_MANAGER_NOW','REMIND_CANDIDATE','RECHECK_SOURCE','FINALISE_WEEK',
    'RECOVER_FINALISED_PAY','PREPARE_FINALISATION','APPLY_CORRECT_FINAL_SOURCE','NO_SHIFTS_TO_IMPORT',
    'ADMIT_SOURCE_INVOICE','MOVE_SOURCE_INVOICE','ACCEPT_NHSP_SOURCE_CHARGES','CHECKLIST','CHECKLIST_ISSUES']){
    const dependencies=deps();
    const response=await dispatchWeeklySourceRequest(request('commands',{action,payload:{}}),{},{},dependencies);
    assert.equal(response.status,403,action);assert.equal(dependencies.calls.length,0);
  }
  for(const tab of ['imports','finalise','history']){
    const dependencies=deps();
    assert.equal((await dispatchWeeklySourceRequest(request(`workspace?tab=${tab}`),{},{},dependencies)).status,403);
    assert.equal(dependencies.calls.length,0);
  }
});

test('combined reduced reads refuse forbidden sections even when the caller forges admin flags',async()=>{
  for(const payload of [{tab:'imports'},{section:'current'},{section:'archive'},{tab:'history'}]){
    const dependencies=deps();
    const response=await dispatchWeeklySourceRequest(request('commands',{
      action:'COMBINED_REVIEW_WORKSPACE',payload:{...payload,role:'admin',is_active:true,payment_golden_key:true}
    }),{},{},dependencies);
    assert.equal(response.status,403);assert.equal(dependencies.calls.length,0);
  }
  assert.equal(WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.directRpcCommands.CHECKLIST,undefined);
  assert.equal(WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.directRpcCommands.CHECKLIST_ISSUES,undefined);
});

test('missing/inactive actors fail 401 before flags; unflagged active users fail 403',async()=>{
  for(const [user,status,code] of [[null,401,'WEEKLY_SOURCE_AUTHENTICATION_REQUIRED'],
    [{...authoriser,is_active:false},401,'WEEKLY_SOURCE_AUTHENTICATION_REQUIRED'],
    [{...authoriser,payment_authoriser:false},403,'PAYMENT_PERMISSION_REQUIRED']]){
    const dependencies=deps(user);
    const response=await dispatchWeeklySourceRequest(request('workspace'),{},{},dependencies);
    assert.equal(response.status,status); assert.equal((await response.json()).error_code,code);
    assert.equal(dependencies.calls.length,0);
  }
});

const backend=fs.readFileSync(new URL('../../broker/src/index.js',import.meta.url),'utf8');
function block(name){
  const start=backend.search(new RegExp(`(?:async )?function ${name}\\(`)); assert.ok(start>=0);
  const ends=['\nfunction ','\nasync function '].map(marker=>backend.indexOf(marker,start+10)).filter(n=>n>start);
  return backend.slice(start,Math.min(...ends));
}
test('the actual shared lifecycle adapter preserves ordinary denial and Source dependency failures',async()=>{
  let applicable=true; let fail=false;
  const context={isActivePaymentActor,weeklySourceAuthoriseRouting,JSON_HEADERS:{'content-type':'application/json'},
    withCORS:(_env,_req,response)=>response,unauthorized:()=>new Response('{}',{status:401}),
    Response,JSON,sbRpc:async()=>{if(fail)throw new Error('PGRST202');return {contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable};}};
  vm.runInNewContext(`${block('weeklySourceScopedLifecycleActor')}\nthis.gate=weeklySourceScopedLifecycleActor;`,context);
  const source=await context.gate({},null,root,authoriser);assert.equal(source.ok,true);assert.equal(source.routing.bound,true);
  applicable=false;
  const ordinary=await context.gate({},null,root,authoriser);assert.equal(ordinary.ok,false);assert.equal(ordinary.response.status,401);
  fail=true;
  const unknown=await context.gate({},null,root,authoriser);assert.equal(unknown.response.status,503);
  assert.equal((await unknown.response.json()).error_code,'WEEKLY_SOURCE_AUTHORISE_ROUTING_UNAVAILABLE');
  const admin=await context.gate({},null,root,{...authoriser,role:'admin',payment_authoriser:false});
  assert.equal(admin.ok,true);assert.equal(admin.routing,null);
  for(const handler of ['handleTimesheetAuthoriseGeneric','handleTimesheetUnauthorise']){
    const body=block(handler);
    assert.ok(body.indexOf('weeklySourceScopedLifecycleActor')<body.indexOf('runCorrectionPairLifecycleIfApplicable'));
    assert.match(body,/weeklySourcePaymentActor: user/);
  }
});

test('Source detail/bulk hints use authenticated actor plus exact service scope and preserve ordinary payloads',async()=>{
  const attach=weeklySourceOfficePresentationInternals.attachWeeklySourceOfficeTimesheetPresentation;
  const payload={timesheet_id:root,sheet_scope:'WEEKLY',actions:{can_save:false},row:{ready_to_pay:false}};
  const projection={applicable:true,contract:'WEEKLY_SOURCE_OFFICE_PRESENTATION_V1',scope:'WEEKLY',root_timesheet_id:root,
    action_state:{authorise_allowed:true,unauthorise_allowed:true}};
  for(const actor of [{...authoriser,role:'admin',payment_authoriser:false},authoriser,
    {...authoriser,payment_authoriser:false,payment_golden_key:true}]){
    const result=await attach({},payload,id,async()=>projection,actor);
    for(const name of ['can_authorise','can_bulk_authorise','can_unauthorise']){
      assert.equal(result[name],true);assert.equal(result.row.actions[name],true);
    }
    assert.equal(result.actions.can_save,false);assert.equal(result.row.ready_to_pay,false);
  }
  for(const actor of [null,{...authoriser,id:root},{...authoriser,is_active:false},{...authoriser,payment_authoriser:false}]){
    const result=await attach({},payload,id,async()=>projection,actor);
    assert.equal(result.can_authorise,false);assert.equal(result.can_bulk_authorise,false);assert.equal(result.can_unauthorise,false);
  }
  assert.deepEqual(await attach({},payload,id,async()=>({applicable:false}),authoriser),payload);
  const locked={...payload,can_authorise:false,actions:{can_bulk_authorise:false},
    row:{actions:{can_unauthorise:false}}};
  const protectedResult=await attach({},locked,id,async()=>projection,authoriser);
  assert.equal(protectedResult.can_authorise,false);
  assert.equal(protectedResult.can_bulk_authorise,false);
  assert.equal(protectedResult.row.actions.can_unauthorise,false);
});
