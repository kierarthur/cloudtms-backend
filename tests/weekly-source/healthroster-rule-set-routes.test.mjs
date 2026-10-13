import test from 'node:test';
import assert from 'node:assert/strict';
import { dispatchWeeklySourceRequest, WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT } from '../../broker/src/weekly-source/routes.js';

const ACTOR='f7110000-0000-4000-8000-000000000001';
const AGENCY='f7110000-0000-4000-8000-000000000002';
const GROUP='f7110000-0000-4000-8000-000000000003';
const REQUEST='f7110000-0000-4000-8000-000000000004';
const env={MYTMS_OFFICE_AGENCY_ID:AGENCY,CANDIDATE_APP_ENVIRONMENT:'TEST'};
function run(path,method='GET',body,overrides={}) {
  const calls=[];
  const deps={requireUser:async (_req,_env,roles)=>{
    assert.ok(Array.isArray(roles)); return {id:ACTOR,role:'admin',is_active:true,payment_authoriser:false,payment_golden_key:false};
  },rpc:async (name,args)=>{calls.push({name,args}); return {contract:'FIXTURE',ok:true};},...overrides};
  return dispatchWeeklySourceRequest(new Request(`https://test.invalid/api/weekly-source/v1/settings/${path}`,{
    method,...(body===undefined?{}:{headers:{'content-type':'application/json'},body:JSON.stringify(body)})
  }),env,{},deps).then(async response=>({status:response.status,body:await response.json(),calls}));
}
test('rule catalogue GET carries only authenticated actor and server deployment',async()=>{
  const result=await run('rule-sets');
  assert.equal(result.status,200);
  assert.deepEqual(result.calls,[{name:'weekly_source_rule_sets_get_v1',args:{p_request:{actor_user_id:ACTOR,agency_id:AGENCY,environment:'TEST'}}}]);
});
test('rule save applies syntax defaults and leaves scope validation to the locked owner',async()=>{
  const result=await run('rule-sets','PUT',{source_group_id:GROUP,rule_set:{id:null,code:'HR_FIXTURE',display_name:'Fixture',expected_version:0},values:{checking_profile_kind:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1'},request_id:REQUEST});
  assert.equal(result.status,200);
  const request=result.calls[0].args.p_request;
  assert.equal(result.calls[0].name,'weekly_source_rule_set_version_save_atomic_v1');
  assert.equal(request.actor_user_id,ACTOR); assert.equal(request.agency_id,AGENCY);
  assert.equal(request.values.earliest_final_weekday,4); assert.equal(request.values.signed_comparison,'ON_EVERY_PUBLICATION');
  assert.equal(request.values.invoice_sender,'CLIENT_SELF_BILL');
  assert.equal(request.feature_enabled,undefined);
});
test('NULL assignment clearing uses path group, closed CAS request, no activation',async()=>{
  const result=await run(`source-groups/${GROUP}/rules`,'PUT',{rule_set_version_id:null,effective_from_week_ending:'2026-11-01',effective_to_week_ending:null,expected_group_version:2,request_id:REQUEST});
  assert.equal(result.status,200);
  assert.equal(result.calls[0].name,'weekly_source_group_rule_assign_atomic_v1');
  assert.equal(result.calls[0].args.p_request.source_group_id,GROUP);
  assert.equal(result.calls[0].args.p_request.rule_set_version_id,null);
  assert.equal(result.calls[0].args.p_request.feature_enabled,undefined);
});
test('client scope, activation flags and guessed capability cannot cross settings transport',async()=>{
  for(const extra of [{actor_user_id:ACTOR},{agency_id:AGENCY},{environment:'LIVE'},{feature_enabled:true},{feature_purpose:'RULE_SET'},{sourceFixedExpenses:true},{publishedSourceExpensesReady:true}]) {
    const result=await run('rule-sets','PUT',{source_group_id:GROUP,rule_set:{id:null,code:'HR_FIXTURE',display_name:'Fixture',expected_version:0},values:{checking_profile_kind:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1'},request_id:REQUEST,...extra});
    assert.equal(result.status,400); assert.equal(result.calls.length,0);
  }
});
test('invalid combinations and withdrawn agency switches are refused before mutation',async()=>{
  for(const values of [
    {checking_profile_kind:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',pay_membership:'ON_ACCEPTED_REPORT_OR_PROTECTED'},
    {checking_profile_kind:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',invoice_sender:'AGENCY_INVOICE',lookback_warning:'OFF'},
    {checking_profile_kind:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',invoice_sender:'AGENCY_INVOICE',signature_required:true},
  ]) {const result=await run('rule-sets','PUT',{source_group_id:GROUP,rule_set:{id:null,code:'HR_FIXTURE',display_name:'Fixture',expected_version:0},values,request_id:REQUEST});
    assert.equal(result.status,400); assert.equal(result.calls.length,0);}
});
test('admin refusal survives for catalogue, save and dated assignment',async()=>{
  for(const [path,method,body] of [['rule-sets','GET'],['rule-sets','PUT',{}],[`source-groups/${GROUP}/rules`,'PUT',{}]]) {
    const result=await run(path,method,body,{requireUser:async()=>{throw Object.assign(new Error('Forbidden'),{status:403,code:'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED'});}});
    assert.equal(result.status,403); assert.equal(result.calls.length,0);
  }
});
test('activation owner is unmounted and rule GET accepts no query context',async()=>{
  const denied=await run('rule-sets?agency_id='+AGENCY);
  assert.equal(denied.status,400); assert.equal(denied.calls.length,0);
  const activation=await run(`source-groups/${GROUP}/rules/activate`,'PUT',{});
  assert.equal(activation.status,404); assert.equal(activation.calls.length,0);
  assert.ok(WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.settingsRoutes.includes('/settings/rule-sets'));
  assert.ok(!WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.settingsRoutes.some(path=>path.includes('activate')));
});
test('locked owner refusal is a plain message and stale API cache is a truthful retryable dependency',async()=>{
  const body={rule_set_version_id:null,effective_from_week_ending:'2026-11-01',expected_group_version:1,request_id:REQUEST};
  const conflict=await run(`source-groups/${GROUP}/rules`,'PUT',body,{rpc:async()=>{throw {status:400,json:{code:'55000',message:'WEEKLY_SOURCE_RULE_FINALISED_WEEK_REFUSED'}};}});
  assert.equal(conflict.status,409);assert.equal(conflict.body.error_code,'WEEKLY_SOURCE_RULE_FINALISED_WEEK_REFUSED');
  assert.match(conflict.body.message,/already finalised/);
  const cache=await run('rule-sets','GET',undefined,{rpc:async()=>{throw {status:404,json:{code:'PGRST202',message:'Schema cache details'}};}});
  assert.equal(cache.status,503);assert.equal(cache.body.error_code,'WEEKLY_SOURCE_RULE_DEPENDENCY_UNAVAILABLE');
  assert.doesNotMatch(cache.body.message,/unchanged|were not changed|Schema cache details/);
});

