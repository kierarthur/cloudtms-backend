import test from 'node:test';
import assert from 'node:assert/strict';
import {dispatchWeeklySourceRequest} from '../../broker/src/weekly-source/routes.js';
import {parseGlobalContactSettingsSave,decodeGlobalContactSettingsState,readGlobalContactSettingsSave,globalContactSettingsOwner} from '../../broker/src/weekly-source/global-contact-timing.mjs';
const uuid='a2222222-2222-4222-8222-222222222222';
const settings=()=>({candidate_reminder_minutes:121,candidate_response_deadline_minutes:763,manager_digest_minutes:137,
 manager_manual_resend_cooldown_minutes:11,candidate_manual_reminder_cooldown_minutes:83,manager_secure_link_days:9,
 automatic_reconcile_actor_user_id:uuid,manager_first_delay_value:0,manager_first_delay_unit:'HOURS',
 manager_followups:[{sequence:1,interval_value:2,interval_unit:'CLIENT_WORKING_DAYS',mode:'MANUAL'}],
 manager_recent_completed_weeks:1,manager_max_work_age_days:14});
const state=()=>({ok:true,settings_version:'9007199254740993',policy_revision:'9007199254740993',settings:settings(),
 cutoff:{kind:'CLIENT_CUTOFF_PRECEDENCE',editable:false,explanation:'The actual Client cut-off overrides routine limits.'},
 actions:[{action:'SAVE_GLOBAL_CONTACT_TIMING',enabled:true,reason:null}]});
const save=()=>({request_id:uuid,expected_settings_version:'9007199254740993',settings:settings()});
test('closed request and response retain revisions above 2^53 exactly',()=>{
 assert.equal(parseGlobalContactSettingsSave(save()).expected_settings_version,'9007199254740993');
 assert.equal(decodeGlobalContactSettingsState(state()).settings_version,'9007199254740993');
 for(const revision of [9007199254740992,'01','+1','1e3','0','9223372036854775808']){
  const body=save();body.expected_settings_version=revision;assert.throws(()=>parseGlobalContactSettingsSave(body));
 }
});
test('old aliases, unknown fields and caller authority reject at existing route boundary',()=>{
 for(const key of ['manager_partial_digest_minutes','manager_manual_send_cooldown_minutes','can_send']){
  const body=save();body.settings[key]=1;assert.throws(()=>parseGlobalContactSettingsSave(body));
 }
 for(const key of ['actor_user_id','agency_id','environment','allowed_now']){
  const body=save();body[key]=uuid;assert.throws(()=>parseGlobalContactSettingsSave(body));
 }
});
test('vector accepts zero or ten ordered steps, rejects malformed values and coercion',()=>{
 for(const count of [0,10]){const body=save();body.settings.manager_followups=Array.from({length:count},(_,index)=>({sequence:index+1,interval_value:1,interval_unit:'HOURS',mode:'AUTOMATIC'}));assert.equal(parseGlobalContactSettingsSave(body).settings.manager_followups.length,count);}
 for(const change of [s=>s.manager_followups.push({...s.manager_followups[0]}),s=>s.manager_followups[0].interval_value=0,
  s=>s.manager_followups[0].sequence='1',s=>s.manager_followups[0].mode='AUTO',s=>s.manager_followups[0].extra=true,
  s=>s.manager_first_delay_value=0.5,s=>s.manager_first_delay_unit='DAYS',s=>s.manager_max_work_age_days=367,
  s=>s.manager_recent_completed_weeks='1',s=>s.manager_followups[0].interval_value=Infinity]){
  const body=save();change(body.settings);assert.throws(()=>parseGlobalContactSettingsSave(body));
 }
});
test('duplicate JSON keys including nested intervals reject before typed parsing',async()=>{
 for(const raw of [JSON.stringify(save()).replace('"request_id":','"request_id":"'+uuid+'","request_id":'),
  JSON.stringify(save()).replace('"sequence":1','"sequence":1,"sequence":1')]){
  await assert.rejects(()=>readGlobalContactSettingsSave(new Request('https://test.invalid',{method:'PUT',body:raw})));
 }
});
test('unqualified or altered response is dependency failure',()=>{
 for(const change of [b=>b.settings_version=1,b=>b.policy_revision='2',b=>b.actions[0].enabled='true',
  b=>b.cutoff.editable=true,b=>b.settings.manager_partial_digest_minutes=360,b=>b.extra=true,
  b=>delete b.settings.automatic_reconcile_actor_user_id]){const body=state();change(body);assert.throws(()=>decodeGlobalContactSettingsState(body),e=>e.status===503);}
});
test('native stale, replay conflict and current permission refusals map explicitly',async()=>{
 for(const [code,sqlstate,status] of [['WEEKLY_SOURCE_SETTINGS_STALE','40001',409],['WEEKLY_SOURCE_SETTINGS_REQUEST_REUSED','40001',409],['WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED','42501',403]]){
  await assert.rejects(()=>globalContactSettingsOwner(()=>Promise.reject({json:{message:code,code:sqlstate}})),e=>e.code===code&&e.status===status);
 }
 const unrelated={json:{message:'UNRELATED',code:'22023'}};await assert.rejects(()=>globalContactSettingsOwner(()=>Promise.reject(unrelated)),e=>e===unrelated);
});

test('body limit cancels an oversized stream and malformed UTF8 never reaches typed parsing',async()=>{
 let cancelled=false;
 const stream=new ReadableStream({pull(controller){controller.enqueue(new Uint8Array(65537));},cancel(){cancelled=true;}},{highWaterMark:0});
 await assert.rejects(()=>readGlobalContactSettingsSave(new Request('https://test.invalid',{method:'PUT',body:stream,duplex:'half'})));
 assert.equal(cancelled,true);
 await assert.rejects(()=>readGlobalContactSettingsSave(new Request('https://test.invalid',{method:'PUT',body:new Uint8Array([0xff])})));
});

async function route(body,{method='PUT',reply=state(),error,user}={}){
 const calls=[];
 const response=await dispatchWeeklySourceRequest(new Request('https://test.invalid/api/weekly-source/v1/settings/global',{
  method,...(method==='PUT'?{body:JSON.stringify(body),headers:{'content-type':'application/json'}}:{})
 }),{MYTMS_OFFICE_AGENCY_ID:uuid,CANDIDATE_APP_ENVIRONMENT:'TEST'},{},{
  requireUser:async()=>user??{id:uuid,role:'admin',is_active:true},
  rpc:async(...args)=>{calls.push(args);if(error)throw error;return reply;}
 });
 return{status:response.status,body:await response.json(),calls};
}
test('actual global HTTP owner injects current actor/deployment and round-trips the closed revision',async()=>{
 for(const method of ['GET','PUT']){
  const result=await route(save(),{method});assert.equal(result.status,200);assert.deepEqual(result.body,state());
  assert.deepEqual(result.calls[0][1],{p_request:{...(method==='PUT'?save():{}),actor_user_id:uuid,agency_id:uuid,environment:'TEST'}});
 }
});
test('actual global route rejects invalid schema before RPC, dependency corruption after RPC and permission loss',async()=>{
 for(const body of [{...save(),actor_user_id:uuid},{...save(),expected_settings_version:1},{...save(),settings:{...settings(),manager_partial_digest_minutes:360}}]){
  const result=await route(body);assert.equal(result.status,400);assert.equal(result.calls.length,0);
 }
 const corrupted=await route(save(),{reply:{...state(),settings_version:1}});assert.equal(corrupted.status,503);assert.equal(corrupted.calls.length,1);
 for(const [code,sqlstate,status] of [['WEEKLY_SOURCE_SETTINGS_STALE','40001',409],['WEEKLY_SOURCE_SETTINGS_REQUEST_REUSED','40001',409],['WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED','42501',403]]){
  const result=await route(save(),{error:{json:{message:code,code:sqlstate}}});assert.equal(result.status,status);assert.equal(result.body.error_code,code);assert.equal(result.calls.length,1);
 }
 const inactive=await route(save(),{user:{id:uuid,role:'admin',is_active:false}});assert.equal(inactive.status,401);assert.equal(inactive.calls.length,0);
});
