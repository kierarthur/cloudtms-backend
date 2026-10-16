import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {webcrypto} from 'node:crypto';
import {parseGlobalContactSettingsSave,decodeGlobalContactSettingsState,readGlobalContactSettingsSave,globalContactSettingsOwner} from './test-runtime/global-contact-timing.mjs';
const root=new URL('.',import.meta.url);
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
function office({uncertain=false}={}){
 const requests=[];let server=state();let failed=false;
 const sandbox={module:{exports:{}},console,crypto:webcrypto,structuredClone,authFetch:async(url,options={})=>{
  if(url.endsWith('/source-groups'))return {ok:true,json:async()=>({source_groups:[]})};
  if(options.method==='PUT'){
   const body=JSON.parse(options.body);requests.push(body);parseGlobalContactSettingsSave(body);
   server={...state(),settings_version:'9007199254740994',policy_revision:'9007199254740994',settings:body.settings};
   if(uncertain&&!failed){failed=true;throw new Error('response lost');}
  }
  return {ok:true,json:async()=>structuredClone(server)};
 }};
 vm.runInNewContext(fs.readFileSync(new URL('forward/settings.js',root),'utf8'),sandbox);
 return {api:sandbox.module.exports,sandbox,requests};
}
function form(d){
 const values={weekly_source_candidate_reminder_hours:d.candidate_reminder_minutes/60,weekly_source_candidate_deadline_hours:d.candidate_response_deadline_minutes/60,
  weekly_source_manager_digest_hours:d.manager_digest_minutes/60,weekly_source_manager_resend_minutes:d.manager_manual_resend_cooldown_minutes,
  weekly_source_candidate_resend_minutes:d.candidate_manual_reminder_cooldown_minutes,weekly_source_manager_link_days:d.manager_secure_link_days,
  weekly_source_manager_first_delay_value:d.manager_first_delay_value,weekly_source_manager_first_delay_unit:d.manager_first_delay_unit,
  weekly_source_manager_recent_completed_weeks:d.manager_recent_completed_weeks,weekly_source_manager_max_work_age_days:d.manager_max_work_age_days};
 return {querySelector(selector){const name=selector.match(/name="([^"]+)"/)?.[1];return name&&Object.hasOwn(values,name)?{value:String(values[name])}:null;},
  querySelectorAll(selector){if(selector==='[data-manager-followup-index]')return d.manager_followups.map(step=>({querySelector(selector){return {value:selector.includes('value')?String(step.interval_value):selector.includes('unit')?step.interval_unit:step.mode};}}));return [];}};
}
test('Office render and edited timing save preserve canonical137/11 and all unrelated configured values',async()=>{
 const o=office(),ctx={};const loaded=await o.api.loadGlobal(ctx);
 assert.equal(loaded.settings_version,'9007199254740993');const html=o.api.globalHtml(loaded);
 assert.ok(html.includes('value="11"'));assert.ok(html.includes('Manager contact timing'));
 const edited=settings();edited.manager_first_delay_value=3;edited.manager_followups=[];
 o.sandbox.document={querySelector:()=>form(edited)};
 await o.api.saveGlobal(ctx);assert.equal(o.requests.length,1);
 const actual=o.requests[0].settings;
 for(const key of ['candidate_reminder_minutes','candidate_response_deadline_minutes','manager_digest_minutes','manager_manual_resend_cooldown_minutes','candidate_manual_reminder_cooldown_minutes','manager_secure_link_days','automatic_reconcile_actor_user_id'])assert.equal(actual[key],settings()[key],key);
 assert.equal(actual.manager_first_delay_value,3);assert.deepEqual(actual.manager_followups,[]);
 assert.equal(Object.hasOwn(actual,'manager_partial_digest_minutes'),false);assert.equal(Object.hasOwn(actual,'manager_manual_send_cooldown_minutes'),false);
});
test('lost save response retries identical native request identity/body',async()=>{
 const o=office({uncertain:true}),ctx={};const loaded=await o.api.loadGlobal(ctx);loaded.draft.manager_first_delay_value=1;loaded.dirty=true;
 await assert.rejects(()=>o.api.saveGlobal(ctx),/response lost/);await o.api.saveGlobal(ctx);
 assert.deepEqual(o.requests[0],o.requests[1]);assert.equal(ctx[o.api.stateKeys.GLOBAL_STATE].settings_version,'9007199254740994');
});
test('changed draft after uncertainty requires fresh read',async()=>{
 const o=office({uncertain:true}),ctx={};const loaded=await o.api.loadGlobal(ctx);loaded.draft.manager_first_delay_value=1;loaded.dirty=true;
 await assert.rejects(()=>o.api.saveGlobal(ctx));loaded.draft.manager_first_delay_value=2;
 await assert.rejects(()=>o.api.saveGlobal(ctx),/refresh settings/);assert.equal(o.requests.length,1);
 await o.api.loadGlobal(ctx,true);assert.equal(ctx[o.api.stateKeys.GLOBAL_STATE].draft.manager_first_delay_value,1);
});
test('fresh server canonical settings replace old cached aliases and defaults',async()=>{
 const o=office(),ctx={[o.api.stateKeys.GLOBAL_STATE]:{loaded:true,draft:{manager_partial_digest_minutes:360,manager_manual_send_cooldown_minutes:5}}};
 const loaded=await o.api.loadGlobal(ctx,true);assert.equal(loaded.draft.manager_digest_minutes,137);assert.equal(loaded.draft.manager_manual_resend_cooldown_minutes,11);
 assert.equal(Object.hasOwn(loaded.draft,'manager_partial_digest_minutes'),false);
});
