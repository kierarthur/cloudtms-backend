import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
import {assertTimesheetCorrectionOfficeActor} from '../../../broker/src/timesheet-correction-office-routes.mjs';
const pin=JSON.parse(fs.readFileSync(new URL('../proposals/install2-a0-auth-tenant-successor-20261014/CURRENT_AUTH_OWNER_SOURCE.json',import.meta.url))),env=pin.config.nonsecret_test_vars;
const actualIndex=fs.readFileSync(new URL('../../../broker/src/index.js',import.meta.url),'utf8');
for(const snippet of [pin.index.tenant_owner_source,pin.index.require_user_source])assert.equal(actualIndex.split(snippet).length,2,'Current root auth owner must equal the frozen full snippet');
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,current={id:id(1),role:'admin',is_active:true,session_version:7};
function auth(payload={},user=current,verified=true){
 const source=pin.index.tenant_owner_source+'\n'+pin.index.require_user_source+'\nreturn requireUser;';
 return Function('verifyToken','sessionSecret','sbGetUserById',source)(async()=>({ok:verified,payload:{typ:'access',sub:current.id,sv:7,
  tenant_id:env.BANKING_PAY_CORRECTION_TENANT_ID_V1,sid:'UNIT_ONLY',...payload}}),()=>undefined,async()=>user);
}
const request=()=>new Request('https://unit.invalid',{headers:{authorization:'Bearer UNIT_ONLY'}});
test('unchanged actual requireUser owner supplies its distinct configured signed-token tenant to final A0 actor admission',async()=>{
 const user=await auth()(env,request(),['admin']);assert.equal(user.tenant_id,env.BANKING_PAY_CORRECTION_TENANT_ID_V1);
 assert.notEqual(user.tenant_id,env.MYTMS_OFFICE_AGENCY_ID);assert.deepEqual(assertTimesheetCorrectionOfficeActor(env,user),{actor_user_id:current.id,environment:'TEST'});
});
test('actual current auth owner foreign/missing signed tenant cannot be substituted with Office agency or caller label',async()=>{
 for(const tenant_id of [env.MYTMS_OFFICE_AGENCY_ID,id(99),undefined]){
  const user=await auth({tenant_id})(env,request(),['admin']);assert.equal(user.tenant_id,null);
  assert.throws(()=>assertTimesheetCorrectionOfficeActor(env,user),/CORRECTION_ACTOR_NOT_ALLOWED/);
 }
});
test('actual current active/session-version/type/role verification order is retained before final actor admission',async()=>{
 for(const [payload,user,verified] of [[{}, {...current,is_active:false},true],[{}, {...current,session_version:8},true],
  [{typ:'refresh'},current,true],[{}, {...current,role:'pay'},true],[{},current,false]]){
  assert.equal(await auth(payload,user,verified)(env,request(),['admin']),null);
 }
});
