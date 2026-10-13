import assert from 'node:assert/strict';import test from 'node:test';
import fs from 'node:fs';import vm from 'node:vm';
import {dispatchWeeklySourceRequest} from '../../broker/src/weekly-source/routes.js';
const source=fs.readFileSync(new URL('../../broker/src/index.js',import.meta.url),'utf8');
function owner(name){
 const start=source.indexOf(`async function ${name}(`);
 assert.ok(start>=0);const end=source.indexOf('\n}',start);
 assert.ok(end>start);return source.slice(start,end+2);
}
const id='f7100000-0000-4000-8000-000000000002',tenant='f7100000-0000-4000-8000-000000000019';
function auth(row,token={typ:'access',sub:id,sv:7,tenant_id:tenant,sid:'test-session',iat:1}){
 const reads=[];
 const context={AUTH:{USERS_TABLE:'tms_users'},sbAuthHeaders:()=>({}),
  sessionSecret:()=>null,verifyToken:async()=>({ok:true,payload:token}),
  bankingPayCorrectionTenantAuthority:()=>tenant,
  fetch:async(url)=>{
   const query=new URL(url);reads.push(query);
   assert.equal(query.origin,'https://test.invalid');assert.equal(query.pathname,'/rest/v1/tms_users');
   assert.equal(query.searchParams.get('id'),'eq.'+id);
   const selected=query.searchParams.get('select').split(',');
   assert.deepEqual(selected,['id','email','role','is_active','password_hash','session_version','payment_authoriser','payment_golden_key']);
   // Model PostgREST projection exactly: omitted fields do not materialise.
   const projected=row===null?[]:[Object.fromEntries(selected.map(key=>[key,row[key]]))];
   return {json:async()=>projected};
  }};
 vm.createContext(context);
 vm.runInContext(owner('sbGetUserById')+'\n'+owner('requireUser')+'\nthis.readActor=requireUser;',context);
 return {reads,requireUser:context.readActor};
}
const active={id,email:'fixture@example.invalid',role:'user',is_active:true,password_hash:'not-a-login',
 session_version:7,payment_authoriser:false,payment_golden_key:false};
const request=()=>new Request('https://test.invalid/api/weekly-source/v1/workspace',
 {headers:{authorization:'Bearer verified-token-boundary-fixture'}});
test('actual current-user projection feeds requireUser and the real PAY route for both legal flag arms',async()=>{
 for(const flags of [{payment_authoriser:true,payment_golden_key:false},{payment_authoriser:true,payment_golden_key:true}]){
  const transport=auth({...active,...flags}),calls=[];
  const response=await dispatchWeeklySourceRequest(request(),{SUPABASE_URL:'https://test.invalid'},{},
   {...transport,rpc:async(name,args)=>{calls.push({name,args});return {ok:true};}});
  assert.equal(response.status,200);assert.equal(transport.reads.length,1);
  assert.deepEqual(calls,[{name:'weekly_source_office_workspace_v1',args:{p_request:{actor_user_id:id,tab:'queries'}}}]);
 }
});
test('current user state and session remain authoritative over flags forged in a verified token',async()=>{
 for(const [row,sv,status] of [[active,7,403],[{...active,is_active:false,payment_authoriser:true},7,401],
  [{...active,payment_authoriser:true},6,401],[null,7,401]]){
  const transport=auth(row,{typ:'access',sub:id,sv,tenant_id:tenant,role:'admin',payment_authoriser:true,payment_golden_key:true});
  let called=false;
  const response=await dispatchWeeklySourceRequest(request(),{SUPABASE_URL:'https://test.invalid'},{},
   {...transport,rpc:async()=>{called=true;return {ok:true};}});
  assert.equal(response.status,status);assert.equal(called,false);assert.equal(transport.reads.length,1);
 }
});
