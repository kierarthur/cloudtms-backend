import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {fixture,copy} from './reference/fixture.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url));
const proof=JSON.parse(fs.readFileSync(path.join(dir,'REVERSE_PROOF.json'))),root=proof.rootWorktree;
const hash=s=>createHash('sha256').update(s).digest('hex');
const read=p=>fs.readFileSync(path.join(dir,p),'utf8').replaceAll('\r\n','\n');
for(const input of proof.inputs)assert.equal(hash(fs.readFileSync(path.join(root,input.path))),input.rawSha256,input.path);
const proposed=read('proposed/broker/src/weekly-source/routes.js');
// Execute the complete proposed route; only URL resolution is adapted so its
// actual pinned I2 dependencies are loaded without writing that worktree.
const rootRoute=pathToFileURL(path.join(root,'broker/src/weekly-source/routes.js'));
const executable=proposed.replace(/(from\s*['"])(\.[^'"]+)(['"])/g,(_,a,b,c)=>a+new URL(b,rootRoute).href+c);
const {dispatchWeeklySourceRequest,WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT}=await import('data:text/javascript;base64,'+Buffer.from(executable).toString('base64'));
const id=n=>`f7160000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const env={CANDIDATE_APP_ENVIRONMENT:'TEST',MYTMS_OFFICE_AGENCY_ID:id(19)};
async function run(action,options={}){
 const f=fixture(),payload=copy(action.endsWith('PREVIEW')?f.request:f.accept),calls=[];
 if(options.edit)options.edit(payload);
 const body={action,payload};
 const response=await dispatchWeeklySourceRequest(new Request('https://isolated.invalid/api/weekly-source/v1/commands'+(options.search||''),{
  method:'POST',body:options.raw??JSON.stringify(body),headers:{'content-type':'application/json'}}),options.env??env,{},
  {requireUser:async()=>options.user??{id:id(1),role:'admin',is_active:true},rpc:async(...args)=>{calls.push(args);if(options.error)throw options.error;return options.reply??copy(action.endsWith('PREVIEW')?f.result:f.saved);}});
 return {status:response.status,value:await response.json(),calls,payload};
}
test('four route contributions and one complete native read successor reverse to entire pinned parents',()=>{
 let restored=proposed;for(const {before,after} of [...proof.routes.replacements].reverse()){assert.equal(restored.split(after).length,2);restored=restored.replace(after,before);}
 assert.equal(restored,read('reference/routes.parent.js.txt'));assert.equal(hash(restored),proof.routes.beforeLfSha256);
 const sql=read('proposed/supabase/repeatable/includes/weekly_contact_manual_enquiry_office_rows_v1.sqlinc');
 assert.equal(sql.replace(proof.read.after,proof.read.before),read('reference/office_rows.parent.sqlinc'));
 assert.equal(sql.split('create or replace function').length-1,1);assert.match(sql,/item->>'section'='questions'/);
 assert.match(sql,/weekly_contact_manual_enquiry_read_v1\(source_group.agency_id,source_group.environment,candidate,client\)/);
 assert.match(sql,/revoke all .* from public,anon,authenticated,service_role;/);
});
for(const action of ['CONTACT_MANUAL_ENQUIRY_PREVIEW','CONTACT_MANUAL_ENQUIRY_ACCEPT'])test('complete actual route dispatches '+action+' once through real typed adapter',async()=>{
 const r=await run(action);assert.equal(r.status,200);assert.equal(r.calls.length,1);
 assert.deepEqual(r.calls[0],[WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.contactManualEnquiryRpcCommands[action],{p_request:{...r.payload,actor_user_id:id(1),agency_id:id(19),environment:'TEST'}},{timeoutMs:20000}]);
 assert.equal(r.value.scope_results[0].scope_revision,'9007199254740993');
 if(action.endsWith('ACCEPT'))assert.equal(r.value.provider_submission_started,false);
});
test('actual route refuses spoofed actor/deployment, numeric witnesses, payment-only role, inactive actor and duplicate JSON keys without RPC',async()=>{
 for(const options of [{edit:p=>p.actor_user_id=id(99)},{edit:p=>p.agency_id=id(99)},{edit:p=>p.scopes[0].expected_scope_revision=9007199254740992},
  {user:{id:id(1),role:'pay',is_active:true,payments:true}},{user:{id:id(1),role:'admin',is_active:false}},
  {raw:'{"action":"CONTACT_MANUAL_ENQUIRY_PREVIEW","action":"CONTACT_MANUAL_ENQUIRY_PREVIEW","payload":{}}'},
  {search:'?actor_user_id='+id(99)},{env:{CANDIDATE_APP_ENVIRONMENT:'TEST'}}]){
  const r=await run('CONTACT_MANUAL_ENQUIRY_PREVIEW',options);assert.ok(r.status>=400);assert.equal(r.calls.length,0);
 }
});
test('known native stale refusal is exact, foreign errors are unavailable, and no response starts another attempt',async()=>{
 const fn='weekly_source_contact_manual_enquiry_accept_v1';
 const r=await run('CONTACT_MANUAL_ENQUIRY_ACCEPT',{error:{fn,json:{message:'CONTACT_MANUAL_ENQUIRY_REVIEW_STALE'}}});
 assert.equal(r.status,409);assert.equal(r.value.error_code,'CONTACT_MANUAL_ENQUIRY_REVIEW_STALE');assert.equal(r.calls.length,1);
 const other=await run('CONTACT_MANUAL_ENQUIRY_ACCEPT',{error:{fn:'unrelated',json:{message:'CONTACT_MANUAL_ENQUIRY_REVIEW_STALE'}}});
 assert.equal(other.status,503);assert.equal(other.value.error_code,'CONTACT_MANUAL_ENQUIRY_DEPENDENCY_UNAVAILABLE');assert.equal(other.calls.length,1);
});
