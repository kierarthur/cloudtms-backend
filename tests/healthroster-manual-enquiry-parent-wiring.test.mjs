import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {fixture,copy} from '../docs/healthroster-build/v6-owner-delivery-20261016/OFFICE_MANUAL_ENQUIRY_PARENT_REVIEW_20261016/reference/fixture.mjs';
const repoRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),dir=path.join(repoRoot,'docs/healthroster-build/v6-owner-delivery-20261016/OFFICE_MANUAL_ENQUIRY_PARENT_REVIEW_20261016');
const proof=JSON.parse(fs.readFileSync(path.join(dir,'REVERSE_PROOF.json'))),root=repoRoot;
const hash=s=>createHash('sha256').update(s).digest('hex');
const read=p=>fs.readFileSync(path.join(dir,p),'utf8').replaceAll('\r\n','\n');
// Reverse only the exact reviewed Source contribution before the older Manual
// parent guards. Runtime cases below still execute the full current router.
const sourceRecipeBytes=fs.readFileSync(path.join(root,'docs/healthroster-build/v6-owner-delivery-20261016/SOURCE_PAY_HTTP_PARENT_REVIEW_20261016/PARENT_EDITS.json'));
assert.equal(hash(sourceRecipeBytes),'b9f136f94a0be936682d07c92943a7e1e0db912977b70ed4f3e17b9cfee33980');
const sourceRecipe=JSON.parse(sourceRecipeBytes);
assert.equal(sourceRecipe.checkpoint,'39aa5b30042711956b83a7bf899f416d2f3b4771');
const sourceRoute=sourceRecipe.files.find(row=>row.target===proof.routes.source);
assert.equal(sourceRoute.edits.length,4);
const actualRouteRaw=fs.readFileSync(path.join(root,proof.routes.source),'utf8');
const laterKeepParentRecipes=[
  {
    "path": "docs/healthroster-build/proposals/kept-current-public-read-successor-20261017/ROUTE_EDITS.json",
    "sha256": "34d34ac3f66371fa1df4782bd8840efb04d7c85c610b4e6aa680677a309426bc"
  },
  {
    "path": "docs/healthroster-build/proposals/source-later-accept-current-successor-20261017/ROUTE_EDITS.json",
    "sha256": "7acde76446945888586f03ce9b017ff1b61a1ac94852fb9e159172a6a21ca438"
  }
].map(pin=>{
 const bytes=fs.readFileSync(path.join(root,pin.path));assert.equal(hash(bytes),pin.sha256);return JSON.parse(bytes);
});
function reverseLaterKeepParent(source){
 for(const recipe of laterKeepParentRecipes){
  assert.equal(hash(source),recipe.outputSha256);
  for(const h of [...recipe.hunks].reverse()){assert.equal(source.split(h.after).length,2);source=source.replace(h.after,()=>h.before);}
  assert.equal(hash(source),recipe.inputSha256);
 }return source;
}
const actualPriorRouteRaw=reverseLaterKeepParent(actualRouteRaw);
const keptRecipeBytes=fs.readFileSync(path.join(root,'docs/healthroster-build/proposals/kept-pay-rekey-current-root-successor-20261016/PARENT_EDITS.json'));
assert.equal(hash(keptRecipeBytes),'b82c3b0ab1a9e1282c5963e4db3b1eaa66948bc5383efa4d4a8821f65d2b2a7a');
const keptRecipe=JSON.parse(keptRecipeBytes);
assert.equal(keptRecipe.target,proof.routes.source);
assert.equal(keptRecipe.inputSha256,sourceRoute.postimageSha256);
assert.equal(keptRecipe.hunks.length,5);
function reverseKeptParent(source){
 assert.equal(hash(source),keptRecipe.outputSha256);
 for(const {before,after} of [...keptRecipe.hunks].reverse()){
  assert.equal(source.split(after).length,2);source=source.replace(after,()=>before);
 }
 assert.equal(hash(source),keptRecipe.inputSha256);return source;
}
let manualOnlyRaw=reverseKeptParent(actualPriorRouteRaw);
for(const {before,after,beforeSha256,afterSha256} of [...sourceRoute.edits].reverse()){
 assert.equal(hash(before),beforeSha256);assert.equal(hash(after),afterSha256);
 assert.equal(manualOnlyRaw.split(after).length,2);manualOnlyRaw=manualOnlyRaw.replace(after,before);
}
assert.equal(hash(manualOnlyRaw),sourceRoute.preimageSha256);
for(const input of proof.inputs)assert.equal(hash(input.path===proof.routes.source?Buffer.from(manualOnlyRaw):fs.readFileSync(path.join(root,input.path))),input.path===proof.routes.source?'34c284a8b19be9ad3320b88b2db62e4d805e297a84e3fa3b0fb245373a910121':input.rawSha256,input.path);
const proposed=fs.readFileSync(path.join(root,proof.routes.source),'utf8').replaceAll('\r\n','\n');
const manualOnly=manualOnlyRaw.replaceAll('\r\n','\n');
assert.equal(hash(manualOnly),proof.routes.afterLfSha256);
// Execute the complete proposed route; only URL resolution is adapted so its
// actual pinned I2 dependencies are loaded without writing that worktree.
const rootRoute=pathToFileURL(path.join(root,'broker/src/weekly-source/routes.js'));
const executable=proposed.replace(/(from\s*['"])(\.[^'"]+)(['"])/g,(_,a,b,c)=>a+new URL(b,rootRoute).href+c);
const {dispatchWeeklySourceRequest,WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT}=await import('data:text/javascript;base64,'+Buffer.from(executable).toString('base64'));
const id=n=>`f7160000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const env={CANDIDATE_APP_ENVIRONMENT:'TEST',MYTMS_OFFICE_AGENCY_ID:id(19)};
test('reviewed kept-parent successor refuses missing, duplicated and unrelated drift',()=>{
 assert.equal(hash(reverseKeptParent(actualPriorRouteRaw)),sourceRoute.postimageSha256);
 const anchor=keptRecipe.hunks[0].after;
 for(const source of [actualPriorRouteRaw.replace(anchor,''),actualPriorRouteRaw+anchor,actualPriorRouteRaw+'\n// drift'])
  assert.throws(()=>reverseKeptParent(source));
});
test('reviewed Later and public Keep parent successors refuse missing duplicated and unrelated drift',()=>{
 assert.equal(hash(reverseLaterKeepParent(actualRouteRaw)),keptRecipe.outputSha256);
 const anchor=laterKeepParentRecipes[0].hunks[0].after;
 for(const value of [actualRouteRaw.replace(anchor,''),actualRouteRaw+anchor,actualRouteRaw+'\n// drift'])
  assert.throws(()=>reverseLaterKeepParent(value));
});
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
 let restored=manualOnly;for(const {before,after} of [...proof.routes.replacements].reverse()){assert.equal(restored.split(after).length,2);restored=restored.replace(after,before);}
 assert.equal(restored,read('reference/routes.parent.js.txt'));assert.equal(hash(restored),proof.routes.beforeLfSha256);
 const sql=fs.readFileSync(path.join(root,'supabase/repeatable/includes/weekly_contact_manual_enquiry_office_rows_v1.sqlinc'),'utf8').replaceAll('\r\n','\n');
 assert.equal(hash(sql),proof.read.afterLfSha256);
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
