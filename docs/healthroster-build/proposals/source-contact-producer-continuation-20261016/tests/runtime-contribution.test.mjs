import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {runWeeklySourceContactRecheck} from '../runtime/contact-recheck-runtime.mjs';
const base=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),root=path.resolve(base,'../../../..');
const recipe=JSON.parse(readFileSync(path.join(base,'RUNTIME_CONTRIBUTION.json'),'utf8'));
let source=execFileSync('git',['show',`${recipe.source_commit}:${recipe.source_path}`],{cwd:root,encoding:'utf8'}).replace(/\r\n/g,'\n');
for(const edit of recipe.edits){assert.equal(source.split(edit.before).length,2);source=source.replace(edit.before,edit.after);}
const start=source.indexOf('export async function runWeeklySourceDelivery('),end=source.indexOf('\nexport async function handleWeeklySourceDeliveryRuntime',start);
assert(start>=0&&end>start);const body=source.slice(start,end).replace(/^export /,'');
const id='00000000-0000-4000-8000-000000000010';
function actualFunction(events){
 const names=['text','canonicalServerUtc','rpc','renderDueIntents','prepareDispatchTargets','deliverTargets',
  'runWeeklySourceCompletedPackCopies','runPendingEntitlementRelease','safeErrorCode','weeklySourceDeploymentContext','runWeeklySourceContactRecheck'];
 const mark=name=>async()=>{events.push(name);return {ok:true};};
 const values=[value=>String(value??'').trim(),value=>new Date(value).toISOString(),
  async(deps,name,request)=>{events.push(name);return deps.rpc(name,{p_request:request});},mark('render'),mark('prepare'),mark('deliver'),
  mark('pack'),mark('release'),()=> 'SAFE_FAULT',()=>({agency_id:id,environment:'TEST'}),runWeeklySourceContactRecheck];
 return new Function(...names,body+'\nreturn runWeeklySourceDelivery;')(...values);
}
test('actual current host contribution runs existing scheduler then bounded recheck before original render/senders',async()=>{
 const events=[];const run=actualFunction(events);const result=await run({}, {rpc:async name=>{
  if(name==='weekly_source_contact_recheck_batch_v1'){events.push(name);return {ok:true,version:'CONTACT_RECHECK_BATCH_RESULT_V1',processed:0,
   held:0,selection_complete:false,results:[]};}return {ok:true};}}, {workerId:'actual-service-worker',limit:3,scheduledStartedAtUtc:'2026-10-16T12:00:00.000Z'});
 assert.deepEqual(events,['weekly_source_query_scheduler_tick_v1','weekly_source_contact_recheck_batch_v1','render','prepare','deliver','pack','release']);
 assert.equal(result.contact_recheck.selection_complete,false);assert.equal(result.delivery.ok,true);
});
test('actual host isolates unknown/malformed recheck outcome and retains all unrelated original delivery/release steps',async()=>{
 for(const mode of ['throw','malformed']){
  const events=[];const run=actualFunction(events);const result=await run({}, {rpc:async name=>{
   if(name==='weekly_source_contact_recheck_batch_v1'){events.push(name);if(mode==='throw')throw Error('private SQL');return {ok:true};}
   return {ok:true};}}, {workerId:'actual-service-worker',limit:3,scheduledStartedAtUtc:'2026-10-16T12:00:00.000Z'});
  assert.deepEqual(result.contact_recheck,{ok:false,error_code:'CONTACT_RECHECK_UNAVAILABLE',processed:0,held:0,results:[]});
  assert.deepEqual(events.slice(-5),['render','prepare','deliver','pack','release']);
 }
});
