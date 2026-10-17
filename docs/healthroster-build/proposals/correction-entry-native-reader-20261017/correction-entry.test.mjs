import test from 'node:test';
import assert from 'node:assert/strict';
import {decodeCorrectionEntry,handleCorrectionEntry,matchCorrectionEntry} from './correction-entry.mjs';
import {readCorrectionEntryForHost} from './office-entry.mjs';
const TS='10000000-0000-4000-8000-000000000001',ACTOR='10000000-0000-4000-8000-000000000002',AGENCY='10000000-0000-4000-8000-000000000003';
const request=(query='')=>new Request(`https://office.invalid/api/timesheets/${TS}/correction-entry${query}`);
const api=path=>`https://office.invalid${path}`;
const result=(extra={})=>({version:'TIMESHEET_CORRECTION_ENTRY_V1',requested_timesheet_id:TS,current_timesheet_id:TS,placement_revision:'9007199254740993',purpose:'CONTRACT_CORRECTION',may_open_review:false,disabled_reason:'REVIEW_UNAVAILABLE',...extra});
const user=(extra={})=>({id:ACTOR,is_active:true,role:'user',payment_authoriser:true,payment_golden_key:false,tenant_id:AGENCY,...extra});
const deps=(extra={})=>({timesheetId:TS,agencyId:AGENCY,requireOfficeUser:async()=>user(),rpc:async()=>result(),...extra});
test('exact path recognition has no loose operation/source route alias',()=>{
 assert.deepEqual(matchCorrectionEntry(request()),{timesheetId:TS});
 for(const suffix of ['/extra','/','%2f','/preview'])assert.equal(matchCorrectionEntry(new Request(request().url+suffix)),null);
});
test('active Admin, payment authoriser and golden key each use exact trusted actor RPC',async()=>{
 for(const actor of [user({role:'admin',payment_authoriser:false}),user(),user({payment_authoriser:false,payment_golden_key:true})]){
  const calls=[];const r=await handleCorrectionEntry(request(),deps({requireOfficeUser:async()=>actor,rpc:async(...args)=>{calls.push(args);return result();}}));
  assert.equal(r.status,200);assert.equal(r.headers.get('cache-control'),'no-store');assert.deepEqual(await r.json(),result());
  assert.deepEqual(calls,[['timesheet_correction_entry_v1',{p_request:{version:'TIMESHEET_CORRECTION_ENTRY_REQUEST_V1',timesheet_id:TS,actor_user_id:ACTOR}}]]);
 }
});
test('inactive and ordinary actors do not reach native RPC',async()=>{
 for(const actor of [user({is_active:false}),user({payment_authoriser:false})]){
  let calls=0;const r=await handleCorrectionEntry(request(),deps({requireOfficeUser:async()=>actor,rpc:async()=>{calls++;throw Error('must not run');}}));
  assert.equal(r.status,403);assert.equal(calls,0);
 }
});
test('missing session or cross-agency token is unauthorised before subject lookup',async()=>{
 for(const actor of [null,user({tenant_id:null}),user({tenant_id:TS})]){
  let calls=0;const r=await handleCorrectionEntry(request(),deps({requireOfficeUser:async()=>actor,rpc:async()=>{calls++;}}));
  assert.equal(r.status,401);assert.equal(calls,0);
 }
});
test('browser query spoof and invalid path never reach authentication or SQL',async()=>{
 for(const [req,timesheetId] of [[request('?actor_user_id='+TS),TS],[request('?agency_id='+TS),TS],[request(),'not-a-uuid']]){
  let called=false;const r=await handleCorrectionEntry(req,deps({timesheetId,requireOfficeUser:async()=>{called=true;}}));
  assert.equal(r.status,400);assert.equal(called,false);
 }
});
test('non-GET is fenced and missing trusted agency fails closed',async()=>{
 assert.equal((await handleCorrectionEntry(new Request(request().url,{method:'POST',body:'{}'}),deps())).status,405);
 assert.equal((await handleCorrectionEntry(request(),deps({agencyId:null}))).status,503);
});
test('SQL visibility refusal is generic and contains no subject data',async()=>{
 for(const error of [{fn:'timesheet_correction_entry_v1',status:403,json:{code:'42501',message:'private-id-secret'}},
  {fn:'timesheet_correction_entry_v1',status:403,body:JSON.stringify({code:'42501',details:'private-id-secret'})}]){
  const r=await handleCorrectionEntry(request(),deps({rpc:async()=>{throw error;}}));assert.equal(r.status,403);
  assert.deepEqual(await r.json(),{error:'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED'});
 }
});
test('missing PostgREST cache, timeout, wrong-function refusal and unexpected errors stay retryable/nonleaking',async()=>{
 for(const error of [{fn:'timesheet_correction_entry_v1',status:404,json:{code:'PGRST202',message:'private-id-secret'}},
  {fn:'timesheet_correction_entry_v1',status:408,body:'private-id-secret'},
  {fn:'other_rpc',json:{code:'42501',message:'private-id-secret'}},Error('private-id-secret')]){
  const r=await handleCorrectionEntry(request(),deps({rpc:async()=>{throw error;}}));assert.equal(r.status,503);
  assert.deepEqual(await r.json(),{error:'CORRECTION_ENTRY_UNAVAILABLE'});
 }
});
test('closed decoder keeps int63 strings above JS safe integer exact',()=>{
 assert.equal(decodeCorrectionEntry(result(),TS).placement_revision,'9007199254740993');
 assert.equal(decodeCorrectionEntry(result({placement_revision:'9223372036854775807'}),TS).placement_revision,'9223372036854775807');
 for(const value of [0,9007199254740992,'01','-1','1.0','1e3','9223372036854775808'])assert.throws(()=>decodeCorrectionEntry(result({placement_revision:value}),TS));
});
test('certified pending is nullable; fabricated/current mismatches are rejected',()=>{
 assert.equal(decodeCorrectionEntry(result({current_timesheet_id:null,disabled_reason:'CURRENT_ALLOCATION_PENDING'}),TS).current_timesheet_id,null);
 for(const extra of [{current_timesheet_id:null},{disabled_reason:'CURRENT_ALLOCATION_PENDING'},
  {requested_timesheet_id:ACTOR},{current_timesheet_id:'bad'}])assert.throws(()=>decodeCorrectionEntry(result(extra),TS));
});
test('original A/history B may resolve to C; canonical native UUIDs have no version/variant restriction',async()=>{
 const current='ffffffff-ffff-ffff-ffff-ffffffffffff',history='00000000-0000-0000-0000-000000000000';
 for(const requested of [TS,history]){
  const reply=result({requested_timesheet_id:requested,current_timesheet_id:current});
  assert.equal(decodeCorrectionEntry(reply,requested).current_timesheet_id,current);
  const r=await handleCorrectionEntry(new Request(`https://office.invalid/api/timesheets/${requested}/correction-entry`),deps({timesheetId:requested,rpc:async()=>reply}));
  assert.equal(r.status,200);assert.equal((await r.json()).current_timesheet_id,current);
 }
});
test('extra money/identity, Daily, false Ready and permissive future response cannot escape',async()=>{
 for(const extra of [{paid_amount:1},{client_id:AGENCY},{purpose:'DAILY_RATE_CORRECTION'},
  {may_open_review:true,disabled_reason:null},{disabled_reason:'private-id-secret'}, {version:'FUTURE'}]){
  const r=await handleCorrectionEntry(request(),deps({rpc:async()=>result(extra)}));assert.equal(r.status,503);
  assert.deepEqual(await r.json(),{error:'CORRECTION_ENTRY_UNAVAILABLE'});
 }
});
const binding=()=>({current:()=>true,initialTimesheetId:TS,context:{timesheetDetails:{timesheet:{sheet_scope:'WEEKLY'}}}});
test('native Office binding reads closed entry without opening a component or modal',async()=>{
 const calls=[];const entry=await readCorrectionEntryForHost({binding:binding(),timesheetId:TS,api,authFetch:async(...args)=>{calls.push(args);return Response.json(result());}});
 assert.equal(entry.may_open_review,false);assert.deepEqual(calls,[[request().url,{method:'GET',cache:'no-store'}]]);
});
test('Daily and wrong/current parent refuse before transport',async()=>{
 const daily=binding();daily.context.timesheetDetails.timesheet.sheet_scope='DAILY';
 await assert.rejects(readCorrectionEntryForHost({binding:daily,timesheetId:TS,authFetch:()=>{throw Error('must not run');}}),/CORRECTION_CURRENT_PARENT_REQUIRED/);
 const b=binding();b.initialTimesheetId=ACTOR;
 await assert.rejects(readCorrectionEntryForHost({binding:b,timesheetId:TS}),/CORRECTION_CURRENT_PARENT_REQUIRED/);
});
test('actor/frame changed while fetching or decoding cannot adopt prior response',async()=>{
 let current=true;const b=binding();b.current=()=>current;
 await assert.rejects(readCorrectionEntryForHost({binding:b,timesheetId:TS,api,authFetch:async()=>{current=false;return Response.json(result());}}),/CORRECTION_CURRENT_PARENT_REQUIRED/);
 current=true;
 await assert.rejects(readCorrectionEntryForHost({binding:b,timesheetId:TS,api,authFetch:async()=>({ok:true,json:async()=>{current=false;return result();}})}),/CORRECTION_CURRENT_PARENT_REQUIRED/);
});
