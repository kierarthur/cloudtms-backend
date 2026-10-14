import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import crypto from 'node:crypto';
import {prepareSubmittedDailyCycleAfterSave} from '../broker/src/weekly-source/submission-cycle-prepare.mjs';
import {currentDailySaveOwner,forwardDailySaveOwner,dailySaveFollowOn,dailySaveImport,indexSavedHash}
  from '../docs/healthroster-build/proposals/checklist-native-20261014/build-daily-save-packet.mjs';
import {officePath,officeSavedHash,officeReadinessEdits,applyOfficeReadinessEdits}
  from '../docs/healthroster-build/proposals/checklist-native-20261014/build-office-readiness-packet.mjs';
import {scanManagedSql,exactAtomicBody} from '../scripts/cloudtms-managed-release-sql.mjs';
const directory=new URL('../docs/healthroster-build/proposals/checklist-native-20261014/',import.meta.url);
const read=name=>fs.readFileSync(new URL(name,directory),'utf8');
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const actorUserId=id(1),timesheetId=id(2),H='a'.repeat(64);
const envelope=result=>({ok:true,contract:'WEEKLY_SOURCE_SUBMITTED_TIMESHEET_PREPARE_V1',actor_user_id:actorUserId,timesheet_id:timesheetId,result});
const genuine=()=>envelope({applies:true,source_group_id:id(3),client_id:id(4),week_ending:'2026-10-11',source_cycle_id:id(5),timesheet_id:timesheetId,submission_witness_sha256:H});
const copy=value=>structuredClone(value);
function makeRoute({body={},metadata=async()=>genuine(),saveError=null}={}){
  const counts={saves:0,metadata:0,audits:[],calls:[]};
  const ts={timesheet_id:timesheetId,sheet_scope:'DAILY',submission_mode:'MANUAL',version:1,is_current:true,reference_number:null};
  const fin={id:id(6),timesheet_id:timesheetId,is_current:true,processing_status:'PROCESSED',hours_day:8,total_hours:8};
  const dependencies={
    requireUser:async()=>({id:actorUserId}),withCORS:(_env,_req,res)=>res,
    unauthorized:()=>new Response('Denied',{status:401}),badRequest:message=>new Response(message,{status:400}),
    notFound:message=>new Response(message,{status:404}),serverError:message=>new Response(message,{status:500}),
    parseJSONBody:async()=>body,guardCurrentTimesheetWrite:async()=>({ok:true,resolved:{current_timesheet_id:timesheetId,requested_timesheet_id:timesheetId,current_version:1}}),
    nowIso:()=> '2026-10-14T18:00:00Z',sbGetOne:async(_env,url)=>url.includes('/timesheets_financials')?fin:ts,
    saveAndRecalculateCanonicalDaily:async(_env,request)=>{counts.saves++;counts.calls.push('SAVE');assert.equal(request.timesheetId,timesheetId);if(saveError)throw saveError;return {timesheet:ts,financials:fin};},
    writeAudit:async(_env,_actor,name)=>{counts.audits.push(name);counts.calls.push(`AUDIT:${name}`);},
    prepareSubmittedDailyCycleAfterSave,
    sbRpc:async(_env,name,args)=>{
      if(name==='weekly_source_submitted_timesheet_prepare_atomic_v1'){
        counts.metadata++;counts.calls.push('METADATA');assert.deepEqual(args,{p_request:{actor_user_id:actorUserId,timesheet_id:timesheetId}});return metadata();
      }
      if(name==='bulk_timesheet_row_patch_v1')return {row_json:{row_key:`timesheet:${timesheetId}`,row_signature:'native-current',row_patch:{timesheet_id:timesheetId}}};
      throw Error(`Unexpected native owner: ${name}`);
    },ok:value=>new Response(JSON.stringify(value),{status:200,headers:{'Content-Type':'application/json'}}),
    console:{log(){},warn(){}}
  };
  const factory=new Function(...Object.keys(dependencies),`return (${forwardDailySaveOwner()});`);
  return {route:factory(...Object.values(dependencies)),counts};
}
test('whole current Daily owner preserves every byte outside one post-save hook, with full reverse and separate import',()=>{
  const current=currentDailySaveOwner(),forward=forwardDailySaveOwner();
  assert.equal(forward.replace(dailySaveFollowOn,''),current);
  assert.equal(read('office-daily-save.forward.jsinc').split('\n').slice(1).join('\n'),forward);
  assert.equal(read('office-daily-save.reverse.jsinc').split('\n').slice(1).join('\n'),current);
  assert.equal(read('office-daily-save.import.jsinc'),dailySaveImport);
  const saved=fs.readFileSync(new URL('../broker/src/index.js',import.meta.url));
  assert.equal(crypto.createHash('sha256').update(saved).digest('hex'),indexSavedHash);
  assert.ok(forward.indexOf(dailySaveFollowOn)>forward.indexOf('const finalTs = await sbGetOne('));
  assert.ok(forward.indexOf(dailySaveFollowOn)>forward.indexOf("'TIMESHEET_PROCESSED'"));
  assert.ok(forward.indexOf(dailySaveFollowOn)<forward.indexOf('if (bulkPatchResponseRequested) {'));
  assert.equal((forward.match(/saveAndRecalculateCanonicalDaily\(env,/g)||[]).length,1);
  assert.doesNotMatch(dailySaveFollowOn,/body\.|Client|client_id|week_ending|source_group_id|Date\(|saveAndRecalculate/);
});
test('actual whole Daily standard/minimal/bulk callers run save and audits before one follow-on while keeping native success variants',async()=>{
  for(const body of [{},{bulk_process:true,minimal_lifecycle_mutation:true},{bulk_authorise:true}]){
    const {route,counts}=makeRoute({body});
    const response=await route({SUPABASE_URL:'https://test.invalid'},new Request('https://test.invalid/daily-save'),timesheetId);
    assert.equal(response.status,200);const value=await response.json();
    assert.equal(value.current_timesheet_id,timesheetId);assert.equal(counts.saves,1);assert.equal(counts.metadata,1);
    assert.ok(counts.calls.indexOf('SAVE')<counts.calls.indexOf('AUDIT:TIMESHEET_PROCESSED'));
    assert.ok(counts.calls.indexOf('AUDIT:TIMESHEET_PROCESSED')<counts.calls.indexOf('METADATA'));
    if(body.minimal_lifecycle_mutation){assert.equal(value.response_context,'minimal_lifecycle_mutation');assert.equal(value.timesheet,undefined);}
    else if(body.bulk_authorise){assert.equal(value.response_context,'bulk_authorise');assert.equal(value.row_signature,'native-current');}
    else assert.equal(value.timesheet.timesheet_id,timesheetId);
  }
});
test('PGRST202 or stale-witness follow-on failure after genuine save discloses saved=true, retains actual ID, and never retries business save',async()=>{
  for(const code of ['PGRST202','WEEKLY_SOURCE_SUBMISSION_CYCLE_WITNESS_STALE']){
    const {route,counts}=makeRoute({metadata:async()=>{throw Object.assign(Error('private diagnostic must not leak'),{code});}});
    const response=await route({SUPABASE_URL:'https://test.invalid'},new Request('https://test.invalid/daily-save'),timesheetId);
    assert.equal(response.status,502);const value=await response.json();
    assert.equal(value.saved,true);assert.equal(value.source_readiness,false);assert.equal(value.timesheet_id,timesheetId);
    assert.equal(value.error_code,'WEEKLY_SOURCE_SUBMISSION_CYCLE_READINESS_FAILED');
    assert.match(value.message,/Timesheet saved/);assert.match(value.message,/do not repeat the save/);
    assert.doesNotMatch(JSON.stringify(value),/private diagnostic|not saved|No changes were/);
    assert.equal(counts.saves,1);assert.equal(counts.metadata,1);assert.ok(counts.audits.includes('TIMESHEET_PROCESSED'));
  }
});
test('business save failure never calls metadata; native non-applicable/actual-submission negatives retain ordinary success',async()=>{
  const failed=makeRoute({saveError:Error('canonical failure')});
  assert.equal((await failed.route({SUPABASE_URL:'https://test.invalid'},new Request('https://test.invalid/daily-save'),timesheetId)).status,500);
  assert.equal(failed.counts.metadata,0);assert.equal(failed.counts.audits.length,0);
  for(const reason of ['NO_ACTUAL_SUBMISSION','NO_SIGNED_GROUP']){
    const ordinary=makeRoute({metadata:async()=>envelope({applies:false,reason})});
    assert.equal((await ordinary.route({SUPABASE_URL:'https://test.invalid'},new Request('https://test.invalid/daily-save'),timesheetId)).status,200);
    assert.equal(ordinary.counts.saves,1);assert.equal(ordinary.counts.metadata,1);
  }
});
test('closed metadata transport rejects actor/scope/witness/date/extra-field corruption without a second native call',async()=>{
  for(const mutate of [value=>{value.actor_user_id=id(30);},value=>{value.timesheet_id=id(30);},value=>{value.result.timesheet_id=id(30);},
    value=>{value.result.week_ending='2026-02-30';},value=>{value.result.submission_witness_sha256='fake';},
    value=>{value.result.client_id='fake';},value=>{value.extra=true;},value=>{value.result.synthetic_cycle=true;}]){
    const value=genuine();mutate(value);let calls=0;
    const result=await prepareSubmittedDailyCycleAfterSave({actorUserId,timesheetId,rpc:async()=>{calls++;return value;}});
    assert.equal(result.saved,true);assert.equal(result.ok,false);assert.equal(calls,1);
  }
  let calls=0;const result=await prepareSubmittedDailyCycleAfterSave({actorUserId:null,timesheetId,rpc:async()=>{calls++;}});
  assert.equal(result.saved,true);assert.equal(calls,0);
});
test('metadata-only replay repeats the same actual native IDs without another business-save owner or locally manufactured cycle',async()=>{
  const requests=[];
  const rpc=async(name,args)=>{requests.push({name,args});return genuine();};
  const first=await prepareSubmittedDailyCycleAfterSave({rpc,actorUserId,timesheetId});
  const replay=await prepareSubmittedDailyCycleAfterSave({rpc,actorUserId,timesheetId});
  assert.deepEqual(replay,first);assert.deepEqual(requests[0],requests[1]);
  const source=fs.readFileSync(new URL('../broker/src/weekly-source/submission-cycle-prepare.mjs',import.meta.url),'utf8');
  assert.doesNotMatch(source,/saveAndRecalculate|enqueue_ts|runTsfin|weekly_source_first_authorise|Date\.now|randomUUID/);
});
test('service RPC is closed, fresh active Admin only, and delegates actual submission/membership/lock order to the one lifecycle dispatcher',()=>{
  const source=fs.readFileSync(new URL('../supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_submission_cycle_service.sqlinc',import.meta.url),'utf8');
  assert.equal(scanManagedSql(exactAtomicBody(source)).statements.length,4);
  assert.match(source,/array\['actor_user_id','timesheet_id'\]/);assert.match(source,/u\.is_active/);assert.match(source,/actor\.role::text is distinct from 'admin'/);
  assert.ok(source.indexOf('weekly_source_query_require_service_v1')<source.indexOf('select u.* into actor'));
  assert.ok(source.indexOf('WEEKLY_SOURCE_SUBMISSION_CYCLE_ACTOR_FORBIDDEN')<source.indexOf('result:=private.weekly_source_checklist_submission_cycle_prepare_v1(timesheet_id,actor.id)'));
  assert.match(source,/revoke all.*from public,anon,authenticated,service_role/);assert.match(source,/grant execute.*to service_role/);
  assert.doesNotMatch(source,/insert into|update public\.|paid_at|timesheets_financials|p_now_utc|p_client_id|first_authorise|bpay_/i);
  const readSource=fs.readFileSync(new URL('../supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_native_checklist.sqlinc',import.meta.url),'utf8');
  assert.doesNotMatch(readSource,/submitted_timesheet_prepare_atomic|submission_cycle_prepare_v1/);
});
test('exact current Office transport carries saved/readiness JSON; all three consumer catches and save guards preserve truthful boundary',async()=>{
  const saved=fs.readFileSync(officePath);assert.equal(crypto.createHash('sha256').update(saved).digest('hex'),officeSavedHash);
  const current=saved.toString('utf8').replaceAll('\r\n','\n'),forward=applyOfficeReadinessEdits(current);
  assert.equal(applyOfficeReadinessEdits(forward,'reverse'),current);assert.equal(officeReadinessEdits().length,7);
  // Compile the complete resulting script without executing DOM/application code.
  new vm.Script(forward);
  const api=current.slice(current.indexOf('async function apiPostJson('),current.indexOf('async function apiPatchJson(')).trim();
  const failure=await prepareSubmittedDailyCycleAfterSave({actorUserId,timesheetId,rpc:async()=>{throw Error('PGRST202');}});
  const post=new Function('API','authFetch',`return (${api});`)(path=>path,async()=>new Response(JSON.stringify(failure),{status:502}));
  let error;try{await post(`/api/timesheets/${timesheetId}/daily-manual-upsert`,{});}catch(value){error=value;}
  assert.equal(error.status,502);assert.deepEqual(error.json,failure);assert.match(error.message,/Timesheet saved/);
  const context=vm.createContext({});vm.runInContext(read('office-daily-save-readiness.jsinc'),context);
  const proved=context.officeDailySavedReadiness(error);assert.equal(proved.saved,true);assert.equal(proved.safe_for_lifecycle,false);
  assert.match(proved.message,/Close and reopen/);assert.doesNotMatch(proved.message,/No changes were fully applied/);
  const state={};context.retainOfficeDailySavedReadiness(state,proved);
  assert.equal(context.blockedOfficeDailySavedReadiness(state,{timesheet_id:timesheetId}),proved);
  assert.equal(context.blockedOfficeDailySavedReadiness(state,{timesheet_id:id(30)}),null);
  assert.equal(context.blockedOfficeDailySavedReadiness({}, {timesheet_id:timesheetId}),null);
  for(const value of [Object.assign(Error('failure'),{json:failure,status:500,path:error.path}),
    Object.assign(Error('failure'),{json:{...failure,saved:false},status:502,path:error.path}),
    Object.assign(Error('failure'),{json:failure,status:502,path:'/api/timesheets/other/authorise'})])assert.equal(context.officeDailySavedReadiness(value),null);
  for(const name of ['standard confirmed-save catch','Bulk Process confirmed-save catch','Bulk Authorise confirmed-save catch']){
    const edit=officeReadinessEdits().find(value=>value.name===name);assert.match(edit.after,/return savedDailyReadiness/);
    assert.doesNotMatch(edit.after,/apiPostJson|saveAndRecalculate|dirty = false|expensesDirty = false/);
  }
});
