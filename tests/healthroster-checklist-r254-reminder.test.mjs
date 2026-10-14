import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';import crypto from 'node:crypto';
import {dispatchWeeklySourceRequest} from '../broker/src/weekly-source/routes.js';
import {checkOnlyReminderAction,checkOnlyReminderRequest,checkOnlyReminderReceipt} from '../broker/src/weekly-source/check-only-candidate-reminder.mjs';
import {d10OfficePins,applyD10ReviewEdits} from '../docs/healthroster-build/proposals/checklist-native-20261014/build-d10-review-packet.mjs';
import {reminderOwnerPacket,reminderCommandOwnerPacket,reminderVisibilityForward,reminderCandidateCopyForward,applyReminderOfficeEdits} from '../docs/healthroster-build/proposals/checklist-native-20261014/build-reminder-owner-packet.mjs';
import {scanManagedSql,exactAtomicBody} from '../scripts/cloudtms-managed-release-sql.mjs';
import {candidateAppBackendInternals} from '../broker/src/candidate-app-backend.js';
const {safeCandidateNotification}=candidateAppBackendInternals;
const directory=new URL('../docs/healthroster-build/proposals/checklist-native-20261014/',import.meta.url),read=p=>fs.readFileSync(new URL(p,directory),'utf8');
const sql=p=>fs.readFileSync(new URL('../supabase/repeatable/healthroster_wp_u3_source_proposals/'+p,import.meta.url),'utf8');
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,actor=id(1),H='a'.repeat(64),scope={source_group_id:id(2),source_cycle_id:id(3),client_id:id(4),week_ending:'2026-10-11'};
const descriptor=()=>({contract:'WEEKLY_SOURCE_CHECK_ONLY_D10_CANDIDATE_REMINDER_ACTION_V1',actor_user_id:actor,permission_view:'ADMIN',scope,
  parent_group_key:'d10g_'+H,parent_proof_hash:H,action_id:H,candidate_context_hash:H});
const body=()=>({action:'CHECK_ONLY_D10_REMIND_CANDIDATE',payload:{descriptor:descriptor(),idempotency_key:id(5)}});
const user=()=>({id:actor,is_active:true,role:'admin',payView:false});
const receipt=()=>({ok:true,contract:'WEEKLY_SOURCE_CHECK_ONLY_CANDIDATE_REMINDER_V1',actor_user_id:actor,scope,action_id:H,idempotency_key:id(5),notification_id:id(6),queued:true,replay:false,next_available_at_utc:'2026-10-14T23:00:00+00:00'});
const copy=v=>structuredClone(v);
test('R254 descriptor scalar hashes/scope bind actual actor and both transport and Office refuse coercible arrays/foreign/open shapes',()=>{
  const ctx=vm.createContext({});vm.runInContext(read('office-d10-candidate-reminder.jsinc'),ctx);
  assert.ok(checkOnlyReminderAction(descriptor(),actor));assert.ok(ctx.normaliseCheckOnlyD10Reminder(descriptor(),actor));
  const mutations=['parent_group_key','parent_proof_hash','action_id','candidate_context_hash'].map(k=>v=>{v[k]=[v[k]];});
  mutations.push(v=>{v.actor_user_id=id(30);},v=>{v.permission_view='PAY_READONLY';},v=>{v.scope=[v.scope];},v=>{v.scope.week_ending='2026-02-30';},
    v=>{v.scope.client_id=[id(4)];},v=>{v.extra='native';},v=>{v.contract='WEEKLY_SOURCE_REQUEST';});
  for(const mutate of mutations){const v=copy(descriptor());mutate(v);assert.throws(()=>checkOnlyReminderAction(v,actor));assert.equal(ctx.normaliseCheckOnlyD10Reminder(v,actor),null);}
  for(const invalid of [{...user(),payView:true},{...user(),is_active:false},{...user(),role:'pay_readonly'}])assert.throws(()=>checkOnlyReminderRequest(body(),invalid));
  for(const k of ['scope','action_id','notification_id','next_available_at_utc']){const v=copy(receipt());v[k]=[v[k]];assert.throws(()=>checkOnlyReminderReceipt(v,checkOnlyReminderRequest(body(),user())));}
});
test('whole actual Weekly Source command route calls one purpose RPC, binds authenticated actor, preserves exact replay and denies Pay/inactive/forged before RPC',async()=>{
  const counts=[];const invoke=async(value,acting=user(),result=receipt())=>{
    const response=await dispatchWeeklySourceRequest(new Request('https://test.invalid/api/weekly-source/v1/commands',{method:'POST',body:JSON.stringify(value)}),{}, {},{
      requireUser:async()=>acting,rpc:async(name,args)=>{counts.push({name,args});return result;}});return {status:response.status,value:await response.json()};};
  assert.equal((await invoke(body())).status,200);assert.deepEqual(counts[0],{name:'weekly_source_check_only_candidate_reminder_atomic_v1',args:{p_request:checkOnlyReminderRequest(body(),user())}});
  assert.equal((await invoke(body(),user(),{...receipt(),replay:true})).value.replay,true);assert.deepEqual(counts[0],counts[1]);
  const before=counts.length;
  for(const invalid of [{...user(),is_active:false},{...user(),role:'staff',roles:['WP_PP']},{...user(),role:'pay_readonly'}])assert.ok((await invoke(body(),invalid)).status>=400);
  for(const value of [{...body(),actor_user_id:actor},{...body(),payload:{...body().payload,actor_user_id:actor}},
    {...body(),payload:{...body().payload,descriptor:{...descriptor(),actor_user_id:id(30)}}},
    {...body(),payload:{...body().payload,idempotency_key:[id(5)]}}])assert.equal((await invoke(value)).status,400);
  assert.equal(counts.length,before);
});
test('whole route treats malformed/unknown result as unconfirmed and maps stale/permission without private native frames or automatic retries',async()=>{
  for(const failure of ['PGRST202','40001','42501','unknown','badreceipt']){
    let calls=0;const response=await dispatchWeeklySourceRequest(new Request('https://test.invalid/api/weekly-source/v1/commands',{method:'POST',body:JSON.stringify(body())}),{},{},{requireUser:async()=>user(),rpc:async()=>{
      calls++;if(failure==='badreceipt')return {...receipt(),paid:true};throw Object.assign(Error('PRIVATE_NATIVE_FRAME'),{json:{code:failure,message:'PRIVATE_NATIVE_FRAME'}});
    }});const result=await response.json();assert.equal(calls,1);assert.ok(response.status>=400);assert.doesNotMatch(JSON.stringify(result),/PRIVATE_NATIVE_FRAME|paid|SENT/);
    assert.equal(response.status,failure==='40001'?409:failure==='42501'?403:502);
  }
});
test('R254 read graph retains native catalogue/advisory, delegates actual pending-submission witness and pure common inbox visibility; only explicit writer invokes catalogue/kernel',()=>{
  const subject=sql('wp_u3_check_only_candidate_reminder_subject.sqlinc'),context=sql('wp_u3_check_only_candidate_reminder_context.sqlinc'),writer=sql('wp_u3_check_only_candidate_reminder_atomic.sqlinc');
  for(const source of [subject,context]){assert.equal(scanManagedSql(exactAtomicBody(source)).statements.length,3);assert.doesNotMatch(source,/\b(?:insert|update|delete|truncate|pg_advisory_xact_lock|for\s+(?:share|update))\b|_import_review_action_catalog_core_v1\s*\(/i);}
  assert.ok(subject.includes('private.weekly_source_actual_timesheet_submission_v1(ts.timesheet_id)'));assert.ok(subject.includes("witness->'actual_submitted'='true'::jsonb"));
  assert.ok(subject.includes("witness->>'work_date'=hr.date_local::text"));assert.ok(subject.includes("witness->>'contract_id'=decision.contract_id::text"));assert.doesNotMatch(subject,/worked_start_iso|WORKER_DRAFT|worker_submitted_at|planned_|settings_authority/);
  assert.ok(context.includes("'ACTUAL_SUBMISSION_ALREADY_PRESENT'"));assert.ok(context.includes('private.weekly_source_check_only_query_rows_v1('));
  assert.ok(context.includes("l.state='ACTIVE' and a.status='ACTIVE' and a.environment=source_group.environment"));
  assert.ok(context.includes('settings.candidate_manual_reminder_cooldown'));assert.doesNotMatch(context,/interval '6 hours'|paid|withheld|holds_pay|generation_id|publication_id/);
  assert.equal(scanManagedSql(exactAtomicBody(writer)).statements.length,4);
  const admission=writer.indexOf('perform private.weekly_source_pay_query_admit_v2();'),membershipGate=writer.indexOf("'candidate-federated-membership:'"),importLock=writer.indexOf('from public.hr_imports'),linkLock=writer.indexOf('from public.candidate_app_global_membership_links'),accountLock=writer.indexOf('from public.candidate_app_accounts');
  assert.ok(admission<membershipGate&&membershipGate<importLock&&importLock<linkLock&&linkLock<accountLock);
  assert.equal((writer.match(/private\._candidate_notification_insert_v1\(/g)||[]).length,1);assert.equal((writer.match(/public\._import_review_action_catalog_core_v1\(/g)||[]).length,1);
  assert.ok(writer.includes("'ADVISORY'"));assert.ok(writer.includes("null,null,'CHECK_ONLY_TIMESHEET_REMINDER'"));assert.doesNotMatch(writer,/blocking\s*=\s*false|weekly_candidate_message_|weekly_candidate_outreach_|weekly_timesheet_submission_request|weekly_source_candidate_reminder_atomic_v1|pay_amount|withheld|paid=true/);
  assert.ok(writer.indexOf('if notification.id is not null then')<writer.indexOf('facts:=private.weekly_source_check_only_candidate_reminder_context_v1'));
  const packet=reminderOwnerPacket();assert.equal(packet.visibilityForward,sql('wp_u3_candidate_notification_visible.forward.sqlinc'));
  assert.equal(packet.visibilityReverse,sql('wp_u3_candidate_notification_visible.reverse.sqlinc'));
  assert.ok(packet.visibilityForward.includes('private.weekly_source_check_only_candidate_reminder_subject_v1('));assert.doesNotMatch(packet.visibilityForward,/_import_review_action_catalog_core_v1\(|pg_advisory|for update|for share/i);
  assert.ok(packet.visibilityForward.indexOf("p_notification.created_at_utc <= p_now_utc")<packet.visibilityForward.indexOf("p_notification.event_type='CHECK_ONLY_TIMESHEET_REMINDER'"));
  assert.ok(packet.visibilityForward.includes("subject->'native_action'=p_notification.template_params->'native_action'"));
});
test('complete native Candidate copy successor preserves every old byte and actual target mapping without inventing a Source request',()=>{
  const packet=reminderOwnerPacket();assert.equal(packet.copyForward,read('candidate-notification-copy.forward.jsinc'));assert.equal(packet.copyReverse,read('candidate-notification-copy.reverse.jsinc'));
  const evaluate=source=>new Function('upper','text','approvedHoursChangedMessage','CANDIDATE_NOTIFICATION_COPY',source+';return candidateNotificationMessage;')(v=>String(v??'').toUpperCase(),v=>String(v??''),()=> 'Native approved hours copy',{CLAIM_CREATED:'Native claim created'});
  const previous=evaluate(packet.copyReverse),next=evaluate(packet.copyForward);
  for(const event of ['CLAIM_CREATED','WEEKLY_SOURCE_REQUEST','WEEKLY_SOURCE_REMINDER','OFFICE_REJECTED','TIMESHEET_HOURS_UPDATED'])
    for(const parameters of [{},{request_kind:'SUBMIT_TIMESHEET',client_name:'Client',week_ending_label:'11 October'},{request_kind:'CHECK_HOURS'}])assert.equal(next(event,parameters),previous(event,parameters));
  assert.equal(next('CHECK_ONLY_TIMESHEET_REMINDER',{sheet_scope:'DAILY',work_date:'2026-10-10',client_name:'Client'}),'Please submit your Daily Timesheet for 2026-10-10 at Client so your hours can be checked.');
  assert.equal(next('CHECK_ONLY_TIMESHEET_REMINDER',{sheet_scope:'WEEKLY',week_ending:'2026-10-11',client_name:'Client'}),'Please submit your Weekly Timesheet for the week ending 2026-10-11 at Client so your hours can be checked.');
  const notification={id:id(6),event_type:'CHECK_ONLY_TIMESHEET_REMINDER',state:'UNREAD',template_params:{},deep_link_json:{type:'daily'}};
  assert.equal(safeCandidateNotification(notification).deep_link_json.destination,'DAILY');
  assert.equal(safeCandidateNotification({...notification,deep_link_json:{contract_week_id:id(10)}}).deep_link_json.destination,'TIMESHEET_DETAIL');
  assert.equal(safeCandidateNotification({...notification,deep_link_json:{}}).deep_link_json.destination,'HOME');
});
function officeFixture(result=async()=>receipt()){
  const calls={commands:[],refresh:0,toasts:[],timers:[]},parent={_token:'native-parent'};
  const nodes=new Map();class Element{constructor(){this.style={};this.dataset={};this.classList={toggle(){},add(){},remove(){},contains:()=>false};this.innerHTML='';this.listeners={};this.disabled=false;}
    setAttribute(){}removeAttribute(){}getAttribute(){return null;}querySelector(sel){if(!nodes.has(sel))nodes.set(sel,new Element());return nodes.get(sel);}querySelectorAll(){return [];}addEventListener(name,fn){this.listeners[name]=fn;}removeEventListener(){}appendChild(){}
    click(){if(this===nodes.get('btnCloseModal'))root.closeModal();else this.listeners.click?.();}}
  const host=new Element(),document={getElementById:id=>{if(id==='irv1Review')return null;if(!nodes.has(id))nodes.set(id,new Element());return nodes.get(id);},createElement:()=>new Element(),querySelector:sel=>sel==='[data-wsa-screen="command"]'?host:null,querySelectorAll:()=>[],addEventListener(){},removeEventListener(){},body:{children:[]},activeElement:null};
  const root={__modalStack:[parent],document,crypto:{randomUUID:()=>id(5)},addEventListener(){},removeEventListener(){},__LOG_MODAL:false,__toast:text=>calls.toasts.push(text),html:v=>v,
    closeModal:()=>root.__modalStack.pop(),CloudTMSWeeklySourceImportWorkspaceV1:{issueCommand:async(action,payload)=>{calls.commands.push(copy({action,payload}));return result();},refresh:async()=>{calls.refresh++;}}};
  const ctx=vm.createContext({globalThis:root,window:root,document,HTMLElement:Element,byId:id=>document.getElementById(id),qs:()=>null,qsa:()=>[],module:{exports:{}},
    SESSION:{user:{id:actor,role:'admin',is_active:true,roles:['admin']}},setTimeout:fn=>{calls.timers.push(fn);return calls.timers.length;},clearTimeout(){},console:{log(){},warn(){},error(){},groupCollapsed(){},groupEnd(){}}});
  const saved=fs.readFileSync(d10OfficePins[0].path,'utf8').replaceAll('\r\n','\n'),composed=applyReminderOfficeEdits(applyD10ReviewEdits(saved,d10OfficePins[0].path));vm.runInContext(composed,ctx);
  const main=fs.readFileSync(d10OfficePins[1].path.replace('import-review-v1.js','main.js'));assert.equal(crypto.createHash('sha256').update(main).digest('hex'),'d86f2ef841f53243997c7f5bb73b913a4a04a2c563378ab47e58d1a5ee82c847');
  const native=main.toString('utf8').replaceAll('\r\n','\n'),start=native.indexOf('function showModal(title, tabs, renderTab, onSave, hasId, onReturn, options) {');vm.runInContext(native.slice(start,native.indexOf('function applyTimesheetLifecyclePatchToModal(',start)),ctx);root.showModal=ctx.showModal;
  const api=ctx.module.exports;return {ctx,root,host,nodes,calls,parent,open:()=>api.handleAction({kind:'CHECK_ONLY_D10_CANDIDATE_REMINDER',label:'Remind candidate',enabled:true,payload:descriptor(),context:{candidate:'Candidate',client:'Client'}}),
    confirm:()=>host.querySelector('[data-wsa-confirm]').listeners.change({target:{checked:true}}),submit:()=>host.querySelector('[data-wsa-run-command]').listeners.click()};
}
test('complete Office caller and actual native push-before-render host show confirmation, stage once, preserve immutable replay and avoid foreign child/session continuations',async()=>{
  const success=officeFixture();assert.equal(success.open(),true);assert.match(success.nodes.get('modalBody').innerHTML,/Remind this candidate/);success.confirm();await success.submit();
  assert.equal(success.calls.commands.length,1);assert.equal(success.calls.refresh,1);assert.deepEqual(success.calls.toasts,['Reminder added to the Candidate App.']);
  let attempt=0;const retry=officeFixture(async()=>{if(!attempt++)throw Error('unknown');return {...receipt(),replay:true};});assert.equal(retry.open(),true);retry.confirm();await retry.submit();assert.equal(retry.calls.refresh,0);
  retry.confirm();await retry.submit();assert.deepEqual(retry.calls.commands[0],retry.calls.commands[1]);assert.equal(retry.calls.refresh,1);
  for(const mutate of [f=>vm.runInContext('SESSION={user:SESSION.user};',f.ctx),f=>vm.runInContext('SESSION.user={...SESSION.user};',f.ctx),
    f=>vm.runInContext("SESSION.user.role='pay_readonly';",f.ctx),f=>vm.runInContext("SESSION.user.roles.push('other');",f.ctx),
    f=>{f.parent._token='changed-parent';},f=>{f.root.__modalStack.at(-1)._token='changed';},f=>{f.root.__modalStack.push({_token:'foreign',kind:'weekly-source-check-only-reminder-v1'});}]){
    let resolve;const pending=new Promise(done=>{resolve=done;}),fixture=officeFixture(()=>pending);assert.equal(fixture.open(),true);fixture.confirm();const run=fixture.submit();mutate(fixture);resolve(receipt());await run;
    assert.equal(fixture.calls.refresh,0);assert.equal(fixture.calls.toasts.length,0);assert.equal(fixture.calls.commands.length,1);
  }
});
test('complete Office/reminder visibility forwards reverse exactly and keep frozen actor/frame owner and native ACL boundaries',()=>{
  const command=reminderCommandOwnerPacket();assert.equal(command.forward.replace(command.hunk,''),command.reverse);
  assert.equal(command.forward,read('broker-r254-command.forward.jsinc'));assert.equal(command.reverse,read('broker-r254-command.reverse.jsinc'));assert.equal(command.importHunk,read('broker-r254-command.import.jsinc'));
  const current=fs.readFileSync(d10OfficePins[0].path,'utf8').replaceAll('\r\n','\n'),d10=applyD10ReviewEdits(current,d10OfficePins[0].path),forward=applyReminderOfficeEdits(d10);
  assert.equal(applyReminderOfficeEdits(forward,'reverse'),d10);assert.equal(forward,read('office-workspace-actions-reminder.forward.jsinc'));assert.equal(d10,read('office-workspace-actions-reminder.reverse.jsinc'));new vm.Script(forward);
  const packet=reminderOwnerPacket(),branch=packet.visibilityForward.slice(packet.visibilityForward.indexOf("  if p_notification.event_type='CHECK_ONLY_TIMESHEET_REMINDER' then"),packet.visibilityForward.indexOf("  if p_notification.deep_link_json->>'destination'='WEEKLY_SOURCE_REQUEST' then"));
  assert.equal(packet.visibilityForward.replace(branch,''),packet.visibilityReverse);assert.equal(reminderVisibilityForward(packet.visibilityReverse),packet.visibilityForward);assert.equal(reminderCandidateCopyForward(packet.copyReverse),packet.copyForward);
  const pins=JSON.parse(read('R254_NATIVE_OWNER_PINS.json'));assert.equal(pins.rootCensus.uniqueInputs,1745);assert.equal(pins.guardedAmendments.length,0);assert.equal(pins.declarations.length,3);assert.equal(pins.nativeExecuted,false);assert.equal(pins.pushDeliveryQualified,false);
});
