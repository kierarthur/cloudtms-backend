import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';
import test from 'node:test';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const uid=n=>'10000000-0000-4000-8000-'+String(n).padStart(12,'0'),hash='a'.repeat(64);
function payload(){const q={schema_version:'WEEKLY_SOURCE_PAID_MATCH_QUESTION_V1',state:'READY',reason_code:null,
 upload_row_id:uid(1),upload_id:uid(2),projection_publication_id:uid(3),candidate_id:uid(4),client_id:uid(5),contract_id:uid(6),
 expected_authority_scope_version:2,expected_row_manifest_hash:hash,expected_paid_overlap_inputs_sha256:'b'.repeat(64),
 choices:[{work_event_id:uid(7),work_date:'2026-10-05',start:'08:00',end:'16:00',break_minutes:30,contract_id:uid(6),same_allowed:true},
 {work_event_id:uid(8),work_date:'2026-10-05',start:'07:00',end:'15:00',break_minutes:20,contract_id:uid(9),same_allowed:false}]};
 return{paid_match_question:q,upload_row_id:q.upload_row_id,contract_id:q.contract_id,candidate:'Candidate test',client:'Client test',shift:'5 Oct 2026',system_hours:'08:00–17:00 · 30 min break',
 recheck_payload:{request_id:uid(10),upload_id:q.upload_id,upload_row_id:q.upload_row_id,projection_publication_id:q.projection_publication_id,
 expected_authority_scope_version:q.expected_authority_scope_version,expected_row_manifest_hash:q.expected_row_manifest_hash}};}
function office({reply,importModule=false}={}){
 const calls=[],timers=[],elements=new Map(),stack=[],host={dataset:{},querySelector:key=>elements.get(key)};
 for(const key of ['target','choice','reason','save','cancel'])elements.set('[data-paid-match-'+key+']',{listeners:{},addEventListener(event,callback){this.listeners[event]=callback;}});
 const authority={issueCommand:async(action,request)=>{calls.push({action,request});return reply?reply(calls.length):{ok:true,status:'CURRENT',publication_id:uid(11),upload_id:uid(2)};},refresh:async()=>{calls.push({refresh:true});},isSourceAdmissionBusy:()=>false};
 let rendered='',closed=0;
 const context=vm.createContext({module:{exports:{}},SESSION:{user:{id:uid(12)}},console,URL,URLSearchParams,TextEncoder,TextDecoder,structuredClone,
 setTimeout:callback=>{timers.push(callback);},clearTimeout:()=>{},CloudTMSWeeklySourceImportWorkspaceV1:authority,__modalStack:stack,
 document:{querySelector:selector=>selector==='[data-paid-match]'?host:null,getElementById:()=>null},
 closeModal:()=>{stack.pop();closed++;},showModal:(_title,_tabs,render,_a,_b,_wire,options)=>{
 const frame={kind:options.kind,setTab:async()=>{rendered=render();}};stack.push(frame);rendered=render();},});
 let source=read(importModule?'forward/frontend/js/weekly-source/import-workspace.js':'forward/frontend/js/weekly-source/workspace-actions.js');
 if(importModule){const needle='    CONTRACT, ENDPOINTS, normaliseWorkspace,';assert.equal(source.split(needle).length,2);
 source=source.replace(needle,'    __sourceTest: {normaliseActions,renderActions,ACTIONS}, CONTRACT, ENDPOINTS, normaliseWorkspace,');}
 vm.runInContext(source,context,{timeout:3000});const api=context.module.exports;
 const flush=()=>{while(timers.length)timers.shift()();};
 return{api,context,calls,authority,stack,flush,rendered:()=>rendered,closed:()=>closed,
 change:(key,value)=>elements.get('[data-paid-match-'+key+']').listeners[key==='reason'?'input':'change']?.({target:{value}}),
 click:async()=>{await elements.get('[data-paid-match-save]').listeners.click?.();flush();}};
}
test('complete current workspace and two Office files reverse byte-for-byte',()=>{
 const proof=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.equal(proof.records.length,3);
 for(const r of proof.records){let s=read('forward/'+r.file);assert.equal(sha(s),r.after_sha256);
 for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,h.before);}assert.equal(s,read('before/'+r.file));assert.equal(sha(s),r.before_sha256);}
});
test('Same exact current DTO carries NONE and native target, no asserted money or actor',()=>{
 const o=office(),p=payload(),model=o.api.normalisePaidMatch(p),request=o.api.buildPaidMatchRequest(model,uid(7),'SAME','Same clinical work');
 assert.equal(request.pay_effect,'NONE');assert.equal(request.work_event_id,uid(7));assert.equal(request.paid_overlap_target_work_event_id,uid(7));assert.equal(request.expected_paid_overlap_inputs_sha256,'b'.repeat(64));
 assert.ok(!('separate_shift'in request));assert.ok(!('actor_user_id'in request));assert.ok(!('paid'in request));assert.ok(!('native_payment_facts'in request));
});
test('Separate remains anchored to actual selected paid shift with NORMAL and no prior event',()=>{
 const o=office(),r=o.api.buildPaidMatchRequest(o.api.normalisePaidMatch(payload()),uid(8),'SEPARATE','Different work');
 assert.equal(r.pay_effect,'NORMAL');assert.equal(r.separate_shift,true);assert.equal(r.paid_overlap_target_work_event_id,uid(8));assert.ok(!('work_event_id'in r));
});
test('unknown/unavailable and extra raw financial facts refuse before confirmation',()=>{
 const o=office();for(const mutate of [p=>p.paid_match_question.state='UNAVAILABLE',p=>p.paid_match_question.native_payment_facts={},p=>p.recheck_payload.actor_user_id=uid(99),p=>p.paid_match_question.expected_paid_overlap_inputs_sha256=true]){
 const p=payload();mutate(p);assert.throws(()=>o.api.normalisePaidMatch(p));}assert.equal(o.calls.length,0);
});
test('publication/upload/header/row/Contract CAS mismatches refuse',()=>{
 const o=office();for(const mutate of [p=>p.recheck_payload.upload_id=uid(90),p=>p.recheck_payload.projection_publication_id=uid(91),p=>p.recheck_payload.expected_authority_scope_version=3,p=>p.upload_row_id=uid(92),p=>p.contract_id=uid(93)]){
 const p=payload();mutate(p);assert.throws(()=>o.api.normalisePaidMatch(p));}
});
test('duplicate clinical targets, forged same eligibility and invalid tuples refuse',()=>{
 const o=office();for(const mutate of [p=>p.paid_match_question.choices.push({...p.paid_match_question.choices[0]}),p=>p.paid_match_question.choices[1].same_allowed=true,p=>p.paid_match_question.choices[0].start='25:00',p=>p.paid_match_question.choices[0].break_minutes='30']){
 const p=payload();mutate(p);assert.throws(()=>o.api.normalisePaidMatch(p));}
});
test('different Contract cannot be selected as Same and reason is mandatory',()=>{
 const o=office(),m=o.api.normalisePaidMatch(payload());assert.throws(()=>o.api.buildPaidMatchRequest(m,uid(8),'SAME','same'));
 assert.throws(()=>o.api.buildPaidMatchRequest(m,uid(7),'SAME','  '));assert.throws(()=>o.api.buildPaidMatchRequest(m,uid(99),'SEPARATE','other'));
});
test('a genuine nullable historical break remains unknown in the clinical display',()=>{
 const o=office(),p=payload();p.paid_match_question.choices[0].break_minutes=null;
 assert.equal(o.api.normalisePaidMatch(p).question.choices[0].break_minutes,null);
 o.api.handleAction({label:'Confirm paid shift match',payload:p});o.flush();assert.match(o.rendered(),/Break not recorded/);
});
test('actual paid modal makes no request on open or incomplete choice',async()=>{
 const o=office();o.api.handleAction({label:'Confirm paid shift match',payload:payload()});o.flush();assert.equal(o.calls.length,0);await o.click();assert.equal(o.calls.length,0);
 assert.match(o.rendered(),/Choose the paid shift/);
});
test('actual modal confirms current native publication and refreshes once',async()=>{
 const o=office();o.api.handleAction({label:'Confirm paid shift match',payload:payload()});o.flush();
 o.change('target',uid(7));o.flush();o.change('choice','SAME');o.change('reason','Same clinical work');await o.click();
 assert.equal(o.calls[0].action,'RECHECK_SOURCE');assert.equal(o.calls[0].request.pay_effect,'NONE');assert.equal(o.closed(),1);assert.equal(o.calls.filter(c=>c.refresh).length,1);
});
test('saved incomplete comparison retries the same request despite later input attempts',async()=>{
 const o=office({reply:()=>{const e=new Error('incomplete');e.code='WEEKLY_SOURCE_RECHECK_INCOMPLETE';throw e;}});
 o.api.handleAction({label:'Confirm paid shift match',payload:payload()});o.flush();o.change('target',uid(7));o.flush();o.change('choice','SAME');o.change('reason','same');await o.click();
 assert.match(o.rendered(),/comparison was saved/);o.change('target',uid(8));o.change('choice','SEPARATE');o.change('reason','changed');await o.click();
 assert.equal(o.calls.length,2);assert.strictEqual(o.calls[0].request,o.calls[1].request);assert.equal(o.calls[1].request.pay_effect,'NONE');assert.equal(o.closed(),0);
});
test('actor, modal or workspace replacement cannot submit or complete the old comparison',async()=>{
 for(const mutate of [o=>o.context.SESSION.user.id=uid(99),o=>o.stack.push({kind:'other'}),o=>o.context.CloudTMSWeeklySourceImportWorkspaceV1={...o.authority}]){
 const o=office();o.api.handleAction({label:'Confirm paid shift match',payload:payload()});o.flush();o.change('target',uid(7));o.flush();o.change('choice','SAME');o.change('reason','same');mutate(o);await o.click();assert.equal(o.calls.length,0);}
 let o;o=office({reply:()=>{o.context.SESSION.user.id=uid(99);return{ok:true,status:'CURRENT',upload_id:uid(2),publication_id:uid(11)};}});
 o.api.handleAction({label:'Confirm paid shift match',payload:payload()});o.flush();o.change('target',uid(7));o.flush();o.change('choice','SAME');o.change('reason','same');await o.click();assert.equal(o.closed(),0);assert.equal(o.calls.length,1);
});
test('foreign/native incomplete or unchanged publication receipt does not close the modal',async()=>{
 for(const reply of [()=>({ok:true,status:'CURRENT',upload_id:uid(99),publication_id:uid(11)}),()=>({ok:true,status:'BUILDING',upload_id:uid(2),publication_id:uid(11)}),()=>({ok:true,status:'CURRENT',upload_id:uid(2),publication_id:uid(3)})]){
 const o=office({reply});o.api.handleAction({label:'Confirm paid shift match',payload:payload()});o.flush();o.change('target',uid(7));o.flush();o.change('choice','SAME');o.change('reason','same');await o.click();assert.equal(o.closed(),0);assert.match(o.rendered(),/not yet confirmed/);}
});
test('actual import action allowlist retains question and disables stale mutation',()=>{
 const o=office({importModule:true}),t=o.api.__sourceTest;
 const actions=t.normaliseActions([{label:'Confirm paid shift match',enabled:true,payload:payload()}],t.ACTIONS.finalise);assert.equal(actions.length,1);
 assert.match(t.renderActions(actions,false,'row'),/Confirm paid shift match/);assert.match(t.renderActions(actions,true,'row'),/disabled/);
 assert.ok(t.ACTIONS.finalise.has('Confirm shift match'));assert.ok(t.ACTIONS.queries.has('Correct this Timesheet'));
});
test('native question preserves actual current-publication/generation and PPv2 actor gates',()=>{
 const s=read('forward/01_paid_match_question.sqlinc'),pp=read('input/current_pp2.sql');
 assert.match(pp,/create or replace function private\.weekly_source_pay_actor_allowed_v1/);assert.match(pp,/or coalesce\(actor\.payment_authoriser,false\)/);
 assert.match(s,/query_current_publication_v1\(v_cycle.id,v_publication.id\)/);assert.match(s,/generation=coalesce\(v_publication.projection_generation,v_publication.authority_scope_version::integer\)/);
 assert.match(s,/v_upload.purpose<>'ORDINARY'/);assert.match(s,/v_cycle.state<>'OPEN'/);assert.match(s,/'VIEW_PAY_WORK'/);assert.match(s,/'APPROVE_PROTECTED_PAY'/);
 assert.ok(s.indexOf('weekly_source_known_source_key_v1')<s.indexOf('v_inputs:=private.weekly_source_paid_overlap_candidates_v1'));
 assert.ok(s.indexOf('weekly_source_identical_rekey_prior_v1')<s.indexOf('v_inputs:=private.weekly_source_paid_overlap_candidates_v1'));
 assert.doesNotMatch(s,/\b(?:insert into|update public|delete from)\b/i);
});
test('readonly question exposes clinical tuple plus whole native input hash, no financial classifier/cash DTO',()=>{
 const s=read('forward/01_paid_match_question.sqlinc');assert.match(s,/'SETTLED_ORIGINAL'/);assert.match(s,/paid\}' is distinct from 'true'::jsonb/);
 assert.match(s,/jsonb_array_length\(v_choices\)=0/);assert.match(s,/'expected_paid_overlap_inputs_sha256',v_inputs->>'inputs_sha256'/);
 assert.doesNotMatch(s,/'native_payment_facts',|'paid',|'paid_amount',|'root_timesheet_ids',/);
 assert.match(s,/from public,anon,authenticated,service_role/);
});
test('current route recheck forwards exact native command and authentic actor; no new HTTP operation',()=>{
 const s=read('input/current_routes.js'),owner=read('input/current_upload_owner.mjs');
 assert.match(s,/const request = \{ \.\.\.payload, actor_user_id: user.id \}/);assert.match(s,/if \(action === 'RECHECK_SOURCE'\)/);
 assert.match(s,/dependencies.recheckUpload\(\{ request, actor: user, env, ctx \}\)/);
 assert.match(owner,/actor_user_id: requiredUuid\(input.actor\?\.id, 'Office user'\)/);assert.match(owner,/'weekly_source_office_recheck_begin_v1', request/);
 assert.match(read('input/native_upload_publication.sql'),/'upload_id',v_publication.upload_id/);assert.match(read('input/native_upload_publication.sql'),/'publication_id',v_publication.id/);
 const writer=fs.readFileSync(path.join(root,'docs/healthroster-build/proposals/paid-shift-match-writer-none-construction-20261014/forward/01_paid_overlap_choice_admit.sqlinc'),'utf8');
 assert.match(writer,/v_observed is distinct from v_inputs/);assert.match(writer,/v_observed->>'inputs_sha256' is distinct from v_expected/);
});
test('PPv2 whole workspace keeps current payment/protected/NHSP/Report branches and bounded additive ACL',()=>{
 const s=read('forward/02_complete_office_workspace.sqlinc'),b=read('before/02_complete_office_workspace.sqlinc'),acl=read('forward/03_office_workspace_acl.sqlinc');
 for(const token of ['v_pay_view','NHSP_FINAL_BACKING_V1','SELF_BILL_REPORT_V1','PROTECTED_SHIFT_MATCH_REQUIRED','APPROVE_PROTECTED_PAY'])assert.equal(s.split(token).length,b.split(token).length,token);
 assert.match(s,/'Confirm paid shift match'/);assert.match(s,/paid_question.value->>'state'='READY'/);assert.match(s,/'paid_match_question',paid_question.value/);
 assert.match(acl,/grant execute on function public.weekly_source_office_workspace_v1\(jsonb\) to service_role/);assert.doesNotMatch(acl,/to (?:anon|authenticated)/);
});
