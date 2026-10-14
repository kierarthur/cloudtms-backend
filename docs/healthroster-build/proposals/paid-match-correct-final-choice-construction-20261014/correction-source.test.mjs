import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';
import assert from 'node:assert/strict';import test from 'node:test';import{createHash}from'node:crypto';import{fileURLToPath}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=p=>fs.readFileSync(path.join(dir,p),'utf8');
const sha=b=>createHash('sha256').update(b).digest('hex');
function moduleVM(){
 const source=read('forward/broker/src/weekly-source/correct-final-source-orchestrator.mjs');
 const context=vm.createContext({Object,JSON,Set,Array,Number,String,__out:{}});
 vm.runInContext(source.replace(/^export\s+/gm,'')+'\n__out={normaliseOfficePreview,normaliseCorrectFinalPaidQuestions,reviewWeeklyCorrectFinalPaidMatchRecheck};',context);
 return context.__out;
}
const id=n=>'10000000-0000-4000-8000-'+String(n).padStart(12,'0'),hash='a'.repeat(64);
const question=()=>({schema_version:'WEEKLY_SOURCE_PAID_MATCH_QUESTION_V1',state:'READY',reason_code:null,
 upload_row_id:id(1),upload_id:id(2),projection_publication_id:id(3),candidate_id:id(4),client_id:id(5),contract_id:id(6),
 expected_authority_scope_version:4,expected_row_manifest_hash:hash,expected_paid_overlap_inputs_sha256:hash,
 choices:[{work_event_id:id(7),work_date:'2026-09-10',start:'08:00',end:'17:00',break_minutes:null,contract_id:id(6),same_allowed:true}]});
const paidRow=()=>({candidate:'Candidate',client:'Client',shift:'Thu 10 Sep',system_hours:'08:00-17:00',paid_match_question:question()});
const preview=()=>({changes:[{candidate:'Candidate',day_date:'Thu 10 Sep',current_final:'08:00-17:00',replacement:'08:00-18:00',result:'Changed',
 current_final_amount:'£90.00',replacement_amount:'£100.00',amount_changed:true}],blockers:[],confirmation_text:'Replace this final version',preview_hash:hash});
test('seven native reverses are exact whole files and unique, preserving literal SQL regex text',()=>{
 const proof=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.equal(proof.reverses.length,7);
 for(const row of proof.reverses){const b=fs.readFileSync(path.join(dir,'before',row.file)),a=fs.readFileSync(path.join(dir,'forward',row.file));
  assert.equal(b.length,row.before_bytes);assert.equal(sha(b),row.before_sha256);assert.equal(a.length,row.after_bytes);assert.equal(sha(a),row.after_sha256);
  let reverse=a.toString();for(const h of [...row.hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,b.toString());}
});
test('native eight-key money/Boolean change passes current complete decoder; predecessor refuses',()=>{
 const out=moduleVM().normaliseOfficePreview(preview());assert.equal(out.changes[0].current_final_amount,'£90.00');assert.equal(out.changes[0].amount_changed,true);
 const old=read('before/broker/src/weekly-source/correct-final-source-orchestrator.mjs').replace(/^export\s+/gm,'');
 const ctx=vm.createContext({__out:{}});vm.runInContext(old+'\n__out={normaliseOfficePreview};',ctx);
 assert.throws(()=>ctx.__out.normaliseOfficePreview(preview()),e=>e.code==='WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID');
});
test('amount_changed must be actual Boolean and no native amount field is silently lost',()=>{
 for(const malformed of['true',1,null]){const p=preview();p.changes[0].amount_changed=malformed;assert.throws(()=>moduleVM().normaliseOfficePreview(p));}
 const p=preview();delete p.changes[0].current_final_amount;assert.throws(()=>moduleVM().normaliseOfficePreview(p));
  const numeric=preview();numeric.changes[0].replacement_amount=100;assert.throws(()=>moduleVM().normaliseOfficePreview(numeric));
});
test('native question decoder retains NULL break and rejects extra/duplicate or wrong same-contract permission',()=>{
 const decoder=moduleVM().normaliseCorrectFinalPaidQuestions;assert.equal(decoder([paidRow()])[0].paid_match_question.choices[0].break_minutes,null);
 assert.throws(()=>decoder([paidRow(),paidRow()]));const row=paidRow();row.paid_match_question.choices[0].same_allowed=false;assert.throws(()=>decoder([row]));
 const extra=paidRow();extra.paid_match_question.paid=true;assert.throws(()=>decoder([extra]));
});
function nativeReceipts(){
 const request={actor_user_id:id(8),request_id:id(9),upload_id:id(2),correction_session_id:id(10),
  expected_authority_scope_version:4,expected_row_manifest_hash:hash};
 const begun={ok:true,status:'BUILDING',purpose:'FINAL_SOURCE_CORRECTION',upload_id:id(2),publication_id:id(11),
  correction_session_id:id(10),correction_review_identity:{schema_version:'WEEKLY_SOURCE_PAID_MATCH_CORRECTION_RECHECK_CONTEXT_V1',
   correction_session_id:id(10),source_cycle_id:id(12),expected_current_final_revision_id:id(13),expected_final_manifest_hash:hash}};
 const published={ok:true,status:'CORRECTION_READY',upload_id:id(2),publication_id:id(11),authority_scope_version:4,
  ready_session_version:7,comparison_manifest_hash:hash,issue_set_hash:hash};return{request,begun,published};
}
test('actual saved publish is reviewed once through unchanged public native review DTO and same actor',async()=>{
 const {request,begun,published}=nativeReceipts();const calls=[];
 const deps={dataRpc:async(name,{p_request})=>{calls.push({name,p_request});return{ok:true,status:'READY_FOR_CONFIRMATION',version:8,
  correction_session_id:id(10),prior_final_revision_id:id(13),upload_id:id(2),office_preview:preview()};}};
 const result=await moduleVM().reviewWeeklyCorrectFinalPaidMatchRecheck(deps,request,begun,published);
 assert.equal(calls.length,1);assert.equal(calls[0].name,'weekly_source_correct_final_review_atomic_v1');
 assert.equal(calls[0].p_request.actor_user_id,id(8));assert.equal(calls[0].p_request.expected_session_version,7);
 assert.equal(calls[0].p_request.replacement_projection_publication_id,id(11));assert.equal(calls[0].p_request.idempotency_key,id(9)+':review');
 assert.equal(result.review_context.expected_session_version,8);assert.equal(result.review_context.replacement_projection_publication_id,id(11));
 assert.equal(result.apply_context.expected_current_final_revision_id,id(13));
});
test('completed replay uses same review key and native version, never appends another choice/rebuild',async()=>{
 const {request,begun,published}=nativeReceipts();const seen=[];const deps={dataRpc:async(_name,{p_request})=>{
  seen.push(p_request);return{ok:true,status:'READY_FOR_CONFIRMATION',version:8,correction_session_id:id(10),prior_final_revision_id:id(13),upload_id:id(2),office_preview:preview()};}};
 const owner=moduleVM();await owner.reviewWeeklyCorrectFinalPaidMatchRecheck(deps,request,begun,published);
 await owner.reviewWeeklyCorrectFinalPaidMatchRecheck(deps,request,{...begun,...published},published);
 assert.deepEqual(seen[0],seen[1]);
});
test('wrong session/upload/publication or native mode refuses before native review',async()=>{
 for(const change of[{published:{publication_id:id(20)}},{published:{status:'CURRENT'}},{begun:{upload_id:id(20)}},{request:{correction_session_id:id(20)}}]){
  const x=nativeReceipts();for(const key of Object.keys(change))Object.assign(x[key],change[key]);let calls=0;
  await assert.rejects(()=>moduleVM().reviewWeeklyCorrectFinalPaidMatchRecheck({dataRpc:()=>{calls++;throw Error('bad');}},x.request,x.begun,x.published));assert.equal(calls,0);
 }
});
test('native blocked review remains blocked and carries genuine questions without apply authority',async()=>{
 const x=nativeReceipts(),p=preview();p.blockers=[{candidate:'Candidate',day_date:'Thu 10 Sep',problem:'Paid overlap',action:'Compare paid shift'}];p.paid_match_questions=[paidRow()];
 const result=await moduleVM().reviewWeeklyCorrectFinalPaidMatchRecheck({dataRpc:async()=>({ok:true,status:'BLOCKED',version:8,
  correction_session_id:id(10),prior_final_revision_id:id(13),upload_id:id(2),office_preview:p})},x.request,x.begun,x.published);
 assert.equal(result.apply_context,null);assert.equal(result.paid_match_questions.length,1);
});
test('scope proof pins real old CURRENT revision/manifest/heads/actor and contains no mutable version in facts result',()=>{
 const s=read('forward/00_correction_choice_scope.sqlinc');for(const required of['v_session.actor_user_id is distinct from p_actor_user_id',
  'v_cycle.current_final_revision_id is distinct from v_session.expected_current_final_revision_id',
  'v_revision.manifest_hash is distinct from v_session.expected_final_manifest_hash','v_cycle.current_complete_upload_id is distinct from v_prior_upload.id',
  "p_confirm and v_session.state is distinct from 'REVIEWED'","'CORRECT_FINAL_SOURCE'"])assert.ok(s.includes(required),required);
 const result=s.slice(s.indexOf('  v_result:='));assert.ok(!result.includes('v_session.version'));assert.ok(!result.includes('v_session.state'));
});
test('fresh correction delegates native rebuild before ordinary head mutations and retains I1 lock order',()=>{
 const s=read('forward/04_complete_office_recheck.sqlinc');const branch=s.indexOf('  if v_correct then\n    -- Existing native rebuild');
 const end=s.indexOf("  update public.weekly_source_projection_publications set state='STALE'",branch);
 const correction=s.slice(branch,end);assert.ok(correction.includes('public.weekly_source_projection_begin_atomic_v1'));assert.ok(!correction.includes('update public.weekly_source_cycles'));
 assert.ok(s.indexOf('v_paid_discovery:=')<s.indexOf('pg_catalog.pg_advisory_xact_lock'));assert.ok(s.indexOf('pg_catalog.pg_advisory_xact_lock')<s.indexOf('v_correction from public.weekly_final_source_correction_sessions'));
 for(const key of['expected_correction_session_version','expected_projection_publication_id','expected_comparison_manifest_hash','expected_issue_set_hash','rebuild_idempotency_key'])assert.ok(correction.includes("'"+key+"'"));
});
test('actual native projection publish owns READY/review invalidation; no Source replacement of that owner',()=>{
 const native=read('input/13102026_1344_projection_publish.sqlinc');assert.ok(native.includes("state='READY',review_idempotency_key=null,review_request_hash=null"));
 assert.ok(native.includes('ready_session_version=v_ready_session_version'));
 const proof=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.ok(!proof.reverses.some(r=>r.source.includes('projection_publish.sqlinc')));
});
test('Correct-Final native paid/invoice/protected/Draft/DIRECT_AMEND guards are retained as input, not changed by choice admission',()=>{
 const s=read('input/10_complete_source_readers_correction.sqlinc');for(const code of['WEEKLY_SOURCE_CORRECTION_ALREADY_PAID','WEEKLY_SOURCE_CORRECTION_INVOICE_LINE_EXISTS',
  'WEEKLY_SOURCE_CORRECTION_PROTECTED_TARGET_EXISTS','WEEKLY_SOURCE_CORRECTION_ACTIVE_PAY_DRAFT','DIRECT_AMEND_RECALCULATE'])assert.ok(s.includes(code),code);
 const proof=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.ok(!proof.reverses.some(r=>r.file.includes('apply')||r.file.includes('prepare')));
});
test('all native input pins and frozen unpublished Legacy dependency remain exact',()=>{
 const p=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));for(const row of p.native_inputs){const b=fs.readFileSync(path.join(dir,row.saved));assert.equal(sha(b),row.sha256);assert.equal(b.length,row.bytes);}
 const legacy=path.join(dir,'..','paid-match-legacy-native-consumer-construction-20261014');assert.equal(sha(fs.readFileSync(path.join(legacy,'FROZEN_ARTIFACT_MANIFEST.json'))),p.legacy_unpublished_manifest_sha256);
});
test('Office full owner reverse preserves its source; no matching-hash Kept history or amount calculation added',()=>{
 const p=JSON.parse(read('COMPLETE_OFFICE_REVERSE.json')),b=read('before/'+p.file);let a=read('forward/'+p.file);
 assert.equal(sha(a),p.after_sha256);for(const h of [...p.hunks].reverse()){assert.equal(a.split(h.after).length,2,h.name);a=a.replace(h.after,()=>h.before);}assert.equal(a,b);
 const s=read('forward/'+p.file);assert.ok(s.includes('onCorrectionConfirmed(result)'));assert.ok(s.includes('initiatingSession?.user === initiatingUser'));
 assert.ok(s.includes("correction ? 'CORRECTION_READY' : 'CURRENT'"));assert.ok(s.includes('expected_correction_session_version:context.expected_session_version'));
});
function uploadVM(){
 const bindings={REPORT_PROFILE:'ACACIUM_SELF_BILL_REPORT_V1',reviewWeeklyCorrectFinalPaidMatchRecheck:moduleVM().reviewWeeklyCorrectFinalPaidMatchRecheck};
 let source=read('forward/broker/src/weekly-source/upload-publication-owner.mjs');
 source=source.replace(/^import\s+\{([\s\S]*?)\}\s+from\s+['"]([^'"]+)['"];?/gm,(_m,names)=>{
  for(const name of names.split(',').map(s=>s.trim()).filter(Boolean))if(!(name in bindings))bindings[name]=()=>{throw Error('UNUSED_IMPORTED_FINANCIAL_OR_TRANSPORT_CALLED:'+name);};return'';
 }).replace(/^export\s+/gm,'');
 const ctx=vm.createContext({...bindings,Object,JSON,Set,Array,Number,String,TextEncoder,__out:{}});
 vm.runInContext(source+'\n__out={createWeeklySourceUploadPublicationOwner};',ctx);return ctx.__out;
}
test('actual whole upload-owner method performs native BEGIN→context→rows→publish→review in order',async()=>{
 const x=nativeReceipts(),calls=[];
 const deps={dataRpc:async(name,payload)=>{calls.push({name,payload});switch(name){
  case'weekly_source_office_recheck_begin_v1':return x.begun;
  case'weekly_source_upload_context_v1':return{ok:true,agency_id:id(21),source_group_id:id(22),source_cycle_id:id(12),
   upload_id:id(2),correction_session_id:id(10),profile_id:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',rows:[]};
  case'weekly_source_projection_rows_apply_atomic_v1':return{ok:true};
  case'weekly_source_projection_publish_atomic_v1':return x.published;
  case'weekly_source_correct_final_review_atomic_v1':return{ok:true,status:'READY_FOR_CONFIRMATION',version:8,
   correction_session_id:id(10),prior_final_revision_id:id(13),upload_id:id(2),office_preview:preview()};
  default:throw Error('Unexpected transport:'+name);
 }}};
 const out=await uploadVM().createWeeklySourceUploadPublicationOwner(deps).recheckUpload({request:x.request,actor:{id:id(8)}});
 assert.deepEqual(calls.map(c=>c.name),['weekly_source_office_recheck_begin_v1','weekly_source_upload_context_v1',
  'weekly_source_projection_rows_apply_atomic_v1','weekly_source_projection_publish_atomic_v1','weekly_source_correct_final_review_atomic_v1']);
 assert.equal(out.status,'CORRECTION_READY');assert.equal(out.correction_review.review_context.expected_session_version,8);
 assert.equal(calls[0].payload.p_request.actor_user_id,id(8));assert.equal(calls[2].payload.p_actor_user_id,id(8));
});
test('same claimed correction cannot be relabelled as ordinary CURRENT; refusal precedes further calls',async()=>{
 const x=nativeReceipts();let n=0;const owner=uploadVM().createWeeklySourceUploadPublicationOwner({dataRpc:async()=>{n++;return{ok:true,status:'CURRENT',upload_id:id(2),publication_id:id(11)};}});
 await assert.rejects(()=>owner.recheckUpload({request:x.request,actor:{id:id(8)}}),e=>e.code==='WEEKLY_SOURCE_CORRECTION_REBUILD_IDENTITY_MISMATCH');assert.equal(n,1);
});
test('wrong actual sealed context leaves saved choice retryable and cannot reach rows/publish/review',async()=>{
 const x=nativeReceipts(),calls=[];const owner=uploadVM().createWeeklySourceUploadPublicationOwner({dataRpc:async(name)=>{
  calls.push(name);if(name==='weekly_source_office_recheck_begin_v1')return x.begun;
  return{ok:true,agency_id:id(21),source_group_id:id(22),source_cycle_id:id(12),upload_id:id(2),correction_session_id:id(99),rows:[]};}});
 await assert.rejects(()=>owner.recheckUpload({request:x.request,actor:{id:id(8)}}),e=>e.code==='WEEKLY_SOURCE_RECHECK_INCOMPLETE'&&e.details.selection_saved===true);
 assert.equal(calls.length,2);
});
test('ordinary completed recheck retains existing result and never calls correction reviewer',async()=>{
 let calls=0;const owner=uploadVM().createWeeklySourceUploadPublicationOwner({dataRpc:async()=>{calls++;return{ok:true,status:'CURRENT',upload_id:id(2),publication_id:id(11)};}});
 const result=await owner.recheckUpload({request:{request_id:id(9),upload_id:id(2)},actor:{id:id(8)}});assert.equal(calls,1);assert.equal(result.status,'CURRENT');assert.ok(!('correction_review'in result));
});
function officeVM(){const ctx=vm.createContext({module:{exports:{}},SESSION:{user:{id:id(8)}},URL,URLSearchParams,console,setTimeout:()=>{},clearTimeout:()=>{}});
 vm.runInContext(read('forward/frontend/js/weekly-source/workspace-actions.js'),ctx);return ctx.module.exports;}
test('actual whole Office normalisers preserve native amount labels and finite corrected request CAS',()=>{
 const api=officeVM(),p=preview(),q=question();const reviewed=api.normaliseCorrectFinalPreview({ok:true,status:'BLOCKED',...p,paid_match_questions:[paidRow()],review_context:{correction_session_id:id(10)}});
 assert.equal(reviewed.changes[0].replacement_amount,'£100.00');assert.equal(reviewed.paid_match_questions.length,1);
 const payload={...paidRow(),upload_row_id:q.upload_row_id,contract_id:q.contract_id,recheck_payload:{request_id:id(9),
  upload_id:q.upload_id,upload_row_id:q.upload_row_id,projection_publication_id:q.projection_publication_id,
  expected_authority_scope_version:q.expected_authority_scope_version,expected_row_manifest_hash:q.expected_row_manifest_hash,
  correction_session_id:id(10),expected_correction_session_version:6,expected_comparison_manifest_hash:hash,expected_issue_set_hash:hash}};
 const model=api.normalisePaidMatch(payload),request=api.buildPaidMatchRequest(model,id(7),'SAME','Same work');
 assert.equal(request.pay_effect,'NONE');assert.equal(request.correction_session_id,id(10));assert.equal(request.expected_correction_session_version,6);
 delete payload.recheck_payload.expected_issue_set_hash;assert.throws(()=>api.normalisePaidMatch(payload));
});
