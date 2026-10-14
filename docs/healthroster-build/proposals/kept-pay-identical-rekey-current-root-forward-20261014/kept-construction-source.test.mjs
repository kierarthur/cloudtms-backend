import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import vm from 'node:vm';import {webcrypto,createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';import test from 'node:test';
import {functionEvents,splitSqlStatements} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(out,'../../../..');
const read=file=>fs.readFileSync(path.join(out,file),'utf8'),sha=s=>createHash('sha256').update(s).digest('hex');
const pins=JSON.parse(read('KEPT_CREDIT_CURRENT_OWNERS_AND_INPUT_PINS.json'));
const rekey=JSON.parse(read('REKEY_CURRENT_CALLERS_AND_INPUT_PINS.json'));
test('Every complete Source owner and runtime successor restores its entire exact current owner',()=>{
 for(const proof of [...pins.changes,...rekey.changes]){let after=read(proof.forward_file),before=read(proof.before_file);
  if(proof.after_sha256)assert.equal(sha(after),proof.after_sha256);
  for(const h of proof.hunks.toReversed()){assert.equal(after.split(h.after).length,2);after=after.replace(h.after,h.before);}
  assert.equal(after,before);if(proof.signature){const a=functionEvents(read(proof.forward_file)).events,b=functionEvents(before).events;assert.equal(a.length,1);assert.equal(a[0].id,b[0].id);}}
});
test('Native transition presence and rebill owners remain actual762 input and untouched in this unfinished packet',()=>{
 const fin=rekey.auxiliary_inputs.find(x=>x.file.includes('0821_healthroster_fin4'));assert.equal(fin.sha256,'762454575b7a6844dba087fb4a76e7c46a374a863e5eac77af13645fd4f5af62');
 assert.equal(pins.bank_owned_sources_changed,false);assert.equal(pins.bank_public_page_permissions_unchanged,true);
 assert.ok(!fs.readdirSync(path.join(out,'forward')).some(x=>x.includes('fin4')||x.includes('source_current')));
});
test('Rekey requires actual finalised profile and exact absent old key/unique tuple, never a schedule fallback',()=>{
 const s=read('forward/01_identical_rekey_prior.sqlinc');assert.match(s,/final_authority_kind is distinct from 'HEALTHROSTER_ACTUAL_ROWS'/);
 assert.match(s,/FINALISED_ROW_ONLY/);assert.match(s,/COMPLETE_FILE_ROWS/);assert.match(s,/bounded_raw_columns_json->>'finalised_by' is distinct from v_row.finalised_by/);
 for(const field of ['work_date','start_at_local','end_at_local','break_minutes'])assert.match(s,new RegExp('old_row[.]'+field));
 assert.match(s,/in_file.upload_id=v_upload.id and in_file.external_source_key=e.profile_external_key/);assert.match(s,/if v_count>1 then/);
 assert.match(s,/WEEKLY_SOURCE_IDENTICAL_REKEY_AMBIGUOUS/);assert.match(s,/if v_count=0 then return null/);assert.ok(!s.includes('schedule_compatible'));
});
test('Native rekey proof cannot be bypassed by omitting the prior event, while read discovery exposes exact ambiguity',()=>{
 const projection=read('forward/03_complete_projection_rows.sqlinc'),context=read('forward/02_complete_upload_context.sqlinc');
 const start=projection.indexOf("if v_source_authority and v_identity_kind='PROFILE_EXTERNAL_KEY'"),end=projection.indexOf('if v_prior_work_event_id is not null then',start);
 assert.ok(start>0&&end>start);const guard=projection.slice(start,end);
 assert.ok(!guard.includes("and v_prior_work_event_id is not null"));assert.match(guard,/v_identical_rekey_prior:=private.weekly_source_identical_rekey_prior_v1/);
 assert.match(guard,/v_identical_rekey_prior is distinct from v_prior_work_event_id/);
 assert.match(context,/if SQLERRM is distinct from 'WEEKLY_SOURCE_IDENTICAL_REKEY_AMBIGUOUS' then raise/);assert.match(context,/v_prior_candidate_count:=2/);
});
test('Keep uniqueness is immutable and audit reack does not rewrite/promote a historical decision',()=>{
 const schema=read('forward/04_kept_as_paid_decisions_schema.sqlinc'),rpc=read('forward/07_keep_as_paid_decide.sqlinc');
 assert.match(schema,/coalesce\(credit_invoice_line_id,'00000000-0000-0000-0000-000000000000'::uuid\),facts_fingerprint/);
 assert.match(schema,/force row level security/);assert.match(schema,/before update or delete/);assert.match(schema,/before truncate/);
 assert.ok(!/\b(update|delete from)\s+public[.]weekly_source_kept_as_paid_decisions\b/i.test(rpc));assert.match(rpc,/on conflict do nothing/);
 assert.match(rpc,/select \* into strict v_decision/);assert.match(rpc,/WEEKLY_SOURCE_KEEP_AS_PAID_ACKNOWLEDGED/);
 assert.ok(rpc.indexOf('v_locks:=')<rpc.indexOf("weekly-source-kept-as-paid-event:"));
 assert.ok(rpc.indexOf('v_facts is distinct from v_expected')<rpc.indexOf('insert into public.weekly_source_kept_as_paid_decisions'));
 assert.equal((rpc.match(/return v_audit.after_json->'result'/g)||[]).length,2);
 assert.ok(rpc.indexOf("v_request_hash:=")<rpc.indexOf('v_audit.actor_user_id is distinct from v_actor'));
 assert.ok(rpc.indexOf('weekly_source_pay_actor_allowed_v1(v_actor)')<rpc.indexOf('select audit.* into v_audit'));
});
test('Latest genuine acknowledgement selection precedes current-facts comparison and contains no matching-hash history filter',()=>{
 const s=read('forward/06_kept_as_paid_context.sqlinc'),start=s.indexOf('select audit.* into v_ack'),end=s.indexOf('if found then',start),query=s.slice(start,end);
 assert.match(query,/acknowledgement_sequence/);assert.ok(!query.includes('facts_fingerprint'));
 assert.ok(s.indexOf('v_decision.facts_fingerprint=v_facts')>end);assert.match(s,/v_ack.after_json->>'actor_user_id'=v_ack.actor_user_id::text/);
});
test('Reserved audit acknowledgement INSERT is admitted only through the actual Keep owner and true decision/request/session/facts',()=>{
 const s=read('forward/12_keep_ack_native_guard.sqlinc');assert.match(s,/get diagnostics v_context=PG_CONTEXT/);
 assert.match(s,/weekly_source_keep_as_paid_decide_atomic_v1/);assert.match(s,/tg_op is distinct from 'INSERT'/);
 for(const field of ['native_backend_pid','native_transaction_id','request_sha256','facts_fingerprint','acknowledgement_sequence'])assert.ok(s.includes(field));
 assert.match(s,/private.weekly_source_kept_as_paid_facts_v1\(v_event.id,v_decision.reason\)/);
 assert.match(s,/before insert on public.audit_events/);assert.match(s,/when \(new.action='WEEKLY_SOURCE_KEEP_AS_PAID_ACKNOWLEDGED'\)/);
 assert.match(s,/before update on public.audit_events/);assert.match(s,/before delete on public.audit_events/);
 assert.match(s,/WEEKLY_SOURCE_KEEP_AS_PAID_ACK_IMMUTABLE/);
 assert.ok(!/\b(update|delete from)\s+public[.]audit_events/i.test(s));
});
test('The actual native NEXT string header and all-page/component membership are consumed without inventing known quantities',()=>{
 const s=read('forward/06_kept_as_paid_context.sqlinc');assert.match(s,/v_header->>'current_origin_state' is distinct from 'APPLIED'/);
 assert.ok(!s.includes("'{current_origin_state,application_state}'"));assert.match(s,/v_header is distinct from v_page#>'\{page,header\}'/);
 assert.match(s,/component_member_identity=v_event.id::text/);assert.match(s,/v_item->>'source_component_id'=v_component::text/);
 assert.match(s,/v_page#>'\{page,complete\}'='true'::jsonb/);assert.match(s,/v_complete and v_matched_component_count=1 and v_page_positive/);
 assert.match(s,/v_complete and v_matched_component_count<>1/);assert.match(s,/if cardinality\(v_roots\)=0 then v_paid:=null/);
 assert.match(s,/'paid_quantity_known',false,'withheld_amount_known',false/);assert.match(s,/Source.*pure-kernel seam/s);
});
test('Presentation credit membership and pay-head binding are byte-identical to the planned CAS facts owner',()=>{
 const facts=read('forward/05_kept_as_paid_facts.sqlinc'),context=read('forward/06_kept_as_paid_context.sqlinc');
 const census=s=>s.slice(s.indexOf('    select binding.invoice_line_id line_id'),s.indexOf('  ) actual',s.indexOf('    select binding.invoice_line_id line_id')));
 assert.equal(census(context),census(facts));
 const head=s=>s.slice(s.indexOf('          if v_movement is null or not exists'),s.indexOf('          if not v_movement=any',s.indexOf('          if v_movement is null or not exists')));
 assert.equal(head(context),head(facts));assert.match(census(facts),/report_row.row_finalisation_state='SOURCE_REPORT_LINE'/);
 assert.match(census(facts),/report_row.upload_id=revision.upload_id/);assert.match(census(facts),/prior_report_row.external_source_key=report_row.external_source_key/);
});
test('Current complete Keep route preserves the entire exact published Undo branch and current permission owner',()=>{
 const before=read('before/routes.js'),after=read('forward/routes.js');assert.equal(sha(before),'425e7fefb430e7db09c777ebf852f0e9aaa02ce17e0ca144eaaf2d58dd7c29a5');
 const start=before.indexOf("  if (action === 'UNDO_NO_ONE_WORKED') {"),end=before.indexOf("  if (action === 'KEEP_AS_PAID') {",start);
 assert.ok(start>0);const next=before.indexOf("  if (action ===",start+20);assert.ok(next>start);assert.ok(after.includes(before.slice(start,next)));
 assert.equal(pins.current_selected_commit,'5db7abee7925fffbeaf7c4e885cce809980fa079');
});
test('Credit admission has one complete Candidate root-set phase before group/cycles and the public first invoice lock',()=>{
 const helper=read('forward/08_credited_targets_admit.sqlinc'),caller=read('forward/09_complete_invoice_apply_edits.sqlinc');
 assert.ok(helper.indexOf('for v_candidate in')<helper.indexOf('perform 1 from public.weekly_source_groups'));
 assert.ok(helper.indexOf('v_before is distinct from v_after')<helper.indexOf("set_config('cloudtms.weekly_source_credit_target_frame'"));
 assert.ok(caller.indexOf('weekly_source_invoice_credited_targets_admit_v1')<caller.indexOf('perform public._ctms_assert_invoice_mutable_draft_v1'));
 assert.match(helper,/native_paid_basis_sha256/);assert.match(helper,/current_pay_head_movement_id/);
});
test('Credit writer requires genuine caller/session/full payload frame; validator preserves historical immutable target identity',()=>{
 const writer=read('forward/10_complete_adjustments_apply.sqlinc'),validator=read('forward/11_complete_adjustment_line_valid.sqlinc');
 for(const field of ['PG_CONTEXT','native_backend_pid','native_transaction_id','payload_sha256','frame_sha256'])assert.ok(writer.includes(field));
 for(const owner of ['_ctms_assert_invoice_mutable_draft_v1','invoice_recompute_totals','weekly_source_invoice_allocation_assert_v1','_audit_insert'])assert.ok(writer.includes(owner));
 assert.match(validator,/credited_work_event_id/);assert.match(validator,/invoice.client_id=event.client_id/);assert.match(validator,/source_group_id/);
 assert.ok(!validator.includes('weekly_source_kept_as_paid_context_v1'));assert.match(validator,/weekly_source_invoice_adjustment_authority_v1/);
});
test('New SQL source owners have finite literal signatures and body delimiter/statement lexing only',()=>{
 for(const file of fs.readdirSync(path.join(out,'forward')).filter(x=>x.endsWith('.sqlinc'))){
  const s=read('forward/'+file);const split=splitSqlStatements(s);assert.equal(split.meta.length,0,file);assert.ok(split.statements.length>0);
  for(const e of functionEvents(s).events.filter(x=>x.kind==='CREATE')){assert.ok(e.id.includes('('));assert.match(e.definition,/security definer/i);}}
});
async function loadRoute(){
 const context=vm.createContext({Request,Response,Headers,URL,URLSearchParams,AbortSignal,TextEncoder,TextDecoder,crypto:webcrypto,
  fetch:()=>{throw new Error('NETWORK_FORBIDDEN_IN_SOURCE_TEST');}});
 const source=read('forward/routes.js');const module=new vm.SourceTextModule(source,{context});
 const actorPin=pins.auxiliary.find(x=>x.file==='broker/src/payment-actor-permission.js');
 const actorSource=execFileSync('git',['--no-replace-objects','show',actorPin.commit+':'+actorPin.file],{cwd:root,maxBuffer:1024*1024}).toString('utf8');assert.equal(sha(actorSource),actorPin.sha256);
 const actor=new vm.SourceTextModule(actorSource,{context});await actor.link(()=>{throw new Error('UNEXPECTED_PERMISSION_IMPORT');});await actor.evaluate();
 const imports={
  './index.js':{parseWeeklySourceFile:()=>{throw new Error('UNEXPECTED_PARSE');}},
  './guard-refusal-record.mjs':{isManagedRootGuardRefusal:()=>false,recordGuardRefusalAfterRollback:()=>{throw new Error('UNEXPECTED_REFUSAL_RECORD');}},
  '../bpay-busy.js':{bpayBusyEnvelope:()=>null},'../payment-actor-permission.js':{isActivePaymentActor:actor.namespace.isActivePaymentActor},
  './protected-action-orchestrator.mjs':{WeeklyProtectedActionError:class extends Error{}},
  '../bpay-next-legacy-fence.js':{normalizeBpayNextOwnerStatus:()=>{throw new Error('UNEXPECTED_OWNER_PROBE');}},
  './rule-set-validation.js':{validateRuleSetValues:()=>{throw new Error('UNEXPECTED_RULES');}},
 };
 await module.link(name=>{assert.ok(imports[name],name);const map=imports[name];return new vm.SyntheticModule(Object.keys(map),function(){for(const [key,value]of Object.entries(map))this.setExport(key,value);},{context});});
 await module.evaluate();return module.namespace.dispatchWeeklySourceRequest;
}
const actorId='11111111-1111-4111-8111-111111111111';
const payload={schema_version:'WEEKLY_SOURCE_KEEP_AS_PAID_REQUEST_V1',work_event_id:'22222222-2222-8222-8222-222222222222',decision:'KEEP_AS_PAID',reason:'NOT_INVOICED_DISAPPEARED',credit_invoice_line_id:null,expected_facts_fingerprint:'a'.repeat(64),idempotency_key:'kept-reviewed-request-0001'};
async function request(dispatch,user,value=payload,bodyExtra={}){const calls=[];const req=new Request('https://source.invalid/api/weekly-source/v1/commands',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({action:'KEEP_AS_PAID',payload:value,...bodyExtra})});
 const response=await dispatch(req,{}, {},{requireUser:async()=>user,rpc:async(name,args)=>{calls.push({name,args});return{ok:true,source_mock:true};}});return{status:response.status,json:await response.json(),calls};}
for(const kind of ['admin','payment_authoriser','payment_golden_key'])test('Actual complete Keep route binds authenticated '+kind+' actor and closed request',async()=>{
 const dispatch=await loadRoute();const user={id:actorId,is_active:true,role:kind==='admin'?'admin':'member',[kind]:true};const r=await request(dispatch,user);
 assert.equal(r.status,200);assert.equal(r.calls.length,1);assert.equal(r.calls[0].name,'weekly_source_keep_as_paid_decide_atomic_v1');assert.equal(r.calls[0].args.p_request.actor_user_id,actorId);
 assert.equal(r.calls[0].args.p_request.work_event_id,payload.work_event_id);assert.equal(r.calls[0].args.p_request.credit_invoice_line_id,null);
});
test('Non-payment and inactive actors cannot reach the native Keep transport',async()=>{
 const dispatch=await loadRoute();for(const user of [{id:actorId,is_active:true,role:'member'},{id:actorId,is_active:false,role:'admin'}]){const r=await request(dispatch,user);assert.ok(r.status===401||r.status===403);assert.equal(r.calls.length,0);}
});
test('Actor spoof, facts/credit schema mismatch and unknown fields refuse before any native call',async()=>{
 const dispatch=await loadRoute(),user={id:actorId,is_active:true,role:'admin'};
 for(const value of [{...payload,actor_user_id:actorId},{...payload,expected_facts_fingerprint:'0'},{...payload,reason:'CREDITED'},
  {...payload,credit_invoice_line_id:actorId},{...payload,paid:true},{...payload,idempotency_key:'x'}]){const r=await request(dispatch,user,value);assert.equal(r.status,400);assert.equal(r.calls.length,0);}
 const extra=await request(dispatch,user,payload,{actor_user_id:actorId});assert.equal(extra.status,400);assert.equal(extra.calls.length,0);
});
