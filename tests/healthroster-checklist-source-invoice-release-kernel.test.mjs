import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
import {functionEvents,sqlTokens} from '../docs/healthroster-build/proposals/checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const root=new URL('../docs/healthroster-build/proposals/checklist-native-invoice-decision-20261014/',import.meta.url);
const read=p=>fs.readFileSync(new URL(p,root),'utf8'),sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const pins=JSON.parse(read('NATIVE_RELEASE_REVERSE.json')),forward=read('NATIVE_RELEASE_QUALIFICATION.forward.sqlinc'),reverse=read('NATIVE_RELEASE_QUALIFICATION.reverse.sqlinc');
const before=fs.readFileSync(new URL('../checklist-native-invoice-20261014/input/public_weekly_source_invoice_hold_release_atomic_v1.sqlinc',root),'utf8');
const defs=new Map(functionEvents(forward).events.filter(e=>e.definition).map(e=>[e.name,e]));
test('complete actual native release wrapper reverses exactly and only shares qualification and placement guards',()=>{
 assert.equal(sha(before),pins.before_sha256);assert.equal(sha(forward),pins.forward_sha256);assert.equal(sha(reverse),pins.reverse_sha256);
 let wrapper=defs.get('public.weekly_source_invoice_hold_release_atomic_v1').definition;assert.equal(sha(wrapper),pins.wrapper_sha256);
 for(const h of [...pins.hunks].reverse()){assert.equal(wrapper.split(h.after).length,2,h.name);wrapper=wrapper.replace(h.after,()=>h.before);}assert.equal(wrapper,before);
 assert.ok(reverse.startsWith(before));assert.equal(scanManagedSql(forward).statements.length,7);assert.equal(scanManagedSql(reverse).statements.length,3);
 const selected=JSON.parse(fs.readFileSync(new URL('../checklist-native-invoice-20261014/NATIVE_INVOICE_OWNER_PINS.json',root))).selected.find(x=>x.name==='public.weekly_source_invoice_hold_release_atomic_v1');
 const current=execFileSync('git',['cat-file','blob',pins.source_commit+':'+selected.file],{maxBuffer:3e6}).toString().replaceAll('\r\n','\n');
 assert.equal(functionEvents(current).events.find(e=>e.name===selected.name).definition,before);
});
test('one complete native qualification prefix retains all actor/scope/current row/rule checks and the exact three lock sites',()=>{
 const core=defs.get('private.weekly_source_invoice_hold_release_context_core_v1').body;
 const start=core.indexOf(pins.lock_sites[0].after);assert.ok(start>0);
 let recovered=core;
 for(const lock of [...pins.lock_sites].reverse()){assert.equal(recovered.split(lock.after).length,2);recovered=recovered.replace(lock.after,()=>lock.before);}
 const prefix=pins.hunks.find(x=>x.name==='single complete native qualification prefix').before;
 assert.ok(recovered.includes(prefix));assert.equal(sha(prefix),pins.prefix_sha256);
 assert.equal(pins.lock_sites.length,3);assert.equal((core.match(/for update;/g)||[]).length,3);
 for(const owner of ['_weekly_source_settings_require_admin_v1','_weekly_source_settings_deployment_v1',
  'weekly_source_office_authority_v1','weekly_source_effective_rule_set_v1','weekly_source_self_bill_report_duplicate_prior_v1'])assert.ok(core.includes('private.'+owner));
 assert.match(core,/s\.normalised_row_hash is distinct from expected_hash/);assert.match(core,/coalesce\(scope_row\.version,c\.version\) is distinct from expected_version/);
 assert.match(core,/v_client_id is null/);
 const tokens=sqlTokens(core).filter(t=>t.kind==='word').map(t=>t.value);assert.deepEqual(tokens.filter(w=>['insert','delete','truncate','create','lock','call','execute','notify'].includes(w)),[]);
});
test('native true wrapper keeps original money, replay and first-invoice guard ordering; read mode is owner-private only',()=>{
 const wrapper=defs.get('public.weekly_source_invoice_hold_release_atomic_v1').body;
 assert.match(wrapper,/release_context_core_v1\(p_request,true\)/);assert.doesNotMatch(wrapper,/p_native_locks|,false\)/);
 const replay=wrapper.indexOf('if found then'),assertion=wrapper.indexOf('hold_first_invoice_assert_v1(s.id)'),hash=wrapper.indexOf("h:=private.weekly_source_sha256_jsonb_v1");
 assert.ok(wrapper.indexOf('net:=s.source_shift_charge_pence;')<replay);assert.ok(replay<assertion&&assertion<hash);
 assert.ok(wrapper.includes("return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_V1','status','RELEASED','idempotent',true"));
 assert.match(forward,/revoke all on function private\.weekly_source_invoice_hold_release_context_core_v1\(jsonb,boolean\) from public,anon,authenticated,service_role/);
 assert.match(forward,/revoke all on function private\.weekly_source_invoice_hold_first_invoice_assert_v1\(uuid\) from public,anon,authenticated,service_role/);
 assert.doesNotMatch(forward,/grant execute|alter function public/);
});
test('same first-invoice predicate is once-only pure and complete source graph exposes every candidate callee and native gate',()=>{
 const assertion=defs.get('private.weekly_source_invoice_hold_first_invoice_assert_v1').body;
 const old=pins.hunks.find(x=>x.name==='same native first-invoice assertion at original post-replay site').before;
 assert.ok(assertion.includes(old.replaceAll('s.id','p_upload_row_id')));
 assert.match(assertion,/i\.status::text='ISSUED'/);assert.match(assertion,/r\.state='PREPARED'/);
 const words=sqlTokens(assertion).filter(t=>t.kind==='word').map(t=>t.value);
 assert.deepEqual(words.filter(w=>['insert','update','delete','execute','call','create','lock','notify'].includes(w)),[]);
 const graph=JSON.parse(read('NATIVE_READ_GRAPH.json'));assert.equal(graph.SQL_executed,false);assert.equal(graph.public_producer_consumes_decision,false);
 for(const r of graph.routines){assert.equal(sha(read(r.saved_file)),r.definition_sha256);for(const id of r.callees)assert.ok(graph.routines.some(x=>x.id===id),id);}
 const flags=graph.routines.filter(x=>x.direct_flags.length||x.row_lock_sites);
 assert.equal(flags.length,1);assert.equal(flags[0].name,'private.weekly_source_invoice_hold_release_context_core_v1');
 assert.deepEqual(flags[0].direct_flags,['update']);assert.equal(flags[0].row_lock_sites,3);
});
test('purpose reader binds actual native blocked member before literal read qualification and never invokes a public action or classifier',()=>{
 const s=read('CHECKLIST_RELEASE_DECISION_READ.sqlinc'),e=functionEvents(s).events.find(x=>x.definition);
 assert.equal(scanManagedSql(s).statements.length,3);
 assert.ok(e.body.indexOf('weekly_source_checklist_source_invoice_block_items_v1(p_scope,p_actor_user_id)')<e.body.indexOf('release_context_core_v1(request,false)'));
 assert.match(e.body,/native_invoice_witness,billing_movement_id/);assert.match(e.body,/normalised_row_hash\) is distinct from 32/);
 assert.equal((e.body.match(/release_context_core_v1\(request,false\)/g)||[]).length,2);
 assert.equal((e.body.match(/hold_first_invoice_assert_v1\(row_id\)/g)||[]).length,2);
 assert.match(e.body,/context is distinct from context_again or blocks is distinct from again/);
 assert.match(e.body,/context->>'client_id' is distinct from p_scope->>'client_id'/);
 assert.match(e.body,/context#>>'\{cycle,id\}' is distinct from p_scope->>'source_cycle_id'/);
 assert.match(e.body,/known',true,'pending_decision',true/);
 assert.doesNotMatch(e.body,/pending_decision',false|public\.weekly_source_invoice_hold_release_atomic|holds_pay|withheld_amount_pence|round\(|abs\(|execute|for update/);
 const words=sqlTokens(e.body).filter(x=>x.kind==='word').map(x=>x.value);
 assert.deepEqual(words.filter(w=>['insert','update','delete','truncate','notify','create','call','execute'].includes(w)),[]);
 assert.match(e.body,/exception when sqlstate '55000' or sqlstate '40001' or sqlstate '22023'/);
 assert.doesNotMatch(e.body,/sqlstate '42501'/);
});
