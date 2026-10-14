import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),read=f=>fs.readFileSync(path.join(dir,f),'utf8');
const sha=b=>createHash('sha256').update(b).digest('hex'),p=JSON.parse(read('COMPLETE_REVERSE_AND_INPUT_PINS.json'));
const before=read('before/01_complete_choice_current.sqlinc'),after=read('forward/01_complete_choice_current.sqlinc');
test('one complete existing current owner uniquely reverses both hunks to actual published Legacy30 bytes',()=>{
 assert.equal(p.hunks.length,2);assert.equal(sha(before),p.before_sha256);assert.equal(sha(after),p.after_sha256);
 let r=after;for(const h of [...p.hunks].reverse()){assert.equal(r.split(h.after).length,2);r=r.replace(h.after,()=>h.before);}assert.equal(r,before);
 assert.equal(before,execFileSync('git',['show',p.commit+':'+p.source],{cwd:root,encoding:'utf8',maxBuffer:4e6}));
});
test('all actual producer and consumer inputs match immutable Git and saved byte hashes',()=>{
 for(const d of p.dependencies){const b=fs.readFileSync(path.join(dir,d.saved));assert.equal(b.length,d.bytes);assert.equal(sha(b),d.sha256);
 assert.deepEqual(b,execFileSync('git',['show',d.commit+':'+d.file],{cwd:root,maxBuffer:5e6}));}
});
test('actual Legacy raw input producer embeds the genuine reader and rederived full domain hash',()=>{
 const s=read('input/02_legacy_signed_source_membership.sqlinc');
 assert.ok(s.includes("'candidate_id',event.candidate_id,'client_id',event.client_id,'actor_user_id',p_actor_user_id"));
 assert.ok(s.includes("'WEEKLY_SOURCE_LEGACY_SIGNED_SOURCE_MEMBERSHIP_INPUT_V1',v_result"));
 assert.ok(s.includes("native_census-'evaluated_at_utc'"));assert.match(s,/weekly_source_office_authority_v1\(p_actor_user_id,'VIEW_PAY_WORK'/);
});
test('only existing two same signatures and owner-private ACL remain with no new persistent write',()=>{
 const e=functionEvents(after).events.filter(x=>x.kind==='CREATE');assert.deepEqual(e.map(x=>x.id),p.existing_signatures);
 assert.equal(e[1].definition,functionEvents(before).events.filter(x=>x.kind==='CREATE')[1].definition);
 assert.equal((after.match(/from public,anon,authenticated,service_role;/g)||[]).length,2);
 assert.doesNotMatch(after,/insert into|update public\.|delete from|create table|grant execute/i);
});
test('recorded finalisation branch original audit and same-target writer suffix remain exact',()=>{
 const b=functionEvents(before).events.find(e=>e.id===p.changed_signature).body,a=functionEvents(after).events.find(e=>e.id===p.changed_signature).body;
 const prefix='  select * into v_choice',end='  v_observed:=private.weekly_source_paid_overlap_candidates_v1';
 assert.equal(a.slice(a.indexOf(prefix),a.indexOf(end)),b.slice(b.indexOf(prefix),b.indexOf(end)));
 const suffix='  select item into v_match';assert.equal(a.slice(a.indexOf(suffix)),b.slice(b.indexOf(suffix)));
 assert.ok(a.includes("'confirmed_by_actor_user_id',v_choice.actor_user_id,'viewer_actor_user_id',p_actor_user_id"));
});
test('current native facts call uses the actual viewer never the immutable confirmer',()=>{
 assert.ok(after.includes('v_observed:=private.weekly_source_paid_overlap_candidates_v1(p_upload_row_id,p_actor_user_id,'));
 assert.doesNotMatch(after,/paid_overlap_candidates_v1\(p_upload_row_id,v_choice\.actor_user_id/);
 assert.ok(after.includes('v_compare_actor:=case when v_compare_side=0 then p_actor_user_id else v_choice.actor_user_id end;'));
});
test('both raw full input hashes and respective Legacy actor hashes are checked before the finite omission',()=>{
 const b=after.slice(after.indexOf('  for v_compare_side in 0..1 loop'),after.indexOf('  select item into v_match'));
 for(const x of ["v_compare_inputs->>'actor_user_id' is distinct from v_compare_actor::text","'WEEKLY_SOURCE_PAID_OVERLAP_INPUTS_V1',v_compare_inputs-'inputs_sha256'","v_compare_basis->>'actor_user_id' is distinct from v_compare_actor::text","'WEEKLY_SOURCE_LEGACY_SIGNED_SOURCE_MEMBERSHIP_INPUT_V1',v_compare_basis-'inputs_sha256'"]){assert.ok(b.includes(x),x);}
 assert.ok(b.indexOf("v_compare_basis->>'inputs_sha256'")<b.indexOf("v_compare_basis:=v_compare_basis-'actor_user_id'-'inputs_sha256'"));
 assert.ok(b.includes("using errcode='55000'"));assert.ok(b.includes("using errcode='40001'"));
});

// Source-selected equality only. This scaffolding hashes canonical JS values
// to test raw transport binding; it does NOT emulate PG JSONB hash bytes,
// native financial classification, authenticated calls or real paid fixtures.
const block=after.slice(after.indexOf('  for v_compare_side in 0..1 loop'),after.indexOf('  select item into v_match'));
const schema=block.match(/if v_compare_basis->>'schema_version'='([^']+)' then/)[1];
const stripKeys=[...block.match(/v_compare_basis:=v_compare_basis([^;]+);/)[1].matchAll(/-'([^']+)'/g)].map(x=>x[1]);
assert.deepEqual(stripKeys,['actor_user_id','inputs_sha256']);
const outerKeys=[...block.match(/v_compare_inputs:=jsonb_set\(v_compare_inputs,'\{overlaps\}',v_compare_overlaps,false\)([^;]+);/)[1].matchAll(/-'([^']+)'/g)].map(x=>x[1]);
const canonical=v=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const hash=(domain,v)=>sha(JSON.stringify([domain,canonical(v)])),clone=v=>structuredClone(v),drop=(v,keys)=>Object.fromEntries(Object.entries(v).filter(([k])=>!keys.includes(k)));
const seal=v=>({...v,inputs_sha256:hash(v.schema_version,drop(v,['inputs_sha256']))});
const legacy=actor=>seal({schema_version:schema,actor_user_id:actor,work_event_id:'event',root_timesheet_id:'root',source_cycle_id:'cycle',source_group_id:'group',candidate_id:'candidate',client_id:'client',member_timesheet_ids:['root'],native_module:{active_owner:'LEGACY',version:1},native_allocation:{state:'AVAILABLE',settlement_count:1},native_terminal_census:{result:'RELEASABLE'},signed_native_memberships:[{native_component:{key_type:'TS_DAY',key_value:'2026-09-30'},signed_member:{movement_id:'movement'},complete_signed_source_members:['event'],native_current_financial_components:[{amount_ex_vat:'native-opaque-value'}]}],paid:null,paid_quantity_known:false,withheld_amount_known:false});
const input=(actor,basis=[legacy(actor)])=>seal({schema_version:'WEEKLY_SOURCE_PAID_OVERLAP_INPUTS_V1',actor_user_id:actor,upload_row_id:'row',overlaps:[{work_event_id:'event',current_pay_snapshot:{state:'CURRENT',snapshot_line_id:'S2',movement_id:'M1'},native_payment_facts:{native_paid_basis:basis,paid:null,paid_evidence_state:'UNKNOWN'}}]});
function comparison(raw,actor){
 const x=clone(raw);if(!x||Array.isArray(x)||x.schema_version!=='WEEKLY_SOURCE_PAID_OVERLAP_INPUTS_V1'||x.actor_user_id!==actor||!Array.isArray(x.overlaps)||x.inputs_sha256!==hash(x.schema_version,drop(x,['inputs_sha256'])))throw new Error('UNBOUND');
 for(const item of x.overlaps){const bs=item.native_payment_facts?.native_paid_basis;if(!Array.isArray(bs))throw new Error('UNBOUND');
 item.native_payment_facts.native_paid_basis=bs.map(b=>{if(b.schema_version!==schema)return b;if(b.actor_user_id!==actor||b.inputs_sha256!==hash(schema,drop(b,['inputs_sha256'])))throw new Error('UNBOUND');return drop(b,stripKeys);});}
 return drop(x,outerKeys);
}
function equal(saved,observed,savedActor='confirmer',viewer='viewer'){return JSON.stringify(canonical(comparison(saved,savedActor)))===JSON.stringify(canonical(comparison(observed,viewer)));}
test('predecessor actual outer-only comparison reproduces different genuine Legacy viewer mismatch',()=>{
 assert.ok(before.includes("(v_observed-'actor_user_id'-'inputs_sha256') is distinct from"));
 assert.notDeepEqual(drop(input('confirmer'),outerKeys),drop(input('viewer'),outerKeys));
});
test('different genuine Legacy viewer retains equal full facts while raw original proof stays immutable',()=>{
 const saved=input('confirmer'),current=input('viewer'),original=clone(saved),observed=clone(current);
 assert.equal(equal(saved,current),true);assert.deepEqual(saved,original);assert.deepEqual(current,observed);
 assert.equal(saved.overlaps[0].native_payment_facts.native_paid_basis[0].actor_user_id,'confirmer');
});
test('NEXT native header and component basis is unchanged and exact across legitimate viewers',()=>{
 const bs=[{root_timesheet_id:'root',native_header:{module_epoch:1,current_origin_state:'APPLIED'},native_component:{component_key:'key',qualification_state:'native-state'}}];
 assert.equal(equal(input('confirmer',bs),input('viewer',clone(bs))),true);
 const beforeBasis=clone(bs);comparison(input('viewer',bs),'viewer');assert.deepEqual(bs,beforeBasis);
});
test('current actor mismatch and immutable confirmer impersonation refuse before comparison',()=>{
 assert.throws(()=>comparison(input('confirmer'),'viewer'),/UNBOUND/);
 const x=input('viewer',[legacy('confirmer')]);assert.throws(()=>comparison(x,'viewer'),/UNBOUND/);
 assert.throws(()=>comparison(input('viewer'),'confirmer'),/UNBOUND/);
});
test('outer raw digest mismatch refuses even when every native basis is unchanged',()=>{
 const x=input('viewer');x.inputs_sha256='wrong';assert.throws(()=>comparison(x,'viewer'),/UNBOUND/);
});
test('actor-bound native digest mismatch refuses even with a re-sealed outer input',()=>{
 const x=legacy('viewer');x.inputs_sha256='wrong';assert.throws(()=>comparison(input('viewer',[x]),'viewer'),/UNBOUND/);
});
test('genuine economic root membership module or unknown data changes still fail full equality',()=>{
 for(const change of [b=>b.root_timesheet_id='foreign',b=>b.member_timesheet_ids.push('other'),b=>b.native_module.version++,b=>b.native_allocation.settlement_count=2,b=>b.signed_native_memberships[0].native_current_financial_components[0].amount_ex_vat='changed',b=>b.signed_native_memberships[0].complete_signed_source_members.push('foreign'),b=>b.future_native_fact='changed']){
 const x=legacy('viewer');change(x);assert.equal(equal(input('confirmer'),input('viewer',[seal(x)])),false);}
});
test('unrecognised schema actor and hash fields are not recursively erased',()=>{
 const a={schema_version:'OTHER_NATIVE_INPUT',actor_user_id:'confirmer',inputs_sha256:'A',payload:{actor_user_id:'nested'}},b={...a,actor_user_id:'viewer',inputs_sha256:'B'};
 assert.equal(equal(input('confirmer',[a]),input('viewer',[b])),false);
});
test('nested actor or hash inside genuine native financial data remains part of facts equality',()=>{
 const a=legacy('confirmer');a.signed_native_memberships[0].metadata={actor_user_id:'A',inputs_sha256:'opaque-A'};
 const b=legacy('viewer');b.signed_native_memberships[0].metadata={actor_user_id:'B',inputs_sha256:'opaque-B'};
 assert.equal(equal(input('confirmer',[seal(a)]),input('viewer',[seal(b)])),false);
});
test('native basis ordering and missing membership cannot be converted to equal facts',()=>{
 const a=legacy('confirmer'),b=legacy('viewer');a.member_timesheet_ids.push('second');b.member_timesheet_ids.unshift('second');
 assert.equal(equal(input('confirmer',[seal(a)]),input('viewer',[seal(b)])),false);
 assert.equal(equal(input('confirmer'),input('viewer',[])),false);
});
test('malformed complete current native basis refuses rather than being erased or marked paid',()=>{
 const x=input('viewer');x.overlaps[0].native_payment_facts.native_paid_basis=null;
 assert.throws(()=>comparison(seal(x),'viewer'),/UNBOUND/);assert.equal(comparison(input('viewer'),'viewer').overlaps[0].native_payment_facts.paid,null);
});
