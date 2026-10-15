import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const p=JSON.parse(read('COMPLETE_REVERSE_AND_INPUT_PINS.json')),before=read('before/01_complete_projection_rows.sqlinc'),after=read('forward/01_complete_projection_rows.sqlinc');
test('complete current selected native writer reverses five unique hunks to exact backed writer58',()=>{
 assert.equal(p.hunks.length,5);assert.equal(sha(before),p.before_sha256);assert.equal(sha(after),p.after_sha256);let r=after;
 for(const h of [...p.hunks].reverse()){assert.equal(r.split(h.after).length,2,h.name);r=r.replace(h.after,()=>h.before);}assert.equal(r,before);
 assert.equal(before,execFileSync('git',['show',p.commit+':'+p.source],{cwd:root,encoding:'utf8',maxBuffer:5e6}));
});
test('five full schema current selector staging and actual choice inputs match raw Git',()=>{
 for(const d of p.inputs){const b=fs.readFileSync(path.join(dir,d.saved));assert.equal(b.length,d.bytes);assert.equal(sha(b),d.sha256);assert.deepEqual(b,execFileSync('git',['show',d.commit+':'+d.file],{cwd:root,maxBuffer:6e6}));}
});
test('native event unique hash supports distinct events without a new table field or old update',()=>{
 const schema=read('input/15092026_1534_source_schema.sql'),at=schema.indexOf('create table public.weekly_work_events ('),end=schema.indexOf('create table',at+12),s=schema.slice(at,end);
 assert.ok(at>=0);assert.match(s,/durable_identity_hash bytea not null check \(pg_catalog\.octet_length\(durable_identity_hash\)=32\)/);
 assert.match(s,/unique \(durable_identity_hash\)/);assert.doesNotMatch(after,/update public\.weekly_work_events|alter table|create table/);
 assert.ok(after.includes(') on conflict (durable_identity_hash) do nothing;'));
});
test('existing SCHEDULE_TUPLE and NHSP MOVEMENT magnitude owners remain separate real grains',()=>{
 const selector=read('input/04102026_0475_current_selector.sql');assert.ok(selector.includes("v_source='NHSP'"));
 assert.ok(selector.includes('bpay_next_source_current_magnitudes'));assert.ok(selector.includes("o.origin_kind='MOVEMENT'"));
 assert.ok(after.includes("'NHSP_FINAL_BACKING_V1'"));assert.ok(after.includes("v_identity_kind<>'SCHEDULE_TUPLE'"));
 assert.doesNotMatch(p.hunks.map(h=>h.after).join('\n'),/new_snapshot_line_id|source_present\s*:=|paid\s*:=|current_pay_snapshot/);
});
test('row intent resets and only actual owner-read paid choice can derive it',()=>{
 assert.ok(after.includes("v_paid_choice:=null;v_paid_inputs:=null;v_pay_effect:='NORMAL';v_paid_choice_id:=null;\n    v_nhsp_paid_separate:=false;"));
 const flag=after.indexOf('v_nhsp_paid_separate:=true;'),readChoice=after.indexOf('select item into v_paid_choice from jsonb_array_elements(v_paid_choices) item');assert.ok(readChoice>0&&flag>readChoice);
 assert.ok(after.includes("v_row_json ?| array['pay_effect','paid_overlap_choice_id','paid_overlap_choice','paid_overlap_inputs']"));
 assert.doesNotMatch(p.hunks.map(h=>h.after).join('\n'),/v_row_json->.*separate|p_rows.*separate/);
});
test('real native I-1 then scope/group/cycle and per-day schedule lock graph is preserved',()=>{
 for(const marker of ['v_paid_choices:=private.weekly_source_paid_overlap_projection_lock_v1','v_paid_recheck is distinct from v_paid_choices','perform pg_catalog.pg_advisory_xact_lock('])assert.equal(after.split(marker).length,before.split(marker).length);
 const start='          perform pg_catalog.pg_advisory_xact_lock(',end='          if v_compatible_count>1';
 assert.equal(after.slice(after.indexOf(start),after.indexOf(end)),before.slice(before.indexOf(start),before.indexOf(end)));
 assert.ok(after.indexOf('weekly_source_paid_overlap_projection_lock_v1')<after.indexOf('v_paid_recheck is distinct from v_paid_choices'));
});
test('complete C1 economic price charge lineage and publication suffix remains byte exact',()=>{
 const marker='    -- Only a resolved, source-authoritative worked row owns a C1 economic';
 assert.ok(before.indexOf(marker)>0);assert.equal(after.slice(after.indexOf(marker)),before.slice(before.indexOf(marker)));
 const a=functionEvents(after).events.filter(e=>e.kind==='CREATE'),b=functionEvents(before).events.filter(e=>e.kind==='CREATE');assert.deepEqual(a.map(e=>e.id),b.map(e=>e.id));assert.equal(a.length,1);
 assert.equal(a[0].id,p.signature);assert.doesNotMatch(after,/grant |revoke |alter function/i);
 assert.ok(a[0].header.includes('security definer'));assert.equal(a[0].header,b[0].header);
});
test('finite intent gate refuses negative unknown price prefinal nonordinary and wrong group/scope',()=>{
 for(const x of ["v_profile.profile_code<>'NHSP_FINAL_BACKING_V1'","v_publication.authority_scope_kind<>'NHSP_REPORT_SCOPE'","v_upload.purpose<>'ORDINARY'","v_group.source_family<>'NHSP'","v_source_row.row_finalisation_state<>'SOURCE_WORKED'","v_source_row.source_shift_charge_pence is null or v_source_row.source_shift_charge_pence<0","v_paid_choice_id is null or v_prior_work_event_id is not null"]){assert.ok(after.includes(x));}
 assert.ok(after.includes("'WEEKLY_SOURCE_PAID_OVERLAP_SEPARATE_SCOPE_INVALID' using errcode='55000'"));
});
test('the required no-reversal and report-scope paid basis admission is explicitly unimplemented not fake enabled',()=>{
 assert.equal(p.current_NHSP_paid_choice_admission_implemented,false);assert.ok(p.remaining_construction.includes('complete native no-reversal chain proof'));
 const admit=read('input/current_choice_admit.sqlinc');assert.ok(admit.includes('weekly_source_paid_overlap_candidates_v1'));
 assert.ok(admit.includes("'WEEKLY_SOURCE_PAID_OVERLAP_PROTECTED_OWNER_REQUIRED'"));assert.ok(admit.includes('weekly_source_lock_and_resolve_families_v1'));
});
test('actual same-actor immutable choice and whole projection lock remain prerequisites not caller flags',()=>{
 const c=read('input/current_choice_and_projection_lock.sqlinc');for(const s of ['v_choice.actor_user_id','v_journal.request_hash','v_audit.after_json','weekly_source_paid_overlap_candidates_v1(p_upload_row_id,p_actor_user_id','weekly_source_lock_and_resolve_families_v1'])assert.ok(c.includes(s));
 assert.equal(after.slice(after.indexOf('  v_paid_choices:=private.weekly_source_paid_overlap_projection_lock_v1'),after.indexOf('    v_paid_choice:=null;')),before.slice(before.indexOf('  v_paid_choices:=private.weekly_source_paid_overlap_projection_lock_v1'),before.indexOf('    v_paid_choice:=null;')));
});
test('actual stable separate discriminator is a persisted immutable choice never Ref Num or an economic key',()=>{
 const added=p.hunks.map(h=>h.after).join('\n');assert.ok(added.includes("'office_separate_choice_id',v_paid_choice_id"));assert.ok(added.includes("'office_separate_target_work_event_id',v_paid_choice->>'paid_overlap_target_work_event_id'"));
 assert.doesNotMatch(added,/external_source_key|profile_external_key\s*:=|economic_key|component_id\s*:=|request_id\s*:=/);
 assert.ok(after.includes("'WEEKLY_WORK_EVENT_SCHEDULE_TUPLE_V1'"));
});

// Native branch selection/uniqueness input scaffolding only. These values do
// not constitute native choice admission, JSONB SHA bytes or financial proof.
const admitted={choice_id:7,target:'old-event',identity:'SCHEDULE_TUPLE',effect:'NORMAL',profile:'NHSP_FINAL_BACKING_V1',scope:'NHSP_REPORT_SCOPE',purpose:'ORDINARY',group:'NHSP',row:'SOURCE_WORKED',source_price:0,prior:null};
function finite(x){if(x.identity!=='SCHEDULE_TUPLE'||x.effect!=='NORMAL')return false;return x.profile==='NHSP_FINAL_BACKING_V1'&&x.scope==='NHSP_REPORT_SCOPE'&&x.purpose==='ORDINARY'&&x.group==='NHSP'&&x.row==='SOURCE_WORKED'&&x.source_price!==null&&x.source_price>=0&&x.choice_id!==null&&x.prior===null;}
const schedule={group:'group',profile:'profile',candidate:'candidate',client:'client',date:'2026-09-30',start:'08:00',end:'20:00'};
const unique=(x,tuple=schedule)=>JSON.stringify({...tuple,...(finite(x)?{office_separate_choice_id:x.choice_id,office_separate_target_work_event_id:x.target}:{})});
function branch(count,x){const f=finite(x);if(count>1&&!f)return 'AMBIGUOUS';if(count===1&&!f)return 'REUSE_COMPATIBLE';return 'EXACT_UNIQUE_HASH';}
test('explicit genuine Separate avoids existing paid compatible event while ordinary still reuses it',()=>{
 assert.ok(before.includes("if v_identity_kind='SCHEDULE_TUPLE' and v_compatible_count=1 then"));
 assert.ok(after.includes("if v_identity_kind='SCHEDULE_TUPLE' and v_compatible_count=1 and not v_nhsp_paid_separate then"));
 assert.equal(branch(1,admitted),'EXACT_UNIQUE_HASH');assert.equal(branch(1,{...admitted,effect:'NONE'}),'REUSE_COMPATIBLE');
 assert.notEqual(unique(admitted),JSON.stringify(schedule));
});
test('same actual immutable choice replays one distinct identity even with two compatible events',()=>{
 assert.equal(branch(2,admitted),'EXACT_UNIQUE_HASH');assert.equal(unique(admitted),unique({...admitted}));
 assert.notEqual(unique(admitted),unique({...admitted,choice_id:8}));
 assert.equal(branch(2,{...admitted,effect:'NONE'}),'AMBIGUOUS');
});
test('nonmatching purpose profile scope group sign and missing real choice never admits separate token',()=>{
 for(const x of [{purpose:'FINAL_SOURCE_CORRECTION'},{profile:'NHSP_PREFINAL_RELEASED_V1'},{scope:'CYCLE'},{group:'ROSTER'},{row:'SOURCE_UNFINALISED'},{source_price:-1},{source_price:null},{choice_id:null},{prior:'old-event'}])assert.equal(finite({...admitted,...x}),false);
});
test('ordinary native token values and every original schedule tuple field remain meaningful',()=>{
 assert.equal(unique({...admitted,effect:'NONE'}),JSON.stringify(schedule));
 for(const key of Object.keys(schedule))assert.notEqual(unique(admitted),unique(admitted,{...schedule,[key]:'foreign'}));
});
