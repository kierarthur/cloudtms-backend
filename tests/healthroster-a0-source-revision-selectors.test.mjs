import test from'node:test';import assert from'node:assert/strict';import fs from'node:fs';import cp from'node:child_process';import{createHash}from'node:crypto';import{scanManagedSql}from'../scripts/cloudtms-managed-release-sql.mjs';
const base=new URL('../docs/healthroster-build/proposals/a0-source-revision-selector-successor-20261015/',import.meta.url),read=f=>fs.readFileSync(new URL(f,base),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');const key=(kind,...ids)=>JSON.stringify([kind,...ids]);
// Native predicate model only: not SQL, MVCC, GiST or parent visibility proof.
const retained=()=>new Set([key('SOURCE_CYCLE_UUID','cycle1'),key('SOURCE_GROUP_REPORT_REVISION','group1','reportScope1'),key('SOURCE_GROUP_REVISION_EVENT','group1','event1')]);
function revision(r){if(!['CURRENT','PREPARED','SUPERSEDED'].includes(r.state))return[];return[key('SOURCE_CYCLE_UUID',r.cycle),...(r.reportScope?[key('SOURCE_GROUP_REPORT_REVISION',r.group,r.reportScope)]:[]),...r.events.map(e=>key('SOURCE_GROUP_REVISION_EVENT',r.group,e))];}
test('actual nested revision current-cycle, Report-scope and event arms each retain their own predicate',()=>{
 const f=retained();for(const r of [{state:'CURRENT',group:'group1',cycle:'cycle1',reportScope:null,events:[]},{state:'PREPARED',group:'group1',cycle:'otherCycle',reportScope:'reportScope1',events:[]},{state:'SUPERSEDED',group:'group1',cycle:'otherCycle',reportScope:null,events:['event1']}])assert.ok(revision(r).some(k=>f.has(k)));
});
test('foreign Source group sharing a selected Report or event cannot become group-OR-child admission',()=>{
 const f=retained();assert.ok(!revision({state:'CURRENT',group:'foreignGroup',cycle:'foreignCycle',reportScope:'reportScope1',events:['event1']}).some(k=>f.has(k)));
 assert.ok(!revision({state:'CURRENT',group:'group1',cycle:'otherCycle',reportScope:'otherScope',events:['otherEvent']}).some(k=>f.has(k)));
});
test('state entry/exit unions preserve selected OLD without inventing an irrelevant draft revision',()=>{
 const current={state:'CURRENT',group:'group1',cycle:'otherCycle',reportScope:null,events:['event1']},draft={...current,state:'DRAFT'};assert.deepEqual(revision(draft),[]);assert.ok([...revision(current),...revision(draft)].some(k=>retained().has(k)));
});
test('all six full owners reverse eleven unique hunks to the actual a525 commit, not only copied before text',()=>{
 const p=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.equal(p.records.length,6);assert.equal(p.records.reduce((n,r)=>n+r.hunks.length,0),11);for(const r of p.records){const before=fs.readFileSync(new URL('before/'+r.file,base));assert.deepEqual(before,cp.execFileSync('git',['cat-file','blob',r.source_commit+':'+r.source_path],{maxBuffer:4e6}));let s=read('forward/'+r.file);assert.equal(sha(s),r.after_sha256);for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length,2,h.label);s=s.replace(h.after,()=>h.before);}assert.equal(s,before.toString());}
});
test('raw OLD/NEW primary/conjunction/current-cycle acquisition precedes the actual parent phase and second acquire',()=>{
 const s=read('forward/05_raw_selector_helpers.sqlinc'),raw=s.slice(0,s.indexOf('-- Raw primary/conjunction acquisition'));
 assert.match(raw,/p_relation='public\.weekly_source_final_revisions'[\s\S]*'SOURCE_CYCLE_UUID','uuid',\(p_row->>'source_cycle_id'\)::uuid::text/);assert.doesNotMatch(raw.replace(/--[^\r\n]*/g,''),/\b(?:from|join)\s+public\./i);
 const trigger=s.slice(s.indexOf('as $source_reference$'));assert.ok(trigger.indexOf('fence_acquire_v1(tokens,false)')<trigger.indexOf('source_revision_tokens_v1('));assert.ok(trigger.indexOf('source_revision_tokens_v1(')<trigger.indexOf('fence_acquire_v1(nested_tokens,false)'));
});
test('parent phase reads exact native Cycle/Revision predicates and unknown snapshot refuses rather than disappearing',()=>{
 const s=read('forward/05_raw_selector_helpers.sqlinc');assert.match(s,/cycle\.source_group_id,revision\.state into strict v_group_id,v_state/);assert.match(s,/snapshot\.final_revision_id=\(p_row->>'id'\)::uuid/);
 assert.match(s,/exception when no_data_found or too_many_rows[\s\S]*CORRECTION_A0_SOURCE_PARENT_SNAPSHOT_UNAVAILABLE.*errcode='40001'/);
 assert.match(s,/if p_relation is null then raise exception/);assert.match(s,/coalesce\(p_row->>'state',''\) not in\('CURRENT','PREPARED','SUPERSEDED'\)/);
 assert.doesNotMatch(s,/create table|set_config\(|execute\s|update public|insert into public|pg_advisory/i);
});
test('footprint aggregates native selected Report/event IDs with genuine Source group, preserving Source CAS and original cycle domain',()=>{
 const s=read('forward/04_native_source_footprint.sqlinc');for(const kind of ['SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT'])assert.ok(s.includes(kind));assert.match(s,/jsonb_build_array\(\(witness->>'source_group_id'\)::uuid,value::uuid\)/);assert.match(s,/'kind','SOURCE_CYCLE_UUID','uuid',p_source_cycle_id::text/);assert.match(s,/'source_cas',witness->'source_cas'/);
 assert.equal((s.match(/witness:=private\.weekly_source_agency_source_dependencies_v1/g)||[]).length,1);
});
test('exact native column metadata and artifact seals are source proof; native visibility and concurrent parent mutation remain held',()=>{
 const e=JSON.parse(read('REVISION_PHYSICAL_COLUMN_EVIDENCE.json'));assert.equal(e.columns.length,8);assert.equal(e.hosted_catalogue_observed,false);assert.equal(sha(cp.execFileSync('git',['cat-file','blob',e.source_commit+':'+e.source_path],{maxBuffer:32e6})),e.source_sha256);
 const m=JSON.parse(read('SOURCE_MANIFEST.json'));assert.equal(m.mount_ready,false);assert.equal(m.native,false);for(const r of m.artifacts){const b=fs.readFileSync(new URL(r.file,base));assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);}let count=0;for(const f of fs.readdirSync(new URL('forward/',base))){const p=scanManagedSql(read('forward/'+f));assert.equal(p.meta.length,0);count+=p.statements.length;}assert.equal(count,43);assert.match(read('SOURCE_BOUNDARY.md'),/source-cycle\/revision immutable-owner and concurrent parent\/child schedules/);
});
