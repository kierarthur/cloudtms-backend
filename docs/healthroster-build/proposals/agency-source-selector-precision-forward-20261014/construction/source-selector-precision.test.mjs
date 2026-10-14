import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import test from 'node:test';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),read=p=>fs.readFileSync(path.join(here,p),'utf8');
const sql=read('85_complete_source_dependency_witness_with_exact_selector_tuples.sqlinc'),sha=s=>crypto.createHash('sha256').update(s).digest('hex');
const reverse=JSON.parse(read('SOURCE_SELECTOR_COMPLETE_REVERSE.json'));
test('UNIT_ONLY complete three-hunk owner reverse exactly restores frozen Source20',()=>{
 let s=sql;assert.equal(sha(s),reverse.forward_sha256);for(const h of [...reverse.hunks].reverse()){
  assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}
 assert.equal(s,read('01_source20_witness_before.sql.txt'));assert.equal(sha(s),reverse.input_sha256);
});
// Explicit effect-free relational model of the literal native CTE predicates.
// These rows are UNIT inputs; no tuple is presented as a hosted native witness.
const unique=a=>[...new Map(a.map(x=>[JSON.stringify(x),x])).values()]
 .sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
function projection(f){return{
 resolution_slots:unique(f.rows.flatMap(r=>f.publications.filter(p=>p.upload_id===r.upload_id)
  .flatMap(p=>f.clients.map(c=>[r.id,p.projection_generation??p.authority_scope_version,c])))),
 revision_client_pairs:unique(f.revisions.flatMap(r=>f.clients.map(c=>[r.id,c]))),
 revision_event_pairs:unique(f.revisions.flatMap(r=>f.events.map(e=>[r.id,e.id]))),
 exact_report_bound_resolution_ids:[...new Set(f.bindings.map(b=>b.bound_row_resolution_id).filter(x=>x!==null))].sort()};}
const f=()=>({rows:[{id:'row1',upload_id:'upload1'},{id:'row2',upload_id:'upload2'}],
 publications:[{upload_id:'upload1',projection_generation:3,authority_scope_version:9},
 {upload_id:'upload2',projection_generation:null,authority_scope_version:7}],clients:['clientA','clientB'],
 revisions:[{id:'rev1'},{id:'rev2'}],events:[{id:'event1'},{id:'event2'}],
 bindings:[{bound_row_resolution_id:'bound-old-generation-resolution'},{bound_row_resolution_id:null}]});
const member=(array,row)=>array.some(x=>JSON.stringify(x)===JSON.stringify(row));
test('UNIT_ONLY empty visible resolution set still retains every actual phantom slot',()=>{
 assert.deepEqual(projection(f()).resolution_slots,[['row1',3,'clientA'],['row1',3,'clientB'],['row2',7,'clientA'],['row2',7,'clientB']]);
});
test('UNIT_ONLY actual generation takes precedence over authority version',()=>{
 const p=projection(f());assert.ok(member(p.resolution_slots,['row1',3,'clientA']));assert.ok(!member(p.resolution_slots,['row1',9,'clientA']));
});
for(const [name,slot] of [['excluded generation',['row1',4,'clientA']],['foreign Client',['row1',3,'clientC']],
 ['other raw row',['row-other',3,'clientA']],['unselected upload row',['row3',7,'clientA']]]){
 test('UNIT_ONLY precise resolution slot excludes '+name,()=>assert.ok(!member(projection(f()).resolution_slots,slot)));
}
test('UNIT_ONLY raw upload-parent identity alone cannot select a resolution slot',()=>{
 const p=projection(f());assert.ok(!member(p.resolution_slots,['upload1']));assert.ok(!member(p.resolution_slots,['row1']));
});
test('UNIT_ONLY publication without Source rows adds no fictional slot',()=>{
 const x=f();x.publications.push({upload_id:'empty',projection_generation:4,authority_scope_version:4});assert.equal(projection(x).resolution_slots.length,4);
});
test('UNIT_ONLY native row IDs remain distinct despite a shared economic event',()=>{
 const x=f();x.rows.push({id:'real-NONE-B-row',upload_id:'upload1'});const p=projection(x);
 assert.ok(member(p.resolution_slots,['real-NONE-B-row',3,'clientA']));assert.ok(member(p.resolution_slots,['row1',3,'clientA']));
});
test('UNIT_ONLY exact Report-bound resolution is retained separately regardless of selected publication generation',()=>{
 const p=projection(f());assert.deepEqual(p.exact_report_bound_resolution_ids,['bound-old-generation-resolution']);
 assert.ok(!p.resolution_slots.some(s=>s[0]==='bound-old-generation-resolution'));
});
test('UNIT_ONLY revision/Client pair preserves every selected conjunction',()=>{
 const p=projection(f());assert.equal(p.revision_client_pairs.length,4);assert.ok(member(p.revision_client_pairs,['rev1','clientA']));
 assert.ok(!member(p.revision_client_pairs,['rev-other','clientA']));assert.ok(!member(p.revision_client_pairs,['rev1','clientC']));
});
test('UNIT_ONLY revision/event pair never becomes revision-OR-event scope',()=>{
 const p=projection(f());assert.equal(p.revision_event_pairs.length,4);assert.ok(member(p.revision_event_pairs,['rev2','event1']));
 assert.ok(!member(p.revision_event_pairs,['rev-other','event1']));assert.ok(!member(p.revision_event_pairs,['rev1','event-other']));
});
test('UNIT_ONLY OLD and NEW resolve to exact predicates separately, preserving selected-to-excluded movement',()=>{
 const p=projection(f()),old=['row1',3,'clientA'],next=['row1',4,'clientC'];
 assert.ok(member(p.resolution_slots,old));assert.ok(!member(p.resolution_slots,next));
 // Guard implementation is root-owned; this proves no unconditional parent
 // membership is supplied by this source projection model.
});
test('UNIT_ONLY complete large projection has no sample/count equality shortcut',()=>{
 const x=f();x.rows=Array.from({length:10001},(_,i)=>({id:'row'+i,upload_id:'upload1'}));const p=projection(x);
 assert.equal(p.resolution_slots.length,20002);assert.ok(member(p.resolution_slots,['row10000',3,'clientB']));
});
test('UNIT_ONLY literal native SQL retains actual publication fallback and native integer generation',()=>{
 assert.match(sql,/coalesce\(publication->>'projection_generation',publication->>'authority_scope_version'\)::integer generation/);
 assert.match(sql,/publication\.upload_id=row_data\.upload_id[\s\S]*cross join unnest\(clients\)/);
 assert.match(sql,/jsonb_build_array\(upload_row_id,generation,client_id\)/);
});
test('UNIT_ONLY literal native SQL uses full revision/Client and revision/event cartesian predicates',()=>{
 assert.match(sql,/facts->'revisions'\) revision cross join unnest\(clients\)/);
 assert.match(sql,/facts->'revisions'\) revision cross join jsonb_array_elements\(facts->'events'\)/);
 assert.match(sql,/facts->'report_bindings'\) binding[\s\S]*bound_row_resolution_id/);
});
test('UNIT_ONLY Source facts read and genuine CAS domain remain single and unchanged',()=>{
 assert.equal((sql.match(/facts:=private\.weekly_source_bulk_source_facts_v1/g)||[]).length,1);
 assert.match(sql,/'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',facts/);
 assert.match(sql,/octet_length\(result::text\)>8388608/);assert.doesNotMatch(sql.replace(/--[^\r\n]*/g,''),/\blimit\s+\d|sample|slice\(/i);
});
test('UNIT_ONLY selector successor introduces no retention/lock/qualification authority or new signature',()=>{
 assert.equal((sql.match(/create or replace function/g)||[]).length,1);
 assert.doesNotMatch(sql,/pg_advisory|fence_acquire|insert into|update public|set_config\(|qualified.*true/);
 assert.match(sql,/revoke all[\s\S]*public,anon,authenticated,service_role/);
});
