import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import cp from 'node:child_process';import {createHash} from 'node:crypto';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
const base=new URL('../docs/healthroster-build/proposals/a0-source-raw-selector-guard-construction-20261015/',import.meta.url);
const read=f=>fs.readFileSync(new URL(f,base),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const sql=read('construction/01_raw_selector_helpers.sqlinc'),key=(kind,...ids)=>JSON.stringify([kind,...ids]);
// Effect-free model of the specified conjunctions only; no native execution.
const resolution=r=>[key('SOURCE_OBJECT','public.weekly_source_row_resolutions',r.id),...(r.upload_row_id!=null&&r.generation!=null&&r.client_id!=null?[key('SOURCE_RESOLUTION_SLOT',r.upload_row_id,r.generation,r.client_id)]:[])];
const pair=(kind,row,left,right)=>row[left]!=null&&row[right]!=null?[key(kind,row[left],row[right])]:[];
test('OLD selected slot is preserved when NEW moves to excluded generation and Client',()=>{
 const old={id:'resolution1',upload_row_id:'row1',generation:3,client_id:'client1'},next={...old,generation:4,client_id:'client2'};
 const union=[...new Set([...resolution(old),...resolution(next)])];assert.ok(union.includes(key('SOURCE_RESOLUTION_SLOT','row1',3,'client1')));assert.ok(union.includes(key('SOURCE_RESOLUTION_SLOT','row1',4,'client2')));
});
test('foreign generation and Client cannot collide through a broad raw upload-row token',()=>{
 const p=resolution({id:'foreign-resolution',upload_row_id:'row1',generation:4,client_id:'foreign-client'});
 assert.ok(!p.includes(key('SOURCE_RESOLUTION_SLOT','row1',3,'client1')));assert.ok(!p.includes(key('SOURCE_OBJECT','public.weekly_source_upload_rows','row1')));
});
test('exact selected Report-bound resolution still conflicts by its primary identity outside the current slot',()=>{
 const p=resolution({id:'selected-report-bound',upload_row_id:'old-row',generation:1,client_id:'old-client'});
 assert.ok(p.includes(key('SOURCE_OBJECT','public.weekly_source_row_resolutions','selected-report-bound')));
});
test('NULL Client in a real resolution does not become a fictional Client or an unconditional row slot',()=>{
 assert.deepEqual(resolution({id:'resolution',upload_row_id:'row1',generation:3,client_id:null}),[key('SOURCE_OBJECT','public.weekly_source_row_resolutions','resolution')]);
});
test('revision/Client and revision/event preserve both conjuncts for snapshot, PAY and expense transition children',()=>{
 const p=pair('SOURCE_REVISION_CLIENT',{final_revision_id:'rev1',client_id:'client1'},'final_revision_id','client_id');
 assert.ok(!p.includes(key('SOURCE_REVISION_CLIENT','rev1','foreign-client')));assert.ok(!p.includes(key('SOURCE_REVISION_CLIENT','foreign-rev','client1')));
 const e=pair('SOURCE_REVISION_EVENT',{final_revision_id:'rev1',work_event_id:'event1'},'final_revision_id','work_event_id');assert.ok(!e.includes(key('SOURCE_REVISION_EVENT','rev1','foreign-event')));
});
test('whole OLD/NEW extraction occurs before the only same-arbiter acquire, without parent/request reads',()=>{
 const body=sql.slice(sql.indexOf('as $source_reference$'));
 const old=body.indexOf('to_jsonb(old)'),next=body.indexOf('to_jsonb(new)'),acquire=body.indexOf('fence_acquire_v1(tokens,false)');assert.ok(old>0&&next>old&&acquire>next);
 assert.doesNotMatch(sql.replace(/--[^\r\n]*/g,''),/\b(?:from|join)\s+(?:public|private)\.|pg_advisory|execute\s|insert into|update public|delete from/i);
 assert.match(body,/if tg_op='DELETE' then return old;end if;return new/);
});
test('only literal ALL-child predicates create parent object tokens; resolution never broadens to a raw parent',()=>{
 const parentCase=sql.slice(sql.indexOf('case p_relation'),sql.indexOf('end case;'));
 assert.doesNotMatch(parentCase,/'public\.weekly_source_row_resolutions' then|generation|SOURCE_RESOLUTION_SLOT/);
 assert.match(parentCase,/when 'public\.weekly_source_self_bill_report_line_bindings'[\s\S]*if p_row->>'state'='CURRENT'/);
 assert.match(sql,/when 'public\.weekly_discrepancy_incidents'[\s\S]*scope_kind:='SOURCE_CYCLE_CLIENT'/);
 assert.doesNotMatch(sql,/incident.*state.*OPEN|state.*OPEN.*incident/);
});
test('closed raw table context and every actual declared column are source-pinned without compiled catalogue claims',()=>{
 const relations=JSON.parse(read('reference/SOURCE_OBJECT_RELATIONS.json'));assert.equal(relations.length,37);for(const r of relations)assert.ok(sql.includes("'"+r+"'"));
 const e=JSON.parse(read('RAW_COLUMN_SOURCE_EVIDENCE.json'));assert.equal(e.required_relations,38);assert.equal(e.raw_column_evidence.length,75);assert.equal(e.compiled_catalogue,false);assert.equal(e.verified_current_source_pins,22);
 assert.ok(e.raw_column_evidence.some(x=>x.relation==='public.weekly_source_rule_sets'&&x.column==='id'));
 for(const p of JSON.parse(read('INPUT_PINS.json')).pins){const b=fs.readFileSync(new URL(p.file,base));assert.equal(sha(b),p.sha256);assert.deepEqual(b,cp.execFileSync('git',['cat-file','blob',p.source_commit+':'+p.source_path],{maxBuffer:4e6}));}
});
test('raw helper manifest is sealed and owner-only SQL has no trigger installation or native acceptance claim',()=>{
 for(const p of JSON.parse(read('SOURCE_MANIFEST.json')).artifacts){const b=fs.readFileSync(new URL(p.file,base));assert.equal(b.length,p.bytes);assert.equal(sha(b),p.sha256);}
 const p=scanManagedSql(sql);assert.equal(p.meta.length,0);assert.equal(p.statements.length,6);
 assert.equal((sql.match(/revoke all on function/g)||[]).length,2);assert.doesNotMatch(sql,/create trigger|drop trigger/);
 assert.match(read('SOURCE_BOUNDARY.md'),/nested revision and lineage phantom predicates/);assert.equal(JSON.parse(read('SOURCE_MANIFEST.json')).mount_ready,false);
});
