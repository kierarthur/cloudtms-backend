import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import cp from 'node:child_process';import {createHash} from 'node:crypto';import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
const base=new URL('../docs/healthroster-build/proposals/a0-source-lineage-selector-successor-20261015/',import.meta.url),read=f=>fs.readFileSync(new URL(f,base),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const key=(kind,parent,client)=>JSON.stringify([kind,parent,client]);
const footprint=(resolutions,events,clients)=>new Set([...resolutions.flatMap(r=>clients.map(c=>key('SOURCE_LINEAGE_RESOLUTION_CLIENT',r,c))),...events.flatMap(e=>clients.map(c=>key('SOURCE_LINEAGE_EVENT_CLIENT',e,c)))]);
const raw=r=>[...(r.client_id!=null&&r.row_resolution_id!=null?[key('SOURCE_LINEAGE_RESOLUTION_CLIENT',r.row_resolution_id,r.client_id)]:[]),...(r.client_id!=null&&r.work_event_id!=null?[key('SOURCE_LINEAGE_EVENT_CLIENT',r.work_event_id,r.client_id)]:[])];
test('native lineage OR has two separate Client-conjoined positive arms',()=>{
 const f=footprint(['resolution1'],['event1'],['client1']);for(const r of [{client_id:'client1',row_resolution_id:'resolution1',work_event_id:'other-event'},{client_id:'client1',row_resolution_id:'other-resolution',work_event_id:'event1'}])assert.ok(raw(r).some(k=>f.has(k)));
});
test('foreign Client sharing a selected resolution or event remains disjoint',()=>{
 const f=footprint(['resolution1'],['event1'],['client1']);assert.ok(!raw({client_id:'foreign-client',row_resolution_id:'resolution1',work_event_id:'event1'}).some(k=>f.has(k)));
 assert.ok(!raw({client_id:'client1',row_resolution_id:'other-resolution',work_event_id:'other-event'}).some(k=>f.has(k)));
});
test('OLD selected lineage remains in the union when NEW leaves both parents and Client',()=>{
 const f=footprint(['resolution1'],['event1'],['client1']),old={client_id:'client1',row_resolution_id:'resolution1',work_event_id:'event1'},next={client_id:'foreign-client',row_resolution_id:'other-resolution',work_event_id:'other-event'};
 assert.ok([...raw(old),...raw(next)].some(k=>f.has(k)));assert.ok(!raw(next).some(k=>f.has(k)));assert.deepEqual(raw({client_id:null,row_resolution_id:'resolution1',work_event_id:'event1'}),[]);
});
test('complete lineage products preserve the final parent without a sample or new count cap',()=>{
 const f=footprint(Array.from({length:10003},(_,i)=>'resolution'+i),['event1'],['client1','client2']);assert.equal(f.size,20008);assert.ok(f.has(key('SOURCE_LINEAGE_RESOLUTION_CLIENT','resolution10002','client2')));
});
test('all six complete owners reverse exactly through eight unique hunks to committed aad0 inputs',()=>{
 const m=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.equal(m.records.length,6);assert.equal(m.records.reduce((n,r)=>n+r.hunks.length,0),8);
 for(const r of m.records){const before=fs.readFileSync(new URL('before/'+r.file,base));assert.deepEqual(before,cp.execFileSync('git',['cat-file','blob',r.source_commit+':'+r.source_path],{maxBuffer:4e6}));let after=read('forward/'+r.file);assert.equal(sha(after),r.after_sha256);for(const h of [...r.hunks].reverse()){assert.equal(after.split(h.after).length,2,h.label);after=after.replace(h.after,()=>h.before);}assert.equal(after,before.toString());}
});
test('literal constructor aggregates complete pairs and raw helper needs no parent lookup or broad object fallback',()=>{
 const c=read('forward/04_native_source_footprint.sqlinc'),r=read('forward/05_raw_selector_helpers.sqlinc');
 assert.match(c,/jsonb_array_elements_text\(identity_key->'values'\) parents\(parent_id\)[\s\S]*cross join jsonb_array_elements_text\(clients\)/);
 assert.equal((c.match(/witness:=private\.weekly_source_agency_source_dependencies_v1/g)||[]).length,1);
 const arm=r.slice(r.indexOf("if p_relation='public.weekly_source_row_timesheet_lineages'"),r.indexOf("if p_relation='public.contracts'"));assert.doesNotMatch(arm,/SOURCE_OBJECT|FAMILY_UUID|select\s|from\s|join\s/i);assert.match(arm,/p_row->>'client_id' is not null/);
});
test('closed original grammar, one arbiter and dependency order accept the two actual lineage domains',()=>{
 for(const file of ['00_source_token_grammar.sqlinc','01_complete_same_arbiter_schema.sqlinc','02_complete_scalar_helpers.sqlinc','03_coverage_and_compatibility_acquire.sqlinc'])for(const k of ['SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT'])assert.ok(read('forward/'+file).includes(k),file+':'+k);
 assert.match(read('forward/00_source_token_grammar.sqlinc'),/expected:=jsonb_build_array\(\(identity->>0\)::uuid,\(identity->>1\)::uuid\)/);
 assert.equal((read('forward/01_complete_same_arbiter_schema.sqlinc').match(/create table /g)||[]).length,1);
});
test('native-source column evidence and complete bytes are sealed; SQL remains unmounted lexical only',()=>{
 const e=JSON.parse(read('LINEAGE_PHYSICAL_COLUMN_EVIDENCE.json'));assert.equal(e.columns.length,3);assert.ok(e.columns.every(x=>x.type==='uuid'));assert.equal(e.hosted_catalogue_observed,false);assert.equal(e.source_path,'supabase/release/current-contract.json');const b=cp.execFileSync('git',['cat-file','blob',e.source_commit+':'+e.source_path],{maxBuffer:32e6});assert.equal(sha(b),e.source_sha256);
 const m=JSON.parse(read('SOURCE_MANIFEST.json'));assert.equal(m.native,false);assert.equal(m.mount_ready,false);for(const r of m.artifacts){const b=fs.readFileSync(new URL(r.file,base));assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);}let n=0;for(const f of fs.readdirSync(new URL('forward/',base))){const p=scanManagedSql(read('forward/'+f));assert.equal(p.meta.length,0);n+=p.statements.length;}assert.equal(n,40);assert.match(read('SOURCE_BOUNDARY.md'),/Nested revision predicates, transitive policy\/claim dependencies/);
});
