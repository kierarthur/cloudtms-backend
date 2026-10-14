import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import cp from 'node:child_process';import {createHash} from 'node:crypto';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
const base=new URL('../docs/healthroster-build/proposals/a0-source-native-footprint-construction-20261015/',import.meta.url);
const read=f=>fs.readFileSync(new URL(f,base),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const sql=read('construction/01_native_source_footprint.sqlinc');
const object=(table,id)=>JSON.stringify(['SOURCE_OBJECT',table,id]);
const tuple=(kind,values)=>JSON.stringify([kind,...values]);
function selectedProjection(w){
 const out=[];for(const relation of w.relations){const pk=relation.relation==='public.timesheets'?'timesheet_id':'id';const key=relation.keys.find(x=>x.column===pk);assert.ok(key);out.push(...key.values.map(id=>object(relation.relation,id)));}
 for(const [kind,field] of [['SOURCE_RESOLUTION_SLOT','resolution_slots'],['SOURCE_REVISION_CLIENT','revision_client_pairs'],['SOURCE_REVISION_EVENT','revision_event_pairs']])out.push(...w.selectors[field].map(values=>tuple(kind,values)));
 return [...new Set(out)].sort();
}
const witness=()=>({relations:[{relation:'public.weekly_source_upload_rows',keys:[{column:'id',values:['rowA','realNoneB']},{column:'upload_id',values:['sharedUpload']}]},{relation:'public.timesheets',keys:[{column:'timesheet_id',values:['realT0']},{column:'contract_id',values:['contract1']}]}],selectors:{resolution_slots:[['rowA',3,'client1'],['realNoneB',3,'client1']],revision_client_pairs:[['rev1','client1']],revision_event_pairs:[['rev1','event1']]}});
test('selected primary row identity cannot be replaced by a shared economic event or arbitrary parent',()=>{
 const p=selectedProjection(witness());assert.ok(p.includes(object('public.weekly_source_upload_rows','rowA')));assert.ok(p.includes(object('public.weekly_source_upload_rows','realNoneB')));
 assert.ok(!p.includes(object('public.weekly_source_upload_rows','sharedUpload')));assert.ok(!p.includes(object('public.timesheets','contract1')));assert.ok(p.includes(object('public.timesheets','realT0')));
});
test('generation, Client and typed conjunction remain distinct even for the same native raw row',()=>{
 const p=selectedProjection(witness());assert.ok(p.includes(tuple('SOURCE_RESOLUTION_SLOT',['rowA',3,'client1'])));
 for(const wrong of [['rowA',4,'client1'],['rowA',3,'client2'],['sharedUpload',3,'client1']])assert.ok(!p.includes(tuple('SOURCE_RESOLUTION_SLOT',wrong)));
 assert.ok(!p.includes(tuple('SOURCE_CYCLE_CLIENT',['rev1','client1'])));assert.ok(!p.includes(tuple('SOURCE_REVISION_EVENT',['rev1','event-other'])));
});
test('complete large selected population preserves the last real row without count or sample equality',()=>{
 const w=witness();w.relations[0].keys[0].values=Array.from({length:10003},(_,i)=>'row'+i);const p=selectedProjection(w);
 assert.ok(p.includes(object('public.weekly_source_upload_rows','row10002')));assert.equal(p.filter(x=>x.startsWith('["SOURCE_OBJECT","public.weekly_source_upload_rows"')).length,10003);
});
test('exact witness and current facts are committed raw inputs; all construction bytes are sealed',()=>{
 for(const p of JSON.parse(read('INPUT_PINS.json')).pins){const b=fs.readFileSync(new URL(p.file,base));assert.equal(sha(b),p.sha256);assert.deepEqual(b,cp.execFileSync('git',['cat-file','blob',p.source_commit+':'+p.source_path],{maxBuffer:4e6}));}
 for(const p of JSON.parse(read('SOURCE_MANIFEST.json')).artifacts){const b=fs.readFileSync(new URL(p.file,base));assert.equal(b.length,p.bytes);assert.equal(sha(b),p.sha256);}
});
test('literal primary mapping covers exactly the actual thirty-two arrays, using physical Timesheet key',()=>{
 const mapping=sql.slice(sql.indexOf('for field in select * from(values'),sql.indexOf(') actual(relation_name,primary_column)'));
 assert.equal([...mapping.matchAll(/\('public\.[a-z_]+','(?:id|timesheet_id)'\)/g)].length,32);
 assert.match(mapping,/\('public\.timesheets','timesheet_id'\)/);assert.doesNotMatch(mapping,/upload_id|client_id|candidate_id|contract_id/);
 assert.match(sql,/where value->>'column'=field\.primary_column/);
});
test('one native reader preserves genuine Source CAS and carries actual family evidence without inventing authority',()=>{
 assert.equal((sql.match(/witness:=private\.weekly_source_agency_source_dependencies_v1/g)||[]).length,1);
 assert.match(sql,/'source_cas',witness->'source_cas'/);assert.match(sql,/'family_bookings',witness->'family_bookings'/);
 const executable=sql.replace(/--[^\r\n]*/g,'');assert.doesNotMatch(executable,/FAMILY_UUID|fence_acquire|pg_advisory|insert into|update public|set_config\(|execute\s|qualified.*true/i);
 assert.match(sql,/revoke all[\s\S]*public,anon,authenticated,service_role/);
});
test('large object/tuple populations aggregate once per native selector rather than copying an array per row',()=>{
 assert.match(sql,/jsonb_agg\(jsonb_build_object\('kind',field\.token_kind,'json',value::text\)\)/);
 assert.doesNotMatch(sql,/for value_text in select jsonb_array_elements_text\(identity_key|for pair in select value from jsonb_array_elements\(selectors->field/);
 assert.doesNotMatch(sql.replace(/--[^\r\n]*/g,''),/\blimit\s+\d|\bsample\b|slice\(/i);
});
test('every tuple is asserted before return; coverage/global and exact report binding use genuine native selectors',()=>{
 assert.match(sql,/for token in select value from jsonb_array_elements\(tokens\)[\s\S]*source_token_assert_v1\(token\)/);
 assert.match(sql,/'SOURCE_GLOBAL_SETTINGS','json','true'/);assert.match(sql,/'SOURCE_CONTRACT_COVERAGE','uuid',identity::text,'coverage',coverage/);
 assert.match(sql,/\('exact_report_bound_resolution_ids','public\.weekly_source_row_resolutions'\)/);
});
test('unmounted SQL is lexical only and remaining raw guards, nested phantoms and ACTIVE recovery are explicit',()=>{
 const parsed=scanManagedSql(sql);assert.equal(parsed.meta.length,0);assert.equal(parsed.statements.length,3);
 assert.equal(JSON.parse(read('SOURCE_MANIFEST.json')).mount_ready,false);assert.match(read('SOURCE_BOUNDARY.md'),/nested revision and lineage phantom predicates/);assert.match(read('SOURCE_BOUNDARY.md'),/ACTIVE between-RPC foreign-writer refusal\/recovery/);
});
