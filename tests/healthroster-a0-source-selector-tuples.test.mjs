import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
const base=new URL('../docs/healthroster-build/proposals/a0-source-selector-tuples-forward-20261014/',import.meta.url);
const read=f=>fs.readFileSync(new URL(f,base),'utf8');
const sha=b=>createHash('sha256').update(b).digest('hex');
test('four exact complete owners reverse eight unique hunks to independently reviewed Source core',()=>{
 const p=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.equal(p.records.length,4);
 assert.equal(p.records.reduce((n,r)=>n+r.hunks.length,0),8);
 for(const r of p.records){const before=fs.readFileSync(new URL('before/'+r.file,base));
  assert.deepEqual(before,execFileSync('git',['cat-file','blob',r.source_commit+':'+r.source_path],{maxBuffer:4e6}));
  assert.equal(sha(before),r.before_sha256);let current=read('forward/'+r.file);assert.equal(sha(current),r.after_sha256);
  for(const h of [...r.hunks].reverse()){assert.equal(current.split(h.after).length,2);current=current.replace(h.after,()=>h.before);}assert.equal(current,before.toString());
 }
});
test('new domains distinguish real revision/client and revision/event from actual cycle pairs',()=>{
 const s=read('forward/00_source_token_grammar.sqlinc');
 for(const kind of ['SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT'])assert.ok(s.includes(kind));
 assert.match(s,/expected:=jsonb_build_array\(\(identity->>0\)::uuid,\(identity->>1\)::uuid\)/);
 assert.doesNotMatch(s,/kind.*:=.*SOURCE_CYCLE_CLIENT|FAMILY_UUID|00000000-0000/);
});
test('resolution slot has a strict native integer between genuine raw row and Client UUIDs',()=>{
 const s=read('forward/00_source_token_grammar.sqlinc');
 assert.match(s,/when kind='SOURCE_RESOLUTION_SLOT' then 3 else 2 end/);
 assert.match(s,/jsonb_typeof\(identity->1\) is distinct from 'number'/);
 assert.match(s,/pg_input_is_valid\(identity->>1,'bigint'\) is distinct from true/);
 assert.match(s,/expected:=jsonb_build_array\(\(identity->>0\)::uuid,\(identity->>1\)::bigint,\(identity->>2\)::uuid\)/);
 assert.match(s,/p_token->>'json' is distinct from expected::text/);
});
test('native selector counterexample: generation and Client cannot be collapsed into a raw-row-only identity',()=>{
 // Equality of the actual tuple, not a database or phantom schedule.
 const row='11111111-1111-1111-1111-111111111111',a='22222222-2222-2222-2222-222222222222',b='33333333-3333-3333-3333-333333333333';
 const key=(r,g,c)=>JSON.stringify([r,g,c]);
 assert.notEqual(key(row,5,a),key(row,5,b));assert.notEqual(key(row,5,a),key(row,6,a));
 assert.equal(key(row,5,a),key(row,5,a));
 assert.notEqual(JSON.stringify(['SOURCE_REVISION_CLIENT',row,a]),JSON.stringify(['SOURCE_CYCLE_CLIENT',row,a]));
});
test('schema, scalar validation/dependency order and compatibility wrapper recognise every exact domain',()=>{
 for(const f of ['01_complete_same_arbiter_schema.sqlinc','02_complete_scalar_helpers.sqlinc','03_coverage_and_compatibility_acquire.sqlinc']){
  const s=read('forward/'+f);for(const kind of ['SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT'])assert.ok(s.includes(kind),f+':'+kind);
 }
 const schema=read('forward/01_complete_same_arbiter_schema.sqlinc');assert.equal((schema.match(/create table /g)||[]).length,1);
 assert.match(schema,/coverage_span with &&,intent_span with &&,retained public\.gist_bool_ops with <>/);
 const wrapper=read('forward/03_coverage_and_compatibility_acquire.sqlinc');assert.match(wrapper,/fence_scalar_acquire_core_v1\(p_tokens,p_retain\);return/);
});
test('every tuple-successor source artifact is byte sealed; SQL remains lexical and unmounted',()=>{
 const m=JSON.parse(read('SOURCE_MANIFEST.json'));assert.equal(m.mount_ready,false);assert.equal(m.native,false);
 for(const r of m.artifacts){const b=fs.readFileSync(new URL(r.file,base));assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);}
 for(const f of fs.readdirSync(new URL('forward/',base))){const s=read('forward/'+f);assert.equal(scanManagedSql(s).meta.length,0);}
 assert.match(read('SOURCE_BOUNDARY.md'),/Actual full footprint, OLD\/NEW raw guards/);
});
