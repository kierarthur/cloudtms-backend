import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
const root=new URL('../docs/healthroster-build/proposals/a0-source-selector-domain-construction-20261014/',import.meta.url);
const read=p=>fs.readFileSync(new URL(p,root),'utf8');
const sha=b=>createHash('sha256').update(b).digest('hex');
test('two complete proposed owners reverse exactly to current A0 Git inputs',()=>{
 const p=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.equal(p.records.length,2);
 for(const r of p.records){const raw=fs.readFileSync(new URL('before/'+r.file,root));
  const git=execFileSync('git',['cat-file','blob',r.source_commit+':'+r.source_path],{maxBuffer:4e6});
  assert.deepEqual(raw,git);assert.equal(raw.length,r.before_bytes);assert.equal(sha(raw),r.before_sha256);
  let s=read('forward/'+r.file);assert.equal(sha(s),r.after_sha256);
  for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}
  assert.equal(s,raw.toString());
 }
});
test('source manifest retains every raw construction byte',()=>{
 const m=JSON.parse(read('SOURCE_MANIFEST.json'));assert.equal(m.mount_ready,false);assert.equal(m.native,false);
 for(const r of m.artifacts){const b=fs.readFileSync(new URL(r.file,root));assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);}
});
test('closed new identities are typed actual objects/pairs/singleton and native date coverage',()=>{
 const sql=read('forward/00_source_token_grammar.sqlinc');assert.match(sql,/expected:=jsonb_build_array\(identity->>0,\(identity->>1\)::uuid\)/);
 assert.match(sql,/expected:=jsonb_build_array\(\(identity->>0\)::uuid,\(identity->>1\)::uuid\)/);
 assert.match(sql,/p_token->>'json' is distinct from expected::text/);assert.match(sql,/is distinct from 'true'/);
 assert.match(sql,/span:=daterange/);assert.doesNotMatch(sql,/execute|FAMILY_UUID|00000000-0000/i);
 assert.match(sql,/from public,anon,authenticated,service_role/);
});
test('original eight-domain path delegates once with original ABI and no Source selector',()=>{
 const wrapper=read('forward/03_coverage_and_compatibility_acquire.sqlinc');
 const oldArm=wrapper.slice(wrapper.indexOf('if not exists(select 1 from jsonb_array_elements(p_tokens)'),wrapper.indexOf('if p_retain is null'));
 assert.equal((oldArm.match(/fence_scalar_acquire_core_v1\(p_tokens,p_retain\)/g)||[]).length,1);
 assert.match(oldArm,/return;end if;/);assert.doesNotMatch(oldArm,/coverage_acquire|update|insert/);
 const scalar=read('forward/02_complete_scalar_helpers.sqlinc');
 for(const kind of ['FAMILY_UUID','PHYSICAL_UUID','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID','BOOKING','KEY_NORM','ALIAS_JSON_STRING'])assert.ok(scalar.includes(kind));
 assert.match(scalar,/pg_current_xact_id\(\)::text::numeric/);assert.match(scalar,/numrange\(v_xid,v_xid\+1,'\[\)'\)/);
});
test('coverage schema uses one arbiter, opposite intent classes and exact retained uniqueness',()=>{
 const schema=read('forward/01_complete_same_arbiter_schema.sqlinc');assert.equal((schema.match(/create table /g)||[]).length,1);
 assert.match(schema,/coverage_span with &&,intent_span with &&,retained public\.gist_bool_ops with <>/);
 assert.match(schema,/where\(uuid_identity is not null and coverage_span is null\)/);
 assert.match(schema,/on private\.timesheet_correction_a0_transaction_fences\(token_kind,uuid_identity,coverage_span\)/);
 assert.match(schema,/where\(retained and coverage_span is not null\)/);assert.doesNotMatch(schema,/range_agg|range_merge|delete from/);
});
test('UNIT_ONLY selected exclusion predicates distinguish disjoint dates and do not merge retained scopes',()=>{
 // Counterexample to a single-client unbounded fence or retained range union.
 // This evaluates the stated predicates only; it is not a GiST/RR test.
 const overlap=(a,b)=>a[0]<=b[1]&&b[0]<=a[1];
 const conflict=(a,b)=>a.client===b.client&&overlap(a.days,b.days)&&overlap(a.xid,b.xid)&&a.retained!==b.retained;
 const a={client:'A',days:[1,7],xid:[-Infinity,Infinity],retained:true};
 const b={client:'A',days:[5,11],xid:[-Infinity,Infinity],retained:true};
 assert.equal(conflict(a,b),false);
 for(const scope of [a,b])assert.equal(conflict(scope,{client:'A',days:[6,6],xid:[10,10.9],retained:false}),true);
 assert.equal(conflict(a,{client:'A',days:[8,14],xid:[10,10.9],retained:false}),false);
 assert.equal(conflict(a,{client:'B',days:[1,7],xid:[10,10.9],retained:false}),false);
 assert.deepEqual(a.days,[1,7]);assert.deepEqual(b.days,[5,11]);
});
test('ordinary coverage deletes only own actual transaction range; timeout restores on either exit',()=>{
 const s=read('forward/03_coverage_and_compatibility_acquire.sqlinc');
 assert.match(s,/xid:=pg_current_xact_id\(\)::text::numeric;span:=numrange\(xid,xid\+1,'\[\)'\)/);
 assert.match(s,/where not f\.retained and f\.token_kind='SOURCE_CONTRACT_COVERAGE'/);
 assert.match(s,/f\.coverage_span=p_coverage and f\.intent_span=span/);
 assert.match(s,/for share nowait/);assert.match(s,/for update nowait/);
 assert.match(s,/exception when others then perform set_config\('lock_timeout',old_timeout,true\);raise/);
 assert.doesNotMatch(s,/delete from public|update public|payment|provider|email/i);
});
test('new Source shape validates before scalar acquisition and range phases follow scalar identities',()=>{
 const s=read('forward/03_coverage_and_compatibility_acquire.sqlinc');
 const body=s.slice(s.indexOf('as $source_acquire$'));const validate=body.indexOf('source_token_assert_v1');
 const acquire=body.indexOf('fence_scalar_acquire_core_v1(scalar_tokens');assert.ok(validate<acquire);
 assert.ok(acquire<body.indexOf('source_coverage_acquire_v1'));
 assert.match(body,/where value->>'kind'='SOURCE_CONTRACT_COVERAGE' order by value/);
});
test('four construction leaves scan with no client SQL or executable database harness',()=>{
 for(const file of fs.readdirSync(new URL('forward/',root))){const sql=read('forward/'+file),scan=scanManagedSql(sql);
  assert.equal(scan.meta.length,0);assert.ok(scan.statements.length);}
 const boundary=read('SOURCE_BOUNDARY.md');assert.match(boundary,/must not be mounted alone/);
 assert.match(boundary,/OLD\/NEW raw guards/);assert.match(boundary,/between-RPC ACTIVE-request/);
});
