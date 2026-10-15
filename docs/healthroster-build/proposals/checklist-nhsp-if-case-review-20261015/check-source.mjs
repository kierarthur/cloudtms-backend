import fs from 'node:fs';
import path from 'node:path';
import cp from 'node:child_process';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const repo=path.resolve(here,'../../../..');
const target=process.argv[2]??path.resolve(repo,'../healthroster-install2-14102026');
const commit='b46eed23eb97a595cd15ab4a8b2d0f950bec3f84';
const rel='docs/healthroster-build/proposals/nhsp-phased-if-case-compilation-fix-20261015';
const sha=b=>createHash('sha256').update(b).digest('hex');
const raw=f=>cp.execFileSync('git',['-C',target,'cat-file','blob',commit+':'+rel+'/'+f],{maxBuffer:8e6});
const read=f=>fs.readFileSync(path.join(target,rel,f));
const pins=[];
function pinned(f){const b=read(f);assert.deepEqual(b,raw(f),f+' saved/raw');pins.push({file:f,bytes:b.length,sha256:sha(b)});return b;}
const manifest=JSON.parse(pinned('SHA256_MANIFEST.json'));
for(const row of manifest.artifacts){const b=pinned(row.file);assert.equal(b.length,row.bytes);assert.equal(sha(b),row.sha256);}
assert.equal(pins.length,21);
const stable=()=>{for(const p of pins){const b=read(p.file);assert.equal(sha(b),p.sha256);assert.deepEqual(b,raw(p.file));}};
const reversals=JSON.parse(read('COMPLETE_REVERSES.json')).reversals;
const sources=JSON.parse(read('INITIAL_SOURCE_PINS.json')).rows;
assert.equal(reversals.length,3);assert.equal(sources.length,3);
let initial='',retry='';
for(let i=0;i<3;i++){
 const r=reversals[i],s=sources[i],before=read(r.before_file),after=read(r.after_file);
 assert.equal(r.commit,'1e54ff551d48b6d6e7b7de5aaf825a6b08ac4622');assert.equal(s.file,r.input_file);
 const pre=cp.execFileSync('git',['-C',target,'cat-file','blob',r.commit+':'+r.input_file],{maxBuffer:8e6});
 assert.deepEqual(before,pre);assert.equal(sha(pre),s.raw_sha256);assert.equal(sha(before),r.before_sha256);assert.equal(sha(after),r.after_sha256);
 assert.equal(r.hunks.length,1);const h=r.hunks[0];assert.equal(h.before,s.expression);assert.equal(h.after,'('+h.before+')');
 assert.equal(before.toString().split(h.before).length,2);assert.equal(after.toString().split(h.after).length,2);
 assert.deepEqual(Buffer.from(before.toString().replace(h.before,()=>h.after)),after);
 assert.deepEqual(Buffer.from(after.toString().replace(h.after,()=>h.before)),before);
 assert.equal(after.length,before.length+2);
 const constructor=e=>'create function private.'+s.functionName+"() returns boolean language plpgsql as $exact_case$ declare v_match jsonb:='{}';v_item jsonb:='{}';begin if "+e+' then return true;end if;return false;end $exact_case$;';
 initial+=(i?'\n':'')+constructor(h.before);retry+=(i?'\n':'')+constructor(h.after);
}
assert.equal(read('INITIAL_NATIVE_TRIAL.sql').toString(),initial.trimEnd());
for(const s of sources)retry+='\nselect private.'+s.functionName+'() as default_unavailable_branch_is_true;';
assert.equal(read('RETRY01_NATIVE_TRIAL.sql').toString(),retry.trimEnd());
for(const [name,count] of [['INITIAL',3],['RETRY01',6]]){
 const result=JSON.parse(read(name+'_NATIVE_RESULT.json'));
 assert.equal(result.database,'cloudtms_test_clone');assert.equal(result.sqlSha256,sha(read(name+'_NATIVE_TRIAL.sql')));
 assert.equal(result.statementCount,count);assert.equal(result.statements.length,count);assert(result.accepted&&result.rolledBack);
 if(name==='INITIAL')assert(result.statements.every(s=>s.error?.code==='42601'));
 else {assert(result.statements.every(s=>!s.error));assert(result.statements.slice(0,3).every(s=>s.command==='CREATE'));assert(result.statements.slice(3).every(s=>s.rows.length===1&&s.rows[0].default_unavailable_branch_is_true===true));}
 const fresh=result.rollbackVerification;assert(fresh.connectedFresh);assert.equal(fresh.database,'cloudtms_test_clone');
 const rows=fresh.statements[0].rows;assert.equal(rows.length,1);assert.equal(rows[0].database,'cloudtms_test_clone');
 for(const s of sources){assert.equal(rows[0][s.functionName+'_absent'],true);assert(read('ABSENCE.sql').toString().includes("to_regprocedure('private."+s.functionName+"()') is null"));}
}
const turns=read('SHARED_TURN_JOURNAL.txt').toString().trim().split('\n').map(l=>JSON.parse(l.slice(l.indexOf('{'))));
assert.equal(turns.length,4);assert.equal(turns.filter(t=>t.status==='TAKEN').length,2);
assert(turns.filter(t=>t.status==='RELEASED').every(t=>t.explicitUnlockConfirmed&&t.connectionClosed&&t.proved));
const own=cp.execFileSync(process.execPath,[path.join(target,rel,'check-source.mjs')],{cwd:target,encoding:'utf8'});assert.equal(JSON.parse(own).source_fix,'PASS');
stable();
console.log(JSON.stringify({verdict:'PASS',source_commit:commit,raw_saved_pins_before_after:pins,complete_owners:3,unique_reversals:3,added_parentheses:6,reconstructed_initial_probes:3,recorded_initial_parser_failures:3,reconstructed_retry_statements:6,recorded_retry_successes:6,recorded_rollback_fresh_absence:true,reviewer_database_calls:0,whole_business_owner_compilation:false,full_nhsp_qualification:false},null,2));
