import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';import {functionEvents,sqlTokens} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=s=>createHash('sha256').update(s).digest('hex'),proof=JSON.parse(read('COMPLETE_APPLIED_CONTEXT_AND_REVERSES.json'));
const selectBodies=s=>functionEvents(s).events.filter(e=>e.kind==='CREATE').map(e=>({id:e.id,body:e.body}));
test('two full reviewed-source reverses and exactly four unique hunks',()=>{
 assert.equal(proof.files.length,2);assert.equal(proof.unique_hunks,4);
 for(const f of proof.files){const b=read(f.before),a=read(f.after);assert.equal(sha(b),f.input_sha256);assert.equal(sha(a),f.output_sha256);let r=a;for(const h of [...f.hunks].reverse()){assert.equal(r.split(h.after).length,2,h.name);r=r.replace(h.after,h.before);}assert.equal(r,b);}
});
test('all four existing signatures/security/ACL remain unchanged, no new owner or table',()=>{
 for(const f of proof.files){const b=read(f.before),a=read(f.after);assert.deepEqual(selectBodies(a).map(e=>e.id),selectBodies(b).map(e=>e.id));for(const e of selectBodies(a)){assert.ok(a.includes('alter function '+e.id+' owner to postgres;'));assert.ok(a.includes('revoke all on function '+e.id+' from public,anon,authenticated,service_role;'));}assert.doesNotMatch(a,/create table|grant execute/i);}
});
test('actual financial/facts bodies unchanged; no different money or raw export presence',()=>{
 for(const f of proof.files){const before=selectBodies(read(f.before))[0].body,after=selectBodies(read(f.after))[0].body;const marker=f.after.includes('facts')?'  select * into strict v_upload from public.weekly_source_uploads where id=v_publication.upload_id;':'  v_source_present:=exists(';
 assert.ok(before.indexOf(marker)>0);assert.equal(after.slice(after.indexOf(marker)),before.slice(before.indexOf(marker)));}
});
test('prepared replacement branch and original-actor binding remain exact',()=>{
 for(const f of proof.files){const b=selectBodies(read(f.before))[0].body,a=selectBodies(read(f.after))[0].body;const start="      (publication.state='CORRECTION_READY'",end="    and profile.profile_code";assert.equal(a.slice(a.indexOf(start),a.indexOf(end)),b.slice(b.indexOf(start),b.indexOf(end)));}
});
test('completed-current proof uses actual APPLIED/current revision/pointer/result fields',()=>{
 const required=["applied.state='APPLIED'",'applied.prepared_final_revision_id=applied.applied_final_revision_id','applied.replacement_correction_upload_id=upload.id','applied.replacement_projection_publication_id=publication.id','publication.correction_session_id=applied.id','applied.completed_at_utc is not null',"final.reason='CORRECT_FINAL_SOURCE' and final.state='CURRENT'",'final.upload_id=upload.id','final.predecessor_revision_id=applied.expected_current_final_revision_id','cycle.current_final_revision_id=final.id',"applied.result_json->>'status'='CORRECTED'","'WEEKLY_SOURCE_CORRECT_FINAL_RESULT_V1'","'WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_RESULT_V1'"];
 for(const f of proof.files){const a=read(f.after),b=read(f.before);for(const clause of required)assert.equal(a.split(clause).length-1,b.split(clause).length-1+2,clause+' added in kernel and wrapper');assert.doesNotMatch(a,/applied\.actor_user_id=p_actor_user_id/);if(f.after.includes('context'))assert.match(a,/weekly_source_office_authority_v1\(p_actor_user_id,'VIEW_PAY_WORK'/);}
});
test('source purpose regression: real completed correction stays correction-purpose and is eligible',()=>{
 // A deliberately narrow purpose selection model read from literal owner
 // clauses. No SQL/security/financial evaluator or database state is claimed.
 const accepts=(s,r)=>r.pubState==='CURRENT'&&r.uploadState==='CURRENT'&&(r.purpose==='ORDINARY'||(s.includes("applied.state='APPLIED'")&&r.purpose==='FINAL_SOURCE_CORRECTION'&&r.sessionState==='APPLIED'&&r.pointersMatch&&r.hashesMatch));
 const r={pubState:'CURRENT',uploadState:'CURRENT',purpose:'FINAL_SOURCE_CORRECTION',sessionState:'APPLIED',pointersMatch:true,hashesMatch:true};for(const f of proof.files){const b=read(f.before),a=read(f.after);assert.equal(accepts(b,r),false);assert.equal(accepts(a,r),true);for(const change of [{sessionState:'COMMITTING'},{pointersMatch:false},{hashesMatch:false},{purpose:'CHECK_ONLY'}])assert.equal(accepts(a,{...r,...change}),false);assert.equal(accepts(a,{...r,purpose:'ORDINARY'}),true);}assert.equal(r.purpose,'FINAL_SOURCE_CORRECTION');
});
test('all real body punctuation is balanced and no read primitive mutates business rows',()=>{
 for(const f of proof.files)for(const e of selectBodies(read(f.after))){let depth=0,opens=0;for(const t of sqlTokens(e.body)){if(t.kind!=='punctuation')continue;if(t.value==='('){depth++;opens++;}if(t.value===')')depth--;assert.ok(depth>=0,e.id);}assert.ok(opens>0);assert.equal(depth,0,e.id);assert.doesNotMatch(e.body,/\b(?:insert into|update public\.|delete from|truncate)\b/i);}
});
