import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';
import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
test('four complete current native consumers reverse all six unique hunks',()=>{
 const d=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.equal(d.records.length,4);assert.equal(d.records.reduce((n,r)=>n+r.hunks.length,0),6);
 for(const r of d.records){let a=read('forward/'+r.output);assert.equal(sha(a),r.after_leaf_sha256);a=a.slice(0,a.indexOf('\nALTER FUNCTION'));assert.equal(sha(a),r.after_definition_sha256);for(const h of [...r.hunks].reverse()){assert.equal(a.split(h.after).length,2);a=a.replace(h.after,h.before);}assert.equal(a,read('before/'+r.output));assert.equal(sha(a),r.before_sha256);}
});
test('whole stored census is rederived; foreign/missing/extra raw choice cannot be a subset pass',()=>{
 const s=read('forward/01_prepared_paid_choice_census.sqlinc');assert.match(s,/where raw\.upload_id=revision\.upload_id order by raw\.source_row_ordinal,raw\.id/);
 assert.match(s,/resolution\.generation=coalesce\(publication\.projection_generation,publication\.authority_scope_version\)/);
 assert.match(s,/v_census is distinct from revision\.paid_overlap_choice_census/);assert.doesNotMatch(s,/where raw\.id in|jsonb_each\(revision\.paid_overlap_choice_census/);
});
test('actual prepared session/publication and native actor, not original confirmer, bind reproof',()=>{
 const s=read('forward/01_prepared_paid_choice_census.sqlinc');for(const marker of ["revision.state is distinct from 'PREPARED'","revision.reason is distinct from 'CORRECT_FINAL_SOURCE'",'revision.finalised_by_user_id is distinct from p_actor_user_id','replacement_correction_upload_id=revision.upload_id','correction_session_id=session.id','weekly_source_paid_overlap_choice_current_v1(v_row.id,p_actor_user_id)',"v_choice->>'viewer_actor_user_id' is distinct from p_actor_user_id::text"])assert.ok(s.includes(marker),marker);
 assert.doesNotMatch(s,/confirmed_by_actor_user_id.*::uuid/);
});
test('recorded-only or stale choice/hash/scope cannot become fresh prepare proof',()=>{
 const s=read('forward/01_prepared_paid_choice_census.sqlinc');assert.match(s,/WEEKLY_SOURCE_PAID_OVERLAP_CURRENT_CHOICE_V1/);assert.doesNotMatch(s,/RECORDED_CHOICE_V1/);
 for(const field of ['candidate_id','client_id','contract_id','choice_id','pay_effect','choice_proof_sha256'])assert.ok(s.includes("v_choice->>'"+field+"' is distinct"),field);
 assert.match(s,/paid_overlap_target_work_event_id.*v_row\.work_event_id::text/);
});
test('native prepare result binds real census before its existing result hash',()=>{
 const s=read('forward/03_complete_prepare_result.sqlinc');assert.ok(s.indexOf("'paid_overlap_choice_census',v_context->'paid_overlap_choice_census'")<s.indexOf("'WEEKLY_SOURCE_CORRECT_FINAL_PREPARE_RESULT_V1'"));
 assert.ok(s.indexOf("if v_session.state in ('PREPARED','COMMITTING','APPLIED')")<s.indexOf('weekly_source_correct_final_prepared_context_v1(v_session.id)'));
});
test('fresh apply rechecks after real root-family gates and before own business CAS',()=>{
 const s=read('forward/04_complete_apply_census_recheck.sqlinc'),proof=s.indexOf('perform private.weekly_source_paid_match_prepared_census_recheck_v1');
 assert.ok(proof>s.indexOf("WEEKLY_SOURCE_CORRECTION_ROOT_NOT_MUTABLE"));assert.ok(proof<s.indexOf("set state='COMMITTING',apply_idempotency_key"));
 assert.match(s,/coalesce\(v_prepared_context->'paid_overlap_choice_census','\{\}'::jsonb\)/);
 assert.ok(s.indexOf("if v_session.state='APPLIED'")<proof);
});
test('prepared root and expense root discovery exclude invoice companions without hiding payable generations',()=>{
 const a=read('forward/02_complete_prepared_context.sqlinc'),e=read('forward/05_complete_current_pay_expenses.sqlinc');
 assert.match(a,/replacement_roots[\s\S]*movement\.pay_effect='NORMAL'/);assert.match(e,/root_events[\s\S]*movement\.pay_effect='NORMAL'/);
 assert.match(e,/ranked\.event_rank=1\s*and ranked\.source_expense_pence>0/);assert.match(e,/target\.state<>'PREPARED'/);
 assert.match(a,/prior\.prior_projection_receipt_id/);
});
test('new read helper has owner-only ACL and contains no business DML or new classifier',()=>{
 const s=read('forward/01_prepared_paid_choice_census.sqlinc');assert.match(s,/from public,anon,authenticated,service_role/);assert.match(s,/language plpgsql stable security definer/);
 assert.doesNotMatch(s,/\b(?:insert into|update public\.|delete from|create table|paid_at|paid_evidence_state)\b/i);
});
test('all full SQL leaves are finite managed source, not native proof',()=>{
 for(const f of fs.readdirSync(path.join(dir,'forward'))){const p=scanManagedSql(read('forward/'+f));assert.ok(p.statements.length);assert.equal(p.meta.length,0);}
});
