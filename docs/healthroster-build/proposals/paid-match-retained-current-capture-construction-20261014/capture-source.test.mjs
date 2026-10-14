import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
test('five full selected owners reverse through eight unique hunks',()=>{
 const p=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));assert.equal(p.records.length,5);assert.equal(p.records.reduce((n,r)=>n+r.hunks.length,0),8);
 for(const r of p.records){let s=read('forward/'+r.file);assert.equal(sha(s),r.after_sha256);for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,h.before);}assert.equal(s,read('before/'+r.file));assert.equal(sha(s),r.before_sha256);}
});
test('source counterexample keeps actual original movement separate from latest NO_CHANGE snapshot',()=>{
 const inputs=read('before/02_complete_overlap_inputs.sqlinc'),fin=fs.readFileSync(path.join(root,'docs/healthroster-build/proposals/paid-match-source-expense-native-construction-20261014/forward/07_complete_fin4_expense.sqlinc'),'utf8');
 assert.match(inputs,/transition.id=v_movement.transition_id/);assert.match(fin,/prior.id=basis.id/);
 const source=read('forward/02_complete_overlap_inputs.sqlinc');assert.match(source,/v_current_snapshot->>'state'='CURRENT'/);
 assert.match(source,/snapshot.id=\(v_current_snapshot->>'snapshot_line_id'\)::uuid/);
 // Explicit identity model of those selected source predicates, not SQL proof.
 const original={movement:'M1',snapshot:'S1'},nativeCurrent={movement:'M1',snapshot:'S2'},prior='S2';
 assert.equal(original.snapshot===prior,false);assert.equal(nativeCurrent.snapshot===prior,true);assert.equal(nativeCurrent.movement,original.movement);
});
test('actual maintained selected-transition/snapshot map replaces independent latest ranking',()=>{
 const s=read('forward/00_current_pay_snapshot.sqlinc');assert.match(s,/origin.origin_id=selected.selected_transition_id/);
 assert.match(s,/origin.order_key=selected.selected_order_key/);assert.match(s,/mapping.snapshot_line_id=s.id/);assert.match(s,/mapping.movement_id=p_movement_id/);
 assert.match(s,/cycle.current_final_revision_id=revision.id/);assert.match(s,/scope.root_timesheet_id=any\(p_native_root_ids\)/);
 assert.match(s,/count\(distinct id\)/);assert.match(s,/CURRENT_SNAPSHOT_AMBIGUOUS/);
 assert.doesNotMatch(s,/order by .*created_at|order by .*finalised_at|order by .*revision_number|limit 1/i);
});
test('genuine excluded missing pay basis retains false export presence rather than creating a new source row',()=>{
 const s=read('forward/00_current_pay_snapshot.sqlinc');assert.match(s,/transition.new_present as source_export_present/);
 assert.match(s,/transition.outcome='CANCEL' and transition.exclude_cancel then transition.previous_snapshot_line_id/);
 assert.match(s,/weekly_source_missing_cancel_transition_assert_v1\(transition.id\)/);
 assert.match(s,/not transition.new_present and transition.new_snapshot_line_id is null/);
 assert.doesNotMatch(s,/insert into|update public|delete from/i);
});
test('unknown current snapshot remains a blocking factual overlap, not dropped or paid-false',()=>{
 const s=read('forward/02_complete_overlap_inputs.sqlinc');assert.match(s,/Retain a factual unknown\/legacy overlap rather than erasing it/);
 assert.match(s,/'current_pay_snapshot',v_current_snapshot,'native_payment_facts',v_payment/);
 assert.doesNotMatch(s,/paid.*:=false|native_payment_facts.*paid.*false/i);
 for(const f of ['03_complete_choice_admit.sqlinc','04_complete_choice_current.sqlinc','05_complete_office_question.sqlinc'])assert.match(read('forward/'+f),/current_pay_snapshot,state\}' is distinct from 'CURRENT'/);
});
test('retained NONE transition binds the complete actual B set, original NORMAL pay identity and raw clinical tuple',()=>{
 const s=read('forward/01_retained_paid_transition_assert.sqlinc');assert.match(s,/t.new_snapshot_line_id is distinct from t.previous_snapshot_line_id/);
 assert.match(s,/old_line.pay_effect<>'NORMAL'/);assert.doesNotMatch(s,/old_line.paid_overlap_choice_id is not null/);
 assert.match(s,/rows is distinct from t.paid_match_source_rows/);assert.match(s,/s.pay_effect='NORMAL'/);assert.match(s,/r.paid_overlap_choice_census->b.upload_row_id/);
 assert.match(s,/raw_row.row_finalisation_state<>'SOURCE_WORKED'/);assert.match(s,/b.actual_net_minutes,b.source_fingerprint/);
 assert.match(s,/m.pay_effect<>'NORMAL'/);assert.match(s,/mapping.snapshot_line_id=old_line.id and mapping.movement_id=m.id/);
 assert.match(s,/current_pay_snapshot,snapshot_line_id/);
});
test('retained journal/audit and current finaliser provenance is exact, never a guessed current reader',()=>{
 const s=read('forward/01_retained_paid_transition_assert.sqlinc');assert.match(s,/viewer_actor_user_id' is distinct from r.finalised_by_user_id/);
 assert.match(s,/journal.request_hash is distinct from private.weekly_source_sha256_jsonb_v1/);assert.match(s,/audit.after_json->'choice' is distinct from to_jsonb\(choice\)/);
 assert.match(s,/audit.after_json->'admission' is distinct from choice.paid_overlap_inputs_json/);assert.match(s,/audit.after_json->'request' is distinct from journal.request_json/);
 assert.match(s,/transition_fingerprint is distinct from private.weekly_source_sha256_jsonb_v1/);
});
test('shared capture reproof precedes existing map and leaves ordinary/absence/cash/mapping branches exact',()=>{
 const s=read('forward/06_complete_shared_capture_proposal.sqlinc'),p=read('before/06_complete_shared_capture_proposal.sqlinc');
 assert.ok(s.indexOf('weekly_source_paid_match_retained_transition_assert_v1')<s.indexOf('select s.movement_id into v_movement'));
 for(const marker of ['BPAY_NEXT_SOURCE_PREVIOUS_SNAPSHOT_UNCAPTURED',"v_t.outcome='CANCEL' and v_t.exclude_cancel",'bpay_next_source_current_toggle_v1',"'TRANSITION',p_transition",'source_present'])assert.equal(s.split(marker).length,p.split(marker).length,marker);
 assert.match(s,/economic_snapshot_id' is distinct from v_economic::text/);assert.doesNotMatch(s,/values\(.*'NORMAL'/);
});
test('two new helpers are owner-only and create no business state/classifier or money',()=>{
 for(const f of ['00_current_pay_snapshot.sqlinc','01_retained_paid_transition_assert.sqlinc']){const s=read('forward/'+f);assert.match(s,/language plpgsql stable security definer/);assert.match(s,/from public,anon,authenticated,service_role/);assert.doesNotMatch(s,/\b(?:insert into|update public|delete from|round\()\b/i);}
});
