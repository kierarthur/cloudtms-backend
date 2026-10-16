import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
const s=fs.readFileSync(new URL('./forward/001_family_entry.sqlinc',import.meta.url),'utf8');
test('Daily takes only common Candidate and worker-control gate',()=>{
 assert.equal((s.match(/workflow_kind in\('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED','DAILY'\)/g)||[]).length,2);
 const c=s.indexOf('perform 1 from public.candidates');const w=s.indexOf('perform 1 from private.bpay_next_worker_control');
 const f=s.indexOf('with actual_workflows');assert.ok(c<w&&w<f);
 assert.match(s.slice(f),/and w.workflow_kind in\('CONTRACT_HOURS','CONTRACT_EXPENSE','CONTRACT_COMBINED'\)\),/);
 assert.doesNotMatch(s.replace(/^\s*--.*$/gm,''),/DAILY_RATE|H103|current_placement_v1|insert into|update public|delete from/i);
});
test('NULL Contract-week retains real anchor target and component owners',()=>{
 for(const key of ['anchor_timesheet_id','target_timesheet_id','e.owning_timesheet_id']) assert.ok(s.includes(key));
 assert.match(s,/union select w.candidate_id,cw.timesheet_id/);assert.match(s,/union select w.candidate_id,e.owning_timesheet_id/);
 assert.doesNotMatch(s,/coalesce\(w\.anchor_timesheet_id,w\.target_timesheet_id/);
});
