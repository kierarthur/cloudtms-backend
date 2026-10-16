// Source recipe tests, not PostgreSQL/native execution evidence.
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {schedulerPrefix,groupedSchedulerFollowups} from '../build-scheduler-prefix.mjs';
const base=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),root=path.resolve(base,'../../../..');
const parent=execFileSync('git',['show','5b02fed2bce49f8cea8cc8a1e39b4a508e902dc4:docs/healthroster-build/proposals/source-contact-producer-continuation-20261016/forward/32_native_scheduler_followups.sqlinc'],{cwd:root,encoding:'utf8'}).replace(/\r\n/g,'\n');
const prefixed=schedulerPrefix(parent),actual=readFileSync(path.join(base,'forward/32_native_scheduler_followups.sqlinc'),'utf8');
test('complete actual scheduler equals only the finite batch-prefix and grouping transformations',()=>{
 assert.equal(actual,groupedSchedulerFollowups(prefixed.output).trimEnd()+'\n');
 assert.equal(readFileSync(path.join(base,'forward/47_native_scheduler_selection.sqlinc'),'utf8'),prefixed.snapshot.trimEnd()+'\n');
});
test('selection copies complete native ordered/limited queries and removes only selection row locking',()=>{
 for(const name of ['v_generation','v_route','v_followup']){
  const start=parent.indexOf(`  for ${name} in\n`),end=parent.indexOf('\n  loop',start);
  const query=parent.slice(start+`  for ${name} in\n`.length,end).replace(/\n    for update skip locked\s*$/,'').trim();
  assert.ok(prefixed.snapshot.includes(query),name+' exact selected producer query');
 }
 assert.equal((prefixed.snapshot.match(/limit v_limit/g)||[]).length,5);
 assert.ok(!/for update|update public|insert into|advisory/i.test(prefixed.snapshot));
});
test('complete deployment/sorted scope prefix precedes every original scheduler business lock or mutation',()=>{
 const prefix=actual.indexOf('perform private.weekly_contact_source_scheduler_selection_lock_v1(v_selected);');
 assert.ok(prefix>0);assert.ok(prefix<actual.indexOf('for update skip locked'));
 assert.ok(prefix<actual.indexOf('update public.weekly_timesheet_submission_requests'));
 assert.ok(actual.includes("submission.id in(select(value->>'id')::uuid from jsonb_array_elements(v_selected->'overdue_requests'))"));
 assert.ok(actual.includes("generation.id in(select(value->>'id')::uuid from jsonb_array_elements(v_selected->'candidate_generations'))"));
 assert.ok(actual.includes("jsonb_array_elements(v_selected->'manager_routes')selected"));
 assert.ok(actual.includes("jsonb_array_elements(v_selected->'collapse_intents')"));
 assert.ok(actual.includes("jsonb_array_elements(v_selected->'followups')"));
});
test('followups partition the complete selected due set and preserve actual native original ownership/current admission',()=>{
 assert.ok(actual.includes("partition by member.value->>'source_cycle_id',member.value->>'manager_address'"));
 assert.ok(actual.includes(')/500 partition_number'));
 assert.ok(actual.includes('weekly_contact_source_manager_followup_group_enqueue_v1(v_followup_group.incident_ids,clock_timestamp())'));
 const group=readFileSync(path.join(base,'forward/49_grouped_automatic_followup.sqlinc'),'utf8');
 assert.ok(group.includes("'routine_step',state->'routine_sequence','basis_accepted_membership_id',state->'basis_accepted_membership_id'"));
 assert.ok(group.includes("member->>'exclusion_reason'<>'MANUAL_ENQUIRY_COOLDOWN'"));
 assert.ok(group.includes("'MANAGER_AUTOMATIC_FOLLOWUP'"));
 assert.ok(group.includes("weekly_contact_source_capture_insert_v1(intent_id,'ROUTINE_FOLLOWUP'"));
 assert.ok(!/update public\.weekly_source_global_settings|manager_send_available_at_utc\s*=|provider_accepted_at_utc\s*=|accepted_at_utc\s*=/i.test(group));
});
