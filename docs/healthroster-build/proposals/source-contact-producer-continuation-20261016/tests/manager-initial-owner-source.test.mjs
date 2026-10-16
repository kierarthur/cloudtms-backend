// Exact complete source recipe, not PostgreSQL/native acceptance proof.
import test from'node:test';
import assert from'node:assert/strict';
import{execFileSync}from'node:child_process';
import{readFileSync}from'node:fs';
import{createHash}from'node:crypto';
import path from'node:path';
import{fileURLToPath}from'node:url';
const base=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),root=path.resolve(base,'../../../..');
const recipe=JSON.parse(readFileSync(path.join(base,'MANAGER_INITIAL_OWNER_INPUTS.json'),'utf8'));
const sha=value=>createHash('sha256').update(value).digest('hex');
const source=execFileSync('git',['show',`${recipe.input.commit}:${recipe.input.path}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
const start=source.indexOf('create or replace function '+recipe.signature.split('(')[0]+'('),end=source.indexOf('$function$;',source.indexOf('as $function$',start));
const parent=source.slice(start,end+12).trimEnd()+'\n',actual=readFileSync(path.join(base,recipe.output.path),'utf8');
function applyRecipe(value){for(const delta of recipe.deltas){assert.equal(value.split(delta.from).length,2,delta.reason);value=value.replace(delta.from,delta.to);}return value;}
test('entire genuine current native parent changes only by the pinned ordered finite seams',()=>{
 assert.equal(sha(source),recipe.input.sha256_normalized_lf);assert.equal(sha(parent),recipe.input.complete_function_sha256);
 assert.equal(applyRecipe(parent),actual);assert.equal(sha(actual),recipe.output.sha256);
});
test('original public request/success and native current actor/cohort/permission guards remain whole',()=>{
 assert.deepEqual(recipe.public_request_keys,['actor_user_id','source_cycle_id','projection_publication_id','incident_ids']);
 for(const retained of["where key not in (\n         'actor_user_id','source_cycle_id','projection_publication_id','incident_ids'",
  "v_incident.manager_action_state in ('RESPONDED','NOT_REQUIRED')",'WEEKLY_SOURCE_MANAGER_SEND_ONE_RECIPIENT_REQUIRED',
  'and not (incident.id=any(v_requested))',"'recipient_generation_id',v_generation_id,\n    'send_available_at_utc',v_send_available"]){
  assert.ok(actual.includes(retained),retained);
 }
 assert.ok(actual.includes('v_incident.id=any(v_initial_ids)and v_comparison.candidate_timesheet_id is null'));
});
test('one selected union produces one captured body and exactly one original route cooldown write',()=>{
 assert.equal((actual.match(/v_generation:=private\.weekly_contact_source_manager_selection_capture_v1/g)||[]).length,1);
 assert.equal((actual.match(/set manager_send_available_at_utc=/g)||[]).length,1);
 assert.ok(!actual.includes('weekly_source_query_manager_intent_v1('));
 assert.ok(!actual.includes('weekly_source_query_send_manager_now_atomic_v1(\n      '));
 assert.ok(actual.indexOf('weekly_contact_source_manager_selection_lock_v1')<actual.indexOf('    for update'));
 assert.ok(actual.indexOf('v_send_available:=clock_timestamp()')>actual.indexOf('weekly_contact_source_manager_selection_capture_v1'));
 assert.equal((actual.match(/v_send_available/g)||[]).length,5);
});
test('initial-only worker transition preserves original attempted owners and mixed question clocks',()=>{
 const attempted=actual.indexOf('weekly_contact_source_render_attempted_v1(render.id)'),activation=actual.indexOf("v_cycle_id,v_selected.candidate_id,v_selected.client_id,'CANDIDATE','MANAGER_DIRECT'");
 assert.ok(attempted>0&&attempted<activation);
 assert.ok(actual.includes("member.state='ACTIONABLE'\n        and not(member.incident_id=any(v_initial_ids))"));
 assert.ok(actual.includes("where candidate_generation_id=v_candidate_generation.id and incident_id=any(v_initial_ids)and state='ACTIONABLE'"));
 assert.ok(actual.includes('weekly_contact_source_recompose_intent_v1(v_worker_intent.id,null)'));
 assert.ok(!/set (?:started_at_utc|reminder_due_at_utc|deadline_at_utc)=/i.test(actual));
});
for(const[name,from,to]of[
 ['unsigned initial admitted','v_incident.id=any(v_initial_ids)and v_comparison.candidate_timesheet_id is null','false'],
 ['accepted predecessor bypassed',"v_followup->>'reason'is distinct from 'SCHEDULED'","false"],
 ['whole question cancelled despite remaining members',"and not(member.incident_id=any(v_initial_ids))","and false"],
 ['unknown owner cleared','private.weekly_contact_source_render_attempted_v1(render.id)','false'],
 ['cooldown bypass','greatest(v_route.manager_send_available_at_utc,v_contact_available)>clock_timestamp()','false']]){
 test('rejects altered source seam: '+name,()=>{const hostile=actual.replace(from,to);assert.notEqual(hostile,actual);assert.notEqual(hostile,applyRecipe(parent));});
}
