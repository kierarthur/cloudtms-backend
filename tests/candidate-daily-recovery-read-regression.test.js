import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const read=p=>fs.readFileSync(path.join(root,p),'utf8').replace(/\r\n/g,'\n');
const repeat=p=>read('supabase/repeatable/'+p);
const definition=(text,name)=>{
  const start=text.indexOf('create or replace function '+name+'(');
  assert.ok(start>=0,name);
  const end=text.indexOf('$function$;',start);
  assert.ok(end>start,name);
  return text.slice(start,end+11);
};
const readPath='28082026_2203_candidate_daily_inapplicable_break_context_v1.sql';
const recoverPath='28082026_2210_candidate_daily_handoff_recovery_v1.sql';
test('view-only Daily skips only an inapplicable settings lookup',()=>{
  const name='private._candidate_daily_break_entry_v1';
  const before=definition(repeat('28082026_2000_candidate_daily_read_context_v1.sql'),name).replace(/^  --.*\n/gm,'');
  const after=definition(repeat(readPath),name).replace(/^  --.*\n/gm,'');
  assert.equal(after,before.replace('if p_client_id is not null then','if v_applicable and p_client_id is not null then'));
  assert.match(repeat(readPath),/revoke all on function[\s\S]*from public,anon,authenticated,service_role;/);
});
test('Daily recovery changes exactly one route assignment and no other transition branch',()=>{
  const name='public.candidate_workflow_transition_atomic_v1';
  const before=definition(repeat('28082026_1913_candidate_daily_submission_admission_v1.sql'),name);
  const after=definition(repeat(recoverPath),name);
  const start=before.indexOf("elsif v_action='CANCEL_MANAGER_HANDOFF' then");
  const prefix=before.slice(0,start),suffix=before.slice(start);
  assert.equal(after,prefix+suffix.replace("state='READY_FOR_MANAGER_APPROVAL',route='ELECTRONIC',",
    "state='READY_FOR_MANAGER_APPROVAL',\n      route=case when v_workflow.workflow_kind='DAILY' then 'PHONE' else 'ELECTRONIC' end,"));
  assert.match(repeat(recoverPath),/revoke all on function[\s\S]*from public,anon,authenticated;/);
});
test('both real first-use verifiers remain mandatory for NEW and UPGRADE',()=>{
  const release=JSON.parse(read('supabase/release/current-release.json'));
  for(const list of [release.verificationFiles,release.newVerificationFiles]) {
    assert.ok(list.includes('supabase/verification/28082026_2203_candidate_daily_inapplicable_break_verification.sql'));
    assert.ok(list.includes('supabase/verification/28082026_1858_candidate_daily_booked_source_verification.sql'));
  }
  const proof=read('supabase/verification/28082026_1858_candidate_daily_booked_source_verification.sql');
  assert.match(proof,/CANCEL_MANAGER_HANDOFF/);
  assert.match(proof,/phone-select-retry/);
  assert.match(proof,/PHONE_APPROVE/);
  assert.match(proof,/rollback;\s*$/i);
});
test('new definitions contain no illegal qualified conditional expressions',()=>{
  for(const file of [readPath,recoverPath]) assert.doesNotMatch(repeat(file),/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
