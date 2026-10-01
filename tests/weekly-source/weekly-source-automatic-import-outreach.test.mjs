import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

test('accepted source comparison starts candidate contact without Office ASK and replay sends nothing', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => {
  const fixture = readFileSync('supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql', 'utf8');
  const sql = fixture.slice(0, fixture.indexOf('do $test$')) + `
do $proof$
declare
  request jsonb:=jsonb_build_object('actor_user_id','e1000000-0000-4000-8000-000000000001',
    'source_cycle_id','e6000000-0000-4000-8000-000000000001',
    'projection_publication_id','e8000000-0000-4000-8000-000000000001');
  issue jsonb:=jsonb_build_object('work_event_id','e9000000-0000-4000-8000-000000000001',
    'candidate_timesheet_id','ea000000-0000-4000-8000-000000000001',
    'candidate_timesheet_revision',1,'candidate_shift_fingerprint',repeat('31',32),
    'contract_id','e4000000-0000-4000-8000-000000000001','issue_family','SOURCE_HOURS_DIFFER','source_presence','PRESENT',
    'candidate_start_at_local','2026-09-01 09:00','candidate_end_at_local','2026-09-01 19:00','candidate_break_minutes',30,
    'system_start_at_local','2026-09-01 09:00','system_end_at_local','2026-09-01 17:00','system_break_minutes',30);
  initial_count bigint; generation uuid; deadline timestamptz; result jsonb;
begin
  result:=public.weekly_source_query_sync_atomic_v1(request||jsonb_build_object('issues',jsonb_build_array(issue)));
  select id,deadline_at_utc into strict generation,deadline
  from public.weekly_candidate_outreach_generations
  where source_cycle_id=(request->>'source_cycle_id')::uuid and request_kind='CHECK_HOURS' and state='ACTIVE';
  perform pg_temp.assert_true(exists(select 1 from public.weekly_message_intents
    where candidate_generation_id=generation and tranche_kind='CANDIDATE_INITIAL'),'first mismatch automatically creates initial candidate intent');
  perform pg_temp.assert_true(not exists(select 1 from public.weekly_message_intents
    where source_cycle_id=(request->>'source_cycle_id')::uuid and audience_kind='MANAGER'),
    'normal first contact must not email manager immediately');
  select count(*) into initial_count from public.weekly_message_intents;
  result:=public.weekly_source_query_sync_atomic_v1(request||jsonb_build_object('issues',jsonb_build_array(issue)));
  perform private.weekly_source_import_outreach_v1((request->>'projection_publication_id')::uuid);
  perform pg_temp.assert_true((select count(*)=initial_count from public.weekly_message_intents),'unchanged replay adds no candidate or manager intent');
  perform pg_temp.assert_true(exists(select 1 from public.weekly_candidate_outreach_generations
    where id=generation and state='ACTIVE' and deadline_at_utc=deadline),'unchanged replay retains original generation and clock');
end;
$proof$;
rollback;`;
  const result=spawnSync(process.env.CLOUDTMS_TEST_PSQL || 'psql', ['-X','-h','127.0.0.1','-p',
    process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
  {input:sql,encoding:'utf8',env:{...process.env,PGOPTIONS:'-c jit=off'}});
  assert.equal(result.status,0,result.stderr || result.error?.message);
});
