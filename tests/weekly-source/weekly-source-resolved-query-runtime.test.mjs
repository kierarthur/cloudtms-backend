import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

test('resolved source decisions survive unchanged rechecks but not changed shift facts', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => {
  const fixture=readFileSync('supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql','utf8');
  assert.equal(fixture.split('do $test$').length,2);
  const sql=fixture.slice(0,fixture.indexOf('do $test$'))+`
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
  result jsonb; incident uuid; before_events bigint; next_issue jsonb;
begin
  result:=public.weekly_source_query_sync_atomic_v1(request||jsonb_build_object('issues',jsonb_build_array(issue)));
  perform pg_temp.assert_true(result->>'new_incidents'='1','initial mismatch raises a question');
  select id into strict incident from public.weekly_discrepancy_incidents
    where work_event_id=(issue->>'work_event_id')::uuid and state='OPEN';
  perform public.weekly_source_query_accept_system_hours_atomic_v1(request||jsonb_build_object('incident_ids',jsonb_build_array(incident)));
  select count(*) into before_events from public.weekly_discrepancy_events;
  result:=public.weekly_source_query_sync_atomic_v1(request||jsonb_build_object('issues',jsonb_build_array(issue)));
  perform pg_temp.assert_true(result->>'new_incidents'='0' and result->>'unchanged_incidents'='1','same resolved facts do not reopen');
  perform pg_temp.assert_true(not exists(select 1 from public.weekly_discrepancy_incidents
    where work_event_id=(issue->>'work_event_id')::uuid and state='OPEN'),'resolved row stays out of outstanding queries');
  perform pg_temp.assert_true((select count(*)=before_events from public.weekly_discrepancy_events),'unchanged recheck adds no duplicate incident history');
  perform pg_temp.assert_true(private.weekly_source_query_resolved_decision_matches_v1(
    'e5000000-0000-4000-8000-000000000001',(issue->>'work_event_id')::uuid,
    issue||jsonb_build_object('candidate_timesheet_revision',99,'candidate_shift_fingerprint',repeat('ab',32),
      'source_row_id',gen_random_uuid(),'source_work_event_link_id',gen_random_uuid())),
    'new evidence IDs and whole-week signature revision are not changed hours');
  for next_issue in select issue||change from jsonb_array_elements(jsonb_build_array(
    jsonb_build_object('candidate_end_at_local','2026-09-01 18:00'),
    jsonb_build_object('candidate_break_minutes',15),jsonb_build_object('system_end_at_local','2026-09-01 16:00'),
    jsonb_build_object('system_break_minutes',15),
    jsonb_build_object('issue_family','SOURCE_MISSING_OR_NOT_AUTHORISED','source_presence','ABSENT',
      'system_start_at_local',null,'system_end_at_local',null,'system_break_minutes',null))) change
  loop
    perform pg_temp.assert_true(not private.weekly_source_query_resolved_decision_matches_v1(
      'e5000000-0000-4000-8000-000000000001',(issue->>'work_event_id')::uuid,next_issue),'changed material facts must not inherit acceptance');
  end loop;
  next_issue:=issue||jsonb_build_object('system_break_minutes',15);
  result:=public.weekly_source_query_sync_atomic_v1(request||jsonb_build_object('issues',jsonb_build_array(next_issue)));
  perform pg_temp.assert_true(result->>'new_incidents'='1','changed source break raises a new episode');
  perform pg_temp.assert_true(exists(select 1 from public.weekly_discrepancy_incidents
    where work_event_id=(issue->>'work_event_id')::uuid and state='OPEN' and episode_number=2),'new episode does not rewrite previous resolution');
  result:=public.weekly_source_query_sync_atomic_v1(request||jsonb_build_object('issues','[]'::jsonb));
  perform pg_temp.assert_true((result->>'resolved_incidents')::integer>=1,'actual match resolves current issue');
  result:=public.weekly_source_query_sync_atomic_v1(request||jsonb_build_object('issues',jsonb_build_array(next_issue)));
  perform pg_temp.assert_true(result->>'new_incidents'='1','genuine recurrence after actual match is not suppressed by old mismatch');
  perform pg_temp.assert_true(not has_function_privilege('authenticated',
    'private.weekly_source_query_resolved_decision_matches_v1(uuid,uuid,jsonb)','EXECUTE'),'decision helper is private');
end;
$proof$;
rollback;`;
  const result=spawnSync(process.env.CLOUDTMS_TEST_PSQL||'psql',['-X','-h','127.0.0.1','-p',
    process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
    {input:sql,encoding:'utf8',env:{...process.env,PGOPTIONS:'-c jit=off'}});
  assert.equal(result.status,0,result.stderr||result.error?.message);
});

test('signed-week comparison preserves Office acceptance on repeated source publication checks', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => {
  const fixture=readFileSync('supabase/verification/15092026_2203_weekly_source_candidate_app_contract_v1.sql','utf8');
  assert.equal(fixture.split('\nrollback;').length,2);
  const proof=`
do $accepted_signed_week$
declare selected_id uuid; result jsonb; prior_events bigint;
begin
  select incident.id into strict selected_id
  from public.weekly_discrepancy_incidents incident
  join public.weekly_work_events event on event.id=incident.work_event_id
  where incident.source_cycle_id='fa600000-0000-4000-8000-000000000001'
    and event.work_date='2026-08-25' and incident.state='OPEN';
  perform public.weekly_source_query_accept_system_hours_atomic_v1(jsonb_build_object(
    'actor_user_id','fa100000-0000-4000-8000-000000000001',
    'source_cycle_id','fa600000-0000-4000-8000-000000000001',
    'projection_publication_id','fa800000-0000-4000-8000-000000000006',
    'incident_ids',jsonb_build_array(selected_id)));
  select count(*) into prior_events from public.weekly_discrepancy_events;
  result:=private.weekly_source_candidate_prefinal_publish_recheck_v1(
    'fa100000-0000-4000-8000-000000000001','fa800000-0000-4000-8000-000000000006');
  perform pg_temp.assert_true(result->>'new_incidents'='0' and result->>'changed_comparisons'='0',
    'signed-week path must not recreate an accepted missing-shift question');
  perform pg_temp.assert_true(exists(select 1 from public.weekly_discrepancy_incidents
    where id=selected_id and state='RESOLVED' and resolution_kind='OFFICE_ACCEPTED_SYSTEM_HOURS'),
    'original signed-week resolution remains immutable');
  perform pg_temp.assert_true(not exists(select 1 from public.weekly_discrepancy_incidents issue
    join public.weekly_discrepancy_incidents accepted on accepted.work_event_id=issue.work_event_id
    where accepted.id=selected_id and issue.state='OPEN'),'no duplicate open question');
  perform pg_temp.assert_true((select count(*)=prior_events from public.weekly_discrepancy_events),
    'unchanged signed-week recheck adds no duplicate question events');
end;
$accepted_signed_week$;
`;
  const sql=fixture.replace('\nrollback;',proof+'\nrollback;');
  const result=spawnSync(process.env.CLOUDTMS_TEST_PSQL||'psql',['-X','-h','127.0.0.1','-p',
    process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
    {input:sql,encoding:'utf8',env:{...process.env,PGOPTIONS:'-c jit=off'}});
  assert.equal(result.status,0,result.stderr||result.error?.message);
});
