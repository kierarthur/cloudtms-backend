import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const sqlUrl = new URL('../../supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql', import.meta.url);

test('requested-week exception is enforced at selection, rendering and render assertion', async () => {
  const sql = await readFile(sqlUrl, 'utf8');
  const helper = sql.slice(sql.indexOf('create or replace function private.weekly_source_manager_due_event_covers_v1'), sql.indexOf('create or replace function private.weekly_source_query_require_service_v1'));
  for (const condition of [
    'incident.candidate_id=cohort.candidate_id',
    'incident.source_cycle_id=cohort.source_cycle_id',
    "requested.state='SUBMITTED_WITH_ISSUES'",
    'requested.client_id=incident.client_id',
    'requested.contract_id=comparison.contract_id',
    'work_event.work_date>requested.week_ending-7',
    'work_event.work_date<=requested.week_ending',
    "incident.candidate_action_state='NOT_REQUIRED'",
  ]) assert.ok(helper.includes(condition), `missing scope condition: ${condition}`);
  assert.equal(sql.split('private.weekly_source_manager_due_event_covers_v1(event.id,incident.id)').length - 1, 4);
});

test('missing-week requests and hours questions retain separate current pointers', async () => {
  const sql = await readFile(sqlUrl, 'utf8');
  assert.match(sql, /generation\.request_kind='SUBMIT_TIMESHEET'\s+then cohort\.current_submission_generation_id else cohort\.current_generation_id end/);
  const migration = await readFile(new URL('../../supabase/migrations/30092026_2059_weekly_source_independent_candidate_requests.sql', import.meta.url), 'utf8');
  assert.match(migration, /candidate_cohort_id,request_kind/);
  assert.match(migration, /current_submission_generation_id/);
});

test('qualified protection is excluded consistently from actual outreach, not only its membership hash', async () => {
  const sql = await readFile(sqlUrl, 'utf8');
  const owner = (name) => {
    const start = sql.lastIndexOf('create or replace function '+name+'(');
    assert.ok(start >= 0, name);
    return sql.slice(start, sql.indexOf('$function$;', start)+12);
  };
  const candidate = owner('private.weekly_source_query_candidate_generation_v1');
  assert.equal(candidate.split('and not private.weekly_source_covered_hours_incident_v1(incident.id)').length-1, 3,
    'first incident, hashed membership and inserted rows have the same coverage predicate');
  const manager = owner('private.weekly_source_query_manager_generation_v1');
  assert.equal(manager.split('and not private.weekly_source_covered_hours_incident_v1(incident.id)').length-1, 2,
    'manager hashed membership and inserted rows agree');
  assert.match(sql, /v_incident\.source_cycle_id<>v_cycle\.id\s+or private\.weekly_source_covered_hours_incident_v1\(v_incident\.id\)/,
    'a stale Candidate response cannot change a newly protected incident');
  assert.match(sql, /v_item\.incident_episode<>v_incident\.episode_number\s+or private\.weekly_source_covered_hours_incident_v1\(v_incident\.id\)/,
    'an accepted Manager link cannot answer a newly protected item');
  assert.match(sql, /incident\.manager_action_state<>'RESPONDED'\s+and not private\.weekly_source_covered_hours_incident_v1\(incident\.id\)/,
    'manual/direct manager renders do not bypass protection with an empty due-event list');
  assert.match(sql, /'CHECK_HOURS' and exists\([\s\S]*?incident\.state='OPEN'\s+and not private\.weekly_source_covered_hours_incident_v1\(incident\.id\)/,
    'reminder selection excludes covered incidents independently of stale membership state');
  const outer = await readFile(new URL('../../supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql',import.meta.url),'utf8');
  assert.match(outer,/incident\.client_id=v_client\.client_id and incident\.state='OPEN'\s+and not private\.weekly_source_covered_hours_incident_v1\(incident\.id\)\s+and incident\.candidate_action_state not in/,
    'outer combined contact selection matches inner command and does not reinsert a hidden protected question');
});
