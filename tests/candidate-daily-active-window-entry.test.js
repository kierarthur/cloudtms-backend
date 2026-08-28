import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const read = path => fs.readFileSync(new URL('../' + path, import.meta.url), 'utf8').replace(/\r\n/g, '\n');
const sql = read('supabase/repeatable/29082026_0012_candidate_daily_active_window_entry_v1.sql');
const proofPath = 'supabase/verification/28082026_1858_candidate_daily_booked_source_verification.sql';

test('ordinary Daily entry changes only the two existing Candidate read/admission owners', () => {
  assert.deepEqual([...sql.matchAll(/create or replace function\s+([a-z0-9_.]+)/gi)].map(m => m[1]),
    ['private._candidate_daily_booked_source_v1', 'public.candidate_daily_tiles_get_v1']);
  assert.doesNotMatch(sql, /(?:create|alter|drop)\s+(?:table|type|trigger|policy|index)\b/i);
  assert.doesNotMatch(sql, /insert into|update public|delete from|pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.match(sql, /from public,anon,authenticated,service_role;/);
  assert.match(sql, /grant execute on function public\.candidate_daily_tiles_get_v1\(jsonb,date,integer\) to service_role;/);
});

test('source admission requires a started exact active booking but never a legacy hours flag', () => {
  const admission = sql.slice(0, sql.indexOf('create or replace function public.candidate_daily_tiles_get_v1'));
  assert.match(admission, /v_day\.shift_starts_at>p_now_utc/);
  assert.doesNotMatch(admission, /v_day\.timesheet_eligible/);
  assert.match(admission, /v_scope\.active_generation_id is distinct from v_generation_id/);
  assert.match(admission, /v_day\.source_row_hash is distinct from p_source->>'source_row_hash'/);
  assert.match(admission, /v_count<>14/);
  assert.match(admission, /v_day\.timesheet_authorised/);
});

test('discovery preserves legacy flags and prefers an existing current Timesheet family', () => {
  assert.match(sql, /then candidate_entry\.eligible else d\.timesheet_eligible end/);
  assert.match(sql, /d\.shift_starts_at<=pg_catalog\.now\(\)/);
  assert.match(sql, /when current_daily\.timesheet_id is not null then[\s\S]*'TIMESHEET_DETAIL'/);
  assert.match(sql, /not exists\(select 1 from public\.timesheets previous where previous\.booking_id=d\.booking_id\)/);
  assert.match(sql, /t\.sheet_scope='DAILY' and t\.is_current and t\.archived_at_utc is null/);
});

test('real first-use proof covers old flag, future shift, rollover and all Daily receipt actions', () => {
  const proof = read(proofPath);
  const release = JSON.parse(read('supabase/release/current-release.json'));
  for (const key of ['verificationFiles', 'newVerificationFiles']) assert.ok(release[key].includes(proofPath));
  for (const text of ['started shift beyond four hours', 'legacy unchanged; future denied',
    'uncertain-response retry', 'Office-authorised withdrawal accepted', 'withdrawn history leaked',
    'Office rejection/replay without finance or Rota access']) assert.ok(proof.includes(text), text);
  assert.match(proof, /^rollback;/m);
  assert.doesNotMatch(proof, /^commit;/m);
});
