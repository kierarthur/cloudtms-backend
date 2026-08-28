import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const read = (path) => fs.readFileSync(new URL('../' + path, import.meta.url), 'utf8').replace(/\r\n/g, '\n');
const names = [
  '1857_candidate_daily_booked_source', '1912_candidate_daily_receipt_context',
  '1913_candidate_daily_submission_admission', '1923_candidate_daily_factual_receipt',
  '1925_candidate_daily_receipt_finalisation', '1928_candidate_daily_receipt_reset',
  '1950_candidate_daily_action_target', '2000_candidate_daily_read_context',
  '2002_candidate_daily_detail_projection', '2017_candidate_daily_timesheet_list',
  '2027_candidate_daily_office_receipt_adapter'
];
const sql = names.map((name) => read(`supabase/repeatable/28082026_${name}_v1.sql`)).join('\n');
const proofPath = 'supabase/verification/28082026_1858_candidate_daily_booked_source_verification.sql';

test('Daily first-submission changes only Candidate routines, not financial or schema owners', () => {
  const routines = [...sql.matchAll(/create or replace function\s+((?:public|private)\.[a-z0-9_]+)/gi)].map((m) => m[1]);
  assert.ok(routines.length >= 18);
  assert.ok(routines.every((name) => /^(public\.candidate_|private\._candidate_)/.test(name)));
  assert.doesNotMatch(sql, /(?:create|alter|drop)\s+(?:table|type|trigger|policy|index)\b/i);
  assert.doesNotMatch(sql, /session_replication_role|disable\s+trigger/i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('Daily first use is mandatory in both NEW and UPGRADE, with rollback-only fixtures', () => {
  const release = JSON.parse(read('supabase/release/current-release.json'));
  for (const key of ['verificationFiles', 'newVerificationFiles']) assert.ok(release[key].includes(proofPath));
  const proof = read(proofPath);
  assert.match(proof, /^begin;/m);
  assert.match(proof, /^rollback;\s*$/m);
  assert.doesNotMatch(proof, /^commit;/im);
  for (const boundary of ['mixed Weekly/Daily', 'Office rejection/replay', 'existing Office resolution',
    'connected submit, PHONE approval', 'stale/foreign/disabled/partial', 'no receipt on read']) {
    assert.ok(proof.includes(boundary), boundary);
  }
});

test('Daily receipt distinguishes successful receipt from financial finalisation', () => {
  const receipt = read('supabase/repeatable/28082026_1923_candidate_daily_factual_receipt_v1.sql');
  assert.doesNotMatch(receipt, /insert into public\.timesheets_financials|timesheet_authorise_generic_atomic|timesheet_daily_manual_process_atomic/i);
  assert.match(receipt, /where timesheet_id=v_timesheet\.timesheet_id and is_current and authorised_at_server is null/i);
  assert.doesNotMatch(receipt, /authorised_at_server\s*=/i);
  const finalise = read('supabase/repeatable/28082026_1925_candidate_daily_receipt_finalisation_v1.sql');
  assert.match(finalise, /'RECEIVED'/);
  assert.match(finalise, /CANDIDATE_DAILY_FACTUAL_RECEIPT_V1/);
  assert.match(sql, /pg_advisory_xact_lock/);
});
