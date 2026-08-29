import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repeatable = fs.readFileSync(path.join(
  repositoryRoot,
  'supabase',
  'repeatable',
  '26082026_1537_candidate_home_draft_identity_v1.sql'
), 'utf8');
const verification = fs.readFileSync(path.join(
  repositoryRoot,
  'supabase',
  'verification',
  '26082026_1538_candidate_home_draft_identity_verification.sql'
), 'utf8');

test('Candidate Home returns mutable draft record identities without inflating workflow counts', () => {
  assert.match(repeatable, /count\(distinct workflow\.id\)/i);
  assert.match(repeatable, /'draft_timesheet_record_ids',v_draft_timesheet_record_ids/i);
  assert.match(repeatable, /'draft_expense_record_ids',v_draft_expense_record_ids/i);
  assert.match(repeatable, /workflow\.contract_week_id::text/i);
  assert.match(repeatable, /workflow\.anchor_timesheet_id::text/i);
  assert.match(repeatable, /workflow\.target_timesheet_id::text/i);
  assert.match(repeatable, /workflow\.state in \('CREATED','WORKER_DRAFT'\)/i);
});

test('Candidate Home draft identity helper remains private and independently verified', () => {
  assert.match(repeatable, /security definer/i);
  assert.match(repeatable, /set search_path=''/i);
  assert.match(repeatable, /revoke all on function private\._candidate_home_summary_v1[\s\S]*from public,anon,authenticated,service_role/i);
  assert.match(verification, /CANDIDATE_HOME_DRAFT_IDENTITY_DEFINITION_MISSING/i);
  assert.match(verification, /service_role/i);
  assert.match(verification, /anon/i);
  assert.match(verification, /authenticated/i);
});
