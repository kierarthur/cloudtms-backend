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
  '26082026_1432_candidate_home_draft_counts_v1.sql'
), 'utf8');
const verification = fs.readFileSync(path.join(
  repositoryRoot,
  'supabase',
  'verification',
  '26082026_1433_candidate_home_draft_counts_verification.sql'
), 'utf8');

test('Candidate Home returns separate mutable hours and expense draft counts', () => {
  assert.match(repeatable, /'draft_timesheet_count',v_draft_timesheet_count/i);
  assert.match(repeatable, /'draft_expense_count',v_draft_expense_count/i);
  assert.match(repeatable, /workflow\.state in \('CREATED','WORKER_DRAFT'\)/i);
  assert.match(repeatable, /workflow\.workflow_kind in \('CONTRACT_HOURS','CONTRACT_COMBINED','DAILY'\)/i);
  assert.match(repeatable, /workflow\.workflow_kind in \('CONTRACT_EXPENSE','CONTRACT_COMBINED'\)/i);
});

test('Candidate Home draft count helper remains private and independently verified', () => {
  assert.match(repeatable, /security definer/i);
  assert.match(repeatable, /set search_path=''/i);
  assert.match(repeatable, /revoke all on function private\._candidate_home_summary_v1[\s\S]*from public,anon,authenticated,service_role/i);
  assert.match(verification, /service_role/i);
  assert.match(verification, /anon/i);
  assert.match(verification, /authenticated/i);
});
