import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repeatablePath = path.join(
  repositoryRoot,
  'supabase',
  'repeatable',
  '25082026_2255_candidate_home_actionable_timesheet_count_v1.sql'
);
const sql = fs.readFileSync(repeatablePath, 'utf8');

test('Candidate Home counts only work that still requires Candidate action', () => {
  assert.match(sql, /create or replace function private\._candidate_home_summary_v1/i);
  assert.match(sql, /has_actionable_rejection/i);
  assert.match(sql, /has_candidate_action/i);
  assert.match(sql, /has_submitted_or_processed_workflow/i);
  assert.match(sql, /'CREATED','WORKER_DRAFT','AWAITING_PAPER_RETURN','REFUSED'/i);
  assert.match(sql, /'WORKER_SUBMITTED','WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'/i);
  assert.match(sql, /'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL'/i);
  assert.match(sql, /'MANAGER_APPROVED','MANAGER_APPROVED_PENDING_FINAL_DOCUMENT'/i);
  assert.match(sql, /'READY_TO_FINALISE','RECEIVED','FINALISED'/i);
  assert.match(sql, /not workflow_state\.has_submitted_or_processed_workflow/i);
});

test('Candidate Home workflow classification follows the same card mapping authority', () => {
  const mappingCalls = sql.match(/private\._candidate_workflow_maps_to_card_v1\(/gi) || [];
  assert.equal(mappingCalls.length, 3);
  assert.match(sql, /not private\._candidate_rejection_replaced_v1\(workflow\.id\)/i);
});

test('Candidate Home replacement preserves private security and SQL runtime rules', () => {
  assert.match(sql, /security definer/i);
  assert.match(sql, /set search_path=''/i);
  assert.match(sql, /owner to postgres/i);
  assert.match(sql, /revoke all on function private\._candidate_home_summary_v1[\s\S]*from public,anon,authenticated,service_role/i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
