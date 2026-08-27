import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8');
const worker = read('broker/src/index.js');
const heartbeat = read('supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_rpc_changes_ping.sql');
const migration = read('supabase/migrations/27082026_1916_candidate_timesheet_summary_revision.sql');
const repeatable = read('supabase/repeatable/27082026_1916_candidate_timesheet_summary_refresh_v1.sql');

test('Candidate Summary heartbeat is opt-in, cursor-only and index bounded', () => {
  assert.match(heartbeat, /if v_candidate_summary_watch then/i);
  assert.match(heartbeat, /revision_row\.revision_seq>v_candidate_summary_cursor/i);
  assert.match(heartbeat, /order by revision_row\.revision_seq,revision_row\.identity_kind,revision_row\.identity_id\s+limit 101/i);
  assert.match(heartbeat, /'candidate_timesheet_summary_cap',100/i);
  assert.doesNotMatch(heartbeat, /jsonb_array_elements\([^)]*candidate_timesheet_summary/i);
});

test('Candidate Summary revision storage is bounded latest-state and private', () => {
  assert.match(migration, /primary key\(identity_kind,identity_id\)/i);
  assert.match(migration, /unique index[^;]+\(revision_seq\)/is);
  assert.match(migration, /revoke all on table private\.candidate_timesheet_summary_revisions\s+from public,anon,authenticated,service_role/i);
  assert.match(repeatable, /on conflict\(identity_kind,identity_id\) do update/i);
  assert.match(repeatable, /grant execute on function public\.candidate_timesheet_summary_cursor_v1\(\) to service_role/i);
  assert.doesNotMatch(repeatable, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('ordinary timesheet authorisation is not a Candidate revision source', () => {
  assert.match(repeatable, /on public\.candidate_submission_workflows/i);
  assert.match(repeatable, /on public\.candidate_approval_requests/i);
  assert.match(repeatable, /on public\.mail_outbox/i);
  assert.doesNotMatch(repeatable, /on public\.timesheets/i);
  assert.match(repeatable, /payment_scope_json \? 'candidate_workflow_id'/i);
});

test('Worker patch endpoint is strictly bounded and skips full Summary totals', () => {
  assert.match(worker, /Candidate Timesheet Summary patch requires 1-200 identities/);
  assert.match(worker, /timesheet_summary_lightweight_rows_v1/);
  assert.match(worker, /\/api\/timesheets\/candidate-summary-patches/);
  assert.match(worker, /\(currentTimesheetId && byTimesheetId\.get\(currentTimesheetId\)\)[\s\S]*\|\| \(timesheetId && byTimesheetId\.get\(timesheetId\)\)[\s\S]*\|\| \(contractWeekId && byContractWeekId\.get\(contractWeekId\)\)/);
  const start = worker.indexOf('async function handleCandidateTimesheetSummaryPatches');
  const end = worker.indexOf('async function handleTimesheetsSummary', start);
  const handler = worker.slice(start, end);
  assert.doesNotMatch(handler, /timesheet_summary_lightweight_totals_v1/);
  assert.doesNotMatch(handler, /handleTimesheetsSummary/);
  assert.doesNotMatch(handler, /candidate.*lifecycle/i);
});
