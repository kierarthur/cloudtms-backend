import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const source = fs.readFileSync(path.join(root, 'broker/src/index.js'), 'utf8');
const helperStart = source.indexOf('async function loadContractWeekWithdrawnSubmissionHistory');
const handlerStart = source.indexOf('async function handleContractWeekStagedEvidenceList', helperStart);
const handlerEnd = source.indexOf('async function handleContractWeekStagedEvidenceUpdateKind', handlerStart);

assert.ok(helperStart >= 0 && handlerStart > helperStart, 'planned-week withdrawn history helper must exist');
assert.ok(handlerEnd > handlerStart, 'staged evidence list handler must exist');
const helper = source.slice(helperStart, handlerStart);
const handler = source.slice(handlerStart, handlerEnd);

test('planned Contract Week history is bound to cancelled workflows for that exact week', () => {
  assert.match(helper, /candidate_submission_workflows/);
  assert.match(helper, /contract_week_id=eq\.\$\{enc\(contractWeekId\)\}/);
  assert.match(helper, /state=eq\.CANCELLED/);
  assert.match(helper, /target_timesheet_id/);
  assert.match(helper, /anchor_timesheet_id/);
  assert.match(helper, /timesheet_id=in\.\(\$\{historicalIdList\}\)/);
  assert.match(helper, /is_current=eq\.false/);
  assert.match(helper, /status=eq\.REVOKED/);
});

test('planned Contract Week returns the same audit-only history shape used by real Timesheets', () => {
  assert.match(helper, /withdrawn_reason: historical\?\.revoked_reason \|\| null/);
  assert.match(helper, /withdrawn_by_display: withdrawnByDisplay/);
  assert.match(helper, /withdrawal_scope: withdrawalScope/);
  assert.match(helper, /display_name: 'Official withdrawn timesheet'/);
  assert.match(helper, /kind: 'ELECTRONIC_SIGNATURES'/);
  assert.match(helper, /preview_mode: 'SIGNATURES',[\s\S]*booking_id: historical\?\.booking_id \|\| null/);
  assert.match(helper, /protected: true,[\s\S]*withdrawn_history: true,[\s\S]*is_view_only: true/);
  assert.match(helper, /can_delete: false,[\s\S]*can_reclassify: false,[\s\S]*can_return_to_queue: false/);
});

test('staged evidence list includes history separately without merging it into editable items', () => {
  assert.match(handler, /loadContractWeekWithdrawnSubmissionHistory\(env, weekId\)/);
  assert.match(handler, /items,[\s\S]*withdrawn_submissions: withdrawnSubmissions/);
  assert.doesNotMatch(handler, /items\.(?:push|unshift|splice)\([^)]*withdrawn/i);
});

test('manager refusals are loaded separately with the exact reason and manager route identity', () => {
  assert.match(helper, /async function loadCandidateManagerRefusalHistory/);
  assert.match(helper, /state=eq\.REFUSED/);
  assert.match(helper, /candidate_approval_requests[\s\S]*refusal_reason,refused_at_utc/);
  assert.match(helper, /candidate_display_name: candidateDisplayById/);
  assert.match(helper, /manager_email: String\(approval\?\.manager_email_normalized/);
  assert.match(helper, /refusal_reason: String\(approval\?\.refusal_reason \|\| workflow\?\.rejection_reason/);
  assert.match(handler, /loadCandidateManagerRefusalHistory\(env, \{ contractWeekId: weekId \}\)/);
  assert.match(handler, /withdrawn_submissions: withdrawnSubmissions,[\s\S]*manager_refusals: managerRefusals/);
});
