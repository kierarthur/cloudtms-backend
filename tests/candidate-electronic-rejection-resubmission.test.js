import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const correction = read(
  'supabase/repeatable/27082026_0423_candidate_electronic_rejection_resubmission_v1.sql'
);
const repair = read(
  'supabase/migrations/27082026_0426_repair_electronic_rejection_replacements.sql'
);
const pendingAuthRepair = read(
  'supabase/migrations/27082026_0447_repair_pending_auth_electronic_rejection_replacements.sql'
);
const verification = read(
  'supabase/verification/27082026_0427_candidate_electronic_rejection_resubmission_verification.sql'
);
const runtime = read('tests/10082026_1817_candidate_finalised_rejection_verification.sql');
const matrix = read('tests/run-candidate-daily-r16-local-matrix.ps1');

test('electronic rejection creates an editable electronic replacement without weakening history', () => {
  assert.match(correction, /v_current\.booking_id[\s\S]*?'ELECTRONIC'/);
  assert.match(correction, /submission_mode_snapshot='ELECTRONIC'/);
  assert.doesNotMatch(correction, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.match(correction, /revoke all on function private\._candidate_timesheet_reject_rotate_v1/);
  assert.doesNotMatch(correction, /grant execute on function private\._candidate_timesheet_reject_rotate_v1/);
});

test('one-time repair is limited to empty audited electronic rejection replacements', () => {
  for (const required of [
    "rotation_audit.action='CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED'",
    "previous_timesheet.submission_mode='ELECTRONIC'",
    "current_timesheet.submission_mode='MANUAL'",
    "financials.processing_status='UNPROCESSED'",
    'financials.paid_at_utc is null',
    'financials.locked_by_invoice_id is null',
    'join lateral (',
    'replacement.replacement_of_workflow_id=workflow.id'
  ]) assert.match(repair, new RegExp(required.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  assert.match(repair, /CANDIDATE_ELECTRONIC_REJECTION_REPLACEMENT_REPAIRED/);
  assert.doesNotMatch(repair, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);

  for (const required of [
    "rotation_audit.action='CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED'",
    "current_timesheet.submission_mode='MANUAL'",
    "financials.processing_status in ('UNPROCESSED','PENDING_AUTH')",
    'financials.authorised_at_utc is null',
    'financials.paid_at_utc is null',
    'financials.locked_by_invoice_id is null',
    'replacement.replacement_of_workflow_id=workflow.id'
  ]) assert.match(pendingAuthRepair, new RegExp(required.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  assert.doesNotMatch(pendingAuthRepair, /previous_timesheet\.submission_mode='ELECTRONIC'/);
  assert.match(pendingAuthRepair, /CANDIDATE_ELECTRONIC_REJECTION_REPLACEMENT_REPAIRED/);
  assert.doesNotMatch(pendingAuthRepair, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('installed-state verification and real resubmission regression are release-wired', () => {
  assert.match(verification, /Unrepaired electronic rejection replacement remains installed/);
  assert.match(runtime, /'RESUBMIT_REJECTED',3,'\{\}'::jsonb/);
  assert.match(runtime, /Electronic rejection replacement was not Candidate-editable/);
  assert.match(matrix, /27082026_0423_candidate_electronic_rejection_resubmission_v1\.sql/);
  assert.match(verification, /financials\.processing_status in \('UNPROCESSED','PENDING_AUTH'\)/);
});
