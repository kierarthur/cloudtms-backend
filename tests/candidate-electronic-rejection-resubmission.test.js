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
const pendingAuthUnlock = read(
  'supabase/migrations/27082026_0459_unlock_repaired_electronic_rejection_replacements.sql'
);
const historicalShapeRepair = read(
  'supabase/migrations/27082026_0514_repair_historical_electronic_rejection_route_and_lifecycle.sql'
);
const verification = read(
  'supabase/verification/27082026_0427_candidate_electronic_rejection_resubmission_verification.sql'
);
const managerRefusalVerification = read(
  'supabase/verification/28082026_0151_candidate_manager_refusal_resubmission_verification.sql'
);
const managerRefusalCorrection = read(
  'supabase/repeatable/28082026_0214_candidate_manager_refusal_resubmission_v1.sql'
);
const timesheetCardRecovery = read(
  'supabase/repeatable/28082026_0222_candidate_timesheet_card_fallback_reinstall_v1.sql'
);
const runtime = read('tests/10082026_1817_candidate_finalised_rejection_verification.sql');
const resubmissionRuntime = read(
  'tests/11082026_1715_candidate_resubmission_idempotency_verification.sql'
);
const matrix = read('tests/run-candidate-daily-r16-local-matrix.ps1');

test('electronic rejection creates an editable electronic replacement without weakening history', () => {
  assert.match(correction, /v_current\.booking_id[\s\S]*?'MANUAL'/);
  assert.match(correction, /submission_mode_snapshot='ELECTRONIC'/);
  assert.match(correction, /'effective_submission_mode','ELECTRONIC'/);
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

  for (const required of [
    "route_repair.action='CANDIDATE_ELECTRONIC_REJECTION_REPLACEMENT_REPAIRED'",
    "rotation_audit.action='CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED'",
    "workflow.route in ('PHONE','EMAIL')",
    "current_timesheet.submission_mode='ELECTRONIC'",
    "financials.processing_status='PENDING_AUTH'",
    "set processing_status='UNPROCESSED'",
    'financials.authorised_at_utc is null',
    'financials.paid_at_utc is null',
    'financials.locked_by_invoice_id is null',
    'replacement.replacement_of_workflow_id=workflow.id'
  ]) assert.match(pendingAuthUnlock, new RegExp(required.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  assert.match(pendingAuthUnlock, /CANDIDATE_ELECTRONIC_REJECTION_REPLACEMENT_UNLOCKED/);
  assert.doesNotMatch(pendingAuthUnlock, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);

  for (const required of [
    "rotation_audit.action='CANDIDATE_ELECTRONIC_REJECTED_VERSION_ROTATED'",
    "workflow.route in ('PHONE','EMAIL')",
    "current_timesheet.submission_mode='MANUAL'",
    "week_row.submission_mode_snapshot='ELECTRONIC'",
    "financials.processing_status in ('UNPROCESSED','PENDING_AUTH')",
    "set processing_status='UNPROCESSED'",
    "coalesce(current_timesheet.actual_schedule_json,'[]'::jsonb) in ('[]'::jsonb,'{}'::jsonb)",
    'financials.authorised_at_utc is null',
    'financials.paid_at_utc is null',
    'financials.locked_by_invoice_id is null',
    'replacement.replacement_of_workflow_id=workflow.id'
  ]) assert.match(historicalShapeRepair, new RegExp(required.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  assert.doesNotMatch(historicalShapeRepair, /set\s+submission_mode\s*=\s*'ELECTRONIC'/i);
  assert.match(historicalShapeRepair, /CANDIDATE_ELECTRONIC_REJECTION_REPLACEMENT_REPAIRED/);
  assert.doesNotMatch(historicalShapeRepair, /rotation_audit\.after_json->>'new_timesheet_id'/);
  assert.doesNotMatch(historicalShapeRepair, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('installed-state verification and real resubmission regression are release-wired', () => {
  assert.match(verification, /Unrepaired electronic rejection replacement remains installed/);
  assert.match(runtime, /'RESUBMIT_REJECTED',3,'\{\}'::jsonb/);
  assert.match(runtime, /Electronic rejection replacement did not preserve unsigned draft storage and electronic route authority/);
  assert.match(matrix, /27082026_0423_candidate_electronic_rejection_resubmission_v1\.sql/);
  assert.match(verification, /financials\.processing_status='PENDING_AUTH'/);
  assert.match(verification, /Repaired electronic rejection replacement remains lifecycle-locked/);
});

test('manager-refused phone workflows share the guarded resubmission authority', () => {
  assert.match(
    managerRefusalCorrection,
    /v_rejected\.state not in \('REJECTED','REFUSED'\)/i
  );
  assert.doesNotMatch(managerRefusalCorrection, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.match(
    managerRefusalVerification,
    /v_source_workflow\.state not in \(''REJECTED'',''REFUSED''\)/i
  );
  assert.match(
    managerRefusalVerification,
    /v_rejected\.state not in \(''REJECTED'',''REFUSED''\)/i
  );
  assert.match(
    resubmissionRuntime,
    /case when v_no=2 then 'REFUSED' else 'REJECTED' end/i
  );
  assert.match(resubmissionRuntime, /COMPLETE_ELECTRONIC_TRANSACTION/i);
  assert.match(resubmissionRuntime, /PHONE manager-refused workflow did not restart through ELECTRONIC/i);
  assert.match(
    timesheetCardRecovery,
    /\\ir 27082026_2350_candidate_timesheet_card_base_expense_fallback_v1\.sql/i
  );
});
