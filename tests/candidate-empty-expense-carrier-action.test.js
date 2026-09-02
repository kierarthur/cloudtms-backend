import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relative) => fs.readFileSync(path.join(root, relative), 'utf8');
const repeatablePath = 'supabase/repeatable/31082026_0557_candidate_empty_expense_carrier_action_v1.sql';
const verifierPath = 'supabase/verification/31082026_0605_candidate_empty_expense_carrier_action_verification.sql';
const repeatable = read(repeatablePath);
const verifier = read(verifierPath);
const release = JSON.parse(read('supabase/release/current-release.json'));

test('empty expense-only carriers add to the existing workflow while genuine drafts continue', () => {
  assert.match(repeatable, /component_kind in \([\s\S]*'MILEAGE_FORM','EXPENSE_EVIDENCE'[\s\S]*and not v_draft_has_content then 'ADD_EXPENSES'/i);
  assert.match(repeatable, /not v_draft_has_content[\s\S]*can_edit_hours[\s\S]*v_workflow\s*:=\s*null/i);
  assert.match(repeatable, /v_code='ADD_EXPENSES' and nullif\(p_action->>'workflow_id',''\) is not null[\s\S]*'destination'[\s\S]*'EXPENSE_CLAIM_EDITOR'/i);
  assert.doesNotMatch(repeatable, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.match(verifier, /'WORKER_DRAFT'[\s\S]*'ADD_EXPENSES'[\s\S]*'CLIENT_DESTINATION'[\s\S]*'EXPENSE_CLAIM_EDITOR'/i);
  assert.match(verifier, /can_edit_hours',true[\s\S]*'ENTER_TIMESHEET'/i);
  assert.match(verifier, /insert into public\.candidate_submission_components[\s\S]*'CONTINUE_EXPENSE_CLAIM'/i);
  assert.match(verifier, /count\(\*\)[\s\S]*candidate_submission_workflows[\s\S]*<>1/i);
});

test('release installs the replacement before its rollback-contained first-use proof', () => {
  assert.ok(repeatablePath.localeCompare('supabase/repeatable/31082026_0322_timesheet_cross_record_overlap_guard_v1.sql') > 0);
  assert.ok(release.verificationFiles.includes(verifierPath));
  assert.ok(release.newVerificationFiles.includes(verifierPath));
});
