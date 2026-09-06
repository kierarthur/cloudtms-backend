import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const workflow = fs.readFileSync(new URL(
  '../supabase/repeatable/04092026_1952_candidate_expense_history_anchor_recovery_v1.sql',
  import.meta.url,
), 'utf8');
const action = fs.readFileSync(new URL(
  '../supabase/repeatable/05092026_0941_candidate_protected_additional_expense_action_v1.sql',
  import.meta.url,
), 'utf8');
const closure = fs.readFileSync(new URL(
  '../supabase/repeatable/06092026_0337_candidate_sequential_expense_manager_approval_gate_v1.sql',
  import.meta.url,
), 'utf8');
const verification = fs.readFileSync(new URL(
  '../supabase/verification/06092026_0345_candidate_sequential_expense_manager_approval_gate_verification.sql',
  import.meta.url,
), 'utf8');
const primaryActionSource = action.slice(
  action.indexOf('create or replace function private._candidate_timesheet_primary_action_v1'),
  action.indexOf('create or replace function private._candidate_timesheet_action_contract_v1'),
);

test('manager-approved finalised expenses no longer block the next claim', () => {
  assert.match(
    workflow,
    /prior\.state not in \([\s\S]*'SUPERSEDED','FINALISED'[\s\S]*\)/i,
  );
  assert.match(
    workflow,
    /approved_claim\.state='FINALISED'[\s\S]*approved_claim\.target_timesheet_id=prior_timesheet\.timesheet_id/i,
  );
  assert.doesNotMatch(
    workflow,
    /prior\.state<>'FINALISED'[\s\S]*prior_fin\.authorised_at_utc is not null/i,
  );
  assert.doesNotMatch(
    primaryActionSource,
    /'AWAITING_PAPER_RETURN','RECEIVED','REFUSED','FINALISED'/i,
  );
});

test('the current closure reapplies workflow creation before phone actions', () => {
  assert.match(
    closure,
    /\\ir 04092026_1952_candidate_expense_history_anchor_recovery_v1\.sql[\s\S]*\\ir 05092026_0941_candidate_protected_additional_expense_action_v1\.sql/i,
  );
});

test('rollback-contained proof covers sequential success and parallel refusal', () => {
  assert.match(
    verification,
    /state='FINALISED'[\s\S]*candidate_workflow_transition_atomic_v1[\s\S]*Second sequential expense workflow was not opened/i,
  );
  assert.match(
    verification,
    /Parallel unfinished expense claim was accepted[\s\S]*CANDIDATE_EXPENSE_CLAIM_ALREADY_ACTIVE/i,
  );
  assert.match(verification, /rollback;/i);
});
