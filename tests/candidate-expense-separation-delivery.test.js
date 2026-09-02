import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const repeatable = await readFile(
  new URL('../supabase/repeatable/02092026_1834_candidate_expense_separation_delivery_v1.sql', import.meta.url),
  'utf8'
);
const broker = await readFile(new URL('../broker/src/index.js', import.meta.url), 'utf8');

test('protected worked records can only anchor a new expense carrier', () => {
  assert.match(repeatable, /'candidate_expenses_allowed',v_expense_route_allowed and \([\s\S]*v_hours<>0[\s\S]*v_import/);
  assert.match(repeatable, /'can_edit_expenses',v_expense_route_allowed and \([\s\S]*v_protected and \(v_hours<>0 or v_additional<>0 or v_import\)/);
  assert.match(repeatable, /'requires_carrier'[\s\S]*v_protected and \(v_hours<>0 or v_additional<>0 or v_import\)/);
  assert.match(repeatable, /'can_attach_expense_evidence'[\s\S]*not v_protected/);
});

test('expense invoice delivery is not suppressed by self-bill hours', () => {
  assert.doesNotMatch(repeatable, /^create function private\._invoice_issue_validate_batch/m);
  assert.match(repeatable, /^create or replace function private\._invoice_issue_validate_batch/m);
  assert.doesNotMatch(repeatable, /^(?:revoke|grant)\b[^\n]*timesheet_qr_refuse_and_reset/m);
  assert.doesNotMatch(repeatable, /^(?:revoke|grant)\b[^\n]*client_(?:create|update)_with_settings_v1/m);
  assert.doesNotMatch(repeatable, /^(?:revoke|grant)\b[^\n]*_invoice_generation_resolve_command_groups/m);
  assert.match(repeatable, /invoice_stream='EXPENSE' then 'EXPENSE_INVOICE_EMAIL'/);
  assert.match(repeatable, /invoice_stream<>'EXPENSE' and f\.invoice_self_bill[\s\S]*self_bill_no_invoices_sent/);
  assert.match(repeatable, /EXPENSE_INVOICE_EMAIL_REQUIRED/);
});

test('Office accepts the same Client and Contract expense policy fields', () => {
  assert.match(broker, /'candidate_expenses_require_separate_timesheet', 'candidate_expense_invoice_email'/);
  assert.match(broker, /candidate_expenses_require_separate_timesheet_override/);
  assert.match(broker, /candidate_expense_invoice_email_override/);
  assert.match(broker, /candidate_expense_invoice_email is required when Candidate expenses use a separate Timesheet/);
});
