import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import {
  classifyExpenseTimesheetPresentation,
  isExplicitOfficeCreatedExpenseRecord
} from '../broker/src/timesheet-expense-presentation.js';

const provedClaim = Object.freeze({
  line_type: 'MILEAGE',
  total_hours: 0,
  actual_schedule_json: {},
  financial_schedule_json: {},
  additional_units_week: {},
  additional_units_per_day: {},
  additional_units_json: {},
  worked_start_iso: null,
  worked_end_iso: null,
  expense_values: { mileage_units: 25 }
});

test('manual expense wording requires the exact Office-created storage shape', () => {
  assert.equal(isExplicitOfficeCreatedExpenseRecord({
    line_type: 'EXPENSES',
    submission_mode: 'MANUAL',
    status: 'SUBMITTED',
    candidate_workflow_id: null,
    workflowRoute: null
  }), true);
  assert.equal(isExplicitOfficeCreatedExpenseRecord({
    line_type: 'EXPENSES',
    submission_mode: 'MANUAL',
    status: 'RECEIVED',
    candidate_workflow_id: 'candidate-workflow'
  }), false);
});

test('expense presentation requires a proved expense or mileage record with no work', () => {
  assert.deepEqual(classifyExpenseTimesheetPresentation({
    ...provedClaim,
    workflowRoute: 'EMAIL'
  }), {
    is_expense_only: true,
    expense_route_kind: 'ELECTRONIC',
    display_route_label: 'Electronic Expense'
  });

  for (const unsafe of [
    { line_type: 'HOURS' },
    { total_hours: 8 },
    { total_hours: -8 },
    { total_hours: null },
    { actual_schedule_json: [{ start: '09:00', end: '17:00' }] },
    { worked_start_iso: '2026-09-05T09:00:00Z' },
    { additional_units_week: { on_call: 1 } },
    { expense_values: { mileage_units: 0 } }
  ]) {
    const result = classifyExpenseTimesheetPresentation({
      ...provedClaim,
      ...unsafe,
      fallbackDisplayLabel: 'Manual Adjustment'
    });
    assert.equal(result.is_expense_only, false);
    assert.equal(result.display_route_label, 'Manual Adjustment');
  }
});

test('expense presentation uses the factual route only after the strict check passes', () => {
  const cases = [
    [{ workflowRoute: 'PHONE' }, 'ELECTRONIC', 'Electronic Expense'],
    [{ workflowRoute: 'EMAIL' }, 'ELECTRONIC', 'Electronic Expense'],
    [{ workflowRoute: 'PAPER' }, 'QR', 'QR Expense'],
    [{ explicitOfficeCreated: true }, 'MANUAL', 'Manual Expense'],
    [{ submissionMode: 'MANUAL' }, 'UNKNOWN', 'Expense'],
    [{}, 'UNKNOWN', 'Expense']
  ];
  for (const [route, kind, label] of cases) {
    const result = classifyExpenseTimesheetPresentation({ ...provedClaim, ...route });
    assert.equal(result.is_expense_only, true);
    assert.equal(result.expense_route_kind, kind);
    assert.equal(result.display_route_label, label);
  }
});

test('a separate expense workflow never renames its worked timesheet anchor', () => {
  const result = classifyExpenseTimesheetPresentation({
    ...provedClaim,
    line_type: 'HOURS',
    total_hours: 8,
    workflowRoute: 'EMAIL',
    routeFamily: 'ELECTRONIC',
    fallbackDisplayLabel: 'Electronic'
  });
  assert.deepEqual(result, {
    is_expense_only: false,
    expense_route_kind: 'UNKNOWN',
    display_route_label: 'Electronic'
  });
});

test('Timesheet Summary reuses the set-wise reader result without another data request', () => {
  const broker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
  const start = broker.indexOf('function attachTimesheetExpensePresentation(rows)');
  const end = broker.indexOf('\nasync function handleCandidateTimesheetSummaryPatches', start);
  assert.ok(start >= 0 && end > start);
  const attachment = broker.slice(start, end);
  assert.doesNotMatch(attachment, /\b(?:fetch|sbFetch|sbGetOne|sbRpc)\s*\(/);

  const summary = readFileSync(
    new URL('../supabase/repeatable/19012026_extras.sql', import.meta.url),
    'utf8'
  );
  assert.match(summary, /expense_presentation_fact\.is_expense_only/);
  assert.match(summary, /v_route_type = 'manual_expense'/);
  assert.match(summary, /v_route_type = 'electronic_expense'/);
  assert.match(summary, /v_route_type = 'qr_expense'/);
  assert.match(summary,
    /REVOKE ALL ON FUNCTION public\.timesheet_summary_lightweight_rows_v1\(jsonb\) FROM PUBLIC, anon, authenticated;/i);
  assert.doesNotMatch(summary,
    /GRANT EXECUTE ON FUNCTION public\.timesheet_summary_lightweight_rows_v1\(jsonb\) TO authenticated;/i);
  const readerStart = summary.indexOf('CREATE OR REPLACE FUNCTION public.timesheet_summary_lightweight_rows_v1');
  const readerEnd = summary.indexOf('\n$function$;', readerStart);
  const reader = summary.slice(readerStart, readerEnd);
  assert.doesNotMatch(reader, /client_reference_settings|FROM public\.client_settings/i);
});
