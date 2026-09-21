import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  adaptWeeklyCorrectFinalServiceSnapshot,
  WEEKLY_CORRECT_FINAL_SNAPSHOT_ADAPTER_CONTRACT,
  WeeklyCorrectFinalSnapshotError,
} from '../../broker/src/weekly-source/correct-final-source-snapshot-adapter.mjs';

const ID = Object.freeze({
  root: 'a1000000-0000-4000-8000-000000000001',
  contract: 'a1000000-0000-4000-8000-000000000002',
  candidate: 'a1000000-0000-4000-8000-000000000003',
  client: 'a1000000-0000-4000-8000-000000000004',
  finalRevision: 'a1000000-0000-4000-8000-000000000005',
  movement: 'a1000000-0000-4000-8000-000000000006',
  event: 'a1000000-0000-4000-8000-000000000007',
});

const HASH = 'a'.repeat(64);

function expectedSegment(overrides = {}) {
  return {
    segment_id: `weekly-source-event:${ID.event}`,
    date: '2026-09-14',
    start: '20:00',
    end: '08:00',
    overnight: true,
    break_mins: 30,
    ref_num: 'SOURCE-REF-1',
    breaks: [],
    hours_day: 1.5,
    hours_night: 10,
    hours_sat: 0,
    hours_sun: 0,
    hours_bh: 0,
    pay_amount: 130,
    charge_amount: 260,
    is_reversal: false,
    exclude_from_pay: false,
    weekly_source: {
      schema_version: 'WEEKLY_SOURCE_SEGMENT_LINEAGE_V1',
      work_event_id: ID.event,
      movement_id: ID.movement,
      final_revision_id: ID.finalRevision,
      pay_vector: {
        rates: { day: 10, night: 11.5, sat: 12, sun: 13, bh: 14 },
      },
      charge_vector: {
        rates: { day: 20, night: 23, sat: 24, sun: 26, bh: 28 },
      },
    },
    ...overrides,
  };
}

function actualSchedule() {
  return [{
    segment_id: `weekly-source-event:${ID.event}`,
    date: '2026-09-14',
    start: '20:00',
    end: '08:00',
    overnight: true,
    break_mins: 30,
    ref_num: 'SOURCE-REF-1',
    breaks: [],
    weekly_source_work_event_id: ID.event,
    weekly_source_movement_id: ID.movement,
    weekly_source_final_revision_id: ID.finalRevision,
  }];
}

function fixture(overrides = {}) {
  const segment = expectedSegment();
  const currentFinancial = {
    additional_units_json: { on_call: 2 },
    additional_pay_ex_vat: 10,
    additional_charge_ex_vat: 15,
    additional_margin_ex_vat: 5,
    expenses_pay_ex_vat: 7.5,
    expenses_charge_ex_vat: 9,
    expenses_description: 'Receipt expense',
    expenses_evidence_r2_key: 'expense/receipt.jpg',
    expenses_evidence_manifest: { schema_version: 'RECEIPT_EVIDENCE_V1', count: 1 },
    mileage_units: 12,
    mileage_pay_ex_vat: 5.4,
    mileage_charge_ex_vat: 6,
    mileage_pay_rate: 0.45,
    mileage_charge_rate: 0.5,
    mileage_evidence_r2_key: 'mileage/evidence.pdf',
    mileage_evidence_manifest: { schema_version: 'MILEAGE_EVIDENCE_V1' },
  };
  const policy = {
    timezone_id: 'Europe/London',
    erni_pct: 13.8,
    apply_erni_to: 'PAYE_ONLY',
    weekly_rate_classification_method: 'SPLIT_RATE_WINDOWS',
  };
  const rootContext = {
    root_timesheet_id: ID.root,
    source_mode: 'HEALTHROSTER_WEEKLY',
    expected_segments: [segment],
    expected_actual_schedule: actualSchedule(),
    expected_source_expenses: [],
    expected_rate_source_refs: {
      schema_version: 'WEEKLY_SOURCE_FINAL_AUTHORITY_RATE_SOURCE_V1',
      root_timesheet_id: ID.root,
      final_revision_id: ID.finalRevision,
      source_expense_manifest_hash: HASH,
    },
  };
  const timesheetContext = {
    effective_timesheet_id: ID.root,
    out_timesheet: {
      timesheet_id: ID.root,
      contract_id: ID.contract,
      version: 3,
      is_current: true,
      sheet_scope: 'WEEKLY',
      settings_authority_json: {
        values: { ...policy, resolved_at_utc: '2026-09-15T12:00:00Z' },
      },
    },
    out_cur_fin: currentFinancial,
  };
  const weeklyContext = {
    timesheet_id: ID.root,
    out_cw: { id: 'a1000000-0000-4000-8000-000000000008' },
    out_contract: {
      id: ID.contract,
      candidate_id: ID.candidate,
      client_id: ID.client,
      role: 'NURSE',
      band: 'BAND 5',
      pay_method_snapshot: 'PAYE',
    },
  };
  const calculatedSegment = {
    ...segment,
    segment_id: 'ts:calculated',
  };
  delete calculatedSegment.weekly_source;
  const calculation = {
    ok: true,
    snapshot: {
      timesheet_id: ID.root,
      pay_day: 10,
      pay_night: 11.5,
      pay_sat: 12,
      pay_sun: 13,
      pay_bh: 14,
      charge_day: 20,
      charge_night: 23,
      charge_sat: 24,
      charge_sun: 26,
      charge_bh: 28,
      invoice_breakdown_json: { mode: 'SEGMENTS', segments: [calculatedSegment] },
    },
  };
  return {
    rootContext,
    timesheetContext,
    weeklyContext,
    calculation,
    ...overrides,
  };
}

test('builds the exact closed service snapshot and preserves additional units, mileage and receipt expenses', () => {
  const snapshot = adaptWeeklyCorrectFinalServiceSnapshot(fixture());
  assert.equal(snapshot.schema_version, 'WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1');
  assert.equal(snapshot.calculator_owner, 'buildWeeklyScheduleSegmentsSnapshot');
  assert.deepEqual(snapshot.source_actual_schedule_json, actualSchedule());
  assert.deepEqual(snapshot.tsfin_snapshot_json.invoice_breakdown_json.segments, [expectedSegment()]);
  assert.deepEqual(snapshot.tsfin_snapshot_json.additional_units_json, { on_call: 2 });
  assert.equal(snapshot.tsfin_snapshot_json.additional_pay_ex_vat, 10);
  assert.equal(snapshot.tsfin_snapshot_json.mileage_pay_ex_vat, 5.4);
  assert.equal(snapshot.tsfin_snapshot_json.expenses_pay_ex_vat, 7.5);
  assert.deepEqual(snapshot.tsfin_snapshot_json.expenses_evidence_manifest, {
    schema_version: 'RECEIPT_EVIDENCE_V1', count: 1,
  });
  assert.equal(snapshot.tsfin_snapshot_json.total_pay_ex_vat, 152.9);
  assert.equal(snapshot.tsfin_snapshot_json.total_charge_ex_vat, 290);
  assert.equal(snapshot.tsfin_snapshot_json.margin_ex_vat, 117.78);
  assert.equal(Object.hasOwn(snapshot.tsfin_snapshot_json.policy_snapshot_json, 'resolved_at_utc'), false);
  assert.equal(Object.keys(snapshot.tsfin_snapshot_json).length, 48);
});

test('replaces ordinary receipt expenses only when the prepared source supplies source-approved expenses', () => {
  const input = fixture();
  input.rootContext.expected_source_expenses = [{
    expense_authority_generation_id: 'a1000000-0000-4000-8000-000000000009',
    source_expense_pence: '1234',
    candidate_reimbursement_ex_vat: '12.34',
    client_charge_ex_vat: '12.34',
  }];
  const snapshot = adaptWeeklyCorrectFinalServiceSnapshot(input).tsfin_snapshot_json;
  assert.equal(snapshot.expenses_pay_ex_vat, 12.34);
  assert.equal(snapshot.expenses_charge_ex_vat, 12.34);
  assert.equal(snapshot.expenses_description, 'Source-approved expenses');
  assert.equal(snapshot.expenses_evidence_r2_key, null);
  assert.deepEqual(snapshot.expenses_evidence_manifest, {
    schema_version: 'WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1',
    authorities: input.rootContext.expected_source_expenses,
    manifest_hash: HASH,
  });
});

test('correcting a prior source expense to zero removes only source-derived expense evidence', () => {
  const input = fixture();
  input.timesheetContext.out_cur_fin.expenses_pay_ex_vat = 12.34;
  input.timesheetContext.out_cur_fin.expenses_charge_ex_vat = 12.34;
  input.timesheetContext.out_cur_fin.expenses_description = 'Source-approved expenses';
  input.timesheetContext.out_cur_fin.expenses_evidence_r2_key = null;
  input.timesheetContext.out_cur_fin.expenses_evidence_manifest = {
    schema_version: 'WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1',
    authorities: [{ source_expense_pence: '1234' }],
    manifest_hash: 'b'.repeat(64),
  };
  const snapshot = adaptWeeklyCorrectFinalServiceSnapshot(input).tsfin_snapshot_json;
  assert.equal(snapshot.expenses_pay_ex_vat, 0);
  assert.equal(snapshot.expenses_charge_ex_vat, 0);
  assert.equal(snapshot.expenses_description, null);
  assert.equal(snapshot.expenses_evidence_manifest, null);
  assert.equal(snapshot.total_pay_ex_vat, 145.4);
});

test('a sealed zero expense authority is an audit tombstone, not Timesheet expense evidence', () => {
  const input = fixture();
  input.rootContext.expected_source_expenses = [{
    expense_authority_generation_id: 'a1000000-0000-4000-8000-000000000010',
    source_expense_pence: '0',
    candidate_reimbursement_ex_vat: '0.00',
    client_charge_ex_vat: '0.00',
  }];
  input.timesheetContext.out_cur_fin.expenses_pay_ex_vat = 12.34;
  input.timesheetContext.out_cur_fin.expenses_charge_ex_vat = 12.34;
  input.timesheetContext.out_cur_fin.expenses_description = 'Source-approved expenses';
  input.timesheetContext.out_cur_fin.expenses_evidence_manifest = {
    schema_version: 'WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1',
    authorities: [{ source_expense_pence: '1234' }],
    manifest_hash: 'b'.repeat(64),
  };
  const snapshot = adaptWeeklyCorrectFinalServiceSnapshot(input).tsfin_snapshot_json;
  assert.equal(snapshot.expenses_pay_ex_vat, 0);
  assert.equal(snapshot.expenses_charge_ex_vat, 0);
  assert.equal(snapshot.expenses_description, null);
  assert.equal(snapshot.expenses_evidence_manifest, null);
  assert.equal(snapshot.total_pay_ex_vat, 145.4);
});

test('changed or removed prepared source rows must be reproduced by the ordinary calculator', () => {
  const changed = fixture();
  changed.rootContext.expected_segments[0] = expectedSegment({
    end: '09:00', hours_night: 11, pay_amount: 141.5, charge_amount: 283,
  });
  assert.throws(
    () => adaptWeeklyCorrectFinalServiceSnapshot(changed),
    (error) => error instanceof WeeklyCorrectFinalSnapshotError
      && error.code === 'WEEKLY_SOURCE_CORRECTION_CALCULATION_MISMATCH',
  );

  const removed = fixture();
  removed.rootContext.expected_segments = [];
  removed.rootContext.expected_actual_schedule = [];
  assert.throws(
    () => adaptWeeklyCorrectFinalServiceSnapshot(removed),
    (error) => error instanceof WeeklyCorrectFinalSnapshotError
      && error.code === 'WEEKLY_SOURCE_CORRECTION_CALCULATION_MISMATCH',
  );

  removed.calculation.snapshot.invoice_breakdown_json.segments = [];
  const removedSnapshot = adaptWeeklyCorrectFinalServiceSnapshot(removed).tsfin_snapshot_json;
  assert.deepEqual(removedSnapshot.invoice_breakdown_json.segments, []);
  assert.equal(removedSnapshot.total_hours, 0);
  assert.equal(removedSnapshot.total_pay_ex_vat, 22.9);
  assert.equal(removedSnapshot.total_charge_ex_vat, 30);
  assert.equal(removedSnapshot.margin_ex_vat, 5.72);
});

test('identity drift and a guessed source-expense hash fail closed', () => {
  const identity = fixture();
  identity.weeklyContext.timesheet_id = 'a2000000-0000-4000-8000-000000000001';
  assert.throws(
    () => adaptWeeklyCorrectFinalServiceSnapshot(identity),
    (error) => error instanceof WeeklyCorrectFinalSnapshotError
      && error.code === 'WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID',
  );

  const expense = fixture();
  expense.rootContext.expected_source_expenses = [{
    source_expense_pence: '100',
    candidate_reimbursement_ex_vat: '1.00',
    client_charge_ex_vat: '1.00',
  }];
  delete expense.rootContext.expected_rate_source_refs.source_expense_manifest_hash;
  assert.throws(
    () => adaptWeeklyCorrectFinalServiceSnapshot(expense),
    (error) => error instanceof WeeklyCorrectFinalSnapshotError
      && error.code === 'WEEKLY_SOURCE_CORRECTION_CALCULATION_CONTEXT_INVALID',
  );
});

test('the adapter owns no Banking Pay, Workbench, Draft or invoice mutation', () => {
  assert.equal(WEEKLY_CORRECT_FINAL_SNAPSHOT_ADAPTER_CONTRACT.browserEconomicsAccepted, false);
  assert.deepEqual(WEEKLY_CORRECT_FINAL_SNAPSHOT_ADAPTER_CONTRACT.preserves, [
    'ADDITIONAL_UNITS', 'MILEAGE', 'NON_SOURCE_EXPENSES',
  ]);
  const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
  const source = fs.readFileSync(
    path.join(repositoryRoot, 'broker/src/weekly-source/correct-final-source-snapshot-adapter.mjs'),
    'utf8',
  );
  assert.equal(/\bsbRpc\b|pay_workbench|pay_batch|create_draft|invoice_lines/i.test(source), false);
});
