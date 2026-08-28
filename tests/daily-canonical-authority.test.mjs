import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import {
  buildCanonicalDailyFinancialSnapshot,
  deriveCanonicalDailyFinancialContext
} from '../broker/src/daily-canonical-authority.js';
import {
  buildCandidateDailyPatchFromFrozenInput,
  mapCanonicalDailyScheduleToIso
} from '../broker/src/daily-schedule-authority.js';
import {
  detectDailyRosterFormatFromRows,
  normalizeNhspDailyBreakMinutes,
  resolveNhspActualColumnIndexes
} from '../broker/src/daily-roster-compat.js';

const nhspDailyRows = [
  ['Timesheets Previously Released'],
  ['Date', 'Ref', 'Staff', 'Unique ID', 'Trust', 'Ward', 'Assignment', 'Contract', '', '', '', 'Actual', '', '', ''],
  ['', '', '', '', '', '', '', 'Start', 'End', 'Break (Minutes)', 'Total', 'Start', 'End', 'Break (Minutes)', 'Total'],
  ['22/08/2026', 'REF-1', 'Example Worker', 'N-1', 'Source Trust', 'Ward A', 'RN', '07:00', '15:00', 60, 7, '08:00', '16:30', 30, 8]
];

const baseSnapshotInput = (overrides = {}) => ({
  env: {},
  timesheet: {
    timesheet_id: '00000000-0000-4000-8000-000000000001',
    version: 1,
    worked_start_iso: '2026-08-08T08:00:00.000Z',
    worked_end_iso: '2026-08-08T18:00:00.000Z',
    break_start_iso: null,
    break_end_iso: null,
    break_minutes: 0,
    authorised_at_server: null
  },
  currentFinancials: { processing_status: 'UNPROCESSED' },
  policy: { apply_erni_to: 'PAYE_ONLY', erni_pct: 13.8 },
  candidate: { pay_method: 'PAYE' },
  candidateId: '00000000-0000-4000-8000-000000000002',
  candidateAssignment: 'ASSIGNED',
  clientId: '00000000-0000-4000-8000-000000000003',
  role: 'NURSE',
  band: 'Band 5',
  ratesRow: {
    pay_day: 10, charge_day: 20,
    pay_night: 11, charge_night: 21,
    pay_sat: 12, charge_sat: 22,
    pay_sun: 13, charge_sun: 23,
    pay_bh: 14, charge_bh: 24,
    source_kind: 'DEFAULT'
  },
  preserveUnprocessed: true,
  subtractBreak(segments, start, end, minutes) {
    assert.equal(start, null);
    assert.equal(end, null);
    assert.equal(minutes, 0, 'explicit no-break must remain zero');
    return segments;
  },
  classifyMinutes() {
    return { hours_day: 10, hours_night: 0, hours_sat: 0, hours_sun: 0, hours_bh: 0 };
  },
  resolveEffectivePayChannel() { return { ok: true }; },
  ...overrides
});

const round2 = (value) => Math.round((Number(value) || 0) * 100) / 100;

// Frozen reference for the economic arithmetic in the pre-extraction DAILY
// branch of runTsfinWorkerOnce. Keeping this in tests proves that moving the
// body into the shared module does not change buckets, rates, pay, charge,
// ERNI/pay cost, margin or invoice totals.
const legacyWorkerEconomicReference = ({ hours, rates, policy, candidatePayMethod, timesheetPayMethod = null }) => {
  const payMethodUpper = String(candidatePayMethod || '').toUpperCase();
  const payMethod = payMethodUpper === 'PAYE'
    ? 'PAYE'
    : payMethodUpper === 'UMBRELLA'
      ? 'UMBRELLA'
      : timesheetPayMethod;
  const pay = {
    day: rates.pay_day ?? null,
    night: rates.pay_night ?? null,
    sat: rates.pay_sat ?? null,
    sun: rates.pay_sun ?? null,
    bh: rates.pay_bh ?? null
  };
  const charge = {
    day: rates.charge_day ?? null,
    night: rates.charge_night ?? null,
    sat: rates.charge_sat ?? null,
    sun: rates.charge_sun ?? null,
    bh: rates.charge_bh ?? null
  };
  const number = (value) => value == null ? 0 : Number(value) || 0;
  const totalPay = round2(
    hours.hours_day * number(pay.day) +
    hours.hours_night * number(pay.night) +
    hours.hours_sat * number(pay.sat) +
    hours.hours_sun * number(pay.sun) +
    hours.hours_bh * number(pay.bh)
  );
  const totalCharge = round2(
    hours.hours_day * number(charge.day) +
    hours.hours_night * number(charge.night) +
    hours.hours_sat * number(charge.sat) +
    hours.hours_sun * number(charge.sun) +
    hours.hours_bh * number(charge.bh)
  );
  const applyTo = String(policy?.apply_erni_to || 'PAYE_ONLY').toUpperCase();
  const rawErni = Number(policy?.erni_pct ?? 0);
  const erni = Number.isFinite(rawErni) && rawErni > 0 ? (rawErni > 1 ? rawErni / 100 : rawErni) : 0;
  const erniApplies = applyTo === 'ALL' || (applyTo === 'PAYE_ONLY' && String(payMethod || '').toUpperCase() === 'PAYE');
  const payCost = round2(erniApplies ? totalPay * (1 + erni) : totalPay);
  return {
    payMethod,
    totalHours: round2(hours.hours_day + hours.hours_night + hours.hours_sat + hours.hours_sun + hours.hours_bh),
    totalPay,
    totalCharge,
    margin: round2(totalCharge - payCost),
    pay,
    charge
  };
};

test('canonical DAILY owner preserves explicit no-break and derives all economics', () => {
  const snapshot = buildCanonicalDailyFinancialSnapshot(baseSnapshotInput());
  assert.equal(snapshot.break_minutes, 0);
  assert.equal(snapshot.total_hours, 10);
  assert.equal(snapshot.total_pay_ex_vat, 100);
  assert.equal(snapshot.total_charge_ex_vat, 200);
  assert.equal(snapshot.margin_ex_vat, 86.2);
  assert.equal(snapshot.processing_status, 'UNPROCESSED');
  assert.equal(snapshot.invoice_breakdown_json.totals.margin_ex_vat, 86.2);
});

test('Daily receipt retains worked hours before Office Client, Candidate or rate resolution', () => {
  const cases = [
    { name: 'unresolved Client', overrides: { clientId: null }, expected: 'CLIENT_UNRESOLVED' },
    { name: 'unassigned Candidate', overrides: { candidateId: null, candidateAssignment: 'UNASSIGNED' }, expected: 'UNASSIGNED' },
    { name: 'missing rates awaiting processing', overrides: {}, expected: 'UNPROCESSED' },
    { name: 'missing rates during canonical processing', overrides: { preserveUnprocessed: false }, expected: 'RATE_MISSING' }
  ];
  for (const entry of cases) {
    for (const managerApproved of [false, true]) {
      const input = baseSnapshotInput({ ratesRow: null, ...entry.overrides });
      input.timesheet.authorised_at_server = managerApproved ? '2026-08-08T18:30:00.000Z' : null;
      const snapshot = buildCanonicalDailyFinancialSnapshot(input);
      assert.equal(snapshot.processing_status, entry.expected, entry.name);
      assert.equal(snapshot.total_hours, 10, `${entry.name}: factual hours are retained`);
      assert.equal(snapshot.pay_day, null, `${entry.name}: absent pay is not an approved zero rate`);
      assert.equal(snapshot.charge_day, null, `${entry.name}: absent charge is not an approved zero rate`);
      assert.equal(snapshot.client_id, input.clientId);
      assert.equal(snapshot.candidate_id, input.candidateId);
      if (entry.expected === 'RATE_MISSING' || entry.expected === 'UNPROCESSED') {
        assert.equal(snapshot.has_rate_issue, true);
      }
    }
  }
});

test('Daily rate context does not invent Client or role matches and does not use ward as a rate key', () => {
  const input = {
    effectiveTimesheetId: '00000000-0000-4000-8000-000000000001',
    context: {
      out_timesheet: { job_title_norm: 'rmn', band: '6', worked_start_iso: '2026-08-28T06:30:00Z', ward_norm: 'Unmapped ward' },
      out_candidate: { id: '00000000-0000-4000-8000-000000000002', roles: ['RMN'], pay_method: 'PAYE' },
      out_client_id: null
    },
    toLocalParts: () => ({ ymd: '2026-08-28' }),
    parseCandidateRoleCodes: (roles) => roles,
    normaliseRole: (role) => role.trim().toUpperCase(),
    normaliseBand: (band) => `Band ${band}`
  };
  const unresolved = deriveCanonicalDailyFinancialContext(input);
  assert.equal(unresolved.clientId, null);
  assert.equal(unresolved.roleForRates, 'RMN');
  assert.deepEqual(unresolved.rateRequest, {
    k: input.effectiveTimesheetId,
    candidate_id: input.context.out_candidate.id,
    client_id: null,
    role: 'RMN',
    band: 'Band 6',
    date: '2026-08-28',
    rate_type: 'PAYE'
  });
  const roleMismatch = deriveCanonicalDailyFinancialContext({
    ...input,
    context: { ...input.context, out_client_id: '00000000-0000-4000-8000-000000000003' },
    timesheetOverride: { job_title_norm: 'HCA', ward_norm: 'Different ward' }
  });
  assert.equal(roleMismatch.roleForRates, null, 'do not substitute a different Candidate role');
  assert.equal(roleMismatch.rateRequest.role, null);
  assert.equal(roleMismatch.clientId, '00000000-0000-4000-8000-000000000003');
  assert.equal(Object.hasOwn(roleMismatch.rateRequest, 'ward'), false);
  assert.equal(Object.hasOwn(roleMismatch.rateRequest, 'ward_norm'), false);
});

test('extracted DAILY core is economically identical to the pre-extraction TSFIN worker arithmetic', () => {
  const cases = [
    {
      name: 'PAYE day with percentage ERNI',
      hours: { hours_day: 10, hours_night: 0, hours_sat: 0, hours_sun: 0, hours_bh: 0 },
      policy: { apply_erni_to: 'PAYE_ONLY', erni_pct: 13.8 },
      payMethod: 'PAYE'
    },
    {
      name: 'Umbrella night without PAYE-only ERNI',
      hours: { hours_day: 0, hours_night: 7.5, hours_sat: 0, hours_sun: 0, hours_bh: 0 },
      policy: { apply_erni_to: 'PAYE_ONLY', erni_pct: 13.8 },
      payMethod: 'UMBRELLA'
    },
    {
      name: 'mixed weekend and bank-holiday with fractional ALL ERNI',
      hours: { hours_day: 1.25, hours_night: 2.5, hours_sat: 3, hours_sun: 4, hours_bh: 5 },
      policy: { apply_erni_to: 'ALL', erni_pct: 0.1 },
      payMethod: 'UMBRELLA'
    },
    {
      name: 'fallback factual timesheet pay method',
      hours: { hours_day: 2, hours_night: 1, hours_sat: 0, hours_sun: 0, hours_bh: 0 },
      policy: { apply_erni_to: 'PAYE_ONLY', erni_pct: 13.8 },
      payMethod: '',
      timesheetPayMethod: 'PAYE'
    }
  ];
  for (const scenario of cases) {
    const rates = baseSnapshotInput().ratesRow;
    const expected = legacyWorkerEconomicReference({
      hours: scenario.hours,
      rates,
      policy: scenario.policy,
      candidatePayMethod: scenario.payMethod,
      timesheetPayMethod: scenario.timesheetPayMethod || null
    });
    const snapshot = buildCanonicalDailyFinancialSnapshot(baseSnapshotInput({
      timesheet: {
        ...baseSnapshotInput().timesheet,
        pay_method: scenario.timesheetPayMethod || null
      },
      policy: scenario.policy,
      candidate: { pay_method: scenario.payMethod },
      preserveUnprocessed: false,
      subtractBreak(segments) { return segments; },
      classifyMinutes() { return scenario.hours; }
    }));
    assert.equal(snapshot.pay_method, expected.payMethod, `${scenario.name}: pay method`);
    assert.equal(snapshot.total_hours, expected.totalHours, `${scenario.name}: total hours`);
    assert.equal(snapshot.total_pay_ex_vat, expected.totalPay, `${scenario.name}: pay`);
    assert.equal(snapshot.total_charge_ex_vat, expected.totalCharge, `${scenario.name}: charge`);
    assert.equal(snapshot.margin_ex_vat, expected.margin, `${scenario.name}: margin`);
    assert.deepEqual(snapshot.invoice_breakdown_json.base_hours.pay_rates, expected.pay, `${scenario.name}: pay rates`);
    assert.deepEqual(snapshot.invoice_breakdown_json.base_hours.charge_rates, expected.charge, `${scenario.name}: charge rates`);
    assert.deepEqual(snapshot.invoice_breakdown_json.totals, {
      total_pay_ex_vat: expected.totalPay,
      total_charge_ex_vat: expected.totalCharge,
      margin_ex_vat: expected.margin
    }, `${scenario.name}: invoice totals`);
  }
});

test('frozen Candidate DAILY input accepts explicit no-break and rejects ambiguous positive break', () => {
  const contract = {
    contract_version: 'CANDIDATE_DAILY_CANONICAL_SAVE_V1',
    timesheet_patch_json: {
      worked_start_iso: '2026-08-08T08:00:00.000Z',
      worked_end_iso: '2026-08-08T18:00:00.000Z',
      break_start_iso: null,
      break_end_iso: null,
      break_minutes: 0,
      actual_schedule_json: [{ date: '2026-08-08', start: '09:00', end: '19:00', break_minutes: 0 }]
    }
  };
  const patch = buildCandidateDailyPatchFromFrozenInput(contract);
  assert.equal(patch.break_minutes, 0);
  assert.equal(patch.break_start_iso, null);
  assert.equal(patch.break_end_iso, null);
  assert.equal(patch.worked_minutes, 600);
  assert.throws(
    () => buildCandidateDailyPatchFromFrozenInput({
      ...contract,
      timesheet_patch_json: { ...contract.timesheet_patch_json, break_minutes: 30 }
    }),
    /CANDIDATE_DAILY_BREAK_TIMES_REQUIRED/
  );
});

test('canonical local schedule mapping records no-break without inventing times', () => {
  const ukLocalToUtcISO = (ymd, hhmm) => `${ymd}T${hhmm}:00.000Z`;
  const mapped = mapCanonicalDailyScheduleToIso({
    date: '2026-08-08', start: '08:00', end: '18:00',
    break_start: '', break_end: '', break_minutes: 0
  }, { ukLocalToUtcISO });
  assert.equal(mapped.error, undefined);
  assert.equal(mapped.break_minutes, 0);
  assert.equal(mapped.break_start_iso, null);
  assert.equal(mapped.break_end_iso, null);
  assert.equal(mapped.normalized_schedule_json.no_break, true);
  assert.equal(mapped.net_worked_minutes, 600);
});

test('Daily format detection maps all four NHSP values from the Actual group', () => {
  const detected = detectDailyRosterFormatFromRows(nhspDailyRows);
  assert.equal(detected.format, 'NHSP');
  assert.deepEqual(detected.matches, ['NHSP']);
  const actual = resolveNhspActualColumnIndexes(nhspDailyRows);
  assert.deepEqual(actual, {
    start: 11,
    end: 12,
    break: 13,
    total: 14,
    groupIndex: 1,
    subIndex: 2
  });
  assert.deepEqual(actual, resolveNhspActualColumnIndexes(nhspDailyRows));
  assert.equal(nhspDailyRows[3][actual.start], '08:00');
  assert.equal(nhspDailyRows[3][actual.end], '16:30');
  assert.equal(nhspDailyRows[3][actual.break], 30);
  assert.equal(nhspDailyRows[3][actual.total], 8);
});

test('Daily detection is content-based, strict, and independent of filenames', () => {
  const healthRosterRows = [[
    'Request ID', 'Date', 'Staff Name', 'Unit', 'Grade', 'Start Time', 'End Time', 'Actual Break', 'Actual Hours'
  ]];
  assert.equal(detectDailyRosterFormatFromRows(healthRosterRows).format, 'HEALTHROSTER');
  assert.equal(
    detectDailyRosterFormatFromRows([...nhspDailyRows, healthRosterRows[0]]).errorCode,
    'DAILY_ROSTER_FORMAT_AMBIGUOUS'
  );
  assert.equal(
    detectDailyRosterFormatFromRows([['anything else']]).errorCode,
    'DAILY_ROSTER_FORMAT_UNSUPPORTED'
  );
});

test('NHSP Daily blank break is explicit zero and malformed Actual break fails closed', () => {
  assert.equal(normalizeNhspDailyBreakMinutes(''), 0);
  assert.equal(normalizeNhspDailyBreakMinutes(null), 0);
  assert.equal(normalizeNhspDailyBreakMinutes('45'), 45);
  assert.throws(() => normalizeNhspDailyBreakMinutes('not minutes'), /NHSP_DAILY_BREAK_INVALID/);
  assert.throws(() => normalizeNhspDailyBreakMinutes(-1), /NHSP_DAILY_BREAK_INVALID/);
});

test('Daily compatibility SQL keeps bounds, revoked-row exclusion, replay, and break inheritance fail closed', async () => {
  const [compatibilitySql, authoritySql, migrationSql, indexSource] = await Promise.all([
    readFile(new URL('../supabase/repeatable/22082026_1606_daily_validation_compatibility_v1.sql', import.meta.url), 'utf8'),
    readFile(new URL('../supabase/repeatable/22082026_1706_daily_validation_compatibility_authorities_v1.sql', import.meta.url), 'utf8'),
    readFile(new URL('../supabase/migrations/22082026_1551_timesheet_break_entry_mode.sql', import.meta.url), 'utf8'),
    readFile(new URL('../broker/src/index.js', import.meta.url), 'utf8')
  ]);
  assert.match(compatibilitySql, /p_coverage_end_date-p_coverage_start_date>365/);
  assert.match(compatibilitySql, /_import_review_effective_authority_core_v1\('HR_DAILY',null,p_client_id,v_today\)/);
  assert.match(compatibilitySql, /v_target_count,0\)>500/);
  assert.match(compatibilitySql, /pg_advisory_xact_lock/);
  assert.match(compatibilitySql, /coverage_operation_key=v_operation_key for update/);
  assert.match(compatibilitySql, /overrideclientsettings,false[\s\S]*timesheet_break_entry_mode is not null/);
  assert.match(compatibilitySql, /CONTRACT_CLIENT_MISMATCH/);
  assert.match(compatibilitySql, /effective_from is null or cs\.effective_from<=v_as_of/);
  assert.ok(
    (authoritySql.match(/ts\.is_current and ts\.revoked_at is null/g) || []).length >= 3,
    'all Daily missing-source branches must exclude revoked current versions'
  );
  assert.match(migrationSql, /START_END_TIMES/);
  assert.match(migrationSql, /DURATION_MINUTES/);
  assert.match(indexSource, /daily_client_id: clientId/);
  assert.match(indexSource, /agency_raw: dailyClientIdNorm \? 'HEALTHROSTER_DAILY' : 'NHSP'/);
  assert.match(indexSource, /p_as_of_date: null/);
});

test('structural single-owner and QR convergence invariants are present', async () => {
  const [indexSource, calculatorSource, transitionSource, finaliseSource, qrSource] = await Promise.all([
    readFile(new URL('../broker/src/index.js', import.meta.url), 'utf8'),
    readFile(new URL('../broker/src/daily-canonical-authority.js', import.meta.url), 'utf8'),
    readFile(new URL('../supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql', import.meta.url), 'utf8'),
    readFile(new URL('../supabase/repeatable/07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql', import.meta.url), 'utf8'),
    readFile(new URL('../supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql', import.meta.url), 'utf8')
  ]);
  assert.equal((calculatorSource.match(/export function buildCanonicalDailyFinancialSnapshot/g) || []).length, 1);
  assert.doesNotMatch(indexSource, /function\s+buildDailySnapshot\s*\(/);
  assert.doesNotMatch(indexSource, /const\s+rotateTimesheetVersion\s*=/);
  assert.match(indexSource, /handleTimesheetCreateManualDaily[\s\S]*?saveAndRecalculateCanonicalDaily/);
  assert.match(indexSource, /handleTimesheetDailyManualUpsert[\s\S]*?saveAndRecalculateCanonicalDaily/);
  const additionalStart = indexSource.indexOf('async function handleTimesheetCreateAdditionalDailyManual');
  const additionalEnd = indexSource.indexOf('\nasync function ', additionalStart + 20);
  const additionalSource = indexSource.slice(additionalStart, additionalEnd === -1 ? indexSource.length : additionalEnd);
  assert.match(additionalSource, /saveAndRecalculateCanonicalDaily/);
  assert.match(additionalSource, /Idempotency-Key|idempotency_key/);
  assert.match(additionalSource, /ADDITIONAL_DAILY_CLEANUP_INCOMPLETE/);
  assert.match(additionalSource, /ts_financials_outbox[\s\S]*timesheets_financials[\s\S]*timesheets/);
  assert.doesNotMatch(additionalSource, /writeSnapshot|buildCanonicalDailyFinancialSnapshot|rate_source_refs_json|invoice_breakdown_json/);
  assert.match(indexSource, /runTsfinWorkerOnce[\s\S]*?buildCanonicalDailyFinancialSnapshot/);
  assert.match(indexSource, /runTsfinWorkerOnce[\s\S]*?rpcTsfinResolveRatesBatch[\s\S]*?buildCanonicalDailyFinancialSnapshot/);
  assert.match(indexSource, /buildCanonicalDailyFinancialSnapshot\(\{[\s\S]*?subtractBreak,[\s\S]*?classifyMinutes,[\s\S]*?resolveEffectivePayChannel/);
  assert.match(indexSource, /buildCandidateDailyAtomicMaterialisation[\s\S]*?buildCanonicalDailyFinancialSnapshot/);
  assert.match(indexSource, /finaliseCandidateDailyThroughCanonicalAuthority[\s\S]*?p_daily_materialisation_json/);
  assert.match(transitionSource, /BEGIN_CANONICAL_DAILY_SAVE/);
  assert.doesNotMatch(transitionSource, /REGISTER_CANONICAL_DAILY_SAVE/);
  assert.match(finaliseSource, /_candidate_daily_save_recalculate_atomic_v1/);
  assert.match(finaliseSource, /_candidate_daily_context_contract_v1/);
  assert.match(finaliseSource, /CANDIDATE_DAILY_LOCKED_CONTEXT_STALE/);
  assert.match(indexSource, /CANDIDATE_DAILY_ATOMIC_FINALISATION_V2/);
  assert.match(indexSource, /canonical_context_sha256_hex/);
  assert.match(finaliseSource, /expected_pre_save_row_signature[\s\S]*CANDIDATE_DAILY_CANONICAL_SAVE_STALE[\s\S]*update public\.timesheets/);
  assert.match(finaliseSource, /timesheet_daily_manual_process_atomic/);
  assert.match(finaliseSource, /timesheet_authorise_generic_atomic/);
  assert.doesNotMatch(qrSource, /select\s+\(v_new_row\)\.\*/i);
  assert.match(qrSource, /explicit route-version field matrix/i);
  assert.match(qrSource, /revoked_at\s*=\s*v_now/i);
  assert.match(qrSource, /TIMESHEET_REVERT_CONTENT_MISMATCH/i);
  assert.match(qrSource, /Unauthorise it before changing its submission route/i);
  assert.match(qrSource, /TIMESHEET_REVERT_FINANCIAL_CONTENT_MISMATCH/i);
  assert.match(qrSource, /'INVALIDATE_QR'/);
  assert.match(qrSource, /'REISSUE_QR'/);
  assert.match(qrSource, /'DISABLE_QR'/);
});
