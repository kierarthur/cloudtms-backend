import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const backend = fs.readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const sql = fs.readFileSync(new URL('../supabase/repeatable/01082026_1622_contract_week_manual_draft_upsert_atomic_v1.sql', import.meta.url), 'utf8');
const evidenceSignatureCompatibility = fs.readFileSync(
  new URL('../supabase/repeatable/25082026_1529_candidate_signature_evidence_timestamp_compatibility_v1.sql', import.meta.url),
  'utf8'
);
const candidateRuntimeFixture = fs.readFileSync(
  new URL('./fixtures/07082026_2155_candidate_app_local_compile_base.sql', import.meta.url),
  'utf8'
);

const section = (startMarker, endMarker) => {
  const start = backend.indexOf(startMarker);
  assert.notEqual(start, -1, `missing ${startMarker}`);
  const end = backend.indexOf(endMarker, start + startMarker.length);
  assert.notEqual(end, -1, `missing ${endMarker}`);
  return backend.slice(start, end);
};

test('planned manual details returns a guarded contract-week authority and explicit process permission', () => {
  const source = section(
    'async function handleContractWeekManualDraftDetails',
    'async function handleTimesheetDetails'
  );
  assert.match(source, /timesheet_lifecycle_guard_signature_v1/);
  assert.match(source, /p_timesheet_id:\s*null/);
  assert.match(source, /p_contract_week_id:\s*cw\.id/);
  assert.match(source, /planned_contract_week_authority_complete:\s*true/);
  assert.match(source, /planned_contract_week_authority_contract_week_id:\s*cw\.id/);
  assert.match(source, /can_process:\s*canProcessPlannedManualWeek/);
});

test('planned lifecycle signature uses the real evidence timestamp contract', () => {
  assert.match(evidenceSignatureCompatibility, /'updated_at',\s*e\.created_at/i);
  assert.match(evidenceSignatureCompatibility, /order\s+by\s+e\.created_at\s*,\s*e\.id/i);
  assert.doesNotMatch(evidenceSignatureCompatibility, /e\.updated_at/i);

  const evidenceTable = candidateRuntimeFixture.slice(
    candidateRuntimeFixture.toLowerCase().indexOf('create table public.timesheet_evidence'),
    candidateRuntimeFixture.toLowerCase().indexOf('create table public.invoice_operations')
  );
  assert.match(evidenceTable, /created_at\s+timestamptz/i);
  assert.doesNotMatch(evidenceTable, /updated_at\s+timestamptz/i);
});

test('planned draft save is atomic and fails closed without the expected signature', () => {
  const source = section(
    'async function handleContractWeekManualDraftUpsert',
    'async function handleContractWeekManualAuthorise'
  );
  assert.match(source, /EXPECTED_ROW_SIGNATURE_REQUIRED/);
  assert.match(source, /contract_week_manual_draft_upsert_atomic_v1/);
  assert.match(source, /p_expected_row_signature:\s*expectedRowSignature/);
  assert.doesNotMatch(source, /method:\s*'PATCH'[\s\S]{0,250}contract_weeks/);
});

test('planned Process requires a signature while physical-timesheet guards remain separate', () => {
  const source = section(
    'async function handleContractWeekManualUpsert',
    'async function handleContractWeeksList'
  );
  assert.match(source, /!currentTimesheetIdForWeek\s*&&\s*!expectedRowSignature/);
  assert.match(source, /guard_fail_missing_planned_row_signature/);
  assert.match(source, /if \(currentTimesheetIdForWeek\)/);
  assert.match(source, /EXPECTED_TIMESHEET_ID_REQUIRED/);
});

test('atomic planned draft RPC locks, compares, updates, and returns the next signature', () => {
  assert.match(sql, /FOR UPDATE/);
  assert.match(sql, /v_week\.timesheet_id IS NOT NULL/);
  assert.match(sql, /ROW_SIGNATURE_MISMATCH/);
  assert.match(sql, /timesheet_lifecycle_guard_signature_v1\([\s\S]*NULL::uuid,[\s\S]*v_week\.id/);
  assert.match(sql, /WHERE cw\.id = v_week\.id[\s\S]*AND cw\.timesheet_id IS NULL/);
  assert.match(sql, /planned_contract_week_authority_complete', true/);
  assert.match(sql, /REVOKE ALL ON FUNCTION[\s\S]*FROM authenticated/);
  assert.match(sql, /GRANT EXECUTE ON FUNCTION[\s\S]*TO service_role/);
});
