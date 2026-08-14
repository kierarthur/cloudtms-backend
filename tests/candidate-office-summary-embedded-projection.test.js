import assert from 'node:assert/strict';
import test from 'node:test';

import { candidateOfficeSummaryInternals } from '../broker/src/index.js';

const uuid = (number) => `00000000-0000-4000-8000-${String(number).padStart(12, '0')}`;

function projection(identity, code = 'CREATED') {
  return {
    ok: true,
    contract_version: 'OFFICE_CANDIDATE_TIMESHEET_V1',
    office_contract_version: 'CLOUDTMS_OFFICE_CANDIDATE_API_V1',
    current_identity: {
      row_key: identity.row_key,
      timesheet_id: identity.timesheet_id,
      contract_week_id: identity.contract_week_id,
      row_signature: identity.expected_row_signature,
      route_family: 'ELECTRONIC'
    },
    candidate_status: { code, label: code, tone: 'neutral' },
    workflow: null,
    manager_approval: null,
    paper_pack: { state: 'NOT_APPLICABLE' },
    rejections: [],
    primary_action: null,
    available_actions: [],
    diagnostics: [],
    refresh_hints: {},
    observed_at_utc: '2026-08-14T08:00:00Z'
  };
}

test('Timesheet Summary embeds bounded exact Candidate projections before returning rows', async () => {
  const rows = Array.from({ length: 101 }, (_, index) => ({
    id: index < 2 ? 'shared-display-key' : `row-${index}`,
    timesheet_id: uuid(index + 1),
    contract_week_id: index % 2 ? uuid(index + 1001) : null,
    backend_row_signature: `signature-${index}`
  }));
  rows.push({ id: 'planned-only', contract_week_id: uuid(5000) });
  const calls = [];
  const rpc = async (_env, functionName, args) => {
    calls.push({ functionName, args });
    return {
      ok: true,
      results: args.p_payload.identities.map((identity) => ({
        ok: true,
        correlation_key: identity.row_key,
        projection: projection(identity)
      }))
    };
  };

  const result = await candidateOfficeSummaryInternals.attachCandidateOfficeSummaryProjections(
    { CANDIDATE_APP_ENVIRONMENT: 'TEST' },uuid(9000),rows,rpc
  );

  assert.equal(calls.length, 2, '101 exact timesheet identities are partitioned into bounded batches');
  assert.ok(calls.every((call) => call.functionName === 'cloudtms_office_candidate_adapter_v1'));
  assert.ok(calls.every((call) => call.args.p_action === 'PROJECT_BATCH'));
  assert.ok(calls.every((call) => call.args.p_payload.surface === 'TIMESHEET_SUMMARY'));
  assert.ok(calls.every((call) => call.args.p_payload.identities.length <= 100));
  assert.ok(calls.every((call) => {
    const keys = call.args.p_payload.identities.map((identity) => identity.row_key);
    return new Set(keys).size === keys.length;
  }), 'duplicate display keys are never collapsed inside one batch');
  for (const row of result.slice(0, 101)) {
    assert.equal(row.candidate_office_projection_loaded, true);
    assert.equal(row.candidate_office_projection?.ok, true);
    assert.equal(row.candidate_office_projection_error, null);
  }
  assert.equal(result[101].candidate_office_projection_loaded, undefined,
    'a planned row without a concrete timesheet keeps the established no-projection boundary');
});

test('Timesheet Summary resolves Manual and import-authoritative routes as immediate blanks without projection work', async () => {
  const rows = [
    { id: 'manual', timesheet_id: uuid(1), route_type: 'WEEKLY_MANUAL', submission_mode: 'MANUAL' },
    { id: 'manual-adjustment', timesheet_id: uuid(2), route_display: 'Weekly Manual Adjustment' },
    { id: 'nhsp', timesheet_id: uuid(3), route_type: 'WEEKLY_NHSP', client_is_nhsp: true },
    { id: 'healthroster', timesheet_id: uuid(4), route_display: 'Weekly HealthRoster', client_autoprocess_hr: true },
    {
      id: 'electronic',timesheet_id: uuid(5),route_type: 'DAILY_ELECTRONIC',
      submission_mode: 'ELECTRONIC',client_autoprocess_hr: true,client_is_nhsp: true
    }
  ];
  const calls = [];
  const rpc = async (_env, _functionName, args) => {
    calls.push(args.p_payload.identities);
    return {
      ok: true,
      results: args.p_payload.identities.map((identity) => ({
        ok: true,correlation_key: identity.row_key,projection: projection(identity)
      }))
    };
  };

  const result = await candidateOfficeSummaryInternals.attachCandidateOfficeSummaryProjections(
    { CANDIDATE_APP_ENVIRONMENT: 'TEST' },uuid(9000),rows,rpc
  );

  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].map((identity) => identity.row_key), ['electronic']);
  for (const row of result.slice(0, 4)) {
    assert.equal(row.candidate_office_projection_loaded, true);
    assert.equal(row.candidate_office_projection_not_applicable, true);
    assert.equal(row.candidate_office_projection, null);
    assert.equal(row.candidate_office_projection_error, null);
  }
  assert.equal(result[4].candidate_office_projection?.ok, true);
});

test('Timesheet Summary embeds a safe row-level error and never starts browser hydration', async () => {
  const rows = [{ id: 'row-one', timesheet_id: uuid(1) }, { id: 'row-two', timesheet_id: uuid(2) }];
  const rpc = async (_env, _functionName, args) => ({
    ok: true,
    results: [
      { ok: true, correlation_key: 'row-one', projection: projection(args.p_payload.identities[0]) },
      { ok: false, correlation_key: 'row-two', error: { code: 'CANDIDATE_OFFICE_PROJECTION_NOT_FOUND', retryable: false, internal: 'not exposed' } }
    ]
  });
  const result = await candidateOfficeSummaryInternals.attachCandidateOfficeSummaryProjections(
    { CANDIDATE_APP_ENVIRONMENT: 'TEST' },uuid(9000),rows,rpc
  );
  assert.equal(result[0].candidate_office_projection?.ok, true);
  assert.deepEqual(result[1].candidate_office_projection_error, {
    code: 'CANDIDATE_OFFICE_PROJECTION_NOT_FOUND', retryable: false
  });
  assert.equal(result[1].candidate_office_projection, null);
});
