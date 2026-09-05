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

test('Timesheet Summary projects Manual and import-authoritative rows because they can own QR or expense-only workflows', async () => {
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
  assert.deepEqual(calls[0].map((identity) => identity.row_key), [
    'manual','manual-adjustment','nhsp','healthroster','electronic'
  ]);
  for (const row of result) {
    assert.equal(row.candidate_office_projection_loaded, true);
    assert.equal(row.candidate_office_projection?.ok, true);
    assert.equal(row.candidate_office_projection_error, null);
  }
});

test('Candidate Office applicability never lets a Manual/import label suppress a QR or expense-only possibility', () => {
  assert.equal(candidateOfficeSummaryInternals.candidateOfficeApplicability({
    route_type: 'DAILY_ELECTRONIC',
    route_display: 'Daily Electronic',
    route_family: 'MANUAL',
    submission_mode: 'ELECTRONIC'
  }), true);
  assert.equal(candidateOfficeSummaryInternals.candidateOfficeApplicability({
    route_type: 'WEEKLY_NHSP',
    route_family: 'ELECTRONIC',
    submission_mode: 'ELECTRONIC'
  }), true);
  assert.equal(candidateOfficeSummaryInternals.candidateOfficeApplicability({
    route_type: 'WEEKLY_MANUAL',
    route_family: 'QR',
    submission_mode: 'MANUAL'
  }), true);
  assert.equal(candidateOfficeSummaryInternals.candidateOfficeApplicability({
    route_type: 'WEEKLY_NHSP_ADJUSTMENT',
    route_family: 'MANUAL',
    submission_mode: 'MANUAL'
  }), null, 'the canonical projection must decide whether this is imported hours or an expense-only carrier');
});

test('Candidate Office leaves Manual and import rows unresolved until the canonical projection identifies their record role', () => {
  for (const routeType of ['DAILY_MANUAL', 'WEEKLY_MANUAL', 'WEEKLY_MANUAL_ADJUSTMENT', 'WEEKLY_NHSP', 'WEEKLY_HEALTHROSTER']) {
    const payload = candidateOfficeSummaryInternals.markCandidateOfficePayloadApplicability({
      row: { route_type: routeType },
      data_row: { route_type: routeType },
      effective: { route_type: routeType },
      timesheet: { route_type: routeType },
      contract_week: { route_type: routeType }
    });
    assert.equal(Object.hasOwn(payload, 'candidate_office_projection_not_applicable'), false, routeType);
    for (const key of ['row', 'data_row', 'effective', 'timesheet', 'contract_week']) {
      assert.equal(Object.hasOwn(payload[key], 'candidate_office_projection_not_applicable'), false, `${routeType}:${key}`);
    }
  }

  const electronic = candidateOfficeSummaryInternals.markCandidateOfficePayloadApplicability({
    timesheet: { route_type: 'DAILY_ELECTRONIC' },
    effective: { route_family: 'MANUAL' }
  });
  assert.equal(electronic.candidate_office_projection_not_applicable, false,
    'the exact DAILY ELECTRONIC route remains Candidate-applicable');

  const unknown = candidateOfficeSummaryInternals.markCandidateOfficePayloadApplicability({
    timesheet: { route_type: 'FUTURE_ROUTE' }
  });
  assert.equal(Object.hasOwn(unknown, 'candidate_office_projection_not_applicable'), false,
    'unknown routes remain fail-closed instead of being silently suppressed');
});

test('Candidate-owned paper waiting is shown as Processed while its exact Candidate status owns the outstanding action', () => {
  const active = {
    processing_status: 'AWAITING_MANUAL_SIGNATURE',
    processing_status_display: 'Processing Delayed',
    tools_stage: 'PROCESSING_DELAYED',
    summary_stage: 'PROCESSING_DELAYED',
    candidate_office_projection: {
      workflow: {
        route: 'PAPER', state: 'AWAITING_PAPER_RETURN', historical: false,
        is_current_action_workflow: true
      }
    }
  };
  candidateOfficeSummaryInternals.reconcileCandidateOwnedProcessingStatus(active);
  assert.equal(active.processing_status, 'AWAITING_MANUAL_SIGNATURE', 'the source fact remains available');
  assert.equal(active.processing_status_display, 'Processed');
  assert.equal(active.tools_stage, 'PROCESSED');
  assert.equal(active.summary_stage, 'PROCESSED');

  for (const untouched of [
    {
      processing_status: 'CLIENT_UNRESOLVED', processing_status_display: 'Processing Delayed',
      tools_stage: 'PROCESSING_DELAYED', candidate_office_projection: active.candidate_office_projection
    },
    {
      processing_status: 'AWAITING_MANUAL_SIGNATURE', processing_status_display: 'Processing Delayed',
      tools_stage: 'PROCESSING_DELAYED', candidate_office_projection: { workflow: null }
    }
  ]) {
    candidateOfficeSummaryInternals.reconcileCandidateOwnedProcessingStatus(untouched);
    assert.equal(untouched.processing_status_display, 'Processing Delayed');
    assert.equal(untouched.tools_stage, 'PROCESSING_DELAYED');
  }
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

test('targeted Summary patch exposes only the agreed visible state and current identity', () => {
  const projection = {
    current_identity: {
      row_key: 'row-1',
      timesheet_id: uuid(1),
      contract_week_id: uuid(2),
      row_signature: 'revision-22'
    },
    candidate_status: { code: 'FINALISED', label: 'Candidate Submission Complete', tone: 'success' }
  };
  const patch = candidateOfficeSummaryInternals.candidateTimesheetSummaryCompactPatch({
    timesheet_id: uuid(1), contract_week_id: uuid(2),
    candidate_name: 'must not be repeated', client_name: 'must not be repeated',
    route_type: 'DAILY_ELECTRONIC', route_display: 'Daily Electronic', route_family: 'ELECTRONIC', sheet_scope: 'DAILY',
    submission_mode: 'ELECTRONIC', submission_mode_snapshot: 'ELECTRONIC',
    processing_status: 'AUTHORISED', processing_status_display: 'Authorised for Invoicing',
    total_hours: 12.5, total_pay_ex_vat: 437.5, margin_ex_vat: 371.87,
    hidden_financial_payload: { should: 'never leave the Worker' },
    candidate_office_projection_loaded: true,
    candidate_office_projection: projection
  });

  assert.deepEqual(Object.keys(patch).sort(), [
    'backend_row_signature','candidate_office_projection','candidate_office_projection_error',
    'candidate_office_projection_loaded','candidate_office_projection_not_applicable','contract_week_id',
    'current_identity','expected_row_signature','id','margin_ex_vat','processing_status',
    'processing_status_display','route_display','route_family','route_type','row_signature','sheet_scope',
    'submission_mode','submission_mode_snapshot','timesheet_id','total_hours','total_pay_ex_vat'
  ].sort());
  assert.equal(patch.expected_row_signature, 'revision-22');
  assert.equal(patch.processing_status_display, 'Authorised for Invoicing');
  assert.equal(patch.total_hours, 12.5);
  assert.equal(Object.hasOwn(patch, 'candidate_name'), false);
  assert.equal(Object.hasOwn(patch, 'client_name'), false);
  assert.equal(Object.hasOwn(patch, 'hidden_financial_payload'), false);
});
