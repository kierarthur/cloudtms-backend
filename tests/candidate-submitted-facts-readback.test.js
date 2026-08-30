import assert from 'node:assert/strict';
import test from 'node:test';

import { candidateAppBackendInternals, handleCandidateAppRequest } from '../broker/src/candidate-app-backend.js';

const { candidateSubmittedFactsProjection, currentSubmittedDisplayWorkflow } = candidateAppBackendInternals;
const workflowId = '00000000-0000-4000-8000-000000000101';

function detail(state = 'READY_FOR_MANAGER_APPROVAL') {
  return {
    ok: true,
    hours: { total_hours: 0, actual_schedule_json: [] },
    expenses: {
      expenses_pay_ex_vat: 0, expenses_description: null, mileage_units: 0,
      mileage_pay_ex_vat: 0, travel_pay_ex_vat: 0,
      accommodation_pay_ex_vat: 0, other_pay_ex_vat: 0
    },
    manager_review: { workflow_id: workflowId, workflow_generation: 2 },
    workflows: [{
      workflow_id: workflowId, workflow_kind: 'CONTRACT_COMBINED', state,
      generation: 2, detail_action_owner: true, updated_at_utc: '2026-08-29T23:08:21Z'
    }]
  };
}

function workflow(state = 'READY_FOR_MANAGER_APPROVAL') {
  return {
    id: workflowId,
    workflow_kind: 'CONTRACT_COMBINED',
    state,
    immutable_submission_json: {
      hours_submission: {
        actual_schedule_json: [
          { date: '2026-07-20', start_time: '08:00', end_time: '14:30', break_minutes: 30 },
          { date: '2026-07-21', start_time: '20:00', end_time: '02:30', break_minutes: 30 }
        ]
      },
      expense_submission: {
        accommodation_amount: 20,
        travel_amount: 5.5,
        other_amount: 2,
        mileage_units: 18,
        description: 'Candidate-submitted expenses'
      }
    }
  };
}

test('selects the exact current submitted workflow used by the manager-review journey', () => {
  const current = detail();
  current.workflows.unshift({
    workflow_id: '00000000-0000-4000-8000-000000000102',
    workflow_kind: 'CONTRACT_HOURS', state: 'REJECTED', generation: 1,
    detail_action_owner: false, updated_at_utc: '2026-08-29T22:00:00Z'
  });
  assert.equal(currentSubmittedDisplayWorkflow(current)?.workflow_id, workflowId);
});

test('reads submitted hours and expense facts while canonical finance remains pending', () => {
  const projected = candidateSubmittedFactsProjection(detail(), workflow());
  assert.equal(projected.hours.total_hours, 12);
  assert.equal(projected.hours.actual_schedule_json.length, 2);
  assert.deepEqual(projected.expenses, {
    expenses_pay_ex_vat: 27.5,
    expenses_description: 'Candidate-submitted expenses',
    mileage_units: 18,
    mileage_pay_ex_vat: 0,
    travel_pay_ex_vat: 5.5,
    accommodation_pay_ex_vat: 20,
    other_pay_ex_vat: 2
  });
  assert.equal(detail().hours.total_hours, 0, 'the authoritative RPC value is not mutated');
});

test('does not overlay rejected, cancelled or finalised workflows', () => {
  for (const state of ['REJECTED', 'CANCELLED', 'FINALISED']) {
    const source = detail(state);
    const projected = candidateSubmittedFactsProjection(source, workflow(state));
    assert.equal(projected, source);
    assert.equal(currentSubmittedDisplayWorkflow(source), null);
  }
});

test('does not invent mileage pay before the financial authority resolves it', () => {
  const source = detail();
  source.expenses.mileage_pay_ex_vat = 4.5;
  const projected = candidateSubmittedFactsProjection(source, workflow());
  assert.equal(projected.expenses.mileage_units, 18);
  assert.equal(projected.expenses.mileage_pay_ex_vat, 4.5);
});

test('the detail route loads immutable facts through the signed-in Candidate and exact generation only', async () => {
  const sessionId = '00000000-0000-4000-8000-000000000111';
  const candidateId = '00000000-0000-4000-8000-000000000112';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-secret-material',
    SUPABASE_URL: 'https://test.example.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const session = {
    session_id: sessionId, id: sessionId,
    account_id: '00000000-0000-4000-8000-000000000113',
    selected_candidate_id: candidateId,
    environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const token = await candidateAppBackendInternals.createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  const reads = [];
  globalThis.fetch = async (input) => {
    const url = String(input);
    if (url.includes('/candidate_app_sessions?')) return Response.json([session]);
    if (url.includes('/candidate_submission_workflows?')) {
      reads.push(url);
      return Response.json([{
        ...workflow(), candidate_id: candidateId, environment: 'TEST', generation: 2,
        immutable_submission_sha256: 'a'.repeat(64)
      }]);
    }
    throw new Error(`unexpected read ${url}`);
  };
  try {
    const response = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflowId}/timesheet-detail`,
      { headers: { authorization: `Bearer ${token}` } }
    ), env, {}, {
      routeAudience: 'PRIVATE',
      async rpc(name) {
        assert.equal(name, 'candidate_app_timesheet_detail_v2');
        return detail();
      }
    });
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.hours.total_hours, 12);
    assert.equal(body.expenses.accommodation_pay_ex_vat, 20);
    assert.equal(reads.length, 1);
    assert.match(reads[0], new RegExp(`candidate_id=eq\\.${candidateId}`));
    assert.match(reads[0], /generation=eq\.2/);
    assert.match(reads[0], /state=eq\.READY_FOR_MANAGER_APPROVAL/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
