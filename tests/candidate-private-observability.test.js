import assert from 'node:assert/strict';
import test from 'node:test';

import { candidatePrivateWorkerInternals } from '../broker/src/candidate-private-worker.js';

test('private Candidate failure diagnostics expose only a closed error code and status', async () => {
  const result = await candidatePrivateWorkerInternals.privateFailureDiagnostic(Response.json({
    error_code: 'MANAGER_ROUTE_REGISTRATION_FAILED',
    details: { bearer: 'must-not-appear' }
  }, { status: 503 }));
  assert.deepEqual(result, {
    status: 503,
    error_code: 'MANAGER_ROUTE_REGISTRATION_FAILED'
  });
  assert.equal(JSON.stringify(result).includes('must-not-appear'), false);
});

test('private Candidate failure diagnostics reject unclosed values and ignore non-failures', async () => {
  assert.deepEqual(await candidatePrivateWorkerInternals.privateFailureDiagnostic(Response.json({
    error_code: 'unsafe value with spaces'
  }, { status: 500 })), {
    status: 500,
    error_code: 'CANDIDATE_PRIVATE_FAILURE_UNCLASSIFIED'
  });
  assert.equal(await candidatePrivateWorkerInternals.privateFailureDiagnostic(
    Response.json({ ok: true }, { status: 200 })
  ), null);
});

test('manager routing does not require Candidate identity-projection authority', () => {
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_FEDERATED_ROUTING_ENABLED: 'TRUE',
    CANDIDATE_AGENCY_ID: '00000000-0000-4000-8000-000000000001',
    CANDIDATE_DATA_PLANE_ID: '00000000-0000-4000-8000-000000000002',
    CANDIDATE_ROUTE_VERSION: '2',
    CANDIDATE_ROUTE_CONTEXT_SECRET: 'test-only-route-context-secret'
  };
  assert.equal(
    candidatePrivateWorkerInternals.federatedRouteConfigurationAvailable(env),
    true
  );
  assert.equal(candidatePrivateWorkerInternals.federatedConfigurationAvailable(env), false);
  assert.equal(candidatePrivateWorkerInternals.federatedConfigurationAvailable({
    ...env,
    CANDIDATE_FEDERATED_IDENTITY_SECRET: 'test-only-candidate-projection-secret'
  }), true);
});
