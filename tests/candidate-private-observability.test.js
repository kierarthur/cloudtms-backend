import assert from 'node:assert/strict';
import test from 'node:test';

import { candidatePrivateWorkerInternals } from '../broker/src/candidate-private-worker.js';
import { candidateAppBackendInternals } from '../broker/src/candidate-app-backend.js';

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

test('private Candidate failure diagnostics safely classify closed client failures', async () => {
  const result = await candidatePrivateWorkerInternals.privateFailureDiagnostic(Response.json({
    error_code: 'CANDIDATE_FEATURE_DISABLED',
    private_details: { candidate_email: 'must-not-appear' }
  }, { status: 400 }));
  assert.deepEqual(result, {
    status: 400,
    error_code: 'CANDIDATE_FEATURE_DISABLED'
  });
  assert.equal(JSON.stringify(result).includes('must-not-appear'), false);
});

test('federated projection diagnostics retain only the closed Candidate error code', () => {
  const error = new Error('RPC candidate_app_federated_session_project_v1 failed 409: private payload omitted');
  error.json = {
    message: 'CANDIDATE_FEDERATED_SESSION_STALE',
    detail: { email: 'must-not-appear' }
  };
  error.body = 'must-not-appear';
  const result = candidatePrivateWorkerInternals.federatedProjectionFailureDiagnostic(error);
  assert.deepEqual(result, { error_code: 'CANDIDATE_FEDERATED_SESSION_STALE' });
  assert.equal(JSON.stringify(result).includes('must-not-appear'), false);
});

test('federated projection diagnostics fail closed without copying transport text', () => {
  const error = new Error('dependency failed for must-not-appear');
  const result = candidatePrivateWorkerInternals.federatedProjectionFailureDiagnostic(error);
  assert.deepEqual(result, { error_code: 'CANDIDATE_FEDERATED_PROJECTION_FAILED' });
  assert.equal(JSON.stringify(result).includes('must-not-appear'), false);
});

test('Candidate transport diagnostics retain safe gateway status without response contents', () => {
  const error = new Error('RPC candidate_workflow_transition_atomic_v1 failed 504: private response omitted');
  error.status = 504;
  error.fn = 'candidate_workflow_transition_atomic_v1';
  error.json = null;
  error.body = 'must-not-appear';
  const result = candidateAppBackendInternals.safeCandidateTransportDiagnostic(error);
  assert.equal(result.transport_function, 'candidate_workflow_transition_atomic_v1');
  assert.equal(result.transport_status, 504);
  assert.equal(result.database_sqlstate, null);
  assert.equal(JSON.stringify(result).includes('must-not-appear'), false);
});

test('federated Candidate readiness requires identity-projection authority', () => {
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_APP_PUBLIC_URL: 'https://mycloudtms.example.test',
    SUPABASE_URL: 'https://miget-gateway.example.test',
    SUPABASE_SERVICE_ROLE_KEY: 'test-only-service-role-key',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'test-only-private-service-secret',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-session-secret',
    CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET: 'test-only-challenge-secret',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-only-upload-secret',
    CANDIDATE_FEDERATED_ROUTING_ENABLED: 'TRUE',
    R2: { put() {} }
  };
  assert.equal(candidatePrivateWorkerInternals.requiredConfigurationAvailable(env), false);
  assert.equal(candidatePrivateWorkerInternals.requiredConfigurationAvailable({
    ...env,
    CANDIDATE_FEDERATED_IDENTITY_SECRET: 'test-only-identity-secret'
  }), true);
  assert.equal(candidatePrivateWorkerInternals.requiredConfigurationAvailable({
    ...env,
    CANDIDATE_FEDERATED_ROUTING_ENABLED: 'FALSE'
  }), true);
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
