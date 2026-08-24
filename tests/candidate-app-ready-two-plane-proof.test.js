import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { signCandidatePrivateRequest } from '../broker/src/candidate-service-auth.js';
import { signCandidateRouteContext } from '../broker/src/candidate-route-context.js';
import candidatePrivateWorker from '../broker/src/candidate-private-worker.js';
import syntheticPrivateWorker from '../candidate-synthetic-private-api/src/index.js';
import {
  appReadyTwoPlaneProofInternals
} from '../candidate-broker/src/app-ready-two-plane-proof.js';
import {
  CANDIDATE_OPERATION_POLICY,
  CANDIDATE_OPERATION_POLICY_SEMANTIC_SHA256,
  candidateOperationById
} from '../candidate-broker/src/candidate-operation-policy.js';

const PRIMARY = Object.freeze({
  agency_id: '6d0aadb2-ddc8-4ee4-ab37-871bae4a0d88',
  data_plane_id: '2553f243-4846-495d-ba62-b524d0c113c5',
  route_secret: 'unit-test-primary-route-secret',
  proof_class: 'REAL_TEST_DATA_PLANE',
  marker: '8fd56d4854038ac97ce9c52837a0c5ee25c80efa0563b594603bd75cf7598d0c'
});
const SYNTHETIC = Object.freeze({
  agency_id: '457bce0f-65ad-4335-b839-96a85c8d18b1',
  data_plane_id: '41a0e81a-7f9b-405f-862d-16c58db30bb9',
  route_secret: 'unit-test-synthetic-route-secret',
  proof_class: 'SYNTHETIC_NON_BUSINESS_FIXTURE',
  marker: 'f078fdc3b8a030b060626117136db67452e46d12c54cc19b2f6097f0d4a08951'
});

function fixture(key, plane) {
  return {
    fixture_key: key,
    agency_id: plane.agency_id,
    data_plane_id: plane.data_plane_id,
    route_version_id: key === 'DATA_PLANE_A'
      ? '00000000-0000-4000-8000-000000000105' : '00000000-0000-4000-8000-000000000205',
    route_version: 1,
    binding_manifest_generation: 1,
    global_account_id: key === 'DATA_PLANE_A'
      ? '00000000-0000-4000-8000-000000000101' : '00000000-0000-4000-8000-000000000201',
    global_session_id: key === 'DATA_PLANE_A'
      ? '00000000-0000-4000-8000-000000000102' : '00000000-0000-4000-8000-000000000202',
    membership_id: key === 'DATA_PLANE_A'
      ? '00000000-0000-4000-8000-000000000103' : '00000000-0000-4000-8000-000000000203',
    membership_generation: 1,
    local_candidate_id: key === 'DATA_PLANE_A'
      ? '00000000-0000-4000-8000-000000000104' : '00000000-0000-4000-8000-000000000204'
  };
}

function workerEnv(plane) {
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_APP_PUBLIC_URL: 'https://testmode.arthur-rai.co.uk',
    SUPABASE_URL: 'https://test.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'unit-test-service-role',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'unit-test-private-service-secret',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'unit-test-session-secret',
    CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET: 'unit-test-challenge-secret',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'unit-test-upload-secret',
    CANDIDATE_FEDERATED_ROUTING_ENABLED: 'TRUE',
    CANDIDATE_AGENCY_ID: plane.agency_id,
    CANDIDATE_DATA_PLANE_ID: plane.data_plane_id,
    CANDIDATE_ROUTE_VERSION: '1',
    CANDIDATE_ROUTE_CONTEXT_SECRET: plane.route_secret,
    CANDIDATE_ROUTE_CONTEXT_READ_KEY_VERSIONS: '1',
    CANDIDATE_FEDERATED_IDENTITY_SECRET: 'unit-test-identity-secret',
    CANDIDATE_APP_READY_PROOF_ENABLED: 'TRUE',
    CANDIDATE_APP_READY_PROOF_CLASS: plane.proof_class,
    CANDIDATE_APP_READY_RUNTIME_MARKER_SHA256: plane.marker,
    R2: { async put() { return { key: 'unit-test-nonce' }; } }
  };
}

async function route(operation, data, plane, overrides = {}) {
  const now = new Date();
  return signCandidateRouteContext({
    v: 1,
    aud: 'candidate-private-api',
    operation_id: operation.operation_id,
    environment: 'TEST',
    global_account_id: data.global_account_id,
    global_session_id: data.global_session_id,
    membership_id: data.membership_id,
    membership_generation: data.membership_generation,
    agency_id: data.agency_id,
    agency_candidate_id: data.local_candidate_id,
    data_plane_id: data.data_plane_id,
    route_version: data.route_version,
    session_epoch: 1,
    issued_at_utc: now.toISOString(),
    expires_at_utc: new Date(now.getTime() + 240_000).toISOString(),
    key_version: 1,
    ...overrides
  }, { secret: plane.route_secret, keyVersion: 1 });
}

async function managerEmailRoute(operation, data, plane, overrides = {}) {
  const now = new Date();
  return signCandidateRouteContext({
    v: 2,
    typ: 'cloudtms-route-context-v2',
    aud: 'candidate-private-api',
    authority_kind: 'MANAGER_EMAIL',
    operation_id: operation.operation_id,
    environment: 'TEST',
    agency_id: data.agency_id,
    data_plane_id: data.data_plane_id,
    route_version_id: data.route_version_id,
    route_version: data.route_version,
    binding_manifest_generation: data.binding_manifest_generation,
    manager_route_ticket_id: '00000000-0000-4000-8000-00000000a101',
    route_revision: 1,
    workflow_route_hmac: 'a'.repeat(64),
    approval_request_route_hmac: 'b'.repeat(64),
    request_generation: 1,
    credential_generation: 1,
    issued_at_utc: now.toISOString(),
    expires_at_utc: new Date(now.getTime() + 240_000).toISOString(),
    nonce: '00000000-0000-4000-8000-00000000a102',
    key_version: 1,
    ...overrides
  }, { secret: plane.route_secret, keyVersion: 1 });
}

async function probeRequest(operation, plane, data) {
  const signedRoute = await route(operation, data, plane);
  const unsigned = new Request('https://private.invalid/private/app-ready/v1/route-probe', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-cloudtms-route-context': signedRoute.envelope,
      'x-cloudtms-route-context-sha256': signedRoute.sha256
    },
    body: JSON.stringify({
      operation_id: operation.operation_id,
      method: operation.method,
      path: operation.path,
      cases: [{ case_id: 'positive', envelope: signedRoute.envelope, sha256: signedRoute.sha256 }]
    })
  });
  return signCandidatePrivateRequest(unsigned, workerEnv(plane));
}

async function managerProbeRequest(operation, plane, data) {
  const signedRoute = await managerEmailRoute(operation, data, plane);
  const unsigned = new Request('https://private.invalid/private/app-ready/v1/route-probe', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-cloudtms-route-context': signedRoute.envelope,
      'x-cloudtms-route-context-sha256': signedRoute.sha256
    },
    body: JSON.stringify({
      operation_id: operation.operation_id,
      method: operation.method,
      path: operation.path,
      cases: [{ case_id: 'positive', envelope: signedRoute.envelope, sha256: signedRoute.sha256 }]
    })
  });
  return signCandidatePrivateRequest(unsigned, workerEnv(plane));
}

test('closed backend operation policy is byte-semantically attested and classifies all 63 operations', async () => {
  const source = JSON.parse(await readFile(
    new URL('../candidate-broker/policy/candidate-operation-policy.json', import.meta.url), 'utf8'
  ));
  assert.equal(createHash('sha256').update(JSON.stringify(source)).digest('hex'),
    CANDIDATE_OPERATION_POLICY_SEMANTIC_SHA256);
  assert.equal(CANDIDATE_OPERATION_POLICY.length, 63);
  assert.equal(CANDIDATE_OPERATION_POLICY.filter((entry) => entry.data_plane_dispatch_required).length, 50);
  assert.equal(CANDIDATE_OPERATION_POLICY.filter((entry) => !entry.data_plane_dispatch_required).length, 13);
  assert.equal(new Set(CANDIDATE_OPERATION_POLICY.map((entry) => entry.operation_id)).size, 63);
  assert.ok(CANDIDATE_OPERATION_POLICY.every((entry) => (
    entry.client_agency_selector_allowed === false && entry.preserves_business_rpc_meaning === true
  )));
  assert.equal(CANDIDATE_OPERATION_POLICY.filter(
    (entry) => entry.isolation_proof_class === 'MANAGER_CREDENTIAL_ROUTED_DATA_PLANE'
  ).length, 6);
  assert.equal(CANDIDATE_OPERATION_POLICY.filter(
    (entry) => entry.isolation_proof_class === 'TYPED_AUTHORITY_ROUTED_DATA_PLANE'
  ).length, 1);
});

test('closed binding catalogue deterministically matches the broker service-binding configuration', async () => {
  const catalogue = JSON.parse(await readFile(
    new URL('../candidate-broker/config/candidate-data-plane-bindings.json', import.meta.url), 'utf8'
  ));
  const wrangler = JSON.parse(await readFile(
    new URL('../candidate-broker/wrangler.jsonc', import.meta.url), 'utf8'
  ));
  const configured = new Map(wrangler.services.map((entry) => [entry.binding, entry.service]));
  assert.equal(catalogue.entries.length, 2);
  for (const entry of catalogue.entries) {
    assert.equal(configured.get(entry.worker_binding), entry.service_name);
  }
  const configuredDataPlanes = new Map(
    [...configured].filter(([binding]) => binding !== 'MYTMS_IDENTITY_DELIVERY')
  );
  assert.equal(configuredDataPlanes.size, catalogue.entries.length);
  assert.equal(configured.get('MYTMS_IDENTITY_DELIVERY'), 'test-cloudtms-backend');
  assert.ok(!catalogue.entries.some((entry) => entry.worker_binding === 'MYTMS_IDENTITY_DELIVERY'));
});

test('control-plane matrix cases are complete, closed and never accept a client selector', () => {
  const fixtures = [fixture('DATA_PLANE_A', PRIMARY), fixture('DATA_PLANE_B', SYNTHETIC)];
  const dispatch = appReadyTwoPlaneProofInternals.controlCases(
    candidateOperationById('getCandidateBootstrap'), fixtures
  );
  const neutral = appReadyTwoPlaneProofInternals.controlCases(
    candidateOperationById('loginCandidate'), fixtures
  );
  assert.equal(dispatch.length, 11);
  assert.equal(neutral.length, 3);
  assert.deepEqual(dispatch.filter((entry) => entry.client_selector_present).map((entry) => entry.case_id),
    ['getCandidateBootstrap_selector']);
  assert.equal(neutral.find((entry) => entry.case_id.endsWith('_neutral')).context, null);
});

test('manager EMAIL proof uses v2 server-owned route authority for all six manager operations and shared upload', () => {
  const fixtures = [fixture('DATA_PLANE_A', PRIMARY), fixture('DATA_PLANE_B', SYNTHETIC)];
  assert.deepEqual(appReadyTwoPlaneProofInternals.managerEmailOperationIds, [
    'startManagerReview',
    'streamManagerReviewDocument',
    'recordManagerReviewProgress',
    'prepareManagerSignature',
    'approveManagerReview',
    'refuseManagerReview',
    'uploadCandidateComponent'
  ]);
  for (const operationId of appReadyTwoPlaneProofInternals.managerEmailOperationIds) {
    const operation = candidateOperationById(operationId);
    const context = appReadyTwoPlaneProofInternals.managerEmailFixtureContext(operation, fixtures[0]);
    const cases = appReadyTwoPlaneProofInternals.controlCases(operation, fixtures, 'MANAGER_EMAIL');
    assert.equal(context.v, 2);
    assert.equal(context.typ, 'cloudtms-route-context-v2');
    assert.equal(context.authority_kind, 'MANAGER_EMAIL');
    assert.equal(context.operation_id, operationId);
    assert.equal(context.route_version_id, fixtures[0].route_version_id);
    assert.equal(context.binding_manifest_generation, fixtures[0].binding_manifest_generation);
    assert.equal(cases.length, 11);
    assert.ok(cases.some((entry) => entry.case_id.endsWith('_wrong_route_id')));
    assert.ok(cases.some((entry) => entry.case_id.endsWith('_stale_binding')));
    assert.ok(cases.some((entry) => entry.case_id.endsWith('_wrong_authority')));
  }
});

test('real TEST private Worker accepts only the signed primary proof context before business RPC', async () => {
  const operation = candidateOperationById('getCandidateBootstrap');
  const data = fixture('DATA_PLANE_A', PRIMARY);
  const response = await candidatePrivateWorker.fetch(
    await probeRequest(operation, PRIMARY, data), workerEnv(PRIMARY), { waitUntil() {} }
  );
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.ok, true);
  assert.equal(body.proof_class, 'REAL_TEST_DATA_PLANE');
  assert.deepEqual(body.results, [{ case_id: 'positive', accepted: true }]);
});

test('real and synthetic private proof adapters accept signed manager EMAIL v2 route context without business RPC', async () => {
  const operation = candidateOperationById('startManagerReview');
  const primaryData = fixture('DATA_PLANE_A', PRIMARY);
  const syntheticData = fixture('DATA_PLANE_B', SYNTHETIC);
  const [primaryResponse, syntheticResponse] = await Promise.all([
    candidatePrivateWorker.fetch(
      await managerProbeRequest(operation, PRIMARY, primaryData), workerEnv(PRIMARY), { waitUntil() {} }
    ),
    syntheticPrivateWorker.fetch(
      await managerProbeRequest(operation, SYNTHETIC, syntheticData), workerEnv(SYNTHETIC)
    )
  ]);
  assert.equal(primaryResponse.status, 200);
  assert.equal(syntheticResponse.status, 200);
  assert.deepEqual((await primaryResponse.json()).results, [{ case_id: 'positive', accepted: true }]);
  assert.deepEqual((await syntheticResponse.json()).results, [{ case_id: 'positive', accepted: true }]);
});

test('synthetic Worker is service-authenticated, replay guarded and contains no business adapter', async () => {
  const operation = candidateOperationById('getCandidateBootstrap');
  const data = fixture('DATA_PLANE_B', SYNTHETIC);
  const env = workerEnv(SYNTHETIC);
  const response = await syntheticPrivateWorker.fetch(await probeRequest(operation, SYNTHETIC, data), env);
  assert.equal(response.status, 200);
  assert.equal((await response.json()).proof_class, 'SYNTHETIC_NON_BUSINESS_FIXTURE');
  const source = await readFile(new URL('../candidate-synthetic-private-api/src/index.js', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /SUPABASE|candidate_app_|mail_outbox|provider|business rpc/i);
});

test('post-signature semantic-body substitution is rejected before nonce or proof handling', async () => {
  const operation = candidateOperationById('getCandidateBootstrap');
  const data = fixture('DATA_PLANE_A', PRIMARY);
  const signed = await probeRequest(operation, PRIMARY, data);
  const tampered = new Request(signed.url, {
    method: signed.method,
    headers: signed.headers,
    body: JSON.stringify({ changed: true })
  });
  const response = await candidatePrivateWorker.fetch(tampered, workerEnv(PRIMARY), { waitUntil() {} });
  assert.equal(response.status, 401);
  assert.equal((await response.json()).error_code, 'CANDIDATE_PRIVATE_SERVICE_AUTH_REQUIRED');
});
