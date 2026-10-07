import assert from 'node:assert/strict';
import test from 'node:test';
import { candidateBrokerInternals, handleCandidateBrokerRequest } from '../candidate-broker/src/candidate-broker.js';

const ids = Object.freeze({
  account: '10000000-0000-4000-8000-000000000001',
  session: '10000000-0000-4000-8000-000000000002',
  renewed: '10000000-0000-4000-8000-000000000003',
  family: '10000000-0000-4000-8000-000000000004',
  membership: '10000000-0000-4000-8000-000000000005'
});

async function refreshFailure({ routeCode = 'AGENCY_CONTEXT_STALE', metadataStatus = 200,
  metadataBody = { ok: true, account_id: ids.account } } = {}) {
  const originalFetch = globalThis.fetch;
  const calls = [];
  let privateCalls = 0;
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST', CANDIDATE_ALLOW_NATIVE_CLIENTS: 'true',
    MYTMS_CONTROL_PLANE_ENABLED: 'TRUE', MYTMS_GLOBAL_AUTH_CUTOVER_ENABLED: 'TRUE',
    MYTMS_CONTROL_PLANE_URL: 'https://control-plane.test.example',
    MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'synthetic-test-service-key-not-live',
    MYTMS_CONTROL_PLANE_TOKEN_DERIVATION_SECRET: 'synthetic-test-token-derivation-secret-not-live',
    MYTMS_CONTROL_PLANE_ACTOR_IDENTITY_SECRET: 'synthetic-test-actor-identity-secret-not-live',
    CANDIDATE_BROKER_REFRESH_TOKEN_SECRET: 'synthetic-test-refresh-secret-not-live',
    CANDIDATE_BROKER_PUBLIC_SESSION_ID_SECRET: 'synthetic-test-public-session-secret-not-live',
    CANDIDATE_GENERAL_RATE_LIMIT: { limit: async () => ({ success: true }) },
    CANDIDATE_AUTH_RATE_LIMIT: { limit: async () => ({ success: true }) },
    CLOUDTMS_PRIVATE: { fetch: async () => { privateCalls += 1; throw new Error('No private dispatch permitted'); } }
  };
  const now = Math.floor(Date.now() / 1000);
  const absoluteExpiry = new Date(Date.now() + 60 * 86400000).toISOString();
  const refreshToken = await candidateBrokerInternals.sealVersionedEnvelope(
    env, candidateBrokerInternals.credentialAuthorities.refresh, 'candidate-broker-refresh-v1', {
      typ: 'candidate_broker_refresh', aud: 'cloudtms-candidate-refresh', env: 'TEST',
      authority: 'CONTROL_PLANE', global_account_id: ids.account,
      global_session_id: ids.session, global_session_family_id: ids.family,
      public_session_id: ids.session, control_plane_refresh_token: 'synthetic-private-refresh-token-not-live',
      session_epoch: 4, rotation: 3, absolute_expires_at_utc: absoluteExpiry,
      iat: now, exp: now + 60 * 86400
    }
  );
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    const args = await request.json();
    calls.push({ operation, args });
    if (operation === 'global_refresh_v1') return Response.json({
      ok: true, account_id: ids.account, session_id: ids.renewed,
      family_id: ids.family, rotation: 4, session_epoch: 5,
      selected_membership_id: ids.membership, issued_at_utc: args.p_now_utc,
      expires_at_utc: args.p_internal_context.expires_at_utc,
      absolute_expires_at_utc: absoluteExpiry
    });
    if (operation === 'agency_route_context_resolve_v1') return Response.json(
      { code: '28000', message: routeCode }, { status: routeCode.endsWith('UNAVAILABLE') ? 503 : 403 }
    );
    if (operation === 'global_session_metadata_v1') return Response.json(metadataBody, { status: metadataStatus });
    throw new Error(`Unexpected control operation: ${operation}`);
  };
  try {
    const response = await handleCandidateBrokerRequest(new Request(
      'https://candidate-api.test.example/candidate-app/v1/auth/refresh', {
        method: 'POST', headers: { 'x-cloudtms-client': 'android', 'content-type': 'application/json' },
        body: JSON.stringify({ refresh_token: refreshToken, session_id: ids.session,
          idempotency_key: '10000000-0000-4000-8000-000000000009' })
      }
    ), env);
    return { status: response.status, body: await response.json(), calls, privateCalls };
  } finally { globalThis.fetch = originalFetch; }
}

test('refresh route failure with invalid renewed session requests reauthentication, not endless agency retry', async () => {
  const result = await refreshFailure({ metadataStatus: 403,
    metadataBody: { code: '28000', message: 'GLOBAL_SESSION_INVALID' } });
  assert.equal(result.status, 401);
  assert.equal(result.body.error_code, 'GLOBAL_SESSION_INVALID');
  assert.deepEqual(result.calls.map(c => c.operation), [
    'global_refresh_v1', 'agency_route_context_resolve_v1', 'global_session_metadata_v1'
  ]);
  const context = result.calls[2].args.p_global_session_context;
  assert.equal(context.account_id, ids.account);
  assert.equal(context.session_id, ids.renewed);
  assert.equal(context.session_epoch, 5);
  assert.deepEqual(context, result.calls[1].args.p_global_session_context);
  assert.equal(result.privateCalls, 0);
  assert.deepEqual(Object.keys(result.body).sort(), ['error_code', 'ok', 'request_id']);
});

test('refresh with valid global identity and unavailable agency retains the original retryable route failure', async () => {
  const result = await refreshFailure();
  assert.equal(result.status, 403);
  assert.equal(result.body.error_code, 'AGENCY_CONTEXT_STALE');
  assert.equal(result.calls.length, 3);
  assert.equal(result.privateCalls, 0);
  assert.equal(JSON.stringify(result.body).includes(ids.account), false);
});

test('structured identity rejection also requests sign-in without disclosing metadata', async () => {
  const result = await refreshFailure({ metadataBody: { ok: false, error_code: 'GLOBAL_SESSION_INVALID' } });
  assert.equal(result.status, 401);
  assert.equal(result.body.error_code, 'GLOBAL_SESSION_INVALID');
  assert.equal(result.privateCalls, 0);
  assert.deepEqual(Object.keys(result.body).sort(), ['error_code', 'ok', 'request_id']);
});

test('failed identity diagnostic never invents a definitive session rejection', async () => {
  const result = await refreshFailure({ metadataStatus: 503,
    metadataBody: { code: '08006', message: 'database temporarily unavailable' } });
  assert.equal(result.status, 503);
  assert.equal(result.body.error_code, 'DEPENDENCY_UNAVAILABLE');
  assert.equal(result.privateCalls, 0);
});

test('unrelated agency dependency failure does not run the diagnostic or alter renewal', async () => {
  const result = await refreshFailure({ routeCode: 'AGENCY_ROUTE_UNAVAILABLE' });
  assert.equal(result.status, 503);
  assert.equal(result.body.error_code, 'AGENCY_ROUTE_UNAVAILABLE');
  assert.deepEqual(result.calls.map(c => c.operation), ['global_refresh_v1', 'agency_route_context_resolve_v1']);
  assert.equal(result.privateCalls, 0);
});
