import assert from 'node:assert/strict';
import test from 'node:test';
import privateWorker from '../broker/src/candidate-private-worker.js';
import { sha256Hex } from '../broker/src/candidate-daily-contract-v1.js';
import { signCandidatePrivateRequest } from '../broker/src/candidate-service-auth.js';
import { signMyTmsGoogleRouteContext, verifyMyTmsGoogleRouteContext,
  googleRouteRequestMatches, myTmsGoogleRouteContextInternals } from '../broker/src/mytms-google-route-context.js';
import { myTmsGoogleControlInternals } from '../candidate-broker/src/mytms-google-control.js';

const ids = Object.fromEntries(['integration','agency','plane','route','operation'].map((name, i) =>
  [name, `30000000-0000-4000-8000-00000000000${i+1}`]));
const canonical = myTmsGoogleRouteContextInternals.canonicalJson;
const hash = value => sha256Hex(new TextEncoder().encode(canonical(value)));
const removal = () => ({ operation_id: ids.operation, candidate_code: 'cid1-ABCDE',
  candidate_source_hmac: 'a'.repeat(64), source_hmac_key_version: 1,
  surname: 'Synthetic', email: 'synthetic@example.invalid', mobile: '07700900161', row_fingerprint: 'b'.repeat(64) });
const bodyFor = () => ({ operation_id: ids.operation, removal_request: removal(), correlation_id: 'rota-removal-test' });
function environment() {
  const nonces = new Set();
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST', CANDIDATE_APP_PUBLIC_URL: 'https://app.example.invalid',
    CANDIDATE_AGENCY_ID: ids.agency, CANDIDATE_DATA_PLANE_ID: ids.plane, CANDIDATE_ROUTE_VERSION: '7',
    CANDIDATE_ROUTE_CONTEXT_SECRET: 'synthetic-route-secret', CANDIDATE_GOOGLE_ROUTE_CONTEXT_KEY_VERSION: '1',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'synthetic-private-secret',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'synthetic-session-secret',
    CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET: 'synthetic-challenge-secret',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'synthetic-upload-secret',
    SUPABASE_URL: 'https://miget.example.invalid', SUPABASE_SERVICE_ROLE_KEY: 'synthetic-service-only',
    R2: { async put(key) { if (nonces.has(key)) return null; nonces.add(key); return { key }; } }
  };
}
async function signedRequest(env, {body = bodyFor(), boundBody = body, purpose = 'ROTA_REMOVE', v = 2,
  path = 'rota-remove', context = {}, now = Date.now()} = {}) {
  const signed = await signMyTmsGoogleRouteContext({
    v, environment: 'TEST', integration_id: ids.integration, agency_id: ids.agency,
    data_plane_id: ids.plane, route_version_id: ids.route, route_version: 7, target_generation: 4,
    operation_id: ids.operation, purpose, project_role: 'MASTER', request_sha256: await hash(boundBody),
    operation_created_at_utc: new Date(now-1000).toISOString(),
    issued_at_utc: new Date(now).toISOString(), expires_at_utc: new Date(now+240000).toISOString(), key_version: 1,
    ...context
  }, env.CANDIDATE_ROUTE_CONTEXT_SECRET, now);
  const request = new Request(`https://private.invalid/private/mytms-google-data/v1/candidates/${path}`, {
    method: 'POST', headers: { 'content-type': 'application/json',
      'x-cloudtms-google-route-context': signed.envelope,
      'x-cloudtms-google-route-context-sha256': signed.sha256 }, body: JSON.stringify(body)
  });
  return { request: await signCandidatePrivateRequest(request, env), signed };
}

test('v2 removal authority binds the exact purpose, registered MASTER and canonical body', async () => {
  const env = environment();
  const { request, signed } = await signedRequest(env);
  const verified = await verifyMyTmsGoogleRouteContext(request, env);
  assert.equal(verified.context.v, 2);
  assert.equal(await googleRouteRequestMatches(verified.context, 'ROTA_REMOVE', bodyFor()), true);
  assert.equal(await googleRouteRequestMatches(verified.context, 'PROVISIONING_ATTACH', bodyFor()), false);
  assert.equal(await googleRouteRequestMatches(verified.context, 'ROTA_REMOVE', {
    ...bodyFor(), removal_request: {...removal(), candidate_code: 'CID1-OTHER'}
  }), false);
  assert.equal(await googleRouteRequestMatches({...verified.context, v: 1}, 'ROTA_REMOVE', bodyFor()), false);
  await assert.rejects(() => signMyTmsGoogleRouteContext({...signed.context, project_role: 'AVAILABILITY'},
    env.CANDIDATE_ROUTE_CONTEXT_SECRET), /CONTEXT_INVALID/);
  assert.equal(await verifyMyTmsGoogleRouteContext(request, {...env, CANDIDATE_DATA_PLANE_ID: ids.integration}), null);
  assert.equal(await verifyMyTmsGoogleRouteContext(request, env, Date.parse(signed.context.expires_at_utc)), null);
});

test('private removal rejects old match/attach authority, changed payload, changed operation and replay before SQL', async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => { calls++; throw new Error('Unexpected SQL request'); };
  try {
    for (const options of [
      {v: 1}, {purpose: 'PROVISIONING_ATTACH'},
      {boundBody: {...bodyFor(), removal_request: {...removal(), surname: 'Changed'}}},
      {body: {...bodyFor(), operation_id: ids.integration}}
    ]) {
      const env = environment();
      const {request} = await signedRequest(env, options);
      const response = await privateWorker.fetch(request, env, {});
      assert.equal(response.status, 401);
    }
    assert.equal(calls, 0);
  } finally { globalThis.fetch = originalFetch; }
});

test('private removal invokes only its narrow service RPC and repeat transport nonce is rejected', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (input, options) => {
    const request = input instanceof Request ? input : new Request(input, options);
    calls.push({path: new URL(request.url).pathname, body: await request.json()});
    return Response.json({ok: true, state: 'REMOVED', operation_id: ids.operation, idempotent_replay: false});
  };
  try {
    const env = environment();
    const {request} = await signedRequest(env);
    const response = await privateWorker.fetch(request.clone(), env, {});
    assert.equal(response.status, 200);
    assert.equal((await response.json()).state, 'REMOVED');
    assert.equal(calls.length, 1);
    assert.match(calls[0].path, /candidate_google_rota_remove_v1$/);
    assert.equal(calls[0].body.p_internal_context.audience, 'GOOGLE_ROTA_REMOVE');
    assert.equal(calls[0].body.p_internal_context.project_role, 'MASTER');
    assert.deepEqual(calls[0].body.p_request, removal());
    assert.equal((await privateWorker.fetch(request, env, {})).status, 401);
    assert.equal(calls.length, 1);
  } finally { globalThis.fetch = originalFetch; }
});

test('public removal obtains a durable exact target pin; caller role cannot authorise a private write', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  let privateCalls = 0;
  const target = {ok: true, state: 'ACTIVE', environment: 'TEST', integration_id: ids.integration,
    agency_id: ids.agency, data_plane_id: ids.plane, route_version_id: ids.route, route_version: 7,
    target_generation: 4, registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST', project_role: 'MASTER'};
  const env = {...environment(), MYTMS_CONTROL_PLANE_ENABLED: 'TRUE',
    MYTMS_CONTROL_PLANE_URL: 'https://control.example.invalid', MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'synthetic-control-key',
    CANDIDATE_DATA_PLANE_CLOUDTMS_TEST_ROUTE_CONTEXT_SECRET: 'synthetic-route-secret',
    CLOUDTMS_PRIVATE: { async fetch(request) {
      privateCalls++;
      assert.match(new URL(request.url).pathname, /candidates\/rota-remove$/);
      const verified = await verifyMyTmsGoogleRouteContext(request, env);
      assert.equal(await googleRouteRequestMatches(verified.context, 'ROTA_REMOVE', await request.json()), true);
      return Response.json({ok: true, state: 'UNLINKED', operation_id: ids.operation});
    } }
  };
  globalThis.fetch = async request => {
    calls.push({path: new URL(request.url).pathname, body: await request.json()});
    return Response.json(target);
  };
  const input = {body: {removal_request: removal(), project_role: 'MASTER'},
    google_context: {integration_key: 'synthetic-master'}, correlation_id: 'rota-removal-test'};
  try {
    assert.equal((await myTmsGoogleControlInternals.removeCandidateRota(input, env)).state, 'UNLINKED');
    assert.match(calls[0].path, /rota_removal_context_get_v1$/);
    assert.equal(calls[0].body.p_request_hash, await hash(removal()));
    assert.equal(privateCalls, 1);
    target.project_role = 'AVAILABILITY';
    await assert.rejects(() => myTmsGoogleControlInternals.removeCandidateRota(input, env), error => error.code === 'SYSTEM_AUTH_FAILED');
    assert.equal(privateCalls, 1);
    Object.assign(target, {ok: false, state: 'CONFLICT', error_code: 'GOOGLE_OPERATION_CONFLICT'});
    assert.equal((await myTmsGoogleControlInternals.removeCandidateRota(input, env)).state, 'CONFLICT');
    assert.equal(privateCalls, 1);
  } finally { globalThis.fetch = originalFetch; }
});
