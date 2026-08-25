import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import { candidateDailySignedMessageBytes, parseCandidateDailyRawTarget } from '../broker/src/candidate-daily-hmac-v1.js';
import { sha256Hex } from '../broker/src/candidate-daily-contract-v1.js';
import { verifyMyTmsGoogleControlRequest } from '../broker/src/mytms-google-control-hmac.js';
import {
  signMyTmsGoogleRouteContext,
  verifyMyTmsGoogleRouteContext
} from '../broker/src/mytms-google-route-context.js';
import { signCandidatePrivateRequest, verifyCandidatePrivateRequest } from '../broker/src/candidate-service-auth.js';
import { myTmsGoogleControlInternals } from '../candidate-broker/src/mytms-google-control.js';

const IDS = Object.freeze({
  integration: '30000000-0000-4000-8000-000000000001',
  agency: '30000000-0000-4000-8000-000000000002',
  plane: '30000000-0000-4000-8000-000000000003',
  route: '30000000-0000-4000-8000-000000000004',
  operation: '30000000-0000-4000-8000-000000000005'
});

function env(overrides = {}) {
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST', CANDIDATE_AGENCY_ID: IDS.agency,
    CANDIDATE_DATA_PLANE_ID: IDS.plane, CANDIDATE_ROUTE_VERSION: '7',
    CANDIDATE_ROUTE_CONTEXT_SECRET: 'synthetic-google-route-secret',
    CANDIDATE_GOOGLE_ROUTE_CONTEXT_KEY_VERSION: '1',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'synthetic-private-service-secret',
    CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_KEY_ID: 'synthetic-google-v1',
    CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_SECRET: 'synthetic-google-hmac-secret',
    ...overrides
  };
}

function nonceStore() {
  const values = new Set();
  return {
    async put(key) {
      if (values.has(key)) return null;
      values.add(key);
      return { key };
    }
  };
}

async function hmacRequest(method, url, body = null, overrides = {}) {
  const environment = env();
  const rawBody = body == null ? new Uint8Array() : new TextEncoder().encode(JSON.stringify(body));
  const target = parseCandidateDailyRawTarget(`${new URL(url).pathname}${new URL(url).search}`);
  const timestamp = String(overrides.timestamp || Math.floor(Date.now() / 1000));
  const nonce = overrides.nonce || 'abcdefghijklmnopqrstuv';
  const correlationId = overrides.correlationId || '01K35Y7N7ER4QY5F7M8D9P0Q1R';
  const idempotencyKey = method === 'POST' ? (overrides.idempotencyKey || 'google-control-test-key-0001') : '';
  const contentSha256 = await sha256Hex(rawBody);
  const fields = {
    method, normalizedPath: target.normalizedPath, normalizedQuery: target.normalizedQuery,
    timestamp, nonce, contentSha256, idempotencyKey, correlationId,
    keyId: environment.CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_KEY_ID
  };
  const message = candidateDailySignedMessageBytes(fields, rawBody);
  const key = await crypto.subtle.importKey(
    'raw', new TextEncoder().encode(environment.CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_SECRET),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  const signature = Array.from(new Uint8Array(await crypto.subtle.sign('HMAC', key, message)),
    byte => byte.toString(16).padStart(2, '0')).join('');
  return new Request(url, {
    method,
    headers: {
      ...(method === 'POST' ? { 'content-type': 'application/json; charset=utf-8' } : {}),
      'content-length': String(rawBody.byteLength),
      'x-cloudtms-key-id': fields.keyId, 'x-cloudtms-signature-version': 'v1',
      'x-cloudtms-timestamp': timestamp, 'x-cloudtms-nonce': nonce,
      'x-cloudtms-content-sha256': contentSha256, 'x-cloudtms-signature': signature,
      'x-correlation-id': correlationId, ...(idempotencyKey ? { 'idempotency-key': idempotencyKey } : {})
    },
    body: rawBody.byteLength ? rawBody : undefined
  });
}

function googleContext() {
  return {
    integration_key: 'master_test', project_identity_hmac: '1'.repeat(64),
    principal_fingerprint: '2'.repeat(64), actor_identity_hmac: '3'.repeat(64),
    source_revision: 'R48_CAPTURE_AUTHORITY_AND_SETTLEMENT_V1'
  };
}

test('Google data-plane route context selects exactly one deployment and service auth v3 binds it', async () => {
  const now = Date.now();
  const environment = env();
  const signed = await signMyTmsGoogleRouteContext({
    environment: 'TEST', integration_id: IDS.integration, agency_id: IDS.agency,
    data_plane_id: IDS.plane, route_version_id: IDS.route, route_version: 7,
    target_generation: 4, operation_id: IDS.operation,
    issued_at_utc: new Date(now).toISOString(),
    expires_at_utc: new Date(now + 240_000).toISOString(), key_version: 1
  }, environment.CANDIDATE_ROUTE_CONTEXT_SECRET, now);
  const unsigned = new Request('https://private.invalid/private/mytms-google-data/v1/candidates/match', {
    method: 'POST', headers: {
      'content-type': 'application/json',
      'x-cloudtms-google-route-context': signed.envelope,
      'x-cloudtms-google-route-context-sha256': signed.sha256
    }, body: '{}'
  });
  const serviceSigned = await signCandidatePrivateRequest(unsigned, environment);
  assert.equal(serviceSigned.headers.get('x-cloudtms-service-version'), 'candidate-private-v3');
  assert.equal(await verifyCandidatePrivateRequest(serviceSigned.clone(), environment), true);
  assert.equal((await verifyMyTmsGoogleRouteContext(serviceSigned, environment, now)).context.operation_id, IDS.operation);

  const changed = new Headers(serviceSigned.headers);
  changed.set('x-cloudtms-google-route-context-sha256', '0'.repeat(64));
  assert.equal(await verifyCandidatePrivateRequest(new Request(serviceSigned, { headers: changed }), environment), false);
  assert.equal(await verifyMyTmsGoogleRouteContext(unsigned, { ...environment, CANDIDATE_AGENCY_ID: IDS.integration }, now), null);
});

test('private Google-control HMAC accepts canonical POST and GET, rejects replay and changed body', async () => {
  const environment = env({ R2: nonceStore() });
  const preflightBody = {
    google_context: googleContext(), operation_id: IDS.operation,
    request_hash: '4'.repeat(64), reservation_token: 'r'.repeat(64),
    candidate_code: 'CID1-ABCDE', surname: 'Example', email: 'person@example.test',
    mobile: '+447700900111', google_source_identity_hmac: '5'.repeat(64), source_hmac_key_version: 1
  };
  const request = await hmacRequest('POST',
    'https://broker.invalid/private/google-control/v1/candidates/provisioning/preflight', preflightBody);
  const verified = await verifyMyTmsGoogleControlRequest(request.clone(), environment);
  assert.equal(verified.ok, true);
  assert.equal(verified.route, 'PROVISIONING_PREFLIGHT');
  assert.deepEqual(verified.googleContext.integration_key, 'master_test');
  assert.equal((await verifyMyTmsGoogleControlRequest(request.clone(), environment)).status, 401);

  const statusUrl = new URL(`https://broker.invalid/private/google-control/v1/candidates/provisioning/${IDS.operation}`);
  for (const [name, value] of Object.entries(googleContext())) statusUrl.searchParams.set(name, value);
  const getRequest = await hmacRequest('GET', statusUrl.toString(), null, { nonce: 'abcdefghijklmnopqrstuw' });
  const getVerified = await verifyMyTmsGoogleControlRequest(getRequest, environment);
  assert.equal(getVerified.ok, true);
  assert.equal(getVerified.route, 'PROVISIONING_STATUS');

  const statusWithoutLength = new URL(`https://broker.invalid/private/google-control/v1/target-switches/${IDS.operation}`);
  for (const [name, value] of Object.entries(googleContext())) statusWithoutLength.searchParams.set(name, value);
  const signedWithoutLength = await hmacRequest('GET', statusWithoutLength.toString(), null, {
    nonce: 'abcdefghijklmnopqrstux'
  });
  const noLengthHeaders = new Headers(signedWithoutLength.headers);
  noLengthHeaders.delete('content-length');
  const noLengthVerified = await verifyMyTmsGoogleControlRequest(
    new Request(signedWithoutLength, { headers: noLengthHeaders }), environment
  );
  assert.equal(noLengthVerified.ok, true);
  assert.equal(noLengthVerified.route, 'TARGET_SWITCH_STATUS');

  const changedBody = new Request(request, { body: JSON.stringify({ ...preflightBody, surname: 'Changed' }) });
  assert.equal((await verifyMyTmsGoogleControlRequest(changedBody, env({ R2: nonceStore() }))).status, 401);
});

test('private Google-control HMAC accepts only the closed integration heartbeat route', async () => {
  const request = await hmacRequest('POST',
    'https://broker.invalid/private/google-control/v1/integrations/heartbeat', {
      google_context: googleContext(), project_role: 'MASTER'
    }, { nonce: 'abcdefghijklmnopqrstu1' });
  const verified = await verifyMyTmsGoogleControlRequest(request, env({ R2: nonceStore() }));
  assert.equal(verified.ok, true);
  assert.equal(verified.route, 'INTEGRATION_HEARTBEAT');
});

test('integration heartbeat authenticates identity before recording server-owned capability facts', async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (request) => {
    const path = new URL(request.url).pathname;
    const body = JSON.parse(await request.text());
    calls.push({ path, body });
    if (path.endsWith('/integration_identity_authenticate_v1')) {
      return new Response(JSON.stringify({
        integration_id: IDS.integration, integration_status: 'DISABLED', project_role: 'MASTER',
        source_revision: googleContext().source_revision, internal_only: true
      }), { status: 200, headers: { 'content-type': 'application/json' } });
    }
    if (path.endsWith('/worker_heartbeat_v1')) {
      return new Response(JSON.stringify({
        ok: true, status: 'RECORDED', expires_at_utc: body.p_expires_at_utc
      }), { status: 200, headers: { 'content-type': 'application/json' } });
    }
    return new Response('{}', { status: 404, headers: { 'content-type': 'application/json' } });
  };
  try {
    const result = await myTmsGoogleControlInternals.integrationHeartbeat({
      body: { project_role: 'MASTER' }, google_context: googleContext(),
      correlation_id: '01K35Y7N7ER4QY5F7M8D9P0Q1R'
    }, {
      MYTMS_CONTROL_PLANE_ENABLED: 'TRUE',
      MYTMS_CONTROL_PLANE_URL: 'https://control.example.test',
      MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-only-service-role-key'
    });
    assert.equal(result.status, 'RECORDED');
    assert.equal(calls.length, 2);
    assert.match(calls[0].path, /integration_identity_authenticate_v1$/);
    assert.match(calls[1].path, /worker_heartbeat_v1$/);
    assert.deepEqual(calls[1].body.p_capability_facts, {
      schema: 1, project_role: 'MASTER',
      source_revision: googleContext().source_revision,
      transport_authority: 'SIGNED_GOOGLE_CONTROL_HMAC_V1',
      candidate_provisioning: true, daily_availability: false
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('integration heartbeat rejects a project-role substitution before registration write', async () => {
  const originalFetch = globalThis.fetch;
  let heartbeatCalls = 0;
  globalThis.fetch = async (request) => {
    if (new URL(request.url).pathname.endsWith('/worker_heartbeat_v1')) heartbeatCalls += 1;
    return new Response(JSON.stringify({
      integration_id: IDS.integration, integration_status: 'DISABLED', project_role: 'AVAILABILITY',
      source_revision: googleContext().source_revision, internal_only: true
    }), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  try {
    await assert.rejects(() => myTmsGoogleControlInternals.integrationHeartbeat({
      body: { project_role: 'MASTER' }, google_context: googleContext(),
      correlation_id: '01K35Y7N7ER4QY5F7M8D9P0Q1R'
    }, {
      MYTMS_CONTROL_PLANE_ENABLED: 'TRUE',
      MYTMS_CONTROL_PLANE_URL: 'https://control.example.test',
      MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-only-service-role-key'
    }), (error) => error?.code === 'SYSTEM_AUTH_FAILED');
    assert.equal(heartbeatCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('public response scrubber never emits internal route, binding or Candidate identifiers', () => {
  const safe = myTmsGoogleControlInternals.safeResult({
    ok: true, state: 'RESERVED', operation_id: IDS.operation,
    candidate_code: 'CID1-ABCDE', reservation_token: 'safe-return-token',
    local_candidate_id: IDS.integration, agency_id: IDS.agency, data_plane_id: IDS.plane,
    route_version_id: IDS.route, registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
    internal_only: true
  }, '01K35Y7N7ER4QY5F7M8D9P0Q1R');
  assert.equal(safe.state, 'RESERVED');
  assert.equal(safe.local_candidate_id, undefined);
  assert.equal(safe.agency_id, undefined);
  assert.equal(safe.data_plane_id, undefined);
  assert.equal(safe.registry_binding_key, undefined);
});

test('target-switch drain facts use one recursively sorted canonical JSON authority', async () => {
  const facts = {
    uncertain_effect_count: 0,
    nested: { z: true, a: ['x', 2, null] },
    active_effect_count: 0,
    drain_snapshot_hash_hex: '6'.repeat(64)
  };
  const expected = '{"active_effect_count":0,"drain_snapshot_hash_hex":"'
    + '6'.repeat(64)
    + '","nested":{"a":["x",2,null],"z":true},"uncertain_effect_count":0}';
  assert.equal(myTmsGoogleControlInternals.canonicalJson(facts), expected);
  assert.equal(await sha256Hex(new TextEncoder().encode(expected)),
    await sha256Hex(new TextEncoder().encode(myTmsGoogleControlInternals.canonicalJson(facts))));
});

test('Google CID attach re-proves the exact Candidate and route before one private write', async () => {
  const calls = [];
  const originalFetch = globalThis.fetch;
  const environment = env({
    MYTMS_CONTROL_PLANE_ENABLED: 'TRUE',
    MYTMS_CONTROL_PLANE_URL: 'https://control.example.test',
    MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-only-service-role-key',
    CANDIDATE_DATA_PLANE_CLOUDTMS_TEST_ROUTE_CONTEXT_SECRET: 'exact-data-plane-route-secret',
    CLOUDTMS_PRIVATE: {
      async fetch(request) {
        const path = new URL(request.url).pathname;
        const body = JSON.parse(await request.text());
        calls.push({ path, body });
        if (path.endsWith('/candidates/match')) {
          return new Response(JSON.stringify({
            ok: true, match_state: 'EXACT', candidate_code_state: 'UNASSIGNED',
            local_candidate_id: IDS.integration
          }), { status: 200, headers: { 'content-type': 'application/json' } });
        }
        if (path.endsWith('/candidates/attach')) {
          return new Response(JSON.stringify({
            ok: true, state: 'ATTACHED', local_candidate_id: IDS.integration,
            candidate_code: 'CID1-ABCDE', idempotent_replay: false
          }), { status: 200, headers: { 'content-type': 'application/json' } });
        }
        return new Response('{}', { status: 404 });
      }
    }
  });
  globalThis.fetch = async (request) => {
    assert.match(new URL(request.url).pathname, /integration_target_resolve_v1$/);
    return new Response(JSON.stringify({
      ok: true, state: 'ACTIVE', environment: 'TEST', integration_id: IDS.integration,
      agency_id: IDS.agency, data_plane_id: IDS.plane, route_version_id: IDS.route,
      route_version: 7, target_generation: 4,
      registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST'
    }), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  try {
    const result = await myTmsGoogleControlInternals.attachCandidateCode({
      body: { operation_id: IDS.operation }, google_context: googleContext(),
      correlation_id: '01K35Y7N7ER4QY5F7M8D9P0Q1R'
    }, environment, {
      agency_id: IDS.agency, local_candidate_id: IDS.integration,
      candidate_code: 'CID1-ABCDE'
    }, {
      surname: 'Example', email: 'person@example.test', mobile: '447700900111',
      google_source_identity_hmac: '5'.repeat(64), source_hmac_key_version: 1
    });
    assert.equal(result.state, 'ATTACHED');
    assert.deepEqual(calls.map(({ path }) => path), [
      '/private/mytms-google-data/v1/candidates/match',
      '/private/mytms-google-data/v1/candidates/attach'
    ]);
    assert.equal(calls[1].body.local_candidate_id, IDS.integration);
    assert.equal(calls[1].body.candidate_code, 'CID1-ABCDE');
    assert.equal(calls[1].body.surname, 'Example');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('Google CID attach fails closed when the exact Candidate changes before commit', async () => {
  const originalFetch = globalThis.fetch;
  let attachCalls = 0;
  const environment = env({
    MYTMS_CONTROL_PLANE_ENABLED: 'TRUE',
    MYTMS_CONTROL_PLANE_URL: 'https://control.example.test',
    MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-only-service-role-key',
    CANDIDATE_DATA_PLANE_CLOUDTMS_TEST_ROUTE_CONTEXT_SECRET: 'exact-data-plane-route-secret',
    CLOUDTMS_PRIVATE: {
      async fetch(request) {
        if (new URL(request.url).pathname.endsWith('/candidates/attach')) attachCalls += 1;
        return new Response(JSON.stringify({ ok: true, match_state: 'NO_MATCH' }), {
          status: 200, headers: { 'content-type': 'application/json' }
        });
      }
    }
  });
  globalThis.fetch = async () => new Response(JSON.stringify({
    ok: true, state: 'ACTIVE', environment: 'TEST', integration_id: IDS.integration,
    agency_id: IDS.agency, data_plane_id: IDS.plane, route_version_id: IDS.route,
    route_version: 7, target_generation: 4,
    registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST'
  }), { status: 200, headers: { 'content-type': 'application/json' } });
  try {
    await assert.rejects(() => myTmsGoogleControlInternals.attachCandidateCode({
      body: { operation_id: IDS.operation }, google_context: googleContext(),
      correlation_id: '01K35Y7N7ER4QY5F7M8D9P0Q1R'
    }, environment, {
      agency_id: IDS.agency, local_candidate_id: IDS.integration,
      candidate_code: 'CID1-ABCDE'
    }, {
      surname: 'Example', email: 'person@example.test', mobile: '447700900111',
      google_source_identity_hmac: '5'.repeat(64), source_hmac_key_version: 1
    }), (error) => error?.code === 'MYTMS_GOOGLE_IDENTITY_CHANGED');
    assert.equal(attachCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('agency identity SQL matches exactly and attaches only to a blank or identical CID', async () => {
  const source = await readFile(new URL(
    '../supabase/repeatable/25082026_2028_candidate_google_conditional_runtime_correction_v1.sql', import.meta.url
  ), 'utf8');
  assert.match(source, /c\.active is true/);
  assert.match(source, /\^CID1-\[0-9A-HJKMNP-TV-Z\]\{5,160\}\$/);
  assert.match(source, /c\.last_name/);
  assert.match(source, /c\.email/);
  assert.match(source, /c\.phone/);
  assert.match(source, /min\(c\.id::text\)::uuid/i);
  assert.match(source, /set key_norm=v_code,updated_at=p_now_utc/i);
  assert.match(source, /nullif\(pg_catalog\.btrim\(coalesce\(c\.key_norm,''\)\),''\) is null/i);
  assert.match(source, /elsif v_existing=v_code then/i);
  assert.match(source, /GOOGLE_PROVISIONING_CID_CONFLICT/);
  assert.match(source, /revoke all .* public,anon,authenticated/is);
  assert.match(source, /grant execute .* service_role/is);
  assert.doesNotMatch(source, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
