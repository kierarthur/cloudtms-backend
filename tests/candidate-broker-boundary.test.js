import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  candidateBrokerInternals,
  handleCandidateBrokerRequest
} from '../candidate-broker/src/candidate-broker.js';
import {
  signCandidatePrivateRequest,
  verifyCandidatePrivateRequest
} from '../broker/src/candidate-service-auth.js';
import candidatePrivateWorker from '../broker/src/candidate-private-worker.js';
import cloudTmsWorker from '../broker/src/index.js';

const ORIGIN = 'https://candidate.test.example';
const PRIVATE_SESSION_ID = '00000000-0000-4000-8000-000000000101';

test('public broker source has no Supabase, R2 or CloudTMS business-authority dependency', async () => {
  const source = await readFile(new URL('../candidate-broker/src/candidate-broker.js', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /SUPABASE_|SUPABASE_SERVICE_ROLE|\benv\.R2\b|candidate-app-backend\.js/);
  assert.match(source, /CLOUDTMS_PRIVATE/);
  const privateConfig = JSON.parse(await readFile(
    new URL('../candidate-private-api/wrangler.jsonc', import.meta.url), 'utf8'
  ));
  assert.equal(privateConfig.workers_dev, false);
  assert.equal(Object.prototype.hasOwnProperty.call(privateConfig.vars, 'SUPABASE_SERVICE_ROLE_KEY'), false);
});

function limiter(success = true) {
  return { async limit() { return { success }; } };
}

function brokerEnvironment(privateFetch) {
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_ALLOWED_ORIGINS: ORIGIN,
    CANDIDATE_ALLOW_NATIVE_CLIENTS: 'true',
    CANDIDATE_BROKER_ACCESS_TOKEN_SECRET: 'test-access-secret-material-that-is-not-live',
    CANDIDATE_BROKER_REFRESH_TOKEN_SECRET: 'test-refresh-secret-material-that-is-not-live',
    CANDIDATE_BROKER_DEVICE_TOKEN_SECRET: 'test-device-secret-material-that-is-not-live',
    CANDIDATE_BROKER_MANAGER_HANDOFF_SECRET: 'test-manager-handoff-secret-material-that-is-not-live',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'test-service-secret-material-that-is-not-live',
    CANDIDATE_GENERAL_RATE_LIMIT: limiter(),
    CANDIDATE_AUTH_RATE_LIMIT: limiter(),
    CANDIDATE_MANAGER_RATE_LIMIT: limiter(),
    CANDIDATE_UPLOAD_RATE_LIMIT: limiter(),
    CLOUDTMS_PRIVATE: { fetch: privateFetch }
  };
}

function browserRequest(path, init = {}) {
  const headers = new Headers(init.headers || {});
  headers.set('origin', ORIGIN);
  headers.set('cf-connecting-ip', '192.0.2.10');
  return new Request(`https://candidate-api.test.example${path}`, { ...init, headers });
}

test('public broker rejects an unexpected browser origin before private API access', async () => {
  let privateCalls = 0;
  const env = brokerEnvironment(async () => {
    privateCalls += 1;
    return new Response('{}');
  });
  const response = await handleCandidateBrokerRequest(new Request(
    'https://candidate-api.test.example/candidate-app/v1/auth/login',
    {
      method: 'POST',
      headers: { origin: 'https://attacker.example', 'content-type': 'application/json' },
      body: JSON.stringify({ email: 'person@example.test', password: 'not-a-real-password' })
    }
  ), env);
  assert.equal(response.status, 403);
  assert.equal((await response.json()).error_code, 'CANDIDATE_ORIGIN_NOT_ALLOWED');
  assert.equal(privateCalls, 0);
});

test('browser preflight is exact-origin and never wildcard', async () => {
  const env = brokerEnvironment(async () => Response.json({ ok: true }));
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/bootstrap', {
    method: 'OPTIONS',
    headers: {
      'access-control-request-method': 'GET',
      'access-control-request-headers': 'authorization, x-request-id'
    }
  }), env);
  assert.equal(response.status, 204);
  assert.equal(response.headers.get('access-control-allow-origin'), ORIGIN);
  assert.notEqual(response.headers.get('access-control-allow-origin'), '*');
});

test('challenge start hides Candidate eligibility and account state', async () => {
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    return Response.json({ ok: false, error_code: 'CANDIDATE_ACCOUNT_NOT_ELIGIBLE' }, { status: 404 });
  });
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/auth/challenge/start', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ email: 'unknown@example.test', purpose: 'RESET' })
  }), env);
  assert.equal(response.status, 202);
  assert.deepEqual(await response.json(), { ok: true, accepted: true });
});

test('public broker wraps private Candidate tokens and signs the service-bound request', async () => {
  let captured;
  const env = brokerEnvironment(async request => {
    captured = request;
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    assert.equal(new URL(request.url).pathname, '/private/candidate-app/v1/auth/login');
    return Response.json({
      ok: true,
      access_token: 'private-access-token',
      refresh_token: 'private-refresh-token',
      session_id: PRIVATE_SESSION_ID,
      access_expires_in_seconds: 900,
      absolute_expires_at_utc: new Date(Date.now() + 86_400_000).toISOString()
    });
  });
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/auth/login', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ email: 'person@example.test', password: 'not-a-real-password' })
  }), env);
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('access-control-allow-origin'), ORIGIN);
  const body = await response.json();
  assert.notEqual(body.access_token, 'private-access-token');
  assert.notEqual(body.refresh_token, 'private-refresh-token');
  assert.equal(JSON.stringify(body).includes('private-access-token'), false);
  assert.equal(JSON.stringify(body).includes('private-refresh-token'), false);
  const access = await candidateBrokerInternals.openEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    body.access_token
  );
  assert.equal(access.internal_access_token, 'private-access-token');
  assert.ok(captured.headers.get('x-cloudtms-service-signature'));
});

test('public Candidate access is unwrapped only for the private service call', async () => {
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    assert.equal(request.headers.get('authorization'), 'Bearer private-access-token');
    assert.equal(new URL(request.url).pathname, '/private/candidate-app/v1/bootstrap');
    return Response.json({ ok: true, candidate: { id: 'safe-candidate-id' } });
  });
  const now = Math.floor(Date.now() / 1000);
  const publicAccess = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      public_session_id: 'public-session', internal_access_token: 'private-access-token',
      iat: now, exp: now + 900
    }
  );
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/bootstrap', {
    headers: { authorization: `Bearer ${publicAccess}` }
  }), env);
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { ok: true, candidate: { id: 'safe-candidate-id' } });
});

test('public refresh envelope preserves private refresh rotation without exposing it', async () => {
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    const body = await request.json();
    assert.equal(body.refresh_token, 'private-old-refresh');
    assert.equal(body.session_id, PRIVATE_SESSION_ID);
    return Response.json({
      ok: true,
      access_token: 'private-new-access',
      refresh_token: 'private-new-refresh',
      session_id: '00000000-0000-4000-8000-000000000102',
      access_expires_in_seconds: 900,
      absolute_expires_at_utc: new Date(Date.now() + 86_400_000).toISOString()
    });
  });
  const now = Math.floor(Date.now() / 1000);
  const publicRefresh = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_REFRESH_TOKEN_SECRET,
    'candidate-broker-refresh-v1',
    {
      typ: 'candidate_broker_refresh', aud: 'cloudtms-candidate-refresh', env: 'TEST',
      public_session_id: 'public-session', internal_session_id: PRIVATE_SESSION_ID,
      internal_refresh_token: 'private-old-refresh', iat: now, exp: now + 3600
    }
  );
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/auth/refresh', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ refresh_token: publicRefresh, session_id: 'public-session' })
  }), env);
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.session_id, 'public-session');
  assert.equal(JSON.stringify(body).includes('private-new-refresh'), false);
  assert.equal(JSON.stringify(body).includes('private-new-access'), false);
});

test('OpenAPI public path inventory and pagination names match the private router', async () => {
  const source = await readFile(new URL('../broker/src/candidate-app-backend.js', import.meta.url), 'utf8');
  const openapi = await readFile(new URL('../docs/candidate-app/CANDIDATE_API_OPENAPI_V1.yaml', import.meta.url), 'utf8');
  const routerPaths = new Set();
  for (const match of source.matchAll(/(?:path\s*===|routeMatch\(path,)\s*`\$\{(CANDIDATE_PREFIX|MANAGER_PREFIX)\}([^`]*)`/g)) {
    const prefix = match[1] === 'CANDIDATE_PREFIX' ? '/candidate-app/v1' : '/candidate-manager/v1';
    const route = `${prefix}${match[2].replace(/:([A-Za-z][A-Za-z0-9]*)/g, '{$1}')}`;
    if (route === '/candidate-manager/v1/workflows/{workflowId}/{action}') {
      for (const action of ['start', 'progress', 'approve', 'refuse']) {
        routerPaths.add(route.replace('{action}', action));
      }
    } else {
      routerPaths.add(route);
    }
  }
  const documentedPaths = new Set(
    [...openapi.matchAll(/^  (\/(?:candidate-app|candidate-manager)\/v1[^:]*):\s*$/gm)].map(match => match[1])
  );
  assert.deepEqual([...documentedPaths].sort(), [...routerPaths].sort());
  assert.doesNotMatch(openapi, /name:\s*page_size/i);
  assert.match(openapi, /Limit:\s*\{name:\s*limit/i);
  assert.match(openapi, /FromDate:\s*\{name:\s*from[\s\S]*ToDate:\s*\{name:\s*to/i);
  assert.match(openapi, /cancel-manager-handoff/i);
  const managerMethods = candidateBrokerInternals.managerActionMethods;
  for (const [action, method] of Object.entries(managerMethods)) {
    const pattern = new RegExp(`^  /candidate-manager/v1/workflows/\\{workflowId\\}/${action}:\\r?\\n    ${method.toLowerCase()}:`, 'm');
    assert.match(openapi, pattern, `${action} must be documented as ${method}`);
  }
});

test('public broker rejects wrong manager methods before the private service binding', async () => {
  let privateCalls = 0;
  const env = brokerEnvironment(async () => {
    privateCalls += 1;
    return Response.json({ ok: true });
  });
  const workflowId = '00000000-0000-4000-8000-000000000201';
  for (const [action, method] of [['start', 'POST'], ['progress', 'GET'], ['approve', 'GET'], ['refuse', 'GET']]) {
    const response = await handleCandidateBrokerRequest(browserRequest(
      `/candidate-manager/v1/workflows/${workflowId}/${action}`,
      { method, headers: { authorization: 'Bearer deliberately-invalid-manager-token' } }
    ), env);
    assert.equal(response.status, 405);
    assert.equal((await response.json()).error_code, 'METHOD_NOT_ALLOWED');
  }
  assert.equal(privateCalls, 0);
});

test('candidate selection never exposes or substitutes the private database session identity', async () => {
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    return Response.json({
      ok: true,
      access_token: 'private-selected-access',
      session_id: PRIVATE_SESSION_ID,
      selected_candidate_id: '00000000-0000-4000-8000-000000000333',
      access_expires_in_seconds: 900
    });
  });
  const now = Math.floor(Date.now() / 1000);
  const publicAccess = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      public_session_id: 'public-session', internal_access_token: 'private-access-token',
      iat: now, exp: now + 900
    }
  );
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/account/select-candidate', {
    method: 'POST',
    headers: { authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json' },
    body: JSON.stringify({ candidate_id: '00000000-0000-4000-8000-000000000333' })
  }), env);
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.session_id, 'public-session');
  assert.equal(JSON.stringify(body).includes(PRIVATE_SESSION_ID), false);
  assert.equal(JSON.stringify(body).includes('private-selected-access'), false);
});

test('same-phone manager handoff is broker sealed and bound to the initiating public session and device', async () => {
  const workflowId = '00000000-0000-4000-8000-000000000401';
  const requestId = '00000000-0000-4000-8000-000000000402';
  let managerAuthorization = '';
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    if (new URL(request.url).pathname.endsWith('/actions/select-phone-approval')) {
      return Response.json({
        ok: true, workflow_id: workflowId, approval_request_id: requestId,
        method: 'PHONE', manager_handoff_token: 'private-phone-manager-token',
        expires_at_utc: new Date(Date.now() + 20 * 60 * 1000).toISOString()
      }, { status: 201 });
    }
    managerAuthorization = request.headers.get('authorization');
    return Response.json({ ok: true, method: 'PHONE', workflow_id: workflowId });
  });
  const now = Math.floor(Date.now() / 1000);
  const publicAccess = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      public_session_id: 'public-phone-session', internal_access_token: 'private-access-token',
      iat: now, exp: now + 900
    }
  );
  const selected = await handleCandidateBrokerRequest(browserRequest(
    `/candidate-app/v1/workflows/${workflowId}/actions/select-phone-approval`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json',
        'x-candidate-device-id': 'test-device-a'
      },
      body: JSON.stringify({ generation: 1, idempotency_key: 'phone-handoff-test' })
    }
  ), env);
  assert.equal(selected.status, 201);
  const handoff = await selected.json();
  assert.notEqual(handoff.manager_handoff_token, 'private-phone-manager-token');
  assert.equal(JSON.stringify(handoff).includes('private-phone-manager-token'), false);

  const manager = await handleCandidateBrokerRequest(browserRequest(
    `/candidate-manager/v1/workflows/${workflowId}/start`, {
      headers: {
        authorization: `Bearer ${handoff.manager_handoff_token}`,
        'x-candidate-session-token': publicAccess,
        'x-candidate-device-id': 'test-device-a'
      }
    }
  ), env);
  assert.equal(manager.status, 200);
  assert.equal(managerAuthorization, 'Bearer private-phone-manager-token');
});

test('device registration never forwards the raw push token to CloudTMS private API', async () => {
  const rawToken = 'raw-device-token-that-must-stay-at-the-broker';
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    const body = await request.json();
    assert.equal(Object.prototype.hasOwnProperty.call(body, 'push_token'), false);
    assert.equal(body.push_provider, 'FCM');
    assert.match(body.push_token_ciphertext_hex, /^[0-9a-f]+$/);
    assert.equal(JSON.stringify(body).includes(rawToken), false);
    assert.equal(await candidateBrokerInternals.decryptDeviceToken(
      env,
      body.push_token_ciphertext_hex
    ), rawToken);
    return Response.json({ ok: true });
  });
  const now = Math.floor(Date.now() / 1000);
  const publicAccess = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      public_session_id: 'public-session', internal_access_token: 'private-access-token',
      iat: now, exp: now + 900
    }
  );
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/account/push-token', {
    method: 'POST',
    headers: { authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json' },
    body: JSON.stringify({ push_provider: 'fcm', push_token: rawToken })
  }), env);
  assert.equal(response.status, 200);
});

test('public rate limits fail closed before private work', async () => {
  let privateCalls = 0;
  const env = brokerEnvironment(async () => {
    privateCalls += 1;
    return Response.json({ ok: true });
  });
  env.CANDIDATE_GENERAL_RATE_LIMIT = limiter(false);
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/auth/challenge/start', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ email: 'person@example.test' })
  }), env);
  assert.equal(response.status, 429);
  assert.equal(response.headers.get('retry-after'), '60');
  assert.equal(privateCalls, 0);
});

test('service request authentication detects body tampering and stale timestamps', async () => {
  const env = brokerEnvironment(async () => Response.json({ ok: true }));
  const original = new Request('https://private.internal/private/candidate-app/v1/auth/login', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"value":1}'
  });
  const signed = await signCandidatePrivateRequest(original, env);
  assert.equal(await verifyCandidatePrivateRequest(signed.clone(), env), true);
  const tampered = new Request(signed.url, {
    method: signed.method,
    headers: signed.headers,
    body: '{"value":2}'
  });
  assert.equal(await verifyCandidatePrivateRequest(tampered, env), false);
  const timestamp = Number(signed.headers.get('x-cloudtms-service-timestamp'));
  assert.equal(await verifyCandidatePrivateRequest(signed, env, timestamp + 301), false);

  const signedQuery = await signCandidatePrivateRequest(new Request(
    'https://private.internal/private/candidate-app/v1/timesheets?cursor=first',
    { headers: { 'idempotency-key': 'safe-key' } }
  ), env);
  const changedQuery = new Request(
    'https://private.internal/private/candidate-app/v1/timesheets?cursor=second',
    { headers: signedQuery.headers }
  );
  assert.equal(await verifyCandidatePrivateRequest(changedQuery, env), false);
  const changedHeader = new Request(signedQuery.url, { headers: signedQuery.headers });
  changedHeader.headers.set('idempotency-key', 'different-key');
  assert.equal(await verifyCandidatePrivateRequest(changedHeader, env), false);
});

test('private Worker has no public Candidate route and requires a signed service request', async () => {
  const nonceObjects = new Map();
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_APP_PUBLIC_URL: ORIGIN,
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'test-service-secret-material-that-is-not-live',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-session-secret-material-that-is-not-live',
    CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET: 'test-challenge-secret-material-that-is-not-live',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-upload-secret-material-that-is-not-live',
    SUPABASE_URL: 'https://test-project.example.supabase.co',
    SUPABASE_SERVICE_ROLE_KEY: 'not-a-real-service-role-key',
    R2: {
      async put(key, value, options = {}) {
        if (options.onlyIf?.etagDoesNotMatch === '*' && nonceObjects.has(key)) return null;
        const row = { key, uploaded: new Date(), customMetadata: options.customMetadata || {} };
        nonceObjects.set(key, row);
        return row;
      },
      async list() { return { objects: Array.from(nonceObjects.values()), truncated: false }; },
      async delete(keys) { for (const key of Array.isArray(keys) ? keys : [keys]) nonceObjects.delete(key); }
    }
  };
  const publicResponse = await candidatePrivateWorker.fetch(
    new Request('https://private.internal/candidate-app/v1/bootstrap'), env, {}
  );
  assert.equal(publicResponse.status, 404);
  const unsignedResponse = await candidatePrivateWorker.fetch(
    new Request('https://private.internal/private/candidate-app/v1/health'), env, {}
  );
  assert.equal(unsignedResponse.status, 401);
  const signed = await signCandidatePrivateRequest(
    new Request('https://private.internal/private/candidate-app/v1/health'), env
  );
  const healthy = await candidatePrivateWorker.fetch(signed, env, {});
  assert.equal(healthy.status, 200);
  const replay = await candidatePrivateWorker.fetch(signed.clone(), env, {});
  assert.equal(replay.status, 401);
  assert.equal((await replay.json()).error_code, 'CANDIDATE_PRIVATE_SERVICE_REPLAY_REJECTED');
});

test('normal CloudTMS Worker rejects public Candidate routes before global preflight', async () => {
  const request = new Request('https://normal-cloudtms.test/candidate-app/v1/bootstrap', {
    method: 'OPTIONS',
    headers: {
      origin: ORIGIN,
      'access-control-request-method': 'GET'
    }
  });
  const response = await cloudTmsWorker.fetch(request, {}, {});
  assert.equal(response.status, 404);
  assert.equal(response.headers.get('access-control-allow-origin'), null);
});
