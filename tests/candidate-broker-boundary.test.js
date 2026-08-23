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
import {
  candidateAppBackendInternals,
  handleCandidateAppRequest
} from '../broker/src/candidate-app-backend.js';

const ORIGIN = 'https://candidate.test.example';
const PRIVATE_SESSION_ID = '00000000-0000-4000-8000-000000000101';
const PUBLIC_SESSION_ID = '00000000-0000-5000-8000-000000000999';

test('public broker source has no Supabase, R2 or CloudTMS business-authority dependency', async () => {
  const source = await readFile(new URL('../candidate-broker/src/candidate-broker.js', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /SUPABASE_|SUPABASE_SERVICE_ROLE|\benv\.R2\b|candidate-app-backend\.js/);
  assert.match(source, /CLOUDTMS_PRIVATE/);
  const privateConfig = JSON.parse(await readFile(
    new URL('../candidate-private-api/wrangler.jsonc', import.meta.url), 'utf8'
  ));
  const publicConfig = JSON.parse(await readFile(
    new URL('../candidate-broker/wrangler.jsonc', import.meta.url), 'utf8'
  ));
  assert.equal(privateConfig.workers_dev, false);
  assert.equal(Object.prototype.hasOwnProperty.call(privateConfig.vars, 'SUPABASE_SERVICE_ROLE_KEY'), false);
  assert.equal(publicConfig.workers_dev, true);
  assert.equal(Object.prototype.hasOwnProperty.call(publicConfig, 'routes'), false);
  assert.deepEqual(
    publicConfig.vars.CANDIDATE_ALLOWED_ORIGINS.split(','),
    ['https://testmode.arthur-rai.co.uk', 'https://mytms-manager-review-test.kier-88a.workers.dev']
  );
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
    CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_SECRET: 'test-device-identity-secret-material-that-is-not-live',
    CANDIDATE_BROKER_MANAGER_HANDOFF_SECRET: 'test-manager-handoff-secret-material-that-is-not-live',
    CANDIDATE_BROKER_PUBLIC_SESSION_ID_SECRET: 'test-public-session-secret-material-that-is-not-live',
    CANDIDATE_BROKER_ACCESS_TOKEN_KEY_VERSION: '1',
    CANDIDATE_BROKER_REFRESH_TOKEN_KEY_VERSION: '1',
    CANDIDATE_BROKER_MANAGER_HANDOFF_KEY_VERSION: '1',
    CANDIDATE_BROKER_PUBLIC_SESSION_ID_KEY_VERSION: '1',
    CANDIDATE_BROKER_DEVICE_TOKEN_ENCRYPTION_KEY_VERSION: '1',
    CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_KEY_VERSION: '1',
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

function base64Url(bytes) {
  return Buffer.from(bytes).toString('base64url');
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

test('cross-origin manager documents expose only the immutable digest and request identity headers', () => {
  const response = candidateBrokerInternals.withCors(new Response('bytes', {
    headers: {
      'content-type': 'image/png',
      'x-cloudtms-content-sha256': 'a'.repeat(64),
      'x-private-routing-secret': 'must-not-be-exposed'
    }
  }), 'https://mytms-manager-review-test.kier-88a.workers.dev');
  assert.equal(
    response.headers.get('access-control-expose-headers'),
    'x-cloudtms-content-sha256, x-request-id'
  );
  assert.equal(
    response.headers.get('access-control-allow-origin'),
    'https://mytms-manager-review-test.kier-88a.workers.dev'
  );
  assert.doesNotMatch(response.headers.get('access-control-expose-headers'), /private/i);
});

test('public wrapping secrets and database-safe device versions are proved before private mutation', async () => {
  let privateCalls = 0;
  const env = brokerEnvironment(async () => {
    privateCalls += 1;
    return Response.json({ ok: true });
  });
  env.CANDIDATE_BROKER_ACCESS_TOKEN_KEY_VERSION = '2';
  env.CANDIDATE_BROKER_ACCESS_TOKEN_READ_KEY_VERSIONS = '1,2';
  const missingAccessSecret = await handleCandidateBrokerRequest(browserRequest(
    '/candidate-app/v1/auth/login', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email: 'person@example.test', password: 'not-a-real-password',
        idempotency_key: 'missing-public-wrapping-secret'
      })
    }
  ), env);
  assert.equal(missingAccessSecret.status, 503);
  assert.equal(
    (await missingAccessSecret.json()).error_code,
    'CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE'
  );
  assert.equal(privateCalls, 0);

  env.CANDIDATE_BROKER_ACCESS_TOKEN_KEY_VERSION = '1';
  env.CANDIDATE_BROKER_ACCESS_TOKEN_READ_KEY_VERSIONS = '1';
  const now = Math.floor(Date.now() / 1000);
  const publicAccess = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      public_session_id: PUBLIC_SESSION_ID, internal_access_token: 'private-access-token',
      iat: now, exp: now + 900
    }
  );
  delete env.CANDIDATE_BROKER_MANAGER_HANDOFF_SECRET;
  const missingManagerSecret = await handleCandidateBrokerRequest(browserRequest(
    '/candidate-app/v1/workflows/00000000-0000-4000-8000-000000000401/actions/select-phone-approval', {
      method: 'POST',
      headers: { authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json' },
      body: JSON.stringify({ generation: 1, idempotency_key: 'missing-phone-wrapping-secret' })
    }
  ), env);
  assert.equal(missingManagerSecret.status, 503);
  assert.equal(
    (await missingManagerSecret.json()).error_code,
    'CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE'
  );
  assert.equal(privateCalls, 0);

  env.CANDIDATE_BROKER_DEVICE_TOKEN_ENCRYPTION_KEY_VERSION = '32768';
  await assert.rejects(
    candidateBrokerInternals.encryptDeviceToken(env, 'device-token'),
    error => error?.code === 'CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE'
  );
});

test('deterministic v2 broker envelopes retain v1 read compatibility and reject tampering', async () => {
  const secret = 'test-envelope-compatibility-secret';
  const purpose = 'candidate-broker-access-v1';
  const payload = {
    typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
    public_session_id: PUBLIC_SESSION_ID, internal_access_token: 'private-access-token',
    iat: 1_786_534_400, exp: 1_786_535_300
  };
  const firstV2 = await candidateBrokerInternals.sealEnvelope(secret, purpose, payload);
  const replayV2 = await candidateBrokerInternals.sealEnvelope(secret, purpose, payload);
  const changedV2 = await candidateBrokerInternals.sealEnvelope(secret, purpose, {
    ...payload, exp: payload.exp + 1
  });
  assert.equal(firstV2, replayV2);
  assert.notEqual(firstV2, changedV2);
  assert.match(firstV2, /^v2\./);
  assert.deepEqual(await candidateBrokerInternals.openEnvelope(secret, purpose, firstV2), payload);
  assert.equal(await candidateBrokerInternals.openEnvelope(secret, `${purpose}-wrong`, firstV2), null);
  assert.equal(await candidateBrokerInternals.openEnvelope(secret, purpose, `${firstV2}.ignored`), null);
  const tampered = `${firstV2.slice(0, -1)}${firstV2.endsWith('A') ? 'B' : 'A'}`;
  assert.equal(await candidateBrokerInternals.openEnvelope(secret, purpose, tampered), null);

  const secretMaterial = await crypto.subtle.digest(
    'SHA-256', new TextEncoder().encode(`${purpose}:${secret}`)
  );
  const key = await crypto.subtle.importKey(
    'raw', secretMaterial, { name: 'AES-GCM' }, false, ['encrypt']
  );
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv, additionalData: new TextEncoder().encode(purpose) },
    key, new TextEncoder().encode(JSON.stringify(payload))
  );
  const v1 = `v1.${base64Url(iv)}.${base64Url(new Uint8Array(ciphertext))}`;
  assert.deepEqual(await candidateBrokerInternals.openEnvelope(secret, purpose, v1), payload);
});

test('versioned public credentials write v4, retain v1/v2/v3 readers and authenticate their key version', async () => {
  const envV1 = brokerEnvironment(async () => Response.json({ ok: true }));
  const envV2 = {
    ...envV1,
    CANDIDATE_BROKER_ACCESS_TOKEN_KEY_VERSION: '2',
    CANDIDATE_BROKER_ACCESS_TOKEN_READ_KEY_VERSIONS: '1,2',
    CANDIDATE_BROKER_REFRESH_TOKEN_KEY_VERSION: '2',
    CANDIDATE_BROKER_REFRESH_TOKEN_READ_KEY_VERSIONS: '1,2',
    CANDIDATE_BROKER_PUBLIC_SESSION_ID_KEY_VERSION: '2',
    CANDIDATE_BROKER_PUBLIC_SESSION_ID_READ_KEY_VERSIONS: '1,2',
    CANDIDATE_BROKER_ACCESS_TOKEN_SECRET_V2: 'test-access-v2-secret-material',
    CANDIDATE_BROKER_REFRESH_TOKEN_SECRET_V2: 'test-refresh-v2-secret-material',
    CANDIDATE_BROKER_PUBLIC_SESSION_ID_SECRET_V2: 'test-public-session-v2-secret-material'
  };
  const purpose = 'candidate-broker-access-v1';
  const payload = {
    typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
    public_session_id: PUBLIC_SESSION_ID, internal_access_token: 'private-access-token',
    iat: 1_786_534_400, exp: 1_786_535_300
  };
  const legacyV2 = await candidateBrokerInternals.sealEnvelope(
    envV1.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET, purpose, payload
  );
  const legacyV3 = await candidateBrokerInternals.sealEnvelope(
    envV1.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET, purpose, payload, 1
  );
  const versionedV1 = await candidateBrokerInternals.sealVersionedEnvelope(
    envV1, candidateBrokerInternals.credentialAuthorities.access, purpose, payload, 1
  );
  const versionedV2 = await candidateBrokerInternals.sealVersionedEnvelope(
    envV2, candidateBrokerInternals.credentialAuthorities.access, purpose, payload, 2
  );
  assert.equal((await candidateBrokerInternals.openVersionedEnvelope(
    envV2, candidateBrokerInternals.credentialAuthorities.access, purpose, legacyV2
  )).key_version, 1);
  assert.equal((await candidateBrokerInternals.openVersionedEnvelope(
    envV2, candidateBrokerInternals.credentialAuthorities.access, purpose, legacyV3
  )).envelope_version, 'v3');
  assert.equal((await candidateBrokerInternals.openVersionedEnvelope(
    envV2, candidateBrokerInternals.credentialAuthorities.access, purpose, versionedV1
  )).key_version, 1);
  assert.equal((await candidateBrokerInternals.openVersionedEnvelope(
    envV2, candidateBrokerInternals.credentialAuthorities.access, purpose, versionedV2
  )).key_version, 2);

  const privateResult = {
    ok: true,
    access_token: 'frozen-private-access',
    refresh_token: 'frozen-private-refresh',
    session_id: PRIVATE_SESSION_ID,
    access_expires_in_seconds: 900,
    issued_at_utc: '2026-08-12T10:00:00.000Z',
    expires_at_utc: '2026-09-11T10:00:00.000Z',
    absolute_expires_at_utc: '2099-11-10T10:00:00.000Z',
    public_credential_versions: {
      contract_version: 'CANDIDATE_PUBLIC_CREDENTIAL_VERSIONS_V1',
      access_key_version: 1,
      refresh_key_version: 1,
      public_session_key_version: 1
    }
  };
  const first = await candidateBrokerInternals.wrapPrivateSession(
    Response.json(privateResult), envV1
  );
  const replayAfterRotation = await candidateBrokerInternals.wrapPrivateSession(
    Response.json(privateResult), envV2
  );
  const firstBody = await first.json();
  const replayBody = await replayAfterRotation.json();
  assert.equal(firstBody.session_id, replayBody.session_id);
  assert.equal(firstBody.access_token, replayBody.access_token);
  assert.equal(firstBody.refresh_token, replayBody.refresh_token);
  assert.match(firstBody.access_token, /^v4\.1\./);

  const rollbackReader = { ...envV2, CANDIDATE_BROKER_ACCESS_TOKEN_KEY_VERSION: '1' };
  assert.deepEqual((await candidateBrokerInternals.openVersionedEnvelope(
    rollbackReader, candidateBrokerInternals.credentialAuthorities.access, purpose, versionedV2
  )).payload, payload);
  const retiredV2Reader = {
    ...rollbackReader,
    CANDIDATE_BROKER_ACCESS_TOKEN_READ_KEY_VERSIONS: '1'
  };
  assert.equal(await candidateBrokerInternals.openVersionedEnvelope(
    retiredV2Reader, candidateBrokerInternals.credentialAuthorities.access, purpose, versionedV2
  ), null);

  const aliasedSecrets = {
    ...envV1,
    CANDIDATE_BROKER_ACCESS_TOKEN_SECRET_V1: 'deliberately-aliased-version-secret',
    CANDIDATE_BROKER_ACCESS_TOKEN_KEY_VERSION: '1',
    CANDIDATE_BROKER_ACCESS_TOKEN_READ_KEY_VERSIONS: '1'
  };
  const issuedUnderOne = await candidateBrokerInternals.sealVersionedEnvelope(
    aliasedSecrets, candidateBrokerInternals.credentialAuthorities.access, purpose, payload, 1
  );
  const onlyVersionTwoReadable = {
    ...envV1,
    CANDIDATE_BROKER_ACCESS_TOKEN_SECRET_V2: 'deliberately-aliased-version-secret',
    CANDIDATE_BROKER_ACCESS_TOKEN_KEY_VERSION: '2',
    CANDIDATE_BROKER_ACCESS_TOKEN_READ_KEY_VERSIONS: '2'
  };
  delete onlyVersionTwoReadable.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET;
  delete onlyVersionTwoReadable.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET_V1;
  const relabelledAsTwo = issuedUnderOne.replace(/^v4\.1\./, 'v4.2.');
  assert.equal(await candidateBrokerInternals.openVersionedEnvelope(
    onlyVersionTwoReadable, candidateBrokerInternals.credentialAuthorities.access,
    purpose, issuedUnderOne
  ), null);
  assert.equal(await candidateBrokerInternals.openVersionedEnvelope(
    onlyVersionTwoReadable, candidateBrokerInternals.credentialAuthorities.access,
    purpose, relabelledAsTwo
  ), null);

  const invalidAliasedConfiguration = {
    ...aliasedSecrets,
    CANDIDATE_BROKER_ACCESS_TOKEN_SECRET_V2: 'deliberately-aliased-version-secret',
    CANDIDATE_BROKER_ACCESS_TOKEN_READ_KEY_VERSIONS: '1,2'
  };
  await assert.rejects(
    candidateBrokerInternals.sealVersionedEnvelope(
      invalidAliasedConfiguration, candidateBrokerInternals.credentialAuthorities.access,
      purpose, payload, 1
    ),
    error => error?.code === 'CANDIDATE_BROKER_KEY_VERSION_UNAVAILABLE'
  );
  const legacyV3Relabelled = (await candidateBrokerInternals.sealEnvelope(
    'deliberately-aliased-version-secret', purpose, payload, 1
  )).replace(/^v3\.1\./, 'v3.2.');
  assert.equal(await candidateBrokerInternals.openVersionedEnvelope(
    invalidAliasedConfiguration, candidateBrokerInternals.credentialAuthorities.access,
    purpose, legacyV3Relabelled
  ), null);
});

test('challenge start hides Candidate eligibility and account state', async () => {
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    return Response.json({ ok: false, error_code: 'CANDIDATE_ACCOUNT_NOT_ELIGIBLE' }, { status: 404 });
  });
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/auth/challenge/start', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      email: 'unknown@example.test', purpose: 'RESET', idempotency_key: 'unknown-reset-request'
    })
  }), env);
  assert.equal(response.status, 202);
  assert.deepEqual(await response.json(), { ok: true, accepted: true });
});

test('public challenge contract rejects invalid keys, preserves conflicts, and masks only eligibility', async () => {
  let privateCalls = 0;
  let receipt = null;
  const env = brokerEnvironment(async request => {
    privateCalls += 1;
    const body = await request.json();
    const semantic = `${body.email}|${body.purpose}|${body.challenge_id || ''}`;
    if (!receipt) {
      receipt = { key: body.idempotency_key, semantic };
      return Response.json({ ok: true, accepted: true }, { status: 202 });
    }
    if (body.idempotency_key === receipt.key && semantic !== receipt.semantic) {
      return Response.json({ ok: false, error_code: 'CANDIDATE_IDEMPOTENCY_CONFLICT' }, { status: 409 });
    }
    return Response.json({ ok: false, error_code: 'CANDIDATE_ACCOUNT_NOT_ELIGIBLE' }, { status: 404 });
  });
  const invoke = body => handleCandidateBrokerRequest(browserRequest(
    '/candidate-app/v1/auth/challenge/start', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body)
    }
  ), env);

  for (const invalid of [undefined, '', 'x'.repeat(201)]) {
    const response = await invoke({
      email: 'person@example.test', purpose: 'ACTIVATE', idempotency_key: invalid
    });
    assert.equal(response.status, 400);
    assert.equal((await response.json()).error_code, 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED');
  }
  assert.equal(privateCalls, 0);

  const first = await invoke({
    email: 'person@example.test', purpose: 'ACTIVATE', idempotency_key: 'challenge-contract-key'
  });
  const replay = await invoke({
    email: 'person@example.test', purpose: 'ACTIVATE', idempotency_key: 'challenge-contract-key'
  });
  const changedEmail = await invoke({
    email: 'changed@example.test', purpose: 'ACTIVATE', idempotency_key: 'challenge-contract-key'
  });
  const changedPurpose = await invoke({
    email: 'person@example.test', purpose: 'RESET', idempotency_key: 'challenge-contract-key'
  });
  assert.equal(first.status, 202);
  assert.equal(replay.status, 202);
  assert.equal(changedEmail.status, 409);
  assert.equal(changedPurpose.status, 409);
  assert.equal((await changedEmail.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
  assert.equal((await changedPurpose.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');

  receipt = null;
  const ineligibleEnv = brokerEnvironment(async () => Response.json(
    { ok: false, error_code: 'CANDIDATE_ACCOUNT_NOT_ELIGIBLE' }, { status: 404 }
  ));
  const ineligible = await handleCandidateBrokerRequest(browserRequest(
    '/candidate-app/v1/auth/challenge/start', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email: 'unknown@example.test', purpose: 'RESET', idempotency_key: 'unknown-account-key'
      })
    }
  ), ineligibleEnv);
  assert.equal(ineligible.status, 202);
  assert.deepEqual(await ineligible.json(), { ok: true, accepted: true });

  const outageEnv = brokerEnvironment(async () => Response.json(
    { ok: false, error_code: 'PRIVATE_FAILURE' }, { status: 503 }
  ));
  const outage = await handleCandidateBrokerRequest(browserRequest(
    '/candidate-app/v1/auth/challenge/start', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        email: 'person@example.test', purpose: 'RESET', idempotency_key: 'outage-key'
      })
    }
  ), outageEnv);
  assert.equal(outage.status, 502);
  assert.equal((await outage.json()).error_code, 'CANDIDATE_PRIVATE_API_UNAVAILABLE');
});

test('public challenge resend exposes durable private throttles and preserves retry timing', async () => {
  for (const [errorCode, retryAfter, terminal] of [
    ['CANDIDATE_CHALLENGE_RESEND_TOO_SOON', 37, false],
    ['CANDIDATE_CHALLENGE_RESEND_LIMIT', null, true]
  ]) {
    const env = brokerEnvironment(async request => {
      assert.equal(await verifyCandidatePrivateRequest(request.clone(), env), true);
      return Response.json({
        ok: false, error_code: errorCode,
        details: { ...(retryAfter ? { retry_after_seconds: retryAfter } : {}), terminal }
      }, {
        status: 429,
        headers: retryAfter ? { 'retry-after': String(retryAfter) } : {}
      });
    });
    const response = await handleCandidateBrokerRequest(browserRequest(
      '/candidate-app/v1/auth/challenge/resend', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          email: 'candidate@example.test', purpose: 'ACTIVATE',
          challenge_id: '00000000-0000-4000-8000-000000000041',
          idempotency_key: `public-throttle-${errorCode}`
        })
      }
    ), env);
    assert.equal(response.status, 429);
    assert.equal((await response.clone().json()).error_code, errorCode);
    assert.equal(response.headers.get('retry-after'), retryAfter ? String(retryAfter) : null);
    assert.equal((await response.json()).details.terminal, terminal);
  }
});

test('public logout unwraps the private bearer and replays one durable private mutation', async () => {
  const originalFetch = globalThis.fetch;
  const sessionId = '00000000-0000-4000-8000-000000000130';
  const otherSessionId = '00000000-0000-4000-8000-000000000131';
  const accountId = '00000000-0000-4000-8000-000000000132';
  const candidateId = '00000000-0000-4000-8000-000000000133';
  let receipt = null;
  let writes = 0;
  let privateEnv;
  let privateAuthorization = '';
  const privateDeps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_account_transition_v1');
      assert.equal(args.p_action, 'LOGOUT');
      if (args.p_payload.replay_probe_only === true
          && !args.p_payload.idempotency_request_sha256) {
        return receipt
          ? { replay_receipt_found: true, request_version_reserved: true, request_key_version: 1 }
          : { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
      }
      if (args.p_payload.replay_probe_only === true) {
        if (args.p_payload.idempotency_request_sha256 !== receipt.request_sha256) {
          throw new Error('CANDIDATE_IDEMPOTENCY_CONFLICT');
        }
        return { ...receipt.response, idempotent_replay: true };
      }
      writes += 1;
      receipt = {
        request_sha256: args.p_payload.idempotency_request_sha256,
        response: { ok: true, session_id: args.p_session_id, status: 'REVOKED' }
      };
      return receipt.response;
    }
  };
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request.clone(), env), true);
    privateAuthorization = request.headers.get('authorization') || '';
    const url = new URL(request.url);
    url.pathname = url.pathname.replace('/private/candidate-app/v1', '/candidate-app/v1');
    const body = await request.arrayBuffer();
    return handleCandidateAppRequest(new Request(url, {
      method: request.method, headers: request.headers, body
    }), privateEnv, {}, privateDeps);
  });
  privateEnv = {
    ...env,
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'public-logout-private-session-secret',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'public-logout-replay-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  globalThis.fetch = async url => {
    const requested = new URL(url);
    if (requested.pathname.endsWith('/candidate_app_sessions')) {
      const requestedId = requested.searchParams.get('id')?.replace(/^eq\./, '');
      return Response.json([{
        id: requestedId, account_id: accountId, environment: 'TEST',
        selected_candidate_id: candidateId, status: 'ACTIVE', rotation: 0,
        expires_at_utc: '2099-01-01T00:00:00.000Z',
        absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
      }]);
    }
    throw new Error(`unexpected REST operation ${requested.pathname}`);
  };
  const privateAccessFor = session => candidateAppBackendInternals.createAccessToken(
    privateEnv, { session_id: session, rotation: 0 }
  );
  const publicAccessFor = async session => candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET, 'candidate-broker-access-v1', {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      public_session_id: session === sessionId ? PUBLIC_SESSION_ID : '00000000-0000-5000-8000-000000000998',
      internal_access_token: await privateAccessFor(session),
      iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 900
    }
  );
  const invoke = async token => handleCandidateBrokerRequest(browserRequest(
    '/candidate-app/v1/auth/logout', {
      method: 'POST', headers: {
        authorization: `Bearer ${token}`, 'content-type': 'application/json'
      },
      body: JSON.stringify({ idempotency_key: 'public-logout-key' })
    }
  ), env);
  try {
    const token = await publicAccessFor(sessionId);
    const first = await invoke(token);
    const replay = await invoke(token);
    const changedSession = await invoke(await publicAccessFor(otherSessionId));
    const missing = await handleCandidateBrokerRequest(browserRequest(
      '/candidate-app/v1/auth/logout', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ idempotency_key: 'missing-access-logout' })
      }
    ), env);
    assert.equal(first.status, 200);
    assert.equal(replay.status, 200);
    assert.equal((await replay.json()).idempotent_replay, true);
    assert.equal(changedSession.status, 409);
    assert.equal((await changedSession.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    assert.equal(missing.status, 401);
    assert.equal(writes, 1);
    assert.match(privateAuthorization, /^Bearer\s+.+/);
    assert.equal(privateAuthorization.includes(token), false);
  } finally {
    globalThis.fetch = originalFetch;
  }
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
      issued_at_utc: new Date(Date.now() - 1000).toISOString(),
      expires_at_utc: new Date(Date.now() + 30 * 86_400_000).toISOString(),
      absolute_expires_at_utc: new Date(Date.now() + 90 * 86_400_000).toISOString()
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
  const access = await candidateBrokerInternals.openVersionedEnvelope(
    env, candidateBrokerInternals.credentialAuthorities.access,
    'candidate-broker-access-v1', body.access_token
  );
  assert.equal(access.payload.internal_access_token, 'private-access-token');
  assert.ok(captured.headers.get('x-cloudtms-service-signature'));
});

test('public login and password completion return byte-stable credentials after a lost response', async () => {
  const originalNow = Date.now;
  let wallClock = originalNow();
  Date.now = () => wallClock;
  const issuedAtUtc = new Date(wallClock - 1000).toISOString();
  const privateResult = {
    ok: true,
    access_token: 'stable-private-access-token',
    refresh_token: 'stable-private-refresh-token',
    session_id: PRIVATE_SESSION_ID,
    access_expires_in_seconds: 900,
    issued_at_utc: issuedAtUtc,
    expires_at_utc: new Date(Date.now() + 30 * 86_400_000).toISOString(),
    absolute_expires_at_utc: new Date(Date.now() + 90 * 86_400_000).toISOString()
  };
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    return Response.json(privateResult);
  });
  try {
    for (const [path, body] of [
      ['/candidate-app/v1/auth/login', {
        email: 'person@example.test', password: 'not-a-real-password', idempotency_key: 'public-login-replay'
      }],
      ['/candidate-app/v1/auth/password/complete', {
        challenge_id: '00000000-0000-4000-8000-000000000111',
        password: 'not-a-real-password', idempotency_key: 'public-password-replay'
      }]
    ]) {
      const invoke = () => handleCandidateBrokerRequest(browserRequest(path, {
        method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body)
      }), env);
      const first = await invoke();
      wallClock += 5000;
      const replay = await invoke();
      assert.equal(first.status, 200, path);
      assert.equal(replay.status, 200, path);
      const firstBody = await first.json();
      const replayBody = await replay.json();
      assert.equal(firstBody.session_id, replayBody.session_id, path);
      assert.equal(firstBody.access_token, replayBody.access_token, path);
      assert.equal(firstBody.refresh_token, replayBody.refresh_token, path);
      assert.equal(firstBody.issued_at_utc, replayBody.issued_at_utc, path);
      assert.equal(firstBody.expires_at_utc, replayBody.expires_at_utc, path);
      assert.equal(firstBody.absolute_expires_at_utc, replayBody.absolute_expires_at_utc, path);
      assert.match(firstBody.access_token, /^v4\.1\./, path);
      assert.match(firstBody.refresh_token, /^v4\.1\./, path);
    }
  } finally {
    Date.now = originalNow;
  }
});

test('real public broker and private API replay one durable login, activation and refresh result', async () => {
  const originalFetch = globalThis.fetch;
  const passwordVerifier = await candidateAppBackendInternals.derivePasswordVerifier(
    'real-public-private-password'
  );

  async function runJourney(action, publicPath, publicBody, oldPublicRefresh = null) {
    let receipt = null;
    let mutationCalls = 0;
    let storedRefreshHash = null;
    let privateEnv;
    const privateDeps = {
      routeAudience: 'PRIVATE',
      async rpc(name, args) {
        assert.equal(name, 'candidate_auth_account_transition_v1');
        assert.equal(args.p_action, action);
        if (args.p_payload.replay_probe_only === true
            && !args.p_payload.idempotency_request_sha256) {
          return receipt
            ? { replay_receipt_found: true, request_version_reserved: true, request_key_version: 1 }
            : { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
        }
        if (args.p_payload.replay_probe_only === true) {
          return { ...receipt, idempotent_replay: true };
        }
        mutationCalls += 1;
        const sessionId = action === 'REFRESH_SESSION'
          ? args.p_payload.new_session_id : args.p_session_id;
        storedRefreshHash = action === 'REFRESH_SESSION'
          ? args.p_payload.new_refresh_token_hash_hex : args.p_payload.refresh_token_hash_hex;
        receipt = {
          ok: true, session_id: sessionId,
          rotation: action === 'REFRESH_SESSION' ? 1 : 0,
          issued_at_utc: new Date(Date.now() - 1000).toISOString(),
          expires_at_utc: new Date(Date.now() + 30 * 86_400_000).toISOString(),
          absolute_expires_at_utc: new Date(Date.now() + 90 * 86_400_000).toISOString(),
          selected_candidate_id: null, selection_required: false,
          token_key_version: 1
        };
        return receipt;
      }
    };
    const env = brokerEnvironment(async request => {
      assert.equal(await verifyCandidatePrivateRequest(request.clone(), env), true);
      const url = new URL(request.url);
      url.pathname = url.pathname.replace('/private/candidate-app/v1', '/candidate-app/v1');
      const body = ['GET', 'HEAD'].includes(request.method) ? undefined : await request.arrayBuffer();
      return handleCandidateAppRequest(new Request(url, {
        method: request.method, headers: request.headers, body
      }), privateEnv, {}, privateDeps);
    });
    privateEnv = {
      ...env,
      CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'real-public-private-session-secret',
      CANDIDATE_AUTH_REPLAY_SECRET_V1: 'real-public-private-replay-secret',
      SUPABASE_URL: 'https://test.supabase.invalid',
      SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    };
    globalThis.fetch = async (url) => {
      const path = new URL(url).pathname;
      if (path.endsWith('/candidate_app_accounts')) return Response.json([{
        id: '00000000-0000-4000-8000-000000000121', environment: 'TEST', status: 'ACTIVE',
        password_scheme: passwordVerifier.scheme,
        password_scheme_version: passwordVerifier.scheme_version,
        password_salt: passwordVerifier.salt_hex,
        password_digest: passwordVerifier.digest_hex,
        password_params_json: passwordVerifier.params,
        locked_until_utc: null
      }]);
      throw new Error(`unexpected REST operation ${path}`);
    };
    const body = oldPublicRefresh
      ? { ...publicBody, refresh_token: oldPublicRefresh, session_id: PUBLIC_SESSION_ID }
      : publicBody;
    const invoke = () => handleCandidateBrokerRequest(browserRequest(publicPath, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body)
    }), env);
    const first = await invoke();
    const replay = await invoke();
    assert.equal(first.status, 200, action);
    assert.equal(replay.status, 200, action);
    const firstBody = await first.json();
    const replayBody = await replay.json();
    assert.equal(firstBody.session_id, replayBody.session_id, action);
    assert.equal(firstBody.access_token, replayBody.access_token, action);
    assert.equal(firstBody.refresh_token, replayBody.refresh_token, action);
    assert.equal(firstBody.access_expires_in_seconds, replayBody.access_expires_in_seconds, action);
    assert.equal(mutationCalls, 1, action);
    const internalRefresh = (await candidateBrokerInternals.openVersionedEnvelope(
      env, candidateBrokerInternals.credentialAuthorities.refresh,
      'candidate-broker-refresh-v1', firstBody.refresh_token
    )).payload;
    const digest = await crypto.subtle.digest(
      'SHA-256', new TextEncoder().encode(internalRefresh.internal_refresh_token)
    );
    assert.equal(
      Array.from(new Uint8Array(digest)).map(value => value.toString(16).padStart(2, '0')).join(''),
      storedRefreshHash,
      action
    );
  }

  try {
    await runJourney('LOGIN_SUCCESS', '/candidate-app/v1/auth/login', {
      email: 'candidate@example.test', password: 'real-public-private-password',
      idempotency_key: 'real-public-login-replay'
    });
    await runJourney('ACTIVATE_PASSWORD', '/candidate-app/v1/auth/password/complete', {
      challenge_id: '00000000-0000-4000-8000-000000000122',
      password: 'real-public-private-password', idempotency_key: 'real-public-activation-replay'
    });
    const refreshEnv = brokerEnvironment(async () => Response.json({ ok: true }));
    const now = Math.floor(Date.now() / 1000);
    const oldPublicRefresh = await candidateBrokerInternals.sealEnvelope(
      refreshEnv.CANDIDATE_BROKER_REFRESH_TOKEN_SECRET, 'candidate-broker-refresh-v1', {
        typ: 'candidate_broker_refresh', aud: 'cloudtms-candidate-refresh', env: 'TEST',
        public_session_id: PUBLIC_SESSION_ID, internal_session_id: PRIVATE_SESSION_ID,
        internal_refresh_token: 'real-public-old-refresh', iat: now, exp: now + 3600
      }
    );
    await runJourney('REFRESH_SESSION', '/candidate-app/v1/auth/refresh', {
      idempotency_key: 'real-public-refresh-replay'
    }, oldPublicRefresh);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('public Candidate access is unwrapped only for the private service call', async () => {
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request.clone(), env), true);
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
      public_session_id: PUBLIC_SESSION_ID, internal_access_token: 'private-access-token',
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
  let forwardedCredentialVersions = null;
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    const body = await request.json();
    assert.equal(body.refresh_token, 'private-old-refresh');
    assert.equal(body.session_id, PRIVATE_SESSION_ID);
    forwardedCredentialVersions = body.public_credential_versions;
    return Response.json({
      ok: true,
      access_token: 'private-new-access',
      refresh_token: 'private-new-refresh',
      session_id: '00000000-0000-4000-8000-000000000102',
      access_expires_in_seconds: 900,
      issued_at_utc: new Date(Date.now() - 1000).toISOString(),
      expires_at_utc: new Date(Date.now() + 30 * 86_400_000).toISOString(),
      absolute_expires_at_utc: new Date(Date.now() + 90 * 86_400_000).toISOString(),
      public_credential_versions: body.public_credential_versions
    });
  });
  env.CANDIDATE_BROKER_PUBLIC_SESSION_ID_KEY_VERSION = '2';
  env.CANDIDATE_BROKER_PUBLIC_SESSION_ID_READ_KEY_VERSIONS = '1,2';
  env.CANDIDATE_BROKER_PUBLIC_SESSION_ID_SECRET_V2 = 'rotated-public-session-secret';
  const now = Math.floor(Date.now() / 1000);
  const publicRefresh = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_REFRESH_TOKEN_SECRET,
    'candidate-broker-refresh-v1',
    {
      typ: 'candidate_broker_refresh', aud: 'cloudtms-candidate-refresh', env: 'TEST',
      public_session_id: PUBLIC_SESSION_ID, internal_session_id: PRIVATE_SESSION_ID,
      internal_refresh_token: 'private-old-refresh', public_session_key_version: 1,
      iat: now, exp: now + 3600
    }
  );
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/auth/refresh', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ refresh_token: publicRefresh, session_id: PUBLIC_SESSION_ID })
  }), env);
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.session_id, PUBLIC_SESSION_ID);
  assert.equal(forwardedCredentialVersions.public_session_key_version, 1);
  assert.equal(JSON.stringify(body).includes('private-new-refresh'), false);
  assert.equal(JSON.stringify(body).includes('private-new-access'), false);
});

test('public refresh returns the same successor credentials on an exact retry', async () => {
  const issuedAtUtc = new Date(Date.now() - 1000).toISOString();
  const absoluteExpiresAtUtc = new Date(Date.now() + 90 * 86_400_000).toISOString();
  const successorSessionId = '00000000-0000-4000-8000-000000000112';
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    return Response.json({
      ok: true, access_token: 'stable-private-successor-access',
      refresh_token: 'stable-private-successor-refresh', session_id: successorSessionId,
      access_expires_in_seconds: 900, issued_at_utc: issuedAtUtc,
      expires_at_utc: new Date(Date.now() + 30 * 86_400_000).toISOString(),
      absolute_expires_at_utc: absoluteExpiresAtUtc
    });
  });
  const now = Math.floor(Date.now() / 1000);
  const publicRefresh = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_REFRESH_TOKEN_SECRET, 'candidate-broker-refresh-v1', {
      typ: 'candidate_broker_refresh', aud: 'cloudtms-candidate-refresh', env: 'TEST',
      public_session_id: PUBLIC_SESSION_ID, internal_session_id: PRIVATE_SESSION_ID,
      internal_refresh_token: 'stable-private-old-refresh', iat: now, exp: now + 3600
    }
  );
  const invoke = () => handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/auth/refresh', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      refresh_token: publicRefresh, session_id: PUBLIC_SESSION_ID,
      idempotency_key: 'public-refresh-replay'
    })
  }), env);
  const first = await invoke();
  const replay = await invoke();
  assert.equal(first.status, 200);
  assert.equal(replay.status, 200);
  const firstBody = await first.json();
  const replayBody = await replay.json();
  assert.equal(firstBody.session_id, PUBLIC_SESSION_ID);
  assert.equal(firstBody.session_id, replayBody.session_id);
  assert.equal(firstBody.access_token, replayBody.access_token);
  assert.equal(firstBody.refresh_token, replayBody.refresh_token);
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
      access_expires_in_seconds: 900,
      issued_at_utc: new Date(now * 1000).toISOString()
    });
  });
  const now = Math.floor(Date.now() / 1000);
  const publicAccess = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      public_session_id: PUBLIC_SESSION_ID, internal_access_token: 'private-access-token',
      iat: now, exp: now + 900
    }
  );
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/account/select-candidate', {
    method: 'POST',
    headers: { authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json' },
    body: JSON.stringify({
      candidate_id: '00000000-0000-4000-8000-000000000333', idempotency_key: 'select-replay'
    })
  }), env);
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.session_id, PUBLIC_SESSION_ID);
  assert.equal(JSON.stringify(body).includes(PRIVATE_SESSION_ID), false);
  assert.equal(JSON.stringify(body).includes('private-selected-access'), false);
  const replay = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/account/select-candidate', {
    method: 'POST',
    headers: { authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json' },
    body: JSON.stringify({
      candidate_id: '00000000-0000-4000-8000-000000000333', idempotency_key: 'select-replay'
    })
  }), env);
  assert.equal(replay.status, 200);
  assert.equal((await replay.json()).access_token, body.access_token);
});

test('same-phone manager handoff is broker sealed and bound to the initiating public session and device', async () => {
  const workflowId = '00000000-0000-4000-8000-000000000401';
  const requestId = '00000000-0000-4000-8000-000000000402';
  const issuedAtUtc = new Date(Date.now() - 1000).toISOString();
  const expiresAtUtc = new Date(Date.now() + 20 * 60 * 1000).toISOString();
  let managerAuthorization = '';
  let frozenPhoneIdentity = null;
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request.clone(), env), true);
    if (new URL(request.url).pathname.endsWith('/actions/select-phone-approval')) {
      const privateBody = await request.json();
      const identity = JSON.stringify({
        workflow_id: workflowId,
        generation: privateBody.generation,
        binding: privateBody.payload.public_broker_binding
      });
      if (frozenPhoneIdentity && frozenPhoneIdentity !== identity) {
        return Response.json(
          { ok: false, error_code: 'CANDIDATE_IDEMPOTENCY_CONFLICT' }, { status: 409 }
        );
      }
      frozenPhoneIdentity = identity;
      return Response.json({
        ok: true, workflow_id: workflowId, approval_request_id: requestId,
        method: 'PHONE', manager_handoff_token: 'private-phone-manager-token',
        public_broker_binding: privateBody.payload.public_broker_binding,
        broker_handoff_key_version: privateBody.payload.broker_handoff_key_version,
        issued_at_utc: issuedAtUtc,
        expires_at_utc: expiresAtUtc
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

  const selectedReplay = await handleCandidateBrokerRequest(browserRequest(
    `/candidate-app/v1/workflows/${workflowId}/actions/select-phone-approval`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json',
        'x-candidate-device-id': 'test-device-a'
      },
      body: JSON.stringify({ generation: 1, idempotency_key: 'phone-handoff-test' })
    }
  ), env);
  assert.equal(selectedReplay.status, 201);
  assert.equal((await selectedReplay.json()).manager_handoff_token, handoff.manager_handoff_token);

  const changedDevice = await handleCandidateBrokerRequest(browserRequest(
    `/candidate-app/v1/workflows/${workflowId}/actions/select-phone-approval`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json',
        'x-candidate-device-id': 'test-device-b'
      },
      body: JSON.stringify({ generation: 1, idempotency_key: 'phone-handoff-test' })
    }
  ), env);
  const secondPublicAccess = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      public_session_id: 'second-public-phone-session', internal_access_token: 'private-access-token',
      iat: now, exp: now + 900
    }
  );
  const changedSession = await handleCandidateBrokerRequest(browserRequest(
    `/candidate-app/v1/workflows/${workflowId}/actions/select-phone-approval`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${secondPublicAccess}`, 'content-type': 'application/json',
        'x-candidate-device-id': 'test-device-a'
      },
      body: JSON.stringify({ generation: 1, idempotency_key: 'phone-handoff-test' })
    }
  ), env);
  assert.equal(changedDevice.status, 409);
  assert.equal(changedSession.status, 409);
  assert.equal((await changedDevice.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
  assert.equal((await changedSession.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');

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
  const forwarded = [];
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request, env), true);
    const body = await request.json();
    assert.equal(Object.prototype.hasOwnProperty.call(body, 'push_token'), false);
    assert.equal(body.push_provider, 'FCM');
    assert.match(body.push_token_ciphertext_hex, /^[0-9a-f]+$/);
    assert.match(body.push_token_identity_hmac, /^[0-9a-f]{64}$/);
    assert.equal(body.push_token_identity_key_version, 1);
    assert.deepEqual(body.push_token_identity_proofs, [{
      key_version: 1, identity_hmac: body.push_token_identity_hmac
    }]);
    assert.equal(JSON.stringify(body).includes(rawToken), false);
    assert.equal(await candidateBrokerInternals.decryptDeviceToken(
      env,
      body.push_token_ciphertext_hex
    ), rawToken);
    forwarded.push(body);
    return Response.json({ ok: true });
  });
  const now = Math.floor(Date.now() / 1000);
  const publicAccess = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      public_session_id: PUBLIC_SESSION_ID, internal_access_token: 'private-access-token',
      iat: now, exp: now + 900
    }
  );
  const response = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/account/push-token', {
    method: 'POST',
    headers: { authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json' },
    body: JSON.stringify({
      push_provider: 'fcm', push_token: rawToken, idempotency_key: 'same-push-registration'
    })
  }), env);
  assert.equal(response.status, 200);
  const replay = await handleCandidateBrokerRequest(browserRequest('/candidate-app/v1/account/push-token', {
    method: 'POST',
    headers: { authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json' },
    body: JSON.stringify({
      push_provider: 'fcm', push_token: rawToken, idempotency_key: 'same-push-registration'
    })
  }), env);
  assert.equal(replay.status, 200);
  assert.equal(forwarded.length, 2);
  assert.notEqual(forwarded[0].push_token_ciphertext_hex, forwarded[1].push_token_ciphertext_hex);
  assert.equal(forwarded[0].push_token_identity_hmac, forwarded[1].push_token_identity_hmac);
  assert.notEqual(
    forwarded[0].push_token_identity_hmac,
    await candidateBrokerInternals.deviceTokenIdentity(
      env, 'FCM', `${rawToken}-changed`, PUBLIC_SESSION_ID
    )
  );
  assert.notEqual(
    forwarded[0].push_token_identity_hmac,
    await candidateBrokerInternals.deviceTokenIdentity(
      env, 'APNS', rawToken, PUBLIC_SESSION_ID
    )
  );
  assert.notEqual(
    forwarded[0].push_token_identity_hmac,
    await candidateBrokerInternals.deviceTokenIdentity(
      env, 'FCM', rawToken, '00000000-0000-4000-8000-000000000099'
    )
  );
});

test('full public push replay accepts the same raw token and conflicts on a changed token', async () => {
  const originalFetch = globalThis.fetch;
  const sessionId = '00000000-0000-4000-8000-000000000601';
  const accountId = '00000000-0000-4000-8000-000000000602';
  const candidateId = '00000000-0000-4000-8000-000000000603';
  let receipt = null;
  let writes = 0;
  let privateEnv;
  const privateDeps = {
    routeAudience: 'PRIVATE',
    async rpc(name, args) {
      assert.equal(name, 'candidate_auth_account_transition_v1');
      assert.equal(args.p_action, 'REGISTER_PUSH_TOKEN');
      if (args.p_payload.replay_probe_only === true
          && !args.p_payload.idempotency_request_sha256) {
        return receipt
          ? {
              replay_receipt_found: true, request_version_reserved: true, request_key_version: 1,
              push_token_identity_key_version: receipt.push_token_identity_key_version
            }
          : { replay_receipt_found: false, request_version_reserved: true, request_key_version: 1 };
      }
      if (args.p_payload.replay_probe_only === true) {
        if (args.p_payload.idempotency_request_sha256 !== receipt.request_sha256) {
          throw new Error('CANDIDATE_IDEMPOTENCY_CONFLICT');
        }
        return { ...receipt.response, idempotent_replay: true };
      }
      writes += 1;
      receipt = {
        request_sha256: args.p_payload.idempotency_request_sha256,
        push_token_identity_key_version: args.p_payload.push_token_identity_key_version,
        response: { ok: true, session_id: sessionId, push_registered: true }
      };
      return receipt.response;
    }
  };
  const env = brokerEnvironment(async request => {
    assert.equal(await verifyCandidatePrivateRequest(request.clone(), env), true);
    const url = new URL(request.url);
    url.pathname = url.pathname.replace('/private/candidate-app/v1', '/candidate-app/v1');
    const body = ['GET', 'HEAD'].includes(request.method) ? undefined : await request.arrayBuffer();
    return handleCandidateAppRequest(new Request(url, {
      method: request.method, headers: request.headers, body
    }), privateEnv, {}, privateDeps);
  });
  privateEnv = {
    ...env,
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'full-public-push-private-session-secret',
    CANDIDATE_AUTH_REPLAY_SECRET_V1: 'full-public-push-replay-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const privateAccess = await candidateAppBackendInternals.createAccessToken(
    privateEnv, { session_id: sessionId, rotation: 0 }
  );
  const now = Math.floor(Date.now() / 1000);
  const publicAccess = await candidateBrokerInternals.sealEnvelope(
    env.CANDIDATE_BROKER_ACCESS_TOKEN_SECRET, 'candidate-broker-access-v1', {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      public_session_id: PUBLIC_SESSION_ID, internal_access_token: privateAccess,
      iat: now, exp: now + 900
    }
  );
  globalThis.fetch = async (url) => {
    const path = new URL(url).pathname;
    if (path.endsWith('/candidate_app_sessions')) return Response.json([{
      id: sessionId, account_id: accountId, environment: 'TEST',
      selected_candidate_id: candidateId, status: 'ACTIVE', rotation: 0,
      expires_at_utc: '2099-01-01T00:00:00.000Z',
      absolute_expires_at_utc: '2099-01-01T00:00:00.000Z'
    }]);
    throw new Error(`unexpected REST operation ${path}`);
  };
  const invoke = pushToken => handleCandidateBrokerRequest(browserRequest(
    '/candidate-app/v1/account/push-token', {
      method: 'POST',
      headers: { authorization: `Bearer ${publicAccess}`, 'content-type': 'application/json' },
      body: JSON.stringify({
        push_provider: 'FCM', push_token: pushToken, idempotency_key: 'full-public-push-replay'
      })
    }
  ), env);
  try {
    const first = await invoke('same-raw-public-push-token');
    const replay = await invoke('same-raw-public-push-token');
    env.CANDIDATE_BROKER_DEVICE_TOKEN_ENCRYPTION_KEY_VERSION = '2';
    env.CANDIDATE_BROKER_DEVICE_TOKEN_ENCRYPTION_READ_KEY_VERSIONS = '1,2';
    env.CANDIDATE_BROKER_DEVICE_TOKEN_SECRET_V2 = 'rotated-public-push-encryption-secret';
    const replayAfterEncryptionRotation = await invoke('same-raw-public-push-token');
    env.CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_KEY_VERSION = '2';
    env.CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_READ_KEY_VERSIONS = '1,2';
    env.CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_SECRET_V2 = 'rotated-public-push-identity-secret';
    const replayAfterIdentityRotation = await invoke('same-raw-public-push-token');
    const conflict = await invoke('changed-raw-public-push-token');
    assert.equal(first.status, 200);
    assert.equal(replay.status, 200);
    assert.equal((await replay.json()).push_registered, true);
    assert.equal(replayAfterEncryptionRotation.status, 200);
    assert.equal((await replayAfterEncryptionRotation.json()).push_registered, true);
    assert.equal(replayAfterIdentityRotation.status, 200);
    assert.equal((await replayAfterIdentityRotation.json()).push_registered, true);
    assert.equal(conflict.status, 409);
    assert.equal((await conflict.json()).error_code, 'CANDIDATE_IDEMPOTENCY_CONFLICT');
    assert.equal(writes, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
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

test('public broker logs only a closed private failure code while preserving public masking', async () => {
  const originalError = console.error;
  const entries = [];
  console.error = (...args) => entries.push(args);
  try {
    const response = await candidateBrokerInternals.publicSafePrivateResponse(Response.json({
      ok: false,
      error_code: 'MANAGER_REVIEW_PRIVATE_DIAGNOSTIC',
      detail: 'must-not-be-logged'
    }, { status: 503 }));
    assert.equal(response.status, 502);
    assert.equal((await response.json()).error_code, 'CANDIDATE_PRIVATE_API_UNAVAILABLE');
    assert.deepEqual(entries, [[
      '[candidate-broker] private request failed',
      { status: 503, error_code: 'MANAGER_REVIEW_PRIVATE_DIAGNOSTIC' }
    ]]);
  } finally {
    console.error = originalError;
  }
});
