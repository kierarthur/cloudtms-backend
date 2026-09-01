import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  candidateRouteContextInternals,
  signCandidateRouteContext,
  verifyCandidateRouteContext
} from '../broker/src/candidate-route-context.js';
import {
  signCandidatePrivateRequest,
  verifyCandidatePrivateRequest
} from '../broker/src/candidate-service-auth.js';
import {
  CANDIDATE_DATA_PLANE_BINDING_MANIFEST,
  candidateDataPlaneRegistryEntry
} from '../candidate-broker/src/candidate-data-plane-registry.generated.js';
import {
  controlPlaneClientInternals,
  controlPlaneEnabled,
  globalAuthCutoverEnabled
} from '../candidate-broker/src/control-plane-client.js';
import {
  candidateBrokerInternals,
  handleCandidateBrokerRequest
} from '../candidate-broker/src/candidate-broker.js';

const IDS = Object.freeze({
  account: '10000000-0000-4000-8000-000000000001',
  session: '10000000-0000-4000-8000-000000000002',
  family: '10000000-0000-4000-8000-000000000007',
  membership: '10000000-0000-4000-8000-000000000003',
  agency: '10000000-0000-4000-8000-000000000004',
  candidate: '10000000-0000-4000-8000-000000000005',
  dataPlane: '10000000-0000-4000-8000-000000000006'
});

test('control-plane failure diagnostics expose only bounded non-secret routing facts', () => {
  const diagnostic = controlPlaneClientInternals.safeControlPlaneDiagnostic(
    404,
    'identity',
    'global_invitation_challenge_start_v1',
    {
      code: 'PGRST202',
      message: 'sensitive upstream message',
      details: 'sensitive upstream details',
      hint: 'sensitive upstream hint'
    }
  );
  assert.deepEqual(diagnostic, {
    status: 404,
    schema: 'identity',
    function_name: 'global_invitation_challenge_start_v1',
    upstream_code: 'PGRST202'
  });
  assert.equal(JSON.stringify(diagnostic).includes('sensitive'), false);
  assert.equal(
    controlPlaneClientInternals.safeControlPlaneDiagnostic(
      400, 'identity', 'global_invitation_challenge_start_v1', { code: 'unsafe free text' }
    ).upstream_code,
    'UNCLASSIFIED'
  );
});

function routeEnvironment(overrides = {}) {
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_AGENCY_ID: IDS.agency,
    CANDIDATE_DATA_PLANE_ID: IDS.dataPlane,
    CANDIDATE_ROUTE_VERSION: '7',
    CANDIDATE_ROUTE_CONTEXT_SECRET: 'test-route-context-secret-that-is-not-live',
    CANDIDATE_ROUTE_CONTEXT_READ_KEY_VERSIONS: '1',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'test-private-service-secret-that-is-not-live',
    ...overrides
  };
}

function routeContext(now = new Date('2026-08-21T16:00:00.000Z')) {
  return {
    v: 1,
    aud: 'candidate-private-api',
    operation_id: 'getCandidateBootstrap',
    environment: 'TEST',
    global_account_id: IDS.account,
    global_session_id: IDS.session,
    global_session_family_id: IDS.family,
    membership_id: IDS.membership,
    membership_generation: 3,
    agency_id: IDS.agency,
    agency_candidate_id: IDS.candidate,
    data_plane_id: IDS.dataPlane,
    route_version: 7,
    session_epoch: 12,
    issued_at_utc: now.toISOString(),
    expires_at_utc: new Date(now.getTime() + 5 * 60_000).toISOString(),
    key_version: 1
  };
}

test('signed route context is canonical, deployment-bound and tamper-evident', async () => {
  const now = new Date('2026-08-21T16:00:00.000Z');
  const env = routeEnvironment();
  const signed = await signCandidateRouteContext(routeContext(now), {
    secret: env.CANDIDATE_ROUTE_CONTEXT_SECRET,
    keyVersion: 1,
    nowMilliseconds: now.getTime()
  });
  assert.match(signed.sha256, /^[a-f0-9]{64}$/);
  const request = new Request('https://private.invalid/private/candidate-app/v1/bootstrap', {
    headers: {
      'x-cloudtms-route-context': signed.envelope,
      'x-cloudtms-route-context-sha256': signed.sha256
    }
  });
  const verified = await verifyCandidateRouteContext(request, env, now.getTime());
  assert.equal(verified.context.membership_generation, 3);
  assert.equal(verified.context.session_epoch, 12);
  assert.equal(verified.context.global_session_family_id, IDS.family);

  const envelopeParts = signed.envelope.split('.');
  const base64UrlAlphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
  const finalSignatureIndex = base64UrlAlphabet.indexOf(envelopeParts[2].at(-1));
  assert.equal(finalSignatureIndex % 4, 0);
  const nonCanonicalSignature = `${envelopeParts[2].slice(0, -1)}${
    base64UrlAlphabet[finalSignatureIndex + 1]
  }`;
  const nonCanonicalEnvelope = `${envelopeParts[0]}.${envelopeParts[1]}.${nonCanonicalSignature}`;
  const nonCanonicalRequest = new Request(request, { headers: {
    ...Object.fromEntries(request.headers),
    'x-cloudtms-route-context': nonCanonicalEnvelope,
    'x-cloudtms-route-context-sha256': await candidateRouteContextInternals.sha256Hex(nonCanonicalEnvelope)
  } });
  assert.equal(
    await verifyCandidateRouteContext(nonCanonicalRequest, env, now.getTime()),
    null,
    'non-canonical base64url must not provide an alternate spelling of a signed route context'
  );

  assert.equal(await verifyCandidateRouteContext(request, {
    ...env, CANDIDATE_AGENCY_ID: '20000000-0000-4000-8000-000000000004'
  }, now.getTime()), null);
  assert.equal(await verifyCandidateRouteContext(request, {
    ...env, CANDIDATE_ROUTE_VERSION: '8'
  }, now.getTime()), null);

  const tampered = new Request(request, { headers: {
    ...Object.fromEntries(request.headers),
    'x-cloudtms-route-context': `${signed.envelope.slice(0, -1)}A`
  } });
  assert.equal(await verifyCandidateRouteContext(tampered, env, now.getTime()), null);
});

test('manager EMAIL route context v2 is canonical, deployment-bound and authority-closed', async () => {
  const now = new Date('2026-08-21T16:00:00.000Z');
  const env = routeEnvironment();
  const context = {
    v: 2, typ: 'cloudtms-route-context-v2', aud: 'candidate-private-api',
    authority_kind: 'MANAGER_EMAIL', operation_id: 'startManagerReview', environment: 'TEST',
    agency_id: IDS.agency, data_plane_id: IDS.dataPlane,
    route_version_id: '10000000-0000-4000-8000-000000000007', route_version: 7,
    binding_manifest_generation: 1,
    manager_route_ticket_id: '10000000-0000-4000-8000-000000000008', route_revision: 2,
    workflow_route_hmac: 'a'.repeat(64), approval_request_route_hmac: 'b'.repeat(64),
    request_generation: 3, credential_generation: 2,
    issued_at_utc: now.toISOString(),
    expires_at_utc: new Date(now.getTime() + 5 * 60_000).toISOString(),
    nonce: '10000000-0000-4000-8000-000000000009', key_version: 1
  };
  const signed = await signCandidateRouteContext(context, {
    secret: env.CANDIDATE_ROUTE_CONTEXT_SECRET, keyVersion: 1, nowMilliseconds: now.getTime()
  });
  const request = new Request('https://private.invalid/private/candidate-manager/v1/workflows/x/start', {
    headers: {
      'x-cloudtms-route-context': signed.envelope,
      'x-cloudtms-route-context-sha256': signed.sha256
    }
  });
  const verified = await verifyCandidateRouteContext(request, env, now.getTime());
  assert.equal(verified.context.authority_kind, 'MANAGER_EMAIL');
  assert.equal(verified.context.manager_route_ticket_id, context.manager_route_ticket_id);
  assert.equal(verified.context.binding_manifest_generation, 1);
  await assert.rejects(
    signCandidateRouteContext({ ...context, authority_kind: 'MANAGER_BROWSER' }, {
      secret: env.CANDIDATE_ROUTE_CONTEXT_SECRET, keyVersion: 1, nowMilliseconds: now.getTime()
    }),
    /CANDIDATE_ROUTE_CONTEXT_AUTHORITY_INVALID/
  );
  assert.equal(await verifyCandidateRouteContext(request, {
    ...env, CANDIDATE_DATA_PLANE_ID: '20000000-0000-4000-8000-000000000006'
  }, now.getTime()), null);
});

test('service-auth v2 binds both route headers while v1 remains exact and rejects injection', async () => {
  const now = new Date();
  const env = routeEnvironment();
  const route = await signCandidateRouteContext(routeContext(now), {
    secret: env.CANDIDATE_ROUTE_CONTEXT_SECRET,
    keyVersion: 1,
    nowMilliseconds: now.getTime()
  });
  const unsignedV2 = new Request('https://private.invalid/private/candidate-app/v1/bootstrap', {
    headers: {
      'x-cloudtms-route-context': route.envelope,
      'x-cloudtms-route-context-sha256': route.sha256,
      'x-request-id': 'route-service-v2'
    }
  });
  const signedV2 = await signCandidatePrivateRequest(unsignedV2, env);
  assert.equal(signedV2.headers.get('x-cloudtms-service-version'), 'candidate-private-v2');
  assert.equal(await verifyCandidatePrivateRequest(signedV2, env), true);

  const changedDigestHeaders = new Headers(signedV2.headers);
  changedDigestHeaders.set('x-cloudtms-route-context-sha256', '0'.repeat(64));
  assert.equal(await verifyCandidatePrivateRequest(
    new Request(signedV2, { headers: changedDigestHeaders }), env
  ), false);

  const signedV1 = await signCandidatePrivateRequest(new Request(
    'https://private.invalid/private/candidate-app/v1/health'
  ), env);
  assert.equal(signedV1.headers.get('x-cloudtms-service-version'), 'candidate-private-v1');
  assert.equal(await verifyCandidatePrivateRequest(signedV1, env), true);
  const injectedHeaders = new Headers(signedV1.headers);
  injectedHeaders.set('x-cloudtms-route-context', route.envelope);
  injectedHeaders.set('x-cloudtms-route-context-sha256', route.sha256);
  assert.equal(await verifyCandidatePrivateRequest(
    new Request(signedV1, { headers: injectedHeaders }), env
  ), false);
});

test('closed generated registry never resolves an unrecognised or client-shaped key', () => {
  const primary = { async fetch() { return Response.json({ ok: true }); } };
  const synthetic = { async fetch() { return Response.json({ ok: true }); } };
  const env = {
    CLOUDTMS_PRIVATE: primary,
    CANDIDATE_SYNTHETIC_SECOND_PRIVATE: synthetic,
    CANDIDATE_DATA_PLANE_CLOUDTMS_TEST_ROUTE_CONTEXT_SECRET: 'primary-secret',
    CANDIDATE_DATA_PLANE_SYNTHETIC_SECOND_ROUTE_CONTEXT_SECRET: 'synthetic-secret'
  };
  assert.equal(
    candidateDataPlaneRegistryEntry('CANDIDATE_DATA_PLANE_CLOUDTMS_TEST', env).binding,
    primary
  );
  assert.equal(
    candidateDataPlaneRegistryEntry('CANDIDATE_DATA_PLANE_SYNTHETIC_SECOND', env).binding,
    synthetic
  );
  assert.equal(candidateDataPlaneRegistryEntry('client_supplied', env), null);
  assert.deepEqual(CANDIDATE_DATA_PLANE_BINDING_MANIFEST.binding_keys, [
    'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
    'CANDIDATE_DATA_PLANE_SYNTHETIC_SECOND'
  ]);
});

test('control-plane and federated routing remain disabled unless explicitly true', async () => {
  assert.equal(controlPlaneEnabled({}), false);
  assert.equal(controlPlaneEnabled({ MYTMS_CONTROL_PLANE_ENABLED: 'false' }), false);
  assert.equal(controlPlaneEnabled({ MYTMS_CONTROL_PLANE_ENABLED: 'TRUE' }), true);
  assert.equal(globalAuthCutoverEnabled({ MYTMS_CONTROL_PLANE_ENABLED: 'TRUE' }), false);
  assert.equal(globalAuthCutoverEnabled({
    MYTMS_CONTROL_PLANE_ENABLED: 'TRUE', MYTMS_GLOBAL_AUTH_CUTOVER_ENABLED: 'true'
  }), true);

  const brokerSource = await readFile(
    new URL('../candidate-broker/src/candidate-broker.js', import.meta.url), 'utf8'
  );
  const registrySource = await readFile(
    new URL('../candidate-broker/src/candidate-data-plane-registry.generated.js', import.meta.url), 'utf8'
  );
  const privateWorkerSource = await readFile(
    new URL('../broker/src/candidate-private-worker.js', import.meta.url), 'utf8'
  );
  assert.doesNotMatch(brokerSource, /env\s*\[\s*(?:request|client|body|choice)/i);
  assert.doesNotMatch(registrySource, /env\s*\[/);
  assert.match(brokerSource, /agency_route_context_resolve_v1/);
  assert.ok(
    privateWorkerSource.indexOf('candidate_app_federated_membership_link_set_v1')
      < privateWorkerSource.indexOf('candidate_app_federated_session_project_v2'),
    'the verified local membership link must be established before session projection'
  );
});

test('agency-local projection source is additive, service-only and route exact', async () => {
  const migration = await readFile(new URL(
    '../supabase/migrations/21082026_1757_candidate_federated_session_projection.sql',
    import.meta.url
  ), 'utf8');
  const repeatable = await readFile(new URL(
    '../supabase/repeatable/21082026_1758_candidate_app_federated_session_project_v1.sql',
    import.meta.url
  ), 'utf8');
  const renewalRepeatable = await readFile(new URL(
    '../supabase/repeatable/31082026_1732_candidate_federated_session_expiry_renewal_v1.sql',
    import.meta.url
  ), 'utf8');
  const monotonicRepeatable = await readFile(new URL(
    '../supabase/repeatable/01092026_1150_candidate_federated_session_monotonic_projection_v1.sql',
    import.meta.url
  ), 'utf8');
  const familyMigration = await readFile(new URL(
    '../supabase/migrations/01092026_2030_candidate_federated_session_family_projection.sql',
    import.meta.url
  ), 'utf8');
  const familyRepeatable = await readFile(new URL(
    '../supabase/repeatable/01092026_2031_candidate_federated_session_family_projection_v2.sql',
    import.meta.url
  ), 'utf8');
  const linkRepeatable = await readFile(new URL(
    '../supabase/repeatable/21082026_2335_candidate_app_federated_membership_link_set_v1.sql',
    import.meta.url
  ), 'utf8');
  const accountIndex = await readFile(new URL(
    '../supabase/migrations/21082026_2348_candidate_federated_membership_account_index.sql',
    import.meta.url
  ), 'utf8');
  assert.match(migration, /auth_source text not null default 'LOCAL'/);
  assert.match(migration, /force row level security/i);
  assert.match(migration, /revoke all .* public,anon,authenticated/i);
  assert.match(repeatable, /membership_generation<>p_membership_generation/);
  assert.match(repeatable, /route_version<>p_route_version/);
  assert.match(repeatable, /session_epoch<>p_session_epoch/);
  assert.match(repeatable, /p_expires_at_utc>p_now_utc\+interval '5 minutes'/);
  assert.match(repeatable, /security definer/i);
  assert.match(repeatable, /grant execute .* service_role/is);
  assert.doesNotMatch(repeatable, /insert into public\.(?:timesheets|contracts|rota|invoices)/i);
  assert.match(renewalRepeatable, /v_expiry:=least\(p_expires_at_utc,p_now_utc\+interval '5 minutes'\)/);
  assert.match(renewalRepeatable, /set expires_at_utc=v_expiry,\s*absolute_expires_at_utc=v_expiry/is);
  assert.match(renewalRepeatable, /returning \* into v_existing/);
  assert.match(renewalRepeatable, /expires_at_utc>p_now_utc and absolute_expires_at_utc>p_now_utc/);
  assert.match(renewalRepeatable, /grant execute .* service_role/is);
  assert.doesNotMatch(renewalRepeatable, /insert into public\.(?:timesheets|contracts|rota|invoices)/i);
  assert.match(monotonicRepeatable, /order by membership_generation desc,route_version desc,session_epoch desc/i);
  assert.match(monotonicRepeatable, /v_latest\.session_epoch>p_session_epoch/i);
  assert.match(monotonicRepeatable, /CANDIDATE_FEDERATED_SESSION_STALE/);
  assert.match(monotonicRepeatable, /global_session_identity_hmac<>p_global_session_identity_hmac/i);
  assert.match(monotonicRepeatable, /grant execute .* service_role/is);
  assert.doesNotMatch(monotonicRepeatable, /insert into public\.(?:timesheets|contracts|rota|invoices)/i);
  assert.match(familyMigration, /global_session_family_identity_hmac bytea/i);
  assert.match(familyMigration,
    /unique index[\s\S]*membership_id,global_session_family_identity_hmac,session_epoch/i);
  assert.doesNotMatch(familyMigration,
    /(?:insert|update|delete)\s+(?:into|from)?\s*public\.(?:timesheets|contracts|rota|invoices)/i);
  assert.match(familyRepeatable,
    /global_session_family_identity_hmac=p_global_session_family_identity_hmac/i);
  assert.match(familyRepeatable, /candidate_app_federated_session_project_v2/);
  assert.match(familyRepeatable, /grant execute .* service_role/is);
  assert.doesNotMatch(familyRepeatable,
    /insert into public\.(?:timesheets|contracts|rota|invoices)/i);
  assert.match(linkRepeatable, /route_context_verified' is distinct from 'true'/);
  assert.match(linkRepeatable, /audience' is distinct from 'FEDERATED_MEMBERSHIP_LINK'/);
  assert.match(linkRepeatable, /p_membership_generation<v_link\.membership_generation/);
  assert.match(linkRepeatable, /status in \('LOCKED','DISABLED'\)/);
  assert.doesNotMatch(linkRepeatable, /password_(?:scheme|salt|digest)\s*=/i);
  assert.doesNotMatch(linkRepeatable, /insert into public\.(?:candidates|timesheets|contracts|rota|invoices)/i);
  assert.match(linkRepeatable, /revoke all .* public,anon,authenticated/is);
  assert.match(linkRepeatable, /grant execute .* service_role/is);
  assert.match(accountIndex, /candidate_app_global_membership_links\(account_id\)/);
  assert.equal(candidateRouteContextInternals.normalizedRouteContext(routeContext()).route_version, 7);
});

function limiter() {
  return { async limit() { return { success: true }; } };
}

function orchestratorEnvironment(primaryFetch, syntheticFetch = async () => Response.json({ ok: true })) {
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_ALLOWED_ORIGINS: 'https://candidate.test.example',
    CANDIDATE_ALLOW_NATIVE_CLIENTS: 'true',
    MYTMS_CONTROL_PLANE_ENABLED: 'true',
    MYTMS_GLOBAL_AUTH_CUTOVER_ENABLED: 'true',
    MYTMS_CONTROL_PLANE_URL: 'https://control-plane.test.invalid',
    MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY: 'test-control-plane-service-role-placeholder',
    MYTMS_CONTROL_PLANE_ACTOR_IDENTITY_SECRET: 'test-control-plane-actor-secret',
    MYTMS_CONTROL_PLANE_TOKEN_DERIVATION_SECRET: 'test-control-plane-token-secret',
    MYTMS_AGENCY_CHOICE_TOKEN_SECRET: 'test-agency-choice-secret',
    MYTMS_AGENCY_CHOICE_TOKEN_KEY_VERSION: '1',
    MYTMS_AGENCY_CHOICE_TOKEN_READ_KEY_VERSIONS: '1',
    MYTMS_GLOBAL_CHALLENGE_TOKEN_SECRET: 'test-global-challenge-secret',
    MYTMS_GLOBAL_CHALLENGE_TOKEN_KEY_VERSION: '1',
    MYTMS_GLOBAL_CHALLENGE_TOKEN_READ_KEY_VERSIONS: '1',
    MYTMS_GLOBAL_PASSWORD_KEY_VERSION: '1',
    CANDIDATE_BROKER_ACCESS_TOKEN_SECRET: 'test-access-secret-material-that-is-not-live',
    CANDIDATE_BROKER_REFRESH_TOKEN_SECRET: 'test-refresh-secret-material-that-is-not-live',
    CANDIDATE_BROKER_PUBLIC_SESSION_ID_SECRET: 'test-public-session-secret-material-that-is-not-live',
    CANDIDATE_BROKER_DEVICE_TOKEN_SECRET: 'test-device-encryption-secret-that-is-not-live',
    CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_SECRET: 'test-device-identity-secret-that-is-not-live',
    CANDIDATE_BROKER_DEVICE_TOKEN_ENCRYPTION_KEY_VERSION: '1',
    CANDIDATE_BROKER_DEVICE_TOKEN_ENCRYPTION_READ_KEY_VERSIONS: '1',
    CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_KEY_VERSION: '1',
    CANDIDATE_BROKER_DEVICE_TOKEN_IDENTITY_READ_KEY_VERSIONS: '1',
    CANDIDATE_BROKER_ACCESS_TOKEN_KEY_VERSION: '1',
    CANDIDATE_BROKER_REFRESH_TOKEN_KEY_VERSION: '1',
    CANDIDATE_BROKER_PUBLIC_SESSION_ID_KEY_VERSION: '1',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'test-private-service-secret-that-is-not-live',
    CANDIDATE_DATA_PLANE_CLOUDTMS_TEST_ROUTE_CONTEXT_SECRET: 'test-primary-route-secret',
    CANDIDATE_DATA_PLANE_SYNTHETIC_SECOND_ROUTE_CONTEXT_SECRET: 'test-synthetic-route-secret',
    CLOUDTMS_PRIVATE: { fetch: primaryFetch },
    CANDIDATE_SYNTHETIC_SECOND_PRIVATE: { fetch: syntheticFetch },
    CANDIDATE_GENERAL_RATE_LIMIT: limiter(),
    CANDIDATE_AUTH_RATE_LIMIT: limiter(),
    CANDIDATE_MANAGER_RATE_LIMIT: limiter(),
    CANDIDATE_UPLOAD_RATE_LIMIT: limiter()
  };
}

async function centralAccessToken(env, overrides = {}) {
  const now = Math.floor(Date.now() / 1000);
  return candidateBrokerInternals.sealVersionedEnvelope(
    env, candidateBrokerInternals.credentialAuthorities.access,
    'candidate-broker-access-v1',
    {
      typ: 'candidate_broker_access', aud: 'cloudtms-candidate-public', env: 'TEST',
      authority: 'CONTROL_PLANE', global_account_id: IDS.account,
      global_session_id: IDS.session, global_session_family_id: IDS.family,
      session_epoch: 12, rotation: 4,
      public_session_id: '10000000-0000-5000-8000-000000000099',
      public_session_key_version: 1, iat: now, exp: now + 900,
      absolute_expires_at_utc: new Date((now + 90 * 86400) * 1000).toISOString(),
      ...overrides
    }, 1
  );
}

function protectedBrowserRequest(token, headers = {}, path = '/candidate-app/v1/bootstrap') {
  return new Request(`https://candidate-api.test.example${path}`, {
    headers: {
      origin: 'https://candidate.test.example',
      authorization: `Bearer ${token}`,
      'cf-connecting-ip': '192.0.2.22',
      ...headers
    }
  });
}

test('enabled orchestrator re-resolves central authority then calls only the registered binding', async () => {
  const originalFetch = globalThis.fetch;
  let primaryCalls = 0;
  let syntheticCalls = 0;
  const privateEnv = {
    ...routeEnvironment({ CANDIDATE_ROUTE_CONTEXT_SECRET: 'test-primary-route-secret' }),
    CANDIDATE_ROUTE_VERSION: '7'
  };
  const env = orchestratorEnvironment(async request => {
    primaryCalls += 1;
    assert.equal(await verifyCandidatePrivateRequest(request.clone(), privateEnv), true);
    const verified = await verifyCandidateRouteContext(request, privateEnv);
    assert.equal(verified.context.global_session_id, IDS.session);
    assert.equal(verified.context.global_session_family_id, IDS.family);
    assert.equal(verified.context.membership_generation, 3);
    assert.equal(request.headers.has('authorization'), false);
    return Response.json({ ok: true, source: 'primary' });
  }, async () => {
    syntheticCalls += 1;
    return Response.json({ ok: true, source: 'synthetic' });
  });
  globalThis.fetch = async request => {
    const url = new URL(request.url);
    assert.equal(url.pathname, '/rest/v1/rpc/agency_route_context_resolve_v1');
    assert.equal(request.headers.get('content-profile'), 'control');
    return Response.json({
      ok: true,
      global_session_id: IDS.session,
      global_account_id: IDS.account,
      session_epoch: 12,
      membership_id: IDS.membership,
      membership_generation: 3,
      local_candidate_id: IDS.candidate,
      agency_id: IDS.agency,
      agency_display_name: 'CloudTMS TEST',
      environment_label: 'TEST',
      data_plane_id: IDS.dataPlane,
      registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
      route_version_id: '10000000-0000-4000-8000-000000000007',
      route_version: 7,
      internal_only: true
    });
  };
  try {
    const token = await centralAccessToken(env);
    const response = await handleCandidateBrokerRequest(protectedBrowserRequest(token, {
      'x-cloudtms-route-context': 'client-route-must-not-control-selection'
    }), env);
    assert.equal(response.status, 200, JSON.stringify(await response.clone().json()));
    assert.equal((await response.json()).source, 'primary');
    assert.equal(primaryCalls, 1);
    assert.equal(syntheticCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('synthetic second-agency route reaches only its closed non-LIVE fixture binding', async () => {
  const originalFetch = globalThis.fetch;
  const synthetic = Object.freeze({
    membership: '50000000-0000-4000-8000-000000000003',
    agency: '50000000-0000-4000-8000-000000000004',
    candidate: '50000000-0000-4000-8000-000000000005',
    dataPlane: '50000000-0000-4000-8000-000000000006',
    routeVersionId: '50000000-0000-4000-8000-000000000007'
  });
  let primaryCalls = 0;
  let syntheticCalls = 0;
  const privateEnv = routeEnvironment({
    CANDIDATE_AGENCY_ID: synthetic.agency,
    CANDIDATE_DATA_PLANE_ID: synthetic.dataPlane,
    CANDIDATE_ROUTE_VERSION: '1',
    CANDIDATE_ROUTE_CONTEXT_SECRET: 'test-synthetic-route-secret'
  });
  const env = orchestratorEnvironment(async () => {
    primaryCalls += 1;
    return Response.json({ ok: true, source: 'primary' });
  }, async request => {
    syntheticCalls += 1;
    assert.equal(await verifyCandidatePrivateRequest(request.clone(), privateEnv), true);
    const verified = await verifyCandidateRouteContext(request, privateEnv);
    assert.equal(verified.context.membership_id, synthetic.membership);
    assert.equal(verified.context.membership_generation, 9);
    assert.equal(verified.context.agency_candidate_id, synthetic.candidate);
    assert.equal(request.headers.has('authorization'), false);
    return Response.json({ ok: true, source: 'synthetic-second' });
  });
  globalThis.fetch = async request => {
    const url = new URL(request.url);
    assert.equal(url.pathname, '/rest/v1/rpc/agency_route_context_resolve_v1');
    return Response.json({
      ok: true,
      global_session_id: IDS.session,
      global_account_id: IDS.account,
      session_epoch: 12,
      membership_id: synthetic.membership,
      membership_generation: 9,
      local_candidate_id: synthetic.candidate,
      agency_id: synthetic.agency,
      agency_display_name: 'Synthetic Second Agency',
      environment_label: 'TEST',
      data_plane_id: synthetic.dataPlane,
      registry_binding_key: 'CANDIDATE_DATA_PLANE_SYNTHETIC_SECOND',
      route_version_id: synthetic.routeVersionId,
      route_version: 1,
      internal_only: true
    });
  };
  try {
    const token = await centralAccessToken(env);
    const response = await handleCandidateBrokerRequest(
      protectedBrowserRequest(token), env
    );
    assert.equal(response.status, 200, JSON.stringify(await response.clone().json()));
    assert.deepEqual(await response.json(), { ok: true, source: 'synthetic-second' });
    assert.equal(primaryCalls, 0);
    assert.equal(syntheticCalls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('manager EMAIL credential resolves centrally and reaches only its exact private plane', async () => {
  const originalFetch = globalThis.fetch;
  const workflowId = '50000000-0000-4000-8000-000000000011';
  const syntheticAgency = '50000000-0000-4000-8000-000000000004';
  const syntheticPlane = '50000000-0000-4000-8000-000000000006';
  let primaryCalls = 0;
  let syntheticCalls = 0;
  const privateEnv = routeEnvironment({
    CANDIDATE_AGENCY_ID: syntheticAgency,
    CANDIDATE_DATA_PLANE_ID: syntheticPlane,
    CANDIDATE_ROUTE_VERSION: '1',
    CANDIDATE_ROUTE_CONTEXT_SECRET: 'test-synthetic-route-secret'
  });
  const env = orchestratorEnvironment(async () => {
    primaryCalls += 1;
    return Response.json({ ok: false });
  }, async request => {
    syntheticCalls += 1;
    assert.equal(await verifyCandidatePrivateRequest(request.clone(), privateEnv), true);
    const route = await verifyCandidateRouteContext(request, privateEnv);
    assert.equal(route.context.authority_kind, 'MANAGER_EMAIL');
    assert.equal(route.context.operation_id, 'startManagerReview');
    assert.equal(route.context.manager_route_ticket_id,
      '50000000-0000-4000-8000-000000000012');
    assert.equal(request.headers.get('authorization'), 'Bearer manager-email-opaque-credential');
    return Response.json({ ok: true, source: 'synthetic-manager-email' });
  });
  env.MYTMS_MANAGER_ROUTE_HMAC_SECRET = 'test-manager-route-hmac-secret-that-is-not-live';
  env.CANDIDATE_BROKER_MANAGER_HANDOFF_SECRET = 'test-manager-phone-secret-that-is-not-live';
  globalThis.fetch = async request => {
    assert.equal(new URL(request.url).pathname, '/rest/v1/rpc/manager_email_route_resolve_v1');
    const body = await request.json();
    assert.equal(body.p_resolution.operation_id, 'startManagerReview');
    assert.match(body.p_resolution.credential_hmac_hex, /^[0-9a-f]{64}$/);
    return Response.json({
      ok: true, authority_kind: 'MANAGER_EMAIL', environment_label: 'TEST',
      agency_id: syntheticAgency, data_plane_id: syntheticPlane,
      registry_binding_key: 'CANDIDATE_DATA_PLANE_SYNTHETIC_SECOND',
      route_version_id: '50000000-0000-4000-8000-000000000007', route_version: 1,
      binding_manifest_generation: 1,
      manager_route_ticket_id: '50000000-0000-4000-8000-000000000012', route_revision: 1,
      workflow_route_hmac_hex: 'c'.repeat(64), approval_request_route_hmac_hex: 'd'.repeat(64),
      request_generation: 1, credential_generation: 1,
      expires_at_utc: new Date(Date.now() + 7 * 86400_000).toISOString()
    });
  };
  try {
    const response = await handleCandidateBrokerRequest(new Request(
      `https://candidate-api.test.example/candidate-manager/v1/workflows/${workflowId}/start`, {
        headers: {
          origin: 'https://candidate.test.example',
          authorization: 'Bearer manager-email-opaque-credential',
          'cf-connecting-ip': '192.0.2.23'
        }
      }
    ), env);
    assert.equal(response.status, 200, JSON.stringify(await response.clone().json()));
    assert.deepEqual(await response.json(), { ok: true, source: 'synthetic-manager-email' });
    assert.equal(primaryCalls, 0);
    assert.equal(syntheticCalls, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('manager EMAIL route failure has zero default-plane fallback', async () => {
  const originalFetch = globalThis.fetch;
  let privateCalls = 0;
  const env = orchestratorEnvironment(async () => {
    privateCalls += 1;
    return Response.json({ ok: true });
  }, async () => {
    privateCalls += 1;
    return Response.json({ ok: true });
  });
  env.MYTMS_MANAGER_ROUTE_HMAC_SECRET = 'test-manager-route-hmac-secret-that-is-not-live';
  env.CANDIDATE_BROKER_MANAGER_HANDOFF_SECRET = 'test-manager-phone-secret-that-is-not-live';
  globalThis.fetch = async () => Response.json(
    { message: 'MANAGER_SECURE_LINK_INVALID', code: '28000' }, { status: 401 }
  );
  try {
    const response = await handleCandidateBrokerRequest(new Request(
      'https://candidate-api.test.example/candidate-manager/v1/workflows/50000000-0000-4000-8000-000000000011/start', {
        headers: {
          origin: 'https://candidate.test.example', authorization: 'Bearer invalid-manager-link',
          'cf-connecting-ip': '192.0.2.24'
        }
      }
    ), env);
    assert.equal(response.status, 401);
    assert.equal(privateCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('stale central access fails before either private agency binding is called', async () => {
  const originalFetch = globalThis.fetch;
  let privateCalls = 0;
  const env = orchestratorEnvironment(async () => {
    privateCalls += 1;
    return Response.json({ ok: true });
  }, async () => {
    privateCalls += 1;
    return Response.json({ ok: true });
  });
  globalThis.fetch = async () => Response.json(
    { message: 'AGENCY_CONTEXT_STALE', code: '28000' }, { status: 400 }
  );
  try {
    const token = await centralAccessToken(env);
    const response = await handleCandidateBrokerRequest(protectedBrowserRequest(
      token, {}, '/candidate-app/v1/account/preferences'
    ), env);
    assert.equal(response.status, 400);
    assert.equal((await response.json()).error_code, 'AGENCY_CONTEXT_STALE');
    assert.equal(privateCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('global login auto-selects exactly one agency and returns no internal route identifier', async () => {
  const originalFetch = globalThis.fetch;
  const env = orchestratorEnvironment(async () => {
    throw new Error('login must not call an agency business plane');
  });
  const started = new Date();
  const firstSessionId = '30000000-0000-4000-8000-000000000001';
  const selectedSessionId = '30000000-0000-4000-8000-000000000002';
  const absoluteExpiry = new Date(started.getTime() + 90 * 86400000).toISOString();
  const seen = [];
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    seen.push(operation);
    const args = await request.json();
    if (operation === 'global_login_metadata_v1') {
      return Response.json({
        ok: true, found: true, account_id: IDS.account,
        credential_scheme: 'PBKDF2-HMAC-SHA256', scheme_version: 1,
        password_salt_hex: '11'.repeat(16),
        parameters: { hash: 'SHA-256', iterations: 100000, length_bytes: 32 },
        credential_authority_sha256_hex: '22'.repeat(32), key_version: 1,
        internal_only: true
      });
    }
    if (operation === 'global_login_v1') {
      return Response.json({
        ok: true, account_id: IDS.account, session_id: firstSessionId,
        family_id: '30000000-0000-4000-8000-000000000003',
        rotation: 0, session_epoch: 1,
        issued_at_utc: args.p_now_utc,
        expires_at_utc: args.p_internal_context.expires_at_utc,
        absolute_expires_at_utc: args.p_internal_context.absolute_expires_at_utc,
        selection_required: true, internal_only: true
      });
    }
    if (operation === 'account_agencies_get_v1') {
      return Response.json({
        ok: true, context_version: 1, auto_select: true, selection_required: false,
        memberships_internal: [{
          membership_id: IDS.membership, membership_generation: 3,
          agency_id: IDS.agency, display_name: 'CloudTMS TEST', environment_label: 'TEST',
          local_candidate_id: IDS.candidate, data_plane_id: IDS.dataPlane,
          registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
          route_version_id: '30000000-0000-4000-8000-000000000007', route_version: 7
        }], internal_only: true
      });
    }
    if (operation === 'agency_session_issue_v1') {
      assert.match(args.p_choice_token, /^v4\.1\./);
      assert.equal(args.p_global_session_context.selected_membership_id, IDS.membership);
      return Response.json({
        ok: true, session_id: selectedSessionId, session_epoch: 2,
        membership_id: IDS.membership, membership_generation: 3,
        agency_id: IDS.agency, agency_display_name: 'CloudTMS TEST', environment_label: 'TEST',
        local_candidate_id: IDS.candidate, data_plane_id: IDS.dataPlane,
        registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
        route_version_id: '30000000-0000-4000-8000-000000000007', route_version: 7,
        issued_at_utc: args.p_global_session_context.issued_at_utc,
        expires_at_utc: args.p_global_session_context.expires_at_utc,
        internal_only: true
      });
    }
    throw new Error(`unexpected operation ${operation}`);
  };
  try {
    const response = await handleCandidateBrokerRequest(new Request(
      'https://candidate-api.test.example/candidate-app/v1/auth/login', {
        method: 'POST',
        headers: {
          origin: 'https://candidate.test.example',
          'cf-connecting-ip': '192.0.2.23',
          'content-type': 'application/json'
        },
        body: JSON.stringify({
          email: 'candidate@example.test', password: 'not-a-real-password',
          idempotency_key: 'global-login-auto-select-proof'
        })
      }
    ), env);
    assert.equal(response.status, 200, JSON.stringify(await response.clone().json()));
    const body = await response.json();
    assert.equal(body.selection_required, false);
    assert.equal(body.agency_context.display_name, 'CloudTMS TEST');
    assert.equal(body.agency_context.environment_label, 'TEST');
    assert.equal(body.agency_context.membership_generation, 3);
    assert.equal(body.agency_context.session_epoch, 2);
    assert.equal(Object.hasOwn(body.agency_context, 'agency_id'), false);
    assert.equal(Object.hasOwn(body.agency_context, 'data_plane_id'), false);
    assert.equal(Object.hasOwn(body.agency_context, 'registry_binding_key'), false);
    const opened = await candidateBrokerInternals.openPublicAccess(new Request(
      'https://candidate.test.invalid', { headers: { authorization: `Bearer ${body.access_token}` } }
    ), env);
    assert.equal(opened.authority, 'CONTROL_PLANE');
    assert.equal(opened.global_session_id, selectedSessionId);
    assert.equal(opened.global_session_family_id,
      '30000000-0000-4000-8000-000000000003');
    assert.deepEqual(seen, [
      'global_login_metadata_v1', 'global_login_v1',
      'account_agencies_get_v1', 'agency_session_issue_v1'
    ]);
    assert.ok(Date.parse(body.absolute_expires_at_utc) <= Date.parse(absoluteExpiry) + 60_000);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('multi-agency choice, session issue and refresh preserve sealed absolute expiry', async () => {
  const originalFetch = globalThis.fetch;
  const env = orchestratorEnvironment(async () => {
    throw new Error('account control routes must not call an agency business plane');
  });
  const sessionFamilyId = '40000000-0000-4000-8000-000000000003';
  const initialToken = await centralAccessToken(env, {
    global_session_family_id: sessionFamilyId
  });
  const absoluteExpiry = new Date(Date.now() + 89 * 86400000).toISOString();
  const selectedSessionId = '40000000-0000-4000-8000-000000000001';
  const refreshedSessionId = '40000000-0000-4000-8000-000000000002';
  const routeVersionId = '40000000-0000-4000-8000-000000000007';
  const seen = [];
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    seen.push(operation);
    const args = await request.json();
    if (operation === 'account_agencies_get_v1') {
      return Response.json({
        ok: true, context_version: 1, auto_select: false, selection_required: true,
        memberships_internal: [
          {
            membership_id: IDS.membership, membership_generation: 3,
            agency_id: IDS.agency, display_name: 'CloudTMS TEST', environment_label: 'TEST',
            local_candidate_id: IDS.candidate, data_plane_id: IDS.dataPlane,
            registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
            route_version_id: routeVersionId, route_version: 7
          },
          {
            membership_id: '50000000-0000-4000-8000-000000000003', membership_generation: 9,
            agency_id: '50000000-0000-4000-8000-000000000004',
            display_name: 'Synthetic Second Agency', environment_label: 'TEST',
            local_candidate_id: '50000000-0000-4000-8000-000000000005',
            data_plane_id: '50000000-0000-4000-8000-000000000006',
            registry_binding_key: 'CANDIDATE_DATA_PLANE_SYNTHETIC_SECOND',
            route_version_id: '50000000-0000-4000-8000-000000000007', route_version: 1
          }
        ], internal_only: true
      });
    }
    if (operation === 'agency_session_issue_v1') {
      assert.equal(args.p_global_session_context.selected_membership_id, IDS.membership);
      return Response.json({
        ok: true, session_id: selectedSessionId, session_epoch: 13,
        membership_id: IDS.membership, membership_generation: 3,
        agency_id: IDS.agency, agency_display_name: 'CloudTMS TEST', environment_label: 'TEST',
        local_candidate_id: IDS.candidate, data_plane_id: IDS.dataPlane,
        registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
        route_version_id: routeVersionId, route_version: 7,
        issued_at_utc: args.p_global_session_context.issued_at_utc,
        expires_at_utc: args.p_global_session_context.expires_at_utc,
        internal_only: true
      });
    }
    if (operation === 'global_refresh_v1') {
      return Response.json({
        ok: true, account_id: IDS.account, session_id: refreshedSessionId,
        family_id: sessionFamilyId, rotation: 6,
        session_epoch: 14, selected_membership_id: IDS.membership,
        issued_at_utc: args.p_now_utc,
        expires_at_utc: args.p_internal_context.expires_at_utc,
        absolute_expires_at_utc: absoluteExpiry, internal_only: true
      });
    }
    if (operation === 'agency_route_context_resolve_v1') {
      return Response.json({
        ok: true, global_session_id: refreshedSessionId, global_account_id: IDS.account,
        session_epoch: 14, membership_id: IDS.membership, membership_generation: 3,
        local_candidate_id: IDS.candidate, agency_id: IDS.agency,
        agency_display_name: 'CloudTMS TEST', environment_label: 'TEST',
        data_plane_id: IDS.dataPlane,
        registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
        route_version_id: routeVersionId, route_version: 7, internal_only: true
      });
    }
    throw new Error(`unexpected operation ${operation}`);
  };
  try {
    const choicesResponse = await handleCandidateBrokerRequest(protectedBrowserRequest(
      initialToken, {}, '/candidate-app/v1/account/agencies'
    ), env);
    assert.equal(choicesResponse.status, 200);
    const choices = await choicesResponse.json();
    assert.equal(choices.selection_required, true);
    assert.equal(choices.agencies.length, 2);
    assert.deepEqual(Object.keys(choices.agencies[0]).sort(), [
      'agency_choice_token', 'display_name', 'environment_label',
      'expires_at_utc', 'membership_generation'
    ]);

    const selectionResponse = await handleCandidateBrokerRequest(new Request(
      'https://candidate-api.test.example/candidate-app/v1/account/agency-session', {
        method: 'POST',
        headers: {
          origin: 'https://candidate.test.example', authorization: `Bearer ${initialToken}`,
          'cf-connecting-ip': '192.0.2.24', 'content-type': 'application/json'
        },
        body: JSON.stringify({
          agency_choice_token: choices.agencies[0].agency_choice_token,
          idempotency_key: '40000000-0000-4000-8000-000000000009'
        })
      }
    ), env);
    assert.equal(selectionResponse.status, 200, JSON.stringify(await selectionResponse.clone().json()));
    const selected = await selectionResponse.json();
    const selectedAccess = await candidateBrokerInternals.openPublicAccess(new Request(
      'https://candidate.test.invalid', { headers: { authorization: `Bearer ${selected.access_token}` } }
    ), env);
    assert.equal(selectedAccess.absolute_expires_at_utc, selected.absolute_expires_at_utc);
    assert.equal(selectedAccess.global_session_family_id, sessionFamilyId);

    const refreshResponse = await handleCandidateBrokerRequest(new Request(
      'https://candidate-api.test.example/candidate-app/v1/auth/refresh', {
        method: 'POST',
        headers: {
          origin: 'https://candidate.test.example', 'cf-connecting-ip': '192.0.2.24',
          'content-type': 'application/json'
        },
        body: JSON.stringify({
          refresh_token: selected.refresh_token, session_id: selected.session_id,
          idempotency_key: '40000000-0000-4000-8000-000000000010'
        })
      }
    ), env);
    assert.equal(refreshResponse.status, 200, JSON.stringify(await refreshResponse.clone().json()));
    const refreshed = await refreshResponse.json();
    assert.equal(refreshed.agency_context.session_epoch, 14);
    assert.equal(refreshed.absolute_expires_at_utc, absoluteExpiry);
    const refreshedAccess = await candidateBrokerInternals.openPublicAccess(new Request(
      'https://candidate.test.invalid', { headers: { authorization: `Bearer ${refreshed.access_token}` } }
    ), env);
    assert.equal(refreshedAccess.global_session_id, refreshedSessionId);
    assert.equal(refreshedAccess.global_session_family_id, sessionFamilyId);
    assert.equal(refreshedAccess.absolute_expires_at_utc, absoluteExpiry);
    assert.deepEqual(seen, [
      'account_agencies_get_v1', 'agency_session_issue_v1',
      'global_refresh_v1', 'agency_route_context_resolve_v1'
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('an existing account receives a sealed pending invitation, accepts once and gets an immediate agency choice', async () => {
  const originalFetch = globalThis.fetch;
  const env = orchestratorEnvironment(async () => {
    throw new Error('pending invitation control must not call an agency business plane');
  });
  const accessToken = await centralAccessToken(env);
  const pendingMembership = '51000000-0000-4000-8000-000000000003';
  const pendingAgency = '51000000-0000-4000-8000-000000000004';
  const invitationId = '51000000-0000-4000-8000-000000000043';
  let accepted = false;
  const seen = [];
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    seen.push(operation);
    const args = await request.json();
    if (operation === 'global_session_metadata_v1') {
      return Response.json({
        ok: true, account_id: IDS.account, verified_emails: ['candidate@example.test'], internal_only: true
      });
    }
    if (operation === 'account_agencies_get_v1') {
      const memberships = [{
        membership_id: IDS.membership, membership_generation: 3,
        agency_id: IDS.agency, display_name: 'Existing Agency', environment_label: 'TEST',
        local_candidate_id: IDS.candidate, data_plane_id: IDS.dataPlane,
        registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
        route_version_id: '51000000-0000-4000-8000-000000000007', route_version: 7
      }];
      if (accepted) memberships.push({
        membership_id: pendingMembership, membership_generation: 5,
        agency_id: pendingAgency, display_name: 'New Inviting Agency', environment_label: 'TEST',
        local_candidate_id: '51000000-0000-4000-8000-000000000005',
        data_plane_id: '51000000-0000-4000-8000-000000000006',
        registry_binding_key: 'CANDIDATE_DATA_PLANE_SYNTHETIC_SECOND',
        route_version_id: '51000000-0000-4000-8000-000000000008', route_version: 1
      });
      return Response.json({
        ok: true, memberships_internal: memberships,
        pending_invitations_internal: accepted ? [] : [{
          invitation_id: invitationId, invitation_generation: 2,
          membership_id: pendingMembership, membership_generation: 4,
          agency_id: pendingAgency, display_name: 'New Inviting Agency',
          environment_label: 'TEST', state: 'PENDING_ACCEPTANCE',
          expires_at_utc: new Date(Date.now() + 3600_000).toISOString()
        }], internal_only: true
      });
    }
    if (operation === 'invitation_offer_accept_v1') {
      assert.equal(args.p_invitation_id, invitationId);
      assert.equal(args.p_membership_id, pendingMembership);
      assert.equal(args.p_expected_membership_generation, 4);
      accepted = true;
      return Response.json({
        ok: true, state: 'ACTIVE', membership_generation: 5,
        membership_id_internal: pendingMembership, agency_id_internal: pendingAgency,
        idempotent_replay: false, internal_only: true
      });
    }
    throw new Error(`unexpected operation ${operation}`);
  };
  try {
    const choicesResponse = await handleCandidateBrokerRequest(protectedBrowserRequest(
      accessToken, {}, '/candidate-app/v1/account/agencies'
    ), env);
    assert.equal(choicesResponse.status, 200);
    const choices = await choicesResponse.json();
    assert.equal(choices.pending_invitations.length, 1);
    assert.equal(choices.pending_invitations[0].display_name, 'New Inviting Agency');
    assert.equal(Object.hasOwn(choices.pending_invitations[0], 'invitation_id'), false);

    const acceptedResponse = await handleCandidateBrokerRequest(new Request(
      'https://candidate-api.test.example/candidate-app/v1/invitations/accept', {
        method: 'POST', headers: {
          origin: 'https://candidate.test.example', authorization: `Bearer ${accessToken}`,
          'cf-connecting-ip': '192.0.2.32', 'content-type': 'application/json'
        }, body: JSON.stringify({
          invitation_offer_token: choices.pending_invitations[0].invitation_offer_token,
          idempotency_key: '51000000-0000-4000-8000-000000000099'
        })
      }
    ), env);
    assert.equal(acceptedResponse.status, 200, JSON.stringify(await acceptedResponse.clone().json()));
    const acceptedBody = await acceptedResponse.json();
    assert.equal(acceptedBody.state, 'ACTIVE');
    assert.equal(acceptedBody.agency_choice.display_name, 'New Inviting Agency');
    assert.equal(Object.hasOwn(acceptedBody, 'membership_id_internal'), false);
    assert.deepEqual(seen, [
      'account_agencies_get_v1', 'global_session_metadata_v1',
      'invitation_offer_accept_v1', 'account_agencies_get_v1'
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('First registration creates a verified account but leaves the named invitation for one explicit acceptance', async () => {
  const originalFetch = globalThis.fetch;
  let delivered = null;
  const privateEnv = routeEnvironment();
  const env = orchestratorEnvironment(async () => {
    throw new Error('global authentication must not call an agency business operation');
  });
  env.MYTMS_IDENTITY_DELIVERY = {
    async fetch(request) {
      assert.equal(await verifyCandidatePrivateRequest(request.clone(), privateEnv), true);
      delivered = await request.json();
      assert.equal(delivered.email, 'candidate@example.test');
      assert.equal(delivered.purpose, 'ACTIVATE');
      assert.match(delivered.token, /^v4\.1\./);
      assert.match(delivered.deterministic_outbox_key, /^MYTMS_AUTH_[a-f0-9]{40}$/);
      return Response.json({ ok: true, accepted: true }, { status: 202 });
    }
  };
  const seen = [];
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    seen.push(operation);
    const args = await request.json();
    if (operation === 'global_challenge_start_v1') {
      assert.match(args.p_internal_context.challenge_id, /^[0-9a-f-]{36}$/);
      assert.match(args.p_internal_context.token_hash_hex, /^[a-f0-9]{64}$/);
      return Response.json({
        ok: true, accepted: true, deliver_email: true,
        challenge_id: args.p_internal_context.challenge_id, generation: 1,
        expires_at_utc: args.p_internal_context.expires_at_utc,
        idempotent_replay: false, internal_only: true
      });
    }
    if (operation === 'global_challenge_verify_v1') {
      assert.equal(args.p_token, delivered.token);
      assert.equal(args.p_challenge_id, delivered.challenge_id);
      return Response.json({
        ok: true, challenge_id: args.p_challenge_id, purpose: 'ACTIVATE',
        expires_at_utc: args.p_internal_context.verification_receipt_expires_at_utc,
        idempotent_replay: false, internal_only: true
      });
    }
    if (operation === 'global_password_complete_v1') {
      assert.equal(args.p_challenge_id, delivered.challenge_id);
      assert.match(args.p_password_proof.password_salt_hex, /^[a-f0-9]{32}$/);
      assert.match(args.p_password_proof.password_digest_hex, /^[a-f0-9]{64}$/);
      return Response.json({
        ok: true, account_id: IDS.account,
        session_id: args.p_internal_context.session_id,
        family_id: args.p_internal_context.family_id, rotation: 0, session_epoch: 1,
        issued_at_utc: args.p_now_utc,
        expires_at_utc: args.p_internal_context.expires_at_utc,
        absolute_expires_at_utc: args.p_internal_context.absolute_expires_at_utc,
        selection_required: true, internal_only: true
      });
    }
    if (operation === 'account_agencies_get_v1') {
      return Response.json({
        ok: true, memberships_internal: [], pending_invitations_internal: [{
          invitation_id: IDS.challenge, invitation_generation: 1,
          membership_id: IDS.membership, membership_generation: 3,
          agency_id: IDS.agency, display_name: 'CloudTMS TEST', environment_label: 'TEST',
          state: 'PENDING_ACCEPTANCE', expires_at_utc: new Date(Date.now() + 300_000).toISOString()
        }], internal_only: true
      });
    }
    throw new Error(`unexpected operation ${operation}`);
  };
  const publicRequest = (path, body) => new Request(`https://candidate-api.test.example${path}`, {
    method: 'POST',
    headers: {
      origin: 'https://candidate.test.example', 'cf-connecting-ip': '192.0.2.25',
      'content-type': 'application/json'
    },
    body: JSON.stringify(body)
  });
  try {
    const start = await handleCandidateBrokerRequest(publicRequest(
      '/candidate-app/v1/auth/challenge/start', {
        email: 'candidate@example.test', purpose: 'ACTIVATE',
        idempotency_key: 'global-challenge-start-proof'
      }
    ), env);
    assert.equal(start.status, 202);
    assert.deepEqual(await start.json(), { ok: true, accepted: true, next_step: 'CHECK_EMAIL' });
    assert.ok(delivered);

    const verify = await handleCandidateBrokerRequest(publicRequest(
      '/candidate-app/v1/auth/challenge/verify', {
        email: 'candidate@example.test', purpose: 'ACTIVATE', token: delivered.token,
        idempotency_key: 'global-challenge-verify-proof'
      }
    ), env);
    assert.equal(verify.status, 200, JSON.stringify(await verify.clone().json()));
    assert.equal((await verify.json()).challenge_id, delivered.challenge_id);

    const complete = await handleCandidateBrokerRequest(publicRequest(
      '/candidate-app/v1/auth/password/complete', {
        challenge_id: delivered.challenge_id, password: 'not-a-real-password',
        idempotency_key: 'global-password-complete-proof'
      }
    ), env);
    assert.equal(complete.status, 200, JSON.stringify(await complete.clone().json()));
    const session = await complete.json();
    assert.equal(session.access_token_type, 'Bearer');
    assert.equal(session.selected_candidate_id, null);
    assert.equal(session.selection_required, true);
    assert.equal(Object.hasOwn(session, 'agency_context'), false);
    assert.equal(Object.hasOwn(session, 'registry_binding_key'), false);
    assert.deepEqual(seen, [
      'global_challenge_start_v1', 'global_challenge_verify_v1',
      'global_password_complete_v1', 'account_agencies_get_v1'
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('password completion fails closed when no invitation or membership can authorize registration', async () => {
  const originalFetch = globalThis.fetch;
  const env = orchestratorEnvironment(async () => {
    throw new Error('unauthorized registration must not reach an agency data plane');
  });
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    assert.equal(operation, 'global_password_complete_v1');
    return Response.json({ message: 'INVITATION_REQUIRED' }, { status: 400 });
  };
  try {
    const response = await handleCandidateBrokerRequest(new Request(
      'https://candidate-api.test.example/candidate-app/v1/auth/password/complete', {
        method: 'POST', headers: {
          origin: 'https://candidate.test.example', 'cf-connecting-ip': '192.0.2.34',
          'content-type': 'application/json'
        }, body: JSON.stringify({
          challenge_id: '10000000-0000-4000-8000-000000000020', password: 'not-a-real-password',
          idempotency_key: 'registration-without-invitation-proof'
        })
      }
    ), env);
    assert.equal(response.status, 403, JSON.stringify(await response.clone().json()));
    assert.equal((await response.json()).error_code, 'CANDIDATE_INVITATION_REQUIRED');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('a valid invitation creates an account without a second email and enters the accepted agency', async () => {
  const originalFetch = globalThis.fetch;
  const env = orchestratorEnvironment(async () => {
    throw new Error('direct invitation setup must not call an agency business plane');
  });
  env.MYTMS_IDENTITY_DELIVERY = {
    async fetch() { throw new Error('direct invitation setup must not send another email'); }
  };
  const invitationToken = 'direct-invitation-token-that-proves-email-possession';
  const routeVersionId = '61000000-0000-4000-8000-000000000007';
  let challengeId;
  const seen = [];
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    seen.push(operation);
    const args = await request.json();
    if (operation === 'global_invitation_challenge_start_v1') {
      challengeId = args.p_internal_context.challenge_id;
      assert.match(args.p_internal_context.invitation_token_hash_hex, /^[a-f0-9]{64}$/);
      assert.equal(JSON.stringify(args).includes(invitationToken), false);
      return Response.json({
        ok: true, accepted: true, direct_setup: true, deliver_email: false,
        challenge_id: challengeId, agency_display_name: 'Arthur Rai Medical Services Limited',
        expires_at_utc: args.p_internal_context.expires_at_utc,
        internal_only: true
      });
    }
    if (operation === 'global_password_complete_v1') {
      assert.equal(args.p_challenge_id, challengeId);
      assert.match(args.p_internal_context.accept_invitation_token_hash_hex, /^[a-f0-9]{64}$/);
      assert.equal(JSON.stringify(args).includes(invitationToken), false);
      return Response.json({
        ok: true, account_id: IDS.account,
        session_id: args.p_internal_context.session_id,
        family_id: args.p_internal_context.family_id, rotation: 0, session_epoch: 1,
        issued_at_utc: args.p_now_utc,
        expires_at_utc: args.p_internal_context.expires_at_utc,
        absolute_expires_at_utc: args.p_internal_context.absolute_expires_at_utc,
        selection_required: true,
        invitation_acceptance_internal: {
          state: 'ACTIVE', membership_id_internal: IDS.membership,
          agency_id_internal: IDS.agency, membership_generation: 4
        },
        internal_only: true
      });
    }
    if (operation === 'account_agencies_get_v1') {
      return Response.json({
        ok: true,
        memberships_internal: [{
          membership_id: IDS.membership, membership_generation: 4,
          agency_id: IDS.agency, display_name: 'Arthur Rai Medical Services Limited', environment_label: 'TEST',
          local_candidate_id: IDS.candidate, data_plane_id: IDS.dataPlane,
          registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
          route_version_id: routeVersionId, route_version: 7
        }], pending_invitations_internal: [], internal_only: true
      });
    }
    if (operation === 'agency_session_issue_v1') {
      assert.equal(args.p_global_session_context.selected_membership_id, IDS.membership);
      return Response.json({
        ok: true, session_id: args.p_global_session_context.new_session_id,
        session_epoch: 2, membership_id: IDS.membership, membership_generation: 4,
        agency_id: IDS.agency, agency_display_name: 'Arthur Rai Medical Services Limited',
        environment_label: 'TEST', local_candidate_id: IDS.candidate,
        data_plane_id: IDS.dataPlane, registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
        route_version_id: routeVersionId, route_version: 7,
        issued_at_utc: args.p_global_session_context.issued_at_utc,
        expires_at_utc: args.p_global_session_context.expires_at_utc,
        internal_only: true
      });
    }
    throw new Error(`unexpected operation ${operation}`);
  };
  const request = (path, body) => new Request(`https://candidate-api.test.example${path}`, {
    method: 'POST', headers: {
      origin: 'https://candidate.test.example', 'cf-connecting-ip': '192.0.2.31',
      'content-type': 'application/json'
    }, body: JSON.stringify(body)
  });
  try {
    const started = await handleCandidateBrokerRequest(request(
      '/candidate-app/v1/auth/challenge/start', {
        invitation_token: invitationToken, purpose: 'ACTIVATE',
        idempotency_key: 'direct-invitation-start-proof'
      }
    ), env);
    assert.equal(started.status, 202, JSON.stringify(await started.clone().json()));
    const startedBody = await started.json();
    assert.equal(startedBody.next_step, 'SET_PASSWORD');
    assert.equal(startedBody.agency_display_name, 'Arthur Rai Medical Services Limited');

    const completed = await handleCandidateBrokerRequest(request(
      '/candidate-app/v1/auth/password/complete', {
        challenge_id: startedBody.challenge_id, invitation_token: invitationToken,
        password: 'not-a-real-password', idempotency_key: 'direct-invitation-complete-proof'
      }
    ), env);
    assert.equal(completed.status, 200, JSON.stringify(await completed.clone().json()));
    const session = await completed.json();
    assert.equal(session.selection_required, false);
    assert.equal(session.agency_context.display_name, 'Arthur Rai Medical Services Limited');
    assert.deepEqual(seen, [
      'global_invitation_challenge_start_v1', 'global_password_complete_v1',
      'account_agencies_get_v1', 'agency_session_issue_v1'
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('global password change revokes centrally and device registration never exposes the raw token', async () => {
  const originalFetch = globalThis.fetch;
  const env = orchestratorEnvironment(async () => {
    throw new Error('central account operations must not call a business plane');
  });
  const token = await centralAccessToken(env);
  const seen = [];
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    seen.push(operation);
    const args = await request.json();
    if (operation === 'global_session_metadata_v1') {
      return Response.json({
        ok: true, account_id: IDS.account, verified_emails: ['candidate@example.test'],
        credential_scheme: 'PBKDF2-HMAC-SHA256', scheme_version: 1,
        password_salt_hex: '33'.repeat(16),
        parameters: { hash: 'SHA-256', iterations: 100000, length_bytes: 32 },
        key_version: 1, credential_authority_sha256_hex: '44'.repeat(32),
        internal_only: true
      });
    }
    if (operation === 'global_password_change_v1') {
      assert.match(args.p_current_password_proof.presented_digest_hex, /^[a-f0-9]{64}$/);
      assert.match(args.p_new_password_proof.password_digest_hex, /^[a-f0-9]{64}$/);
      return Response.json({
        ok: true, account_id: IDS.account, session_version: 8,
        reauthentication_required: true, idempotent_replay: false
      });
    }
    if (operation === 'agency_route_context_resolve_v1') {
      return Response.json({
        ok: true, global_session_id: IDS.session, global_account_id: IDS.account,
        session_epoch: 12, membership_id: IDS.membership, membership_generation: 3,
        local_candidate_id: IDS.candidate, agency_id: IDS.agency,
        agency_display_name: 'CloudTMS TEST', environment_label: 'TEST',
        data_plane_id: IDS.dataPlane,
        registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
        route_version_id: '70000000-0000-4000-8000-000000000007',
        route_version: 7, internal_only: true
      });
    }
    if (operation === 'device_registration_upsert_v1') {
      assert.equal(JSON.stringify(args).includes('raw-device-token-proof'), false);
      assert.match(args.p_token_identity_hmac, /^[a-f0-9]{64}$/);
      assert.match(args.p_token_ciphertext, /^\\x[0-9a-f]+$/);
      return Response.json({ ok: true, status: 'ACTIVE', internal_only: true });
    }
    throw new Error(`unexpected operation ${operation}`);
  };
  try {
    const password = await handleCandidateBrokerRequest(new Request(
      'https://candidate-api.test.example/candidate-app/v1/account/password', {
        method: 'POST', headers: {
          origin: 'https://candidate.test.example', authorization: `Bearer ${token}`,
          'cf-connecting-ip': '192.0.2.26', 'content-type': 'application/json'
        }, body: JSON.stringify({
          current_password: 'not-a-real-password', password: 'another-test-password',
          idempotency_key: 'global-password-change-proof'
        })
      }
    ), env);
    assert.equal(password.status, 200, JSON.stringify(await password.clone().json()));
    assert.deepEqual(await password.json(), {
      ok: true, account_id: IDS.account, session_version: 8
    });

    const device = await handleCandidateBrokerRequest(new Request(
      'https://candidate-api.test.example/candidate-app/v1/account/push-token', {
        method: 'POST', headers: {
          origin: 'https://candidate.test.example', authorization: `Bearer ${token}`,
          'cf-connecting-ip': '192.0.2.26', 'content-type': 'application/json'
        }, body: JSON.stringify({
          push_provider: 'FCM', push_token: 'raw-device-token-proof',
          idempotency_key: 'global-device-registration-proof'
        })
      }
    ), env);
    assert.equal(device.status, 200, JSON.stringify(await device.clone().json()));
    assert.deepEqual(await device.json(), { ok: true, status: 'ACTIVE', provider: 'FCM' });
    assert.deepEqual(seen, [
      'global_session_metadata_v1', 'global_password_change_v1',
      'agency_route_context_resolve_v1', 'device_registration_upsert_v1'
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('permanent account deletion requires password confirmation and remains control-plane only', async () => {
  const originalFetch = globalThis.fetch;
  const env = orchestratorEnvironment(async () => {
    throw new Error('account deletion must never call an agency business plane');
  });
  const token = await centralAccessToken(env);
  const seen = [];
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    seen.push(operation);
    const args = await request.json();
    if (operation === 'global_session_metadata_v1') {
      return Response.json({
        ok: true, account_id: IDS.account, verified_emails: ['candidate@example.test'],
        credential_scheme: 'PBKDF2-HMAC-SHA256', scheme_version: 1,
        password_salt_hex: '33'.repeat(16),
        parameters: { hash: 'SHA-256', iterations: 100000, length_bytes: 32 },
        key_version: 1, credential_authority_sha256_hex: '44'.repeat(32),
        internal_only: true
      });
    }
    if (operation === 'global_account_delete_v1') {
      assert.match(args.p_current_password_proof.presented_digest_hex, /^[a-f0-9]{64}$/);
      assert.equal(JSON.stringify(args).includes('not-a-real-password'), false);
      assert.equal(args.p_idempotency_key, '80000000-0000-4000-8000-000000000099');
      return Response.json({
        ok: true, status: 'DELETED', account_deleted: true, access_revoked: true,
        session_version: 9, memberships_revoked: 2, invitations_revoked: 1,
        devices_revoked: 2, idempotent_replay: false, internal_only: true
      });
    }
    throw new Error(`unexpected operation ${operation}`);
  };
  const invoke = body => handleCandidateBrokerRequest(new Request(
    'https://candidate-api.test.example/candidate-app/v1/account/delete', {
      method: 'POST', headers: {
        origin: 'https://candidate.test.example', authorization: `Bearer ${token}`,
        'cf-connecting-ip': '192.0.2.32', 'content-type': 'application/json'
      }, body: JSON.stringify(body)
    }
  ), env);
  try {
    const missingConfirmation = await invoke({
      current_password: 'not-a-real-password', confirmation: 'DELETE_ACCOUNT',
      idempotency_key: '80000000-0000-4000-8000-000000000099'
    });
    assert.equal(missingConfirmation.status, 400);
    assert.equal((await missingConfirmation.json()).error_code, 'ACCOUNT_DELETE_CONFIRMATION_INVALID');
    assert.deepEqual(seen, []);

    const deleted = await invoke({
      current_password: 'not-a-real-password', confirmation: 'DELETE_MY_ACCOUNT',
      idempotency_key: '80000000-0000-4000-8000-000000000099'
    });
    assert.equal(deleted.status, 200, JSON.stringify(await deleted.clone().json()));
    assert.deepEqual(await deleted.json(), {
      ok: true, account_deleted: true, access_revoked: true
    });
    assert.deepEqual(seen, ['global_session_metadata_v1', 'global_account_delete_v1']);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('public invitation inspection is enumeration-safe and never exposes control-plane identifiers', async () => {
  const originalFetch = globalThis.fetch;
  const env = orchestratorEnvironment(async () => {
    throw new Error('invitation inspection must not call an agency business plane');
  });
  const invitationToken = 'opaque-invitation-token-for-inspection-proof';
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    assert.equal(operation, 'invitation_inspect_v1');
    const args = await request.json();
    assert.match(args.p_token_hash, /^\\x[a-f0-9]{64}$/);
    assert.equal(JSON.stringify(args).includes(invitationToken), false);
    return Response.json({
      ok: true, state: 'VALID', next_step: 'SIGN_IN',
      agency_display_name: 'CloudTMS TEST',
      agency_id: IDS.agency, membership_id: IDS.membership,
      internal_only: true
    });
  };
  try {
    const response = await handleCandidateBrokerRequest(new Request(
      'https://candidate-api.test.example/candidate-app/v1/invitations/inspect', {
        method: 'POST', headers: {
          origin: 'https://candidate.test.example', 'cf-connecting-ip': '192.0.2.27',
          'content-type': 'application/json'
        }, body: JSON.stringify({ invitation_token: invitationToken })
      }
    ), env);
    assert.equal(response.status, 200, JSON.stringify(await response.clone().json()));
    assert.deepEqual(await response.json(), {
      ok: true, state: 'VALID', next_step: 'SIGN_IN',
      agency_display_name: 'CloudTMS TEST'
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('invitation acceptance requires verified email and returns only the closed public receipt', async () => {
  const originalFetch = globalThis.fetch;
  const env = orchestratorEnvironment(async () => {
    throw new Error('invitation acceptance must not call an agency business plane');
  });
  const accessToken = await centralAccessToken(env);
  const invitationToken = 'opaque-invitation-token-for-acceptance-proof';
  const idempotencyKey = '80000000-0000-4000-8000-000000000001';
  const seen = [];
  let verifiedEmails = [];
  globalThis.fetch = async request => {
    const operation = new URL(request.url).pathname.split('/').pop();
    seen.push(operation);
    if (operation === 'global_session_metadata_v1') {
      return new Response(JSON.stringify({
        ok: true, account_id: IDS.account, verified_emails: [...verifiedEmails], internal_only: true
      }), { status: 200, headers: { 'content-type': 'application/json' } });
    }
    if (operation === 'invitation_accept_with_context_v2') {
      const args = await request.json();
      assert.deepEqual(args.p_verified_context.verified_emails, ['candidate@example.test']);
      assert.equal(args.p_idempotency_key, idempotencyKey);
      assert.match(args.p_token_hash, /^\\x[a-f0-9]{64}$/);
      assert.equal(JSON.stringify(args).includes(invitationToken), false);
      return Response.json({
        ok: true, state: 'ACTIVE', membership_generation: 4,
        idempotent_replay: false, membership_id_internal: IDS.membership,
        agency_id_internal: IDS.agency, internal_only: true
      });
    }
    if (operation === 'account_agencies_get_v1') {
      return Response.json({
        ok: true,
        memberships_internal: [{
          membership_id: IDS.membership, membership_generation: 4,
          agency_id: IDS.agency, display_name: 'CloudTMS TEST', environment_label: 'TEST',
          local_candidate_id: IDS.candidate, data_plane_id: IDS.dataPlane,
          registry_binding_key: 'CANDIDATE_DATA_PLANE_CLOUDTMS_TEST',
          route_version_id: '80000000-0000-4000-8000-000000000007', route_version: 7
        }],
        pending_invitations_internal: [], internal_only: true
      });
    }
    throw new Error(`unexpected operation ${operation}`);
  };
  const request = () => new Request(
    'https://candidate-api.test.example/candidate-app/v1/invitations/accept', {
      method: 'POST', headers: {
        origin: 'https://candidate.test.example', authorization: `Bearer ${accessToken}`,
        'cf-connecting-ip': '192.0.2.28', 'content-type': 'application/json'
      }, body: JSON.stringify({
        invitation_token: invitationToken, idempotency_key: idempotencyKey
      })
    }
  );
  try {
    const unverified = await handleCandidateBrokerRequest(request(), env);
    assert.equal(unverified.status, 403, JSON.stringify({
      body: await unverified.clone().json(), seen
    }));
    assert.equal((await unverified.json()).error_code, 'EMAIL_VERIFICATION_REQUIRED');

    verifiedEmails = ['candidate@example.test'];
    const accepted = await handleCandidateBrokerRequest(request(), env);
    assert.equal(accepted.status, 200, JSON.stringify(await accepted.clone().json()));
    const acceptedBody = await accepted.json();
    assert.equal(acceptedBody.ok, true);
    assert.equal(acceptedBody.state, 'ACTIVE');
    assert.equal(acceptedBody.membership_generation, 4);
    assert.equal(acceptedBody.idempotent_replay, false);
    assert.equal(acceptedBody.agency_choice.display_name, 'CloudTMS TEST');
    assert.match(acceptedBody.agency_choice.agency_choice_token, /^v4\.1\./);
    assert.deepEqual(seen, [
      'global_session_metadata_v1', 'global_session_metadata_v1',
      'invitation_accept_with_context_v2', 'account_agencies_get_v1'
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
