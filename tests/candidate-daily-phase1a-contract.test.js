import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

import {
  CANDIDATE_DAILY_BOOTSTRAP_ROUTE,
  CANDIDATE_DAILY_ROUTE_CATALOGUE,
  candidateDailyAllowedErrorTriples,
  candidateDailyErrorBody,
  candidateDailyCapability,
  composePhase1aDailyCapability,
  createCorrelationId,
  evaluateCandidateDailyPolicy,
  findCandidateDailyRoute,
  isAllowedCandidateDailyError,
  isValidCorrelationId,
  phase1aCandidateDailyFacts,
  rebuildCandidateDailyErrorBody,
  readBoundedDailyJson,
  sha256Hex,
  validateDailyIdempotency
} from '../broker/src/candidate-daily-contract-v1.js';
import {
  candidateDailyHmacInternals,
  candidateDailySignedMessageBytes,
  normalizeCandidateDailyQuery,
  parseCandidateDailyRawTarget,
  purgeCandidateDailySystemNonces,
  validateCandidateDailyRawHeaders,
  verifyCandidateDailySystemRequest
} from '../broker/src/candidate-daily-hmac-v1.js';
import {
  composeCandidateBootstrapPhase1a,
  handleCandidateDailyPhase1aRequest,
  handleCandidateDailySystemPhase1aRequest
} from '../broker/src/candidate-daily-phase1a.js';
import {
  candidateAppBackendInternals,
  handleCandidateAppRequest
} from '../broker/src/candidate-app-backend.js';
import candidatePrivateWorker from '../broker/src/candidate-private-worker.js';
import {
  candidateBrokerInternals,
  handleCandidateBrokerRequest
} from '../candidate-broker/src/candidate-broker.js';

const vectors = JSON.parse(fs.readFileSync(
  new URL('./fixtures/candidate-daily-r5/canonicalization-v1-vectors.json', import.meta.url),
  'utf8'
));
const errorMatrix = JSON.parse(fs.readFileSync(
  new URL('./fixtures/candidate-daily-r5/r5-operation-error-matrix.json', import.meta.url),
  'utf8'
));

const expectedRoutes = [
  ['GET', '/candidate-app/v1/daily/tiles', 'getCandidateDailyTiles', 'CANDIDATE_DAILY_READ', 'CANDIDATE_SURFACE'],
  ['PATCH', '/candidate-app/v1/daily/availability', 'applyCandidateDailyAvailability', 'CANDIDATE_DAILY_COMMAND', 'CANDIDATE_SURFACE'],
  ['GET', '/candidate-app/v1/daily/past-shifts', 'getCandidateDailyPastShifts', 'CANDIDATE_DAILY_READ', 'CANDIDATE_SURFACE'],
  ['GET', '/candidate-app/v1/daily/content/{kind}', 'getCandidateDailyContent', 'CANDIDATE_DAILY_READ', 'CANDIDATE_SURFACE'],
  ['GET', '/candidate-app/v1/daily/emergency-window', 'getCandidateDailyEmergencyWindow', 'CANDIDATE_DAILY_READ', 'CANDIDATE_SURFACE'],
  ['POST', '/candidate-app/v1/daily/running-late/options', 'getCandidateDailyRunningLateOptions', 'CANDIDATE_DAILY_READ', 'CANDIDATE_SURFACE'],
  ['POST', '/candidate-app/v1/daily/running-late/preview', 'previewCandidateDailyRunningLate', 'CANDIDATE_DAILY_READ', 'CANDIDATE_SURFACE'],
  ['POST', '/candidate-app/v1/daily/running-late/send', 'sendCandidateDailyRunningLate', 'CANDIDATE_DAILY_COMMAND', 'CANDIDATE_SURFACE'],
  ['POST', '/candidate-app/v1/daily/emergencies', 'raiseCandidateDailyEmergency', 'CANDIDATE_DAILY_COMMAND', 'CANDIDATE_SURFACE'],
  ['POST', '/candidate-app/v1/daily/message-seen', 'markCandidateDailyMessageSeen', 'CANDIDATE_DAILY_COMMAND', 'CANDIDATE_SURFACE'],
  ['GET', '/candidate-app/v1/daily/effects/{effect_key}', 'getCandidateDailyEffectStatus', 'CANDIDATE_DAILY_READ', 'CANDIDATE_SURFACE'],
  ['POST', '/candidate-system/v1/google-availability/legacy/tiles', 'googleAvailabilityLegacyTiles', 'LEGACY_COMPAT_READ', 'LEGACY_COMPAT'],
  ['POST', '/candidate-system/v1/google-availability/legacy/availability', 'googleAvailabilityLegacyApply', 'LEGACY_COMPAT_COMMAND', 'LEGACY_COMPAT'],
  ['POST', '/candidate-system/v1/google-availability/legacy/timesheet-authorisation-status', 'googleAvailabilityLegacyTimesheetAuthorisationStatus', 'LEGACY_COMPAT_READ', 'LEGACY_COMPAT'],
  ['POST', '/candidate-system/v1/google-availability/rota-generations', 'googleAvailabilityPublishRotaGenerations', 'SIGNED_SYSTEM_COMMAND', 'SIGNED_SYSTEM_SYNC'],
  ['POST', '/candidate-system/v1/google-availability/sheet-edits', 'googleAvailabilityApplySheetEdits', 'SIGNED_SYSTEM_COMMAND', 'SIGNED_SYSTEM_SYNC'],
  ['POST', '/candidate-system/v1/google-availability/projection/claim', 'googleAvailabilityClaimProjection', 'SIGNED_SYSTEM_COMMAND', 'SIGNED_SYSTEM_SYNC'],
  ['POST', '/candidate-system/v1/google-availability/projection/complete', 'googleAvailabilityCompleteProjection', 'SIGNED_SYSTEM_COMMAND', 'SIGNED_SYSTEM_SYNC'],
  ['POST', '/candidate-system/v1/google-availability/sync-status', 'googleAvailabilityReadSyncStatus', 'SIGNED_SYSTEM_READ', 'SIGNED_SYSTEM_SYNC'],
  ['POST', '/candidate-system/v1/google-availability/reconciliation', 'googleAvailabilityApplyReconciliation', 'SIGNED_SYSTEM_COMMAND', 'SIGNED_SYSTEM_SYNC'],
  ['POST', '/candidate-system/v1/google-availability/legacy/availability-status', 'googleAvailabilityLegacyStatus', 'LEGACY_COMPAT_READ', 'LEGACY_COMPAT'],
  ['POST', '/candidate-system/v1/google-availability/effects/claim', 'googleAvailabilityEffectClaim', 'SIGNED_SYSTEM_COMMAND', 'SIGNED_SYSTEM_SYNC'],
  ['POST', '/candidate-system/v1/google-availability/effects/complete', 'googleAvailabilityEffectComplete', 'SIGNED_SYSTEM_COMMAND', 'SIGNED_SYSTEM_SYNC'],
  ['POST', '/candidate-system/v1/google-availability/effects/status', 'googleAvailabilityEffectStatus', 'SIGNED_SYSTEM_READ', 'SIGNED_SYSTEM_SYNC']
];

function toBytes(value) {
  return new TextEncoder().encode(value);
}

function toBase64(bytes) {
  return Buffer.from(bytes).toString('base64');
}

async function hmacHex(secret, bytes) {
  const key = await crypto.subtle.importKey(
    'raw', toBytes(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  return Buffer.from(await crypto.subtle.sign('HMAC', key, bytes)).toString('hex');
}

async function vectorRequest(vector, overrides = {}) {
  const body = overrides.body ?? vector.body;
  const bodyBytes = toBytes(body);
  const normalizedPath = overrides.normalized_path ?? vector.normalized_path;
  const normalizedQuery = overrides.normalized_query ?? vector.normalized_query;
  const method = overrides.method ?? vector.method;
  const contentHash = overrides.content_hash ?? await sha256Hex(bodyBytes);
  const fields = {
    method,
    normalizedPath,
    normalizedQuery,
    timestamp: overrides.timestamp ?? vector.timestamp,
    nonce: overrides.nonce ?? vector.nonce,
    contentSha256: contentHash,
    idempotencyKey: overrides.idempotency_key ?? vector.idempotency_key,
    correlationId: overrides.correlation_id ?? vector.correlation_id
  };
  const message = candidateDailySignedMessageBytes(fields, bodyBytes);
  const signature = overrides.signature_hex ?? await hmacHex(vectors.key_ascii, message);
  const headers = {
    'content-type': 'application/json; charset=utf-8',
    'content-length': String(bodyBytes.byteLength),
    'x-cloudtms-key-id': overrides.key_id ?? vector.key_id,
    'x-cloudtms-signature-version': 'v1',
    'x-cloudtms-timestamp': fields.timestamp,
    'x-cloudtms-nonce': fields.nonce,
    'x-cloudtms-content-sha256': contentHash,
    'x-cloudtms-signature': signature,
    'x-correlation-id': fields.correlationId,
    ...(fields.idempotencyKey ? { 'idempotency-key': fields.idempotencyKey } : {})
  };
  return new Request(
    `https://broker.test${normalizedPath}${normalizedQuery ? `?${normalizedQuery}` : ''}`,
    { method, headers, body: bodyBytes }
  );
}

function memoryNonceStore() {
  const keys = new Set();
  const objects = new Map();
  return {
    keys,
    objects,
    async put(key, _body, options = {}) {
      if (keys.has(key)) return null;
      keys.add(key);
      const consumedEpoch = Number(options.customMetadata?.consumed_epoch);
      objects.set(key, {
        key,
        ...(Number.isFinite(consumedEpoch) ? { uploaded: new Date(consumedEpoch * 1000) } : {}),
        customMetadata: { ...(options.customMetadata || {}) }
      });
      return { key };
    },
    async list({ prefix = '', cursor = undefined } = {}) {
      assert.equal(cursor, undefined);
      return {
        objects: [...objects.values()].filter((object) => object.key.startsWith(prefix)),
        truncated: false
      };
    },
    async delete(keyOrKeys) {
      for (const key of Array.isArray(keyOrKeys) ? keyOrKeys : [keyOrKeys]) {
        keys.delete(key);
        objects.delete(key);
      }
    }
  };
}

function hmacEnv(store = memoryNonceStore()) {
  return {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_KEY_ID: vectors.active_key_id,
    CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_SECRET: vectors.key_ascii,
    R2: store
  };
}

test('Phase 1A route catalogue exactly matches all 24 merged-R5 Daily operations', () => {
  const actual = CANDIDATE_DAILY_ROUTE_CATALOGUE.map((entry) => [
    entry.method, entry.path, entry.operationId, entry.routeClass, entry.accessPolicy
  ]);
  assert.deepEqual(actual, expectedRoutes);
  for (const [method, path, operationId] of expectedRoutes) {
    const concretePath = path.replace('{kind}', 'policy').replace('{effect_key}', 'effect_123');
    assert.equal(findCandidateDailyRoute(method, concretePath)?.operationId, operationId);
  }
  assert.equal(findCandidateDailyRoute('DELETE', '/candidate-app/v1/daily/tiles'), null);
  assert.equal(findCandidateDailyRoute('POST', '/candidate-system/v1/google-availability/not-approved'), null);

  const candidateRead = findCandidateDailyRoute('GET', '/candidate-app/v1/daily/tiles');
  const candidateCommand = findCandidateDailyRoute('PATCH', '/candidate-app/v1/daily/availability');
  const candidateEffect = findCandidateDailyRoute('POST', '/candidate-app/v1/daily/emergencies');
  const systemRead = findCandidateDailyRoute('POST', '/candidate-system/v1/google-availability/sync-status');
  const systemEffect = findCandidateDailyRoute('POST', '/candidate-system/v1/google-availability/effects/claim');
  assert.deepEqual(
    [candidateRead.ratePerMinute, candidateRead.maxInFlight, candidateRead.maxBodyBytes, candidateRead.deadlineMs],
    [60, 6, 32 * 1024, 12_000]
  );
  assert.deepEqual(
    [candidateCommand.ratePerMinute, candidateCommand.maxInFlight, candidateCommand.maxBodyBytes, candidateCommand.deadlineMs],
    [12, 1, 32 * 1024, 10_000]
  );
  assert.deepEqual(
    [candidateEffect.ratePerMinute, candidateEffect.maxEffectKeyInFlight, candidateEffect.deadlineMs],
    [6, 1, 20_000]
  );
  assert.deepEqual(
    [systemRead.ratePerMinute, systemRead.maxInFlight, systemRead.maxBodyBytes, systemRead.deadlineMs],
    [120, 8, 256 * 1024, 12_000]
  );
  assert.deepEqual(
    [systemEffect.ratePerMinute, systemEffect.maxInFlight, systemEffect.deadlineMs],
    [120, 8, 20_000]
  );
});

test('all 25 Daily/bootstrap error matrices are exact and every encoder output is closed-schema', () => {
  const byOperation = new Map([
    [CANDIDATE_DAILY_BOOTSTRAP_ROUTE.operationId, CANDIDATE_DAILY_BOOTSTRAP_ROUTE],
    ...CANDIDATE_DAILY_ROUTE_CATALOGUE.map((route) => [route.operationId, route])
  ]);
  assert.equal(errorMatrix.operations.length, 25);
  assert.equal(byOperation.size, 25);
  const sortTriple = (left, right) => (
    left.status - right.status
      || left.error_code.localeCompare(right.error_code)
      || left.retry_class.localeCompare(right.retry_class)
  );
  for (const operation of errorMatrix.operations) {
    const route = byOperation.get(operation.operation_id);
    assert.ok(route, operation.operation_id);
    const expected = operation.errors.map((entry) => ({ ...entry })).sort(sortTriple);
    const actual = candidateDailyAllowedErrorTriples(route).sort(sortTriple);
    assert.deepEqual(actual, expected, operation.operation_id);
    for (const entry of operation.errors) {
      assert.equal(isAllowedCandidateDailyError(
        route, entry.status, entry.error_code, entry.retry_class
      ), true, `${operation.operation_id}:${entry.status}:${entry.error_code}`);
      const encoded = candidateDailyErrorBody(
        entry.error_code, entry.retry_class, '01K2ABCDEF0123456789ABCDEF'
      );
      assert.deepEqual(Object.keys(encoded).sort(), [
        'correlation_id', 'error_code', 'message', 'ok', 'retry_class'
      ]);
      assert.equal(encoded.ok, false);
      assert.equal(encoded.message.length >= 1 && encoded.message.length <= 160, true);
    }
  }

  const fieldDetails = {
    kind: 'FIELD_ERRORS', fields: [{ field: 'availability', reason: 'Invalid value.' }]
  };
  const conflictDetails = {
    kind: 'CONFLICT', current_availability_version: 12, status_path: '/candidate-app/v1/daily/tiles'
  };
  const retryDetails = { kind: 'RETRY', retry_after_seconds: 60 };
  for (const details of [fieldDetails, conflictDetails, retryDetails]) {
    assert.deepEqual(
      candidateDailyErrorBody('VALIDATION_FAILED', 'DO_NOT_RETRY', '01K2ABCDEF0123456789ABCDEF', details).details,
      details
    );
  }
  assert.equal(candidateDailyErrorBody(
    'VALIDATION_FAILED', 'DO_NOT_RETRY', '01K2ABCDEF0123456789ABCDEF',
    { unavailable_reason: 'GLOBAL_DISABLED' }
  ).details, undefined);
});

test('public Daily response boundary rebuilds only allowed errors and rejects private leakage or drift', async () => {
  const route = findCandidateDailyRoute('GET', '/candidate-app/v1/daily/tiles');
  const correlationId = '01K2ABCDEF0123456789ABCDEF';
  const makeResponse = (status, body, headerCorrelation = correlationId) => new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'x-correlation-id': headerCorrelation
    }
  });
  const acceptedBody = candidateDailyErrorBody(
    'CANDIDATE_DAILY_DISABLED', 'REFRESH', correlationId
  );
  const accepted = await candidateBrokerInternals.publicSafeDailyResponse(
    makeResponse(403, acceptedBody), correlationId, route
  );
  assert.equal(accepted.status, 403);
  assert.deepEqual(await accepted.json(), acceptedBody);

  const rejectedBodies = [
    { ...acceptedBody, internal_stack: 'must-never-cross' },
    Object.fromEntries(Object.entries(acceptedBody).filter(([key]) => key !== 'message')),
    { ...acceptedBody, error_code: 'NOT_FOUND' },
    { ok: true, correlation_id: correlationId, internal_token: 'must-never-cross' },
    { ...acceptedBody, correlation_id: '01K2ABCDEF0123456789ABCDEG' },
    { ...acceptedBody, details: { unavailable_reason: 'GLOBAL_DISABLED' } }
  ];
  for (const body of rejectedBodies) {
    const response = await candidateBrokerInternals.publicSafeDailyResponse(
      makeResponse(body.error_code === 'NOT_FOUND' ? 403 : 403, body), correlationId, route
    );
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), candidateDailyErrorBody(
      'CANDIDATE_DAILY_NOT_READY', 'STATUS_CHECK', correlationId
    ));
  }

  const bootstrapLeak = await candidateBrokerInternals.publicSafeDailyResponse(
    makeResponse(503, { ...candidateDailyErrorBody(
      'DEPENDENCY_UNAVAILABLE', 'RETRY_AFTER', correlationId
    ), internal_database: 'private' }),
    correlationId,
    CANDIDATE_DAILY_BOOTSTRAP_ROUTE
  );
  assert.deepEqual(await bootstrapLeak.json(), candidateDailyErrorBody(
    'DEPENDENCY_UNAVAILABLE', 'RETRY_AFTER', correlationId
  ));
});

test('single policy authority preserves bootstrap and fails each Candidate surface input closed', () => {
  assert.deepEqual(evaluateCandidateDailyPolicy('BASELINE_BOOTSTRAP', {}), { allowed: true });
  assert.deepEqual(candidateDailyCapability(phase1aCandidateDailyFacts()), {
    enabled: false,
    unavailable_reason: 'GLOBAL_DISABLED'
  });
  assert.equal(evaluateCandidateDailyPolicy('CANDIDATE_SURFACE', {
    globalEnabled: true, entitled: false, sourceIdentityReady: true, authorityReady: true
  }).reason, 'NOT_ENTITLED');
  assert.equal(evaluateCandidateDailyPolicy('CANDIDATE_SURFACE', {
    globalEnabled: true, entitled: true, sourceIdentityReady: false, authorityReady: true
  }).reason, 'SOURCE_IDENTITY_NOT_READY');
  assert.equal(evaluateCandidateDailyPolicy('CANDIDATE_SURFACE', {
    globalEnabled: true, entitled: true, sourceIdentityReady: true, authorityReady: false
  }).reason, 'AUTHORITY_NOT_READY');
  assert.equal(evaluateCandidateDailyPolicy('CANDIDATE_SURFACE', {
    inputsReadable: false
  }).reason, 'AUTHORITY_UNREADABLE');
  assert.deepEqual(evaluateCandidateDailyPolicy('CANDIDATE_SURFACE', {
    globalEnabled: true, entitled: true, sourceIdentityReady: true, authorityReady: true
  }), { allowed: true });
  assert.equal(evaluateCandidateDailyPolicy('LEGACY_COMPAT', {
    globalEnabled: false,
    systemAuthVerified: true,
    nonceConsumed: true,
    environmentTrusted: true,
    stableOperationIdentity: true,
    approvedSourceMapping: true,
    authorityModeCompatible: true,
    transitionReady: true
  }).allowed, true);
  assert.equal(evaluateCandidateDailyPolicy('SIGNED_SYSTEM_SYNC', {
    globalEnabled: false,
    systemAuthVerified: true,
    nonceConsumed: true,
    environmentTrusted: true,
    sourceScopeReady: true,
    authorityModeCompatible: true,
    transitionReady: true
  }).allowed, true);
});

test('bootstrap is additive, keeps accepted legacy daily entitlement and remains globally disabled', () => {
  const baseline = {
    ok: true,
    entitlements: { contract: true, daily: true, gck_present: true },
    feature_flags: { existing: true }
  };
  const actual = composePhase1aDailyCapability(baseline);
  assert.deepEqual(actual.entitlements, baseline.entitlements);
  assert.deepEqual(actual.feature_flags, baseline.feature_flags);
  assert.deepEqual(actual.capabilities.daily_availability, {
    enabled: false,
    unavailable_reason: 'GLOBAL_DISABLED'
  });
  assert.deepEqual(composeCandidateBootstrapPhase1a(baseline), actual);
});

test('candidate correlation and idempotency middleware implement the closed R5 rules', () => {
  for (let index = 0; index < 100; index += 1) assert.equal(isValidCorrelationId(createCorrelationId()), true);
  const command = findCandidateDailyRoute('PATCH', '/candidate-app/v1/daily/availability');
  const read = findCandidateDailyRoute('GET', '/candidate-app/v1/daily/tiles');
  assert.equal(validateDailyIdempotency(command, new Request('https://test/', {
    headers: { 'idempotency-key': 'candidate_key_0001' }
  })).ok, true);
  assert.equal(validateDailyIdempotency(command, new Request('https://test/')).ok, false);
  assert.equal(validateDailyIdempotency(command, new Request('https://test/', {
    headers: { 'idempotency-key': 'candidate_key_0001' }
  }), { idempotency_key: 'duplicate' }).ok, false);
  assert.equal(validateDailyIdempotency(read, new Request('https://test/', {
    headers: { 'idempotency-key': 'candidate_key_0001' }
  })).ok, false);
});

test('public Daily transport enforces 32 KiB bodies and route-specific idempotency while dark', async () => {
  const command = findCandidateDailyRoute('PATCH', '/candidate-app/v1/daily/availability');
  const read = findCandidateDailyRoute('GET', '/candidate-app/v1/daily/tiles');
  await assert.rejects(
    candidateBrokerInternals.validateCandidateDailyTransport(new Request(
      'https://candidate.test/candidate-app/v1/daily/availability', {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: '{}'
      }
    ), command),
    (error) => error?.status === 400 && error?.code === 'VALIDATION_FAILED'
  );
  await assert.rejects(
    candidateBrokerInternals.validateCandidateDailyTransport(new Request(
      'https://candidate.test/candidate-app/v1/daily/availability', {
        method: 'PATCH',
        headers: {
          'content-type': 'text/plain',
          'idempotency-key': 'phase1a-valid-key-0000'
        },
        body: '{}'
      }
    ), command),
    (error) => error?.status === 400 && error?.code === 'VALIDATION_FAILED'
  );
  await assert.rejects(
    candidateBrokerInternals.validateCandidateDailyTransport(new Request(
      'https://candidate.test/candidate-app/v1/daily/availability', {
        method: 'PATCH',
        headers: {
          'content-type': 'application/json',
          'idempotency-key': 'phase1a-valid-key-0001'
        },
        body: JSON.stringify({ idempotency_key: 'forbidden-body-copy' })
      }
    ), command),
    (error) => error?.status === 400 && error?.code === 'VALIDATION_FAILED'
  );
  await assert.rejects(
    candidateBrokerInternals.validateCandidateDailyTransport(new Request(
      'https://candidate.test/candidate-app/v1/daily/availability', {
        method: 'PATCH',
        headers: {
          'content-type': 'application/json',
          'idempotency-key': 'phase1a-valid-key-0002'
        },
        body: JSON.stringify({ payload: 'x'.repeat(33 * 1024) })
      }
    ), command),
    (error) => error?.status === 413 && error?.code === 'PAYLOAD_TOO_LARGE'
  );
  await assert.rejects(
    candidateBrokerInternals.validateCandidateDailyTransport(new Request(
      'https://candidate.test/candidate-app/v1/daily/tiles', {
        headers: { 'idempotency-key': 'phase1a-read-key-0001' }
      }
    ), read),
    (error) => error?.status === 400 && error?.code === 'VALIDATION_FAILED'
  );
  await candidateBrokerInternals.validateCandidateDailyTransport(new Request(
    'https://candidate.test/candidate-app/v1/daily/availability', {
      method: 'PATCH',
      headers: {
        'content-type': 'application/json',
        'idempotency-key': 'phase1a-valid-key-0003'
      },
      body: '{}'
    }
  ), command);
});

test('Candidate JSON framing rejects under-declared and over-declared Content-Length exactly', async () => {
  for (const declared of ['1', '3']) {
    const request = new Request('https://candidate.test/candidate-app/v1/daily/availability', {
      method: 'PATCH',
      headers: {
        'content-type': 'application/json',
        'content-length': declared,
        'idempotency-key': 'phase1a-framing-key-0001'
      },
      body: '{}'
    });
    const parsed = await readBoundedDailyJson(request.clone(), 32 * 1024);
    assert.deepEqual(
      { ok: parsed.ok, status: parsed.status, errorCode: parsed.errorCode },
      { ok: false, status: 400, errorCode: 'VALIDATION_FAILED' },
      `declared=${declared}`
    );
    await assert.rejects(
      candidateBrokerInternals.validateCandidateDailyTransport(
        request,
        findCandidateDailyRoute('PATCH', '/candidate-app/v1/daily/availability')
      ),
      (error) => error?.status === 400 && error?.code === 'VALIDATION_FAILED'
    );
  }
});

test('all Daily browser-side 403 paths remain public 403 FORBIDDEN errors', async () => {
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_ALLOWED_ORIGINS: 'https://allowed.example',
    CANDIDATE_ALLOW_NATIVE_CLIENTS: 'FALSE'
  };
  const correlationId = '01K2ABCDEF0123456789ABCDEF';
  const cases = [
    new Request('https://candidate.test/candidate-app/v1/daily/tiles', {
      headers: { origin: 'https://rejected.example', 'x-correlation-id': correlationId }
    }),
    new Request('https://candidate.test/candidate-app/v1/daily/tiles', {
      headers: { 'x-correlation-id': correlationId }
    }),
    new Request('https://candidate.test/candidate-app/v1/daily/tiles', {
      method: 'OPTIONS',
      headers: {
        origin: 'https://allowed.example',
        'access-control-request-method': 'GET',
        'access-control-request-headers': 'x-private-internal',
        'x-correlation-id': correlationId
      }
    })
  ];
  for (const request of cases) {
    const response = await handleCandidateBrokerRequest(request, env);
    assert.equal(response.status, 403);
    assert.deepEqual(await response.json(), candidateDailyErrorBody(
      'FORBIDDEN', 'DO_NOT_RETRY', correlationId
    ));
  }
});

test('unchanged R5 query and raw-parser corpus passes production Worker canonicalization', () => {
  for (const entry of vectors.query_canonicalization_cases) {
    assert.equal(normalizeCandidateDailyQuery(entry.raw_query), entry.normalized_query, entry.id);
  }
  for (const entry of vectors.raw_parser_cases) {
    let parsed = null;
    let error = null;
    try {
      validateCandidateDailyRawHeaders(entry.headers);
      parsed = parseCandidateDailyRawTarget(entry.raw_target);
    } catch (caught) {
      error = caught.message;
    }
    assert.equal(error, entry.expected_error ?? null, entry.id);
    if (!error) {
      assert.equal(parsed.normalizedPath, entry.expected_path, entry.id);
      assert.equal(parsed.normalizedQuery, entry.expected_query, entry.id);
    }
  }
});

test('unchanged R5 positive vectors match production Worker prefix/message/hash/HMAC bytes', async () => {
  for (const vector of vectors.positive_vectors) {
    const body = toBytes(vector.body);
    const fields = {
      method: vector.method,
      normalizedPath: vector.normalized_path,
      normalizedQuery: vector.normalized_query,
      timestamp: vector.timestamp,
      nonce: vector.nonce,
      contentSha256: vector.body_sha256,
      idempotencyKey: vector.idempotency_key,
      correlationId: vector.correlation_id
    };
    const message = candidateDailySignedMessageBytes(fields, body);
    assert.equal(await sha256Hex(body), vector.body_sha256, vector.id);
    assert.equal(toBase64(message.slice(0, message.byteLength - body.byteLength)), vector.canonical_prefix_base64, vector.id);
    assert.equal(await sha256Hex(message), vector.signed_message_sha256, vector.id);
    assert.equal(await hmacHex(vectors.key_ascii, message), vector.signature_hex, vector.id);
  }
});

test('actual private Worker verifier accepts valid signed requests and rejects transport nonce replay', async () => {
  const vector = vectors.positive_vectors.find((entry) => entry.id === 'route_valid_legacy_availability');
  const store = memoryNonceStore();
  const env = hmacEnv(store);
  const first = await verifyCandidateDailySystemRequest(await vectorRequest(vector), env, {
    nowSeconds: vectors.verification_epoch,
    nonceStore: store
  });
  assert.equal(first.ok, true);
  assert.equal(first.route.operationId, 'googleAvailabilityLegacyApply');
  const second = await verifyCandidateDailySystemRequest(await vectorRequest(vector), env, {
    nowSeconds: vectors.verification_epoch,
    nonceStore: store
  });
  assert.deepEqual(
    { ok: second.ok, status: second.status, code: second.errorCode },
    { ok: false, status: 401, code: 'SYSTEM_AUTH_FAILED' }
  );
  assert.equal([...store.keys][0].startsWith('candidate-daily-google-nonces/v1/test/test-primary-v1/'), true);
});

test('nonce retention is at least 600 seconds from server consumption at both clock-skew edges', async () => {
  const vector = vectors.positive_vectors.find((entry) => entry.id === 'route_valid_legacy_tiles');
  const store = memoryNonceStore();
  const nowSeconds = 1786790405;
  for (const [timestamp, nonce] of [
    [nowSeconds - candidateDailyHmacInternals.MAX_CLOCK_SKEW_SECONDS, 'pastSkewNonce0000000001'],
    [nowSeconds + candidateDailyHmacInternals.MAX_CLOCK_SKEW_SECONDS, 'futureSkewNonce0000001']
  ]) {
    const result = await verifyCandidateDailySystemRequest(await vectorRequest(vector, {
      timestamp: String(timestamp), nonce
    }), hmacEnv(store), { nowSeconds, nonceStore: store });
    assert.equal(result.ok, true, `${timestamp}:${nonce}`);
  }
  assert.equal(store.keys.size, 2);
  for (const object of store.objects.values()) {
    assert.equal(object.customMetadata.signed_request_epoch === String(nowSeconds - 300)
      || object.customMetadata.signed_request_epoch === String(nowSeconds + 300), true);
    assert.equal(object.customMetadata.consumed_epoch, String(nowSeconds));
    assert.equal(object.customMetadata.expires_epoch, String(nowSeconds + 600));
  }
  await purgeCandidateDailySystemNonces({ R2: store }, nowSeconds + 599);
  assert.equal(store.keys.size, 2);
  await purgeCandidateDailySystemNonces({ R2: store }, nowSeconds + 600);
  assert.equal(store.keys.size, 0);
});

test('actual private Worker verifier fails closed for body, key, timestamp, query and configuration drift', async () => {
  const vector = vectors.positive_vectors.find((entry) => entry.id === 'route_valid_legacy_tiles');
  const wrongBody = await verifyCandidateDailySystemRequest(await vectorRequest(vector, {
    body: `${vector.body} `,
    content_hash: vector.body_sha256,
    signature_hex: vector.signature_hex
  }), hmacEnv(), { nowSeconds: vectors.verification_epoch });
  assert.equal(wrongBody.errorCode, 'SYSTEM_AUTH_FAILED');

  const wrongKey = await verifyCandidateDailySystemRequest(await vectorRequest(vector, {
    key_id: 'retired-key'
  }), hmacEnv(), { nowSeconds: vectors.verification_epoch });
  assert.equal(wrongKey.errorCode, 'SYSTEM_AUTH_FAILED');

  const stale = await verifyCandidateDailySystemRequest(await vectorRequest(vector), hmacEnv(), {
    nowSeconds: vectors.verification_epoch + 301
  });
  assert.equal(stale.errorCode, 'SYSTEM_AUTH_FAILED');

  const query = await verifyCandidateDailySystemRequest(await vectorRequest(vector, {
    normalized_query: 'a=1'
  }), hmacEnv(), { nowSeconds: vectors.verification_epoch });
  assert.equal(query.errorCode, 'VALIDATION_FAILED');

  const unavailable = await verifyCandidateDailySystemRequest(await vectorRequest(vector), {
    CANDIDATE_APP_ENVIRONMENT: 'TEST', R2: memoryNonceStore()
  }, { nowSeconds: vectors.verification_epoch });
  assert.equal(unavailable.errorCode, 'DEPENDENCY_UNAVAILABLE');
});

test('all unchanged R5 negative vectors are rejected by the production parser/verifier boundary', async () => {
  const positives = new Map(vectors.positive_vectors.map((entry) => [entry.id, entry]));
  const rejected = [];
  for (const entry of vectors.negative_vectors) {
    const base = positives.get(entry.base_id);
    const mutation = entry.mutation;
    if (mutation.duplicate_header || mutation.ambiguous_header_casing || mutation.header_outer_whitespace
        || mutation.transfer_ambiguity) {
      const headerPairs = [
        ['Content-Length', String(toBytes(base.body).byteLength)],
        ['X-CloudTMS-Key-Id', base.key_id],
        ['X-CloudTMS-Signature-Version', 'v1'],
        ['X-CloudTMS-Timestamp', base.timestamp],
        ['X-CloudTMS-Nonce', base.nonce],
        ['X-CloudTMS-Content-SHA256', base.body_sha256],
        ['X-CloudTMS-Signature', base.signature_hex],
        ['X-Correlation-Id', base.correlation_id],
        ...(base.idempotency_key ? [['Idempotency-Key', base.idempotency_key]] : [])
      ];
      if (mutation.duplicate_header) {
        const original = headerPairs.find(([name]) => name.toLowerCase() === mutation.duplicate_header.toLowerCase());
        headerPairs.push([mutation.duplicate_header, original?.[1] || 'duplicate']);
      }
      if (mutation.ambiguous_header_casing) {
        const original = headerPairs.find(([name]) => name.toLowerCase() === mutation.ambiguous_header_casing.toLowerCase());
        headerPairs.push([mutation.ambiguous_header_casing, original?.[1] || 'duplicate']);
      }
      if (mutation.header_outer_whitespace) {
        const target = headerPairs.find(([name]) => name.toLowerCase() === mutation.header_outer_whitespace.toLowerCase());
        target[1] = ` ${target[1]}`;
      }
      if (mutation.transfer_ambiguity) headerPairs.push(['Transfer-Encoding', 'chunked']);
      assert.throws(() => validateCandidateDailyRawHeaders(headerPairs), undefined, entry.id);
      rejected.push(entry.id);
      continue;
    }
    if (mutation.path_invalid) {
      assert.throws(
        () => parseCandidateDailyRawTarget(`${base.normalized_path}${mutation.path_invalid}`),
        undefined,
        entry.id
      );
      rejected.push(entry.id);
      continue;
    }
    if (mutation.method || mutation.normalized_path) {
      const method = mutation.method || base.method;
      const path = mutation.normalized_path || base.normalized_path;
      assert.equal(findCandidateDailyRoute(method, path), null, entry.id);
      rejected.push(entry.id);
      continue;
    }

    let body = base.body;
    if (mutation.body_append) body += mutation.body_append;
    if (mutation.body) body = mutation.body;
    if (mutation.body_replace) body = body.replace(...mutation.body_replace);
    if (mutation.body_prefix_bom) body = `\ufeff${body}`;
    const overrides = {
      body,
      timestamp: mutation.timestamp ?? base.timestamp,
      correlation_id: mutation.correlation_id ?? base.correlation_id,
      idempotency_key: mutation.idempotency_key ?? base.idempotency_key,
      key_id: mutation.key_id ?? base.key_id,
      normalized_query: mutation.raw_query ? normalizeCandidateDailyQuery(mutation.raw_query) : base.normalized_query,
      content_hash: mutation.content_hash ?? (mutation.resign ? undefined : base.body_sha256),
      signature_hex: mutation.signature_hex ?? (mutation.resign ? undefined : base.signature_hex)
    };
    const store = memoryNonceStore();
    const first = await verifyCandidateDailySystemRequest(await vectorRequest(base, overrides), hmacEnv(store), {
      nowSeconds: vectors.verification_epoch,
      nonceStore: store
    });
    if (mutation.verify_twice && first.ok) {
      const second = await verifyCandidateDailySystemRequest(await vectorRequest(base, overrides), hmacEnv(store), {
        nowSeconds: vectors.verification_epoch,
        nonceStore: store
      });
      assert.equal(second.ok, false, entry.id);
    } else {
      assert.equal(first.ok, false, entry.id);
    }
    rejected.push(entry.id);
  }
  assert.deepEqual(rejected, vectors.negative_vectors.map((entry) => entry.id));
});

test('Phase 1A candidate and signed-system dispatch is present but dark and effect-free', async () => {
  const candidate = await handleCandidateDailyPhase1aRequest(new Request(
    'https://private.test/candidate-app/v1/daily/tiles',
    { headers: { 'x-correlation-id': '01K2ABCDEF0123456789ABCDEF' } }
  ), { session_id: 'session' });
  assert.equal(candidate.status, 403);
  assert.deepEqual(await candidate.json(), {
    ok: false,
    correlation_id: '01K2ABCDEF0123456789ABCDEF',
    error_code: 'CANDIDATE_DAILY_DISABLED',
    retry_class: 'REFRESH',
    message: 'Daily availability is not currently enabled.'
  });

  const vector = vectors.positive_vectors.find((entry) => entry.id === 'route_valid_legacy_tiles');
  const system = await handleCandidateDailySystemPhase1aRequest(await vectorRequest(vector), hmacEnv(), {
    nowSeconds: vectors.verification_epoch
  });
  assert.equal(system.status, 503);
  assert.equal((await system.json()).error_code, 'DEPENDENCY_UNAVAILABLE');
});

test('actual bootstrap/private dispatch preserves baseline and adds only disabled Daily capability', async () => {
  const originalFetch = globalThis.fetch;
  const session = {
    session_id: '00000000-0000-4000-8000-000000000901',
    id: '00000000-0000-4000-8000-000000000901',
    account_id: '00000000-0000-4000-8000-000000000902',
    environment: 'TEST',
    selected_candidate_id: '00000000-0000-4000-8000-000000000903',
    status: 'ACTIVE',
    rotation: 0,
    expires_at_utc: '2099-08-16T11:00:00.000Z',
    absolute_expires_at_utc: '2099-08-16T12:00:00.000Z'
  };
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'phase1a-test-private-session-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const token = await candidateAppBackendInternals.createAccessToken(env, session);
  globalThis.fetch = async (url) => {
    assert.match(String(url), /candidate_app_sessions/);
    return Response.json([session]);
  };
  try {
    const deps = {
      routeAudience: 'PRIVATE',
      async rpc(name) {
        if (name === 'candidate_daily_tiles_get_v1') throw new Error('CANDIDATE_DAILY_DISABLED');
        assert.equal(name, 'candidate_app_bootstrap_v1');
        return {
          ok: true,
          entitlements: { contract: true, daily: true, gck_present: true },
          feature_flags: { existing: true },
          capabilities: { existing_capability: { enabled: true } }
        };
      }
    };
    const response = await handleCandidateAppRequest(new Request(
      'https://private.test/candidate-app/v1/bootstrap', {
        headers: {
          authorization: `Bearer ${token}`,
          'x-correlation-id': '01K2ABCDEF0123456789ABCDEJ'
        }
      }
    ), env, {}, deps);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('x-correlation-id'), '01K2ABCDEF0123456789ABCDEJ');
    const body = await response.json();
    assert.deepEqual(body.entitlements, { contract: true, daily: true, gck_present: true });
    assert.deepEqual(body.capabilities.existing_capability, { enabled: true });
    assert.deepEqual(body.capabilities.daily_availability, {
      enabled: false,
      unavailable_reason: 'AUTHORITY_UNREADABLE'
    });

    const daily = await handleCandidateAppRequest(new Request(
      'https://private.test/candidate-app/v1/daily/tiles', {
        headers: {
          authorization: `Bearer ${token}`,
          'x-correlation-id': '01K2ABCDEF0123456789ABCDEK'
        }
      }
    ), env, {}, deps);
    assert.equal(daily.status, 403);
    assert.equal((await daily.json()).error_code, 'CANDIDATE_DAILY_DISABLED');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('signed Google system request crosses public broker and private service once, stays dark, and rejects nonce replay', async () => {
  const nonceStore = memoryNonceStore();
  const env = {
    ...hmacEnv(nonceStore),
    CANDIDATE_APP_PUBLIC_URL: 'https://candidate.test.invalid',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'phase1a-private-service-test-secret',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'phase1a-private-session-test-secret',
    CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET: 'phase1a-private-challenge-test-secret',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'phase1a-private-upload-test-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    CANDIDATE_DAILY_SYSTEM_RATE_LIMIT: { async limit() { return { success: true }; } }
  };
  let privateCalls = 0;
  env.CLOUDTMS_PRIVATE = {
    async fetch(request) {
      privateCalls += 1;
      return candidatePrivateWorker.fetch(request, env, { waitUntil() {} });
    }
  };
  const vector = vectors.positive_vectors.find((entry) => entry.id === 'route_valid_legacy_tiles');
  const correlationId = createCorrelationId();
  const request = await vectorRequest(vector, {
    timestamp: String(Math.floor(Date.now() / 1000)),
    nonce: 'phase1aRoundTripNonce01',
    correlation_id: correlationId
  });
  const first = await handleCandidateBrokerRequest(request.clone(), env);
  assert.equal(first.status, 503);
  assert.equal(first.headers.get('access-control-allow-origin'), null);
  assert.equal(first.headers.get('x-correlation-id'), correlationId);
  assert.deepEqual(await first.json(), {
    ok: false,
    correlation_id: correlationId,
    error_code: 'DEPENDENCY_UNAVAILABLE',
    retry_class: 'RETRY_AFTER',
    message: 'A required service is temporarily unavailable.'
  });
  assert.equal(privateCalls, 1);
  assert.equal(nonceStore.keys.size, 2);

  const replay = await handleCandidateBrokerRequest(request.clone(), env);
  assert.equal(replay.status, 401);
  assert.equal((await replay.json()).error_code, 'SYSTEM_AUTH_FAILED');
  assert.equal(privateCalls, 2);
  assert.equal(nonceStore.keys.size, 3);
});

test('signed-system public edge generates a valid correlation for missing/invalid input and never calls private', async () => {
  const vector = vectors.positive_vectors.find((entry) => entry.id === 'route_valid_legacy_tiles');
  let privateCalls = 0;
  const env = {
    CLOUDTMS_PRIVATE: { async fetch() { privateCalls += 1; return new Response(null, { status: 503 }); } }
  };
  for (const supplied of [null, 'not-a-ulid']) {
    const source = await vectorRequest(vector);
    const headers = new Headers(source.headers);
    if (supplied === null) headers.delete('x-correlation-id');
    else headers.set('x-correlation-id', supplied);
    const request = new Request(source, { headers });
    const response = await handleCandidateBrokerRequest(request, env);
    const body = await response.json();
    assert.equal(response.status, 400);
    assert.equal(body.error_code, 'VALIDATION_FAILED');
    assert.equal(body.retry_class, 'DO_NOT_RETRY');
    assert.equal(body.message, 'The request is not valid.');
    assert.equal(isValidCorrelationId(body.correlation_id), true);
    assert.equal(response.headers.get('x-correlation-id'), body.correlation_id);
  }
  assert.equal(privateCalls, 0);
});

test('signed-system pre-auth limiter cannot be bypassed by rotating unverified key IDs', async () => {
  const rateCounts = new Map();
  let privateCalls = 0;
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'phase1a-private-service-rate-secret',
    CANDIDATE_DAILY_GOOGLE_HMAC_PRIMARY_KEY_ID: 'trusted-primary',
    CANDIDATE_DAILY_GOOGLE_HMAC_OVERLAP_KEY_ID: 'trusted-overlap',
    CANDIDATE_DAILY_SYSTEM_RATE_LIMIT: {
      async limit({ key }) {
        const count = (rateCounts.get(key) || 0) + 1;
        rateCounts.set(key, count);
        return { success: count <= 120 };
      }
    },
    CLOUDTMS_PRIVATE: {
      async fetch(request) {
        privateCalls += 1;
        const correlationId = request.headers.get('x-correlation-id');
        return new Response(JSON.stringify(candidateDailyErrorBody(
          'DEPENDENCY_UNAVAILABLE', 'RETRY_AFTER', correlationId
        )), {
          status: 503,
          headers: {
            'content-type': 'application/json; charset=utf-8',
            'x-correlation-id': correlationId
          }
        });
      }
    }
  };
  const baseHeaders = {
    'content-type': 'application/json; charset=utf-8',
    'content-length': '2',
    'idempotency-key': 'phase1a-rate-key-0001',
    'x-cloudtms-signature-version': 'v1',
    'x-cloudtms-timestamp': '1786790400',
    'x-cloudtms-nonce': 'rateLimitNonce000001',
    'x-cloudtms-content-sha256': '0'.repeat(64),
    'x-cloudtms-signature': '0'.repeat(64),
    'x-correlation-id': '01K2ABCDEF0123456789ABCDEF',
    'cf-connecting-ip': '203.0.113.9'
  };
  for (let index = 0; index < 150; index += 1) {
    const response = await handleCandidateBrokerRequest(new Request(
      'https://broker.test/candidate-system/v1/google-availability/legacy/availability', {
        method: 'POST',
        headers: { ...baseHeaders, 'x-cloudtms-key-id': `attacker-key-${index}` },
        body: '{}'
      }
    ), env);
    assert.equal(response.status, index < 120 ? 503 : 429, `request ${index + 1}`);
  }
  assert.equal(privateCalls, 120);
  assert.deepEqual(candidateBrokerInternals.candidateDailySystemRateKeys(new Request(
    'https://broker.test/', {
      headers: { 'cf-connecting-ip': '203.0.113.9', 'x-cloudtms-key-id': 'untrusted' }
    }
  ), env), ['preauth-ip:203.0.113.9', 'key:invalid-key']);
  assert.deepEqual(candidateBrokerInternals.candidateDailySystemRateKeys(new Request(
    'https://broker.test/', {
      headers: { 'cf-connecting-ip': '203.0.113.9', 'x-cloudtms-key-id': 'trusted-overlap' }
    }
  ), env), ['preauth-ip:203.0.113.9', 'key:trusted-overlap']);
});

test('Fetch-normalized duplicate signed headers are rejected by the real public/private path', async () => {
  const nonceStore = memoryNonceStore();
  const env = {
    ...hmacEnv(nonceStore),
    CANDIDATE_APP_PUBLIC_URL: 'https://candidate.test.invalid',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'phase1a-private-service-duplicate-header-secret',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'phase1a-private-session-duplicate-header-secret',
    CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET: 'phase1a-private-challenge-duplicate-header-secret',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'phase1a-private-upload-duplicate-header-secret',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
    CANDIDATE_DAILY_SYSTEM_RATE_LIMIT: { async limit() { return { success: true }; } }
  };
  env.CLOUDTMS_PRIVATE = {
    async fetch(request) { return candidatePrivateWorker.fetch(request, env, { waitUntil() {} }); }
  };
  const body = '{}';
  const headers = new Headers([
    ['content-type', 'application/json; charset=utf-8'],
    ['content-length', '2'],
    ['x-cloudtms-key-id', vectors.active_key_id],
    ['x-cloudtms-key-id', 'attacker-duplicate'],
    ['x-cloudtms-signature-version', 'v1'],
    ['x-cloudtms-timestamp', '1786790400'],
    ['x-cloudtms-nonce', 'duplicateHeaderNonce1'],
    ['x-cloudtms-content-sha256', '0'.repeat(64)],
    ['x-cloudtms-signature', '0'.repeat(64)],
    ['x-correlation-id', '01K2ABCDEF0123456789ABCDEF']
  ]);
  assert.match(headers.get('x-cloudtms-key-id'), /,/);
  const response = await handleCandidateBrokerRequest(new Request(
    'https://broker.test/candidate-system/v1/google-availability/legacy/tiles', {
      method: 'POST', headers, body
    }
  ), env);
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error_code, 'VALIDATION_FAILED');
});
