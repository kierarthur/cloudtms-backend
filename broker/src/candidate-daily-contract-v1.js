const encoder = new TextEncoder();

export const CANDIDATE_DAILY_PUBLIC_PREFIX = '/candidate-app/v1/daily';
export const CANDIDATE_DAILY_SYSTEM_PREFIX = '/candidate-system/v1/google-availability';
export const CANDIDATE_DAILY_PRIVATE_SYSTEM_PREFIX = '/private/candidate-system/v1/google-availability';
export const CANDIDATE_DAILY_HMAC_VERSION = 'v1';
export const CANDIDATE_DAILY_CONTRACT_VERSION = 'CANDIDATE_DAILY_R5_PHASE1A';

const ULID_RE = /^[0-7][0-9A-HJKMNP-TV-Z]{25}$/;
const IDEMPOTENCY_KEY_RE = /^[A-Za-z0-9._~:+\-/]{16,128}$/;
const CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
const DAILY_RETRY_CLASSES = new Set([
  'DO_NOT_RETRY', 'REAUTHENTICATE', 'REFRESH', 'RETRY_SAME_KEY', 'RETRY_AFTER', 'STATUS_CHECK'
]);

const DAILY_ERROR_MESSAGES = Object.freeze({
  AVAILABILITY_DATE_NOT_EDITABLE: 'This availability date cannot be changed.',
  AVAILABILITY_VERSION_CONFLICT: 'Availability changed. Refresh before trying again.',
  BATCH_IN_PROGRESS: 'This batch is still in progress.',
  CANDIDATE_DAILY_DISABLED: 'Daily availability is not currently enabled.',
  CANDIDATE_DAILY_NOT_READY: 'Daily availability is not ready yet.',
  COMMAND_IN_PROGRESS: 'This operation is still in progress.',
  DAILY_GENERATION_UNAVAILABLE: 'The current rota generation is not available yet.',
  DEPENDENCY_UNAVAILABLE: 'A required service is temporarily unavailable.',
  EFFECT_NOT_FOUND: 'The requested operation result was not found.',
  EFFECT_STATUS_UNKNOWN: 'The operation result is not yet known.',
  FORBIDDEN: 'You are not permitted to perform this operation.',
  GENERATION_INCOMPLETE: 'The supplied rota generation is incomplete.',
  IDEMPOTENCY_KEY_REUSED: 'This operation key was already used for a different request.',
  IDENTITY_LINK_AMBIGUOUS: 'The source identity matches more than one Candidate.',
  IDENTITY_LINK_MISSING: 'The source identity is not linked to a Candidate.',
  INTERNAL_ERROR: 'CloudTMS could not complete the request.',
  LEASE_CONFLICT: 'Another worker currently owns this operation.',
  LEASE_EXPIRED_STATUS_REQUIRED: 'The previous lease expired. Check the current status.',
  NOT_FOUND: 'The requested item was not found.',
  PROJECTION_STALE_COMPLETION: 'This projection result is no longer current.',
  RATE_LIMITED: 'Too many requests were made. Try again later.',
  SEMANTIC_REJECTION: 'The request is not valid for the current state.',
  SOURCE_EVENT_CONFLICT: 'This source event conflicts with an existing event.',
  SOURCE_IDENTITY_NOT_READY: 'Candidate source identity is not ready yet.',
  SYSTEM_AUTH_FAILED: 'System authentication failed.',
  UNAUTHENTICATED: 'Authentication is required.',
  VALIDATION_FAILED: 'The request is not valid.'
});

function errorTriple(status, errorCode, retryClass) {
  return `${status}:${errorCode}:${retryClass}`;
}

const BASE_ERROR_TRIPLES = Object.freeze({
  BASELINE_BOOTSTRAP: Object.freeze([
    errorTriple(401, 'UNAUTHENTICATED', 'REAUTHENTICATE'),
    errorTriple(429, 'RATE_LIMITED', 'RETRY_AFTER'),
    errorTriple(500, 'INTERNAL_ERROR', 'RETRY_AFTER'),
    errorTriple(503, 'DEPENDENCY_UNAVAILABLE', 'RETRY_AFTER')
  ]),
  CANDIDATE_DAILY_READ: Object.freeze([
    errorTriple(400, 'VALIDATION_FAILED', 'DO_NOT_RETRY'),
    errorTriple(401, 'UNAUTHENTICATED', 'REAUTHENTICATE'),
    errorTriple(403, 'CANDIDATE_DAILY_DISABLED', 'REFRESH'),
    errorTriple(403, 'FORBIDDEN', 'DO_NOT_RETRY'),
    errorTriple(409, 'SOURCE_IDENTITY_NOT_READY', 'STATUS_CHECK'),
    errorTriple(429, 'RATE_LIMITED', 'RETRY_AFTER'),
    errorTriple(500, 'INTERNAL_ERROR', 'RETRY_AFTER'),
    errorTriple(503, 'CANDIDATE_DAILY_NOT_READY', 'STATUS_CHECK')
  ]),
  CANDIDATE_DAILY_COMMAND: Object.freeze([
    errorTriple(400, 'VALIDATION_FAILED', 'DO_NOT_RETRY'),
    errorTriple(401, 'UNAUTHENTICATED', 'REAUTHENTICATE'),
    errorTriple(403, 'CANDIDATE_DAILY_DISABLED', 'REFRESH'),
    errorTriple(403, 'FORBIDDEN', 'DO_NOT_RETRY'),
    errorTriple(409, 'SOURCE_IDENTITY_NOT_READY', 'STATUS_CHECK'),
    errorTriple(409, 'IDEMPOTENCY_KEY_REUSED', 'DO_NOT_RETRY'),
    errorTriple(409, 'COMMAND_IN_PROGRESS', 'STATUS_CHECK'),
    errorTriple(429, 'RATE_LIMITED', 'RETRY_AFTER'),
    errorTriple(500, 'INTERNAL_ERROR', 'STATUS_CHECK'),
    errorTriple(503, 'CANDIDATE_DAILY_NOT_READY', 'STATUS_CHECK')
  ]),
  LEGACY_COMPAT_READ: Object.freeze([
    errorTriple(400, 'VALIDATION_FAILED', 'DO_NOT_RETRY'),
    errorTriple(401, 'SYSTEM_AUTH_FAILED', 'DO_NOT_RETRY'),
    errorTriple(403, 'FORBIDDEN', 'DO_NOT_RETRY'),
    errorTriple(409, 'SOURCE_IDENTITY_NOT_READY', 'STATUS_CHECK'),
    errorTriple(409, 'IDENTITY_LINK_MISSING', 'STATUS_CHECK'),
    errorTriple(409, 'IDENTITY_LINK_AMBIGUOUS', 'DO_NOT_RETRY'),
    errorTriple(429, 'RATE_LIMITED', 'RETRY_AFTER'),
    errorTriple(500, 'INTERNAL_ERROR', 'RETRY_AFTER'),
    errorTriple(503, 'DEPENDENCY_UNAVAILABLE', 'RETRY_AFTER')
  ]),
  LEGACY_COMPAT_COMMAND: Object.freeze([
    errorTriple(400, 'VALIDATION_FAILED', 'DO_NOT_RETRY'),
    errorTriple(401, 'SYSTEM_AUTH_FAILED', 'DO_NOT_RETRY'),
    errorTriple(403, 'FORBIDDEN', 'DO_NOT_RETRY'),
    errorTriple(409, 'IDEMPOTENCY_KEY_REUSED', 'DO_NOT_RETRY'),
    errorTriple(409, 'COMMAND_IN_PROGRESS', 'STATUS_CHECK'),
    errorTriple(409, 'SOURCE_IDENTITY_NOT_READY', 'STATUS_CHECK'),
    errorTriple(409, 'IDENTITY_LINK_MISSING', 'STATUS_CHECK'),
    errorTriple(409, 'IDENTITY_LINK_AMBIGUOUS', 'DO_NOT_RETRY'),
    errorTriple(429, 'RATE_LIMITED', 'RETRY_AFTER'),
    errorTriple(500, 'INTERNAL_ERROR', 'STATUS_CHECK'),
    errorTriple(503, 'DEPENDENCY_UNAVAILABLE', 'RETRY_AFTER')
  ]),
  SIGNED_SYSTEM_READ: Object.freeze([
    errorTriple(400, 'VALIDATION_FAILED', 'DO_NOT_RETRY'),
    errorTriple(401, 'SYSTEM_AUTH_FAILED', 'DO_NOT_RETRY'),
    errorTriple(403, 'FORBIDDEN', 'DO_NOT_RETRY'),
    errorTriple(429, 'RATE_LIMITED', 'RETRY_AFTER'),
    errorTriple(500, 'INTERNAL_ERROR', 'RETRY_AFTER'),
    errorTriple(503, 'DEPENDENCY_UNAVAILABLE', 'RETRY_AFTER')
  ]),
  SIGNED_SYSTEM_COMMAND: Object.freeze([
    errorTriple(400, 'VALIDATION_FAILED', 'DO_NOT_RETRY'),
    errorTriple(401, 'SYSTEM_AUTH_FAILED', 'DO_NOT_RETRY'),
    errorTriple(403, 'FORBIDDEN', 'DO_NOT_RETRY'),
    errorTriple(409, 'IDEMPOTENCY_KEY_REUSED', 'DO_NOT_RETRY'),
    errorTriple(409, 'COMMAND_IN_PROGRESS', 'STATUS_CHECK'),
    errorTriple(429, 'RATE_LIMITED', 'RETRY_AFTER'),
    errorTriple(500, 'INTERNAL_ERROR', 'STATUS_CHECK'),
    errorTriple(503, 'DEPENDENCY_UNAVAILABLE', 'RETRY_AFTER')
  ])
});

const OPERATION_ERROR_TRIPLES = Object.freeze({
  getCandidateDailyTiles: [errorTriple(409, 'DAILY_GENERATION_UNAVAILABLE', 'STATUS_CHECK')],
  applyCandidateDailyAvailability: [
    errorTriple(409, 'AVAILABILITY_VERSION_CONFLICT', 'REFRESH'),
    errorTriple(422, 'AVAILABILITY_DATE_NOT_EDITABLE', 'DO_NOT_RETRY'),
    errorTriple(422, 'GENERATION_INCOMPLETE', 'REFRESH')
  ],
  getCandidateDailyContent: [errorTriple(404, 'NOT_FOUND', 'DO_NOT_RETRY')],
  getCandidateDailyRunningLateOptions: [
    errorTriple(404, 'NOT_FOUND', 'DO_NOT_RETRY'),
    errorTriple(422, 'SEMANTIC_REJECTION', 'DO_NOT_RETRY')
  ],
  previewCandidateDailyRunningLate: [
    errorTriple(404, 'NOT_FOUND', 'DO_NOT_RETRY'),
    errorTriple(422, 'SEMANTIC_REJECTION', 'DO_NOT_RETRY')
  ],
  sendCandidateDailyRunningLate: [
    errorTriple(404, 'NOT_FOUND', 'DO_NOT_RETRY'),
    errorTriple(409, 'EFFECT_STATUS_UNKNOWN', 'STATUS_CHECK'),
    errorTriple(422, 'SEMANTIC_REJECTION', 'DO_NOT_RETRY')
  ],
  raiseCandidateDailyEmergency: [
    errorTriple(409, 'EFFECT_STATUS_UNKNOWN', 'STATUS_CHECK'),
    errorTriple(422, 'SEMANTIC_REJECTION', 'DO_NOT_RETRY')
  ],
  markCandidateDailyMessageSeen: [
    errorTriple(404, 'NOT_FOUND', 'DO_NOT_RETRY'),
    errorTriple(409, 'EFFECT_STATUS_UNKNOWN', 'STATUS_CHECK')
  ],
  getCandidateDailyEffectStatus: [
    errorTriple(404, 'EFFECT_NOT_FOUND', 'DO_NOT_RETRY'),
    errorTriple(409, 'EFFECT_STATUS_UNKNOWN', 'STATUS_CHECK')
  ],
  googleAvailabilityLegacyTiles: [errorTriple(409, 'DAILY_GENERATION_UNAVAILABLE', 'STATUS_CHECK')],
  googleAvailabilityLegacyApply: [
    errorTriple(409, 'AVAILABILITY_VERSION_CONFLICT', 'REFRESH'),
    errorTriple(422, 'AVAILABILITY_DATE_NOT_EDITABLE', 'DO_NOT_RETRY')
  ],
  googleAvailabilityLegacyTimesheetAuthorisationStatus: [errorTriple(404, 'NOT_FOUND', 'DO_NOT_RETRY')],
  googleAvailabilityPublishRotaGenerations: [
    errorTriple(409, 'SOURCE_EVENT_CONFLICT', 'DO_NOT_RETRY'),
    errorTriple(409, 'BATCH_IN_PROGRESS', 'STATUS_CHECK'),
    errorTriple(422, 'GENERATION_INCOMPLETE', 'DO_NOT_RETRY')
  ],
  googleAvailabilityApplySheetEdits: [
    errorTriple(409, 'SOURCE_EVENT_CONFLICT', 'DO_NOT_RETRY'),
    errorTriple(409, 'BATCH_IN_PROGRESS', 'STATUS_CHECK')
  ],
  googleAvailabilityClaimProjection: [
    errorTriple(409, 'BATCH_IN_PROGRESS', 'STATUS_CHECK'),
    errorTriple(409, 'LEASE_CONFLICT', 'STATUS_CHECK'),
    errorTriple(409, 'IDENTITY_LINK_MISSING', 'STATUS_CHECK'),
    errorTriple(409, 'IDENTITY_LINK_AMBIGUOUS', 'DO_NOT_RETRY')
  ],
  googleAvailabilityCompleteProjection: [
    errorTriple(409, 'LEASE_CONFLICT', 'STATUS_CHECK'),
    errorTriple(409, 'PROJECTION_STALE_COMPLETION', 'STATUS_CHECK'),
    errorTriple(409, 'LEASE_EXPIRED_STATUS_REQUIRED', 'STATUS_CHECK')
  ],
  googleAvailabilityReadSyncStatus: [
    errorTriple(404, 'NOT_FOUND', 'DO_NOT_RETRY'),
    errorTriple(409, 'IDENTITY_LINK_MISSING', 'STATUS_CHECK'),
    errorTriple(409, 'IDENTITY_LINK_AMBIGUOUS', 'DO_NOT_RETRY')
  ],
  googleAvailabilityApplyReconciliation: [
    errorTriple(409, 'SOURCE_EVENT_CONFLICT', 'DO_NOT_RETRY'),
    errorTriple(409, 'IDENTITY_LINK_MISSING', 'STATUS_CHECK'),
    errorTriple(409, 'IDENTITY_LINK_AMBIGUOUS', 'DO_NOT_RETRY')
  ],
  googleAvailabilityLegacyStatus: [errorTriple(404, 'NOT_FOUND', 'DO_NOT_RETRY')],
  googleAvailabilityEffectClaim: [
    errorTriple(409, 'EFFECT_STATUS_UNKNOWN', 'STATUS_CHECK'),
    errorTriple(409, 'LEASE_CONFLICT', 'STATUS_CHECK'),
    errorTriple(409, 'SOURCE_EVENT_CONFLICT', 'DO_NOT_RETRY')
  ],
  googleAvailabilityEffectComplete: [
    errorTriple(404, 'EFFECT_NOT_FOUND', 'DO_NOT_RETRY'),
    errorTriple(409, 'EFFECT_STATUS_UNKNOWN', 'STATUS_CHECK'),
    errorTriple(409, 'LEASE_CONFLICT', 'STATUS_CHECK'),
    errorTriple(409, 'LEASE_EXPIRED_STATUS_REQUIRED', 'STATUS_CHECK')
  ],
  googleAvailabilityEffectStatus: [
    errorTriple(404, 'EFFECT_NOT_FOUND', 'DO_NOT_RETRY'),
    errorTriple(409, 'EFFECT_STATUS_UNKNOWN', 'STATUS_CHECK')
  ]
});

const candidateRead = Object.freeze({
  routeClass: 'CANDIDATE_DAILY_READ',
  accessPolicy: 'CANDIDATE_SURFACE',
  ratePerMinute: 60,
  maxInFlight: 6,
  maxBodyBytes: 32 * 1024,
  deadlineMs: 12_000,
  idempotency: 'FORBIDDEN'
});

const candidateCommand = Object.freeze({
  routeClass: 'CANDIDATE_DAILY_COMMAND',
  accessPolicy: 'CANDIDATE_SURFACE',
  ratePerMinute: 12,
  maxInFlight: 1,
  maxBodyBytes: 32 * 1024,
  deadlineMs: 10_000,
  idempotency: 'REQUIRED'
});

const candidateEffect = Object.freeze({
  ...candidateCommand,
  ratePerMinute: 6,
  maxEffectKeyInFlight: 1,
  deadlineMs: 20_000,
  externalEffect: true
});

const systemRead = Object.freeze({
  routeClass: 'SIGNED_SYSTEM_READ',
  accessPolicy: 'SIGNED_SYSTEM_SYNC',
  ratePerMinute: 120,
  maxInFlight: 8,
  maxBodyBytes: 256 * 1024,
  deadlineMs: 12_000,
  idempotency: 'FORBIDDEN',
  signedSystem: true
});

const systemCommand = Object.freeze({
  routeClass: 'SIGNED_SYSTEM_COMMAND',
  accessPolicy: 'SIGNED_SYSTEM_SYNC',
  ratePerMinute: 120,
  maxInFlight: 8,
  maxBodyBytes: 256 * 1024,
  deadlineMs: 10_000,
  idempotency: 'REQUIRED',
  signedSystem: true
});

const systemEffect = Object.freeze({
  ...systemCommand,
  deadlineMs: 20_000,
  externalEffect: true
});

const legacyRead = Object.freeze({
  ...systemRead,
  routeClass: 'LEGACY_COMPAT_READ',
  accessPolicy: 'LEGACY_COMPAT'
});
const legacyCommand = Object.freeze({
  ...systemCommand,
  routeClass: 'LEGACY_COMPAT_COMMAND',
  accessPolicy: 'LEGACY_COMPAT'
});

function route(method, path, operationId, policy) {
  return Object.freeze({ method, path, operationId, ...policy });
}

export const CANDIDATE_DAILY_ROUTE_CATALOGUE = Object.freeze([
  route('GET', '/candidate-app/v1/daily/tiles', 'getCandidateDailyTiles', candidateRead),
  route('PATCH', '/candidate-app/v1/daily/availability', 'applyCandidateDailyAvailability', candidateCommand),
  route('GET', '/candidate-app/v1/daily/past-shifts', 'getCandidateDailyPastShifts', candidateRead),
  route('GET', '/candidate-app/v1/daily/content/{kind}', 'getCandidateDailyContent', candidateRead),
  route('GET', '/candidate-app/v1/daily/emergency-window', 'getCandidateDailyEmergencyWindow', candidateRead),
  route('POST', '/candidate-app/v1/daily/running-late/options', 'getCandidateDailyRunningLateOptions', candidateRead),
  route('POST', '/candidate-app/v1/daily/running-late/preview', 'previewCandidateDailyRunningLate', candidateRead),
  route('POST', '/candidate-app/v1/daily/running-late/send', 'sendCandidateDailyRunningLate', candidateEffect),
  route('POST', '/candidate-app/v1/daily/emergencies', 'raiseCandidateDailyEmergency', candidateEffect),
  route('POST', '/candidate-app/v1/daily/message-seen', 'markCandidateDailyMessageSeen', candidateCommand),
  route('GET', '/candidate-app/v1/daily/effects/{effect_key}', 'getCandidateDailyEffectStatus', candidateRead),

  route('POST', '/candidate-system/v1/google-availability/legacy/tiles', 'googleAvailabilityLegacyTiles', legacyRead),
  route('POST', '/candidate-system/v1/google-availability/legacy/availability', 'googleAvailabilityLegacyApply', legacyCommand),
  route('POST', '/candidate-system/v1/google-availability/legacy/timesheet-authorisation-status', 'googleAvailabilityLegacyTimesheetAuthorisationStatus', legacyRead),
  route('POST', '/candidate-system/v1/google-availability/rota-generations', 'googleAvailabilityPublishRotaGenerations', systemCommand),
  route('POST', '/candidate-system/v1/google-availability/sheet-edits', 'googleAvailabilityApplySheetEdits', systemCommand),
  route('POST', '/candidate-system/v1/google-availability/projection/claim', 'googleAvailabilityClaimProjection', systemCommand),
  route('POST', '/candidate-system/v1/google-availability/projection/complete', 'googleAvailabilityCompleteProjection', systemCommand),
  route('POST', '/candidate-system/v1/google-availability/sync-status', 'googleAvailabilityReadSyncStatus', systemRead),
  route('POST', '/candidate-system/v1/google-availability/reconciliation', 'googleAvailabilityApplyReconciliation', systemCommand),
  route('POST', '/candidate-system/v1/google-availability/legacy/availability-status', 'googleAvailabilityLegacyStatus', legacyRead),
  route('POST', '/candidate-system/v1/google-availability/effects/claim', 'googleAvailabilityEffectClaim', systemEffect),
  route('POST', '/candidate-system/v1/google-availability/effects/complete', 'googleAvailabilityEffectComplete', systemEffect),
  route('POST', '/candidate-system/v1/google-availability/effects/status', 'googleAvailabilityEffectStatus', systemRead)
]);

export const CANDIDATE_DAILY_BOOTSTRAP_ROUTE = Object.freeze({
  method: 'GET',
  path: '/candidate-app/v1/bootstrap',
  operationId: 'getCandidateBootstrap',
  routeClass: 'BASELINE_BOOTSTRAP',
  accessPolicy: 'BASELINE_BOOTSTRAP',
  signedSystem: false
});

function pathPattern(path) {
  return new RegExp(`^${path.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\\\{[^/]+\\\}/g, '[^/]+')}$`);
}

const compiledRoutes = CANDIDATE_DAILY_ROUTE_CATALOGUE.map((entry) => ({
  ...entry,
  pattern: pathPattern(entry.path)
}));

export function findCandidateDailyRoute(method, pathname) {
  const verb = String(method || '').toUpperCase();
  const path = String(pathname || '');
  return compiledRoutes.find((entry) => entry.method === verb && entry.pattern.test(path)) || null;
}

export function isCandidateDailyPath(pathname) {
  return String(pathname || '').startsWith(`${CANDIDATE_DAILY_PUBLIC_PREFIX}/`);
}

export function isCandidateDailySystemPath(pathname) {
  return String(pathname || '').startsWith(`${CANDIDATE_DAILY_SYSTEM_PREFIX}/`);
}

export function isCandidateDailyPrivateSystemPath(pathname) {
  return String(pathname || '').startsWith(`${CANDIDATE_DAILY_PRIVATE_SYSTEM_PREFIX}/`);
}

export function isValidCorrelationId(value) {
  return ULID_RE.test(String(value || ''));
}

export function createCorrelationId(nowMs = Date.now(), random = crypto.getRandomValues(new Uint8Array(10))) {
  let timestamp = BigInt(Math.max(0, Math.min(Number(nowMs), 281474976710655)));
  const output = new Array(26);
  for (let index = 9; index >= 0; index -= 1) {
    output[index] = CROCKFORD[Number(timestamp & 31n)];
    timestamp >>= 5n;
  }
  let randomValue = 0n;
  for (const byte of random) randomValue = (randomValue << 8n) | BigInt(byte);
  for (let index = 25; index >= 10; index -= 1) {
    output[index] = CROCKFORD[Number(randomValue & 31n)];
    randomValue >>= 5n;
  }
  return output.join('');
}

export function candidateCorrelationId(request) {
  const supplied = String(request?.headers?.get?.('x-correlation-id') || '');
  return isValidCorrelationId(supplied) ? supplied : createCorrelationId();
}

export function requestWithCandidateCorrelation(request, correlationId = candidateCorrelationId(request)) {
  const headers = new Headers(request.headers);
  headers.set('x-correlation-id', correlationId);
  return new Request(request, { headers });
}

export function validateDailyIdempotency(routeDefinition, request, body = null) {
  const value = String(request.headers.get('idempotency-key') || '');
  const bodyObject = body && typeof body === 'object' && !Array.isArray(body) ? body : null;
  if (bodyObject && Object.keys(bodyObject).some((key) => key.toLowerCase().replace(/[-_]/g, '') === 'idempotencykey')) {
    return { ok: false, errorCode: 'VALIDATION_FAILED' };
  }
  if (routeDefinition.idempotency === 'REQUIRED') {
    return IDEMPOTENCY_KEY_RE.test(value)
      ? { ok: true, idempotencyKey: value }
      : { ok: false, errorCode: 'VALIDATION_FAILED' };
  }
  return value
    ? { ok: false, errorCode: 'VALIDATION_FAILED' }
    : { ok: true, idempotencyKey: '' };
}

export function evaluateCandidateDailyPolicy(policy, facts = {}) {
  if (policy === 'BASELINE_BOOTSTRAP') return { allowed: true };
  if (facts.inputsReadable === false) {
    return { allowed: false, reason: policy === 'CANDIDATE_SURFACE' ? 'AUTHORITY_UNREADABLE' : 'DEPENDENCY_UNAVAILABLE' };
  }
  if (policy === 'CANDIDATE_SURFACE') {
    if (facts.globalEnabled !== true) return { allowed: false, reason: 'GLOBAL_DISABLED' };
    if (facts.entitled !== true) return { allowed: false, reason: 'NOT_ENTITLED' };
    if (facts.sourceIdentityReady !== true) return { allowed: false, reason: 'SOURCE_IDENTITY_NOT_READY' };
    if (facts.authorityReady !== true) return { allowed: false, reason: 'AUTHORITY_NOT_READY' };
    return { allowed: true };
  }
  const signed = facts.systemAuthVerified === true && facts.nonceConsumed === true;
  const common = signed && facts.environmentTrusted === true
    && facts.authorityModeCompatible === true && facts.transitionReady === true;
  if (policy === 'LEGACY_COMPAT') {
    return {
      allowed: common && facts.stableOperationIdentity === true && facts.approvedSourceMapping === true,
      ...(common && facts.stableOperationIdentity === true && facts.approvedSourceMapping === true
        ? {} : { reason: 'DEPENDENCY_UNAVAILABLE' })
    };
  }
  if (policy === 'SIGNED_SYSTEM_SYNC') {
    return {
      allowed: common && facts.sourceScopeReady === true,
      ...(common && facts.sourceScopeReady === true ? {} : { reason: 'DEPENDENCY_UNAVAILABLE' })
    };
  }
  return { allowed: false, reason: 'DEPENDENCY_UNAVAILABLE' };
}

export function candidateDailyCapability(facts = {}) {
  const result = evaluateCandidateDailyPolicy('CANDIDATE_SURFACE', facts);
  return result.allowed
    ? { enabled: true }
    : { enabled: false, unavailable_reason: result.reason };
}

export function phase1aCandidateDailyFacts() {
  return Object.freeze({
    inputsReadable: true,
    globalEnabled: false,
    entitled: false,
    sourceIdentityReady: false,
    authorityReady: false
  });
}

export function composePhase1aDailyCapability(bootstrap) {
  const source = bootstrap && typeof bootstrap === 'object' && !Array.isArray(bootstrap) ? bootstrap : {};
  const existingCapabilities = source.capabilities && typeof source.capabilities === 'object'
    && !Array.isArray(source.capabilities) ? source.capabilities : {};
  return {
    ...source,
    capabilities: {
      ...existingCapabilities,
      daily_availability: candidateDailyCapability(phase1aCandidateDailyFacts())
    }
  };
}

function exactObjectKeys(value, allowed, required = allowed) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const keys = Object.keys(value);
  return keys.every((key) => allowed.includes(key)) && required.every((key) => keys.includes(key));
}

export function normalizeCandidateDailyErrorDetails(details) {
  if (details === undefined) return undefined;
  if (!details || typeof details !== 'object' || Array.isArray(details)) return null;
  if (details.kind === 'FIELD_ERRORS') {
    if (!exactObjectKeys(details, ['kind', 'fields']) || !Array.isArray(details.fields)
        || details.fields.length < 1 || details.fields.length > 20) return null;
    const fields = [];
    for (const field of details.fields) {
      if (!exactObjectKeys(field, ['field', 'reason'])
          || typeof field.field !== 'string' || field.field.length > 120
          || typeof field.reason !== 'string' || field.reason.length > 160) return null;
      fields.push({ field: field.field, reason: field.reason });
    }
    return { kind: 'FIELD_ERRORS', fields };
  }
  if (details.kind === 'CONFLICT') {
    if (!exactObjectKeys(details, ['kind', 'current_availability_version', 'status_path'], ['kind'])) return null;
    const normalized = { kind: 'CONFLICT' };
    if (details.current_availability_version !== undefined) {
      if (!Number.isSafeInteger(details.current_availability_version) || details.current_availability_version < 0) return null;
      normalized.current_availability_version = details.current_availability_version;
    }
    if (details.status_path !== undefined) {
      if (typeof details.status_path !== 'string' || details.status_path.length > 240) return null;
      normalized.status_path = details.status_path;
    }
    return normalized;
  }
  if (details.kind === 'RETRY') {
    if (!exactObjectKeys(details, ['kind', 'retry_after_seconds'])
        || !Number.isSafeInteger(details.retry_after_seconds)
        || details.retry_after_seconds < 1 || details.retry_after_seconds > 3600) return null;
    return { kind: 'RETRY', retry_after_seconds: details.retry_after_seconds };
  }
  return null;
}

export function isAllowedCandidateDailyError(routeDefinition, status, errorCode, retryClass) {
  if (!routeDefinition) return false;
  const value = errorTriple(status, errorCode, retryClass);
  return (BASE_ERROR_TRIPLES[routeDefinition.routeClass] || []).includes(value)
    || (OPERATION_ERROR_TRIPLES[routeDefinition.operationId] || []).includes(value);
}

export function candidateDailyAllowedErrorTriples(routeDefinition) {
  if (!routeDefinition) return [];
  return [
    ...(BASE_ERROR_TRIPLES[routeDefinition.routeClass] || []),
    ...(OPERATION_ERROR_TRIPLES[routeDefinition.operationId] || [])
  ].map((value) => {
    const [status, errorCode, retryClass] = value.split(':');
    return { status: Number(status), error_code: errorCode, retry_class: retryClass };
  });
}

export function candidateDailyErrorBody(errorCode, retryClass, correlationId, details = undefined) {
  const safeCorrelationId = isValidCorrelationId(correlationId) ? correlationId : createCorrelationId();
  const safeErrorCode = Object.hasOwn(DAILY_ERROR_MESSAGES, errorCode) ? errorCode : 'INTERNAL_ERROR';
  const safeRetryClass = DAILY_RETRY_CLASSES.has(retryClass) ? retryClass : 'RETRY_AFTER';
  const normalizedDetails = normalizeCandidateDailyErrorDetails(details);
  return {
    ok: false,
    correlation_id: safeCorrelationId,
    error_code: safeErrorCode,
    retry_class: safeRetryClass,
    message: DAILY_ERROR_MESSAGES[safeErrorCode],
    ...(normalizedDetails && normalizedDetails !== null ? { details: normalizedDetails } : {})
  };
}

export function rebuildCandidateDailyErrorBody(routeDefinition, status, source, correlationId) {
  const allowedKeys = ['ok', 'correlation_id', 'error_code', 'retry_class', 'message', 'details'];
  const requiredKeys = ['ok', 'correlation_id', 'error_code', 'retry_class', 'message'];
  if (!exactObjectKeys(source, allowedKeys, requiredKeys) || source.ok !== false
      || source.correlation_id !== correlationId || !isValidCorrelationId(source.correlation_id)
      || typeof source.message !== 'string' || source.message.length < 1 || source.message.length > 160
      || !isAllowedCandidateDailyError(routeDefinition, status, source.error_code, source.retry_class)) return null;
  const details = normalizeCandidateDailyErrorDetails(source.details);
  if (source.details !== undefined && details === null) return null;
  return candidateDailyErrorBody(source.error_code, source.retry_class, correlationId, details);
}

export function dailyErrorResponse(status, errorCode, retryClass, correlationId, details = undefined) {
  const body = candidateDailyErrorBody(errorCode, retryClass, correlationId, details);
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff',
      'x-correlation-id': body.correlation_id
    }
  });
}

export function boundedBodyLength(request, maximumBytes) {
  const raw = request.headers.get('content-length');
  if (raw == null || raw === '') return { ok: true, declared: null };
  if (!/^(?:0|[1-9][0-9]*)$/.test(raw)) return { ok: false };
  const declared = Number(raw);
  return Number.isSafeInteger(declared) && declared <= maximumBytes
    ? { ok: true, declared }
    : { ok: false, tooLarge: declared > maximumBytes };
}

export async function readBoundedDailyJson(request, maximumBytes) {
  const declared = boundedBodyLength(request, maximumBytes);
  if (!declared.ok) {
    return { ok: false, status: declared.tooLarge ? 413 : 400, errorCode: declared.tooLarge ? 'PAYLOAD_TOO_LARGE' : 'VALIDATION_FAILED' };
  }
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > maximumBytes) return { ok: false, status: 413, errorCode: 'PAYLOAD_TOO_LARGE' };
  if (declared.declared != null && declared.declared !== bytes.byteLength) {
    return { ok: false, status: 400, errorCode: 'VALIDATION_FAILED' };
  }
  if (!bytes.byteLength) return { ok: true, body: {}, bytes };
  try {
    const body = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
    if (!body || typeof body !== 'object' || Array.isArray(body)) throw new Error('object required');
    return { ok: true, body, bytes };
  } catch {
    return { ok: false, status: 400, errorCode: 'VALIDATION_FAILED' };
  }
}

export async function sha256Hex(bytes) {
  const value = bytes instanceof Uint8Array ? bytes : encoder.encode(String(bytes == null ? '' : bytes));
  return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', value)))
    .map((byte) => byte.toString(16).padStart(2, '0')).join('');
}
