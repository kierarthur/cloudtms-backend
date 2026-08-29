const encoder = new TextEncoder();

export const CANDIDATE_DAILY_PUBLIC_PREFIX = '/candidate-app/v1/daily';
export const CANDIDATE_DAILY_SYSTEM_PREFIX = '/candidate-system/v1/google-availability';
export const CANDIDATE_DAILY_PRIVATE_SYSTEM_PREFIX = '/private/candidate-system/v1/google-availability';
export const CANDIDATE_DAILY_HMAC_VERSION = 'v1';
export const CANDIDATE_DAILY_CONTRACT_VERSION = 'CANDIDATE_DAILY_R8_PHASE1B';

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
  IDENTITY_LINK_CONFLICT: 'The source identity conflicts with an existing Candidate link.',
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
    errorTriple(409, 'IDENTITY_LINK_CONFLICT', 'DO_NOT_RETRY'),
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

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const SHA256_RE = /^[a-f0-9]{64}$/;
const SOURCE_HMAC_RE = /^[a-f0-9]{64}$/;
const ACTION_ROW_SIGNATURE_RE = /^[a-f0-9]{32}$/;
const OPAQUE_TOKEN_RE = /^[A-Za-z0-9._~-]{16,512}$/;
const AVAILABILITY_VALUES = new Set(['PENDING', 'NOT_AVAILABLE', 'LONG_DAY', 'NIGHT', 'LONG_DAY_OR_NIGHT']);
const LEGACY_VALUES = new Set(['', 'N/A', 'LD', 'N', 'LD/N']);
const FRESHNESS_REASONS = new Set([
  'GENERATION_MISSING', 'GENERATION_INCOMPLETE', 'GENERATION_STALE',
  'PROJECTION_LAG', 'TERMINAL_OUTBOX', 'IDENTITY_NOT_READY'
]);

function stringValue(value, minimum = 0, maximum = 2048, pattern = null) {
  return typeof value === 'string' && value.length >= minimum && value.length <= maximum
    && (!pattern || pattern.test(value));
}

function integerValue(value, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) {
  return Number.isSafeInteger(value) && value >= minimum && value <= maximum;
}

function dateTimeValue(value) {
  return stringValue(value, 1, 64) && Number.isFinite(Date.parse(value));
}

function calendarDateValue(value) {
  if (!stringValue(value, 10, 10, DATE_RE)) return false;
  const date = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value;
}

function normalizeFreshness(source) {
  const required = ['generation_version', 'generation_published_at', 'generation_age_seconds',
    'canonical_version', 'accepted_canonical_cursor', 'required_visible_cursor',
    'projection_oldest_pending_seconds', 'generation_max_age_seconds', 'projection_warning_seconds',
    'ready', 'reasons', 'overlay_proof_cursor', 'effective_visible_cursor', 'delivered_visible_cursor'];
  if (!exactObjectKeys(source, required) || !integerValue(source.generation_version, 1)
      || !dateTimeValue(source.generation_published_at)
      || !required.filter((key) => key.endsWith('_seconds') || key.endsWith('_cursor') || key === 'canonical_version')
        .every((key) => integerValue(source[key],
          ['generation_max_age_seconds', 'projection_warning_seconds'].includes(key) ? 1 : 0))
      || typeof source.ready !== 'boolean' || !Array.isArray(source.reasons)
      || source.reasons.length > 10 || !source.reasons.every((item) => FRESHNESS_REASONS.has(item))) return null;
  return { ...source, reasons: [...source.reasons] };
}

export function normalizeCandidateDailyBookedSource(source) {
  if (!exactObjectKeys(source, ['generation_id', 'work_date', 'source_row_hash', 'booking_id'])
      || !stringValue(source.generation_id, 36, 36) || !UUID_RE.test(source.generation_id)
      || !stringValue(source.work_date, 10, 10) || !DATE_RE.test(source.work_date)
      || !stringValue(source.source_row_hash, 64, 64) || !/^[a-f0-9]{64}$/.test(source.source_row_hash)
      || !stringValue(source.booking_id, 1, 128) || source.booking_id.trim() !== source.booking_id) return null;
  const date = new Date(`${source.work_date}T00:00:00Z`);
  if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0, 10) !== source.work_date) return null;
  return { generation_id: source.generation_id, work_date: source.work_date,
    source_row_hash: source.source_row_hash, booking_id: source.booking_id };
}

function normalizeActionTarget(source) {
  if (source == null) return null;
  if (!source || typeof source !== 'object' || Array.isArray(source)) return null;
  const kind = source.target_kind;
  if (kind === 'BOOKED_DAILY_SHIFT') {
    if (!exactObjectKeys(source, ['target_kind', 'source'])) return null;
    const bookedSource = normalizeCandidateDailyBookedSource(source.source);
    return bookedSource ? { target_kind: kind, source: bookedSource } : null;
  } else if (kind === 'TIMESHEET_DETAIL') {
    if (!exactObjectKeys(source, ['target_kind', 'timesheet_id', 'workflow_id', 'row_signature'],
      ['target_kind', 'timesheet_id', 'row_signature']) || !UUID_RE.test(source.timesheet_id)
      || (source.workflow_id !== undefined && source.workflow_id !== null && !UUID_RE.test(source.workflow_id))
      || !ACTION_ROW_SIGNATURE_RE.test(source.row_signature)) return null;
  } else if (kind === 'CONTRACT_WEEK_DETAIL') {
    if (!exactObjectKeys(source, ['target_kind', 'contract_week_id', 'timesheet_id', 'workflow_id', 'row_signature'],
      ['target_kind', 'contract_week_id', 'row_signature']) || !UUID_RE.test(source.contract_week_id)
      || (source.timesheet_id !== undefined && source.timesheet_id !== null && !UUID_RE.test(source.timesheet_id))
      || (source.workflow_id !== undefined && source.workflow_id !== null && !UUID_RE.test(source.workflow_id))
      || !ACTION_ROW_SIGNATURE_RE.test(source.row_signature)) return null;
  } else if (kind === 'WORKFLOW_DETAIL') {
    if (!exactObjectKeys(source, ['target_kind', 'workflow_id', 'workflow_generation', 'row_signature'])
      || !UUID_RE.test(source.workflow_id) || !integerValue(source.workflow_generation, 1)
      || !ACTION_ROW_SIGNATURE_RE.test(source.row_signature)) return null;
  } else return null;
  return { ...source };
}

function normalizeDailyTilesResult(source) {
  const allowed = ['candidate_id', 'window_start', 'window_end', 'generation_id', 'generation_version',
    'availability_version', 'freshness', 'cohorts', 'tiles'];
  const required = allowed;
  if (!exactObjectKeys(source, allowed, required) || !UUID_RE.test(source.candidate_id)
      || !calendarDateValue(source.window_start) || !calendarDateValue(source.window_end)
      || !UUID_RE.test(source.generation_id) || !integerValue(source.generation_version, 1)
      || !integerValue(source.availability_version, 0) || !Array.isArray(source.cohorts)
      || !Array.isArray(source.tiles) || source.tiles.length !== 14) return null;
  const freshness = normalizeFreshness(source.freshness);
  if (!freshness) return null;
  const cohorts = source.cohorts.map((item) => exactObjectKeys(item, ['display_name', 'role', 'subject_token'],
    ['display_name', 'role']) && stringValue(item.display_name, 1, 160)
    && stringValue(item.role, 1, 100)
    && (item.subject_token === undefined || OPAQUE_TOKEN_RE.test(item.subject_token)) ? { ...item } : null);
  if (cohorts.some((item) => item === null)) return null;
  const tiles = [];
  for (const item of source.tiles) {
    const tileAllowed = ['date', 'display_day', 'display_date', 'booked', 'system_blocked', 'editable',
      'status', 'availability', 'shift_info', 'hospital', 'ward', 'job_title', 'booking_ref', 'shift_type',
      'booking_id', 'timesheet_authorised', 'timesheet_eligible', 'action_target',
      'shift_starts_at', 'shift_ends_at', 'week_ending_date', 'break_entry'];
    const tileRequired = ['date', 'display_day', 'display_date', 'booked', 'system_blocked', 'editable',
      'status', 'availability'];
    if (!exactObjectKeys(item, tileAllowed, tileRequired) || !calendarDateValue(item.date)
        || !stringValue(item.display_day, 1, 16) || !stringValue(item.display_date, 1, 16)
        || typeof item.booked !== 'boolean' || typeof item.system_blocked !== 'boolean'
        || typeof item.editable !== 'boolean' || !AVAILABILITY_VALUES.has(item.availability)
        || !new Set(['BOOKED', 'BLOCKED', ...AVAILABILITY_VALUES]).has(item.status)) return null;
    for (const key of ['shift_info', 'hospital', 'ward', 'job_title', 'booking_ref', 'shift_type', 'booking_id']) {
      if (item[key] !== undefined && item[key] !== null
          && !stringValue(item[key], key === 'ward' ? 0 : 1, key === 'shift_info' ? 256 : 160)) return null;
    }
    for (const key of ['timesheet_authorised', 'timesheet_eligible']) {
      if (item[key] !== undefined && item[key] !== null && typeof item[key] !== 'boolean') return null;
    }
    // These existing database timestamps are required for UK-time Rota tiles.
    // Keep the response closed: accept only optional, timezone-qualified values.
    for (const key of ['shift_starts_at', 'shift_ends_at']) {
      if (item[key] !== undefined && item[key] !== null
          && (!dateTimeValue(item[key])
            || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(item[key]))) return null;
    }
    if (item.week_ending_date !== undefined) {
      const day = new Date(`${item.date}T00:00:00Z`);
      day.setUTCDate(day.getUTCDate() + (7 - day.getUTCDay()) % 7);
      if (!item.booked || !calendarDateValue(item.week_ending_date)
          || day.toISOString().slice(0, 10) !== item.week_ending_date) return null;
    }
    let breakEntry;
    if (item.break_entry !== undefined) {
      // PostgreSQL strips nested nulls from future, non-editable booked tiles.
      // Restore only the explicit NOT_APPLICABLE null, never missing editable authority.
      const rawEntry = item.break_entry;
      const entry = rawEntry?.applicable === false && rawEntry.source === 'NOT_APPLICABLE'
        && !Object.prototype.hasOwnProperty.call(rawEntry, 'mode')
        ? { ...rawEntry, mode: null } : rawEntry;
      if (!item.booked || !exactObjectKeys(entry, ['applicable', 'mode', 'source', 'reason', 'context_version', 'context_token'])
          || typeof entry.applicable !== 'boolean'
          || !['START_END_TIMES', 'DURATION_MINUTES', null].includes(entry.mode)
          || !['CONTRACT_OVERRIDE', 'CLIENT_SETTINGS', 'DEFAULT', 'NOT_APPLICABLE'].includes(entry.source)
          || !stringValue(entry.reason, 1, 160)
          || entry.context_version !== 'CANDIDATE_BREAK_ENTRY_V1'
          || !stringValue(entry.context_token, 64, 64, /^[a-f0-9]{64}$/)
          || (entry.applicable && (entry.mode === null || entry.source === 'NOT_APPLICABLE'))
          || (!entry.applicable && (entry.mode !== null || entry.source !== 'NOT_APPLICABLE'))) return null;
      breakEntry = entry;
    }
    const target = item.action_target === undefined ? undefined : normalizeActionTarget(item.action_target);
    if (item.action_target !== undefined && item.action_target !== null && !target) return null;
    if (target?.target_kind === 'BOOKED_DAILY_SHIFT'
        && (!item.booked || item.timesheet_authorised === true || item.timesheet_eligible === false
          || !item.week_ending_date || !item.break_entry?.applicable
          || !item.shift_starts_at || !item.shift_ends_at
          || Date.parse(item.shift_ends_at) <= Date.parse(item.shift_starts_at)
          || target.source.generation_id !== source.generation_id
          || target.source.work_date !== item.date || target.source.booking_id !== item.booking_id)) return null;
    tiles.push({ ...item, ...(breakEntry !== undefined ? { break_entry: breakEntry } : {}),
      ...(item.action_target !== undefined ? { action_target: target } : {}) });
  }
  return { ...source, freshness, cohorts, tiles };
}

function normalizePastShiftsResult(source) {
  if (!exactObjectKeys(source, ['items', 'next_cursor', 'limit'], ['items', 'limit'])
      || !Array.isArray(source.items) || source.items.length > 100 || !integerValue(source.limit, 1, 100)
      || (source.next_cursor !== undefined && source.next_cursor !== null
        && !OPAQUE_TOKEN_RE.test(source.next_cursor))) return null;
  const items = source.items.map((item) => {
    const allowed = ['date', 'display_date', 'shift_type', 'starts_at', 'ends_at', 'notes', 'hospital',
      'ward', 'booking_reference', 'job_title', 'status', 'action_target'];
    const required = ['date', 'display_date', 'shift_type', 'starts_at', 'ends_at', 'hospital',
      'ward', 'booking_reference', 'job_title', 'status'];
    if (!exactObjectKeys(item, allowed, required) || !DATE_RE.test(item.date)
        || !stringValue(item.display_date, 1, 64) || !stringValue(item.shift_type, 1, 80)
        || !dateTimeValue(item.starts_at) || !dateTimeValue(item.ends_at)
        || !stringValue(item.hospital, 1, 160) || !stringValue(item.status, 1, 80)) return null;
    for (const [key, maximum] of [['notes', 1000], ['ward', 160], ['booking_reference', 128], ['job_title', 160]]) {
      if (item[key] !== null && !stringValue(item[key], 0, maximum)) return null;
    }
    const target = item.action_target === undefined ? undefined : normalizeActionTarget(item.action_target);
    if (item.action_target !== undefined && item.action_target !== null && !target) return null;
    return { ...item, ...(item.action_target !== undefined ? { action_target: target } : {}) };
  });
  return items.some((item) => item === null) ? null : { ...source, items };
}

function normalizeCohortDisplay(item) {
  return exactObjectKeys(item, ['display_name', 'role', 'subject_token'], ['display_name', 'role'])
    && stringValue(item.display_name, 1, 160) && stringValue(item.role, 1, 100)
    && (item.subject_token === undefined || OPAQUE_TOKEN_RE.test(item.subject_token)) ? { ...item } : null;
}

function normalizeEmergencyContactDisplay(item) {
  return exactObjectKeys(item, ['display_name', 'role', 'callable_mobile', 'subject_token'],
    ['display_name', 'role', 'callable_mobile'])
    && stringValue(item.display_name, 1, 160) && stringValue(item.role, 1, 100)
    && stringValue(item.callable_mobile, 7, 32) && /^[+0-9 ()-]+$/.test(item.callable_mobile)
    && (item.subject_token === undefined || OPAQUE_TOKEN_RE.test(item.subject_token)) ? { ...item } : null;
}

function normalizeEmergencyContactGroups(source) {
  if (!exactObjectKeys(source, ['current', 'previous', 'next'])
      || !['current', 'previous', 'next'].every((key) => Array.isArray(source[key]) && source[key].length <= 100)) {
    return null;
  }
  const output = {};
  for (const key of ['current', 'previous', 'next']) {
    output[key] = source[key].map(normalizeEmergencyContactDisplay);
    if (output[key].some((entry) => entry === null)) return null;
  }
  return output;
}

function normalizeCandidateEffectResult(source) {
  if (!exactObjectKeys(source, ['effect_key', 'operation', 'status', 'created_at', 'updated_at', 'safe_message'],
    ['effect_key', 'operation', 'status', 'created_at', 'updated_at']) || !OPAQUE_TOKEN_RE.test(source.effect_key)
    || !['RUNNING_LATE_SEND', 'CANNOT_ATTEND', 'LEAVE_EARLY', 'DNA', 'MESSAGE_SEEN'].includes(source.operation)
    || !['IN_PROGRESS', 'COMPLETED', 'FAILED_FINAL', 'UNKNOWN'].includes(source.status)
    || !dateTimeValue(source.created_at) || !dateTimeValue(source.updated_at)
    || (source.safe_message !== undefined && !stringValue(source.safe_message, 0, 320))) return null;
  return { ...source };
}

function normalizeLegacyTilesResult(source) {
  if (!exactObjectKeys(source, ['tiles', 'candidateName', 'lastLoadedAt'], ['tiles'])
      || !Array.isArray(source.tiles) || source.tiles.length !== 14
      || (source.candidateName !== undefined && !stringValue(source.candidateName, 0, 160))
      || (source.lastLoadedAt !== undefined && !dateTimeValue(source.lastLoadedAt))) return null;
  const tiles = source.tiles.map((item) => {
    const allowed = ['ymd', 'displayDay', 'displayDate', 'booked', 'editable', 'status', 'shiftInfo',
      'hospital', 'ward', 'jobTitle', 'bookingRef', 'shiftType', 'booking_id', 'timesheet_authorised', 'timesheet_eligible'];
    const required = ['ymd', 'displayDay', 'displayDate', 'booked', 'editable', 'status'];
    if (!exactObjectKeys(item, allowed, required) || !DATE_RE.test(item.ymd)
        || !stringValue(item.displayDay, 2, 16) || !stringValue(item.displayDate, 5, 16)
        || typeof item.booked !== 'boolean' || typeof item.editable !== 'boolean'
        || !stringValue(item.status, 1, 80)) return null;
    for (const key of ['shiftInfo', 'hospital', 'ward', 'jobTitle', 'bookingRef', 'shiftType', 'booking_id']) {
      if (item[key] !== undefined && item[key] !== null && !stringValue(item[key], 1, key === 'shiftInfo' ? 256 : 160)) return null;
    }
    for (const key of ['timesheet_authorised', 'timesheet_eligible']) {
      if (item[key] !== undefined && item[key] !== null && typeof item[key] !== 'boolean') return null;
    }
    return { ...item };
  });
  if (tiles.some((item) => item === null)) return null;
  return { ...source, tiles };
}

function normalizeIndexedOutcomes(source, options = {}) {
  if (!Array.isArray(source) || source.length < 1 || source.length > (options.maximum || 100)) return null;
  const output = [];
  for (const item of source) {
    if (!item || typeof item !== 'object' || Array.isArray(item) || !integerValue(item.index, 0, 99)) return null;
    if (options.kind === 'generation') {
      if (!exactObjectKeys(item, ['index', 'status', 'generation_id', 'generation_version', 'error_code'], ['index', 'status'])
          || !['COMMITTED', 'REPLAYED', 'REJECTED'].includes(item.status)
          || (item.generation_id !== undefined && !UUID_RE.test(item.generation_id))
          || (item.generation_version !== undefined && !integerValue(item.generation_version, 1))) return null;
    } else if (options.kind === 'sheet') {
      if (!exactObjectKeys(item, ['index', 'status', 'availability_version', 'error_code'], ['index', 'status'])
          || !['COMMITTED', 'REPLAYED', 'REJECTED'].includes(item.status)
          || (item.availability_version !== undefined && !integerValue(item.availability_version, 1))) return null;
    } else if (options.kind === 'projection') {
      if (!exactObjectKeys(item, ['index', 'accepted', 'state', 'delivered_visible_cursor'], ['index', 'accepted', 'state'])
          || typeof item.accepted !== 'boolean'
          || !['DELIVERED', 'RETRY', 'DEFERRED_OVERLAY', 'TERMINAL', 'LEASE_CONFLICT'].includes(item.state)
          || (item.delivered_visible_cursor !== undefined && !integerValue(item.delivered_visible_cursor, 0))) return null;
    } else if (options.kind === 'reconciliation') {
      if (!exactObjectKeys(item, ['index', 'classification', 'error_code'], ['index', 'classification'])
          || !['MATCH', 'REPAIR_PROJECTION', 'CANONICAL_COMMAND_REQUIRED', 'NOT_ENROLLED', 'AMBIGUOUS', 'TERMINAL_CONFLICT'].includes(item.classification)) return null;
    }
    if (item.error_code !== undefined && !stringValue(item.error_code, 1, 80)) return null;
    output.push({ ...item });
  }
  return output;
}

function normalizeCandidateDailyResult(operationId, source) {
  if (!source || typeof source !== 'object' || Array.isArray(source)) return null;
  if (operationId === 'getCandidateDailyTiles') return normalizeDailyTilesResult(source);
  if (operationId === 'getCandidateDailyPastShifts') return normalizePastShiftsResult(source);
  if (operationId === 'getCandidateDailyContent') {
    const appInfoValid = exactObjectKeys(source.appInfo, ['version', 'buildTs'])
      && stringValue(source.appInfo.version, 0, 80) && stringValue(source.appInfo.buildTs, 0, 80);
    if (!appInfoValid) return null;
    if (['hospital-addresses', 'accommodation-contacts'].includes(source.kind)) {
      return exactObjectKeys(source, ['kind', 'title', 'html', 'appInfo'])
        && stringValue(source.title, 1, 160) && stringValue(source.html, 0, 200000)
        ? { ...source, appInfo: { ...source.appInfo } } : null;
    }
    return exactObjectKeys(source,
      ['kind', 'title', 'html', 'message_token', 'message_kind', 'acknowledgement_mode', 'appInfo'])
      && source.kind === 'candidate-message'
      && stringValue(source.title, 1, 160) && stringValue(source.html, 1, 200000)
      && OPAQUE_TOKEN_RE.test(source.message_token)
      && ['WELCOME', 'ALERT'].includes(source.message_kind)
      && source.acknowledgement_mode === 'ALL'
      ? { ...source, appInfo: { ...source.appInfo } } : null;
  }
  if (operationId === 'getCandidateDailyEmergencyWindow') {
    if (!exactObjectKeys(source, ['eligible', 'grace_minutes_after_start', 'shifts'])
        || typeof source.eligible !== 'boolean' || source.grace_minutes_after_start !== 600
        || !Array.isArray(source.shifts) || source.shifts.length > 20) return null;
    const shifts = source.shifts.map((item) => {
      if (!exactObjectKeys(item, ['emergency_shift_token', 'date', 'starts_at', 'ends_at', 'display_label',
        'allowed_issues', 'colleague_groups', 'dna_subjects'], ['emergency_shift_token', 'date', 'starts_at', 'ends_at',
        'display_label', 'allowed_issues', 'colleague_groups']) || !OPAQUE_TOKEN_RE.test(item.emergency_shift_token)
        || !DATE_RE.test(item.date) || !dateTimeValue(item.starts_at) || !dateTimeValue(item.ends_at)
        || !stringValue(item.display_label, 1, 256) || !Array.isArray(item.allowed_issues)
        || item.allowed_issues.length < 1 || item.allowed_issues.length > 4
        || new Set(item.allowed_issues).size !== item.allowed_issues.length
        || !item.allowed_issues.every((value) => ['RUNNING_LATE', 'CANNOT_ATTEND', 'LEAVE_EARLY', 'DNA'].includes(value))) return null;
      const colleagueGroups = normalizeEmergencyContactGroups(item.colleague_groups);
      if (!colleagueGroups) return null;
      const dnaSubjects = item.dna_subjects === undefined ? undefined
        : Array.isArray(item.dna_subjects) && item.dna_subjects.length <= 100
          ? item.dna_subjects.map(normalizeEmergencyContactDisplay) : null;
      if (dnaSubjects === null || dnaSubjects?.some((entry) => entry === null)) return null;
      return { ...item, colleague_groups: colleagueGroups,
        ...(dnaSubjects !== undefined ? { dna_subjects: dnaSubjects } : {}) };
    });
    return shifts.some((item) => item === null) ? null : { ...source, shifts };
  }
  if (operationId === 'getCandidateDailyRunningLateOptions') {
    if (!exactObjectKeys(source, ['options']) || !Array.isArray(source.options)
        || source.options.length < 1 || source.options.length > 12) return null;
    const options = source.options.map((item) => exactObjectKeys(item,
      ['running_late_option_token', 'minutes', 'label', 'arrival_at'])
      && OPAQUE_TOKEN_RE.test(item.running_late_option_token) && integerValue(item.minutes, 1, 1440)
      && stringValue(item.label, 1, 160) && dateTimeValue(item.arrival_at) ? { ...item } : null);
    return options.some((item) => item === null) ? null : { options };
  }
  if (operationId === 'previewCandidateDailyRunningLate') {
    return exactObjectKeys(source, ['arrival_at', 'preview_text']) && dateTimeValue(source.arrival_at)
      && stringValue(source.preview_text, 1, 1000) ? { ...source } : null;
  }
  if (['sendCandidateDailyRunningLate', 'raiseCandidateDailyEmergency', 'markCandidateDailyMessageSeen',
    'getCandidateDailyEffectStatus'].includes(operationId)) return normalizeCandidateEffectResult(source);
  if (operationId === 'applyCandidateDailyAvailability') {
    if (!exactObjectKeys(source, ['command_id', 'availability_version', 'changed_dates'])
        || !UUID_RE.test(source.command_id) || !integerValue(source.availability_version, 1)
        || !Array.isArray(source.changed_dates) || source.changed_dates.length < 1 || source.changed_dates.length > 14
        || !source.changed_dates.every((value) => DATE_RE.test(value))) return null;
    return { ...source, changed_dates: [...source.changed_dates] };
  }
  if (operationId === 'googleAvailabilityLegacyTiles') return normalizeLegacyTilesResult(source);
  if (operationId === 'googleAvailabilityLegacyApply') {
    if (!exactObjectKeys(source, ['request_receipt_id', 'committed_version', 'outcomes'], ['request_receipt_id', 'outcomes'])
        || !UUID_RE.test(source.request_receipt_id)
        || (source.committed_version !== undefined && !integerValue(source.committed_version, 1))
        || !Array.isArray(source.outcomes) || source.outcomes.length < 1 || source.outcomes.length > 14) return null;
    const outcomes = source.outcomes.map((item) => exactObjectKeys(item, ['date', 'applied', 'reason'], ['date', 'applied'])
      && DATE_RE.test(item.date) && typeof item.applied === 'boolean'
      && (item.reason === undefined || ['INVALID_VALUE', 'DUPLICATE_DATE', 'OUTSIDE_WINDOW', 'BOOKED', 'BLOCKED', 'NOT_EDITABLE'].includes(item.reason))
      ? { ...item } : null);
    return outcomes.some((item) => item === null) ? null : { ...source, outcomes };
  }
  if (operationId === 'googleAvailabilityLegacyTimesheetAuthorisationStatus') {
    return exactObjectKeys(source, ['booking_id', 'authorised', 'status_version'])
      && stringValue(source.booking_id, 1, 128) && typeof source.authorised === 'boolean'
      && integerValue(source.status_version, 0) ? { ...source } : null;
  }
  if (['googleAvailabilityPublishRotaGenerations', 'googleAvailabilityApplySheetEdits',
    'googleAvailabilityCompleteProjection', 'googleAvailabilityApplyReconciliation'].includes(operationId)) {
    const kind = operationId === 'googleAvailabilityPublishRotaGenerations' ? 'generation'
      : operationId === 'googleAvailabilityApplySheetEdits' ? 'sheet'
        : operationId === 'googleAvailabilityCompleteProjection' ? 'projection' : 'reconciliation';
    const maximum = kind === 'generation' ? 50 : 100;
    const outcomes = normalizeIndexedOutcomes(source.outcomes, { kind, maximum });
    return exactObjectKeys(source, ['batch_receipt_id', 'outcomes']) && UUID_RE.test(source.batch_receipt_id)
      && outcomes ? { batch_receipt_id: source.batch_receipt_id, outcomes } : null;
  }
  if (operationId === 'googleAvailabilityClaimProjection') {
    if (!exactObjectKeys(source, ['claim_request_id', 'batch_receipt_id', 'lease_set_expires_at', 'items'])
        || !UUID_RE.test(source.claim_request_id) || !UUID_RE.test(source.batch_receipt_id)
        || !dateTimeValue(source.lease_set_expires_at) || !Array.isArray(source.items) || source.items.length > 100) return null;
    const items = source.items.map((item) => exactObjectKeys(item,
      ['outbox_id', 'lease_token', 'lease_expires_at', 'candidate_source_hmac', 'date', 'availability_version', 'availability'])
      && UUID_RE.test(item.outbox_id) && stringValue(item.lease_token, 16, 256)
      && dateTimeValue(item.lease_expires_at) && SOURCE_HMAC_RE.test(item.candidate_source_hmac)
      && DATE_RE.test(item.date) && integerValue(item.availability_version, 1) && LEGACY_VALUES.has(item.availability)
      ? { ...item } : null);
    return items.some((item) => item === null) ? null : { ...source, items };
  }
  if (operationId === 'googleAvailabilityReadSyncStatus') {
    if (!exactObjectKeys(source, ['items']) || !Array.isArray(source.items)
        || source.items.length < 1 || source.items.length > 100) return null;
    const items = source.items.map((item) => {
      const freshness = normalizeFreshness(item?.freshness);
      return exactObjectKeys(item, ['candidate_source_hmac', 'freshness'])
        && SOURCE_HMAC_RE.test(item.candidate_source_hmac) && freshness
        ? { candidate_source_hmac: item.candidate_source_hmac, freshness } : null;
    });
    return items.some((item) => item === null) ? null : { items };
  }
  if (operationId === 'googleAvailabilityLegacyStatus') {
    if (!exactObjectKeys(source, ['request_id', 'state', 'terminal_response'], ['request_id', 'state'])
        || !UUID_RE.test(source.request_id) || !['IN_PROGRESS', 'COMPLETED', 'FAILED_FINAL'].includes(source.state)) return null;
    if (source.terminal_response !== undefined && source.terminal_response !== null) {
      const nested = rebuildCandidateDailySuccessBody({ operationId: 'googleAvailabilityLegacyApply' }, 200,
        source.terminal_response, source.terminal_response.correlation_id);
      if (!nested) return null;
      return { ...source, terminal_response: nested };
    }
    return { ...source, ...(source.terminal_response !== undefined ? { terminal_response: null } : {}) };
  }
  if (operationId === 'googleAvailabilityEffectClaim') {
    if (!exactObjectKeys(source, ['effect_receipt_id', 'state', 'lease_token', 'lease_expires_at', 'safe_result'],
      ['effect_receipt_id', 'state']) || !UUID_RE.test(source.effect_receipt_id)
      || !['CLAIMED', 'COMPLETED', 'FAILED_FINAL', 'UNKNOWN'].includes(source.state)
      || (source.lease_token !== undefined && source.lease_token !== null && !stringValue(source.lease_token, 16, 256))
      || (source.lease_expires_at !== undefined && source.lease_expires_at !== null && !dateTimeValue(source.lease_expires_at))
      || (source.safe_result !== undefined && source.safe_result !== null
        && (!exactObjectKeys(source.safe_result, [], []) || Object.keys(source.safe_result).length))) return null;
    return { ...source, ...(source.safe_result && { safe_result: {} }) };
  }
  if (operationId === 'googleAvailabilityEffectComplete') {
    return exactObjectKeys(source, ['effect_receipt_id', 'state']) && UUID_RE.test(source.effect_receipt_id)
      && ['COMPLETED', 'FAILED_FINAL', 'UNKNOWN'].includes(source.state) ? { ...source } : null;
  }
  if (operationId === 'googleAvailabilityEffectStatus') {
    return exactObjectKeys(source, ['effect_key', 'operation', 'status', 'created_at', 'updated_at'])
      && stringValue(source.effect_key, 16, 256)
      && ['RUNNING_LATE_SEND', 'CANNOT_ATTEND', 'LEAVE_EARLY', 'DNA', 'MESSAGE_SEEN', 'ESCALATION_STEP', 'ACKNOWLEDGEMENT'].includes(source.operation)
      && ['IN_PROGRESS', 'COMPLETED', 'FAILED_FINAL', 'UNKNOWN'].includes(source.status)
      && dateTimeValue(source.created_at) && dateTimeValue(source.updated_at) ? { ...source } : null;
  }
  return null;
}

export function rebuildCandidateDailySuccessBody(routeDefinition, status, source, correlationId) {
  if (status !== 200 || !exactObjectKeys(source, ['ok', 'correlation_id', 'result'])
      || source.ok !== true || source.correlation_id !== correlationId || !isValidCorrelationId(correlationId)) return null;
  const result = normalizeCandidateDailyResult(routeDefinition?.operationId, source.result);
  return result ? { ok: true, correlation_id: correlationId, result } : null;
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
