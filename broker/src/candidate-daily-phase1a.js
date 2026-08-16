import {
  candidateCorrelationId,
  composePhase1aDailyCapability,
  dailyErrorResponse,
  evaluateCandidateDailyPolicy,
  findCandidateDailyRoute,
  isCandidateDailyPath,
  phase1aCandidateDailyFacts
} from './candidate-daily-contract-v1.js';
import { verifyCandidateDailySystemRequest } from './candidate-daily-hmac-v1.js';

export function composeCandidateBootstrapPhase1a(bootstrap) {
  return composePhase1aDailyCapability(bootstrap);
}

export function candidateBootstrapCorrelation(request) {
  return candidateCorrelationId(request);
}

export async function handleCandidateDailyPhase1aRequest(request, access) {
  const url = new URL(request.url);
  if (!isCandidateDailyPath(url.pathname)) return null;
  const correlationId = candidateCorrelationId(request);
  const route = findCandidateDailyRoute(request.method, url.pathname);
  if (!route || route.signedSystem) {
    return dailyErrorResponse(400, 'VALIDATION_FAILED', 'DO_NOT_RETRY', correlationId);
  }
  if (!access || !access.session_id) {
    return dailyErrorResponse(401, 'UNAUTHENTICATED', 'REAUTHENTICATE', correlationId);
  }
  const policy = evaluateCandidateDailyPolicy(route.accessPolicy, phase1aCandidateDailyFacts());
  if (!policy.allowed) {
    return dailyErrorResponse(403, 'CANDIDATE_DAILY_DISABLED', 'REFRESH', correlationId);
  }
  return dailyErrorResponse(503, 'DEPENDENCY_UNAVAILABLE', 'RETRY_AFTER', correlationId);
}

export async function handleCandidateDailySystemPhase1aRequest(request, env, options = {}) {
  const verification = await verifyCandidateDailySystemRequest(request, env, options);
  if (!verification.ok) {
    const correlationId = verification.correlationId || String(request.headers.get('x-correlation-id') || '');
    return dailyErrorResponse(
      verification.status,
      verification.errorCode,
      verification.retryClass,
      correlationId
    );
  }
  const policy = evaluateCandidateDailyPolicy(verification.route.accessPolicy, {
    inputsReadable: true,
    systemAuthVerified: true,
    nonceConsumed: true,
    environmentTrusted: true,
    stableOperationIdentity: false,
    approvedSourceMapping: false,
    sourceScopeReady: false,
    authorityModeCompatible: false,
    transitionReady: false
  });
  return dailyErrorResponse(
    503,
    policy.allowed ? 'DEPENDENCY_UNAVAILABLE' : policy.reason,
    'RETRY_AFTER',
    verification.correlationId
  );
}

export const candidateDailyPhase1aInternals = Object.freeze({
  phase1aCandidateDailyFacts
});
