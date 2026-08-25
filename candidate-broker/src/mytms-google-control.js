import { signCandidatePrivateRequest } from '../../broker/src/candidate-service-auth.js';
import { signMyTmsGoogleRouteContext } from '../../broker/src/mytms-google-route-context.js';
import { candidateDataPlaneRegistryEntry } from './candidate-data-plane-registry.generated.js';
import { controlPlaneEnabled, controlPlaneRpc } from './control-plane-client.js';

const PREFIX = '/private/google-control/v1';
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const HEX_RE = /^[a-f0-9]{64}$/;
const MAX_BYTES = 128 * 1024;

function text(value) {
  return String(value == null ? '' : value).trim();
}

function upper(value) {
  return text(value).toUpperCase();
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map(key => (
      `${JSON.stringify(key)}:${canonicalJson(value[key])}`
    )).join(',')}}`;
  }
  return JSON.stringify(typeof value === 'undefined' ? null : value);
}

function json(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' }
  });
}

function uuid(value, code = 'MYTMS_GOOGLE_REQUEST_INVALID') {
  const output = text(value).toLowerCase();
  if (!UUID_RE.test(output)) throw new MyTmsGoogleControlError(400, code);
  return output;
}

function object(value, code = 'MYTMS_GOOGLE_REQUEST_INVALID') {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new MyTmsGoogleControlError(400, code);
  }
  return value;
}

function integer(value, minimum, code = 'MYTMS_GOOGLE_REQUEST_INVALID') {
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output < minimum) throw new MyTmsGoogleControlError(400, code);
  return output;
}

async function sha256Hex(value) {
  const bytes = value instanceof Uint8Array ? value : new TextEncoder().encode(String(value));
  return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)),
    byte => byte.toString(16).padStart(2, '0')).join('');
}

export class MyTmsGoogleControlError extends Error {
  constructor(status, code) {
    super(code);
    this.status = status;
    this.code = code;
  }
}

export function isMyTmsGoogleControlPath(path) {
  return String(path || '').startsWith(PREFIX);
}

function forwardedHeaders(request, bodyLength) {
  const headers = new Headers();
  for (const name of [
    'content-type', 'idempotency-key', 'x-correlation-id', 'x-cloudtms-key-id',
    'x-cloudtms-signature-version', 'x-cloudtms-timestamp', 'x-cloudtms-nonce',
    'x-cloudtms-content-sha256', 'x-cloudtms-signature'
  ]) {
    const value = request.headers.get(name);
    if (value != null) headers.set(name, value);
  }
  headers.set('content-length', String(bodyLength));
  headers.set('x-cloudtms-public-client', 'signed-google-control');
  return headers;
}

async function boundedBytes(request) {
  const declared = Number(request.headers.get('content-length') || 0);
  if (declared > MAX_BYTES) throw new MyTmsGoogleControlError(413, 'MYTMS_GOOGLE_REQUEST_TOO_LARGE');
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > MAX_BYTES) throw new MyTmsGoogleControlError(413, 'MYTMS_GOOGLE_REQUEST_TOO_LARGE');
  return bytes;
}

async function boundedJson(response) {
  const declared = Number(response.headers.get('content-length') || 0);
  if (declared > MAX_BYTES) throw new MyTmsGoogleControlError(502, 'MYTMS_GOOGLE_RESPONSE_INVALID');
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_BYTES) throw new MyTmsGoogleControlError(502, 'MYTMS_GOOGLE_RESPONSE_INVALID');
  try { return object(JSON.parse(new TextDecoder().decode(bytes)), 'MYTMS_GOOGLE_RESPONSE_INVALID'); } catch (error) {
    if (error instanceof MyTmsGoogleControlError) throw error;
    throw new MyTmsGoogleControlError(502, 'MYTMS_GOOGLE_RESPONSE_INVALID');
  }
}

async function verifyThroughPrivateWorker(request, env) {
  if (!env.CLOUDTMS_PRIVATE || typeof env.CLOUDTMS_PRIVATE.fetch !== 'function') {
    throw new MyTmsGoogleControlError(503, 'DEPENDENCY_UNAVAILABLE');
  }
  if (request.headers.has('origin') || request.headers.has('cookie')
      || request.headers.has('authorization') || request.headers.has('transfer-encoding')
      || request.headers.has('content-encoding')) {
    throw new MyTmsGoogleControlError(401, 'SYSTEM_AUTH_FAILED');
  }
  const body = ['GET', 'HEAD'].includes(request.method) ? new Uint8Array() : await boundedBytes(request.clone());
  const url = new URL(request.url);
  url.protocol = 'https:';
  url.hostname = 'cloudtms-candidate-private.internal';
  url.port = '';
  const unsigned = new Request(url.toString(), {
    method: request.method,
    headers: forwardedHeaders(request, body.byteLength),
    body: body.byteLength ? body : undefined,
    redirect: 'manual',
    signal: AbortSignal.timeout(8_000)
  });
  const response = await env.CLOUDTMS_PRIVATE.fetch(await signCandidatePrivateRequest(unsigned, env));
  const result = await boundedJson(response);
  if (!response.ok || result.ok !== true || result.verified !== true) {
    throw new MyTmsGoogleControlError(response.status, text(result.error_code) || 'SYSTEM_AUTH_FAILED');
  }
  return result;
}

async function targetForGoogleContext(env, googleContext) {
  const target = await controlPlaneRpc(env, 'google_control', 'integration_target_resolve_v1', {
    p_google_context: googleContext, p_now_utc: new Date().toISOString()
  });
  if (target.ok !== true || target.state !== 'ACTIVE') {
    throw new MyTmsGoogleControlError(503, text(target.error_code) || 'MYTMS_GOOGLE_TARGET_UNAVAILABLE');
  }
  const registry = candidateDataPlaneRegistryEntry(target.registry_binding_key, env);
  if (!registry || registry.environment !== upper(target.environment)) {
    throw new MyTmsGoogleControlError(503, 'MYTMS_GOOGLE_TARGET_UNAVAILABLE');
  }
  return { target, registry };
}

async function exactCandidateMatch(verified, env) {
  const body = object(verified.body);
  const operationId = uuid(body.operation_id);
  const { target, registry } = await targetForGoogleContext(env, verified.google_context);
  const now = Date.now();
  const routeContext = await signMyTmsGoogleRouteContext({
    environment: target.environment,
    integration_id: target.integration_id,
    agency_id: target.agency_id,
    data_plane_id: target.data_plane_id,
    route_version_id: target.route_version_id,
    route_version: target.route_version,
    target_generation: target.target_generation,
    operation_id: operationId,
    issued_at_utc: new Date(now).toISOString(),
    expires_at_utc: new Date(now + 4 * 60_000).toISOString(),
    key_version: registry.keyVersion
  }, registry.routeContextSecret, now);
  const matchBody = {
    operation_id: operationId,
    candidate_code: text(body.candidate_code),
    surname: text(body.surname), email: text(body.email), mobile: text(body.mobile),
    google_source_identity_hmac: text(body.google_source_identity_hmac).toLowerCase(),
    source_hmac_key_version: integer(body.source_hmac_key_version, 1),
    correlation_id: verified.correlation_id
  };
  const raw = JSON.stringify(matchBody);
  const headers = new Headers({
    'content-type': 'application/json; charset=utf-8',
    'content-length': String(new TextEncoder().encode(raw).byteLength),
    'x-cloudtms-public-client': 'mytms-google-orchestrator',
    'x-cloudtms-google-route-context': routeContext.envelope,
    'x-cloudtms-google-route-context-sha256': routeContext.sha256
  });
  const unsigned = new Request(
    'https://cloudtms-candidate-private.internal/private/mytms-google-data/v1/candidates/match',
    { method: 'POST', headers, body: raw, redirect: 'manual', signal: AbortSignal.timeout(8_000) }
  );
  const response = await registry.binding.fetch(await signCandidatePrivateRequest(unsigned, env));
  const match = await boundedJson(response);
  if (!response.ok || match.ok !== true || !['EXACT', 'NO_MATCH', 'AMBIGUOUS'].includes(match.match_state)) {
    throw new MyTmsGoogleControlError(503, text(match.error_code) || 'MYTMS_GOOGLE_MATCH_UNAVAILABLE');
  }
  return { match, target, registry, operationId };
}

async function attachCandidateCode(verified, env, commitContext, rowFacts) {
  const operationId = uuid(verified.body.operation_id);
  const { match, target, registry } = await exactCandidateMatch({
    ...verified,
    body: {
      operation_id: operationId,
      candidate_code: text(commitContext.candidate_code),
      surname: text(rowFacts.surname),
      email: text(rowFacts.email),
      mobile: text(rowFacts.mobile),
      google_source_identity_hmac: text(rowFacts.google_source_identity_hmac),
      source_hmac_key_version: rowFacts.source_hmac_key_version
    }
  }, env);
  if (match.match_state !== 'EXACT'
      || text(match.local_candidate_id).toLowerCase() !== text(commitContext.local_candidate_id).toLowerCase()
      || text(target.agency_id).toLowerCase() !== text(commitContext.agency_id).toLowerCase()) {
    throw new MyTmsGoogleControlError(409, 'MYTMS_GOOGLE_IDENTITY_CHANGED');
  }
  const now = Date.now();
  const routeContext = await signMyTmsGoogleRouteContext({
    environment: target.environment,
    integration_id: target.integration_id,
    agency_id: target.agency_id,
    data_plane_id: target.data_plane_id,
    route_version_id: target.route_version_id,
    route_version: target.route_version,
    target_generation: target.target_generation,
    operation_id: operationId,
    issued_at_utc: new Date(now).toISOString(),
    expires_at_utc: new Date(now + 4 * 60_000).toISOString(),
    key_version: registry.keyVersion
  }, registry.routeContextSecret, now);
  const attachBody = {
    operation_id: operationId,
    local_candidate_id: text(commitContext.local_candidate_id),
    candidate_code: text(commitContext.candidate_code),
    surname: text(rowFacts.surname), email: text(rowFacts.email), mobile: text(rowFacts.mobile),
    google_source_identity_hmac: text(rowFacts.google_source_identity_hmac).toLowerCase(),
    source_hmac_key_version: integer(rowFacts.source_hmac_key_version, 1),
    correlation_id: verified.correlation_id
  };
  const raw = JSON.stringify(attachBody);
  const headers = new Headers({
    'content-type': 'application/json; charset=utf-8',
    'content-length': String(new TextEncoder().encode(raw).byteLength),
    'x-cloudtms-public-client': 'mytms-google-orchestrator',
    'x-cloudtms-google-route-context': routeContext.envelope,
    'x-cloudtms-google-route-context-sha256': routeContext.sha256
  });
  const unsigned = new Request(
    'https://cloudtms-candidate-private.internal/private/mytms-google-data/v1/candidates/attach',
    { method: 'POST', headers, body: raw, redirect: 'manual', signal: AbortSignal.timeout(8_000) }
  );
  const response = await registry.binding.fetch(await signCandidatePrivateRequest(unsigned, env));
  const result = await boundedJson(response);
  if (!response.ok || result.ok !== true || !['ATTACHED', 'UNCHANGED'].includes(text(result.state))) {
    throw new MyTmsGoogleControlError(response.status, text(result.error_code) || 'MYTMS_GOOGLE_LINK_UNAVAILABLE');
  }
  return result;
}

function requestHash(body) {
  const value = text(body.request_hash).toLowerCase();
  if (!HEX_RE.test(value)) throw new MyTmsGoogleControlError(400, 'MYTMS_GOOGLE_REQUEST_HASH_INVALID');
  return value;
}

async function provisioningPreflight(verified, env) {
  const body = object(verified.body);
  if (!HEX_RE.test(text(body.google_source_identity_hmac).toLowerCase())) {
    throw new MyTmsGoogleControlError(400, 'MYTMS_GOOGLE_SOURCE_IDENTITY_INVALID');
  }
  const { match, target, operationId } = await exactCandidateMatch(verified, env);
  const googleContext = { ...verified.google_context, reservation_token: text(body.reservation_token) };
  const result = await controlPlaneRpc(env, 'google_control', 'provisioning_preflight_v1', {
    p_google_context: googleContext,
    p_operation_id: operationId,
    p_idempotency_key: verified.idempotency_key,
    p_request_hash: requestHash(body),
    p_agency_id: target.agency_id,
    p_candidate_match_facts: match,
    p_candidate_code: text(body.candidate_code),
    p_correlation_id: verified.correlation_id,
    p_now_utc: new Date().toISOString()
  });
  return result;
}

async function provisioningCommit(verified, env) {
  const body = object(verified.body);
  const operationId = uuid(body.operation_id);
  const now = new Date().toISOString();
  const commitContext = await controlPlaneRpc(
    env, 'google_control', 'provisioning_commit_context_get_v1',
    { p_google_context: verified.google_context, p_operation_id: operationId, p_now_utc: now }
  );
  if (commitContext.ok !== true) return commitContext;
  const rowFacts = object(body.google_row_facts);
  if (text(rowFacts.candidate_code) !== text(commitContext.candidate_code)) {
    throw new MyTmsGoogleControlError(409, 'MYTMS_GOOGLE_LINK_CONFLICT');
  }
  await attachCandidateCode(verified, env, commitContext, rowFacts);
  return controlPlaneRpc(env, 'google_control', 'provisioning_commit_v1', {
    p_google_context: verified.google_context,
    p_operation_id: operationId,
    p_reservation_token: text(body.reservation_token),
    p_request_hash: requestHash(body),
    p_google_row_facts: rowFacts,
    p_agency_link_facts: {
      local_candidate_id: commitContext.local_candidate_id,
      candidate_code: commitContext.candidate_code
    },
    p_correlation_id: verified.correlation_id,
    p_now_utc: now
  });
}

function pathUuid(path, suffix = '') {
  const escaped = suffix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = new RegExp(`^${PREFIX}/(?:candidates/provisioning|target-switches)/([0-9a-f-]{36})${escaped}$`, 'i').exec(path);
  return match ? uuid(match[1]) : (() => { throw new MyTmsGoogleControlError(400, 'MYTMS_GOOGLE_REQUEST_INVALID'); })();
}

async function operatorContext(verified, env, body = {}) {
  if (upper(env.MYTMS_GOOGLE_SWITCH_COORDINATOR_ENABLED) !== 'TRUE') {
    throw new MyTmsGoogleControlError(503, 'MYTMS_GOOGLE_SWITCH_DISABLED');
  }
  return {
    actor_role: 'CONTROL_PLANE_OPERATOR', authorized: true, authorization_version: 1,
    authorized_capabilities: ['GOOGLE_TARGET_SWITCH'],
    actor_identity_hmac: await sha256Hex(`mytms-google-switch-operator-v1:${verified.key_id}`),
    switch_nonce_hash_hex: text(body.switch_nonce_hash_hex).toLowerCase(),
    expires_at_utc: text(body.expires_at_utc)
  };
}

async function targetSwitchPrepare(verified, env) {
  const body = object(verified.body);
  const current = await targetForGoogleContext(env, verified.google_context);
  const operator = await operatorContext(verified, env, body);
  const destination = await controlPlaneRpc(env, 'google_control', 'target_switch_destination_resolve_v1', {
    p_operator_context: operator, p_agency_key: text(body.target_agency_key).toLowerCase()
  });
  if (destination.ok !== true) return destination;
  return controlPlaneRpc(env, 'google_control', 'target_switch_prepare_v1', {
    p_operator_context: operator,
    p_from_agency_id: current.target.agency_id,
    p_to_agency_id: destination.agency_id,
    p_expected_from_route_version: current.target.route_version,
    p_expected_to_route_version: destination.route_version,
    p_drain_snapshot_hash: text(body.drain_snapshot_hash).toLowerCase(),
    p_idempotency_key: verified.idempotency_key,
    p_correlation_id: verified.correlation_id,
    p_now_utc: new Date().toISOString()
  });
}

async function targetSwitchAttest(verified, env) {
  const body = object(verified.body);
  const facts = object(body.source_principal_and_drain_facts);
  const factsHash = await sha256Hex(canonicalJson(facts));
  if (text(body.facts_hash).toLowerCase() !== factsHash) {
    throw new MyTmsGoogleControlError(400, 'MYTMS_GOOGLE_SWITCH_FACTS_HASH_INVALID');
  }
  return controlPlaneRpc(env, 'google_control', 'target_switch_attest_v1', {
    p_google_context: verified.google_context,
    p_switch_id: pathUuid(verified.path, '/attest'),
    p_project_role: upper(body.project_role), p_nonce: text(body.nonce),
    p_source_principal_and_drain_facts: facts,
    p_facts_hash: factsHash,
    p_correlation_id: verified.correlation_id, p_now_utc: new Date().toISOString()
  });
}

async function targetSwitchTransition(verified, env) {
  const body = object(verified.body);
  const action = verified.path.endsWith('/COMMIT') ? 'COMMIT' : 'ABORT';
  return controlPlaneRpc(env, 'google_control', 'target_switch_transition_v1', {
    p_operator_context: await operatorContext(verified, env, body),
    p_switch_id: pathUuid(verified.path, `/${action}`), p_action: action,
    p_expected_generation: integer(body.expected_generation, 1),
    p_reason: text(body.reason), p_idempotency_key: verified.idempotency_key,
    p_correlation_id: verified.correlation_id, p_now_utc: new Date().toISOString()
  });
}

async function dispatch(verified, env) {
  switch (verified.route) {
    case 'PROVISIONING_PREFLIGHT': return provisioningPreflight(verified, env);
    case 'PROVISIONING_COMMIT': return provisioningCommit(verified, env);
    case 'PROVISIONING_STATUS':
      return controlPlaneRpc(env, 'google_control', 'provisioning_status_get_v1', {
        p_google_context: verified.google_context,
        p_operation_id: pathUuid(verified.path), p_correlation_id: verified.correlation_id,
        p_now_utc: new Date().toISOString()
      });
    case 'TARGET_SWITCH_PREPARE': return targetSwitchPrepare(verified, env);
    case 'TARGET_SWITCH_ATTEST': return targetSwitchAttest(verified, env);
    case 'TARGET_SWITCH_TRANSITION': return targetSwitchTransition(verified, env);
    case 'TARGET_SWITCH_STATUS':
      return controlPlaneRpc(env, 'google_control', 'target_switch_status_get_v1', {
        p_google_context: verified.google_context,
        p_switch_id: pathUuid(verified.path), p_now_utc: new Date().toISOString()
      });
    default: throw new MyTmsGoogleControlError(404, 'MYTMS_GOOGLE_ROUTE_NOT_FOUND');
  }
}

function safeResult(result, correlationId) {
  const source = object(result, 'MYTMS_GOOGLE_RESPONSE_INVALID');
  const safe = { ok: source.ok === true, correlation_id: correlationId };
  for (const key of [
    'state', 'status', 'error_code', 'operation_id', 'candidate_code', 'reservation_token',
    'idempotent_replay', 'switch_id', 'generation', 'expires_at_utc',
    'attestation_count', 'current_project_attested'
  ]) if (source[key] !== undefined) safe[key] = source[key];
  return safe;
}

export async function handleMyTmsGoogleControlRequest(request, env) {
  const correlationId = text(request.headers.get('x-correlation-id'));
  try {
    if (!controlPlaneEnabled(env)) throw new MyTmsGoogleControlError(503, 'CONTROL_PLANE_DISABLED');
    const verified = await verifyThroughPrivateWorker(request, env);
    const result = await dispatch(verified, env);
    const safe = safeResult(result, verified.correlation_id);
    return json(safe.ok ? 200 : safe.state === 'CONFLICT' ? 409 : 400, safe);
  } catch (error) {
    const status = Number(error?.status);
    return json(Number.isSafeInteger(status) ? status : 503, {
      ok: false,
      error_code: text(error?.code || error?.message) || 'DEPENDENCY_UNAVAILABLE',
      correlation_id: correlationId || null
    });
  }
}

export const myTmsGoogleControlInternals = Object.freeze({
  attachCandidateCode, boundedBytes, boundedJson, canonicalJson, dispatch, exactCandidateMatch, forwardedHeaders,
  operatorContext, pathUuid, requestHash, safeResult, targetForGoogleContext
});
