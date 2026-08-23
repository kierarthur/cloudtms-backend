const MAX_CONTROL_PLANE_RESPONSE_BYTES = 256 * 1024;

export class CandidateControlPlaneError extends Error {
  constructor(status, code) {
    super(code);
    this.status = status;
    this.code = code;
  }
}

function text(value) {
  return String(value == null ? '' : value).trim();
}

export function controlPlaneEnabled(env) {
  return text(env.MYTMS_CONTROL_PLANE_ENABLED).toUpperCase() === 'TRUE';
}

export function globalAuthCutoverEnabled(env) {
  return controlPlaneEnabled(env)
    && text(env.MYTMS_GLOBAL_AUTH_CUTOVER_ENABLED).toUpperCase() === 'TRUE';
}

function controlPlaneConfiguration(env) {
  const endpoint = text(env.MYTMS_CONTROL_PLANE_URL);
  const serviceRoleKey = text(env.MYTMS_CONTROL_PLANE_SERVICE_ROLE_KEY);
  let url;
  try {
    url = new URL(endpoint);
  } catch {
    throw new CandidateControlPlaneError(503, 'CONTROL_PLANE_CONFIGURATION_UNAVAILABLE');
  }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash
      || !serviceRoleKey) {
    throw new CandidateControlPlaneError(503, 'CONTROL_PLANE_CONFIGURATION_UNAVAILABLE');
  }
  url.pathname = url.pathname.replace(/\/$/, '');
  return { endpoint: url.toString().replace(/\/$/, ''), serviceRoleKey };
}

async function boundedJson(response) {
  const declared = Number(response.headers.get('content-length') || 0);
  if (declared > MAX_CONTROL_PLANE_RESPONSE_BYTES) {
    throw new CandidateControlPlaneError(502, 'CONTROL_PLANE_RESPONSE_INVALID');
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_CONTROL_PLANE_RESPONSE_BYTES) {
    throw new CandidateControlPlaneError(502, 'CONTROL_PLANE_RESPONSE_INVALID');
  }
  try {
    return bytes.byteLength ? JSON.parse(new TextDecoder().decode(bytes)) : null;
  } catch {
    throw new CandidateControlPlaneError(502, 'CONTROL_PLANE_RESPONSE_INVALID');
  }
}

function safeControlPlaneError(status, body) {
  const message = text(body?.message).toUpperCase();
  const closed = new Set([
    'GLOBAL_SESSION_INVALID', 'AGENCY_CHOICE_INVALID', 'GLOBAL_SESSION_ROTATION_INVALID',
    'GLOBAL_ACCOUNT_IDENTITY_INVALID', 'AGENCY_CONTEXT_STALE', 'AGENCY_ROUTE_UNAVAILABLE',
    'AGENCY_CONTEXT_INVALID', 'CANDIDATE_AUTHORITY_REVOKED', 'IDEMPOTENCY_CONFLICT',
    'INVITATION_INVALID', 'INVITATION_EXPIRED', 'INVITATION_CONFLICT',
    'EMAIL_VERIFICATION_REQUIRED', 'GLOBAL_LOGIN_INVALID', 'GLOBAL_REFRESH_TOKEN_REUSE',
    'MEMBERSHIP_ADMIN_DISABLED', 'MEMBERSHIP_TRANSITION_INVALID',
    'MEMBERSHIP_GENERATION_CONFLICT', 'MEMBERSHIP_NOT_FOUND',
    'GLOBAL_CHALLENGE_INVALID', 'GLOBAL_CHALLENGE_ATTEMPT_LIMIT',
    'GLOBAL_CHALLENGE_RESEND_TOO_SOON', 'GLOBAL_CHALLENGE_RESEND_LIMIT',
    'VERIFIED_CHALLENGE_INVALID', 'CURRENT_PASSWORD_INVALID', 'PASSWORD_PROOF_INVALID',
    'MANAGER_ROUTE_NOT_FOUND', 'MANAGER_ROUTE_STALE', 'MANAGER_ROUTE_NOT_CALLABLE',
    'MANAGER_ROUTE_OPERATION_FORBIDDEN', 'MANAGER_ROUTE_REVISION_CONFLICT',
    'MANAGER_SECURE_LINK_INVALID'
  ]);
  if (closed.has(message)) return message;
  if (status === 401 || status === 403) return 'CONTROL_PLANE_AUTH_FAILED';
  if (status === 409) return 'IDEMPOTENCY_CONFLICT';
  return status >= 500 ? 'DEPENDENCY_UNAVAILABLE' : 'CONTROL_PLANE_REQUEST_REJECTED';
}

export async function controlPlaneRpc(env, schema, functionName, args, options = {}) {
  if (!controlPlaneEnabled(env)) {
    throw new CandidateControlPlaneError(503, 'CONTROL_PLANE_DISABLED');
  }
  if (!/^[a-z][a-z0-9_]{0,62}$/.test(schema)
      || !/^[a-z][a-z0-9_]{0,127}$/.test(functionName)) {
    throw new CandidateControlPlaneError(500, 'CONTROL_PLANE_RPC_INVALID');
  }
  const { endpoint, serviceRoleKey } = controlPlaneConfiguration(env);
  const request = new Request(`${endpoint}/rest/v1/rpc/${functionName}`, {
    method: 'POST',
    headers: {
      apikey: serviceRoleKey,
      authorization: `Bearer ${serviceRoleKey}`,
      'content-type': 'application/json; charset=utf-8',
      'content-profile': schema,
      'accept-profile': schema,
      accept: 'application/json',
      'x-client-info': 'cloudtms-candidate-orchestrator/control-plane-v1'
    },
    body: JSON.stringify(args || {}),
    signal: AbortSignal.timeout(
      Number.isSafeInteger(options.timeoutMs) && options.timeoutMs > 0 ? options.timeoutMs : 8_000
    )
  });
  let response;
  try {
    response = await fetch(request);
  } catch {
    throw new CandidateControlPlaneError(503, 'DEPENDENCY_UNAVAILABLE');
  }
  const body = await boundedJson(response);
  if (!response.ok) {
    throw new CandidateControlPlaneError(
      response.status >= 500 ? 503 : response.status,
      safeControlPlaneError(response.status, body)
    );
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    throw new CandidateControlPlaneError(502, 'CONTROL_PLANE_RESPONSE_INVALID');
  }
  return body;
}

export const controlPlaneClientInternals = Object.freeze({
  boundedJson,
  controlPlaneConfiguration,
  safeControlPlaneError
});
