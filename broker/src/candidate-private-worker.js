import {
  candidateAppBackendInternals,
  handleCandidateAppRequest,
  processPendingCandidatePaperPacks
} from './candidate-app-backend.js';
import {
  candidateFederatedIdentityHmac,
  verifyCandidateRouteContext
} from './candidate-route-context.js';
import { verifyCandidatePrivateRequest } from './candidate-service-auth.js';
import { createCandidatePrivateDependencies } from './index.js';
import { handleCandidateDailySystemPhase1bRequest } from './candidate-daily-phase1b.js';
import { purgeCandidateDailySystemNonces } from './candidate-daily-hmac-v1.js';
import {
  purgeMyTmsGoogleControlNonces,
  verifyMyTmsGoogleControlRequest
} from './mytms-google-control-hmac.js';
import { verifyMyTmsGoogleRouteContext } from './mytms-google-route-context.js';
import { candidateOperationForRequest } from '../../candidate-broker/src/candidate-operation-policy.js';
import {
  handleCandidateAppReadyPrivateProbe,
  PRIVATE_APP_READY_PROOF_PATH
} from './candidate-app-ready-private-proof.js';

const PRIVATE_CANDIDATE_PREFIX = '/private/candidate-app/v1';
const PRIVATE_MANAGER_PREFIX = '/private/candidate-manager/v1';
const PRIVATE_SYSTEM_PREFIX = '/private/candidate-system/v1';
const PRIVATE_GOOGLE_CONTROL_PREFIX = '/private/google-control/v1';
const PRIVATE_GOOGLE_DATA_PREFIX = '/private/mytms-google-data/v1';

function json(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff'
    }
  });
}

async function privateFailureDiagnostic(response) {
  if (!(response instanceof Response) || response.status < 500) return null;
  let source = {};
  try {
    const declared = Number(response.headers.get('content-length') || 0);
    if (declared > 4096) throw new Error('response too large');
    const bytes = new Uint8Array(await response.clone().arrayBuffer());
    if (bytes.byteLength > 4096) throw new Error('response too large');
    source = bytes.byteLength ? JSON.parse(new TextDecoder().decode(bytes)) : {};
  } catch {
    source = {};
  }
  const candidate = String(source?.error_code || '').trim().toUpperCase();
  return {
    status: response.status,
    error_code: /^[A-Z][A-Z0-9_]{2,100}$/.test(candidate)
      ? candidate : 'CANDIDATE_PRIVATE_FAILURE_UNCLASSIFIED'
  };
}

function requiredConfigurationAvailable(env) {
  return Boolean(
    String(env.CANDIDATE_APP_ENVIRONMENT || '').trim()
    && String(env.CANDIDATE_APP_PUBLIC_URL || '').trim()
    && String(env.SUPABASE_URL || '').trim()
    && String(env.SUPABASE_SERVICE_ROLE_KEY || '').trim()
    && String(env.CANDIDATE_PRIVATE_SERVICE_SECRET || '').trim()
    && String(env.CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET || '').trim()
    && String(env.CANDIDATE_PRIVATE_CHALLENGE_TOKEN_SECRET || '').trim()
    && String(env.CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET || '').trim()
    && env.R2 && typeof env.R2.put === 'function'
  );
}

function federatedRoutingEnabled(env) {
  return String(env.CANDIDATE_FEDERATED_ROUTING_ENABLED || '').trim().toUpperCase() === 'TRUE';
}

function federatedRouteConfigurationAvailable(env) {
  return Boolean(
    federatedRoutingEnabled(env)
    && String(env.CANDIDATE_AGENCY_ID || '').trim()
    && String(env.CANDIDATE_DATA_PLANE_ID || '').trim()
    && String(env.CANDIDATE_ROUTE_VERSION || '').trim()
    && String(env.CANDIDATE_ROUTE_CONTEXT_SECRET || '').trim()
  );
}

function federatedConfigurationAvailable(env) {
  return Boolean(
    federatedRouteConfigurationAvailable(env)
    && String(env.CANDIDATE_FEDERATED_IDENTITY_SECRET || '').trim()
  );
}

function privateCandidateOperation(request) {
  const url = new URL(request.url);
  let path = url.pathname;
  if (path.startsWith(PRIVATE_CANDIDATE_PREFIX)) {
    path = `/candidate-app/v1${path.slice(PRIVATE_CANDIDATE_PREFIX.length)}`;
  } else if (path.startsWith(PRIVATE_MANAGER_PREFIX)) {
    path = `/candidate-manager/v1${path.slice(PRIVATE_MANAGER_PREFIX.length)}`;
  } else if (path.startsWith(PRIVATE_SYSTEM_PREFIX)) {
    path = `/candidate-system/v1${path.slice(PRIVATE_SYSTEM_PREFIX.length)}`;
  }
  return candidateOperationForRequest(request.method, path);
}

async function consumeServiceNonce(request, env) {
  const nonce = String(request.headers.get('x-cloudtms-service-nonce') || '').trim().toLowerCase();
  const timestamp = String(request.headers.get('x-cloudtms-service-timestamp') || '').trim();
  if (!/^[0-9a-f-]{36}$/.test(nonce) || !/^\d{10,}$/.test(timestamp)) return false;
  const key = `candidate-private-nonces/${String(env.CANDIDATE_APP_ENVIRONMENT).toLowerCase()}/${timestamp}-${nonce}`;
  const result = await env.R2.put(key, new Uint8Array(), {
    onlyIf: { etagDoesNotMatch: '*' },
    customMetadata: { purpose: 'candidate-private-request-nonce', timestamp }
  });
  return Boolean(result);
}

async function purgeExpiredServiceNonces(env, nowSeconds = Math.floor(Date.now() / 1000)) {
  if (!env.R2?.list || !env.R2?.delete) return;
  let cursor;
  do {
    const page = await env.R2.list({ prefix: 'candidate-private-nonces/', cursor, limit: 1000 });
    const expired = (page.objects || []).filter((object) => {
      const name = String(object.key || '').split('/').pop() || '';
      const timestamp = Number(name.split('-')[0]);
      return Number.isFinite(timestamp) && timestamp < nowSeconds - 600;
    }).map((object) => object.key);
    if (expired.length) await env.R2.delete(expired);
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
}

async function removePrivatePrefix(request) {
  const url = new URL(request.url);
  if (url.pathname.startsWith(PRIVATE_CANDIDATE_PREFIX)) {
    url.pathname = `/candidate-app/v1${url.pathname.slice(PRIVATE_CANDIDATE_PREFIX.length)}`;
  } else if (url.pathname.startsWith(PRIVATE_MANAGER_PREFIX)) {
    url.pathname = `/candidate-manager/v1${url.pathname.slice(PRIVATE_MANAGER_PREFIX.length)}`;
  } else if (url.pathname.startsWith(PRIVATE_SYSTEM_PREFIX)) {
    url.pathname = `/candidate-system/v1${url.pathname.slice(PRIVATE_SYSTEM_PREFIX.length)}`;
  }
  const headers = new Headers(request.headers);
  for (const name of Array.from(headers.keys())) {
    if (name.startsWith('x-cloudtms-service-') || name.startsWith('x-cloudtms-route-')
        || name === 'origin' || name === 'cookie') headers.delete(name);
  }
  const hasBody = !['GET', 'HEAD'].includes(request.method);
  const body = hasBody ? new Uint8Array(await request.arrayBuffer()) : undefined;
  return new Request(url.toString(), {
    method: request.method,
    headers,
    body,
    redirect: 'manual'
  });
}

async function projectFederatedRequest(request, env, routeContext) {
  const now = new Date();
  const context = routeContext.context;
  const identitySecret = env.CANDIDATE_FEDERATED_IDENTITY_SECRET;
  const globalAccountHmac = await candidateFederatedIdentityHmac(
    identitySecret, context.environment, context.global_account_id
  );
  const globalSessionHmac = await candidateFederatedIdentityHmac(
    identitySecret, context.environment, context.global_session_id
  );
  const deps = createCandidatePrivateDependencies(env, 'PRIVATE');
  const link = await deps.rpc('candidate_app_federated_membership_link_set_v1', {
    p_internal_context: {
      route_context_verified: true,
      audience: 'FEDERATED_MEMBERSHIP_LINK'
    },
    p_environment: context.environment,
    p_global_account_identity_hmac: `\\x${globalAccountHmac}`,
    p_membership_id: context.membership_id,
    p_membership_generation: context.membership_generation,
    p_candidate_id: context.agency_candidate_id,
    p_candidate_code: null,
    p_target_state: 'ACTIVE',
    p_now_utc: now.toISOString()
  });
  if (!link?.ok || !['LINKED', 'UPDATED'].includes(String(link.status || ''))) {
    throw new Error('CANDIDATE_FEDERATED_MEMBERSHIP_LINK_FAILED');
  }
  const projection = await deps.rpc('candidate_app_federated_session_project_v1', {
    p_environment: context.environment,
    p_global_account_identity_hmac: `\\x${globalAccountHmac}`,
    p_global_session_identity_hmac: `\\x${globalSessionHmac}`,
    p_membership_id: context.membership_id,
    p_membership_generation: context.membership_generation,
    p_candidate_id: context.agency_candidate_id,
    p_route_version: context.route_version,
    p_session_epoch: context.session_epoch,
    p_now_utc: now.toISOString(),
    p_expires_at_utc: context.expires_at_utc
  });
  if (!projection?.ok || !projection.session_id) {
    throw new Error('CANDIDATE_FEDERATED_SESSION_PROJECTION_FAILED');
  }
  const accessToken = await candidateAppBackendInternals.createAccessToken(env, projection);
  const headers = new Headers(request.headers);
  headers.set('authorization', `Bearer ${accessToken}`);
  return new Request(request, { headers });
}

function managerRouteRequest(request, routeContext, authorityOverride = null) {
  const context = routeContext.context;
  const authorityKind = authorityOverride || context.authority_kind;
  const headers = new Headers(request.headers);
  for (const name of Array.from(headers.keys())) {
    if (name.startsWith('x-cloudtms-manager-route-')) headers.delete(name);
  }
  headers.set('x-cloudtms-manager-route-authority', authorityKind);
  if (authorityKind === 'MANAGER_EMAIL') {
    headers.set('x-cloudtms-manager-route-ticket', context.manager_route_ticket_id);
    headers.set('x-cloudtms-manager-route-revision', String(context.route_revision));
    headers.set('x-cloudtms-manager-route-workflow-hmac', context.workflow_route_hmac);
    headers.set('x-cloudtms-manager-route-request-hmac', context.approval_request_route_hmac);
    headers.set('x-cloudtms-manager-route-request-generation', String(context.request_generation));
    headers.set('x-cloudtms-manager-route-credential-generation', String(context.credential_generation));
  }
  return new Request(request, { headers });
}

async function boundedObject(request, maximumBytes = 128 * 1024) {
  const declared = Number(request.headers.get('content-length') || 0);
  if (declared > maximumBytes) throw new Error('MYTMS_GOOGLE_REQUEST_INVALID');
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > maximumBytes) throw new Error('MYTMS_GOOGLE_REQUEST_INVALID');
  const value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('MYTMS_GOOGLE_REQUEST_INVALID');
  }
  return value;
}

async function handleGoogleDataMatch(request, env, routeContext) {
  if (request.method !== 'POST'
      || new URL(request.url).pathname !== `${PRIVATE_GOOGLE_DATA_PREFIX}/candidates/match`) {
    return json(404, { ok: false, error_code: 'MYTMS_GOOGLE_DATA_ROUTE_NOT_FOUND' });
  }
  let body;
  try {
    body = await boundedObject(request.clone());
  } catch {
    return json(400, { ok: false, error_code: 'MYTMS_GOOGLE_MATCH_INVALID' });
  }
  if (String(body.operation_id || '').toLowerCase() !== routeContext.context.operation_id) {
    return json(401, { ok: false, error_code: 'MYTMS_GOOGLE_ROUTE_CONTEXT_INVALID' });
  }
  try {
    const result = await createCandidatePrivateDependencies(env, 'PRIVATE').rpc(
      'candidate_google_provisioning_match_v1',
      {
        p_internal_context: {
          route_context_verified: true,
          audience: 'GOOGLE_PROVISIONING_MATCH',
          environment: routeContext.context.environment,
          agency_id: routeContext.context.agency_id,
          data_plane_id: routeContext.context.data_plane_id,
          route_version_id: routeContext.context.route_version_id,
          target_generation: routeContext.context.target_generation,
          integration_id: routeContext.context.integration_id
        },
        p_candidate_code: body.candidate_code,
        p_surname: body.surname,
        p_email: body.email,
        p_mobile: body.mobile,
        p_google_source_identity_hmac: body.google_source_identity_hmac,
        p_source_hmac_key_version: body.source_hmac_key_version,
        p_correlation_id: body.correlation_id
      }
    );
    return json(200, result);
  } catch {
    return json(503, { ok: false, error_code: 'MYTMS_GOOGLE_DATA_DEPENDENCY_UNAVAILABLE' });
  }
}

export default {
  async fetch(request, env, ctx) {
    const path = new URL(request.url).pathname;
    const privateRoute = path.startsWith(PRIVATE_CANDIDATE_PREFIX)
      || path.startsWith(PRIVATE_MANAGER_PREFIX)
      || path.startsWith(PRIVATE_SYSTEM_PREFIX)
      || path.startsWith(PRIVATE_GOOGLE_CONTROL_PREFIX)
      || path.startsWith(PRIVATE_GOOGLE_DATA_PREFIX)
      || path === PRIVATE_APP_READY_PROOF_PATH;
    if (!privateRoute) return json(404, { ok: false, error_code: 'CANDIDATE_PRIVATE_ROUTE_NOT_FOUND' });
    if (!await verifyCandidatePrivateRequest(request, env)) {
      return json(401, { ok: false, error_code: 'CANDIDATE_PRIVATE_SERVICE_AUTH_REQUIRED' });
    }
    if (!requiredConfigurationAvailable(env)) {
      return json(503, { ok: false, error_code: 'CANDIDATE_PRIVATE_CONFIGURATION_UNAVAILABLE' });
    }
    if (!await consumeServiceNonce(request, env)) {
      return json(401, { ok: false, error_code: 'CANDIDATE_PRIVATE_SERVICE_REPLAY_REJECTED' });
    }
    if (path === PRIVATE_APP_READY_PROOF_PATH) {
      return handleCandidateAppReadyPrivateProbe(request, env);
    }
    if (path.startsWith(PRIVATE_GOOGLE_CONTROL_PREFIX)) {
      if (request.headers.has('x-cloudtms-route-context')
          || request.headers.has('x-cloudtms-google-route-context')) {
        return json(401, { ok: false, error_code: 'MYTMS_GOOGLE_CONTROL_CONTEXT_INVALID' });
      }
      const verified = await verifyMyTmsGoogleControlRequest(request, env);
      if (!verified.ok) {
        return json(verified.status, { ok: false, error_code: verified.errorCode });
      }
      return json(200, {
        ok: true,
        verified: true,
        route: verified.route,
        path: verified.path,
        body: verified.body,
        google_context: verified.googleContext,
        correlation_id: verified.correlationId,
        idempotency_key: verified.idempotencyKey,
        key_id: verified.keyId,
        request_hash: verified.requestHash
      });
    }
    if (path.startsWith(PRIVATE_GOOGLE_DATA_PREFIX)) {
      if (request.headers.has('x-cloudtms-route-context')) {
        return json(401, { ok: false, error_code: 'MYTMS_GOOGLE_ROUTE_CONTEXT_INVALID' });
      }
      const googleRouteContext = await verifyMyTmsGoogleRouteContext(request, env);
      if (!googleRouteContext) {
        return json(401, { ok: false, error_code: 'MYTMS_GOOGLE_ROUTE_CONTEXT_INVALID' });
      }
      return handleGoogleDataMatch(request, env, googleRouteContext);
    }
    const hasRouteContext = request.headers.has('x-cloudtms-route-context')
      || request.headers.has('x-cloudtms-route-context-sha256');
    if (Array.from(request.headers.keys()).some(
      name => name.startsWith('x-cloudtms-manager-route-')
    )) {
      return json(401, { ok: false, error_code: 'CANDIDATE_ROUTE_CONTEXT_INVALID' });
    }
    let routeContext = null;
    if (hasRouteContext) {
      if (!federatedRouteConfigurationAvailable(env)) {
        return json(503, { ok: false, error_code: 'CANDIDATE_FEDERATED_ROUTING_UNAVAILABLE' });
      }
      routeContext = await verifyCandidateRouteContext(request, env);
      if (!routeContext) {
        return json(401, { ok: false, error_code: 'CANDIDATE_ROUTE_CONTEXT_INVALID' });
      }
      const operation = privateCandidateOperation(request);
      if (!operation?.data_plane_dispatch_required
          || operation.operation_id !== routeContext.context.operation_id) {
        return json(401, { ok: false, error_code: 'CANDIDATE_ROUTE_CONTEXT_OPERATION_INVALID' });
      }
      if (path.startsWith(PRIVATE_SYSTEM_PREFIX)) {
        return json(401, { ok: false, error_code: 'CANDIDATE_ROUTE_CONTEXT_AUDIENCE_INVALID' });
      }
      const authorityKind = routeContext.context.authority_kind || 'CANDIDATE_SESSION';
      const managerAuthority = ['MANAGER_EMAIL', 'MANAGER_PHONE'].includes(authorityKind);
      const sharedManagerUpload = path.startsWith(`${PRIVATE_CANDIDATE_PREFIX}/uploads/`);
      const legacyPhoneAuthority = routeContext.context.v === 1
        && request.headers.has('authorization')
        && (path.startsWith(PRIVATE_MANAGER_PREFIX) || sharedManagerUpload);
      if (path.startsWith(PRIVATE_MANAGER_PREFIX)) {
        if (!(managerAuthority || legacyPhoneAuthority)) {
          return json(401, { ok: false, error_code: 'CANDIDATE_ROUTE_CONTEXT_AUDIENCE_INVALID' });
        }
        request = managerRouteRequest(
          request, routeContext, legacyPhoneAuthority ? 'MANAGER_PHONE' : null
        );
      } else if (path.startsWith(PRIVATE_CANDIDATE_PREFIX)
          && (managerAuthority || legacyPhoneAuthority)) {
        if (!sharedManagerUpload) {
          return json(401, { ok: false, error_code: 'CANDIDATE_ROUTE_CONTEXT_AUDIENCE_INVALID' });
        }
        request = managerRouteRequest(
          request, routeContext, legacyPhoneAuthority ? 'MANAGER_PHONE' : null
        );
      } else if (path.startsWith(PRIVATE_CANDIDATE_PREFIX)) {
        if (!federatedConfigurationAvailable(env)) {
          return json(503, { ok: false, error_code: 'CANDIDATE_FEDERATED_ROUTING_UNAVAILABLE' });
        }
        try {
          request = await projectFederatedRequest(request, env, routeContext);
        } catch {
          return json(401, { ok: false, error_code: 'CANDIDATE_AUTHORITY_REVOKED' });
        }
      }
    }
    if (path === `${PRIVATE_CANDIDATE_PREFIX}/health`) {
      return json(requiredConfigurationAvailable(env) ? 200 : 503, {
        ok: requiredConfigurationAvailable(env),
        service: 'cloudtms-candidate-private-api'
      });
    }
    if (!requiredConfigurationAvailable(env)) {
      return json(503, { ok: false, error_code: 'CANDIDATE_PRIVATE_CONFIGURATION_UNAVAILABLE' });
    }
    if (path.startsWith(PRIVATE_SYSTEM_PREFIX)) {
      const response = await handleCandidateDailySystemPhase1bRequest(
        await removePrivatePrefix(request), env, createCandidatePrivateDependencies(env, 'PRIVATE')
      );
      const headers = new Headers(response.headers);
      headers.set('x-cloudtms-private-api', 'candidate-daily-r8-phase1b');
      return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
    }
    const response = await handleCandidateAppRequest(
      await removePrivatePrefix(request),
      env,
      ctx,
      createCandidatePrivateDependencies(env, 'PRIVATE')
    );
    if (!response) return json(404, { ok: false, error_code: 'CANDIDATE_PRIVATE_ROUTE_NOT_FOUND' });
    const failure = await privateFailureDiagnostic(response);
    if (failure) console.error('[candidate-private] request failed', failure);
    const headers = new Headers(response.headers);
    headers.delete('access-control-allow-origin');
    headers.delete('access-control-allow-credentials');
    headers.set('x-cloudtms-private-api', 'candidate-v1');
    return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
  },
  async scheduled(controller, env, ctx) {
    if (!requiredConfigurationAvailable(env)) return;
    ctx.waitUntil(purgeExpiredServiceNonces(env));
    ctx.waitUntil(purgeCandidateDailySystemNonces(env));
    ctx.waitUntil(purgeMyTmsGoogleControlNonces(env));
    ctx.waitUntil(processPendingCandidatePaperPacks(
      env,
      createCandidatePrivateDependencies(env, 'PRIVATE'),
      10
    ));
  }
};

export const candidatePrivateWorkerInternals = Object.freeze({
  removePrivatePrefix,
  requiredConfigurationAvailable,
  consumeServiceNonce,
  purgeExpiredServiceNonces,
  purgeCandidateDailySystemNonces,
  purgeMyTmsGoogleControlNonces,
  federatedRoutingEnabled,
  federatedRouteConfigurationAvailable,
  federatedConfigurationAvailable,
  projectFederatedRequest,
  managerRouteRequest,
  handleGoogleDataMatch,
  boundedObject,
  privateFailureDiagnostic,
  privateCandidateOperation
});
