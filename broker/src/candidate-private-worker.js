import { handleCandidateAppRequest, processPendingCandidatePaperPacks } from './candidate-app-backend.js';
import { verifyCandidatePrivateRequest } from './candidate-service-auth.js';
import { createCandidatePrivateDependencies } from './index.js';
import { handleCandidateDailySystemPhase1aRequest } from './candidate-daily-phase1a.js';
import { purgeCandidateDailySystemNonces } from './candidate-daily-hmac-v1.js';

const PRIVATE_CANDIDATE_PREFIX = '/private/candidate-app/v1';
const PRIVATE_MANAGER_PREFIX = '/private/candidate-manager/v1';
const PRIVATE_SYSTEM_PREFIX = '/private/candidate-system/v1';

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
    if (name.startsWith('x-cloudtms-service-') || name === 'origin' || name === 'cookie') headers.delete(name);
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

export default {
  async fetch(request, env, ctx) {
    const path = new URL(request.url).pathname;
    const privateRoute = path.startsWith(PRIVATE_CANDIDATE_PREFIX)
      || path.startsWith(PRIVATE_MANAGER_PREFIX)
      || path.startsWith(PRIVATE_SYSTEM_PREFIX);
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
      const response = await handleCandidateDailySystemPhase1aRequest(
        await removePrivatePrefix(request), env
      );
      const headers = new Headers(response.headers);
      headers.set('x-cloudtms-private-api', 'candidate-daily-r5-phase1a');
      return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
    }
    const response = await handleCandidateAppRequest(
      await removePrivatePrefix(request),
      env,
      ctx,
      createCandidatePrivateDependencies(env, 'PRIVATE')
    );
    if (!response) return json(404, { ok: false, error_code: 'CANDIDATE_PRIVATE_ROUTE_NOT_FOUND' });
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
  purgeCandidateDailySystemNonces
});
