import {
  handleCandidateAppReadyPrivateProbe,
  PRIVATE_APP_READY_PROOF_PATH
} from '../../broker/src/candidate-app-ready-private-proof.js';
import { verifyCandidatePrivateRequest } from '../../broker/src/candidate-service-auth.js';

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

async function consumeServiceNonce(request, env) {
  const nonce = String(request.headers.get('x-cloudtms-service-nonce') || '').trim().toLowerCase();
  const timestamp = String(request.headers.get('x-cloudtms-service-timestamp') || '').trim();
  if (!/^[0-9a-f-]{36}$/.test(nonce) || !/^\d{10,}$/.test(timestamp)
      || !env.R2 || typeof env.R2.put !== 'function') return false;
  const key = `candidate-app-ready-synthetic-nonces/${timestamp}-${nonce}`;
  return Boolean(await env.R2.put(key, new Uint8Array(), {
    onlyIf: { etagDoesNotMatch: '*' },
    customMetadata: { purpose: 'candidate-app-ready-synthetic-request-nonce', timestamp }
  }));
}

export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname !== PRIVATE_APP_READY_PROOF_PATH) {
      return json(404, { ok: false, error_code: 'SYNTHETIC_DATA_PLANE_ROUTE_NOT_FOUND' });
    }
    if (!await verifyCandidatePrivateRequest(request, env)) {
      return json(401, { ok: false, error_code: 'CANDIDATE_PRIVATE_SERVICE_AUTH_REQUIRED' });
    }
    if (!await consumeServiceNonce(request, env)) {
      return json(401, { ok: false, error_code: 'CANDIDATE_PRIVATE_SERVICE_REPLAY_REJECTED' });
    }
    return handleCandidateAppReadyPrivateProbe(request, env);
  }
};

export const candidateSyntheticPrivateInternals = Object.freeze({ consumeServiceNonce });
