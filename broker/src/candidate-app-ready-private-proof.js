import { candidateOperationById } from '../../candidate-broker/src/candidate-operation-policy.js';
import { verifyCandidateRouteContext } from './candidate-route-context.js';

export const PRIVATE_APP_READY_PROOF_PATH = '/private/app-ready/v1/route-probe';
const SHA256_RE = /^[0-9a-f]{64}$/;

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

function proofConfiguration(env) {
  const enabled = String(env.CANDIDATE_APP_READY_PROOF_ENABLED || '').trim().toUpperCase() === 'TRUE';
  const environment = String(env.CANDIDATE_APP_ENVIRONMENT || '').trim().toUpperCase();
  const proofClass = String(env.CANDIDATE_APP_READY_PROOF_CLASS || '').trim();
  const marker = String(env.CANDIDATE_APP_READY_RUNTIME_MARKER_SHA256 || '').trim().toLowerCase();
  if (!enabled || environment !== 'TEST'
      || !['REAL_TEST_DATA_PLANE', 'SYNTHETIC_NON_BUSINESS_FIXTURE'].includes(proofClass)
      || !SHA256_RE.test(marker)) return null;
  return { proofClass, marker };
}

async function boundedBody(request) {
  const declared = Number(request.headers.get('content-length') || 0);
  if (Number.isFinite(declared) && declared > 128 * 1024) throw new Error('BODY_TOO_LARGE');
  const bytes = new Uint8Array(await request.arrayBuffer());
  if (bytes.byteLength > 128 * 1024) throw new Error('BODY_TOO_LARGE');
  const body = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  if (!body || typeof body !== 'object' || Array.isArray(body)) throw new Error('BODY_INVALID');
  return body;
}

export async function handleCandidateAppReadyPrivateProbe(request, env) {
  if (request.method !== 'POST' || new URL(request.url).pathname !== PRIVATE_APP_READY_PROOF_PATH) {
    return json(404, { ok: false, error_code: 'APP_READY_PROOF_ROUTE_NOT_FOUND' });
  }
  const configuration = proofConfiguration(env);
  if (!configuration) {
    return json(404, { ok: false, error_code: 'APP_READY_PROOF_DISABLED' });
  }
  const routeContext = await verifyCandidateRouteContext(request, env);
  if (!routeContext) {
    return json(401, { ok: false, error_code: 'APP_READY_ROUTE_CONTEXT_REJECTED' });
  }
  let body;
  try {
    body = await boundedBody(request.clone());
  } catch {
    return json(400, { ok: false, error_code: 'APP_READY_PROBE_BODY_INVALID' });
  }
  const operation = candidateOperationById(body.operation_id);
  if (!operation?.data_plane_dispatch_required
      || operation.method !== body.method || operation.path !== body.path
      || operation.operation_id !== routeContext.context.operation_id) {
    return json(401, { ok: false, error_code: 'APP_READY_OPERATION_CONTEXT_REJECTED' });
  }
  if (body.fault_mode === 'UNAVAILABLE') {
    return json(503, {
      ok: false,
      error_code: 'APP_READY_PROBE_SIMULATED_UNAVAILABLE',
      proof_class: configuration.proofClass,
      runtime_marker_sha256: configuration.marker
    });
  }
  if (!Array.isArray(body.cases) || body.cases.length < 1 || body.cases.length > 16) {
    return json(400, { ok: false, error_code: 'APP_READY_PROBE_CASES_INVALID' });
  }
  const results = [];
  for (const testCase of body.cases) {
    const caseId = String(testCase?.case_id || '').trim();
    if (!/^[a-z][a-z0-9_]{0,79}$/.test(caseId)) {
      return json(400, { ok: false, error_code: 'APP_READY_PROBE_CASE_INVALID' });
    }
    const headers = new Headers();
    if (typeof testCase.envelope === 'string') {
      headers.set('x-cloudtms-route-context', testCase.envelope);
    }
    if (typeof testCase.sha256 === 'string') {
      headers.set('x-cloudtms-route-context-sha256', testCase.sha256);
    }
    const verified = await verifyCandidateRouteContext(
      new Request('https://cloudtms-candidate-private.internal/app-ready-inner', { headers }), env
    );
    const accepted = Boolean(verified && verified.context.operation_id === operation.operation_id);
    results.push({ case_id: caseId, accepted });
  }
  return json(200, {
    ok: true,
    status: 'ROUTE_PROBE_ACCEPTED',
    operation_id: operation.operation_id,
    proof_class: configuration.proofClass,
    runtime_marker_sha256: configuration.marker,
    route_context_sha256: routeContext.sha256,
    results
  });
}

export const candidateAppReadyPrivateProofInternals = Object.freeze({
  boundedBody,
  proofConfiguration
});
