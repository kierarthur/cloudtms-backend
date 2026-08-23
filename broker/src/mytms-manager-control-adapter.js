import { controlPlaneRpc } from '../../candidate-broker/src/control-plane-client.js';
import {
  signCandidatePrivateRequest,
  verifyCandidatePrivateRequest
} from './candidate-service-auth.js';

export const MYTMS_MANAGER_CONTROL_ADAPTER_PATH =
  '/private/mytms-control/v1/manager-route-rpc';

const ALLOWED_FUNCTIONS = new Set([
  'manager_email_route_register_v1',
  'manager_email_route_transition_v1',
  'manager_review_origin_resolve_v1'
]);
const MAX_BODY_BYTES = 256 * 1024;

function text(value) {
  return String(value == null ? '' : value).trim();
}

function adapterAuthEnv(env) {
  return {
    CANDIDATE_APP_ENVIRONMENT: env.CANDIDATE_APP_ENVIRONMENT,
    CANDIDATE_PRIVATE_SERVICE_SECRET: env.MYTMS_MANAGER_CONTROL_ADAPTER_SECRET
  };
}

function json(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'referrer-policy': 'no-referrer',
      'x-content-type-options': 'nosniff'
    }
  });
}

async function boundedJson(input) {
  const declared = Number(input.headers.get('content-length') || 0);
  if (declared > MAX_BODY_BYTES) throw new Error('MYTMS_MANAGER_CONTROL_REQUEST_TOO_LARGE');
  const bytes = new Uint8Array(await input.arrayBuffer());
  if (bytes.byteLength > MAX_BODY_BYTES) throw new Error('MYTMS_MANAGER_CONTROL_REQUEST_TOO_LARGE');
  if (!bytes.byteLength) return null;
  try {
    return JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw new Error('MYTMS_MANAGER_CONTROL_REQUEST_INVALID');
  }
}

function closedCall(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)
      || Object.keys(body).some((key) => !['schema', 'function_name', 'args'].includes(key))
      || text(body.schema) !== 'control'
      || !ALLOWED_FUNCTIONS.has(text(body.function_name))
      || !body.args || typeof body.args !== 'object' || Array.isArray(body.args)) {
    throw new Error('MYTMS_MANAGER_CONTROL_OPERATION_NOT_ALLOWED');
  }
  return {
    schema: 'control',
    functionName: text(body.function_name),
    args: body.args
  };
}

async function consumeAdapterNonce(request, env) {
  const nonce = text(request.headers.get('x-cloudtms-service-nonce')).toLowerCase();
  const timestamp = text(request.headers.get('x-cloudtms-service-timestamp'));
  if (!/^[0-9a-f-]{36}$/.test(nonce) || !/^\d{10,}$/.test(timestamp)
      || !env.R2 || typeof env.R2.put !== 'function') return false;
  const result = await env.R2.put(
    `mytms-manager-control-adapter-nonces/${text(env.CANDIDATE_APP_ENVIRONMENT).toLowerCase()}/${timestamp}-${nonce}`,
    new Uint8Array(),
    {
      onlyIf: { etagDoesNotMatch: '*' },
      customMetadata: { purpose: 'manager-control-adapter', timestamp }
    }
  );
  return Boolean(result);
}

export async function purgeMyTmsManagerControlAdapterNonces(
  env,
  nowSeconds = Math.floor(Date.now() / 1000)
) {
  if (!env.R2?.list || !env.R2?.delete) return;
  let cursor;
  do {
    const page = await env.R2.list({
      prefix: 'mytms-manager-control-adapter-nonces/',
      cursor,
      limit: 1000
    });
    const expired = (page.objects || []).filter((object) => {
      const name = text(object.key).split('/').pop() || '';
      const timestamp = Number(name.split('-')[0]);
      return Number.isFinite(timestamp) && timestamp < nowSeconds - 600;
    }).map((object) => object.key);
    if (expired.length) await env.R2.delete(expired);
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
}

export async function handleMyTmsManagerControlAdapter(request, env) {
  if (request.method !== 'POST'
      || new URL(request.url).pathname !== MYTMS_MANAGER_CONTROL_ADAPTER_PATH) {
    return json(404, { ok: false, error_code: 'MYTMS_MANAGER_CONTROL_ROUTE_NOT_FOUND' });
  }
  try {
    if (!await verifyCandidatePrivateRequest(request, adapterAuthEnv(env))
        || !await consumeAdapterNonce(request, env)) {
      return json(401, { ok: false, error_code: 'MYTMS_MANAGER_CONTROL_AUTHORITY_INVALID' });
    }
    const call = closedCall(await boundedJson(request));
    const result = await controlPlaneRpc(env, call.schema, call.functionName, call.args);
    return json(200, { ok: true, result });
  } catch (error) {
    const candidate = text(error?.message || error).toUpperCase();
    const errorCode = /^[A-Z][A-Z0-9_]{2,100}$/.test(candidate)
      ? candidate : 'MYTMS_MANAGER_CONTROL_UNAVAILABLE';
    const status = errorCode === 'MYTMS_MANAGER_CONTROL_OPERATION_NOT_ALLOWED' ? 403 : 503;
    return json(status, { ok: false, error_code: errorCode });
  }
}

export async function managerControlPlaneRpc(env, schema, functionName, args) {
  const binding = env.MYTMS_MANAGER_CONTROL_ADAPTER;
  if (!binding || typeof binding.fetch !== 'function') {
    return controlPlaneRpc(env, schema, functionName, args);
  }
  const call = closedCall({ schema, function_name: functionName, args });
  const unsigned = new Request(
    `https://cloudtms-manager-control.internal${MYTMS_MANAGER_CONTROL_ADAPTER_PATH}`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json; charset=utf-8' },
      body: JSON.stringify({
        schema: call.schema,
        function_name: call.functionName,
        args: call.args
      })
    }
  );
  const response = await binding.fetch(await signCandidatePrivateRequest(
    unsigned, adapterAuthEnv(env)
  ));
  const payload = await boundedJson(response);
  if (!response.ok || payload?.ok !== true) {
    const code = text(payload?.error_code).toUpperCase();
    throw new Error(/^[A-Z][A-Z0-9_]{2,100}$/.test(code)
      ? code : 'MYTMS_MANAGER_CONTROL_UNAVAILABLE');
  }
  return payload.result;
}

export const myTmsManagerControlAdapterInternals = Object.freeze({
  adapterAuthEnv,
  boundedJson,
  closedCall,
  consumeAdapterNonce
});
