const encoder = new TextEncoder();

const EFFECT_OPERATIONS = new Set([
  'RUNNING_LATE_SEND', 'CANNOT_ATTEND', 'LEAVE_EARLY', 'DNA', 'MESSAGE_SEEN'
]);

function exactObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function stableJson(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(',')}}`;
}

function base64Url(bytes) {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

async function sha256Hex(value) {
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(String(value))));
  return [...digest].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

async function hmac(secret, value) {
  const key = await crypto.subtle.importKey('raw', encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return base64Url(new Uint8Array(await crypto.subtle.sign('HMAC', key, encoder.encode(value))));
}

function unwrapRpc(value, name) {
  let output = value;
  if (Array.isArray(output) && output.length === 1) output = output[0];
  if (exactObject(output) && Object.hasOwn(output, name)) output = output[name];
  if (!exactObject(output)) throw new Error('DEPENDENCY_UNAVAILABLE');
  return output;
}

async function invokeRpc(rpc, name, args) {
  if (typeof rpc !== 'function') throw new Error('DEPENDENCY_UNAVAILABLE');
  return unwrapRpc(await rpc(name, args, { timeoutMs: 10_000 }), name);
}

function googleConfiguration(env) {
  const enabled = String(env?.CANDIDATE_DAILY_SPECIALIST_GOOGLE_ENABLED || '').trim().toUpperCase() === 'TRUE';
  const url = String(env?.CANDIDATE_DAILY_SPECIALIST_GOOGLE_URL || '').trim();
  const keyId = String(env?.CANDIDATE_DAILY_SPECIALIST_GOOGLE_KEY_ID || '').trim();
  const secret = String(env?.CANDIDATE_DAILY_SPECIALIST_GOOGLE_SECRET || '').trim();
  if (!enabled || !/^https:\/\//i.test(url) || keyId.length < 1 || secret.length < 32) {
    throw new Error('DEPENDENCY_UNAVAILABLE');
  }
  return { url, keyId, secret };
}

async function callGoogleSpecialist(env, operation, payload, correlationId, effectKey = null) {
  const config = googleConfiguration(env);
  const issuedAt = new Date();
  const unsigned = {
    schema_version: 'CLOUDTMS_CANDIDATE_SPECIALIST_V1',
    environment: String(env.CANDIDATE_APP_ENVIRONMENT || '').trim().toUpperCase(),
    operation,
    request_id: correlationId,
    issued_at: issuedAt.toISOString(),
    expires_at: new Date(issuedAt.getTime() + 60_000).toISOString(),
    nonce: crypto.randomUUID(),
    ...(effectKey ? { effect_key: effectKey } : {}),
    payload
  };
  const body = {
    ...unsigned,
    key_id: config.keyId,
    signature: await hmac(config.secret, stableJson(unsigned))
  };
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 12_000);
  let response;
  try {
    response = await fetch(config.url, {
      method: 'POST',
      headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
      body: JSON.stringify(body),
      signal: controller.signal
    });
  } finally {
    clearTimeout(timeout);
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > 64 * 1024) throw new Error('DEPENDENCY_UNAVAILABLE');
  let result;
  try {
    result = bytes.byteLength ? JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)) : null;
  } catch {
    throw new Error('DEPENDENCY_UNAVAILABLE');
  }
  if (!response.ok || !exactObject(result) || result.request_id !== correlationId) {
    const error = new Error(exactObject(result) && typeof result.error_code === 'string'
      ? result.error_code : 'DEPENDENCY_UNAVAILABLE');
    error.definitive = response.status >= 400 && response.status < 500;
    throw error;
  }
  if (result.ok !== true) {
    const error = new Error(typeof result.error_code === 'string' ? result.error_code : 'DEPENDENCY_UNAVAILABLE');
    error.definitive = ['VALIDATION_FAILED', 'SYSTEM_AUTH_FAILED', 'NOT_FOUND'].includes(error.message);
    throw error;
  }
  if (!exactObject(result.result)) throw new Error('DEPENDENCY_UNAVAILABLE');
  return {
    result: result.result,
    providerReference: typeof result.provider_reference === 'string' ? result.provider_reference : null
  };
}

function safeEffectMessage(operation, outcome) {
  if (outcome === 'COMPLETED') {
    return operation === 'RUNNING_LATE_SEND'
      ? 'Your running-late update has been sent through the agency service.'
      : operation === 'MESSAGE_SEEN'
        ? 'The message has been marked as seen.'
        : 'Your attendance update has been sent through the agency service.';
  }
  if (outcome === 'FAILED_FINAL') return 'The agency communication could not be sent. Please contact the agency.';
  return 'The final delivery result is not yet known. Do not submit it again; CloudTMS will check the existing request.';
}

async function executeEffect(env, rpc, request, operation) {
  if (!EFFECT_OPERATIONS.has(operation) || !request.idempotency_key) throw new Error('VALIDATION_FAILED');
  const now = new Date().toISOString();
  const claim = await invokeRpc(rpc, 'candidate_daily_effect_claim_candidate_v1', {
    p_internal_context: request.candidate_context,
    p_operation: operation,
    p_input: request.input,
    p_executor_id: 'candidate-private-google-specialist-v1',
    p_idempotency_key: request.idempotency_key,
    p_lease_seconds: 120,
    p_now_utc: now,
    p_correlation_id: request.correlation_id
  });
  if (claim.state !== 'CLAIMED') {
    if (!exactObject(claim.safe_result)) throw new Error('DEPENDENCY_UNAVAILABLE');
    return { result: claim.safe_result, idempotent_replay: true };
  }
  let outcome = 'UNKNOWN';
  let providerHash = null;
  try {
    const delivered = await callGoogleSpecialist(env, operation, claim.effect_payload,
      request.correlation_id, claim.effect_key);
    outcome = 'COMPLETED';
    providerHash = delivered.providerReference ? await sha256Hex(delivered.providerReference) : null;
  } catch (error) {
    outcome = error?.definitive === true ? 'FAILED_FINAL' : 'UNKNOWN';
  }
  const completed = await invokeRpc(rpc, 'candidate_daily_effect_complete_candidate_v1', {
    p_internal_context: request.candidate_context,
    p_effect_receipt_id: claim.effect_receipt_id,
    p_lease_token: claim.lease_token,
    p_outcome: outcome,
    p_provider_reference_hash: providerHash,
    p_safe_message: safeEffectMessage(operation, outcome),
    p_now_utc: new Date().toISOString(),
    p_correlation_id: request.correlation_id
  });
  return { result: completed, idempotent_replay: false };
}

export function createCandidateDailySpecialist(env, rpc) {
  return async function candidateDailySpecialist(request) {
    switch (request.operation_id) {
      case 'getCandidateDailyPastShifts':
        return { result: await invokeRpc(rpc, 'candidate_daily_specialist_read_v1', {
          p_internal_context: request.candidate_context, p_operation: 'PAST_SHIFTS', p_input: request.input,
          p_now_utc: new Date().toISOString(), p_correlation_id: request.correlation_id
        }) };
      case 'getCandidateDailyContent': {
        if (['hospital-addresses', 'accommodation-contacts'].includes(request.input.kind)) {
          return { result: await invokeRpc(
            rpc,
            'candidate_daily_information_candidate_v1',
            {
              p_internal_context: request.candidate_context,
              p_kind: request.input.kind,
              p_now_utc: new Date().toISOString(),
              p_correlation_id: request.correlation_id
            }
          ) };
        }
        let input = request.input;
        if (request.input.kind === 'candidate-message') {
          const context = await invokeRpc(rpc, 'candidate_daily_specialist_read_v1', {
            p_internal_context: request.candidate_context, p_operation: 'MESSAGE_CONTEXT', p_input: {},
            p_now_utc: new Date().toISOString(), p_correlation_id: request.correlation_id
          });
          if (!exactObject(context.candidate)) throw new Error('SOURCE_IDENTITY_NOT_READY');
          input = { ...request.input, candidate: context.candidate };
        }
        const google = await callGoogleSpecialist(env, 'CONTENT_READ', input, request.correlation_id);
        return { result: google.result };
      }
      case 'getCandidateDailyEmergencyWindow':
        return { result: await invokeRpc(rpc, 'candidate_daily_specialist_read_v1', {
          p_internal_context: request.candidate_context, p_operation: 'EMERGENCY_WINDOW', p_input: {},
          p_now_utc: new Date().toISOString(), p_correlation_id: request.correlation_id
        }) };
      case 'getCandidateDailyRunningLateOptions':
        return { result: await invokeRpc(rpc, 'candidate_daily_specialist_read_v1', {
          p_internal_context: request.candidate_context, p_operation: 'RUNNING_LATE_OPTIONS', p_input: request.input,
          p_now_utc: new Date().toISOString(), p_correlation_id: request.correlation_id
        }) };
      case 'previewCandidateDailyRunningLate':
        return { result: await invokeRpc(rpc, 'candidate_daily_specialist_read_v1', {
          p_internal_context: request.candidate_context, p_operation: 'RUNNING_LATE_PREVIEW', p_input: request.input,
          p_now_utc: new Date().toISOString(), p_correlation_id: request.correlation_id
        }) };
      case 'sendCandidateDailyRunningLate':
        return executeEffect(env, rpc, request, 'RUNNING_LATE_SEND');
      case 'raiseCandidateDailyEmergency':
        return executeEffect(env, rpc, request, request.input.type);
      case 'markCandidateDailyMessageSeen':
        return executeEffect(env, rpc, request, 'MESSAGE_SEEN');
      case 'getCandidateDailyEffectStatus':
        return { result: await invokeRpc(rpc, 'candidate_daily_effect_status_candidate_v1', {
          p_internal_context: request.candidate_context, p_effect_key: request.input.effect_key,
          p_now_utc: new Date().toISOString(), p_correlation_id: request.correlation_id
        }) };
      default:
        throw new Error('VALIDATION_FAILED');
    }
  };
}

export const candidateDailySpecialistInternals = Object.freeze({
  stableJson,
  safeEffectMessage
});
