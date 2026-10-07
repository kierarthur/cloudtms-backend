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
  let bytes;
  try {
    response = await fetch(config.url, {
      method: 'POST',
      headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
      body: JSON.stringify(body),
      signal: controller.signal
    });
    if (!response.body) throw new Error('DEPENDENCY_UNAVAILABLE');
    const reader = response.body.getReader();
    const chunks = []; let length = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        length += value.byteLength;
        if (length > 64 * 1024) { await reader.cancel(); throw new Error('DEPENDENCY_UNAVAILABLE'); }
        chunks.push(value);
      }
    } finally { reader.releaseLock(); }
    bytes = new Uint8Array(length); let offset = 0;
    for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  } catch {
    // A transport timeout is uncertain, not an internal application failure.
    throw new Error('DEPENDENCY_UNAVAILABLE');
  } finally {
    clearTimeout(timeout);
  }
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
  return 'Checking confirmation of your alert. It will not be sent again.';
}

async function effectStatus(env, rpc, request, effectKey) {
  // The database must prove ownership before Google is consulted. The original
  // transport receipt remains unchanged; a saved Google completion can resolve
  // its uncertainty without executing or claiming another effect.
  const receipt = await invokeRpc(rpc, 'candidate_daily_effect_status_candidate_v1', {
    p_internal_context: request.candidate_context, p_effect_key: effectKey,
    p_now_utc: new Date().toISOString(), p_correlation_id: request.correlation_id
  });
  if (!['UNKNOWN','IN_PROGRESS'].includes(receipt.status)) return receipt;
  if (receipt.effect_key !== effectKey || !/^[a-f0-9]{64}$/.test(effectKey) || !EFFECT_OPERATIONS.has(receipt.operation)) throw new Error('DEPENDENCY_UNAVAILABLE');
  const pending = { ...receipt, safe_message: safeEffectMessage(receipt.operation,'UNKNOWN') };
  try {
    const response = await callGoogleSpecialist(env,'EFFECT_STATUS_READ',
      { effect_key:effectKey, operation:receipt.operation },request.correlation_id,effectKey);
    const status = response.result;
    const updated = Date.parse(status.updated_at);
    const created = Date.parse(receipt.created_at);
    if (status.effect_key !== effectKey || status.operation !== receipt.operation || status.status !== 'COMPLETED'
        || !Number.isFinite(updated) || !Number.isFinite(created) || updated < created || updated > Date.now()+30_000) return pending;
    return { ...receipt, status:'COMPLETED', updated_at:new Date(updated).toISOString(),
      safe_message:safeEffectMessage(receipt.operation,'COMPLETED') };
  } catch {
    return pending; // Read failure never becomes success or a new send.
  }
}

async function executeEffect(env, rpc, request, operation) {
  if (!EFFECT_OPERATIONS.has(operation) || !request.idempotency_key) throw new Error('VALIDATION_FAILED');
  // Resolve an existing receipt before any source fetch. A timed-out/completed
  // effect must not be sent again merely because a roster has since changed.
  const replay = await specialistRead(rpc, request, 'EFFECT_REPLAY', {
    operation, input: request.input, idempotency_key: request.idempotency_key
  });
  if (replay.state !== 'ABSENT') {
    if (!exactObject(replay.safe_result)) throw new Error('DEPENDENCY_UNAVAILABLE');
    return { result: ['UNKNOWN','IN_PROGRESS'].includes(replay.safe_result.status)
      ? await effectStatus(env,rpc,request,replay.safe_result.effect_key) : replay.safe_result, idempotent_replay: true };
  }
  if (operation !== 'MESSAGE_SEEN') {
    await refreshEmergencyRoster(env, rpc, request, request.input.emergency_shift_token);
  }
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
    return { result: ['UNKNOWN','IN_PROGRESS'].includes(claim.safe_result.status)
      ? await effectStatus(env,rpc,request,claim.safe_result.effect_key) : claim.safe_result, idempotent_replay: true };
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

async function specialistRead(rpc, request, operation, input) {
  return invokeRpc(rpc, 'candidate_daily_specialist_read_v1', {
    p_internal_context: request.candidate_context, p_operation: operation, p_input: input,
    p_now_utc: new Date().toISOString(), p_correlation_id: request.correlation_id
  });
}

async function refreshEmergencyRoster(env, rpc, request, shiftToken = null) {
  const context = await specialistRead(rpc, request, 'EMERGENCY_ROSTER_CONTEXT',
    shiftToken ? { emergency_shift_token: shiftToken } : {});
  if (!Array.isArray(context.anchors) || context.anchors.length > 5
      || (shiftToken && context.anchors.length !== 1)) throw new Error('DEPENDENCY_UNAVAILABLE');
  // These are independent, read-only Google requests. Publication revalidates
  // each exact current database booking; no caller supplies contacts or scope.
  await Promise.all(context.anchors.map(async (anchor) => {
    if (!exactObject(anchor) || !exactObject(anchor.candidate) || !exactObject(anchor.shift)
        || !/^[a-f0-9]{64}$/.test(anchor.emergency_shift_token)
        || (shiftToken && anchor.emergency_shift_token !== shiftToken)) throw new Error('DEPENDENCY_UNAVAILABLE');
    const roster = await callGoogleSpecialist(env, 'EMERGENCY_ROSTER_READ', anchor, request.correlation_id);
    const published = await specialistRead(rpc, request, 'EMERGENCY_ROSTER_PUBLISH', {
      emergency_shift_token: anchor.emergency_shift_token, roster: roster.result
    });
    if (published.accepted !== true) throw new Error('DEPENDENCY_UNAVAILABLE');
  }));
}

async function emergencyRead(env, rpc, request, operation) {
  // The RPC validates the current booking/generation and the private roster's
  // five-minute lease on every read. Reuse only that database-owned authority;
  // fetching Google again for each choice/preview can exceed the public read
  // deadline. New effects still take the fresh-roster path above.
  try {
    return await specialistRead(rpc, request, operation, request.input);
  } catch (error) {
    const missingRoster = [error?.json?.message, error?.message].some(value =>
      typeof value === 'string' && /(?:^|[^A-Z0-9_])DEPENDENCY_UNAVAILABLE(?:$|[^A-Z0-9_])/.test(value));
    if (!missingRoster) throw error;
  }
  await refreshEmergencyRoster(env, rpc, request, request.input.emergency_shift_token);
  return specialistRead(rpc, request, operation, request.input);
}

export function createCandidateDailySpecialist(env, rpc, executionContext = null) {
  function retainedEffect(request, operation) {
    const work = executeEffect(env,rpc,request,operation);
    // Keep this one claimed attempt alive if the public HTTP deadline closes.
    // The caller still receives the original success/error; no retry is started.
    if (typeof executionContext?.waitUntil === 'function') {
      executionContext.waitUntil(work.then(() => undefined,() => undefined));
    }
    return work;
  }
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
        return { result: await emergencyRead(env, rpc, request, 'EMERGENCY_WINDOW') };
      case 'getCandidateDailyRunningLateOptions':
        return { result: await emergencyRead(env, rpc, request, 'RUNNING_LATE_OPTIONS') };
      case 'previewCandidateDailyRunningLate':
        return { result: await emergencyRead(env, rpc, request, 'RUNNING_LATE_PREVIEW') };
      case 'sendCandidateDailyRunningLate':
        return retainedEffect(request, 'RUNNING_LATE_SEND');
      case 'raiseCandidateDailyEmergency':
        return retainedEffect(request, request.input.type);
      case 'markCandidateDailyMessageSeen':
        return retainedEffect(request, 'MESSAGE_SEEN');
      case 'getCandidateDailyEffectStatus':
        return { result: await effectStatus(env,rpc,request,request.input.effect_key) };
      default:
        throw new Error('VALIDATION_FAILED');
    }
  };
}

export const candidateDailySpecialistInternals = Object.freeze({
  stableJson,
  safeEffectMessage
});
