const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX = /^[0-9a-f]{64}$/;
const CLOCK = /^(?:[01][0-9]|2[0-3]):[0-5][0-9]$/;
const DAYS = Object.freeze(['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']);
const MAX_BYTES = 16_384;
const uuid = value => typeof value === 'string' && UUID.test(value);
const clock = value => typeof value === 'string' && CLOCK.test(value);
const ERRORS = Object.freeze({
  PUSH_TIMING_REQUEST_INVALID: 400,
  WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED: 403,
  PUSH_TIMING_DEPLOYMENT_UNAVAILABLE: 503,
  PUSH_TIMING_IDEMPOTENCY_CONFLICT: 409,
  PUSH_TIMING_REVISION_CONFLICT: 409,
  PUSH_TIMING_DEPENDENCY_UNAVAILABLE: 503
});

export class PushTimingOfficeError extends Error {
  constructor(code, status = ERRORS[code] || 503) { super(code); this.code = code; this.status = status; }
}
const invalid = () => { throw new PushTimingOfficeError('PUSH_TIMING_REQUEST_INVALID'); };
function exact(value, keys) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
}
export function isPushTimingRevision(value) {
  return typeof value === 'string' && /^[1-9][0-9]*$/.test(value)
    && (value.length < 19 || (value.length === 19 && value <= '9223372036854775807'));
}
export function validatePushTimingPolicy(policy) {
  if (!exact(policy, ['version', 'enabled', 'timezone', 'normal_quiet_start', 'normal_quiet_end', 'day_rules', 'bank_holiday_rule'])
    || policy.version !== 'PUSH_TIMING_POLICY_V1' || typeof policy.enabled !== 'boolean'
    || policy.timezone !== 'Europe/London' || !clock(policy.normal_quiet_start)
    || !clock(policy.normal_quiet_end) || (policy.enabled && policy.normal_quiet_start === policy.normal_quiet_end)
    || !exact(policy.day_rules, DAYS)) invalid();
  let viable = false;
  for (const day of [...DAYS, 'bank_holiday_rule']) {
    const rule = day === 'bank_holiday_rule' ? policy[day] : policy.day_rules[day];
    if (rule?.mode === 'INHERIT' || rule?.mode === 'NO_PUSH_DAY') {
      if (!exact(rule, ['mode'])) invalid();
    } else if (rule?.mode === 'ALLOW_WINDOW') {
      if (!exact(rule, ['mode', 'start', 'end']) || !clock(rule.start) || !clock(rule.end)
        || rule.start >= rule.end) invalid();
    } else invalid();
    if (day !== 'bank_holiday_rule' && rule.mode !== 'NO_PUSH_DAY') viable = true;
  }
  if (!viable) invalid();
  return policy;
}

// Keep duplicate/escaped-duplicate evidence until it has been checked. This is
// a bounded scanner, followed by the native JSON parser for the full grammar.
export function parsePushTimingJson(text) {
  const stack = [];
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (char === '{' || char === '[') {
      stack.push(char === '{' ? new Set() : null);
      if (stack.length > 12) invalid();
    } else if (char === '}' || char === ']') stack.pop();
    else if (char === '"') {
      const start = index;
      index += 1;
      for (; index < text.length; index += 1) {
        if (text[index] === '\\') index += 1;
        else if (text[index] === '"') break;
      }
      let token;
      try { token = JSON.parse(text.slice(start, index + 1)); } catch { invalid(); }
      let next = index + 1;
      while (next < text.length && /\s/.test(text[next])) next += 1;
      if (text[next] === ':') {
        const keys = stack.at(-1);
        if (!(keys instanceof Set) || keys.has(token)) invalid();
        keys.add(token);
      }
    }
  }
  try { return JSON.parse(text); } catch { invalid(); }
}
async function readRequest(req) {
  if (!req.body) invalid();
  const reader = req.body.getReader();
  const chunks = [];
  let size = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_BYTES) { await reader.cancel(); invalid(); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  let text;
  try { text = new TextDecoder('utf-8', { fatal: true }).decode(bytes); } catch { invalid(); }
  return parsePushTimingJson(text);
}
const response = (value, status = 200) => new Response(JSON.stringify(value), { status,
  headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' } });

function decodeState(value, save, requestId) {
  const keys = save ? ['ok', 'version', 'request_id', 'revision', 'policy', 'policy_sha256', 'updated_at_utc']
    : ['ok', 'version', 'revision', 'policy', 'policy_sha256', 'updated_at_utc', 'can_edit'];
  if (!exact(value, keys) || value.ok !== true
    || value.version !== (save ? 'PUSH_TIMING_SAVED_V1' : 'PUSH_TIMING_STATE_V1')
    || !isPushTimingRevision(value.revision) || typeof value.policy_sha256 !== 'string' || !HEX.test(value.policy_sha256)
    || typeof value.updated_at_utc !== 'string'
    || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/.test(value.updated_at_utc)
    || !Number.isFinite(Date.parse(value.updated_at_utc))
    || (save ? value.request_id !== requestId : value.can_edit !== true)) {
    throw new PushTimingOfficeError('PUSH_TIMING_DEPENDENCY_UNAVAILABLE');
  }
  try { validatePushTimingPolicy(value.policy); }
  catch { throw new PushTimingOfficeError('PUSH_TIMING_DEPENDENCY_UNAVAILABLE'); }
  return value;
}

export function createPushTimingOfficeHandler({ requireAdmin, rpc, deploymentContext }) {
  if (![requireAdmin, rpc, deploymentContext].every(value => typeof value === 'function')) {
    throw new Error('PUSH_TIMING_DEPENDENCY_REQUIRED');
  }
  return async function handlePushTimingOffice(env, req) {
    let functionName;
    try {
      const user = await requireAdmin(env, req);
      if (!user || !uuid(user.id)) throw new PushTimingOfficeError('WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED');
      const url = new URL(req.url);
      if (url.search || !['GET', 'PUT'].includes(req.method)) invalid();
      let deployment;
      try { deployment = deploymentContext(env); }
      catch { throw new PushTimingOfficeError('PUSH_TIMING_DEPLOYMENT_UNAVAILABLE'); }
      if (!exact(deployment, ['agency_id', 'environment']) || !uuid(deployment.agency_id)
        || !['TEST', 'LIVE'].includes(deployment.environment)) {
        throw new PushTimingOfficeError('PUSH_TIMING_DEPLOYMENT_UNAVAILABLE');
      }
      const save = req.method === 'PUT';
      let body = { version: 'PUSH_TIMING_GET_V1' };
      if (save) {
        body = await readRequest(req);
        if (!exact(body, ['version', 'request_id', 'expected_revision', 'policy'])
          || body.version !== 'PUSH_TIMING_SAVE_V1' || !uuid(body.request_id)
          || !isPushTimingRevision(body.expected_revision)) invalid();
        validatePushTimingPolicy(body.policy);
      }
      functionName = save ? 'candidate_push_timing_save_v1' : 'candidate_push_timing_get_v1';
      const result = await rpc(env, functionName, {
        p_request: { ...body, actor_user_id: user.id, ...deployment }
      }, { timeoutMs: 20_000 });
      const decoded = Array.isArray(result) && result.length === 1 ? result[0] : result;
      return response(decodeState(decoded, save, body.request_id));
    } catch (error) {
      if (error instanceof PushTimingOfficeError) return response({ ok: false, error_code: error.code }, error.status);
      // Existing sbRpc attaches the real PostgREST object to json and fn.
      // Return only a closed known refusal, never its raw detail or response.
      const closed = (!error?.fn || error.fn === functionName) && error?.json?.message;
      if (closed && Object.hasOwn(ERRORS, closed)) return response({ ok: false, error_code: closed }, ERRORS[closed]);
      if (error?.status === 401 || error?.status === 403) {
        return response({ ok: false, error_code: 'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED' }, error.status);
      }
      return response({ ok: false, error_code: 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' }, 503);
    }
  };
}
