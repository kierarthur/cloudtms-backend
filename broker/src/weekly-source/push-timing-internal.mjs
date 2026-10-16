import { verifyWeeklySourceDeliveryRequest } from './delivery-auth.mjs';
import { isPushTimingRevision, readPushTimingRequest } from './push-timing-office.mjs';

export const PUSH_TIMING_INTERNAL_PATH = '/internal/weekly-source/push-timing/v1';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX = /^[0-9a-f]{64}$/;
const utc = value => typeof value === 'string' && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/.test(value) && Number.isFinite(Date.parse(value));
const uuid = value => typeof value === 'string' && UUID.test(value);
const hex = value => typeof value === 'string' && HEX.test(value);
const exact = (value, keys) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const errors = Object.freeze({ PUSH_TIMING_REQUEST_INVALID: 400, PUSH_TIMING_DEPLOYMENT_UNAVAILABLE: 503,
  PUSH_TIMING_DEPENDENCY_UNAVAILABLE: 503, PUSH_TIMING_DISPATCH_SCOPE_INVALID: 403,
  CONTACT_PERMIT_INVALID: 409, CONTACT_PERMIT_EXPIRED: 409 });
const reply = (value, status = 200) => new Response(JSON.stringify(value), { status,
  headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' } });
function invalid() { throw Object.assign(new Error('PUSH_TIMING_REQUEST_INVALID'), { code: 'PUSH_TIMING_REQUEST_INVALID' }); }

export function decodePushTimingServiceRequest(body) {
  if (!exact(body, ['operation','request'])) invalid();
  const fields = body.operation === 'READ_TIMING'
    ? ['dispatch_target_id','dispatch_command_id','target_snapshot_hash','provider']
    : body.operation === 'CONSUME_START'
      ? ['permit_id','provider_attempt_id','dispatch_target_id','dispatch_command_id','target_snapshot_hash','provider','content_hash'] : null;
  if (!fields || !exact(body.request, fields)) invalid();
  for (const field of fields) {
    const value = body.request[field];
    if (field.endsWith('_id') ? !uuid(value) : field.endsWith('_hash') ? !hex(value) : !['APNS','FCM'].includes(value)) invalid();
  }
  return body;
}
export function decodePushTimingServiceReply(value, body) {
  if (body.operation === 'READ_TIMING') {
    if (!exact(value, ['ok','version','dispatch_target_id','policy_revision','policy_sha256','calendar_digest','allowed_now','reason','next_allowed_utc','expires_at_utc'])
      || value.ok !== true || value.version !== 'PUSH_TIMING_DISPATCH_STATE_V1'
      || value.dispatch_target_id !== body.request.dispatch_target_id || !isPushTimingRevision(value.policy_revision)
      || !hex(value.policy_sha256) || !hex(value.calendar_digest) || typeof value.allowed_now !== 'boolean'
      || !['ALLOWED','QUIET','EXPIRED_TIMING','OBSOLETE'].includes(value.reason)
      || value.allowed_now !== (value.reason === 'ALLOWED') || !utc(value.expires_at_utc)
      || !(value.next_allowed_utc === null || utc(value.next_allowed_utc))
      || (['ALLOWED','QUIET'].includes(value.reason) && value.next_allowed_utc === null)) {
      throw new Error('PUSH_TIMING_DEPENDENCY_UNAVAILABLE');
    }
  } else if (value?.ok === false) {
    if (!exact(value, ['ok','reason','provider_attempt_id','call_once']) || value.reason !== 'ALREADY_CONSUMED'
      || value.provider_attempt_id !== body.request.provider_attempt_id || value.call_once !== false) {
      throw new Error('PUSH_TIMING_DEPENDENCY_UNAVAILABLE');
    }
  } else if (!exact(value, ['ok','version','permit_id','provider_attempt_id','call_once','consumed_at_utc'])
    || value.ok !== true || value.version !== 'CONTACT_CONSUMED_V1' || value.permit_id !== body.request.permit_id
    || value.provider_attempt_id !== body.request.provider_attempt_id || value.call_once !== true || !utc(value.consumed_at_utc)) {
    throw new Error('PUSH_TIMING_DEPENDENCY_UNAVAILABLE');
  }
  return value;
}
export function createPushTimingInternalHandler({ rpc, deploymentContext }) {
  if (typeof rpc !== 'function' || typeof deploymentContext !== 'function') throw new Error('PUSH_TIMING_DEPENDENCY_REQUIRED');
  return async function handlePushTimingInternal(request, env) {
    if (new URL(request.url).pathname !== PUSH_TIMING_INTERNAL_PATH) return null;
    let fn;
    try {
      if (request.method !== 'POST' || new URL(request.url).search
        || !(await verifyWeeklySourceDeliveryRequest(request, env))) return reply({ ok: false, error_code: 'PUSH_TIMING_INTERNAL_AUTH_REQUIRED' }, 401);
      const body = decodePushTimingServiceRequest(await readPushTimingRequest(request));
      let deployment;
      try { deployment = deploymentContext(env); } catch { throw new Error('PUSH_TIMING_DEPLOYMENT_UNAVAILABLE'); }
      if (!exact(deployment, ['agency_id','environment']) || !uuid(deployment.agency_id)
        || !['TEST','LIVE'].includes(deployment.environment)) throw new Error('PUSH_TIMING_DEPLOYMENT_UNAVAILABLE');
      fn = body.operation === 'READ_TIMING' ? 'candidate_push_timing_for_dispatch_v1' : 'weekly_contact_submission_consume_v1';
      const value = await rpc(env, fn, { p_request: { ...body.request,
        ...(body.operation === 'READ_TIMING' ? { version: 'PUSH_TIMING_DISPATCH_READ_V1' } : {}), ...deployment } }, { timeoutMs: 10_000 });
      const native = Array.isArray(value) && value.length === 1 ? value[0] : value;
      return reply(decodePushTimingServiceReply(native, body));
    } catch (error) {
      const code = error?.code || (error?.fn === fn ? error?.json?.message : null) || error?.message;
      return reply({ ok: false, error_code: Object.hasOwn(errors, code) ? code : 'PUSH_TIMING_DEPENDENCY_UNAVAILABLE' }, errors[code] || 503);
    }
  };
}
