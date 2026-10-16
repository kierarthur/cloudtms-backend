import { parsePushTimingJson } from './push-timing-office.mjs';

export const CONTACT_PAUSE_RPC_COMMANDS = Object.freeze({
  CONTACT_PAUSE_GET: 'weekly_source_contact_pause_get_v1',
  CONTACT_PAUSE: 'weekly_source_contact_pause_atomic_v1',
  CONTACT_RESUME: 'weekly_source_contact_resume_atomic_v1',
});
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/;
const codes = Object.freeze({ CONTACT_REQUEST_INVALID:400, CONTACT_SCOPE_NOT_FOUND:403,
  WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED:403, CONTACT_DEPLOYMENT_UNAVAILABLE:503,
  CONTACT_IDEMPOTENCY_CONFLICT:409, CONTACT_REVISION_CONFLICT:409, CONTACT_REVISION_EXHAUSTED:409,
  CONTACT_DEPENDENCY_UNAVAILABLE:503 });
const uuid = v => typeof v === 'string' && UUID.test(v);
const instant = v => typeof v === 'string' && UTC.test(v) && Number.isFinite(Date.parse(v));
const revision = v => typeof v === 'string' && /^(0|[1-9][0-9]*)$/.test(v)
  && (v.length < 19 || (v.length === 19 && v <= '9223372036854775807'));
const exact = (v,keys) => v !== null && typeof v === 'object' && !Array.isArray(v)
  && Object.keys(v).length === keys.length && keys.every(k => Object.hasOwn(v,k));
class ContactOfficeError extends Error { constructor(code) { super(code);this.code=code; } }
const invalid = () => { throw new ContactOfficeError('CONTACT_REQUEST_INVALID'); };
const unavailable = () => { throw new ContactOfficeError('CONTACT_DEPENDENCY_UNAVAILABLE'); };

export function isContactPauseCommand(action) { return typeof action==='string' && Object.hasOwn(CONTACT_PAUSE_RPC_COMMANDS, action); }
export function parseContactPauseCommandBody(raw) {
  try {
    if (new TextEncoder().encode(raw).byteLength > 16_384) invalid();
    return parsePushTimingJson(raw);
  } catch { invalid(); }
}
export function contactPauseRequest(body, actor, deployment) {
  if (!exact(body,['action','payload']) || !isContactPauseCommand(body.action)) invalid();
  const p=body.payload;
  const keys=body.action==='CONTACT_PAUSE_GET' ? ['candidate_id','client_id']
    : ['candidate_id','client_id','request_id','expected_revision','audience'];
  const accepted=Object.hasOwn(p || {},'note') && body.action!=='CONTACT_PAUSE_GET' ? [...keys,'note'] : keys;
  if (!exact(p,accepted) || !uuid(p.candidate_id) || !uuid(p.client_id)) invalid();
  if (body.action!=='CONTACT_PAUSE_GET' && (!uuid(p.request_id) || !revision(p.expected_revision)
    || !['WORKER','MANAGER','BOTH'].includes(p.audience)
    || (Object.hasOwn(p,'note') && p.note !== null
      && (typeof p.note!=='string' || [...p.note].length>500)))) invalid();
  if (!actor || !uuid(actor.id) || actor.is_active!==true || actor.role!=='admin') {
    throw new ContactOfficeError('WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED');
  }
  if (!exact(deployment,['agency_id','environment']) || !uuid(deployment.agency_id)
    || !['TEST','LIVE'].includes(deployment.environment)) throw new ContactOfficeError('CONTACT_DEPLOYMENT_UNAVAILABLE');
  return {...p,actor_user_id:actor.id,...deployment};
}
function audience(value) {
  if (!exact(value,['paused','paused_until_utc','paused_until_local','reason'])
    || typeof value.paused!=='boolean'
    || (value.reason!==null && (typeof value.reason!=='string' || [...value.reason].length>500))) unavailable();
  if (value.paused ? (!instant(value.paused_until_utc) || typeof value.paused_until_local!=='string'
    || !/^\d{4}-\d{2}-\d{2} \d{2}:\d{2} Europe\/London$/.test(value.paused_until_local))
    : (value.paused_until_utc!==null || value.paused_until_local!==null)) unavailable();
}
function scope(value,request,mutation) {
  const keys=['scope_id','candidate_id','client_id','revision','worker','manager','can_pause','can_resume','in_flight'];
  if (!exact(value,keys) || (value.scope_id!==null && !uuid(value.scope_id))
    || (mutation && !uuid(value.scope_id)) || value.candidate_id!==request.candidate_id || value.client_id!==request.client_id
    || !revision(value.revision) || typeof value.can_pause!=='boolean' || typeof value.can_resume!=='boolean'
    || !Array.isArray(value.in_flight) || value.in_flight.length>500) unavailable();
  audience(value.worker);audience(value.manager);
  for (const item of value.in_flight) {
    if (!exact(item,['permit_id','owner_kind','delivery_id','provider_attempt_id','channel','status','started_at_utc','memberships'])
      || !uuid(item.permit_id) || !uuid(item.delivery_id) || (item.provider_attempt_id!==null && !uuid(item.provider_attempt_id))
      || !['WEEKLY_SOURCE_TARGET','TIMESHEET_QUERY_OUTBOX'].includes(item.owner_kind)
      || !['PUSH','EMAIL'].includes(item.channel) || !['STARTED','AMBIGUOUS'].includes(item.status)
      || !instant(item.started_at_utc) || !Array.isArray(item.memberships) || !item.memberships.length
      || item.memberships.length>500) unavailable();
    for (const m of item.memberships) if (!exact(m,['scope_id','candidate_id','client_id','audience','revision'])
      || !uuid(m.scope_id) || m.candidate_id!==request.candidate_id || m.client_id!==request.client_id
      || !['WORKER','MANAGER'].includes(m.audience) || !revision(m.revision)) unavailable();
  }
}
export function decodeContactPauseReply(value,action,request) {
  const get=action==='CONTACT_PAUSE_GET';
  if (!exact(value,get ? ['ok','version','scope']
    : ['ok','version','request_id','operation','audience','changed','recheck_task_id','automatic_send','scope'])
    || value.ok!==true || value.version!==(get?'CONTACT_PAUSE_STATE_V1':'CONTACT_PAUSE_RECEIPT_V1')) unavailable();
  if (!get && (value.request_id!==request.request_id || value.operation!==(action==='CONTACT_PAUSE'?'PAUSE':'RESUME')
    || value.audience!==request.audience || typeof value.changed!=='boolean' || value.automatic_send!==false
    || ((value.changed || action==='CONTACT_RESUME') ? !uuid(value.recheck_task_id) : value.recheck_task_id!==null))) unavailable();
  scope(value.scope,request,!get);
  // Do not compare a replay with current wall-clock/revision: it is the exact
  // immutable acknowledgement, which may truthfully predate a later command.
  return value;
}
export async function handleContactPauseOfficeCommand({body,user,deploymentContext,rpc,search=''}) {
  let rpcName;
  try {
    if (search) invalid();
    let deployment;
    try { deployment=deploymentContext(); } catch { throw new ContactOfficeError('CONTACT_DEPLOYMENT_UNAVAILABLE'); }
    const request=contactPauseRequest(body,user,deployment);
    rpcName=CONTACT_PAUSE_RPC_COMMANDS[body.action];
    const value=await rpc(rpcName,request);
    return Response.json(decodeContactPauseReply(value,body.action,request),{headers:{'cache-control':'no-store'}});
  } catch(error) {
    const code=error instanceof ContactOfficeError ? error.code
      : error?.fn===rpcName && Object.hasOwn(codes,error?.json?.message) ? error.json.message : 'CONTACT_DEPENDENCY_UNAVAILABLE';
    return Response.json({ok:false,error_code:code},{status:codes[code]||503,headers:{'cache-control':'no-store'}});
  }
}
