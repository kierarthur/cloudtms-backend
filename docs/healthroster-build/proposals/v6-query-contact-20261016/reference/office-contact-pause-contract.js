// Generated from Health contact-pause-office.mjs SHA256 47d3e4db19d29addfedaefd63c36c8b13a43abf4fe4d5a260531b2a1a51ceca9.
(function(root){'use strict';
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


function audience(value) {
  if (!exact(value,['paused','paused_until_utc','paused_until_local','reason'])
    || typeof value.paused!=='boolean'
    || (value.reason!==null && (typeof value.reason!=='string' || [...value.reason].length>500))) unavailable();
  if (value.paused ? (!instant(value.paused_until_utc) || typeof value.paused_until_local!=='string'
    || !/^\d{4}-\d{2}-\d{2} \d{2}:\d{2} Europe\/London$/.test(value.paused_until_local))
    : (value.paused_until_utc!==null || value.paused_until_local!==null)) unavailable();
}
function scope(value,request,mutation) {
  const keys=['scope_id','candidate_id','client_id','revision','next_pause_until_utc','next_pause_until_local','worker','manager','can_pause','can_resume','in_flight'];
  if (!exact(value,keys) || (value.scope_id!==null && !uuid(value.scope_id))
    || (mutation && !uuid(value.scope_id)) || value.candidate_id!==request.candidate_id || value.client_id!==request.client_id
    || !revision(value.revision) || !instant(value.next_pause_until_utc)
    || typeof value.next_pause_until_local!=='string' || !/^\d{4}-\d{2}-\d{2} \d{2}:\d{2} Europe\/London$/.test(value.next_pause_until_local)
    || typeof value.can_pause!=='boolean' || typeof value.can_resume!=='boolean'
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
function decodeContactPauseReply(value,action,request) {
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

const api=Object.freeze({decode:decodeContactPauseReply});
if(typeof module==='object'&&module.exports)module.exports=api;
root.CloudTMSContactPauseContractV1=api;
})(typeof window==='object'?window:globalThis);
