// Closed transport proof only; SQL owns native action, account and cooldown.
const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i,HASH=/^[0-9a-f]{64}$/;
const closed=(v,keys)=>v!==null&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
const uuid=v=>typeof v==='string'&&UUID.test(v),hash=v=>typeof v==='string'&&HASH.test(v);
const date=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v+'T00:00:00Z'))&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
const keys=['contract','actor_user_id','permission_view','scope','parent_group_key','parent_proof_hash','action_id','candidate_context_hash'];
export class CheckOnlyReminderError extends Error{constructor(code,status=400){super(code);this.code=code;this.status=status;}}
export function checkOnlyReminderAction(value,actorUserId){
  if(!closed(value,keys)||value.contract!=='WEEKLY_SOURCE_CHECK_ONLY_D10_CANDIDATE_REMINDER_ACTION_V1'
    ||!uuid(actorUserId)||value.actor_user_id!==actorUserId||value.permission_view!=='ADMIN'
    ||!closed(value.scope,['source_group_id','source_cycle_id','client_id','week_ending'])
    ||!['source_group_id','source_cycle_id','client_id'].every(k=>uuid(value.scope[k]))||!date(value.scope.week_ending)
    ||typeof value.parent_group_key!=='string'||!/^d10g_[0-9a-f]{64}$/.test(value.parent_group_key)
    ||!['parent_proof_hash','action_id','candidate_context_hash'].every(k=>hash(value[k])))
    throw new CheckOnlyReminderError('WEEKLY_SOURCE_CHECK_ONLY_REMINDER_REQUEST_INVALID');
  return structuredClone(value);
}
export function checkOnlyReminderRequest(body,user){
  if(user?.is_active!==true||user?.payView!==false||String(user?.role).toLowerCase()!=='admin')
    throw new CheckOnlyReminderError('WEEKLY_SOURCE_CHECK_ONLY_REMINDER_ACTOR_FORBIDDEN',403);
  if(!closed(body,['action','payload'])||body.action!=='CHECK_ONLY_D10_REMIND_CANDIDATE'
    ||!closed(body.payload,['descriptor','idempotency_key'])||!uuid(body.payload.idempotency_key))
    throw new CheckOnlyReminderError('WEEKLY_SOURCE_CHECK_ONLY_REMINDER_REQUEST_INVALID');
  const action=checkOnlyReminderAction(body.payload.descriptor,user.id);
  return {actor_user_id:user.id,scope:action.scope,parent_group_key:action.parent_group_key,
    parent_proof_hash:action.parent_proof_hash,action_id:action.action_id,candidate_context_hash:action.candidate_context_hash,
    idempotency_key:body.payload.idempotency_key};
}
export function checkOnlyReminderReceipt(value,request){
  if(!closed(value,['ok','contract','actor_user_id','scope','action_id','idempotency_key','notification_id','queued','replay','next_available_at_utc'])
    ||value.ok!==true||value.contract!=='WEEKLY_SOURCE_CHECK_ONLY_CANDIDATE_REMINDER_V1'
    ||value.actor_user_id!==request.actor_user_id||value.action_id!==request.action_id||value.idempotency_key!==request.idempotency_key
    ||!closed(value.scope,Object.keys(request.scope))||!Object.keys(request.scope).every(k=>value.scope[k]===request.scope[k])
    ||!uuid(value.notification_id)||value.queued!==true||typeof value.replay!=='boolean'
    ||typeof value.next_available_at_utc!=='string'||!/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(value.next_available_at_utc)
    ||!Number.isFinite(Date.parse(value.next_available_at_utc)))
    throw new CheckOnlyReminderError('WEEKLY_SOURCE_CHECK_ONLY_REMINDER_RESULT_UNCONFIRMED',502);
  return value;
}
