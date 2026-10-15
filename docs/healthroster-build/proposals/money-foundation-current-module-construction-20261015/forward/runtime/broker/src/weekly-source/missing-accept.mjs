const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const HASH=/^[0-9a-f]{64}$/;
const KEYS=Object.freeze(['schema_version','work_event_id','root_timesheet_id','projection_publication_id',
  'expected_root_version','expected_facts_fingerprint','expected_inventory_digest','idempotency_key']);
export class MissingAcceptRequestError extends Error {
  constructor(){super('This proposal could not be prepared.');this.name='MissingAcceptRequestError';this.code='WEEKLY_SOURCE_MISSING_ACCEPT_PREPARE_INVALID';}
}
export function missingAcceptPrepareRequest(payload,actorUserId){
  if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.getPrototypeOf(payload)!==Object.prototype
    ||Object.keys(payload).length!==KEYS.length||!KEYS.every(key=>Object.hasOwn(payload,key))
    ||payload.schema_version!=='WEEKLY_SOURCE_MISSING_ACCEPT_PREPARE_V1'
    ||!['work_event_id','root_timesheet_id','projection_publication_id'].every(key=>typeof payload[key]==='string'&&UUID.test(payload[key]))
    ||typeof actorUserId!=='string'||!UUID.test(actorUserId)
    ||!Number.isSafeInteger(payload.expected_root_version)||payload.expected_root_version<1||payload.expected_root_version>2147483647
    ||!['expected_facts_fingerprint','expected_inventory_digest'].every(key=>typeof payload[key]==='string'&&HASH.test(payload[key]))
    ||typeof payload.idempotency_key!=='string'||payload.idempotency_key!==payload.idempotency_key.trim()
    ||payload.idempotency_key.length<16||payload.idempotency_key.length>200)throw new MissingAcceptRequestError();
  return {...payload,actor_user_id:actorUserId};
}
export function missingAcceptContextRequest(payload,actorUserId){
  const keys=['schema_version','work_event_id','root_timesheet_id','projection_publication_id'];
  if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.getPrototypeOf(payload)!==Object.prototype
    ||Object.keys(payload).length!==keys.length||!keys.every(key=>Object.hasOwn(payload,key))
    ||payload.schema_version!=='WEEKLY_SOURCE_MISSING_ACCEPT_CONTEXT_V1'
    ||!keys.slice(1).every(key=>typeof payload[key]==='string'&&UUID.test(payload[key]))
    ||typeof actorUserId!=='string'||!UUID.test(actorUserId))throw new MissingAcceptRequestError();
  return {...payload,actor_user_id:actorUserId};
}
