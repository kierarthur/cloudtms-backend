// Strict adapters for the existing /settings/global GET and PUT owners.
import {parsePushTimingJson} from './push-timing-office.mjs';
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const REV=/^[1-9][0-9]*$/;
const MAX='9223372036854775807';
const REQUIRED=Object.freeze(['candidate_reminder_minutes','candidate_response_deadline_minutes','manager_digest_minutes',
 'manager_manual_resend_cooldown_minutes','candidate_manual_reminder_cooldown_minutes','manager_secure_link_days',
 'manager_first_delay_value','manager_first_delay_unit','manager_followups','manager_recent_completed_weeks','manager_max_work_age_days']);
export class GlobalContactTimingError extends Error {
 constructor(code,status=400){super(status===409?'Settings changed. Refresh settings before editing again.':
  status===503?'Settings cannot be confirmed right now. Refresh settings.':status===403?'Current Office administrator permission is required.':'The global contact settings request is invalid.');this.code=code;this.status=status;}
}
function invalid(code='WEEKLY_SOURCE_GLOBAL_SETTINGS_VALUES_INVALID'){throw new GlobalContactTimingError(code);}
function closed(x,allowed,required=allowed){
 if(!x||typeof x!=='object'||Array.isArray(x)||Object.keys(x).some(k=>!allowed.includes(k))||required.some(k=>!Object.hasOwn(x,k)))invalid();
}
function integer(x,min,max){if(!Number.isSafeInteger(x)||x<min||x>max)invalid();}
function revision(x){if(typeof x!=='string'||!REV.test(x)||x.length>MAX.length||(x.length===MAX.length&&x>MAX))invalid();return x;}
function interval(value,unit,zero){integer(value,zero?0:1,unit==='HOURS'?8784:366);if(!['HOURS','CALENDAR_DAYS','CLIENT_WORKING_DAYS'].includes(unit))invalid();}
export function validateGlobalContactSettings(x,{response=false}={}){
 closed(x,[...REQUIRED,'automatic_reconcile_actor_user_id'],response?[...REQUIRED,'automatic_reconcile_actor_user_id']:REQUIRED);
 integer(x.candidate_reminder_minutes,1,719);integer(x.candidate_response_deadline_minutes,x.candidate_reminder_minutes+1,10080);
 integer(x.manager_digest_minutes,1,10080);integer(x.manager_manual_resend_cooldown_minutes,5,1440);
 integer(x.candidate_manual_reminder_cooldown_minutes,60,10080);integer(x.manager_secure_link_days,1,30);
 interval(x.manager_first_delay_value,x.manager_first_delay_unit,true);
 integer(x.manager_recent_completed_weeks,1,52);integer(x.manager_max_work_age_days,1,366);
 if(!Array.isArray(x.manager_followups)||x.manager_followups.length>10)invalid();
 x.manager_followups.forEach((step,index)=>{
  closed(step,['sequence','interval_value','interval_unit','mode']);
  if(step.sequence!==index+1||!['MANUAL','AUTOMATIC'].includes(step.mode))invalid();
  interval(step.interval_value,step.interval_unit,false);
 });
 if(Object.hasOwn(x,'automatic_reconcile_actor_user_id')&&x.automatic_reconcile_actor_user_id!==null&&
  (typeof x.automatic_reconcile_actor_user_id!=='string'||!UUID.test(x.automatic_reconcile_actor_user_id)))invalid();
 return structuredClone(x);
}
export function parseGlobalContactSettingsSave(body){
 try{
  closed(body,['request_id','expected_settings_version','settings']);
  if(typeof body.request_id!=='string'||!UUID.test(body.request_id))invalid();
  return {request_id:body.request_id,expected_settings_version:revision(body.expected_settings_version),settings:validateGlobalContactSettings(body.settings)};
 }catch(error){if(error instanceof GlobalContactTimingError)throw new GlobalContactTimingError('WEEKLY_SOURCE_GLOBAL_SETTINGS_SAVE_REQUEST_INVALID');throw error;}
}
export async function readGlobalContactSettingsSave(request){
 try{
  // Stop reading at the boundary rather than buffer an unbounded request first.
  if(!request.body)invalid();
  const reader=request.body.getReader(),chunks=[];let length=0;
  try{
   for(;;){
    const {done,value}=await reader.read();if(done)break;
    length+=value.byteLength;
    if(length>65536){await reader.cancel();invalid();}
    chunks.push(value);
   }
  }finally{reader.releaseLock();}
  const bytes=new Uint8Array(length);let offset=0;
  for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.byteLength;}
  return parseGlobalContactSettingsSave(parsePushTimingJson(new TextDecoder('utf-8',{fatal:true}).decode(bytes)));
 }catch(error){if(error instanceof GlobalContactTimingError)throw error;throw new GlobalContactTimingError('WEEKLY_SOURCE_GLOBAL_SETTINGS_SAVE_REQUEST_INVALID');}
}
export async function globalContactSettingsOwner(work){
 try{return await work();}catch(error){
  if(error instanceof GlobalContactTimingError)throw error;
  const code=error?.json?.message,sqlstate=error?.json?.code;
  const expected={WEEKLY_SOURCE_SETTINGS_STALE:['40001',409],WEEKLY_SOURCE_SETTINGS_REQUEST_REUSED:['40001',409],
   WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED:['42501',403],WEEKLY_SOURCE_GLOBAL_SETTINGS_VALUES_INVALID:['22023',400],
   WEEKLY_SOURCE_GLOBAL_SETTINGS_SAVE_REQUEST_INVALID:['22023',400],WEEKLY_SOURCE_SETTINGS_VERSION_EXHAUSTED:['22003',409]};
  if(Object.hasOwn(expected,code)&&expected[code][0]===sqlstate)throw new GlobalContactTimingError(code,expected[code][1]);
  throw error;
 }
}
export function decodeGlobalContactSettingsState(body){
 try{
  closed(body,['ok','settings_version','policy_revision','settings','cutoff','actions']);
  if(body.ok!==true||revision(body.settings_version)!==revision(body.policy_revision))invalid();
  validateGlobalContactSettings(body.settings,{response:true});
  closed(body.cutoff,['kind','editable','explanation']);
  if(body.cutoff.kind!=='CLIENT_CUTOFF_PRECEDENCE'||body.cutoff.editable!==false||
   typeof body.cutoff.explanation!=='string'||!body.cutoff.explanation||body.cutoff.explanation.length>500)invalid();
  if(!Array.isArray(body.actions)||body.actions.length!==1)invalid();
  closed(body.actions[0],['action','enabled','reason']);
  if(body.actions[0].action!=='SAVE_GLOBAL_CONTACT_TIMING'||body.actions[0].enabled!==true||body.actions[0].reason!==null)invalid();
  return structuredClone(body);
 }catch(error){if(error instanceof GlobalContactTimingError)throw new GlobalContactTimingError('WEEKLY_SOURCE_SETTINGS_DEPENDENCY_UNAVAILABLE',503);throw error;}
}
