// Existing email drain adapter only. It neither builds a body nor sends mail.
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX=/^[0-9a-f]{64}$/;
const query=row=>String(row?.type||'').trim().toUpperCase()==='TIMESHEET_QUERY';
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const exact=(value,keys)=>object(value)&&Object.keys(value).length===keys.length&&keys.every(k=>Object.hasOwn(value,k));
const uuid=value=>typeof value==='string'&&UUID.test(value);
const instant=value=>value===null||(typeof value==='string'&&/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/.test(value)&&Number.isFinite(Date.parse(value)));
export function createTimesheetQueryContactDrain({rpc,deployment,workerId,leaseToken}){
 if(typeof rpc!=='function'||!exact(deployment,['agency_id','environment'])||!uuid(deployment.agency_id)
  ||!['TEST','LIVE'].includes(deployment.environment)||typeof workerId!=='string'||!workerId||workerId.length>200
  ||typeof leaseToken!=='string'||!leaseToken||leaseToken.length>500)return Object.freeze({
   wake:async()=>({ok:false,reason:'CONTACT_QUERY_DRAIN_CONFIGURATION_INVALID'}),
   prepare:async row=>query(row)?{applicable:true,allowed:false,reason:'CONTACT_QUERY_DRAIN_CONFIGURATION_INVALID'}:{applicable:false,allowed:true},
   start:async row=>query(row)?{applicable:true,allowed:false,reason:'CONTACT_QUERY_DRAIN_CONFIGURATION_INVALID'}:{applicable:false,allowed:true},
   reconcileProviderBatch:async(response,rowIds,rowsById)=>rowIds.filter(id=>query(rowsById.get(String(id)))).map(id=>({id,disposition:'RESULT_UNCONFIRMED',reason:'CONTACT_QUERY_DRAIN_CONFIGURATION_INVALID'}))
  });
 const permits=new Map();
 const call=(name,request)=>rpc(name,{p_request:{...request,worker_id:workerId,...deployment}});
 const identity=row=>{
  if(!query(row)||!uuid(row.id)||row.context_kind!=='TIMESHEET_QUERY_DELIVERY'||!uuid(row.context_id)
   ||row.attempt_lease_token!==leaseToken)throw new Error('CONTACT_QUERY_NATIVE_CLAIM_INVALID');
  return {mail_outbox_id:row.id,query_delivery_id:row.context_id,attempt_lease_token:leaseToken};
 };
 const held=reason=>({applicable:true,allowed:false,reason});
 async function wake(){
  try{
   const value=await call('timesheet_query_email_recompose_ready_v1',{version:'TIMESHEET_QUERY_RECOMPOSE_READY_V1',limit:25});
   if(!exact(value,['ok','version','results'])||value.ok!==true||value.version!=='TIMESHEET_QUERY_RECOMPOSE_READY_RESULT_V1'||!Array.isArray(value.results)||value.results.length>25)throw new Error();
   if(value.results.some(result=>result?.disposition==='HELD_NATIVE_RECHECK'))return {ok:false,reason:'CONTACT_QUERY_NATIVE_WAKE_HELD'};
   return {ok:true};
  }catch{return {ok:false,reason:'CONTACT_QUERY_NATIVE_WAKE_UNAVAILABLE'};}
 }
 async function prepare(row){
  if(!query(row))return {applicable:false,allowed:true};
  try{
   const id=identity(row),value=await call('timesheet_query_email_recompose_v1',{version:'TIMESHEET_QUERY_RECOMPOSE_V1',...id});
   if(!exact(value,['ok','version','mail_outbox_id','query_delivery_id','original_delivery_id','disposition','replacements','excluded','next_eligible_at_utc'])
    ||value.ok!==true||value.version!=='TIMESHEET_QUERY_RECOMPOSE_RESULT_V1'||value.mail_outbox_id!==id.mail_outbox_id||value.query_delivery_id!==id.query_delivery_id
    ||!uuid(value.original_delivery_id)||!['CURRENT','RECOMPOSED','PAUSED','CHECK_ORIGINAL_ATTEMPT','OBSOLETE'].includes(value.disposition)
    ||!Array.isArray(value.replacements)||value.replacements.length>1||!Array.isArray(value.excluded)||value.excluded.length>500||!instant(value.next_eligible_at_utc)
    ||value.replacements.some(r=>!exact(r,['mail_outbox_id','query_delivery_id'])||!uuid(r.mail_outbox_id)||!uuid(r.query_delivery_id))
    ||value.excluded.some(r=>!exact(r,['action_id','issue_id','reason'])||typeof r.action_id!=='string'||!HEX.test(r.action_id)||!uuid(r.issue_id)
      ||!['ALREADY_ACCEPTED','CHECK_ORIGINAL_ATTEMPT','OBSOLETE','CONTACT_PAUSED'].includes(r.reason)))throw new Error();
   if(value.disposition==='CURRENT'){
    if(value.replacements.length!==1||value.replacements[0].mail_outbox_id!==id.mail_outbox_id||value.replacements[0].query_delivery_id!==id.query_delivery_id)throw new Error();
    return {applicable:true,allowed:true};
   }
   return held(value.disposition);
  }catch{return held('CONTACT_QUERY_NATIVE_RECOMPOSITION_UNAVAILABLE');}
 }
 async function start(row){
  if(!query(row))return {applicable:false,allowed:true};
  try{
   const id=identity(row),value=await call('weekly_contact_submission_start_v1',{version:'CONTACT_START_V1',owner_kind:'TIMESHEET_QUERY_OUTBOX',...id});
   const base=['ok','version','owner_kind','channel','mail_outbox_id','query_delivery_id'];
   if(value?.ok===true){
    if(!exact(value,[...base,'contact_permit_id','content_hash','call_once'])||value.version!=='CONTACT_START_RESULT_V1'||value.owner_kind!=='TIMESHEET_QUERY_OUTBOX'
     ||value.channel!=='EMAIL'||value.mail_outbox_id!==id.mail_outbox_id||value.query_delivery_id!==id.query_delivery_id
     ||!uuid(value.contact_permit_id)||typeof value.content_hash!=='string'||!HEX.test(value.content_hash)||value.call_once!==true)throw new Error();
    if(permits.has(row.id))return held('CHECK_ORIGINAL_ATTEMPT');
    permits.set(row.id,{...id,contact_permit_id:value.contact_permit_id,content_hash:value.content_hash});
    return {applicable:true,allowed:true};
   }
   if(!exact(value,[...base,'reason','call_once','next_eligible_at_utc'])||value.ok!==false||value.version!=='CONTACT_START_RESULT_V1'
    ||value.owner_kind!=='TIMESHEET_QUERY_OUTBOX'||value.channel!=='EMAIL'||value.mail_outbox_id!==id.mail_outbox_id||value.query_delivery_id!==id.query_delivery_id
    ||!['CHECK_ORIGINAL_ATTEMPT','OBSOLETE','NOT_STARTED_PAUSED'].includes(value.reason)||value.call_once!==false||!instant(value.next_eligible_at_utc))throw new Error();
   return held(value.reason);
  }catch{return held('CONTACT_QUERY_START_OUTCOME_UNCONFIRMED');}
 }
 async function record(row,providerResult){
  const permit=permits.get(row.id);
  if(!permit)return {id:row.id,disposition:'RESULT_UNCONFIRMED',reason:'CONTACT_QUERY_ORIGINAL_PERMIT_MISSING'};
  try{
   const value=await call('timesheet_query_email_contact_result_v1',{version:'TIMESHEET_QUERY_RESULT_V1',...permit,provider_result:providerResult});
   if(!exact(value,['ok','version','contact_permit_id','mail_outbox_id','query_delivery_id','disposition'])||value.ok!==true||value.version!=='TIMESHEET_QUERY_RESULT_RECORDED_V1'
    ||value.contact_permit_id!==permit.contact_permit_id||value.mail_outbox_id!==row.id||value.query_delivery_id!==permit.query_delivery_id
    ||!['ACCEPTED','AMBIGUOUS'].includes(value.disposition)||((providerResult.status==='SUCCESS')!==(value.disposition==='ACCEPTED')))throw new Error();
   return {id:row.id,disposition:value.disposition,provider_message_id:providerResult.provider_message_id};
  }catch{return {id:row.id,disposition:'RESULT_UNCONFIRMED',reason:'CONTACT_QUERY_RESULT_OUTCOME_UNCONFIRMED'};}
 }
 async function reconcileProviderBatch(response,rowIds,rowsById){
  let results=null;
  try{const body=typeof response?.body==='string'?JSON.parse(response.body):response?.body;if(response?.ok===true&&Array.isArray(body?.results))results=body.results;}catch{}
  const outcomes=[];
  for(const id of rowIds){
   const row=rowsById.get(String(id));if(!query(row))continue;
   const matches=Array.isArray(results)?results.filter(r=>String(r?.outbox_id||r?.outboxId||r?.meta?.outbox_id||r?.meta?.outboxId||r?.meta?.id||'')===String(id)):[];
   const result=matches.length===1?matches[0]:null;
   const status=String(result?.status||'').toUpperCase();
   const rawId=result?.provider_message_id??result?.providerMessageId??null;
   const providerId=typeof rawId==='string'&&rawId.length>0&&rawId.length<=500?rawId:null;
   const accepted=result&&status==='SUCCESS'&&(rawId===null||providerId!==null);
   const error=accepted?null:String(result?.error||result?.body||result?.message||'Provider outcome is being checked').slice(0,1000);
   outcomes.push(await record(row,{status:accepted?'SUCCESS':status==='FAILED'?'FAILED':'UNKNOWN',provider_message_id:providerId,error}));
  }
  return outcomes;
 }
 return Object.freeze({wake,prepare,start,reconcileProviderBatch});
}
