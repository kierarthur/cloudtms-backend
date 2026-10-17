// Health contribution, intentionally not registered in the shared Worker root.
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const KEYS=['current_timesheet_id','disabled_reason','may_open_review','placement_revision','purpose','requested_timesheet_id','version'];
const unavailable=()=>reply(503,'CORRECTION_ENTRY_UNAVAILABLE');
function reply(status,error){return new Response(JSON.stringify({error}),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store'}});}
export function matchCorrectionEntry(req){
 const url=new URL(req.url),match=/^\/api\/timesheets\/([0-9a-f-]+)\/correction-entry$/.exec(url.pathname);
 return match?{timesheetId:match[1]}:null;
}
export function decodeCorrectionEntry(value,timesheetId){
 if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).sort().join('|')!==KEYS.join('|')
  ||value.version!=='TIMESHEET_CORRECTION_ENTRY_V1'||value.requested_timesheet_id!==timesheetId
  ||!UUID.test(timesheetId)||value.purpose!=='CONTRACT_CORRECTION'
  ||typeof value.placement_revision!=='string'||!/^(0|[1-9][0-9]*)$/.test(value.placement_revision)
  ||BigInt(value.placement_revision)>9223372036854775807n||value.may_open_review!==false
  ||!['REVIEW_UNAVAILABLE','CURRENT_ALLOCATION_PENDING'].includes(value.disabled_reason)
  ||(value.current_timesheet_id===null?value.disabled_reason!=='CURRENT_ALLOCATION_PENDING'
    :typeof value.current_timesheet_id!=='string'||!UUID.test(value.current_timesheet_id)||value.disabled_reason!=='REVIEW_UNAVAILABLE')){
  throw new Error('CORRECTION_ENTRY_INVALID_RESPONSE');
 }
 return Object.freeze({...value});
}
export async function handleCorrectionEntry(req,{timesheetId,agencyId,requireOfficeUser,rpc}){
 const url=new URL(req.url);
 if(req.method!=='GET')return reply(405,'METHOD_NOT_ALLOWED');
 if(!UUID.test(timesheetId)||url.search||req.body!==null)return reply(400,'CORRECTION_ENTRY_INVALID_REQUEST');
 if(typeof agencyId!=='string'||!UUID.test(agencyId))return unavailable();
 let user;
 try{user=await requireOfficeUser();}catch{return unavailable();}
 if(!user||!UUID.test(String(user.id||'').toLowerCase())||user.tenant_id!==agencyId)return reply(401,'Unauthorized');
 // Existing requireUser re-reads the current active user. SQL repeats WP-PP.
 if(user.is_active!==true||!(String(user.role||'').trim().toLowerCase()==='admin'
   ||user.payment_authoriser===true||user.payment_golden_key===true))return reply(403,'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED');
 let raw;
 try{
  raw=await rpc('timesheet_correction_entry_v1',{p_request:{version:'TIMESHEET_CORRECTION_ENTRY_REQUEST_V1',timesheet_id:timesheetId,actor_user_id:user.id}});
 }catch(error){
  // sbRpc's real json/body/status/fn envelope is consumed, never returned/logged.
  let json=error?.json;
  if(!json&&typeof error?.body==='string'){try{json=JSON.parse(error.body);}catch{}}
  if(json?.code==='42501'&&error?.fn==='timesheet_correction_entry_v1'&&[401,403].includes(error?.status))return reply(403,'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED');
  return unavailable();
 }
 try{return new Response(JSON.stringify(decodeCorrectionEntry(raw,timesheetId)),{status:200,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store'}});}catch{return unavailable();}
}
