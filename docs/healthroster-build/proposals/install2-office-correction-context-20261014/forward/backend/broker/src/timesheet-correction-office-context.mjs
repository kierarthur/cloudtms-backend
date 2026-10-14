// Bounded Office read consumer of existing native public projections. Raw
// TSFIN/PREPARE/private U3 context is never a public DTO or an action allowance.
import { assertTimesheetCorrectionOfficeActor } from './timesheet-correction-office-routes.mjs';
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const same=(a,b)=>typeof a==='string'&&typeof b==='string'&&UUID.test(a)&&UUID.test(b)&&a.toLowerCase()===b.toLowerCase();
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
function refuse(code,status=409){const e=new Error(code);e.code=code;e.status=status;throw e;}
function need(value,code,status){if(!value)refuse(code,status);}
function projection(value,candidateId){
  if(value===null)return null;
  need(object(value)&&value.schema_version===1&&same(value.candidate_id,candidateId)&&
    ['request_id','root_timesheet_id','reversed_timesheet_id','replacement_timesheet_id'].every(key=>UUID.test(value[key]||''))&&
    value.correction_id===`awc:${value.root_timesheet_id}:${value.request_id}`&&
    ['WAITING','APPLIED','CANCELLED'].includes(value.state)&&Number.isSafeInteger(value.state_revision)&&value.state_revision>0&&
    ['SIGNED_TIMESHEET_CORRECTION','AGENCY_INVOICE_CORRECTION'].includes(value.origin),'CORRECTION_CONTEXT_CONTRACT_UNPROVED');
  return {request_id:value.request_id,correction_id:value.correction_id,origin:value.origin,state:value.state,
    state_revision:value.state_revision,root_timesheet_id:value.root_timesheet_id,reversed_timesheet_id:value.reversed_timesheet_id,
    replacement_timesheet_id:value.replacement_timesheet_id};
}
export async function readTimesheetCorrectionOfficeContext(env,request,timesheetId,owners){
  need(request.method==='GET'&&UUID.test(timesheetId||'')&&[...new URL(request.url).searchParams].length===0,
    'CORRECTION_PAYLOAD_INVALID',400);
  const currentUser=await owners.requireUser(env,request,[]);
  const actor=assertTimesheetCorrectionOfficeActor(env,currentUser,'PAY_LIFECYCLE');
  const view=currentUser.role==='admin'?'ADMIN':'PAY',id=timesheetId.toLowerCase();
  // Existing native W14/W15 resolves and scopes the physical target before
  // request readers. Flags alone cannot expose an ordinary/Daily request.
  if(view==='PAY')await owners.assertScope({timesheetId:id,user:currentUser});
  const contexts=await owners.readContexts([id]);
  need(Array.isArray(contexts)&&contexts.length===1,'CORRECTION_CONTEXT_TARGET_UNPROVED');
  const context=contexts[0],ts=context?.out_timesheet,candidateId=context?.out_candidate?.id;
  need(same(context?.effective_timesheet_id,id)&&same(ts?.timesheet_id,id)&&ts.is_current===true&&ts.archived_at_utc==null&&
    Number.isSafeInteger(ts.version)&&ts.version>0&&UUID.test(candidateId||''),'CORRECTION_CONTEXT_TARGET_UNPROVED');
  // CREATE's genuine existing owner admits an already owned physical P0 only.
  // An untouched original returns null; it cannot be made eligible from raw
  // physical or finance facts. Preserve that absence without an empty fake DTO.
  const create=view==='ADMIN'?projection(await owners.readCreateProjection({candidateId,timesheetId:id}),candidateId):null;
  if(create!==null)need(same(create.replacement_timesheet_id,id)&&ts.correction_kind==='WHOLE_TIMESHEET_REPLACEMENT'&&
    ts.correction_id===create.correction_id,'CORRECTION_CONTEXT_TARGET_UNPROVED');
  const current=projection(await owners.readRequestProjection({candidateId,timesheetId:id}),candidateId);
  if(current!==null)need([current.root_timesheet_id,current.reversed_timesheet_id,current.replacement_timesheet_id].some(value=>same(value,id))||
    (ts.correction_kind==='WHOLE_TIMESHEET_REVERSAL'&&ts.correction_id===current.correction_id),'CORRECTION_CONTEXT_TARGET_UNPROVED');
  if(create!==null)need(current!==null&&JSON.stringify(current)===JSON.stringify(create),'CORRECTION_REQUEST_CAS_STALE');
  let requestDescriptor=null;
  if(current!==null){
    const authority=await owners.readRequestAuthority({requestId:current.request_id,actor});
    need(object(authority)&&Object.keys(authority).length===6&&authority.contract_version==='TIMESHEET_CORRECTION_REQUEST_AUTHORITY_V1'&&
      same(authority.request_id,current.request_id)&&authority.state===current.state&&
      typeof authority.state_revision==='string'&&/^[1-9]\d{0,15}$/.test(authority.state_revision)&&
      Number.isSafeInteger(Number(authority.state_revision))&&Number(authority.state_revision)===current.state_revision&&
      same(authority.replacement_timesheet_id,current.replacement_timesheet_id)&&
      (authority.return_invoice_id===null||UUID.test(authority.return_invoice_id||'')), 'CORRECTION_REQUEST_CAS_STALE');
    requestDescriptor=view==='ADMIN'?{...current,return_invoice_id:authority.return_invoice_id}:current;
  }
  const afterUser=await owners.requireUser(env,request,[]),after=assertTimesheetCorrectionOfficeActor(env,afterUser,'PAY_LIFECYCLE');
  need(after.actor_user_id===actor.actor_user_id&&(afterUser.role==='admin'?'ADMIN':'PAY')===view,'CORRECTION_ACTOR_NOT_ALLOWED',403);
  if(view==='PAY')await owners.assertScope({timesheetId:id,user:afterUser});
  // Only source identity and real saved native request state/CAS are exposed.
  // No can_send/qualification/readiness/payment/hold or prepared data is minted.
  return {ok:true,contract_version:'TIMESHEET_CORRECTION_OFFICE_CONTEXT_V1',view,
    target:{timesheet_id:id,timesheet_version:ts.version},create_descriptor:create,request:requestDescriptor};
}
