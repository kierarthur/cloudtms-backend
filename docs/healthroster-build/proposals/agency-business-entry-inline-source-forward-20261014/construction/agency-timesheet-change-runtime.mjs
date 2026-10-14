// Concrete one-click continuation. Only literal current native owners at the
// actual broker mount supply these dependencies. No phase, target, snapshot,
// qualification or actor comes from a browser result or a Worker count.
const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const SHA=/^[0-9a-f]{64}$/;
const CODE=/^[A-Z][A-Z0-9_]{0,199}$/;
const PHASES=new Set(['CAPTURED','DRAFT_REMOVED','UNAUTHORISED','SCHEDULE_APPLIED',
 'RECOMPUTE_PENDING','FINANCIAL_CURRENT','AUTHORISED','COMPLETED']);
const phaseOrder=[...PHASES];
const fail=code=>{throw Object.assign(new Error(code),{code});};
const plain=v=>!!v&&typeof v==='object'&&!Array.isArray(v)
 &&[Object.prototype,null].includes(Object.getPrototypeOf(v));
const exact=(v,keys,code)=>{if(!plain(v)||Object.keys(v).sort().join('|')!==[...keys].sort().join('|'))fail(code);};
const bound=(v,n,code)=>{let s;try{s=JSON.stringify(v);}catch{fail(code);}
 if(typeof s!=='string'||new TextEncoder().encode(s).byteLength>n)fail(code);};
const id=(v,code)=>{if(typeof v!=='string'||!UUID.test(v))fail(code);return v;};
const clone=v=>JSON.parse(JSON.stringify(v));
const nativeBtrim=s=>s.replace(/^ +| +$/g,'');
export function agencyBusinessRequest(payload,actorUserId){
 const code='AGENCY_CHANGE_BUSINESS_REQUEST_INVALID';
 exact(payload,['contract_week_id','reason','expected_signature','request_key'],code);bound(payload,16384,code);
 id(payload.contract_week_id,code);id(actorUserId,code);
 for(const [key,max] of [['reason',2000],['request_key',200],['expected_signature',512]]){
  const s=payload[key];if(typeof s!=='string'||[...s].length<1||[...s].length>max
   ||(key!=='expected_signature'&&s!==nativeBtrim(s)))fail(code);}
 return {p_contract_week_id:payload.contract_week_id,p_reason:payload.reason,
  p_expected_signature:payload.expected_signature,p_request_key:payload.request_key,p_actor_user_id:actorUserId};
}
export function decodeAgencyBusinessResult(value,args,previous=null){
 const code='AGENCY_CHANGE_NATIVE_BUSINESS_RESULT_INVALID';
 exact(value,['schema_version','request_id','request_key','request_sha256','request_version','state','phase',
  'view_actor_user_id','contract_week_id','timesheet_id','timesheet_version','current_row_signature','completed',
  'completed_actor_user_id','return_invoice_no','completion_sha256','last_refusal_code','return_hold','continuation','code'],code);
 bound(value,16384,code);id(value.request_id,code);id(value.timesheet_id,code);
 if(value.schema_version!=='WEEKLY_SOURCE_AGENCY_BUSINESS_RESULT_V1'
  ||value.request_key!==args.p_request_key||value.view_actor_user_id!==args.p_actor_user_id
  ||value.contract_week_id!==args.p_contract_week_id||typeof value.request_sha256!=='string'||!SHA.test(value.request_sha256)
  ||!Number.isSafeInteger(value.request_version)||value.request_version<1
  ||!Number.isSafeInteger(value.timesheet_version)||value.timesheet_version<1
  ||typeof value.current_row_signature!=='string'||value.current_row_signature.length<1||value.current_row_signature.length>512
  ||!['ACTIVE','COMPLETED'].includes(value.state)||!PHASES.has(value.phase)
  ||typeof value.completed!=='boolean'||typeof value.return_hold!=='boolean'
  ||typeof value.code!=='string'||!CODE.test(value.code)
  ||(value.last_refusal_code!==null&&(typeof value.last_refusal_code!=='string'||!CODE.test(value.last_refusal_code))))fail(code);
 const continuation=value.state==='COMPLETED'?'NONE':value.phase==='UNAUTHORISED'?'MANUAL'
  :['RECOMPUTE_PENDING','AUTHORISED'].includes(value.phase)?'WORKER':'NONE';
 if(value.continuation!==continuation||value.completed!==(value.state==='COMPLETED')
  ||value.completed!==(value.phase==='COMPLETED'))fail(code);
 if(value.completed){id(value.completed_actor_user_id,code);
  if(value.code!=='OK'||value.return_hold||value.last_refusal_code!==null
   ||typeof value.completion_sha256!=='string'||!SHA.test(value.completion_sha256)
   ||(value.return_invoice_no!==null&&(typeof value.return_invoice_no!=='string'||value.return_invoice_no.length>200)))fail(code);
 }else if(value.completed_actor_user_id!==null||value.completion_sha256!==null||value.return_invoice_no!==null
  ||value.code!==(value.last_refusal_code??'AGENCY_INVOICE_TSFIN_NOT_READY'))fail(code);
 if(previous&&(value.request_id!==previous.request_id||value.request_sha256!==previous.request_sha256
  ||value.timesheet_id!==previous.timesheet_id||value.timesheet_version!==previous.timesheet_version
  ||value.request_version<previous.request_version
  ||phaseOrder.indexOf(value.phase)<phaseOrder.indexOf(previous.phase)))fail('AGENCY_CHANGE_NATIVE_BUSINESS_TARGET_CHANGED');
 return clone(value);
}
const causal=error=>{
 for(const code of [error?.code,error?.json?.message])if(typeof code==='string'&&CODE.test(code))return code;
 return 'AGENCY_CHANGE_INLINE_NATIVE_DEPENDENCY_UNAVAILABLE';
};
export function decodeAgencyInlineReply(value,args){
 const code='AGENCY_CHANGE_NATIVE_BUSINESS_RESULT_INVALID';
 exact(value,['result','runtime_refusal_code'],code);bound(value,18432,code);
 const result=decodeAgencyBusinessResult(value.result,args);
 if(value.runtime_refusal_code!==null&&(typeof value.runtime_refusal_code!=='string'||!CODE.test(value.runtime_refusal_code)))fail(code);
 if(result.completed&&value.runtime_refusal_code!==null)fail(code);
 return {result,runtime_refusal_code:value.runtime_refusal_code};
}
export function createAgencyTimesheetChangeRuntime({nativeCommand,runManual,runWorkerOnce}){
 if([nativeCommand,runManual,runWorkerOnce].some(f=>typeof f!=='function'))fail('AGENCY_CHANGE_INLINE_DEPENDENCY_INVALID');
 return async({request,actor})=>{
  const args=agencyBusinessRequest(request,actor?.id);let current=null,runtimeRefusal=null;
  const read=async()=>{current=decodeAgencyBusinessResult(await nativeCommand(args),args,current);return current;};
  await read();if(current.completed)return {result:current,runtime_refusal_code:null};
  if(current.continuation==='MANUAL'){
   try{await runManual(current.request_id);}catch(error){runtimeRefusal=causal(error);}
   // The genuine manual result never replaces stored phase/target authority.
   await read();if(runtimeRefusal)return {result:current,runtime_refusal_code:current.completed?null:runtimeRefusal};
  }
  // At most one genuine MANUAL recompute and one authorised validation turn.
  // The existing targeted dequeue retains leases. No force enqueue, generic
  // drain, sleep, floating continuation, or queue-count-to-completion shortcut.
  for(let turn=0;turn<2&&current.continuation==='WORKER'&&!current.completed;turn++){
   const before=current;
   try{await runWorkerOnce(current.timesheet_id);}catch(error){runtimeRefusal=causal(error);}
   await read();
   if(runtimeRefusal||current.completed||current.request_version===before.request_version)break;
  }
  return {result:current,runtime_refusal_code:current.completed?null:runtimeRefusal};
 };
}
