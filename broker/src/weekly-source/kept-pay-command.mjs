import{parsePushTimingJson}from'./push-timing-office.mjs';
const id=x=>typeof x==='string'&&/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(x);
const positiveCounter=x=>typeof x==='string'&&/^[1-9][0-9]{0,18}$/.test(x)&&(x.length<19||x<='9223372036854775807');
const closed=(x,keys)=>x!==null&&typeof x==='object'&&!Array.isArray(x)&&Object.keys(x).length===keys.length&&keys.every(k=>Object.hasOwn(x,k));
const fail=(code,status=400)=>{throw Object.assign(new Error(code),{code,status});};
export function parseKeptPayCommand(text){const value=parsePushTimingJson(text);validateKeptPayCommand(value);return value;}
export function validateKeptPayCommand(body){
 const keys=['schema_version','work_event_id','decision','reason','credit_invoice_line_id','expected_facts_fingerprint','idempotency_key'];
 const p=body?.payload;
 if(!closed(body,['action','payload'])||body.action!=='KEEP_AS_PAID'||!closed(p,keys)
   ||p.schema_version!=='WEEKLY_SOURCE_KEEP_AS_PAID_REQUEST_V1'||!id(p.work_event_id)||p.decision!=='KEEP_AS_PAID'
   ||!['CREDITED','NOT_INVOICED_DISAPPEARED'].includes(p.reason)
   ||(p.reason==='CREDITED'?!id(p.credit_invoice_line_id):p.credit_invoice_line_id!==null)
   ||typeof p.expected_facts_fingerprint!=='string'||!/^[0-9a-f]{64}$/.test(p.expected_facts_fingerprint)
   ||typeof p.idempotency_key!=='string'||p.idempotency_key!==p.idempotency_key.trim()
   ||p.idempotency_key.length<16||p.idempotency_key.length>200)fail('WEEKLY_SOURCE_KEEP_AS_PAID_REQUEST_INVALID');
 return p;
}
export function decodeKeptPayAcknowledgement(value,payload,actorId){
 const keys=['ok','decision','decision_id','work_event_id','reason','credit_invoice_line_id','facts_fingerprint','actor_user_id',
   'acknowledged_at_utc','acknowledgement_sequence','idempotent_replay','pay_changed','invoice_changed'];
 if(!closed(value,keys)||value.ok!==true||value.decision!=='KEEP_AS_PAID'||!id(value.decision_id)
   ||value.work_event_id!==payload.work_event_id||value.reason!==payload.reason||value.credit_invoice_line_id!==payload.credit_invoice_line_id
   ||value.facts_fingerprint!==payload.expected_facts_fingerprint||value.actor_user_id!==actorId
   ||typeof value.acknowledged_at_utc!=='string'||!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)$/.test(value.acknowledged_at_utc)
   ||!Number.isFinite(Date.parse(value.acknowledged_at_utc))||!positiveCounter(value.acknowledgement_sequence)
   ||typeof value.idempotent_replay!=='boolean'||value.pay_changed!==false||value.invoice_changed!==false)fail('WEEKLY_SOURCE_KEEP_AS_PAID_RESULT_INVALID',502);
 return value;
}
export function keptPayFailure(error){
 const finite={WEEKLY_SOURCE_KEEP_AS_PAID_REQUEST_INVALID:['22023',400],WEEKLY_SOURCE_KEEP_AS_PAID_IDEMPOTENCY_COLLISION:['22023',409],
   WEEKLY_SOURCE_KEEP_AS_PAID_FACTS_CHANGED:['40001',409],WEEKLY_SOURCE_KEEP_AS_PAID_SCOPE_CHANGED:['40001',409],
   WEEKLY_SOURCE_KEEP_AS_PAID_NOT_ELIGIBLE:['55000',409],WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED:['42501',403],
   WEEKLY_SOURCE_OFFICE_ACTOR_INACTIVE:['42501',403],WEEKLY_SOURCE_CLIENT_NOT_IN_GROUP:['22023',403],WEEKLY_SOURCE_GROUP_NOT_ACTIVE:['22023',403]};
 if(error?.fn==='weekly_source_keep_as_paid_decide_atomic_v1'){
  const row=Object.hasOwn(finite,error?.json?.message)?finite[error.json.message]:null;if(row&&row[0]===error.json.code)return{code:error.json.message,status:row[1]};
 }
 return{code:'WEEKLY_SOURCE_KEEP_AS_PAID_DEPENDENCY_REQUIRED',status:503};
}
// The actual command parent previously checked bytes only after arrayBuffer.
// Preserve its empty-body and parser semantics while bounding retained bytes.
export async function readWeeklySourceBoundedText(req,maximumBytes){
 let reader,done=false;
 try{
  if(!Number.isSafeInteger(maximumBytes)||maximumBytes<0)fail('WEEKLY_SOURCE_REQUEST_INVALID');
  const length=req.headers.get('content-length');
  if(length!==null&&(!/^\d+$/.test(length)||Number(length)>maximumBytes))fail('WEEKLY_SOURCE_REQUEST_TOO_LARGE',413);
  if(req.body===null){done=true;return '';}
  reader=req.body.getReader();const decoder=new TextDecoder('utf-8',{fatal:true}),parts=[];let bytes=0;
  for(;;){const part=await reader.read();if(part.done)break;
   if(!(part.value instanceof Uint8Array))fail('WEEKLY_SOURCE_REQUEST_INVALID');
   if(part.value.byteLength>maximumBytes-bytes)fail('WEEKLY_SOURCE_REQUEST_TOO_LARGE',413);
   bytes+=part.value.byteLength;parts.push(decoder.decode(part.value,{stream:true}));
  }
  parts.push(decoder.decode());done=true;return parts.join('');
 }catch(error){if(error?.code==='WEEKLY_SOURCE_REQUEST_TOO_LARGE')throw error;fail('WEEKLY_SOURCE_REQUEST_INVALID');}
 finally{if(reader){if(!done)try{await reader.cancel();}catch{}reader.releaseLock();}}
}
