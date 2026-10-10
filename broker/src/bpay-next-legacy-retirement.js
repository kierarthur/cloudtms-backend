/** F-13: exact old transport schemas and verified receipt-before-ACK support.
 * Existing completed native owners are evidence; no legacy writer is invoked.
 * Unknown/nonterminal/missing/overflow scopes remain retained for reconciliation.
 * No raw payload, lease, bank/provider data, new certificate or digest is stored.
 */
const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const LEGACY_UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const ENCODER=new TextEncoder();
const REASON='BPAY_NEXT_LEGACY_ENVELOPE_RETIRED';
const OP_KEYS=['version','operation_id','operation_type','pay_batch_id','root_operation_id','successor_relation','phase','run_after_utc','source','enqueued_at_utc'];
const CERT_KEYS=['message_type','contract_version','continuation_sequence','session_id','actor_user_id','certificate_uuid','lease_owner','enqueued_at_utc'];
const WAKE_KEYS=['message_type','contract_version','wake_depth','wake_id','wake_reason','origin','scope_kind','requested_profile','previous_drain_invocation_id','session_id','candidate_id','actor_user_id','enqueued_at_utc'];
const CERT_STATUS_KEYS=['ok','workbench_session_id','session_version','progress_counter_version','authority_fence_generation','selected_constituent_count','session_current_ready','certificate_lifecycle','certificate_uuid','certificate_contract','certification_id','overall_digest_sha256','certificate_ready_for_draft','certificate_build_pending','certificate_continue_polling','certificate_recovery_required','certificate_error_code','certificate_error_message','maximum_selected_constituents','no_candidate_locks','read_only'];
const OP_FIELDS=['id','operation_type','status','runner_state','requires_user_action','pay_batch_id','root_operation_id','workbench_session_id','completed_at_utc','lease_owner','locked_by','scope_freeze_status','source_scope_seed_complete','frozen_candidate_scope_count','frozen_selected_row_count','frozen_source_session_version','frozen_source_snapshot_run_id'];
const AUDIT_FIELDS=['id','object_type','object_id_text','action','reason','correlation_id','after_json'];
const record=v=>v!==null&&typeof v==='object'&&!Array.isArray(v)&&[Object.prototype,null].includes(Object.getPrototypeOf(v));
function exact(v,keys){
  if(!record(v))return false;
  const own=Reflect.ownKeys(v);
  return own.length===keys.length&&own.every(k=>typeof k==='string'&&keys.includes(k)
    &&Object.getOwnPropertyDescriptor(v,k)?.enumerable===true
    &&Object.hasOwn(Object.getOwnPropertyDescriptor(v,k),'value'));
}
const uuid=v=>typeof v==='string'&&UUID.test(v);
const legacyUuid=v=>typeof v==='string'&&LEGACY_UUID.test(v);
const sameId=(a,b)=>uuid(a)&&uuid(b)&&a.toLowerCase()===b.toLowerCase();
const optionalUuid=v=>v===null||legacyUuid(v);
const text=(v,max)=>typeof v==='string'&&v.length>0&&v===v.trim()&&ENCODER.encode(v).byteLength<=max&&!/[\u0000-\u001f\u007f]/.test(v);
function timestamp(v,producer=false){
  if(typeof v!=='string'||!(producer?/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d\.\d{3}Z$/:/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/).test(v))return false;
  const d=new Date(v.slice(0,10)+'T00:00:00Z');
  return !v.startsWith('0000')&&Number.isFinite(Date.parse(v))&&Number.isFinite(d.getTime())&&d.toISOString().slice(0,10)===v.slice(0,10);
}
function equal(a,b){
  if(a===b)return true;
  if(Array.isArray(a))return Array.isArray(b)&&a.length===b.length&&a.every((v,i)=>equal(v,b[i]));
  return record(a)&&record(b)&&Object.keys(a).length===Object.keys(b).length&&Object.keys(a).every(k=>Object.hasOwn(b,k)&&equal(a[k],b[k]));
}
/** NEXT schemas stay owned by the current metadata/financial adapters. */
export function classifyBpayNextContinuation(body,sourcePolicy){
  try{
    if(!record(body)||Reflect.ownKeys(body).some(k=>typeof k!=='string'
      ||Object.getOwnPropertyDescriptor(body,k)?.enumerable!==true
      ||!Object.hasOwn(Object.getOwnPropertyDescriptor(body,k),'value'))
      ||ENCODER.encode(JSON.stringify(body)).byteLength>2048)return 'UNKNOWN';
    if(Object.hasOwn(body,'runtime_action')||Object.hasOwn(body,'action'))return 'NEXT_CANDIDATE';
    if(exact(body,OP_KEYS)){
      const policy=typeof sourcePolicy==='function'?sourcePolicy(body.source):null;
      if(body.version!==1||!legacyUuid(body.operation_id)||!optionalUuid(body.pay_batch_id)||!optionalUuid(body.root_operation_id)
        ||!text(body.source,160)||!policy?.types?.includes(body.operation_type)||!policy.relations.includes(body.successor_relation)
        ||(body.phase!==null&&(typeof body.phase!=='string'||! /^[A-Z][A-Z0-9_]{0,79}$/.test(body.phase)))
        ||(body.run_after_utc!==null&&(!timestamp(body.run_after_utc,true)||body.successor_relation!=='SELF'))
        ||(body.successor_relation==='CHILD'&&(!['PAYMENT_SETTLEMENT','REMITTANCE_QUEUE'].includes(body.operation_type)||body.root_operation_id===null))
        ||(body.successor_relation==='ROOT'&&!sameId(body.root_operation_id,body.operation_id))
        ||!timestamp(body.enqueued_at_utc,true))return 'UNKNOWN';
      return 'LEGACY_OPERATION_V1';
    }
    if(exact(body,CERT_KEYS)&&body.message_type==='WORKBENCH_SETTLED_CERTIFICATE_CONTINUATION_V8'
      &&body.contract_version===1&&Number.isInteger(body.continuation_sequence)&&body.continuation_sequence>=1&&body.continuation_sequence<=1024
      &&['session_id','actor_user_id','certificate_uuid'].every(k=>legacyUuid(body[k]))
      &&text(body.lease_owner,512)&&timestamp(body.enqueued_at_utc,true))return 'LEGACY_CERTIFICATE_V8';
    if(exact(body,WAKE_KEYS)&&body.message_type==='BANKING_PAY_WORKBENCH_DRAIN_WAKE_V1'&&body.contract_version===1
      &&Number.isInteger(body.wake_depth)&&body.wake_depth>=1&&body.wake_depth<=32&&legacyUuid(body.wake_id)
      &&['PRE_ENTRY_COMMITTED_WORK','POST_PASS_MORE_DUE','LEGACY_MORE_DUE'].includes(body.wake_reason)
      &&['origin','previous_drain_invocation_id'].every(k=>body[k]===null||text(body[k],160))
      &&['session_id','candidate_id','actor_user_id'].every(k=>optionalUuid(body[k]))&&body.requested_profile==='NUDGE'
      &&((body.scope_kind==='GLOBAL'&&body.session_id===null&&body.candidate_id===null)
        ||(body.scope_kind==='SESSION'&&body.session_id!==null&&body.candidate_id===null)
        ||(body.scope_kind==='CANDIDATE'&&body.candidate_id!==null))&&timestamp(body.enqueued_at_utc,true))return 'LEGACY_WORKBENCH_WAKE_V1';
  }catch{}
  return 'UNKNOWN';
}
function auditId(id){
  // Preserve all 128 native ID bits; no truncation, generated digest or upsert.
  if(typeof id!=='string'||!(/^[0-9a-f]{32}$/i.test(id)||UUID.test(id)))return null;
  const hex=id.replaceAll('-','').toLowerCase();
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
}
async function boundedJson(response,signal){
  if(!(response instanceof Response)||!response.ok||!response.body)throw Error('BPAY_NEXT_RETIREMENT_DEPENDENCY_UNAVAILABLE');
  const length=response.headers.get('content-length');
  if(length!==null&&(!/^\d+$/.test(length)||Number(length)>65536)){await response.body.cancel();throw Error('BPAY_NEXT_RETIREMENT_RESPONSE_INVALID');}
  const reader=response.body.getReader(),decoder=new TextDecoder('utf-8',{fatal:true});let size=0,parts=[];
  const cancel=()=>{void reader.cancel().catch(()=>{});};signal.addEventListener('abort',cancel,{once:true});
  try{
    if(signal.aborted)throw Error('BPAY_NEXT_RETIREMENT_DEPENDENCY_UNAVAILABLE');
    while(true){const {done,value}=await reader.read();if(done)break;size+=value.byteLength;if(size>65536)throw Error('BPAY_NEXT_RETIREMENT_RESPONSE_INVALID');parts.push(decoder.decode(value,{stream:true}));}
    parts.push(decoder.decode());return JSON.parse(parts.join(''));
  }finally{signal.removeEventListener('abort',cancel);await reader.cancel().catch(()=>{});reader.releaseLock();}
}
/** Fixed service-only native reads and audit INSERT/GET. No business writer. */
export function createBpayNextLegacyRetirement(config){
  const base=new URL(config.baseUrl),headers=new Headers(config.headers),fetchImpl=config.fetchImpl??fetch;
  if(base.username||base.password||base.search||base.hash||typeof fetchImpl!=='function'
    ||(base.protocol!=='https:'&&!(base.protocol==='http:'&&['localhost','127.0.0.1','[::1]'].includes(base.hostname)))
    ||!/^Bearer [A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(headers.get('authorization')??''))throw TypeError('BPAY_NEXT_RETIREMENT_TRANSPORT_INVALID');
  try{
    const encoded=headers.get('authorization').slice(7).split('.')[1].replace(/-/g,'+').replace(/_/g,'/');
    const claims=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(Uint8Array.from(atob(encoded),c=>c.charCodeAt(0))));
    if(!record(claims)||claims.role!=='service_role')throw Error();
  }catch{throw TypeError('BPAY_NEXT_RETIREMENT_TRANSPORT_INVALID');}
  headers.set('content-type','application/json');headers.set('accept','application/json');
  const endpoint=base.href.replace(/\/+$/,'')+'/rest/v1/';
  return async function retire(message,family,ownerEpoch){
    const id=auditId(message.id),body=message.body;
    if(!id||!['LEGACY_OPERATION_V1','LEGACY_CERTIFICATE_V8','LEGACY_WORKBENCH_WAKE_V1'].includes(family))return {retired:false,reason:'NATIVE_ID_OR_SCHEMA_UNAVAILABLE'};
    const controller=new AbortController();let timer;
    async function read(path,args){
      const response=await fetchImpl(endpoint+path,{method:args?'POST':'GET',headers:new Headers(headers),
        ...(args?{body:JSON.stringify(args)}:{}),signal:controller.signal,redirect:'error'});
      return boundedJson(response,controller.signal);
    }
    async function qualify(){
      if(family==='LEGACY_CERTIFICATE_V8'){
        const s=await read('rpc/pay_workbench_settled_certificate_status_v8',{p_workbench_session_id:body.session_id,p_actor_user_id:body.actor_user_id});
        if(!exact(s,CERT_STATUS_KEYS)||s.ok!==true||!sameId(s.workbench_session_id,body.session_id)||!sameId(s.certificate_uuid,body.certificate_uuid)
          ||s.certificate_lifecycle!=='SEALED_CURRENT'||s.session_current_ready!==true||s.certificate_ready_for_draft!==true
          ||s.certificate_build_pending!==false||s.certificate_continue_polling!==false||s.certificate_recovery_required!==false
          ||s.certificate_contract!=='WORKBENCH_SETTLED_CERTIFICATION_V2'||!/^WORKBENCH_SETTLED_CERTIFICATION_V2:[0-9a-f]{64}$/.test(s.certification_id??'')
          ||! /^[0-9a-f]{64}$/.test(s.overall_digest_sha256??'')||s.read_only!==true||s.no_candidate_locks!==true
          ||!['session_version','progress_counter_version','authority_fence_generation'].every(k=>Number.isSafeInteger(s[k])&&s[k]>=1)
          ||!Number.isInteger(s.selected_constituent_count)||s.selected_constituent_count<1||s.selected_constituent_count>50000
          ||s.maximum_selected_constituents!==50000||s.certificate_error_code!==null||s.certificate_error_message!==null)return null;
        return {disposition:'CERTIFICATE_BUILD_COMPLETE',session_id:body.session_id.toLowerCase(),actor_user_id:body.actor_user_id.toLowerCase(),
          certificate_uuid:body.certificate_uuid.toLowerCase(),continuation_sequence:body.continuation_sequence};
      }
      if(family==='LEGACY_OPERATION_V1'&&body.operation_type==='DRAFT_CREATE'&&body.successor_relation==='SELF'){
        const rows=await read(`banking_pay_operations?id=eq.${body.operation_id}&select=${OP_FIELDS.join(',')}&limit=2`);
        if(!Array.isArray(rows)||rows.length!==1||!exact(rows[0],OP_FIELDS))return null;
        const o=rows[0];
        if(!sameId(o.id,body.operation_id)||o.operation_type!=='DRAFT_CREATE'||o.status!=='COMPLETE'||o.runner_state!=='COMPLETE'
          ||o.requires_user_action!==false||!timestamp(o.completed_at_utc)
          ||o.lease_owner!==null||o.locked_by!==null||!uuid(o.workbench_session_id)
          ||!(o.pay_batch_id===null?body.pay_batch_id===null:sameId(o.pay_batch_id,body.pay_batch_id))
          ||!(o.root_operation_id===null?body.root_operation_id===null:sameId(o.root_operation_id,body.root_operation_id))
          ||o.scope_freeze_status!=='FROZEN'||o.source_scope_seed_complete!==true||!uuid(o.frozen_source_snapshot_run_id)
          ||!Number.isSafeInteger(o.frozen_source_session_version)||o.frozen_source_session_version<1
          ||!Number.isInteger(o.frozen_selected_row_count)||o.frozen_selected_row_count<1
          ||!Number.isInteger(o.frozen_candidate_scope_count)||o.frozen_candidate_scope_count<1||o.frozen_candidate_scope_count>100)return null;
        const scopes=await read(`banking_pay_operation_candidate_scope?operation_id=eq.${body.operation_id}&select=id,operation_id,workbench_session_id,pay_batch_id,source_session_version,source_snapshot_run_id,status&order=id&limit=101`);
        if(!Array.isArray(scopes)||scopes.length!==o.frozen_candidate_scope_count||scopes.length>100
          ||new Set(scopes.map(s=>s.id)).size!==scopes.length
          ||scopes.some(s=>!exact(s,['id','operation_id','workbench_session_id','pay_batch_id','source_session_version','source_snapshot_run_id','status'])||!uuid(s.id)
            ||!sameId(s.operation_id,o.id)||!sameId(s.workbench_session_id,o.workbench_session_id)||!uuid(s.pay_batch_id)
            ||s.source_session_version!==o.frozen_source_session_version||!sameId(s.source_snapshot_run_id,o.frozen_source_snapshot_run_id)
            ||!['ALLOCATED','DRAFTED'].includes(s.status)))return null;
        const batches=[...new Set(scopes.map(s=>s.pay_batch_id.toLowerCase()))].sort();
        if(o.pay_batch_id!==null&&!batches.includes(o.pay_batch_id.toLowerCase()))return null;
        const census=await read('rpc/bpay_next_cutover_readiness_page_v1',{p_request:{section:'LEGACY',category:'LEGACY_BATCHES',after_id:null,limit:100}});
        if(!record(census)||census.active_owner!=='NEXT'||census.owner_epoch!==ownerEpoch||census.section!=='LEGACY'||census.category!=='LEGACY_BATCHES'
          ||!Array.isArray(census.categories)||census.categories.length!==1||census.categories[0]?.category!=='LEGACY_BATCHES'
          ||!Array.isArray(census.categories[0].items)||census.categories[0].items.length>100)return null;
        const items=census.categories[0].items;
        if(!batches.every(batch=>items.filter(i=>sameId(i.id,batch)&&i.classification==='OBSOLETE_BOOKKEEPING'&&i.blocking===false).length===1))return null;
        return {disposition:'COMPLETED_UNUSED_DRAFT',operation_id:o.id.toLowerCase(),session_id:o.workbench_session_id.toLowerCase(),batch_ids:batches};
      }
      // No exact existing native scope closure was established for these wakes.
      // This is retention, never a guessed empty/global or terminal money ACK.
      return null;
    }
    async function run(){
      const disposition=await qualify();
      if(disposition===null)return {retired:false,reason:'NATIVE_DISPOSITION_UNPROVEN'};
      const packet={message_id:message.id,family,...disposition};
      const expected={id,object_type:'bpay_next_legacy_envelope_retirement',object_id_text:message.id,
        action:REASON,reason:disposition.disposition,correlation_id:message.id,after_json:packet};
      if(ENCODER.encode(JSON.stringify(expected)).byteLength>8192)return {retired:false,reason:'RETIREMENT_RECEIPT_TOO_LARGE'};
      const path=`audit_events?id=eq.${id}&select=${AUDIT_FIELDS.join(',')}&limit=2`;
      function verify(rows){return Array.isArray(rows)&&rows.length===1&&exact(rows[0],AUDIT_FIELDS)&&equal(rows[0],expected);}
      const existing=await read(path);
      if(verify(existing))return {retired:true,created:false,receipt_id:id,reason:REASON};
      if(!Array.isArray(existing)||existing.length!==0)return {retired:false,reason:'RETIREMENT_RECEIPT_CONFLICT'};
      const postHeaders=new Headers(headers);postHeaders.set('prefer','return=minimal');
      const response=await fetchImpl(endpoint+'audit_events',{method:'POST',headers:postHeaders,
        body:JSON.stringify({...expected,actor_user_id:null,actor_display:'Banking Pay NEXT',actor_role_at_time:'system'}),
        signal:controller.signal,redirect:'error'});
      const created=response instanceof Response&&response.ok;
      const conflict=response instanceof Response&&response.status===409;
      await response?.body?.cancel().catch(()=>{});
      if(!created&&!conflict)throw Error('BPAY_NEXT_RETIREMENT_RECEIPT_UNAVAILABLE');
      if(!verify(await read(path)))return {retired:false,reason:'RETIREMENT_RECEIPT_UNVERIFIED'};
      return {retired:true,created,receipt_id:id,reason:REASON};
    }
    try{
      const deadline=new Promise((resolve,reject)=>{timer=setTimeout(()=>{controller.abort();reject(Error('BPAY_NEXT_RETIREMENT_DEPENDENCY_UNAVAILABLE'));},8000);});
      return await Promise.race([run(),deadline]);
    }finally{clearTimeout(timer);controller.abort();}
  };
}
