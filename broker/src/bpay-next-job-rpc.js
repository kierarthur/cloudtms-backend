// Fixed service-only transport. No Office endpoint, discovery, retry or body
// buffering. The job adapter bounds bytes/deadline and reports uncertainty.
const KINDS=new Set(['PAYE_NET_ENTRY','TRANSFER_BUILD','CSV_SETTLEMENT',
  'CSV_RETURN','CASH_REISSUE','SIMPLE_CANCEL','POSITION_APPLY','PREPARE','REISSUE_CANCEL','INTERNAL_SETTLEMENT','PREPARATION_EXPIRY','PREPARATION_DISCARD','CASE_CREATE','CASE_EDIT','CASE_RESTRUCTURE','CASE_PAUSE','CASE_RESUME','CASE_WRITE_OFF']);
export function createBpayNextFinancialJobRpc({baseUrl,headers,fetchImpl=fetch}) {
  const endpoint=new URL(`${String(baseUrl||'').replace(/\/+$/,'')}/rest/v1/rpc/bpay_next_financial_job_v1`);
  if(!['http:','https:'].includes(endpoint.protocol)||endpoint.username||endpoint.password) {
    throw new TypeError('BPAY_NEXT_JOB_TRANSPORT_INVALID');
  }
  return async(action,jobId,jobKind,args,options)=>{
    if(!['CLAIM','STEP','FAILURE'].includes(action)||action==='FAILURE'&&jobKind!=='PREPARE'||!KINDS.has(jobKind)||!jobId||!options?.signal) {
      throw new TypeError('BPAY_NEXT_JOB_TRANSPORT_INVALID');
    }
    const requestHeaders=new Headers(headers);
    requestHeaders.set('content-type','application/json');
    requestHeaders.set('accept','application/json');
    requestHeaders.set('X-CloudTMS-Banking-Pay-Route-Class','OPERATION_NUDGE');
    return fetchImpl(endpoint.href,{
      method:'POST',headers:requestHeaders,
      body:JSON.stringify({p_action:action,p_job_id:jobId,p_job_kind:jobKind,p_args:args}),
      signal:options.signal,redirect:'error'
    });
  };
}

// One bounded internal bookkeeping call using the actual STEP invocation.
// A lost response never authorises ACK or adoption of an active lease.
export async function recordBpayNextPrepareFailure(rpc,evidence) {
  if(!evidence||evidence.job_kind!=='PREPARE'||evidence.action!=='STEP')return false;
  const controller=new AbortController();let timer;
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new Error('BPAY_NEXT_QUEUE_FAILURE_RECORD_UNAVAILABLE'));},8000);});
  try{return await Promise.race([(async()=>{
    const {job_id,job_kind,action,...args}=evidence;
    const response=await rpc('FAILURE',job_id,job_kind,args,{signal:controller.signal});
    if(!(response instanceof Response)||!response.ok||!response.body)throw Error('BPAY_NEXT_QUEUE_FAILURE_RECORD_UNAVAILABLE');
    const length=response.headers.get('content-length');if(length!==null&&(!/^\d+$/.test(length)||Number(length)>16384)){await response.body.cancel();throw Error('BPAY_NEXT_QUEUE_FAILURE_RECORD_UNAVAILABLE');}
    const reader=response.body.getReader(),decoder=new TextDecoder('utf-8',{fatal:true});let bytes=0,text='';
    const cancel=()=>void reader.cancel().catch(()=>{});controller.signal.addEventListener('abort',cancel,{once:true});
    try{while(true){if(controller.signal.aborted)throw Error('BPAY_NEXT_QUEUE_FAILURE_RECORD_UNAVAILABLE');const chunk=await reader.read();if(chunk.done)break;bytes+=chunk.value.byteLength;if(bytes>16384)throw Error('BPAY_NEXT_QUEUE_FAILURE_RECORD_UNAVAILABLE');text+=decoder.decode(chunk.value,{stream:true});}text+=decoder.decode();}
    finally{await reader.cancel().catch(()=>{});reader.releaseLock();controller.signal.removeEventListener('abort',cancel);}
    const r=JSON.parse(text),keys=['recorded','job_id','needs_attention','next_retry_at_utc','replay'];
    if(!r||typeof r!=='object'||Object.keys(r).length!==keys.length||keys.some(k=>!Object.hasOwn(r,k))
      ||r.job_id!==job_id||typeof r.recorded!=='boolean'||typeof r.needs_attention!=='boolean'||typeof r.replay!=='boolean'
      ||r.next_retry_at_utc!==null&&(typeof r.next_retry_at_utc!=='string'||!Number.isFinite(Date.parse(r.next_retry_at_utc))))throw Error('BPAY_NEXT_QUEUE_FAILURE_RECORD_UNAVAILABLE');
    return r.recorded;
  })(),deadline]);}finally{clearTimeout(timer);controller.abort();}
}
