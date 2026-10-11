// Fixed service-only transport. No Office endpoint, discovery, retry or body
// buffering. The job adapter bounds bytes/deadline and reports uncertainty.
const KINDS=new Set(['PAYE_NET_ENTRY','TRANSFER_BUILD','CSV_SETTLEMENT',
  'CSV_RETURN','CASH_REISSUE','SIMPLE_CANCEL','POSITION_APPLY','PREPARE','REISSUE_CANCEL','INTERNAL_SETTLEMENT','PREPARATION_EXPIRY','PREPARATION_DISCARD','CASE_CREATE','CASE_WRITE_OFF']);
export function createBpayNextFinancialJobRpc({baseUrl,headers,fetchImpl=fetch}) {
  const endpoint=new URL(`${String(baseUrl||'').replace(/\/+$/,'')}/rest/v1/rpc/bpay_next_financial_job_v1`);
  if(!['http:','https:'].includes(endpoint.protocol)||endpoint.username||endpoint.password) {
    throw new TypeError('BPAY_NEXT_JOB_TRANSPORT_INVALID');
  }
  return async(action,jobId,jobKind,args,options)=>{
    if(!['CLAIM','STEP'].includes(action)||!KINDS.has(jobKind)||!jobId||!options?.signal) {
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
