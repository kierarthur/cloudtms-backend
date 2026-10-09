// Fixed raw transports only; the handler owns bounds/deadline/closed decoding.
import {BPAY_NEXT_PREPARATION_INPUTS,BPAY_NEXT_PREPARATION_PAGE_INPUTS} from './bpay-next-preparation.js';
function create({baseUrl,headers,fetchImpl=fetch},page){
  const endpoint=new URL(`${String(baseUrl||'').replace(/\/+$/,'')}/rest/v1/rpc/${page?'bpay_next_preparation_page_v1':'bpay_next_preparation_intake_v1'}`);
  if(!['http:','https:'].includes(endpoint.protocol)||endpoint.username||endpoint.password)throw new TypeError('BPAY_NEXT_PREPARATION_TRANSPORT_INVALID');
  return async(name,args,options)=>{
    if(!Object.hasOwn(page?BPAY_NEXT_PREPARATION_PAGE_INPUTS:BPAY_NEXT_PREPARATION_INPUTS,name)
      ||!options?.actorUserId||!options.signal)throw new TypeError('BPAY_NEXT_PREPARATION_TRANSPORT_INVALID');
    const requestHeaders=new Headers(headers);requestHeaders.set('content-type','application/json');requestHeaders.set('accept','application/json');
    requestHeaders.set('X-CloudTMS-Banking-Pay-Route-Class',page?'READ':'OPERATION_NUDGE');
    return fetchImpl(endpoint.href,{method:'POST',headers:requestHeaders,redirect:'error',signal:options.signal,
      body:JSON.stringify({p_actor_user_id:options.actorUserId,[page?'p_kind':'p_action']:name,p_args:args})});
  };
}
export const createBpayNextPreparationRpc=options=>create(options,false);
export const createBpayNextPreparationPageRpc=options=>create(options,true);
