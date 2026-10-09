// Two closed wrappers: financial intake and exact original-credit binding.
// No caller-selected RPC/SQL, retry
// or response buffering: the handler owns byte bounds and uncertainty reporting.
const ACTIONS = new Set(['PAYE_NET','CASE_PAYOUT','TRANSFER_BEGIN','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL',
  'CSV_ISSUE','CSV_SETTLEMENT','CSV_RETURN','INTERNAL_SETTLE','PREPARATION_EXPIRE','WRITE_OFF','BIND_STORED_CREDIT']);
const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
export function createBpayNextFinancialCommandRpc({ baseUrl, headers, fetchImpl=fetch }) {
  const endpoint=new URL(`${String(baseUrl || '').replace(/\/+$/, '')}/rest/v1/rpc/bpay_next_financial_intake_v1`);
  const bindEndpoint=new URL(`${String(baseUrl || '').replace(/\/+$/, '')}/rest/v1/rpc/bpay_next_bind_stored_credit_v1`);
  if (!['http:','https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password) {
    throw new TypeError('BPAY_NEXT_COMMAND_TRANSPORT_INVALID');
  }
  return async (action,args,options) => {
    if (!ACTIONS.has(action) || !options?.actorUserId || !options.signal) {
      throw new TypeError('BPAY_NEXT_COMMAND_TRANSPORT_INVALID');
    }
    if(action==='BIND_STORED_CREDIT' && (!args || typeof args!=='object' || Array.isArray(args)
      || Object.keys(args).length!==2 || !Object.hasOwn(args,'command_id') || !Object.hasOwn(args,'legacy_case_id')
      || !UUID.test(args.command_id) || !UUID.test(args.legacy_case_id) || !UUID.test(options.actorUserId))) {
      throw new TypeError('BPAY_NEXT_COMMAND_TRANSPORT_INVALID');
    }
    const requestHeaders=new Headers(headers);
    requestHeaders.set('content-type','application/json');
    requestHeaders.set('accept','application/json');
    requestHeaders.set('X-CloudTMS-Banking-Pay-Route-Class','OPERATION_NUDGE');
    const bind=action==='BIND_STORED_CREDIT';
    return fetchImpl(bind?bindEndpoint.href:endpoint.href,{
      method:'POST',headers:requestHeaders,
      body:JSON.stringify(bind?{p_actor_user_id:options.actorUserId,p_command_id:args.command_id,p_legacy_case_id:args.legacy_case_id}
        :{p_action:action,p_args:args,p_actor_user_id:options.actorUserId}),
      signal:options.signal,redirect:'error'
    });
  };
}
