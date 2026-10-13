// Fixed read-only transport; authenticated actor is supplied by the broker.
// Return the raw Response so the page handler bounds bytes before JSON parse.
const KINDS = new Set(['SHIFTS', 'BREAKS', 'RATE_DETAILS', 'RATE_SCHEDULE']);
export function createBpayNextFrozenPageRpc({ baseUrl, headers, fetchImpl = fetch }) {
  const endpoint = new URL(`${String(baseUrl || '').replace(/\/+$/, '')}/rest/v1/rpc/bpay_next_frozen_detail_page_v1`);
  if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password) {
    throw new TypeError('BPAY_NEXT_FROZEN_TRANSPORT_INVALID');
  }
  return async (kind, args, options) => {
    if (!KINDS.has(kind) || !options?.actorUserId || !options.signal) {
      throw new TypeError('BPAY_NEXT_FROZEN_TRANSPORT_INVALID');
    }
    const requestHeaders = new Headers(headers);
    requestHeaders.set('content-type', 'application/json');
    requestHeaders.set('accept', 'application/json');
    requestHeaders.set('X-CloudTMS-Banking-Pay-Route-Class', 'PREVIEW_PAGE');
    return fetchImpl(endpoint.href, {
      method: 'POST',
      headers: requestHeaders,
      body: JSON.stringify({ ...args, p_kind: kind, p_actor_user_id: options.actorUserId }),
      signal: options.signal,
      redirect: 'error'
    });
  };
}
