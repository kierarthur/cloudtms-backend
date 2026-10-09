// One fixed read-only endpoint. No generic/private RPC, writes or retries.
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const uuid = value => typeof value === 'string' && UUID.test(value);
const ordinal = value => value === null || (Number.isInteger(value) && value >= 0 && value <= 2147483647);
const INPUTS = Object.freeze({
  HEADER: [], WORKS: ['p_after_work_id'], MEMBERS: ['p_after_member_no'],
  SHIFTS: ['p_run_line_id', 'p_after_detail_no'],
  BREAKS: ['p_run_line_id', 'p_shift_detail_id', 'p_after_break_no'],
  RATE_DETAILS: ['p_run_line_id', 'p_after_bucket'],
  RATE_SCHEDULE: ['p_run_work_id', 'p_after_rate_family', 'p_after_rate_code']
});

export function createBpayNextFrozenAdvicePageRpc({ baseUrl, headers, fetchImpl = fetch }) {
  const endpoint = new URL(`${String(baseUrl || '').replace(/\/+$/, '')}/rest/v1/rpc/bpay_next_original_csv_advice_page_v1`);
  if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password || typeof fetchImpl !== 'function') {
    throw new TypeError('BPAY_NEXT_ADVICE_TRANSPORT_INVALID');
  }
  return async (kind, args, options) => {
    const allowed = Object.hasOwn(INPUTS, kind) ? INPUTS[kind] : null;
    if (!allowed || !args || typeof args !== 'object' || Array.isArray(args)
      || !uuid(args.p_instruction_id) || !uuid(options?.actorUserId)
      || !(options?.signal instanceof AbortSignal) || !Number.isInteger(args.p_limit)
      || args.p_limit < 1 || args.p_limit > 100
      || Object.keys(args).some(key => !['p_instruction_id', 'p_limit', ...allowed].includes(key))) {
      throw new TypeError('BPAY_NEXT_ADVICE_TRANSPORT_INVALID');
    }
    // Refuse malformed child/cursor input before network, including half tuples.
    if ((kind === 'WORKS' && !(args.p_after_work_id === null || uuid(args.p_after_work_id)))
      || (kind === 'MEMBERS' && !(args.p_after_member_no === null || (typeof args.p_after_member_no === 'string'
        && /^(?:0|[1-9]\d{0,18})$/.test(args.p_after_member_no) && BigInt(args.p_after_member_no) <= 9223372036854775807n)))
      || (['SHIFTS', 'BREAKS', 'RATE_DETAILS'].includes(kind) && !uuid(args.p_run_line_id))
      || (kind === 'SHIFTS' && !ordinal(args.p_after_detail_no))
      || (kind === 'BREAKS' && (!uuid(args.p_shift_detail_id) || !ordinal(args.p_after_break_no)))
      || (kind === 'RATE_DETAILS' && !(args.p_after_bucket === null || ['BH', 'DAY', 'NIGHT', 'SAT', 'SUN'].includes(args.p_after_bucket)))
      || (kind === 'RATE_SCHEDULE' && (!uuid(args.p_run_work_id)
        || !((args.p_after_rate_family === null && args.p_after_rate_code === null)
          || (['ADDITIONAL', 'MILEAGE', 'STANDARD'].includes(args.p_after_rate_family)
            && typeof args.p_after_rate_code === 'string' && Array.from(args.p_after_rate_code).length >= 1
            && Array.from(args.p_after_rate_code).length <= 128))))) {
      throw new TypeError('BPAY_NEXT_ADVICE_TRANSPORT_INVALID');
    }
    const requestHeaders = new Headers(headers);
    requestHeaders.set('content-type', 'application/json');
    requestHeaders.set('accept', 'application/json');
    requestHeaders.set('X-CloudTMS-Banking-Pay-Route-Class', 'PREVIEW_PAGE');
    return fetchImpl(endpoint.href, {
      method: 'POST', headers: requestHeaders,
      body: JSON.stringify({ ...args, p_kind: kind, p_actor_user_id: options.actorUserId }),
      signal: options.signal, redirect: 'error'
    });
  };
}
