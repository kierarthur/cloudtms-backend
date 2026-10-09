/**
 * INTERNAL single-message financial continuation consumer. No enrollment,
 * discovery, Office route, binding/configuration, batch drain or provider call.
 * The trusted queue producer supplies {version:1,...exact financial job input}.
 * SQL remains the authority for kind, Candidate order, nonce, epoch and cursor;
 * Policy X: no live Timesheet/finance/history fallback or fabricated money.
 *
 * sendContinuation must resolve ONLY after durable queue publication. It must
 * preserve this exact JSON envelope and must not log lease authority. Awaiting
 * it before ack preserves the new CLAIM token and each next-page checkpoint.
 * Publication can succeed and its receipt be lost: duplicate continuations are
 * expected; the existing owner replay rules, not queue IDs, prevent duplication.
 *
 * onFailure receives a closed, token-free summary. For definite permanent
 * failures it may return ACK_RECORDED only AFTER durable, idempotent failure
 * recording/recovery handoff (keyed by message_id/job_id/action). That ACK is a
 * failed queue delivery disposition, NOT database job completion. Anything
 * else, including a missing/failed callback, retries the original message.
 *
 * Retry delays are polling backoff, never evidence a server lease has expired.
 * A lost CLAIM/send retries CLAIM without adopting its possibly live nonce.
 * A lost STEP/send retries the SAME original lease/epoch/cursor/limit. No
 * re-claim, automatic second owner call, client-clock lease test or drain loop.
 * Integration must configure durable DLQ/recovery for exhausted retries; this
 * module does not promise eventual completion or silently drop failed work.
 */

import { stepBpayNextFinancialJob, BPAY_NEXT_JOB_LIMITS } from './bpay-next-job-step.js';

export const BPAY_NEXT_FINANCIAL_QUEUE_LIMITS = Object.freeze({
  envelopeBytes: BPAY_NEXT_JOB_LIMITS.requestBytes,
  pageRows: BPAY_NEXT_JOB_LIMITS.pageRows,
  retrySeconds: 30,
  claimWaitSeconds: 120
});

/** @typedef {import('./bpay-next-job-step.js').FinancialJobClaim|import('./bpay-next-job-step.js').FinancialJobSingleStep|import('./bpay-next-job-step.js').FinancialJobPageStep|import('./bpay-next-job-step.js').FinancialJobPositionStep|import('./bpay-next-job-step.js').FinancialJobPinStep|import('./bpay-next-job-step.js').FinancialJobPreparePageStep|import('./bpay-next-job-step.js').FinancialJobPrepareCompleteStep|import('./bpay-next-job-step.js').FinancialJobCaseNetPageStep} FinancialCall */
/** @typedef {FinancialCall & {version:1}} FinancialQueueEnvelope */
/** @typedef {{id:string,attempts:number,body:unknown,ack:()=>void,retry:(options:{delaySeconds:number})=>void}} FinancialQueueMessage */
/** @typedef {{message_id:string|null,attempts:number|null,action:'CLAIM'|'STEP'|null,job_id:string|null,job_kind:string|null,error_code:string,retryable:boolean,outcome_unknown:boolean,recovery:string}} FinancialQueueFailure */
/** @typedef {import('./bpay-next-job-step.js').FinancialJobDependencies & {sendContinuation?:(envelope:Readonly<FinancialQueueEnvelope>)=>Promise<unknown>,onFailure?:(failure:Readonly<FinancialQueueFailure>)=>Promise<'ACK_RECORDED'|'RETRY'>}} FinancialQueueDependencies */

const KINDS = ['PAYE_NET_ENTRY', 'TRANSFER_BUILD', 'CSV_SETTLEMENT', 'CSV_RETURN', 'CASH_REISSUE', 'SIMPLE_CANCEL', 'POSITION_APPLY', 'PREPARE', 'REISSUE_CANCEL', 'INTERNAL_SETTLEMENT', 'PREPARATION_EXPIRY', 'CASE_CREATE', 'CASE_WRITE_OFF'];
const PAGED = ['TRANSFER_BUILD', 'CSV_SETTLEMENT', 'INTERNAL_SETTLEMENT', 'SIMPLE_CANCEL', 'PREPARATION_EXPIRY'];
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const KEYS = ['version', 'action', 'job_id', 'job_kind', 'lease_seconds', 'lease_nonce', 'owner_epoch', 'expected_cursor', 'limit',
  'expected_phase', 'expected_work_cursor', 'expected_component_cursor'];
const ENCODER = new TextEncoder();
const errorResult = (error_code, retryable = false, outcome_unknown = false, recovery = 'NONE') =>
  ({ ok: false, error_code, retryable, outcome_unknown, recovery });

function envelopeInput(body) {
  if (body === null || typeof body !== 'object' || Array.isArray(body)
    || ![Object.prototype, null].includes(Object.getPrototypeOf(body))
    || !Object.hasOwn(body, 'version')) return null;
  const keys = Reflect.ownKeys(body);
  if (keys.length > 10 || keys.some((key) => typeof key !== 'string' || !KEYS.includes(key)
    || !Object.getOwnPropertyDescriptor(body, key)?.enumerable
    || !Object.hasOwn(Object.getOwnPropertyDescriptor(body, key), 'value'))) return null;
  if (body.version !== 1) return null;
  if (keys.some((key) => body[key] !== null && !['string', 'number'].includes(typeof body[key])
    || (typeof body[key] === 'string' && body[key].length >
      ((body.job_kind === 'POSITION_APPLY' && key === 'expected_cursor')
        || (body.job_kind === 'PREPARE' && key === 'expected_component_cursor') ? 512 : 128)))) return null;
  if (ENCODER.encode(JSON.stringify(body)).byteLength > BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.envelopeBytes) return null;
  // The existing adapter validates the exact CLAIM/kind-specific STEP schema
  // before any RPC. Do not create an alternate financial input validator here.
  return Object.freeze(Object.fromEntries(keys.filter((key) => key !== 'version').map((key) => [key, body[key]])));
}

function failureSummary(message, call, result) {
  return Object.freeze({
    message_id: typeof message.id === 'string' && message.id.length <= 128 ? message.id : null,
    attempts: Number.isSafeInteger(message.attempts) && message.attempts > 0 ? message.attempts : null,
    action: ['CLAIM', 'STEP'].includes(call?.action) ? call.action : null,
    job_id: typeof call?.job_id === 'string' && UUID.test(call.job_id) ? call.job_id : null,
    job_kind: KINDS.includes(call?.job_kind) ? call.job_kind : null,
    error_code: result.error_code, retryable: result.retryable,
    outcome_unknown: result.outcome_unknown, recovery: result.recovery
  });
}

function retry(message, result) {
  message.retry({ delaySeconds: result.recovery === 'WAIT_FOR_LEASE_EXPIRY'
    ? BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.claimWaitSeconds
    : BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.retrySeconds });
  return { disposition: 'RETRY', error_code: result.error_code,
    outcome_unknown: result.outcome_unknown, recovery: result.recovery };
}

async function failure(message, call, result, dependencies) {
  if (result.retryable || result.outcome_unknown) return retry(message, result);
  if (typeof dependencies.onFailure !== 'function') return retry(message, result);
  try {
    if (await dependencies.onFailure(failureSummary(message, call, result)) === 'ACK_RECORDED') {
      message.ack();
      return { disposition: 'ACK', reason: 'FAILURE_RECORDED', error_code: result.error_code };
    }
  } catch {
    return retry(message, errorResult('BPAY_NEXT_QUEUE_FAILURE_RECORD_UNAVAILABLE', true, false, 'RETRY_CALL'));
  }
  return retry(message, result);
}

function continuation(call, result) {
  if (call.job_kind === 'PAYE_NET_ENTRY' && (result.phase === 'FINANCE_ALLOCATE'
    || call.expected_phase === 'FINANCE_ALLOCATE')) {
    if (result.phase !== 'FINANCE_ALLOCATE' || (call.action === 'STEP' && (result.cursor === null
      || (call.expected_cursor !== null && BigInt(result.cursor) <= BigInt(call.expected_cursor))))) return null;
    return Object.freeze({ version: 1, action: 'STEP', job_id: call.job_id, job_kind: call.job_kind,
      lease_nonce: call.action === 'CLAIM' ? result.lease_nonce : call.lease_nonce,
      owner_epoch: call.action === 'CLAIM' ? result.owner_epoch : call.owner_epoch,
      expected_phase: 'FINANCE_ALLOCATE', expected_cursor: result.cursor,
      limit: call.limit ?? BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.pageRows });
  }
  if (call.job_kind === 'PREPARE') {
    const next = prepareContinuation(call, result);
    // Compare exact opaque state, not JS lexical ordering: PostgreSQL owns
    // component-key collation and the complete phase-specific cursor tuple.
    if (call.action === 'STEP' && call.expected_phase === result.phase
      && (['NEW', 'PINNING'].includes(result.phase) ? call.expected_cursor === result.cursor
        : ['RESERVE', 'ALLOCATE'].includes(result.phase) ? call.expected_work_cursor === result.work_cursor
          && call.expected_component_cursor === result.component_cursor
          : result.phase === 'FINANCE_ALLOCATE' ? result.cursor === null
            || (call.expected_cursor !== null && BigInt(result.cursor) <= BigInt(call.expected_cursor)) : true)) return null;
    return next;
  }
  if (call.job_kind === 'POSITION_APPLY') {
    if (call.action === 'STEP' && call.expected_phase === result.phase && call.expected_cursor === result.cursor) return null;
    return Object.freeze({ version: 1, action: 'STEP', job_id: call.job_id, job_kind: call.job_kind,
      lease_nonce: call.action === 'CLAIM' ? result.lease_nonce : call.lease_nonce,
      owner_epoch: call.action === 'CLAIM' ? result.owner_epoch : call.owner_epoch,
      expected_phase: result.phase, expected_cursor: result.cursor,
      limit: call.limit ?? BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.pageRows });
  }
  if (call.action === 'CLAIM') {
    return Object.freeze({ version: 1, action: 'STEP', job_id: call.job_id, job_kind: call.job_kind,
      lease_nonce: result.lease_nonce, owner_epoch: result.owner_epoch,
      ...(PAGED.includes(call.job_kind) ? { expected_cursor: result.cursor,
        limit: BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.pageRows } : {}) });
  }
  // An unfinished page must yield a strictly later checkpoint, including
  // owner replay returning an already-advanced current cursor. Never create
  // an infinite same-cursor continuation from a semantically bad receipt.
  if (!PAGED.includes(call.job_kind) || result.cursor === null
    || (call.expected_cursor !== null && BigInt(result.cursor) <= BigInt(call.expected_cursor))) return null;
  return Object.freeze({ version: 1, ...call, expected_cursor: result.cursor });
}

function prepareContinuation(call, result) {
  return Object.freeze({ version: 1, action: 'STEP', job_id: call.job_id, job_kind: 'PREPARE',
    lease_nonce: call.action === 'CLAIM' ? result.lease_nonce : call.lease_nonce,
    owner_epoch: call.action === 'CLAIM' ? result.owner_epoch : call.owner_epoch,
    expected_phase: result.phase,
    ...(['NEW', 'PINNING'].includes(result.phase) ? { expected_cursor: result.cursor }
      : ['RESERVE', 'ALLOCATE'].includes(result.phase) ? { expected_work_cursor: result.work_cursor,
        expected_component_cursor: result.component_cursor,
        limit: call.limit ?? BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.pageRows }
        : result.phase === 'FINANCE_ALLOCATE' ? { expected_cursor: result.cursor,
          limit: call.limit ?? BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.pageRows } : {}) });
}

/**
 * The eventual Worker queue handler must await this once for each Message.
 * This function makes at most one owner-adapter call and publishes at most one
 * continuation. It invokes ack/retry as message methods, preserving their this.
 * @param {FinancialQueueMessage} message
 * @param {FinancialQueueDependencies} dependencies
 * @returns {Promise<Object>} Token-free queue disposition, not a job receipt.
 */
export async function handleBpayNextFinancialQueueMessage(message, dependencies = {}) {
  if (!message || typeof message.ack !== 'function' || typeof message.retry !== 'function') {
    // Never return successfully to a Worker with an un-disposable Message:
    // successful queue-handler return would implicitly acknowledge its batch.
    throw new TypeError('BPAY_NEXT_QUEUE_MESSAGE_INTERFACE_INVALID');
  }
  let call;
  try { call = envelopeInput(message.body); } catch { call = null; }
  if (!call) return failure(message, null, errorResult('BPAY_NEXT_QUEUE_ENVELOPE_INVALID'), dependencies);
  if (typeof dependencies.financialJobRpc !== 'function'
    || typeof dependencies.sendContinuation !== 'function'
    || typeof dependencies.onFailure !== 'function') {
    return retry(message, errorResult('BPAY_NEXT_QUEUE_DEPENDENCY_UNAVAILABLE', true, false, 'RETRY_CALL'));
  }
  const outcome = await stepBpayNextFinancialJob(call, { financialJobRpc: dependencies.financialJobRpc });
  if (!outcome.ok) {
    if (call.action === 'CLAIM' && outcome.error_code === 'BPAY_NEXT_JOB_TERMINAL') {
      message.ack();
      return { disposition: 'ACK', reason: 'TERMINAL' };
    }
    return failure(message, call, outcome, dependencies);
  }
  if (call.action === 'STEP' && outcome.result.done) {
    message.ack();
    return { disposition: 'ACK', reason: 'DONE' };
  }
  const next = continuation(call, outcome.result);
  if (!next) return retry(message, errorResult('BPAY_NEXT_QUEUE_CONTINUATION_INVALID', true, true, 'RETRY_SAME_STEP'));
  try {
    await dependencies.sendContinuation(next);
  } catch {
    return retry(message, errorResult('BPAY_NEXT_QUEUE_CONTINUATION_SEND_UNKNOWN', true, true,
      call.action === 'CLAIM' ? 'WAIT_FOR_LEASE_EXPIRY' : 'RETRY_SAME_STEP'));
  }
  message.ack();
  return { disposition: 'ACK', reason: 'CONTINUATION_PERSISTED' };
}
