/**
 * Native identity wakes only: the database owns every lease, phase and cursor.
 * Each delivery takes a fresh metadata CLAIM and executes one bounded STEP.
 * No active lease is adopted. An unknown reply retries this identity; native
 * expiry and fresh CLAIM recover the committed cursor without repricing.
 * Await continuation publication before ACK. Failure recording remains the
 * original closed, durable callback; no financial or provider drain here.
 * Policy X: accepted inputs and original money owners remain authoritative.
 */

import { stepBpayNextFinancialJob, BPAY_NEXT_JOB_LIMITS } from './bpay-next-job-step.js';

export const BPAY_NEXT_FINANCIAL_QUEUE_LIMITS = Object.freeze({
  envelopeBytes: BPAY_NEXT_JOB_LIMITS.requestBytes,
  pageRows: BPAY_NEXT_JOB_LIMITS.pageRows,
  retrySeconds: 30,
  claimWaitSeconds: 120
});

/** @typedef {import('./bpay-next-job-step.js').FinancialJobClaim|import('./bpay-next-job-step.js').FinancialJobSingleStep|import('./bpay-next-job-step.js').FinancialJobPageStep|import('./bpay-next-job-step.js').FinancialJobPositionStep|import('./bpay-next-job-step.js').FinancialJobPinStep|import('./bpay-next-job-step.js').FinancialJobPreparePageStep|import('./bpay-next-job-step.js').FinancialJobPrepareCompleteStep|import('./bpay-next-job-step.js').FinancialJobCaseNetPageStep} FinancialCall */
/** @typedef {{id:string,kind:string}} FinancialQueueEnvelope */
/** @typedef {{id:string,attempts:number,body:unknown,ack:()=>void,retry:(options:{delaySeconds:number})=>void}} FinancialQueueMessage */
/** @typedef {{message_id:string|null,attempts:number|null,action:'CLAIM'|'STEP'|null,job_id:string|null,job_kind:string|null,error_code:string,retryable:boolean,outcome_unknown:boolean,recovery:string}} FinancialQueueFailure */
/** @typedef {import('./bpay-next-job-step.js').FinancialJobDependencies & {sendContinuation?:(envelope:Readonly<FinancialQueueEnvelope>)=>Promise<unknown>,onFailure?:(failure:Readonly<FinancialQueueFailure>)=>Promise<'ACK_RECORDED'|'RETRY'>}} FinancialQueueDependencies */

const KINDS = ['PAYE_NET_ENTRY', 'TRANSFER_BUILD', 'CSV_SETTLEMENT', 'CSV_RETURN', 'CASH_REISSUE', 'SIMPLE_CANCEL', 'POSITION_APPLY', 'PREPARE', 'REISSUE_CANCEL', 'INTERNAL_SETTLEMENT', 'PREPARATION_EXPIRY','PREPARATION_DISCARD', 'CASE_CREATE','CASE_EDIT','CASE_PAUSE','CASE_RESUME', 'CASE_WRITE_OFF'];
const PAGED = ['TRANSFER_BUILD', 'CSV_SETTLEMENT', 'INTERNAL_SETTLEMENT', 'SIMPLE_CANCEL', 'PREPARATION_EXPIRY','PREPARATION_DISCARD'];
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const ENCODER = new TextEncoder();
const errorResult = (error_code, retryable = false, outcome_unknown = false, recovery = 'NONE') =>
  ({ ok: false, error_code, retryable, outcome_unknown, recovery });

/** Closed two-field native wake; no tokens or nested input encoded in IDs. */
export function bpayNextQueueIdentity(body) {
  if (body === null || typeof body !== 'object' || Array.isArray(body)
    || ![Object.prototype, null].includes(Object.getPrototypeOf(body))) return null;
  const keys = Reflect.ownKeys(body);
  if (keys.length !== 2 || keys.some(key => typeof key !== 'string' || !['id','kind'].includes(key)
    || !Object.getOwnPropertyDescriptor(body,key)?.enumerable
    || !Object.hasOwn(Object.getOwnPropertyDescriptor(body,key),'value'))) return null;
  if (![...KINDS,'ENROLL','EXPIRY','WORKBENCH'].includes(body.kind)
    || typeof body.id !== 'string' || (body.kind === 'ENROLL' ? body.id !== '1' : !UUID.test(body.id))
    || ENCODER.encode(JSON.stringify(body)).byteLength > BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.envelopeBytes) return null;
  return Object.freeze({id:body.id,kind:body.kind});
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
    const evidence = call?.action === 'STEP' && call.job_kind === 'POSITION_APPLY'
      && typeof result.definite_position_cause === 'string'
      ? Object.freeze({job_id:call.job_id,lease_nonce:call.lease_nonce,owner_epoch:call.owner_epoch,
        expected_phase:call.expected_phase,expected_cursor:call.expected_cursor,error_code:result.definite_position_cause}) : null;
    if (await dependencies.onFailure(failureSummary(message, call, result), evidence) === 'ACK_RECORDED') {
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
    return Object.freeze({ action: 'STEP', job_id: call.job_id, job_kind: call.job_kind,
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
          : ['FINANCE_ALLOCATE', 'WORKBENCH_WORK', 'WORKBENCH_CASE', 'WORKER_RELEASE_WORK', 'WORKER_RELEASE_CASE', 'WORKER_RELEASE_FINAL'].includes(result.phase) ? result.cursor === null
            || (call.expected_cursor !== null && BigInt(result.cursor) <= BigInt(call.expected_cursor)) : true)) return null;
    return next;
  }
  if (call.job_kind === 'POSITION_APPLY') {
    if (call.action === 'STEP' && call.expected_phase === result.phase && call.expected_cursor === result.cursor) return null;
    return Object.freeze({ action: 'STEP', job_id: call.job_id, job_kind: call.job_kind,
      lease_nonce: call.action === 'CLAIM' ? result.lease_nonce : call.lease_nonce,
      owner_epoch: call.action === 'CLAIM' ? result.owner_epoch : call.owner_epoch,
      expected_phase: result.phase, expected_cursor: result.cursor,
      limit: call.limit ?? BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.pageRows });
  }
  if (call.action === 'CLAIM') {
    return Object.freeze({ action: 'STEP', job_id: call.job_id, job_kind: call.job_kind,
      lease_nonce: result.lease_nonce, owner_epoch: result.owner_epoch,
      ...(PAGED.includes(call.job_kind) ? { expected_cursor: result.cursor,
        limit: BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.pageRows } : {}) });
  }
  // An unfinished page must yield a strictly later checkpoint, including
  // owner replay returning an already-advanced current cursor. Never create
  // an infinite same-cursor continuation from a semantically bad receipt.
  if (!PAGED.includes(call.job_kind) || result.cursor === null
    || (call.expected_cursor !== null && BigInt(result.cursor) <= BigInt(call.expected_cursor))) return null;
  return Object.freeze({ ...call, expected_cursor: result.cursor });
}

function prepareContinuation(call, result) {
  return Object.freeze({ action: 'STEP', job_id: call.job_id, job_kind: 'PREPARE',
    lease_nonce: call.action === 'CLAIM' ? result.lease_nonce : call.lease_nonce,
    owner_epoch: call.action === 'CLAIM' ? result.owner_epoch : call.owner_epoch,
    expected_phase: result.phase,
    ...(['NEW', 'PINNING', 'WORKBENCH_WORK', 'WORKBENCH_CASE'].includes(result.phase) ? { expected_cursor: result.cursor, ...(['WORKBENCH_WORK','WORKBENCH_CASE'].includes(result.phase) ? {limit:BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.pageRows} : {}) }
      : ['RESERVE', 'ALLOCATE'].includes(result.phase) ? { expected_work_cursor: result.work_cursor,
        expected_component_cursor: result.component_cursor,
        limit: call.limit ?? BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.pageRows }
        : ['FINANCE_ALLOCATE', 'WORKER_RELEASE_WORK', 'WORKER_RELEASE_CASE', 'WORKER_RELEASE_FINAL'].includes(result.phase) ? { expected_cursor: result.cursor,
          limit: call.limit ?? BPAY_NEXT_FINANCIAL_QUEUE_LIMITS.pageRows } : {}) });
}

/** Fresh metadata CLAIM, then at most one bounded financial STEP. */
export async function handleBpayNextFinancialQueueMessage(message, dependencies = {}) {
  if (!message || typeof message.ack !== 'function' || typeof message.retry !== 'function') {
    throw new TypeError('BPAY_NEXT_QUEUE_MESSAGE_INTERFACE_INVALID');
  }
  let wake;
  try { wake = bpayNextQueueIdentity(message.body); } catch { wake = null; }
  if (!wake || !KINDS.includes(wake.kind)) return failure(message, null, errorResult('BPAY_NEXT_QUEUE_ENVELOPE_INVALID'), dependencies);
  if (typeof dependencies.financialJobRpc !== 'function'
    || typeof dependencies.sendContinuation !== 'function'
    || typeof dependencies.onFailure !== 'function') {
    return retry(message, errorResult('BPAY_NEXT_QUEUE_DEPENDENCY_UNAVAILABLE', true, false, 'RETRY_CALL'));
  }
  const claim = Object.freeze({action:'CLAIM',job_id:wake.id,job_kind:wake.kind,lease_seconds:120});
  const adapter = {financialJobRpc:dependencies.financialJobRpc,onStepMeasurement:dependencies.onStepMeasurement};
  const claimed = await stepBpayNextFinancialJob(claim, adapter);
  if (!claimed.ok) {
    if (claimed.error_code === 'BPAY_NEXT_JOB_TERMINAL') {
      message.ack(); return {disposition:'ACK',reason:'TERMINAL'};
    }
    return failure(message, claim, claimed, dependencies);
  }
  const call = continuation(claim, claimed.result);
  if (!call) return retry(message,errorResult('BPAY_NEXT_QUEUE_CONTINUATION_INVALID',true,true,'WAIT_FOR_LEASE_EXPIRY'));
  const outcome = await stepBpayNextFinancialJob(call,adapter);
  if (!outcome.ok) return failure(message,call,outcome,dependencies);
  if (outcome.result.done) {message.ack();return {disposition:'ACK',reason:'DONE'};}
  if (!continuation(call,outcome.result)) {
    return retry(message,errorResult('BPAY_NEXT_QUEUE_CONTINUATION_INVALID',true,true,'WAIT_FOR_LEASE_EXPIRY'));
  }
  try {await dependencies.sendContinuation(wake);} catch {
    return retry(message,errorResult('BPAY_NEXT_QUEUE_CONTINUATION_SEND_UNKNOWN',true,true,'WAIT_FOR_LEASE_EXPIRY'));
  }
  message.ack(); return {disposition:'ACK',reason:'CONTINUATION_PERSISTED'};
}
