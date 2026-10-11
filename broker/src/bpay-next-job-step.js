import {PAUSE_RECEIPT_SCHEMA,pauseValue,casePauseReceipt} from './bpay-next-case-contract.js';
/**
 * INTERNAL one-call financial job adapter. This is not a Request handler,
 * Office route, job discovery/enrollment API, queue binding or Candidate drain.
 * Inject a service-only raw transport to public.bpay_next_financial_job_v1:
 * {p_action: action,p_job_id: jobId,p_job_kind: jobKind,p_args: args}.
 * It must forward signal, retain the fixed RPC target and return raw Response
 * before buffering. SQL independently gates service JWT/NEXT and dispatches
 * only to the closed current job kinds, preserving scoped lease/replay
 * authority and lock order. Policy X: no live Timesheet/finance/history fallback.
 *
 * Fresh CLAIM receipt stays invocation-local for the identity-wake consumer. A lost CLAIM response cannot expose
 * or adopt an active stored nonce: wait for lease expiry and claim afresh.
 * Unknown queue STEP outcome retries the native identity, waits for lease
 * expiry and claims the current durable cursor. Direct retained STEP replay
 * still uses exact original job/kind/nonce/epoch/cursor/page inputs.
 * A DONE claim is terminal, not an instruction to retry CLAIM indefinitely;
 * use the retained STEP input for the owner's completed replay when needed.
 * WAIT_FOR_PREDECESSOR is an ordered CLAIM deferral, not a failed job: let
 * the earlier Candidate command finish before trying this exact CLAIM again.
 * JOB_CLAIM_RETRY means the exact fresh supported state became claimable
 * during contention; RETRY_CALL signals a later call, never re-dispatch here.
 * No automatic repeat, re-claim, next-page call or external side effect here.
 */

export const BPAY_NEXT_JOB_LIMITS = Object.freeze({
  requestBytes: 2048, responseBytes: 128 * 1024, timeoutMs: 8000, pageRows: 100
});

/** @typedef {'PAYE_NET_ENTRY'|'TRANSFER_BUILD'|'CSV_SETTLEMENT'|'CSV_RETURN'|'CASH_REISSUE'|'SIMPLE_CANCEL'|'POSITION_APPLY'|'PREPARE'|'REISSUE_CANCEL'|'INTERNAL_SETTLEMENT'|'PREPARATION_EXPIRY'|'PREPARATION_DISCARD'|'CASE_CREATE'|'CASE_EDIT'|'CASE_PAUSE'|'CASE_RESUME'|'CASE_WRITE_OFF'} FinancialJobKind */
/** @typedef {{action:'CLAIM',job_id:string,job_kind:FinancialJobKind,lease_seconds:number}} FinancialJobClaim */
/** @typedef {{action:'STEP',job_id:string,job_kind:'PAYE_NET_ENTRY'|'CSV_RETURN'|'CASH_REISSUE'|'REISSUE_CANCEL'|'CASE_CREATE'|'CASE_EDIT'|'CASE_PAUSE'|'CASE_RESUME'|'CASE_WRITE_OFF',lease_nonce:string,owner_epoch:string}} FinancialJobSingleStep */
/** @typedef {{action:'STEP',job_id:string,job_kind:'TRANSFER_BUILD'|'CSV_SETTLEMENT'|'INTERNAL_SETTLEMENT'|'SIMPLE_CANCEL'|'PREPARATION_EXPIRY'|'PREPARATION_DISCARD',lease_nonce:string,owner_epoch:string,expected_cursor:string|null,limit:number}} FinancialJobPageStep */
/** @typedef {{action:'STEP',job_id:string,job_kind:'POSITION_APPLY',lease_nonce:string,owner_epoch:string,expected_phase:'NEW'|'REMOVED'|'SNOOZE',expected_cursor:string|null,limit:number}} FinancialJobPositionStep */
/** @typedef {{action:'STEP',job_id:string,job_kind:'PREPARE',lease_nonce:string,owner_epoch:string,expected_phase:'NEW'|'PINNING',expected_cursor:string|null}} FinancialJobPinStep */
/** @typedef {{action:'STEP',job_id:string,job_kind:'PREPARE',lease_nonce:string,owner_epoch:string,expected_phase:'RESERVE'|'ALLOCATE',expected_work_cursor:string|null,expected_component_cursor:string|null,limit:number}} FinancialJobPreparePageStep */
/** @typedef {{action:'STEP',job_id:string,job_kind:'PREPARE',lease_nonce:string,owner_epoch:string,expected_phase:'FINANCE_ALLOCATE'|'WORKBENCH_WORK'|'WORKBENCH_CASE'|'WORKER_RELEASE_WORK'|'WORKER_RELEASE_CASE'|'WORKER_RELEASE_FINAL',expected_cursor:string|null,limit:number}} FinancialJobPrepareCompleteStep */
/** @typedef {{action:'STEP',job_id:string,job_kind:'PAYE_NET_ENTRY',lease_nonce:string,owner_epoch:string,expected_phase:'FINANCE_ALLOCATE',expected_cursor:string|null,limit:number}} FinancialJobCaseNetPageStep */
/** @typedef {{signal:AbortSignal,timeoutMs:number,maxResponseBytes:number,routeClass:'OPERATION_NUDGE'}} FinancialJobRpcOptions */
/** @typedef {{financialJobRpc?:(action:'CLAIM'|'STEP',jobId:string,jobKind:FinancialJobKind,args:Readonly<Record<string,string|number|null>>,options:FinancialJobRpcOptions)=>Promise<Response>,onStepMeasurement?:(measurement:Readonly<{job_id:string,kind:FinancialJobKind,step:string,rows:string|null,bytes:number|null,ms:number}>,payQueryBusy:boolean)=>void}} FinancialJobDependencies */

const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const DECIMAL_INTEGER = /^(?:0|[1-9]\d{0,18})$/;
const CASE_MONEY = /^(?:0|[1-9]\d*)(?:\.\d{1,2})?$/;
const WRITE_OFF_ISSUES = ['BPAY_NEXT_WRITE_OFF_INPUT_PENDING','BPAY_NEXT_WRITE_OFF_PROTECTED_CAPACITY','BPAY_NEXT_WRITE_OFF_AMOUNT_EXCEEDS_BALANCE',
  'BPAY_NEXT_WRITE_OFF_COMPONENT_CHANGED','BPAY_NEXT_WRITE_OFF_ORIGIN_UNBOUND','BPAY_NEXT_WRITE_OFF_NO_BALANCE'];
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?Z$/;
const ENCODER = new TextEncoder();
const PAGED = ['TRANSFER_BUILD', 'CSV_SETTLEMENT', 'INTERNAL_SETTLEMENT', 'SIMPLE_CANCEL', 'PREPARATION_EXPIRY', 'PREPARATION_DISCARD'];
const PREPARE_RELEASE_PHASES = ['WORKER_RELEASE_WORK', 'WORKER_RELEASE_CASE', 'WORKER_RELEASE_FINAL'];
const PREPARE_PHASES = ['NEW', 'PINNING', 'RESERVE', 'ALLOCATE', 'FINANCE_ALLOCATE', 'WORKBENCH_WORK', 'WORKBENCH_CASE', ...PREPARE_RELEASE_PHASES];
const CASE_PREPARE_ISSUES = ['CASE_SELECTION_REQUIRED', 'CASE_TARGET_UNSUPPORTED', 'CASE_INPUT_UNBOUND',
  'CASE_WEEK_BASIS_UNBOUND', 'CASE_RETURN_FLOOR_UNBOUND', 'CASE_NO_PAYABLE_AMOUNT'];
const PREPARE_ISSUES = ['SELECTED_WORK_NOT_APPROVED', 'SELECTED_WORK_POSITION_PENDING',
  'SELECTED_WORK_REVISION_CHANGED',
  'CAPTURE_POSITION_CHANGED', 'POSITION_COMPONENT_EVIDENCE_MISSING',
  'POSITION_APPROVAL_AMOUNT_MISMATCH', 'FINANCIAL_RESOLUTION_REQUIRED', 'TARGET_UMBRELLA_MISSING',
  'SOURCE_PAY_QUERY_OPEN', 'SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE',
  'UMBRELLA_WORK_EXECUTION_UNSUPPORTED', 'SOURCE_BOOK_APPROVAL_UNBOUND',
  'SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE', ...CASE_PREPARE_ISSUES];
const CLAIM_PHASES = Object.freeze({
  PAYE_NET_ENTRY: ['NEW', 'FINANCE_ALLOCATE'], TRANSFER_BUILD: ['WORK'], CSV_SETTLEMENT: ['MEMBERS'], INTERNAL_SETTLEMENT: ['MEMBERS'],
  CSV_RETURN: ['MEMBERS'], CASH_REISSUE: ['MEMBERS'], SIMPLE_CANCEL: ['NEW', 'RELEASE'],
  POSITION_APPLY: ['NEW', 'REMOVED', 'SNOOZE'], PREPARE: PREPARE_PHASES, REISSUE_CANCEL: ['NEW'], PREPARATION_EXPIRY: ['NEW', 'RELEASE'], PREPARATION_DISCARD: ['NEW', 'RELEASE'], CASE_CREATE: ['NEW'], CASE_EDIT: ['NEW'], CASE_PAUSE: ['NEW'], CASE_RESUME: ['NEW'], CASE_WRITE_OFF: ['NEW']
});
const COMMON = { job_id: 'uuid', job_kind: 'kind', owner_epoch: 'positive', lease_until_utc: 'timestamp?', done: 'boolean', replay: 'boolean' };
const { replay: unusedPositionReplay, ...POSITION_COMMON } = COMMON;
const OUTPUTS = Object.freeze({
  PAYE_NET_ENTRY: { ...COMMON, phase: 'netPhase', projection_id: 'uuid' },
  TRANSFER_BUILD: { ...COMMON, phase: 'transferPhase', transfer_id: 'uuid', transfer_status: 'transferStatus', cursor: 'positive?', member_count: 'integer', rows_visited: 'pageCount' },
  CSV_SETTLEMENT: { ...COMMON, phase: 'outcomePhase', cursor: 'positive?', posted_members: 'integer', rows_visited: 'pageCount' },
  INTERNAL_SETTLEMENT: { ...COMMON, phase: 'outcomePhase', cursor: 'positive?', posted_members: 'integer', rows_visited: 'pageCount' },
  CSV_RETURN: { ...COMMON, phase: 'completePhase', return_cash_id: 'uuid' },
  CASH_REISSUE: { ...COMMON, phase: 'completePhase', transfer_id: 'uuid' },
  CASE_CREATE: { ...COMMON, phase: 'caseCreatedPhase', case_id: 'uuid' },
  CASE_PAUSE: {...COMMON,...PAUSE_RECEIPT_SCHEMA},
  CASE_RESUME: {...COMMON,...PAUSE_RECEIPT_SCHEMA},
  CASE_EDIT: {...COMMON,phase:'caseEditPhase',case_id:'uuid',case_revision:'positive',rule_id:'uuid',issue_code:'caseEditIssue?'},
  CASE_WRITE_OFF: { ...COMMON, phase: 'writeOffPhase', case_id: 'uuid', case_component_id: 'uuid',
    applied_amount: 'caseMoney', remaining_amount: 'caseMoney', protected_amount: 'caseMoney', event_id: 'uuid?', issue_code: 'writeOffIssue?' },
  REISSUE_CANCEL: { ...COMMON, phase: 'reissueCancelPhase', transfer_id: 'uuid', blocked_code: 'code?' },
  SIMPLE_CANCEL: { ...COMMON, phase: 'cancelPhase', run_worker_id: 'uuid', cursor: 'positive?', released_line_count: 'integer', rows_visited: 'pageCount', holds_released: 'pageCount', blocked_code: 'code?' },
  PREPARATION_EXPIRY: { ...COMMON, phase: 'expiryPhase', cursor: 'positive?', rows_visited: 'pageCount',
    work_released: 'pageCount', case_holds_released: 'pageCount', run_worker_id: 'uuid?' },
  PREPARATION_DISCARD: { ...COMMON, phase: 'discardStepPhase', cursor: 'positive?', rows_visited: 'pageCount',
    work_released: 'pageCount', case_holds_released: 'pageCount', run_worker_id: 'uuid?' },
  // The position TABLE owner does not expose a replay boolean. Do not infer
  // one from zero rows: a genuine empty phase-completion page can visit zero.
  POSITION_APPLY: { ...POSITION_COMMON, phase: 'positionPhase', cursor: 'component?', rows_visited: 'pageCount' },
  PREPARE: { ...COMMON, phase: 'preparePhase', cursor: 'positive?', work_cursor: 'uuid?', component_cursor: 'component?',
    run_worker_id: 'uuid', worker_status: 'workerStatus', issue_code: 'prepareIssue?', issue_work_id: 'uuid?', rows_visited: 'pageCount?' }
});
// Expiry can retire a PREPARE that was never claimed and has no worker.
// This separate schema must not weaken any ordinary PREPARE receipt.
const EXPIRED_PREPARE_OUTPUT = { ...OUTPUTS.PREPARE, phase: 'expiredPhase',
  run_worker_id: 'uuid?', worker_status: 'workerStatus?', issue_code:'retiredPrepareIssue?' };
const DISCARDED_PREPARE_OUTPUT = { ...OUTPUTS.PREPARE, phase: 'discardedPhase',
  run_worker_id: 'uuid?', worker_status: 'workerStatus?', issue_code:'retiredPrepareIssue?' };
const CASE_NET_OUTPUT = { ...COMMON, phase: 'caseNetPhase', projection_id: 'uuid?',
  cursor: 'positive?', rows_visited: 'pageCount', run_worker_id: 'uuid', issue_code: 'caseNetIssue?' };
const CLAIM_OUTPUT = { job_id: 'uuid', job_kind: 'kind', phase: 'claimPhase', lease_nonce: 'uuid', owner_epoch: 'positive', lease_until_utc: 'timestamp', cursor: 'positive?' };
const POSITION_CLAIM_OUTPUT = { ...CLAIM_OUTPUT, phase: 'positionClaimPhase', cursor: 'component?' };
const PREPARE_CLAIM_OUTPUT = { ...CLAIM_OUTPUT, phase: 'prepareClaimPhase', work_cursor: 'uuid?', component_cursor: 'component?' };

class JobError extends Error {
  constructor(code, retryable = false, unknown = false, recovery = 'NONE') {
    super(code);
    this.code = code;
    this.retryable = retryable;
    this.unknown = unknown;
    this.recovery = recovery;
  }
}
const fault = (code, retryable = false, unknown = false, recovery = 'NONE') =>
  new JobError(code, retryable, unknown, recovery);
const record = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
function integer(value, positive = false) {
  return typeof value === 'string' && DECIMAL_INTEGER.test(value)
    && BigInt(value) <= 9223372036854775807n && (!positive || value !== '0');
}
function field(value, type) {
  if(type.startsWith('pause'))return pauseValue(value,type);
  if (type.endsWith('?')) return value === null || field(value, type.slice(0, -1));
  switch (type) {
    case 'uuid': return typeof value === 'string' && UUID.test(value);
    case 'snoozeCursor': return typeof value === 'string' && UUID.test(value) && value === value.toLowerCase();
    case 'kind': return typeof value === 'string' && Object.hasOwn(CLAIM_PHASES, value);
    case 'positive': return integer(value, true);
    case 'integer': return integer(value);
    case 'pageCount': return integer(value) && BigInt(value) <= 100n;
    case 'component': return typeof value === 'string' && [...value].length >= 1 && [...value].length <= 256
      && !value.includes('\u0000') && new TextDecoder().decode(ENCODER.encode(value)) === value;
    case 'boolean': return typeof value === 'boolean';
    case 'timestamp': return typeof value === 'string' && TIMESTAMP.test(value)
      && value.slice(0, 4) !== '0000' && Number.isFinite(Date.parse(value))
      && new Date(`${value.slice(0, 10)}T00:00:00Z`).toISOString().slice(0, 10) === value.slice(0, 10);
    case 'code': return typeof value === 'string' && /^BPAY_NEXT_[A-Z0-9_]{1,246}$/.test(value);
    case 'claimPhase': return ['NEW', 'WORK', 'MEMBERS', 'RELEASE', 'FINANCE_ALLOCATE'].includes(value);
    case 'positionClaimPhase': return ['NEW', 'REMOVED', 'SNOOZE'].includes(value);
    case 'prepareClaimPhase': return PREPARE_PHASES.includes(value);
    case 'positionPhase': return ['NEW', 'REMOVED', 'SNOOZE', 'DONE'].includes(value);
    case 'preparePhase': return [...PREPARE_PHASES, 'READY', 'REVIEW', 'CANCELLED'].includes(value);
    case 'expiryPhase': return ['RELEASE', 'EXPIRED'].includes(value);
    case 'discardStepPhase': return ['RELEASE','DISCARDED','BLOCKED'].includes(value);
    case 'discardedPhase': return value === 'DISCARDED';
    case 'expiredPhase': return value === 'EXPIRED';
    case 'prepareIssue': return PREPARE_ISSUES.includes(value);
    case 'retiredPrepareIssue': return value==='PF08_NEEDS_ATTENTION'||PREPARE_ISSUES.includes(value);
    case 'workerStatus': return ['PREPARING', 'READY', 'REVIEW', 'DRAFT', 'ISSUED', 'COMPLETE', 'CANCELLING', 'CANCELLED'].includes(value);
    case 'netPhase': return value === 'PROJECTED';
    case 'caseNetPhase': return ['FINANCE_ALLOCATE', 'PROJECTED', 'REVIEW'].includes(value);
    case 'caseNetIssue': return ['CASE_INPUT_UNBOUND', 'CASE_WEEK_BASIS_UNBOUND', 'CASE_RETURN_FLOOR_UNBOUND'].includes(value);
    case 'transferPhase': return ['BUILDING', 'MEMBERS_READY'].includes(value);
    case 'transferStatus': return ['BUILDING', 'MEMBERS_READY', 'DRAFT', 'SCHEDULED', 'ISSUED_CSV',
      'SUBMITTED', 'UNKNOWN', 'SETTLED', 'RETURNED', 'REFUSED', 'CANCELLED', 'INTERNAL_PROCESSING', 'INTERNAL_SETTLED'].includes(value);
    case 'outcomePhase': return ['MEMBERS', 'COMPLETE'].includes(value);
    case 'completePhase': return value === 'COMPLETE';
    case 'caseEditPhase': return ['CASE_EDITED','BLOCKED'].includes(value);
    case 'caseEditIssue': return ['BPAY_NEXT_CASE_HAS_ACTIVITY','BPAY_NEXT_CASE_HELD_BY_DRAFT','BPAY_NEXT_CASE_EDIT_UNSUPPORTED','BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID'].includes(value);
    case 'caseCreatedPhase': return value === 'CASE_CREATED';
    case 'caseMoney': return typeof value === 'string' && CASE_MONEY.test(value);
    case 'writeOffPhase': return ['WRITTEN_OFF','BLOCKED','REVIEW'].includes(value);
    case 'writeOffIssue': return WRITE_OFF_ISSUES.includes(value);
    case 'cancelPhase': return ['REQUESTED', 'CANCELLING', 'CANCELLED', 'BLOCKED'].includes(value);
    case 'reissueCancelPhase': return ['CANCELLED','BLOCKED'].includes(value);
    default: return false;
  }
}
function exact(value, schema) {
  return record(value) && Object.keys(value).length === Object.keys(schema).length
    && Object.entries(schema).every(([key, type]) => Object.hasOwn(value, key) && field(value[key], type));
}
function callInput(input) {
  const invalid = () => { throw fault('BPAY_NEXT_JOB_REQUEST_INVALID'); };
  if (!record(input) || !['CLAIM', 'STEP'].includes(input.action)
    || !field(input.job_kind, 'kind') || !field(input.job_id, 'uuid')) invalid();
  const keys = input.action === 'CLAIM' ? ['lease_seconds']
    : input.job_kind === 'POSITION_APPLY' ? ['lease_nonce', 'owner_epoch', 'expected_phase', 'expected_cursor', 'limit']
      : input.job_kind === 'PREPARE' ? ['lease_nonce', 'owner_epoch', 'expected_phase',
        ...(['NEW', 'PINNING'].includes(input.expected_phase) ? ['expected_cursor']
          : ['RESERVE', 'ALLOCATE'].includes(input.expected_phase) ? ['expected_work_cursor', 'expected_component_cursor', 'limit']
            : ['FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE', ...PREPARE_RELEASE_PHASES].includes(input.expected_phase) ? ['expected_cursor', 'limit'] : [])]
        : input.job_kind === 'PAYE_NET_ENTRY' && Object.hasOwn(input, 'expected_phase')
          ? ['lease_nonce', 'owner_epoch', 'expected_phase', 'expected_cursor', 'limit']
        : PAGED.includes(input.job_kind) ? ['lease_nonce', 'owner_epoch', 'expected_cursor', 'limit']
      : ['lease_nonce', 'owner_epoch'];
  if (Object.keys(input).length !== keys.length + 3
    || Object.keys(input).some((key) => !['action', 'job_id', 'job_kind', ...keys].includes(key))
    || keys.some((key) => !Object.hasOwn(input, key))) invalid();
  if (input.action === 'CLAIM') {
    if (!Number.isInteger(input.lease_seconds) || input.lease_seconds < 1 || input.lease_seconds > 120) invalid();
  } else {
    if (!field(input.lease_nonce, 'uuid') || !field(input.owner_epoch, 'positive')) invalid();
    if (input.job_kind === 'PAYE_NET_ENTRY' && Object.hasOwn(input, 'expected_phase')
      && (input.expected_phase !== 'FINANCE_ALLOCATE' || !field(input.expected_cursor, 'positive?')
        || !Number.isInteger(input.limit) || input.limit < 1 || input.limit > 100)) invalid();
    if (PAGED.includes(input.job_kind) && (!field(input.expected_cursor, 'positive?')
      || !Number.isInteger(input.limit) || input.limit < 1 || input.limit > 100)) invalid();
    if (input.job_kind === 'POSITION_APPLY' && (!field(input.expected_phase, 'positionClaimPhase')
      || !field(input.expected_cursor, input.expected_phase === 'SNOOZE' ? 'snoozeCursor?' : 'component?') || !Number.isInteger(input.limit)
      || input.limit < 1 || input.limit > 100)) invalid();
    if (input.job_kind === 'PREPARE') {
      if (!PREPARE_PHASES.includes(input.expected_phase)) invalid();
      if (['NEW', 'PINNING'].includes(input.expected_phase) && !field(input.expected_cursor, 'positive?')) invalid();
      if (['RESERVE', 'ALLOCATE'].includes(input.expected_phase) && (!field(input.expected_work_cursor, 'uuid?')
        || !field(input.expected_component_cursor, 'component?')
        || (input.expected_work_cursor === null) !== (input.expected_component_cursor === null)
        || !Number.isInteger(input.limit) || input.limit < 1 || input.limit > 100)) invalid();
      if (['FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE', ...PREPARE_RELEASE_PHASES].includes(input.expected_phase) && (!field(input.expected_cursor, 'positive?')
        || !Number.isInteger(input.limit) || input.limit < 1 || input.limit > 100)) invalid();
    }
  }
  if (ENCODER.encode(JSON.stringify(input)).byteLength > BPAY_NEXT_JOB_LIMITS.requestBytes) invalid();
  return { action: input.action, jobId: input.job_id, jobKind: input.job_kind,
    args: Object.freeze(Object.fromEntries(keys.map((key) => [key, input[key]]))) };
}
function resultInput(payload, call, unknown) {
  const invalid = () => { throw unknown('BPAY_NEXT_JOB_RESPONSE_INVALID'); };
  const claimSchema = call.jobKind === 'POSITION_APPLY' ? POSITION_CLAIM_OUTPUT
    : call.jobKind === 'PREPARE' ? PREPARE_CLAIM_OUTPUT : CLAIM_OUTPUT;
  const caseNet = call.jobKind === 'PAYE_NET_ENTRY' && call.args.expected_phase === 'FINANCE_ALLOCATE';
  const expiredPrepare = call.action === 'STEP' && call.jobKind === 'PREPARE' && payload?.phase === 'EXPIRED';
  const discardedPrepare = call.action === 'STEP' && call.jobKind === 'PREPARE' && payload?.phase === 'DISCARDED';
  if (!exact(payload, call.action === 'CLAIM' ? claimSchema : caseNet ? CASE_NET_OUTPUT
    : expiredPrepare ? EXPIRED_PREPARE_OUTPUT : discardedPrepare ? DISCARDED_PREPARE_OUTPUT : OUTPUTS[call.jobKind])
    || payload.job_id.toLowerCase() !== call.jobId.toLowerCase() || payload.job_kind !== call.jobKind) invalid();
  if (call.action === 'CLAIM') {
    if (!CLAIM_PHASES[call.jobKind].includes(payload.phase)
      || (!PAGED.includes(call.jobKind) && !['POSITION_APPLY', 'PREPARE'].includes(call.jobKind)
        && !(call.jobKind === 'PAYE_NET_ENTRY' && payload.phase === 'FINANCE_ALLOCATE') && payload.cursor !== null)) invalid();
    if (call.jobKind === 'POSITION_APPLY' && payload.phase === 'SNOOZE' && !field(payload.cursor, 'snoozeCursor?')) invalid();
    if (call.jobKind === 'PREPARE' && !prepareCheckpoint(payload)) invalid();
    return payload;
  }
  if (payload.lease_until_utc !== null
    || (!payload.done && payload.owner_epoch !== call.args.owner_epoch)) invalid();
  if (['PAYE_NET_ENTRY', 'CSV_RETURN', 'CASH_REISSUE', 'REISSUE_CANCEL', 'CASE_CREATE', 'CASE_EDIT', 'CASE_PAUSE', 'CASE_RESUME', 'CASE_WRITE_OFF'].includes(call.jobKind) && !caseNet && !payload.done) invalid();
  if(['CASE_PAUSE','CASE_RESUME'].includes(call.jobKind)&&(payload.phase==='PENDING'||!casePauseReceipt(Object.fromEntries(Object.keys(PAUSE_RECEIPT_SCHEMA).map(k=>[k,payload[k]])))||payload.action!==call.jobKind))invalid();
  if(call.jobKind==='CASE_EDIT'&&((payload.phase==='BLOCKED')!==(payload.issue_code!==null)))invalid();
  if (call.jobKind === 'CASE_WRITE_OFF') {
    if (payload.case_id.toLowerCase() !== payload.case_component_id.toLowerCase()
      || pennies(payload.protected_amount) > pennies(payload.remaining_amount)) invalid();
    if (payload.phase === 'WRITTEN_OFF') {
      if (pennies(payload.applied_amount) <= 0n || payload.event_id === null || payload.issue_code !== null) invalid();
    } else if (pennies(payload.applied_amount) !== 0n || payload.event_id !== null || payload.issue_code === null
      || (payload.phase === 'REVIEW') !== ['BPAY_NEXT_WRITE_OFF_COMPONENT_CHANGED','BPAY_NEXT_WRITE_OFF_ORIGIN_UNBOUND'].includes(payload.issue_code)) invalid();
  }
  if (caseNet && (payload.done !== (payload.phase !== 'FINANCE_ALLOCATE')
    || (payload.phase === 'PROJECTED') !== (payload.projection_id !== null)
    || (payload.phase === 'REVIEW') !== (payload.issue_code !== null)
    || (payload.done && payload.cursor !== null)
    || (!payload.done && payload.cursor === null)
    || BigInt(payload.rows_visited) > BigInt(call.args.limit)
    || (payload.replay && payload.rows_visited !== '0'))) invalid();
  if (call.jobKind==='REISSUE_CANCEL' && (payload.phase==='BLOCKED')!==(payload.blocked_code!==null)) invalid();
  if (call.jobKind === 'TRANSFER_BUILD') {
    if (payload.done !== (payload.phase === 'MEMBERS_READY')
      || (!payload.done && payload.transfer_status !== 'BUILDING')
      || (payload.done && payload.transfer_status === 'BUILDING')) invalid();
  }
  if (['CSV_SETTLEMENT', 'INTERNAL_SETTLEMENT'].includes(call.jobKind) && payload.done !== (payload.phase === 'COMPLETE')) invalid();
  if (call.jobKind === 'SIMPLE_CANCEL') {
    if (payload.done !== ['CANCELLED', 'BLOCKED'].includes(payload.phase)
      || (payload.phase === 'BLOCKED') !== (payload.blocked_code !== null)
      || (payload.phase === 'REQUESTED' && (payload.released_line_count !== '0' || payload.holds_released !== '0'))
      || BigInt(payload.holds_released) > BigInt(payload.rows_visited)) invalid();
  }
  if (call.jobKind === 'POSITION_APPLY' && ((payload.phase === 'SNOOZE' && !field(payload.cursor, 'snoozeCursor?'))
    || payload.done !== (payload.phase === 'DONE')
    || (payload.done && payload.cursor !== null) || BigInt(payload.rows_visited) > BigInt(call.args.limit))) invalid();
  if (call.jobKind === 'PREPARATION_EXPIRY' && (payload.done !== (payload.phase === 'EXPIRED')
    || (!payload.done && payload.cursor === null)
    || BigInt(payload.work_released) + BigInt(payload.case_holds_released) > BigInt(payload.rows_visited)
    || (payload.run_worker_id === null && (payload.work_released !== '0' || payload.case_holds_released !== '0'))
    || (payload.replay && (payload.work_released !== '0' || payload.case_holds_released !== '0')))) invalid();
  if (call.jobKind === 'PREPARATION_DISCARD' && (payload.done !== ['DISCARDED','BLOCKED'].includes(payload.phase)
    || (!payload.done && payload.cursor === null)
    || BigInt(payload.work_released) + BigInt(payload.case_holds_released) > BigInt(payload.rows_visited)
    || (payload.run_worker_id === null && (payload.work_released !== '0' || payload.case_holds_released !== '0'))
    || (payload.phase === 'BLOCKED' && (payload.work_released !== '0' || payload.case_holds_released !== '0'))
    || (payload.replay && (payload.work_released !== '0' || payload.case_holds_released !== '0')))) invalid();
  if (call.jobKind === 'PREPARE') {
    if((discardedPrepare||expiredPrepare)&&payload.issue_code==='PF08_NEEDS_ATTENTION'&&payload.issue_work_id!==null)invalid();
    if (discardedPrepare) {
      if (!payload.done || !payload.replay || payload.rows_visited !== '0'
        || (payload.work_cursor === null) !== (payload.component_cursor === null)
        || (payload.cursor !== null && payload.work_cursor !== null)
        || (payload.run_worker_id === null) !== (payload.worker_status === null)
        || (payload.issue_code === null && payload.issue_work_id !== null)
        || (payload.run_worker_id === null && (payload.issue_code !== null || payload.issue_work_id !== null))) invalid();
      return payload;
    }
    if (expiredPrepare) {
      if (!payload.done || !payload.replay || payload.rows_visited !== '0'
        || (payload.work_cursor === null) !== (payload.component_cursor === null)
        || (payload.cursor !== null && payload.work_cursor !== null)
        || (payload.run_worker_id === null) !== (payload.worker_status === null)
        || (payload.worker_status !== null && !['PREPARING', 'REVIEW', 'READY', 'CANCELLING', 'CANCELLED'].includes(payload.worker_status))
        || (payload.issue_code === null && payload.issue_work_id !== null)
        || (payload.run_worker_id === null && (payload.issue_code !== null || payload.issue_work_id !== null))) invalid();
      return payload;
    }
    const projecting = ['WORKBENCH_WORK','WORKBENCH_CASE'].includes(payload.phase);
    const releasing = PREPARE_RELEASE_PHASES.includes(payload.phase);
    const reviewing = payload.phase === 'REVIEW' || (projecting && payload.worker_status === 'REVIEW');
    if (!prepareCheckpoint(payload) || payload.done !== ['READY', 'REVIEW', 'CANCELLED'].includes(payload.phase)
      || reviewing !== (payload.issue_code !== null)
      || (reviewing && (CASE_PREPARE_ISSUES.includes(payload.issue_code)
        ? payload.issue_work_id !== null : payload.issue_work_id === null))
      || (!reviewing && payload.issue_work_id !== null)
      || (payload.phase === 'REVIEW' && payload.worker_status !== 'REVIEW')
      || (projecting && !['READY','REVIEW','CANCELLED'].includes(payload.worker_status))
      || (releasing && payload.worker_status !== 'CANCELLING')
      || (payload.phase === 'CANCELLED' && payload.worker_status !== 'CANCELLED')
      || (!payload.done && !projecting && !releasing && payload.worker_status !== 'PREPARING')
      || (payload.phase === 'READY' && !['READY', 'DRAFT', 'ISSUED', 'COMPLETE', 'CANCELLING', 'CANCELLED'].includes(payload.worker_status))
      || (payload.rows_visited !== null && BigInt(payload.rows_visited) > BigInt(call.args.limit ?? 1))
      || (payload.replay && payload.rows_visited !== null && payload.rows_visited !== '0')) invalid();
  }
  if (PAGED.includes(call.jobKind) && (BigInt(payload.rows_visited) > BigInt(call.args.limit)
    || (payload.replay && payload.rows_visited !== '0'))) invalid();
  return payload;
}
function pennies(value) {
  const [whole, fraction = ''] = value.split('.');
  return BigInt(whole) * 100n + BigInt(fraction.padEnd(2, '0'));
}

function prepareCheckpoint(payload) {
  if (['NEW', 'PINNING'].includes(payload.phase)) return payload.work_cursor === null && payload.component_cursor === null;
  if (['FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE', ...PREPARE_RELEASE_PHASES].includes(payload.phase)) return payload.work_cursor === null && payload.component_cursor === null;
  if (['RESERVE', 'ALLOCATE'].includes(payload.phase)) return payload.cursor === null
    && (payload.work_cursor === null) === (payload.component_cursor === null);
  return payload.cursor === null && payload.work_cursor === null && payload.component_cursor === null;
}

// Read at most 128KiB encoded bytes before parsing any JSON; cancel unread
// bodies on error/deadline, including late responses from an ignored signal.
async function limitedJson(response, signal, unknown, measurement) {
  const maximum = BPAY_NEXT_JOB_LIMITS.responseBytes;
  const declared = response.headers.get('content-length');
  if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > maximum)) {
    if (response.body) await response.body.cancel().catch(() => {});
    throw unknown('BPAY_NEXT_JOB_RESPONSE_TOO_LARGE');
  }
  if (!response.body) throw unknown('BPAY_NEXT_JOB_RESPONSE_INVALID');
  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  const parts = [];
  let bytes = 0;
  measurement.bytes = 0;
  const cancel = () => { void reader.cancel().catch(() => {}); };
  signal.addEventListener('abort', cancel, { once: true });
  try {
    if (signal.aborted) throw unknown('BPAY_NEXT_JOB_DEPENDENCY_TIMEOUT');
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      measurement.bytes = bytes;
      if (bytes > maximum) throw unknown('BPAY_NEXT_JOB_RESPONSE_TOO_LARGE');
      parts.push(decoder.decode(value, { stream: true }));
    }
    parts.push(decoder.decode());
    return JSON.parse(parts.join(''));
  } catch (error) {
    await reader.cancel().catch(() => {});
    if (error instanceof JobError) throw error;
    throw unknown('BPAY_NEXT_JOB_RESPONSE_INVALID');
  } finally {
    signal.removeEventListener('abort', cancel);
    reader.releaseLock();
  }
}
function dependencyError(error, attempted, action) {
  if (error instanceof JobError) return error;
  const db = error?.json ?? error;
  if (db?.code === 'PGRST202') return fault('BPAY_NEXT_JOB_DEPENDENCY_UNAVAILABLE', true, false, 'RETRY_CALL');
  if (db?.code === '42501' || [401, 403].includes(error?.status)) return fault('BPAY_NEXT_JOB_FORBIDDEN');
  if (db?.code === 'P0002') return fault('BPAY_NEXT_JOB_NOT_FOUND');
  if (['22023', '22003', '22P02'].includes(db?.code)) return fault('BPAY_NEXT_JOB_REQUEST_INVALID');
  if (['23514', '23505'].includes(db?.code)) {
    return fault(db.message === 'BPAY_NEXT_JOB_KIND_MISMATCH' ? 'BPAY_NEXT_JOB_KIND_MISMATCH' : 'BPAY_NEXT_JOB_CONFLICT');
  }
  if (db?.code === '55000') {
    if (db.message === 'BPAY_NEXT_MODULE_NOT_ACTIVE') return fault('BPAY_NEXT_JOB_MODULE_NOT_ACTIVE');
    if (db.message === 'BPAY_NEXT_JOB_TERMINAL') return fault('BPAY_NEXT_JOB_TERMINAL');
    if (db.message === 'BPAY_NEXT_JOB_LEASE_BUSY') return fault('BPAY_NEXT_JOB_LEASE_BUSY', true, false, 'WAIT_FOR_LEASE_EXPIRY');
    if (db.message === 'BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR') return fault('BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR', true, false, 'WAIT_FOR_PREDECESSOR');
    if (db.message === 'BPAY_NEXT_JOB_CLAIM_RETRY') return fault('BPAY_NEXT_JOB_CLAIM_RETRY', true, false, 'RETRY_CALL');
    if (db.message === 'BPAY_NEXT_PREPARATION_EXPIRING') return fault('BPAY_NEXT_PREPARATION_EXPIRING', true, false, 'RETRY_SAME_STEP');
    if (db.message === 'BPAY_NEXT_CASE_ALLOCATION_REQUIRED') return fault('BPAY_NEXT_JOB_CASE_ALLOCATION_UNSUPPORTED');
    if (db.message === 'BPAY_NEXT_JOB_PHASE_UNSUPPORTED') return fault('BPAY_NEXT_JOB_PHASE_UNSUPPORTED');
    return fault('BPAY_NEXT_JOB_NOT_ELIGIBLE');
  }
  if (['40001', '40P01', '55P03', '57014'].includes(db?.code)) {
    return fault('BPAY_NEXT_JOB_TRANSACTION_RETRY', true, false, 'RETRY_CALL');
  }
  if (db?.code === '54000' && db.message === 'BPAY_NEXT_JOB_RESPONSE_INVALID') return fault('BPAY_NEXT_JOB_RESPONSE_INVALID');
  const recovery = attempted ? action === 'CLAIM' ? 'WAIT_FOR_LEASE_EXPIRY' : 'RETRY_SAME_STEP' : 'RETRY_CALL';
  return fault(error?.name === 'AbortError' || error?.status === 408
    ? 'BPAY_NEXT_JOB_DEPENDENCY_TIMEOUT' : 'BPAY_NEXT_JOB_DEPENDENCY_UNAVAILABLE', true, attempted, recovery);
}

/**
 * @param {FinancialJobClaim|FinancialJobSingleStep|FinancialJobPageStep|FinancialJobPositionStep|FinancialJobPinStep|FinancialJobPreparePageStep|FinancialJobPrepareCompleteStep} input
 * @param {FinancialJobDependencies} dependencies
 * @returns {Promise<Object>} Closed typed success or sanitized recovery result.
 */
export async function stepBpayNextFinancialJob(input, dependencies = {}) {
  let call, controller, timer;
  let attempted = false;
  let started, payQueryBusy = false, definitePositionCause = null, rollbackConfirmed=false;
  const measurement = { rows: null, bytes: null };
  const observeBusy = (error) => {
    const db = error?.json ?? error;
    if (db?.code === '55P03' && db.message === 'WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY') payQueryBusy = true;
  };
  try {
    call = callInput(input);
    if (typeof dependencies.financialJobRpc !== 'function') throw fault('BPAY_NEXT_JOB_DEPENDENCY_UNAVAILABLE', true, false, 'RETRY_CALL');
    controller = new AbortController();
    const unknown = (code) => fault(code, true, true,
      call.action === 'CLAIM' ? 'WAIT_FOR_LEASE_EXPIRY' : 'RETRY_SAME_STEP');
    const timeout = new Promise((resolve, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(unknown('BPAY_NEXT_JOB_DEPENDENCY_TIMEOUT'));
      }, BPAY_NEXT_JOB_LIMITS.timeoutMs);
    });
    const send = async () => {
      attempted = true;
      started = performance.now();
      const response = await dependencies.financialJobRpc(call.action, call.jobId, call.jobKind, call.args, {
        signal: controller.signal, timeoutMs: BPAY_NEXT_JOB_LIMITS.timeoutMs,
        maxResponseBytes: BPAY_NEXT_JOB_LIMITS.responseBytes, routeClass: 'OPERATION_NUDGE'
      });
      if (!(response instanceof Response)) throw unknown('BPAY_NEXT_JOB_RESPONSE_INVALID');
      const payload = await limitedJson(response, controller.signal, unknown, measurement);
      if (!response.ok) {
        if(call.action==='STEP'&&call.jobKind==='PREPARE'
          &&['40001','40P01','55P03','57014','23514','23503','23505','55000','22023','22003','22P02','P0002'].includes(payload?.code))rollbackConfirmed=true;
        observeBusy(payload);
        // Internal provenance only, never arbitrary DB text or lease logs.
        // A parsed SQL rejection of this exact STEP proves rollback; normal
        // claim/predecessor/lease and uncertain transport recovery stay native.
        if (call.action === 'STEP' && call.jobKind === 'POSITION_APPLY'
          && ['23514','23503','55000'].includes(payload?.code)
          && typeof payload.message === 'string' && /^BPAY_NEXT_[A-Z0-9_]{1,240}$/.test(payload.message)
          && !['BPAY_NEXT_POSITION_LEASE_STALE','BPAY_NEXT_POSITION_PREDECESSOR_MISMATCH',
            'BPAY_NEXT_MODULE_NOT_ACTIVE','BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING',
            'BPAY_NEXT_JOB_LEASE_BUSY','BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR','BPAY_NEXT_JOB_CLAIM_RETRY',
            'BPAY_NEXT_JOB_TERMINAL','BPAY_NEXT_JOB_KIND_MISMATCH'].includes(payload.message)) {
          definitePositionCause = payload.message;
        }
        if (definitePositionCause && payload.code === '23503') throw fault('BPAY_NEXT_JOB_CONFLICT');
        throw dependencyError({ status: response.status, json: payload }, true, call.action);
      }
      if(call.jobKind==='PREPARE'&&payload?.deferred===true){
        const keys=['job_id','job_kind','deferred','needs_attention','next_retry_at_utc'];
        if(!record(payload)||Object.keys(payload).length!==keys.length||keys.some(k=>!Object.hasOwn(payload,k))
          ||payload.job_id!==call.jobId||payload.job_kind!=='PREPARE'||typeof payload.needs_attention!=='boolean'
          ||payload.next_retry_at_utc!==null&&(typeof payload.next_retry_at_utc!=='string'||!Number.isFinite(Date.parse(payload.next_retry_at_utc))))throw unknown('BPAY_NEXT_JOB_RESPONSE_INVALID');
        return {ok:false,error_code:'BPAY_NEXT_PREPARE_RETRY_DEFERRED',retryable:false,outcome_unknown:false,recovery:'NONE',durable_deferred:true};
      }
      const result = resultInput(payload, call, unknown);
      // Only the validated owner's per-call visited count has this meaning.
      // CLAIM/single-step receipts and unknown outcomes have no such count.
      measurement.rows = Object.hasOwn(result, 'rows_visited') ? result.rows_visited : null;
      return { ok: true, action: call.action, job_kind: call.jobKind, result };
    };
    return await Promise.race([send(), timeout]);
  } catch (error) {
    observeBusy(error);
    const safe = dependencyError(error, attempted, call?.action);
    return { ok: false, error_code: safe.code, retryable: safe.retryable,
      outcome_unknown: safe.unknown, recovery: safe.recovery,
      ...(definitePositionCause && !safe.retryable && !safe.unknown ? { definite_position_cause: definitePositionCause } : {}),
      ...(rollbackConfirmed&&!safe.unknown?{rollback_confirmed:true}:{}) };
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    if (attempted && typeof dependencies.onStepMeasurement === 'function') {
      try {
        const report = dependencies.onStepMeasurement(Object.freeze({
          job_id: call.jobId, kind: call.jobKind,
          step: call.action === 'CLAIM' ? 'CLAIM' : call.args.expected_phase ? `STEP:${call.args.expected_phase}` : 'STEP',
          rows: measurement.rows, bytes: measurement.bytes,
          ms: Math.max(0, performance.now() - started)
        }), payQueryBusy);
        // Observation never participates in SQL success or queue disposition.
        if (typeof report?.catch === 'function') report.catch(() => {});
      } catch {}
    }
  }
}
