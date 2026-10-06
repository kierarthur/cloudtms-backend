/**
 * Closed authenticated intake for implemented frozen PAYE/case-payout/CSV
 * owners. Policy X: no current Timesheet/history/finance fallback or job drain.
 * Inject the existing active Office requireUser guard as requireOfficeUser.
 * financialCommandRpc calls the closed public.bpay_next_financial_intake_v1 with
 * {p_action: action, p_args: args, p_actor_user_id: options.actorUserId}, forward
 * its AbortSignal and return a raw Response before buffering. The wrapper
 * independently checks service JWT, active admin and NEXT; payment-authoriser
 * gates remain in the CSV/outcome/reissue owners. BIND_STORED_CREDIT alone uses
 * the fixed public.bpay_next_bind_stored_credit_v1(actor,command,legacy_case),
 * never caller-selected RPC or caller money. CORS/route wiring stays with
 * the broker. This module does not claim installation/deployment availability.
 *
 * A timeout or invalid/missing response can follow a committed mutation.
 * outcome_unknown requires retrying the EXACT same action/IDs/evidence, never
 * generating a replacement command, instruction or receipt ID. No automatic
 * retry occurs here. Accepted commands still need the separate worker drain.
 */

export const BPAY_NEXT_COMMAND_LIMITS = Object.freeze({
  requestBytes: 2048,
  responseBytes: 128 * 1024,
  timeoutMs: 8000
});

/** @typedef {'PAYE_NET'|'CASE_PAYOUT'|'TRANSFER_BEGIN'|'CANCEL_REQUEST'|'CASH_REISSUE'|'REISSUE_CANCEL'|'CSV_ISSUE'|'CSV_SETTLEMENT'|'CSV_RETURN'|'INTERNAL_SETTLE'|'PREPARATION_EXPIRE'|'WRITE_OFF'|'BIND_STORED_CREDIT'} FinancialAction */
/** @typedef {{id: string, role: string}} ValidatedOfficeUser */
/** @typedef {{actorUserId: string, signal: AbortSignal, timeoutMs: number, maxResponseBytes: number, routeClass: 'OPERATION_NUDGE'}} FinancialRpcOptions */
/**
 * @typedef {Object} CommandDependencies
 * @property {(request: Request, roles: string[]) => Promise<ValidatedOfficeUser|null>} requireOfficeUser Existing active/session-version Office guard.
 * @property {(action: FinancialAction, args: Readonly<Record<string,string>>, options: FinancialRpcOptions) => Promise<Response>} [financialCommandRpc]
 */

const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const MONEY = /^(?:0|[1-9]\d{0,15})(?:\.\d{1,2})?$/;
const CASE_MONEY = /^(?:0|[1-9]\d*)(?:\.\d{1,2})?$/;
const WRITE_OFF_ISSUES = ['BPAY_NEXT_WRITE_OFF_PROTECTED_CAPACITY','BPAY_NEXT_WRITE_OFF_AMOUNT_EXCEEDS_BALANCE',
  'BPAY_NEXT_WRITE_OFF_COMPONENT_CHANGED','BPAY_NEXT_WRITE_OFF_ORIGIN_UNBOUND','BPAY_NEXT_WRITE_OFF_NO_BALANCE'];
const BIGINT = /^(?:0|[1-9]\d{0,18})$/;
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;
const ENCODER = new TextEncoder();
const TRANSFER_PHASES = ['BUILDING', 'MEMBERS_READY', 'DRAFT', 'SCHEDULED', 'ISSUED_CSV',
  'SUBMITTED', 'UNKNOWN', 'SETTLED', 'RETURNED', 'REFUSED', 'CANCELLED', 'INTERNAL_PROCESSING', 'INTERNAL_SETTLED'];
const INPUTS = Object.freeze({
  BIND_STORED_CREDIT: { command_id: 'uuid', legacy_case_id: 'uuid' },
  WRITE_OFF: { command_id: 'uuid', case_component_id: 'uuid', expected_component_revision: 'positiveBigint', scope: 'writeOffScope', reason: 'writeOffReason' },
  PAYE_NET: { command_id: 'uuid', run_worker_id: 'uuid', entered_paye_net: 'money' },
  CASE_PAYOUT: { command_id: 'uuid', run_worker_id: 'uuid', expected_projection_revision: 'projectionRevision' },
  TRANSFER_BEGIN: { command_id: 'uuid', run_worker_id: 'uuid' },
  CANCEL_REQUEST: { command_id: 'uuid', run_worker_id: 'uuid' },
  CASH_REISSUE: { command_id: 'uuid', return_cash_id: 'uuid' },
  REISSUE_CANCEL: { command_id: 'uuid', transfer_id: 'uuid' },
  CSV_ISSUE: { instruction_id: 'uuid', transfer_id: 'uuid' },
  INTERNAL_SETTLE: { command_id: 'uuid', transfer_id: 'uuid' },
  PREPARATION_EXPIRE: { command_id: 'uuid', run_id: 'uuid', expected_deadline: 'timestamp' },
  CSV_SETTLEMENT: { command_id: 'uuid', transfer_id: 'uuid', receipt_id: 'receipt', amount: 'positiveMoney', occurred_at: 'timestamp' },
  CSV_RETURN: { command_id: 'uuid', transfer_id: 'uuid', receipt_id: 'receipt', amount: 'positiveMoney', occurred_at: 'timestamp' }
});
const OUTPUTS = Object.freeze({
  BIND_STORED_CREDIT: { case_id: 'uuid', command_id: 'uuid', sequence: 'positiveBigint', phase: 'storedCreditPhase', replay: 'boolean' },
  WRITE_OFF: { command_id: 'uuid', case_id: 'uuid', case_component_id: 'uuid', sequence: 'positiveBigint', phase: 'writeOffPhase',
    requested_scope: 'writeOffScope', applied_amount: 'caseMoney?', remaining_amount: 'caseMoney?', protected_amount: 'caseMoney?',
    event_id: 'uuid?', issue_code: 'writeOffIssue?', replay: 'boolean' },
  PAYE_NET: { command_id: 'uuid', run_worker_id: 'uuid', sequence: 'positiveBigint', request_no: 'positiveBigint', phase: 'netPhase', replay: 'boolean' },
  CASE_PAYOUT: { command_id: 'uuid', run_worker_id: 'uuid', sequence: 'positiveBigint', request_no: 'positiveBigint', phase: 'netPhase', replay: 'boolean' },
  TRANSFER_BEGIN: { command_id: 'uuid', run_worker_id: 'uuid', sequence: 'positiveBigint', transfer_id: 'uuid', phase: 'transferPhase', replay: 'boolean' },
  CANCEL_REQUEST: { command_id: 'uuid', run_worker_id: 'uuid', sequence: 'positiveBigint', phase: 'cancelPhase', cursor: 'positiveBigint?', released_line_count: 'bigint', blocked_code: 'code?', replay: 'boolean' },
  CASH_REISSUE: { command_id: 'uuid', return_cash_id: 'uuid', sequence: 'positiveBigint', transfer_id: 'uuid?', phase: 'reissuePhase', replay: 'boolean' },
  REISSUE_CANCEL: { command_id: 'uuid', transfer_id: 'uuid', sequence: 'positiveBigint', phase: 'reissueCancelPhase', blocked_code: 'code?', replay: 'boolean' },
  CSV_ISSUE: { instruction_id: 'uuid', transfer_id: 'uuid', file_name: 'file', csv_sha256: 'sha256', csv_text: 'csv', phase: 'csvPhase', payment_recorded: 'boolean', replay: 'boolean' },
  CSV_SETTLEMENT: { command_id: 'uuid', transfer_id: 'uuid', outcome_id: 'uuid', sequence: 'positiveBigint', posting_complete: 'boolean', phase: 'outcomePhase', replay: 'boolean' },
  INTERNAL_SETTLE: { command_id: 'uuid', transfer_id: 'uuid', internal_receipt_id: 'uuid', sequence: 'positiveBigint', posting_complete: 'boolean', phase: 'outcomePhase', replay: 'boolean' },
  PREPARATION_EXPIRE: { command_id: 'uuid', run_id: 'uuid', sequence: 'positiveBigint', phase: 'expiryPhase', expected_candidates: 'bigint', completed_candidates: 'bigint', replay: 'boolean' },
  CSV_RETURN: { command_id: 'uuid', transfer_id: 'uuid', outcome_id: 'uuid', sequence: 'positiveBigint', posting_complete: 'boolean', phase: 'outcomePhase', replay: 'boolean' }
});

class CommandError extends Error {
  constructor(code, status, retryable = false, outcomeUnknown = false) {
    super(code);
    this.code = code;
    this.status = status;
    this.retryable = retryable;
    this.outcomeUnknown = outcomeUnknown;
  }
}
const fault = (code, status = 503, retryable = false, unknown = false) =>
  new CommandError(code, status, retryable, unknown);

function json(status, body) {
  return new Response(JSON.stringify(body), { status, headers: {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store', 'x-content-type-options': 'nosniff'
  } });
}
function record(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function date(value) {
  if (value.slice(0, 4) === '0000') return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}
function bigint(value, positive = false) {
  return typeof value === 'string' && BIGINT.test(value)
    && BigInt(value) <= 9223372036854775807n && (!positive || value !== '0');
}
function field(value, type) {
  if (type.endsWith('?')) return value === null || field(value, type.slice(0, -1));
  switch (type) {
    case 'uuid': return typeof value === 'string' && UUID.test(value);
    case 'money': return typeof value === 'string' && MONEY.test(value);
    case 'positiveMoney': return field(value, 'money') && /[1-9]/.test(value);
    case 'caseMoney': return typeof value === 'string' && CASE_MONEY.test(value);
    case 'positiveCaseMoney': return field(value, 'caseMoney') && /[1-9]/.test(value)
      && ENCODER.encode(value).byteLength <= 1024;
    case 'writeOffScope': return ['AMOUNT','ALL'].includes(value);
    case 'writeOffPhase': return ['REQUESTED','WRITTEN_OFF','BLOCKED','REVIEW'].includes(value);
    case 'writeOffIssue': return WRITE_OFF_ISSUES.includes(value);
    case 'writeOffReason': return typeof value === 'string' && !value.includes('\0') && value.trim() !== ''
      && ENCODER.encode(value).byteLength >= 1 && ENCODER.encode(value).byteLength <= 1024;
    case 'bigint': return bigint(value);
    case 'projectionRevision': return bigint(value) && BigInt(value) < 9223372036854775807n;
    case 'positiveBigint': return bigint(value, true);
    case 'boolean': return typeof value === 'boolean';
    case 'receipt': return typeof value === 'string' && !value.includes('\0')
      && ENCODER.encode(value).byteLength >= 1 && ENCODER.encode(value).byteLength <= 256;
    case 'timestamp': return typeof value === 'string' && TIMESTAMP.test(value)
      && date(value.slice(0, 10)) && Number.isFinite(Date.parse(value));
    case 'code': return typeof value === 'string' && /^BPAY_NEXT_[A-Z0-9_]+$/.test(value)
      && ENCODER.encode(value).byteLength <= 256;
    case 'file': return typeof value === 'string' && ENCODER.encode(value).byteLength >= 1
      && ENCODER.encode(value).byteLength <= 256;
    case 'sha256': return typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
    case 'csv': return typeof value === 'string' && ENCODER.encode(value).byteLength >= 1
      && ENCODER.encode(value).byteLength <= 120000;
    case 'netPhase': return value === 'ACCEPTED_PENDING_PROJECTION';
    case 'storedCreditPhase': return value === 'ACCEPTED_PENDING_CASE';
    case 'transferPhase': return TRANSFER_PHASES.includes(value);
    case 'cancelPhase': return ['REQUESTED', 'CANCELLING', 'CANCELLED', 'BLOCKED'].includes(value);
    case 'reissuePhase': return ['ACCEPTED_PENDING_BUILD', 'TRANSFER_CREATED'].includes(value);
    case 'reissueCancelPhase': return ['REQUESTED', 'CANCELLED', 'BLOCKED'].includes(value);
    case 'csvPhase': return value === 'ISSUED_CSV';
    case 'outcomePhase': return ['ACCEPTED_PENDING_POSTING', 'POSTED'].includes(value);
    case 'expiryPhase': return ['CANCELLING', 'EXPIRED'].includes(value);
    default: return false;
  }
}

// Bound encoded transport bytes before JSON parsing or retaining any result.
async function limitedJson(input, maximum, request = false, signal = null) {
  const invalid = request ? 'BPAY_NEXT_COMMAND_REQUEST_INVALID' : 'BPAY_NEXT_COMMAND_RESPONSE_INVALID';
  const large = request ? 'BPAY_NEXT_COMMAND_REQUEST_TOO_LARGE' : 'BPAY_NEXT_COMMAND_RESPONSE_TOO_LARGE';
  const status = request ? 400 : 503;
  const declared = input.headers.get('content-length');
  if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > maximum)) {
    if (input.body) await input.body.cancel().catch(() => {});
    throw fault(large, request ? 413 : 503, !request, !request);
  }
  if (!input.body) throw fault(invalid, status, !request, !request);
  const reader = input.body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  const parts = [];
  let bytes = 0;
  const cancel = () => { void reader.cancel().catch(() => {}); };
  signal?.addEventListener('abort', cancel, { once: true });
  try {
    if (signal?.aborted) {
      await reader.cancel().catch(() => {});
      throw fault('BPAY_NEXT_COMMAND_DEPENDENCY_TIMEOUT', 503, true, true);
    }
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > maximum) {
        await reader.cancel().catch(() => {});
        throw fault(large, request ? 413 : 503, !request, !request);
      }
      parts.push(decoder.decode(value, { stream: true }));
    }
    parts.push(decoder.decode());
    try { return JSON.parse(parts.join('')); } catch { throw fault(invalid, status, !request, !request); }
  } catch (error) {
    await reader.cancel().catch(() => {});
    if (error instanceof CommandError) throw error;
    throw fault(invalid, status, !request, !request);
  } finally {
    signal?.removeEventListener('abort', cancel);
    reader.releaseLock();
  }
}

function commandCall(body) {
  const invalid = () => { throw fault('BPAY_NEXT_COMMAND_REQUEST_INVALID', 400); };
  if (!record(body) || !Object.hasOwn(INPUTS, body.action)) invalid();
  const schema = body.action === 'PAYE_NET' && Object.hasOwn(body, 'expected_projection_revision')
    ? { ...INPUTS.PAYE_NET, expected_projection_revision: 'projectionRevision' }
    : body.action === 'WRITE_OFF' && body.scope === 'AMOUNT'
      ? { ...INPUTS.WRITE_OFF, amount: 'positiveCaseMoney' } : INPUTS[body.action];
  const keys = Object.keys(schema);
  if (Object.keys(body).length !== keys.length + 1
    || Object.keys(body).some((key) => key !== 'action' && !Object.hasOwn(schema, key))
    || keys.some((key) => !Object.hasOwn(body, key) || !field(body[key], schema[key]))) invalid();
  const args = Object.fromEntries(keys.map((key) => [key, body[key]]));
  return { action: body.action, args: Object.freeze(args) };
}

function commandResult(payload, call) {
  const invalid = () => { throw fault('BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true, true); };
  const schema = OUTPUTS[call.action];
  const keys = Object.keys(schema);
  if (!record(payload) || Object.keys(payload).length !== keys.length
    || keys.some((key) => !Object.hasOwn(payload, key) || !field(payload[key], schema[key]))) invalid();
  for (const key of ['run_id', 'run_worker_id', 'return_cash_id', 'transfer_id', 'instruction_id', 'case_component_id']) {
    if (call.args[key] && payload[key]?.toLowerCase() !== call.args[key].toLowerCase()) invalid();
  }
  if (call.args.command_id && payload.command_id.toLowerCase() !== call.args.command_id.toLowerCase()) {
    // Existing evidence owners replay the original receipt's command identity,
    // even if the caller supplied a different command ID for that same receipt.
    if (!['CSV_SETTLEMENT', 'CSV_RETURN'].includes(call.action) || !payload.replay) invalid();
  }
  if (call.action === 'BIND_STORED_CREDIT'
    && payload.case_id.toLowerCase() !== call.args.command_id.toLowerCase()) invalid();
  if (call.action === 'CASH_REISSUE'
    && payload.phase !== (payload.transfer_id === null ? 'ACCEPTED_PENDING_BUILD' : 'TRANSFER_CREATED')) invalid();
  if (['CSV_SETTLEMENT', 'CSV_RETURN', 'INTERNAL_SETTLE'].includes(call.action)
    && payload.phase !== (payload.posting_complete ? 'POSTED' : 'ACCEPTED_PENDING_POSTING')) invalid();
  if (['CANCEL_REQUEST','REISSUE_CANCEL'].includes(call.action)
    && (payload.phase === 'BLOCKED') !== (payload.blocked_code !== null)) invalid();
  if (call.action === 'PREPARATION_EXPIRE'
    && (BigInt(payload.completed_candidates) > BigInt(payload.expected_candidates)
      || (payload.phase === 'EXPIRED'
        && payload.completed_candidates !== payload.expected_candidates))) invalid();
  if (call.action === 'WRITE_OFF') {
    if (payload.requested_scope !== call.args.scope || payload.case_id.toLowerCase() !== payload.case_component_id.toLowerCase()) invalid();
    if (payload.phase === 'REQUESTED') {
      if (['applied_amount','remaining_amount','protected_amount','event_id','issue_code'].some((key) => payload[key] !== null)) invalid();
    } else {
      if (['applied_amount','remaining_amount','protected_amount'].some((key) => payload[key] === null)
        || pennies(payload.protected_amount) > pennies(payload.remaining_amount)) invalid();
      if (payload.phase === 'WRITTEN_OFF') {
        if (pennies(payload.applied_amount) <= 0n || payload.event_id?.toLowerCase() !== call.args.command_id.toLowerCase()
          || payload.issue_code !== null || (call.args.scope === 'ALL' && pennies(payload.remaining_amount) !== 0n)
          || (call.args.scope === 'AMOUNT' && pennies(payload.applied_amount) !== pennies(call.args.amount))) invalid();
      } else if (pennies(payload.applied_amount) !== 0n || payload.event_id !== null || payload.issue_code === null
        || (payload.phase === 'REVIEW') !== ['BPAY_NEXT_WRITE_OFF_COMPONENT_CHANGED','BPAY_NEXT_WRITE_OFF_ORIGIN_UNBOUND'].includes(payload.issue_code)) invalid();
    }
  }
  return payload;
}
// Exact validation only. SQL owns the financial balances and W arithmetic.
function pennies(value) {
  const [whole, fraction = ''] = value.split('.');
  return BigInt(whole) * 100n + BigInt(fraction.padEnd(2, '0'));
}

function dependencyError(error, attempted = false) {
  if (error instanceof CommandError) return error;
  const db = error?.json ?? error;
  if (db?.code === 'PGRST202') return fault('BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE', 503, true);
  if (db?.code === '42501' || [401, 403].includes(error?.status)) return fault('BPAY_NEXT_COMMAND_FORBIDDEN', 403);
  if (db?.code === 'P0002') return fault('BPAY_NEXT_COMMAND_SCOPE_NOT_FOUND', 404);
  if (['22023', '22003', '22007', '22008', '22P02'].includes(db?.code)) return fault('BPAY_NEXT_COMMAND_REQUEST_INVALID', 400);
  if (['23514', '23505'].includes(db?.code)) return fault('BPAY_NEXT_COMMAND_CONFLICT', 409);
  if (db?.code === '55000') {
    if (db.message === 'BPAY_NEXT_MODULE_NOT_ACTIVE') return fault('BPAY_NEXT_COMMAND_MODULE_NOT_ACTIVE', 503);
    if (['BPAY_NEXT_CSV_CONFIGURED_FORMAT_NOT_YET_SUPPORTED', 'BPAY_NEXT_CSV_CONFIGURED_COLUMNS_NOT_YET_SUPPORTED'].includes(db.message)) {
      return fault('BPAY_NEXT_COMMAND_CSV_CONFIGURATION_UNSUPPORTED', 409);
    }
    return fault('BPAY_NEXT_COMMAND_NOT_ELIGIBLE', 409);
  }
  if (db?.code === '54000' && db.message === 'BPAY_NEXT_COMMAND_RESPONSE_INVALID') {
    return fault('BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503);
  }
  if (error?.name === 'AbortError' || error?.status === 408) {
    return fault('BPAY_NEXT_COMMAND_DEPENDENCY_TIMEOUT', 503, true, attempted);
  }
  return fault('BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE', 503, true, attempted);
}

/**
 * @param {Request} request
 * @param {Partial<CommandDependencies>} dependencies
 * @returns {Promise<Response>}
 */
export async function handleBpayNextCommand(request, dependencies = {}) {
  if (request.method !== 'POST') return json(405, {
    ok: false, error_code: 'BPAY_NEXT_COMMAND_METHOD_NOT_ALLOWED', retryable: false, outcome_unknown: false
  });
  let controller, timer;
  let attempted = false;
  try {
    if (typeof dependencies.requireOfficeUser !== 'function') throw fault('BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE', 503, true);
    const user = await dependencies.requireOfficeUser(request, ['admin']);
    if (!user || !field(user.id, 'uuid')) throw fault('BPAY_NEXT_COMMAND_UNAUTHORIZED', 401);
    if (user.role !== 'admin') throw fault('BPAY_NEXT_COMMAND_FORBIDDEN', 403);
    const call = commandCall(await limitedJson(request, BPAY_NEXT_COMMAND_LIMITS.requestBytes, true));
    if (typeof dependencies.financialCommandRpc !== 'function') throw fault('BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE', 503, true);
    controller = new AbortController();
    const timeout = new Promise((resolve, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(fault('BPAY_NEXT_COMMAND_DEPENDENCY_TIMEOUT', 503, true, true));
      }, BPAY_NEXT_COMMAND_LIMITS.timeoutMs);
    });
    const send = async () => {
      attempted = true;
      const response = await dependencies.financialCommandRpc(call.action, call.args, {
        actorUserId: user.id, signal: controller.signal,
        timeoutMs: BPAY_NEXT_COMMAND_LIMITS.timeoutMs,
        maxResponseBytes: BPAY_NEXT_COMMAND_LIMITS.responseBytes,
        routeClass: 'OPERATION_NUDGE'
      });
      if (!(response instanceof Response)) throw fault('BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true, true);
      const payload = await limitedJson(response, BPAY_NEXT_COMMAND_LIMITS.responseBytes, false, controller.signal);
      if (!response.ok) throw dependencyError({ status: response.status, json: payload }, true);
      const result = { ok: true, action: call.action, result: commandResult(payload, call) };
      if (ENCODER.encode(JSON.stringify(result)).byteLength > BPAY_NEXT_COMMAND_LIMITS.responseBytes) {
        throw fault('BPAY_NEXT_COMMAND_RESPONSE_TOO_LARGE', 503, true, true);
      }
      return result;
    };
    return json(200, await Promise.race([send(), timeout]));
  } catch (error) {
    const safe = dependencyError(error, attempted);
    return json(safe.status, {
      ok: false, error_code: safe.code, retryable: safe.retryable, outcome_unknown: safe.outcomeUnknown
    });
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
