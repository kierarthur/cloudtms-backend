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

import {caseRestructureInput,caseRestructureToken,RESTRUCTURE_ISSUES,CASE_INPUT_SCHEMA,caseInput,caseToken,caseEditToken,pauseReason,equal} from './bpay-next-case-contract.js';

export const BPAY_NEXT_COMMAND_LIMITS = Object.freeze({
  requestBytes: 2048,
  caseCreateRequestBytes: 32768,
  caseEditRequestBytes: 49152,
  casePauseRequestBytes: 16384,
  responseBytes: 128 * 1024,
  timeoutMs: 8000
});

// EX-01 owns this finite registry. Later cards supply their actual native
// scope/acceptance seams; a registry entry alone does not install an action.
export const BPAY_NEXT_PROTECTED_ACTIONS = Object.freeze(['EXECUTE_REQUEST','EXECUTION_APPROVE',
  'CSV_FILE_ISSUE','CSV_ISSUE','CSV_DISPOSITION','CANCEL_SELECTION','CANCEL_REQUEST',
  'RELEASE_REFUSED','CASH_REISSUE','REISSUE_CANCEL','RETURN_SELECTION','CSV_RETURN',
  'OUTCOME_REVIEW_RESOLVE','WRITE_OFF','PAYE_SAME_WEEK_OVERRIDE']);
const protectedAction = (action) => BPAY_NEXT_PROTECTED_ACTIONS.includes(action);

/** @typedef {'CASE_CREATE'|'PAYE_NET'|'CASE_PAYOUT'|'TRANSFER_BEGIN'|'CANCEL_REQUEST'|'CASH_REISSUE'|'REISSUE_CANCEL'|'CSV_ISSUE'|'CSV_SETTLEMENT'|'CSV_RETURN'|'INTERNAL_SETTLE'|'PREPARATION_EXPIRE'|'WRITE_OFF'|'BIND_STORED_CREDIT'} FinancialAction */
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
const WRITE_OFF_ISSUES = ['BPAY_NEXT_WRITE_OFF_INPUT_PENDING','BPAY_NEXT_WRITE_OFF_PROTECTED_CAPACITY','BPAY_NEXT_WRITE_OFF_AMOUNT_EXCEEDS_BALANCE',
  'BPAY_NEXT_WRITE_OFF_COMPONENT_CHANGED','BPAY_NEXT_WRITE_OFF_ORIGIN_UNBOUND','BPAY_NEXT_WRITE_OFF_NO_BALANCE'];
const BIGINT = /^(?:0|[1-9]\d{0,18})$/;
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;
const ENCODER = new TextEncoder();
const TRANSFER_PHASES = ['BUILDING', 'MEMBERS_READY', 'DRAFT', 'SCHEDULED', 'ISSUED_CSV',
  'SUBMITTED', 'UNKNOWN', 'SETTLED', 'RETURNED', 'REFUSED', 'CANCELLED', 'INTERNAL_PROCESSING', 'INTERNAL_SETTLED'];
const INPUTS = Object.freeze({
  CASE_CREATE: {command_id:'uuid',...CASE_INPUT_SCHEMA,preview_token:'caseToken'},
  CASE_PAUSE: {command_id:'uuid',case_id:'uuid',expected_case_revision:'positiveBigint',reason:'pauseReason'},
  CASE_RESUME: {command_id:'uuid',case_id:'uuid',expected_case_revision:'positiveBigint',reason:'pauseReason'},
  CASE_RESTRUCTURE: {command_id:'uuid',case_id:'uuid',expected_case_revision:'positiveBigint',input:'restructureInput',preview_token:'restructureToken'},
  CASE_EDIT: {command_id:'uuid',case_id:'uuid',expected_case_revision:'positiveBigint',...CASE_INPUT_SCHEMA,preview_token:'caseEditToken'},
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
  CASE_CREATE: {case_id:'uuid',command_id:'uuid',sequence:'positiveBigint',phase:'storedCreditPhase',replay:'boolean'},
  CASE_PAUSE: {case_id:'uuid',candidate_id:'uuid',command_id:'uuid',action:'pauseAction',sequence:'positiveBigint',expected_case_revision:'positiveBigint',phase:'pauseAdmissionPhase',replay:'boolean'},
  CASE_RESUME: {case_id:'uuid',candidate_id:'uuid',command_id:'uuid',action:'pauseAction',sequence:'positiveBigint',expected_case_revision:'positiveBigint',phase:'pauseAdmissionPhase',replay:'boolean'},
  CASE_RESTRUCTURE: {case_id:'uuid',candidate_id:'uuid',command_id:'uuid',sequence:'positiveBigint',expected_case_revision:'positiveBigint',rule_id:'uuid',phase:'restructureAdmission',replay:'boolean'},
  CASE_EDIT: {case_id:'uuid',command_id:'uuid',sequence:'positiveBigint',case_revision:'positiveBigint',rule_id:'uuid',phase:'caseEditPhase',replay:'boolean'},
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
  if(type==='pauseReason')return pauseReason(value);
  if(type==='restructureInput')return caseRestructureInput(value);
  if(type==='restructureToken')return caseRestructureToken(value);
  if(type==='restructureAdmission')return value==='ACCEPTED_PENDING_RESTRUCTURE';
  if(type==='pauseAction')return ['CASE_PAUSE','CASE_RESUME'].includes(value);
  if(type==='pauseAdmissionPhase')return value==='ACCEPTED_PENDING_STATUS';
  if(type==='caseToken')return caseToken(value);
  if(type==='caseEditToken')return caseEditToken(value);
  if(type==='caseEditPhase')return value==='ACCEPTED_PENDING_EDIT';
  switch (type) {
    case 'uuid': return typeof value === 'string' && UUID.test(value);
    case 'payChannel': return ['PAYE','UMBRELLA'].includes(value);
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
    case 'caseNetReceiptPhase': return ['ACCEPTED_PENDING_PROJECTION','PROJECTED','REFUSED','CANCELLED'].includes(value);
    case 'netReplacementStage': return ['PRECHECK','STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING','COMPLETE','REFUSED','CANCELLED'].includes(value);
    case 'netRefusal': return ['CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND'].includes(value);
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
  const schema = body.action === 'CANCEL_REQUEST' && (Object.hasOwn(body, 'run_id') || Object.hasOwn(body, 'pay_channel'))
    ? { ...INPUTS.CANCEL_REQUEST, run_id: 'uuid', pay_channel: 'payChannel' }
    : body.action === 'PAYE_NET' && Object.hasOwn(body, 'expected_projection_revision')
    ? { ...INPUTS.PAYE_NET, expected_projection_revision: 'projectionRevision' }
    : body.action === 'WRITE_OFF' && body.scope === 'AMOUNT'
      ? { ...INPUTS.WRITE_OFF, amount: 'positiveCaseMoney' } : INPUTS[body.action];
  const tokenPresent = Object.hasOwn(body, 'reauth_token');
  if (tokenPresent && (!protectedAction(body.action) || typeof body.reauth_token !== 'string'
    || !body.reauth_token || ENCODER.encode(body.reauth_token).byteLength > 1536)) invalid();
  const keys = Object.keys(schema);
  if (Object.keys(body).length !== keys.length + 1 + Number(tokenPresent)
    || Object.keys(body).some((key) => key !== 'action' && key !== 'reauth_token' && !Object.hasOwn(schema, key))
    || keys.some((key) => !Object.hasOwn(body, key) || (!['CASE_CREATE','CASE_EDIT'].includes(body.action)&&!field(body[key], schema[key])))) invalid();
  const args = Object.fromEntries(keys.map((key) => [key, body[key]]));
  if(ENCODER.encode(JSON.stringify(body)).byteLength>(['CASE_PAUSE','CASE_RESUME'].includes(body.action)?16384:body.action==='CASE_RESTRUCTURE'?32768:body.action==='CASE_EDIT'?49152:body.action==='CASE_CREATE'?BPAY_NEXT_COMMAND_LIMITS.caseCreateRequestBytes:BPAY_NEXT_COMMAND_LIMITS.requestBytes))throw fault('BPAY_NEXT_COMMAND_REQUEST_TOO_LARGE',413);
  if(body.action==='CASE_CREATE'){const {command_id,preview_token,...input}=args;if(!field(command_id,'uuid')||!caseInput(input)||!caseToken(preview_token)||!equal(preview_token.input,input))invalid();}
  if(['CASE_PAUSE','CASE_RESUME'].includes(body.action)&&BigInt(args.expected_case_revision)>=9223372036854775807n)invalid();
  if(body.action==='CASE_RESTRUCTURE'&&(!caseRestructureToken(args.preview_token,args)||BigInt(args.expected_case_revision)>=9223372036854775807n))invalid();
  if(body.action==='CASE_EDIT'){const {command_id,case_id,expected_case_revision,preview_token,...input}=args;
    if(!field(command_id,'uuid')||!field(case_id,'uuid')||!field(expected_case_revision,'positiveBigint')||!caseInput(input)
      ||!caseEditToken(preview_token,{case_id,expected_case_revision})||!equal(preview_token.input,input))invalid();}
  return { action: body.action, args: Object.freeze(args), token: tokenPresent ? body.reauth_token : null };
}

export function qualifyBpayNextReauthIntent(body) {
  if(body?.action==='PAYE_SAME_WEEK_OVERRIDE'){
    const {action,...args}=body,c=args.context,n=c?.namespace;
    const closed=(x,keys)=>record(x)&&Object.keys(x).length===keys.length&&keys.every(k=>Object.hasOwn(x,k));
    if(!closed(args,['run_id','command_id','decision','reason','context'])||!field(args.run_id,'uuid')||!field(args.command_id,'uuid')
      ||!['VERIFY_BOTH','VERIFY_PAYE'].includes(args.decision)||typeof args.reason!=='string'||!args.reason.trim()
      ||args.reason.includes('\0')||ENCODER.encode(args.reason).byteLength>500
      ||!closed(c,['namespace','workbench_revision','adoption_revision'])||!field(c.workbench_revision,'bigint')||!field(c.adoption_revision,'bigint')
      ||!closed(n,['agency_id','owner_epoch','actor_user_id','official_pay_date','scope'])||!field(n.agency_id,'uuid')||!field(n.actor_user_id,'uuid')
      ||!field(n.owner_epoch,'positiveBigint')||!(typeof n.official_pay_date==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(n.official_pay_date)&&date(n.official_pay_date))||!closed(n.scope,['candidate_id','client_id','channel'])
      ||!field(n.scope.candidate_id,'uuid?')||!field(n.scope.client_id,'uuid?')||!['ALL','PAYE','UMBRELLA'].includes(n.scope.channel)
      ||ENCODER.encode(JSON.stringify(body)).byteLength>2048)throw fault('BPAY_NEXT_REAUTH_REQUEST_INVALID',400);
    return {action,args:Object.freeze(args),token:null};
  }
  const call = commandCall(body);
  if (!protectedAction(call.action) || call.token !== null) throw fault('BPAY_NEXT_REAUTH_REQUEST_INVALID',400);
  return call;
}

export async function readBpayNextReauthJson(response, signal, action = null) {
  if (!(response instanceof Response)) throw fault('BPAY_NEXT_COMMAND_RESPONSE_INVALID',503);
  let payload;
  try { payload = await limitedJson(response,BPAY_NEXT_COMMAND_LIMITS.responseBytes,false,signal); }
  catch (error) {
    const safe = dependencyError(error);
    // Preview and proof recording precede the financial RPC.
    throw fault(safe.code,safe.status,safe.retryable,false);
  }
  if (!response.ok && action==='PAYE_SAME_WEEK_OVERRIDE' && payload?.code==='23514'
    && payload?.message==='BPAY_NEXT_CHOICE_CONTEXT_CHANGED') throw fault('BPAY_NEXT_CHOICE_CONTEXT_CHANGED',409);
  if (!response.ok) throw dependencyError({status:response.status,json:payload});
  return payload;
}

export function qualifyBpayNextReauthPreview(payload,action) {
  const keys=['action_kind','action_label','candidate_count','candidate_name','total_amount','amount_basis','scope_sha256','accepted'];
  if (!record(payload) || Object.keys(payload).length!==keys.length || keys.some(k=>!Object.hasOwn(payload,k))
    || payload.action_kind!==action || !protectedAction(action) || typeof payload.action_label!=='string'
    || !payload.action_label || payload.action_label.length>80 || (action==='PAYE_SAME_WEEK_OVERRIDE'?payload.candidate_count!==null:payload.candidate_count!==1)
    || typeof payload.candidate_name!=='string' || !payload.candidate_name || payload.candidate_name.length>512
    || (action==='PAYE_SAME_WEEK_OVERRIDE'?(payload.total_amount!==null||payload.amount_basis!=='PAYE_POLICY')
      :(!field(payload.total_amount,'caseMoney') || (action==='WRITE_OFF'?payload.amount_basis!=='SOURCE_DEBT':!['CASH','DRAFT_GROSS'].includes(payload.amount_basis))))
    || typeof payload.accepted!=='boolean'
    || !(field(payload.scope_sha256,'sha256') || (payload.accepted && action==='CSV_ISSUE' && payload.scope_sha256===null))) {
    throw fault('BPAY_NEXT_COMMAND_RESPONSE_INVALID',503);
  }
  return payload;
}

export async function handleBpayNextReauthPreview(request,dependencies={}) {
  if(request.method!=='POST')return json(405,{ok:false,error_code:'BPAY_NEXT_COMMAND_METHOD_NOT_ALLOWED'});
  let timer,controller;
  try {
    const user=await dependencies.requireOfficeUser?.(request,['admin']);
    if(!user || !field(user.id,'uuid'))throw fault('BPAY_NEXT_COMMAND_UNAUTHORIZED',401);
    if(user.role!=='admin')throw fault('BPAY_NEXT_COMMAND_FORBIDDEN',403);
    const call=qualifyBpayNextReauthIntent(await limitedJson(request,2048,true));
    controller=new AbortController();
    const timeout=new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(fault('BPAY_NEXT_COMMAND_DEPENDENCY_TIMEOUT',503,true));},8000);});
    const read=async()=>{
      const payload=qualifyBpayNextReauthPreview(await readBpayNextReauthJson(await dependencies.reauthPreviewRpc(call.action,call.args,
        {actorUserId:user.id,signal:controller.signal}),controller.signal,call.action),call.action);
      const policy=await dependencies.secondFactorRequired();
      if(typeof policy!=='boolean' || controller.signal.aborted)throw fault('BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE',503,true);
      return {ok:true,preview:{...payload,second_factor_required:policy}};
    };
    return json(200,await Promise.race([read(),timeout]));
  } catch(error) {
    const safe=dependencyError(error);return json(safe.status,{ok:false,error_code:safe.code,retryable:safe.retryable,outcome_unknown:false});
  } finally {if(timer!==undefined)clearTimeout(timer);}
}

function validCaseNetReceipt(p,command){
 if(p.replacement_id===null)return p.replacement_stage===null&&p.projection_id===null&&p.issue_code===null&&p.replay&&p.phase==='ACCEPTED_PENDING_PROJECTION';
 if(p.replacement_id.toLowerCase()!==command.toLowerCase()||p.replacement_stage===null)return false;
 const expected=p.replacement_stage==='COMPLETE'?'PROJECTED':p.replacement_stage==='REFUSED'?'REFUSED':p.replacement_stage==='CANCELLED'?'CANCELLED':'ACCEPTED_PENDING_PROJECTION';
 return p.phase===expected&&(p.phase==='PROJECTED')===(p.projection_id!==null)&&(p.phase==='REFUSED')===(p.issue_code!==null);
}
function commandResult(payload, call) {
  const invalid = () => { throw fault('BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true, true); };
  const caseNet=call.action==='CASE_PAYOUT'||call.action==='PAYE_NET'&&Object.hasOwn(call.args,'expected_projection_revision');
  const schema = caseNet?{...OUTPUTS[call.action],phase:'caseNetReceiptPhase',replacement_id:'uuid?',replacement_stage:'netReplacementStage?',projection_id:'uuid?',issue_code:'netRefusal?'}:OUTPUTS[call.action];
  const keys = Object.keys(schema);
  if (!record(payload) || Object.keys(payload).length !== keys.length
    || keys.some((key) => !Object.hasOwn(payload, key) || !field(payload[key], schema[key]))) invalid();
  for (const key of ['run_id', 'run_worker_id', 'return_cash_id', 'transfer_id', 'instruction_id', 'case_component_id']) {
    if (call.args[key] && !(call.action === 'CANCEL_REQUEST' && key === 'run_id')
      && payload[key]?.toLowerCase() !== call.args[key].toLowerCase()) invalid();
  }
  if (call.args.command_id && payload.command_id.toLowerCase() !== call.args.command_id.toLowerCase()) {
    // Existing evidence owners replay the original receipt's command identity,
    // even if the caller supplied a different command ID for that same receipt.
    if (call.action !== 'CSV_SETTLEMENT' || !payload.replay) invalid();
  }
  if(caseNet&&!validCaseNetReceipt(payload,call.args.command_id))invalid();
  if (['BIND_STORED_CREDIT','CASE_CREATE'].includes(call.action)
    && payload.case_id.toLowerCase() !== call.args.command_id.toLowerCase()) invalid();
  if(['CASE_PAUSE','CASE_RESUME'].includes(call.action)&&(payload.action!==call.action||payload.case_id.toLowerCase()!==call.args.case_id.toLowerCase()||payload.expected_case_revision!==call.args.expected_case_revision))invalid();
  if(call.action==='CASE_RESTRUCTURE'&&(payload.case_id.toLowerCase()!==call.args.case_id.toLowerCase()||payload.candidate_id.toLowerCase()!==call.args.preview_token.context.candidate_id.toLowerCase()||payload.rule_id.toLowerCase()!==call.args.command_id.toLowerCase()||payload.expected_case_revision!==call.args.expected_case_revision))invalid();
  if(call.action==='CASE_EDIT'&&(payload.case_id.toLowerCase()!==call.args.case_id.toLowerCase()
    ||payload.rule_id.toLowerCase()!==call.args.command_id.toLowerCase()
    ||BigInt(payload.case_revision)!==BigInt(call.args.expected_case_revision)+1n))invalid();
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
    if (payload.requested_scope !== call.args.scope) invalid();
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

function dependencyError(error, attempted = false, action = null) {
  if (error instanceof CommandError) return error;
  const db = error?.json ?? error;
  if(action==='CASE_CREATE'&&db?.code==='55000'&&db.message==='BPAY_NEXT_CASE_CREATE_SOURCE_CHANNEL_UNBOUND')return fault(db.message,409);
  if(['CASE_PAUSE','CASE_RESUME'].includes(action)&&['BPAY_NEXT_CASE_PAUSE_UNSUPPORTED','BPAY_NEXT_CASE_PAUSE_STATE_CHANGED','BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID','BPAY_NEXT_CASE_PAUSE_REPLAY_CONFLICT'].includes(db?.message))return fault(db.message,409);
  if(action==='CASE_RESTRUCTURE'&&[...RESTRUCTURE_ISSUES,'BPAY_NEXT_CASE_RESTRUCTURE_REPLAY_CONFLICT'].includes(db?.message))return fault(db.message,409);
  if(action==='CASE_EDIT'&&['BPAY_NEXT_CASE_HAS_ACTIVITY','BPAY_NEXT_CASE_HELD_BY_DRAFT','BPAY_NEXT_CASE_EDIT_UNSUPPORTED',
    'BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID','BPAY_NEXT_CASE_EDIT_REPLAY_CONFLICT'].includes(db?.message))return fault(db.message,409);
  if (db?.message === 'BPAY_NEXT_REAUTH_REQUIRED' || db?.code === 'BPAY_NEXT_REAUTH_REQUIRED') return fault('BPAY_NEXT_REAUTH_REQUIRED',403);
  if (db?.message === 'BPAY_NEXT_REAUTH_STALE' || db?.code === 'BPAY_NEXT_REAUTH_STALE') return fault('BPAY_NEXT_REAUTH_STALE',403);
  if(db?.message==='BPAY_NEXT_CASE_PREVIEW_CHANGED')return fault('BPAY_NEXT_CASE_PREVIEW_CHANGED',409);
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
    const call = commandCall(await limitedJson(request, BPAY_NEXT_COMMAND_LIMITS.caseEditRequestBytes, true));
    if (typeof dependencies.financialCommandRpc !== 'function') throw fault('BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE', 503, true);
    controller = new AbortController();
    const timeout = new Promise((resolve, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(fault('BPAY_NEXT_COMMAND_DEPENDENCY_TIMEOUT', 503, true, attempted));
      }, BPAY_NEXT_COMMAND_LIMITS.timeoutMs);
    });
    const send = async () => {
      let args=call.args;
      if(protectedAction(call.action)) {
        // No token can first issue a CSV. The SQL owner permits only an exact
        // already-issued instruction readback on this explicit exception.
        if(!call.token && call.action!=='CSV_ISSUE')throw fault('BPAY_NEXT_REAUTH_REQUIRED',403);
        if(call.token) {
          if(typeof dependencies.verifyBpayNextActionReauth!=='function' || typeof dependencies.reauthRecordRpc!=='function') {
            throw fault('BPAY_NEXT_COMMAND_DEPENDENCY_UNAVAILABLE',503,true);
          }
          const verified=await dependencies.verifyBpayNextActionReauth(call.token,user,call.action);
          if(!verified)throw fault('BPAY_NEXT_REAUTH_REQUIRED',403);
          const proof=await readBpayNextReauthJson(await dependencies.reauthRecordRpc(verified,
            {actorUserId:user.id,signal:controller.signal}),controller.signal);
          const nativeId=call.args.command_id || call.args.instruction_id;
          if(!record(proof) || Object.keys(proof).length!==2 || !field(proof.reauth_proof_id,'uuid')
            || !field(proof.consumed_command_id,'uuid?'))throw fault('BPAY_NEXT_COMMAND_RESPONSE_INVALID',503,true);
          if(proof.consumed_command_id!==null && proof.consumed_command_id.toLowerCase()!==nativeId.toLowerCase())throw fault('BPAY_NEXT_REAUTH_REQUIRED',403);
          if(proof.consumed_command_id===null && verified.second_factor_used===false) {
            const required=await dependencies.secondFactorRequired?.();
            if(typeof required!=='boolean' || required)throw fault('BPAY_NEXT_REAUTH_REQUIRED',403);
          }
          args=Object.freeze({...call.args,reauth_proof_id:proof.reauth_proof_id});
        }
      }
      if(controller.signal.aborted)throw fault('BPAY_NEXT_COMMAND_DEPENDENCY_TIMEOUT',503,true);
      attempted = true;
      const response = await dependencies.financialCommandRpc(call.action, args, {
        actorUserId: user.id, signal: controller.signal,
        timeoutMs: BPAY_NEXT_COMMAND_LIMITS.timeoutMs,
        maxResponseBytes: BPAY_NEXT_COMMAND_LIMITS.responseBytes,
        routeClass: 'OPERATION_NUDGE'
      });
      if (!(response instanceof Response)) throw fault('BPAY_NEXT_COMMAND_RESPONSE_INVALID', 503, true, true);
      const payload = await limitedJson(response, BPAY_NEXT_COMMAND_LIMITS.responseBytes, false, controller.signal);
      if (!response.ok) throw dependencyError({ status: response.status, json: payload }, true, call.action);
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
