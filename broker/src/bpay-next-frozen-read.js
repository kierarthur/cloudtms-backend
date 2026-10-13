/**
 * One frozen detail page for Review/Draft consumers (Policy X).
 *
 * Integration: inject requireUser(env, request, roles) as requireOfficeUser.
 * frozenPageRpc must be an authorised service-only wrapper returning a raw
 * Response. It must check the current actor's permission and exact run/group
 * scope, then delegate to the corresponding private page in
 * 04102026_0025_banking_pay_next_frozen_detail_pages_v1.sql and the additive
 * frozen shift page v2. The service-only transport wrapper is defined in
 * 04102026_0250_banking_pay_next_frozen_detail_service_v1.sql. It calls
 * public.bpay_next_frozen_detail_page_v1 with
 * {...args, p_kind: kind, p_actor_user_id: options.actorUserId}. Its service-JWT,
 * active-admin, enabled-module and exact scope checks must be installed/proved
 * before wiring a browser route; availability is not claimed by this module.
 *
 * The wrapper must return numeric(18,6) columns as decimal TEXT. Parsing the
 * existing numeric JSON response first and then String(number) loses digits.
 * Pass through the AbortSignal and return the response before buffering it;
 * sbRpc currently calls res.text() and cannot supply this transport boundary.
 * No current Timesheet, Source head, contract, candidate or history dependency
 * is accepted here. Missing immutable detail remains an explicit error.
 */

export const BPAY_NEXT_FROZEN_READ_LIMITS = Object.freeze({
  requestBytes: 2048,
  responseBytes: 128 * 1024,
  pageRows: 100,
  timeoutMs: 8000
});

/** @typedef {'SHIFTS'|'BREAKS'|'RATE_DETAILS'|'RATE_SCHEDULE'} FrozenDetailKind */
/** @typedef {{id: string, role: string}} ValidatedOfficeUser */
/** @typedef {{actorUserId: string, signal: AbortSignal, timeoutMs: number, maxResponseBytes: number}} FrozenRpcOptions */
/**
 * @typedef {Object} FrozenReadDependencies
 * @property {(request: Request, roles: string[]) => Promise<ValidatedOfficeUser|null>} requireOfficeUser
 * @property {(kind: FrozenDetailKind, args: Record<string, string|number|null>, options: FrozenRpcOptions) => Promise<Response>} [frozenPageRpc]
 */
/**
 * @typedef {Object} FrozenShiftRow
 * @property {string} shift_detail_id
 * @property {number} detail_no
 * @property {string} work_date
 * @property {string|null} shift_start_at
 * @property {string|null} shift_end_at
 * @property {string|null} shift_start_local
 * @property {string|null} shift_end_local
 * @property {boolean|null} shift_overnight
 * @property {number|null} submitted_minutes
 * @property {number|null} approved_minutes
 * @property {string|null} approved_hours Exact decimal text; never Number().
 * @property {string|null} detail_label
 * @property {string|null} segment_pay_ex_vat Exact effective authorised segment amount.
 * @property {boolean|null} pay_excluded
 * @property {string|null} hours_day
 * @property {string|null} hours_night
 * @property {string|null} hours_sat
 * @property {string|null} hours_sun
 * @property {string|null} hours_bh
 */
/** @typedef {{break_detail_id: string, break_no: number, break_start_at: string|null, break_end_at: string|null, break_start_local: string|null, break_end_local: string|null, break_minutes: number}} FrozenBreakRow */
/** @typedef {{rate_detail_id: string, bucket: string, approved_hours: string, source_pay_rate: string|null}} FrozenRateDetailRow */
/** @typedef {{rate_schedule_id: string, rate_family: string, rate_code: string, unit_label: string|null, paye_rate: string|null, umbrella_rate: string|null, charge_rate: string|null}} FrozenRateScheduleRow */

const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const DECIMAL = /^-?(?:0|[1-9]\d{0,11})(?:\.\d{1,6})?$/;
const CLOCK = /^(?:[01]\d|2[0-3]):[0-5]\d$/;
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/;
const BUCKETS = ['BH', 'DAY', 'NIGHT', 'SAT', 'SUN'];
const FAMILIES = ['ADDITIONAL', 'MILEAGE', 'STANDARD'];
const ENCODER = new TextEncoder();
const ORIGIN_SCHEMA=Object.freeze({"classification":"classification","revision_id":"uuid","origin_kind":"originKind","source_kind":"sourceKind","sheet_scope":"dailyScope?","contract_id":"uuid?","client_id":"uuid?","work_date":"date?","physical_timesheet_id":"uuid","physical_timesheet_version":"positiveInt","candidate_display_name":"label","candidate_reference":"label?","client_display_name":"label","job_title":"label?","band_label":"label?","timesheet_reference":"label?","pay_channel":"channel","authorised_at_utc":"timestamp?"});
const USED_LINE_SCHEMA=Object.freeze({component_kind:'component',component_key:'componentKey',unit_label:'capturedText?',approved_quantity:'capturedDecimal?',approved_unit_rate:'capturedDecimal?'});
const SCHEMAS = Object.freeze({
  SHIFTS: {
    shift_detail_id: 'uuid', detail_no: 'positiveInt', work_date: 'date',
    shift_start_at: 'timestamp?', shift_end_at: 'timestamp?',
    shift_start_local: 'clock?', shift_end_local: 'clock?',
    shift_overnight: 'boolean?', submitted_minutes: 'int?',
    approved_minutes: 'int?', approved_hours: 'decimal?', detail_label: 'label?',
    segment_pay_ex_vat: 'money?', pay_excluded: 'boolean?',
    hours_day: 'decimal?', hours_night: 'decimal?', hours_sat: 'decimal?',
    hours_sun: 'decimal?', hours_bh: 'decimal?'
  },
  BREAKS: {
    break_detail_id: 'uuid', break_no: 'positiveInt',
    break_start_at: 'timestamp?', break_end_at: 'timestamp?',
    break_start_local: 'clock?', break_end_local: 'clock?', break_minutes: 'int'
  },
  RATE_DETAILS: {
    rate_detail_id: 'uuid', bucket: 'bucket', approved_hours: 'decimal',
    source_pay_rate: 'decimal?'
  },
  RATE_SCHEDULE: {
    rate_schedule_id: 'uuid', rate_family: 'family', rate_code: 'code',
    unit_label: 'unit?', paye_rate: 'decimal?', umbrella_rate: 'decimal?',
    charge_rate: 'decimal?'
  }
});

class FrozenReadError extends Error {
  constructor(code, status, retryable) {
    super(code);
    this.code = code;
    this.status = status;
    this.retryable = retryable;
  }
}

function fault(code, status = 503, retryable = false) {
  return new FrozenReadError(code, status, retryable);
}

function json(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff'
    }
  });
}

function record(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function integer(value, minimum = 0) {
  return Number.isInteger(value) && value >= minimum && value <= 2147483647;
}

function date(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

function field(value, type) {
  if (type.endsWith('?')) return value === null || field(value, type.slice(0, -1));
  switch (type) {
    case 'classification': return ['CLASSIFIED','UNCLASSIFIED_MISMATCH','UNKNOWN'].includes(value);
    case 'originKind': return ['CONTRACT','DAILY'].includes(value);
    case 'sourceKind': return ['ORDINARY','SOURCE','PROTECTED','EXPENSE','ADJUSTMENT'].includes(value);
    case 'component': return ['WORK','PROTECTED_WORK','EXPENSE','MILEAGE','ADDITIONAL','ADJUSTMENT'].includes(value);
    case 'componentKey': return typeof value==='string'&&!value.includes('\0')&&[...value].length>=1&&[...value].length<=256;
    case 'capturedText': return typeof value==='string'&&!value.includes('\0')&&ENCODER.encode(value).byteLength<=120000;
    case 'capturedDecimal': return typeof value==='string'&&value.length<=120000&&/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(value);
    case 'dailyScope': return value === 'DAILY';
    case 'channel': return ['PAYE','UMBRELLA'].includes(value);
    case 'uuid': return typeof value === 'string' && UUID.test(value);
    case 'int': return integer(value);
    case 'positiveInt': return integer(value, 1);
    case 'date': return date(value);
    case 'boolean': return typeof value === 'boolean';
    case 'decimal': return typeof value === 'string' && DECIMAL.test(value);
    case 'money': return typeof value === 'string' && /^-?(?:0|[1-9]\d{0,15})(?:\.\d{1,2})?$/.test(value);
    case 'clock': return typeof value === 'string' && CLOCK.test(value);
    case 'timestamp': return typeof value === 'string' && TIMESTAMP.test(value)
      && date(value.slice(0, 10)) && Number.isFinite(Date.parse(value));
    case 'bucket': return BUCKETS.includes(value);
    case 'family': return FAMILIES.includes(value);
    case 'code': return typeof value === 'string' && Array.from(value).length >= 1
      && Array.from(value).length <= 245 && !value.includes('\0');
    case 'unit': return typeof value === 'string' && ENCODER.encode(value).byteLength <= 512;
    case 'label': return typeof value === 'string' && ENCODER.encode(value).byteLength <= 8192;
    default: return false;
  }
}

// Read the transport's byte boundary before JSON parsing or copying any rows.
async function limitedJson(input, maximum, request = false, signal = null) {
  const code = request ? 'BPAY_NEXT_FROZEN_REQUEST_INVALID' : 'BPAY_NEXT_FROZEN_RESPONSE_INVALID';
  const tooLarge = request ? 'BPAY_NEXT_FROZEN_REQUEST_TOO_LARGE' : 'BPAY_NEXT_FROZEN_PAGE_TOO_LARGE';
  const status = request ? 400 : 503;
  const declared = input.headers.get('content-length');
  if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > maximum)) {
    if (input.body) await input.body.cancel().catch(() => {});
    throw fault(tooLarge, request ? 413 : 503);
  }
  if (!input.body) throw fault(code, status);
  const reader = input.body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  const parts = [];
  let bytes = 0;
  const cancel = () => { void reader.cancel().catch(() => {}); };
  signal?.addEventListener('abort', cancel, { once: true });
  try {
    if (signal?.aborted) {
      await reader.cancel().catch(() => {});
      throw fault('BPAY_NEXT_FROZEN_DEPENDENCY_TIMEOUT', 503, true);
    }
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > maximum) {
        await reader.cancel().catch(() => {});
        throw fault(tooLarge, request ? 413 : 503);
      }
      parts.push(decoder.decode(value, { stream: true }));
    }
    parts.push(decoder.decode());
    try { return JSON.parse(parts.join('')); } catch { throw fault(code, status); }
  } catch (error) {
    if (error instanceof FrozenReadError) throw error;
    throw fault(code, status);
  } finally {
    signal?.removeEventListener('abort', cancel);
    reader.releaseLock();
  }
}

function pageCall(body) {
  const invalid = () => { throw fault('BPAY_NEXT_FROZEN_REQUEST_INVALID', 400); };
  if (!record(body) || !Object.hasOwn(SCHEMAS, body.kind)) invalid();
  const limit = body.limit ?? 50;
  if (!integer(limit, 1) || limit > BPAY_NEXT_FROZEN_READ_LIMITS.pageRows) invalid();
  const args = { p_limit: limit };
  let allowed;
  if (body.kind === 'RATE_SCHEDULE') {
    allowed = ['kind', 'run_work_id', 'after_rate_family', 'after_rate_code', 'limit'];
    if (!field(body.run_work_id, 'uuid')) invalid();
    const family = body.after_rate_family ?? null;
    const code = body.after_rate_code ?? null;
    if ((family === null) !== (code === null)
      || (family !== null && (!field(family, 'family') || !field(code, 'code')))) invalid();
    args.p_run_work_id = body.run_work_id;
    args.p_after_rate_family = family;
    args.p_after_rate_code = code;
  } else {
    if (!field(body.run_line_id, 'uuid')) invalid();
    args.p_run_line_id = body.run_line_id;
    if (body.kind === 'SHIFTS' || body.kind === 'BREAKS') {
      const key = body.kind === 'SHIFTS' ? 'after_detail_no' : 'after_break_no';
      allowed = ['kind', 'run_line_id', key, 'limit'];
      const after = body[key] ?? null;
      if (after !== null && !integer(after)) invalid();
      args[`p_${key}`] = after;
      if (body.kind === 'BREAKS') {
        allowed.push('shift_detail_id');
        if (!field(body.shift_detail_id, 'uuid')) invalid();
        args.p_shift_detail_id = body.shift_detail_id;
      }
    } else {
      allowed = ['kind', 'run_line_id', 'after_bucket', 'limit'];
      const after = body.after_bucket ?? null;
      if (after !== null && !field(after, 'bucket')) invalid();
      args.p_after_bucket = after;
    }
  }
  // Actor, candidate, Timesheet and arbitrary RPC names are never body fields.
  if (Object.keys(body).some((key) => !allowed.includes(key))) invalid();
  return { kind: body.kind, args, limit };
}

function nextCursor(kind, row) {
  switch (kind) {
    case 'SHIFTS': return { after_detail_no: row.detail_no };
    case 'BREAKS': return { after_break_no: row.break_no };
    case 'RATE_DETAILS': return { after_bucket: row.bucket };
    case 'RATE_SCHEDULE': return { after_rate_family: row.rate_family, after_rate_code: row.rate_code };
  }
}

function pageRows(value, call) {
  const invalid = () => { throw fault('BPAY_NEXT_FROZEN_RESPONSE_INVALID'); };
  if (!Array.isArray(value) || value.length > call.limit) invalid();
  const schema = SCHEMAS[call.kind];
  const keys = Object.keys(schema);
  const seen = new Set();
  let previous = call.args.p_after_detail_no ?? call.args.p_after_break_no ?? 0;
  let previousBucket = call.args.p_after_bucket ?? '';
  for (const row of value) {
    if (!record(row) || Object.keys(row).length !== keys.length
      || keys.some((key) => !Object.hasOwn(row, key) || !field(row[key], schema[key]))) invalid();
    const cursor = nextCursor(call.kind, row);
    const cursorKey = JSON.stringify(cursor);
    if (seen.has(cursorKey)) invalid();
    seen.add(cursorKey);
    if (call.kind === 'SHIFTS' || call.kind === 'BREAKS') {
      const ordinal = row.detail_no ?? row.break_no;
      if (ordinal <= previous) invalid();
      previous = ordinal;
      const prefix = call.kind === 'SHIFTS' ? 'shift' : 'break';
      const start = row[`${prefix}_start_at`], end = row[`${prefix}_end_at`];
      const localStart = row[`${prefix}_start_local`], localEnd = row[`${prefix}_end_local`];
      // PostgreSQL enforces endpoint order at microsecond precision. JS Date
      // truncates to milliseconds; do not reprice/rewrite or reject that detail.
      if ((start === null) !== (end === null) || (localStart === null) !== (localEnd === null)) invalid();
      if (call.kind === 'SHIFTS' && row.approved_minutes === null
        && row.approved_hours === null && start === null && localStart === null) invalid();
      if (call.kind === 'SHIFTS') {
        const facts = ['segment_pay_ex_vat', 'pay_excluded', 'hours_day',
          'hours_night', 'hours_sat', 'hours_sun', 'hours_bh'];
        const present = facts.filter((key) => row[key] !== null).length;
        if (present !== 0 && present !== facts.length) invalid();
        if (present !== 0) {
          for (const key of facts.slice(2)) {
            if (row[key].startsWith('-') && !/^-0(?:\.0{1,6})?$/.test(row[key])) invalid();
          }
          if (row.pay_excluded && !/^-?0(?:\.0{1,2})?$/.test(row.segment_pay_ex_vat)) invalid();
        }
      }
    } else if (call.kind === 'RATE_DETAILS') {
      if (row.bucket <= previousBucket) invalid();
      previousBucket = row.bucket;
    } else {
      // Text tuple order belongs to PostgreSQL's collation, not JS localeCompare.
      if (row.rate_family === call.args.p_after_rate_family
        && row.rate_code === call.args.p_after_rate_code) invalid();
      if ((row.rate_family === 'STANDARD' && !BUCKETS.includes(row.rate_code))
        || (row.rate_family === 'MILEAGE' && row.rate_code !== 'MILE')) invalid();
    }
  }
  return value;
}

function capturedOrigin(value) {
  const keys=Object.keys(ORIGIN_SCHEMA);
  if(!record(value)||Object.keys(value).length!==keys.length
    ||keys.some(key=>!Object.hasOwn(value,key)||!field(value[key],ORIGIN_SCHEMA[key]))
    ||(value.origin_kind==='DAILY'&&(value.sheet_scope!=='DAILY'||value.contract_id!==null||value.work_date===null))
    ||(value.origin_kind==='CONTRACT'&&value.contract_id===null)
    ||((value.classification==='CLASSIFIED')!==(value.client_id!==null)))throw fault('BPAY_NEXT_FROZEN_RESPONSE_INVALID');
  return value;
}

function dependencyError(error) {
  if (error instanceof FrozenReadError) return error;
  const db = error?.json ?? error;
  if (db?.code === 'PGRST202') return fault('BPAY_NEXT_FROZEN_DEPENDENCY_UNAVAILABLE', 503, true);
  if (db?.code === 'P0002') return fault('BPAY_NEXT_FROZEN_DETAIL_NOT_FOUND', 404);
  if (['BPAY_NEXT_FROZEN_DETAIL_NOT_AVAILABLE', 'BPAY_NEXT_RATE_SCHEDULE_NOT_AVAILABLE',
    'BPAY_NEXT_BREAK_SHIFT_NOT_IN_FROZEN_LINE'].includes(db?.message)) {
    return fault('BPAY_NEXT_FROZEN_DETAIL_NOT_AVAILABLE', 409);
  }
  if (db?.code === '42501' || [401, 403].includes(error?.status)) return fault('BPAY_NEXT_FROZEN_FORBIDDEN', 403);
  if (error?.name === 'AbortError' || error?.status === 408) return fault('BPAY_NEXT_FROZEN_DEPENDENCY_TIMEOUT', 503, true);
  return fault('BPAY_NEXT_FROZEN_DEPENDENCY_UNAVAILABLE', 503, true);
}

/**
 * Read one page only; caller requests later pages separately. CORS and route
 * dispatch remain with the existing broker. A full page returns a continuation
 * cursor; an empty final page is valid and no total-population scan is needed.
 * @param {Request} request
 * @param {Partial<FrozenReadDependencies>} dependencies
 * @returns {Promise<Response>}
 */
// Closed frozen facts only. BigInt checks conservation; it never quotes/reprices.
function frozenSelectedTreatment(value, runLineId) {
  if (value === null) return true;
  const exactKeys = (v, keys) => record(v) && Object.keys(v).length === keys.length && keys.every(k => Object.hasOwn(v,k));
  const decimal = v => typeof v === 'string' && /^(?:0|[1-9]\d{0,11})(?:\.\d{1,6})?$/.test(v);
  const quoteRate = v => typeof v === 'string' && v.length<=120000 && /^(?:0|[1-9]\d*)(?:\.\d{1,6})?$/.test(v);
  const zero = v => typeof v==='string' && /^0(?:\.0+)?$/.test(v);
  const money = v => typeof v === 'string' && /^(?:0|[1-9]\d{0,15})(?:\.\d{1,2})?$/.test(v);
  const cents = v => { const [w,f=''] = v.split('.'); return BigInt(w)*100n+BigInt(f.padEnd(2,'0')); };
  const quote = (v, rate) => exactKeys(v,rate?['ex','vat','inc','rate']:['ex','vat','inc'])
    && ['ex','vat','inc'].every(k=>money(v[k])) && cents(v.ex)+cents(v.vat)===cents(v.inc)
    && (!rate || v.rate===null || quoteRate(v.rate));
  const keys=['run_line_id','run_work_id','run_worker_id','candidate_id','captured_revision_id','approved_line_id','component_key',
    'source_pay_channel','target_pay_channel','target_umbrella_id','target_umbrella_vat_chargeable','valuation_policy_id','valuation_window_id',
    'valuation_date','source_consumed_ex_vat','component_quote','buckets'];
  if (!exactKeys(value,keys) || !keys.slice(0,6).every(k=>field(value[k],'uuid'))
    || typeof runLineId!=='string' || value.run_line_id.toLowerCase()!==runLineId.toLowerCase()
    || typeof value.component_key!=='string' || value.component_key.includes('\0') || [...value.component_key].length<1 || [...value.component_key].length>256
    || !['PAYE','UMBRELLA'].includes(value.source_pay_channel) || !['PAYE','UMBRELLA'].includes(value.target_pay_channel)
    || !field(value.valuation_policy_id,'uuid') || !field(value.valuation_window_id,'uuid') || !field(value.valuation_date,'date')
    || !money(value.source_consumed_ex_vat) || !quote(value.component_quote,false)
    || !Array.isArray(value.buckets) || value.buckets.length<1 || value.buckets.length>5) return false;
  if (value.target_pay_channel==='PAYE'
    ? value.target_umbrella_id!==null || value.target_umbrella_vat_chargeable!==null
    : !field(value.target_umbrella_id,'uuid') || typeof value.target_umbrella_vat_chargeable!=='boolean') return false;
  const bucketKeys=['rate_family','rate_code','decision_id','decision_version','treatment','resolution_mode','approved_units','included_units','source_rate',
    'source_ex_vat','manual_replacement_rate','manual_amount_ex_vat','quote','valuation_policy_id','valuation_window_id'];
  const seen=new Set(); let ex=0n, vat=0n, inc=0n;
  for (const b of value.buckets) {
    if (!exactKeys(b,bucketKeys) || !['STANDARD','ADDITIONAL','ADJUSTMENT'].includes(b.rate_family)
      || typeof b.rate_code!=='string' || b.rate_code.includes('\0') || [...b.rate_code].length<1 || [...b.rate_code].length>(b.rate_family==='ADDITIONAL'?245:256)
      || (b.rate_family==='STANDARD' && !['BH','DAY','NIGHT','SAT','SUN'].includes(b.rate_code))
      || !['PINNED','NATIVE_DEFAULT'].includes(b.treatment)
      || !['SUGGESTED_EQUIVALENT_BASIS','MANUAL_REPLACEMENT_RATE','MANUAL_AMOUNT'].includes(b.resolution_mode)
      || !['approved_units','included_units','source_rate','manual_replacement_rate'].every(k=>b[k]===null||decimal(b[k]))
      || !['source_ex_vat','manual_amount_ex_vat'].every(k=>b[k]===null||money(b[k])) || !quote(b.quote,true)
      || !field(b.valuation_policy_id,'uuid') || !field(b.valuation_window_id,'uuid')
      || b.valuation_policy_id.toLowerCase()!==value.valuation_policy_id.toLowerCase()
      || b.valuation_window_id.toLowerCase()!==value.valuation_window_id.toLowerCase()) return false;
    if (b.treatment==='PINNED'
      ? !field(b.decision_id,'uuid') || typeof b.decision_version!=='string' || !/^[1-9]\d{0,18}$/.test(b.decision_version) || BigInt(b.decision_version)>9223372036854775807n
      : b.decision_id!==null || b.decision_version!==null || b.rate_family!=='STANDARD' || b.resolution_mode!=='SUGGESTED_EQUIVALENT_BASIS') return false;
    if (b.resolution_mode==='SUGGESTED_EQUIVALENT_BASIS'
      ? b.manual_replacement_rate!==null || b.manual_amount_ex_vat!==null
      : b.resolution_mode==='MANUAL_REPLACEMENT_RATE'
        ? b.manual_replacement_rate===null || b.manual_amount_ex_vat!==null
        : b.manual_amount_ex_vat===null || b.manual_replacement_rate!==null || b.quote.rate!==null) return false;
    const key=JSON.stringify([b.rate_family,b.rate_code]); if(seen.has(key))return false;seen.add(key);
    ex+=cents(b.quote.ex);vat+=cents(b.quote.vat);inc+=cents(b.quote.inc);
  }
  if(zero(value.source_consumed_ex_vat) && (ex!==0n || vat!==0n || inc!==0n || value.buckets.some(b=>!zero(b.included_units)||!zero(b.source_ex_vat)||b.quote.rate!==null)))return false;
  return ex===cents(value.component_quote.ex) && vat===cents(value.component_quote.vat) && inc===cents(value.component_quote.inc)
    && (value.target_pay_channel==='UMBRELLA' && value.target_umbrella_vat_chargeable || vat===0n);
}

// Original advice owns instruction admission; reuse only native detail rows/cursors.
export function originalAdviceDetailCall(body) {
  return pageCall(body);
}
export function validateOriginalAdviceDetailPage(payload, body) {
  const call=pageCall(body), keys=call.kind==='RATE_DETAILS'?['rows','complete','selected_treatment']:['rows','complete'];
  if(!record(payload)||Object.keys(payload).length!==keys.length||keys.some(k=>!Object.hasOwn(payload,k))
    ||typeof payload.complete!=='boolean'||(call.kind==='RATE_DETAILS'&&!frozenSelectedTreatment(payload.selected_treatment,call.args.p_run_line_id)))
    throw fault('BPAY_NEXT_FROZEN_RESPONSE_INVALID');
  const rows=pageRows(payload.rows,call);
  if(!payload.complete&&rows.length===0)throw fault('BPAY_NEXT_FROZEN_RESPONSE_INVALID');
  const result={ok:true,kind:call.kind,rows,complete:payload.complete,next_cursor:payload.complete?null:nextCursor(call.kind,rows.at(-1))};
  if(call.kind==='RATE_DETAILS')result.selected_treatment=payload.selected_treatment;
  if(ENCODER.encode(JSON.stringify(result)).byteLength>BPAY_NEXT_FROZEN_READ_LIMITS.responseBytes)throw fault('BPAY_NEXT_FROZEN_PAGE_TOO_LARGE');
  return result;
}

export async function handleBpayNextFrozenRead(request, dependencies = {}) {
  if (request.method !== 'POST') return json(405, { ok: false, error_code: 'BPAY_NEXT_FROZEN_METHOD_NOT_ALLOWED', retryable: false });
  let controller, timer;
  try {
    if (typeof dependencies.requireOfficeUser !== 'function') throw fault('BPAY_NEXT_FROZEN_DEPENDENCY_UNAVAILABLE', 503, true);
    const user = await dependencies.requireOfficeUser(request, ['admin']);
    if (!user || !field(user.id, 'uuid')) throw fault('BPAY_NEXT_FROZEN_UNAUTHORIZED', 401);
    if (user.role !== 'admin') throw fault('BPAY_NEXT_FROZEN_FORBIDDEN', 403);
    const call = pageCall(await limitedJson(request, BPAY_NEXT_FROZEN_READ_LIMITS.requestBytes, true));
    if (typeof dependencies.frozenPageRpc !== 'function') throw fault('BPAY_NEXT_FROZEN_DEPENDENCY_UNAVAILABLE', 503, true);
    controller = new AbortController();
    const timeout = new Promise((resolve, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(fault('BPAY_NEXT_FROZEN_DEPENDENCY_TIMEOUT', 503, true));
      }, BPAY_NEXT_FROZEN_READ_LIMITS.timeoutMs);
    });
    const read = async () => {
      const response = await dependencies.frozenPageRpc(call.kind, call.args, {
        actorUserId: user.id,
        signal: controller.signal,
        timeoutMs: BPAY_NEXT_FROZEN_READ_LIMITS.timeoutMs,
        maxResponseBytes: BPAY_NEXT_FROZEN_READ_LIMITS.responseBytes
      });
      if (!(response instanceof Response)) throw fault('BPAY_NEXT_FROZEN_RESPONSE_INVALID');
      const payload = await limitedJson(response, BPAY_NEXT_FROZEN_READ_LIMITS.responseBytes, false, controller.signal);
      if (!response.ok) throw dependencyError({ status: response.status, json: payload });
      if (!record(payload) || Object.keys(payload).length !== (call.kind==='RATE_DETAILS'?5:4)
        || !Object.hasOwn(payload, 'rows') || !Object.hasOwn(payload,'origin') || !Object.hasOwn(payload,'used_line') || typeof payload.complete !== 'boolean') {
        throw fault('BPAY_NEXT_FROZEN_RESPONSE_INVALID');
      }
      if(call.kind==='RATE_DETAILS'&&(!Object.hasOwn(payload,'selected_treatment')||!frozenSelectedTreatment(payload.selected_treatment,call.args.p_run_line_id)))throw fault('BPAY_NEXT_FROZEN_RESPONSE_INVALID');
      const used=payload.used_line,usedKeys=Object.keys(USED_LINE_SCHEMA);
      if(call.kind==='RATE_DETAILS' ? !record(used)||Object.keys(used).length!==usedKeys.length
        ||usedKeys.some(key=>!Object.hasOwn(used,key)||!field(used[key],USED_LINE_SCHEMA[key])) : used!==null)throw fault('BPAY_NEXT_FROZEN_RESPONSE_INVALID');
      const rows = pageRows(payload.rows, call);
      const complete = payload.complete;
      if (!complete && rows.length === 0) throw fault('BPAY_NEXT_FROZEN_RESPONSE_INVALID');
      const result = {
        ok: true, kind: call.kind, rows, complete, origin: capturedOrigin(payload.origin), used_line:used,
        next_cursor: complete ? null : nextCursor(call.kind, rows.at(-1))
      };
      if(call.kind==='RATE_DETAILS')result.selected_treatment=payload.selected_treatment;
      if (ENCODER.encode(JSON.stringify(result)).byteLength > BPAY_NEXT_FROZEN_READ_LIMITS.responseBytes) {
        throw fault('BPAY_NEXT_FROZEN_PAGE_TOO_LARGE');
      }
      return result;
    };
    return json(200, await Promise.race([read(), timeout]));
  } catch (error) {
    const safe = dependencyError(error);
    return json(safe.status, { ok: false, error_code: safe.code, retryable: safe.retryable });
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
