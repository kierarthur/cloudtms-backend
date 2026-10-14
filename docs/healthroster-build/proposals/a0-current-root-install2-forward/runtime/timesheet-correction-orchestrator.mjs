/**
 * Service-only correction orchestration. No routes, persistence, transport,
 * calculators, notifications, or storage bindings live in this module.
 * Native SQL owns each complete financial unit and all mutation admission.
 * @typedef {{actor_user_id:string, environment:'TEST'}} CorrectionActor
 * @typedef {{request_id:string, expected_state_revision:number}} RequestAction
 * @typedef {{contract_version?:'TIMESHEET_CORRECTION_REQUEST_AUTHORITY_V1',request_id:string,state:'WAITING'|'APPLIED'|'CANCELLED',state_revision:number|string,
 * replacement_timesheet_id:string,return_invoice_id:string|null}} RequestAuthority
 * @typedef {{assertActor:(context:unknown,action:string)=>Promise<CorrectionActor>,
 * rpc:(name:string,args:object)=>Promise<object>,
 * buildReplacementSnapshot?:(input:{prepare:object,actor:CorrectionActor})=>Promise<object>,
 * buildOriginalSnapshot?:(input:{prepare:object,actor:CorrectionActor})=>Promise<object>,
 * readRequestAuthority?:(input:{requestId:string,actor:CorrectionActor})=>Promise<RequestAuthority>,
 * resolveLifecycleItem?:(input:{item:object,timesheetId:string,selectedTimesheetIds:string[],action:string,actor:CorrectionActor})=>Promise<object|null>,
 * runOrdinaryLifecycle?:(input:{items:object[],action:string,actor:CorrectionActor})=>Promise<object>,
 * deleteCommittedTimesheetR2Keys?:(applyResult:object,options:{deleteOperationId:string,requestedTimesheetId:string})=>Promise<object>
 * }} CorrectionDependencies
 */

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const HASH = /^[0-9a-f]{64}$/;
const CORRECTION_ID = /^awc:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}):([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/;
const STATES = new Set(['WAITING', 'APPLIED', 'CANCELLED']);
const ORIGINS = new Set(['SIGNED_TIMESHEET_CORRECTION', 'AGENCY_INVOICE_CORRECTION']);
const MAX_ITEMS = 100;
const MAX_JSON_NODES = 50000;
const OWN = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const TOP_SIGNED = new Set([
  'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh', 'total_hours',
  'total_pay_ex_vat', 'total_charge_ex_vat', 'margin_ex_vat',
  'pay_vat_amount_snapshot', 'pay_total_inc_vat_snapshot',
  'expenses_pay_ex_vat', 'expenses_charge_ex_vat', 'travel_pay_ex_vat', 'travel_charge_ex_vat',
  'accommodation_pay_ex_vat', 'accommodation_charge_ex_vat', 'other_pay_ex_vat', 'other_charge_ex_vat',
  'mileage_units', 'mileage_pay_ex_vat', 'mileage_charge_ex_vat',
  'additional_pay_ex_vat', 'additional_charge_ex_vat', 'additional_margin_ex_vat'
]);
const COMPONENT_SIGNED = new Set([
  'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh', 'hours_total', 'total_hours',
  'pay_amount', 'charge_amount', 'pay_ex_vat', 'charge_ex_vat', 'margin_ex_vat',
  'total_pay_ex_vat', 'total_charge_ex_vat', 'unit_count', 'units', 'total_units', 'units_total',
  // Requires the paired recursive and SEGMENTS SQL guard extension described in the handoff.
  'pay_cost_ex_vat_including_erni', 'vat_amount', 'total_inc_vat'
]);
const BASE_BUCKETS = new Set(['day', 'night', 'sat', 'sun', 'bh']);
const COMPONENT_LOCKS = new Set(['invoice_locked_invoice_id', 'invoice_locked_at_utc', 'pay_locked', 'pay_locked_at_utc']);
const ROW_METADATA = new Set(['id', 'is_current', 'created_at', 'updated_at', 'computed_at_utc']);
const ROW_LIFECYCLE = new Set([
  'authorised_at_utc', 'authorised_by_user_id', 'paid_at_utc', 'paid_by_user_id', 'payment_reference',
  'locked_by_invoice_id', 'locked_at_utc', 'unlocked_by_credit_note_id',
  'remittance_last_sent_at_utc', 'remittance_send_count', 'processed_at_utc', 'processed_by_user_id',
  'pay_on_hold', 'pay_on_hold_reason', 'pay_on_hold_since_utc'
]);
const SEGMENT_KEYS = new Set([
  'row_key', 'segment_id', 'date', 'work_date', 'start', 'end', 'start_time', 'end_time',
  'worked_start_iso', 'worked_end_iso', 'start_utc', 'end_utc', 'ref_num', 'reference',
  'booking_reference', 'band', 'break_entry'
]);
const TS_ALIASES = [
  'timesheet_id', 'timesheetId', 'current_timesheet_id', 'currentTimesheetId',
  'requested_timesheet_id', 'requestedTimesheetId'
];
const ITEM_KEYS = new Set([...TS_ALIASES, 'row_key', 'expected_timesheet_id', 'expected_version', 'expected_row_signature']);

export class CorrectionOrchestrationError extends Error {
  constructor(code, status = 409, details = {}) {
    super(code);
    this.name = 'CorrectionOrchestrationError';
    this.code = code;
    this.status = status;
    // Only bounded identifiers/codes belong here. No snapshots or raw source values.
    this.details = Object.freeze({ ...details });
  }
}
function fail(code, status = 409, details) { throw new CorrectionOrchestrationError(code, status, details); }
function object(value, code = 'CORRECTION_PAYLOAD_INVALID') {
  if (!value || typeof value !== 'object' || Array.isArray(value) ||
      ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail(code, 400);
  return value;
}
function closed(value, keys) {
  object(value);
  if (Object.keys(value).some(k => !keys.has(k))) fail('CORRECTION_PAYLOAD_FIELD_NOT_ALLOWED', 400);
}
function uuid(value, code = 'CORRECTION_PAYLOAD_INVALID') {
  if (typeof value !== 'string' || !UUID.test(value)) fail(code, 400);
  return value.toLowerCase();
}
function sameUuid(a, b) { return typeof a === 'string' && typeof b === 'string' && a.toLowerCase() === b.toLowerCase(); }
function integer(value, code = 'CORRECTION_PAYLOAD_INVALID') {
  if (!Number.isSafeInteger(value) || value < 1) fail(code, 400);
  return value;
}
function hash(value) { if (typeof value !== 'string' || !HASH.test(value)) fail('CORRECTION_RECEIPT_INVALID'); }
function positive(value) { return typeof value === 'number' && Number.isSafeInteger(value) && value > 0; }
function correctionIdentity(value, requestId, rootId = null) {
  const match = typeof value === 'string' ? CORRECTION_ID.exec(value) : null;
  if (!match || !sameUuid(match[2], requestId) || (rootId !== null && !sameUuid(match[1], rootId))) {
    fail('CORRECTION_RECEIPT_IDENTITY_MISMATCH');
  }
  return value;
}
function cloneJson(value) {
  let nodes = 0;
  const visit = (v, depth) => {
    if (++nodes > MAX_JSON_NODES || depth > 32) fail('CORRECTION_JSON_BOUND_EXCEEDED', 400);
    if (v === null || typeof v === 'boolean') return v;
    if (typeof v === 'string') {
      if (v.length > 262144) fail('CORRECTION_JSON_BOUND_EXCEEDED', 400);
      return v;
    }
    if (typeof v === 'number') {
      if (!Number.isFinite(v) || Math.abs(v) > Number.MAX_SAFE_INTEGER) fail('CORRECTION_FINANCIAL_NUMBER_UNSAFE');
      return v;
    }
    if (Array.isArray(v)) return v.map(x => visit(x, depth + 1));
    object(v);
    const out = Object.create(null);
    for (const [k, x] of Object.entries(v)) {
      if (['__proto__', 'constructor', 'prototype'].includes(k)) fail('CORRECTION_JSON_KEY_INVALID', 400);
      out[k] = visit(x, depth + 1);
    }
    return out;
  };
  return visit(value, 0);
}
function freezeJson(v) {
  if (v && typeof v === 'object') { Object.values(v).forEach(freezeJson); Object.freeze(v); }
  return v;
}
function negate(value) {
  if (value === null) return null;
  if (typeof value === 'number') return value === 0 ? 0 : -value;
  if (typeof value !== 'string' || !/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(value)) {
    fail('CORRECTION_REVERSAL_FACT_INVALID');
  }
  // Sign change only: preserve exact decimal precision, exponent and trailing zeroes.
  if (value.startsWith('-')) return value.slice(1);
  return `-${value.startsWith('+') ? value.slice(1) : value}`;
}
function componentSigned(path) {
  const key = path.at(-1), parent = path.at(-2);
  return COMPONENT_SIGNED.has(key) || parent === 'days' || (parent === 'base_hours' && BASE_BUCKETS.has(key));
}
function unknownEconomicFact(key) {
  return /(?:^|_)(?:hours?|units?|amount|ex_vat|inc_vat)(?:_|$)/i.test(key) &&
    !/(?:rate|pct|percent|id|key|name|kind|type|code|date|iso)/i.test(key);
}
function reverseComponents(value, path = []) {
  if (Array.isArray(value)) return value.map((v, i) => reverseComponents(v, [...path, String(i)]));
  if (value && typeof value === 'object') {
    const out = Object.create(null);
    for (const [k, v] of Object.entries(value)) {
      if (COMPONENT_LOCKS.has(k)) continue;
      if (k === 'is_reversal') { out[k] = true; continue; }
      out[k] = reverseComponents(v, [...path, k]);
    }
    return out;
  }
  if (componentSigned(path)) return negate(value);
  if (value !== null && (typeof value === 'number' || typeof value === 'string') && unknownEconomicFact(path.at(-1) || '')) {
    fail('CORRECTION_REVERSAL_UNSUPPORTED_FACT');
  }
  return value;
}
function snapshotIdentity(snapshot, expected, { original = false, canonicalInput = false } = {}) {
  object(snapshot, 'CORRECTION_CANONICAL_SNAPSHOT_INVALID');
  if (!sameUuid(snapshot.timesheet_id, expected.timesheetId) || snapshot.timesheet_version !== expected.version ||
      !sameUuid(snapshot.candidate_id, expected.candidateId) || !sameUuid(snapshot.client_id, expected.clientId) ||
      (snapshot.is_stale !== false && !(canonicalInput && !OWN(snapshot, 'is_stale'))) || snapshot.has_rate_issue !== false || snapshot.has_pay_channel_issue !== false ||
      typeof snapshot.pay_method !== 'string' || !snapshot.pay_method.trim() ||
      !snapshot.invoice_breakdown_json || typeof snapshot.invoice_breakdown_json !== 'object' || Array.isArray(snapshot.invoice_breakdown_json)) {
    fail('CORRECTION_CANONICAL_SNAPSHOT_INVALID');
  }
  if (!['AGGREGATE', 'SEGMENTS'].includes(snapshot.invoice_breakdown_json.mode)) fail('CORRECTION_REVERSAL_COMPONENT_MODE_UNSUPPORTED');
  if (snapshot.invoice_breakdown_json.mode === 'SEGMENTS' && !Array.isArray(snapshot.invoice_breakdown_json.segments)) {
    fail('CORRECTION_REVERSAL_COMPONENT_SHAPE_MISMATCH');
  }
  if (!original && ['authorised_at_utc', 'paid_at_utc', 'locked_by_invoice_id'].some(k => snapshot[k] != null && snapshot[k] !== '')) {
    fail('CORRECTION_CANONICAL_SNAPSHOT_INVALID');
  }
}

/**
 * Clone exact frozen T0 economics; this is a sign operation, never a calculator.
 * Unknown quantity/money facts refuse rather than silently create an incomplete reversal.
 * @param {object} originalSnapshot Exact authoritative existing TSFIN JSON.
 * @param {{timesheetId:string,version:number,candidateId:string,clientId:string}} target
 */
export function cloneFrozenCorrectionReversal(originalSnapshot, target) {
  const source = cloneJson(originalSnapshot);
  uuid(source.timesheet_id, 'CORRECTION_CANONICAL_SNAPSHOT_INVALID'); integer(source.timesheet_version, 'CORRECTION_CANONICAL_SNAPSHOT_INVALID');
  uuid(target.timesheetId); integer(target.version); uuid(target.candidateId); uuid(target.clientId);
  snapshotIdentity(source, {
    timesheetId: source.timesheet_id, version: source.timesheet_version,
    candidateId: target.candidateId, clientId: target.clientId
  }, { original: true });
  if (sameUuid(source.timesheet_id, target.timesheetId) || target.version !== 1) fail('CORRECTION_REVERSAL_IDENTITY_INVALID');
  const result = cloneJson(source);
  for (const k of Object.keys(result)) {
    if (ROW_METADATA.has(k) || ROW_LIFECYCLE.has(k)) delete result[k];
    else if (TOP_SIGNED.has(k)) result[k] = negate(result[k]);
    else if (!['invoice_breakdown_json', 'additional_units_json'].includes(k) && result[k] !== null && unknownEconomicFact(k)) {
      // Historical rates, clock/break facts and description/evidence are identities.
      if (!['break_minutes', 'expenses_description', 'mileage_description'].includes(k) &&
          !k.endsWith('_evidence_r2_key')) fail('CORRECTION_REVERSAL_UNSUPPORTED_FACT');
    }
  }
  result.invoice_breakdown_json = reverseComponents(source.invoice_breakdown_json);
  if (result.invoice_breakdown_json.mode === 'SEGMENTS') {
    for (const segment of result.invoice_breakdown_json.segments) {
      object(segment, 'CORRECTION_REVERSAL_COMPONENT_SHAPE_MISMATCH');
      segment.is_reversal = true;
    }
  }
  if (OWN(source, 'additional_units_json')) result.additional_units_json = reverseComponents(source.additional_units_json, ['additional_units_json']);
  result.timesheet_id = target.timesheetId.toLowerCase();
  result.timesheet_version = 1;
  result.processing_status = 'PENDING_AUTH';
  snapshotIdentity(result, target);
  return result;
}

function actionPayload(payload) {
  closed(payload, new Set(['request_id', 'expected_state_revision']));
  return { request_id: uuid(payload.request_id), expected_state_revision: integer(payload.expected_state_revision) };
}
function sendPayload(payload) {
  closed(payload, new Set(['timesheet_id', 'expected_version', 'proposal', 'reason', 'request_key']));
  uuid(payload.timesheet_id); integer(payload.expected_version);
  if (typeof payload.reason !== 'string' || !payload.reason.trim() || payload.reason.trim().length > 1000 ||
      typeof payload.request_key !== 'string' || !payload.request_key.trim() || payload.request_key.length > 200) {
    fail('CORRECTION_PAYLOAD_INVALID', 400);
  }
  closed(payload.proposal, new Set(['schedule', 'changed_dates']));
  if (!Array.isArray(payload.proposal.schedule) || payload.proposal.schedule.length > MAX_ITEMS) fail('CORRECTION_PAYLOAD_INVALID', 400);
  for (const s of payload.proposal.schedule) {
    closed(s, SEGMENT_KEYS);
    for (const [k, v] of Object.entries(s)) {
      if (k === 'break_entry') {
        object(v);
        const keys = v.kind === 'NO_BREAK' ? ['kind', 'no_break', 'break_minutes'] : v.kind === 'DURATION_MINUTES'
          ? ['kind', 'break_minutes'] : v.kind === 'START_END_TIMES'
            ? ['kind', 'break_start', 'break_end', 'calculated_break_minutes'] : null;
        if (!keys) fail('CORRECTION_PAYLOAD_INVALID', 400);
        closed(v, new Set(keys));
        if (v.kind === 'NO_BREAK' && (v.break_minutes !== 0 || v.no_break !== true)) fail('CORRECTION_PAYLOAD_INVALID', 400);
        if (v.kind === 'DURATION_MINUTES' && (!Number.isInteger(v.break_minutes) || v.break_minutes < 1 || v.break_minutes > 1440)) fail('CORRECTION_PAYLOAD_INVALID', 400);
        if (v.kind === 'START_END_TIMES' &&
            (!/^([01]\d|2[0-3]):[0-5]\d$/.test(v.break_start || '') || !/^([01]\d|2[0-3]):[0-5]\d$/.test(v.break_end || '') ||
             !Number.isInteger(v.calculated_break_minutes) || v.calculated_break_minutes < 1 || v.calculated_break_minutes > 1440)) {
          fail('CORRECTION_PAYLOAD_INVALID', 400);
        }
      } else if (v !== null && (typeof v !== 'string' || v.length > 1000)) fail('CORRECTION_PAYLOAD_INVALID', 400);
    }
  }
  if (OWN(payload.proposal, 'changed_dates') &&
      (!Array.isArray(payload.proposal.changed_dates) || payload.proposal.changed_dates.length > MAX_ITEMS ||
       payload.proposal.changed_dates.some(d => typeof d !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(d)))) {
    fail('CORRECTION_PAYLOAD_INVALID', 400);
  }
  return cloneJson(payload); // Do not rewrite the immutable SEND fingerprint/reason/proposal.
}
function rpcSuccess(value) {
  object(value, 'CORRECTION_RPC_RECEIPT_INVALID');
  if (value.ok !== true || value.success === false || value.all_success === false ||
      (Array.isArray(value.errors) && value.errors.length) || (typeof value.fail_count === 'number' && value.fail_count !== 0)) {
    const rawCode = value.code || value.error_code || value.error?.code;
    fail(typeof rawCode === 'string' && /^[A-Z][A-Z0-9_]{1,100}$/.test(rawCode) ? rawCode : 'CORRECTION_NATIVE_OWNER_FAILED');
  }
  return value;
}
function safeFailureCode(error) {
  if (error instanceof CorrectionOrchestrationError) return error.code;
  for (const code of [error?.business_code, error?.error_code, error?.code, error?.message]) {
    if (typeof code === 'string' && /^[A-Z][A-Z0-9_]{1,100}$/.test(code)) return code;
  }
  return 'CORRECTION_NATIVE_OWNER_FAILED';
}
function terminalRevision(receipt, payload, replayAllowed) {
  if (!sameUuid(receipt.request_id, payload.request_id) || !positive(receipt.state_revision) ||
      !(receipt.state_revision === payload.expected_state_revision ||
        (replayAllowed && receipt.replayed === true && receipt.state_revision === payload.expected_state_revision + 1))) {
    fail('CORRECTION_REQUEST_CAS_STALE');
  }
}
function appliedReceipt(value, payload, prepare = null) {
  const r = rpcSuccess(value);
  if (r.state !== 'APPLIED') fail('CORRECTION_REQUEST_NOT_APPLIED');
  for (const k of ['request_id', 'reversed_timesheet_id', 'reversal_timesheet_id', 'replacement_timesheet_id',
    'original_financial_id', 'reversal_financial_id', 'replacement_financial_id']) uuid(r[k], 'CORRECTION_RECEIPT_INVALID');
  correctionIdentity(r.correction_id, r.request_id, prepare?.authority?.root_timesheet_id ?? null);
  if (new Set([r.reversed_timesheet_id, r.reversal_timesheet_id, r.replacement_timesheet_id].map(x => x.toLowerCase())).size !== 3 ||
      new Set([r.original_financial_id, r.reversal_financial_id, r.replacement_financial_id].map(x => x.toLowerCase())).size !== 3 ||
      typeof r.original_authorisation_required !== 'boolean' ||
      (OWN(r, 'original_materialised_in_unit') && typeof r.original_materialised_in_unit !== 'boolean')) fail('CORRECTION_RECEIPT_INVALID');
  for (const role of ['original', 'reversal', 'replacement']) {
    hash(r[`${role}_financial_authority_hash`]); hash(r[`${role}_timesheet_authority_hash`]);
  }
  hash(r.prepare_fingerprint);
  if (!Array.isArray(r.ordinary_notification_timesheet_ids) || r.ordinary_notification_timesheet_ids.length !== 1 ||
      !sameUuid(r.ordinary_notification_timesheet_ids[0], r.replacement_timesheet_id)) fail('CORRECTION_NOTIFICATION_SCOPE_INVALID');
  if (prepare) {
    const a = prepare.authority;
    if (!sameUuid(r.request_id, payload.request_id) || r.state_revision !== payload.expected_state_revision + 1 ||
        r.prepare_fingerprint !== prepare.prepare_fingerprint || !sameUuid(r.reversed_timesheet_id, a.original_timesheet_id) ||
        !sameUuid(r.reversal_timesheet_id, a.planned_reversal_timesheet_id) || !sameUuid(r.replacement_timesheet_id, a.replacement_timesheet_id) ||
        r.original_authorisation_required !== prepare.original_authorisation_required ||
        r.original_materialised_in_unit !== prepare.original_materialisation_required ||
        (!prepare.original_materialisation_required && (!sameUuid(r.original_financial_id, a.original_financial_id) ||
          r.original_financial_authority_hash !== a.original_financial_authority_hash)) ||
        (prepare.original_materialisation_required && r.original_authorisation_required !== true)) fail('CORRECTION_RECEIPT_IDENTITY_MISMATCH');
  } else terminalRevision(r, payload, true);
  if (r.authorisation != null) {
    const count = r.original_authorisation_required ? 3 : 2;
    const members = [r.reversal_timesheet_id, r.replacement_timesheet_id, ...(count === 3 ? [r.reversed_timesheet_id] : [])].map(x => x.toLowerCase());
    object(r.authorisation, 'CORRECTION_RECEIPT_INVALID');
    if (r.authorisation.ok !== true || r.authorisation.all_success !== true || r.authorisation.success_count !== count ||
        !Array.isArray(r.authorisation.results) || r.authorisation.results.length !== count ||
        r.authorisation.results.some(x => !x || x.success !== true || x.action !== 'AUTHORISE' ||
          !members.includes(String(x.current_timesheet_id || '').toLowerCase())) ||
        new Set(r.authorisation.results.map(x => x.current_timesheet_id.toLowerCase())).size !== count ||
        r.authorisation.failure_count !== 0) fail('CORRECTION_COMPLETE_UNIT_AUTHORISATION_FAILED');
  } else if (prepare && r.replayed !== true) fail('CORRECTION_COMPLETE_UNIT_AUTHORISATION_FAILED');
  return r;
}
function waitingPrepare(value, payload) {
  rpcSuccess(value);
  if (value.contract_version !== 'TIMESHEET_CORRECTION_PREPARE_V1') fail('CORRECTION_PREPARE_CONTRACT_INVALID');
  hash(value.prepare_fingerprint);
  const a = object(value.authority, 'CORRECTION_PREPARE_AUTHORITY_INVALID');
  if (!sameUuid(a.request_id, payload.request_id) || a.state_revision !== payload.expected_state_revision) fail('CORRECTION_REQUEST_CAS_STALE');
  for (const k of ['root_timesheet_id', 'original_timesheet_id', 'replacement_timesheet_id', 'planned_reversal_timesheet_id']) uuid(a[k], 'CORRECTION_PREPARE_AUTHORITY_INVALID');
  uuid(value.candidate_id, 'CORRECTION_PREPARE_AUTHORITY_INVALID'); uuid(value.client_id, 'CORRECTION_PREPARE_AUTHORITY_INVALID');
  if (!positive(a.original_timesheet_version) || !positive(a.replacement_version) || typeof a.target_revision !== 'string' || !HASH.test(a.target_revision) || a.proposal_revision !== 1 ||
      !ORIGINS.has(a.origin) || typeof value.original_materialisation_required !== 'boolean' ||
      typeof value.original_authorisation_required !== 'boolean' ||
      new Set([a.original_timesheet_id, a.replacement_timesheet_id, a.planned_reversal_timesheet_id].map(x => x.toLowerCase())).size !== 3) {
    fail('CORRECTION_PREPARE_AUTHORITY_INVALID');
  }
  const frozen = object(value.frozen_original, 'CORRECTION_PREPARE_AUTHORITY_INVALID');
  const t0 = object(frozen.timesheet, 'CORRECTION_PREPARE_AUTHORITY_INVALID');
  const p0 = object(value.replacement_timesheet, 'CORRECTION_PREPARE_AUTHORITY_INVALID');
  correctionIdentity(p0.correction_id, payload.request_id, a.root_timesheet_id);
  if (!sameUuid(t0.timesheet_id, a.original_timesheet_id) || t0.version !== a.original_timesheet_version ||
      !sameUuid(p0.timesheet_id, a.replacement_timesheet_id) || p0.version !== a.replacement_version ||
      !sameUuid(p0.parent_timesheet_id, a.root_timesheet_id) || p0.correction_kind !== 'WHOLE_TIMESHEET_REPLACEMENT' ||
      p0.is_current !== true || p0.archived_at_utc != null || p0.authorised_at_server != null ||
      typeof t0.booking_id !== 'string' || !t0.booking_id || typeof p0.booking_id !== 'string' || !p0.booking_id ||
      t0.booking_id === p0.booking_id) fail('CORRECTION_PREPARE_AUTHORITY_INVALID');
  if (!value.original_materialisation_required) {
    uuid(a.original_financial_id, 'CORRECTION_PREPARE_AUTHORITY_INVALID'); hash(a.original_financial_authority_hash);
    if (!sameUuid(value.original_current_snapshot?.id, a.original_financial_id) ||
        frozen.financial_authority_hash !== a.original_financial_authority_hash) fail('CORRECTION_PREPARE_AUTHORITY_INVALID');
    snapshotIdentity(value.original_current_snapshot, {
      timesheetId: a.original_timesheet_id, version: a.original_timesheet_version,
      candidateId: value.candidate_id, clientId: value.client_id
    }, { original: true });
  } else if (value.original_current_snapshot !== null || a.original_financial_id !== null ||
             a.original_financial_authority_hash !== null || frozen.financial_authority_hash !== null ||
             frozen.financial_snapshot !== null || t0.authorised_at_server !== null ||
             value.original_authorisation_required !== true) fail('CORRECTION_PREPARE_AUTHORITY_INVALID');
  return freezeJson(cloneJson(value));
}
function requestAuthority(value, payload, { replay = false } = {}) {
  object(value, 'CORRECTION_REQUEST_AUTHORITY_UNPROVED');
  if (OWN(value, 'ok') || value.success === false) rpcSuccess(value);
  if (!sameUuid(value.request_id, payload.request_id) || !STATES.has(value.state) || !positive(value.state_revision) ||
      !(value.state_revision === payload.expected_state_revision ||
        (replay && value.state !== 'WAITING' && value.state_revision === payload.expected_state_revision + 1))) fail('CORRECTION_REQUEST_CAS_STALE');
  uuid(value.replacement_timesheet_id, 'CORRECTION_REQUEST_AUTHORITY_UNPROVED');
  if (value.return_invoice_id != null) uuid(value.return_invoice_id, 'CORRECTION_REQUEST_AUTHORITY_UNPROVED');
  return freezeJson(cloneJson(value));
}
function cancelReceipt(value, payload, authority) {
  const r = rpcSuccess(value);
  if (r.state !== 'CANCELLED') fail('CORRECTION_REQUEST_NOT_CANCELLED');
  if (authority.state === 'WAITING') {
    if (!sameUuid(r.request_id, payload.request_id) || r.state_revision !== authority.state_revision + 1) fail('CORRECTION_REQUEST_CAS_STALE');
  } else terminalRevision(r, payload, true);
  if (!sameUuid(r.replacement_timesheet_id, authority.replacement_timesheet_id)) fail('CORRECTION_RECEIPT_IDENTITY_MISMATCH');
  uuid(r.delete_operation_id, 'CORRECTION_CANCEL_LAWFUL_REMOVAL_UNPROVED');
  if (!Number.isSafeInteger(r.retained_immutable_component_asset_count) || r.retained_immutable_component_asset_count < 0) fail('CORRECTION_CANCEL_LAWFUL_REMOVAL_UNPROVED');
  const removal = object(r.lawful_removal, 'CORRECTION_CANCEL_LAWFUL_REMOVAL_UNPROVED');
  if (removal.ok === false || removal.success === false ||
      (Array.isArray(removal.errors) && removal.errors.length)) fail('CORRECTION_CANCEL_COMPLETE_UNIT_FAILED');
  if (removal.decision === 'PERMANENT_DELETE') {
    if (removal.apply_performed !== true || !Array.isArray(removal.deleted_timesheet_ids) || removal.deleted_timesheet_ids.length !== 1 ||
        !sameUuid(removal.deleted_timesheet_ids[0], authority.replacement_timesheet_id) || !Array.isArray(removal.r2_cleanup_keys) ||
        removal.r2_cleanup_keys.length > 256 || removal.r2_cleanup_keys.some(k => typeof k !== 'string' || !k.trim() || k.length > 1024) ||
        new Set(removal.r2_cleanup_keys).size !== removal.r2_cleanup_keys.length) fail('CORRECTION_CANCEL_LAWFUL_REMOVAL_UNPROVED');
  } else {
    // Native archive_transition returns action:'ARCHIVE'; it has no permanent-delete receipt.
    if (removal.ok !== true || removal.action !== 'ARCHIVE' || removal.decision !== 'ARCHIVED' ||
        !Array.isArray(removal.timesheet_ids) || removal.timesheet_ids.length !== 1 ||
        !sameUuid(removal.timesheet_ids[0], authority.replacement_timesheet_id) || removal.archived_count !== 1 ||
        (removal.r2_cleanup_keys != null && (!Array.isArray(removal.r2_cleanup_keys) || removal.r2_cleanup_keys.length))) {
      fail('CORRECTION_CANCEL_LAWFUL_REMOVAL_UNPROVED');
    }
  }
  return r;
}
function cleanupReceipt(value, cancel) {
  const r = object(value, 'CORRECTION_COMMITTED_CLEANUP_RECEIPT_INVALID');
  const keys = cancel.lawful_removal.r2_cleanup_keys;
  if (typeof r.ok !== 'boolean' || !sameUuid(r.delete_operation_id, cancel.delete_operation_id) ||
      !sameUuid(r.requested_timesheet_id, cancel.replacement_timesheet_id) ||
      !Array.isArray(r.deleted_timesheet_ids) || r.deleted_timesheet_ids.length !== 1 ||
      !sameUuid(r.deleted_timesheet_ids[0], cancel.replacement_timesheet_id) || r.requested_key_count !== keys.length ||
      typeof r.r2_cleanup_complete !== 'boolean' || typeof r.durable_retry_recorded !== 'boolean' ||
      !Array.isArray(r.deleted_keys) || !Array.isArray(r.deferred_keys) ||
      [...r.deleted_keys, ...r.deferred_keys].some(k => !keys.includes(k)) ||
      ['attempted_key_count', 'deleted_key_count', 'already_missing_key_count', 'deferred_key_count'].some(k =>
        !Number.isSafeInteger(r[k]) || r[k] < 0 || r[k] > keys.length) ||
      r.deleted_key_count !== r.deleted_keys.length || r.deferred_key_count !== r.deferred_keys.length ||
      (r.r2_cleanup_complete && (r.ok !== true || r.deferred_keys.length !== 0))) {
    fail('CORRECTION_COMMITTED_CLEANUP_RECEIPT_INVALID');
  }
  return r;
}
function lifecycleItem(item) {
  closed(item, ITEM_KEYS);
  const ids = TS_ALIASES.filter(k => OWN(item, k)).map(k => uuid(item[k]));
  if (OWN(item, 'row_key')) {
    if (typeof item.row_key !== 'string' || !item.row_key.startsWith('timesheet:')) fail('CORRECTION_PAYLOAD_INVALID', 400);
    ids.push(uuid(item.row_key.slice(10)));
  }
  if (!ids.length || new Set(ids).size !== 1) fail('CANDIDATE_CONTEXT_STALE');
  if (OWN(item, 'expected_timesheet_id') && !sameUuid(uuid(item.expected_timesheet_id), ids[0])) fail('CANDIDATE_CONTEXT_STALE');
  if (OWN(item, 'expected_version')) integer(item.expected_version);
  if (OWN(item, 'expected_row_signature') && (typeof item.expected_row_signature !== 'string' ||
      !item.expected_row_signature || item.expected_row_signature.length > 1024)) fail('CORRECTION_PAYLOAD_INVALID', 400);
  return { item: cloneJson(item), timesheetId: ids[0] };
}
function nativeTransitionItems(unit) {
  const count = unit.expected_member_count;
  const items = unit.transition_items;
  if (![2, 3].includes(count) || !Array.isArray(items) || items.length !== count ||
      !UUID.test(unit.reversal_timesheet_id || '') ||
      (count === 3 && (unit.original_authorisation_required !== true || !UUID.test(unit.original_timesheet_id || '')))) {
    fail('CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
  }
  const ids = [unit.replacement_timesheet_id, unit.reversal_timesheet_id,
    ...(count === 3 ? [unit.original_timesheet_id] : [])].map(x => x.toLowerCase());
  if (new Set(ids).size !== count || items.some(i => !i || !ids.includes(String(i.timesheet_id || '').toLowerCase()) ||
      !sameUuid(i.expected_timesheet_id, i.timesheet_id) || !positive(i.expected_version) ||
      i.lifecycle_group_size !== count || typeof i.lifecycle_group_id !== 'string' || !i.lifecycle_group_id.startsWith('correction:')) ||
      new Set(items.map(i => i.timesheet_id.toLowerCase())).size !== count ||
      new Set(items.map(i => i.lifecycle_group_id)).size !== 1) fail('CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
  // These items are from the existing whole transition owner, never client group hints.
  return cloneJson(items);
}

/** @param {CorrectionDependencies} deps */
export function createTimesheetCorrectionOrchestrator(deps) {
  if (typeof deps?.rpc !== 'function' || typeof deps?.assertActor !== 'function') fail('CORRECTION_SERVICE_DEPENDENCY_MISSING', 503);
  async function actor(context, action) {
    const result = await deps.assertActor(context, action);
    object(result, 'CORRECTION_ACTOR_NOT_ALLOWED');
    if (OWN(result, 'ok') || result.success === false) rpcSuccess(result);
    uuid(result.actor_user_id, 'CORRECTION_ACTOR_NOT_ALLOWED');
    if (result.environment !== 'TEST') fail('CORRECTION_ENVIRONMENT_NOT_ALLOWED', 403);
    return Object.freeze({ actor_user_id: result.actor_user_id.toLowerCase(), environment: 'TEST' });
  }
  async function rawPrepare(p, a) {
    return rpcSuccess(await deps.rpc('timesheet_correction_prepare_v1', { p_request_id: p.request_id, p_actor_user_id: a.actor_user_id }));
  }
  async function readAuthority(p, a, options) {
    if (typeof deps.readRequestAuthority !== 'function') fail('CORRECTION_REQUEST_AUTHORITY_UNPROVED', 503);
    let value = await deps.readRequestAuthority({ requestId: p.request_id, actor: a });
    object(value, 'CORRECTION_REQUEST_AUTHORITY_UNPROVED');
    // The exact service read returns bigint CAS as decimal text. Decode only
    // that closed versioned wire contract; retain existing normalized internal
    // callbacks and refuse precision loss before a mutation RPC.
    if (OWN(value, 'contract_version') || typeof value.state_revision === 'string') {
      const keys = new Set(['contract_version', 'request_id', 'state', 'state_revision',
        'replacement_timesheet_id', 'return_invoice_id']);
      if (value.contract_version !== 'TIMESHEET_CORRECTION_REQUEST_AUTHORITY_V1' ||
          Object.keys(value).some(k => !keys.has(k)) ||
          typeof value.state_revision !== 'string' || !/^[1-9][0-9]{0,15}$/.test(value.state_revision) ||
          !Number.isSafeInteger(Number(value.state_revision))) fail('CORRECTION_REQUEST_AUTHORITY_UNPROVED');
      value = { ...value, state_revision: Number(value.state_revision) };
    }
    return requestAuthority(value, p, options);
  }
  async function authoriseWithActor(p, a) {
    const raw = await rawPrepare(p, a);
    if (raw.state != null) return appliedReceipt(raw, p);
    const prepare = waitingPrepare(raw, p);
    let original;
    if (prepare.original_materialisation_required) {
      if (typeof deps.buildOriginalSnapshot !== 'function') fail('CORRECTION_CANONICAL_ORIGINAL_PRODUCER_UNAVAILABLE', 503);
      original = cloneJson(await deps.buildOriginalSnapshot({ prepare, actor: a }));
      snapshotIdentity(original, {
        timesheetId: prepare.authority.original_timesheet_id, version: prepare.authority.original_timesheet_version,
        candidateId: prepare.candidate_id, clientId: prepare.client_id
      }, { canonicalInput: true });
    }
    if (typeof deps.buildReplacementSnapshot !== 'function') fail('CORRECTION_CANONICAL_PRODUCER_UNAVAILABLE', 503);
    const replacement = cloneJson(await deps.buildReplacementSnapshot({ prepare, actor: a }));
    const authority = prepare.authority;
    snapshotIdentity(replacement, {
      timesheetId: authority.replacement_timesheet_id, version: authority.replacement_version,
      candidateId: prepare.candidate_id, clientId: prepare.client_id
    }, { canonicalInput: true });
    const prepared = { prepare_fingerprint: prepare.prepare_fingerprint, replacement_snapshot: replacement };
    if (prepare.original_materialisation_required) {
      prepared.original_snapshot = original;
      // No A0 reversal is calculated from preview input. The existing atomic
      // SQL writes/rereads actual original and signs that stored output.
    } else {
      prepared.reversal_snapshot = cloneFrozenCorrectionReversal(prepare.original_current_snapshot, {
        timesheetId: authority.planned_reversal_timesheet_id, version: 1,
        candidateId: prepare.candidate_id, clientId: prepare.client_id
      });
    }
    const result = await deps.rpc('timesheet_correction_authorise_atomic_v1', {
      p_request_id: p.request_id, p_expected_state_revision: p.expected_state_revision,
      p_prepared: prepared, p_actor_user_id: a.actor_user_id
    });
    return appliedReceipt(result, p, prepare);
  }
  async function send(payload, context) {
    const p = sendPayload(payload), a = await actor(context, 'SEND');
    const r = rpcSuccess(await deps.rpc('timesheet_correction_send_v1', { p_request: p, p_actor_user_id: a.actor_user_id }));
    uuid(r.request_id, 'CORRECTION_RECEIPT_INVALID'); uuid(r.replacement_timesheet_id, 'CORRECTION_RECEIPT_INVALID');
    if (!STATES.has(r.state) || !positive(r.state_revision) ||
        (r.replacement_booking_id != null && (typeof r.replacement_booking_id !== 'string' || !r.replacement_booking_id.trim())) ||
        (r.replayed !== true && (r.state !== 'WAITING' || r.state_revision !== 1 || r.proposal_revision !== 1 || typeof r.target_revision !== 'string' || !HASH.test(r.target_revision) ||
          typeof r.replacement_booking_id !== 'string' || !r.replacement_booking_id.trim())) ||
        (OWN(r, 'target_revision') && (typeof r.target_revision !== 'string' || !HASH.test(r.target_revision))) ||
        (OWN(r, 'proposal_revision') && r.proposal_revision !== 1)) {
      fail('CORRECTION_SEND_RECEIPT_INVALID');
    }
    return r;
  }
  async function prepare(payload, context) {
    const p = actionPayload(payload), a = await actor(context, 'PREPARE');
    const raw = await rawPrepare(p, a);
    if (raw.state === 'APPLIED') return freezeJson(cloneJson(appliedReceipt(raw, p)));
    if (raw.state === 'CANCELLED') {
      const authority = await readAuthority(p, a, { replay: true });
      return freezeJson(cloneJson(cancelReceipt(raw, p, authority)));
    }
    return waitingPrepare(raw, p); // Internal DTO: root must never serialize this to public clients.
  }
  async function authorise(payload, context) {
    const p = actionPayload(payload), a = await actor(context, 'AUTHORISE');
    return authoriseWithActor(p, a);
  }
  async function cancel(payload, context) {
    const p = actionPayload(payload), a = await actor(context, 'CANCEL');
    // PREPARE requires finalized signed P0; pending Cancel uses only the narrow native request reader.
    const authority = await readAuthority(p, a, { replay: true });
    if (authority.state === 'APPLIED') fail('CORRECTION_REQUEST_NOT_WAITING');
    if (typeof deps.deleteCommittedTimesheetR2Keys !== 'function') fail('CORRECTION_COMMITTED_CLEANUP_OWNER_UNAVAILABLE', 503);
    const r = cancelReceipt(await deps.rpc('timesheet_correction_cancel_atomic_v1', {
      p_environment: a.environment, p_request_id: p.request_id,
      p_expected_state_revision: p.expected_state_revision, p_actor_user_id: a.actor_user_id
    }), p, authority);
    if (r.lawful_removal.decision !== 'PERMANENT_DELETE' || r.lawful_removal.r2_cleanup_keys.length === 0) return r;
    try {
      const cleanup = await deps.deleteCommittedTimesheetR2Keys(cloneJson(r.lawful_removal), {
        deleteOperationId: r.delete_operation_id, requestedTimesheetId: r.replacement_timesheet_id
      });
      return { ...r, cleanup: cleanupReceipt(cleanup, r) };
    } catch {
      // Cancellation is already committed. The existing owner handles durable cleanup retry;
      // expose a truthful pending follow-up, never imply the SQL transaction rolled back.
      return { ...r, cleanup: { attempted: true, cleanup_pending: true, code: 'CORRECTION_COMMITTED_CLEANUP_OWNER_FAILED' } };
    }
  }
  async function returnToDraft(payload, context) {
    const p = actionPayload(payload), a = await actor(context, 'RETURN');
    const authority = await readAuthority(p, a); // Strict terminal CAS; RETURN RPC has no CAS argument.
    if (authority.state !== 'APPLIED') fail('CORRECTION_REQUEST_NOT_APPLIED');
    const r = rpcSuccess(await deps.rpc('timesheet_correction_return_to_draft_v1', {
      p_request_id: p.request_id, p_actor_user_id: a.actor_user_id
    }));
    if (r.request_id != null && !sameUuid(r.request_id, p.request_id)) fail('CORRECTION_RECEIPT_IDENTITY_MISMATCH');
    if (authority.return_invoice_id == null) {
      if (r.not_required !== true || r.return_invoice_id != null) fail('CORRECTION_RETURN_RECEIPT_INVALID');
    } else if (!sameUuid(r.return_invoice_id, authority.return_invoice_id) ||
               typeof r.return_pending !== 'boolean' ||
               (r.action != null && r.action !== 'RETURN') ||
               (r.return_pending && r.blocker_code !== 'AGENCY_INVOICE_TIMESHEET_RETURNING') ||
               (!r.return_pending && r.action !== 'RETURN' && r.empty_replacement !== true && r.replayed !== true)) {
      fail('CORRECTION_RETURN_RECEIPT_INVALID');
    }
    return r; // Pending is a named native hold, not a completed return.
  }
  async function runLifecycle(payload, context) {
    closed(payload, new Set(['action', 'items']));
    if (!['AUTHORISE', 'UNAUTHORISE'].includes(payload.action) || !Array.isArray(payload.items) ||
        !payload.items.length || payload.items.length > MAX_ITEMS) fail('CORRECTION_PAYLOAD_INVALID', 400);
    const selected = payload.items.map(lifecycleItem), a = await actor(context, payload.action);
    if (typeof deps.resolveLifecycleItem !== 'function' || typeof deps.runOrdinaryLifecycle !== 'function') {
      fail('CORRECTION_LIFECYCLE_RESOLVER_UNAVAILABLE', 503);
    }
    const units = [], requests = new Map(), requestSelections = new Map(), ordinary = [], ordinaryIds = new Set();
    const selectedTimesheetIds = Object.freeze([...new Set(selected.map(s => s.timesheetId))]);
    // Complete admission/resolution before the first mutation. Each request remains its own atomic unit.
    for (const s of selected) {
      const r = await deps.resolveLifecycleItem({ ...s, selectedTimesheetIds, action: payload.action, actor: a });
      if (r == null) {
        if (!ordinaryIds.has(s.timesheetId)) { ordinaryIds.add(s.timesheetId); ordinary.push(s.item); }
        continue;
      }
      object(r, 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      if (OWN(r, 'ok') || r.success === false) rpcSuccess(r);
      if (r.valid === false || r.action_ready === false) fail('CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      if (r.kind !== 'CORRECTION_REQUEST' || !sameUuid(r.selected_timesheet_id, s.timesheetId) || !STATES.has(r.state)) fail('CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      const id = uuid(r.request_id, 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      integer(r.state_revision, 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED'); uuid(r.replacement_timesheet_id, 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      const p0 = sameUuid(s.timesheetId, r.replacement_timesheet_id), r0 = r.reversal_timesheet_id != null && sameUuid(s.timesheetId, r.reversal_timesheet_id);
      const a0 = r.original_authorisation_required === true && sameUuid(s.timesheetId, r.original_timesheet_id);
      if (!p0 && !r0 && !a0) fail('CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      if (!requestSelections.has(id)) requestSelections.set(id, new Set());
      requestSelections.get(id).add(s.timesheetId);
      const prior = requests.get(id);
      if (prior && (prior.state !== r.state || prior.state_revision !== r.state_revision ||
        !sameUuid(prior.replacement_timesheet_id, r.replacement_timesheet_id) ||
        (prior.reversal_timesheet_id ?? null) !== (r.reversal_timesheet_id ?? null) ||
        (prior.original_authorisation_required === true) !== (r.original_authorisation_required === true) ||
        (prior.original_timesheet_id ?? null) !== (r.original_timesheet_id ?? null))) fail('CORRECTION_REQUEST_CAS_STALE');
      if (!prior) {
        if (units.some(u => sameUuid(u.replacement_timesheet_id, r.replacement_timesheet_id))) fail('CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
        const frozen = freezeJson(cloneJson(r));
        requests.set(id, frozen); units.push(frozen);
      }
    }
    // An original alone is an ordinary action, never authority to apply a pending correction.
    // Exact server resolver may group A0 only when a physical request leg is also selected.
    for (const [id, u] of requests) {
      const members = requestSelections.get(id);
      if (u.original_authorisation_required === true && members.has(String(u.original_timesheet_id).toLowerCase()) &&
          !members.has(u.replacement_timesheet_id.toLowerCase()) &&
          !(u.reversal_timesheet_id && members.has(u.reversal_timesheet_id.toLowerCase()))) fail('CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
    }
    if (payload.action === 'UNAUTHORISE' && units.some(u => u.state !== 'APPLIED')) fail('CORRECTION_REQUEST_NOT_APPLIED');
    if (units.some(u => u.state === 'CANCELLED')) fail('CORRECTION_REQUEST_CANCELLED');
    // APPLIED whole groups and ordinary rows stay in the unchanged native lifecycle owner.
    for (const u of units.filter(u => u.state === 'APPLIED')) ordinary.push(...nativeTransitionItems(u));
    const results = [];
    for (const u of units.filter(u => u.state === 'WAITING')) {
      try {
        const receipt = await authoriseWithActor({ request_id: u.request_id, expected_state_revision: u.state_revision }, a);
        results.push({ kind: 'CORRECTION_REQUEST', request_id: u.request_id, success: true, receipt });
      } catch (e) {
        results.push({ kind: 'CORRECTION_REQUEST', request_id: u.request_id, success: false,
          code: safeFailureCode(e) });
      }
    }
    if (ordinary.length) {
      try {
        const receipt = await deps.runOrdinaryLifecycle({ items: ordinary, action: payload.action, actor: a });
        object(receipt, 'CORRECTION_LIFECYCLE_RECEIPT_INVALID');
        const success = receipt.ok === true && receipt.all_success === true && receipt.failure_count === 0 &&
          Array.isArray(receipt.results) && receipt.results.length > 0 && receipt.results.every(r => r?.success === true);
        results.push({ kind: 'NATIVE_LIFECYCLE', success, receipt });
      } catch (e) {
        results.push({ kind: 'NATIVE_LIFECYCLE', success: false,
          code: safeFailureCode(e) });
      }
    }
    return { ok: true, all_success: results.every(r => r.success), results,
      ordinary_notification_timesheet_ids: [...new Set(results.filter(r => r.kind === 'CORRECTION_REQUEST' && r.success)
        .flatMap(r => r.receipt.ordinary_notification_timesheet_ids))] };
  }
  return Object.freeze({ send, prepare, authorise, cancel, returnToDraft, runLifecycle });
}
