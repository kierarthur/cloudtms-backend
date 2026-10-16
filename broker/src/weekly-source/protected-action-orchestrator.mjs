import {
  buildWeeklySourceC1Components,
  listWeeklySourceC1ComponentKeys,
} from '../banking-pay/weekly-source-c1-components.mjs';
import {
  prepareWeeklySourceC1Publication,
} from '../banking-pay/weekly-source-c1-publication.mjs';
import {
  buildWeeklyProtectedC1Stream,
} from '../banking-pay/weekly-source-c1-stream.mjs';
import {
  composeWeeklyProtectedTargetSchedule,
} from './protected-target-schedule.js';
import { buildWeeklyProtectedNextTarget } from './protected-next-preview.mjs';
import { sourceProtectedCalculationContext } from './source-native-economic.mjs';
import { sourceNativeContinuationFromRpcError, sourcePayDecisionReadAction } from './source-native-contract.mjs';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;
const TIME = /^(?:[01]\d|2[0-3]):[0-5]\d$/;
const HASH = /^[0-9a-f]{64}$/;

const ACTION = Object.freeze({
  APPROVE_PROTECTED_HOURS: 'APPROVE',
  AMEND_PROTECTED_HOURS: 'AMEND',
  WITHDRAW_PROTECTED_HOURS: 'WITHDRAW',
  WAIT_FOR_SOURCE: 'WAIT',
  ACCEPT_SOURCE_AND_RECONCILE: 'RECONCILE',
  RECORD_NOT_WORKED: 'RECORD_NOT_WORKED',
});

const BASE_KEYS = new Set([
  'actor_user_id', 'source_cycle_id', 'work_event_id', 'reason',
  'idempotency_key', 'recover_unknown_outcome',
]);
const SCHEDULE_KEYS = new Set(['work_date', 'start', 'end', 'break_minutes']);
const APPROVE_KEYS = new Set([
  ...BASE_KEYS, ...SCHEDULE_KEYS, 'candidate_id', 'client_id', 'contract_id',
  'week_ending_date', 'evidence_timesheet_id',
]);
const AMEND_KEYS = new Set([
  ...BASE_KEYS, ...SCHEDULE_KEYS, 'family_id', 'expected_family_bound_version',
  'evidence_timesheet_id',
]);
const FOLLOW_UP_KEYS = new Set([
  ...BASE_KEYS, 'family_id', 'expected_family_bound_version',
  'expected_source_hash', 'expected_source_revision',
]);

export class WeeklyProtectedActionError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'WeeklyProtectedActionError';
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

function fail(code, message, details) {
  throw new WeeklyProtectedActionError(code, message, details);
}

function object(value, code, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail(code, `${label} is unavailable.`);
  return value;
}

function text(value, code, label) {
  const result = String(value ?? '').trim();
  if (!result) fail(code, `${label} is unavailable.`);
  return result;
}

function uuid(value, label) {
  const result = text(value, 'WEEKLY_PROTECTED_ACTION_INVALID', label).toLowerCase();
  if (!UUID.test(result)) fail('WEEKLY_PROTECTED_ACTION_INVALID', `${label} is invalid.`);
  return result;
}

function positiveInteger(value, label) {
  const token = String(value ?? '').trim();
  if (!/^[1-9]\d*$/.test(token)) {
    fail('WEEKLY_PROTECTED_ACTION_INVALID', `${label} is invalid.`);
  }
  const number = Number(token);
  if (!Number.isSafeInteger(number)) {
    fail('WEEKLY_PROTECTED_ACTION_INVALID', `${label} is invalid.`);
  }
  return token;
}

function exactKeys(value, allowed) {
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) fail('WEEKLY_PROTECTED_ACTION_UNKNOWN_FIELD', `Protected-hours field ${key} is not permitted.`);
  }
}

function scheduleFromRequest(request) {
  const workDate = text(request.work_date, 'WEEKLY_PROTECTED_SCHEDULE_INVALID', 'Shift date');
  const start = text(request.start, 'WEEKLY_PROTECTED_SCHEDULE_INVALID', 'Shift start');
  const end = text(request.end, 'WEEKLY_PROTECTED_SCHEDULE_INVALID', 'Shift finish');
  const breakMinutes = Number(request.break_minutes);
  if (!DATE.test(workDate) || !TIME.test(start) || !TIME.test(end)
      || !Number.isSafeInteger(breakMinutes) || breakMinutes < 0) {
    fail('WEEKLY_PROTECTED_SCHEDULE_INVALID', 'The protected shift date, times or break are invalid.');
  }
  const startMinutes = Number(start.slice(0, 2)) * 60 + Number(start.slice(3));
  const endMinutes = Number(end.slice(0, 2)) * 60 + Number(end.slice(3));
  const elapsed = endMinutes > startMinutes
    ? endMinutes - startMinutes
    : (24 * 60) - startMinutes + endMinutes;
  if (start === end || breakMinutes >= elapsed) {
    fail('WEEKLY_PROTECTED_SCHEDULE_INVALID', 'The protected shift must contain positive worked time.');
  }
  const endDate = endMinutes > startMinutes ? workDate : addUtcDays(workDate, 1);
  return Object.freeze({
    work_date: workDate,
    start_at_local: `${workDate} ${start}:00`,
    end_at_local: `${endDate} ${end}:00`,
    break_minutes: breakMinutes,
  });
}

function addUtcDays(date, days) {
  const value = new Date(`${date}T00:00:00.000Z`);
  if (!Number.isFinite(value.valueOf())) fail('WEEKLY_PROTECTED_SCHEDULE_INVALID', 'The shift date is invalid.');
  value.setUTCDate(value.getUTCDate() + days);
  return value.toISOString().slice(0, 10);
}

async function privateUuid(seed) {
  const bytes = new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(seed)));
  bytes[6] = (bytes[6] & 0x0f) | 0x80;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes.slice(0, 16)].map((part) => part.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function requireDependencies(dependencies) {
  for (const name of ['dataRpc', 'calculateWeeklySnapshot']) {
    if (typeof dependencies?.[name] !== 'function') {
      fail('WEEKLY_PROTECTED_DEPENDENCY_UNAVAILABLE', 'Approved hours cannot be changed right now.', { dependency: name });
    }
  }
}

async function rpc(dependencies, name, pRequest) {
  try {
    return await dependencies.dataRpc(name, { p_request: pRequest });
  } catch (error) {
    const continuation = sourceNativeContinuationFromRpcError(error, name);
    if (!continuation) throw error;
    const refusal = new WeeklyProtectedActionError('SOURCE_PAY_DECISION_RESULT_REQUIRED',
      'Open the saved Source pay decision to continue.', {
        retryable: false, decision_id: continuation.decision_id, request_id: continuation.request_id,
        next_action: sourcePayDecisionReadAction(continuation.decision_id, continuation.request_id),
      });
    refusal.status = 409;
    throw refusal;
  }
}

function normalizeRequest(action, request) {
  const input = object(request, 'WEEKLY_PROTECTED_ACTION_INVALID', 'Protected-hours action');
  const kind = ACTION[action];
  if (!kind) fail('WEEKLY_PROTECTED_ACTION_INVALID', 'The protected-hours action is not supported.');
  exactKeys(input, kind === 'APPROVE' ? APPROVE_KEYS : kind === 'AMEND' ? AMEND_KEYS : FOLLOW_UP_KEYS);
  const reason = text(input.reason, 'WEEKLY_PROTECTED_ACTION_INVALID', 'Reason');
  const idempotencyKey = text(input.idempotency_key, 'WEEKLY_PROTECTED_ACTION_INVALID', 'Action reference');
  if (reason.length > 1000 || idempotencyKey.length < 16 || idempotencyKey.length > 200) {
    fail('WEEKLY_PROTECTED_ACTION_INVALID', 'The protected-hours reason or action reference is invalid.');
  }
  if (Object.hasOwn(input, 'recover_unknown_outcome')
      && typeof input.recover_unknown_outcome !== 'boolean') {
    fail('WEEKLY_PROTECTED_ACTION_INVALID', 'The protected-hours recovery choice is invalid.');
  }
  if (kind === 'APPROVE' && !DATE.test(String(input.week_ending_date ?? ''))) {
    fail('WEEKLY_PROTECTED_ACTION_INVALID', 'Week ending date is invalid.');
  }
  if (Object.hasOwn(input, 'expected_source_hash') || Object.hasOwn(input, 'expected_source_revision')) {
    if (kind !== 'RECONCILE' || !HASH.test(input.expected_source_hash ?? '')
        || !UUID.test(input.expected_source_revision ?? '')) {
      fail('WEEKLY_PROTECTED_ACTION_INVALID', 'The reviewed source version is invalid.');
    }
  }
  return Object.freeze({
    input,
    kind,
    actorUserId: uuid(input.actor_user_id, 'Office user'),
    sourceCycleId: uuid(input.source_cycle_id, 'Source cycle'),
    familyId: kind === 'APPROVE' ? null : uuid(input.family_id, 'Protected-hours family'),
    expectedFamilyBoundVersion: kind === 'APPROVE'
      ? null
      : positiveInteger(input.expected_family_bound_version, 'Protected-hours version'),
    workEventId: input.work_event_id == null ? null : uuid(input.work_event_id, 'Work event'),
    schedule: ['APPROVE', 'AMEND'].includes(kind) ? scheduleFromRequest(input) : null,
    reason,
    idempotencyKey,
    recover: input.recover_unknown_outcome === true,
  });
}

async function prepare(normalized, dependencies) {
  const { input, kind, schedule } = normalized;
  if (kind === 'APPROVE') {
    return rpc(dependencies, 'weekly_exceptional_pay_prepare_family_v1', {
      actor_user_id: normalized.actorUserId,
      source_cycle_id: normalized.sourceCycleId,
      candidate_id: uuid(input.candidate_id, 'Candidate'),
      client_id: uuid(input.client_id, 'Client'),
      contract_id: uuid(input.contract_id, 'Contract'),
      week_ending_date: text(input.week_ending_date, 'WEEKLY_PROTECTED_ACTION_INVALID', 'Week ending date'),
      work_event_id: normalized.workEventId,
      ...schedule,
      evidence_timesheet_id: input.evidence_timesheet_id == null
        ? null : uuid(input.evidence_timesheet_id, 'Evidence Timesheet'),
      reason: normalized.reason,
      idempotency_key: normalized.idempotencyKey,
    });
  }
  return rpc(dependencies, 'weekly_exceptional_pay_prepare_action_v1', {
    schema_version: 'WEEKLY_PROTECTED_ACTION_PREPARE_V1',
    actor_user_id: normalized.actorUserId,
    family_id: normalized.familyId,
    source_cycle_id: normalized.sourceCycleId,
    work_event_id: uuid(input.work_event_id, 'Work event'),
    action: kind,
    expected_family_bound_version: normalized.expectedFamilyBoundVersion,
    protected_schedule: kind === 'AMEND' ? schedule : null,
    ...(input.expected_source_hash ? { expected_source_hash: input.expected_source_hash,
      expected_source_revision: input.expected_source_revision } : {}),
    reason: normalized.reason,
    idempotency_key: normalized.idempotencyKey,
  });
}

async function loadContext(normalized, prepared, dependencies) {
  const protectedSchedule = prepared.protected_schedule ?? normalized.schedule;
  if (!protectedSchedule) {
    fail('WEEKLY_PROTECTED_CONTEXT_INVALID', 'The protected-hours schedule is unavailable.');
  }
  const context = await rpc(dependencies, 'weekly_exceptional_pay_action_context_v1', {
    schema_version: 'WEEKLY_PROTECTED_ACTION_CONTEXT_V1',
    actor_user_id: normalized.actorUserId,
    family_id: prepared.family_id,
    orchestration_run_id: prepared.orchestration_run_id,
    source_cycle_id: prepared.source_cycle_id,
    work_event_id: prepared.work_event_id,
    protected_schedule: protectedSchedule,
    evidence_timesheet_id: normalized.input.evidence_timesheet_id ?? null,
  });
  if (context?.ok !== true || context?.contract !== 'WEEKLY_PROTECTED_ACTION_CONTEXT_V1') {
    fail('WEEKLY_PROTECTED_CONTEXT_INVALID', 'The protected-hours authority is incomplete.');
  }
  for (const field of [
    'agency_id', 'candidate_id', 'client_id', 'contract_id', 'root_timesheet_id',
    'work_event_id', 'family_id', 'orchestration_run_id',
  ]) uuid(context[field], field);
  if (!Array.isArray(context.source_segments) || !Array.isArray(context.protected_decisions)
      || !Array.isArray(context.client_sources) || !Array.isArray(context.source_expenses)) {
    fail('WEEKLY_PROTECTED_CONTEXT_INVALID', 'The protected-hours source authority is incomplete.');
  }
  if (normalized.input.expected_source_hash
      && (context.source_proposal?.source_hash !== normalized.input.expected_source_hash
        || context.source_proposal?.source_revision !== normalized.input.expected_source_revision)) {
    fail('WEEKLY_PROTECTED_REVIEW_SOURCE_CHANGED', 'The final source changed. Review it again before reconciling.');
  }
  return context;
}

async function publicationStatus(normalized, prepared, dependencies) {
  const status = await rpc(
    dependencies,
    'weekly_exceptional_pay_action_publication_status_v1',
    {
      schema_version: 'WEEKLY_PROTECTED_ACTION_PUBLICATION_STATUS_V1',
      actor_user_id: normalized.actorUserId,
      family_id: prepared.family_id,
      orchestration_run_id: prepared.orchestration_run_id,
    },
  );
  if (status?.ok !== true || typeof status.staged !== 'boolean') {
    fail('WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID', 'The protected-hours publication status is unavailable.');
  }
  if (status.staged) {
    uuid(status.publication_request_id, 'Publication request');
    const digest = text(
      status.request_sha256,
      'WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID',
      'Publication digest',
    ).toLowerCase();
    if (!HASH.test(digest)) {
      fail('WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID', 'The protected-hours publication digest is invalid.');
    }
  }
  return status;
}

async function completeLocalDecision(normalized, prepared, status, dependencies) {
  const result = await rpc(dependencies, 'weekly_exceptional_pay_complete_local_v1', {
    schema_version: 'WEEKLY_PROTECTED_LOCAL_COMPLETE_V1',
    actor_user_id: normalized.actorUserId,
    publication_request_id: status.publication_request_id,
    expected_request_sha256: status.request_sha256,
    idempotency_key: `${normalized.idempotencyKey}:local-complete`,
  });
  // Preserve the current Source returned-refusal code/message/details first.
  if (result?.ok !== true || !['PUBLISHED', 'SAVED_PENDING_FREEZE'].includes(result.outcome)) {
    fail(result?.code ?? 'WEEKLY_PROTECTED_LOCAL_COMPLETE_REFUSED',
      'The protected hours could not be saved. Your changes have been kept.', result ?? {});
  }
  const accepted = localResult(result, prepared);
  if (accepted.publication_request_id !== status.publication_request_id) {
    fail('WEEKLY_PROTECTED_LOCAL_RECEIPT_INVALID',
      'The saved protected-hours result belongs to another request.');
  }
  return accepted;
}

function zeroRootSnapshot(context, calculated) {
  const snapshot = { ...object(calculated, 'WEEKLY_PROTECTED_CALCULATION_INVALID', 'Weekly calculation') };
  return Object.freeze({
    schema_version: 'WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1',
    calculator_owner: 'buildWeeklyScheduleSegmentsSnapshot',
    source_actual_schedule_json: [],
    tsfin_snapshot_json: Object.freeze({
      timesheet_id: snapshot.timesheet_id,
      timesheet_version: snapshot.timesheet_version,
      basis: context.source_mode === 'NHSP_WEEKLY' ? 'NHSP' : 'HEALTHROSTER_SELF_BILL',
      candidate_id: snapshot.candidate_id,
      client_id: snapshot.client_id,
      role: snapshot.role,
      band: snapshot.band,
      pay_method: snapshot.pay_method,
      policy_snapshot_json: context.policy,
      rate_source_refs_json: context.zero_rate_source_refs,
      hours_day: 0, hours_night: 0, hours_sat: 0, hours_sun: 0, hours_bh: 0,
      total_hours: 0,
      pay_day: snapshot.pay_day,
      pay_night: snapshot.pay_night,
      pay_sat: snapshot.pay_sat,
      pay_sun: snapshot.pay_sun,
      pay_bh: snapshot.pay_bh,
      charge_day: snapshot.charge_day,
      charge_night: snapshot.charge_night,
      charge_sat: snapshot.charge_sat,
      charge_sun: snapshot.charge_sun,
      charge_bh: snapshot.charge_bh,
      additional_units_json: {}, additional_pay_ex_vat: 0,
      additional_charge_ex_vat: 0, additional_margin_ex_vat: 0,
      expenses_pay_ex_vat: 0, expenses_charge_ex_vat: 0,
      expenses_description: null, expenses_evidence_r2_key: null,
      expenses_evidence_manifest: null, mileage_units: 0, mileage_pay_ex_vat: 0,
      mileage_charge_ex_vat: 0, mileage_pay_rate: null, mileage_charge_rate: null,
      mileage_evidence_r2_key: null, mileage_evidence_manifest: null,
      total_pay_ex_vat: 0, total_charge_ex_vat: 0, margin_ex_vat: 0,
      candidate_assignment: 'ASSIGNED',
      processing_status: 'PENDING_AUTH',
      invoice_breakdown_json: {
        mode: 'SEGMENTS', segments: [],
        additional: { units: {}, pay_ex_vat: 0, charge_ex_vat: 0, margin_ex_vat: 0 },
        totals: { total_pay_ex_vat: 0, total_charge_ex_vat: 0, margin_ex_vat: 0 },
      },
    }),
  });
}

async function ensureRootFinancial(normalized, prepared, context, dependencies) {
  if (!context.requires_zero_financial) return context;
  const calculated = await dependencies.calculateWeeklySnapshot({
    context,
    schedule: [],
    zeroRoot: true,
  });
  const serviceSnapshot = zeroRootSnapshot(context, calculated.snapshot ?? calculated);
  const result = await rpc(dependencies, 'weekly_source_target_managed_root_prepare_atomic_v1', {
    schema_version: 'WEEKLY_SOURCE_TARGET_MANAGED_ROOT_PREPARE_REQUEST_V1',
    actor_user_id: normalized.actorUserId,
    target_family_id: prepared.family_id,
    root_timesheet_id: prepared.root_timesheet_id,
    source_cycle_id: prepared.source_cycle_id,
    service_snapshot: serviceSnapshot,
    idempotency_key: `${normalized.idempotencyKey}:zero-root`,
  });
  if (result?.ok !== true || result?.outcome !== 'PREPARED') {
    fail(result?.error_code ?? 'WEEKLY_PROTECTED_ROOT_PREPARE_REFUSED', 'The protected-hours Timesheet is not ready.', result ?? {});
  }
  return loadContext(normalized, prepared, dependencies);
}

function stageSnapshot(calculation, target) {
  const snapshot = object(calculation.snapshot ?? calculation, 'WEEKLY_PROTECTED_CALCULATION_INVALID', 'Weekly calculation');
  if (String(snapshot.timesheet_id ?? '').toLowerCase() !== String(target.root_timesheet_id).toLowerCase()) {
    fail('WEEKLY_PROTECTED_CALCULATION_ROOT_MISMATCH', 'The Weekly calculation belongs to another Timesheet.');
  }
  return snapshot;
}

/**
 * Execute one Office protected-hours action through the existing Weekly
 * calculator and CloudTMS-owned complete-entitlement route. The immutable C1
 * stream remains historical calculation evidence, not an external Save gate.
 * This owner never calculates
 * a residual, creates a Draft, mutates an invoice or calls Banking Pay.
 */
function nextReceipt(value, prepared) {
  const keys = ['ok', 'outcome', 'idempotent_replay', 'family_id', 'orchestration_run_id',
    'approval_id', 'generation_id', 'work_id', 'revision_id', 'command_id', 'agency_sequence',
    'new_family_bound_version', 'approved_pay_ex_vat', 'state'];
  if (!value || typeof value !== 'object' || Array.isArray(value)
      || Object.keys(value).length !== keys.length || keys.some((key) => !Object.hasOwn(value, key))
      || value.ok !== true || value.outcome !== 'PUBLISHED_NEXT' || value.state !== 'NEXT_PUBLISHED'
      || typeof value.idempotent_replay !== 'boolean'
      || value.family_id !== prepared.family_id || value.orchestration_run_id !== prepared.orchestration_run_id
      || ['family_id', 'orchestration_run_id', 'approval_id', 'generation_id', 'work_id', 'revision_id', 'command_id']
        .some((key) => typeof value[key] !== 'string' || !UUID.test(value[key]))
      || ['agency_sequence', 'new_family_bound_version']
        .some((key) => typeof value[key] !== 'string' || !/^[1-9]\d{0,18}$/.test(value[key]) || BigInt(value[key]) > 9223372036854775807n)
      || typeof value.approved_pay_ex_vat !== 'string' || !/^-?(0|[1-9]\d*)\.\d{2}$/.test(value.approved_pay_ex_vat)) {
    fail('WEEKLY_PROTECTED_NEXT_RECEIPT_INVALID', 'The protected-hours publication outcome is unknown. Retry the same action reference.');
  }
  return value;
}

function closedObject(value, keys) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}

function positiveBigintText(value) {
  return typeof value === 'string' && /^[1-9]\d{0,18}$/.test(value)
    && BigInt(value) <= 9223372036854775807n;
}

function nonnegativeBigintText(value) {
  return typeof value === 'string' && /^(0|[1-9]\d{0,18})$/.test(value)
    && BigInt(value) <= 9223372036854775807n;
}

function localResult(value, prepared, replay = false) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
      || value.ok !== true || !['PUBLISHED', 'SAVED_PENDING_FREEZE'].includes(value.outcome)
      || typeof value.idempotent_replay !== 'boolean' || (replay && value.idempotent_replay !== true)
      || value.family_id !== prepared.family_id
      || ['family_id', 'generation_id', 'publication_request_id']
        .some((key) => typeof value[key] !== 'string' || !UUID.test(value[key]))
      || typeof value.requires_first_authorisation !== 'boolean'
      || !['UNAUTHORISED_TSFIN', 'COMMON_ENTITLEMENT_HEAD'].includes(value.authority)
      || value.requires_first_authorisation !== (value.authority === 'UNAUTHORISED_TSFIN')
      || value.state !== (value.outcome === 'SAVED_PENDING_FREEZE' ? 'PENDING' : 'LIVE')
      || (value.outcome === 'SAVED_PENDING_FREEZE' && value.requires_first_authorisation)) {
    fail('WEEKLY_PROTECTED_LOCAL_RECEIPT_INVALID',
      'The saved protected-hours result could not be confirmed. Retry the same action reference.');
  }
  return value;
}

function localReceipt(value, prepared, published) {
  const keys = ['authority', 'family_id', 'orchestration_run_id', 'publication_request_id',
    'request_sha256', 'generation_id', 'state', 'requires_first_authorisation',
    'idempotent_replay', 'result', 'banking_receipt'];
  if (!closedObject(value, keys) || value.authority !== 'SOURCE_LOCAL'
      || value.family_id !== prepared.family_id || value.orchestration_run_id !== prepared.orchestration_run_id
      || ['family_id', 'orchestration_run_id', 'publication_request_id', 'generation_id']
        .some((key) => typeof value[key] !== 'string' || !UUID.test(value[key]))
      || typeof value.request_sha256 !== 'string' || !HASH.test(value.request_sha256)
      || !['COMPLETE', 'PENDING_FREEZE'].includes(value.state)
      || typeof value.requires_first_authorisation !== 'boolean' || value.idempotent_replay !== true) {
    fail('WEEKLY_PROTECTED_LOCAL_RECEIPT_INVALID', 'The saved protected-hours receipt is invalid.');
  }
  const result = localResult(value.result, prepared, true);
  if (result.generation_id !== value.generation_id || result.publication_request_id !== value.publication_request_id
      || result.requires_first_authorisation !== value.requires_first_authorisation
      || (result.outcome === 'SAVED_PENDING_FREEZE') !== (value.state === 'PENDING_FREEZE')) {
    fail('WEEKLY_PROTECTED_LOCAL_RECEIPT_INVALID', 'The saved protected-hours identities do not agree.');
  }
  const bank = value.banking_receipt;
  // Local COMPLETE/PUBLISHED includes an unauthorised TSFIN save. Only the
  // separately sealed original Banking mapping may certify publication here.
  if (published !== (bank !== null)
      || ((value.requires_first_authorisation || value.state === 'PENDING_FREEZE') && bank !== null)) {
    fail('WEEKLY_PROTECTED_LOCAL_RECEIPT_INVALID', 'The saved payment-publication status does not agree.');
  }
  if (bank !== null) {
    const bankKeys = ['source_head_id', 'work_id', 'revision_id', 'command_id',
      'agency_sequence', 'publication_status', 'application'];
    const appKeys = ['ok', 'code', 'work_id', 'owner_epoch', 'current_revision_id', 'applied_revision_id',
      'source_event_id', 'source_head_id', 'financial_snapshot_id', 'physical_root_id',
      'physical_root_version', 'revision_seal', 'source_inventory_digest', 'application_state',
      'command_id', 'agency_sequence', 'financial_view_revision'];
    const app = bank.application;
    if (!closedObject(bank, bankKeys)
        || ['source_head_id', 'work_id', 'revision_id', 'command_id']
          .some((key) => typeof bank[key] !== 'string' || !UUID.test(bank[key]))
        || !positiveBigintText(bank.agency_sequence)
        || !['QUEUED', 'APPLYING', 'APPLIED'].includes(bank.publication_status)
        || !closedObject(app, appKeys) || typeof app.ok !== 'boolean'
        || !['OK', 'UNAVAILABLE', 'INCOMPATIBLE'].includes(app.code)
        || !['APPLIED', 'PENDING', 'UNAVAILABLE', 'INCOMPATIBLE'].includes(app.application_state)
        || app.ok !== (app.code === 'OK')
        || (app.code === 'OK' ? !['APPLIED', 'PENDING'].includes(app.application_state) : app.application_state !== app.code)
        || app.work_id !== bank.work_id || app.source_head_id !== bank.source_head_id
        || app.source_event_id !== bank.source_head_id || app.command_id !== bank.command_id
        || app.agency_sequence !== bank.agency_sequence
        || ['current_revision_id', 'applied_revision_id', 'financial_snapshot_id', 'physical_root_id']
          .some((key) => app[key] !== null && (typeof app[key] !== 'string' || !UUID.test(app[key])))
        || (app.owner_epoch !== null && !positiveBigintText(app.owner_epoch))
        || (app.financial_view_revision !== null && !nonnegativeBigintText(app.financial_view_revision))
        || (app.physical_root_version !== null
          && (!Number.isSafeInteger(app.physical_root_version) || app.physical_root_version < 1))
        || (app.revision_seal !== null && (typeof app.revision_seal !== 'string'
          || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/.test(app.revision_seal)))
        || (app.source_inventory_digest !== null
          && (typeof app.source_inventory_digest !== 'string' || !HASH.test(app.source_inventory_digest)))
        || (app.ok && (app.current_revision_id !== bank.revision_id || app.owner_epoch === null
          || app.financial_view_revision === null || app.physical_root_id === null
          || app.physical_root_version === null || app.revision_seal === null || app.source_inventory_digest === null
          || (app.application_state === 'APPLIED'
            ? app.applied_revision_id !== bank.revision_id || bank.publication_status !== 'APPLIED'
            : app.applied_revision_id === bank.revision_id || bank.publication_status === 'APPLIED')))) {
      fail('WEEKLY_PROTECTED_LOCAL_RECEIPT_INVALID', 'The saved payment-publication receipt is invalid.');
    }
    // A later authorised revision may make this unchanged I3 read INCOMPATIBLE.
    // It does not erase the historical mapping or invite a second publication.
  }
  return result;
}


export async function orchestrateWeeklyProtectedAction(input = {}) {
  const dependencies = object(input.dependencies, 'WEEKLY_PROTECTED_DEPENDENCY_UNAVAILABLE', 'Protected-hours dependencies');
  requireDependencies(dependencies);
  const normalized = normalizeRequest(input.action, input.request);
  const prepared = await prepare(normalized, dependencies);
  if (prepared?.family_id == null || prepared?.orchestration_run_id == null) {
    fail('WEEKLY_PROTECTED_PREPARE_INVALID', 'The protected-hours action was not prepared.');
  }
  const preparedKind = String(prepared.request_kind ?? '').toUpperCase();
  const preparedKindAllowed = normalized.kind === 'APPROVE'
    ? ['APPROVE', 'AMEND'].includes(preparedKind)
    : preparedKind === normalized.kind;
  if (!preparedKindAllowed) {
    fail('WEEKLY_PROTECTED_ACTION_KIND_CHANGED', 'The protected-hours action changed before it could be prepared.');
  }

  let nextRoute = false;
  if (normalized.kind !== 'WAIT') {
    const route = await rpc(dependencies, 'weekly_exceptional_pay_next_publication_status_v1', {
      schema_version: 'WEEKLY_PROTECTED_NEXT_STATUS_V1',
      actor_user_id: normalized.actorUserId,
      family_id: prepared.family_id,
      orchestration_run_id: prepared.orchestration_run_id,
    });
    if (!closedObject(route, ['ok', 'route', 'published', 'receipt'])
        || route.ok !== true || !['NEXT', 'C1', 'SOURCE_LOCAL'].includes(route.route)
        || typeof route.published !== 'boolean'
        || (route.route === 'C1' && (route.published || route.receipt !== null))
        || (route.route === 'NEXT' && !route.published && route.receipt !== null)
        || (route.route === 'SOURCE_LOCAL' && route.published && route.receipt === null)) {
      fail('WEEKLY_PROTECTED_NEXT_STATUS_INVALID', 'The protected-hours owner is unavailable.');
    }
    if (route.route === 'SOURCE_LOCAL' && route.receipt !== null) {
      return localReceipt(route.receipt, prepared, route.published);
    }
    nextRoute = route.route === 'NEXT';
    if (route.published) {
      // All non-NEXT published states were handled/rejected above.
      if (!nextRoute || route.receipt?.idempotent_replay !== true) {
        fail('WEEKLY_PROTECTED_NEXT_RECEIPT_INVALID',
          'The saved protected-hours receipt is invalid.');
      }
      return nextReceipt(route.receipt, prepared);
    }
    if (!nextRoute) {
      const status = await publicationStatus(normalized, prepared, dependencies);
      if (status.staged) {
        // Same current local receipt; no context/calculation/external C1 call.
        return completeLocalDecision(normalized, prepared, status, dependencies);
      }
      if (prepared.run_state === 'COMPLETE' && prepared.idempotent_replay === true) {
        fail('WEEKLY_PROTECTED_PUBLICATION_STATUS_INVALID',
          'The protected-hours publication status is unavailable.');
      }
      if (normalized.recover) {
        fail('C1_DURABLE_RECOVERY_NOT_REQUIRED',
          'There is no unresolved protected-hours publication to recover.');
      }
    } else if (prepared.run_state === 'COMPLETE') {
      // Defensive contradiction: DB says direct NEXT but returns no saved receipt.
      fail('WEEKLY_PROTECTED_NEXT_RECEIPT_INVALID',
        'The saved protected-hours receipt is invalid.');
    }
  }

  let context = await loadContext(normalized, prepared, dependencies);
  if (!nextRoute && normalized.kind !== 'WAIT') {
    context = await ensureRootFinancial(normalized, prepared, context, dependencies);
  }

  const target = composeWeeklyProtectedTargetSchedule({
    sourceSegments: context.source_segments,
    decisions: context.protected_decisions,
  });
  if (context.action === 'WAIT') {
    const currentHash = text(
      context.current_target_vector_sha256,
      'WEEKLY_PROTECTED_CONTEXT_INVALID',
      'Current protected target',
    ).toLowerCase();
    if (!HASH.test(currentHash)) fail('WEEKLY_PROTECTED_CONTEXT_INVALID', 'The current protected target is invalid.');
    return rpc(dependencies, 'weekly_exceptional_pay_wait_atomic_v1', {
      schema_version: 'WEEKLY_PROTECTED_WAIT_V1',
      actor_user_id: normalized.actorUserId,
      family_id: context.family_id,
      orchestration_run_id: context.orchestration_run_id,
      source_cycle_id: context.source_cycle_id,
      work_event_id: context.work_event_id,
      expected_family_bound_version: context.family_bound_version,
      expected_target_vector_sha256: currentHash,
      source_proposal: context.source_proposal,
      protected_schedule: context.protected_schedule,
      reason: normalized.reason,
      idempotency_key: `${normalized.idempotencyKey}:wait`,
    });
  }

  const calculation = await dependencies.calculateWeeklySnapshot({
    context: context.protected_current_placement ? sourceProtectedCalculationContext(context) : context,
    schedule: target.segments,
    zeroRoot: false,
    ...(nextRoute ? { retainNextPreviewFacts: true } : {}),
  });
  const nativePayload = await buildWeeklyProtectedNativePayload({ context, target, calculation, nextRoute,
    actorUserId: normalized.actorUserId, reason: normalized.reason, idempotencyKey: normalized.idempotencyKey });
  if (nativePayload.route === 'NEXT') {
    const result = await rpc(dependencies, 'weekly_exceptional_pay_publish_next_v1', nativePayload.publish_request);
    return nextReceipt(result, prepared);
  }
  const staged = await rpc(dependencies, 'weekly_exceptional_pay_stage_c1_request_v1', nativePayload.stage_request);
  if (staged?.ok !== true || staged.publication_request_id !== nativePayload.complete_request.publication_request_id
      || staged.request_sha256 !== nativePayload.complete_request.expected_request_sha256) {
    fail('WEEKLY_PROTECTED_STAGE_RESULT_INVALID', 'The protected hours could not be prepared safely.');
  }
  return completeLocalDecision(normalized, prepared, staged, dependencies);
}

// Pure authoring shared by ordinary actions and Source confirmation. It performs
// no RPC, ensure, run creation, staging or financial publication.
export async function buildWeeklyProtectedNativePayload({ context, target, calculation, nextRoute, actorUserId, reason, idempotencyKey }) {
  const normalized = { actorUserId, reason, idempotencyKey };
  const calculationContext = context.protected_current_placement ? sourceProtectedCalculationContext(context) : context;
  const financialSnapshot = stageSnapshot(calculation, calculationContext);

  if (nextRoute) {
    const targetSnapshot = buildWeeklyProtectedNextTarget({ context: calculationContext, target, calculation });
    const publishRequest = {
      schema_version: 'WEEKLY_PROTECTED_NEXT_PUBLISH_V1',
      actor_user_id: normalized.actorUserId,
      family_id: context.family_id,
      orchestration_run_id: context.orchestration_run_id,
      source_cycle_id: context.source_cycle_id,
      client_id: context.client_id,
      work_event_id: context.work_event_id,
      evidence_timesheet_id: context.candidate_submission?.submission_id ?? null,
      expected_family_bound_version: context.family_bound_version,
      protected_schedule: context.protected_schedule,
      rate_classification: {
        schema_version: 'WEEKLY_PROTECTED_SERVER_CALCULATION_V1',
        calculator_owner: 'buildWeeklyScheduleSegmentsSnapshot',
        policy_sha256: context.zero_rate_source_refs?.effective_policy_sha256 ?? null,
      },
      source_proposal: context.source_proposal,
      target_snapshot: targetSnapshot,
      current_comparison_revision_id: context.current_comparison_revision_id,
      current_final_revision_id: context.current_final_revision_id,
      reason: normalized.reason,
      idempotency_key: `${normalized.idempotencyKey}:next`,
    };
    return Object.freeze({ route: 'NEXT', publish_request: publishRequest });
  }
  const currentFinancial = object(
    calculationContext.current_financial,
    'WEEKLY_PROTECTED_CONTEXT_INVALID',
    'Current Weekly financial record',
  );
  const currentFinancialId = uuid(currentFinancial.id, 'Current Weekly financial record');
  const componentInput = {
    root_timesheet_id: calculationContext.root_timesheet_id,
    financial_snapshot: financialSnapshot,
    expenses: context.source_expenses,
  };
  const componentKeys = listWeeklySourceC1ComponentKeys(componentInput);
  const componentIds = new Map();
  await Promise.all(componentKeys.map(async (key) => componentIds.set(key, await privateUuid(
    `weekly-protected-component-v1\u001f${context.family_id}\u001f${normalized.idempotencyKey}\u001f${key}`,
  ))));
  const components = buildWeeklySourceC1Components({
    ...componentInput,
    component_id_for_key: (key) => componentIds.get(key),
  });
  const requestId = await privateUuid(
    `weekly-protected-request-v1\u001f${context.family_id}\u001f${normalized.idempotencyKey}`,
  );
  const preparedStream = await buildWeeklyProtectedC1Stream({
    agency_id: context.agency_id,
    actor_user_id: normalized.actorUserId,
    request_id: requestId,
    candidate_id: context.candidate_id,
    contract_id: calculationContext.contract_id,
    root_timesheet_id: calculationContext.root_timesheet_id,
    week_ending_date: context.week_ending_date,
    source_mode: context.source_mode,
    client_sources: context.client_sources,
    candidate_submission: context.candidate_submission,
    root_financial: context.root_financial,
    provider: context.provider,
    components,
  });
  const publication = await prepareWeeklySourceC1Publication({
    prepared_stream: preparedStream,
    start_facts: {
      request_sequence: String(context.request_sequence),
      expected_head_revision: String(context.expected_head_revision),
      financial_row_id: currentFinancialId,
    },
  });
  const stageRequest = {
    schema_version: 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_V1',
    actor_user_id: normalized.actorUserId,
    family_id: context.family_id,
    orchestration_run_id: context.orchestration_run_id,
    source_cycle_id: context.source_cycle_id,
    client_id: context.client_id,
    work_event_id: context.work_event_id,
    evidence_timesheet_id: context.candidate_submission?.submission_id ?? null,
    expected_family_bound_version: context.family_bound_version,
    protected_schedule: context.protected_schedule,
    rate_classification: {
      schema_version: 'WEEKLY_PROTECTED_SERVER_CALCULATION_V1',
      calculator_owner: 'buildWeeklyScheduleSegmentsSnapshot',
      policy_sha256: context.zero_rate_source_refs?.effective_policy_sha256 ?? null,
    },
    source_proposal: context.source_proposal,
    target_snapshot: {
      schema_version: 'WEEKLY_PROTECTED_TARGET_SNAPSHOT_V1',
      actual_schedule_json: target.segments,
      tsfin_snapshot_json: financialSnapshot,
    },
    current_comparison_revision_id: context.current_comparison_revision_id,
    current_final_revision_id: context.current_final_revision_id,
    c1_request: { ...publication.request, request_sha256: publication.request_sha256 },
    c1_sources: preparedStream.stream.sources,
    c1_components: preparedStream.stream.components,
    reason: normalized.reason,
    idempotency_key: `${normalized.idempotencyKey}:stage`,
  };
  return Object.freeze({ route: 'SOURCE_LOCAL', stage_request: stageRequest, complete_request: {
    schema_version: 'WEEKLY_PROTECTED_LOCAL_COMPLETE_V1', actor_user_id: normalized.actorUserId,
    publication_request_id: requestId, expected_request_sha256: publication.request_sha256,
    idempotency_key: `${normalized.idempotencyKey}:local-complete`,
  } });
}

export const WEEKLY_PROTECTED_ACTION_ORCHESTRATOR_CONTRACT = Object.freeze({
  version: 'WEEKLY_PROTECTED_ACTION_ORCHESTRATOR_V1',
  actions: Object.freeze(Object.keys(ACTION)),
  browserFinancialFactsAccepted: false,
  c1AutomaticRetry: false,
  externalC1RequiredForSave: false,
  workbenchBypass: false,
  invoiceMutation: false,
});
