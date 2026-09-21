import {
  buildWeeklySourceC1Components,
  listWeeklySourceC1ComponentKeys,
} from '../banking-pay/weekly-source-c1-components.mjs';
import {
  publishDurableWeeklySourceC1,
  recoverDurableWeeklySourceC1,
  stageAndPublishDurableWeeklySourceC1,
} from '../banking-pay/weekly-source-c1-durable-publication.mjs';
import {
  prepareWeeklySourceC1Publication,
} from '../banking-pay/weekly-source-c1-publication.mjs';
import {
  buildWeeklyProtectedC1Stream,
} from '../banking-pay/weekly-source-c1-stream.mjs';
import {
  composeWeeklyProtectedTargetSchedule,
} from './protected-target-schedule.js';

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
  for (const name of ['dataRpc', 'calculateWeeklySnapshot', 'c1RawRpc']) {
    if (typeof dependencies?.[name] !== 'function') {
      fail('WEEKLY_PROTECTED_DEPENDENCY_UNAVAILABLE', 'Approved hours cannot be changed right now.', { dependency: name });
    }
  }
}

async function rpc(dependencies, name, pRequest) {
  return dependencies.dataRpc(name, { p_request: pRequest });
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

function durablePublicationInput(normalized, status, dependencies) {
  return {
    data_rpc: dependencies.dataRpc,
    c1_raw_rpc: dependencies.c1RawRpc,
    actor_user_id: normalized.actorUserId,
    publication_request_id: status.publication_request_id,
    expected_request_sha256: status.request_sha256,
  };
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
 * calculator and complete-entitlement C1 route.  This owner never calculates
 * a residual, creates a Draft, mutates an invoice or calls Banking Pay.
 */
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

  if (normalized.kind !== 'WAIT') {
    const status = await publicationStatus(normalized, prepared, dependencies);
    if (status.staged) {
      const durable = durablePublicationInput(normalized, status, dependencies);
      return normalized.recover
        ? recoverDurableWeeklySourceC1(durable)
        : publishDurableWeeklySourceC1(durable);
    }
    if (normalized.recover) {
      fail(
        'C1_DURABLE_RECOVERY_NOT_REQUIRED',
        'There is no unresolved protected-hours publication to recover.',
      );
    }
  }
  let context = await loadContext(normalized, prepared, dependencies);
  context = await ensureRootFinancial(normalized, prepared, context, dependencies);

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
    context,
    schedule: target.segments,
    zeroRoot: false,
  });
  const financialSnapshot = stageSnapshot(calculation, context);
  const currentFinancial = object(
    context.current_financial,
    'WEEKLY_PROTECTED_CONTEXT_INVALID',
    'Current Weekly financial record',
  );
  const currentFinancialId = uuid(currentFinancial.id, 'Current Weekly financial record');
  const componentInput = {
    root_timesheet_id: context.root_timesheet_id,
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
    contract_id: context.contract_id,
    root_timesheet_id: context.root_timesheet_id,
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
  const durableInput = {
    data_rpc: dependencies.dataRpc,
    c1_raw_rpc: dependencies.c1RawRpc,
    stage_request: stageRequest,
    actor_user_id: normalized.actorUserId,
    publication_request_id: requestId,
    expected_request_sha256: publication.request_sha256,
  };
  return stageAndPublishDurableWeeklySourceC1(durableInput);
}

export const WEEKLY_PROTECTED_ACTION_ORCHESTRATOR_CONTRACT = Object.freeze({
  version: 'WEEKLY_PROTECTED_ACTION_ORCHESTRATOR_V1',
  actions: Object.freeze(Object.keys(ACTION)),
  browserFinancialFactsAccepted: false,
  c1AutomaticRetry: false,
  workbenchBypass: false,
  invoiceMutation: false,
});
