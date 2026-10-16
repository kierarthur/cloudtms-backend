import { projectWeeklySourceC1ComponentEconomics } from '../banking-pay/weekly-source-c1-components.mjs';
import { decodeSourceProtectedCurrentPlacement, sourceNativeClosed } from './source-native-contract.mjs';
import { buildWeeklyProtectedNextTarget } from './protected-next-preview.mjs';

const encoder = new TextEncoder();
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HASH = /^[0-9a-f]{64}$/;
const fail = code => { const error = new Error(code); error.code = code; error.status = 409; throw error; };
function object(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) {
    fail('SOURCE_NATIVE_ECONOMIC_OBJECT_INVALID');
  }
  return value;
}
function string(value) {
  if (typeof value !== 'string' || value.includes('\0') || /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(value)) {
    fail('SOURCE_NATIVE_JSON_STRING_INVALID');
  }
  return JSON.stringify(value);
}
// PostgreSQL jsonb uses byte length then unsigned UTF-8 byte order for object
// keys and expands exponent-form JSON numerics. The payload is newly authored
// JSON; no round-trip claim is made about already parsed SQL numeric scales.
function compareKeys(a, b) {
  const left = encoder.encode(a); const right = encoder.encode(b);
  if (left.length !== right.length) return left.length - right.length;
  for (let i = 0; i < left.length; i++) if (left[i] !== right[i]) return left[i] - right[i];
  return 0;
}
function numeric(value) {
  if (!Number.isFinite(value)) fail('SOURCE_NATIVE_JSON_NUMBER_INVALID');
  const token = JSON.stringify(value);
  const match = /^(-?)(\d+)(?:\.(\d+))?[eE]([+-]?\d+)$/.exec(token);
  if (!match) return token;
  const digits = match[2] + (match[3] ?? '');
  const point = match[2].length + Number(match[4]);
  return `${match[1]}${point <= 0 ? `0.${'0'.repeat(-point)}${digits}`
    : point >= digits.length ? `${digits}${'0'.repeat(point - digits.length)}` : `${digits.slice(0, point)}.${digits.slice(point)}`}`;
}
export function sourceNativeJsonbText(value, stack = new Set()) {
  if (value === null) return 'null';
  if (typeof value === 'string') return string(value);
  if (typeof value === 'boolean') return String(value);
  if (typeof value === 'number') return numeric(value);
  if (typeof value !== 'object' || stack.has(value)) fail('SOURCE_NATIVE_JSON_VALUE_INVALID');
  stack.add(value);
  let result;
  if (Array.isArray(value)) {
    if (Object.keys(value).length !== value.length || !Object.keys(value).every((key, index) => key === String(index))) {
      fail('SOURCE_NATIVE_JSON_ARRAY_INVALID');
    }
    result = `[${value.map(item => sourceNativeJsonbText(item, stack)).join(', ')}]`;
  } else {
    object(value);
    result = `{${Object.keys(value).sort(compareKeys).map(key => `${string(key)}: ${sourceNativeJsonbText(value[key], stack)}`).join(', ')}}`;
  }
  stack.delete(value);
  return result;
}
export async function hashSourceNativeJson(domain, value) {
  if (typeof domain !== 'string' || !domain || domain.includes('\x1f')) fail('SOURCE_NATIVE_JSON_DOMAIN_INVALID');
  const bytes = await crypto.subtle.digest('SHA-256', encoder.encode(`${domain}\x1f${sourceNativeJsonbText(value)}`));
  return [...new Uint8Array(bytes)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}
function clone(value) { sourceNativeJsonbText(value); return structuredClone(value); }

// The actual native calculator remains injected by its existing Worker owner.
// This selects its genuine current B inputs while keeping the original A
// context/evidence intact for native Source admission and prior commitments.
export function sourceProtectedCalculationContext(context) {
  object(context);
  const placement = decodeSourceProtectedCurrentPlacement(context.protected_current_placement, { family_id: context.family_id,
    original_root_timesheet_id: context.root_timesheet_id });
  const current = context.current_entitlement;
  if (!sourceNativeClosed(current, ['timesheet', 'contract_record', 'current_financial', 'current_approval_work_id'])
      || current.timesheet?.timesheet_id !== placement.current_timesheet_id
      || current.timesheet?.contract_id !== placement.current_contract_id
      || current.timesheet?.version !== placement.current_timesheet_version
      || current.contract_record?.id !== placement.current_contract_id
      || current.current_approval_work_id !== placement.current_approval_work_id
      || (current.current_financial?.id ?? null) !== placement.current_financial_id
      || current.contract_record?.candidate_id !== context.candidate_id || current.contract_record?.client_id !== context.client_id
      || current.timesheet?.archived_at_utc != null || current.timesheet?.is_current !== true) {
    fail('SOURCE_NATIVE_CURRENT_ENTITLEMENT_INVALID');
  }
  const policy = placement.moved ? current.timesheet.settings_authority_json?.values : context.policy;
  object(policy);
  const { resolved_at_utc: ignored, ...effectivePolicy } = policy; void ignored;
  return Object.freeze({ ...context, root_timesheet_id: placement.current_timesheet_id,
    contract_id: placement.current_contract_id, timesheet: current.timesheet, contract_record: current.contract_record,
    current_financial: current.current_financial, policy: effectivePolicy });
}

export async function buildSourceUnprotectEconomicTarget({ context, calculation, schedule, sourceObservationSha256,
  quantityPartition, priorCommitmentCutSha256 }) {
  object(context); object(calculation);
  if (context.action !== 'WITHDRAW' || !UUID.test(context.work_event_id) || !HASH.test(sourceObservationSha256)
      || !HASH.test(priorCommitmentCutSha256) || !Array.isArray(schedule) || !Array.isArray(quantityPartition)
      || quantityPartition.length > 1000) fail('SOURCE_NATIVE_ECONOMIC_CONTEXT_INVALID');
  const placement = decodeSourceProtectedCurrentPlacement(context.protected_current_placement, { family_id: context.family_id,
    original_root_timesheet_id: context.root_timesheet_id });
  const current = sourceProtectedCalculationContext(context);
  if (context.requires_zero_financial === true || current.current_financial === null) fail('SOURCE_NATIVE_PREREQUISITE_NOT_PREPARED');
  const snapshot = object(calculation.snapshot ?? calculation);
  if (snapshot.timesheet_id !== placement.current_timesheet_id || snapshot.timesheet_version !== placement.current_timesheet_version
      || snapshot.candidate_id !== context.candidate_id || snapshot.client_id !== context.client_id
      || snapshot.invoice_breakdown_json?.mode !== 'SEGMENTS' || !Array.isArray(snapshot.invoice_breakdown_json.segments)) {
    fail('SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID');
  }
  const policySha256 = await hashSourceNativeJson('SOURCE_UNPROTECT_CALCULATION_POLICY_V1', {
    policy: snapshot.policy_snapshot_json, rate_source_refs: snapshot.rate_source_refs_json,
    contract_record: current.contract_record,
  });
  const components = context.next_source_segments === null
    ? projectWeeklySourceC1ComponentEconomics({ root_timesheet_id: placement.current_timesheet_id,
      financial_snapshot: snapshot, expenses: context.source_expenses })
    : buildWeeklyProtectedNextTarget({ context: current, target: { segments: schedule }, calculation }).components;
  // The full genuine snapshot is retained. It has no newly allocated run,
  // publication or component transport IDs. Stable segment/evidence/tax/VAT/
  // additional/mileage/charge/provider fields are not reduced to a money total.
  const result = {
    version: 'SOURCE_UNPROTECT_ECONOMIC_TARGET_V1', family_id: context.family_id,
    root_timesheet_id: context.root_timesheet_id, work_event_id: context.work_event_id,
    accepted_placement_sha256: placement.moved ? placement.placement_certificate_sha256 : null,
    actual_schedule_json: clone(schedule), tsfin_snapshot_json: clone(snapshot),
    component_economics: clone(components),
    quantity_partition: clone(quantityPartition), provider: clone(context.provider), policy_sha256: policySha256,
    source_observation_sha256: sourceObservationSha256,
    approval_evidence_sha256: placement.immutable_original_evidence_sha256,
    prior_commitment_cut_sha256: priorCommitmentCutSha256,
  };
  return Object.freeze({ economic_target: result,
    economic_target_sha256: await hashSourceNativeJson('SOURCE_UNPROTECT_ECONOMIC_TARGET_V1', result),
    calculation_policy_sha256: policySha256 });
}
