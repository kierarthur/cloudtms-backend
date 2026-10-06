// Protected preview identity only. The established calculator owns every
// clock, bucket, rate and amount. This helper neither authenticates an approval
// basis nor selects historical evidence: the Source caller must supply its
// complete, positively qualified prior components (or [] for a genuine first
// approval). Noncanonical already-used identities are deliberately refused.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
export const PROTECTED_COMPONENT_IDENTITY_VERSION = 'WEEKLY_PROTECTED_COMPONENT_IDENTITY_V1';

function fail(code) { const error = new Error(code); error.code = code; throw error; }
function object(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail('WEEKLY_PROTECTED_COMPONENT_IDENTITY_INVALID');
  }
  return value;
}
function event(value) {
  if (typeof value !== 'string' || !UUID.test(value)) fail('WEEKLY_PROTECTED_COMPONENT_EVENT_INVALID');
  return value;
}
function rows(value) {
  // Preserve the manifest admitted by the existing Source owner. This
  // identity-only projection must not introduce a new per-root shift ceiling.
  if (!Array.isArray(value)) fail('WEEKLY_PROTECTED_COMPONENT_MANIFEST_INVALID');
  return value;
}

export function projectWeeklyProtectedComponentIdentity({ calculation, schedule, approvedComponents }) {
  object(calculation);
  if (calculation.ok !== true) fail('WEEKLY_PROTECTED_COMPONENT_CALCULATION_INVALID');
  const snapshot = object(calculation.snapshot);
  const breakdown = object(snapshot.invoice_breakdown_json);
  const outputs = rows(breakdown.segments);
  const accepted = rows(schedule);
  const prior = rows(approvedComponents);
  if (outputs.length !== accepted.length) fail('WEEKLY_PROTECTED_COMPONENT_MANIFEST_NOT_EXACT');

  const priorEvents = new Set();
  for (const value of prior) {
    const component = object(value);
    if (component.component_kind !== 'WORKED_TIME') continue;
    // Preserve both parts of an already-used component tuple. Never silently
    // rename a frozen legacy component or net it against a newly minted key.
    const id = component.component_member_identity;
    if (typeof id !== 'string' || !UUID.test(id)
        || component.economic_key_type !== 'SEGMENT'
        || component.economic_key_value !== `weekly-source-event:${id}`) {
      fail('WEEKLY_PROTECTED_COMPONENT_LEGACY_IDENTITY_UNAVAILABLE');
    }
    if (priorEvents.has(id)) fail('WEEKLY_PROTECTED_COMPONENT_EVENT_DUPLICATE');
    priorEvents.add(id);
  }

  const seen = new Set();
  const segments = outputs.map((value, index) => {
    const output = object(value);
    const input = object(accepted[index]);
    const id = event(input.work_event_id);
    if (seen.has(id)) fail('WEEKLY_PROTECTED_COMPONENT_EVENT_DUPLICATE');
    seen.add(id);
    // This is an exact output-ordinal association from the same calculator
    // invocation, not a clock/date search for a historical work identity.
    if (output.date !== input.date || output.start !== input.start || output.end !== input.end
        || output.break_mins !== input.break_mins
        || !['WAIT', 'SOURCE', 'ACCEPTED_SOURCE'].includes(input.protected_target_state)) {
      fail('WEEKLY_PROTECTED_COMPONENT_OUTPUT_NOT_EXACT');
    }
    if (output.weekly_source != null && output.weekly_source.work_event_id !== id) {
      fail('WEEKLY_PROTECTED_COMPONENT_SOURCE_IDENTITY_CONFLICT');
    }
    if (Object.hasOwn(output, 'weekly_protected_component_identity')) {
      fail('WEEKLY_PROTECTED_COMPONENT_ALREADY_PROJECTED');
    }
    return {
      ...output,
      segment_id: `weekly-source-event:${id}`,
      // Separate nonmonetary metadata: NEVER fabricate a weekly_source block
      // that initial approval would mistake for actual imported rate evidence.
      weekly_protected_component_identity: {
        schema_version: PROTECTED_COMPONENT_IDENTITY_VERSION,
        work_event_id: id,
      },
    };
  });
  return { ...calculation, snapshot: { ...snapshot,
    invoice_breakdown_json: { ...breakdown, segments } } };
}
