// Finite Office correction transport. Native owners retain actor, scope,
// qualification, replay, CAS, locks, economics and committed cleanup authority.
import { isActivePaymentActor } from './payment-actor-permission.js';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const same = (a, b) => UUID.test(a || '') && UUID.test(b || '') && a.toLowerCase() === b.toLowerCase();
const STATES = new Set(['WAITING', 'APPLIED', 'CANCELLED']);
const MAX_BODY_BYTES = 262144;
const ACTIONS = Object.freeze({
  '/api/timesheets/corrections/send': 'send',
  '/api/timesheets/corrections/authorise': 'authorise',
  '/api/timesheets/corrections/cancel': 'cancel',
  '/api/timesheets/corrections/return-to-draft': 'returnToDraft',
  '/api/timesheets/corrections/lifecycle': 'runLifecycle'
});
function refuse(code, status = 409) {
  const error = new Error(code); error.code = code; error.status = status; throw error;
}
function required(condition, code, status) { if (!condition) refuse(code, status); }
function safeRevision(value) { return Number.isSafeInteger(value) && value > 0; }

export function matchTimesheetCorrectionOfficeRoute(method, pathname) {
  return method === 'POST' && Object.hasOwn(ACTIONS, pathname) ? ACTIONS[pathname] : null;
}

// The MyTMS Office agency identifies Source deployment; the independently
// configured auth tenant is the authority used by existing requireUser tokens.
// Both are server values. This is admission, never a qualification capability.
export function assertTimesheetCorrectionOfficeActor(env, user, mode = 'ADMIN') {
  required(['ADMIN', 'PAY_LIFECYCLE'].includes(mode), 'CORRECTION_ACTOR_NOT_ALLOWED', 403);
  required(env?.CANDIDATE_APP_ENVIRONMENT === 'TEST', 'CORRECTION_ENVIRONMENT_NOT_ALLOWED', 403);
  required(UUID.test(env?.MYTMS_OFFICE_AGENCY_ID || ''), 'CORRECTION_DEPLOYMENT_UNAVAILABLE', 503);
  const authTenant = env?.BANKING_PAY_CORRECTION_TENANT_ID_V1;
  required(typeof authTenant === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(authTenant),
    'CORRECTION_DEPLOYMENT_UNAVAILABLE', 503);
  required(object(user) && (user.role === 'admin' || (mode === 'PAY_LIFECYCLE' && isActivePaymentActor(user))) && user.is_active === true && UUID.test(user.id || '') &&
    same(user.tenant_id, authTenant), 'CORRECTION_ACTOR_NOT_ALLOWED', 403);
  return Object.freeze({ actor_user_id: user.id.toLowerCase(), environment: 'TEST' });
}

async function boundedBody(request) {
  required(request.headers.get('content-type')?.split(';', 1)[0].trim().toLowerCase() === 'application/json',
    'CORRECTION_PAYLOAD_INVALID', 400);
  const length = request.headers.get('content-length');
  if (length !== null) required(/^[0-9]+$/.test(length) && Number(length) <= MAX_BODY_BYTES, 'CORRECTION_PAYLOAD_TOO_LARGE', 413);
  required(request.body !== null, 'CORRECTION_PAYLOAD_INVALID', 400);
  const reader = request.body.getReader(), chunks = []; let size = 0;
  try {
    for (;;) {
      const next = await reader.read(); if (next.done) break;
      size += next.value.byteLength;
      if (size > MAX_BODY_BYTES) { await reader.cancel(); refuse('CORRECTION_PAYLOAD_TOO_LARGE', 413); }
      chunks.push(next.value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  let payload;
  try { payload = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); }
  catch { refuse('CORRECTION_PAYLOAD_INVALID', 400); }
  required(object(payload), 'CORRECTION_PAYLOAD_INVALID', 400);
  return payload;
}

// PREPARE is deliberately absent from ACTIONS. Internal financial/signature
// authority is consumed by the orchestrator and never returned as a detail DTO.
export async function handleTimesheetCorrectionOfficeAction(env, request, action, owners) {
  required(Object.values(ACTIONS).includes(action), 'CORRECTION_ROUTE_NOT_FOUND', 404);
  const url = new URL(request.url);
  required(request.method === 'POST' && matchTimesheetCorrectionOfficeRoute(request.method, url.pathname) === action &&
    [...url.searchParams].length === 0, 'CORRECTION_PAYLOAD_INVALID', 400);
  const payLifecycle = action === 'authorise' || action === 'runLifecycle';
  const user = await owners.requireUser(env, request, payLifecycle ? [] : ['admin']);
  const actor = assertTimesheetCorrectionOfficeActor(env, user, payLifecycle ? 'PAY_LIFECYCLE' : 'ADMIN');
  const payload = await boundedBody(request);
  const orchestrator = owners.createOrchestrator({ user, actor, request, action });
  const receipt = await orchestrator[action](payload, request);
  // Native committed receipts remain raw; no prepared snapshot or error detail
  // is synthesized. A follow-up failure cannot turn committed SQL into rollback.
  let followUpPending = false;
  if (action === 'authorise' || action === 'runLifecycle') {
    try { await owners.afterAuthorise(receipt, actor); }
    catch { followUpPending = true; }
  }
  return { status: receipt?.all_success === false ? 207 : 200,
    body: followUpPending ? { ...receipt, follow_up_pending: true } : receipt };
}

// Resolve actual physical membership, not a caller correction/request label.
// These are typed service readers wired to literal existing RPC owners in index.
export function createTimesheetCorrectionLifecycleResolver(owners) {
  let admitted = null;
  async function resolveAll(ids, action, actor) {
    required(Array.isArray(ids) && ids.length > 0 && ids.length <= 100 && ids.every(id => UUID.test(id)) &&
      new Set(ids.map(id => id.toLowerCase())).size === ids.length, 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
    const contexts = await owners.readContexts(ids);
    required(Array.isArray(contexts) && contexts.length === ids.length, 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
    const physical = new Map();
    for (const context of contexts) {
      const id = context?.effective_timesheet_id, ts = context?.out_timesheet;
      required(ids.some(x => same(x, id)) && !physical.has(String(id).toLowerCase()) && object(ts) &&
        same(ts.timesheet_id, id) && ts.is_current === true && ts.archived_at_utc == null &&
        UUID.test(context?.out_candidate?.id || ''), 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      physical.set(id.toLowerCase(), { ts, candidateId: context.out_candidate.id });
    }
    // Current physical rows have been resolved above. No caller Source flag
    // or reserved future reversal is used as a service scope witness.
    if (typeof owners.assertSubjects === 'function') await owners.assertSubjects([...physical.keys()], actor, action);
    const units = new Map(), members = new Map();
    for (const [id, context] of physical) {
      const projection = await owners.readProjection(id, context.candidateId);
      if (projection === null) {
        required(!['WHOLE_TIMESHEET_REPLACEMENT', 'WHOLE_TIMESHEET_REVERSAL'].includes(context.ts.correction_kind),
          'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
        continue;
      }
      required(object(projection) && projection.schema_version === 1 && UUID.test(projection.request_id || '') &&
        UUID.test(projection.root_timesheet_id || '') && UUID.test(projection.reversed_timesheet_id || '') &&
        UUID.test(projection.replacement_timesheet_id || '') && same(projection.candidate_id, context.candidateId) &&
        STATES.has(projection.state) && safeRevision(projection.state_revision) &&
        projection.correction_id === `awc:${projection.root_timesheet_id}:${projection.request_id}`,
        'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      // Original-only selection remains ordinary. A selected actual P0/R0 leg
      // must carry this exact stored correction identity before it can group A0.
      if (!same(id, projection.replacement_timesheet_id) && context.ts.correction_kind !== 'WHOLE_TIMESHEET_REVERSAL') continue;
      required(context.ts.correction_id === projection.correction_id, 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      let unit = units.get(projection.request_id);
      if (unit) required(unit.state === projection.state && unit.state_revision === projection.state_revision &&
        same(unit.replacement_timesheet_id, projection.replacement_timesheet_id), 'CORRECTION_REQUEST_CAS_STALE');
      else {
        const prepared = await owners.readPrepare({ request_id: projection.request_id, expected_state_revision: projection.state_revision });
        if (projection.state === 'WAITING') {
          const a = prepared?.authority;
          required(prepared?.contract_version === 'TIMESHEET_CORRECTION_PREPARE_V1' && same(a?.request_id, projection.request_id) &&
            a.state_revision === projection.state_revision && same(a.replacement_timesheet_id, projection.replacement_timesheet_id) &&
            same(a.original_timesheet_id, projection.reversed_timesheet_id) && UUID.test(a.planned_reversal_timesheet_id || '') &&
            typeof prepared.original_authorisation_required === 'boolean', 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
          unit = { request_id: projection.request_id, state: 'WAITING', state_revision: projection.state_revision,
            replacement_timesheet_id: a.replacement_timesheet_id, reversal_timesheet_id: null,
            original_authorisation_required: prepared.original_authorisation_required,
            original_timesheet_id: a.original_timesheet_id, candidate_id: projection.candidate_id };
        } else if (projection.state === 'APPLIED') {
          required(prepared?.state === 'APPLIED' && same(prepared.request_id, projection.request_id) &&
            prepared.state_revision === projection.state_revision && same(prepared.replacement_timesheet_id, projection.replacement_timesheet_id) &&
            same(prepared.reversed_timesheet_id, projection.reversed_timesheet_id) && UUID.test(prepared.reversal_timesheet_id || ''),
            'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
          const transition = await owners.readTransition(id, action, actor);
          required(transition?.ok === true && transition.valid === true && transition.action_ready === true &&
            transition.action === action && transition.correction_shape === 'WHOLE_REVERSAL_REPLACEMENT' &&
            same(transition.request_id, projection.request_id) && transition.correction_id === projection.correction_id &&
            same(transition.root_timesheet_id, projection.root_timesheet_id) && /^[0-9a-f]{64}$/.test(transition.chain_fingerprint || '') &&
            [2, 3].includes(transition.expected_member_count) && Array.isArray(transition.transition_items),
            'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
          // Exact native request identity is awc:<root>:<request>; approval
          // prefixes that stored identity with correction: for these group
          // fields. Its native count, versions and IDs remain
          // the actual read-only transition owner's values, in native order.
          unit = { request_id: projection.request_id, state: 'APPLIED', state_revision: projection.state_revision,
            replacement_timesheet_id: prepared.replacement_timesheet_id, reversal_timesheet_id: prepared.reversal_timesheet_id,
            original_authorisation_required: transition.expected_member_count === 3,
            original_timesheet_id: prepared.reversed_timesheet_id, candidate_id: projection.candidate_id,
            expected_member_count: transition.expected_member_count,
            transition_items: transition.transition_items.map(item => ({ ...item,
              lifecycle_group_id: `correction:${transition.correction_id}`, lifecycle_group_size: transition.expected_member_count })) };
        } else refuse('CORRECTION_REQUEST_CANCELLED');
        if (unit.state === 'APPLIED' && typeof owners.assertSubjects === 'function') {
          required(unit.transition_items.length === unit.expected_member_count && unit.transition_items.every(item =>
            UUID.test(item.timesheet_id || '')) && new Set(unit.transition_items.map(item => item.timesheet_id.toLowerCase())).size === unit.expected_member_count,
            'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
          await owners.assertSubjects(unit.transition_items.map(item => item.timesheet_id), actor, action);
        }
        units.set(projection.request_id, unit);
      }
      required(same(id, unit.replacement_timesheet_id) || same(id, unit.reversal_timesheet_id), 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      members.set(id, unit);
    }
    for (const [id, context] of physical) {
      if (members.has(id)) continue;
      const joins = [...units.values()].filter(unit => unit.original_authorisation_required === true &&
        same(unit.original_timesheet_id, id) && same(unit.candidate_id, context.candidateId));
      required(joins.length <= 1, 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
      if (joins.length === 1) members.set(id, joins[0]);
    }
    return { ids: [...ids], action, actorId: actor.actor_user_id, members };
  }
  return async function resolve({ timesheetId, selectedTimesheetIds, action, actor }) {
    if (admitted === null) admitted = resolveAll(selectedTimesheetIds, action, actor);
    const scope = await admitted;
    required(scope.action === action && same(scope.actorId, actor.actor_user_id) &&
      JSON.stringify(scope.ids) === JSON.stringify(selectedTimesheetIds), 'CORRECTION_LIFECYCLE_SCOPE_UNPROVED');
    const unit = scope.members.get(timesheetId.toLowerCase());
    return unit ? { ...unit, kind: 'CORRECTION_REQUEST', selected_timesheet_id: timesheetId } : null;
  };
}
