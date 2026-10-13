import { stageWeeklyProtectedAction } from './protected-action-orchestrator-stage-only.mjs';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HASH = /^[0-9a-f]{64}$/;
export class WeeklyProtectedBulkError extends Error {
  constructor(code) { super(code); this.code = code; }
}
const refuse = (code) => { throw new WeeklyProtectedBulkError(code); };
const object = (value) => value && typeof value === 'object' && !Array.isArray(value);

// Trusted Worker composition only. Root injects the authenticated actor and
// mounts the existing commands route after every required native/app gate.
export async function orchestrateWeeklyProtectedBulk({ request, actorUserId, dependencies, maxRounds = 8 } = {}) {
  if (!object(request) || Object.keys(request).some((key) => ![
    'source_cycle_id', 'mode', 'expected_selection_sha256', 'bulk_request_key',
  ].includes(key)) || !UUID.test(request.source_cycle_id ?? '') || !UUID.test(actorUserId ?? '')
    || typeof dependencies?.dataRpc !== 'function') refuse('WEEKLY_SOURCE_BULK_REQUEST_INVALID');
  if (!Number.isSafeInteger(maxRounds) || maxRounds < 1 || maxRounds > 32) refuse('WEEKLY_SOURCE_BULK_REQUEST_INVALID');
  const rpc = async (payload) => {
    const result = await dependencies.dataRpc('weekly_source_protect_all_signed_atomic_v1', {
      p_request: { ...payload, actor_user_id: actorUserId, source_cycle_id: request.source_cycle_id },
    });
    if (!object(result)) refuse('WEEKLY_SOURCE_BULK_RESPONSE_INVALID');
    return result;
  };
  if (request.mode === 'PREVIEW') {
    if (Object.hasOwn(request, 'bulk_request_key') || Object.hasOwn(request, 'expected_selection_sha256'))
      refuse('WEEKLY_SOURCE_BULK_REQUEST_INVALID');
    return rpc({ mode: 'PREVIEW' });
  }
  if (request.mode !== 'COMMIT' || !HASH.test(request.expected_selection_sha256 ?? '')
    || typeof request.bulk_request_key !== 'string' || request.bulk_request_key.length < 16
    || request.bulk_request_key.length > 120 || !/^[A-Za-z0-9:_-]+$/.test(request.bulk_request_key)
    || typeof dependencies.calculateWeeklySnapshot !== 'function')
    refuse('WEEKLY_SOURCE_BULK_REQUEST_INVALID');
  const shared = { mode: 'COMMIT', bulk_request_key: request.bulk_request_key };
  const admitted = await rpc({ ...shared, phase: 'ADMIT', expected_selection_sha256: request.expected_selection_sha256 });
  if (admitted.ok !== true || admitted.phase !== 'ADMITTED') return admitted;
  const results = [];
  let completedBefore = [];
  try {
  for (let round = 0; round < maxRounds; round += 1) {
    const scope = await rpc({ ...shared, phase: 'ROUND' });
    if (scope.ok !== true) return { ...scope, results, whole_command_atomic: false };
    if (scope.phase !== 'ROUND' || !Array.isArray(scope.remaining) || !Array.isArray(scope.pending_stages)
      || !object(scope.selection) || scope.max_stages_per_round !== 1) refuse('WEEKLY_SOURCE_BULK_RESPONSE_INVALID');
    if (scope.remaining.length === 0) return { ok: true, completed: true, results,
      completed_work_event_ids: scope.completed_work_event_ids, whole_command_atomic: false };
    if (Array.isArray(scope.completed_work_event_ids)) completedBefore = scope.completed_work_event_ids;
    if (scope.pending_stages.length > 1) refuse('WEEKLY_SOURCE_BULK_ROUND_INVALID');
    let staged;
    if (scope.pending_stages.length === 1) {
      const pending = scope.pending_stages[0];
      const item = scope.remaining.find((row) => row.work_event_id === pending.work_event_id);
      if (!item) refuse('WEEKLY_SOURCE_BULK_ORIGINAL_CLAIM_CHANGED');
      // Recover through the same native prepare/status path. No financial value
      // or publication hash from the browser is forwarded.
      staged = await stage(item, scope.selection, shared.bulk_request_key, actorUserId, dependencies);
      if (staged.publication_request_id !== pending.publication_request_id) refuse('WEEKLY_SOURCE_BULK_REPLAY_CONFLICT');
    } else {
      staged = await stage(scope.remaining[0], scope.selection, shared.bulk_request_key, actorUserId, dependencies);
    }
    const result = await rpc({ ...shared, phase: 'APPLY', staged: [{
      publication_request_id: staged.publication_request_id, request_sha256: staged.request_sha256,
    }] });
    results.push(result);
    if (result.ok !== true) return { ok: false, completed: false, partial: completedBefore.length > 0 || results.some((row) => row.ok === true),
      results, continuation_required: true, whole_command_atomic: false };
  }
  } catch (error) {
    const code = /^[A-Z][A-Z0-9_]{0,119}$/.test(error?.code ?? '') ? error.code : 'PROTECTION_REFUSED';
    return { ok: false, completed: false, code, results, completed_work_event_ids: completedBefore,
      partial: completedBefore.length > 0 || results.some((row) => row.ok === true),
      continuation_required: true, whole_command_atomic: false };
  }
  return { ok: true, completed: false, results, continuation_required: true, whole_command_atomic: false };
}

async function stage(item, selection, bulkKey, actor, dependencies) {
  if (!object(item) || !object(item.claim) || item.claim.signature_kind !== 'CANDIDATE_WORKFLOW'
    || !UUID.test(item.work_event_id ?? '') || !HASH.test(selection.selection_sha256 ?? '')
    || !HASH.test(selection.source_cas?.source_cas_sha256 ?? '')) refuse('WEEKLY_SOURCE_BULK_RESPONSE_INVALID');
  return stageWeeklyProtectedAction({ action: 'APPROVE_PROTECTED_HOURS', dependencies,
    request: { actor_user_id: actor, source_cycle_id: selection.source_cycle_id,
      candidate_id: item.candidate_id, client_id: item.client_id, contract_id: item.contract_id,
      work_event_id: item.work_event_id, evidence_timesheet_id: item.claim.evidence_timesheet_id,
      week_ending_date: item.claim.week_ending_date, work_date: item.claim.work_date,
      start: item.claim.start, end: item.claim.end, break_minutes: item.claim.break_minutes,
      reason: 'Protected from the signed MyTMS submission',
      idempotency_key: `${bulkKey}:shift:${item.work_event_id}` },
    bulkSignedSelectionProof: { expected_selection_sha256: selection.selection_sha256,
      expected_source_cas_sha256: selection.source_cas.source_cas_sha256,
      incident_id: item.incident_id, bulk_request_key: bulkKey },
  });
}
