import { stageWeeklyProtectedAction } from './protected-action-orchestrator-stage-only.mjs';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HASH = /^[0-9a-f]{64}$/;
const object = (value) => value && typeof value === 'object' && !Array.isArray(value);
const refuse = (code) => { const error = new Error(code); error.code = code; throw error; };

// Existing finalisation/import owner will invoke this trusted composition only
// after root installs and qualifies the complete native vector assertion.
// No route, scheduler, financial zero or automatic actor is configured here.
export async function orchestrateWeeklyAutomaticReconcile({ sourceCycleId, workEventId, dependencies } = {}) {
  if (!UUID.test(sourceCycleId ?? '') || !UUID.test(workEventId ?? '')
    || typeof dependencies?.dataRpc !== 'function' || typeof dependencies?.calculateWeeklySnapshot !== 'function')
    refuse('WEEKLY_SOURCE_AUTOMATIC_RECONCILE_REQUEST_INVALID');
  const rpc = (mode, fields = {}) => dependencies.dataRpc('weekly_source_automatic_reconcile_atomic_v1', {
    p_request: { source_cycle_id: sourceCycleId, work_event_id: workEventId, mode, ...fields },
  });
  // The server checks the actual qualified capability/assertion before any
  // prepare RPC or calculator. Missing qualification refuses here.
  const preview = await rpc('PREVIEW');
  if (!object(preview)) refuse('WEEKLY_SOURCE_AUTOMATIC_RECONCILE_RESPONSE_INVALID');
  if (preview.ok !== true) return preview;
  let staged = preview.pending_stage;
  if (staged == null) {
    const selection = preview.selection;
    if (!object(selection)) refuse('WEEKLY_SOURCE_AUTOMATIC_RECONCILE_RESPONSE_INVALID');
    if (selection.eligible !== true) return { ok: true, cleared: false, selection };
    const wait = selection.qualified_wait;
    const source = selection.source_proposal;
    if (!object(wait) || !object(source) || !HASH.test(selection.selection_sha256 ?? '')
      || !UUID.test(selection.actor_user_id ?? '') || selection.source_cycle_id !== sourceCycleId
      || selection.work_event_id !== workEventId || !UUID.test(wait.family_id ?? '')
      || !/^[1-9][0-9]*$/.test(String(wait.family_bound_version ?? ''))
      || !HASH.test(source.source_hash ?? '') || !UUID.test(source.source_revision ?? ''))
      refuse('WEEKLY_SOURCE_AUTOMATIC_RECONCILE_RESPONSE_INVALID');
    staged = await stageWeeklyProtectedAction({ action: 'ACCEPT_SOURCE_AND_RECONCILE', dependencies,
      automaticReconcileSelectionSha256: selection.selection_sha256,
      request: { actor_user_id: selection.actor_user_id, source_cycle_id: sourceCycleId, work_event_id: workEventId,
        family_id: wait.family_id, expected_family_bound_version: wait.family_bound_version,
        expected_source_hash: source.source_hash, expected_source_revision: source.source_revision,
        reason: 'Hours matched the client’s finalised shift',
        idempotency_key: `automatic-reconcile:${sourceCycleId}:${workEventId}:${selection.selection_sha256}` },
    });
  }
  if (!object(staged) || !UUID.test(staged.publication_request_id ?? '') || !HASH.test(staged.request_sha256 ?? ''))
    refuse('WEEKLY_SOURCE_AUTOMATIC_RECONCILE_RESPONSE_INVALID');
  return rpc('COMPLETE', { publication_request_id: staged.publication_request_id, request_sha256: staged.request_sha256 });
}
