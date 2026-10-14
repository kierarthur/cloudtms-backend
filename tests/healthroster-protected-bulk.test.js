import test from 'node:test';
import assert from 'node:assert/strict';
import { orchestrateWeeklyProtectedBulk } from '../broker/src/weekly-source/protected-bulk-action.mjs';
import { ID, HASH, dependenciesFor } from './fixtures/healthroster-protected-orchestrator-fixture.mjs';
const key = 'bulk-protect-fixture-0001';
const item = { incident_id: ID.source, work_event_id: ID.event, candidate_id: ID.candidate,
  client_id: ID.client, contract_id: ID.contract, claim: { signature_kind: 'CANDIDATE_WORKFLOW',
    evidence_timesheet_id: ID.root, week_ending_date: '2026-09-20', work_date: '2026-09-14',
    start: '09:00', end: '17:00', break_minutes: 30 } };
const selection = { source_cycle_id: ID.cycle, selection_sha256: HASH,
  source_cas: { source_cas_sha256: HASH } };
const request = { source_cycle_id: ID.cycle, mode: 'COMMIT', expected_selection_sha256: HASH, bulk_request_key: key };
function fixture({ failAfterFirst = false } = {}) {
  const dependencies = dependenciesFor('APPROVE');
  const originalRpc = dependencies.dataRpc;
  let rounds = 0;
  const rpcCalls = [];
  dependencies.dataRpc = async (name, args) => {
    if (name !== 'weekly_source_protect_all_signed_atomic_v1') {
      const result = await originalRpc(name, args);
      if (name === 'weekly_exceptional_pay_action_context_v1') result.candidate_submission = {
        source_system: 'CLOUDTMS_CANDIDATE_TIMESHEET', external_identity: ID.root,
        external_revision: '1', document_sha256: HASH, work_date: '2026-09-14',
        submitted_by_candidate_id: ID.candidate, submission_id: ID.root,
        submitted_at_utc: '2026-09-20T18:00:00Z', submitted_minutes: 450,
      };
      return result;
    }
    rpcCalls.push(args.p_request);
    if (args.p_request.mode === 'PREVIEW') return { ok: true, mode: 'PREVIEW' };
    if (args.p_request.phase === 'ADMIT') return { ok: true, phase: 'ADMITTED' };
    if (args.p_request.phase === 'ROUND') {
      rounds += 1;
      if (rounds > 1 && failAfterFirst) throw Object.assign(new Error('bounded source refusal'), { code: 'WEEKLY_SOURCE_BULK_SOURCE_CHANGED' });
      return { ok: true, phase: 'ROUND', selection, remaining: rounds === 1 ? [item] : [],
        pending_stages: [], max_stages_per_round: 1, completed_work_event_ids: rounds === 1 ? [] : [ID.event] };
    }
    if (args.p_request.phase === 'APPLY') return { ok: true, phase: 'ROUND_COMPLETED', native_result: { outcome: 'PUBLISHED' } };
    throw new Error('Unexpected phase');
  };
  return { dependencies, rpcCalls };
}

test('bulk stages one server-owned claim and calls native APPLY with only its exact request hash', async () => {
  const { dependencies, rpcCalls } = fixture();
  const result = await orchestrateWeeklyProtectedBulk({ request, actorUserId: ID.actor, dependencies });
  assert.equal(result.completed, true);
  assert.equal(result.whole_command_atomic, false);
  assert.deepEqual(rpcCalls.map((row) => row.phase), ['ADMIT', 'ROUND', 'APPLY', 'ROUND']);
  const stage = dependencies.staged();
  assert.deepEqual(stage.bulk_signed_selection_proof, { expected_selection_sha256: HASH,
    expected_source_cas_sha256: HASH, incident_id: ID.source, bulk_request_key: key });
  assert.equal(stage.evidence_timesheet_id, ID.root);
  assert.deepEqual(Object.keys(rpcCalls[2].staged[0]).sort(), ['publication_request_id', 'request_sha256']);
  assert.equal(dependencies.calls.some(([name]) => name === 'weekly_exceptional_pay_complete_local_v1'), false);
});

test('a later Source refusal reports already committed rounds and keeps the original request key', async () => {
  const { dependencies, rpcCalls } = fixture({ failAfterFirst: true });
  const result = await orchestrateWeeklyProtectedBulk({ request, actorUserId: ID.actor, dependencies });
  assert.equal(result.completed, false);
  assert.equal(result.partial, true);
  assert.equal(result.code, 'WEEKLY_SOURCE_BULK_SOURCE_CHANGED');
  assert.equal(result.results.length, 1);
  assert.ok(rpcCalls.every((row) => row.bulk_request_key === key));
});

test('trusted round budget returns an honest continuation after native completion', async () => {
  const { dependencies } = fixture();
  const result = await orchestrateWeeklyProtectedBulk({ request, actorUserId: ID.actor, dependencies, maxRounds: 1 });
  assert.equal(result.completed, false);
  assert.equal(result.continuation_required, true);
  assert.equal(result.results.length, 1);
});

test('preview makes one read-only owner call and rejects browser actors, financial payloads and phase controls', async () => {
  const { dependencies, rpcCalls } = fixture();
  await orchestrateWeeklyProtectedBulk({ request: { source_cycle_id: ID.cycle, mode: 'PREVIEW' }, actorUserId: ID.actor, dependencies });
  assert.equal(rpcCalls.length, 1);
  for (const override of [{ actor_user_id: ID.actor }, { pay_ex_vat: 100 }, { staged: [] }, { phase: 'APPLY' }]) {
    await assert.rejects(orchestrateWeeklyProtectedBulk({ request: { ...request, ...override }, actorUserId: ID.actor, dependencies }),
      { code: 'WEEKLY_SOURCE_BULK_REQUEST_INVALID' });
  }
  assert.equal(rpcCalls.length, 1);
});
