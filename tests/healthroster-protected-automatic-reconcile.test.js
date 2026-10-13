import test from 'node:test';
import assert from 'node:assert/strict';
import { orchestrateWeeklyAutomaticReconcile } from '../broker/src/weekly-source/protected-automatic-reconcile.mjs';
import { ID, HASH, dependenciesFor } from './fixtures/healthroster-protected-orchestrator-fixture.mjs';
import { readFileSync } from 'node:fs';
import { produceAutomaticReconcileStageFixture } from '../scripts/healthroster-automatic-reconcile-fixture.mjs';

const input = (dependencies) => ({ sourceCycleId: ID.cycle, workEventId: ID.event, dependencies });
const preview = { ok: true, selection: { eligible: true, actor_user_id: ID.actor, source_cycle_id: ID.cycle,
  work_event_id: ID.event, selection_sha256: HASH, qualified_wait: { family_id: ID.family, family_bound_version: '4' },
  source_proposal: { source_hash: HASH, source_revision: ID.cycle } } };

test('missing qualified delta capability refuses before any native preparation or calculator', async () => {
  const calls = [];
  const dependencies = { calculateWeeklySnapshot: () => assert.fail('calculator must not run'),
    dataRpc: async (name, payload) => { calls.push([name, payload]);
      const error = new Error('Unavailable'); error.code = 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE'; throw error; } };
  await assert.rejects(orchestrateWeeklyAutomaticReconcile(input(dependencies)),
    { code: 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' });
  assert.equal(calls.length, 1);
  assert.equal(calls[0][1].p_request.mode, 'PREVIEW');
});

test('no configured actor or unequal clocks returns the server decision without staging', async () => {
  for (const result of [{ ok: false, code: 'AUTOMATIC_ACTOR_UNAVAILABLE' },
    { ok: true, selection: { eligible: false, code: 'PROTECTED_SOURCE_HOURS_DIFFER' } }]) {
    const calls = [];
    const dependencies = { calculateWeeklySnapshot: () => assert.fail('calculator must not run'),
      dataRpc: async (name) => { calls.push(name); return result; } };
    const actual = await orchestrateWeeklyAutomaticReconcile(input(dependencies));
    assert.equal(actual.cleared ?? false, false);
    assert.equal(calls.length, 1);
  }
});

test('actual native reconcile staging seals the server selection and completion receives IDs only', async () => {
  const dependencies = dependenciesFor('RECONCILE');
  const native = dependencies.dataRpc; const calls = [];
  dependencies.dataRpc = async (name, payload) => {
    if (name !== 'weekly_source_automatic_reconcile_atomic_v1') return native(name, payload);
    calls.push(payload.p_request);
    return payload.p_request.mode === 'PREVIEW' ? preview : { ok: false, code: 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' };
  };
  const result = await orchestrateWeeklyAutomaticReconcile(input(dependencies));
  assert.equal(result.ok, false);
  assert.equal(result.cleared ?? false, false);
  assert.equal(dependencies.staged().automatic_reconcile_selection_sha256, HASH);
  assert.deepEqual(Object.keys(calls[1]).sort(), ['mode','publication_request_id','request_sha256','source_cycle_id','work_event_id'].sort());
  assert.equal(dependencies.calls.some(([name]) => name === 'weekly_exceptional_pay_complete_local_v1'), false);
});

test('server-proved pending stage recovery does no new calculation or native preparation', async () => {
  const calls = [];
  const dependencies = { calculateWeeklySnapshot: () => assert.fail('calculator must not run'),
    dataRpc: async (name, payload) => { calls.push([name,payload.p_request]);
      return payload.p_request.mode === 'PREVIEW' ? { ok: true, pending_stage: { publication_request_id: ID.run, request_sha256: HASH } }
        : { ok: false, code: 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' }; } };
  const result = await orchestrateWeeklyAutomaticReconcile(input(dependencies));
  assert.equal(result.ok, false);
  assert.deepEqual(calls.map(([, payload]) => payload.mode), ['PREVIEW', 'COMPLETE']);
});

test('immutable shape fixture is produced by the actual native transport and component identity owners', async () => {
  const frozen = JSON.parse(readFileSync(new URL('./fixtures/healthroster-automatic-reconcile-produced-stage.json', import.meta.url), 'utf8'));
  assert.deepEqual(await produceAutomaticReconcileStageFixture(), frozen);
  const segments = frozen.stage_request.target_snapshot.tsfin_snapshot_json.invoice_breakdown_json.segments;
  assert.equal(segments[0].segment_id, `weekly-source-event:${ID.event}`);
  assert.equal(segments[0].weekly_protected_component_identity.work_event_id, ID.event);
  assert.equal(Object.hasOwn(frozen, 'delta_certificate'), false);
});
