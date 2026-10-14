import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { orchestrateWeeklyProtectedAction as individual } from '../broker/src/weekly-source/protected-action-orchestrator.mjs';
import { orchestrateWeeklyProtectedAction as successor, stageWeeklyProtectedAction } from '../broker/src/weekly-source/protected-action-orchestrator-stage-only.mjs';
import { ID, HASH, browserRequest, dependenciesFor } from './fixtures/healthroster-protected-orchestrator-fixture.mjs';

const cases = [
  ['APPROVE_PROTECTED_HOURS', 'APPROVE', {}],
  ['APPROVE_PROTECTED_HOURS', 'APPROVE', { preparedKind: 'AMEND', sourceExpense: false }],
  ['AMEND_PROTECTED_HOURS', 'AMEND', {}],
  ['WITHDRAW_PROTECTED_HOURS', 'WITHDRAW', {}],
  ['WAIT_FOR_SOURCE', 'WAIT', {}],
  ['ACCEPT_SOURCE_AND_RECONCILE', 'RECONCILE', {}],
  ['RECORD_NOT_WORKED', 'RECORD_NOT_WORKED', { sourcePresent: false }],
  ['APPROVE_PROTECTED_HOURS', 'APPROVE', { zeroRoot: true }],
];
for (const [action, kind, options] of cases) {
  test(`individual ${action} ${JSON.stringify(options)} keeps every native call and result`, async () => {
    const originalDependencies = dependenciesFor(kind, options);
    const successorDependencies = dependenciesFor(kind, options);
    const request = browserRequest(action);
    assert.deepEqual(await successor({ action, request, dependencies: successorDependencies }),
      await individual({ action, request, dependencies: originalDependencies }));
    assert.deepEqual(successorDependencies.calls, originalDependencies.calls);
  });
}

test('trusted fresh stage uses the real native calculator/stage and never completes a decision', async () => {
  const dependencies = dependenciesFor('APPROVE');
  const result = await stageWeeklyProtectedAction({ action: 'APPROVE_PROTECTED_HOURS',
    request: browserRequest('APPROVE_PROTECTED_HOURS'), dependencies });
  assert.equal(result.contract, 'WEEKLY_PROTECTED_STAGE_ONLY_V1');
  assert.equal(result.family_id, ID.family);
  assert.equal(result.work_event_id, ID.event);
  assert.equal(result.publication_request_id, dependencies.staged().c1_request.request_id);
  assert.equal(result.request_sha256, dependencies.staged().c1_request.request_sha256);
  assert.equal(dependencies.calls.filter(([name]) => name === 'CALCULATE').length, 1);
  assert.equal(dependencies.calls.filter(([name]) => name === 'weekly_exceptional_pay_stage_c1_request_v1').length, 1);
  assert.equal(dependencies.calls.some(([name]) => ['weekly_exceptional_pay_complete_local_v1', 'C1_RAW'].includes(name)), false);
});

test('exact staged recovery preserves immutable request and does no recalculation/publication', async () => {
  const dependencies = dependenciesFor('APPROVE', { publicationStatus: { ok: true, staged: true,
    publication_request_id: ID.run, request_sha256: HASH } });
  const result = await stageWeeklyProtectedAction({ action: 'APPROVE_PROTECTED_HOURS',
    request: browserRequest('APPROVE_PROTECTED_HOURS'), dependencies });
  assert.equal(result.publication_request_id, ID.run);
  assert.deepEqual(dependencies.calls.map(([name]) => name), [
    'weekly_exceptional_pay_prepare_family_v1', 'weekly_exceptional_pay_action_publication_status_v1',
  ]);
});

test('stage-only rejects WAIT and does not accept browser finance or a completion switch', async () => {
  for (const request of [browserRequest('APPROVE_PROTECTED_HOURS', { pay_amount: 100 }),
    browserRequest('APPROVE_PROTECTED_HOURS', { stage_only: true })]) {
    const dependencies = dependenciesFor('APPROVE');
    await assert.rejects(stageWeeklyProtectedAction({ action: 'APPROVE_PROTECTED_HOURS', request, dependencies }),
      { code: 'WEEKLY_PROTECTED_ACTION_UNKNOWN_FIELD' });
    assert.equal(dependencies.calls.length, 0);
  }
  await assert.rejects(stageWeeklyProtectedAction({ action: 'WAIT_FOR_SOURCE' }),
    { code: 'WEEKLY_PROTECTED_STAGE_ONLY_ACTION_INVALID' });
});

test('no new command/route is mounted by the adjacent source package', () => {
  const routes = readFileSync(new URL('../broker/src/weekly-source/routes.js', import.meta.url), 'utf8');
  assert.doesNotMatch(routes, /protected-action-orchestrator-stage-only|PROTECT_ALL_SIGNED/);
});
