import assert from 'node:assert/strict';
import test from 'node:test';
import { createPhaseController, WEEKLY_SOURCE_PHASE_ORDER } from './phase-controller.mjs';

function passing(phase) {
  return async () => ({
    executed: true,
    pass: true,
    evidence: [{ phase, result: 'PASS' }],
    cleanup: phase.startsWith('db:') ? { complete: true } : null,
    sourceDigest: 'a'.repeat(64),
  });
}

test('TH-029 phase controller runs the exact fixed order and seals each phase', async () => {
  const calls = [];
  const executors = Object.fromEntries(WEEKLY_SOURCE_PHASE_ORDER.map((phase) => [phase, async () => {
    calls.push(phase);
    return passing(phase)();
  }]));
  const result = await createPhaseController({ executors }).runAll();
  assert.deepEqual(calls, WEEKLY_SOURCE_PHASE_ORDER);
  assert.equal(result.pass, true);
  assert.equal(result.results.length, 8);
  assert(result.results.every((item) => /^[a-f0-9]{64}$/.test(item.evidenceDigest)));
});

test('TH-029 complete run fails before execution when an owner is missing', async () => {
  const executors = Object.fromEntries(WEEKLY_SOURCE_PHASE_ORDER.slice(0, -1).map((phase) => [phase, passing(phase)]));
  await assert.rejects(
    () => createPhaseController({ executors }).runAll(),
    (error) => error.code === 'WEEKLY_SOURCE_ALL_PHASES_MISSING' && error.details.missing[0] === 'model',
  );
});

test('TH-029 an unexecuted phase or database phase without cleanup cannot pass', async () => {
  await assert.rejects(
    () => createPhaseController({ executors: { builders: async () => ({ executed: false, pass: true, evidence: [{}] }) } }).runPhase('builders'),
    (error) => error.code === 'WEEKLY_SOURCE_PHASE_NOT_PROVED',
  );
  await assert.rejects(
    () => createPhaseController({ executors: { 'db:new': async () => ({ executed: true, pass: true, evidence: [{}] }) } }).runPhase('db:new'),
    (error) => error.code === 'WEEKLY_SOURCE_PHASE_CLEANUP_MISSING',
  );
});
