import assert from 'node:assert/strict';
import test from 'node:test';
import { createFixedClock } from './fixed-clock.mjs';

test('TH-015 fixed clock proves before, exactly at and after a deadline without sleeping', () => {
  const clock = createFixedClock('2026-09-15T14:59:59Z');
  assert.equal(clock.nowUtc(), '2026-09-15T14:59:59.000Z');
  assert.equal(clock.advanceBy({ seconds: 1 }), '2026-09-15T15:00:00.000Z');
  assert.equal(clock.advanceBy({ seconds: 1 }), '2026-09-15T15:00:01.000Z');
  assert.deepEqual(clock.history(), [
    '2026-09-15T14:59:59.000Z',
    '2026-09-15T15:00:00.000Z',
    '2026-09-15T15:00:01.000Z'
  ]);
});

test('TH-015 fixed UTC authority remains deterministic across the British DST transition', () => {
  const clock = createFixedClock('2026-03-29T00:59:59Z');
  clock.advanceBy({ seconds: 1 });
  assert.equal(clock.nowUtc(), '2026-03-29T01:00:00.000Z');
  assert.equal(clock.now().getTime(), Date.parse('2026-03-29T01:00:00Z'));
});

test('TH-015 refuses browser-shaped time, invalid advances and backwards travel', () => {
  assert.throws(() => createFixedClock('2026-09-15T10:00:00+01:00'), /ending in Z/);
  assert.throws(() => createFixedClock('2026-02-30T10:00:00Z'), /real UTC instant/);
  const clock = createFixedClock('2026-09-15T09:00:00Z');
  assert.throws(() => clock.advanceBy({ minutes: -1 }), /non-negative/);
  assert.throws(() => clock.advanceTo('2026-09-15T08:59:59Z'), /backwards/);
});
