import test from 'node:test';
import assert from 'node:assert/strict';

import { normalisePostgresTimestampIso } from '../src/timestamp-normalisation.js';

test('normalises PostgreSQL timestamptz text used by imported correction schedules', () => {
  assert.equal(
    normalisePostgresTimestampIso('2026-06-30 08:00:00+00'),
    '2026-06-30T08:00:00.000Z'
  );
  assert.equal(
    normalisePostgresTimestampIso('2026-06-30 09:00:00+0100'),
    '2026-06-30T08:00:00.000Z'
  );
});

test('preserves browser-style ISO instants and rejects invalid values', () => {
  assert.equal(
    normalisePostgresTimestampIso('2026-06-30T08:00:00Z'),
    '2026-06-30T08:00:00.000Z'
  );
  assert.equal(normalisePostgresTimestampIso('not-a-timestamp'), null);
  assert.equal(normalisePostgresTimestampIso(null), null);
});
