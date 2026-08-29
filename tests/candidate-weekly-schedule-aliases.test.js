import assert from 'node:assert/strict';
import test from 'node:test';

import { candidateWeeklyScheduleInternals } from '../broker/src/index.js';

test('Candidate weekly schedules accept the canonical start_time and end_time fields', () => {
  assert.deepEqual(
    candidateWeeklyScheduleInternals.candidateScheduleLocalTimeAliases({
      date: '2026-07-28',
      start_time: '09:00',
      end_time: '17:00'
    }),
    { start: '09:00', end: '17:00' }
  );
});

test('legacy start and end fields remain compatible and take precedence', () => {
  assert.deepEqual(
    candidateWeeklyScheduleInternals.candidateScheduleLocalTimeAliases({
      start: '08:00',
      end: '16:00',
      start_time: '09:00',
      end_time: '17:00'
    }),
    { start: '08:00', end: '16:00' }
  );
});

test('normalised schedule fields can fall back to the original factual segment', () => {
  assert.deepEqual(
    candidateWeeklyScheduleInternals.candidateScheduleLocalTimeAliases(
      { date: '2026-07-29' },
      { start_time: '10:30', end_time: '18:30' }
    ),
    { start: '10:30', end: '18:30' }
  );
});

