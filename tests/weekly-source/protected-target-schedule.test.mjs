import assert from 'node:assert/strict';
import test from 'node:test';

import {
  composeWeeklyProtectedTargetSchedule,
  WEEKLY_PROTECTED_TARGET_SCHEDULE_VERSION,
} from '../../broker/src/weekly-source/protected-target-schedule.js';

const EVENT_A = 'a1000000-0000-4000-8000-000000000001';
const EVENT_B = 'a1000000-0000-4000-8000-000000000002';
const EVENT_C = 'a1000000-0000-4000-8000-000000000003';

const source = (workEventId, date, start, end, breakMinutes, extra = {}) => ({
  work_event_id: workEventId,
  date,
  start,
  end,
  break_mins: breakMinutes,
  reference_number: `REF-${workEventId.at(-1)}`,
  ...extra,
});

test('uses source truth for ordinary events and fixed Office truth only for waiting events', () => {
  const result = composeWeeklyProtectedTargetSchedule({
    sourceSegments: [
      source(EVENT_A, '2026-09-01', '09:00', '17:00', 30),
      source(EVENT_B, '2026-09-02', '09:00', '17:00', 30),
    ],
    decisions: [{
      work_event_id: EVENT_A,
      state: 'WAIT',
      fixed_schedule: { date: '2026-09-01', start: '05:00', end: '15:00', break_mins: 60 },
    }],
  });
  assert.equal(result.version, WEEKLY_PROTECTED_TARGET_SCHEDULE_VERSION);
  assert.deepEqual(result.segments.map((row) => ({
    event: row.work_event_id,
    start: row.start,
    end: row.end,
    break: row.break_mins,
    state: row.protected_target_state,
  })), [
    { event: EVENT_A, start: '05:00', end: '15:00', break: 60, state: 'WAIT' },
    { event: EVENT_B, start: '09:00', end: '17:00', break: 30, state: 'SOURCE' },
  ]);
});

test('adds a waiting protected shift even when no source row or candidate Timesheet exists', () => {
  const result = composeWeeklyProtectedTargetSchedule({
    decisions: [{
      work_event_id: EVENT_C,
      state: 'WAIT',
      fixed_schedule: { date: '2026-09-03', start: '20:00', end: '08:00', break_minutes: 60 },
    }],
  });
  assert.equal(result.segments.length, 1);
  assert.equal(result.segments[0].work_event_id, EVENT_C);
  assert.equal(result.segments[0].protected_target_state, 'WAIT');
});

test('accept source follows changed source and complete source disappearance', () => {
  const changed = composeWeeklyProtectedTargetSchedule({
    sourceSegments: [source(EVENT_A, '2026-09-01', '09:00', '18:00', 30)],
    decisions: [{ work_event_id: EVENT_A, state: 'ACCEPTED_SOURCE' }],
  });
  assert.equal(changed.segments[0].end, '18:00');
  assert.equal(changed.segments[0].protected_target_state, 'ACCEPTED_SOURCE');

  const disappeared = composeWeeklyProtectedTargetSchedule({
    sourceSegments: [],
    decisions: [{ work_event_id: EVENT_A, state: 'ACCEPTED_SOURCE' }],
  });
  assert.deepEqual(disappeared.segments, []);
});

test('ten protected hours remain stable while waiting and reconcile only to the complete accepted source schedule', () => {
  const protectedTenHours = {
    work_event_id: EVENT_A,
    state: 'WAIT',
    fixed_schedule: { date: '2026-09-01', start: '09:00', end: '19:00', break_mins: 0 },
  };
  const waitingAgainstNine = composeWeeklyProtectedTargetSchedule({
    sourceSegments: [source(EVENT_A, '2026-09-01', '09:00', '18:00', 0)],
    decisions: [protectedTenHours],
  });
  assert.deepEqual(waitingAgainstNine.segments.map(({ start, end, break_mins, protected_target_state }) => ({
    start, end, break_mins, protected_target_state,
  })), [{ start: '09:00', end: '19:00', break_mins: 0, protected_target_state: 'WAIT' }]);

  const acceptedNine = composeWeeklyProtectedTargetSchedule({
    sourceSegments: [source(EVENT_A, '2026-09-01', '09:00', '18:00', 0)],
    decisions: [{ work_event_id: EVENT_A, state: 'ACCEPTED_SOURCE' }],
  });
  assert.equal(acceptedNine.segments[0].end, '18:00');

  const acceptedTen = composeWeeklyProtectedTargetSchedule({
    sourceSegments: [source(EVENT_A, '2026-09-01', '09:00', '19:00', 0)],
    decisions: [{ work_event_id: EVENT_A, state: 'ACCEPTED_SOURCE' }],
  });
  assert.equal(acceptedTen.segments[0].end, '19:00');

  const acceptedEleven = composeWeeklyProtectedTargetSchedule({
    sourceSegments: [source(EVENT_A, '2026-09-01', '09:00', '20:00', 0)],
    decisions: [{ work_event_id: EVENT_A, state: 'ACCEPTED_SOURCE' }],
  });
  assert.equal(acceptedEleven.segments[0].end, '20:00');

  const acceptedAbsence = composeWeeklyProtectedTargetSchedule({
    sourceSegments: [],
    decisions: [{ work_event_id: EVENT_A, state: 'ACCEPTED_SOURCE' }],
  });
  assert.deepEqual(acceptedAbsence.segments, []);
});

test('record not worked removes the event without affecting another source event', () => {
  const result = composeWeeklyProtectedTargetSchedule({
    sourceSegments: [
      source(EVENT_A, '2026-09-01', '09:00', '17:00', 30),
      source(EVENT_B, '2026-09-02', '09:00', '17:00', 30),
    ],
    decisions: [{ work_event_id: EVENT_A, state: 'NOT_WORKED' }],
  });
  assert.deepEqual(result.segments.map((row) => row.work_event_id), [EVENT_B]);
});

test('refuses duplicate events, invalid schedules and unknown decisions', () => {
  assert.throws(() => composeWeeklyProtectedTargetSchedule({
    sourceSegments: [
      source(EVENT_A, '2026-09-01', '09:00', '17:00', 30),
      source(EVENT_A, '2026-09-01', '09:00', '18:00', 30),
    ],
  }), { code: 'WEEKLY_PROTECTED_SOURCE_EVENT_DUPLICATE' });
  assert.throws(() => composeWeeklyProtectedTargetSchedule({
    decisions: [{
      work_event_id: EVENT_A,
      state: 'WAIT',
      fixed_schedule: { date: '2026-09-01', start: '09:00', end: '09:00', break_mins: 0 },
    }],
  }), { code: 'WEEKLY_PROTECTED_FIXED_SCHEDULE_INVALID' });
  assert.throws(() => composeWeeklyProtectedTargetSchedule({
    decisions: [{ work_event_id: EVENT_A, state: 'PAY_NOW' }],
  }), { code: 'WEEKLY_PROTECTED_DECISION_STATE_INVALID' });
});
