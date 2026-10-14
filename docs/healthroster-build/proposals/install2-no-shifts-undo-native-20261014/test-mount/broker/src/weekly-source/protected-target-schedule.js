const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const TIME_PATTERN = /^(?:[01]\d|2[0-3]):[0-5]\d$/;
const DECISION_STATES = new Set(['WAIT', 'ACCEPTED_SOURCE', 'NOT_WORKED']);

export const WEEKLY_PROTECTED_TARGET_SCHEDULE_VERSION =
  'WEEKLY_PROTECTED_TARGET_SCHEDULE_V1';

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function object(value, code, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail(code, `${label} is unavailable.`);
  }
  return value;
}

function uuid(value, code, label) {
  const normalised = String(value ?? '').trim().toLowerCase();
  if (!UUID_PATTERN.test(normalised)) fail(code, `${label} is invalid.`);
  return normalised;
}

function text(value, code, label) {
  const normalised = String(value ?? '').trim();
  if (!normalised) fail(code, `${label} is unavailable.`);
  return normalised;
}

function schedule(value, code) {
  const row = object(value, code, 'Shift');
  const workDate = String(row.work_date ?? row.date ?? '').trim();
  const start = String(row.start ?? row.start_time ?? '').trim();
  const end = String(row.end ?? row.end_time ?? '').trim();
  const breakMinutes = Number(row.break_minutes ?? row.break_mins ?? 0);
  if (!DATE_PATTERN.test(workDate) || !TIME_PATTERN.test(start) || !TIME_PATTERN.test(end)) {
    fail(code, 'The shift date, start or finish is invalid.');
  }
  if (!Number.isSafeInteger(breakMinutes) || breakMinutes < 0) {
    fail(code, 'The shift break must be a non-negative whole number of minutes.');
  }
  const startMinutes = Number(start.slice(0, 2)) * 60 + Number(start.slice(3));
  const endMinutes = Number(end.slice(0, 2)) * 60 + Number(end.slice(3));
  const elapsedMinutes = endMinutes > startMinutes
    ? endMinutes - startMinutes
    : (24 * 60) - startMinutes + endMinutes;
  if (start === end || breakMinutes >= elapsedMinutes) {
    fail(code, 'The shift finish and break do not leave positive worked time.');
  }
  return Object.freeze({
    date: workDate,
    start,
    end,
    break_mins: breakMinutes,
  });
}

function sourceSegment(value) {
  const row = object(value, 'WEEKLY_PROTECTED_SOURCE_SEGMENT_INVALID', 'Source shift');
  const workEventId = uuid(
    row.work_event_id,
    'WEEKLY_PROTECTED_SOURCE_EVENT_INVALID',
    'Source work event',
  );
  return Object.freeze({
    workEventId,
    schedule: schedule(row, 'WEEKLY_PROTECTED_SOURCE_SEGMENT_INVALID'),
    original: Object.freeze({ ...row }),
  });
}

function decision(value) {
  const row = object(value, 'WEEKLY_PROTECTED_DECISION_INVALID', 'Protected-hours decision');
  const workEventId = uuid(
    row.work_event_id,
    'WEEKLY_PROTECTED_DECISION_EVENT_INVALID',
    'Protected-hours work event',
  );
  const state = text(
    row.state,
    'WEEKLY_PROTECTED_DECISION_STATE_INVALID',
    'Protected-hours decision',
  ).toUpperCase();
  if (!DECISION_STATES.has(state)) {
    fail('WEEKLY_PROTECTED_DECISION_STATE_INVALID', 'The protected-hours decision is invalid.');
  }
  const fixedSchedule = state === 'WAIT'
    ? schedule(row.fixed_schedule, 'WEEKLY_PROTECTED_FIXED_SCHEDULE_INVALID')
    : null;
  return Object.freeze({ workEventId, state, fixedSchedule });
}

function sortedUnique(rows, key, duplicateCode) {
  const seen = new Set();
  for (const row of rows) {
    const value = key(row);
    if (seen.has(value)) fail(duplicateCode, 'The same work event was supplied more than once.');
    seen.add(value);
  }
  return rows;
}

function outputSegment(source, protectedDecision) {
  const activeSchedule = protectedDecision?.state === 'WAIT'
    ? protectedDecision.fixedSchedule
    : source?.schedule;
  if (!activeSchedule) return null;
  return Object.freeze({
    ...(source?.original ?? {}),
    ...activeSchedule,
    work_event_id: protectedDecision?.workEventId ?? source.workEventId,
    protected_target_state: protectedDecision?.state ?? 'SOURCE',
  });
}

/**
 * Compose the complete Candidate + Contract + week schedule before it is fed
 * to the established Weekly financial snapshot owner.
 *
 * Source shifts remain authoritative for every event that Office has not put
 * on Wait.  A waiting event uses the immutable Office-approved schedule; an
 * accepted-source event follows the current source (including disappearance),
 * and a not-worked event contributes no worked segment.  This helper performs
 * no rate, pay, recovery, invoice or Banking calculation.
 */
export function composeWeeklyProtectedTargetSchedule(input = {}) {
  const sourceRows = sortedUnique(
    (Array.isArray(input.sourceSegments) ? input.sourceSegments : []).map(sourceSegment),
    (row) => row.workEventId,
    'WEEKLY_PROTECTED_SOURCE_EVENT_DUPLICATE',
  );
  const decisions = sortedUnique(
    (Array.isArray(input.decisions) ? input.decisions : []).map(decision),
    (row) => row.workEventId,
    'WEEKLY_PROTECTED_DECISION_EVENT_DUPLICATE',
  );

  const sourceByEvent = new Map(sourceRows.map((row) => [row.workEventId, row]));
  const decisionByEvent = new Map(decisions.map((row) => [row.workEventId, row]));
  const eventIds = new Set([...sourceByEvent.keys(), ...decisionByEvent.keys()]);
  const segments = [];
  for (const workEventId of eventIds) {
    const source = sourceByEvent.get(workEventId) ?? null;
    const protectedDecision = decisionByEvent.get(workEventId) ?? null;
    if (protectedDecision?.state === 'NOT_WORKED') continue;
    const segment = outputSegment(source, protectedDecision);
    if (segment) segments.push(segment);
  }
  segments.sort((left, right) => (
    left.date.localeCompare(right.date)
    || left.start.localeCompare(right.start)
    || left.end.localeCompare(right.end)
    || left.work_event_id.localeCompare(right.work_event_id)
  ));

  return Object.freeze({
    version: WEEKLY_PROTECTED_TARGET_SCHEDULE_VERSION,
    segments: Object.freeze(segments),
    source_event_count: sourceRows.length,
    decision_event_count: decisions.length,
    waiting_event_count: decisions.filter((row) => row.state === 'WAIT').length,
    accepted_source_event_count: decisions.filter((row) => row.state === 'ACCEPTED_SOURCE').length,
    not_worked_event_count: decisions.filter((row) => row.state === 'NOT_WORKED').length,
  });
}

