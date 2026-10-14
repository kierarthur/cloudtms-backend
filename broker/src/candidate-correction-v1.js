import { validateCandidateCorrectionV1, validateManagerCorrectionV1 } from './candidate-correction-v1.validator.js';

export const CORRECTION_CAPABILITY_HEADER = 'x-cloudtms-correction-contract';
export const CORRECTION_CAPABILITY = 'HEALTHROSTER_CORRECTION_V1';
const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
const object = value => Boolean(value && typeof value === 'object' && !Array.isArray(value));
const clock = /^([01]\d|2[0-3]):[0-5]\d$/;
const london = new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });
const invalid = () => { const error = new Error('CANDIDATE_CONTEXT_STALE'); error.code = error.message; error.status = 409; throw error; };

export function correctionCapability(request, required = false) {
  const value = request.headers.get(CORRECTION_CAPABILITY_HEADER);
  if ((value !== null && value !== CORRECTION_CAPABILITY) || (required && value === null)) {
    const error = new Error('CANDIDATE_CORRECTION_CLIENT_REQUIRED');
    error.code = error.message; error.status = 409; throw error;
  }
  return value === CORRECTION_CAPABILITY;
}

function same(values) {
  if (!values.length || new Set(values).size !== 1) invalid();
  return values[0];
}
function clockValue(value) {
  if (typeof value !== 'string') invalid();
  if (clock.test(value)) return value;
  if (!/^\d{4}-\d{2}-\d{2}T/.test(value) || !/(?:Z|[+-]\d{2}:\d{2})$/.test(value)
      || !Number.isFinite(Date.parse(value))) invalid();
  return london.format(new Date(value));
}
function aliases(source, keys, convert = value => value) {
  return keys.filter(key => own(source, key) && source[key] != null && source[key] !== '').map(key => convert(source[key]));
}
function canonicalBreak(source, converters) {
  const flat = {};
  const windows = source.breaks;
  if (windows != null && (!Array.isArray(windows) || windows.length > 1)) invalid();
  const starts = aliases(source, ['break_start', 'breakStart', 'break_start_time', 'break_start_iso'], clockValue);
  const ends = aliases(source, ['break_end', 'breakEnd', 'break_end_time', 'break_end_iso'], clockValue);
  if (windows?.length) {
    if (!object(windows[0])) invalid();
    starts.push(clockValue(windows[0].start)); ends.push(clockValue(windows[0].end));
  }
  const minutes = aliases(source, ['break_minutes', 'break_mins', 'unpaid_break_minutes'], value => {
    if ((typeof value !== 'number' && typeof value !== 'string') || !Number.isSafeInteger(Number(value))
        || Number(value) < 0 || Number(value) > 1440) invalid();
    return Number(value);
  });
  if (starts.length !== 0 || ends.length !== 0) {
    if (!starts.length || !ends.length) invalid();
    flat.break_start = same(starts); flat.break_end = same(ends);
    const duration = converters.segmentBreak(flat).break_minutes;
    if (duration < 1 || (minutes.length && same(minutes) !== duration)) invalid();
    flat.break_minutes = duration;
  } else if (minutes.length) flat.break_minutes = same(minutes);
  const noBreak = source.no_break === true || source.noBreak === true;
  if (noBreak) {
    if (starts.length || (minutes.length && same(minutes) !== 0)) invalid();
    flat.break_minutes = 0;
  }
  let info;
  if (own(source, 'break_entry')) {
    if (!object(source.break_entry)) invalid();
    const mode = source.break_entry.kind;
    converters.normaliseAdaptiveBreakEntry(source.break_entry, mode);
    info = converters.segmentBreak({ break_entry: source.break_entry });
    if (own(flat, 'break_minutes')) {
      const other = converters.segmentBreak(flat);
      if (info.break_minutes !== other.break_minutes || info.break_display_mode !== other.break_display_mode
          || info.break_start_local !== other.break_start_local || info.break_end_local !== other.break_end_local) invalid();
    }
  } else {
    if (!own(flat, 'break_minutes')) invalid();
    info = converters.segmentBreak(flat);
  }
  if (info.break_display_mode === 'NONE') return { kind: 'NO_BREAK', no_break: true, break_minutes: 0 };
  if (info.break_display_mode === 'MINUTES_ONLY') return { kind: 'DURATION_MINUTES', break_minutes: info.break_minutes };
  return { kind: 'START_END_TIMES', break_start: info.break_start_local, break_end: info.break_end_local,
    calculated_break_minutes: info.break_minutes };
}

/** Purpose serializer of immutable correction facts. No financial/configuration
 * fields, Rota lookup, positional part identities or inferred missing breaks. */
export function correctionSchedule(source, converters) {
  if (!Array.isArray(source)) invalid();
  const result = source.map(part => {
    if (!object(part)) invalid();
    const date = same(aliases(part, ['date', 'work_date', 'worked_date']));
    if (typeof date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(date)) invalid();
    const start = same(aliases(part, ['start', 'start_time', 'worked_start', 'actual_start', 'worked_start_iso', 'start_utc', 'start_iso'], clockValue));
    const end = same(aliases(part, ['end', 'end_time', 'finish', 'finish_time', 'worked_end', 'actual_end', 'worked_end_iso', 'end_utc', 'end_iso'], clockValue));
    if (start === end) invalid();
    const output = { date, start_time: start, end_time: end, break_entry: canonicalBreak(part, converters) };
    const minute = value => Number(value.slice(0, 2)) * 60 + Number(value.slice(3));
    const workedMinutes = (minute(end) - minute(start) + 1440) % 1440;
    const rest = output.break_entry;
    const breakMinutes = rest.kind === 'START_END_TIMES' ? rest.calculated_break_minutes : rest.break_minutes;
    if (breakMinutes >= workedMinutes || (rest.kind === 'START_END_TIMES'
        && (minute(rest.break_start) - minute(start) + 1440) % 1440 + breakMinutes > workedMinutes)) invalid();
    const references = aliases(part, ['ref_num', 'reference', 'booking_reference']);
    if (references.length) same(references);
    for (const key of ['row_key', 'segment_id', 'work_date', 'start', 'end', 'worked_start_iso', 'worked_end_iso',
      'start_utc', 'end_utc', 'ref_num', 'reference', 'booking_reference', 'band']) {
      if (own(part, key)) output[key] = part[key];
    }
    const instants = ['worked_start_iso', 'start_utc', 'start_iso', 'worked_end_iso', 'end_utc', 'end_iso'];
    const begin = aliases(part, instants.slice(0, 3), Date.parse);
    const finish = aliases(part, instants.slice(3), Date.parse);
    if (begin.length) same(begin); if (finish.length) same(finish);
    if (begin.length && finish.length && (finish[0] <= begin[0] || finish[0] - begin[0] > 86400000)) invalid();
    return output;
  });
  const ids = new Set();
  for (const part of result) {
    const id = part.segment_id ? `segment:${part.segment_id}` : part.row_key ? `row:${part.row_key}` : null;
    if (id && ids.has(id)) invalid(); if (id) ids.add(id);
    if (!id && result.filter(other => other.date === part.date).length > 1) invalid();
  }
  return result;
}

export function serializeCandidateCorrection(raw, timesheetId, converters) {
  if (!object(raw)) invalid();
  const output = {};
  for (const key of ['schema_version', 'request_id', 'correction_id', 'origin', 'state', 'state_revision',
    'proposal_revision', 'target_revision', 'root_timesheet_id', 'reversed_timesheet_id', 'original_booking_id',
    'replacement_timesheet_id', 'replacement_booking_id', 'reason']) output[key] = raw[key];
  output.original_schedule = correctionSchedule(raw.original_schedule, converters);
  output.proposed_schedule = correctionSchedule(raw.proposed_schedule, converters);
  const comparable = part => JSON.stringify({
    identity: part.segment_id ? `segment:${part.segment_id}` : part.row_key ? `row:${part.row_key}` : `single:${part.date}`,
    start: part.start_time, end: part.end_time, break_entry: part.break_entry
  });
  const dates = [...new Set([...output.original_schedule, ...output.proposed_schedule].map(part => part.date))].sort();
  output.changed_dates = dates.filter(date => JSON.stringify(output.original_schedule.filter(part => part.date === date).map(comparable).sort())
    !== JSON.stringify(output.proposed_schedule.filter(part => part.date === date).map(comparable).sort()));
  output.draft_scope = structuredClone(raw.draft_scope);
  if (!validateCandidateCorrectionV1(output) || ![raw.root_timesheet_id, raw.reversed_timesheet_id, raw.replacement_timesheet_id].includes(timesheetId)
      || output.replacement_timesheet_id === output.root_timesheet_id || output.replacement_timesheet_id === output.reversed_timesheet_id
      || output.original_booking_id === output.replacement_booking_id) invalid();
  const scope = output.draft_scope;
  if (scope.request_id !== output.request_id || scope.replacement_timesheet_id !== output.replacement_timesheet_id
      || scope.allowed_timesheet_aliases.some(id => id !== output.replacement_timesheet_id)
      || scope.allowed_booking_aliases.some(id => id !== output.replacement_booking_id)
      || (output.state === 'WAITING' && (scope.allowed_timesheet_aliases.length !== 1 || scope.allowed_booking_aliases.length !== 1))
      || (output.state !== 'WAITING' && (scope.allowed_timesheet_aliases.length || scope.allowed_booking_aliases.length))) invalid();
  return output;
}

export function serializeManagerCorrection(raw, result, converters) {
  const output = { schema_version: raw?.schema_version, request_id: raw?.request_id,
    correction_id: raw?.correction_id, reason: raw?.reason,
    original_schedule: correctionSchedule(raw?.original_schedule, converters),
    reviewed_schedule: correctionSchedule(raw?.reviewed_schedule, converters),
    review_witness: structuredClone(raw?.review_witness) };
  if (!validateManagerCorrectionV1(output)) invalid();
  const witness = output.review_witness;
  const component = result.ordered_components.find(part => part.component_id === witness.hours_component_id);
  if (witness.workflow_id !== result.workflow_id || witness.workflow_generation !== result.workflow_generation
      || witness.approval_request_id !== result.approval_request_id || witness.manifest_sha256 !== result.manifest_sha256
      || !component || component.component_kind !== 'HOURS_TIMESHEET'
      || component.content_sha256 !== witness.review_content_sha256) invalid();
  return output;
}

export function correctionPresentation(raw, reviewed, converters) {
  const original = correctionSchedule(raw.original_schedule, converters);
  const after = correctionSchedule(reviewed, converters);
  const approvedAt = raw.original_manager_approved_at_utc;
  if (raw.origin === 'SIGNED_TIMESHEET_CORRECTION' && (!approvedAt || !Number.isFinite(Date.parse(approvedAt)))) invalid();
  const approvalDate = approvedAt ? new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London', day: 'numeric', month: 'long', year: 'numeric'
  }).format(new Date(approvedAt)) : null;
  const line = (part, index) => {
    const entry = part.break_entry;
    const rest = entry.kind === 'NO_BREAK' ? 'No break' : entry.kind === 'DURATION_MINUTES'
      ? `${entry.break_minutes} minute break` : `Break ${entry.break_start}-${entry.break_end}`;
    return `${part.date} | Part ${index + 1}${part.segment_id || part.row_key ? ` (${part.segment_id || part.row_key})` : ''}: ${part.start_time}-${part.end_time} | ${rest}`;
  };
  return { schema_version: 1, request_id: raw.request_id, correction_id: raw.correction_id,
    reason: raw.reason, original_schedule: original, reviewed_schedule: after,
    header_lines: ['Corrected Timesheet', `Reason from the office: ${raw.reason}`,
      ...(approvalDate ? [`Original manager approval: ${approvalDate}`] : []),
      raw.origin === 'SIGNED_TIMESHEET_CORRECTION' ? 'Previously approved hours:' : 'Original recorded hours:',
      ...original.map(line), 'Submitted corrected hours:', ...after.map(line)] };
}
