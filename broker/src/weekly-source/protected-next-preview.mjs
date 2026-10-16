// Supplemental evidence for a server-only, non-writing protected preview.
// Imported Source money is never taken from this calculator result.
const BUCKETS = Object.freeze(['day', 'night', 'sat', 'sun', 'bh']);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;
const CLOCK = /^(?:[01]\d|2[0-3]):[0-5]\d$/;
const HASH = /^[0-9a-f]{64}$/;
const encoder = new TextEncoder();

function fail(code) { const error = new Error(code); error.code = code; throw error; }
function object(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail('WEEKLY_PROTECTED_NEXT_FACTS_INVALID');
  return value;
}
function uuid(value) {
  if (typeof value !== 'string' || !UUID.test(value)) fail('WEEKLY_PROTECTED_NEXT_EVENT_INVALID');
  return value;
}
function decimal(value, scale, { nullable = false, number = false } = {}) {
  if (value == null && nullable) return null;
  if (typeof value !== 'string' && !(number && typeof value === 'number' && Number.isFinite(value))) {
    fail('WEEKLY_PROTECTED_NEXT_DECIMAL_INVALID');
  }
  const token = String(value);
  const match = /^(-?)(0|[1-9]\d*)(?:\.(\d+))?$/.exec(token);
  if (!match || (match[3]?.length > scale && /[1-9]/.test(match[3].slice(scale)))) {
    fail('WEEKLY_PROTECTED_NEXT_DECIMAL_INVALID');
  }
  const fraction = (match[3] ?? '').slice(0, scale).padEnd(scale, '0');
  const integer = BigInt(match[2] + fraction);
  const negative = match[1] && integer !== 0n ? '-' : '';
  return `${negative}${match[2]}${scale ? `.${fraction}` : ''}`;
}
function pence(value) {
  if (typeof value !== 'string' || !/^-?(0|[1-9]\d*)$/.test(value)) fail('WEEKLY_PROTECTED_NEXT_PENCE_INVALID');
  const amount = BigInt(value); const absolute = amount < 0n ? -amount : amount;
  return `${amount < 0n ? '-' : ''}${absolute / 100n}.${String(absolute % 100n).padStart(2, '0')}`;
}
function minutes(value) {
  if (!Number.isSafeInteger(value) || value < 0) fail('WEEKLY_PROTECTED_NEXT_MINUTES_INVALID');
  return value;
}
function rates(raw, hours) {
  object(raw);
  return Object.fromEntries(BUCKETS.map((bucket) => {
    // The established split-rate owner normalises finite values to Number.
    // A missing/invalid occupied rate is not permitted to acquire guessed proof.
    if (raw[bucket] == null || raw[bucket] === '' || !Number.isFinite(Number(raw[bucket]))) {
      if (BigInt(hours[bucket].replace('.', '')) !== 0n) fail('WEEKLY_PROTECTED_NEXT_USED_RATE_MISSING');
      return [bucket, null];
    }
    return [bucket, decimal(Number(raw[bucket]), 6, { number: true })];
  }));
}

export function retainWeeklyProtectedPreviewFacts({ scheduleSegment, financialSegment, payRates, chargeRates }) {
  const schedule = object(scheduleSegment); const financial = object(financialSegment);
  const workEventId = uuid(schedule.work_event_id);
  if (!['WAIT', 'SOURCE', 'ACCEPTED_SOURCE'].includes(schedule.protected_target_state)) fail('WEEKLY_PROTECTED_NEXT_CALCULATOR_INVALID');
  if (schedule.protected_target_state !== 'WAIT') {
    // The calculator's temporary reprice is not evidence for imported work.
    // Only carry identity; buildWeeklyProtectedNextTarget uses genuine vectors.
    return Object.freeze({ work_event_id: workEventId, protected_target_state: schedule.protected_target_state });
  }
  if (financial.ok !== true || financial.calculatorVersion !== 'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1'
      || !['SPLIT_RATE_WINDOWS', 'WHOLE_SHIFT_START_DAY'].includes(financial.mode)
      || ![1, -1].includes(financial.sign)) fail('WEEKLY_PROTECTED_NEXT_CALCULATOR_INVALID');
  const hours = Object.fromEntries(BUCKETS.map((bucket) => [bucket, decimal(financial.hours?.[bucket], 6, { number: true })]));
  const bucketMinutes = Object.fromEntries(BUCKETS.map((bucket) => [bucket, minutes(financial.bucketMinutes?.[bucket])]));
  return Object.freeze({
    work_event_id: workEventId,
    protected_target_state: schedule.protected_target_state,
    protected_next_preview: Object.freeze({
      schema_version: 'WEEKLY_PROTECTED_NEXT_PREVIEW_V1',
      calculator_owner: 'buildWeeklyScheduleSegmentsSnapshot',
      calculator_version: financial.calculatorVersion,
      mode: financial.mode, category: financial.category,
      sign: financial.sign, bucket_minutes: Object.freeze(bucketMinutes),
      paid_minutes: minutes(financial.paidMinutes), break_minutes: minutes(financial.breakMinutes),
      hours: Object.freeze(hours), rates: Object.freeze(rates(payRates, hours)),
      charge_rates: Object.freeze(rates(chargeRates, hours)),
      pay_ex_vat: pence(financial.payPence), charge_ex_vat: pence(financial.chargePence),
    }),
  });
}

function unique(rows, event) {
  if (!Array.isArray(rows) || rows.length > 100) fail('WEEKLY_PROTECTED_NEXT_PAGE_BOUND');
  const result = new Map();
  for (const raw of rows) {
    const row = object(raw); const id = uuid(event(row));
    if (result.has(id)) fail('WEEKLY_PROTECTED_NEXT_EVENT_DUPLICATE');
    result.set(id, row);
  }
  return result;
}
function shift(row, schedule, financial, provenance, protectedWork) {
  if (!DATE.test(schedule.date) || !CLOCK.test(schedule.start) || !CLOCK.test(schedule.end)
      || row.date !== schedule.date || row.start !== schedule.start || row.end !== schedule.end
      || row.break_mins !== schedule.break_mins || typeof row.overnight !== 'boolean') {
    fail('WEEKLY_PROTECTED_NEXT_SHIFT_NOT_EXACT');
  }
  const hours = Object.fromEntries(BUCKETS.map((bucket) => [bucket, decimal(financial.hours?.[bucket], 6)]));
  const sourceRates = Object.fromEntries(BUCKETS.map((bucket) => [bucket, decimal(financial.rates?.[bucket], 6, { nullable: true })]));
  const chargeRates = Object.fromEntries(BUCKETS.map((bucket) => [bucket, decimal(financial.charge_rates?.[bucket], 6, { nullable: true })]));
  for (const bucket of BUCKETS) {
    // Imported zero/excluded Source vectors can legitimately retain occupied
    // hours with an unknown pay rate. Preserve that fact, never infer a rate.
    if (protectedWork && BigInt(hours[bucket].replace('.', '')) !== 0n && sourceRates[bucket] == null) fail('WEEKLY_PROTECTED_NEXT_USED_RATE_MISSING');
  }
  // A duration-only break deliberately keeps null endpoints. Exact local
  // windows are retained only when the accepted preview/source supplied them.
  const breaks = (Array.isArray(row.breaks) ? row.breaks : []).map((raw) => {
    const item = object(raw);
    if (!CLOCK.test(item.start) || !CLOCK.test(item.end)) fail('WEEKLY_PROTECTED_NEXT_BREAK_INVALID');
    const start = Number(item.start.slice(0, 2)) * 60 + Number(item.start.slice(3));
    const end = Number(item.end.slice(0, 2)) * 60 + Number(item.end.slice(3));
    const duration = end > start ? end - start : 1440 - start + end;
    if (!duration || duration >= 1440) fail('WEEKLY_PROTECTED_NEXT_BREAK_INVALID');
    return { start: item.start, end: item.end, minutes: duration };
  });
  if (breaks.length > 100 || (breaks.length && breaks.reduce((sum, item) => sum + item.minutes, 0) !== row.break_mins)) {
    fail('WEEKLY_PROTECTED_NEXT_BREAK_NOT_EXACT');
  }
  return {
    kind: 'WORK', work_event_id: uuid(schedule.work_event_id), protected: protectedWork,
    date: row.date, start: row.start, end: row.end, overnight: row.overnight,
    start_at_utc: row.start_utc ?? null, end_at_utc: row.end_utc ?? null,
    break_minutes: minutes(row.break_mins), breaks,
    hours, rates: sourceRates, charge_rates: chargeRates,
    pay_ex_vat: decimal(financial.pay_ex_vat, 2), charge_ex_vat: decimal(financial.charge_ex_vat, 2),
    source_provenance: provenance,
  };
}

export function buildWeeklyProtectedNextTarget({ context, target, calculation }) {
  object(context); object(target); object(calculation);
  const snapshot = object(calculation.snapshot ?? calculation);
  if (snapshot.timesheet_id !== context.root_timesheet_id) fail('WEEKLY_PROTECTED_NEXT_ROOT_MISMATCH');
  const schedules = unique(target.segments, (row) => row.work_event_id);
  const previews = unique(snapshot.invoice_breakdown_json?.segments, (row) => row.work_event_id);
  const sources = unique(context.next_source_segments, (row) => row.weekly_source?.work_event_id);
  if (previews.size !== schedules.size) fail('WEEKLY_PROTECTED_NEXT_MANIFEST_NOT_EXACT');
  const components = [];
  for (const [id, schedule] of schedules) {
    const preview = previews.get(id);
    if (!preview || preview.protected_target_state !== schedule.protected_target_state) fail('WEEKLY_PROTECTED_NEXT_MANIFEST_NOT_EXACT');
    if (schedule.protected_target_state === 'WAIT') {
      const financial = object(preview.protected_next_preview);
      if (financial.schema_version !== 'WEEKLY_PROTECTED_NEXT_PREVIEW_V1'
          || financial.calculator_owner !== 'buildWeeklyScheduleSegmentsSnapshot') fail('WEEKLY_PROTECTED_NEXT_CALCULATOR_INVALID');
      components.push(shift(preview, schedule, financial, null, true));
    } else {
      const source = sources.get(id); if (!source) fail('WEEKLY_PROTECTED_NEXT_SOURCE_PROOF_MISSING');
      const evidence = object(source.weekly_source);
      const provenance = {};
      for (const key of ['movement_id', 'final_revision_id', 'row_resolution_id', 'economic_snapshot_id']) provenance[key] = uuid(evidence[key]);
      if (typeof evidence.calculation_fingerprint !== 'string' || !HASH.test(evidence.calculation_fingerprint)) fail('WEEKLY_PROTECTED_NEXT_SOURCE_PROOF_MISSING');
      provenance.calculation_fingerprint = evidence.calculation_fingerprint;
      components.push(shift(source, schedule, {
        hours: evidence.pay_vector?.hours, rates: evidence.pay_vector?.rates,
        charge_rates: evidence.charge_vector?.rates,
        pay_ex_vat: pence(evidence.pay_vector?.total_pence),
        charge_ex_vat: pence(evidence.charge_vector?.total_pence),
      }, provenance, false));
    }
  }
  const expenses = unique(context.source_expenses, (row) => row.work_event_id);
  for (const [id, expense] of expenses) {
    if (expense.authority_kind !== 'SOURCE_EXPENSE') fail('WEEKLY_PROTECTED_NEXT_EXPENSE_PROOF_MISSING');
    components.push({ kind: 'EXPENSE', work_event_id: id, source_expense_id: uuid(expense.source_expense_id),
      final_revision_id: uuid(expense.final_revision_id), source_observation_kind: expense.source_observation_kind,
      authority_sha256: expense.document_sha256,
      pay_ex_vat: decimal(expense.pay_ex_vat, 2), charge_ex_vat: decimal(expense.charge_ex_vat, 2) });
  }
  if (components.length > 100) fail('WEEKLY_PROTECTED_NEXT_PAGE_BOUND');
  const result = { schema_version: 'WEEKLY_PROTECTED_NEXT_TARGET_V1', components };
  if (encoder.encode(JSON.stringify(result)).byteLength > 120000) fail('WEEKLY_PROTECTED_NEXT_BYTE_BOUND');
  return Object.freeze(result);
}
