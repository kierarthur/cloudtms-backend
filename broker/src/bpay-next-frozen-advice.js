/**
 * Minimal printable ORIGINAL CSV advice, not the completed Stage-7 remittance
 * product. No delivery/configuration, issue persistence, outcome or reissue.
 *
 * Internal caller: renderBpayNextFrozenAdvice(originalOfficeRequest,
 *   {instruction_id, page_limit?}, {requireOfficeUser, advicePageRpc}).
 * Office identity comes from requireUser, never browser input. advicePageRpc is
 * the fixed raw 0255 service transport; never sbRpc (which buffers before bounds).
 * The HEADER is pinned i -> t.projection_id -> immutable p, not worker.latest.
 * WORKS and MEMBERS pin rw.captured_revision_id and approved_line_id; immutable
 * labels/rates are not read from today's Candidate, Timesheet or settings.
 * Decimal TEXT is printed unchanged. Only row COUNTS use BigInt, never money.
 * One page at each nested level is retained, and HTML chunks obey backpressure.
 */
import { handleBpayNextFrozenRead } from './bpay-next-frozen-read.js';

export const BPAY_NEXT_FROZEN_ADVICE_LIMITS = Object.freeze({
  pageRows: 100, pageBytes: 128 * 1024, timeoutMs: 8000, chunkBytes: 16 * 1024
});

const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const ENCODER = new TextEncoder();
const TEXT_LIMIT = 8192;
const SCHEMAS = Object.freeze({
  HEADER: {
    instruction_id: 'uuid', transfer_id: 'uuid', projection_id: 'uuid', projection_no: 'positive',
    input_kind: 'input', run_id: 'uuid', run_worker_id: 'uuid', candidate_id: 'uuid',
    pay_date: 'date', target_pay_channel: 'paye', beneficiary_name_snapshot: 'name',
    payment_reference: 'name', issued_at_utc: 'timestamp', member_count: 'positive',
    gross_ex_vat: 'money', gross_vat: 'money', gross_inc_vat: 'money', entered_paye_net: 'money?',
    accepted_gross_additions: 'money', accepted_gross_deductions: 'money', accepted_net_additions: 'money',
    accepted_recoveries: 'money', cash_amount: 'money', projection_cash_amount: 'money', destination_kind: 'destination'
  },
  WORK: {
    run_work_id: 'uuid', work_id: 'uuid', captured_revision_id: 'uuid', physical_timesheet_id: 'uuid',
    physical_timesheet_version: 'positiveInt', source_kind: 'source', detail_kind: 'detail',
    week_ending_date: 'date', candidate_display_name: 'name', candidate_reference: 'text?',
    client_display_name: 'name', job_title: 'text?', band_label: 'text?', timesheet_reference: 'text?',
    expected_rate_schedule_count: 'int'
  },
  LINE: {
    run_line_id: 'uuid', run_work_id: 'uuid', captured_revision_id: 'uuid', line_no: 'positive',
    component_key: 'name', component_kind: 'component', work_date: 'date?', unit_label: 'text?',
    approved_quantity: 'decimal?', approved_unit_rate: 'decimal?', expected_rate_detail_count: 'bucketCount',
    evidence_ref: 'text?', approved_source_ex_vat: 'money', source_consumed_ex_vat: 'money',
    frozen_ex_vat: 'money', frozen_vat: 'money', frozen_inc_vat: 'money'
  },
  CASE: {
    case_instruction_id: 'uuid', allocation_result_id: 'uuid', case_id: 'uuid', case_kind: 'caseKind',
    case_subtype: 'name', tax_treatment: 'name', instruction_kind: 'caseInstruction', direction: 'direction',
    payroll_stage: 'stage', resolution_ref: 'name', explanation: 'text?',
    allocated_target_ex_vat: 'money', allocated_target_vat: 'money', allocated_target_inc_vat: 'money'
  }
});

class AdviceError extends Error {
  constructor(code, status = 503) { super(code); this.name = 'AdviceError'; this.code = code; this.status = status; }
}
const invalid = () => { throw new AdviceError('BPAY_NEXT_ADVICE_RESPONSE_INVALID'); };
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const sameId = (a, b) => a.toLowerCase() === b.toLowerCase();
const zero = value => /^-?0(?:\.0+)?$/.test(value);
const nonnegative = value => !value.startsWith('-') || zero(value);
function exact(value, keys) {
  return object(value) && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
}
function field(value, type) {
  if (type.endsWith('?')) return value === null || field(value, type.slice(0, -1));
  if (['text', 'name', 'money', 'decimal', 'positive'].includes(type)) {
    if (typeof value !== 'string' || ENCODER.encode(value).byteLength > TEXT_LIMIT) return false;
  }
  switch (type) {
    case 'uuid': return typeof value === 'string' && UUID.test(value);
    case 'text': return true;
    case 'name': return value.trim().length > 0;
    case 'money': return /^-?(?:0|[1-9]\d*)(?:\.\d{1,2})?$/.test(value);
    case 'decimal': return /^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(value);
    case 'positive': return /^[1-9]\d{0,18}$/.test(value) && BigInt(value) <= 9223372036854775807n;
    case 'int': return Number.isInteger(value) && value >= 0 && value <= 2147483647;
    case 'positiveInt': return field(value, 'int') && value > 0;
    case 'bucketCount': return field(value, 'int') && value <= 5;
    case 'date': {
      if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
      const date = new Date(`${value}T00:00:00Z`);
      return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value;
    }
    case 'timestamp': return typeof value === 'string'
      && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)
      && field(value.slice(0, 10), 'date') && Number.isFinite(Date.parse(value));
    case 'paye': return value === 'PAYE';
    case 'destination': return ['OWN', 'ONEOFF'].includes(value);
    case 'input': return ['PAYE_MANUAL', 'PAYE_IMPORT', 'CASE_PAYOUT'].includes(value);
    case 'source': return ['ORDINARY', 'SOURCE', 'PROTECTED', 'EXPENSE', 'ADJUSTMENT'].includes(value);
    case 'detail': return ['SHIFT', 'AGGREGATE', 'FIXED'].includes(value);
    case 'component': return ['WORK', 'PROTECTED_WORK', 'EXPENSE', 'MILEAGE', 'ADDITIONAL', 'ADJUSTMENT'].includes(value);
    case 'caseKind': return ['LOAN', 'ADVANCE', 'OVERPAYMENT', 'MANUAL_DEBT', 'CREDIT'].includes(value);
    case 'caseInstruction': return ['PAYOUT', 'RECOVERY', 'CREDIT'].includes(value);
    case 'direction': return ['PAYMENT', 'DEDUCTION'].includes(value);
    case 'stage': return ['GROSS_ADD', 'GROSS_DEDUCT', 'NET_ADD', 'NET_DEDUCT'].includes(value);
    default: return false;
  }
}
function validate(value, schema) {
  if (!exact(value, Object.keys(schema)) || !Object.entries(schema).every(([key, type]) => field(value[key], type))) invalid();
  return value;
}
// Compare textual representations only; no arithmetic/revaluation of money.
function decimalEqual(a, b) {
  const canonical = value => value.replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, '').replace(/^-0$/, '0');
  return canonical(a) === canonical(b);
}
function member(row) {
  if (!exact(row, ['member_no', 'subject_kind', 'signed_cash_contribution', 'work', 'line', 'case'])
    || !field(row.member_no, 'positive') || !field(row.signed_cash_contribution, 'money')) invalid();
  if (row.subject_kind === 'WORK') {
    validate(row.work, SCHEMAS.WORK); validate(row.line, SCHEMAS.LINE);
    if (row.case !== null || !sameId(row.line.run_work_id, row.work.run_work_id)
      || !sameId(row.line.captured_revision_id, row.work.captured_revision_id)
      || !['source_consumed_ex_vat', 'frozen_ex_vat', 'frozen_vat', 'frozen_inc_vat'].every(key => nonnegative(row.line[key]))
      || !decimalEqual(row.signed_cash_contribution, row.line.frozen_inc_vat)) invalid();
  } else if (row.subject_kind === 'CASE') {
    if (row.work !== null || row.line !== null) invalid();
    validate(row.case, SCHEMAS.CASE);
    if (!['allocated_target_ex_vat', 'allocated_target_vat', 'allocated_target_inc_vat'].every(key => nonnegative(row.case[key]))) invalid();
    const signed = row.case.direction === 'DEDUCTION' ? `-${row.case.allocated_target_inc_vat}` : row.case.allocated_target_inc_vat;
    if (!decimalEqual(signed, row.signed_cash_contribution)) invalid();
  } else if (row.subject_kind !== 'NET_ADJUSTMENT' || row.work !== null || row.line !== null || row.case !== null) invalid();
  return row;
}
const escape = value => String(value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
const display = value => value === null ? 'Not recorded' : escape(value);
const money = value => value === null ? 'Not payroll earnings' : `£${escape(value)}`;
const pair = (label, value) => `<dt>${label}</dt><dd>${display(value)}</dd>`;

async function readJson(response, signal) {
  if (!(response instanceof Response) || !response.body) invalid();
  if (!response.ok) {
    await response.body.cancel().catch(() => {});
    throw new AdviceError('BPAY_NEXT_ADVICE_PAGE_UNAVAILABLE', response.status === 403 ? 403 : 503);
  }
  const declared = response.headers.get('content-length');
  if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) > BPAY_NEXT_FROZEN_ADVICE_LIMITS.pageBytes)) {
    await response.body.cancel().catch(() => {}); throw new AdviceError('BPAY_NEXT_ADVICE_PAGE_TOO_LARGE');
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let bytes = 0; const parts = [];
  const cancel = () => { void reader.cancel().catch(() => {}); };
  signal.addEventListener('abort', cancel, { once: true });
  try {
    while (true) {
      if (signal.aborted) throw new AdviceError('BPAY_NEXT_ADVICE_CANCELLED');
      const { value, done } = await reader.read();
      if (signal.aborted) throw new AdviceError('BPAY_NEXT_ADVICE_CANCELLED');
      if (done) break;
      bytes += value.byteLength;
      if (bytes > BPAY_NEXT_FROZEN_ADVICE_LIMITS.pageBytes) throw new AdviceError('BPAY_NEXT_ADVICE_PAGE_TOO_LARGE');
      parts.push(decoder.decode(value, { stream: true }));
    }
    parts.push(decoder.decode());
    try { return JSON.parse(parts.join('')); } catch { invalid(); }
  } catch (error) {
    await reader.cancel().catch(() => {});
    throw error instanceof AdviceError ? error : new AdviceError('BPAY_NEXT_ADVICE_RESPONSE_INVALID');
  } finally { signal.removeEventListener('abort', cancel); reader.releaseLock(); }
}
async function page(kind, args, context) {
  const controller = new AbortController();
  let rejectAborted;
  const aborted = new Promise((_, reject) => { rejectAborted = reject; });
  const abort = () => { controller.abort(); rejectAborted(new AdviceError('BPAY_NEXT_ADVICE_CANCELLED')); };
  context.signal.addEventListener('abort', abort, { once: true });
  if (context.signal.aborted) abort();
  let timer;
  try {
    const task = (async () => {
      const response = await context.rpc(kind, { p_instruction_id: context.instructionId, p_limit: context.limit, ...args }, {
        actorUserId: context.actor.id, signal: controller.signal,
        timeoutMs: BPAY_NEXT_FROZEN_ADVICE_LIMITS.timeoutMs, maxResponseBytes: BPAY_NEXT_FROZEN_ADVICE_LIMITS.pageBytes
      });
      // If transport ignores AbortSignal and returns late, close its raw body.
      if (controller.signal.aborted) { await response?.body?.cancel().catch(() => {}); throw new AdviceError('BPAY_NEXT_ADVICE_PAGE_UNAVAILABLE'); }
      const body = await readJson(response, controller.signal);
      if (!exact(body, ['rows', 'complete']) || !Array.isArray(body.rows)
        || body.rows.length > context.limit || typeof body.complete !== 'boolean'
        || (!body.complete && body.rows.length === 0)) invalid();
      return body;
    })();
    const deadline = new Promise((_, reject) => {
      timer = setTimeout(() => { controller.abort(); reject(new AdviceError('BPAY_NEXT_ADVICE_PAGE_TIMEOUT')); }, BPAY_NEXT_FROZEN_ADVICE_LIMITS.timeoutMs);
    });
    return await Promise.race([task, deadline, aborted]);
  } finally { clearTimeout(timer); context.signal.removeEventListener('abort', abort); }
}
// Reuse the SAVED frozen-detail validator, including all endpoint pairs, signed
// decimal/bucket semantics and exact next-cursors. Only transport gains an
// instruction scope through new0255; no second divergent detail schema here.
async function detail(body, context) {
  const response = await handleBpayNextFrozenRead(new Request('https://bpay-next.invalid/internal-frozen-detail', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ...body, limit: context.limit }), signal: context.signal
  }), {
    requireOfficeUser: async () => context.actor,
    frozenPageRpc: (kind, args, options) => context.rpc(kind, { ...args, p_instruction_id: context.instructionId }, options)
  });
  const parsed = await readJson(response, context.signal);
  if (!exact(parsed, ['ok', 'kind', 'rows', 'complete', 'next_cursor']) || parsed.ok !== true || parsed.kind !== body.kind) invalid();
  return parsed;
}
async function* detailRows(body, context) {
  let cursor = {};
  while (true) {
    const result = await detail({ ...body, ...cursor }, context);
    for (const row of result.rows) yield row;
    if (result.complete) return;
    if (result.next_cursor === null || JSON.stringify(cursor) === JSON.stringify(result.next_cursor)) invalid();
    cursor = result.next_cursor;
  }
}
function workHeading(work) {
  return `<h3>${escape(work.candidate_display_name)} — ${escape(work.client_display_name)}</h3><dl>`
    + pair('Works reference', work.candidate_reference) + pair('Job', work.job_title) + pair('Band', work.band_label)
    + pair('Timesheet reference', work.timesheet_reference) + pair('Week ending', work.week_ending_date)
    + pair('Captured physical Timesheet', work.physical_timesheet_id) + pair('Physical version', work.physical_timesheet_version)
    + pair('Work identity', work.work_id) + pair('Captured approval revision', work.captured_revision_id) + '</dl>';
}
function tableStart(labels) { return `<table><thead><tr>${labels.map(label => `<th>${label}</th>`).join('')}</tr></thead><tbody>`; }
function tableRow(values) { return `<tr>${values.map(value => `<td>${display(value)}</td>`).join('')}</tr>`; }
async function* workRows(context) {
  let afterWork = null;
  while (true) {
    const result = await page('WORKS', { p_after_work_id: afterWork }, context);
    for (const work of result.rows) {
      validate(work, SCHEMAS.WORK);
      if (afterWork !== null && work.work_id.toLowerCase() <= afterWork.toLowerCase()) invalid();
      afterWork = work.work_id;
      yield work;
    }
    if (result.complete) return;
  }
}
async function* availableRates(work, context) {
  yield '<h4>All available frozen rate options</h4>' + tableStart(['Rate family', 'Code', 'Unit', 'PAYE rate', 'Umbrella rate', 'Charge rate']);
  let rates = 0;
  for await (const rate of detailRows({ kind: 'RATE_SCHEDULE', run_work_id: work.run_work_id }, context)) {
    rates += 1;
    if (rates > work.expected_rate_schedule_count) invalid();
    yield tableRow([rate.rate_family, rate.rate_code, rate.unit_label, rate.paye_rate, rate.umbrella_rate, rate.charge_rate]);
  }
  if (rates !== work.expected_rate_schedule_count) throw new AdviceError('BPAY_NEXT_ADVICE_FROZEN_DETAIL_MISSING');
  yield '</tbody></table>';
  if (rates === 0) yield '<p>No available rate options recorded in this captured approval.</p>';
}
async function* html(header, context) {
  yield '<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Frozen CSV payment advice</title>'
    + '<style>body{font:14px sans-serif;margin:2em}table{border-collapse:collapse;width:100%}th,td{border:1px solid #888;padding:.3em;text-align:left;overflow-wrap:anywhere}dt{font-weight:bold}dd{margin-bottom:.4em;overflow-wrap:anywhere}section{margin-top:2em}@media print{thead{display:table-header-group}tr{break-inside:avoid}}.integrity{color:#900}</style></head><body>'
    + '<p data-advice-state="incomplete">INCOMPLETE until the final completion marker. Original CSV instruction advice; issuance is not settlement.</p>';
  yield `<h1>Frozen CSV payment advice</h1><h2>${escape(header.beneficiary_name_snapshot)}</h2><dl>`
    + pair('Instruction', header.instruction_id) + pair('Original transfer', header.transfer_id)
    + pair('Run / worker', `${header.run_id} / ${header.run_worker_id}`) + pair('Pay date', header.pay_date)
    + pair('Payment reference', header.payment_reference) + pair('Issued at (recorded timezone)', header.issued_at_utc)
    + pair('Accepted original projection', `${header.projection_id} / ${header.projection_no}`) + '</dl>';
  yield '<h2>Accepted original Candidate projection (GBP)</h2><p>These frozen payroll and case totals describe the whole accepted projection, not another payment by this instruction.</p><dl>'
    + `<dt>Selected payroll gross ex VAT</dt><dd>${money(header.gross_ex_vat)}</dd>`
    + `<dt>Selected VAT</dt><dd>${money(header.gross_vat)}</dd><dt>Selected payroll gross including VAT</dt><dd>${money(header.gross_inc_vat)}</dd>`
    + `<dt>Original accepted payroll net</dt><dd>${money(header.entered_paye_net)}</dd>`
    + `<dt>Gross additions</dt><dd>${money(header.accepted_gross_additions)}</dd><dt>Gross deductions</dt><dd>${money(header.accepted_gross_deductions)}</dd>`
    + `<dt>Net additions (including Payment Advance/credit cash)</dt><dd>${money(header.accepted_net_additions)}</dd>`
    + `<dt>Accepted net recoveries</dt><dd>${money(header.accepted_recoveries)}</dd>`
    + `<dt>Accepted total bank cash (all destinations)</dt><dd>${money(header.projection_cash_amount)}</dd></dl>`
    + '<h2>Original bank instruction (this destination)</h2><dl>'
    + pair('Frozen destination kind', header.destination_kind === 'OWN' ? 'Candidate own account' : 'Stored one-off account')
    + `<dt>Original bank cash instruction</dt><dd>${money(header.cash_amount)}</dd></dl>`;
  yield '<section><h2>Original instruction constituents</h2><p>Selected money below is the frozen paid difference. Approved quantities, segment money and rates describe the captured approval; they are not new hours paid and are not multiplied to reconstruct payment.</p>';
  // Actual0130/0410 append WORK members by line_no;1620 numbers frozen lines
  // by work_id/component. Merge that retained order with the WORKS keyset,
  // holding only the current page and work. A missing work/rate page cannot
  // silently disappear, and no candidate-wide set of already-seen IDs exists.
  const works = workRows(context)[Symbol.asyncIterator](); let activeWork = null;
  let afterMember = null; let count = 0n; let adjustments = 0;
  while (true) {
    const result = await page('MEMBERS', { p_after_member_no: afterMember }, context);
    for (const raw of result.rows) {
      const row = member(raw);
      if (header.destination_kind === 'ONEOFF' && (row.subject_kind !== 'CASE'
        || row.case.case_kind !== 'CREDIT' || row.case.tax_treatment !== 'NON_TAXABLE'
        || row.case.instruction_kind !== 'CREDIT' || row.case.direction !== 'PAYMENT' || row.case.payroll_stage !== 'NET_ADD')) invalid();
      if (afterMember !== null && BigInt(row.member_no) <= BigInt(afterMember)) invalid();
      afterMember = row.member_no; count += 1n;
      if (count > BigInt(header.member_count)) invalid();
      if (row.subject_kind === 'NET_ADJUSTMENT') {
        adjustments += 1;
        yield `<h3>Recorded payroll net adjustment</h3><p>Signed instruction constituent: ${money(row.signed_cash_contribution)}. This is not another work payment or a recomputed deduction.</p>`;
      } else if (row.subject_kind === 'CASE') {
        const item = row.case;
        yield `<h3>${escape(item.case_kind === 'LOAN' ? 'Payment Advance' : item.case_kind)} / ${escape(item.instruction_kind)} / ${escape(item.payroll_stage)}</h3><dl>`
          + pair('Case', item.case_id) + pair('Frozen case instruction', item.case_instruction_id)
          + pair('Accepted allocation result', item.allocation_result_id) + pair('Subtype', item.case_subtype === 'LOAN' ? 'Payment Advance' : item.case_subtype)
          + pair('Tax treatment', item.tax_treatment) + pair('Direction', item.direction)
          + pair('Resolution reference', item.resolution_ref) + pair('Explanation', item.explanation)
          + `<dt>Allocated ex VAT / VAT / including VAT</dt><dd>${money(item.allocated_target_ex_vat)} / ${money(item.allocated_target_vat)} / ${money(item.allocated_target_inc_vat)}</dd>`
          + `<dt>Signed cash constituent</dt><dd>${money(row.signed_cash_contribution)}</dd></dl>`;
      } else {
        const line = row.line; const work = row.work;
        if (activeWork === null || !sameId(activeWork.work_id, work.work_id)) {
          const next = await works.next();
          if (next.done || !sameId(next.value.work_id, work.work_id)) throw new AdviceError('BPAY_NEXT_ADVICE_FROZEN_DETAIL_MISSING');
          activeWork = next.value;
          if (!Object.keys(SCHEMAS.WORK).every(key => activeWork[key] === work[key])) invalid();
          yield workHeading(work);
          yield* availableRates(work, context);
        } else if (!Object.keys(SCHEMAS.WORK).every(key => activeWork[key] === work[key])) invalid();
        yield workHeading(work) + `<h4>${escape(line.component_kind)} — ${escape(line.component_key)}</h4><dl>`
          + pair('Run line', line.run_line_id) + pair('Work date', line.work_date)
          + pair('Approved quantity (not paid hours)', line.approved_quantity) + pair('Approved unit', line.unit_label)
          + pair('Approved unit rate', line.approved_unit_rate) + pair('Expense / component evidence', line.evidence_ref)
          + `<dt>Full captured approved component source amount</dt><dd>${money(line.approved_source_ex_vat)}</dd>`
          + `<dt>Selected source difference</dt><dd>${money(line.source_consumed_ex_vat)}</dd>`
          + `<dt>Selected payment ex VAT / VAT / including VAT</dt><dd>${money(line.frozen_ex_vat)} / ${money(line.frozen_vat)} / ${money(line.frozen_inc_vat)}</dd></dl>`;
        if (zero(line.frozen_inc_vat)) yield '<p>Explicit frozen zero payment line; its approved detail is retained.</p>';
        let shifts = 0;
        for await (const shift of detailRows({ kind: 'SHIFTS', run_line_id: line.run_line_id }, context)) {
          shifts += 1;
          yield '<h4>Captured shift / submitted-versus-approved detail</h4><dl>'
            + pair('Work date', shift.work_date) + pair('Label', shift.detail_label)
            + pair('Recorded start (timezone retained)', shift.shift_start_at) + pair('Recorded end (timezone retained)', shift.shift_end_at)
            + pair('Local start (no timezone inferred)', shift.shift_start_local) + pair('Local end (no timezone inferred)', shift.shift_end_local)
            + pair('Overnight flag', shift.shift_overnight) + pair('Submitted minutes', shift.submitted_minutes)
            + pair('Approved minutes', shift.approved_minutes) + pair('Approved hours', shift.approved_hours) + '</dl>';
          if (shift.shift_start_at === null && shift.shift_start_local === null) yield '<p>Timestamp-free approved detail; no shift endpoints invented.</p>';
          if (shift.segment_pay_ex_vat !== null) {
            yield `<dl><dt>Effective approved segment amount (not selected paid difference)</dt><dd>${money(shift.segment_pay_ex_vat)}</dd>`
              + pair('Pay excluded', shift.pay_excluded) + pair('DAY hours', shift.hours_day) + pair('NIGHT hours', shift.hours_night)
              + pair('SAT hours', shift.hours_sat) + pair('SUN hours', shift.hours_sun) + pair('BH hours', shift.hours_bh) + '</dl>';
          }
          yield tableStart(['Break start (recorded)', 'Break end (recorded)', 'Local start', 'Local end', 'Duration minutes']);
          for await (const item of detailRows({ kind: 'BREAKS', run_line_id: line.run_line_id, shift_detail_id: shift.shift_detail_id }, context)) {
            yield tableRow([item.break_start_at, item.break_end_at, item.break_start_local, item.break_end_local, item.break_minutes]);
            if (item.break_start_at === null && item.break_start_local === null) yield '<tr><td colspan="5">Duration-only break; no clock endpoints invented.</td></tr>';
          }
          yield '</tbody></table>';
        }
        if (work.detail_kind === 'SHIFT' && ['WORK', 'PROTECTED_WORK'].includes(line.component_kind) && shifts === 0) {
          throw new AdviceError('BPAY_NEXT_ADVICE_FROZEN_DETAIL_MISSING');
        }
        if (shifts === 0) yield `<p>${escape(work.detail_kind)} component: no recorded shift timestamps; no aggregate hours inferred.</p>`;
        yield tableStart(['Captured used-rate bucket', 'Approved hours (not paid hours)', 'Recorded source pay rate']);
        let rates = 0;
        for await (const rate of detailRows({ kind: 'RATE_DETAILS', run_line_id: line.run_line_id }, context)) {
          rates += 1;
          if (rates > line.expected_rate_detail_count) invalid();
          yield tableRow([rate.bucket, rate.approved_hours, rate.source_pay_rate]);
        }
        if (rates !== line.expected_rate_detail_count) throw new AdviceError('BPAY_NEXT_ADVICE_FROZEN_DETAIL_MISSING');
        yield '</tbody></table>';
      }
    }
    if (result.complete) break;
  }
  if (!(await works.next()).done) throw new AdviceError('BPAY_NEXT_ADVICE_INSTRUCTION_MEMBERS_MISSING');
  if (count !== BigInt(header.member_count) || adjustments !== (header.destination_kind === 'OWN' ? 1 : 0)) {
    throw new AdviceError('BPAY_NEXT_ADVICE_INSTRUCTION_MEMBERS_MISSING');
  }
  yield '</section><p data-advice-state="complete">COMPLETE: all original instruction constituents and recorded frozen detail rendered.</p></body></html>';
}
async function* chunks(fragments, signal) {
  try {
    for await (const fragment of fragments) {
      if (signal.aborted) return;
      const bytes = ENCODER.encode(fragment);
      for (let offset = 0; offset < bytes.byteLength; offset += BPAY_NEXT_FROZEN_ADVICE_LIMITS.chunkBytes) {
        if (signal.aborted) return;
        yield bytes.subarray(offset, offset + BPAY_NEXT_FROZEN_ADVICE_LIMITS.chunkBytes);
      }
    }
  } catch (error) {
    if (signal.aborted) return;
    // A printable partial document must visibly fail, never masquerade as
    // complete after a missing page. No raw dependency/body/label in the error.
    const code = error instanceof AdviceError ? error.code : 'BPAY_NEXT_ADVICE_PAGE_UNAVAILABLE';
    yield ENCODER.encode(`<p class="integrity" data-advice-state="failed">INTEGRITY ERROR: ${code}. Advice is incomplete; do not use as complete payment advice.</p></body></html>`);
  }
}
export async function renderBpayNextFrozenAdvice(request, input, dependencies = {}) {
  const failure = error => Response.json({ ok: false, error_code: error.code }, {
    status: error.status, headers: { 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' }
  });
  let controller; let removeAbort = () => {};
  try {
    if (!(request instanceof Request) || !object(input) || !UUID.test(input.instruction_id || '')
      || Object.keys(input).some(key => !['instruction_id', 'page_limit'].includes(key))
      || !Number.isInteger(input.page_limit ?? 50) || (input.page_limit ?? 50) < 1 || (input.page_limit ?? 50) > 100) {
      throw new AdviceError('BPAY_NEXT_ADVICE_REQUEST_INVALID', 400);
    }
    if (typeof dependencies.requireOfficeUser !== 'function' || typeof dependencies.advicePageRpc !== 'function') {
      throw new AdviceError('BPAY_NEXT_ADVICE_DEPENDENCY_UNAVAILABLE');
    }
    const actor = await dependencies.requireOfficeUser(request, ['admin']);
    if (!actor || !UUID.test(actor.id || '') || actor.role !== 'admin') throw new AdviceError('BPAY_NEXT_ADVICE_FORBIDDEN', 403);
    controller = new AbortController();
    const abort = () => controller.abort();
    request.signal.addEventListener('abort', abort, { once: true });
    if (request.signal.aborted) abort();
    removeAbort = () => request.signal.removeEventListener('abort', abort);
    const context = { actor, instructionId: input.instruction_id, limit: input.page_limit ?? 50,
      rpc: dependencies.advicePageRpc, signal: controller.signal };
    const result = await page('HEADER', {}, context);
    if (!result.complete || result.rows.length !== 1) invalid();
    const header = validate(result.rows[0], SCHEMAS.HEADER);
    if (!sameId(header.instruction_id, input.instruction_id) || zero(header.cash_amount) || header.cash_amount.startsWith('-')
      || zero(header.projection_cash_amount) || header.projection_cash_amount.startsWith('-')
      || !['gross_ex_vat', 'gross_vat', 'gross_inc_vat', 'accepted_gross_additions', 'accepted_gross_deductions',
        'accepted_net_additions', 'accepted_recoveries'].every(key => nonnegative(header[key]))
      || (header.entered_paye_net !== null && !nonnegative(header.entered_paye_net))
      || (header.input_kind === 'CASE_PAYOUT') !== (header.entered_paye_net === null)) invalid();
    const iterator = chunks(html(header, context), controller.signal)[Symbol.asyncIterator]();
    const body = new ReadableStream({
      async pull(stream) {
        try {
          const item = await iterator.next();
          if (item.done) { removeAbort(); stream.close(); }
          else stream.enqueue(item.value);
        } catch { removeAbort(); controller.abort(); stream.error(new AdviceError('BPAY_NEXT_ADVICE_PAGE_UNAVAILABLE')); }
      },
      async cancel() { controller.abort(); removeAbort(); await iterator.return(); }
    }, { highWaterMark: 0 });
    return new Response(body, { headers: {
      'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff',
      'content-security-policy': "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
      'x-bpay-next-advice-template': 'original-csv-html-v1'
    } });
  } catch (error) {
    controller?.abort(); removeAbort();
    return failure(error instanceof AdviceError ? error : new AdviceError('BPAY_NEXT_ADVICE_PAGE_UNAVAILABLE'));
  }
}
