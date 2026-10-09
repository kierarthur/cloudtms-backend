import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { renderBpayNextFrozenAdvice, BPAY_NEXT_FROZEN_ADVICE_LIMITS } from '../broker/src/bpay-next-frozen-advice.js';
import { createBpayNextFrozenAdvicePageRpc } from '../broker/src/bpay-next-frozen-advice-rpc.js';

const id = number => `00000000-0000-4000-8000-${number.toString(16).padStart(12, '0')}`;
const ACTOR = id(1), INSTRUCTION = id(2), TRANSFER = id(3), PROJECTION = id(4), RUN = id(5), WORKER = id(6);
const WORK_ID = id(7), RUN_WORK = id(8), REVISION = id(9), PHYSICAL = id(10), LINE = id(11), SHIFT = id(12);
const officeRequest = () => new Request('https://broker.test.invalid/api/banking/pay/next/advice/' + INSTRUCTION);
const work = (change = {}) => ({
  run_work_id: RUN_WORK, work_id: WORK_ID, captured_revision_id: REVISION,
  physical_timesheet_id: PHYSICAL, physical_timesheet_version: 1, source_kind: 'ORDINARY', detail_kind: 'SHIFT',
  week_ending_date: '2026-10-04', candidate_display_name: 'Frozen Candidate', candidate_reference: 'W001',
  client_display_name: 'Frozen Client', job_title: 'Frozen Job', band_label: 'Frozen Band', timesheet_reference: 'T001',
  expected_rate_schedule_count: 2, ...change
});
const line = (change = {}) => ({
  run_line_id: LINE, run_work_id: RUN_WORK, captured_revision_id: REVISION, line_no: '1', component_key: 'DAY',
  component_kind: 'WORK', work_date: '2026-09-28', unit_label: 'hour', approved_quantity: '5.000000',
  approved_unit_rate: '20.000000', expected_rate_detail_count: 1, evidence_ref: null,
  approved_source_ex_vat: '100.00', source_consumed_ex_vat: '100.00', frozen_ex_vat: '100.00',
  frozen_vat: '0.00', frozen_inc_vat: '100.00', ...change
});
const header = (change = {}) => ({
  instruction_id: INSTRUCTION, transfer_id: TRANSFER, projection_id: PROJECTION, projection_no: '1', input_kind: 'PAYE_MANUAL',
  run_id: RUN, run_worker_id: WORKER, candidate_id: id(13), pay_date: '2026-10-02', target_pay_channel: 'PAYE',
  beneficiary_name_snapshot: 'Frozen Candidate', payment_reference: 'BP-NEXT-original', issued_at_utc: '2026-10-02T10:00:00.123456+00:00',
  member_count: '3', gross_ex_vat: '100.00', gross_vat: '0.00', gross_inc_vat: '100.00', entered_paye_net: '75.00',
  accepted_gross_additions: '0', accepted_gross_deductions: '0', accepted_net_additions: '0', accepted_recoveries: '10.00', cash_amount: '65.00',
  projection_cash_amount: change.cash_amount ?? '65.00', destination_kind: 'OWN', ...change
});
const caseItem = (change = {}) => ({
  case_instruction_id: id(14), allocation_result_id: id(15), case_id: id(16), case_kind: 'MANUAL_DEBT', case_subtype: 'MANUAL_DEBT',
  tax_treatment: 'NON_TAXABLE', instruction_kind: 'RECOVERY', direction: 'DEDUCTION', payroll_stage: 'NET_DEDUCT',
  resolution_ref: 'fixed-case-ref', explanation: 'Approved recovery', allocated_target_ex_vat: '10', allocated_target_vat: '0', allocated_target_inc_vat: '10',
  ...change
});
const workMember = (change = {}) => ({ member_no: '1', subject_kind: 'WORK', signed_cash_contribution: '100.00',
  work: work(), line: line(), case: null, ...change });
const caseMember = (change = {}) => ({ member_no: '2', subject_kind: 'CASE', signed_cash_contribution: '-10.00',
  work: null, line: null, case: caseItem(), ...change });
const adjustment = (change = {}) => ({ member_no: '3', subject_kind: 'NET_ADJUSTMENT', signed_cash_contribution: '-25.00',
  work: null, line: null, case: null, ...change });
const shift = (change = {}) => ({
  shift_detail_id: SHIFT, detail_no: 1, work_date: '2026-09-28', shift_start_at: '2026-09-28T09:00:00.000001+01:00',
  shift_end_at: '2026-09-28T14:30:00.000001+01:00', shift_start_local: null, shift_end_local: null, shift_overnight: false,
  submitted_minutes: 330, approved_minutes: 300, approved_hours: '5.000000', detail_label: 'Old approved shift',
  segment_pay_ex_vat: '100.00', pay_excluded: false, hours_day: '5.000000', hours_night: '0.000000',
  hours_sat: '0.000000', hours_sun: '0.000000', hours_bh: '0.000000', ...change
});
function fixture(overrides = {}) {
  const data = {
    HEADER: [header()], WORKS: [work()], MEMBERS: [workMember(), caseMember(), adjustment()], SHIFTS: [shift()],
    BREAKS: [{ break_detail_id: id(17), break_no: 1, break_start_at: null, break_end_at: null, break_start_local: null, break_end_local: null, break_minutes: 30 }],
    RATE_DETAILS: [{ rate_detail_id: id(18), bucket: 'DAY', approved_hours: '5.000000', source_pay_rate: '20.000000' }],
    RATE_SCHEDULE: [
      { rate_schedule_id: id(19), rate_family: 'ADDITIONAL', rate_code: 'UNUSED', unit_label: 'unit', paye_rate: '999999999999.123456', umbrella_rate: null, charge_rate: '9.123456' },
      { rate_schedule_id: id(20), rate_family: 'STANDARD', rate_code: 'DAY', unit_label: 'hour', paye_rate: '20.000000', umbrella_rate: '24.000000', charge_rate: '30.000000' }
    ], ...overrides
  };
  const calls = [];
  return {
    data, calls,
    async requireOfficeUser(request, roles) { assert.ok(request instanceof Request); assert.deepEqual(roles, ['admin']); return { id: ACTOR, role: 'admin' }; },
    async advicePageRpc(kind, args, options) {
      calls.push({ kind, args, options });
      assert.equal(args.p_instruction_id, INSTRUCTION); assert.equal(options.actorUserId, ACTOR); assert.ok(options.signal instanceof AbortSignal);
      let rows = data[kind];
      if (kind === 'WORKS') rows = rows.filter(row => args.p_after_work_id === null || row.work_id > args.p_after_work_id);
      if (kind === 'MEMBERS') rows = rows.filter(row => args.p_after_member_no === null || BigInt(row.member_no) > BigInt(args.p_after_member_no));
      if (kind === 'SHIFTS') rows = rows.filter(row => row.detail_no > (args.p_after_detail_no ?? 0));
      if (kind === 'BREAKS') rows = rows.filter(row => row.break_no > (args.p_after_break_no ?? 0));
      if (kind === 'RATE_DETAILS') rows = rows.filter(row => args.p_after_bucket === null || row.bucket > args.p_after_bucket);
      if (kind === 'RATE_SCHEDULE') rows = rows.filter(row => args.p_after_rate_family === null || row.rate_family > args.p_after_rate_family
        || (row.rate_family === args.p_after_rate_family && row.rate_code > args.p_after_rate_code));
      const selected = rows.slice(0, args.p_limit);
      return Response.json({ rows: selected, complete: kind === 'HEADER' || selected.length < args.p_limit });
    }
  };
}
async function render(dependencies = fixture(), input = {}) {
  return renderBpayNextFrozenAdvice(officeRequest(), { instruction_id: INSTRUCTION, ...input }, dependencies);
}
function complete(text) { assert.match(text, /data-advice-state="complete"/); assert.doesNotMatch(text, /data-advice-state="failed"/); }

test('actual HTML renders original gross100/payrollnet75/recovery10/cash65 separately with frozen detail and all available options', async () => {
  const deps = fixture(); const response = await render(deps, { page_limit: 1 }); const text = await response.text();
  assert.equal(response.headers.get('content-type'), 'text/html; charset=utf-8');
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.match(response.headers.get('content-security-policy'), /default-src 'none'/);
  complete(text);
  for (const expected of ['£100.00', '£75.00', '£10.00', '£65.00', '£-25.00', '5.000000', '30', 'UNUSED', '999999999999.123456',
    'fixed-case-ref', 'Frozen Job', 'Frozen Band', '2026-10-02T10:00:00.123456+00:00', '2026-09-28T09:00:00.000001+01:00']) assert.ok(text.includes(expected), expected);
  assert.match(text, /issuance is not settlement/); assert.match(text, /Duration-only break; no clock endpoints invented/);
  assert.equal(deps.calls.filter(call => call.kind === 'RATE_SCHEDULE').length, 3);
  assert.deepEqual(deps.calls.filter(call => call.kind === 'MEMBERS').map(call => call.args.p_after_member_no), [null, '1', '2', '3']);
  assert.deepEqual(deps.calls.filter(call => call.kind === 'SHIFTS').map(call => call.args.p_after_detail_no), [null, 1]);
});

test('hostile labels, unit codes, references and case explanations remain escaped text, never active HTML', async () => {
  const attack = '<script>alert("&")</script><img src=x onerror=alert(1)>\'hostile\'';
  const w = work({ candidate_display_name: attack, client_display_name: attack, job_title: attack, band_label: attack, timesheet_reference: attack });
  const deps = fixture({ HEADER: [header({ beneficiary_name_snapshot: attack, payment_reference: attack })], WORKS: [w],
    MEMBERS: [workMember({ work: w, line: line({ evidence_ref: attack, unit_label: attack }) }), caseMember({ case: caseItem({ explanation: attack, resolution_ref: attack }) }), adjustment()],
    SHIFTS: [shift({ detail_label: attack })], RATE_SCHEDULE: [] });
  w.expected_rate_schedule_count = 0;
  const text = await (await render(deps)).text(); complete(text);
  assert.doesNotMatch(text, /<script|<img/); assert.match(text, /&lt;script&gt;/); assert.match(text, /&quot;&amp;&quot;/); assert.match(text, /&#39;hostile&#39;/);
});

test('later current Timesheet/settings and latest NET mutation cannot change original issued output', async () => {
  const deps = fixture({ SHIFTS: [shift({ approved_hours: '7.500000', approved_minutes: 450 })],
    MEMBERS: [workMember({ line: line({ approved_quantity: '7.500000', approved_source_ex_vat: '150.00', source_consumed_ex_vat: '150.00', frozen_ex_vat: '150.00', frozen_inc_vat: '150.00' }), signed_cash_contribution: '150.00' }), caseMember(), adjustment({ signed_cash_contribution: '-75.00' })] });
  const before = await (await render(deps)).text();
  deps.currentTimesheet = { hours: '8', money: '160', label: 'NEW MUTABLE LABEL', sourceVersion: 2 };
  deps.currentSettings = { rate: '999', band: 'NEW BAND' }; deps.latestProjection = { entered_paye_net: '999' };
  const after = await (await render(deps)).text();
  assert.equal(after, before); complete(after); assert.match(after, /7.500000/); assert.match(after, /£150.00/); assert.doesNotMatch(after, /NEW MUTABLE LABEL|NEW BAND|£999/);
});

test('split OWN70 and ONEOFF40 print frozen instruction cash separately from accepted projection110', async () => {
  const own = fixture({ HEADER: [header({ cash_amount: '70.00', projection_cash_amount: '110.00', entered_paye_net: '70.00',
    accepted_net_additions: '40.00', accepted_recoveries: '0.00', member_count: '2' })],
    MEMBERS: [workMember(), adjustment({ member_no: '2', signed_cash_contribution: '-30.00' })] });
  const external = fixture({ HEADER: [header({ destination_kind: 'ONEOFF', cash_amount: '40.00', projection_cash_amount: '110.00',
    entered_paye_net: '70.00', accepted_net_additions: '40.00', accepted_recoveries: '0.00', member_count: '1' })], WORKS: [],
    MEMBERS: [caseMember({ member_no: '1', signed_cash_contribution: '40.00', case: caseItem({ case_kind: 'CREDIT', case_subtype: 'MANUAL_CREDIT',
      instruction_kind: 'CREDIT', direction: 'PAYMENT', payroll_stage: 'NET_ADD', allocated_target_ex_vat: '40.00',
      allocated_target_inc_vat: '40.00', explanation: 'Genuine original one-off credit' }) })] });
  for (const [deps, kind, amount] of [[own, 'Candidate own account', '70.00'], [external, 'Stored one-off account', '40.00']]) {
    const text = await (await render(deps, { page_limit: 1 })).text(); complete(text);
    assert.match(text, /Accepted total bank cash \(all destinations\)<\/dt><dd>£110.00/);
    assert.ok(text.includes(`Original bank cash instruction</dt><dd>£${amount}`));
    assert.ok(text.includes(kind));
    assert.match(text, /whole accepted projection, not another payment by this instruction/);
  }
  const externalBefore = await (await render(external)).text();
  external.currentOneoffBank = { beneficiary: 'CHANGED', version: 'new', amount: '999' };
  external.currentCandidate = { bank: 'new' }; external.latestProjection = { cash: '999' };
  const externalAfter = await (await render(external)).text();
  assert.equal(externalAfter, externalBefore); complete(externalAfter);
  assert.doesNotMatch(externalAfter, /Recorded payroll net adjustment|Frozen Job|Captured shift|CHANGED|£999/);
});

test('ONEOFF rejects cross-leg WORK, net adjustment or noncredit constituents and OWN retains one adjustment', async () => {
  const credit = caseMember({ member_no: '1', signed_cash_contribution: '40.00', case: caseItem({ case_kind: 'CREDIT',
    instruction_kind: 'CREDIT', direction: 'PAYMENT', payroll_stage: 'NET_ADD', allocated_target_ex_vat: '40.00', allocated_target_inc_vat: '40.00' }) });
  for (const rows of [[workMember()], [adjustment({ member_no: '1' })], [caseMember({ member_no: '1' })],
    [credit, adjustment({ member_no: '2' })]]) {
    const text = await (await render(fixture({ HEADER: [header({ destination_kind: 'ONEOFF', cash_amount: '40.00',
      projection_cash_amount: '110.00', member_count: String(rows.length) })], WORKS: [], MEMBERS: rows }))).text();
    assert.match(text, /data-advice-state="failed"/); assert.doesNotMatch(text, /data-advice-state="complete"/);
  }
  for (const rows of [[workMember()], [workMember(), adjustment({ member_no: '2' }), adjustment()]]) {
    const text = await (await render(fixture({ HEADER: [header({ member_count: String(rows.length) })], MEMBERS: rows }))).text();
    assert.match(text, /data-advice-state="failed"/); assert.doesNotMatch(text, /data-advice-state="complete"/);
  }
});

test('new frozen header fields are closed typed decimal text and explicit destination, not a live-bank fallback', async () => {
  for (const change of [{ projection_cash_amount: 110 }, { projection_cash_amount: '0.00' },
    { projection_cash_amount: '-110.00' }, { destination_kind: 'ARBITRARY' }, { current_bank: {} }]) {
    const response = await render(fixture({ HEADER: [header(change)] }));
    assert.equal(response.status, 503); assert.equal((await response.json()).error_code, 'BPAY_NEXT_ADVICE_RESPONSE_INVALID');
  }
});

test('top-up prints selected10 distinctly from full approval160 and8 approved hours without manufacturing paid hours', async () => {
  const deps = fixture({ HEADER: [header({ gross_ex_vat: '10.00', gross_inc_vat: '10.00', entered_paye_net: '10.00', accepted_recoveries: '0.00', cash_amount: '10.00', member_count: '2' })],
    MEMBERS: [workMember({ signed_cash_contribution: '10.00', line: line({ approved_quantity: '8.000000', approved_source_ex_vat: '160.00', source_consumed_ex_vat: '10.00', frozen_ex_vat: '10.00', frozen_inc_vat: '10.00' }) }), adjustment({ member_no: '2', signed_cash_contribution: '0.00' })],
    SHIFTS: [shift({ approved_hours: '8.000000', approved_minutes: 480, segment_pay_ex_vat: '160.00', hours_day: '8.000000' })] });
  const text = await (await render(deps)).text(); complete(text);
  assert.match(text, /Full captured approved component source amount<\/dt><dd>£160.00/);
  assert.match(text, /Selected source difference<\/dt><dd>£10.00/);
  assert.match(text, /Approved quantity \(not paid hours\)<\/dt><dd>8.000000/);
  assert.doesNotMatch(text, /paid hours<\/dt>|£170/);
});

test('Source overnight and unknown rate retain exact local clocks without inferred timezone or blended rate', async () => {
  const source = work({ source_kind: 'SOURCE', detail_kind: 'AGGREGATE' });
  const deps = fixture({ WORKS: [source], MEMBERS: [workMember({ work: source }), caseMember(), adjustment()],
    SHIFTS: [shift({ shift_start_at: null, shift_end_at: null, shift_start_local: '21:00', shift_end_local: '07:30', shift_overnight: true, segment_pay_ex_vat: null,
      pay_excluded: null, hours_day: null, hours_night: null, hours_sat: null, hours_sun: null, hours_bh: null })],
    RATE_DETAILS: [{ rate_detail_id: id(18), bucket: 'NIGHT', approved_hours: '5.000000', source_pay_rate: null }] });
  const text = await (await render(deps)).text(); complete(text);
  assert.match(text, /Local start \(no timezone inferred\)<\/dt><dd>21:00/); assert.match(text, /Local end \(no timezone inferred\)<\/dt><dd>07:30/);
  assert.match(text, /Overnight flag<\/dt><dd>true/); assert.match(text, /<td>NIGHT<\/td><td>5.000000<\/td><td>Not recorded<\/td>/);
});

test('expense-only fixed component retains payment/evidence and honest absent shifts', async () => {
  const expense = work({ source_kind: 'EXPENSE', detail_kind: 'FIXED', expected_rate_schedule_count: 0 });
  const deps = fixture({ WORKS: [expense], RATE_SCHEDULE: [], RATE_DETAILS: [], SHIFTS: [],
    MEMBERS: [workMember({ work: expense, line: line({ component_kind: 'EXPENSE', expected_rate_detail_count: 0, approved_quantity: null, approved_unit_rate: null, evidence_ref: 'expense-original-ref' }) }), caseMember(), adjustment()] });
  const text = await (await render(deps)).text(); complete(text); assert.match(text, /expense-original-ref/);
  assert.match(text, /FIXED component: no recorded shift timestamps; no aggregate hours inferred/);
});

test('explicit zero WORK retains genuine quantity/detail; no fabricated external zero instruction', async () => {
  const deps = fixture({ HEADER: [header({ member_count: '4' })], MEMBERS: [workMember(),
    workMember({ member_no: '2', signed_cash_contribution: '0.00', line: line({ run_line_id: id(40), line_no: '2', approved_source_ex_vat: '0.00', source_consumed_ex_vat: '0.00', frozen_ex_vat: '0.00', frozen_inc_vat: '0.00', component_key: 'excluded' }) }),
    caseMember({ member_no: '3' }), adjustment({ member_no: '4' })] });
  const base = deps.advicePageRpc;
  deps.advicePageRpc = (kind, args, options) => kind === 'SHIFTS' && args.p_run_line_id === id(40)
    ? Response.json({ rows: [shift({ shift_detail_id: id(41), segment_pay_ex_vat: '0.00', pay_excluded: true })], complete: true })
    : base(kind, args, options);
  const text = await (await render(deps)).text(); complete(text); assert.match(text, /Explicit frozen zero payment line/);
  const response = await render(fixture({ HEADER: [header({ cash_amount: '0.00' })] }));
  assert.equal(response.status, 503); assert.equal((await response.json()).error_code, 'BPAY_NEXT_ADVICE_RESPONSE_INVALID');
});

test('CASE_PAYOUT shows loan cash separately from absent payroll net; no earnings invented', async () => {
  const deps = fixture({ HEADER: [header({ input_kind: 'CASE_PAYOUT', entered_paye_net: null, gross_ex_vat: '0.00', gross_inc_vat: '0.00',
    accepted_recoveries: '0.00', accepted_net_additions: '35.00', cash_amount: '35.00', member_count: '2' })], WORKS: [],
    MEMBERS: [caseMember({ member_no: '1', signed_cash_contribution: '35.00', case: caseItem({ case_kind: 'LOAN', case_subtype: 'LOAN', instruction_kind: 'PAYOUT',
      direction: 'PAYMENT', payroll_stage: 'NET_ADD', allocated_target_ex_vat: '35', allocated_target_inc_vat: '35' }) }), adjustment({ member_no: '2', signed_cash_contribution: '0.00' })] });
  const text = await (await render(deps)).text(); complete(text);
  assert.match(text, /Original accepted payroll net<\/dt><dd>Not payroll earnings/); assert.match(text, /Original bank cash instruction<\/dt><dd>£35.00/);
});

test('mandatory frozen detail absence and prematurely completed member pages are visibly incomplete', async () => {
  for (const overrides of [ { WORKS: [] }, { SHIFTS: [] }, { RATE_DETAILS: [] }, { RATE_SCHEDULE: [] }, { MEMBERS: [workMember()] } ]) {
    const text = await (await render(fixture(overrides))).text();
    assert.match(text, /data-advice-state="failed"/); assert.doesNotMatch(text, /data-advice-state="complete"/);
    assert.match(text, /INTEGRITY ERROR/);
  }
});

test('WORKS/member merge refuses skipped work or divergent immutable labels without a candidate-wide ID set', async () => {
  const other = work({ work_id: id(70), run_work_id: id(71), captured_revision_id: id(72) });
  for (const deps of [ fixture({ WORKS: [work(), other] }),
    fixture({ WORKS: [work({ job_title: 'different frozen label' })] }),
    fixture({ MEMBERS: [workMember({ work: other, line: line({ run_work_id: id(71), captured_revision_id: id(72) }) }), caseMember(), adjustment()] }) ]) {
    const text = await (await render(deps)).text(); assert.match(text, /data-advice-state="failed"/); assert.doesNotMatch(text, /data-advice-state="complete"/);
  }
});

test('a lost second page and repeating cursor cannot produce a successful completion marker', async () => {
  for (const mode of ['lost', 'repeat', 'empty-incomplete']) {
    const deps = fixture(); const base = deps.advicePageRpc;
    deps.advicePageRpc = async (kind, args, options) => {
      if (kind === 'MEMBERS' && args.p_after_member_no !== null) {
        if (mode === 'lost') throw new Error('PRIVATE CREDENTIAL SECRET MUST NOT BE OUTPUT');
        if (mode === 'empty-incomplete') return Response.json({ rows: [], complete: false });
        return Response.json({ rows: [workMember()], complete: false });
      }
      return base(kind, args, options);
    };
    const text = await (await render(deps, { page_limit: 1 })).text();
    assert.match(text, /data-advice-state="failed"/); assert.doesNotMatch(text, /data-advice-state="complete"|PRIVATE CREDENTIAL/);
  }
});

test('foreign child/revision, inconsistent signed case value and new unrecognised fields are refused', async () => {
  for (const bad of [workMember({ line: line({ captured_revision_id: id(99) }) }), workMember({ line: { ...line(), mutableTimesheet: {} } }),
    caseMember({ signed_cash_contribution: '10.00' }), adjustment({ subject_kind: 'CASH_REISSUE' })]) {
    const text = await (await render(fixture({ MEMBERS: [bad, adjustment({ member_no: '4' })] }))).text();
    assert.match(text, /data-advice-state="failed"/); assert.doesNotMatch(text, /data-advice-state="complete"/);
  }
});

test('authoritative financial values must remain decimal text and unsigned category amounts', async () => {
  for (const item of [header({ gross_inc_vat: 100 }), header({ entered_paye_net: null }), header({ accepted_recoveries: '-10.00' }),
    header({ cash_amount: '9007199254740993.01', entered_paye_net: '9007199254740993.01' })]) {
    const response = await render(fixture({ HEADER: [item] }));
    if (typeof item.cash_amount === 'string' && item.cash_amount.startsWith('9007199')) {
      const text = await response.text(); complete(text); assert.match(text, /£9007199254740993.01/);
    } else assert.equal(response.status, 503);
  }
});

test('bounded raw Response stream rejects excessive bytes before JSON and closes its reader', async () => {
  let cancelled = false;
  const deps = fixture(); deps.advicePageRpc = async () => new Response(new ReadableStream({
    start(controller) { controller.enqueue(new Uint8Array(BPAY_NEXT_FROZEN_ADVICE_LIMITS.pageBytes + 1)); },
    cancel() { cancelled = true; }
  }));
  const response = await render(deps); assert.equal(response.status, 503);
  assert.equal((await response.json()).error_code, 'BPAY_NEXT_ADVICE_PAGE_TOO_LARGE'); assert.equal(cancelled, true);
});

test('abort closes a late raw transport body even when the dependency ignored cancellation', async () => {
  const controller = new AbortController(); let resolveResponse; let cancelled = false;
  const deps = fixture();
  deps.advicePageRpc = () => new Promise(resolve => { resolveResponse = resolve; });
  const pending = renderBpayNextFrozenAdvice(new Request('https://broker.test.invalid/advice', { signal: controller.signal }), { instruction_id: INSTRUCTION }, deps);
  while (!resolveResponse) await new Promise(resolve => setTimeout(resolve, 0));
  controller.abort();
  const response = await pending; assert.equal(response.status, 503);
  resolveResponse(new Response(new ReadableStream({ cancel() { cancelled = true; } })));
  await new Promise(resolve => setTimeout(resolve, 0)); assert.equal(cancelled, true);
});

test('zero buffering until consumption; cancel aborts page signal and does not fetch later rows', async () => {
  const deps = fixture(); const response = await render(deps, { page_limit: 1 });
  assert.equal(deps.calls.length, 1); // only bounded HEADER, no eager WORK/MEMBER/detail fetch
  const reader = response.body.getReader(); await reader.read(); await reader.cancel();
  assert.equal(deps.calls.length, 1); assert.equal(deps.calls[0].options.signal.aborted, false); // completed header signal is already released
  const controller = new AbortController(); const request = new Request('https://broker.test.invalid/advice', { signal: controller.signal });
  let aborted = false; let memberCalls = 0; const pending = fixture(); const base = pending.advicePageRpc;
  pending.advicePageRpc = (kind, args, options) => {
    if (kind !== 'MEMBERS') return base(kind, args, options);
    memberCalls += 1;
    return new Promise((_, reject) => options.signal.addEventListener('abort', () => { aborted = true; reject(new Error('aborted')); }, { once: true }));
  };
  const streamed = await renderBpayNextFrozenAdvice(request, { instruction_id: INSTRUCTION }, pending);
  const reading = streamed.text();
  while (memberCalls === 0) await new Promise(resolve => setTimeout(resolve, 0));
  controller.abort(); await reading;
  assert.equal(aborted, true); assert.equal(memberCalls, 1);
});

test('300 actual rendered constituents use bounded pages/chunks and all rate options only once per work', async () => {
  const deps = fixture({ HEADER: [header({ member_count: '301', gross_ex_vat: '30000.00', gross_inc_vat: '30000.00',
    entered_paye_net: '29975.00', accepted_recoveries: '0.00', cash_amount: '29975.00' })] }); const base = deps.advicePageRpc;
  let maxPage = 0, nextMember = 0;
  deps.advicePageRpc = async (kind, args, options) => {
    if (kind !== 'MEMBERS') return base(kind, args, options);
    const start = args.p_after_member_no === null ? 1 : Number(args.p_after_member_no) + 1;
    assert.equal(start, nextMember + 1);
    const rows = [];
    for (let n = start; n <= 301 && rows.length < args.p_limit; n++) rows.push(n === 301 ? adjustment({ member_no: '301' })
      : workMember({ member_no: String(n), line: line({ run_line_id: id(100 + n), line_no: String(n) }) }));
    nextMember += rows.length; maxPage = Math.max(maxPage, rows.length);
    return Response.json({ rows, complete: rows.length < args.p_limit });
  };
  const response = await render(deps, { page_limit: 17 }); const reader = response.body.getReader();
  let maximumChunk = 0, byteTotal = 0, tail = '';
  while (true) {
    const item = await reader.read(); if (item.done) break;
    maximumChunk = Math.max(maximumChunk, item.value.byteLength); byteTotal += item.value.byteLength;
    tail = new TextDecoder().decode(item.value);
  }
  assert.equal(nextMember, 301); assert.equal(maxPage, 17); assert.ok(maximumChunk <= BPAY_NEXT_FROZEN_ADVICE_LIMITS.chunkBytes);
  assert.ok(byteTotal > 500000); assert.match(tail, /data-advice-state="complete"/);
  assert.equal(deps.calls.filter(call => call.kind === 'RATE_SCHEDULE').length, 1);
});

test('long escaped labels split into capped byte chunks without changing decoded output', async () => {
  const label = '&'.repeat(8192); const w = work({ candidate_display_name: label });
  const response = await render(fixture({ WORKS: [w], MEMBERS: [workMember({ work: w }), caseMember(), adjustment()] }));
  const reader = response.body.getReader(); const decoder = new TextDecoder(); let text = ''; let max = 0;
  while (true) { const item = await reader.read(); if (item.done) break; max = Math.max(max, item.value.byteLength); text += decoder.decode(item.value, { stream: true }); }
  text += decoder.decode(); complete(text); assert.ok(max <= 16384); assert.ok(text.includes('&amp;'.repeat(8192)));
});

test('strict auth and request identity fail before transport; caller cannot provide actor, money or labels', async () => {
  for (const input of [{ instruction_id: 'bad' }, { entered_paye_net: '75.00' }, { candidate_display_name: 'fake' }, { actor_user_id: ACTOR }, { page_limit: 101 }]) {
    const deps = fixture(); const response = await render(deps, input); assert.equal(response.status, 400); assert.equal(deps.calls.length, 0);
  }
  const deps = fixture(); deps.requireOfficeUser = async () => ({ id: ACTOR, role: 'candidate' });
  assert.equal((await render(deps)).status, 403); assert.equal(deps.calls.length, 0);
});

test('fixed raw adapter binds validated actor/instruction, redirect:error and exact read endpoint without retries', async () => {
  const calls = []; const rpc = createBpayNextFrozenAdvicePageRpc({ baseUrl: 'https://db.test.invalid', headers: { authorization: 'Bearer TEST' },
    fetchImpl: async (...args) => { calls.push(args); return Response.json({ rows: [], complete: true }); } });
  await rpc('HEADER', { p_instruction_id: INSTRUCTION, p_limit: 1 }, { actorUserId: ACTOR, signal: new AbortController().signal });
  assert.equal(calls.length, 1); assert.equal(calls[0][0], 'https://db.test.invalid/rest/v1/rpc/bpay_next_original_csv_advice_page_v1');
  assert.deepEqual(JSON.parse(calls[0][1].body), { p_instruction_id: INSTRUCTION, p_limit: 1, p_kind: 'HEADER', p_actor_user_id: ACTOR });
  assert.equal(calls[0][1].redirect, 'error');
  for (const [kind, args] of [['arbitrary_rpc', {}], ['HEADER', { p_actor_user_id: ACTOR }], ['HEADER', { p_clock: '2026-10-03' }]]) {
    await assert.rejects(rpc(kind, { p_instruction_id: INSTRUCTION, p_limit: 1, ...args }, { actorUserId: ACTOR, signal: new AbortController().signal }));
  }
  assert.equal(calls.length, 1);
  for (const [kind, args] of [ ['WORKS', { p_after_work_id: 'wrong' }], ['MEMBERS', { p_after_member_no: 1 }],
    ['SHIFTS', { p_run_line_id: LINE, p_after_detail_no: -1 }], ['BREAKS', { p_run_line_id: LINE, p_after_break_no: null }],
    ['RATE_DETAILS', { p_run_line_id: LINE, p_after_bucket: 'guessed' }],
    ['RATE_SCHEDULE', { p_run_work_id: RUN_WORK, p_after_rate_family: 'STANDARD', p_after_rate_code: null }] ]) {
    await assert.rejects(rpc(kind, { p_instruction_id: INSTRUCTION, p_limit: 1, ...args }, { actorUserId: ACTOR, signal: new AbortController().signal }));
  }
  assert.equal(calls.length, 1);
});

test('new SQL is read-only captured authority, not latest/current reconstruction, and grants service only', () => {
  const sql = readFileSync(new URL('../supabase/repeatable/04102026_0255_banking_pay_next_original_csv_advice_page_v1.sql', import.meta.url), 'utf8');
  assert.match(sql, /where id=v_transfer\.projection_id/); assert.match(sql, /r\.id=rw\.captured_revision_id/);
  assert.match(sql, /m\.transfer_id=\$1 and m\.member_no>coalesce/); assert.match(sql, /order by m\.member_no limit \$12/);
  assert.match(sql, /set timezone='UTC'/); assert.match(sql, /octet_length\(v_row\.row_value::text\)/);
  assert.match(sql, /where transfer_id=v_transfer\.id/);
  assert.match(sql, /'cash_amount',v_instruction\.cash_amount::text/);
  assert.match(sql, /'projection_cash_amount',v_projection\.cash_amount::text,'destination_kind',v_destination_kind/);
  assert.match(sql, /v_net\.own_amount\+v_net\.external_amount is distinct from v_projection\.cash_amount/);
  assert.match(sql, /v_transfer\.cash_amount is distinct from v_net\.own_amount/);
  assert.match(sql, /v_destination\.amount<>v_transfer\.cash_amount/);
  assert.doesNotMatch(sql, /perform private\.bpay_next_destination_execution_leg|:=private\.bpay_next_destination_execution_leg|from public\.pay_finance_case_oneoff|from public\.candidates|bank_version_at_utc/);
  assert.doesNotMatch(sql, /\b(insert into|update private|delete from|count\s*\(|sum\s*\(|offset\s+|current_revision_id|net_projection_revision)\b/i);
  assert.match(sql, /from public,anon,authenticated;/); assert.match(sql, /to service_role;/);
  assert.doesNotMatch(sql, /grant.*private\.bpay_next_advice_work/i);
});
