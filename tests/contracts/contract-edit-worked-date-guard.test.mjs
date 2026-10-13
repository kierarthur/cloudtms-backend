import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import { protectedContractWorkedDates, readContractWorkedDateBoundary, readContractWorkedDatePage,
  contractWorkedDateRefusal } from '../../broker/src/contract-worked-date-guard.js';

const id = n => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const contractId = id(1);
const row = patch => ({ timesheet_id: id(2), contract_id: contractId, status: 'STORED',
  is_current: true, revoked_at: null, archived_at_utc: null, sheet_scope: 'WEEKLY', line_type: 'HOURS',
  actual_schedule_json: [{ date: '2026-10-07' }], worked_start_iso: null, worked_end_iso: null, worked_minutes: null,
  authorised_at_server: null, r2_nurse_key: null, r2_auth_key: null, img_sha256_nurse: null,
  img_sha256_auth: null, qr_signed_at_utc: null, qr_signed_hash: null, candidate_manager_approved_at_utc: null, ...patch });
const financial = patch => ({ id: id(3), timesheet_id: id(2), is_current: true, client_id: id(4),
  actual_minutes_by_day_json: {}, external_source_rows_json: {}, processing_status: 'UNPROCESSED',
  total_hours: 0, total_pay_ex_vat: 0, total_charge_ex_vat: 0, processed_at_utc: null, authorised_at_utc: null,
  locked_by_invoice_id: null, paid_at_utc: null, ...patch });
const shift = patch => ({ id: id(5), contract_id: contractId, timesheet_id: id(2), client_id: id(4),
  work_date: '2026-10-07', cancelled_at_utc: null, ...patch });

for (const route of ['ordinary weekly', 'TIMESHEET_AUTHORITY', 'NHSP', 'import-authoritative', 'older HealthRoster']) {
  test(`CE-1/2/3 ${route}: guard uses first/last worked days and permits a mid-week end`, () => {
    const boundary = protectedContractWorkedDates([row({ actual_schedule_json: [{ date: '2026-10-05' }, { date: '2026-10-07' }] })], [], [], contractId);
    assert.match(contractWorkedDateRefusal(boundary, '2026-10-01', '2026-10-06'), /07\/10\/2026/);
    assert.equal(contractWorkedDateRefusal(boundary, '2026-10-01', '2026-10-08'), null);
    assert.match(contractWorkedDateRefusal(boundary, '2026-10-06', '2026-10-11'), /05\/10\/2026/);
  });
}

test('CE-4 / I-9 Daily uses London worked date across BST midnight, never its Sunday week ending', () => {
  const boundary = protectedContractWorkedDates([row({ sheet_scope: 'DAILY', week_ending_date: '2026-10-11',
    actual_schedule_json: [], worked_start_iso: '2026-10-06T23:30:00Z' })], [], [], contractId);
  assert.equal(boundary.min_ts_date, '2026-10-07'); assert.equal(boundary.max_ts_date, '2026-10-07');
  assert.match(contractWorkedDateRefusal(boundary, '2026-10-01', '2026-10-06'), /07\/10\/2026/);
  assert.equal(contractWorkedDateRefusal(boundary, '2026-10-01', '2026-10-08'), null);
});

test('CE-5 excludes existing draft/void/cancelled status variants, revoked, archived and superseded records', () => {
  const statuses = ['DRAFT', 'VOID', 'VOIDED', 'CANCELLED', 'CANCELED'];
  const rows = statuses.map((status, index) => row({ timesheet_id: id(10 + index), status, actual_schedule_json: null }));
  rows.push(row({ timesheet_id: id(20), revoked_at: '2026-10-12T00:00:00Z' }),
    row({ timesheet_id: id(21), archived_at_utc: '2026-10-12T00:00:00Z' }), row({ timesheet_id: id(22), is_current: false }));
  const boundary = protectedContractWorkedDates(rows, [], [], contractId);
  assert.equal(boundary.min_ts_date, null); assert.equal(boundary.max_ts_date, null);
  assert.equal(contractWorkedDateRefusal(boundary, '2026-10-10', '2026-10-12'), null);
});

test('CE-6 empty-schedule import original unions current financial days, source rows and noncancelled shifts', () => {
  const boundary = protectedContractWorkedDates([row({ actual_schedule_json: [] })], [financial({
    actual_minutes_by_day_json: { '2026-10-06': 660 },
    external_source_rows_json: { HEALTHROSTER_WEEKLY: [{ date: '2026-10-08', raw_row: { date: '2099-12-31' } }], source_system: 'HEALTHROSTER', evidence_kind: 'HEALTHROSTER_WEEKLY', source_systems: ['HEALTHROSTER'] }
  }), financial({ id: id(6), is_current: false, actual_minutes_by_day_json: { '2020-01-01': 660 } })],
  [shift({ work_date: '2026-10-05' }), shift({ id: id(7), work_date: '2026-10-09', cancelled_at_utc: '2026-10-10T00:00:00Z' })], contractId);
  assert.equal(boundary.min_ts_date, '2026-10-05'); assert.equal(boundary.max_ts_date, '2026-10-08');
  assert.match(contractWorkedDateRefusal(boundary, '2026-10-01', '2026-10-07'), /08\/10\/2026/);
  assert.equal(contractWorkedDateRefusal(boundary, '2026-10-01', '2026-10-09'), null);
});

test('CN-1/CN-2 / I-9 newer start moved back to Wednesday is allowed only when no earlier worked day exists', () => {
  const boundary = protectedContractWorkedDates([row()], [], [], contractId);
  assert.equal(contractWorkedDateRefusal(boundary, '2026-10-07', '2026-10-11'), null);
  const earlier = protectedContractWorkedDates([row({ actual_schedule_json: [{ date: '2026-10-06' }, { date: '2026-10-07' }] })], [], [], contractId);
  assert.match(contractWorkedDateRefusal(earlier, '2026-10-07', '2026-10-11'), /06\/10\/2026/);
});

test('known legacy financial JSON shapes and current typed lineage contribute only their worked dates', () => {
  for (const external_source_rows_json of [[{ work_date: '2026-10-08' }], { HR_WEEKLY: [{ date: '2026-10-08' }] },
    { NHSP_WEEKLY: [{ date: '2026-10-08' }] }, { IMPORTED_WEEKLY_MIXED: [{ date: '2026-10-08' }] },
    { schema_version: 'WEEKLY_SOURCE_ORDINARY_EXTERNAL_ROWS_V1', source_mode: 'ROSTER', segments: [
      { pay_vector: { clocks: { work_date: '2026-10-08' } }, charge_vector: { clocks: { work_date: '2026-10-08' } } }
    ] }]) {
    const boundary = protectedContractWorkedDates([row()], [financial({ external_source_rows_json })], [], contractId);
    assert.equal(boundary.max_ts_date, '2026-10-08');
  }
  const arrayMinutes = financial({ actual_minutes_by_day_json: [{ date: '2026-10-09', minutes: 660 }] });
  assert.equal(protectedContractWorkedDates([row()], [arrayMinutes], [], contractId).max_ts_date, '2026-10-09');
});

test('missing actual work, malformed JSON/date/aliases, foreign rows and unknown source formats cannot become an empty range', () => {
  for (const run of [
    () => protectedContractWorkedDates([row({ actual_schedule_json: null })], [], [], contractId),
    () => protectedContractWorkedDates([row({ actual_schedule_json: 'not-json' })], [], [], contractId),
    () => protectedContractWorkedDates([row({ actual_schedule_json: [{ date: '2026-02-30' }] })], [], [], contractId),
    () => protectedContractWorkedDates([row({ actual_schedule_json: [{ date: '2026-10-07', work_date: '2026-10-08' }] })], [], [], contractId),
    () => protectedContractWorkedDates([row({ contract_id: id(99) })], [], [], contractId),
    () => protectedContractWorkedDates([row()], [financial({ external_source_rows_json: { UNKNOWN: [{ date: '2026-10-07' }] } })], [], contractId),
    () => protectedContractWorkedDates([row({ sheet_scope: 'DAILY', actual_schedule_json: [], worked_start_iso: '2026-10-07T01:00:00' })], [], [], contractId)
  ]) assert.throws(run, error => error.code === 'CONTRACT_WORKED_DATES_UNAVAILABLE');
  const emptyExpenses = protectedContractWorkedDates([row({ line_type: 'EXPENSES', actual_schedule_json: [] })], [], [], contractId);
  assert.equal(emptyExpenses.min_ts_date, null);
});

test('proved empty RECEIVED record and empty Contract contribute no date without a fictitious draft status', () => {
  for (const financials of [[], [financial()]]) {
    const boundary = protectedContractWorkedDates([row({ status: 'RECEIVED', actual_schedule_json: [] })], financials, [], contractId);
    assert.equal(boundary.min_ts_date, null); assert.equal(boundary.max_ts_date, null);
    assert.equal(contractWorkedDateRefusal(boundary, '2026-10-10', '2026-10-11'), null);
  }
  assert.equal(protectedContractWorkedDates([], [], [], contractId).min_ts_date, null);
});

test('empty hours with signed/authorised/processed/money witness or missing empty proof remain held', () => {
  for (const patch of [{ worked_minutes: 1 }, { authorised_at_server: '2026-10-07T00:00:00Z' },
    { worked_end_iso: '2026-10-07T00:00:00Z' },
    { img_sha256_nurse: 'signature-digest' }, { candidate_manager_approved_at_utc: '2026-10-07T00:00:00Z' },
    { worked_minutes: undefined }, { actual_schedule_json: null }]) {
    const record = row({ actual_schedule_json: [], ...patch });
    if (patch.worked_minutes === undefined && Object.hasOwn(patch, 'worked_minutes')) delete record.worked_minutes;
    assert.throws(() => protectedContractWorkedDates([record], [], [], contractId), { code: 'CONTRACT_WORKED_DATES_UNAVAILABLE' });
  }
  for (const patch of [{ total_hours: 1 }, { total_charge_ex_vat: -1 }, { processed_at_utc: '2026-10-07T00:00:00Z' },
    { authorised_at_utc: '2026-10-07T00:00:00Z' }, { locked_by_invoice_id: id(9) }, { processing_status: 'READY_FOR_INVOICE' },
    { total_hours: null }]) {
    assert.throws(() => protectedContractWorkedDates([row({ actual_schedule_json: [] })], [financial(patch)], [], contractId),
      { code: 'CONTRACT_WORKED_DATES_UNAVAILABLE' });
  }
});

test('complete source reader paginates beyond server page caps and requests only verified physical columns', async () => {
  const rows = Array.from({ length: 503 }, (_, index) => row({ timesheet_id: id(index + 100), actual_schedule_json: [{ date: '2026-10-07' }] }));
  let timesheetCalls = 0; let financialCalls = 0;
  const boundary = await readContractWorkedDateBoundary({ contractId, baseUrl: 'https://source.invalid', async readPage(target, options) {
    assert.equal(options.preferExactCount, true); const url = new URL(target);
    const table = url.pathname.split('/').at(-1); const offset = Number(url.searchParams.get('offset'));
    if (table === 'timesheets') {
      timesheetCalls++; assert.equal(url.searchParams.get('is_current'), 'eq.true');
      assert.equal(url.searchParams.get('revoked_at'), 'is.null'); assert.equal(url.searchParams.get('archived_at_utc'), 'is.null');
      assert.equal(url.searchParams.get('select').split(',').includes('work_date'), false);
      return { rows: rows.slice(offset, offset + 100), total: rows.length };
    }
    if (table === 'timesheets_financials') { financialCalls++; assert.equal(url.searchParams.get('is_current'), 'eq.true'); }
    return { rows: [], total: 0 };
  } });
  assert.equal(timesheetCalls, 6); assert.equal(financialCalls, 11); assert.equal(boundary.max_ts_date, '2026-10-07');
});

test('failed reads, missing/changing counts and duplicate/truncated pages refuse the save proof', async () => {
  for (const readPage of [async () => { throw new Error('SQL/source error'); }, async () => ({ rows: [] }),
    async () => ({ rows: [], total: 1 }), async () => ({ rows: [row(), row()], total: 2 })]) {
    await assert.rejects(readContractWorkedDateBoundary({ contractId, baseUrl: 'https://source.invalid', readPage }),
      error => error.code === 'CONTRACT_WORKED_DATES_UNAVAILABLE');
  }
  let calls = 0;
  await assert.rejects(readContractWorkedDateBoundary({ contractId, baseUrl: 'https://source.invalid',
    async readPage() { return { rows: [row({ timesheet_id: id(++calls + 100) })], total: calls === 1 ? 2 : 3 }; } }),
    error => error.code === 'CONTRACT_WORKED_DATES_UNAVAILABLE');
});

test('strict source transport rejects even HTTP 200 malformed/nullable JSON instead of silently returning no rows', async () => {
  const previous = globalThis.fetch;
  try {
    for (const payload of ['not-json', 'null', '{}']) {
      globalThis.fetch = async () => new Response(payload, { headers: { 'content-range': '*/0' } });
      await assert.rejects(readContractWorkedDatePage('https://source.invalid', { headers: {} }), error => error.code === 'CONTRACT_WORKED_DATES_UNAVAILABLE');
    }
    globalThis.fetch = async () => Response.json([], { headers: { 'content-range': '*/0' } });
    assert.deepEqual(await readContractWorkedDatePage('https://source.invalid', { headers: {} }), { rows: [], total: 0 });
  } finally { globalThis.fetch = previous; }
});

const source = (await readFile(new URL('../../broker/src/index.js', import.meta.url), 'utf8')).replace(/\r\n/g, '\n');
function section(start, end) { return source.slice(source.indexOf(start), source.indexOf(end, source.indexOf(start))); }

test('actual PATCH and PUT refuse a boundary violation or source failure before every mutation, with auth first', async () => {
  const patch = section('async function handleContractsUpdate', '// Lightweight checker');
  const put = section('async function handleContractsReplace', 'async function handleContractsDuplicate');
  const names = ['requireUser', 'parseJSONBody', 'sbGetOne', 'contractHasProgressedTimesheetEvidence', 'withCORS',
    'unauthorized', 'notFound', 'badRequest', 'serverError', 'enc', 'clampBool', 'toYmd', 'contractWorkedDateBoundary',
    'contractWorkedDateRefusal', 'normaliseAdditionalRates', 'nowIso', 'normalizeContractQueryEmailOverride', 'sbHeaders'];
  for (const [handlerSource, handlerName, method] of [[patch, 'handleContractsUpdate', 'PATCH'], [put, 'handleContractsReplace', 'PUT']]) {
    let mutations = 0;
    const previous = globalThis.fetch; globalThis.fetch = async () => { mutations++; throw new Error('Allowed mutation reached'); };
    const boundary = protectedContractWorkedDates([row()], [], [], contractId);
    let failSource = false; let authorized = true;
    const deps = { requireUser: async () => authorized ? { role: 'admin' } : null, parseJSONBody: req => req.json(),
      sbGetOne: async (_env, target) => String(target).includes('/contracts?') ? {
        id: contractId, start_date: '2026-10-01', end_date: '2026-10-11', pay_method_snapshot: 'PAYE',
        week_ending_weekday_snapshot: 0, overrideclientsettings: false, rates_json: {}, mileage_pay_rate: 0, mileage_charge_rate: 0
      } : String(target).includes('/timesheets?') ? { timesheet_id: id(2) } : null,
      contractHasProgressedTimesheetEvidence: async () => false, withCORS: (_env, _req, result) => result,
      unauthorized: () => ({ status: 401 }), notFound: message => ({ status: 404, message }),
      badRequest: message => ({ status: 400, message }), serverError: message => ({ status: 500, message }),
      enc: encodeURIComponent, clampBool: (value, fallback) => typeof value === 'boolean' ? value : fallback,
      toYmd: value => value || null, contractWorkedDateBoundary: async () => {
        if (failSource) throw new Error('Could not verify the recorded work dates. Please try again before saving.'); return boundary;
      }, contractWorkedDateRefusal, normaliseAdditionalRates: value => value || null, nowIso: () => '2026-10-13T00:00:00Z',
      normalizeContractQueryEmailOverride: () => ({ send_ts_queries_to_different_email: false, ts_queries_alt_email_address: null }), sbHeaders: () => ({}) };
    const handler = new Function(...names, `${handlerSource}; return ${handlerName};`)(...names.map(name => deps[name]));
    const request = () => new Request('https://source.invalid/contracts', { method, body: JSON.stringify({
      start_date: '2026-10-01', end_date: '2026-10-06', client_id: id(4), pay_method_snapshot: 'PAYE',
      week_ending_weekday_snapshot: 0, rates_json: {} }) });
    try {
      const refusal = await handler({}, request(), contractId);
      assert.equal(refusal.status, 400); assert.match(refusal.message, /07\/10\/2026/);
      failSource = true;
      assert.equal((await handler({}, request(), contractId)).status, 500);
      authorized = false;
      assert.equal((await handler({}, request(), contractId)).status, 401);
      assert.equal(mutations, 0);
      authorized = true; failSource = false;
      const allowed = new Request('https://source.invalid/contracts', { method, body: JSON.stringify({
        start_date: '2026-10-01', end_date: '2026-10-08', client_id: id(4), pay_method_snapshot: 'PAYE',
        week_ending_weekday_snapshot: 0, rates_json: {} }) });
      await assert.rejects(handler({}, allowed, contractId), /Allowed mutation reached/);
      assert.equal(mutations, 1, 'an allowed mid-week end reaches the existing writer');
    } finally { globalThis.fetch = previous; }
  }
});

test('existing boundary-check response names actual violating dates and never a Timesheet week edge', async () => {
  const body = section('async function handleContractsCheckTimesheetBoundary', '// === NEW: strict full-replace');
  const boundary = protectedContractWorkedDates([row()], [financial()], [], contractId);
  const names = ['requireUser', 'parseJSONBody', 'withCORS', 'unauthorized', 'badRequest', 'serverError',
    'toYmd', 'contractWorkedDateBoundary', 'sbFetch', 'enc', 'ok'];
  const deps = { requireUser: async () => ({ role: 'admin' }), parseJSONBody: req => req.json(),
    withCORS: (_env, _req, result) => result, unauthorized: () => ({ status: 401 }), badRequest: message => ({ status: 400, message }),
    serverError: message => ({ status: 500, message }), toYmd: value => value,
    contractWorkedDateBoundary: async () => boundary, sbFetch: async () => ({ rows: [{ id: id(4), name: 'Synthetic Client' }] }),
    enc: encodeURIComponent, ok: value => value };
  const handler = new Function(...names, `${body}; return handleContractsCheckTimesheetBoundary;`)(...names.map(name => deps[name]));
  const response = await handler({ SUPABASE_URL: 'https://source.invalid' }, new Request('https://source.invalid/contracts/check', {
    method: 'POST', body: JSON.stringify({ contract_id: contractId, start_date: '2026-10-01', end_date: '2026-10-06' })
  }));
  assert.equal(response.ok, false); assert.equal(response.max_ts_date, '2026-10-07');
  assert.deepEqual(response.violations, [{ timesheet_id: id(2), date: '2026-10-07', status: 'STORED', client_id: id(4), client_name: 'Synthetic Client' }]);
});

test('both normalizers reuse the checked worked bounds and clone-and-extend stays byte-identical to base', () => {
  for (const handler of [section('async function handleContractsUpdate', '// Lightweight checker'),
    section('async function handleContractsReplace', 'async function handleContractsDuplicate')]) {
    assert.equal((handler.match(/await contractWorkedDateBoundary/g) || []).length, 1);
    assert.match(handler, /const \{ min_ts_date, max_ts_date \} = protectedWorkBoundary/);
    assert.doesNotMatch(handler, /select=work_date,status/);
  }
  const original = execFileSync('git', ['show', 'HEAD:broker/src/index.js'], { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 }).replace(/\r\n/g, '\n');
  const clone = text => { const from = text.indexOf('async function handleContractsCloneAndExtend');
    return text.slice(from, text.indexOf('\nasync function ', from + 1)); };
  assert.equal(clone(source), clone(original));
});
