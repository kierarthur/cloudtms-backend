import assert from 'node:assert/strict';
import test from 'node:test';
import { weeklySourceCoverageWarnings, assertWeeklySourceCoverageFloor } from '../../broker/src/weekly-source/coverage-warnings.mjs';
import { adaptWeeklySourceParserOutput } from '../../broker/src/weekly-source/upload-staging-adapter.mjs';
import { createWeeklySourceUploadPublicationOwner } from '../../broker/src/weekly-source/upload-publication-owner.mjs';
import { dispatchWeeklySourceRequest } from '../../broker/src/weekly-source/routes.js';

const id = n => `10000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const parsed = { ok: true, profileId: 'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1', rows: [{ date: '2026-10-06' }, { date: '2026-10-07' }] };
const context = { ok: true, agency_id: id(1), source_group_id: id(2), source_cycle_id: id(3), client_id: id(4),
  source_family: 'ROSTER', client_name: 'Synthetic Client', authority_scope_version: 1,
  coverage_context: { contract: 'WEEKLY_SOURCE_UPLOAD_COVERAGE_CONTEXT_V1', coverage_floor_local_date: '2026-09-28',
    oldest_protected_work_date: '2026-10-05', last_completed_contract_week: '2026-10-04',
    lookback_warning_enabled: true, has_current_final_source: true } };

test('three independent warnings are amber, one sentence and keep Upload anyway', () => {
  const result = weeklySourceCoverageWarnings(parsed, { coverage: { start_local_date: '2026-10-06', end_local_date: '2026-10-11' } }, context);
  assert.deepEqual(result.map(row => row.code), ['WEEKLY_SOURCE_LOOKBACK_AFTER_PROTECTED',
    'WEEKLY_SOURCE_COVERAGE_AFTER_COMPLETED_WEEK', 'WEEKLY_SOURCE_WIDER_COVERAGE_CANCELLATIONS']);
  for (const row of result) { assert.equal(row.tone, 'warning'); assert.equal(row.action_label, 'Upload anyway'); }
  assert.match(result[0].message, /05\/10\/2026/); assert.match(result[1].message, /04\/10\/2026/);
});

test('lookback off/equality and no saved source remove only their own warnings', () => {
  const facts = { ...context.coverage_context, lookback_warning_enabled: false, has_current_final_source: false,
    last_completed_contract_week: '2026-10-11' };
  assert.deepEqual(weeklySourceCoverageWarnings(parsed, {}, { coverage_context: facts }), []);
  assert.deepEqual(weeklySourceCoverageWarnings({ ...parsed, rows: [{ date: '2026-10-05' }] }, {},
    { coverage_context: { ...facts, lookback_warning_enabled: true } }), []);
});

test('legacy/off/null rule set, NHSP, report and Mode A retain no new warning or floor behavior', () => {
  for (const legacy of [{}, { coverage_context: null }]) {
    assert.deepEqual(weeklySourceCoverageWarnings(parsed, {}, legacy), []);
    assert.doesNotThrow(() => assertWeeklySourceCoverageFloor(parsed, { coverage: { start_local_date: '2020-01-01', end_local_date: '2026-10-11' } }, legacy));
  }
  for (const profileId of ['NHSP_PREFINAL_RELEASED_V1', 'NHSP_FINAL_BACKING_V1', 'ACACIUM_SELF_BILL_REPORT_V1', 'MODE_A']) {
    assert.deepEqual(weeklySourceCoverageWarnings({ ...parsed, profileId }, {}, context), []);
    assert.doesNotThrow(() => assertWeeklySourceCoverageFloor({ ...parsed, profileId }, {}, { coverage_context: {} }));
  }
});

test('go-live floor checks the complete confirmation in the real adapter before census or staging', () => {
  const body = { coverage: { start_local_date: '2026-09-27', end_local_date: '2026-10-11' } };
  assert.throws(() => adaptWeeklySourceParserOutput(parsed, body, context), error =>
    error.code === 'WEEKLY_SOURCE_COVERAGE_BEFORE_RULE_SET' && /28\/09\/2026/.test(error.message));
  assert.doesNotThrow(() => assertWeeklySourceCoverageFloor(parsed, { coverage: { start_local_date: '2026-09-28', end_local_date: '2026-10-11' } }, context));
});

test('malformed declared saved facts cannot silently suppress warnings or a floor', () => {
  for (const facts of [{}, { ...context.coverage_context, coverage_floor_local_date: '2026-02-30' },
    { ...context.coverage_context, has_current_final_source: null }, { ...context.coverage_context, oldest_protected_work_date: 'wrong' }]) {
    assert.throws(() => weeklySourceCoverageWarnings(parsed, {}, { coverage_context: facts }),
      { code: 'WEEKLY_SOURCE_COVERAGE_CONTEXT_INVALID' });
  }
});

test('actual preview owner supplies server warnings; the checking preview remains accepted', async () => {
  const calls = [];
  const owner = createWeeklySourceUploadPublicationOwner({ dataRpc: async (name, args) => {
    calls.push({ name, args });
    if (name === 'weekly_source_client_cycle_resolve_atomic_v1') return { ok: true, source_cycle_id: id(3) };
    return context;
  } });
  const result = await owner.previewUpload({ body: { profile_id: parsed.profileId }, bytes: new Uint8Array([1]),
    actor: { id: id(5) }, parseWeeklySourceFile: async () => parsed });
  assert.equal(result.parsed.ok, true);
  assert.equal(result.coverage_warnings[0].action_label, 'Upload anyway');
  assert.deepEqual(result.accept_context.coverage_context, context.coverage_context);
  assert.equal(calls.some(call => /stage|seal|prepare|finalise/.test(call.name)), false);
});

test('actual accept owner rejects a before-floor file before the first stage mutation', async () => {
  const calls = [];
  const owner = createWeeklySourceUploadPublicationOwner({ dataRpc: async name => { calls.push(name); return context; } });
  await assert.rejects(owner.acceptUpload({ body: { profile_id: parsed.profileId,
    coverage: { start_local_date: '2026-09-27', end_local_date: '2026-10-11' } },
    bytes: new Uint8Array([1]), actor: { id: id(5) }, parseWeeklySourceFile: async () => parsed }),
    { code: 'WEEKLY_SOURCE_COVERAGE_BEFORE_RULE_SET' });
  assert.deepEqual(calls, ['weekly_source_upload_context_v1']);
});

test('actual preview route preserves warnings and legacy omission without browser role inference', async () => {
  for (const warnings of [undefined, weeklySourceCoverageWarnings(parsed, {}, context)]) {
    const response = await dispatchWeeklySourceRequest(new Request('https://source.invalid/api/weekly-source/v1/uploads/preview', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ file_key: 'synthetic.xlsx' })
    }), {}, {}, { requireUser: async () => ({ id: id(5), role: 'admin' }), loadFileBytes: async () => new Uint8Array([1]),
      previewUpload: async () => ({ parsed, accept_context: {}, ...(warnings === undefined ? {} : { coverage_warnings: warnings }) }) });
    assert.equal(response.status, 200); const result = await response.json();
    if (warnings === undefined) assert.equal(Object.hasOwn(result, 'coverage_warnings'), false);
    else assert.deepEqual(result.coverage_warnings, warnings);
  }
});

test('forced Prepare returns exact S12 user wording; unrelated SQL errors stay unchanged', async () => {
  const error = Object.assign(new Error('RPC weekly_source_import_prepare_atomic_v1 failed 400: technical envelope'), {
    status: 400, json: { code: '55000', message: 'WEEKLY_SOURCE_FINAL_FILE_UPLOADED_TOO_EARLY',
      details: 'The final file can be uploaded from Thursday 00:00 (08/10/2026).' } });
  const response = await dispatchWeeklySourceRequest(new Request('https://source.invalid/api/weekly-source/v1/commands', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ action: 'PREPARE_FINALISATION', payload: { upload_id: id(6) } })
  }), {}, {}, { requireUser: async () => ({ id: id(5), role: 'admin' }), rpc: async () => { throw error; } });
  assert.equal(response.status, 409);
  assert.deepEqual(await response.json(), { ok: false, error_code: error.json.message, message: error.json.details });
  error.json.message = 'SOME_EXISTING_ERROR';
  const unchanged = await dispatchWeeklySourceRequest(new Request('https://source.invalid/api/weekly-source/v1/commands', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ action: 'PREPARE_FINALISATION', payload: { upload_id: id(6) } })
  }), {}, {}, { requireUser: async () => ({ id: id(5), role: 'admin' }), rpc: async () => { throw error; } });
  assert.equal(unchanged.status, 400); assert.equal((await unchanged.json()).message, error.message);
});
