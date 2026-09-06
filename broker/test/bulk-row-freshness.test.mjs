import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildBulkRowFreshnessRequest,
  buildBulkRowResolutionAttempts,
  handleBulkRowFreshnessRequest,
  resolveBulkRowFreshness
} from '../src/bulk-row-freshness.js';

const TS1 = '11111111-1111-4111-8111-111111111111';
const CW1 = '22222222-2222-4222-8222-222222222222';

function request(overrides = {}) {
  return {
    surface: 'bulk_process',
    classification: null,
    row_key: `timesheet:${TS1}`,
    previous_row_key: null,
    timesheet_id: TS1,
    contract_week_id: CW1,
    known_signature: 'sig-1',
    current_section: 'unprocessed_eligible',
    actor_user_id: '33333333-3333-4333-8333-333333333333',
    filters: { show_weekly_manual: 'true', show_daily_manual: 'true' },
    ...overrides
  };
}

function row(overrides = {}) {
  return {
    row_key: `timesheet:${TS1}`,
    timesheet_id: TS1,
    contract_week_id: CW1,
    backend_row_signature: 'sig-1',
    bulk_process_bucket: 'UNPROCESSED',
    ...overrides
  };
}

function rpcScenario({ patch = row(), processRows = null, authoriseRows = null, patchSequence = null, inspectDataset = null } = {}) {
  const calls = [];
  let patchIndex = 0;
  const rpc = async (fn, args, options) => {
    calls.push({ fn, args, options });
    if (fn === 'bulk_timesheet_row_patch_v1') {
      const value = patchSequence ? patchSequence[Math.min(patchIndex++, patchSequence.length - 1)] : patch;
      return value && Object.keys(value).length ? [{ row_json: value }] : [];
    }
    if (typeof inspectDataset === 'function') inspectDataset(fn, args.p_filters);
    if (fn === 'bulk_process_dataset_v1') {
      const source = processRows || { unprocessed_rows: [patch], processed_rows: [] };
      return source;
    }
    if (fn === 'bulk_authorise_dataset_v1') return { rows: authoriseRows || [] };
    throw new Error(`Unexpected RPC ${fn}`);
  };
  return { rpc, calls };
}

test('unchanged current row is a cache-safe CURRENT result', async () => {
  const scenario = rpcScenario();
  const result = await resolveBulkRowFreshness(request(), scenario.rpc);
  assert.equal(result.outcome, 'CURRENT');
  assert.equal(result.changed, false);
  assert.equal(result.target_section, 'unprocessed_eligible');
  assert.equal(scenario.calls.length, 2);
});

test('changed signature in the same section is CURRENT + changed', async () => {
  const changed = row({ backend_row_signature: 'sig-2', evidence_badges: [{ kind: 'TIMESHEET' }] });
  const result = await resolveBulkRowFreshness(request(), rpcScenario({ patch: changed }).rpc);
  assert.equal(result.outcome, 'CURRENT');
  assert.equal(result.changed, true);
  assert.deepEqual(result.row.evidence_badges, [{ kind: 'TIMESHEET' }]);
});

for (const transition of [
  { name: 'unprocessed to processed', from: 'unprocessed_eligible', to: 'processed_eligible', bucket: 'PROCESSED' },
  { name: 'processed to unprocessed', from: 'processed_eligible', to: 'unprocessed_eligible', bucket: 'UNPROCESSED' }
]) {
  test(`Bulk Process reconciles ${transition.name}`, async () => {
    const changed = row({ backend_row_signature: 'sig-2', bulk_process_bucket: transition.bucket });
    const processRows = transition.to === 'processed_eligible'
      ? { unprocessed_rows: [], processed_rows: [changed] }
      : { unprocessed_rows: [changed], processed_rows: [] };
    const result = await resolveBulkRowFreshness(request({ current_section: transition.from }), rpcScenario({ patch: changed, processRows }).rpc);
    assert.equal(result.outcome, 'MOVED');
    assert.equal(result.target_section, transition.to);
  });
}

test('duplicate-expense review row remains visible without becoming bulk-authorisable', async () => {
  const reviewRow = row({
    bulk_authorise_section: 'processed_review_required',
    bulk_authorise_block_code: 'DUPLICATE_EXPENSE_REVIEW_REQUIRED',
    can_bulk_authorise: false,
    can_bulk_unauthorise: false
  });
  const result = await resolveBulkRowFreshness(
    request({
      surface: 'bulk_authorise',
      classification: 'TIMESHEETS',
      current_section: 'processed_review_required'
    }),
    rpcScenario({ patch: reviewRow, authoriseRows: [reviewRow] }).rpc
  );
  assert.equal(result.outcome, 'CURRENT');
  assert.equal(result.changed, false);
  assert.equal(result.eligible_for_surface, true);
  assert.equal(result.target_section, 'processed_review_required');
  assert.equal(result.row.can_bulk_authorise, false);
  assert.equal(result.row.bulk_authorise_block_code, 'DUPLICATE_EXPENSE_REVIEW_REQUIRED');
});

for (const transition of [
  { name: 'unauthorised to authorised', from: 'processed_eligible', to: 'authorised_eligible', authorised: true },
  { name: 'authorised to unauthorised', from: 'authorised_eligible', to: 'processed_eligible', authorised: false }
]) {
  test(`Bulk Authorise reconciles ${transition.name}`, async () => {
    const changed = row({ backend_row_signature: 'sig-2', is_authorised: transition.authorised, bulk_authorise_section: transition.to });
    const result = await resolveBulkRowFreshness(
      request({ surface: 'bulk_authorise', classification: 'TIMESHEETS', current_section: transition.from }),
      rpcScenario({ patch: changed, authoriseRows: [changed] }).rpc
    );
    assert.equal(result.outcome, 'MOVED');
    assert.equal(result.target_section, transition.to);
  });
}

test('authorised row is removed from Bulk Process', async () => {
  const changed = row({ is_authorised: true, processing_status: 'AUTHORISED' });
  const result = await resolveBulkRowFreshness(request(), rpcScenario({ patch: changed, processRows: { unprocessed_rows: [], processed_rows: [] } }).rpc);
  assert.equal(result.outcome, 'REMOVED');
  assert.equal(result.reason, 'AUTHORISED_OUT_OF_BULK_PROCESS');
});

test('unprocessed row is removed from Bulk Authorise', async () => {
  const changed = row({ bulk_process_bucket: 'UNPROCESSED', processing_status: 'UNPROCESSED' });
  const result = await resolveBulkRowFreshness(
    request({ surface: 'bulk_authorise', classification: 'TIMESHEETS', current_section: 'processed_eligible' }),
    rpcScenario({ patch: changed, authoriseRows: [] }).rpc
  );
  assert.equal(result.outcome, 'REMOVED');
  assert.equal(result.reason, 'UNPROCESSED_OUT_OF_BULK_AUTHORISE');
});

test('archived row is removed with an authoritative reason', async () => {
  const changed = row({ is_archived: true, stage: 'ARCHIVED' });
  const result = await resolveBulkRowFreshness(request(), rpcScenario({ patch: changed, processRows: { unprocessed_rows: [], processed_rows: [] } }).rpc);
  assert.equal(result.outcome, 'REMOVED');
  assert.equal(result.reason, 'ARCHIVED');
});

test('row is DELETED only after every stable identity attempt fails', async () => {
  const scenario = rpcScenario({ patchSequence: [{}, {}, {}] });
  const result = await resolveBulkRowFreshness(request(), scenario.rpc);
  assert.equal(result.outcome, 'DELETED');
  assert.equal(result.reason, 'ROW_NOT_FOUND');
  assert.equal(scenario.calls.length, 3);
});

for (const identityTransition of [
  { name: 'contract week to timesheet', old: `contract_week:${CW1}`, canonical: `timesheet:${TS1}` },
  { name: 'timesheet to contract week', old: `timesheet:${TS1}`, canonical: `contract_week:${CW1}` }
]) {
  test(`canonical identity transition: ${identityTransition.name}`, async () => {
    const changed = row({ row_key: identityTransition.canonical, backend_row_signature: 'sig-2' });
    const result = await resolveBulkRowFreshness(
      request({ row_key: identityTransition.old }),
      rpcScenario({ patch: changed, processRows: { unprocessed_rows: [changed], processed_rows: [] } }).rpc
    );
    assert.equal(result.row_key, identityTransition.canonical);
    assert.equal(result.changed, true);
  });
}

test('stable contract-week fallback follows a stale row-key miss', async () => {
  const changed = row({ row_key: `timesheet:${TS1}` });
  const scenario = rpcScenario({ patchSequence: [{}, changed] });
  const result = await resolveBulkRowFreshness(request({ row_key: `contract_week:${CW1}` }), scenario.rpc);
  assert.equal(result.outcome, 'CURRENT');
  assert.equal(result.resolution_attempts, 2);
  assert.equal(scenario.calls[1].args.p_filters.contract_week_id, CW1);
});

test('request parser rejects invalid surface and missing identifiers', () => {
  assert.throws(() => buildBulkRowFreshnessRequest('https://test.invalid/api?surface=other&row_key=x'), /surface/);
  assert.throws(() => buildBulkRowFreshnessRequest('https://test.invalid/api?surface=bulk_process'), /identity/);
  assert.throws(() => buildBulkRowFreshnessRequest('https://test.invalid/api?surface=bulk_authorise&row_key=x'), /classification/);
});

test('resolution attempts do not conjunct stale row keys with stable IDs', () => {
  const attempts = buildBulkRowResolutionAttempts(request());
  assert.deepEqual(attempts[0].row_keys, [`timesheet:${TS1}`]);
  assert.equal(attempts[0].contract_week_id, undefined);
  assert.equal(attempts[1].contract_week_id, CW1);
  assert.equal(attempts[2].timesheet_id, TS1);
});

test('existing invoiced/unissued filter is forwarded and cannot expand policy', async () => {
  let observed = null;
  const scenario = rpcScenario({
    patch: row({ bulk_authorise_section: 'authorised_eligible', is_authorised: true, bulk_process_bucket: 'PROCESSED', processing_status: 'AUTHORISED' }),
    authoriseRows: [],
    inspectDataset: (_fn, filters) => { observed = filters; }
  });
  const result = await resolveBulkRowFreshness(
    request({
      surface: 'bulk_authorise',
      classification: 'TIMESHEETS',
      current_section: 'authorised_eligible',
      filters: { show_authorised_invoiced_unissued: 'false' }
    }),
    scenario.rpc
  );
  assert.equal(observed.show_authorised_invoiced_unissued, 'false');
  assert.equal(result.outcome, 'REMOVED');
  assert.equal(result.reason, 'FILTERED_OUT');
});

test('ordinary import-derived row cannot be introduced into Bulk Process', async () => {
  const importRow = row({ is_import_authoritative: true, adjustment_source: 'IMPORT_DERIVED' });
  const result = await resolveBulkRowFreshness(
    request(),
    rpcScenario({ patch: importRow, processRows: { unprocessed_rows: [], processed_rows: [] } }).rpc
  );
  assert.equal(result.outcome, 'REMOVED');
});

test('HTTP handler rejects unauthenticated callers before an RPC', async () => {
  let called = false;
  const response = await handleBulkRowFreshnessRequest({
    requireUser: async () => null,
    sbRpc: async () => { called = true; },
    withCORS: (_env, _req, value) => value,
    unauthorized: () => new Response('{}', { status: 401 })
  }, {}, new Request(`https://test.invalid/api?surface=bulk_process&row_key=timesheet:${TS1}`));
  assert.equal(response.status, 401);
  assert.equal(called, false);
});

test('HTTP handler returns bounded timeout failure and permits retry', async () => {
  const error = Object.assign(new Error('timeout'), { status: 408 });
  const response = await handleBulkRowFreshnessRequest({
    requireUser: async () => ({ id: 'admin-id' }),
    sbRpc: async () => { throw error; },
    withCORS: (_env, _req, value) => value,
    unauthorized: () => new Response('{}', { status: 401 })
  }, {}, new Request(`https://test.invalid/api?surface=bulk_process&row_key=timesheet:${TS1}`));
  const payload = await response.json();
  assert.equal(response.status, 504);
  assert.equal(payload.error_code, 'FRESHNESS_TIMEOUT');
  assert.equal(payload.soft_failure, true);
});
