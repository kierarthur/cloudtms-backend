import assert from 'node:assert/strict';
import test from 'node:test';

import { invoiceAsyncHttpInternals } from '../../broker/src/invoice-async-http.js';

import {
  WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_CONTRACT,
  WEEKLY_SOURCE_INVOICE_BATCH_CANDIDATES_CONTRACT,
  WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_CONTRACT,
  admitWeeklySourceInvoiceBatch,
  loadWeeklySourceInvoiceBatchCandidates,
  mergeWeeklySourceCandidateEnvelope,
  normaliseWeeklySourceInvoiceBatchSelectionContract,
  preflightWeeklySourceInvoiceBatch,
  splitWeeklySourceCandidateRequest,
  weeklySourceAdmissionInvoiceId,
  weeklySourceInvoiceBatchInternals,
} from '../../broker/src/weekly-source/invoice-batch-integration.mjs';

const HASH_A = 'a'.repeat(64);
const HASH_B = 'b'.repeat(64);
const MANIFEST_A = '00000000-0000-4000-8000-000000000101';
const MANIFEST_B = '00000000-0000-4000-8000-000000000102';
const CYCLE_A = '00000000-0000-4000-8000-000000000201';
const CYCLE_B = '00000000-0000-4000-8000-000000000202';
const CLIENT_ID = '00000000-0000-4000-8000-000000000301';
const ACTOR_ID = '00000000-0000-4000-8000-000000000401';
const INVOICE_A = '00000000-0000-4000-8000-000000000501';
const INVOICE_B = '00000000-0000-4000-8000-000000000502';

const selectionKey = manifestId => `weekly-source-manifest:${manifestId}`;

function sourceRow({
  manifestId = MANIFEST_A,
  cycleId = CYCLE_A,
  revision = HASH_A,
  weekEnding = '2026-09-06',
  reportNumber = 'BR-001',
} = {}) {
  return {
    selection_key: selectionKey(manifestId),
    source_revision: revision,
    source_kind: 'WEEKLY_FINAL_SOURCE',
    invoice_stream: 'WEEKLY_FINAL_SOURCE',
    selectable: true,
    row_status: 'READY',
    generation_state: 'NOT_GENERATED',
    week_ending_date: weekEnding,
    client_id: CLIENT_ID,
    client_name: 'Example Trust',
    candidate_ids: ['00000000-0000-4000-8000-000000000601'],
    candidate_names: ['Example Worker'],
    candidate_name: 'Example Worker',
    report_number: reportNumber,
    movement_count: 1,
    total_ex_vat: 100,
    vat_amount: 20,
    total_inc_vat: 120,
    currency: 'GBP',
    action_blocker_codes: [],
    informational_codes: [],
    released_after_dispute: false,
    client_manifest_id: manifestId,
    source_cycle_id: cycleId,
  };
}

function sourceEnvelope({ rows = [sourceRow()], mode = 'PAGE', refs = [] } = {}) {
  const selectedTotal = mode === 'CONFIRM' ? refs.length : rows.length;
  return {
    contract_version: WEEKLY_SOURCE_INVOICE_BATCH_CANDIDATES_CONTRACT,
    mode,
    snapshot_hash: HASH_B,
    rows,
    page: { total_count: rows.length, returned_count: rows.length },
    selection_summary: {
      exact: true,
      eligible_total: rows.length,
      selected_total: selectedTotal,
      blocked_total: 0,
    },
    selected_manifest_refs: refs,
  };
}

function ordinaryEnvelope() {
  return {
    contract_version: 'INVOICE_BATCH_CANDIDATES_V2',
    action: 'GENERATE',
    mode: 'PAGE',
    rows: [{ selection_key: 'ordinary-row', selectable: true }],
    page: { total_count: 1, returned_count: 1 },
    selection_summary: {
      exact: true,
      eligible_total: 1,
      selected_total: 1,
      blocked_total: 0,
    },
    totals: { total: 1 },
  };
}

function query(overrides = {}) {
  return {
    mode: 'PAGE',
    page_size: 100,
    filters: {},
    sort: {},
    selection: {
      contract_version: 'INVOICE_BATCH_SELECTION_V2',
      mode: 'IMPLICIT_ALL',
      default_selected: true,
      rules: [],
    },
    ...overrides,
  };
}

function signedSnapshot() {
  return {
    contract_version: 'INVOICE_BATCH_SNAPSHOT_V2',
    action: 'GENERATE',
    at_utc: '2026-09-16T10:00:00.000Z',
    revision: '1',
    expires_at_utc: '2026-09-16T10:30:00.000Z',
    key_id: 'weekly-source-test',
    token: 'signed-test-snapshot',
  };
}

function selectionRoot() {
  return {
    contract_version: 'INVOICE_BATCH_SELECTION_ROOT_V2',
    query: {
      contract_version: 'INVOICE_BATCH_QUERY_V2',
      action: 'GENERATE',
      mode: 'PAGE',
      snapshot: signedSnapshot(),
      page_size: 100,
      cursor: null,
      filters: {},
      sort: {},
      selection: query().selection,
    },
    selection: query().selection,
  };
}

function ordinarySummary(selectedTotal) {
  return {
    contract_version: 'INVOICE_BATCH_CANDIDATES_V2',
    action: 'GENERATE',
    mode: 'SUMMARY',
    rows: [],
    page: {
      page_size: 0,
      returned_count: 0,
      total_count: selectedTotal,
      has_more: false,
    },
    totals: {
      filtered_total: selectedTotal,
      display_total: selectedTotal,
      eligible_total: selectedTotal,
      selected_total: selectedTotal,
      excluded_total: 0,
      blocked_total: 0,
    },
    selection_summary: {
      eligible_total: selectedTotal,
      selected_total: selectedTotal,
      excluded_total: 0,
      blocked_total: 0,
      exact: true,
    },
    group_selection: [],
    facets: {},
    filter_hash: HASH_A,
    query_hash: HASH_A,
    selection_hash: HASH_A,
  };
}

function confirmRequest() {
  return new Request('https://example.test/api/invoices/batch-generate/confirm', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'idempotency-key': 'weekly-source-confirm-test',
    },
    body: JSON.stringify({
      selection_contract: selectionRoot(),
      weekly_source_selection_contract: {
        contract_version: WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_CONTRACT,
        snapshot_hash: HASH_B,
      },
    }),
  });
}

test('ordinary candidate request is preserved exactly when the optional source hash is removed', () => {
  const body = {
    contract_version: 'INVOICE_BATCH_QUERY_V2',
    action: 'GENERATE',
    mode: 'PAGE',
    page_size: 100,
    filters: { client_ids: [CLIENT_ID] },
    sort: { sort_key: 'CLIENT_NAME', sort_direction: 'ASC' },
    selection: { contract_version: 'INVOICE_BATCH_SELECTION_V2' },
    weekly_source_snapshot_hash: HASH_A,
  };
  const expectedOrdinary = structuredClone(body);
  delete expectedOrdinary.weekly_source_snapshot_hash;

  const split = splitWeeklySourceCandidateRequest(body, 'GENERATE');
  assert.deepEqual(split.ordinaryBody, expectedOrdinary);
  assert.equal(split.weeklySourceRequested, true);
  assert.equal(split.weeklySourceSnapshotHash, HASH_A);
  assert.equal(
    splitWeeklySourceCandidateRequest(expectedOrdinary, 'GENERATE').weeklySourceRequested,
    false,
  );
  assert.throws(
    () => splitWeeklySourceCandidateRequest(body, 'ISSUE'),
    /BATCH_QUERY_UNKNOWN_FIELD/,
  );
});

test('source PAGE always requests the complete bounded manifest list for continuous scrolling', () => {
  const request = weeklySourceInvoiceBatchInternals.sourceRpcRequest(query(), null);
  assert.equal(request.mode, 'PAGE');
  assert.equal(request.page_size, 5000);
  assert.deepEqual(request.filters, {});
  assert.deepEqual(request.selection.rules, []);
});

test('source candidates validate strictly and merge without changing ordinary rows', async () => {
  const calls = [];
  const deps = {
    async rpc(name, args) {
      calls.push({ name, args });
      return sourceEnvelope();
    },
  };
  const source = await loadWeeklySourceInvoiceBatchCandidates(deps, query(), null);
  assert.equal(source.rows.length, 1);
  assert.equal(calls[0].name, 'weekly_source_invoice_batch_candidates_v1');
  assert.equal(calls[0].args.p_request.page_size, 5000);

  const ordinary = ordinaryEnvelope();
  const merged = mergeWeeklySourceCandidateEnvelope(ordinary, source);
  assert.deepEqual(merged.rows, ordinary.rows);
  assert.equal(merged.selection_summary.selected_total, 2);
  assert.equal(merged.selection_summary.ordinary_selected_total, 1);
  assert.equal(merged.selection_summary.weekly_source_selected_total, 1);

  await assert.rejects(
    loadWeeklySourceInvoiceBatchCandidates({
      async rpc() {
        return {
          ...sourceEnvelope(),
          page: { total_count: 2, returned_count: 1 },
        };
      },
    }, query(), null),
    /WEEKLY_SOURCE_INVOICE_BATCH_CONTRACT_INVALID/,
    'a truncated final-source list must fail closed instead of silently omitting a week',
  );
  assert.deepEqual(merged.weekly_source.rows, source.rows);

  const invalid = sourceEnvelope({ rows: [{ ...sourceRow(), source_kind: 'OTHER' }] });
  await assert.rejects(
    loadWeeklySourceInvoiceBatchCandidates({ rpc: async () => invalid }, query(), null),
    /WEEKLY_SOURCE_INVOICE_BATCH_CONTRACT_INVALID/,
  );
});

test('preflight carries exact manifest identities and admission returns an outcome for every row', async () => {
  const refs = [
    {
      selection_key: selectionKey(MANIFEST_A),
      client_manifest_id: MANIFEST_A,
      expected_manifest_hash: HASH_A,
      client_id: CLIENT_ID,
      source_cycle_id: CYCLE_A,
      finalisation_week_ending: '2026-09-06',
      report_number: 'BR-001',
    },
    {
      selection_key: selectionKey(MANIFEST_B),
      client_manifest_id: MANIFEST_B,
      expected_manifest_hash: HASH_B,
      client_id: CLIENT_ID,
      source_cycle_id: CYCLE_B,
      finalisation_week_ending: '2026-09-13',
      report_number: 'BR-002',
    },
  ];
  const calls = [];
  const deps = {
    async rpc(name, args) {
      calls.push({ name, args });
      if (name === 'weekly_source_invoice_batch_candidates_v1') {
        return sourceEnvelope({ mode: 'CONFIRM', rows: [], refs });
      }
      return {
        ok: true,
        contract_version: WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_CONTRACT,
        atomic: true,
        selected_count: 2,
        per_manifest_results: [
          { ...refs[0], invoice_ids: [INVOICE_A], status: 'ADMITTED', idempotent: false },
          { ...refs[1], invoice_ids: [INVOICE_B], status: 'ADMITTED', idempotent: false },
        ],
      };
    },
  };
  const contract = normaliseWeeklySourceInvoiceBatchSelectionContract({
    contract_version: WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_CONTRACT,
    snapshot_hash: HASH_B,
  });
  const preflight = await preflightWeeklySourceInvoiceBatch(deps, query(), contract);
  const admission = await admitWeeklySourceInvoiceBatch(
    deps,
    ACTOR_ID,
    'weekly-source-batch-test',
    preflight,
  );

  assert.equal(calls[0].name, 'weekly_source_invoice_batch_candidates_v1');
  assert.equal(calls[0].args.p_request.mode, 'CONFIRM');
  assert.equal(calls[1].name, 'weekly_source_invoice_batch_admit_atomic_v1');
  assert.deepEqual(calls[1].args.p_request.selected_manifests, refs);
  assert.equal(admission.per_manifest_results.length, 2);
  assert.equal(weeklySourceAdmissionInvoiceId(admission), null,
    'two report weeks must never be mistaken for one generate-and-view invoice');
});

test('admission rejects missing, duplicate or malformed per-row outcomes', async () => {
  const preflight = {
    snapshot_hash: HASH_A,
    selected_manifest_refs: [
      {
        selection_key: selectionKey(MANIFEST_A),
        client_manifest_id: MANIFEST_A,
      },
    ],
  };
  await assert.rejects(
    admitWeeklySourceInvoiceBatch({
      rpc: async () => ({
        ok: true,
        contract_version: WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_CONTRACT,
        atomic: true,
        selected_count: 0,
        per_manifest_results: [],
      }),
    }, ACTOR_ID, 'bad-admission', preflight),
    /WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_CONTRACT_INVALID/,
  );
});

test('mixed confirmation performs both read-only preflights before any source or ordinary write', async () => {
  const calls = [];
  await assert.rejects(
    invoiceAsyncHttpInternals.handleBatchGenerateConfirm(
      {},
      confirmRequest(),
      {},
      { id: ACTOR_ID },
      {
        async rpc(name) {
          calls.push(name);
          if (name === 'invoice_batch_generate_candidates') return ordinarySummary(1);
          if (name === 'weekly_source_invoice_batch_candidates_v1') {
            throw Object.assign(new Error('BATCH_SOURCE_CHANGED'), { code: 'BATCH_SOURCE_CHANGED' });
          }
          throw new Error(`unexpected write ${name}`);
        },
      },
    ),
    /BATCH_SOURCE_CHANGED/,
  );
  assert.deepEqual(calls, [
    'invoice_batch_generate_candidates',
    'weekly_source_invoice_batch_candidates_v1',
  ]);
});

test('two selected source weeks return two exact row outcomes and never start the ordinary route', async () => {
  const refs = [
    {
      selection_key: selectionKey(MANIFEST_A), client_manifest_id: MANIFEST_A,
      expected_manifest_hash: HASH_A, client_id: CLIENT_ID, source_cycle_id: CYCLE_A,
      finalisation_week_ending: '2026-09-06', report_number: 'BR-001',
    },
    {
      selection_key: selectionKey(MANIFEST_B), client_manifest_id: MANIFEST_B,
      expected_manifest_hash: HASH_B, client_id: CLIENT_ID, source_cycle_id: CYCLE_B,
      finalisation_week_ending: '2026-09-13', report_number: 'BR-002',
    },
  ];
  const calls = [];
  const response = await invoiceAsyncHttpInternals.handleBatchGenerateConfirm(
    {},
    confirmRequest(),
    {},
    { id: ACTOR_ID },
    {
      async rpc(name) {
        calls.push(name);
        if (name === 'invoice_batch_generate_candidates') return ordinarySummary(0);
        if (name === 'weekly_source_invoice_batch_candidates_v1') {
          return sourceEnvelope({ mode: 'CONFIRM', rows: [], refs });
        }
        if (name === 'weekly_source_invoice_batch_admit_atomic_v1') {
          return {
            ok: true,
            contract_version: WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_CONTRACT,
            atomic: true,
            selected_count: 2,
            per_manifest_results: [
              { ...refs[0], invoice_ids: [INVOICE_A], status: 'ADMITTED' },
              { ...refs[1], invoice_ids: [INVOICE_B], status: 'ADMITTED' },
            ],
          };
        }
        throw new Error(`unexpected ordinary write ${name}`);
      },
    },
  );
  const body = await response.json();
  assert.equal(response.status, 200);
  assert.equal(body.weekly_source_per_row_results.length, 2);
  assert.deepEqual(
    body.weekly_source_per_row_results.map(row => row.invoice_ids[0]),
    [INVOICE_A, INVOICE_B],
  );
  assert.equal(calls.includes('invoice_operation_start_batch'), false);
});

test('mixed ordinary start failure reports committed source rows as an explicit partial outcome', async () => {
  const ref = {
    selection_key: selectionKey(MANIFEST_A), client_manifest_id: MANIFEST_A,
    expected_manifest_hash: HASH_A, client_id: CLIENT_ID, source_cycle_id: CYCLE_A,
    finalisation_week_ending: '2026-09-06', report_number: 'BR-001',
  };
  const calls = [];
  const response = await invoiceAsyncHttpInternals.handleBatchGenerateConfirm(
    {},
    confirmRequest(),
    {},
    { id: ACTOR_ID },
    {
      async rpc(name) {
        calls.push(name);
        if (name === 'invoice_batch_generate_candidates') return ordinarySummary(1);
        if (name === 'weekly_source_invoice_batch_candidates_v1') {
          return sourceEnvelope({ mode: 'CONFIRM', rows: [], refs: [ref] });
        }
        if (name === 'weekly_source_invoice_batch_admit_atomic_v1') {
          return {
            ok: true,
            contract_version: WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_CONTRACT,
            atomic: true,
            selected_count: 1,
            per_manifest_results: [
              { ...ref, invoice_ids: [INVOICE_A], status: 'ADMITTED' },
            ],
          };
        }
        if (name === 'invoice_operation_start_batch') {
          throw Object.assign(new Error('ORDINARY_START_FAILED'), { code: 'ORDINARY_START_FAILED' });
        }
        throw new Error(`unexpected RPC ${name}`);
      },
    },
  );
  const body = await response.json();
  assert.equal(response.status, 207);
  assert.equal(body.ok, true);
  assert.equal(body.partial, true);
  assert.equal(body.error, 'ORDINARY_START_FAILED');
  assert.equal(body.weekly_source_per_row_results[0].invoice_ids[0], INVOICE_A);
  assert.ok(calls.indexOf('weekly_source_invoice_batch_admit_atomic_v1')
    < calls.indexOf('invoice_operation_start_batch'));
});
