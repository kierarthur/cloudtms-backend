import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import {
  createInvoiceDocumentAccessToken,
  verifyInvoiceDocumentAccessToken
} from '../broker/src/invoice-document-access.js';
import {
  getInvoiceQueueRuntimeConfig,
  validateQueueRuntimeConfiguration,
  invoiceQueueRuntimeInternals,
  processInvoiceDatabaseChunksBatch,
  processInvoiceDocumentChunksBatch,
  drainInvoiceOperations,
  runInvoiceReconciliationCycle,
  runAutoInvoiceCycleAsync
} from '../broker/src/invoice-queue-runtime.js';
import {
  buildAttachmentIndexHtml,
  buildProfessionalInvoiceHtml,
  buildElectronicTimesheetHtml,
  buildHealthRosterSupportHtml,
  buildNhspSupportHtml,
  buildHigherRateSupportHtml,
  escapeInvoiceDocumentHtml
} from '../broker/src/invoice-document-templates.js';
import {
  parseInvoiceAsyncAllowedUserIds,
  isInvoiceAsyncUserAllowed,
  signInvoiceDrainRequest,
  verifyInvoiceDrainSignature
} from '../broker/src/invoice-queue-security.js';
import { invoiceAsyncHttpInternals } from '../broker/src/invoice-async-http.js';
import { handleInvoiceAsyncHttpRequest } from '../broker/src/invoice-async-http.js';
import {
  buildMergeReceipt,
  buildPhysicalReceipt,
  flattenLeafInputReceipts,
  verifyMergeReceiptTree
} from '../invoice-document-processor/src/receipt-contract.js';

const V8_ACTOR_ID = '00000000-0000-4000-8000-000000000010';
const V8_FUNCTION_MANIFEST = '7b6afc07613387a8dec0d1bfa5375344dc017f3369940ee987c5857b85ee7830';
const V8_CURSOR_SECRET = 'test-session-secret-with-more-than-thirty-two-characters';

function v8DatabaseContract(overrides = {}) {
  return {
    contract_version: 'INVOICE_ASYNC_DB_V2',
    ready: true,
    candidate_query_contract: 'INVOICE_BATCH_QUERY_V2',
    candidate_response_contract: 'INVOICE_BATCH_CANDIDATES_V2',
    selection_contract: 'INVOICE_BATCH_SELECTION_V2',
    selection_root_contract: 'INVOICE_BATCH_SELECTION_ROOT_V2',
    progress_contract: 'INVOICE_BATCH_PROGRESS_V2',
    function_hash_manifest: V8_FUNCTION_MANIFEST,
    indexes_ready: true,
    snapshot_signing_ready: true,
    operation_control_idempotency_ready: true,
    missing_function_count: 0,
    private_exposure_count: 0,
    forbidden_dependency_count: 0,
    public_candidate_dependency_count: 0,
    legacy_runtime_exposure_count: 0,
    ...overrides
  };
}

function v8ProcessorBinding() {
  return {
    async fetch() {
      return new Response(JSON.stringify({
        ok: true,
        processor_policy_version: 'INVOICE_PROCESSOR_LIMITS_V4',
        processor_implementation_version: 'cloudtms-invoice-document-worker-v6',
        supported_media_types: ['application/pdf', 'image/jpeg', 'image/png'],
        receipt_contracts: {
          object: 'ACTUAL_BYTES_OBJECT_RECEIPT_V3',
          logical: 'LOGICAL_SOURCE_RECEIPT_V3',
          merge: 'ACTUAL_BYTES_MERGE_RECEIPT_V3',
          root: 'DOCUMENT_ROOT_RECEIPT_V3',
          ordered_input: 'ACTUAL_ORDERED_INPUT_V1'
        },
        container_ready: true,
        native_tools_ready: true
      }), { status: 200, headers: { 'content-type': 'application/json' } });
    }
  };
}

function v8Environment(overrides = {}) {
  return {
    INVOICE_ASYNC_PIPELINE_ENABLED: 'true',
    INVOICE_ASYNC_SCHEDULED_ENABLED: 'false',
    INVOICE_DOCUMENT_PROCESSOR_ENABLED: 'true',
    INVOICE_ASYNC_ALLOWED_USER_IDS: V8_ACTOR_ID,
    INVOICE_ACTOR_USER_ID: V8_ACTOR_ID,
    INVOICE_ASYNC_EXPECTED_DB_CONTRACT: 'INVOICE_ASYNC_DB_V2',
    INVOICE_ASYNC_EXPECTED_FUNCTION_MANIFEST: V8_FUNCTION_MANIFEST,
    INVOICE_ASYNC_BUILD_ID: 'invoice-async-v8-test',
    INVOICE_DOCUMENT_PROCESSOR_SECRET: 'test-processor-secret-with-more-than-thirty-two-characters',
    INVOICE_DOCUMENT_ACCESS_SECRET: 'test-document-secret-with-more-than-thirty-two-characters',
    INVOICE_QUEUE_DISPATCH_SECRET: 'test-dispatch-secret-with-more-than-thirty-two-characters',
    SESSION_TOKEN_SECRET: V8_CURSOR_SECRET,
    SUPABASE_URL: `https://supabase-${crypto.randomUUID()}.test`,
    SUPABASE_SERVICE_ROLE_KEY: 'test-only',
    INVOICE_DOCUMENT_PROCESSOR: v8ProcessorBinding(),
    INVOICE_QUEUE_DISPATCHER: { fetch: async () => new Response('{}', { status: 202 }) },
    R2: {},
    ...overrides
  };
}

function v8Rpc(handler = async name => {
  throw new Error(`Unexpected RPC ${name}`);
}, contract = v8DatabaseContract()) {
  return async (name, args) => {
    if (name === 'invoice_async_contract_get_v2') return contract;
    return handler(name, args);
  };
}

function v8Actor() {
  return { id: V8_ACTOR_ID, role: 'admin', active: true };
}

function v8Selection() {
  return {
    contract_version: 'INVOICE_BATCH_SELECTION_V2',
    mode: 'IMPLICIT_ALL',
    default_selected: true,
    rules: []
  };
}

function v8Snapshot(action = 'GENERATE', overrides = {}) {
  return {
    contract_version: 'INVOICE_BATCH_SNAPSHOT_V2',
    action,
    at_utc: '2026-07-27T12:00:00.000Z',
    revision: '1844',
    expires_at_utc: '2026-07-27T12:30:00.000Z',
    key_id: 'test-v8-key',
    token: 'signed-database-snapshot-token',
    ...overrides
  };
}

function v8Query(action = 'GENERATE', overrides = {}) {
  return {
    contract_version: 'INVOICE_BATCH_QUERY_V2',
    action,
    mode: 'PAGE',
    snapshot: v8Snapshot(action),
    page_size: 100,
    cursor: null,
    filters: {},
    sort: {},
    selection: v8Selection(),
    ...overrides
  };
}

test('runtime configuration is disabled by default and clamps every limit', () => {
  const config = getInvoiceQueueRuntimeConfig({
    INVOICE_USER_NUDGE_DATABASE_CLAIM_LIMIT: '999',
    INVOICE_USER_NUDGE_DOCUMENT_CLAIM_LIMIT: '-5',
    INVOICE_QUEUE_LEASE_SECONDS: '2',
    INVOICE_SCHEDULED_DRAIN_DEADLINE_MS: '999999',
    INVOICE_ASSET_CONCURRENCY: '20'
  });
  assert.equal(config.enabled, false);
  assert.equal(config.userNudgeDatabaseClaimLimit, 20);
  assert.equal(config.userNudgeDocumentClaimLimit, 1);
  assert.equal(config.leaseSeconds, 15);
  assert.equal(config.scheduledDrainDeadlineMs, 25000);
  assert.equal(config.assetConcurrency, 8);
  assert.equal(config.maximumContinuationDepth, 4);
});

test('unified outbox cursor is signed, filter-bound and preserves independent source positions', async () => {
  const env = { SESSION_TOKEN_SECRET: 'test-session-secret-with-more-than-thirty-two-characters' };
  const payload = {
    v: 1,
    snapshot_at_utc: '2026-07-24T12:00:00.000Z',
    filters_hash: 'a'.repeat(64),
    sort: 'created_at_utc_desc_channel_rank_id_desc',
    legacy: {
      created_at_utc: '2026-07-24T11:59:00.000Z',
      id: '00000000-0000-4000-8000-000000000001'
    },
    invoice: {
      created_at_utc: '2026-07-24T11:58:00.000Z',
      id: '00000000-0000-4000-8000-000000000002'
    },
    totals: { legacy: 12, invoice: 8 }
  };
  const token = await invoiceAsyncHttpInternals.encodeUnifiedOutboxCursor(env, payload);
  assert.deepEqual(await invoiceAsyncHttpInternals.decodeUnifiedOutboxCursor(env, token), payload);
  const [payloadPart, signaturePart] = token.split('.');
  const tampered = `${payloadPart}.${signaturePart.startsWith('a') ? 'b' : 'a'}${signaturePart.slice(1)}`;
  await assert.rejects(
    () => invoiceAsyncHttpInternals.decodeUnifiedOutboxCursor(env, tampered),
    /OUTBOX_CURSOR_INVALID/
  );
});

test('database batch advances all claims through one RPC call', async () => {
  const calls = [];
  const claims = Array.from({ length: 100 }, (_, index) => ({
    chunk_id: `00000000-0000-4000-8000-${String(index).padStart(12, '0')}`,
    lease_token: `10000000-0000-4000-8000-${String(index).padStart(12, '0')}`,
    fence_token: index + 1,
    operation_control_version: 1
  }));
  const result = await processInvoiceDatabaseChunksBatch({}, claims, {
    rpc: async (name, args) => {
      calls.push({ name, args });
      return claims.map(claim => ({ chunk_id: claim.chunk_id, accepted: true }));
    }
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].name, 'invoice_operation_advance_batch');
  assert.equal(calls[0].args.p_claims.length, 100);
  assert.equal(result.advanced, 100);
});

test('queue lane selection keeps database and document work separate', () => {
  const database = invoiceQueueRuntimeInternals.lanesToChunkTypes(['DATABASE']);
  assert.ok(database.database.includes('GENERATION_GROUP'));
  assert.equal(database.document.length, 0);
  const document = invoiceQueueRuntimeInternals.lanesToChunkTypes(['DOCUMENT']);
  assert.ok(document.document.includes('PDF_MERGE'));
  assert.equal(document.database.length, 0);
});

test('browser receipt hashes use PostgreSQL jsonb key ordering', () => {
  assert.equal(
    invoiceQueueRuntimeInternals.postgresJsonbText({
      very_long_key: 3,
      b: 2,
      a: 1
    }),
    '{"a": 1, "b": 2, "very_long_key": 3}'
  );
});

test('document access token is signed, expires, and binds its purpose', async () => {
  const secret = 'test-only-secret-with-more-than-thirty-two-characters';
  const token = await createInvoiceDocumentAccessToken(secret, {
    sub: 'actor',
    entity_type: 'INVOICE',
    entity_id: '00000000-0000-4000-8000-000000000001',
    document_version_id: '00000000-0000-4000-8000-000000000002',
    r2_key: 'invoice-documents/version/final.pdf',
    purpose: 'DOWNLOAD'
  }, { nowMs: 1_000_000, ttlSeconds: 60 });
  const valid = await verifyInvoiceDocumentAccessToken(secret, token, {
    nowMs: 1_010_000,
    expectedPurpose: 'DOWNLOAD'
  });
  assert.equal(valid.ok, true);
  assert.equal(valid.claims.r2_key, undefined);
  const wrongPurpose = await verifyInvoiceDocumentAccessToken(secret, token, {
    nowMs: 1_010_000,
    expectedPurpose: 'PREVIEW'
  });
  assert.equal(wrongPurpose.code, 'INVOICE_DOCUMENT_TOKEN_PURPOSE_MISMATCH');
  const expired = await verifyInvoiceDocumentAccessToken(secret, token, {
    nowMs: 1_100_000
  });
  assert.equal(expired.code, 'INVOICE_DOCUMENT_TOKEN_EXPIRED');
});

test('canonical command payload preserves canonical members and rejects malformed IDs', () => {
  const request = new Request('https://example.test/api/invoices', {
    method: 'POST',
    headers: { 'idempotency-key': 'command-1' }
  });
  const command = invoiceAsyncHttpInternals.generationCommandFromBody(request, {
    timesheet_ids: [
      '00000000-0000-4000-8000-000000000002',
      '00000000-0000-4000-8000-000000000001',
      '00000000-0000-4000-8000-000000000001'
    ],
    canonical_source_members: [{ source_type: 'TIMESHEET', source_id: 'member-1' }],
    target_invoice_week: '2026-07-20'
  });
  assert.deepEqual(command.source_ids, [
    '00000000-0000-4000-8000-000000000001',
    '00000000-0000-4000-8000-000000000002'
  ]);
  assert.equal(command.canonical_source_members.length, 1);
  assert.throws(() => invoiceAsyncHttpInternals.generationCommandFromBody(request, {
    timesheet_ids: ['not-a-uuid']
  }), /VALID_UUID_ARRAY_REQUIRED/);
});

test('invoice HTML is deterministic and escapes all mutable presentation values', () => {
  assert.equal(escapeInvoiceDocumentHtml('<script>&"'), '&lt;script&gt;&amp;&quot;');
  const model = {
    schema_version: 'INVOICE_RENDER_MODEL_V1',
    purpose: 'DRAFT_PREVIEW',
    document_type: 'INVOICE',
    invoice_number: 'INV-1<script>',
    supplier: {
      legal_name: 'Supplier & Co',
      registered_address: ['1 Supplier Street'],
      vat_registration_number: 'GB123456789',
      trading_name: 'Supplier Trading',
      company_registration_number: '12345678',
      contact_email: 'accounts@example.test'
    },
    customer: {
      legal_name: 'Customer "A"',
      billing_address: ['2 Customer Road'],
      account_reference: 'CLI-TEST'
    },
    references: {},
    lines: [{
      row_key: 'line-1',
      source_invoice_line_id: '00000000-0000-4000-8000-000000000001',
      source_key: 'source-1',
      description: '<unsafe>',
      worker: 'Worker One',
      reference: 'REF-1',
      unit: 'HOUR',
      quantity: '1.0000',
      unit_price: '10.0000',
      net_amount: '10.00',
      vat_rate: '20.00',
      vat_amount: '2.00',
      gross_amount: '12.00',
      display_order: 1
    }],
    vat_breakdown: [{ rate: '20.00', net_amount: '10.00', vat_amount: '2.00', gross_amount: '12.00' }],
    totals: { net: 10, vat: 2, gross: 12 },
    payment: { terms_text: '30 days from invoice date' },
    credit_note: { is_credit_note: false },
    self_bill: { is_self_bill: false },
    legal_wording: [],
    template_version: 'invoice-professional-v1'
  };
  const first = buildProfessionalInvoiceHtml(model);
  const second = buildProfessionalInvoiceHtml(model);
  assert.equal(first, second);
  assert.ok(first.includes('INV-1&lt;script&gt;'));
  assert.ok(!first.includes('<unsafe>'));
  assert.ok(first.includes('Rate / item type'));
  assert.ok(first.includes('Candidate / worker'));
  assert.ok(first.includes('Worker One'));
  assert.ok(first.includes('Cost per unit'));
  assert.ok(first.includes('30 days from invoice date'));
  assert.ok(!first.includes('Supplier Trading'));
  assert.ok(!first.includes('Company no:'));
  assert.ok(!first.includes('Account: CLI-TEST'));
});

test('consolidated invoice hides the top worker field but keeps workers in line rows', () => {
  const model = {
    schema_version: 'INVOICE_RENDER_MODEL_V1',
    purpose: 'DRAFT_PREVIEW',
    document_type: 'INVOICE',
    invoice_number: 'INV-CONSOLIDATED',
    currency: 'GBP',
    supplier: { legal_name: 'Supplier', registered_address: ['1 Supplier Street'] },
    customer: { legal_name: 'Customer', billing_address: ['2 Customer Road'] },
    references: {},
    candidate_summary: 'Multiple workers',
    lines: ['Worker One', 'Worker Two'].map((worker, index) => ({
      row_key: `line-${index + 1}`,
      source_invoice_line_id: `00000000-0000-4000-8000-00000000000${index + 1}`,
      source_key: `source-${index + 1}`,
      description: index ? 'Additional rate' : 'Day',
      worker,
      reference: `REF-${index + 1}`,
      unit: 'hours',
      quantity: '1',
      unit_price: '10',
      net_amount: '10',
      vat_rate: '0',
      vat_amount: '0',
      gross_amount: '10',
      display_order: index + 1
    })),
    vat_breakdown: [{ rate: '0', net_amount: '20', vat_amount: '0', gross_amount: '20' }],
    totals: { net: 20, vat: 0, gross: 20 },
    payment: { terms_text: '30 days from invoice date' },
    credit_note: { is_credit_note: false },
    self_bill: { is_self_bill: false },
    legal_wording: [],
    template_version: 'invoice-professional-v1'
  };
  const html = buildProfessionalInvoiceHtml(model);
  assert.ok(html.includes('Worker One'));
  assert.ok(html.includes('Worker Two'));
  assert.ok(html.includes('Additional rate'));
  assert.doesNotMatch(html, /<div class="label">Candidate \/ worker<\/div>/);
});

test('frozen presentation identity is recomputed and pay-side fields are rejected', async () => {
  const model = {
    schema_version: 'INVOICE_RENDER_MODEL_V1',
    purpose: 'DRAFT_PREVIEW',
    document_type: 'INVOICE',
    invoice_number: 'INV-HASH-1',
    currency: 'GBP',
    supplier: {
      legal_name: 'Supplier',
      registered_address: ['1 Supplier Street'],
      vat_registration_number: 'GB123456789'
    },
    customer: { legal_name: 'Customer', billing_address: ['2 Customer Road'] },
    references: {},
    branding: { logo: {} },
    lines: [{
      row_key: 'line-1',
      source_invoice_line_id: '00000000-0000-4000-8000-000000000001',
      source_key: 'source-1',
      description: 'Day hours',
      reference: '',
      unit: 'hours',
      quantity: '1.0000',
      unit_price: '10.0000',
      net_amount: '10.00',
      vat_rate: '20.00',
      vat_amount: '2.00',
      gross_amount: '12.00',
      display_order: 1
    }],
    vat_breakdown: [{ rate: '20.00', net_amount: '10.00', vat_amount: '2.00', gross_amount: '12.00' }],
    totals: {
      net: '10.00',
      vat: '2.00',
      gross: '12.00',
      amount_paid: '0',
      amount_credited: '0',
      amount_outstanding: '12.00'
    },
    payment: { terms_text: '30 days from invoice date' },
    credit_note: { is_credit_note: false },
    self_bill: { is_self_bill: false },
    legal_wording: [],
    template_version: 'invoice-professional-v1'
  };
  const canonical = invoiceQueueRuntimeInternals.postgresJsonbText(model);
  const expectedHash = createHash('sha256').update(canonical).digest('hex');
  const verified = await invoiceQueueRuntimeInternals.verifyFrozenPresentationModelHash(
    'INVOICE_CORE',
    model,
    {
      presentation_model_schema_version: 'INVOICE_RENDER_MODEL_V1',
      presentation_model_hash: expectedHash,
      template_version: 'invoice-professional-v1'
    }
  );
  assert.equal(verified.presentation_model_hash, expectedHash);
  await assert.rejects(
    () => invoiceQueueRuntimeInternals.verifyFrozenPresentationModelHash(
      'INVOICE_CORE',
      { ...model, totals: { ...model.totals, gross: 13 } },
      {
        presentation_model_schema_version: 'INVOICE_RENDER_MODEL_V1',
        presentation_model_hash: expectedHash,
        template_version: 'invoice-professional-v1'
      }
    ),
    /INVOICE_PRESENTATION_LINE_TOTAL_MISMATCH/
  );
  assert.throws(
    () => buildProfessionalInvoiceHtml({
      ...model,
      lines: [{ ...model.lines[0], pay_rate: 8 }]
    }),
    /INVOICE_PRESENTATION_PAY_SIDE_FIELD_FORBIDDEN/
  );
});

test('attachment index renders one logical row with physical page totals', () => {
  const html = buildAttachmentIndexHtml({
    display_rows: [{
      row_id: 'attachment-1',
      document_type: 'Manual timesheet',
      start_page: 5,
      page_count: 4,
      physical_part_count: 2
    }]
  });
  assert.ok(html.includes('Manual timesheet'));
  assert.ok(html.includes('5'));
  assert.ok(html.includes('4'));
});

test('attachment pagination correlates the DB logical source key', () => {
  const layout = {
    expected_logical_attachment_count: 1,
    expected_physical_page_count: 5,
    pagination_stream: [
      { input_type: 'INVOICE_CORE', page_count: 1 },
      { input_type: 'ATTACHMENT_INDEX' },
      { input_type: 'SECTION_SEPARATOR', page_count: 1 },
      {
        input_type: 'TIMESHEET',
        logical_source_key: 'timesheet:source-1',
        physical_part_id: 'timesheet:source-1:1',
        page_count: 2,
        document_type: 'TIMESHEET'
      }
    ]
  };
  const rows = invoiceQueueRuntimeInternals.deriveAttachmentDisplayMap(
    layout,
    1
  );
  assert.deepEqual(rows, [{
    row_id: 'timesheet:source-1',
    attachment_number: undefined,
    worker: undefined,
    week_or_date: undefined,
    document_type: 'TIMESHEET',
    evidence_description: undefined,
    reference: undefined,
    start_page: 4,
    page_count: 2
  }]);
  assert.throws(
    () => invoiceQueueRuntimeInternals.deriveAttachmentDisplayMap({
      pagination_stream: [{ input_type: 'TIMESHEET', page_count: 1 }]
    }, 0),
    /ATTACHMENT_PAGINATION_LOGICAL_ROW_MISSING/
  );
});

test('retired synchronous generation route returns 410 and starts no work', async () => {
  let rpcCalled = false;
  const request = new Request('https://example.test/api/invoices', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'idempotency-key': 'request-1'
    },
    body: JSON.stringify({
      timesheet_ids: ['00000000-0000-4000-8000-000000000001']
    })
  });
  const response = await handleInvoiceAsyncHttpRequest(request, v8Environment(), {}, {
    requireUser: async () => v8Actor(),
    rpc: async () => {
      rpcCalled = true;
      throw new Error('Retired route must not reach the database');
    }
  });
  assert.equal(response.status, 410);
  assert.equal((await response.json()).error.code, 'INVOICE_LEGACY_ROUTE_RETIRED');
  assert.equal(rpcCalled, false);
});

test('attachment pagination correlates the DB logical source key', () => {
  const layout = {
    expected_logical_attachment_count: 1,
    expected_physical_page_count: 5,
    pagination_stream: [
      { input_type: 'INVOICE_CORE', page_count: 1 },
      { input_type: 'ATTACHMENT_INDEX' },
      { input_type: 'SECTION_SEPARATOR', page_count: 1 },
      {
        input_type: 'TIMESHEET',
        logical_source_key: 'timesheet:source-1',
        physical_part_id: 'timesheet:source-1:1',
        page_count: 2,
        document_type: 'TIMESHEET'
      }
    ]
  };
  const rows = invoiceQueueRuntimeInternals.deriveAttachmentDisplayMap(
    layout,
    1
  );
  assert.deepEqual(rows, [{
    row_id: 'timesheet:source-1',
    attachment_number: undefined,
    worker: undefined,
    week_or_date: undefined,
    document_type: 'TIMESHEET',
    evidence_description: undefined,
    reference: undefined,
    start_page: 4,
    page_count: 2
  }]);
  assert.throws(
    () => invoiceQueueRuntimeInternals.deriveAttachmentDisplayMap({
      pagination_stream: [{ input_type: 'TIMESHEET', page_count: 1 }]
    }, 0),
    /ATTACHMENT_PAGINATION_LOGICAL_ROW_MISSING/
  );
});

test('document view returns 200 only for an exact READY version', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async request => {
    const url = new URL(
      typeof request === 'string'
        ? request
        : (request instanceof URL ? request.href : request.url)
    );
    assert.equal(url.pathname, '/rest/v1/invoice_document_versions');
    assert.equal(url.searchParams.get('entity_type'), 'eq.INVOICE');
    assert.equal(url.searchParams.get('purpose'), 'eq.DRAFT_PREVIEW');
    return new Response(JSON.stringify([{
      id: '00000000-0000-4000-8000-000000000020',
      entity_type: 'INVOICE',
      entity_id: '00000000-0000-4000-8000-000000000021',
      purpose: 'DRAFT_PREVIEW',
      r2_key: 'immutable/preview.pdf',
      sha256: 'a'.repeat(64),
      size_bytes: 1024,
      page_count: 2,
      status: 'READY'
    }]), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  try {
    const response = await handleInvoiceAsyncHttpRequest(
      new Request(
        'https://example.test/api/invoices/00000000-0000-4000-8000-000000000021/render',
        {
          method: 'POST',
          headers: { 'idempotency-key': 'view-ready-command' },
          body: '{}'
        }
      ),
      v8Environment(),
      {},
      {
        requireUser: async () => v8Actor(),
        rpc: v8Rpc(async name => {
          if (name === 'invoice_detail_get') return { invoice: { id: '00000000-0000-4000-8000-000000000021', status: 'DRAFT' } };
          assert.equal(name, 'invoice_operation_start_batch');
          return [{
            accepted: true,
            status: 'READY',
            reused_ready: true,
            document_version_id: '00000000-0000-4000-8000-000000000020'
          }];
        })
      }
    );
    const body = await response.json();
    assert.equal(response.status, 200, JSON.stringify(body));
    assert.equal(body.contract_version, 'INVOICE_VIEWER_V2');
    assert.equal(body.viewer_state, 'READY');
    assert.equal(body.document_version.sha256, 'a'.repeat(64));
    assert.equal(body.document_version.r2_key, undefined);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('unfiltered unified outbox includes bounded invoice operation rows', async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async request => {
    const url = new URL(
      typeof request === 'string'
        ? request
        : (request instanceof URL ? request.href : request.url)
    );
    assert.ok(['/rest/v1/invoice_operations', '/rest/v1/v_outbox_unified'].includes(url.pathname));
    if (url.pathname === '/rest/v1/invoice_operations') {
      assert.equal(url.searchParams.get('operation_type'), 'neq.OPERATION_CONTROL_REQUEST');
    }
    const rows = url.pathname === '/rest/v1/invoice_operations'
      ? [{
          id: '00000000-0000-4000-8000-000000000030',
          operation_type: 'BUILD_DOCUMENT',
          entity_type: 'INVOICE',
          entity_id: '00000000-0000-4000-8000-000000000031',
          status: 'RUNNING',
          phase: 'RENDERING',
          created_at_utc: '2026-07-24T12:00:00.000Z',
          run_after_utc: null
        }]
      : [{
          channel: 'EMAIL',
          outbox_id: '00000000-0000-4000-8000-000000000032',
          status: 'QUEUED',
          created_at_utc: '2026-07-24T11:00:00.000Z'
        }];
    return new Response(JSON.stringify(rows), {
      status: 200,
      headers: {
        'content-type': 'application/json',
        'content-range': '0-0/1'
      }
    });
  };
  try {
    const response = await handleInvoiceAsyncHttpRequest(
      new Request('https://example.test/api/outbox?limit=50'),
      v8Environment(),
      {},
      {
        requireUser: async () => v8Actor(),
        rpc: v8Rpc(async () => {
          throw new Error('Unified cursor listing must not use the offset RPC');
        })
      }
    );
    const body = await response.json();
    assert.equal(response.status, 200, JSON.stringify(body));
    assert.equal(body.total_count, 2);
    assert.equal(body.items[0].channel, 'INVOICE');
    assert.equal(body.items[1].channel, 'EMAIL');
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('native receipt contract derives physical receipts from actual bytes metadata', async () => {
  const descriptors = [
    {
      descriptor: {
        input_chunk_id: 'leaf-1',
        input_order: 1,
        r2_key: 'immutable/leaf-1.pdf',
        sha256: 'a'.repeat(64),
        page_count: 2,
        size_bytes: 120,
        logical_source_key: 'timesheet:1',
        logical_manifest_ordinal: 7,
        physical_part_no: 1
      }
    },
    {
      descriptor: {
        input_chunk_id: 'leaf-2',
        input_order: 2,
        r2_key: 'immutable/leaf-2.pdf',
        sha256: 'b'.repeat(64),
        page_count: 3,
        size_bytes: 180,
        logical_source_key: 'timesheet:1',
        logical_manifest_ordinal: 7,
        physical_part_no: 2
      }
    }
  ];
  const actualInputs = [
    { input_order: 1, r2_key: 'immutable/leaf-1.pdf', sha256: 'a'.repeat(64), page_count: 2, size_bytes: 120 },
    { input_order: 2, r2_key: 'immutable/leaf-2.pdf', sha256: 'b'.repeat(64), page_count: 3, size_bytes: 180 }
  ];
  const receipt = await buildMergeReceipt(
    {},
    {
      processor_policy_version: 'INVOICE_PROCESSOR_LIMITS_V4',
      plan_generation: 1
    },
    descriptors,
    { actual_inputs: actualInputs, processor_version: 'test-native-v1' },
    { r2_key: 'immutable/merged.pdf', sha256: 'd'.repeat(64), size_bytes: 300, page_count: 5 }
  );
  assert.equal(receipt.input_receipts.length, 2);
  assert.equal(receipt.physical_receipts.length, 2);
  assert.equal(receipt.combined_logical_receipt_root.length, 64);
  assert.equal(receipt.combined_physical_receipt_root.length, 64);
  assert.equal(flattenLeafInputReceipts(receipt).length, 2);

  await assert.rejects(
    () => buildPhysicalReceipt(descriptors[0], {
      ...actualInputs[0],
      sha256: 'f'.repeat(64)
    }, 0),
    /INPUT_SHA256_MISMATCH/
  );
});

test('controlled cohort parsing is exact, deduplicated, and admin-only', () => {
  const id = '00000000-0000-4000-8000-000000000010';
  const parsed = parseInvoiceAsyncAllowedUserIds(` ${id},${id.toUpperCase()} `);
  assert.equal(parsed.ok, true);
  assert.deepEqual(parsed.ids, [id]);
  assert.equal(parseInvoiceAsyncAllowedUserIds('not-a-uuid').ok, false);
  assert.equal(isInvoiceAsyncUserAllowed({ INVOICE_ASYNC_PIPELINE_ENABLED: 'true', INVOICE_ASYNC_ALLOWED_USER_IDS: id }, { id, role: 'admin', active: true }).allowed, true);
  assert.equal(isInvoiceAsyncUserAllowed({ INVOICE_ASYNC_PIPELINE_ENABLED: 'true', INVOICE_ASYNC_ALLOWED_USER_IDS: id }, { id, role: 'user', active: true }).code, 'INVOICE_ASYNC_ADMIN_REQUIRED');
});

test('drain request signatures bind lane order, nonce, depth, and timestamp', async () => {
  const secret = 'test-dispatch-secret-with-at-least-thirty-two-characters';
  const payload = { timestamp: 123456, nonce: crypto.randomUUID(), depth: 2, lanes: ['MERGE', 'DATABASE'], priority_class: 'VIEW_NOW' };
  const signature = await signInvoiceDrainRequest(secret, payload);
  assert.equal(await verifyInvoiceDrainSignature(secret, { ...payload, lanes: ['DATABASE', 'MERGE'] }, signature), true);
  assert.equal(await verifyInvoiceDrainSignature(secret, { ...payload, depth: 3 }, signature), false);
});

test('candidate GET is rejected as method-not-allowed before any service-role read', async () => {
  const id = '00000000-0000-4000-8000-000000000010';
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices/batch-generate/candidates'),
    { INVOICE_ASYNC_PIPELINE_ENABLED: 'true', INVOICE_ASYNC_ALLOWED_USER_IDS: id },
    {},
    { requireUser: async () => ({ id, role: 'user', active: true }), rpc: async () => assert.fail('RPC must not be called') }
  );
  assert.equal(response.status, 405);
  assert.equal((await response.json()).error, 'BATCH_QUERY_POST_REQUIRED');
});

test('retired legacy batch generation never returns an accepted response', async () => {
  const id = '00000000-0000-4000-8000-000000000010';
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ timesheet_ids: ['00000000-0000-4000-8000-000000000011'] }) }),
    { INVOICE_ASYNC_PIPELINE_ENABLED: 'true', INVOICE_ASYNC_ALLOWED_USER_IDS: id },
    {},
    { requireUser: async () => ({ id, role: 'admin', active: true }), rpc: async () => [{ accepted: false, error: 'SOURCE_CHANGED' }] }
  );
  assert.equal(response.status, 410);
  const body = await response.json();
  assert.equal(body.error.code, 'INVOICE_LEGACY_ROUTE_RETIRED');
});

test('issued document view selects the exact FINAL_ISSUE version and never queues preview', async () => {
  const originalFetch = globalThis.fetch;
  const actor = '00000000-0000-4000-8000-000000000010';
  const invoice = '00000000-0000-4000-8000-000000000012';
  const version = '00000000-0000-4000-8000-000000000013';
  let started = false;
  globalThis.fetch = async request => {
    const url = new URL(typeof request === 'string' ? request : (request instanceof URL ? request.href : request.url));
    assert.equal(url.searchParams.get('purpose'), 'eq.FINAL_ISSUE');
    assert.equal(url.searchParams.get('id'), `eq.${version}`);
    return new Response(JSON.stringify([{ id: version, entity_type: 'INVOICE', entity_id: invoice, purpose: 'FINAL_ISSUE', status: 'READY', r2_key: 'immutable/final.pdf', sha256: 'e'.repeat(64), size_bytes: 1000, page_count: 3 }]), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  try {
    const response = await handleInvoiceAsyncHttpRequest(
      new Request(`https://example.test/api/invoices/${invoice}/render`, {
        method: 'POST',
        headers: { 'idempotency-key': 'view-final-command' },
        body: '{}'
      }),
      v8Environment({ INVOICE_ASYNC_ALLOWED_USER_IDS: actor }),
      {},
      { requireUser: async () => ({ id: actor, role: 'admin', active: true }), rpc: v8Rpc(async name => {
        if (name === 'invoice_detail_get') return { invoice: { id: invoice, status: 'ISSUED', issued_document_version_id: version } };
        if (name === 'invoice_operation_start_batch') started = true;
        return [];
      }) }
    );
    assert.equal(response.status, 200);
    const body = await response.json();
    assert.equal(body.viewer_state, 'READY');
    assert.equal(body.document_version.purpose, 'FINAL_ISSUE');
    assert.equal(body.document_version.r2_key, undefined);
    assert.equal(started, false);
  } finally { globalThis.fetch = originalFetch; }
});

test('professional source templates use explicit allowlisted fields', () => {
  const ts = buildElectronicTimesheetHtml({
    schema_version: 'TIMESHEET_RENDER_MODEL_V1',
    timesheet_id: '00000000-0000-4000-8000-000000000001',
    candidate: { id: 'worker-1', name: 'Worker' },
    client: { id: 'client-1', name: 'Client' },
    contract: { id: 'contract-1', reference: 'C-1' },
    work: { hospital: 'Hospital', site: 'Hospital', ward: 'Ward' },
    week_ending_date: '2026-07-26',
    submission_mode: 'ELECTRONIC',
    sheet_scope: 'DAILY',
    references: { whole: 'TS-1', day: [], segment: [] },
    additional_units: {},
    authorisation: {
      authorised: true,
      name: null,
      role: null,
      authorised_at_utc: '2026-07-24T16:00:00Z'
    },
    signatures: { candidate: {}, authoriser: {} },
    qr: { required: false, signed: false },
    daily_schedule_rows: [{ date: '2026-07-24', worked_start: '08:00', worked_end: '16:00', break_minutes: 30, hours: 7.5, display_order: 1 }],
    weekly_schedule_rows: [],
    template_version: 'timesheet-professional-v1'
  });
  assert.ok(ts.includes('Hospital'));
  assert.ok(ts.includes('7.5'));
  assert.ok(ts.includes('08:00 – 16:00'));
  assert.ok(ts.includes('Authorised: Yes · 24/07/2026'));
  assert.ok(!ts.includes('Additional units: </div>'));
  assert.ok(!ts.includes('Additional units: Additional units'));
  assert.ok(!ts.includes(' · by </div>'));
  const hr = buildHealthRosterSupportHtml({ schema_version: 'HEALTHROSTER_PRESENTATION_V1', rows: [{ worker: 'Worker', assignment: 'Assignment', shift_date: null, shift_times: null, site: null, ward: null, reference: null, units_hours: null, validation_state: null, source_identity: null, secret_future_key: 'must-not-render' }] });
  const nhsp = buildNhspSupportHtml({ schema_version: 'NHSP_PRESENTATION_V1', rows: [{ worker: 'Worker', nhsp_shift_id: 'SHIFT-1', booking_reference: null, site_ward: null, shift_date: null, shift_times: null, hours_units: null, source_identity: null, validation_state: null, secret_future_key: 'must-not-render' }] });
  const higher = buildHigherRateSupportHtml({ schema_version: 'HIGHER_RATE_PRESENTATION_V1', rows: [{ worker_source: 'Worker', shift_date: null, original_rate: null, applied_rate: '25.00', units: null, display_amount: null, reason: null, approval_identity: null, reference: null, secret_future_key: 'must-not-render' }] });
  assert.ok(hr.includes('Assignment'));
  assert.ok(nhsp.includes('SHIFT-1'));
  assert.ok(higher.includes('25.00'));
  assert.ok(!`${hr}${nhsp}${higher}`.includes('must-not-render'));
});

test('receipt evidence rejects wrong order, omission, and duplicate descriptors', async () => {
  const descriptor = order => ({ descriptor: { input_chunk_id: `leaf-${order}`, input_order: order, r2_key: `immutable/${order}.pdf`, sha256: String(order).repeat(64), page_count: 1, size_bytes: 100, logical_source_key: `source:${order}`, logical_manifest_ordinal: order, physical_part_no: 1 } });
  const inputs = [descriptor(1), descriptor(2)];
  const actual = [
    { input_order: 2, r2_key: 'immutable/2.pdf', sha256: '2'.repeat(64), page_count: 1, size_bytes: 100 },
    { input_order: 1, r2_key: 'immutable/1.pdf', sha256: '1'.repeat(64), page_count: 1, size_bytes: 100 }
  ];
  await assert.rejects(() => buildMergeReceipt({}, { processor_policy_version: 'INVOICE_PROCESSOR_LIMITS_V4', plan_generation: 1 }, inputs, { actual_inputs: actual }, { r2_key: 'out.pdf', sha256: 'f'.repeat(64), size_bytes: 200, page_count: 2 }), /INPUT_ORDER_MISMATCH/);
  await assert.rejects(() => buildMergeReceipt({}, { processor_policy_version: 'INVOICE_PROCESSOR_LIMITS_V4', plan_generation: 1 }, inputs, { actual_inputs: actual.slice(0, 1) }, { r2_key: 'out.pdf', sha256: 'f'.repeat(64), size_bytes: 100, page_count: 1 }), /ACTUAL_INPUT_COUNT_MISMATCH/);
});

test('final receipt verification recomputes the complete merge tree', async () => {
  const inputs = [1, 2].map(order => ({
    descriptor: {
      input_chunk_id: `leaf-${order}`,
      input_order: order,
      r2_key: `immutable/${order}.pdf`,
      sha256: String(order).repeat(64),
      page_count: 1,
      size_bytes: 100,
      logical_source_key: `source:${order}`,
      logical_manifest_ordinal: order,
      physical_part_no: 1
    }
  }));
  const actualInputs = inputs.map((input, index) => ({
    input_order: index + 1,
    r2_key: input.descriptor.r2_key,
    sha256: input.descriptor.sha256,
    page_count: 1,
    size_bytes: 100
  }));
  const receipt = await buildMergeReceipt(
    {},
    { processor_policy_version: 'INVOICE_PROCESSOR_LIMITS_V4', plan_generation: 1 },
    inputs,
    { actual_inputs: actualInputs, processor_version: 'cloudtms-native-v6' },
    {
      r2_key: 'immutable/final.pdf',
      sha256: 'f'.repeat(64),
      size_bytes: 200,
      page_count: 2
    }
  );
  const verified = await verifyMergeReceiptTree(receipt, {
    maximum_depth: 4,
    maximum_receipts: 10
  });
  assert.equal(verified.leaves.length, 2);
  assert.equal(verified.output.sha256, 'f'.repeat(64));
  const tampered = structuredClone(receipt);
  tampered.input_receipts[0].actual_sha256 = 'e'.repeat(64);
  await assert.rejects(
    () => verifyMergeReceiptTree(tampered, {
      maximum_depth: 4,
      maximum_receipts: 10
    }),
    /INPUT_PHYSICAL_RECEIPT_MISMATCH/
  );
});
test('unrelated API routes bypass async admin and cohort checks', async () => {
  let authCalls = 0;
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/users/me/grid-prefs'),
    { INVOICE_ASYNC_PIPELINE_ENABLED: 'true', INVOICE_ASYNC_ALLOWED_USER_IDS: '00000000-0000-4000-8000-000000000010' },
    {},
    { requireUser: async () => { authCalls += 1; return null; }, rpc: async () => assert.fail('RPC must not be called') }
  );
  assert.equal(response, null);
  assert.equal(authCalls, 0);
});

test('complete heartbeat RPC failure aborts owned work before completion submission', async () => {
  const chunkId = '00000000-0000-4000-8000-000000000041';
  const leaseToken = '00000000-0000-4000-8000-000000000042';
  let completionCalled = false;
  let processorAborted = false;
  const claim = {
    chunk_id: chunkId,
    lease_token: leaseToken,
    fence_token: 4,
    operation_control_version: 1,
    lease_expires_at_utc: new Date(Date.now() + 120_000).toISOString()
  };
  const config = {
    ...getInvoiceQueueRuntimeConfig({}),
    processorEnabled: true,
    heartbeatMs: 10,
    heartbeatFailureAbortMarginMs: 30_000,
    maximumConsecutiveHeartbeatFailures: 1,
    finalOwnershipCheckRequired: true,
    assetConcurrency: 1,
    nativeRequestTimeoutMs: 5000
  };
  const env = {
    INVOICE_DOCUMENT_PROCESSOR_SECRET:
      'processor-test-secret-with-more-than-thirty-two-characters',
    INVOICE_DOCUMENT_PROCESSOR: {
      fetch(_url, init) {
        return new Promise((resolve, reject) => {
          init.signal.addEventListener('abort', () => {
            processorAborted = true;
            reject(Object.assign(new Error('aborted'), { code: 'PROCESSOR_ABORTED' }));
          }, { once: true });
        });
      }
    }
  };
  const result = await processInvoiceDocumentChunksBatch(env, [claim], {
    config,
    rpc: async name => {
      if (name === 'invoice_work_context_batch') {
        return [{
          accepted: true,
          status: 'OK',
          chunk_id: chunkId,
          chunk_type: 'ASSET_INSPECT',
          expected_result_identity: {
            chunk_id: chunkId,
            fence_token: 4,
            action: 'ASSET_INSPECT',
            plan_generation: 1,
            processor_policy_version: 'INVOICE_PROCESSOR_LIMITS_V4',
            immutable_destination_prefix: 'invoice-assets/test/'
          },
          original_r2_key: 'source/test.pdf'
        }];
      }
      if (name === 'invoice_work_touch_batch') {
        throw Object.assign(new Error('touch unavailable'), { code: 'TOUCH_UNAVAILABLE' });
      }
      if (name === 'invoice_work_complete_batch') {
        completionCalled = true;
        return [];
      }
      throw new Error(`Unexpected RPC ${name}`);
    }
  });
  assert.equal(processorAborted, true);
  assert.equal(completionCalled, false);
  assert.equal(result.ownership_lost, 1);
  assert.equal(result.completed, 0);
});

test('USER_NUDGE advances database work only and dispatches released document lanes', async () => {
  const claimedTypes = [];
  const dispatcherPayloads = [];
  let claimCount = 0;
  const result = await drainInvoiceOperations({
    INVOICE_ASYNC_PIPELINE_ENABLED: 'true',
    INVOICE_QUEUE_DISPATCH_SECRET:
      'dispatcher-test-secret-with-more-than-thirty-two-characters',
    INVOICE_QUEUE_DISPATCHER: {
      async fetch(_url, init) {
        dispatcherPayloads.push(JSON.parse(init.body));
        return new Response('{}', { status: 202 });
      }
    }
  }, {
    mode: 'user_nudge',
    lanes: ['DATABASE'],
    config: {
      ...getInvoiceQueueRuntimeConfig({}),
      enabled: true,
      userNudgeDatabaseClaimLimit: 2,
      userNudgeDocumentClaimLimit: 2,
      userNudgeCycles: 1,
      userNudgeDeadlineMs: 5000,
      scheduledDocumentClaimLimit: 2,
      safetyMarginMs: 100,
      continuationEnabled: true,
      maximumContinuationDepth: 4
    },
    rpc: async (name, args) => {
      if (name === 'invoice_work_claim_batch') {
        claimedTypes.push(args.p_chunk_types);
        claimCount += 1;
        return claimCount === 1 ? [{
          chunk_id: '00000000-0000-4000-8000-000000000051',
          lease_token: '00000000-0000-4000-8000-000000000052',
          fence_token: 1,
          operation_control_version: 1
        }] : [];
      }
      if (name === 'invoice_operation_advance_batch') {
        return [{
          accepted: true,
          status: 'WAITING',
          released_chunk_types: ['PDF_MERGE']
        }];
      }
      throw new Error(`Unexpected RPC ${name}`);
    }
  });
  assert.equal(claimedTypes.length, 1);
  assert.equal(claimedTypes[0].includes('PDF_MERGE'), false);
  assert.equal(result.document.claimed, 0);
  assert.equal(result.continuation_dispatched, true);
  assert.ok(dispatcherPayloads[0].lanes.includes('PDF_MERGE'));
});

test('reconciliation uses bounded keyset pages and emits a signed continuation cursor', async () => {
  const originalFetch = globalThis.fetch;
  const actor = '00000000-0000-4000-8000-000000000060';
  const dispatcherBodies = [];
  let scopePage = 0;
  globalThis.fetch = async request => {
    const url = new URL(
      typeof request === 'string'
        ? request
        : (request instanceof URL ? request.href : request.url)
    );
    if (url.pathname.endsWith('/tms_users')) {
      return new Response(JSON.stringify([{ id: actor, is_active: true, role: 'admin' }]), { status: 200 });
    }
    assert.ok(url.pathname.endsWith('/invoice_operations'));
    scopePage += 1;
    const start = (scopePage - 1) * 2;
    const rows = Array.from({ length: 3 }, (_, index) => ({
      id: `00000000-0000-4000-8000-${String(start + index + 70).padStart(12, '0')}`,
      updated_at_utc: new Date(Date.UTC(2026, 6, 20, 0, 0, start + index)).toISOString()
    }));
    return new Response(JSON.stringify(rows), { status: 200 });
  };
  try {
    const result = await runInvoiceReconciliationCycle({
      INVOICE_ASYNC_PIPELINE_ENABLED: 'true',
      INVOICE_ACTOR_USER_ID: actor,
      SUPABASE_URL: 'https://supabase.test',
      SUPABASE_SERVICE_ROLE_KEY: 'test-only',
      INVOICE_QUEUE_DISPATCH_SECRET:
        'dispatcher-test-secret-with-more-than-thirty-two-characters',
      INVOICE_QUEUE_DISPATCHER: {
        async fetch(_url, init) {
          dispatcherBodies.push(JSON.parse(init.body));
          return new Response('{}', { status: 202 });
        }
      }
    }, {
      config: {
        ...getInvoiceQueueRuntimeConfig({}),
        enabled: true,
        reconciliationPageSize: 2,
        reconciliationMaximumPagesPerInvocation: 1,
        continuationEnabled: true,
        maximumContinuationDepth: 4
      },
      rpc: async name => {
        if (name === 'invoice_operation_start_batch') {
          return [{
            accepted: true,
            operation_id: '00000000-0000-4000-8000-000000000090',
            operation_type: 'RECONCILE',
            status: 'QUEUED'
          }];
        }
        if (name === 'invoice_work_claim_batch') return [];
        throw new Error(`Unexpected RPC ${name}`);
      }
    });
    assert.equal(result.rows_examined, 2);
    assert.equal(result.continuation_requested, true);
    assert.ok(result.next_cursor.operation_id);
    assert.ok(dispatcherBodies.some(body => body.reconciliation_cursor));
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('capabilities remain available while async is disabled and do not disclose cohort IDs', async () => {
  const actor = '00000000-0000-4000-8000-000000000010';
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoice-async/capabilities'),
    {
      INVOICE_ASYNC_PIPELINE_ENABLED: 'false',
      INVOICE_DOCUMENT_PROCESSOR_ENABLED: 'true',
      INVOICE_ASYNC_ALLOWED_USER_IDS: actor
    },
    {},
    {
      requireUser: async () => ({ id: actor, role: 'admin', active: true }),
      rpc: async () => assert.fail('Capability route must not call a queue RPC')
    }
  );
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.pipeline_enabled, false);
  assert.equal(body.enabled_for_user, false);
  assert.equal(JSON.stringify(body).includes(actor), false);
});

test('single issue requires the revision reviewed by the modal', async () => {
  const actor = '00000000-0000-4000-8000-000000000010';
  const invoice = '00000000-0000-4000-8000-000000000011';
  const response = await handleInvoiceAsyncHttpRequest(
    new Request(`https://example.test/api/invoices/${invoice}/issue`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}'
    }),
    v8Environment({ INVOICE_ASYNC_ALLOWED_USER_IDS: actor }),
    {},
    {
      requireUser: async () => ({ id: actor, role: 'admin', active: true }),
      rpc: v8Rpc(async () => assert.fail('Issue RPC must not run without expected revision'))
    }
  );
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error, 'EXPECTED_INVOICE_REVISION_REQUIRED');
});

test('batch generation submits one V2 selection root and omits caller time authority', async () => {
  const actor = '00000000-0000-4000-8000-000000000010';
  const calls = [];
  const background = [];
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices/batch-generate/confirm', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'idempotency-key': 'batch-root-token'
      },
      body: JSON.stringify({
        selection_contract: {
          contract_version: 'INVOICE_BATCH_SELECTION_ROOT_V2',
          query: v8Query('GENERATE'),
          selection: v8Selection()
        }
      })
    }),
    v8Environment({ INVOICE_ASYNC_ALLOWED_USER_IDS: actor }),
    {
      waitUntil(promise) {
        background.push(promise);
      }
    },
    {
      requireUser: async () => ({ id: actor, role: 'admin', active: true }),
      rpc: v8Rpc(async (name, args) => {
        calls.push({ name, args });
        if (name === 'invoice_operation_start_batch') {
          assert.equal(args.p_commands.length, 1);
          assert.equal(args.p_commands[0].command_type, 'GENERATE_SELECTED');
          assert.equal(args.p_commands[0].selection_contract.contract_version, 'INVOICE_BATCH_SELECTION_ROOT_V2');
          assert.equal(args.p_now_utc, undefined);
          return [{
            command_no: 1,
            accepted: true,
            created: true,
            status: 'QUEUED',
            operation_id: '00000000-0000-4000-8000-000000000013',
            selection_contract_version: 'INVOICE_BATCH_SELECTION_V2',
            selection_expansion_pending: true
          }];
        }
        if (name === 'invoice_work_claim_batch') return [];
        throw new Error(`Unexpected RPC ${name}`);
      })
    }
  );
  assert.equal(response.status, 202);
  const result = await response.json();
  assert.equal(result.contract_version, 'INVOICE_BATCH_SELECTION_ROOT_V2');
  assert.equal(result.root_operation_id, '00000000-0000-4000-8000-000000000013');
  assert.equal(result.selection_expansion_pending, true);
  assert.equal(calls.filter(call => call.name === 'invoice_operation_start_batch').length, 1);
  await Promise.all(background);
});

test('Generate-and-view adopts an authoritative existing draft-view command', async () => {
  const actor = '00000000-0000-4000-8000-000000000010';
  const invoiceId = '00000000-0000-4000-8000-000000000011';
  const selectionKey = 'invoice:00000000-0000-4000-8000-000000000011';
  const query = v8Query('GENERATE', {
    mode: 'EXPLICIT_KEYS',
    selection_keys: [selectionKey],
    expected_source_revisions: { [selectionKey]: '7' }
  });
  delete query.page_size;
  delete query.cursor;
  let submitted = null;
  const background = [];
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices/batch-generate/confirm', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'idempotency-key': 'generate-and-view-token'
      },
      body: JSON.stringify({
        selection_contract: {
          contract_version: 'INVOICE_BATCH_SELECTION_ROOT_V2',
          query,
          selection: v8Selection()
        }
      })
    }),
    v8Environment({ INVOICE_ASYNC_ALLOWED_USER_IDS: actor }),
    { waitUntil(promise) { background.push(promise); } },
    {
      requireUser: async () => ({ id: actor, role: 'admin', active: true }),
      rpc: v8Rpc(async (name, args) => {
        if (name === 'invoice_batch_generate_candidates') {
          const candidateQuery = args.p_query;
          return {
            contract_version: 'INVOICE_BATCH_CANDIDATES_V2',
            action: 'GENERATE',
            mode: 'EXPLICIT_KEYS',
            snapshot: candidateQuery.snapshot,
            normalised_filter: candidateQuery.filters,
            normalised_sort: candidateQuery.sort,
            filter_hash: await invoiceAsyncHttpInternals.hashInvoiceBatchFilter(
              'GENERATE', candidateQuery.filters, candidateQuery.sort
            ),
            query_hash: await invoiceAsyncHttpInternals.hashInvoiceBatchQuery(
              'GENERATE', candidateQuery.filters, candidateQuery.sort,
              candidateQuery.snapshot
            ),
            selection_hash: await invoiceAsyncHttpInternals.hashInvoiceBatchSelection(
              candidateQuery.selection
            ),
            rows: [{
              selection_key: selectionKey,
              source_revision: '7',
              selectable: true,
              command_payload: {
                command_type: 'VIEW_INVOICE_DOCUMENT',
                invoice_id: invoiceId,
                purpose: 'DRAFT_PREVIEW',
                expected_revision: 7,
                source_revision: '7'
              }
            }],
            page: {
              page_size: 1,
              returned_count: 1,
              total_count: 1,
              has_more: false
            },
            totals: {
              filtered_total: 1,
              display_total: 1,
              eligible_total: 1,
              selected_total: 1,
              excluded_total: 0,
              blocked_total: 0
            },
            selection_summary: {
              eligible_total: 1,
              selected_total: 1,
              excluded_total: 0,
              blocked_total: 0,
              exact: true
            },
            group_selection: [],
            facets: {}
          };
        }
        if (name === 'invoice_operation_start_batch') {
          submitted = args.p_commands[0];
          return [{
            command_no: 1,
            command_type: 'VIEW_INVOICE_DOCUMENT',
            accepted: true,
            created: true,
            status: 'QUEUED',
            operation_id: '00000000-0000-4000-8000-000000000012'
          }];
        }
        if (name === 'invoice_work_claim_batch') return [];
        throw new Error(`Unexpected RPC ${name}`);
      })
    }
  );
  await Promise.all(background);
  assert.equal(response.status, 202);
  assert.equal(submitted.command_type, 'VIEW_INVOICE_DOCUMENT');
  assert.equal(submitted.invoice_id, invoiceId);
  assert.equal(submitted.purpose, 'DRAFT_PREVIEW');
  assert.equal(submitted.expected_revision, '7');
  assert.equal(submitted.source_revision, '7');
  assert.equal(submitted.priority_reason, 'VIEW_NOW');
  assert.equal(submitted.command_token, 'generate-and-view-token');
  assert.equal(submitted.source_ids, undefined);
});

test('invoice batch filters and sort are strictly allowlisted and canonical', () => {
  const query = {
    filters: {
      client_ids: [
        '00000000-0000-4000-8000-000000000002',
        '00000000-0000-4000-8000-000000000001',
        '00000000-0000-4000-8000-000000000001'
      ],
      week_endings: ['2026-07-26'],
      status_codes: ['ready'],
      allow_early: true,
      display_mode: 'ready'
    },
    sort: {
      group_preset: 'client_week_candidate',
      sort_key: 'total_inc_vat',
      sort_direction: 'desc'
    }
  };
  assert.deepEqual(invoiceAsyncHttpInternals.normaliseInvoiceBatchFilters(query, 'GENERATE'), {
    client_ids: [
      '00000000-0000-4000-8000-000000000001',
      '00000000-0000-4000-8000-000000000002'
    ],
    candidate_ids: [],
    week_endings: ['2026-07-26'],
    week_ending_from: null,
    week_ending_to: null,
    status_codes: ['READY'],
    blocker_codes: [],
    search: null,
    allow_early: true,
    display_mode: 'READY',
    invoice_streams: []
  });
  assert.deepEqual(invoiceAsyncHttpInternals.normaliseInvoiceBatchSort(query, 'GENERATE'), {
    group_preset: 'CLIENT_WEEK_CANDIDATE',
    sort_key: 'TOTAL_INC_VAT',
    sort_direction: 'DESC'
  });
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchFilters({ filters: { arbitrary_sql: 'x' } }, 'GENERATE'),
    /INVOICE_BATCH_FILTER_UNKNOWN_FIELD/
  );
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchSort({ sort: { sort_key: 'invoice_number' } }, 'GENERATE'),
    /INVOICE_BATCH_SORT_KEY_INVALID/
  );
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchFilters({ filters: { invoice_streams: ['NHSP'] } }, 'GENERATE'),
    /INVOICE_BATCH_FILTER_UNKNOWN_FIELD/
  );
});

test('invoice batch selection ledger accepts ordered semantic V2 selectors', () => {
  const contract = invoiceAsyncHttpInternals.normaliseInvoiceBatchSelectionRules({
    selection: {
      contract_version: 'INVOICE_BATCH_SELECTION_V2',
      mode: 'IMPLICIT_ALL',
      default_selected: true,
      rules: [
        {
          sequence: 1,
          action: 'exclude',
          selector: { type: 'row', selection_key: 'group:1' }
        },
        {
          sequence: 2,
          action: 'include',
          selector: {
            type: 'week_client',
            week_ending_date: '2026-07-26',
            client_id: '00000000-0000-4000-8000-000000000001'
          }
        },
        {
          sequence: 3,
          action: 'exclude',
          selector: {
            type: 'dimension_group',
            status_code: 'READY',
            candidate_id: '00000000-0000-4000-8000-000000000002'
          }
        }
      ]
    }
  });
  assert.equal(contract.rules[0].action, 'EXCLUDE');
  assert.equal(contract.rules[1].selector.type, 'WEEK_CLIENT');
  assert.deepEqual(contract.rules[2].selector, {
    type: 'DIMENSION_GROUP',
    candidate_id: '00000000-0000-4000-8000-000000000002',
    status_code: 'READY'
  });
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchSelectionRules({
      contract_version: 'INVOICE_BATCH_SELECTION_V2',
      mode: 'IMPLICIT_ALL',
      default_selected: true,
      rules: [
        { sequence: 2, action: 'EXCLUDE', selector: { type: 'ROW', selection_key: 'b' } },
        { sequence: 1, action: 'EXCLUDE', selector: { type: 'ROW', selection_key: 'a' } }
      ]
    }),
    /BATCH_SELECTION_RULE_SEQUENCE_INVALID/
  );
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchSelectionRules({
      contract_version: 'INVOICE_BATCH_SELECTION_V2',
      mode: 'IMPLICIT_ALL',
      default_selected: true,
      rules: [{
        sequence: 1,
        action: 'INCLUDE',
        selector: { type: 'GROUP_KEY', group_key: 'server-only' }
      }]
    }),
    /BATCH_SELECTION_SELECTOR/
  );
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchSelectionRules({
      contract_version: 'INVOICE_BATCH_SELECTION_V2',
      mode: 'IMPLICIT_ALL',
      default_selected: true,
      rules: [{
        sequence: 1,
        action: 'INCLUDE',
        selector: {
          type: 'ROW',
          selection_key: 'group:1',
          client_id: '00000000-0000-4000-8000-000000000001'
        }
      }]
    }),
    /BATCH_SELECTION_SELECTOR_INVALID/
  );
});

test('invoice batch cursor is expiring, HMAC protected and query-bound', async () => {
  const env = { SESSION_TOKEN_SECRET: V8_CURSOR_SECRET };
  const sort = {
    group_preset: 'WEEK_CLIENT_CANDIDATE',
    sort_key: 'WEEK_ENDING_DATE',
    sort_direction: 'ASC'
  };
  const now = Date.now();
  const snapshot = v8Snapshot('GENERATE', {
    at_utc: new Date(now - 1_000).toISOString(),
    expires_at_utc: new Date(now + 60_000).toISOString()
  });
  const token = await invoiceAsyncHttpInternals.encodeInvoiceBatchCursor(env, {
    action: 'GENERATE',
    snapshot,
    filter_hash: 'a'.repeat(64),
    query_hash: 'b'.repeat(64),
    sort,
    next_cursor_values: {
      after_selection_key: 'group:1',
      after_sort_date: '2026-07-26'
    },
    issued_at_utc: new Date(now).toISOString()
  });
  const decoded = await invoiceAsyncHttpInternals.decodeInvoiceBatchCursor(env, token, {
    action: 'GENERATE',
    snapshot,
    filter_hash: 'a'.repeat(64),
    query_hash: 'b'.repeat(64),
    sort
  });
  assert.equal(decoded.keyset.after_selection_key, 'group:1');
  await assert.rejects(
    () => invoiceAsyncHttpInternals.decodeInvoiceBatchCursor(env, token, {
      action: 'GENERATE',
      snapshot,
      filter_hash: 'b'.repeat(64),
      query_hash: 'b'.repeat(64),
      sort
    }),
    /BATCH_CURSOR_FILTER_MISMATCH/
  );
  const tampered = `${token.slice(0, -1)}${token.endsWith('a') ? 'b' : 'a'}`;
  await assert.rejects(
    () => invoiceAsyncHttpInternals.decodeInvoiceBatchCursor(env, tampered, {
      action: 'GENERATE',
      snapshot,
      filter_hash: 'a'.repeat(64),
      query_hash: 'b'.repeat(64),
      sort
    }),
    /BATCH_CURSOR_INVALID/
  );
  const expiredSnapshot = v8Snapshot('GENERATE', {
    at_utc: new Date(now - 120_000).toISOString(),
    expires_at_utc: new Date(now - 60_000).toISOString()
  });
  const expired = await invoiceAsyncHttpInternals.encodeInvoiceBatchCursor(env, {
    action: 'GENERATE',
    snapshot: expiredSnapshot,
    filter_hash: 'a'.repeat(64),
    query_hash: 'b'.repeat(64),
    sort,
    next_cursor_values: {
      after_selection_key: 'group:1',
      after_sort_date: '2026-07-26'
    },
    issued_at_utc: new Date(now - 90_000).toISOString()
  });
  await assert.rejects(
    () => invoiceAsyncHttpInternals.decodeInvoiceBatchCursor(env, expired),
    /BATCH_CURSOR_EXPIRED/
  );
});

test('invoice batch query hash includes the complete signed snapshot identity', async () => {
  const filters = invoiceAsyncHttpInternals.normaliseInvoiceBatchFilters({}, 'GENERATE');
  const sort = invoiceAsyncHttpInternals.normaliseInvoiceBatchSort({}, 'GENERATE');
  const first = await invoiceAsyncHttpInternals.hashInvoiceBatchQuery(
    'GENERATE', filters, sort, v8Snapshot('GENERATE')
  );
  const second = await invoiceAsyncHttpInternals.hashInvoiceBatchQuery(
    'GENERATE', filters, sort, v8Snapshot('GENERATE', {
      at_utc: '2026-07-27T12:00:00.001Z'
    })
  );
  assert.match(first, /^[0-9a-f]{64}$/);
  assert.notEqual(first, second);
});

test('V2 candidate envelopes remain typed and legacy envelopes fail closed', () => {
  const parsed = invoiceAsyncHttpInternals.candidateGroupsFromRpc({
    contract_version: 'INVOICE_BATCH_CANDIDATES_V2',
    action: 'GENERATE',
    mode: 'PAGE',
    rows: [{ selection_key: 'group:1' }],
    page: { page_size: 1, returned_count: 1, total_count: 2, has_more: true },
    totals: {
      filtered_total: 2,
      display_total: 2,
      eligible_total: 2,
      selected_total: 2,
      excluded_total: 0,
      blocked_total: 0
    },
    selection_summary: {
      eligible_total: 2,
      selected_total: 2,
      excluded_total: 0,
      blocked_total: 0,
      exact: false
    },
    group_selection: [],
    facets: {},
    filter_hash: 'a'.repeat(64),
    query_hash: 'b'.repeat(64),
    selection_hash: 'c'.repeat(64)
  });
  assert.equal(parsed.kind, 'V2');
  assert.equal(parsed.legacy, false);
  assert.equal(parsed.rows[0].selection_key, 'group:1');
  assert.equal(parsed.page.has_more, true);
  assert.throws(() => invoiceAsyncHttpInternals.candidateGroupsFromRpc({
    contract_version: 'INVOICE_BATCH_CANDIDATES_V1',
    rows: []
  }), /INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH/);
});

test('SUMMARY group records require canonical selectors and consistent state', () => {
  const selector = {
    type: 'WEEK',
    week_ending_date: '2026-07-26'
  };
  const base = {
    contract_version: 'INVOICE_BATCH_CANDIDATES_V2',
    action: 'GENERATE',
    mode: 'SUMMARY',
    rows: [],
    page: { page_size: 0, returned_count: 0, total_count: 2, has_more: false },
    totals: {
      filtered_total: 2,
      display_total: 2,
      eligible_total: 2,
      selected_total: 2,
      excluded_total: 0,
      blocked_total: 0
    },
    selection_summary: {
      eligible_total: 2,
      selected_total: 2,
      excluded_total: 0,
      blocked_total: 0,
      exact: true
    },
    group_selection: [{
      selector,
      group_key: null,
      eligible_total: 2,
      selected_total: 2,
      state: 'CHECKED',
      has_hidden_override: false
    }],
    facets: {},
    filter_hash: 'a'.repeat(64),
    query_hash: 'b'.repeat(64),
    selection_hash: 'c'.repeat(64)
  };

  const parsed = invoiceAsyncHttpInternals.candidateGroupsFromRpc(base);
  assert.deepEqual(parsed.group_selection[0].selector, selector);
  assert.equal(parsed.group_selection[0].group_key, null);

  for (const group of [
    { ...base.group_selection[0], selector: undefined },
    Object.fromEntries(Object.entries(base.group_selection[0]).filter(([key]) => key !== 'group_key')),
    { ...base.group_selection[0], unexpected: true },
    { ...base.group_selection[0], eligible_total: '2' },
    { ...base.group_selection[0], group_key: {} },
    { ...base.group_selection[0], state: 'CHECKED', has_hidden_override: true },
    { ...base.group_selection[0], eligible_total: 0, selected_total: 0, state: 'DISABLED', has_hidden_override: true }
  ]) {
    assert.throws(
      () => invoiceAsyncHttpInternals.candidateGroupsFromRpc({
        ...base,
        group_selection: [group]
      }),
      /INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH/
    );
  }
  assert.throws(
    () => invoiceAsyncHttpInternals.candidateGroupsFromRpc({
      ...base,
      group_selection: [base.group_selection[0], base.group_selection[0]]
    }),
    /INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH/
  );
});

test('SUMMARY group-selector normalization accepts 400 and rejects duplicates or 401', () => {
  const selectors = Array.from({ length: 400 }, (_, index) => ({
    type: 'ROW',
    selection_key: `row:${index + 1}`
  }));
  const summaryQuery = v8Query('GENERATE', {
    mode: 'SUMMARY',
    group_selectors: selectors
  });
  delete summaryQuery.page_size;
  delete summaryQuery.cursor;
  const normalized = invoiceAsyncHttpInternals.normaliseInvoiceBatchQueryBody(
    summaryQuery,
    'GENERATE'
  );
  assert.equal(normalized.group_selectors.length, 400);
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchQueryBody(
      {
        ...summaryQuery,
        group_selectors: [...selectors, { type: 'ROW', selection_key: 'row:401' }]
      },
      'GENERATE'
    ),
    /INVOICE_BATCH_QUERY_MODE_FIELD_INVALID/
  );
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchQueryBody(
      {
        ...summaryQuery,
        group_selectors: [selectors[0], { type: 'row', selection_key: 'row:1' }]
      },
      'GENERATE'
    ),
    /BATCH_SELECTION_SELECTOR_INVALID/
  );
});

test('POST candidate route returns V8 and keeps database keysets behind an opaque cursor', async () => {
  const env = v8Environment();
  const calls = [];
  const snapshot = v8Snapshot('GENERATE', {
    at_utc: new Date(Date.now() - 1_000).toISOString(),
    expires_at_utc: new Date(Date.now() + 60_000).toISOString()
  });
  const candidateRpc = async (name, args) => {
    assert.equal(name, 'invoice_batch_generate_candidates');
    calls.push(args.p_query);
    const query = args.p_query;
    const responseSnapshot = query.snapshot || snapshot;
    const filterHash = await invoiceAsyncHttpInternals.hashInvoiceBatchFilter(
      'GENERATE', query.filters, query.sort
    );
    const queryHash = await invoiceAsyncHttpInternals.hashInvoiceBatchQuery(
      'GENERATE', query.filters, query.sort, responseSnapshot
    );
    const selectionHash = await invoiceAsyncHttpInternals.hashInvoiceBatchSelection(query.selection);
    return {
      contract_version: 'INVOICE_BATCH_CANDIDATES_V2',
      action: 'GENERATE',
      mode: 'PAGE',
      snapshot: responseSnapshot,
      normalised_filter: query.filters,
      normalised_sort: query.sort,
      filter_hash: filterHash,
      query_hash: queryHash,
      selection_hash: selectionHash,
      rows: [{ selection_key: calls.length === 1 ? 'group:1' : 'group:2', selectable: true }],
      page: calls.length === 1
        ? {
            page_size: 25,
            returned_count: 1,
            total_count: 2,
            has_more: true,
            next_cursor_values: {
              after_selection_key: 'group:1',
              after_sort_text: 'client one'
            }
          }
        : {
            page_size: 25,
            returned_count: 1,
            total_count: 2,
            has_more: false,
            next_cursor_values: null
          },
      totals: {
        filtered_total: 2,
        display_total: 2,
        eligible_total: 2,
        blocked_total: 0,
        selected_total: 2,
        excluded_total: 0
      },
      selection_summary: {
        eligible_total: 2,
        selected_total: 2,
        excluded_total: 0,
        blocked_total: 0,
        exact: false
      },
      group_selection: [],
      facets: {}
    };
  };
  const firstRequest = {
    contract_version: 'INVOICE_BATCH_QUERY_V2',
    action: 'GENERATE',
    mode: 'PAGE',
    snapshot: null,
    page_size: 25,
    cursor: null,
    filters: {},
    sort: {
      group_preset: 'WEEK_CLIENT_CANDIDATE',
      sort_key: 'CLIENT_NAME',
      sort_direction: 'ASC'
    },
    selection: v8Selection()
  };
  const first = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices/batch-generate/candidates', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(firstRequest)
    }),
    env,
    {},
    { requireUser: async () => v8Actor(), rpc: v8Rpc(candidateRpc) }
  );
  const firstBody = await first.json();
  assert.equal(first.status, 200, JSON.stringify(firstBody));
  assert.equal(first.headers.get('x-invoice-async-contract-version'), 'INVOICE_ASYNC_BACKEND_V8');
  assert.ok(firstBody.page.next_cursor);
  assert.equal(
    Object.prototype.hasOwnProperty.call(firstBody.normalised_filter, 'invoice_streams'),
    false
  );
  assert.equal(calls[0].cursor, null);

  const second = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices/batch-generate/candidates', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        ...firstRequest,
        snapshot: firstBody.snapshot,
        cursor: firstBody.page.next_cursor
      })
    }),
    env,
    {},
    { requireUser: async () => v8Actor(), rpc: v8Rpc(candidateRpc) }
  );
  const secondBody = await second.json();
  assert.equal(second.status, 200, JSON.stringify(secondBody));
  assert.deepEqual(calls[1].cursor, {
    after_selection_key: 'group:1',
    after_sort_text: 'client one'
  });
  assert.equal(secondBody.page.next_cursor, null);
});

test('SUMMARY candidate route enforces exact group-selector coverage and request order', async () => {
  const env = v8Environment();
  const weekSelector = { type: 'WEEK', week_ending_date: '2026-07-26' };
  const clientSelector = {
    type: 'CLIENT',
    client_id: '00000000-0000-4000-8000-000000000020'
  };
  const extraSelector = {
    type: 'STATUS',
    status_code: 'READY'
  };
  const requestBody = v8Query('GENERATE', {
    mode: 'SUMMARY',
    group_selectors: [weekSelector, clientSelector]
  });
  delete requestBody.page_size;
  delete requestBody.cursor;
  const groupRecord = selector => ({
    selector,
    group_key: null,
    eligible_total: 1,
    selected_total: 1,
    state: 'CHECKED',
    has_hidden_override: false
  });
  const requestWithGroups = async returnedGroups => handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices/batch-generate/candidates', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(requestBody)
    }),
    env,
    {},
    {
      requireUser: async () => v8Actor(),
      rpc: v8Rpc(async (name, args) => {
        assert.equal(name, 'invoice_batch_generate_candidates');
        const query = args.p_query;
        return {
          contract_version: 'INVOICE_BATCH_CANDIDATES_V2',
          action: 'GENERATE',
          mode: 'SUMMARY',
          snapshot: query.snapshot,
          normalised_filter: query.filters,
          normalised_sort: query.sort,
          filter_hash: await invoiceAsyncHttpInternals.hashInvoiceBatchFilter(
            'GENERATE', query.filters, query.sort
          ),
          query_hash: await invoiceAsyncHttpInternals.hashInvoiceBatchQuery(
            'GENERATE', query.filters, query.sort, query.snapshot
          ),
          selection_hash: await invoiceAsyncHttpInternals.hashInvoiceBatchSelection(
            query.selection
          ),
          rows: [],
          page: {
            page_size: 0,
            returned_count: 0,
            total_count: 1,
            has_more: false
          },
          totals: {
            filtered_total: 1,
            display_total: 1,
            eligible_total: 1,
            selected_total: 1,
            excluded_total: 0,
            blocked_total: 0
          },
          selection_summary: {
            eligible_total: 1,
            selected_total: 1,
            excluded_total: 0,
            blocked_total: 0,
            exact: true
          },
          group_selection: returnedGroups,
          facets: {}
        };
      })
    }
  );

  const orderedResponse = await requestWithGroups([
    groupRecord(clientSelector),
    groupRecord(weekSelector)
  ]);
  const orderedBody = await orderedResponse.json();
  assert.equal(orderedResponse.status, 200, JSON.stringify(orderedBody));
  assert.deepEqual(
    orderedBody.group_selection.map(group => group.selector),
    [weekSelector, clientSelector]
  );

  for (const returnedGroups of [
    [groupRecord(weekSelector)],
    [
      groupRecord(weekSelector),
      groupRecord(clientSelector),
      groupRecord(extraSelector)
    ]
  ]) {
    const response = await requestWithGroups(returnedGroups);
    const body = await response.json();
    assert.equal(response.status, 503, JSON.stringify(body));
    assert.equal(body.error, 'INVOICE_BATCH_CANDIDATE_CONTRACT_MISMATCH');
  }
});

test('invoice batch JSON reader enforces the byte limit and object-only contract', async () => {
  const exact = await invoiceAsyncHttpInternals.readInvoiceBatchJsonBody(
    new Request('https://example.test', {
      method: 'POST',
      body: JSON.stringify({ value: '1234' })
    }),
    { maximumBytes: 16 }
  );
  assert.deepEqual(exact, { value: '1234' });
  await assert.rejects(
    () => invoiceAsyncHttpInternals.readInvoiceBatchJsonBody(
      new Request('https://example.test', {
        method: 'POST',
        body: JSON.stringify({ value: '12345' })
      }),
      { maximumBytes: 16 }
    ),
    /BATCH_REQUEST_TOO_LARGE/
  );
  await assert.rejects(
    () => invoiceAsyncHttpInternals.readInvoiceBatchJsonBody(
      new Request('https://example.test', { method: 'POST', body: '[]' }),
      { maximumBytes: 16 }
    ),
    /BATCH_REQUEST_OBJECT_REQUIRED/
  );
  await assert.rejects(
    () => invoiceAsyncHttpInternals.readInvoiceBatchJsonBody(
      new Request('https://example.test', { method: 'POST', body: '{' }),
      { maximumBytes: 16 }
    ),
    /BATCH_REQUEST_JSON_INVALID/
  );
});

test('external command identity is caller-stable and never falls back to delivery identity', () => {
  const withoutCommand = new Request('https://example.test', { method: 'POST' });
  assert.throws(
    () => invoiceAsyncHttpInternals.commandToken(withoutCommand, {
      delivery_request_token: 'delivery-only'
    }),
    /BATCH_COMMAND_TOKEN_REQUIRED/
  );
  const withHeader = new Request('https://example.test', {
    method: 'POST',
    headers: { 'idempotency-key': 'stable-command-token' }
  });
  assert.equal(
    invoiceAsyncHttpInternals.commandToken(withHeader, {
      delivery_request_token: 'separate-delivery-token'
    }),
    'stable-command-token'
  );

  const matchingSources = new Request('https://example.test', {
    method: 'POST',
    headers: {
      'idempotency-key': 'stable-command-token',
      'x-idempotency-key': 'stable-command-token'
    }
  });
  assert.equal(
    invoiceAsyncHttpInternals.commandToken(
      matchingSources,
      {
        command_token: ' stable-command-token ',
        request_token: 'stable-command-token'
      },
      {
        bodyFields: ['command_token', 'request_token'],
        invalidCode: 'OPERATION_CONTROL_REQUEST_TOKEN_INVALID'
      }
    ),
    'stable-command-token'
  );

  const conflictingSources = new Request('https://example.test', {
    method: 'POST',
    headers: {
      'idempotency-key': 'header-token',
      'x-idempotency-key': 'different-header-token'
    }
  });
  assert.throws(
    () => invoiceAsyncHttpInternals.commandToken(
      conflictingSources,
      { command_token: 'body-token' },
      { invalidCode: 'GENERATE_COMMAND_TOKEN_INVALID' }
    ),
    /GENERATE_COMMAND_TOKEN_INVALID/
  );
});

test('V8 snapshot, facet, explicit-key, and delivery token boundaries fail closed', async () => {
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchSnapshot({
      ...v8Snapshot('GENERATE'),
      key_id: undefined
    }, 'GENERATE'),
    /BATCH_SNAPSHOT_INVALID/
  );
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchFacetRequest({
      kinds: ['CLIENTS'],
      cursors: { candidates: 'opaque-but-unrequested' }
    }),
    /BATCH_FACET_REQUEST_INVALID/
  );
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchExplicitKeys({
      selection_keys: ['one', 'two'],
      expected_source_revisions: { one: '1', two: '2' }
    }, { maximum: 1, exactCount: 1 }),
    /BATCH_EXPLICIT_KEYS_INVALID/
  );
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseDeliveryRequestToken(
      'same-token',
      'same-token'
    ),
    /DELIVERY_REQUEST_TOKEN_INVALID/
  );
});

test('operation-control envelope is exact, durable-tokened, and DB-hashed', async () => {
  const operationId = '00000000-0000-4000-8000-000000000091';
  let submitted = null;
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoice-operations/control', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        contract_version: 'INVOICE_OPERATION_CONTROL_V2',
        command_token: 'stable-operation-control-token',
        actions: [{ action: 'CANCEL', operation_id: operationId }]
      })
    }),
    v8Environment(),
    {},
    {
      requireUser: async () => v8Actor(),
      rpc: v8Rpc(async (name, args) => {
        assert.equal(name, 'invoice_operation_control_batch');
        submitted = args;
        return [{
          operation_id: operationId,
          action: 'CANCEL',
          accepted: false,
          error: { code: 'OPERATION_NOT_FOUND' }
        }];
      })
    }
  );
  assert.equal(response.status, 200);
  assert.equal(submitted.p_now_utc, undefined);
  assert.equal(
    submitted.p_actions.contract_version,
    'INVOICE_OPERATION_CONTROL_V2'
  );
  assert.equal(
    submitted.p_actions.request_token,
    'stable-operation-control-token'
  );
  assert.match(submitted.p_actions.request_hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(submitted.p_actions.actions, [{
    operation_id: operationId,
    action: 'CANCEL'
  }]);

  const same = await invoiceAsyncHttpInternals.invoiceOperationControlEnvelope(
    V8_ACTOR_ID,
    'stable-operation-control-token',
    submitted.p_actions.actions
  );
  assert.equal(same.request_hash, submitted.p_actions.request_hash);
  const changed = await invoiceAsyncHttpInternals.invoiceOperationControlEnvelope(
    V8_ACTOR_ID,
    'stable-operation-control-token',
    [{ action: 'RAISE_PRIORITY', operation_id: operationId }]
  );
  assert.notEqual(changed.request_hash, submitted.p_actions.request_hash);
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceOperationControlAction({
      action: 'RESCHEDULE',
      operation_id: operationId,
      scheduled_for_utc: new Date(Date.now() + 60_000).toISOString()
    }),
    /OPERATION_CONTROL_ACTION_SCHEMA_INVALID/
  );
});

test('operation-control rejects conflicting token sources before any RPC call', async () => {
  const operationId = '00000000-0000-4000-8000-000000000191';
  let rpcCalls = 0;
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoice-operations/control', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'idempotency-key': 'header-token',
        'x-idempotency-key': 'header-token'
      },
      body: JSON.stringify({
        contract_version: 'INVOICE_OPERATION_CONTROL_V2',
        command_token: 'body-token',
        request_token: 'body-token',
        actions: [{ action: 'CANCEL', operation_id: operationId }]
      })
    }),
    v8Environment(),
    {},
    {
      requireUser: async () => v8Actor(),
      rpc: v8Rpc(async () => {
        rpcCalls += 1;
        return [];
      })
    }
  );
  assert.equal(response.status, 400);
  assert.equal(
    (await response.json()).error,
    'OPERATION_CONTROL_REQUEST_TOKEN_INVALID'
  );
  assert.equal(rpcCalls, 0);
});

test('work-starting document GET and loose credit-note inputs are rejected', async () => {
  const deps = {
    requireUser: async () => v8Actor(),
    rpc: v8Rpc(async name => assert.fail(`Unexpected RPC ${name}`))
  };
  const getResponse = await handleInvoiceAsyncHttpRequest(
    new Request(
      'https://example.test/api/timesheets/00000000-0000-4000-8000-000000000092/pdf',
      { method: 'GET' }
    ),
    v8Environment(),
    {},
    deps
  );
  assert.equal(getResponse.status, 405);
  assert.equal(
    (await getResponse.json()).error,
    'DOCUMENT_PREPARATION_POST_REQUIRED'
  );

  const creditResponse = await handleInvoiceAsyncHttpRequest(
    new Request(
      'https://example.test/api/invoices/00000000-0000-4000-8000-000000000093/credit-note',
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          reason: 'Legacy alias must not be accepted',
          command_token: 'credit-command'
        })
      }
    ),
    v8Environment(),
    {},
    deps
  );
  assert.equal(creditResponse.status, 400);
  assert.equal((await creditResponse.json()).error, 'CREDIT_NOTE_REQUEST_INVALID');
});

test('delivery part number is an exact bounded positive integer', async () => {
  const response = await handleInvoiceAsyncHttpRequest(
    new Request(
      'https://example.test/api/invoices/00000000-0000-4000-8000-000000000094/email',
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          command_token: 'delivery-command',
          delivery_request_token: 'delivery-request',
          delivery_part_number: 101
        })
      }
    ),
    v8Environment(),
    {},
    {
      requireUser: async () => v8Actor(),
      rpc: v8Rpc(async name => assert.fail(`Unexpected RPC ${name}`))
    }
  );
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error, 'DELIVERY_PART_NUMBER_INVALID');
});

test('Batch Issue supports Issue-and-send and Issue-only with separate identities', async () => {
  const rootId = '00000000-0000-4000-8000-000000000077';
  const execute = async deliver => {
    const env = v8Environment();
    let submitted = null;
    const background = [];
    const body = {
      selection_contract: {
        contract_version: 'INVOICE_BATCH_SELECTION_ROOT_V2',
        query: v8Query('ISSUE'),
        selection: v8Selection()
      },
      deliver,
      command_token: deliver ? 'issue-send-command' : 'issue-only-command',
      ...(deliver ? {
        delivery_request_token: 'delivery-request-token',
        delivery_intent: {
          route_mode: 'SERVER_RESOLVED',
          template_version: 'INVOICE_EMAIL_V2'
        }
      } : {})
    };
    const response = await handleInvoiceAsyncHttpRequest(
      new Request('https://example.test/api/invoices/batch-issue/confirm', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body)
      }),
      env,
      { waitUntil(promise) { background.push(promise); } },
      {
        requireUser: async () => v8Actor(),
        rpc: v8Rpc(async (name, args) => {
          if (name === 'invoice_operation_start_batch') {
            assert.equal(args.p_commands.length, 1);
            submitted = args.p_commands[0];
            return [{
              command_no: 1,
              command_type: 'ISSUE_INVOICES',
              accepted: true,
              created: true,
              status: 'QUEUED',
              operation_id: rootId,
              selection_contract_version: 'INVOICE_BATCH_SELECTION_V2',
              selection_expansion_pending: true
            }];
          }
          if (name === 'invoice_work_claim_batch') return [];
          throw new Error(`Unexpected RPC ${name}`);
        })
      }
    );
    await Promise.all(background);
    return { response, submitted };
  };

  const send = await execute(true);
  assert.equal(send.response.status, 202);
  assert.equal(send.submitted.deliver, true);
  assert.equal(send.submitted.command_token, 'issue-send-command');
  assert.equal(send.submitted.delivery_request_token, 'delivery-request-token');
  assert.equal(send.submitted.delivery_intent.route_mode, 'SERVER_RESOLVED');
  assert.notEqual(send.submitted.command_token, send.submitted.delivery_request_token);

  const issueOnly = await execute(false);
  assert.equal(issueOnly.response.status, 202);
  assert.equal(issueOnly.submitted.deliver, false);
  assert.equal(issueOnly.submitted.command_token, 'issue-only-command');
  assert.equal(issueOnly.submitted.delivery_request_token, undefined);
  assert.deepEqual(issueOnly.submitted.delivery_intent, {});
});

test('result cursors bind root, category and atomic result-page revision', async () => {
  const now = Date.now();
  const env = { SESSION_TOKEN_SECRET: V8_CURSOR_SECRET };
  const root = '00000000-0000-4000-8000-000000000080';
  const chunk = '00000000-0000-4000-8000-000000000081';
  const token = await invoiceAsyncHttpInternals.encodeInvoiceBatchResultCursorV2(env, {
    root_operation_id: root,
    action: 'GENERATE',
    result_category: 'FAILED',
    result_page_revision: '51',
    next_cursor_values: {
      after_selection_key: 'scope:51',
      after_chunk_id: chunk
    },
    issued_at_utc: new Date(now).toISOString()
  });
  const decoded = await invoiceAsyncHttpInternals.decodeInvoiceBatchResultCursorV2(env, token, {
    root_operation_id: root,
    action: 'GENERATE',
    result_category: 'FAILED',
    result_page_revision: '51'
  });
  assert.deepEqual(decoded.keyset, {
    after_selection_key: 'scope:51',
    after_chunk_id: chunk
  });
  await assert.rejects(
    () => invoiceAsyncHttpInternals.decodeInvoiceBatchResultCursorV2(env, token, {
      root_operation_id: root,
      action: 'GENERATE',
      result_category: 'BLOCKED',
      result_page_revision: '51'
    }),
    /OPERATION_RESULT_CURSOR_INVALID/
  );
  await assert.rejects(
    () => invoiceAsyncHttpInternals.decodeInvoiceBatchResultCursorV2(env, token, {
      root_operation_id: root,
      action: 'GENERATE',
      result_category: 'FAILED',
      result_page_revision: '52'
    }),
    /OPERATION_RESULT_CURSOR_STALE/
  );
});

test('capabilities advertise mandatory V8 features only when every dependency is ready', async () => {
  const env = v8Environment();
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoice-async/capabilities'),
    env,
    {},
    {
      requireUser: async () => v8Actor(),
      rpc: v8Rpc()
    }
  );
  const body = await response.json();
  assert.equal(response.status, 200);
  assert.equal(body.contract_version, 'INVOICE_ASYNC_BACKEND_V8');
  assert.equal(body.database_contract_ready, true);
  assert.equal(body.deployment_contract_ready, true);
  assert.equal(body.enabled_for_user, true);
  assert.ok(Object.values(body.feature_flags).every(value => value === true));

  const badEnv = v8Environment();
  const mismatch = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoice-async/capabilities'),
    badEnv,
    {},
    {
      requireUser: async () => v8Actor(),
      rpc: v8Rpc(undefined, v8DatabaseContract({ function_hash_manifest: 'f'.repeat(64) }))
    }
  );
  const mismatchBody = await mismatch.json();
  assert.equal(mismatchBody.database_contract_ready, false);
  assert.equal(mismatchBody.deployment_contract_ready, false);
  assert.equal(mismatchBody.enabled_for_user, false);
  assert.ok(Object.values(mismatchBody.feature_flags).every(value => value === false));
});

test('an unreleased local build identity cannot satisfy deployment readiness', async () => {
  const env = v8Environment({
    INVOICE_ASYNC_BUILD_ID: 'invoice-async-v8-local-uncommitted'
  });
  const result = validateQueueRuntimeConfiguration(env);
  assert.equal(result.ok, false);
  assert.ok(result.errors.includes('INVOICE_ASYNC_BUILD_ID_UNRELEASED'));
});

test('automatic Generate creates one V2 selection root rather than enumerating candidates', async () => {
  const env = v8Environment();
  const calls = [];
  const result = await runAutoInvoiceCycleAsync(env, {
    config: getInvoiceQueueRuntimeConfig(env),
    actorUserId: V8_ACTOR_ID,
    validateSystemActor: async () => true,
    rpc: async (name, args) => {
      calls.push({ name, args });
      if (name === 'invoice_batch_generate_candidates') {
        return {
          contract_version: 'INVOICE_BATCH_CANDIDATES_V2',
          action: 'GENERATE',
          mode: 'PAGE',
          snapshot: v8Snapshot('GENERATE'),
          rows: [],
          selection_summary: { selected_total: 250 }
        };
      }
      if (name === 'invoice_operation_start_batch') {
        assert.equal(args.p_commands.length, 1);
        assert.equal(args.p_commands[0].command_type, 'GENERATE_SELECTED');
        assert.equal(args.p_commands[0].selection_contract.contract_version, 'INVOICE_BATCH_SELECTION_ROOT_V2');
        return [{
          command_no: 1,
          accepted: true,
          created: true,
          status: 'QUEUED',
          operation_id: '00000000-0000-4000-8000-000000000090'
        }];
      }
      if (name === 'invoice_work_claim_batch') return [];
      throw new Error(`Unexpected RPC ${name}`);
    }
  });
  assert.equal(result.ok, true);
  assert.equal(result.submitted, 250);
  assert.equal(calls.filter(call => call.name === 'invoice_operation_start_batch').length, 1);
});

test('Generate and Issue confirmation reject a missing signed snapshot before root creation', async () => {
  for (const action of ['GENERATE', 'ISSUE']) {
    const suffix = action === 'GENERATE' ? 'batch-generate' : 'batch-issue';
    let started = false;
    const body = {
      selection_contract: {
        contract_version: 'INVOICE_BATCH_SELECTION_ROOT_V2',
        query: v8Query(action, { snapshot: null }),
        selection: v8Selection()
      },
      command_token: `${action.toLowerCase()}-token`,
      ...(action === 'ISSUE' ? { deliver: false } : {})
    };
    const response = await handleInvoiceAsyncHttpRequest(
      new Request(`https://example.test/api/invoices/${suffix}/confirm`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body)
      }),
      v8Environment(),
      {},
      {
        requireUser: async () => v8Actor(),
        rpc: v8Rpc(async name => {
          if (name === 'invoice_operation_start_batch') started = true;
          throw new Error(`Unexpected RPC ${name}`);
        })
      }
    );
    assert.equal(response.status, 400);
    assert.equal((await response.json()).error, 'BATCH_SNAPSHOT_REQUIRED');
    assert.equal(started, false);
  }
});

test('V8 routes fail closed when the native document processor is not ready', async () => {
  const env = v8Environment({
    INVOICE_DOCUMENT_PROCESSOR: {
      fetch: async () => new Response(JSON.stringify({ ok: false }), { status: 503 })
    }
  });
  let candidateCalled = false;
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices/batch-generate/candidates', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        contract_version: 'INVOICE_BATCH_QUERY_V2',
        action: 'GENERATE',
        mode: 'PAGE',
        snapshot: null,
        page_size: 100,
        cursor: null,
        filters: {},
        sort: {},
        selection: v8Selection()
      })
    }),
    env,
    {},
    {
      requireUser: async () => v8Actor(),
      rpc: v8Rpc(async name => {
        if (name === 'invoice_batch_generate_candidates') candidateCalled = true;
        throw new Error(`Unexpected RPC ${name}`);
      })
    }
  );
  assert.equal(response.status, 503);
  assert.equal((await response.json()).error, 'INVOICE_ASYNC_TEMPORARILY_UNAVAILABLE');
  assert.equal(candidateCalled, false);
});

test('invoice mail preparation contains no legacy PDF rendering fallback', () => {
  const source = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
  const start = source.indexOf('async function buildEmailPayloadFromOutboxRow');
  const end = source.indexOf('async function limitOrLinkAttachments', start);
  assert.ok(start >= 0 && end > start);
  const emailPreparation = source.slice(start, end);
  assert.equal(emailPreparation.includes('ensureInvoicePdf('), false);
  assert.equal(emailPreparation.includes('invoice_pdf_r2_key'), false);
  assert.ok(emailPreparation.includes('INVOICE_ATTACHMENT_EXACT_DESCRIPTOR_REQUIRED'));
  assert.ok(emailPreparation.includes('purpose=eq.FINAL_ISSUE&status=eq.READY'));
});

test('invoice summary projection uses only installed document authority and exposes no storage key', () => {
  const source = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
  const start = source.indexOf('async function handleListInvoices');
  const end = source.indexOf('\nasync function ', start + 1);
  assert.ok(start >= 0 && end > start);
  const listHandler = source.slice(start, end);
  assert.equal(listHandler.includes("'last_issue_error_json'"), false);
  assert.equal(listHandler.includes("'invoice_pdf_r2_key'"), false);
  assert.ok(listHandler.includes("'preview_document_version_id'"));
  assert.ok(listHandler.includes("'issued_document_version_id'"));
  assert.ok(listHandler.includes("'active_document_operation_id'"));
  assert.ok(listHandler.includes("'active_issue_operation_id'"));
  assert.ok(listHandler.includes('attachment_expected'));
  assert.ok(listHandler.includes('attachment_ready'));
  assert.ok(listHandler.includes('attachment_state'));
  assert.ok(listHandler.includes('&timesheet_id=not.is.null'));
});
