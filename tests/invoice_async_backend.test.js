import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import {
  createInvoiceDocumentAccessToken,
  verifyInvoiceDocumentAccessToken
} from '../broker/src/invoice-document-access.js';
import {
  getInvoiceQueueRuntimeConfig,
  invoiceQueueRuntimeInternals,
  processInvoiceDatabaseChunksBatch,
  processInvoiceDocumentChunksBatch,
  drainInvoiceOperations,
  runInvoiceReconciliationCycle
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
  const tampered = `${token.slice(0, -1)}${token.endsWith('a') ? 'b' : 'a'}`;
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
      vat_registration_number: 'GB123456789'
    },
    customer: { legal_name: 'Customer "A"', billing_address: ['2 Customer Road'] },
    references: {},
    lines: [{
      row_key: 'line-1',
      source_invoice_line_id: '00000000-0000-4000-8000-000000000001',
      source_key: 'source-1',
      description: '<unsafe>',
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
    payment: {},
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
    payment: {},
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

test('generation HTTP route submits once and returns before queued work drains', async () => {
  const calls = [];
  const background = [];
  const rpc = async (name, args) => {
    calls.push({ name, args });
    if (name === 'invoice_operation_start_batch') {
      return [{
        operation_id: '00000000-0000-4000-8000-000000000099',
        status: 'QUEUED',
        accepted: true,
        created: true,
      }];
    }
    if (name === 'invoice_work_claim_batch') return [];
    throw new Error(`Unexpected RPC ${name}`);
  };
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
  const response = await handleInvoiceAsyncHttpRequest(request, {
    INVOICE_ASYNC_PIPELINE_ENABLED: 'true',
    INVOICE_DOCUMENT_PROCESSOR_ENABLED: 'false',
    INVOICE_QUEUE_CONTINUATION_DELAY_MS: '0',
    INVOICE_ASYNC_ALLOWED_USER_IDS: '00000000-0000-4000-8000-000000000010'
  }, {
    waitUntil(promise) { background.push(promise); }
  }, {
    requireUser: async () => ({ id: '00000000-0000-4000-8000-000000000010', role: 'admin', active: true }),
    rpc
  });
  assert.equal(response.status, 202);
  assert.equal(calls.filter(call => call.name === 'invoice_operation_start_batch').length, 1);
  assert.equal(background.length, 1);
  await Promise.all(background);
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
        { method: 'POST', body: '{}' }
      ),
      {
        INVOICE_ASYNC_PIPELINE_ENABLED: 'true',
        SUPABASE_URL: 'https://supabase.test',
        SUPABASE_SERVICE_ROLE_KEY: 'test-only',
        INVOICE_ASYNC_ALLOWED_USER_IDS: '00000000-0000-4000-8000-000000000010'
      },
      {},
      {
        requireUser: async () => ({
          id: '00000000-0000-4000-8000-000000000010',
          role: 'admin',
          active: true
        }),
        rpc: async name => {
          if (name === 'invoice_detail_get') return { invoice: { id: '00000000-0000-4000-8000-000000000021', status: 'DRAFT' } };
          assert.equal(name, 'invoice_operation_start_batch');
          return [{
            accepted: true,
            status: 'READY',
            reused_ready: true,
            document_version_id: '00000000-0000-4000-8000-000000000020'
          }];
        }
      }
    );
    const body = await response.json();
    assert.equal(response.status, 200, JSON.stringify(body));
    assert.equal(body.ready, true);
    assert.equal(body.document_version.sha256, 'a'.repeat(64));
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
      {
        INVOICE_ASYNC_PIPELINE_ENABLED: 'true',
        SUPABASE_URL: 'https://supabase.test',
        SUPABASE_SERVICE_ROLE_KEY: 'test-only',
        SESSION_TOKEN_SECRET: 'test-session-secret-with-more-than-thirty-two-characters',
        INVOICE_ASYNC_ALLOWED_USER_IDS: '00000000-0000-4000-8000-000000000010'
      },
      {},
      {
        requireUser: async () => ({
          id: '00000000-0000-4000-8000-000000000010',
          role: 'admin'
        }),
        rpc: async () => { throw new Error('Unified cursor listing must not use the offset RPC'); }
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

test('non-admin async candidate GET is forbidden before any service-role read', async () => {
  const id = '00000000-0000-4000-8000-000000000010';
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices/batch-generate/candidates'),
    { INVOICE_ASYNC_PIPELINE_ENABLED: 'true', INVOICE_ASYNC_ALLOWED_USER_IDS: id },
    {},
    { requireUser: async () => ({ id, role: 'user', active: true }), rpc: async () => assert.fail('RPC must not be called') }
  );
  assert.equal(response.status, 403);
});

test('an all-rejected command batch does not return accepted 202', async () => {
  const id = '00000000-0000-4000-8000-000000000010';
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ timesheet_ids: ['00000000-0000-4000-8000-000000000011'] }) }),
    { INVOICE_ASYNC_PIPELINE_ENABLED: 'true', INVOICE_ASYNC_ALLOWED_USER_IDS: id },
    {},
    { requireUser: async () => ({ id, role: 'admin', active: true }), rpc: async () => [{ accepted: false, error: 'SOURCE_CHANGED' }] }
  );
  assert.equal(response.status, 409);
  const body = await response.json();
  assert.equal(body.accepted_count, 0);
  assert.equal(body.rejected_count, 1);
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
      new Request(`https://example.test/api/invoices/${invoice}/render`, { method: 'POST', body: '{}' }),
      { INVOICE_ASYNC_PIPELINE_ENABLED: 'true', INVOICE_ASYNC_ALLOWED_USER_IDS: actor, SUPABASE_URL: 'https://supabase.test', SUPABASE_SERVICE_ROLE_KEY: 'test-only' },
      {},
      { requireUser: async () => ({ id: actor, role: 'admin', active: true }), rpc: async name => {
        if (name === 'invoice_detail_get') return { invoice: { id: invoice, status: 'ISSUED', issued_document_version_id: version } };
        if (name === 'invoice_operation_start_batch') started = true;
        return [];
      } }
    );
    assert.equal(response.status, 200);
    assert.equal((await response.json()).document_version.purpose, 'FINAL_ISSUE');
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
    authorisation: {
      authorised: true,
      name: 'Authoriser',
      role: 'Manager',
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
    {
      INVOICE_ASYNC_PIPELINE_ENABLED: 'true',
      INVOICE_DOCUMENT_PROCESSOR_ENABLED: 'true',
      INVOICE_ASYNC_ALLOWED_USER_IDS: actor
    },
    {},
    {
      requireUser: async () => ({ id: actor, role: 'admin', active: true }),
      rpc: async () => assert.fail('Issue RPC must not run without expected revision')
    }
  );
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error, 'EXPECTED_INVOICE_REVISION_REQUIRED');
});

test('batch generation re-resolves selected scopes and correlates start results by command_no', async () => {
  const actor = '00000000-0000-4000-8000-000000000010';
  const timesheet = '00000000-0000-4000-8000-000000000011';
  const scopeKey = 'scope:client:week';
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
        rows: [{
          scope_key: scopeKey,
          canonical_source_revision: 'revision-1',
          canonical_command: {
            source_ids: ['00000000-0000-4000-8000-999999999999'],
            actor_user_id: '00000000-0000-4000-8000-999999999998'
          }
        }]
      })
    }),
    {
      INVOICE_ASYNC_PIPELINE_ENABLED: 'true',
      INVOICE_DOCUMENT_PROCESSOR_ENABLED: 'true',
      INVOICE_ASYNC_ALLOWED_USER_IDS: actor
    },
    {
      waitUntil(promise) {
        background.push(promise);
      }
    },
    {
      requireUser: async () => ({ id: actor, role: 'admin', active: true }),
      rpc: async (name, args) => {
        calls.push({ name, args });
        if (name === 'invoice_batch_generate_candidates') {
          assert.deepEqual(args.p_scope_keys, [scopeKey]);
          return [{
            client_id: '00000000-0000-4000-8000-000000000012',
            groups: [{
              group_key: scopeKey,
              canonical_source_revision: 'revision-1',
              command_payload: {
                command_type: 'GENERATE_SELECTED',
                group_key: scopeKey,
                canonical_source_ids: [timesheet],
                canonical_source_members: [{
                  source_type: 'TIMESHEET',
                  source_id: timesheet
                }],
                source_revision: 'revision-1'
              }
            }]
          }];
        }
        if (name === 'invoice_operation_start_batch') {
          assert.deepEqual(args.p_commands[0].source_ids, [timesheet]);
          assert.equal(args.p_commands[0].actor_user_id, undefined);
          return [{
            command_no: 1,
            accepted: true,
            created: true,
            status: 'QUEUED',
            operation_id: '00000000-0000-4000-8000-000000000013'
          }];
        }
        if (name === 'invoice_work_claim_batch') return [];
        throw new Error(`Unexpected RPC ${name}`);
      }
    }
  );
  assert.equal(response.status, 202);
  const result = await response.json();
  assert.equal(result.results_invoices[0].scope_key, scopeKey);
  assert.equal(calls.filter(call => call.name === 'invoice_operation_start_batch').length, 1);
  await Promise.all(background);
});

test('invoice batch filters and sort are strictly allowlisted and canonical', () => {
  const query = new URLSearchParams([
    ['client_ids', '00000000-0000-4000-8000-000000000002,00000000-0000-4000-8000-000000000001'],
    ['client_ids', '00000000-0000-4000-8000-000000000001'],
    ['week_endings', '2026-07-26'],
    ['status_codes', 'ready'],
    ['allow_early', 'true'],
    ['display_mode', 'ready'],
    ['group_preset', 'client_week_candidate'],
    ['sort_key', 'total_inc_vat'],
    ['sort_direction', 'desc'],
    ['page_size', '50']
  ]);
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
    display_mode: 'READY'
  });
  assert.deepEqual(invoiceAsyncHttpInternals.normaliseInvoiceBatchSort(query, 'GENERATE'), {
    group_preset: 'CLIENT_WEEK_CANDIDATE',
    sort_key: 'TOTAL_INC_VAT',
    sort_direction: 'DESC'
  });
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchFilters({ filters: { arbitrary_sql: 'x' } }, 'GENERATE'),
    /BATCH_FILTER_FIELD_UNSUPPORTED/
  );
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchSort({ sort: { sort_key: 'invoice_number' } }, 'GENERATE'),
    /INVOICE_BATCH_SORT_KEY_INVALID/
  );
});

test('invoice batch selection ledger accepts only ordered V1 selector rules', () => {
  const contract = invoiceAsyncHttpInternals.normaliseInvoiceBatchSelectionRules({
    selection: {
      contract_version: 'INVOICE_BATCH_SELECTION_V1',
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
        }
      ]
    }
  });
  assert.equal(contract.rules[0].action, 'EXCLUDE');
  assert.equal(contract.rules[1].selector.type, 'WEEK_CLIENT');
  assert.throws(
    () => invoiceAsyncHttpInternals.normaliseInvoiceBatchSelectionRules({
      contract_version: 'INVOICE_BATCH_SELECTION_V1',
      mode: 'IMPLICIT_ALL',
      default_selected: true,
      rules: [
        { sequence: 2, action: 'EXCLUDE', selector: { type: 'ROW', selection_key: 'b' } },
        { sequence: 1, action: 'EXCLUDE', selector: { type: 'ROW', selection_key: 'a' } }
      ]
    }),
    /BATCH_SELECTION_RULE_SEQUENCE_INVALID/
  );
});

test('invoice batch cursor is HMAC protected and bound to action, filter and sort', async () => {
  const env = { SESSION_TOKEN_SECRET: 'test-session-secret-with-more-than-thirty-two-characters' };
  const sort = {
    group_preset: 'WEEK_CLIENT_CANDIDATE',
    sort_key: 'WEEK_ENDING_DATE',
    sort_direction: 'ASC'
  };
  const token = await invoiceAsyncHttpInternals.encodeInvoiceBatchCursor(env, {
    action: 'GENERATE',
    snapshot_at_utc: '2026-07-26T12:00:00.000Z',
    filter_hash: 'a'.repeat(64),
    sort,
    next_cursor_values: {
      after_selection_key: 'group:1',
      after_sort_date: '2026-07-26'
    }
  });
  const decoded = await invoiceAsyncHttpInternals.decodeInvoiceBatchCursor(env, token, {
    action: 'GENERATE',
    filter_hash: 'a'.repeat(64),
    sort
  });
  assert.equal(decoded.cursor.after_selection_key, 'group:1');
  await assert.rejects(
    () => invoiceAsyncHttpInternals.decodeInvoiceBatchCursor(env, token, {
      action: 'GENERATE',
      filter_hash: 'b'.repeat(64),
      sort
    }),
    /BATCH_CURSOR_FILTER_MISMATCH/
  );
  const tampered = `${token.slice(0, -1)}${token.endsWith('a') ? 'b' : 'a'}`;
  await assert.rejects(
    () => invoiceAsyncHttpInternals.decodeInvoiceBatchCursor(env, tampered, {
      action: 'GENERATE',
      filter_hash: 'a'.repeat(64),
      sort
    }),
    /BATCH_CURSOR_INVALID/
  );
});

test('invoice batch DB filter hash matches the installed PostgreSQL jsonb contract', async () => {
  const filters = invoiceAsyncHttpInternals.normaliseInvoiceBatchFilters(new URLSearchParams(), 'GENERATE');
  const sort = invoiceAsyncHttpInternals.normaliseInvoiceBatchSort(new URLSearchParams(), 'GENERATE');
  const first = await invoiceAsyncHttpInternals.hashInvoiceBatchQuery(
    'GENERATE', filters, sort, '2026-07-26T12:00:00.000Z', { db_candidate_filter_hash: true }
  );
  const second = await invoiceAsyncHttpInternals.hashInvoiceBatchQuery(
    'GENERATE', filters, sort, '2026-07-27T12:00:00.000Z', { db_candidate_filter_hash: true }
  );
  const cursorBoundFirst = await invoiceAsyncHttpInternals.hashInvoiceBatchQuery(
    'GENERATE', filters, sort, '2026-07-26T12:00:00.000Z'
  );
  const cursorBoundSecond = await invoiceAsyncHttpInternals.hashInvoiceBatchQuery(
    'GENERATE', filters, sort, '2026-07-27T12:00:00.000Z'
  );
  assert.match(first, /^[0-9a-f]{64}$/);
  assert.equal(first, second);
  assert.notEqual(cursorBoundFirst, cursorBoundSecond);
});

test('V1 candidate envelopes remain typed and retain paging metadata', () => {
  const parsed = invoiceAsyncHttpInternals.candidateGroupsFromRpc({
    contract_version: 'INVOICE_BATCH_CANDIDATES_V1',
    action: 'GENERATE',
    rows: [{ selection_key: 'group:1' }],
    page: { has_more: true },
    totals: { all: 2 },
    facets: {},
    filter_hash: 'a'.repeat(64)
  });
  assert.equal(parsed.kind, 'V1');
  assert.equal(parsed.legacy, false);
  assert.equal(parsed.rows[0].selection_key, 'group:1');
  assert.equal(parsed.page.has_more, true);
});
test('paged candidate route returns V7 and signs the database keyset cursor', async () => {
  const actor = '00000000-0000-4000-8000-000000000010';
  const env = {
    INVOICE_ASYNC_PIPELINE_ENABLED: 'true',
    INVOICE_DOCUMENT_PROCESSOR_ENABLED: 'true',
    INVOICE_ASYNC_ALLOWED_USER_IDS: actor,
    SESSION_TOKEN_SECRET: 'test-session-secret-with-more-than-thirty-two-characters'
  };
  let capturedQuery = null;
  const response = await handleInvoiceAsyncHttpRequest(
    new Request('https://example.test/api/invoices/batch-generate/candidates?page_size=25&sort_key=client_name'),
    env,
    {},
    {
      requireUser: async () => ({ id: actor, role: 'admin', active: true }),
      rpc: async (name, args) => {
        assert.equal(name, 'invoice_batch_generate_candidates');
        capturedQuery = args.p_query;
        const filterHash = await invoiceAsyncHttpInternals.hashInvoiceBatchQuery(
          'GENERATE', args.p_query.filters, args.p_query.sort, args.p_query.snapshot_at_utc,
          { db_candidate_filter_hash: true }
        );
        return {
          contract_version: 'INVOICE_BATCH_CANDIDATES_V1', action: 'GENERATE', mode: 'PAGE',
          snapshot_at_utc: args.p_query.snapshot_at_utc,
          normalised_filter: args.p_query.filters,
          normalised_sort: args.p_query.sort,
          filter_hash: filterHash,
          rows: [{ selection_key: 'group:1' }],
          page: {
            page_size: 25, has_more: true,
            next_cursor_values: { after_selection_key: 'group:1', after_sort_text: 'client one' }
          },
          totals: { all: 2 }, facets: {},
          selection_seed: { mode: 'IMPLICIT_ALL', default_selected: true }
        };
      }
    }
  );
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('x-invoice-async-contract-version'), 'INVOICE_ASYNC_BACKEND_V7');
  assert.equal(capturedQuery.page_size, 25);
  assert.equal(capturedQuery.sort.sort_key, 'CLIENT_NAME');
  const body = await response.json();
  assert.ok(body.page.next_cursor);
  const decoded = await invoiceAsyncHttpInternals.decodeInvoiceBatchCursor(env, body.page.next_cursor, {
    action: 'GENERATE', filter_hash: body.filter_hash, sort: body.normalised_sort
  });
  assert.equal(decoded.cursor.after_selection_key, 'group:1');
});