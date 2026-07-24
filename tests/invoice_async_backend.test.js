import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createInvoiceDocumentAccessToken,
  verifyInvoiceDocumentAccessToken
} from '../broker/src/invoice-document-access.js';
import {
  getInvoiceQueueRuntimeConfig,
  invoiceQueueRuntimeInternals,
  processInvoiceDatabaseChunksBatch
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
  flattenLeafInputReceipts
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
    invoice_number: 'INV-1<script>',
    supplier: { name: 'Supplier & Co' },
    customer: { name: 'Customer "A"' },
    lines: [{ description: '<unsafe>', quantity: 1, unit_price: 10, net: 10 }],
    net_total: 10,
    vat_total: 2,
    gross_total: 12
  };
  const first = buildProfessionalInvoiceHtml(model);
  const second = buildProfessionalInvoiceHtml(model);
  assert.equal(first, second);
  assert.ok(first.includes('INV-1&lt;script&gt;'));
  assert.ok(!first.includes('<unsafe>'));
});

test('attachment index renders one logical row with physical page totals', () => {
  const html = buildAttachmentIndexHtml({
    display_rows: [{
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
    assert.equal(response.status, 200);
    const body = await response.json();
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
    assert.equal(url.pathname, '/rest/v1/invoice_operations');
    return new Response(JSON.stringify([{
      id: '00000000-0000-4000-8000-000000000030',
      operation_type: 'BUILD_DOCUMENT',
      entity_type: 'INVOICE',
      entity_id: '00000000-0000-4000-8000-000000000031',
      status: 'RUNNING',
      phase: 'RENDERING',
      created_at_utc: '2026-07-24T12:00:00.000Z',
      run_after_utc: null
    }]), {
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
        INVOICE_ASYNC_ALLOWED_USER_IDS: '00000000-0000-4000-8000-000000000010'
      },
      {},
      {
        requireUser: async () => ({
          id: '00000000-0000-4000-8000-000000000010',
          role: 'admin'
        }),
        rpc: async name => {
          assert.equal(name, 'outbox_unified_list');
          return {
            items: [{
              channel: 'EMAIL',
              outbox_id: '00000000-0000-4000-8000-000000000032',
              status: 'QUEUED',
              created_at_utc: '2026-07-24T11:00:00.000Z'
            }],
            total_count: 1
          };
        }
      }
    );
    assert.equal(response.status, 200);
    const body = await response.json();
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
  const ts = buildElectronicTimesheetHtml({ candidate_name: 'Worker', client_name: 'Client', site: 'Hospital', ward: 'Ward', daily_schedule_rows: [{ date: '2026-07-24', worked_start: '08:00', worked_end: '16:00', break_minutes: 30, hours: 7.5 }] });
  assert.ok(ts.includes('Hospital'));
  assert.ok(ts.includes('7.5'));
  const hr = buildHealthRosterSupportHtml({ rows: [{ worker: 'Worker', assignment: 'Assignment', secret_future_key: 'must-not-render' }] });
  const nhsp = buildNhspSupportHtml({ rows: [{ worker: 'Worker', nhsp_shift_id: 'SHIFT-1', secret_future_key: 'must-not-render' }] });
  const higher = buildHigherRateSupportHtml({ rows: [{ worker_source: 'Worker', applied_rate: '25.00', secret_future_key: 'must-not-render' }] });
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
