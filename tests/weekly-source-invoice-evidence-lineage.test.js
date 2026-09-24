import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createHash, webcrypto } from 'node:crypto';
import test from 'node:test';

// Exercise the shipped helper with a deterministic PostgREST stub. The source
// is extracted from the Worker so this cannot pass against a parallel copy.
const worker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const start = worker.indexOf('async function weeklySourceInvoiceEvidenceRows(');
const end = worker.indexOf('\nasync function handleInvoiceSourceEvidenceDownload(', start);
assert.ok(start > 0 && end > start);
const loadHelper = (sbFetch) => new Function('sbFetch',
  `${worker.slice(start, end)}\nreturn weeklySourceInvoiceEvidenceRows;`)(sbFetch);
const downloadEnd = worker.indexOf('\nasync function handleGetInvoice(', end);
assert.ok(downloadEnd > end);
const loadDownload = (dependencies) => new Function(...Object.keys(dependencies),
  `${worker.slice(end, downloadEnd)}\nreturn handleInvoiceSourceEvidenceDownload;`)(...Object.values(dependencies));

const invoiceA = 'a0000000-0000-4000-8000-000000000001';
const invoiceB = 'a0000000-0000-4000-8000-000000000002';
const presentationA = 'b0000000-0000-4000-8000-000000000001';
const presentationB = 'b0000000-0000-4000-8000-000000000002';
const revisionA = 'c0000000-0000-4000-8000-000000000001';
const revisionB = 'c0000000-0000-4000-8000-000000000002';
const uploadA = 'd0000000-0000-4000-8000-000000000001';
const uploadB = 'd0000000-0000-4000-8000-000000000002';

const tables = {
  weekly_source_invoice_line_bindings: [
    { invoice_id: invoiceA, presentation_line_id: presentationA, state: 'CURRENT' },
    { invoice_id: invoiceA, presentation_line_id: presentationA, state: 'CURRENT' },
    { invoice_id: invoiceB, presentation_line_id: presentationB, state: 'CURRENT' },
    { invoice_id: invoiceA, presentation_line_id: presentationB, state: 'SUPERSEDED' }
  ],
  weekly_source_invoice_presentation_lines: [
    { id: presentationA, final_revision_id: revisionA },
    { id: presentationB, final_revision_id: revisionB }
  ],
  weekly_source_final_revisions: [
    { id: revisionA, upload_id: uploadA },
    { id: revisionB, upload_id: uploadB }
  ],
  weekly_source_uploads: [
    { id: uploadA, original_filename: 'report-a.xlsx', content_sha256: 'ab'.repeat(32),
      byte_count: 100, uploaded_at_utc: '2026-09-23T12:00:00Z',
      file_metadata_json: { source_file_r2_key: 'files/20260923/a.xlsx' } },
    { id: uploadB, original_filename: 'report-b.xlsx', content_sha256: 'cd'.repeat(32),
      byte_count: 200, uploaded_at_utc: '2026-09-23T13:00:00Z',
      file_metadata_json: { source_file_r2_key: 'files/20260923/b.xlsx' } }
  ]
};

function stub(database) {
  return async (_env, rawUrl) => {
    const url = new URL(rawUrl);
    const table = url.pathname.split('/').at(-1);
    const filters = [...url.searchParams.entries()].filter(([name]) => !['select', 'limit', 'offset'].includes(name));
    const rows = (database[table] || []).filter((row) => filters.every(([name, value]) => {
      if (value.startsWith('eq.')) return String(row[name]) === value.slice(3);
      if (value.startsWith('in.(')) return value.slice(4, -1).split(',').includes(String(row[name]));
      throw new Error(`Unexpected filter ${name}=${value}`);
    }));
    return { rows: rows.slice(Number(url.searchParams.get('offset') || 0),
      Number(url.searchParams.get('offset') || 0) + Number(url.searchParams.get('limit') || 1000)) };
  };
}

test('invoice evidence follows only CURRENT bound presentation lines after a move', async () => {
  const helper = loadHelper(stub(tables));
  const a = await helper({ SUPABASE_URL: 'https://test.invalid' }, invoiceA);
  const b = await helper({ SUPABASE_URL: 'https://test.invalid' }, invoiceB);
  assert.deepEqual(a.map((row) => row.upload_id), [uploadA]);
  assert.deepEqual(b.map((row) => row.upload_id), [uploadB]);
  assert.equal(a[0]._source_file_r2_key, 'files/20260923/a.xlsx');
  assert.equal(a[0].filename, 'report-a.xlsx');
});

test('incomplete invoice-to-upload lineage fails closed', async () => {
  const incomplete = { ...tables,
    weekly_source_final_revisions: tables.weekly_source_final_revisions.slice(1) };
  const helper = loadHelper(stub(incomplete));
  await assert.rejects(() => helper({ SUPABASE_URL: 'https://test.invalid' }, invoiceA),
    /WEEKLY_SOURCE_EVIDENCE_LINEAGE_INCOMPLETE/);
});

test('an invalid invoice id cannot select any report', async () => {
  const helper = loadHelper(stub(tables));
  await assert.rejects(() => helper({ SUPABASE_URL: 'https://test.invalid' }, 'all'),
    /INVALID_INVOICE_ID/);
});

test('download rejects a different upload and serves only digest-matching linked bytes', async () => {
  const bytes = new TextEncoder().encode('original-source-report');
  const digest = createHash('sha256').update(bytes).digest('hex');
  const requests = [];
  const evidence = [{ upload_id: uploadA, filename: 'report-a.xlsx',
    byte_count: bytes.length, _source_sha256: digest,
    _source_file_r2_key: 'files/20260923/a.xlsx' }];
  const download = loadDownload({
    requireUser: async () => ({ id: 'e0000000-0000-4000-8000-000000000001' }),
    sbRpc: async (_env, name, request) => {
      requests.push({ name, request });
      return { is_weekly_source_invoice: true };
    },
    unwrapRpcJsonb: (result) => result,
    weeklySourceInvoiceEvidenceRows: async () => evidence,
    withCORS: (_env, _req, response) => response,
    unauthorized: () => new Response('', { status: 401 }),
    notFound: () => new Response('', { status: 404 }),
    serverError: () => new Response('', { status: 500 }),
    isOfficeFileDownloadKeyAllowed: (_env, key) => key.startsWith('files/'),
    crypto: webcrypto,
    Response
  });
  const env = { R2_BUCKET: { get: async (key) => key === 'files/20260923/a.xlsx'
    ? { size: bytes.length, arrayBuffer: async () => bytes.buffer } : null } };
  const denied = await download(env, new Request('https://test.invalid'), invoiceA, uploadB);
  assert.equal(denied.status, 404);
  const allowed = await download(env, new Request('https://test.invalid'), invoiceA, uploadA);
  assert.equal(allowed.status, 200);
  assert.deepEqual(new Uint8Array(await allowed.arrayBuffer()), bytes);
  assert.match(allowed.headers.get('Content-Disposition'), /report-a\.xlsx/);
  assert.equal(allowed.headers.get('Cache-Control'), 'no-store');
  assert.equal(requests.length, 2);
  assert.ok(requests.every((entry) => entry.name === 'weekly_source_invoice_edit_context_v1'
    && entry.request.p_request.invoice_id === invoiceA));
});
