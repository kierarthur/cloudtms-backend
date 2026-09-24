import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createHash, webcrypto } from 'node:crypto';
import test from 'node:test';

const worker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const sql = readFileSync(new URL('../supabase/repeatable/24092026_1440_weekly_source_invoice_evidence_lookup_v1.sql', import.meta.url), 'utf8');
const acl = readFileSync(new URL('../supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql', import.meta.url), 'utf8');
const aclVerification = readFileSync(new URL('../supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql', import.meta.url), 'utf8');
const start = worker.indexOf('async function weeklySourceInvoiceEvidenceRows(');
const end = worker.indexOf('\nasync function handleInvoiceSourceEvidenceDownload(', start);
assert.ok(start > 0 && end > start);
const helper = (rpc) => new Function('sbRpc', 'unwrapRpcJsonb',
  `${worker.slice(start, end)}\nreturn weeklySourceInvoiceEvidenceRows;`)(rpc, value => value);
const downloadEnd = worker.indexOf('\nasync function handleGetInvoice(', end);
assert.ok(downloadEnd > end);
const loadDownload = dependencies => new Function(...Object.keys(dependencies),
  `${worker.slice(end, downloadEnd)}\nreturn handleInvoiceSourceEvidenceDownload;`)(...Object.values(dependencies));

const invoice = 'a0000000-0000-4000-8000-000000000001';
const otherInvoice = 'a0000000-0000-4000-8000-000000000002';
const upload = 'd0000000-0000-4000-8000-000000000001';
const actor = 'e0000000-0000-4000-8000-000000000001';

test('exact service-only SQL links only CURRENT invoice bindings to a final revision and upload', () => {
  assert.match(sql, /weekly_source_query_require_service_v1\(\)/);
  assert.match(sql, /weekly_source_office_authority_v1\(/);
  assert.match(sql, /binding\.invoice_id=v_invoice_id and binding\.state='CURRENT'/);
  assert.match(sql, /revision\.id=presentation\.final_revision_id/);
  assert.match(sql, /upload\.id=revision\.upload_id/);
  assert.match(sql, /v_binding_count<>v_joined_count/);
  assert.match(sql, /revoke all on function public\.weekly_source_invoice_evidence_v1\(jsonb\) from public,anon,authenticated/);
  assert.match(sql, /grant execute on function public\.weekly_source_invoice_evidence_v1\(jsonb\) to service_role/);
  assert.doesNotMatch(sql, /grant\s+select\s+on\s+public\.weekly_source_/i);
  assert.match(acl, /\('public\.weekly_source_invoice_evidence_v1\(jsonb\)'\)/);
  assert.match(aclVerification, /'public\.weekly_source_invoice_evidence_v1\(jsonb\)'/);
});

test('Worker accepts only the actor-scoped RPC result and never queries protected tables', async () => {
  const calls = [];
  const get = helper(async (_env, name, payload) => {
    calls.push({ name, payload });
    return { is_weekly_source_invoice: true, evidence: [{ upload_id: upload,
      filename: 'source.xlsx', uploaded_at_utc: '2026-09-23T12:00:00Z', byte_count: 123,
      content_sha256: 'ab'.repeat(32), source_file_r2_key: 'files/20260923/a.xlsx' }] };
  });
  const rows = await get({}, invoice, actor);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0], { name: 'weekly_source_invoice_evidence_v1',
    payload: { p_request: { actor_user_id: actor, invoice_id: invoice } } });
  assert.deepEqual(rows.map(row => row.upload_id), [upload]);
  assert.equal(rows[0]._source_file_r2_key, 'files/20260923/a.xlsx');
  assert.equal(rows[0].download_available, true);
  await assert.rejects(() => get({}, 'all', actor), /INVALID_INVOICE_EVIDENCE_REQUEST/);
  await assert.rejects(() => get({}, invoice, 'all'), /INVALID_INVOICE_EVIDENCE_REQUEST/);
  await assert.rejects(() => helper(async () => ({ is_weekly_source_invoice: false, evidence: [] }))({}, otherInvoice, actor),
    /WEEKLY_SOURCE_EVIDENCE_LINEAGE_INCOMPLETE/);
});

test('download denies unrelated upload and returns only byte-identical report', async () => {
  const bytes = new TextEncoder().encode('original-source-report');
  const digest = createHash('sha256').update(bytes).digest('hex');
  const requests = [];
  const evidence = [{ upload_id: upload, filename: 'report-a.xlsx',
    byte_count: bytes.length, _source_sha256: digest,
    _source_file_r2_key: 'files/20260923/a.xlsx' }];
  const download = loadDownload({
    requireUser: async () => ({ id: actor }),
    sbRpc: async (_env, name, request) => { requests.push({ name, request });
      return { is_weekly_source_invoice: true }; },
    unwrapRpcJsonb: result => result,
    weeklySourceInvoiceEvidenceRows: async () => evidence,
    withCORS: (_env, _req, response) => response,
    unauthorized: () => new Response('', { status: 401 }),
    notFound: () => new Response('', { status: 404 }),
    serverError: () => new Response('', { status: 500 }),
    isOfficeFileDownloadKeyAllowed: (_env, key) => key.startsWith('files/'),
    crypto: webcrypto, Response,
  });
  const env = { R2_BUCKET: { get: async key => key === 'files/20260923/a.xlsx'
    ? { size: bytes.length, arrayBuffer: async () => bytes.buffer } : null } };
  assert.equal((await download(env, new Request('https://test.invalid'), invoice, otherInvoice)).status, 404);
  const allowed = await download(env, new Request('https://test.invalid'), invoice, upload);
  assert.equal(allowed.status, 200);
  assert.deepEqual(new Uint8Array(await allowed.arrayBuffer()), bytes);
  assert.match(allowed.headers.get('Content-Disposition'), /report-a\.xlsx/);
  assert.equal(allowed.headers.get('Cache-Control'), 'no-store');
  assert.equal(requests.length, 2);
  assert.ok(requests.every(entry => entry.name === 'weekly_source_invoice_edit_context_v1'
    && entry.request.p_request.invoice_id === invoice));
});
