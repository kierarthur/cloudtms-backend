import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';

const workerPath = new URL('../src/worker.js', import.meta.url);
const receiptUrl = new URL('../src/receipt-contract.js', import.meta.url).href;
const workerSource = (await readFile(workerPath, 'utf8'))
  .replace("import { Container, getContainer } from '@cloudflare/containers';", 'class Container {}\nconst getContainer = () => { throw new Error("CONTAINER_NOT_AVAILABLE_IN_UNIT_TEST"); };')
  .replace("'./receipt-contract.js'", JSON.stringify(receiptUrl));
const { invoiceDocumentProcessorInternals } = await import(`data:text/javascript;base64,${Buffer.from(workerSource).toString('base64')}`);

const {
  findInputDescriptors,
  resolveR2Inputs,
  framedBody,
  resultIdentity,
  putImmutableProcessorArtifact,
  classifyError
} = invoiceDocumentProcessorInternals;

function objectFromBytes(bytes, metadata = {}) {
  const data = Uint8Array.from(bytes);
  return {
    size: data.byteLength,
    customMetadata: metadata,
    httpMetadata: { contentType: 'application/pdf' },
    body: new ReadableStream({ pull(controller) { controller.enqueue(data); controller.close(); } })
  };
}

test('V4 action descriptors use inspected original identity and verify existing candidate', () => {
  assert.deepEqual(findInputDescriptors('ASSET_NORMALISE', {
    expected_original_r2_key: 'original/source.pdf',
    expected_original_sha256: 'a'.repeat(64),
    expected_original_size_bytes: 123,
    expected_original_media_type: 'application/pdf'
  }), [{ r2_key: 'original/source.pdf', media_type: 'application/pdf', size_bytes: 123, sha256: 'a'.repeat(64) }]);
  assert.deepEqual(findInputDescriptors('DOCUMENT_VERIFY', {
    final_candidate_key: 'final/candidate.pdf',
    final_candidate_sha256: 'b'.repeat(64),
    final_candidate_size_bytes: 456
  }), [{ r2_key: 'final/candidate.pdf', sha256: 'b'.repeat(64), size_bytes: 456 }]);
});

test('R2 inputs reject duplicates, missing objects, and stored identity mismatch before transfer', async () => {
  const bucket = { async get(key) { return key === 'one.pdf' ? objectFromBytes([1, 2], { sha256: 'a'.repeat(64) }) : null; } };
  await assert.rejects(() => resolveR2Inputs(bucket, [{ r2_key: 'one.pdf' }, { r2_key: 'one.pdf' }]), /INPUT_R2_KEY_DUPLICATE/);
  await assert.rejects(() => resolveR2Inputs(bucket, [{ r2_key: 'missing.pdf' }]), /INPUT_R2_OBJECT_MISSING/);
  await assert.rejects(() => resolveR2Inputs(bucket, [{ r2_key: 'one.pdf', size_bytes: 3 }]), /INPUT_SIZE_MISMATCH/);
  await assert.rejects(() => resolveR2Inputs(bucket, [{ r2_key: 'one.pdf', size_bytes: 2, sha256: 'b'.repeat(64) }]), /INPUT_STORED_HASH_MISMATCH/);
});

test('framed request streams prefix, header, and body incrementally and detects premature EOF', async () => {
  const input = { object: objectFromBytes([1, 2, 3]), header: { r2_key: 'one.pdf' } };
  const reader = framedBody({ action: 'PDF_MERGE' }, [input]).getReader();
  const chunks = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  assert.equal(chunks.length, 3);
  assert.equal(chunks[0].byteLength, 8);
  assert.deepEqual([...chunks[2]], [1, 2, 3]);
  const short = objectFromBytes([1, 2]); short.size = 3;
  await assert.rejects(async () => {
    const stream = framedBody({ action: 'PDF_MERGE' }, [{ object: short }]);
    const r = stream.getReader();
    while (!(await r.read()).done) {}
  }, /INPUT_PREMATURE_EOF/);
});

test('immutable processor writes use full hash and reject conflicting existing objects', async () => {
  const identity = { chunk_id: '00000000-0000-4000-8000-000000000001', fence_token: 2, plan_generation: 1, document_version_id: '00000000-0000-4000-8000-000000000002', action: 'PDF_MERGE', processor_policy_version: 'INVOICE_PROCESSOR_LIMITS_V4' };
  const metadata = { sha256: 'c'.repeat(64), size_bytes: 3, page_count: 1, processor_version: 'cloudtms-native-v4' };
  let putOptions;
  const bucket = { async head() { return null; }, async put(key, body, options) { assert.ok(key.includes('c'.repeat(64))); putOptions = options; return {}; } };
  await putImmutableProcessorArtifact(bucket, `immutable/${metadata.sha256}.pdf`, new Uint8Array([1, 2, 3]), identity, metadata);
  assert.equal(putOptions.onlyIf.etagDoesNotMatch, '*');
  assert.equal(putOptions.customMetadata.sha256, metadata.sha256);
  const conflict = { async head() { return { size: 3, customMetadata: { sha256: 'd'.repeat(64) } }; } };
  await assert.rejects(() => putImmutableProcessorArtifact(conflict, 'immutable/existing.pdf', new Uint8Array([1]), identity, metadata), /IMMUTABLE_ARTIFACT_KEY_CONFLICT/);
});

test('processor error categories distinguish timeout, input, policy, identity and bugs', () => {
  assert.equal(classifyError(new Error('PROCESSOR_TIMEOUT')), 'PROCESSOR_TIMEOUT');
  assert.equal(classifyError(new Error('ASSET_MEDIA_TYPE_UNSUPPORTED')), 'PERMANENT_INPUT');
  assert.equal(classifyError(new Error('MERGE_INPUT_LIMIT_EXCEEDED')), 'POLICY_VIOLATION');
  assert.equal(classifyError(new Error('INPUT_HASH_MISMATCH')), 'IDENTITY_MISMATCH');
  assert.equal(classifyError(new Error('unexpected')), 'PROCESSOR_BUG');
  assert.equal(resultIdentity({ action: 'DOCUMENT_VERIFY', processor_policy_version: 'INVOICE_PROCESSOR_LIMITS_V4' }).output_prefix, undefined);
});