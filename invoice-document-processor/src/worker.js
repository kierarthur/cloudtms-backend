import { Container, getContainer } from '@cloudflare/containers';
import {
  buildMergeReceipt,
  flattenLeafInputReceipts,
  hashJoined,
  hashPostgresJsonb,
  verifyMergeReceiptTree
} from './receipt-contract.js';
import {
  invoiceProcessorFieldsFromHeaders,
  invoiceProcessorSha256Hex,
  validateInvoiceProcessorRequestFields,
  verifyInvoiceProcessorRequest
} from '../../shared/invoice-processor-security.js';

const encoder = new TextEncoder();
const MAX_REQUEST_BYTES = 2 * 1024 * 1024;
const POLICY_VERSION = 'INVOICE_PROCESSOR_LIMITS_V4';
const IMPLEMENTATION_VERSION = 'cloudtms-invoice-document-worker-v6';
const SUPPORTED_MEDIA_TYPES = Object.freeze(['application/pdf','image/jpeg','image/png']);
const RECEIPT_CONTRACTS = Object.freeze({
  object: 'ACTUAL_BYTES_OBJECT_RECEIPT_V3',
  logical: 'LOGICAL_SOURCE_RECEIPT_V3',
  merge: 'ACTUAL_BYTES_MERGE_RECEIPT_V3',
  root: 'DOCUMENT_ROOT_RECEIPT_V3',
  ordered_input: 'ACTUAL_ORDERED_INPUT_V1'
});

export class InvoiceDocumentContainer extends Container {
  defaultPort = 8080;
  requiredPorts = [8080];
  sleepAfter = '10s';
  enableInternet = false;
  pingEndpoint = 'health';
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' } });
}

function hasInternalProcessorMarker(request) {
  return request.method === 'POST'
    && request.headers.get('x-cloudtms-internal-service') === 'invoice-document-v1';
}

async function authenticateProcessorRequest(request, env, text) {
  if (!hasInternalProcessorMarker(request)) return { ok: false, code: 'PROCESSOR_CALLER_INVALID' };
  const bodySha256 = await invoiceProcessorSha256Hex(text);
  const fields = invoiceProcessorFieldsFromHeaders(request, bodySha256);
  const validated = validateInvoiceProcessorRequestFields(fields);
  if (!validated.ok) return validated;
  const signature = request.headers.get('x-cloudtms-signature');
  if (!(await verifyInvoiceProcessorRequest(env.INVOICE_DOCUMENT_PROCESSOR_SECRET, fields, signature))) {
    return { ok: false, code: 'PROCESSOR_REQUEST_SIGNATURE_INVALID' };
  }
  return { ok: true, fields };
}

function findInputDescriptors(action, context) {
  if (action === 'ASSET_INSPECT') return [{ r2_key: context.original_r2_key, media_type: context.declared_media_type, size_bytes: context.expected_original_size_bytes, sha256: context.expected_original_sha256, require_stored_sha256: false }];
  if (action === 'ASSET_NORMALISE') return [{ r2_key: context.expected_original_r2_key || context.original_r2_key, media_type: context.expected_original_media_type || context.detected_media_type, size_bytes: context.expected_original_size_bytes, sha256: context.expected_original_sha256, require_stored_sha256: false }];
  if (action === 'PDF_MERGE') return Array.isArray(context.ordered_inputs) ? context.ordered_inputs.map(input => ({ ...input, require_stored_sha256: true })) : [];
  if (action === 'DOCUMENT_VERIFY') return [{ r2_key: context.final_candidate_key, sha256: context.final_candidate_sha256, size_bytes: context.final_candidate_size_bytes, require_stored_sha256: true }];
  throw Object.assign(new Error('UNSUPPORTED_NATIVE_ACTION'), { code: 'UNSUPPORTED_NATIVE_ACTION' });
}

function safePositive(value, fallback = null) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number > 0 ? number : fallback;
}

async function resolveR2Inputs(bucket, descriptors) {
  if (!descriptors.length) throw Object.assign(new Error('PROCESSOR_INPUT_REQUIRED'), { code: 'PROCESSOR_INPUT_REQUIRED' });
  const keys = descriptors.map(item => String(item.r2_key || ''));
  if (keys.some(key => !key)) throw Object.assign(new Error('INPUT_R2_KEY_MISSING'), { code: 'INPUT_R2_KEY_MISSING' });
  if (new Set(keys).size !== keys.length) throw Object.assign(new Error('INPUT_R2_KEY_DUPLICATE'), { code: 'INPUT_R2_KEY_DUPLICATE' });
  const resolved = [];
  for (let index = 0; index < descriptors.length; index += 1) {
    const descriptor = descriptors[index];
    const key = keys[index];
    const object = await bucket.get(key);
    if (!object) throw Object.assign(new Error('INPUT_R2_OBJECT_MISSING'), { code: 'INPUT_R2_OBJECT_MISSING' });
    if (descriptor.size_bytes != null && Number(descriptor.size_bytes) !== Number(object.size)) throw Object.assign(new Error('INPUT_SIZE_MISMATCH'), { code: 'INPUT_SIZE_MISMATCH' });
    const expectedHash = String(descriptor.sha256 || '').toLowerCase();
    const storedHash = String(object.customMetadata?.sha256 || '').toLowerCase();
    if (descriptor.require_stored_sha256 && !storedHash) throw Object.assign(new Error('INPUT_STORED_HASH_MISSING'), { code: 'INPUT_STORED_HASH_MISSING' });
    if (expectedHash && storedHash && expectedHash !== storedHash) throw Object.assign(new Error('INPUT_STORED_HASH_MISMATCH'), { code: 'INPUT_STORED_HASH_MISMATCH' });
    resolved.push({ descriptor, object, header: { input_order: index + 1, input_chunk_id: descriptor.input_chunk_id || null, r2_key: key, size_bytes: object.size, expected_sha256: descriptor.sha256 || null, media_type: descriptor.media_type || object.httpMetadata?.contentType || 'application/octet-stream' } });
  }
  return resolved;
}

function validateNativeInputLimits(action, context, inputs) {
  const limits = action.startsWith('ASSET_') ? (context.output_profile || {}) : (context.limits || context.verification_policy || {});
  const totalBytes = inputs.reduce((sum, input) => sum + Number(input.object.size || 0), 0);
  const maximumSingle = safePositive(limits.max_source_bytes || limits.max_single_input_bytes || limits.max_part_input_bytes);
  if (maximumSingle && inputs.some(input => Number(input.object.size) > maximumSingle)) throw Object.assign(new Error('PROCESSOR_SINGLE_INPUT_LIMIT_EXCEEDED'), { code: 'PROCESSOR_SINGLE_INPUT_LIMIT_EXCEEDED' });
  if (action === 'PDF_MERGE') {
    const maximumInputs = safePositive(limits.max_inputs);
    const maximumBytes = safePositive(limits.max_input_bytes);
    if (maximumInputs && inputs.length > maximumInputs) throw Object.assign(new Error('MERGE_INPUT_COUNT_EXCEEDS_POLICY'), { code: 'MERGE_INPUT_COUNT_EXCEEDS_POLICY' });
    if (maximumBytes && totalBytes > maximumBytes) throw Object.assign(new Error('MERGE_INPUT_BYTES_EXCEED_POLICY'), { code: 'MERGE_INPUT_BYTES_EXCEED_POLICY' });
  }
  if (action === 'DOCUMENT_VERIFY') {
    const maximum = safePositive(limits.max_receipts);
    if (maximum && Number(context.expected_physical_input_count || 0) > maximum) throw Object.assign(new Error('VERIFY_RECEIPTS_EXCEED_POLICY'), { code: 'VERIFY_RECEIPTS_EXCEED_POLICY' });
  }
  return totalBytes;
}

function framedBody(header, inputs, signal) {
  const headerBytes = encoder.encode(JSON.stringify(header));
  const prefix = new Uint8Array(8);
  new DataView(prefix.buffer).setBigUint64(0, BigInt(headerBytes.byteLength));
  let stage = 0;
  let inputIndex = 0;
  let reader = null;
  let bytesRead = 0;
  const cancelReader = async reason => { if (reader) { try { await reader.cancel(reason); } catch {} reader = null; } };
  return new ReadableStream({
    async pull(controller) {
      try {
        if (signal?.aborted) throw Object.assign(new Error('PROCESSOR_REQUEST_ABORTED'), { code: 'PROCESSOR_REQUEST_ABORTED' });
        if (stage === 0) { controller.enqueue(prefix); stage = 1; return; }
        if (stage === 1) { controller.enqueue(headerBytes); stage = 2; return; }
        while (inputIndex < inputs.length) {
          if (!reader) { reader = inputs[inputIndex].object.body.getReader(); bytesRead = 0; }
          const item = await reader.read();
          if (!item.done) {
            bytesRead += item.value.byteLength;
            if (bytesRead > Number(inputs[inputIndex].object.size)) throw Object.assign(new Error('INPUT_EXTRA_BYTES'), { code: 'INPUT_EXTRA_BYTES' });
            controller.enqueue(item.value);
            return;
          }
          if (bytesRead !== Number(inputs[inputIndex].object.size)) throw Object.assign(new Error('INPUT_PREMATURE_EOF'), { code: 'INPUT_PREMATURE_EOF' });
          reader.releaseLock(); reader = null; inputIndex += 1;
        }
        controller.close();
      } catch (error) { await cancelReader(error); controller.error(error); }
    },
    async cancel(reason) { await cancelReader(reason); }
  });
}

function fixedLengthFramedBody(header, inputs, signal, FixedLengthStreamConstructor = globalThis.FixedLengthStream) {
  if (typeof FixedLengthStreamConstructor !== 'function') {
    throw Object.assign(new Error('PROCESSOR_FIXED_LENGTH_STREAM_UNAVAILABLE'), {
      code: 'PROCESSOR_FIXED_LENGTH_STREAM_UNAVAILABLE',
      category: 'TRANSIENT_INFRASTRUCTURE'
    });
  }
  const headerBytes = encoder.encode(JSON.stringify(header));
  const length = inputs.reduce(
    (total, input) => total + Number(input.object.size || 0),
    8 + headerBytes.byteLength
  );
  if (!Number.isSafeInteger(length) || length < 1) {
    throw Object.assign(new Error('PROCESSOR_REQUEST_LENGTH_INVALID'), {
      code: 'PROCESSOR_REQUEST_LENGTH_INVALID',
      category: 'IDENTITY_MISMATCH'
    });
  }
  const fixed = new FixedLengthStreamConstructor(length);
  return {
    body: fixed.readable,
    length,
    completion: framedBody(header, inputs, signal).pipeTo(fixed.writable)
  };
}

function fixedLengthReadableBody(body, length, FixedLengthStreamConstructor = globalThis.FixedLengthStream) {
  if (!body || typeof body.pipeTo !== 'function') {
    return { body, completion: Promise.resolve() };
  }
  if (typeof FixedLengthStreamConstructor !== 'function') {
    throw Object.assign(new Error('PROCESSOR_FIXED_LENGTH_STREAM_UNAVAILABLE'), {
      code: 'PROCESSOR_FIXED_LENGTH_STREAM_UNAVAILABLE',
      category: 'TRANSIENT_INFRASTRUCTURE'
    });
  }
  const expectedLength = Number(length);
  if (!Number.isSafeInteger(expectedLength) || expectedLength < 1) {
    throw Object.assign(new Error('PROCESSOR_RESPONSE_LENGTH_INVALID'), {
      code: 'PROCESSOR_RESPONSE_LENGTH_INVALID',
      category: 'IDENTITY_MISMATCH'
    });
  }
  const fixed = new FixedLengthStreamConstructor(expectedLength);
  return {
    body: fixed.readable,
    completion: body.pipeTo(fixed.writable)
  };
}

function resultIdentity(expected) {
  return { chunk_id: expected.chunk_id, fence_token: expected.fence_token, action: expected.action, document_version_id: expected.document_version_id || undefined, document_asset_id: expected.document_asset_id || undefined, plan_generation: expected.plan_generation, source_revision: expected.source_revision || undefined, template_version: expected.template_version || undefined, processor_policy_version: expected.processor_policy_version, render_kind: expected.render_kind || undefined, ordered_input_hash: expected.ordered_input_hash || undefined, output_prefix: expected.immutable_destination_prefix };
}

function decodeProcessorHeader(response) {
  const encoded = response.headers.get('x-cloudtms-result');
  if (!encoded || encoded.length > 65536) throw Object.assign(new Error('PROCESSOR_RESULT_HEADER_INVALID'), { code: 'PROCESSOR_RESULT_HEADER_INVALID' });
  try { return JSON.parse(new TextDecoder().decode(Uint8Array.from(atob(encoded.replace(/-/g, '+').replace(/_/g, '/')), character => character.charCodeAt(0)))); }
  catch { throw Object.assign(new Error('PROCESSOR_RESULT_HEADER_INVALID'), { code: 'PROCESSOR_RESULT_HEADER_INVALID' }); }
}

function validateProcessorMetadata(metadata, response, action, context) {
  if (!/^[0-9a-f]{64}$/i.test(String(metadata.sha256 || ''))) throw Object.assign(new Error('PROCESSOR_OUTPUT_HASH_INVALID'), { code: 'PROCESSOR_OUTPUT_HASH_INVALID' });
  if (!Number.isSafeInteger(Number(metadata.size_bytes)) || Number(metadata.size_bytes) < 1) throw Object.assign(new Error('PROCESSOR_OUTPUT_SIZE_INVALID'), { code: 'PROCESSOR_OUTPUT_SIZE_INVALID' });
  if (!Number.isSafeInteger(Number(metadata.page_count)) || Number(metadata.page_count) < 1) throw Object.assign(new Error('PROCESSOR_OUTPUT_PAGE_COUNT_INVALID'), { code: 'PROCESSOR_OUTPUT_PAGE_COUNT_INVALID' });
  if (metadata.parse_verified !== true) throw Object.assign(new Error('PROCESSOR_OUTPUT_PARSE_NOT_VERIFIED'), { code: 'PROCESSOR_OUTPUT_PARSE_NOT_VERIFIED' });
  if (!String(metadata.processor_version || '').startsWith('cloudtms-native-')) throw Object.assign(new Error('PROCESSOR_VERSION_INVALID'), { code: 'PROCESSOR_VERSION_INVALID' });
  const length = Number(response.headers.get('content-length'));
  if (Number.isFinite(length) && length !== Number(metadata.size_bytes)) throw Object.assign(new Error('PROCESSOR_RESPONSE_SIZE_MISMATCH'), { code: 'PROCESSOR_RESPONSE_SIZE_MISMATCH' });
  if (action === 'PDF_MERGE' && (
    !Array.isArray(metadata.actual_inputs)
    || metadata.actual_inputs.length !== Number(context.ordered_inputs?.length || 0)
    || metadata.actual_inputs.reduce((sum, input) => sum + Number(input.page_count || 0), 0)
      !== Number(metadata.page_count)
  )) throw Object.assign(new Error('PROCESSOR_INPUT_RECEIPTS_MISSING'), { code: 'PROCESSOR_INPUT_RECEIPTS_MISSING' });
  if (action === 'ASSET_NORMALISE') {
    const actual = Array.isArray(metadata.actual_inputs) ? metadata.actual_inputs : [];
    const expectedKey = String(context.expected_original_r2_key || context.original_r2_key || '');
    if (
      actual.length !== 1
      || String(actual[0]?.r2_key || '') !== expectedKey
      || !/^[0-9a-f]{64}$/i.test(String(actual[0]?.sha256 || ''))
      || !Number.isSafeInteger(Number(actual[0]?.size_bytes))
      || Number(actual[0]?.size_bytes) < 1
    ) {
      throw Object.assign(new Error('PROCESSOR_SOURCE_IDENTITY_MISSING'), { code: 'PROCESSOR_SOURCE_IDENTITY_MISSING' });
    }
  }
  const limits = action === 'ASSET_NORMALISE'
    ? (context.output_profile || {})
    : (context.limits || context.verification_policy || {});
  const maximumOutputBytes = action === 'ASSET_NORMALISE'
    ? safePositive(limits.max_pdf_part_bytes)
    : action === 'PDF_MERGE'
      ? Math.min(safePositive(limits.max_input_bytes) || Number.MAX_SAFE_INTEGER, 512 * 1024 * 1024)
      : null;
  if (maximumOutputBytes && Number(metadata.size_bytes) > maximumOutputBytes) {
    throw Object.assign(new Error('PROCESSOR_OUTPUT_BYTES_EXCEED_POLICY'), { code: 'PROCESSOR_OUTPUT_BYTES_EXCEED_POLICY', category: 'POLICY_VIOLATION' });
  }
}

function artifactMetadata(identity, metadata) {
  return {
    sha256: String(metadata.sha256), size_bytes: String(metadata.size_bytes), chunk_id: String(identity.chunk_id),
    fence_token: String(identity.fence_token), plan_generation: String(identity.plan_generation ?? ''),
    document_version_id: String(identity.document_version_id || ''), document_asset_id: String(identity.document_asset_id || ''),
    action: String(identity.action), processor_policy_version: String(identity.processor_policy_version),
    processor_version: String(metadata.processor_version)
  };
}

function artifactMatches(object, expected) {
  return !!object && Number(object.size) === Number(expected.size_bytes)
    && Object.entries(expected).every(([key, value]) => object.customMetadata?.[key] === String(value));
}

async function putImmutableProcessorArtifact(bucket, key, body, identity, metadata) {
  const expected = artifactMetadata(identity, metadata);
  const existing = await bucket.head(key);
  if (existing) {
    if (artifactMatches(existing, expected)) {
      try { await body?.cancel?.('immutable-artifact-reused'); } catch {}
      return { reused: true, object: existing };
    }
    try { await body?.cancel?.('immutable-artifact-conflict'); } catch {}
    throw Object.assign(new Error('IMMUTABLE_ARTIFACT_KEY_CONFLICT'), { code: 'IMMUTABLE_ARTIFACT_KEY_CONFLICT' });
  }
  const prepared = fixedLengthReadableBody(body, metadata.size_bytes);
  const [result] = await Promise.all([
    bucket.put(key, prepared.body, { onlyIf: { etagDoesNotMatch: '*' }, sha256: metadata.sha256, httpMetadata: { contentType: 'application/pdf' }, customMetadata: expected }),
    prepared.completion
  ]);
  if (result) return { reused: false, object: result };
  const raced = await bucket.head(key);
  if (artifactMatches(raced, expected)) return { reused: true, object: raced };
  throw Object.assign(new Error('IMMUTABLE_ARTIFACT_KEY_CONFLICT'), { code: 'IMMUTABLE_ARTIFACT_KEY_CONFLICT' });
}

async function buildVerificationOnlyResult(identity, context, metadata) {
  if (context.verification_mode !== 'VERIFY_EXISTING_CANDIDATE') {
    throw Object.assign(new Error('VERIFICATION_MODE_UNSUPPORTED'), { code: 'VERIFICATION_MODE_UNSUPPORTED' });
  }

  const expected = [
    [context.final_candidate_key, metadata.verified_candidate_r2_key, 'FINAL_CANDIDATE_KEY_MISMATCH'],
    [context.final_candidate_sha256, metadata.verified_candidate_sha256, 'FINAL_CANDIDATE_HASH_MISMATCH'],
    [context.final_candidate_size_bytes, metadata.verified_candidate_size_bytes, 'FINAL_CANDIDATE_SIZE_MISMATCH'],
    [context.expected_page_count, metadata.actual_page_count, 'FINAL_PAGE_COUNT_MISMATCH']
  ];

  for (const [wanted, actual, code] of expected) {
    if (wanted != null && wanted !== '' && String(wanted) !== String(actual)) {
      throw Object.assign(new Error(code), { code });
    }
  }

  if (identity.processor_policy_version !== POLICY_VERSION) {
    throw Object.assign(new Error('PROCESSOR_POLICY_UNSUPPORTED'), { code: 'PROCESSOR_POLICY_UNSUPPORTED' });
  }

  const root = context.final_merge_receipt || {};
  if (root.receipt_contract !== 'ACTUAL_BYTES_MERGE_RECEIPT_V3') {
    throw Object.assign(new Error('RECEIPT_STRUCTURE_INVALID'), { code: 'RECEIPT_STRUCTURE_INVALID' });
  }

  const calculatedFinalMergeReceiptHash = await hashPostgresJsonb(root);
  if (context.final_merge_receipt_hash
      && calculatedFinalMergeReceiptHash !== context.final_merge_receipt_hash) {
    throw Object.assign(new Error('FINAL_MERGE_RECEIPT_HASH_MISMATCH'), {
      code: 'FINAL_MERGE_RECEIPT_HASH_MISMATCH',
      detail: {
        expected: context.final_merge_receipt_hash,
        actual: calculatedFinalMergeReceiptHash
      }
    });
  }

  const verificationPolicy = context.verification_policy || {};
  const verifiedTree = await verifyMergeReceiptTree(root, {
    maximum_depth: safePositive(verificationPolicy.max_merge_levels || verificationPolicy.max_depth) || 8,
    maximum_receipts: safePositive(verificationPolicy.max_receipts) || 10000
  });

  /*
   * The processor's verified candidate metadata must match the independently
   * verified root merge output.  The database already checks the legal payload
   * candidate identity; this check prevents a forged or stale root receipt from
   * describing a different output object while the container reports a valid
   * candidate PDF.
   */
  const rootOutputAssertions = [
    [metadata.verified_candidate_r2_key, verifiedTree.output.r2_key, 'FINAL_ROOT_OUTPUT_KEY_MISMATCH'],
    [metadata.verified_candidate_sha256, verifiedTree.output.sha256, 'FINAL_ROOT_OUTPUT_HASH_MISMATCH'],
    [metadata.verified_candidate_size_bytes, verifiedTree.output.size_bytes, 'FINAL_ROOT_OUTPUT_SIZE_MISMATCH'],
    [metadata.actual_page_count, verifiedTree.output.page_count, 'FINAL_ROOT_OUTPUT_PAGE_COUNT_MISMATCH']
  ];
  for (const [wanted, actual, code] of rootOutputAssertions) {
    if (wanted == null || wanted === '' || actual == null || actual === '' || String(wanted) !== String(actual)) {
      throw Object.assign(new Error(code), { code });
    }
  }

  if (context.expected_physical_input_count != null
      && verifiedTree.leaves.length !== Number(context.expected_physical_input_count)) {
    throw Object.assign(new Error('FINAL_PHYSICAL_INPUT_COUNT_MISMATCH'), { code: 'FINAL_PHYSICAL_INPUT_COUNT_MISMATCH' });
  }

  const logicalCoverage = new Map();
  for (const leaf of verifiedTree.leaves) {
    const logicalOrdinal = String(
      leaf.logical_ordinal ?? leaf.logical_manifest_ordinal ?? ''
    );
    const logicalSourceKey = String(leaf.logical_source_key || '');
    if (!logicalOrdinal || !logicalSourceKey) {
      throw Object.assign(new Error('FINAL_INPUT_COVERAGE_INVALID'), { code: 'FINAL_INPUT_COVERAGE_INVALID' });
    }
    const existing = logicalCoverage.get(logicalOrdinal);
    if (existing && existing !== logicalSourceKey) {
      throw Object.assign(new Error('FINAL_LOGICAL_INPUT_ORDINAL_CONFLICT'), { code: 'FINAL_LOGICAL_INPUT_ORDINAL_CONFLICT' });
    }
    logicalCoverage.set(logicalOrdinal, logicalSourceKey);
  }

  const logicalRows = [...logicalCoverage.entries()]
    .map(([logicalOrdinal, logicalSourceKey]) => ({ logicalOrdinal, logicalSourceKey }))
    .sort((left, right) => Number(left.logicalOrdinal) - Number(right.logicalOrdinal)
      || left.logicalSourceKey.localeCompare(right.logicalSourceKey));

  for (let index = 0; index < logicalRows.length; index += 1) {
    if (Number(logicalRows[index].logicalOrdinal) !== index + 1) {
      throw Object.assign(new Error('FINAL_LOGICAL_INPUT_ORDER_INVALID'), { code: 'FINAL_LOGICAL_INPUT_ORDER_INVALID' });
    }
  }

  const logicalCount = logicalRows.length;
  if (context.expected_logical_source_count != null
      && logicalCount !== Number(context.expected_logical_source_count)) {
    throw Object.assign(new Error('FINAL_LOGICAL_INPUT_COUNT_MISMATCH'), { code: 'FINAL_LOGICAL_INPUT_COUNT_MISMATCH' });
  }

  const assembledInputCoverageHash = await hashJoined(logicalRows.map(row => [
    row.logicalOrdinal,
    row.logicalSourceKey
  ].join('|')));

  if (context.expected_coverage_hash && assembledInputCoverageHash !== context.expected_coverage_hash) {
    throw Object.assign(new Error('FINAL_INPUT_COVERAGE_MISMATCH'), {
      code: 'FINAL_INPUT_COVERAGE_MISMATCH',
      detail: {
        expected: context.expected_coverage_hash,
        actual: assembledInputCoverageHash
      }
    });
  }
  const resolvedCoverageHash = context.resolved_input_coverage_hash || context.resolved_logical_input_hash || '';
  if (resolvedCoverageHash && assembledInputCoverageHash !== resolvedCoverageHash) {
    throw Object.assign(new Error('FINAL_RESOLVED_INPUT_COVERAGE_MISMATCH'), {
      code: 'FINAL_RESOLVED_INPUT_COVERAGE_MISMATCH',
      detail: {
        expected: resolvedCoverageHash,
        actual: assembledInputCoverageHash
      }
    });
  }

  const assertions = [
    [context.expected_logical_root_receipt, verifiedTree.logicalRoot, 'FINAL_LOGICAL_RECEIPT_MISMATCH'],
    [context.expected_physical_root_receipt, verifiedTree.physicalRoot, 'FINAL_PHYSICAL_RECEIPT_MISMATCH'],
    [context.expected_ordered_input_root, verifiedTree.childReceiptHash, 'FINAL_ORDERED_INPUT_RECEIPT_MISMATCH'],
    [context.root_merge_receipt_identity, verifiedTree.rootIdentity, 'FINAL_ROOT_IDENTITY_MISMATCH'],
    [context.expected_physical_input_hash, verifiedTree.physicalInputHash, 'FINAL_PHYSICAL_INPUT_HASH_MISMATCH']
  ];

  for (const [wanted, actual, code] of assertions) {
    if (wanted && wanted !== actual) throw Object.assign(new Error(code), { code });
  }

  return {
    ...identity,
    ...metadata,
    // Generic output aliases are required by invoice_work_complete_batch's
    // DOCUMENT_VERIFY contract, in addition to the verified_candidate_*
    // fields returned by the native verifier.
    r2_key: metadata.verified_candidate_r2_key,
    sha256: metadata.verified_candidate_sha256,
    size_bytes: metadata.verified_candidate_size_bytes,
    page_count: metadata.actual_page_count,
    manifest_hash: context.expected_manifest_hash,
    manifest_coverage_verified: true,
    ordering_verified: true,
    parse_verified: true,
    assembled_input_coverage_hash: assembledInputCoverageHash,
    actual_logical_root_receipt: verifiedTree.logicalRoot,
    actual_physical_root_receipt: verifiedTree.physicalRoot,
    actual_ordered_input_root: verifiedTree.childReceiptHash,
    actual_physical_ordered_input_hash: verifiedTree.orderedInputRoot,
    root_merge_receipt_identity: verifiedTree.rootIdentity,
    root_merge_receipt_hash: calculatedFinalMergeReceiptHash,
    assembled_input_count: logicalCount,
    assembled_physical_input_count: verifiedTree.leaves.length,
    assembled_physical_input_hash: verifiedTree.physicalInputHash,
    actual_input_receipts: verifiedTree.leaves
  };
}

async function processWithContainer(env, payload, signal) {
  const context = payload.context || {};
  const identity = resultIdentity(payload.expected_result_identity || {});
  const action = String(identity.action || '').toUpperCase();
  if (!identity.chunk_id || identity.processor_policy_version !== POLICY_VERSION) throw Object.assign(new Error('PROCESSOR_IDENTITY_INCOMPLETE'), { code: 'PROCESSOR_IDENTITY_INCOMPLETE' });
  if (action !== 'DOCUMENT_VERIFY' && !identity.output_prefix) throw Object.assign(new Error('PROCESSOR_OUTPUT_PREFIX_MISSING'), { code: 'PROCESSOR_OUTPUT_PREFIX_MISSING' });
  const descriptors = findInputDescriptors(action, context);
  const inputs = await resolveR2Inputs(env.R2, descriptors);
  validateNativeInputLimits(action, context, inputs);
  const header = { action, context, processor_policy_version: POLICY_VERSION, action_timeout_ms: Number(context.action_timeout_ms || 120000), inputs: inputs.map(input => input.header) };
  const framed = fixedLengthFramedBody(header, inputs, signal);
  const request = new Request('http://container/process', { method: 'POST', headers: { 'content-type': 'application/x-cloudtms-framed-files-v1' }, body: framed.body, signal });
  const containerIdentity = await invoiceProcessorSha256Hex(
    `${identity.chunk_id}|${identity.fence_token}|${action}`
  );
  const container = getContainer(
    env.INVOICE_DOCUMENT_CONTAINER,
    `invoice-native-${containerIdentity.slice(0, 32)}`
  );
  const [response] = await Promise.all([
    container.fetch(request),
    framed.completion
  ]);
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw Object.assign(new Error(body.message || body.code || `CONTAINER_HTTP_${response.status}`), { code: body.code || `CONTAINER_HTTP_${response.status}`, category: body.category });
  }
  if (action === 'ASSET_INSPECT') {
    const body = await response.json();
    return { ...identity, ...(body.result || body) };
  }
  if (action === 'DOCUMENT_VERIFY') {
    const body = await response.json();
    return buildVerificationOnlyResult(identity, context, body.result || body);
  }
  const metadata = decodeProcessorHeader(response);
  try {
    validateProcessorMetadata(metadata, response, action, context);
  } catch (error) {
    try { await response.body?.cancel?.('processor-metadata-invalid'); } catch {}
    throw error;
  }
  const outputKey = `${identity.output_prefix}${action.toLowerCase()}-${metadata.sha256}.pdf`;
  await putImmutableProcessorArtifact(env.R2, outputKey, response.body, identity, metadata);
  const result = { ...identity, r2_key: outputKey, sha256: metadata.sha256, size_bytes: metadata.size_bytes, page_count: metadata.page_count, parse_verified: metadata.parse_verified === true, output_type: 'application/pdf', processor_version: metadata.processor_version };
  if (action === 'ASSET_NORMALISE') {
    const consumed = metadata.actual_inputs?.[0] || {};
    result.consumed_original_r2_key = consumed.r2_key;
    result.consumed_original_sha256 = consumed.sha256;
    result.consumed_original_size_bytes = consumed.size_bytes;
  }
  if (action === 'PDF_MERGE') {
    const receipt = await buildMergeReceipt(context, identity, inputs, metadata, result);
    result.input_count = inputs.length; result.input_receipts = receipt.input_receipts; result.merge_receipt = receipt;
  }
  return result;
}

function classifyError(error) {
  if (error?.category) return error.category;
  const code = String(error?.code || error?.message || '').toUpperCase();
  if (/TIMEOUT|ABORT/.test(code)) return 'PROCESSOR_TIMEOUT';
  if (/UNSUPPORTED|EMPTY|TRUNCATED|CORRUPT|ENCRYPTED|PASSWORD/.test(code)) return 'PERMANENT_INPUT';
  if (/POLICY|LIMIT|EXCEED/.test(code)) return 'POLICY_VIOLATION';
  if (/MISMATCH|IDENTITY|CONFLICT|DUPLICATE|MISSING/.test(code)) return 'IDENTITY_MISMATCH';
  if (/HTTP_5|FETCH|NETWORK|UNAVAILABLE/.test(code)) return 'TRANSIENT_INFRASTRUCTURE';
  return 'PROCESSOR_BUG';
}

export async function handleInvoiceDocumentProcessorRequest(request, env) {
  const path = new URL(request.url).pathname;
  if (!hasInternalProcessorMarker(request) || !['/process','/ready'].includes(path)) {
    return json({ ok: false, code: 'NOT_FOUND' }, 404);
  }
  if (!env.R2 || !env.INVOICE_DOCUMENT_CONTAINER) return json({ ok: false, code: 'PROCESSOR_BINDING_MISSING', category: 'TRANSIENT_INFRASTRUCTURE' }, 503);
  if (!env.INVOICE_DOCUMENT_PROCESSOR_SECRET) {
    return json({ ok: false, code: 'INVOICE_DOCUMENT_PROCESSOR_SECRET_MISSING', category: 'TRANSIENT_INFRASTRUCTURE', retryable: false }, 503);
  }
  if (Number(request.headers.get('content-length') || 0) > MAX_REQUEST_BYTES) return json({ ok: false, code: 'PROCESSOR_REQUEST_TOO_LARGE', category: 'POLICY_VIOLATION' }, 413);
  try {
    const text = await request.text();
    if (encoder.encode(text).byteLength > MAX_REQUEST_BYTES) throw Object.assign(new Error('PROCESSOR_REQUEST_TOO_LARGE'), { code: 'PROCESSOR_REQUEST_TOO_LARGE' });
    const authentication = await authenticateProcessorRequest(request, env, text);
    if (!authentication.ok) return json({ ok: false, code: authentication.code, category: 'IDENTITY_MISMATCH', retryable: false }, 403);
    const payload = JSON.parse(text);
    if (path === '/ready') {
      if (text !== '{}' || authentication.fields.action !== 'READY' || authentication.fields.processor_policy_version !== POLICY_VERSION) {
        return json({ ok: false, code: 'PROCESSOR_READY_REQUEST_INVALID', category: 'IDENTITY_MISMATCH', retryable: false }, 403);
      }
      const container = getContainer(env.INVOICE_DOCUMENT_CONTAINER, 'invoice-native-readiness-v4');
      const response = await container.fetch(new Request('http://container/health', {
        method: 'GET',
        signal: AbortSignal.timeout(3000)
      }));
      const health = await response.json().catch(() => null);
      const containerReady = response.ok && health?.ok === true;
      return json({
        ok: containerReady,
        service: 'invoice-document-processor',
        processor_policy_version: POLICY_VERSION,
        processor_implementation_version: IMPLEMENTATION_VERSION,
        supported_media_types: SUPPORTED_MEDIA_TYPES,
        receipt_contracts: RECEIPT_CONTRACTS,
        container_ready: containerReady,
        native_tools_ready: containerReady
          && health?.qpdf_ready === true
          && health?.poppler_ready === true
          && health?.imagemagick_ready === true
          && health?.img2pdf_ready === true,
        checked_at_utc: new Date().toISOString()
      }, containerReady ? 200 : 503);
    }
    const expected = payload?.expected_result_identity || {};
    const identityAssertions = [
      [authentication.fields.chunk_id, expected.chunk_id],
      [String(authentication.fields.fence_token), String(expected.fence_token)],
      [authentication.fields.action, String(expected.action || '').toUpperCase()],
      [String(authentication.fields.plan_generation), String(expected.plan_generation ?? '')],
      [authentication.fields.processor_policy_version, expected.processor_policy_version]
    ];
    if (identityAssertions.some(([header, body]) => header !== body)) {
      return json({ ok: false, code: 'PROCESSOR_REQUEST_IDENTITY_MISMATCH', category: 'IDENTITY_MISMATCH', retryable: false }, 403);
    }
    const timeoutMs = Math.max(10000, Math.min(300000, Number(payload.context?.action_timeout_ms || 120000)));
    const signal = AbortSignal.any([request.signal, AbortSignal.timeout(timeoutMs)]);
    return json({ ok: true, result: await processWithContainer(env, payload, signal) });
  } catch (error) {
    const code = String(error?.code || error?.message || 'INVOICE_DOCUMENT_PROCESSOR_FAILED').slice(0, 120);
    const category = classifyError(error);
    return json({ ok: false, code, category, retryable: category === 'TRANSIENT_INFRASTRUCTURE' || category === 'PROCESSOR_TIMEOUT', message: String(error?.message || code).slice(0, 500) }, ['PERMANENT_INPUT','POLICY_VIOLATION','IDENTITY_MISMATCH'].includes(category) ? 422 : 503);
  }
}

export default { fetch(request, env) { return handleInvoiceDocumentProcessorRequest(request, env); } };
export const invoiceDocumentProcessorInternals = Object.freeze({
  findInputDescriptors,
  resolveR2Inputs,
  framedBody,
  fixedLengthFramedBody,
  fixedLengthReadableBody,
  resultIdentity,
  buildMergeReceipt,
  flattenLeafInputReceipts,
  verifyMergeReceiptTree,
  putImmutableProcessorArtifact,
  buildVerificationOnlyResult,
  processWithContainer,
  classifyError,
  authenticateProcessorRequest
});
