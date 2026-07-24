const encoder = new TextEncoder();

export async function sha256Hex(bytes) {
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

export function postgresJsonbText(value) {
  if (Array.isArray(value)) return `[${value.map(postgresJsonbText).join(', ')}]`;
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort((left, right) => {
      const a = encoder.encode(left); const b = encoder.encode(right);
      if (a.byteLength !== b.byteLength) return a.byteLength - b.byteLength;
      for (let index = 0; index < a.byteLength; index += 1) if (a[index] !== b[index]) return a[index] - b[index];
      return 0;
    });
    return `{${keys.map(key => `${JSON.stringify(key)}: ${postgresJsonbText(value[key])}`).join(', ')}}`;
  }
  return JSON.stringify(value);
}

export async function hashText(value) { return sha256Hex(encoder.encode(String(value))); }
export async function hashPostgresJsonb(value) { return hashText(postgresJsonbText(value)); }
export async function hashJoined(values) { return hashText(values.join('||')); }

function requiredNumber(value, code) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) throw new Error(code);
  return number;
}

export async function buildPhysicalReceipt(input, actual, index) {
  const descriptor = input.descriptor || {};
  const order = requiredNumber(actual?.input_order, 'INPUT_ORDER_INVALID');
  if (order !== index + 1 || Number(descriptor.input_order || order) !== order) throw new Error('INPUT_ORDER_MISMATCH');
  if (String(actual.r2_key || '') !== String(descriptor.r2_key || '')) throw new Error('INPUT_R2_KEY_MISMATCH');
  if (!/^[0-9a-f]{64}$/i.test(String(actual.sha256 || ''))) throw new Error('INPUT_SHA256_INVALID');
  if (descriptor.sha256 && descriptor.sha256 !== actual.sha256) throw new Error('INPUT_SHA256_MISMATCH');
  if (descriptor.page_count != null && Number(descriptor.page_count) !== Number(actual.page_count)) throw new Error('INPUT_PAGE_COUNT_MISMATCH');
  if (descriptor.size_bytes != null && Number(descriptor.size_bytes) !== Number(actual.size_bytes)) throw new Error('INPUT_SIZE_MISMATCH');
  const base = {
    input_chunk_id: descriptor.input_chunk_id,
    actual_input_order: String(order),
    actual_r2_key: actual.r2_key,
    actual_sha256: actual.sha256,
    actual_page_count: String(requiredNumber(actual.page_count, 'INPUT_PAGE_COUNT_INVALID')),
    actual_size_bytes: String(requiredNumber(actual.size_bytes, 'INPUT_SIZE_INVALID'))
  };
  if (descriptor.child_merge_receipt) {
    const childHash = await hashPostgresJsonb(descriptor.child_merge_receipt);
    if (descriptor.child_merge_receipt_hash && descriptor.child_merge_receipt_hash !== childHash) throw new Error('CHILD_MERGE_RECEIPT_HASH_MISMATCH');
    if (descriptor.child_merge_receipt.output_sha256 !== actual.sha256 || Number(descriptor.child_merge_receipt.output_page_count) !== Number(actual.page_count)) throw new Error('CHILD_MERGE_OUTPUT_IDENTITY_MISMATCH');
    return { ...base, actual_child_merge_receipt: descriptor.child_merge_receipt, actual_child_merge_receipt_hash: childHash };
  }
  const physicalReceipt = await hashPostgresJsonb({
    receipt_contract: 'ACTUAL_BYTES_OBJECT_RECEIPT_V3',
    logical_source_key: descriptor.logical_source_key,
    logical_manifest_ordinal: Number(descriptor.logical_manifest_ordinal),
    physical_part_no: Number(descriptor.physical_part_no),
    object_key: actual.r2_key,
    stored_sha256: actual.sha256,
    expected_page_count: Number(actual.page_count),
    expected_byte_count: Number(actual.size_bytes)
  });
  if (descriptor.expected_physical_receipt && descriptor.expected_physical_receipt !== physicalReceipt) throw new Error('INPUT_PHYSICAL_RECEIPT_MISMATCH');
  return { ...base, logical_source_key: descriptor.logical_source_key, logical_manifest_ordinal: String(descriptor.logical_manifest_ordinal), physical_part_no: String(descriptor.physical_part_no), actual_physical_receipt: physicalReceipt };
}

export function physicalReceiptsFromInputs(inputs, receipts) {
  return inputs.flatMap((input, index) => {
    const child = receipts[index]?.actual_child_merge_receipt;
    if (Array.isArray(child?.physical_receipts)) return child.physical_receipts.map(receipt => ({ ...receipt }));
    return [{ logical_source_key: input.descriptor.logical_source_key, logical_manifest_ordinal: Number(input.descriptor.logical_manifest_ordinal), physical_part_no: Number(input.descriptor.physical_part_no), physical_receipt: receipts[index]?.actual_physical_receipt }];
  }).sort((left, right) => Number(left.logical_manifest_ordinal) - Number(right.logical_manifest_ordinal) || Number(left.physical_part_no) - Number(right.physical_part_no));
}

export async function calculateReceiptRoots(physicalReceipts) {
  if (!physicalReceipts.length || physicalReceipts.some(receipt => !receipt.physical_receipt)) throw new Error('PHYSICAL_RECEIPT_SET_INVALID');
  const physicalRoot = await hashJoined(physicalReceipts.map(row => row.physical_receipt));
  const grouped = new Map();
  for (const receipt of physicalReceipts) {
    const key = String(receipt.logical_source_key || '');
    if (!key) throw new Error('LOGICAL_SOURCE_KEY_MISSING');
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(receipt);
  }
  const logicalReceipts = [];
  for (const [logicalSourceKey, rows] of grouped) {
    const ordered = rows.sort((a, b) => Number(a.logical_manifest_ordinal) - Number(b.logical_manifest_ordinal) || Number(a.physical_part_no) - Number(b.physical_part_no));
    const ordinal = Math.min(...ordered.map(row => Number(row.logical_manifest_ordinal)));
    logicalReceipts.push({ logical_source_key: logicalSourceKey, logical_manifest_ordinal: ordinal, logical_receipt: await hashPostgresJsonb({ receipt_contract: 'LOGICAL_SOURCE_RECEIPT_V3', logical_source_key: logicalSourceKey, logical_manifest_ordinal: ordinal, ordered_physical_receipts: ordered.map(row => row.physical_receipt).join('||') }) });
  }
  logicalReceipts.sort((a, b) => a.logical_manifest_ordinal - b.logical_manifest_ordinal || a.logical_source_key.localeCompare(b.logical_source_key));
  return { physicalRoot, logicalRoot: await hashJoined(logicalReceipts.map(row => row.logical_receipt)), logicalReceipts };
}

function orderedInputIdentity(descriptor, actual, receipt, index) {
  return [index + 1, descriptor.input_chunk_id, actual.r2_key, actual.sha256, actual.page_count, actual.size_bytes, receipt.actual_child_merge_receipt_hash || ''].join('|');
}

export async function buildMergeReceipt(context, identity, inputs, metadata, output) {
  if (!Array.isArray(metadata.actual_inputs) || metadata.actual_inputs.length !== inputs.length) throw new Error('ACTUAL_INPUT_COUNT_MISMATCH');
  const inputReceipts = [];
  for (let index = 0; index < inputs.length; index += 1) inputReceipts.push(await buildPhysicalReceipt(inputs[index], metadata.actual_inputs[index], index));
  const physicalReceipts = physicalReceiptsFromInputs(inputs, inputReceipts);
  const roots = await calculateReceiptRoots(physicalReceipts);
  const actualChildReceiptHash = await hashJoined(inputReceipts.map(receipt => receipt.actual_physical_receipt || receipt.actual_child_merge_receipt_hash));
  const actualOrderedInputHash = await hashJoined(metadata.actual_inputs.map((actual, index) => orderedInputIdentity(inputs[index].descriptor, actual, inputReceipts[index], index)));
  const assertions = [
    [identity.ordered_input_hash || context.expected_ordered_input_hash, actualOrderedInputHash, 'ORDERED_INPUT_ROOT_MISMATCH'],
    [context.expected_child_receipt_hash, actualChildReceiptHash, 'CHILD_RECEIPT_ROOT_MISMATCH'],
    [context.expected_logical_receipt_root, roots.logicalRoot, 'LOGICAL_RECEIPT_ROOT_MISMATCH'],
    [context.expected_physical_receipt_root, roots.physicalRoot, 'PHYSICAL_RECEIPT_ROOT_MISMATCH']
  ];
  for (const [expected, actual, code] of assertions) if (expected && expected !== actual) throw new Error(code);
  return {
    receipt_contract: 'ACTUAL_BYTES_MERGE_RECEIPT_V3', processor_version: metadata.processor_version,
    processor_policy_version: identity.processor_policy_version, plan_generation: identity.plan_generation,
    input_receipts: inputReceipts, actual_ordered_input_hash: actualOrderedInputHash,
    actual_child_receipt_hash: actualChildReceiptHash, combined_logical_receipt_root: roots.logicalRoot,
    combined_physical_receipt_root: roots.physicalRoot, physical_receipts: physicalReceipts,
    output_r2_key: output.r2_key, output_sha256: output.sha256,
    output_size_bytes: output.size_bytes, output_page_count: output.page_count
  };
}

export function flattenLeafInputReceipts(mergeReceipt, output = []) {
  for (const receipt of (mergeReceipt?.input_receipts || [])) {
    if (receipt?.actual_child_merge_receipt) flattenLeafInputReceipts(receipt.actual_child_merge_receipt, output);
    else if (receipt?.actual_r2_key && receipt?.actual_sha256) output.push({ logical_ordinal: String(receipt.logical_manifest_ordinal), physical_part_no: String(receipt.physical_part_no), r2_key: receipt.actual_r2_key, sha256: receipt.actual_sha256, page_count: receipt.actual_page_count, size_bytes: receipt.actual_size_bytes });
  }
  return output;
}