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

function requiredNonnegativeNumber(value, code) {
  if (value == null || String(value).trim() === '') throw new Error(code);
  return requiredNumber(value, code);
}

export async function buildPhysicalReceipt(input, actual, index) {
  const descriptor = input.descriptor || {};
  const order = requiredNumber(actual?.input_order, 'INPUT_ORDER_INVALID');
  if (order < 1 || order !== index + 1 || Number(descriptor.input_order || order) !== order) throw new Error('INPUT_ORDER_MISMATCH');
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
  if (Number(base.actual_page_count) < 1) throw new Error('INPUT_PAGE_COUNT_INVALID');
  if (Number(base.actual_size_bytes) < 1) throw new Error('INPUT_SIZE_INVALID');
  if (descriptor.child_merge_receipt) {
    const childHash = await hashPostgresJsonb(descriptor.child_merge_receipt);
    if (descriptor.child_merge_receipt_hash && descriptor.child_merge_receipt_hash !== childHash) throw new Error('CHILD_MERGE_RECEIPT_HASH_MISMATCH');
    if (descriptor.child_merge_receipt.output_sha256 !== actual.sha256 || Number(descriptor.child_merge_receipt.output_page_count) !== Number(actual.page_count)) throw new Error('CHILD_MERGE_OUTPUT_IDENTITY_MISMATCH');
    return { ...base, actual_child_merge_receipt: descriptor.child_merge_receipt, actual_child_merge_receipt_hash: childHash };
  }
  if (!String(descriptor.logical_source_key || '')) throw new Error('LOGICAL_SOURCE_KEY_MISSING');
  requiredNonnegativeNumber(
    descriptor.logical_manifest_ordinal,
    'LOGICAL_MANIFEST_ORDINAL_INVALID'
  );
  if (requiredNumber(descriptor.physical_part_no, 'PHYSICAL_PART_NUMBER_INVALID') < 1) throw new Error('PHYSICAL_PART_NUMBER_INVALID');
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

export async function calculateReceiptRoots(physicalReceipts, options = {}) {
  if (!physicalReceipts.length || physicalReceipts.some(receipt => !receipt.physical_receipt)) throw new Error('PHYSICAL_RECEIPT_SET_INVALID');
  const maximumReceipts = Number(options.maximum_receipts || 10000);
  if (!Number.isSafeInteger(maximumReceipts) || maximumReceipts < 1
    || physicalReceipts.length > maximumReceipts) throw new Error('RECEIPT_COUNT_EXCEEDED');
  const orderedPhysical = [...physicalReceipts].sort((left, right) =>
    Number(left.logical_manifest_ordinal) - Number(right.logical_manifest_ordinal)
    || Number(left.physical_part_no) - Number(right.physical_part_no)
    || String(left.logical_source_key).localeCompare(String(right.logical_source_key))
  );
  const tuples = orderedPhysical.map(row =>
    `${row.logical_source_key}|${row.logical_manifest_ordinal}|${row.physical_part_no}`
  );
  if (new Set(tuples).size !== tuples.length) throw new Error('RECEIPT_STRUCTURE_INVALID');
  const physicalRoot = await hashJoined(orderedPhysical.map(row => row.physical_receipt));
  const grouped = new Map();
  for (const receipt of orderedPhysical) {
    const key = String(receipt.logical_source_key || '');
    if (!key) throw new Error('LOGICAL_SOURCE_KEY_MISSING');
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(receipt);
  }
  const logicalReceipts = [];
  for (const [logicalSourceKey, rows] of grouped) {
    const ordered = rows.sort((a, b) => Number(a.logical_manifest_ordinal) - Number(b.logical_manifest_ordinal) || Number(a.physical_part_no) - Number(b.physical_part_no));
    const ordinals = new Set(ordered.map(row => Number(row.logical_manifest_ordinal)));
    if (ordinals.size !== 1) throw new Error('RECEIPT_STRUCTURE_INVALID');
    const parts = ordered.map(row => Number(row.physical_part_no));
    if (parts.some((part, index) => part !== index + 1)) throw new Error('RECEIPT_STRUCTURE_INVALID');
    const ordinal = parts.length ? Number(ordered[0].logical_manifest_ordinal) : 0;
    logicalReceipts.push({ logical_source_key: logicalSourceKey, logical_manifest_ordinal: ordinal, logical_receipt: await hashPostgresJsonb({ receipt_contract: 'LOGICAL_SOURCE_RECEIPT_V3', logical_source_key: logicalSourceKey, logical_manifest_ordinal: ordinal, ordered_physical_receipts: ordered.map(row => row.physical_receipt).join('||') }) });
  }
  logicalReceipts.sort((a, b) => a.logical_manifest_ordinal - b.logical_manifest_ordinal || a.logical_source_key.localeCompare(b.logical_source_key));
  return {
    physicalRoot,
    logicalRoot: await hashJoined(logicalReceipts.map(row => row.logical_receipt)),
    logicalReceipts,
    physicalReceiptCount: orderedPhysical.length,
    logicalReceiptCount: logicalReceipts.length
  };
}

function orderedInputIdentity(descriptor, actual, receipt, index) {
  return [index + 1, descriptor.input_chunk_id, actual.r2_key, actual.sha256, actual.page_count, actual.size_bytes, receipt.actual_child_merge_receipt_hash || ''].join('|');
}

export async function buildMergeReceipt(context, identity, inputs, metadata, output) {
  if (!Array.isArray(metadata.actual_inputs) || metadata.actual_inputs.length !== inputs.length) throw new Error('ACTUAL_INPUT_COUNT_MISMATCH');
  const inputReceipts = [];
  for (let index = 0; index < inputs.length; index += 1) inputReceipts.push(await buildPhysicalReceipt(inputs[index], metadata.actual_inputs[index], index));
  const physicalReceipts = physicalReceiptsFromInputs(inputs, inputReceipts);
  const roots = await calculateReceiptRoots(physicalReceipts, {
    maximum_receipts: Number(context?.limits?.max_receipts || 10000)
  });
  const actualPageSum = metadata.actual_inputs.reduce(
    (sum, input) => sum + Number(input.page_count || 0),
    0
  );
  if (actualPageSum !== Number(output.page_count)) throw new Error('MERGE_OUTPUT_PAGE_COUNT_MISMATCH');
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
    output_size_bytes: output.size_bytes, output_page_count: output.page_count,
    page_numbering_applied: output.page_numbering_applied === true,
    page_numbering_contract: output.page_numbering_contract || null,
    page_numbering_excluded_pages:
      Array.isArray(output.page_numbering_excluded_pages)
        ? output.page_numbering_excluded_pages
        : [],
    physical_receipt_count: roots.physicalReceiptCount,
    logical_receipt_count: roots.logicalReceiptCount
  };
}

export function flattenLeafInputReceipts(mergeReceipt, output = [], options = {}, depth = 0) {
  const maximumDepth = Number(options.maximum_depth || 8);
  const maximumReceipts = Number(options.maximum_receipts || 10000);
  const state = options.__state || {
    tuples: new Set(),
    output
  };

  if (!Number.isSafeInteger(maximumDepth) || maximumDepth < 1) throw new Error('RECEIPT_DEPTH_LIMIT_INVALID');
  if (!Number.isSafeInteger(maximumReceipts) || maximumReceipts < 1) throw new Error('RECEIPT_COUNT_LIMIT_INVALID');
  if (depth > maximumDepth) throw new Error('RECEIPT_DEPTH_EXCEEDED');

  if (!mergeReceipt || mergeReceipt.receipt_contract !== 'ACTUAL_BYTES_MERGE_RECEIPT_V3' || !Array.isArray(mergeReceipt.input_receipts)) {
    throw new Error('RECEIPT_STRUCTURE_INVALID');
  }

  for (const receipt of mergeReceipt.input_receipts) {
    if (!receipt || typeof receipt !== 'object') throw new Error('RECEIPT_STRUCTURE_INVALID');

    if (receipt.actual_child_merge_receipt) {
      if (!receipt.actual_child_merge_receipt_hash) throw new Error('CHILD_MERGE_RECEIPT_HASH_MISSING');
      flattenLeafInputReceipts(receipt.actual_child_merge_receipt, output, { ...options, __state: state }, depth + 1);
      continue;
    }

    const logicalSourceKey = String(receipt.logical_source_key || '');
    const logicalOrdinal = requiredNonnegativeNumber(
      receipt.logical_manifest_ordinal,
      'LOGICAL_MANIFEST_ORDINAL_INVALID'
    );
    const physicalPartNo = Number(receipt.physical_part_no);
    const pageCount = Number(receipt.actual_page_count);
    const sizeBytes = Number(receipt.actual_size_bytes);
    const r2Key = String(receipt.actual_r2_key || '');
    const sha256 = String(receipt.actual_sha256 || '').toLowerCase();
    const physicalReceipt = String(receipt.actual_physical_receipt || '');

    if (!logicalSourceKey) throw new Error('LOGICAL_SOURCE_KEY_MISSING');
    if (!Number.isSafeInteger(physicalPartNo) || physicalPartNo < 1) throw new Error('PHYSICAL_PART_NUMBER_INVALID');
    if (!r2Key) throw new Error('INPUT_R2_KEY_MISSING');
    if (!/^[0-9a-f]{64}$/.test(sha256)) throw new Error('INPUT_SHA256_INVALID');
    if (!Number.isSafeInteger(pageCount) || pageCount < 1) throw new Error('INPUT_PAGE_COUNT_INVALID');
    if (!Number.isSafeInteger(sizeBytes) || sizeBytes < 1) throw new Error('INPUT_SIZE_INVALID');
    if (!/^[0-9a-f]{64}$/.test(physicalReceipt)) throw new Error('INPUT_PHYSICAL_RECEIPT_MISSING');

    const tuple = `${logicalSourceKey}|${logicalOrdinal}|${physicalPartNo}`;
    if (state.tuples.has(tuple)) throw new Error('RECEIPT_STRUCTURE_INVALID');
    state.tuples.add(tuple);

    output.push({
      input_chunk_id: receipt.input_chunk_id || null,
      logical_source_key: logicalSourceKey,
      logical_ordinal: String(logicalOrdinal),
      logical_manifest_ordinal: String(logicalOrdinal),
      physical_part_no: String(physicalPartNo),
      r2_key: r2Key,
      sha256,
      page_count: String(pageCount),
      size_bytes: String(sizeBytes),
      physical_receipt: physicalReceipt
    });

    if (output.length > maximumReceipts) throw new Error('RECEIPT_COUNT_EXCEEDED');
  }

  if (depth === 0) {
    const byLogical = new Map();
    for (const leaf of output) {
      const key = `${leaf.logical_source_key}|${leaf.logical_ordinal}`;
      if (!byLogical.has(key)) byLogical.set(key, []);
      byLogical.get(key).push(Number(leaf.physical_part_no));
    }
    for (const parts of byLogical.values()) {
      const ordered = [...parts].sort((a, b) => a - b);
      for (let index = 0; index < ordered.length; index += 1) {
        if (ordered[index] !== index + 1) throw new Error('RECEIPT_STRUCTURE_INVALID');
      }
    }
  }

  return output;
}

export async function verifyMergeReceiptTree(mergeReceipt, options = {}) {
  const maximumDepth = Number(options.maximum_depth || 8);
  const maximumReceipts = Number(options.maximum_receipts || 10000);

  const assertReceipt = async (receipt, depth = 0) => {
    if (depth > maximumDepth) throw new Error('RECEIPT_DEPTH_EXCEEDED');
    if (!receipt || receipt.receipt_contract !== 'ACTUAL_BYTES_MERGE_RECEIPT_V3' || !Array.isArray(receipt.input_receipts)) {
      throw new Error('RECEIPT_STRUCTURE_INVALID');
    }
    if (!receipt.output_r2_key || !/^[0-9a-f]{64}$/i.test(String(receipt.output_sha256 || ''))) {
      throw new Error('MERGE_OUTPUT_IDENTITY_INVALID');
    }
    if (!Number.isSafeInteger(Number(receipt.output_page_count)) || Number(receipt.output_page_count) < 1) {
      throw new Error('MERGE_OUTPUT_PAGE_COUNT_INVALID');
    }
    if (!Number.isSafeInteger(Number(receipt.output_size_bytes)) || Number(receipt.output_size_bytes) < 1) {
      throw new Error('MERGE_OUTPUT_SIZE_INVALID');
    }

    const directInputReceipts = [];
    const physicalReceipts = [];

    for (let index = 0; index < receipt.input_receipts.length; index += 1) {
      const inputReceipt = receipt.input_receipts[index];
      if (!inputReceipt || typeof inputReceipt !== 'object') throw new Error('RECEIPT_STRUCTURE_INVALID');

      const inputOrder = Number(inputReceipt.actual_input_order);
      if (!Number.isSafeInteger(inputOrder) || inputOrder !== index + 1) throw new Error('INPUT_ORDER_MISMATCH');

      if (!inputReceipt.actual_r2_key || !/^[0-9a-f]{64}$/i.test(String(inputReceipt.actual_sha256 || ''))) {
        throw new Error('INPUT_IDENTITY_INVALID');
      }
      if (!Number.isSafeInteger(Number(inputReceipt.actual_page_count)) || Number(inputReceipt.actual_page_count) < 1) {
        throw new Error('INPUT_PAGE_COUNT_INVALID');
      }
      if (!Number.isSafeInteger(Number(inputReceipt.actual_size_bytes)) || Number(inputReceipt.actual_size_bytes) < 1) {
        throw new Error('INPUT_SIZE_INVALID');
      }

      if (inputReceipt.actual_child_merge_receipt) {
        if (!inputReceipt.actual_child_merge_receipt_hash) {
          throw new Error('CHILD_MERGE_RECEIPT_HASH_MISSING');
        }
        const child = await assertReceipt(inputReceipt.actual_child_merge_receipt, depth + 1);
        const childHash = await hashPostgresJsonb(inputReceipt.actual_child_merge_receipt);
        if (inputReceipt.actual_child_merge_receipt_hash !== childHash) {
          throw new Error('CHILD_MERGE_RECEIPT_HASH_MISMATCH');
        }
        if (String(inputReceipt.actual_child_merge_receipt.output_r2_key || '') !== String(inputReceipt.actual_r2_key || '')) {
          throw new Error('CHILD_MERGE_OUTPUT_IDENTITY_MISMATCH');
        }
        if (String(inputReceipt.actual_child_merge_receipt.output_sha256 || '') !== String(inputReceipt.actual_sha256 || '')) {
          throw new Error('CHILD_MERGE_OUTPUT_IDENTITY_MISMATCH');
        }
        if (String(inputReceipt.actual_child_merge_receipt.output_page_count || '') !== String(inputReceipt.actual_page_count || '')) {
          throw new Error('CHILD_MERGE_OUTPUT_IDENTITY_MISMATCH');
        }
        if (String(inputReceipt.actual_child_merge_receipt.output_size_bytes || '') !== String(inputReceipt.actual_size_bytes || '')) {
          throw new Error('CHILD_MERGE_OUTPUT_IDENTITY_MISMATCH');
        }
        physicalReceipts.push(...child.physicalReceipts);
        directInputReceipts.push({
          receipt: inputReceipt,
          childHash
        });
        continue;
      }

      const logicalSourceKey = String(inputReceipt.logical_source_key || '');
      const logicalOrdinal = requiredNonnegativeNumber(
        inputReceipt.logical_manifest_ordinal,
        'LOGICAL_MANIFEST_ORDINAL_INVALID'
      );
      const physicalPartNo = Number(inputReceipt.physical_part_no);

      if (!logicalSourceKey) throw new Error('LOGICAL_SOURCE_KEY_MISSING');
      if (!Number.isSafeInteger(physicalPartNo) || physicalPartNo < 1) throw new Error('PHYSICAL_PART_NUMBER_INVALID');

      const recalculatedPhysicalReceipt = await hashPostgresJsonb({
        receipt_contract: 'ACTUAL_BYTES_OBJECT_RECEIPT_V3',
        logical_source_key: logicalSourceKey,
        logical_manifest_ordinal: logicalOrdinal,
        physical_part_no: physicalPartNo,
        object_key: inputReceipt.actual_r2_key,
        stored_sha256: inputReceipt.actual_sha256,
        expected_page_count: Number(inputReceipt.actual_page_count),
        expected_byte_count: Number(inputReceipt.actual_size_bytes)
      });

      if (inputReceipt.actual_physical_receipt !== recalculatedPhysicalReceipt) {
        throw new Error('INPUT_PHYSICAL_RECEIPT_MISMATCH');
      }

      physicalReceipts.push({
        logical_source_key: logicalSourceKey,
        logical_manifest_ordinal: logicalOrdinal,
        physical_part_no: physicalPartNo,
        physical_receipt: recalculatedPhysicalReceipt,
        input_chunk_id: inputReceipt.input_chunk_id || null,
        r2_key: inputReceipt.actual_r2_key,
        sha256: inputReceipt.actual_sha256,
        page_count: String(inputReceipt.actual_page_count),
        size_bytes: String(inputReceipt.actual_size_bytes)
      });

      directInputReceipts.push({
        receipt: inputReceipt,
        childHash: ''
      });
    }

    if (physicalReceipts.length > maximumReceipts) throw new Error('RECEIPT_COUNT_EXCEEDED');

    const roots = await calculateReceiptRoots(physicalReceipts, { maximum_receipts: maximumReceipts });

    const actualChildReceiptHash = await hashJoined(receipt.input_receipts.map(inputReceipt =>
      inputReceipt.actual_physical_receipt || inputReceipt.actual_child_merge_receipt_hash
    ));

    const actualOrderedInputHash = await hashJoined(receipt.input_receipts.map((inputReceipt, index) => [
      index + 1,
      inputReceipt.input_chunk_id,
      inputReceipt.actual_r2_key,
      inputReceipt.actual_sha256,
      inputReceipt.actual_page_count,
      inputReceipt.actual_size_bytes,
      inputReceipt.actual_child_merge_receipt_hash || ''
    ].join('|')));

    const outputPageCount = Number(receipt.output_page_count);
    const childPageSum = receipt.input_receipts.reduce((sum, inputReceipt) =>
      sum + Number(inputReceipt.actual_page_count || 0), 0
    );

    if (!Number.isSafeInteger(outputPageCount) || outputPageCount < 1) throw new Error('MERGE_OUTPUT_PAGE_COUNT_INVALID');
    if (childPageSum !== outputPageCount) throw new Error('MERGE_OUTPUT_PAGE_COUNT_MISMATCH');

    const requiredRootFields = [
      ['actual_child_receipt_hash', receipt.actual_child_receipt_hash],
      ['actual_ordered_input_hash', receipt.actual_ordered_input_hash],
      ['combined_logical_receipt_root', receipt.combined_logical_receipt_root],
      ['combined_physical_receipt_root', receipt.combined_physical_receipt_root]
    ];
    for (const [field, value] of requiredRootFields) {
      if (!/^[0-9a-f]{64}$/i.test(String(value || ''))) {
        throw new Error(`MERGE_RECEIPT_ROOT_FIELD_MISSING:${field}`);
      }
    }

    const assertions = [
      [receipt.actual_child_receipt_hash, actualChildReceiptHash, 'CHILD_RECEIPT_ROOT_MISMATCH'],
      [receipt.actual_ordered_input_hash, actualOrderedInputHash, 'ORDERED_INPUT_ROOT_MISMATCH'],
      [receipt.combined_logical_receipt_root, roots.logicalRoot, 'LOGICAL_RECEIPT_ROOT_MISMATCH'],
      [receipt.combined_physical_receipt_root, roots.physicalRoot, 'PHYSICAL_RECEIPT_ROOT_MISMATCH']
    ];

    for (const [expected, actual, code] of assertions) {
      if (expected !== actual) throw new Error(code);
    }

    return {
      receipt,
      logicalRoot: roots.logicalRoot,
      physicalRoot: roots.physicalRoot,
      orderedInputRoot: actualOrderedInputHash,
      childReceiptHash: actualChildReceiptHash,
      physicalReceipts,
      physicalReceiptCount: roots.physicalReceiptCount,
      logicalReceiptCount: roots.logicalReceiptCount,
      pageCount: outputPageCount,
      output: {
        r2_key: receipt.output_r2_key,
        sha256: receipt.output_sha256,
        size_bytes: receipt.output_size_bytes,
        page_count: receipt.output_page_count
      }
    };
  };

  const verified = await assertReceipt(mergeReceipt, 0);
  const leaves = flattenLeafInputReceipts(mergeReceipt, [], {
    maximum_depth: maximumDepth,
    maximum_receipts: maximumReceipts
  }).sort((a, b) =>
    Number(a.logical_ordinal) - Number(b.logical_ordinal)
    || Number(a.physical_part_no) - Number(b.physical_part_no)
    || String(a.logical_source_key).localeCompare(String(b.logical_source_key))
  );

  // This must match the database completion authority's expected_physical_input_hash
  // formula: logical ordinal, physical part, object key and object hash only.
  const physicalInputHash = await hashJoined(leaves.map(row => [
    row.logical_ordinal,
    row.physical_part_no,
    row.r2_key,
    row.sha256
  ].join('|')));

  return {
    ...verified,
    leaves,
    physicalInputHash,
    // The database DOCUMENT_VERIFY payload uses expected_ordered_input_root for the
    // root child-receipt hash, not the lower-level physical ordered-input hash.
    // Preserve both values: orderedInputRoot remains the merge receipt's actual
    // ordered input hash, while childReceiptHash is the root value used for
    // final document-root identity.
    rootIdentity: await hashPostgresJsonb({
      receipt_contract: 'DOCUMENT_ROOT_RECEIPT_V3',
      logical_root: verified.logicalRoot,
      physical_root: verified.physicalRoot,
      ordered_input_root: verified.childReceiptHash,
      page_count: Number(verified.pageCount),
      output_sha256: verified.output.sha256
    })
  };
}
