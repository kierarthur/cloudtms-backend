const SHA256 = /^[0-9a-f]{64}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export const WEEKLY_SOURCE_INVOICE_BATCH_CANDIDATES_CONTRACT =
  'WEEKLY_SOURCE_INVOICE_BATCH_CANDIDATES_V1';
export const WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_CONTRACT =
  'WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_V1';
export const WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_CONTRACT =
  'WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_V1';

function fail(code, status = 400) {
  throw Object.assign(new Error(code), { code, status });
}

function object(value, code = 'WEEKLY_SOURCE_INVOICE_BATCH_CONTRACT_INVALID') {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail(code);
  return value;
}

function rpcValue(value, functionName) {
  let result = value;
  if (Array.isArray(result) && result.length === 1) result = result[0];
  if (result && typeof result === 'object' && !Array.isArray(result)
      && Object.prototype.hasOwnProperty.call(result, functionName)) {
    result = result[functionName];
  }
  if (Array.isArray(result) && result.length === 1) result = result[0];
  return result;
}

function snapshotHash(value, { allowNull = false } = {}) {
  const text = String(value ?? '').trim().toLowerCase();
  if (!text && allowNull) return null;
  if (!SHA256.test(text)) fail('WEEKLY_SOURCE_INVOICE_BATCH_SNAPSHOT_INVALID');
  return text;
}

function sourceSelectionKey(value) {
  const text = String(value ?? '').trim().toLowerCase();
  if (!/^weekly-source-manifest:[0-9a-f-]{36}$/.test(text)) {
    fail('WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_KEY_INVALID');
  }
  return text;
}

function validateSourceRow(value) {
  const row = object(value);
  const selectionKey = sourceSelectionKey(row.selection_key);
  const manifestId = String(row.client_manifest_id ?? '').trim().toLowerCase();
  const cycleId = String(row.source_cycle_id ?? '').trim().toLowerCase();
  if (!UUID.test(manifestId) || !UUID.test(cycleId)
      || selectionKey !== `weekly-source-manifest:${manifestId}`
      || !SHA256.test(String(row.source_revision ?? '').trim().toLowerCase())
      || String(row.source_kind || '').trim().toUpperCase() !== 'WEEKLY_FINAL_SOURCE'
      || typeof row.selectable !== 'boolean'
      || !Array.isArray(row.candidate_ids)
      || !Array.isArray(row.candidate_names)
      || !Array.isArray(row.action_blocker_codes)
      || !Array.isArray(row.informational_codes)) {
    fail('WEEKLY_SOURCE_INVOICE_BATCH_CONTRACT_INVALID', 502);
  }
  return row;
}

export function splitWeeklySourceCandidateRequest(rawBody, action) {
  const body = object(rawBody, 'BATCH_QUERY_INVALID');
  const ordinaryBody = { ...body };
  const supplied = Object.prototype.hasOwnProperty.call(
    ordinaryBody,
    'weekly_source_snapshot_hash'
  );
  const hash = supplied
    ? snapshotHash(ordinaryBody.weekly_source_snapshot_hash, { allowNull: true })
    : null;
  delete ordinaryBody.weekly_source_snapshot_hash;
  if (supplied && String(action || '').trim().toUpperCase() !== 'GENERATE') {
    fail('BATCH_QUERY_UNKNOWN_FIELD');
  }
  return {
    ordinaryBody,
    weeklySourceRequested: supplied,
    weeklySourceSnapshotHash: hash
  };
}

function sourceRpcRequest(queryValue, sourceHash, options = {}) {
  const query = object(queryValue);
  const mode = String(options.mode || query.mode || 'PAGE').trim().toUpperCase();
  const request = {
    mode,
    snapshot_hash: sourceHash || null,
    filters: structuredClone(query.filters || {}),
    sort: structuredClone(query.sort || {}),
    selection: structuredClone(query.selection || {})
  };
  if (mode === 'PAGE' || mode === 'EXPLICIT_KEYS') {
    request.page_size = mode === 'PAGE'
      ? 5000
      : Math.max(1, Math.min(5000, Number(query.page_size || 1)));
  }
  const keys = options.selectionKeys || query.selection_keys;
  const revisions = options.expectedSourceRevisions || query.expected_source_revisions;
  if (Array.isArray(keys)) request.selection_keys = keys.map(sourceSelectionKey);
  if (revisions && typeof revisions === 'object' && !Array.isArray(revisions)) {
    request.expected_source_revisions = Object.fromEntries(
      Object.entries(revisions).map(([key, revision]) => [
        sourceSelectionKey(key),
        snapshotHash(revision)
      ])
    );
  }
  return request;
}

export async function loadWeeklySourceInvoiceBatchCandidates(deps, query, sourceHash, options = {}) {
  const functionName = 'weekly_source_invoice_batch_candidates_v1';
  const raw = await deps.rpc(functionName, {
    p_request: sourceRpcRequest(query, sourceHash, options)
  });
  const value = object(rpcValue(raw, functionName));
  const mode = String(options.mode || query.mode || 'PAGE').trim().toUpperCase();
  const totalCount = Number(value.page?.total_count);
  const returnedCount = Number(value.page?.returned_count);
  const selectedTotal = Number(value.selection_summary?.selected_total);
  if (value.contract_version !== WEEKLY_SOURCE_INVOICE_BATCH_CANDIDATES_CONTRACT
      || String(value.mode || '').trim().toUpperCase() !== mode
      || !SHA256.test(String(value.snapshot_hash || '').trim().toLowerCase())
      || !Array.isArray(value.rows)
      || !value.page || typeof value.page !== 'object' || Array.isArray(value.page)
      || !value.selection_summary || typeof value.selection_summary !== 'object'
      || value.selection_summary.exact !== true
      || !Number.isSafeInteger(totalCount) || totalCount < 0
      || !Number.isSafeInteger(returnedCount) || returnedCount < 0
      || returnedCount !== value.rows.length
      || !Number.isSafeInteger(selectedTotal) || selectedTotal < 0
      || (mode === 'PAGE' && totalCount !== returnedCount)
      || !Array.isArray(value.selected_manifest_refs)
      || (mode === 'CONFIRM'
        && value.selected_manifest_refs.length !== selectedTotal)) {
    fail('WEEKLY_SOURCE_INVOICE_BATCH_CONTRACT_INVALID', 502);
  }
  value.rows.forEach(validateSourceRow);
  const keys = new Set();
  for (const row of value.rows) {
    if (keys.has(row.selection_key)) fail('WEEKLY_SOURCE_INVOICE_BATCH_CONTRACT_INVALID', 502);
    keys.add(row.selection_key);
  }
  return value;
}

function numeric(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) && number >= 0 ? number : 0;
}

export function mergeWeeklySourceCandidateEnvelope(ordinaryEnvelope, sourceEnvelope) {
  const ordinary = object(ordinaryEnvelope);
  const source = object(sourceEnvelope);
  const ordinarySummary = object(ordinary.selection_summary);
  const sourceSummary = object(source.selection_summary);
  const mode = String(ordinary.mode || '').trim().toUpperCase();
  const topLevelRows = mode === 'EXPLICIT_KEYS'
    && Array.isArray(ordinary.rows) && ordinary.rows.length === 0
    ? source.rows
    : ordinary.rows;
  return {
    ...ordinary,
    rows: topLevelRows,
    selection_summary: {
      ...ordinarySummary,
      exact: ordinarySummary.exact === true && sourceSummary.exact === true,
      eligible_total: numeric(ordinarySummary.eligible_total) + numeric(sourceSummary.eligible_total),
      selected_total: numeric(ordinarySummary.selected_total) + numeric(sourceSummary.selected_total),
      blocked_total: numeric(ordinarySummary.blocked_total) + numeric(sourceSummary.blocked_total),
      ordinary_selected_total: numeric(ordinarySummary.selected_total),
      weekly_source_selected_total: numeric(sourceSummary.selected_total)
    },
    totals: {
      ...(ordinary.totals || {}),
      weekly_source_total: numeric(source.page?.total_count),
      weekly_source_ready_total: numeric(sourceSummary.eligible_total),
      weekly_source_blocked_total: numeric(sourceSummary.blocked_total)
    },
    weekly_source: {
      contract_version: source.contract_version,
      snapshot_hash: source.snapshot_hash,
      rows: source.rows,
      page: source.page,
      selection_summary: source.selection_summary
    }
  };
}

export function normaliseWeeklySourceInvoiceBatchSelectionContract(value) {
  const contract = object(value, 'WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_INVALID');
  if (Object.keys(contract).some(key => ![
    'contract_version', 'snapshot_hash'
  ].includes(key))
      || contract.contract_version !== WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_CONTRACT) {
    fail('WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_INVALID');
  }
  return {
    contract_version: WEEKLY_SOURCE_INVOICE_BATCH_SELECTION_CONTRACT,
    snapshot_hash: snapshotHash(contract.snapshot_hash)
  };
}

export function isWeeklySourceSelectionKey(value) {
  return /^weekly-source-manifest:[0-9a-f-]{36}$/i.test(String(value || '').trim());
}

export async function preflightWeeklySourceInvoiceBatch(
  deps,
  query,
  sourceContract,
  options = {}
) {
  const contract = normaliseWeeklySourceInvoiceBatchSelectionContract(sourceContract);
  const result = await loadWeeklySourceInvoiceBatchCandidates(
    deps,
    query,
    contract.snapshot_hash,
    {
      mode: 'CONFIRM',
      selectionKeys: options.selectionKeys,
      expectedSourceRevisions: options.expectedSourceRevisions
    }
  );
  for (const ref of result.selected_manifest_refs) {
    const item = object(ref);
    if (!UUID.test(String(item.client_manifest_id || ''))
        || !UUID.test(String(item.client_id || ''))
        || !UUID.test(String(item.source_cycle_id || ''))
        || !SHA256.test(String(item.expected_manifest_hash || ''))
        || !isWeeklySourceSelectionKey(item.selection_key)) {
      fail('WEEKLY_SOURCE_INVOICE_BATCH_CONTRACT_INVALID', 502);
    }
  }
  return result;
}

export async function admitWeeklySourceInvoiceBatch(
  deps,
  actorUserId,
  commandToken,
  preflight
) {
  const refs = preflight?.selected_manifest_refs;
  if (!Array.isArray(refs) || refs.length === 0) return null;
  const functionName = 'weekly_source_invoice_batch_admit_atomic_v1';
  const raw = await deps.rpc(functionName, {
    p_request: {
      actor_user_id: String(actorUserId || '').trim().toLowerCase(),
      command_token: String(commandToken || '').trim(),
      snapshot_hash: String(preflight.snapshot_hash || '').trim().toLowerCase(),
      selected_manifests: refs
    }
  });
  const value = object(rpcValue(raw, functionName));
  if (value.ok !== true
      || value.contract_version !== WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_CONTRACT
      || value.atomic !== true
      || !Array.isArray(value.per_manifest_results)
      || Number(value.selected_count) !== value.per_manifest_results.length
      || value.per_manifest_results.length !== refs.length) {
    fail('WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_CONTRACT_INVALID', 502);
  }
  for (const row of value.per_manifest_results) {
    if (!isWeeklySourceSelectionKey(row?.selection_key)
        || !UUID.test(String(row?.client_manifest_id || ''))
        || !Array.isArray(row?.invoice_ids)
        || row.invoice_ids.some(id => !UUID.test(String(id || '')))) {
      fail('WEEKLY_SOURCE_INVOICE_BATCH_ADMISSION_CONTRACT_INVALID', 502);
    }
  }
  return value;
}

export function weeklySourceAdmissionInvoiceId(admission) {
  const ids = [...new Set(
    (admission?.per_manifest_results || [])
      .flatMap(row => Array.isArray(row?.invoice_ids) ? row.invoice_ids : [])
      .map(value => String(value || '').trim().toLowerCase())
      .filter(value => UUID.test(value))
  )];
  return ids.length === 1 ? ids[0] : null;
}

export const weeklySourceInvoiceBatchInternals = Object.freeze({
  snapshotHash,
  sourceSelectionKey,
  sourceRpcRequest,
  validateSourceRow,
  rpcValue
});
