import puppeteer from '@cloudflare/puppeteer';
import { PDFDocument } from 'pdf-lib';
import {
  buildInvoiceSourceDocumentHtml,
  buildProfessionalInvoiceHtml
} from './invoice-document-templates.js';
import { signInvoiceDrainRequest, verifyInvoiceDrainSignature } from './invoice-queue-security.js';
import {
  invoiceProcessorIdentityHeaders,
  invoiceProcessorSha256Hex,
  signInvoiceProcessorRequest
} from '../../shared/invoice-processor-security.js';
import { validateFrozenPresentationModel } from './invoice-presentation-contract.js';

export const INVOICE_DATABASE_CHUNK_TYPES = Object.freeze([
  'GENERATION_GROUP',
  'DOCUMENT_PLAN',
  'ISSUE_INVOICE',
  'DELIVERY_PREPARE',
  'RECONCILE'
]);

export const INVOICE_DOCUMENT_CHUNK_TYPES = Object.freeze([
  'ASSET_INSPECT',
  'ASSET_NORMALISE',
  'SOURCE_RENDER',
  'INVOICE_CORE_RENDER',
  'PDF_MERGE',
  'DOCUMENT_VERIFY'
]);

const BROWSER_ACTIONS = new Set(['SOURCE_RENDER', 'INVOICE_CORE_RENDER']);
const NATIVE_ACTIONS = new Set([
  'ASSET_INSPECT',
  'ASSET_NORMALISE',
  'PDF_MERGE',
  'DOCUMENT_VERIFY'
]);
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const PROCESSOR_POLICY_VERSION = 'INVOICE_PROCESSOR_LIMITS_V4';
const activeNudges = new Set();

export function parseBoundedInteger(value, fallback, minimum, maximum) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return fallback;
  return Math.max(minimum, Math.min(maximum, Math.trunc(numeric)));
}

export function parseBooleanFlag(value, fallback = false) {
  const normalised = String(value ?? '').trim().toLowerCase();
  if (!normalised) return fallback;
  if (normalised === 'true') return true;
  if (normalised === 'false') return false;
  return fallback;
}

function resolveActionConcurrency(env) {
  return Object.freeze({
    asset: parseBoundedInteger(env.INVOICE_ASSET_CONCURRENCY, 2, 1, 8),
    sourceRender: parseBoundedInteger(env.INVOICE_SOURCE_RENDER_CONCURRENCY, 2, 1, 8),
    coreRender: parseBoundedInteger(env.INVOICE_CORE_RENDER_CONCURRENCY, 2, 1, 8),
    merge: parseBoundedInteger(env.INVOICE_MERGE_CONCURRENCY, 1, 1, 4),
    verify: parseBoundedInteger(env.INVOICE_VERIFY_CONCURRENCY, 2, 1, 8)
  });
}

function resolveNudgeBudget(env) {
  return Object.freeze({
    database: parseBoundedInteger(env.INVOICE_USER_NUDGE_DATABASE_CLAIM_LIMIT, 4, 1, 20),
    document: parseBoundedInteger(env.INVOICE_USER_NUDGE_DOCUMENT_CLAIM_LIMIT, 2, 1, 10),
    deadlineMs: parseBoundedInteger(env.INVOICE_USER_NUDGE_DEADLINE_MS, 8000, 2000, 20000),
    cycles: parseBoundedInteger(env.INVOICE_USER_NUDGE_CYCLES, 2, 1, 2)
  });
}

function resolveScheduledBudget(env) {
  return Object.freeze({
    database: parseBoundedInteger(env.INVOICE_SCHEDULED_DATABASE_CLAIM_LIMIT, 50, 1, 100),
    document: parseBoundedInteger(env.INVOICE_SCHEDULED_DOCUMENT_CLAIM_LIMIT, 12, 1, 100),
    deadlineMs: parseBoundedInteger(env.INVOICE_SCHEDULED_DRAIN_DEADLINE_MS, 22000, 2000, 25000),
    cycles: parseBoundedInteger(env.INVOICE_SCHEDULED_MAX_CYCLES, 4, 1, 10)
  });
}

export function isInvoiceAsyncPipelineEnabled(env) {
  return parseBooleanFlag(env?.INVOICE_ASYNC_PIPELINE_ENABLED);
}

export function getInvoiceQueueRuntimeConfig(env = {}) {
  const concurrency = resolveActionConcurrency(env);
  const nudge = resolveNudgeBudget(env);
  const scheduled = resolveScheduledBudget(env);
  const config = {
    enabled: isInvoiceAsyncPipelineEnabled(env),
    scheduledEnabled: parseBooleanFlag(env.INVOICE_ASYNC_SCHEDULED_ENABLED),
    processorEnabled: parseBooleanFlag(env.INVOICE_DOCUMENT_PROCESSOR_ENABLED),
    continuationEnabled: parseBooleanFlag(env.INVOICE_QUEUE_CONTINUATION_ENABLED, true),
    workerId: String(env.INVOICE_QUEUE_WORKER_ID || 'cloudtms-invoice-worker').slice(0, 120),
    databaseConcurrency: parseBoundedInteger(env.INVOICE_DATABASE_CONCURRENCY, 1, 1, 4),
    assetConcurrency: concurrency.asset,
    sourceRenderConcurrency: concurrency.sourceRender,
    coreRenderConcurrency: concurrency.coreRender,
    mergeConcurrency: concurrency.merge,
    verifyConcurrency: concurrency.verify,
    userNudgeDatabaseClaimLimit: nudge.database,
    userNudgeDocumentClaimLimit: nudge.document,
    scheduledDatabaseClaimLimit: scheduled.database,
    scheduledDocumentClaimLimit: scheduled.document,
    userNudgeDeadlineMs: nudge.deadlineMs,
    scheduledDrainDeadlineMs: scheduled.deadlineMs,
    userNudgeCycles: nudge.cycles,
    scheduledCycles: scheduled.cycles,
    leaseSeconds: parseBoundedInteger(env.INVOICE_QUEUE_LEASE_SECONDS, 120, 15, 600),
    maximumContinuationDepth: 4,
    safetyMarginMs: parseBoundedInteger(env.INVOICE_QUEUE_SAFETY_MARGIN_MS, 1500, 250, 5000),
    heartbeatMs: parseBoundedInteger(env.INVOICE_PROCESSOR_HEARTBEAT_MS, 20000, 5000, 60000),
    heartbeatRpcTimeoutMs: parseBoundedInteger(env.INVOICE_HEARTBEAT_RPC_TIMEOUT_MS, 8000, 1000, 30000),
    finalTouchRpcTimeoutMs: parseBoundedInteger(env.INVOICE_FINAL_TOUCH_RPC_TIMEOUT_MS, 8000, 1000, 30000),
    nativeRequestTimeoutMs: parseBoundedInteger(env.INVOICE_NATIVE_REQUEST_TIMEOUT_MS, 120000, 10000, 300000),
    browserRenderTimeoutMs: parseBoundedInteger(env.INVOICE_BROWSER_RENDER_TIMEOUT_MS, 45000, 5000, 90000),
    browserRenderOutputMaxBytes: parseBoundedInteger(env.INVOICE_BROWSER_RENDER_OUTPUT_MAX_BYTES, 16777216, 1048576, 67108864),
    browserRenderOutputMaxPages: parseBoundedInteger(env.INVOICE_BROWSER_RENDER_OUTPUT_MAX_PAGES, 250, 1, 1000),
    browserInMemoryPdfMaxBytes: parseBoundedInteger(env.INVOICE_BROWSER_MEMORY_PDF_MAX_BYTES, 16777216, 1048576, 33554432),
    heartbeatFailureAbortMarginMs: parseBoundedInteger(env.INVOICE_HEARTBEAT_FAILURE_ABORT_MARGIN_MS, 30000, 5000, 90000),
    maximumConsecutiveHeartbeatFailures: parseBoundedInteger(env.INVOICE_MAXIMUM_CONSECUTIVE_HEARTBEAT_FAILURES, 2, 1, 5),
    finalOwnershipCheckRequired: parseBooleanFlag(env.INVOICE_FINAL_OWNERSHIP_CHECK_REQUIRED, true),
    maximumEmbeddedRenderAssets: parseBoundedInteger(env.INVOICE_MAXIMUM_EMBEDDED_RENDER_ASSETS, 4, 1, 16),
    maximumEmbeddedRenderAssetBytes: parseBoundedInteger(env.INVOICE_MAXIMUM_EMBEDDED_RENDER_ASSET_BYTES, 4194304, 262144, 16777216),
    reconciliationPageSize: parseBoundedInteger(env.INVOICE_RECONCILIATION_PAGE_SIZE, 100, 1, 100),
    reconciliationMaximumPagesPerInvocation: parseBoundedInteger(env.INVOICE_RECONCILIATION_MAXIMUM_PAGES, 4, 1, 4),
    userNudgeProcessesExternalWork: parseBooleanFlag(env.INVOICE_USER_NUDGE_PROCESSES_EXTERNAL_WORK, false),
    autoStartBatchSize: parseBoundedInteger(env.INVOICE_AUTO_START_BATCH_SIZE, 500, 1, 1000),
    autoMaximumStartBatchesPerInvocation: parseBoundedInteger(env.INVOICE_AUTO_MAX_START_BATCHES, 2, 1, 10),
    expectedDbContract: String(env.INVOICE_ASYNC_EXPECTED_DB_CONTRACT || '').trim(),
    expectedFunctionManifest: String(env.INVOICE_ASYNC_EXPECTED_FUNCTION_MANIFEST || '').trim().toLowerCase(),
    invoiceAsyncBuildId: String(env.INVOICE_ASYNC_BUILD_ID || '').trim().slice(0, 200),
    candidateCursorTtlSeconds: parseBoundedInteger(
      env.INVOICE_BATCH_CANDIDATE_CURSOR_TTL_SECONDS,
      1800,
      60,
      1800
    ),
    resultCursorTtlSeconds: parseBoundedInteger(
      env.INVOICE_BATCH_RESULT_CURSOR_TTL_SECONDS,
      86400,
      300,
      86400
    ),
    batchRequestMaxBytes: parseBoundedInteger(
      env.INVOICE_BATCH_REQUEST_MAX_BYTES,
      4194304,
      65536,
      4194304
    ),
    summaryMaximumKeys: parseBoundedInteger(
      env.INVOICE_BATCH_SUMMARY_MAX_KEYS,
      25000,
      100,
      25000
    ),
    summaryTimeoutMs: parseBoundedInteger(
      env.INVOICE_BATCH_SUMMARY_TIMEOUT_MS,
      10000,
      1000,
      10000
    ),
    expectedProcessorImplementationVersion: String(
      env.INVOICE_EXPECTED_PROCESSOR_IMPLEMENTATION_VERSION
        || 'cloudtms-invoice-document-worker-v6'
    ).trim(),
    nativeToolReadinessRequired: parseBooleanFlag(
      env.INVOICE_NATIVE_TOOL_READINESS_REQUIRED,
      true
    ),
    processorPolicyVersion: String(env.INVOICE_PROCESSOR_POLICY_VERSION || PROCESSOR_POLICY_VERSION),
    systemActorUserId: String(env.INVOICE_ACTOR_USER_ID || '').trim()
  };
  return Object.freeze(config);
}

export function validateQueueRuntimeConfiguration(env = {}) {
  const config = getInvoiceQueueRuntimeConfig(env);
  const errors = [];
  if (config.processorPolicyVersion !== PROCESSOR_POLICY_VERSION) errors.push('INVOICE_PROCESSOR_POLICY_VERSION_INVALID');
  if (config.maximumContinuationDepth !== 4) errors.push('INVOICE_CONTINUATION_DEPTH_INVALID');
  if (config.enabled && !config.processorEnabled) errors.push('INVOICE_PROCESSOR_DISABLED');
  if (config.enabled && !config.systemActorUserId) errors.push('INVOICE_ACTOR_USER_ID_MISSING');
  if (config.enabled && !env.INVOICE_QUEUE_DISPATCHER) errors.push('INVOICE_QUEUE_DISPATCHER_BINDING_MISSING');
  if (config.processorEnabled && !env.INVOICE_DOCUMENT_PROCESSOR) errors.push('INVOICE_DOCUMENT_PROCESSOR_BINDING_MISSING');
  if (config.processorEnabled && !env.INVOICE_DOCUMENT_PROCESSOR_SECRET) errors.push('INVOICE_DOCUMENT_PROCESSOR_SECRET_MISSING');
  if (config.browserRenderOutputMaxBytes > config.browserInMemoryPdfMaxBytes) errors.push('INVOICE_BROWSER_MEMORY_LIMIT_INVALID');
  if (config.heartbeatFailureAbortMarginMs >= config.leaseSeconds * 1000) errors.push('INVOICE_HEARTBEAT_ABORT_MARGIN_INVALID');
  if (config.heartbeatRpcTimeoutMs >= config.heartbeatMs) errors.push('INVOICE_HEARTBEAT_RPC_TIMEOUT_INVALID');
  if (config.finalTouchRpcTimeoutMs >= config.heartbeatFailureAbortMarginMs) errors.push('INVOICE_FINAL_TOUCH_RPC_TIMEOUT_INVALID');
  if (config.processorEnabled && !config.expectedProcessorImplementationVersion) errors.push('INVOICE_PROCESSOR_IMPLEMENTATION_VERSION_MISSING');
  if (config.userNudgeProcessesExternalWork) errors.push('INVOICE_USER_NUDGE_EXTERNAL_WORK_UNSAFE');
  if (config.expectedDbContract !== 'INVOICE_ASYNC_DB_V2') {
    errors.push('INVOICE_ASYNC_EXPECTED_DB_CONTRACT_INVALID');
  }
  if (!/^[0-9a-f]{64}$/.test(config.expectedFunctionManifest)) {
    errors.push('INVOICE_ASYNC_EXPECTED_FUNCTION_MANIFEST_INVALID');
  }
  if (!config.invoiceAsyncBuildId) {
    errors.push('INVOICE_ASYNC_BUILD_ID_MISSING');
  } else if (/^invoice-async-v8-local-uncommitted$/i.test(config.invoiceAsyncBuildId)) {
    errors.push('INVOICE_ASYNC_BUILD_ID_UNRELEASED');
  }
  const candidateCursorSecret = env.INVOICE_BATCH_CANDIDATE_CURSOR_SECRET || env.SESSION_TOKEN_SECRET;
  const resultCursorSecret = env.INVOICE_BATCH_RESULT_CURSOR_SECRET || env.SESSION_TOKEN_SECRET;
  if (!candidateCursorSecret || String(candidateCursorSecret).length < 32) {
    errors.push('INVOICE_BATCH_CANDIDATE_CURSOR_SECRET_MISSING');
  }
  if (!resultCursorSecret || String(resultCursorSecret).length < 32) {
    errors.push('INVOICE_BATCH_RESULT_CURSOR_SECRET_MISSING');
  }
  return { ok: errors.length === 0, errors, config };
}

function rowsFromRpc(value) {
  if (Array.isArray(value)) return value;
  if (value && Array.isArray(value.rows)) {
    if (value.rows.length === 1 && Array.isArray(value.rows[0])) return value.rows[0];
    return value.rows;
  }
  if (value && Array.isArray(value.data)) return value.data;
  return value == null ? [] : [value];
}

function valueFromRpc(value) {
  const rows = rowsFromRpc(value);
  if (rows.length === 1 && rows[0] && typeof rows[0] === 'object') {
    const values = Object.values(rows[0]);
    if (values.length === 1 && (Array.isArray(values[0]) || typeof values[0] === 'object')) {
      return values[0];
    }
  }
  return rows;
}

export function claimIdentity(row) {
  return {
    chunk_id: row.chunk_id,
    lease_token: row.lease_token,
    fence_token: row.fence_token,
    operation_control_version: row.operation_control_version
  };
}

function lanesToChunkTypes(lanes) {
  const requested = new Set((Array.isArray(lanes) ? lanes : [lanes])
    .map(value => String(value || '').trim().toUpperCase())
    .filter(Boolean));
  const all = requested.size === 0 || requested.has('ALL');
  return {
    database: all || requested.has('DATABASE')
      ? INVOICE_DATABASE_CHUNK_TYPES
      : INVOICE_DATABASE_CHUNK_TYPES.filter(type => requested.has(type)),
    document: all || requested.has('DOCUMENT')
      ? INVOICE_DOCUMENT_CHUNK_TYPES
      : INVOICE_DOCUMENT_CHUNK_TYPES.filter(type => requested.has(type))
  };
}

async function sha256Hex(bytes) {
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, '0')).join('');
}

function postgresJsonbText(value) {
  if (Array.isArray(value)) return `[${value.map(postgresJsonbText).join(', ')}]`;
  if (value && typeof value === 'object') {
    const keyBytes = key => new TextEncoder().encode(key);
    const compareKeys = (left, right) => {
      const a = keyBytes(left);
      const b = keyBytes(right);
      if (a.byteLength !== b.byteLength) return a.byteLength - b.byteLength;
      const length = Math.min(a.byteLength, b.byteLength);
      for (let index = 0; index < length; index += 1) {
        if (a[index] !== b[index]) return a[index] - b[index];
      }
      return 0;
    };
    return `{${Object.keys(value).sort(compareKeys).map(key =>
      `${JSON.stringify(key)}: ${postgresJsonbText(value[key])}`).join(', ')}}`;
  }
  return JSON.stringify(value);
}

async function verifyFrozenPresentationModelHash(renderKind, model, expected = {}, options = {}) {
  const kind = String(renderKind || '').trim().toUpperCase();
  if (!kind) {
    throw Object.assign(new Error('RENDER_MODEL_KIND_MISSING'), { code: 'RENDER_MODEL_KIND_MISSING' });
  }
  if (!model || typeof model !== 'object' || Array.isArray(model)) {
    throw Object.assign(new Error('RENDER_MODEL_MISSING'), { code: 'RENDER_MODEL_MISSING' });
  }

  const expectedSchema = String(
    expected.presentation_model_schema_version
      || expected.expected_presentation_model_schema_version
      || options.presentationModelSchemaVersion
      || ''
  ).trim();

  let validated;
  let schemaVersion;

  /*
   * ATTACHMENT_INDEX is deliberately two-stage:
   *   1. context/frozen identity: small deterministic layout seed from SQL
   *   2. final render model: same seed plus displayed rows derived after page counts
   *
   * The DB context hash is over the layout seed, not the final displayed rows.
   * Therefore this helper verifies the frozen seed hash here; renderBrowserDocument
   * separately validates the final ATTACHMENT_INDEX render model after rows are derived.
   */
  if (kind === 'ATTACHMENT_INDEX' && !Array.isArray(model.display_rows)) {
    validated = model;
    schemaVersion = String(model.schema_version || expectedSchema || 'ATTACHMENT_INDEX_PRESENTATION_V1');
  } else {
    validated = validateFrozenPresentationModel(kind, model, {
      templateVersion: options.templateVersion || expected.template_version
    });
    schemaVersion = String(validated.schema_version || expectedSchema || '');
  }

  const calculatedPresentationHash = await sha256Hex(
    new TextEncoder().encode(postgresJsonbText(validated))
  );

  const expectedPresentationHash = String(
    expected.presentation_model_hash
      || expected.expected_presentation_model_hash
      || options.presentationModelHash
      || ''
  ).trim().toLowerCase();

  if (!/^[0-9a-f]{64}$/.test(expectedPresentationHash)) {
    throw Object.assign(new Error('RENDER_MODEL_HASH_MISSING'), {
      code: 'RENDER_MODEL_HASH_MISSING',
      detail: { render_kind: kind, schema_version: schemaVersion }
    });
  }

  if (calculatedPresentationHash !== expectedPresentationHash) {
    throw Object.assign(new Error('RENDER_MODEL_HASH_MISMATCH'), {
      code: 'RENDER_MODEL_HASH_MISMATCH',
      detail: {
        render_kind: kind,
        schema_version: schemaVersion,
        expected: expectedPresentationHash,
        actual: calculatedPresentationHash
      }
    });
  }

  if (expectedSchema && schemaVersion !== expectedSchema) {
    throw Object.assign(new Error('RENDER_MODEL_SCHEMA_MISMATCH'), {
      code: 'RENDER_MODEL_SCHEMA_MISMATCH',
      detail: { render_kind: kind, expected: expectedSchema, actual: schemaVersion }
    });
  }

  const expectedSnapshotHash = String(expected.snapshot_hash || options.snapshotHash || '').trim().toLowerCase();
  if (expectedSnapshotHash && !/^[0-9a-f]{64}$/.test(expectedSnapshotHash)) {
    throw Object.assign(new Error('FROZEN_SNAPSHOT_HASH_INVALID'), {
      code: 'FROZEN_SNAPSHOT_HASH_INVALID',
      detail: { render_kind: kind, value: expectedSnapshotHash }
    });
  }

  return Object.freeze({
    model: validated,
    render_kind: kind,
    presentation_model_schema_version: schemaVersion,
    presentation_model_hash: calculatedPresentationHash,
    expected_presentation_model_hash: expectedPresentationHash,
    snapshot_hash: expectedSnapshotHash || undefined
  });
}

async function pdfPageCount(bytes) {
  const document = await PDFDocument.load(bytes, { ignoreEncryption: false, updateMetadata: false });
  return document.getPageCount();
}

function processorIdentity(contextRow) {
  const expected = contextRow.expected_result_identity || {};
  return {
    chunk_id: expected.chunk_id || contextRow.chunk_id,
    fence_token: expected.fence_token,
    action: expected.action || contextRow.chunk_type,
    document_version_id: expected.document_version_id || contextRow.document_version_id || undefined,
    document_asset_id: expected.document_asset_id || contextRow.document_asset_id || undefined,
    plan_generation: expected.plan_generation,
    source_revision: expected.source_revision || undefined,
    template_version: expected.template_version || undefined,
    processor_policy_version: expected.processor_policy_version,
    render_kind: expected.render_kind || contextRow.context?.render_kind || undefined,
    ordered_input_hash: expected.ordered_input_hash || undefined,
    output_prefix: expected.immutable_destination_prefix
      || contextRow.context?.immutable_destination_prefix
  };
}

function compactError(error, retryable = true) {
  return {
    code: String(error?.code || error?.message || 'INVOICE_PROCESSOR_FAILED').slice(0, 120),
    message: String(error?.message || error || 'Invoice processor failed').slice(0, 500),
    retryable
  };
}

async function putImmutableInvoiceArtifact(bucket, key, bytes, metadata) {
  const expected = Object.fromEntries(Object.entries(metadata).map(([name, value]) => [name, String(value ?? '')]));
  const existing = await bucket.head(key);
  if (existing) {
    const actual = existing.customMetadata || {};
    if (Number(existing.size) === Number(bytes.byteLength)
      && Object.entries(expected).every(([name, value]) => actual[name] === value)) return existing;
    throw Object.assign(new Error('IMMUTABLE_ARTIFACT_KEY_CONFLICT'), { code: 'IMMUTABLE_ARTIFACT_KEY_CONFLICT' });
  }
  const written = await bucket.put(key, bytes, {
    onlyIf: { etagDoesNotMatch: '*' },
    sha256: expected.sha256,
    httpMetadata: { contentType: 'application/pdf' },
    customMetadata: expected
  });
  if (written) return written;
  const raced = await bucket.head(key);
  if (raced && Number(raced.size) === Number(bytes.byteLength)
    && Object.entries(expected).every(([name, value]) => raced.customMetadata?.[name] === value)) return raced;
  throw Object.assign(new Error('IMMUTABLE_ARTIFACT_KEY_CONFLICT'), { code: 'IMMUTABLE_ARTIFACT_KEY_CONFLICT' });
}

function deriveAttachmentDisplayMap(layout, finalIndexPageCount) {
  const stream = Array.isArray(layout.pagination_stream) ? layout.pagination_stream : [];
  const displayed = new Map();
  const physicalParts = new Set();
  let currentPage = 1;
  for (const part of stream) {
    const pages = Math.max(0, Number(part.page_count || part.pages || 0));
    if (!Number.isSafeInteger(pages)) throw Object.assign(new Error('ATTACHMENT_PAGINATION_PAGE_COUNT_INVALID'), { code: 'ATTACHMENT_PAGINATION_PAGE_COUNT_INVALID' });
    const kind = String(part.kind || part.section_type || '').toUpperCase();
    if (kind === 'CORE') currentPage += pages;
    else if (kind === 'ATTACHMENT_INDEX') currentPage += finalIndexPageCount;
    else if (kind === 'SEPARATOR') currentPage += pages;
    else {
      const rowId = String(part.display_row_id || part.logical_source_id || '');
      if (!rowId) throw Object.assign(new Error('ATTACHMENT_PAGINATION_LOGICAL_ROW_MISSING'), { code: 'ATTACHMENT_PAGINATION_LOGICAL_ROW_MISSING' });
      const physicalPartId = String(part.physical_part_id || part.artifact_id || `${rowId}:${part.physical_part_no || ''}`);
      if (physicalParts.has(physicalPartId)) throw Object.assign(new Error('ATTACHMENT_PAGINATION_PHYSICAL_PART_DUPLICATE'), { code: 'ATTACHMENT_PAGINATION_PHYSICAL_PART_DUPLICATE' });
      physicalParts.add(physicalPartId);
      if (rowId) {
        const row = displayed.get(rowId) || {
          row_id: rowId,
          attachment_number: part.attachment_number,
          worker: part.worker || part.source,
          week_or_date: part.week_or_date,
          document_type: part.document_type || part.input_type,
          evidence_description: part.evidence_description || part.description,
          reference: part.reference,
          start_page: currentPage,
          page_count: 0
        };
        row.page_count += pages;
        displayed.set(rowId, row);
      }
      currentPage += pages;
    }
  }
  const rows = [...displayed.values()];
  if (rows.some((row, index) => index > 0 && row.start_page < rows[index - 1].start_page)) {
    throw Object.assign(new Error('ATTACHMENT_PAGINATION_START_PAGE_ORDER_INVALID'), { code: 'ATTACHMENT_PAGINATION_START_PAGE_ORDER_INVALID' });
  }
  const expectedRows = Number(layout.expected_logical_attachment_count ?? layout.expected_displayed_row_count);
  if (Number.isSafeInteger(expectedRows) && expectedRows >= 0 && rows.length !== expectedRows) {
    throw Object.assign(new Error('ATTACHMENT_PAGINATION_LOGICAL_ROW_COUNT_MISMATCH'), { code: 'ATTACHMENT_PAGINATION_LOGICAL_ROW_COUNT_MISMATCH' });
  }
  const expectedPages = Number(layout.expected_physical_page_count);
  if (Number.isSafeInteger(expectedPages) && expectedPages >= 0 && currentPage - 1 !== expectedPages) {
    throw Object.assign(new Error('ATTACHMENT_PAGINATION_PHYSICAL_PAGE_COUNT_MISMATCH'), { code: 'ATTACHMENT_PAGINATION_PHYSICAL_PAGE_COUNT_MISMATCH' });
  }
  return rows;
}

async function resolveApprovedRenderAsset(env, identity, cache = new Map()) {
  if (!identity?.r2_key) return {};
  const expectedHash = String(identity.sha256 || '').toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(expectedHash)) {
    throw Object.assign(new Error('RENDER_ASSET_HASH_REQUIRED'), { code: 'RENDER_ASSET_HASH_REQUIRED' });
  }
  const cacheKey = `${String(identity.r2_key)}|${expectedHash}`;
  if (cache.has(cacheKey)) return cache.get(cacheKey);
  const object = await env.R2.get(String(identity.r2_key));
  if (!object) throw Object.assign(new Error('RENDER_ASSET_MISSING'), { code: 'RENDER_ASSET_MISSING' });
  if (identity.size_bytes != null && Number(identity.size_bytes) !== Number(object.size)) throw Object.assign(new Error('RENDER_ASSET_SIZE_MISMATCH'), { code: 'RENDER_ASSET_SIZE_MISMATCH' });
  const storedHash = String(object.customMetadata?.sha256 || '').toLowerCase();
  if (storedHash && storedHash !== expectedHash) throw Object.assign(new Error('RENDER_ASSET_HASH_MISMATCH'), { code: 'RENDER_ASSET_HASH_MISMATCH' });
  if (Number(object.size) > 2 * 1024 * 1024) throw Object.assign(new Error('RENDER_ASSET_TOO_LARGE'), { code: 'RENDER_ASSET_TOO_LARGE' });
  const mediaType = String(identity.media_type || object.httpMetadata?.contentType || 'image/png');
  if (!['image/png','image/jpeg'].includes(mediaType)) throw Object.assign(new Error('RENDER_ASSET_MEDIA_UNSUPPORTED'), { code: 'RENDER_ASSET_MEDIA_UNSUPPORTED' });
  const bytes = new Uint8Array(await object.arrayBuffer());
  const actualHash = await sha256Hex(bytes);
  if (actualHash !== expectedHash) throw Object.assign(new Error('RENDER_ASSET_HASH_MISMATCH'), { code: 'RENDER_ASSET_HASH_MISMATCH' });
  let binary = '';
  for (let offset = 0; offset < bytes.length; offset += 32768) binary += String.fromCharCode(...bytes.subarray(offset, offset + 32768));
  const resolved = Object.freeze({
    data_url: `data:${mediaType};base64,${btoa(binary)}`,
    size_bytes: bytes.byteLength,
    sha256: actualHash,
    media_type: mediaType
  });
  cache.set(cacheKey, resolved);
  return resolved;
}

async function resolveEmbeddedBrandingAssets(env, sourceModel, config) {
  const model = structuredClone(sourceModel || {});
  const cache = new Map();
  const resolveIdentity = async identity => {
    if (!identity?.r2_key) return identity;
    return { ...identity, ...(await resolveApprovedRenderAsset(env, identity, cache)) };
  };
  if (model.branding?.logo?.r2_key) {
    model.branding.logo = await resolveIdentity(model.branding.logo);
  }
  if (model.signatures?.candidate?.r2_key) {
    model.signatures.candidate = await resolveIdentity(model.signatures.candidate);
  }
  if (model.signatures?.authoriser?.r2_key) {
    model.signatures.authoriser = await resolveIdentity(model.signatures.authoriser);
  }
  const assets = [...cache.values()];
  if (assets.length > config.maximumEmbeddedRenderAssets) {
    throw Object.assign(new Error('RENDER_ASSET_COUNT_LIMIT_EXCEEDED'), { code: 'RENDER_ASSET_COUNT_LIMIT_EXCEEDED' });
  }
  const aggregateBytes = assets.reduce((sum, asset) => sum + Number(asset.size_bytes || 0), 0);
  if (aggregateBytes > config.maximumEmbeddedRenderAssetBytes) {
    throw Object.assign(new Error('RENDER_ASSET_AGGREGATE_SIZE_EXCEEDED'), { code: 'RENDER_ASSET_AGGREGATE_SIZE_EXCEEDED' });
  }
  return model;
}
async function renderBrowserDocument(env, contextRow, config, signal) {
  if (!env.BROWSER) throw Object.assign(new Error('INVOICE_BROWSER_BINDING_MISSING'), { code: 'INVOICE_BROWSER_BINDING_MISSING' });
  if (!env.R2) throw Object.assign(new Error('INVOICE_R2_BINDING_MISSING'), { code: 'INVOICE_R2_BINDING_MISSING' });

  const context = contextRow.context || {};
  const identity = processorIdentity(contextRow);
  const renderKind = String(identity.render_kind || context.render_kind || '').toUpperCase();
  const expectedIdentity = contextRow.expected_result_identity || {};
  const templateVersion = identity.template_version || context.template_version;

  const frozenPresentationModel = context.frozen_presentation_model;
  const expectedModelIdentity = {
    presentation_model_schema_version: context.presentation_model_schema_version || expectedIdentity.presentation_model_schema_version,
    presentation_model_hash: context.presentation_model_hash || expectedIdentity.presentation_model_hash,
    snapshot_hash: context.snapshot_hash || expectedIdentity.snapshot_hash,
    template_version: templateVersion
  };

  const verifiedModel = await verifyFrozenPresentationModelHash(
    renderKind,
    frozenPresentationModel,
    expectedModelIdentity,
    { templateVersion }
  );

  const layout = context.attachment_index_layout || {};
  let model;

  if (renderKind === 'ATTACHMENT_INDEX') {
    const baseModel = verifiedModel.model && typeof verifiedModel.model === 'object'
      ? structuredClone(verifiedModel.model)
      : {};
    model = {
      ...baseModel,
      schema_version: baseModel.schema_version || expectedModelIdentity.presentation_model_schema_version || 'ATTACHMENT_INDEX_PRESENTATION_V1',
      display_rows: deriveAttachmentDisplayMap(
        layout,
        Number(layout.expected_index_page_count || 1)
      )
    };
    validateFrozenPresentationModel('ATTACHMENT_INDEX', model, { templateVersion });
  } else {
    const embeddedModel = await resolveEmbeddedBrandingAssets(env, verifiedModel.model, config);
    validateFrozenPresentationModel(renderKind, embeddedModel, { templateVersion });
    model = embeddedModel;
  }

  const html = renderKind === 'INVOICE_CORE'
    ? buildProfessionalInvoiceHtml(model)
    : buildInvoiceSourceDocumentHtml(renderKind, model);

  const browserTimeout = AbortSignal.timeout(config.browserRenderTimeoutMs);
  const combinedSignal = signal ? AbortSignal.any([signal, browserTimeout]) : browserTimeout;
  const browser = await puppeteer.launch(env.BROWSER);
  let page = null;

  const abort = () => {
    void page?.close().catch(() => undefined);
    void browser.close().catch(() => undefined);
  };
  combinedSignal.addEventListener('abort', abort, { once: true });

  try {
    if (combinedSignal.aborted) {
      throw Object.assign(
        new Error(signal?.aborted ? 'OWNERSHIP_LOST' : 'BROWSER_RENDER_TIMEOUT'),
        { code: signal?.aborted ? 'OWNERSHIP_LOST' : 'BROWSER_RENDER_TIMEOUT' }
      );
    }

    page = await browser.newPage();
    await page.setRequestInterception(true);
    page.on('request', request => {
      const url = request.url();
      if (url.startsWith('data:') || url.startsWith('about:')) void request.continue();
      else void request.abort('blockedbyclient');
    });

    await page.setContent(html, {
      waitUntil: 'domcontentloaded',
      timeout: config.browserRenderTimeoutMs
    });
    await page.emulateMediaType('print');

    const buffer = await page.pdf({
      format: 'A4',
      printBackground: true,
      preferCSSPageSize: true,
      displayHeaderFooter: true,
      headerTemplate: '<span></span>',
      footerTemplate: '<div style="font-size:8px;width:100%;text-align:center;color:#667085"><span class="pageNumber"></span> / <span class="totalPages"></span></div>',
      margin: { top: '12mm', right: '12mm', bottom: '16mm', left: '12mm' }
    });

    const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);

    if (config.browserRenderOutputMaxBytes > config.browserInMemoryPdfMaxBytes) {
      throw Object.assign(new Error('INVOICE_BROWSER_MEMORY_LIMIT_INVALID'), { code: 'INVOICE_BROWSER_MEMORY_LIMIT_INVALID' });
    }
    if (bytes.byteLength > config.browserRenderOutputMaxBytes) {
      throw Object.assign(new Error('BROWSER_RENDER_OUTPUT_TOO_LARGE'), { code: 'BROWSER_RENDER_OUTPUT_TOO_LARGE' });
    }

    const sha256 = await sha256Hex(bytes);
    const pageCount = await pdfPageCount(bytes);

    if (pageCount > config.browserRenderOutputMaxPages) {
      throw Object.assign(new Error('BROWSER_RENDER_PAGE_LIMIT_EXCEEDED'), { code: 'BROWSER_RENDER_PAGE_LIMIT_EXCEEDED' });
    }

    const outputPrefix = String(identity.output_prefix || '');
    if (!outputPrefix) {
      throw Object.assign(new Error('INVOICE_OUTPUT_PREFIX_MISSING'), { code: 'INVOICE_OUTPUT_PREFIX_MISSING' });
    }

    const r2Key = `${outputPrefix}${renderKind.toLowerCase()}-${sha256}.pdf`;
    const metadata = {
      sha256,
      size_bytes: bytes.byteLength,
      chunk_id: identity.chunk_id,
      fence_token: identity.fence_token,
      plan_generation: identity.plan_generation,
      document_version_id: identity.document_version_id,
      render_kind: renderKind,
      processor_policy_version: identity.processor_policy_version,
      template_version: identity.template_version,
      presentation_model_schema_version: verifiedModel.presentation_model_schema_version,
      presentation_model_hash: verifiedModel.presentation_model_hash,
      snapshot_hash: verifiedModel.snapshot_hash || ''
    };

    await putImmutableInvoiceArtifact(env.R2, r2Key, bytes, metadata);

    const result = {
      ...identity,
      output_prefix: outputPrefix,
      output_type: 'application/pdf',
      r2_key: r2Key,
      sha256,
      size_bytes: bytes.byteLength,
      page_count: pageCount,
      parse_verified: true,
      processor_version: 'cloudtms-browser-renderer-v4',
      presentation_model_schema_version: verifiedModel.presentation_model_schema_version,
      presentation_model_hash: verifiedModel.presentation_model_hash,
      snapshot_hash: verifiedModel.snapshot_hash
    };

    if (renderKind === 'ATTACHMENT_INDEX') {
      const rows = deriveAttachmentDisplayMap(layout, pageCount);
      const displayedPageMapHash = await sha256Hex(new TextEncoder().encode(postgresJsonbText(rows)));
      const paginationStream = Array.isArray(layout.pagination_stream) ? layout.pagination_stream : [];
      const paginationStreamHash = await sha256Hex(new TextEncoder().encode(postgresJsonbText(paginationStream)));

      result.layout_phase = layout.layout_phase;
      result.layout_pass = layout.layout_pass;
      result.layout_page_count = pageCount;
      result.final_index_page_count = pageCount;
      result.displayed_page_map = rows;
      result.displayed_row_ids = rows.map(row => row.row_id);
      result.displayed_start_pages = rows.map(row => row.start_page);
      result.displayed_page_counts = rows.map(row => row.page_count);
      result.displayed_row_count = rows.length;
      result.displayed_start_pages_hash = displayedPageMapHash;
      result.displayed_page_map_hash = displayedPageMapHash;
      result.pagination_stream_hash = paginationStreamHash;
      result.layout_identity_hash = await sha256Hex(
        new TextEncoder().encode(postgresJsonbText(layout.determinism || {}))
      );
      result.displayed_rows_verified = true;
    }

    return result;
  } finally {
    combinedSignal.removeEventListener('abort', abort);
    await page?.close().catch(() => undefined);
    await browser.close().catch(() => undefined);
  }
}

async function runNativeProcessor(env, contextRow, config, signal) {
  if (!env.INVOICE_DOCUMENT_PROCESSOR) {
    throw Object.assign(new Error('INVOICE_DOCUMENT_PROCESSOR_BINDING_MISSING'), { code: 'INVOICE_DOCUMENT_PROCESSOR_BINDING_MISSING', category: 'TRANSIENT_INFRASTRUCTURE' });
  }
  const identity = processorIdentity(contextRow);
  const body = JSON.stringify({
    expected_result_identity: contextRow.expected_result_identity,
    context: contextRow.context
  });
  const fields = {
    method: 'POST',
    path: '/process',
    timestamp: Date.now(),
    nonce: crypto.randomUUID(),
    chunk_id: identity.chunk_id,
    fence_token: identity.fence_token,
    action: identity.action,
    plan_generation: identity.plan_generation,
    processor_policy_version: identity.processor_policy_version,
    body_sha256: await invoiceProcessorSha256Hex(body)
  };
  const signature = await signInvoiceProcessorRequest(env.INVOICE_DOCUMENT_PROCESSOR_SECRET, fields);
  const timeout = AbortSignal.timeout(config.nativeRequestTimeoutMs);
  const combinedSignal = signal ? AbortSignal.any([signal, timeout]) : timeout;
  const response = await env.INVOICE_DOCUMENT_PROCESSOR.fetch(
    'https://invoice-document-processor.internal/process',
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        ...invoiceProcessorIdentityHeaders(fields, signature)
      },
      body,
      signal: combinedSignal
    }
  );
  const result = await response.json();
  if (!response.ok || result?.ok === false) {
    const error = new Error(result?.message || result?.code || `PROCESSOR_HTTP_${response.status}`);
    error.code = result?.code || `PROCESSOR_HTTP_${response.status}`;
    error.category = result?.category || (response.status >= 500 ? 'TRANSIENT_INFRASTRUCTURE' : 'PROCESSOR_BUG');
    error.retryable = result?.retryable === true;
    throw error;
  }
  return result.result || result;
}

export async function checkInvoiceDocumentProcessorReady(env, options = {}) {
  if (!env.INVOICE_DOCUMENT_PROCESSOR) return { ok: false, code: 'INVOICE_DOCUMENT_PROCESSOR_BINDING_MISSING' };
  if (!env.INVOICE_DOCUMENT_PROCESSOR_SECRET) return { ok: false, code: 'INVOICE_DOCUMENT_PROCESSOR_SECRET_MISSING' };
  const body = '{}';
  const fields = {
    method: 'POST',
    path: '/ready',
    timestamp: Date.now(),
    nonce: crypto.randomUUID(),
    chunk_id: '',
    fence_token: '',
    action: 'READY',
    plan_generation: '',
    processor_policy_version: PROCESSOR_POLICY_VERSION,
    body_sha256: await invoiceProcessorSha256Hex(body)
  };
  const signature = await signInvoiceProcessorRequest(env.INVOICE_DOCUMENT_PROCESSOR_SECRET, fields);
  const config = options.config || getInvoiceQueueRuntimeConfig(env);
  const timeoutMs = parseBoundedInteger(options.timeoutMs, 5000, 1000, 10000);
  try {
    const response = await env.INVOICE_DOCUMENT_PROCESSOR.fetch(
      'https://invoice-document-processor.internal/ready',
      {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          ...invoiceProcessorIdentityHeaders(fields, signature)
        },
        body,
        signal: AbortSignal.timeout(timeoutMs)
      }
    );
    const text = await response.text();
    if (new TextEncoder().encode(text).byteLength > 16384) {
      return { ok: false, code: 'INVOICE_PROCESSOR_READY_RESPONSE_TOO_LARGE' };
    }
    let result = null;
    try { result = text ? JSON.parse(text) : null; } catch {}
    if (!response.ok || result?.ok !== true) return { ok: false, code: result?.code || `INVOICE_PROCESSOR_READY_HTTP_${response.status}` };
    if (result.processor_policy_version !== PROCESSOR_POLICY_VERSION) return { ok: false, code: 'INVOICE_PROCESSOR_READY_POLICY_MISMATCH' };
    const media = Array.isArray(result.supported_media_types)
      ? [...result.supported_media_types].sort()
      : [];
    if (JSON.stringify(media) !== JSON.stringify(['application/pdf', 'image/jpeg', 'image/png'].sort())) {
      return { ok: false, code: 'INVOICE_PROCESSOR_MEDIA_CONTRACT_MISMATCH' };
    }
    const receipts = result.receipt_contracts || {};
    if (
      receipts.object !== 'ACTUAL_BYTES_OBJECT_RECEIPT_V3'
      || receipts.logical !== 'LOGICAL_SOURCE_RECEIPT_V3'
      || receipts.merge !== 'ACTUAL_BYTES_MERGE_RECEIPT_V3'
      || receipts.root !== 'DOCUMENT_ROOT_RECEIPT_V3'
      || receipts.ordered_input !== 'ACTUAL_ORDERED_INPUT_V1'
    ) return { ok: false, code: 'INVOICE_PROCESSOR_RECEIPT_CONTRACT_MISMATCH' };
    const implementation = String(result.processor_implementation_version || '');
    if (!implementation || implementation !== config.expectedProcessorImplementationVersion) {
      return { ok: false, code: 'INVOICE_PROCESSOR_IMPLEMENTATION_MISMATCH' };
    }
    if (!result.container_ready) return { ok: false, code: 'INVOICE_PROCESSOR_NATIVE_NOT_READY' };
    if (config.nativeToolReadinessRequired && !result.native_tools_ready) {
      return { ok: false, code: 'INVOICE_PROCESSOR_NATIVE_TOOLS_NOT_READY' };
    }
    return {
      ok: true,
      processor_policy_version: result.processor_policy_version,
      processor_implementation_version: implementation.slice(0, 120)
    };
  } catch {
    return { ok: false, code: 'INVOICE_DOCUMENT_PROCESSOR_NOT_READY' };
  }
}

export async function runInvoiceDocumentProcessor(env, contextRow, options = {}) {
  const signal = options.signal;
  const config = options.config || getInvoiceQueueRuntimeConfig(env);
  if (!config.processorEnabled) throw new Error('INVOICE_DOCUMENT_PROCESSOR_DISABLED');
  const action = String(contextRow.chunk_type || contextRow.expected_result_identity?.action || '').toUpperCase();
  const identity = processorIdentity(contextRow);
  if (!UUID_PATTERN.test(String(identity.chunk_id || ''))) {
    throw new Error('INVOICE_PROCESSOR_CHUNK_ID_INVALID');
  }
  if (!Number.isSafeInteger(Number(identity.fence_token)) || Number(identity.fence_token) < 0) {
    throw new Error('INVOICE_PROCESSOR_FENCE_INVALID');
  }
  if (identity.processor_policy_version !== PROCESSOR_POLICY_VERSION) {
    throw new Error('INVOICE_PROCESSOR_POLICY_UNSUPPORTED');
  }
  if (!BROWSER_ACTIONS.has(action) && !NATIVE_ACTIONS.has(action)) {
    throw new Error('INVOICE_PROCESSOR_ACTION_UNSUPPORTED');
  }
  const outputPrefix = String(identity.output_prefix || '');
  if (
    action !== 'DOCUMENT_VERIFY' && (
      !outputPrefix || outputPrefix.includes('..')
      || !/^(invoice-documents|invoice-assets)\//.test(outputPrefix)
    )
  ) {
    throw new Error('INVOICE_PROCESSOR_OUTPUT_PREFIX_INVALID');
  }
  if (BROWSER_ACTIONS.has(action)) return renderBrowserDocument(env, contextRow, config, signal);
  return runNativeProcessor(env, contextRow, config, signal);
}

async function mapWithConcurrency(items, concurrency, mapper) {
  const results = new Array(items.length);
  let next = 0;
  async function worker() {
    while (next < items.length) {
      const index = next;
      next += 1;
      results[index] = await mapper(items[index], index);
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()));
  return results;
}

function flattenReleasedValues(value, found = new Set()) {
  if (Array.isArray(value)) {
    for (const item of value) flattenReleasedValues(item, found);
    return found;
  }
  if (!value || typeof value !== 'object') return found;
  for (const [key, child] of Object.entries(value)) {
    const name = String(key).toLowerCase();
    if (['chunk_type','new_chunk_type','released_chunk_type'].includes(name)) found.add(String(child || '').toUpperCase());
    if (['released_chunk_types','chunk_types'].includes(name) && Array.isArray(child)) {
      for (const type of child) found.add(String(type || '').toUpperCase());
    }
    if (['operation_type','child_operation_type'].includes(name)) {
      const type = String(child || '').toUpperCase();
      if (type === 'BUILD_DOCUMENT') for (const lane of ['DOCUMENT_PLAN', ...INVOICE_DOCUMENT_CHUNK_TYPES]) found.add(lane);
      if (type.includes('RECONCILE')) found.add('RECONCILE');
      else if (['GENERATE','ISSUE','DELIVERY'].some(value => type.includes(value))) found.add('DATABASE');
    }
    if (child && typeof child === 'object') flattenReleasedValues(child, found);
  }
  return found;
}

export function deriveReleasedInvoiceLanes(results) {
  const found = new Set();
  for (const row of rowsFromRpc(results)) {
    if (row?.accepted === false || String(row?.status || '').toUpperCase() === 'REJECTED') continue;
    flattenReleasedValues(row, found);
    const phase = String(row?.new_phase || row?.phase || '').toUpperCase();
    if (phase.includes('ASSET')) for (const lane of ['ASSET_INSPECT','ASSET_NORMALISE']) found.add(lane);
    if (phase.includes('RENDER')) for (const lane of ['SOURCE_RENDER','INVOICE_CORE_RENDER']) found.add(lane);
    if (phase.includes('ASSEMBL') || phase.includes('MERGE')) found.add('PDF_MERGE');
    if (phase.includes('VERIFY')) found.add('DOCUMENT_VERIFY');
    if (phase.includes('DELIVER') || phase.includes('ISSUE') || phase.includes('PLAN') || phase.includes('VALIDAT')) found.add('DATABASE');
  }
  return [...found].filter(type => type === 'DATABASE' || [...INVOICE_DATABASE_CHUNK_TYPES, ...INVOICE_DOCUMENT_CHUNK_TYPES].includes(type)).sort();
}
export async function processInvoiceDatabaseChunksBatch(env, claims, options) {
  if (!claims.length) return { claimed: 0, advanced: 0, rejected: 0, released_lanes: [], results: [] };
  const response = await options.rpc('invoice_operation_advance_batch', {
    p_claims: claims.map(claimIdentity),
    p_now_utc: new Date().toISOString()
  });
  const results = valueFromRpc(response);
  const accepted = results.filter(row => row?.accepted !== false && String(row?.status || '').toUpperCase() !== 'REJECTED');
  return {
    claimed: claims.length,
    advanced: accepted.length,
    rejected: results.length - accepted.length,
    released_lanes: deriveReleasedInvoiceLanes(accepted),
    results
  };
}

function actionConcurrency(config, chunkType) {
  switch (String(chunkType || '').toUpperCase()) {
    case 'ASSET_INSPECT': case 'ASSET_NORMALISE': return config.assetConcurrency;
    case 'SOURCE_RENDER': return config.sourceRenderConcurrency;
    case 'INVOICE_CORE_RENDER': return config.coreRenderConcurrency;
    case 'PDF_MERGE': return config.mergeConcurrency;
    case 'DOCUMENT_VERIFY': return config.verifyConcurrency;
    default: return 1;
  }
}

function createActiveDocumentJob(context, claim) {
  const now = Date.now();
  return {
    context,
    claim,
    abortController: new AbortController(),
    latestLeaseExpiry:
      context?.lease_expires_at_utc
      || claim?.lease_expires_at_utc
      || null,
    lastSuccessfulTouchAt: now,
    heartbeatFailureCount: 0,
    ownershipUncertain: false,
    ownershipState: 'OWNED',
    abortReason: null,
    completionAllowed: true,
    processorActive: false,
    processorFinished: false,
    processorPromise: null
  };
}

function touchAccepted(row) {
  return row?.accepted === true && !['REJECTED','STALE','OWNERSHIP_LOST'].includes(String(row?.status || '').toUpperCase());
}

function abortInvoiceDocumentJob(job, reason = 'OWNERSHIP_LOST') {
  if (!job || job.ownershipState === 'OWNERSHIP_LOST') return false;
  job.ownershipState = 'OWNERSHIP_LOST';
  job.abortReason = reason;
  job.completionAllowed = false;
  if (!job.abortController.signal.aborted) job.abortController.abort(reason);
  return true;
}

function applyInvoiceHeartbeatResults(jobs, rows) {
  const byId = new Map(rows.map(row => [row?.chunk_id, row]));
  const now = Date.now();
  let ownershipLost = 0;
  for (const job of jobs) {
    const row = byId.get(job.claim?.chunk_id);
    if (!touchAccepted(row)) {
      if (abortInvoiceDocumentJob(job, 'OWNERSHIP_LOST')) ownershipLost += 1;
      continue;
    }
    job.lastSuccessfulTouchAt = now;
    job.heartbeatFailureCount = 0;
    job.ownershipUncertain = false;
    if (row.lease_expires_at_utc) job.latestLeaseExpiry = row.lease_expires_at_utc;
  }
  return ownershipLost;
}

function markInvoiceHeartbeatFailure(jobs, config, now = Date.now()) {
  let aborted = 0;
  for (const job of jobs) {
    if (job.ownershipState !== 'OWNED') continue;
    job.heartbeatFailureCount += 1;
    job.ownershipUncertain = true;
    const expiryMs = Date.parse(job.latestLeaseExpiry || '');
    const tooCloseToExpiry = Number.isFinite(expiryMs)
      && now >= expiryMs - config.heartbeatFailureAbortMarginMs;
    const failureLimitReached =
      job.heartbeatFailureCount >= config.maximumConsecutiveHeartbeatFailures;
    if (tooCloseToExpiry || failureLimitReached) {
      if (abortInvoiceDocumentJob(job, 'OWNERSHIP_UNCERTAIN')) aborted += 1;
    }
  }
  return aborted;
}

async function heartbeatActiveInvoiceJobs(rpc, jobs, timeoutMs) {
  const raw = await rpc('invoice_work_touch_batch', {
    p_touches: jobs.map(job => ({
      ...claimIdentity(job.claim),
      progress: { status_message: 'Processing document' }
    })),
    p_now_utc: new Date().toISOString()
  }, {
    routeClass: 'INVOICE_QUEUE_HEARTBEAT',
    purpose: 'INVOICE_LEASE_TOUCH',
    timeoutMs
  });
  return valueFromRpc(raw);
}

async function performFinalInvoiceOwnershipCheck(rpc, jobs, config) {
  if (!config.finalOwnershipCheckRequired) return { ownership_lost: 0, checked: 0 };
  const cutoff = Date.now() - config.heartbeatMs;
  const required = jobs.filter(job =>
    job.completionAllowed
    && job.ownershipState === 'OWNED'
    && (job.ownershipUncertain || job.lastSuccessfulTouchAt <= cutoff)
  );
  if (!required.length) return { ownership_lost: 0, checked: 0 };
  try {
    const rows = await heartbeatActiveInvoiceJobs(rpc, required, config.finalTouchRpcTimeoutMs);
    return {
      ownership_lost: applyInvoiceHeartbeatResults(required, rows),
      checked: required.length
    };
  } catch {
    for (const job of required) abortInvoiceDocumentJob(job, 'OWNERSHIP_UNCERTAIN');
    return { ownership_lost: required.length, checked: required.length };
  }
}

export async function processInvoiceDocumentChunksBatch(env, claims, options) {
  if (!claims.length) {
    return {
      claimed: 0,
      processed: 0,
      rejected: 0,
      completed: 0,
      processor_succeeded: 0,
      db_completion_accepted: 0,
      db_completion_retry: 0,
      db_completion_permanent_failed: 0,
      db_completion_rejected: 0,
      context_failed: 0,
      ownership_lost: 0,
      released_lanes: []
    };
  }
  const rpc = options.rpc;
  const config = options.config || getInvoiceQueueRuntimeConfig(env);
  const contextResponse = await rpc('invoice_work_context_batch', {
    p_claims: claims.map(claimIdentity), p_now_utc: new Date().toISOString()
  });
  const contexts = valueFromRpc(contextResponse);
  const valid = contexts.filter(row => row?.accepted === true && row?.status === 'OK');
  const contextErrors = contexts.filter(row => row?.accepted === true && row?.status === 'CONTEXT_ERROR');
  const rejected = contexts.filter(row => row?.accepted !== true);
  const claimById = new Map(claims.map(claim => [claim.chunk_id, claim]));
  const activeJobs = new Map(
    [...valid, ...contextErrors].map(context => [
      context.chunk_id,
      createActiveDocumentJob(context, claimById.get(context.chunk_id))
    ])
  );
  let heartbeatStopped = false;
  let heartbeatPromise = Promise.resolve();
  let heartbeatTimer = null;
  let nextHeartbeatDelayMs = config.heartbeatMs;

  const heartbeat = async () => {
    const jobs = [...activeJobs.values()].filter(job =>
      job.ownershipState === 'OWNED' && job.completionAllowed
    );
    if (!jobs.length) return;
    try {
      const rows = await heartbeatActiveInvoiceJobs(rpc, jobs, config.heartbeatRpcTimeoutMs);
      applyInvoiceHeartbeatResults(jobs, rows);
      nextHeartbeatDelayMs = config.heartbeatMs;
    } catch (error) {
      markInvoiceHeartbeatFailure(jobs, config);
      nextHeartbeatDelayMs = Math.min(5000, config.heartbeatMs);
      console.warn(JSON.stringify({ event: 'invoice_document_heartbeat_failed', code: String(error?.code || error?.message || 'HEARTBEAT_FAILED').slice(0, 120), active_count: jobs.length }));
    }
  };
  const scheduleHeartbeat = () => {
    if (heartbeatStopped) return;
    heartbeatTimer = setTimeout(() => {
      heartbeatPromise = heartbeatPromise.then(heartbeat).finally(scheduleHeartbeat);
    }, nextHeartbeatDelayMs);
  };
  if (activeJobs.size) scheduleHeartbeat();

  const groups = new Map();
  for (const context of valid) {
    const type = String(context.chunk_type || 'UNKNOWN');
    groups.set(type, [...(groups.get(type) || []), context]);
  }
  const processorResults = [];
  try {
    for (const [type, rows] of groups) {
      const results = await mapWithConcurrency(rows, actionConcurrency(config, type), async contextRow => {
        const job = activeJobs.get(contextRow.chunk_id);
        if (!job || job.ownershipState !== 'OWNED') return { ownership_lost: true, chunk_id: contextRow.chunk_id };
        try {
          job.processorActive = true;
          job.processorPromise = runInvoiceDocumentProcessor(env, contextRow, { config, signal: job.abortController.signal });
          const result = await job.processorPromise;
          if (job.ownershipState !== 'OWNED') return { ownership_lost: true, chunk_id: contextRow.chunk_id };
          return { ...claimIdentity(job.claim), chunk_id: contextRow.chunk_id, outcome: 'SUCCESS', result };
        } catch (error) {
          if (job.ownershipState !== 'OWNED' || job.abortController.signal.aborted) {
            return { ownership_lost: true, chunk_id: contextRow.chunk_id };
          }
          const code = String(error?.code || error?.message || '').toUpperCase();
          const permanent = [
            'ASSET_MEDIA_TYPE_UNSUPPORTED','ASSET_EMPTY','ASSET_TRUNCATED','ASSET_CORRUPT',
            'ASSET_PDF_ENCRYPTED','ASSET_SOURCE_IDENTITY_CHANGED','IMMUTABLE_ARTIFACT_KEY_CONFLICT',
            'ATTACHMENT_INDEX_LAYOUT_UNSTABLE','POLICY_VIOLATION','IDENTITY_MISMATCH'
          ].some(value => code.includes(value));
          return { ...claimIdentity(job.claim), chunk_id: contextRow.chunk_id, outcome: permanent ? 'FAILED' : 'RETRY', error: compactError(error, !permanent) };
        } finally {
          job.processorActive = false;
          job.processorFinished = true;
        }
      });
      processorResults.push(...results);
    }
  } finally {
    heartbeatStopped = true;
    if (heartbeatTimer) clearTimeout(heartbeatTimer);
    await heartbeatPromise;
  }

  await performFinalInvoiceOwnershipCheck(rpc, [...activeJobs.values()], config);
  const ownershipLostIds = new Set([
    ...processorResults.filter(row => row?.ownership_lost === true).map(row => row.chunk_id),
    ...[...activeJobs.values()]
      .filter(job => !job.completionAllowed || job.ownershipState !== 'OWNED')
      .map(job => job.claim?.chunk_id)
  ].filter(Boolean));
  const terminalProcessorResults = processorResults.filter(row =>
    row?.ownership_lost !== true
    && !ownershipLostIds.has(row.chunk_id)
    && activeJobs.get(row.chunk_id)?.completionAllowed !== false
  );
  const contextFailureResults = contextErrors
    .filter(row => !ownershipLostIds.has(row.chunk_id))
    .map(row => ({
      ...claimIdentity(claimById.get(row.chunk_id)),
      outcome: row.retryable ? 'RETRY' : 'FAILED',
      error: { code: row.code || 'INVOICE_DOCUMENT_CONTEXT_ERROR', retryable: row.retryable === true, context_size_bytes: row.context_size_bytes }
    }));
  const completionPayload = [...terminalProcessorResults, ...contextFailureResults];
  let completion = [];
  if (completionPayload.length) {
    completion = valueFromRpc(await rpc('invoice_work_complete_batch', {
      p_results: completionPayload, p_now_utc: new Date().toISOString()
    }));
  }
  const completionAccepted = completion.filter(row =>
    row?.accepted === true
    && !['REJECTED','STALE','OWNERSHIP_LOST'].includes(String(row?.status || '').toUpperCase())
  );
  const completionRetry = completionAccepted.filter(row =>
    ['QUEUED','WAITING','RETRY_WAIT'].includes(String(row?.status || '').toUpperCase())
  );
  const completionPermanent = completionAccepted.filter(row =>
    ['FAILED','DEAD_LETTER'].includes(String(row?.status || '').toUpperCase())
  );
  const completionComplete = completionAccepted.filter(row =>
    String(row?.status || '').toUpperCase() === 'COMPLETE'
  );
  return {
    claimed: claims.length, valid_contexts: valid.length, rejected: rejected.length,
    processed: completionPayload.length,
    processor_succeeded: terminalProcessorResults.filter(row => row.outcome === 'SUCCESS').length,
    completed: completionComplete.length,
    db_completion_accepted: completionAccepted.length,
    db_completion_retry: completionRetry.length,
    db_completion_permanent_failed: completionPermanent.length,
    db_completion_rejected: completion.length - completionAccepted.length,
    context_failed: contextFailureResults.length,
    ownership_lost: ownershipLostIds.size,
    released_lanes: deriveReleasedInvoiceLanes(completionAccepted),
    context_rejections: rejected.map(row => ({ chunk_id: row.chunk_id, code: row.code })),
    completion
  };
}

async function claimBatch(rpc, chunkTypes, workerId, limit, leaseSeconds) {
  if (!chunkTypes.length || limit < 1) return [];
  const response = await rpc('invoice_work_claim_batch', {
    p_chunk_types: chunkTypes,
    p_worker_id: workerId,
    p_limit: limit,
    p_lease_seconds: leaseSeconds,
    p_now_utc: new Date().toISOString()
  });
  return rowsFromRpc(response);
}

function normaliseInvoiceLanes(lanes) {
  const allowed = new Set(['ALL','DATABASE','DOCUMENT', ...INVOICE_DATABASE_CHUNK_TYPES, ...INVOICE_DOCUMENT_CHUNK_TYPES]);
  const values = [...new Set((Array.isArray(lanes) ? lanes : [lanes]).map(value => String(value || '').trim().toUpperCase()).filter(Boolean))];
  if (!values.length) return ['ALL'];
  const invalid = values.filter(value => !allowed.has(value));
  if (invalid.length) throw Object.assign(new Error('INVOICE_DRAIN_LANE_INVALID'), { code: 'INVOICE_DRAIN_LANE_INVALID' });
  return values.includes('ALL') ? ['ALL'] : values.sort();
}

export function deriveInvoiceOperationLanes(operationSummaries, additionalLanes = []) {
  const lanes = new Set(normaliseInvoiceLanes(additionalLanes.length ? additionalLanes : []));
  lanes.delete('ALL');
  for (const row of rowsFromRpc(operationSummaries)) {
    if (row?.accepted === false) continue;
    const type = String(row?.operation_type || row?.command_type || row?.type || '').toUpperCase();
    const phase = String(row?.phase || row?.current_phase || '').toUpperCase();
    if (type.includes('RECONCILE')) lanes.add('RECONCILE');
    else lanes.add('DATABASE');
    if (type.includes('DOCUMENT') || type.includes('VIEW')) {
      for (const lane of ['DOCUMENT_PLAN', ...INVOICE_DOCUMENT_CHUNK_TYPES]) lanes.add(lane);
    }
    if (type.includes('ASSET')) for (const lane of ['ASSET_INSPECT','ASSET_NORMALISE','DOCUMENT_PLAN']) lanes.add(lane);
    if (type.includes('GENERAT') || type.includes('ISSUE')) {
      for (const lane of ['DOCUMENT_PLAN', ...INVOICE_DOCUMENT_CHUNK_TYPES]) lanes.add(lane);
    }
    if (phase.includes('ASSET')) for (const lane of ['ASSET_INSPECT','ASSET_NORMALISE']) lanes.add(lane);
    if (phase.includes('RENDER')) for (const lane of ['SOURCE_RENDER','INVOICE_CORE_RENDER']) lanes.add(lane);
    if (phase.includes('MERGE') || phase.includes('ASSEMBL')) lanes.add('PDF_MERGE');
    if (phase.includes('VERIFY')) lanes.add('DOCUMENT_VERIFY');
    for (const released of deriveReleasedInvoiceLanes([row])) lanes.add(released);
  }
  return [...lanes].sort();
}

function normaliseInvoicePriorityClass(value) {
  const priority = String(value || 'INTERACTIVE').trim().toUpperCase();
  return ['VIEW_NOW','INTERACTIVE','AUTOMATIC','SCHEDULED','RECONCILE'].includes(priority) ? priority : 'INTERACTIVE';
}

function selectInvoiceDrainMode(options) {
  const mode = String(options.mode || '').toLowerCase();
  if (['user_nudge','scheduled','internal'].includes(mode)) return mode;
  if (Number(options.continuationDepth || 0) > 0) return 'internal';
  return options.priorityClass === 'SCHEDULED' ? 'scheduled' : 'user_nudge';
}

export async function requestFreshInvoiceContinuation(env, options = {}) {
  const config = options.config || getInvoiceQueueRuntimeConfig(env);
  const depth = Number(options.continuationDepth || 0) + 1;
  if (!config.continuationEnabled || depth > config.maximumContinuationDepth) {
    return { dispatched: false, code: 'INVOICE_CONTINUATION_LIMIT_REACHED' };
  }
  if (!env.INVOICE_QUEUE_DISPATCHER) return { dispatched: false, code: 'INVOICE_QUEUE_DISPATCHER_BINDING_MISSING' };
  const payload = {
    timestamp: Date.now(), nonce: crypto.randomUUID(), depth,
    lanes: normaliseInvoiceLanes(options.lanes || ['ALL']),
    priority_class: normaliseInvoicePriorityClass(options.priorityClass || 'SCHEDULED')
  };
  if (options.reconciliationCursor != null) {
    if (!payload.lanes.includes('RECONCILE')) return { dispatched: false, code: 'INVOICE_RECONCILIATION_CURSOR_LANE_INVALID' };
    const cursor = options.reconciliationCursor;
    if (
      !cursor
      || !Number.isFinite(Date.parse(cursor.snapshot_at_utc || ''))
      || !Number.isFinite(Date.parse(cursor.updated_at_utc || ''))
      || !UUID_PATTERN.test(String(cursor.operation_id || ''))
    ) return { dispatched: false, code: 'INVOICE_RECONCILIATION_CURSOR_INVALID' };
    payload.reconciliation_cursor = {
      snapshot_at_utc: new Date(cursor.snapshot_at_utc).toISOString(),
      updated_at_utc: new Date(cursor.updated_at_utc).toISOString(),
      operation_id: String(cursor.operation_id).toLowerCase()
    };
  }
  const signature = await signInvoiceDrainRequest(env.INVOICE_QUEUE_DISPATCH_SECRET, payload);
  try {
    const response = await env.INVOICE_QUEUE_DISPATCHER.fetch('https://invoice-queue-dispatcher.internal/dispatch', {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-cloudtms-internal-service': 'invoice-queue-main-v1' },
      body: JSON.stringify({ ...payload, signature })
    });
    if (!response.ok) return { dispatched: false, code: `INVOICE_CONTINUATION_HTTP_${response.status}` };
    return { dispatched: true, depth };
  } catch (error) {
    console.warn(JSON.stringify({ event: 'invoice_continuation_dispatch_failed', code: String(error?.code || error?.message || 'DISPATCH_FAILED').slice(0, 120), depth }));
    return { dispatched: false, code: 'INVOICE_CONTINUATION_DISPATCH_FAILED' };
  }
}

export async function drainInvoiceOperations(env, options = {}) {
  const config = options.config || getInvoiceQueueRuntimeConfig(env);
  if (!config.enabled) return { ok: true, skipped: true, code: 'INVOICE_ASYNC_PIPELINE_DISABLED' };
  if (typeof options.rpc !== 'function') throw new Error('INVOICE_QUEUE_RPC_ADAPTER_REQUIRED');
  const mode = selectInvoiceDrainMode(options);
  const userMode = mode === 'user_nudge';
  const databaseClaimLimit = userMode ? config.userNudgeDatabaseClaimLimit : config.scheduledDatabaseClaimLimit;
  const documentClaimLimit = userMode ? 0 : config.scheduledDocumentClaimLimit;
  const maxCycles = userMode ? config.userNudgeCycles : config.scheduledCycles;
  const startedAt = Date.now();
  const deadline = startedAt + (userMode ? config.userNudgeDeadlineMs : config.scheduledDrainDeadlineMs) - config.safetyMarginMs;
  const requestedLanes = new Set(normaliseInvoiceLanes(options.lanes || ['ALL']));
  const cycles = [];
  let continuationRequired = false;
  for (let cycle = 0; cycle < maxCycles && Date.now() < deadline; cycle += 1) {
    const resolved = lanesToChunkTypes([...requestedLanes]);
    const workerSuffix = crypto.randomUUID();
    const databaseClaims = await claimBatch(options.rpc, resolved.database, `${config.workerId}:db:${workerSuffix}`, databaseClaimLimit, config.leaseSeconds);
    const database = await processInvoiceDatabaseChunksBatch(env, databaseClaims, { rpc: options.rpc, config });
    for (const lane of database.released_lanes || []) requestedLanes.add(lane);
    const released = lanesToChunkTypes([...requestedLanes]);
    const documentClaims = !userMode && Date.now() < deadline
      ? await claimBatch(options.rpc, released.document, `${config.workerId}:doc:${workerSuffix}`, documentClaimLimit, config.leaseSeconds)
      : [];
    const document = await processInvoiceDocumentChunksBatch(env, documentClaims, { rpc: options.rpc, config });
    for (const lane of document.released_lanes || []) requestedLanes.add(lane);
    const explicitDocumentLanes = lanesToChunkTypes([...requestedLanes]).document.length > 0;
    continuationRequired =
      databaseClaims.length >= databaseClaimLimit
      || (!userMode && documentClaimLimit > 0 && documentClaims.length >= documentClaimLimit)
      || (database.released_lanes || []).length > 0
      || (document.released_lanes || []).length > 0
      || (userMode && explicitDocumentLanes);
    cycles.push({ database, document });
    if (!databaseClaims.length && !documentClaims.length) break;
    if (userMode || !continuationRequired) break;
  }
  const aggregate = (key, field) => cycles.reduce((total, cycle) => total + Number(cycle[key]?.[field] || 0), 0);
  const summary = {
    ok: true, invocation_id: crypto.randomUUID(), mode,
    elapsed_ms: Date.now() - startedAt, cycles: cycles.length,
    database: { claimed: aggregate('database','claimed'), advanced: aggregate('database','advanced'), rejected: aggregate('database','rejected') },
    document: { claimed: aggregate('document','claimed'), processed: aggregate('document','processed'), completed: aggregate('document','completed'), rejected: aggregate('document','rejected'), ownership_lost: aggregate('document','ownership_lost') },
    continuation_required: continuationRequired,
    released_lanes: normaliseInvoiceLanes([...requestedLanes])
  };
  if (continuationRequired) {
    const continuation = await requestFreshInvoiceContinuation(env, {
      config, lanes: [...requestedLanes], continuationDepth: Number(options.continuationDepth || 0),
      priorityClass: options.priorityClass || (userMode ? 'INTERACTIVE' : 'SCHEDULED'),
      reconciliationCursor: options.reconciliationCursor
    });
    summary.continuation_dispatched = continuation.dispatched;
    summary.continuation_code = continuation.code;
  } else summary.continuation_dispatched = false;
  console.log(JSON.stringify({ event: 'invoice_queue_drain', ...summary }));
  return summary;
}

export async function nudgeInvoiceOperations(env, operationSummaries, options = {}) {
  const config = options.config || getInvoiceQueueRuntimeConfig(env);
  if (!config.enabled) return { scheduled: false, code: 'INVOICE_ASYNC_PIPELINE_DISABLED' };
  const accepted = rowsFromRpc(operationSummaries).filter(row => row?.accepted !== false && !['COMPLETE','FAILED','DEAD_LETTER','CANCELLED','SUPERSEDED'].includes(String(row?.status || '').toUpperCase()));
  const operationIds = [...new Set(accepted.map(row => row.operation_id).filter(Boolean))].sort();
  const lanes = deriveInvoiceOperationLanes(accepted, options.lanes || []);
  const priorityClass = normaliseInvoicePriorityClass(options.priorityClass);
  const key = `${priorityClass}:${lanes.join(',')}:${operationIds.join(',') || 'anonymous'}`;
  if (activeNudges.has(key)) return { scheduled: false, coalesced: true, key };
  activeNudges.add(key);
  const task = drainInvoiceOperations(env, { ...options, config, lanes, mode: 'user_nudge', priorityClass })
    .finally(() => activeNudges.delete(key));
  if (options.ctx) {
    options.ctx.waitUntil(task);
    return { scheduled: true, key, operation_ids: operationIds, lanes, priority_class: priorityClass };
  }
  await task;
  return { scheduled: true, key, operation_ids: operationIds, lanes, priority_class: priorityClass };
}

export async function handleInvoiceQueueDrainRequest(request, env, options = {}) {
  if (request.headers.get('x-cloudtms-internal-service') !== 'invoice-queue-dispatcher-v1') return new Response(JSON.stringify({ ok: false, code: 'INVOICE_DRAIN_CALLER_INVALID' }), { status: 403, headers: { 'content-type': 'application/json' } });
  const contentLength = Number(request.headers.get('content-length') || 0);
  if (contentLength > 8192) return new Response(JSON.stringify({ ok: false, code: 'INVOICE_DRAIN_REQUEST_TOO_LARGE' }), { status: 413, headers: { 'content-type': 'application/json' } });
  const body = await request.json().catch(() => null);
  if (!body) return new Response(JSON.stringify({ ok: false, code: 'INVOICE_DRAIN_REQUEST_INVALID' }), { status: 400, headers: { 'content-type': 'application/json' } });
  const payload = {
    timestamp: body.timestamp,
    nonce: body.nonce,
    depth: body.depth,
    lanes: body.lanes,
    priority_class: body.priority_class,
    reconciliation_cursor: body.reconciliation_cursor
  };
  const age = Math.abs(Date.now() - Number(payload.timestamp));
  const validDepth = Number.isInteger(Number(payload.depth)) && Number(payload.depth) >= 0 && Number(payload.depth) <= 4;
  let lanes;
  try { lanes = normaliseInvoiceLanes(payload.lanes); } catch { lanes = null; }
  const cursor = payload.reconciliation_cursor;
  const validCursor = cursor == null || (
    lanes?.includes('RECONCILE')
    && Number.isFinite(Date.parse(cursor.snapshot_at_utc || ''))
    && Number.isFinite(Date.parse(cursor.updated_at_utc || ''))
    && UUID_PATTERN.test(String(cursor.operation_id || ''))
  );
  const signatureValid = age <= 60000 && validDepth && lanes && validCursor
    ? await verifyInvoiceDrainSignature(env.INVOICE_QUEUE_DISPATCH_SECRET, payload, body.signature)
    : false;
  if (!signatureValid) return new Response(JSON.stringify({ ok: false, code: 'INVOICE_DRAIN_AUTH_INVALID' }), { status: 403, headers: { 'content-type': 'application/json' } });
  const task = (async () => {
    if (payload.reconciliation_cursor) {
      await runInvoiceReconciliationCycle(env, {
        rpc: options.rpc,
        config: options.config,
        continuationDepth: Number(payload.depth),
        reconciliationCursor: payload.reconciliation_cursor
      });
    }
    return drainInvoiceOperations(env, {
      rpc: options.rpc,
      config: options.config,
      lanes,
      mode: 'internal',
      continuationDepth: Number(payload.depth),
      priorityClass: payload.priority_class
    });
  })();
  if (options.ctx) options.ctx.waitUntil(task); else await task;
  return new Response(JSON.stringify({ ok: true, accepted: true, depth: Number(payload.depth) }), { status: 202, headers: { 'content-type': 'application/json' } });
}
export async function validateInvoiceSystemActor(env, actorUserId) {
  const id = String(actorUserId || '').trim();
  if (!UUID_PATTERN.test(id) || !env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) return false;
  const url = new URL(`${env.SUPABASE_URL}/rest/v1/tms_users`);
  url.searchParams.set('id', `eq.${id}`);
  url.searchParams.set('select', 'id,is_active,role');
  url.searchParams.set('limit', '1');
  const response = await fetch(url, { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } });
  if (!response.ok) return false;
  const rows = await response.json().catch(() => []);
  return rows.length === 1 && rows[0]?.is_active === true && String(rows[0]?.role || '').toLowerCase() === 'admin';
}
export async function runAutoInvoiceCycleAsync(env, options = {}) {
  const config = options.config || getInvoiceQueueRuntimeConfig(env);
  if (!config.enabled) return { ok: true, skipped: true, code: 'INVOICE_ASYNC_PIPELINE_DISABLED' };
  const rpc = options.rpc;
  const actorUserId = String(options.actorUserId || env.INVOICE_ACTOR_USER_ID || '').trim();
  const validateSystemActor = options.validateSystemActor || validateInvoiceSystemActor;
  if (!actorUserId || !(await validateSystemActor(env, actorUserId))) {
    return { ok: false, skipped: true, code: 'INVOICE_SYSTEM_ACTOR_INVALID' };
  }
  if (typeof rpc !== 'function') {
    return { ok: false, skipped: true, code: 'INVOICE_RPC_UNAVAILABLE' };
  }
  const selection = {
    contract_version: 'INVOICE_BATCH_SELECTION_V2',
    mode: 'IMPLICIT_ALL',
    default_selected: true,
    rules: []
  };
  const filters = {
    client_ids: [],
    candidate_ids: [],
    week_endings: [],
    week_ending_from: null,
    week_ending_to: null,
    status_codes: [],
    blocker_codes: [],
    search: null,
    allow_early: false,
    display_mode: 'ALL',
    invoice_streams: []
  };
  const sort = {
    group_preset: 'WEEK_CLIENT_CANDIDATE',
    sort_key: 'WEEK_ENDING_DATE',
    sort_direction: 'ASC'
  };
  const rawFirstPage = await rpc('invoice_batch_generate_candidates', {
    p_query: {
      contract_version: 'INVOICE_BATCH_QUERY_V2',
      action: 'GENERATE',
      mode: 'PAGE',
      snapshot: null,
      page_size: 1,
      cursor: null,
      filters,
      sort,
      selection
    }
  });
  const unwrapCandidateEnvelope = value => {
    if (value?.contract_version === 'INVOICE_BATCH_CANDIDATES_V2'
        && value?.action === 'GENERATE'
        && Array.isArray(value?.rows)) {
      return value;
    }
    if (Array.isArray(value)) {
      return value.length === 1 ? unwrapCandidateEnvelope(value[0]) : null;
    }
    if (value && typeof value === 'object') {
      for (const key of ['data', 'rows']) {
        if (Array.isArray(value[key]) && value[key].length === 1) {
          const nested = unwrapCandidateEnvelope(value[key][0]);
          if (nested) return nested;
        }
      }
      const values = Object.values(value);
      if (values.length === 1) return unwrapCandidateEnvelope(values[0]);
    }
    return null;
  };
  const firstPage = unwrapCandidateEnvelope(rawFirstPage) || {};
  if (firstPage.contract_version !== 'INVOICE_BATCH_CANDIDATES_V2'
      || firstPage.action !== 'GENERATE'
      || !firstPage.snapshot
      || typeof firstPage.snapshot !== 'object') {
    return { ok: false, skipped: true, code: 'INVOICE_AUTO_CANDIDATE_CONTRACT_INVALID' };
  }
  const selectedTotal = Number(firstPage?.selection_summary?.selected_total || 0);
  if (!Number.isSafeInteger(selectedTotal) || selectedTotal < 1) {
    return { ok: true, candidates: 0, submitted: 0, operations: [] };
  }
  const commandToken = `AUTO:GENERATE:${String(firstPage.snapshot.revision || '')}`;
  const startResponse = await rpc('invoice_operation_start_batch', {
    p_commands: [{
      command_type: 'GENERATE_SELECTED',
      selection_contract: {
        contract_version: 'INVOICE_BATCH_SELECTION_ROOT_V2',
        query: {
          contract_version: 'INVOICE_BATCH_QUERY_V2',
          action: 'GENERATE',
          mode: 'PAGE',
          snapshot: firstPage.snapshot,
          page_size: 100,
          cursor: null,
          filters,
          sort,
          selection
        },
        selection
      },
      command_token: commandToken
    }],
    p_actor_user_id: actorUserId
  });
  const operations = valueFromRpc(startResponse);
  if (operations.length !== 1 || Number(operations[0]?.command_no) !== 1) {
    throw Object.assign(new Error('INVOICE_AUTO_START_RESULT_CORRELATION_INVALID'), {
      code: 'INVOICE_AUTO_START_RESULT_CORRELATION_INVALID'
    });
  }
  await nudgeInvoiceOperations(env, operations, {
    ...options,
    config,
    lanes: ['DATABASE']
  });
  return {
    ok: true,
    candidates: selectedTotal,
    submitted: selectedTotal,
    root_operation_id: operations[0]?.operation_id || null,
    operations
  };
}

export async function runInvoiceReconciliationCycle(env, options = {}) {
  const config = options.config || getInvoiceQueueRuntimeConfig(env);
  if (!config.enabled) return { ok: true, skipped: true, code: 'INVOICE_ASYNC_PIPELINE_DISABLED' };
  const rpc = options.rpc;
  const actorUserId = String(options.actorUserId || env.INVOICE_ACTOR_USER_ID || '').trim();
  if (!actorUserId || !(await validateInvoiceSystemActor(env, actorUserId))) return { ok: false, skipped: true, code: 'INVOICE_SYSTEM_ACTOR_INVALID' };
  const olderThanSeconds = parseBoundedInteger(options.olderThanSeconds, 300, 60, 86400);
  const nowMs = Date.now();
  const cutoffUtc = new Date(nowMs - olderThanSeconds * 1000).toISOString();
  const initialCursor = options.reconciliationCursor || null;
  const snapshotUtc = initialCursor?.snapshot_at_utc
    ? new Date(initialCursor.snapshot_at_utc).toISOString()
    : new Date(nowMs).toISOString();
  let cursor = initialCursor ? {
    snapshot_at_utc: snapshotUtc,
    updated_at_utc: new Date(initialCursor.updated_at_utc).toISOString(),
    operation_id: String(initialCursor.operation_id).toLowerCase()
  } : null;
  let rowsExamined = 0;
  let rowsSubmitted = 0;
  let hasMore = false;
  const reconciliation = [];
  for (
    let page = 0;
    page < config.reconciliationMaximumPagesPerInvocation;
    page += 1
  ) {
    const query = new URL(`${env.SUPABASE_URL}/rest/v1/invoice_operations`);
    query.searchParams.set('status', 'in.(QUEUED,RUNNING,WAITING,RETRY_WAIT,BLOCKED)');
    query.searchParams.set('select', 'id,updated_at_utc');
    query.searchParams.set('and', `(updated_at_utc.lt.${cutoffUtc},updated_at_utc.lte.${snapshotUtc})`);
    if (cursor) {
      query.searchParams.set(
        'or',
        `(updated_at_utc.gt.${cursor.updated_at_utc},and(updated_at_utc.eq.${cursor.updated_at_utc},id.gt.${cursor.operation_id}))`
      );
    }
    query.searchParams.set('order', 'updated_at_utc.asc,id.asc');
    query.searchParams.set('limit', String(config.reconciliationPageSize + 1));
    const response = await fetch(query, {
      headers: {
        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
        authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
      }
    });
    const loaded = await response.json().catch(() => []);
    if (!response.ok || !Array.isArray(loaded)) throw Object.assign(new Error('INVOICE_RECONCILIATION_SCOPE_QUERY_FAILED'), { code: 'INVOICE_RECONCILIATION_SCOPE_QUERY_FAILED' });
    hasMore = loaded.length > config.reconciliationPageSize;
    const rows = loaded.slice(0, config.reconciliationPageSize);
    rowsExamined += rows.length;
    if (!rows.length) { hasMore = false; break; }
    const operationIds = rows.map(row => row?.id).filter(id => UUID_PATTERN.test(String(id || '')));
    if (operationIds.length) {
      const started = await rpc('invoice_operation_start_batch', {
        p_commands: [{
          command_type: 'RECONCILE',
          operation_ids: operationIds,
          older_than_seconds: olderThanSeconds,
          max_rows: config.reconciliationPageSize
        }],
        p_actor_user_id: actorUserId,
        p_now_utc: new Date().toISOString()
      });
      const operationRows = valueFromRpc(started);
      reconciliation.push(...operationRows);
      rowsSubmitted += operationIds.length;
    }
    const last = rows.at(-1);
    cursor = {
      snapshot_at_utc: snapshotUtc,
      updated_at_utc: new Date(last.updated_at_utc).toISOString(),
      operation_id: String(last.id).toLowerCase()
    };
    if (!hasMore) break;
  }
  if (reconciliation.length) {
    await nudgeInvoiceOperations(env, reconciliation, {
      ...options,
      config,
      lanes: ['RECONCILE'],
      priorityClass: 'RECONCILE'
    });
  }
  let continuation = { dispatched: false };
  if (hasMore && cursor) {
    continuation = await requestFreshInvoiceContinuation(env, {
      config,
      lanes: ['RECONCILE'],
      continuationDepth: Number(options.continuationDepth || 0),
      priorityClass: 'RECONCILE',
      reconciliationCursor: cursor
    });
  }
  return {
    ok: true,
    rows_examined: rowsExamined,
    operations_scoped: rowsSubmitted,
    reconciliation,
    next_cursor: hasMore ? cursor : null,
    continuation_requested: hasMore,
    continuation_dispatched: continuation.dispatched === true
  };
}

export const invoiceQueueRuntimeInternals = Object.freeze({
  rowsFromRpc,
  valueFromRpc,
  lanesToChunkTypes,
  parseBoundedInteger,
  postgresJsonbText,
  processorIdentity,
  compactError,
  verifyFrozenPresentationModelHash
});


