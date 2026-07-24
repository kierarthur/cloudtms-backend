import puppeteer from '@cloudflare/puppeteer';
import { PDFDocument } from 'pdf-lib';
import {
  buildInvoiceSourceDocumentHtml,
  buildProfessionalInvoiceHtml
} from './invoice-document-templates.js';
import { signInvoiceDrainRequest, verifyInvoiceDrainSignature } from './invoice-queue-security.js';

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
    nativeRequestTimeoutMs: parseBoundedInteger(env.INVOICE_NATIVE_REQUEST_TIMEOUT_MS, 120000, 10000, 300000),
    browserRenderTimeoutMs: parseBoundedInteger(env.INVOICE_BROWSER_RENDER_TIMEOUT_MS, 45000, 5000, 90000),
    browserRenderOutputMaxBytes: parseBoundedInteger(env.INVOICE_BROWSER_RENDER_OUTPUT_MAX_BYTES, 16777216, 1048576, 67108864),
    browserRenderOutputMaxPages: parseBoundedInteger(env.INVOICE_BROWSER_RENDER_OUTPUT_MAX_PAGES, 250, 1, 1000),
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
  let currentPage = 1;
  for (const part of stream) {
    const pages = Math.max(0, Number(part.page_count || part.pages || 0));
    const kind = String(part.kind || part.section_type || '').toUpperCase();
    if (kind === 'CORE') currentPage += pages;
    else if (kind === 'ATTACHMENT_INDEX') currentPage += finalIndexPageCount;
    else if (kind === 'SEPARATOR') currentPage += pages;
    else {
      const rowId = String(part.display_row_id || part.logical_source_id || '');
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
  return [...displayed.values()];
}

async function resolveApprovedRenderAsset(env, identity) {
  if (!identity?.r2_key) return identity || {};
  const object = await env.R2.get(String(identity.r2_key));
  if (!object) throw Object.assign(new Error('RENDER_ASSET_MISSING'), { code: 'RENDER_ASSET_MISSING' });
  if (identity.size_bytes != null && Number(identity.size_bytes) !== Number(object.size)) throw Object.assign(new Error('RENDER_ASSET_SIZE_MISMATCH'), { code: 'RENDER_ASSET_SIZE_MISMATCH' });
  if (!identity.sha256 || object.customMetadata?.sha256 !== identity.sha256) throw Object.assign(new Error('RENDER_ASSET_HASH_MISMATCH'), { code: 'RENDER_ASSET_HASH_MISMATCH' });
  if (Number(object.size) > 2 * 1024 * 1024) throw Object.assign(new Error('RENDER_ASSET_TOO_LARGE'), { code: 'RENDER_ASSET_TOO_LARGE' });
  const mediaType = String(identity.media_type || object.httpMetadata?.contentType || 'image/png');
  if (!['image/png','image/jpeg'].includes(mediaType)) throw Object.assign(new Error('RENDER_ASSET_MEDIA_UNSUPPORTED'), { code: 'RENDER_ASSET_MEDIA_UNSUPPORTED' });
  const bytes = new Uint8Array(await object.arrayBuffer());
  let binary = '';
  for (let offset = 0; offset < bytes.length; offset += 32768) binary += String.fromCharCode(...bytes.subarray(offset, offset + 32768));
  return { ...identity, data_url: `data:${mediaType};base64,${btoa(binary)}` };
}

async function resolveEmbeddedBrandingAssets(env, sourceModel) {
  const model = structuredClone(sourceModel || {});
  if (model.branding?.r2_key) model.branding = await resolveApprovedRenderAsset(env, model.branding);
  for (const key of ['candidate_signature','nurse_signature','authoriser_signature']) {
    if (model[key]?.r2_key) model[key] = await resolveApprovedRenderAsset(env, model[key]);
  }
  return model;
}
async function renderBrowserDocument(env, contextRow, config, signal) {
  if (!env.BROWSER) throw Object.assign(new Error('INVOICE_BROWSER_BINDING_MISSING'), { code: 'INVOICE_BROWSER_BINDING_MISSING' });
  if (!env.R2) throw Object.assign(new Error('INVOICE_R2_BINDING_MISSING'), { code: 'INVOICE_R2_BINDING_MISSING' });
  const context = contextRow.context || {};
  const identity = processorIdentity(contextRow);
  const renderKind = String(identity.render_kind || context.render_kind || '').toUpperCase();
  const frozenSnapshot = context.model || context.frozen_presentation_model
    || context.frozen_invoice_snapshot || context.snapshot || {};
  const presentationModel = frozenSnapshot.presentation_model || frozenSnapshot.render_model || frozenSnapshot;
  const embeddedModel = await resolveEmbeddedBrandingAssets(env, presentationModel);
  const layout = context.attachment_index_layout || {};
  const model = renderKind === 'ATTACHMENT_INDEX'
    ? { ...embeddedModel, display_rows: deriveAttachmentDisplayMap(layout, Number(layout.expected_index_page_count || 1)) }
    : embeddedModel;
  const html = renderKind === 'INVOICE_CORE'
    ? buildProfessionalInvoiceHtml(model)
    : buildInvoiceSourceDocumentHtml(renderKind, model);
  const browser = await puppeteer.launch(env.BROWSER);
  const abort = () => { void browser.close().catch(() => undefined); };
  signal?.addEventListener('abort', abort, { once: true });
  try {
    if (signal?.aborted) throw Object.assign(new Error('OWNERSHIP_LOST'), { code: 'OWNERSHIP_LOST' });
    const page = await browser.newPage();
    await page.setRequestInterception(true);
    page.on('request', request => {
      const url = request.url();
      if (url.startsWith('data:') || url.startsWith('about:')) void request.continue();
      else void request.abort('blockedbyclient');
    });
    await page.setContent(html, { waitUntil: 'domcontentloaded', timeout: config.browserRenderTimeoutMs });
    await page.emulateMediaType('print');
    const buffer = await page.pdf({
      format: 'A4', printBackground: true, preferCSSPageSize: true,
      displayHeaderFooter: true,
      headerTemplate: '<span></span>',
      footerTemplate: '<div style="font-size:8px;width:100%;text-align:center;color:#667085"><span class="pageNumber"></span> / <span class="totalPages"></span></div>',
      margin: { top: '12mm', right: '12mm', bottom: '16mm', left: '12mm' }
    });
    const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
    if (bytes.byteLength > config.browserRenderOutputMaxBytes) throw Object.assign(new Error('BROWSER_RENDER_OUTPUT_TOO_LARGE'), { code: 'BROWSER_RENDER_OUTPUT_TOO_LARGE' });
    const sha256 = await sha256Hex(bytes);
    const pageCount = await pdfPageCount(bytes);
    if (pageCount > config.browserRenderOutputMaxPages) throw Object.assign(new Error('BROWSER_RENDER_PAGE_LIMIT_EXCEEDED'), { code: 'BROWSER_RENDER_PAGE_LIMIT_EXCEEDED' });
    const outputPrefix = String(identity.output_prefix || '');
    if (!outputPrefix) throw Object.assign(new Error('INVOICE_OUTPUT_PREFIX_MISSING'), { code: 'INVOICE_OUTPUT_PREFIX_MISSING' });
    const r2Key = `${outputPrefix}${renderKind.toLowerCase()}-${sha256}.pdf`;
    const metadata = {
      sha256, size_bytes: bytes.byteLength, chunk_id: identity.chunk_id,
      fence_token: identity.fence_token, plan_generation: identity.plan_generation,
      document_version_id: identity.document_version_id, render_kind: renderKind,
      processor_policy_version: identity.processor_policy_version,
      template_version: identity.template_version
    };
    await putImmutableInvoiceArtifact(env.R2, r2Key, bytes, metadata);
    const result = {
      ...identity, output_prefix: outputPrefix, output_type: 'application/pdf', r2_key: r2Key,
      sha256, size_bytes: bytes.byteLength, page_count: pageCount, parse_verified: true,
      processor_version: 'cloudtms-browser-renderer-v4'
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
      result.layout_identity_hash = await sha256Hex(new TextEncoder().encode(postgresJsonbText(layout.determinism || {})));
      result.displayed_rows_verified = true;
    }
    return result;
  } finally {
    signal?.removeEventListener('abort', abort);
    await browser.close().catch(() => undefined);
  }
}

async function runNativeProcessor(env, contextRow, config, signal) {
  if (!env.INVOICE_DOCUMENT_PROCESSOR) {
    throw new Error('INVOICE_DOCUMENT_PROCESSOR_BINDING_MISSING');
  }
  const timeout = AbortSignal.timeout(config.nativeRequestTimeoutMs);
  const combinedSignal = signal ? AbortSignal.any([signal, timeout]) : timeout;
  const response = await env.INVOICE_DOCUMENT_PROCESSOR.fetch(
    'https://invoice-document-processor.internal/process',
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-cloudtms-internal-service': 'invoice-document-v1'
      },
      body: JSON.stringify({
        expected_result_identity: contextRow.expected_result_identity,
        context: contextRow.context
      }),
      signal: combinedSignal
    }
  );
  const result = await response.json();
  if (!response.ok || result?.ok === false) {
    const error = new Error(result?.message || result?.code || `PROCESSOR_HTTP_${response.status}`);
    error.code = result?.code || `PROCESSOR_HTTP_${response.status}`;
    throw error;
  }
  return result.result || result;
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
  return {
    context, claim, abortController: new AbortController(),
    latestLeaseExpiry: claim.lease_expires_at_utc || null,
    ownershipState: 'OWNED', processorPromise: null
  };
}

function touchAccepted(row) {
  return row?.accepted === true && !['REJECTED','STALE','OWNERSHIP_LOST'].includes(String(row?.status || '').toUpperCase());
}

export async function processInvoiceDocumentChunksBatch(env, claims, options) {
  if (!claims.length) return { claimed: 0, processed: 0, rejected: 0, completed: 0, ownership_lost: 0 };
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
  const activeJobs = new Map(valid.map(context => [context.chunk_id, createActiveDocumentJob(context, claimById.get(context.chunk_id))]));
  let heartbeatStopped = false;
  let heartbeatPromise = Promise.resolve();
  let heartbeatTimer = null;

  const heartbeat = async () => {
    const jobs = [...activeJobs.values()].filter(job => job.ownershipState === 'OWNED');
    if (!jobs.length) return;
    try {
      const raw = await rpc('invoice_work_touch_batch', {
        p_touches: jobs.map(job => ({
          ...claimIdentity(job.claim), progress: { status_message: 'Processing document' }
        })),
        p_now_utc: new Date().toISOString()
      });
      const rows = valueFromRpc(raw);
      const byId = new Map(rows.map(row => [row.chunk_id, row]));
      for (const job of jobs) {
        const row = byId.get(job.claim.chunk_id);
        if (!touchAccepted(row)) {
          job.ownershipState = 'OWNERSHIP_LOST';
          job.abortController.abort('OWNERSHIP_LOST');
          activeJobs.delete(job.claim.chunk_id);
        } else if (row.lease_expires_at_utc) job.latestLeaseExpiry = row.lease_expires_at_utc;
      }
    } catch (error) {
      console.warn(JSON.stringify({ event: 'invoice_document_heartbeat_failed', code: String(error?.code || error?.message || 'HEARTBEAT_FAILED').slice(0, 120), active_count: jobs.length }));
    }
  };
  const scheduleHeartbeat = () => {
    if (heartbeatStopped) return;
    heartbeatTimer = setTimeout(() => {
      heartbeatPromise = heartbeatPromise.then(heartbeat).finally(scheduleHeartbeat);
    }, config.heartbeatMs);
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
          job.processorPromise = runInvoiceDocumentProcessor(env, contextRow, { config, signal: job.abortController.signal });
          const result = await job.processorPromise;
          if (job.ownershipState !== 'OWNED') return { ownership_lost: true, chunk_id: contextRow.chunk_id };
          return { ...claimIdentity(job.claim), outcome: 'SUCCESS', result };
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
          return { ...claimIdentity(job.claim), outcome: permanent ? 'FAILED' : 'RETRY', error: compactError(error, !permanent) };
        } finally {
          activeJobs.delete(contextRow.chunk_id);
        }
      });
      processorResults.push(...results);
    }
  } finally {
    heartbeatStopped = true;
    if (heartbeatTimer) clearTimeout(heartbeatTimer);
    await heartbeatPromise;
  }

  const ownershipLost = processorResults.filter(row => row?.ownership_lost === true);
  const terminalProcessorResults = processorResults.filter(row => row?.ownership_lost !== true);
  const contextFailureResults = contextErrors.map(row => ({
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
  return {
    claimed: claims.length, valid_contexts: valid.length, rejected: rejected.length,
    processed: completionPayload.length,
    completed: completion.filter(row => row?.accepted === true && !['REJECTED','STALE'].includes(String(row?.status || '').toUpperCase())).length,
    ownership_lost: ownershipLost.length,
    context_rejections: rejected.map(row => ({ chunk_id: row.chunk_id, code: row.code })), completion
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
  const documentClaimLimit = userMode ? config.userNudgeDocumentClaimLimit : config.scheduledDocumentClaimLimit;
  const maxCycles = userMode ? config.userNudgeCycles : config.scheduledCycles;
  const startedAt = Date.now();
  const deadline = startedAt + (userMode ? config.userNudgeDeadlineMs : config.scheduledDrainDeadlineMs) - config.safetyMarginMs;
  const requestedLanes = new Set(normaliseInvoiceLanes(options.lanes || ['ALL']));
  const cycles = [];
  let likelyRunnable = false;
  for (let cycle = 0; cycle < maxCycles && Date.now() < deadline; cycle += 1) {
    const resolved = lanesToChunkTypes([...requestedLanes]);
    const workerSuffix = crypto.randomUUID();
    const databaseClaims = await claimBatch(options.rpc, resolved.database, `${config.workerId}:db:${workerSuffix}`, databaseClaimLimit, config.leaseSeconds);
    const database = await processInvoiceDatabaseChunksBatch(env, databaseClaims, { rpc: options.rpc, config });
    for (const lane of database.released_lanes || []) requestedLanes.add(lane);
    const released = lanesToChunkTypes([...requestedLanes]);
    const documentClaims = Date.now() < deadline
      ? await claimBatch(options.rpc, released.document, `${config.workerId}:doc:${workerSuffix}`, documentClaimLimit, config.leaseSeconds)
      : [];
    const document = await processInvoiceDocumentChunksBatch(env, documentClaims, { rpc: options.rpc, config });
    likelyRunnable = databaseClaims.length >= databaseClaimLimit
      || documentClaims.length >= documentClaimLimit
      || database.advanced > 0 || document.completed > 0;
    cycles.push({ database, document });
    if (!databaseClaims.length && !documentClaims.length) { likelyRunnable = false; break; }
    if (!likelyRunnable) break;
  }
  const aggregate = (key, field) => cycles.reduce((total, cycle) => total + Number(cycle[key]?.[field] || 0), 0);
  const summary = {
    ok: true, invocation_id: crypto.randomUUID(), mode,
    elapsed_ms: Date.now() - startedAt, cycles: cycles.length,
    database: { claimed: aggregate('database','claimed'), advanced: aggregate('database','advanced'), rejected: aggregate('database','rejected') },
    document: { claimed: aggregate('document','claimed'), processed: aggregate('document','processed'), completed: aggregate('document','completed'), rejected: aggregate('document','rejected'), ownership_lost: aggregate('document','ownership_lost') },
    continuation_required: likelyRunnable,
    released_lanes: normaliseInvoiceLanes([...requestedLanes])
  };
  if (likelyRunnable) {
    const continuation = await requestFreshInvoiceContinuation(env, {
      config, lanes: [...requestedLanes], continuationDepth: Number(options.continuationDepth || 0),
      priorityClass: options.priorityClass || (userMode ? 'INTERACTIVE' : 'SCHEDULED')
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
  const payload = { timestamp: body.timestamp, nonce: body.nonce, depth: body.depth, lanes: body.lanes, priority_class: body.priority_class };
  const age = Math.abs(Date.now() - Number(payload.timestamp));
  const validDepth = Number.isInteger(Number(payload.depth)) && Number(payload.depth) >= 0 && Number(payload.depth) <= 4;
  let lanes;
  try { lanes = normaliseInvoiceLanes(payload.lanes); } catch { lanes = null; }
  const signatureValid = age <= 60000 && validDepth && lanes
    ? await verifyInvoiceDrainSignature(env.INVOICE_QUEUE_DISPATCH_SECRET, payload, body.signature)
    : false;
  if (!signatureValid) return new Response(JSON.stringify({ ok: false, code: 'INVOICE_DRAIN_AUTH_INVALID' }), { status: 403, headers: { 'content-type': 'application/json' } });
  const task = drainInvoiceOperations(env, { rpc: options.rpc, config: options.config, lanes, mode: 'internal', continuationDepth: Number(payload.depth), priorityClass: payload.priority_class });
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
  if (!actorUserId || !(await validateInvoiceSystemActor(env, actorUserId))) return { ok: false, skipped: true, code: 'INVOICE_SYSTEM_ACTOR_INVALID' };
  const candidateResponse = await rpc('invoice_autoinvoice_candidate_groups', {
    p_limit: parseBoundedInteger(env.INVOICE_AUTO_GROUP_LIMIT, 500, 1, 5000)
  });
  const candidates = rowsFromRpc(candidateResponse).filter(row => row?.eligible_for_submission === true);
  if (!candidates.length) return { ok: true, candidates: 0, operations: [] };
  const commands = candidates.map(row => ({
    command_type: 'GENERATE_AUTO',
    source_ids: row.source_ids,
    canonical_source_members: row.canonical_source_members,
    target_invoice_week: row.invoice_week_start,
    consolidation_mode: row.consolidation_mode,
    source_revision_hash: row.source_revision_hash,
    invoice_stream: row.stream,
    allow_early: false,
    command_token: `AUTO:${row.client_id}:${row.invoice_week_start}:${row.source_revision_hash}`
  }));
  const startResponse = await rpc('invoice_operation_start_batch', {
    p_commands: commands,
    p_actor_user_id: actorUserId,
    p_now_utc: new Date().toISOString()
  });
  const operations = valueFromRpc(startResponse);
  await nudgeInvoiceOperations(env, operations, {
    ...options,
    config,
    lanes: ['DATABASE']
  });
  return { ok: true, candidates: candidates.length, operations };
}

export async function runInvoiceReconciliationCycle(env, options = {}) {
  const config = options.config || getInvoiceQueueRuntimeConfig(env);
  if (!config.enabled) return { ok: true, skipped: true, code: 'INVOICE_ASYNC_PIPELINE_DISABLED' };
  const rpc = options.rpc;
  const actorUserId = String(options.actorUserId || env.INVOICE_ACTOR_USER_ID || '').trim();
  if (!actorUserId || !(await validateInvoiceSystemActor(env, actorUserId))) return { ok: false, skipped: true, code: 'INVOICE_SYSTEM_ACTOR_INVALID' };
  const query = new URL(`${env.SUPABASE_URL}/rest/v1/invoice_operations`);
  query.searchParams.set('status', 'in.(QUEUED,RUNNING,WAITING,RETRY_WAIT,BLOCKED)');
  query.searchParams.set('select', 'id');
  query.searchParams.set('order', 'updated_at_utc.asc,id.asc');
  query.searchParams.set('limit', '100');
  const response = await fetch(query, {
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
    }
  });
  const rows = await response.json().catch(() => []);
  if (!response.ok) throw new Error('INVOICE_RECONCILIATION_SCOPE_QUERY_FAILED');
  const operationIds = rows.map(row => row?.id).filter(Boolean);
  if (!operationIds.length) return { ok: true, operations_scoped: 0, reconciliation: [] };
  const started = await rpc('invoice_operation_start_batch', {
    p_commands: [{
      command_type: 'RECONCILE',
      operation_ids: operationIds,
      older_than_seconds: 300,
      max_rows: 100
    }],
    p_actor_user_id: actorUserId,
    p_now_utc: new Date().toISOString()
  });
  const operations = valueFromRpc(started);
  await nudgeInvoiceOperations(env, operations, {
    ...options,
    config,
    lanes: ['RECONCILE']
  });
  return { ok: true, operations_scoped: operationIds.length, reconciliation: operations };
}

export const invoiceQueueRuntimeInternals = Object.freeze({
  rowsFromRpc,
  valueFromRpc,
  lanesToChunkTypes,
  parseBoundedInteger,
  postgresJsonbText,
  processorIdentity,
  compactError
});


