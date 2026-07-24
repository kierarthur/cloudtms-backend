import {
  buildInvoiceDocumentDownloadUrl,
  createInvoiceDocumentAccessToken,
  verifyInvoiceDocumentAccessToken
} from './invoice-document-access.js';
import {
  isInvoiceAsyncPipelineEnabled,
  nudgeInvoiceOperations
} from './invoice-queue-runtime.js';
import { isInvoiceAsyncUserAllowed } from './invoice-queue-security.js';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const JSON_HEADERS = Object.freeze({ 'content-type': 'application/json; charset=utf-8' });

function jsonResponse(body, status = 200, headers = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...JSON_HEADERS, ...headers }
  });
}

function rpcValue(value) {
  if (Array.isArray(value)) {
    if (value.length === 1 && value[0] && typeof value[0] === 'object') {
      const values = Object.values(value[0]);
      if (values.length === 1 && (Array.isArray(values[0]) || typeof values[0] === 'object')) {
        return values[0];
      }
    }
    return value;
  }
  if (value && Array.isArray(value.rows)) return rpcValue(value.rows);
  if (value && Array.isArray(value.data)) return rpcValue(value.data);
  return value;
}

function canonicalUuidArray(values) {
  const input = Array.isArray(values) ? values : [];
  const ids = input.map(value => String(value || '').trim().toLowerCase());
  if (!ids.length || ids.some(value => !UUID_PATTERN.test(value))) {
    throw new Error('VALID_UUID_ARRAY_REQUIRED');
  }
  return [...new Set(ids)].sort();
}

function boolValue(value, fallback = false) {
  if (value === true || value === false) return value;
  const normalised = String(value ?? '').trim().toLowerCase();
  if (!normalised) return fallback;
  return ['1', 'true', 'yes', 'y', 'on'].includes(normalised);
}

function canonicalEmailArray(value) {
  const values = Array.isArray(value)
    ? value
    : String(value || '').split(/[;,]/g);
  const emails = values
    .map(item => String(item || '').trim().toLowerCase())
    .filter(Boolean);
  if (emails.some(email => !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))) {
    throw new Error('INVALID_RECIPIENT_EMAIL');
  }
  return [...new Set(emails)].sort();
}

function canonicalDeliveryPolicy(value) {
  const policy = String(value || 'ATTACH').trim().toUpperCase();
  if (!['ATTACH','SPLIT','SECURE_LINK'].includes(policy)) throw new Error('DELIVERY_POLICY_INVALID');
  return policy;
}

function commandToken(req, body = {}) {
  return String(
    body.command_token
    || body.delivery_request_token
    || req.headers.get('idempotency-key')
    || req.headers.get('x-idempotency-key')
    || crypto.randomUUID()
  ).slice(0, 200);
}

function generationCommandFromBody(req, body, commandType = 'GENERATE_SELECTED') {
  const canonical = body.canonical_command && typeof body.canonical_command === 'object'
    ? structuredClone(body.canonical_command)
    : {};
  const sourceIds = body.source_ids || body.timesheet_ids || canonical.source_ids || canonical.timesheet_ids;
  return {
    ...canonical,
    command_type: commandType,
    source_ids: canonicalUuidArray(sourceIds),
    canonical_source_members:
      body.canonical_source_members || canonical.canonical_source_members || undefined,
    consolidation_mode:
      body.consolidation_mode || canonical.consolidation_mode || 'NONE',
    allow_early: boolValue(body.allow_early ?? canonical.allow_early, false),
    target_invoice_week:
      body.target_invoice_week || body.invoice_week_start || canonical.target_invoice_week || undefined,
    command_token: commandToken(req, body)
  };
}

async function startCommands(env, req, ctx, user, commands, deps, lanes = ['ALL'], options = {}) {
  const raw = await deps.rpc('invoice_operation_start_batch', { p_commands: commands, p_actor_user_id: user.id, p_now_utc: new Date().toISOString() });
  const value = rpcValue(raw);
  const operations = Array.isArray(value) ? value : (value ? [value] : []);
  const created = operations.filter(row => row?.accepted !== false && row?.created === true);
  const reusedActive = operations.filter(row => row?.accepted !== false && row?.reused_active === true);
  const reusedReady = operations.filter(row => row?.accepted !== false && row?.reused_ready === true);
  const stableCode = row => String(row?.code || row?.error_code || row?.terminal_error?.code || row?.terminal_error || row?.error || '').trim().toUpperCase();
  const conflictCode = row => /(?:CONFLICT|SOURCE_CHANGED|STALE|ACTIVE_OPERATION|ALREADY_ACTIVE|NOT_READY|TERMINAL|BLOCKED)/.test(stableCode(row));
  const blocked = operations.filter(row => row?.blocked === true || !!row?.terminal_error);
  const conflicted = operations.filter(row => row?.accepted === false && !row?.blocked && !row?.terminal_error && conflictCode(row));
  const rejected = operations.filter(row => row?.accepted === false && !row?.blocked && !row?.terminal_error && !conflictCode(row));
  const active = [...created, ...reusedActive].filter((row, index, rows) => rows.findIndex(item => item.operation_id === row.operation_id) === index);
  const nudge = active.length ? await nudgeInvoiceOperations(env, active, { ctx, rpc: deps.rpc, lanes, priorityClass: options.priorityClass || 'INTERACTIVE' }) : { scheduled: false, code: 'NO_ACTIVE_WORK' };
  let status = 202;
  if (!operations.length) status = 502;
  else if (!active.length && reusedReady.length && !blocked.length && !conflicted.length && !rejected.length) status = 200;
  else if (!active.length && rejected.length && !blocked.length && !conflicted.length && !reusedReady.length) status = 400;
  else if (!active.length && (blocked.length || conflicted.length) && !reusedReady.length) status = 409;
  else if ((active.length || reusedReady.length) && (blocked.length || conflicted.length || rejected.length)) status = 207;
  const basePayload = { ok: active.length > 0 || reusedReady.length > 0, accepted: active.length > 0, accepted_count: active.length, created_count: created.length, reused_active_count: reusedActive.length, reused_ready_count: reusedReady.length, blocked_count: blocked.length, conflict_count: conflicted.length, rejected_count: rejected.length + conflicted.length, operation_ids: [...new Set(operations.map(row => row?.operation_id).filter(Boolean))], per_command_results: operations, nudge_state: nudge };
  const extraPayload = typeof options.extendResult === 'function' ? options.extendResult(basePayload, operations) : {};
  return jsonResponse({ ...basePayload, ...(extraPayload && typeof extraPayload === 'object' ? extraPayload : {}) }, status);
}

async function requireActor(env, req, deps, adminOnly = false) {
  return deps.requireUser(env, req, adminOnly ? ['admin'] : []);
}

async function parseBody(req) {
  try {
    const value = await req.json();
    return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
  } catch {
    return null;
  }
}

async function handleCandidates(req, deps, rpcName) {
  const url = new URL(req.url);
  const rawLimit = Number(url.searchParams.get('limit'));
  const defaultLimit = rpcName === 'invoice_batch_issue_candidates' ? 2000 : 5000;
  const maximum = rpcName === 'invoice_batch_issue_candidates' ? 2000 : 5000;
  const limit = Number.isFinite(rawLimit)
    ? Math.max(1, Math.min(maximum, Math.trunc(rawLimit)))
    : defaultLimit;
  const result = await deps.rpc(rpcName, {
    p_allow_early: boolValue(url.searchParams.get('allow_early'), false),
    p_limit: limit
  });
  const resolved = rpcValue(result);
  const candidates = Array.isArray(resolved) ? resolved : (Array.isArray(resolved?.candidates) ? resolved.candidates : []);
  const groups = Array.isArray(resolved?.groups) ? resolved.groups : candidates;
  return jsonResponse({ ok: true, candidates, groups, limit });
}

async function handleNhspCandidates(req, deps) {
  const url = new URL(req.url);
  const raw = await deps.rpc('invoice_batch_generate_candidates', {
    p_allow_early: boolValue(url.searchParams.get('allow_early'), false),
    p_limit: 5000
  });
  const clientId = String(url.searchParams.get('client_id') || '').trim();
  const candidates = (Array.isArray(rpcValue(raw)) ? rpcValue(raw) : [])
    .filter(row => {
      const command = row?.canonical_command
        || row?.command_ready_payload
        || row?.command_payload
        || row;
      const members = Array.isArray(command?.canonical_source_members)
        ? command.canonical_source_members
        : [];
      const nhsp = String(command?.command_type || '').toUpperCase() === 'GENERATE_NHSP'
        || String(command?.invoice_stream || command?.stream || '').toUpperCase().includes('NHSP')
        || members.some(member =>
          String(member?.source_type || member?.source_kind || '').toUpperCase().includes('NHSP'));
      return nhsp && (!clientId || String(command?.client_id || row?.client_id || '') === clientId);
    });
  return jsonResponse({
    ok: true,
    candidate_family: 'NHSP',
    candidates
  });
}

async function handleBatchGenerateConfirm(env, req, ctx, user, deps) {
  const body = await parseBody(req);
  const rows = body?.rows;
  if (!Array.isArray(rows) || !rows.length || rows.length > 500) return jsonResponse({ error: 'rows[] must contain 1..500 canonical candidate rows' }, 400);
  const rootToken = commandToken(req, body).slice(0, 100);
  const scopeKeys = rows.map(row => String(row.scope_key || row.group_key || row.canonical_command?.scope_key || '').trim());
  if (scopeKeys.some(value => !value) || new Set(scopeKeys).size !== scopeKeys.length) return jsonResponse({ error: 'UNIQUE_SCOPE_KEY_REQUIRED' }, 400);
  const commands = rows.map((row, index) => {
    const canonical = structuredClone(row.canonical_command || row.command_payload || row.command_ready_payload || {});
    if (!canonical || typeof canonical !== 'object' || Array.isArray(canonical)) throw new Error('CANONICAL_COMMAND_REQUIRED');
    return generationCommandFromBody(req, { canonical_command: canonical, allow_early: body.allow_early ?? canonical.allow_early, command_token: (rootToken + ':' + scopeKeys[index]).slice(0, 200) }, canonical.command_type || 'GENERATE_SELECTED');
  });
  return startCommands(env, req, ctx, user, commands, deps, ['DATABASE'], {
    extendResult: (summary, operationRows) => ({
      enqueued: summary.accepted_count,
      generated: summary.reused_ready_count,
      results_invoices: operationRows.map((operation, index) => ({
        scope_key: scopeKeys[index] || null,
        operation_id: operation?.operation_id || null,
        status: operation?.reused_ready === true ? 'READY' : (operation?.status || (operation?.accepted === false ? 'REJECTED' : 'QUEUED')),
        ok: operation?.accepted !== false,
        error: operation?.error || operation?.terminal_error || null,
        created: operation?.created === true,
        reused_active: operation?.reused_active === true,
        reused_ready: operation?.reused_ready === true
      })),
      invoice_ids: [...new Set(operationRows.flatMap(operation => operation?.created_invoice_ids || operation?.invoice_ids || []).filter(id => UUID_PATTERN.test(String(id))))]
    })
  });
}

async function handleBatchIssueConfirm(env, req, ctx, user, deps) {
  const body = await parseBody(req) || {};
  const rows = Array.isArray(body.rows) ? body.rows : [];
  const invoiceIds = canonicalUuidArray(body.invoice_ids || rows.map(row => row.invoice_id));
  const deliver = boolValue(body.deliver ?? body.send_email, false);
  const policy = canonicalDeliveryPolicy(body.delivery_policy);
  const requestToken = commandToken(req, body);
  const deliveryRequestToken = String(body.delivery_request_token || requestToken).slice(0, 200);
  const expectedRevisions = Object.fromEntries(rows.filter(row => row?.invoice_id && row?.document_revision != null).map(row => [String(row.invoice_id).toLowerCase(), String(row.document_revision)]));
  const command = { command_type: 'ISSUE_INVOICES', invoice_ids: invoiceIds, expected_revisions: expectedRevisions, allow_early: boolValue(body.allow_early, false), deliver, command_token: requestToken, delivery_intent: deliver ? { recipient_set: canonicalEmailArray(body.recipient_set || body.to || []), cc: canonicalEmailArray(body.cc || []), bcc: canonicalEmailArray(body.bcc || []), delivery_policy: policy, template_version: body.template_version || 'INVOICE_EMAIL_V1', delivery_request_token: deliveryRequestToken } : { deliver: false, delivery_request_token: deliveryRequestToken } };
  return startCommands(env, req, ctx, user, [command], deps, ['DATABASE','DOCUMENT'], {
    extendResult: (summary, operationRows) => {
      const root = operationRows[0] || {};
      const rootStatus = root.reused_ready === true ? 'ISSUED' : (root.status || (root.accepted === false ? 'REJECTED' : 'QUEUED'));
      return {
        enqueued: summary.accepted_count,
        invoice_results_enriched: invoiceIds.map(invoiceId => ({
          invoice_id: invoiceId,
          operation_id: root.operation_id || null,
          status: rootStatus,
          ok: root.accepted !== false,
          error: root.error || root.terminal_error || null,
          legal_issue_state: root.result?.legal_issue_state || root.legal_issue_state || (root.reused_ready === true ? 'ISSUED' : 'IN_PROGRESS'),
          delivery_state: deliver ? (root.result?.delivery_state || root.delivery_state || 'IN_PROGRESS') : 'NOT_REQUESTED'
        })),
        invoice_results: [],
        email_outbox: [],
        delivery_requested: deliver
      };
    }
  });
}

async function handleViewDocument(env, req, ctx, user, deps, entityType, entityId) {
  if (!UUID_PATTERN.test(entityId)) return jsonResponse({ error: 'INVALID_ENTITY_ID' }, 400);
  const body = req.method === 'POST' ? (await parseBody(req) || {}) : {};
  const serviceHeaders = { apikey: env.SUPABASE_SERVICE_ROLE_KEY, authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` };
  const loadReadyVersion = async (versionId, purpose) => {
    const query = new URL(`${env.SUPABASE_URL}/rest/v1/invoice_document_versions`);
    if (versionId) query.searchParams.set('id', `eq.${versionId}`);
    query.searchParams.set('entity_type', `eq.${entityType}`);
    query.searchParams.set('entity_id', `eq.${entityId}`);
    query.searchParams.set('purpose', `eq.${purpose}`);
    query.searchParams.set('status', 'eq.READY');
    query.searchParams.set('select', 'id,entity_type,entity_id,purpose,source_revision,template_version,r2_key,sha256,size_bytes,page_count,status');
    query.searchParams.set('order', 'ready_at_utc.desc.nullslast,id.desc');
    query.searchParams.set('limit', '1');
    const response = await fetch(query, { headers: serviceHeaders });
    const rows = await response.json().catch(() => []);
    const version = Array.isArray(rows) ? rows[0] : null;
    return response.ok && version?.r2_key && version?.sha256 && Number(version?.size_bytes) > 0 && Number(version?.page_count) > 0 ? version : null;
  };
  if (entityType === 'INVOICE') {
    const rawDetail = await deps.rpc('invoice_detail_get', { p_invoice_id: entityId, p_actor_user_id: user.id });
    const detailValue = rpcValue(rawDetail);
    const detail = Array.isArray(detailValue) ? detailValue[0] : detailValue;
    const header = detail?.invoice || detail?.header || detail || {};
    const status = String(header.status || detail?.invoice_status || '').toUpperCase();
    if (['ISSUED','PAID'].includes(status)) {
      const issuedVersionId = header.issued_document_version_id || detail?.issued_document_version_id;
      if (!UUID_PATTERN.test(String(issuedVersionId || ''))) return jsonResponse({ error: 'ISSUED_DOCUMENT_POINTER_MISSING' }, 409);
      const version = await loadReadyVersion(issuedVersionId, 'FINAL_ISSUE');
      return version
        ? jsonResponse({ ok: true, ready: true, status: 'READY', document_version: version })
        : jsonResponse({ error: 'ISSUED_DOCUMENT_INTEGRITY_FAILURE', document_version_id: issuedVersionId }, 409);
    }
  }
  const purpose = entityType === 'INVOICE' ? 'DRAFT_PREVIEW' : 'TIMESHEET';
  const command = {
    command_type: entityType === 'INVOICE' ? 'VIEW_INVOICE_DOCUMENT' : 'VIEW_TIMESHEET_DOCUMENT',
    [entityType === 'INVOICE' ? 'invoice_id' : 'timesheet_id']: entityId,
    purpose,
    priority_reason: body.priority_reason || 'VIEW_NOW',
    template_version: body.template_version
  };
  const operationsValue = rpcValue(await deps.rpc('invoice_operation_start_batch', { p_commands: [command], p_actor_user_id: user.id, p_now_utc: new Date().toISOString() }));
  const operations = Array.isArray(operationsValue) ? operationsValue : [operationsValue];
  const result = operations[0];
  if (result?.accepted === true && result?.reused_ready === true && result?.document_version_id) {
    const version = await loadReadyVersion(result.document_version_id, purpose);
    return version
      ? jsonResponse({ ok: true, ready: true, status: 'READY', document_version: version })
      : jsonResponse({ error: 'READY_DOCUMENT_IDENTITY_INVALID', document_version_id: result.document_version_id }, 409);
  }
  if (result?.accepted === false || result?.blocked === true || result?.terminal_error) {
    return jsonResponse({ ok: false, status: result?.status || 'BLOCKED', operation: result, error: result?.terminal_error || result?.error || 'DOCUMENT_PREPARATION_BLOCKED' }, 409);
  }
  const nudge = await nudgeInvoiceOperations(env, operations, { ctx, rpc: deps.rpc, lanes: ['DATABASE','DOCUMENT'], priorityClass: 'VIEW_NOW' });
  return jsonResponse({ ok: true, accepted: true, ready: false, status: result?.status || 'QUEUED', operations, nudge }, 202);
}
async function handleIssueOne(env, req, ctx, user, deps, invoiceId) {
  const body = await parseBody(req) || {};
  const deliver = boolValue(body.deliver ?? body.send_email, false);
  const requestToken = commandToken(req, body);
  return startCommands(env, req, ctx, user, [{
    command_type: 'ISSUE_INVOICES',
    invoice_ids: canonicalUuidArray([invoiceId]),
    allow_early: boolValue(body.allow_early, false),
    deliver,
    command_token: requestToken,
    delivery_intent: deliver ? {
      recipient_set: canonicalEmailArray(body.recipient_set || body.to || []),
      cc: canonicalEmailArray(body.cc || []),
      bcc: canonicalEmailArray(body.bcc || []),
      delivery_policy: canonicalDeliveryPolicy(body.delivery_policy),
      template_version: body.template_version || 'INVOICE_EMAIL_V1',
      delivery_request_token: body.delivery_request_token || requestToken
    } : { deliver: false }
  }], deps, ['DATABASE', 'DOCUMENT']);
}

async function handleDeliverOne(env, req, ctx, user, deps, invoiceId) {
  const body = await parseBody(req) || {};
  return startCommands(env, req, ctx, user, [{
    command_type: 'DELIVER_INVOICES',
    invoice_ids: canonicalUuidArray([invoiceId]),
    recipient_set: canonicalEmailArray(body.recipient_set || body.to || []),
    cc: canonicalEmailArray(body.cc || []),
    bcc: canonicalEmailArray(body.bcc || []),
    delivery_policy: canonicalDeliveryPolicy(body.delivery_policy),
    delivery_template_version: body.template_version || 'INVOICE_EMAIL_V1',
    delivery_part_number: Number(body.delivery_part_number || 1),
    delivery_request_token: commandToken(req, body)
  }], deps, ['DATABASE']);
}

async function handleOperationGet(req, user, deps, operationId) {
  const url = new URL(req.url);
  const body = req.method === 'POST' ? (await parseBody(req) || {}) : {};
  const ids = operationId
    ? canonicalUuidArray([operationId])
    : canonicalUuidArray(body.operation_ids);
  const result = await deps.rpc('invoice_operation_get', {
    p_operation_ids: ids,
    p_actor_user_id: user.id,
    p_mode: String(body.mode || url.searchParams.get('mode') || 'PROGRESS').toUpperCase()
  });
  return jsonResponse({ ok: true, operations: rpcValue(result) });
}

async function handleOperationControl(env, req, ctx, user, deps) {
  const body = await parseBody(req);
  const actions = Array.isArray(body?.actions) ? body.actions : [];
  if (!actions.length || actions.length > 100) return jsonResponse({ error: 'actions[] must contain 1..100 items' }, 400);
  const allowed = new Set(['RETRY','CANCEL','RESCHEDULE','RAISE_PRIORITY']);
  const safeActions = actions.map(raw => {
    const action = String(raw?.action || '').toUpperCase();
    const operationId = String(raw?.operation_id || '').toLowerCase();
    if (!allowed.has(action) || !UUID_PATTERN.test(operationId)) throw new Error('OPERATION_CONTROL_ACTION_INVALID');
    const item = { operation_id: operationId, action };
    if (action === 'RETRY' && raw.retry_chunk_id != null) {
      if (!UUID_PATTERN.test(String(raw.retry_chunk_id))) throw new Error('RETRY_CHUNK_ID_INVALID');
      item.retry_chunk_id = String(raw.retry_chunk_id).toLowerCase();
    }
    if (action === 'RESCHEDULE') {
      const timestamp = new Date(raw.run_after_utc || raw.scheduled_for_utc || '');
      if (!Number.isFinite(timestamp.getTime())) throw new Error('RESCHEDULE_TIMESTAMP_INVALID');
      item.run_after_utc = timestamp.toISOString();
    }
    return item;
  });
  const operations = rpcValue(await deps.rpc('invoice_operation_control_batch', { p_actions: safeActions, p_actor_user_id: user.id, p_now_utc: new Date().toISOString() }));
  await nudgeInvoiceOperations(env, operations, { ctx, rpc: deps.rpc, lanes: ['ALL'] });
  return jsonResponse({ ok: true, results: operations });
}
function normaliseUnifiedOutboxPayload(value) {
  const unwrapped = rpcValue(value);
  if (unwrapped && typeof unwrapped === 'object' && !Array.isArray(unwrapped)) {
    const items = Array.isArray(unwrapped.items)
      ? unwrapped.items
      : (Array.isArray(unwrapped.rows) ? unwrapped.rows : []);
    const total = Number(
      unwrapped.total_count
      ?? unwrapped.total
      ?? unwrapped.count
      ?? items.length
    );
    return {
      ...unwrapped,
      items,
      total_count: Number.isFinite(total) ? total : items.length
    };
  }
  const items = Array.isArray(unwrapped) ? unwrapped : [];
  return { items, total_count: items.length };
}

function invoiceOperationOutboxRow(row) {
  const progress = row.progress_json && typeof row.progress_json === 'object' ? row.progress_json : {};
  const result = row.result_json && typeof row.result_json === 'object' ? row.result_json : {};
  return { channel: 'INVOICE', outbox_id: row.id, id: row.id, outbox_type: row.operation_type, type: row.operation_type, entity_type: row.entity_type || null, entity_id: row.entity_id || null, status: row.status, queue_state: row.status, phase: row.phase, legal_issue_state: result.legal_issue_state || progress.legal_issue_state || 'NOT_REQUESTED', delivery_state: result.delivery_state || progress.delivery_state || 'NOT_REQUESTED', requires_user_action: row.requires_user_action === true, progress_summary: { completed_units: Number(row.completed_units || 0), total_units: Number(row.total_units || 0), failed_units: Number(row.failed_units || 0), pages_complete: Number(progress.pages_complete || 0), pages_total: Number(progress.pages_total || 0), status_message: String(progress.status_message || '').slice(0, 200) || null }, retry_summary: { run_after_utc: row.run_after_utc || null, attempt_count: Number(progress.attempt_count || 0) }, error_code: String(row.error_json?.code || row.error_json?.error_code || '').slice(0, 120) || null, created_at_utc: row.created_at_utc, scheduled_for_utc: row.run_after_utc || null, effective_ready_at_utc: row.run_after_utc || row.created_at_utc, change_seq: Number(row.change_seq || 0), parent_operation_id: row.parent_operation_id || null };
}

function compareUnifiedOutboxRows(left, right, sortBy, sortDir) {
  const key = sortBy === 'channel'
    ? 'channel'
    : (sortBy === 'status' ? 'status' : sortBy);
  const a = left?.[key] ?? '';
  const b = right?.[key] ?? '';
  const numericA = /_at_utc$/.test(key) ? Date.parse(a) : NaN;
  const numericB = /_at_utc$/.test(key) ? Date.parse(b) : NaN;
  const compared = Number.isFinite(numericA) && Number.isFinite(numericB)
    ? numericA - numericB
    : String(a).localeCompare(String(b));
  if (compared !== 0) return sortDir === 'asc' ? compared : -compared;
  return String(left?.outbox_id || left?.id || '').localeCompare(
    String(right?.outbox_id || right?.id || '')
  ) * (sortDir === 'asc' ? 1 : -1);
}

async function handleInvoiceOutboxList(env, req, deps) {
  const url = new URL(req.url);
  const limit = Math.max(1, Math.min(500, Math.trunc(Number(url.searchParams.get('limit')) || 50)));
  const offset = Math.max(0, Math.trunc(Number(url.searchParams.get('offset')) || 0));
  const status = String(url.searchParams.get('queue_state') || url.searchParams.get('status') || '').trim().toUpperCase();
  const channel = String(url.searchParams.get('channel') || '').trim().toUpperCase();
  const search = String(url.searchParams.get('search') || '').trim().toLowerCase();
  const sortBy = String(url.searchParams.get('sort_by') || 'created_at_utc').trim();
  const sortDir = String(url.searchParams.get('sort_dir') || 'desc').trim().toLowerCase();
  if (!new Set(['created_at_utc','scheduled_for_utc','effective_ready_at_utc','status','channel']).has(sortBy) || !['asc','desc'].includes(sortDir)) return jsonResponse({ error: 'INVALID_OUTBOX_SORT' }, 400);
  const requestedRows = Math.min(1000, offset + limit);
  const query = new URL(`${env.SUPABASE_URL}/rest/v1/invoice_operations`);
  query.searchParams.set('select', 'id,operation_type,entity_type,entity_id,status,phase,priority,total_units,completed_units,failed_units,progress_json,result_json,error_json,requires_user_action,change_seq,created_at_utc,updated_at_utc,run_after_utc,parent_operation_id');
  if (status && /^[A-Z_]+$/.test(status)) query.searchParams.set('status', `eq.${status}`);
  const operationType = String(url.searchParams.get('operation_type') || '').trim().toUpperCase();
  if (operationType && /^[A-Z_]+$/.test(operationType)) query.searchParams.set('operation_type', `eq.${operationType}`);
  const entityId = String(url.searchParams.get('entity_id') || '').trim().toLowerCase();
  if (entityId) {
    if (!UUID_PATTERN.test(entityId)) return jsonResponse({ error: 'INVALID_ENTITY_ID' }, 400);
    query.searchParams.set('entity_id', `eq.${entityId}`);
  }
  const requiresAction = url.searchParams.get('requires_user_action');
  if (requiresAction === 'true' || requiresAction === 'false') query.searchParams.set('requires_user_action', `eq.${requiresAction}`);
  if (search) {
    if (UUID_PATTERN.test(search)) query.searchParams.set('or', `(id.eq.${search},entity_id.eq.${search})`);
    else if (/^[a-z0-9 _-]{1,80}$/i.test(search)) {
      const term = search.replace(/ +/g, '%');
      query.searchParams.set('or', `(operation_type.ilike.*${term}*,phase.ilike.*${term}*)`);
    } else return jsonResponse({ error: 'INVALID_OUTBOX_SEARCH' }, 400);
  }
  query.searchParams.set('order', 'created_at_utc.desc,id.desc');
  query.searchParams.set('limit', String(channel === 'INVOICE' ? limit : requestedRows));
  query.searchParams.set('offset', String(channel === 'INVOICE' ? offset : 0));
  const invoicePromise = fetch(query, { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`, prefer: 'count=exact' } });
  const legacyPromise = channel === 'INVOICE' ? Promise.resolve(null) : deps.rpc('outbox_unified_list', { p_status: status || null, p_channel: channel || null, p_search: search || null, p_queue_state: status || null, p_limit: requestedRows, p_offset: 0, p_sort_by: sortBy, p_sort_dir: sortDir });
  const [response, legacyRaw] = await Promise.all([invoicePromise, legacyPromise]);
  const invoiceRows = await response.json().catch(() => []);
  if (!response.ok || !Array.isArray(invoiceRows)) return jsonResponse({ error: 'INVOICE_OPERATION_LIST_FAILED' }, 502);
  const invoiceItems = invoiceRows.map(invoiceOperationOutboxRow);
  const invoiceTotalRaw = Number((response.headers.get('content-range') || '').split('/')[1]);
  const invoiceTotal = Number.isFinite(invoiceTotalRaw) ? invoiceTotalRaw : invoiceItems.length;
  const legacy = normaliseUnifiedOutboxPayload(legacyRaw);
  const combined = channel === 'INVOICE' ? invoiceItems : [...legacy.items, ...invoiceItems].sort((left, right) => compareUnifiedOutboxRows(left, right, sortBy, sortDir));
  const items = channel === 'INVOICE' ? invoiceItems : combined.slice(offset, offset + limit);
  return jsonResponse({ ok: true, channel: channel || null, total_count: channel === 'INVOICE' ? invoiceTotal : Number(legacy.total_count || 0) + invoiceTotal, limit, offset, returned_count: items.length, items, merge_window: channel === 'INVOICE' ? null : requestedRows, merge_truncated: channel === 'INVOICE' ? false : combined.length >= requestedRows * 2 });
}
async function handleInvoiceOutboxControl(env, req, ctx, user, deps, operationId, action) {
  const body = req.method === 'POST' ? (await parseBody(req) || {}) : {};
  const controlAction = {
    operation_id: canonicalUuidArray([operationId])[0],
    action,
    ...(action === 'RESCHEDULE' ? {
      run_after_utc: body.scheduled_for_utc || body.scheduledForUtc
    } : {}),
    ...(action === 'RETRY' && body.retry_chunk_id ? {
      retry_chunk_id: body.retry_chunk_id
    } : {})
  };
  const result = await deps.rpc('invoice_operation_control_batch', {
    p_actions: [controlAction],
    p_actor_user_id: user.id,
    p_now_utc: new Date().toISOString()
  });
  const operations = rpcValue(result);
  await nudgeInvoiceOperations(env, operations, { ctx, rpc: deps.rpc, lanes: ['ALL'] });
  return jsonResponse({ ok: true, results: operations });
}

async function recordInvoiceDocumentAccessAudit(env, req, claims, outcome) {
  try {
    const nonceBytes = new TextEncoder().encode(String(claims?.nonce || ''));
    const nonceDigest = await crypto.subtle.digest('SHA-256', nonceBytes);
    const nonceHash = Array.from(new Uint8Array(nonceDigest))
      .map(value => value.toString(16).padStart(2, '0')).join('');
    const response = await fetch(`${env.SUPABASE_URL}/rest/v1/audit_events`, {
      method: 'POST',
      headers: {
        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
        authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
        'content-type': 'application/json',
        Prefer: 'return=minimal'
      },
      body: JSON.stringify({
        object_type: 'invoice_document_versions',
        object_id_text: String(claims?.document_version_id || ''),
        action: 'INVOICE_DOCUMENT_TOKEN_ACCESS',
        before_json: null,
        after_json: {
          invoice_id: claims?.entity_id || null,
          access_purpose: claims?.purpose || null,
          nonce_hash: nonceHash || null,
          outcome: String(outcome || 'UNKNOWN').slice(0, 64)
        },
        reason: 'Secure immutable invoice-document access',
        actor_user_id: UUID_PATTERN.test(String(claims?.sub || '')) ? claims.sub : null,
        actor_display: 'Secure invoice document recipient',
        actor_role_at_time: 'document_recipient',
        ip: req.headers.get('cf-connecting-ip') || null,
        user_agent: req.headers.get('user-agent') || null,
        correlation_id: String(claims?.document_version_id || '') || null
      })
    });
    if (!response.ok) console.warn('[INVOICE_DOCUMENT_ACCESS_AUDIT_FAILED]', response.status);
  } catch (error) {
    console.warn('[INVOICE_DOCUMENT_ACCESS_AUDIT_ERROR]', String(error?.message || error));
  }
}

async function handleDocumentAccess(env, req) {
  const token = new URL(req.url).searchParams.get('token');
  const verification = await verifyInvoiceDocumentAccessToken(
    env.INVOICE_DOCUMENT_ACCESS_SECRET,
    token,
    { expectedPurpose: 'DOWNLOAD' }
  );
  if (!verification.ok) return jsonResponse({ error: verification.code }, 401);
  if (!env.R2) return jsonResponse({ error: 'INVOICE_R2_BINDING_MISSING' }, 503);
  const claims = verification.claims;
  if (
    String(claims.entity_type || '').toUpperCase() !== 'INVOICE'
    || !UUID_PATTERN.test(String(claims.entity_id || ''))
    || !UUID_PATTERN.test(String(claims.document_version_id || ''))
  ) {
    return jsonResponse({ error: 'INVOICE_DOCUMENT_TOKEN_ENTITY_INVALID' }, 401);
  }
  const serviceHeaders = {
    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
  };
  const versionUrl = new URL(`${env.SUPABASE_URL}/rest/v1/invoice_document_versions`);
  versionUrl.searchParams.set('id', `eq.${claims.document_version_id}`);
  versionUrl.searchParams.set('entity_type', 'eq.INVOICE');
  versionUrl.searchParams.set('entity_id', `eq.${claims.entity_id}`);
  versionUrl.searchParams.set('purpose', 'eq.FINAL_ISSUE');
  versionUrl.searchParams.set('status', 'eq.READY');
  versionUrl.searchParams.set('select', 'id,entity_id,r2_key,sha256,size_bytes,page_count');
  versionUrl.searchParams.set('limit', '1');
  const [versionResponse, invoiceResponse] = await Promise.all([
    fetch(versionUrl, { headers: serviceHeaders }),
    fetch(
      `${env.SUPABASE_URL}/rest/v1/invoices`
        + `?id=eq.${encodeURIComponent(claims.entity_id)}`
        + `&issued_document_version_id=eq.${encodeURIComponent(claims.document_version_id)}`
        + '&status=in.(ISSUED,PAID)&select=id,issued_document_version_id&limit=1',
      { headers: serviceHeaders }
    )
  ]);
  const [versions, invoices] = await Promise.all([
    versionResponse.json().catch(() => []),
    invoiceResponse.json().catch(() => [])
  ]);
  const version = Array.isArray(versions) ? versions[0] : null;
  if (
    !versionResponse.ok
    || !invoiceResponse.ok
    || !version?.r2_key
    || !Array.isArray(invoices)
    || !invoices[0]
  ) {
    return jsonResponse({ error: 'INVOICE_DOCUMENT_ACCESS_REVOKED' }, 404);
  }
  const object = await env.R2.get(version.r2_key);
  if (!object) {
    await recordInvoiceDocumentAccessAudit(env, req, claims, 'DOCUMENT_NOT_FOUND');
    return jsonResponse({ error: 'DOCUMENT_NOT_FOUND' }, 404);
  }
  const metadata = object.customMetadata || {};
  if (
    !metadata.sha256 || !metadata.size_bytes || !metadata.document_version_id
    || !metadata.chunk_id || !metadata.fence_token
    || metadata.sha256 !== version.sha256
    || Number(metadata.size_bytes) !== Number(version.size_bytes)
    || Number(version.size_bytes) !== Number(object.size)
    || metadata.document_version_id !== version.id
    || !UUID_PATTERN.test(String(metadata.chunk_id))
    || !Number.isSafeInteger(Number(metadata.fence_token))
  ) {
    await recordInvoiceDocumentAccessAudit(env, req, claims, 'STORAGE_IDENTITY_MISMATCH');
    return jsonResponse({ error: 'INVOICE_DOCUMENT_STORAGE_IDENTITY_MISMATCH' }, 409);
  }
  await recordInvoiceDocumentAccessAudit(env, req, claims, 'ALLOWED');
  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set('content-type', headers.get('content-type') || 'application/pdf');
  headers.set('content-disposition', `attachment; filename="${String(
    verification.claims.filename || 'invoice.pdf'
  ).replace(/["\r\n]/g, '_')}"`);
  headers.set('cache-control', 'private, no-store');
  headers.set('x-content-type-options', 'nosniff');
  return new Response(object.body, { headers });
}

export async function createReadyInvoiceDocumentLink(env, descriptor, actorUserId) {
  if (!env.INVOICE_DOCUMENT_ACCESS_SECRET || String(env.INVOICE_DOCUMENT_ACCESS_SECRET).length < 32) throw new Error('INVOICE_DOCUMENT_ACCESS_SECRET_MISSING');
  if (String(descriptor?.purpose || 'FINAL_ISSUE').toUpperCase() !== 'FINAL_ISSUE') throw new Error('INVOICE_SECURE_LINK_FINAL_ISSUE_REQUIRED');
  if (!UUID_PATTERN.test(String(descriptor?.entity_id || '')) || !UUID_PATTERN.test(String(descriptor?.document_version_id || ''))) throw new Error('INVOICE_SECURE_LINK_IDENTITY_INVALID');
  const token = await createInvoiceDocumentAccessToken(
    env.INVOICE_DOCUMENT_ACCESS_SECRET,
    {
      sub: actorUserId,
      entity_type: descriptor.entity_type,
      entity_id: descriptor.entity_id,
      document_version_id: descriptor.document_version_id,
      recipient_set_hash: descriptor.recipient_set_hash || undefined,
      purpose: 'DOWNLOAD',
      filename: descriptor.filename || 'invoice.pdf'
    },
    { ttlSeconds: Number(env.INVOICE_DOCUMENT_ACCESS_TTL_SECONDS || 300) }
  );
  const configuredBase = env.INVOICE_DOCUMENT_PUBLIC_BASE_URL
    || env.PUBLIC_APP_URL
    || env.PUBLIC_DOWNLOAD_BASE_URL
    || 'https://testmode.arthur-rai.co.uk';
  return buildInvoiceDocumentDownloadUrl(new URL(configuredBase).origin, token);
}

function match(pathname, pattern) {
  const actual = pathname.split('/').filter(Boolean);
  const expected = pattern.split('/').filter(Boolean);
  if (actual.length !== expected.length) return null;
  const params = {};
  for (let index = 0; index < expected.length; index += 1) {
    if (expected[index].startsWith(':')) params[expected[index].slice(1)] = decodeURIComponent(actual[index]);
    else if (actual[index] !== expected[index]) return null;
  }
  return params;
}

function invoiceErrorStatus(error) {
  const code = String(error?.code || error?.message || error || '').toUpperCase();
  if (/INVALID|MALFORMED|REQUIRED|UUID|JSON/.test(code)) return 400;
  if (/UNAUTHENTICATED|SESSION/.test(code)) return 401;
  if (/FORBIDDEN|ADMIN_REQUIRED|PERMISSION/.test(code)) return 403;
  if (/CONFLICT|SOURCE_CHANGED|ACTIVE_OPERATION|NOT_READY|BLOCKED|TERMINAL/.test(code)) return 409;
  if (/BACKPRESSURE|RATE_LIMIT/.test(code)) return 429;
  if (/UNAVAILABLE|BINDING_MISSING|CONFIGURATION/.test(code)) return 503;
  return 500;
}
function isInvoiceAsyncRoute(req, url) {
  const path = url.pathname;
  const method = req.method;
  if (method === 'GET' && path === '/api/invoice-documents/access') return true;
  if (method === 'GET' && [
    '/api/invoices/batch-generate/candidates',
    '/api/invoices/batch-issue/candidates',
    '/api/nhsp/invoices/candidates'
  ].includes(path)) return true;
  if (method === 'POST' && [
    '/api/invoices/batch-generate/confirm',
    '/api/invoices/batch-issue/confirm',
    '/api/invoice-operations/get',
    '/api/invoice-operations/control',
    '/api/invoices',
    '/api/invoices/tsfin/by-week',
    '/api/invoices/create-expenses',
    '/api/nhsp/invoices/run'
  ].includes(path)) return true;
  if (method === 'GET' && path === '/api/outbox') {
    const channel = String(url.searchParams.get('channel') || '').trim().toUpperCase();
    return !channel || channel === 'INVOICE';
  }
  if (method === 'GET' && match(path, '/api/invoice-operations/:operation_id')) return true;
  const outbox = match(path, '/api/outbox/:channel/:operation_id');
  if (outbox && String(outbox.channel).toUpperCase() === 'INVOICE' && ['GET', 'DELETE'].includes(method)) return true;
  const retry = match(path, '/api/outbox/:channel/:operation_id/retry');
  if (retry && String(retry.channel).toUpperCase() === 'INVOICE' && method === 'POST') return true;
  const reschedule = match(path, '/api/outbox/:channel/:operation_id/reschedule');
  if (reschedule && String(reschedule.channel).toUpperCase() === 'INVOICE' && method === 'POST') return true;
  if (method === 'POST' && match(path, '/api/invoices/:invoice_id/render')) return true;
  if (['GET', 'POST'].includes(method) && match(path, '/api/timesheets/:timesheet_id/pdf')) return true;
  if (method === 'POST' && match(path, '/api/invoices/:invoice_id/issue')) return true;
  if (method === 'POST' && match(path, '/api/invoices/:invoice_id/email')) return true;
  if (method === 'POST' && match(path, '/api/invoices/:invoice_id/credit-note')) return true;
  if (method === 'GET' && match(path, '/api/invoices/:invoice_id')) return true;
  return false;
}

export async function handleInvoiceAsyncHttpRequest(req, env, ctx, deps) {
  const url = new URL(req.url);
  const path = url.pathname;
  if (req.method === 'GET' && path === '/api/invoice-documents/access') {
    return handleDocumentAccess(env, req);
  }
  if (!isInvoiceAsyncPipelineEnabled(env)) return null;
  if (!isInvoiceAsyncRoute(req, url)) return null;

  const user = await requireActor(env, req, deps, false);
  if (!user) return jsonResponse({ error: 'UNAUTHENTICATED' }, 401);
  const cohort = isInvoiceAsyncUserAllowed(env, { ...user, active: user.is_active ?? user.active, roles: [user.role, user.user_role, user.user_type] });
  if (!cohort.allowed) {
    if (cohort.code === 'INVOICE_ASYNC_USER_OUTSIDE_COHORT') return null;
    if (cohort.code === 'INVOICE_ASYNC_ALLOWLIST_INVALID') return jsonResponse({ error: cohort.code }, 503);
    return jsonResponse({ error: cohort.code }, 403);
  }

  try {
    if (req.method === 'GET' && path === '/api/invoices/batch-generate/candidates') {
      return handleCandidates(req, deps, 'invoice_batch_generate_candidates');
    }
    if (req.method === 'GET' && path === '/api/invoices/batch-issue/candidates') {
      return handleCandidates(req, deps, 'invoice_batch_issue_candidates');
    }
    if (req.method === 'GET' && path === '/api/nhsp/invoices/candidates') {
      return handleNhspCandidates(req, deps);
    }
    if (req.method === 'POST' && path === '/api/invoices/batch-generate/confirm') {
      return handleBatchGenerateConfirm(env, req, ctx, user, deps);
    }
    if (req.method === 'POST' && path === '/api/invoices/batch-issue/confirm') {
      return handleBatchIssueConfirm(env, req, ctx, user, deps);
    }
    if (req.method === 'POST' && path === '/api/invoice-operations/get') {
      return handleOperationGet(req, user, deps);
    }
    if (req.method === 'POST' && path === '/api/invoice-operations/control') {
      return handleOperationControl(env, req, ctx, user, deps);
    }
    if (req.method === 'GET' && path === '/api/outbox') {
      const channel = String(url.searchParams.get('channel') || '').trim().toUpperCase();
      if (!channel || channel === 'INVOICE') {
        return handleInvoiceOutboxList(env, req, deps);
      }
    }

    let params = match(path, '/api/invoice-operations/:operation_id');
    if (params && req.method === 'GET') {
      return handleOperationGet(req, user, deps, params.operation_id);
    }
    params = match(path, '/api/outbox/:channel/:operation_id');
    if (params && String(params.channel).toUpperCase() === 'INVOICE') {
      if (req.method === 'GET') {
        return handleOperationGet(req, user, deps, params.operation_id);
      }
      if (req.method === 'DELETE') {
        return handleInvoiceOutboxControl(
          env, req, ctx, user, deps, params.operation_id, 'CANCEL'
        );
      }
    }
    params = match(path, '/api/outbox/:channel/:operation_id/retry');
    if (
      params
      && req.method === 'POST'
      && String(params.channel).toUpperCase() === 'INVOICE'
    ) {
      return handleInvoiceOutboxControl(
        env, req, ctx, user, deps, params.operation_id, 'RETRY'
      );
    }
    params = match(path, '/api/outbox/:channel/:operation_id/reschedule');
    if (
      params
      && req.method === 'POST'
      && String(params.channel).toUpperCase() === 'INVOICE'
    ) {
      return handleInvoiceOutboxControl(
        env, req, ctx, user, deps, params.operation_id, 'RESCHEDULE'
      );
    }
    params = match(path, '/api/invoices/:invoice_id/render');
    if (params && req.method === 'POST') {
      return handleViewDocument(env, req, ctx, user, deps, 'INVOICE', params.invoice_id);
    }
    params = match(path, '/api/timesheets/:timesheet_id/pdf');
    if (params && ['GET', 'POST'].includes(req.method)) {
      return handleViewDocument(env, req, ctx, user, deps, 'TIMESHEET', params.timesheet_id);
    }
    params = match(path, '/api/invoices/:invoice_id/issue');
    if (params && req.method === 'POST') {
      return handleIssueOne(env, req, ctx, user, deps, params.invoice_id);
    }
    params = match(path, '/api/invoices/:invoice_id/email');
    if (params && req.method === 'POST') {
      return handleDeliverOne(env, req, ctx, user, deps, params.invoice_id);
    }
    params = match(path, '/api/invoices/:invoice_id/credit-note');
    if (params && req.method === 'POST') {
      const body = await parseBody(req) || {};
      return startCommands(env, req, ctx, user, [{
        command_type: 'GENERATE_CREDIT_NOTE',
        base_invoice_id: params.invoice_id,
        credit_reason: body.credit_reason || body.reason,
        command_token: commandToken(req, body)
      }], deps, ['DATABASE']);
    }
    params = match(path, '/api/invoices/:invoice_id');
    if (params && req.method === 'GET') {
      const detail = await deps.rpc('invoice_detail_get', {
        p_invoice_id: params.invoice_id,
        p_actor_user_id: user.id
      });
      return jsonResponse(rpcValue(detail));
    }

    if (req.method === 'POST' && path === '/api/invoices') {
      const body = await parseBody(req);
      if (!body) return jsonResponse({ error: 'Invalid JSON' }, 400);
      return startCommands(env, req, ctx, user, [
        generationCommandFromBody(req, body, 'GENERATE_SELECTED')
      ], deps, ['DATABASE']);
    }
    if (req.method === 'POST' && path === '/api/invoices/tsfin/by-week') {
      const body = await parseBody(req);
      if (!body) return jsonResponse({ error: 'Invalid JSON' }, 400);
      return startCommands(env, req, ctx, user, [
        generationCommandFromBody(req, body, 'GENERATE_BY_WEEK')
      ], deps, ['DATABASE']);
    }
    if (req.method === 'POST' && path === '/api/invoices/create-expenses') {
      const body = await parseBody(req);
      if (!body) return jsonResponse({ error: 'Invalid JSON' }, 400);
      return startCommands(env, req, ctx, user, [
        generationCommandFromBody(req, body, 'GENERATE_EXPENSES')
      ], deps, ['DATABASE']);
    }
    if (req.method === 'POST' && path === '/api/nhsp/invoices/run') {
      const body = await parseBody(req);
      if (!body) return jsonResponse({ error: 'Invalid JSON' }, 400);
      const sourceIds = body.nhsp_shift_ids || body.source_ids || body.timesheet_ids;
      return startCommands(env, req, ctx, user, [{
        ...generationCommandFromBody(req, { ...body, source_ids: sourceIds }, 'GENERATE_NHSP'),
        nhsp_shift_ids: body.nhsp_shift_ids
          ? canonicalUuidArray(body.nhsp_shift_ids)
          : undefined
      }], deps, ['DATABASE']);
    }
    return null;
  } catch (error) {
    const code = String(error?.code || error?.message || error || 'INVOICE_PIPELINE_UNEXPECTED').slice(0, 160);
    return jsonResponse({ error: code }, invoiceErrorStatus(error));
  }
}

export const invoiceAsyncHttpInternals = Object.freeze({
  rpcValue,
  canonicalUuidArray,
  canonicalEmailArray,
  boolValue,
  generationCommandFromBody,
  match
});
