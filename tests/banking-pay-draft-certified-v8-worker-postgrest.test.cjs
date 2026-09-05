const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const broker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const postgrestOrigin = String(process.env.H2_V8_LOCAL_POSTGREST_URL || '').replace(/\/$/, '');
const targetCount = Number(process.env.H2_V8_LOCAL_TARGET || 0);
const enabled = /^https?:\/\//.test(postgrestOrigin) && [101, 1001, 5000].includes(targetCount);

const SESSION_ID = '20000000-0000-4000-8000-000000000001';
const ACTOR_ID = '10000000-0000-4000-8000-000000000001';

function unwrap(value) {
  let current = value;
  if (Array.isArray(current) && current.length === 1) current = current[0];
  return current;
}

async function requestJson(url, init) {
  const response = await fetch(url, init);
  const text = await response.text();
  if (!response.ok) {
    throw Object.assign(new Error(`LOCAL_POSTGREST_FAILED:${response.status}:${text.slice(0, 500)}`), {
      status: response.status
    });
  }
  return text ? JSON.parse(text) : null;
}

test('the exact Draft Worker stages 101/1,001/5,000 through local PostgREST without expanded arrays', {
  skip: !enabled,
  timeout: 15 * 60 * 1000
}, async () => {
  const functionStart = broker.indexOf('async function advanceBankingPayDraftCreateOperation');
  const functionEnd = broker.indexOf('async function handleBankingPaySnoozeValidate', functionStart);
  assert.ok(functionStart >= 0 && functionEnd > functionStart);

  const certifiedApi = await import(new URL('../broker/src/banking-pay-draft-certified-v8.js', `file://${__filename}`).href);
  const insertItemsApi = await import(new URL('../broker/src/banking-pay-draft-insert-items.js', `file://${__filename}`).href);
  const headroomApi = await import(new URL('../broker/src/banking-pay-create-draft-headroom.js', `file://${__filename}`).href);

  const rpcCalls = [];
  const tableReads = [];
  const sbRpc = async (_env, name, args) => {
    const startedAt = Date.now();
    const payload = await requestJson(`${postgrestOrigin}/rpc/${encodeURIComponent(name)}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify(args || {})
    });
    rpcCalls.push({ name, elapsed_ms: Date.now() - startedAt, payload: unwrap(payload) });
    return payload;
  };
  const sbFetch = async (_env, address) => {
    const source = new URL(address);
    const relativePath = source.pathname.replace(/^\/rest\/v1/, '');
    const url = `${postgrestOrigin}${relativePath}${source.search}`;
    const startedAt = Date.now();
    const rows = await requestJson(url, { headers: { accept: 'application/json' } });
    tableReads.push({ path: relativePath, elapsed_ms: Date.now() - startedAt, row_count: Array.isArray(rows) ? rows.length : 0 });
    return { rows: Array.isArray(rows) ? rows : [] };
  };

  const buildBankingPayOperationPublicPayload = (row) => JSON.parse(JSON.stringify(row || {}));
  const context = vm.createContext({
    assertBankingPayWorkbenchContract: async () => true,
    buildBankingPayOperationPublicPayload,
    buildCertifiedDraftTerminalResultV8: certifiedApi.buildCertifiedDraftTerminalResultV8,
    console,
    encodeURIComponent,
    evaluateCreateDraftRecoveryHeadroom: headroomApi.evaluateCreateDraftRecoveryHeadroom,
    isCertifiedDeferredFinanceOnlyInsertItemsResult: insertItemsApi.isCertifiedDeferredFinanceOnlyInsertItemsResult,
    isCertifiedEmptyTimesheetSnapshotsResult: insertItemsApi.isCertifiedEmptyTimesheetSnapshotsResult,
    Map,
    revolutEnsurePayeesReadyFromPreview: async () => ({ ok: true, remaining: 0, failed: 0, next_cursor: null }),
    sbFetch,
    sbRpc,
    scheduleBankingPayWorkbenchDrainWithDurableWake: async () => ({ ok: true, scheduled: false, skipped: true }),
    Set,
    structuredClone,
    tsfinBestEffortMakeReadyForDraft: async () => ({ ok: true, remaining: 0, made_ready: 0, failed: 0, still_pending: 0, next_cursor: null }),
    validateCertifiedDraftOperationProjectionV8: certifiedApi.validateCertifiedDraftOperationProjectionV8,
    validateCertifiedDraftTerminalContextV8: certifiedApi.validateCertifiedDraftTerminalContextV8,
    WeakSet
  });
  vm.runInContext(`${broker.slice(functionStart, functionEnd)}\nglobalThis.advanceDraft=advanceBankingPayDraftCreateOperation;`, context);

  const inactiveOverride = {
    continue: false,
    verified: false,
    used: false,
    pay_date: '2099-04-03',
    pay_week_start: '2099-03-30',
    pay_week_end: '2099-04-05',
    reason: null,
    verified_by_user_id: null,
    verified_at_utc: null,
    reauth_purpose: null,
    guardrail_code: null
  };
  const idempotencyKey = `h2-v8-worker-postgrest-${targetCount}`;
  const envelope = unwrap(await sbRpc({}, 'pay_workbench_settled_certificate_current_reference_issue_v8', {
    p_workbench_session_id: SESSION_ID,
    p_session_version: 1,
    p_progress_counter_version: 1,
    p_pay_channel_scope: 'ALL',
    p_idempotency_key: idempotencyKey,
    p_same_week_paye_override: inactiveOverride
  }));
  assert.equal(certifiedApi.validateCurrentCertificateIssuerEnvelopeV8(envelope).ok, true, JSON.stringify(envelope));
  const started = unwrap(await sbRpc({}, 'banking_pay_draft_certified_operation_start_v8', {
    p_certificate_reference: envelope.certificate_reference,
    p_actor_user_id: ACTOR_ID,
    p_idempotency_key: idempotencyKey
  }));
  assert.equal(started.ok, true, JSON.stringify(started));
  const operationId = started.operation_id;
  assert.match(operationId, /^[0-9a-f-]{36}$/i);

  const fetchOperation = async () => {
    const { rows } = await sbFetch({}, `${postgrestOrigin}/rest/v1/banking_pay_operations?id=eq.${operationId}&select=*`);
    assert.equal(rows.length, 1);
    return rows[0];
  };

  const startedAt = Date.now();
  let operation = await fetchOperation();
  const phaseHistory = [];
  let workerCalls = 0;
  let maxWorkerCallMs = 0;
  while (String(operation.phase || '').toUpperCase() !== 'DRAIN_TSFIN') {
    const beforePhase = String(operation.phase || '').toUpperCase();
    const callStartedAt = Date.now();
    const result = await context.advanceDraft(
      { SUPABASE_URL: postgrestOrigin },
      operation,
      { id: ACTOR_ID },
      {
        singleStep: true,
        maxPhaseUnits: 1,
        maxChunksPerCall: 1,
        requestBudgetMs: 15000,
        lockOwner: `H2_V8_WORKER_POSTGREST:${targetCount}`
      }
    );
    const elapsedMs = Date.now() - callStartedAt;
    maxWorkerCallMs = Math.max(maxWorkerCallMs, elapsedMs);
    workerCalls += 1;
    assert.ok(workerCalls <= 256, 'bounded Worker staging did not terminate');
    assert.notEqual(String(result.status || '').toUpperCase(), 'FAILED', JSON.stringify(result));
    operation = await fetchOperation();
    phaseHistory.push({ before: beforePhase, after: String(operation.phase || '').toUpperCase(), elapsed_ms: elapsedMs });
  }

  const replay = unwrap(await sbRpc({}, 'banking_pay_draft_certificate_stage_advance_v8', {
    p_operation_id: operationId,
    p_worker_id: `H2_V8_WORKER_POSTGREST:${targetCount}`
  }));
  assert.equal(replay.ok, true, JSON.stringify(replay));
  assert.equal(replay.replayed, true, JSON.stringify(replay));
  assert.equal(replay.work_kind, 'CERTIFICATE_STAGE_ALREADY_COMPLETE', JSON.stringify(replay));

  const scopeRows = await requestJson(
    `${postgrestOrigin}/banking_pay_operation_candidate_scope?operation_id=eq.${operationId}&select=candidate_id,pay_channel,selected_preview_row_ids_json,selected_timesheet_ids_json,selected_finance_case_ids_json,selected_canonical_preview_lines_json,effective_canonical_preview_lines_json`,
    { headers: { accept: 'application/json' } }
  );
  assert.equal(scopeRows.length, Math.min(targetCount, 4));
  for (const row of scopeRows) {
    for (const key of [
      'selected_preview_row_ids_json',
      'selected_timesheet_ids_json',
      'selected_finance_case_ids_json',
      'selected_canonical_preview_lines_json',
      'effective_canonical_preview_lines_json'
    ]) assert.deepEqual(row[key], [], `${key} must remain compact`);
  }

  assert.ok(rpcCalls.every((call) => call.elapsed_ms < 15000), JSON.stringify(rpcCalls.filter((call) => call.elapsed_ms >= 15000)));
  assert.ok(maxWorkerCallMs < 15000, `Worker staging call exceeded the unchanged budget: ${maxWorkerCallMs}ms`);
  assert.equal(phaseHistory[0].before, 'INITIALISE');
  assert.ok(phaseHistory.every((entry) => [
    'INITIALISE',
    'CERTIFICATE_CONSTITUENT_REFS',
    'CERTIFICATE_PARTITION_REFS',
    'CANDIDATE_SCOPE',
    'CERTIFICATE_FINAL_FREEZE'
  ].includes(entry.before)), JSON.stringify(phaseHistory));
  assert.ok(phaseHistory.some((entry) => entry.before === 'CERTIFICATE_PARTITION_REFS'));
  assert.equal(phaseHistory.at(-1).after, 'DRAIN_TSFIN');

  console.log('H2_V8_WORKER_POSTGREST_RESULT', JSON.stringify({
    target_count: targetCount,
    operation_id: operationId,
    worker_calls: workerCalls,
    rpc_calls: rpcCalls.length,
    table_reads: tableReads.length,
    max_worker_call_ms: maxWorkerCallMs,
    max_rpc_ms: Math.max(...rpcCalls.map((call) => call.elapsed_ms)),
    elapsed_ms: Date.now() - startedAt,
    final_phase: operation.phase,
    response_loss_replay: true,
    expanded_candidate_scope_arrays: false
  }));
});
