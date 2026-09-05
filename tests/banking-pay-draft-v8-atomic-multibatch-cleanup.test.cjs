const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const broker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const MODULE_URL = new URL('../broker/src/banking-pay-draft-certified-v8.js', `file://${__filename}`);

const OPERATION_ID = 'a8460000-0000-4000-8000-000000000001';
const SESSION_ID = 'a8460000-0000-4000-8000-000000000002';
const ACTOR_ID = 'a8460000-0000-4000-8000-000000000003';
const PAYE_BATCH_ID = 'a8460000-0000-4000-8000-000000000004';
const UMBRELLA_BATCH_ID = 'a8460000-0000-4000-8000-000000000005';
const PAYE_SCOPE_ID = 'a8460000-0000-4000-8000-000000000006';
const UMBRELLA_SCOPE_ID = 'a8460000-0000-4000-8000-000000000007';
const PAYE_CANDIDATE_ROW_ID = 'a8460000-0000-4000-8000-000000000008';
const UMBRELLA_CANDIDATE_ROW_ID = 'a8460000-0000-4000-8000-000000000009';

const sha = character => character.repeat(64);

test('a current V8 failure after PAYE and Umbrella shells aborts both partial Drafts before terminal failure', async () => {
  const certifiedApi = await import(MODULE_URL.href);
  const insertItemsApi = await import(new URL('../broker/src/banking-pay-draft-insert-items.js', `file://${__filename}`).href);
  const headroomApi = await import(new URL('../broker/src/banking-pay-create-draft-headroom.js', `file://${__filename}`).href);
  const functionStart = broker.indexOf('async function advanceBankingPayDraftCreateOperation');
  const functionEnd = broker.indexOf('async function handleBankingPaySnoozeValidate', functionStart);
  assert.ok(functionStart >= 0 && functionEnd > functionStart);

  const override = {
    continue: false,
    guardrail_code: null,
    pay_date: '2026-09-04',
    pay_week_end: '2026-09-06',
    pay_week_start: '2026-08-31',
    reason: null,
    reauth_purpose: null,
    used: false,
    verified: false,
    verified_at_utc: null,
    verified_by_user_id: null
  };
  const reference = {
    candidate_filter_id: null,
    certification_id: `WORKBENCH_SETTLED_CERTIFICATION_V2:${sha('a')}`,
    client_filter_id: null,
    filter_context_digest_sha256: sha('b'),
    idempotency_key: 'h2-v8-atomic-multibatch-cleanup',
    manifest_digest_sha256: sha('c'),
    overall_digest_sha256: sha('a'),
    pay_channel_scope: 'ALL',
    same_week_paye_override: override
  };
  const input = {
    workbench_settled_certificate_reference_v8: reference,
    pay_channel_scope: 'ALL',
    draft_scope: 'ALL',
    rail_provider_snapshot: 'REVOLUT',
    rail_env_snapshot: 'PROD',
    same_week_paye_override: override
  };
  assert.equal(certifiedApi.validateCertifiedDraftOperationProjectionV8(input).ok, true);

  const scopes = [
    { id: PAYE_SCOPE_ID, pay_batch_id: PAYE_BATCH_ID, pay_channel: 'PAYE' },
    { id: UMBRELLA_SCOPE_ID, pay_batch_id: UMBRELLA_BATCH_ID, pay_channel: 'UMBRELLA' }
  ];
  const batchRows = [
    { id: PAYE_BATCH_ID, status: 'DRAFT', execution_commit_state: 'NOT_SUBMITTED', source_workbench_session_id: SESSION_ID },
    { id: UMBRELLA_BATCH_ID, status: 'DRAFT', execution_commit_state: 'NOT_SUBMITTED', source_workbench_session_id: SESSION_ID }
  ];
  const rpcCalls = [];
  const fetchCalls = [];
  const abortCalls = [];

  const sbRpc = async (_env, name, args) => {
    rpcCalls.push({ name, args: JSON.parse(JSON.stringify(args || {})) });
    if (name === 'banking_pay_draft_advance_bounded_v8') {
      return {
        handled: true,
        ok: false,
        work_kind: 'TERMINAL_FAILURE_REQUIRED',
        code: 'H2_SYNTHETIC_SECOND_CHANNEL_FAILURE',
        message: 'Synthetic failure after both channel shells exist.'
      };
    }
    if (name === 'pay_batch_abort_failed_draft_create_partial') {
      abortCalls.push(args.p_pay_batch_id);
      return {
        ok: true,
        cleanup_mode: 'FAILED_DRAFT_CREATE_PARTIAL_ABORT',
        status_after: 'CANCELLED',
        draft_creation_failed_partial: true,
        batch_action_blocked: true
      };
    }
    if (name === 'banking_pay_batch_signal_touch') return { ok: true, version: rpcCalls.length };
    if (name === 'pay_batch_display_summary_refresh') return { ok: true };
    if (name === 'banking_pay_operation_finish') {
      return {
        operation_id: OPERATION_ID,
        operation_type: 'DRAFT_CREATE',
        status: args.p_status,
        phase: 'INSERT_ITEMS',
        workbench_session_id: SESSION_ID,
        error_json: args.p_error_json
      };
    }
    throw new Error(`UNEXPECTED_RPC:${name}`);
  };

  const sbFetch = async (_env, address) => {
    const url = new URL(address);
    fetchCalls.push(`${url.pathname}${url.search}`);
    if (url.pathname.endsWith('/banking_pay_operation_candidate_scope')) return { rows: scopes };
    if (url.pathname.endsWith('/banking_pay_operation_chunks')) return { rows: [] };
    if (url.pathname.endsWith('/pay_batches')) {
      const idFilter = url.searchParams.get('id') || '';
      if (idFilter.startsWith('in.')) return { rows: batchRows };
      const id = idFilter.replace(/^eq\./, '');
      return { rows: batchRows.filter(row => row.id === id) };
    }
    if (url.pathname.endsWith('/pay_batch_candidates')) {
      const batchId = (url.searchParams.get('pay_batch_id') || '').replace(/^eq\./, '');
      return { rows: [{ id: batchId === PAYE_BATCH_ID ? PAYE_CANDIDATE_ROW_ID : UMBRELLA_CANDIDATE_ROW_ID }] };
    }
    if (url.pathname.endsWith('/pay_batch_items')) return { rows: [{ id: 'a8460000-0000-4000-8000-00000000000a' }] };
    if (url.pathname.endsWith('/pay_bank_transfers')) return { rows: [] };
    throw new Error(`UNEXPECTED_FETCH:${url.pathname}${url.search}`);
  };

  const context = vm.createContext({
    assertBankingPayWorkbenchContract: async () => true,
    buildBankingPayOperationPublicPayload: row => JSON.parse(JSON.stringify(row || {})),
    buildCertifiedDraftTerminalResultV8: certifiedApi.buildCertifiedDraftTerminalResultV8,
    console,
    encodeURIComponent,
    evaluateCreateDraftRecoveryHeadroom: headroomApi.evaluateCreateDraftRecoveryHeadroom,
    isCertifiedDeferredFinanceOnlyInsertItemsResult: insertItemsApi.isCertifiedDeferredFinanceOnlyInsertItemsResult,
    isCertifiedEmptyTimesheetSnapshotsResult: insertItemsApi.isCertifiedEmptyTimesheetSnapshotsResult,
    Map,
    revolutEnsurePayeesReadyFromPreview: async () => ({ ok: true, remaining: 0, failed: 0 }),
    sbFetch,
    sbRpc,
    scheduleBankingPayWorkbenchDrainWithDurableWake: async () => ({ ok: true, skipped: true }),
    Set,
    structuredClone,
    tsfinBestEffortMakeReadyForDraft: async () => ({ ok: true, remaining: 0, failed: 0 }),
    validateCertifiedDraftOperationProjectionV8: certifiedApi.validateCertifiedDraftOperationProjectionV8,
    validateCertifiedDraftTerminalContextV8: certifiedApi.validateCertifiedDraftTerminalContextV8,
    WeakSet
  });
  vm.runInContext(`${broker.slice(functionStart, functionEnd)}\nglobalThis.advanceDraft=advanceBankingPayDraftCreateOperation;`, context);

  const result = await context.advanceDraft(
    { SUPABASE_URL: 'https://h2-v8-cleanup.invalid' },
    {
      operation_id: OPERATION_ID,
      operation_type: 'DRAFT_CREATE',
      status: 'RUNNING',
      phase: 'INSERT_ITEMS',
      actor_user_id: ACTOR_ID,
      workbench_session_id: SESSION_ID,
      input_json: input,
      config_json: {},
      progress_json: {
        draft_v8_certificate_partition_receipt_sha256: sha('d'),
        created_pay_batch_ids: [PAYE_BATCH_ID, UMBRELLA_BATCH_ID],
        batch_shells: [
          { pay_batch_id: PAYE_BATCH_ID, pay_channel: 'PAYE' },
          { pay_batch_id: UMBRELLA_BATCH_ID, pay_channel: 'UMBRELLA' }
        ]
      }
    },
    { id: ACTOR_ID },
    { singleStep: true, maxPhaseUnits: 1, maxChunksPerCall: 1, requestBudgetMs: 15000, lockOwner: 'H2_V8_ATOMIC_MULTIBATCH' }
  );

  assert.equal(result.status, 'FAILED');
  assert.equal(result.error_json.cleanup_status, 'COMPLETE');
  assert.equal(result.error_json.draft_creation_failed_partial, true);
  assert.equal(result.error_json.batch_action_blocked, true);
  assert.deepEqual([...abortCalls].sort(), [PAYE_BATCH_ID, UMBRELLA_BATCH_ID].sort());
  assert.equal(new Set(abortCalls).size, 2);
  assert.equal(rpcCalls.filter(call => call.name === 'pay_batch_abort_failed_draft_create_partial').length, 2);
  assert.equal(rpcCalls.filter(call => call.name === 'banking_pay_operation_finish').length, 1);
  assert.ok(fetchCalls.some(value => value.includes('/pay_batch_items?')));
  assert.ok(fetchCalls.some(value => value.includes('/pay_bank_transfers?')));
  assert.equal(rpcCalls.some(call => /provider|settlement|remittance|execute_payment|bank_transfer/i.test(call.name)), false);
});
