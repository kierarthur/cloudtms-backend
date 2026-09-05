const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const MODULE_URL = new URL('../broker/src/banking-pay-draft-certified-v8.js', `file://${__filename}`);
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');

const IDS = Object.freeze({
  operation: '10000000-0000-4000-8000-000000000001',
  session: '20000000-0000-4000-8000-000000000002',
  snapshot: '30000000-0000-4000-8000-000000000003',
  payeBatch: '40000000-0000-4000-8000-000000000004',
  umbrellaBatch: '50000000-0000-4000-8000-000000000005',
  candidateA: '60000000-0000-4000-8000-000000000006',
  candidateB: '70000000-0000-4000-8000-000000000007',
  rowA: '80000000-0000-4000-8000-000000000008',
  rowB: '90000000-0000-4000-8000-000000000009',
  replacement: 'a0000000-0000-4000-8000-00000000000a',
  replacementSnapshot: 'b0000000-0000-4000-8000-00000000000b'
});

function context(replacement = false) {
  return {
    contract: 'BANKING_PAY_DRAFT_TERMINAL_CONTEXT_V8',
    ok: true,
    operation_id: IDS.operation,
    workbench_session_id: IDS.session,
    source_session_id: IDS.session,
    source_session_version: 11,
    source_snapshot_run_id: IDS.snapshot,
    source_session_signature: 'signed-session-proof',
    pay_batch_ids: [IDS.payeBatch, IDS.umbrellaBatch],
    created_pay_batch_ids: [IDS.payeBatch, IDS.umbrellaBatch],
    skipped_empty_pay_batch_ids: [],
    cancelled_empty_pay_batch_ids: [],
    primary_pay_batch_id: IDS.payeBatch,
    pay_batch_id: IDS.payeBatch,
    created_batch_count: 2,
    created_batches: [
      { pay_batch_id: IDS.payeBatch, pay_channel: 'PAYE' },
      { pay_batch_id: IDS.umbrellaBatch, pay_channel: 'UMBRELLA' }
    ],
    paye_pay_batch_id: IDS.payeBatch,
    umbrella_pay_batch_id: IDS.umbrellaBatch,
    candidate_count: 2,
    reservation_availability: {
      applied: false,
      skipped_preview_row_count: 0,
      clipped_preview_row_count: 0,
      skipped_item_rows: 0,
      clipped_item_rows: 0,
      message: null,
      preview_rows: []
    },
    patch_results: [
      {
        pay_batch_id: IDS.payeBatch,
        ok: true,
        patch_applied: true,
        affected_candidate_count: 1,
        affected_candidate_ids: [IDS.candidateA],
        affected_row_count: 1,
        patched_row_count: 1,
        patched_row_ids: [IDS.rowA],
        targeted_refresh_enqueued_count: 1,
        targeted_refresh_candidate_ids: [IDS.candidateA]
      },
      {
        pay_batch_id: IDS.umbrellaBatch,
        ok: true,
        patch_applied: !replacement,
        replacement_session_required: replacement,
        affected_candidate_count: 1,
        affected_candidate_ids: [IDS.candidateB],
        affected_row_count: 1,
        patched_row_count: 1,
        patched_row_ids: [IDS.rowB]
      }
    ],
    replacement_session_required: replacement,
    replacement_idempotency_key: `DRAFT_CREATE:${IDS.operation}:BATCHES:${IDS.payeBatch},${IDS.umbrellaBatch}`
  };
}

test('the row-backed terminal builder preserves the accepted V1 success fields without selection arrays', async () => {
  const api = await import(MODULE_URL.href);
  const validated = api.validateCertifiedDraftTerminalContextV8(context());
  assert.equal(validated.ok, true);
  const wake = { ok: true, scheduled: true, worker_scope: 'SESSION', wait_until_used: true };
  const result = api.buildCertifiedDraftTerminalResultV8(validated, null, wake);
  assert.equal(result.ok, true);
  assert.equal(result.operation_type, 'DRAFT_CREATE');
  assert.deepEqual(result.pay_batch_ids, [IDS.payeBatch, IDS.umbrellaBatch]);
  assert.deepEqual(result.created_pay_batch_ids, result.pay_batch_ids);
  assert.deepEqual(result.created_batches, context().created_batches);
  assert.equal(result.paye_pay_batch_id, IDS.payeBatch);
  assert.equal(result.umbrella_pay_batch_id, IDS.umbrellaBatch);
  assert.equal(result.candidate_count, 2);
  assert.equal(result.post_action_refresh.mode, 'PATCH_EXISTING_SESSION');
  assert.equal(result.post_create_refresh.patch_applied, true);
  assert.equal(result.post_create_refresh.targeted_refresh_enqueued, true);
  assert.deepEqual(result.post_create_refresh.targeted_refresh_candidate_ids, [IDS.candidateA]);
  assert.deepEqual(result.post_create_refresh.affected_candidate_ids, [IDS.candidateA, IDS.candidateB]);
  assert.equal(result.worker_wake_scheduled, true);
  assert.equal(result.session.adopted_replacement_session, false);
  assert.deepEqual(api.findExpandedSelectionKeys(result), []);
});

test('replacement vocabulary is used only after the existing patch owner explicitly requests it', async () => {
  const api = await import(MODULE_URL.href);
  const validated = api.validateCertifiedDraftTerminalContextV8(context(true));
  assert.equal(validated.ok, true);
  assert.throws(() => api.buildCertifiedDraftTerminalResultV8(validated), /replacement/i);

  const replacement = {
    ok: true,
    source_session_version: 11,
    source_discarded_at_utc: '2026-09-03T13:00:00.000Z',
    replacement_session_id: IDS.replacement,
    replacement_session_version: 1,
    replacement_session_signature: 'replacement-signature',
    replacement_snapshot_run_id: IDS.replacementSnapshot,
    replacement_idempotency_key: context(true).replacement_idempotency_key,
    replacement_created: true,
    old_rows_retained: true,
    atomic_replacement: true,
    replacement_session: {
      session_id: IDS.replacement,
      session_version: 1,
      session_signature: 'replacement-signature',
      snapshot_run_id: IDS.replacementSnapshot
    }
  };
  const result = api.buildCertifiedDraftTerminalResultV8(
    validated,
    replacement,
    { ok: true, already_running: true, worker_scope: 'SESSION' }
  );
  assert.equal(result.action, 'ADOPT_REPLACEMENT_SESSION');
  assert.equal(result.replacement_session_id, IDS.replacement);
  assert.equal(result.session.session_id, IDS.replacement);
  assert.equal(result.session.source_session_id, IDS.session);
  assert.equal(result.post_create_refresh.replacement_available, true);
  assert.equal(result.post_create_refresh.old_rows_retained, true);
  assert.equal(result.worker_wake_scheduled, true);
  assert.deepEqual(api.findExpandedSelectionKeys(result), []);
});

test('terminal context validation rejects batch, count and identity drift', async () => {
  const api = await import(MODULE_URL.href);
  for (const mutate of [
    (value) => { value.source_session_id = IDS.replacement; },
    (value) => { value.candidate_count = 50001; },
    (value) => { value.created_pay_batch_ids = [IDS.payeBatch]; },
    (value) => { value.created_batches[0].pay_channel = 'ALL'; },
    (value) => { value.patch_results.pop(); },
    (value) => { value.reservation_availability = []; }
  ]) {
    const value = context();
    mutate(value);
    assert.equal(api.validateCertifiedDraftTerminalContextV8(value).ok, false);
  }
});

test('the terminal SQL reuses existing owners and contains no payment-policy vocabulary', () => {
  const contextOwner = read('supabase/repeatable/03092026_1451_banking_pay_draft_terminal_context_v8.sql');
  const finishOwner = read('supabase/repeatable/02092026_2331_banking_pay_draft_terminal_finish_v8.sql');
  const boundedOwner = read('supabase/repeatable/02092026_2330_banking_pay_draft_bounded_advance_v8.sql');
  assert.match(boundedOwner, /pay_workbench_patch_preview_after_batch_mutation/);
  assert.match(finishOwner, /POST_DRAFT_LIVE_AUTHORITY_V2/);
  assert.match(finishOwner, /DRAFT_CREATE_REPLACEMENT_TERMINAL_RESULT_MISMATCH/);
  assert.match(contextOwner, /BANKING_PAY_DRAFT_TERMINAL_CONTEXT_V8/);
  assert.doesNotMatch(contextOwner, /GROSS_ADD|GROSS_DEDUCT|NET_ADD|NET_DEDUCT|VAT_RATE|headroom|PAYMENT_ADVANCE_REPAYMENT|LOAN_REPAYMENT/i);
});

