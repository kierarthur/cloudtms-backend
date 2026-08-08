const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const wrangler = fs.readFileSync(path.join(root, 'wrangler.toml'), 'utf8');
const recoverySql = fs.readFileSync(path.join(root, 'supabase', 'repeatable', '04082026_2314_banking_pay_operation_continuation_recovery_due_v1.sql'), 'utf8');
const genericOperationSql = [
  '05082026_0914_banking_pay_operation_claim_next.sql',
  '05082026_0915_banking_pay_operation_release_lease.sql'
].map((name) => fs.readFileSync(path.join(root, 'supabase', 'repeatable', name), 'utf8')).join('\n');

function functionBody(name) {
  const start = worker.indexOf(`function ${name}`);
  assert.notEqual(start, -1, `${name} missing`);
  const next = worker.indexOf('\nfunction ', start + 10);
  const nextAsync = worker.indexOf('\nasync function ', start + 10);
  const boundaries = [next, nextAsync].filter((value) => value > start);
  const end = boundaries.length ? Math.min(...boundaries) : worker.length;
  return worker.slice(start, end);
}

test('one queue message makes one generic claim-and-advance call and never preclaims', () => {
  const body = functionBody('processBankingPayContinuationMessage');
  assert.equal((body.match(/claimAndAdvanceOneBankingPayOperation\(/g) || []).length, 1);
  assert.doesNotMatch(body, /banking_pay_operation_claim_next/);
  assert.match(body, /executionContext:\s*options\.executionContext\s*\|\|\s*null/);
  assert.match(body, /workerResult\.claimed\s*!==\s*true/);
  assert.match(body, /safeDelayedReasons\s*=\s*new Set\(\['RUN_AFTER_NOT_DUE', 'LEASE_ACTIVE'\]\)/);
  assert.match(body, /continuation_suppressed:\s*true/);
  assert.match(body, /LEASE_ACTIVE_RETRY/);
});

test('a future scheduled no-bank payment is committed by the schedule owner and completes without provider submission', () => {
  const body = functionBody('advanceBankingPayExecuteOperation');
  const noBankStart = body.indexOf('if (scheduledNoBankProof.valid === true)');
  const manualStart = body.indexOf('if (isLocalManualSettlementMode())', noBankStart);
  assert.ok(noBankStart >= 0 && manualStart > noBankStart);
  const noBankBranch = body.slice(noBankStart, manualStart);
  assert.match(noBankBranch, /await rpc\('pay_batch_schedule'/);
  assert.match(noBankBranch, /return complete\(Object\.assign\(/);
  assert.match(noBankBranch, /provider_submission_suppressed:\s*true/);
  assert.doesNotMatch(noBankBranch, /WAIT_FOR_SCHEDULED_NO_BANK_PAYMENT/);
});

test('queue continuation is explicit, bounded and awaits a successor before acknowledgement', () => {
  const queueBody = functionBody('handleBankingPayContinuationQueue');
  assert.match(queueBody, /messages\.length !== 1/);
  assert.match(queueBody, /BANKING_PAY_CONTINUATION_BATCH_SIZE_INVALID/);
  assert.match(queueBody, /flag === false/);
  assert.match(queueBody, /message\.ack\(\)/);
  assert.match(queueBody, /message\.retry\(/);
  assert.match(queueBody, /await enqueueBankingPayOperationContinuations/);
  assert.match(queueBody, /processBankingPayContinuationMessage\(env, message, \{ executionContext: ctx \}\)/);
  assert.match(worker, /encoded\.byteLength > 2048/);
  assert.match(worker, /sourceRows\.length > 50/);
});

test('parent execute and settlement operations hand children to the queue', () => {
  const execute = functionBody('advanceBankingPayExecuteOperation');
  const settlement = functionBody('advanceBankingPaySettlementOperation');
  assert.match(execute, /successor_relation:\s*'CHILD'/);
  assert.match(settlement, /successor_relation:\s*'CHILD'/);
  assert.match(worker, /successor_relation:\s*'ROOT'/);
  assert.match(worker, /releaseState = 'WAITING_CHILD'/);
  assert.match(genericOperationSql, /v_release_state IN \('WAITING_CHILD', 'WAIT_CHILD'\)/);
  assert.match(genericOperationSql, /child_operation\.root_operation_id = operation_row\.id/);
  assert.match(genericOperationSql, /child_operation\.status[\s\S]{0,200}'REVIEW_REQUIRED'/);
  assert.match(genericOperationSql, /banking_pay_operation_claim_next[\s\S]*?SET search_path TO 'pg_catalog', 'private', 'extensions', 'pg_temp'/);
  assert.match(genericOperationSql, /REVOKE ALL ON FUNCTION public\.banking_pay_operation_claim_next\(uuid,uuid,text,integer,boolean,text\[\]\) FROM authenticated/);
  assert.match(genericOperationSql, /banking_pay_operation_release_lease[\s\S]*?SET search_path TO 'pg_catalog', 'private', 'extensions', 'pg_temp'/);
  assert.match(genericOperationSql, /REVOKE ALL ON FUNCTION public\.banking_pay_operation_release_lease\(uuid,text,text,integer,jsonb,jsonb,jsonb,text,uuid\) FROM authenticated/);
});

test('semantic progress excludes lease timestamps and uses the five-strike fuse', () => {
  const value = functionBody('buildBankingPayContinuationProgressValue');
  assert.doesNotMatch(value, /universal[\s\S]{0,500}runner_state:/);
  assert.doesNotMatch(value, /universal[\s\S]{0,500}run_after_utc:/);
  assert.doesNotMatch(value, /lease_owner|lease_expires|locked_by|updated_at/);
  assert.match(worker, /continuation_no_progress_count/);
  assert.match(worker, />= 5/);
  assert.match(worker, /WAITING_USER_REVIEW/);
});

test('generic continuation boundary rejects unsafe and unknown descriptors before sending', () => {
  const mapper = functionBody('buildBankingPayContinuationMessage');
  const enqueue = functionBody('enqueueBankingPayOperationContinuations');
  assert.match(worker, /function bankingPayContinuationSourcePolicy/);
  assert.match(mapper, /BANKING_PAY_CONTINUATION_SOURCE_INVALID/);
  assert.match(mapper, /BANKING_PAY_CONTINUATION_EMBEDDED_SOURCE_INVALID/);
  assert.match(mapper, /BANKING_PAY_CONTINUATION_SOURCE_SCOPE_INVALID/);
  assert.match(mapper, /BANKING_PAY_CONTINUATION_TERMINAL_REQUIRED_INVALID/);
  assert.match(mapper, /BANKING_PAY_CONTINUATION_USER_WAIT_REQUIRED_INVALID/);
  assert.match(mapper, /BANKING_PAY_CONTINUATION_NONE_REQUIRED_INVALID/);
  assert.match(mapper, /BANKING_PAY_CONTINUATION_WAIT_PHASE_INVALID/);
  assert.ok(enqueue.indexOf('buildBankingPayContinuationMessage') < enqueue.indexOf('await env.BANKING_PAY_CONTINUATION_QUEUE.send'));
});

test('payment correction dispatcher preserves the exact SQL retry descriptor', () => {
  const body = functionBody('advancePaymentCorrectionOperation');
  assert.match(body, /result\.continuation/);
  assert.match(body, /PAYMENT_CORRECTION_SQL_CONTINUATION_INVALID/);
  assert.match(body, /sqlContinuationSource\.run_after_utc/);
  assert.match(body, /reason: String\(sqlContinuationSource\.reason/);
  assert.match(body, /parsePaymentCorrectionWorkbenchNudgeEnvelope/);
  assert.match(body, /schedulePaymentCorrectionWorkbenchNudge/);
  assert.match(body, /workbench_refresh_nudge_scheduled/);
});

test('terminal correction refresh hands one database-owned session to a durable Workbench nudge', () => {
  const parser = functionBody('parsePaymentCorrectionWorkbenchNudgeEnvelope');
  const nudge = functionBody('schedulePaymentCorrectionWorkbenchNudge');
  const runnable = functionBody('advanceBankingPayRunnableOperations');
  const cron = functionBody('bankingCronTick');
  const genericAdvance = functionBody('claimAndAdvanceOneBankingPayOperation');
  assert.match(parser, /PAYMENT_CORRECTION_WORKBENCH_NUDGE_V1/);
  assert.match(parser, /UNKNOWN_FIELD/);
  assert.match(parser, /COUNT_MISMATCH/);
  assert.match(nudge, /nudgeBankingPayWorkbenchDrain\(env, executionContext/);
  assert.match(nudge, /enqueueBankingPayWorkbenchDrainWake/);
  assert.match(nudge, /cron_fallback_required:\s*true/);
  assert.match(runnable, /executionContext:\s*opts\.executionContext\s*\|\|\s*null/);
  assert.match(cron, /executionContext:\s*opts\.executionContext\s*\|\|\s*null/);
  assert.match(genericAdvance, /executionContext:\s*opts\.executionContext\s*\|\|\s*null/);
  assert.match(worker, /bankingCronTick\(env, \{ executionContext: ctx \}\)/);
});

test('recovery RPC is read-only, bounded and calculates the effective stale threshold', () => {
  assert.match(recoverySql, /LANGUAGE sql/i);
  assert.match(recoverySql, /\bSTABLE\b/i);
  assert.doesNotMatch(recoverySql, /\b(INSERT|UPDATE|DELETE)\b/i);
  assert.match(recoverySql, /greatest\(coalesce\(p_stale_after_seconds, 90\), 90\)/i);
  assert.match(recoverySql, /lock_seconds/i);
  assert.match(recoverySql, /max_advance_ms/i);
  assert.match(recoverySql, /LIMIT \(SELECT row_limit FROM validated\)/i);
  assert.match(recoverySql, /runner_state[\s\S]{0,120}'WAITING_CHILD'/i);
  assert.match(recoverySql, /child_operation\.root_operation_id = operation_row\.id/i);
  assert.match(recoverySql, /child_operation\.status[\s\S]{0,200}'REVIEW_REQUIRED'/i);
});

test('TEST queue configuration has batch one, retries, concurrency and a DLQ', () => {
  assert.match(wrangler, /binding\s*=\s*"BANKING_PAY_CONTINUATION_QUEUE"/);
  assert.match(wrangler, /max_batch_size\s*=\s*1/);
  assert.match(wrangler, /max_retries\s*=\s*10/);
  assert.match(wrangler, /retry_delay\s*=\s*5/);
  assert.match(wrangler, /max_concurrency\s*=\s*2/);
  assert.match(wrangler, /dead_letter_queue\s*=\s*"test-cloudtms-banking-pay-continuation-dlq"/);
});

test('browser-facing cancellation routes use bounded RPCs and retire direct process', () => {
  assert.match(worker, /pay_batch_payment_status_page_v1/);
  assert.match(worker, /pay_payment_correction_status_get_v1/);
  assert.match(worker, /pay_payment_correction_reauth_bind_v1/);
  assert.match(worker, /pay_payment_correction_integrity_check_v1/);
  assert.match(worker, /PAYMENT_CORRECTION_PROCESS_ROUTE_RETIRED/);
  const routes = worker.slice(worker.indexOf("'/api/banking/pay/batch/:id/payment-status'"));
  assert.doesNotMatch(routes.slice(0, 12000), /return handleBankingPayCorrectionProcess\(/);
});

test('manual confirmed-not-paid remains evidence-only', () => {
  const body = functionBody('handleBankingPayPaymentStatusResolveV1');
  assert.match(body, /CONFIRMED_NOT_PAID/);
  assert.match(body, /suppress_auto_unwind/);
  assert.doesNotMatch(body, /pay_payment_correction_request_start/);
});

test('all normal Worker call sites retire the four owner-only compatibility RPCs', () => {
  assert.doesNotMatch(worker, /sbRpc\(env, 'pay_payment_cancel_not_sent_and_recalculate(?:_with_workbench_refr|_complete_v1)?'/);
  assert.doesNotMatch(worker, /sbRpc\(env, 'pay_payment_confirm_no_money_and_unwind'/);
});

test('reauthenticated start rechecks every signed plan field and the database-bound expiry', () => {
  const body = functionBody('handleBankingPayCorrectionStartPreparedV1');
  for (const field of ['requested_action', 'selected_candidate_count', 'selected_active_item_count', 'selected_amount_pence', 'reason_hash', 'evidence_hash', 'outcome_hash']) {
    assert.match(body, new RegExp(`${field}:`));
  }
  assert.match(body, /reauth_expires_at_utc/);
  assert.match(body, /REAUTH_PROOF_INVALID/);
  assert.match(body, /verifyPaymentReversalReauth\(env, user, user, token/);
});
