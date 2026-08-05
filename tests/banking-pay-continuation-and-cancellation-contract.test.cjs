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
});

test('queue continuation is explicit, bounded and awaits a successor before acknowledgement', () => {
  const queueBody = functionBody('handleBankingPayContinuationQueue');
  assert.match(queueBody, /flag === false/);
  assert.match(queueBody, /message\.ack\(\)/);
  assert.match(queueBody, /message\.retry\(/);
  assert.match(queueBody, /await enqueueBankingPayOperationContinuations/);
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
  const witness = functionBody('buildBankingPayContinuationProgressWitness');
  assert.doesNotMatch(witness, /lease_owner|lease_expires|locked_by|updated_at/);
  assert.match(worker, /continuation_no_progress_count/);
  assert.match(worker, />= 5/);
  assert.match(worker, /WAITING_USER_REVIEW/);
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
    assert.match(body, new RegExp(`payload\\.${field}`));
  }
  assert.match(body, /reauth_expires_at_utc/);
  assert.match(body, /REAUTH_PROOF_INVALID/);
});
