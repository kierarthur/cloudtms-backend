const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const worker = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const retrySql = fs.readFileSync(path.join(
  root,
  'supabase',
  'repeatable',
  '07082026_1538_pay_payment_correction_retry_planning_v1.sql'
), 'utf8');

test('planning retry reuses one exact operation and is limited to the pre-selection review state', () => {
  assert.match(retrySql, /operation_row\.operation_type = 'PAYMENT_CORRECTION'/);
  assert.match(retrySql, /operation_row\.input_json ->> 'correction_request_id' = p_correction_request_id::text/);
  assert.match(retrySql, /v_request\.status[\s\S]*'PLANNING'/);
  assert.match(retrySql, /v_operation\.status[\s\S]*'REVIEW_REQUIRED'/);
  assert.match(retrySql, /v_operation\.phase[\s\S]*'PREPARE_SELECTION'/);
  assert.match(retrySql, /v_error_code <> 'BANKING_PAY_OPERATION_ADVANCE_FAILED'/);
  assert.match(retrySql, /v_retry_count >= 3/);
  assert.doesNotMatch(retrySql, /INSERT\s+INTO\s+public\.pay_payment_correction_requests/i);
  assert.doesNotMatch(retrySql, /INSERT\s+INTO\s+public\.banking_pay_operations/i);
});

test('whole-Draft expand retry reuses the same authorised operation only before durable cancellation work exists', () => {
  assert.match(retrySql, /v_retry_mode := 'DRAFT_EXPAND'/);
  assert.match(retrySql, /v_request\.status[\s\S]*'AUTHORISED'[\s\S]*'AUTHORIZED'/);
  assert.match(retrySql, /v_operation\.phase[\s\S]*'EXPAND_WORK'/);
  assert.match(retrySql, /v_request\.plan_json ->> 'requested_action'[\s\S]*'DRAFT_CANCEL'/);
  assert.match(retrySql, /v_batch\.status[\s\S]*'DRAFT'/);
  assert.match(retrySql, /public\.banking_pay_operation_transfer_scope/);
  assert.match(retrySql, /PAYMENT_CORRECTION_DRAFT_EXPAND_RETRY_EVIDENCE_EXISTS/);
  assert.match(retrySql, /processing_retry_count/);
  assert.match(retrySql, /PAYMENT_CORRECTION_PROCESSING_RETRY_QUEUED/);
});

test('planning retry refuses any durable work or provider evidence before making the same operation runnable', () => {
  for (const relation of [
    'banking_pay_operation_chunks',
    'pay_payment_correction_request_candidates',
    'pay_payment_correction_work_items',
    'pay_payment_correction_items',
    'pay_payment_correction_actions',
    'banking_pay_operation_provider_attempts'
  ]) {
    assert.match(retrySql, new RegExp(`public\\.${relation}`));
  }
  assert.match(retrySql, /action_row\.action IS DISTINCT FROM 'REQUEST'/);
  assert.match(retrySql, /SET status = 'RUNNING'[\s\S]*runner_state = 'RUNNABLE'[\s\S]*requires_user_action = false/);
  assert.match(retrySql, /'operation_id', v_operation\.id[\s\S]*'successor_relation', 'SELF'/);
});

test('Worker exposes and executes the typed safe planning and processing retry actions', () => {
  assert.match(worker, /result\.planning_retry_available = true/);
  assert.match(worker, /result\.processing_retry_available = true/);
  assert.match(worker, /result\.available_actions = Array\.from\(new Set\(\[[\s\S]*'RETRY_PLANNING'/);
  assert.match(worker, /'RETRY_PROCESSING'/);
  assert.match(worker, /\['AUTHORISE','USE_GOLDEN_KEY','REJECT','CANCEL','RETRY_PLANNING','RETRY_PROCESSING'\]/);
  assert.match(worker, /sbRpc\(env, 'pay_payment_correction_retry_planning_v1'/);
  assert.match(worker, /PAYMENT_CORRECTION_RETRY_PLANNING/);
  assert.match(worker, /PAYMENT_CORRECTION_RETRY_PROCESSING/);
});

test('planning retry RPC remains service-only', () => {
  assert.match(retrySql, /SECURITY DEFINER/);
  assert.match(retrySql, /REVOKE ALL ON FUNCTION public\.pay_payment_correction_retry_planning_v1\(uuid, uuid\) FROM PUBLIC/);
  assert.match(retrySql, /FROM anon/);
  assert.match(retrySql, /FROM authenticated/);
  assert.match(retrySql, /GRANT EXECUTE ON FUNCTION public\.pay_payment_correction_retry_planning_v1\(uuid, uuid\) TO service_role/);
});
