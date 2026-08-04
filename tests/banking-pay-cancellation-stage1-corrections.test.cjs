const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const read = (name) => fs.readFileSync(path.join(root, 'supabase', 'repeatable', name), 'utf8');

const selection = read('04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql');
const processChunk = read('04082026_1209_pay_payment_correction_process_chunk.sql');
const preBank = read('04082026_1158_pay_pre_bank_cancel_apply_work_item.sql');
const noMoney = read('04082026_1158_pay_no_money_unwind_apply_work_item.sql');
const checker = read('04082026_1148_pay_payment_correction_integrity_check_v1.sql');
const status = read('04082026_1145_pay_payment_correction_status_get_v1.sql');
const expiry = read('04082026_1144_pay_payment_correction_expire_due_v1.sql');
const due = read('04082026_1154_pay_batches_claim_due_scheduled.sql');
const eventIngest = read('04082026_1210_pay_bank_event_ingest.sql');

test('all Stage 1 functions use executable PostgreSQL special-expression syntax', () => {
  const stageOneSql = fs.readdirSync(path.join(root, 'supabase', 'repeatable'))
    .filter((name) => /^04082026_.*\.sql$/.test(name))
    .map(read)
    .join('\n');
  assert.doesNotMatch(stageOneSql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('selection and phase processing require a non-null matching unexpired lease', () => {
  for (const source of [selection, processChunk]) {
    assert.match(source, /p_worker_id IS NULL OR pg_catalog\.btrim\(p_worker_id\) = ''/);
    assert.match(source, /'code', 'LEASE_REQUIRED'/);
    assert.match(source, /coalesce\(v_operation\.lease_owner, v_operation\.locked_by\) IS NULL/i);
    assert.match(source, /lease_expires_at_utc, v_operation\.lock_expires_at_utc\)[\s\S]{0,120}IS NULL/i);
  }
});

test('service scheduling cannot inject time through SECURITY DEFINER', () => {
  assert.match(due, /p_now_utc IS NOT NULL AND session_user <> 'postgres'/);
  assert.doesNotMatch(due, /p_now_utc IS NOT NULL AND current_user <> 'postgres'/);
  assert.match(due, /v_now timestamptz := pg_catalog\.clock_timestamp\(\)/);
});

test('planning and both candidate helpers bind and revalidate the complete 512-row source scope', () => {
  const required = [
    'pay_batch_item_breakdowns', 'pay_batch_timesheet_snapshots',
    'timesheet_pay_state_history', 'pay_advance_reservations',
    'pay_finance_case_components', 'pay_manual_adjustment_carry_forwards',
    'pay_advances', 'pay_bank_transfers', 'pay_bank_transfer_events',
    'banking_pay_operation_transfer_scope',
    'banking_pay_operation_transfer_scope_items',
    'pay_batch_paye_net_inputs', 'mail_outbox',
  ];
  for (const table of required) assert.match(selection, new RegExp(`public\\.${table}`));
  for (const source of [preBank, noMoney]) {
    assert.match(source, /v_current_source_row_count <> v_membership\.source_row_count/);
    assert.match(source, /v_current_source_row_count > 512/);
    assert.match(source, /v_current_candidate_scope_hash IS DISTINCT FROM v_membership\.candidate_scope_hash/);
    assert.match(source, /'CANDIDATE_SCOPE_TOO_LARGE'/);
    assert.match(source, /'SOURCE_SCOPE_CHANGED'/);
    const revalidation = source.indexOf('v_current_source_row_count <> v_membership.source_row_count');
    const firstFinancialWrite = source.indexOf('INSERT INTO public.pay_payment_correction_items');
    assert.ok(revalidation > 0 && revalidation < firstFinancialWrite);
  }
});

test('finalisation and checker reconcile Overview with candidate net-bank amounts', () => {
  assert.match(processChunk, /sum\(active_candidate\.net_bank_amount\)/i);
  assert.match(processChunk, /total_bank_out = v_active_net_bank_amount/);
  assert.match(processChunk, /'active_frozen_source_item_amount_pence'/);
  assert.match(processChunk, /'active_paye_schedule_amount_pence'/);
  assert.match(processChunk, /'active_transfer_amount_pence'/);
  assert.match(checker, /sum\(active_candidate\.net_bank_amount\)/i);
  assert.doesNotMatch(checker, /sum\(active_item\.amount_inc_vat\)[\s\S]{0,200}v_expected_active_total/i);
});

test('unselected and blocked financial scope has mandatory pre/post proof', () => {
  assert.match(selection, /'unselected_scope_hash_before', v_unselected_scope_hash_before/);
  assert.match(processChunk, /v_unselected_scope_hash_after := private\.pay_payment_correction_sha256_v1/);
  assert.match(processChunk, /v_blocked_scope_mismatch_count > 0/);
  assert.match(processChunk, /PAYMENT_CORRECTION_SCOPE_INTEGRITY_CONFLICT/);
  assert.match(checker, /v_unselected_before_hash IS NULL OR v_unselected_after_hash IS NULL/);
  assert.match(checker, /jsonb_build_array\('UNSELECTED_SCOPE_CHANGED'\)/);
  assert.match(checker, /v_blocked_scope_mismatch_count > 0/);
});

test('Workbench staging is distinct from actual job and candidate freshness', () => {
  for (const source of [status, checker]) {
    for (const state of ['NOT_STAGED', 'STAGED', 'PENDING', 'CURRENT', 'FAILED']) {
      assert.match(source, new RegExp(`'${state}'`));
    }
    assert.match(source, /banking_pay_workbench_jobs/);
    assert.match(source, /banking_pay_workbench_session_candidate_state/);
    assert.match(source, /source_change_seq >= candidate_freshness\.job_generation/);
  }
  assert.match(processChunk, /'workbench_refresh_status', CASE/);
  assert.doesNotMatch(processChunk, /'workbench_refresh_complete', true/);
});

test('expiry selection is non-starving and user-visible expiry is audit typed', () => {
  const selectionLimit = expiry.indexOf('LIMIT p_limit');
  assert.ok(expiry.indexOf('request_row.authorised_at_utc IS NULL') < selectionLimit);
  assert.ok(expiry.indexOf('work_item.status = \'APPLIED\'') < selectionLimit);
  assert.ok(expiry.indexOf('correction_item.status = \'APPLIED\'') < selectionLimit);
  assert.doesNotMatch(expiry, /v_request\.source_bank_event_id/);
  assert.doesNotMatch(expiry, /v_request\.accepted_resolution_json/);
  assert.match(status, /metadata_json ->> 'audit_code' = 'UNAPPROVED_REQUEST_EXPIRED'/);
  assert.doesNotMatch(status, /requested_at_utc <= pg_catalog\.clock_timestamp\(\) - interval '24 hours'/);
});

test('late paid evidence demotes the installed authorised batch state', () => {
  assert.match(eventIngest, /replacement_batch\.status IN \('AUTHORISED_FOR_PAYMENT','SCHEDULED','EXECUTING'\)/);
  assert.doesNotMatch(eventIngest, /replacement_batch\.status IN \('AUTHORISED','SCHEDULED','EXECUTING'\)/);
});
