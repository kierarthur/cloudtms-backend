const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const migrationPath = path.join(
  root,
  'supabase',
  'migrations',
  '04082026_1126_banking_pay_cancellation_schema.sql'
);

const stageOneRepeatables = [
  '04082026_1141_pay_payment_correction_sha256_v1.sql',
  '04082026_1142_pay_payment_mutation_guard_v1.sql',
  '04082026_1143_pay_payment_correction_reauth_bind_v1.sql',
  '04082026_1144_pay_payment_correction_expire_due_v1.sql',
  '04082026_1145_pay_payment_correction_status_get_v1.sql',
  '04082026_1146_pay_batch_payment_status_page_v1.sql',
  '04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql',
  '04082026_1148_pay_payment_correction_integrity_check_v1.sql',
  '04082026_1154_banking_pay_operation_start.sql',
  '04082026_1154_pay_bank_transfers_claim_provider_submit_chunk.sql',
  '04082026_1154_pay_batch_auth_apply_action.sql',
  '04082026_1154_pay_batches_claim_due_scheduled.sql',
  '04082026_1154_pay_payment_correction_authorise.sql',
  '04082026_1158_pay_batch_auth_start.sql',
  '04082026_1158_pay_batch_schedule.sql',
  '04082026_1158_pay_no_money_unwind_apply_work_item.sql',
  '04082026_1158_pay_pre_bank_cancel_apply_work_item.sql',
  '04082026_1206_pay_batch_cancel.sql',
  '04082026_1207_pay_payment_correction_request_start.sql',
  '04082026_1208_pay_payment_correction_expand_work.sql',
  '04082026_1209_pay_payment_correction_process_chunk.sql',
  '04082026_1210_pay_bank_event_ingest.sql',
  '04082026_1211_pay_settle_rail.sql',
  '04082026_1231_retire_pay_payment_cancel_not_sent_and_recalculate.sql',
  '04082026_1231_retire_pay_payment_cancel_not_sent_and_recalculate_complete_v1.sql',
  '04082026_1231_retire_pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr.sql',
  '04082026_1231_retire_pay_payment_confirm_no_money_and_unwind.sql',
];

const compatibilityNames = [
  'pay_payment_cancel_not_sent_and_recalculate',
  'pay_payment_cancel_not_sent_and_recalculate_complete_v1',
  'pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr',
  'pay_payment_confirm_no_money_and_unwind',
];

function read(relativePath) {
  return fs.readFileSync(path.join(root, relativePath), 'utf8');
}

function readRepeatable(name) {
  return fs.readFileSync(path.join(repeatableDir, name), 'utf8');
}

test('Stage 1 has the exact 8 new, 15 amended, and 4 retired SQL files', () => {
  for (const file of stageOneRepeatables) {
    assert.equal(fs.existsSync(path.join(repeatableDir, file)), true, file);
  }
  assert.equal(stageOneRepeatables.length, 27);
  assert.equal(stageOneRepeatables.filter((name) => name.includes('_retire_')).length, 4);
  assert.equal(fs.existsSync(migrationPath), true);
});

test('schema migration stays inside the locked additive boundary', () => {
  const sql = fs.readFileSync(migrationPath, 'utf8');
  assert.match(sql, /CREATE TABLE public\.pay_payment_correction_request_candidates/);
  assert.match(sql, /ENABLE ROW LEVEL SECURITY/);
  assert.match(sql, /FORCE ROW LEVEL SECURITY/);
  assert.doesNotMatch(sql, /CREATE POLICY/i);
  for (const column of [
    'reauth_proof_hash',
    'reauth_expires_at_utc',
    'reauth_consumed_at_utc',
    'banking_pay_candidate_cancellation_enabled',
    'banking_pay_correction_max_candidates',
    'banking_pay_correction_max_active_items',
    'banking_pay_correction_max_active_items_per_candidate',
    'banking_pay_correction_max_source_rows_per_candidate',
  ]) {
    assert.match(sql, new RegExp(`\\b${column}\\b`));
  }
  const configRows = sql.match(/\('PAYMENT_CORRECTION',[^\n]+\)/g) || [];
  assert.equal(configRows.length, 5);
  for (const row of configRows) {
    assert.match(row, /,\s*1,\s*(?:25|100),\s*7500,\s*60,\s*false\)$/);
  }
  assert.doesNotMatch(sql, /CREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER/i);
});

test('compatibility bodies are owner-only and absent from the new normal call graph', () => {
  const normalFiles = stageOneRepeatables.filter((name) => !name.includes('_retire_'));
  const normalSql = normalFiles.map(readRepeatable).join('\n');
  for (const name of compatibilityNames) {
    assert.doesNotMatch(normalSql, new RegExp(`\\b${name}\\s*\\(`));
  }
  for (const file of stageOneRepeatables.filter((name) => name.includes('_retire_'))) {
    const sql = readRepeatable(file);
    assert.match(sql, /OWNER TO postgres;/);
    assert.match(sql, /REVOKE ALL ON FUNCTION[\s\S]*FROM service_role;/);
    assert.doesNotMatch(sql, /GRANT EXECUTE[\s\S]*TO service_role;/);
  }
});

test('the correction runner owns one bounded phase and the only Workbench calls', () => {
  const processSql = read('supabase/repeatable/04082026_1209_pay_payment_correction_process_chunk.sql');
  const allSql = stageOneRepeatables.map(readRepeatable).join('\n');
  assert.doesNotMatch(processSql, /^\s*WHILE\b/im);
  assert.match(processSql, /FOR UPDATE SKIP LOCKED/);
  assert.match(processSql, /v_claim_limit\s*:=\s*pg_catalog\.least\(p_limit,\s*25\)/i);
  assert.match(processSql, /LIMIT 100/);
  assert.equal(
    (allSql.match(/pay_workbench_(?:patch_preview_after_batch_mutation_cancel_safe_v1|enqueue_candidate_refresh_many)\s*\(/g) || []).length,
    2
  );
  assert.equal(
    (processSql.match(/pay_workbench_(?:patch_preview_after_batch_mutation_cancel_safe_v1|enqueue_candidate_refresh_many)\s*\(/g) || []).length,
    2
  );
  assert.doesNotMatch(allSql, /(?:LIMIT|chunk_size|max_chunk_size)\s*=?\s*500\b/i);
  assert.doesNotMatch(allSql, /DELETE\s+FROM\s+public\.pay_payment_correction_request_candidates/i);
});

test('every financial mutation boundary uses the shared mode-aware guard', () => {
  const required = [
    '04082026_1154_pay_bank_transfers_claim_provider_submit_chunk.sql',
    '04082026_1154_pay_batch_auth_apply_action.sql',
    '04082026_1154_pay_batches_claim_due_scheduled.sql',
    '04082026_1154_pay_payment_correction_authorise.sql',
    '04082026_1158_pay_batch_auth_start.sql',
    '04082026_1158_pay_batch_schedule.sql',
    '04082026_1158_pay_no_money_unwind_apply_work_item.sql',
    '04082026_1158_pay_pre_bank_cancel_apply_work_item.sql',
    '04082026_1207_pay_payment_correction_request_start.sql',
    '04082026_1210_pay_bank_event_ingest.sql',
    '04082026_1211_pay_settle_rail.sql',
  ];
  for (const file of required) {
    assert.match(readRepeatable(file), /private\.pay_payment_mutation_guard_v1\s*\(/, file);
  }
});

test('late paid evidence is classified before completed-event idempotent return', () => {
  const sql = read('supabase/repeatable/04082026_1210_pay_bank_event_ingest.sql');
  const classification = sql.indexOf('INTO v_paid_after_release');
  const completedBranch = sql.indexOf("IF v_normalised_state = 'COMPLETED'");
  assert.notEqual(classification, -1);
  assert.notEqual(completedBranch, -1);
  assert.equal(classification < completedBranch, true);
  assert.equal((sql.match(/public\.pay_settle_rail\s*\(/g) || []).length, 1);
  assert.match(sql, /PAID_EVIDENCE_AFTER_RELEASE/);
  assert.match(sql, /AUTHORITATIVE_EVENT/);
});

test('cancellation refreshes Overview totals and removes fully cancelled PAYE rows from active scope', () => {
  for (const file of [
    'supabase/repeatable/04082026_1158_pay_pre_bank_cancel_apply_work_item.sql',
    'supabase/repeatable/04082026_1158_pay_no_money_unwind_apply_work_item.sql',
  ]) {
    const sql = read(file);
    assert.match(sql, /COALESCE\(public\.pay_batch_items\.is_voided, false\) = false/);
    assert.match(sql, /WHEN COALESCE\(affected_candidate_sums\.active_item_count, 0\) = 0 THEN 0::numeric\(12,2\)/);
    assert.match(sql, /UPDATE public\.pay_batches AS batch_to_refresh/);
    assert.match(sql, /total_bank_out\s*=\s*COALESCE/);
    assert.match(sql, /public\.pay_batch_paye_net_inputs/);
  }
  const processSql = read('supabase/repeatable/04082026_1209_pay_payment_correction_process_chunk.sql');
  assert.match(processSql, /public\.pay_batch_display_summary_refresh\(v_request\.pay_batch_id\)/);
  assert.match(processSql, /p_touch_overview\s*:=\s*true/);
  assert.match(processSql, /'active_paye_schedule_derived_from_unvoided_frozen_items', true/);
  assert.match(processSql, /'paye_schedule_updated'/);
});
