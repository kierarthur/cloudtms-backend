const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const ownerPath = path.join(
  root,
  'supabase/repeatable/03092026_1200_banking_pay_draft_certificate_stage_advance_v8.sql',
);
const source = fs.readFileSync(ownerPath, 'utf8');
const adapterPath = path.join(
  root,
  'supabase/repeatable/02092026_2311_banking_pay_draft_frozen_certificate_adapters_v8.sql',
);
const adapters = fs.readFileSync(adapterPath, 'utf8');
const runtimePath = path.join(
  root,
  'tests/03092026_1201_banking_pay_draft_certificate_stage_advance_v8_runtime.sql',
);
const runtime = fs.readFileSync(runtimePath, 'utf8');

test('certificate stage coordinator owns one exact bounded service routine', () => {
  assert.equal((source.match(/CREATE OR REPLACE FUNCTION/gi) || []).length, 1);
  assert.match(source, /public\.banking_pay_draft_certificate_stage_advance_v8\(\s*p_operation_id uuid,\s*p_worker_id text\s*\)/i);
  assert.match(source, /SECURITY DEFINER/i);
  assert.match(source, /SET search_path TO ''/i);
  assert.match(source, /REVOKE ALL[\s\S]*FROM PUBLIC, anon, authenticated/i);
  assert.match(source, /GRANT EXECUTE[\s\S]*TO service_role/i);
});

test('one invocation performs at most one page or one final freeze', () => {
  assert.equal((source.match(/pay_workbench_draft_certificate_constituent_ref_page_v8\(/g) || []).length, 1);
  assert.equal((source.match(/pay_workbench_draft_certificate_partition_ref_page_v8\(/g) || []).length, 1);
  assert.equal((source.match(/pay_workbench_prepare_draft_scope_from_frozen_page_v8\(/g) || []).length, 1);
  assert.equal((source.match(/pay_workbench_draft_certificate_final_freeze_v8\(/g) || []).length, 1);
  assert.doesNotMatch(source, /\bLOOP\b|\bWHILE\b/i);
  assert.match(source, /'page_call_count', 1/);
  assert.match(source, /'freeze_call_count', 1/);
});

test('operation row is the sole concurrency fence and Candidate rows are never locked', () => {
  assert.match(source, /FROM public\.banking_pay_operations[\s\S]*FOR UPDATE/i);
  assert.match(source, /FROM private\.banking_pay_draft_frozen_certificate_scopes_v8[\s\S]*FOR UPDATE/i);
  assert.doesNotMatch(source, /FROM\s+(?:public\.)?candidates[\s\S]{0,300}FOR UPDATE/i);
  assert.doesNotMatch(source, /banking_pay_workbench_(?:preview_rows|session_candidate_state)[\s\S]{0,300}FOR UPDATE/i);
});

test('the first staging call initialises the exact compact scope and exact replay cannot drift', () => {
  const initialiseOwner = adapters.slice(
    adapters.indexOf('CREATE OR REPLACE FUNCTION private.banking_pay_draft_frozen_certificate_scope_initialise_v8'),
    adapters.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_draft_certificate_constituent_ref_page_v8'),
  );
  assert.match(source, /COALESCE\(v_operation\.phase, ''\)\)\) = 'INITIALISE'[\s\S]*private\.banking_pay_draft_frozen_certificate_scope_initialise_v8\(\s*p_operation_id/i);
  assert.match(adapters, /CREATE OR REPLACE FUNCTION private\.banking_pay_draft_frozen_certificate_scope_initialise_v8\(\s*p_operation_id uuid\s*\)/i);
  assert.match(adapters, /banking_pay_workbench_settled_certificate_operation_links_v8[\s\S]*link_state <> 'STAGING'/i);
  assert.match(adapters, /banking_pay_workbench_settled_certificate_channel_manifests_v8[\s\S]*manifest_digest_sha256 IS DISTINCT FROM v_link\.channel_manifest_digest_sha256/i);
  assert.match(adapters, /constituent_count NOT BETWEEN 1 AND 50000/);
  assert.match(adapters, /INSERT INTO private\.banking_pay_draft_frozen_certificate_scopes_v8/);
  assert.match(adapters, /ON CONFLICT \(operation_id\) DO NOTHING/);
  assert.match(adapters, /DRAFT_CERTIFICATE_SCOPE_INITIALISE_REPLAY_CONFLICT/);
  assert.match(adapters, /REVOKE ALL ON FUNCTION private\.banking_pay_draft_frozen_certificate_scope_initialise_v8\(uuid\)[\s\S]*FROM PUBLIC, anon, authenticated, service_role/i);
  assert.doesNotMatch(initialiseOwner, /GRANT EXECUTE ON FUNCTION private\.banking_pay_draft_frozen_certificate_scope_initialise_v8\(uuid\)[\s\S]*TO service_role/i);
});

test('existing timeout and lock budgets remain unchanged', () => {
  assert.match(source, /banking_pay_hot_path_budget_apply\('WORKBENCH_CHUNK'\)/);
  assert.doesNotMatch(source, /set_config\s*\(\s*['"](?:statement_timeout|lock_timeout|idle_in_transaction_session_timeout)/i);
  assert.doesNotMatch(source, /SET\s+(?:LOCAL\s+)?(?:statement_timeout|lock_timeout|idle_in_transaction_session_timeout)/i);
});

test('coordinator transports receipts only and contains no payment policy', () => {
  const executableSource = source.replace(/^\s*--.*$/gm, '');
  for (const forbidden of [
    'GROSS_ADD', 'GROSS_DEDUCT', 'NET_ADD', 'NET_DEDUCT',
    'OVERPAYMENT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT',
    'MANUAL_DEBT_RECOVERY', 'MANUAL_CREDIT_PAYOUT',
  ]) {
    assert.doesNotMatch(executableSource, new RegExp(`\\b${forbidden}\\b`));
  }
  assert.doesNotMatch(executableSource, /pay_batch_apply_finance_adjustments|pay_batch_insert_items_from_preview|provider|settlement|remittance/i);
});

test('response-loss continuation is derived only from committed receipt state', () => {
  assert.match(source, /ORDER BY receipt_row\.page_sequence DESC\s+LIMIT 1/);
  assert.match(source, /CASE WHEN v_last\.operation_id IS NULL THEN NULL ELSE v_last\.next_after_ordinal END/);
  assert.match(source, /CASE WHEN v_last\.operation_id IS NULL THEN NULL ELSE v_last\.receipt_digest_sha256 END/);
  assert.match(source, /v_scope\.freeze_state = 'FROZEN'[\s\S]*CERTIFICATE_STAGE_ALREADY_COMPLETE/);
});

test('staging refuses a mixed old/new operation that already entered a business phase', () => {
  assert.match(source, /DRAFT_CERTIFICATE_STAGE_ADVANCE_PHASE_MISMATCH/);
  assert.match(source, /'INITIALISE',[\s\S]*'CERTIFICATE_CONSTITUENT_REFS',[\s\S]*'CERTIFICATE_PARTITION_REFS',[\s\S]*'CANDIDATE_SCOPE',[\s\S]*'CERTIFICATE_FINAL_FREEZE'/);
  assert.match(runtime, /SET phase = 'INSERT_ITEMS'[\s\S]*DRAFT_CERTIFICATE_STAGE_ADVANCE_PHASE_MISMATCH/);
  assert.match(runtime, /phase mismatch wrote a staging receipt/);
});

test('dual-engine runtime fixture exercises multi-page member streams and final freeze', () => {
  assert.match(runtime, /FOR v_call IN 1\.\.8 LOOP/);
  assert.match(runtime, /'CERTIFICATE_CONSTITUENT_REFS', 513/);
  assert.match(runtime, /'CERTIFICATE_PARTITION_REFS', 513/);
  assert.match(runtime, /'CANDIDATE_SCOPE', 3/);
  assert.match(runtime, /page_call_count[\s\S]*freeze_call_count[\s\S]*<> 1/);
  assert.match(runtime, /phase FROM public\.banking_pay_operations[\s\S]*'DRAIN_TSFIN'/);
  assert.match(runtime, /CERTIFICATE_STAGE_ALREADY_COMPLETE/);
  assert.match(runtime, /ROLLBACK;[\s\S]*coordinator fixture rollback left durable rows/);
});

test('runtime fixture cannot accidentally exercise financial or provider owners', () => {
  const executableRuntime = runtime.replace(/^\s*--.*$/gm, '');
  assert.doesNotMatch(executableRuntime,
    /pay_batch_(?:insert|apply|finalize|finalise|execute|settle)|provider|remittance|bank_transfer/i);
});
