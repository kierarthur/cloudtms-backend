const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const ownerPath = path.join(
  root,
  'supabase',
  'repeatable',
  '02092026_2315_banking_pay_draft_constituent_parity_v8.sql'
);
const runtimePath = path.join(
  root,
  'tests',
  '02092026_2357_banking_pay_draft_constituent_parity_v8_runtime.sql'
);
const adapterPath = path.join(
  root,
  'supabase',
  'repeatable',
  '02092026_2311_banking_pay_draft_frozen_certificate_adapters_v8.sql'
);
const owner = fs.readFileSync(ownerPath, 'utf8');
const runtime = fs.readFileSync(runtimePath, 'utf8');
const adapter = fs.readFileSync(adapterPath, 'utf8');

test('constituent parity has one private comparator and one bounded service entry', () => {
  assert.equal((owner.match(/CREATE OR REPLACE FUNCTION/g) || []).length, 2);
  assert.match(owner, /private\.pay_workbench_draft_constituent_parity_compare_v8\(\s*p_operation_id uuid,\s*p_constituent_ordinal integer/);
  assert.match(owner, /public\.pay_workbench_draft_constituent_parity_page_v8\(\s*p_operation_id uuid,\s*p_after_constituent_ordinal integer DEFAULT NULL,\s*p_limit integer DEFAULT 256/);
  assert.match(owner, /p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 256/);
  assert.match(owner, /REVOKE ALL ON FUNCTION private\.pay_workbench_draft_constituent_parity_compare_v8\(uuid,integer\) FROM PUBLIC, anon, authenticated, service_role/);
  assert.match(owner, /REVOKE ALL ON FUNCTION public\.pay_workbench_draft_constituent_parity_page_v8\(uuid,integer,integer,text\) FROM PUBLIC, anon, authenticated/);
  assert.match(owner, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_draft_constituent_parity_page_v8\(uuid,integer,integer,text\) TO service_role/);
});

test('parity maps the certified constituent to actual owner artifacts by exact preview identity', () => {
  assert.match(owner, /frozen_ref\.constituent_ordinal = p_constituent_ordinal/);
  assert.match(owner, /entry\.certificate_uuid = frozen_ref\.certificate_uuid/);
  assert.match(owner, /member\.constituent_ordinal = p_constituent_ordinal/);
  assert.match(owner, /allocation_row\.allocation_basis_json->>'preview_row_id'/);
  assert.match(owner, /allocation_row\.allocation_basis_json#>>'\{line,preview_row_id\}'/);
  assert.match(owner, /= v_entry\.materialised_preview_row_id::text/);
  assert.match(owner, /linked_allocation\.pay_batch_item_id/);
  assert.match(owner, /reservation\.pay_batch_item_id = item\.id/);
  assert.match(owner, /workbench_settled_certificate_binding_v8/);
  assert.match(owner, /WORKBENCH_SETTLED_CERTIFICATE_BINDING_V8/);
});

test('the row-backed adapter carries only an exact certificate identity binding into owner input', () => {
  assert.match(adapter, /'workbench_settled_certificate_binding_v8', pg_catalog\.jsonb_build_object\(/);
  assert.match(adapter, /'binding_contract_version', 'WORKBENCH_SETTLED_CERTIFICATE_BINDING_V8'/);
  assert.match(adapter, /'certificate_uuid', entry\.certificate_uuid::text/);
  assert.match(adapter, /'constituent_digest_sha256', entry\.constituent_digest_sha256/);
  assert.match(adapter, /'constituent_ordinal', entry\.constituent_ordinal/);
  assert.match(adapter, /'source_identity_digest_sha256', entry\.source_identity_digest_sha256/);
  assert.doesNotMatch(adapter, /workbench_settled_certificate_binding_v8[\s\S]{0,800}(?:GROSS_ADD|GROSS_DEDUCT|NET_ADD|NET_DEDUCT|VAT_RATE|PAYMENT_ADVANCE_REPAYMENT|LOAN_REPAYMENT)/i);
});

test('parity compares identities and values but owns no payment policy', () => {
  for (const field of [
    'expected_item_amount_ex_vat',
    'expected_allocated_recovery_amount_ex_vat',
    'expected_reservation_amount_ex_vat',
    'expected_allocation_basis_kind',
    'expected_allocation_result',
    'expected_allocation_source_digest_sha256',
    'expected_item_semantic_kind',
    'expected_item_source_identity_digest_sha256',
    'expected_item_source_digest_sha256',
    'expected_reservation_applicability',
    'expected_reservation_source_digest_sha256',
    'operation_source_key',
    'pay_channel',
    'finance_case_id',
    'finance_component_id',
    'comparison_status'
  ]) {
    assert.match(owner, new RegExp(`\\b${field}\\b`));
  }
  assert.doesNotMatch(owner, /GROSS_ADD|GROSS_DEDUCT|NET_ADD|NET_DEDUCT|PAYMENT_ADVANCE_REPAYMENT|LOAN_REPAYMENT|MANUAL_DEBT_RECOVERY|VAT_RATE/i);
  assert.doesNotMatch(owner, /UPDATE\s+public\.(?:finance_cases|payment_advances|pay_batch_items|pay_advance_reservations)/i);
  assert.doesNotMatch(owner, /INSERT\s+INTO\s+public\.(?:pay_batch_items|pay_advance_reservations)/i);
  assert.match(
    owner,
    /abs\(v_allocation_total\)[\s\S]{0,240}abs\(v_entry\.expected_allocated_recovery_amount_ex_vat::numeric\)/i,
    'recovery headroom magnitude must be compared with the signed Draft effect without changing either owner'
  );
  assert.match(runtime, /expected_allocation_basis_kind='WORKBENCH_RECOVERY_HEADROOM_V1'/);
  assert.match(runtime, /expected_allocated_recovery_amount_ex_vat='25\.00'/);
  assert.match(runtime, /expected_allocation_result='ALLOCATED_WITHIN_CERTIFIED_HEADROOM'/);
  assert.match(
    runtime,
    /v_item_finance[^;]*'OVERPAYMENT_RECOVERY'[^;]*,-25,-5,-30,'UMBRELLA'/s,
    'the fixture must retain positive Workbench recovery capacity and a separately signed negative Draft item'
  );
});

test('mismatch and response-loss replay fail closed', () => {
  for (const code of [
    'DRAFT_PARITY_ACTUAL_OUTPUT_MISSING',
    'DRAFT_PARITY_ALLOCATION_IDENTITY_MISMATCH',
    'DRAFT_PARITY_CERTIFICATE_BINDING_MISMATCH',
    'DRAFT_PARITY_ALLOCATION_SOURCE_EVIDENCE_MISMATCH',
    'DRAFT_PARITY_ALLOCATION_AMOUNT_MISMATCH',
    'DRAFT_PARITY_ITEM_SOURCE_EVIDENCE_MISMATCH',
    'DRAFT_PARITY_ITEM_IDENTITY_OR_AMOUNT_MISMATCH',
    'DRAFT_PARITY_RESERVATION_MISMATCH',
    'DRAFT_PARITY_SOURCE_RESERVATION_EVIDENCE_MISMATCH',
    'DRAFT_PARITY_RECOVERY_ALLOCATION_MISMATCH',
    'ACTUAL_OUTPUT_CHANGED_AFTER_COMPARISON',
    'PAGE_REPLAY_CHANGED'
  ]) {
    assert.match(owner, new RegExp(code));
  }
  assert.doesNotMatch(
    owner,
    /DRAFT_PARITY_SOURCE_RESERVATION_AMOUNT_MISMATCH/,
    'a prior source reservation amount is evidence of prior occupancy, not the amount of the new residual Draft item'
  );
  assert.match(owner, /expected_reservation_amount_ex_vat/);
  assert.match(owner, /expected_reservation_source_digest_sha256/);
  assert.match(owner, /v_allocation_total/);
  assert.match(owner, /expected_item_amount_ex_vat/);
  assert.match(owner, /v_bad_reservation_count/);
  assert.match(owner, /expected_previous_receipt_sha256/);
  assert.match(owner, /receipt_digest_sha256/);
  assert.match(owner, /stage_kind constant text := 'CONSTITUENT_PARITY'/);
  assert.doesNotMatch(owner, /finance_case_id IS NOT NULL[\s\S]{0,500}ACTUAL_RESERVATION_MISSING/i);
});

test('dual-engine runtime fixture proves PAYE, Umbrella finance, replay, tamper and rollback', () => {
  assert.match(runtime, /v_candidate_paye/);
  assert.match(runtime, /'PAYE'/);
  assert.match(runtime, /v_candidate_umbrella/);
  assert.match(runtime, /'UMBRELLA'/);
  assert.match(runtime, /'OVERPAYMENT_RECOVERY'/);
  assert.match(runtime, /constituent_ordinal,staged_page_sequence/);
  assert.match(runtime, /\(v_operation_id,v_certificate_uuid,0,0\),\(v_operation_id,v_certificate_uuid,1,0\)/);
  assert.match(runtime, /replayed/i);
  assert.match(runtime, /DRAFT_PARITY_FACT_MISMATCH/);
  assert.match(runtime, /ROLLBACK\s*;/i);
  assert.doesNotMatch(runtime, /provider|settlement|remittance/i);
});

test('parity owner does not relax any database budget', () => {
  assert.doesNotMatch(owner, /SET\s+(?:LOCAL\s+)?(?:statement_timeout|lock_timeout|idle_in_transaction_session_timeout)/i);
  assert.doesNotMatch(owner, /^\s*(?:COMMIT|ROLLBACK)\s*;/im);
  assert.doesNotMatch(owner, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
