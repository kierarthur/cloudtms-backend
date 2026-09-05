const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');

const base = Object.freeze({
  migration: read('supabase/migrations/02092026_2300_banking_pay_workbench_settled_certificate_v8.sql'),
  build: read('supabase/repeatable/02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql'),
  reader: read('supabase/repeatable/02092026_2302_banking_pay_workbench_settled_certificate_digest_reader_v8.sql'),
  parity: read('supabase/repeatable/02092026_2315_banking_pay_draft_constituent_parity_v8.sql'),
  insertItems: read('supabase/repeatable/02092026_1040_banking_pay_draft_insert_items_finance_handoff_v1.sql'),
  paired: read('supabase/repeatable/21072026_1235_05_timesheet_correction_chain_scope_v1.sql'),
  scale: read('tests/03092026_1010_banking_pay_workbench_settled_certificate_v8_scale_verification.sql')
});

function occurrences(source, text) {
  return source.split(text).length - 1;
}

function guards(source) {
  return occurrences(source.migration, 'UNIQUE (certificate_uuid, preview_row_id)') === 2
    && source.migration.includes('UNIQUE (certificate_uuid, constituent_ordinal)')
    && source.build.includes("'amount_sign', CASE WHEN v_amount < 0 THEN 'NEGATIVE' WHEN v_amount > 0 THEN 'POSITIVE' ELSE 'ZERO' END")
    && occurrences(source.build, "'prior_paid_amount_ex_vat', v_prior_paid") === 2
    && occurrences(source.build, "'source_identity_digest_sha256', v_source_identity_digest") === 2
    && source.build.includes('AND UPPER(fact.economic_key_type)=v_key_type')
    && source.build.includes('v_session.scope_change_generation_target <> v_session.scope_change_generation_applied')
    && source.build.includes('v_session.scope_change_generation_target <> v_session.scope_change_generation_shadow_checked')
    && occurrences(source.reader, 'AND receipt.page_digest_sha256=p_expected_previous_receipt_sha256') === 3
    && source.parity.includes('v_candidate_scope.resolved_pay_channel IS DISTINCT FROM v_entry.resolved_pay_channel')
    && source.parity.includes('pg_catalog.round(v_allocation_total, 2) IS DISTINCT FROM pg_catalog.round(v_entry.expected_item_amount_ex_vat::numeric, 2)')
    && source.parity.includes("allocation_projection.certificate_binding_json->>'source_identity_digest_sha256' IS DISTINCT FROM v_entry.expected_item_source_identity_digest_sha256")
    && source.insertItems.includes('normalised_source_rows.amount_ex_vat,\n        v_vat_rate_pct,')
    && source.insertItems.includes("WHEN 'PAYMENT_ADVANCE_REPAYMENT' THEN 'LOAN_REPAYMENT'")
    && !source.insertItems.includes("v_deferred_finance_aliases constant text[] := ARRAY[\n    'LOAN_REPAYMENT'")
    && source.paired.includes("u.actual_member_count=(u.envelope->>'expected_member_count')::integer")
    && source.paired.includes("u.reversal_count=1")
    && source.paired.includes("u.replacement_count=case when u.envelope->>'correction_shape'='REVERSAL_ONLY' then 0 else 1 end")
    && source.scale.includes("RAISE EXCEPTION 'H1_V8_ENTRY_STREAM_COUNT_MISMATCH'")
    && source.scale.includes("RAISE EXCEPTION 'H1_V8_PARTITION_STREAM_COUNT_MISMATCH'");
}

function changed(source, file, from, to) {
  assert.ok(source[file].includes(from), `${file} mutation preimage is missing`);
  return { ...source, [file]: source[file].replace(from, to) };
}

const mutations = Object.freeze([
  ['DROP_SELECTED_CONSTITUENT', 'scale', "RAISE EXCEPTION 'H1_V8_ENTRY_STREAM_COUNT_MISMATCH'", "RAISE EXCEPTION 'IGNORED_ENTRY_STREAM_COUNT'"],
  ['DUPLICATE_SELECTED_CONSTITUENT', 'migration', 'UNIQUE (certificate_uuid, preview_row_id)', '/* uniqueness removed */'],
  ['FLIP_PAY_CHANNEL', 'parity', 'v_candidate_scope.resolved_pay_channel IS DISTINCT FROM v_entry.resolved_pay_channel', 'v_candidate_scope.resolved_pay_channel IS NOT DISTINCT FROM v_entry.resolved_pay_channel'],
  ['FLIP_SIGN', 'build', "'amount_sign', CASE WHEN v_amount < 0 THEN 'NEGATIVE' WHEN v_amount > 0 THEN 'POSITIVE' ELSE 'ZERO' END", "'amount_sign', 'POSITIVE'"],
  ['CHANGE_AMOUNT_BY_ONE_PENNY', 'parity', 'pg_catalog.round(v_allocation_total, 2) IS DISTINCT FROM pg_catalog.round(v_entry.expected_item_amount_ex_vat::numeric, 2)', 'pg_catalog.round(v_allocation_total + 0.01, 2) IS DISTINCT FROM pg_catalog.round(v_entry.expected_item_amount_ex_vat::numeric, 2)'],
  ['CHANGE_VAT_BY_ONE_PENNY', 'insertItems', 'normalised_source_rows.amount_ex_vat,\n        v_vat_rate_pct,', 'normalised_source_rows.amount_ex_vat + 0.01,\n        v_vat_rate_pct,'],
  ['CHANGE_ECONOMIC_KEY', 'build', 'AND UPPER(fact.economic_key_type)=v_key_type', 'AND UPPER(fact.economic_key_type)<>v_key_type'],
  ['CHANGE_SOURCE_IDENTITY_DIGEST', 'parity', "allocation_projection.certificate_binding_json->>'source_identity_digest_sha256' IS DISTINCT FROM v_entry.expected_item_source_identity_digest_sha256", "allocation_projection.certificate_binding_json->>'source_identity_digest_sha256' IS NOT DISTINCT FROM v_entry.expected_item_source_identity_digest_sha256"],
  ['DROP_PARTITION_MEMBER', 'migration', 'UNIQUE (certificate_uuid, constituent_ordinal)', '/* partition membership uniqueness removed */'],
  ['CHANGE_PRIOR_PAID_RESIDUAL', 'build', "'prior_paid_amount_ex_vat', v_prior_paid", "'prior_paid_amount_ex_vat', '0.00'"],
  ['REUSE_STALE_PAGE_RECEIPT', 'reader', 'AND receipt.page_digest_sha256=p_expected_previous_receipt_sha256', 'AND TRUE /* stale receipt accepted */'],
  ['CHANGE_SCOPE_GENERATION', 'build', 'v_session.scope_change_generation_target <> v_session.scope_change_generation_applied', 'FALSE /* changed generation accepted */'],
  ['SUBSTITUTE_HIDDEN_FINANCE_ALIAS', 'insertItems', "v_deferred_finance_aliases constant text[] := ARRAY[\n    'OVERPAYMENT_RECOVERY'", "v_deferred_finance_aliases constant text[] := ARRAY[\n    'LOAN_REPAYMENT',\n    'OVERPAYMENT_RECOVERY'"],
  ['DROP_PAIRED_TIMESHEET_LEG', 'paired', "u.replacement_count=case when u.envelope->>'correction_shape'='REVERSAL_ONLY' then 0 else 1 end", 'u.replacement_count=0']
]);

test('all 14 bounded Banking Pay mutation operators are killed by exact source guards', () => {
  assert.equal(guards(base), true, 'the unmodified candidate must satisfy every zero-drift guard');
  const killed = [];
  for (const [operator, file, from, to] of mutations) {
    const mutant = changed(base, file, from, to);
    assert.equal(guards(mutant), false, `${operator} survived`);
    killed.push(operator);
  }
  assert.equal(killed.length, 14);
  assert.equal(new Set(killed).size, 14);
});

test('the mutation suite exactly matches the frozen parity-matrix operator catalogue', () => {
  const matrix = JSON.parse(read('tests/fixtures/banking-pay-create-draft-v8-policy-parity-matrix-v1.json'));
  assert.deepEqual(mutations.map(([operator]) => operator), matrix.mutation_operators);
});
