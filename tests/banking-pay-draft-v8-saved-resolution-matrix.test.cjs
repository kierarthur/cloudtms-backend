const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const readText = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const readJson = relativePath => JSON.parse(readText(relativePath));
const casesPath = 'tests/fixtures/banking-pay-draft-v8-saved-resolution-cases-v1.json';
const runnerPath = 'scripts/verify-banking-pay-draft-v8-saved-resolution-matrix.mjs';
const writerPath = 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql';
const consumerPath = 'supabase/repeatable/17082026_2052_pay_finance_resolution_cancel_authority.sql';

function validate(candidate) {
  assert.equal(candidate.contract, 'BANKING_PAY_DRAFT_V8_SAVED_RESOLUTION_CASES_V1');
  assert.equal(candidate.evidence_scope, 'WRITER_PRODUCED_CURRENT_OWNER_TO_CONSUMER_FIRST_DIVERGENCE');
  assert.match(candidate.v1_v8_typed_parity_status, /^OPEN_/);
  assert.equal(candidate.materialised_row_ceiling, 2);
  assert.equal(candidate.configured_scalar_ceiling, 50000);
  assert.equal(candidate.cases.length, 2);
  assert.deepEqual(candidate.cases.map(row => row.class_id).sort(), [
    'saved_payment_method_resolution','saved_rate_resolution'
  ]);
  assert.equal(new Set(candidate.cases.map(row => row.case_id)).size, 2);
  for (const row of candidate.cases) {
    assert.notEqual(row.candidate_target_pay_method, row.component_source_pay_method);
    assert.equal(row.expected_current_consumer_provenance, 'STALE_SAVED_RESOLUTION');
    assert.equal(row.expected_current_v8_outcome, 'TYPED_PRE_DRAFT_REJECTION_ZERO_DRAFT_WRITES');
    assert.ok(row.source_amount_ex_vat > 0);
    assert.ok(row.source_units > 0);
    assert.ok(row.source_rate > 0);
    assert.ok(row.source_charge_rate > 0);
    assert.ok(row.relevant_erni_pct >= 0);
    assert.ok(row.expected_target_amount_ex_vat > 0);
    assert.ok(row.expected_reconciled_recovery_amount_ex_vat > 0);
    assert.ok(row.expected_reconciled_recovery_amount_ex_vat <= row.source_amount_ex_vat);
    assert.equal(
      Number((row.expected_reconciled_recovery_amount_ex_vat + row.expected_reconciled_recovery_amount_vat).toFixed(2)),
      row.expected_reconciled_recovery_amount_inc_vat
    );
  }
}

test('saved-resolution cases bind exact writer-owned source, target, amount, rate, ERNI and VAT inputs without fabricating a fingerprint', () => {
  validate(readJson(casesPath));
  const runner = readText(runnerPath);
  for (const marker of [
    'pay_finance_component_resolutions_apply(',
    'H2_WRITER_PRODUCED_RESOLUTION_SHAPE_CHANGED',
    'H2_SAVED_RESOLUTION_FIRST_DIVERGENCE_CHANGED',
    'H2_V8_SAVED_RESOLUTION_REJECT=',
    'TARGET_PAY_METHOD_MISMATCH',
    'writer_consumer_notice'
  ]) assert.ok(runner.includes(marker), marker);
  assert.doesNotMatch(runner, /pay_finance_component_fingerprint\s*\(/i,
    'the fixture must consume the writer-produced fingerprint, never construct one');
  assert.doesNotMatch(runner, /generate_series\s*\(\s*1\s*,\s*50000\s*\)/i);
});

test('current source proves the writer and consumers use different fingerprint target-basis objects', () => {
  const writer = readText(writerPath);
  const consumer = readText(consumerPath);
  const writerStart = writer.indexOf('CREATE OR REPLACE FUNCTION public.pay_finance_component_resolutions_apply(');
  const writerEnd = writer.indexOf('\nCREATE OR REPLACE FUNCTION ', writerStart + 1);
  assert.ok(writerStart >= 0 && writerEnd > writerStart);
  const writerFunction = writer.slice(writerStart, writerEnd);
  assert.ok(writer.includes('p_target_basis_json         => v_target_basis_json'));
  assert.ok(writer.includes("'resolution_mode', v_resolution_mode::text"));
  assert.ok(writer.includes("'calculation_kind', v_resolution_mode::text"));
  assert.doesNotMatch(writerFunction, /'resolution_family'/,
    'the current sole writer does not emit the resolution family required later');
  assert.ok(consumer.includes("coalesce(pfc.saved_resolution_payload_json, pfc.saved_resolution_result_json, '{}'::jsonb)"));
  assert.ok(consumer.includes("then 'STALE_SAVED_RESOLUTION'"));
  assert.ok(consumer.includes("component_row.saved_resolution_payload_json->>'resolution_family'"));
  assert.ok(consumer.includes("component_row.saved_resolution_result_json->>'resolution_family'"));
  assert.ok(consumer.includes('WORKBENCH_FINANCE_RESOLUTION_OWNER_AMBIGUOUS'));
});

test('saved-resolution evidence remains an open Draft-output gate until runtime reaches a complete typed V8 Draft', () => {
  const candidate = JSON.parse(JSON.stringify(readJson(casesPath)));
  candidate.v1_v8_typed_parity_status = 'PARITY_PASS';
  assert.throws(() => validate(candidate));
  const falseSuccess = JSON.parse(JSON.stringify(readJson(casesPath)));
  falseSuccess.cases[0].expected_current_v8_outcome = 'FULL_TYPED_DRAFT_OUTPUT_PASS';
  assert.throws(() => validate(falseSuccess));
});
