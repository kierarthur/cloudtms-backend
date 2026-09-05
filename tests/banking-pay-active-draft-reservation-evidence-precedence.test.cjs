const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const historicalPath = path.join(root,
  'supabase/repeatable/01092026_1459_banking_pay_signed_recovery_draft_v1.sql');
const replacementPath = path.join(root,
  'supabase/repeatable/05092026_0655_banking_pay_active_draft_reservation_evidence_precedence_v1.sql');
const runtimePath = path.join(root,
  'tests/13082026_1942_banking_pay_james_rate_authority_runtime_verification.sql');
const runnerPath = path.join(root,
  'scripts/verify-banking-pay-draft-v1-v8-cancellation-parity.mjs');

const historical = fs.readFileSync(historicalPath, 'utf8');
const replacement = fs.readFileSync(replacementPath, 'utf8');
const runtime = fs.readFileSync(runtimePath, 'utf8');
const runner = fs.readFileSync(runnerPath, 'utf8');
const sha256 = (value) => crypto.createHash('sha256').update(value).digest('hex');

const requiredMarkers = [
  "normalized.evidence_origin='CASE_COMPONENT'",
  "='ORDINARY'",
  'normalized.component_fallback IS NULL',
  "normalized.evidence_json->'saved_resolution_payload_json' IS NULL",
  'normalized.nested_economic_key_type=normalized.economic_key_type',
  'normalized.nested_economic_key_value=normalized.economic_key_value',
  'normalized.evidence_source_pay_method=normalized.sealed_source_pay_method',
  "specific.evidence_origin='TOP_LEVEL_SOURCE_BASIS'",
  'specific.nested_allocation_row_id IS NOT NULL',
  "specific.source_basis_json->>'source'=",
  "'banking_pay_workbench_preview_rows'",
  'specific.nested_economic_key_type=specific.economic_key_type',
  'specific.nested_economic_key_value=specific.economic_key_value',
  'specific.nested_timesheet_id=specific.timesheet_id',
  'specific.allocated_amount_ex_vat,',
  'specific.nested_timesheet_id,',
  'specific.component_member_identity,',
  'specific.bucket_code',
  'precedence.precedence_key_json||jsonb_build_array(precedence.evidence_source_pay_method)',
  'normalized.timesheet_id,',
  'normalized.component_member_identity,',
  'normalized.bucket_code)',
  'normalized.evidence_source_pay_method)',
  "specific.source_basis_json->>'source_reservation_amount_ex_vat','')",
  "~ '^-?\\d+(\\.\\d+)?$'",
  "source_reservation_amount_ex_vat')::numeric,2)=",
  'ROUND(specific.parent_amount_ex_vat,2)=',
  'nested_top_level_allocation_precedence AS MATERIALIZED',
  'nested_top_level_allocation_precedence_any_method AS MATERIALIZED',
  'nested_top_level_allocation_precedence_exact_method AS MATERIALIZED',
  'precedence_any.precedence_key_json=jsonb_build_array(',
  'precedence_exact.precedence_key_json=jsonb_build_array(',
  "'RATE_AUTHORITY_NESTED_AMOUNT_OVERCONSUMED'",
  "'RATE_AUTHORITY_PARENT_COMPONENT_RECONCILIATION_MISMATCH'"
];

function validatesExactPrecedence(source) {
  return requiredMarkers.every((marker) => source.includes(marker));
}

test('historical owner remains byte-identical and replacement is one exact function', () => {
  assert.equal(sha256(historical),
    'ebbc86882312d06c1421b5dbe6abf62880f65beefe5ccf4bf7bb1d73c702b7b7');
  assert.equal((replacement.match(/CREATE OR REPLACE FUNCTION\s+private\.pay_workbench_sealed_rate_component_projection_v1\s*\(/g) || []).length, 1);
  assert.equal((replacement.match(/CREATE OR REPLACE FUNCTION\s+/g) || []).length, 1);
  assert.doesNotMatch(replacement, /pay_batch_finalize_reservations_and_markers\s*\(/);
  assert.match(replacement, /LANGUAGE sql\s+STABLE\s+PARALLEL UNSAFE\s+SECURITY DEFINER\s+SET search_path = ''/);
  assert.match(replacement, /OWNER TO postgres/);
  for (const role of ['PUBLIC', 'anon', 'authenticated', 'service_role']) {
    assert.ok(replacement.includes(`FROM ${role};`));
  }
  assert.ok(replacement.includes('TO postgres;'));
});

test('only an exact allocation-backed ordinary compatibility view is suppressed', () => {
  assert.equal(validatesExactPrecedence(replacement), true);
  assert.ok(replacement.includes("AS nested_economic_key_type"));
  assert.ok(replacement.includes("AS nested_economic_key_value"));
  assert.ok(replacement.includes("AS nested_timesheet_id"));
  assert.ok(replacement.includes("AS nested_allocation_row_id"));
  assert.doesNotMatch(replacement,
    /AND EXISTS\(\s*SELECT 1\s*FROM nested_evidence_digest specific\s*WHERE specific\.fact_identity=normalized\.fact_identity\s*AND specific\.evidence_origin='TOP_LEVEL_SOURCE_BASIS'/);
  assert.match(replacement,
    /nested_top_level_allocation_precedence AS MATERIALIZED[\s\S]*?SELECT DISTINCT specific\.fact_identity/);
});

test('runtime proof binds the red case and strict mismatch negatives', () => {
  for (const marker of [
    'CANCELLED_DRAFT_DUPLICATE_RESERVATION_VIEW_NOT_COLLAPSED',
    'CANCELLED_DRAFT_DIFFERENT_AMOUNT_DID_NOT_FAIL_CLOSED',
    'CANCELLED_DRAFT_DIFFERENT_KEY_DID_NOT_FAIL_CLOSED',
    'CANCELLED_DRAFT_MISSING_ALLOCATION_ID_DID_NOT_FAIL_CLOSED',
    'RATE_AUTHORITY_NESTED_AMOUNT_OVERCONSUMED',
    'ROLLBACK;'
  ]) assert.ok(runtime.includes(marker), marker);
});

test('cancellation oracle installs the exact candidate only under an explicit test flag', () => {
  assert.ok(runner.includes('H2_CANCEL_APPLY_RESERVATION_EVIDENCE_PRECEDENCE'));
  assert.ok(runner.includes('installReservationEvidencePrecedence(target)'));
  assert.ok(runner.includes('PGOPTIONS=-c statement_timeout=15s -c lock_timeout=1500ms'));
});

test('every boundary mutation is killed', () => {
  const mutations = requiredMarkers.map((marker, index) => {
    assert.equal(replacement.split(marker).length - 1 >= 1, true, marker);
    return replacement.replaceAll(marker, `H2_MUTATION_${index}`);
  });
  assert.equal(mutations.length, 34);
  assert.equal(mutations.filter(validatesExactPrecedence).length, 0);
});
