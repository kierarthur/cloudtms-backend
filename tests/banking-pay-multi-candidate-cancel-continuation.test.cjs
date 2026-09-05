const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8').replace(/\r\n/g, '\n');
const historical = read('supabase/repeatable/04082026_1158_pay_pre_bank_cancel_apply_work_item.sql');
const replacement = read('supabase/repeatable/04092026_2118_banking_pay_multi_candidate_cancel_continuation_v1.sql');

const sha256 = (text) => crypto.createHash('sha256').update(text).digest('hex');

function continuationViolations(source) {
  const required = [
    ["v_candidate_scope_contract_version = 1", 'legacy request-array branch'],
    ["v_candidate_scope_contract_version = 2", 'V2 normalized-membership branch'],
    ['public.pay_payment_correction_request_candidates AS request_member', 'immutable V2 request membership'],
    ['request_member.correction_request_id = v_work_item.correction_request_id', 'exact request binding'],
    ['request_work.selection_hash = request_member.candidate_scope_hash', 'work-to-membership hash binding'],
    ["request_work.work_kind = 'PRE_BANK_CANCEL'", 'exact cancellation work kind'],
    ["request_work.selection_json->>'source_correction_request_id' = v_work_item.correction_request_id::text", 'source request binding'],
    ["request_work.selection_json->>'source_row_count_semantics' = 'FINANCIAL_ONLY'", 'V2 financial-only semantics'],
    ['v_original_expected_valid_count = v_original_expected_json_count', 'duplicate/malformed membership rejection'],
    ['v_original_expected_belongs_count = v_original_expected_json_count', 'batch ownership proof'],
    ['v_original_expected_same_request_voided_count > 0', 'same-request sibling proof'],
    ['v_original_expected_disallowed_state_count = 0', 'foreign correction rejection'],
    ['v_active_outside_original_count = 0', 'complete active batch membership'],
    ['v_current_expected_outside_original_count = 0', 'current Candidate subset proof'],
    ['v_applied_sibling_count > 0', 'applied sibling proof'],
    ["v_request_membership_count = COALESCE(NULLIF(v_request.plan_json->>'selected_candidate_count', '')::integer, -1)", 'request-wide Candidate count proof'],
    ['v_request_membership_count > 1', 'multi-Candidate-only continuation'],
    ['v_request_membership_work_mismatch_count = 0', 'complete work-item mapping'],
    ["'NORMALIZED_REQUEST_CANDIDATE_MEMBERSHIP'", 'diagnostic source identity'],
  ];
  return required.filter(([needle]) => !source.includes(needle)).map(([, label]) => label);
}

test('historical policy owner remains byte-identical and replacement is one later exact owner', () => {
  assert.equal(sha256(historical), 'd71a4311cee098ced9e96e49dc3eef76a9a6a5e4d7eea7933f80eedc2011fd56');
  assert.equal((replacement.match(/CREATE OR REPLACE FUNCTION public\.pay_pre_bank_cancel_apply_work_item\s*\(/g) || []).length, 1);
  assert.equal((replacement.match(/CREATE OR REPLACE FUNCTION /g) || []).length, 1);
  assert.match(replacement, /LANGUAGE plpgsql\s+VOLATILE\s+SECURITY DEFINER/);
  assert.match(replacement, /SET search_path TO pg_catalog, private, extensions, pg_temp/);
  assert.match(replacement, /SET statement_timeout TO '6000ms'/);
  assert.match(replacement, /SET lock_timeout TO '1000ms'/);
  assert.match(replacement, /ALTER FUNCTION public\.pay_pre_bank_cancel_apply_work_item\(uuid,uuid\) OWNER TO postgres/);
  assert.match(replacement, /REVOKE ALL ON FUNCTION public\.pay_pre_bank_cancel_apply_work_item\(uuid,uuid\) FROM PUBLIC/);
  assert.match(replacement, /GRANT EXECUTE ON FUNCTION public\.pay_pre_bank_cancel_apply_work_item\(uuid,uuid\) TO service_role/);
});

test('V2 same-request continuation uses normalized immutable membership while legacy V1 remains explicit', () => {
  assert.deepEqual(continuationViolations(replacement), []);
  assert.match(replacement, /jsonb_array_elements_text\([\s\S]*v_request\.selection_json->'expected_pay_batch_item_ids'[\s\S]*WHERE v_candidate_scope_contract_version = 1/);
  assert.match(replacement, /CROSS JOIN LATERAL unnest\(request_member\.pay_batch_item_ids\)/);
  assert.match(replacement, /v_original_expected_active_count \+ v_original_expected_same_request_voided_count = v_original_expected_json_count/);
});

test('continuation remains a narrow classification exception and cannot bypass ordinary safety gates', () => {
  const classifierIndex = replacement.indexOf("v_classification IN ('PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION', 'CANCELLED_BEFORE_BANK_SUBMISSION')");
  const mutationIndex = replacement.indexOf('UPDATE public.pay_batch_items AS items_to_void');
  assert.ok(classifierIndex > 0);
  assert.ok(mutationIndex > classifierIndex);
  assert.match(replacement, /PRE_BANK_CANCEL_CLASSIFICATION_REQUIRED/);
  assert.match(replacement, /SELECT_FULL_UNPAID_PAYMENT_SCOPE_REQUIRED/);
  assert.match(replacement, /SOURCE_SCOPE_CHANGED/);
  assert.match(replacement, /COMMUNICATION_CLEANUP_UNSAFE/);
  assert.ok(replacement.indexOf('public._pay_payment_movement_classify') < classifierIndex);
  assert.match(replacement, /v_has_settlement_evidence/);
  assert.match(replacement, /v_has_bank_submission_evidence/);
});

test('each continuation safety boundary has a mutation that the source validator kills', () => {
  const operators = [
    "v_candidate_scope_contract_version = 1",
    "v_candidate_scope_contract_version = 2",
    'public.pay_payment_correction_request_candidates AS request_member',
    'request_member.correction_request_id = v_work_item.correction_request_id',
    'request_work.selection_hash = request_member.candidate_scope_hash',
    "request_work.work_kind = 'PRE_BANK_CANCEL'",
    "request_work.selection_json->>'source_correction_request_id' = v_work_item.correction_request_id::text",
    "request_work.selection_json->>'source_row_count_semantics' = 'FINANCIAL_ONLY'",
    'v_original_expected_valid_count = v_original_expected_json_count',
    'v_original_expected_belongs_count = v_original_expected_json_count',
    'v_original_expected_same_request_voided_count > 0',
    'v_original_expected_disallowed_state_count = 0',
    'v_active_outside_original_count = 0',
    'v_current_expected_outside_original_count = 0',
    'v_applied_sibling_count > 0',
    "v_request_membership_count = COALESCE(NULLIF(v_request.plan_json->>'selected_candidate_count', '')::integer, -1)",
    'v_request_membership_count > 1',
    'v_request_membership_work_mismatch_count = 0',
  ];
  for (const operator of operators) {
    const mutated = replacement.replaceAll(operator, `REMOVED_${crypto.randomUUID()}`);
    assert.notEqual(mutated, replacement, `mutation target missing: ${operator}`);
    assert.ok(continuationViolations(mutated).length > 0, `mutation survived: ${operator}`);
  }
});

test('replacement adds no timeout relaxation, overload, policy vocabulary or second economic owner', () => {
  assert.doesNotMatch(replacement, /SET statement_timeout TO '(?:[7-9]|\d{2,})\d{3}ms'/);
  assert.doesNotMatch(replacement, /CREATE OR REPLACE FUNCTION public\.pay_pre_bank_cancel_apply_work_item\s*\([^)]*,[^)]*,/);
  assert.doesNotMatch(replacement, /new payment policy|recalculate payment amount|override pay channel/i);
  assert.equal((replacement.match(/pay_payment_correction_request_candidates AS request_member/g) || []).length, 2);
});
