import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = (path) => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const prepare = read('supabase/repeatable/04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql');
const expand = read('supabase/repeatable/04082026_1208_pay_payment_correction_expand_work.sql');
const preBank = read('supabase/repeatable/04082026_1158_pay_pre_bank_cancel_apply_work_item.sql');
const noMoney = read('supabase/repeatable/04082026_1158_pay_no_money_unwind_apply_work_item.sql');
const sourceBuild = read('supabase/repeatable/07082026_1013_pay_workbench_candidate_source_build_chunk.sql');
const failJob = read('supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql');

test('new cancellation requests freeze financial-only candidate scope V2 without reinterpreting legacy work', () => {
  assert.match(prepare, /v_request\.status = 'PLANNING'[\s\S]*pay_payment_correction_request_candidates[\s\S]*PREPARE_SELECTION[\s\S]*v_candidate_scope_contract_version := 2/);
  assert.match(prepare, /ELSE\s+v_candidate_scope_contract_version := 1;/);
  assert.match(prepare, /'candidate_scope_contract_version', v_candidate_scope_contract_version/);
  assert.match(prepare, /'source_row_count_semantics', v_source_row_count_semantics/);
  assert.match(prepare, /'communication_cleanup_contract_version', v_communication_cleanup_contract_version/);
  assert.match(prepare, /PAYMENT_CORRECTION_CANDIDATE_SCOPE_V2/);
  assert.match(prepare, /IF v_candidate_scope_contract_version = 1 THEN\s+v_source_row_count := v_source_row_count \+ v_matching_queued_count;/);
  assert.doesNotMatch(
    prepare,
    /IF v_candidate_scope_contract_version = 2 THEN\s+v_source_row_count := v_source_row_count \+ v_matching_queued_count;/,
  );
});

test('expanded work preserves and validates the frozen request authority', () => {
  assert.match(expand, /candidate_scope_contract_version/);
  assert.match(expand, /candidate_scope_hash_version/);
  assert.match(expand, /source_row_count_semantics/);
  assert.match(expand, /communication_cleanup_contract_version/);
  assert.match(expand, /PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED/);
  assert.match(expand, /PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH/);
  assert.match(expand, /work_row\.candidate_id IS DISTINCT FROM candidate_row\.candidate_id/);
  assert.match(expand, /work_row\.selection_hash IS DISTINCT FROM member_row\.candidate_scope_hash/);
});

for (const [name, source] of [
  ['pre-bank cancellation', preBank],
  ['no-money unwind', noMoney],
]) {
  test(`${name} keeps volatile communications out of V2 financial identity and fails closed before mutation`, () => {
    assert.match(source, /candidate_scope_contract_version/);
    assert.match(source, /PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED/);
    assert.match(source, /PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH/);
    assert.match(source, /COMMUNICATION_CLEANUP_UNSAFE/);
    assert.match(source, /matching_queued_count/);
    assert.match(source, /already_sent_untouched_count/);
    assert.match(source, /other_terminal_untouched_count/);
    assert.match(source, /IF v_candidate_scope_contract_version = 1 THEN/);
    assert.match(source, /PAYMENT_CORRECTION_CANDIDATE_SCOPE_V2/);

    const unsafe = source.indexOf("'code', 'COMMUNICATION_CLEANUP_UNSAFE'");
    const mutation = source.slice(unsafe).search(/\n\s*(?:DELETE FROM|UPDATE public\.)/);
    assert.ok(unsafe >= 0, 'typed communication blocker must exist');
    assert.ok(mutation > 0, 'communication safety must be established before financial mutation');
  });
}

test('workbench rebuild has one frozen-scope projection for both correction consumers', () => {
  assert.match(sourceBuild, /CREATE TEMP TABLE _bpay_wb_open_correction_scope_v1 ON COMMIT DROP AS/);
  assert.equal(
    (sourceBuild.match(/FROM pg_temp\._bpay_wb_open_correction_scope_v1 AS correction_scope/g) || []).length,
    2,
  );
  assert.equal(
    (sourceBuild.match(/public\._pay_payment_correction_selected_items\(/g) || []).length,
    1,
    'the compatibility helper may remain only in the no-membership legacy fallback',
  );
  assert.match(sourceBuild, /PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISSING/);
  assert.match(sourceBuild, /PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH/);
  assert.match(sourceBuild, /PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED/);
  assert.match(sourceBuild, /correction_request\.status IN \([\s\S]*'BLOCKED'[\s\S]*\)/);
});

test('deterministic frozen-scope violations terminalise instead of retry-looping', () => {
  for (const code of [
    'PAYMENT_CORRECTION_SCOPE_TYPE_REQUIRED',
    'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISSING',
    'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH',
    'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED',
  ]) {
    assert.ok(failJob.includes(`'${code}'`), `missing deterministic classifier: ${code}`);
  }
  assert.match(failJob, /v_is_deterministic_stage_error/);
});

test('the patch does not add live post-Draft economic fallback or alter provider routing', () => {
  for (const source of [prepare, expand, preBank, noMoney, sourceBuild, failJob]) {
    assert.doesNotMatch(source, /min\s*\([^)]*::uuid/i);
  }
  assert.doesNotMatch(sourceBuild, /live finance-component identity fallback/i);
  assert.doesNotMatch(sourceBuild, /provider[_ ]route/i);
});
