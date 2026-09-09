import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const testDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(testDir, '..');
const fixturePath = path.join(testDir, 'fixtures', '07092026_2030_h12_source_less_cancellation_prechange_v1.json');
const fixture = JSON.parse(readFileSync(fixturePath, 'utf8'));

function read(relativePath) {
  return readFileSync(path.join(repoRoot, ...relativePath.split('/')), 'utf8');
}

function hash(relativePath) {
  return createHash('sha256').update(readFileSync(path.join(repoRoot, ...relativePath.split('/')))).digest('hex');
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function classifySourceLess(input) {
  if (input.amount_inc_vat === null || input.amount_inc_vat === undefined) return ['SOURCE_LESS_AMBIGUOUS', 'MISSING_AMOUNT_INC_VAT'];
  if (Math.round(Number(input.amount_inc_vat) * 100) === 0) return ['SOURCE_LESS_AMBIGUOUS', 'ZERO_AMOUNT'];
  if (input.amount_ex_vat === null || input.amount_ex_vat === undefined) return ['SOURCE_LESS_AMBIGUOUS', 'MISSING_AMOUNT_EX_VAT'];
  if (input.amount_vat === null || input.amount_vat === undefined) return ['SOURCE_LESS_AMBIGUOUS', 'MISSING_AMOUNT_VAT'];
  if (!String(input.description ?? '').trim()) return ['SOURCE_LESS_AMBIGUOUS', 'MISSING_DESCRIPTION'];
  if (!input.candidate_id_present) return ['SOURCE_LESS_AMBIGUOUS', 'MISSING_CANDIDATE_CONTEXT'];
  if (!['PAYE', 'UMBRELLA'].includes(String(input.pay_channel ?? '').trim().toUpperCase())) {
    return ['SOURCE_LESS_AMBIGUOUS', 'UNSUPPORTED_OR_MISSING_PAY_CHANNEL'];
  }
  if (String(input.pay_channel).trim().toUpperCase() === 'UMBRELLA' && !input.umbrella_payee_present) {
    return ['SOURCE_LESS_AMBIGUOUS', 'MISSING_UMBRELLA_PAYEE_CONTEXT'];
  }
  return ['SOURCE_LESS_CARRY_FORWARD_SAFE', null];
}

function validate(candidate) {
  assert.equal(candidate.artifact, 'H12_SOURCE_LESS_CANCELLATION_PRECHANGE_EVIDENCE_V1');
  assert.equal(candidate.mode, 'TEST_AND_EVIDENCE_ONLY_PRECHANGE_GUARD');
  assert.equal(candidate.implementation_status, 'NOT_IMPLEMENTED');
  assert.deepEqual(candidate.supported_user_scope, ['ENTIRE_CANDIDATE_PAYMENT', 'ENTIRE_DRAFT_OR_RUN']);

  assert.equal(candidate.c016.prechange_status, 'PROVED_CURRENT_POLICY_DIVERGENCE_NOT_FIXED');
  assert.equal(candidate.c016.current_veto.code, 'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS');
  assert.equal(candidate.c016.current_veto.owners.length, 2);
  assert.equal(candidate.c016.detector_contract.source_less_safe_requires.length, 8);
  assert.equal(candidate.c016.detector_contract.ambiguity_reasons.length, 8);
  assert.equal(candidate.c016.current_safe_path.can_carry_forward_automatically, true);
  assert.equal(candidate.c016.current_alert_lifecycle.after_source_batch_cancelled, 'DISAPPEARS');
  assert.equal(candidate.c016.current_alert_lifecycle.durable_post_cancellation_investigation_source, false);
  assert.equal(candidate.c016.future_correction_guard_only.automatic_carry_forward_for_ambiguous_item, false);
  assert.equal(candidate.c016.future_correction_guard_only.implementation_authorised, false);

  assert.equal(candidate.c020.corrected_classification,
    'CANONICAL_UNPAID_GENERATION_UNREACHABLE_HOSTILE_OR_LEGACY_FAIL_CLOSED');
  assert.equal(candidate.c020.canonical_unpaid_generation.can_emit_other_work_kind, false);
  assert.deepEqual(candidate.c020.table_allowed_work_kinds, ['PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL']);
  assert.equal(candidate.c020.separate_paid_owner.owner, 'public.pay_settled_payment_reversal_apply_work_item');
  assert.equal(candidate.c020.unsupported_branch.code, 'BLOCKED_BY_UNSUPPORTED_SOURCE');
  assert.equal(candidate.c020.blocks_canonical_confirmed_unpaid_cancellation, false);
  assert.equal(candidate.c020.implementation_authorised, false);
  assert.match(candidate.c020.relationship_to_c016, /^NONE\./);
}

test('fixture binds the exact clean source identity and leaves every production owner unchanged', () => {
  validate(fixture);
  for (const sourceFile of fixture.source_files) {
    assert.equal(readFileSync(path.join(repoRoot, ...sourceFile.path.split('/'))).byteLength, sourceFile.bytes);
    assert.equal(hash(sourceFile.path), sourceFile.sha256);
  }
});

test('current PRE_BANK and NO_MONEY routines block source-less ambiguity before cancellation writes', () => {
  for (const relativePath of [
    'supabase/repeatable/04092026_2118_banking_pay_multi_candidate_cancel_continuation_v1.sql',
    'supabase/repeatable/04082026_1158_pay_no_money_unwind_apply_work_item.sql'
  ]) {
    const sql = read(relativePath);
    const detectAt = sql.indexOf('v_manual_adjustment_result := public._pay_detect_manual_adjustments_for_carry_forward');
    const ambiguityAt = sql.indexOf("'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS'", detectAt);
    const returnAt = sql.indexOf("RETURN jsonb_build_object('ok', false, 'status', 'BLOCKED'", ambiguityAt);
    const carryAt = sql.indexOf('v_carry_forward_create_result := public._pay_manual_adjustment_carry_forward_create', ambiguityAt);
    const ledgerAt = sql.indexOf('INSERT INTO public.pay_payment_correction_items', carryAt);
    const voidAt = sql.indexOf('UPDATE public.pay_batch_items AS items_to_void', ledgerAt);
    assert.ok(detectAt >= 0 && ambiguityAt > detectAt && returnAt > ambiguityAt);
    assert.ok(carryAt > returnAt && ledgerAt > carryAt && voidAt > ledgerAt);
    assert.match(sql.slice(detectAt, carryAt), /carry_forward_blockers/);
    assert.match(sql.slice(detectAt, carryAt), /can_carry_forward_automatically/);
  }
});

test('complete PAYE and Umbrella source-less facts enter the existing safe carry-forward path', () => {
  for (const row of fixture.c016.deterministic_cases) {
    const [classification, reason] = classifySourceLess(row.input);
    assert.equal(classification, row.expected_classification, row.case_id);
    assert.equal(reason, row.expected_reason ?? null, row.case_id);
  }
  const detector = read('supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
  for (const token of fixture.c016.detector_contract.source_less_safe_requires) {
    const sourceToken = {
      NON_NULL_AMOUNT_INC_VAT: 'scoped_items.amount_inc_vat IS NOT NULL',
      NON_ZERO_ROUNDED_AMOUNT_INC_VAT: 'round(scoped_items.amount_inc_vat, 2) <> 0',
      NON_NULL_AMOUNT_EX_VAT: 'scoped_items.amount_ex_vat IS NOT NULL',
      NON_NULL_AMOUNT_VAT: 'scoped_items.amount_vat IS NOT NULL',
      NON_BLANK_DESCRIPTION: "NULLIF(btrim(COALESCE(scoped_items.description, '')), '') IS NOT NULL",
      CANDIDATE_ID: 'scoped_items.candidate_id IS NOT NULL',
      PAY_CHANNEL_PAYE_OR_UMBRELLA: "IN ('PAYE', 'UMBRELLA')",
      UMBRELLA_PAYEE_CONTEXT_WHEN_UMBRELLA: 'scoped_items.effective_umbrella_id IS NOT NULL'
    }[token];
    assert.ok(detector.includes(sourceToken), token);
  }
  assert.match(detector, /THEN 'SOURCE_LESS_CARRY_FORWARD_SAFE'/);
  assert.match(detector, /'manual_adjustment_carry_forward_required', v_source_less_safe_count > 0/);
  assert.match(detector, /'can_carry_forward_automatically', v_source_less_ambiguous_count = 0/);
});

test('all eight incomplete-fact classifications remain explicit and deterministic', () => {
  const base = {
    amount_inc_vat: '-25.00', amount_ex_vat: '-25.00', amount_vat: '0.00',
    description: 'Frozen manual deduction', candidate_id_present: true,
    pay_channel: 'PAYE', umbrella_payee_present: false
  };
  const cases = [
    [{ ...base, amount_inc_vat: null }, 'MISSING_AMOUNT_INC_VAT'],
    [{ ...base, amount_inc_vat: '0.001' }, 'ZERO_AMOUNT'],
    [{ ...base, amount_ex_vat: null }, 'MISSING_AMOUNT_EX_VAT'],
    [{ ...base, amount_vat: null }, 'MISSING_AMOUNT_VAT'],
    [{ ...base, description: ' ' }, 'MISSING_DESCRIPTION'],
    [{ ...base, candidate_id_present: false }, 'MISSING_CANDIDATE_CONTEXT'],
    [{ ...base, pay_channel: 'OTHER' }, 'UNSUPPORTED_OR_MISSING_PAY_CHANNEL'],
    [{ ...base, pay_channel: 'UMBRELLA', umbrella_payee_present: false }, 'MISSING_UMBRELLA_PAYEE_CONTEXT']
  ];
  for (const [input, expectedReason] of cases) {
    assert.deepEqual(classifySourceLess(input), ['SOURCE_LESS_AMBIGUOUS', expectedReason]);
  }
});

test('current manual-adjustment alert source is excluded once its source batch is CANCELLED', () => {
  const sql = read('supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
  assert.match(sql, /CREATE OR REPLACE FUNCTION public\.banking_alerts_active_for_user\(/);
  assert.match(sql, /carry_forward_candidate\.source_pay_batch_id AS pay_batch_id[\s\S]{0,900}carry_forward_candidate_batch\.status[\s\S]{0,120}NOT IN \('CANCELLED','CANCELED'\)/i);
  assert.match(sql, /'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'[\s\S]{0,1300}SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS/);
  assert.match(sql, /'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'[\s\S]{0,700}'GROUPED'[\s\S]{0,700}'ACTIVE'/);
  assert.doesNotMatch(sql, /MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED/);
});

test('C020 cannot be emitted by canonical unpaid work expansion and remains fail closed', () => {
  const expand = read('supabase/repeatable/04082026_1208_pay_payment_correction_expand_work.sql');
  const process = read('supabase/repeatable/04092026_2350_banking_pay_cancellation_completion_v1.sql');
  const schema = read('supabase/migrations/30042026_1341_newcolumns_reverse_payments.sql');
  const paidOwner = read('supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
  assert.match(expand, /v_work_kind := CASE WHEN v_request\.correction_kind = 'NO_MONEY_UNWIND'[\s\S]{0,100}THEN 'NO_MONEY_UNWIND' ELSE 'PRE_BANK_CANCEL' END/);
  assert.match(process, /IF v_work\.work_kind = 'PRE_BANK_CANCEL'[\s\S]{0,1800}ELSIF v_work\.work_kind = 'NO_MONEY_UNWIND'[\s\S]{0,700}BLOCKED_BY_UNSUPPORTED_SOURCE/);
  assert.match(schema, /CHECK \(work_kind IN \([\s\S]{0,150}'PRE_BANK_CANCEL'[\s\S]{0,100}'NO_MONEY_UNWIND'[\s\S]{0,100}'SETTLED_REVERSAL'/);
  assert.match(paidOwner, /CREATE OR REPLACE FUNCTION public\.pay_settled_payment_reversal_apply_work_item\(/);
  assert.match(paidOwner, /PAID_SETTLED_RECOVERY_REQUIRED/);
  assert.match(paidOwner, /AMEND_AND_RECOVER_OVERPAYMENT/);
});

test('mutation: C016 cannot be recast as acceptable pre-change behavior', () => {
  const mutated = clone(fixture);
  mutated.c016.prechange_status = 'PROVED_CORRECT';
  assert.throws(() => validate(mutated));
});

test('mutation: missing-source cancellation cannot silently invent a carry-forward', () => {
  const mutated = clone(fixture);
  mutated.c016.future_correction_guard_only.automatic_carry_forward_for_ambiguous_item = true;
  assert.throws(() => validate(mutated));
});

test('mutation: C020 cannot become an automatic unpaid adapter', () => {
  const mutated = clone(fixture);
  mutated.c020.canonical_unpaid_generation.can_emit_other_work_kind = true;
  assert.throws(() => validate(mutated));
});

test('mutation: C020 cannot be said to block canonical confirmed-unpaid cancellation', () => {
  const mutated = clone(fixture);
  mutated.c020.blocks_canonical_confirmed_unpaid_cancellation = true;
  assert.throws(() => validate(mutated));
});
