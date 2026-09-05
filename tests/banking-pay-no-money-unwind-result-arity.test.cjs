const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const sourcePath = path.resolve(
  __dirname,
  '../supabase/repeatable/04082026_1158_pay_no_money_unwind_apply_work_item.sql'
);
const source = fs.readFileSync(sourcePath, 'utf8');
const parityHarness = fs.readFileSync(
  path.resolve(__dirname, '../scripts/verify-banking-pay-draft-v1-v8-cancellation-parity.mjs'),
  'utf8'
);
const installedVerification = fs.readFileSync(
  path.resolve(
    __dirname,
    '../supabase/verification/05092026_1415_banking_pay_no_money_unwind_result_arity_verification.sql'
  ),
  'utf8'
);
const auditHelperSource = fs.readFileSync(
  path.resolve(__dirname, '../supabase/baseline/22082026_1503_cloudtms_test_routines_00.sql'),
  'utf8'
);

function readBalancedCall(text, callStart) {
  const open = text.indexOf('(', callStart);
  assert.ok(open >= 0, 'jsonb_build_object opening parenthesis is missing');
  let depth = 0;
  let dollarTag = null;
  let quote = null;
  let lineComment = false;
  let blockComment = false;
  let topLevelCommas = 0;

  for (let index = open; index < text.length; index += 1) {
    const current = text[index];
    const next = text[index + 1];

    if (lineComment) {
      if (current === '\n') lineComment = false;
      continue;
    }
    if (blockComment) {
      if (current === '*' && next === '/') {
        blockComment = false;
        index += 1;
      }
      continue;
    }
    if (dollarTag) {
      if (text.startsWith(dollarTag, index)) {
        index += dollarTag.length - 1;
        dollarTag = null;
      }
      continue;
    }
    if (quote) {
      if (current === quote && next === quote) {
        index += 1;
      } else if (current === quote) {
        quote = null;
      }
      continue;
    }
    if (current === '-' && next === '-') {
      lineComment = true;
      index += 1;
      continue;
    }
    if (current === '/' && next === '*') {
      blockComment = true;
      index += 1;
      continue;
    }
    if (current === "'" || current === '"') {
      quote = current;
      continue;
    }
    if (current === '$') {
      const match = text.slice(index).match(/^\$[A-Za-z0-9_]*\$/);
      if (match) {
        dollarTag = match[0];
        index += dollarTag.length - 1;
        continue;
      }
    }
    if (current === '(') {
      depth += 1;
      continue;
    }
    if (current === ')') {
      depth -= 1;
      if (depth === 0) {
        const body = text.slice(open + 1, index);
        return {
          body,
          end: index + 1,
          argumentCount: body.trim() === '' ? 0 : topLevelCommas + 1
        };
      }
      continue;
    }
    if (current === ',' && depth === 1) topLevelCommas += 1;
  }
  assert.fail('jsonb_build_object closing parenthesis is missing');
}

function resultObjectCalls(text) {
  const statementStart = text.indexOf('v_result := jsonb_build_object(');
  assert.ok(statementStart >= 0, 'v_result construction is missing');
  const statementEnd = text.indexOf('\n  );\n\n  UPDATE public.pay_payment_correction_work_items', statementStart);
  assert.ok(statementEnd > statementStart, 'v_result construction terminator is missing');
  const statement = text.slice(statementStart, statementEnd + 5);
  const calls = [];
  let searchFrom = statement.indexOf('jsonb_build_object(');
  while (searchFrom >= 0) {
    const call = readBalancedCall(statement, searchFrom);
    calls.push(call);
    const separator = statement.slice(call.end).match(/^\s*\|\|\s*jsonb_build_object\(/);
    if (!separator) break;
    searchFrom = call.end + separator[0].lastIndexOf('jsonb_build_object(');
  }
  return calls;
}

function assertSafeResultArity(text) {
  const calls = resultObjectCalls(text);
  assert.equal(calls.length, 3, 'the result envelope must remain three bounded additive objects');
  for (const call of calls) {
    assert.equal(call.argumentCount % 2, 0, 'jsonb_build_object must receive name/value pairs');
    assert.ok(call.argumentCount <= 100, `jsonb_build_object has ${call.argumentCount} arguments`);
  }
  return calls.map((call) => call.argumentCount);
}

test('failed-payment release builds the unchanged result envelope with bounded PostgreSQL function arity', () => {
  assert.deepEqual(assertSafeResultArity(source), [92, 40, 16]);
  for (const requiredField of [
    'selected_candidate_count',
    'voided_item_count',
    'released_reservation_count',
    'restored_component_count',
    'active_batch_amount_inc_vat_after',
    'classification_result',
    'provider_evidence_result',
    'rail_state_summary',
    'workbench_refresh'
  ]) {
    assert.match(source, new RegExp(`'${requiredField}'`));
  }
});

test('mutation guard kills restoration of the oversized single result constructor', () => {
  const mutant = source.replace(
    /'blockers', '\[\]'::jsonb\s*\) \|\| jsonb_build_object\(\s*'manual_adjustment_support_details_json'/,
    "'blockers', '[]'::jsonb,\n    'manual_adjustment_support_details_json'"
  );
  assert.notEqual(mutant, source, 'arity mutation did not apply');
  assert.throws(() => assertSafeResultArity(mutant));
});

test('one-Candidate failed-payment release verifies durable work-item evidence rather than optional debug audit', () => {
  assert.match(source, /PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_RESULT/);
  assert.match(source, /_imp_debug_audit/i);
  assert.match(auditHelperSource, /if not v_invoice_debug then[\s\S]*?return;/i);
  assert.match(parityHarness, /audit_row\.action = 'PAYMENT_CORRECTION_NO_MONEY_UNWIND_WORK_RESULT'/);
  assert.match(
    parityHarness,
    /correction_work_item_result_evidence_count[\s\S]*result_json->>'correction_item_kind' = 'NO_MONEY_UNWIND'/
  );
  assert.match(
    parityHarness,
    /after\.correction_work_item_result_evidence_count, after\.applied_work_item_count/
  );
});

test('release verification binds the bounded result envelope and preserves the established security and budgets', () => {
  assert.match(installedVerification, /v_result_join_count <> 2/);
  assert.match(installedVerification, /statement_timeout=6000ms/);
  assert.match(installedVerification, /lock_timeout=1000ms/);
  assert.match(installedVerification, /acl_row\.grantee = 0/);
  assert.match(installedVerification, /has_function_privilege\('anon'/);
  assert.match(installedVerification, /has_function_privilege\('authenticated'/);
  assert.match(installedVerification, /has_function_privilege\('service_role'/);
  assert.doesNotMatch(installedVerification, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(installedVerification, /^\s*(?:INSERT|UPDATE|DELETE|MERGE|TRUNCATE)\b/im);
});

test('failed-payment cancellation explicitly proves request and process response-loss replay', () => {
  assert.match(parityHarness, /PREPARE_SELECTION_RESPONSE_LOSS_REPLAY/);
  assert.match(parityHarness, /replayedStart\.value\.existing_request, true/);
  assert.match(parityHarness, /PROCESS_CHUNKS_RESPONSE_LOSS_REPLAY/);
  assert.match(parityHarness, /assert\.deepEqual\(effectAfterReplay, effectAfterCommittedResponse\)/);
  assert.match(parityHarness, /candidate_financial_effect_repeated: false/);
});
