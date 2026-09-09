const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const sourcePath = path.resolve(
  __dirname,
  '../supabase/repeatable/07092026_1932_banking_pay_unpaid_cancellation_sourceless_apply_v1.sql'
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
  const nestedCallStarts = [];

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
    if (text.startsWith('jsonb_build_object(', index)) nestedCallStarts.push(index);
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
          argumentCount: body.trim() === '' ? 0 : topLevelCommas + 1,
          nestedCallStarts,
          start: callStart
        };
      }
      continue;
    }
    if (current === ',' && depth === 1) topLevelCommas += 1;
  }
  assert.fail('jsonb_build_object closing parenthesis is missing');
}

function resultObjectCalls(text) {
  const functionStart = text.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_no_money_unwind_apply_work_item('
  );
  assert.ok(functionStart >= 0, 'final pay_no_money_unwind_apply_work_item owner is missing');
  const functionEnd = text.indexOf('\n$function$;', functionStart);
  assert.ok(functionEnd > functionStart, 'final pay_no_money_unwind_apply_work_item terminator is missing');
  const functionSource = text.slice(functionStart, functionEnd);
  const statementStart = functionSource.indexOf('v_result := jsonb_build_object(');
  assert.ok(statementStart >= 0, 'v_result construction is missing');
  const statementEnd = functionSource.indexOf(
    'UPDATE public.pay_payment_correction_work_items',
    statementStart
  );
  assert.ok(statementEnd > statementStart, 'v_result construction terminator is missing');
  const statement = functionSource.slice(statementStart, statementEnd);
  const topLevelCalls = [];
  let searchFrom = statement.indexOf('jsonb_build_object(');
  while (searchFrom >= 0) {
    const call = readBalancedCall(statement, searchFrom);
    topLevelCalls.push(call);
    const separator = statement.slice(call.end).match(/^\s*\|\|\s*jsonb_build_object\(/);
    if (!separator) break;
    searchFrom = call.end + separator[0].lastIndexOf('jsonb_build_object(');
  }

  const allCallStarts = new Set(
    topLevelCalls.flatMap((call) => [call.start, ...call.nestedCallStarts])
  );
  const allCalls = Array.from(allCallStarts)
    .sort((left, right) => left - right)
    .map((callStart) => readBalancedCall(statement, callStart));
  return { allCalls, topLevelCalls };
}

function assertSafeResultArity(text) {
  const { allCalls, topLevelCalls } = resultObjectCalls(text);
  for (const call of allCalls) {
    assert.equal(call.argumentCount % 2, 0, 'jsonb_build_object must receive name/value pairs');
    assert.ok(call.argumentCount <= 100, `jsonb_build_object has ${call.argumentCount} arguments`);
  }
  assert.equal(topLevelCalls.length, 4, 'the result envelope must remain four bounded additive objects');
  return {
    all: allCalls.map((call) => call.argumentCount),
    topLevel: topLevelCalls.map((call) => call.argumentCount)
  };
}

test('final failed-payment owner builds the result envelope with bounded PostgreSQL function arity', () => {
  const arity = assertSafeResultArity(source);
  assert.deepEqual(arity.topLevel, [86, 22, 40, 16]);
  assert.deepEqual(arity.all, [86, 22, 40, 16, 18]);
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

test('mutation guard rejects removal of every result split boundary', () => {
  const mutations = [
    {
      label: '43/11 boundary',
      pattern: /('communication_cleanup_contract_version', CASE[\s\S]*?\bEND)\s*\)\s*\|\|\s*jsonb_build_object\(\s*('matching_queued_count')/,
      replacement: '$1,\n    $2',
      expectedError: /jsonb_build_object has 108 arguments/
    },
    {
      label: '11/20 boundary',
      pattern: /('blockers', '\[\]'::jsonb)\s*\)\s*\|\|\s*jsonb_build_object\(\s*('manual_adjustment_support_details_json')/,
      replacement: '$1,\n    $2',
      expectedError: /four bounded additive objects/
    },
    {
      label: '20/8 boundary',
      pattern: /('rail_state_summary', COALESCE\(v_rail_state_summary_json, '\{\}'::jsonb\))\s*\)\s*\|\|\s*jsonb_build_object\(\s*('workbench_refresh_status')/,
      replacement: '$1,\n    $2',
      expectedError: /four bounded additive objects/
    }
  ];

  for (const mutation of mutations) {
    const mutant = source.replace(mutation.pattern, mutation.replacement);
    assert.notEqual(mutant, source, `${mutation.label} mutation did not apply`);
    assert.throws(
      () => assertSafeResultArity(mutant),
      mutation.expectedError,
      `${mutation.label} mutation survived`
    );
  }
});

test('balanced arity guard scans nested result constructors', () => {
  const oversizedPairs = Array.from(
    { length: 42 },
    (_, index) => `'oversized_${index}', ${index}`
  ).join(',\n      ');
  const mutant = source.replace(
    /('workbench_refresh', jsonb_build_object\(\s*)('status')/,
    `$1${oversizedPairs},\n      $2`
  );
  assert.notEqual(mutant, source, 'nested arity mutation did not apply');
  assert.throws(() => assertSafeResultArity(mutant), /jsonb_build_object has 102 arguments/);
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
  assert.match(installedVerification, /v_result_join_count <> 3/);
  for (const boundaryField of [
    'communication_cleanup_contract_version',
    'matching_queued_count',
    'blockers',
    'manual_adjustment_support_details_json',
    'rail_state_summary',
    'workbench_refresh_status'
  ]) {
    assert.match(installedVerification, new RegExp(boundaryField));
  }
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
