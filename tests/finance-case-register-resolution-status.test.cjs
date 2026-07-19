const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const migrationPath = path.join(
  __dirname,
  '..',
  'supabase',
  'migrations',
  '19072026_0120_fix_finance_case_resolution_register.sql'
);

const sql = fs.readFileSync(migrationPath, 'utf8');
const componentRollup = sql.slice(
  sql.indexOf('component_rollup AS ('),
  sql.indexOf('oneoff_bank_details AS (')
);

test('finance register only counts actionable pay-channel switches as unresolved', () => {
  assert.ok(componentRollup.length > 0, 'component_rollup CTE was not found');
  assert.match(componentRollup, /JOIN pay_advances component_case/);
  assert.match(componentRollup, /JOIN candidates component_candidate/);
  assert.match(
    componentRollup,
    /source_pay_method[\s\S]*?IS DISTINCT FROM[\s\S]*?component_candidate\.pay_method/
  );
  assert.match(
    componentRollup,
    /component_case\.case_type = 'MANUAL_DEBT_ADJUSTMENT'[\s\S]*?source_units[\s\S]*?source_rate[\s\S]*?source_charge_rate/
  );
  assert.match(
    componentRollup,
    /saved_target_pay_method[\s\S]*?IS DISTINCT FROM[\s\S]*?component_candidate\.pay_method/
  );
});

test('same-channel and fixed-conversion components cannot create false blocked counts', () => {
  const needsOperatorResolution = ({
    classification = 'TAXABLE_CHANNEL_SENSITIVE',
    sourcePayMethod,
    targetPayMethod,
    caseType = 'OVERPAYMENT',
    actionableBucket = true,
    stale = false,
    savedTargetPayMethod = null,
    savedResolutionMode = null
  }) => (
    classification === 'TAXABLE_CHANNEL_SENSITIVE'
    && sourcePayMethod !== targetPayMethod
    && (caseType === 'MANUAL_DEBT_ADJUSTMENT' || actionableBucket)
    && (
      stale
      || savedTargetPayMethod == null
      || savedTargetPayMethod !== targetPayMethod
      || savedResolutionMode == null
    )
  );

  assert.equal(needsOperatorResolution({ sourcePayMethod: 'PAYE', targetPayMethod: 'PAYE' }), false);
  assert.equal(needsOperatorResolution({ sourcePayMethod: 'PAYE', targetPayMethod: 'UMBRELLA' }), true);
  assert.equal(needsOperatorResolution({
    sourcePayMethod: 'PAYE',
    targetPayMethod: 'UMBRELLA',
    actionableBucket: false
  }), false);
  assert.equal(needsOperatorResolution({
    sourcePayMethod: 'PAYE',
    targetPayMethod: 'UMBRELLA',
    savedTargetPayMethod: 'UMBRELLA',
    savedResolutionMode: 'SUGGESTED_EQUIVALENT_BASIS'
  }), false);
});
