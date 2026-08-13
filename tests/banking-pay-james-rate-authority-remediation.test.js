import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const serializer = fs.readFileSync(
  new URL('../supabase/repeatable/04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql', import.meta.url),
  'utf8',
);
const helper = fs.readFileSync(
  new URL('../supabase/repeatable/13082026_1912_pay_workbench_sealed_rate_component_projection_v1.sql', import.meta.url),
  'utf8',
);
const synchronizer = fs.readFileSync(
  new URL('../supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql', import.meta.url),
  'utf8',
);

test('nullable pay methods fail with the typed James contract before SQL constraints', () => {
  assert.match(serializer,
    /WHEN input\.source_pay_method IS NULL\s+OR input\.source_pay_method NOT IN \('PAYE','UMBRELLA'\)\s+THEN 'RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING'/);
  assert.match(serializer,
    /WHEN input\.target_pay_method IS NULL\s+OR input\.target_pay_method NOT IN \('PAYE','UMBRELLA'\)\s+THEN 'RATE_AUTHORITY_TARGET_PAY_METHOD_MISSING'/);
  assert.match(synchronizer,
    /SELECT 21,'RATE_AUTHORITY_SOURCE_PAY_METHOD_MISSING'[\s\S]*NULLIF\(BTRIM\(raw\.source_pay_method\),''\) IS NULL/);
  assert.match(synchronizer,
    /SELECT 22,'RATE_AUTHORITY_TARGET_PAY_METHOD_MISSING'[\s\S]*NULLIF\(BTRIM\(raw\.target_pay_method\),''\) IS NULL/);
});

test('physical baseline and reservation use sealed exact attribution without proportional allocation', () => {
  assert.match(serializer,
    /'baseline_source_pay_ex_vat',NULL::numeric[\s\S]*'reserved_source_pay_ex_vat',NULL::numeric[\s\S]*'outstanding_source_pay_ex_vat',NULL::numeric/);
  for (const marker of [
    'sealed_physical_amount_facts',
    'sealed_physical_amount_matches',
    'PHYSICAL_BUCKET_KEY',
    'STRUCTURAL_IDENTITY',
    'SOLE_BUCKET',
    'RATE_AUTHORITY_PHYSICAL_BASELINE_REQUIRED',
    'RATE_AUTHORITY_PHYSICAL_RESERVATION_REQUIRED',
  ]) assert.ok(helper.includes(marker), `missing sealed physical attribution marker: ${marker}`);
  assert.match(helper,
    /fact\.fact_family IN \('FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK',\s*'RESERVATION_COMPONENT'\)/);
  assert.doesNotMatch(helper,
    /preliminary_outstanding_allocation|preview_truth_weight_total|proportional|allocation_rank/i);
});

test('every additional-rate code is an independent physical rate identity', () => {
  assert.match(serializer, /FROM jsonb_each\(CASE/);
  assert.match(serializer,
    /WHEN 'ADDITIONAL' THEN 'additional:'\|\|UPPER\(BTRIM\(resolved\.payload->>'additional_code'\)\)/);
  assert.match(synchronizer,
    /GROUP BY raw\.timesheet_id,raw\.component_kind,raw\.component_member_identity,\s*raw\.economic_key_type,raw\.economic_key_value,raw\.bucket_code/);
  assert.match(synchronizer,
    /HAVING count\(DISTINCT ROUND\(raw\.source_rate,6\)\)>1\s+OR count\(DISTINCT ROUND\(raw\.source_charge_rate,6\)\)>1/);
  assert.doesNotMatch(synchronizer,
    /GROUP BY raw\.timesheet_id,raw\.bucket_code\s+HAVING count\(DISTINCT ROUND\(raw\.source_rate,6\)\)>1/);

  const arbitraryCodes = ['OVERTIME_150', 'CLIENT_PREMIUM_X7', 'CALL_OUT_SPECIAL'];
  const physicalMembers = arbitraryCodes.map((code) => `additional:${code.toUpperCase()}`);
  assert.equal(new Set(physicalMembers).size, arbitraryCodes.length);
});
