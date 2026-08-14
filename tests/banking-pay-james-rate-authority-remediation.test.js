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
const sourceBuild = fs.readFileSync(
  new URL('../supabase/repeatable/07082026_1013_pay_workbench_candidate_source_build_chunk.sql', import.meta.url),
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
    'sealed_parent_facts',
    'nested_evidence_raw',
    'nested_evidence_normalized',
    'exact_allocation_matched',
    'sealed_physical_amount_attribution',
    'truth_residual_sources',
    'RATE_AUTHORITY_NESTED_AMOUNT_OVERCONSUMED',
    'RATE_AUTHORITY_PARENT_COMPONENT_RECONCILIATION_MISMATCH',
  ]) assert.ok(helper.includes(marker), `missing sealed physical attribution marker: ${marker}`);
  assert.ok(helper.includes("'FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK'"));
  assert.match(helper, /fact\.fact_family='RESERVATION_COMPONENT'/);
  assert.match(helper, /'component_fallback','WORKED_TIME_AMOUNT'/);
  assert.doesNotMatch(helper,
    /preliminary_outstanding_allocation|preview_truth_weight_total|proportional|allocation_rank/i);
  assert.doesNotMatch(helper,
    /public\.(?:timesheets|timesheets_financials|candidates|umbrellas|settings_finance_windows|pay_batch_items|pay_batches|pay_batch_candidates|pay_advance_reservations)\b/i);
});

test('serializer and synchronizer share the defensive segment-key canonicalisation', () => {
  assert.match(serializer,
    /COALESCE\(\s*NULLIF\(BTRIM\(input\.payload#>>'\{segment,segment_key\}'\),''\),\s*NULLIF\(BTRIM\(input\.payload#>>'\{segment,segment_id\}'\),''\)\s*\) AS segment_key/);
  assert.match(synchronizer,
    /'segment_key',COALESCE\(\s*NULLIF\(BTRIM\(component\.value#>>'\{source_basis_json,segment_key\}'\),''\),\s*NULLIF\(BTRIM\(component\.value#>>'\{source_basis_json,segment_id\}'\),''\)\)/);
});

test('source build seals active item reservations and synchronizer replaces all live reservation workspaces', () => {
  for (const marker of [
    'ACTIVE_ITEM_RESERVATION:',
    "'~ITEM:'",
    'RESERVATION_ECONOMIC_KEY_MISSING',
    'RESERVATION_ECONOMIC_KEY_CONFLICT',
    'RESERVATION_DUPLICATE_LOGICAL_OWNER',
    'pay_batch_items_active_reservation',
    'public._pay_batch_item_economic_components',
  ]) assert.ok(sourceBuild.includes(marker), `missing active reservation marker: ${marker}`);

  for (const table of [
    'reserved_batch_items',
    'reserved_by_source_ref',
    'reserved_total_by_timesheet',
    'reserved_segment_key_map',
    'reserved_segment_sums',
    'reserved_preview_segment_ords',
    'reserved_additional_by_code',
  ]) {
    assert.match(synchronizer,
      new RegExp(`LIKE pg_temp\\.${table} INCLUDING ALL[\\s\\S]*RENAME TO ${table}`));
  }
  assert.match(synchronizer, /tmp_sync_sealed_reservation_items/);
});

test('negative worked-time recovery preserves the unchanged fixed residual identity', () => {
  for (const source of [helper, synchronizer]) {
    assert.ok(source.includes('WORKED_TIME_RESIDUAL'));
    assert.ok(source.includes('WORKED_TIME_AMOUNT'));
    assert.ok(source.includes('worked-time-residual:'));
  }
  assert.match(synchronizer,
    /component_kind IN \('WORKED_TIME','WORKED_TIME_RESIDUAL'\)/);
  assert.match(synchronizer,
    /'date',MIN\(COALESCE\(segment\.source_segment->>'date',[\s\S]*segment\.economic_key_type='TS_DAY'/);
  assert.match(synchronizer,
    /THEN 'worked-time-residual:'\|\|UPPER\(BTRIM\(component\.value->>'component_key_type'\)\)/);
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
