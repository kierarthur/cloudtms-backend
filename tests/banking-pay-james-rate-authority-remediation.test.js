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

test('paired negative hours use the preview builder signed delta contract without absolute-value drift', () => {
  assert.match(helper,
    /SUM\(fact\.source_units\) FILTER\(\s*WHERE fact\.authority_kind='BASELINE'\)/);
  assert.match(helper,
    /parsed_source_units,0\)\s*- bucket\.attributed_baseline_source_units[\s\S]*AS raw_delta_source_units/);
  assert.match(helper,
    /parsed_source_pay_ex_vat-bucket\.attributed_baseline_ex_vat[\s\S]*AS raw_delta_before_reservation_ex/);
  assert.match(helper,
    /raw_delta_before_reservation_ex-delta\.attributed_reserved_ex_vat[\s\S]*AS builder_component_amount_ex_vat/);
  assert.match(helper,
    /GREATEST\(delta\.raw_delta_source_units,0\)[\s\S]*THEN ROUND\(GREATEST\(delta\.raw_delta_source_units,0\),6\) END AS builder_source_units/);
  assert.doesNotMatch(helper,
    /ABS\([^\n]*raw_delta_(?:source_units|before_reservation_ex|charge_ex_vat)[^\n]*\)\s+AS/);
});

test('signed finance recovery is non-allocative sealed movement evidence, not negative hours', () => {
  assert.match(helper,
    /parent_item_type[\s\S]*key_resolution_source[\s\S]*is_signed_non_charge_recovery/);
  assert.match(helper,
    /parent\.is_signed_non_charge_recovery IS NOT TRUE[\s\S]*nested_evidence_shape_failures/);
  assert.match(helper,
    /parent\.is_signed_non_charge_recovery THEN 0::numeric/);
  assert.match(helper, /SIGNED_NON_CHARGE_RECOVERY_V1/);
  assert.match(helper,
    /parent\.is_signed_non_charge_recovery IS NOT TRUE[\s\S]*RATE_AUTHORITY_PARENT_SOURCE_CHARGE_MISSING/);
});

test('zero-outstanding sealed buckets remain audit evidence but are not required modal components', () => {
  assert.match(helper,
    /AS builder_component_expected[\s\S]*FROM bucket_builder_delta delta/);
  assert.match(helper,
    /'builder_component_expected',bucket\.builder_component_expected/);
  assert.match(helper,
    /'builder_component_expected',ABS\(ROUND\(synthetic\.outstanding_ex_vat,2\)\)>0\.005/);
  assert.match(synchronizer,
    /sealed\.evidence_json#>>'\{physical_bucket,builder_component_expected\}'='true'[\s\S]*builder\.timesheet_id IS NULL/);
  assert.match(synchronizer,
    /FROM pg_temp\.tmp_sync_sealed_rate_projection sealed\s+WHERE sealed\.evidence_json#>>'\{physical_bucket,builder_component_expected\}'='true'/);
});

test('negative preview metadata compares signed component amount with authoritative outstanding', () => {
  assert.match(synchronizer,
    /preview_total\.preview_truth_ex_vat\s*- authoritative_component\.outstanding_ex_vat/);
  assert.doesNotMatch(synchronizer,
    /preview_total\.preview_truth_ex_vat\s*- authoritative_component\.truth_ex_vat/);
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

test('James proves source method at the exact economic key without changing the helper return contract', () => {
  for (const marker of [
    'source_method_evidence',
    'source_method_authority_summary',
    'source_method_authority_tier',
    'selected_authority_priority',
    'source_method_authority',
    'distinct_supported_source_method_count',
    'complete_evidence_digest',
    'RATE_AUTHORITY_SOURCE_PAY_METHOD_CONFLICT',
  ]) assert.ok(helper.includes(marker), `missing exact source-method authority marker: ${marker}`);

  assert.match(helper,
    /GROUP BY evidence\.timesheet_id,evidence\.economic_key_type,evidence\.economic_key_value/);
  assert.match(helper, /10::integer AS authority_priority,[\s\S]*'LIVE_OCCURRENCE'/);
  assert.match(helper, /20::integer,[\s\S]*'SEALED_PARENT'/);
  assert.match(helper, /'evidence_sample'/);
  assert.match(helper,
    /CASE WHEN COALESCE\(bucket\.validated_failure,economic\.failure_code\) IS NOT NULL\s+THEN 'FAILED'/);
  assert.doesNotMatch(helper, /MIN\((?:source\.)?source_pay_method\)/i);
  assert.doesNotMatch(helper, /distinct_supported_source_method_count\s+text/i);
  assert.doesNotMatch(helper,
    /allocation\.economic_key_type IN \('TS_DAY','TS_TOTAL'\)[\s\S]{0,160}allocation\.matched_count=0[\s\S]{0,160}THEN 0/);
});

test('reservation additional-code aggregation groups by the exact selected expression', () => {
  const selectedExpression = /UPPER\(NULLIF\(BTRIM\(COALESCE\(\s*breakdown\.value->>'bucket_code',\s*CASE WHEN sealed\.component_key_type='ADDITIONAL_CODE'\s*THEN sealed\.component_key_value END\)\),''\)\)/g;
  assert.equal((synchronizer.match(selectedExpression) || []).length, 2);
});

test('synchronizer proves singleton method cardinality before case persistence and never uses target as source', () => {
  assert.match(synchronizer, /RATE_AUTHORITY_SOURCE_PAY_METHOD_CONFLICT/);
  assert.match(synchronizer, /complete_component_method_digest/);
  assert.match(synchronizer, /component_method_sample/);
  assert.match(synchronizer,
    /FROM pg_temp\.tmp_sync_timesheet_case_candidates candidate[\s\S]*distinct_supported_method_count<>1/);
  assert.doesNotMatch(synchronizer, /COALESCE\(component\.source_pay_method,candidate_pay_method\)/i);
  assert.doesNotMatch(synchronizer, /COALESCE\(component\.source_pay_method,current_target_pay_method\)/i);
  assert.doesNotMatch(synchronizer, /MIN\(sealed\.source_pay_method\)/i);
});
