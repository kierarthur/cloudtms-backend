import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const sql = fs.readFileSync(
  path.join(root, 'supabase/repeatable/31072026_1720_pay_batch_nested_resolution_breakdowns.sql'),
  'utf8'
);

test('replacement preserves the function signature and security boundary', () => {
  assert.match(sql, /CREATE OR REPLACE FUNCTION public\.pay_batch_build_item_breakdowns\(p_pay_batch_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_operation_id uuid DEFAULT NULL::uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb\)/);
  assert.match(sql, /LANGUAGE plpgsql/);
  assert.match(sql, /SECURITY DEFINER/);
  assert.match(sql, /SET search_path TO 'public'/);
  assert.doesNotMatch(sql, /DROP FUNCTION|ALTER FUNCTION|GRANT |REVOKE /i);
});

test('nested frozen resolution buckets replace their wrappers as the breakdown basis', () => {
  assert.match(sql, /frozen_resolution_payload_json->'case_components'/);
  assert.match(sql, /wrapper_component_json->'resolution_rows'/);
  assert.match(sql, /\{payload_json,bucket_resolutions\}/);
  assert.match(sql, /wrapper_component_json[\s\S]*\|\| resolution_entry\.resolution_json[\s\S]*\|\| bucket_entry\.bucket_json/);
  assert.match(sql, /WHERE NOT EXISTS \([\s\S]*fallback_resolution[\s\S]*fallback_bucket/);
});

test('the target basis is matched by frozen timesheet, economic key, date, and bucket', () => {
  assert.match(sql, /item_timesheet_id_text/);
  assert.match(sql, /component_timesheet_id_text/);
  assert.match(sql, /item_economic_key_type = 'TS_DAY'/);
  assert.match(sql, /source_basis_work_date/);
  assert.match(sql, /item_bucket_code IS NULL[\s\S]*component_bucket_code = item_match\.item_bucket_code/);
  assert.match(sql, /component_json->>'target_units'/);
  assert.match(sql, /component_json->>'target_rate'/);
});

test('Policy X is preserved: no live finance identity fallback or amount-derived units/rate is introduced', () => {
  assert.match(sql, /Policy X/);
  assert.doesNotMatch(sql, /JOIN\s+public\.(candidate_finance|candidate_finance_components|finance_case_components|timesheet_financials)/i);
  assert.doesNotMatch(sql, /derived_amount[^\n]*\/[^\n]*derived_rate|amount_ex_vat[^\n]*\/[^\n]*rate/i);
});
