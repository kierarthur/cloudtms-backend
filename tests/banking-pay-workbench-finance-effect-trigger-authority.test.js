import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');

const authority = read('supabase/repeatable/08082026_0327_pay_workbench_finance_effect_trigger_authority.sql');
const candidate = read('supabase/repeatable/04082026_1219_pay_workbench_mark_candidate_dirty.sql');
const finance = read('supabase/repeatable/04082026_1219_pay_workbench_mark_finance_case_dirty.sql');

test('late authority restores both finance-effect proposal triggers', () => {
  const candidateInclude = authority.indexOf('\\ir 04082026_1219_pay_workbench_mark_candidate_dirty.sql');
  const financeInclude = authority.indexOf('\\ir 04082026_1219_pay_workbench_mark_finance_case_dirty.sql');

  assert.ok(candidateInclude >= 0);
  assert.ok(financeInclude > candidateInclude);
  assert.match(candidate, /pay_workbench_effect_capture_mode/i);
  assert.match(candidate, /pg_temp\._bpay_wb_expected_effects/i);
  assert.match(candidate, /proposed\s*=\s*true/i);
  assert.match(finance, /pay_workbench_effect_capture_mode/i);
  assert.match(finance, /pg_temp\._bpay_wb_expected_effects/i);
  assert.match(finance, /proposed\s*=\s*true/i);
});

test('restored trigger owners retain the existing Policy X and ACL boundary', () => {
  for (const source of [candidate, finance]) {
    assert.match(source, /SECURITY DEFINER/i);
    assert.match(source, /REVOKE ALL[\s\S]+FROM PUBLIC, anon, authenticated, service_role/i);
    assert.match(source, /GRANT EXECUTE[\s\S]+TO postgres/i);
    assert.doesNotMatch(source, /pay_batch_items[\s\S]+INSERT[\s\S]+frozen_source_basis_json/i);
  }
});
