import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = (path) => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const canonical = read('supabase/repeatable/07082026_1013_pay_workbench_candidate_source_build_chunk.sql');
const lateAuthority = read('supabase/repeatable/08082026_0322_pay_workbench_candidate_source_build_chunk_authority.sql');

test('late authority reasserts the profile-2 source-build owner after historical omnibus files', () => {
  assert.match(lateAuthority, /\\ir 07082026_1013_pay_workbench_candidate_source_build_chunk\.sql/);
  assert.match(canonical, /effect_plan_sealed/i);
  assert.match(canonical, /PAY_WORKBENCH_EFFECT_PLAN_CAPTURE_ROLLBACK/i);
  assert.match(canonical, /fact_family='EXPECTED_FINANCE_EFFECT'/i);
  assert.match(canonical, /reconcile_capture_timing_json/i);
  assert.match(canonical, /PAY_WORKBENCH_CANONICAL_PUBLICATION_DIGEST_MISMATCH/i);
});

test('capture is durably sealed before execute and does not weaken Policy X', () => {
  const captureBranch = canonical.indexOf("effect_plan_sealed')::boolean,false) IS NOT TRUE");
  const executeCall = canonical.lastIndexOf('v_sync_result:=public.pay_sync_overpayments_from_preview');
  assert.ok(captureBranch >= 0, 'capture branch must exist');
  assert.ok(executeCall > captureBranch, 'execute must follow the sealed capture branch');
  assert.match(canonical, /PAY_WORKBENCH_EXPECTED_EFFECT_PLAN_MISMATCH/i);
  assert.match(canonical, /captured_candidate_generation/i);
  assert.match(canonical, /captured_source_change_seq/i);
  assert.doesNotMatch(canonical, /statement_timeout\s*=/i);
});
