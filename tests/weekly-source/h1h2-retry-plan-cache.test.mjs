import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const retry = fs.readFileSync(path.join(root,
  'supabase/repeatable/03102026_0600_stage2_plan_cache_after_h1h2_retry.sql'), 'utf8');
const owner = fs.readFileSync(path.join(root,
  'supabase/repeatable/26092026_0207_banking_pay_stage2_session_plan_cache_mode_v1.sql'), 'utf8');
const recoveryOwner = fs.readFileSync(path.join(root,
  'supabase/repeatable/26092026_0207_banking_pay_stage2_recovery_order_floor_v1.sql'), 'utf8');

test('H1/H2 retry restores the exact reviewed A35 planner settings after the old trigger body replay', () => {
  assert.match(retry, /^\\ir 26092026_0207_banking_pay_stage2_session_plan_cache_mode_v1\.sql$/m);
  assert.match(owner, /ALTER FUNCTION public\.pay_timesheet_summary_pay_state_refresh_trigger\(\) SET plan_cache_mode = force_custom_plan;/);
  assert.match(owner, /ALTER FUNCTION public\._pay_batch_item_economic_components\(uuid, uuid\[\]\) SET plan_cache_mode = force_custom_plan;/);
  assert.match(owner, /A35_POSTCONDITION_FAILED/);
  const jitSettings = [...recoveryOwner.matchAll(/^ALTER FUNCTION .* SET jit = off;$/gm)].map(x => x[0]);
  assert.equal(jitSettings.length, 6);
  for (const statement of jitSettings) assert.ok(retry.includes(statement), statement);
});
