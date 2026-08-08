const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const source = fs.readFileSync(path.join(
  root,
  'supabase',
  'migrations',
  '08082026_1137_repair_banking_pay_scope_generation_gap.sql',
), 'utf8');

test('scope generation repair is exact, evidence-fenced and metadata-only', () => {
  assert.match(source, /8d441fff-4153-413c-a522-72b6903a754f/i);
  assert.match(source, /bfdc14ec-82a6-566c-b6d5-bf760ecaf030/i);
  assert.match(source, /v_expected_generation constant bigint := 96/i);
  assert.match(source, /v_expected_target_count constant integer := 9/i);
  assert.match(source, /bounded_scope_state_precedes_job/i);
  assert.match(source, /scope_tx\.state = 'FINALIZED'/i);
  assert.match(source, /scope_tx\.allocated_generation = v_expected_generation/i);
  assert.match(source, /v_job\.status NOT IN \('QUEUED', 'RUNNING'\)/i);
  assert.match(source, /registry\.dirty_generation, v_expected_generation/i);
  assert.match(source, /scope_state\.dirty_generation, v_expected_generation/i);
  assert.doesNotMatch(source, /\b(?:INSERT|UPDATE|DELETE)\s+(?:INTO\s+)?public\.banking_pay_workbench_jobs\b/i);
  assert.doesNotMatch(source, /\b(?:INSERT|UPDATE|DELETE)\s+(?:INTO\s+)?public\.(?:pay_batches|bank_transfers)\b/i);
});
