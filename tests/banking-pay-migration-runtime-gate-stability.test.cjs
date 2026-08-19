const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const workflow = fs.readFileSync(path.join(root, '.github', 'workflows', 'supabase-migrate.yml'), 'utf8');
const deterministicRuntime = fs.readFileSync(
  path.join(root, 'tests', '13082026_1942_banking_pay_james_rate_authority_runtime_verification.sql'),
  'utf8',
);
const liveReadonly = fs.readFileSync(
  path.join(root, 'supabase', 'verification', '13082026_1943_banking_pay_james_rate_authority_readonly.sql'),
  'utf8',
);

test('migration gate runs deterministic James fixtures without mutable TEST build state', () => {
  assert.match(workflow, /-f tests\/13082026_1942_banking_pay_james_rate_authority_runtime_verification\.sql/);
  assert.doesNotMatch(deterministicRuntime, /\\ir\s+.*13082026_1943_banking_pay_james_rate_authority_readonly\.sql/);
  assert.match(deterministicRuntime, /\nBEGIN;\n/);
  assert.match(deterministicRuntime, /\nROLLBACK;\s*$/);
  assert.match(deterministicRuntime, /mutable TEST[\s-]+build lifecycle state must not block an unrelated database deployment/);
});

test('manual live James diagnostic selects only a completed sealed build', () => {
  assert.match(liveReadonly, /build\.status='COMPLETE'/);
  assert.match(liveReadonly, /build\.private_stage='COMPLETE'/);
  assert.match(liveReadonly, /build\.completed_at_utc IS NOT NULL/);
  assert.doesNotMatch(liveReadonly, /^\s*(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE)\b/im);
});
