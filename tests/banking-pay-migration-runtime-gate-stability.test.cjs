const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const sourceWorkflow = fs.readFileSync(path.join(root, '.github', 'workflows', 'supabase-migrate.yml'), 'utf8');
const releaseWorkflow = fs.readFileSync(path.join(root, '.github', 'workflows', 'database-release.yml'), 'utf8');
const candidateRuntimeWorkflow = fs.readFileSync(
  path.join(root, '.github', 'workflows', 'candidate-db-runtime.yml'),
  'utf8',
);
const releaseManifest = JSON.parse(fs.readFileSync(
  path.join(root, 'supabase', 'release', 'current-release.json'),
  'utf8',
));
const deterministicRuntime = fs.readFileSync(
  path.join(root, 'tests', '13082026_1942_banking_pay_james_rate_authority_runtime_verification.sql'),
  'utf8',
);
const liveReadonly = fs.readFileSync(
  path.join(root, 'supabase', 'verification', '13082026_1943_banking_pay_james_rate_authority_readonly.sql'),
  'utf8',
);

test('migration gate runs deterministic James fixtures without mutable TEST build state', () => {
  assert.ok(releaseManifest.verificationFiles.includes(
    'tests/13082026_1942_banking_pay_james_rate_authority_runtime_verification.sql',
  ));
  assert.match(releaseWorkflow, /npm run db:apply/);
  assert.match(sourceWorkflow, /This workflow performs source checks only/);
  assert.doesNotMatch(deterministicRuntime, /\\ir\s+.*13082026_1943_banking_pay_james_rate_authority_readonly\.sql/);
  assert.match(deterministicRuntime, /\nBEGIN;\n/);
  assert.match(deterministicRuntime, /\nROLLBACK;\s*$/);
  assert.match(deterministicRuntime, /mutable TEST[\s-]+build lifecycle state must not block an unrelated database deployment/);
  assert.doesNotMatch(deterministicRuntime, /v_template_build|FIXTURE_TEMPLATE_BUILD_MISSING/);
  assert.match(deterministicRuntime, /INSERT INTO public\.banking_pay_snapshot_runs/);
  assert.match(deterministicRuntime, /INSERT INTO public\.banking_pay_workbench_sessions/);
  assert.match(deterministicRuntime, /INSERT INTO private\.banking_pay_workbench_economic_builds/);
});

test('manual live James diagnostic selects only a completed sealed build', () => {
  assert.match(liveReadonly, /build\.status='COMPLETE'/);
  assert.match(liveReadonly, /build\.private_stage='COMPLETE'/);
  assert.match(liveReadonly, /build\.completed_at_utc IS NOT NULL/);
  assert.doesNotMatch(liveReadonly, /^\s*(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE)\b/im);
});

test('database workflows reuse psql and bound the package-manager fallback', () => {
  for (const databaseWorkflow of [releaseWorkflow, candidateRuntimeWorkflow]) {
    assert.match(databaseWorkflow, /timeout-minutes:\s*5/);
    assert.match(databaseWorkflow, /command -v psql/);
    assert.match(databaseWorkflow, /timeout 180s/);
    assert.match(databaseWorkflow, /Acquire::Retries=3/);
    assert.match(databaseWorkflow, /Acquire::http::Timeout=30/);
    assert.match(databaseWorkflow, /Acquire::https::Timeout=30/);
    assert.match(databaseWorkflow, /psql --version/);
  }

  assert.match(sourceWorkflow, /\.github\/workflows\/candidate-db-runtime\.yml/);
});
