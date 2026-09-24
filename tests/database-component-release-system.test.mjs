import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const manifest = JSON.parse(readFileSync(
  new URL('../supabase/release/weekly-source-invoice-evidence-component.json', import.meta.url),
  'utf8',
));
const script = readFileSync(
  new URL('../scripts/cloudtms-db-component-release.mjs', import.meta.url),
  'utf8',
);
const workflow = readFileSync(
  new URL('../.github/workflows/weekly-source-invoice-evidence-component-release.yml', import.meta.url),
  'utf8',
);

const expectedFiles = [
  'supabase/repeatable/15092026_1534_weekly_source_invoice_batch_integration_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql',
];
const openStage2 = [
  'supabase/verification/17092026_0610_weekly_source_withdrawal_supersession_v1.sql',
  'supabase/verification/17092026_0800_weekly_source_workbench_seams_v1.sql',
  'supabase/verification/17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql',
];

test('component authority is an exact TEST-only two-file allowlist', () => {
  assert.equal(manifest.environment, 'TEST');
  assert.equal(manifest.mode, 'COMPONENT_UPGRADE');
  assert.deepEqual(manifest.files.map((item) => item.path), expectedFiles);
  assert.ok(manifest.files.every((item) => /^[0-9a-f]{64}$/.test(item.beforeSha256)
    && /^[0-9a-f]{64}$/.test(item.afterSha256)));
  assert.deepEqual(manifest.excludedOpenScope, openStage2);
  assert.ok(openStage2.every((file) => !manifest.verificationFiles.includes(file)));
});

test('component source check proves the checked-in hashes without a database', () => {
  const result = spawnSync(process.execPath,
    ['scripts/cloudtms-db-component-release.mjs', 'check', '--environment=TEST'],
    { cwd: new URL('..', import.meta.url), encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.match(result.stdout, /"source": "PASS"/);
});

test('release runner refuses LIVE before attempting a database connection', () => {
  const result = spawnSync(process.execPath,
    ['scripts/cloudtms-db-component-release.mjs', 'plan', '--environment=LIVE'],
    { cwd: new URL('..', import.meta.url), encoding: 'utf8' });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /TEST-only/);
});

test('component route has no arbitrary manifest or verifier-skip input', () => {
  assert.doesNotMatch(script, /options\[['"]manifest/);
  assert.doesNotMatch(script, /skip[-_ ]verif/i);
  assert.match(script, /Unrecognised installed repeatable hash/);
  assert.match(script, /Component ledger did not reach the exact after hashes/);
  assert.match(script, /runAtomic\(atomicSql\(manifest, false/);
});

test('workflow is manual, TEST-only, test-branch-only for APPLY and serialised with full release', () => {
  assert.match(workflow, /workflow_dispatch:/);
  assert.doesNotMatch(workflow, /database-live|MIGET_DATABASE_URL_LIVE|\bLIVE\b/);
  assert.match(workflow, /group: cloudtms-database-release-TEST/);
  assert.match(workflow, /refs\/heads\/test/);
  assert.match(workflow, /db:component:plan/);
  assert.match(workflow, /db:component:apply/);
});
