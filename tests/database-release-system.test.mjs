import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import {
  closureFor, inventory, readJson, repoRoot, sqlDateKey, verifyIntegrity,
} from '../scripts/cloudtms-db-release-lib.mjs';

const read = relative => fs.readFileSync(path.join(repoRoot, relative), 'utf8');

test('date ordering distinguishes valid ISO from UK filenames', () => {
  assert.equal(sqlDateKey('20260218_smoke_once_only.sql'), '20260218_smoke_once_only.sql'.replace(/^20260218/, '20260218'));
  assert.equal(sqlDateKey('18022026_0951_example.sql'), '20260218_0951_example.sql');
  assert.ok(sqlDateKey('20261340_invalid.sql').startsWith('ZZZZZZZZ_'));
  assert.ok(sqlDateKey('32132026_invalid.sql').startsWith('ZZZZZZZZ_'));
});

test('inventory covers uppercase legacy SQL and recursive repeatable closures', () => {
  const current = inventory();
  assert.ok(current.migrations.some(x => x.path.endsWith('13072026_1648_MIGRATION.SQL')));
  assert.ok(current.migrations.some(x => x.path.endsWith('26052026_1525_INDEX_OUTBOX.SQL')));
  const withIncludes = current.repeatables.find(x => x.paths.length > 1);
  assert.ok(withIncludes, 'expected a repeatable with included support pages');
  assert.equal(closureFor(withIncludes.path).sha256, withIncludes.sha256);
});

test('migration immutability and protected Candidate boundary pass', () => {
  const current = verifyIntegrity();
  const lock = readJson('supabase/release/migration-lock.json');
  assert.equal(current.migrations.length, lock.migrations.length);
  assert.equal(readJson('supabase/release/protected-boundary-lock.json').files.length, 5);
});

test('migration locking is append-only and cannot bless an edited historical migration', () => {
  const engine = read('scripts/cloudtms-db-release.mjs');
  const bible = read('docs/DATABASE_RELEASE_BIBLE.md');
  assert.match(engine, /Refusing to relock changed migration/);
  assert.match(engine, /Locked migration missing/);
  assert.doesNotMatch(engine, /if \(kind === 'migration'\) writeMigrationLock\(\)/);
  assert.match(bible, /only appends new migration hashes/);
});

test('normal push workflow cannot mutate a database', () => {
  const workflow = read('.github/workflows/supabase-migrate.yml');
  assert.match(workflow, /Database source verification \(no deploy\)/);
  assert.doesNotMatch(workflow, /SUPABASE_DB_URL|CLOUDTMS_DATABASE_URL|psql\s+"?\$|db:apply/);
  assert.doesNotMatch(workflow, /marking existing migrations as applied|__BOOTSTRAPPED__/);
});

test('manual release is dispatch-only, environment-protected, and two phase', () => {
  const workflow = read('.github/workflows/database-release.yml');
  assert.match(workflow, /workflow_dispatch:/);
  assert.doesNotMatch(workflow, /\npush:/);
  assert.match(workflow, /database-live/);
  assert.match(workflow, /database-test/);
  assert.match(workflow, /secrets\.CLOUDTMS_DATABASE_URL/);
  assert.match(workflow, /secrets\.SUPABASE_DB_URL_TEST/);
  assert.match(workflow, /Read-only release plan/);
  assert.match(workflow, /if: inputs\.phase == 'APPLY'/);
  assert.match(workflow, /APPLY \$\{\{ inputs\.environment \}\} \$\{\{ inputs\.mode \}\}/);
});

test('release engine has fail-closed NEW, ADOPT, and UPGRADE gates', () => {
  const source = read('scripts/cloudtms-db-release.mjs');
  assert.match(source, /NEW requires an empty application schema/);
  assert.match(source, /mode === 'ADOPT'/);
  assert.match(source, /Installed migration hash mismatch/);
  assert.match(source, /Database contract differs in/);
  assert.doesNotMatch(source, /__BOOTSTRAPPED__|marking existing migrations/);
  assert.match(source, /mode === 'NEW'[\s\S]*for \(const item of current\.repeatables\) psql\(\{ file: item\.path \}\)[\s\S]*recordInventory/);
});

test('contract export normalises null ACLs to one-dimensional effective defaults', () => {
  const source = read('supabase/release/export_contract.sql');
  assert.doesNotMatch(source, /aclexplode\(coalesce\([^)]*,\s*'\{\}'::aclitem\[\]\)\)/s);
  assert.match(source, /acldefault\([\s\S]*c\.relowner/);
  assert.match(source, /acldefault\('f'::"char", p\.proowner\)/);
  assert.match(source, /acldefault\('n'::"char", n\.nspowner\)/);
});

test('Bible preserves Policy X and protected security boundary', () => {
  const bible = read('docs/DATABASE_RELEASE_BIBLE.md');
  assert.match(bible, /post-draft uses frozen batch artifacts only/);
  assert.match(bible, /protected-boundary-lock\.json/);
  assert.match(bible, /There is no blind baselining/);
  assert.match(read('AGENTS.md'), /Mandatory database release process/);
});
