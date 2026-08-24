import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import {
  canonicalSqlBytes, closureFor, deadlockRetryCountForFile, inventory,
  legacyUpgradeInventory,
  mapGeneratedAclBaselineSql, mapLogicalPostgresOwnerSql,
  readJson, repoRoot, sha256, sqlDateKey,
  validateTarget, verifyIntegrity,
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

test('migration identity is stable across Windows checkout line endings', () => {
  const relative = 'supabase/migrations/20260218_smoke_once_only.sql';
  const lock = readJson('supabase/release/migration-lock.json').migrations.find(item => item.path === relative);
  assert.ok(lock);
  assert.equal(sha256(canonicalSqlBytes(relative)), lock.sha256);
});

test('deadlock retries are limited to an immutable transaction-safe migration', () => {
  const providerOwnerMigration =
    'supabase/migrations/24082026_0232_miget_provider_owner_defaults.sql';
  assert.equal(deadlockRetryCountForFile(providerOwnerMigration), 3);
  assert.equal(deadlockRetryCountForFile('supabase/migrations/20260218_smoke_once_only.sql'), 0);
  assert.match(read(providerOwnerMigration), /begin;[\s\S]*\\ir \.\.\/baseline\/[\s\S]*commit;/);
});

test('migration immutability and protected Candidate boundary pass', () => {
  const current = verifyIntegrity();
  const lock = readJson('supabase/release/migration-lock.json');
  assert.equal(current.migrations.length, lock.migrations.length);
  assert.equal(readJson('supabase/release/baseline-repeatable-lock.json').repeatables.length, 298);
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
  assert.match(workflow, /secrets\.MIGET_DATABASE_URL_TEST/);
  assert.match(workflow, /vars\.MIGET_DATABASE_TARGET_TEST/);
  assert.match(workflow, /secrets\.MIGET_DATABASE_URL_LIVE/);
  assert.match(workflow, /vars\.MIGET_DATABASE_TARGET_LIVE/);
  assert.doesNotMatch(workflow, /secrets\.SUPABASE_DB_URL_TEST/);
  assert.doesNotMatch(workflow, /inputs\.environment == 'LIVE'[\s\S]{0,200}secrets\.(?:CLOUDTMS_DATABASE_URL|SUPABASE_DB_URL)/);
  assert.match(workflow, /Read-only release plan/);
  assert.match(workflow, /if: inputs\.phase == 'APPLY'/);
  assert.match(workflow, /APPLY \$\{\{ inputs\.environment \}\} \$\{\{ inputs\.mode \}\}/);
  assert.match(workflow, /CLOUDTMS_LOGICAL_POSTGRES_OWNER:\s*CURRENT_USER/);
  assert.match(workflow, /LEGACY_UPGRADE/);
});

test('one-time LIVE provider clone is protected, source-read-only and destination-blank-only', () => {
  const workflow = read('.github/workflows/live-miget-clone.yml');
  assert.match(workflow, /workflow_dispatch:/);
  assert.match(workflow, /environment:\s*database-live/);
  assert.match(workflow, /secrets\.MIGET_DATABASE_URL_LIVE/);
  assert.match(workflow, /secrets\.CLOUDTMS_DATABASE_URL \|\| secrets\.SUPABASE_DB_URL/);
  assert.match(workflow, /Miget LIVE destination is not blank/);
  assert.match(workflow, /pg_dump[^\n]*--no-owner[^\n]*--no-privileges[^\n]*--schema=public[^\n]*--schema=maintenance/);
  assert.match(workflow, /sed -i -E[^\n]*SCHEMA - public/);
  assert.match(workflow, /pg_restore[^\n]*--no-owner[^\n]*--no-privileges[^\n]*--exit-on-error[^\n]*--use-list/);
  assert.match(workflow, /Exact table row-count verification failed/);
  assert.match(workflow, /Exact sequence-state verification failed/);
  assert.doesNotMatch(workflow, /service_role[^\n]*bypassrls|alter role service_role bypassrls/i);
  assert.match(workflow, /create role postgres nologin noinherit/);
  assert.match(workflow, /create role supabase_admin nologin noinherit/);
  assert.doesNotMatch(workflow, /create role (?:postgres|supabase_admin)[^\n]*(?:^|[^a-z])login\b/im);
  assert.match(workflow, /where e\.extname in \('pgcrypto', 'uuid-ossp'\)/);
  assert.doesNotMatch(workflow, /grant execute on all functions in schema extensions/i);
  assert.match(workflow, /docker run --rm[\s\S]*sha256sum \/work\/live\.dump/);
  assert.doesNotMatch(workflow, /chmod[^\n]*live\.dump/i);
  assert.doesNotMatch(workflow, /actions\/upload-artifact/);
});

test('provider database owner mapping is explicit, bounded and fail closed', () => {
  const previous = process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
  try {
    process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = 'CURRENT_USER';
    assert.equal(
      mapLogicalPostgresOwnerSql('alter function public.example() owner to postgres;'),
      'alter function public.example() OWNER TO CURRENT_USER;',
    );
    assert.equal(
      mapLogicalPostgresOwnerSql('alter function public.example() owner to "postgres";'),
      'alter function public.example() OWNER TO CURRENT_USER;',
    );
    assert.equal(
      mapLogicalPostgresOwnerSql('alter default privileges for role "postgres" in schema public grant execute on functions to service_role;'),
      'ALTER DEFAULT PRIVILEGES FOR ROLE CURRENT_USER in schema public grant execute on functions to service_role;',
    );
    assert.equal(
      mapGeneratedAclBaselineSql('grant execute on function public.example() to "postgres";'),
      'grant execute on function public.example() to CURRENT_USER;',
    );
    assert.equal(
      mapGeneratedAclBaselineSql('revoke all on function private.example() from PUBLIC, authenticator, supabase_admin;'),
      'revoke all on function private.example() from PUBLIC, supabase_admin;',
    );
    process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = 'UNSAFE_ROLE';
    assert.throws(() => mapLogicalPostgresOwnerSql('select 1;'), /must be CURRENT_USER/);
  } finally {
    if (previous === undefined) delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
    else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = previous;
  }
});

test('hosted target validation accepts a provider-neutral database locator and fails closed', () => {
  const previousUrl = process.env.CLOUDTMS_DATABASE_URL;
  try {
    process.env.CLOUDTMS_DATABASE_URL = 'postgresql://automation:secret@postgres.example/cloudtms_test_clone?sslmode=require';
    assert.doesNotThrow(() => validateTarget('TEST', 'cloudtms_test_clone'));
    assert.throws(() => validateTarget('TEST', 'another_database'), /CLOUDTMS_EXPECTED_TARGET/);
    assert.throws(() => validateTarget('TEST', ''), /CLOUDTMS_EXPECTED_TARGET/);
  } finally {
    if (previousUrl === undefined) delete process.env.CLOUDTMS_DATABASE_URL;
    else process.env.CLOUDTMS_DATABASE_URL = previousUrl;
  }
});

test('release engine has fail-closed NEW, ADOPT, UPGRADE, and one-time legacy upgrade gates', () => {
  const source = read('scripts/cloudtms-db-release.mjs');
  assert.match(source, /NEW requires an empty application schema/);
  assert.match(source, /mode === 'ADOPT'/);
  assert.match(source, /Installed migration hash mismatch/);
  assert.match(source, /Database contract differs in/);
  assert.match(source, /LEGACY_UPGRADE is restricted to LIVE/);
  assert.match(source, /refuses a database already carrying managed identity/);
  assert.match(source, /adoptLegacyInventoryAtomically/);
  assert.match(source, /release\.legacyUpgradeBootstrapFile/);
  assert.match(source, /assertLegacyTransitionShimsReplaced\(\)/);
  assert.match(source, /CLOUDTMS_LEGACY_TRANSITION_SHIM/);
  assert.doesNotMatch(source, /marking existing migrations/);
  assert.match(source, /mode === 'NEW'[\s\S]*baselineRepeatableLock[\s\S]*pendingRepeatables[\s\S]*runBankingPayCatalogPreapply[\s\S]*for \(const item of pendingRepeatables\) psql\(\{ file: item\.path \}\)[\s\S]*recordInventory/);
});

test('legacy transition bootstrap is bounded and must be replaced before adoption', () => {
  const release = readJson('supabase/release/current-release.json');
  assert.equal(
    release.legacyUpgradeBootstrapFile,
    'supabase/release/24082026_1128_legacy_upgrade_trigger_shims.sql',
  );
  const bootstrap = read(release.legacyUpgradeBootstrapFile);
  assert.match(bootstrap, /to_regprocedure/);
  assert.match(bootstrap, /CLOUDTMS_LEGACY_TRANSITION_SHIM/);
  assert.match(bootstrap, /return OLD/);
  assert.match(bootstrap, /return NEW/);
  assert.doesNotMatch(bootstrap, /insert\s+into|update\s+public\.|delete\s+from|truncate/i);
});

test('legacy upgrade inventory accepts only an exact historical subset and one bootstrap marker', () => {
  const current = {
    migrations: [
      { path: 'supabase/migrations/01012026_first.sql', sha256: 'a'.repeat(64) },
      { path: 'supabase/migrations/02012026_second.sql', sha256: 'b'.repeat(64) },
    ],
    repeatables: [{ path: 'supabase/repeatable/current.sql', sha256: 'c'.repeat(64) }],
  };
  const planned = legacyUpgradeInventory(current, ['__BOOTSTRAPPED__', '01012026_first.sql']);
  assert.equal(planned.installedCount, 1);
  assert.deepEqual(planned.pendingMigrations.map(item => item.path), [
    'supabase/migrations/02012026_second.sql',
  ]);
  assert.equal(planned.pendingRepeatables.length, 1);
  assert.throws(
    () => legacyUpgradeInventory(current, ['__BOOTSTRAPPED__', 'unknown.sql']),
    /absent from repository/,
  );
  assert.throws(
    () => legacyUpgradeInventory(current, ['01012026_first.sql']),
    /exactly one __BOOTSTRAPPED__/,
  );
  assert.throws(
    () => legacyUpgradeInventory(current, ['__BOOTSTRAPPED__', '01012026_first.sql', '01012026_first.sql']),
    /duplicate/,
  );
});

test('contract export normalises null ACLs to one-dimensional effective defaults', () => {
  const source = read('supabase/release/export_contract.sql');
  assert.doesNotMatch(source, /aclexplode\(coalesce\([^)]*,\s*'\{\}'::aclitem\[\]\)\)/s);
  assert.match(source, /acldefault\([\s\S]*c\.relowner/);
  assert.match(source, /acldefault\('f'::"char", p\.proowner\)/);
  assert.match(source, /acldefault\('n'::"char", n\.nspowner\)/);
  assert.match(source, /when rolname=current_user then 'postgres'/);
  assert.match(source, /owner_role\.logical_name = 'postgres'/);
});

test('Bible preserves Policy X and protected security boundary', () => {
  const bible = read('docs/DATABASE_RELEASE_BIBLE.md');
  assert.match(bible, /post-draft uses frozen batch artifacts only/);
  assert.match(bible, /protected-boundary-lock\.json/);
  assert.match(bible, /There is no blind baselining/);
  assert.match(read('AGENTS.md'), /Mandatory database release process/);
});
