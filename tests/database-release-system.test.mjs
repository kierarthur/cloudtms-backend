import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import {
  canonicalSqlBytes, closureFor, deadlockRetryCountForFile, inventory,
  formatPlanSection, legacyUpgradeInventory,
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

test('TEST contract proof is dispatch-only, read-only, and search-path invariant', () => {
  const workflow = read('.github/workflows/database-contract-export.yml');
  assert.match(workflow, /workflow_dispatch:/);
  assert.doesNotMatch(workflow, /\npush:/);
  assert.match(workflow, /permissions:\s*\n\s*contents:\s*read/);
  assert.match(workflow, /secrets\.MIGET_DATABASE_URL_TEST/);
  assert.match(workflow, /db:plan -- --environment=TEST --mode=UPGRADE/);
  assert.doesNotMatch(workflow, /db:apply|wrangler\s+deploy|email.*drain/i);
  for (const callerPath of [
    'pg_catalog,public,pg_temp',
    'public,auth,pg_catalog,pg_temp',
    'auth,private,pg_catalog,public,pg_temp',
  ]) assert.match(workflow, new RegExp(`pgoptions: -c search_path=${callerPath}`));
  assert.match(workflow, /PGOPTIONS:\s*\$\{\{ matrix\.pgoptions \}\}/);
  assert.match(workflow, /FIELD_DIFF_COUNT=/);
  assert.match(workflow, /cmp --silent/);
  assert.match(workflow, /if:\s*always\(\)[\s\S]*actions\/upload-artifact@v4/);
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
  assert.match(workflow, /inputs\.environment.*TEST.*inputs\.mode.*UPGRADE/s);
  assert.match(workflow, /standing managed TEST UPGRADE authority/i);
  assert.match(workflow, /GITHUB_REPOSITORY.*kierarthur\/cloudtms-backend/);
  assert.match(workflow, /GITHUB_REF.*refs\/heads\/test/);
  assert.match(workflow, /CLOUDTMS_RELEASE_APPROVAL=\$expected/);
  assert.match(workflow, /CLOUDTMS_RELEASE_REQUESTED_APPROVAL/);
  assert.doesNotMatch(workflow, /CLOUDTMS_RELEASE_APPROVAL:\s*\$\{\{ inputs\.approval \}\}/);
  assert.match(workflow, /CLOUDTMS_LOGICAL_POSTGRES_OWNER:\s*CURRENT_USER/);
  assert.match(workflow, /LEGACY_UPGRADE/);
  assert.match(workflow, /google\/osv-scanner-action\/osv-scanner-action@8deb546fdb875b9996d27d4950be7312dac076a1/);
  assert.match(workflow, /--config=\.\/osv-scanner\.toml/);
  assert.match(workflow, /--lockfile=\.\/package-lock\.json/);
  assert.match(workflow, /npm run security:dependencies/);
  assert.match(workflow, /npm run security:secrets/);
  const sourceGate = workflow.slice(workflow.indexOf('source-gate:'), workflow.indexOf('\n  release:'));
  assert.doesNotMatch(sourceGate, /npm run security:verify/);
});

test('OSV exceptions are limited to the two false SheetJS findings on the exact patched release', () => {
  const config = read('osv-scanner.toml');
  const ignoredIds = [...config.matchAll(/^id = "([^"]+)"$/gm)].map((match) => match[1]);
  assert.deepEqual(ignoredIds, ['GHSA-4r6h-8v6p-xvw6', 'GHSA-5pgg-2g8v-p4x9']);
  assert.equal((config.match(/^ignoreUntil = 2026-12-31$/gm) ?? []).length, 2);
  assert.match(config, /xlsx 0\.20\.3[\s\S]*fixed in 0\.19\.3/);
  assert.match(config, /xlsx 0\.20\.3[\s\S]*fixed in 0\.20\.2/);
  assert.doesNotMatch(config, /\[\[PackageOverrides\]\]|ignore\s*=\s*true/);

  const packageJson = readJson('package.json');
  assert.equal(packageJson.dependencies.xlsx, 'https://cdn.sheetjs.com/xlsx-0.20.3/xlsx-0.20.3.tgz');
  const packageLock = readJson('package-lock.json');
  assert.equal(packageLock.packages['node_modules/xlsx'].version, '0.20.3');
  assert.equal(packageLock.packages['node_modules/xlsx'].resolved, packageJson.dependencies.xlsx);
});

test('standing database authority is limited to managed TEST UPGRADE', () => {
  const workflow = read('.github/workflows/database-release.yml');
  const authorityStart = workflow.indexOf('Using standing managed TEST UPGRADE authority');
  assert.notEqual(authorityStart, -1);
  const authorityBlock = workflow.slice(Math.max(0, authorityStart - 900), authorityStart + 500);
  assert.match(authorityBlock, /inputs\.environment.*TEST/s);
  assert.match(authorityBlock, /inputs\.mode.*UPGRADE/s);
  assert.match(authorityBlock, /GITHUB_REPOSITORY/);
  assert.match(authorityBlock, /GITHUB_REF/);
  assert.doesNotMatch(authorityBlock, /LIVE.*standing managed TEST UPGRADE/s);
  assert.match(workflow, /else[\s\S]*CLOUDTMS_RELEASE_REQUESTED_APPROVAL[\s\S]*Exact APPLY approval phrase/);
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
    assert.equal(
      mapLogicalPostgresOwnerSql("SET plpgsql_check.mode TO 'disabled'\nSET search_path = ''\nselect 1;"),
      "SET search_path = ''\nselect 1;",
    );
    assert.equal(
      mapLogicalPostgresOwnerSql("SET \"plpgsql_check.mode\" = 'disabled'\r\nSET lock_timeout = '5s';\r\n"),
      "SET lock_timeout = '5s';\r\n",
    );
    assert.equal(
      mapLogicalPostgresOwnerSql(
        "SET plpgsql_check.profiler TO 'off'\nSET plpgsql_check.tracer TO 'off'\nSET statement_timeout TO '2min'\n",
      ),
      "SET statement_timeout TO '2min'\n",
    );
    assert.equal(
      mapLogicalPostgresOwnerSql(
        "ALTER FUNCTION public.example(\n  uuid,\n  text\n) SET plpgsql_check.mode TO 'disabled';\nREVOKE ALL ON FUNCTION public.example(uuid,text) FROM PUBLIC;\n",
      ),
      'REVOKE ALL ON FUNCTION public.example(uuid,text) FROM PUBLIC;\n',
    );
    assert.equal(
      mapLogicalPostgresOwnerSql(
        "ALTER FUNCTION public.keep_me(uuid) OWNER TO postgres;\nCREATE OR REPLACE FUNCTION public.example(\n  uuid,\n  text\n) RETURNS text LANGUAGE sql AS 'select null::text';\nALTER FUNCTION public.example(\n  uuid,\n  text\n)\n  SET plpgsql_check.mode TO 'disabled';\nREVOKE ALL ON FUNCTION public.example(uuid,text) FROM PUBLIC;\n",
      ),
      "ALTER FUNCTION public.keep_me(uuid) OWNER TO CURRENT_USER;\nCREATE OR REPLACE FUNCTION public.example(\n  uuid,\n  text\n) RETURNS text LANGUAGE sql AS 'select null::text';\nREVOKE ALL ON FUNCTION public.example(uuid,text) FROM PUBLIC;\n",
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
  assert.match(source, /contractDifferenceDetails/);
  assert.match(source, /LEGACY_UPGRADE is restricted to LIVE/);
  assert.match(source, /refuses a database already carrying managed identity/);
  assert.match(source, /adoptLegacyInventoryAtomically/);
  assert.match(source, /release\.legacyUpgradeBootstrapFiles/);
  assert.match(source, /assertLegacyTransitionShimsReplaced\(\)/);
  assert.match(source, /CLOUDTMS_LEGACY_TRANSITION_SHIM/);
  assert.match(source, /function reloadPostgrestSchemaCache\(\)[\s\S]*notify pgrst, 'reload schema';/i);
  assert.match(source, /assertLegacyTransitionShimsReplaced\(\);[\s\S]*reloadPostgrestSchemaCache\(\);[\s\S]*runVerifiers\(mode\)/);
  assert.match(source, /reloadPostgrestSchemaCache\(\);[\s\S]*runVerifiers\(mode\);[\s\S]*compareExpected\(release\.contractPath\)/);
  assert.doesNotMatch(source, /marking existing migrations/);
  assert.match(source, /mode === 'NEW'[\s\S]*controlPlaneIndex[\s\S]*postBaselineMigrations[\s\S]*for \(const item of postBaselineMigrations\) psql\(\{ file: item\.path \}\)[\s\S]*baselineRepeatableLock[\s\S]*pendingRepeatables[\s\S]*runBankingPayCatalogPreapply[\s\S]*for \(const item of pendingRepeatables\) psql\(\{ file: item\.path \}\)[\s\S]*recordInventory/);
  assert.match(source, /mode === 'NEW'[\s\S]*release\.newVerificationFiles/);
  const release = readJson('supabase/release/current-release.json');
  assert.ok(release.verificationFiles.some(file => file.includes('banking_pay_james_rate_authority_runtime_verification')));
  assert.ok(!release.newVerificationFiles.some(file => file.includes('banking_pay_james_rate_authority_runtime_verification')));
  assert.ok(release.newVerificationFiles.includes('supabase/verification/26082026_0044_candidate_manager_authoriser_policy_v2_verification.sql'));
});

test('contract drift diagnostics name relation additions, removals, and changed definitions without row data', async () => {
  const { contractDifferenceDetails } = await import('../scripts/cloudtms-db-release-lib.mjs');
  const base = {
    relations: [
      { schema: 'public', name: 'alpha', kind: 'r', columns: [{ name: 'id', type: 'uuid' }] },
      { schema: 'public', name: 'removed', kind: 'v', columns: [] },
    ],
  };
  const installed = {
    relations: [
      { schema: 'public', name: 'alpha', kind: 'r', columns: [{ name: 'id', type: 'text' }] },
      { schema: 'private', name: 'unexpected', kind: 'r', columns: [] },
    ],
  };

  assert.deepEqual(contractDifferenceDetails(base, installed, ['relations']), [
    'unexpected installed relation private.unexpected:r',
    'changed installed relation public.alpha:r fields=columns',
    'missing installed relation public.removed:v',
  ]);
  assert.deepEqual(contractDifferenceDetails(base, installed, ['routines']), []);
});

test('legacy transition bootstrap is bounded and must be replaced before adoption', () => {
  const release = readJson('supabase/release/current-release.json');
  assert.deepEqual(release.legacyUpgradeBootstrapFiles, [
    'supabase/repeatable/08042026_1151_newtablesbanking.sql',
    'supabase/release/24082026_1128_legacy_upgrade_trigger_shims.sql',
  ]);
  const schemaBootstrap = read(release.legacyUpgradeBootstrapFiles[0]);
  assert.match(schemaBootstrap, /CREATE TABLE IF NOT EXISTS public\.banking_pay_workbench_sessions/);
  assert.match(schemaBootstrap, /Safe to rerun/);
  const bootstrap = read(release.legacyUpgradeBootstrapFiles[1]);
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

test('read-only release plans render every exact pending authority path and hash', () => {
  assert.equal(formatPlanSection('PENDING MIGRATIONS', []), 'PENDING MIGRATIONS: none');
  assert.equal(
    formatPlanSection('PENDING MIGRATIONS', [
      { path: 'supabase/migrations/24082026_example.sql', sha256: 'a'.repeat(64) },
    ]),
    `PENDING MIGRATIONS:\n- supabase/migrations/24082026_example.sql sha256=${'a'.repeat(64)}`,
  );
  assert.equal(
    formatPlanSection('PENDING/CHANGED REPEATABLES', [
      { path: 'supabase/repeatable/example.sql', sha256: 'b'.repeat(64) },
    ], 'closure_sha256'),
    `PENDING/CHANGED REPEATABLES:\n- supabase/repeatable/example.sql closure_sha256=${'b'.repeat(64)}`,
  );

  const engine = read('scripts/cloudtms-db-release.mjs');
  assert.match(engine, /mode === 'UPGRADE'[\s\S]*assertUpgradeLedger\(current\)/);
  assert.match(engine, /mode === 'UPGRADE'[\s\S]*PENDING MIGRATIONS/);
  assert.match(engine, /mode === 'UPGRADE'[\s\S]*PENDING\/CHANGED REPEATABLES/);
});

test('contract export normalises null ACLs to one-dimensional effective defaults', () => {
  const source = read('supabase/release/export_contract.sql');
  assert.doesNotMatch(source, /aclexplode\(coalesce\([^)]*,\s*'\{\}'::aclitem\[\]\)\)/s);
  assert.match(source, /acldefault\([\s\S]*c\.relowner/);
  assert.match(source, /acldefault\('f'::"char", p\.proowner\)/);
  assert.match(source, /acldefault\('n'::"char", n\.nspowner\)/);
  assert.equal((source.match(/select distinct\s+case when a\.grantee = 0 then 'PUBLIC'/gi) || []).length, 4);
  assert.equal((source.match(/\) expanded_acl/g) || []).length, 4);
  assert.match(source, /when rolname=current_user then 'postgres'/);
  assert.match(source, /d\.defaclrole = \([\s\S]*role_row\.rolname = current_user/);
  assert.match(source, /provider image can also contain a physical role[\s\S]*literally named `postgres`/);
  assert.match(source, /default_acl_rows as \(/);
  assert.match(source, /select distinct owner_name, schema_name, object_kind, contract_row/);
});

test('contract export is provider and upgrade-history neutral without weakening security fields', () => {
  const source = read('supabase/release/export_contract.sql');
  const requirePinnedExporterPath = candidate => {
    assert.match(candidate, /set search_path = pg_catalog, public;\s*\n\s*with/i);
    assert.equal((candidate.match(/set search_path = pg_catalog, public;/gi) || []).length, 1);
    assert.doesNotMatch(
      candidate.match(/set search_path\s*=\s*[^;]+;/i)?.[0] ?? '',
      /\bauth\b/i,
    );
  };
  requirePinnedExporterPath(source);
  for (const unsafeMutation of [
    source.replace('set search_path = pg_catalog, public;\n', ''),
    source.replace('set search_path = pg_catalog, public;', 'set search_path = public, pg_catalog;'),
    source.replace('set search_path = pg_catalog, public;', 'set search_path = pg_catalog, auth, public;'),
  ]) assert.throws(() => requirePinnedExporterPath(unsafeMutation));
  assert.doesNotMatch(source, /'position',\s*a\.attnum/);
  assert.match(source, /order by a\.attname collate "C"/);
  assert.match(source, /config_value !~ '\^plpgsql_check\[\.\]'/);
  assert.match(source, /regexp_replace\([\s\S]*plpgsql_check\\\\\./);
  assert.match(source, /p\.proname = 'cloudtms_data_api_mfa_gate'/);
  assert.match(source, /expanded_acl\.grantee = 'authenticator'/);
  assert.match(source, /'security_definer', p\.prosecdef/);
  assert.match(source, /'definition_sha256'/);
  assert.match(source, /jsonb_agg\([\s\S]*case when role_name::text=current_user then 'postgres'[\s\S]*order by \(case when role_name::text=current_user then 'postgres'[\s\S]*collate "C"/);
  assert.doesNotMatch(source, /jsonb_agg\([\s\S]*role_name::text[\s\S]*order by ordinality/);
});

test('contract export keeps cross-schema auth foreign keys explicitly qualified', () => {
  const contract = JSON.parse(read('supabase/release/current-contract.json'));
  const relation = contract.relations.find(item =>
    item.schema === 'public' && item.name === 'pay_manual_adjustment_carry_forwards');
  assert.ok(relation, 'pay manual-adjustment carry-forward relation is absent');
  const constraints = new Map(relation.constraints.map(item => [item.name, item.definition]));
  for (const name of [
    'pay_manual_adjustment_carry_forwards_created_by_fkey',
    'pay_manual_adjustment_carry_forwards_created_by_user_id_fkey',
  ]) {
    assert.equal(
      constraints.get(name),
      'FOREIGN KEY (created_by_user_id) REFERENCES auth.users(id)',
      `${name} must retain its exact cross-schema auth.users target`,
    );
  }
});

test('contract export normalises only the exact PostgreSQL 18 ordinary relation NOT NULL duplicate', () => {
  const source = read('supabase/release/export_contract.sql');
  const constraintStart = source.indexOf("'constraints', coalesce((");
  const constraintEnd = source.indexOf("'indexes', coalesce((", constraintStart);
  assert.notEqual(constraintStart, -1);
  assert.notEqual(constraintEnd, -1);
  const constraintSource = source.slice(constraintStart, constraintEnd);

  assert.match(source, /columns\[\]\.not_null/);
  assert.match(source, /does not treat the generated[\s\S]*constraint name as separate application authority/);
  assert.match(constraintSource, /where con\.conrelid = c\.oid[\s\S]*and not \(\s*con\.contype = 'n'/);
  for (const guard of [
    /con\.contypid = 0::oid/,
    /con\.conindid = 0::oid/,
    /con\.conparentid = 0::oid/,
    /con\.confrelid = 0::oid/,
    /not con\.condeferrable/,
    /not con\.condeferred/,
    /con\.convalidated/,
    /con\.conislocal/,
    /con\.coninhcount = 0/,
    /not con\.connoinherit/,
    /con\.conkey is not null/,
    /pg_catalog\.cardinality\(con\.conkey\) = 1/,
    /con\.confkey is null/,
    /con\.conpfeqop is null/,
    /con\.conppeqop is null/,
    /con\.conffeqop is null/,
    /con\.conexclop is null/,
    /con\.conbin is null/,
    /not_null_column\.attrelid = con\.conrelid/,
    /not_null_column\.attnum = con\.conkey\[1\]/,
    /not not_null_column\.attisdropped/,
    /not_null_column\.attnotnull/,
  ]) assert.match(constraintSource, guard);
  assert.equal((source.match(/con\.contype = 'n'/g) || []).length, 1);
  assert.doesNotMatch(source, /server_version|server_version_num|current_setting\s*\(\s*'server_version/);
});

const portabilityPg17Url = process.env.CLOUDTMS_RELEASE_PORTABILITY_PG17_URL;
const portabilityPg18Url = process.env.CLOUDTMS_RELEASE_PORTABILITY_PG18_URL;
assert.equal(
  Boolean(portabilityPg17Url),
  Boolean(portabilityPg18Url),
  'set both CLOUDTMS_RELEASE_PORTABILITY_PG17_URL and CLOUDTMS_RELEASE_PORTABILITY_PG18_URL, or neither',
);

if (portabilityPg17Url && portabilityPg18Url) {
  test('full contract export is invariant to caller search_path on PostgreSQL 17 and 18', () => {
    const psql = process.env.PSQL_BIN || 'psql';
    const exporterPath = path.join(repoRoot, 'supabase/release/export_contract.sql').replaceAll('\\', '/');
    const expectedCanonical = JSON.stringify(readJson('supabase/release/current-contract.json'));
    const callerPaths = [
      'pg_catalog, public, pg_temp',
      'public, auth, pg_catalog, pg_temp',
      'auth, private, pg_catalog, public, pg_temp',
    ];
    const exportFullContract = (databaseUrl, callerPath) => {
      const connection = new URL(databaseUrl);
      assert.ok(['postgres:', 'postgresql:'].includes(connection.protocol));
      assert.ok(['127.0.0.1', 'localhost'].includes(connection.hostname),
        'search_path runtime proof is restricted to a task-owned local PostgreSQL database');
      const childEnv = {
        ...process.env,
        PGHOST: connection.hostname,
        PGPORT: connection.port || '5432',
        PGUSER: decodeURIComponent(connection.username || ''),
        PGDATABASE: decodeURIComponent(connection.pathname.replace(/^\//, '')),
      };
      if (connection.password) childEnv.PGPASSWORD = decodeURIComponent(connection.password);
      const output = execFileSync(psql, ['-X', '-q', '-w', '-v', 'ON_ERROR_STOP=1'], {
        cwd: repoRoot,
        encoding: 'utf8',
        env: childEnv,
        input: `\\pset tuples_only on
\\pset format unaligned
select 'SERVER_MAJOR=' || current_setting('server_version_num');
set search_path = ${callerPath};
\\ir '${exporterPath}'
`,
        maxBuffer: 64 * 1024 * 1024,
      });
      const lines = output.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
      const serverLine = lines.find(line => line.startsWith('SERVER_MAJOR='));
      const contractLine = lines.find(line => line.startsWith('{') && line.includes('"contract_version"'));
      assert.ok(serverLine, 'server version was not emitted');
      assert.ok(contractLine, 'contract JSON was not emitted');
      const contract = JSON.parse(contractLine);
      const carryForwards = contract.relations.find(item =>
        item.schema === 'public' && item.name === 'pay_manual_adjustment_carry_forwards');
      assert.ok(carryForwards, 'pay manual-adjustment carry-forward relation is absent');
      const constraints = new Map(carryForwards.constraints.map(item => [item.name, item.definition]));
      for (const name of [
        'pay_manual_adjustment_carry_forwards_created_by_fkey',
        'pay_manual_adjustment_carry_forwards_created_by_user_id_fkey',
      ]) assert.equal(
        constraints.get(name),
        'FOREIGN KEY (created_by_user_id) REFERENCES auth.users(id)',
      );
      const canonical = JSON.stringify(contract);
      return {
        serverVersionNum: serverLine.split('=')[1],
        canonical,
        hash: sha256(canonical),
      };
    };

    const resultsByMajor = new Map();
    for (const [expectedMajor, databaseUrl] of [
      ['17', portabilityPg17Url],
      ['18', portabilityPg18Url],
    ]) {
      const results = callerPaths.map(callerPath => exportFullContract(databaseUrl, callerPath));
      for (const result of results) {
        assert.equal(result.serverVersionNum.slice(0, 2), expectedMajor);
        assert.equal(result.canonical, expectedCanonical);
        assert.equal(result.hash, sha256(expectedCanonical));
      }
      assert.equal(new Set(results.map(result => result.canonical)).size, 1);
      assert.equal(new Set(results.map(result => result.hash)).size, 1);
      resultsByMajor.set(expectedMajor, results[0]);
    }
    assert.equal(resultsByMajor.get('17').canonical, resultsByMajor.get('18').canonical);
    assert.equal(resultsByMajor.get('17').hash, resultsByMajor.get('18').hash);
  });

  test('PostgreSQL 17 and 18 export one identical portable relation contract with all non-NOT-NULL constraints retained', () => {
    const psql = process.env.PSQL_BIN || 'psql';
    const exporterPath = path.join(repoRoot, 'supabase/release/export_contract.sql').replaceAll('\\', '/');
    const exportProbe = databaseUrl => {
      const connection = new URL(databaseUrl);
      assert.ok(['postgres:', 'postgresql:'].includes(connection.protocol));
      assert.ok(['127.0.0.1', 'localhost'].includes(connection.hostname),
        'portability runtime proof is restricted to a task-owned local PostgreSQL database');
      const childEnv = {
        ...process.env,
        PGHOST: connection.hostname,
        PGPORT: connection.port || '5432',
        PGUSER: decodeURIComponent(connection.username || ''),
        PGDATABASE: decodeURIComponent(connection.pathname.replace(/^\//, '')),
      };
      if (connection.password) childEnv.PGPASSWORD = decodeURIComponent(connection.password);
      const output = execFileSync(psql, ['-X', '-q', '-w', '-v', 'ON_ERROR_STOP=1'], {
        cwd: repoRoot,
        encoding: 'utf8',
        env: childEnv,
        input: `\\pset tuples_only on
\\pset format unaligned
begin;
create table public.h1_release_contract_portability_parent (
  id bigint primary key
);
create table public.h1_release_contract_portability_probe (
  id bigint constraint h1_release_contract_probe_id_not_null not null,
  parent_id bigint,
  required_text text constraint h1_release_contract_probe_text_not_null not null,
  optional_text text,
  amount integer,
  span int8range,
  constraint h1_release_contract_probe_pk primary key (id),
  constraint h1_release_contract_probe_unique unique (required_text),
  constraint h1_release_contract_probe_check check (amount >= 0),
  constraint h1_release_contract_probe_fk foreign key (parent_id)
    references public.h1_release_contract_portability_parent (id),
  constraint h1_release_contract_probe_exclude exclude using gist (span with &&)
);
create table public.h1_release_contract_portability_inherited_parent (
  inherited_required integer not null
);
create table public.h1_release_contract_portability_inherited_child ()
inherits (public.h1_release_contract_portability_inherited_parent);
select 'H1_RAW_NOT_NULL_CONSTRAINTS=' || count(*)::text
from pg_catalog.pg_constraint
where conrelid = 'public.h1_release_contract_portability_probe'::regclass
  and contype = 'n';
select 'H1_RAW_INHERITED_NOT_NULL_CONSTRAINTS=' || count(*)::text
from pg_catalog.pg_constraint
where conrelid = 'public.h1_release_contract_portability_inherited_child'::regclass
  and contype = 'n';
\\ir '${exporterPath}'
rollback;
`,
        maxBuffer: 64 * 1024 * 1024,
      });
      const lines = output.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
      const rawLine = lines.find(line => line.startsWith('H1_RAW_NOT_NULL_CONSTRAINTS='));
      const rawInheritedLine = lines.find(line =>
        line.startsWith('H1_RAW_INHERITED_NOT_NULL_CONSTRAINTS='));
      const contractLine = lines.find(line => line.startsWith('{') && line.includes('"contract_version"'));
      assert.ok(rawLine, 'raw NOT NULL constraint count was not emitted');
      assert.ok(rawInheritedLine, 'raw inherited NOT NULL constraint count was not emitted');
      assert.ok(contractLine, 'contract JSON was not emitted');
      const contract = JSON.parse(contractLine);
      const relation = contract.relations.find(item =>
        item.schema === 'public' && item.name === 'h1_release_contract_portability_probe');
      const inheritedChild = contract.relations.find(item =>
        item.schema === 'public'
        && item.name === 'h1_release_contract_portability_inherited_child');
      assert.ok(relation, 'portability probe relation is absent from the contract');
      assert.ok(inheritedChild, 'inherited NOT NULL probe relation is absent from the contract');
      return {
        rawNotNullConstraintCount: Number(rawLine.split('=')[1]),
        rawInheritedNotNullConstraintCount: Number(rawInheritedLine.split('=')[1]),
        relation,
        inheritedChild,
      };
    };

    const pg17 = exportProbe(portabilityPg17Url);
    const pg18 = exportProbe(portabilityPg18Url);
    assert.equal(pg17.rawNotNullConstraintCount, 0);
    assert.ok(pg18.rawNotNullConstraintCount >= 2);
    assert.deepEqual(pg18.relation, pg17.relation);

    const columns = new Map(pg18.relation.columns.map(column => [column.name, column]));
    assert.equal(columns.get('required_text')?.not_null, true);
    assert.equal(columns.get('optional_text')?.not_null, false);
    const constraintTypes = new Set(pg18.relation.constraints.map(constraint => constraint.type));
    for (const type of ['c', 'f', 'p', 'u', 'x']) assert.ok(constraintTypes.has(type));
    assert.equal(constraintTypes.has('n'), false);

    assert.equal(pg17.rawInheritedNotNullConstraintCount, 0);
    assert.equal(pg18.rawInheritedNotNullConstraintCount, 1);
    assert.equal(
      pg18.inheritedChild.columns.find(column => column.name === 'inherited_required')?.not_null,
      true,
    );
    assert.equal(
      pg18.inheritedChild.constraints.filter(constraint => constraint.type === 'n').length,
      1,
      'a PG18 inherited NOT NULL constraint is not the ordinary local duplicate and must remain visible',
    );
    assert.equal(
      pg17.inheritedChild.constraints.some(constraint => constraint.type === 'n'),
      false,
    );
  });
}

test('private Candidate Daily Miget policies are exact, reproducible and grant no table privilege', () => {
  const source = read('supabase/migrations/26082026_0312_private_daily_service_rls_reconciliation.sql');
  const tables = [
    'candidate_daily_authority_scopes',
    'candidate_daily_authority_transitions',
    'candidate_daily_batch_receipts',
    'candidate_daily_entitlements',
    'candidate_daily_external_effect_receipts',
    'candidate_daily_source_links',
    'candidate_daily_sync_state',
  ];
  for (const table of tables) assert.match(source, new RegExp(`'${table}'`));
  assert.match(source, /cloudtms_miget_service_owner_all/);
  assert.match(source, /for all to %I, service_role using \(true\) with check \(true\)/i);
  assert.doesNotMatch(source, /grant\s+(?:select|insert|update|delete|all)\s+on\s+table/i);
  assert.doesNotMatch(source, /\b(?:insert|update|delete|truncate)\s+(?:into|from|table)\s+private\.candidate_daily_/i);
});

test('Bible preserves Policy X and protected security boundary', () => {
  const bible = read('docs/DATABASE_RELEASE_BIBLE.md');
  assert.match(bible, /post-draft uses frozen batch artifacts only/);
  assert.match(bible, /protected-boundary-lock\.json/);
  assert.match(bible, /There is no blind baselining/);
  assert.match(read('AGENTS.md'), /Mandatory database release process/);
});
