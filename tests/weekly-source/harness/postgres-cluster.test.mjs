import assert from 'node:assert/strict';
import test from 'node:test';
import {
  assertPostgresHarnessConnectionTarget,
  runPostgresHarness,
  validatePostgresHarnessOptions,
  validatePostgresHarnessUpgradeIdentity,
  WEEKLY_SOURCE_POSTGRES_HARNESS_CONTRACT,
} from './postgres-cluster.mjs';

test('TH-004 target guard permits only the exact local fixture database', () => {
  assert.equal(WEEKLY_SOURCE_POSTGRES_HARNESS_CONTRACT.namedConnectionGroups, true);
  assert.equal(assertPostgresHarnessConnectionTarget('postgresql://postgres@127.0.0.1:55499/banking_modal_v2_test', 55499), true);
  for (const target of [
    'postgresql://postgres@example.invalid:55499/banking_modal_v2_test',
    'postgresql://postgres@127.0.0.1:55499/cloudtms_test_clone',
    'postgresql://postgres@127.0.0.1:55499/uofvkfi5',
    'postgresql://postgres:password@127.0.0.1:55499/banking_modal_v2_test',
  ]) {
    assert.throws(() => assertPostgresHarnessConnectionTarget(target, 55499), (error) => error.code === 'POSTGRES_HARNESS_TARGET_REFUSED');
  }
  assert.equal(WEEKLY_SOURCE_POSTGRES_HARNESS_CONTRACT.broadPrune, false);
  assert.deepEqual(WEEKLY_SOURCE_POSTGRES_HARNESS_CONTRACT.upgradeRestoreRoles,
    ['anon', 'authenticated', 'service_role', 'authenticator', 'supabase_admin']);
});

test('TH-004 UPGRADE bootstraps the exact frozen provider roles before pg_restore', async () => {
  const source = await import('node:fs/promises').then(({ readFile }) =>
    readFile(new URL('./postgres-cluster.mjs', import.meta.url), 'utf8'));
  const bootstrap = source.indexOf('const upgradeRestoreRoleBootstrapSha256 = ensureUpgradeRestoreRoles');
  const restore = source.indexOf("options.pgRestoreBin ?? process.env.PG_RESTORE_BIN ?? 'pg_restore'");
  assert(bootstrap > 0);
  assert(restore > bootstrap);
  for (const role of WEEKLY_SOURCE_POSTGRES_HARNESS_CONTRACT.upgradeRestoreRoles) {
    assert.match(source, new RegExp(`create role ${role}\\b`, 'i'));
  }
  assert.match(source, /service_role' and not rolcanlogin and rolbypassrls and rolinherit/i);
});

test('TH-004 UPGRADE binds the release to the reviewed immutable snapshot identity', async () => {
  assert.deepEqual(validatePostgresHarnessUpgradeIdentity({
    mode: 'UPGRADE',
    environment: 'TEST',
    customerKey: 'weekly-source-harness-plan62-env',
  }), {
    environment: 'TEST',
    customerKey: 'weekly-source-harness-plan62-env',
  });
  assert.throws(() => validatePostgresHarnessUpgradeIdentity({
    mode: 'UPGRADE', environment: 'LIVE', customerKey: 'weekly-source-harness-plan62-env',
  }), (error) => error.code === 'POSTGRES_HARNESS_UPGRADE_ENVIRONMENT_REFUSED');
  assert.throws(() => validatePostgresHarnessUpgradeIdentity({
    mode: 'UPGRADE', environment: 'TEST', customerKey: '',
  }), (error) => error.code === 'POSTGRES_HARNESS_UPGRADE_CUSTOMER_KEY_INVALID');
  assert.throws(() => validatePostgresHarnessUpgradeIdentity({
    mode: 'NEW', environment: 'TEST', customerKey: 'weekly-source-harness-plan62-env',
  }), (error) => error.code === 'POSTGRES_HARNESS_UPGRADE_IDENTITY_NOT_APPLICABLE');
  assert.equal(WEEKLY_SOURCE_POSTGRES_HARNESS_CONTRACT.upgradeIdentityIsImmutable, true);
  const source = await import('node:fs/promises').then(({ readFile }) =>
    readFile(new URL('./postgres-cluster.mjs', import.meta.url), 'utf8'));
  const restore = source.indexOf("options.pgRestoreBin ?? process.env.PG_RESTORE_BIN ?? 'pg_restore'");
  const installedIdentity = source.indexOf('const installedIdentity = query', restore);
  const release = source.indexOf("['scripts/cloudtms-db-release.mjs', 'apply']", installedIdentity);
  assert(restore > 0);
  assert(installedIdentity > restore);
  assert(release > installedIdentity);
  assert.doesNotMatch(source.slice(installedIdentity, release), /update\s+private\.cloudtms_database_identity/i);
});

test('TH-004 option guard requires NEW/UPGRADE, an explicit run ID and absolute worktree', () => {
  assert.equal(validatePostgresHarnessOptions({ mode: 'NEW', runId: 'plan6-proof', repoRoot: process.cwd() }).database, 'banking_modal_v2_test');
  assert.throws(() => validatePostgresHarnessOptions({ mode: 'LIVE', runId: 'plan6-proof', repoRoot: process.cwd() }), /NEW or UPGRADE/);
  assert.throws(() => validatePostgresHarnessOptions({ mode: 'NEW', runId: 'bad id', repoRoot: process.cwd() }), /run ID/);
});

function fakeRunner({ version = '17.10', versionNumber = '170010' } = {}) {
  const calls = [];
  let containerExists = false;
  let volumeExists = false;
  const labels = JSON.stringify({
    'cloudtms.owner': 'weekly-source-harness',
    'cloudtms.run': 'plan6-version-new',
    'cloudtms.mode': 'NEW',
  });
  return {
    calls,
    run(command, args) {
      calls.push([command, ...args]);
      if (command === 'git') return { status: 0, stdout: `${'a'.repeat(40)}\n`, stderr: '' };
      if (command === 'psql') {
        if (args.includes('-f')) return { status: 0, stdout: 'fixture passed\n', stderr: '' };
        const sql = args[args.indexOf('-c') + 1];
        if (sql.includes("to_regnamespace('extensions')") && sql.includes("to_regclass('auth.users')")) {
          return { status: 0, stdout: '1\n', stderr: '' };
        }
        if (sql.includes("server_version_num")) return { status: 0, stdout: `${versionNumber}\n`, stderr: '' };
        if (sql.includes("server_version")) return { status: 0, stdout: `${version}\n`, stderr: '' };
        if (sql.includes('current_database')) return { status: 0, stdout: 'banking_modal_v2_test\n', stderr: '' };
      }
      if (command === process.execPath) return { status: 0, stdout: 'release passed\n', stderr: '' };
      if (command !== 'docker') return { status: 1, stdout: '', stderr: 'unsupported' };
      if (args[0] === 'version') return { status: 0, stdout: '29.7.2\n', stderr: '' };
      if (args[0] === 'image') return { status: 0, stdout: '[]', stderr: '' };
      if (args[0] === 'volume' && args[1] === 'create') { volumeExists = true; return { status: 0, stdout: `${args.at(-1)}\n`, stderr: '' }; }
      if (args[0] === 'run') { containerExists = true; return { status: 0, stdout: 'fake-container-id\n', stderr: '' }; }
      if (args[0] === 'exec') return { status: 0, stdout: 'accepting connections\n', stderr: '' };
      if (args[0] === 'inspect' && args.some((value) => value.includes('HostPort'))) return { status: 0, stdout: '55499\n', stderr: '' };
      if (args[0] === 'inspect') return containerExists ? { status: 0, stdout: `${labels}\n`, stderr: '' } : { status: 1, stdout: '', stderr: 'absent' };
      if (args[0] === 'volume' && args[1] === 'inspect') return volumeExists ? { status: 0, stdout: `${labels}\n`, stderr: '' } : { status: 1, stdout: '', stderr: 'absent' };
      if (args[0] === 'rm') { containerExists = false; return { status: 0, stdout: '', stderr: '' }; }
      if (args[0] === 'volume' && args[1] === 'rm') { volumeExists = false; return { status: 0, stdout: '', stderr: '' }; }
      return { status: 1, stdout: '', stderr: 'unsupported docker call' };
    },
  };
}

test('TH-004 wrong PostgreSQL patch version fails closed and cleans only exact owned resources', async () => {
  const runner = fakeRunner();
  await assert.rejects(
    () => runPostgresHarness({
      mode: 'NEW',
      runId: 'plan6-version-new',
      repoRoot: process.cwd(),
      psqlBin: 'psql',
      commandRunner: runner,
    }),
    (error) => error.code === 'POSTGRES_HARNESS_VERSION_REFUSED',
  );
  const text = JSON.stringify(runner.calls);
  assert(!text.includes('codex-weekly-source-plan6-pg17'));
  assert(text.includes('codex-weekly-source-harness-new-plan6-version-new-pg17'));
  assert(runner.calls.some((call) => call[1] === 'rm' && call[2] === '-f'));
  assert(runner.calls.some((call) => call[1] === 'volume' && call[2] === 'rm'));
  assert(!runner.calls.some((call) => call.includes('prune')));
});

test('TH-004 exact PostgreSQL 17.11 still refuses success without scenario and cleanup evidence', async () => {
  const runner = fakeRunner({ version: '17.11 (Debian 17.11-1.pgdg13+1)', versionNumber: '170011' });
  await assert.rejects(
    () => runPostgresHarness({
      mode: 'NEW',
      runId: 'plan6-version-new',
      repoRoot: process.cwd(),
      psqlBin: 'psql',
      commandRunner: runner,
    }),
    (error) => error.code === 'POSTGRES_HARNESS_SCENARIO_EXECUTOR_REQUIRED',
  );
});

test('TH-004 an explicitly preserved failed run can reuse its exact owned cluster and cleans it after a pass', async () => {
  const runner = fakeRunner({ version: '17.11 (Debian 17.11-1.pgdg13+1)', versionNumber: '170011' });
  await assert.rejects(
    () => runPostgresHarness({
      mode: 'NEW',
      runId: 'plan6-version-new',
      repoRoot: process.cwd(),
      psqlBin: 'psql',
      commandRunner: runner,
      preserveOnFailure: true,
    }),
    (error) => error.code === 'POSTGRES_HARNESS_SCENARIO_EXECUTOR_REQUIRED',
  );
  assert.equal(runner.calls.filter((call) => call[0] === 'docker' && call[1] === 'run').length, 1);
  assert.equal(runner.calls.some((call) => call[1] === 'rm'), false);

  const result = await runPostgresHarness({
    mode: 'NEW',
    runId: 'plan6-version-new',
    repoRoot: process.cwd(),
    psqlBin: 'psql',
    commandRunner: runner,
    reuseOwnedResources: true,
    executeScenarios: async () => ({ complete: true, cleanup: { complete: true } }),
  });
  assert.equal(result.cleanup.complete, true);
  assert.equal(runner.calls.filter((call) => call[0] === 'docker' && call[1] === 'run').length, 1);
  assert(runner.calls.some((call) => call[1] === 'rm' && call[2] === '-f'));
});

test('TH-004 the scenario executor is handed a named-connection group bound to the proved target', async () => {
  const runner = fakeRunner({ version: '17.11 (Debian 17.11-1.pgdg13+1)', versionNumber: '170011' });
  let received = null;
  await runPostgresHarness({
    mode: 'NEW',
    runId: 'plan6-version-new',
    repoRoot: process.cwd(),
    psqlBin: 'psql',
    commandRunner: runner,
    executeScenarios: async (context) => {
      received = context;
      return { complete: true, cleanup: { complete: true } };
    },
  });
  assert.equal(typeof received.openNamedSessionGroup, 'function');
  // 23A section 13 needs two or more named connections.
  await assert.rejects(
    () => received.openNamedSessionGroup({ database: 'banking_modal_v2_test', names: ['solo'] }),
    (error) => error.code === 'NAMED_SESSION_GROUP_TOO_SMALL',
  );
  await assert.rejects(
    () => received.openNamedSessionGroup({ database: 'Not-A-Database', names: ['one', 'two'] }),
    (error) => error.code === 'NAMED_SESSION_DATABASE_INVALID',
  );
  // The group inherits the exact host, port and user the controller proved. A scenario
  // cannot substitute its own target, so named sessions cannot leave the local container.
  for (const override of [
    { baseConnectionUrl: 'postgresql://postgres@10.0.0.1:5432/elsewhere' },
    { expectedPort: 5432 },
  ]) {
    await assert.rejects(
      () => received.openNamedSessionGroup({ database: 'banking_modal_v2_test', names: ['one', 'two'], ...override }),
      (error) => error.code === 'POSTGRES_HARNESS_NAMED_SESSION_TARGET_REFUSED',
      JSON.stringify(override),
    );
  }
});
