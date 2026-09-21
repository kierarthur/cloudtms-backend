import { createHash } from 'node:crypto';
import { lstat, readFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { canonicalDigest, cloneJson, deepFreeze } from './canonical-json.mjs';
import { openNamedSessionGroup } from './named-connections.mjs';

const IMAGE = 'postgres:17.11-bookworm';
const DATABASE = 'banking_modal_v2_test';
const OWNER_LABEL = 'cloudtms.owner=weekly-source-harness';
const RUN_ID_PATTERN = /^[a-z0-9](?:[a-z0-9-]{1,38}[a-z0-9])$/;
const SNAPSHOT_CUSTOMER_KEY_PATTERN = /^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,198}[A-Za-z0-9])?$/;
const UPGRADE_RESTORE_ROLE_BOOTSTRAP_SQL = `
  do $weekly_source_upgrade_roles$
  begin
    if not exists (select 1 from pg_roles where rolname='anon') then
      create role anon inherit nologin nobypassrls;
    end if;
    if not exists (select 1 from pg_roles where rolname='authenticated') then
      create role authenticated inherit nologin nobypassrls;
    end if;
    if not exists (select 1 from pg_roles where rolname='service_role') then
      create role service_role inherit nologin bypassrls;
    end if;
    if not exists (select 1 from pg_roles where rolname='authenticator') then
      create role authenticator inherit nologin nobypassrls;
    end if;
    if not exists (select 1 from pg_roles where rolname='supabase_admin') then
      create role supabase_admin inherit nologin nobypassrls;
    end if;
  end
  $weekly_source_upgrade_roles$;
`;

const UPGRADE_LEGACY_WEEKLY_ACL_PROOF_SQL = `
  select (
    pg_catalog.has_function_privilege('service_role','public.weekly_import_apply_phase2(uuid,text)','EXECUTE')
    and pg_catalog.has_function_privilege('authenticated','public.weekly_import_apply_phase2(uuid,text)','EXECUTE')
    and not pg_catalog.has_function_privilege('anon','public.weekly_import_apply_phase2(uuid,text)','EXECUTE')
    and pg_catalog.has_function_privilege('service_role','public.weekly_import_phase2(uuid,text)','EXECUTE')
    and pg_catalog.has_function_privilege('authenticated','public.weekly_import_phase2(uuid,text)','EXECUTE')
    and not pg_catalog.has_function_privilege('anon','public.weekly_import_phase2(uuid,text)','EXECUTE')
    and pg_catalog.has_function_privilege('service_role','public.weekly_import_changed_hours_phase3(uuid,text)','EXECUTE')
    and not pg_catalog.has_function_privilege('authenticated','public.weekly_import_changed_hours_phase3(uuid,text)','EXECUTE')
    and not pg_catalog.has_function_privilege('anon','public.weekly_import_changed_hours_phase3(uuid,text)','EXECUTE')
    and not pg_catalog.has_function_privilege('service_role','public.weekly_import_apply_cancellations(uuid,jsonb,uuid)','EXECUTE')
    and not pg_catalog.has_function_privilege('authenticated','public.weekly_import_apply_cancellations(uuid,jsonb,uuid)','EXECUTE')
    and not pg_catalog.has_function_privilege('anon','public.weekly_import_apply_cancellations(uuid,jsonb,uuid)','EXECUTE')
    and not pg_catalog.has_function_privilege('service_role','public.weekly_import_create_cancellation_corrections(uuid,uuid,uuid)','EXECUTE')
    and not pg_catalog.has_function_privilege('authenticated','public.weekly_import_create_cancellation_corrections(uuid,uuid,uuid)','EXECUTE')
    and not pg_catalog.has_function_privilege('anon','public.weekly_import_create_cancellation_corrections(uuid,uuid,uuid)','EXECUTE')
  )::integer;
`;

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourcePostgresHarnessError';
  error.code = code;
  error.details = deepFreeze(cloneJson(details));
  throw error;
}

function safeResult(result, label) {
  if (!result || !Number.isInteger(result.status)) fail('POSTGRES_HARNESS_COMMAND_INVALID', `${label} returned no exit status.`);
  return {
    status: result.status,
    stdout: String(result.stdout ?? ''),
    stderr: String(result.stderr ?? ''),
  };
}

export function createLocalCommandRunner() {
  return Object.freeze({
    run(command, args, options = {}) {
      const result = spawnSync(command, args, {
        cwd: options.cwd,
        env: options.env,
        input: options.input,
        encoding: 'utf8',
        timeout: options.timeoutMs ?? 120_000,
        maxBuffer: options.maxBuffer ?? 64 * 1024 * 1024,
        windowsHide: true,
      });
      if (result.error) {
        return { status: -1, stdout: '', stderr: result.error.code || result.error.name };
      }
      return safeResult(result, command);
    },
  });
}

function requireSuccess(runner, command, args, options, code, message) {
  const result = safeResult(runner.run(command, args, options), command);
  if (result.status !== 0) {
    const diagnostic = String(result.stderr || result.stdout || '')
      .replace(/postgres(?:ql)?:\/\/\S+/gi, '[local database target redacted]')
      .replace(/\bBearer\s+\S+/gi, 'Bearer [redacted]')
      .replace(/\b(password|passphrase|secret|api[_-]?key|authorization)\s*[:=]\s*\S+/gi, '$1=[redacted]')
      .trim()
      .slice(-2000);
    fail(code, diagnostic ? `${message} ${diagnostic}` : message, {
      exitStatus: result.status,
      errorKind: result.stderr.trim().split(/\s+/)[0] || null,
    });
  }
  return result;
}

function names(mode, runId) {
  const stem = `codex-weekly-source-harness-${mode.toLowerCase()}-${runId}`;
  return { container: `${stem}-pg17`, volume: `${stem}-pgdata` };
}

function localUrl(port) {
  if (!Number.isInteger(port) || port < 1024 || port > 65535) fail('POSTGRES_HARNESS_PORT_INVALID', 'Docker returned an invalid local PostgreSQL port.');
  return `postgresql://postgres@127.0.0.1:${port}/${DATABASE}`;
}

function assertNoConnectionEvidence(value, location = '$') {
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertNoConnectionEvidence(item, `${location}[${index}]`));
    return;
  }
  if (value && typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      if (/database[_-]?url|connection[_-]?(?:url|string)|password|secret/i.test(key)) {
        fail('POSTGRES_HARNESS_EVIDENCE_UNSAFE', `${location}.${key} cannot be written to harness evidence.`);
      }
      assertNoConnectionEvidence(item, `${location}.${key}`);
    }
    return;
  }
  if (typeof value === 'string' && /postgres(?:ql)?:\/\//i.test(value)) {
    fail('POSTGRES_HARNESS_EVIDENCE_UNSAFE', `${location} contains a database connection target.`);
  }
}

function assertConnectionTarget(connectionUrl, expectedPort) {
  let url;
  try {
    url = new URL(connectionUrl);
  } catch {
    fail('POSTGRES_HARNESS_TARGET_INVALID', 'The local PostgreSQL connection target is invalid.');
  }
  const database = decodeURIComponent(url.pathname.replace(/^\//, ''));
  if (
    url.protocol !== 'postgresql:'
    || url.hostname !== '127.0.0.1'
    || Number(url.port) !== expectedPort
    || database !== DATABASE
    || decodeURIComponent(url.username) !== 'postgres'
    || url.password
  ) {
    fail('POSTGRES_HARNESS_TARGET_REFUSED', 'The PostgreSQL target is not the exact disposable local harness database.');
  }
}

export function assertPostgresHarnessConnectionTarget(connectionUrl, expectedPort) {
  assertConnectionTarget(connectionUrl, expectedPort);
  return true;
}

async function requireSnapshot(snapshotPath, expectedSha256) {
  if (!snapshotPath || !/^[a-f0-9]{64}$/.test(expectedSha256 ?? '')) {
    fail('POSTGRES_HARNESS_UPGRADE_SNAPSHOT_REQUIRED', 'UPGRADE requires a frozen snapshot path and its reviewed SHA-256.');
  }
  const resolved = path.resolve(snapshotPath);
  const stat = await lstat(resolved).catch(() => null);
  if (!stat?.isFile() || stat.isSymbolicLink()) {
    fail('POSTGRES_HARNESS_UPGRADE_SNAPSHOT_INVALID', 'The frozen UPGRADE snapshot must be a regular, non-symbolic-link file.');
  }
  const bytes = await readFile(resolved);
  const actual = createHash('sha256').update(bytes).digest('hex');
  if (actual !== expectedSha256) {
    fail('POSTGRES_HARNESS_UPGRADE_SNAPSHOT_HASH_MISMATCH', 'The frozen UPGRADE snapshot digest does not match its reviewed authority.');
  }
  return { path: resolved, sha256: actual };
}

export function validatePostgresHarnessUpgradeIdentity({ mode, environment, customerKey }) {
  if (mode !== 'UPGRADE') {
    if (environment || customerKey) {
      fail('POSTGRES_HARNESS_UPGRADE_IDENTITY_NOT_APPLICABLE', 'A frozen snapshot identity is accepted only for UPGRADE.');
    }
    return null;
  }
  if (environment !== 'TEST') {
    fail('POSTGRES_HARNESS_UPGRADE_ENVIRONMENT_REFUSED', 'The frozen UPGRADE snapshot must have the reviewed TEST environment identity.');
  }
  if (!SNAPSHOT_CUSTOMER_KEY_PATTERN.test(customerKey ?? '') || String(customerKey).includes('|')) {
    fail('POSTGRES_HARNESS_UPGRADE_CUSTOMER_KEY_INVALID', 'UPGRADE requires the exact reviewed non-empty snapshot customer key.');
  }
  return deepFreeze({ environment, customerKey });
}

function query(runner, psqlBin, connectionUrl, sql) {
  const result = requireSuccess(
    runner,
    psqlBin,
    [connectionUrl, '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-c', sql],
    { timeoutMs: 60_000 },
    'POSTGRES_HARNESS_QUERY_FAILED',
    'The disposable PostgreSQL proof query failed.',
  );
  return result.stdout.trim();
}

async function ensureLocalProviderPrerequisites({ mode, runner, psqlBin, connectionUrl, repoRoot }) {
  if (mode !== 'NEW') return null;
  const readiness = query(runner, psqlBin, connectionUrl, `
    select (
      to_regnamespace('extensions') is not null
      and to_regnamespace('auth') is not null
      and to_regnamespace('vault') is not null
      and to_regclass('auth.users') is not null
      and to_regclass('vault.secrets') is not null
      and to_regprocedure('vault.create_secret(text,text,text,uuid)') is not null
      and exists (select 1 from pg_roles where rolname = 'anon')
      and exists (select 1 from pg_roles where rolname = 'authenticated')
      and exists (select 1 from pg_roles where rolname = 'service_role')
      and exists (select 1 from pg_roles where rolname = 'authenticator')
      and exists (select 1 from pg_roles where rolname = 'supabase_admin')
    )::integer;
  `);
  if (readiness === '1') return 'ALREADY_PRESENT';
  const fixture = path.join(repoRoot, 'tests', 'fixtures', '28082026_1229_banking_modal_local_pg17_prerequisites.sql');
  const bytes = await readFile(fixture);
  requireSuccess(
    runner,
    psqlBin,
    [connectionUrl, '-X', '-v', 'ON_ERROR_STOP=1', '-f', fixture],
    { timeoutMs: 120_000, maxBuffer: 64 * 1024 * 1024 },
    'POSTGRES_HARNESS_PROVIDER_FIXTURE_FAILED',
    'The exact local PostgreSQL provider-prerequisite fixture failed.',
  );
  const confirmed = query(runner, psqlBin, connectionUrl, `
    select (
      to_regnamespace('extensions') is not null
      and to_regclass('auth.users') is not null
      and to_regclass('vault.secrets') is not null
      and to_regprocedure('vault.create_secret(text,text,text,uuid)') is not null
    )::integer;
  `);
  if (confirmed !== '1') fail('POSTGRES_HARNESS_PROVIDER_FIXTURE_INCOMPLETE', 'The local provider-prerequisite fixture did not establish its exact contract.');
  return createHash('sha256').update(bytes).digest('hex');
}

function ensureUpgradeRestoreRoles({ mode, runner, psqlBin, connectionUrl }) {
  if (mode !== 'UPGRADE') return null;
  query(runner, psqlBin, connectionUrl, UPGRADE_RESTORE_ROLE_BOOTSTRAP_SQL);
  const confirmed = query(runner, psqlBin, connectionUrl, `
    select (
      (select count(*) from pg_roles
       where rolname in ('anon','authenticated','service_role','authenticator','supabase_admin'))=5
      and not exists (
        select 1 from pg_roles
        where rolname in ('anon','authenticated','authenticator','supabase_admin')
          and (rolcanlogin or rolbypassrls or not rolinherit)
      )
      and exists (
        select 1 from pg_roles
        where rolname='service_role' and not rolcanlogin and rolbypassrls and rolinherit
      )
    )::integer;
  `);
  if (confirmed !== '1') {
    fail('POSTGRES_HARNESS_UPGRADE_ROLE_BOOTSTRAP_INVALID', 'The UPGRADE restore roles do not match the frozen local provider contract.');
  }
  return createHash('sha256').update(UPGRADE_RESTORE_ROLE_BOOTSTRAP_SQL).digest('hex');
}

async function waitForReady(runner, container) {
  for (let attempt = 0; attempt < 120; attempt += 1) {
    const result = safeResult(runner.run('docker', ['exec', container, 'pg_isready', '-U', 'postgres', '-d', DATABASE]), 'docker');
    if (result.status === 0) return;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  fail('POSTGRES_HARNESS_START_TIMEOUT', 'The task-owned PostgreSQL container did not become ready.');
}

function inspectOwned(runner, kind, name, runId, mode) {
  const command = kind === 'container' ? ['inspect', '--format', '{{json .Config.Labels}}', name] : ['volume', 'inspect', '--format', '{{json .Labels}}', name];
  const result = safeResult(runner.run('docker', command), 'docker');
  if (result.status !== 0) return { exists: false, owned: false };
  let labels;
  try {
    labels = JSON.parse(result.stdout.trim());
  } catch {
    fail('POSTGRES_HARNESS_OWNERSHIP_INVALID', `Cannot verify ownership of ${kind} ${name}.`);
  }
  const owned = labels['cloudtms.owner'] === 'weekly-source-harness'
    && labels['cloudtms.run'] === runId
    && labels['cloudtms.mode'] === mode;
  return { exists: true, owned };
}

function cleanupOwned(runner, resourceNames, runId, mode) {
  const result = { containerRemoved: false, volumeRemoved: false };
  const container = inspectOwned(runner, 'container', resourceNames.container, runId, mode);
  if (container.exists) {
    if (!container.owned) fail('POSTGRES_HARNESS_CLEANUP_OWNERSHIP_REFUSED', 'Refusing to remove an unowned Docker container.');
    requireSuccess(runner, 'docker', ['rm', '-f', resourceNames.container], {}, 'POSTGRES_HARNESS_CLEANUP_FAILED', 'Task-owned PostgreSQL container cleanup failed.');
    result.containerRemoved = true;
  }
  const volume = inspectOwned(runner, 'volume', resourceNames.volume, runId, mode);
  if (volume.exists) {
    if (!volume.owned) fail('POSTGRES_HARNESS_CLEANUP_OWNERSHIP_REFUSED', 'Refusing to remove an unowned Docker volume.');
    requireSuccess(runner, 'docker', ['volume', 'rm', resourceNames.volume], {}, 'POSTGRES_HARNESS_CLEANUP_FAILED', 'Task-owned PostgreSQL volume cleanup failed.');
    result.volumeRemoved = true;
  }
  const containerAfter = inspectOwned(runner, 'container', resourceNames.container, runId, mode);
  const volumeAfter = inspectOwned(runner, 'volume', resourceNames.volume, runId, mode);
  if (containerAfter.exists || volumeAfter.exists) fail('POSTGRES_HARNESS_CLEANUP_RESIDUE', 'Task-owned PostgreSQL resources remain after cleanup.');
  return deepFreeze({ ...result, complete: true, freshProbe: 'ABSENT' });
}

function releaseEnvironment({ mode, connectionUrl, repoRoot, environment, customerKey, psqlBin, runner }) {
  const head = requireSuccess(runner, 'git', ['rev-parse', 'HEAD'], { cwd: repoRoot }, 'POSTGRES_HARNESS_GIT_HEAD_FAILED', 'Cannot establish the exact repository commit.').stdout.trim();
  if (!/^[a-f0-9]{40}$/.test(head)) fail('POSTGRES_HARNESS_GIT_HEAD_INVALID', 'The repository commit identity is invalid.');
  return {
    ...process.env,
    CLOUDTMS_ALLOW_LOCAL: '1',
    CLOUDTMS_DATABASE_URL: connectionUrl,
    CLOUDTMS_EXPECTED_DATABASE: DATABASE,
    CLOUDTMS_ENVIRONMENT: environment,
    CLOUDTMS_RELEASE_MODE: mode,
    CLOUDTMS_CUSTOMER_KEY: customerKey,
    CLOUDTMS_LOGICAL_POSTGRES_OWNER: 'CURRENT_USER',
    CLOUDTMS_RELEASE_APPROVAL: `APPLY TEST ${mode} ${head}`,
    PSQL_BIN: psqlBin,
  };
}

export function validatePostgresHarnessOptions({ mode, runId, repoRoot }) {
  if (!['NEW', 'UPGRADE'].includes(mode)) fail('POSTGRES_HARNESS_MODE_INVALID', 'Database harness mode must be NEW or UPGRADE.');
  if (!RUN_ID_PATTERN.test(runId ?? '')) fail('POSTGRES_HARNESS_RUN_ID_INVALID', 'A short, explicit task run ID is required.');
  if (!path.isAbsolute(repoRoot ?? '')) fail('POSTGRES_HARNESS_REPOSITORY_INVALID', 'An absolute backend worktree path is required.');
  return deepFreeze({ mode, runId, repoRoot: path.resolve(repoRoot), ...names(mode, runId), database: DATABASE, image: IMAGE });
}

export async function runPostgresHarness(options) {
  const checked = validatePostgresHarnessOptions(options);
  const reviewedSnapshot = checked.mode === 'UPGRADE'
    ? await requireSnapshot(options.upgradeSnapshotPath, options.upgradeSnapshotSha256)
    : null;
  const reviewedUpgradeIdentity = validatePostgresHarnessUpgradeIdentity({
    mode: checked.mode,
    environment: options.upgradeExpectedEnvironment,
    customerKey: options.upgradeExpectedCustomerKey,
  });
  const runner = options.commandRunner ?? createLocalCommandRunner();
  const psqlBin = options.psqlBin ?? process.env.PSQL_BIN ?? 'psql';
  const resourceNames = { container: checked.container, volume: checked.volume };
  const environment = reviewedUpgradeIdentity?.environment ?? 'TEST';
  const customerKey = reviewedUpgradeIdentity?.customerKey ?? `weekly-source-harness-${checked.runId}`;
  let createdVolume = false;
  let createdContainer = false;
  let managedResources = false;
  let cleanup = null;
  let failure = null;
  let successResult = null;
  try {
    requireSuccess(runner, 'docker', ['version', '--format', '{{.Server.Version}}'], {}, 'POSTGRES_HARNESS_DOCKER_UNAVAILABLE', 'Docker Desktop is unavailable.');
    const preContainer = inspectOwned(runner, 'container', checked.container, checked.runId, checked.mode);
    const preVolume = inspectOwned(runner, 'volume', checked.volume, checked.runId, checked.mode);
    if (preContainer.exists !== preVolume.exists) {
      fail('POSTGRES_HARNESS_RESOURCE_SET_INCOMPLETE', 'The task-owned PostgreSQL container and volume must either both exist or both be absent.');
    }
    if (preContainer.exists) {
      if (!preContainer.owned || !preVolume.owned) {
        fail('POSTGRES_HARNESS_RESOURCE_ALREADY_EXISTS', 'The exact Docker resource name is not owned by this harness run.');
      }
      if (options.reuseOwnedResources !== true) {
        fail('POSTGRES_HARNESS_RESOURCE_ALREADY_EXISTS', 'The exact task-owned Docker resource name already exists; explicit reuse is required.');
      }
      managedResources = true;
    }
    let image = safeResult(runner.run('docker', ['image', 'inspect', IMAGE]), 'docker');
    if (image.status !== 0 && options.allowImagePull === true) {
      requireSuccess(runner, 'docker', ['pull', IMAGE], { timeoutMs: 600_000 }, 'POSTGRES_HARNESS_IMAGE_PULL_FAILED', 'The exact PostgreSQL 17.11 image could not be obtained.');
      image = safeResult(runner.run('docker', ['image', 'inspect', IMAGE]), 'docker');
    }
    if (image.status !== 0) fail('POSTGRES_HARNESS_IMAGE_MISSING', `The exact ${IMAGE} image is unavailable; no substitute version is permitted.`);

    if (!managedResources) {
      requireSuccess(runner, 'docker', [
        'volume', 'create',
        '--label', OWNER_LABEL,
        '--label', `cloudtms.run=${checked.runId}`,
        '--label', `cloudtms.mode=${checked.mode}`,
        checked.volume,
      ], {}, 'POSTGRES_HARNESS_VOLUME_CREATE_FAILED', 'Task-owned PostgreSQL volume creation failed.');
      createdVolume = true;
      requireSuccess(runner, 'docker', [
        'run', '-d',
        '--name', checked.container,
        '--label', OWNER_LABEL,
        '--label', `cloudtms.run=${checked.runId}`,
        '--label', `cloudtms.mode=${checked.mode}`,
        '-e', 'POSTGRES_HOST_AUTH_METHOD=trust',
        '-e', `POSTGRES_DB=${DATABASE}`,
        '-p', '127.0.0.1::5432',
        '-v', `${checked.volume}:/var/lib/postgresql/data`,
        IMAGE,
      ], { timeoutMs: 120_000 }, 'POSTGRES_HARNESS_CONTAINER_CREATE_FAILED', 'Task-owned PostgreSQL container creation failed.');
      createdContainer = true;
      managedResources = true;
    }
    await waitForReady(runner, checked.container);
    const portText = requireSuccess(
      runner,
      'docker',
      ['inspect', '--format', '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}', checked.container],
      {},
      'POSTGRES_HARNESS_PORT_DISCOVERY_FAILED',
      'Cannot discover the task-owned local PostgreSQL port.',
    ).stdout.trim();
    const port = Number(portText);
    const connectionUrl = localUrl(port);
    assertConnectionTarget(connectionUrl, port);
    const version = query(runner, psqlBin, connectionUrl, "select current_setting('server_version');");
    const versionNumber = query(runner, psqlBin, connectionUrl, "select current_setting('server_version_num');");
    const database = query(runner, psqlBin, connectionUrl, 'select current_database();');
    // Official images may append a distro build suffix to server_version.
    // server_version_num is the exact machine contract; the text guard keeps
    // the evidence human-readable without rejecting that legitimate suffix.
    if (!/^17\.11(?:\s|$|\()/.test(version) || versionNumber !== '170011') {
      fail('POSTGRES_HARNESS_VERSION_REFUSED', `PostgreSQL 17.11 is required; the task-owned container reported ${version}.`);
    }
    if (database !== DATABASE) fail('POSTGRES_HARNESS_DATABASE_REFUSED', 'The task-owned container opened the wrong database.');

    const providerFixtureSha256 = await ensureLocalProviderPrerequisites({
      mode: checked.mode,
      runner,
      psqlBin,
      connectionUrl,
      repoRoot: checked.repoRoot,
    });

    let snapshot = null;
    const upgradeRestoreRoleBootstrapSha256 = ensureUpgradeRestoreRoles({
      mode: checked.mode,
      runner,
      psqlBin,
      connectionUrl,
    });
    if (checked.mode === 'UPGRADE') {
      snapshot = reviewedSnapshot;
      requireSuccess(
        runner,
        options.pgRestoreBin ?? process.env.PG_RESTORE_BIN ?? 'pg_restore',
        ['--exit-on-error', '--no-owner', '--dbname', connectionUrl, snapshot.path],
        { timeoutMs: 600_000, maxBuffer: 256 * 1024 * 1024 },
        'POSTGRES_HARNESS_UPGRADE_RESTORE_FAILED',
        'The frozen UPGRADE snapshot could not be restored.',
      );
      const installedIdentity = query(runner, psqlBin, connectionUrl, `
        select environment || '|' || coalesce(customer_key,'')
        from private.cloudtms_database_identity
        where singleton;
      `);
      if (installedIdentity !== `${environment}|${customerKey}`) {
        fail('POSTGRES_HARNESS_UPGRADE_IDENTITY_MISMATCH', 'The restored database identity does not match the reviewed frozen-snapshot identity.');
      }
      if (query(runner, psqlBin, connectionUrl, UPGRADE_LEGACY_WEEKLY_ACL_PROOF_SQL) !== '1') {
        fail(
          'POSTGRES_HARNESS_UPGRADE_LEGACY_ACL_MISMATCH',
          'The restored snapshot does not preserve the exact legacy Weekly import permissions.',
        );
      }
    }

    const release = requireSuccess(
      runner,
      process.execPath,
      ['scripts/cloudtms-db-release.mjs', 'apply'],
      {
        cwd: checked.repoRoot,
        env: releaseEnvironment({
          mode: checked.mode,
          connectionUrl,
          repoRoot: checked.repoRoot,
          environment,
          customerKey,
          psqlBin,
          runner,
        }),
        timeoutMs: options.releaseTimeoutMs ?? 3_600_000,
        maxBuffer: 256 * 1024 * 1024,
      },
      'POSTGRES_HARNESS_RELEASE_FAILED',
      `The repository ${checked.mode} release owner failed.`,
    );
    if (typeof options.executeScenarios !== 'function') {
      fail('POSTGRES_HARNESS_SCENARIO_EXECUTOR_REQUIRED', 'Database release proof is incomplete without transaction scenario execution.');
    }
    const scenarioEvidence = await options.executeScenarios({
      mode: checked.mode,
      connectionUrl,
      database: DATABASE,
      serverVersion: version,
      // 23A section 13: advisory-lock and concurrency proofs need a dedicated serial group
      // with two or more named connections. The scenario executor receives that capability
      // bound to the target this controller has already proved, so it cannot drift.
      openNamedSessionGroup: async (request = {}) => {
        if ('baseConnectionUrl' in request || 'expectedPort' in request) {
          fail('POSTGRES_HARNESS_NAMED_SESSION_TARGET_REFUSED', 'A scenario cannot supply its own named-session target; the proved local target is used.');
        }
        return openNamedSessionGroup({
          ...request,
          baseConnectionUrl: connectionUrl,
          expectedPort: port,
          psqlBin,
        });
      },
    });
    if (scenarioEvidence?.complete !== true || scenarioEvidence?.cleanup?.complete !== true) {
      fail('POSTGRES_HARNESS_SCENARIOS_INCOMPLETE', 'Database transaction scenarios or their row cleanup proof are incomplete.');
    }
    assertNoConnectionEvidence(scenarioEvidence);
    successResult = {
      schemaVersion: 'WEEKLY_SOURCE_POSTGRES_HARNESS_RESULT_V1',
      mode: checked.mode,
      database,
      serverVersion: version,
      image: IMAGE,
      releaseOutputDigest: createHash('sha256').update(release.stdout).digest('hex'),
      snapshotSha256: snapshot?.sha256 ?? null,
      upgradeIdentitySha256: reviewedUpgradeIdentity
        ? createHash('sha256').update(`${snapshot.sha256}|${environment}|${customerKey}`).digest('hex')
        : null,
      upgradeRestoreRoleBootstrapSha256,
      providerFixtureSha256,
      scenarioEvidence: cloneJson(scenarioEvidence),
      resourceNames,
    };
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    if (managedResources && !(failure && options.preserveOnFailure === true)) {
      try {
        cleanup = cleanupOwned(runner, resourceNames, checked.runId, checked.mode);
      } catch (cleanupError) {
        if (!failure) throw cleanupError;
        failure.cleanupErrorCode = cleanupError.code ?? 'POSTGRES_HARNESS_CLEANUP_FAILED';
      }
    } else if (managedResources && failure) {
      cleanup = deepFreeze({
        complete: false,
        preservedForDiagnosticRerun: true,
        container: resourceNames.container,
        volume: resourceNames.volume,
      });
    }
  }
  return deepFreeze({ ...successResult, cleanup });
}

export const WEEKLY_SOURCE_POSTGRES_HARNESS_CONTRACT = deepFreeze({
  version: 'WEEKLY_SOURCE_POSTGRES_HARNESS_V1',
  image: IMAGE,
  database: DATABASE,
  localHostOnly: true,
  exactServerVersion: '17.11',
  broadPrune: false,
  preservesUnownedResources: true,
  explicitOwnedReuse: true,
  optionalPreserveOnFailure: true,
  namedConnectionGroups: true,
  upgradeRestoreRoles: Object.freeze(['anon', 'authenticated', 'service_role', 'authenticator', 'supabase_admin']),
  upgradeIdentityIsImmutable: true,
});
