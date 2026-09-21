import { createHash } from 'node:crypto';
import { lstat, readFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { verifyWeeklySourceComponentFileSet } from './component-database-file-set.mjs';

const IMAGE = 'postgres:17.11-bookworm';
const DATABASE = 'banking_modal_v2_test';
const OWNER = 'weekly-source-component-harness';
const RUN_ID = /^[a-z0-9](?:[a-z0-9-]{1,38}[a-z0-9])$/;
const SHA256 = /^[a-f0-9]{64}$/;

const ROLE_BOOTSTRAP = `
do $roles$
begin
  if not exists(select 1 from pg_roles where rolname='anon') then create role anon inherit nologin nobypassrls; end if;
  if not exists(select 1 from pg_roles where rolname='authenticated') then create role authenticated inherit nologin nobypassrls; end if;
  if not exists(select 1 from pg_roles where rolname='service_role') then create role service_role inherit nologin bypassrls; end if;
  if not exists(select 1 from pg_roles where rolname='authenticator') then create role authenticator inherit nologin nobypassrls; end if;
  if not exists(select 1 from pg_roles where rolname='supabase_admin') then create role supabase_admin inherit nologin nobypassrls; end if;
end
$roles$;
`;

const NEW_PROVIDER_BOOTSTRAP = `
create schema if not exists extensions;
create schema if not exists auth;
create schema if not exists storage;
create schema if not exists vault;
create schema if not exists realtime;
create table if not exists auth.users(id uuid primary key);
create or replace function auth.role() returns text language sql stable
as $auth_role$ select nullif(pg_catalog.current_setting('request.jwt.claim.role', true), '') $auth_role$;
create or replace function auth.uid() returns uuid language sql stable
as $auth_uid$ select nullif(pg_catalog.current_setting('request.jwt.claim.sub', true), '')::uuid $auth_uid$;
create or replace function auth.jwt() returns jsonb language sql stable
as $auth_jwt$ select coalesce(nullif(pg_catalog.current_setting('request.jwt.claims', true), '')::jsonb, '{}'::jsonb) $auth_jwt$;
create table if not exists vault.decrypted_secrets(
  id uuid primary key,
  secret text not null,
  decrypted_secret text not null,
  name text,
  description text
);
create or replace function vault.create_secret(p_secret text, p_name text default null, p_description text default null)
returns uuid
language plpgsql
as $vault_stub$
declare
  v_id uuid := pg_catalog.md5(pg_catalog.random()::text || pg_catalog.clock_timestamp()::text)::uuid;
begin
  insert into vault.decrypted_secrets(id, secret, decrypted_secret, name, description)
  values(v_id, p_secret, p_secret, p_name, p_description);
  return v_id;
end
$vault_stub$;
`;

const ACL_PROOF = `
select (
  has_function_privilege('service_role','public.weekly_import_apply_phase2(uuid,text)','EXECUTE')
  and has_function_privilege('authenticated','public.weekly_import_apply_phase2(uuid,text)','EXECUTE')
  and not has_function_privilege('anon','public.weekly_import_apply_phase2(uuid,text)','EXECUTE')
  and has_function_privilege('service_role','public.weekly_import_phase2(uuid,text)','EXECUTE')
  and has_function_privilege('authenticated','public.weekly_import_phase2(uuid,text)','EXECUTE')
  and not has_function_privilege('anon','public.weekly_import_phase2(uuid,text)','EXECUTE')
  and has_function_privilege('service_role','public.weekly_import_changed_hours_phase3(uuid,text)','EXECUTE')
  and not has_function_privilege('authenticated','public.weekly_import_changed_hours_phase3(uuid,text)','EXECUTE')
  and not has_function_privilege('anon','public.weekly_import_changed_hours_phase3(uuid,text)','EXECUTE')
  and not has_function_privilege('service_role','public.weekly_import_apply_cancellations(uuid,jsonb,uuid)','EXECUTE')
  and not has_function_privilege('authenticated','public.weekly_import_apply_cancellations(uuid,jsonb,uuid)','EXECUTE')
  and not has_function_privilege('anon','public.weekly_import_apply_cancellations(uuid,jsonb,uuid)','EXECUTE')
  and not has_function_privilege('service_role','public.weekly_import_create_cancellation_corrections(uuid,uuid,uuid)','EXECUTE')
  and not has_function_privilege('authenticated','public.weekly_import_create_cancellation_corrections(uuid,uuid,uuid)','EXECUTE')
  and not has_function_privilege('anon','public.weekly_import_create_cancellation_corrections(uuid,uuid,uuid)','EXECUTE')
)::integer;
`;

function fail(code, message) {
  const error = new Error(message);
  error.code = code;
  throw error;
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    env: options.env ?? process.env,
    input: options.input,
    encoding: 'utf8',
    timeout: options.timeoutMs ?? 120_000,
    maxBuffer: options.maxBuffer ?? 256 * 1024 * 1024,
    windowsHide: true,
  });
  if (result.error || result.status !== 0) {
    const diagnostic = String(result.stderr || result.stdout || result.error?.message || '')
      .replace(/postgres(?:ql)?:\/\/\S+/gi, '[local database target redacted]')
      .replace(/\b(password|passphrase|secret|authorization)\s*[:=]\s*\S+/gi, '$1=[redacted]')
      .trim().slice(-12000);
    fail(options.code ?? 'WEEKLY_SOURCE_COMPONENT_COMMAND_FAILED', `${options.message ?? 'Component proof command failed.'}${diagnostic ? ` ${diagnostic}` : ''}`);
  }
  return String(result.stdout ?? '');
}

function inspectLabels(kind, name) {
  const args = kind === 'container'
    ? ['inspect', '--format', '{{json .Config.Labels}}', name]
    : ['volume', 'inspect', '--format', '{{json .Labels}}', name];
  const result = spawnSync('docker', args, { encoding: 'utf8', windowsHide: true });
  if (result.status !== 0) return null;
  return JSON.parse(String(result.stdout).trim());
}

function assertOwned(kind, name, runId) {
  const labels = inspectLabels(kind, name);
  if (!labels || labels['cloudtms.owner'] !== OWNER || labels['cloudtms.run'] !== runId) {
    fail('WEEKLY_SOURCE_COMPONENT_RESOURCE_NOT_OWNED', `Refusing to manage unowned ${kind} ${name}.`);
  }
}

function cleanup(names, runId) {
  if (inspectLabels('container', names.container)) {
    assertOwned('container', names.container, runId);
    run('docker', ['rm', '-f', names.container], { message: 'Component PostgreSQL container cleanup failed.' });
  }
  if (inspectLabels('volume', names.volume)) {
    assertOwned('volume', names.volume, runId);
    run('docker', ['volume', 'rm', names.volume], { message: 'Component PostgreSQL volume cleanup failed.' });
  }
  if (inspectLabels('container', names.container) || inspectLabels('volume', names.volume)) {
    fail('WEEKLY_SOURCE_COMPONENT_CLEANUP_RESIDUE', 'Component PostgreSQL resources remain after cleanup.');
  }
  return { complete: true, containerRemoved: true, volumeRemoved: true, freshProbe: 'ABSENT' };
}

function psql(psqlBin, connectionUrl, args, options = {}) {
  return run(psqlBin, [connectionUrl, '-X', '-v', 'ON_ERROR_STOP=1', ...args], {
    ...options,
    timeoutMs: options.timeoutMs ?? 1_200_000,
  });
}

function query(psqlBin, connectionUrl, sql) {
  return psql(psqlBin, connectionUrl, ['-q', '-A', '-t', '-c', sql], {
    message: 'Component PostgreSQL query failed.',
  }).trim();
}

async function waitReady(container) {
  for (let attempt = 0; attempt < 120; attempt += 1) {
    // The official image briefly starts an init-time PostgreSQL server and
    // then shuts it down before exec'ing the final server as PID 1.  A plain
    // pg_isready can therefore return a false green during that planned
    // restart.  Require the final PID 1 server as well as an accepting target.
    const result = spawnSync('docker', [
      'exec', container, 'sh', '-c',
      `test "$(cat /proc/1/comm)" = postgres && pg_isready -U postgres -d ${DATABASE}`,
    ], { encoding: 'utf8', windowsHide: true });
    if (result.status === 0) return;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  fail('WEEKLY_SOURCE_COMPONENT_START_TIMEOUT', 'Component PostgreSQL did not become ready.');
}

async function requireSnapshot(snapshotPath, expectedSha256) {
  if (!snapshotPath || !SHA256.test(expectedSha256 ?? '')) fail('WEEKLY_SOURCE_COMPONENT_SNAPSHOT_REQUIRED', 'The reviewed upgrade snapshot and SHA-256 are required.');
  const resolved = path.resolve(snapshotPath);
  const stat = await lstat(resolved).catch(() => null);
  if (!stat?.isFile() || stat.isSymbolicLink()) fail('WEEKLY_SOURCE_COMPONENT_SNAPSHOT_INVALID', 'The component snapshot is unavailable.');
  const actual = createHash('sha256').update(await readFile(resolved)).digest('hex');
  if (actual !== expectedSha256) fail('WEEKLY_SOURCE_COMPONENT_SNAPSHOT_HASH_MISMATCH', 'The component snapshot hash does not match its authority.');
  return { path: resolved, sha256: actual };
}

async function readJsonFile(file, code) {
  const stat = await lstat(file).catch(() => null);
  if (!stat?.isFile() || stat.isSymbolicLink()) fail(code, `Component NEW input is unavailable: ${path.basename(file)}.`);
  return JSON.parse(await readFile(file, 'utf8'));
}

async function componentNewPlan(repoRoot) {
  const release = await readJsonFile(path.join(repoRoot, 'supabase', 'release', 'current-release.json'), 'WEEKLY_SOURCE_COMPONENT_NEW_RELEASE_MISSING');
  const lock = await readJsonFile(path.join(repoRoot, 'supabase', 'release', 'migration-lock.json'), 'WEEKLY_SOURCE_COMPONENT_NEW_LOCK_MISSING');
  const controlPlaneIndex = lock.migrations.findIndex((item) => item.path === release.controlPlaneMigration);
  if (controlPlaneIndex < 0) fail('WEEKLY_SOURCE_COMPONENT_NEW_CONTROL_PLANE_MISSING', 'The component NEW control-plane anchor is absent from the migration lock.');
  const files = [
    ...release.baselineFiles,
    release.controlPlaneMigration,
    release.bootstrapFile,
    ...lock.migrations.slice(controlPlaneIndex + 1).map((item) => item.path),
  ];
  for (const relative of files) {
    const absolute = path.join(repoRoot, relative);
    const stat = await lstat(absolute).catch(() => null);
    if (!stat?.isFile() || stat.isSymbolicLink()) fail('WEEKLY_SOURCE_COMPONENT_NEW_FILE_INVALID', `Component NEW input is unavailable: ${relative}.`);
  }
  return { release, postBaselineMigrations: lock.migrations.slice(controlPlaneIndex + 1), files };
}

export async function runWeeklySourceComponentPostgresHarness(options) {
  if (!RUN_ID.test(options.runId ?? '')) fail('WEEKLY_SOURCE_COMPONENT_RUN_ID_INVALID', 'A short component run id is required.');
  if (!path.isAbsolute(options.repoRoot ?? '')) fail('WEEKLY_SOURCE_COMPONENT_REPO_INVALID', 'An absolute backend worktree path is required.');
  if (typeof options.executeScenarios !== 'function') fail('WEEKLY_SOURCE_COMPONENT_SCENARIOS_REQUIRED', 'The real database scenario executor is required.');
  const repoRoot = path.resolve(options.repoRoot);
  const mode = String(options.mode ?? 'UPGRADE').toUpperCase();
  if (!['NEW', 'UPGRADE'].includes(mode)) fail('WEEKLY_SOURCE_COMPONENT_MODE_INVALID', 'Component mode must be NEW or UPGRADE.');
  const snapshot = mode === 'UPGRADE'
    ? await requireSnapshot(options.snapshotPath, options.snapshotSha256)
    : null;
  const newPlan = mode === 'NEW' ? await componentNewPlan(repoRoot) : null;
  const fileSet = await verifyWeeklySourceComponentFileSet(repoRoot);
  const psqlBin = options.psqlBin ?? process.env.PSQL_BIN ?? 'psql';
  const pgRestoreBin = options.pgRestoreBin ?? process.env.PG_RESTORE_BIN ?? 'pg_restore';
  const names = {
    container: `codex-ws-component-${options.runId}-pg17`,
    volume: `codex-ws-component-${options.runId}-pgdata`,
  };
  let managed = false;
  let failure = null;
  let success;
  let cleanupResult = null;
  try {
    run('docker', ['version', '--format', '{{.Server.Version}}'], { message: 'Docker Desktop is unavailable.' });
    if (inspectLabels('container', names.container) || inspectLabels('volume', names.volume)) {
      fail('WEEKLY_SOURCE_COMPONENT_RESOURCE_EXISTS', 'The component run id already owns or collides with Docker resources.');
    }
    run('docker', ['image', 'inspect', IMAGE], { message: `The exact ${IMAGE} image is unavailable.` });
    run('docker', ['volume', 'create', '--label', `cloudtms.owner=${OWNER}`, '--label', `cloudtms.run=${options.runId}`, names.volume], { message: 'Component PostgreSQL volume creation failed.' });
    managed = true;
    run('docker', [
      'run', '-d', '--name', names.container,
      '--label', `cloudtms.owner=${OWNER}`, '--label', `cloudtms.run=${options.runId}`,
      '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', '-e', `POSTGRES_DB=${DATABASE}`,
      '-p', '127.0.0.1::5432', '-v', `${names.volume}:/var/lib/postgresql/data`, IMAGE,
    ], { message: 'Component PostgreSQL container creation failed.' });
    await waitReady(names.container);
    const port = Number(run('docker', ['inspect', '--format', '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}', names.container]).trim());
    if (!Number.isInteger(port) || port < 1024 || port > 65535) fail('WEEKLY_SOURCE_COMPONENT_PORT_INVALID', 'Docker returned an invalid local port.');
    const connectionUrl = `postgresql://postgres@127.0.0.1:${port}/${DATABASE}`;
    const version = query(psqlBin, connectionUrl, "select current_setting('server_version_num')||'|'||current_setting('server_version');");
    if (!/^170011\|17\.11(?:\s|$|\()/.test(version)) fail('WEEKLY_SOURCE_COMPONENT_POSTGRES_VERSION_REFUSED', `PostgreSQL 17.11 is required; received ${version}.`);
    query(psqlBin, connectionUrl, ROLE_BOOTSTRAP);
    if (mode === 'UPGRADE') {
      run(pgRestoreBin, ['--exit-on-error', '--no-owner', '--dbname', connectionUrl, snapshot.path], {
        timeoutMs: 600_000,
        message: 'The reviewed component snapshot could not be restored.',
      });
    } else {
      query(psqlBin, connectionUrl, NEW_PROVIDER_BOOTSTRAP);
      for (const relative of newPlan.release.baselineFiles) {
        psql(psqlBin, connectionUrl, ['-f', path.join(repoRoot, relative)], { message: `Component NEW baseline failed for ${relative}.` });
      }
      psql(psqlBin, connectionUrl, ['-f', path.join(repoRoot, newPlan.release.controlPlaneMigration)], { message: 'Component NEW control plane failed.' });
      query(psqlBin, connectionUrl, "insert into private.cloudtms_database_identity(singleton,environment,customer_key) values(true,'TEST','weekly-source-harness-plan62-env') on conflict(singleton) do update set environment=excluded.environment,customer_key=excluded.customer_key;");
      psql(psqlBin, connectionUrl, ['-v', 'cloudtms_environment=TEST', '-f', path.join(repoRoot, newPlan.release.bootstrapFile)], { message: 'Component NEW bootstrap failed.' });
      for (const item of newPlan.postBaselineMigrations) {
        psql(psqlBin, connectionUrl, ['-f', path.join(repoRoot, item.path)], { message: `Component NEW migration failed for ${item.path}.` });
      }
    }
    const identity = query(psqlBin, connectionUrl, "select environment||'|'||coalesce(customer_key,'') from private.cloudtms_database_identity where singleton;");
    if (identity !== 'TEST|weekly-source-harness-plan62-env') fail('WEEKLY_SOURCE_COMPONENT_IDENTITY_REFUSED', 'The component database identity is wrong.');
    if (query(psqlBin, connectionUrl, ACL_PROOF) !== '1') fail('WEEKLY_SOURCE_COMPONENT_BASELINE_ACL_REFUSED', 'The restored legacy Weekly import permissions differ from the exact TEST baseline.');

    const applied = [];
    for (const file of fileSet.files) {
      if (mode === 'NEW' && file.relative.startsWith('supabase/migrations/')) continue;
      const absolute = path.join(repoRoot, file.relative);
      const output = psql(psqlBin, connectionUrl, ['-f', absolute], {
        message: `Component SQL failed for ${file.relative}.`,
      });
      applied.push({ ...file, outputSha256: createHash('sha256').update(output).digest('hex') });
    }
    if (query(psqlBin, connectionUrl, ACL_PROOF) !== '1') fail('WEEKLY_SOURCE_COMPONENT_POST_APPLY_ACL_REFUSED', 'Component installation changed legacy Weekly import permissions.');
    const scenarioEvidence = await options.executeScenarios({
      mode,
      connectionUrl,
      database: DATABASE,
      serverVersion: version.split('|').slice(1).join('|'),
      repoRoot,
      resultDirectory: options.resultDirectory,
      runSerialOnUpgrade: mode === 'UPGRADE',
    });
    if (scenarioEvidence?.complete !== true || scenarioEvidence?.cleanup?.complete !== true) {
      fail('WEEKLY_SOURCE_COMPONENT_SCENARIOS_INCOMPLETE', 'Component database scenarios or cleanup proof are incomplete.');
    }
    if (
      scenarioEvidence.releaseEvidenceEligible !== false
      || !Array.isArray(scenarioEvidence.dependencyPending)
      || scenarioEvidence.dependencyPending.length === 0
      || scenarioEvidence.serialDependencyPending !== true
      || !/^[a-f0-9]{64}$/.test(scenarioEvidence.componentResultEnvelopeDigest ?? '')
    ) {
      fail('WEEKLY_SOURCE_COMPONENT_BOUNDARY_MISSING', 'The component proof must name its incomplete HANDOVER 2 dependency, write its bounded result envelope and remain ineligible for release evidence.');
    }
    if (
      scenarioEvidence.realWorldEvidence?.evidenceScope !== 'LOCAL_LIMB_ONLY'
      || scenarioEvidence.realWorldEvidence?.bankingPayBoundary !== 'HANDOVER2_BOUNDARY_EMULATED'
      || scenarioEvidence.realWorldEvidence?.releaseEvidenceEligible !== false
      || !scenarioEvidence.realWorldEvidence?.results?.every((item) => (
        item.evidenceScope === 'LOCAL_LIMB_ONLY'
        && item.bankingPayBoundary === 'HANDOVER2_BOUNDARY_EMULATED'
        && item.releaseEvidenceEligible === false
      ))
    ) {
      fail('WEEKLY_SOURCE_COMPONENT_REAL_WORLD_BOUNDARY_MISSING', 'Every populated component journey must be labelled LOCAL_LIMB_ONLY and HANDOVER2_BOUNDARY_EMULATED.');
    }
    success = {
      schemaVersion: 'WEEKLY_SOURCE_COMPONENT_POSTGRES_RESULT_V1',
      status: 'PASS_WITH_HANDOVER2_PENDING',
      boundary: 'WEEKLY_SOURCE_COMPONENT_ONLY_BANKING_PAY_NOT_CERTIFIED',
      evidenceScope: 'LOCAL_LIMB_ONLY',
      bankingPayBoundary: 'HANDOVER2_BOUNDARY_EMULATED',
      releaseEvidenceEligible: false,
      image: IMAGE,
      postgresVersion: version,
      mode,
      snapshotSha256: snapshot?.sha256 ?? null,
      newBaselineFileCount: newPlan?.release?.baselineFiles?.length ?? null,
      newPostBaselineMigrationCount: newPlan?.postBaselineMigrations?.length ?? null,
      baselineCommit: fileSet.baselineCommit,
      migrationCount: fileSet.migrationCount,
      repeatableCount: fileSet.repeatableCount,
      excludedHandover2Files: fileSet.excludedHandover2Files,
      pendingHandover2Verifiers: scenarioEvidence.dependencyPending,
      pendingHandover2SerialSuites: scenarioEvidence.serialSuites.suites,
      componentResultEnvelopeDigest: scenarioEvidence.componentResultEnvelopeDigest,
      appliedFiles: applied,
      scenarioEvidence,
      resources: names,
    };
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    if (managed && !(failure && options.preserveOnFailure === true)) cleanupResult = cleanup(names, options.runId);
    else if (managed) cleanupResult = { complete: false, preservedForDiagnosticRerun: true, ...names };
  }
  return { ...success, cleanup: cleanupResult };
}
