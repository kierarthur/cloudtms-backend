import { spawnSync } from 'node:child_process';
import {
  canonicalContractHash,
  exportContract,
  inventory,
  psql,
  repoRoot,
  writeJson,
} from './cloudtms-db-release-lib.mjs';

const outputPath = 'supabase/release/current-contract.json';
const localHosts = new Set(['localhost', '127.0.0.1', '::1', 'host.docker.internal']);

function requireLocalPostgres17() {
  const rawUrl = process.env.CLOUDTMS_DATABASE_URL;
  if (!rawUrl) throw new Error('CLOUDTMS_DATABASE_URL is required');

  const url = new URL(rawUrl);
  if (!localHosts.has(url.hostname)) {
    throw new Error('Contract sealing is local-only; a hosted database was refused');
  }

  process.env.CLOUDTMS_ALLOW_LOCAL = '1';
  const versionNumber = Number(psql({ sql: "select current_setting('server_version_num')" }));
  if (!Number.isInteger(versionNumber) || Math.floor(versionNumber / 10000) !== 17) {
    throw new Error('Contract sealing requires the already-proved local PostgreSQL 17 database');
  }

  const ownsPrivateSchema = psql({
    sql: "select (nspowner=(select oid from pg_catalog.pg_roles where rolname=current_user))::text from pg_catalog.pg_namespace where nspname='private'",
  });
  if (ownsPrivateSchema !== 'true') {
    throw new Error('Connect as the local proof release owner so provider-neutral ownership is generated correctly');
  }
}

function git(args) {
  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8' });
  if (result.status !== 0) {
    throw new Error((result.stderr || result.stdout || `git ${args[0]} failed`).trim());
  }
  return result.stdout;
}

function requestedReplayBase() {
  const replayArguments = process.argv.slice(2);
  if (replayArguments.length === 0) return null;
  if (replayArguments.length !== 1 || !replayArguments[0].startsWith('--replay-from=')) {
    throw new Error('Use only --replay-from=<exact 40-character ancestor commit>');
  }
  const baseCommit = replayArguments[0].slice('--replay-from='.length);
  if (!/^[0-9a-f]{40}$/.test(baseCommit)) {
    throw new Error('--replay-from requires an exact 40-character lowercase commit');
  }
  git(['merge-base', '--is-ancestor', baseCommit, 'HEAD']);
  return baseCommit;
}

function changedPaths(baseCommit, diffFilter) {
  return new Set(git([
    'diff', '--name-only', `--diff-filter=${diffFilter}`, `${baseCommit}..HEAD`, '--',
    'supabase/migrations', 'supabase/repeatable',
  ]).split(/\r?\n/).filter(Boolean).map(file => file.replaceAll('\\', '/')));
}

function replayChangedDatabaseSources(baseCommit) {
  const deleted = changedPaths(baseCommit, 'D');
  if (deleted.size > 0) {
    throw new Error(`Local replay refuses deleted database source: ${[...deleted].join(', ')}`);
  }

  const changed = changedPaths(baseCommit, 'ACMR');
  const current = inventory();
  const migrations = current.migrations.filter(item => changed.has(item.path));
  for (const migration of migrations) {
    const existedAtBase = spawnSync(
      'git', ['cat-file', '-e', `${baseCommit}:${migration.path}`],
      { cwd: repoRoot, encoding: 'utf8' },
    );
    if (existedAtBase.status === 0) {
      throw new Error(`Local replay refuses an edited historical migration: ${migration.path}`);
    }
    psql({ file: migration.path });
  }

  // inventory() is the release authority: it returns roots in sqlDateKey order.
  // Selecting from that ordered array prevents ordinary filename sorting from
  // allowing an August authority to overwrite a later September definition.
  const repeatables = current.repeatables.filter(item =>
    item.paths.some(includedPath => changed.has(includedPath))
  );
  const covered = new Set(repeatables.flatMap(item => item.paths));
  const uncovered = [...changed].filter(item =>
    item.startsWith('supabase/repeatable/') && !covered.has(item)
  );
  if (uncovered.length > 0) {
    throw new Error(`Changed repeatable source is outside the canonical inventory: ${uncovered.join(', ')}`);
  }
  for (const repeatable of repeatables) psql({ file: repeatable.path });

  console.log(
    `Local replay used canonical release order for ${migrations.length} migrations `
      + `and ${repeatables.length} repeatables since ${baseCommit}.`,
  );
}

try {
  requireLocalPostgres17();
  const replayBase = requestedReplayBase();
  if (replayBase) replayChangedDatabaseSources(replayBase);
  const contract = exportContract();
  writeJson(outputPath, contract);
  console.log(`Local PostgreSQL 17 contract sealed: ${canonicalContractHash(contract)}`);
} catch (error) {
  console.error(`Local contract sealing failed: ${error.message}`);
  process.exitCode = 1;
}
