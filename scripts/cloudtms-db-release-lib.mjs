import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

let logicalOwnerSqlRoot;

export function mapLogicalPostgresOwnerSql(source) {
  const mode = process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER || '';
  if (!mode) return source;
  if (mode !== 'CURRENT_USER') {
    throw new Error('CLOUDTMS_LOGICAL_POSTGRES_OWNER must be CURRENT_USER when set');
  }
  return String(source)
    .replace(/\bowner\s+to\s+(?:"postgres"|postgres)(?=\s*;)/gi, 'OWNER TO CURRENT_USER')
    .replace(
      /\balter\s+default\s+privileges\s+for\s+role\s+(?:"postgres"|postgres)/gi,
      'ALTER DEFAULT PRIVILEGES FOR ROLE CURRENT_USER',
    );
}

export function mapGeneratedAclBaselineSql(source) {
  return mapLogicalPostgresOwnerSql(source)
    .replace(/"postgres"/g, 'CURRENT_USER')
    .replace(/^grant EXECUTE on function public\.cloudtms_data_api_mfa_gate\(\) to "authenticator";\r?\n/gm, '')
    .replace(/,\s*authenticator(?=\s*[,;])/g, '');
}

const DEADLOCK_RETRY_SAFE_FILES = new Map([
  [
    'supabase/migrations/24082026_0232_miget_provider_owner_defaults.sql',
    'c323ff65c84dae66a5bd9a4b9da83fa48af0b5df91e2424f31d9bd95b5db7f1d',
  ],
]);

export function deadlockRetryCountForFile(file) {
  const absolute = path.isAbsolute(file) ? file : path.join(repoRoot, file);
  const relative = path.relative(repoRoot, absolute).replaceAll('\\', '/');
  const expectedSha256 = DEADLOCK_RETRY_SAFE_FILES.get(relative);
  if (!expectedSha256) return 0;
  const actualSha256 = sha256(
    Buffer.from(fs.readFileSync(absolute, 'utf8').replaceAll('\r\n', '\n'), 'utf8'),
  );
  if (actualSha256 !== expectedSha256) {
    throw new Error(`Deadlock-retry-safe SQL changed: ${relative}`);
  }
  return 3;
}

function copyLogicalOwnerSqlTree(source, destination) {
  fs.mkdirSync(destination, { recursive: true });
  for (const entry of fs.readdirSync(source, { withFileTypes: true })) {
    const sourcePath = path.join(source, entry.name);
    const destinationPath = path.join(destination, entry.name);
    if (entry.isDirectory()) copyLogicalOwnerSqlTree(sourcePath, destinationPath);
    else if (entry.isFile() && entry.name.toLowerCase().endsWith('.sql')) {
      fs.writeFileSync(
        destinationPath,
        entry.name === '22082026_1505_cloudtms_test_acl_baseline.sql'
          ? mapGeneratedAclBaselineSql(fs.readFileSync(sourcePath, 'utf8'))
          : mapLogicalPostgresOwnerSql(fs.readFileSync(sourcePath, 'utf8')),
      );
    }
  }
}

function executableSqlFile(file) {
  const mode = process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER || '';
  const absolute = path.isAbsolute(file) ? file : path.join(repoRoot, file);
  if (!mode) return absolute;
  mapLogicalPostgresOwnerSql('');
  const authorityRoot = path.join(repoRoot, 'supabase');
  const relative = path.relative(authorityRoot, absolute);
  if (relative.startsWith('..') || path.isAbsolute(relative)) {
    const source = fs.readFileSync(absolute, 'utf8');
    const mapped = mapLogicalPostgresOwnerSql(source);
    if (mapped !== source) {
      throw new Error('Logical owner mapping is limited to SQL authority under supabase/');
    }
    return absolute;
  }
  if (!logicalOwnerSqlRoot) {
    logicalOwnerSqlRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'cloudtms-logical-owner-'));
    copyLogicalOwnerSqlTree(authorityRoot, path.join(logicalOwnerSqlRoot, 'supabase'));
    process.once('exit', () => {
      try { fs.rmSync(logicalOwnerSqlRoot, { recursive: true, force: true }); } catch {}
    });
  }
  return path.join(logicalOwnerSqlRoot, 'supabase', relative);
}

export function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

export function sqlDateKey(filename) {
  const base = path.basename(filename);
  let m = base.match(/^(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(.*)$/);
  if (m && validDate(+m[1], +m[2], +m[3])) return `${m[1]}${m[2]}${m[3]}${m[4]}`;
  m = base.match(/^(0[1-9]|[12]\d|3[01])(0[1-9]|1[0-2])(20\d{2})(.*)$/);
  if (m && validDate(+m[3], +m[2], +m[1])) return `${m[3]}${m[2]}${m[1]}${m[4]}`;
  return `ZZZZZZZZ_${base}`;
}

function validDate(year, month, day) {
  const d = new Date(Date.UTC(year, month - 1, day));
  return d.getUTCFullYear() === year && d.getUTCMonth() === month - 1 && d.getUTCDate() === day;
}

export function sqlFiles(relativeDir) {
  const absolute = path.join(repoRoot, relativeDir);
  const diskFiles = fs.readdirSync(absolute, { withFileTypes: true })
    .filter(entry => entry.isFile() && entry.name.toLowerCase().endsWith('.sql'))
    .map(entry => path.posix.join(relativeDir.replaceAll('\\', '/'), entry.name));
  const tracked = spawnSync('git', ['ls-files', '-z', '--', relativeDir], {
    cwd: repoRoot,
    encoding: 'utf8',
  });
  if (tracked.status !== 0) throw new Error(`Cannot enumerate committed SQL paths under ${relativeDir}`);
  const trackedFiles = tracked.stdout
    .split('\0')
    .filter(file => file && file.toLowerCase().endsWith('.sql'));
  const trackedByCaseFoldedPath = new Map(trackedFiles.map(file => [file.toLowerCase(), file]));
  const untrackedFiles = diskFiles.filter(file => !trackedByCaseFoldedPath.has(file.toLowerCase()));
  return [...trackedFiles, ...untrackedFiles]
    .sort((a, b) => sqlDateKey(a).localeCompare(sqlDateKey(b)) || a.localeCompare(b));
}

export function canonicalSqlBytes(relativeFile) {
  const bytes = fs.readFileSync(path.join(repoRoot, relativeFile));
  return Buffer.from(bytes.toString('utf8').replaceAll('\r\n', '\n'), 'utf8');
}

export function closureFor(relativeFile, allowedRoot = 'supabase/repeatable') {
  const active = new Set();
  const ordered = [];
  const allowed = path.resolve(repoRoot, allowedRoot);
  function visit(rel) {
    const absolute = path.resolve(repoRoot, rel);
    if (absolute !== allowed && !absolute.startsWith(`${allowed}${path.sep}`)) {
      throw new Error(`SQL include escapes ${allowedRoot}: ${rel}`);
    }
    const normal = path.relative(repoRoot, absolute).replaceAll('\\', '/');
    if (active.has(normal)) throw new Error(`Recursive SQL include cycle: ${normal}`);
    if (!fs.existsSync(absolute)) throw new Error(`Missing SQL include: ${normal}`);
    active.add(normal);
    const bytes = canonicalSqlBytes(normal);
    ordered.push({ path: normal, bytes });
    const text = bytes.toString('utf8');
    for (const line of text.split(/\r?\n/)) {
      const match = line.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/);
      if (!match) continue;
      const include = match[1] ?? match[2] ?? match[3];
      visit(path.relative(repoRoot, path.resolve(path.dirname(absolute), include)));
    }
    active.delete(normal);
  }
  visit(relativeFile);
  const digestInput = Buffer.concat(ordered.flatMap(item => [Buffer.from(`${item.path}\0`), item.bytes, Buffer.from('\0')]));
  return { paths: ordered.map(item => item.path), sha256: sha256(digestInput) };
}

export function inventory() {
  const migrations = sqlFiles('supabase/migrations').map(p => ({ path: p, sha256: sha256(canonicalSqlBytes(p)) }));
  const repeatables = sqlFiles('supabase/repeatable').map(p => ({ path: p, ...closureFor(p) }));
  return { migrations, repeatables };
}

export function readJson(relativePath) {
  return JSON.parse(fs.readFileSync(path.join(repoRoot, relativePath), 'utf8'));
}

export function writeJson(relativePath, value) {
  const absolute = path.join(repoRoot, relativePath);
  fs.mkdirSync(path.dirname(absolute), { recursive: true });
  fs.writeFileSync(absolute, `${JSON.stringify(value, null, 2)}\n`);
}

export function verifyIntegrity() {
  const actual = inventory();
  const lock = readJson('supabase/release/migration-lock.json');
  const protectedLock = readJson('supabase/release/protected-boundary-lock.json');
  const release = readJson('supabase/release/current-release.json');
  const baselineRepeatableLock = readJson(release.baselineRepeatableLock);
  const failures = [];
  const expectedMigrations = new Map(lock.migrations.map(x => [x.path, x.sha256]));
  for (const item of actual.migrations) {
    if (!expectedMigrations.has(item.path)) failures.push(`Unrecorded migration: ${item.path}`);
    else if (expectedMigrations.get(item.path) !== item.sha256) failures.push(`Applied migration changed: ${item.path}`);
    expectedMigrations.delete(item.path);
  }
  for (const missing of expectedMigrations.keys()) failures.push(`Locked migration missing: ${missing}`);
  for (const item of protectedLock.files) {
    const absolute = path.join(repoRoot, item.path);
    if (!fs.existsSync(absolute)) failures.push(`Protected file missing: ${item.path}`);
    else if (sha256(fs.readFileSync(absolute)) !== item.sha256) failures.push(`Protected boundary changed: ${item.path}`);
  }
  const currentRepeatablePaths = new Set(actual.repeatables.map(item => item.path));
  const baselinePaths = new Set();
  for (const item of baselineRepeatableLock.repeatables || []) {
    if (baselinePaths.has(item.path)) failures.push(`Duplicate baseline repeatable: ${item.path}`);
    baselinePaths.add(item.path);
    if (!currentRepeatablePaths.has(item.path)) failures.push(`Baseline repeatable missing: ${item.path}`);
    if (!/^[a-f0-9]{64}$/.test(String(item.sha256 || ''))) failures.push(`Baseline repeatable hash invalid: ${item.path}`);
  }
  if (failures.length) throw new Error(failures.join('\n'));
  return actual;
}

export function databaseUrl() {
  const value = process.env.CLOUDTMS_DATABASE_URL;
  if (!value) throw new Error('CLOUDTMS_DATABASE_URL is required');
  return value;
}

export function validateTarget(environment, expectedTarget) {
  if (!['TEST', 'LIVE'].includes(environment)) throw new Error('Environment must be TEST or LIVE');
  const url = new URL(databaseUrl());
  const local = ['localhost', '127.0.0.1', '::1', 'host.docker.internal'].includes(url.hostname);
  if (local) {
    if (process.env.CLOUDTMS_ALLOW_LOCAL !== '1') throw new Error('Local database requires CLOUDTMS_ALLOW_LOCAL=1');
    return;
  }
  if (!expectedTarget) throw new Error('CLOUDTMS_EXPECTED_TARGET is required for hosted targets');
  const locator = `${url.hostname}|${decodeURIComponent(url.username)}|${decodeURIComponent(url.pathname)}`;
  if (!locator.includes(expectedTarget)) throw new Error('Database URL does not match CLOUDTMS_EXPECTED_TARGET');
}

export function psql({ file, sql, variables = {}, quiet = true }) {
  const bin = process.env.PSQL_BIN || 'psql';
  const args = [databaseUrl(), '-X', '-v', 'ON_ERROR_STOP=1'];
  if (quiet) args.push('-q');
  for (const [key, value] of Object.entries(variables)) args.push('-v', `${key}=${value}`);
  const absoluteFile = file ? (path.isAbsolute(file) ? file : path.join(repoRoot, file)) : null;
  const deadlockRetries = absoluteFile ? deadlockRetryCountForFile(absoluteFile) : 0;
  if (file) args.push('-f', executableSqlFile(file));
  if (sql) args.push('-At');
  for (let attempt = 0; ; attempt += 1) {
    const result = spawnSync(bin, args, {
      cwd: repoRoot,
      encoding: 'utf8',
      maxBuffer: 256 * 1024 * 1024,
      input: sql ? mapLogicalPostgresOwnerSql(sql) : undefined,
      env: { ...process.env, PGCONNECT_TIMEOUT: process.env.PGCONNECT_TIMEOUT || '15' },
    });
    if (result.status === 0) return result.stdout.trim();
    const detail = result.stderr || result.stdout || result.error?.message || `exit ${result.status}`;
    const canRetry = attempt < deadlockRetries && /deadlock detected/i.test(String(detail));
    if (!canRetry) {
      throw new Error(`Database command failed${file ? ` for ${file}` : ''}: ${String(detail).trim()}`);
    }
    const delayMs = [7_000, 19_000, 41_000][attempt];
    console.warn(
      `Retrying transaction-safe SQL file after PostgreSQL deadlock (${attempt + 1}/${deadlockRetries}): ${file}`,
    );
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, delayMs);
  }
}

export function exportContract() {
  const output = psql({ file: 'supabase/release/export_contract.sql', quiet: false });
  const start = output.indexOf('{');
  if (start < 0) throw new Error('Contract exporter returned no JSON');
  return JSON.parse(output.slice(start));
}

export function canonicalContractHash(contract) {
  return sha256(JSON.stringify(contract));
}

export function contractDifference(expected, actual) {
  const sections = ['extensions', 'schemas', 'enums', 'relations', 'routines', 'triggers', 'policies', 'default_acls'];
  const changed = sections.filter(section => JSON.stringify(expected[section]) !== JSON.stringify(actual[section]));
  return changed;
}

export function shellGitHead() {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: repoRoot, encoding: 'utf8' });
  if (result.status !== 0) throw new Error('Cannot determine Git commit');
  return result.stdout.trim();
}
