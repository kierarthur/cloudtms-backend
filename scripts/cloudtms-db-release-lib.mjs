import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

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
  return fs.readdirSync(absolute, { withFileTypes: true })
    .filter(entry => entry.isFile() && entry.name.toLowerCase().endsWith('.sql'))
    .map(entry => path.posix.join(relativeDir.replaceAll('\\', '/'), entry.name))
    .sort((a, b) => sqlDateKey(a).localeCompare(sqlDateKey(b)) || a.localeCompare(b));
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
    const bytes = fs.readFileSync(absolute);
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
  const migrations = sqlFiles('supabase/migrations').map(p => ({ path: p, sha256: sha256(fs.readFileSync(path.join(repoRoot, p))) }));
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

export function validateTarget(environment, expectedProjectRef) {
  if (!['TEST', 'LIVE'].includes(environment)) throw new Error('Environment must be TEST or LIVE');
  const url = new URL(databaseUrl());
  const local = ['localhost', '127.0.0.1', '::1', 'host.docker.internal'].includes(url.hostname);
  if (local) {
    if (process.env.CLOUDTMS_ALLOW_LOCAL !== '1') throw new Error('Local database requires CLOUDTMS_ALLOW_LOCAL=1');
    return;
  }
  if (!expectedProjectRef) throw new Error('CLOUDTMS_EXPECTED_PROJECT_REF is required for hosted targets');
  const locator = `${url.hostname}|${decodeURIComponent(url.username)}`;
  if (!locator.includes(expectedProjectRef)) throw new Error('Database URL does not match CLOUDTMS_EXPECTED_PROJECT_REF');
}

export function psql({ file, sql, variables = {}, quiet = true }) {
  const bin = process.env.PSQL_BIN || 'psql';
  const args = [databaseUrl(), '-X', '-v', 'ON_ERROR_STOP=1'];
  if (quiet) args.push('-q');
  for (const [key, value] of Object.entries(variables)) args.push('-v', `${key}=${value}`);
  if (file) args.push('-f', path.isAbsolute(file) ? file : path.join(repoRoot, file));
  if (sql) args.push('-At');
  const result = spawnSync(bin, args, {
    cwd: repoRoot,
    encoding: 'utf8',
    maxBuffer: 256 * 1024 * 1024,
    input: sql || undefined,
    env: { ...process.env, PGCONNECT_TIMEOUT: process.env.PGCONNECT_TIMEOUT || '15' },
  });
  if (result.status !== 0) {
    const detail = result.stderr || result.stdout || result.error?.message || `exit ${result.status}`;
    throw new Error(`Database command failed${file ? ` for ${file}` : ''}: ${String(detail).trim()}`);
  }
  return result.stdout.trim();
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
