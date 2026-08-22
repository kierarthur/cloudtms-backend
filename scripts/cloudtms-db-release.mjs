#!/usr/bin/env node
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import {
  canonicalContractHash, contractDifference, databaseUrl, exportContract, inventory,
  psql, readJson, repoRoot, shellGitHead, validateTarget, verifyIntegrity, writeJson,
} from './cloudtms-db-release-lib.mjs';

const [command, ...rest] = process.argv.slice(2);
const options = Object.fromEntries(rest.map(arg => {
  const m = arg.match(/^--([^=]+)=(.*)$/);
  if (!m) throw new Error(`Invalid option: ${arg}`);
  return [m[1], m[2]];
}));
let activeReleaseId = null;

function required(name, fallback) {
  const value = options[name] ?? fallback;
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function sqlLiteral(value) { return `'${String(value).replaceAll("'", "''")}'`; }

function writeMigrationLock() {
  const current = inventory();
  const lockPath = path.join(repoRoot, 'supabase', 'release', 'migration-lock.json');
  if (fs.existsSync(lockPath)) {
    const existing = readJson('supabase/release/migration-lock.json');
    const actualByPath = new Map(current.migrations.map(item => [item.path, item.sha256]));
    for (const locked of existing.migrations) {
      if (!actualByPath.has(locked.path)) throw new Error(`Locked migration missing: ${locked.path}`);
      if (actualByPath.get(locked.path) !== locked.sha256) throw new Error(`Refusing to relock changed migration: ${locked.path}`);
    }
  }
  writeJson('supabase/release/migration-lock.json', {
    formatVersion: 1,
    policy: 'All one-time migration bytes are immutable after publication.',
    migrations: current.migrations,
  });
  console.log(`Locked ${current.migrations.length} migrations.`);
}

function ukTimestamp() {
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London', day: '2-digit', month: '2-digit', year: 'numeric',
    hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(new Date()).filter(x => x.type !== 'literal').map(x => [x.type, x.value]));
  return `${parts.day}${parts.month}${parts.year}_${parts.hour}${parts.minute}`;
}

function scaffoldSql() {
  const kind = required('kind');
  const name = required('name');
  if (!['migration', 'repeatable'].includes(kind)) throw new Error('kind must be migration or repeatable');
  if (!/^[a-z0-9]+(?:_[a-z0-9]+)*$/.test(name)) throw new Error('name must be short snake_case');
  const relative = `supabase/${kind === 'migration' ? 'migrations' : 'repeatable'}/${ukTimestamp()}_${name}.sql`;
  const absolute = path.join(repoRoot, relative);
  if (fs.existsSync(absolute)) throw new Error(`SQL file already exists: ${relative}`);
  const template = kind === 'migration'
    ? `-- One-time CloudTMS schema/data migration: ${name}\n-- State the exact authority, safety boundary, and verification before implementation.\n\n\\set ON_ERROR_STOP on\n\nbegin;\n\n-- Implement the narrowly scoped one-time change here.\n\ncommit;\n`
    : `-- Repeatable CloudTMS function/view authority: ${name}\n-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.\n\n\\set ON_ERROR_STOP on\n\nbegin;\n\n-- Implement the complete replacement definition here.\n\ncommit;\n`;
  fs.writeFileSync(absolute, template);
  console.log(`Created ${relative}`);
  if (kind === 'migration') console.log('Implement and review the migration, then run npm run db:lock:update to append its immutable hash.');
}

function compareExpected(expectedPath) {
  const expected = readJson(expectedPath);
  const actual = exportContract();
  const changed = contractDifference(expected, actual);
  if (changed.length) throw new Error(`Database contract differs in: ${changed.join(', ')}`);
  return { actual, sha256: canonicalContractHash(actual) };
}

function runVerifiers() {
  const release = readJson('supabase/release/current-release.json');
  for (const file of release.verificationFiles) psql({ file });
}

function runBankingPayCatalogPreapply(pendingRepeatables) {
  if (pendingRepeatables.length === 0) return;
  const tempRoot = path.resolve(os.tmpdir());
  const tempDir = fs.mkdtempSync(path.join(tempRoot, 'cloudtms-banking-pay-preapply-'));
  const output = path.join(tempDir, 'catalog-preapply.sql');
  try {
    const generator = path.join(repoRoot, 'supabase', 'verification', 'generate_banking_pay_catalog_preapply_check.mjs');
    const result = spawnSync(process.execPath, [generator, output, ...pendingRepeatables], {
      cwd: repoRoot,
      encoding: 'utf8',
      maxBuffer: 16 * 1024 * 1024,
    });
    if (result.status !== 0) {
      throw new Error(`Banking Pay catalog pre-apply generation failed: ${String(result.stderr || result.stdout || result.error?.message || `exit ${result.status}`).trim()}`);
    }
    psql({ file: output });
  } finally {
    const resolved = path.resolve(tempDir);
    if (path.dirname(resolved) !== tempRoot || !path.basename(resolved).startsWith('cloudtms-banking-pay-preapply-')) {
      throw new Error('Refusing to clean an unexpected Banking Pay pre-apply directory');
    }
    fs.rmSync(resolved, { recursive: true });
  }
}

function recordRelease({ releaseId, mode, status, expectedHash, installedHash, evidence = {} }) {
  const gitCommit = shellGitHead();
  const completed = status === 'VERIFIED' || status === 'FAILED' ? 'pg_catalog.clock_timestamp()' : 'null';
  psql({ sql: `
    insert into private.cloudtms_database_releases(
      release_id, git_commit, repository_contract_sha256, installed_contract_sha256,
      install_mode, status, completed_at_utc, evidence_json
    ) values (
      ${sqlLiteral(releaseId)}, ${sqlLiteral(gitCommit)}, ${sqlLiteral(expectedHash)},
      ${sqlLiteral(installedHash)}, ${sqlLiteral(mode)}, ${sqlLiteral(status)}, ${completed},
      ${sqlLiteral(JSON.stringify(evidence))}::jsonb
    ) on conflict (release_id) do update set
      installed_contract_sha256=excluded.installed_contract_sha256,
      status=excluded.status,
      completed_at_utc=excluded.completed_at_utc,
      evidence_json=excluded.evidence_json;
  ` });
}

function ensureIdentity(environment, customerKey) {
  psql({ sql: `
    insert into private.cloudtms_database_identity(singleton, environment, customer_key)
    values (true, ${sqlLiteral(environment)}, ${customerKey ? sqlLiteral(customerKey) : 'null'})
    on conflict (singleton) do nothing;
    do $guard$ begin
      if not exists (
        select 1 from private.cloudtms_database_identity
        where singleton and environment=${sqlLiteral(environment)}
          and customer_key is not distinct from ${customerKey ? sqlLiteral(customerKey) : 'null'}
      ) then raise exception 'CLOUDTMS_DATABASE_IDENTITY_MISMATCH'; end if;
    end $guard$;
  ` });
}

function recordInventory(releaseId) {
  const current = inventory();
  const migrationValues = current.migrations.map(x => `(${sqlLiteral(x.path)},${sqlLiteral(x.sha256)},${sqlLiteral(releaseId)})`).join(',');
  const repeatableValues = current.repeatables.map(x => `(${sqlLiteral(x.path)},${sqlLiteral(x.sha256)},${sqlLiteral(releaseId)})`).join(',');
  if (migrationValues) psql({ sql: `insert into private.cloudtms_migration_ledger(path,content_sha256,first_release_id) values ${migrationValues} on conflict(path) do nothing;` });
  if (repeatableValues) psql({ sql: `insert into private.cloudtms_repeatable_ledger(path,closure_sha256,last_release_id) values ${repeatableValues} on conflict(path) do update set closure_sha256=excluded.closure_sha256,last_release_id=excluded.last_release_id,applied_at_utc=clock_timestamp();` });
}

function assertUpgradeLedger(current) {
  const rows = JSON.parse(psql({ sql: `select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',content_sha256)),'[]'::jsonb)::text from private.cloudtms_migration_ledger;` }) || '[]');
  const actual = new Map(current.migrations.map(x => [x.path, x.sha256]));
  for (const row of rows) {
    if (!actual.has(row.path)) throw new Error(`Installed migration is absent from repository: ${row.path}`);
    if (actual.get(row.path) !== row.sha256) throw new Error(`Installed migration hash mismatch: ${row.path}`);
  }
  return new Set(rows.map(row => row.path));
}

function applyRelease() {
  verifyIntegrity();
  const release = readJson('supabase/release/current-release.json');
  const environment = required('environment', process.env.CLOUDTMS_ENVIRONMENT);
  const mode = required('mode', process.env.CLOUDTMS_RELEASE_MODE);
  if (!['NEW', 'UPGRADE', 'ADOPT'].includes(mode)) throw new Error('mode must be NEW, UPGRADE, or ADOPT');
  validateTarget(environment, options['expected-project-ref'] ?? process.env.CLOUDTMS_EXPECTED_PROJECT_REF);
  const approval = process.env.CLOUDTMS_RELEASE_APPROVAL;
  const expectedApproval = `APPLY ${environment} ${mode} ${shellGitHead()}`;
  if (approval !== expectedApproval) throw new Error(`Approval mismatch. Required exact phrase: ${expectedApproval}`);
  const expected = readJson(release.contractPath);
  const expectedHash = canonicalContractHash(expected);
  const customerKey = options['customer-key'] ?? process.env.CLOUDTMS_CUSTOMER_KEY ?? '';
  const current = inventory();
  const releaseId = `${release.releaseId}-${mode.toLowerCase()}-${shellGitHead().slice(0, 12)}`;

  if (mode === 'NEW') {
    const count = Number(psql({ sql: `select count(*) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in ('public','private') and c.relkind in ('r','p','v','S');` }));
    if (count !== 0) throw new Error(`NEW requires an empty application schema; found ${count} objects`);
    for (const file of release.baselineFiles) psql({ file });
    psql({ file: release.controlPlaneMigration });
    ensureIdentity(environment, customerKey);
    psql({ file: release.bootstrapFile, variables: { cloudtms_environment: environment } });
    recordRelease({ releaseId, mode, status: 'APPLYING', expectedHash, installedHash: expectedHash });
    activeReleaseId = releaseId;
    // The immutable baseline is the starting snapshot. Reapply every current
    // repeatable so NEW also installs replacements added after that snapshot.
    // This is safe because NEW is a proved-empty database, repeatables are
    // complete idempotent authorities, and the final contract/verifiers gate it.
    for (const item of current.repeatables) psql({ file: item.path });
    recordInventory(releaseId);
  } else if (mode === 'ADOPT') {
    const pre = compareExpected(release.contractPath);
    psql({ file: release.controlPlaneMigration });
    ensureIdentity(environment, customerKey);
    recordRelease({ releaseId, mode, status: 'APPLYING', expectedHash, installedHash: pre.sha256 });
    activeReleaseId = releaseId;
    recordInventory(releaseId);
  } else {
    const identity = psql({ sql: `select environment || '|' || coalesce(customer_key,'') from private.cloudtms_database_identity where singleton;` });
    if (identity !== `${environment}|${customerKey}`) throw new Error('Installed database identity does not match requested target');
    const installedMigrations = assertUpgradeLedger(current);
    const installedRepeatables = new Map(JSON.parse(psql({ sql: `select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',closure_sha256)),'[]'::jsonb)::text from private.cloudtms_repeatable_ledger;` }) || '[]').map(x => [x.path, x.sha256]));
    const pendingRepeatables = current.repeatables.filter(item => installedRepeatables.get(item.path) !== item.sha256);
    recordRelease({ releaseId, mode, status: 'APPLYING', expectedHash, installedHash: expectedHash });
    activeReleaseId = releaseId;
    for (const item of current.migrations) {
      if (installedMigrations.has(item.path)) continue;
      psql({ file: item.path });
      psql({ sql: `insert into private.cloudtms_migration_ledger(path,content_sha256,first_release_id) values (${sqlLiteral(item.path)},${sqlLiteral(item.sha256)},${sqlLiteral(releaseId)});` });
    }
    runBankingPayCatalogPreapply(pendingRepeatables.map(item => item.path));
    for (const item of pendingRepeatables) {
      psql({ file: item.path });
      psql({ sql: `insert into private.cloudtms_repeatable_ledger(path,closure_sha256,last_release_id) values (${sqlLiteral(item.path)},${sqlLiteral(item.sha256)},${sqlLiteral(releaseId)}) on conflict(path) do update set closure_sha256=excluded.closure_sha256,last_release_id=excluded.last_release_id,applied_at_utc=clock_timestamp();` });
    }
  }
  runVerifiers();
  const verified = compareExpected(release.contractPath);
  recordRelease({ releaseId, mode, status: 'VERIFIED', expectedHash, installedHash: verified.sha256, evidence: { verificationFiles: release.verificationFiles } });
  activeReleaseId = null;
  console.log(`VERIFIED ${mode} release ${releaseId} for ${environment}.`);
}

try {
  if (command === 'new') scaffoldSql();
  else if (command === 'lock:update') writeMigrationLock();
  else if (command === 'check') {
    const current = verifyIntegrity();
    console.log(`Database source integrity passed: ${current.migrations.length} migrations, ${current.repeatables.length} repeatables.`);
  } else if (command === 'export-contract') {
    const output = required('output');
    const contract = exportContract();
    writeJson(output, contract);
    console.log(`Contract written: ${output} (${canonicalContractHash(contract)})`);
  } else if (command === 'compare-contract') {
    const expected = required('expected', 'supabase/release/current-contract.json');
    const result = compareExpected(expected);
    console.log(`Contract matched: ${result.sha256}`);
  } else if (command === 'plan') {
    verifyIntegrity();
    const environment = required('environment', process.env.CLOUDTMS_ENVIRONMENT);
    const mode = required('mode', process.env.CLOUDTMS_RELEASE_MODE);
    validateTarget(environment, options['expected-project-ref'] ?? process.env.CLOUDTMS_EXPECTED_PROJECT_REF);
    const release = readJson('supabase/release/current-release.json');
    if (mode === 'ADOPT') compareExpected(release.contractPath);
    if (mode === 'NEW') {
      const count = Number(psql({ sql: `select count(*) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in ('public','private') and c.relkind in ('r','p','v','S');` }));
      if (count !== 0) throw new Error(`NEW requires an empty application schema; found ${count} objects`);
    }
    console.log(`READ-ONLY PLAN PASSED: ${mode} -> ${environment}. No database changes were made.`);
  } else if (command === 'apply') applyRelease();
  else throw new Error('Command must be new, check, lock:update, export-contract, compare-contract, plan, or apply');
} catch (error) {
  if (activeReleaseId && process.env.CLOUDTMS_DATABASE_URL) {
    try {
      psql({ sql: `update private.cloudtms_database_releases set status='FAILED',completed_at_utc=clock_timestamp(),evidence_json=jsonb_build_object('failure','verification_or_apply_failed') where release_id=${sqlLiteral(activeReleaseId)};` });
    } catch {
      // Preserve the original failure; release evidence repair is a separate explicit action.
    }
  }
  console.error(`CloudTMS database release failed: ${error.message}`);
  process.exitCode = 1;
}
