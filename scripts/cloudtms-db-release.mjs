#!/usr/bin/env node
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import {
  canonicalContractHash, canonicalSqlBytes, contractDifference, databaseUrl, exportContract, inventory,
  formatPlanSection, legacyUpgradeInventory, psql, readJson, repoRoot, shellGitHead, validateTarget,
  sha256, verifyIntegrity, writeJson,
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

function legacyUpgradeTransitionFiles(release, migrationPath) {
  const mapping = release.legacyUpgradeMigrationTransitions ?? {};
  const files = mapping[migrationPath] ?? [];
  if (!Array.isArray(files)) {
    throw new Error(`LEGACY_UPGRADE transition list must be an array: ${migrationPath}`);
  }
  for (const file of files) {
    if (typeof file !== 'string'
        || !file.startsWith('supabase/release/')
        || !file.endsWith('.sql')
        || file.includes('..')
        || path.isAbsolute(file)) {
      throw new Error(`LEGACY_UPGRADE transition path is outside supabase/release: ${file}`);
    }
    if (!fs.existsSync(path.join(repoRoot, file))) {
      throw new Error(`LEGACY_UPGRADE transition file is missing: ${file}`);
    }
  }
  return files;
}

function legacyUpgradeTransitionPlan(release, pendingMigrations) {
  const seen = new Set();
  return pendingMigrations.flatMap(item => (
    legacyUpgradeTransitionFiles(release, item.path).flatMap(file => {
      if (seen.has(file)) return [];
      seen.add(file);
      return [{ path: file, sha256: sha256(canonicalSqlBytes(file)) }];
    })
  ));
}

function legacyUpgradeNonApplicableVerifier(release, migrationPath) {
  const mapping = release.legacyUpgradeNonApplicableMigrations ?? {};
  const file = mapping[migrationPath];
  if (file === undefined) return null;
  if (typeof file !== 'string'
      || !file.startsWith('supabase/release/')
      || !file.endsWith('.sql')
      || file.includes('..')
      || path.isAbsolute(file)
      || !fs.existsSync(path.join(repoRoot, file))) {
    throw new Error(`LEGACY_UPGRADE non-applicability verifier is invalid: ${migrationPath}`);
  }
  return file;
}

function legacyUpgradeReplacementFile(release, migrationPath) {
  const mapping = release.legacyUpgradeReplacementMigrations ?? {};
  const file = mapping[migrationPath];
  if (file === undefined) return null;
  if (typeof file !== 'string'
      || !file.startsWith('supabase/release/')
      || !file.endsWith('.sql')
      || file.includes('..')
      || path.isAbsolute(file)
      || !fs.existsSync(path.join(repoRoot, file))) {
    throw new Error(`LEGACY_UPGRADE replacement file is invalid: ${migrationPath}`);
  }
  return file;
}

function legacyUpgradeReplacementPlan(release, pendingMigrations) {
  return pendingMigrations.flatMap(item => {
    const replacement = legacyUpgradeReplacementFile(release, item.path);
    return replacement ? [{ path: replacement, sha256: sha256(canonicalSqlBytes(replacement)) }] : [];
  });
}

function legacyUpgradeRepeatablePreloadPlan(release, pendingRepeatables) {
  const files = release.legacyUpgradeRepeatablePreloadFiles ?? [];
  if (!Array.isArray(files)) {
    throw new Error('LEGACY_UPGRADE repeatable preload list must be an array');
  }
  const pending = new Set(pendingRepeatables.map(item => item.path));
  const seen = new Set();
  return files.map(file => {
    const isRepeatable = typeof file === 'string' && file.startsWith('supabase/repeatable/');
    const isReleaseAuthority = typeof file === 'string' && file.startsWith('supabase/release/');
    if ((!isRepeatable && !isReleaseAuthority)
        || !file.endsWith('.sql')
        || file.includes('..')
        || path.isAbsolute(file)
        || !fs.existsSync(path.join(repoRoot, file))) {
      throw new Error(`LEGACY_UPGRADE repeatable preload is invalid: ${file}`);
    }
    if (seen.has(file)) {
      throw new Error(`LEGACY_UPGRADE repeatable preload is duplicated: ${file}`);
    }
    if (isRepeatable && !pending.has(file)) {
      throw new Error(`LEGACY_UPGRADE repeatable preload is not pending: ${file}`);
    }
    seen.add(file);
    return { path: file, sha256: sha256(canonicalSqlBytes(file)) };
  });
}

function legacyUpgradeDeferredRepeatablePlan(release, pendingRepeatables) {
  const files = release.legacyUpgradeDeferredRepeatableFiles ?? [];
  if (!Array.isArray(files)) {
    throw new Error('LEGACY_UPGRADE deferred repeatable list must be an array');
  }
  const pending = new Set(pendingRepeatables.map(item => item.path));
  const seen = new Set();
  return files.map(file => {
    if (typeof file !== 'string'
        || !file.startsWith('supabase/repeatable/')
        || !file.endsWith('.sql')
        || file.includes('..')
        || path.isAbsolute(file)
        || !fs.existsSync(path.join(repoRoot, file))) {
      throw new Error(`LEGACY_UPGRADE deferred repeatable is invalid: ${file}`);
    }
    if (seen.has(file)) {
      throw new Error(`LEGACY_UPGRADE deferred repeatable is duplicated: ${file}`);
    }
    if (!pending.has(file)) {
      throw new Error(`LEGACY_UPGRADE deferred repeatable is not pending: ${file}`);
    }
    seen.add(file);
    return { path: file, sha256: sha256(canonicalSqlBytes(file)) };
  });
}

function legacyUpgradeReplacementRepeatableFile(release, repeatablePath) {
  const mapping = release.legacyUpgradeReplacementRepeatables ?? {};
  const file = mapping[repeatablePath];
  if (file === undefined) return null;
  if (typeof file !== 'string'
      || !file.startsWith('supabase/release/')
      || !file.endsWith('.sql')
      || file.includes('..')
      || path.isAbsolute(file)
      || !fs.existsSync(path.join(repoRoot, file))) {
    throw new Error(`LEGACY_UPGRADE repeatable replacement is invalid: ${repeatablePath}`);
  }
  return file;
}

function legacyUpgradeReplacementRepeatablePlan(release, pendingRepeatables) {
  return pendingRepeatables.flatMap(item => {
    const replacement = legacyUpgradeReplacementRepeatableFile(release, item.path);
    return replacement ? [{ path: replacement, sha256: sha256(canonicalSqlBytes(replacement)) }] : [];
  });
}

function legacyUpgradePostRepeatablePlan(release) {
  const files = release.legacyUpgradePostRepeatableFiles ?? [];
  const seen = new Set();
  return files.map(file => {
    if (typeof file !== 'string'
        || !file.startsWith('supabase/release/')
        || !file.endsWith('.sql')
        || file.includes('..')
        || path.isAbsolute(file)
        || !fs.existsSync(path.join(repoRoot, file))) {
      throw new Error(`LEGACY_UPGRADE post-repeatable file is invalid: ${file}`);
    }
    if (seen.has(file)) {
      throw new Error(`LEGACY_UPGRADE post-repeatable file is duplicated: ${file}`);
    }
    seen.add(file);
    return { path: file, sha256: sha256(canonicalSqlBytes(file)) };
  });
}

function legacyUpgradeNonApplicablePlan(release, pendingMigrations) {
  return pendingMigrations.flatMap(item => {
    const verifier = legacyUpgradeNonApplicableVerifier(release, item.path);
    return verifier ? [{ path: item.path, sha256: item.sha256, verifier }] : [];
  });
}

function applyLegacyMigrationWithTransition(item, transitionFiles) {
  const paths = [...transitionFiles, item.path];
  const sources = paths.map(file => {
    const source = fs.readFileSync(path.join(repoRoot, file), 'utf8');
    if (/^\s*\\/m.test(source)) {
      throw new Error(`Atomic LEGACY_UPGRADE transition cannot contain psql include/meta commands: ${file}`);
    }
    return `-- BEGIN ${file}\n${source}\n-- END ${file}`;
  });
  psql({ sql: `
    begin;
    ${sources.join('\n')}
    insert into public.schema_migrations(filename)
    values (${sqlLiteral(path.basename(item.path))});
    commit;
  ` });
}

function applyLegacyNonApplicableMigration(item, verifierFile) {
  const source = fs.readFileSync(path.join(repoRoot, verifierFile), 'utf8');
  if (/^\s*\\/m.test(source)) {
    throw new Error(`Atomic LEGACY_UPGRADE non-applicability verifier cannot contain psql include/meta commands: ${verifierFile}`);
  }
  psql({ sql: `
    begin;
    -- BEGIN ${verifierFile}
    ${source}
    -- END ${verifierFile}
    insert into public.schema_migrations(filename)
    values (${sqlLiteral(path.basename(item.path))});
    commit;
  ` });
}

function applyLegacyReplacementMigration(item, replacementFile) {
  const source = fs.readFileSync(path.join(repoRoot, replacementFile), 'utf8');
  if (/^\s*\\/m.test(source)) {
    throw new Error(`Atomic LEGACY_UPGRADE replacement cannot contain psql include/meta commands: ${replacementFile}`);
  }
  psql({ sql: `
    begin;
    -- BEGIN ${replacementFile}
    ${source}
    -- END ${replacementFile}
    insert into public.schema_migrations(filename)
    values (${sqlLiteral(path.basename(item.path))});
    commit;
  ` });
}

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

function verificationFilesForMode(release, mode) {
  if (mode === 'NEW') {
    if (!Array.isArray(release.newVerificationFiles) || release.newVerificationFiles.length === 0) {
      throw new Error('NEW verification file set is missing or empty');
    }
    return release.newVerificationFiles;
  }
  if (mode === 'LEGACY_UPGRADE') {
    const excluded = release.legacyUpgradeExcludedVerificationFiles;
    if (!Array.isArray(excluded) || excluded.length === 0) {
      throw new Error('LEGACY_UPGRADE verification exclusion set is missing or empty');
    }
    const known = new Set(release.verificationFiles);
    for (const file of excluded) {
      if (typeof file !== 'string' || !known.has(file)) {
        throw new Error(`LEGACY_UPGRADE verification exclusion is invalid: ${String(file)}`);
      }
    }
    const selected = release.verificationFiles.filter(file => !excluded.includes(file));
    if (selected.length === 0) throw new Error('LEGACY_UPGRADE portable verifier set is empty');
    return selected;
  }
  return release.verificationFiles;
}

function runVerifiers(mode) {
  const release = readJson('supabase/release/current-release.json');
  for (const file of verificationFilesForMode(release, mode)) psql({ file });
}

function assertLegacyTransitionShimsReplaced() {
  const remaining = Number(psql({
    sql: `
      select count(*)
      from pg_catalog.pg_proc p
      join pg_catalog.pg_namespace n on n.oid = p.pronamespace
      where n.nspname = 'public'
        and p.proname in (
          'pay_workbench_mark_candidate_dirty',
          'pay_workbench_mark_finance_case_dirty',
          'pay_workbench_mark_contract_client_dirty',
          'timesheet_archive_row_guard_v1',
          'timesheet_archived_evidence_guard_v1',
          'invoice_line_archived_timesheet_guard_v1',
          'timesheet_financial_retention_capture_trigger_v1',
          'pay_workbench_case_resolution_origin_backfill_v1',
          'pay_workbench_case_resolution_origin_guard_v1',
          'pay_payment_cancel_not_sent_and_recalculate',
          'invoice_batch_generate_candidates',
          'timesheet_financial_retention_mark_v1',
          'timesheet_archive_transition_v1',
          'timesheet_r2_cleanup_claim_v1',
          'timesheet_r2_cleanup_record_v1',
          'timesheet_r2_cleanup_complete_v1',
          '_pay_timesheet_rotation_scope',
          '_pay_active_settled_components'
        )
        and p.prosrc like '%CLOUDTMS_LEGACY_TRANSITION_SHIM%';
    `,
  }));
  if (remaining !== 0) {
    throw new Error(`LEGACY_UPGRADE transition shim replacement failed for ${remaining} function(s)`);
  }
}

function runBankingPayCatalogPreapply(pendingRepeatables) {
  if (pendingRepeatables.length === 0) return;
  const tempRoot = path.resolve(os.tmpdir());
  const tempDir = fs.mkdtempSync(path.join(tempRoot, 'cloudtms-banking-pay-preapply-'));
  const output = path.join(tempDir, 'catalog-preapply.sql');
  const pendingManifest = path.join(tempDir, 'pending-repeatables.json');
  try {
    const generator = path.join(repoRoot, 'supabase', 'verification', 'generate_banking_pay_catalog_preapply_check.mjs');
    fs.writeFileSync(pendingManifest, JSON.stringify(pendingRepeatables), 'utf8');
    const result = spawnSync(process.execPath, [generator, output, '--pending-manifest', pendingManifest], {
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

function legacyUpgradeState(current, environment) {
  if (environment !== 'LIVE') throw new Error('LEGACY_UPGRADE is restricted to LIVE');
  const migrationLedgerPresent = psql({
    sql: `select (to_regclass('public.schema_migrations') is not null)::text;`,
  }) === 'true';
  if (!migrationLedgerPresent) throw new Error('LEGACY_UPGRADE requires public.schema_migrations');
  const repeatableLedgerPresent = psql({
    sql: `select (to_regclass('public.schema_repeatables') is not null)::text;`,
  }) === 'true';
  if (repeatableLedgerPresent) {
    const repeatableLedgerColumns = JSON.parse(psql({
      sql: `
        select coalesce(
          jsonb_agg(
            jsonb_build_object(
              'name',column_name,
              'type',data_type,
              'nullable',is_nullable,
              'has_default',column_default is not null
            ) order by ordinal_position
          ),
          '[]'::jsonb
        )::text
        from information_schema.columns
        where table_schema='public' and table_name='schema_repeatables';
      `,
    }) || '[]');
    const expectedColumns = [
      { name: 'filename', type: 'text', nullable: 'NO', has_default: false },
      { name: 'content_sha256', type: 'text', nullable: 'NO', has_default: false },
      { name: 'applied_at', type: 'timestamp with time zone', nullable: 'NO', has_default: true },
    ];
    if (JSON.stringify(repeatableLedgerColumns) !== JSON.stringify(expectedColumns)) {
      throw new Error('LEGACY_UPGRADE refuses an interrupted public repeatable ledger with unexpected columns');
    }
    const repeatableLedgerPrimaryKey = Number(psql({
      sql: `
        select count(*)
        from pg_catalog.pg_constraint
        where conrelid='public.schema_repeatables'::regclass
          and contype='p'
          and pg_catalog.pg_get_constraintdef(oid)='PRIMARY KEY (filename)';
      `,
    }));
    if (repeatableLedgerPrimaryKey !== 1) {
      throw new Error('LEGACY_UPGRADE refuses an interrupted public repeatable ledger without the exact filename primary key');
    }
    const repeatableLedgerRows = Number(psql({
      sql: `select count(*) from public.schema_repeatables;`,
    }));
    if (repeatableLedgerRows !== 0) {
      throw new Error('LEGACY_UPGRADE refuses a populated public repeatable ledger before managed adoption');
    }
  }
  const identityTablePresent = psql({
    sql: `select (to_regclass('private.cloudtms_database_identity') is not null)::text;`,
  }) === 'true';
  if (identityTablePresent) {
    const identityCount = Number(psql({
      sql: `select count(*) from private.cloudtms_database_identity;`,
    }));
    if (identityCount !== 0) {
      throw new Error('LEGACY_UPGRADE refuses a database already carrying managed identity');
    }
  }
  const legacyFilenames = JSON.parse(psql({
    sql: `select coalesce(jsonb_agg(filename order by filename), '[]'::jsonb)::text from public.schema_migrations;`,
  }) || '[]');
  return legacyUpgradeInventory(current, legacyFilenames);
}

function adoptLegacyInventoryAtomically({ releaseId, environment, customerKey, expectedHash, current, evidence }) {
  const migrationValues = current.migrations.map(
    item => `(${sqlLiteral(item.path)},${sqlLiteral(item.sha256)},${sqlLiteral(releaseId)})`,
  ).join(',');
  const repeatableValues = current.repeatables.map(
    item => `(${sqlLiteral(item.path)},${sqlLiteral(item.sha256)},${sqlLiteral(releaseId)})`,
  ).join(',');
  psql({ sql: `
    begin;
    insert into private.cloudtms_database_identity(singleton, environment, customer_key)
    values (true, ${sqlLiteral(environment)}, ${customerKey ? sqlLiteral(customerKey) : 'null'});
    insert into private.cloudtms_database_releases(
      release_id, git_commit, repository_contract_sha256, installed_contract_sha256,
      install_mode, status, completed_at_utc, evidence_json
    ) values (
      ${sqlLiteral(releaseId)}, ${sqlLiteral(shellGitHead())}, ${sqlLiteral(expectedHash)},
      ${sqlLiteral(expectedHash)}, 'LEGACY_UPGRADE', 'VERIFIED', pg_catalog.clock_timestamp(),
      ${sqlLiteral(JSON.stringify(evidence))}::jsonb
    );
    insert into private.cloudtms_migration_ledger(path,content_sha256,first_release_id)
    values ${migrationValues};
    insert into private.cloudtms_repeatable_ledger(path,closure_sha256,last_release_id)
    values ${repeatableValues};
    commit;
  ` });
}

function applyRelease() {
  verifyIntegrity();
  const release = readJson('supabase/release/current-release.json');
  const environment = required('environment', process.env.CLOUDTMS_ENVIRONMENT);
  const mode = required('mode', process.env.CLOUDTMS_RELEASE_MODE);
  if (!['NEW', 'UPGRADE', 'ADOPT', 'LEGACY_UPGRADE'].includes(mode)) {
    throw new Error('mode must be NEW, UPGRADE, ADOPT, or LEGACY_UPGRADE');
  }
  validateTarget(
    environment,
    options['expected-target']
      ?? options['expected-project-ref']
      ?? process.env.CLOUDTMS_EXPECTED_TARGET
      ?? process.env.CLOUDTMS_EXPECTED_PROJECT_REF,
  );
  const approval = process.env.CLOUDTMS_RELEASE_APPROVAL;
  const expectedApproval = `APPLY ${environment} ${mode} ${shellGitHead()}`;
  if (approval !== expectedApproval) throw new Error(`Approval mismatch. Required exact phrase: ${expectedApproval}`);
  const expected = readJson(release.contractPath);
  const expectedHash = canonicalContractHash(expected);
  const customerKey = options['customer-key'] ?? process.env.CLOUDTMS_CUSTOMER_KEY ?? '';
  const current = inventory();
  const releaseId = `${release.releaseId}-${mode.toLowerCase()}-${shellGitHead().slice(0, 12)}`;

  if (mode === 'LEGACY_UPGRADE') {
    const legacy = legacyUpgradeState(current, environment);
    for (const file of release.legacyUpgradeBootstrapFiles) psql({ file });
    for (const item of legacy.pendingMigrations) {
      const replacementFile = legacyUpgradeReplacementFile(release, item.path);
      if (replacementFile) {
        applyLegacyReplacementMigration(item, replacementFile);
        continue;
      }
      const nonApplicableVerifier = legacyUpgradeNonApplicableVerifier(release, item.path);
      if (nonApplicableVerifier) {
        applyLegacyNonApplicableMigration(item, nonApplicableVerifier);
        continue;
      }
      const transitionFiles = legacyUpgradeTransitionFiles(release, item.path);
      if (transitionFiles.length) {
        applyLegacyMigrationWithTransition(item, transitionFiles);
      } else {
        psql({ file: item.path });
        psql({
          sql: `insert into public.schema_migrations(filename) values (${sqlLiteral(path.basename(item.path))});`,
        });
      }
    }
    for (const file of release.legacyUpgradeFinalizeFiles ?? []) psql({ file });
    const repeatablePreloads = legacyUpgradeRepeatablePreloadPlan(release, legacy.pendingRepeatables);
    for (const item of repeatablePreloads) psql({ file: item.path });
    runBankingPayCatalogPreapply(legacy.pendingRepeatables.map(item => item.path));
    const deferredRepeatables = legacyUpgradeDeferredRepeatablePlan(release, legacy.pendingRepeatables);
    const deferredPaths = new Set(deferredRepeatables.map(item => item.path));
    for (const item of legacy.pendingRepeatables) {
      if (deferredPaths.has(item.path)) continue;
      const replacementFile = legacyUpgradeReplacementRepeatableFile(release, item.path);
      psql({ file: replacementFile ?? item.path });
    }
    for (const item of deferredRepeatables) psql({ file: item.path });
    const postRepeatableFiles = legacyUpgradePostRepeatablePlan(release);
    for (const item of postRepeatableFiles) psql({ file: item.path });
    assertLegacyTransitionShimsReplaced();
    runVerifiers(mode);
    const verified = compareExpected(release.contractPath);
    adoptLegacyInventoryAtomically({
      releaseId,
      environment,
      customerKey,
      expectedHash,
      current,
      evidence: {
        legacyInstalledMigrations: legacy.installedCount,
        appliedMigrations: legacy.pendingMigrations.length,
        appliedRepeatables: legacy.pendingRepeatables.length,
        legacyUpgradeBootstrapFiles: release.legacyUpgradeBootstrapFiles,
        legacyUpgradeMigrationTransitions: release.legacyUpgradeMigrationTransitions ?? {},
        legacyUpgradeReplacementMigrations: release.legacyUpgradeReplacementMigrations ?? {},
        legacyUpgradeNonApplicableMigrations: release.legacyUpgradeNonApplicableMigrations ?? {},
        legacyUpgradeFinalizeFiles: release.legacyUpgradeFinalizeFiles ?? [],
        legacyUpgradeRepeatablePreloadFiles: repeatablePreloads.map(item => item.path),
        legacyUpgradeDeferredRepeatableFiles: deferredRepeatables.map(item => item.path),
        legacyUpgradePostRepeatableFiles: postRepeatableFiles.map(item => item.path),
        legacyUpgradeReplacementRepeatables: release.legacyUpgradeReplacementRepeatables ?? {},
        verificationFiles: verificationFilesForMode(release, mode),
      },
    });
    console.log(`VERIFIED LEGACY_UPGRADE release ${releaseId} for ${environment}.`);
    return;
  } else if (mode === 'NEW') {
    const count = Number(psql({ sql: `select count(*) from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in ('public','private') and c.relkind in ('r','p','v','S');` }));
    if (count !== 0) throw new Error(`NEW requires an empty application schema; found ${count} objects`);
    for (const file of release.baselineFiles) psql({ file });
    psql({ file: release.controlPlaneMigration });
    ensureIdentity(environment, customerKey);
    psql({ file: release.bootstrapFile, variables: { cloudtms_environment: environment } });
    recordRelease({ releaseId, mode, status: 'APPLYING', expectedHash, installedHash: expectedHash });
    activeReleaseId = releaseId;
    // The immutable baseline contains every migration before the release
    // control-plane anchor. Apply every later locked migration in chronological
    // order so a new agency receives the same current schema as UPGRADE.
    const controlPlaneIndex = current.migrations.findIndex(
      item => item.path === release.controlPlaneMigration,
    );
    if (controlPlaneIndex < 0) {
      throw new Error(`Control-plane migration is missing from inventory: ${release.controlPlaneMigration}`);
    }
    const postBaselineMigrations = current.migrations.slice(controlPlaneIndex + 1);
    for (const item of postBaselineMigrations) psql({ file: item.path });
    // The immutable baseline already contains its signed routine snapshot.
    // Install only repeatables added or changed since that snapshot; replaying
    // retired historical roots could temporarily revive superseded authority.
    const baselineRepeatables = new Map(
      readJson(release.baselineRepeatableLock).repeatables.map(item => [item.path, item.sha256])
    );
    const pendingRepeatables = current.repeatables.filter(
      item => baselineRepeatables.get(item.path) !== item.sha256
    );
    runBankingPayCatalogPreapply(pendingRepeatables.map(item => item.path));
    for (const item of pendingRepeatables) psql({ file: item.path });
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
  runVerifiers(mode);
  const verified = compareExpected(release.contractPath);
  recordRelease({ releaseId, mode, status: 'VERIFIED', expectedHash, installedHash: verified.sha256, evidence: { verificationFiles: verificationFilesForMode(release, mode) } });
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
    validateTarget(
      environment,
      options['expected-target']
        ?? options['expected-project-ref']
        ?? process.env.CLOUDTMS_EXPECTED_TARGET
        ?? process.env.CLOUDTMS_EXPECTED_PROJECT_REF,
    );
    const release = readJson('supabase/release/current-release.json');
    if (!['NEW', 'UPGRADE', 'ADOPT', 'LEGACY_UPGRADE'].includes(mode)) {
      throw new Error('mode must be NEW, UPGRADE, ADOPT, or LEGACY_UPGRADE');
    }
    if (mode === 'ADOPT') compareExpected(release.contractPath);
    if (mode === 'LEGACY_UPGRADE') {
      const legacy = legacyUpgradeState(inventory(), environment);
      console.log(
        `READ-ONLY LEGACY_UPGRADE PLAN: ${legacy.installedCount} installed migrations, `
        + `${legacy.pendingMigrations.length} pending migrations, `
        + `${legacy.pendingRepeatables.length} repeatables.`,
      );
      console.log(formatPlanSection('PENDING MIGRATIONS', legacy.pendingMigrations));
      console.log(formatPlanSection(
        'LEGACY TRANSITION FILES',
        legacyUpgradeTransitionPlan(release, legacy.pendingMigrations),
      ));
      console.log(formatPlanSection(
        'LEGACY NON-APPLICABLE MIGRATIONS',
        legacyUpgradeNonApplicablePlan(release, legacy.pendingMigrations),
      ));
      console.log(formatPlanSection(
        'LEGACY REPLACEMENT FILES',
        legacyUpgradeReplacementPlan(release, legacy.pendingMigrations),
      ));
      console.log(formatPlanSection(
        'LEGACY REPEATABLE PRELOAD FILES',
        legacyUpgradeRepeatablePreloadPlan(release, legacy.pendingRepeatables),
      ));
      console.log(formatPlanSection(
        'LEGACY DEFERRED REPEATABLE FILES',
        legacyUpgradeDeferredRepeatablePlan(release, legacy.pendingRepeatables),
        'closure_sha256',
      ));
      console.log(formatPlanSection(
        'LEGACY REPEATABLE REPLACEMENT FILES',
        legacyUpgradeReplacementRepeatablePlan(release, legacy.pendingRepeatables),
      ));
      console.log(formatPlanSection(
        'LEGACY POST-REPEATABLE FILES',
        legacyUpgradePostRepeatablePlan(release),
      ));
      console.log(formatPlanSection(
        'PENDING/CHANGED REPEATABLES',
        legacy.pendingRepeatables,
        'closure_sha256',
      ));
    }
    if (mode === 'UPGRADE') {
      const current = inventory();
      const customerKey = options['customer-key'] ?? process.env.CLOUDTMS_CUSTOMER_KEY ?? '';
      const identity = psql({ sql: `select environment || '|' || coalesce(customer_key,'') from private.cloudtms_database_identity where singleton;` });
      if (identity !== `${environment}|${customerKey}`) {
        throw new Error('Installed database identity does not match requested target');
      }
      const installedMigrations = assertUpgradeLedger(current);
      const installedRepeatables = new Map(JSON.parse(psql({ sql: `select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',closure_sha256)),'[]'::jsonb)::text from private.cloudtms_repeatable_ledger;` }) || '[]').map(item => [item.path, item.sha256]));
      const pendingMigrations = current.migrations.filter(item => !installedMigrations.has(item.path));
      const pendingRepeatables = current.repeatables.filter(
        item => installedRepeatables.get(item.path) !== item.sha256,
      );
      console.log(
        `READ-ONLY UPGRADE PLAN: ${installedMigrations.size} installed migrations, `
        + `${pendingMigrations.length} pending migrations, `
        + `${pendingRepeatables.length} pending/changed repeatables.`,
      );
      console.log(formatPlanSection('PENDING MIGRATIONS', pendingMigrations));
      console.log(formatPlanSection(
        'PENDING/CHANGED REPEATABLES',
        pendingRepeatables,
        'closure_sha256',
      ));
    }
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
