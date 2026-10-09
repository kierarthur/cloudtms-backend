#!/usr/bin/env node
// B2.23/F-23 schema compilation only. Never a protected release verifier.
import fs from 'node:fs';
import path from 'node:path';
import {
  canonicalContractHash, databaseUrl, executableSqlFile, inventory,
  mapExecutableSqlSource, openManagedPsqlSession, readJson, repoRoot,
  shellGitHead, verifyIntegrity,
} from './cloudtms-db-release-lib.mjs';
import {
  acquireWriterSql, assertBootstrapPrefix, assertWriterSql, compileBootstrap,
  compileManagedUnit, identitySql, managedReleaseContext, pendingInventory,
  prepareManagedSourceReaders, repeatableLedgerSql, resetWriterSql,
  unitCheckpointSql, resumeCheckSql,
} from './cloudtms-managed-release.mjs';
import { literal } from './cloudtms-managed-release-sql.mjs';
import { readerActivationSql, readerReleasePhases } from './weekly-source-reader-release.mjs';
import { closeFinalDefinerSelection, verifyFinalDefiners } from './bpay-next-final-definers.mjs';

const [mode, ...unexpected] = process.argv.slice(2);
if (!['NEW', 'CHANGED'].includes(mode) || unexpected.length) {
  throw Error('Use bpay-next-local-schema.mjs NEW or CHANGED; no other options are accepted');
}
const target = new URL(databaseUrl());
if (!['postgres:', 'postgresql:'].includes(target.protocol)
    || target.search || target.hash || !['localhost', '127.0.0.1'].includes(target.hostname)
    || target.port !== '55439' || decodeURIComponent(target.pathname) !== '/banking_modal_v2_test'
    || target.username !== 'cloudtms_bpay_local_owner'
    || process.env.CLOUDTMS_ALLOW_LOCAL !== '1'
    || process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER !== 'CURRENT_USER') {
  throw Error('BPAY_NEXT_LOCAL_SCHEMA_TARGET_INVALID');
}
// URI host validation is insufficient when libpq inherits hostaddr/service.
// These are process-only overrides; no machine/user environment is changed.
delete process.env.PGSERVICE;
delete process.env.PGSERVICEFILE;
process.env.PGHOSTADDR = '127.0.0.1';
process.env.PGHOST = '127.0.0.1';
process.env.PGPORT = '55439';
process.env.PGDATABASE = 'banking_modal_v2_test';
process.env.PGUSER = 'cloudtms_bpay_local_owner';
const current = verifyIntegrity();
const release = readJson('supabase/release/current-release.json');
const checkpointPath = 'docs/bpay-next/LOCAL_SCHEMA_CHECKPOINT.json';
const previous = fs.existsSync(path.join(repoRoot, checkpointPath)) ? readJson(checkpointPath) : null;
const checkpoint = value => {
  const absolute = path.join(repoRoot, checkpointPath);
  const temporary = absolute + `.preparing-${process.pid}`;
  fs.writeFileSync(temporary, JSON.stringify(value, null, 2) + '\n', { flag: 'wx' });
  fs.renameSync(temporary, absolute);
};
const options = { root: repoRoot, mapSource: mapExecutableSqlSource,
  executableFile: file => executableSqlFile(file, { canonical: true }) };
let context;
let session;
const execute = sql => session.execute(sql);
const reset = () => execute(resetWriterSql + assertWriterSql);
const ledgerSql = `select jsonb_build_object(
  'migrations',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',content_sha256)),'[]'::jsonb) from private.cloudtms_migration_ledger),
  'repeatables',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',closure_sha256)),'[]'::jsonb) from private.cloudtms_repeatable_ledger))::text;`;
try {
  session = openManagedPsqlSession();
  if ((await execute(acquireWriterSql)).trim() !== 't') throw Error('BPAY_NEXT_LOCAL_SCHEMA_WRITER_BUSY');
  await execute(assertWriterSql);
  const identity = JSON.parse(await execute(`select jsonb_build_object(
    'database',current_database(),'major',current_setting('server_version_num')::integer/10000,
    'owner',current_user,'session',session_user,'superuser',r.rolsuper,'bypassrls',r.rolbypassrls)
    from pg_catalog.pg_roles r where r.rolname=current_user;`));
  if (identity.database !== 'banking_modal_v2_test' || identity.major !== 17
      || identity.owner !== 'cloudtms_bpay_local_owner' || identity.session !== identity.owner
      || identity.superuser || identity.bypassrls) throw Error('BPAY_NEXT_LOCAL_SCHEMA_IDENTITY_INVALID');
  const present = (await execute("select (to_regclass('private.cloudtms_database_releases') is not null)::text;")).trim() === 'true';
  if (mode === 'NEW') {
    if (present) throw Error('BPAY_NEXT_LOCAL_NEW_REQUIRES_EMPTY_APPLICATION_SCHEMA');
    context = managedReleaseContext({ root: repoRoot, current, release, context: {
      expectedDatabase: identity.database, releaseId: `bpay-next-local-schema-${shellGitHead()}`,
      gitCommit: shellGitHead(), expectedHash: canonicalContractHash(readJson(release.contractPath)),
      environment: 'TEST', customerKey: '', mode: 'NEW',
    } });
    const anchor = assertBootstrapPrefix(current, release);
    const baseline = new Map(readJson(release.baselineRepeatableLock).repeatables.map(row => [row.path, row.sha256]));
    // Classify every selected closure before the first application DDL.
    let envelope = { migrations: current.migrations.slice(anchor + 1), repeatables: current.repeatables.filter(row => baseline.get(row.path) !== row.sha256) };
    if (fs.existsSync(path.join(repoRoot, 'docs/bpay-next/final-definer-closures.json'))) envelope = closeFinalDefinerSelection(envelope, current);
    for (const row of envelope.migrations) compileManagedUnit(row, 'migration', options);
    for (const row of envelope.repeatables) {
      compileManagedUnit(row, 'repeatable', options);
    }
    const bootstrap = compileBootstrap({ root: repoRoot, current, release, context, mapSource: mapExecutableSqlSource });
    checkpoint({ formatVersion: 1, status: 'LOCAL_SCHEMA_PREPARED', context, inventory: current,
      database: identity.database, owner: identity.owner, fullProtectedVerification: 'PENDING' });
    await reset();
    await execute(bootstrap);
    // The engine bootstrap receipt remains APPLYING, never VERIFIED. This is
    // a local schema compiler, with no catalogue rehearsal or fixture checks.
    checkpoint({ formatVersion: 1, status: 'LOCAL_SCHEMA_LOADING', context, inventory: current,
      database: identity.database, owner: identity.owner, fullProtectedVerification: 'PENDING' });
  } else {
    if (!present || previous?.formatVersion !== 1 || !previous.context
        || !['LOCAL_SCHEMA_PREPARED', 'LOCAL_SCHEMA_LOADING', 'LOCAL_SCHEMA_COMPILED'].includes(previous.status)
        || previous.database !== identity.database || previous.owner !== identity.owner
        || !/^bpay-next-local-schema-[0-9a-f]{40}$/.test(previous.context.releaseId)
        || previous.context.releaseId !== `bpay-next-local-schema-${previous.context.gitCommit}`
        || previous.context.mode !== 'NEW' || previous.context.environment !== 'TEST'
        || previous.context.expectedDatabase !== identity.database || previous.context.customerKey !== '') {
      throw Error('BPAY_NEXT_LOCAL_SCHEMA_CHECKPOINT_REQUIRED');
    }
    context = previous.context;
    await execute(identitySql(context));
    if ((await execute(resumeCheckSql(context))).trim() !== 'true') throw Error('BPAY_NEXT_LOCAL_SCHEMA_RESUME_MISMATCH');
    const receipt = (await execute(`select exists(select 1 from private.cloudtms_database_releases
      where release_id=${literal(context.releaseId)} and status='APPLYING')::text;`)).trim();
    if (receipt !== 'true') throw Error('BPAY_NEXT_LOCAL_SCHEMA_BOOTSTRAP_RECEIPT_INVALID');
  }
  const installed = JSON.parse(await execute(ledgerSql));
  const currentRoots = new Set(current.repeatables.map(row => row.path));
  for (const row of installed.repeatables) {
    if (!currentRoots.has(row.path)) throw Error(`BPAY_NEXT_LOCAL_INSTALLED_REPEATABLE_DELETED: ${row.path}`);
  }
  let pending = pendingInventory(current, installed);
  if (fs.existsSync(path.join(repoRoot, 'docs/bpay-next/final-definer-closures.json'))) pending = closeFinalDefinerSelection(pending, current);
  // Deleted repeatable authority cannot silently disappear from a checkpoint.
  if (previous?.inventory) {
    const roots = new Set(current.repeatables.map(row => row.path));
    for (const row of previous.inventory.repeatables) {
      if (!roots.has(row.path)) throw Error(`BPAY_NEXT_LOCAL_REPEATABLE_DELETED: ${row.path}`);
    }
  }
  const selected = new Map(pending.repeatables.map(row => [row.path, row]));
  const closureMapPath = path.join(repoRoot, 'docs/bpay-next/final-definer-closures.json');
  if (fs.existsSync(closureMapPath)) {
    const chains = JSON.parse(fs.readFileSync(closureMapPath, 'utf8')).chains;
    if (!Array.isArray(chains)) throw Error('BPAY_NEXT_LOCAL_FINAL_DEFINER_MAP_INVALID');
    let changed;
    do {
      changed = false;
      for (const chain of chains) {
        if (!chain.predecessors?.some(file => selected.has(file))) continue;
        for (const file of chain.finalClosures) {
          const row = current.repeatables.find(item => item.path === file);
          if (!row) throw Error(`BPAY_NEXT_LOCAL_FINAL_CLOSURE_MISSING: ${file}`);
          if (!selected.has(file)) { selected.set(file, row); changed = true; }
        }
      }
    } while (changed);
  }
  if (pending.migrations.length || selected.size) {
    for (const file of [
      'supabase/repeatable/24082026_0322_miget_postrestore_rpc_browser_isolation.sql',
      'supabase/repeatable/26082026_0230_general_security_definer_browser_isolation_v2.sql',
      'supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql',
    ]) {
      const row = current.repeatables.find(item => item.path === file);
      if (!row) throw Error(`BPAY_NEXT_LOCAL_ACL_CLOSURE_MISSING: ${file}`);
      selected.set(file, row);
    }
  }
  const chosen = current.repeatables.filter(row => selected.has(row.path));
  if (fs.existsSync(closureMapPath)) verifyFinalDefiners({ selectedRoots: chosen.map(row => row.path), selectedMigrations: pending.migrations.map(row => row.path), changed: new Set() });
  const phases = readerReleasePhases(chosen, current.repeatables);
  const migrationUnits = pending.migrations.map(row => compileManagedUnit(row, 'migration', options));
  const ordinaryUnits = phases.ordinary.map(row => compileManagedUnit(row, 'repeatable', options));
  const readerUnits = new Map(phases.readers.map(row => [row.path, compileManagedUnit(row, 'repeatable', options)]));
  checkpoint({ formatVersion: 1, status: 'LOCAL_SCHEMA_LOADING', context, inventory: current,
    database: identity.database, owner: identity.owner,
    selected: { migrations: pending.migrations, repeatables: chosen }, fullProtectedVerification: 'PENDING' });
  const apply = async unit => {
    await reset();
    console.log(`LOCAL LOAD ${unit.kind} ${unit.item.path}`);
    await execute(unitCheckpointSql(unit, context));
  };
  for (const unit of migrationUnits) await apply(unit);
  for (const unit of ordinaryUnits) await apply(unit);
  if (phases.readers.length) {
    await reset();
    await prepareManagedSourceReaders(execute);
    await reset();
    await execute(assertWriterSql + readerActivationSql(phases.readers,
      file => readerUnits.get(file).closure.source,
      row => assertWriterSql + repeatableLedgerSql(row, context)) + assertWriterSql);
  }
  // Independent security final closures must run AFTER newer routine bodies.
  for (const file of [
    'supabase/repeatable/24082026_0322_miget_postrestore_rpc_browser_isolation.sql',
    'supabase/repeatable/26082026_0230_general_security_definer_browser_isolation_v2.sql',
    'supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql',
  ]) {
    if (selected.has(file)) await apply(compileManagedUnit(selected.get(file), 'repeatable', options));
  }
  await reset();
  const remaining = pendingInventory(current, JSON.parse(await execute(ledgerSql)));
  if (remaining.migrations.length || remaining.repeatables.length) throw Error('BPAY_NEXT_LOCAL_SOURCE_CHECKPOINT_INCOMPLETE');
  // A concurrent editor must not make the saved receipt claim different bytes.
  if (JSON.stringify(inventory()) !== JSON.stringify(current)) throw Error('BPAY_NEXT_LOCAL_SOURCE_CHANGED_DURING_LOAD');
  await execute("notify pgrst,'reload schema';");
  checkpoint({ formatVersion: 1, status: 'LOCAL_SCHEMA_COMPILED', context,
    database: identity.database, owner: identity.owner, sourceCommit: shellGitHead(),
    compiledAtUtc: new Date().toISOString(), inventory: current,
    selected: { migrations: pending.migrations, repeatables: chosen }, fullProtectedVerification: 'PENDING' });
  console.log(`LOCAL_SCHEMA_COMPILED (${pending.migrations.length} migrations, ${chosen.length} repeatable closures); protected release verification pending.`);
} finally {
  if (session) await session.close();
}
