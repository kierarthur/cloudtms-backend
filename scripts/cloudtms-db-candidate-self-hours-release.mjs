#!/usr/bin/env node
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import {
  closureFor, exportContract, mapLogicalPostgresOwnerSql, psql, repoRoot,
  shellGitHead, validateExpectedDatabase, validateTarget,
} from './cloudtms-db-release-lib.mjs';

const componentId = 'candidate-source-self-hours-20260926';
const manifestPath = 'supabase/release/candidate-source-self-hours-component.json';
const expectedFiles = [
  'supabase/repeatable/04092026_1603_candidate_expense_email_admission_v1.sql',
  'supabase/repeatable/04092026_1952_candidate_expense_history_anchor_recovery_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql',
  'supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql',
  'supabase/repeatable/17092026_1100_weekly_source_candidate_view_producer_v1.sql',
];
const expectedVerifiers = [
  'supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql',
  'supabase/verification/15092026_2203_weekly_source_candidate_app_contract_v1.sql',
  'supabase/verification/17092026_1100_weekly_source_candidate_view_producer_v1.sql',
  'supabase/verification/27082026_1947_candidate_named_security_verification_v3.sql',
];
const [command] = process.argv.slice(2);
const manifest = JSON.parse(fs.readFileSync(path.join(repoRoot, manifestPath), 'utf8'));
const quote = value => `'${String(value).replaceAll("'", "''")}'`;
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
const routineKey = value => `${value.schema}.${value.identity}`;

function gitBytes(commit, relative) {
  const result = spawnSync('git', ['show', `${commit}:${relative}`], {
    cwd: repoRoot, encoding: null, maxBuffer: 64 * 1024 * 1024,
  });
  if (result.status !== 0) throw new Error(`Cannot read pinned base source: ${relative}`);
  return result.stdout;
}

function closureHash(relative, bytes) {
  const canonical = Buffer.from(bytes.toString('utf8').replaceAll('\r\n', '\n'));
  return sha256(Buffer.concat([Buffer.from(`${relative}\0`), canonical, Buffer.from('\0')]));
}

function contractAtBase() {
  return JSON.parse(gitBytes(manifest.baseCommit, 'supabase/release/current-contract.json').toString('utf8'));
}

function currentContract() {
  return JSON.parse(fs.readFileSync(path.join(repoRoot, 'supabase/release/current-contract.json'), 'utf8'));
}

function selected(contract) {
  const byKey = new Map(contract.routines.map(row => [routineKey(row), row]));
  return manifest.routineIdentities.flatMap(key => byKey.has(key) ? [byKey.get(key)] : []);
}

function expectedBeforeRoutines() {
  const overrides = manifest.measuredBeforeDefinitionOverrides;
  const rows = selected(contractAtBase()).map(row => ({
    ...row, definition_sha256: overrides[routineKey(row)] ?? row.definition_sha256,
  }));
  if (Object.keys(overrides).length !== 2
      || Object.keys(overrides).some(key => !rows.some(row => routineKey(row) === key))
      || sha256(JSON.stringify(rows)) !== manifest.measuredBeforeRoutineContractSha256) {
    throw new Error('Measured PostgreSQL 17 before-routine seal changed');
  }
  return rows;
}

function check() {
  if (manifest.formatVersion !== 1 || manifest.componentId !== componentId
      || manifest.environment !== 'TEST' || manifest.database !== 'cloudtms_test_clone'
      || !/^[0-9a-f]{40}$/.test(manifest.baseCommit)
      || JSON.stringify(manifest.files.map(row => row.path)) !== JSON.stringify(expectedFiles)
      || JSON.stringify(manifest.verifiers) !== JSON.stringify(expectedVerifiers)
      || manifest.routineIdentities.length !== 8
      || new Set(manifest.routineIdentities).size !== 8) {
    throw new Error('Fixed TEST component identity, file set or routine inventory changed');
  }
  for (const file of manifest.files) {
    const before = gitBytes(manifest.baseCommit, file.path);
    const after = fs.readFileSync(path.join(repoRoot, file.path));
    if (sha256(before) !== file.beforeSourceSha256
        || closureHash(file.path, before) !== file.beforeClosureSha256
        || sha256(after) !== file.afterSourceSha256
        || closureFor(file.path).sha256 !== file.afterClosureSha256) {
      throw new Error(`Pinned component file hash mismatch: ${file.path}`);
    }
    if (closureFor(file.path).paths.length !== 1) {
      throw new Error(`Component source includes an unapproved SQL dependency: ${file.path}`);
    }
  }
  for (const verifier of manifest.verifiers) {
    if (!fs.existsSync(path.join(repoRoot, verifier))) throw new Error(`Missing verifier: ${verifier}`);
  }
  const before = contractAtBase();
  const after = currentContract();
  for (const section of Object.keys(before)) {
    if (section !== 'routines' && JSON.stringify(before[section]) !== JSON.stringify(after[section])) {
      throw new Error(`Unrelated contract section changed: ${section}`);
    }
  }
  const beforeMap = new Map(before.routines.map(row => [routineKey(row), row]));
  const afterMap = new Map(after.routines.map(row => [routineKey(row), row]));
  const changed = new Set([...beforeMap.keys(), ...afterMap.keys()].filter(key =>
    JSON.stringify(beforeMap.get(key)) !== JSON.stringify(afterMap.get(key))));
  if (JSON.stringify([...changed].sort()) !== JSON.stringify([...manifest.routineIdentities].sort())) {
    throw new Error('Generated contract delta is not exactly the approved eight routines');
  }
  if (expectedBeforeRoutines().length !== 7 || selected(after).length !== 8) {
    throw new Error('Candidate component before/after routine cardinality changed');
  }
  console.log(JSON.stringify({ componentId, phase: 'CHECK', files: 5, changedRoutines: 8 }));
}

function requireTestTarget() {
  if (process.env.CLOUDTMS_ENVIRONMENT !== 'TEST') throw new Error('TEST only');
  validateTarget('TEST', process.env.CLOUDTMS_EXPECTED_TARGET);
  if (validateExpectedDatabase(process.env.CLOUDTMS_EXPECTED_DATABASE) !== manifest.database
      || psql({ sql: 'select pg_catalog.current_database();' }) !== manifest.database
      || psql({ sql: 'select environment from private.cloudtms_database_identity where singleton;' }) !== 'TEST') {
    throw new Error('Agency TEST database identity mismatch');
  }
}

function ledger() {
  const rows = JSON.parse(psql({ sql: `select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object('path',path,'hash',closure_sha256,'release',last_release_id)
    order by path),'[]'::jsonb)::text from private.cloudtms_repeatable_ledger
    where path in (${expectedFiles.map(quote).join(',')});` }));
  const byPath = new Map(rows.map(row => [row.path, row]));
  return manifest.files.map(file => {
    const installed = byPath.get(file.path);
    if (!installed || ![file.beforeClosureSha256, file.afterClosureSha256].includes(installed.hash)) {
      throw new Error(`Unknown installed repeatable authority: ${file.path}`);
    }
    return { ...file, installedHash: installed.hash, priorRelease: installed.release,
      pending: installed.hash !== file.afterClosureSha256 };
  });
}

function unwrapSource(relative) {
  const source = fs.readFileSync(path.join(repoRoot, relative), 'utf8')
    .replace(/^\s*\\set\s+ON_ERROR_STOP\s+on\s*$/gim, '');
  if ((source.match(/^\s*begin;\s*$/gim) ?? []).length !== 1
      || (source.match(/^\s*commit;\s*$/gim) ?? []).length !== 1
      || /^\s*\\/m.test(source)) {
    throw new Error(`Unexpected SQL transaction boundary: ${relative}`);
  }
  return source.replace(/^\s*(?:begin|commit);\s*$/gim, '').trim();
}

function verifierBody(relative) {
  const raw = fs.readFileSync(path.join(repoRoot, relative), 'utf8')
    .replace(/^\s*\\(?:set|pset)\b.*$/gim, '');
  if (/^\s*\\/m.test(raw) || /^\s*commit;\s*$/im.test(raw)) {
    throw new Error(`Verifier has an unapproved command: ${relative}`);
  }
  const starts = [...raw.matchAll(/^\s*begin;\s*$/gim)];
  const ends = [...raw.matchAll(/^\s*rollback;\s*$/gim)];
  if (starts.length === 0 && ends.length === 0) return raw.trim();
  if (starts.length !== 1 || ends.length !== 1 || starts[0].index >= ends[0].index) {
    throw new Error(`Verifier transaction shape changed: ${relative}`);
  }
  return raw.slice(starts[0].index + starts[0][0].length, ends[0].index).trim();
}

function snapshotSql(table) {
  const exportSource = fs.readFileSync(path.join(repoRoot, 'supabase/release/export_contract.sql'), 'utf8');
  const query = exportSource.slice(exportSource.indexOf('\nwith\n')).trim().replace(/::text;\s*$/, ';');
  if (!query.startsWith('with\n') || !query.endsWith(';')) throw new Error('Contract exporter shape changed');
  return `create temp table ${table}(value) on commit drop as ${query}`;
}

function selectedSql(table) {
  return `(select coalesce(pg_catalog.jsonb_agg(row order by row->>'schema',row->>'identity'),'[]'::jsonb)
    from ${table} snapshot cross join lateral pg_catalog.jsonb_array_elements(snapshot.value->'routines') row
    where (row->>'schema')||'.'||(row->>'identity') in
      (${manifest.routineIdentities.map(quote).join(',')}))`;
}

function outsideSql(table) {
  return `(select pg_catalog.jsonb_set(snapshot.value,'{routines}',
    coalesce((select pg_catalog.jsonb_agg(row order by row->>'schema',row->>'identity')
      from pg_catalog.jsonb_array_elements(snapshot.value->'routines') row
      where (row->>'schema')||'.'||(row->>'identity') not in
        (${manifest.routineIdentities.map(quote).join(',')})),'[]'::jsonb))
    from ${table} snapshot)`;
}

function assertCatalogue(beforeTable, afterTable) {
  return `do $candidate_component_catalogue$ begin
    if ${selectedSql(beforeTable)} is distinct from ${quote(JSON.stringify(expectedBeforeRoutines()))}::jsonb
      then raise exception 'CANDIDATE_SELF_HOURS_BEFORE_CONTRACT_MISMATCH'; end if;
    if ${selectedSql(afterTable)} is distinct from ${quote(JSON.stringify(selected(currentContract())))}::jsonb
      then raise exception 'CANDIDATE_SELF_HOURS_AFTER_CONTRACT_MISMATCH'; end if;
    if ${outsideSql(beforeTable)} is distinct from ${outsideSql(afterTable)}
      then raise exception 'CANDIDATE_SELF_HOURS_OUTSIDE_CATALOGUE_CHANGED: %',
        (select pg_catalog.string_agg(b.key,',' order by b.key)
         from pg_catalog.jsonb_each(${outsideSql(beforeTable)}) b
         join pg_catalog.jsonb_each(${outsideSql(afterTable)}) a using (key)
         where b.value is distinct from a.value); end if;
  end $candidate_component_catalogue$;`;
}

function runAtomic(sql, phase) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'cloudtms-candidate-hours-release-'));
  const file = path.join(directory, `${phase}.sql`);
  try {
    fs.writeFileSync(file, mapLogicalPostgresOwnerSql(sql), { flag: 'wx' });
    psql({ file });
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
}

function definitions() { return manifest.files.map(file => unwrapSource(file.path)).join('\n\n'); }

function plan() {
  check(); requireTestTarget();
  console.log(JSON.stringify({ componentId, phase: 'PLAN', database: manifest.database,
    files: ledger().map(row => ({ path: row.path, pending: row.pending, installedHash: row.installedHash })) }));
}

function assertAlreadyInstalled() {
  if (JSON.stringify(selected(exportContract())) !== JSON.stringify(selected(currentContract()))) {
    throw new Error('Ledger says installed but the exact Candidate routine contract differs');
  }
}

function rehearse() {
  check(); requireTestTarget();
  const state = ledger();
  if (state.some(row => !row.pending) && state.some(row => row.pending)) {
    throw new Error('Partial installed component is not an approved starting state');
  }
  if (state.every(row => !row.pending)) {
    assertAlreadyInstalled();
    console.log(JSON.stringify({ componentId, phase: 'REHEARSE', alreadyInstalled: true }));
    return;
  }
  const verifiers = manifest.verifiers.map((file, index) =>
    `savepoint candidate_component_verifier_${index};\n${verifierBody(file)}\n`
    + `rollback to savepoint candidate_component_verifier_${index};\n`
    + `release savepoint candidate_component_verifier_${index};`).join('\n\n');
  runAtomic(`\\set ON_ERROR_STOP on\nbegin;\nset local lock_timeout='10s';\nset local statement_timeout='120s';\n`
    + `select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0));\n`
    + `${snapshotSql('candidate_component_before')}\n${definitions()}\n`
    + `${snapshotSql('candidate_component_after')}\n${assertCatalogue('candidate_component_before','candidate_component_after')}\n`
    + `${verifiers}\nrollback;`, 'rehearse');
  console.log(JSON.stringify({ componentId, phase: 'REHEARSE', rolledBack: true,
    verifiedFiles: manifest.verifiers }));
}

function apply() {
  check(); requireTestTarget();
  const commit = shellGitHead();
  if (process.env.CLOUDTMS_RELEASE_APPROVAL !== `APPLY TEST COMPONENT ${componentId} ${commit}`) {
    throw new Error('Exact protected TEST component approval phrase is missing');
  }
  if (process.env.CLOUDTMS_ALLOW_LOCAL !== '1'
      && (process.env.GITHUB_REPOSITORY !== 'kierarthur/cloudtms-backend'
        || process.env.GITHUB_REF !== 'refs/heads/test')) {
    throw new Error('Hosted APPLY requires the protected canonical TEST branch workflow');
  }
  const state = ledger();
  if (state.some(row => !row.pending) && state.some(row => row.pending)) {
    throw new Error('Partial installed component is not an approved starting state');
  }
  if (state.every(row => !row.pending)) {
    assertAlreadyInstalled();
    console.log(JSON.stringify({ componentId, phase: 'APPLY', alreadyInstalled: true }));
    return;
  }
  const releaseId = `${componentId}-${commit.slice(0, 12)}`;
  const componentHash = sha256(JSON.stringify(selected(currentContract())));
  const ledgerSql = state.map(row => `do $ledger_check$ begin
    update private.cloudtms_repeatable_ledger
    set closure_sha256=${quote(row.afterClosureSha256)},last_release_id=${quote(releaseId)},
        applied_at_utc=pg_catalog.clock_timestamp()
    where path=${quote(row.path)} and closure_sha256=${quote(row.beforeClosureSha256)};
    if not found then raise exception 'CANDIDATE_COMPONENT_LEDGER_CONFLICT'; end if;
    end $ledger_check$;`).join('\n');
  runAtomic(`\\set ON_ERROR_STOP on\nbegin;\nset local lock_timeout='10s';\nset local statement_timeout='120s';\n`
    + `select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0));\n`
    + `${snapshotSql('candidate_component_before')}\n${definitions()}\n`
    + `${snapshotSql('candidate_component_after')}\n${assertCatalogue('candidate_component_before','candidate_component_after')}\n`
    + `insert into private.cloudtms_database_releases(
      release_id,git_commit,repository_contract_sha256,installed_contract_sha256,
      install_mode,status,completed_at_utc,evidence_json
    ) values (${quote(releaseId)},${quote(commit)},${quote(componentHash)},${quote(componentHash)},
      'UPGRADE','APPLYING',null,
      ${quote(JSON.stringify({ component: componentId, scope: 'exact-eight-routine-delta' }))}::jsonb);
    ${ledgerSql}
    update private.cloudtms_database_releases
      set status='VERIFIED',completed_at_utc=pg_catalog.clock_timestamp()
      where release_id=${quote(releaseId)} and status='APPLYING';
    select pg_catalog.pg_notify('pgrst','reload schema');\ncommit;`, 'apply');
  const after = ledger();
  if (after.some(row => row.pending)) throw new Error('Component ledger did not reach exact approved hashes');
  const installed = exportContract();
  if (JSON.stringify(selected(installed)) !== JSON.stringify(selected(currentContract()))) {
    throw new Error('Installed Candidate routine contract does not match the release receipt');
  }
  console.log(JSON.stringify({ componentId, phase: 'APPLY', releaseId, database: manifest.database,
    verified: true, files: after.map(row => row.path) }));
}

if (command === 'check') check();
else if (command === 'plan') plan();
else if (command === 'rehearse') rehearse();
else if (command === 'apply') apply();
else throw new Error('Use check, plan, rehearse or apply');
