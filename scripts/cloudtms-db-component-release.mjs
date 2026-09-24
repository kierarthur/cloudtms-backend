#!/usr/bin/env node
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import {
  mapLogicalPostgresOwnerSql,
  psql,
  repoRoot,
  shellGitHead,
  validateExpectedDatabase,
  validateTarget,
} from './cloudtms-db-release-lib.mjs';

const MANIFEST_PATH = 'supabase/release/weekly-source-invoice-evidence-component.json';
const COMPONENT_ID = 'weekly-source-invoice-evidence-20260924';
const EXACT_FILES = [
  'supabase/repeatable/15092026_1534_weekly_source_invoice_batch_integration_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql',
];
const EXACT_VERIFIERS = [
  'supabase/verification/15092026_1534_weekly_source_invoice_batch_integration_v1.sql',
  'supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql',
  'supabase/verification/24092026_1800_weekly_source_invoice_evidence_component_v1.sql',
];
const EXACT_NODE_TESTS = ['tests/database-component-release-system.test.mjs'];
const OPEN_STAGE2_VERIFIERS = [
  'supabase/verification/17092026_0610_weekly_source_withdrawal_supersession_v1.sql',
  'supabase/verification/17092026_0800_weekly_source_workbench_seams_v1.sql',
  'supabase/verification/17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql',
];

const [command, ...rawOptions] = process.argv.slice(2);
const options = Object.fromEntries(rawOptions.map((arg) => {
  const match = arg.match(/^--([^=]+)=(.*)$/);
  if (!match) throw new Error(`Invalid option: ${arg}`);
  return [match[1], match[2]];
}));

function sqlLiteral(value) { return `'${String(value).replaceAll("'", "''")}'`; }
function sha256(bytes) { return crypto.createHash('sha256').update(bytes).digest('hex'); }
function sameArray(a, b) { return JSON.stringify(a) === JSON.stringify(b); }

function readManifest() {
  const manifest = JSON.parse(fs.readFileSync(path.join(repoRoot, MANIFEST_PATH), 'utf8'));
  if (manifest.formatVersion !== 1 || manifest.componentId !== COMPONENT_ID
      || manifest.environment !== 'TEST' || manifest.mode !== 'COMPONENT_UPGRADE') {
    throw new Error('The fixed TEST component manifest identity is invalid');
  }
  if (!sameArray(manifest.files.map((item) => item.path), EXACT_FILES)
      || !sameArray(manifest.verificationFiles, EXACT_VERIFIERS)
      || !sameArray(manifest.nodeTests, EXACT_NODE_TESTS)
      || !sameArray(manifest.excludedOpenScope, OPEN_STAGE2_VERIFIERS)) {
    throw new Error('The fixed component allowlist has changed');
  }
  if (!/^[0-9a-f]{40}$/.test(manifest.baseCommit)) throw new Error('Invalid component base commit');
  for (const item of manifest.files) {
    if (!/^[0-9a-f]{40}$/.test(item.beforeCommit)
        || !/^[0-9a-f]{64}$/.test(item.beforeSourceSha256)
        || !/^[0-9a-f]{64}$/.test(item.beforeClosureSha256)
        || !/^[0-9a-f]{64}$/.test(item.afterSourceSha256)
        || !/^[0-9a-f]{64}$/.test(item.afterClosureSha256)) {
      throw new Error(`Invalid component hash for ${item.path}`);
    }
    const actual = sha256(fs.readFileSync(path.join(repoRoot, item.path)));
    if (actual !== item.afterSourceSha256) throw new Error(`Current component source hash mismatch: ${item.path}`);
    const result = spawnSync('git', ['show', `${item.beforeCommit}:${item.path}`], {
      cwd: repoRoot, encoding: null, maxBuffer: 32 * 1024 * 1024,
    });
    if (result.status !== 0 || sha256(result.stdout) !== item.beforeSourceSha256) {
      throw new Error(`Base component source hash mismatch: ${item.path}`);
    }
  }
  for (const relative of [...manifest.verificationFiles, ...manifest.nodeTests]) {
    if (!fs.existsSync(path.join(repoRoot, relative))) throw new Error(`Component evidence file is missing: ${relative}`);
  }
  return manifest;
}

function assertTestTarget() {
  if ((options.environment || process.env.CLOUDTMS_ENVIRONMENT) !== 'TEST') {
    throw new Error('This component release is TEST-only');
  }
  if (options.mode && options.mode !== 'COMPONENT_UPGRADE') {
    throw new Error('This component release supports only COMPONENT_UPGRADE');
  }
  validateTarget('TEST', process.env.CLOUDTMS_EXPECTED_TARGET);
  const expected = validateExpectedDatabase(process.env.CLOUDTMS_EXPECTED_DATABASE);
  if (psql({ sql: 'select pg_catalog.current_database();' }) !== expected) {
    throw new Error('Connected database does not match CLOUDTMS_EXPECTED_DATABASE');
  }
  const environment = psql({
    sql: "select environment from private.cloudtms_database_identity where singleton is true;",
  });
  if (environment !== 'TEST') throw new Error('CLOUDTMS_DATABASE_IDENTITY_MISMATCH');
}

function ledgerState(manifest) {
  const rows = psql({ sql: `
    select path || E'\\t' || closure_sha256 || E'\\t' || last_release_id
    from private.cloudtms_repeatable_ledger
    where path in (${manifest.files.map((item) => sqlLiteral(item.path)).join(',')})
    order by path;
  ` });
  const installed = new Map(rows ? rows.split(/\r?\n/).map((line) => {
    const [pathValue, hashValue, releaseValue] = line.split('\t');
    return [pathValue, { hashValue, releaseValue }];
  }) : []);
  return manifest.files.map((item) => {
    const actual = installed.get(item.path);
    if (!actual) throw new Error(`Required repeatable ledger entry is missing: ${item.path}`);
    if (![item.beforeClosureSha256, item.afterClosureSha256].includes(actual.hashValue)) {
      throw new Error(`Unrecognised installed repeatable hash: ${item.path}`);
    }
    return { ...item, installedClosureSha256: actual.hashValue,
      installedLastReleaseId: actual.releaseValue,
      pending: actual.hashValue !== item.afterClosureSha256 };
  });
}

function executableBody(bytes, relative) {
  const source = bytes.toString('utf8').replace(/^\uFEFF/, '');
  const withoutMeta = source.replace(/^\s*\\set\s+ON_ERROR_STOP\s+on\s*\r?\n/im, '');
  const begins = [...withoutMeta.matchAll(/^\s*begin;\s*$/gim)];
  const commits = [...withoutMeta.matchAll(/^\s*commit;\s*$/gim)];
  if (begins.length !== 1 || commits.length !== 1 || begins[0].index > commits[0].index) {
    throw new Error(`Component file must have one outer BEGIN/COMMIT: ${relative}`);
  }
  return withoutMeta
    .replace(/^\s*begin;\s*$/im, '')
    .replace(/^\s*commit;\s*$/im, '')
    .trim();
}

function gitBytes(commit, relative) {
  const result = spawnSync('git', ['show', `${commit}:${relative}`], {
    cwd: repoRoot, encoding: null, maxBuffer: 32 * 1024 * 1024,
  });
  if (result.status !== 0) throw new Error(`Cannot read ${relative} at ${commit}`);
  return result.stdout;
}

function componentContractHash(manifest) {
  return sha256(JSON.stringify(manifest.files.map((item) => ({
    path: item.path, closureSha256: item.afterClosureSha256,
  }))));
}

function atomicSql(manifest, useAfter, releaseId, priorState = []) {
  const bodies = manifest.files.map((item) => executableBody(
    useAfter ? fs.readFileSync(path.join(repoRoot, item.path)) : gitBytes(item.beforeCommit, item.path),
    item.path,
  ));
  const priorByPath = new Map(priorState.map((item) => [item.path, item]));
  const ledger = manifest.files.map((item) => {
    const hash = useAfter ? item.afterClosureSha256 : item.beforeClosureSha256;
    const ledgerReleaseId = useAfter
      ? releaseId
      : priorByPath.get(item.path)?.installedLastReleaseId;
    if (!ledgerReleaseId) throw new Error(`Prior ledger release identity is missing: ${item.path}`);
    return `insert into private.cloudtms_repeatable_ledger(path,closure_sha256,last_release_id)
      values (${sqlLiteral(item.path)},${sqlLiteral(hash)},${sqlLiteral(ledgerReleaseId)})
      on conflict(path) do update set closure_sha256=excluded.closure_sha256,
        last_release_id=excluded.last_release_id,applied_at_utc=pg_catalog.clock_timestamp();`;
  });
  const contractHash = componentContractHash(manifest);
  const releaseStart = useAfter ? `insert into private.cloudtms_database_releases(
      release_id,git_commit,repository_contract_sha256,installed_contract_sha256,
      install_mode,status,completed_at_utc,evidence_json
    ) values (
      ${sqlLiteral(releaseId)},${sqlLiteral(shellGitHead())},${sqlLiteral(contractHash)},
      ${sqlLiteral(contractHash)},'UPGRADE','APPLYING',null,
      ${sqlLiteral(JSON.stringify({ contract: 'CLOUDTMS_TEST_COMPONENT_RELEASE_V1', componentId: COMPONENT_ID }))}::jsonb
    ) on conflict(release_id) do update set
      git_commit=excluded.git_commit,
      repository_contract_sha256=excluded.repository_contract_sha256,
      installed_contract_sha256=excluded.installed_contract_sha256,
      status='APPLYING',started_at_utc=pg_catalog.clock_timestamp(),
      completed_at_utc=null,evidence_json=excluded.evidence_json;` : '';
  const releaseFailure = useAfter ? '' : `update private.cloudtms_database_releases
    set status='FAILED',completed_at_utc=pg_catalog.clock_timestamp(),
      evidence_json=evidence_json || '{"failure":"component_verification_failed_and_rolled_back"}'::jsonb
    where release_id=${sqlLiteral(releaseId)};`;
  return mapLogicalPostgresOwnerSql([
    '\\set ON_ERROR_STOP on',
    'begin;',
    "set local lock_timeout='10s';",
    "set local statement_timeout='120s';",
    "select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0));",
    releaseStart,
    ...bodies,
    ...ledger,
    releaseFailure,
    "select pg_catalog.pg_notify('pgrst','reload schema');",
    'commit;',
    '',
  ].join('\n\n'));
}

function markComponentVerified(releaseId, evidence) {
  psql({ sql: `update private.cloudtms_database_releases
    set status='VERIFIED',completed_at_utc=pg_catalog.clock_timestamp(),
      evidence_json=evidence_json || ${sqlLiteral(JSON.stringify(evidence))}::jsonb
    where release_id=${sqlLiteral(releaseId)} and status='APPLYING';
    do $verify$ begin
      if not exists (select 1 from private.cloudtms_database_releases
        where release_id=${sqlLiteral(releaseId)} and status='VERIFIED') then
        raise exception 'CLOUDTMS_COMPONENT_RELEASE_NOT_VERIFIED';
      end if;
    end $verify$;` });
}

function runAtomic(sql, label) {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cloudtms-component-release-'));
  const file = path.join(tempDir, `${label}.sql`);
  try {
    fs.writeFileSync(file, sql, { encoding: 'utf8', flag: 'wx' });
    psql({ file });
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

function runVerifiers(manifest) {
  for (const file of manifest.verificationFiles) psql({ file });
}

function verifierPrelude(relative, stopMarker = null) {
  let source = fs.readFileSync(path.join(repoRoot, relative), 'utf8').replace(/^\uFEFF/, '');
  if (stopMarker) {
    const index = source.indexOf(stopMarker);
    if (index < 0) throw new Error(`Rehearsal stop marker is absent: ${relative}`);
    source = source.slice(0, index);
  }
  return source.replace(/^\s*\\(?:set|pset)\b.*$/gim, '').trim();
}

function installedDefinitionReceipt() {
  const identities = [
    'public.weekly_source_invoice_batch_candidates_v1(jsonb)',
    'public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)',
    'public.weekly_source_invoice_edit_context_v1(jsonb)',
    'public.weekly_source_office_workspace_v1(jsonb)',
    'public.weekly_source_office_timesheet_presentation_v1(jsonb)',
    'public.weekly_source_office_bulk_query_action_atomic_v1(jsonb)',
    'public.weekly_source_no_shifts_attest_atomic_v1(jsonb)',
  ];
  return JSON.parse(psql({ sql: `select pg_catalog.jsonb_object_agg(identity,
      pg_catalog.md5(pg_catalog.pg_get_functiondef(identity::regprocedure)))
    from pg_catalog.unnest(array[${identities.map(sqlLiteral).join(',')}]::text[]) identity;` }));
}

function checkSource() {
  const manifest = readManifest();
  for (const open of OPEN_STAGE2_VERIFIERS) {
    if (manifest.verificationFiles.includes(open)) throw new Error(`Open Stage 2 verifier entered component scope: ${open}`);
  }
  console.log(JSON.stringify({ componentId: COMPONENT_ID, source: 'PASS', commit: shellGitHead() }, null, 2));
  return manifest;
}

function plan() {
  const manifest = checkSource();
  assertTestTarget();
  const state = ledgerState(manifest);
  console.log(JSON.stringify({ componentId: COMPONENT_ID, environment: 'TEST', phase: 'PLAN', files: state }, null, 2));
}

function rehearse() {
  const manifest = checkSource();
  assertTestTarget();
  ledgerState(manifest);
  const before = installedDefinitionReceipt();
  const definitions = manifest.files.map((item) => executableBody(
    fs.readFileSync(path.join(repoRoot, item.path)), item.path,
  ));
  const invoiceCatalogueChecks = verifierPrelude(
    manifest.verificationFiles[0],
    '\\set weekly_source_verification_outer_transaction true',
  );
  const componentChecks = verifierPrelude(manifest.verificationFiles[2]);
  runAtomic(mapLogicalPostgresOwnerSql([
    '\\set ON_ERROR_STOP on',
    'begin;',
    "set local lock_timeout='10s';",
    "set local statement_timeout='120s';",
    ...definitions,
    invoiceCatalogueChecks,
    componentChecks,
    'rollback;',
    '',
  ].join('\n\n')), 'rehearse');
  const after = installedDefinitionReceipt();
  if (JSON.stringify(before) !== JSON.stringify(after)) {
    throw new Error('Rollback rehearsal changed an installed definition');
  }
  console.log(JSON.stringify({ componentId: COMPONENT_ID, environment: 'TEST',
    phase: 'REHEARSE_ROLLBACK', rolledBack: true, installedDefinitionsUnchanged: true }, null, 2));
}

function apply() {
  const manifest = checkSource();
  assertTestTarget();
  const commit = shellGitHead();
  const expectedApproval = `APPLY TEST COMPONENT ${COMPONENT_ID} ${commit}`;
  if (process.env.CLOUDTMS_RELEASE_APPROVAL !== expectedApproval) {
    throw new Error('Exact TEST component APPLY approval phrase does not match this commit');
  }
  const before = ledgerState(manifest);
  const changed = before.some((item) => item.pending);
  const releaseId = `${COMPONENT_ID}-${commit.slice(0, 12)}`;
  try {
    runAtomic(atomicSql(manifest, true, releaseId, before), 'apply');
    runVerifiers(manifest);
    const after = ledgerState(manifest);
    if (after.some((item) => item.pending)) throw new Error('Component ledger did not reach the exact after hashes');
    const receipt = { componentId: COMPONENT_ID, environment: 'TEST', phase: 'APPLY',
      changed, commit, files: after, installedDefinitions: installedDefinitionReceipt(),
      excludedOpenScope: OPEN_STAGE2_VERIFIERS };
    markComponentVerified(releaseId, {
      verification: 'PASS',
      exactFiles: manifest.files.map((item) => item.path),
      excludedOpenScope: OPEN_STAGE2_VERIFIERS,
    });
    console.log(JSON.stringify(receipt, null, 2));
  } catch (error) {
    if (changed) runAtomic(atomicSql(manifest, false, releaseId, before), 'rollback');
    throw error;
  }
}

if (command === 'check') checkSource();
else if (command === 'plan') plan();
else if (command === 'rehearse') rehearse();
else if (command === 'apply') apply();
else throw new Error('Usage: cloudtms-db-component-release.mjs check|plan|rehearse|apply --environment=TEST');
