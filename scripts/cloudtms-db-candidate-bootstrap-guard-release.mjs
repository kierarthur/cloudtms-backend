#!/usr/bin/env node
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import {
  canonicalSqlBytes, exportContract, mapLogicalPostgresOwnerSql, psql,
  repoRoot, shellGitHead, validateExpectedDatabase, validateTarget,
} from './cloudtms-db-release-lib.mjs';

const component = 'candidate-source-bootstrap-guard-20260926';
const base = 'ac760759c23d96ce10a8453550dfcef28b6cabb8';
const file = 'supabase/repeatable/04092026_1603_candidate_expense_email_admission_v1.sql';
const key = 'private._candidate_record_capabilities_v1(p_timesheet_id uuid, p_contract_week_id uuid, p_proposed_claim jsonb)';
const sourceHash = 'f283c6011f70e4bf2534a7499d40ae797053f5d5e4cf115c3169ecb01d3f228c';
const beforeHash = 'b1c16ba7a443d989b733429137a432d2b3d1c5c85be9b423d3139043a6fbc85d';
const afterHash = 'ed0aed43dfa2db694f661cced13bad0f8e8be8fa74409de1e71748e983b0551d';
const quote = value => `'${String(value).replaceAll("'", "''")}'`;
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
const identity = row => `${row.schema}.${row.identity}`;
const source = fs.readFileSync(path.join(repoRoot, file), 'utf8');
const pattern = /^create or replace function private\._candidate_record_capabilities_v1\([\s\S]*?\n\$function\$;/gim;

function gitText(relative) {
  const result = spawnSync('git', ['show', `${base}:${relative}`], {
    cwd: repoRoot, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024,
  });
  if (result.status !== 0) throw new Error(`Cannot read pinned predecessor: ${relative}`);
  return result.stdout;
}

function soleDefinition(text) {
  const matches = [...text.matchAll(pattern)];
  if (matches.length !== 1) throw new Error('Expected exactly one Candidate capability definition');
  return matches[0][0];
}

function selected(contract) {
  const rows = contract.routines.filter(row => identity(row) === key);
  if (rows.length !== 1) throw new Error('Candidate capability contract cardinality changed');
  return rows[0];
}

function check() {
  if (sha256(canonicalSqlBytes(file)) !== sourceHash) throw new Error('Pinned source hash mismatch');
  const previous = gitText(file);
  if (previous.replace(soleDefinition(previous), '') !== source.replace(soleDefinition(source), '')) {
    throw new Error('The repeatable contains another unapproved change');
  }
  const oldContract = JSON.parse(gitText('supabase/release/current-contract.json'));
  const newContract = JSON.parse(fs.readFileSync(path.join(repoRoot, 'supabase/release/current-contract.json'), 'utf8'));
  const oldRow = selected(oldContract);
  const newRow = selected(newContract);
  if (oldRow.definition_sha256 !== beforeHash || newRow.definition_sha256 !== afterHash
      || JSON.stringify({ ...oldRow, definition_sha256: afterHash }) !== JSON.stringify(newRow)) {
    throw new Error('One-routine before/after contract mismatch');
  }
  oldContract.routines = oldContract.routines.map(row => identity(row) === key ? newRow : row);
  if (JSON.stringify(oldContract) !== JSON.stringify(newContract)) {
    throw new Error('Unrelated generated contract change');
  }
  return { before: oldRow, after: newRow };
}

function target() {
  if (process.env.CLOUDTMS_ENVIRONMENT !== 'TEST') throw new Error('TEST only');
  validateTarget('TEST', process.env.CLOUDTMS_EXPECTED_TARGET);
  if (validateExpectedDatabase(process.env.CLOUDTMS_EXPECTED_DATABASE) !== 'cloudtms_test_clone'
      || psql({ sql: 'select pg_catalog.current_database();' }) !== 'cloudtms_test_clone'
      || psql({ sql: 'select environment from private.cloudtms_database_identity where singleton;' }) !== 'TEST') {
    throw new Error('Wrong protected agency TEST database');
  }
}

function snapshotSql(table) {
  const exporter = fs.readFileSync(path.join(repoRoot, 'supabase/release/export_contract.sql'), 'utf8');
  const query = exporter.slice(exporter.indexOf('\nwith\n')).trim().replace(/::text;\s*$/, ';');
  if (!query.startsWith('with\n') || !query.endsWith(';')) throw new Error('Contract exporter changed');
  return `create temp table ${table}(value) on commit drop as ${query}`;
}

function selectedSql(table) {
  return `(select row from ${table} snapshot cross join lateral
    pg_catalog.jsonb_array_elements(snapshot.value->'routines') row
    where (row->>'schema')||'.'||(row->>'identity')=${quote(key)})`;
}

function outsideSql(table) {
  return `(select pg_catalog.jsonb_set(snapshot.value,'{routines}',
    coalesce((select pg_catalog.jsonb_agg(row order by row->>'schema',row->>'identity')
      from pg_catalog.jsonb_array_elements(snapshot.value->'routines') row
      where (row->>'schema')||'.'||(row->>'identity')<>${quote(key)}),'[]'::jsonb))
    from ${table} snapshot)`;
}

function assertSql(before, after, expectedBefore, expectedAfter) {
  return `do $guard$ begin
    if ${selectedSql(before)} is distinct from ${quote(JSON.stringify(expectedBefore))}::jsonb
      then raise exception 'CANDIDATE_BOOTSTRAP_BEFORE_CONTRACT_MISMATCH'; end if;
    if ${selectedSql(after)} is distinct from ${quote(JSON.stringify(expectedAfter))}::jsonb
      then raise exception 'CANDIDATE_BOOTSTRAP_AFTER_CONTRACT_MISMATCH'; end if;
    if ${outsideSql(before)} is distinct from ${outsideSql(after)}
      then raise exception 'CANDIDATE_BOOTSTRAP_UNRELATED_CATALOGUE_CHANGED'; end if;
  end $guard$;`;
}

function definition() {
  return `${soleDefinition(source)}
alter function private._candidate_record_capabilities_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private._candidate_record_capabilities_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;
grant execute on function private._candidate_record_capabilities_v1(uuid,uuid,jsonb) to postgres;`;
}

function runSql(sql, name) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'cloudtms-candidate-bootstrap-'));
  const targetFile = path.join(directory, `${name}.sql`);
  try {
    fs.writeFileSync(targetFile, mapLogicalPostgresOwnerSql(sql), { flag: 'wx' });
    return psql({ file: targetFile });
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
}

function behaviorSql() {
  // The real TEST root must remain readable even if its optional source
  // policy is absent. This is read-only and contained by the release txn.
  return `do $candidate_bootstrap_behavior$ declare
    v_week public.contract_weeks%rowtype;
    v_capabilities jsonb;
  begin
    select cw.* into v_week from public.contract_weeks cw
    join public.contracts c on c.id=cw.contract_id
    where cw.week_ending_date between '2026-09-01'::date and '2026-10-01'::date
      and cw.week_ending_date-6 <= (pg_catalog.transaction_timestamp() at time zone 'Europe/London')::date
      and cw.additional_seq=0 and not cw.is_adjustment
      and (private._candidate_route_family_v1(cw.timesheet_id,cw.id)->>'import_authoritative')='true'
      and (select count(*) from public.weekly_source_group_clients gc
           join public.weekly_source_groups g on g.id=gc.source_group_id
           where gc.client_id=c.client_id and g.active
             and cw.week_ending_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date))<>1
    order by cw.week_ending_date desc,cw.id limit 1;
    if v_week.id is null then raise exception 'CANDIDATE_BOOTSTRAP_TEST_ROOT_MISSING'; end if;
    v_capabilities:=private._candidate_record_capabilities_v1(
      v_week.timesheet_id,v_week.id,'{}'::jsonb);
    if v_capabilities is null or not (v_capabilities ? 'candidate_source_self_entry_allowed')
      or v_capabilities->>'candidate_source_self_entry_allowed' <> 'false'
      then raise exception 'CANDIDATE_BOOTSTRAP_CAPABILITY_MISSING'; end if;
  end $candidate_bootstrap_behavior$;`;
}

function run(command) {
  const contract = check();
  if (command === 'check') return { component, phase: 'CHECK', routines: 1 };
  if (!['plan', 'rehearse', 'apply'].includes(command)) throw new Error('Expected check|plan|rehearse|apply');
  target();
  const installed = selected(exportContract());
  const alreadyInstalled = JSON.stringify(installed) === JSON.stringify(contract.after);
  if (!alreadyInstalled && JSON.stringify(installed) !== JSON.stringify(contract.before)) {
    throw new Error('Installed TEST Candidate capability is neither pinned predecessor nor successor');
  }
  if (command === 'plan') return { component, phase: 'PLAN', database: 'cloudtms_test_clone', pending: !alreadyInstalled };
  const opening = `\\set ON_ERROR_STOP on
begin;
set local lock_timeout='10s';
set local statement_timeout='120s';
select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0));`;
  const before = snapshotSql('candidate_bootstrap_before');
  const after = snapshotSql('candidate_bootstrap_after');
  const change = `${before}\n${definition()}\n${after}\n${assertSql('candidate_bootstrap_before','candidate_bootstrap_after',installed,contract.after)}\n${behaviorSql()}`;
  if (command === 'rehearse') {
    runSql(`${opening}\n${change}\nrollback;`, 'rehearse');
    if (JSON.stringify(selected(exportContract())) !== JSON.stringify(installed)) {
      throw new Error('Rollback rehearsal changed installed routine');
    }
    return { component, phase: 'REHEARSE', rolledBack: true, alreadyInstalled };
  }
  const commit = shellGitHead();
  if (process.env.CLOUDTMS_RELEASE_APPROVAL !== `APPLY TEST COMPONENT ${component} ${commit}`
      || (process.env.CLOUDTMS_ALLOW_LOCAL !== '1'
        && (process.env.GITHUB_REPOSITORY !== 'kierarthur/cloudtms-backend'
          || process.env.GITHUB_REF !== 'refs/heads/test'))) {
    throw new Error('Exact protected canonical TEST approval missing');
  }
  if (alreadyInstalled) return { component, phase: 'APPLY', alreadyInstalled: true };
  const releaseId = `${component}-${commit.slice(0, 12)}`;
  const hash = sha256(JSON.stringify(contract.after));
  runSql(`${opening}\n${change}\ninsert into private.cloudtms_database_releases(
    release_id,git_commit,repository_contract_sha256,installed_contract_sha256,
    install_mode,status,completed_at_utc,evidence_json)
    values (${quote(releaseId)},${quote(commit)},${quote(hash)},${quote(hash)},
      'UPGRADE','VERIFIED',pg_catalog.clock_timestamp(),
      ${quote(JSON.stringify({ component, scope: 'one Candidate read-projection routine', fullRepeatablesInstalled: false }))}::jsonb);
    select pg_catalog.pg_notify('pgrst','reload schema');\ncommit;`, 'apply');
  if (JSON.stringify(selected(exportContract())) !== JSON.stringify(contract.after)) {
    throw new Error('Installed one-routine contract mismatch after commit');
  }
  return { component, phase: 'APPLY', verified: true, releaseId, routines: 1,
    fullRepeatablesInstalled: false };
}

console.log(JSON.stringify(run(process.argv[2])));
