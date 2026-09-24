#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  canonicalSqlBytes, closureFor, sha256, shellGitHead, psql, repoRoot,
  validateTarget, validateExpectedDatabase, exportContract,
} from './cloudtms-db-release-lib.mjs';

export const manifestPath = 'supabase/release/candidate-expense-carrier-component.json';
export const manifest = JSON.parse(fs.readFileSync(path.join(repoRoot, manifestPath), 'utf8'));
const quote = value => "'" + String(value).replaceAll("'", "''") + "'";
const read = file => canonicalSqlBytes(file).toString('utf8');
const identity = row => row.schema + '.' + row.identity;

export function unwrap(text, mode) {
  const value = text.replace(/^\s*\\set\s+ON_ERROR_STOP\s+on\s*$/gim, '');
  const count = word => (value.match(new RegExp('^\\s*' + word + ';\\s*$', 'gim')) || []).length;
  const wrapped = count('begin');
  if (wrapped > 1 || count('commit') !== (mode === 'source' ? 1 : 0)
      || count('rollback') !== (mode === 'source' ? 0 : wrapped)
      || (mode === 'source' && wrapped !== 1) || /^\s*\\/m.test(value)) {
    throw new Error('Unexpected component transaction/metacommand boundary');
  }
  return value.replace(/^\s*(?:begin|commit|rollback);\s*$/gim, '');
}

export function checkSource() {
  if (manifest.componentId !== 'candidate-expense-carrier-repair-20260924'
      || manifest.environment !== 'TEST' || manifest.database !== 'cloudtms_test_clone'
      || manifest.beforeRoutines.length !== 7 || manifest.afterRoutines.length !== 10) {
    throw new Error('Component identity/count mismatch');
  }
  for (const file of [manifest.source, ...manifest.verifiers]) {
    if (sha256(canonicalSqlBytes(file.path)) !== file.sha256) {
      throw new Error('Component source hash mismatch: ' + file.path);
    }
  }
  if (closureFor(manifest.source.path).sha256 !== manifest.source.closureSha256) throw new Error('Closure mismatch');
  const sql = read(manifest.source.path);
  if (/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i.test(sql)) throw new Error('Invalid conditional expression');
  unwrap(sql, 'source');
  manifest.verifiers.forEach(file => unwrap(read(file.path), 'verifier'));
  const contract = JSON.parse(read('supabase/release/current-contract.json'));
  for (const expected of manifest.afterRoutines) {
    if (JSON.stringify(contract.routines.find(row => identity(row) === identity(expected)))
        !== JSON.stringify(expected)) throw new Error('Generated component contract mismatch');
  }
  if (new Set(manifest.afterRoutines.map(identity)).size !== 10) throw new Error('Duplicate authority');
  return true;
}

// Full provider-neutral catalogue in the SAME transaction. Unrelated tables,
// functions, triggers, policies and ACLs must remain unchanged.
export function snapshotSql(table) {
  const exporter = read('supabase/release/export_contract.sql');
  const query = exporter.slice(exporter.indexOf('\nwith\n')).trim().replace(/::text;\s*$/, ';');
  if (!query.startsWith('with\n') || !query.endsWith(';')) throw new Error('Unexpected exporter');
  return 'set local search_path=pg_catalog,public;\ncreate temp table ' + table
    + '(value) on commit drop as ' + query;
}

function assertRoutines(table, expected) {
  const keys = manifest.afterRoutines.map(identity);
  return 'do $component_guard$ declare actual jsonb; begin '
    + 'select coalesce(jsonb_agg(row order by row->>\'schema\',row->>\'identity\'),\'[]\'::jsonb) into actual '
    + 'from ' + table + ' snapshot cross join lateral jsonb_array_elements(snapshot.value->\'routines\') row '
    + 'where (row->>\'schema\')||\'.\'||(row->>\'identity\') in (' + keys.map(quote).join(',') + '); '
    + 'if actual is distinct from ' + quote(JSON.stringify(expected)) + '::jsonb then '
    + "raise exception 'EXPENSE_COMPONENT_ROUTINE_CONTRACT_MISMATCH'; end if; end $component_guard$;";
}

function outsideContract(table) {
  const keys = manifest.afterRoutines.map(identity).map(quote).join(',');
  return "(select jsonb_set(snapshot.value,'{routines}',coalesce((select jsonb_agg(row order by row->>'schema',row->>'identity')"
    + " from jsonb_array_elements(snapshot.value->'routines') row where (row->>'schema')||'.'||(row->>'identity') not in ("
    + keys + ")),'[]'::jsonb)) from " + table + " snapshot)";
}

export function verificationSql() {
  // Roll back test fixtures only. Never let an embedded ROLLBACK undo DDL.
  return manifest.verifiers.map((file, i) => 'savepoint component_check_' + i + ';\n'
    + unwrap(read(file.path), 'verifier') + '\nrollback to savepoint component_check_' + i
    + ';\nrelease savepoint component_check_' + i + ';').join('\n');
}

export function installationSql({ installed = false, repair = false } = {}) {
  return snapshotSql('expense_component_before') + '\n'
    + assertRoutines('expense_component_before', installed ? manifest.afterRoutines : manifest.beforeRoutines) + '\n'
    + unwrap(read(manifest.source.path), 'source') + '\n' + verificationSql() + '\n'
    + snapshotSql('expense_component_after') + '\n'
    + assertRoutines('expense_component_after', manifest.afterRoutines) + '\n'
    + 'do $outside$ begin if ' + outsideContract('expense_component_before') + ' is distinct from '
    + outsideContract('expense_component_after')
    + " then raise exception 'EXPENSE_COMPONENT_UNRELATED_CATALOGUE_CHANGE'; end if; end $outside$;\n"
    + (repair ? cleanupSql() : '');
}

export function cleanupSql() {
  // Only the three inspected terminal, unmaterialised TEST reservations.
  // Ordinary removal authority preserves workflow and cancellation history.
  return `do $repair$
declare target uuid; result jsonb; contract_before jsonb;
begin
  select to_jsonb(c) into contract_before from public.contracts c
  where c.id='42b04c70-d2a2-4477-b4e9-437c0d16bf61' for update;
  if contract_before is null then raise exception 'EXPENSE_REPAIR_CONTRACT_MISSING'; end if;
  foreach target in array array[
    '3f48e4c7-d758-4547-9a34-b9ed001eac85'::uuid,
    '84389b4b-a5d4-4b73-9e19-997956a55819'::uuid,
    'efb96e8a-88be-43d4-ae2d-578b4e1a8e81'::uuid
  ] loop
    if exists(select 1 from public.contract_weeks where id=target) then
      if not exists(select 1 from public.contract_weeks where id=target
        and contract_id='42b04c70-d2a2-4477-b4e9-437c0d16bf61'
        and week_ending_date='2026-09-13' and timesheet_id is null)
      then raise exception 'EXPENSE_REPAIR_TARGET_CHANGED'; end if;
      result:=private._candidate_empty_provisional_expense_cleanup_v1('TEST',target);
      if result->>'deleted' is distinct from 'true' then raise exception 'EXPENSE_REPAIR_NOT_ELIGIBLE'; end if;
    elsif not exists(select 1 from public.candidate_submission_workflows
      where contract_id='42b04c70-d2a2-4477-b4e9-437c0d16bf61' and contract_week_id is null
        and input_snapshot_json#>>'{office_permanent_delete_tombstone,previous_contract_week_id}'=target::text)
    then raise exception 'EXPENSE_REPAIR_HISTORY_MISSING'; end if;
  end loop;
  if contract_before is distinct from (select to_jsonb(c) from public.contracts c
    where c.id='42b04c70-d2a2-4477-b4e9-437c0d16bf61')
    or not exists(select 1 from public.contract_weeks where id='ed198010-f333-414d-b1e9-e527fcdbfd20'
      and timesheet_id='96ae7689-7698-48bf-8af3-9e5eedec8300')
  then raise exception 'EXPENSE_REPAIR_ROOT_OR_CONTRACT_CHANGED'; end if;
end $repair$;`;
}

function target() {
  if (process.env.CLOUDTMS_ENVIRONMENT !== 'TEST') throw new Error('TEST only');
  validateTarget('TEST', process.env.CLOUDTMS_EXPECTED_TARGET);
  if (validateExpectedDatabase(process.env.CLOUDTMS_EXPECTED_DATABASE) !== manifest.database
      || psql({ sql: 'select current_database();' }) !== manifest.database
      || psql({ sql: 'select environment from private.cloudtms_database_identity where singleton;' }) !== 'TEST') {
    throw new Error('Wrong protected TEST database');
  }
}

function verifyInstalledReadOnly(expected) {
  const keys = new Set(manifest.afterRoutines.map(identity));
  const actual = exportContract().routines.filter(row => keys.has(identity(row)));
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error('Installed component routine contract mismatch');
  }
}

export function releaseLedgerSql(commit) {
  const releaseId = manifest.componentId + '-' + commit.slice(0, 12);
  const contractHash = sha256(JSON.stringify(manifest.afterRoutines));
  // The repeatable row has a non-deferrable FK to its release parent.
  // Both writes remain inside the installation transaction.
  return 'insert into private.cloudtms_database_releases(release_id,git_commit,repository_contract_sha256,installed_contract_sha256,install_mode,status,completed_at_utc,evidence_json) values ('
    + [releaseId, commit, contractHash, contractHash, 'UPGRADE', 'VERIFIED'].map(quote).join(',')
    + ",clock_timestamp(),jsonb_build_object('contract','CLOUDTMS_TEST_COMPONENT_RELEASE_V1','scope','provisional expense carriers only','banking_pay_stage2_closed',false));\n"
    + 'insert into private.cloudtms_repeatable_ledger(path,closure_sha256,last_release_id) values ('
    + [manifest.source.path, manifest.source.closureSha256, releaseId].map(quote).join(',') + ');\n';
}

export function run(command) {
  checkSource();
  if (command === 'check') return { component: manifest.componentId, source: 'PASS' };
  if (!['plan', 'rehearse', 'apply'].includes(command)) throw new Error('Expected check|plan|rehearse|apply');
  target();
  const ledgerSql = 'select closure_sha256 from private.cloudtms_repeatable_ledger where path=' + quote(manifest.source.path) + ';';
  const ledger = psql({ sql: ledgerSql });
  if (ledger && ledger !== manifest.source.closureSha256) throw new Error('Unexpected component ledger');
  const installed = !!ledger;
  const opening = "begin;\nset local lock_timeout='5s'; set local statement_timeout='120s';\n"
    + "select pg_advisory_xact_lock(hashtextextended('cloudtms_database_release_admission_v1',0));\n";
  if (command === 'plan') {
    verifyInstalledReadOnly(installed ? manifest.afterRoutines : manifest.beforeRoutines);
    return { component: manifest.componentId, phase: 'PLAN', pending: !installed, database: manifest.database };
  }
  if (command === 'rehearse') {
    psql({ sql: opening + installationSql({ installed, repair: true })
      + (installed ? '' : releaseLedgerSql(shellGitHead())) + '\nrollback;' });
    if (psql({ sql: ledgerSql }) !== ledger) throw new Error('Rehearsal changed ledger');
    return { component: manifest.componentId, phase: 'REHEARSE', rolledBack: true };
  }
  const commit = shellGitHead();
  if (process.env.CLOUDTMS_RELEASE_APPROVAL !== 'APPLY TEST COMPONENT ' + manifest.componentId + ' ' + commit) {
    throw new Error('Missing exact commit-bound TEST approval');
  }
  const releaseId = manifest.componentId + '-' + commit.slice(0, 12);
  psql({ sql: opening
    + "do $ledger$ begin if coalesce((select closure_sha256 from private.cloudtms_repeatable_ledger where path="
    + quote(manifest.source.path) + "),'')<>" + quote(ledger) + " then raise exception 'EXPENSE_COMPONENT_LEDGER_CHANGED'; end if; end $ledger$;\n"
    + installationSql({ installed, repair: true }) + '\n'
    + (installed ? '' : releaseLedgerSql(commit))
    + "notify pgrst,'reload schema';\ncommit;" });
  // Installation identity proof only; user-journey testing is a later step.
  verifyInstalledReadOnly(manifest.afterRoutines);
  if (psql({ sql: ledgerSql }) !== manifest.source.closureSha256) throw new Error('Component ledger not installed');
  return { component: manifest.componentId, phase: 'APPLY', installed: true, commit, releaseId };
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  console.log(JSON.stringify(run(process.argv[2])));
}
