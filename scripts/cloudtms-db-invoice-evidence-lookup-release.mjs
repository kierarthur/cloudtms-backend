#!/usr/bin/env node
// Exact lookup-plus-ACL TEST component. The prior two invoice definitions are never reinstalled.
import fs from 'node:fs';
import path from 'node:path';
import {
  canonicalSqlBytes, closureFor, mapLogicalPostgresOwnerSql, psql, repoRoot,
  sha256, shellGitHead, validateExpectedDatabase, validateTarget,
} from './cloudtms-db-release-lib.mjs';

const file = 'supabase/repeatable/24092026_1440_weekly_source_invoice_evidence_lookup_v1.sql';
const verifier = 'supabase/verification/24092026_1440_weekly_source_invoice_evidence_lookup_v1.sql';
const aclFile = 'supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql';
const aclVerifier = 'supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql';
const component = 'weekly-source-invoice-evidence-lookup-20260924';
const expectedSource = 'c637b5ca7c0679bf2e09afb626e53ff9aa861e7f9c85bd903ab8b2e56aac2aa4';
const expectedClosure = '2dbce37aa2b80a72ae6bfa4c27952aa45d87ca3c6999c1ff4cccc467aeab2254';
const expectedAclSource = '3b1fcf95f4b2dc787b655224e0731e8994a7a5cb33a779bcb935c0e312b3648d';
const expectedAclClosure = 'c7100c8a358754365085d85da3eb3eae98c92843e0fa14c054fd67b072030db0';
const beforeAclClosure = 'ba6eaf893c96c868cb389279e7e49a9ee0e536af81bc35ab640140eb238a6c2f';
const falseVerifiedRelease = `${component}-0982851fdaa5`;
const quote = value => `'${String(value).replaceAll("'", "''")}'`;
const command = process.argv[2];

function source() {
  if (sha256(canonicalSqlBytes(file)) !== expectedSource
      || closureFor(file).sha256 !== expectedClosure
      || sha256(canonicalSqlBytes(aclFile)) !== expectedAclSource
      || closureFor(aclFile).sha256 !== expectedAclClosure) {
    throw new Error('Exact evidence lookup source hash mismatch');
  }
  const sql = fs.readFileSync(path.join(repoRoot, file), 'utf8');
  const checks = fs.readFileSync(path.join(repoRoot, verifier), 'utf8');
  const aclSql = fs.readFileSync(path.join(repoRoot, aclFile), 'utf8');
  const aclChecks = fs.readFileSync(path.join(repoRoot, aclVerifier), 'utf8');
  if (!/create or replace function public\.weekly_source_invoice_evidence_v1\(p_request jsonb\)/i.test(sql)
      || !/revoke all on function public\.weekly_source_invoice_evidence_v1\(jsonb\) from public,anon,authenticated/i.test(sql)
      || !/grant execute on function public\.weekly_source_invoice_evidence_v1\(jsonb\) to service_role/i.test(sql)
      || !checks.includes('WEEKLY_SOURCE_INVOICE_EVIDENCE_CONTRACT_INVALID')
      || !aclSql.includes("('public.weekly_source_invoice_evidence_v1(jsonb)')")
      || !aclChecks.includes("'public.weekly_source_invoice_evidence_v1(jsonb)'")) {
    throw new Error('Evidence lookup authority or verifier missing');
  }
  return { sql, checks, aclSql, aclChecks };
}

function target() {
  if (process.env.CLOUDTMS_ENVIRONMENT !== 'TEST') throw new Error('TEST only');
  validateTarget('TEST', process.env.CLOUDTMS_EXPECTED_TARGET);
  const expected = validateExpectedDatabase(process.env.CLOUDTMS_EXPECTED_DATABASE);
  if (expected !== 'cloudtms_test_clone' || psql({ sql: 'select current_database();' }) !== expected
      || psql({ sql: 'select environment from private.cloudtms_database_identity where singleton is true;' }) !== 'TEST') {
    throw new Error('Wrong protected TEST database');
  }
}

function ledger(relative) {
  return psql({ sql: `select closure_sha256 from private.cloudtms_repeatable_ledger where path=${quote(relative)};` });
}

function body(text) {
  const withoutMeta = text.replace(/^\s*\\set\s+ON_ERROR_STOP\s+on\s*$/gim, '');
  if ((withoutMeta.match(/^\s*begin;\s*$/gim) || []).length !== 1
      || (withoutMeta.match(/^\s*commit;\s*$/gim) || []).length !== 1) {
    throw new Error('Expected one outer SQL transaction');
  }
  return withoutMeta.replace(/^\s*begin;\s*$/im, '').replace(/^\s*commit;\s*$/im, '');
}

function checks(text, hasRollbackWrapper = false) {
  const withoutMeta = text.replace(/^\s*\\set\s+ON_ERROR_STOP\s+on\s*$/gim, '');
  const begins = (withoutMeta.match(/^\s*begin;\s*$/gim) || []).length;
  const rollbacks = (withoutMeta.match(/^\s*rollback;\s*$/gim) || []).length;
  const commits = (withoutMeta.match(/^\s*commit;\s*$/gim) || []).length;
  if (begins !== Number(hasRollbackWrapper) || rollbacks !== Number(hasRollbackWrapper) || commits !== 0) {
    throw new Error('Unexpected verifier transaction boundary');
  }
  // The release owns one transaction. An embedded verifier ROLLBACK would undo
  // the installed routine while later ledger inserts autocommit as VERIFIED.
  return withoutMeta.replace(/^\s*begin;\s*$/im, '').replace(/^\s*rollback;\s*$/im, '');
}

const { sql, checks: verification, aclSql, aclChecks } = source();
if (command === 'check') {
  console.log(JSON.stringify({ component, source: 'PASS', commit: shellGitHead() }));
} else if (['plan', 'rehearse', 'apply'].includes(command)) {
  target();
  const installed = ledger(file);
  const aclInstalled = ledger(aclFile);
  if (installed && installed !== expectedClosure) throw new Error('Unexpected installed lookup ledger hash');
  if (![beforeAclClosure, expectedAclClosure].includes(aclInstalled)) {
    throw new Error('Unexpected installed Weekly Source ACL closure');
  }
  const oid = psql({ sql: "select to_regprocedure('public.weekly_source_invoice_evidence_v1(jsonb)') is not null;" });
  const repairNeeded = installed === expectedClosure && aclInstalled === expectedAclClosure && oid === 'f'
    && psql({ sql: `select last_release_id from private.cloudtms_repeatable_ledger where path=${quote(file)};` }) === falseVerifiedRelease
    && psql({ sql: `select last_release_id from private.cloudtms_repeatable_ledger where path=${quote(aclFile)};` }) === falseVerifiedRelease
    && psql({ sql: `select status from private.cloudtms_database_releases where release_id=${quote(falseVerifiedRelease)};` }) === 'VERIFIED';
  if ((installed === expectedClosure) !== (oid === 't') && !repairNeeded) {
    throw new Error('Lookup ledger and installed routine disagree');
  }
  if (command === 'plan') {
    console.log(JSON.stringify({ component, phase: 'PLAN', database: 'cloudtms_test_clone',
      pending: !installed || aclInstalled !== expectedAclClosure || repairNeeded, repairNeeded,
      files: [file, aclFile] }));
  } else if (command === 'rehearse') {
    psql({ sql: mapLogicalPostgresOwnerSql(`begin;
      set local lock_timeout='10s'; set local statement_timeout='120s';
      ${body(sql)}
      ${body(aclSql)}
      ${checks(verification)}
      ${checks(aclChecks, true)}
      rollback;`) });
    const after = psql({ sql: "select to_regprocedure('public.weekly_source_invoice_evidence_v1(jsonb)') is not null;" });
    if (after !== oid || ledger(file) !== installed || ledger(aclFile) !== aclInstalled) {
      throw new Error('Rehearsal did not roll back cleanly');
    }
    console.log(JSON.stringify({ component, phase: 'REHEARSE', rolledBack: true }));
  } else {
    const commit = shellGitHead();
    if (process.env.CLOUDTMS_RELEASE_APPROVAL !== `APPLY TEST COMPONENT ${component} ${commit}`) {
      throw new Error('Exact commit-bound TEST approval phrase missing');
    }
    if (installed && aclInstalled === expectedAclClosure && oid === 't') {
      console.log(JSON.stringify({ component, phase: 'APPLY', changed: false, installed }));
      process.exit(0);
    }
    if (!repairNeeded && (installed || aclInstalled !== beforeAclClosure)) {
      throw new Error('Partial evidence/ACL installation requires a fresh audit');
    }
    const releaseId = `${component}-${repairNeeded ? 'repair-' : ''}${commit.slice(0, 12)}`;
    const contractHash = sha256(`${file}\0${expectedClosure}\0${aclFile}\0${expectedAclClosure}`);
    const guard = repairNeeded
      ? `if not exists(select 1 from private.cloudtms_repeatable_ledger where path=${quote(file)}
            and closure_sha256=${quote(expectedClosure)} and last_release_id=${quote(falseVerifiedRelease)})
         or not exists(select 1 from private.cloudtms_repeatable_ledger where path=${quote(aclFile)}
            and closure_sha256=${quote(expectedAclClosure)} and last_release_id=${quote(falseVerifiedRelease)})
         or not exists(select 1 from private.cloudtms_database_releases where release_id=${quote(falseVerifiedRelease)} and status='VERIFIED')
         or pg_catalog.to_regprocedure('public.weekly_source_invoice_evidence_v1(jsonb)') is not null then`
      : `if exists(select 1 from private.cloudtms_repeatable_ledger where path=${quote(file)})
         or not exists(select 1 from private.cloudtms_repeatable_ledger
           where path=${quote(aclFile)} and closure_sha256=${quote(beforeAclClosure)})
         or pg_catalog.to_regprocedure('public.weekly_source_invoice_evidence_v1(jsonb)') is not null then`;
    psql({ sql: mapLogicalPostgresOwnerSql(`begin;
      set local lock_timeout='10s'; set local statement_timeout='120s';
      select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0));
      do $guard$ begin
        ${guard}
          raise exception 'WEEKLY_SOURCE_EVIDENCE_LOOKUP_ALREADY_PRESENT';
        end if;
      end $guard$;
      ${body(sql)}
      ${body(aclSql)}
      ${checks(verification)}
      ${checks(aclChecks, true)}
      do $installed$ begin
        if pg_catalog.to_regprocedure('public.weekly_source_invoice_evidence_v1(jsonb)') is null
           or not pg_catalog.has_function_privilege('service_role','public.weekly_source_invoice_evidence_v1(jsonb)','EXECUTE') then
          raise exception 'WEEKLY_SOURCE_EVIDENCE_LOOKUP_INSTALL_NOT_VISIBLE';
        end if;
      end $installed$;
      ${repairNeeded ? `update private.cloudtms_database_releases
        set status='FAILED', evidence_json=evidence_json || pg_catalog.jsonb_build_object(
          'false_verified_reason','ACL verifier outer ROLLBACK undid installed SQL before ledger autocommit',
          'corrected_by',${quote(releaseId)})
        where release_id=${quote(falseVerifiedRelease)} and status='VERIFIED';` : ''}
      insert into private.cloudtms_database_releases(
        release_id,git_commit,repository_contract_sha256,installed_contract_sha256,
        install_mode,status,completed_at_utc,evidence_json
      ) values (${quote(releaseId)},${quote(commit)},${quote(contractHash)},${quote(contractHash)},
        'UPGRADE','VERIFIED',pg_catalog.clock_timestamp(),
        pg_catalog.jsonb_build_object('contract','CLOUDTMS_TEST_COMPONENT_RELEASE_V1',
          'scope','invoice evidence lookup and Weekly Source service allowlist',
          'repairs_false_verified_release',${repairNeeded ? quote(falseVerifiedRelease) : 'null'}));
      ${repairNeeded ? `update private.cloudtms_repeatable_ledger
        set last_release_id=${quote(releaseId)},applied_at_utc=pg_catalog.clock_timestamp()
        where path=${quote(file)} and closure_sha256=${quote(expectedClosure)}
          and last_release_id=${quote(falseVerifiedRelease)};`
      : `insert into private.cloudtms_repeatable_ledger(path,closure_sha256,last_release_id)
        values (${quote(file)},${quote(expectedClosure)},${quote(releaseId)});`}
      update private.cloudtms_repeatable_ledger
        set closure_sha256=${quote(expectedAclClosure)},last_release_id=${quote(releaseId)},
            applied_at_utc=pg_catalog.clock_timestamp()
        where path=${quote(aclFile)} and closure_sha256=${quote(repairNeeded ? expectedAclClosure : beforeAclClosure)}
          ${repairNeeded ? `and last_release_id=${quote(falseVerifiedRelease)}` : ''};
      notify pgrst, 'reload schema';
      commit;`) });
    if (ledger(file) !== expectedClosure || ledger(aclFile) !== expectedAclClosure
        || psql({ sql: "select to_regprocedure('public.weekly_source_invoice_evidence_v1(jsonb)') is not null;" }) !== 't'
        || psql({ sql: `select status from private.cloudtms_database_releases where release_id=${quote(releaseId)};` }) !== 'VERIFIED') {
      throw new Error('Installed lookup/ACL/ledger did not match exact source after commit');
    }
    console.log(JSON.stringify({ component, phase: 'APPLY', changed: true, releaseId, commit,
      closure: expectedClosure, aclClosure: expectedAclClosure, repairedFalseVerification: repairNeeded,
      excludedBankingPayStage2: true }));
  }
} else {
  throw new Error('Usage: cloudtms-db-invoice-evidence-lookup-release.mjs check|plan|rehearse|apply');
}
