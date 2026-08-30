import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import {
  canonicalSqlBytes, closureFor, deadlockRetryCountForFile, inventory,
  formatPlanSection, legacyUpgradeInventory,
  mapGeneratedAclBaselineSql, mapLogicalPostgresOwnerSql,
  readJson, repoRoot, sha256, sqlDateKey,
  validateTarget, verifyIntegrity,
} from '../scripts/cloudtms-db-release-lib.mjs';

const read = relative => fs.readFileSync(path.join(repoRoot, relative), 'utf8');

test('date ordering distinguishes valid ISO from UK filenames', () => {
  assert.equal(sqlDateKey('20260218_smoke_once_only.sql'), '20260218_smoke_once_only.sql'.replace(/^20260218/, '20260218'));
  assert.equal(sqlDateKey('18022026_0951_example.sql'), '20260218_0951_example.sql');
  assert.ok(sqlDateKey('20261340_invalid.sql').startsWith('ZZZZZZZZ_'));
  assert.ok(sqlDateKey('32132026_invalid.sql').startsWith('ZZZZZZZZ_'));
});

test('inventory covers uppercase legacy SQL and recursive repeatable closures', () => {
  const current = inventory();
  assert.ok(current.migrations.some(x => x.path.endsWith('13072026_1648_MIGRATION.SQL')));
  assert.ok(current.migrations.some(x => x.path.endsWith('26052026_1525_INDEX_OUTBOX.SQL')));
  const withIncludes = current.repeatables.find(x => x.paths.length > 1);
  assert.ok(withIncludes, 'expected a repeatable with included support pages');
  assert.equal(closureFor(withIncludes.path).sha256, withIncludes.sha256);
});

test('migration identity is stable across Windows checkout line endings', () => {
  const relative = 'supabase/migrations/20260218_smoke_once_only.sql';
  const lock = readJson('supabase/release/migration-lock.json').migrations.find(item => item.path === relative);
  assert.ok(lock);
  assert.equal(sha256(canonicalSqlBytes(relative)), lock.sha256);
});

test('deadlock retries are limited to an immutable transaction-safe migration', () => {
  const providerOwnerMigration =
    'supabase/migrations/24082026_0232_miget_provider_owner_defaults.sql';
  assert.equal(deadlockRetryCountForFile(providerOwnerMigration), 3);
  assert.equal(deadlockRetryCountForFile('supabase/migrations/20260218_smoke_once_only.sql'), 0);
  assert.match(read(providerOwnerMigration), /begin;[\s\S]*\\ir \.\.\/baseline\/[\s\S]*commit;/);
});

test('migration immutability and protected Candidate boundary pass', () => {
  const current = verifyIntegrity();
  const lock = readJson('supabase/release/migration-lock.json');
  assert.equal(current.migrations.length, lock.migrations.length);
  assert.equal(readJson('supabase/release/baseline-repeatable-lock.json').repeatables.length, 298);
  assert.equal(readJson('supabase/release/protected-boundary-lock.json').files.length, 5);
});

test('migration locking is append-only and cannot bless an edited historical migration', () => {
  const engine = read('scripts/cloudtms-db-release.mjs');
  const bible = read('docs/DATABASE_RELEASE_BIBLE.md');
  assert.match(engine, /Refusing to relock changed migration/);
  assert.match(engine, /Locked migration missing/);
  assert.doesNotMatch(engine, /if \(kind === 'migration'\) writeMigrationLock\(\)/);
  assert.match(bible, /only appends new migration hashes/);
});

test('normal push workflow cannot mutate a database', () => {
  const workflow = read('.github/workflows/supabase-migrate.yml');
  assert.match(workflow, /Database source verification \(no deploy\)/);
  assert.doesNotMatch(workflow, /SUPABASE_DB_URL|CLOUDTMS_DATABASE_URL|psql\s+"?\$|db:apply/);
  assert.doesNotMatch(workflow, /marking existing migrations as applied|__BOOTSTRAPPED__/);
});

test('manual release is dispatch-only, environment-protected, and two phase', () => {
  const workflow = read('.github/workflows/database-release.yml');
  assert.match(workflow, /workflow_dispatch:/);
  assert.doesNotMatch(workflow, /\npush:/);
  assert.match(workflow, /database-live/);
  assert.match(workflow, /database-test/);
  assert.match(workflow, /secrets\.MIGET_DATABASE_URL_TEST/);
  assert.match(workflow, /vars\.MIGET_DATABASE_TARGET_TEST/);
  assert.match(workflow, /secrets\.MIGET_DATABASE_URL_LIVE/);
  assert.match(workflow, /vars\.MIGET_DATABASE_TARGET_LIVE/);
  assert.doesNotMatch(workflow, /secrets\.SUPABASE_DB_URL_TEST/);
  assert.doesNotMatch(workflow, /inputs\.environment == 'LIVE'[\s\S]{0,200}secrets\.(?:CLOUDTMS_DATABASE_URL|SUPABASE_DB_URL)/);
  assert.match(workflow, /Read-only release plan/);
  assert.match(workflow, /if: inputs\.phase == 'APPLY'/);
  assert.match(workflow, /APPLY \$\{\{ inputs\.environment \}\} \$\{\{ inputs\.mode \}\}/);
  assert.match(workflow, /inputs\.environment.*TEST.*inputs\.mode.*UPGRADE/s);
  assert.match(workflow, /standing managed TEST UPGRADE authority/i);
  assert.match(workflow, /GITHUB_REPOSITORY.*kierarthur\/cloudtms-backend/);
  assert.match(workflow, /GITHUB_REF.*refs\/heads\/test/);
  assert.match(workflow, /CLOUDTMS_RELEASE_APPROVAL=\$expected/);
  assert.match(workflow, /CLOUDTMS_RELEASE_REQUESTED_APPROVAL/);
  assert.doesNotMatch(workflow, /CLOUDTMS_RELEASE_APPROVAL:\s*\$\{\{ inputs\.approval \}\}/);
  assert.match(workflow, /CLOUDTMS_LOGICAL_POSTGRES_OWNER:\s*CURRENT_USER/);
  assert.match(workflow, /LEGACY_UPGRADE/);
});

test('standing database authority is limited to managed TEST UPGRADE', () => {
  const workflow = read('.github/workflows/database-release.yml');
  const authorityStart = workflow.indexOf('Using standing managed TEST UPGRADE authority');
  assert.notEqual(authorityStart, -1);
  const authorityBlock = workflow.slice(Math.max(0, authorityStart - 900), authorityStart + 500);
  assert.match(authorityBlock, /inputs\.environment.*TEST/s);
  assert.match(authorityBlock, /inputs\.mode.*UPGRADE/s);
  assert.match(authorityBlock, /GITHUB_REPOSITORY/);
  assert.match(authorityBlock, /GITHUB_REF/);
  assert.doesNotMatch(authorityBlock, /LIVE.*standing managed TEST UPGRADE/s);
  assert.match(workflow, /else[\s\S]*CLOUDTMS_RELEASE_REQUESTED_APPROVAL[\s\S]*Exact APPLY approval phrase/);
});

test('one-time LIVE provider clone is protected, source-read-only and destination-blank-only', () => {
  const workflow = read('.github/workflows/live-miget-clone.yml');
  assert.match(workflow, /workflow_dispatch:/);
  assert.match(workflow, /environment:\s*database-live/);
  assert.match(workflow, /secrets\.MIGET_DATABASE_URL_LIVE/);
  assert.match(workflow, /secrets\.CLOUDTMS_DATABASE_URL \|\| secrets\.SUPABASE_DB_URL/);
  assert.match(workflow, /Miget LIVE destination is not blank/);
  assert.match(workflow, /pg_dump[^\n]*--no-owner[^\n]*--no-privileges[^\n]*--schema=public[^\n]*--schema=maintenance/);
  assert.match(workflow, /sed -i -E[^\n]*SCHEMA - public/);
  assert.match(workflow, /pg_restore[^\n]*--no-owner[^\n]*--no-privileges[^\n]*--exit-on-error[^\n]*--use-list/);
  assert.match(workflow, /Exact table row-count verification failed/);
  assert.match(workflow, /Exact sequence-state verification failed/);
  assert.doesNotMatch(workflow, /service_role[^\n]*bypassrls|alter role service_role bypassrls/i);
  assert.match(workflow, /create role postgres nologin noinherit/);
  assert.match(workflow, /create role supabase_admin nologin noinherit/);
  assert.doesNotMatch(workflow, /create role (?:postgres|supabase_admin)[^\n]*(?:^|[^a-z])login\b/im);
  assert.match(workflow, /where e\.extname in \('pgcrypto', 'uuid-ossp'\)/);
  assert.doesNotMatch(workflow, /grant execute on all functions in schema extensions/i);
  assert.match(workflow, /docker run --rm[\s\S]*sha256sum \/work\/live\.dump/);
  assert.doesNotMatch(workflow, /chmod[^\n]*live\.dump/i);
  assert.doesNotMatch(workflow, /actions\/upload-artifact/);
});

test('provider database owner mapping is explicit, bounded and fail closed', () => {
  const previous = process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
  try {
    process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = 'CURRENT_USER';
    assert.equal(
      mapLogicalPostgresOwnerSql('alter function public.example() owner to postgres;'),
      'alter function public.example() OWNER TO CURRENT_USER;',
    );
    assert.equal(
      mapLogicalPostgresOwnerSql('alter function public.example() owner to "postgres";'),
      'alter function public.example() OWNER TO CURRENT_USER;',
    );
    assert.equal(
      mapLogicalPostgresOwnerSql('alter default privileges for role "postgres" in schema public grant execute on functions to service_role;'),
      'ALTER DEFAULT PRIVILEGES FOR ROLE CURRENT_USER in schema public grant execute on functions to service_role;',
    );
    assert.equal(
      mapGeneratedAclBaselineSql('grant execute on function public.example() to "postgres";'),
      'grant execute on function public.example() to CURRENT_USER;',
    );
    assert.equal(
      mapGeneratedAclBaselineSql('revoke all on function private.example() from PUBLIC, authenticator, supabase_admin;'),
      'revoke all on function private.example() from PUBLIC, supabase_admin;',
    );
    assert.equal(
      mapLogicalPostgresOwnerSql("SET plpgsql_check.mode TO 'disabled'\nSET search_path = ''\nselect 1;"),
      "SET search_path = ''\nselect 1;",
    );
    assert.equal(
      mapLogicalPostgresOwnerSql("SET \"plpgsql_check.mode\" = 'disabled'\r\nSET lock_timeout = '5s';\r\n"),
      "SET lock_timeout = '5s';\r\n",
    );
    process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = 'UNSAFE_ROLE';
    assert.throws(() => mapLogicalPostgresOwnerSql('select 1;'), /must be CURRENT_USER/);
  } finally {
    if (previous === undefined) delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
    else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER = previous;
  }
});

test('hosted target validation accepts a provider-neutral database locator and fails closed', () => {
  const previousUrl = process.env.CLOUDTMS_DATABASE_URL;
  try {
    process.env.CLOUDTMS_DATABASE_URL = 'postgresql://automation:secret@postgres.example/cloudtms_test_clone?sslmode=require';
    assert.doesNotThrow(() => validateTarget('TEST', 'cloudtms_test_clone'));
    assert.throws(() => validateTarget('TEST', 'another_database'), /CLOUDTMS_EXPECTED_TARGET/);
    assert.throws(() => validateTarget('TEST', ''), /CLOUDTMS_EXPECTED_TARGET/);
  } finally {
    if (previousUrl === undefined) delete process.env.CLOUDTMS_DATABASE_URL;
    else process.env.CLOUDTMS_DATABASE_URL = previousUrl;
  }
});

test('release engine has fail-closed NEW, ADOPT, UPGRADE, and one-time legacy upgrade gates', () => {
  const source = read('scripts/cloudtms-db-release.mjs');
  assert.match(source, /NEW requires an empty application schema/);
  assert.match(source, /mode === 'ADOPT'/);
  assert.match(source, /Installed migration hash mismatch/);
  assert.match(source, /Database contract differs in/);
  assert.match(source, /LEGACY_UPGRADE is restricted to LIVE/);
  assert.match(source, /refuses a database already carrying managed identity/);
  assert.match(source, /adoptLegacyInventoryAtomically/);
  assert.match(source, /release\.legacyUpgradeBootstrapFiles/);
  assert.match(source, /assertLegacyTransitionShimsReplaced\(\)/);
  assert.match(source, /CLOUDTMS_LEGACY_TRANSITION_SHIM/);
  assert.doesNotMatch(source, /marking existing migrations/);
  assert.match(source, /mode === 'NEW'[\s\S]*controlPlaneIndex[\s\S]*postBaselineMigrations[\s\S]*for \(const item of postBaselineMigrations\) psql\(\{ file: item\.path \}\)[\s\S]*baselineRepeatableLock[\s\S]*pendingRepeatables[\s\S]*runBankingPayCatalogPreapply[\s\S]*for \(const item of pendingRepeatables\) psql\(\{ file: item\.path \}\)[\s\S]*recordInventory/);
  assert.match(source, /mode === 'NEW'[\s\S]*release\.newVerificationFiles/);
  assert.match(source, /mode === 'LEGACY_UPGRADE'[\s\S]*legacyUpgradeExcludedVerificationFiles/);
  const release = readJson('supabase/release/current-release.json');
  assert.ok(release.verificationFiles.some(file => file.includes('banking_pay_james_rate_authority_runtime_verification')));
  assert.ok(!release.newVerificationFiles.some(file => file.includes('banking_pay_james_rate_authority_runtime_verification')));
  assert.deepEqual(release.legacyUpgradeExcludedVerificationFiles, [
    'tests/13082026_1942_banking_pay_james_rate_authority_runtime_verification.sql',
  ]);
  assert.deepEqual(
    release.verificationFiles.filter(file => !release.legacyUpgradeExcludedVerificationFiles.includes(file)),
    release.newVerificationFiles,
  );
  assert.match(
    read(release.legacyUpgradeExcludedVerificationFiles[0]),
    /Run against TEST after the four James runtime repeatables are installed/i,
  );
  assert.ok(release.newVerificationFiles.includes('supabase/verification/26082026_0044_candidate_manager_authoriser_policy_v2_verification.sql'));
});

test('legacy transition bootstrap is bounded and must be replaced before adoption', () => {
  const release = readJson('supabase/release/current-release.json');
  assert.deepEqual(release.legacyUpgradeBootstrapFiles, [
    'supabase/release/30082026_0030_miget_auth_compatibility_bootstrap.sql',
    'supabase/release/30082026_0205_miget_vault_compatibility_bootstrap.sql',
    'supabase/repeatable/08042026_1151_newtablesbanking.sql',
    'supabase/release/30082026_0055_legacy_workbench_shared_context_bootstrap.sql',
    'supabase/release/30082026_0120_legacy_structural_baseline_gap_bootstrap.sql',
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_trg_invoice_document_invalidate.sql',
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_trg_timesheet_document_invalidate.sql',
    'supabase/release/30082026_0145_legacy_invoice_candidate_scope_authority.sql',
    'supabase/repeatable/25072026_2153_banking_pay_selection_carry_runtime.sql',
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_document_advance_batch_v6_downstream.sql',
    'supabase/repeatable/24072026_1217_invoice_async_processor_contract_v4/24072026_1217_private_invoice_document_advance_batch.sql',
    'supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql',
    'supabase/repeatable/04082026_1302_pay_workbench_session_replay_replaced_queue_v1.sql',
    'supabase/release/24082026_1128_legacy_upgrade_trigger_shims.sql',
  ]);
  assert.equal(release.baselineFiles[0], release.legacyUpgradeBootstrapFiles[0]);
  assert.equal(release.baselineFiles[1], release.legacyUpgradeBootstrapFiles[1]);
  assert.deepEqual(release.legacyUpgradeFinalizeFiles, [
    'supabase/release/30082026_0130_legacy_structural_baseline_gap_finalize.sql',
  ]);
  assert.deepEqual(release.legacyUpgradeRepeatablePreloadFiles, [
    'supabase/repeatable/04082026_1147_pay_current_timesheet_entitlement_components_from_build_v1.sql',
    'supabase/repeatable/20072026_1220_allow_bounded_cleanup_of_local_review.sql',
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_source_reference_validate_batch.sql',
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_generation_resolve_command_groups.sql',
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_generation_vat_policy_batch.sql',
    'supabase/repeatable/21072026_1235_10_invoice_correction_pair_scope_v1.sql',
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_correction_validate_batch.sql',
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_reference_rows_batch.sql',
    'supabase/release/30082026_0350_legacy_candidate_feature_current_preload.sql',
    'supabase/repeatable/30082026_0345_invoice_delivery_routes_batch_v1.sql',
    'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_issue_validate_batch.sql',
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1806_private_invoice_batch_generate_classification_v2.sql',
    'supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1806_private_invoice_batch_issue_classification_v2.sql',
    'supabase/release/30082026_0321_legacy_pay_batch_status_helper_preload.sql',
  ]);
  const catalogPreload = read(release.legacyUpgradeRepeatablePreloadFiles[0]);
  assert.match(catalogPreload, /create or replace function private\.pay_current_timesheet_entitlement_components_from_build_v1\(/i);
  assert.match(catalogPreload, /from private\.banking_pay_workbench_economic_build_facts/i);
  const cleanupBasePreload = read(release.legacyUpgradeRepeatablePreloadFiles[1]);
  assert.match(cleanupBasePreload, /create or replace function public\._pay_execute_operation_cleanup_failed_local_artifacts_base\(/i);
  assert.match(cleanupBasePreload, /revoke all on function public\._pay_execute_operation_cleanup_failed_local_artifacts_base/i);
  for (const invoiceHelperIndex of [2, 3, 4, 6, 7, 9, 10, 11, 12]) {
    const invoiceHelper = read(release.legacyUpgradeRepeatablePreloadFiles[invoiceHelperIndex]);
    assert.match(invoiceHelper, /create (?:or replace )?function private\._invoice_/i);
    assert.doesNotMatch(invoiceHelper, /\b(?:insert into|update|delete from|truncate)\b/i);
  }
  const correctionPairScopePreload = read(release.legacyUpgradeRepeatablePreloadFiles[5]);
  assert.match(correctionPairScopePreload, /create or replace function public\.invoice_correction_pair_scope_v1\(/i);
  assert.doesNotMatch(correctionPairScopePreload, /\b(?:insert into|delete from|truncate)\b/i);
  assert.doesNotMatch(correctionPairScopePreload, /^\s*update\s+(?:public|private)\./im);
  const candidateFeaturePreload = read(release.legacyUpgradeRepeatablePreloadFiles[8]);
  const candidateFeatureOwner = read('supabase/repeatable/07082026_2059_candidate_app_private_helpers_v1.sql');
  assert.match(candidateFeaturePreload, /create or replace function private\._candidate_feature_enabled_current_v1\(/i);
  assert.ok(candidateFeatureOwner.includes(candidateFeaturePreload.match(/create or replace function[\s\S]*?\$function\$;/i)[0]));
  assert.match(candidateFeaturePreload, /revoke all on function private\._candidate_feature_enabled_current_v1\(text\) from public, anon, authenticated, service_role/i);
  const statusHelperPreload = read(release.legacyUpgradeRepeatablePreloadFiles[13]);
  const statusHelperBaseline = read('supabase/baseline/22082026_1503_cloudtms_test_routines_01.sql');
  const statusHelperStart = statusHelperBaseline.indexOf('-- _pay_batch_status_is_active_reservation(text)');
  const statusHelperEnd = statusHelperBaseline.indexOf('-- _pay_batch_validate_freshness_base_v1(uuid,uuid,boolean)');
  assert.ok(statusHelperStart >= 0 && statusHelperEnd > statusHelperStart);
  assert.ok(statusHelperPreload.includes(
    statusHelperBaseline.slice(statusHelperStart, statusHelperEnd).trim(),
  ));
  assert.match(statusHelperPreload, /revoke all on function public\._pay_batch_status_is_active_reservation\(text\) from public, anon, authenticated, service_role/i);
  assert.match(statusHelperPreload, /grant execute on function public\._pay_batch_status_is_active_reservation\(text\) to service_role/i);
  const freshnessChunkOwner = read('supabase/repeatable/30082026_0342_pay_batch_validate_freshness_chunk_v1.sql');
  assert.match(freshnessChunkOwner, /revoke all privileges on function public\.pay_batch_validate_freshness_chunk\(uuid, uuid, uuid, uuid, integer\) from PUBLIC, anon, authenticated, service_role, authenticator, supabase_admin/i);
  assert.doesNotMatch(freshnessChunkOwner, /grant execute on function public\.pay_batch_validate_freshness_chunk\([^;]+\) to service_role/i);
  const importReviewUiContract = read('supabase/repeatable/22072026_0052_import_review_ui_contract_v1.sql');
  assert.match(importReviewUiContract, /to_regprocedure\('public\.import_review_contract_version_get_v1\(\)'\) is not null/i);
  assert.match(importReviewUiContract, /execute 'revoke all on function public\.import_review_contract_version_get_v1\(\) from public,anon,authenticated'/i);
  assert.match(importReviewUiContract, /execute 'grant execute on function public\.import_review_contract_version_get_v1\(\) to service_role'/i);
  assert.deepEqual(release.legacyUpgradeDeferredRepeatableFiles, [
    'supabase/repeatable/23072026_1217_disable_plpgsql_check_for_correction_chain_banking.sql',
  ]);
  const deferredPlpgsqlGuard = read(release.legacyUpgradeDeferredRepeatableFiles[0]);
  assert.equal((deferredPlpgsqlGuard.match(/::regprocedure/g) || []).length, 48);
  assert.match(deferredPlpgsqlGuard, /plpgsql_check\.fatal_errors TO %L/);
  assert.deepEqual(release.legacyUpgradePostRepeatableFiles, [
    'supabase/release/30082026_0236_legacy_general_browser_view_isolation_replacement.sql',
  ]);
  const postRepeatableViewIsolation = read(release.legacyUpgradePostRepeatableFiles[0]);
  assert.match(postRepeatableViewIsolation, /alter view %I\.%I set \(security_invoker=true\)/i);
  assert.match(postRepeatableViewIsolation, /revoke all privileges on table %I\.%I from PUBLIC, anon, authenticated/i);
  assert.match(postRepeatableViewIsolation, /f7b3b9ccf07dd052c65b98932af9a76c/);
  assert.deepEqual(release.legacyUpgradeReplacementRepeatables, {
    'supabase/repeatable/22082026_1402_candidate_named_security_definer_browser_isolation.sql':
      'supabase/release/30082026_0341_legacy_candidate_named_security_replacement.sql',
  });
  const candidateRpcReplacement = read(
    release.legacyUpgradeReplacementRepeatables[
      'supabase/repeatable/22082026_1402_candidate_named_security_definer_browser_isolation.sql'
    ],
  );
  assert.match(candidateRpcReplacement, /9a750d0555772cb2902c02ec73d56711/);
  assert.match(candidateRpcReplacement, /1358ce0e91782fffa75f9199067f6bdd/);
  assert.match(candidateRpcReplacement, /d74699ad8c6938055b2d83e883feeee9/);
  assert.match(candidateRpcReplacement, /revoke all privileges on function %s from PUBLIC, anon, authenticated/);
  assert.doesNotMatch(candidateRpcReplacement, /\bgrant\s+/i);
  assert.doesNotMatch(
    candidateRpcReplacement,
    /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i,
  );
  assert.doesNotMatch(candidateRpcReplacement, /\b(?:insert into|update|delete from|truncate|create function|replace function)\b/i);
  assert.deepEqual(release.legacyUpgradeMigrationTransitions, {
    'supabase/migrations/06082026_0440_banking_pay_workbench_terminal_reconciliation_window.sql': [
      'supabase/release/30082026_0214_legacy_workbench_terminal_profile_prepare.sql',
    ],
    'supabase/migrations/06082026_0754_banking_pay_workbench_four_lane_32_burst.sql': [
      'supabase/release/30082026_0215_legacy_workbench_four_lane_profile_prepare.sql',
    ],
  });
  assert.deepEqual(release.legacyUpgradeNonApplicableMigrations, {
    'supabase/migrations/08082026_1137_repair_banking_pay_scope_generation_gap.sql':
      'supabase/release/30082026_0218_legacy_scope_generation_gap_nonapplicability.sql',
  });
  const scopeGapNonApplicability = read(
    release.legacyUpgradeNonApplicableMigrations[
      'supabase/migrations/08082026_1137_repair_banking_pay_scope_generation_gap.sql'
    ],
  );
  assert.match(scopeGapNonApplicability, /8d441fff-4153-413c-a522-72b6903a754f/);
  assert.match(scopeGapNonApplicability, /bfdc14ec-82a6-566c-b6d5-bf760ecaf030/);
  assert.match(scopeGapNonApplicability, /TEST_FIXTURE_PRESENT_IN_LIVE/);
  assert.doesNotMatch(scopeGapNonApplicability, /update\s+|delete\s+|insert\s+|truncate\s+|drop\s+/i);
  assert.deepEqual(release.legacyUpgradeReplacementMigrations, {
    'supabase/migrations/22082026_1302_general_browser_relation_sequence_isolation.sql':
      'supabase/release/30082026_0222_legacy_general_browser_isolation_replacement.sql',
    'supabase/migrations/22082026_1302_general_browser_rpc_isolation.sql':
      'supabase/release/30082026_0233_legacy_general_browser_rpc_isolation_replacement.sql',
    'supabase/migrations/22082026_1302_general_browser_view_isolation.sql':
      'supabase/release/30082026_0236_legacy_general_browser_view_isolation_replacement.sql',
    'supabase/migrations/22082026_1402_candidate_named_legacy_relation_isolation.sql':
      'supabase/release/30082026_0239_legacy_candidate_named_relation_isolation_replacement.sql',
    'supabase/migrations/24082026_0232_miget_provider_owner_defaults.sql':
      'supabase/release/30082026_0255_legacy_miget_provider_owner_defaults_replacement.sql',
  });
  const generalIsolationReplacement = read(
    release.legacyUpgradeReplacementMigrations[
      'supabase/migrations/22082026_1302_general_browser_relation_sequence_isolation.sql'
    ],
  );
  assert.match(generalIsolationReplacement, /a4713d3e744b0e2fa6c82a317948ab69/);
  assert.match(generalIsolationReplacement, /581acf649985f7457facbd1f9c1bda9f/);
  assert.match(generalIsolationReplacement, /v_count<>126/);
  assert.match(generalIsolationReplacement, /v_count<>8/);
  assert.match(generalIsolationReplacement, /enable row level security/);
  assert.match(generalIsolationReplacement, /revoke all privileges on table/);
  assert.match(generalIsolationReplacement, /revoke all privileges on sequence/);
  assert.match(generalIsolationReplacement, /LEGACY_GENERAL_SEQUENCE_ISOLATION_FAILED/);
  assert.match(generalIsolationReplacement, /has_table_privilege\('anon',c\.oid,'TRUNCATE'\)/);
  assert.match(generalIsolationReplacement, /has_table_privilege\('authenticated',c\.oid,'TRIGGER'\)/);
  assert.match(generalIsolationReplacement, /alter default privileges for role current_user/);
  assert.match(generalIsolationReplacement, /v_service_hash_after<>v_service_hash_before/);
  assert.doesNotMatch(generalIsolationReplacement, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  const generalRpcIsolationReplacement = read(
    release.legacyUpgradeReplacementMigrations[
      'supabase/migrations/22082026_1302_general_browser_rpc_isolation.sql'
    ],
  );
  assert.match(generalRpcIsolationReplacement, /a0b546b38607f66e1afaec987f15ede7/);
  assert.match(generalRpcIsolationReplacement, /v_count<>353/);
  assert.match(generalRpcIsolationReplacement, /type_namespace\.nspname\|\|'\.'\|\|argument_type\.typname/);
  assert.match(generalRpcIsolationReplacement, /revoke all privileges on function/);
  assert.match(generalRpcIsolationReplacement, /grant execute on function %s to service_role/);
  assert.match(generalRpcIsolationReplacement, /LEGACY_GENERAL_RPC_SERVICE_ACL_CHANGED/);
  assert.match(generalRpcIsolationReplacement, /LEGACY_GENERAL_RPC_BROWSER_EXECUTE_REMAINS/);
  assert.doesNotMatch(generalRpcIsolationReplacement, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  const generalViewIsolationReplacement = read(
    release.legacyUpgradeReplacementMigrations[
      'supabase/migrations/22082026_1302_general_browser_view_isolation.sql'
    ],
  );
  assert.match(generalViewIsolationReplacement, /2140eb627004e18241adb94c085b57bf/);
  assert.match(generalViewIsolationReplacement, /f7b3b9ccf07dd052c65b98932af9a76c/);
  assert.match(generalViewIsolationReplacement, /alter view %I\.%I set \(security_invoker=true\)/);
  assert.match(generalViewIsolationReplacement, /grant select on table %I\.%I to service_role/);
  assert.match(generalViewIsolationReplacement, /revoke all privileges on table/);
  assert.doesNotMatch(generalViewIsolationReplacement, /create\s+(?:or\s+replace\s+)?view/i);
  assert.doesNotMatch(generalViewIsolationReplacement, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  const candidateRelationIsolationReplacement = read(
    release.legacyUpgradeReplacementMigrations[
      'supabase/migrations/22082026_1402_candidate_named_legacy_relation_isolation.sql'
    ],
  );
  assert.match(candidateRelationIsolationReplacement, /56579812da56cf68a033dc225936531f/);
  assert.match(candidateRelationIsolationReplacement, /e3517fd0dc706d587a2c7275f370bca5/);
  assert.match(candidateRelationIsolationReplacement, /LEGACY_CANDIDATE_NAMED_TABLE_SERVICE_ACL_CHANGED/);
  assert.match(candidateRelationIsolationReplacement, /alter table %I\.%I enable row level security/);
  assert.match(candidateRelationIsolationReplacement, /alter view %I\.%I set \(security_invoker=true\)/);
  assert.match(candidateRelationIsolationReplacement, /from PUBLIC, anon, authenticated/);
  assert.doesNotMatch(candidateRelationIsolationReplacement, /\b(?:insert into|update public\.|delete from|drop table|truncate table)\b/i);
  assert.doesNotMatch(candidateRelationIsolationReplacement, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  const providerOwnerDefaultsReplacement = read(
    release.legacyUpgradeReplacementMigrations[
      'supabase/migrations/24082026_0232_miget_provider_owner_defaults.sql'
    ],
  );
  assert.match(providerOwnerDefaultsReplacement, /grant all privileges on all tables in schema public to service_role/);
  assert.match(providerOwnerDefaultsReplacement, /grant all privileges on all sequences in schema public to service_role/);
  assert.match(providerOwnerDefaultsReplacement, /candidate_manager_email_route_receipts/);
  assert.match(providerOwnerDefaultsReplacement, /import_review_events_id_seq/);
  assert.match(providerOwnerDefaultsReplacement, /revoke all privileges on all functions in schema private/);
  assert.match(providerOwnerDefaultsReplacement, /cloudtms_data_api_mfa_gate/);
  assert.match(providerOwnerDefaultsReplacement, /alter default privileges for role current_user/);
  assert.doesNotMatch(providerOwnerDefaultsReplacement, /22082026_1505_cloudtms_test_acl_baseline/);
  assert.doesNotMatch(providerOwnerDefaultsReplacement, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  const terminalProfilePrepare = read(release.legacyUpgradeMigrationTransitions[
    'supabase/migrations/06082026_0440_banking_pay_workbench_terminal_reconciliation_window.sql'
  ][0]);
  assert.match(terminalProfilePrepare, /cron_source_build_parallelism = 2/);
  assert.match(terminalProfilePrepare, /nudge_source_build_parallelism = 4/);
  assert.match(terminalProfilePrepare, /set banking_pay_workbench_cron_source_build_parallelism = 0/);
  assert.match(terminalProfilePrepare, /v_updated <> 1/);
  const fourLaneProfilePrepare = read(release.legacyUpgradeMigrationTransitions[
    'supabase/migrations/06082026_0754_banking_pay_workbench_four_lane_32_burst.sql'
  ][0]);
  assert.match(fourLaneProfilePrepare, /cron_source_build_parallelism = 0/);
  assert.match(fourLaneProfilePrepare, /set banking_pay_workbench_cron_source_build_parallelism = 1/);
  assert.match(fourLaneProfilePrepare, /cron_source_build_parallel_bursts = 32/);
  assert.match(fourLaneProfilePrepare, /cron_source_build_runtime_floor_ms = 45000/);
  assert.doesNotMatch(`${terminalProfilePrepare}\n${fourLaneProfilePrepare}`, /delete\s+from|insert\s+into|truncate|drop\s+/i);
  const authBootstrap = read(release.legacyUpgradeBootstrapFiles[0]);
  assert.match(authBootstrap, /create schema if not exists auth/);
  assert.match(authBootstrap, /create table if not exists auth\.users/);
  assert.match(authBootstrap, /create or replace function auth\.uid\(\)/);
  assert.match(authBootstrap, /create or replace function auth\.role\(\)/);
  assert.match(authBootstrap, /create or replace function auth\.jwt\(\)/);
  assert.match(authBootstrap, /current_setting\('request\.jwt\.claim\.sub', true\)/);
  assert.match(authBootstrap, /grant usage on schema auth to anon, authenticated, service_role/);
  assert.doesNotMatch(authBootstrap, /insert\s+into\s+auth\.users|update\s+auth\.users|delete\s+from\s+auth\.users/i);
  const vaultBootstrap = read(release.legacyUpgradeBootstrapFiles[1]);
  assert.match(vaultBootstrap, /VAULT_COMPATIBILITY_PARTIAL_AUTHORITY_REFUSED/);
  assert.match(vaultBootstrap, /extensions\.pgp_sym_encrypt/);
  assert.match(vaultBootstrap, /create view vault\.decrypted_secrets/i);
  assert.match(vaultBootstrap, /revoke all on schema vault/);
  assert.match(vaultBootstrap, /revoke all on table vault\._cloudtms_key_material, vault\.secrets, vault\.decrypted_secrets/);
  assert.doesNotMatch(vaultBootstrap, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(vaultBootstrap, /raise notice|select\s+.*decrypted_secret/i);
  assert.ok(release.verificationFiles.includes(
    'supabase/verification/30082026_0220_miget_vault_compatibility_verification.sql',
  ));
  assert.ok(release.newVerificationFiles.includes(
    'supabase/verification/30082026_0220_miget_vault_compatibility_verification.sql',
  ));
  const schemaBootstrap = read(release.legacyUpgradeBootstrapFiles[2]);
  assert.match(schemaBootstrap, /CREATE TABLE IF NOT EXISTS public\.banking_pay_workbench_sessions/);
  assert.match(schemaBootstrap, /Safe to rerun/);
  const sharedContextBootstrap = read(release.legacyUpgradeBootstrapFiles[3]);
  assert.match(sharedContextBootstrap, /CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_workbench_sessions_shared_context_open/);
  assert.match(sharedContextBootstrap, /GROUP BY session_signature, pay_date, week_ending_cutoff/);
  assert.match(sharedContextBootstrap, /HAVING count\(\*\) > 1/);
  assert.match(sharedContextBootstrap, /unexpected definition; refusing bootstrap/);
  assert.doesNotMatch(sharedContextBootstrap, /DROP\s+INDEX|INSERT\s+INTO|UPDATE\s+public\.|DELETE\s+FROM|TRUNCATE/i);
  const structuralGapBootstrap = read(release.legacyUpgradeBootstrapFiles[4]);
  assert.match(structuralGapBootstrap, /create table if not exists public\.import_apply_operations/);
  assert.match(structuralGapBootstrap, /create table if not exists public\.pay_manual_adjustment_carry_forwards/);
  assert.match(structuralGapBootstrap, /create table if not exists public\.schema_repeatables/);
  assert.match(structuralGapBootstrap, /create sequence public\.migration_smoke_once_only_id_seq/);
  assert.match(structuralGapBootstrap, /create table public\.migration_smoke_once_only/);
  assert.match(structuralGapBootstrap, /migration_smoke_once_only_pkey primary key/);
  assert.match(structuralGapBootstrap, /never its two[\s\S]*historical TEST rows/i);
  assert.doesNotMatch(structuralGapBootstrap, /insert\s+into\s+public\.migration_smoke_once_only/i);
  assert.match(structuralGapBootstrap, /add column if not exists temp_log boolean not null default false/);
  assert.match(structuralGapBootstrap, /settings_defaults\.temp_log definition mismatch/);
  assert.match(structuralGapBootstrap, /enable row level security/);
  assert.match(structuralGapBootstrap, /revoke all privileges[^;]+from public, anon, authenticated, service_role/);
  assert.doesNotMatch(structuralGapBootstrap, /insert\s+into|update\s+public\.|delete\s+from|truncate/i);
  const structuralGapFinalize = read(release.legacyUpgradeFinalizeFiles[0]);
  assert.match(structuralGapFinalize, /pay_manual_adjustment_carry_f_source_correction_request_id_fkey/);
  assert.match(structuralGapFinalize, /pay_manual_adjustment_carry_f_source_correction_work_item__fkey/);
  assert.match(structuralGapFinalize, /pay_manual_adjustment_carry_forwards_source_request_fkey/);
  assert.match(structuralGapFinalize, /pay_manual_adjustment_carry_forwards_source_work_item_fkey/);
  assert.match(structuralGapFinalize, /if not exists/);
  assert.doesNotMatch(structuralGapFinalize, /insert\s+into|update\s+public\.|delete\s+from|truncate/i);
  const invoiceInvalidationBootstrap = read(release.legacyUpgradeBootstrapFiles[5]);
  const timesheetInvalidationBootstrap = read(release.legacyUpgradeBootstrapFiles[6]);
  assert.match(invoiceInvalidationBootstrap, /create or replace function public\.trg_invoice_document_invalidate\(\)/i);
  assert.match(timesheetInvalidationBootstrap, /create or replace function public\.trg_timesheet_document_invalidate\(\)/i);
  const invoiceCandidateScopeBootstrap = read(release.legacyUpgradeBootstrapFiles[7]);
  const structuralRoutineBaseline = read('supabase/baseline/22082026_1503_cloudtms_test_routines_15.sql');
  const exactPrivateStart = structuralRoutineBaseline.indexOf('CREATE OR REPLACE FUNCTION private._invoice_batch_generate_candidates_legacy_20260726(');
  const exactPrivateEnd = structuralRoutineBaseline.indexOf('-- private._invoice_batch_generate_classification_v2(boolean,text[],timestamp with time zone)');
  assert.ok(exactPrivateStart >= 0 && exactPrivateEnd > exactPrivateStart);
  assert.ok(invoiceCandidateScopeBootstrap.includes(
    structuralRoutineBaseline.slice(exactPrivateStart, exactPrivateEnd).trim(),
  ));
  assert.match(invoiceCandidateScopeBootstrap, /create or replace function public\.invoice_batch_generate_candidates\(/i);
  assert.match(invoiceCandidateScopeBootstrap, /drop function if exists public\.invoice_batch_generate_candidates\(boolean, integer\);/i);
  assert.doesNotMatch(invoiceCandidateScopeBootstrap, /drop function[^;]+cascade/i);
  assert.match(invoiceCandidateScopeBootstrap, /CLOUDTMS_LEGACY_TRANSITION_SHIM/);
  assert.match(invoiceCandidateScopeBootstrap, /LEGACY_UPGRADE_IN_PROGRESS/);
  const selectionCarryBootstrap = read(release.legacyUpgradeBootstrapFiles[8]);
  assert.match(selectionCarryBootstrap, /create or replace function public\.trg_banking_pay_preview_selection_carry_apply\(\)/i);
  const documentAdvanceDownstreamBootstrap = read(release.legacyUpgradeBootstrapFiles[9]);
  const documentAdvanceBootstrap = read(release.legacyUpgradeBootstrapFiles[10]);
  assert.match(documentAdvanceDownstreamBootstrap, /create or replace function private\._invoice_document_advance_batch_v6_downstream\(/i);
  assert.match(documentAdvanceBootstrap, /create or replace function private\._invoice_document_advance_batch\(/i);
  const failJobBootstrap = read(release.legacyUpgradeBootstrapFiles[11]);
  assert.match(failJobBootstrap, /create or replace function public\.pay_workbench_fail_job\(/i);
  assert.match(failJobBootstrap, /revoke all on function public\.pay_workbench_fail_job/i);
  assert.match(failJobBootstrap, /grant execute on function public\.pay_workbench_fail_job/i);
  const queueReplayBootstrap = read(release.legacyUpgradeBootstrapFiles[12]);
  assert.match(queueReplayBootstrap, /create or replace function public\.pay_workbench_session_replay_replaced_queue_v1\(/i);
  assert.match(queueReplayBootstrap, /revoke all on function public\.pay_workbench_session_replay_replaced_queue_v1/i);
  assert.match(queueReplayBootstrap, /grant execute on function public\.pay_workbench_session_replay_replaced_queue_v1/i);
  const bootstrap = read(release.legacyUpgradeBootstrapFiles[13]);
  assert.match(bootstrap, /to_regprocedure/);
  assert.match(bootstrap, /CLOUDTMS_LEGACY_TRANSITION_SHIM/);
  assert.match(bootstrap, /return OLD/);
  assert.match(bootstrap, /return NEW/);
  assert.match(bootstrap, /timesheet_archive_transition_capability/);
  assert.match(bootstrap, /timesheet_archive_row_guard_v1/);
  assert.match(bootstrap, /timesheet_archived_evidence_guard_v1/);
  assert.match(bootstrap, /invoice_line_archived_timesheet_guard_v1/);
  assert.match(bootstrap, /timesheet_financial_retention_capture_trigger_v1/);
  assert.match(bootstrap, /pay_workbench_case_resolution_origin_backfill_v1/);
  assert.match(bootstrap, /pay_workbench_case_resolution_origin_guard_v1/);
  assert.match(bootstrap, /LEGACY_UPGRADE_REQUIRES_CANONICAL_ORIGIN_BACKFILL/);
  assert.match(bootstrap, /trg_bpay_wb_case_resolution_origin_guard/);
  assert.match(bootstrap, /pay_payment_cancel_not_sent_and_recalculate/);
  assert.match(bootstrap, /'scope_type', v_scope_type,[\s\S]*'pay_batch_id', p_pay_batch_id::text/);
  assert.match(bootstrap, /timesheet_financial_retention_mark_v1/);
  assert.match(bootstrap, /timesheet_archive_transition_v1/);
  assert.match(bootstrap, /timesheet_r2_cleanup_claim_v1/);
  assert.match(bootstrap, /timesheet_r2_cleanup_record_v1/);
  assert.match(bootstrap, /timesheet_r2_cleanup_complete_v1/);
  assert.match(bootstrap, /create function public\._pay_timesheet_rotation_scope\(p_timesheet_ids uuid\[\]\)/i);
  assert.match(bootstrap, /create function public\._pay_active_settled_components\(p_timesheet_ids uuid\[\]\)/i);
  assert.match(bootstrap, /where false \/\* CLOUDTMS_LEGACY_TRANSITION_SHIM \*\//);
  assert.match(bootstrap, /LEGACY_UPGRADE_IN_PROGRESS/);
  assert.match(bootstrap, /revoke all on function/);
  assert.doesNotMatch(bootstrap, /insert\s+into|update\s+public\.|delete\s+from|truncate/i);
  const engine = read('scripts/cloudtms-db-release.mjs');
  assert.match(engine, /applyLegacyMigrationWithTransition/);
  assert.match(engine, /applyLegacyNonApplicableMigration/);
  assert.match(engine, /applyLegacyReplacementMigration/);
  assert.match(engine, /begin;[\s\S]*insert into public\.schema_migrations\(filename\)[\s\S]*commit;/);
  assert.match(engine, /LEGACY TRANSITION FILES/);
  assert.match(engine, /LEGACY NON-APPLICABLE MIGRATIONS/);
  assert.match(engine, /LEGACY REPLACEMENT FILES/);
  for (const shimName of [
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
    '_pay_active_settled_components',
  ]) {
    assert.match(engine, new RegExp(`'${shimName}'`));
  }
});

test('legacy upgrade inventory accepts only an exact historical subset and one bootstrap marker', () => {
  const current = {
    migrations: [
      { path: 'supabase/migrations/01012026_first.sql', sha256: 'a'.repeat(64) },
      { path: 'supabase/migrations/02012026_second.sql', sha256: 'b'.repeat(64) },
    ],
    repeatables: [{ path: 'supabase/repeatable/current.sql', sha256: 'c'.repeat(64) }],
  };
  const planned = legacyUpgradeInventory(current, ['__BOOTSTRAPPED__', '01012026_first.sql']);
  assert.equal(planned.installedCount, 1);
  assert.deepEqual(planned.pendingMigrations.map(item => item.path), [
    'supabase/migrations/02012026_second.sql',
  ]);
  assert.equal(planned.pendingRepeatables.length, 1);
  assert.throws(
    () => legacyUpgradeInventory(current, ['__BOOTSTRAPPED__', 'unknown.sql']),
    /absent from repository/,
  );
  assert.throws(
    () => legacyUpgradeInventory(current, ['01012026_first.sql']),
    /exactly one __BOOTSTRAPPED__/,
  );
  assert.throws(
    () => legacyUpgradeInventory(current, ['__BOOTSTRAPPED__', '01012026_first.sql', '01012026_first.sql']),
    /duplicate/,
  );
});

test('read-only release plans render every exact pending authority path and hash', () => {
  assert.equal(formatPlanSection('PENDING MIGRATIONS', []), 'PENDING MIGRATIONS: none');
  assert.equal(
    formatPlanSection('PENDING MIGRATIONS', [
      { path: 'supabase/migrations/24082026_example.sql', sha256: 'a'.repeat(64) },
    ]),
    `PENDING MIGRATIONS:\n- supabase/migrations/24082026_example.sql sha256=${'a'.repeat(64)}`,
  );
  assert.equal(
    formatPlanSection('PENDING/CHANGED REPEATABLES', [
      { path: 'supabase/repeatable/example.sql', sha256: 'b'.repeat(64) },
    ], 'closure_sha256'),
    `PENDING/CHANGED REPEATABLES:\n- supabase/repeatable/example.sql closure_sha256=${'b'.repeat(64)}`,
  );

  const engine = read('scripts/cloudtms-db-release.mjs');
  assert.match(engine, /mode === 'UPGRADE'[\s\S]*assertUpgradeLedger\(current\)/);
  assert.match(engine, /mode === 'UPGRADE'[\s\S]*PENDING MIGRATIONS/);
  assert.match(engine, /mode === 'UPGRADE'[\s\S]*PENDING\/CHANGED REPEATABLES/);
});

test('contract export normalises null ACLs to one-dimensional effective defaults', () => {
  const source = read('supabase/release/export_contract.sql');
  assert.doesNotMatch(source, /aclexplode\(coalesce\([^)]*,\s*'\{\}'::aclitem\[\]\)\)/s);
  assert.match(source, /acldefault\([\s\S]*c\.relowner/);
  assert.match(source, /acldefault\('f'::"char", p\.proowner\)/);
  assert.match(source, /acldefault\('n'::"char", n\.nspowner\)/);
  assert.equal((source.match(/select distinct\s+case when a\.grantee = 0 then 'PUBLIC'/gi) || []).length, 4);
  assert.equal((source.match(/\) expanded_acl/g) || []).length, 4);
  assert.match(source, /when rolname=current_user then 'postgres'/);
  assert.match(source, /owner_role\.logical_name = 'postgres'/);
});

test('contract export is provider and upgrade-history neutral without weakening security fields', () => {
  const source = read('supabase/release/export_contract.sql');
  assert.doesNotMatch(source, /'position',\s*a\.attnum/);
  assert.match(source, /order by a\.attname collate "C"/);
  assert.match(source, /config_value !~ '\^plpgsql_check\[\.\]'/);
  assert.match(source, /regexp_replace\([\s\S]*plpgsql_check\\\\\./);
  assert.match(source, /p\.proname = 'cloudtms_data_api_mfa_gate'/);
  assert.match(source, /expanded_acl\.grantee = 'authenticator'/);
  assert.match(source, /'security_definer', p\.prosecdef/);
  assert.match(source, /'definition_sha256'/);
});

test('private Candidate Daily Miget policies are exact, reproducible and grant no table privilege', () => {
  const source = read('supabase/migrations/26082026_0312_private_daily_service_rls_reconciliation.sql');
  const tables = [
    'candidate_daily_authority_scopes',
    'candidate_daily_authority_transitions',
    'candidate_daily_batch_receipts',
    'candidate_daily_entitlements',
    'candidate_daily_external_effect_receipts',
    'candidate_daily_source_links',
    'candidate_daily_sync_state',
  ];
  for (const table of tables) assert.match(source, new RegExp(`'${table}'`));
  assert.match(source, /cloudtms_miget_service_owner_all/);
  assert.match(source, /for all to %I, service_role using \(true\) with check \(true\)/i);
  assert.doesNotMatch(source, /grant\s+(?:select|insert|update|delete|all)\s+on\s+table/i);
  assert.doesNotMatch(source, /\b(?:insert|update|delete|truncate)\s+(?:into|from|table)\s+private\.candidate_daily_/i);
});

test('Bible preserves Policy X and protected security boundary', () => {
  const bible = read('docs/DATABASE_RELEASE_BIBLE.md');
  assert.match(bible, /post-draft uses frozen batch artifacts only/);
  assert.match(bible, /protected-boundary-lock\.json/);
  assert.match(bible, /There is no blind baselining/);
  assert.match(read('AGENTS.md'), /Mandatory database release process/);
});
