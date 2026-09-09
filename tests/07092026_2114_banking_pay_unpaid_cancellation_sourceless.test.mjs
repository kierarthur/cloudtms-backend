import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  assertAlertCompletenessSafety,
  assertStatusAdmissionSafety,
  assertFinalActivationCutoverSafety,
  assertNoMoneyResultObjectAritySafety,
  assertPlanCommunicationSafety,
  extractFunction,
  materialise,
  patchAlerts,
  patchApply,
  patchDiagnostic,
  patchExpandWork,
  patchIntegrityCheck,
  patchPlan,
  patchSelectionPrepare,
  patchStatusAdmission,
} from '../scripts/generate-banking-pay-unpaid-cancellation-sourceless-v1.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = relative => fs.readFileSync(path.join(root, relative), 'utf8').replaceAll('\r\n', '\n');
const bytes = relative => fs.readFileSync(path.join(root, relative));
const hash = value => crypto.createHash('sha256').update(value).digest('hex');
const count = (source, needle) => source.split(needle).length - 1;

const paths = {
  monolith: 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql',
  preBank: 'supabase/repeatable/04092026_2118_banking_pay_multi_candidate_cancel_continuation_v1.sql',
  noMoney: 'supabase/repeatable/04082026_1158_pay_no_money_unwind_apply_work_item.sql',
  prepare: 'supabase/repeatable/05092026_0405_banking_pay_one_candidate_cancellation_scope_integrity_v1.sql',
  expand: 'supabase/repeatable/04082026_1208_pay_payment_correction_expand_work.sql',
  integrity: 'supabase/repeatable/04082026_1148_pay_payment_correction_integrity_check_v1.sql',
  statusPage: 'supabase/repeatable/04082026_1146_pay_batch_payment_status_page_v1.sql',
  admissionOut: 'supabase/repeatable/07092026_2136_banking_pay_unpaid_cancellation_sourceless_admission_v1.sql',
  applyOut: 'supabase/repeatable/07092026_1932_banking_pay_unpaid_cancellation_sourceless_apply_v1.sql',
  alertOut: 'supabase/repeatable/07092026_1933_banking_pay_unpaid_cancellation_investigation_alert_v1.sql',
  prepareOut: 'supabase/repeatable/07092026_2013_banking_pay_unpaid_cancellation_communication_v2_prepare_v1.sql',
  expandOut: 'supabase/repeatable/07092026_2014_banking_pay_unpaid_cancellation_communication_v2_expand_v1.sql',
  integrityOut: 'supabase/repeatable/07092026_2021_banking_pay_payment_correction_integrity_communication_v2_v1.sql',
  statusAdmissionOut: 'supabase/repeatable/07092026_2135_banking_pay_unpaid_cancellation_status_admission_v1.sql',
  cutoverRepair: 'supabase/migrations/07092026_1931_banking_pay_legacy_terminal_correction_cutover_repair_v1.sql',
  legacyBlockedCutover: 'supabase/migrations/07092026_1933_banking_pay_legacy_applied_with_blockers_cutover_v1.sql',
  migration: 'supabase/migrations/07092026_1932_banking_pay_unpaid_cancellation_sourceless_alert_index_v1.sql',
  verifier: 'supabase/verification/07092026_2115_banking_pay_unpaid_cancellation_sourceless_verification.sql',
};

function assertLegacyCutoverRepairSafety(source) {
  assert.equal(count(source, 'DO $legacy_terminal_correction_cutover_repair$'), 1);
  assert.match(source, /set local statement_timeout = '6000ms';/);
  assert.match(source, /set local lock_timeout = '1000ms';/);
  assert.match(source, /v_environment IS DISTINCT FROM 'TEST'[\s\S]+RETURN;/);
  assert.equal(count(source, "request_row.requested_at_utc < '2026-08-15 00:00:00+00'::timestamptz"), 2);
  assert.equal(count(source, "request_row.correction_kind = 'PRE_BANK_CANCEL'"), 2);
  assert.equal(count(source, "request_row.status = 'PROCESSING'"), 2);
  assert.match(source, /work_row\.status NOT IN \(\s*'APPLIED', 'SKIPPED', 'BLOCKED', 'FAILED_FINAL', 'CANCELLED'\s*\)/);
  assert.equal(count(source, "operation_row.status = 'REVIEW_REQUIRED'"), 2);
  assert.equal(count(source, "operation_row.phase = 'FINALISE'"), 2);
  assert.match(source, /v_eligible_request_count IS DISTINCT FROM v_active_request_count/);
  assert.match(source, /v_eligible_request_count > 8/);
  assert.match(source, /private\.pay_payment_mutation_guard_v1\([\s\S]+?'CORRECTION_APPLY'/);
  assert.match(source, /public\.pay_payment_correction_process_chunk\([\s\S]+?v_target\.request_id,[\s\S]+?100,[\s\S]+?v_worker_id/);
  assert.match(source, /v_result->>'code' IS DISTINCT FROM 'PAYMENT_CORRECTION_FINALISED'/);
  assert.match(source, /'LEGACY_WORKBENCH_REFRESH_REQUIRES_CURRENT_AUTHORITY'/);
  assert.match(source, /PAYMENT_CORRECTION_LEGACY_CUTOVER_NOT_CLEAN/);
  assert.doesNotMatch(source, /\b(DELETE|TRUNCATE|MERGE)\b/i);
  assert.doesNotMatch(source, /UPDATE\s+public\.(pay_batches|pay_batch_items|pay_bank_transfers|pay_bank_transfer_events)/i);
  assert.doesNotMatch(source, /provider|mail_outbox|remittance/i);
}

const applyConfig = {
  label: 'pre-bank apply',
  correctionKind: 'PRE_BANK_CANCEL',
  selectedTableStart: '  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_selected;',
  capacityMailStart: '  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_capacity_mail_scope;',
  capacityMailEnd: '  PERFORM 1\n  FROM public.pay_batch_items AS locked_instruction_items',
  applyMailStart: '  DROP TABLE IF EXISTS pg_temp._tmp_pre_bank_cancel_mail_scope_matches;',
  applyMailEnd: '  INSERT INTO public.app_change_counters(entity_key, seq, updated_at)',
  manualBlockCondition: "  IF jsonb_array_length(COALESCE(v_manual_adjustment_result->'carry_forward_blockers', '[]'::jsonb)) > 0\n     OR COALESCE((v_manual_adjustment_result->>'can_carry_forward_automatically')::boolean, true) IS NOT TRUE THEN",
  resultFieldCount: 1,
  resultFieldCommaCount: 0,
  resultFieldNeedsTrailingComma: false,
  communicationResultCount: 1,
  membershipCommunicationCheckCount: 1,
};

test('generator is deterministic and historical owners remain byte-identical', () => {
  const expectedHashes = {
    [paths.monolith]: '8b3cb3e112ae227a80bf2e661272264c3d6145d0e2ff6ad8d88e8eee2db1553f',
    [paths.preBank]: '4a40fc4911382946dc5ba26d25d5963e552f01e9cada7280b2b4e69db427e43b',
    [paths.noMoney]: '2934fff3ee503cbf4a8336d449944bcf6c125fb7de87c3a7cd88aafe2b73b8cc',
    [paths.prepare]: '47d79ef8f50a6a313e2ac20d0ff9b6e4d6ec5ee6c99a7d0be376724564b10634',
    [paths.expand]: '7d944938263dd27a77bc24632a48e2ff9283e01a004a74a3337e790711f2ad51',
    [paths.integrity]: '4c99dfb5c62225bd72cc92dd1fb5162668e581c58a5424f47e1b9392189fa7e4',
    [paths.statusPage]: 'c0f38a0555482ad78021c33ecc398bbaf835f0ae30b3dce9bb396e33c74a7bee',
  };
  for (const [file, expected] of Object.entries(expectedHashes)) {
    assert.equal(hash(bytes(file)), expected, file);
  }

  const artifacts = materialise();
  assert.equal(read(paths.admissionOut), artifacts.admission);
  assert.equal(read(paths.applyOut), artifacts.apply);
  assert.equal(read(paths.alertOut), artifacts.alert);
  assert.equal(read(paths.prepareOut), artifacts.communicationPrepare);
  assert.equal(read(paths.expandOut), artifacts.communicationExpand);
  assert.equal(read(paths.integrityOut), artifacts.integrityCheck);
  assert.equal(read(paths.statusAdmissionOut), artifacts.statusAdmission);
});

test('status page makes only safe source-less ambiguity advisory and retains every payment fence', () => {
  const statusAdmission = read(paths.statusAdmissionOut);
  const definition = extractFunction(statusAdmission, 'public.pay_batch_payment_status_page_v1');
  assertStatusAdmissionSafety(definition);
  assert.equal(count(definition, 'AS source_less_manual_adjustment_advisory'), 1);
  assert.equal(count(definition, 'OR candidate_provider_precedence_index.source_less_manual_adjustment_advisory'), 2);
  assert.match(definition, /canonical_provider_state = 'NO_TRANSFER_EVIDENCE'[\s\S]+provider_request_sent IS NOT TRUE[\s\S]+provider_external_id_present IS NOT TRUE/);
  assert.match(definition, /canonical_provider_state = 'TERMINAL_NO_MONEY'[\s\S]+terminal_no_money IS TRUE/);
  const noTransferBranch = definition.slice(
    definition.indexOf("canonical_provider_state = 'NO_TRANSFER_EVIDENCE'"),
    definition.indexOf("canonical_provider_state = 'TERMINAL_NO_MONEY'")
  );
  const terminalNoMoneyBranch = definition.slice(
    definition.indexOf("canonical_provider_state = 'TERMINAL_NO_MONEY'"),
    definition.indexOf(') AS source_less_manual_adjustment_advisory')
  );
  assert.match(noTransferBranch, /provider_outage IS NOT TRUE/);
  assert.doesNotMatch(terminalNoMoneyBranch, /provider_outage/);
  assert.match(definition, /provider_pending_non_final IS NOT TRUE/);
  assert.match(definition, /provider_facts\.provider_submission_in_progress IS NOT TRUE/);
  assert.equal(count(definition, 'candidate_provider_precedence_index.carry_forward_freshness_blocked IS NOT TRUE'), 3);
  assert.doesNotMatch(definition, /public\.pay_payment_cancelability_diagnostic\s*\(/);
  assert.match(statusAdmission, /REVOKE ALL ON FUNCTION public\.pay_batch_payment_status_page_v1\([^)]+\) FROM PUBLIC, anon, authenticated, service_role;/);
  assert.match(statusAdmission, /GRANT EXECUTE ON FUNCTION public\.pay_batch_payment_status_page_v1\([^)]+\) TO service_role;/);
});

test('fresh communication V2 assignment is narrow and old contracts are preserved', () => {
  const originalPlan = extractFunction(read(paths.monolith), 'public.pay_payment_correction_plan');
  const admission = read(paths.admissionOut);
  const plan = extractFunction(admission, 'public.pay_payment_correction_plan');
  const prepare = read(paths.prepareOut);
  const expand = read(paths.expandOut);
  assertPlanCommunicationSafety(plan);
  const legacyStart = '  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_plan_mail;';
  const legacyEnd = '  v_work_item_count := CASE';
  const originalLegacyMail = originalPlan.slice(
    originalPlan.indexOf(legacyStart),
    originalPlan.indexOf(legacyEnd)
  );
  assert.ok(plan.includes(`  ELSE\n${originalLegacyMail}  END IF;\n`));
  assert.equal(count(plan, 'public.mail_outbox'), count(originalPlan, 'public.mail_outbox'));
  assert.match(plan, /v_recommended_action = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'[\s\S]+can_pre_provider_cancel/);
  assert.match(plan, /v_recommended_action = 'NO_MONEY_UNWIND_AND_RECALCULATE'[\s\S]+can_no_money_unwind/);
  assert.match(plan, /v_scope_type IN \('BATCH', 'CANDIDATES'\)/);
  assert.match(plan, /resolved_full_payment_scope_json,is_full_scope/);
  assert.doesNotMatch(
    plan.slice(plan.indexOf('v_financial_cancellation_communication_v2 :='), plan.indexOf('IF v_financial_cancellation_communication_v2 THEN')),
    /requested_action|CORRECTION_REVIEW|DRAFT_REMOVE_FROM_BATCH/i
  );
  assert.match(plan, /'follow_up_cancellation_notice_permitted', false/);
  assert.match(plan, /'follow_up_cancellation_notice_requires_proved_original_sent', true/);
  assert.equal(count(prepare, 'CREATE OR REPLACE FUNCTION public.pay_payment_correction_selection_prepare_chunk_v1('), 1);
  assert.match(prepare, /v_request\.status = 'PLANNING'[\s\S]+NOT EXISTS \([\s\S]+pay_payment_correction_request_candidates[\s\S]+NOT EXISTS \([\s\S]+PREPARE_SELECTION[\s\S]+CANDIDATE_SCOPE/);
  assert.match(prepare, /v_fresh_candidate_scope_assignment := true/);
  assert.match(prepare, /ELSIF v_fresh_candidate_scope_assignment THEN[\s\S]+v_communication_cleanup_contract_version := 2/);
  assert.match(prepare, /v_communication_cleanup_contract_raw IN \('1', '2'\)/);
  assert.match(prepare, /IF v_communication_cleanup_contract_version = 2 THEN[\s\S]+v_matching_queued_count := 0;[\s\S]+ELSE[\s\S]+FROM public\.mail_outbox/);
  assert.equal(count(prepare, "'communication_cleanup_contract_version', 1"), 0);

  assert.equal(count(expand, 'CREATE OR REPLACE FUNCTION public.pay_payment_correction_expand_work('), 1);
  assert.match(expand, /v_communication_cleanup_contract_raw NOT IN \('1', '2'\)/);
  assert.match(expand, /'communication_cleanup_contract_version', v_communication_cleanup_contract_version/);
  assert.match(expand, /IS DISTINCT FROM v_communication_cleanup_contract_version::text/);
  assert.doesNotMatch(expand, /'communication_cleanup_contract_version', 1/);
  assert.match(
    expand,
    /'source_row_count_semantics', v_source_row_count_semantics\s+\) \|\| CASE WHEN v_candidate_scope_contract_version = 2 THEN\s+pg_catalog\.jsonb_build_object\(\s+'communication_cleanup_contract_version'/
  );
  assert.doesNotMatch(
    expand,
    /'created_by', 'pay_payment_correction_expand_work',[\s\S]{0,400}'source_row_count_semantics', v_source_row_count_semantics,\s+'communication_cleanup_contract_version'/
  );
});

test('apply owners preserve communication V1 exactly and isolate mail-free V2', () => {
  const apply = read(paths.applyOut);
  assert.equal(count(apply, 'CREATE OR REPLACE FUNCTION public.pay_pre_bank_cancel_apply_work_item('), 1);
  assert.equal(count(apply, 'CREATE OR REPLACE FUNCTION public.pay_no_money_unwind_apply_work_item('), 1);
  assert.equal(count(apply, 'PAYMENT_CORRECTION_LEGACY_COMMUNICATION_SCOPE_RESTAGE_REQUIRED'), 0);
  assert.equal(count(apply, 'IF v_communication_cleanup_contract_version = 1 THEN'), 4);
  assert.match(apply, /v_communication_cleanup_contract_plan_raw NOT IN \('1', '2'\)/);
  assert.match(apply, /v_communication_cleanup_contract_work_raw IS DISTINCT FROM v_communication_cleanup_contract_plan_raw/);
  assert.match(apply, /financial_cancellation_independent_of_mail', v_communication_cleanup_contract_version = 2/);
  assert.match(apply, /mail_outbox_read_performed', v_communication_cleanup_contract_version = 1/);
  assert.equal(count(apply, "'follow_up_cancellation_notice_permitted', true"), 0);
  assert.ok(count(apply, "'follow_up_cancellation_notice_requires_proved_original_sent'") >= 4);
  assert.match(apply, /request_work\.selection_json->>'communication_cleanup_contract_version' = v_communication_cleanup_contract_version::text/);
  assert.ok(count(apply, 'public.mail_outbox') > 0, 'legacy mail-sensitive branch was erased');
  const noMoneyDefinition = extractFunction(apply, 'public.pay_no_money_unwind_apply_work_item');
  assertNoMoneyResultObjectAritySafety(noMoneyDefinition);
  const splitBoundary = `'communication_cleanup_contract_version', CASE
      WHEN v_candidate_scope_contract_version = 2 THEN v_communication_cleanup_contract_version ELSE NULL::integer END
  ) || jsonb_build_object(
    'matching_queued_count', v_matching_queued_count,`;
  assert.throws(() => assertNoMoneyResultObjectAritySafety(
    noMoneyDefinition.replace(
      splitBoundary,
      `'communication_cleanup_contract_version', CASE
      WHEN v_candidate_scope_contract_version = 2 THEN v_communication_cleanup_contract_version ELSE NULL::integer END,
    'matching_queued_count', v_matching_queued_count,`
    )
  ));
});

test('source-less ambiguity remains truthful while complete proved-unpaid cancellation is advisory', () => {
  const admission = read(paths.admissionOut);
  const apply = read(paths.applyOut);
  assert.equal(count(admission, 'CREATE OR REPLACE FUNCTION public.pay_payment_cancelability_diagnostic('), 1);
  assert.equal(count(admission, 'CREATE OR REPLACE FUNCTION public._pay_payment_movement_classify('), 1);
  assert.equal(count(admission, 'CREATE OR REPLACE FUNCTION public.pay_payment_correction_plan('), 1);
  assert.match(admission, /v_scope_is_full[\s\S]+v_scope_type IN \('BATCH', 'CANDIDATES'\)[\s\S]+v_is_local_not_sent OR v_has_terminal_no_money/);
  assert.match(admission, /can_carry_forward_automatically', v_can_carry_forward_automatically/);
  assert.match(admission, /PRESERVE_FROZEN_FACTS_NO_RECONSTRUCTION/);
  assert.ok(count(extractFunction(admission, 'public.pay_payment_correction_plan'), 'public.mail_outbox') > 0);
  assert.match(apply, /classification' = 'SOURCE_LESS_AMBIGUOUS'/);
  assert.equal(
    count(
      apply,
      "IF (jsonb_array_length(COALESCE(v_manual_adjustment_result->'carry_forward_blockers', '[]'::jsonb)) > 0\n     OR COALESCE((v_manual_adjustment_result->>'can_carry_forward_automatically')::boolean, true) IS NOT TRUE\n  )\n     AND COALESCE(NULLIF(v_classification_result->>'source_restoration_investigation_required', '')::boolean, false) IS NOT TRUE THEN"
    ),
    2,
    'both apply owners must make the complete manual-blocker expression advisory'
  );
  assert.doesNotMatch(
    apply,
    /carry_forward_blockers'[\s\S]{0,300}IS NOT TRUE\n     AND COALESCE\(NULLIF\(v_classification_result->>'source_restoration_investigation_required'/,
    'unparenthesized OR/AND precedence would still block an approved investigation case'
  );
  assert.match(apply, /'status', 'NEEDS_INVESTIGATION'/);
  assert.match(apply, /'source_reconstruction_performed', false/);
  assert.match(apply, /'source_carry_forward_created', false/);
});

test('investigation alert is per item, action-required, indexed and preference-compatible', () => {
  const alert = read(paths.alertOut);
  const migration = read(paths.migration);
  assert.equal(count(alert, 'CREATE OR REPLACE FUNCTION public.banking_alerts_active_for_user('), 1);
  assert.equal(count(alert, 'CREATE OR REPLACE FUNCTION public.banking_alert_preferences_get('), 1);
  assert.equal(count(alert, 'CREATE OR REPLACE FUNCTION public.banking_alert_preferences_update('), 1);
  assert.match(alert, /'MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED'::text AS alert_kind/);
  assert.match(alert, /'ACTION_REQUIRED'::text AS severity/);
  assert.match(alert, /'pay_batch_item'::text AS entity_kind/);
  assert.match(alert, /investigation_row\.pay_batch_item_id AS entity_id/);
  assert.match(alert, /'pay_batch_id', investigation_row\.pay_batch_id::text/);
  assert.match(alert, /NOT EXISTS \([\s\S]+banking_alert_acknowledgements AS investigation_ack[\s\S]+investigation_ack\.entity_id = correction_item\.pay_batch_item_id/);
  const investigationRows = alert.slice(
    alert.indexOf('  source_restoration_investigation_rows AS MATERIALIZED ('),
    alert.indexOf('  source_restoration_investigation_alerts AS MATERIALIZED (')
  );
  assert.match(alert, /ELSIF p_limit = 0 THEN\n    v_limit := NULL::integer;/);
  assert.doesNotMatch(
    investigationRows,
    /^\s+LIMIT\b/im,
    'the source CTE must not truncate the global alert count/hash before the established detail-page limit'
  );
  assert.match(alert, /limited_alerts AS MATERIALIZED \([\s\S]+LIMIT v_limit/);
  assert.match(alert, /signal_aggregate AS MATERIALIZED \([\s\S]+FROM alert_rows/);
  assert.ok(count(alert, "'MANUAL_ADJUSTMENT_INVESTIGATION_REQUIRED',") >= 3);
  assert.equal(count(migration, 'CREATE INDEX IF NOT EXISTS'), 3);
  assert.equal(count(migration, "after_snapshot_json #>> '{source_restoration,status}' = 'NEEDS_INVESTIGATION'"), 3);
  assert.match(
    migration,
    /operation_row\.operation_type = 'PAYMENT_CORRECTION'[\s\S]+NOT IN \(\s*'COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED'\s*\)/,
    'the release guard must not misclassify terminal REVIEW_REQUIRED audit history as active work'
  );
});

test('final activation and release verifier reject every nonterminal correction without migration', () => {
  const admission = read(paths.admissionOut);
  const migration = read(paths.migration);
  const verifier = read(paths.verifier);
  assert.match(admission, /begin;[\s\S]+LOCK TABLE public\.pay_payment_correction_requests IN SHARE ROW EXCLUSIVE MODE;[\s\S]+LOCK TABLE public\.banking_pay_operations IN SHARE ROW EXCLUSIVE MODE;[\s\S]+CREATE OR REPLACE FUNCTION public\.pay_payment_cancelability_diagnostic/);
  for (const status of ['APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'REJECTED', 'CANCELLED']) {
    assert.match(admission, new RegExp(`'${status}'`));
  }
  for (const status of ['COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED']) {
    assert.match(admission, new RegExp(`'${status}'`));
    assert.match(migration, new RegExp(`'${status}'`));
  }
  assert.match(admission, /PAYMENT_CORRECTION_NONTERMINAL_CUTOVER_BLOCKED/);
  assertFinalActivationCutoverSafety(admission);
  assert.doesNotMatch(admission, /migrate|restage|compatibility execution/i);
  assert.match(verifier, /operation_row\.operation_type = 'PAYMENT_CORRECTION'/);
  assert.match(verifier, /PAYMENT_CORRECTION_NONTERMINAL_CUTOVER_BLOCKED/);
  assert.match(verifier, /pg_catalog\.substring\(\s*v_definition,\s*v_no_transfer_branch_start,/);
  assert.match(verifier, /pg_catalog\.substring\(\s*v_definition,\s*v_terminal_no_money_branch_start,/);
  assert.doesNotMatch(verifier, /pg_catalog\.substring\(\s*v_definition\s+FROM/i);
  assert.doesNotMatch(verifier, /\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE)\s+(INTO\s+|FROM\s+)?public\./i);
  assert.match(verifier, /begin;[\s\S]+rollback;/i);
});

test('legacy TEST cutover closes only bounded terminal-work cancellations through the existing owner', () => {
  const repair = read(paths.cutoverRepair);
  const followingMigration = read(paths.migration);
  assertLegacyCutoverRepairSafety(repair);
  assert.ok(
    paths.cutoverRepair.localeCompare(paths.migration) < 0,
    'the repair must sort before the unchanged zero-active cutover gate'
  );
  assert.match(followingMigration, /PAYMENT_CORRECTION_RELEASE_REQUIRES_ZERO_ACTIVE_REQUESTS/);
});

test('legacy TEST cutover boundary mutations fail closed', () => {
  const repair = read(paths.cutoverRepair);
  const mutations = [
    source => source.replace("IF v_environment IS DISTINCT FROM 'TEST' THEN", 'IF false THEN'),
    source => source.replaceAll("request_row.correction_kind = 'PRE_BANK_CANCEL'", 'true'),
    source => source.replaceAll("request_row.status = 'PROCESSING'", 'true'),
    source => source.replaceAll("request_row.requested_at_utc < '2026-08-15 00:00:00+00'::timestamptz", 'true'),
    source => source.replaceAll("operation_row.status = 'REVIEW_REQUIRED'", 'true'),
    source => source.replaceAll("operation_row.phase = 'FINALISE'", 'true'),
    source => source.replace('v_eligible_request_count IS DISTINCT FROM v_active_request_count', 'false'),
    source => source.replace('v_eligible_request_count > 8', 'false'),
    source => source.replace("v_result->>'code' IS DISTINCT FROM 'PAYMENT_CORRECTION_FINALISED'", 'false'),
    source => source.replace('PAYMENT_CORRECTION_LEGACY_CUTOVER_NOT_CLEAN', 'PAYMENT_CORRECTION_LEGACY_CUTOVER_UNCHECKED'),
  ];
  for (const [index, mutation] of mutations.entries()) {
    assert.throws(
      () => assertLegacyCutoverRepairSafety(mutation(repair)),
      undefined,
      `legacy cutover mutation ${index + 1} survived`
    );
  }
});

test('legacy applied-with-blockers audit residue is terminalised without changing money or policy', () => {
  const cutover = read(paths.legacyBlockedCutover);
  const noticeMigration = read('supabase/migrations/07092026_2300_banking_pay_payment_cancellation_mail_outbox_v1.sql');
  assert.match(cutover, /v_environment IS DISTINCT FROM 'TEST'[\s\S]+RETURN;/);
  assert.match(cutover, /request_row\.status = 'APPLIED_WITH_BLOCKERS'/);
  assert.match(cutover, /request_row\.requested_at_utc < '2026-08-15 00:00:00\+00'::timestamptz/);
  assert.match(cutover, /batch_row\.status = 'DRAFT'/);
  assert.match(cutover, /batch_row\.execution_commit_state = 'NOT_SUBMITTED'/);
  assert.match(cutover, /NOT EXISTS \([\s\S]+public\.pay_bank_transfers/);
  assert.match(cutover, /PRE_BANK_CANCEL_CLASSIFICATION_REQUIRED/);
  assert.match(cutover, /PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION/);
  assert.match(cutover, /LEGACY_WORKBENCH_REFRESH_REQUIRES_CURRENT_AUTHORITY/);
  assert.match(cutover, /v_active_request_count IS DISTINCT FROM 1/);
  assert.match(cutover, /SET status = 'BLOCKED'/);
  assert.match(cutover, /'preserved_applied_work', true/);
  assert.match(cutover, /'financial_rows_changed', false/);
  assert.doesNotMatch(cutover, /\b(DELETE|TRUNCATE|MERGE)\b/i);
  assert.doesNotMatch(cutover, /UPDATE\s+public\.(pay_batches|pay_batch_items|pay_bank_transfers|pay_advances)/i);
  assert.doesNotMatch(cutover, /provider|mail_outbox|remittance/i);
  assert.ok(
    paths.legacyBlockedCutover.localeCompare('supabase/migrations/07092026_2300_banking_pay_payment_cancellation_mail_outbox_v1.sql') < 0,
    'the legacy terminal cutover must sort before the cancellation-notice cutover'
  );
  assert.match(noticeMigration, /PAYMENT_CANCELLATION_NOTICE_CUTOVER_ACTIVE_REQUESTS/);
});

test('integrity checker reproduces exact V1, V2/comm1 and V2/comm2 hash contracts', () => {
  const integrity = read(paths.integrityOut);
  assert.equal(count(integrity, 'CREATE OR REPLACE FUNCTION public.pay_payment_correction_integrity_check_v1('), 1);
  assert.match(integrity, /v_candidate_scope_contract_raw IS NULL[\s\S]+candidate_scope_hash_version' IS NULL[\s\S]+source_row_count_semantics' IS NULL/);
  assert.match(integrity, /v_candidate_scope_contract_raw = '1'[\s\S]+candidate_scope_hash_version' = '1'[\s\S]+FINANCIAL_AND_QUEUED_COMMUNICATIONS/);
  assert.match(integrity, /v_candidate_scope_contract_raw = '2'[\s\S]+v_communication_cleanup_contract_raw IN \('1', '2'\)/);
  assert.match(integrity, /'communication_cleanup_contract_version', v_communication_cleanup_contract_version/);
  assert.match(integrity, /SELECTION_CONTRACT_MISMATCH/);
  assert.match(integrity, /SELECTION_PREPARATION_IN_PROGRESS/);
  assert.match(integrity, /STABLE[\s\S]+PARALLEL RESTRICTED[\s\S]+SECURITY DEFINER[\s\S]+statement_timeout TO '5000ms'/);
});

test('boundary mutations fail closed in the generator', () => {
  const monolith = read(paths.monolith);
  const planDefinition = extractFunction(monolith, 'public.pay_payment_correction_plan');
  const patchedPlan = patchPlan(planDefinition);
  const prepareDefinition = extractFunction(read(paths.prepare), 'public.pay_payment_correction_selection_prepare_chunk_v1');
  const expandDefinition = extractFunction(read(paths.expand), 'public.pay_payment_correction_expand_work');
  const integrityDefinition = extractFunction(read(paths.integrity), 'public.pay_payment_correction_integrity_check_v1');
  const preBankDefinition = extractFunction(read(paths.preBank), 'public.pay_pre_bank_cancel_apply_work_item');
  const statusDefinition = extractFunction(read(paths.statusPage), 'public.pay_batch_payment_status_page_v1');
  const patchedStatus = patchStatusAdmission(statusDefinition);
  const finalAdmission = read(paths.admissionOut);

  assert.throws(() => patchDiagnostic(
    extractFunction(monolith, 'public.pay_payment_cancelability_diagnostic')
      .replace('v_manual_carry_forward_blocker_count integer := 0;', 'v_manual_carry_forward_blocker_total integer := 0;')
  ));
  for (const mutation of [
    source => source.replace("v_scope_type IN ('BATCH', 'CANDIDATES')", "v_scope_type IN ('BATCH', 'CANDIDATES', 'ITEMS')"),
    source => source.replace(
      "        v_recommended_action = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'\n        AND COALESCE(NULLIF(v_classification_result->>'can_pre_provider_cancel', '')::boolean, false)",
      "        COALESCE(p_selection_json->>'requested_action', '') = 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'\n        AND COALESCE(NULLIF(v_classification_result->>'can_pre_provider_cancel', '')::boolean, false)"
    ),
    source => source.replace("v_classification_result->>'can_no_money_unwind'", "v_classification_result->>'can_pre_provider_cancel'"),
    source => source.replace('COALESCE(v_draft_removal_requested, false) IS NOT TRUE', 'true'),
    source => source.replace("'follow_up_cancellation_notice_permitted', false", "'follow_up_cancellation_notice_permitted', true"),
  ]) {
    assert.throws(() => assertPlanCommunicationSafety(mutation(patchedPlan)));
  }
  for (const mutation of [
    source => source.replace('LOCK TABLE public.pay_payment_correction_requests IN SHARE ROW EXCLUSIVE MODE;', '-- request lock removed'),
    source => source.replace("'FAILED', 'REJECTED', 'CANCELLED'", "'FAILED', 'REJECTED', 'CANCELLED', 'PROCESSING'"),
    source => source.replace("'COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED'", "'COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED', 'RUNNING'"),
    source => source.replace('operation_row.status IS NULL', 'false'),
  ]) {
    assert.throws(() => assertFinalActivationCutoverSafety(mutation(finalAdmission)));
  }
  assert.throws(() => patchSelectionPrepare(
    prepareDefinition.replace(
      "            v_candidate_scope_contract_version := 2;\n        ELSE",
      "            v_candidate_scope_contract_version := 3;\n        ELSE"
    )
  ));
  assert.throws(() => patchExpandWork(
    expandDefinition.replace('SELECT COALESCE(settings_row.banking_pay_draft_overlay_fast_cancel_v1_enabled,false)', 'SELECT false')
  ));
  assert.throws(() => patchIntegrityCheck(
    integrityDefinition.replace('v_recalculated_selection_hash :=', 'v_recalculated_selection_digest :=')
  ));
  assert.throws(() => patchApply(
    preBankDefinition.replace(applyConfig.capacityMailStart, '  -- removed capacity boundary'),
    applyConfig
  ));
  assert.throws(() => patchAlerts(
    extractFunction(monolith, 'public.banking_alerts_active_for_user')
      .replace('  latest_success_events AS MATERIALIZED (', '  success_events AS MATERIALIZED (')
  ));
  const completeAlert = patchAlerts(
    extractFunction(monolith, 'public.banking_alerts_active_for_user')
  );
  const investigationBoundary = completeAlert.indexOf(
    '  source_restoration_investigation_alerts AS MATERIALIZED ('
  );
  assert.ok(investigationBoundary > 0);
  const prematurelyLimitedAlert =
    completeAlert.slice(0, investigationBoundary).replace(/\n  \),\n$/, '\n    LIMIT 501\n  ),\n')
    + completeAlert.slice(investigationBoundary);
  assert.throws(() => assertAlertCompletenessSafety(prematurelyLimitedAlert));
  assert.throws(() => assertAlertCompletenessSafety(
    completeAlert.replace('ELSIF p_limit = 0 THEN\n    v_limit := NULL::integer;', 'ELSIF p_limit = 0 THEN\n    v_limit := 500;')
  ));
  assert.throws(() => assertAlertCompletenessSafety(
    completeAlert.replace('    LIMIT v_limit\n  ),', '    LIMIT 100\n  ),')
  ));
  const signalStart = completeAlert.indexOf('  signal_aggregate AS MATERIALIZED (');
  const signalEnd = completeAlert.indexOf('  aggregate_result AS MATERIALIZED (', signalStart);
  assert.ok(signalStart >= 0 && signalEnd > signalStart);
  const limitedSignalAlert = completeAlert.slice(0, signalStart)
    + completeAlert.slice(signalStart, signalEnd).replace('    FROM alert_rows\n', '    FROM limited_alerts AS alert_rows\n')
    + completeAlert.slice(signalEnd);
  assert.throws(() => assertAlertCompletenessSafety(limitedSignalAlert));
  for (const mutation of [
    source => source.replace('candidate_provider_precedence_index.complete_candidate_instruction_scope\n', 'true\n'),
    source => source.replace('candidate_provider_precedence_index.provider_pending_non_final IS NOT TRUE\n', 'true\n'),
    source => source.replace(
      'candidate_provider_precedence_index.provider_outcome_unknown IS NOT TRUE\n                 AND provider_facts.provider_submission_in_progress IS NOT TRUE\n',
      'candidate_provider_precedence_index.provider_outcome_unknown IS NOT TRUE\n                 AND true\n'
    ),
    source => source.replace("candidate_provider_precedence_index.canonical_provider_state = 'TERMINAL_NO_MONEY'\n", 'true\n'),
    source => source.replace(
      "candidate_provider_precedence_index.canonical_provider_state = 'TERMINAL_NO_MONEY'\n                     AND candidate_provider_precedence_index.terminal_no_money IS TRUE",
      "candidate_provider_precedence_index.canonical_provider_state = 'TERMINAL_NO_MONEY'\n                     AND candidate_provider_precedence_index.provider_outage IS NOT TRUE\n                     AND candidate_provider_precedence_index.terminal_no_money IS TRUE"
    ),
    source => source.replace('candidate_provider_precedence_index.carry_forward_freshness_blocked IS NOT TRUE\n', 'true\n'),
    source => source.replace('OR candidate_provider_precedence_index.source_less_manual_adjustment_advisory\n', 'OR true\n'),
  ]) {
    assert.throws(() => assertStatusAdmissionSafety(mutation(patchedStatus)));
  }
});
