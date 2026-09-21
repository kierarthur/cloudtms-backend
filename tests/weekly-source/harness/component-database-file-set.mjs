import path from 'node:path';
import { lstat, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { sqlDateKey } from '../../../scripts/cloudtms-db-release-lib.mjs';

export const WEEKLY_SOURCE_COMPONENT_BASELINE_COMMIT = '44bf8b7bd6307df6c0fba9bdaedc7c4cdf978efc';

export const WEEKLY_SOURCE_COMPONENT_MIGRATIONS = Object.freeze([
  'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql',
  'supabase/migrations/15092026_2310_weekly_source_delivery_targets.sql',
  'supabase/migrations/15092026_2335_weekly_source_finalisation_pay_orchestration.sql',
  'supabase/migrations/18092026_0900_weekly_source_withdrawal_supersession.sql',
  'supabase/migrations/18092026_1500_weekly_source_external_publication_arrival.sql',
  'supabase/migrations/18092026_1520_weekly_source_pending_release_review_items.sql',
  'supabase/migrations/18092026_1740_weekly_source_banking_pay_absence.sql',
  'supabase/migrations/21092026_1817_weekly_source_audit_event_order.sql',
  'supabase/migrations/21092026_2012_invoice_discounting_ledger_revision.sql',
]);

export const HANDOVER2_OWNED_REPEATABLE_EXCLUSIONS = Object.freeze([
  'supabase/repeatable/04082026_1146_pay_workbench_timesheet_input_fingerprint_v1.sql',
  'supabase/repeatable/04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql',
]);

export const WEEKLY_SOURCE_COMPONENT_REPEATABLES = Object.freeze([
  // Invoice-discounting is the existing invoice-finance owner, not Banking
  // Pay or Workbench.  The Weekly Source change is limited to fail-closed
  // recompute, one active Draft and stale-Draft refusal and is therefore part
  // of the locally executable component boundary.
  'supabase/repeatable/02022026_payroll_ID_new_tables_and_triggers.sql',
  'supabase/repeatable/02082026_1626_hr_weekly_candidate_not_worked_resolution.sql',
  'supabase/repeatable/02092026_0325_candidate_paper_break_entry_v1.sql',
  'supabase/repeatable/02092026_1833_weekly_source_invoice_issue_validator_v1.sql',
  'supabase/repeatable/02092026_1834_candidate_expense_separation_delivery_v1.sql',
  'supabase/repeatable/07082026_2225_candidate_app_qr_settings_invoice_replacements_v1.sql',
  'supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql',
  'supabase/repeatable/15092026_1534_00_weekly_source_private_classifiers_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_charge_acceptance_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_correct_final_source_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_invoice_admission_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_invoice_batch_integration_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_invoice_batch_ordinary_isolation_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_projection_build_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_protected_pay_publisher_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_settings_admin_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_settings_expenses_rates_v1.sql',
  'supabase/repeatable/15092026_1534_01_weekly_source_summary_delay_presentation_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_timesheet_lineage_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_upload_context_v1.sql',
  'supabase/repeatable/15092026_1534_weekly_source_upload_publication_v1.sql',
  'supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql',
  'supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql',
  'supabase/repeatable/15092026_2336_weekly_source_finalisation_pay_orchestration_v1.sql',
  'supabase/repeatable/17092026_0100_weekly_source_banking_pay_absence_v1.sql',
  'supabase/repeatable/17092026_0200_weekly_source_rotation_authority_v1.sql',
  'supabase/repeatable/17092026_0300_weekly_source_entitlement_publication_v1.sql',
  'supabase/repeatable/17092026_0400_weekly_source_freeze_census_v1.sql',
  'supabase/repeatable/17092026_0600_weekly_source_first_authorisation_v1.sql',
  'supabase/repeatable/17092026_0700_weekly_source_pending_entitlement_release_v1.sql',
  'supabase/repeatable/17092026_0800_weekly_source_mode_a_dispatch_v1.sql',
  'supabase/repeatable/17092026_0900_weekly_source_pinned_owner_rotation_guard_v1.sql',
  'supabase/repeatable/17092026_1000_weekly_source_settlement_allocation_v1.sql',
  'supabase/repeatable/17092026_1100_weekly_source_candidate_view_producer_v1.sql',
  'supabase/repeatable/17092026_1200_weekly_source_audit_and_export_v1.sql',
  'supabase/repeatable/17092026_1300_weekly_source_external_publication_arrival_v1.sql',
  'supabase/repeatable/17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql',
  'supabase/repeatable/17092026_1500_weekly_source_row_admission_guards_v1.sql',
  'supabase/repeatable/19012026_extras.sql',
  'supabase/repeatable/19092026_0100_weekly_source_correction_cancel_v1.sql',
  'supabase/repeatable/19092026_1645_weekly_source_completed_pack_copy_v1.sql',
  'supabase/repeatable/21072026_1235_07_timesheet_paid_uninvoiced_rollover_v1.sql',
  'supabase/repeatable/21072026_1235_24_hr_weekly_phase3_apply_adjustment_truth_3arg.sql',
  'supabase/repeatable/21072026_1235_25_hr_weekly_phase3_apply_adjustment_truth_4arg.sql',
  'supabase/repeatable/21072026_1235_26_nhsp_weekly_phase3_apply_adjustment_truth.sql',
  'supabase/repeatable/21072026_1235_30_timesheet_unauthorise_atomic.sql',
  'supabase/repeatable/21072026_1235_33_timesheet_unauthorise_bulk_atomic.sql',
  'supabase/repeatable/21072026_1235_35_contract_week_manual_unprocess_atomic.sql',
  'supabase/repeatable/21072026_1235_37_tsfin_write_current_snapshot_single_bounded.sql',
  'supabase/repeatable/21072026_1235_38_tsfin_write_snapshots_and_complete.sql',
  'supabase/repeatable/21072026_1235_59_invoice_issue_one.sql',
  'supabase/repeatable/21072026_1820_00_import_review_internal_core.sql',
  'supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql',
  'supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql',
  'supabase/repeatable/21072026_1820_07_nhsp_weekly_apply_transactional.sql',
  'supabase/repeatable/21072026_1820_09_weekly_import_apply_cancellations.sql',
  'supabase/repeatable/21072026_1820_10_nhsp_weekly_apply_cancellations.sql',
  'supabase/repeatable/21072026_1820_11_weekly_import_create_cancellation_corrections.sql',
  'supabase/repeatable/21072026_1820_13_hr_weekly_validation_preview.sql',
  'supabase/repeatable/22082026_1706_daily_validation_compatibility_authorities_v1.sql',
  // The structural baseline predates the candidate weekly-entry detail owner.
  // A genuinely NEW database therefore needs this post-baseline dependency
  // before the later daily projection and paper-break replacements run.
  'supabase/repeatable/23082026_1737_candidate_app_weekly_entry_context_v1.sql',
  'supabase/repeatable/26082026_0043_candidate_manager_authoriser_policy_v2.sql',
  'supabase/repeatable/27082026_0423_candidate_electronic_rejection_resubmission_v1.sql',
  'supabase/repeatable/27082026_1255_candidate_weekly_withdrawal_reset_v1.sql',
  'supabase/repeatable/27082026_2004_candidate_weekly_withdrawal_null_target_v1.sql',
  'supabase/repeatable/27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql',
  'supabase/repeatable/28082026_1928_candidate_daily_receipt_reset_v1.sql',
  'supabase/repeatable/28082026_2002_candidate_daily_detail_projection_v1.sql',
  'supabase/repeatable/29082026_1914_contract_week_delete_boundary_reconciliation.sql',
  // The baseline carries only the fail-closed installation sentinel.  Install
  // the post-baseline effective-settings owner before Weekly Source verifiers
  // call it on a genuinely NEW database.
  'supabase/repeatable/03092026_1641_contract_settings_effective_authority_v1.sql',
]);

function canonicalOrder(left, right) {
  const dateOrder = sqlDateKey(path.basename(left)).localeCompare(sqlDateKey(path.basename(right)), 'en');
  return dateOrder || left.localeCompare(right, 'en');
}

export function orderedWeeklySourceComponentFiles() {
  return Object.freeze([
    ...[...WEEKLY_SOURCE_COMPONENT_MIGRATIONS].sort(canonicalOrder),
    ...[...WEEKLY_SOURCE_COMPONENT_REPEATABLES].sort(canonicalOrder),
  ]);
}

export async function verifyWeeklySourceComponentFileSet(repoRoot) {
  const all = orderedWeeklySourceComponentFiles();
  const unique = new Set(all);
  if (unique.size !== all.length) throw new Error('WEEKLY_SOURCE_COMPONENT_FILE_SET_DUPLICATE');
  for (const excluded of HANDOVER2_OWNED_REPEATABLE_EXCLUSIONS) {
    if (unique.has(excluded)) throw new Error(`WEEKLY_SOURCE_COMPONENT_HANDOVER2_FILE_INCLUDED:${excluded}`);
  }
  const files = [];
  for (const relative of all) {
    const absolute = path.join(repoRoot, relative);
    const stat = await lstat(absolute).catch(() => null);
    if (!stat?.isFile() || stat.isSymbolicLink()) throw new Error(`WEEKLY_SOURCE_COMPONENT_FILE_INVALID:${relative}`);
    const bytes = await readFile(absolute);
    files.push(Object.freeze({ relative, sha256: createHash('sha256').update(bytes).digest('hex') }));
  }
  return Object.freeze({
    baselineCommit: WEEKLY_SOURCE_COMPONENT_BASELINE_COMMIT,
    migrationCount: WEEKLY_SOURCE_COMPONENT_MIGRATIONS.length,
    repeatableCount: WEEKLY_SOURCE_COMPONENT_REPEATABLES.length,
    excludedHandover2Files: HANDOVER2_OWNED_REPEATABLE_EXCLUSIONS,
    files: Object.freeze(files),
  });
}
