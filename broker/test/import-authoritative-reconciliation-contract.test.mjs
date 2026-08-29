import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const read = (path) => readFileSync(new URL(path, import.meta.url), 'utf8');
const core = read('../../supabase/repeatable/21072026_1820_00_import_review_internal_core.sql');
const lifecycle = read('../../supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql');
const hrApply = read('../../supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql');
const nhspApply = read('../../supabase/repeatable/21072026_1820_07_nhsp_weekly_apply_transactional.sql');
const hrPhase3 = read('../../supabase/repeatable/21072026_1235_24_hr_weekly_phase3_apply_adjustment_truth_3arg.sql');
const nhspPhase3 = read('../../supabase/repeatable/21072026_1235_26_nhsp_weekly_phase3_apply_adjustment_truth.sql');
const paidRollover = read('../../supabase/repeatable/21072026_1235_07_timesheet_paid_uninvoiced_rollover_v1.sql');
const authorise = read('../../supabase/repeatable/21072026_1235_32_timesheet_authorise_bulk_atomic.sql');
const unauthorise = read('../../supabase/repeatable/21072026_1235_33_timesheet_unauthorise_bulk_atomic.sql');
const bulk = read('../../supabase/repeatable/13062026_1544_process_authorise_unprocess_unauthorise.sql');
const invoice = read('../../supabase/repeatable/21072026_1235_10_invoice_correction_pair_scope_v1.sql');
const dailyApply = read('../../supabase/repeatable/21072026_1820_08_hr_daily_apply_transactional.sql');

function functionBody(source, name) {
  const marker = `create or replace function public.${name}(`;
  const start = source.toLowerCase().indexOf(marker.toLowerCase());
  assert.notEqual(start, -1, `${name} must be defined exactly once`);
  assert.equal(source.toLowerCase().indexOf(marker.toLowerCase(), start + marker.length), -1, `${name} must not be duplicated`);
  const end = source.toLowerCase().indexOf('$function$;', start);
  assert.notEqual(end, -1, `${name} must have a complete body`);
  return source.slice(start, end + '$function$;'.length);
}

test('effective invoice balance is source-scoped, bounded and signed across issued invoice evidence', () => {
  const body = functionBody(core, '_import_review_effective_invoice_balance_core_v1');
  assert.match(body, /p_max_sources integer default 100/);
  assert.match(body, /IMPORT_REVIEW_SOURCE_LIMIT_EXCEEDED/);
  assert.match(body, /i\.status in \('ISSUED','PAID','ON_HOLD'\)/);
  assert.match(body, /v_b_day:=v_b_day\+v_component_day/);
  assert.match(body, /historical_missing_timesheet_ids/);
  assert.match(body, /archived_timesheet_ids/);
  assert.match(body, /case when ae\.object_type='timesheets' then ae\.object_id_text end/);
  assert.match(body, /effective_invoice_fingerprint/);
  assert.match(body, /B_standard_representable/);
  assert.match(body, /ignored_nonhours_invoice_line_ids/);
  assert.match(body, /generation_role_evidence/);
  assert.match(body, /effective_hours_net_is_zero/);
  assert.match(body, /effective_hours_net_is_positive/);
  assert.match(body, /effective_hours_net_is_negative/);
  assert.match(body, /physically_missing_mutable_roles/);
  assert.match(body, /FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED/);
});

test('catalogue evaluates every source in deterministic batches without limiting the import to 100 shifts', () => {
  const body = functionBody(core, '_import_review_action_catalog_core_v1');
  assert.match(body, /reconciliation_batch/);
  assert.match(body, /row_number\(\) over \(order by f\.source_row_key\)/);
  assert.match(body, /jsonb_agg\(jsonb_build_object\([\s\S]*?order by f\.source_row_key\)/);
  assert.match(body, /AMEND_SOURCE/);
  assert.match(body, /AMEND_PAID_UNINVOICED_SOURCE/);
  assert.match(body, /AMEND_EXISTING_REPLACEMENT/);
  assert.match(body, /CREATE_REVERSAL_REPLACEMENT/);
  assert.match(body, /source_scope_fingerprint'[\s\S]*?\)\s*\|\|\s*jsonb_build_object\(/);
  assert.match(body, /c\.resolved_client_id is not null[\s\S]*?c\.resolved_contract_id is not null[\s\S]*?then public\.import_auto_authorise_policy_resolve_v1/);
});

test('apply envelope and guard freeze and re-attest exact reconciliation units', () => {
  const envelope = functionBody(core, '_import_review_apply_envelope_core_v1');
  const guard = functionBody(lifecycle, 'import_review_apply_guard_v1');
  for (const field of [
    'B_effective_invoice_ids',
    'B_invoice_fingerprint',
    'M_active_member_ids',
    'A_schedule_json',
    'reviewed_existing_correction_id',
    'repair_identity_mode',
    'review_policy_basis_kind',
    'review_policy_basis_fingerprint',
    'unit_fingerprint',
  ]) {
    assert.match(envelope, new RegExp(`'${field}'`));
  }
  assert.match(guard, /pg_temp\.import_review_reconciliation_units_v1/);
  assert.match(guard, /for update/);
  assert.match(guard, /unit_fingerprint/);
  assert.match(guard, /IMPORT_REVIEW_SELECTED_ACTION_STALE/);
});

test('lifecycle transition prepares, validates and authorises exact members idempotently', () => {
  const body = functionBody(lifecycle, 'import_review_correction_generation_transition_v1');
  assert.match(body, /v_action not in \('PREPARE','VALIDATE','AUTHORISE'\)/);
  assert.match(body, /timesheet_unauthorise_bulk_atomic/);
  assert.match(body, /timesheet_authorise_bulk_atomic/);
  assert.match(body, /AMEND_PAID_UNINVOICED_SOURCE/);
  assert.match(body, /v_all_authorised/);
  assert.match(body, /IMPORT_REVIEW_RECONCILIATION_LIFECYCLE_STATE_INVALID/);
  assert.match(body, /unit_fingerprints/);
  assert.match(body, /IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH/);
  assert.match(body, /correction_operation_contract,correction_units/);
  assert.match(body, /applied_result_fingerprint/);
  assert.match(body, /applied_timesheet_id/);
  assert.match(body, /historical_paid_tsfin_id/);
  assert.match(body, /current_shell_tsfin_id/);
});

test('bulk lifecycle RPCs accept only the transaction-bound temporary capability', () => {
  for (const source of [authorise, unauthorise]) {
    assert.match(source, /pg_temp\.import_review_lifecycle_capability_v1/);
    assert.match(source, /cloudtms\.import_reconciliation_capability_token/);
    assert.match(source, /txid_current\(\)/);
    assert.match(source, /expected_timesheet_id/);
  }
});

test('NHSP and HealthRoster Weekly callers use the same four reconciliation routes', () => {
  for (const source of [hrApply, nhspApply]) {
    assert.match(source, /AMEND_SOURCE/);
    assert.match(source, /AMEND_PAID_UNINVOICED_SOURCE/);
    assert.match(source, /AMEND_EXISTING_REPLACEMENT/);
    assert.match(source, /CREATE_REVERSAL_REPLACEMENT/);
    assert.match(source, /import_review_correction_generation_transition_v1/);
    assert.match(source, /timesheet_paid_uninvoiced_rollover_v1/);
    assert.match(source, /v_paid_origin_operation\.state<>'COMPLETE'/);
    assert.match(source, /applied_result_fingerprint/);
    assert.match(source, /IMPORT_REVIEW_PAID_ROLLOVER_SHELL_ORIGIN_INCOMPLETE/);
    assert.match(source, /IMPORT_REVIEW_PAID_ROLLOVER_SHELL_POLICY_CHANGED/);
    assert.match(source, /order by u\.source_timesheet_id/);
    assert.match(source, /IMPORT_AUTHORITATIVE_RECONCILIATION_V1/);
  }
});

test('paid-uninvoiced rollover supports an ordinary authoritative source without inventing a correction chain', () => {
  const body = functionBody(paidRollover, 'timesheet_paid_uninvoiced_rollover_v1');
  assert.match(body, /v_is_ordinary_source:=v_route='AMEND_PAID_UNINVOICED_SOURCE'/);
  assert.match(body, /B_effective_invoice_ids/);
  assert.match(body, /B_effective_invoice_line_ids/);
  assert.match(body, /\{B_hours,total_hours\}/);
  assert.match(body, /PAID_TSFIN_ROLLOVER_OPERATION_UNIT_NOT_UNIQUE/);
  assert.match(body, /PAID_TSFIN_ROLLOVER_PREFLIGHT_STALE/);
  assert.match(body, /PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_PREFLIGHT_INVALID/);
  assert.match(body, /import_apply_operation_id/);
  assert.match(body, /import_authoritative_unit_fingerprint/);
  assert.match(body, /historical_paid_tsfin_id/);
  assert.match(body, /new_current_tsfin_id/);
});

test('both Weekly phase-3 functions repair missing roles and preserve source-specific basis', () => {
  const hr = functionBody(hrPhase3, 'hr_weekly_phase3_apply_adjustment_truth');
  const nhsp = functionBody(nhspPhase3, 'nhsp_weekly_phase3_apply_adjustment_truth');
  for (const body of [hr, nhsp]) {
    assert.match(body, /v_reconciliation_unit/);
    assert.match(body, /insert into public\.timesheets/);
    assert.match(body, /B_standard_schedule_json/);
    assert.match(body, /A_schedule_json/);
    assert.match(body, /import_authoritative_reconciliation/);
    assert.match(body, /archived_at_utc is null/);
    assert.match(body, /repair_identity_mode/);
    assert.match(body, /FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED/);
    assert.match(body, /IMPORT_REVIEW_MUTABLE_GENERATION_CONTRACT_WEEK_AMBIGUOUS/);
    assert.match(body, /reversal_timesheet_id/);
    assert.match(body, /replacement_timesheet_id/);
    assert.match(body, /applied_result_fingerprint/);
  }
  assert.match(hr, /HEALTHROSTER/);
  assert.match(nhsp, /NHSP/);
});

test('Bulk Authorise labels only an exact current import correction pair and fails source disagreement closed', () => {
  const rowPatch = functionBody(bulk, 'bulk_timesheet_row_patch_v1');
  const dataset = functionBody(bulk, 'bulk_authorise_dataset_v1');
  for (const body of [rowPatch, dataset]) {
    assert.match(body, /CHANGED_HOURS_REVERSAL/);
    assert.match(body, /CHANGED_HOURS_REPLACEMENT/);
    assert.match(body, /IMPORT_CORRECTION/);
    assert.match(body, /BULK_AUTHORISE_CORRECTION_SOURCE_CONFLICT/);
    assert.match(body, /HealthRoster Reversal/);
    assert.match(body, /HealthRoster Corrected Hours/);
    assert.match(body, /NHSP Reversal/);
    assert.match(body, /NHSP Corrected Hours/);
  }
});

test('invoice compatibility remains narrowly gated to a committed exact reconciliation pair', () => {
  const body = functionBody(invoice, 'invoice_correction_pair_scope_v1');
  assert.match(body, /IMPORT_AUTHORITATIVE_RECONCILIATION_V1/);
  assert.match(body, /o\.state='COMPLETE'/);
  assert.match(body, /historical_missing_timesheet_ids/);
  assert.match(body, /effective_invoice_fingerprint/);
  assert.match(body, /v_pair_member_count<>2/);
  assert.match(body, /v_pair_reversal_count<>1/);
  assert.match(body, /v_pair_replacement_count<>1/);
  assert.match(body, /total_pay_ex_vat<>-coalesce/);
});

test('HealthRoster Daily remains validation-only and has no reconciliation-generation path', () => {
  const body = functionBody(dailyApply, 'hr_daily_apply_transactional');
  assert.doesNotMatch(body, /IMPORT_AUTHORITATIVE_RECONCILIATION_V1/);
  assert.doesNotMatch(body, /import_review_correction_generation_transition_v1/);
  assert.doesNotMatch(body, /timesheet_paid_uninvoiced_rollover_v1/);
  assert.doesNotMatch(body, /CREATE_REVERSAL_REPLACEMENT/);
});
