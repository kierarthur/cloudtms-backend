const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repoRoot = path.resolve(__dirname, '..');
const cancellationGuardPath = path.join(
  repoRoot,
  'supabase',
  'repeatable',
  '23072026_1402_disable_plpgsql_check_for_banking_cancel.sql'
);

const correctionGuardPath = path.join(
  repoRoot,
  'supabase',
  'repeatable',
  '23072026_1217_disable_plpgsql_check_for_correction_chain_banking.sql'
);

const cancellationGuard = fs.readFileSync(cancellationGuardPath, 'utf8');
const correctionGuard = fs.readFileSync(correctionGuardPath, 'utf8');

const correctionEntryPointSources = [
  ['21072026_1235_00_import_correction_policy_helpers.sql', [
    '_ctms_import_correction_classify_v1',
    '_ctms_correction_policy_leg_read_v1'
  ]],
  ['21072026_1235_00b_import_correction_runtime_guards.sql', [
    '_ctms_candidate_correction_residuals_v1',
    '_ctms_rewrite_source_build_correction_negative_components_v1',
    '_ctms_rewrite_sync_authoritative_correction_negative_components_v1',
    '_ctms_rewrite_sync_correction_cases_v1',
    '_ctms_assert_payload_corrections_fresh_v1',
    '_ctms_materialise_candidate_correction_residuals_v1'
  ]],
  ['21072026_1235_05_timesheet_correction_chain_scope_v1.sql', ['timesheet_correction_chain_scope_v1']],
  ['21072026_1235_09_pay_correction_chain_residual_v1.sql', ['pay_correction_chain_residual_v1']],
  ['04082026_1213_pay_workbench_candidate_source_build_chunk.sql', ['pay_workbench_candidate_source_build_chunk']],
  ['04082026_1210_pay_sync_overpayments_from_preview.sql', ['pay_sync_overpayments_from_preview']],
  ['21072026_1235_38_tsfin_write_snapshots_and_complete.sql', ['tsfin_write_snapshots_and_complete']],
  ['21072026_1235_41_pay_workbench_session_apply_case_resolution.sql', [
    'pay_workbench_session_apply_case_resolution'
  ]],
  ['19072026_1405_revalidate_recovery_headroom_after_materialisation.sql', ['pay_workbench_worker_drain_chunk_revalidated_v1']],
  ['19072026_1405_revalidate_recovery_headroom_after_materialisation.sql', [
    'pay_workbench_revalidate_zero_retained_recovery_headroom_v1'
  ]],
  ['20072026_1133_resolve_frozen_recovery_timesheet_identity.sql', [
    '_pay_policy_x_resolve_post_draft_economic_key'
  ]],
  ['26052026_2100HRS_NEW_FUNCTIONS.sql', [
    '_pay_week_start_monday',
    'pay_paye_guardrails',
    'pay_preview_build_context',
    'pay_workbench_session_recompute_progress_counters',
    'pay_workbench_session_compact_progress_json',
    '_pay_batch_item_source_reservation_amount_ex_vat',
    'pay_preview_candidate_collect_scope',
    'pay_workbench_worker_drain_chunk',
    'pay_finance_case_apply_taxable_channel_restructure'
  ]]
];

test('draft cancellation and post-cancel refresh entry points disable faulty passive instrumentation', () => {
  for (const signature of [
    /ALTER FUNCTION public\.pay_payment_cancelability_diagnostic\(\s*uuid,\s*jsonb,\s*uuid,\s*text\s*\) SET plpgsql_check\.mode TO 'disabled';/s,
    /ALTER FUNCTION public\.pay_payment_cancel_not_sent_and_recalculate_complete_v1\(\s*uuid,\s*jsonb,\s*uuid,\s*text,\s*text,\s*jsonb\s*\) SET plpgsql_check\.mode TO 'disabled';/s,
    /ALTER FUNCTION public\.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1\(\s*uuid,\s*uuid,\s*text,\s*uuid,\s*jsonb\s*\) SET plpgsql_check\.mode TO 'disabled';/s,
    /ALTER FUNCTION public\.pay_workbench_session_clear_all_decisions\(\s*uuid,\s*uuid\s*\) SET plpgsql_check\.mode TO 'disabled';/s
  ]) {
    assert.match(cancellationGuard, signature);
  }
});

test('instrumentation guards do not redefine Banking Pay economic or mutation bodies', () => {
  const combined = `${correctionGuard}\n${cancellationGuard}`;
  assert.doesNotMatch(combined, /\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\b/i);
  assert.doesNotMatch(combined, /\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE)\b/i);
  assert.doesNotMatch(combined, /\bpay_batch_items\b|\bpay_advance_reservations\b|\bpay_bank_transfers\b/i);
});

test('banking-alert polling disables the same faulty passive instrumentation', () => {
  assert.match(
    correctionGuard,
    /'public\.banking_alerts_refresh_for_user\(uuid,text,integer\)'::regprocedure/
  );
});

test('case-resolution discovery disables faulty passive instrumentation before validation', () => {
  assert.match(
    correctionGuard,
    /'public\.pay_workbench_session_apply_case_resolution\(uuid,uuid,jsonb\)'::regprocedure/
  );
});

test('preview context and its nested PL/pgSQL helpers disable faulty passive instrumentation', () => {
  for (const signature of [
    /'public\._pay_week_start_monday\(date\)'::regprocedure/,
    /'public\.pay_paye_guardrails\(date,uuid,uuid\)'::regprocedure/,
    /'public\.pay_preview_build_context\(date,date,uuid,uuid,uuid,jsonb\)'::regprocedure/
  ]) {
    assert.match(correctionGuard, signature);
  }
});

test('correction-chain entry point definitions retain the checker guard when deployed independently', () => {
  for (const [fileName, functionNames] of correctionEntryPointSources) {
    const sql = fs.readFileSync(path.join(repoRoot, 'supabase', 'repeatable', fileName), 'utf8');
    for (const functionName of functionNames) {
      const escapedName = functionName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const definitions = [
        ...sql.matchAll(new RegExp(
          `CREATE\\s+OR\\s+REPLACE\\s+FUNCTION\\s+public\\.${escapedName}\\b[\\s\\S]*?AS\\s+\\$function\\$`,
          'gi'
        ))
      ];
      assert.ok(definitions.length > 0, `${functionName} definition is present in ${fileName}`);
      for (const definition of definitions) {
        assert.match(
          definition[0],
          /SET\s+"?plpgsql_check\.mode"?\s+(?:TO|=)\s+'disabled'/i,
          `${functionName} keeps its guard when ${fileName} is installed without the later ALTER repeatable`
        );
      }
    }
  }
});
