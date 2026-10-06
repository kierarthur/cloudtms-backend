// Exact canonical LF, recursive-closure pins. This is execution packaging
// authority, not a migration ledger or permission to change accepted SQL.
// Every row was inspected for the committed stages described below. Unknown
// envelopes and changed closures fail before admission or installation.
export const BOOTSTRAP_PIN = '73dea8756a84a272a9ab53762c912c39594200fe596d39fc45764ecf300321fd';
// Original migrations represented by the immutable August structural baseline,
// including its release control-plane anchor. A backdated newly registered
// migration must never be falsely receipted as part of that old baseline.
export const BASELINE_MIGRATION_PREFIX = Object.freeze({count:182,
  sha256:'df03223c3aeadcff642c223c79c2bddb54a50a976d0721d06be8e960752c4757'});
const rows = [
  ['migrations/08092026_0517_banking_pay_candidate_dirty_active_cohort_indexes_v1.sql','5755485f86e3a9b8c722350062e6b74aeebc3509772b342c48e41743a3a52b1d','INDEX_REBUILD'],
  ['migrations/08092026_1159_banking_pay_cancel_return_frozen_scope_lookup_v1.sql','5d58fdc097d20b3b331db7eaab3bd6fef70cae04fb986f75abbfdc7daebcdad0','INDEX_REBUILD'],
  ['migrations/26092026_0210_banking_pay_stage2_backfill.sql','0e7e8fd7e9fd99b66cc000a7920f25a320887e45fc123b664dd89d25b1902726','BACKFILL'],
  ['repeatable/19012026_extras.sql','7b3e8daa845a6aff85feabc572498976d7c4ea66c2ca5322c64a5195cd58c1aa','EXTRAS'],
  ['repeatable/02022026_payroll_ID_new_tables_and_triggers.sql','aaa26c600609e273a10bc257869dbc6019b33c13398bd46ed9a81711d1b6163e','PAYROLL_TRIGGERS'],
  ['repeatable/25072026_0002_private_invoice_presentation_snapshot_batch.sql','4fbbd5b5ea3c3cdeaa2f8bc983125986c57db9bd4eaea42b7769354f5f182d1d','INVOICE_GATE'],
  ['repeatable/25072026_0003_invoice_presentation_runtime_authority.sql','7b4a28e9f6a327f2a00029ec41c6593b725245cfe9214856b9a4f4bc37ef1314','INVOICE_GATE'],
  ['repeatable/30072026_1216_invoice_timesheet_refs_ward_selective_numbering.sql','306489585aec541a38700bb7a9bca96b178fc291a644716a7079d45cf4a79747','INVOICE_GATE'],
  ['repeatable/27082026_1436_candidate_withdrawal_read_authority_v1.sql','7c60201f3e7754626e84b029add5a210e95e8e27b68011a098bd2d44777caf59','WITHDRAWAL'],
  ['repeatable/27082026_2004_candidate_weekly_withdrawal_null_target_v1.sql','bc833e10afe19ee25c73688b66b0eb29030c7fa610f0b4d403226106427a8710','WITHDRAWAL'],
  ['repeatable/28082026_1159_banking_pay_modal_structure_v2.sql','8bc91372695b7e4e3cd10020a63c98d46097775f25211f415e2f2f0048a1d938','MODAL'],
  ['repeatable/28082026_1308_banking_pay_modal_ready_members.sql','17230dedbd5e630b9412ec6d96db1f5441859394b04d6206ee381f31eab7145f','MODAL'],
  ['repeatable/28082026_1424_banking_pay_modal_candidate_selection_core.sql','7259dd4106e9fee997524c769eb7480fdca42f37dcf5aaef9789d6b6626c2a30','MODAL'],
  ['repeatable/28082026_1424_banking_pay_modal_selection_owner_bridge.sql','9626a3d392d6812583175b7d9cb43ba6bec753bb29e80048fba4eed2a9470f20','MODAL'],
  ['repeatable/28082026_1657_banking_pay_modal_payee_readiness_projection.sql','3636eb76745ecc7576d3e4fab8e28dd65bc36edbab6f7b9f4d37267f58b6086f','MODAL'],
  ['repeatable/28082026_1708_banking_pay_modal_bank_sources.sql','7c65853cc53b25cf7ccb963242d4b47248292b901a28cdb523e54fb0f781601a','MODAL'],
  ['repeatable/28082026_1915_banking_pay_modal_finance_tasks.sql','0e49983745f8fff6083a1018c9c4dc6128b017dd9f316588b2b02f65a4909d24','MODAL'],
  ['repeatable/28082026_1935_banking_pay_modal_bank_jobs.sql','ef379502ec5b3ab0d31bac96c36ba2dd70986a1de396af62b53d5376b3f39f03','MODAL'],
  ['repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql','48bc4bbbaa190e48034d6818165d683407d7bb6750eebdf495123ad8a208f6b6','FINGERPRINT'],
  ['repeatable/02092026_0325_candidate_paper_break_entry_v1.sql','34211673c6ad8439d50c7da1e1bab85778f507305f9d387a7266f139601c2991','WITHDRAWAL'],
  ['repeatable/03092026_1641_contract_settings_effective_authority_v1.sql','bd5ef8add0b2066e5b63a48f739dd8a6cd40e2643e6ddbdc12aaaaea0746a55d','SETTINGS'],
  ['repeatable/05092026_1200_banking_pay_draft_v8_final_authority_closure.sql','1cef768e9f97a22fd965834d6459363c383774ea50c979117d4bd16a1b33fcdb','FINGERPRINT'],
  ['repeatable/05092026_1712_legacy_extras_current_authority_repair_v1.sql','d22073c39a738e0959f8bbfbb661a6e91a37fb374b2f1dba4c83158727d29d43','EXTRAS'],
  ['repeatable/05092026_2350_timesheet_expense_presentation_release_closure.sql','6780e521b1257df86dba85599f2fe0d6f0330c648f3f8467365432e3dd065007','EXTRAS'],
  ['repeatable/06092026_0337_candidate_sequential_expense_manager_approval_gate_v1.sql','93d1b11ad5d75b897cc035906907ff61f63a0459f7a053f19402ffc3d6ee72c0','WITHDRAWAL'],
  ['repeatable/07092026_0205_candidate_weekly_break_entry_final_authority_v1.sql','0cb2c7ba9e4ac67428b6cdf064b5f37c88c19fd0e61f987782bc7f654657bcdf','WITHDRAWAL'],
  ['repeatable/07092026_0331_candidate_sent_paper_retirement_final_authority_v1.sql','4d794027444cd02adddfd8962a33b142b552b4d69fe0da97bbc1c3364d0f4b70','WITHDRAWAL'],
  ['repeatable/07092026_0611_candidate_invoice_current_authority_closure_v1.sql','cb6800b20a846be3d29bc9c4a5818fd645c8aa4dd73c7fb74dee27bc4115e1a3','INVOICE_SETTINGS'],
  ['repeatable/08092026_1615_invoice_correction_stream_parity_v1.sql','693c068fac56d4755e8a63664250097a994d3085e9cc32fd8a1f23fb24bfdcbf','INVOICE_SETTINGS'],
  ['repeatable/09092026_1500_candidate_duplicate_expense_anchor_final_authority_v1.sql','50abfe9d60a09cc645966a09d593b68c83de20e099b8207b5b8a61b917abaa90','WITHDRAWAL'],
  ['repeatable/09092026_1548_banking_pay_restored_authority_canonicalisation_v1.sql','e2a9edb74bfd6188be4a51863a9c38304858f8695cb62f333987998a85714e91','MODAL'],
  ['repeatable/09092026_1900_candidate_expense_update_submit_final_authority_v1.sql','4b7e0028ac224ee0c15d11a019f2e007b54501927f5b38ed6f96cb8c6db5ec4f','EXPENSE_REPAIR'],
  ['repeatable/22092026_1620_banking_pay_installed_owner_reassert_v1.sql','14447b519484557d69439246cc6b3e2c7efd0197b70432d29d36dafa7f4666a1','MODAL'],
  ['repeatable/26092026_0208_banking_pay_stage2_backfill_complete_v1.sql','346d43137bb2ca23de7988a774465204b728aa758d86931303b3604dda3e9de7','BACKFILL'],
  ['repeatable/28092026_1234_stage2_hosted_final_authority_reassert_v1.sql','e95320c23ee1236d4ff469412c157f334233bd1b4fe90877fdf5cbb24a6561db','SOURCE_REASSERT'],
  ['repeatable/06102026_1041_source_pending_order_canonical_reassert.sql','087ef13f52e24925527033115ceb7a127ce29f62e70cd440bce2dceee8eef7ec','SOURCE_REASSERT'],
  ['repeatable/03102026_0300_finalizer_instrumentation_after_authority_closures.sql','2c86099adcb66732872c56923b9eb1806967761af7a5a8912663360f020b6b81','INSTRUMENTATION'],
  ['repeatable/03102026_0600_stage2_plan_cache_after_h1h2_retry.sql','b7f5a59950b185313a9059733cde9f031fd6cdc9327b15bbbbaa7ed3044f21ba','PLAN_CACHE'],
];
export const REPLAY_REASONS = Object.freeze({
  INDEX_REBUILD: 'Only exclusively named concurrent indexes: original DROP IF EXISTS removes interrupted invalid builds; CREATE and final indisvalid/indisready assertions rerun. No data writes.',
  BACKFILL: 'Original procedure retains internal COMMIT per batch. Only missing/stale derived references and missing protected events are repaired; IS DISTINCT FROM and stable missing-event guards make committed batches replay no-ops. Trigger state restored before each commit. Native partial-batch/restart proof required; no amount/status/snapshot rewrite.',
  EXTRAS: 'Each committed stage replaces definitions/ACL; matching DROP IF EXISTS precedes trigger recreation. Conditional legacy overload drop verifies replacement then returns when absent. No caller execution or business-row writes.',
  PAYROLL_TRIGGERS: 'Two definition/ACL stages and conditional existing-table trigger recreation. DROP IF EXISTS precedes matching CREATE; notifications only reload schema. No payroll allocation or payment writes.',
  INVOICE_GATE: 'Exact original active-work gset/if gate retained. Included CROR definitions and ACL only. Checkpoint requires the captured invoice_presentation_active_work variable false, including the original successful NOTICE-only skipped-DDL branch. Busy cutover refuses instead of claiming applied.',
  WITHDRAWAL: 'Definition/ACL stages only, matching trigger DROP/recreate and indexes IF NOT EXISTS. Included paper-retirement source patch detects installed exact new strings before replacement and verifies old/new single occurrences. No withdrawal/retirement owner is invoked.',
  MODAL: 'Inspected included stages replace routines/ACL, matching trigger/overload DROP IF EXISTS and schema-reload notifications only. No workbench/payment owner is invoked and no business rows are changed.',
  FINGERPRINT: 'Definition stages replay as above. One DO repairs only missing/stale snooze natural_expiry_source_fingerprint from exact retained canonical digest in locked <=100 pages. Completed digest matches are no-ops; no amount/state writes.',
  SETTINGS: 'Definition/trigger stages replay. Backfill touches only missing settings_authority_json; planned-week refresh refuses actual Timesheets and returns on matching fingerprint. Quiet-window inputs remain stable; no hours/rates/TSFIN/finance changes.',
  INVOICE_SETTINGS: 'Retained settings repair as SETTINGS; invoice CREATE-without-OR routines have preceding matching DROP IF EXISTS in their original committed stage. Other stages replace definitions/ACL and readonly privilege guards; no invoice issue/delivery owner invoked.',
  EXPENSE_REPAIR: 'Definition/trigger stages replay. Reconcile selects only canonical expense amount/mileage mismatch; sync increments generation/emits stable ON CONFLICT component event only if changed, so a committed DO no longer qualifies. Separate unsent queued-manager-mail generation repair has IS DISTINCT FROM current approval generation. No mail send or payment execution.',
  SOURCE_REASSERT: 'Definition/ACL stages, exact notification trigger DROP IF EXISTS then CREATE and role ACL guards. No Source financial or notification caller executes.',
  INSTRUMENTATION: 'Definition stage then readonly exact function-hash guard; rerun replaces same body and rechecks same two allowed hashes. No financial writes.',
  PLAN_CACHE: 'Two stages set exact retained function configuration then readonly owner/configuration guards. Reapplying the same SET attributes is idempotent; no business writes.',
});
export const MANAGED_EXCEPTIONS = Object.freeze(rows.map(([file, sha256, reason]) => Object.freeze({
  path: `supabase/${file}`, sha256, reason, rationale: REPLAY_REASONS[reason],
})));

export function executionPolicy(closure) {
  const entry = MANAGED_EXCEPTIONS.find(row => row.path === closure.path);
  if (!entry) return null;
  if (entry.sha256 !== closure.closureHash) throw Error(`MANAGED_REPLAY_POLICY_HASH_MISMATCH: ${closure.path}`);
  return entry;
}
