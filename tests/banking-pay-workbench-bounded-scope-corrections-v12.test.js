import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = path => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');
const dispatcher = read('supabase/repeatable/04082026_1213_pay_workbench_candidate_source_build_chunk.sql');
const continuation = read('supabase/repeatable/04082026_1219_pay_workbench_enqueue_stage_continuation.sql');
const closure = read('supabase/repeatable/04082026_1151_pay_workbench_timesheet_dependency_closure_v2.sql');
const entitlement = read('supabase/repeatable/04082026_1147_pay_current_timesheet_entitlement_components_from_build_v1.sql');
const syncCore = read('supabase/repeatable/04082026_1210_pay_sync_overpayments_from_workbench_workspace_v1.sql');
const activeSync = syncCore.replace(/\/\*[\s\S]*?\*\//g, '');
const transition = read('supabase/repeatable/04082026_1202_pay_workbench_financial_scope_dirty_transition_v1.sql');
const triggers = read('supabase/repeatable/04082026_1234_banking_pay_bounded_scope_triggers_v12.sql');
const claim = read('supabase/repeatable/04082026_1141_pay_workbench_source_build_attempt_claim_start_v1.sql');
const deleteEligibility = read('supabase/repeatable/04082026_1219_candidate_delete_eligibility.sql');
const deleteApply = read('supabase/repeatable/04082026_1219_candidate_delete_apply.sql');
const markCandidate = read('supabase/repeatable/04082026_1219_pay_workbench_mark_candidate_dirty.sql');
const correctionMigration = read('supabase/migrations/04082026_2042_banking_pay_bounded_scope_corrections.sql');

test('D1 sealing cursors are valid only for their owning stage', () => {
  assert.match(continuation, /WHEN 'PREPARE_SCOPE' THEN v_private_cursor_kind IN \('SCOPE_SELECT','SEED_SCOPE_SEAL'\)/);
  assert.match(continuation, /WHEN 'DEPENDENCY_CLOSURE' THEN v_private_cursor_kind IN \('DEPENDENCY_CLOSURE','DEPENDENCY_SCOPE_SEAL'\)/);
});
test('D2 fact pages carry and verify a cumulative replay-safe chain', () => {
  assert.match(dispatcher, /v_next_cumulative_fact_count:=v_cumulative_fact_count\+v_page_count/);
  assert.match(dispatcher, /v_previous_page\.cursor_end_hash<>v_cursor_start_hash/);
  assert.match(dispatcher, /v_existing_page\.attempt_id IS DISTINCT FROM v_attempt_id/);
  assert.match(dispatcher, /v_existing_page\.cumulative_fact_count<>v_next_cumulative_fact_count/);
  const replayReturn = dispatcher.indexOf("'stage_status','PAGE_REPLAYED'");
  const counterMutation = dispatcher.indexOf('fact_row_count=scope_row.fact_row_count+');
  assert.ok(replayReturn >= 0 && counterMutation > replayReturn);
});
test('D3 physical live-input pages precede durable derived-family paging', () => {
  assert.match(dispatcher, /v_unit_families text\[\]:=ARRAY\[\s*'LIVE_ENTITLEMENT_INPUT','FROZEN_SETTLED_COMPONENT','PAY_STATE_FALLBACK'/);
  assert.match(dispatcher, /ELSIF v_fact_family='LIVE_ENTITLEMENT_INPUT'[\s\S]*LIMIT v_fact_limit\+1/);
});
test('D4 finance closure deduplicates frontier authority', () => {
  assert.match(closure, /SELECT DISTINCT frontier_component\.finance_case_id/);
  assert.match(closure, /SELECT DISTINCT frontier_component\.id,frontier_component\.finance_case_id/);
  assert.match(closure, /SELECT reservation_row\.id[\s\S]*UNION/);
});
test('D5 component digests are derived internally', () => {
  assert.match(syncCore, /p_mismatch_choices:=[\s\S]{0,220}- 'overpayment_sync_negative_component_digest'/);
  assert.match(syncCore, /v_authoritative_negative_component_digest/);
  assert.match(syncCore, /v_authoritative_settled_baseline_digest/);
});
test('D6 reconciliation consumes sealed facts and fences live metadata', () => {
  assert.match(entitlement, /fact\.fact_family='ENTITLEMENT_COMPONENT'/);
  assert.doesNotMatch(activeSync, /_pay_current_timesheet_entitlement_components\s*\(/);
  assert.doesNotMatch(activeSync, /_pay_reserved_components\s*\(/);
  assert.doesNotMatch(activeSync, /_pay_active_settled_components\s*\(/);
  assert.ok((syncCore.match(/private\.pay_workbench_timesheet_input_fingerprint_v1/g) || []).length >= 2);
});
test('D7 pay-batch update and delete have statement backstops', () => {
  assert.match(triggers, /trg_bpay_wb_batch_items_update_dirty_v1\s+AFTER UPDATE ON public\.pay_batch_items/);
  assert.match(triggers, /trg_bpay_wb_batch_items_delete_dirty_v1\s+AFTER DELETE ON public\.pay_batch_items/);
  assert.match(transition, /TG_TABLE_NAME='pay_batch_items'[\s\S]*frozen_component_snapshot_json[\s\S]*FROM old_rows[\s\S]*FROM new_rows/);
});
test('D8 fallback snapshots become typed facts with active precedence', () => {
  assert.match(dispatcher, /_pay_timesheet_components\([\s\S]*WITH ORDINALITY/);
  assert.match(dispatcher, /fact_family='FROZEN_SETTLED_COMPONENT'[\s\S]*NOT EXISTS/);
});
test('D9 bootstrap CLOSED requires zero-difference proof', () => {
  assert.match(dispatcher, /_pay_current_timesheet_entitlement_components\(ARRAY\[page\.timesheet_id\]::uuid\[\]\)/);
  assert.match(dispatcher, /v_bootstrap_unit_relevant:=v_bootstrap_unit_relevant OR v_bootstrap_page_relevant/);
  assert.match(dispatcher, /economic_state=CASE WHEN v_bootstrap_unit_relevant THEN 'DIRTY' ELSE 'CLOSED' END/);
});
test('D10 global facts are active and scoped', () => {
  assert.match(dispatcher, /COALESCE\(reservation\.status,''\)\)\) IN \('RESERVED','COMMITTED'\)/);
  assert.match(dispatcher, /reservation\.released_at_utc IS NULL/);
  assert.match(dispatcher, /JOIN private\.banking_pay_workbench_economic_build_facts case_fact[\s\S]*case_fact\.fact_family='FINANCE_CASE_IDENTITY'/);
});
test('D11 claim and recovery use bounded overfetch', () => {
  assert.match(claim, /v_scan_limit integer:=50/);
  assert.match(claim, /LIMIT v_scan_limit/);
  assert.match(claim, /CONTINUE WHEN NOT pg_catalog\.pg_try_advisory_xact_lock/);
});
test('D12 cleanup has a finite terminal state', () => {
  for (const phase of ['CANONICAL_STAGE', 'FACT_PAGES', 'FACTS', 'SCOPE', 'ATTEMPTS', 'HEADER_FINALISE', 'COMPLETE']) assert.match(dispatcher, new RegExp(`'${phase}'`));
  assert.match(dispatcher, /v_cleanup_kind='COMPLETE'[\s\S]*'has_more',false/);
});
test('D13 terminal DEAD jobs detach before deletion', () => {
  assert.match(deleteEligibility, /status NOT IN \('SUCCEEDED','FAILED','DEAD'\)/);
  assert.match(deleteApply, /status IN \('SUCCEEDED','FAILED','DEAD'\)/);
});
test('D14 cascade suppression resolves child-row ownership', () => {
  assert.match(markCandidate, /v_trigger_table='candidates'[\s\S]*v_delete_owner_candidate_id:=\(v_old_row->>'id'\)::uuid/);
  assert.match(markCandidate, /v_old_row->>'candidate_id'[\s\S]*v_delete_owner_candidate_id:=\(v_old_row->>'candidate_id'\)::uuid/);
  assert.match(markCandidate, /pg_backend_pid\(\)[\s\S]*txid_current\(\)/);
});
test('D15 expected effects are sealed then independently observed', () => {
  assert.match(correctionMigration, /'EXPECTED_FINANCE_EFFECT'/);
  assert.match(dispatcher, /set_config\('cloudtms\.pay_workbench_effect_capture_mode','capture',true\)/);
  assert.match(dispatcher, /effect_plan_digest/);
  assert.match(syncCore, /fact\.fact_family='EXPECTED_FINANCE_EFFECT'/);
  assert.match(syncCore, /proposed IS NOT TRUE OR observed IS NOT TRUE/);
  assert.match(transition, /expected\.proposed IS TRUE[\s\S]*expected\.observed IS NOT TRUE/);
  assert.equal((triggers.match(/CREATE TRIGGER trg_bpay_wb_observe_/g) || []).length, 9);
});

