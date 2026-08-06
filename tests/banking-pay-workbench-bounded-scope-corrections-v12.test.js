import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = path => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');
const dispatcher = read('supabase/repeatable/04082026_1213_pay_workbench_candidate_source_build_chunk.sql');
const projection = read('supabase/repeatable/04082026_2313_pay_workbench_unit_projection_v1.sql');
const occurrence = read('supabase/repeatable/04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql');
const effectNormaliser = read('supabase/repeatable/04082026_2315_pay_workbench_finance_effect_normalise_row_v1.sql');
const continuation = read('supabase/repeatable/04082026_1219_pay_workbench_enqueue_stage_continuation.sql');
const scopeSelector = read('supabase/repeatable/04082026_1144_pay_workbench_candidate_bounded_scope_v1.sql');
const closure = read('supabase/repeatable/04082026_1151_pay_workbench_timesheet_dependency_closure_v2.sql');
const entitlement = read('supabase/repeatable/04082026_1147_pay_current_timesheet_entitlement_components_from_build_v1.sql');
const syncCore = read('supabase/repeatable/04082026_1210_pay_sync_overpayments_from_workbench_workspace_v1.sql');
const activeSync = syncCore.replace(/\/\*[\s\S]*?\*\//g, '');
const transition = read('supabase/repeatable/04082026_1202_pay_workbench_financial_scope_dirty_transition_v1.sql');
const triggers = read('supabase/repeatable/04082026_1234_banking_pay_bounded_scope_triggers_v12.sql');
const claim = read('supabase/repeatable/04082026_1141_pay_workbench_source_build_attempt_claim_start_v1.sql');
const authority = read('supabase/repeatable/05082026_1229_pay_workbench_financial_source_authority_v1.sql');
const componentCore = read('supabase/repeatable/05082026_1230_pay_workbench_canonical_component_core_v1.sql');
const financePage = read('supabase/repeatable/05082026_1231_pay_workbench_finance_item_authority_page_v1.sql');
const financePageBundle = read('supabase/repeatable/05082026_1414_pay_workbench_finance_item_authority_page_bundle_v1.sql');
const occurrenceBundle = read('supabase/repeatable/05082026_1232_pay_workbench_unit_economic_occurrence_bundle_v1.sql');
const v128Migration = read('supabase/migrations/05082026_1227_banking_pay_bounded_scope_v128_operational_state.sql');
const deleteEligibility = read('supabase/repeatable/04082026_1219_candidate_delete_eligibility.sql');
const deleteApply = read('supabase/repeatable/04082026_1219_candidate_delete_apply.sql');
const markCandidate = read('supabase/repeatable/04082026_1219_pay_workbench_mark_candidate_dirty.sql');
const markFinance = read('supabase/repeatable/04082026_1219_pay_workbench_mark_finance_case_dirty.sql');
const correctionMigration = read('supabase/migrations/04082026_2042_banking_pay_bounded_scope_corrections.sql');

test('D1 sealing cursors are valid only for their owning stage', () => {
  assert.match(continuation, /WHEN 'PREPARE_SCOPE' THEN v_private_cursor_kind IN \('SCOPE_SELECT','SEED_SCOPE_SEAL'\)/);
  assert.match(continuation, /WHEN 'DEPENDENCY_CLOSURE' THEN v_private_cursor_kind IN \('DEPENDENCY_CLOSURE','DEPENDENCY_SCOPE_SEAL'\)/);
  assert.match(scopeSelector, /v_next_cursor:=jsonb_build_object\(\s*'cursor_kind','SCOPE_SELECT'/);
  assert.doesNotMatch(scopeSelector, /v_next_cursor:=jsonb_build_object\(\s*'cursor_kind','PREPARE_SCOPE'/);
});
test('D2 fact pages carry and verify a cumulative replay-safe chain', () => {
  assert.match(dispatcher, /v_next_cumulative_fact_count:=v_cumulative_fact_count\+v_page_count/);
  assert.match(dispatcher, /v_previous_page\.cursor_end_hash<>v_cursor_start_hash/);
  assert.match(dispatcher, /v_existing_page\.attempt_id IS DISTINCT FROM v_attempt_id/);
  assert.match(dispatcher, /v_existing_page\.cumulative_fact_count<>v_next_cumulative_fact_count/);
  assert.match(dispatcher, /expected_source_count IS DISTINCT FROM v_expected_source_count/);
  assert.match(dispatcher, /'source_exhausted',v_is_final/);
  const replayReturn = dispatcher.indexOf("'stage_status','PAGE_REPLAYED'");
  const counterMutation = dispatcher.indexOf('fact_row_count=scope_row.fact_row_count+');
  assert.ok(replayReturn >= 0 && counterMutation > replayReturn);
});
test('D3 physical live-input pages precede durable derived-family paging', () => {
  assert.match(dispatcher, /WITH scope_page AS MATERIALIZED/);
  assert.match(dispatcher, /LIMIT v_fact_limit\+1[\s\S]*LEFT JOIN LATERAL/);
  const fixedProjection = dispatcher.match(
    /WITH scope_page AS MATERIALIZED[\s\S]*?LEFT JOIN LATERAL \(([\s\S]*?)\) canonical ON true/i,
  );
  assert.ok(fixedProjection);
  assert.match(fixedProjection[1], /current_member\.is_current IS TRUE/i);
  assert.doesNotMatch(fixedProjection[1], /current_member\.(?:revoked_at|archived_at_utc)/i);
  assert.match(dispatcher, /COALESCE\(canonical\.timesheet_id,fallback_member\.timesheet_id\) AS projected_timesheet_id/i);
  assert.match(dispatcher, /fallback_scope\.build_id=v_build_id[\s\S]*fallback_scope\.dependency_unit_key=v_unit_key/i);
  assert.match(dispatcher, /ORDER BY fallback_scope\.stable_ordinal,fallback_row\.timesheet_id/i);
  assert.match(dispatcher, /'SEALED_FAMILY_FALLBACK_NO_CURRENT'/i);
  assert.match(dispatcher, /private\.pay_workbench_unit_economic_occurrence_bundle_v1\(/);
  assert.match(projection, /fallback_member AS \(/i);
  assert.match(projection, /COALESCE\(canonical\.canonical_timesheet_id,fallback_member\.fallback_timesheet_id\) AS projected_timesheet_id/i);
  assert.match(projection, /member\.stable_ordinal,member\.family_timesheet_id/i);
  assert.match(occurrence, /paged_raw_rows AS MATERIALIZED/);
  assert.match(occurrence, /generate_series\([\s\S]*v_limit-1/);
  assert.match(occurrence, /UPPER\(COALESCE\(canonical\.sheet_scope::text,''\)\)='DAILY'/);
  assert.match(occurrence, /standard_items_page AS MATERIALIZED/);
  assert.match(occurrence, /standard_items_page[\s\S]*LIMIT v_limit\+1/);
  assert.match(occurrence, /fallback_states AS MATERIALIZED[\s\S]*LIMIT v_limit\+1/);
  assert.match(occurrence, /paged_occurrence AS MATERIALIZED/);
  assert.match(occurrence, /WHERE reservation_row\.pay_batch_item_id=item\.id/);
  assert.match(occurrence, /pg_column_size\(COALESCE\(canonical\.additional_units_json[\s\S]*<=65536[\s\S]*jsonb_each/);
  assert.doesNotMatch(dispatcher, /_pay_current_timesheet_entitlement_components\s*\(/);
  assert.doesNotMatch(dispatcher, /_pay_active_settled_components\s*\(/);
  assert.match(occurrenceBundle, /raw_page_count/);
  assert.match(occurrenceBundle, /resolved_page_count/);
  assert.match(occurrenceBundle, /failed_page_count/);
  assert.match(occurrenceBundle, /raw_source_exhausted/);
  assert.match(dispatcher, /raw_page_evidence_digest/);
  assert.doesNotMatch(dispatcher, /\bMIN\s*\(\s*(?:projected_timesheet_id|projection\.timesheet_id)\s*\)/i);
  assert.match(dispatcher, /SELECT projection_ids\.projected_timesheet_id INTO v_next_projection_id[\s\S]*ORDER BY projection_ids\.projected_timesheet_id[\s\S]*LIMIT 1/);
  assert.match(dispatcher, /SELECT projection\.timesheet_id INTO v_next_projection_id[\s\S]*projection\.timesheet_id>v_input_projection_id[\s\S]*ORDER BY projection\.timesheet_id[\s\S]*LIMIT 1/);
});
test('D4 finance closure deduplicates frontier authority', () => {
  assert.match(closure, /SELECT DISTINCT frontier_component\.finance_case_id/);
  assert.match(closure, /SELECT DISTINCT frontier_component\.id,frontier_component\.finance_case_id/);
  assert.match(closure, /SELECT reservation_row\.id[\s\S]*UNION/);
  assert.match(closure, /LOOP[\s\S]*TRUNCATE pg_temp\._bpay_wb_closure_edge_page_v2[\s\S]*EXIT WHEN EXISTS\(SELECT 1 FROM pg_temp\._bpay_wb_closure_edge_page_v2\)[\s\S]*OR v_family>=10/);
  assert.match(closure, /v_family:=v_family\+1;\s*v_last_key:=NULL;\s*END LOOP;/);
  assert.match(closure, /IF v_used\+1\+\(CASE WHEN v_scope_exists THEN 0 ELSE 1 END\)>v_limit THEN EXIT; END IF;/);
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
  assert.doesNotMatch(activeSync, /from public\.pay_batch_items/i);
  assert.doesNotMatch(activeSync, /from public\.pay_advance_reservations/i);
  assert.match(syncCore, /fact\.fact_family in \('FROZEN_SETTLED_COMPONENT','RESERVATION_COMPONENT'\)/);
  assert.match(syncCore, /FULL OUTER JOIN actual USING\(timesheet_id,line_identity\)/);
  assert.match(syncCore, /economic_target AS/);
  const canonicalScopeFence = syncCore.slice(
    syncCore.lastIndexOf('IF EXISTS(', syncCore.indexOf('PAY_WORKBENCH_CANONICAL_FACT_SCOPE_INCOMPLETE')),
    syncCore.indexOf('PAY_WORKBENCH_CANONICAL_FACT_SCOPE_INCOMPLETE'),
  );
  assert.match(canonicalScopeFence, /pg_temp\.tmp_sync_authoritative_components component/i);
  assert.doesNotMatch(canonicalScopeFence, /source_relation='UNIT_PROJECTION'/i);
  assert.doesNotMatch(canonicalScopeFence, /economic_build_scope scope_row/i);
  assert.match(syncCore, /INSERT INTO pg_temp\.timesheet_case_rollup_effective\(/i);
  assert.match(syncCore, /JOIN pg_temp\.ts_baseline baseline/i);
  assert.match(syncCore, /PAY_WORKBENCH_CANONICAL_PRESENTATION_METADATA_INCOMPLETE/i);
  assert.match(syncCore, /UNIT_PROJECTION also contains physical[\s\S]*dependency evidence[\s\S]*must not invent a presentation row/i);
  assert.match(syncCore, /DELETE FROM pg_temp\.timesheet_case_rollup_effective preview_row[\s\S]*NOT EXISTS\(SELECT 1 FROM pg_temp\.tmp_sync_authoritative_components component/i);
  assert.match(syncCore, /'economic_authority','SEALED_ECONOMIC_BUILD_FACTS'/i);
  assert.doesNotMatch(syncCore, /fact_truth AS \([\s\S]*?GROUP BY target\.timesheet_id\s+HAVING/i);
  assert.match(syncCore, /v_bounded_economic_component_digest[\s\S]*FROM pg_temp\.tmp_sync_authoritative_components component/i);
  assert.match(syncCore, /PAY_WORKBENCH_CANONICAL_PRESENTATION_ALLOCATION_MISMATCH/);
  assert.match(syncCore, /private\.pay_workbench_canonical_component_core_v1/);
  assert.match(syncCore, /GROUP BY timesheet_id,line_identity,key_type,key_value/);
  assert.match(syncCore, /OR count\(\*\)>1/);
  assert.match(syncCore, /SELECT DISTINCT timesheet_id,key_type,key_value,component_core/);
  assert.match(syncCore, /economic_component_digest/);
  assert.match(componentCore, /source_basis_identity/);
  assert.match(syncCore, /PAY_WORKBENCH_CANONICAL_LINE_IDENTITY_CONFLICT/);
  assert.doesNotMatch(syncCore, /actual_line_count,0\)>1/);
  assert.ok((syncCore.match(/private\.pay_workbench_timesheet_input_fingerprint_v1/g) || []).length >= 2);
  assert.match(syncCore, /CREATE TEMP TABLE pg_temp\._bpay_wb_pre_sync_freshness_v1\(/);
  assert.match(syncCore, /pre_sync\.input_fingerprint IS DISTINCT FROM scope_row\.captured_input_fingerprint/);
  assert.match(syncCore, /current_fingerprint\.revision_json-'pay_state_digest'[\s\S]*pre_sync\.revision_json-'pay_state_digest'/);
  for (const field of ['last_settled_snapshot_json', 'last_settled_signature', 'last_settled_pay_batch_id', 'last_settled_at_utc']) {
    assert.ok((syncCore.match(new RegExp(`'${field}'`, 'g')) || []).length >= 2, `${field} must be fenced before and after finance DML`);
  }
  assert.match(syncCore, /settled_authority_digest[\s\S]*PAY_WORKBENCH_RECONCILIATION_ATTEMPT_STALE/);
});
test('D7 pay-batch update and delete have statement backstops', () => {
  assert.match(triggers, /trg_bpay_wb_batch_items_update_dirty_v1\s+AFTER UPDATE ON public\.pay_batch_items/);
  assert.match(triggers, /trg_bpay_wb_batch_items_delete_dirty_v1\s+AFTER DELETE ON public\.pay_batch_items/);
  assert.match(transition, /TG_TABLE_NAME='pay_batch_items'[\s\S]*frozen_component_snapshot_json[\s\S]*FROM old_rows[\s\S]*FROM new_rows/);
});
test('D8 fallback snapshots become typed facts with active precedence', () => {
  assert.doesNotMatch(occurrence, /_pay_timesheet_components\(/);
  assert.match(occurrence, /segment_occurrence AS/);
  assert.match(occurrence, /additional_occurrence AS/);
  assert.match(occurrence, /expense_occurrence AS/);
  assert.match(occurrence, /adjustment_occurrence AS/);
  assert.match(occurrence, /evidence_occurrence AS/);
  assert.match(occurrence, /resolution_failure/);
  assert.match(dispatcher, /PAY_WORKBENCH_ECONOMIC_KEY_RESOLUTION_INCOMPLETE/);
  assert.match(dispatcher, /fact_family='FROZEN_SETTLED_COMPONENT'[\s\S]*NOT EXISTS/);
  assert.match(occurrence, /FALLBACK_SEGMENT_NOT_OBJECT/);
  assert.match(occurrence, /FALLBACK_ADJUSTMENT_AMOUNT_INVALID/);
  assert.match(occurrence, /FALLBACK_SNAPSHOT_EXCEEDS_ENVELOPE/);
  assert.match(authority, /pg_input_is_valid\(value_text,'uuid'\)/);
  assert.match(authority, /_OWNER_CONFLICT/);
  assert.match(authority, /_CANDIDATE_CONFLICT/);
  assert.match(authority, /_FINANCE_CASE_CONFLICT/);
  assert.match(dispatcher, /reservation\.frozen_source_basis_json/);
  assert.match(dispatcher, /reservation\.frozen_component_snapshot_json/);
  assert.match(dispatcher, /RESERVATION_TS_DAY_INVALID/);
  assert.match(occurrence, /pg_input_is_valid/);
});
test('D9 bootstrap CLOSED requires zero-difference proof', () => {
  assert.match(dispatcher, /private\.pay_current_timesheet_entitlement_components_from_build_v1\(\s*v_build_id,v_bootstrap_unit_key\)/);
  assert.doesNotMatch(dispatcher, /_pay_current_timesheet_entitlement_components\(ARRAY\[page\.timesheet_id\]/);
  assert.match(dispatcher, /v_bootstrap_unit_relevant:=v_bootstrap_unit_relevant OR v_bootstrap_page_relevant/);
  assert.match(dispatcher, /economic_state=CASE WHEN v_bootstrap_unit_relevant THEN 'DIRTY' ELSE 'CLOSED' END/);
  assert.match(dispatcher, /raw_current_occurrence_count/);
  assert.match(dispatcher, /resolved_baseline_occurrence_count/);
  assert.match(dispatcher, /banking_pay_workbench_economic_build_fact_pages page/);
  assert.match(dispatcher, /page\.expected_source_count=\(page\.cursor_end_json->>'raw_physical_source_count'\)::integer/);
  assert.match(dispatcher, /PAY_WORKBENCH_BOOTSTRAP_CLOSED_AUTHORITY_UNPROVED/);
});
test('D10 global facts are active and scoped', () => {
  assert.match(dispatcher, /COALESCE\(reservation\.status,''\)\)\) IN \('RESERVED','COMMITTED'\)/);
  assert.match(dispatcher, /reservation\.released_at_utc IS NULL/);
  assert.match(dispatcher, /JOIN private\.banking_pay_workbench_economic_build_facts case_fact[\s\S]*case_fact\.fact_family='FINANCE_CASE_IDENTITY'/);
  assert.match(dispatcher, /event\.id=\([\s\S]*SELECT event_latest\.id[\s\S]*LIMIT 1/);
  assert.match(dispatcher, /ACTIVE_CORRECTION_CHAIN_BATCH_ITEM/);
  assert.doesNotMatch(dispatcher, /upper\(btrim\(COALESCE\(event\.reason,''\)\)\) IN \([\s\S]{0,100}'PREVIEW_FINANCE_SYNC'/);
});
test('D11 claim and recovery use durable keyset progress before exact row locks', () => {
  assert.match(claim, /v_scan_limit integer:=50/);
  assert.match(claim, /LIMIT v_scan_limit/);
  assert.match(claim, /claim_scan_deferral_count/);
  assert.match(claim, /recovery_scan_deferral_count/);
  assert.match(claim, /claim_scan_generation/);
  assert.match(claim, /recovery_scan_generation/);
  assert.match(claim, /banking_pay_workbench_queue_scan_state/);
  assert.match(claim, /cursor_object_id=v_claim\.id/);
  assert.ok(claim.indexOf('cursor_object_id=v_claim.id') < claim.indexOf('FOR UPDATE OF claimed_job SKIP LOCKED'));
  assert.match(claim, /ORDER BY scan_generation,chain_rank/);
  assert.match(claim, /ORDER BY scan_generation,attempt\.lease_expires_at_utc/);
  assert.match(claim, /WHERE id=v_recovery\.job_id FOR UPDATE SKIP LOCKED/);
  assert.match(claim, /WHERE id=v_recovery\.attempt_id FOR UPDATE SKIP LOCKED/);
  assert.match(v128Migration, /PRIMARY KEY \(lane_identity,scan_kind,scan_scope_key\)/);
  assert.match(claim, /GREATEST\(1,power\(2,LEAST\(v_recovery\.attempt_count,8\)\)/);
  assert.match(claim, /-'claim_scan_deferred_reason'-'claim_scan_deferral_count'-'claim_scan_generation'/);
  assert.equal((claim.match(/'result_code','LANE_SCAN_BUSY'/g) || []).length, 2);
  assert.match(claim, /scan_kind='RECOVERY'[\s\S]{0,180}FOR UPDATE SKIP LOCKED/);
  assert.match(claim, /scan_kind='CLAIM'[\s\S]{0,180}FOR UPDATE SKIP LOCKED/);
  assert.match(claim, /IF v_claim\.serial_blocked THEN[\s\S]{0,360}CONTINUE;\s+END IF/);
  assert.doesNotMatch(claim, /IF v_claim\.serial_blocked THEN[\s\S]{0,1200}UPDATE public\.banking_pay_workbench_jobs/);
  assert.doesNotMatch(claim, /IF NOT pg_catalog\.pg_try_advisory_xact_lock[\s\S]{0,1200}UPDATE public\.banking_pay_workbench_jobs/);
});

test('V1.2.8 physical additional identities are lossless and live dates fail closed', () => {
  assert.match(occurrence, /octet_length\(additional\.key\)/);
  assert.match(occurrence, /encode\(convert_to\(additional\.key,'UTF8'\),'hex'\)/);
  assert.match(occurrence, /FALLBACK_ADDITIONAL_CODE_MISSING/);
  assert.match(occurrence, /SEGMENT_DATE_INVALID/);
  assert.match(occurrence, /pg_input_is_valid\(segment\.value->>'date','date'\)/);
});

test('V1.2.9 finance movement discovery bounds every scope and safety branch before authority work', () => {
  assert.match(financePage, /scope_timesheet AS MATERIALIZED/);
  assert.match(financePage, /direct_owner_item AS MATERIALIZED/);
  assert.match(financePage, /component_owner_item AS MATERIALIZED/);
  assert.match(financePage, /case_owner_item AS MATERIALIZED/);
  assert.match(financePage, /frozen_owner_item AS MATERIALIZED/);
  assert.match(financePage, /candidate_settled_item AS MATERIALIZED/);
  assert.match(financePage, /candidate_transfer_item AS MATERIALIZED/);
  assert.match(financePage, /candidate_reservation_item AS MATERIALIZED/);
  assert.match(financePage, /candidate_item_key AS MATERIALIZED/);
  assert.match(financePage, /item_page AS MATERIALIZED/);
  assert.doesNotMatch(financePage, /relevant_item_id AS MATERIALIZED/);
  assert.match(financePageBundle, /raw_page_count/);
  assert.match(financePageBundle, /raw_source_exhausted/);
  assert.match(financePageBundle, /evidence_digest/);
  assert.match(financePage, /frozen_source_basis_json @> jsonb_build_object/);
  assert.match(dispatcher, /'FINANCE_ITEM_AUTHORITY'/);
  assert.doesNotMatch(activeSync, /legacy_finance_components_v127 AS/);
});

test('V1.2.7 parent-child page boundaries retain every physical occurrence', () => {
  for (const occurrenceCount of [24, 25, 26, 49, 50, 51, 75, 76, 100, 101]) {
    const sourceKeys = Array.from({ length: occurrenceCount }, (_, index) =>
      `20:projected:item-${String(Math.floor(index / 3)).padStart(4, '0')}:${String((index % 3) + 1).padStart(8, '0')}`);
    const accepted = [];
    let cursor = null;
    while (true) {
      const lookahead = sourceKeys.filter(key => cursor === null || key > cursor).slice(0, 26);
      accepted.push(...lookahead.slice(0, 25));
      if (lookahead.length <= 25) break;
      cursor = lookahead[24];
    }
    assert.deepEqual(accepted, sourceKeys, `physical occurrence loss at count ${occurrenceCount}`);
  }
});

test('V1.2.7 durable scan generations reach an eligible row beyond blocked prefixes', () => {
  for (const blockedCount of [50, 100]) {
    const jobs = Array.from({ length: blockedCount + 1 }, (_, index) => ({
      id: index + 1,
      blocked: index < blockedCount,
      generation: 0,
    }));
    let claimed = null;
    for (let call = 0; call < 5 && claimed === null; call += 1) {
      const page = [...jobs].sort((a, b) => a.generation - b.generation || a.id - b.id).slice(0, 50);
      for (const job of page) {
        if (job.blocked) job.generation += 1;
        else { claimed = job.id; break; }
      }
    }
    assert.equal(claimed, blockedCount + 1);
  }
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
  assert.match(markCandidate, /v_delete_context regclass := pg_catalog\.to_regclass\('pg_temp\._bpay_candidate_delete_context_v1'\)/);
  assert.match(markCandidate, /EXECUTE \$delete_context\$[\s\S]*FROM pg_temp\._bpay_candidate_delete_context_v1 context[\s\S]*\$delete_context\$ INTO v_delete_context_suppressed/);
  assert.doesNotMatch(markCandidate, /AND EXISTS\(\s*SELECT 1 FROM pg_temp\._bpay_candidate_delete_context_v1 context/);
});
test('D15 expected effects are sealed then independently observed', () => {
  assert.match(correctionMigration, /'EXPECTED_FINANCE_EFFECT'/);
  assert.match(effectNormaliser, /GENERATED_NON_NULL/);
  assert.match(effectNormaliser, /PAY_WORKBENCH_EXPECTED_EFFECT_LIFECYCLE_TIMESTAMP_MISMATCH/);
  assert.match(markCandidate, /private\.pay_workbench_finance_effect_normalise_row_v1/);
  assert.match(markFinance, /private\.pay_workbench_finance_effect_normalise_row_v1/);
  assert.match(transition, /private\.pay_workbench_finance_effect_normalise_row_v1/);
  assert.match(transition, /FROM new_rows r JOIN old_rows old_row ON old_row\.id=r\.id/);
  assert.match(markFinance, /v_trigger_table='pay_finance_case_events'[\s\S]*component\.linked_timesheet_id/);
  assert.match(dispatcher, /set_config\('cloudtms\.pay_workbench_effect_capture_mode','capture',true\)/);
  assert.match(dispatcher, /effect_plan_digest/);
  assert.match(dispatcher, /logical_source_id/);
  assert.match(dispatcher, /identified\.raw_effect->>'relation_name'='pay_advances'[\s\S]{0,80}THEN identified\.logical_source_id/);
  assert.match(syncCore, /fact\.fact_family='EXPECTED_FINANCE_EFFECT'/);
  assert.match(syncCore, /_bpay_wb_effect_identity_map/);
  assert.match(syncCore, /actual_source_id/);
  assert.match(syncCore, /proposed IS NOT TRUE OR observed IS NOT TRUE/);
  assert.match(transition, /expected\.proposed IS TRUE[\s\S]*expected\.observed IS NOT TRUE/);
  assert.equal((triggers.match(/CREATE TRIGGER trg_bpay_wb_observe_/g) || []).length, 9);
});

