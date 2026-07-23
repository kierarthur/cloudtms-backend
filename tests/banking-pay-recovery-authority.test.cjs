const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const sqlPath = path.resolve(__dirname, '../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
const sql = fs.readFileSync(sqlPath, 'utf8');
const sourceBuildSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_39_pay_workbench_candidate_source_build_chunk.sql'),
  'utf8'
);
const overpaymentSyncSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_40_pay_sync_overpayments_from_preview.sql'),
  'utf8'
);
const correctionRuntimeSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_00b_import_correction_runtime_guards.sql'),
  'utf8'
);
const correctionResidualSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql'),
  'utf8'
);
const correctionPlpgsqlGuardSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/23072026_1217_disable_plpgsql_check_for_correction_chain_banking.sql'),
  'utf8'
);
const financeAdjustmentSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql'),
  'utf8'
);
const workerSource = fs.readFileSync(path.resolve(__dirname, '../broker/src/index.js'), 'utf8');

function functionBody(name, nextName, source = sql) {
  const functionMarker = `CREATE OR REPLACE FUNCTION public.${name}`;
  const start = source.indexOf(functionMarker);
  assert.ok(start >= 0, `${name} must exist`);
  const end = nextName
    ? source.indexOf(`CREATE OR REPLACE FUNCTION public.${nextName}`, start + functionMarker.length)
    : source.indexOf('CREATE OR REPLACE FUNCTION public.', start + functionMarker.length);
  return source.slice(start, end > start ? end : source.length);
}

test('finance recovery allocation uses central outstanding-component authority', () => {
  const body = functionBody('pay_preview_candidate_build_finance_case_baseline', 'pay_preview_candidate_build_finance_case_rows');
  const resetIndex = body.indexOf('drop table if exists pg_temp.candidate_authoritative_recovery_headroom');
  const authorityIndex = body.indexOf('candidate_authoritative_recovery_headroom');
  const allocationIndex = body.indexOf('create temporary table finance_case_recovery_rows_base');

  assert.ok(resetIndex >= 0 && authorityIndex >= resetIndex && allocationIndex > authorityIndex, 'repeatable authoritative recovery headroom must be rebuilt before allocation');
  assert.match(body, /public\._pay_outstanding_components\([\s\S]*rts\.timesheet_ids[\s\S]*null::uuid/);
  assert.match(body, /sum\(greatest\(coalesce\(oc\.outstanding_ex_vat, 0\), 0\)\)/);

  const allocationBlock = body.slice(allocationIndex, body.indexOf('create temporary table manual_debt_recovery_rows', allocationIndex));
  assert.match(allocationBlock, /carh\.authoritative_recovery_headroom_ex/);
  assert.doesNotMatch(allocationBlock, /cr\.non_mismatch_total_ex/);
});

test('Banking Pay admits active finance cases only', () => {
  const contextBody = functionBody('pay_preview_build_context', 'pay_preview_candidate_build_canonical_lines');
  const contextFinanceStart = contextBody.indexOf('FROM public.v_finance_cases_register AS finance_case_row');
  assert.ok(contextFinanceStart >= 0, 'candidate context finance-case scope must be present');
  const contextFinanceBlock = contextBody.slice(contextFinanceStart, contextBody.indexOf('ORDER BY candidate_scope_row.id', contextFinanceStart));
  assert.match(contextFinanceBlock, /UPPER\(COALESCE\(finance_case_row\.status::text, ''\)\) = 'ACTIVE'/);
  assert.doesNotMatch(contextFinanceBlock, /'ACTIVE', 'PAUSED'/);

  const collectBody = functionBody('pay_preview_candidate_collect_scope', 'pay_workbench_session_open');
  const baselineStart = collectBody.indexOf('create temporary table finance_case_baseline_scope');
  const baselineEnd = collectBody.indexOf('create temporary table manual_adjustment_carry_forward_scope', baselineStart);
  assert.ok(baselineStart >= 0 && baselineEnd > baselineStart, 'candidate finance-case baseline scope must be present');
  const baselineBlock = collectBody.slice(baselineStart, baselineEnd);
  assert.match(baselineBlock, /upper\(coalesce\(vfcr\.status::text,''\)\) = 'ACTIVE'/);
  assert.doesNotMatch(baselineBlock, /'ACTIVE', 'PAUSED'/);
});

test('candidate-filtered Banking Pay scope excludes unrelated finance-case candidates', () => {
  const contextBody = functionBody('pay_preview_build_context', 'pay_preview_candidate_build_canonical_lines');
  const pageStart = contextBody.indexOf('page_rows AS (');
  const pageEnd = contextBody.indexOf('LIMIT (v_page_limit + 1)', pageStart);
  assert.ok(pageStart >= 0 && pageEnd > pageStart, 'candidate page scope must be present');

  const pageBlock = contextBody.slice(pageStart, pageEnd);
  const hardBoundaryIndex = pageBlock.search(
    /v_effective_candidate_id IS NULL\s+OR candidate_scope_row\.id = v_effective_candidate_id/
  );
  const eligibilityIndex = pageBlock.search(
    /AND \(\s+\(\s+v_effective_candidate_id IS NOT NULL/
  );
  const financeCaseIndex = pageBlock.indexOf('FROM public.v_finance_cases_register AS finance_case_row');

  assert.ok(hardBoundaryIndex >= 0, 'selected candidate must be an outer scope boundary');
  assert.ok(eligibilityIndex > hardBoundaryIndex, 'eligibility alternatives must remain inside the selected-candidate boundary');
  assert.ok(financeCaseIndex > eligibilityIndex, 'finance-case eligibility must not bypass the selected-candidate boundary');
});

test('draft staging freezes line-level recovery component identity into a single nested component', () => {
  const start = sql.toLowerCase().indexOf(
    'create or replace function public.pay_batch_stage_operation_candidate_chunk_context'
  );
  const end = sql.toLowerCase().indexOf('create or replace function public.', start + 80);
  assert.ok(start >= 0, 'pay_batch_stage_operation_candidate_chunk_context must exist');
  const body = sql.slice(start, end > start ? end : sql.length);
  assert.match(
    body,
    /line_type', ''\)\)\) in \('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT'\)/
  );
  assert.match(body, /jsonb_array_length\(coalesce\(line_element\.value->'case_components'/);
  assert.match(body, /'finance_component_id', line_element\.value->>'finance_component_id'/);
  assert.match(
    body,
    /line_element\.value#>>'\{frozen_component_snapshot_json,source_pay_method\}'/
  );
  assert.match(body, /'frozen_source_amount'/);
});

test('finance adjustment draft creation treats signed recovery component amounts as magnitudes', () => {
  const body = functionBody('pay_batch_apply_finance_adjustments', null, financeAdjustmentSql);
  assert.match(
    body,
    /preview_due_amount_ex_vat',''\) ~ '\^-\?\\d\+\(\\\.\\d\+\)\?\$' then abs\(\(comp\.comp_json->>'preview_due_amount_ex_vat'\)::numeric\)/
  );
  assert.match(
    body,
    /target_pay_ex_vat',''\) ~ '\^-\?\\d\+\(\\\.\\d\+\)\?\$' then abs\(\(comp\.comp_json->>'target_pay_ex_vat'\)::numeric\)/
  );
  assert.match(body, /source_amount_ex_vat',''\)[\s\S]*abs\(\(comp\.comp_json->>'source_amount_ex_vat'\)::numeric\)/);
  assert.match(body, /'OVERPAYMENT_RECOVERY' as item_type[\s\S]*\(-fa\.take_target_ex\)::numeric\(12,2\)/);
  assert.match(body, /OPERATION_ALLOCATION_ROWS_NOT_LINKED/);
});

test('canonical finance rows expose scheduled and current-run recovery amounts separately', () => {
  const body = functionBody('pay_preview_candidate_build_canonical_lines', 'pay_preview_candidate_build_summary_fragment');
  assert.match(body, /'nominal_due_amount_ex_vat', round\(coalesce\(fcl\.nominal_due_amount_ex_vat, 0\), 2\)/);
  assert.match(body, /'recoverable_this_pay_run_ex_vat', round\(greatest\(coalesce\(fcl\.due_amount_ex_vat, 0\), 0\), 2\)/);
  assert.match(body, /'next_due_week_start', case when fcl\.next_due_week_start is null then null else fcl\.next_due_week_start::text end/);
});

test('source-build attestation accepts complete durable or protected coverage only', () => {
  const body = functionBody('pay_workbench_candidate_source_build_chunk', null, sourceBuildSql);
  const attestationStart = body.indexOf('A successful reconciliation need not echo a component');
  assert.ok(attestationStart >= 0, 'durable coverage attestation must be present');
  const attestationBlock = body.slice(attestationStart, body.indexOf('v_sync_result_code :=', attestationStart));

  assert.match(attestationBlock, /v_sync_uncovered_component_count, 0\) = 0/);
  assert.match(attestationBlock, /v_sync_durable_component_count, 0\)[\s\S]*v_sync_protected_component_count, 0\)[\s\S]*v_sync_negative_component_count, 0\)/);
  assert.match(attestationBlock, /IF COALESCE\(v_sync_candidate_covered, false\) IS NOT TRUE THEN/);
  assert.doesNotMatch(attestationBlock, /OR COALESCE\(v_sync_uncovered_component_count/);
});

test('active-reservation protection audit events are idempotent', () => {
  const body = functionBody('pay_sync_overpayments_from_preview', null, overpaymentSyncSql);

  assert.match(body, /protection_event\.event_type = 'SYNC_SHRINK_DEFERRED_ACTIVE_RESERVATION'[\s\S]*protection_event\.before_json IS NOT DISTINCT FROM v_case_before_json[\s\S]*protection_event\.after_json = jsonb_build_object/);
  assert.match(body, /protection_event\.event_type = 'SYNC_CLEAR_DEFERRED_ACTIVE_RESERVATION'[\s\S]*protection_event\.before_json = jsonb_build_object[\s\S]*protection_event\.after_json = jsonb_build_object/);
});


test('targeted correction refresh expands to the complete correction chain before residual materialisation', () => {
  const body = functionBody('pay_workbench_candidate_source_build_chunk', null, sourceBuildSql);
  const expansionStart = body.indexOf('A targeted refresh of one import-authoritative correction leg must rebuild');
  const materialiseStart = body.indexOf('_ctms_materialise_candidate_correction_residuals_v1');

  assert.ok(expansionStart >= 0, 'targeted correction-chain expansion must be present');
  assert.ok(materialiseStart > expansionStart, 'the complete chain must be in scope before residual materialisation');
  const expansionEnd = body.indexOf('v_linked_timesheet_ids_json := to_jsonb', expansionStart) + 160;
  assert.ok(expansionEnd > expansionStart, 'expanded chain scope must be persisted for collection');
  const expansionBlock = body.slice(expansionStart, expansionEnd);
  assert.match(expansionBlock, /timesheet_correction_chain_scope_v1\([\s\S]*member_timesheet_ids/);
  assert.match(expansionBlock, /v_linked_timesheet_ids_json := to_jsonb/);
  assert.match(expansionBlock, /PRE_DRAFT_LIVE_TRUTH/);
  assert.doesNotMatch(expansionBlock, /PAY_BATCH|bank_csv_export_json|settlement/);
});

test('correction-chain channel resolution uses the candidate current pay method once per chain', () => {
  const start = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_candidate_correction_residuals_v1'
  );
  const end = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_rewrite_source_build_correction_negative_components_v1',
    start
  );
  assert.ok(start >= 0 && end > start, 'candidate correction residual helper must exist');
  const helperBody = correctionRuntimeSql.slice(start, end);

  assert.match(helperBody, /from public\.candidates as candidate_row[\s\S]*where candidate_row\.id = p_candidate_id/);
  assert.match(helperBody, /v_target_pay_method[\s\S]*public\.pay_correction_chain_residual_v1\([\s\S]*v_target_pay_method/);
  assert.match(helperBody, /CORRECTION_CHAIN_TARGET_PAY_METHOD_REQUIRED/);
  assert.doesNotMatch(
    helperBody,
    /case when upper\(btrim\(coalesce\(tf\.pay_method,[\s\S]*as target_pay_method/
  );
  assert.doesNotMatch(helperBody, /r\.target_pay_method/);
});

test('correction residual source authority excludes its own finance cases', () => {
  const body = functionBody(
    'pay_correction_chain_residual_v1',
    null,
    correctionResidualSql
  );
  const sourceStart = body.indexOf(
    'Correction-chain source truth must come from the timesheet entitlement'
  );
  const sourceEnd = body.indexOf('source_basis AS (', sourceStart);
  assert.ok(
    sourceStart >= 0 && sourceEnd > sourceStart,
    'correction source-authority block must exist'
  );
  const sourceBlock = body.slice(sourceStart, sourceEnd);

  assert.match(
    sourceBlock,
    /public\._pay_current_timesheet_entitlement_components\(\s*v_member_ids\s*\)/
  );
  assert.match(
    sourceBlock,
    /public\._pay_reserved_components\(\s*v_member_ids,\s*p_exclude_pay_batch_id\s*\)/
  );
  assert.match(
    sourceBlock,
    /truth_ex_vat[\s\S]*baseline_ex_vat[\s\S]*reserved_ex_vat/
  );
  assert.doesNotMatch(sourceBlock, /public\._pay_outstanding_components\s*\(/);

  assert.match(body, /family_components AS \([\s\S]*pay_finance_case_components/);
  assert.match(body, /settled_movements AS \(/);
  assert.match(body, /active_case_reservations AS \(/);
  assert.doesNotMatch(body, /\bNHSP\b|\bHR_WEEKLY\b|\bHR_DAILY\b/);
});


test('correction residual carrier replaces stale root components with the chain-wide residual', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  assert.ok(start >= 0 && end > start, 'correction residual materialiser must exist');
  const body = correctionRuntimeSql.slice(start, end);
  assert.match(body, /'case_components',jsonb_build_array\([\s\S]*'target_pay_ex_vat',\(v_component->>'target_outstanding_ex_vat'\)::numeric/);
  assert.match(body, /'source_entitlement_amount_ex_vat',abs\(\(v_component->>'truth_ex_vat'\)::numeric\)/);
  assert.match(body, /'source_reservation_amount_ex_vat',abs\(\(v_component->>'target_outstanding_ex_vat'\)::numeric\)/);
  assert.match(body, /'section_amount_ex_vat',\(v_component->>'target_outstanding_ex_vat'\)::numeric/);
  assert.match(
    body,
    /'economic_key',coalesce\(l\.source_row_json->'economic_key','\{\}'::jsonb\)\|\|jsonb_build_object\([\s\S]*'timesheet_id',v_root_id::text,[\s\S]*'key_type',v_component->>'component_key_type',[\s\S]*'key_value',v_component->>'component_key_value'/
  );
  assert.match(body, /economic_key_json=jsonb_build_object\([\s\S]*'timesheet_id',v_root_id::text/);
  assert.match(body, /'preview_contract',coalesce\(l\.source_row_json->'preview_contract','\{\}'::jsonb\)\|\|jsonb_build_object/);
  assert.match(body, /'selection_amount_ex_vat',\(v_component->>'target_outstanding_ex_vat'\)::numeric/);
});

test('correction-chain finance sync couples member cases before the central case lifecycle runs', () => {
  const helperStart = correctionRuntimeSql.indexOf('create or replace function public._ctms_rewrite_sync_correction_cases_v1');
  const helperEnd = correctionRuntimeSql.indexOf('create or replace function public._ctms_assert_payload_corrections_fresh_v1', helperStart);
  assert.ok(helperStart >= 0 && helperEnd > helperStart, 'correction-chain finance sync helper must exist');
  const helperBody = correctionRuntimeSql.slice(helperStart, helperEnd);

  assert.match(helperBody, /public\._ctms_candidate_correction_residuals_v1\(/);
  assert.match(helperBody, /v_member_ids && p_scope_timesheet_ids/);
  assert.match(helperBody, /CORRECTION_CHAIN_ACTIVE_FINANCE_RESERVATION/);
  assert.match(helperBody, /delete from pg_temp\.tmp_sync_timesheet_case_candidates[\s\S]*timesheet_id = any\(v_member_ids\)/);
  assert.match(helperBody, /insert into pg_temp\.tmp_sync_timesheet_case_candidates[\s\S]*v_root_id/);
  assert.match(helperBody, /'overpayment_component_authority', 'PRE_DRAFT_LIVE_TRUTH'/);
  assert.match(helperBody, /'source_family_key', v_residual->>'source_family_key'/);
  assert.doesNotMatch(helperBody, /\bNHSP\b|\bHR_WEEKLY\b|\bHR_DAILY\b/);

  const syncBody = functionBody('pay_sync_overpayments_from_preview', null, overpaymentSyncSql);
  const rewriteIndex = syncBody.indexOf('_ctms_rewrite_sync_correction_cases_v1');
  const countIndex = syncBody.indexOf('select count(*)::int into v_timesheet_case_count');
  const lifecycleIndex = syncBody.indexOf('for v_target_case_row in');
  assert.ok(rewriteIndex >= 0 && countIndex > rewriteIndex && lifecycleIndex > countIndex);
});

test('source-build attestation rewrites correction members to the coupled chain residual', () => {
  const helperStart = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_rewrite_source_build_correction_negative_components_v1'
  );
  const helperEnd = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_rewrite_sync_correction_cases_v1',
    helperStart
  );
  assert.ok(helperStart >= 0 && helperEnd > helperStart, 'source-build correction rewrite helper must exist');
  const helperBody = correctionRuntimeSql.slice(helperStart, helperEnd);

  assert.match(helperBody, /public\._ctms_candidate_correction_residuals_v1\(/);
  assert.match(helperBody, /delete from pg_temp\._tmp_pay_wb_sync_negative_components/i);
  assert.match(helperBody, /insert into pg_temp\._tmp_pay_wb_sync_negative_components/i);
  assert.match(helperBody, /v_residual->>'residual_fingerprint'/);
  assert.match(helperBody, /target_outstanding_ex_vat/);
  assert.match(helperBody, /insert into pg_temp\._tmp_pay_wb_sync_rotation_scope/i);

  const rewriteIndex = sourceBuildSql.indexOf(
    '_ctms_rewrite_source_build_correction_negative_components_v1'
  );
  const countIndex = sourceBuildSql.indexOf('SELECT COUNT(*)::integer,', rewriteIndex);
  assert.ok(rewriteIndex >= 0 && countIndex > rewriteIndex, 'chain residual rewrite must precede source-build attestation counts');
  assert.match(
    sourceBuildSql.slice(countIndex, countIndex + 1400),
    /ROUND\(negative_component\.reserved_ex_vat, 2\)::numeric\(12,2\)/
  );
  assert.equal(
    (sourceBuildSql.match(/_ctms_rewrite_source_build_correction_negative_components_v1\s*\(/g) || []).length,
    2,
    'initial and post-sync attestations must both couple correction chains'
  );
  assert.match(
    sourceBuildSql,
    /TRUNCATE TABLE pg_temp\._tmp_pay_wb_sync_negative_components[\s\S]*INSERT INTO pg_temp\._tmp_pay_wb_sync_negative_components[\s\S]*_ctms_rewrite_source_build_correction_negative_components_v1[\s\S]*v_post_sync_negative_digest/
  );
  assert.match(
    sourceBuildSql,
    /LIMIT 20[\s\S]*post_sync_negative_components/
  );
});

test('post-sync attestation rebuilds rotation and settled-baseline authority from the final scope', () => {
  const postScopeIndex = sourceBuildSql.indexOf(
    "v_post_sync_scope_digest := md5("
  );
  const postNegativeIndex = sourceBuildSql.indexOf(
    'TRUNCATE TABLE pg_temp._tmp_pay_wb_sync_negative_components',
    postScopeIndex
  );
  const postAttestationIndex = sourceBuildSql.indexOf(
    'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_OVERPAYMENT_SYNC_UNATTESTED',
    postNegativeIndex
  );
  assert.ok(
    postScopeIndex >= 0 && postNegativeIndex > postScopeIndex && postAttestationIndex > postNegativeIndex,
    'post-sync scope, negative-component rebuild and attestation must remain ordered'
  );

  const postSyncBlock = sourceBuildSql.slice(postScopeIndex, postAttestationIndex);
  assert.match(
    postSyncBlock,
    /TRUNCATE TABLE pg_temp\._tmp_pay_wb_sync_rotation_scope[\s\S]*public\._pay_timesheet_rotation_scope\(\s*COALESCE\(v_post_sync_scope_timesheet_ids/
  );
  assert.match(
    postSyncBlock,
    /post_timesheet_pay_state\.last_settled_signature/
  );
  assert.match(
    postSyncBlock,
    /public\._pay_active_settled_components\(\s*ARRAY\[post_negative\.timesheet_id\]::uuid\[\]/
  );
  assert.doesNotMatch(
    postSyncBlock,
    /post_negative\.outstanding_ex_vat,\s*NULL::text/
  );
});

test('central overpayment sync attests the same coupled correction-chain residual as source build', () => {
  const helperStart = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_rewrite_sync_authoritative_correction_negative_components_v1'
  );
  const helperEnd = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_rewrite_sync_correction_cases_v1',
    helperStart
  );
  assert.ok(helperStart >= 0 && helperEnd > helperStart, 'central correction attestation helper must exist');
  const helperBody = correctionRuntimeSql.slice(helperStart, helperEnd);

  assert.match(helperBody, /public\._ctms_candidate_correction_residuals_v1\(/);
  assert.match(helperBody, /delete from pg_temp\.tmp_sync_authoritative_negative_components/i);
  assert.match(helperBody, /insert into pg_temp\.tmp_sync_authoritative_negative_components/i);
  assert.match(helperBody, /CORRECTION_CHAIN_SYNC_SCOPE_MUST_INCLUDE_ROOT/);
  assert.match(helperBody, /target_outstanding_ex_vat/);

  const rewriteIndex = overpaymentSyncSql.indexOf(
    '_ctms_rewrite_sync_authoritative_correction_negative_components_v1'
  );
  const countIndex = overpaymentSyncSql.indexOf('COUNT(*)::integer,', rewriteIndex);
  assert.ok(rewriteIndex >= 0 && countIndex > rewriteIndex, 'central chain rewrite must precede digest attestation');
  assert.match(
    overpaymentSyncSql.slice(countIndex, countIndex + 1500),
    /ROUND\(negative_component\.reserved_ex_vat, 2\)::numeric\(12,2\)/
  );
  const metadataGuardIndex = overpaymentSyncSql.indexOf(
    'PAY_SYNC_OVERPAYMENTS_AUTHORITATIVE_NEGATIVE_COMPONENT_METADATA_MISMATCH'
  );
  const memberRootIndex = overpaymentSyncSql.lastIndexOf(
    'WITH correction_member_roots AS',
    metadataGuardIndex
  );
  assert.ok(memberRootIndex >= 0 && metadataGuardIndex > memberRootIndex);
  assert.match(
    overpaymentSyncSql.slice(memberRootIndex, metadataGuardIndex),
    /COALESCE\(correction_member\.root_timesheet_id, raw_case\.timesheet_id\)/
  );
});

test('the Supabase pldbgapi2 workaround is scoped to the correction-chain Banking entry points', () => {
  assert.equal(
    (correctionPlpgsqlGuardSql.match(/SET plpgsql_check\.mode TO 'disabled'/g) || []).length,
    7
  );
  assert.match(correctionPlpgsqlGuardSql, /pay_correction_chain_residual_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_ctms_candidate_correction_residuals_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_workbench_candidate_source_build_chunk\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_sync_overpayments_from_preview\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_preview_candidate_collect_scope\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_workbench_worker_drain_chunk\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_workbench_worker_drain_chunk_revalidated_v1\s*\(/);
  assert.doesNotMatch(correctionPlpgsqlGuardSql, /\bUPDATE\b|\bINSERT\b|\bDELETE\b|\bTRUNCATE\b|\bDROP\b/i);
});

test('negative correction residuals preserve the finance-case carrier while positive residuals stay pay lines', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  const body = correctionRuntimeSql.slice(start, end);
  assert.match(body, /CORRECTION_CHAIN_OVERPAYMENT_FINANCE_CASE_CARRIER_REQUIRED/);
  assert.match(body, /target_outstanding_ex_vat'[\s\S]*< 0[\s\S]*source_row_json->>'finance_case_id'/);
  assert.match(body, /when round\(coalesce\(nullif\(v_component->>'target_outstanding_ex_vat',''\)::numeric,0\),2\) < 0[\s\S]*then '\{\}'::jsonb/);
  assert.match(body, /else jsonb_build_object\([\s\S]*'amount_ex_vat',\(v_component->>'target_outstanding_ex_vat'\)::numeric/);
});

test('zero-value correction residual components do not require a Banking Pay carrier row', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  assert.ok(start >= 0 && end > start, 'correction residual materialiser must exist');
  const body = correctionRuntimeSql.slice(start, end);
  const missingCarrierStart = body.indexOf('if v_carrier_row_id is null then');
  const missingCarrierEnd = body.indexOf("raise exception 'CORRECTION_RESIDUAL_SOURCE_COMPONENT_MISSING'", missingCarrierStart);
  assert.ok(missingCarrierStart >= 0 && missingCarrierEnd > missingCarrierStart, 'missing-carrier guard must exist');
  const missingCarrierGuard = body.slice(missingCarrierStart, missingCarrierEnd);
  assert.match(
    missingCarrierGuard,
    /round\(coalesce\(nullif\(v_component->>'target_outstanding_ex_vat',''\)::numeric,0\),2\)=0[\s\S]*continue;/
  );
});

test('an older source-build failure is non-blocking after a newer equivalent build has succeeded', () => {
  const body = functionBody('pay_workbench_fail_job', 'pay_workbench_snapshot_enqueue_scope');
  const supersessionStart = body.indexOf("v_obsolete_reason := 'SUPERSEDED_BY_NEWER_SUCCESSFUL_SOURCE_BUILD'");
  assert.ok(supersessionStart >= 0, 'source-build failure supersession guard must exist');

  const completedBuildCheck = body.slice(
    body.lastIndexOf("IF v_is_obsolete IS NOT TRUE", supersessionStart),
    supersessionStart + 100
  );
  assert.match(completedBuildCheck, /completed_source_build\.created_at_utc > v_job_created_at_utc/);
  assert.match(completedBuildCheck, /completed_seq\.source_change_seq = v_job_source_change_seq/);
  assert.match(completedBuildCheck, /completed_source_row\.source_build_run_id = completed_run\.source_build_run_id_text::uuid/);
  assert.match(completedBuildCheck, /completed_source_row\.status, ''\)\)\) = 'CURRENT'/);
  assert.match(body, /'superseded_at_utc', v_now::text[\s\S]*'non_blocking_terminal_failure', true/);
  assert.match(body, /IF v_new_status = 'FAILED'[\s\S]*v_canonical_job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'[\s\S]*v_is_obsolete IS NOT TRUE/);
});

test('import-authoritative Banking Pay correction guards remain source-system neutral', () => {
  const materialiserStart = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const materialiserEnd = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', materialiserStart);
  assert.ok(materialiserStart >= 0 && materialiserEnd > materialiserStart, 'correction residual materialiser must exist');
  const materialiserBody = correctionRuntimeSql.slice(materialiserStart, materialiserEnd);
  assert.doesNotMatch(materialiserBody, /source_system\s*(?:=|in)\s*/i);

  const failBody = functionBody('pay_workbench_fail_job', 'pay_workbench_snapshot_enqueue_scope');
  assert.doesNotMatch(failBody, /\bNHSP\b|\bHR_WEEKLY\b|\bHR_DAILY\b/);
});

test('preview materialisation does not double-clamp correction-chain residuals to the root timesheet', () => {
  const body = functionBody('pay_workbench_preview_rows_materialise_chunk', 'pay_workbench_mark_finance_case_dirty');
  assert.match(body, /AS is_correction_chain_residual/);
  assert.match(body, /result_row_json->>'source_family_key'[\s\S]*LIKE 'correction-chain:%'/);
  assert.equal((body.match(/is_correction_chain_residual IS NOT TRUE/g) || []).length, 4);
  assert.match(body, /clamp_economic_key_to_outstanding/);
});


test('scheduled Banking Pay prerequisites and invoice-PDF queue calls fail fast when Supabase is unavailable', () => {
  const settingsStart = workerSource.indexOf('async function loadSettingsDefaults');
  const settingsEnd = workerSource.indexOf('async function loadCompanySettings', settingsStart);
  const settingsBody = workerSource.slice(settingsStart, settingsEnd);
  assert.match(settingsBody, /settings_defaults[\s\S]*\{ timeoutMs: 8000 \}/);
  assert.match(settingsBody, /Object\.defineProperty\(out, '__load_failed', \{ value: false, enumerable: false \}\)/);
  assert.match(settingsBody, /Object\.defineProperty\(out, '__load_failed', \{ value: true, enumerable: false \}\)/);

  const dateContextStart = workerSource.indexOf('async function resolveBankingPayOfficialDateContext');
  const dateContextEnd = workerSource.indexOf('async function', dateContextStart + 20);
  const dateContextBody = workerSource.slice(dateContextStart, dateContextEnd);
  assert.match(dateContextBody, /pay_banking_official_date_context_v1[\s\S]*routeClass: 'DISPLAY'[\s\S]*purpose: 'BANKING_PAY_OFFICIAL_DATE_CONTEXT'[\s\S]*timeoutMs: 8000/);

  const sbFetchStart = workerSource.indexOf('async function sbFetch');
  const sbFetchEnd = workerSource.indexOf('async function sbRpc', sbFetchStart);
  const sbFetchBody = workerSource.slice(sbFetchStart, sbFetchEnd);
  assert.match(sbFetchBody, /delete init\.timeoutMs/);
  assert.match(sbFetchBody, /new AbortController\(\)/);
  assert.match(sbFetchBody, /SUPABASE_FETCH_TIMEOUT/);
  assert.match(sbFetchBody, /clearTimeout\(timeoutHandle\)/);

  const invpdfStart = workerSource.indexOf('const drainInvpdfOnce = async () =>');
  const invpdfEnd = workerSource.indexOf('const scheduledStartedAtUtc', invpdfStart);
  const invpdfBody = workerSource.slice(invpdfStart, invpdfEnd);
  assert.equal((invpdfBody.match(/timeoutMs: 10000/g) || []).length, 4);

  const scheduledStart = workerSource.indexOf('const scheduledStartedAtUtc');
  const scheduledEnd = workerSource.indexOf('const scheduledTasks =', scheduledStart);
  const scheduledBody = workerSource.slice(scheduledStart, scheduledEnd);
  assert.equal((scheduledBody.match(/const scheduledSettingsPromise = loadSettingsDefaults\(env\);/g) || []).length, 1);
  assert.equal((scheduledBody.match(/await getScheduledSettings\(\)/g) || []).length, 3);
  assert.equal((scheduledBody.match(/code: 'SCHEDULED_DATABASE_UNAVAILABLE'/g) || []).length, 3);
  assert.match(scheduledBody, /settings && settings\.__load_failed !== true \? settings : null/);
});
