const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const normalizeSql = (value) => String(value).replace(/\r\n/g, '\n');
const readSql = (file) => normalizeSql(fs.readFileSync(file, 'utf8'));

const sqlPath = path.resolve(__dirname, '../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
const sql = fs.readFileSync(sqlPath, 'utf8');
const sourceBuildSql = fs.readFileSync(
  path.resolve(__dirname, './fixtures/banking-pay-installed-baselines/21072026_1235_39_pay_workbench_candidate_source_build_chunk.sql'),
  'utf8'
);
const boundedSourceBuildSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/04082026_1213_pay_workbench_candidate_source_build_chunk.sql'),
  'utf8'
);
const overpaymentSyncSql = fs.readFileSync(
  path.resolve(__dirname, './fixtures/banking-pay-installed-baselines/21072026_1235_40_pay_sync_overpayments_from_preview.sql'),
  'utf8'
);
const sessionCaseResolutionSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_41_pay_workbench_session_apply_case_resolution.sql'),
  'utf8'
);
const correctionRuntimeSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_00b_import_correction_runtime_guards.sql'),
  'utf8'
);
const correctionResidualSql = readSql(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql')
);
const correctionPlpgsqlGuardSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/23072026_1217_disable_plpgsql_check_for_correction_chain_banking.sql'),
  'utf8'
);
const financeAdjustmentSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql'),
  'utf8'
);
const frozenRecoveryIdentitySql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/20072026_1133_resolve_frozen_recovery_timesheet_identity.sql'),
  'utf8'
);
const laterFreshnessSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/26072026_1519_pay_batch_validate_freshness_correction_chain_wrapper.sql'),
  'utf8'
);
const transferScopeSeedSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/20072026_1215_align_transfer_scope_with_bank_projection.sql'),
  'utf8'
);
const settleRailSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/04082026_1211_pay_settle_rail.sql'),
  'utf8'
);
const preBankCancelSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/04082026_1158_pay_pre_bank_cancel_apply_work_item.sql'),
  'utf8'
);
const noMoneyUnwindSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/04082026_1158_pay_no_money_unwind_apply_work_item.sql'),
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

function lastFunctionBody(name, source = sql) {
  const functionMarker = `CREATE OR REPLACE FUNCTION public.${name}`;
  const start = source.lastIndexOf(functionMarker);
  assert.ok(start >= 0, `${name} must exist`);
  const end = source.indexOf('CREATE OR REPLACE FUNCTION public.', start + functionMarker.length);
  return source.slice(start, end > start ? end : source.length);
}

test('zero-raw residual SQL assertions are portable across LF and CRLF checkouts', () => {
  const lfFixture = normalizeSql(correctionResidualSql);
  const crlfFixture = lfFixture.replace(/\n/g, '\r\n');
  assert.equal(normalizeSql(crlfFixture), lfFixture);
});

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

test('post-draft recovery identity accepts linked timesheet identity only from frozen artifacts', () => {
  const body = functionBody(
    '_pay_policy_x_resolve_post_draft_economic_key',
    null,
    frozenRecoveryIdentitySql
  );
  assert.match(body, /v_frozen_source_basis_json->>'linked_timesheet_id'/);
  assert.match(
    body,
    /v_frozen_component_snapshot_json#>>'\{source_basis_json,linked_timesheet_id\}'/
  );
  assert.match(body, /v_frozen_component_snapshot_json->>'linked_timesheet_id'/);
  assert.match(body, /v_breakdown_meta_json->>'linked_timesheet_id'/);
  assert.match(body, /v_frozen_source_basis_json->>'carrier_timesheet_id'/);
  assert.match(
    body,
    /v_frozen_component_snapshot_json#>>'\{source_basis_json,carrier_timesheet_id\}'/
  );
  assert.match(body, /v_frozen_component_snapshot_json->>'carrier_timesheet_id'/);
  assert.match(body, /v_breakdown_meta_json->>'carrier_timesheet_id'/);
  assert.match(
    body,
    /COUNT\(DISTINCT frozen_timesheet_candidate\.candidate_value\)/i
  );
  assert.match(
    body,
    /v_frozen_timesheet_id_text\s*:=\s*COALESCE\(\s*v_frozen_linked_timesheet_id_text,\s*v_frozen_direct_timesheet_id_text,\s*v_frozen_carrier_timesheet_id_text\s*\)/i
  );
  assert.match(
    body,
    /v_frozen_linked_timesheet_id_count[\s\S]*v_frozen_direct_timesheet_id_count[\s\S]*v_frozen_carrier_timesheet_id_count[\s\S]*CONFLICTING_FROZEN_TIMESHEET_ID/i
  );
  assert.match(body, /linked_timesheet_id is the stable correction-root/i);
  assert.match(body, /_pay_policy_x_assert_economic_key/);
  assert.doesNotMatch(body, /FROM public\.timesheets|JOIN public\.timesheets/);
});

test('finance adjustment draft creation treats signed recovery component amounts as magnitudes', () => {
  const body = functionBody('pay_batch_apply_finance_adjustments', null, financeAdjustmentSql);
  for (const [stage, nextStage] of [
    ['STAGE_16AA_APPLY_OVERPAYMENT_RECOVERY', 'STAGE_16B_APPLY_MANUAL_DEBT_RECOVERY'],
    ['STAGE_16B_APPLY_MANUAL_DEBT_RECOVERY', 'STAGE_16C_APPLY_PAYMENT_ADVANCE_REPAYMENTS'],
    ['STAGE_16C_APPLY_PAYMENT_ADVANCE_REPAYMENTS', 'STAGE_16BD_INSERT_DORMANT_RECOVERY_TEMPLATES']
  ]) {
    const start = body.indexOf(`v_stage := '${stage}'`);
    const end = body.indexOf(`v_stage := '${nextStage}'`, start);
    assert.ok(start >= 0 && end > start, `${stage} must have a bounded materialisation block`);
    const recoveryBlock = body.slice(start, end);
    assert.ok(
      (
        recoveryBlock.match(
          /preview_due_amount_ex_vat',''\) ~ '\^-\?\\d\+\(\\\.\\d\+\)\?\$' then abs\(\(comp\.comp_json->>'preview_due_amount_ex_vat'\)::numeric\)/g
        ) || []
      ).length >= 3,
      `${stage} must normalise the parsed amount, component sum and component filter as positive recovery magnitudes`
    );
  }
  assert.match(
    body,
    /target_pay_ex_vat',''\) ~ '\^-\?\\d\+\(\\\.\\d\+\)\?\$' then abs\(\(comp\.comp_json->>'target_pay_ex_vat'\)::numeric\)/
  );
  assert.match(body, /source_amount_ex_vat',''\)[\s\S]*abs\(\(comp\.comp_json->>'source_amount_ex_vat'\)::numeric\)/);
  assert.match(body, /'OVERPAYMENT_RECOVERY' as item_type[\s\S]*\(-fa\.take_target_ex\)::numeric\(12,2\)/);
  assert.match(body, /OPERATION_ALLOCATION_ROWS_NOT_LINKED/);
});

test('finance adjustment recovery is capped by the selected server preview amount', () => {
  const body = functionBody('pay_batch_apply_finance_adjustments', null, financeAdjustmentSql);
  const recoveryStart = body.indexOf("v_stage := 'STAGE_16AA_APPLY_OVERPAYMENT_RECOVERY'");
  const recoveryEnd = body.indexOf("v_stage := 'STAGE_16B_APPLY_MANUAL_DEBT_RECOVERY'", recoveryStart);
  assert.ok(recoveryStart >= 0 && recoveryEnd > recoveryStart, 'overpayment recovery stage must exist');
  const recoveryBlock = body.slice(recoveryStart, recoveryEnd);

  assert.match(
    recoveryBlock,
    /selected_case_row_caps as \([\s\S]*sum\(scr\.row_due_amount_ex_vat\)[\s\S]*selected_row_due_amount_ex_vat/
  );
  assert.match(
    recoveryBlock,
    /least\(\s*round\(sum\(scr\.component_due_amount_ex_vat\), 2\),\s*max\(src\.selected_row_due_amount_ex_vat\)\s*\)::numeric\(12,2\) as due_amount_ex_vat/
  );
  assert.match(
    recoveryBlock,
    /src\.umbrella_id is not distinct from scr\.umbrella_id/
  );
  assert.match(
    recoveryBlock,
    /selected_case_component_rows_ranked as \([\s\S]*partition by component_row\.finance_case_id, component_row\.preview_row_id[\s\S]*prior_component_due_amount_ex_vat/
  );
  assert.match(
    recoveryBlock,
    /selected_case_component_rows as \([\s\S]*ranked_row\.row_due_amount_ex_vat - ranked_row\.prior_component_due_amount_ex_vat[\s\S]*as component_due_amount_ex_vat/
  );
  assert.match(
    recoveryBlock,
    /'uncapped_preview_due_amount_ex_vat'[\s\S]*'preview_due_amount_ex_vat'[\s\S]*'allocated_source_due_amount_ex_vat'/
  );
  assert.match(
    recoveryBlock,
    /abs\(\(ranked_row\.comp_json->>'allocated_source_due_amount_ex_vat'\)::numeric\)[\s\S]*\/ abs\(\(ranked_row\.comp_json->>'preview_due_amount_ex_vat'\)::numeric\)/
  );
  assert.match(
    recoveryBlock,
    /selected_case_rows as \([\s\S]*from selected_case_component_rows scr[\s\S]*join selected_case_row_caps src/,
    'the grouped recovery authority must aggregate the capped component rows'
  );
  assert.doesNotMatch(
    recoveryBlock,
    /selected_case_rows as \([\s\S]*from selected_case_component_rows_raw scr[\s\S]*join selected_case_row_caps src/,
    'the grouped recovery authority must not bypass the capped component-row hand-off'
  );
});

test('finance adjustment draft creation freezes correction payout authority into a positive destination group', () => {
  const body = functionBody('pay_batch_apply_finance_adjustments', null, financeAdjustmentSql);
  assert.match(
    body,
    /pbi\.item_type in \('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT'\)[\s\S]*matching_positive_group\.week_ending_bucket/
  );
  assert.match(
    body,
    /left join lateral \([\s\S]*positive_item\.pay_batch_candidate_id = pbi\.pay_batch_candidate_id[\s\S]*round\(coalesce\(positive_item\.amount_inc_vat, positive_item\.amount_ex_vat, 0\), 2\) > 0/
  );
  assert.match(
    body,
    /pbi\.item_type in \('LOAN_PAYOUT','MANUAL_CREDIT_PAYOUT','UNDERPAYMENT_PAYMENT','OVERPAYMENT_RECOVERY','MANUAL_DEBT_RECOVERY','LOAN_REPAYMENT'\)/
  );
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
  assert.match(attestationBlock, /v_sync_durable_component_count, 0\)[\s\S]*v_sync_protected_component_count, 0\)[\s\S]*v_sync_resolution_pending_component_count, 0\)[\s\S]*v_sync_negative_component_count, 0\)/);
  assert.match(attestationBlock, /IF COALESCE\(v_sync_candidate_covered, false\) IS NOT TRUE THEN/);
  assert.doesNotMatch(attestationBlock, /OR COALESCE\(v_sync_uncovered_component_count/);
});

test('pay-method correction resolution is surfaced without creating or clearing finance authority', () => {
  const helperStart = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_rewrite_sync_correction_cases_v1'
  );
  const helperEnd = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_assert_payload_corrections_fresh_v1',
    helperStart
  );
  const helperBody = correctionRuntimeSql.slice(helperStart, helperEnd);
  assert.match(
    helperBody,
    /CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED[\s\S]*delete from pg_temp\.tmp_sync_timesheet_case_candidates[\s\S]*v_resolution_pending_member_ids[\s\S]*continue;/
  );
  assert.match(
    helperBody,
    /CORRECTION_CHAIN_ACTIVE_FINANCE_RESERVATION[\s\S]*CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED/
  );
  assert.match(
    helperBody,
    /active_batch_candidate\.candidate_id = v_candidate_id[\s\S]*active_batch\.cancelled_at_utc is null[\s\S]*'DRAFT'[\s\S]*'EXECUTING'[\s\S]*'AUTHORISED_FOR_PAYMENT'/
  );
  assert.match(
    helperBody,
    /batch_item\.frozen_component_snapshot_json->>'correction_root_id'[\s\S]*= v_root_id::text/
  );
  assert.match(
    helperBody,
    /delete from pg_temp\.tmp_sync_timesheet_case_candidates candidate_row[\s\S]*candidate_row\.timesheet_id = any\(v_member_ids\);[\s\S]*continue;/
  );

  const syncBody = functionBody(
    'pay_sync_overpayments_from_preview',
    null,
    overpaymentSyncSql
  );
  assert.match(
    syncBody,
    /v_correction_rewrite_result[\s\S]*resolution_pending_member_timesheet_ids/
  );
  assert.match(
    syncBody,
    /and not \([\s\S]*v_resolution_pending_member_ids[\s\S]*pa\.linked_timesheet_id[\s\S]*any\(v_resolution_pending_member_ids\)/
  );
  assert.match(
    syncBody,
    /CORRECTION_RESIDUAL_NOT_READY_FOR_OVERPAYMENT_SYNC/
  );
  assert.match(
    syncBody,
    /CORRECTION_CHAIN_RESERVATION_OVERRUN[\s\S]*pay_batch_items active_batch_item[\s\S]*pay_batch_candidates active_batch_candidate[\s\S]*active_batch_candidate\.candidate_id = v_correction_candidate/
  );
  assert.match(
    syncBody,
    /active_batch_item\.frozen_component_snapshot_json->>'correction_root_id'[\s\S]*= coalesce\(r->>'root_timesheet_id',''\)/
  );
  assert.match(
    syncBody,
    /active_batch\.cancelled_at_utc is null[\s\S]*'DRAFT'[\s\S]*'EXECUTING'[\s\S]*'AUTHORISED_FOR_PAYMENT'/
  );
  assert.match(
    overpaymentSyncSql,
    /REVOKE ALL ON FUNCTION public\.pay_sync_overpayments_from_preview\([\s\S]*\) FROM PUBLIC, anon, authenticated;/
  );
  assert.match(
    overpaymentSyncSql,
    /GRANT EXECUTE ON FUNCTION public\.pay_sync_overpayments_from_preview\([\s\S]*\) TO service_role;/
  );
  assert.match(
    syncBody,
    /v_resolution_pending_root_ids uuid\[\][\s\S]*CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED[\s\S]*root_timesheet_id/
  );
  assert.match(
    syncBody,
    /authoritative_component\.timesheet_id[\s\S]*ANY\(COALESCE\(v_resolution_pending_root_ids, ARRAY\[\]::uuid\[\]\)\)/
  );
  const pendingMetadataGuards = [
    ...syncBody.matchAll(
      /authoritative_component\.timesheet_id\s*=\s*ANY\(COALESCE\(v_resolution_pending_root_ids, ARRAY\[\]::uuid\[\]\)\)[\s\S]{0,240}?authoritative_component\.timesheet_id\s*=\s*ANY\(COALESCE\(v_resolution_pending_member_ids, ARRAY\[\]::uuid\[\]\)\)/g
    ),
  ];
  assert.equal(
    pendingMetadataGuards.length,
    3,
    'all authoritative-negative metadata guards must exempt both pending roots and their member timesheets'
  );
  assert.match(
    syncBody,
    /raw_case\.timesheet_id[\s\S]*ANY\(COALESCE\(v_resolution_pending_member_ids, ARRAY\[\]::uuid\[\]\)\)[\s\S]*tmp_sync_raw_negative_timesheet_rows/
  );
});

test('only the current overpayment-sync function body remains in repeatable sources', () => {
  const repeatableDir = path.resolve(__dirname, '../supabase/repeatable');
  const definitions = [];
  for (const entry of fs.readdirSync(repeatableDir, { withFileTypes: true })) {
    if (!entry.isFile()) continue;
    const source = fs.readFileSync(path.join(repeatableDir, entry.name), 'utf8');
    const matches = source.match(
      /CREATE OR REPLACE FUNCTION\s+public\.pay_sync_overpayments_from_preview\s*\(/gi
    );
    for (let index = 0; index < (matches || []).length; index += 1) {
      definitions.push(entry.name);
    }
  }
  assert.deepEqual(definitions, ['04082026_1210_pay_sync_overpayments_from_preview.sql']);
});

test('source build attests pending pay-method resolution but draft gate remains fail-closed', () => {
  const sourceBody = functionBody(
    'pay_workbench_candidate_source_build_chunk',
    null,
    sourceBuildSql
  );
  assert.match(
    sourceBody,
    /correction_resolution_pending_member_timesheet_ids[\s\S]*THEN 'RESOLUTION_PENDING'/
  );
  assert.match(
    sourceBody,
    /v_current_resolution_pending_member_ids uuid\[\][\s\S]*PAY_WORKBENCH_SOURCE_BUILD_ATTESTATION[\s\S]*CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED/
  );
  assert.match(
    sourceBody,
    /negative_component\.timesheet_id = ANY\([\s\S]*v_current_resolution_pending_member_ids/
  );
  assert.match(
    sourceBody,
    /PAY_METHOD_RESOLUTION_REQUIRED/
  );

  const materialiserStart = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_materialise_candidate_correction_residuals_v1'
  );
  const materialiserEnd = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_enrich_correction_resolution_payload_v1',
    materialiserStart
  );
  const materialiserBody = correctionRuntimeSql.slice(
    materialiserStart,
    materialiserEnd
  );
  assert.match(
    materialiserBody,
    /CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED[\s\S]*v_resolution_pending:=true;[\s\S]*CORRECTION_RESIDUAL_NOT_DRAFTABLE/
  );
  assert.match(
    materialiserBody,
    /v_resolution_pending[\s\S]*for v_component[\s\S]*v_component_needs_resolution:=v_resolution_pending[\s\S]*resolution_required[\s\S]*resolution_complete/
  );
  assert.match(
    materialiserBody,
    /when v_resolution_pending then 'cases_resolutions'[\s\S]*when v_component_needs_resolution then 'RESOLUTION_REQUIRED'[\s\S]*when v_resolution_pending then 'WAITING_LINKED_RESOLUTION'/
  );
  assert.doesNotMatch(
    materialiserBody,
    /CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED[\s\S]{0,1800}?continue;[\s\S]{0,200}?CORRECTION_RESIDUAL_NOT_DRAFTABLE/
  );
  assert.match(
    materialiserBody,
    /CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED[\s\S]*update public\.banking_pay_workbench_candidate_source_lines l[\s\S]*l\.line_key like[\s\S]*'correction-chain:'\|\|v_root_id::text\|\|':%'[\s\S]*canonical_correction_key[\s\S]*status='SUPERSEDED'/
  );
  assert.match(
    materialiserBody,
    /CORRECTION_CHAIN_RESERVATION_OVERRUN[\s\S]*pay_batch_items active_batch_item[\s\S]*active_batch_candidate\.candidate_id = p_candidate_id/
  );
  assert.match(
    materialiserBody,
    /active_batch\.cancelled_at_utc is null[\s\S]*'DRAFT'[\s\S]*'EXECUTING'[\s\S]*'AUTHORISED_FOR_PAYMENT'/
  );
  assert.match(
    materialiserBody,
    /active_batch_item\.frozen_component_snapshot_json->>'correction_root_id'[\s\S]*= v_root_id::text[\s\S]*continue;/
  );
  assert.match(
    materialiserBody,
    /member_timesheet_ids[\s\S]*banking_pay_workbench_candidate_source_lines l[\s\S]*status='SUPERSEDED'[\s\S]*l\.section='cases_resolutions'[\s\S]*economic_key_json->>'key_type'[\s\S]*'TS_TOTAL','TS_DAY'[\s\S]*continue;/,
    'an active frozen draft must suppress only unresolved raw worked-time aliases without suppressing ordinary pay or independent expense domains'
  );
  assert.match(
    materialiserBody,
    /jsonb_array_elements\([\s\S]*v_residual->'components'[\s\S]*target_outstanding_ex_vat[\s\S]*<> 0[\s\S]*pay_batch_items active_batch_item/,
    'active-draft alias suppression must not hide a genuine later correction delta'
  );

  const draftGateStart = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_assert_session_correction_residuals_draftable_v1'
  );
  const draftGateEnd = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_materialise_candidate_correction_residuals_v1',
    draftGateStart
  );
  const draftGateBody = correctionRuntimeSql.slice(draftGateStart, draftGateEnd);
  assert.match(
    draftGateBody,
    /draftable[\s\S]*CORRECTION_RESIDUAL_NOT_DRAFTABLE/
  );
  assert.doesNotMatch(
    draftGateBody,
    /CORRECTION_CHAIN_PAY_METHOD_RESOLUTION_REQUIRED[\s\S]*continue;/
  );
});

test('case resolution refreshes the complete candidate after advancing the session version', () => {
  const resolutionBody = functionBody(
    'pay_workbench_session_apply_case_resolution',
    null,
    sessionCaseResolutionSql
  );
  assert.match(
    resolutionBody,
    /UPDATE public\.banking_pay_workbench_sessions[\s\S]*SET version = public\.banking_pay_workbench_sessions\.version \+ 1[\s\S]*v_case_refresh_scope_kind := 'CANDIDATE_FULL_LIVE';[\s\S]*pay_workbench_enqueue_session_candidate_refresh/
  );
  assert.match(
    resolutionBody,
    /'targeted_timesheet_ids', CASE[\s\S]*v_case_refresh_scope_kind = 'CANDIDATE_FULL_LIVE' THEN '\[\]'::jsonb/
  );
  assert.match(
    resolutionBody,
    /'linked_timesheet_ids', CASE[\s\S]*v_case_refresh_scope_kind = 'CANDIDATE_FULL_LIVE' THEN '\[\]'::jsonb/
  );
});

test('source-build continuation attests correction-chain residual fingerprints without weakening ordinary baseline checks', () => {
  const sourceBuildBody = functionBody(
    'pay_workbench_candidate_source_build_chunk',
    null,
    sourceBuildSql
  );
  assert.match(
    sourceBuildBody,
    /finance_case\.baseline_signature IS NOT DISTINCT FROM negative_component\.baseline_signature[\s\S]*finance_component\.source_family_key[\s\S]*LIKE 'correction-chain:%'[\s\S]*correction_chain_residual_fingerprint[\s\S]*finance_case\.baseline_signature IS NOT DISTINCT FROM/
  );
});

test('bounded source-build continuation accepts only exact build, generation, cursor and attempt authority', () => {
  const body = functionBody(
    'pay_workbench_candidate_source_build_chunk',
    null,
    boundedSourceBuildSql
  );
  assert.match(body, /v_build_id uuid:=NULLIF\(v_payload->>'economic_build_id',''\)::uuid/);
  assert.match(body, /v_attempt_id uuid:=NULLIF\(v_payload->>'attempt_id',''\)::uuid/);
  assert.match(body, /v_attempt_nonce uuid:=NULLIF\(v_payload->>'attempt_nonce',''\)::uuid/);
  assert.match(body, /v_registry\.dirty_generation IS DISTINCT FROM v_build\.captured_candidate_generation/);
  assert.match(body, /v_registry\.current_source_change_seq IS DISTINCT FROM v_build\.source_change_seq/);
  assert.match(body, /attempt\.attempt_nonce=v_attempt_nonce[\s\S]*attempt\.attempt_status='STARTED'/);
  assert.match(body, /v_cursor_start_hash:=md5\(v_cursor::text\)/);
  assert.match(body, /PAY_WORKBENCH_FACT_PAGE_REPLAY_CONFLICT/);
  assert.doesNotMatch(body, /old_marker_accepted_as_authority|SOURCE_BUILD_CONTINUATION_DURABLE_ATTESTATION_REUSED/);
});

test('candidate source-build stage dispatcher is owner-only behind the service-role attempt RPCs', () => {
  assert.match(
    boundedSourceBuildSql,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_candidate_source_build_chunk\(uuid,uuid,jsonb,jsonb,integer\) FROM PUBLIC,anon,authenticated,service_role;/i
  );
  assert.match(
    boundedSourceBuildSql,
    /GRANT EXECUTE ON FUNCTION public\.pay_workbench_candidate_source_build_chunk\(uuid,uuid,jsonb,jsonb,integer\) TO postgres;/i
  );
  assert.doesNotMatch(
    boundedSourceBuildSql,
    /GRANT EXECUTE ON FUNCTION public\.pay_workbench_candidate_source_build_chunk\([^;]+\) TO service_role;/i
  );
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
  assert.match(
    expansionBlock,
    /requested_chain\.chain_json->'member_timesheet_ids'[\s\S]*_ctms_import_correction_classify_v1\(chain_member\.value::uuid\)/
  );
  assert.doesNotMatch(
    expansionBlock,
    /WHERE COALESCE\(\(public\._ctms_import_correction_classify_v1\(requested_scope\.timesheet_id\)/
  );
  assert.match(expansionBlock, /expanded_target_scope[\s\S]*INTO v_targeted_timesheet_ids/);
  assert.match(expansionBlock, /v_targeted_timesheet_ids_json := to_jsonb/);
  assert.match(expansionBlock, /v_linked_timesheet_ids_json := to_jsonb/);
  assert.match(
    expansionBlock,
    /SELECT expanded_member_ids\.timesheet_id[\s\S]*FROM expanded_member_ids[\s\S]*INTO v_targeted_timesheet_ids/
  );
  assert.match(expansionBlock, /PRE_DRAFT_LIVE_TRUTH/);
  assert.doesNotMatch(expansionBlock, /PAY_BATCH|bank_csv_export_json|settlement/);
});

test('full-live source scope promotes valid import-authoritative correction chain members before and after sync', () => {
  const body = functionBody('pay_workbench_candidate_source_build_chunk', null, sourceBuildSql);
  const preSyncStart = body.indexOf('Full-live seeds normally contain only current/frozen carriers');
  const preSyncEnd = body.indexOf('SELECT rotation_scope.canonical_timesheet_id', preSyncStart);
  assert.ok(preSyncStart >= 0 && preSyncEnd > preSyncStart, 'pre-sync full-live chain expansion must exist');
  const preSyncBlock = body.slice(preSyncStart, preSyncEnd);
  assert.match(preSyncBlock, /timesheet_correction_chain_scope_v1\(/);
  assert.match(preSyncBlock, /member_timesheet_ids/);
  assert.match(preSyncBlock, /is_import_authoritative_correction/);
  assert.match(preSyncBlock, /correction_member\.value::uuid/);
  assert.match(
    preSyncBlock,
    /JOIN public\.timesheets AS correction_seed_timesheet[\s\S]*FROM public\.timesheets_financials AS correction_seed_financials[\s\S]*correction_seed_financials\.candidate_id = p_candidate_id/,
    'stale retained identifiers must be rejected before invoking the correction-chain helper'
  );

  const postSyncStart = body.indexOf(
    'SELECT correction_member.value::uuid',
    body.indexOf('post_expanded_timesheet_ids AS (')
  );
  const postSyncEnd = body.indexOf('SELECT rotation_scope.canonical_timesheet_id', postSyncStart);
  assert.ok(postSyncStart >= 0 && postSyncEnd > postSyncStart, 'post-sync full-live chain expansion must exist');
  const postSyncBlock = body.slice(postSyncStart, postSyncEnd);
  assert.match(postSyncBlock, /timesheet_correction_chain_scope_v1\(/);
  assert.match(postSyncBlock, /member_timesheet_ids/);
  assert.match(postSyncBlock, /is_import_authoritative_correction/);
  assert.match(
    postSyncBlock,
    /JOIN public\.timesheets AS correction_seed_timesheet[\s\S]*FROM public\.timesheets_financials AS correction_seed_financials[\s\S]*correction_seed_financials\.candidate_id = p_candidate_id/,
    'post-sync correction-chain expansion must ignore stale or cross-candidate retained identifiers'
  );
});

test('full-live collection receives correction-chain carriers through a bounded internal-only handoff', () => {
  const sourceBody = functionBody('pay_workbench_candidate_source_build_chunk', null, sourceBuildSql);
  assert.equal(
    (sourceBody.match(/'source_build_force_include_timesheet_ids', COALESCE\(v_post_sync_scope_timesheet_ids_json, '\[\]'::jsonb\)/g) || []).length,
    2,
    'the preview decisions and final collector context must carry the post-sync authoritative scope'
  );

  const collectBody = functionBody('pay_preview_candidate_collect_scope', 'pay_workbench_session_open');
  const fullLiveStart = collectBody.indexOf(
    "COALESCE(v_refresh_scope_kind, 'CANDIDATE_FULL_LIVE') = 'CANDIDATE_FULL_LIVE'"
  );
  const fullLiveEnd = collectBody.indexOf('v_targeted_timesheet_ids_requested_json :=', fullLiveStart);
  assert.ok(fullLiveStart >= 0 && fullLiveEnd > fullLiveStart, 'full-live source page must exist');
  const fullLiveBlock = collectBody.slice(fullLiveStart, fullLiveEnd);

  assert.match(fullLiveBlock, /v_internal_reconciliation_authorised IS TRUE/);
  assert.match(fullLiveBlock, /source_build_force_include_timesheet_ids/);
  assert.match(fullLiveBlock, /WITH ORDINALITY AS forced_source_values\(value, position\)/);
  assert.match(fullLiveBlock, /forced_source_values\.position <= 500/);
  assert.match(
    fullLiveBlock,
    /JOIN public\.timesheets AS forced_candidate_timesheet[\s\S]*FROM public\.timesheets_financials AS forced_candidate_financials[\s\S]*forced_candidate_financials\.candidate_id = v_candidate_id/,
    'forced source identities must be restricted to the candidate being rebuilt'
  );
});

test('paged source build materialises coupled correction residuals only after the terminal page', () => {
  const body = functionBody('pay_workbench_candidate_source_build_chunk', null, sourceBuildSql);
  assert.match(
    body,
    /IF COALESCE\(v_has_more, false\) IS NOT TRUE THEN[\s\S]*PERFORM public\._ctms_materialise_candidate_correction_residuals_v1\([\s\S]*END IF;/,
    'an intermediate page must not validate a correction chain before all source carriers are accumulated'
  );
  assert.equal(
    (body.match(/_ctms_materialise_candidate_correction_residuals_v1\s*\(/g) || []).length,
    1,
    'the terminal-page guard must be the only correction residual materialisation call'
  );
});

test('terminal source build reports the post-materialisation carrier count as authoritative', () => {
  const body = functionBody('pay_workbench_candidate_source_build_chunk', null, sourceBuildSql);
  const materialiseIndex = body.indexOf('_ctms_materialise_candidate_correction_residuals_v1');
  const postMaterialisationCountIndex = body.indexOf(
    'FROM public.banking_pay_workbench_candidate_source_lines AS post_materialisation_source_rows'
  );

  assert.ok(materialiseIndex >= 0, 'correction residual materialisation must exist');
  assert.ok(
    postMaterialisationCountIndex > materialiseIndex,
    'CURRENT source rows must be recounted after canonical materialisation retires raw aliases'
  );
  assert.match(
    body,
    /'current_source_row_count_authoritative', COALESCE\(v_has_more, false\) IS NOT TRUE/
  );
});

test('source-build completion handles a proven targeted empty carrier set without broad candidate cleanup', () => {
  const body = functionBody('pay_workbench_complete_job', 'pay_workbench_fail_job');

  assert.match(
    body,
    /v_current_source_row_count_authoritative := LOWER\(BTRIM\(COALESCE\([\s\S]*current_source_row_count_authoritative/
  );
  assert.match(
    body,
    /v_current_source_row_count_authoritative[\s\S]*COALESCE\(v_current_source_row_count, 0\) > 0[\s\S]*v_current_source_row_count_authoritative IS NOT TRUE/
  );
  assert.match(body, /PAY_WORKBENCH_SOURCE_EMPTY_TARGET_SCOPE_REQUIRED/);
  assert.match(
    body,
    /v_delta_refresh_scope_kind = 'TARGETED_TIMESHEETS'[\s\S]*v_source_empty_targeted_timesheet_count, 0\) = 0/
  );
  assert.match(
    body,
    /WHEN v_delta_refresh_scope_kind = 'TARGETED_TIMESHEETS' THEN 'READY'[\s\S]*ELSE 'SOURCE_EMPTY'/
  );
  assert.equal(
    (body.match(/v_delta_refresh_scope_kind <> 'TARGETED_TIMESHEETS'\s+OR source_empty_/g) || []).length,
    3,
    'source-line, line-work and preview cleanup must stay within an explicit targeted timesheet scope'
  );
  assert.match(
    body,
    /source_empty_line_work\.status[\s\S]*'PENDING'[\s\S]*'PROCESSING'[\s\S]*'RUNNING'[\s\S]*'QUEUED'[\s\S]*'DIRTY'[\s\S]*'READY'[\s\S]*'MATERIALISED'[\s\S]*'MATERIALIZED'/,
    'a targeted empty rebuild must retire already-materialised line work so a later preview pass cannot recreate a stale row'
  );
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

test('correction residual uses one signed settlement and reservation ledger across zero', () => {
  const body = functionBody(
    'pay_correction_chain_residual_v1',
    null,
    correctionResidualSql
  );
  const sourceStart = body.indexOf('source_basis AS (');
  const componentStart = body.indexOf('component_result AS (', sourceStart);
  const balancedStart = body.indexOf('component_balanced AS (', componentStart);
  assert.ok(
    sourceStart >= 0 && componentStart > sourceStart && balancedStart > componentStart,
    'source-basis, component-result and balanced-component stages must exist'
  );

  const sourceBlock = body.slice(sourceStart, componentStart);
  assert.match(sourceBlock, /'settled_recovery_ex'/);
  assert.match(sourceBlock, /'settled_underpayment_ex'/);
  assert.match(sourceBlock, /'reserved_recovery_ex'/);
  assert.match(sourceBlock, /'reserved_underpayment_ex'/);

  const componentBlock = body.slice(componentStart, balancedStart);
  assert.match(
    componentBlock,
    /raw_component\.raw_outstanding_ex_vat\s*\+\s*raw_component\.settled_recovery_ex\s*\+\s*raw_component\.reserved_recovery_ex\s*-\s*raw_component\.settled_underpayment_ex\s*-\s*raw_component\.reserved_underpayment_ex/
  );
  assert.doesNotMatch(componentBlock, /LEAST\s*\(\s*0|GREATEST\s*\(\s*0/i);

  const signedOutstanding = ({
    raw = 0,
    settledRecovery = 0,
    reservedRecovery = 0,
    settledUnderpayment = 0,
    reservedUnderpayment = 0,
  }) => Number((
    raw
    + settledRecovery
    + reservedRecovery
    - settledUnderpayment
    - reservedUnderpayment
  ).toFixed(2));

  assert.equal(signedOutstanding({ raw: -17.39 }), -17.39);
  assert.equal(signedOutstanding({ settledRecovery: 17.39 }), 17.39);
  assert.equal(signedOutstanding({ settledUnderpayment: 17.39 }), -17.39);
  assert.equal(
    signedOutstanding({ raw: -17.39, reservedRecovery: 17.39 }),
    0
  );
});

test('normalised pay-method correction target is not proportioned twice', () => {
  assert.match(
    correctionResidualSql,
    /WHEN component_row\.resolution_required[\s\S]*AND component_row\.resolution_complete[\s\S]*sign\(component_row\.effective_outstanding_ex_vat\)[\s\S]*abs\(COALESCE\([\s\S]*component_row\.resolved_target_amount_ex_vat/
  );
  assert.doesNotMatch(
    correctionResidualSql,
    /resolved_target_amount_ex_vat[\s\S]{0,500}effective_outstanding_ex_vat[\s\S]{0,200}raw_outstanding_ex_vat/
  );
});

test('correction-chain resolution normalisation preserves each member identity', () => {
  const start = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_normalise_correction_case_resolutions_v1'
  );
  const end = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_clear_correction_chain_snoozes_v1',
    start
  );
  assert.ok(start >= 0 && end > start);
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(
    body,
    /v_component_timesheet_id:=coalesce\([\s\S]*v_component->>'carrier_timesheet_id'[\s\S]*substring\(btrim\(v_resolution\.case_key\) from 11\)::uuid/
  );
  assert.match(body, /v_component_timesheet_id<>all\(v_member_ids\)/);
  assert.match(body, /set timesheet_id=v_component_timesheet_id/);
  assert.match(body, /'linked_timesheet_id',v_component_timesheet_id::text/);
  assert.doesNotMatch(body, /set timesheet_id=p_anchor_timesheet_id/);
});

test('linked correction resolution expands to required components without preview rows', () => {
  const start = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_normalise_correction_case_resolutions_v1'
  );
  const end = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_clear_correction_chain_snoozes_v1',
    start
  );
  assert.ok(start >= 0 && end > start);
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(
    body,
    /if v_resolution\.id is null then[\s\S]*source_family_key=v_residual->>'source_family_key'/
  );
  assert.match(
    body,
    /insert into public\.banking_pay_workbench_session_case_resolutions[\s\S]*'applied_via_linked_scope',true/
  );
  assert.match(
    body,
    /v_component->>'source_basis_fingerprint'[\s\S]*upper\(v_component->>'component_key_type'\)[\s\S]*v_component->>'component_key_value'/
  );
  assert.match(
    body,
    /CORRECTION_CHAIN_RESOLUTION_CARRIER_ID_REQUIRED/
  );
});

test('linked correction expansion never preempts an explicit pending canonical carry', () => {
  const start = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_normalise_correction_case_resolutions_v1'
  );
  const end = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_clear_correction_chain_snoozes_v1',
    start
  );
  assert.ok(start >= 0 && end > start);
  const body = correctionRuntimeSql.slice(start, end);

  const missingComponentBranch = body.indexOf('if v_resolution.id is null then');
  const pendingCarryGuard = body.indexOf(
    'banking_pay_workbench_case_resolution_carry_registrations',
    missingComponentBranch
  );
  const linkedClone = body.indexOf(
    "source_family_key=v_residual->>'source_family_key'",
    pendingCarryGuard
  );

  assert.ok(missingComponentBranch >= 0);
  assert.ok(pendingCarryGuard > missingComponentBranch);
  assert.ok(linkedClone > pendingCarryGuard);
  assert.match(
    body.slice(pendingCarryGuard, linkedClone),
    /target_session_id=p_session_id[\s\S]*candidate_id=p_candidate_id[\s\S]*status='PENDING'[\s\S]*resolution_scope_kind='CORRECTION_COMPONENT'[\s\S]*canonical_resolution_key=[\s\S]*v_component->>'canonical_correction_key'[\s\S]*continue/
  );
});

test('materialised carried correction authority is recategorised after resolution replay', () => {
  const start = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_materialise_candidate_correction_residuals_v1'
  );
  const end = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_enrich_correction_resolution_payload_v1',
    start
  );
  assert.ok(start >= 0 && end > start);
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(
    body,
    /set section=case[\s\S]*when v_resolution_pending then 'cases_resolutions'[\s\S]*when v_component_outstanding>0 then 'canonical_preview_lines'[\s\S]*else 'blocked_for_pay'/
  );
  assert.match(
    body,
    /'presentation_section',case[\s\S]*when v_resolution_pending then 'CASES_RESOLUTIONS'[\s\S]*when v_component_outstanding>0 then 'READY_TO_PAY'[\s\S]*else 'BLOCKED_FOR_PAY'/
  );
  assert.match(
    body,
    /'case_needs_resolution_now',v_component_needs_resolution[\s\S]*'is_case_resolution_satisfied',not v_component_needs_resolution[\s\S]*'policy_x_pre_draft_key_resolved',not v_component_needs_resolution/
  );
  assert.match(
    body,
    /'is_ready_for_draft',not v_resolution_pending and v_component_outstanding>0[\s\S]*'is_excluded_from_allocation',v_resolution_pending or v_component_outstanding<=0/
  );
  assert.match(
    body,
    /'presentation_reason',case[\s\S]*when v_component_needs_resolution then 'PAY_METHOD_RESOLUTION_REQUIRED'[\s\S]*when v_resolution_pending then 'LINKED_CORRECTION_RESOLUTION_PENDING'[\s\S]*when v_component_outstanding>0 then 'READY_TO_PAY'[\s\S]*else 'NO_PAY_HEADROOM'/
  );
});

test('correction-chain finance sync can build recovery from a non-recovery member template', () => {
  const start = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_rewrite_sync_correction_cases_v1'
  );
  const end = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_normalise_correction_case_resolutions_v1',
    start
  );
  assert.ok(start >= 0 && end > start);
  const body = correctionRuntimeSql.slice(start, end);

  assert.doesNotMatch(
    body,
    /where candidate_row\.candidate_id = v_candidate_id[\s\S]{0,180}and candidate_row\.desired_case_type = 'OVERPAYMENT'/
  );
  assert.match(
    body,
    /when candidate_row\.desired_case_type =\s*'OVERPAYMENT'::public\.pay_finance_case_type_enum then 0/
  );
  assert.match(
    body,
    /'OVERPAYMENT'::public\.pay_advance_kind_enum/
  );
  assert.match(
    body,
    /'OVERPAYMENT'::public\.pay_advance_reason_enum/
  );
  assert.doesNotMatch(
    body,
    /CORRECTION_CHAIN_OVERPAYMENT_SYNC_TEMPLATE_REQUIRED/
  );
});

test('zero-raw correction residual preserves VAT and cross-channel target authority', () => {
  const body = functionBody(
    'pay_correction_chain_residual_v1',
    null,
    correctionResidualSql
  );
  const balancedStart = body.indexOf('component_balanced AS (');
  const outputStart = body.indexOf('\n  )\n  SELECT', balancedStart);
  assert.ok(
    balancedStart >= 0 && outputStart > balancedStart,
    'balanced correction component stage must exist'
  );
  const balancedBlock = body.slice(balancedStart, outputStart);

  assert.match(
    balancedBlock,
    /WHEN component_row\.truth_ex_vat <> 0[\s\S]*component_row\.truth_inc_vat \/ component_row\.truth_ex_vat/
  );
  assert.match(
    balancedBlock,
    /WHEN component_row\.baseline_ex_vat <> 0[\s\S]*component_row\.baseline_inc_vat \/ component_row\.baseline_ex_vat/
  );
  assert.match(
    balancedBlock,
    /WHEN component_row\.effective_outstanding_ex_vat = 0[\s\S]*THEN 0[\s\S]*sign\(component_row\.effective_outstanding_ex_vat\)[\s\S]*resolved_target_amount_ex_vat/
  );
});

test('candidate recompute cannot reuse a READY state older than current source evidence', () => {
  const body = functionBody(
    'pay_workbench_session_recompute_candidate',
    'pay_workbench_session_recompute'
  );
  assert.match(
    body,
    /banking_pay_workbench_candidate_source_lines[\s\S]*status, ''\)\)\) = 'CURRENT'/
  );
  assert.match(
    body,
    /banking_pay_workbench_jobs[\s\S]*status, ''\)\)\) = 'SUCCEEDED'[\s\S]*WORKBENCH_CANDIDATE_SOURCE_BUILD/
  );
  assert.match(
    body,
    /app_change_counters[\s\S]*'pay_candidate:' \|\| p_candidate_id::text/
  );

  const readyStart = body.indexOf(
    "UPPER(BTRIM(COALESCE(v_candidate_state_row.status, ''))) = 'READY'"
  );
  const readyEnd = body.indexOf('RETURN jsonb_build_object(', readyStart);
  assert.ok(readyStart >= 0 && readyEnd > readyStart, 'READY fast path must exist');
  const readyGuard = body.slice(readyStart, readyEnd);
  assert.match(
    readyGuard,
    /v_candidate_state_row\.source_change_seq[\s\S]*v_current_source_change_seq[\s\S]*v_live_source_change_seq/
  );
  assert.match(
    readyGuard,
    /v_current_source_change_seq[\s\S]*>= COALESCE\(v_live_source_change_seq/
  );

  const upsertStart = body.indexOf(
    'INSERT INTO public.banking_pay_workbench_session_candidate_state'
  );
  const upsertBlock = body.slice(upsertStart);
  assert.match(upsertBlock, /COALESCE\(v_current_source_change_seq, 0\)/);
  assert.match(
    upsertBlock,
    /source_change_seq = EXCLUDED\.source_change_seq/
  );
  assert.match(
    body,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_session_recompute_candidate\([\s\S]*FROM PUBLIC, anon, authenticated/
  );
  assert.match(
    body,
    /GRANT EXECUTE ON FUNCTION public\.pay_workbench_session_recompute_candidate\([\s\S]*TO service_role/
  );
});

test('full preview materialisation publishes a current bounded candidate state', () => {
  const body = functionBody(
    'pay_workbench_preview_rows_materialise_chunk',
    'pay_workbench_mark_finance_case_dirty'
  );
  const publishStart = body.indexOf(
    'Materialisation is the final full-refresh evidence boundary'
  );
  const sampleStart = body.indexOf(
    'SELECT COALESCE(jsonb_agg(jsonb_build_object(',
    publishStart
  );
  assert.ok(
    publishStart >= 0 && sampleStart > publishStart,
    'candidate-state publication must follow materialisation and precede final reporting'
  );
  const publishBlock = body.slice(publishStart, sampleStart);

  assert.match(
    publishBlock,
    /pay_workbench_delta_update_candidate_state_v1\(uuid,uuid,uuid,jsonb\)/
  );
  assert.match(
    publishBlock,
    /WHERE scope_delta\.new_status = 'MATERIALISED'[\s\S]*LIMIT 100/
  );
  assert.match(
    publishBlock,
    /banking_pay_workbench_candidate_source_lines[\s\S]*source_change_seq/
  );
  assert.match(
    publishBlock,
    /banking_pay_workbench_jobs[\s\S]*WORKBENCH_CANDIDATE_SOURCE_BUILD/
  );
  assert.match(
    publishBlock,
    /'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'/
  );
  assert.match(
    publishBlock,
    /stale_candidate_state_update_skipped[\s\S]*PAY_WORKBENCH_CANDIDATE_STATE_PUBLISH_FAILED/
  );
  assert.match(
    body,
    /'candidate_state_update_count',[\s\S]*COALESCE\(v_candidate_state_update_count, 0\)/
  );
});


test('correction residual carrier replaces stale root components with the chain-wide residual', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  assert.ok(start >= 0 && end > start, 'correction residual materialiser must exist');
  const body = correctionRuntimeSql.slice(start, end);
  assert.match(body, /'case_components',jsonb_build_array\([\s\S]*'component_resolution_key',[\s\S]*v_component->>'canonical_correction_key'[\s\S]*'target_pay_ex_vat',case[\s\S]*when v_component_needs_resolution then null[\s\S]*else v_component_outstanding/);
  assert.match(body, /'source_entitlement_amount_ex_vat',abs\(\(v_component->>'truth_ex_vat'\)::numeric\)/);
  assert.match(
    body,
    /'source_reservation_amount_ex_vat',case[\s\S]*when v_component_needs_resolution[\s\S]*then v_suggestion_current_basis[\s\S]*else abs\([\s\S]*v_component->>'effective_source_outstanding_ex_vat'/
  );
  assert.match(body, /'section_amount_ex_vat',v_component_outstanding/);
  assert.match(
    body,
    /'economic_key',coalesce\(l\.source_row_json->'economic_key','\{\}'::jsonb\)\|\|jsonb_build_object\([\s\S]*'timesheet_id',v_root_id::text,[\s\S]*'key_type',v_component->>'component_key_type',[\s\S]*'key_value',v_component->>'component_key_value'/
  );
  assert.match(body, /economic_key_json=jsonb_build_object\([\s\S]*'timesheet_id',v_root_id::text/);
  assert.match(body, /'preview_contract',coalesce\(l\.source_row_json->'preview_contract','\{\}'::jsonb\)\|\|jsonb_build_object/);
  assert.match(body, /'selection_amount_ex_vat',case[\s\S]*when v_resolution_pending then 0[\s\S]*else v_component_outstanding/);
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
    /post_active_settled_component_basis AS \([\s\S]*public\._pay_active_settled_components\(\s*COALESCE\(v_post_sync_scope_timesheet_ids[\s\S]*GROUP BY post_active_settled_component\.timesheet_id/
  );
  assert.doesNotMatch(
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
  assert.match(
    helperBody,
    /coalesce\(\(v_residual->>'draftable'\)::boolean, false\) is not true[\s\S]*continue;/
  );

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
    'WITH correction_residuals AS',
    metadataGuardIndex
  );
  assert.ok(memberRootIndex >= 0 && metadataGuardIndex > memberRootIndex);
  assert.match(
    overpaymentSyncSql.slice(memberRootIndex, metadataGuardIndex),
    /correction_component_totals AS/
  );
  assert.match(
    overpaymentSyncSql.slice(memberRootIndex, metadataGuardIndex),
    /component\.value->>'truth_ex_vat'/
  );
  assert.match(
    overpaymentSyncSql.slice(memberRootIndex, metadataGuardIndex),
    /NOT EXISTS \([\s\S]*FROM correction_member_roots AS correction_member/
  );
});

test('the Supabase pldbgapi2 workaround is scoped to the correction-chain Banking entry points', () => {
  assert.equal(
    (correctionPlpgsqlGuardSql.match(/::regprocedure/g) || []).length,
    48,
    'the workaround must remain limited to the 48 proven call-stack entry points'
  );
  for (const setting of [
    ['mode', 'disabled'],
    ['profiler', 'off'],
    ['tracer', 'off'],
    ['constants_tracing', 'off'],
    ['cursors_leaks', 'off'],
    ['strict_cursors_leaks', 'off'],
    ['fatal_errors', 'off']
  ]) {
    assert.match(
      correctionPlpgsqlGuardSql,
      new RegExp(`plpgsql_check\\.${setting[0]} TO %L[\\s\\S]*?${setting[1]}`)
    );
  }
  assert.match(correctionPlpgsqlGuardSql, /pay_correction_chain_residual_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_ctms_correction_policy_leg_read_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_ctms_assert_payload_corrections_fresh_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /timesheet_correction_chain_scope_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_pay_batch_item_source_reservation_amount_ex_vat\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_pay_policy_x_resolve_post_draft_economic_key\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_ctms_import_correction_classify_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_ctms_candidate_correction_residuals_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_ctms_rewrite_source_build_correction_negative_components_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_ctms_rewrite_sync_authoritative_correction_negative_components_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_ctms_materialise_candidate_correction_residuals_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_ctms_rewrite_sync_correction_cases_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_workbench_candidate_source_build_chunk\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_preview_candidate_build_timesheet_snapshots\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_preview_candidate_build_finance_case_baseline\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_preview_candidate_build_canonical_lines\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_workbench_preview_line_economic_key\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_sync_overpayments_from_preview\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_preview_candidate_collect_scope\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_workbench_worker_drain_chunk\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_workbench_worker_drain_chunk_revalidated_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_finance_case_apply_taxable_channel_restructure\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /tsfin_write_snapshots_and_complete\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_export_bank_csv\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_pay_batch_bank_payment_projection_rows\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /_pay_batch_validate_freshness_base_v1\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_batch_validate_freshness\s*\(/);
  assert.match(correctionPlpgsqlGuardSql, /pay_batch_freshness_scope_seed\s*\(/);
  assert.equal(
    (correctionPlpgsqlGuardSql.match(/pay_batch_validate_freshness_chunk\s*\(/g) || []).length,
    2,
    'both chunked freshness overloads must disable the faulty runtime debugger instrumentation'
  );
  assert.equal(
    (correctionPlpgsqlGuardSql.match(/pay_batch_freshness_result_get\s*\(/g) || []).length,
    2,
    'both freshness-result overloads must disable the faulty runtime debugger instrumentation'
  );
  assert.match(
    correctionPlpgsqlGuardSql,
    /pay_settle_manual_confirm\s*\(/,
    'manual CSV settlement must disable the same faulty runtime debugger instrumentation'
  );
  assert.doesNotMatch(correctionPlpgsqlGuardSql, /\bUPDATE\b|\bINSERT\b|\bDELETE\b|\bTRUNCATE\b|\bDROP\b/i);
  assert.match(overpaymentSyncSql, /SET plpgsql_check\.mode TO 'disabled'/);
  assert.match(
    overpaymentSyncSql,
    /_pay_workbench_authoritative_scope_valid_v1[\s\S]*WHEN authoritative_session\.version IS NULL THEN 1[\s\S]*ELSE authoritative_session\.version[\s\S]*END = p_session_version/
  );
  assert.doesNotMatch(
    overpaymentSyncSql,
    /COALESCE\(authoritative_session\.version,\s*1\)\s*=\s*v_workbench_session_version/
  );
  assert.match(
    overpaymentSyncSql,
    /v_authoritative_session_valid :=[\s\S]*public\._pay_workbench_authoritative_scope_valid_v1\([\s\S]*v_workbench_session_id/
  );
  assert.match(
    overpaymentSyncSql,
    /REVOKE ALL ON FUNCTION public\._pay_workbench_authoritative_scope_valid_v1\([\s\S]*FROM PUBLIC/
  );
});

test('taxable finance restructure persists the exact fingerprint basis consumed by preview', () => {
  const body = functionBody(
    'pay_finance_case_apply_taxable_channel_restructure',
    'pay_manual_debt_adjustment_resolve_taxable_channel_change'
  );

  assert.match(
    body,
    /resolution_fingerprint\s*=\s*public\.pay_finance_component_fingerprint\([\s\S]*nullif\(v_suggestion->'suggested'->>'erni_rate_pct', ''\)::numeric[\s\S]*jsonb_strip_nulls\([\s\S]*'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE'[\s\S]*'effective_pay_date', v_effective_pay_date::text[\s\S]*'target_remaining_amount_inc_vat', trc\.target_remaining_inc[\s\S]*'note', nullif\(btrim\(coalesce\(p_note, ''\)\), ''\)/
  );
  assert.doesNotMatch(
    body,
    /taxable_channel_restructure_effective_pay_date/
  );
});

test('negative correction residuals preserve the finance-case carrier while positive residuals stay pay lines', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  const body = correctionRuntimeSql.slice(start, end);
  assert.match(body, /CORRECTION_CHAIN_OVERPAYMENT_FINANCE_CASE_CARRIER_REQUIRED/);
  assert.match(body, /v_component_outstanding < 0[\s\S]*source_row_json->>'finance_case_id'/);
  assert.match(
    body,
    /when v_component_outstanding < 0[\s\S]*'amount_ex_vat',0[\s\S]*'nominal_due_amount_ex_vat',[\s\S]*round\(abs\(v_component_outstanding\),2\)[\s\S]*'recoverable_this_pay_run_ex_vat',0/,
    'negative carriers must retain dated finance authority while remaining non-allocatable until headroom is known'
  );
  assert.match(
    body,
    /'amount_display',case[\s\S]*when v_component_needs_resolution[\s\S]*v_suggestion_current_basis[\s\S]*'preview_amount_ex_vat',0[\s\S]*'section_amount_ex_vat',0[\s\S]*'section_amount_display',case[\s\S]*when v_component_needs_resolution[\s\S]*v_suggestion_current_basis[\s\S]*'component_amount_ex_vat',0/,
    'an unresolved negative carrier must expose its decision amount without making any economic amount allocatable'
  );
  assert.match(body, /else jsonb_build_object\([\s\S]*'amount_ex_vat',v_component_outstanding/);
});

test('materialised correction chains suppress every non-carrier unresolved raw member row', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(body, /v_carrier_row_ids uuid\[\]/);
  assert.match(body, /v_carrier_row_ids:=array_append\(v_carrier_row_ids,v_carrier_row_id\)/);
  assert.match(body, /case when l\.line_key=v_line_key then 0 else 1 end/);
  assert.match(
    body,
    /and l\.timesheet_id=any\(v_member_ids\)[\s\S]*and l\.section in \('cases_resolutions','canonical_preview_lines'\)[\s\S]*and not exists \([\s\S]*unnest\(coalesce\(v_carrier_row_ids,array\[\]::uuid\[\]\)\)[\s\S]*carrier_row_id=l\.id/
  );
  assert.doesNotMatch(body, /not \(l\.id=any\(v_carrier_row_ids\)\)/);
});

test('positive dated correction residuals can claim one unretained raw TS_TOTAL carrier', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(
    body,
    /v_carrier_row_id is null[\s\S]*v_component_outstanding > 0[\s\S]*l\.section in \('cases_resolutions','canonical_preview_lines'\)[\s\S]*economic_key_json->>'key_type',''\)\)='TS_TOTAL'/
  );
  assert.match(
    body,
    /from unnest\([\s\S]*coalesce\(v_carrier_row_ids,array\[\]::uuid\[\]\)[\s\S]*retained_carrier\.carrier_row_id=l\.id/
  );
  assert.match(
    body,
    /negative component may[\s\S]*only while the coupled chain is still pending[\s\S]*Once the chain is resolved[\s\S]*must use its exact finance-case row/i
  );
});

test('positive correction carriers preserve source and target amounts on their separate authorities', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(body, /CORRECTION_CHAIN_SOURCE_OUTSTANDING_REQUIRED/);
  assert.match(body, /CORRECTION_CHAIN_SOURCE_PAY_METHOD_REQUIRED/);
  assert.match(
    body,
    /jsonb_array_elements_text\([\s\S]*v_component->'source_pay_methods'[\s\S]*in \('PAYE','UMBRELLA'\)/
  );
  assert.match(body, /'source_pay_method',v_source_pay_method/);
  assert.match(body, /'target_pay_method',upper\(v_component->>'target_pay_method'\)/);
  for (const field of [
    'source_pay_ex_vat',
    'source_amount_ex_vat',
    'source_reservation_amount_ex_vat',
    'remaining_source_amount'
  ]) {
    assert.match(
      body,
      new RegExp(
        String.raw`'${field}',case[\s\S]*?when v_component_needs_resolution` +
        String.raw`[\s\S]*?then v_suggestion_current_basis` +
        String.raw`[\s\S]*?else abs[\s\S]*?` +
        String.raw`v_component->>'effective_source_outstanding_ex_vat'`
      ),
      `${field} must project the rebased source basis while channel resolution is pending and the live source authority otherwise`
    );
  }
  assert.doesNotMatch(
    body,
    /'source_(?:pay_ex_vat|amount_ex_vat|reservation_amount_ex_vat)',abs\(\(v_component->>'target_outstanding_ex_vat'\)::numeric\)/
  );
  assert.match(
    body,
    /'target_pay_ex_vat',case[\s\S]*when v_component_needs_resolution then null[\s\S]*else v_component_outstanding/
  );
});

test('resolved correction carriers remain selectable after draft cancellation refresh', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(body, /'case_resolution_satisfied_now',not v_component_needs_resolution/);
  assert.match(body, /'has_resolved_rate',not v_component_needs_resolution/);
  assert.match(body, /'resolved_rate_family','BUCKETED'/);
  assert.match(
    body,
    /'blocked_reason_codes',case[\s\S]*when v_component_needs_resolution[\s\S]*PAY_METHOD_RESOLUTION_REQUIRED[\s\S]*when v_resolution_pending[\s\S]*LINKED_CORRECTION_RESOLUTION_PENDING[\s\S]*when v_component_outstanding>0 then '\[\]'::jsonb[\s\S]*jsonb_build_array\('NO_PAY_HEADROOM'\)/
  );
  assert.match(
    body,
    /'case_resolution_summary',[\s\S]*'case_needs_resolution',v_component_needs_resolution[\s\S]*'case_resolution_satisfied_now',not v_component_needs_resolution[\s\S]*'unresolved_taxable_count',case[\s\S]*when v_component_needs_resolution then 1[\s\S]*'blocked_reason_codes',case/
  );
  assert.match(
    body,
    /'preview_contract',[\s\S]*'key_type',v_component->>'component_key_type'[\s\S]*'key_value',v_component->>'component_key_value'[\s\S]*'selection_allowed',not v_resolution_pending and v_component_outstanding>0/
  );
  assert.match(
    body,
    /'is_excluded_from_allocation',v_resolution_pending or v_component_outstanding<=0/
  );
});

test('pending negative correction components use a locked decision carrier before finance sync', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(
    body,
    /if v_carrier_row_id is null[\s\S]*v_component_outstanding > 0[\s\S]*or v_resolution_pending[\s\S]*then/
  );
  assert.match(
    body,
    /if v_component_outstanding < 0[\s\S]*v_resolution_pending,false\) is not true[\s\S]*v_carrier_has_finance_case,false\) is not true[\s\S]*CORRECTION_CHAIN_OVERPAYMENT_FINANCE_CASE_CARRIER_REQUIRED/
  );
  assert.match(
    body,
    /'selection_allowed',not v_resolution_pending and v_component_outstanding>0/
  );
  assert.match(
    body,
    /'is_excluded_from_allocation',v_resolution_pending or v_component_outstanding<=0/
  );
});

test('targeted source builds ignore correction chains wholly outside the dirty timesheet family', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(body, /v_chain_in_source_build boolean/);
  assert.match(
    body,
    /source_line\.source_build_run_id=p_source_build_run_id[\s\S]*source_line\.timesheet_id=any\(v_member_ids\)/
  );
  assert.match(
    body,
    /if coalesce\(v_chain_in_source_build,false\) is not true then[\s\S]*continue;[\s\S]*end if;[\s\S]*for v_component/
  );
  assert.match(body, /CORRECTION_RESIDUAL_SOURCE_COMPONENT_MISSING/);
});

test('multi-component correction recoveries expand the aggregate finance parent before canonical materialisation', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  assert.ok(start >= 0 && end > start, 'correction residual materialiser must exist');
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(
    body,
    /v_component_outstanding < 0[\s\S]*v_finance_component_json[\s\S]*v_finance_component_due:=round\(abs\(v_component_outstanding\),2\)[\s\S]*insert into public\.banking_pay_workbench_candidate_source_lines/
  );
  assert.match(
    body,
    /'finance_component_id'[\s\S]*v_finance_component_json[\s\S]*'case_components'[\s\S]*jsonb_build_array\(v_finance_component_json\)/
  );
  assert.match(
    body,
    /set status='SUPERSEDED'[\s\S]*aggregate_line\.source_row_json->'case_components'[\s\S]*exact_component_line/
  );
  assert.match(
    body,
    /coalesce\([\s\S]*nested_component\.value->>'source_family_key'[\s\S]*\)<>coalesce\(v_residual->>'source_family_key',''\)/
  );
  assert.match(
    body,
    /'nominal_due_amount_ex_vat',[\s\S]*round\(abs\(v_component_outstanding\),2\)[\s\S]*'recoverable_this_pay_run_ex_vat',0/,
    'each split recovery must retain its dated nominal due while remaining non-allocatable until headroom revalidation'
  );
});

test('an upstream correction pay-method resolution cannot be applied a second time by finance sync', () => {
  const rewriteStart = correctionRuntimeSql.indexOf('create or replace function public._ctms_rewrite_sync_correction_cases_v1');
  const rewriteEnd = correctionRuntimeSql.indexOf('create or replace function public._ctms_assert_session_correction_residuals_draftable_v1', rewriteStart);
  const rewriteBody = correctionRuntimeSql.slice(rewriteStart, rewriteEnd);
  assert.match(rewriteBody, /'source_pay_method', v_residual->>'target_pay_method'/);
  assert.match(rewriteBody, /'upstream_correction_pay_method_resolution_applied', true/);

  const syncBody = functionBody('pay_finance_components_sync_from_preview');
  assert.match(syncBody, /v_upstream_correction_resolution_applied boolean := false/);
  assert.match(syncBody, /upstream_correction_pay_method_resolution_applied/);
  assert.match(syncBody, /saved_target_pay_method = CASE[\s\S]*WHEN v_upstream_correction_resolution_applied THEN NULL/);
  assert.match(syncBody, /saved_resolution_mode = CASE[\s\S]*WHEN v_upstream_correction_resolution_applied THEN NULL/);
  assert.match(syncBody, /saved_resolution_payload_json = CASE[\s\S]*WHEN v_upstream_correction_resolution_applied THEN NULL/);
  assert.match(syncBody, /resolution_fingerprint = CASE[\s\S]*WHEN v_upstream_correction_resolution_applied THEN NULL/);
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
    /v_component_outstanding=0[\s\S]*continue;/
  );
});

test('canonical correction carriers preserve server-owned suggested-rate evidence', () => {
  const start = correctionRuntimeSql.indexOf('create or replace function public._ctms_materialise_candidate_correction_residuals_v1');
  const end = correctionRuntimeSql.indexOf('create or replace function public._ctms_enrich_correction_resolution_payload_v1', start);
  assert.ok(start >= 0 && end > start, 'correction residual materialiser must exist');
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(body, /v_suggested_component jsonb/);
  assert.match(
    body,
    /source_component\.value\s*->'suggested_resolution_payload_json'[\s\S]*source_component\.value\s*->'suggested_resolution_result_json'/
  );
  assert.match(body, /CORRECTION_CHAIN_SUGGESTED_RESOLUTION_REQUIRED/);
  assert.match(body, /'is_actionable_resolution_row',v_component_needs_resolution/);
  assert.match(body, /'has_suggested_resolution',v_has_suggested_resolution/);
  assert.match(
    body,
    /'suggested_resolution_payload_json',case[\s\S]*v_suggested_component[\s\S]*'suggested_resolution_result_json',case/
  );
  assert.doesNotMatch(
    body,
    /case_components',jsonb_build_array\(\s*v_suggested_component\s*\|\|/,
    'raw source identity must not replace the canonical residual identity'
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

test('changed or incomplete source evidence requeues a previously skipped candidate line', () => {
  assert.match(
    sql,
    /WHEN UPPER\(BTRIM\(COALESCE\(public\.banking_pay_workbench_candidate_line_work\.status, ''\)\)\) = 'SKIPPED' THEN[\s\S]*?work_payload_json IS DISTINCT FROM EXCLUDED\.work_payload_json[\s\S]*?result_row_json IS NULL[\s\S]*?THEN EXCLUDED\.status/,
    'a skipped line must return to PENDING when its server-owned source evidence changes or its prior result is incomplete'
  );
  assert.match(
    sql,
    /result_row_json = CASE[\s\S]*?WHEN UPPER\(BTRIM\(COALESCE\(public\.banking_pay_workbench_candidate_line_work\.status, ''\)\)\) = 'SKIPPED' THEN[\s\S]*?work_payload_json IS DISTINCT FROM EXCLUDED\.work_payload_json[\s\S]*?result_row_json IS NULL[\s\S]*?THEN NULL::jsonb/,
    'the stale skipped result must be cleared before the changed line is reprocessed'
  );
});

test('draft items freeze correction-chain residual identity before reservation finalisation', () => {
  const body = functionBody('pay_batch_insert_items_from_preview', 'pay_batch_finalize_reservations_and_markers');
  assert.match(body, /'source_family_key', NULLIF\(BTRIM\(COALESCE\(normalised_rows\.line_json->>'source_family_key'/);
  assert.equal(
    (body.match(/'correction_chain_residual', CASE/g) || []).length,
    2,
    'both component and source-basis snapshots must freeze correction-chain residual evidence'
  );
  assert.match(
    body,
    /jsonb_typeof\(normalised_rows\.line_json->'correction_chain_residual'\) = 'object'[\s\S]*normalised_rows\.line_json->'correction_chain_residual'/
  );
  assert.match(
    body,
    /correction_chain_component,resolution_rows[\s\S]*payload_json,bucket_resolutions[\s\S]*component_key_type[\s\S]*prepared_rows\.key_type[\s\S]*component_key_value[\s\S]*prepared_rows\.key_value[\s\S]*source_pay_ex_vat/,
    'a resolved correction item must freeze its source amount from the matching canonical resolution bucket rather than the target-valued preview total'
  );
  assert.match(
    body,
    /'source_amount_ex_vat', normalised_rows\.frozen_source_amount[\s\S]*'target_amount_ex_vat', normalised_rows\.amount_ex_vat/,
    'the frozen resolution result must preserve distinct source and target amounts'
  );
});

test('resolved segment breakdowns reconcile to the payable batch-item remainder', () => {
  const body = functionBody('pay_batch_build_item_breakdowns', 'pay_batch_assert_integrity');

  assert.match(
    body,
    /frozen_target_amount_ex_vat[\s\S]*amount_ex_vat[\s\S]*AS item_total_ex_vat/,
    'the frozen payable batch-item total must be the breakdown allocation total'
  );
  assert.match(
    body,
    /AS provisional_component_ex_vat[\s\S]*AS allocated_amount_ex_vat/,
    'multiple resolved components must be proportioned and rounding-safe'
  );
  assert.match(
    body,
    /allocated_amount_ex_vat[\s\S]*AS amount_ex_vat[\s\S]*allocated_amount_ex_vat[\s\S]*derived_amount_vat[\s\S]*AS amount_inc_vat/,
    'breakdown ex-VAT and gross amounts must reconcile to the actual payable delta'
  );
  assert.match(
    body,
    /resolved_rate_resolution_id[\s\S]*resolution_state[\s\S]*FIXED[\s\S]*is_actionable_resolution_row[\s\S]*source_pay_ex_vat[\s\S]*EXISTS[\s\S]*explicit_resolved_component/,
    'an automatic fixed negative member must not be scaled into the operator breakdown when explicit resolved members explain the same economic key'
  );
  assert.match(
    body,
    /'full_resolved_target_amount_ex_vat'[\s\S]*derived_amount_ex_vat/,
    'the full resolved target remains frozen as provenance rather than being lost'
  );
});

test('final reservation checks use frozen correction-chain residual evidence without Policy X drift', () => {
  const marker = 'CREATE OR REPLACE FUNCTION public.pay_batch_finalize_reservations_and_markers';
  const start = sql.indexOf(marker);
  assert.ok(start >= 0, 'pay_batch_finalize_reservations_and_markers must exist');
  const end = sql.indexOf('$function$;', start + marker.length);
  assert.ok(end > start, 'pay_batch_finalize_reservations_and_markers must have a bounded body');
  const body = sql.slice(start, end + '$function$;'.length);
  assert.match(body, /MISSING_FROZEN_CORRECTION_CHAIN_RESIDUAL/);
  assert.match(body, /MISSING_FROZEN_CORRECTION_CHAIN_COMPONENT/);
  assert.match(body, /effective_source_outstanding_ex_vat/);
  assert.match(body, /FROZEN_CORRECTION_CHAIN_RESIDUAL/);
  assert.match(
    body,
    /WHERE scoped_component_rows\.is_correction_chain_residual IS NOT TRUE[\s\S]*public\._pay_outstanding_components/
  );
  assert.match(
    body,
    /WHEN scoped_component_rows\.is_correction_chain_residual[\s\S]*frozen_chain_outstanding_ex_vat[\s\S]*ELSE ROUND\(COALESCE\(outstanding_component_rows\.outstanding_ex_vat/
  );
  assert.doesNotMatch(body, /pay_correction_chain_residual_v1\s*\(/);
  assert.doesNotMatch(body, /\bNHSP\b|\bHR_WEEKLY\b|\bHR_DAILY\b/);
});

test('final reservation checks accept only a fresh exact pre-draft resolved target remainder', () => {
  const marker = 'CREATE OR REPLACE FUNCTION public.pay_batch_finalize_reservations_and_markers';
  const start = sql.indexOf(marker);
  assert.ok(start >= 0, 'pay_batch_finalize_reservations_and_markers must exist');
  const end = sql.indexOf('$function$;', start + marker.length);
  assert.ok(end > start, 'pay_batch_finalize_reservations_and_markers must have a bounded body');
  const body = sql.slice(start, end + '$function$;'.length);

  assert.match(body, /resolved_component_is_fresh/);
  assert.match(body, /is_resolution_stale/);
  assert.match(body, /is_stale_saved_resolution/);
  assert.match(body, /resolved_rate_resolution_id/);
  assert.match(
    body,
    /resolved_source_amount_ex_vat[\s\S]*outstanding_component_rows\.truth_ex_vat[\s\S]*resolved_target_amount_ex_vat[\s\S]*outstanding_component_rows\.baseline_ex_vat[\s\S]*requested_target_amount_ex_vat/
  );
  assert.match(
    body,
    /ROUND\(COALESCE\(outstanding_component_rows\.reserved_ex_vat,\s*0\),\s*2\)\s*=\s*0/,
    'an active reservation for the same economic component must block a second resolved draft'
  );
  assert.match(body, /FROZEN_FRESH_PRE_DRAFT_RESOLUTION_TARGET_REMAINDER/);
  assert.match(body, /FROZEN_FRESH_PRE_DRAFT_RESOLUTION_MISMATCH_OR_RESERVED/);
  assert.match(
    body,
    /SUM\([\s\S]*frozen_resolution_component\.component_json->>'source_pay_ex_vat'[\s\S]*SUM\([\s\S]*ready_preview_amount_ex_vat/,
    'fresh coupled members sharing one economic key must be validated as one source and target authority'
  );
  assert.match(
    body,
    /case_resolution_summary,case_resolution_satisfied_now/,
    'the canonical case-level resolution must be satisfied before member aggregates are accepted'
  );
  assert.doesNotMatch(
    body,
    /AS frozen_resolution_component\(component_json\)[\s\S]{0,600}LIMIT\s+1/i,
    'final reservation validation must not choose an arbitrary member from a coupled TS_DAY component'
  );
  assert.match(
    body,
    /reservation_requested_amount_ex_vat[\s\S]*requested_target_amount_ex_vat[\s\S]*requested_source_amount_ex_vat/,
    'fresh resolved components must validate the target remainder while preserving source authority'
  );
  assert.match(body, /ELSE 'LIVE_PRE_DRAFT_OUTSTANDING'/);
});

test('post-draft source reservations aggregate every fresh resolved member before the frozen target amount', () => {
  assert.equal(
    (
      sql.match(
        /CREATE OR REPLACE FUNCTION public\._pay_batch_item_source_reservation_amount_ex_vat\(/g
      ) || []
    ).length,
    1,
    'the repeatable must contain one authoritative source-reservation helper body'
  );
  const body = lastFunctionBody('_pay_batch_item_source_reservation_amount_ex_vat');
  const frozenResolvedSourceIndex = body.search(
    /SELECT ROUND\(\r?\n\s+SUM\(\(frozen_resolution_component\.component_json/
  );
  const frozenSourceAmountIndex = body.indexOf('IF v_frozen_source_amount IS NOT NULL THEN');

  assert.ok(
    frozenResolvedSourceIndex >= 0 && frozenSourceAmountIndex > frozenResolvedSourceIndex,
    'the frozen resolved source entitlement must be checked before the target-valued frozen_source_amount fallback'
  );
  assert.ok(
    body.includes("v_frozen_resolution_payload_json->'case_components'"),
    'the frozen resolution case-component catalogue must be the source authority'
  );
  assert.match(body, /component_key_type[\s\S]*v_resolved_key_type/);
  assert.match(body, /component_key_value[\s\S]*v_resolved_key_value/);
  assert.match(body, /source_pay_ex_vat/);
  assert.match(
    body,
    /SUM\(\(frozen_resolution_component\.component_json->>'source_pay_ex_vat'\)::numeric\)/,
    'coupled members sharing one TS_DAY key must contribute to one frozen source authority'
  );
  assert.doesNotMatch(
    body.slice(frozenResolvedSourceIndex, frozenSourceAmountIndex),
    /LIMIT\s+1\s*;/i,
    'the helper must not select an arbitrary member from a coupled component'
  );
  assert.match(body, /is_resolution_stale/);
  assert.match(body, /is_stale_saved_resolution/);
  assert.match(body, /requires_resolution/);
  assert.match(body, /resolved_rate_resolution_id/);
  assert.doesNotMatch(
    body.slice(frozenResolvedSourceIndex, frozenSourceAmountIndex),
    /_pay_outstanding_components|pay_correction_chain_residual_v1/,
    'post-draft source entitlement must come only from frozen batch evidence'
  );
});

test('Umbrella deductions share the coupled Umbrella destination during draft integrity', () => {
  const body = functionBody('pay_batch_assert_integrity');
  assert.equal(
    (body.match(/when di\.pay_channel = 'UMBRELLA' then/gi) || []).length,
    3,
    'entity kind, entity id and bank hash must all use the Umbrella destination'
  );
  assert.doesNotMatch(
    body,
    /di\.item_type IN \('SEGMENT_DELTA','EXPENSE_DELTA','ADJUSTMENT_DELTA','MILEAGE_DELTA'\)[\s\S]{0,120}di\.pay_channel = 'UMBRELLA'/,
    'recovery and other deductions must not fall back to a separate candidate destination'
  );
});

test('batch freshness excludes its own correction-chain reservation and compares the live chain source', () => {
  const marker = 'CREATE OR REPLACE FUNCTION public.pay_batch_validate_freshness';
  const start = laterFreshnessSql.lastIndexOf(marker);
  assert.ok(start >= 0, 'the active batch freshness function must exist');
  const end = laterFreshnessSql.indexOf(
    'CREATE OR REPLACE FUNCTION public.',
    start + marker.length
  );
  const body = laterFreshnessSql.slice(
    start,
    end > start ? end : laterFreshnessSql.length
  );

  assert.match(body, /_pay_batch_validate_freshness_base_v1\(/);
  assert.match(body, /correction_items AS \(/);
  assert.match(body, /source_family_key[\s\S]*LIKE 'correction-chain:%'/);
  assert.match(
    body,
    /public\.pay_correction_chain_residual_v1\([\s\S]*p_pay_batch_id,[\s\S]*100[\s\S]*\) AS live_residual/,
    'freshness must reconstruct live correction-chain truth while excluding this batch reservation'
  );
  assert.match(body, /expected_components AS \(/);
  assert.match(body, /live_components AS \(/);
  assert.match(body, /fresh_families AS \(/);
  assert.match(body, /flat_correction_items AS \(/);
  assert.match(body, /flat_chain_results AS \(/);
  assert.match(
    body,
    /frozen_chain_fingerprint[\s\S]*chain_fingerprint/,
    'flat canonical carrier snapshots must retain exact chain-fingerprint parity'
  );
  assert.match(
    body,
    /frozen_policy_fingerprint[\s\S]*correction_financials_policy_envelope_fingerprint/,
    'flat canonical carrier snapshots must retain exact Policy X envelope parity'
  );
  assert.match(
    body,
    /flat_item\.expected_target_ex[\s\S]*target_outstanding_ex_vat/,
    'a cross-pay-method carrier must compare its frozen target remainder to the live target remainder'
  );
  assert.match(
    body,
    /flat_item\.result_source_ex[\s\S]*flat_item\.expected_ex/,
    'the frozen result source amount must agree with the physical frozen source authority'
  );
  assert.match(
    body,
    /flat_item\.snapshot_source_ex[\s\S]*flat_item\.expected_ex/,
    'the frozen component snapshot must agree with the physical frozen source authority'
  );
  assert.match(
    body,
    /flat_item\.result_target_ex[\s\S]*flat_item\.expected_target_ex[\s\S]*flat_item\.physical_target_ex[\s\S]*flat_item\.expected_target_ex/,
    'the frozen result and physical target amount must agree with the live target remainder'
  );
  assert.match(
    body,
    /v_remaining_key_diff_count = 0[\s\S]*RESERVATION_CHANGED/,
    'the ordinary false-positive reservation reason must be removed only when no real key diff remains'
  );
  assert.match(
    sql,
    /REVOKE ALL ON FUNCTION public\._pay_batch_validate_freshness_base_v1\([\s\S]*FROM PUBLIC, anon, authenticated, service_role/,
    'the base implementation must remain owner-only'
  );
  assert.doesNotMatch(body, /\bNHSP\b|\bHR_WEEKLY\b|\bHR_DAILY\b/);
});

test('batch freshness suppresses only a fully proven resolved-timesheet self-reservation diff', () => {
  const marker = 'CREATE OR REPLACE FUNCTION public.pay_batch_validate_freshness';
  const start = laterFreshnessSql.lastIndexOf(marker);
  assert.ok(start >= 0, 'the active batch freshness function must exist');
  const end = laterFreshnessSql.indexOf(
    'CREATE OR REPLACE FUNCTION public.',
    start + marker.length
  );
  const body = laterFreshnessSql.slice(
    start,
    end > start ? end : laterFreshnessSql.length
  );

  assert.match(body, /linked_resolution_items AS \(/);
  assert.match(
    body,
    /public\._pay_outstanding_components\([\s\S]*p_pay_batch_id[\s\S]*\) AS outstanding_component/,
    'linked rows must compare live truth and other reservations while excluding only this batch'
  );
  assert.match(body, /case_resolution_satisfied_now/);
  assert.match(body, /resolved_rate_applied_via_linked_scope/);
  assert.match(body, /resolved_rate_source_anchor_case_key/);
  assert.match(body, /resolved_rate_source_anchor_timesheet_id/);
  assert.match(body, /saved_resolution_result_json,applied_via_linked_scope/);
  assert.match(
    body,
    /COUNT\(\*\)::integer AS component_count[\s\S]*ROUND\(SUM\([\s\S]*source_pay_ex_vat[\s\S]*resolved_source_ex_vat[\s\S]*ROUND\(SUM\([\s\S]*target_pay_ex_vat[\s\S]*resolved_target_ex_vat/,
    'every frozen member for one economic key must be aggregated rather than accepting a single segment'
  );
  assert.match(
    body,
    /component_count[\s\S]*= linked_resolution_item\.source_numeric_count[\s\S]*component_count[\s\S]*= linked_resolution_item\.target_numeric_count/,
    'partial or non-numeric frozen member sets must remain stale'
  );
  assert.match(
    body,
    /jsonb_array_elements\(v_fresh_chain_components\)[\s\S]*fresh_anchor/,
    'the linked row must be anchored to a correction-chain family already proven fresh'
  );
  assert.match(
    body,
    /NOT linked_resolution_item\.applied_via_linked_scope[\s\S]*anchor_timesheet_id[\s\S]*= linked_resolution_item\.timesheet_id[\s\S]*'timesheet:'[\s\S]*linked_resolution_item\.timesheet_id::text/,
    'a local approved rate may suppress only an exact self-anchored timesheet component'
  );
  assert.match(
    body,
    /outstanding_component\.truth_ex_vat[\s\S]*resolved_source_ex_vat/,
    'the current source component must still equal the frozen resolved source'
  );
  assert.match(
    body,
    /outstanding_component\.baseline_ex_vat[\s\S]*resolved_source_ex_vat/,
    'the settled baseline must still equal the complete frozen source authority'
  );
  assert.match(
    body,
    /outstanding_component\.other_reserved_ex_vat[\s\S]*<= 0\.01/,
    'a competing active reservation must prevent suppression'
  );
  assert.match(
    body,
    /frozen_resolution_result_json[\s\S]*target_amount_ex_vat[\s\S]*amount_ex_vat/,
    'the frozen result and physical batch amount must agree'
  );
  assert.match(
    body,
    /GREATEST\([\s\S]*resolved_target_ex_vat[\s\S]*baseline_ex_vat[\s\S]*0[\s\S]*\)/,
    'the physical item must equal the exact target-side remainder after settled baseline'
  );
  assert.doesNotMatch(
    body,
    /DELETE\s+FROM|UPDATE\s+public\.pay_|INSERT\s+INTO\s+public\.pay_/i,
    'freshness validation must remain read-only'
  );
});

test('batch freshness compares resolved deductions and reservations on frozen source authority', () => {
  const marker = 'CREATE OR REPLACE FUNCTION public._pay_batch_validate_freshness_base_v1';
  const start = sql.lastIndexOf(marker);
  assert.ok(start >= 0, 'the active base freshness function must exist');
  const end = sql.indexOf('CREATE OR REPLACE FUNCTION public.', start + marker.length);
  const body = sql.slice(start, end > start ? end : sql.length);

  assert.match(
    body,
    /coalesce\(par\.reserved_source_amount,\s*par\.reserved_amount,\s*0\)/i,
    'a cross-pay-method reservation must be compared using its frozen source amount'
  );
  assert.match(
    body,
    /when pbi_rt\.frozen_source_amount is not null then abs\(pbi_rt\.frozen_source_amount\)[\s\S]*frozen_remaining_source_amount/i,
    'the frozen selected source amount must take precedence over a larger case residual'
  );
  assert.match(
    body,
    /frozen_resolution_result_json->>'case_source_weekly_due'/i,
    'resolved scheduled deductions must retain their source-side weekly authority'
  );
  assert.equal(
    (body.match(/sum\(coalesce\(abs\(pbi(?:_md|_ln)?\.frozen_source_amount\),\s*-pbi(?:_md|_ln)?\.amount_ex_vat/gi) || []).length,
    3,
    'overpayment, manual-debt and loan freshness must all compare source-side amounts'
  );
  assert.equal(
    (
      body.match(
        /v_scope\s*<>\s*'PAYE'\s*or\s*coalesce\(pbc(?:_md|_ln)?\.awaiting_net_amount,\s*false\)\s*=\s*false/gi
      ) || []
    ).length,
    3,
    'PAYE overpayment, manual-debt and loan deductions must not self-stale before the first net input exists'
  );
  assert.equal(
    (
      body.match(
        /v_scope\s*<>\s*'PAYE'\s*or\s*upper\(coalesce\((?:rt|pbi|pbi_md|pbi_ln)\.paye_treatment,\s*'NET_DEDUCT'\)\)\s*not in\s*\('GROSS_DEDUCT',\s*'GROSS_DEDUCTION',\s*'DEDUCT_GROSS'\)/gi
      ) || []
    ).length,
    6,
    'PAYE gross-side deductions must stay outside the three later net-allocation templates and frozen net comparisons'
  );
});

test('the sole public freshness wrapper delegates recovery comparisons to the owner-only base', () => {
  assert.match(
    laterFreshnessSql,
    /public\._pay_batch_validate_freshness_base_v1\(\s*p_pay_batch_id,\s*p_actor_user_id,\s*p_allow_large_full_scan\s*\)/i
  );
  assert.doesNotMatch(
    laterFreshnessSql,
    /\bpar\.reserved_amount\b|\bpbi_rt\.amount_ex_vat\b/i,
    'the public correction wrapper must not reimplement or override base recovery economics'
  );
});

test('rail settlement rolls resolved recoveries up on frozen source authority', () => {
  const body = functionBody('pay_settle_rail', null, settleRailSql);

  assert.match(
    body,
    /insert into _tmp_repay_taken[\s\S]*pbi\.frozen_source_amount[\s\S]*pbi\.amount_ex_vat[\s\S]*as taken_amount/i,
    'the case roll-up must consume the same frozen source amount as its finance component'
  );
  assert.equal(
    (body.match(/pbi\.frozen_source_amount/g) || []).length >= 2,
    true,
    'both the selected rows and positive-value guard must use frozen source authority'
  );
  assert.match(
    body,
    /if exists \([\s\S]*pay_finance_case_components pfc_any[\s\S]*sum\(coalesce\(pfc_open\.remaining_source_amount,\s*0\)\)[\s\S]*into v_new_out/i,
    'componentised cases must roll up their post-settlement balance from the open source-component ledger'
  );
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

test('paged remittance rows expose server-resolved candidate, umbrella and CloudTMS user names', () => {
  const start = workerSource.indexOf('const enrichCommunicationRecipientNames = async');
  const end = workerSource.indexOf('const normaliseRouteToken =', start);
  assert.ok(start >= 0 && end > start, 'bounded communication recipient enrichment must exist');
  const body = workerSource.slice(start, end);

  assert.match(body, /table: 'candidates'/);
  assert.match(body, /table: 'umbrellas'/);
  assert.match(body, /table: 'tms_users'/);
  assert.match(body, /recipient_display_name: recipientDisplayName/);
  assert.doesNotMatch(
    body,
    /if\s*\(\s*trimText\(row\.recipient_display_name\)\s*\)\s*return\s+row/,
    'authoritative candidate and umbrella identities must replace stale projected recipient labels',
  );
  assert.match(
    body,
    /recipientKind\s*===\s*'umbrella'[\s\S]{0,200}umbrellaNames\.get\(recipientId\)\s*\|\|\s*suppliedDisplayName/,
    'umbrella remittance rows must display the umbrella entity name when the authoritative recipient id resolves',
  );
  assert.match(body, /const chunkSize = 75/);
  assert.match(workerSource, /const payload = await enrichCommunicationRecipientNames\(section, rawPayload\)/);
});

test('successful remittance delivery wakes the Banking Pay overview and remittance lanes', () => {
  const start = workerSource.indexOf('const markRemittanceDrainResult = async');
  const end = workerSource.indexOf('let sentStateRetryCount = 0', start);
  assert.ok(start >= 0 && end > start, 'remittance drain result handler must exist');
  const body = workerSource.slice(start, end);

  assert.match(body, /pay_remittance_mark_sent/);
  assert.match(body, /touchBankingPayBatchPaymentStateChanged\(env, payBatchId/);
  assert.match(body, /lanes: \['overview', 'remittances'\]/);
  assert.match(body, /REMITTANCE_DELIVERY_RECORDED/);
  assert.match(body, /PAYOUT_NOTICE_DELIVERY_RECORDED/);
});

test('explicit correction residual remaining is not consumed twice during finance component refresh', () => {
  const start = sql.indexOf('CREATE OR REPLACE FUNCTION public.pay_finance_components_sync_from_preview');
  const end = sql.indexOf('\\nCREATE OR REPLACE FUNCTION ', start + 20);
  const body = sql.slice(start, end > start ? end : undefined);

  assert.match(
    body,
    /v_new_remaining_source_amount\s*:=\s*greatest\([\s\S]*incoming_remaining_source_amount[\s\S]*\)::numeric\(12,2\)/
  );
  assert.match(body, /Correction[\s\S]*residual inputs already include settled recovery\/underpayment/);
  assert.match(
    body,
    /v_effective_source_amount\s*:=\s*greatest\([\s\S]*v_line_source_amount[\s\S]*v_consumed_amount[\s\S]*v_new_remaining_source_amount/
  );
});

test('cancelling or unwinding an unsettled draft does not add its reservation to component outstanding', () => {
  const preBankCancel = functionBody('pay_pre_bank_cancel_apply_work_item', null, preBankCancelSql);
  const noMoneyUnwind = functionBody('pay_no_money_unwind_apply_work_item', null, noMoneyUnwindSql);

  for (const body of [preBankCancel, noMoneyUnwind]) {
    assert.match(
      body,
      /remaining_source_amount,\s*0\)\s+AS remaining_after/
    );
    assert.doesNotMatch(
      body,
      /remaining_source_amount,\s*0\)\s*\+\s*COALESCE\(component_restore\.restore_source_amount/
    );
  }
});

test('a non-draftable correction pay-method mismatch cannot leave raw recovery authority behind', () => {
  const sourceStart = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_rewrite_source_build_correction_negative_components_v1'
  );
  const syncStart = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_rewrite_sync_authoritative_correction_negative_components_v1'
  );
  const nextStart = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_rewrite_sync_correction_cases_v1'
  );
  assert.ok(sourceStart >= 0 && syncStart > sourceStart && nextStart > syncStart);
  const sourceRewrite = correctionRuntimeSql.slice(sourceStart, syncStart);
  const syncRewrite = correctionRuntimeSql.slice(syncStart, nextStart);

  assert.match(
    sourceRewrite,
    /draftable[\s\S]*delete from pg_temp\._tmp_pay_wb_sync_negative_components[\s\S]*timesheet_id = any\(v_member_ids\)/
  );
  assert.match(
    syncRewrite,
    /draftable[\s\S]*delete from pg_temp\.tmp_sync_authoritative_negative_components[\s\S]*timesheet_id = any\(v_member_ids\)/
  );
});

test('correction resolution normalisation reuses the stable canonical row and removes only member aliases', () => {
  const start = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_normalise_correction_case_resolutions_v1'
  );
  const end = correctionRuntimeSql.indexOf(
    'create or replace function public._ctms_clear_correction_chain_snoozes_v1',
    start
  );
  assert.ok(start >= 0 && end > start, 'the correction resolution normaliser must exist');
  const body = correctionRuntimeSql.slice(start, end);

  assert.match(
    body,
    /resolution_identity_key=\s*v_component->>'canonical_correction_key'[\s\S]*for update/
  );
  assert.match(
    body,
    /v_normalised_resolution_id:=coalesce\(\s*v_canonical_resolution_id,\s*v_resolution\.id\s*\)/
  );
  assert.match(
    body,
    /where id=v_normalised_resolution_id/
  );
  assert.match(
    body,
    /alias_resolution\.timesheet_id=any\(v_member_ids\)[\s\S]*component_key_type[\s\S]*component_key_value/
  );
  assert.match(
    body,
    /'case_resolution_ids',v_normalised_resolution_ids/
  );
  assert.match(
    body,
    /'resolution_identity_keys',v_normalised_resolution_identity_keys/
  );
  assert.doesNotMatch(
    body,
    /delete from public\.pay_batch_items|update public\.pay_batch_items/,
    'normalising a pre-draft decision must never alter frozen batch artefacts'
  );
});

test('case resolution apply proves origin against the surviving canonical resolution IDs', () => {
  const applySql = sessionCaseResolutionSql;

  assert.match(
    applySql,
    /v_correction_normalise_result\s*:=\s*public\._ctms_normalise_correction_case_resolutions_v1/i
  );
  assert.match(
    applySql,
    /v_case_resolution_ids\s*:=\s*COALESCE\(\s*v_correction_normalise_result->'case_resolution_ids'/i
  );
  assert.match(
    applySql,
    /v_resolution_identity_keys\s*:=\s*COALESCE\(\s*v_correction_normalise_result->'resolution_identity_keys'/i
  );
  assert.match(
    applySql,
    /ON selected_resolution\.resolution_id = origin_row\.id/i
  );
});

test('failed local settlement invokes the bounded pre-provider cleanup before allowing another execution', () => {
  const start = workerSource.indexOf("if (settlementAdvance.terminal === true && settlementStatus !== 'COMPLETE')");
  const end = workerSource.indexOf("return moreWork('WAIT_LOCAL_SETTLEMENT'", start);
  assert.ok(start >= 0 && end > start, 'terminal local settlement failure branch must exist');
  const body = workerSource.slice(start, end);

  assert.match(body, /pay_execute_operation_cleanup_failed_local_artifacts/);
  assert.match(body, /p_failure_phase:\s*'WAIT_LOCAL_SETTLEMENT'/);
  assert.match(body, /provider_submission_attempted:\s*false/);
  assert.match(body, /safe_to_retry:\s*failedLocalArtifactCleanup\s*&&\s*failedLocalArtifactCleanup\.safe_to_retry\s*===\s*true/);
  assert.match(body, /retry_blocked:\s*!\(failedLocalArtifactCleanup\s*&&\s*failedLocalArtifactCleanup\.safe_to_retry\s*===\s*true\)/);
  assert.ok(
    body.indexOf('pay_execute_operation_cleanup_failed_local_artifacts') < body.indexOf('return reviewRequired('),
    'safe local cleanup must complete before the parent operation is marked for review',
  );
});

test('explicit-zero execution preserves the complete server-owned seed proof through Worker normalisation', () => {
  const seedBody = functionBody('pay_execute_bank_transfer_scope_seed', null, transferScopeSeedSql);
  const resolverStart = workerSource.indexOf('const resolveAuthoritativeProjectionProof =');
  const resolverEnd = workerSource.indexOf('const projectionProofProgressPatch =', resolverStart);
  const patchEnd = workerSource.indexOf('const routeNoTransferSeedResult =', resolverEnd);
  assert.ok(resolverStart >= 0 && resolverEnd > resolverStart && patchEnd > resolverEnd);

  const resolverBody = workerSource.slice(resolverStart, resolverEnd);
  const progressPatchBody = workerSource.slice(resolverEnd, patchEnd);
  const requiredProofFields = [
    'global_missing_explicit_paye_input_count',
    'global_explicit_zero_count',
    'global_positive_bank_payment_count',
    'global_invalid_payment_row_count',
    'scoped_missing_explicit_paye_input_count',
    'scoped_explicit_zero_count',
    'scoped_positive_bank_payment_count',
    'scoped_invalid_payment_row_count',
    'global_paye_net_state_hash',
    'global_bank_payment_projection_hash',
    'scoped_paye_net_state_hash',
    'scoped_bank_payment_projection_hash',
    'scoped_no_transfer_execution',
  ];

  for (const field of requiredProofFields) {
    assert.match(seedBody, new RegExp(`'${field}'`), `database seed must emit ${field}`);
    assert.match(resolverBody, new RegExp(field), `Worker resolver must retain ${field}`);
    assert.match(progressPatchBody, new RegExp(field), `durable progress proof must retain ${field}`);
  }
  assert.match(
    resolverBody,
    /noBankPaymentExecution !== true \|\| completeNoBankProofPresent/,
    'a pure no-bank route must fail closed unless the complete proof survives normalisation'
  );
  assert.match(
    progressPatchBody,
    /no_bank_payment_proof:\s*\(durableProof\.no_bank_payment_execution === true[\s\S]*\?\s*durableProof\s*:\s*null/,
    'the complete durable proof must be written as the nested database-authorisation proof'
  );
});

test('a zero-transfer settlement does not report a benign completion-notice skip as a communication failure', () => {
  const body = lastFunctionBody('pay_batch_display_summary_refresh');

  assert.match(
    body,
    /completion_notice_skip_reason[\s\S]*skip_reason[\s\S]*NO_MATERIALISED_TRANSFERS/
  );
  assert.match(
    body,
    /<>\s*'NO_MATERIALISED_TRANSFERS'[\s\S]*completion_notice_skipped_count/
  );
});

test('PAYE remittance scope includes a real zero-net payment-and-deduction batch', () => {
  const body = lastFunctionBody('pay_operation_remittance_scope_seed');
  const candidateScopeStart = body.indexOf('), candidate_scope AS (');
  const umbrellaScopeStart = body.indexOf('), umbrella_scope AS (', candidateScopeStart);
  assert.ok(candidateScopeStart >= 0 && umbrellaScopeStart > candidateScopeStart);
  const candidateScope = body.slice(candidateScopeStart, umbrellaScopeStart);

  assert.match(
    candidateScope,
    /HAVING[\s\S]*SUM\(eligible_item_rows\.item_amount\)[\s\S]*COUNT\(\*\) FILTER \(WHERE eligible_item_rows\.item_amount > 0\) > 0[\s\S]*COUNT\(\*\) FILTER \(WHERE eligible_item_rows\.item_amount < 0\) > 0/
  );
  assert.match(
    candidateScope,
    /remittance_overrides_enabled[\s\S]*remittance_receive_enabled[\s\S]*paye_remittances_enabled/,
    'zero-net PAYE remittances must retain the candidate-over-global receive preference'
  );
  assert.match(candidateScope, /'CANDIDATE_REMITTANCE'/);
  assert.doesNotMatch(
    candidateScope,
    /INSERT INTO public\.pay_bank_transfers|UPDATE public\.pay_bank_transfers/,
    'remittance eligibility must never create or alter a bank transfer'
  );
});
