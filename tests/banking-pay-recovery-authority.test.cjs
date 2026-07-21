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
