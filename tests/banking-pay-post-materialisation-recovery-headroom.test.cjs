const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repeatablePath = path.resolve(
  __dirname,
  '../supabase/repeatable/19072026_1405_revalidate_recovery_headroom_after_materialisation.sql'
);
const repeatableSql = fs.readFileSync(repeatablePath, 'utf8');
const workerSource = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);
const selectionRepeatableSql = fs.readFileSync(
  path.resolve(__dirname, '../supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'),
  'utf8'
);

test('repeatable follows the required SQL function naming and placement convention', () => {
  assert.match(path.basename(repeatablePath), /^\d{8}_\d{4}_[a-z0-9_]+\.sql$/);
  assert.match(
    repeatableSql,
    /CREATE OR REPLACE FUNCTION public\.pay_workbench_revalidate_zero_retained_recovery_headroom_v1\(/
  );
});

test('revalidator is terminal, open-session and candidate scoped', () => {
  assert.match(repeatableSql, /FROM public\.banking_pay_workbench_sessions AS session_row[\s\S]*FOR UPDATE/);
  assert.match(repeatableSql, /PAY_WORKBENCH_RECOVERY_HEADROOM_SESSION_NOT_OPEN/);
  assert.match(repeatableSql, /FROM public\.banking_pay_workbench_session_scope AS scope_row[\s\S]*scope_row\.candidate_id = p_candidate_id/);
  assert.match(repeatableSql, /DEFERRED_UNTIL_FINAL_MATERIALISATION/);
  assert.match(repeatableSql, /line_work\.status[\s\S]*NOT IN \([\s\S]*'MATERIALISED'[\s\S]*'SKIPPED'/);
});

test('revalidator uses final retained positive pay to promote stale no-headroom recoveries', () => {
  assert.match(repeatableSql, /v_retained_positive_headroom/);
  assert.match(repeatableSql, /GREATEST\(\(preview_row\.row_json->>'amount_ex_vat'\)::numeric, 0\)/);
  assert.match(repeatableSql, /IF COALESCE\(v_retained_positive_headroom, 0\) > 0 THEN/);
  assert.match(repeatableSql, /_tmp_pay_wb_positive_headroom_recovery/);
  assert.match(repeatableSql, /positive_by_channel/);
  assert.equal(
    (repeatableSql.match(/post_draft_overlay_applied[\s\S]{0,160}NOT IN \('true', 't', '1', 'yes', 'y', 'on'\)/g) || []).length,
    5,
    'aggregate, channel and repair paths must exclude frozen/post-draft overlays'
  );
  assert.match(repeatableSql, /v_repaired_authority_scope_count/);
  assert.match(repeatableSql, /REPAIRED_EXISTING_RECOVERY_AUTHORITY_SCOPE/);
  assert.match(
    repeatableSql,
    /materialisation_recovery_headroom_revalidated[\s\S]*PRE_DRAFT_LIVE_WORKBENCH_ONLY[\s\S]*pay_workbench_preview_line_contract_ok/,
    'an already-promoted pre-draft recovery must be contract-checked before its authority scope is repaired'
  );
  assert.match(repeatableSql, /presentation_reason', NULL/);
  assert.match(repeatableSql, /'presentation_section', 'READY_TO_PAY'/);
  assert.match(repeatableSql, /'recoverable_this_pay_run_ex_vat', promotion\.recoverable_amount_ex_vat/);
  assert.match(repeatableSql, /'draftable', true/);
  assert.match(repeatableSql, /'selection_allowed', true/);
  assert.match(
    repeatableSql,
    /'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'/,
    'a promoted ready row must satisfy the canonical draft-seed authority contract'
  );
  assert.match(repeatableSql, /pay_workbench_preview_line_contract_ok/);
  assert.match(repeatableSql, /'action', 'PROMOTED_RECOVERY_WITH_RETAINED_POSITIVE_PAY'/);
  assert.match(repeatableSql, /RETAINED_POSITIVE_PAY_PRESENT/);
});

test('revalidator caps every component recovery to the selected positive-pay headroom', () => {
  assert.match(repeatableSql, /_tmp_pay_wb_ready_recovery_allocation/);
  assert.match(repeatableSql, /v_selection_intent_mode[\s\S]*EXPLICIT_INCLUDE[\s\S]*IMPLICIT_ALL/);
  assert.match(
    repeatableSql,
    /v_selection_intent_mode <> 'EXPLICIT_INCLUDE'[\s\S]*positive_row\.selected[\s\S]*selection_state/
  );
  assert.match(
    repeatableSql,
    /LEAST\([\s\S]*ranked_recovery\.nominal_due_amount_ex_vat[\s\S]*ranked_recovery\.positive_headroom_ex_vat - ranked_recovery\.prior_nominal_due_amount_ex_vat/
  );
  assert.match(repeatableSql, /'recoverable_this_pay_run_ex_vat', allocation\.recoverable_amount_ex_vat/);
  assert.match(repeatableSql, /'recovery_group_recoverable_this_pay_run_ex_vat', allocation\.channel_recoverable_amount_ex_vat/);
  assert.match(repeatableSql, /'nominal_due_amount_ex_vat', promotion\.nominal_due_amount_ex_vat/);
  assert.match(repeatableSql, /'superseded_reason', 'RECOVERY_EXCEEDS_SELECTED_POSITIVE_HEADROOM'/);
  assert.match(repeatableSql, /'nominal_due_amount_ex_vat', allocation\.nominal_due_amount_ex_vat/);
  assert.equal(
    (
      repeatableSql.match(
        /WHEN COALESCE\(\([\s\S]{0,900}preview_due_amount_ex_vat[\s\S]{0,900}WHEN COALESCE\((?:ready_row|blocked_row)\.row_json->>'amount_ex_vat'/g
      ) || []
    ).length,
    2,
    'both ready and blocked recovery allocation must prefer component due over a previously capped row amount'
  );
});

test('zero retained headroom demotes recovery and clears draft selection', () => {
  assert.match(repeatableSql, /'readiness_state', 'BLOCKED_FOR_PAY'/);
  assert.match(repeatableSql, /'presentation_reason', 'NO_PAY_HEADROOM'/);
  assert.match(repeatableSql, /'recoverable_this_pay_run_ex_vat', 0/);
  assert.match(repeatableSql, /'draftable', false/);
  assert.match(repeatableSql, /'is_ready_for_draft', false/);
  assert.match(repeatableSql, /'selection_allowed', false/);
  assert.match(repeatableSql, /selection_state = 'SUPERSEDED'/);
  assert.match(repeatableSql, /pay_workbench_session_recompute_progress_counters/);
  assert.doesNotMatch(
    repeatableSql,
    /SELECT DISTINCT ON \([\s\S]{0,240}finance_case_id[\s\S]{0,240}line_type/,
    'component recoveries must not collapse into one parent row for a finance case'
  );
  assert.match(repeatableSql, /ready_row\.id AS ready_preview_row_id/);
  assert.match(repeatableSql, /ready_row\.row_key AS blocked_row_key/);
  assert.match(repeatableSql, /recovery_row\.ready_preview_row_id = ready_recovery_row\.id/);
});

test('selection mutation immediately revalidates recovery headroom for every changed candidate', () => {
  const functionStart = selectionRepeatableSql.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_selected_rows');
  const functionEnd = selectionRepeatableSql.indexOf('\nCREATE OR REPLACE FUNCTION ', functionStart + 1);
  assert.ok(functionStart >= 0, 'selection RPC definition must exist');
  const functionBody = selectionRepeatableSql.slice(
    functionStart,
    functionEnd > functionStart ? functionEnd : selectionRepeatableSql.length
  );
  assert.equal(
    (functionBody.match(/pay_workbench_revalidate_zero_retained_recovery_headroom_v1\(/g) || []).length,
    2,
    'global and ordinary selection paths must both revalidate recovery headroom'
  );
  assert.match(functionBody, /_tmp_pay_wb_global_selection_rows/);
  assert.match(functionBody, /_tmp_pay_wb_global_selection_candidates/);
  assert.match(functionBody, /_tmp_pay_wb_update_actions/);
  assert.equal(
    (functionBody.match(/SELECT session_row\.progress_counter_version[\s\S]{0,180}INTO v_next_progress_counter_version/g) || []).length,
    2,
    'both selection paths must return the authoritative progress version produced by revalidation'
  );
  assert.match(functionBody, /v_server_selected_ids := COALESCE\(v_selected_ids, '\[\]'::jsonb\)/);
});

test('materialisation executor invokes revalidation before completing its terminal job', () => {
  const start = workerSource.indexOf("if (jobType === 'WORKBENCH_PREVIEW_ROWS_MATERIALISE')");
  const end = workerSource.indexOf("if (jobType === 'PAYEE_READINESS_ENSURE')", start);
  assert.ok(start >= 0 && end > start, 'materialisation executor block must exist');
  const body = workerSource.slice(start, end);

  const materialiseIndex = body.indexOf("rpc('pay_workbench_preview_rows_materialise_chunk'");
  const revalidateIndex = body.indexOf("rpc('pay_workbench_revalidate_zero_retained_recovery_headroom_v1'");
  const finishIndex = body.indexOf("return finish('pay_workbench_preview_rows_materialise_chunk'");
  assert.ok(materialiseIndex >= 0 && revalidateIndex > materialiseIndex && finishIndex > revalidateIndex);
  assert.match(body, /result\?\.has_more !== true/);
  assert.match(body, /uuidRe\.test\(candidateId\)/);
  assert.match(body, /policy_x_authority_scope: 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'/);
});

test('aggregate database worker is wrapped without changing its core drain function', () => {
  assert.match(
    repeatableSql,
    /CREATE OR REPLACE FUNCTION public\.pay_workbench_worker_drain_chunk_revalidated_v1\(/
  );
  assert.match(
    repeatableSql,
    /v_drain_result := public\.pay_workbench_worker_drain_chunk\(/
  );
  assert.match(
    repeatableSql,
    /JOIN public\.banking_pay_workbench_jobs AS completed_job[\s\S]*completed_job\.id = \(job_item->>'job_id'\)::uuid/
  );
  assert.match(
    repeatableSql,
    /WORKBENCH_PREVIEW_ROWS_MATERIALISE[\s\S]*pay_workbench_revalidate_zero_retained_recovery_headroom_v1/
  );
  assert.match(
    workerSource,
    /sbRpc\(env, 'pay_workbench_worker_drain_chunk_revalidated_v1'/
  );
  assert.doesNotMatch(
    workerSource,
    /sbRpc\(env, 'pay_workbench_worker_drain_chunk',/
  );
});

test('draft creation guard uses only the submitted scope and keeps pay channels independent', () => {
  const selectedContractsIndex = workerSource.indexOf('const selectedPreviewRowContracts = effectiveDraftRowIds.map');
  const recoveryGuardIndex = workerSource.indexOf('const recoveryHeadroomByCandidateChannel = new Map()', selectedContractsIndex);
  assert.ok(selectedContractsIndex >= 0 && recoveryGuardIndex > selectedContractsIndex);
  assert.match(workerSource, /line_type: lineType \|\| null/);
  assert.match(workerSource, /const groupKey = `\$\{candidateId\}\|\$\{payChannel\}`/);
  assert.match(workerSource, /const amountPence = Math\.round\(amount \* 100\)/);
  assert.match(workerSource, /if \(amountPence > 0\) current\.positive_pence \+= amountPence/);
  assert.match(workerSource, /if \(amountPence < 0 && financeRecoveryLineTypes\.has\(lineType\)\)/);
  assert.match(workerSource, /if \(totals\.recovery_pence > totals\.positive_pence\)/);
  assert.match(workerSource, /BANKING_PAY_CREATE_DRAFT_RECOVERY_HEADROOM_INVALID/);
  assert.match(workerSource, /no_batch_created: true/);
});

test('change remains pre-draft and does not touch frozen batch, provider or settlement artifacts', () => {
  assert.match(repeatableSql, /PRE_DRAFT_LIVE_WORKBENCH_ONLY/);
  assert.match(repeatableSql, /post_draft_artifacts_touched', false/);
  assert.match(repeatableSql, /payment_execution_started', false/);
  assert.doesNotMatch(repeatableSql, /\bpay_batches\b|\bpay_batch_items\b|\bpay_bank_transfers\b|\bsettle\b|\bremittance\b|\brevolut\b/i);
});
