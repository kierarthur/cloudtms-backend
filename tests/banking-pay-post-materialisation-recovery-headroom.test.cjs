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
    2,
    'both aggregate and channel headroom must exclude frozen/post-draft overlays'
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
  assert.match(repeatableSql, /'action', 'RETAINED_POSITIVE_PAY_PRESENT'/);
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
