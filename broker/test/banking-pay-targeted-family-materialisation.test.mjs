import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const read = (relativePath) =>
  fs.readFileSync(path.join(root, relativePath), 'utf8');

const helperSql = read(
  'supabase/repeatable/29072026_0018_pay_workbench_refresh_dependency_closure_v1.sql'
);
const sourceBuildSql = read(
  'supabase/repeatable/21072026_1235_39_pay_workbench_candidate_source_build_chunk.sql'
);
const draftSql = read(
  'supabase/repeatable/21072026_1235_46_pay_workbench_prepare_draft_scope_seed.sql'
);
const workbenchSql = read(
  'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'
);
const contractSql = read(
  'supabase/repeatable/25072026_1615_banking_pay_canonical_correction_carrier.sql'
);
const workerSource = read('broker/src/index.js');
const importReviewSource = read('broker/src/import-review.js');

test('targeted dependency helper is internal, bounded, and non-economic', () => {
  assert.match(
    helperSql,
    /CREATE OR REPLACE FUNCTION public\._pay_workbench_refresh_dependency_closure_v1/
  );
  assert.match(helperSql, /SECURITY INVOKER/);
  assert.match(helperSql, /FOR v_iteration IN 1\.\.4 LOOP/);
  assert.match(helperSql, /p_max_timesheets integer DEFAULT 250/);
  assert.match(helperSql, /p_max_finance_cases integer DEFAULT 100/);
  assert.match(helperSql, /economic_calculation_performed', false/);
  assert.match(helperSql, /queue_mutation_performed', false/);
  assert.match(
    helperSql,
    /REVOKE ALL ON FUNCTION public\._pay_workbench_refresh_dependency_closure_v1[\s\S]*FROM service_role/
  );
  assert.doesNotMatch(helperSql, /\bINSERT\s+INTO\s+public\.banking_pay_workbench_jobs\b/i);
  assert.doesNotMatch(helperSql, /\bUPDATE\s+public\.banking_pay_workbench_jobs\b/i);
});

test('dirty trigger enqueue no longer performs synchronous session preflight fanout', () => {
  const functionBody = workbenchSql.match(
    /CREATE OR REPLACE FUNCTION public\.pay_workbench_dirty_event_enqueue\([\s\S]*?\n\$function\$;/
  )?.[0] || '';
  assert.ok(functionBody);
  assert.doesNotMatch(
    functionBody,
    /pay_workbench_authorise_delta_hotkey_preflight/
  );
  assert.doesNotMatch(
    functionBody,
    /FOR\s+v_session_record\s+IN/i
  );
  assert.match(functionBody, /banking_pay_workbench_jobs/);
});

test('worker-owned refresh and draft functions are not browser-callable', () => {
  for (const functionName of [
    'pay_workbench_dirty_event_enqueue',
    'pay_workbench_candidate_dirty_apply_job_process',
    'pay_workbench_preview_rows_materialise_chunk',
    'pay_workbench_delta_update_candidate_state_v1'
  ]) {
    assert.match(
      workbenchSql,
      new RegExp(
        `REVOKE ALL ON FUNCTION public\\.${functionName}[\\s\\S]*?FROM PUBLIC, anon, authenticated`
      )
    );
  }

  assert.match(
    draftSql,
    /REVOKE ALL ON FUNCTION public\.pay_workbench_prepare_draft_scope_seed[\s\S]*?FROM PUBLIC, anon, authenticated/
  );
});

test('worker and source builder close targeted finance and timesheet families', () => {
  assert.match(
    workbenchSql,
    /pay_workbench_candidate_dirty_apply_job_process[\s\S]*_pay_workbench_refresh_dependency_closure_v1/
  );
  assert.match(
    workbenchSql,
    /'finance_case_ids', COALESCE\(to_jsonb\(v_finance_case_ids\), '\[\]'::jsonb\)/
  );
  assert.match(
    sourceBuildSql,
    /_pay_workbench_refresh_dependency_closure_v1/
  );
  assert.match(sourceBuildSql, /'dependency_family_kind'/);
  assert.match(sourceBuildSql, /'dependency_family_key'/);
  assert.match(
    sourceBuildSql,
    /source_line_retire\.source_row_json->>'finance_case_id'/
  );
});

test('materialisation accepts only current source or completed projection evidence', () => {
  const functionBody = workbenchSql.match(
    /CREATE OR REPLACE FUNCTION public\.pay_workbench_preview_rows_materialise_chunk\([\s\S]*?\n\$function\$;/
  )?.[0] || '';
  assert.ok(functionBody);
  assert.match(functionBody, /accepted_source\.status = 'CURRENT'/);
  assert.match(
    functionBody,
    /accepted_source\.source_build_run_id[\s\S]*result_row_json->>'source_build_run_id'/
  );
  assert.match(
    functionBody,
    /accepted_projection\.write_phase[\s\S]*'WRITE_COMPLETE'/
  );
  assert.match(
    functionBody,
    /accepted_projection\.fallback_required, false\) IS NOT TRUE/
  );
});

test('READY publication fails closed for stale or incomplete candidate authority', () => {
  const functionBody = workbenchSql.match(
    /CREATE OR REPLACE FUNCTION public\.pay_workbench_delta_update_candidate_state_v1\([\s\S]*?\n\$function\$;/
  )?.[0] || '';
  assert.ok(functionBody);
  assert.match(functionBody, /ACCEPTED_SOURCE_RUN_SEQUENCE_AMBIGUOUS/);
  assert.match(functionBody, /SOURCE_CHANGE_SEQUENCE_NOT_CURRENT/);
  assert.match(functionBody, /SOURCE_FAMILY_INCOMPLETE/);
  assert.match(
    functionBody,
    /source_guard\.source_change_seq[\s\S]{0,160}v_source_change_seq/,
    'historical DIRTY audit rows must not block adoption of the accepted source sequence',
  );
  assert.match(functionBody, /LINE_WORK_NOT_TERMINAL/);
  assert.match(functionBody, /FINANCE_CASE_COMPONENT_DUPLICATED/);
  assert.match(functionBody, /FINANCE_CASE_FAMILY_MISMATCH/);
  assert.match(
    functionBody,
    /preview_finance\.row_key\s*=\s*source_finance\.line_key/,
  );
  assert.match(
    workbenchSql,
    /v_candidate_state_source_build_run_id[\s\S]*pay_workbench_delta_update_candidate_state_v1\(\$1,\$2,\$3,\$4\)[\s\S]*v_candidate_state_source_build_run_id/,
  );
  assert.match(functionBody, /SET status = 'PENDING'/);
});

test('draft seed independently rejects stale authority without rebuilding', () => {
  assert.match(draftSql, /PAY_WORKBENCH_DRAFT_STALE_CANDIDATE_AUTHORITY/);
  assert.match(draftSql, /CANDIDATE_STATE_SEQUENCE_STALE/);
  assert.match(draftSql, /SOURCE_FAMILY_DUPLICATED/);
  assert.match(draftSql, /LINE_WORK_NOT_TERMINAL/);
  assert.doesNotMatch(
    draftSql,
    /incomplete_line\.status[\s\S]{0,180}'READY'/,
    'materialised READY line-work retained for audit is terminal and must not block draft creation',
  );
  assert.doesNotMatch(
    draftSql,
    /pay_workbench_candidate_source_build_chunk\s*\(/
  );
});

test('Banking Pay and Import Review fail closed on the additive contract marker', () => {
  const marker = 'BANKING_PAY_TARGETED_FAMILY_MATERIALISATION_V1';
  assert.match(contractSql, new RegExp(marker));
  assert.match(workerSource, new RegExp(marker));
  assert.match(importReviewSource, new RegExp(marker));
  assert.match(
    workerSource,
    /projectionContract\.targeted_family_materialisation_version/
  );
  assert.match(
    importReviewSource,
    /contract\.targeted_family_materialisation_version/
  );
});

test('the targeted fix does not introduce new financial calculations', () => {
  const changedSql = [helperSql, sourceBuildSql, draftSql].join('\n');
  assert.doesNotMatch(
    helperSql,
    /\b(vat|erni|gross_deduct|net_pay|target_rate|source_rate)\b/i
  );
  assert.match(changedSql, /policy_x_authority_scope/);
  assert.doesNotMatch(
    helperSql,
    /pay_batch_apply_finance_adjustments\s*\(/
  );
});
