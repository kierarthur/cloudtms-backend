const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const fixturePath = path.join(
  __dirname,
  'fixtures',
  '07092026_2130_h12_source_less_cancellation_runtime_v1.json'
);
const sqlPath = path.join(
  __dirname,
  '07092026_2131_h12_source_less_cancellation_runtime_verification.sql'
);
const runnerPath = path.join(
  root,
  'scripts',
  'verify-banking-pay-source-less-cancellation-v1.mjs'
);
const baseRunnerPath = path.join(
  root,
  'scripts',
  'verify-banking-pay-draft-v1-v8-cancellation-parity.mjs'
);
const scheduledFixturePath = path.join(
  root,
  'tests',
  '04092026_2340_banking_pay_scheduled_due_local_prepare_runtime.sql'
);
const preBankPath = path.join(
  root,
  'supabase',
  'repeatable',
  '04082026_1158_pay_pre_bank_cancel_apply_work_item.sql'
);
const noMoneyPath = path.join(
  root,
  'supabase',
  'repeatable',
  '04082026_1158_pay_no_money_unwind_apply_work_item.sql'
);
const sourceLessApplyPath = path.join(
  root,
  'supabase',
  'repeatable',
  '07092026_1932_banking_pay_unpaid_cancellation_sourceless_apply_v1.sql'
);
const statusPagePath = path.join(
  root,
  'supabase',
  'repeatable',
  '04082026_1146_pay_batch_payment_status_page_v1.sql'
);
const manualCarryForwardOwnerPath = path.join(
  root,
  'supabase',
  'repeatable',
  '04092026_1330_banking_pay_manual_carry_forward_selection_authority_v1.sql'
);
const manualCarryForwardCandidatePath = path.join(
  root,
  'supabase',
  'repeatable',
  '07092026_2140_banking_pay_manual_carry_forward_economic_identity_v1.sql'
);
const semanticHelpersPath = path.join(
  root,
  'supabase',
  'repeatable',
  '09082026_0712_banking_pay_semantic_ready_helpers.sql'
);
const workerPath = path.join(root, 'broker', 'src', 'index.js');
const candidateDirtyCohortRelativePath = 'supabase/repeatable/08092026_0518_banking_pay_candidate_dirty_cohort_authority_v1.sql';
const scopeInvalidatorRelativePath = 'supabase/repeatable/08092026_0804_pay_workbench_scope_invalidate_pair_arrays_v1.sql';
const cancelReturnFrozenScopeIndexRelativePath = 'supabase/migrations/08092026_1159_banking_pay_cancel_return_frozen_scope_lookup_v1.sql';
const cancelReturnSelectionIntentRelativePath = 'supabase/repeatable/08092026_1200_banking_pay_cancel_return_selection_intent_v1.sql';
const certifiedPreviewFinalSelectionCountRelativePath = 'supabase/repeatable/08092026_1201_banking_pay_certified_preview_final_selection_count_v1.sql';
const noMoneyWorkbenchReturnRelativePath = 'supabase/repeatable/09092026_0020_banking_pay_no_money_workbench_return_v1.sql';
const scopeInvalidatorHistoricalRelativePath = 'supabase/repeatable/04082026_1139_pay_workbench_scope_invalidate_v1.sql';
const scopeInvalidatorHistoricalSha256 = 'f304e2d072d9c93f8fbe1e4ab9998b64d926a161c7b6ef4bde86dcb3ca681538';

const fixture = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));
const sql = fs.readFileSync(sqlPath, 'utf8').replaceAll('\r\n', '\n');
const runner = fs.readFileSync(runnerPath, 'utf8').replaceAll('\r\n', '\n');
const baseRunner = fs.readFileSync(baseRunnerPath);
const scheduledFixture = fs.readFileSync(scheduledFixturePath, 'utf8').replaceAll('\r\n', '\n');
const statusPage = fs.readFileSync(statusPagePath, 'utf8').replaceAll('\r\n', '\n');
const sourceLessApply = fs.readFileSync(sourceLessApplyPath, 'utf8').replaceAll('\r\n', '\n');
const semanticHelpers = fs.readFileSync(semanticHelpersPath, 'utf8').replaceAll('\r\n', '\n');
const worker = fs.readFileSync(workerPath, 'utf8').replaceAll('\r\n', '\n');

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function runHarness(argument) {
  const result = spawnSync(process.execPath, [runnerPath, argument], {
    cwd: root,
    env: {
      ...process.env,
      H12_SOURCE_LESS_MODE: 'CURRENT_RED',
      H12_SOURCE_LESS_CANDIDATE_SQL_PATHS: ''
    },
    encoding: 'utf8',
    maxBuffer: 16 * 1024 * 1024,
    windowsHide: true
  });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  return JSON.parse(result.stdout);
}

function runBlockedCandidateDescribe() {
  const paths = [
    'supabase/repeatable/07092026_2013_banking_pay_unpaid_cancellation_communication_v2_prepare_v1.sql',
    'supabase/repeatable/07092026_2014_banking_pay_unpaid_cancellation_communication_v2_expand_v1.sql',
    'supabase/repeatable/07092026_2021_banking_pay_payment_correction_integrity_communication_v2_v1.sql',
    'supabase/repeatable/07092026_1932_banking_pay_unpaid_cancellation_sourceless_apply_v1.sql',
    'supabase/repeatable/07092026_2135_banking_pay_unpaid_cancellation_status_admission_v1.sql',
    'supabase/repeatable/07092026_2140_banking_pay_manual_carry_forward_economic_identity_v1.sql'
  ];
  return spawnSync(process.execPath, [runnerPath, '--describe'], {
    cwd: root,
    env: {
      ...process.env,
      H12_SOURCE_LESS_MODE: 'CANDIDATE_GREEN',
      H12_SOURCE_LESS_CANDIDATE_SQL_PATHS: paths.join(';')
    },
    encoding: 'utf8',
    maxBuffer: 16 * 1024 * 1024,
    windowsHide: true
  });
}

function runCandidateHarness(argument, paths, sourcePath = runnerPath) {
  return spawnSync(process.execPath, [sourcePath, argument], {
    cwd: root,
    env: {
      ...process.env,
      H12_SOURCE_LESS_MODE: 'CANDIDATE_GREEN',
      H12_SOURCE_LESS_CANDIDATE_SQL_PATHS: paths.join(';')
    },
    encoding: 'utf8',
    maxBuffer: 16 * 1024 * 1024,
    windowsHide: true
  });
}

function runCandidateDescribe(paths, sourcePath = runnerPath) {
  return runCandidateHarness('--describe', paths, sourcePath);
}

function runMutatedRunner(mutate) {
  const source = fs.readFileSync(runnerPath, 'utf8').replaceAll('\r\n', '\n');
  const mutated = mutate(source);
  assert.notEqual(mutated, source, 'runner mutation must change the source');
  const temporaryPath = path.join(
    path.dirname(runnerPath),
    `.h12-source-less-mutation-${process.pid}-${crypto.randomUUID()}.mjs`
  );
  fs.writeFileSync(temporaryPath, mutated);
  try {
    return runCandidateDescribe(
      fixture.execution_manifest.candidate_sql_paths_in_install_order,
      temporaryPath
    );
  } finally {
    fs.rmSync(temporaryPath, { force: true });
  }
}

test('source-less cancellation matrix covers the exact finite 40 active logical cells in 16 packed runs', () => {
  assert.equal(fixture.artifact, 'H12_SOURCE_LESS_CANCELLATION_RUNTIME_MATRIX_V1');
  assert.equal(fixture.matrix.logical_case_count, 40);
  assert.equal(fixture.matrix.physical_run_count, 16);
  assert.deepEqual(fixture.matrix.axes.engines, ['PG17', 'PG18']);
  assert.deepEqual(fixture.matrix.axes.routes, ['PRE_BANK', 'NO_MONEY']);
  assert.deepEqual(fixture.matrix.axes.scopes, ['ONE_CANDIDATE', 'WHOLE_DRAFT']);
  assert.equal(new Set(fixture.matrix.axes.reasons).size, 5);
  assert.equal(fixture.reason_cases.length, 5);

  const described = runHarness('--describe');
  assert.equal(described.runtime_executed, false);
  assert.equal(described.logical_case_count, 40);
  assert.equal(described.physical_run_count, 16);
  assert.equal(described.candidate_source_count, 0);
  assert.equal(new Set(described.logical_cases.map((row) => row.case_id)).size, 40);
  assert.ok(described.physical_runs.every((row) => (
    row.batch_channel === 'PAYE'
      ? row.logical_case_ids.length === 3
      : row.logical_case_ids.length === 2
  )));
});

test('dual-engine execution manifest is bounded, self-contained and frozen-source gated', () => {
  const manifest = fixture.execution_manifest;
  assert.equal(
    manifest.status,
    'READY_AWAITING_FROZEN_COHORT_HASHES_AND_INDEPENDENT_REVIEW_PASS'
  );
  assert.equal(manifest.runner, 'scripts/verify-banking-pay-source-less-cancellation-v1.mjs');
  assert.equal(manifest.mode, 'CANDIDATE_GREEN');
  assert.equal(manifest.command_group_count, 8);
  assert.equal(manifest.physical_run_count, 16);
  assert.equal(manifest.logical_case_count, 40);
  assert.equal(manifest.maximum_command_wall_time_ms, 900000);
  assert.equal(manifest.active_historical_request_runtime_cases, 0);
  assert.equal(manifest.candidate_sql_paths_in_install_order.length, 19);
  assert.equal(new Set(manifest.candidate_sql_paths_in_install_order).size, 19);
  assert.deepEqual(
    manifest.candidate_sql_paths_in_install_order.slice(-5),
    [
      candidateDirtyCohortRelativePath,
      scopeInvalidatorRelativePath,
      cancelReturnSelectionIntentRelativePath,
      certifiedPreviewFinalSelectionCountRelativePath,
      noMoneyWorkbenchReturnRelativePath
    ]
  );
  assert.ok(manifest.candidate_sql_paths_in_install_order.includes(cancelReturnFrozenScopeIndexRelativePath));
  assert.equal(manifest.groups.length, 8);
  assert.equal(new Set(manifest.groups.map((row) => row.group_id)).size, 8);
  assert.equal(manifest.groups.reduce((sum, row) => sum + row.physical_runs, 0), 16);
  assert.equal(manifest.groups.reduce((sum, row) => sum + row.logical_cells, 0), 40);
  assert.ok(manifest.groups.every((row) => (
    row.channels.join(',') === 'PAYE,UMBRELLA'
      && row.physical_runs === 2
      && row.logical_cells === 5
      && row.result_status === 'PENDING_FROZEN_SOURCE'
      && /^h12_rg5_builder_cancel_pg(?:17|18)$/.test(row.target_database)
      && /^h12_sourceless_baseline_pg(?:17|18)$/.test(row.template_database)
  )));
  assert.deepEqual(
    manifest.groups.map((row) => `${row.engine}:${row.route}:${row.scope}`),
    [
      'PG17:PRE_BANK:ONE_CANDIDATE',
      'PG17:PRE_BANK:WHOLE_DRAFT',
      'PG17:NO_MONEY:ONE_CANDIDATE',
      'PG17:NO_MONEY:WHOLE_DRAFT',
      'PG18:PRE_BANK:ONE_CANDIDATE',
      'PG18:PRE_BANK:WHOLE_DRAFT',
      'PG18:NO_MONEY:ONE_CANDIDATE',
      'PG18:NO_MONEY:WHOLE_DRAFT'
    ]
  );
  assert.ok(manifest.preflight.some((row) => /NO EDIT IN FLIGHT/.test(row)));
  assert.ok(manifest.preflight.some((row) => /SHA-256 values are captured again after/.test(row)));
  assert.deepEqual(manifest.result_contract, {
    required_status: 'PASS',
    request_status: 'APPLIED',
    operation_status: 'COMPLETE',
    exact_retry_same_request_and_operation: true,
    lost_reply_no_double_effect: true,
    competing_claim_no_double_effect: true,
    workbench_terminal_current: true,
    provider_settlement_remittance_actions: 0,
    policy_or_economic_change: false,
    timeout_relaxation: false,
    source_hashes_unchanged_during_run: true
  });
  for (const relativePath of manifest.candidate_sql_paths_in_install_order) {
    assert.equal(fs.existsSync(path.join(root, ...relativePath.split('/'))), true, relativePath);
  }
  assert.match(runner, /H12_SOURCE_LESS_ENGINE/);
  assert.match(runner, /H12_SOURCE_LESS_ROUTE/);
  assert.match(runner, /H12_SOURCE_LESS_SCOPE/);
  assert.match(runner, /H12_SOURCE_LESS_CHANNEL/);
  assert.match(runner, /candidateSourceSha256Before/);
  assert.match(runner, /candidateSourceSha256After/);
  assert.match(runner, /candidate SQL changed while the runtime matrix was executing/);
  assert.match(runner, /candidate_source_hashes_unchanged: true/);
});

test('scope invalidator candidate install is exact, ordered and fail-closed before database work', () => {
  const installContract = fixture.candidate_install_contract;
  assert.equal(installContract.required_candidate_dirty_cohort_owner_path, candidateDirtyCohortRelativePath);
  assert.equal(installContract.required_scope_invalidator_owner_path, scopeInvalidatorRelativePath);
  assert.equal(installContract.required_cancel_return_frozen_scope_index_path, cancelReturnFrozenScopeIndexRelativePath);
  assert.equal(installContract.required_cancel_return_selection_intent_owner_path, cancelReturnSelectionIntentRelativePath);
  assert.equal(installContract.required_certified_preview_final_selection_count_owner_path, certifiedPreviewFinalSelectionCountRelativePath);
  assert.equal(installContract.required_no_money_workbench_return_owner_path, noMoneyWorkbenchReturnRelativePath);
  assert.deepEqual(installContract.scope_invalidator_historical_include, {
    path: scopeInvalidatorHistoricalRelativePath,
    include_basename: path.basename(scopeInvalidatorHistoricalRelativePath),
    sha256: scopeInvalidatorHistoricalSha256,
    exact_include_count: 1
  });

  const invalidatorSource = fs.readFileSync(
    path.join(root, ...scopeInvalidatorRelativePath.split('/')),
    'utf8'
  ).replaceAll('\r\n', '\n');
  const includes = [...invalidatorSource.matchAll(/^\s*\\ir\s+([^\s]+)\s*$/gm)]
    .map((match) => match[1]);
  assert.deepEqual(includes, [path.basename(scopeInvalidatorHistoricalRelativePath)]);
  assert.equal(
    sha256(fs.readFileSync(path.join(root, ...scopeInvalidatorHistoricalRelativePath.split('/')))),
    scopeInvalidatorHistoricalSha256
  );

  const accepted = runCandidateDescribe(fixture.execution_manifest.candidate_sql_paths_in_install_order);
  assert.equal(accepted.status, 0, `${accepted.stdout}\n${accepted.stderr}`);
  assert.equal(JSON.parse(accepted.stdout).candidate_source_count, 19);

  const reversed = [...fixture.execution_manifest.candidate_sql_paths_in_install_order];
  reversed.splice(-5, 2, scopeInvalidatorRelativePath, candidateDirtyCohortRelativePath);
  const reversedResult = runCandidateDescribe(reversed);
  assert.notEqual(reversedResult.status, 0);
  assert.match(`${reversedResult.stdout}\n${reversedResult.stderr}`, /must immediately follow the dirty-cohort owner/);

  const omittedResult = runCandidateDescribe(
    fixture.execution_manifest.candidate_sql_paths_in_install_order.filter(
      (relativePath) => relativePath !== scopeInvalidatorRelativePath
    )
  );
  assert.notEqual(omittedResult.status, 0);
  assert.match(`${omittedResult.stdout}\n${omittedResult.stderr}`, /scope invalidator replacement must immediately follow/);

  const wrongIncludeResult = runMutatedRunner((source) => source.replace(
    `const SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_PATH = '${scopeInvalidatorHistoricalRelativePath}';`,
    `const SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_PATH = '${candidateDirtyCohortRelativePath}';`
  ));
  assert.notEqual(wrongIncludeResult.status, 0);
  assert.match(`${wrongIncludeResult.stdout}\n${wrongIncludeResult.stderr}`, /must have one exact historical include/);

  const wrongHashResult = runMutatedRunner((source) => source.replace(
    scopeInvalidatorHistoricalSha256,
    '0'.repeat(64)
  ));
  assert.notEqual(wrongHashResult.status, 0);
  assert.match(`${wrongHashResult.stdout}\n${wrongHashResult.stderr}`, /historical include hash changed/);

  assert.match(runner, /mktemp', '-d', '\/tmp\/h12-source-less-scope-invalidator-XXXXXX'/);
  assert.match(runner, /\[candidatePath, candidateContainerPath\][\s\S]{0,240}\[H12_SCOPE_INVALIDATOR_HISTORICAL_INCLUDE_PATH, historicalContainerPath\]/);
  assert.match(runner, /finally \{[\s\S]{0,160}\['rm', '-rf', includeDirectory\]/);
});

test('fixture binds current red evidence without weakening schema-impossible detector categories', () => {
  assert.equal(fixture.schema_unreachable_detector_guards.length, 3);
  const detectorGuards = new Map(
    fixture.schema_unreachable_detector_guards.map((row) => [row.reason, row])
  );
  assert.equal(detectorGuards.get('MISSING_CANDIDATE_CONTEXT').handling, 'SOURCE_GUARD_ONLY_DO_NOT_WEAKEN_SCHEMA');
  assert.equal(detectorGuards.get('UNSUPPORTED_OR_MISSING_PAY_CHANNEL').handling, 'SOURCE_GUARD_ONLY_DO_NOT_WEAKEN_SCHEMA');
  assert.equal(
    detectorGuards.get('MISSING_UMBRELLA_PAYEE_CONTEXT').handling,
    'STATIC_HOSTILE_OR_HISTORY_GUARD_ONLY_UNLESS_ALL_FOUR_SOURCES_ARE_INDEPENDENTLY_PROVED_ABSENT'
  );
  assert.equal(detectorGuards.get('MISSING_UMBRELLA_PAYEE_CONTEXT').active_runtime_matrix, false);
  assert.equal(
    detectorGuards.get('MISSING_UMBRELLA_PAYEE_CONTEXT').observed_pg17_fixture_trace.classification,
    'FIXTURE_SHAPE_MISMATCH_NOT_DETECTOR_GAP'
  );
  assert.equal(
    fixture.immutable_prechange_mail_fixture.sha256,
    '492959d64126156839e21503f0fc39145f1cc713f6c3f3a1742ed7e37e37e95f'
  );
  assert.equal(fixture.immutable_prechange_mail_fixture.bytes, 36766);
  assert.equal(fixture.current_red_contract.required_token, 'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS');
  assert.deepEqual(
    fixture.current_red_contract.observed_dual_engine_control.map((row) => row.engine),
    ['PG17', 'PG18']
  );
  assert.ok(fixture.current_red_contract.observed_dual_engine_control.every(
    (row) => row.status === 'EXPECTED_CURRENT_RED_ADMISSION_VETO'
      && row.logical_cell_count === 3
      && row.financial_cancellation_applied === false
  ));
  assert.deepEqual(
    fixture.current_red_contract.route_scope_specific_observed_symptoms.PRE_BANK_ONE_CANDIDATE.required_evidence,
    ['draft_cancel_eligible=false']
  );
  assert.deepEqual(
    fixture.current_red_contract.route_scope_specific_observed_symptoms.PRE_BANK_WHOLE_DRAFT.required_evidence,
    ['PAYMENT_CORRECTION_SELECTION_EMPTY', 'DESCRIPTOR_INVALID']
  );
  assert.deepEqual(
    fixture.current_red_contract.route_scope_specific_observed_symptoms.NO_MONEY_ALL_SCOPES.required_evidence,
    ['rows=[]', 'eligible_matching_count=0']
  );
  for (const ownerPath of [preBankPath, noMoneyPath]) {
    const owner = fs.readFileSync(ownerPath, 'utf8');
    assert.match(owner, /SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS/);
  }
});

test('SQL fixture constructs mixed ordinary, safe and all five source-proved active ambiguity shapes', () => {
  for (const reason of fixture.matrix.axes.reasons) {
    assert.match(sql, new RegExp(`'${reason}'`));
  }
  assert.match(sql, /'ORDINARY_SOURCE_BACKED'/);
  assert.match(sql, /'SOURCE_LESS_SAFE'/);
  assert.match(sql, /'SOURCE_LESS_AMBIGUOUS'/);
  assert.match(sql, /source_backed_count'[\s\S]*?< 1/);
  assert.match(runner, /detector\.safe_minimum, 1/);
  assert.match(runner, /detector\.ambiguous_minimum, expectedReasons\.length/);
  assert.match(sql, /H12_PREACTIVATION_FIXTURE_CARDINALITY_MISMATCH/);
  assert.match(sql, /batch_row\.status IN \('DRAFT', 'SCHEDULED'\)/);
  assert.doesNotMatch(sql, /batch_row\.status IN \([^\r\n]*'(?:COMMITTED|COMPLETED|PAID|SETTLED|CANCELLED|CANCELED)'/);
  assert.match(runner, /H12_DETECTOR_REASON_SET_MISMATCH/);
  assert.match(runner, /h12LastAmbiguityActivation/);
  assert.match(runner, /activate ambiguity only after payment-state preparation/);
  assert.match(runner, /installH12Fixture\(target\);[\s\S]{0,200}h12LastAmbiguityActivation/);
  assert.match(runner, /convergeH12FixtureCreatedWorkbenchJobs\(target, batchId\)/);
  assert.match(runner, /DIRTY_TRIGGER:PAY_BANK_TRANSFERS:UPDATE/);
  assert.match(runner, /PAY_BANK_TRANSFER_EVENTS_INSERT/);
  assert.match(runner, /PAY_BANK_TRANSFER_EVENTS_UPDATE/);
  assert.match(runner, /observedReasons/);
  assert.match(runner, /expectedReasonSet/);
  assert.match(runner, /setup_jobs_converged_through_worker_order: true/);
  assert.match(runner, /normal_lane_quiescent_before_source_lane: true/);
  assert.match(runner, /correction_request_created_after_convergence: true/);
  assert.match(runner, /succeeded_failed_audit_history_preserved: true/);
  assert.doesNotMatch(runner, /iteration < 320/);
  assert.deepEqual(fixture.established_current_source_prerequisites.disposable_payment_date_contract, {
    source_fixture_edited: false,
    operation_runtime_expression: '(current_date + 1)::text',
    authorisation_runtime_expression: 'current_date + 1',
    operation_and_authorisation_dates_equal: true,
    applied_identically_in_red_and_green: true,
    purpose: 'Keep the rollback-only scheduled-payment fixture future-dated across a local-midnight/database-timezone boundary without changing schedule or payment policy.'
  });
  assert.match(scheduledFixture, /'payment_date', current_date::text/);
  assert.doesNotMatch(scheduledFixture, /'payment_date', \(current_date \+ 1\)::text/);
  assert.match(scheduledFixture, /\n    current_date,\n    'ALL',/);
  assert.doesNotMatch(scheduledFixture, /\n    \(current_date \+ 1\),\n    'ALL',/);
  assert.match(runner, /H12 disposable future payment date/);
  assert.match(runner, /H12 disposable operation payment date/);
  assert.match(runner, /H12 disposable authorisation payment date/);
  assert.match(runner, /\(current_date \+ 1\)/);
  assert.doesNotMatch(sql, /ALTER\s+TABLE[\s\S]{0,100}(?:DROP|NOT\s+VALID).*CONSTRAINT/i);
});

test('cancellation harness preserves production Workbench scheduling and never deletes durable jobs', () => {
  assert.doesNotMatch(
    runner,
    /DELETE\s+FROM\s+public\.banking_pay_workbench_jobs/i,
    'the harness must never manufacture quiescence by deleting durable Workbench jobs'
  );
  assert.doesNotMatch(
    baseRunner.toString('utf8'),
    /DELETE\s+FROM\s+public\.banking_pay_workbench_jobs/i,
    'the base runner must never manufacture quiescence by deleting durable Workbench jobs'
  );
  const baseRunnerText = baseRunner.toString('utf8').replaceAll('\r\n', '\n');
  assert.match(baseRunnerText, /banking_pay_source_publication_identity_write_v1_enabled = true/);
  assert.match(baseRunnerText, /banking_pay_source_publication_identity_enforce_v1_enabled = true/);
  assert.match(baseRunnerText, /banking_pay_same_authority_build_election_v1_enabled = true/);
  assert.match(baseRunnerText, /pay_workbench_repair_invalid_dirty_apply_jobs_v1/);
  assert.match(baseRunnerText, /pay_workbench_repair_invalid_source_authority_jobs_v1/);
  assert.match(baseRunnerText, /route: 'PRECLAIM_INVALID_DIRTY_REPAIR'/);
  assert.match(baseRunnerText, /route: 'PRECLAIM_INVALID_SOURCE_REPAIR'/);
  assert.match(baseRunnerText, /drain all normal\/dirty work[\s\S]{0,220}quiescence before entering the[\s\S]{0,100}source-build lane/i);
  assert.match(baseRunnerText, /H2_CANCEL_WORKBENCH_SOURCE_LANE_BLOCKED_BY_NORMAL_WORK/);
  assert.match(baseRunnerText, /assertCancellationWorkbenchPreScheduleInvariant\(target, batchId\)/);
  assert.match(baseRunnerText, /invalid_source_job_contract_count/);
  assert.match(baseRunnerText, /invalid_active_build_contract_count/);
  const drainStart = baseRunnerText.indexOf('function drainCancellationWorkbenchSourceBuilds');
  const drainEnd = baseRunnerText.indexOf('\nfunction release(', drainStart);
  const drainSource = baseRunnerText.slice(drainStart, drainEnd);
  assert.ok(drainStart >= 0 && drainEnd > drainStart);
  assert.ok(
    drainSource.indexOf("route: 'NORMAL'")
      < drainSource.indexOf('pay_workbench_source_build_attempt_claim_start_v1'),
    'the complete normal lane must precede the first source-build claim'
  );

  const h12CurrentStart = runner.indexOf('function establishH12CurrentWorkbenchAuthority');
  const h12CurrentEnd = runner.indexOf('\nfunction readH12PreparedSnapshotBoundary', h12CurrentStart);
  assert.ok(h12CurrentStart >= 0 && h12CurrentEnd > h12CurrentStart);
  const h12CurrentSource = runner.slice(h12CurrentStart, h12CurrentEnd);
  const assertCompleteSessionHistoryBoundary = (source) => {
    const completeConvergence = source.indexOf('const completeSessionConvergence = []');
    const firstDrain = source.indexOf(
      'const convergence = drainCancellationWorkbenchSourceBuilds(target, siblingBatchId)',
      completeConvergence
    );
    const historyCapture = source.indexOf('const historicalBefore = queryJson(target');
    const quiescentReplay = source.indexOf('const quiescentSessionReplay = []');
    const replayDrain = source.indexOf(
      'const replay = drainCancellationWorkbenchSourceBuilds(target, siblingBatchId)',
      quiescentReplay
    );
    const sourceHistoryCheck = source.indexOf(
      'assert.equal(sha256(after.historical_source_rows), sourceHistorySha256'
    );
    const previewHistoryCheck = source.indexOf(
      'assert.equal(sha256(after.historical_preview_rows), previewHistorySha256'
    );
    assert.ok(completeConvergence >= 0, 'complete session convergence evidence is required');
    assert.ok(firstDrain > completeConvergence, 'every sibling must drain before history capture');
    assert.ok(historyCapture > firstDrain, 'history cannot be captured during active sibling convergence');
    assert.ok(quiescentReplay > historyCapture, 'a separate post-capture replay is required');
    assert.ok(replayDrain > quiescentReplay, 'every sibling must be replayed after history capture');
    assert.ok(sourceHistoryCheck > replayDrain, 'source history must remain exact across replay');
    assert.ok(previewHistoryCheck > replayDrain, 'preview history must remain exact across replay');
    assert.match(source, /assert\.equal\(replay\.iteration_count, 0/);
    assert.match(source, /replay\.safe_steps\.some\(\(step\) => Number\(step\.processed \|\| 0\) !== 0\)/);
    assert.match(source, /h12_history_preservation_boundary = 'AFTER_COMPLETE_SESSION_CONVERGENCE'/);
    assert.match(source, /h12_quiescent_replay_proved_no_work = true/);
  };
  assertCompleteSessionHistoryBoundary(h12CurrentSource);
  for (const [label, mutate] of [
    ['pre-history sibling drain', (source) => source.replace(
      'const convergence = drainCancellationWorkbenchSourceBuilds(target, siblingBatchId)',
      'const convergence = { currentness: {}, iteration_count: 0, safe_steps: [] }'
    )],
    ['post-history sibling replay', (source) => source.replace(
      'const replay = drainCancellationWorkbenchSourceBuilds(target, siblingBatchId)',
      'const replay = { currentness: {}, iteration_count: 0, safe_steps: [] }'
    )],
    ['zero-work replay assertion', (source) => source.replace(
      'assert.equal(replay.iteration_count, 0',
      'assert.equal(replay.iteration_count, replay.iteration_count'
    )],
    ['exact source history comparison', (source) => source.replace(
      'assert.equal(sha256(after.historical_source_rows), sourceHistorySha256',
      'assert.equal(sourceHistorySha256, sourceHistorySha256'
    )],
    ['exact preview history comparison', (source) => source.replace(
      'assert.equal(sha256(after.historical_preview_rows), previewHistorySha256',
      'assert.equal(previewHistorySha256, previewHistorySha256'
    )]
  ]) {
    const mutated = mutate(h12CurrentSource);
    assert.notEqual(mutated, h12CurrentSource, `${label} mutation must apply`);
    assert.throws(
      () => assertCompleteSessionHistoryBoundary(mutated),
      undefined,
      `${label} mutation must be killed`
    );
  }
  assert.doesNotMatch(h12CurrentSource, /establishCurrentWorkbenchAuthority\(target, siblingBatchId\)/);
  assert.match(h12CurrentSource, /source_row\.status = 'SUPERSEDED'/);
  assert.match(h12CurrentSource, /preview_row\.status = 'SUPERSEDED'/);
  assert.match(h12CurrentSource, /duplicate_current_ordinal_count/);
  assert.match(h12CurrentSource, /active_sibling_job_count/);
  assert.match(h12CurrentSource, /active_sibling_build_count/);
  assert.match(h12CurrentSource, /h12_history_preserved = true/);
});

test('cancellation scale normalization uses the current no-work shape without inventing rate identity', () => {
  const baseRunnerText = baseRunner.toString('utf8').replaceAll('\r\n', '\n');
  const start = baseRunnerText.indexOf('function normalizeCancellationScaleFinancials');
  const end = baseRunnerText.indexOf('\nfunction readCancellationWorkbenchSchedulingState', start);
  assert.ok(start >= 0 && end > start);
  const normalization = baseRunnerText.slice(start, end);

  assert.match(normalization, /worked_start_iso = NULL,[\s\S]*worked_end_iso = NULL/);
  assert.match(normalization, /actual_schedule_json = NULL/);
  assert.match(normalization, /actual_minutes_by_day_json = NULL/);
  assert.match(
    normalization,
    /invoice_breakdown_json = '\{"mode":"SEGMENTS","segments":\[\],"totals":\{"total_pay_ex_vat":0,"total_charge_ex_vat":0,"margin_ex_vat":0\}\}'::jsonb/
  );
  assert.match(normalization, /invalid_no_work_carrier_count/);
  assert.match(normalization, /invalid_zero_unit_carrier_count/);
  assert.match(normalization, /invalid_original_timesheet_shape_count/);
  assert.match(normalization, /invalid_original_parent_total_count/);
  assert.match(normalization, /invalid_original_financial_schedule_count/);
  assert.match(normalization, /invalid_adjustment_cardinality_count/);
  assert.match(normalization, /total_pay_ex_vat IS DISTINCT FROM 1\.00/);
  assert.match(normalization, /invoice_breakdown_json IS DISTINCT FROM '\{\}'::jsonb/);
  assert.match(normalization, /adjustment_count <> 1/);
  assert.match(normalization, /invalid_adjustment_shape_count/);
  assert.match(normalization, /adjustment_snapshot_sha256/);
  assert.match(normalization, /retained_adjustment_count/);
  assert.match(normalization, /retained_adjustment_total_ex_vat/);
  assert.match(normalization, /retained_adjustment_snapshot_sha256/);
  assert.doesNotMatch(normalization, /worked_end_iso = (?:timesheet_row|financial_row)\.worked_start_iso/);
  assert.doesNotMatch(normalization, /rate_source_refs_json\s*=/);
  assert.doesNotMatch(normalization, /physical_bucket_key\s*=/);
  assert.doesNotMatch(normalization, /(?:UPDATE|DELETE\s+FROM|INSERT\s+INTO)\s+public\.ts_pay_adjustments/i);

  const productionStart = baseRunnerText.indexOf('function establishProductionShapedWorkbenchSource');
  const productionEnd = baseRunnerText.indexOf('\nfunction assertProductionShapedWorkbenchComponents', productionStart);
  assert.ok(productionStart >= 0 && productionEnd > productionStart);
  const productionShape = baseRunnerText.slice(productionStart, productionEnd);
  assert.match(productionShape, /invoice_breakdown_json = '\{\}'::jsonb/);
  assert.match(productionShape, /worked_minutes = 60/);
  assert.match(productionShape, /break_minutes = 0/);
  assert.match(productionShape, /empty SEGMENTS envelope would suppress the genuine TS_DAY row/);

  const componentProofStart = baseRunnerText.indexOf('function assertProductionShapedWorkbenchComponents');
  const componentProofEnd = baseRunnerText.indexOf('\nfunction establishCurrentWorkbenchAuthority', componentProofStart);
  assert.ok(componentProofStart >= 0 && componentProofEnd > componentProofStart);
  const componentProof = baseRunnerText.slice(componentProofStart, componentProofEnd);
  assert.match(componentProof, /components\.key_type = 'TS_DAY'/);
  assert.match(componentProof, /components\.key_type = 'ADJUSTMENT_CODE'/);
  assert.match(componentProof, /invalid_ts_day_amount_count/);
  assert.match(componentProof, /invalid_adjustment_amount_count/);
  assert.match(componentProof, /invalid_production_source_shape_count/);
});

test('mail-independence fixture covers unsafe queued, claimed, mismatched and sent evidence', () => {
  assert.equal(fixture.mail_independence_fixture.rows_per_target_candidate, 4);
  assert.deepEqual(
    fixture.mail_independence_fixture.states.map((row) => row.fixture_kind),
    [
      'UNSAFE_RELATED_QUEUED',
      'UNSAFE_RELATED_CLAIMED',
      'MISMATCHED_QUEUED',
      'RELATED_SENT'
    ]
  );
  assert.equal(
    fixture.mail_independence_fixture.required_after_cancellation.apply_owner_mail_outbox_reads_or_writes,
    0
  );
  assert.match(sql, /private\.h12_source_less_cancellation_mail_fixture_v1/);
  assert.match(sql, /h12-mail-claimed-before-cancel/);
  assert.match(sql, /binding_intentionally_incomplete', true/);
  assert.match(sql, /pg_catalog\.md5\(pg_catalog\.to_jsonb\(inserted\)::text\)/);
  assert.doesNotMatch(sql, /email_outbox_claim_ready_batch\s*\(/i);
  assert.doesNotMatch(sql, /_pay_payment_correction_mail_scope_match\s*\(/i);
});

test('conditional cancellation follow-up remains separate, SENT-only and binds the frozen local owner', () => {
  const followUp = fixture.conditional_cancellation_follow_up_contract;
  assert.equal(followUp.status, 'FROZEN_LOCAL_CANDIDATE_RUNTIME_PROOF_PENDING');
  assert.equal(
    followUp.migration_path,
    'supabase/migrations/07092026_2300_banking_pay_payment_cancellation_mail_outbox_v1.sql'
  );
  assert.equal(
    followUp.migration_sha256,
    '4902b4bf030b41f0e223a5b9db6e00113e0d3320066a8598c42822ea86c13a94'
  );
  assert.equal(
    followUp.owner_path,
    'supabase/repeatable/07092026_2301_banking_pay_payment_cancellation_notice_reconcile_v1.sql'
  );
  assert.equal(
    followUp.owner_sha256,
    '682445ae51d19f04a82eeae239d2b75e95d477872e703a3bdf1b69b9677b2189'
  );
  assert.equal(followUp.financial_cancellation_dependency, false);
  assert.equal(followUp.runs_inside_financial_apply_owner, false);
  assert.equal(followUp.external_sender_invoked_by_runtime_fixture, false);
  assert.equal(followUp.original_outbox_business_mail_provider_fields_byte_identical, true);
  assert.deepEqual(
    followUp.qualifying_sent_original_permitted_server_diagnostic_columns,
    [
      'cancel_notice_tracked',
      'cancel_notice_reconciled_sent_at_utc',
      'cancel_notice_next_attempt_at_utc',
      'cancel_notice_result_code'
    ]
  );
  assert.equal(followUp.unrelated_queued_failed_rows_fully_byte_identical, true);
  assert.equal(
    followUp.server_diagnostic_columns_change_no_delivery_payment_or_cancellation_fact,
    true
  );
  assert.equal(followUp.one_follow_up_required_only_if.length, 5);
  assert.equal(followUp.zero_follow_up_cells.length, 6);
  assert.equal(followUp.retry_and_lost_reply_maximum_follow_up_count, 1);
  assert.match(followUp.recipient_rule, /Copy the authoritative original outbox recipient/);
  assert.equal(
    followUp.deduplication_identity,
    'PAYMENT_CANCELLATION_NOTICE_V1:<immutable original outbox ID>'
  );
  assert.equal(
    fixture.candidate_install_contract.required_conditional_cancellation_follow_up_owner_path,
    'supabase/repeatable/07092026_2301_banking_pay_payment_cancellation_notice_reconcile_v1.sql'
  );
});

test('candidate manifest is frozen for local proof but remains non-release evidence', () => {
  assert.equal(
    fixture.status_page_admission_reconciliation.smallest_policy_neutral_successor.source_manifest_status,
    'FINAL_SOURCE_MANIFEST_REFROZEN'
  );
  assert.equal(fixture.candidate_install_contract.shared_release_files_may_be_edited, false);
  assert.equal(fixture.candidate_install_contract.current_owner_files_may_be_edited, false);
});

test('communication contracts preserve legacy replay and grant mail independence only to explicit fresh V2', () => {
  const compatibility = fixture.communication_contract_compatibility;
  assert.equal(compatibility.total_case_count, 4);
  assert.equal(compatibility.contract_case_count, 3);
  assert.equal(compatibility.cutover_guard_case_count, 1);
  const byId = new Map(compatibility.contracts.map((row) => [row.case_id, row]));
  assert.deepEqual([...byId.keys()], [
    'LEGACY_V1_EXACT_REPLAY',
    'LEGACY_V2_COMMUNICATION_V1_EXACT_REPLAY',
    'FRESH_V2_COMMUNICATION_V2_MAIL_INDEPENDENT'
  ]);
  assert.equal(byId.get('LEGACY_V1_EXACT_REPLAY').candidate_scope_contract_version, 1);
  assert.equal(
    byId.get('LEGACY_V1_EXACT_REPLAY').source_row_count_semantics,
    'FINANCIAL_AND_QUEUED_COMMUNICATIONS'
  );
  assert.equal(
    byId.get('LEGACY_V2_COMMUNICATION_V1_EXACT_REPLAY').communication_cleanup_contract_version,
    1
  );
  assert.equal(
    byId.get('FRESH_V2_COMMUNICATION_V2_MAIL_INDEPENDENT').communication_cleanup_contract_version,
    2
  );
  assert.equal(
    byId.get('FRESH_V2_COMMUNICATION_V2_MAIL_INDEPENDENT').mail_independent_financial_cancellation,
    true
  );
  assert.ok(compatibility.contracts.slice(0, 2).every(
    (row) => row.must_not_be_reinterpreted_as_fresh_v2 === true
  ));
  assert.equal(compatibility.runtime_proof_required.deployment_date_fallback_forbidden, true);
  assert.equal(compatibility.runtime_proof_required.candidate_paths_pending_exact_source_assignment, true);
  assert.deepEqual(compatibility.cutover_guard_cells, [{
    case_id: 'NONTERMINAL_OLD_REQUEST_DETECTED_NOT_REINTERPRETED',
    setup: 'Persist a nonterminal request under its original V1 or V2/communication-V1 marker before the candidate owners are installed.',
    expected_behavior: 'The candidate detects and resumes the exact persisted contract. It never assigns communication V2 by deployment date, retry time or missing optional output fields.',
    release_safety_evidence_only: true,
    ui_retry_claim: false
  }]);
  assert.equal(compatibility.release_precondition.active_nonterminal_v1_or_v2_communication_v1_request_count, 0);
  assert.deepEqual(compatibility.release_precondition.nonterminal_request_statuses, [
    'PLANNING', 'PLANNED', 'REQUESTED', 'AWAITING_AUTHORISATION',
    'AUTHORISED', 'EXPANDED', 'PROCESSING'
  ]);
  assert.deepEqual(compatibility.release_precondition.nonterminal_linked_payment_correction_operation_statuses, [
    'QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION',
    'WAITING_PROVIDER', 'REVIEW_REQUIRED'
  ]);
  assert.deepEqual(compatibility.release_precondition.terminal_request_history_statuses, [
    'APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'REJECTED', 'CANCELLED'
  ]);
  assert.deepEqual(compatibility.release_precondition.terminal_operation_history_statuses, [
    'COMPLETE', 'FAILED', 'CANCELLED'
  ]);
  assert.equal(compatibility.release_precondition.unexpected_or_null_status_fails_closed, true);
  assert.equal(compatibility.release_precondition.fail_closed_if_nonzero, true);
  assert.equal(compatibility.release_precondition.release_safety_evidence_only, true);
  assert.equal(compatibility.release_precondition.ui_retry_claim, false);
  assert.match(sql, /H12_ACTIVE_OLD_COMMUNICATION_CONTRACT_RELEASE_PRECONDITION_FAILED/);
  assert.match(sql, /STOP_RELEASE_AND_REVIEW_EXISTING_NONTERMINAL_REQUESTS/);
  assert.match(sql, /linked_operation\.operation_type = 'PAYMENT_CORRECTION'/);
  assert.match(sql, /linked_operation\.input_json->>'correction_request_id' = old_request\.id::text/);
  assert.match(sql, /linked_operation\.status IS NULL/);
  assert.match(sql, /'QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION'/);
  assert.match(sql, /'WAITING_PROVIDER', 'REVIEW_REQUIRED'/);
});

test('integrity-hash gate binds all three contracts and kills mixed, missing and unsupported markers', () => {
  const integrity = fixture.integrity_hash_compatibility;
  assert.match(integrity.prechange_first_divergence, /false SELECTION_HASH_MISMATCH/);
  assert.deepEqual(integrity.required_exact_hash_contracts, [
    'LEGACY_V1',
    'LEGACY_V2_COMMUNICATION_V1',
    'FRESH_V2_COMMUNICATION_V2'
  ]);
  assert.deepEqual(integrity.invalid_marker_cases, [
    'MIXED_REQUEST_AND_WORK_ITEM_MARKERS',
    'MISSING_REQUIRED_FRESH_V2_MARKER',
    'UNSUPPORTED_CANDIDATE_SCOPE_VERSION',
    'UNSUPPORTED_COMMUNICATION_CLEANUP_VERSION'
  ]);
  assert.equal(integrity.invalid_marker_outcome, 'FAIL_CLOSED_BEFORE_FINANCIAL_MUTATION');
  assert.equal(
    integrity.new_additive_checker_owner_path,
    'supabase/repeatable/07092026_2021_banking_pay_payment_correction_integrity_communication_v2_v1.sql'
  );
  assert.equal(integrity.path_status, 'GENERATOR_AUTHENTIC_PATH_ASSIGNED_LOCAL_CANDIDATE_PROOF_PENDING');
  assert.equal(integrity.payment_policy_scope_expanded, false);
});

test('Current Payment Status gap and smallest policy-neutral successor are fully mapped', () => {
  const reconciliation = fixture.status_page_admission_reconciliation;
  assert.equal(reconciliation.classification, 'CURRENT_PAYMENT_STATUS_SOURCE_LESS_CANCELLATION_ACTION_WITHHELD');
  assert.equal(reconciliation.current_owner.source_sha256, sha256(fs.readFileSync(statusPagePath)));
  assert.equal(reconciliation.current_owner.definition_sha256, '14ef0f93702c8d07a3cce7f572a0a0e9f5da677b61c473e557cb4a52859052dd');
  assert.equal(reconciliation.current_owner.identity, 'public.pay_batch_payment_status_page_v1(uuid,uuid,jsonb,text,text,integer,jsonb)');
  assert.equal(reconciliation.current_owner.statement_timeout_ms, 5000);
  assert.equal(
    reconciliation.history.manual_carry_forward_veto_introduced_by_commit,
    '7542f9c0fc05fd31cb9f9151958524a22270cc55'
  );
  assert.deepEqual(reconciliation.current_owner.execute_grantees, ['postgres', 'service_role']);
  assert.equal(reconciliation.current_owner.sole_repeatable_definition, true);
  assert.equal(
    (statusPage.match(/manual_carry_forward_blocked IS NOT TRUE/g) || []).length,
    2,
    'current owner must reproduce the exact two source-less cancellation vetoes'
  );
  assert.deepEqual(reconciliation.smallest_policy_neutral_successor.changed_predicates_exactly, [
    'release_failed_payment_eligible no longer requires manual_carry_forward_blocked IS NOT TRUE',
    'pre_provider_cancel_eligible no longer requires manual_carry_forward_blocked IS NOT TRUE'
  ]);
  assert.equal(reconciliation.smallest_policy_neutral_successor.manual_source_less_economics_reconstructed, false);
  assert.equal(reconciliation.smallest_policy_neutral_successor.manual_source_less_carry_forward_created, false);
  assert.equal(reconciliation.smallest_policy_neutral_successor.output_schema_changed, false);
  assert.equal(reconciliation.smallest_policy_neutral_successor.new_ui_action_added, false);
  assert.equal(
    reconciliation.smallest_policy_neutral_successor.provisional_local_owner_path,
    'supabase/repeatable/07092026_2135_banking_pay_unpaid_cancellation_status_admission_v1.sql'
  );
  assert.equal(
    reconciliation.smallest_policy_neutral_successor.provisional_local_owner_source_sha256,
    '86fb2a3f8bafb2bb0669026d49c51e0a488cd2e8884751baddf114c7134e0195'
  );
  assert.equal(
    reconciliation.smallest_policy_neutral_successor.transient_reviewed_source_sha256_rejected_after_disk_drift,
    '940128517fecbb68a8073062d64dab8f1494ffdcbaf7562ac50f76ac0f285de3'
  );
  assert.equal(
    reconciliation.smallest_policy_neutral_successor.source_manifest_status,
    'FINAL_SOURCE_MANIFEST_REFROZEN'
  );
  assert.equal(
    sha256(fs.readFileSync(path.join(root, reconciliation.smallest_policy_neutral_successor.provisional_local_owner_path))),
    reconciliation.smallest_policy_neutral_successor.provisional_local_owner_source_sha256
  );
  assert.equal(reconciliation.smallest_policy_neutral_successor.unchanged_hard_gates.length, 11);
  for (const sourceGuard of [
    /v_batch_terminal IS NOT TRUE/,
    /paid_or_settled IS NOT TRUE/,
    /provider_outcome_unknown IS NOT TRUE/,
    /provider_submission_in_progress IS NOT TRUE/,
    /carry_forward_freshness_blocked IS NOT TRUE/,
    /complete_candidate_instruction_scope/
  ]) assert.match(statusPage, sourceGuard);
  assert.match(statusPage, /SET statement_timeout TO '5000ms'/);
  assert.match(statusPage, /STABLE[\s\S]*PARALLEL RESTRICTED[\s\S]*SECURITY DEFINER/);
  assert.match(statusPage, /GRANT EXECUTE ON FUNCTION public\.pay_batch_payment_status_page_v1[\s\S]*TO service_role/);
});

test('status successor output deltas and every hard negative remain deterministic', () => {
  const reconciliation = fixture.status_page_admission_reconciliation;
  assert.deepEqual(reconciliation.expected_green_output_delta.pre_bank.available_actions, [[], ['DRAFT_CANCEL']]);
  assert.deepEqual(reconciliation.expected_green_output_delta.pre_bank.payment_display_state, ['ACTIVE', 'ACTIVE']);
  assert.deepEqual(reconciliation.expected_green_output_delta.terminal_no_money.available_actions, [[], ['RELEASE_FAILED_PAYMENT']]);
  assert.deepEqual(reconciliation.expected_green_output_delta.terminal_no_money.payment_display_state, ['BLOCKED', 'NOT_PAID']);
  assert.equal(reconciliation.expected_green_output_delta.all_other_row_amount_identity_status_and_snapshot_fields_byte_identical, true);
  assert.equal(reconciliation.deterministic_negative_cells.length, 13);
  assert.equal(reconciliation.deterministic_positive_cells.length, 6);
  assert.deepEqual(
    reconciliation.relevant_boolean_and_action_outputs.changed_only_because_cancellation_becomes_actionable,
    [
      'release_failed_payment_eligible',
      'pre_provider_cancel_eligible',
      'draft_cancel_eligible',
      'is_not_paid',
      'eligible_action_codes',
      'available_actions',
      'display_status',
      'payment_display_state',
      'plain_blocker'
    ]
  );
  assert.equal(reconciliation.relevant_boolean_and_action_outputs.required_unchanged.length, 17);
  assert.equal(reconciliation.pagination_and_filter_assertions.length, 4);
  assert.match(statusPage, /'draft_cancel_eligible', v_batch\.status = 'DRAFT' AND page_rows\.pre_provider_cancel_eligible/);
  assert.match(statusPage, /'release_failed_payment_eligible', page_rows\.release_failed_payment_eligible/);
  assert.match(statusPage, /'available_actions', page_rows\.available_actions/);
  assert.match(statusPage, /'plain_blocker', CASE/);
  assert.match(statusPage, /p_limit \+ 1[\s\S]*LIMIT p_limit/);
});

test('all Current Payment Status callers retain independent downstream safety gates', () => {
  const callers = fixture.status_page_admission_reconciliation.callers_and_consumers;
  assert.equal(callers.length, 4);
  assert.match(worker, /async function handleBankingPayPaymentStatusPageV1[\s\S]*pay_batch_payment_status_page_v1/);
  assert.match(worker, /async function handleBankingPayPaymentStatusResolveV1[\s\S]*pay_batch_payment_status_page_v1[\s\S]*PAYMENT_STATUS_RESOLUTION_CONTEXT_STALE/);
  assert.match(semanticHelpers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_cancel_reversion_proof_core_v1[\s\S]*pay_batch_payment_status_page_v1[\s\S]*CURRENT_CANCELABILITY_AUTHORITY_REJECTED/);
  assert.match(semanticHelpers, /CREATE OR REPLACE FUNCTION public\.pay_payment_cancellation_route_diagnostic_v1[\s\S]*pay_batch_payment_status_page_v1[\s\S]*pay_workbench_cancel_reversion_proof_core_v1/);
});

test('manual carry-forward producer omission and exact identity-only candidate are evidence-bound', () => {
  const reconciliation = fixture.manual_carry_forward_economic_identity_reconciliation;
  const currentOwner = fs.readFileSync(manualCarryForwardOwnerPath, 'utf8').replaceAll('\r\n', '\n');
  const candidate = fs.readFileSync(manualCarryForwardCandidatePath, 'utf8').replaceAll('\r\n', '\n');
  assert.equal(reconciliation.classification, 'CURRENT_CANONICAL_PRODUCER_IDENTITY_OMISSION');
  assert.equal(reconciliation.policy_or_economic_defect, false);
  assert.equal(reconciliation.current_owner.source_sha256, sha256(fs.readFileSync(manualCarryForwardOwnerPath)));
  assert.equal(reconciliation.candidate_owner.source_sha256, sha256(fs.readFileSync(manualCarryForwardCandidatePath)));
  assert.equal(reconciliation.current_owner.emits_selectable_ready_manual_carry_forward, true);
  assert.equal(reconciliation.current_owner.emits_required_nested_economic_key, false);
  assert.match(currentOwner, /'line_type', 'MANUAL_ADJUSTMENT_CARRY_FORWARD'/);
  assert.doesNotMatch(
    currentOwner,
    /'component_key_type', 'MANUAL_CARRY_FORWARD'[\s\S]{0,500}'economic_key'/
  );
  assert.match(candidate, /'component_key_type', 'MANUAL_CARRY_FORWARD'/);
  assert.match(candidate, /'component_key_value', cf_lines\.manual_adjustment_carry_forward_id::text/);
  assert.match(candidate, /'key_type', 'MANUAL_CARRY_FORWARD'/);
  assert.match(candidate, /'key_value', cf_lines\.manual_adjustment_carry_forward_id::text/);
  assert.match(
    candidate,
    /'economic_key', jsonb_strip_nulls\(jsonb_build_object\([\s\S]{0,300}'timesheet_id'[\s\S]{0,300}'key_type', 'MANUAL_CARRY_FORWARD'[\s\S]{0,300}'key_value'/
  );
  assert.equal(reconciliation.candidate_owner.amount_sign_tax_vat_channel_payee_or_eligibility_changed, false);
  assert.equal(reconciliation.current_red_evidence_preserved, true);
  assert.equal(reconciliation.candidate_owner.pg17_preserved_red_boundary_replay.all_terminal_current, true);
  assert.equal(reconciliation.candidate_owner.pg17_preserved_red_boundary_replay.candidate_count, 2);
  assert.equal(reconciliation.candidate_owner.pg17_preserved_red_boundary_replay.terminal_current_count, 2);
  assert.deepEqual(
    reconciliation.candidate_owner.pg17_preserved_red_boundary_replay.source_build_elapsed_ms,
    [1193, 939]
  );
  assert.equal(
    reconciliation.candidate_owner.pg17_preserved_red_boundary_replay.source_preview_missing_or_changed_amount_channel_count,
    0
  );
  assert.equal(reconciliation.candidate_owner.pg17_preserved_red_boundary_replay.pg18_and_complete_candidate_matrix_pending, true);
});

test('candidate harness requires both exact apply owners and preserves old-replay source vocabulary', () => {
  assert.match(runner, /07092026_2013_banking_pay_unpaid_cancellation_communication_v2_prepare_v1\.sql/);
  assert.match(runner, /07092026_2014_banking_pay_unpaid_cancellation_communication_v2_expand_v1\.sql/);
  assert.match(runner, /07092026_2140_banking_pay_manual_carry_forward_economic_identity_v1\.sql/);
  assert.match(runner, /manual carry-forward identity owner missing/);
  assert.equal(
    fixture.candidate_install_contract.required_manual_carry_forward_identity_owner_path,
    'supabase/repeatable/07092026_2140_banking_pay_manual_carry_forward_economic_identity_v1.sql'
  );
  assert.match(runner, /superseded never-created 2011\/2012 path must not be consumed/);
  assert.match(runner, /pay_payment_correction_selection_prepare_chunk_v1/);
  assert.match(runner, /pay_payment_correction_expand_work/);
  assert.match(runner, /pay_payment_correction_integrity_check_v1/);
  assert.match(runner, /integrity checker must use its exact additive owner path/);
  assert.match(runner, /SET\\s\+statement_timeout\\s\+TO\\s\+'5000ms'/);
  assert.match(runner, /pay_pre_bank_cancel_apply_work_item/);
  assert.match(runner, /pay_no_money_unwind_apply_work_item/);
  assert.match(runner, /must retain legacy mail replay/);
  assert.match(runner, /must retain the historical V1\\\/V2-c1 mail matcher/);
  assert.match(runner, /explicit communication-contract V2 path/);
  assert.match(runner, /FINANCIAL_AND_QUEUED_COMMUNICATIONS/);
  assert.match(runner, /FINANCIAL_ONLY/);
  assert.match(runner, /ownerCount, 1/);
  assert.match(runner, /candidate definition missing or duplicated/);
  assert.match(runner, /mail_changed_or_missing_count/);
  assert.match(runner, /mail_status_mismatch_count/);
  assert.match(runner, /mail_lease_mismatch_count/);
  assert.match(runner, /mail_claimed_fixture_count/);
  assert.match(runner, /mail_sent_fixture_count/);
  assert.match(runner, /FROM changed AS item_row/);
  assert.match(runner, /work_item_communication_contract_mismatch_count/);
  assert.match(runner, /applied_result_communication_contract_mismatch_count/);
  assert.match(runner, /current owner unexpectedly passed/);
  assert.match(runner, /current-red cancellation admission veto absent/);
});

test('harness preserves the established runner and its generated transforms parse on both engines', () => {
  assert.equal(
    sha256(baseRunner),
    'd783120af8fb86ed72fc277e09b5b828a1c6f1c8069cb9151e53f26c14feeade'
  );
  const checked = runHarness('--check-transform');
  assert.equal(checked.ok, true);
  assert.deepEqual(checked.checked_engines, ['PG17', 'PG18']);
  assert.equal(checked.runtime_executed, false);
  const candidateChecked = runCandidateHarness(
    '--check-transform',
    fixture.execution_manifest.candidate_sql_paths_in_install_order
  );
  assert.equal(candidateChecked.status, 0, `${candidateChecked.stdout}\n${candidateChecked.stderr}`);
  const candidateCheckResult = JSON.parse(candidateChecked.stdout);
  assert.equal(candidateCheckResult.ok, true);
  assert.deepEqual(candidateCheckResult.checked_engines, ['PG17', 'PG18']);
  assert.equal(candidateCheckResult.runtime_executed, false);
});

test('prepared snapshot optimization is bounded at the exact pre-route Workbench boundary', () => {
  assert.match(runner, /function readH12PreparedSnapshotBoundary\(target\)/);
  assert.match(runner, /function prepareH12FreshPreCancellationBoundary\(target\)/);
  assert.match(
    runner,
    /boundary: 'AFTER_WORKBENCH_PRE_SCHEDULE_INVARIANT_BEFORE_SCHEDULED_PREPARATION'/
  );
  assert.match(runner, /H12 immutable pre-cancellation snapshot boundary/);
  assert.match(runner, /route_mutation_started: false/);
  assert.match(runner, /private\.h12_source_less_cancellation_fixture_v1/);
  assert.match(runner, /private\.h12_source_less_cancellation_mail_fixture_v1/);
  assert.match(runner, /fixture_absent: true/);
  assert.match(runner, /PARTITION BY batch_row\.source_workbench_session_id/);
  assert.match(runner, /one_anchor_per_distinct_source_session: true/);
  assert.match(runner, /runBaseCancellation\(target, anchor\.channel, anchor\.batch_id\)/);
  assert.match(runner, /prepared snapshots cannot prepare themselves/);

  const boundaryTransform = runner.indexOf('H12 immutable pre-cancellation snapshot boundary');
  const ambiguityTransform = runner.indexOf('activate ambiguity only after payment-state preparation');
  assert.ok(boundaryTransform >= 0 && ambiguityTransform > boundaryTransform);
  const baseInvariant = baseRunner.toString('utf8').indexOf(
    'const preScheduleWorkbenchInvariant = assertCancellationWorkbenchPreScheduleInvariant(target, batchId);'
  );
  const baseRouteMutation = baseRunner.toString('utf8').indexOf(
    "const scheduledPreparation = paymentState === 'SCHEDULED_LOCAL_NOT_SENT'"
  );
  assert.ok(baseInvariant >= 0 && baseRouteMutation > baseInvariant);

  assert.match(runner, /pg_temp\.h12_relation_manifest_v1/);
  assert.match(runner, /row_multiset_md5/);
  assert.match(runner, /pg_catalog\.pg_sequences/);
  assert.match(runner, /function_catalog_md5/);
  assert.match(runner, /trigger_catalog_md5/);
  assert.match(runner, /constraint_catalog_md5/);
  assert.match(runner, /H12_PREPARED_SNAPSHOT_SEMANTIC_DRIFT/);
  assert.match(runner, /H12_PREPARED_SNAPSHOT_EXACT_DRIFT/);
  assert.match(runner, /assert\.deepEqual\(cloneBoundary\.exact_manifest, legacyBoundary\.exact_manifest\)/);
  assert.match(runner, /legacy_fresh_to_prepared_clone_exact_equal: true/);
  assert.match(runner, /prepared_snapshot_verified_before_mutation: true/);
  assert.match(runner, /for \(const channel of \['PAYE', 'UMBRELLA'\]\)/);
  assert.match(runner, /provider_attempt_count/);
  assert.match(runner, /provider_event_count/);
  assert.match(runner, /provider_effect_count/);
  assert.match(runner, /settlement_effect_count/);
  assert.match(runner, /remittance_effect_count/);
  assert.match(runner, /external_effect_count/);
  assert.match(runner, /provider_attempts_absent/);
  assert.match(runner, /provider_events_absent/);
  assert.match(runner, /provider_effects_absent/);
  assert.match(runner, /external_effects_absent/);
  assert.match(runner, /semantic\.provider_attempt_count, 0/);
  assert.match(runner, /semantic\.provider_event_count, 0/);
  assert.match(runner, /semantic\.provider_effect_count, 0/);
  assert.match(runner, /semantic\.external_effect_count, 0/);

  assert.match(runner, /H12_PREPARED_TEMPLATE_DATABASE/);
  assert.match(runner, /\^h12_rg5_prepared_pg\(\?:17\|18\)\$/);
  assert.match(runner, /H12_SOURCE_LESS_USE_PREPARED_SNAPSHOT: 'true'/);
  assert.match(runner, /templateDatabase = usePreparedSnapshot \|\| verifyPreparedSnapshot/);
  assert.match(runner, /target\.database, \/\^\(\?:h2_cancel_\(\?:v1\|v8\)\|h12_rg5_builder_cancel\)_pg/);
  assert.match(runner, /H12 result path must stay directly inside codex_outputs\/h12-banking-draft-v8/);
  assert.match(runner, /P12_SOURCE_LESS_CANCELLATION_RUNTIME_RESULTS_\$\{enginesKey\}_V1\.json/);
  assert.match(runner, /function dropTaskOwnedTargetDatabase\(engine\)/);
  assert.match(
    runner,
    /if \(!outerExecutionCompleted[\s\S]{0,160}snapshotLifecycleStarted\.has\(engineName\)[\s\S]{0,160}!KEEP_FAILED_TARGET_DATABASE\)/
  );
  assert.match(runner, /dropTaskOwnedTargetDatabase\(engines\[engineName\]\)/);
  assert.match(runner, /dropPreparedTemplateDatabase\(engines\[engineName\]\)/);
  assert.doesNotMatch(runner, /DELETE\s+FROM\s+public\.banking_pay_workbench_jobs/i);
});

test('result output rejects a pre-existing direct-child junction before runtime', () => {
  const outputDirectory = path.join(root, 'codex_outputs', 'h12-banking-draft-v8');
  const targetDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'h12-result-reparse-target-'));
  const resultLink = path.join(
    outputDirectory,
    `P12_SOURCE_LESS_CANCELLATION_RUNTIME_RESULTS_PG17_REPARSE_${process.pid}_V1.json`
  );
  try {
    fs.symlinkSync(targetDirectory, resultLink, process.platform === 'win32' ? 'junction' : 'dir');
    const rejected = spawnSync(process.execPath, [runnerPath, '--check-result-path'], {
      cwd: root,
      env: {
        ...process.env,
        H12_SOURCE_LESS_MODE: 'CURRENT_RED',
        H12_SOURCE_LESS_CANDIDATE_SQL_PATHS: '',
        H12_SOURCE_LESS_ENGINE: 'PG17',
        H12_SOURCE_LESS_RESULT_PATH: resultLink
      },
      encoding: 'utf8',
      maxBuffer: 16 * 1024 * 1024,
      windowsHide: true
    });
    assert.notEqual(rejected.status, 0);
    assert.match(
      `${rejected.stdout}\n${rejected.stderr}`,
      /H12_RESULT_DESTINATION_REPARSE_POINT_REJECTED/
    );
    assert.doesNotMatch(`${rejected.stdout}\n${rejected.stderr}`, /runtime_executed":true/);
  } finally {
    try {
      fs.unlinkSync(resultLink);
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
    }
    fs.rmSync(targetDirectory, { recursive: true, force: true });
  }
});

test('no-money runtime result guard covers all 82 returned keys, 85 persisted keys and both replay paths', () => {
  const definitionStart = sourceLessApply.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_no_money_unwind_apply_work_item('
  );
  const definitionEnd = sourceLessApply.indexOf(
    '\nALTER FUNCTION public.pay_no_money_unwind_apply_work_item(uuid,uuid)',
    definitionStart
  );
  assert.ok(definitionStart >= 0 && definitionEnd > definitionStart);
  const definition = sourceLessApply.slice(definitionStart, definitionEnd);
  const resultStart = definition.indexOf('  v_result := jsonb_build_object(');
  const resultEnd = definition.indexOf(
    '\n\n  UPDATE public.pay_payment_correction_work_items AS applied_work_item',
    resultStart
  );
  assert.ok(resultStart >= 0 && resultEnd > resultStart);
  const resultPieces = definition
    .slice(resultStart, resultEnd)
    .split(/\n  \) \|\| jsonb_build_object\(\n/);
  const resultKeys = resultPieces.flatMap((piece) => (
    [...piece.matchAll(/^    '([^']+)',/gm)].map((match) => match[1])
  ));
  assert.deepEqual(resultPieces.map((piece) => [...piece.matchAll(/^    '([^']+)',/gm)].length), [43, 11, 20, 8]);
  assert.equal(resultKeys.length, 82);
  assert.equal(new Set(resultKeys).size, 82);
  assert.equal(
    sha256([...resultKeys].sort().join('\n')),
    '97ef9838f2003cae69da7f539a65d572a069ca01a20d9f0a96a5580685b69bdd'
  );
  assert.match(runner, /SOURCE_LESS_NO_MONEY_PERSISTED_WRAPPER_KEYS/);
  assert.match(runner, /'candidate_scope_hash',\s*\n\s*'created_by',\s*\n\s*'selection_ordinal'/);
  assert.match(runner, /expectedPersistedKeys\.length, 85/);
  assert.match(runner, /Object\.hasOwn\(returnedResult, key\)/);
  assert.match(runner, /Object\.hasOwn\(persistedResult, key\)/);
  assert.match(runner, /Object\.hasOwn\(persistedContractResult, key\)/);
  assert.match(runner, /returnedResult\.blocker, null/);
  assert.match(runner, /persistedResult\.blocker, null/);
  assert.match(runner, /assert\.deepEqual\(returnedResult, persistedContractResult/);
  assert.match(runner, /returned_persisted_contract_jsonb_equal/);
  assert.match(runner, /returned_persisted_contract_canonical_text_equal/);
  assert.match(runner, /returned_persisted_contract_sha256_equal/);
  assert.match(runner, /H12_NO_MONEY_RESULT_PROCESS_RESPONSE_LOSS_REPLAY_MISMATCH/);
  assert.match(runner, /H12_NO_MONEY_RESULT_EXACT_REQUEST_REPLAY_MISMATCH/);

  const wrongKeySetHash = runMutatedRunner((source) => source.replace(
    '97ef9838f2003cae69da7f539a65d572a069ca01a20d9f0a96a5580685b69bdd',
    '0000000000000000000000000000000000000000000000000000000000000000'
  ));
  assert.notEqual(wrongKeySetHash.status, 0);
  assert.match(`${wrongKeySetHash.stdout}\n${wrongKeySetHash.stderr}`, /source-less apply result key set changed/);
});

test('runtime contract preserves cancellation, exact evidence and fixed budgets without external effects', () => {
  assert.equal(fixture.policy_boundary.confirmed_unpaid_cancellation_must_complete, true);
  assert.equal(fixture.policy_boundary.ambiguous_adjustment_automatic_carry_forward, false);
  assert.equal(fixture.policy_boundary.ambiguous_adjustment_future_economics_inferred, false);
  assert.equal(fixture.policy_boundary.mail_can_veto_cancellation, false);
  assert.equal(fixture.policy_boundary.timeout_relaxation, false);
  assert.match(runner, /cancellationScope === 'WHOLE_BATCH'/);
  assert.match(runner, /paymentState === 'DRAFT'/);
  assert.match(runner, /DRAFT_PAYMENT_CANCELLATION_EXACT_REPLAY/);
  assert.match(runner, /request_row\.selection_json->>'source_context' = 'pay_batch_cancel'/);
  assert.match(runner, /exactReplay\.exact_pair_count, 1/);
  assert.match(worker, /async function readExactDraftCancellationReplayV1/);
  assert.match(worker, /const exactReplay = await readExactDraftCancellationReplayV1/);
  assert.equal(fixture.candidate_green_contract.ambiguous_source_less_item_carry_forward_count, 0);
  assert.equal(fixture.candidate_green_contract.ambiguous_source_less_item_payable_rebuild_count, 0);
  assert.equal(fixture.candidate_green_contract.provider_attempt_delta, 0);
  assert.equal(fixture.candidate_green_contract.settlement_delta, 0);
  assert.equal(fixture.candidate_green_contract.remittance_delta, 0);
  assert.equal(fixture.candidate_green_contract.prepare_expand_apply_statement_timeout_ms, 6000);
  assert.equal(fixture.candidate_green_contract.prepare_expand_apply_lock_timeout_ms, 1000);
  assert.equal(fixture.candidate_green_contract.integrity_checker_statement_timeout_ms, 5000);
  assert.equal(fixture.candidate_green_contract.outer_harness_statement_timeout_ms, 15000);
  assert.equal(fixture.candidate_green_contract.outer_harness_lock_timeout_ms, 1500);
  assert.equal(fixture.candidate_green_contract.outer_harness_idle_in_transaction_timeout_ms, 30000);
  assert.equal(fixture.established_current_source_prerequisites.applied_identically_in_red_and_green, true);
  assert.equal(fixture.established_current_source_prerequisites.counted_as_source_less_candidate_changes, false);
  assert.equal(fixture.established_current_source_prerequisites.owner_paths.length, 6);
  for (const ownerPath of fixture.established_current_source_prerequisites.owner_paths) {
    assert.equal(fs.existsSync(path.join(root, ...ownerPath.split('/'))), true, ownerPath);
  }
  assert.match(runner, /ESTABLISHED_CURRENT_BASELINE_FLAGS/);
  assert.match(runner, /H2_CANCEL_APPLY_BANK_EVENT_CLASSIFICATION: 'true'/);
  assert.match(runner, /H2_CANCEL_APPLY_NO_MONEY_RESULT_ARITY: 'true'/);
  assert.match(runner, /SOURCE_LESS_APPLY_PATH/);
  assert.match(runner, /noMoneyResultPairCounts/);
  assert.match(runner, /\[43, 11, 20, 8\]/);
  assert.match(runner, /noMoneyResultKeys\.length, 82/);
  assert.match(runner, /source-less apply result contains duplicate fields/);
  assert.match(runner, /ownerBudgetedRpcs/);
  assert.match(runner, /call\.elapsed_ms < 6000/);
  assert.match(runner, /H2_CANCEL_SIMULATE_RESPONSE_LOSS/);
  assert.match(runner, /h12CompetingClaim/);
  assert.match(runner, /completed_request_replay_same_request/);
  assert.match(runner, /completed_request_replay_route/);
  assert.match(runner, /WORKER_FIXED_DRAFT_CANCELLATION_LOOKUP/);
  assert.match(runner, /WORKER_ALL_MATCHING_PAYMENT_STATUS_REQUEST_START/);
  assert.match(
    runner,
    /'mode','ALL_MATCHING',[\s\S]*?'snapshot_token','\$\{page\.snapshot_token\}'/
  );
  assert.doesNotMatch(
    runner,
    /'mode','ALL_MATCHING',[\s\S]*?'snapshot_token','\$\{page\.explicit_snapshot_token\}'/
  );
  assert.match(runner, /no-money durable result evidence for both cancellation scopes/);
  assert.match(runner, /NO_MONEY_RELEASE is completed by the correction owner for both scopes/);
  assert.match(runner, /cancellation_audit_present: terminalNoMoney/);
  assert.match(runner, /FAILED_PAYMENT_RELEASE_CONFIRMED_NOT_PAID/);
  assert.match(baseRunner.toString('utf8'), /FAILED_PAYMENT_CONFIRMED_NO_MONEY_RELEASED_BY_USER/);
  assert.match(runner, /Worker-owned no-money correction reason/);
  assert.match(runner, /readH12WholeBatchCancellationFinancials/);
  assert.match(runner, /candidate_authority_digest_sha256/);
  assert.match(runner, /correction_authority_digest_sha256/);
  assert.match(runner, /snapshot_scope: cancellationScope === 'ONE_CANDIDATE' \? 'CANDIDATE' : 'WHOLE_BATCH'/);
  assert.match(runner, /H12 route-specific durable work-item evidence kind/);
  assert.match(runner, /terminalNoMoney \? 'NO_MONEY_UNWIND' : 'PRE_BANK_CANCEL'/);
  assert.match(runner, /H12 exact pre-apply Draft cancellation stable constituent and unrelated selection identity/);
  assert.match(runner, /H12 exact post-drain Draft cancellation stable constituent and unrelated selection identity/);
  assert.match(runner, /H12_DRAFT_CANCEL_STABLE_CONSTITUENT_NOT_RETURNED_UNSELECTED/);
  assert.match(runner, /H12_DRAFT_CANCEL_UNRELATED_READY_SELECTION_CHANGED/);
  assert.match(runner, /H12_ONE_CANDIDATE_UNRELATED_SELECTED_CONTROL_MISSING/);
  assert.match(runner, /NO_MONEY_WORKBENCH_RETURN_PATH/);
  assert.match(runner, /no-money Workbench-return owner must be the final candidate source/);
  assert.match(runner, /h12DraftCancelSelectionIdentityBefore/);
  assert.doesNotMatch(
    runner,
    /const h12DraftCancelSelectionIdentityBefore = paymentState === 'DRAFT'/
  );
  assert.doesNotMatch(
    runner,
    /const h12DraftCancelSelectionIdentityAfter = workbenchDrain && paymentState === 'DRAFT'/
  );
  assert.match(runner, /banking_pay_draft_frozen_constituent_payloads_v8/);
  assert.match(runner, /frozen_payload\.payload_json->>'section'/);
  assert.match(runner, /frozen_payload\.payload_json->>'key_type'/);
  assert.doesNotMatch(
    runner,
    /JOIN public\.banking_pay_workbench_preview_rows AS frozen_preview/
  );
  assert.match(runner, /_pay_workbench_preview_selection_key_v1/);
  assert.match(runner, /KEEP_FAILED_TARGET_DATABASE/);
  assert.match(runner, /!KEEP_FAILED_TARGET_DATABASE/);
  assert.match(runner, /current_row\.status = 'READY'/);
  assert.match(runner, /current_row\.selection_state = 'UNSELECTED'/);
  assert.match(runner, /current_row\.row_json->>'selection_user_override' = 'UNSELECTED'/);
  assert.doesNotMatch(runner, /H12 Draft cancellation return-history count expectation/);
  assert.match(runner, /H12 isolated target database binding/);
  assert.match(runner, /provider_settlement_remittance_actions: 0/);
});

test('fixture is restricted to disposable PG17/PG18 clones and invokes no external owner', () => {
  assert.match(sql, /\^\(h2_cancel_v8\|h12_rg5_builder_cancel\)_pg\(17\|18\)\$/);
  assert.match(sql, /H12_DISPOSABLE_DATABASE_REQUIRED/);
  assert.match(sql, /source_item\.description AS ordinary_description/);
  assert.match(sql, /target_candidate\.ordinary_amount_inc_vat/);
  assert.match(sql, /'external_side_effects_invoked', false/);
  assert.match(sql, /'mail_sender_or_provider_invoked', false/);
  assert.doesNotMatch(sql, /public\.(?:execute|submit|send|dispatch|settle|remit)[a-z0-9_]*\s*\(/i);
});
