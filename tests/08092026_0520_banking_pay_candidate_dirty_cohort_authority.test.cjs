const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8')
  .replace(/\r\n/g, '\n');

const migrationPath = 'supabase/migrations/08092026_0517_banking_pay_candidate_dirty_active_cohort_indexes_v1.sql';
const leafPath = 'supabase/repeatable/08092026_0518_banking_pay_candidate_dirty_cohort_authority_v1.sql';
const verifierPath = 'supabase/verification/08092026_0519_banking_pay_candidate_dirty_cohort_authority_verification.sql';
const runtimePath = 'tests/08092026_0521_banking_pay_candidate_dirty_cohort_runtime_verification.sql';
const concurrencyPath = 'tests/08092026_0521_banking_pay_candidate_dirty_cohort_concurrency.runtime.test.cjs';
const finalClosurePath = 'supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql';
const historicalOwnerPath = 'supabase/repeatable/07082026_1016_banking_pay_targeted_delta_runtime.sql';

const migration = read(migrationPath);
const leaf = read(leafPath);
const verifier = read(verifierPath);
const finalClosure = read(finalClosurePath);
const historicalOwner = read(historicalOwnerPath);
const helperEnd = leaf.indexOf('-- PROCESSOR_REPLACEMENT_INSERTION_POINT');
assert.notEqual(helperEnd, -1);
const helper = leaf.slice(0, helperEnd);
const processor = leaf.slice(helperEnd);

const processorSignature = 'CREATE OR REPLACE FUNCTION public.pay_workbench_candidate_dirty_apply_job_process(';
const extractProcessor = (source, label) => {
  const starts = [...source.matchAll(new RegExp(
    processorSignature.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'),
    'g',
  ))].map(match => match.index);
  assert.equal(starts.length, 1, `${label} must own the processor exactly once`);
  const terminator = '\n$function$;';
  const end = source.indexOf(terminator, starts[0]);
  assert.notEqual(end, -1, `${label} processor terminator missing`);
  return source.slice(starts[0], end + terminator.length);
};

test('historical current owner is byte-identical and 0518 changes only the cohort authority plus its preserved result count', () => {
  const historicalBytes = fs.readFileSync(path.join(root, historicalOwnerPath));
  assert.equal(historicalBytes.length, 393154);
  assert.equal(
    crypto.createHash('sha256').update(historicalBytes).digest('hex'),
    '8625756727a679e03851aa470a4b8269b8ac1a9e7dca038caa5da491564c061b',
  );

  const historicalProcessor = extractProcessor(historicalOwner, 'historical owner');
  const replacementProcessor = extractProcessor(leaf, '0518 leaf');
  assert.equal(historicalProcessor.split('\n').length, 1427);

  const oldDeclaration = "  v_scope_reissue_result jsonb := '{}'::jsonb;";
  const newDeclarations = [
    "  v_dirty_cohort_result jsonb := '{}'::jsonb;",
    '  v_dirty_cohort_action text := NULL::text;',
    '  v_dirty_cohort_authority_scope text := NULL::text;',
    '  v_dirty_cohort_full_authority_reusable boolean := false;',
    '  v_dirty_cohort_fast_path_reusable boolean := false;',
  ].join('\n');
  assert.equal(historicalProcessor.split(oldDeclaration).length - 1, 1);
  assert.equal(replacementProcessor.split(newDeclarations).length - 1, 1);

  const blockStart = '  -- The trigger invalidates the exact row known at write time.';
  const blockEnd = "  PERFORM public._temp_diag_log('TEMP_TRIGGER_DIRTY_STAGE', 'TEMP_BANKING_PAY_DIRTY', p_job_id::text, jsonb_build_object('function_name', 'pay_workbench_candidate_dirty_apply_job_process', 'stage', 'dirty_worker_apply_start'";
  const locateBlock = (source, label) => {
    assert.equal(source.split(blockStart).length - 1, 1, `${label} block start`);
    assert.equal(source.split(blockEnd).length - 1, 1, `${label} block end`);
    const start = source.indexOf(blockStart);
    const end = source.indexOf(blockEnd, start);
    assert.ok(end > start, `${label} authority block order`);
    return { start, end, text: source.slice(start, end) };
  };
  const oldBlock = locateBlock(historicalProcessor, 'historical owner');
  const newBlock = locateBlock(replacementProcessor, '0518 leaf');

  const familyDeclaration = '  v_family_timesheet_ids uuid[] := ARRAY[]::uuid[];';
  const reportedFamilyDeclaration = `${familyDeclaration}\n  v_reported_family_timesheet_count integer := 0;`;
  const historicalFamilyResult = "'family_timesheet_count', COALESCE(array_length(v_family_timesheet_ids, 1), 0)";
  const preservedFamilyResult = "'family_timesheet_count', v_reported_family_timesheet_count";
  assert.equal(historicalProcessor.split(familyDeclaration).length - 1, 1);
  assert.equal(historicalProcessor.split(historicalFamilyResult).length - 1, 2);

  let expected = historicalProcessor
    .replace(oldDeclaration, newDeclarations)
    .replace(familyDeclaration, reportedFamilyDeclaration)
    .replaceAll(historicalFamilyResult, preservedFamilyResult);
  const expectedBlock = locateBlock(expected, 'declaration-substituted owner');
  expected = expected.slice(0, expectedBlock.start)
    + newBlock.text
    + expected.slice(expectedBlock.end);
  const firstDifference = [...Array(Math.max(expected.length, replacementProcessor.length)).keys()]
    .find(index => expected[index] !== replacementProcessor[index]);
  assert.equal(
    expected,
    replacementProcessor,
    `processor differs outside declared substitutions at byte ${firstDifference}; expected=${JSON.stringify(expected.slice(firstDifference - 40, firstDifference + 40))}; actual=${JSON.stringify(replacementProcessor.slice(firstDifference - 40, firstDifference + 40))}`,
  );
  assert.notEqual(oldBlock.text, newBlock.text);
});

test('0517 is a retry-safe standalone concurrent migration with two exact active-cohort access paths', () => {
  assert.doesNotMatch(migration, /^\s*(?:BEGIN|COMMIT)\s*;/gmi);
  assert.equal((migration.match(/DROP INDEX CONCURRENTLY IF EXISTS/g) || []).length, 2);
  assert.equal((migration.match(/CREATE INDEX CONCURRENTLY/g) || []).length, 2);
  assert.match(migration, /ON public\.banking_pay_workbench_jobs \(candidate_id, id\)[\s\S]*candidate_id IS NOT NULL/);
  assert.match(migration, /lower\(btrim\(COALESCE\(payload_json ->> 'candidate_id', ''\)\)\)[\s\S]*candidate_id IS NULL/);
  assert.match(migration, /job_type = 'WORKBENCH_CANDIDATE_DIRTY_APPLY'/);
  assert.match(migration, /status IN \('QUEUED', 'RUNNING'\)/);
  assert.doesNotMatch(migration, /UNIQUE\s+INDEX/i);
  assert.doesNotMatch(migration, /dedupe_key/i);
});

test('0518 is a leaf-only private cohort owner with uncapped deterministic admission and correction isolation', () => {
  assert.doesNotMatch(leaf, /^\s*\\ir\s+/gmi);
  assert.match(helper, /CREATE OR REPLACE FUNCTION private\.pay_workbench_candidate_dirty_cohort_stage_v1/);
  assert.match(helper, /SECURITY INVOKER/);
  assert.match(helper, /SET search_path = ''/);
  assert.match(helper, /pg_try_advisory_xact_lock[\s\S]*_pay_workbench_candidate_serial_key/);
  assert.match(helper, /candidate_job\.candidate_id = p_candidate_id[\s\S]*legacy_job\.candidate_id IS NULL/);
  assert.match(helper, /ORDER BY locked_job\.id\s+FOR UPDATE OF locked_job/);
  assert.match(helper, /v_member_ids uuid\[\] := ARRAY\[\]::uuid\[\]/);
  assert.match(helper, /v_admitted_member_ids uuid\[\] := ARRAY\[\]::uuid\[\]/);
  assert.match(helper, /v_excluded_member_ids uuid\[\] := ARRAY\[\]::uuid\[\]/);
  assert.match(helper, /INTO v_member_ids[\s\S]*UNION[\s\S]*AS snapshot_job/);
  assert.match(helper, /pg_catalog\.unnest\(v_admitted_member_ids\)/);
  assert.match(helper, /v_admitted_member_ids := pg_catalog\.array_remove/);
  assert.doesNotMatch(helper, /\bpg_temp\b/i);
  assert.doesNotMatch(helper, /CREATE TEMP|DROP TABLE|TRUNCATE/i);
  assert.match(helper, /row inserted after this ID snapshot is a late arrival/i);
  assert.doesNotMatch(helper, /v_member_count\s*>\s*\d+/i);
  assert.doesNotMatch(helper, /CAPACITY_EXHAUSTED|TOO_MANY_(?:MEMBERS|JOBS)/i);
  assert.match(helper, /REQUEST_OWNED_CORRECTION_UNFINISHED/);
  assert.match(helper, /CORRECTION_OWNED_DIRTY_CAUSAL_V1/);
  assert.match(helper, /correction_operation\.status IN \([\s\S]*'WAITING_AUTHORISATION'/);
  assert.match(helper, /COHORT_MEMBER_FULL_SCOPE/);
  assert.match(helper, /COHORT_EMPTY_EFFECTIVE_TIMESHEET_SCOPE/);
  assert.match(helper, /v_authority_scope = 'CANDIDATE_FULL_LIVE'[\s\S]*AND v_all_full_markers_match/);
  assert.doesNotMatch(helper, /v_member_count = 1\s+OR v_all_full_markers_match/);
});

test('one helper call unions bounded roots, lets ALL dominate and stages one token/source sequence atomically', () => {
  assert.equal((helper.match(/private\.pay_workbench_scope_invalidate_v1\(/g) || []).length, 1);
  assert.equal((helper.match(/public\._change_bump\(/g) || []).length, 1);
  assert.match(helper, /array_agg\([\s\S]*DISTINCT raw_scope\.value_text::uuid/);
  assert.match(helper, /INTO v_timesheet_root_ids/);
  assert.match(helper, /INTO v_finance_case_root_ids/);
  assert.match(helper, /v_timesheet_root_count > 250/);
  assert.match(helper, /v_finance_case_root_count > 100/);
  assert.match(helper, /v_authority_scope := 'CANDIDATE_FULL_LIVE'/);
  assert.match(helper, /v_invalidation_timesheet_ids := ARRAY\[NULL::uuid\]/);
  assert.match(helper, /'skip_candidate_job_enqueue', true/);
  assert.match(helper, /'DIRTY_APPLY_CANDIDATE_COHORT_REISSUE'/);
  assert.match(helper, /UPDATE public\.banking_pay_workbench_jobs AS candidate_job[\s\S]*WHERE candidate_job\.id = ANY\(v_admitted_member_ids\)/);
  assert.match(helper, /WHEN candidate_job\.id = p_job_id THEN 'QUEUED'[\s\S]*ELSE candidate_job\.status/);
  assert.match(helper, /candidate_job\.payload_json[\s\S]*- 'scope_change_tx_token'[\s\S]*- 'scope_change_generation'[\s\S]*- 'bounded_scope_state_precedes_job'/);
  assert.doesNotMatch(helper, /- 'reasons'|- 'reason_latest'|- 'targeted_timesheet_ids'|- 'linked_timesheet_ids'|- 'finance_case_ids'|- 'correction_dirty_contexts'/);
  assert.match(helper, /'dirty_apply_cohort_tx_token', v_stage_token/);
  assert.match(helper, /'latest_source_change_seq', v_common_source_seq/);
  assert.match(helper, /'action', 'COHORT_REISSUED_PENDING_FINALIZATION'/);
});

test('processor bypasses the removed one-job reissue and runs a genuine Candidate-full path when cohort authority is full', () => {
  assert.doesNotMatch(processor, /DIRTY_APPLY_EFFECTIVE_SCOPE_REISSUE/);
  assert.doesNotMatch(processor, /v_scope_reissue_result/);
  assert.equal((processor.match(/private\.pay_workbench_scope_invalidate_v1\(/g) || []).length, 0);
  assert.match(processor, /v_dirty_cohort_action := 'FAST_PATH_FINALIZED_COHORT'/);
  assert.match(processor, /private\.pay_workbench_candidate_dirty_cohort_stage_v1\(/);
  assert.match(processor, /ELSIF v_dirty_cohort_action <> 'REUSE_FINALIZED_AUTHORITY'/);
  assert.match(processor, /PAY_WORKBENCH_DIRTY_COHORT_REUSE_PROOF_MISMATCH/);
  const fullOverride = processor.indexOf(
    "IF v_dirty_cohort_authority_scope = 'CANDIDATE_FULL_LIVE' THEN",
  );
  const sessionScan = processor.indexOf('FOR v_session_row IN', fullOverride);
  assert.ok(fullOverride > -1 && sessionScan > fullOverride);
  const fullBlock = processor.slice(fullOverride, sessionScan);
  for (const assignment of [
    "v_dependency_closure_requires_full := true",
    "v_targeted_timesheet_ids := ARRAY[]::uuid[]",
    "v_linked_timesheet_ids := ARRAY[]::uuid[]",
    "v_finance_case_ids := ARRAY[]::uuid[]",
    "v_family_timesheet_ids := ARRAY[]::uuid[]",
    "v_effective_bounded_timesheet_ids := ARRAY[]::uuid[]",
    "v_refresh_scope_kind := 'CANDIDATE_FULL_LIVE'",
    'v_is_authorise_delta_targeted := false',
    "v_payload := v_payload\n      - 'targeted_timesheet_ids'\n      - 'linked_timesheet_ids'\n      - 'finance_case_ids'",
  ]) assert.ok(fullBlock.includes(assignment), `missing full-path assignment: ${assignment}`);
  assert.match(processor, /v_reported_family_timesheet_count integer := 0/);
  assert.match(processor, /dirty_apply_cohort_effective_timesheet_count'[\s\S]*v_reported_family_timesheet_count/);
  assert.match(processor, /'family_timesheet_count', v_reported_family_timesheet_count/);
});

test('finalized cohort members use an exact own-token fast path instead of rescanning all active siblings', () => {
  const fastPath = processor.slice(
    processor.indexOf('v_dirty_cohort_fast_path_reusable :='),
    processor.indexOf('IF v_dirty_cohort_authority_scope =', processor.indexOf('v_dirty_cohort_fast_path_reusable :=')),
  );
  assert.match(fastPath, /v_preceding_scope_authority_reusable/);
  assert.match(fastPath, /DIRTY_APPLY_COHORT_AUTHORITY_V1/);
  assert.match(fastPath, /dirty_apply_cohort_candidate_id/);
  assert.match(fastPath, /dirty_apply_cohort_tx_token/);
  assert.match(fastPath, /cardinality\(v_effective_bounded_timesheet_ids\)>0[\s\S]*'TARGETED_UNION'/);
  assert.match(fastPath, /OR v_dirty_cohort_full_authority_reusable/);
  assert.doesNotMatch(fastPath, /authority_scope', ''\) IN \([\s\S]*TARGETED_UNION[\s\S]*CANDIDATE_FULL_LIVE/);
  assert.match(fastPath, /IF v_dirty_cohort_fast_path_reusable THEN[\s\S]*ELSE[\s\S]*pay_workbench_candidate_dirty_cohort_stage_v1/);
  assert.doesNotMatch(fastPath, /FROM public\.banking_pay_workbench_jobs/);
});

test('cohort helper stays inside Policy X freshness metadata and preserves existing processor economics', () => {
  for (const table of [
    'pay_batches',
    'pay_batch_items',
    'banking_pay_operations',
    'banking_pay_operation_provider_attempts',
    'banking_pay_operation_settlement_scope',
    'banking_pay_operation_remittance_scope',
  ]) {
    assert.doesNotMatch(
      helper,
      new RegExp(`(?:INSERT\\s+INTO|UPDATE|DELETE\\s+FROM)\\s+(?:public\\.)?${table}\\b`, 'i'),
      `helper must not mutate ${table}`,
    );
  }
  assert.match(helper, /'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'/);
  assert.match(helper, /'economic_calculation_performed', false/);
  assert.match(processor, /pay_workbench_authorise_delta_hotkey_preflight/);
  assert.match(processor, /pay_workbench_enqueue_candidate_refresh/);
});

test('standalone verifier and runtime matrix are present while shared F7 wiring remains explicitly pending', () => {
  const runtime = read(runtimePath);
  const concurrency = read(concurrencyPath);
  assert.match(verifier, /PAY_WORKBENCH_DIRTY_COHORT_HELPER_AUTHORITY_INVALID/);
  assert.match(verifier, /PAY_WORKBENCH_DIRTY_COHORT_INDEX_CONTRACT_INVALID/);
  assert.match(verifier, /v_helper_execute_grantees IS DISTINCT FROM/);
  assert.match(verifier, /v_processor_execute_grantees IS DISTINCT FROM/);
  assert.match(verifier, /v_helper_execute_grant_option/);
  assert.match(verifier, /v_processor_execute_grant_option/);
  assert.match(runtime, /SET LOCAL statement_timeout = '120s'/);
  assert.match(runtime, /SET LOCAL lock_timeout = '5s'/);
  for (const proof of [
    'ALL_PLUS_TARGETED',
    'DISJOINT_TARGETED',
    'ACTIVE_MEMBER_COUNT_101',
    'ACTIVE_MEMBER_COUNT_101_FAST_PATH_BUDGET_EXCEEDED',
    'TARGETED_FINALIZED_TO_FULL_DRIFT',
    'TARGETED_FINALIZED_TO_FULL_EXACT_FULL_REPLAY_FAILED',
    'CORRECTION_OWNED_EXCLUSION',
    'LATE_ARRIVAL',
    'RESPONSE_LOSS_REPLAY',
    'LEGACY_INDEX_PLAN',
    'DIRECT_INDEX_PLAN',
    'POLICY_X_CORRECTION_FIRST_PROCESSOR_ROW_CHANGED',
    'POLICY_X_CORRECTION_SECOND_PROCESSOR_ROW_CHANGED',
    'POLICY_X_NO_MONEY_EFFECT',
  ]) assert.match(runtime, new RegExp(proof));
  for (const proof of [
    'runAsync',
    'winnerApplicationName',
    'candidate_serial_delayed',
    'CANDIDATE_SERIAL_LOCK_BUSY',
    'waitForLockWait',
    "wait_event_type='Lock'",
    'dirty_apply_cohort_member_count',
  ]) assert.match(concurrency, new RegExp(proof));
  assert.ok(concurrency.includes(
    'const jobIds = [randomUUID(), randomUUID(), randomUUID()].sort()',
  ));
  assert.ok(
    (concurrency.match(/pay_workbench_candidate_dirty_apply_job_process/g) || []).length >= 4,
    'concurrency harness must launch overlapping public processors and a late-arrival processor',
  );
  assert.match(concurrency, /SET LOCAL statement_timeout='15s'/);
  assert.match(concurrency, /SET LOCAL lock_timeout='10s'/);
  assert.match(concurrency, /loserElapsedMs < 2000/);
  assert.equal(
    finalClosure.includes('08092026_0518_banking_pay_candidate_dirty_cohort_authority_v1.sql'),
    false,
    'REQUIRED_NOT_YET_APPLIED: F7 must be wired only after shared/H1 reconciliation',
  );
});
