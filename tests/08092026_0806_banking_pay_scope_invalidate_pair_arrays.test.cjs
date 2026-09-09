const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repo = path.resolve(__dirname, '..');
const historicalPath = 'supabase/repeatable/04082026_1139_pay_workbench_scope_invalidate_v1.sql';
const leafPath = 'supabase/repeatable/08092026_0804_pay_workbench_scope_invalidate_pair_arrays_v1.sql';
const verifierPath = 'supabase/verification/08092026_0805_pay_workbench_scope_invalidate_pair_arrays_verification.sql';
// 0807 is an owner-capable, rollback-only local fixture. It must never enter
// current-release; coherent release wiring must add only 0805 to both verifier
// arrays and retain a separate deployed service-role first-use proof.
const runtimePath = 'tests/08092026_0807_banking_pay_scope_invalidate_pair_arrays_runtime_verification.sql';

const readBuffer = relativePath => fs.readFileSync(path.join(repo, relativePath));
const read = relativePath => readBuffer(relativePath).toString('utf8').replaceAll('\r\n', '\n');
const sha256 = buffer => createHash('sha256').update(buffer).digest('hex');

function extractInvalidator(source) {
  const startMarker = 'CREATE OR REPLACE FUNCTION private.pay_workbench_scope_invalidate_v1(';
  const endMarker = '$function$;';
  assert.equal(source.split(startMarker).length - 1, 1, 'expected one invalidator definition');
  const start = source.indexOf(startMarker);
  const end = source.indexOf(endMarker, start);
  assert.notEqual(end, -1, 'invalidator terminator missing');
  return source.slice(start, end + endMarker.length);
}

function replaceRangeOnce(source, startMarker, endMarker, replacement, label) {
  assert.equal(source.split(startMarker).length - 1, 1, `${label}: start anchor must occur once`);
  const start = source.indexOf(startMarker);
  const endStart = source.indexOf(endMarker, start);
  assert.notEqual(endStart, -1, `${label}: end anchor missing`);
  assert.equal(source.indexOf(endMarker, endStart + endMarker.length), -1,
    `${label}: end anchor must occur once after start`);
  return source.slice(0, start) + replacement
    + source.slice(endStart + endMarker.length);
}

function normalizedHistoricalSkeleton(source) {
  let result = replaceRangeOnce(
    source,
    '  CREATE TEMP TABLE IF NOT EXISTS pg_temp._bpay_wb_invalidation_pairs_v1(',
    '  GET DIAGNOSTICS v_pair_count = ROW_COUNT;',
    '  PAIR_MATERIALIZATION',
    'historical pair materialization',
  );
  assert.equal((result.match(/pg_temp\._bpay_wb_invalidation_pairs_v1/g) || []).length, 8);
  result = result.replaceAll('pg_temp._bpay_wb_invalidation_pairs_v1', 'PAIR_SOURCE');
  result = result.replaceAll('PAIR_SOURCE AS invalidation_pair', 'PAIR_SOURCE');
  return result.replace(/\s+/g, ' ').trim();
}

function normalizedPairArraySkeleton(source) {
  let result = source.replace(
    '  v_pair_candidate_ids uuid[] := ARRAY[]::uuid[];\n'
      + '  v_pair_timesheet_ids uuid[] := ARRAY[]::uuid[];\n',
    '',
  );
  assert.notEqual(result, source, 'pair-array declarations must occur exactly at the declaration anchor');
  assert.equal((result.match(/v_pair_candidate_ids uuid\[\]/g) || []).length, 0);

  result = replaceRangeOnce(
    result,
    '  SELECT COALESCE(\n           array_agg(\n             canonical_pair.candidate_id',
    '  v_pair_count := cardinality(v_pair_candidate_ids);',
    '  PAIR_MATERIALIZATION',
    'pair-array materialization',
  );

  const pairSource = /ROWS FROM \(\s*pg_catalog\.unnest\(v_pair_candidate_ids\),\s*pg_catalog\.unnest\(v_pair_timesheet_ids\)\s*\) AS invalidation_pair\(candidate_id,timesheet_id\)/g;
  assert.equal((result.match(pairSource) || []).length, 8,
    'every historical pair-source read must use aligned ROWS FROM arrays');
  result = result.replace(pairSource, 'PAIR_SOURCE');
  return result.replace(/\s+/g, ' ').trim();
}

test('historical owner is immutable and replayed exactly once before the correction', () => {
  const historicalBytes = readBuffer(historicalPath);
  const leaf = read(leafPath);
  assert.equal(historicalBytes.length, 11026);
  assert.equal(sha256(historicalBytes), 'f304e2d072d9c93f8fbe1e4ab9998b64d926a161c7b6ef4bde86dcb3ca681538');
  assert.equal((leaf.match(/^\\ir 04082026_1139_pay_workbench_scope_invalidate_v1\.sql$/gm) || []).length, 1);
  assert.ok(
    leaf.indexOf('\\ir 04082026_1139_pay_workbench_scope_invalidate_v1.sql')
      < leaf.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_scope_invalidate_v1('),
  );
  assert.equal((leaf.match(/^\\ir\s+/gm) || []).length, 1);
  assert.equal((leaf.match(/^BEGIN;$/gm) || []).length, 1);
  assert.equal((leaf.match(/^COMMIT;$/gm) || []).length, 1);
  assert.equal((leaf.match(/^(?:BEGIN|COMMIT|ROLLBACK);$/gm) || []).length, 2);
  assert.match(leaf, /\\set ON_ERROR_STOP on\nBEGIN;\n\\ir 04082026_1139/);
  assert.ok(leaf.indexOf('BEGIN;') < leaf.indexOf('\\ir 04082026_1139'));
  assert.ok(leaf.lastIndexOf('COMMIT;') > leaf.lastIndexOf('GRANT EXECUTE ON FUNCTION'));
});

test('release inventory deterministically applies the pair-array owner after the cohort caller', async () => {
  const { closureFor, sqlDateKey, sqlFiles } = await import('../scripts/cloudtms-db-release-lib.mjs');
  const cohortPath = 'supabase/repeatable/08092026_0518_banking_pay_candidate_dirty_cohort_authority_v1.sql';
  const repeatables = sqlFiles('supabase/repeatable');
  assert.ok(sqlDateKey(cohortPath) < sqlDateKey(leafPath));
  assert.ok(repeatables.indexOf(cohortPath) < repeatables.indexOf(leafPath));
  assert.deepEqual(closureFor(leafPath).paths, [leafPath, historicalPath]);

  const invalidatorOwners = repeatables.filter(relativePath =>
    read(relativePath).includes(
      'CREATE OR REPLACE FUNCTION private.pay_workbench_scope_invalidate_v1(',
    ),
  );
  assert.deepEqual(invalidatorOwners, [historicalPath, leafPath]);
  assert.equal(invalidatorOwners.at(-1), leafPath);
});

test('corrected function differs only in pair materialization and pair-source reads', () => {
  const historical = extractInvalidator(read(historicalPath));
  const corrected = extractInvalidator(read(leafPath));
  assert.equal(normalizedPairArraySkeleton(corrected), normalizedHistoricalSkeleton(historical));
});

test('pair arrays are canonicalized together and no caller-controlled temp relation remains', () => {
  const corrected = extractInvalidator(read(leafPath));
  assert.doesNotMatch(corrected, /pg_temp|_bpay_wb_invalidation_pairs_v1|CREATE\s+TEMP|DROP\s+TABLE|TRUNCATE/i);
  assert.match(corrected, /v_pair_candidate_ids uuid\[\] := ARRAY\[\]::uuid\[\];/);
  assert.match(corrected, /v_pair_timesheet_ids uuid\[\] := ARRAY\[\]::uuid\[\];/);
  assert.equal((corrected.match(/ROWS FROM/g) || []).length, 9);
  assert.equal((corrected.match(/pg_catalog\.unnest\(v_pair_candidate_ids\)/g) || []).length, 8);
  assert.equal((corrected.match(/pg_catalog\.unnest\(v_pair_timesheet_ids\)/g) || []).length, 8);
  assert.equal((corrected.match(/pg_catalog\.unnest\(COALESCE\(p_candidate_ids/g) || []).length, 1);
  assert.equal((corrected.match(/pg_catalog\.unnest\(COALESCE\(p_timesheet_ids/g) || []).length, 1);
  assert.match(corrected, /SELECT DISTINCT input_pair\.candidate_id,input_pair\.timesheet_id/);
  assert.equal((corrected.match(/ORDER BY canonical_pair\.candidate_id,\s*canonical_pair\.timesheet_id NULLS FIRST/g) || []).length, 2);
  assert.match(corrected, /v_pair_count := cardinality\(v_pair_candidate_ids\);/);
  assert.doesNotMatch(corrected, /array_remove|array_positions|WITH ORDINALITY|dynamic|EXECUTE\s+format/i);
});

test('identity, defaults, execution metadata and owner-only ACL stay exact', () => {
  const leaf = read(leafPath);
  const corrected = extractInvalidator(leaf);
  assert.match(corrected, /p_scope_change_tx_token uuid DEFAULT NULL::uuid,/);
  assert.match(corrected, /p_payload_json jsonb DEFAULT '\{\}'::jsonb/);
  assert.match(corrected, /RETURNS jsonb\s+LANGUAGE plpgsql\s+VOLATILE\s+PARALLEL UNSAFE\s+SECURITY INVOKER\s+SET search_path = ''/);
  assert.match(leaf, /ALTER FUNCTION private\.pay_workbench_scope_invalidate_v1\(uuid\[\],uuid\[\],text,uuid,jsonb\) OWNER TO postgres;/);
  assert.match(leaf, /REVOKE ALL ON FUNCTION private\.pay_workbench_scope_invalidate_v1\(uuid\[\],uuid\[\],text,uuid,jsonb\) FROM PUBLIC,anon,authenticated,service_role;/);
  assert.match(leaf, /GRANT EXECUTE ON FUNCTION private\.pay_workbench_scope_invalidate_v1\(uuid\[\],uuid\[\],text,uuid,jsonb\) TO postgres;/);
  assert.doesNotMatch(leaf, /GRANT EXECUTE[^;]+TO (?:anon|authenticated|service_role)/i);
});

test('current caller closure retains the exact invalidator call and privileged boundary', () => {
  const callers = [
    ['supabase/repeatable/08092026_0518_banking_pay_candidate_dirty_cohort_authority_v1.sql', 1],
    ['supabase/repeatable/04082026_1202_pay_workbench_financial_scope_dirty_transition_v1.sql', 1],
    ['supabase/repeatable/04082026_1219_candidate_pay_method_change_refresh_scope_v1.sql', 1],
    ['supabase/repeatable/04082026_1219_pay_timesheet_summary_pay_state_refresh_trigger.sql', 1],
    ['supabase/repeatable/04082026_1219_pay_workbench_contract_client_dirty_fanout_chunk.sql', 1],
    ['supabase/repeatable/04082026_1219_pay_workbench_dirty_event_enqueue.sql', 1],
    ['supabase/repeatable/07082026_1017_pay_workbench_enqueue_candidate_refresh.sql', 1],
    ['supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql', 1],
  ];
  for (const [file, expected] of callers) {
    assert.equal((read(file).match(/private\.pay_workbench_scope_invalidate_v1\(/g) || []).length, expected, file);
  }
  const cohort = read(callers[0][0]);
  assert.match(cohort, /CREATE OR REPLACE FUNCTION private\.pay_workbench_candidate_dirty_cohort_stage_v1[\s\S]*SECURITY INVOKER/);
  assert.match(cohort, /CREATE OR REPLACE FUNCTION public\.pay_workbench_candidate_dirty_apply_job_process[\s\S]*SECURITY DEFINER/);
  assert.match(cohort, /GRANT EXECUTE ON FUNCTION public\.pay_workbench_candidate_dirty_apply_job_process\([\s\S]*TO postgres, service_role;/);
});

test('verifier and runtime gates bind ACL, hostile cross-role routes, trigger families, scale and Policy X', () => {
  const verifier = read(verifierPath);
  const runtime = read(runtimePath);
  assert.match(verifier, /aclexplode/);
  assert.match(verifier, /v_direct_callers IS DISTINCT FROM v_expected_direct_callers/);
  assert.match(verifier, /THEN caller_proc\.prosecdef IS DISTINCT FROM FALSE/);
  assert.match(verifier, /ELSE caller_proc\.prosecdef IS DISTINCT FROM TRUE/);
  assert.match(verifier, /pay_workbench_financial_scope_dirty_transition_v1/);
  assert.match(verifier, /pay_timesheet_summary_pay_state_refresh_trigger/);
  assert.match(verifier, /v_rows_from_count <> 9/);
  assert.match(runtime, /SET LOCAL ROLE service_role/);
  assert.match(runtime, /Local-owner rollback-only proof/);
  assert.match(runtime, /_bpay_wb_invalidation_pairs_v1/);
  assert.match(runtime, /pay_workbench_dirty_event_enqueue/);
  assert.match(runtime, /pay_workbench_candidate_dirty_apply_job_process/);
  assert.match(runtime, /PAIR_ARRAY_NULL_ARRAYS_ZERO_SHAPE_FAILED/);
  assert.match(runtime, /PAIR_ARRAY_EMPTY_ARRAYS_ZERO_SHAPE_FAILED/);
  assert.match(runtime, /PAIR_ARRAY_MISSING_TIMESHEET_SIDE_EFFECT/);
  assert.match(runtime, /GET STACKED DIAGNOSTICS v_error_detail=PG_EXCEPTION_DETAIL/);
  assert.match(runtime, /'kind'\s*\n\s*IS DISTINCT FROM 'CANDIDATE_NOT_FOUND'/);
  assert.match(runtime, /'kind'\s*\n\s*IS DISTINCT FROM 'TIMESHEET_CANDIDATE_MISMATCH'/);
  assert.match(runtime, /PAIR_ARRAY_CROSSED_ALIGNMENT_FAILED/);
  assert.match(runtime, /PAIR_ARRAY_CROSSED_ALIGNMENT_REPLAY_FAILED/);
  assert.match(runtime, /SET status='RUNNING'[\s\S]*job_coalesced_count'\)::integer IS DISTINCT FROM 2/);
  assert.match(runtime, /v_alignment_candidate_low_id >= v_alignment_candidate_high_id/);
  assert.match(runtime, /v_alignment_timesheet_for_high_id >=\s*\n\s*v_alignment_timesheet_for_low_id/);
  assert.match(runtime, /generate_series\(1,5000\)/);
  assert.equal((runtime.match(/'CCR-'\|\|\(fixture\.tms_ref_base/g) || []).length, 6);
  assert.match(runtime, /array_agg\(scale_candidate\.timesheet_id ORDER BY scale_candidate\.ordinal\)/);
  assert.match(runtime, /\(v_result->>'timesheet_count'\)::integer IS DISTINCT FROM 5000/);
  assert.match(runtime, /\(v_result->>'state_inserted_count'\)::integer IS DISTINCT FROM 5000/);
  assert.equal((runtime.match(/SET LOCAL statement_timeout = '15s';/g) || []).length, 2);
  assert.match(runtime, /SET LOCAL lock_timeout = '1500ms';/);
  assert.match(runtime, /v_elapsed_ms >= 15000/);
  assert.doesNotMatch(runtime, /statement_timeout = '(?:30|120)s'|lock_timeout = '5s'|v_elapsed_ms [><=]+ 30000/);
  assert.match(runtime, /PAY_WORKBENCH_SCOPE_INVALIDATION_OWNERSHIP_MISMATCH/);
  assert.match(runtime, /POLICY_X_NO_MONEY_EFFECT/);
  assert.doesNotMatch(runtime, /<>/);
  assert.doesNotMatch(runtime, /\(v_(?:result|replay)->>[^\n]+\)\s*(?:::[a-z]+)?\s*<>/i);
  assert.match(runtime, /ROLLBACK;\s*$/);
  assert.doesNotMatch(runtime, /\bCOMMIT\s*;/i);
});
