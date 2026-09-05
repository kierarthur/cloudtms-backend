const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const normalizeLf = value => String(value || '').replace(/\r\n/g, '\n');
const readRepeatable = name => normalizeLf(fs.readFileSync(path.join(repeatableDir, name), 'utf8'));
const closureForFixture = (entryName, overrides = new Map()) => {
  const active = new Set();
  const ordered = [];
  const visit = name => {
    const normal = name.replaceAll('\\', '/');
    assert.equal(active.has(normal), false, `recursive fixture include: ${normal}`);
    active.add(normal);
    const source = normalizeLf(overrides.get(normal) ?? readRepeatable(normal));
    ordered.push({ path: `supabase/repeatable/${normal}`, source });
    for (const line of source.split(/\r?\n/)) {
      const match = line.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/);
      if (!match) continue;
      const include = match[1] ?? match[2] ?? match[3];
      visit(path.posix.normalize(path.posix.join(path.posix.dirname(normal), include)));
    }
    active.delete(normal);
  };
  visit(entryName);
  const bytes = Buffer.concat(ordered.flatMap(item => [
    Buffer.from(`${item.path}\0`),Buffer.from(item.source),Buffer.from('\0'),
  ]));
  return {
    paths: ordered.map(item => item.path),
    sha256: crypto.createHash('sha256').update(bytes).digest('hex'),
  };
};
const historicalReplayName = '08082026_0902_reassert_authorities_after_legacy_monolith.sql';
const finalClosureName = '30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql';
const f013OrderGuard = JSON.parse(fs.readFileSync(
  path.join(root, 'tests/fixtures/banking-pay-h1-h2-f013-release-order-v1.json'),
  'utf8',
));
const dirtyRuntime = fs.readFileSync(
  path.join(root, 'supabase/repeatable/07082026_1016_banking_pay_targeted_delta_runtime.sql'),
  'utf8',
);
const repair = fs.readFileSync(
  path.join(root, 'supabase/repeatable/30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql'),
  'utf8',
);
const verification = fs.readFileSync(
  path.join(root, 'supabase/verification/31082026_0014_banking_pay_dirty_apply_family_authority_repair_verification.sql'),
  'utf8',
);
const worker = fs.readFileSync(path.join(root, 'broker/src/index.js'), 'utf8');

test('dirty apply proves the final family scope and reissues one canonical authority before session fan-out', () => {
  const start = dirtyRuntime.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_workbench_candidate_dirty_apply_job_process(',
  );
  assert.notEqual(start, -1);
  const body = dirtyRuntime.slice(start);

  const familyNormalisation = body.indexOf(
    'public._pay_workbench_normalise_timesheet_rotation_scope_payload',
  );
  const proof = body.indexOf('v_preceding_scope_authority_reusable :=', familyNormalisation);
  const reissue = body.indexOf('private.pay_workbench_scope_invalidate_v1(', proof);
  const sessionScan = body.indexOf('FOR v_session_row IN', reissue);
  const canonicalEnqueue = body.indexOf('public.pay_workbench_enqueue_candidate_refresh(', sessionScan);

  assert.ok(familyNormalisation > -1);
  assert.ok(proof > familyNormalisation);
  assert.ok(reissue > proof);
  assert.ok(sessionScan > reissue);
  assert.ok(canonicalEnqueue > sessionScan);
  assert.match(body.slice(familyNormalisation, reissue), /banking_pay_scope_change_transactions/i);
  assert.match(body.slice(familyNormalisation, reissue), /banking_pay_workbench_timesheet_scope_state/i);
  assert.match(body.slice(familyNormalisation, reissue), /scope_change_generation/i);
  assert.match(body.slice(reissue, sessionScan), /skip_candidate_job_enqueue['"\s,:]+true/i);
  assert.match(body.slice(reissue, sessionScan), /PAY_WORKBENCH_EFFECTIVE_SCOPE_REISSUE_NOT_STAGED/i);
  assert.match(body.slice(reissue, sessionScan), /public\._change_bump\('pay_candidate:'/i);
  assert.match(body.slice(reissue, sessionScan), /preinvalidated_scope_reissue_pending_finalization/i);
  assert.match(body.slice(reissue, sessionScan), /SET status='QUEUED'/i);
  assert.doesNotMatch(body.slice(reissue, sessionScan), /SET CONSTRAINTS/i);
  assert.match(body, /preinvalidated_scope_reissued/i);
});

test('repair targets only the two proved terminal dirty-apply failures and retains DEAD history', () => {
  const closureFiles = [
    historicalReplayName,
    '07082026_1016_banking_pay_targeted_delta_runtime.sql',
    '28082026_1424_banking_pay_modal_selection_owner_bridge.sql',
    '29082026_0613_banking_pay_replaced_candidate_owner_repair_v1.sql',
    '30082026_1232_candidate_qr_document_revision_order_v1.sql',
    '29082026_0326_banking_pay_release_authority_repair_v1.sql',
  ];
  let previousIndex = -1;
  for (const file of closureFiles) {
    const index = repair.indexOf(`\\ir ${file}`);
    assert.ok(index > previousIndex, `missing or misordered final authority closure: ${file}`);
    previousIndex = index;
  }
  assert.match(repair, /CREATE OR REPLACE FUNCTION public\.pay_workbench_repair_invalid_dirty_apply_jobs_v1\(/i);
  assert.match(repair, /PAY_WORKBENCH_PRECEDING_SCOPE_INVALIDATION_UNPROVED/i);
  assert.match(repair, /PAY_WORKBENCH_STALE_PREINVALIDATED_AUTHORITY_NOT_CURRENT/i);
  assert.match(repair, /invalid_job\.status='DEAD'/i);
  assert.match(repair, /successful_job\.status='SUCCEEDED'/i);
  assert.match(repair, /pg_try_advisory_xact_lock/i);
  assert.match(repair, /private\.pay_workbench_scope_invalidate_v1\(/i);
  assert.match(repair, /ARRAY\[NULL::uuid\]/i);
  assert.match(repair, /successor_job\.status IN \('QUEUED','RUNNING'\)/i);
  assert.match(repair, /v_successor_tx_state IS DISTINCT FROM 'PENDING'/i);
  assert.match(repair, /v_registry_tx_token IS DISTINCT FROM v_successor\.scope_change_tx_token/i);
  assert.match(repair, /successor_finalization_staged/i);
  assert.match(repair, /PAY_WORKBENCH_INVALID_DIRTY_APPLY_REPAIR_POSTCONDITION_FAILED/i);
  assert.doesNotMatch(repair, /UPDATE public\.banking_pay_workbench_jobs AS invalid_job[\s\S]*SET status/i);
  assert.match(repair, /FROM PUBLIC,anon,authenticated/i);
  assert.match(repair, /TO postgres,service_role/i);
  const restoredServiceOnlyAuthorities = [
    'public._pay_active_settled_components(uuid[])',
    'public.bulk_authorise_dataset_v1(jsonb)',
    'public.bulk_authorise_row_context_v1(jsonb)',
    'public.bulk_process_dataset_v1(jsonb)',
    'public.bulk_process_row_context_v1(jsonb)',
    'public.bulk_timesheet_row_patch_v1(jsonb)',
    'public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb)',
    'public.pay_preview_candidate_build_canonical_lines(jsonb,uuid)',
    'public.pay_preview_candidate_build_finance_case_baseline(jsonb,uuid)',
    'public.pay_timesheet_summary_pay_state_refresh_trigger()',
    'public.pay_workbench_contract_client_dirty_fanout_chunk(uuid,jsonb,integer)',
    'public.pay_workbench_dirty_apply_jobs_chunk(integer,timestamptz,uuid,uuid,text,integer)',
    'public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb)',
    'public.pay_workbench_enqueue_stage_continuation(uuid,uuid,text,jsonb,uuid,jsonb,uuid,text,integer,integer)',
    'public.pay_workbench_repair_invalid_source_build_poison(uuid,uuid,integer,timestamptz,text)',
    'public.pay_workbench_session_clear_case_resolution(uuid,uuid,jsonb)',
    'public.pay_workbench_session_clone_eligibility_v1(uuid,uuid,uuid,jsonb)',
    'public.pay_workbench_session_set_selected_rows(uuid,jsonb,uuid)',
    'public.pay_workbench_worker_drain_chunk(integer,timestamptz,uuid,uuid,text[],text,integer)',
    'public.timesheet_authorise_bulk_atomic(jsonb,uuid,timestamptz)',
    'public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamptz,text)',
    'public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text)',
    'public.timesheet_lifecycle_guard_signature_v1(uuid,uuid,boolean)',
    'public.timesheet_qr_send_enqueue_v1(uuid,uuid,uuid,text,timestamptz)',
  ];
  for (const identity of restoredServiceOnlyAuthorities) {
    assert.ok(
      repair.includes(`REVOKE ALL ON FUNCTION ${identity} FROM PUBLIC,anon,authenticated,service_role;`),
      `missing exact browser-role revoke for ${identity}`,
    );
    assert.ok(
      repair.includes(`GRANT EXECUTE ON FUNCTION ${identity} TO service_role;`),
      `missing exact service-role grant for ${identity}`,
    );
    assert.ok(
      verification.includes(`'${identity}'::regprocedure`),
      `missing exact installed ACL verification for ${identity}`,
    );
  }
  for (const hash of [
    '930d55e60b1599fcdba40ab7b5308ba5991a666f7a92b23f39d8c33a481af5e3',
    'ac3a122f00af03e35bb2c40e82ddb114571f7252a6ac31d9bfec23d7cb3afc19',
    '363aeab20aed70b8396793808f9a2263766e984d66914317bdf0a767e6e0f360',
    '7d622194f7bca877bf8420cb6f10f9ad46a69bad118c5f8fb9ed16810492d98c',
    '090fcbd7a66ade81f107635c360a038a514a5c26358c0b4aa716bdea91245347',
  ]) assert.match(verification, new RegExp(hash));
  for (const config of ['search_path=public', 'search_path=""']) {
    assert.ok(verification.includes(config));
  }
  assert.match(verification, /actual_acl IS DISTINCT FROM pg_catalog\.jsonb_build_array/);
  assert.match(verification, /SELECT DISTINCT[\s\S]*function_acl\.grantee=proc\.proowner[\s\S]*pg_catalog\.pg_get_userbyid\(function_acl\.grantee\)='postgres'[\s\S]*THEN 'OWNER'/i);
  assert.match(verification, /normalised_acl\.grantee COLLATE "C"[\s\S]*normalised_acl\.privilege COLLATE "C"/i);
  assert.doesNotMatch(verification, /'grantee','postgres'/i);
  assert.match(verification, /BANKING_PAY_FINAL_AUTHORITY_CLOSURE_MISMATCH/);
});

test('immutable historical replay is the first transitive dependency of the established final closure', () => {
  const historicalReplay = readRepeatable(historicalReplayName);
  assert.equal(
    crypto.createHash('sha256').update(historicalReplay).digest('hex'),
    '3483e69bbc1ca13ba151b75b59e7b8e192f96f9df2627b829340b4d4e50d62c5',
  );
  assert.equal([...historicalReplay.matchAll(/^\\ir\s+([^\s]+\.sql)\s*$/gmi)].length, 46);

  const failJobName = '04082026_1219_pay_workbench_fail_job.sql';
  const failJobMutation = new Map([[
    failJobName,
    `${readRepeatable(failJobName)}\n-- H1 closure-dependency mutation fixture\n`,
  ]]);
  const historicalBefore = closureForFixture(historicalReplayName);
  const historicalAfter = closureForFixture(historicalReplayName, failJobMutation);
  const finalBefore = closureForFixture(finalClosureName);
  const finalAfter = closureForFixture(finalClosureName, failJobMutation);
  assert.notEqual(historicalAfter.sha256, historicalBefore.sha256);
  assert.notEqual(finalAfter.sha256, finalBefore.sha256);
  assert.ok(finalBefore.paths.includes(`supabase/repeatable/${historicalReplayName}`));
  assert.ok(finalBefore.paths.includes(`supabase/repeatable/${failJobName}`));

  const preChangeSource = repair.replace(`\\ir ${historicalReplayName}\n`, '');
  assert.notEqual(preChangeSource, repair, 'red fixture must remove the missing dependency anchor');
  const preChangeOverride = new Map([[finalClosureName, preChangeSource]]);
  const preChangeBefore = closureForFixture(finalClosureName, preChangeOverride);
  const preChangeAfter = closureForFixture(finalClosureName, new Map([
    ...preChangeOverride,
    ...failJobMutation,
  ]));
  assert.equal(preChangeAfter.sha256, preChangeBefore.sha256,
    'pre-change final closure must reproduce omission of the transitive H1 dependency');

  const unrelatedName = '01092026_1647_banking_pay_signed_recovery_classifier_v1.sql';
  const unrelatedMutation = new Map([[
    unrelatedName,
    `${readRepeatable(unrelatedName)}\n-- unrelated closure fixture\n`,
  ]]);
  assert.equal(
    closureForFixture(finalClosureName, unrelatedMutation).sha256,
    finalBefore.sha256,
    'an unrelated repeatable must not change the bounded final closure',
  );
});

test('release inventory orders H1 roots, historical reassertions, then the established final closure', async () => {
  const { inventory } = await import('../scripts/cloudtms-db-release-lib.mjs');
  const paths = inventory().repeatables.map(item => item.path);
  const indexOf = name => {
    const index = paths.indexOf(`supabase/repeatable/${name}`);
    assert.notEqual(index, -1, `repeatable absent from release inventory: ${name}`);
    return index;
  };
  const h1Roots = [
    '04082026_1219_pay_workbench_fail_job.sql',
    '04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql',
    '07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql',
  ].map(indexOf);
  const focusedReassert = indexOf('08082026_0313_pay_workbench_fail_job_authority.sql');
  const historicalReassert = indexOf(historicalReplayName);
  const finalClosure = indexOf(finalClosureName);
  for (const rootIndex of h1Roots) {
    assert.ok(rootIndex < focusedReassert);
    assert.ok(rootIndex < historicalReassert);
  }
  assert.ok(focusedReassert < finalClosure);
  assert.ok(historicalReassert < finalClosure);
});

test('F7 does not overwrite INSERT_ITEMS and the exact combined H2 owner supersedes the rejected proposal', async () => {
  const targetDefinition = /CREATE\s+OR\s+REPLACE\s+FUNCTION\s+public\.pay_batch_insert_items_from_preview\s*\(/ig;
  const closure = closureForFixture(finalClosureName);
  const closureTargetOwners = closure.paths.filter((entryPath) => {
    const name = entryPath.replace(/^supabase\/repeatable\//, '');
    targetDefinition.lastIndex = 0;
    return targetDefinition.test(readRepeatable(name));
  });
  assert.deepEqual(closureTargetOwners, [], 'F7 closure must not redefine INSERT_ITEMS');

  const { inventory } = await import('../scripts/cloudtms-db-release-lib.mjs');
  const repeatables = inventory().repeatables;
  const targetOwners = repeatables.filter(({ path: entryPath }) => {
    targetDefinition.lastIndex = 0;
    return targetDefinition.test(normalizeLf(fs.readFileSync(path.join(root, entryPath), 'utf8')));
  });
  const finalClosureIndex = repeatables.findIndex(({ path: entryPath }) => (
    entryPath === f013OrderGuard.h1_final_closure_path
  ));
  assert.notEqual(finalClosureIndex, -1);
  assert.ok(targetOwners.some(({ path: entryPath }) => entryPath === f013OrderGuard.historical_owner_path));
  const laterOwners = targetOwners.filter(({ path: entryPath }) => (
    repeatables.findIndex(({ path: candidatePath }) => candidatePath === entryPath) > finalClosureIndex
  ));

  assert.equal(
    f013OrderGuard.status,
    'COMPATIBILITY_ONLY_PROVISIONAL_INTERFACE_DIVERGENCE_RECORDED_NO_RUNTIME_CORRECTION_AUTHORISED',
  );
  assert.equal(f013OrderGuard.latest_h2_evidence_only_status.provisional_interface_divergence_rows, 5);
  assert.equal(f013OrderGuard.latest_h2_evidence_only_status.final_proved_defect_rows, 0);
  assert.equal(
    f013OrderGuard.latest_h2_evidence_only_status.dual_engine_variant_result,
    'PG17.11_20_OF_20_PASS_AND_PG18.6_20_OF_20_PASS_ROLLBACK_ONLY',
  );
  assert.deepEqual(
    f013OrderGuard.latest_h2_evidence_only_status.downstream_phase_proved_correct_categories,
    { OVERPAYMENT_RECOVERY: 4 },
  );
  assert.deepEqual(
    f013OrderGuard.latest_h2_evidence_only_status.not_exercised_phases,
    ['CANONICAL_PREVIEW', 'VALIDATE_SESSION_AND_PREPARE', 'SCOPE_SEED', 'ALLOCATION_SEED'],
  );
  assert.equal(
    f013OrderGuard.latest_h2_evidence_only_status.artifacts_sha256[
      'tests/01092026_2313_banking_pay_draft_finance_category_end_to_end_verification.sql'
    ],
    'ba32d34f591277cd4a91e1e892eb265cfbb9fcfb2e4092f19c174dece7eec975',
  );
  assert.match(f013OrderGuard.h2_obsolete_provisional_owner_path, /^supabase\/repeatable\/\d{8}_\d{4}_[a-z0-9_]+[.]sql$/);
  assert.deepEqual(
    laterOwners.map(({ path: entryPath }) => entryPath),
    [
      f013OrderGuard.combined_h12_reconciliation.predecessor_owner_path,
      f013OrderGuard.combined_h12_reconciliation.current_owner_path,
    ],
    'the unified tree must contain the exact reviewed INSERT_ITEMS successor chain',
  );
  const predecessorOwner = laterOwners[0];
  assert.equal(
    crypto.createHash('sha256').update(normalizeLf(fs.readFileSync(path.join(root, predecessorOwner.path), 'utf8'))).digest('hex'),
    f013OrderGuard.combined_h12_reconciliation.predecessor_owner_sha256,
  );
  const combinedOwner = laterOwners.at(-1);
  assert.equal(
    crypto.createHash('sha256').update(normalizeLf(fs.readFileSync(path.join(root, combinedOwner.path), 'utf8'))).digest('hex'),
    f013OrderGuard.combined_h12_reconciliation.current_owner_sha256,
  );
  assert.equal(f013OrderGuard.combined_h12_reconciliation.economic_or_policy_delta_count, 0);
  assert.deepEqual(
    f013OrderGuard.combined_h12_reconciliation.visible_certified_finance_aliases,
    [
      'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
      'MANUAL_DEBT_RECOVERY',
      'LOAN_PAYOUT',
      'OVERPAYMENT_RECOVERY',
      'PAYMENT_ADVANCE_REPAYMENT',
      'UNDERPAYMENT_PAYMENT',
    ],
  );
  assert.equal(
    fs.existsSync(path.join(root, f013OrderGuard.h2_obsolete_provisional_owner_path)),
    false,
    'obsolete F-013 proposal must remain excluded from the candidate',
  );
  assert.match(f013OrderGuard.current_h1_only_disposition, /all-eight-alias premise (?:is|remains) invalidated/i);
  assert.match(f013OrderGuard.current_h1_only_disposition, /PAYMENT_ADVANCE_REPAYMENT to LOAN_REPAYMENT/i);
  assert.match(f013OrderGuard.current_h1_only_disposition, /MANUAL_CREDIT_ADJUSTMENT_PAYMENT to MANUAL_CREDIT_PAYOUT/i);
  assert.match(f013OrderGuard.current_h1_only_disposition, /PROVISIONAL_PROVED_INTERFACE_DIVERGENCE/);
  assert.match(f013OrderGuard.current_h1_only_disposition, /final proved defect rows are zero/i);
  assert.match(f013OrderGuard.current_h1_only_disposition, /no runtime correction is authorised/i);
  assert.equal(closure.paths.includes(f013OrderGuard.h2_obsolete_provisional_owner_path), false);
});

test('Workbench drain runs the bounded dirty-apply repair before ordinary claims', () => {
  const drainStart = worker.indexOf('async function drainBankingPayWorkbenchJobs');
  assert.notEqual(drainStart, -1);
  const repairCall = worker.indexOf("'pay_workbench_repair_invalid_dirty_apply_jobs_v1'", drainStart);
  const firstClaim = worker.indexOf("'pay_workbench_worker_drain_chunk_revalidated_v1'", repairCall);
  assert.ok(repairCall > drainStart);
  assert.ok(firstClaim > repairCall);
  assert.match(worker.slice(repairCall, firstClaim), /all_state_transitions_proven/i);
  assert.match(worker.slice(repairCall, firstClaim), /remaining_invalid_unrepaired_count/i);
  assert.match(worker.slice(repairCall, firstClaim), /WORKBENCH_INVALID_DIRTY_APPLY_REPAIR_FAILED/i);
});
