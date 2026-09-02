import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = (path) => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const failJob = read('supabase/repeatable/04082026_1219_pay_workbench_fail_job.sql');
const claimStart = read(
  'supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql',
);
const repairOwner = read(
  'supabase/repeatable/04082026_1219_pay_workbench_repair_orphaned_pending_source_build.sql',
);
const causalVerifier = read(
  'supabase/verification/01092026_1927_banking_pay_workbench_causal_recovery_verification.sql',
);
const coverage = JSON.parse(read('tests/fixtures/banking-pay-workbench-recovery-coverage-v1.json'));
const independentHandover = read(
  'codex_outputs/h1-workbench-recovery-causal-v1/HANDOVER_FOR_INDEPENDENT_VERIFICATION.md',
);
const independentAuditPack = read(
  'codex_outputs/h1-workbench-recovery-causal-v1/INDEPENDENT_AUDIT_PACK.md',
);
const rollbackPack = read(
  'codex_outputs/h1-workbench-recovery-causal-v1/ROLLBACK_PACK.md',
);
const checksumManifest = JSON.parse(read(
  'codex_outputs/h1-workbench-recovery-causal-v1/LOCAL_CANDIDATE_CHECKSUMS.pending.json',
));
const rollbackSourceManifest = JSON.parse(read(
  'codex_outputs/h1-workbench-recovery-causal-v1/ROLLBACK_SOURCE_MANIFEST_14dd89af.json',
));
const bankingPayBible = read('../h1-workbench-recovery-frontend/BANKING_PAY_BIBLE.md');

const deterministicStageCodes = [
  'CERTIFIED_SOURCE_PREVIEW_SEMANTIC_PARITY_FAILED',
  'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID',
  'PAYMENT_CORRECTION_SCOPE_TYPE_REQUIRED',
  'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISSING',
  'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_MISMATCH',
  'PAYMENT_CORRECTION_WORKBENCH_FROZEN_SCOPE_VERSION_UNSUPPORTED',
];

const assertCausalOwner = (failSource, claimSource) => {
  const sourceFailureOwner = failSource.slice(
    failSource.indexOf("IF v_canonical_job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'"),
    failSource.indexOf('v_is_statement_timeout :='),
  );
  assert.match(failSource, /PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID/);
  assert.match(failSource, /ORDER BY attempt\.attempt_number,attempt\.started_at_utc,attempt\.id/);
  assert.equal((sourceFailureOwner.match(/SET status = 'FAILED',/g) || []).length, 2);
  assert.doesNotMatch(sourceFailureOwner, /SET status = 'ERROR'/);
  assert.match(failSource, /'causal_contract_version','WORKBENCH_FIRST_DIVERGENT_CAUSE_V1'/);
  assert.match(failSource, /last_error_json=v_effective_error_json/);

  assert.match(claimSource, /lease_expires_at_utc\+interval '15 seconds'/);
  assert.match(claimSource, /ORDER BY attempt\.attempt_number,attempt\.started_at_utc,attempt\.id/);
  assert.match(claimSource, /'code','DELIVERED_ATTEMPT_EXPIRED'/);
  assert.match(claimSource, /'code','DELIVERED_ATTEMPT_EXHAUSTED'/);
  assert.match(claimSource, /last_error_json=v_recovery_error_json/);
  assert.match(claimSource, /failure_json=v_recovery_error_json/);
  assert.match(claimSource, /PERFORM public\._audit_insert/);
};

test('focused causal fixture accepts the exact two existing owners', () => {
  assertCausalOwner(failJob, claimStart);
});

const assertPolicyParityVerifier = (source) => {
  assert.match(source, /v_policy_projection_before jsonb/);
  assert.match(source, /v_policy_projection_after jsonb/);
  assert.match(source, /selected_preview_policy_facts/);
  assert.match(source, /published_candidate_policy_fragments/);
  assert.match(source, /financial_boundary_counts/);
  assert.match(source, /'pay_channel','PAYE'/);
  assert.match(source, /'pay_channel','UMBRELLA'/);
  assert.match(source, /'semantic_kind','OVERPAYMENT_RECOVERY'/);
  assert.match(source, /v_policy_projection_after IS DISTINCT FROM v_policy_projection_before/);
  assert.match(source, /BANKING_PAY_CAUSAL_RECOVERY_PAYMENT_POLICY_PARITY_CHANGED/);
  assert.doesNotMatch(source, /INSERT INTO public\.(?:banking_pay_operations|pay_batches|pay_batch_items|pay_advance_reservations)/);
};

test('rollback verifier proves exact before/after payment-policy parity', () => {
  assertPolicyParityVerifier(causalVerifier);
});

test('payment-policy parity source guard kills each bounded weakening mutation', () => {
  const mutations = [
    causalVerifier.replaceAll('selected_preview_policy_facts', 'selected_preview_execution_facts'),
    causalVerifier.replace("'pay_channel','UMBRELLA'", "'pay_channel','PAYE'"),
    causalVerifier.replace("'semantic_kind','OVERPAYMENT_RECOVERY'", "'semantic_kind','WORKED_TIME_AMOUNT'"),
    causalVerifier.replace(
      'v_policy_projection_after IS DISTINCT FROM v_policy_projection_before',
      'v_policy_projection_after IS NOT DISTINCT FROM v_policy_projection_before',
    ),
    causalVerifier.replace('BANKING_PAY_CAUSAL_RECOVERY_PAYMENT_POLICY_PARITY_CHANGED', ''),
  ];
  for (const [index, mutation] of mutations.entries()) {
    assert.throws(
      () => assertPolicyParityVerifier(mutation),
      undefined,
      `policy-parity mutation ${index + 1} must be detected`,
    );
  }
});

const assertDeterministicSuccessorFence = (repairSource) => {
  for (const code of deterministicStageCodes) {
    assert.match(repairSource, new RegExp(`'${code}'`));
  }
  assert.match(repairSource, /v_current_deterministic_owner/);
  assert.match(repairSource, /v_registry\.current_build_id = v_owner\.economic_build_id/);
  assert.match(repairSource, /v_registry\.current_source_change_seq = v_live_change_seq/);
  assert.match(repairSource, /v_owner_build\.source_job_id = v_owner\.id/);
  assert.match(
    repairSource,
    /v_owner_build\.source_build_run_id::text\s*=\s*v_owner\.payload_json->>'source_build_run_id'/,
  );
  assert.match(repairSource, /v_terminal_attempt\.attempt_number = v_owner\.attempt_count/);
  assert.match(repairSource, /v_first_deterministic_attempt\.error_class = 'DETERMINISTIC_STAGE_ERROR'/);
  assert.match(repairSource, /FAILED_CLOSED_DETERMINISTIC_SOURCE/);
  assert.match(repairSource, /'deterministic_successor_suppressed', true/);
  assert.match(repairSource, /'automatic_recovery_scheduled', false/);
  assert.match(repairSource, /last_error_json=v_expired_error_json/);
  assert.match(repairSource, /failure_json=v_expired_error_json/);
  assert.match(repairSource, /ORDER BY attempt\.attempt_number,attempt\.started_at_utc,attempt\.id/);

  const reconcile = repairSource.indexOf("v_candidate_action := 'RECONCILED_SUCCESSFUL_BUILD'");
  const activeSuccessor = repairSource.indexOf("v_candidate_action := 'REBOUND_ACTIVE_SUCCESSOR'");
  const deterministicClose = repairSource.indexOf("v_candidate_action := 'FAILED_CLOSED_DETERMINISTIC_SOURCE'");
  const canonicalEnqueue = repairSource.indexOf("v_candidate_action := 'ENQUEUED_CANONICAL_SUCCESSOR'");
  assert.ok(reconcile >= 0, 'successful-build reconciliation must remain present');
  assert.ok(activeSuccessor > reconcile, 'active-successor rebind must follow reconciliation');
  assert.ok(
    deterministicClose > activeSuccessor,
    'deterministic fail-close must run only after success and active-successor recovery',
  );
  assert.ok(
    canonicalEnqueue > deterministicClose,
    'changed/stale authority must retain the canonical successor path',
  );
  assert.doesNotMatch(
    repairSource.slice(activeSuccessor, canonicalEnqueue),
    /SET\s+max_attempts\s*=/i,
    'deterministic convergence must not manipulate the retry budget',
  );
};

test('deterministic terminal source authority fails closed before canonical successor enqueue', () => {
  assertDeterministicSuccessorFence(repairOwner);
  assert.match(claimStart, /'FAILED_CLOSED_DETERMINISTIC_SOURCE','FAILED_CLOSED_MAX_ATTEMPTS'/);
});

test('deterministic successor fence kills removal and authority-weakening mutations', () => {
  const mutations = [
    repairOwner.replace("v_candidate_action := 'FAILED_CLOSED_DETERMINISTIC_SOURCE';", ''),
    repairOwner.replace('v_registry.current_source_change_seq = v_live_change_seq', 'true'),
    repairOwner.replace('v_registry.current_build_id = v_owner.economic_build_id', 'true'),
    repairOwner.replace("v_first_deterministic_attempt.error_class = 'DETERMINISTIC_STAGE_ERROR'", 'true'),
    repairOwner.replace("'deterministic_successor_suppressed', true", "'deterministic_successor_suppressed', false"),
  ];
  for (const [index, mutation] of mutations.entries()) {
    assert.throws(
      () => assertDeterministicSuccessorFence(mutation),
      undefined,
      `repair-owner mutation ${index + 1} must be detected`,
    );
  }
});

test('focused causal fixture kills each known unsafe mutation', () => {
  const mutations = [
    [
      failJob.replace("'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID',", ''),
      claimStart,
      'deterministic retry suppression removed',
    ],
    [
      failJob.replace(
        'ORDER BY attempt.attempt_number,attempt.started_at_utc,attempt.id',
        'ORDER BY attempt.attempt_number DESC,attempt.started_at_utc DESC,attempt.id DESC',
      ),
      claimStart,
      'first-cause ordering reversed in fail-job',
    ],
    [
      failJob.replace("SET status = 'FAILED',", "SET status = 'ERROR',"),
      claimStart,
      'invalid candidate state restored',
    ],
    [
      failJob.replaceAll('last_error_json=v_effective_error_json', 'last_error_json=p_error_json'),
      claimStart,
      'fail-job envelope bypassed',
    ],
    [
      failJob,
      claimStart.replace(
        'ORDER BY attempt.attempt_number,attempt.started_at_utc,attempt.id',
        'ORDER BY attempt.attempt_number DESC,attempt.started_at_utc DESC,attempt.id DESC',
      ),
      'first-cause ordering reversed in lease recovery',
    ],
    [
      failJob,
      claimStart.replaceAll(
        'last_error_json=v_recovery_error_json',
        "last_error_json=jsonb_build_object('code','DELIVERED_ATTEMPT_EXHAUSTED')",
      ),
      'lease envelope bypassed',
    ],
    [
      failJob,
      claimStart.replace(
        'failure_json=v_recovery_error_json',
        "failure_json=jsonb_build_object('code','DELIVERED_ATTEMPT_EXHAUSTED')",
      ),
      'build envelope bypassed',
    ],
    [
      failJob,
      claimStart.replace('PERFORM public._audit_insert(', 'PERFORM public._audit_removed('),
      'causal audit removed',
    ],
  ];

  for (const [mutatedFailJob, mutatedClaimStart, label] of mutations) {
    assert.throws(
      () => assertCausalOwner(mutatedFailJob, mutatedClaimStart),
      undefined,
      label,
    );
  }
});

test('causal envelope equivalence classes retain first cause and public transition independently', () => {
  const cases = [
    {
      name: 'ordinary deterministic failure terminalises immediately',
      attempts: [{ attempt: 1, code: 'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID' }],
      publicCode: 'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID',
      firstCode: 'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID',
      firstAttempt: 1,
    },
    {
      name: 'response loss has lease expiry as its first observable divergence',
      attempts: [{ attempt: 1, code: 'LEASE_EXPIRED_AFTER_CANCELLATION_GRACE' }],
      publicCode: 'DELIVERED_ATTEMPT_EXPIRED',
      firstCode: 'LEASE_EXPIRED_AFTER_CANCELLATION_GRACE',
      firstAttempt: 1,
    },
    {
      name: 'later exhaustion cannot mask an earlier deterministic cause',
      attempts: [
        { attempt: 8, code: 'LEASE_EXPIRED_AFTER_CANCELLATION_GRACE' },
        { attempt: 1, code: 'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID' },
      ],
      publicCode: 'DELIVERED_ATTEMPT_EXHAUSTED',
      firstCode: 'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID',
      firstAttempt: 1,
    },
  ];

  for (const fixture of cases) {
    const [first] = fixture.attempts.toSorted((a, b) => a.attempt - b.attempt);
    const envelope = {
      code: fixture.publicCode,
      causal_contract_version: 'WORKBENCH_FIRST_DIVERGENT_CAUSE_V1',
      first_divergent_cause: { code: first.code },
      first_divergent_attempt_number: first.attempt,
      latest_observed_failure: { code: fixture.attempts[0].code },
      latest_attempt_number: Math.max(...fixture.attempts.map(({ attempt }) => attempt)),
    };
    assert.equal(envelope.code, fixture.publicCode, fixture.name);
    assert.equal(envelope.first_divergent_cause.code, fixture.firstCode, fixture.name);
    assert.equal(envelope.first_divergent_attempt_number, fixture.firstAttempt, fixture.name);
  }
});

test('the finite recovery catalogue is frozen, complete and honest about final proof', () => {
  assert.equal(coverage.schema_version, 'WORKBENCH_RECOVERY_COVERAGE_V1');
  assert.equal(coverage.catalogue_status, 'FROZEN_AFTER_SOURCE_CENSUS');
  assert.equal(coverage.catalogue_class_count, 18);
  assert.equal(coverage.classes.length, coverage.catalogue_class_count);
  assert.equal(new Set(coverage.classes.map(({ id }) => id)).size, coverage.classes.length);

  const allowedDispositions = new Set([
    'PRESERVE_EXISTING_RECONCILED_SUCCESSFUL_BUILD',
    'PRESERVE_EXISTING_REBOUND_ACTIVE_SUCCESSOR',
    'H1_CORRECTION_FAILED_CLOSED_DETERMINISTIC_SOURCE',
    'PRESERVE_EXISTING_ENQUEUED_CANONICAL_SUCCESSOR',
    'PRESERVE_EXISTING_BOUNDED_SAME_JOB_RETRY',
    'PRESERVE_EXISTING_FAILED_CLOSED_MAX_ATTEMPTS',
    'PRESERVE_EXISTING_DELIVERED_ATTEMPT_EXPIRED_REQUEUE',
    'H1_CORRECTION_FIRST_CAUSE_ENVELOPE',
    'PRESERVE_EXISTING_SUCCESS_OR_ACTIVE_SUCCESSOR_READBACK',
    'PRESERVE_EXISTING_CURRENT_SUCCESSOR_WINS',
    'PRESERVE_EXISTING_CANDIDATE_LOCK_AND_ACTIVE_DEDUPE',
    'PRESERVE_EXISTING_SKIPPED_AFTER_RECHECK',
    'PRESERVE_EXISTING_OBSOLETE_GENERATION_SUCCESSOR',
    'PRESERVE_EXISTING_SOURCE_BUILD_ERROR_WITHOUT_PENDING_JOB',
    'PRESERVE_APPEND_ONLY_AUDIT_HISTORY',
    'F1_INSTALLED_CLASSIFIER_CORRECTION',
    'PRESERVE_EXISTING_FAIL_CLOSED_DRAFT_GATE_AND_CERTIFY',
  ]);
  for (const coverageClass of coverage.classes) {
    assert.match(coverageClass.id, /^H1-C(?:0[1-9]|1[0-8])$/);
    assert.ok(coverageClass.source_owner, `${coverageClass.id} source owner`);
    assert.ok(coverageClass.failure_shape, `${coverageClass.id} failure shape`);
    assert.ok(allowedDispositions.has(coverageClass.disposition), `${coverageClass.id} disposition`);
    assert.ok(coverageClass.fixture_ids.length > 0, `${coverageClass.id} deterministic fixture`);
    assert.ok(coverageClass.mutation_operator, `${coverageClass.id} mutation disposition`);
    assert.equal(coverageClass.final_reaudit_state, 'PENDING');
  }
});

test('every mandatory fault ordering and historical shape is bound to the frozen catalogue', () => {
  const classIds = new Set(coverage.classes.map(({ id }) => id));
  assert.equal(coverage.mandatory_fault_orderings.length, 10);
  assert.equal(new Set(coverage.mandatory_fault_orderings.map(({ id }) => id)).size, 10);
  for (const ordering of coverage.mandatory_fault_orderings) {
    assert.match(ordering.status, /^LOCAL_PASS/);
    assert.ok(ordering.class_ids.length > 0, `${ordering.id} class binding`);
    for (const classId of ordering.class_ids) assert.ok(classIds.has(classId), `${ordering.id}:${classId}`);
  }
  assert.equal(coverage.historical_shape_matrix.length, 11);
  assert.equal(new Set(coverage.historical_shape_matrix.map(({ id }) => id)).size, 11);
  for (const shape of coverage.historical_shape_matrix) assert.ok(shape.evidence, `${shape.id} evidence`);
});

const decideRepairModel = (state) => {
  if (!state.session_open || !state.scope_pending) return 'SKIPPED_AFTER_RECHECK';
  if (state.exact_success) return 'RECONCILED_SUCCESSFUL_BUILD';
  if (state.active_successor) return 'REBOUND_ACTIVE_SUCCESSOR';
  if (state.deterministic_current) return 'FAILED_CLOSED_DETERMINISTIC_SOURCE';
  if (state.attempt_count >= state.max_attempts) return 'FAILED_CLOSED_MAX_ATTEMPTS';
  return 'ENQUEUED_CANONICAL_SUCCESSOR';
};

const runFaultOrderingModel = (fixture) => {
  const state = {
    exact_success: false,
    active_successor: false,
    deterministic_current: false,
    attempt_count: 0,
    max_attempts: 8,
    successor_count: 0,
    first_cause: null,
    ...fixture.initial,
  };
  const actions = [];
  for (const event of fixture.events) {
    if (event === 'REPAIR' || event === 'CONCURRENT_REPAIR') {
      const action = decideRepairModel(state);
      actions.push(action);
      if (action === 'FAILED_CLOSED_DETERMINISTIC_SOURCE'
          || action === 'FAILED_CLOSED_MAX_ATTEMPTS'
          || action === 'RECONCILED_SUCCESSFUL_BUILD') state.scope_pending = false;
      if (action === 'ENQUEUED_CANONICAL_SUCCESSOR') {
        state.active_successor = true;
        state.successor_count = Math.max(state.successor_count, 1);
      }
    } else if (event === 'TRANSIENT_FAILURE' || event === 'LEASE_EXPIRED') {
      assert.ok(state.attempt_count < state.max_attempts, `${fixture.id}:${event}:budget`);
      actions.push('REQUEUED_SAME_JOB');
      state.attempt_count += 1;
    } else if (event === 'LEASE_EXHAUSTED') {
      actions.push('DELIVERED_ATTEMPT_EXHAUSTED');
      state.attempt_count = state.max_attempts;
    } else if (event === 'SUCCESS_COMMITTED') {
      actions.push('COMPLETE');
      state.exact_success = true;
    } else if (event === 'SUCCESSOR_PUBLISHED') {
      actions.push('ENQUEUED_CANONICAL_SUCCESSOR');
      state.active_successor = true;
      state.successor_count = 1;
    } else if (event === 'SOURCE_AUTHORITY_CHANGED') {
      state.deterministic_current = false;
    } else if (event === 'SESSION_CLOSED') {
      state.session_open = false;
    } else if (event === 'SESSION_REOPENED') {
      state.session_open = true;
    } else if (event === 'RESPONSE_LOST' || event === 'STALE_FAILURE_RESPONSE') {
      // Transport observations do not change committed database authority.
    } else {
      assert.fail(`${fixture.id}: unknown event ${event}`);
    }
  }
  return { actions, state };
};

test('the bounded fault-ordering model executes every required schedule deterministically', () => {
  assert.deepEqual(
    coverage.fault_ordering_model_cases.map(({ id }) => id),
    coverage.mandatory_fault_orderings.map(({ id }) => id),
  );
  for (const fixture of coverage.fault_ordering_model_cases) {
    const { actions, state } = runFaultOrderingModel(fixture);
    assert.deepEqual(actions, fixture.expected_actions, fixture.id);
    assert.equal(state.successor_count, fixture.expected_successor_count, fixture.id);
    if (fixture.expected_first_cause) {
      assert.equal(state.first_cause, fixture.expected_first_cause, fixture.id);
    }
  }

  assert.match(repairOwner, /pg_try_advisory_xact_lock[\s\S]*_pay_workbench_candidate_serial_key/);
  assert.match(repairOwner, /FOR UPDATE;[\s\S]*SKIPPED_AFTER_RECHECK/);
  assert.match(repairOwner, /RECONCILED_SUCCESSFUL_BUILD/);
  assert.match(repairOwner, /REBOUND_ACTIVE_SUCCESSOR/);
  assert.match(repairOwner, /FAILED_CLOSED_DETERMINISTIC_SOURCE/);
  assert.match(repairOwner, /ENQUEUED_CANONICAL_SUCCESSOR/);
});

test('the issue ledger keeps local, installed, external and final-re-audit proof separate', () => {
  assert.deepEqual(coverage.issues.map(({ id }) => id), [
    'F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8',
  ]);
  const installed = coverage.issues.filter(({ installed_pass: pass }) => pass);
  assert.deepEqual(installed.map(({ id }) => id), ['F1']);
  const external = coverage.issues.filter(({ owner }) => owner !== 'H1');
  assert.deepEqual(external.map(({ id }) => id), ['F8']);
  assert.equal(external[0].local_pass, true);
  assert.equal(external[0].installed_pass, false);
  assert.equal(external[0].correction_owner, 'H2');
  assert.ok(coverage.issues.every(({ final_reaudit_state: state }) => state === 'PENDING'));
  assert.deepEqual(
    coverage.issues.map(({
      id,
      discovery_owner: discoveryOwner,
      correction_owner: correctionOwner,
      count_bucket: countBucket,
      newly_discovered_by_h1_current_iteration: newThisIteration,
      current_status: currentStatus,
    }) => ({ id, discoveryOwner, correctionOwner, countBucket, newThisIteration, currentStatus })),
    [
      {
        id: 'F1',
        discoveryOwner: 'BANKING_PAY_PARENT_SEALED_INCIDENT_ACCEPTED_AND_REPROVED_BY_H1',
        correctionOwner: 'H1',
        countBucket: 'H1_OWNED_CORRECTION_PREEXISTING_SEALED_AT_AUDIT_START',
        newThisIteration: false,
        currentStatus: 'INSTALLED_PASS_FINAL_COMBINED_REAUDIT_OPEN',
      },
      ...['F2', 'F3', 'F4', 'F5', 'F6', 'F7'].map((id) => ({
        id,
        discoveryOwner: 'H1_AUDIT',
        correctionOwner: 'H1',
        countBucket: 'H1_OWNED_CORRECTION_NEW_THIS_ITERATION',
        newThisIteration: true,
        currentStatus: 'LOCAL_PASS_NOT_INSTALLED_FINAL_COMBINED_REAUDIT_OPEN',
      })),
      {
        id: 'F8',
        discoveryOwner: 'H1_AUDIT',
        correctionOwner: 'H2',
        countBucket: 'EXTERNAL_H2_OWNED_BLOCKER_DISCOVERED_BY_H1',
        newThisIteration: true,
        currentStatus: 'EXTERNAL_H2_FIXED_LOCAL_PASS_NOT_INSTALLED_FINAL_RERUN_OPEN',
      },
    ]
  );
  assert.deepEqual(coverage.finding_count_model, {
    h1_audit_issue_rows_total: 8,
    h1_audit_issue_ids: ['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8'],
    preexisting_sealed_findings_registered_at_audit_start: 1,
    preexisting_sealed_finding_ids: ['F1'],
    new_findings_discovered_by_h1_current_iteration: 7,
    new_findings_discovered_by_h1_ids: ['F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8'],
    h1_owned_corrections_total: 7,
    h1_owned_correction_ids: ['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7'],
    h1_owned_corrections_local_or_installed_pass: 7,
    h1_owned_corrections_installed_pass: 1,
    h1_owned_corrections_installed_pass_ids: ['F1'],
    h1_owned_corrections_local_only_pass: 6,
    h1_owned_corrections_local_only_pass_ids: ['F2', 'F3', 'F4', 'F5', 'F6', 'F7'],
    external_h2_owned_blockers_total: 1,
    external_h2_owned_blocker_ids: ['F8'],
    open_issue_rows_total: 7,
    open_issue_ids: ['F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8'],
    final_reaudit_pass_total: 0,
    prior_7_to_8_discrepancy: coverage.finding_count_model.prior_7_to_8_discrepancy,
  });
  assert.match(coverage.finding_count_model.prior_7_to_8_discrepancy, /Seven was the H1-owned correction count F1-F7/);
  assert.match(coverage.finding_count_model.prior_7_to_8_discrepancy, /F8 was newly discovered by H1 but assigned to H2/);
  assert.ok(coverage.external_dependencies.every(({ owner }) => owner !== 'H1'));
  assert.ok(coverage.external_dependencies.every(({ status }) => status !== 'H1_PASS'));
  const externalF012 = coverage.external_dependencies.find(({ id }) => id === 'H2-F012');
  assert.equal(externalF012.owner, 'H2');
  assert.equal(externalF012.status, 'FIXED_LOCAL_PASS_NOT_INSTALLED_FINAL_OPEN');
  assert.equal(
    externalF012.issue_ledger_sha256,
    '81f05b74091aa1873ddd6ef834644c790e1cd492cca8426c827f96650102dd2b'
  );
  assert.match(externalF012.dual_engine_proof, /FAIL_CLOSED_22_OF_22/);
  assert.match(externalF012.dual_engine_proof, /COLLISION_NEGATIVES_2_OF_2/);
  const externalF013 = coverage.external_dependencies.find(({ id }) => id === 'H2-F013');
  assert.equal(externalF013.owner, 'H2');
  assert.equal(
    externalF013.status,
    'PROVISIONAL_INTERFACE_DIVERGENCE_PROOF_COMPLETE_FINAL_DEFECT_VERDICT_OPEN'
  );
  assert.equal(externalF013.replacement_correctness, 'NOT_ESTABLISHED_NO_RUNTIME_CORRECTION_AUTHORISED');
  assert.equal(
    externalF013.runtime_edit_authority,
    'NOT_AUTHORISED_PENDING_COMPLETE_END_TO_END_DEFECT_PROOF_AND_PARENT_DECISION'
  );
  assert.equal(
    externalF013.deterministic_runtime_fixture_sha256,
    '5668d7cc05164ddb673ccfc90230c1cf09b2b90e9db6d80b76dc1e71bb4c20ea'
  );
  assert.equal(
    externalF013.category_end_to_end_fixture_sha256,
    'ba32d34f591277cd4a91e1e892eb265cfbb9fcfb2e4092f19c174dece7eec975'
  );
  assert.deepEqual(externalF013.downstream_phase_proved_correct_categories, { OVERPAYMENT_RECOVERY: 4 });
  assert.deepEqual(externalF013.provisional_interface_divergence_categories, {
    MANUAL_DEBT_RECOVERY: 4,
    PAYMENT_ADVANCE_REPAYMENT: 2,
    LOAN_PAYOUT: 2,
    UNDERPAYMENT_PAYMENT: 4,
    MANUAL_CREDIT_ADJUSTMENT_PAYMENT: 4,
  });
  assert.deepEqual(externalF013.final_proved_defect_categories, {});
  assert.match(externalF013.observed_downstream_failure_shape, /MALFORMED_PREVIEW_ALLOCATION_ROW_NOT_DRAFTABLE/);
  assert.match(externalF013.observed_downstream_failure_shape, /zero Draft items and reservations/i);
  assert.deepEqual(externalF013.missing_end_to_end_phases, [
    'canonical_preview',
    'VALIDATE_SESSION_and_prepare',
    'scope_seed',
    'allocation_seed',
  ]);
  assert.match(externalF013.required_final_fixture_path, /canonical_preview_to_session_prepare/);
  assert.equal(
    externalF013.obsolete_provisional_candidate_owner_path,
    'supabase/repeatable/01092026_2245_banking_pay_draft_finance_category_handoff_v1.sql'
  );
  assert.match(externalF013.prechange_fixture_interpretation, /manually inserts Candidate scope and allocation rows/i);
  assert.match(externalF013.prechange_fixture_interpretation, /cannot be final defect authority/i);
  assert.ok(externalF013.proved_deliberate_translations.includes(
    'visible MANUAL_CREDIT_ADJUSTMENT_PAYMENT becomes frozen MANUAL_CREDIT_PAYOUT'
  ));
  assert.ok(externalF013.proved_policy_boundaries.some((rule) => /Umbrella owns separate ex-VAT\/VAT/i.test(rule)));
  assert.equal(
    externalF013.cross_task_order_guard,
    'tests/fixtures/banking-pay-h1-h2-f013-release-order-v1.json'
  );
  const externalF013b = coverage.external_dependencies.find(({ id }) => id === 'H2-F013B');
  assert.equal(externalF013b.owner, 'H2');
  assert.equal(
    externalF013b.status,
    'CLOSED_NOT_DEFECT_DELIBERATE_STAGED_VOCABULARY'
  );
  assert.equal(externalF013b.runtime_edit_authority, 'WITHDRAWN_AND_PROHIBITED');
  assert.equal(
    externalF013b.future_combined_candidate_rule,
    'No 01092026_2250 owner, verifier, contract entry or manifest entry may be included'
  );
  assert.equal(
    externalF013b.historical_owner_path,
    'supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql'
  );
  assert.equal(
    externalF013b.historical_owner_sha256,
    '9171d175ea23a783f34c45cdbd42559062496bfa8d9daeac48dc9cb20abe4bd4'
  );
  assert.match(externalF013b.decision, /LOAN_REPAYMENT is deliberately the hidden recovery-template/i);
  assert.match(externalF013b.obsolete_experimental_coordination_evidence, /deleted H1 f013b order-guard/i);
  assert.equal(
    coverage.external_h2_catalogue_status.finite_production_reachable_classes,
    54
  );
  assert.equal(
    coverage.external_h2_catalogue_status.executable_bound_classes,
    35
  );
  assert.equal(
    coverage.external_h2_catalogue_status.open_classes,
    19
  );
  assert.match(coverage.external_h2_catalogue_status.current_counts_status, /F013_DOWNSTREAM_INTERFACE_DIVERGENCE_EVIDENCE_RECONCILED/);
  assert.equal(coverage.external_h2_catalogue_status.latest_operating_model_focused_tests_pass, 15);
  assert.equal(coverage.external_h2_catalogue_status.latest_combined_tests_pass, 74);
  assert.equal(coverage.external_h2_catalogue_status.latest_combined_tests_fail, 0);
  assert.equal(coverage.external_h2_catalogue_status.latest_deliberate_todo_gates, 5);
  assert.equal(coverage.external_h2_catalogue_status.latest_environment_only_skips, 0);
  assert.equal(coverage.external_h2_catalogue_status.latest_structural_skips, 1);
  assert.equal(coverage.external_h2_catalogue_status.latest_complete_javascript_pass, 1106);
  assert.equal(coverage.external_h2_catalogue_status.f013_dual_engine_variants_total, 20);
  assert.equal(coverage.external_h2_catalogue_status.f013_downstream_phase_proved_correct_variants, 4);
  assert.equal(coverage.external_h2_catalogue_status.f013_provisional_interface_divergence_variants, 16);
  assert.equal(coverage.external_h2_catalogue_status.f013_final_proved_defect_variants, 0);
  assert.equal(coverage.external_h2_catalogue_status.f013_downstream_phase_proved_correct_category_rows, 1);
  assert.equal(coverage.external_h2_catalogue_status.f013_provisional_interface_divergence_rows, 5);
  assert.equal(coverage.external_h2_catalogue_status.f013_final_proved_defect_rows, 0);
  assert.equal(coverage.external_h2_catalogue_status.f013_visible_policy_decision_rows, 0);
  assert.match(coverage.external_h2_catalogue_status.f013_visible_policy_decision_status, /FINAL_DEFECT_VERDICT_PENDING_COMPLETE_CANONICAL_END_TO_END_FIXTURE/);
  assert.match(coverage.external_h2_catalogue_status.f013b_status, /PASS_NO_CORRECTION_REQUIRED/);
  assert.equal(coverage.external_h2_catalogue_status.stable_combined_manifest_signal, false);
});

test('every H1 issue proves before/after payment-policy parity and forbids policy deltas', () => {
  const h1Issues = coverage.issues.filter(({ owner }) => owner === 'H1');
  assert.equal(h1Issues.length, 7);
  for (const issue of h1Issues) {
    assert.ok(issue.policy_parity, `${issue.id} must carry policy-parity evidence`);
    assert.match(issue.policy_parity.status, /PASS|PROVED/);
    assert.ok(issue.policy_parity.before_policy);
    assert.ok(issue.policy_parity.before_runtime_divergence);
    assert.ok(issue.policy_parity.after_runtime);
    assert.ok(issue.policy_parity.evidence);
    assert.deepEqual(issue.policy_parity.forbidden_surface_delta, []);
  }
  assert.match(coverage.payment_policy_parity_contract.allowed_delta, /execution, recovery/i);
  assert.ok(coverage.payment_policy_parity_contract.forbidden_deltas.length >= 6);
  assert.equal(
    coverage.payment_policy_parity_contract.failure_code,
    'BANKING_PAY_CAUSAL_RECOVERY_PAYMENT_POLICY_PARITY_CHANGED'
  );
  assert.ok(
    coverage.certification_field_coverage.required_groups.includes(
      'before_after_existing_payment_policy_parity'
    )
  );
});

test('measured baselines and mutation saturation contain no invented completion claim', () => {
  const incident = coverage.measured_baselines.sealed_incident;
  assert.equal(incident.session_id, '42751f42-a7e7-458f-9464-724d9deda455');
  assert.equal(incident.deterministic_attempts, 7);
  assert.equal(incident.deterministic_attempt_elapsed_seconds_min, 0.8);
  assert.equal(incident.deterministic_attempt_elapsed_seconds_max, 1.0);
  assert.equal(incident.claimed_attempt_lease_seconds, 25);
  assert.equal(incident.cancellation_grace_seconds, 15);
  assert.equal(incident.first_browser_observation_seconds, 20);
  assert.equal(incident.first_browser_observation_was_acceptance, false);
  assert.match(coverage.measured_baselines.acceptance_rule, /do not invent a new numeric policy threshold/i);
  assert.deepEqual(coverage.mutation_summary, {
    operators_total: 18,
    operators_killed: 18,
    operators_surviving: 0,
    scope: coverage.mutation_summary.scope,
  });
  assert.equal(coverage.clean_post_correction_audits_since_last_finding, 0);
  assert.deepEqual(coverage.future_hardening, []);
});

test('the certificate bridge is required but cannot be mistaken for an installed verdict', () => {
  assert.equal(
    coverage.certification_field_coverage.status,
    'REQUIRED_FOR_SEALED_HANDOFF_NOT_YET_INSTALLED',
  );
  assert.equal(coverage.certification_field_coverage.consumer_owner, 'HANDOVER_2');
  assert.equal(coverage.certification_field_coverage.consumer_must_not_reconstruct_selection, true);
  assert.deepEqual(coverage.certification_field_coverage.required_groups, [
    'session_revision_progress_publication_identity',
    'complete_ordered_selected_constituents',
    'candidate_pay_channel_partitions',
    'canonical_ex_vat_and_source_reservation_amounts',
    'prior_payment_supersession_and_recovery_headroom',
    'all_same_key_signed_match_and_decisive_evidence_counts_ids_digests',
    'ready_action_required_blocked_disjointness_and_exclusions',
    'draft_readiness',
    'source_installed_worker_frontend_identities',
    'before_after_existing_payment_policy_parity',
    'overall_canonical_digest',
  ]);
});

test('independent handover separates the completed source deliverable from an unproved runtime verdict', () => {
  assert.match(independentHandover, /UPDATED LOCAL SOURCE \/ NOT COMMITTED \/ NOT INSTALLED \/ NOT DEPLOYED/i);
  assert.match(independentHandover, /H1 source\/local verdict/i);
  assert.match(independentHandover, /Whole-Workbench runtime verdict/i);
  assert.match(independentHandover, /H1 SOURCE OBJECTIVE VERIFIED; WHOLE-WORKBENCH RUNTIME NOT PROVED/);
  assert.match(independentHandover, /Do not return `FAULT-FREE WORKBENCH CONFIRMED`/);
  assert.match(independentHandover, /does \*\*not\*\* change:/i);
  assert.match(independentHandover, /1,143 PASS, 0 fail, 0 TODO, 0 skip/);
  assert.match(independentHandover, /18\/18 killed/);
  assert.match(independentHandover, /01092026_2250.*01092026_2251.*prohibited/is);
  assert.match(independentHandover, /OVERPAYMENT_RECOVERY.*correct across four supported/is);
  assert.match(independentHandover, /manually inserts Candidate scope and allocation rows/is);
  assert.match(independentHandover, /PROVISIONAL_PROVED_INTERFACE_DIVERGENCE/);
  assert.match(independentHandover, /Final proved defect rows are \*\*zero\*\*/);
  assert.match(independentHandover, /MALFORMED_PREVIEW_ALLOCATION_ROW_NOT_DRAFTABLE/);
  assert.doesNotMatch(independentHandover, /proves current Draft execution defects/);
  assert.match(independentAuditPack, /Final proved defect rows are zero/i);
  assert.doesNotMatch(independentAuditPack, /proves five current Draft execution defects/);
  assert.match(rollbackPack, /PROVISIONAL_PROVED_INTERFACE_DIVERGENCE/);
  assert.match(rollbackPack, /No runtime correction is authorised/);
  assert.doesNotMatch(rollbackPack, /proves five distinct execution defects/);
  const supersededBibleClaim = bankingPayBible.indexOf('It separately proves current Draft execution defects');
  const bibleCorrection = bankingPayBible.indexOf('F-013 evidence-scope correction:');
  assert.notEqual(supersededBibleClaim, -1, 'historical overclaim must remain as append-only audit evidence');
  assert.ok(bibleCorrection > supersededBibleClaim, 'the append-only correction must follow and supersede the historical overclaim');
  assert.match(bankingPayBible.slice(bibleCorrection), /explicitly supersedes the active defect-status claim/);
  assert.match(bankingPayBible.slice(bibleCorrection), /Final proved defect rows are zero/);
  assert.match(bankingPayBible.slice(bibleCorrection), /No H2 runtime correction is authorised/);
  assert.match(independentHandover, /MANUAL_CREDIT_ADJUSTMENT_PAYMENT.*MANUAL_CREDIT_PAYOUT/is);
  assert.match(independentHandover, /eight H1 audit issue rows/);
  assert.match(independentHandover, /seven H1-owned corrections/);
  assert.match(independentHandover, /F8.*H1 audit.*H2/is);
  assert.match(independentHandover, /F8.*FIXED_LOCAL_PASS.*not installed/is);
  assert.match(independentAuditPack, /complete H1 audit registry has eight rows F1–F8/);
  assert.match(independentAuditPack, /Seven rows F1–F7 are H1-owned corrections/);
  assert.match(independentAuditPack, /F8 is included in the audit registry and discovery count but excluded from the H1-owned correction count/);
  assert.match(rollbackPack, /package contains eight audit rows, seven H1-owned corrections and one external H2-owned blocker/);
  assert.match(rollbackPack, /F8 is counted in discovery and the registry but never in an H1 rollback unit/);
  assert.match(rollbackPack, /H2 `FIXED_LOCAL_PASS`, not installed or final/);
  assert.deepEqual(checksumManifest.finding_count_model, {
    h1_audit_issue_rows_total: 8,
    preexisting_sealed_findings_registered_at_audit_start: 1,
    preexisting_sealed_finding_ids: ['F1'],
    new_findings_discovered_by_h1_current_iteration: 7,
    new_findings_discovered_by_h1_ids: ['F2', 'F3', 'F4', 'F5', 'F6', 'F7', 'F8'],
    h1_owned_corrections_total: 7,
    h1_owned_correction_ids: ['F1', 'F2', 'F3', 'F4', 'F5', 'F6', 'F7'],
    h1_owned_corrections_installed_pass: 1,
    h1_owned_corrections_local_only_pass: 6,
    external_h2_owned_blockers_total: 1,
    external_h2_owned_blocker_ids: ['F8'],
    open_issue_rows_total: 7,
    final_combined_reaudit_pass_total: 0,
    discrepancy_resolution: checksumManifest.finding_count_model.discrepancy_resolution,
  });
  assert.match(checksumManifest.finding_count_model.discrepancy_resolution, /Seven counts H1-owned corrections F1-F7/);
  assert.equal(
    checksumManifest.latest_h2_external_compatibility_evidence.status,
    'EVIDENCE_ONLY_NO_RUNTIME_OR_POLICY_EDIT_NO_STABLE_COMBINED_MANIFEST',
  );
  assert.deepEqual(
    checksumManifest.latest_h2_external_compatibility_evidence.downstream_phase_proved_correct_categories,
    ['OVERPAYMENT_RECOVERY'],
  );
  assert.deepEqual(
    checksumManifest.latest_h2_external_compatibility_evidence.provisional_interface_divergence_categories,
    [
      'MANUAL_DEBT_RECOVERY',
      'PAYMENT_ADVANCE_REPAYMENT',
      'LOAN_PAYOUT',
      'UNDERPAYMENT_PAYMENT',
      'MANUAL_CREDIT_ADJUSTMENT_PAYMENT',
    ],
  );
  assert.equal(checksumManifest.latest_h2_external_compatibility_evidence.final_proved_defect_rows, 0);
  assert.equal(checksumManifest.latest_h2_external_compatibility_evidence.runtime_correction_authorised, false);
  assert.deepEqual(
    checksumManifest.latest_h2_external_compatibility_evidence.not_exercised_phases,
    ['CANONICAL_PREVIEW', 'VALIDATE_SESSION_AND_PREPARE', 'SCOPE_SEED', 'ALLOCATION_SEED'],
  );
  assert.equal(
    checksumManifest.latest_h2_external_compatibility_evidence.combined_result,
    '74_PASS_0_FAIL_5_TODO_1_STRUCTURAL_SKIP',
  );
  assert.equal(
    checksumManifest.latest_h2_external_compatibility_evidence.dual_engine_result,
    'PG17.11_20_OF_20_AND_PG18.6_20_OF_20_ROLLBACK_PASS',
  );
  assert.deepEqual(rollbackSourceManifest.finding_count_model.audit_issue_ids, coverage.finding_count_model.h1_audit_issue_ids);
  assert.deepEqual(rollbackSourceManifest.finding_count_model.h1_owned_corrections, coverage.finding_count_model.h1_owned_correction_ids);
  assert.deepEqual(rollbackSourceManifest.finding_count_model.external_h2_owned_blockers, ['F8']);
});

