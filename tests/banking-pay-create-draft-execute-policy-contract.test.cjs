const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const os = require('node:os');
const { execFileSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
const outDir = path.join(root, 'codex_outputs', 'banking-pay-create-draft-policy-v1');
const contractPath = path.join(outDir, 'BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.json');
const schemaPath = path.join(outDir, 'BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.schema.json');
const humanPath = path.join(outDir, 'BANKING_PAY_HOW_EVERY_PAYMENT_TYPE_WORKS.md');
const codexPath = path.join(outDir, 'CODEX_ZERO_DRIFT_IMPLEMENTATION_SPEC.md');
const lifecyclePath = path.join(outDir, 'BANKING_PAY_CREATE_DRAFT_TO_EXECUTE_PAYMENT_LIFECYCLE_MAP_V1.md');
const checksumPath = path.join(outDir, 'CHECKSUMS.json');
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
const read = file => fs.readFileSync(file);
const json = file => JSON.parse(fs.readFileSync(file, 'utf8'));
const contract = json(contractPath);

test('generated artifact checksums are complete and exact', () => {
  const checksums = json(checksumPath);
  assert.equal(checksums.contract, 'BANKING_PAY_POLICY_CONTRACT_ARTIFACT_CHECKSUMS_V1');
  assert.deepEqual(checksums.files.map(row => row.file).sort(), [
    path.basename(contractPath), path.basename(schemaPath), path.basename(humanPath), path.basename(codexPath), path.basename(lifecyclePath)
  ].sort());
  for (const row of checksums.files) {
    const bytes = read(path.join(outDir, row.file));
    assert.equal(bytes.length, row.bytes, row.file);
    assert.equal(sha256(bytes), row.sha256, row.file);
  }
});

test('canonical contract has unique, source-bound families and a complete finite class ledger', () => {
  assert.equal(contract.contract, 'BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1');
  const familyIds = contract.payment_families.map(row => row.family_id);
  assert.equal(new Set(familyIds).size, familyIds.length);
  assert.equal(familyIds.length, contract.coverage_gates.payment_family_count);
  assert.ok(familyIds.includes('paired_timesheet_reversal_replacement'));
  assert.ok(familyIds.includes('paired_timesheet_reversal_only'));
  for (const row of contract.payment_families) {
    assert.ok(row.real_world_event.length > 10, row.family_id);
    assert.ok(row.selection.length > 10, row.family_id);
    assert.ok(row.amount_owner.length > 10, row.family_id);
    assert.ok(row.visible_aliases.length > 0, row.family_id);
    assert.ok(row.frozen_item_types.length > 0, row.family_id);
    assert.ok(row.identity.length > 0, row.family_id);
    assert.ok(row.variants.length > 0, row.family_id);
    assert.ok(row.fail_closed.length > 0, row.family_id);
    assert.ok(row.source_evidence.length > 0, row.family_id);
  }
  const classIds = contract.finite_equivalence_classes.map(row => row.class_id);
  assert.equal(new Set(classIds).size, classIds.length);
  assert.equal(classIds.length, contract.coverage_gates.finite_equivalence_class_count);
  assert.ok(classIds.length >= 80);
  for (const row of contract.finite_equivalence_classes) {
    assert.ok(familyIds.includes(row.family_id), `${row.class_id} has unknown family ${row.family_id}`);
    assert.match(row.proof_rule, /exact selected constituent identity/);
  }
});

test('every cited source path and numeric line range exists at the sealed source head', () => {
  for (const family of contract.payment_families) {
    for (const evidence of family.source_evidence) {
      const fullPath = path.join(root, evidence.path);
      assert.ok(fs.existsSync(fullPath), `${family.family_id}: missing ${evidence.path}`);
      const lineCount = fs.readFileSync(fullPath, 'utf8').split(/\r?\n/).length;
      const match = /^(\d+)(?:-(\d+|end))?$/.exec(evidence.lines);
      assert.ok(match, `${family.family_id}: invalid line range ${evidence.lines}`);
      const first = Number(match[1]);
      const last = match[2] === 'end' ? lineCount : Number(match[2] || first);
      assert.ok(first >= 1 && first <= lineCount, `${family.family_id}: first line out of range`);
      assert.ok(last >= first && last <= lineCount, `${family.family_id}: last line out of range`);
    }
  }
});

test('reviewed source keeps installed Miget reconciliation explicitly open after Banking source changes', () => {
  const reconciliation = contract.authority.source_install_reconciliation;
  assert.equal(reconciliation.installed_commit_is_ancestor_of_reviewed_source, true);
  assert.equal(reconciliation.status, 'OPEN_FRESH_MIGET_AUDIT_REQUIRED');
  assert.ok(reconciliation.banking_or_draft_paths_changed_since_installed_release.length > 0);
  assert.ok(reconciliation.banking_or_draft_paths_changed_since_installed_release.includes('broker/src/index.js'));
  assert.ok(reconciliation.banking_or_draft_paths_changed_since_installed_release.includes('supabase/release/current-contract.json'));
  assert.equal(reconciliation.bounded_live_routine_hash_match_count, 0);
  assert.equal(reconciliation.bounded_live_routine_hash_mismatch_count, null);
  assert.match(reconciliation.proof_rule, /requires a fresh audit/);
  assert.equal(contract.authority.bounded_current_test_occurrence_evidence.correction_pair_shape_count_rows, 0);
  assert.match(contract.authority.bounded_current_test_occurrence_evidence.interpretation, /not evidence that .* can be omitted/);
});

test('HANDOVER 1 is bound as a data-free pre-Draft contract, never a fabricated certificate instance', () => {
  const upstream = contract.upstream_workbench_certificate_contract;
  assert.equal(upstream.contract_name, 'WORKBENCH_SETTLED_CERTIFICATION_V1');
  assert.equal(upstream.artifact_sha256, '569eac8f790478453ea47e75d84f3c5f0553ec36894ddb7fcde0570db85a16a8');
  assert.equal(upstream.artifact_bytes, 20449);
  assert.match(upstream.artifact_status, /NOT_INSTALLED_NOT_DEPLOYED_NOT_FINAL/);
  assert.match(upstream.consumer_rule, /do not reconstruct Workbench selection/);
  assert.ok(upstream.h1_owned_input_facts.some(value => value.includes('ordered selected constituent')));
  assert.ok(upstream.prohibited_h1_outputs.includes('Draft parity verdict'));
  assert.match(upstream.activation_gate, /No populated sealed certificate instance/);
});

test('current canonical source binds every visible finance alias to its deliberate frozen vocabulary', () => {
  const preview = fs.readFileSync(path.join(root, 'supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql'), 'utf8');
  const finance = fs.readFileSync(path.join(root, 'supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql'), 'utf8');
  const expected = new Map([
    ['LOAN_PAYOUT', 'LOAN_PAYOUT'],
    ['PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT'],
    ['OVERPAYMENT_RECOVERY', 'OVERPAYMENT_RECOVERY'],
    ['UNDERPAYMENT_PAYMENT', 'UNDERPAYMENT_PAYMENT'],
    ['MANUAL_CREDIT_ADJUSTMENT_PAYMENT', 'MANUAL_CREDIT_PAYOUT'],
    ['MANUAL_DEBT_RECOVERY', 'MANUAL_DEBT_RECOVERY']
  ]);
  const rows = contract.payment_families.filter(row => row.source_evidence.some(ev => ev.symbol === 'finance_case_lines'));
  assert.equal(rows.length, 6);
  for (const row of rows) {
    const visible = row.visible_aliases[0];
    const frozen = row.frozen_item_types[0];
    assert.equal(frozen, expected.get(visible), visible);
    assert.ok(preview.includes(`'${visible}'`), visible);
    assert.ok(finance.includes(`'${frozen}'`), frozen);
  }
  assert.equal(contract.known_execution_findings_separate_from_policy.find(row => row.finding.includes('F-013b')).status.startsWith('NOT A DEFECT'), true);
});

test('source vocabulary census has no unmapped visible payment type or unsupported policy token', () => {
  const registry = contract.source_vocabulary_registry;
  const corpus = [
    'supabase/repeatable/05082026_1545_pay_preview_candidate_build_canonical_lines.sql',
    'supabase/repeatable/21072026_1235_05_timesheet_correction_chain_scope_v1.sql',
    'supabase/repeatable/21072026_1235_08_timesheet_correction_pair_transition_v1.sql',
    'supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql',
    'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'
  ].map(file => fs.readFileSync(path.join(root, file), 'utf8')).join('\n');
  for (const [group, values] of Object.entries(registry)) {
    if (!Array.isArray(values)) continue;
    for (const token of values) assert.ok(corpus.includes(token), `${group}: ${token}`);
  }
  const visibleMapped = new Set(contract.payment_families.flatMap(row => row.visible_aliases));
  for (const token of registry.visible_payment_line_types) assert.ok(visibleMapped.has(token), token);
  const frozenMapped = new Set(contract.payment_families.flatMap(row => row.frozen_item_types));
  for (const token of registry.deliberately_hidden_or_frozen_aliases) assert.ok(frozenMapped.has(token), token);
});

test('PAYE gross/net rules and finance variants preserve distinct policy', () => {
  assert.deepEqual(contract.gross_net_policy.map(row => row.rule_id), [
    'PAYE_GROSS_ADD','PAYE_GROSS_DEDUCT','PAYE_NET_ADD','PAYE_NET_DEDUCT','UMBRELLA_NONE'
  ]);
  const byId = Object.fromEntries(contract.payment_families.map(row => [row.family_id, row]));
  assert.match(byId.payment_advance_payout.variants.find(v => v.channel === 'PAYE').paye_treatment, /NET_ADD/);
  assert.match(byId.payment_advance_repayment.variants.find(v => v.channel === 'PAYE').paye_treatment, /NET_DEDUCT/);
  assert.match(byId.overpayment_recovery.variants.find(v => v.channel === 'PAYE').paye_treatment, /TAXABLE.*GROSS_DEDUCT.*NON_TAXABLE.*NET_DEDUCT/);
  assert.match(byId.underpayment_payment.variants.find(v => v.channel === 'PAYE').paye_treatment, /TAXABLE.*GROSS_ADD.*NON_TAXABLE.*NET_ADD/);
  assert.match(byId.manual_credit_adjustment.variants.find(v => v.channel === 'PAYE').paye_treatment, /TAXABLE.*GROSS_ADD.*NON_TAXABLE.*NET_ADD/);
  assert.match(byId.manual_debt_adjustment.variants.find(v => v.channel === 'PAYE').paye_treatment, /TAXABLE.*GROSS_DEDUCT.*NON_TAXABLE.*NET_DEDUCT/);
  for (const id of ['payment_advance_payout','payment_advance_repayment','overpayment_recovery','underpayment_payment','manual_credit_adjustment','manual_debt_adjustment']) {
    assert.match(byId[id].variants.find(v => v.channel === 'UMBRELLA').paye_treatment, /NONE/);
  }
});

test('paired Timesheet contract is exact, atomic and component-preserving', () => {
  const scope = fs.readFileSync(path.join(root, 'supabase/repeatable/21072026_1235_05_timesheet_correction_chain_scope_v1.sql'), 'utf8');
  const transition = fs.readFileSync(path.join(root, 'supabase/repeatable/21072026_1235_08_timesheet_correction_pair_transition_v1.sql'), 'utf8');
  const residual = fs.readFileSync(path.join(root, 'supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql'), 'utf8');
  for (const token of ['REVERSAL_ONLY','REVERSAL_REPLACEMENT','CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT','expected_member_count','chain_fingerprint']) assert.ok(scope.includes(token), token);
  for (const token of ['AUTHORISE','UNAUTHORISE','PROCESS','UNPROCESS','CORRECTION_TRANSITION_CHAIN_STALE']) assert.ok(transition.includes(token), token);
  for (const token of ['CORRECTION_RESIDUAL_CHAIN_IDENTITY_MISMATCH','CORRECTION_RESIDUAL_CANDIDATE_MISMATCH','TS_DAY','truth_ex_vat','baseline_ex_vat','reserved_ex_vat','ordered_member_timesheet_ids','TAXABLE_CHANNEL_SENSITIVE']) assert.ok(residual.includes(token), token);
  const pairClasses = contract.finite_equivalence_classes.filter(row => row.class_id.startsWith('paired_'));
  assert.ok(pairClasses.length >= 10);
  const reversalOnly = contract.payment_families.find(row => row.family_id === 'paired_timesheet_reversal_only');
  assert.equal(reversalOnly.member_deletion_policy.remove_negative_reversal_only.startsWith('Strictly prohibited.'), true);
  assert.match(reversalOnly.member_deletion_policy.remove_positive_replacement_only, /current standard delete does not implement it/i);
  assert.match(reversalOnly.member_deletion_policy.remove_both, /Valid cancellation of the correction/i);
  assert.match(reversalOnly.member_deletion_policy.draft_safety_boundary, /Never make Create Draft accept a REVERSAL_REPLACEMENT unit with a missing member/);
  assert.deepEqual(contract.coverage_gates.required_paired_member_deletion_subcases, [
    'genuine REVERSAL_ONLY remains valid',
    'delete positive replacement only requires explicit upstream reclassification and preserves negative recovery',
    'delete negative reversal only is prohibited',
    'delete the complete pair is valid subject to existing financial retention/archive authority'
  ]);
});

test('all current Create Draft phase owners and Worker RPCs are frozen', () => {
  const worker = fs.readFileSync(path.join(root, 'broker/src/index.js'), 'utf8');
  const phases = contract.create_draft_stage_contract.map(row => row.phase);
  assert.deepEqual(phases, [
    'VALIDATE_SESSION','SYNC_SELECTED_ROWS','WAIT_FOR_PREVIEW_READY','SEED_CANDIDATE_SCOPE','DRAIN_TSFIN','ENSURE_PAYEE_READINESS','SEED_ALLOCATION_ROWS',
    'CREATE_BATCH_SHELLS','INSERT_CANDIDATES','INSERT_ITEMS','APPLY_FINANCE_ADJUSTMENTS','FINALISE_RESERVATIONS',
    'POPULATE_CANDIDATE_SUMMARIES','CREATE_TIMESHEET_SNAPSHOTS','BUILD_ITEM_BREAKDOWNS','ASSERT_INTEGRITY','POST_CREATE_REFRESH'
  ]);
  for (const phase of phases) assert.ok(worker.includes(`'${phase}'`) || worker.includes(`phase === '${phase}'`), phase);
  const transitions = [
    ['VALIDATE_SESSION', 'SYNC_SELECTED_ROWS'],
    ['SYNC_SELECTED_ROWS', 'WAIT_FOR_PREVIEW_READY'],
    ['WAIT_FOR_PREVIEW_READY', 'SEED_CANDIDATE_SCOPE'],
    ['SEED_CANDIDATE_SCOPE', 'DRAIN_TSFIN'],
    ['DRAIN_TSFIN', 'ENSURE_PAYEE_READINESS'],
    ['ENSURE_PAYEE_READINESS', 'SEED_ALLOCATION_ROWS']
  ];
  for (const [from, to] of transitions) {
    const branchStart = worker.indexOf(`phase === '${from}'`);
    assert.ok(branchStart >= 0, from);
    const branchEnd = worker.indexOf("\n    if (phase === '", branchStart + 1);
    const branch = worker.slice(branchStart, branchEnd >= 0 ? branchEnd : undefined);
    assert.ok(branch.includes(`'${to}'`), `${from} must advance to ${to}`);
  }
  for (const rpc of [
    'pay_workbench_prepare_draft','pay_workbench_prepare_draft_scope_seed','pay_workbench_prepare_draft_allocation_rows_seed',
    'pay_batch_insert_candidates_from_preview','pay_batch_insert_items_from_preview','pay_batch_apply_finance_adjustments',
    'pay_batch_finalize_reservations_and_markers','pay_batch_populate_candidate_summaries',
    'pay_batch_create_timesheet_snapshots','pay_batch_build_item_breakdowns','pay_batch_assert_integrity'
  ]) assert.ok(worker.includes(`'${rpc}'`), rpc);
});

test('downstream contract covers every mandated Banking Pay consumer without route-specific semantics', () => {
  const names = contract.downstream_consumers.map(row => row.consumer).join('|');
  for (const fragment of ['Current Payment Status','PAYE Worksheet','Umbrella','Overview','Execute Payment','provider','Bank transfer','settlement','Cancellation','reversion','Frontend']) assert.match(names, new RegExp(fragment, 'i'));
  for (const row of contract.durable_artifact_contracts) {
    assert.match(row.comparison_rule, /full typed V1 and candidate rows/);
    assert.match(row.comparison_rule, /no money\/status\/type\/key\/hash normalization/);
  }
  assert.match(contract.operation_response_contract.rule, /without a V2-specific compatibility branch/);
  assert.equal(contract.downstream_owner_census.length, 14);
  for (const row of contract.downstream_owner_census) {
    assert.ok(row.owner.length > 5, row.boundary);
    assert.ok(row.reads.length > 0, row.boundary);
    assert.ok(row.decision.length > 20, row.boundary);
    if (row.boundary !== 'Frontend interpretation') {
      const hashes = Array.isArray(row.installed_definition_sha256) ? row.installed_definition_sha256 : [row.installed_definition_sha256];
      assert.ok(hashes.length > 0, row.boundary);
      for (const hash of hashes) assert.match(hash, /^[0-9a-f]{64}$/, row.boundary);
    }
  }
});

test('durable relation names are exact current owners, not conceptual aliases', () => {
  const artifacts = contract.durable_artifact_contracts.map((row) => row.artifact);
  const serialized = JSON.stringify(contract);
  const currentContractText = fs.readFileSync(path.join(root, 'supabase/release/current-contract.json'), 'utf8');

  assert.ok(artifacts.includes('banking_pay_operation_candidate_allocation_rows'));
  assert.ok(artifacts.includes('pay_advance_reservations and timesheet_financial_retention'));
  assert.ok(!serialized.includes('pay_batch_item_allocations'));
  assert.ok(!serialized.includes('pay_source_reservations'));
  assert.ok(!serialized.includes('pay_batch_retention_markers'));

  for (const relation of [
    'banking_pay_operation_candidate_allocation_rows',
    'pay_advance_reservations',
    'timesheet_financial_retention',
    'pay_batch_timesheet_snapshots',
    'pay_batch_item_breakdowns'
  ]) {
    assert.match(currentContractText, new RegExp(`"name"\\s*:\\s*"${relation}"`));
  }
});

test('scale and reliability rules prohibit the 100 cap, giant arrays and timeout cheating', () => {
  const invariants = contract.non_negotiable_invariants.join('\n');
  assert.match(invariants, /beyond 100/);
  assert.match(invariants, /No Draft or Execute RPC budget may be increased, removed or bypassed/);
  const classes = new Set(contract.finite_equivalence_classes.map(row => row.class_id));
  for (const id of ['over_100_distinct_timesheets','multi_segment_over_100','pagination_1001','boundary_50000','boundary_50001_reject','duplicate_delivery_replay','response_loss_replay','concurrent_same_selection','atomic_multibatch_failure']) assert.ok(classes.has(id), id);
});

test('human guide is a complete projection with both flowcharts and every family', () => {
  const human = fs.readFileSync(humanPath, 'utf8');
  assert.ok((human.match(/```mermaid/g) || []).length >= 2);
  assert.match(human, /Paired Timesheets — the special flow/);
  assert.match(human, /Deleting only the positive replacement while retaining the negative reversal is a valid user-confirmed business outcome/);
  assert.match(human, /Deleting only the negative reversal is prohibited/);
  assert.match(human, /Deleting the complete correction pair is valid/);
  assert.match(human, new RegExp(`${contract.finite_equivalence_classes.length} finite logical classes`));
  for (const row of contract.payment_families) assert.ok(human.includes(row.title), row.family_id);
});

test('chronological Draft-to-Execute map binds every downstream owner and lifecycle branch without becoming a second policy oracle', () => {
  const lifecycle = fs.readFileSync(lifecyclePath, 'utf8');
  assert.match(lifecycle, /canonical JSON contract remains the sole policy authority/);
  for (const row of contract.downstream_owner_census) assert.ok(lifecycle.includes(row.owner), row.boundary);
  for (const phrase of [
    'PAYE Worksheet is an intentional intermediate step',
    'GROSS_ADD', 'GROSS_DEDUCT', 'NET_ADD', 'NET_DEDUCT',
    'Umbrella preparation stays separate',
    'IMMEDIATE', 'SCHEDULED', 'CSV_SETTLEMENT', 'EXTERNAL_SETTLEMENT',
    'Cancellation before provider payment',
    'whole-Candidate cancellation are different contracts',
    'SCHEDULED_LOCAL_NOT_SENT', 'LOCAL_PREPARED_NOT_SENT',
    'Every unrelated Candidate',
    'Executed-not-paid correction and certified reversion',
    'paired-Timesheet',
    'no route-specific branch',
    'Any real provider, payment, settlement or remittance action'
  ]) assert.ok(lifecycle.includes(phrase), phrase);
  assert.match(lifecycle, /88 finite classes/);
  assert.match(lifecycle, /Implementation parity:\*\* remains a separate executable gate/);
});

test('whole-Candidate cancellation remains scoped across untouched Draft and future-dated unsent execution states', () => {
  const lifecycle = fs.readFileSync(lifecyclePath, 'utf8');
  const selectionPrepare = fs.readFileSync(path.join(root, 'supabase/repeatable/04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql'), 'utf8');
  const selectedItems = fs.readFileSync(path.join(root, 'supabase/repeatable/09082026_1403_pay_payment_correction_selected_items_draft_scope.sql'), 'utf8');
  const processChunk = fs.readFileSync(path.join(root, 'supabase/repeatable/04082026_1209_pay_payment_correction_process_chunk.sql'), 'utf8');
  const helpers = fs.readFileSync(path.join(root, 'supabase/repeatable/09082026_0712_banking_pay_semantic_ready_helpers.sql'), 'utf8');
  const preBank = fs.readFileSync(path.join(root, 'supabase/repeatable/04082026_1158_pay_pre_bank_cancel_apply_work_item.sql'), 'utf8');
  const batchCancel = fs.readFileSync(path.join(root, 'supabase/repeatable/04082026_1206_pay_batch_cancel.sql'), 'utf8');

  assert.match(selectedItems, /v_scope_type\s+NOT IN \('BATCH', 'CANDIDATES', 'TRANSFER', 'UMBRELLA_PAYMENT_GROUP'\)/);
  assert.match(selectedItems, /v_scope_type = 'CANDIDATES'[\s\S]*pay_batch_candidates\.id = ANY\(v_pay_batch_candidate_ids\)/);
  assert.match(selectionPrepare, /'scope_type', 'CANDIDATES'[\s\S]*'work_unit', 'CANDIDATE'/);
  assert.match(selectionPrepare, /WHEN v_requested_action = 'DRAFT_CANCEL'[\s\S]*v_batch\.status = 'DRAFT'/);
  assert.match(selectionPrepare, /WHEN v_requested_action IN \('PRE_BANK_CANCEL', 'CANCEL_PAYMENT'\)/);
  assert.match(processChunk, /private\.pay_pre_bank_cancel_apply_work_page_v1\(/);
  assert.match(helpers, /CREATE OR REPLACE FUNCTION private\.pay_workbench_draft_overlay_remove_page_v1\(/);
  assert.match(helpers, /CREATE OR REPLACE FUNCTION private\.pay_pre_bank_cancel_apply_work_page_v1\(/);
  assert.match(preBank, /v_classification IN \('LOCAL_PREPARED_NOT_SENT', 'SCHEDULED_LOCAL_NOT_SENT'\)/);
  assert.match(preBank, /'selected_candidate_scope_complete'/);
  assert.match(batchCancel, /'scope_type',\s*'BATCH'/);
  assert.doesNotMatch(batchCancel, /'scope_type',\s*'CANDIDATES'/);

  for (const phrase of [
    'whole-batch Draft wrapper only',
    '`DRAFT_CANCEL` overlay path',
    '`SCHEDULED_LOCAL_NOT_SENT`',
    '`PRE_PROVIDER_CANCEL_AND_RECALCULATE`',
    'every unrelated Candidate'
  ]) assert.ok(lifecycle.includes(phrase), phrase);
});

test('current source changes only provenance while the frozen policy remains byte-for-byte equivalent', () => {
  const before = [contractPath, schemaPath, humanPath, codexPath, lifecyclePath, checksumPath].map(file => sha256(read(file)));
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cloudtms-banking-policy-v1-'));
  try {
    execFileSync(process.execPath, [
      path.join(root, 'scripts/generate-banking-pay-policy-contract-v1.mjs'),
      `--output-dir=${tempDir}`
    ], {
      cwd: root,
      env: { ...process.env, BANKING_MODAL_FRONTEND_ROOT: 'C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/TEST-Frontend' },
      stdio: 'pipe'
    });
    for (const file of [
      'BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.schema.json',
      'BANKING_PAY_HOW_EVERY_PAYMENT_TYPE_WORKS.md',
      'CODEX_ZERO_DRIFT_IMPLEMENTATION_SPEC.md',
      'BANKING_PAY_CREATE_DRAFT_TO_EXECUTE_PAYMENT_LIFECYCLE_MAP_V1.md'
    ]) {
      assert.equal(
        sha256(fs.readFileSync(path.join(tempDir, file))),
        sha256(fs.readFileSync(path.join(outDir, file))),
        `${file} policy projection drifted`
      );
    }

    const generatedContract = json(path.join(tempDir, 'BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.json'));
    const stripSourceProvenance = value => {
      const copy = structuredClone(value);
      delete copy.authority.repository.backend_git_commit;
      delete copy.authority.repository.backend_tree;
      delete copy.authority.repository.current_contract_file_sha256;
      delete copy.authority.frontend_bible.git_commit;
      delete copy.authority.frontend_bible.sha256;
      delete copy.authority.source_install_reconciliation.paths_changed_since_installed_release;
      delete copy.authority.source_install_reconciliation.banking_or_draft_paths_changed_since_installed_release;
      return copy;
    };
    assert.deepEqual(
      stripSourceProvenance(generatedContract),
      stripSourceProvenance(contract),
      'a business-policy or owner-source field changed'
    );
    const currentHead = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim();
    assert.equal(generatedContract.authority.repository.backend_git_commit, currentHead);
    if (contract.authority.repository.backend_git_commit !== currentHead) {
      assert.notEqual(generatedContract.authority.repository.backend_git_commit, contract.authority.repository.backend_git_commit);
    }
    const after = [contractPath, schemaPath, humanPath, codexPath, lifecyclePath, checksumPath].map(file => sha256(read(file)));
    assert.deepEqual(after, before);
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});

test('prohibited policy guesses and abandoned finance-owner proposals are absent', () => {
  for (const relative of [
    'supabase/repeatable/01092026_2245_banking_pay_draft_finance_category_handoff_v1.sql',
    'supabase/repeatable/01092026_2250_banking_pay_draft_finance_stage_alias_v1.sql',
    'supabase/verification/01092026_2251_banking_pay_draft_finance_stage_alias_v1.sql'
  ]) assert.equal(fs.existsSync(path.join(root, relative)), false, relative);
  const text = fs.readFileSync(codexPath, 'utf8');
  assert.doesNotMatch(text, /normalise LOAN_REPAYMENT to PAYMENT_ADVANCE_REPAYMENT/i);
  assert.match(JSON.stringify(contract), /LOAN_REPAYMENT is the hidden\/frozen item vocabulary/);
});
