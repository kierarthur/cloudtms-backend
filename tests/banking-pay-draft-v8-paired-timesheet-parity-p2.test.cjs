const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const json = relativePath => JSON.parse(read(relativePath));
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
const functionDefinition = (sql, functionName) => {
  const escaped = functionName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const startMatch = new RegExp(`create\\s+or\\s+replace\\s+function\\s+public\\.${escaped}\\s*\\(`, 'i').exec(sql);
  assert.ok(startMatch, `missing function ${functionName}`);
  const tail = sql.slice(startMatch.index);
  const next = /\ncreate\s+or\s+replace\s+function\s+/i.exec(tail.slice(startMatch[0].length));
  return next ? tail.slice(0, startMatch[0].length + next.index) : tail;
};

const fixturePath = 'tests/fixtures/banking-pay-draft-v8-paired-timesheet-parity-p2-v1.json';
const runtimePath = 'tests/04092026_1200_banking_pay_draft_v8_paired_timesheet_parity_p2_runtime.sql';
const resultPath = 'codex_outputs/h2-draft-parity/P2_PAIRED_TIMESHEET_PARITY_RESULTS_V1.json';

const expectedClasses = [
  'paired_reversal_replacement_paye',
  'paired_reversal_replacement_umbrella',
  'paired_cross_channel_resolution',
  'paired_broken_or_duplicate_leg',
  'paired_stale_fingerprint',
  'paired_mixed_candidate_or_client',
  'paired_paid_or_invoiced_conflict',
  'paired_transition_replay',
  'paired_draft_response_loss_replay',
  'paired_reversal_only_paye',
  'paired_reversal_only_umbrella'
];

test('P2 freezes all 11 paired-Timesheet classes without inventing V1 parity or a policy change', () => {
  const fixture = json(fixturePath);
  assert.equal(fixture.contract, 'BANKING_PAY_DRAFT_V8_PAIRED_TIMESHEET_PARITY_P2_V1');
  assert.equal(fixture.group_id, 'P2_PAIRED_TIMESHEET_LIFECYCLE');
  assert.equal(fixture.scope.proof_kind, 'CURRENT_V8_POLICY_OWNER_PROOF');
  assert.match(fixture.scope.v1_v8_typed_parity_status, /^OPEN_/);
  assert.equal(fixture.scope.policy_delta_allowed, false);
  assert.equal(
    fixture.scope.production_runtime_edits,
    'ONE_LOCAL_PROVISIONAL_OVERLAP_OWNER_REPLACEMENT_NOT_INSTALLED'
  );
  assert.equal(fixture.scope.provider_payment_settlement_remittance_actions, false);
  assert.deepEqual(fixture.classes.map(row => row.class_id), expectedClasses);
  assert.equal(new Set(fixture.classes.map(row => row.class_id)).size, expectedClasses.length);
  assert.equal(fixture.classes.filter(row => row.assertion_profile === 'REVERSAL_ONLY_RECOVERY_HANDOFF_AND_HEADROOM_EXCLUSION').length, 2);
  assert.equal(fixture.member_deletion_policy.genuine_reversal_only.required_result, 'VALID');
  assert.match(fixture.member_deletion_policy.delete_replacement_only_from_changed_hours_pair.current_result, /^NOT_IMPLEMENTED/);
  assert.equal(fixture.member_deletion_policy.delete_reversal_only_from_changed_hours_pair.required_result, 'PROHIBITED');
  assert.match(fixture.member_deletion_policy.delete_complete_pair.current_result, /^SUPPORTED_AS_ONE_EXACT_TWO_ROW/);
});

test('P2 owner identities and the preserved first-divergence harness are byte-bound', () => {
  const fixture = json(fixturePath);
  const result = json(resultPath);
  for (const owner of fixture.owners) {
    assert.equal(sha256(read(owner.path)), owner.sha256, owner.path);
  }
  assert.equal(
    sha256(read(runtimePath)),
    result.runtime_fixture_sha256
  );
  assert.equal(
    sha256(read(result.local_correction_owner)),
    result.local_correction_owner_sha256
  );
  assert.equal(
    result.pre_change_divergence.historical_first_attempt_runtime_fixture_sha256,
    'a2db7fc2b848bd370060e66a9bb609efbcea55ec79fefca00ce04d0ab45b6bbd'
  );
});

test('P2 records the exact pre-change divergence and narrow local pass without claiming installed or V1 parity', () => {
  const result = json(resultPath);
  const finding = result.pre_change_divergence;
  assert.equal(result.status, 'FIXED_LOCAL_PASS_NOT_INSTALLED_POLICY_PARITY_OPEN');
  assert.equal(finding.classification, 'SOURCE_AND_RUNTIME_PROVED_OVERLAP_OWNER_CONFLICT');
  assert.equal(finding.first_blocked_class_id, 'paired_reversal_replacement_paye');
  assert.deepEqual(finding.runtime_error, {
    sqlstate: 'PT409',
    message: 'TIMESHEET_WORK_INTERVAL_OVERLAP',
    owner: 'private._timesheet_authorisation_overlap_trg_v1',
    owner_source: 'supabase/repeatable/31082026_0317_timesheet_cross_record_overlap_guard_v1.sql',
    introduced_by_commit: '756fb259'
  });
  assert.equal(finding.persistent_write_delta, 0);
  assert.equal(finding.external_action_delta, 0);
  assert.equal(result.local_correction.policy_or_economic_change, false);
  assert.equal(result.local_correction.fail_closed_guards.length, 6);
  assert.ok(result.local_correction.preserved_blocks.includes('ordinary overlapping Timesheets'));
  assert.ok(result.local_correction.preserved_blocks.includes('unrelated overlapping Timesheets'));
  assert.equal(result.engines.pg17.status, 'PASS_ROLLBACK_ONLY');
  assert.equal(result.engines.pg18.status, 'PASS_ROLLBACK_ONLY');
  assert.equal(result.engines.pg17.result_digest_sha256, result.engines.pg18.result_digest_sha256);
  assert.equal(result.counts.classes_runtime_pass, 11);
  assert.equal(result.counts.classes_runtime_fail, 0);
  assert.equal(result.counts.classes_runtime_blocked, 0);
  assert.equal(result.counts.v1_v8_parity_pass, 0);
  assert.deepEqual(result.class_results.map(row => row.class_id), expectedClasses);
  assert.ok(result.class_results.every(row => row.status.startsWith('PASS_')));
  assert.ok(result.blocking_gates.includes('MIGET_TEST_INSTALL_AND_READBACK'));
  assert.equal(result.blocking_gates.includes('REVERSAL_ONLY_NEGATIVE_RESIDUAL_COMPLETE_DRAFT_RECOVERY_PROOF'), false);
  assert.equal(result.reversal_only_recovery_completion.classification, 'DELIBERATE_CROSS_CLASS_EXECUTABLE_POLICY_PROOF');
  assert.deepEqual(result.reversal_only_recovery_completion.covering_p4_classes, [
    'overpayment_zero_headroom', 'overpayment_exact_headroom', 'overpayment_partial_headroom'
  ]);
  assert.equal(result.reversal_only_recovery_completion.policy_or_economic_change, false);
  assert.equal(result.member_deletion_policy_verification.proved_current_outcomes.genuine_reversal_only, 'VALID_AND_RETAINED_AS_RECOVERY_AUTHORITY');
  assert.equal(result.member_deletion_policy_verification.proved_current_outcomes.positive_only, 'INVALID_FAIL_CLOSED');
  assert.equal(result.member_deletion_policy_verification.open_upstream_gap.runtime_correction_status, 'NOT_IMPLEMENTED');
  assert.equal(result.member_deletion_policy_verification.policy_or_economic_change, false);
  assert.ok(result.blocking_gates.includes('UPSTREAM_REPLACEMENT_ONLY_DELETE_TO_REVERSAL_ONLY_TRANSITION_NOT_IMPLEMENTED_OR_PROVED'));
  assert.equal(
    sha256(read(result.reversal_only_recovery_completion.draft_headroom_proof)),
    result.reversal_only_recovery_completion.draft_headroom_proof_sha256
  );
});

test('P2 distinguishes canonical reversal-only, forbidden positive-only and atomic whole-pair deletion without weakening Draft validation', () => {
  const claim = read('supabase/repeatable/21072026_1820_00a_import_apply_operation_claim_v2.sql');
  const chain = read('supabase/repeatable/21072026_1235_05_timesheet_correction_chain_scope_v1.sql');
  const standardDelete = read('supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
  const deletePreview = functionDefinition(standardDelete, 'timesheet_standard_delete_preview_v1');
  const deleteApply = functionDefinition(standardDelete, 'timesheet_standard_delete_apply_v1');
  const deleteOwners = `${deletePreview}\n${deleteApply}`;
  const result = json(resultPath);

  assert.match(claim, /correction_action'\)\)='CANCELLATION'[\s\S]{0,120}?correction_shape'\)\)<>'REVERSAL_ONLY'/i);
  assert.match(claim, /correction_action'\)\)='CHANGED_HOURS'[\s\S]{0,120}?correction_shape'\)\)<>'REVERSAL_REPLACEMENT'/i);
  assert.match(claim, /case when v_shape='REVERSAL_ONLY' then jsonb_build_array\('REVERSAL'\)/i);

  assert.match(chain, /correction_shape' in \('REVERSAL_ONLY','REVERSAL_REPLACEMENT'\)/i);
  assert.match(chain, /u\.reversal_count=1/i);
  assert.match(chain, /u\.replacement_count=case when u\.envelope->>'correction_shape'='REVERSAL_ONLY' then 0 else 1 end/i);

  assert.match(deletePreview, /Import-authoritative changed-hours members are one exact two-row removal[\s\S]{0,1000}?correction_shape'='REVERSAL_REPLACEMENT'[\s\S]{0,300}?array_agg\(value::uuid ORDER BY value::uuid\)/i);
  assert.match(deletePreview, /v_is_correction_pair:=cardinality\(v_timesheet_ids\)=2/i);
  assert.match(deleteApply, /DELETE FROM public\.timesheets AS target_timesheet[\s\S]{0,120}?timesheet_id = ANY\(v_timesheet_ids\)/i);
  assert.doesNotMatch(deleteOwners, /correction_shape[^;]{0,120}?REVERSAL_ONLY/i);

  assert.equal(result.member_deletion_policy_verification.open_upstream_gap.finding,
    'STANDARD_DELETE_HAS_NO_EXPLICIT_REMOVE_REPLACEMENT_AND_RECLASSIFY_SURVIVING_REVERSAL_TRANSITION');
  assert.match(result.member_deletion_policy_verification.open_upstream_gap.forbidden_shortcut,
    /Do not make Create Draft accept a malformed REVERSAL_REPLACEMENT/);
});

test('current source and local replacement preserve the ordinary overlap rule but exempt only an exact certified family', () => {
  const producer = read('supabase/repeatable/21072026_1235_24_hr_weekly_phase3_apply_adjustment_truth_3arg.sql');
  const lifecycle = read('supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql');
  const historicalOverlap = read('supabase/repeatable/31082026_0317_timesheet_cross_record_overlap_guard_v1.sql');
  const replacement = read('supabase/repeatable/04092026_1230_timesheet_correction_family_overlap_compatibility_v1.sql');

  assert.match(producer, /parent_ts\.timesheet_id=v_base_timesheet_id and parent_ts\.is_current and parent_ts\.archived_at_utc is null/i);
  assert.match(producer, /insert into public\.timesheets[\s\S]*?v_booking_id,[\s\S]*?1,[\s\S]*?true,[\s\S]*?'RECEIVED'/i);
  assert.match(producer, /parent_timesheet_id = v_base_timesheet_id/i);
  assert.match(producer, /where t\.correction_id=v_correction_id and t\.is_current and t\.archived_at_utc is null[\s\S]*?CHANGED_HOURS_REVERSAL[\s\S]*?CHANGED_HOURS_REPLACEMENT/i);
  assert.doesNotMatch(producer, /update public\.timesheets\s+(?:as\s+)?parent_ts[\s\S]{0,400}?is_current\s*=\s*false/i);

  assert.match(lifecycle, /parent_ts\.timesheet_id=\(v_unit->>'parent_timesheet_id'\)::uuid[\s\S]*?parent_ts\.is_current and parent_ts\.archived_at_utc is null/i);
  assert.match(lifecycle, /v_result:=public\.timesheet_authorise_bulk_atomic\(v_items,p_actor_user_id/i);

  assert.match(historicalOverlap, /where t\.timesheet_id is distinct from p_exclude_timesheet_id/i);
  assert.match(historicalOverlap, /message='TIMESHEET_WORK_INTERVAL_OVERLAP'/i);
  assert.doesNotMatch(historicalOverlap, /v_exact_correction_member_ids/i);

  assert.equal((replacement.match(/create\s+or\s+replace\s+function/gi) || []).length, 1);
  assert.match(replacement, /_ctms_import_correction_classify_v1\(p_exclude_timesheet_id\)/i);
  assert.match(replacement, /timesheet_correction_chain_scope_v1\(\s*p_exclude_timesheet_id,false,32,100\s*\)/i);
  assert.match(replacement, /v_exact_chain->'requested_correction_unit'->>'valid'/i);
  assert.match(replacement, /not exists \([\s\S]*?correction_units[\s\S]*?->>'valid'/i);
  assert.match(replacement, /v_exact_correction_member_count<>cardinality\(v_exact_correction_member_ids\)/i);
  assert.match(replacement, /p_exclude_timesheet_id<>all\(v_exact_correction_member_ids\)/i);
  assert.match(replacement, /where t\.timesheet_id is distinct from p_exclude_timesheet_id[\s\S]*?not \(t\.timesheet_id=any\(v_exact_correction_member_ids\)\)/i);
  assert.match(replacement, /message='TIMESHEET_WORK_INTERVAL_OVERLAP'/i);
  assert.doesNotMatch(replacement, /pay_batch|provider|settlement|remittance|total_pay_ex_vat|pay_vat_rate/i);
  assert.doesNotMatch(replacement, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('P2 runtime remains rollback-only under existing budgets and cannot invoke external payment owners', () => {
  const sql = read(runtimePath);
  assert.match(sql, /^\\set ON_ERROR_STOP on/m);
  assert.match(sql, /^BEGIN;$/m);
  assert.match(sql, /^SET LOCAL jit\s*=\s*off;$/m);
  assert.match(sql, /^SET LOCAL statement_timeout\s*=\s*'15s';$/m);
  assert.match(sql, /^SET LOCAL lock_timeout\s*=\s*'1500ms';$/m);
  assert.match(sql, /^SET LOCAL idle_in_transaction_session_timeout\s*=\s*'30s';$/m);
  assert.equal((sql.match(/^ROLLBACK;$/gm) || []).length, 1);
  assert.equal((sql.match(/^COMMIT;$/gm) || []).length, 0);
  assert.match(sql, /_ctms_rewrite_sync_correction_cases_v1\s*\(/i);
  assert.match(sql, /desired_case_type'='OVERPAYMENT'/i);
  assert.match(sql, /source_original_paid_amount'\)::numeric=100/i);
  assert.match(sql, /source_corrected_paid_amount'\)::numeric=0/i);
  assert.match(sql, /PASS_CURRENT_V8_RECOVERY_HANDOFF/g);
  assert.doesNotMatch(sql, /\b(?:pay_settle|provider_submit|remittance_generate|bank_transfer_submit)\s*\(/i);
});

test('P2 evidence cannot be mutated into installed/V1 parity or weaken exact-family safeguards', () => {
  const result = json(resultPath);
  const mutations = [
    value => { value.status = 'PASS'; },
    value => { value.v1_v8_typed_parity_status = 'PASS'; },
    value => { value.counts.v1_v8_parity_pass = 11; },
    value => { value.counts.classes_runtime_pass = 10; },
    value => { value.pre_change_divergence.runtime_error.message = 'IGNORED'; },
    value => { value.local_correction.policy_or_economic_change = true; },
    value => { value.local_correction.fail_closed_guards.pop(); },
    value => { value.blocking_gates = []; },
    value => { value.reversal_only_recovery_completion.policy_or_economic_change = true; },
    value => { value.member_deletion_policy_verification.open_upstream_gap.runtime_correction_status = 'FIXED'; }
  ];
  const validate = value => {
    assert.equal(value.status, 'FIXED_LOCAL_PASS_NOT_INSTALLED_POLICY_PARITY_OPEN');
    assert.match(value.v1_v8_typed_parity_status, /^OPEN_/);
    assert.equal(value.counts.v1_v8_parity_pass, 0);
    assert.equal(value.counts.classes_runtime_pass, 11);
    assert.equal(value.pre_change_divergence.runtime_error.message, 'TIMESHEET_WORK_INTERVAL_OVERLAP');
    assert.equal(value.local_correction.policy_or_economic_change, false);
    assert.equal(value.local_correction.fail_closed_guards.length, 6);
    assert.equal(value.reversal_only_recovery_completion.policy_or_economic_change, false);
    assert.equal(value.member_deletion_policy_verification.open_upstream_gap.runtime_correction_status, 'NOT_IMPLEMENTED');
    assert.ok(value.blocking_gates.length > 0);
  };
  validate(result);
  for (const mutate of mutations) {
    const changed = structuredClone(result);
    mutate(changed);
    assert.throws(() => validate(changed));
  }
});
