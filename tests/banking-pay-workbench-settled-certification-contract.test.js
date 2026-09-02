import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const contractPath = path.resolve(
  here,
  '../codex_outputs/h1-workbench-recovery-causal-v1/WORKBENCH_SETTLED_CERTIFICATION_V1.contract.json'
);
const contract = JSON.parse(fs.readFileSync(contractPath, 'utf8'));
const shape = contract.required_instance_shape;
const payload = shape.sealed_payload;
const constituent = payload.selected_constituents[0];
const partition = payload.selected_partitions[0];

test('settled certification stays explicitly local and unsealed until installed acceptance', () => {
  assert.equal(contract.artifact_status, 'LOCAL_CANDIDATE_NOT_INSTALLED_NOT_DEPLOYED_NOT_FINAL');
  assert.equal(contract.contract_name, 'WORKBENCH_SETTLED_CERTIFICATION_V1');
  assert.equal(contract.contract_version, 1);
  assert.equal(contract.consumer_owner, 'HANDOVER_2');
  assert.match(contract.consumer_rule, /do not reconstruct Workbench selection/i);
  assert.match(contract.authority_boundary, /only Workbench\/pre-Draft expected facts/i);
  assert.match(contract.authority_boundary, /no Draft-produced allocation row/i);
  assert.match(contract.authority_boundary, /no .* parity verdict/i);
  assert.equal(shape.schema_version, 'WORKBENCH_SETTLED_CERTIFICATION_V1');
  assert.match(shape.certification_id, /^WORKBENCH_SETTLED_CERTIFICATION_V1:/);
  assert.ok(contract.open_gates.includes('populated certificate instance'));
});

test('digest contract reuses the existing broker canonical owner and binds the full payload', () => {
  assert.match(contract.canonicalization.owner, /broker\/src\/index\.js stableStringify plus sha256Hex/);
  assert.equal(contract.canonicalization.arrays, 'order preserved');
  assert.equal(contract.canonicalization.encoding, 'UTF-8');
  assert.equal(contract.canonicalization.hash, 'SHA-256 lowercase hexadecimal');
  assert.equal(contract.canonicalization.overall_preimage, 'stableStringify(sealed_payload)');
  assert.match(contract.canonicalization.certification_id_rule, /<overall_digest_sha256>$/);
});

test('authority and publication identities bind one exact current Workbench generation', () => {
  for (const key of [
    'session_id', 'session_version', 'progress_counter_version', 'progress_state',
    'source_snapshot_run_id', 'session_signature', 'pay_date', 'week_ending_cutoff',
    'filters_digest_sha256', 'scope_change_generation_target',
    'scope_change_generation_applied', 'scope_change_generation_shadow_checked'
  ]) assert.ok(Object.hasOwn(payload.authority, key), `missing authority.${key}`);

  const publication = payload.publication_set.publications[0];
  for (const key of [
    'scope_ordinal', 'candidate_id', 'candidate_state_id', 'candidate_state_status',
    'source_change_seq', 'source_build_run_id', 'source_publication_id',
    'certified_publication_session_version', 'publication_attestation_version',
    'publication_attestation_digest_sha256', 'publication_parity_ok',
    'publication_attested_at_utc'
  ]) assert.ok(Object.hasOwn(publication, key), `missing publication.${key}`);
});

test('completeness binds section universes, exclusions, paging, current jobs and Draft readiness', () => {
  const completeness = payload.completeness;
  for (const section of ['ready', 'action_required', 'blocked']) {
    const universe = completeness.section_universes[section];
    assert.ok(universe);
    assert.ok(Object.hasOwn(universe, 'row_count'));
    assert.ok(Object.hasOwn(universe, 'ordered_stable_identity_digests'));
    assert.ok(Object.hasOwn(universe, 'universe_digest_sha256'));
  }
  for (const exclusion of ['active_draft', 'ineligible', 'snoozed']) {
    const universe = completeness.exclusion_universes[exclusion];
    assert.ok(universe);
    assert.ok(Object.hasOwn(universe, 'row_count'));
    assert.ok(Object.hasOwn(universe, 'ordered_stable_identity_digests'));
    assert.ok(Object.hasOwn(universe, 'universe_digest_sha256'));
  }
  for (const key of [
    'all_selected_rows_loaded', 'server_selected_ids_equal_materialised_selected_ids',
    'ready_action_required_blocked_pairwise_disjoint', 'unloaded_selection_gap_count',
    'queued_current_job_count', 'running_current_job_count', 'unresolved_current_job_count',
    'invalid_current_job_pointer_count', 'historical_terminal_rows_are_not_current_authority',
    'create_draft_gate'
  ]) assert.ok(Object.hasOwn(completeness, key), `missing completeness.${key}`);
});

test('every selected constituent exposes the exact H2 comparison surface', () => {
  for (const key of [
    'constituent_ordinal', 'preview_row_id', 'materialised_preview_row_id',
    'presentation_preview_row_id', 'row_key', 'line_id', 'source_identity',
    'candidate_publication_ordinal', 'candidate_id', 'client_id', 'timesheet_id',
    'resolved_pay_channel', 'resolved_payment_method', 'amount_sign', 'semantic_kind',
    'economic_key', 'canonical_amount_ex_vat', 'source_reservation_amount_ex_vat',
    'prior_payment', 'supersession', 'recovery_headroom', 'expected_pre_draft_facts',
    'component_evidence', 'readiness_class', 'selection_state', 'selected', 'draftable',
    'is_ready_for_draft', 'constituent_digest_sha256'
  ]) assert.ok(Object.hasOwn(constituent, key), `missing constituent.${key}`);

  for (const key of [
    'recovery_headroom_allocation_expectation', 'item_expectation', 'source_reservation_expectation'
  ]) {
    assert.ok(Object.hasOwn(constituent.expected_pre_draft_facts, key), `missing expected_pre_draft_facts.${key}`);
  }
  const expected = constituent.expected_pre_draft_facts;
  assert.match(expected.item_expectation.expected_item_source_identity_digest_sha256, /never a Draft item identity/i);
  assert.match(expected.source_reservation_expectation.ordered_active_source_reservation_ids, /never Draft reservation row ids/i);

  const evidence = constituent.component_evidence;
  for (const prefix of [
    'all_same_economic_key_component',
    'full_signed_pre_signature_match',
    'decisive_signed_evidence'
  ]) {
    assert.ok(Object.hasOwn(evidence, `${prefix}_count`), `missing ${prefix}_count`);
    assert.ok(Object.hasOwn(evidence, `${prefix}_ordered_ids`), `missing ${prefix}_ordered_ids`);
  }
  assert.ok(Object.hasOwn(evidence, 'all_same_economic_key_components_digest_sha256'));
  assert.ok(Object.hasOwn(evidence, 'full_signed_pre_signature_matches_digest_sha256'));
  assert.ok(Object.hasOwn(evidence, 'decisive_signed_evidence_digest_sha256'));
});

test('candidate and pay-channel partitions are independently count, order, total and digest bound', () => {
  assert.ok(Object.hasOwn(payload, 'selected_partition_count'));
  assert.ok(Object.hasOwn(payload, 'selected_partitions_ordering'));
  assert.ok(Object.hasOwn(payload, 'selected_partitions_digest_sha256'));
  assert.ok(Object.hasOwn(payload, 'selected_canonical_amount_ex_vat_total'));
  for (const key of [
    'partition_ordinal', 'candidate_id', 'resolved_pay_channel',
    'ordered_constituent_ordinals', 'ordered_constituent_identity_digests',
    'constituent_count', 'canonical_amount_ex_vat_total', 'partition_digest_sha256'
  ]) assert.ok(Object.hasOwn(partition, key), `missing partition.${key}`);
});

test('certificate fail-closes unless before/after payment-policy parity is exact', () => {
  const parity = payload.payment_policy_parity;
  assert.equal(parity.contract_version, 'WORKBENCH_PAYMENT_POLICY_PARITY_V1');
  assert.ok(Object.hasOwn(parity, 'before_policy_projection_digest_sha256'));
  assert.ok(Object.hasOwn(parity, 'after_policy_projection_digest_sha256'));
  assert.equal(parity.digests_equal, true);
  assert.equal(parity.execution_recovery_delta_only, true);
  assert.equal(parity.forbidden_policy_delta_count, 0);
  assert.equal(parity.no_payment_policy_change, true);
  assert.ok(parity.compared_surfaces.length >= 7);
  assert.match(parity.runtime_fixture, /01092026_1927_banking_pay_workbench_causal_recovery_verification[.]sql$/);
  assert.match(parity.runtime_fixture_result, /exact before\/after policy projection equality PASS/i);
});

test('proof identities and fail-closed validation retain source, install, Worker, frontend and Bible boundaries', () => {
  for (const key of [
    'backend_source_commit', 'backend_candidate_tree', 'generated_contract_sha256',
    'targeted_manifest_sha256', 'database_target', 'database_user_identity',
    'postgres_server_version', 'database_release_id', 'database_release_commit',
    'installed_contract_sha256', 'installed_routine_definition_sha256_by_identity',
    'worker_name', 'worker_source_commit', 'worker_deployment_id',
    'frontend_source_commit', 'frontend_deployment_id', 'frontend_asset_marker',
    'bible_source_commit'
  ]) assert.ok(Object.hasOwn(payload.proof_identities, key), `missing proof_identities.${key}`);

  const validationText = contract.validation_semantics.join('\n');
  assert.match(validationText, /cardinality only after the full signed pre-signature filter/i);
  assert.match(validationText, /Workbench\/pre-Draft allocation, item and source-reservation expectation/i);
  assert.match(validationText, /Draft-produced allocation-row identity/i);
  assert.match(validationText, /does not replace or alter Draft economics/i);
  assert.match(validationText, /installed database, Worker, frontend, generated contract and Bible identities/i);
  assert.match(validationText, /payment_policy_parity is present/i);
  assert.match(validationText, /forbidden_policy_delta_count is zero/i);
});

