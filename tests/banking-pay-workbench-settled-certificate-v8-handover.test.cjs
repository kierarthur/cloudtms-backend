const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const root = path.resolve(__dirname, '..');
const pack = path.join(root, 'codex_outputs', 'h1-workbench-certificate-v8-implementation');

function readJson(name) {
  return JSON.parse(fs.readFileSync(path.join(pack, name), 'utf8'));
}

function sha256(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

test('V8 handover cannot claim installed or final recovery proof', () => {
  const evidence = readJson('IMPLEMENTATION_EVIDENCE.json');
  const contract = readJson('H2_DEPENDENCY_CONTRACT.json');
  assert.equal(evidence.workbench_recovery_pass, false);
  assert.equal(evidence.installed_baseline.v8_installed, false);
  assert.equal(evidence.policy_change, false);
  assert.equal(evidence.h2_consumer_implemented_by_h1, false);
  assert.equal(contract.ready_for_h2_installed_integration, false);
  assert.equal(contract.required_final_statement, 'HANDOVER 2 DEPENDENCY REMAINS UNAVAILABLE');
  assert.equal(evidence.status, 'LOCAL_IMPLEMENTATION_CANDIDATE_AWAITING_H2_INDEPENDENT_REVIEW_NOT_INSTALLED_NOT_DEPLOYED');
  assert.equal(evidence.source.backend_implementation_commit, 'f6abc37d71879e204ea8dba11900c385c07c2959');
  assert.equal(evidence.source.frontend_implementation_commit, '6ec5f56cf236c43ad1864ae44514d1d634230aae');
});

test('V8 handover preserves 50,000 support while documenting 5,000 pressure evidence', () => {
  const evidence = readJson('IMPLEMENTATION_EVIDENCE.json');
  const contract = readJson('H2_DEPENDENCY_CONTRACT.json');
  assert.equal(evidence.certificate.maximum_selected_constituents, 50000);
  assert.equal(evidence.certificate.practical_pressure_test_rows, 5000);
  assert.equal(evidence.certificate.requested_page_limit, 256);
  assert.equal(evidence.certificate.maximum_build_emission_rows, 64);
  assert.equal(contract.reader_rules.maximum_page_size, 256);
  assert.equal(contract.producer.maximum_requested_page_size, 256);
  assert.equal(contract.producer.maximum_build_emission_rows, 64);
  assert.equal(evidence.authoritative_function_count, 39);
  assert.equal(evidence.worker_postgrest_pressure.target_count, 5000);
  assert.equal(evidence.worker_postgrest_pressure.completed, true);
  assert.equal(evidence.worker_postgrest_pressure.timeout_or_lost_continuation, false);
});

test('V8 handover keeps H1 expected facts separate from H2 Draft outputs', () => {
  const contract = readJson('H2_DEPENDENCY_CONTRACT.json');
  assert.equal(contract.h1_expected_pre_draft_facts_only, true);
  assert.equal(contract.excluded_h2_owned_outputs.length, 6);
  assert.match(contract.retention_policy, /^INDEFINITE_/);
  assert.equal(contract.policy_change, false);
});

test('H1 cross-audit is bound to the exact current H2 sixteen-gate checklist', () => {
  const audit = readJson('H1_CREATE_DRAFT_LIFECYCLE_PRODUCER_AUDIT_V1.json');
  assert.equal(audit.h2_control.lifecycle_contract_sha256, 'b13b5be38eeee54c587d26605c86015b08fbac7a39124b826c3136798a336727');
  assert.equal(audit.h2_control.lifecycle_test_sha256, '210898e32078b1c80dc8095ede6dd0fc0e5c1c3b34b4f805881a3a85672ad87c');
  assert.equal(audit.h2_control.policy_contract_sha256, '3952c019426334a6c04b568226d019fc915f635d2411b7a295229c188beef42c');
  assert.equal(audit.h2_control.payment_families, 15);
  assert.equal(audit.h2_control.finite_equivalence_classes, 88);
  assert.equal(audit.h2_control.draft_stages, 17);
  assert.deepEqual(audit.gates.map(gate => gate.id), Array.from({ length: 16 }, (_, index) => `H2-L${String(index + 1).padStart(2, '0')}`));
  assert.equal(audit.h1_unexamined_gate_count, 0);
  assert.equal(audit.ready_for_h2_installed_integration, false);
  assert.equal(audit.required_statement, 'HANDOVER 2 DEPENDENCY REMAINS UNAVAILABLE');
});

test('checksum manifest matches every sealed pack artifact', () => {
  const manifest = readJson('CHECKSUMS.json');
  assert.equal(manifest.no_edit_in_flight, true);
  for (const item of manifest.files) {
    const file = path.join(pack, item.path);
    assert.equal(fs.statSync(file).size, item.bytes, item.path);
    assert.equal(sha256(file), item.sha256, item.path);
  }
});
