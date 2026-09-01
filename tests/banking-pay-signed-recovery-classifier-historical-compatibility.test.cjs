const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const repeatablePath = path.join(
  root,
  'supabase',
  'repeatable',
  '01092026_1647_banking_pay_signed_recovery_classifier_v1.sql'
);
const verifierPath = path.join(
  root,
  'supabase',
  'verification',
  '01092026_1648_banking_pay_signed_recovery_classifier_verification.sql'
);
const releasePath = path.join(root, 'supabase', 'release', 'current-release.json');
const sql = fs.readFileSync(repeatablePath, 'utf8');
const verifier = fs.readFileSync(verifierPath, 'utf8');
const release = JSON.parse(fs.readFileSync(releasePath, 'utf8'));

test('the correction replaces only the existing frozen-document classifier', () => {
  const definitions = [...sql.matchAll(
    /CREATE OR REPLACE FUNCTION\s+(?:public|private)\.([a-z0-9_]+)/gi
  )].map((match) => match[1]);
  assert.deepEqual(definitions, [
    'pay_batch_signed_non_charge_recovery_evidence_v1',
  ]);
  assert.doesNotMatch(sql, /\b(?:INSERT|UPDATE|DELETE|TRUNCATE)\b/i);
  assert.doesNotMatch(sql, /\b(?:FROM|JOIN)\s+(?:public|private)\./i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('cardinality applies only after the complete signed non-charge pre-signature', () => {
  const selectStart = sql.indexOf('SELECT COUNT(*)::integer');
  const cardinality = sql.indexOf("'MATCHED_COMPONENT_CARDINALITY'");
  assert.ok(selectStart >= 0, 'candidate selection must be present');
  assert.ok(cardinality > selectStart, 'cardinality guard must follow candidate selection');
  const candidateFilter = sql.slice(selectStart, cardinality);

  assert.match(candidateFilter, /component_fallback[\s\S]*WORKED_TIME_AMOUNT/i);
  assert.match(candidateFilter, /authoritative_truth_ex_vat[\s\S]*ROUND[\s\S]*=\s*0/i);
  assert.match(candidateFilter, /authoritative_baseline_ex_vat[\s\S]*ROUND[\s\S]*<\s*0/i);
  assert.match(sql, /IF v_matching_count = 0 THEN[\s\S]*RETURN NULL/i);
  assert.match(sql, /IF v_matching_count <> 1 THEN[\s\S]*MATCHED_COMPONENT_CARDINALITY/i);
});

test('rollback first use covers ordinary history, one genuine signed return and duplicate signed proof', () => {
  for (const required of [
    'BANKING_PAY_ORDINARY_MULTI_COMPONENT_MISCLASSIFIED',
    'BANKING_PAY_MIXED_SIGNED_RECOVERY_NOT_RECOGNISED',
    'BANKING_PAY_DUPLICATE_SIGNED_RECOVERY_ACCEPTED',
    'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID',
    'ordinary_component_a',
    'ordinary_component_b',
    'valid_component',
    "exception when sqlstate '23514'",
    'rollback;',
  ]) assert.match(verifier, new RegExp(required, 'i'));
});

test('the mandatory verifier runs on NEW and UPGRADE releases', () => {
  const verifierName =
    'supabase/verification/01092026_1648_banking_pay_signed_recovery_classifier_verification.sql';
  assert.ok(release.verificationFiles.includes(verifierName));
  assert.ok(release.newVerificationFiles.includes(verifierName));
});

test('the private helper retains its established owner and closed browser/service ACL', () => {
  assert.match(
    sql,
    /ALTER FUNCTION private\.pay_batch_signed_non_charge_recovery_evidence_v1\(jsonb\)[\s\S]*OWNER TO postgres;/i
  );
  assert.match(
    sql,
    /REVOKE ALL ON FUNCTION private\.pay_batch_signed_non_charge_recovery_evidence_v1\(jsonb\)[\s\S]*FROM PUBLIC, anon, authenticated, service_role;[\s\S]*GRANT EXECUTE ON FUNCTION private\.pay_batch_signed_non_charge_recovery_evidence_v1\(jsonb\)[\s\S]*TO postgres;/i
  );
});
