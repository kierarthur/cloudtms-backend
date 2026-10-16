import assert from 'node:assert/strict';
import test from 'node:test';
import { decodeSourceProtectedCurrentPlacement } from '../../broker/src/weekly-source/source-native-contract.mjs';

const id = n => `92000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const original = () => ({ version: 'PROTECTED_CURRENT_PLACEMENT_V1', moved: false,
  family_id: id(1), family_root_id: id(2), member_key: id(2), original_root_timesheet_id: id(2),
  original_contract_id: id(3), current_timesheet_id: id(2), current_contract_id: id(3),
  current_financial_id: null, current_timesheet_version: 1, current_approval_work_id: null,
  original_canonical_work_id: null, move_operation_id: null, placement_revision: '0',
  immutable_original_evidence_sha256: 'a'.repeat(64), source_invoice_certificate_sha256: 'b'.repeat(64),
  placement_certificate_sha256: 'c'.repeat(64) });
const moved = () => ({ ...original(), moved: true, current_timesheet_id: id(4), current_contract_id: id(5),
  current_financial_id: id(6), current_timesheet_version: 2, move_operation_id: id(7), placement_revision: '9007199254740993',
  current_approval_work_id: id(8), original_canonical_work_id: id(9) });

test('Genuine initial pending placement preserves null financial/Work facts and canonical zero revision', () => {
  const value = original(); assert.equal(decodeSourceProtectedCurrentPlacement(value), value);
  const approved = { ...value, current_approval_work_id: id(8), original_canonical_work_id: id(8) };
  assert.equal(decodeSourceProtectedCurrentPlacement(approved), approved);
});
test('Accepted moved placement keeps original A/Contract A distinct from B and lossless revision', () => {
  const value = moved(); assert.equal(decodeSourceProtectedCurrentPlacement(value), value);
  // A later accepted move back to Contract A still has a distinct physical B.
  assert.equal(decodeSourceProtectedCurrentPlacement({ ...value, current_contract_id: value.original_contract_id }).moved, true);
});
for (const key of Object.keys(original())) test(`Protected placement requires exact field ${key}`, () => {
  const value = moved(); delete value[key]; assert.throws(() => decodeSourceProtectedCurrentPlacement(value));
});
test('Current placement refuses archive bypasses, copied evidence flags and supplied monetary authority', () => {
  for (const key of ['allow_archived', 'current_entitlement', 'receipt_id', 'amount', 'approved']) {
    assert.throws(() => decodeSourceProtectedCurrentPlacement({ ...moved(), [key]: true }));
  }
});
test('Moved evidence needs an actual operation and positive canonical bigint revision', () => {
  for (const revision of ['0', '01', '+1', 1, '9223372036854775808']) {
    assert.throws(() => decodeSourceProtectedCurrentPlacement({ ...moved(), placement_revision: revision }));
  }
  assert.throws(() => decodeSourceProtectedCurrentPlacement({ ...moved(), move_operation_id: null }));
  assert.throws(() => decodeSourceProtectedCurrentPlacement({ ...moved(), current_timesheet_id: id(2) }));
});
test('Unmoved result cannot claim B or a different current approval Work', () => {
  for (const extra of [{ move_operation_id: id(7) }, { placement_revision: '1' }, { current_timesheet_id: id(4) },
    { current_contract_id: id(5) }, { current_approval_work_id: id(8), original_canonical_work_id: id(9) }]) {
    assert.throws(() => decodeSourceProtectedCurrentPlacement({ ...original(), ...extra }));
  }
});
test('Placement does not cross requested family, member, current B or reviewed certificate', () => {
  for (const key of ['family_id', 'family_root_id', 'member_key', 'original_root_timesheet_id', 'current_timesheet_id', 'current_contract_id']) {
    assert.throws(() => decodeSourceProtectedCurrentPlacement(moved(), { [key]: id(20) }));
  }
  assert.throws(() => decodeSourceProtectedCurrentPlacement(moved(), { placement_revision: '2' }));
  assert.throws(() => decodeSourceProtectedCurrentPlacement(moved(), { placement_certificate_sha256: 'd'.repeat(64) }));
});
test('Financial version and all certificate digests remain typed and bounded', () => {
  for (const version of [0, '2', 1.5, 2147483648]) {
    assert.throws(() => decodeSourceProtectedCurrentPlacement({ ...moved(), current_timesheet_version: version }));
  }
  for (const key of ['immutable_original_evidence_sha256', 'source_invoice_certificate_sha256', 'placement_certificate_sha256']) {
    assert.throws(() => decodeSourceProtectedCurrentPlacement({ ...moved(), [key]: null }));
  }
});
