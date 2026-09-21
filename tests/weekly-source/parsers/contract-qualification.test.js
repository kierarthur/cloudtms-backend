import assert from 'node:assert/strict';
import test from 'node:test';

import { qualifyWeeklySourceContract } from '../../../broker/src/weekly-source/contract-qualification.js';

function contract(id, result = 'EXACT', overrides = {}) {
  return {
    contractId: id,
    candidateId: 'candidate-1',
    clientId: 'client-1',
    validFrom: '2026-01-01',
    validTo: '2026-12-31',
    weeklySourceApplicable: true,
    displayLabel: `Contract ${id}`,
    payType: id === 'b' ? 'UMBRELLA' : 'PAYE',
    priceObservation: { result },
    ...overrides,
  };
}

const base = {
  sourceMode: 'NHSP_WEEKLY',
  candidateId: 'candidate-1',
  clientId: 'client-1',
  workDate: '2026-09-07',
};

test('price disparity cannot eliminate an otherwise valid Contract', () => {
  const result = qualifyWeeklySourceContract({ ...base, contracts: [contract('a'), contract('b', 'MISMATCH')] });
  assert.equal(result.state, 'MULTIPLE_MATCHING_CONTRACTS');
  assert.deepEqual(result.eligibleContractIds, ['a', 'b']);
});

test('several base Contracts require Office choice regardless of price result', () => {
  const result = qualifyWeeklySourceContract({ ...base, contracts: [contract('a'), contract('b', 'SOURCE_ROUNDING_EQUIVALENT'), contract('c', 'MISMATCH')] });
  assert.equal(result.state, 'MULTIPLE_MATCHING_CONTRACTS');
  assert.deepEqual(result.eligibleContractIds, ['a', 'b', 'c']);
  assert.deepEqual(result.choices.map((item) => item.contractId), ['a', 'b', 'c']);
});

// Plan 6.2 `24 §8`: "A Contract whose price cannot be calculated does not block
// another Contract that is independently verified compatible. The row blocks
// only when no safe unique answer exists." Gap row XSG-012, build item G6-15.
// This supersedes the Plan 6 rule that any UNVERIFIABLE base Contract blocked
// cardinality outright.
test('an unavailable price check cannot silently choose another otherwise valid Contract', () => {
  const result = qualifyWeeklySourceContract({ ...base, contracts: [contract('a'), contract('b', 'UNVERIFIABLE')] });
  assert.equal(result.state, 'MULTIPLE_MATCHING_CONTRACTS');
  assert.deepEqual(result.eligibleContractIds, ['a', 'b']);
  assert.deepEqual(result.unverifiableContractIds, []);
});

test('one base Contract is selected even when its price check needs separate correction', () => {
  const result = qualifyWeeklySourceContract({ ...base, contracts: [contract('a', 'UNVERIFIABLE')] });
  assert.equal(result.state, 'RESOLVED');
  assert.equal(result.selectedContractId, 'a');
  assert.deepEqual(result.eligibleContractIds, ['a']);
  assert.deepEqual(result.choices, []);
});

test('one valid base Contract is selected for a zero source charge', () => {
  const result = qualifyWeeklySourceContract({ ...base, contracts: [contract('a', 'ZERO_SOURCE_CHARGE')] });
  assert.equal(result.state, 'RESOLVED');
  assert.equal(result.selectedContractId, 'a');
  assert.equal(result.selectionMethod, 'AUTO_UNIQUE');
});

// `25 §7` Removed: "Rejecting an otherwise unique safe Contract merely because
// band/role text differs." Gap row XSG-011, build item G6-14.
test('mappedRoleCode is no longer a base filter', () => {
  const result = qualifyWeeklySourceContract({
    ...base,
    mappedRoleCode: 'RGN-B6',
    contracts: [contract('a', 'EXACT', { mappedRoleCode: 'RGN-B5' })],
  });
  assert.equal(result.state, 'RESOLVED');
  assert.equal(result.selectedContractId, 'a');
  assert.equal(result.selectionMethod, 'AUTO_UNIQUE');
});

// `25 §7`: "If several remain, use verified band/role only as a tie-breaker."
test('a verified band/role match narrows several eligible Contracts to one', () => {
  const result = qualifyWeeklySourceContract({
    ...base,
    contracts: [
      contract('a', 'EXACT', { verifiedRoleBandMatch: false }),
      contract('b', 'EXACT', { verifiedRoleBandMatch: true }),
    ],
  });
  assert.equal(result.state, 'RESOLVED');
  assert.equal(result.selectedContractId, 'b');
  assert.deepEqual(result.narrowedBy, ['VERIFIED_ROLE_BAND']);
});

// `24 §8`: "none remain after the label filter: ignore the label filter and
// return to the original eligible shortlist."
test('a band/role filter that would leave none is discarded', () => {
  const result = qualifyWeeklySourceContract({
    ...base,
    mappedRoleCode: 'RGN-B9',
    contracts: [
      contract('a', 'EXACT', { mappedRoleCode: 'RGN-B5' }),
      contract('b', 'EXACT', { mappedRoleCode: 'RGN-B6' }),
    ],
  });
  assert.equal(result.state, 'MULTIPLE_MATCHING_CONTRACTS');
  assert.deepEqual(result.eligibleContractIds, ['a', 'b']);
  assert.deepEqual(result.narrowedBy, []);
});

// `25 §7` orders compatible schedule before the band/role tie-breaker.
test('schedule compatibility narrows before band/role', () => {
  const result = qualifyWeeklySourceContract({
    ...base,
    contracts: [
      contract('a', 'EXACT', { scheduleCompatible: false, verifiedRoleBandMatch: true }),
      contract('b', 'EXACT', { scheduleCompatible: true, verifiedRoleBandMatch: false }),
    ],
  });
  assert.equal(result.state, 'RESOLVED');
  assert.equal(result.selectedContractId, 'b');
  assert.deepEqual(result.narrowedBy, ['SCHEDULE_COMPATIBILITY']);
});

// `24 §9` orders an explicit relationship first and Office confirmation last,
// so neither is displaced by an automatic tie-break.
test('an Office selection and prior lineage are honoured against the pre-narrowing set', () => {
  const contracts = [
    contract('a', 'EXACT', { scheduleCompatible: false, verifiedRoleBandMatch: false }),
    contract('b', 'EXACT', { scheduleCompatible: true, verifiedRoleBandMatch: true }),
  ];
  const selected = qualifyWeeklySourceContract({ ...base, contracts, officeSelectedContractId: 'a' });
  assert.equal(selected.selectionMethod, 'OFFICE_SELECTED');
  assert.equal(selected.selectedContractId, 'a');
  const lineage = qualifyWeeklySourceContract({ ...base, contracts, priorAcceptedContractId: 'a' });
  assert.equal(lineage.selectionMethod, 'DURABLE_LINEAGE');
  assert.equal(lineage.selectedContractId, 'a');
});

test('Office and durable-lineage selections must still satisfy the current exact qualification set', () => {
  const contracts = [contract('a'), contract('b')];
  const lineage = qualifyWeeklySourceContract({ ...base, contracts, priorAcceptedContractId: 'b' });
  assert.equal(lineage.selectedContractId, 'b');
  assert.equal(lineage.selectionMethod, 'DURABLE_LINEAGE');
  const selected = qualifyWeeklySourceContract({ ...base, contracts, officeSelectedContractId: 'a' });
  assert.equal(selected.selectionMethod, 'OFFICE_SELECTED');
  assert.throws(() => qualifyWeeklySourceContract({ ...base, contracts, officeSelectedContractId: 'c' }), { code: 'WEEKLY_SOURCE_CONTRACT_SELECTION_STALE' });
});

test('HealthRoster/generic roster ignores source money and uses only exact available criteria', () => {
  const result = qualifyWeeklySourceContract({
    ...base,
    sourceMode: 'HEALTHROSTER_WEEKLY',
    contracts: [
      contract('a', 'MISMATCH'),
      contract('b', 'UNVERIFIABLE'),
      contract('wrong-client', 'EXACT', { clientId: 'client-2' }),
      contract('expired', 'EXACT', { validTo: '2026-09-01' }),
    ],
  });
  assert.equal(result.state, 'MULTIPLE_MATCHING_CONTRACTS');
  assert.deepEqual(result.eligibleContractIds, ['a', 'b']);
});

test('pay type and pay rates never narrow the qualifying set', () => {
  const result = qualifyWeeklySourceContract({
    ...base,
    contracts: [contract('a', 'EXACT', { payType: 'PAYE', payRate: 10 }), contract('b', 'EXACT', { payType: 'UMBRELLA', payRate: 99 })],
  });
  assert.equal(result.state, 'MULTIPLE_MATCHING_CONTRACTS');
  assert.deepEqual(result.eligibleContractIds, ['a', 'b']);
});
