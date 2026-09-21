import assert from 'node:assert/strict';
import test from 'node:test';
import {
  assertNotWildcardAuthority,
  compareDifferentialCaptures,
  createDifferentialCapture,
  summariseDifferentialCoverage,
  WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT,
  WEEKLY_SOURCE_DIFFERENTIAL_SURFACES,
} from './differential-protection-contract.mjs';

const COMMIT = 'a'.repeat(40);

function capture(phase, surfaces, protectionId = 'PROT-ROTATION-001') {
  return createDifferentialCapture({
    protectionId,
    phase,
    surfaces,
    capturedAtUtc: '2026-09-17T09:00:00Z',
    repositoryCommit: COMMIT,
  });
}

test('TH-020 the database differential is populated but does not claim complete eight-surface protection', () => {
  assert.equal(WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.status, 'POPULATED_DATABASE_LIMB');
  assert.equal(WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.populated, true);
  assert.equal(WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.baselinesCaptured, 28);
  assert.equal(WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.completeProtectedAreas, 0);
  assert.equal(WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.declaredExternalSurfaceCount, 1);
  assert.equal(WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.protectedAreaCount, 29);
  assert.equal(WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.requiredExport, 'runWeeklySourceHarnessPhase');
  assert.deepEqual(
    WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.newInPlan62.map((row) => [row.protectionId, row.requiredDifferential]),
    [['PROT-ROTATION-001', 'ROT-010'], ['PROT-UNAUTH-001', 'UNA-012']],
  );
  assert.equal(WEEKLY_SOURCE_DIFFERENTIAL_SURFACES.length, 8);
});

test('TH-020 a capture must name real 23A surfaces, a fixed clock and an exact commit', () => {
  const good = capture('BEFORE', { 'database rows and projections': { rows: 3 } });
  assert.match(good.captureDigest, /^[a-f0-9]{64}$/);
  assert.throws(
    () => createDifferentialCapture({ protectionId: 'PROT-ROTATION-001', phase: 'MIDDLE', surfaces: {}, capturedAtUtc: '2026-09-17T09:00:00Z', repositoryCommit: COMMIT }),
    (error) => error.code === 'DIFFERENTIAL_PHASE_INVALID',
  );
  assert.throws(
    () => capture('BEFORE', {}),
    (error) => error.code === 'DIFFERENTIAL_SURFACES_EMPTY',
  );
  assert.throws(
    () => capture('BEFORE', { 'source feature': { rows: 1 } }),
    (error) => error.code === 'DIFFERENTIAL_SURFACE_UNKNOWN',
  );
  assert.throws(
    () => createDifferentialCapture({ protectionId: 'PROT-ROTATION-001', phase: 'BEFORE', surfaces: { 'report/export fields': {} }, capturedAtUtc: 'today', repositoryCommit: COMMIT }),
    (error) => error.code === 'DIFFERENTIAL_CLOCK_INVALID',
  );
  assert.throws(
    () => createDifferentialCapture({ protectionId: 'PROT-ROTATION-001', phase: 'BEFORE', surfaces: { 'report/export fields': {} }, capturedAtUtc: '2026-09-17T09:00:00Z', repositoryCommit: 'HEAD' }),
    (error) => error.code === 'DIFFERENTIAL_COMMIT_INVALID',
  );
});

test('TH-020 only an independently resolved Requirement ID may authorise a difference, and never a wildcard', () => {
  const before = capture('BEFORE', { 'database rows and projections': { note: 'x' }, 'C1 request shape': { shape: 1 } });
  const afterSame = capture('AFTER', { 'database rows and projections': { note: 'x' }, 'C1 request shape': { shape: 1 } });
  assert.equal(compareDifferentialCaptures(before, afterSame).pass, true);

  const afterChanged = capture('AFTER', { 'database rows and projections': { note: 'y' }, 'C1 request shape': { shape: 1 } });
  const unauthorised = compareDifferentialCaptures(before, afterChanged);
  assert.equal(unauthorised.pass, false);
  assert.deepEqual(unauthorised.adverseSurfaces, ['database rows and projections']);

  const requirementIndex = new Map([['SRC-REQ-001', {}]]);
  const authorised = compareDifferentialCaptures(before, afterChanged, {
    requirementIndex,
    expectedDifferences: [{
      protectionId: 'PROT-ROTATION-001',
      surface: 'database rows and projections',
      requirementId: 'SRC-REQ-001',
      reason: 'Managed-root guard adds one refusal row to the rotation audit.',
    }],
  });
  assert.equal(authorised.pass, true);

  for (const [entry, code] of [
    [{ protectionId: 'PROT-ROTATION-001', surface: 'database rows and projections', requirementId: 'SRC-REQ-001', reason: 'all' }, 'DIFFERENTIAL_AUTHORITY_WILDCARD_REFUSED'],
    [{ protectionId: 'PROT-ROTATION-001', surface: 'database rows and projections', requirementId: 'SRC-REQ-001', reason: 'layout update for the editor' }, 'DIFFERENTIAL_AUTHORITY_WILDCARD_REFUSED'],
    [{ protectionId: 'PROT-ROTATION-001', surface: 'database rows and projections', requirementId: '', reason: 'A precise reason.' }, 'DIFFERENTIAL_EXPECTED_REQUIREMENT_REQUIRED'],
    [{ protectionId: 'PROT-ROTATION-001', surface: 'database rows and projections', requirementId: 'ROT-001', reason: 'A precise reason.' }, 'DIFFERENTIAL_EXPECTED_REQUIREMENT_ORPHAN'],
    [{ protectionId: 'PROT-UNAUTH-001', surface: 'database rows and projections', requirementId: 'SRC-REQ-001', reason: 'A precise reason.' }, 'DIFFERENTIAL_EXPECTED_PROTECTION_MISMATCH'],
  ]) {
    assert.throws(
      () => compareDifferentialCaptures(before, afterChanged, { requirementIndex, expectedDifferences: [entry] }),
      (error) => error.code === code,
      code,
    );
  }
  assert.throws(() => assertNotWildcardAuthority('miscellaneous', 'reason'), (error) => error.code === 'DIFFERENTIAL_AUTHORITY_WILDCARD_REFUSED');
  assert.throws(() => assertNotWildcardAuthority('  ', 'reason'), (error) => error.code === 'DIFFERENTIAL_AUTHORITY_MISSING');
});

test('TH-020 differential coverage goes red for every protected ID without a passing pair', () => {
  const ledgers = { protected: [{ 'Protection ID': 'PROT-ROTATION-001' }, { 'Protection ID': 'PROT-UNAUTH-001' }] };
  const passing = compareDifferentialCaptures(
    capture('BEFORE', { 'report/export fields': { a: 1 } }),
    capture('AFTER', { 'report/export fields': { a: 1 } }),
  );
  assert.throws(
    () => summariseDifferentialCoverage(ledgers, [passing]),
    (error) => error.code === 'DIFFERENTIAL_COVERAGE_INCOMPLETE' && error.details.firstMissing === 'PROT-UNAUTH-001',
  );
  // A one-sided capture proves nothing, so it cannot be offered as coverage.
  assert.throws(
    () => compareDifferentialCaptures(capture('BEFORE', { 'report/export fields': { a: 1 } }), capture('BEFORE', { 'report/export fields': { a: 1 } })),
    (error) => error.code === 'DIFFERENTIAL_PAIR_INVALID',
  );
  assert.throws(
    () => summariseDifferentialCoverage(ledgers, [{ schemaVersion: 'WRONG' }]),
    (error) => error.code === 'DIFFERENTIAL_RESULT_INVALID',
  );
  const second = compareDifferentialCaptures(
    capture('BEFORE', { 'report/export fields': { a: 1 } }, 'PROT-UNAUTH-001'),
    capture('AFTER', { 'report/export fields': { a: 1 } }, 'PROT-UNAUTH-001'),
  );
  const complete = summariseDifferentialCoverage(ledgers, [passing, second]);
  assert.equal(complete.complete, true);
  assert.equal(complete.passingCount, 2);
});
