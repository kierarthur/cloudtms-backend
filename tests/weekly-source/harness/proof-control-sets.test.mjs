import assert from 'node:assert/strict';
import test from 'node:test';
import { parseProofRSeries, proofSectionLines } from './proof-control-sets.mjs';

const SAMPLE = [
  '## 11. Forbidden',
  '',
  'Nothing here.',
  '',
  '## 12. Required tests (rollback-contained, PostgreSQL 17.11; all release-blocking)',
  '',
  '| # | Scenario | Required result |',
  '| --- | --- | --- |',
  '| R1 | Pending to batch fully cancelled | released; one receipt |',
  '| R2 | Pending to batch settled | released; 5.2 tuples |',
  '| R3 | Implementation gate (not a database test): writer census | every writer has a binding |',
  '',
  '## 13. Outcome',
  '',
  '| R9 | A row outside section 12 | must never be collected |',
].join('\n');

test('TH-027 the R-series is derived from the pinned proof section, not from a loose transcription', () => {
  const rows = parseProofRSeries(SAMPLE, { section: '12', requiredCount: 3 });
  assert.deepEqual(rows.map((row) => row.proof_id), ['R1', 'R2', 'R3']);
  assert.equal(rows[0].scenario, 'Pending to batch fully cancelled');
  assert.equal(rows[0].required_result, 'released; one receipt');
  assert.equal(rows[0].authority, 'proof/32 section 12');
  // The pack classifies one row as an implementation gate. That is recorded, never excused:
  // it still needs executed evidence like every other id.
  assert.equal(rows[0].implementation_gate, false);
  assert.equal(rows[2].implementation_gate, true);
});

test('TH-027 a proof section stops at the next heading and refuses a wrong count or sequence', () => {
  assert.equal(proofSectionLines(SAMPLE, '12').some((line) => line.includes('outside section 12')), false);
  assert.throws(
    () => parseProofRSeries(SAMPLE, { section: '12', requiredCount: 44 }),
    (error) => error.code === 'PROOF_CONTROL_COUNT_INVALID' && error.details.actual === 3,
  );
  assert.throws(
    () => parseProofRSeries(SAMPLE, { section: '99', requiredCount: 3 }),
    (error) => error.code === 'PROOF_CONTROL_SECTION_MISSING',
  );
  const gapped = SAMPLE.replace('| R2 |', '| R7 |');
  assert.throws(
    () => parseProofRSeries(gapped, { section: '12', requiredCount: 3 }),
    (error) => error.code === 'PROOF_CONTROL_ID_SEQUENCE_INVALID',
  );
  const empty = SAMPLE.replace('| R2 | Pending to batch settled | released; 5.2 tuples |', '| R2 |  | released; 5.2 tuples |');
  assert.throws(
    () => parseProofRSeries(empty, { section: '12', requiredCount: 3 }),
    (error) => error.code === 'PROOF_CONTROL_ROW_INCOMPLETE',
  );
});
