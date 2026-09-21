import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { canonicalDigest } from './canonical-json.mjs';
import {
  DEFAULT_MANIFEST_PATH,
  loadAndVerifyControllingLedgers,
  MANIFEST_SCHEMA_VERSION,
  verifyExecutedCoverage,
  WEEKLY_SOURCE_CONTROL_SETS,
} from './evidence-coverage-ledger.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));

function resultEnvelope(overrides = {}) {
  const body = {
    schemaVersion: 'WEEKLY_SOURCE_TEST_RESULT_V1',
    scenarioId: 'WS-COVERAGE-001',
    status: 'PASS',
    requirementIds: ['REQ-001'],
    acceptanceIds: ['ACC-001'],
    protectedIds: ['PROT-ONE-001'],
    modelIds: ['MODEL-001'],
    spiIds: ['SPI-001'],
    issIds: ['ISS-001'],
    uiStateIds: ['UI-001'],
    ftiIds: ['FTI-001'],
    xsgIds: ['XSG-001'],
    h2Ids: ['H2-001'],
    proofIds: ['R1'],
    controllingRequirementIds: ['SRC-001'],
    c1: { category: 'NONE', emulator: false },
    cleanup: { complete: true },
    ...overrides,
  };
  return { ...body, evidenceDigest: canonicalDigest(body) };
}

// One row per Plan 6.2 control set, so a test ledger has the same eleven-set shape the real
// loader returns. A missing set is a hard failure, never a silently empty gate.
function minimalLedgers(overrides = {}) {
  return {
    acceptance: [{ id: 'ACC-001' }],
    requirements: [{ requirement_id: 'REQ-001' }],
    protected: [{ 'Protection ID': 'PROT-ONE-001' }],
    models: [{ 'Model case ID': 'MODEL-001' }],
    spi: [{ scenario_id: 'SPI-001' }],
    iss: [{ issue_case: 'ISS-001' }],
    ui: [{ ui_state: 'UI-001' }],
    fti: [{ touchpoint_id: 'FTI-001' }],
    xsg: [{ gap_id: 'XSG-001' }],
    h2: [{ contract_id: 'H2-001' }],
    proofs: [{ proof_id: 'R1' }],
    controllingRequirements: ['SRC-001'],
    pendingExecution: [],
    reExecutionRequired: [],
    ...overrides,
  };
}

test('TH-027 validates the real Plan 6.2 controlling ledgers when their sealed pack is supplied', async (t) => {
  const packRoot = process.env.CLOUDTMS_WEEKLY_SOURCE_PACK_ROOT;
  if (!packRoot) return t.skip('CLOUDTMS_WEEKLY_SOURCE_PACK_ROOT is not set');
  const ledgers = await loadAndVerifyControllingLedgers(packRoot);
  assert.equal(ledgers.packVersion, 'PLAN_6_2_2026-09-17');
  assert.equal(ledgers.acceptance.length, 967);
  assert.equal(ledgers.generated.length, 967);
  assert.equal(ledgers.requirements.length, 64);
  assert.equal(ledgers.protected.length, 29);
  assert.equal(ledgers.models.length, 25);
  assert.equal(ledgers.workItems.length, 31);
  assert.equal(ledgers.spi.length, 106);
  assert.equal(ledgers.iss.length, 14);
  assert.equal(ledgers.ui.length, 22);
  assert.equal(ledgers.fti.length, 60);
  assert.equal(ledgers.xsg.length, 41);
  assert.equal(ledgers.h2.length, 41);
  assert.equal(ledgers.proofs.length, 44);
  assert.equal(ledgers.proofs.at(0).proof_id, 'R1');
  assert.equal(ledgers.proofs.at(-1).proof_id, 'R44');
  // 41 added Plan 6.2 rows plus the 3 flagged for re-execution: 44 rows that used to make
  // the loader abort, now carried forward as a named must-execute list.
  assert.equal(ledgers.pendingExecution.length, 41);
  assert.deepEqual(ledgers.reExecutionRequired, ['FIN-C05', 'INV-030', 'PRC-006']);
  for (const id of ['ROT-001', 'ROT-012', 'UNA-001', 'UNA-019', 'PSP-015', 'NAI-MYT-001', 'UI-AUD-031']) {
    assert(ledgers.pendingExecution.includes(id), `${id} must be carried as not yet executed`);
  }
  // The 41 new rows resolve against the generated-case ledger's own requirement namespace.
  for (const id of ['ROT-001', 'UNA-001', 'PAY-PROT-001', 'UI-021', 'POL-05', 'PROT-INVDOC-001']) {
    assert(ledgers.controllingRequirements.includes(id), `${id} must be a resolvable controlling requirement`);
  }
});

test('TH-027 the sealed pack is read in place and never from a repository copy', async () => {
  await assert.rejects(
    () => loadAndVerifyControllingLedgers(path.join(here, '..')),
    (error) => error.code === 'COVERAGE_PACK_ROOT_INSIDE_REPOSITORY',
  );
  await assert.rejects(
    () => loadAndVerifyControllingLedgers(''),
    (error) => error.code === 'COVERAGE_PACK_ROOT_REQUIRED',
  );
});

test('TH-027 executed coverage requires every Plan 6.2 control set, not only the Plan 6 four', () => {
  const ledgers = minimalLedgers();
  const proof = verifyExecutedCoverage(ledgers, [resultEnvelope()]);
  assert.equal(proof.complete, true);
  assert.equal(proof.controlSetCount, 11);
  assert.equal(proof.coveredAcceptance, 1);
  assert.equal(proof.coveredRequirements, 1);
  for (const set of WEEKLY_SOURCE_CONTROL_SETS) assert.equal(proof.coveredCounts[set.key], 1, set.key);

  assert.throws(
    () => verifyExecutedCoverage({ ...ledgers, acceptance: [...ledgers.acceptance, { id: 'ACC-002' }] }, [resultEnvelope()]),
    (error) => error.code === 'COVERAGE_EXECUTION_INCOMPLETE' && error.details.firstMissingAcceptance === 'ACC-002',
  );
  assert.throws(
    () => verifyExecutedCoverage({ ...ledgers, requirements: [...ledgers.requirements, { requirement_id: 'REQ-002' }] }, [resultEnvelope()]),
    (error) => error.code === 'COVERAGE_EXECUTION_INCOMPLETE' && error.details.firstMissingRequirement === 'REQ-002',
  );
  // Every new set goes red on its own, with a named first missing id.
  for (const [key, row, expectedId] of [
    ['spi', { scenario_id: 'SPI-002' }, 'SPI-002'],
    ['iss', { issue_case: 'ISS-002' }, 'ISS-002'],
    ['ui', { ui_state: 'UI-002' }, 'UI-002'],
    ['fti', { touchpoint_id: 'FTI-002' }, 'FTI-002'],
    ['xsg', { gap_id: 'XSG-002' }, 'XSG-002'],
    ['h2', { contract_id: 'H2-002' }, 'H2-002'],
    ['proofs', { proof_id: 'R2' }, 'R2'],
  ]) {
    const { detailSuffix: suffix } = WEEKLY_SOURCE_CONTROL_SETS.find((set) => set.key === key);
    assert.throws(
      () => verifyExecutedCoverage({ ...ledgers, [key]: [...ledgers[key], row] }, [resultEnvelope()]),
      (error) => error.code === 'COVERAGE_EXECUTION_INCOMPLETE' && error.details[`firstMissing${suffix}`] === expectedId,
      `${key} must go red for an unproved id`,
    );
  }
});

test('TH-027 a control set that is not supplied at all fails closed instead of counting as complete', () => {
  for (const set of WEEKLY_SOURCE_CONTROL_SETS) {
    const ledgers = minimalLedgers();
    delete ledgers[set.key];
    assert.throws(
      () => verifyExecutedCoverage(ledgers, [resultEnvelope()]),
      (error) => error.code === 'COVERAGE_LEDGER_SET_MISSING' && error.details.controlSet === set.key,
      `${set.key} must fail closed when absent`,
    );
  }
});

test('TH-027 a row the pack has not marked COVERED stays red until executed evidence names it', () => {
  const ledgers = minimalLedgers({
    acceptance: [{ id: 'ACC-001' }, { id: 'ROT-001' }],
    pendingExecution: ['ROT-001'],
  });
  assert.throws(
    () => verifyExecutedCoverage(ledgers, [resultEnvelope()]),
    (error) => error.code === 'COVERAGE_EXECUTION_INCOMPLETE'
      && error.details.firstMissingPendingExecution === 'ROT-001'
      && error.details.missingPendingExecutionCount === 1,
  );
  const proved = verifyExecutedCoverage(ledgers, [resultEnvelope({ acceptanceIds: ['ACC-001', 'ROT-001'] })]);
  assert.equal(proved.complete, true);
  assert.deepEqual(proved.missing.pendingExecution, []);
});

test('TH-027 rejects orphan mappings, stale evidence digests and missing cleanup', () => {
  const ledgers = minimalLedgers();
  assert.throws(
    () => verifyExecutedCoverage(ledgers, [resultEnvelope({ acceptanceIds: ['ACC-UNKNOWN'] })]),
    (error) => error.code === 'COVERAGE_RESULT_ACCEPTANCE_ORPHAN',
  );
  assert.throws(
    () => verifyExecutedCoverage(ledgers, [resultEnvelope({ requirementIds: ['REQ-UNKNOWN'] })]),
    (error) => error.code === 'COVERAGE_RESULT_REQUIREMENT_ORPHAN',
  );
  assert.throws(
    () => verifyExecutedCoverage(ledgers, [resultEnvelope({ proofIds: ['R99'] })]),
    (error) => error.code === 'COVERAGE_RESULT_PROOF_ORPHAN',
  );
  assert.throws(
    () => verifyExecutedCoverage(ledgers, [resultEnvelope({ spiIds: ['SPI-999'] })]),
    (error) => error.code === 'COVERAGE_RESULT_SPI_ORPHAN',
  );
  // ROT-001 is a controlling requirement, never one of the 64 atomic requirement IDs.
  assert.throws(
    () => verifyExecutedCoverage(ledgers, [resultEnvelope({ requirementIds: ['ROT-001'] })]),
    (error) => error.code === 'COVERAGE_RESULT_REQUIREMENT_ORPHAN',
  );
  assert.throws(
    () => verifyExecutedCoverage(ledgers, [resultEnvelope({ controllingRequirementIds: ['NOT-A-REQUIREMENT'] })]),
    (error) => error.code === 'COVERAGE_RESULT_CONTROLLING_REQUIREMENT_ORPHAN',
  );
  const stale = resultEnvelope();
  stale.evidenceDigest = '0'.repeat(64);
  assert.throws(() => verifyExecutedCoverage(ledgers, [stale]), (error) => error.code === 'COVERAGE_RESULT_DIGEST_INVALID');
  assert.throws(
    () => verifyExecutedCoverage(ledgers, [resultEnvelope({ cleanup: { complete: false } })]),
    (error) => error.code === 'COVERAGE_RESULT_CLEANUP_MISSING',
  );
  assert.throws(
    () => verifyExecutedCoverage(ledgers, [resultEnvelope({ c1: { category: 'COMPLETE_ENTITLEMENT', emulator: true } })]),
    (error) => error.code === 'COVERAGE_C1_EMULATOR_NOT_RELEASE_EVIDENCE',
  );
});

test('TH-027 the control-set table matches the Plan 6.2 control index counts', () => {
  const expected = new Map([
    ['acceptance', 967],
    ['requirements', 64],
    ['protected', 29],
    ['models', 25],
    ['spi', 106],
    ['iss', 14],
    ['ui', 22],
    ['fti', 60],
    ['xsg', 41],
    ['h2', 41],
    ['proofs', 44],
  ]);
  assert.equal(WEEKLY_SOURCE_CONTROL_SETS.length, expected.size);
  for (const set of WEEKLY_SOURCE_CONTROL_SETS) {
    assert.equal(set.requiredCount, expected.get(set.key), `${set.key} required count`);
    assert(
      set.envelopeField && set.idField && set.authority && set.orphanCode && set.detailSuffix,
      `${set.key} must name its envelope field, id field, authority, orphan code and detail suffix`,
    );
  }
  // The Plan 6 error codes and detail keys are preserved, not renamed.
  const legacy = new Map([
    ['acceptance', ['COVERAGE_RESULT_ACCEPTANCE_ORPHAN', 'Acceptance']],
    ['requirements', ['COVERAGE_RESULT_REQUIREMENT_ORPHAN', 'Requirement']],
    ['protected', ['COVERAGE_RESULT_PROTECTION_ORPHAN', 'Protected']],
    ['models', ['COVERAGE_RESULT_MODEL_ORPHAN', 'Model']],
  ]);
  for (const [key, [orphanCode, detailSuffix]] of legacy) {
    const set = WEEKLY_SOURCE_CONTROL_SETS.find((item) => item.key === key);
    assert.equal(set.orphanCode, orphanCode);
    assert.equal(set.detailSuffix, detailSuffix);
  }
});

test('TH-027 the checked-in manifest pins every control set exactly once, at its required count', async () => {
  const manifest = JSON.parse(await readFile(DEFAULT_MANIFEST_PATH, 'utf8'));
  assert.equal(manifest.schemaVersion, MANIFEST_SCHEMA_VERSION);
  assert.equal(manifest.packVersion, 'PLAN_6_2_2026-09-17');
  const pinned = new Map();
  for (const [name, entry] of Object.entries({ ...manifest.files, ...manifest.proofFiles })) {
    assert.match(entry.sha256, /^[a-f0-9]{64}$/, name);
    assert(Number.isInteger(entry.rows) && entry.rows > 0, `${name} must pin a row count`);
    if (!entry.controlSet) continue;
    assert(!pinned.has(entry.controlSet), `${entry.controlSet} is pinned twice`);
    pinned.set(entry.controlSet, entry);
  }
  assert.equal(pinned.size, WEEKLY_SOURCE_CONTROL_SETS.length);
  for (const set of WEEKLY_SOURCE_CONTROL_SETS) {
    const entry = pinned.get(set.key);
    assert(entry, `${set.key} has no pinned source`);
    assert.equal(entry.rows, set.requiredCount, `${set.key} pinned row count`);
  }
  // Re-pinned to Plan 6.2, not left on the Plan 6 digests.
  const stalePlan6 = [
    '66fd6fd2a1a93165aa935292cc38c4522e8259b6b5810b6ee2f4327954cc8554',
    '4dc047c4b4d63baf08beb22912b205cc012d53504b354b5720899f783fae65fd',
    '0979809aa1315e48b5f7f83d1ba03c80033e6a34838b0e2b86f2d83e8b846227',
    '78b194a782f20dc43a016d7316701bf987916c8e3e17aaf7fe775dc7474e57d6',
    '10e7886d2430a1cd1591a27bd4e90d5ff7e8e1346f0e0ab272bb590f7dcc1538',
    '74cb55f68f444cc8143ae244f69a199abac92c1c4873639a631c506869641626',
  ];
  const text = JSON.stringify(manifest);
  for (const digest of stalePlan6) assert(!text.includes(digest), `stale Plan 6 digest ${digest} is still pinned`);
  assert(!text.includes('PLAN_6_2026-09-15'), 'the manifest still names the Plan 6 pack version');
  // The NHSBR-019 supersession is the corrected Plan 6.2 wording, not the Plan 6 text.
  const supersession = manifest.reviewedExpectedResultSupersessions['NHSBR-019'];
  assert.match(supersession.acceptanceText, /^Exact and all four named same-sign one-penny outcomes/);
  assert.match(supersession.generatedCaseText, /uses exact signed source pence/);
  assert(supersession.authority.length > 0);
});
