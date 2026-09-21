import { createHash } from 'node:crypto';
import { lstat, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { canonicalDigest, cloneJson, deepFreeze } from './canonical-json.mjs';
import { parseProofRSeries } from './proof-control-sets.mjs';
import { assertSpiLedgerMatchesExecutionMap, verifySpiExecutionEvidence } from './spi-execution-map.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_MANIFEST_PATH = path.join(here, 'controlling-ledger-manifest.json');
const REPOSITORY_ROOT = path.resolve(here, '..', '..', '..');
const MANIFEST_SCHEMA_VERSION = 'WEEKLY_SOURCE_CONTROLLING_LEDGER_MANIFEST_V2';

// Plan 6.2 completion is counted over eleven controlling sets, not four.
// Required counts come from `P:\annexes\plan6_2-control-index.csv` (P62-* rows) and from
// `P:\26_PLAN_6_2_IMPLEMENTATION_AND_PROOF_LEDGER.md`. They are held here as well as in the
// manifest so that lowering a manifest row count cannot quietly shrink the gate.
export const WEEKLY_SOURCE_CONTROL_SETS = deepFreeze([
  {
    key: 'acceptance',
    envelopeField: 'acceptanceIds',
    orphanCode: 'COVERAGE_RESULT_ACCEPTANCE_ORPHAN',
    detailSuffix: 'Acceptance',
    source: 'annexes/acceptance-tests.csv',
    idField: 'id',
    requiredCount: 967,
    authority: 'P62-HARNESS; 05_ACCEPTANCE_TEST_MATRIX.md',
  },
  {
    key: 'requirements',
    envelopeField: 'requirementIds',
    orphanCode: 'COVERAGE_RESULT_REQUIREMENT_ORPHAN',
    detailSuffix: 'Requirement',
    source: 'annexes/live-implementation-ledger.csv',
    idField: 'requirement_id',
    requiredCount: 64,
    authority: '22_LIVE_IMPLEMENTATION_LEDGER.md (atomic requirements)',
  },
  {
    key: 'protected',
    envelopeField: 'protectedIds',
    orphanCode: 'COVERAGE_RESULT_PROTECTION_ORPHAN',
    detailSuffix: 'Protected',
    source: 'annexes/protected-functionality-matrix.csv',
    idField: 'Protection ID',
    requiredCount: 29,
    authority: 'P62-PROTECTED (29 named protected areas)',
  },
  {
    key: 'models',
    envelopeField: 'modelIds',
    orphanCode: 'COVERAGE_RESULT_MODEL_ORPHAN',
    detailSuffix: 'Model',
    source: 'annexes/combination-coverage-matrix.csv',
    idField: 'Model case ID',
    requiredCount: 25,
    authority: '23_TESTING_FEASIBILITY_AND_BUILDER_PLAN.md combination models',
  },
  {
    key: 'spi',
    envelopeField: 'spiIds',
    orphanCode: 'COVERAGE_RESULT_SPI_ORPHAN',
    detailSuffix: 'Spi',
    source: 'annexes/source-pay-invoice-scenario-matrix.csv',
    idField: 'scenario_id',
    requiredCount: 106,
    authority: 'P62-SCENARIOS (SPI-001..SPI-106)',
  },
  {
    key: 'iss',
    envelopeField: 'issIds',
    orphanCode: 'COVERAGE_RESULT_ISS_ORPHAN',
    detailSuffix: 'Iss',
    source: 'annexes/invoice-issue-real-route-matrix.csv',
    idField: 'issue_case',
    requiredCount: 14,
    authority: 'P62-ISSUE (ISS-001..ISS-014)',
  },
  {
    key: 'ui',
    envelopeField: 'uiStateIds',
    orphanCode: 'COVERAGE_RESULT_UI_STATE_ORPHAN',
    detailSuffix: 'UiState',
    source: 'annexes/ui-lifecycle-state-matrix.csv',
    idField: 'ui_state',
    requiredCount: 22,
    authority: 'P62-UI (UI-001..UI-022)',
  },
  {
    key: 'fti',
    envelopeField: 'ftiIds',
    orphanCode: 'COVERAGE_RESULT_FTI_ORPHAN',
    detailSuffix: 'Fti',
    source: 'annexes/financial-touchpoint-impact-matrix.csv',
    idField: 'touchpoint_id',
    requiredCount: 60,
    authority: 'P62-FIN-TOUCH (FTI-001..FTI-060)',
  },
  {
    key: 'xsg',
    envelopeField: 'xsgIds',
    orphanCode: 'COVERAGE_RESULT_XSG_ORPHAN',
    detailSuffix: 'Xsg',
    source: 'annexes/cross-system-gap-ledger.csv',
    idField: 'gap_id',
    requiredCount: 41,
    authority: 'P62-GAPS (XSG-001..XSG-041)',
  },
  {
    key: 'h2',
    envelopeField: 'h2Ids',
    orphanCode: 'COVERAGE_RESULT_H2_ORPHAN',
    detailSuffix: 'H2',
    source: 'annexes/handover2-acceptance-contract.csv',
    idField: 'contract_id',
    requiredCount: 41,
    authority: 'P62-H2-MACHINE (H2-001..H2-041)',
  },
  {
    key: 'proofs',
    envelopeField: 'proofIds',
    orphanCode: 'COVERAGE_RESULT_PROOF_ORPHAN',
    detailSuffix: 'Proof',
    source: 'proof/32_PENDING_PUBLICATION_OWNER_SPECIFICATION_20260917.md section 12',
    idField: 'proof_id',
    requiredCount: 44,
    authority: 'proof/32 section 12 (R1-R44); cited by gate 4, H2-040 and XSG-041',
  },
]);

const CONTROL_SET_BY_KEY = new Map(WEEKLY_SOURCE_CONTROL_SETS.map((set) => [set.key, set]));

// Every `coverage_status` spelling Plan 6.2 actually uses, reduced to its enumerated prefix.
// An unknown spelling is a hard failure: silent acceptance is how a stale ledger passes.
const COVERAGE_STATUS_PREFIXES = deepFreeze([
  { prefix: 'COVERED', classification: 'COVERED' },
  { prefix: 'CORRECTED ', classification: 'RE_EXECUTION_REQUIRED' },
  { prefix: 'REQUIRED - NOT YET EXECUTED', classification: 'NOT_YET_EXECUTED' },
]);

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourceCoverageLedgerError';
  error.code = code;
  error.details = deepFreeze(cloneJson(details));
  throw error;
}

export function parseCsv(text, label) {
  const rows = [];
  let row = [];
  let field = '';
  let quoted = false;
  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];
    if (quoted) {
      if (character === '"' && text[index + 1] === '"') {
        field += '"';
        index += 1;
      } else if (character === '"') quoted = false;
      else field += character;
      continue;
    }
    if (character === '"') quoted = true;
    else if (character === ',') {
      row.push(field);
      field = '';
    } else if (character === '\n') {
      row.push(field.replace(/\r$/, ''));
      rows.push(row);
      row = [];
      field = '';
    } else field += character;
  }
  if (quoted) fail('COVERAGE_CSV_INVALID', `${label} contains an unterminated quoted field.`);
  if (field.length || row.length) {
    row.push(field.replace(/\r$/, ''));
    rows.push(row);
  }
  if (rows.length < 2) fail('COVERAGE_CSV_EMPTY', `${label} has no data rows.`);
  const headers = rows.shift();
  if (new Set(headers).size !== headers.length || headers.some((header) => !header)) {
    fail('COVERAGE_CSV_HEADERS_INVALID', `${label} has empty or duplicate headers.`);
  }
  const parsed = rows.filter((values) => values.some((value) => value !== '')).map((values, index) => {
    if (values.length !== headers.length) {
      fail('COVERAGE_CSV_ROW_INVALID', `${label} row ${index + 2} has ${values.length} fields; expected ${headers.length}.`);
    }
    return Object.fromEntries(headers.map((header, fieldIndex) => [header, values[fieldIndex]]));
  });
  // Non-enumerable so the header list never reaches a canonical digest or a cloned envelope.
  Object.defineProperty(parsed, 'headers', { value: Object.freeze([...headers]), enumerable: false });
  return parsed;
}

async function regularFile(filePath, label) {
  const stat = await lstat(filePath).catch(() => null);
  if (!stat?.isFile() || stat.isSymbolicLink()) {
    fail('COVERAGE_LEDGER_FILE_INVALID', `${label} must be a regular, non-symbolic-link file.`);
  }
  return readFile(filePath);
}

function uniqueIndex(rows, field, label) {
  const result = new Map();
  for (const row of rows) {
    const key = row[field];
    if (!key) fail('COVERAGE_LEDGER_ID_MISSING', `${label} contains a row with no ${field}.`);
    if (result.has(key)) fail('COVERAGE_LEDGER_ID_DUPLICATE', `${label} contains duplicate ID ${key}.`);
    result.set(key, row);
  }
  return result;
}

function splitIds(value) {
  return String(value ?? '').split(';').map((item) => item.trim()).filter(Boolean);
}

function requireColumns(rows, fileName, idField, columns) {
  for (const row of rows) {
    for (const column of columns) {
      if (!String(row[column] ?? '').trim()) {
        fail('COVERAGE_CONTROL_ROW_INCOMPLETE', `${fileName} row ${row[idField]} has an empty ${column}.`);
      }
    }
  }
}

function classifyCoverageStatus(status, id) {
  const matched = COVERAGE_STATUS_PREFIXES.find((entry) => String(status ?? '').startsWith(entry.prefix));
  if (!matched) {
    fail('COVERAGE_CASE_STATUS_UNKNOWN', `Generated-case ledger row ${id} carries an unrecognised coverage_status.`, {
      allowedPrefixes: COVERAGE_STATUS_PREFIXES.map((entry) => entry.prefix),
    });
  }
  return matched.classification;
}

async function manifestAt(manifestPath) {
  const bytes = await regularFile(manifestPath, 'Controlling ledger manifest');
  const manifest = JSON.parse(bytes.toString('utf8'));
  if (manifest.schemaVersion !== MANIFEST_SCHEMA_VERSION) {
    fail('COVERAGE_MANIFEST_VERSION_INVALID', 'The controlling ledger manifest version is unsupported.');
  }
  return manifest;
}

function resolvePackRoot(packRoot) {
  if (!packRoot) fail('COVERAGE_PACK_ROOT_REQUIRED', 'The controlling Plan 6.2 pack root is required.');
  const resolved = path.resolve(packRoot);
  const relative = path.relative(REPOSITORY_ROOT, resolved);
  if (relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative))) {
    // The sealed pack is read-only and is never copied into the repository. Reading a
    // repository-local copy would pin the harness to a duplicate nobody reseals.
    fail('COVERAGE_PACK_ROOT_INSIDE_REPOSITORY', 'The sealed pack must be read in place, not from a copy inside the backend worktree.');
  }
  return resolved;
}

async function readPinnedFile(packRoot, relativeName, expected, kind) {
  const segments = relativeName.split('/');
  if (segments.some((segment) => !segment || segment === '.' || segment === '..' || segment.includes('\\'))) {
    fail('COVERAGE_FILE_PATH_INVALID', `Unsafe pinned ${kind} name ${relativeName}.`);
  }
  const filePath = path.join(packRoot, ...segments);
  const expectedDirectory = path.join(packRoot, ...segments.slice(0, -1));
  if (path.dirname(filePath) !== expectedDirectory) fail('COVERAGE_FILE_PATH_INVALID', `Unsafe pinned ${kind} name ${relativeName}.`);
  const bytes = await regularFile(filePath, relativeName);
  const digest = createHash('sha256').update(bytes).digest('hex');
  if (digest !== expected.sha256) {
    fail('COVERAGE_LEDGER_HASH_MISMATCH', `${relativeName} does not match the controlling Plan 6.2 ledger.`, {
      expected: expected.sha256,
      actual: digest,
    });
  }
  return bytes;
}

function requireDeclaredCount(setKey, actual, source) {
  const set = CONTROL_SET_BY_KEY.get(setKey);
  if (!set) return;
  if (actual !== set.requiredCount) {
    fail('COVERAGE_CONTROL_SET_COUNT_INVALID', `${source} yields ${actual} ${setKey} rows; the Plan 6.2 control index requires ${set.requiredCount}.`, {
      controlSet: setKey,
      expected: set.requiredCount,
      actual,
      authority: set.authority,
    });
  }
}

export async function loadAndVerifyControllingLedgers(packRoot, { manifestPath = DEFAULT_MANIFEST_PATH } = {}) {
  const resolvedPackRoot = resolvePackRoot(packRoot);
  const manifest = await manifestAt(manifestPath);
  const annexRoot = path.resolve(resolvedPackRoot, 'annexes');
  const loaded = {};
  const setRows = {};
  for (const [fileName, expected] of Object.entries(manifest.files)) {
    const filePath = path.join(annexRoot, fileName);
    if (path.dirname(filePath) !== annexRoot) fail('COVERAGE_FILE_PATH_INVALID', `Unsafe ledger filename ${fileName}.`);
    const bytes = await readPinnedFile(resolvedPackRoot, `annexes/${fileName}`, expected, 'annex');
    const rows = parseCsv(bytes.toString('utf8'), fileName);
    if (rows.length !== expected.rows) {
      fail('COVERAGE_LEDGER_ROW_COUNT_MISMATCH', `${fileName} has ${rows.length} rows; expected ${expected.rows}.`);
    }
    loaded[fileName] = rows;
    if (expected.controlSet) {
      requireDeclaredCount(expected.controlSet, rows.length, fileName);
      setRows[expected.controlSet] = rows;
    }
  }

  for (const [relativeName, expected] of Object.entries(manifest.proofFiles ?? {})) {
    const bytes = await readPinnedFile(resolvedPackRoot, relativeName, expected, 'proof');
    if (expected.controlSet !== 'proofs') {
      fail('COVERAGE_PROOF_CONTROL_SET_UNKNOWN', `Pinned proof ${relativeName} names an unsupported control set.`);
    }
    const rows = parseProofRSeries(bytes.toString('utf8'), {
      section: expected.section,
      requiredCount: expected.rows,
    });
    requireDeclaredCount('proofs', rows.length, relativeName);
    setRows.proofs = rows;
    loaded[relativeName] = rows;
  }

  for (const set of WEEKLY_SOURCE_CONTROL_SETS) {
    if (!setRows[set.key]) {
      fail('COVERAGE_CONTROL_SET_UNPINNED', `Control set ${set.key} has no pinned source in the controlling ledger manifest.`, {
        controlSet: set.key,
        expectedSource: set.source,
        requiredCount: set.requiredCount,
      });
    }
  }

  const acceptance = uniqueIndex(setRows.acceptance, 'id', 'acceptance-tests.csv');
  const generated = uniqueIndex(loaded['generated-case-ledger.csv'], 'test_id', 'generated-case-ledger.csv');
  if (acceptance.size !== generated.size) {
    fail('COVERAGE_CASE_LEDGER_CARDINALITY_MISMATCH', 'Acceptance and generated-case ledgers differ in size.');
  }
  const pendingExecution = [];
  const reExecutionRequired = [];
  const controllingRequirements = new Map();
  for (const [id, row] of acceptance) {
    const mapped = generated.get(id);
    if (!mapped) fail('COVERAGE_CASE_LEDGER_MISSING', `Generated-case ledger has no row for ${id}.`);
    const exact = {
      setup: mapped.setup,
      action: mapped.action,
      expected_result: mapped.exact_expected_result,
      mandatory_gate: mapped.mandatory_gate,
    };
    const source = {
      setup: row.setup,
      action: row.action,
      expected_result: row.expected_result,
      mandatory_gate: row.mandatory_gate,
    };
    if (canonicalDigest(exact) !== canonicalDigest(source)) {
      const supersession = manifest.reviewedExpectedResultSupersessions?.[id];
      const onlyExpectedTextChanged = exact.setup === source.setup
        && exact.action === source.action
        && exact.mandatory_gate === source.mandatory_gate
        && source.expected_result === supersession?.acceptanceText
        && exact.expected_result === supersession?.generatedCaseText
        && typeof supersession?.authority === 'string'
        && supersession.authority.length > 0;
      if (!onlyExpectedTextChanged) {
        fail('COVERAGE_CASE_LEDGER_DRIFT', `Generated-case ledger row ${id} differs from acceptance authority.`);
      }
    }
    const controlling = splitIds(mapped.controlling_requirement_ids);
    if (controlling.length === 0) {
      fail('COVERAGE_CASE_MAPPING_INVALID', `Generated-case ledger row ${id} lacks controlling traceability.`);
    }
    for (const requirementId of controlling) {
      if (!controllingRequirements.has(requirementId)) controllingRequirements.set(requirementId, []);
      controllingRequirements.get(requirementId).push(id);
    }
    // Plan 6.2 adds 41 acceptance rows and corrects 3 more, so 44 rows are not `COVERED`.
    // They are carried forward as a named must-execute list rather than refused at load
    // time; `verifyExecutedCoverage` still goes red for every one of them until real
    // executed evidence names it.
    const classification = classifyCoverageStatus(mapped.coverage_status, id);
    if (classification === 'NOT_YET_EXECUTED') pendingExecution.push(id);
    else if (classification === 'RE_EXECUTION_REQUIRED') reExecutionRequired.push(id);
  }
  for (const id of generated.keys()) {
    if (!acceptance.has(id)) fail('COVERAGE_CASE_LEDGER_ORPHAN', `Generated-case ledger contains orphan ${id}.`);
  }

  const requirementRows = setRows.requirements;
  const requirementIndex = uniqueIndex(requirementRows, 'requirement_id', 'live-implementation-ledger.csv');
  const acceptanceWithAtomicRequirement = new Set();
  for (const row of requirementRows) {
    const linkedAcceptance = splitIds(row.acceptance_case_ids);
    if (Number(row.acceptance_case_count) !== linkedAcceptance.length || linkedAcceptance.length === 0) {
      fail('COVERAGE_REQUIREMENT_CASE_COUNT_INVALID', `${row.requirement_id} has an invalid acceptance-case count.`);
    }
    for (const acceptanceId of linkedAcceptance) {
      if (!acceptance.has(acceptanceId)) {
        fail('COVERAGE_REQUIREMENT_ACCEPTANCE_ORPHAN', `${row.requirement_id} names unknown acceptance ${acceptanceId}.`);
      }
      acceptanceWithAtomicRequirement.add(acceptanceId);
    }
  }
  requireDeclaredCount('requirements', requirementIndex.size, 'live-implementation-ledger.csv');

  // Every acceptance row must be reachable from at least one controlling requirement, even
  // where the 64-row atomic ledger does not name it. The rows that no atomic requirement
  // reaches are reported, not hidden: today that is a pack linkage gap, not a harness one.
  const acceptanceWithoutAtomicRequirement = [...acceptance.keys()].filter((id) => !acceptanceWithAtomicRequirement.has(id));

  const protectedRows = setRows.protected;
  const protectedIndex = uniqueIndex(protectedRows, 'Protection ID', 'protected-functionality-matrix.csv');
  for (const row of protectedRows) {
    if (row.Status !== 'Mandatory' || !row['Required differential proof']) {
      fail('COVERAGE_PROTECTION_INVALID', `${row['Protection ID']} is not a mandatory, evidenced protection.`);
    }
  }

  const modelIndex = uniqueIndex(setRows.models, 'Model case ID', 'combination-coverage-matrix.csv');

  // P62-SCENARIOS: "Every row has exactly nine fields and a non-empty real_test_gate
  // (packager-enforced) and an executed result from the real owner."
  const spiRows = setRows.spi;
  if (spiRows.headers?.length !== 9) {
    fail('COVERAGE_SCENARIO_MATRIX_SHAPE_INVALID', `source-pay-invoice-scenario-matrix.csv has ${spiRows.headers?.length ?? 0} fields; the control index requires exactly nine.`);
  }
  requireColumns(spiRows, 'source-pay-invoice-scenario-matrix.csv', 'scenario_id', ['real_test_gate']);
  assertSpiLedgerMatchesExecutionMap(spiRows);
  requireColumns(setRows.iss, 'invoice-issue-real-route-matrix.csv', 'issue_case', ['real_entry_point', 'expected_result']);
  requireColumns(setRows.ui, 'ui-lifecycle-state-matrix.csv', 'ui_state', ['server_phase']);
  requireColumns(setRows.fti, 'financial-touchpoint-impact-matrix.csv', 'touchpoint_id', ['ownership_boundary', 'exact_owner_file']);
  requireColumns(setRows.xsg, 'cross-system-gap-ledger.csv', 'gap_id', ['mandatory_proof', 'status']);
  requireColumns(setRows.h2, 'handover2-acceptance-contract.csv', 'contract_id', ['exact_rule', 'release_evidence']);

  const workItemIndex = uniqueIndex(loaded['testing-harness-work-items.csv'], 'work_item', 'testing-harness-work-items.csv');
  const requiredWorkItems = loaded['testing-harness-work-items.csv'].length;
  for (let number = 1; number <= requiredWorkItems; number += 1) {
    const id = `TH-${String(number).padStart(3, '0')}`;
    if (!workItemIndex.has(id)) fail('COVERAGE_WORK_ITEM_MISSING', `Harness work item ${id} is absent.`);
  }

  const sets = {};
  for (const set of WEEKLY_SOURCE_CONTROL_SETS) sets[set.key] = setRows[set.key];

  return deepFreeze({
    manifest,
    packVersion: manifest.packVersion,
    packRoot: resolvedPackRoot,
    acceptance: setRows.acceptance,
    generated: [...generated.values()],
    requirements: requirementRows,
    protected: protectedRows,
    models: [...modelIndex.values()],
    spi: setRows.spi,
    iss: setRows.iss,
    ui: setRows.ui,
    fti: setRows.fti,
    xsg: setRows.xsg,
    h2: setRows.h2,
    proofs: setRows.proofs,
    workItems: [...workItemIndex.values()],
    protectedIdCount: protectedIndex.size,
    controllingRequirements: [...controllingRequirements.keys()].sort((left, right) => left.localeCompare(right, 'en')),
    pendingExecution: pendingExecution.sort((left, right) => left.localeCompare(right, 'en')),
    reExecutionRequired: reExecutionRequired.sort((left, right) => left.localeCompare(right, 'en')),
    acceptanceWithoutAtomicRequirement,
    digest: canonicalDigest(loaded),
  });
}

function buildIndexes(ledgers) {
  const indexes = {};
  for (const set of WEEKLY_SOURCE_CONTROL_SETS) {
    const rows = ledgers[set.key];
    if (!Array.isArray(rows)) {
      fail('COVERAGE_LEDGER_SET_MISSING', `Executed coverage needs the ${set.key} control set; it was not supplied.`, {
        controlSet: set.key,
        expectedSource: set.source,
      });
    }
    indexes[set.key] = uniqueIndex(rows, set.idField, `${set.key} ledger`);
  }
  indexes.controllingRequirements = new Set(ledgers.controllingRequirements ?? []);
  return indexes;
}

function verifyEnvelope(envelope, indexes) {
  if (envelope?.schemaVersion !== 'WEEKLY_SOURCE_TEST_RESULT_V1' || envelope.status !== 'PASS') {
    fail('COVERAGE_RESULT_NOT_PASSING', 'Coverage accepts only passing Weekly Source result envelopes.');
  }
  if (envelope.cleanup?.complete !== true) {
    fail('COVERAGE_RESULT_CLEANUP_MISSING', `Result ${envelope.scenarioId} has no complete cleanup proof.`);
  }
  if (envelope.c1?.emulator === true || envelope.c1?.releaseEvidenceEligible === false) {
    fail(
      'COVERAGE_C1_EMULATOR_NOT_RELEASE_EVIDENCE',
      `Result ${envelope.scenarioId} uses the C1 emulator and cannot satisfy complete release coverage.`,
    );
  }
  if (canonicalDigest(Object.fromEntries(Object.entries(envelope).filter(([key]) => key !== 'evidenceDigest'))) !== envelope.evidenceDigest) {
    fail('COVERAGE_RESULT_DIGEST_INVALID', `Result ${envelope.scenarioId} has an invalid evidence digest.`);
  }
  for (const set of WEEKLY_SOURCE_CONTROL_SETS) {
    for (const id of envelope[set.envelopeField] ?? []) {
      if (!indexes[set.key].has(id)) {
        fail(set.orphanCode, `${envelope.scenarioId} names unknown ${set.key} id ${id}.`, {
          controlSet: set.key,
          id,
          authority: set.authority,
        });
      }
    }
  }
  // `controllingRequirementIds` is the generated-case ledger's own requirement namespace
  // (SRC-*, QRY-*, ROT-001, UNA-001, ...). It is validated but not gated: the atomic
  // 64-row ledger remains the gated requirement set.
  for (const id of envelope.controllingRequirementIds ?? []) {
    if (!indexes.controllingRequirements.has(id)) {
      fail('COVERAGE_RESULT_CONTROLLING_REQUIREMENT_ORPHAN', `${envelope.scenarioId} names unknown controlling requirement ${id}.`);
    }
  }
}

export function verifyExecutedCoverage(ledgers, resultEnvelopes, { requireComplete = true } = {}) {
  if (!Array.isArray(resultEnvelopes)) throw new TypeError('resultEnvelopes must be an array');
  const indexes = buildIndexes(ledgers);
  const covered = {};
  for (const set of WEEKLY_SOURCE_CONTROL_SETS) covered[set.key] = new Set();
  const coveredControllingRequirements = new Set();
  const evidenceDigests = new Set();
  for (const envelope of resultEnvelopes) {
    verifyEnvelope(envelope, indexes);
    if (evidenceDigests.has(envelope.evidenceDigest)) {
      fail('COVERAGE_RESULT_DUPLICATE', `Evidence ${envelope.evidenceDigest} is duplicated.`);
    }
    evidenceDigests.add(envelope.evidenceDigest);
    for (const set of WEEKLY_SOURCE_CONTROL_SETS) {
      (envelope[set.envelopeField] ?? []).forEach((id) => covered[set.key].add(id));
    }
    (envelope.controllingRequirementIds ?? []).forEach((id) => coveredControllingRequirements.add(id));
  }

  const missing = {};
  for (const set of WEEKLY_SOURCE_CONTROL_SETS) {
    missing[set.key] = [...indexes[set.key].keys()].filter((id) => !covered[set.key].has(id));
  }
  // The 44 Plan 6.2 rows that are not `COVERED` in the generated-case ledger are surfaced
  // separately so a release record cannot describe them as "carried forward" while unproved.
  missing.pendingExecution = [...(ledgers.pendingExecution ?? []), ...(ledgers.reExecutionRequired ?? [])]
    .filter((id) => !covered.acceptance.has(id))
    .sort((left, right) => left.localeCompare(right, 'en'));

  const missingTotal = WEEKLY_SOURCE_CONTROL_SETS.reduce((total, set) => total + missing[set.key].length, 0);
  const complete = missingTotal === 0 && missing.pendingExecution.length === 0;
  const spiExecution = ledgers.spi.length === 106
    ? verifySpiExecutionEvidence(ledgers.spi, resultEnvelopes, { allowExternalHandover2: !requireComplete })
    : null;
  if (requireComplete && !complete) {
    const details = {
      missingTotal,
      missingPendingExecutionCount: missing.pendingExecution.length,
      firstMissingPendingExecution: missing.pendingExecution[0] ?? null,
    };
    // `detailSuffix` keeps the Plan 6 key spellings (missingAcceptanceCount,
    // firstMissingRequirement, ...) so an existing evidence reader keeps working, and gives
    // each new control set its own named first-missing id.
    for (const set of WEEKLY_SOURCE_CONTROL_SETS) {
      details[`missing${set.detailSuffix}Count`] = missing[set.key].length;
      details[`firstMissing${set.detailSuffix}`] = missing[set.key][0] ?? null;
    }
    fail('COVERAGE_EXECUTION_INCOMPLETE', 'Executed Weekly Source evidence does not cover every controlling row.', details);
  }

  const coveredCounts = {};
  for (const set of WEEKLY_SOURCE_CONTROL_SETS) coveredCounts[set.key] = covered[set.key].size;

  return deepFreeze({
    complete,
    resultCount: resultEnvelopes.length,
    controlSetCount: WEEKLY_SOURCE_CONTROL_SETS.length,
    coveredCounts,
    coveredAcceptance: covered.acceptance.size,
    coveredRequirements: covered.requirements.size,
    coveredProtected: covered.protected.size,
    coveredModels: covered.models.size,
    coveredControllingRequirements: coveredControllingRequirements.size,
    missing,
    evidenceDigest: canonicalDigest([...evidenceDigests].sort()),
    spiExecution,
  });
}

export async function readResultEnvelopes(filePaths) {
  if (!Array.isArray(filePaths) || filePaths.length === 0) {
    fail('COVERAGE_RESULT_FILES_REQUIRED', 'At least one executed result envelope is required.');
  }
  const results = [];
  for (const filePath of [...filePaths].sort((left, right) => left.localeCompare(right, 'en'))) {
    const bytes = await regularFile(path.resolve(filePath), path.basename(filePath));
    results.push(JSON.parse(bytes.toString('utf8')));
  }
  return results;
}

export { DEFAULT_MANIFEST_PATH, MANIFEST_SCHEMA_VERSION, COVERAGE_STATUS_PREFIXES };
