// Populated protected-function differential contract.
//
// `P:\23A_EXECUTABLE_TEST_HARNESS_SPECIFICATION.md` section 12 requires an approved baseline
// for every protected ID, re-run after each implementation slice and compared surface by
// surface. Plan 6.2 raises the protected matrix from 27 to 29 rows by adding
// `PROT-ROTATION-001` (ordinary Timesheet rotation outside the managed scope, differential
// `ROT-010`) and `PROT-UNAUTH-001` (ordinary `Unauthorise`, differential `UNA-012`).
//
// `scripts/run-weekly-source-harness.mjs` already requires
// `CLOUDTMS_WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER` for the `differential` phase and for `all`,
// The database adapter now measures 28 database-owned areas against distinct PostgreSQL
// builds. PROT-APP-001 is deliberately owned by the rendered browser/device proof, because
// no database object can measure MyTMS layout or navigation.

import { canonicalDigest, cloneJson, deepFreeze } from './canonical-json.mjs';

// Verbatim surface list, 23A section 12.
export const WEEKLY_SOURCE_DIFFERENTIAL_SURFACES = deepFreeze([
  'database rows and projections',
  'backend response schema and value',
  'Office command availability and visible layout',
  'Candidate actions, evidence and expense availability',
  'invoice membership/value/document/export',
  'report/export fields',
  'C1 request shape',
  'existing Workbench/Banking-owned output supplied by the separate boundary evidence',
]);

// 23A section 12: "No expected-difference file may use a wildcard such as `all`,
// `miscellaneous`, `layout update` or `source feature`."
export const WEEKLY_SOURCE_DIFFERENTIAL_WILDCARDS = deepFreeze(['all', 'miscellaneous', 'layout update', 'source feature']);

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourceDifferentialContractError';
  error.code = code;
  error.details = deepFreeze(cloneJson(details));
  throw error;
}

export function assertNotWildcardAuthority(text, label) {
  const value = String(text ?? '').trim();
  if (!value) fail('DIFFERENTIAL_AUTHORITY_MISSING', `${label} must state an exact authorised difference.`);
  const normalised = value.toLowerCase();
  for (const wildcard of WEEKLY_SOURCE_DIFFERENTIAL_WILDCARDS) {
    if (normalised === wildcard || normalised.startsWith(`${wildcard} `) || normalised === `${wildcard}.`) {
      fail('DIFFERENTIAL_AUTHORITY_WILDCARD_REFUSED', `${label} uses the refused wildcard "${wildcard}".`);
    }
  }
  return true;
}

/**
 * One captured side of a protected differential. `phase` is `BEFORE` or `AFTER`; both sides
 * must name the same protected ID and the same surfaces, or the pair proves nothing.
 */
export function createDifferentialCapture({ protectionId, phase, surfaces, capturedAtUtc, repositoryCommit }) {
  if (!protectionId) fail('DIFFERENTIAL_PROTECTION_ID_REQUIRED', 'A differential capture must name its protected ID.');
  if (!['BEFORE', 'AFTER'].includes(phase)) fail('DIFFERENTIAL_PHASE_INVALID', 'A differential capture is BEFORE or AFTER.');
  if (!surfaces || typeof surfaces !== 'object' || Array.isArray(surfaces)) {
    fail('DIFFERENTIAL_SURFACES_INVALID', 'Differential surfaces must be an object keyed on the 23A section 12 surface names.');
  }
  const keys = Object.keys(surfaces);
  const unknown = keys.filter((key) => !WEEKLY_SOURCE_DIFFERENTIAL_SURFACES.includes(key));
  if (unknown.length) fail('DIFFERENTIAL_SURFACE_UNKNOWN', 'A differential capture named a surface outside 23A section 12.', { unknown });
  if (keys.length === 0) fail('DIFFERENTIAL_SURFACES_EMPTY', `${protectionId} captured no surface at all.`);
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(String(capturedAtUtc ?? ''))) {
    fail('DIFFERENTIAL_CLOCK_INVALID', 'A differential capture needs a fixed UTC capture time.');
  }
  if (!/^[a-f0-9]{40}$/.test(String(repositoryCommit ?? ''))) {
    fail('DIFFERENTIAL_COMMIT_INVALID', 'A differential capture needs the exact repository commit it was taken at.');
  }
  const body = {
    schemaVersion: 'WEEKLY_SOURCE_DIFFERENTIAL_CAPTURE_V1',
    protectionId,
    phase,
    capturedAtUtc,
    repositoryCommit,
    surfaceDigests: Object.fromEntries(
      keys.sort().map((key) => [key, canonicalDigest(surfaces[key])]),
    ),
  };
  return deepFreeze({ ...body, captureDigest: canonicalDigest(body) });
}

/**
 * Compare one protected ID's BEFORE and AFTER captures.
 *
 * A difference passes only when an expected-difference entry names the exact surface and an
 * independently loaded Requirement ID from the controlling 64-row atomic ledger or the
 * canonical 203-row policy registry. Everything else is an
 * adverse change. The caller may not construct that index from the expected-difference file.
 */
export function compareDifferentialCaptures(before, after, { expectedDifferences = [], requirementIndex = null } = {}) {
  if (before?.protectionId !== after?.protectionId) {
    fail('DIFFERENTIAL_PAIR_MISMATCH', 'A differential pair must describe the same protected ID.');
  }
  if (before?.phase !== 'BEFORE' || after?.phase !== 'AFTER') {
    fail('DIFFERENTIAL_PAIR_INVALID', 'A differential pair needs one BEFORE and one AFTER capture.');
  }
  const beforeSurfaces = Object.keys(before.surfaceDigests).sort();
  const afterSurfaces = Object.keys(after.surfaceDigests).sort();
  if (canonicalDigest(beforeSurfaces) !== canonicalDigest(afterSurfaces)) {
    fail('DIFFERENTIAL_SURFACE_SET_CHANGED', `${before.protectionId} compared different surfaces before and after.`);
  }
  const authorised = new Map();
  for (const entry of expectedDifferences) {
    if (entry?.protectionId !== before.protectionId) {
      fail('DIFFERENTIAL_EXPECTED_PROTECTION_MISMATCH', 'An expected difference names a different protected ID.');
    }
    if (!WEEKLY_SOURCE_DIFFERENTIAL_SURFACES.includes(entry?.surface)) {
      fail('DIFFERENTIAL_EXPECTED_SURFACE_UNKNOWN', 'An expected difference names a surface outside 23A section 12.');
    }
    assertNotWildcardAuthority(entry?.reason, `${before.protectionId} expected difference reason`);
    if (!entry?.requirementId) {
      fail('DIFFERENTIAL_EXPECTED_REQUIREMENT_REQUIRED', 'Only an independently resolved Requirement ID may authorise an expected difference.');
    }
    if (requirementIndex && !requirementIndex.has(entry.requirementId)) {
      fail('DIFFERENTIAL_EXPECTED_REQUIREMENT_ORPHAN', `${entry.requirementId} is not an atomic implementation requirement.`);
    }
    authorised.set(entry.surface, entry);
  }
  const differences = [];
  for (const surface of beforeSurfaces) {
    if (before.surfaceDigests[surface] === after.surfaceDigests[surface]) continue;
    const entry = authorised.get(surface);
    if (!entry) {
      differences.push({ surface, authorised: false, requirementId: null });
      continue;
    }
    differences.push({ surface, authorised: true, requirementId: entry.requirementId });
  }
  const adverse = differences.filter((item) => !item.authorised);
  return deepFreeze({
    schemaVersion: 'WEEKLY_SOURCE_DIFFERENTIAL_RESULT_V1',
    protectionId: before.protectionId,
    pass: adverse.length === 0,
    surfaceCount: beforeSurfaces.length,
    differences,
    adverseSurfaces: adverse.map((item) => item.surface),
    beforeDigest: before.captureDigest,
    afterDigest: after.captureDigest,
  });
}

/**
 * Which of the controlling protected IDs actually have a passing before/after pair.
 * A protected ID with only one side, or with an adverse difference, is not covered.
 */
export function summariseDifferentialCoverage(ledgers, differentialResults, { requireComplete = true } = {}) {
  if (!Array.isArray(ledgers?.protected)) {
    fail('DIFFERENTIAL_PROTECTED_LEDGER_REQUIRED', 'The controlling protected-functionality matrix is required.');
  }
  if (!Array.isArray(differentialResults)) throw new TypeError('differentialResults must be an array');
  const required = ledgers.protected.map((row) => row['Protection ID']);
  const passing = new Set();
  for (const result of differentialResults) {
    if (result?.schemaVersion !== 'WEEKLY_SOURCE_DIFFERENTIAL_RESULT_V1') {
      fail('DIFFERENTIAL_RESULT_INVALID', 'Differential coverage accepts only differential result records.');
    }
    if (!required.includes(result.protectionId)) {
      fail('DIFFERENTIAL_RESULT_ORPHAN', `${result.protectionId} is not a controlling protected ID.`);
    }
    if (result.pass === true) passing.add(result.protectionId);
  }
  const missing = required.filter((id) => !passing.has(id));
  if (requireComplete && missing.length) {
    fail('DIFFERENTIAL_COVERAGE_INCOMPLETE', 'Protected differentials do not cover every controlling protected ID.', {
      requiredCount: required.length,
      passingCount: passing.size,
      missingCount: missing.length,
      firstMissing: missing[0] ?? null,
    });
  }
  return deepFreeze({
    complete: missing.length === 0,
    requiredCount: required.length,
    passingCount: passing.size,
    missing,
  });
}

export const WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT = deepFreeze({
  version: 'WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT_V1',
  status: 'POPULATED_DATABASE_LIMB',
  populated: true,
  adapterPath: 'tests/weekly-source/adapters/differential-phase-adapter.mjs',
  adapterEnvironmentVariable: 'CLOUDTMS_WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER',
  requiredExport: 'runWeeklySourceHarnessPhase',
  phase: 'differential',
  authority: '23A_EXECUTABLE_TEST_HARNESS_SPECIFICATION.md section 12; annexes/protected-functionality-matrix.csv',
  protectedAreaCount: 29,
  newInPlan62: Object.freeze([
    { protectionId: 'PROT-ROTATION-001', requiredDifferential: 'ROT-010', note: 'every rotation entry point E1-E11; audit and result hashes identical' },
    { protectionId: 'PROT-UNAUTH-001', requiredDifferential: 'UNA-012', note: 'ordinary unauthorise owners; owner definition hashes unchanged' },
  ]),
  surfaces: WEEKLY_SOURCE_DIFFERENTIAL_SURFACES,
  expectedDifferenceRule: 'Only an independently resolved Requirement ID from the controlling atomic ledger or canonical 203-row policy registry may authorise a difference; wildcard reasons are refused.',
  baselinesCaptured: 28,
  completeProtectedAreas: 0,
  declaredExternalSurfaceCount: 1,
  openWork: Object.freeze([
    'The database limb is not complete protected-area proof. Every applicable backend response, Office, Candidate, invoice/document/export, report, C1 and Workbench-owned surface still needs independently linked evidence.',
    'PROT-APP-001 remains owned by the separate rendered browser and physical-device differential evidence.',
  ]),
});
