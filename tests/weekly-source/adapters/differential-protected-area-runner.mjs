// Weekly Source Plan 6.2 - the 29-area protected differential, run over two real builds (WP-16d).
//
// WHAT "BEFORE" MEANS HERE, AND WHY IT IS NOT THE SAME STATE TWICE.
//
// WP-16c built its BEFORE side by taking the AFTER clone and re-applying, from
// `git show origin/test:<file>`, only the repeatables that `git status` reported as MODIFIED.
// That leaves every file the project ADDED in place: on the tree this package measured, that
// is 6 new migrations and 34 new repeatables, so 361 new routines, 102 new relations, 133 new
// triggers and 102 new policies were present on both sides of that comparison.
//
// This runner does not build either side. It takes the names of TWO DATABASES THAT WERE BUILT
// SEPARATELY through `scripts/cloudtms-db-release.mjs apply` in NEW mode:
//
//   BEFORE - built from the committed baseline tree, materialised read-only with
//            `git archive HEAD | tar -x` into a scratch directory. No checkout, no branch
//            operation, no change to any worktree file.
//   AFTER  - built from the current worktree, with this programme's uncommitted work in place.
//
// The runner refuses to compare two databases whose installed migration and repeatable ledgers
// are identical, because such a pair proves nothing.

import path from 'node:path';
import { canonicalDigest } from '../harness/canonical-json.mjs';
import { createResultEnvelope, writeResultEnvelope } from '../harness/result-envelope.mjs';
import {
  compareDifferentialCaptures,
  createDifferentialCapture,
} from '../harness/differential-protection-contract.mjs';
import {
  WEEKLY_SOURCE_NEW_OWNER_PATTERN,
  WEEKLY_SOURCE_PROTECTED_AREA_RULES,
} from './differential-protected-area-rules.mjs';
import {
  digestRows,
  excludedOwnerSqlFor,
  measurementSqlFor,
} from './differential-protected-area-measure.mjs';

const DATABASE_SURFACE = 'database rows and projections';

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourceProtectedAreaDifferentialError';
  error.code = code;
  error.details = details;
  throw error;
}

/**
 * Prove the two sides are different states before measuring anything. A differential whose
 * two sides came from the same build has already been produced twice in this programme; this
 * gate makes that outcome impossible rather than merely discouraged.
 */
export function assertDistinctBuilds(beforeIdentity, afterIdentity) {
  if (beforeIdentity.migrations === afterIdentity.migrations
    && beforeIdentity.repeatables === afterIdentity.repeatables
    && beforeIdentity.routines === afterIdentity.routines) {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_SAME_STATE',
      'BEFORE and AFTER carry the same installed ledger and routine count, so the pair proves nothing.',
      { beforeIdentity, afterIdentity });
  }
  return true;
}

/**
 * Run the full 29. `query(database, sql)` is injected so the caller owns the connection and
 * this module never holds a target of its own.
 */
export async function runProtectedAreaDifferential({
  query, beforeDatabase, afterDatabase, repositoryCommit, capturedAtUtc,
  expectedDifferences = [], requirementIndex = null, resultDirectory = null,
  buildProvenance = {},
}) {
  const identityFor = (database) => ({
    migrations: Number(query(database, 'select count(*)::text from private.cloudtms_migration_ledger;')),
    repeatables: Number(query(database, 'select count(*)::text from private.cloudtms_repeatable_ledger;')),
    routines: Number(query(database, "select count(*)::text from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace where n.nspname in ('public','private');")),
  });
  const beforeIdentity = identityFor(beforeDatabase);
  const afterIdentity = identityFor(afterDatabase);
  assertDistinctBuilds(beforeIdentity, afterIdentity);

  const measured = [];
  const unmeasurable = [];
  const results = [];

  for (const rule of WEEKLY_SOURCE_PROTECTED_AREA_RULES) {
    if (rule.unmeasurable) {
      unmeasurable.push({
        protectionId: rule.protectionId,
        reason: rule.unmeasurable,
        declaredExternalSurface: true,
      });
      continue;
    }
    const sql = measurementSqlFor(rule, WEEKLY_SOURCE_NEW_OWNER_PATTERN);
    const beforeRows = query(beforeDatabase, sql).split('\n').map((line) => line.trim()).filter(Boolean);
    const afterRows = query(afterDatabase, sql).split('\n').map((line) => line.trim()).filter(Boolean);
    if (beforeRows.length === 0 && afterRows.length === 0) {
      unmeasurable.push({
        protectionId: rule.protectionId,
        reason: 'the stated owner rule matched no installed object on either side, so nothing was compared',
        declaredExternalSurface: false,
      });
      continue;
    }
    const excludedSql = excludedOwnerSqlFor(rule, WEEKLY_SOURCE_NEW_OWNER_PATTERN);
    const excludedAfter = excludedSql ? Number(query(afterDatabase, excludedSql).trim()) : 0;

    const beforeMeasure = digestRows(beforeRows);
    const afterMeasure = digestRows(afterRows);
    const before = createDifferentialCapture({
      protectionId: rule.protectionId,
      phase: 'BEFORE',
      surfaces: { [DATABASE_SURFACE]: beforeMeasure },
      capturedAtUtc,
      repositoryCommit,
    });
    const after = createDifferentialCapture({
      protectionId: rule.protectionId,
      phase: 'AFTER',
      surfaces: { [DATABASE_SURFACE]: afterMeasure },
      capturedAtUtc,
      repositoryCommit,
    });
    const result = compareDifferentialCaptures(before, after, {
      expectedDifferences: expectedDifferences.filter((entry) => entry.protectionId === rule.protectionId),
      requirementIndex,
    });
    results.push(result);

    const addedRows = afterRows.filter((row) => !beforeRows.includes(row));
    const removedRows = beforeRows.filter((row) => !afterRows.includes(row));
    measured.push({
      protectionId: rule.protectionId,
      requiredProof: rule.requiredProof,
      partial: rule.partial ?? null,
      excludedNewSourceOwners: excludedAfter,
      beforeRowCount: beforeRows.length,
      afterRowCount: afterRows.length,
      beforeDigest: beforeMeasure.digest,
      afterDigest: afterMeasure.digest,
      identical: beforeMeasure.digest === afterMeasure.digest,
      addedRows,
      removedRows,
      pass: result.pass,
      differences: result.differences,
    });
  }

  const identical = measured.filter((item) => item.identical);
  const differing = measured.filter((item) => !item.identical);
  const authorised = differing.filter((item) => item.pass);
  const unauthorised = differing.filter((item) => !item.pass);

  const summary = {
    protectedAreaCount: WEEKLY_SOURCE_PROTECTED_AREA_RULES.length,
    measuredCount: measured.length,
    identicalCount: identical.length,
    differingAuthorisedCount: authorised.length,
    differingUnauthorisedCount: unauthorised.length,
    unmeasurableCount: unmeasurable.length,
    unexpectedUnmeasurableCount: unmeasurable.filter((item) => !item.declaredExternalSurface).length,
  };
  const protectionResults = measured.map((item) => ({
    protectedId: item.protectionId,
    surface: 'DATABASE',
    result: item.pass ? 'PASS' : 'FAIL',
    executedChecks: ['BEFORE_CAPTURE', 'AFTER_CAPTURE', 'AUTHORISED_DIFFERENCE_COMPARISON'],
    observedResults: [
      `before=${item.beforeDigest}`,
      `after=${item.afterDigest}`,
      `identical=${item.identical}`,
      `authorisedDifferences=${item.differences.length}`,
    ],
    prohibitedOutcomeChecks: [item.pass ? 'NO_UNAUTHORISED_DIFFERENCE' : 'UNAUTHORISED_DIFFERENCE_FOUND'],
  }));

  if (resultDirectory) {
    const expected = {
      ...summary,
      differingUnauthorisedCount: 0,
      unexpectedUnmeasurableCount: 0,
      protectionResults,
    };
    const actual = { ...summary, protectionResults };
    const envelope = createResultEnvelope({
      scenario: {
        schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
        scenarioId: 'WS-DIFFERENTIAL-PROTECTED-29-001',
        fixedSeed: canonicalDigest('weekly-source-differential-protected-29-v1'),
        requirementIds: [...new Set(expectedDifferences.map((entry) => entry.requirementId))].sort(),
        // This execution proves only the database limb of the eight-surface
        // differential. It must not populate protectedIds, which the coverage
        // gate interprets as complete protected-area proof.
        protectedIds: [],
      },
      repositories: [{ repository: 'cloudtms-backend', commit: repositoryCommit }],
      database: { used: true, engine: 'PostgreSQL', mode: 'DIFFERENTIAL', templateDatabase: null },
      generatedSources: [],
      parser: { used: false },
      clockValuesUtc: [capturedAtUtc],
      executedOwners: measured.map((item) => item.protectionId),
      oracle: { expected, expectedDigest: canonicalDigest(expected) },
      actual,
      comparison: {
        pass: canonicalDigest(actual) === canonicalDigest(expected),
        actualDigest: canonicalDigest(actual),
        firstDivergence: unauthorised[0]
          ? { protectionId: unauthorised[0].protectionId, differences: unauthorised[0].differences }
          : (unmeasurable[0] ?? null),
      },
      c1: { category: 'NONE', emulator: false, releaseEvidenceEligible: true },
      outbox: { calls: [] },
      projectionDigests: [],
      protectedIds: [],
      // Keep the complete before/after census in the result envelope. A
      // summary-only result is not independently reviewable and can conceal
      // which protected owners changed.
      foundation: {
        buildProvenance: { ...buildProvenance, beforeIdentity, afterIdentity },
        measured,
        unmeasurable,
      },
      cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
    });
    await writeResultEnvelope(path.join(resultDirectory, 'differential-protected-29.json'), envelope);
  }

  return {
    executed: true,
    proofScope: 'DATABASE_SURFACE_ONLY',
    pass: unauthorised.length === 0
      && unmeasurable.every((item) => item.declaredExternalSurface === true),
    summary,
    buildProvenance: { ...buildProvenance, beforeIdentity, afterIdentity },
    measured,
    unmeasurable,
    results,
  };
}
