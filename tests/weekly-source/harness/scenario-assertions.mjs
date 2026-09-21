import { canonicalDigest, canonicalJson, cloneJson, deepFreeze } from './canonical-json.mjs';
import { compareActualWithOracle, findFirstDivergence } from './expected-outcome-oracle.mjs';

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourceScenarioAssertionError';
  error.code = code;
  error.details = deepFreeze(cloneJson(details));
  throw error;
}

function requireStringArray(value, label) {
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string' || item.length === 0)) {
    fail('SCENARIO_ASSERTION_INPUT_INVALID', `${label} must be an array of non-empty strings.`);
  }
  return [...value].sort((left, right) => left.localeCompare(right, 'en'));
}

function visibleText(projections) {
  if (!Array.isArray(projections)) {
    fail('SCENARIO_ASSERTION_INPUT_INVALID', 'Projection evidence must be an array.');
  }
  const values = [];
  const walk = (value) => {
    if (typeof value === 'string') values.push(value);
    else if (Array.isArray(value)) value.forEach(walk);
    else if (value && typeof value === 'object') Object.values(value).forEach(walk);
  };
  projections.forEach(walk);
  return values.join('\n');
}

function normalizeC1Category(value) {
  const category = value?.category ?? value?.publicationCategory ?? 'NONE';
  if (!['NONE', 'COMPLETE_ENTITLEMENT', 'CERTIFIED_EMPTY', 'WAITING', 'REFUSED'].includes(category)) {
    fail('SCENARIO_C1_CATEGORY_INVALID', 'C1 evidence contains an unsupported publication category.');
  }
  return category;
}

function assertGeneratedSources(generatedSources) {
  if (!Array.isArray(generatedSources)) {
    fail('SCENARIO_SOURCE_EVIDENCE_INVALID', 'Generated-source evidence must be an array.');
  }
  for (const source of generatedSources) {
    if (!source || typeof source !== 'object' || !source.uploadKey || !source.profile) {
      fail('SCENARIO_SOURCE_EVIDENCE_INVALID', 'Generated-source evidence is incomplete.');
    }
    if (source.fileName === null) {
      if (source.sha256 !== null || source.byteCount !== 0 || source.parserNormalFormDigest !== null) {
        fail('SCENARIO_NO_SOURCE_EVIDENCE_INVALID', 'A no-source fixture must have no bytes or parser result.');
      }
      continue;
    }
    if (!/^[a-f0-9]{64}$/.test(source.sha256 || '') || !/^[a-f0-9]{64}$/.test(source.parserNormalFormDigest || '')) {
      fail('SCENARIO_SOURCE_EVIDENCE_INVALID', 'Generated source and parser evidence require SHA-256 digests.');
    }
    if (source.roundTripPass !== true) {
      fail('SCENARIO_SOURCE_ROUND_TRIP_FAILED', `Generated source ${source.uploadKey} did not round-trip through the product parser.`);
    }
  }
}

export function assertScenarioResult({
  scenario,
  oracle,
  actual,
  projections = [],
  c1 = { category: 'NONE' },
  generatedSources = [],
  executedAcceptanceIds,
  executedProtectedIds,
}) {
  if (!scenario?.scenarioId || !oracle?.expectedDigest) {
    fail('SCENARIO_ASSERTION_INPUT_INVALID', 'A validated scenario and declared-outcome oracle are required.');
  }
  assertGeneratedSources(generatedSources);
  const comparison = compareActualWithOracle(actual, oracle);
  if (!comparison.pass) {
    const divergence = {
      ...comparison.firstDivergence,
      expected: comparison.firstDivergence?.expected ?? null,
      actual: comparison.firstDivergence?.actual ?? null,
    };
    fail(
      'SCENARIO_ACTUAL_DIVERGED',
      `Scenario ${scenario.scenarioId} diverged at ${comparison.firstDivergence.path}.`,
      divergence,
    );
  }

  const expectedC1 = scenario.expected.c1PublicationCategory ?? 'NONE';
  const actualC1 = normalizeC1Category(c1);
  if (actualC1 !== expectedC1) {
    fail('SCENARIO_C1_CATEGORY_DIVERGED', `Scenario ${scenario.scenarioId} returned the wrong C1 category.`, {
      expected: expectedC1,
      actual: actualC1,
    });
  }

  const text = visibleText(projections);
  for (const expectedText of scenario.expected.visibleAssertions) {
    if (!text.includes(expectedText)) {
      fail('SCENARIO_VISIBLE_ASSERTION_MISSING', `Expected visible text is absent: ${expectedText}`);
    }
  }
  for (const forbiddenText of scenario.expected.forbiddenAssertions) {
    if (text.includes(forbiddenText)) {
      fail('SCENARIO_FORBIDDEN_ASSERTION_PRESENT', `Forbidden visible text is present: ${forbiddenText}`);
    }
  }

  const acceptanceIds = requireStringArray(executedAcceptanceIds, 'executedAcceptanceIds');
  const protectedIds = requireStringArray(executedProtectedIds, 'executedProtectedIds');
  const undeclaredProtected = protectedIds.filter((id) => !scenario.protectedIds.includes(id));
  if (undeclaredProtected.length) {
    fail('SCENARIO_PROTECTION_MAPPING_UNDECLARED', 'Executed protected checks are not declared by the scenario.', {
      ids: undeclaredProtected,
    });
  }
  if (acceptanceIds.length === 0) {
    fail('SCENARIO_ACCEPTANCE_MAPPING_MISSING', 'A passing scenario must name at least one executed acceptance ID.');
  }

  const projectionDigests = projections.map((projection, index) => ({
    name: String(projection?.name ?? `projection-${index + 1}`),
    digest: canonicalDigest(projection),
  }));
  return deepFreeze({
    pass: true,
    comparison,
    c1Category: actualC1,
    acceptanceIds,
    protectedIds,
    projectionDigests,
    assertionDigest: canonicalDigest({
      scenarioId: scenario.scenarioId,
      actual: comparison.actualDigest,
      c1: actualC1,
      acceptanceIds,
      protectedIds,
      projectionDigests,
    }),
  });
}

export function compareBoundedResult(expected, actual, label = 'bounded result') {
  const difference = findFirstDivergence(expected, actual);
  return deepFreeze({
    label,
    pass: difference === null,
    firstDivergence: difference,
    expectedDigest: canonicalDigest(expected),
    actualDigest: canonicalDigest(actual),
    exact: canonicalJson(expected) === canonicalJson(actual),
  });
}
