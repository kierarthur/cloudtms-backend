import { canonicalDigest, canonicalJson, cloneJson, deepFreeze } from './canonical-json.mjs';

function compareBy(fields) {
  return (left, right) => {
    for (const field of fields) {
      const result = String(left?.[field] ?? '').localeCompare(String(right?.[field] ?? ''), 'en');
      if (result !== 0) return result;
    }
    return canonicalJson(left).localeCompare(canonicalJson(right), 'en');
  };
}

function sumPence(rows, field) {
  return rows.reduce((total, row) => total + BigInt(row[field]), 0n).toString();
}

export function normalizeOutcome(expected, { strict = true } = {}) {
  const normalized = cloneJson(expected);
  const requiredArrays = ['sourceMovements', 'invoiceLines', 'approvedHours', 'communications', 'visibleAssertions', 'forbiddenAssertions'];
  if (strict && requiredArrays.some((field) => !Array.isArray(normalized?.[field]))) {
    throw new TypeError('Declared expected outcome is missing a required array');
  }
  normalized.sourceMovements?.sort(compareBy(['sourceRowKey', 'kind']));
  normalized.invoiceLines?.sort(compareBy(['clientKey', 'cycleKey', 'sourceRowKey']));
  normalized.approvedHours?.sort(compareBy(['candidateKey', 'contractKey', 'weekKey']));
  for (const item of normalized.approvedHours || []) {
    item.shifts?.sort(compareBy(['workDate', 'start', 'end', 'key']));
  }
  normalized.communications?.sort(compareBy(['audience', 'kind', 'groupKey']));
  normalized.visibleAssertions?.sort((a, b) => a.localeCompare(b, 'en'));
  normalized.forbiddenAssertions?.sort((a, b) => a.localeCompare(b, 'en'));
  return normalized;
}

function boundedLeaf(value) {
  if (typeof value === 'string') return value.length <= 160 ? value : `${value.slice(0, 157)}...`;
  if (value === null || typeof value !== 'object') return value;
  return `[${Array.isArray(value) ? 'array' : 'object'}]`;
}

export function findFirstDivergence(expected, actual, path = '$') {
  if (canonicalJson(expected) === canonicalJson(actual)) return null;
  if (Array.isArray(expected) && Array.isArray(actual)) {
    const length = Math.max(expected.length, actual.length);
    for (let index = 0; index < length; index += 1) {
      if (index >= expected.length || index >= actual.length) {
        return { path: `${path}[${index}]`, expected: boundedLeaf(expected[index]), actual: boundedLeaf(actual[index]) };
      }
      const difference = findFirstDivergence(expected[index], actual[index], `${path}[${index}]`);
      if (difference) return difference;
    }
  }
  if (expected && actual && typeof expected === 'object' && typeof actual === 'object' && !Array.isArray(expected) && !Array.isArray(actual)) {
    const keys = [...new Set([...Object.keys(expected), ...Object.keys(actual)])].sort();
    for (const key of keys) {
      if (!Object.hasOwn(expected, key) || !Object.hasOwn(actual, key)) {
        return { path: `${path}.${key}`, expected: boundedLeaf(expected[key]), actual: boundedLeaf(actual[key]) };
      }
      const difference = findFirstDivergence(expected[key], actual[key], `${path}.${key}`);
      if (difference) return difference;
    }
  }
  return { path, expected: boundedLeaf(expected), actual: boundedLeaf(actual) };
}

export function buildExpectedOutcomeOracle(scenario) {
  if (!scenario?.expected) throw new TypeError('A validated scenario with declared expected facts is required');
  // Invoice pence is copied from the policy declaration. In particular, an NHSP
  // scenario declares its signed Commission + Total Cost value here; this oracle
  // never substitutes CloudTMS calculated charge or imports a product calculator.
  const expected = normalizeOutcome(scenario.expected);
  const arithmetic = {
    sourceMovementInvoiceExVatPence: sumPence(expected.sourceMovements, 'invoiceExVatPence'),
    invoiceExVatPence: sumPence(expected.invoiceLines, 'exVatPence'),
    approvedWorkedMinutes: expected.approvedHours.reduce(
      (total, group) => total + group.shifts.reduce((groupTotal, shift) => groupTotal + shift.workedMinutes, 0),
      0
    )
  };
  return deepFreeze({
    oracleVersion: 'WEEKLY_SOURCE_DECLARED_OUTCOME_ORACLE_V1',
    scenarioId: scenario.scenarioId,
    expected,
    arithmetic,
    expectedDigest: canonicalDigest(expected)
  });
}

export function compareActualWithOracle(actual, oracle) {
  if (!oracle || oracle.oracleVersion !== 'WEEKLY_SOURCE_DECLARED_OUTCOME_ORACLE_V1') {
    throw new TypeError('A Weekly Source declared-outcome oracle is required');
  }
  const normalizedActual = normalizeOutcome(actual, { strict: false });
  const firstDivergence = findFirstDivergence(oracle.expected, normalizedActual);
  return deepFreeze({
    pass: firstDivergence === null,
    firstDivergence,
    expectedDigest: oracle.expectedDigest,
    actualDigest: canonicalDigest(normalizedActual)
  });
}
