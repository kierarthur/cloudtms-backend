import { canonicalDigest, canonicalJson, deepFreeze } from './canonical-json.mjs';
import { buildFoundationRecordPlan, FoundationBuilderError } from './foundation-record-builder.mjs';

const PRODUCT_OUTCOME_KINDS = /SOURCE_MOVEMENT|INVOICE_LINE|PROTECTED_HOURS|C1_PUBLICATION|QUERY_INCIDENT|FINAL_SOURCE/;

function firstDifference(left, right, location = '$') {
  if (canonicalJson(left) === canonicalJson(right)) return null;
  if (Array.isArray(left) && Array.isArray(right)) {
    const length = Math.max(left.length, right.length);
    for (let index = 0; index < length; index += 1) {
      if (index >= left.length || index >= right.length) return `${location}[${index}]`;
      const nested = firstDifference(left[index], right[index], `${location}[${index}]`);
      if (nested) return nested;
    }
  }
  if (left && right && typeof left === 'object' && typeof right === 'object' && !Array.isArray(left) && !Array.isArray(right)) {
    for (const key of [...new Set([...Object.keys(left), ...Object.keys(right)])].sort()) {
      if (!Object.hasOwn(left, key) || !Object.hasOwn(right, key)) return `${location}.${key}`;
      const nested = firstDifference(left[key], right[key], `${location}.${key}`);
      if (nested) return nested;
    }
  }
  return location;
}

export function auditFoundationRecordPlan(scenario, candidatePlan) {
  if (!candidatePlan || candidatePlan.mode !== 'NON_DATABASE_INPUT_PLAN') {
    throw new FoundationBuilderError('FOUNDATION_AUDIT_MODE_INVALID', 'Only a non-database foundation input plan may be audited here');
  }
  for (const stage of candidatePlan.stages || []) {
    for (const record of stage.records || []) {
      if (PRODUCT_OUTCOME_KINDS.test(String(record.recordKind))) {
        throw new FoundationBuilderError('FOUNDATION_PRODUCT_OUTCOME_FORBIDDEN', `Foundation plan contains product outcome ${record.recordKind}`);
      }
    }
  }
  const expected = buildFoundationRecordPlan(scenario);
  const difference = firstDifference(expected, candidatePlan);
  if (difference) {
    throw new FoundationBuilderError('FOUNDATION_AUDIT_MISMATCH', `Foundation input plan differs from declared scenario facts at ${difference}`);
  }
  return deepFreeze({
    certificateVersion: 'WEEKLY_SOURCE_FOUNDATION_PLAN_AUDIT_V1',
    certificateKind: 'PRE_DATABASE_FOUNDATION_PLAN_AUDIT',
    scenarioId: scenario.scenarioId,
    planDigest: candidatePlan.planDigest,
    auditedRecordCount: candidatePlan.stages.reduce((total, stage) => total + stage.records.length, 0),
    certificateDigest: canonicalDigest({ scenarioId: scenario.scenarioId, planDigest: candidatePlan.planDigest })
  });
}

