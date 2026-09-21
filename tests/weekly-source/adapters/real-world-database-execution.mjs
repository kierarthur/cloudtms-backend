import { createHash } from 'node:crypto';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';

import { canonicalDigest, deepFreeze } from '../harness/canonical-json.mjs';
import { loadScenariosFromDirectory } from '../harness/scenario-loader.mjs';
import { runWeeklySourceScenario } from '../harness/scenario-runner.mjs';
import { normalizeOutcome } from '../harness/expected-outcome-oracle.mjs';
import {
  buildRealWorldActionExecutionPlan,
  REAL_WORLD_DATABASE_MODES,
  REAL_WORLD_REQUIRED_OBSERVATIONS,
} from '../harness/real-world-action-contract.mjs';

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function requireFactory(factory) {
  if (typeof factory !== 'function') {
    fail(
      'WEEKLY_SOURCE_REAL_WORLD_PRODUCT_ADAPTER_REQUIRED',
      'NEW and UPGRADE database proof require the real Weekly Source product/database adapter.',
    );
  }
  return factory;
}

function assertDependencies(value, scenarioId, mode) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail('WEEKLY_SOURCE_REAL_WORLD_DEPENDENCIES_INVALID', `${scenarioId}/${mode} returned no adapter dependencies.`);
  }
  const required = [
    'parseSource', 'executeProductAction', 'collectObservedState', 'collectProjections',
    'collectCoverage', 'repositoryEvidence', 'databaseEvidence', 'cleanupScenario',
  ];
  for (const name of required) {
    if (typeof value[name] !== 'function') {
      fail('WEEKLY_SOURCE_REAL_WORLD_DEPENDENCY_MISSING', `${scenarioId}/${mode} is missing ${name}.`);
    }
  }
  if (typeof value.foundation?.applyPrerequisites !== 'function'
      || typeof value.foundation?.auditPrerequisites !== 'function') {
    fail('WEEKLY_SOURCE_REAL_WORLD_FOUNDATION_ADAPTER_MISSING', `${scenarioId}/${mode} has no audited foundation adapter.`);
  }
  return value;
}

function assertEnvelope(envelope, scenario, mode, plan, componentBoundary) {
  if (envelope?.schemaVersion !== 'WEEKLY_SOURCE_TEST_RESULT_V1' || envelope.status !== 'PASS') {
    fail('WEEKLY_SOURCE_REAL_WORLD_ENVELOPE_FAILED', `${scenario.scenarioId}/${mode} did not produce a passing result envelope.`);
  }
  const executed = Array.isArray(envelope.executedOwners) ? envelope.executedOwners : [];
  if (executed.length !== scenario.actions.length) {
    fail('WEEKLY_SOURCE_REAL_WORLD_ACTION_COUNT_MISMATCH', `${scenario.scenarioId}/${mode} did not execute every declared action.`, {
      expected: scenario.actions.length,
      actual: executed.length,
    });
  }
  for (const [index, action] of scenario.actions.entries()) {
    const owner = executed[index];
    if (owner?.kind !== action.kind || typeof owner?.name !== 'string' || owner.name.length === 0) {
      fail('WEEKLY_SOURCE_REAL_WORLD_ACTION_OWNER_MISMATCH', `${scenario.scenarioId}/${mode} action ${index + 1} has no exact product owner.`);
    }
    const allowed = plan.actions[index].owners;
    if (!allowed.some((name) => owner.name === name || owner.name.startsWith(`${name}#`))) {
      fail('WEEKLY_SOURCE_REAL_WORLD_ACTION_OWNER_UNREGISTERED', `${scenario.scenarioId}/${mode} action ${index + 1} used an unregistered owner.`, {
        kind: action.kind,
        owner: owner.name,
        allowed,
      });
    }
  }
  if (envelope.database?.used !== true || envelope.database?.engine !== 'PostgreSQL') {
    fail('WEEKLY_SOURCE_REAL_WORLD_DATABASE_EVIDENCE_MISSING', `${scenario.scenarioId}/${mode} did not bind its result to PostgreSQL.`);
  }
  if (componentBoundary === true
      && (envelope.database?.evidenceScope !== 'LOCAL_LIMB_ONLY'
        || envelope.database?.bankingPayBoundary !== 'HANDOVER2_BOUNDARY_EMULATED'
        || envelope.database?.releaseEvidenceEligible !== false
        || envelope.c1?.evidenceScope !== 'LOCAL_LIMB_ONLY'
        || envelope.c1?.bankingPayBoundary !== 'HANDOVER2_BOUNDARY_EMULATED'
        || envelope.c1?.emulator !== true)) {
    fail(
      'WEEKLY_SOURCE_REAL_WORLD_BOUNDARY_LABEL_MISSING',
      `${scenario.scenarioId}/${mode} did not preserve the LOCAL_LIMB_ONLY and HANDOVER2_BOUNDARY_EMULATED evidence labels.`,
    );
  }
  if (envelope.parser?.mode !== 'PRODUCT_PARSER_ROUND_TRIP') {
    fail('WEEKLY_SOURCE_REAL_WORLD_PARSER_EVIDENCE_MISSING', `${scenario.scenarioId}/${mode} bypassed the production parser round trip.`);
  }
  if (canonicalDigest(normalizeOutcome(envelope.actual, { strict: false })) !== envelope.actualDigest) {
    fail('WEEKLY_SOURCE_REAL_WORLD_ACTUAL_DIGEST_INVALID', `${scenario.scenarioId}/${mode} actual observations are not the compared database observations.`);
  }
  if (canonicalDigest(envelope.actual) === canonicalDigest(scenario.expected)
      && envelope.database?.rowsReadBack !== true) {
    fail('WEEKLY_SOURCE_REAL_WORLD_EXPECTED_COPY_REFUSED', `${scenario.scenarioId}/${mode} did not prove that matching actual data came from database read-back.`);
  }
  for (const observation of REAL_WORLD_REQUIRED_OBSERVATIONS.filter((name) => !['audit', 'forbiddenOutcomes'].includes(name))) {
    if (!Array.isArray(envelope.actual?.[observation])) {
      fail('WEEKLY_SOURCE_REAL_WORLD_OBSERVATION_MISSING', `${scenario.scenarioId}/${mode} omitted ${observation}.`);
    }
  }
  const projectionNames = new Set((envelope.projectionDigests ?? []).map((item) => item.name));
  for (const observation of ['audit', 'forbiddenOutcomes']) {
    if (!projectionNames.has(observation)) {
      fail('WEEKLY_SOURCE_REAL_WORLD_OBSERVATION_MISSING', `${scenario.scenarioId}/${mode} omitted independently read ${observation}.`);
    }
  }
}

/**
 * Runs the populated sample journeys through the ordinary scenario runner on
 * the exact NEW/UPGRADE clone. This module owns no business behavior: the
 * supplied adapter must seed prerequisites only and must call shipped owners
 * for every action. No broad verifier result can satisfy this gate.
 */
export async function executeRealWorldDatabaseJourneys({
  mode,
  scenarioDirectory,
  resultDirectory,
  createScenarioDependencies,
  database,
}) {
  if (!REAL_WORLD_DATABASE_MODES.includes(mode)) {
    fail('WEEKLY_SOURCE_REAL_WORLD_MODE_INVALID', 'Real-world database journeys require NEW or UPGRADE.');
  }
  const loaded = await loadScenariosFromDirectory(scenarioDirectory);
  const scenarios = loaded.scenarios.filter((scenario) => scenario.tags?.includes('REAL_WORLD'));
  if (scenarios.length !== 2) {
    fail('WEEKLY_SOURCE_REAL_WORLD_SCENARIO_SET_INVALID', `Expected two populated real-world scenarios, received ${scenarios.length}.`);
  }
  const factory = requireFactory(createScenarioDependencies);
  await mkdir(resultDirectory, { recursive: true });
  const results = [];
  for (const scenario of scenarios) {
    const plan = buildRealWorldActionExecutionPlan(scenario);
    const dependencies = assertDependencies(await factory({ mode, scenario, plan, database }), scenario.scenarioId, mode);
    const fileName = `real-world-${mode.toLowerCase()}-${scenario.scenarioId.toLowerCase()}.json`;
    const resultPath = path.join(resultDirectory, fileName);
    const envelope = await runWeeklySourceScenario({ scenario, dependencies, resultPath });
    const persisted = JSON.parse(await readFile(resultPath, 'utf8'));
    if (persisted.evidenceDigest !== envelope.evidenceDigest) {
      fail('WEEKLY_SOURCE_REAL_WORLD_PERSISTED_EVIDENCE_MISMATCH', `${scenario.scenarioId}/${mode} persisted evidence changed.`);
    }
    assertEnvelope(envelope, scenario, mode, plan, database.componentBoundary === true);
    results.push(deepFreeze({
      scenarioId: scenario.scenarioId,
      mode,
      actionCount: scenario.actions.length,
      resultFile: fileName,
      evidenceDigest: envelope.evidenceDigest,
      actualDigest: envelope.actualDigest,
      actionPlanDigest: createHash('sha256').update(JSON.stringify(plan)).digest('hex'),
      evidenceScope: envelope.database?.evidenceScope ?? null,
      bankingPayBoundary: envelope.database?.bankingPayBoundary ?? null,
      releaseEvidenceEligible: envelope.database?.releaseEvidenceEligible ?? null,
    }));
  }
  return deepFreeze({
    complete: true,
    mode,
    scenarioCount: results.length,
    actionCount: results.reduce((total, item) => total + item.actionCount, 0),
    evidenceScope: database.componentBoundary === true ? 'LOCAL_LIMB_ONLY' : 'FULL_RELEASE_PATH',
    bankingPayBoundary: database.componentBoundary === true
      ? 'HANDOVER2_BOUNDARY_EMULATED'
      : 'HANDOVER2_BOUNDARY_EXECUTED',
    releaseEvidenceEligible: database.componentBoundary !== true,
    results,
  });
}
