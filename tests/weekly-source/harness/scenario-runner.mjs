import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { canonicalDigest, cloneJson, deepFreeze } from './canonical-json.mjs';
import { buildExpectedOutcomeOracle } from './expected-outcome-oracle.mjs';
import { buildExpectedSourceModel } from './expected-source-model.mjs';
import { createExternalEffectFakes, withExternalNetworkDenied } from './external-effect-fakes.mjs';
import { createFixedClock } from './fixed-clock.mjs';
import { auditFoundationRecordPlan } from './foundation-record-audit.mjs';
import { buildFoundationRecordPlan } from './foundation-record-builder.mjs';
import { createResultEnvelope, writeResultEnvelope } from './result-envelope.mjs';
import { assertScenarioResult, compareBoundedResult } from './scenario-assertions.mjs';
import { cleanupScenarioWorkspace, createScenarioWorkspace } from './scenario-cleanup.mjs';
import { buildSourceFile } from './source-file-builder.mjs';

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourceScenarioRunnerError';
  error.code = code;
  error.details = deepFreeze(cloneJson(details));
  throw error;
}

function callable(value, label) {
  if (typeof value !== 'function') fail('SCENARIO_RUNNER_DEPENDENCY_MISSING', `${label} is required.`);
  return value;
}

function requireObject(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail('SCENARIO_RUNNER_DEPENDENCY_INVALID', `${label} must be a JSON object.`);
  }
  return value;
}

function assertFoundationCertificate(plan, certificate) {
  const value = requireObject(certificate, 'Foundation read-back certificate');
  if (
    value.passed !== true
    || value.scenarioId !== plan.scenarioId
    || value.planDigest !== plan.planDigest
    || !/^[a-f0-9]{64}$/.test(String(value.preconditionDigest ?? ''))
  ) {
    fail('SCENARIO_FOUNDATION_AUDIT_FAILED', 'The foundation was not certified from a read-back before product actions.');
  }
  return deepFreeze(cloneJson(value));
}

function normalizeActionOwner(value, actionKind) {
  const owner = requireObject(value, `Action result for ${actionKind}`);
  if (typeof owner.owner !== 'string' || owner.owner.length === 0 || typeof owner.result !== 'object') {
    fail('SCENARIO_ACTION_RESULT_INVALID', `Action ${actionKind} did not return a bounded product-owner result.`);
  }
  if (owner.seededOutcome === true) {
    fail('SCENARIO_PRODUCT_OUTCOME_SEED_FORBIDDEN', `Action ${actionKind} attempted to seed a product outcome.`);
  }
  return deepFreeze(cloneJson(owner));
}

function sourceOptionsFor(dependencies, upload) {
  const definitions = dependencies.sourceDefinitions ?? {};
  return {
    genericProfile: definitions[upload.key] ?? definitions[upload.profile] ?? null,
    nhspReportHeadingName: dependencies.nhspReportHeadingName,
  };
}

async function buildAndParseSources({ scenario, dependencies, workspace }) {
  const parseSource = callable(dependencies.parseSource, 'dependencies.parseSource');
  const generated = [];
  for (const upload of scenario.sourceUploads) {
    const artifact = buildSourceFile(scenario, upload, sourceOptionsFor(dependencies, upload));
    const expected = buildExpectedSourceModel(scenario, upload);
    if (artifact.bytes === null) {
      generated.push(deepFreeze({
        uploadKey: upload.key,
        profile: upload.profile,
        fileName: null,
        byteCount: 0,
        sha256: null,
        parserNormalFormDigest: null,
        expectedNormalFormDigest: expected.modelDigest,
        roundTripPass: true,
      }));
      continue;
    }
    const target = path.join(workspace.path, artifact.fileName);
    await writeFile(target, artifact.bytes, { flag: 'wx' });
    const parsed = requireObject(await parseSource({
      scenario,
      upload,
      artifact: Object.freeze({ ...artifact, bytes: Buffer.from(artifact.bytes) }),
      filePath: target,
    }), `Parser result for ${upload.key}`);
    if (!parsed.normalForm || typeof parsed.normalForm !== 'object') {
      fail('SCENARIO_PARSER_RESULT_INVALID', `The product parser returned no normal form for ${upload.key}.`);
    }
    const comparison = compareBoundedResult(expected, parsed.normalForm, `source:${upload.key}`);
    if (!comparison.pass) {
      fail('SCENARIO_SOURCE_ROUND_TRIP_FAILED', `Source ${upload.key} diverged at ${comparison.firstDivergence.path}.`, comparison.firstDivergence);
    }
    generated.push(deepFreeze({
      uploadKey: upload.key,
      profile: upload.profile,
      fileName: artifact.fileName,
      mediaType: artifact.mediaType,
      byteCount: artifact.byteCount,
      sha256: artifact.sha256,
      parserProfile: String(parsed.parserProfile ?? upload.profile),
      parserVersion: String(parsed.parserVersion ?? 'UNDECLARED'),
      parserNormalFormDigest: canonicalDigest(parsed.normalForm),
      expectedNormalFormDigest: expected.modelDigest,
      roundTripPass: true,
    }));
  }
  return deepFreeze(generated);
}

async function executeActions({ scenario, dependencies, clock, generatedSources, effects }) {
  const executeProductAction = callable(dependencies.executeProductAction, 'dependencies.executeProductAction');
  const results = [];
  for (let actionIndex = 0; actionIndex < scenario.actions.length; actionIndex += 1) {
    const action = scenario.actions[actionIndex];
    if (action.atUtc) clock.advanceTo(action.atUtc);
    const repeatCount = action.repeatCount ?? 1;
    if (action.kind === 'ADVANCE_CLOCK') {
      if (!action.atUtc) fail('SCENARIO_CLOCK_ACTION_INVALID', 'ADVANCE_CLOCK requires atUtc.');
      results.push(deepFreeze({
        actionNumber: actionIndex + 1,
        repetition: 1,
        kind: action.kind,
        atUtc: clock.nowUtc(),
        owner: 'TEST_FIXED_CLOCK',
        result: { advanced: true },
      }));
      continue;
    }
    for (let repetition = 1; repetition <= repeatCount; repetition += 1) {
      const ownerResult = normalizeActionOwner(await executeProductAction({
        scenario,
        action: cloneJson(action),
        actionNumber: actionIndex + 1,
        repetition,
        nowUtc: clock.nowUtc(),
        generatedSources,
        effects,
      }), action.kind);
      results.push(deepFreeze({
        actionNumber: actionIndex + 1,
        repetition,
        kind: action.kind,
        atUtc: clock.nowUtc(),
        owner: ownerResult.owner,
        result: ownerResult.result,
      }));
    }
  }
  return deepFreeze(results);
}

function outboxSummary(effects) {
  const summary = effects.summary();
  return deepFreeze({
    email: summary.email.count,
    push: summary.push.count,
    provider: summary.provider.count,
    r2: summary.r2.count,
    digest: canonicalDigest(summary),
  });
}

/**
 * Execute a validated scenario through injected real product owners.  The
 * harness owns inputs, time and assertions only.  It has no route for seeding
 * source movements, invoices, protected hours or C1 outcomes.
 */
export async function runWeeklySourceScenario({ scenario, dependencies, resultPath = null }) {
  requireObject(scenario, 'scenario');
  requireObject(dependencies, 'dependencies');
  const effects = dependencies.effects ?? createExternalEffectFakes(dependencies.effectOutcomes);
  const clock = createFixedClock(scenario.clock.initialUtc);
  const foundationPlan = buildFoundationRecordPlan(scenario);
  auditFoundationRecordPlan(scenario, foundationPlan);
  const oracle = buildExpectedOutcomeOracle(scenario);
  // Create the owned workspace only after all pure scenario checks pass so a
  // malformed input cannot leave a directory before the cleanup guard exists.
  const workspace = await createScenarioWorkspace(scenario.scenarioId, dependencies.workspaceOptions);
  let productCleanupProof = null;
  let workspaceCleanupProof = null;
  let completed = false;
  let pendingEnvelopeInput = null;
  try {
    await withExternalNetworkDenied(async ({ networkAttempts }) => {
      const applyPrerequisites = callable(dependencies.foundation?.applyPrerequisites, 'foundation.applyPrerequisites');
      const auditPrerequisites = callable(dependencies.foundation?.auditPrerequisites, 'foundation.auditPrerequisites');
      await applyPrerequisites({ scenario, plan: foundationPlan });
      const foundationCertificate = assertFoundationCertificate(
        foundationPlan,
        await auditPrerequisites({ scenario, plan: foundationPlan }),
      );
      const generatedSources = await buildAndParseSources({ scenario, dependencies, workspace });
      const actionResults = await executeActions({ scenario, dependencies, clock, generatedSources, effects });
      effects.assertAllDeclaredOutcomesUsed();
      if (networkAttempts() !== 0) {
        fail('SCENARIO_EXTERNAL_NETWORK_ATTEMPTED', 'The scenario attempted external network access.');
      }
      const observed = requireObject(await callable(dependencies.collectObservedState, 'dependencies.collectObservedState')({
        scenario,
        nowUtc: clock.nowUtc(),
        actionResults,
      }), 'Observed scenario state');
      const projections = await callable(dependencies.collectProjections, 'dependencies.collectProjections')({
        scenario,
        nowUtc: clock.nowUtc(),
      });
      const c1 = dependencies.collectC1
        ? await dependencies.collectC1({ scenario, actionResults })
        : { category: 'NONE', requestDigest: null, emulator: false };
      const coverage = requireObject(await callable(dependencies.collectCoverage, 'dependencies.collectCoverage')({
        scenario,
        actionResults,
      }), 'Scenario coverage');
      const assertion = assertScenarioResult({
        scenario,
        oracle,
        actual: observed,
        projections,
        c1,
        generatedSources,
        executedAcceptanceIds: coverage.acceptanceIds,
        executedProtectedIds: coverage.protectedIds,
      });
      pendingEnvelopeInput = {
        scenario,
        repositories: await callable(dependencies.repositoryEvidence, 'dependencies.repositoryEvidence')(),
        database: await callable(dependencies.databaseEvidence, 'dependencies.databaseEvidence')(),
        generatedSources,
        parser: {
          mode: 'PRODUCT_PARSER_ROUND_TRIP',
          normalFormDigest: canonicalDigest(generatedSources.map((item) => item.parserNormalFormDigest)),
        },
        clockValuesUtc: clock.history(),
        executedOwners: actionResults.map((item) => ({ kind: item.kind, name: item.owner })),
        oracle,
        actual: observed,
        comparison: assertion.comparison,
        c1: cloneJson(c1),
        outbox: outboxSummary(effects),
        projectionDigests: assertion.projectionDigests,
        acceptanceIds: assertion.acceptanceIds,
        protectedIds: assertion.protectedIds,
        modelIds: coverage.modelIds ?? [],
        foundation: foundationCertificate,
        cleanup: null,
      };
      completed = true;
    }, { allowedLoopbackOrigins: dependencies.allowedLoopbackOrigins ?? [] });
  } finally {
    let cleanupError = null;
    try {
      productCleanupProof = await callable(dependencies.cleanupScenario, 'dependencies.cleanupScenario')({
        scenario,
        completed,
      });
      if (productCleanupProof?.complete !== true) {
        fail('SCENARIO_PRODUCT_CLEANUP_INCOMPLETE', 'The product adapter did not prove fresh cleanup.');
      }
    } catch (error) {
      cleanupError = error;
    }
    try {
      workspaceCleanupProof = await cleanupScenarioWorkspace(workspace);
    } catch (error) {
      cleanupError ??= error;
    }
    if (cleanupError) throw cleanupError;
  }

  // The return inside the guarded block is deliberately replaced only after
  // cleanup.  This makes it impossible to emit PASS evidence before residue is
  // proved absent.
  const cleanup = deepFreeze({
    complete: true,
    product: cloneJson(productCleanupProof),
    workspace: cloneJson(workspaceCleanupProof),
  });
  const envelope = createResultEnvelope({ ...pendingEnvelopeInput, cleanup });
  if (resultPath) await writeResultEnvelope(resultPath, envelope);
  return envelope;
}
