import { deepFreeze } from './canonical-json.mjs';

const id = (number) => `SPI-${String(number).padStart(3, '0')}`;
const range = (first, last) => Array.from({ length: last - first + 1 }, (_, offset) => id(first + offset));

const evidence = Object.freeze({
  parser: 'WS-SERVICE-PARSER-PROFILES-001',
  databaseComponentNew: 'WS-DATABASE-COMPONENT-NEW-PG17-001',
  databaseComponentUpgrade: 'WS-DATABASE-COMPONENT-UPGRADE-PG17-001',
  protectedCrossSurface: 'WS-DIFFERENTIAL-PROTECTED-29-001',
  mytms: 'WS-MYTMS-CANDIDATE-MANAGER-EXACT-001',
  office: 'WS-BROWSER-OFFICE-COMPLETE-R26-001',
});

const databaseComponentEvidence = Object.freeze([
  evidence.databaseComponentNew,
  evidence.databaseComponentUpgrade,
]);

const local = (ids, evidenceGroup, requiredEvidenceScenarioIds) => ids.map((scenarioId) => ({
  scenarioId,
  owner: 'WEEKLY_SOURCE',
  status: 'EXECUTION_REQUIRED',
  evidenceGroup,
  requiredEvidenceScenarioIds,
}));

const superseded = (ids, evidenceGroup, requiredEvidenceScenarioIds, supersededBy) => ids.map((scenarioId) => ({
  scenarioId,
  owner: 'WEEKLY_SOURCE',
  status: 'SUPERSEDED_NOT_APPLICABLE',
  supersededBy,
  evidenceGroup,
  requiredEvidenceScenarioIds,
}));

const handover2 = (ids) => ids.map((scenarioId) => ({
  scenarioId,
  owner: 'HANDOVER2',
  status: 'EXTERNAL_HANDOVER2_PENDING',
  evidenceGroup: 'handover2-financial-boundary',
  requiredEvidenceScenarioIds: [],
}));

const shared = (ids, evidenceGroup, requiredEvidenceScenarioIds) => ids.map((scenarioId) => ({
  scenarioId,
  owner: 'SHARED_WEEKLY_SOURCE_AND_HANDOVER2',
  status: 'LOCAL_EXECUTION_AND_EXTERNAL_RESULT_REQUIRED',
  evidenceGroup,
  requiredEvidenceScenarioIds,
  externalOwner: 'HANDOVER2',
}));

// One row for every sealed source/pay/invoice scenario.  This is deliberately
// independent of the prose gate labels: a result may count a local SPI id only
// after every named executable envelope for its route has passed.  HANDOVER 2
// rows are never claimed by a Weekly Source adapter.
export const WEEKLY_SOURCE_SPI_EXECUTION_MAP = deepFreeze([
  ...shared(range(1, 18), 'shared-local-limbs', []),
  ...local(range(19, 28), 'source-ingestion', [evidence.parser, ...databaseComponentEvidence]),
  ...superseded([id(29)], 'source-ingestion', [evidence.parser, ...databaseComponentEvidence], 'PHD-014'),
  ...local([id(30)], 'source-ingestion', [evidence.parser, ...databaseComponentEvidence]),
  ...local(range(31, 40), 'finalisation-and-pay', [evidence.parser, ...databaseComponentEvidence]),
  ...local(range(41, 45), 'mode-a', [evidence.parser, ...databaseComponentEvidence]),
  ...local(range(46, 47), 'protected-differential', [evidence.protectedCrossSurface]),
  ...local(range(48, 60), 'invoice-and-correction', databaseComponentEvidence),
  ...local(range(61, 64), 'mytms-candidate', [evidence.mytms]),
  ...shared(range(65, 91), 'shared-local-limbs', []),
  ...local([id(92)], 'mytms-candidate', [evidence.mytms]),
  ...shared(range(93, 106), 'shared-local-limbs', []),
]);

function exactSpiOutcomes(envelope) {
  const declared = new Set(envelope?.spiIds ?? []);
  const outcomes = Array.isArray(envelope?.actual?.spiOutcomes)
    ? envelope.actual.spiOutcomes
    : [];
  return outcomes.filter((outcome) => {
    if (!outcome || outcome.localResult !== 'PASS' || !declared.has(outcome.scenarioId)) return false;
    const observations = outcome.observations;
    return observations && typeof observations === 'object'
      && Array.isArray(observations.executedActions) && observations.executedActions.length > 0
      && Array.isArray(observations.databaseReadbacks) && observations.databaseReadbacks.length > 0
      && Array.isArray(observations.prohibitedOutcomeChecks) && observations.prohibitedOutcomeChecks.length > 0
      && typeof observations.actualInvoiceResult === 'string' && observations.actualInvoiceResult.length > 0
      && typeof observations.actualOfficeAuditResult === 'string' && observations.actualOfficeAuditResult.length > 0;
  });
}

export function spiIdsForEvidenceGroup(group) {
  return WEEKLY_SOURCE_SPI_EXECUTION_MAP
    .filter((row) => row.owner !== 'HANDOVER2' && row.evidenceGroup === group)
    .map((row) => row.scenarioId);
}

export function assertSpiLedgerMatchesExecutionMap(spiRows) {
  const ledgerIds = spiRows.map((row) => row.scenario_id).sort();
  const mappedIds = WEEKLY_SOURCE_SPI_EXECUTION_MAP.map((row) => row.scenarioId).sort();
  if (ledgerIds.length !== 106 || new Set(ledgerIds).size !== 106) {
    throw Object.assign(new Error('The sealed SPI ledger must contain 106 unique scenario ids.'), {
      code: 'SPI_EXECUTION_LEDGER_INVALID',
    });
  }
  if (JSON.stringify(ledgerIds) !== JSON.stringify(mappedIds)) {
    throw Object.assign(new Error('The SPI execution map is not an exact one-to-one copy of the sealed scenario ids.'), {
      code: 'SPI_EXECUTION_MAP_DRIFT',
    });
  }
  return true;
}

export function verifySpiExecutionEvidence(spiRows, resultEnvelopes, { allowExternalHandover2 = false } = {}) {
  assertSpiLedgerMatchesExecutionMap(spiRows);
  const passingScenarioIds = new Set(resultEnvelopes
    .filter((envelope) => envelope?.schemaVersion === 'WEEKLY_SOURCE_TEST_RESULT_V1' && envelope.status === 'PASS')
    .map((envelope) => envelope.scenarioId));
  // An id in `spiIds` is only a declaration of intended coverage.  It is not
  // proof.  Credit is granted only when the same passing envelope contains a
  // separately observed per-SPI outcome with executed actions, independent
  // database read-backs and prohibited-outcome checks.  This prevents a broad
  // parser/component profile from promoting dozens of unexecuted scenarios.
  const exactClaims = new Map();
  for (const envelope of resultEnvelopes.filter((item) => item?.schemaVersion === 'WEEKLY_SOURCE_TEST_RESULT_V1' && item.status === 'PASS')) {
    for (const outcome of exactSpiOutcomes(envelope)) {
      const claims = exactClaims.get(outcome.scenarioId) ?? [];
      claims.push({ envelopeScenarioId: envelope.scenarioId, outcome });
      exactClaims.set(outcome.scenarioId, claims);
    }
  }
  const rows = WEEKLY_SOURCE_SPI_EXECUTION_MAP.map((mapping) => {
    if (mapping.owner === 'HANDOVER2') {
      const claimed = exactClaims.has(mapping.scenarioId);
      return {
        ...mapping,
        result: claimed ? 'PASS' : (allowExternalHandover2 ? 'EXTERNAL_HANDOVER2_PENDING' : 'MISSING'),
        missingEvidenceScenarioIds: [],
        claimed,
      };
    }
    const missingEvidenceScenarioIds = mapping.requiredEvidenceScenarioIds
      .filter((scenarioId) => !passingScenarioIds.has(scenarioId));
    const claimed = exactClaims.has(mapping.scenarioId);
    const localResult = claimed && missingEvidenceScenarioIds.length === 0
      ? (mapping.status === 'SUPERSEDED_NOT_APPLICABLE' ? 'NOT_APPLICABLE' : 'PASS')
      : 'MISSING';
    const sharedBoundary = mapping.owner === 'SHARED_WEEKLY_SOURCE_AND_HANDOVER2';
    const result = localResult === 'MISSING'
      ? 'MISSING'
      : (sharedBoundary ? 'EXTERNAL_HANDOVER2_PENDING' : localResult);
    return {
      ...mapping,
      result,
      localResult,
      externalResult: sharedBoundary ? 'EXTERNAL_HANDOVER2_PENDING' : 'NOT_APPLICABLE',
      missingEvidenceScenarioIds,
      claimed,
      exactClaimingEvidenceScenarioIds: (exactClaims.get(mapping.scenarioId) ?? [])
        .map((claim) => claim.envelopeScenarioId),
    };
  });
  const localMissing = rows.filter((row) => row.owner !== 'HANDOVER2' && !['PASS', 'NOT_APPLICABLE'].includes(row.localResult ?? row.result));
  const externalMissing = rows.filter((row) => row.owner === 'HANDOVER2' && row.result === 'MISSING');
  const sharedExternalPending = rows.filter((row) => row.owner === 'SHARED_WEEKLY_SOURCE_AND_HANDOVER2' && row.externalResult === 'EXTERNAL_HANDOVER2_PENDING');
  const localSpiReady = localMissing.length === 0;
  return deepFreeze({
    complete: localSpiReady && externalMissing.length === 0 && sharedExternalPending.length === 0,
    weeklySourceSpiLocalReady: localSpiReady,
    // The SPI ledger is only one part of the controlling completion matrix.
    // It must never independently declare the whole plug-in ready.
    weeklySourcePluginReady: false,
    pluginReadinessAuthority: 'COMBINED_IMPLEMENTATION_COMPLETION_MATRIX',
    weeklySourcePassCount: rows.filter((row) => row.owner !== 'HANDOVER2' && row.localResult === 'PASS').length,
    weeklySourceNotApplicableCount: rows.filter((row) => row.owner === 'WEEKLY_SOURCE' && row.result === 'NOT_APPLICABLE').length,
    externalPendingCount: rows.filter((row) => row.result === 'EXTERNAL_HANDOVER2_PENDING').length,
    localMissing,
    externalMissing,
    rows,
  });
}
