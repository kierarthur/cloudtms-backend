import { canonicalDigest, cloneJson, deepFreeze } from './canonical-json.mjs';
import { DeterministicIdentityRegistry } from './deterministic-identities.mjs';
import { requireScenarioUpload } from './workbook-fixture-utils.mjs';

export function buildExpectedSourceModel(scenario, uploadOrKey) {
  const { upload } = requireScenarioUpload(scenario, uploadOrKey);
  const registry = new DeterministicIdentityRegistry(scenario.scenarioId);
  const nhspFinal = upload.profile === 'NHSP_FINAL_BACKING_V1';
  const rows = upload.physicalRows.map((row, ordinal) => {
    const commission = row.commissionPence === null ? null : BigInt(row.commissionPence);
    const totalCost = row.totalCostPence === null ? null : BigInt(row.totalCostPence);
    return {
      ordinal,
      fixtureRowId: registry.uuid(`expected-source-row:${upload.key}`, ordinal),
      sourceRowKey: row.key,
      candidateKey: row.candidateKey,
      contractKey: row.contractKey ?? null,
      requestId: row.requestId ?? null,
      workDate: row.workDate,
      actualStart: row.actualStart,
      actualEnd: row.actualEnd,
      actualBreakMinutes: row.actualBreakMinutes,
      actualWorkedMinutes: row.actualWorkedMinutes,
      sign: row.sign,
      finalisation: row.finalisation,
      statusText: row.statusText ?? null,
      finalisedBy: row.finalisedBy ?? null,
      sourceExpensePence: row.sourceExpensePence ?? null,
      // For NHSP final reports only, invoice value is the declared signed
      // Commission + Total Cost. Calculated charge is comparison evidence and
      // is intentionally absent from this expected source model.
      nhspSignedInvoiceExVatPence: nhspFinal && commission !== null && totalCost !== null
        ? (commission + totalCost).toString()
        : null,
      pricingEvidence: nhspFinal ? {
        commissionPence: row.commissionPence ?? null,
        totalCostPence: row.totalCostPence ?? null
      } : null
    };
  });
  const modelWithoutDigest = {
    modelVersion: 'WEEKLY_SOURCE_EXPECTED_PHYSICAL_MODEL_V1',
    scenarioId: scenario.scenarioId,
    uploadKey: upload.key,
    profile: upload.profile,
    stage: upload.stage,
    scope: {
      clientKey: upload.clientKey,
      trustName: upload.trustName ?? null,
      reportNumber: upload.reportNumber ?? null,
      cycleUtc: upload.cycleUtc ?? null,
      coverageStart: upload.coverageStart ?? null,
      coverageEnd: upload.coverageEnd ?? null,
      complete: upload.complete ?? null
    },
    mutations: cloneJson(upload.mutations || []),
    expectedAcceptance: (upload.mutations || []).length === 0,
    rows
  };
  return deepFreeze({ ...modelWithoutDigest, modelDigest: canonicalDigest(modelWithoutDigest) });
}

