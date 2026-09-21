import assert from 'node:assert/strict';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { parseWeeklySourceFile, WEEKLY_SOURCE_PROFILE_IDS } from '../../../broker/src/weekly-source/index.js';
import { buildFoundationRecordPlan } from './foundation-record-builder.mjs';
import { auditFoundationRecordPlan } from './foundation-record-audit.mjs';
import { buildExpectedSourceModel } from './expected-source-model.mjs';
import { loadScenariosFromDirectory } from './scenario-loader.mjs';
import { buildSourceFile } from './source-file-builder.mjs';
import {
  buildRealWorldActionExecutionPlan,
  REAL_WORLD_DATABASE_MODES,
  REAL_WORLD_REQUIRED_OBSERVATIONS,
} from './real-world-action-contract.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const scenarioDirectory = path.resolve(here, '../../fixtures/weekly-source/scenarios');

const fixedExpenseProfile = Object.freeze({
  profileId: 'SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_V1',
  defaultRateFamily: 'STD',
  unitCostByRateFamilyPence: { STD: '2000' },
  businessHierarchy: { grandParent: 'Scenario group', parent: 'Scenario parent', unit: 'Scenario unit' },
  agencyDisplayName: 'Scenario Agency',
});

function parseProfile(upload) {
  if (upload.profile === 'NHSP_PREFINAL_RELEASED_V1') return WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1;
  if (upload.profile === 'NHSP_FINAL_BACKING_V1') return WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1;
  if (upload.profile.endsWith('LAYOUT_A_V1')) return WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1;
  if (upload.profile.endsWith('LAYOUT_B_V1')) return WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1;
  if (upload.profile === 'GENERIC_WEEKLY_COMPLETE_V1') return WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1;
  throw new Error(`No real parser mapping for ${upload.profile}`);
}

function builderOptions(upload) {
  if (upload.profile === 'NHSP_FINAL_BACKING_V1') return { nhspReportHeadingName: 'Scenario Agency' };
  if (upload.profile === 'GENERIC_WEEKLY_COMPLETE_V1') return { genericProfile: fixedExpenseProfile };
  return {};
}

function parserOptions(upload) {
  const options = { profileId: parseProfile(upload) };
  if (upload.profile === 'NHSP_FINAL_BACKING_V1') options.configuredNhspReportHeadingName = 'Scenario Agency';
  return options;
}

test('real-world scenarios are populated, deterministic and declare observable business outcomes', async () => {
  const loaded = await loadScenariosFromDirectory(scenarioDirectory);
  const scenarios = loaded.scenarios.filter((scenario) => scenario.tags?.includes('REAL_WORLD'));
  assert.deepEqual(scenarios.map((scenario) => scenario.scenarioId), [
    'WS-REAL-WORLD-NHSP-001',
    'WS-REAL-WORLD-ROSTER-001',
  ]);
  for (const scenario of scenarios) {
    assert.ok(scenario.foundation.candidates.length > 0, `${scenario.scenarioId} has a Candidate`);
    assert.ok(scenario.foundation.clients.length > 0, `${scenario.scenarioId} has a Client`);
    assert.ok(scenario.foundation.contracts.length > 0, `${scenario.scenarioId} has a Contract`);
    assert.ok(scenario.foundation.timesheets.length > 0, `${scenario.scenarioId} has a Timesheet`);
    assert.ok(scenario.sourceUploads.length > 0, `${scenario.scenarioId} has source uploads`);
    assert.ok(scenario.actions.length > 0, `${scenario.scenarioId} has Office actions`);
    assert.ok(scenario.expected.sourceMovements.length > 0, `${scenario.scenarioId} declares source movements`);
    assert.ok(scenario.expected.invoiceLines.length > 0, `${scenario.scenarioId} declares invoice lines`);
    assert.ok(scenario.expected.approvedHours.length > 0, `${scenario.scenarioId} declares approved hours`);
    const plan = buildFoundationRecordPlan(scenario);
    const audit = auditFoundationRecordPlan(scenario, plan);
    assert.equal(audit.scenarioId, scenario.scenarioId);
    assert.match(audit.certificateDigest, /^[a-f0-9]{64}$/);
    assert.equal(plan.stages.length, 11);
    const execution = buildRealWorldActionExecutionPlan(scenario);
    assert.deepEqual(execution.modes, REAL_WORLD_DATABASE_MODES);
    assert.deepEqual(execution.requiredObservations, REAL_WORLD_REQUIRED_OBSERVATIONS);
    assert.equal(execution.actions.length, scenario.actions.length);
    assert.equal(execution.parserOnlyCanPass, false);
    assert.equal(execution.expectedMayBeCopiedToActual, false);
    assert(execution.actions.every((action) => action.owners.length > 0));
    assert(execution.actions.every((action) => action.observes.length > 0));
  }
});

test('the real-world fixtures cover every action kind that the database journey adapter must execute', async () => {
  const loaded = await loadScenariosFromDirectory(scenarioDirectory);
  const plans = loaded.scenarios
    .filter((scenario) => scenario.tags?.includes('REAL_WORLD'))
    .map(buildRealWorldActionExecutionPlan);
  const kinds = [...new Set(plans.flatMap((plan) => plan.actions.map((action) => action.kind)))].sort();
  assert.deepEqual(kinds, [
    'AUTHORISE',
    'CONFIRM_CURRENT_UPLOAD',
    'CREATE_INVOICE_BATCH',
    'FINALISE',
    'PROTECT_HOURS',
    'READ_PROJECTION',
    'RECONCILE_PROTECTED_HOURS',
    'UPLOAD_SOURCE',
  ]);
});

test('real-world generated source files pass the production parsers with exact Actual hours, signs and source money', async () => {
  const loaded = await loadScenariosFromDirectory(scenarioDirectory);
  for (const scenario of loaded.scenarios.filter((item) => item.tags?.includes('REAL_WORLD'))) {
    for (const upload of scenario.sourceUploads) {
      const expected = buildExpectedSourceModel(scenario, upload);
      const artifact = buildSourceFile(scenario, upload, builderOptions(upload));
      const parsed = await parseWeeklySourceFile(artifact.bytes, parserOptions(upload));
      assert.equal(parsed.ok, true, `${scenario.scenarioId}/${upload.key} is accepted by the production parser`);
      assert.equal(parsed.rows.length, expected.rows.length, `${scenario.scenarioId}/${upload.key} row count`);
      for (const [index, row] of expected.rows.entries()) {
        const actual = parsed.rows[index];
        assert.equal(
          actual.actual?.totalMinutes ?? actual.wholeShiftInputs?.paidMinutes,
          row.actualWorkedMinutes,
          `${upload.key}/${row.sourceRowKey} Actual minutes`,
        );
        if (upload.profile === 'NHSP_FINAL_BACKING_V1') {
          assert.equal(actual.pricingEvidence.sourceTotalPence, row.nhspSignedInvoiceExVatPence, `${upload.key}/${row.sourceRowKey} signed source total`);
          assert.equal(
            actual.rowKind,
            row.sign === 'FULL_NEGATIVE' ? 'FULL_REVERSAL' : 'POSITIVE_SOURCE_SHIFT',
            `${upload.key}/${row.sourceRowKey} physical sign`,
          );
        }
        if (upload.profile === 'GENERIC_WEEKLY_COMPLETE_V1') {
          assert.equal(actual.sourceFixedExpense.pence, row.sourceExpensePence, `${upload.key}/${row.sourceRowKey} source-fixed expense`);
        }
      }
    }
  }
});
