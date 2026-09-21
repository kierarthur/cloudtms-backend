import { buildNhspWorkbook } from './nhsp-workbook-common.mjs';
import { requireScenarioUpload, SourceFixtureError } from './workbook-fixture-utils.mjs';

export function buildNhspBackingReport(scenario, uploadOrKey, { reportHeadingName } = {}) {
  const context = requireScenarioUpload(scenario, uploadOrKey);
  if (context.upload.profile !== 'NHSP_FINAL_BACKING_V1' || context.upload.stage !== 'FINAL') {
    throw new SourceFixtureError('NHSP_FINAL_PROFILE_REQUIRED', 'This writer accepts only the versioned NHSP final backing-report profile');
  }
  return buildNhspWorkbook({ scenario, ...context, final: true, reportHeadingName });
}
