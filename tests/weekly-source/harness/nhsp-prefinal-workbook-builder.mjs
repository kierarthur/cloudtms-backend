import { buildNhspWorkbook } from './nhsp-workbook-common.mjs';
import { requireScenarioUpload, SourceFixtureError } from './workbook-fixture-utils.mjs';

export function buildNhspPrefinalWorkbook(scenario, uploadOrKey) {
  const context = requireScenarioUpload(scenario, uploadOrKey);
  if (context.upload.profile !== 'NHSP_PREFINAL_RELEASED_V1' || context.upload.stage !== 'PREFINAL') {
    throw new SourceFixtureError('NHSP_PREFINAL_PROFILE_REQUIRED', 'This writer accepts only the versioned NHSP pre-final profile');
  }
  return buildNhspWorkbook({ scenario, ...context, final: false });
}

