import { buildHealthRosterWorkbook } from './healthroster-workbook-common.mjs';
import { requireScenarioUpload, SourceFixtureError } from './workbook-fixture-utils.mjs';

const PROFILES = new Set([
  'HEALTHROSTER_SELF_BILL_LAYOUT_A_V1',
  'HEALTHROSTER_TIMESHEET_AUTHORITY_LAYOUT_A_V1'
]);

export function buildHealthRosterLayoutAWorkbook(scenario, uploadOrKey) {
  const context = requireScenarioUpload(scenario, uploadOrKey);
  if (!PROFILES.has(context.upload.profile)) {
    throw new SourceFixtureError('HEALTHROSTER_LAYOUT_A_PROFILE_REQUIRED', 'This writer accepts only a versioned HealthRoster Layout A profile');
  }
  return buildHealthRosterWorkbook({ scenario, ...context, layout: 'A' });
}

