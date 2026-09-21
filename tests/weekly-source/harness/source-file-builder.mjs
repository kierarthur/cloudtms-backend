import { buildGenericSource } from './generic-source-builder.mjs';
import { buildHealthRosterLayoutAWorkbook } from './healthroster-layout-a-builder.mjs';
import { buildHealthRosterLayoutBWorkbook } from './healthroster-layout-b-builder.mjs';
import { buildNhspBackingReport } from './nhsp-backing-report-builder.mjs';
import { buildNhspPrefinalWorkbook } from './nhsp-prefinal-workbook-builder.mjs';
import { buildSourceFixedExpenseWholeShiftCsv } from './source-fixed-expense-whole-shift-csv-builder.mjs';
import { requireScenarioUpload, SourceFixtureError } from './workbook-fixture-utils.mjs';

const LAYOUT_A = new Set(['HEALTHROSTER_SELF_BILL_LAYOUT_A_V1', 'HEALTHROSTER_TIMESHEET_AUTHORITY_LAYOUT_A_V1']);
const LAYOUT_B = new Set(['HEALTHROSTER_SELF_BILL_LAYOUT_B_V1', 'HEALTHROSTER_TIMESHEET_AUTHORITY_LAYOUT_B_V1']);

export function buildSourceFile(scenario, uploadOrKey, { genericProfile, nhspReportHeadingName } = {}) {
  const { upload } = requireScenarioUpload(scenario, uploadOrKey);
  if (upload.profile === 'NHSP_PREFINAL_RELEASED_V1') return buildNhspPrefinalWorkbook(scenario, upload);
  if (upload.profile === 'NHSP_FINAL_BACKING_V1') {
    return buildNhspBackingReport(scenario, upload, { reportHeadingName: nhspReportHeadingName });
  }
  if (LAYOUT_A.has(upload.profile)) return buildHealthRosterLayoutAWorkbook(scenario, upload);
  if (LAYOUT_B.has(upload.profile)) return buildHealthRosterLayoutBWorkbook(scenario, upload);
  if (upload.profile === 'GENERIC_WEEKLY_COMPLETE_V1') {
    if (genericProfile?.profileId === 'SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_V1') {
      return buildSourceFixedExpenseWholeShiftCsv(scenario, upload, genericProfile);
    }
    return buildGenericSource(scenario, upload, genericProfile);
  }
  if (upload.profile === 'NONE_ORDINARY_WEEKLY' || upload.profile === 'NONE_DAILY') {
    if (upload.physicalRows.length || upload.stage !== 'NONE') {
      throw new SourceFixtureError('NO_SOURCE_PROFILE_HAS_ROWS', 'A no-source scenario cannot declare source rows or an import stage');
    }
    return Object.freeze({
      profile: upload.profile,
      fileName: null,
      mediaType: null,
      sheetName: null,
      bytes: null,
      byteCount: 0,
      sha256: null
    });
  }
  throw new SourceFixtureError('SOURCE_PROFILE_UNSUPPORTED', `No test-only writer is registered for ${upload.profile}`);
}
