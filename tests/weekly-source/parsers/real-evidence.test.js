import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

import { parseWeeklySourceFile, WEEKLY_SOURCE_PROFILE_IDS } from '../../../broker/src/weekly-source/index.js';
import { adaptWeeklySourceParserOutput } from '../../../broker/src/weekly-source/upload-staging-adapter.mjs';

const evidenceRoot = process.env.CLOUDTMS_WEEKLY_SOURCE_EVIDENCE_DIR;
const evidenceAvailable = Boolean(evidenceRoot && fs.existsSync(evidenceRoot));
const step6EvidenceRoot = process.env.CLOUDTMS_WEEKLY_SOURCE_STEP6_EVIDENCE_DIR;
const step6EvidenceAvailable = Boolean(step6EvidenceRoot && fs.existsSync(step6EvidenceRoot));

test('locked restricted source workbooks satisfy their exact profiles and censuses', { skip: !evidenceAvailable }, async () => {
  const cases = [
    {
      relativePath: 'nhsp/NHSP released shifts example.xlsx',
      profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
      sha256: '32f956ca76db807ef8aa9b60951371d30601e7bba50878a453a407a804bbfe2b',
      expected: { economic: 308, fullReversals: 0, wardContinuations: 308 },
      worksheet: 'Export (1)', physicalRows: 619,
      columns: { actualStart: 'L', actualEnd: 'M', actualBreak: 'N', actualTotal: 'O', commission: 'P', totalCost: 'Q' },
    },
    {
      relativePath: 'nhsp/BACKING REPORT 1.xlsx',
      profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
      configuredNhspReportHeadingName: 'Arthur Rai Medical Servic',
      sha256: '1ee118b5ab2a885a751bcef8f7e374bef595bc1057fc5971b27cd6d145558a1b',
      expected: { economic: 42, fullReversals: 0, wardContinuations: 42 },
      worksheet: 'Export (7)', physicalRows: 89,
      columns: { actualStart: 'L', actualEnd: 'M', actualBreak: 'N', actualTotal: 'O', commission: 'P', fmc: 'Q', totalCost: 'R' },
    },
    {
      relativePath: 'nhsp/BACKING REPORT 2.xlsx',
      profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
      configuredNhspReportHeadingName: 'Arthur Rai Medical Servic',
      sha256: 'b82dfbea99d99169491dae79b3ecbfa9fa1159dd11a499e17e37cc76737d1ba0',
      expected: { economic: 32, fullReversals: 1, wardContinuations: 32 },
      worksheet: 'Export (5)', physicalRows: 69,
      columns: { actualStart: 'L', actualEnd: 'M', actualBreak: 'N', actualTotal: 'O', commission: 'P', fmc: 'Q', totalCost: 'R' },
    },
    {
      relativePath: 'healthroster/Surrey Timesheet Export.xlsx',
      profileId: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
      sha256: 'a61e9aceaf20f24be8273dea5744028626591d285fe967fa7ab9103e54e99c95',
      expected: { total: 327, finalisedWorked: 319, unfinalised: 8, explicitZero: 0, blocked: 0 },
      worksheet: 'Export', physicalRows: 328,
      columns: { actualStart: 'M', actualEnd: 'N', actualBreak: 'O', actualTotal: 'P', finalisedDate: 'S', finalisedBy: 'AY' },
    },
    {
      relativePath: 'healthroster/Whittington FULL.xlsx',
      profileId: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1,
      sha256: '853484e262e8e2501202bd9044469cd00e8fdb3a82a5c30d5ee664eb8ad924de',
      expected: { total: 27, finalisedWorked: 26, unfinalised: 1, explicitZero: 0, blocked: 0 },
      worksheet: 'Export', physicalRows: 28,
      columns: { actualStart: 'AU', actualEnd: 'AV', actualBreak: 'AW', actualTotal: 'AY', finalisedBy: 'BM' },
    },
    {
      relativePath: 'other-self-bill/Source-fixed expense example.csv',
      profileId: WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1,
      sha256: 'd45414ec0571e04fb69ce08a4bda14aa812e8a85e5dda5c5ffd3a1d421e9228e',
      expected: { total: 12, sourceShifts: 7, sourceZero: 5, sourceExpensePresent: 12, sourceExpenseOmittedZero: 0 },
      worksheet: 'CSV', physicalRows: 13,
      columns: { expenses: 'AH', totalHours: 'AI', lineId: 'AK', bookingStart: 'BK', bookingEnd: 'BL' },
    },
  ];
  for (const fixture of cases) {
    const result = await parseWeeklySourceFile(fs.readFileSync(path.join(evidenceRoot, fixture.relativePath)), fixture);
    assert.equal(result.ok, true, `${fixture.relativePath} must be accepted`);
    assert.equal(result.sourceFileSha256, fixture.sha256);
    assert.equal(result.profileId, fixture.profileId);
    assert.equal(result.profileVersion, '1');
    assert.equal(result.selectedWorksheet.name, fixture.worksheet);
    assert.equal(result.selectedWorksheet.physicalRowCount, fixture.physicalRows);
    for (const [field, column] of Object.entries(fixture.columns)) assert.equal(result.resolvedColumnMap[field].column, column);
    assert.deepEqual(result.rowCounts, fixture.expected);
  }
});

test('the supplied multi-file Excel HTML wrapper is refused', { skip: !evidenceAvailable }, async () => {
  const bytes = fs.readFileSync(path.join(evidenceRoot, 'nhsp/NHSP released shifts example.htm'));
  await assert.rejects(parseWeeklySourceFile(bytes, {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
  }), { code: 'HTML_WRAPPER_NOT_SUPPORTED' });
});

test('Step 6 real reports satisfy the production profiles', { skip: !step6EvidenceAvailable }, async () => {
  const cases = [
    {
      relativePath: 'healthroster/west-london-timesheet-authority.xlsx',
      profileId: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
      sha256: '647a99945d79fa5eea81d99a0fd14df5e726dc6701fc98d720f621b4d5d69b90',
      expected: { total: 10, finalisedWorked: 10, unfinalised: 0, explicitZero: 0, blocked: 0 },
      worksheet: 'Export', physicalRows: 11,
      columns: { actualStart: 'N', actualEnd: 'O', actualBreak: 'P', actualTotal: 'Q', finalisedDate: 'T', finalisedBy: 'AZ' },
    },
    {
      relativePath: 'nhsp/prefinal-previously-released-2026-09-09.xls',
      profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
      sha256: 'baddbaf92d3225e623c7091235d3b594444cb784d18eacc6f31c1a4d93ad148b',
      expected: { economic: 437, fullReversals: 0, wardContinuations: 0 },
      worksheet: 'HTML_TABLE', physicalRows: 440,
      columns: { actualStart: 'L', actualEnd: 'M', actualBreak: 'N', actualTotal: 'O', commission: 'P', totalCost: 'Q' },
    },
    ...[
      ['BR1-2026-09-09.xls', '1937b480083ca77861eee5a23182e8d308271cc4acf1a549fe2cc734d17a75d0', 17, 22],
      ['BR2-2026-09-09.xls', '5b0f181c97d87320410261bd7143209033bd229dc040c1f447b0d608a371bdca', 10, 15],
      ['BR3-2026-09-09.xls', 'b876162030d34ca6fa7dc0543c06e41a9728a499bee5a6c421243b16373f7c05', 2, 7],
    ].map(([file, sha256, economic, physicalRows]) => ({
      relativePath: `nhsp/${file}`,
      profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
      configuredNhspReportHeadingName: 'Arthur Rai Medical Servic',
      sha256,
      expected: { economic, fullReversals: 0, wardContinuations: 0 },
      worksheet: 'HTML_TABLE', physicalRows,
      columns: { actualStart: 'L', actualEnd: 'M', actualBreak: 'N', actualTotal: 'O', commission: 'P', fmc: 'Q', totalCost: 'R' },
    })),
  ];

  for (const fixture of cases) {
    const result = await parseWeeklySourceFile(fs.readFileSync(path.join(step6EvidenceRoot, fixture.relativePath)), fixture);
    assert.equal(result.ok, true, `${fixture.relativePath} must be accepted: ${JSON.stringify(result.fatalErrors)}`);
    assert.equal(result.sourceFileSha256, fixture.sha256);
    assert.equal(result.profileId, fixture.profileId);
    assert.equal(result.selectedWorksheet.name, fixture.worksheet);
    assert.equal(result.selectedWorksheet.physicalRowCount, fixture.physicalRows);
    for (const [field, column] of Object.entries(fixture.columns)) assert.equal(result.resolvedColumnMap[field].column, column);
    assert.deepEqual(result.rowCounts, fixture.expected);
    if (fixture.relativePath.endsWith('.xls')) {
      const staged = adaptWeeklySourceParserOutput(result, {
        actor_user_id: '90000000-0000-4000-8000-000000000001',
        original_filename: path.basename(fixture.relativePath),
      }, {
        environment: 'TEST',
        agency_id: '90000000-0000-4000-8000-000000000002',
        source_group_id: '90000000-0000-4000-8000-000000000030',
        source_cycle_id: '90000000-0000-4000-8000-000000000031',
        report_scope_id: '90000000-0000-4000-8000-000000000050',
        client_id: '90000000-0000-4000-8000-000000000040',
      });
      assert.equal(result.sourceKind, 'HTML');
      assert.equal(result.selectedWorksheet.workbookPartAndSheetFingerprint, null);
      assert.equal(staged.beginRequest.parser_summary_json.source_kind, 'HTML');
      assert.equal(staged.beginRequest.workbook_part_and_sheet_fingerprint, null);
    }
  }
});
