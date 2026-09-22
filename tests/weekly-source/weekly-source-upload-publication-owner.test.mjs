import assert from 'node:assert/strict';
import test from 'node:test';
import * as XLSX from 'xlsx';

import {
  parseWeeklySourceFile,
  ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS,
  WEEKLY_SOURCE_PROFILE_IDS,
} from '../../broker/src/weekly-source/index.js';
import { adaptWeeklySourceParserOutput } from '../../broker/src/weekly-source/upload-staging-adapter.mjs';
import {
  buildWeeklySourceProjectionRows,
  createWeeklySourceUploadPublicationOwner,
} from '../../broker/src/weekly-source/upload-publication-owner.mjs';

const ID = Object.freeze({
  actor: '10000000-0000-4000-8000-000000000001',
  agency: '10000000-0000-4000-8000-000000000002',
  group: '10000000-0000-4000-8000-000000000003',
  cycle: '10000000-0000-4000-8000-000000000004',
  client: '10000000-0000-4000-8000-000000000005',
  upload: '10000000-0000-4000-8000-000000000006',
  publication: '10000000-0000-4000-8000-000000000007',
  rebuiltPublication: '10000000-0000-4000-8000-00000000000a',
  uploadRow: '10000000-0000-4000-8000-000000000008',
  correction: '10000000-0000-4000-8000-000000000009',
});

function workbookBytes(rows, name = 'Export') {
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet(rows), name);
  return XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
}

function nhspRows(final) {
  const rows = [
    [final ? 'Agency Backing Report 123 for Exact Heading Agency' : 'Timesheets Previously Released'],
    [
      'Date', 'Ref Num', 'Agency Worker Name', 'Agency Worker Unique Id', 'Trust', 'Ward', 'Assignment',
      'Contract', null, null, null, 'Actual', null, null, null, 'Commission',
      ...(final ? ['FMC'] : []), 'Total Cost', ...(final ? ['Rate'] : []),
    ],
    [null, null, null, null, null, null, null, 'Start', 'End', 'Break In Minutes', 'Total', 'Start', 'End', 'Break In Minutes', 'Total'],
  ];
  const row = ['2026-09-01', 1001, 'Worker One', 'W1', 'Trust One', 'Ward', 'ROLE', null, null, null, null, '09:00', '17:00', 30, '7:30', 52.5];
  if (final) row.push(0, 209.85, 'Basic'); else row.push(209.85);
  rows.push(row);
  if (final) {
    rows.push(['FrameWork Management Charge', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 0]);
    rows.push(['Total', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 209.85]);
  }
  return rows;
}

function csvBytes() {
  const values = Object.fromEntries(ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS.map((header) => [header, '']));
  Object.assign(values, {
    grand_parent_business_unit_name: 'Group', parent_business_unit_name: 'Parent', business_unit_name: 'Unit',
    Candidate: 'Worker', 'Candidate Uid': 'UID', 'Candidate Id': 'CID', 'Booking Id': 'B1',
    'Timesheet Id': 'T1', Expenses: '12.50', 'Total Hours': '7.50', 'Line ID': 'L1',
    'Business Unit ID': 'BU1', 'Booking Start': '2026-09-01 09:00:00', 'Booking End': '2026-09-01 17:00:00',
  });
  const escape = (value) => /[",\r\n]/.test(value) ? `"${value.replaceAll('"', '""')}"` : value;
  return new TextEncoder().encode(
    `${ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS.map(escape).join(',')}\r\n${ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS.map((key) => escape(values[key])).join(',')}\r\n`,
  );
}

const context = Object.freeze({
  environment: 'TEST', agency_id: ID.agency, source_group_id: ID.group,
  source_cycle_id: ID.cycle, report_scope_id: null, client_id: ID.client, client_name: 'Client A',
});

const body = Object.freeze({
  actor_user_id: ID.actor, original_filename: 'source.xlsx',
  coverage: { start_local_date: '2026-09-01', end_local_date: '2026-09-01' },
});

async function parseCase(profileId) {
  if (profileId === WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1) {
    return parseWeeklySourceFile(workbookBytes(nhspRows(false)), { profileId });
  }
  if (profileId === WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1) {
    return parseWeeklySourceFile(workbookBytes(nhspRows(true)), {
      profileId, configuredNhspReportHeadingName: 'Exact Heading', expectedTrust: 'Trust One',
    });
  }
  if (profileId === WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1) {
    return parseWeeklySourceFile(workbookBytes([
      ['Request Id', 'Staff', 'Date', 'From', 'To', 'Break', 'Start', 'End', 'Actual Break', 'Hours', 'Finalised Date', 'Timesheet Finalised By'],
      ['A1', 'Worker', '2026-09-01', '09:00', '17:00', 30, '09:00', '17:00', 30, '7:30', '2026-09-02', 'Manager'],
    ]), { profileId });
  }
  if (profileId === WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1) {
    return parseWeeklySourceFile(workbookBytes([
      ['Request Id', 'Status', 'Staff', 'Date', 'Start', 'End', 'Actual Start', 'Actual End', 'Actual Break', 'Actual Hours', 'Timesheet Finalised By'],
      ['B1', 'Timesheet Finalised', 'Worker', '2026-09-01', '09:00', '17:00', '09:00', '17:00', 30, '7:30', 'Manager'],
    ]), { profileId });
  }
  return parseWeeklySourceFile(csvBytes(), { profileId });
}

test('strict staging adapter preserves a complete physical census for all five profiles', async () => {
  for (const profileId of Object.values(WEEKLY_SOURCE_PROFILE_IDS)) {
    const parsed = await parseCase(profileId);
    assert.equal(parsed.ok, true, profileId);
    const final = profileId === WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1;
    const adapted = adaptWeeklySourceParserOutput(parsed, {
      ...body,
      ...(final ? { coverage: undefined } : {}),
    }, {
      ...context,
      ...(final ? { report_scope_id: ID.group, client_name: 'Trust One' } : {}),
    });
    assert.equal(adapted.physicalRows.length, parsed.selectedWorksheet.physicalOrdinalSpan, profileId);
    assert.equal(adapted.normalisedRows.length, parsed.rows.length, profileId);
    assert.equal(adapted.beginRequest.physical_row_count,
      adapted.beginRequest.header_count + adapted.beginRequest.trailer_count
      + adapted.beginRequest.continuation_count + adapted.beginRequest.accepted_count);
    assert.ok(adapted.physicalRows.every((row, index) => row.source_row_ordinal === index + 1));
    if (final) {
      assert.deepEqual(adapted.moneyEvidence.map((item) => item.money_field_kind),
        ['COMMISSION', 'TOTAL_COST', 'FMC', 'BOTTOM_TOTAL_COST']);
      assert.equal(adapted.beginRequest.file_metadata_json.nhsp_report_number, '123');
    }
    if (profileId === WEEKLY_SOURCE_PROFILE_IDS.ROSTER_WEEKLY_SUMMARY_ACTUAL_V1) {
      assert.equal(adapted.expenseEvidence[0].parsed_pence, '1250');
      assert.equal(adapted.normalisedRows[0].break_minutes, 30);
    }
  }
});

test('pre-final NHSP unverifiable money remains accepted checking evidence with a seal-compatible parse state', async () => {
  const rows = nhspRows(false);
  rows[3][15] = 'not-money';
  const parsed = await parseWeeklySourceFile(workbookBytes(rows), {
    profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_PREFINAL_RELEASED_V1,
  });
  assert.equal(parsed.ok, true);
  assert.equal(parsed.warnings.length, 1);
  const adapted = adaptWeeklySourceParserOutput(parsed, body, context);
  assert.equal(adapted.normalisedRows[0].source_money_parse_state, 'INVALID');
  assert.deepEqual(adapted.moneyEvidence.map((item) => item.parse_state), ['INVALID', 'INVALID']);
  assert.equal(adapted.normalisedRows[0].source_shift_charge_pence, null);
});

test('NHSP preview resolves the server-owned report heading before parsing', async () => {
  const calls = [];
  const owner = createWeeklySourceUploadPublicationOwner({
    rpc: async (name, args) => {
      calls.push([name, args.p_request ?? args]);
      if (name === 'weekly_source_upload_context_v1') {
        return {
          ok: true,
          ...context,
          authority_scope_version: 1,
          nhsp_report_heading_name: 'Exact Heading',
          client_name: 'Trust One',
        };
      }
      throw new Error(`Unexpected RPC ${name}`);
    },
  });
  const parsed = { ok: true, profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1 };
  const result = await owner.previewUpload({
    body: {
      source_group_id: ID.group,
      source_cycle_id: ID.cycle,
      client_id: ID.client,
      profile_id: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
      parser_options: {
        profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
        configuredNhspReportHeadingName: 'Browser must not control this',
      },
    },
    bytes: new Uint8Array([1]),
    actor: { id: ID.actor },
    parseWeeklySourceFile: async (_bytes, options) => {
      assert.deepEqual(options, {
        profileId: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
        configuredNhspReportHeadingName: 'Exact Heading',
        expectedTrust: 'Trust One',
      });
      return parsed;
    },
  });
  assert.equal(result.parsed, parsed);
  assert.deepEqual(calls.map(([name]) => name), ['weekly_source_upload_context_v1']);
  assert.equal(calls[0][1].operation, 'DISCOVER_SCOPE');
});

test('acceptUpload stages, seals and publishes a complete unresolved census without browser economics', async () => {
  const calls = [];
  let appliedRows;
  const parsed = await parseCase(WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1);
  const rpc = async (name, args) => {
    calls.push(name);
    const request = args.p_request ?? args;
    if (name === 'weekly_source_upload_context_v1' && request.operation === 'DISCOVER_SCOPE') {
      return { ok: true, ...context, authority_scope_version: 1, nhsp_report_heading_name: null };
    }
    if (name === 'weekly_source_upload_stage_begin_atomic_v1') return { ok: true, status: 'STAGING', upload_id: ID.upload };
    if (name === 'weekly_source_upload_stage_rows_atomic_v1') return { ok: true, status: 'STAGING' };
    if (name === 'weekly_source_upload_seal_atomic_v1') {
      return { ok: true, status: 'CURRENT', logical_upload_id: ID.upload, authority_scope_version: 2, row_manifest_hash: '1'.repeat(64) };
    }
    if (name === 'weekly_source_projection_begin_atomic_v1') {
      return { ok: true, status: 'BUILDING', publication_id: ID.publication, authority_scope_version: 2 };
    }
    if (name === 'weekly_source_upload_context_v1' && request.operation === 'BUILD_PROJECTION') {
      return {
        ok: true, ...context, profile_id: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
        upload_id: ID.upload, authority_scope_version: 2,
        rows: [{
          upload_row_id: ID.uploadRow, source_row_ordinal: 2, candidate_match_count: 0,
          candidate_id: null, client_id: ID.client, contracts: [], row_finalisation_state: 'SOURCE_WORKED',
        }],
      };
    }
    if (name === 'weekly_source_projection_rows_apply_atomic_v1') {
      appliedRows = args.p_rows;
      return { ok: true, applied_row_count: 1 };
    }
    if (name === 'weekly_source_projection_publish_atomic_v1') {
      return { ok: true, status: 'CURRENT', publication_id: ID.publication, comparison_manifest_hash: '2'.repeat(64), issue_set_hash: '3'.repeat(64) };
    }
    throw new Error(`Unexpected RPC ${name}`);
  };
  const owner = createWeeklySourceUploadPublicationOwner({ rpc });
  const result = await owner.acceptUpload({
    body: {
      source_group_id: ID.group, source_cycle_id: ID.cycle, client_id: ID.client,
      profile_id: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
      original_filename: 'source.xlsx', coverage: body.coverage,
    },
    bytes: new Uint8Array([1]), actor: { id: ID.actor },
    parseWeeklySourceFile: async (_bytes, options) => {
      assert.deepEqual(options, { profileId: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1, expectedClient: 'Client A' });
      return parsed;
    },
  });
  assert.equal(result.status, 'CURRENT');
  assert.equal(appliedRows.length, 1);
  assert.equal(appliedRows[0].mapping_state, 'CANDIDATE_NOT_FOUND');
  assert.equal(JSON.stringify(appliedRows).includes('gross_pay'), false);
  assert.deepEqual(calls, [
    'weekly_source_upload_context_v1', 'weekly_source_upload_stage_begin_atomic_v1',
    'weekly_source_upload_stage_rows_atomic_v1', 'weekly_source_upload_seal_atomic_v1',
    'weekly_source_projection_begin_atomic_v1', 'weekly_source_upload_context_v1',
    'weekly_source_projection_rows_apply_atomic_v1', 'weekly_source_projection_publish_atomic_v1',
  ]);
});

test('Correct-final exact replay returns the sealed durable proofs and current correction version', async () => {
  const calls = [];
  const parsed = await parseCase(WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1);
  const hashes = { rows: '1'.repeat(64), comparison: '2'.repeat(64), issues: '3'.repeat(64) };
  const projectionContext = {
    ok: true, ...context,
    profile_id: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
    upload_id: ID.upload,
    authority_scope_version: 7,
    correction_session_id: ID.correction,
    correction_session_version: 4,
    projection_publication_id: ID.publication,
    projection_state: 'CORRECTION_READY',
    row_manifest_hash: hashes.rows,
    comparison_manifest_hash: hashes.comparison,
    issue_set_hash: hashes.issues,
    rows: [],
  };
  const rpc = async (name, args) => {
    calls.push(name);
    const request = args.p_request ?? args;
    if (name === 'weekly_source_upload_context_v1' && request.operation === 'DISCOVER_SCOPE') {
      assert.equal(request.source_group_id, null);
      return { ok: true, ...context, authority_scope_version: 7 };
    }
    if (name === 'weekly_source_upload_stage_begin_atomic_v1') {
      assert.equal(request.expected_correction_session_version, 1);
      return { ok: true, status: 'DUPLICATE', logical_upload_id: ID.upload };
    }
    if (name === 'weekly_source_projection_begin_atomic_v1') {
      return {
        ok: true, status: 'CORRECTION_READY', publication_id: ID.publication,
        upload_id: ID.upload, authority_scope_version: 7, idempotent: true,
      };
    }
    if (name === 'weekly_source_upload_context_v1' && request.operation === 'BUILD_PROJECTION') {
      return projectionContext;
    }
    throw new Error(`Unexpected RPC ${name}`);
  };
  const owner = createWeeklySourceUploadPublicationOwner({
    rpc,
    loadFileBytes: async (_env, key) => {
      assert.equal(key, 'weekly-source/correct.xlsx');
      return new Uint8Array([1]);
    },
    parseWeeklySourceFile: async (_bytes, options) => {
      assert.equal(options.profileId, WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1);
      return parsed;
    },
  });
  const result = await owner.stageReplacementSource({
    actor_user_id: ID.actor,
    correction_session_id: ID.correction,
    expected_session_version: 1,
    source_cycle_id: ID.cycle,
    report_scope_id: null,
    replacement_source: {
      file_key: 'weekly-source/correct.xlsx',
      profile_id: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
      coverage: body.coverage,
    },
  });
  assert.deepEqual(result, {
    ok: true,
    status: 'CORRECTION_READY',
    correction_session_id: ID.correction,
    replacement_upload_id: ID.upload,
    replacement_projection_publication_id: ID.publication,
    expected_authority_scope_version: 7,
    expected_row_manifest_hash: hashes.rows,
    expected_comparison_manifest_hash: hashes.comparison,
    expected_issue_set_hash: hashes.issues,
    version: 4,
    idempotent_replay: true,
  });
  assert.deepEqual(calls, [
    'weekly_source_upload_context_v1',
    'weekly_source_upload_stage_begin_atomic_v1',
    'weekly_source_projection_begin_atomic_v1',
    'weekly_source_upload_context_v1',
  ]);
});

test('Correct-final Recheck rebuilds a fresh projection from the immutable upload and current mapping', async () => {
  const calls = [];
  let contextCalls = 0;
  let appliedRows;
  const hashes = {
    rows: '1'.repeat(64),
    priorComparison: '2'.repeat(64),
    priorIssues: '3'.repeat(64),
    comparison: '4'.repeat(64),
    issues: '5'.repeat(64),
  };
  const rpc = async (name, args) => {
    calls.push([name, args]);
    const request = args.p_request ?? args;
    if (name === 'weekly_source_projection_begin_atomic_v1') {
      assert.equal(request.upload_id, ID.upload);
      assert.equal(request.correction_session_id, ID.correction);
      assert.equal(request.expected_correction_session_version, 4);
      assert.equal(request.expected_projection_publication_id, ID.publication);
      assert.equal(request.expected_row_manifest_hash, hashes.rows);
      return {
        ok: true,
        status: 'BUILDING',
        publication_id: ID.rebuiltPublication,
        projection_generation: 8,
        rows_applied: false,
        idempotent: false,
      };
    }
    if (name === 'weekly_source_upload_context_v1') {
      contextCalls += 1;
      assert.equal(request.operation, 'BUILD_PROJECTION');
      assert.equal(request.upload_id, ID.upload);
      return {
        ok: true,
        ...context,
        profile_id: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
        upload_id: ID.upload,
        correction_session_id: ID.correction,
        correction_session_version: contextCalls === 1 ? 4 : 5,
        projection_publication_id: contextCalls === 1 ? ID.publication : ID.rebuiltPublication,
        projection_state: 'CORRECTION_READY',
        row_manifest_hash: hashes.rows,
        comparison_manifest_hash: contextCalls === 1 ? hashes.priorComparison : hashes.comparison,
        issue_set_hash: contextCalls === 1 ? hashes.priorIssues : hashes.issues,
        rows: [],
      };
    }
    if (name === 'weekly_source_projection_rows_apply_atomic_v1') {
      appliedRows = args.p_rows;
      assert.equal(args.p_publication_id, ID.rebuiltPublication);
      return { ok: true, applied_row_count: 0 };
    }
    if (name === 'weekly_source_projection_publish_atomic_v1') {
      assert.equal(request.publication_id, ID.rebuiltPublication);
      return {
        ok: true,
        status: 'CORRECTION_READY',
        publication_id: ID.rebuiltPublication,
        ready_session_version: 5,
        comparison_manifest_hash: hashes.comparison,
        issue_set_hash: hashes.issues,
        idempotent: false,
      };
    }
    throw new Error(`Unexpected RPC ${name}`);
  };
  const owner = createWeeklySourceUploadPublicationOwner({
    rpc,
    loadFileBytes: async () => { throw new Error('Recheck must not reload replacement bytes.'); },
    parseWeeklySourceFile: async () => { throw new Error('Recheck must not parse or restage the upload.'); },
  });
  const result = await owner.rebuildReplacementProjection({
    actor_user_id: ID.actor,
    correction_session_id: ID.correction,
    expected_session_version: 4,
    source_cycle_id: ID.cycle,
    report_scope_id: null,
    replacement_upload_id: ID.upload,
    replacement_projection_publication_id: ID.publication,
    expected_authority_scope_version: 7,
    expected_row_manifest_hash: hashes.rows,
    expected_comparison_manifest_hash: hashes.priorComparison,
    expected_issue_set_hash: hashes.priorIssues,
    idempotency_key: 'correct-final-recheck-build-0001',
  });
  assert.deepEqual(appliedRows, []);
  assert.deepEqual(result, {
    ok: true,
    status: 'CORRECTION_READY',
    correction_session_id: ID.correction,
    replacement_upload_id: ID.upload,
    replacement_projection_publication_id: ID.rebuiltPublication,
    expected_authority_scope_version: 7,
    expected_row_manifest_hash: hashes.rows,
    expected_comparison_manifest_hash: hashes.comparison,
    expected_issue_set_hash: hashes.issues,
    version: 5,
    idempotent_replay: false,
  });
  assert.deepEqual(calls.map(([name]) => name), [
    'weekly_source_projection_begin_atomic_v1',
    'weekly_source_upload_context_v1',
    'weekly_source_projection_rows_apply_atomic_v1',
    'weekly_source_projection_publish_atomic_v1',
    'weekly_source_upload_context_v1',
  ]);
});

test('Correct-final Recheck exactly replays an already-published fresh projection', async () => {
  const calls = [];
  const hashes = {
    rows: '1'.repeat(64),
    priorComparison: '2'.repeat(64),
    priorIssues: '3'.repeat(64),
    comparison: '4'.repeat(64),
    issues: '5'.repeat(64),
  };
  const rpc = async (name, args) => {
    calls.push(name);
    const request = args.p_request ?? args;
    if (name === 'weekly_source_projection_begin_atomic_v1') {
      return {
        ok: true,
        status: 'CORRECTION_READY',
        publication_id: ID.rebuiltPublication,
        ready_session_version: 5,
        comparison_manifest_hash: hashes.comparison,
        issue_set_hash: hashes.issues,
        rows_applied: true,
        idempotent: true,
      };
    }
    if (name === 'weekly_source_upload_context_v1') {
      assert.equal(request.upload_id, ID.upload);
      return {
        ok: true,
        ...context,
        profile_id: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
        upload_id: ID.upload,
        correction_session_id: ID.correction,
        correction_session_version: 5,
        projection_publication_id: ID.rebuiltPublication,
        projection_state: 'CORRECTION_READY',
        row_manifest_hash: hashes.rows,
        comparison_manifest_hash: hashes.comparison,
        issue_set_hash: hashes.issues,
        rows: [],
      };
    }
    throw new Error(`Exact replay must not call ${name}`);
  };
  const owner = createWeeklySourceUploadPublicationOwner({
    rpc,
    loadFileBytes: async () => { throw new Error('Exact replay must not reload bytes.'); },
    parseWeeklySourceFile: async () => { throw new Error('Exact replay must not parse bytes.'); },
  });
  const result = await owner.rebuildReplacementProjection({
    actor_user_id: ID.actor,
    correction_session_id: ID.correction,
    expected_session_version: 4,
    source_cycle_id: ID.cycle,
    report_scope_id: null,
    replacement_upload_id: ID.upload,
    replacement_projection_publication_id: ID.publication,
    expected_authority_scope_version: 7,
    expected_row_manifest_hash: hashes.rows,
    expected_comparison_manifest_hash: hashes.priorComparison,
    expected_issue_set_hash: hashes.priorIssues,
    idempotency_key: 'correct-final-recheck-replay-0001',
  });
  assert.equal(result.replacement_projection_publication_id, ID.rebuiltPublication);
  assert.equal(result.version, 5);
  assert.equal(result.idempotent_replay, true);
  assert.deepEqual(calls, [
    'weekly_source_projection_begin_atomic_v1',
    'weekly_source_upload_context_v1',
  ]);
});

test('ambiguous exact Contracts remain a chooser and a supplied choice is revalidated', () => {
  const contract = (suffix) => ({
    contract_id: `20000000-0000-4000-8000-00000000000${suffix}`,
    candidate_id: ID.actor,
    client_id: ID.client,
    valid_from: '2026-01-01',
    valid_to: null,
    display_label: `Contract ${suffix}`,
    pay_type: 'PAYE',
    weekly_source_applicable: true,
    effective_policy: {
      authority_mode: 'TIMESHEET_AUTHORITY', c1_source_mode: null,
      weekly_rate_classification_method: 'SPLIT_RATE_WINDOWS', policy_sha256: String(suffix).repeat(64),
    },
    settings_authority: { values: {} },
    rates_json: {},
  });
  const projectionContext = {
    profile_id: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1,
    rows: [{
      upload_row_id: ID.uploadRow, source_row_ordinal: 2, candidate_match_count: 1,
      candidate_id: ID.actor, client_id: ID.client, work_date: '2026-09-01',
      row_finalisation_state: 'SOURCE_UNFINALISED', external_source_key: 'A1',
      contracts: [contract(1), contract(2)], prior_accepted_contract_id: null,
    }],
  };
  const chooser = buildWeeklySourceProjectionRows(projectionContext)[0];
  assert.equal(chooser.mapping_state, 'CONTRACT_SELECTION_REQUIRED');
  assert.deepEqual(chooser.qualifying_contract_ids, [contract(1).contract_id, contract(2).contract_id]);

  const selected = buildWeeklySourceProjectionRows(projectionContext, {
    contract_selections: { 2: contract(2).contract_id },
  })[0];
  assert.equal(selected.mapping_state, 'RESOLVED');
  assert.equal(selected.contract_id, contract(2).contract_id);
  assert.equal(selected.contract_selection_method, 'OFFICE_SELECTED');

  assert.throws(() => buildWeeklySourceProjectionRows(projectionContext, {
    contract_selections: { 2: '20000000-0000-4000-8000-000000000099' },
  }), { code: 'WEEKLY_SOURCE_CONTRACT_SELECTION_STALE' });
});

test('resolved source-authority rows are calculated by the canonical rate and economic owners', () => {
  const contractId = '30000000-0000-4000-8000-000000000001';
  const settings = {
    values: {
      timezone_id: 'Europe/London', day_start: '06:00:00', day_end: '20:00:00',
      night_start: '20:00:00', night_end: '06:00:00', sat_start: '00:00:00', sat_end: '00:00:00',
      sun_start: '00:00:00', sun_end: '00:00:00', bh_start: '00:00:00', bh_end: '00:00:00', bh_list: [],
    },
  };
  const rates = {
    paye_day: 10, paye_night: 10, paye_sat: 10, paye_sun: 10, paye_bh: 10,
    charge_day: 20, charge_night: 20, charge_sat: 20, charge_sun: 20, charge_bh: 20,
  };
  const economicContext = {
    profile_id: WEEKLY_SOURCE_PROFILE_IDS.NHSP_FINAL_BACKING_V1,
    rows: [{
      upload_row_id: ID.uploadRow, source_row_ordinal: 4, external_source_key: 'REF-1',
      candidate_match_count: 1, candidate_id: ID.actor, client_id: ID.client,
      work_date: '2026-09-01', start_at_local: '2026-09-01T09:00:00',
      end_at_local: '2026-09-01T17:00:00', break_minutes: 30, actual_net_minutes: 450,
      row_finalisation_state: 'SOURCE_WORKED', source_commission_pence: '0',
      source_total_cost_pence: '15000', source_shift_charge_pence: '15000',
      contracts: [{
        contract_id: contractId, candidate_id: ID.actor, client_id: ID.client,
        valid_from: '2026-01-01', valid_to: null, display_label: 'RGN', pay_type: 'PAYE',
        rates_json: rates, contract_updated_at: '2026-09-01T00:00:00Z', weekly_source_applicable: true,
        effective_policy: {
          authority_mode: 'SOURCE_AUTHORITY', self_bill_enabled: true, c1_source_mode: 'NHSP_WEEKLY',
          weekly_rate_classification_method: 'SPLIT_RATE_WINDOWS',
          duration_break_tie_rule: 'EARLIEST_LONGEST_PORTION', policy_sha256: 'a'.repeat(64),
        },
        settings_authority: settings,
      }],
      prior_accepted_contract_id: null, prior_work_event_id: null,
    }],
  };
  const resolved = buildWeeklySourceProjectionRows(economicContext)[0];
  assert.equal(resolved.mapping_state, 'RESOLVED');
  assert.equal(resolved.contract_id, contractId);
  assert.equal(resolved.economic_snapshot.total_pay_pence, '7500');
  assert.equal(resolved.economic_snapshot.calculated_charge_pence, '15000');
  assert.deepEqual(resolved.economic_snapshot.bucket_minutes,
    { day: 450, night: 0, sat: 0, sun: 0, bh: 0 });
  assert.equal(resolved.qualification_observations[0].comparison_result, 'EXACT');
  assert.equal(resolved.charge_check.comparison_result, 'EXACT');
  assert.equal(resolved.link_kind, 'POSITIVE_SOURCE');
});

test('split-rate projection uses the policy defaults when optional rate-window settings are blank', () => {
  const contractId = '30000000-0000-4000-8000-000000000002';
  const resolved = buildWeeklySourceProjectionRows({
    profile_id: WEEKLY_SOURCE_PROFILE_IDS.HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1,
    rows: [{
      upload_row_id: ID.uploadRow, source_row_ordinal: 2, external_source_key: 'HR-DEFAULT-1',
      candidate_match_count: 1, candidate_id: ID.actor, client_id: ID.client,
      work_date: '2026-09-14', start_at_local: '2026-09-14T20:00:00',
      end_at_local: '2026-09-15T08:00:00', break_minutes: 60, actual_net_minutes: 660,
      row_finalisation_state: 'SOURCE_WORKED',
      contracts: [{
        contract_id: contractId, candidate_id: ID.actor, client_id: ID.client,
        valid_from: '2026-01-01', valid_to: null, display_label: 'Band 5', pay_type: 'PAYE',
        rates_json: {
          paye_day: 10, paye_night: 12, paye_sat: 13, paye_sun: 14, paye_bh: 15,
          charge_day: 20, charge_night: 24, charge_sat: 26, charge_sun: 28, charge_bh: 30,
        },
        contract_updated_at: '2026-09-01T00:00:00Z', weekly_source_applicable: true,
        effective_policy: {
          authority_mode: 'SOURCE_AUTHORITY', self_bill_enabled: true, c1_source_mode: 'HEALTHROSTER_WEEKLY',
          weekly_rate_classification_method: 'SPLIT_RATE_WINDOWS',
          duration_break_tie_rule: 'EARLIEST_LONGEST_PORTION', policy_sha256: 'b'.repeat(64),
        },
        settings_authority: { values: { timezone_id: 'Europe/London' } },
      }],
      prior_accepted_contract_id: null, prior_work_event_id: null,
    }],
  })[0];
  assert.equal(resolved.mapping_state, 'RESOLVED');
  assert.deepEqual(resolved.economic_snapshot.bucket_minutes,
    { day: 120, night: 540, sat: 0, sun: 0, bh: 0 });
  assert.equal(resolved.economic_snapshot.calculated_charge_pence, '25600');
});
