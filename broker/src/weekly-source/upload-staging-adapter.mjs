import { XLSX_BINARY64_SAME_VALUE_PENCE_V1 } from './money.js';

const NHSP = new Set(['NHSP_PREFINAL_RELEASED_V1', 'NHSP_FINAL_BACKING_V1']);
const HEALTHROSTER = new Set([
  'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
  'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
]);

export const WEEKLY_SOURCE_UPLOAD_ADAPTER_VERSION = 'WEEKLY_SOURCE_UPLOAD_ADAPTER_V1';

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

const text = (value) => String(value ?? '').trim();

function addDay(date) {
  const value = new Date(`${date}T12:00:00Z`);
  value.setUTCDate(value.getUTCDate() + 1);
  return value.toISOString().slice(0, 10);
}

function localTimestamp(date, clock, overnight = false) {
  if (!date || !clock) return null;
  return `${overnight ? addDay(date) : date}T${clock}:00`;
}

function sourceEvidenceKind(evidence) {
  const sourceKind = text(evidence?.sourceKind).toUpperCase();
  if (sourceKind === 'CSV') return 'CSV_DECODED_TEXT';
  if (sourceKind === 'HTML') return 'HTML_DECODED_TEXT';
  return evidence?.cellKind === 'NUMBER' || evidence?.cellKind === 'DATE'
    ? 'XLSX_NUMERIC_TOKEN'
    : 'XLSX_STRING_TOKEN';
}

function columnIndex(parsed, field) {
  return Number(parsed.resolvedColumnMap?.[field]?.index ?? 0);
}

function evidenceRecord(parsed, sourceRowOrdinal, field, evidence, parsedPence, parseState, extra = {}) {
  if (!evidence) return null;
  return {
    source_row_ordinal: sourceRowOrdinal,
    source_column_index: columnIndex(parsed, field),
    cell_coordinate: evidence.coordinate ?? null,
    source_kind: sourceEvidenceKind(evidence),
    original_token: String(evidence.originalToken ?? ''),
    decoded_token: String(evidence.decodedToken ?? ''),
    cell_type_marker: evidence.cellKind ?? null,
    formula_present: evidence.formulaPresent === true,
    parse_state: parseState,
    parsed_pence: parsedPence == null ? null : String(parsedPence),
    source_file_sha256: parsed.sourceFileSha256,
    ...extra,
  };
}

function moneyParseState(evidence, issueCode = evidence?.parseIssue) {
  const issue = text(issueCode).toUpperCase();
  if (evidence?.formulaPresent === true) return 'FORMULA';
  if (evidence?.parseState === 'PARSED') return 'VALID';
  if (issue === 'SOURCE_MONEY_BLANK') return 'MISSING';
  if (issue === 'SOURCE_MONEY_EXCESS_PRECISION') return 'EXCESS_PRECISION';
  if (issue === 'SOURCE_MONEY_OUT_OF_RANGE') return 'OVERFLOW';
  if (issue === 'SOURCE_MONEY_UNVERIFIABLE_CELL_TYPE') return 'UNSUPPORTED_CELL_TYPE';
  return 'INVALID';
}

function nhspMoneyEvidence(parsed) {
  const records = [];
  for (const row of parsed.rows ?? []) {
    const components = row.pricingEvidence?.components ?? {};
    const entries = [
      ['COMMISSION', 'commission', components.commission],
      ['TOTAL_COST', 'totalCost', components.totalCost],
      ['FMC', 'fmc', components.fmc],
    ];
    for (const [moneyFieldKind, field, evidence] of entries) {
      if (!evidence) continue;
      records.push({
        ...evidenceRecord(parsed, row.physicalRow, field, evidence, evidence.parsedPence, moneyParseState(evidence)),
        money_field_kind: moneyFieldKind,
      });
    }
  }
  const trailer = parsed.trailers?.total;
  if (trailer?.evidence) {
    records.push({
      ...evidenceRecord(
        parsed,
        trailer.physicalRow,
        'totalCost',
        trailer.evidence,
        trailer.totalCostPence,
        trailer.evidence.formulaPresent ? 'FORMULA' : 'VALID',
      ),
      money_field_kind: 'BOTTOM_TOTAL_COST',
    });
  }
  return records;
}

function rosterExpenseEvidence(parsed) {
  return (parsed.rows ?? []).map((row) => evidenceRecord(
    parsed,
    row.physicalRow,
    'expenses',
    row.sourceFixedExpense?.sourceEvidence,
    row.sourceFixedExpense?.pence,
    row.sourceFixedExpense?.state === 'OMITTED_ZERO' ? 'OMITTED_ZERO'
      : row.sourceFixedExpense?.state === 'PRESENT' ? 'VALID' : 'INVALID',
    { lexical_profile_version: 'SOURCE_FIXED_EXPENSE_PENCE_V1' },
  )).filter(Boolean);
}

function boundedRawNhsp(row) {
  return {
    worker_name: row.workerName,
    worker_unique_id: row.workerUniqueId,
    trust: row.trust,
    ward: row.ward,
    assignment: row.assignment,
    reference_number: row.referenceNumber,
  };
}

function boundedRawRoster(row) {
  return {
    candidate: row.candidate,
    candidate_uid: row.candidateUid,
    candidate_id: row.candidateId,
    payroll_number: row.payrollNumber,
    business_unit_id: row.clientScope?.businessUnitId,
    business_unit_name: row.clientScope?.businessUnitName,
    booking_id: row.bookingId,
    timesheet_id: row.timesheetId,
    booking_reference: row.bookingReference,
    job_id: row.jobId,
    job_type: row.jobType,
    supply_type: row.supplyType,
  };
}

function normalisedRow(parsed, row, context) {
  if (NHSP.has(parsed.profileId)) {
    return {
      source_row_ordinal: row.physicalRow,
      external_source_key: row.referenceNumber,
      source_candidate_identity: row.workerUniqueId || row.workerName,
      source_client_identity: row.trust,
      work_date: row.date,
      start_at_local: localTimestamp(row.date, row.actual.start),
      end_at_local: localTimestamp(row.date, row.actual.end, row.actual.overnight),
      break_minutes: row.actual.breakMinutes,
      actual_net_minutes: row.actual.totalMinutes,
      row_finalisation_state: 'SOURCE_WORKED',
      finalised_by: null,
      role_band_source: row.assignment,
      source_commission_pence: row.pricingEvidence.commissionPence,
      source_total_cost_pence: row.pricingEvidence.totalCostPence,
      source_shift_charge_pence: row.pricingEvidence.sourceTotalPence,
      source_money_parse_state: row.pricingEvidence.state === 'PARSED'
        ? 'VALID'
        : moneyParseState(
          Object.values(row.pricingEvidence.components ?? {}).find((component) => component?.parseState !== 'PARSED'),
          row.pricingEvidence.issueCode,
        ),
      source_qualification_profile_version: 'NHSP_TWO_COMPONENT_PENCE_V1',
      source_expense_pence: null,
      source_expense_parse_state: 'NOT_APPLICABLE',
      bounded_raw_columns_json: boundedRawNhsp(row),
    };
  }
  if (HEALTHROSTER.has(parsed.profileId)) {
    const worked = row.rowKind === 'FINALISED_WORKED';
    return {
      source_row_ordinal: row.physicalRow,
      external_source_key: row.requestId,
      source_candidate_identity: row.workerName,
      source_client_identity: context.client_name || String(context.client_id),
      work_date: row.date,
      start_at_local: worked ? localTimestamp(row.date, row.actual.start) : null,
      end_at_local: worked ? localTimestamp(row.date, row.actual.end, row.actual.overnight) : null,
      break_minutes: worked ? row.actual.breakMinutes : null,
      actual_net_minutes: worked ? row.actual.totalMinutes : 0,
      row_finalisation_state: worked ? 'SOURCE_WORKED' : 'SOURCE_UNFINALISED',
      finalised_by: row.finalisedBy ?? null,
      role_band_source: null,
      source_commission_pence: null,
      source_total_cost_pence: null,
      source_shift_charge_pence: null,
      source_money_parse_state: 'NOT_APPLICABLE',
      source_qualification_profile_version: null,
      source_expense_pence: null,
      source_expense_parse_state: 'NOT_APPLICABLE',
      bounded_raw_columns_json: {
        worker_name: row.workerName,
        request_id: row.requestId,
        finalised_by: row.finalisedBy ?? null,
      },
    };
  }
  const start = row.wholeShiftInputs.bookingStartLocal;
  const end = row.wholeShiftInputs.bookingEndLocal;
  return {
    source_row_ordinal: row.physicalRow,
    external_source_key: row.lineId,
    source_candidate_identity: row.candidateId || row.candidateUid || row.payrollNumber || row.candidate,
    source_client_identity: row.clientScope.businessUnitId,
    work_date: row.workDate,
    start_at_local: start,
    end_at_local: end,
    break_minutes: row.wholeShiftInputs.breakMinutes,
    actual_net_minutes: row.wholeShiftInputs.paidMinutes,
    row_finalisation_state: row.rowKind === 'SOURCE_ZERO' ? 'SOURCE_ABSENT_ZERO' : 'SOURCE_WORKED',
    finalised_by: null,
    role_band_source: row.jobType || row.supplyType,
    source_commission_pence: null,
    source_total_cost_pence: null,
    source_shift_charge_pence: null,
    source_money_parse_state: 'NOT_APPLICABLE',
    source_qualification_profile_version: null,
    source_expense_pence: row.sourceFixedExpense.pence,
    source_expense_parse_state: row.sourceFixedExpense.state === 'PRESENT' ? 'VALID'
      : row.sourceFixedExpense.state === 'OMITTED_ZERO' ? 'OMITTED_ZERO' : 'INVALID',
    bounded_raw_columns_json: boundedRawRoster(row),
  };
}

function classifications(parsed) {
  const accepted = new Set((parsed.rows ?? []).map((row) => row.physicalRow));
  const presentation = new Set((parsed.presentationRows ?? []).map((row) => row.physicalRow));
  const trailer = new Set([
    parsed.trailers?.frameworkManagementCharge?.physicalRow,
    parsed.trailers?.total?.physicalRow,
  ].filter(Boolean));
  return (parsed.physicalRows ?? []).map((row) => ({
    source_row_ordinal: row.physicalRow,
    bounded_raw_cells_json: row.boundedRawCells ?? {},
    classification: row.physicalRow <= parsed.headerRow ? 'HEADER'
      : accepted.has(row.physicalRow) ? 'ACCEPTED_SHIFT'
        : trailer.has(row.physicalRow) ? 'TRAILER'
          : presentation.has(row.physicalRow) ? 'PROFILE_PROVED_NON_ECONOMIC_CONTINUATION'
            : 'PROFILE_PROVED_NON_ECONOMIC_CONTINUATION',
  }));
}

function healthRosterFinalisationMap(parsed) {
  return Object.fromEntries(Object.entries(parsed.resolvedColumnMap ?? {}).map(([key, value]) => [key, {
    column_index: value.index,
    column: value.column,
    header_coordinate: `${value.column}${parsed.headerRow}`,
  }]));
}

function coverage(parsed, body) {
  if (parsed.profileId === 'NHSP_FINAL_BACKING_V1') {
    return {
      suggested_coverage_start_local_date: null,
      suggested_coverage_end_local_date: null,
      confirmed_coverage_start_local_date: null,
      confirmed_coverage_end_local_date: null,
      coverage_timezone: null,
      coverage_confirmation_version: null,
      coverage_confirmed_at_utc: null,
      coverage_shrink_acknowledged: null,
      coverage_state: 'COMPLETE',
      coverage_proof_kind: 'NHSP_TRUST_REPORT_SCOPE',
    };
  }
  const dates = (parsed.rows ?? []).map((row) => row.date ?? row.workDate).filter(Boolean).sort();
  const suggestedStart = dates[0] ?? null;
  const suggestedEnd = dates.at(-1) ?? null;
  const supplied = body.coverage && typeof body.coverage === 'object' ? body.coverage : {};
  // Pack 14 section 5.4.3: the service SUGGESTS the file's own first and last
  // work date; Office CONFIRMS the range for which the export is complete. The
  // confirmation is deliberately not forced to equal the rows present, because
  // section 5.4.5 bullet 4 cancels a Request Id that is absent INSIDE confirmed
  // coverage, and a withdrawn shift at the edge of the week can only be seen by
  // a confirmed range wider than what arrived. Office supplying nothing means
  // "I confirm the suggested range". A narrower range is refused: the finaliser
  // admits no row outside confirmed coverage.
  const confirmedStart = supplied.start_local_date ?? supplied.confirmed_start_local_date ?? suggestedStart;
  const confirmedEnd = supplied.end_local_date ?? supplied.confirmed_end_local_date ?? suggestedEnd;
  if (suggestedStart && suggestedEnd
      && (!confirmedStart || !confirmedEnd
          || confirmedStart > suggestedStart || confirmedEnd < suggestedEnd)) {
    fail('WEEKLY_SOURCE_COVERAGE_CONFIRMATION_NARROWER_THAN_EVIDENCE',
      'The confirmed period must cover every date present in the source file.');
  }
  if (!suggestedStart && (!confirmedStart || !confirmedEnd)) {
    fail('WEEKLY_SOURCE_EMPTY_COVERAGE_CONFIRMATION_REQUIRED', 'Confirm the empty source period.');
  }
  return {
    suggested_coverage_start_local_date: suggestedStart,
    suggested_coverage_end_local_date: suggestedEnd,
    confirmed_coverage_start_local_date: confirmedStart,
    confirmed_coverage_end_local_date: confirmedEnd,
    coverage_timezone: 'Europe/London',
    coverage_confirmation_version: 'WEEKLY_SOURCE_OFFICE_COVERAGE_CONFIRMATION_V1',
    coverage_confirmed_at_utc: supplied.confirmed_at_utc ?? new Date().toISOString(),
    coverage_shrink_acknowledged: supplied.shrink_acknowledged === true,
    coverage_state: 'COMPLETE',
    coverage_proof_kind: text(supplied.proof_kind).toUpperCase() || (
      HEALTHROSTER.has(parsed.profileId)
        ? 'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION'
        : suggestedStart ? 'OFFICE_COMPLETE_EXPORT_ATTESTATION' : 'EXPLICIT_EMPTY_CONFIRMATION'
    ),
  };
}

export function adaptWeeklySourceParserOutput(parsed, body = {}, context = {}) {
  if (!parsed || parsed.ok !== true) {
    fail('WEEKLY_SOURCE_PARSE_NOT_ACCEPTABLE', 'The source file has unresolved parsing errors.', {
      fatalErrors: parsed?.fatalErrors ?? [],
    });
  }
  const physicalRows = classifications(parsed);
  if (physicalRows.length !== Number(parsed.selectedWorksheet?.physicalOrdinalSpan ?? 0)) {
    fail('WEEKLY_SOURCE_PHYSICAL_CENSUS_INCOMPLETE', 'The source row census is incomplete.');
  }
  const normalisedRows = (parsed.rows ?? []).map((row) => normalisedRow(parsed, row, context));
  const count = (classification) => physicalRows.filter((row) => row.classification === classification).length;
  const metadata = {
    client_id: context.client_id ?? null,
    ...(parsed.profileId === 'NHSP_FINAL_BACKING_V1' ? {
      nhsp_report_number: parsed.scope?.backingReportNumber,
      nhsp_report_heading_name: parsed.scope?.reportHeadingName,
    } : {}),
    ...(HEALTHROSTER.has(parsed.profileId) ? {
      saved_finalisation_profile_map: healthRosterFinalisationMap(parsed),
    } : {}),
  };
  const headerMap = Object.fromEntries(Object.entries(parsed.resolvedColumnMap ?? {}).map(([key, value]) => [key, {
    column_index: value.index,
    column: value.column,
    coordinate: `${value.column}${parsed.headerRow}`,
  }]));
  return Object.freeze({
    beginRequest: {
      actor_user_id: body.actor_user_id,
      environment: context.environment,
      agency_id: context.agency_id,
      source_group_id: context.source_group_id,
      source_cycle_id: context.source_cycle_id,
      report_scope_id: context.report_scope_id ?? null,
      client_id: context.client_id ?? null,
      original_filename: text(body.original_filename || body.filename || body.file_key).slice(0, 255),
      content_sha256: parsed.sourceFileSha256,
      byte_count: parsed.fileFacts.byteLength,
      profile_code: parsed.profileId,
      profile_version: parsed.profileVersion,
      parser_version: parsed.parserVersion,
      normaliser_version: `${parsed.profileId}:${parsed.profileVersion}:${WEEKLY_SOURCE_UPLOAD_ADAPTER_VERSION}`,
      workbook_part_and_sheet_fingerprint: parsed.selectedWorksheet.workbookPartAndSheetFingerprint,
      header_coordinate_map_json: headerMap,
      money_lexical_authority_version: NHSP.has(parsed.profileId) ? XLSX_BINARY64_SAME_VALUE_PENCE_V1 : null,
      purpose: body.purpose ?? 'ORDINARY',
      correction_session_id: body.correction_session_id ?? null,
      ...(body.expected_correction_session_version == null ? {} : {
        expected_correction_session_version: body.expected_correction_session_version,
      }),
      ...coverage(parsed, body),
      physical_row_count: physicalRows.length,
      header_count: count('HEADER'),
      trailer_count: count('TRAILER'),
      continuation_count: count('PROFILE_PROVED_NON_ECONOMIC_CONTINUATION'),
      accepted_count: normalisedRows.length,
      blocking_economic_duplicate_count: 0,
      malformed_count: 0,
      blocked_count: 0,
      file_metadata_json: metadata,
      parser_summary_json: {
        parser_version: parsed.parserVersion,
        adapter_version: WEEKLY_SOURCE_UPLOAD_ADAPTER_VERSION,
        selected_worksheet: parsed.selectedWorksheet,
        row_counts: parsed.rowCounts,
        warning_count: parsed.warnings?.length ?? 0,
      },
    },
    physicalRows,
    normalisedRows,
    moneyEvidence: NHSP.has(parsed.profileId) ? nhspMoneyEvidence(parsed) : [],
    expenseEvidence: parsed.profileId === 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
      ? rosterExpenseEvidence(parsed) : [],
  });
}
