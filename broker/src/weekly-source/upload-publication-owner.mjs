import { createHash } from 'node:crypto';
import { adaptWeeklySourceParserOutput } from './upload-staging-adapter.mjs';
import { qualifyWeeklySourceContract } from './contract-qualification.js';
import { canonicalWeeklyShiftFinancialSegment } from './weekly-rate-owner.js';
import { compareWeeklySourceShiftPrice } from './source-price-comparator.js';
import { buildWeeklySourceCanonicalEconomicSnapshot } from './economic-snapshot.js';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const NHSP = new Set(['NHSP_PREFINAL_RELEASED_V1', 'NHSP_FINAL_BACKING_V1']);
const ROSTER_EXTERNAL = new Set([
  'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
  'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
  'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1',
]);
const BUCKETS = Object.freeze(['day', 'night', 'sat', 'sun', 'bh']);

export const WEEKLY_SOURCE_UPLOAD_PUBLICATION_OWNER_VERSION =
  'WEEKLY_SOURCE_UPLOAD_PUBLICATION_OWNER_V1';

export class WeeklySourceUploadPublicationError extends Error {
  constructor(code, message, status = 409, details = {}) {
    super(message);
    this.name = 'WeeklySourceUploadPublicationError';
    this.code = code;
    this.status = status;
    this.details = Object.freeze({ ...details });
  }
}

function fail(code, message, status = 409, details = {}) {
  throw new WeeklySourceUploadPublicationError(code, message, status, details);
}

const text = (value) => String(value ?? '').trim();
const upper = (value) => text(value).toUpperCase();

function requiredUuid(value, label) {
  const result = text(value).toLowerCase();
  if (!UUID.test(result)) fail('WEEKLY_SOURCE_UPLOAD_REQUEST_INVALID', `${label} is invalid.`, 400);
  return result;
}

function optionalUuid(value, label) {
  return value == null || value === '' ? null : requiredUuid(value, label);
}

function requiredPositiveInteger(value, label) {
  const token = String(value ?? '').trim();
  if (!/^\d+$/.test(token)) fail('WEEKLY_SOURCE_UPLOAD_REQUEST_INVALID', `${label} is invalid.`, 400);
  const result = Number(token);
  if (!Number.isSafeInteger(result) || result < 1) {
    fail('WEEKLY_SOURCE_UPLOAD_REQUEST_INVALID', `${label} is invalid.`, 400);
  }
  return result;
}

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stable(value[key])]));
  }
  return value;
}

function sha256(value) {
  return createHash('sha256').update(JSON.stringify(stable(value))).digest('hex');
}

function requiredSha256(value, label) {
  const result = text(value).toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(result)) {
    fail('WEEKLY_SOURCE_PUBLICATION_PROOF_INVALID', `${label} is invalid.`, 502);
  }
  return result;
}

function unwrap(value, name) {
  let result = value;
  if (Array.isArray(result) && result.length === 1) [result] = result;
  if (result && typeof result === 'object' && Object.hasOwn(result, name)) result = result[name];
  if (Array.isArray(result) && result.length === 1) [result] = result;
  return result;
}

async function rpc(dependencies, name, args, { request = true } = {}) {
  const caller = dependencies.dataRpc ?? dependencies.rpc;
  if (typeof caller !== 'function') {
    fail('WEEKLY_SOURCE_UPLOAD_DEPENDENCY_UNAVAILABLE', 'Weekly source data is unavailable.', 503, { dependency: 'rpc' });
  }
  const payload = request ? { p_request: args } : args;
  return unwrap(await caller(name, payload, { timeoutMs: 120_000 }), name);
}

function scopeRequest(body, actorUserId, operation = 'DISCOVER_SCOPE', uploadId = null) {
  return {
    operation,
    actor_user_id: actorUserId,
    source_group_id: optionalUuid(body.source_group_id, 'Source group'),
    source_cycle_id: optionalUuid(body.source_cycle_id, 'Source cycle'),
    report_scope_id: optionalUuid(body.report_scope_id, 'Report scope'),
    client_id: optionalUuid(body.client_id, 'Client'),
    upload_id: uploadId,
  };
}

function validateContext(context) {
  if (!context || context.ok !== true || !UUID.test(text(context.agency_id))
      || !UUID.test(text(context.source_group_id)) || !UUID.test(text(context.source_cycle_id))) {
    fail('WEEKLY_SOURCE_CONTEXT_INVALID', 'The source scope could not be verified.', 502);
  }
  return context;
}

function requireSelectedClient(context) {
  if (context.client_selection_required === true || (!context.client_id && context.source_family === 'ROSTER')) {
    fail('WEEKLY_SOURCE_CLIENT_SELECTION_REQUIRED', 'Choose the Client for this source file.', 409, {
      choices: Array.isArray(context.client_choices) ? context.client_choices : [],
    });
  }
}

function parserOptions(body, context) {
  const requested = body.parser_options && typeof body.parser_options === 'object'
    ? body.parser_options : {};
  const profileId = text(body.profile_id || requested.profileId);
  if (!profileId) fail('WEEKLY_SOURCE_PROFILE_REQUIRED', 'Choose the source file type.', 400);
  return {
    profileId,
    ...(profileId === 'NHSP_FINAL_BACKING_V1' ? {
      configuredNhspReportHeadingName: context.nhsp_report_heading_name,
      expectedTrust: context.client_name,
    } : {}),
    ...(!NHSP.has(profileId) ? { expectedClient: context.client_name } : {}),
  };
}

function chunks(values, size) {
  const result = [];
  for (let index = 0; index < values.length; index += size) result.push(values.slice(index, index + size));
  return result;
}

function clockMinutes(value) {
  const match = /^(\d{2}):(\d{2})(?::\d{2})?$/.exec(text(value));
  if (!match || Number(match[1]) > 23 || Number(match[2]) > 59) {
    fail('WEEKLY_SOURCE_RATE_WINDOW_INVALID', 'A saved rate window is invalid.', 500);
  }
  return Number(match[1]) * 60 + Number(match[2]);
}

function inWindow(minute, start, end) {
  return start === end || (start < end ? minute >= start && minute < end : minute >= start || minute < end);
}

function localParts(instant, timeZone) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone, year: 'numeric', month: '2-digit', day: '2-digit',
    weekday: 'short', hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(new Date(instant));
  const get = (type) => parts.find((part) => part.type === type)?.value;
  const hour = get('hour') === '24' ? '00' : get('hour');
  return {
    date: `${get('year')}-${get('month')}-${get('day')}`,
    weekday: upper(get('weekday')),
    minute: Number(hour) * 60 + Number(get('minute')),
    local: `${get('year')}-${get('month')}-${get('day')}T${hour}:${get('minute')}:00`,
  };
}

function localToUtc(local, timeZone) {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):00$/.exec(text(local));
  if (!match) fail('WEEKLY_SOURCE_LOCAL_TIME_INVALID', 'A source shift time is invalid.');
  const naive = Date.UTC(...match.slice(1).map(Number).map((value, index) => index === 1 ? value - 1 : value));
  const matches = [];
  for (let offset = -840; offset <= 840; offset += 15) {
    const candidate = naive - offset * 60_000;
    if (localParts(candidate, timeZone).local === local) matches.push(candidate);
  }
  const unique = [...new Set(matches)];
  if (unique.length !== 1) {
    fail(
      unique.length ? 'WEEKLY_SOURCE_LOCAL_TIME_AMBIGUOUS' : 'WEEKLY_SOURCE_LOCAL_TIME_NONEXISTENT',
      'A source shift time cannot be resolved unambiguously.',
    );
  }
  return new Date(unique[0]).toISOString();
}

function ratesFor(contract) {
  const rates = contract.rates_json ?? {};
  const prefix = upper(contract.pay_type) === 'PAYE' ? 'paye' : 'umb';
  const payRates = {};
  const chargeRates = {};
  for (const bucket of BUCKETS) {
    payRates[bucket] = Number(rates[`${prefix}_${bucket}`]);
    chargeRates[bucket] = Number(rates[`charge_${bucket}`]);
  }
  const valid = [...Object.values(payRates), ...Object.values(chargeRates)]
    .every((value) => Number.isFinite(value) && value > 0);
  return { payRates, chargeRates, valid };
}

function ratePortions(row, contract, startInstant, endInstant) {
  const values = contract.settings_authority?.values ?? {};
  const timezone = text(values.timezone_id) || 'Europe/London';
  const defaultWindows = {
    day: { start: '06:00:00', end: '20:00:00' },
    sat: { start: '00:00:00', end: '00:00:00' },
    sun: { start: '00:00:00', end: '00:00:00' },
    bh: { start: '00:00:00', end: '00:00:00' },
  };
  const windows = Object.fromEntries(['day', 'sat', 'sun', 'bh'].map((bucket) => [bucket, {
    start: clockMinutes(values[`${bucket}_start`] || defaultWindows[bucket].start),
    end: clockMinutes(values[`${bucket}_end`] || defaultWindows[bucket].end),
  }]));
  const holidays = new Set(Array.isArray(values.bh_list) ? values.bh_list.map(String) : []);
  const startMs = new Date(startInstant).getTime();
  const endMs = new Date(endInstant).getTime();
  const portions = [];
  for (let cursor = startMs; cursor < endMs; cursor += 60_000) {
    const local = localParts(cursor, timezone);
    const bucket = holidays.has(local.date) && inWindow(local.minute, windows.bh.start, windows.bh.end)
      ? 'bh'
      : local.weekday === 'SUN' && inWindow(local.minute, windows.sun.start, windows.sun.end)
        ? 'sun'
        : local.weekday === 'SAT' && inWindow(local.minute, windows.sat.start, windows.sat.end)
          ? 'sat'
          : inWindow(local.minute, windows.day.start, windows.day.end) ? 'day' : 'night';
    const previous = portions.at(-1);
    if (previous?.bucket === bucket && new Date(previous.endInstant).getTime() === cursor) {
      previous.endInstant = new Date(cursor + 60_000).toISOString();
    } else {
      portions.push({
        bucket,
        startInstant: new Date(cursor).toISOString(),
        endInstant: new Date(cursor + 60_000).toISOString(),
      });
    }
  }
  if (portions.length === 0) fail('WEEKLY_SOURCE_SHIFT_INTERVAL_INVALID', 'A source shift interval is invalid.');
  return portions;
}

function calculationFor(row, contract) {
  const policy = contract.effective_policy ?? {};
  const values = contract.settings_authority?.values ?? {};
  const { payRates, chargeRates, valid } = ratesFor(contract);
  if (!valid) return { valid: false, code: 'CONTRACT_RATE_AUTHORITY_INCOMPLETE' };
  const timezone = text(values.timezone_id) || 'Europe/London';
  const startInstant = localToUtc(row.start_at_local, timezone);
  const endInstant = localToUtc(row.end_at_local, timezone);
  const mode = upper(policy.weekly_rate_classification_method);
  const sign = NHSP.has(row.profile_id) && BigInt(String(row.source_shift_charge_pence ?? 0)) < 0n ? -1 : 1;
  const financialInput = mode === 'WHOLE_SHIFT_START_DAY' ? {
    mode,
    payRates,
    chargeRates,
    sign,
    startInstant,
    endInstant,
    timeZone: timezone,
    bankHolidayDates: Array.isArray(values.bh_list) ? values.bh_list : [],
    breakEvidence: { durationMinutes: row.break_minutes },
  } : {
    mode: 'SPLIT_RATE_WINDOWS',
    payRates,
    chargeRates,
    sign,
    ratePortions: ratePortions(row, contract, startInstant, endInstant),
    breakMinutes: row.break_minutes,
    durationBreakTieRule: policy.duration_break_tie_rule,
  };
  try {
    const sourceMode = NHSP.has(row.profile_id) ? 'NHSP_WEEKLY' : 'HEALTHROSTER_WEEKLY';
    const comparison = compareWeeklySourceShiftPrice({
      sourceMode,
      financialInput,
      ...(sourceMode === 'NHSP_WEEKLY' ? { sourceChargePence: row.source_shift_charge_pence } : {}),
    });
    if (Number(comparison.calculation.paidMinutes) !== Number(row.actual_net_minutes)
        || Number(comparison.calculation.breakMinutes) !== Number(row.break_minutes)) {
      return { valid: false, code: 'WEEKLY_SOURCE_ACTUAL_MINUTES_DO_NOT_MATCH_LOCAL_INTERVAL' };
    }
    return { valid: true, sourceMode, mode, payRates, chargeRates, comparison, financialInput };
  } catch (error) {
    return { valid: false, code: error.code ?? 'WEEKLY_SOURCE_CALCULATION_FAILED' };
  }
}

function selectionFor(body, ordinal) {
  const selections = body.contract_selections;
  if (!selections || typeof selections !== 'object' || Array.isArray(selections)) return null;
  return selections[String(ordinal)] ?? null;
}

function mappingResult(state) {
  if (state === 'NO_ELIGIBLE_CONTRACT' || state === 'NO_COST_COMPATIBLE_CONTRACT') {
    return ['NO_ELIGIBLE_CONTRACT', state];
  }
  if (state === 'MULTIPLE_MATCHING_CONTRACTS') return ['CONTRACT_SELECTION_REQUIRED', 'MULTIPLE_MATCHING_CONTRACTS'];
  if (state === 'CONTRACT_COST_CHECK_UNAVAILABLE') return ['SOURCE_ROW_BLOCKED', state];
  return ['SOURCE_ROW_BLOCKED', 'WEEKLY_SOURCE_MAPPING_UNRESOLVED'];
}

function profileLink(row, sourceAuthority, sourceMode) {
  if (!sourceAuthority) return 'TIMESHEET_EVIDENCE';
  if (row.profile_id === 'NHSP_PREFINAL_RELEASED_V1' || row.row_finalisation_state === 'SOURCE_UNFINALISED') {
    return 'PROVISIONAL_SOURCE';
  }
  if (row.row_finalisation_state === 'SOURCE_ABSENT_ZERO') return 'ZERO_SOURCE';
  if (sourceMode === 'NHSP_WEEKLY' && BigInt(String(row.source_shift_charge_pence ?? 0)) < 0n) {
    return 'FULL_NEGATIVE_SOURCE';
  }
  return 'POSITIVE_SOURCE';
}

function projectionRow(source, body) {
  const row = { ...source, profile_id: body.profile_id };
  const base = {
    upload_row_id: row.upload_row_id,
    candidate_id: row.candidate_id,
    client_id: row.client_id,
    prior_work_event_id: row.prior_work_event_id,
    qualifying_contract_ids: [],
    qualification_observations: [],
  };
  if (Number(row.candidate_match_count) === 0) {
    return { ...base, mapping_state: 'CANDIDATE_NOT_FOUND', blocker_code: 'CANDIDATE_NOT_FOUND' };
  }
  if (Number(row.candidate_match_count) !== 1) {
    return { ...base, candidate_id: null, mapping_state: 'SOURCE_ROW_BLOCKED', blocker_code: 'CANDIDATE_MAPPING_AMBIGUOUS' };
  }
  if (!row.client_id) return { ...base, mapping_state: 'CLIENT_NOT_FOUND', blocker_code: 'CLIENT_NOT_FOUND' };

  const calculated = new Map();
  const contracts = (row.contracts ?? []).map((contract) => {
    let economic = null;
    if (row.start_at_local && row.end_at_local && Number(row.actual_net_minutes) > 0) {
      economic = calculationFor(row, contract);
      calculated.set(contract.contract_id, economic);
    }
    return {
      contractId: contract.contract_id,
      candidateId: contract.candidate_id,
      clientId: contract.client_id,
      validFrom: contract.valid_from,
      validTo: contract.valid_to,
      displayLabel: contract.display_label,
      payType: contract.pay_type,
      // Contract identity is decided from reliable Candidate/Client/date and
      // applicability facts. Price calculation safety is evaluated after that
      // identity decision, so an unsafe calculation produces its real blocker
      // instead of the misleading "No matching contract" state.
      weeklySourceApplicable: contract.weekly_source_applicable === true,
      // G6-5: server-derived narrowing facts. `schedule_compatible` is false
      // only when the Contract's own declared standard schedule has no entry
      // for the worked weekday; `verified_role_band_match` comes from the
      // established `assignment_band_mappings` authority. Both are
      // tie-breakers inside `qualifyWeeklySourceContract`, never base filters.
      scheduleCompatible: contract.schedule_compatible !== false,
      verifiedRoleBandMatch: contract.verified_role_band_match === true,
      priceObservation: NHSP.has(row.profile_id) ? {
        result: economic?.valid ? economic.comparison.result : 'UNVERIFIABLE',
      } : undefined,
    };
  });
  const mode = NHSP.has(row.profile_id) ? 'NHSP_WEEKLY' : 'HEALTHROSTER_WEEKLY';
  const qualified = qualifyWeeklySourceContract({
    sourceMode: mode,
    candidateId: row.candidate_id,
    clientId: row.client_id,
    workDate: row.work_date,
    contracts,
    officeSelectedContractId: selectionFor(body, row.source_row_ordinal),
    priorAcceptedContractId: row.prior_accepted_contract_id,
  });
  if (qualified.state !== 'RESOLVED') {
    const [mappingState, blockerCode] = mappingResult(qualified.state);
    return {
      ...base,
      mapping_state: mappingState,
      blocker_code: blockerCode,
      qualifying_contract_ids: qualified.eligibleContractIds,
    };
  }
  const contract = (row.contracts ?? []).find((entry) => entry.contract_id === qualified.selectedContractId);
  const policy = contract.effective_policy;
  const sourceAuthority = policy.authority_mode === 'SOURCE_AUTHORITY';
  const sourceMode = policy.c1_source_mode;
  const result = {
    ...base,
    mapping_state: 'RESOLVED',
    contract_id: qualified.selectedContractId,
    contract_selection_method: qualified.selectionMethod,
    qualifying_contract_ids: qualified.eligibleContractIds,
    effective_policy_fingerprint: policy.policy_sha256,
    // G6-13 / XSG-010. `24 §9`: "NHSP Reference Number, ward, location and
    // band are immutable evidence values but not sole durable work identity.
    // They may change during correction." `25 §7` Removed: "Using NHSP
    // Reference Number, ward, location or band as the sole durable work
    // identity." Only the three roster profiles, whose external key is the
    // roster system's own line identity, carry PROFILE_EXTERNAL_KEY. Every
    // other profile — NHSP included — resolves through the schedule tuple:
    // exact Candidate, actual Client and worked date with a COMPATIBLE
    // schedule, which is `24 §9` step 2 word for word. WP-37 corrected this:
    // it previously said "start and end", and keying identity on the exact
    // Actual times made `24 §6.2` ("Paid eight hours becomes nine") create a
    // SECOND work event instead of a later change on one root. The server owns
    // that rule; the broker asserts no identity of its own. The Reference
    // Number is still staged on the upload row as `external_source_key` and
    // retained in `bounded_raw_columns_json.reference_number`, so it stays
    // available as evidence and in audit.
    identity_kind: ROSTER_EXTERNAL.has(row.profile_id) ? 'PROFILE_EXTERNAL_KEY' : 'SCHEDULE_TUPLE',
    profile_external_key: ROSTER_EXTERNAL.has(row.profile_id) ? (row.external_source_key ?? null) : null,
    link_kind: profileLink(row, sourceAuthority, sourceMode),
  };
  const economic = calculated.get(contract.contract_id);
  if (sourceAuthority && row.row_finalisation_state === 'SOURCE_WORKED') {
    if (!economic?.valid) {
      return { ...base, mapping_state: 'SOURCE_ROW_BLOCKED', blocker_code: economic?.code ?? 'CONTRACT_RATE_AUTHORITY_INCOMPLETE' };
    }
    result.economic_snapshot = buildWeeklySourceCanonicalEconomicSnapshot({
      resolvedFacts: {
        sourceMode,
        rateMethod: upper(policy.weekly_rate_classification_method),
        payRates: economic.payRates,
        chargeRates: economic.chargeRates,
      },
      priceComparison: economic.comparison,
    });
    if (sourceMode === 'NHSP_WEEKLY') {
      result.qualification_observations = qualified.eligibleContractIds.map((contractId) => {
        const eligible = calculated.get(contractId);
        const candidateContract = row.contracts.find((entry) => entry.contract_id === contractId);
        return {
          contract_id: contractId,
          contract_revision_fingerprint: sha256({
            contract_id: contractId,
            updated_at: candidateContract.contract_updated_at,
            rates_json: candidateContract.rates_json,
            policy: candidateContract.effective_policy,
            settings: candidateContract.settings_authority,
          }),
          source_shift_charge_pence: String(row.source_shift_charge_pence),
          canonical_calculated_pence: eligible.comparison.calculatedComparisonChargePence,
          comparison_result: eligible.comparison.result,
          reason_codes: [eligible.comparison.result],
        };
      });
      result.charge_check = {
        row_sign_kind: BigInt(String(row.source_shift_charge_pence)) < 0n ? 'FULL_NEGATIVE' : 'POSITIVE',
        source_commission_pence: String(row.source_commission_pence),
        source_total_cost_pence: String(row.source_total_cost_pence),
        source_shift_charge_pence: String(row.source_shift_charge_pence),
        calculated_segment_charge_pence: economic.comparison.calculatedComparisonChargePence,
        comparison_result: economic.comparison.result,
        comparison_reason_code: economic.comparison.result,
        phase_severity: ['MISMATCH', 'ZERO_SOURCE_CHARGE'].includes(economic.comparison.result)
          ? 'PROVISIONAL_WARNING'
          : 'NONE',
        blocker_code: null,
      };
    }
  }
  return result;
}

export function buildWeeklySourceProjectionRows(context, body = {}) {
  if (!context || !Array.isArray(context.rows)) {
    fail('WEEKLY_SOURCE_PROJECTION_CONTEXT_INVALID', 'The source comparison context is invalid.', 502);
  }
  return context.rows.map((row) => projectionRow(row, {
    ...body,
    profile_id: context.profile_id,
  }));
}

async function publishProjection(dependencies, actorUserId, uploadId, version, body) {
  const begun = await rpc(dependencies, 'weekly_source_projection_begin_atomic_v1', {
    actor_user_id: actorUserId,
    upload_id: uploadId,
    expected_authority_scope_version: version,
  });
  if (!begun?.ok) fail(begun?.reason_code ?? 'WEEKLY_SOURCE_PROJECTION_STALE', 'The source changed before publication.');
  if (['CURRENT', 'CORRECTION_READY'].includes(begun.status)) return begun;
  if (begun.status !== 'BUILDING') fail('WEEKLY_SOURCE_PROJECTION_BEGIN_INVALID', 'The source comparison could not begin.', 502);
  const context = validateContext(await rpc(dependencies, 'weekly_source_upload_context_v1', {
    ...scopeRequest(body, actorUserId, 'BUILD_PROJECTION', uploadId),
  }));
  const rows = buildWeeklySourceProjectionRows(context, body);
  await rpc(dependencies, 'weekly_source_projection_rows_apply_atomic_v1', {
    p_actor_user_id: actorUserId,
    p_publication_id: begun.publication_id,
    p_rows: rows,
  }, { request: false });
  const published = await rpc(dependencies, 'weekly_source_projection_publish_atomic_v1', {
    actor_user_id: actorUserId,
    publication_id: begun.publication_id,
  });
  if (!published?.ok || !['CURRENT', 'CORRECTION_READY'].includes(published.status)) {
    fail(
      published?.reason_code ?? 'WEEKLY_SOURCE_PUBLICATION_FAILED',
      'The source comparison could not be published.',
      published?.status === 'STALE' ? 409 : 502,
    );
  }
  return published;
}

async function rebuildCorrectionProjection(dependencies, input, body, actorUserId) {
  const begun = await rpc(dependencies, 'weekly_source_projection_begin_atomic_v1', {
    actor_user_id: actorUserId,
    upload_id: input.replacement_upload_id,
    expected_authority_scope_version: input.expected_authority_scope_version,
    correction_session_id: input.correction_session_id,
    expected_correction_session_version: input.expected_session_version,
    expected_projection_publication_id: input.replacement_projection_publication_id,
    expected_row_manifest_hash: input.expected_row_manifest_hash,
    expected_comparison_manifest_hash: input.expected_comparison_manifest_hash,
    expected_issue_set_hash: input.expected_issue_set_hash,
    rebuild_idempotency_key: input.idempotency_key,
  });
  if (!begun?.ok) {
    fail(
      begun?.reason_code ?? 'WEEKLY_SOURCE_CORRECTION_REBUILD_STALE',
      'The replacement comparison changed before it could be rebuilt.',
    );
  }
  if (begun.publication_id === input.replacement_projection_publication_id) {
    fail(
      'WEEKLY_SOURCE_CORRECTION_REBUILD_NOT_FRESH',
      'The replacement comparison was not rebuilt.',
      502,
    );
  }

  let published = begun;
  if (begun.status === 'BUILDING') {
    const context = validateContext(await rpc(dependencies, 'weekly_source_upload_context_v1', {
      ...scopeRequest(body, actorUserId, 'BUILD_PROJECTION', input.replacement_upload_id),
    }));
    const rows = buildWeeklySourceProjectionRows(context);
    if (begun.rows_applied !== true) {
      await rpc(dependencies, 'weekly_source_projection_rows_apply_atomic_v1', {
        p_actor_user_id: actorUserId,
        p_publication_id: begun.publication_id,
        p_rows: rows,
      }, { request: false });
    }
    published = await rpc(dependencies, 'weekly_source_projection_publish_atomic_v1', {
      actor_user_id: actorUserId,
      publication_id: begun.publication_id,
    });
  }
  if (!published?.ok || published.status !== 'CORRECTION_READY') {
    fail(
      published?.reason_code ?? 'WEEKLY_SOURCE_CORRECTION_REBUILD_FAILED',
      'The replacement comparison could not be rebuilt.',
      published?.status === 'STALE' ? 409 : 502,
    );
  }
  return {
    ...published,
    rebuild_begin_idempotent: begun.idempotent === true,
  };
}

async function stageParsed(dependencies, body, parsed, context, actorUserId) {
  const adapted = adaptWeeklySourceParserOutput(parsed, { ...body, actor_user_id: actorUserId }, context);
  const started = await rpc(dependencies, 'weekly_source_upload_stage_begin_atomic_v1', adapted.beginRequest);
  if (!started?.ok) fail(started?.reason_code ?? 'WEEKLY_SOURCE_UPLOAD_BEGIN_FAILED', 'The source file could not be staged.', 502);
  if (started.status === 'CONFLICT') fail(started.reason_code ?? 'WEEKLY_SOURCE_UPLOAD_CONFLICT', 'This source file conflicts with an existing upload.');
  const uploadId = started.upload_id ?? started.logical_upload_id;
  if (!uploadId) fail('WEEKLY_SOURCE_UPLOAD_ID_MISSING', 'The staged source identity is missing.', 502);
  if (started.status !== 'DUPLICATE') {
    const batchCount = Math.max(
      Math.ceil(adapted.physicalRows.length / 2_000),
      Math.ceil(adapted.normalisedRows.length / 2_000),
      Math.ceil(adapted.moneyEvidence.length / 8_000),
      Math.ceil(adapted.expenseEvidence.length / 2_000),
      1,
    );
    const physical = chunks(adapted.physicalRows, 2_000);
    const rows = chunks(adapted.normalisedRows, 2_000);
    const money = chunks(adapted.moneyEvidence, 8_000);
    const expenses = chunks(adapted.expenseEvidence, 2_000);
    for (let index = 0; index < batchCount; index += 1) {
      await rpc(dependencies, 'weekly_source_upload_stage_rows_atomic_v1', {
        actor_user_id: actorUserId,
        upload_id: uploadId,
        physical_rows: physical[index] ?? [],
        normalised_rows: rows[index] ?? [],
        money_evidence: money[index] ?? [],
        expense_evidence: expenses[index] ?? [],
      });
    }
  }
  const sealed = started.status === 'DUPLICATE' ? started : await rpc(
    dependencies,
    'weekly_source_upload_seal_atomic_v1',
    { actor_user_id: actorUserId, upload_id: uploadId },
  );
  if (!sealed?.ok || !['CURRENT', 'CORRECTION_READY', 'DUPLICATE'].includes(sealed.status)) {
    fail(sealed?.reason_code ?? 'WEEKLY_SOURCE_UPLOAD_SEAL_FAILED', 'The source file could not be sealed.', 409);
  }
  const version = Number(sealed.authority_scope_version ?? context.authority_scope_version);
  if (!Number.isSafeInteger(version) || version < 1) {
    fail('WEEKLY_SOURCE_SCOPE_VERSION_INVALID', 'The source version is invalid.', 502);
  }
  const published = await publishProjection(dependencies, actorUserId, uploadId, version, body);
  const publishedContext = body.purpose === 'FINAL_SOURCE_CORRECTION'
    ? validateContext(await rpc(dependencies, 'weekly_source_upload_context_v1', {
      ...scopeRequest(body, actorUserId, 'BUILD_PROJECTION', uploadId),
    }))
    : null;
  return { adapted, started, sealed, published, publishedContext, uploadId, version };
}

export function createWeeklySourceUploadPublicationOwner(dependencies = {}) {
  return Object.freeze({
    async previewUpload({ body = {}, bytes, actor, parseWeeklySourceFile }) {
      const actorUserId = requiredUuid(actor?.id, 'Office user');
      const context = validateContext(await rpc(
        dependencies,
        'weekly_source_upload_context_v1',
        scopeRequest(body, actorUserId),
      ));
      requireSelectedClient(context);
      if (typeof parseWeeklySourceFile !== 'function') {
        fail('WEEKLY_SOURCE_PARSER_UNAVAILABLE', 'The source parser is unavailable.', 503);
      }
      const parsed = await parseWeeklySourceFile(bytes, parserOptions(body, context));
      if (parsed?.ok !== true) {
        const reason = upper(parsed?.fatalErrors?.[0]?.code) || 'WEEKLY_SOURCE_PARSE_REJECTED';
        await rpc(dependencies, 'weekly_source_upload_attempt_record_atomic_v1', {
          actor_user_id: actorUserId,
          environment: context.environment,
          agency_id: context.agency_id,
          source_group_id: context.source_group_id,
          source_cycle_id: context.source_cycle_id,
          report_scope_id: context.report_scope_id,
          client_id: context.client_id,
          purpose: body.purpose ?? 'ORDINARY',
          correction_session_id: body.correction_session_id ?? null,
          original_filename: text(body.original_filename || body.filename || body.file_key).slice(0, 255),
          byte_count: parsed?.fileFacts?.byteLength,
          content_sha256: parsed?.sourceFileSha256,
          parser_version: parsed?.parserVersion,
          profile_version: parsed?.profileId ? `${parsed.profileId}:${parsed.profileVersion}` : null,
          normaliser_version: WEEKLY_SOURCE_UPLOAD_PUBLICATION_OWNER_VERSION,
          result: 'REJECTED',
          reason_code: reason,
        });
      }
      return { parsed, context };
    },

    async recordUploadPreview({ body = {}, parsed, actor }) {
      const actorUserId = requiredUuid(actor?.id, 'Office user');
      const context = validateContext(await rpc(
        dependencies,
        'weekly_source_upload_context_v1',
        scopeRequest(body, actorUserId),
      ));
      requireSelectedClient(context);
      if (parsed?.ok !== true) {
        const reason = upper(parsed?.fatalErrors?.[0]?.code) || 'WEEKLY_SOURCE_PARSE_REJECTED';
        await rpc(dependencies, 'weekly_source_upload_attempt_record_atomic_v1', {
          actor_user_id: actorUserId,
          environment: context.environment,
          agency_id: context.agency_id,
          source_group_id: context.source_group_id,
          source_cycle_id: context.source_cycle_id,
          report_scope_id: context.report_scope_id,
          client_id: context.client_id,
          purpose: body.purpose ?? 'ORDINARY',
          correction_session_id: body.correction_session_id ?? null,
          original_filename: text(body.original_filename || body.filename || body.file_key).slice(0, 255),
          byte_count: parsed?.fileFacts?.byteLength,
          content_sha256: parsed?.sourceFileSha256,
          parser_version: parsed?.parserVersion,
          profile_version: parsed?.profileId ? `${parsed.profileId}:${parsed.profileVersion}` : null,
          normaliser_version: WEEKLY_SOURCE_UPLOAD_PUBLICATION_OWNER_VERSION,
          result: 'REJECTED',
          reason_code: reason,
        });
      }
      return { ok: parsed?.ok === true, context };
    },

    async acceptUpload({ body = {}, bytes, actor, parseWeeklySourceFile }) {
      const actorUserId = requiredUuid(actor?.id, 'Office user');
      const context = validateContext(await rpc(
        dependencies,
        'weekly_source_upload_context_v1',
        scopeRequest(body, actorUserId),
      ));
      requireSelectedClient(context);
      if (typeof parseWeeklySourceFile !== 'function') {
        fail('WEEKLY_SOURCE_PARSER_UNAVAILABLE', 'The source parser is unavailable.', 503);
      }
      const parsed = await parseWeeklySourceFile(bytes, parserOptions(body, context));
      const result = await stageParsed(dependencies, body, parsed, context, actorUserId);
      return {
        ok: true,
        status: result.published.status,
        upload_id: result.uploadId,
        authority_scope_version: result.version,
        publication_id: result.published.publication_id,
        idempotent: result.started.status === 'DUPLICATE' || result.published.idempotent === true,
        source_profile: parsed.profileId,
      };
    },

    async stageReplacementSource(input = {}) {
      const actorUserId = requiredUuid(input.actor_user_id, 'Office user');
      const expectedSessionVersion = requiredPositiveInteger(
        input.expected_session_version,
        'Correction version',
      );
      const replacement = input.replacement_source && typeof input.replacement_source === 'object'
        ? input.replacement_source : {};
      if (typeof dependencies.loadFileBytes !== 'function') {
        fail('WEEKLY_SOURCE_FILE_STORAGE_UNAVAILABLE', 'The replacement source cannot be read.', 503);
      }
      const bytes = await dependencies.loadFileBytes(input.env, text(replacement.file_key), {
        maximumBytes: 25 * 1024 * 1024,
      });
      const body = {
        ...replacement,
        source_cycle_id: input.source_cycle_id,
        report_scope_id: input.report_scope_id,
        source_group_id: replacement.source_group_id,
        client_id: replacement.client_id,
        purpose: 'FINAL_SOURCE_CORRECTION',
        correction_session_id: input.correction_session_id,
        expected_correction_session_version: expectedSessionVersion,
      };
      const context = validateContext(await rpc(
        dependencies,
        'weekly_source_upload_context_v1',
        scopeRequest(body, actorUserId),
      ));
      requireSelectedClient(context);
      if (typeof dependencies.parseWeeklySourceFile !== 'function') {
        fail('WEEKLY_SOURCE_PARSER_UNAVAILABLE', 'The source parser is unavailable.', 503);
      }
      const parsed = await dependencies.parseWeeklySourceFile(bytes, parserOptions(body, context));
      const staged = await stageParsed(dependencies, body, parsed, context, actorUserId);
      const publishedContext = staged.publishedContext;
      if (!publishedContext || publishedContext.correction_session_id !== input.correction_session_id
          || publishedContext.projection_publication_id !== staged.published.publication_id
          || publishedContext.projection_state !== 'CORRECTION_READY') {
        fail('WEEKLY_SOURCE_CORRECTION_PUBLICATION_STALE', 'The replacement source changed before it was prepared.');
      }
      const correctionVersion = Number(publishedContext.correction_session_version);
      if (!Number.isSafeInteger(correctionVersion) || correctionVersion < 1) {
        fail('WEEKLY_SOURCE_CORRECTION_VERSION_INVALID', 'The replacement source version is invalid.', 502);
      }
      return {
        ok: true,
        status: 'CORRECTION_READY',
        correction_session_id: input.correction_session_id,
        replacement_upload_id: staged.uploadId,
        replacement_projection_publication_id: staged.published.publication_id,
        expected_authority_scope_version: staged.version,
        expected_row_manifest_hash: requiredSha256(
          publishedContext.row_manifest_hash ?? staged.sealed.row_manifest_hash,
          'Replacement row proof',
        ),
        expected_comparison_manifest_hash: requiredSha256(
          publishedContext.comparison_manifest_hash ?? staged.published.comparison_manifest_hash,
          'Replacement comparison proof',
        ),
        expected_issue_set_hash: requiredSha256(
          publishedContext.issue_set_hash ?? staged.published.issue_set_hash,
          'Replacement issue proof',
        ),
        version: correctionVersion,
        idempotent_replay: staged.started.status === 'DUPLICATE' || staged.published.idempotent === true,
      };
    },

    async rebuildReplacementProjection(input = {}) {
      const actorUserId = requiredUuid(input.actor_user_id, 'Office user');
      const correctionSessionId = requiredUuid(input.correction_session_id, 'Correction session');
      const replacementUploadId = requiredUuid(input.replacement_upload_id, 'Replacement upload');
      const priorPublicationId = requiredUuid(
        input.replacement_projection_publication_id,
        'Replacement comparison',
      );
      const expectedSessionVersion = requiredPositiveInteger(
        input.expected_session_version,
        'Correction version',
      );
      const expectedAuthorityScopeVersion = requiredPositiveInteger(
        input.expected_authority_scope_version,
        'Authority scope version',
      );
      const idempotencyKey = text(input.idempotency_key);
      if (idempotencyKey.length < 16 || idempotencyKey.length > 200) {
        fail('WEEKLY_SOURCE_CORRECTION_REBUILD_REQUEST_INVALID', 'The recheck reference is invalid.', 400);
      }
      const expectedRowManifestHash = requiredSha256(
        input.expected_row_manifest_hash,
        'Replacement row proof',
      );
      const expectedComparisonManifestHash = requiredSha256(
        input.expected_comparison_manifest_hash,
        'Replacement comparison proof',
      );
      const expectedIssueSetHash = requiredSha256(
        input.expected_issue_set_hash,
        'Replacement issue proof',
      );
      const body = {
        source_cycle_id: input.source_cycle_id,
        report_scope_id: input.report_scope_id,
        purpose: 'FINAL_SOURCE_CORRECTION',
        correction_session_id: correctionSessionId,
      };
      const published = await rebuildCorrectionProjection(dependencies, {
        correction_session_id: correctionSessionId,
        replacement_upload_id: replacementUploadId,
        replacement_projection_publication_id: priorPublicationId,
        expected_session_version: expectedSessionVersion,
        expected_authority_scope_version: expectedAuthorityScopeVersion,
        expected_row_manifest_hash: expectedRowManifestHash,
        expected_comparison_manifest_hash: expectedComparisonManifestHash,
        expected_issue_set_hash: expectedIssueSetHash,
        idempotency_key: idempotencyKey,
      }, body, actorUserId);
      const publishedContext = validateContext(await rpc(
        dependencies,
        'weekly_source_upload_context_v1',
        scopeRequest(body, actorUserId, 'BUILD_PROJECTION', replacementUploadId),
      ));
      if (publishedContext.correction_session_id !== correctionSessionId
          || publishedContext.upload_id !== replacementUploadId
          || publishedContext.projection_publication_id !== published.publication_id
          || publishedContext.projection_state !== 'CORRECTION_READY'
          || publishedContext.row_manifest_hash !== expectedRowManifestHash) {
        fail(
          'WEEKLY_SOURCE_CORRECTION_REBUILD_STALE',
          'The rebuilt replacement comparison changed before review.',
        );
      }
      const readySessionVersion = requiredPositiveInteger(
        published.ready_session_version,
        'Rebuilt correction version',
      );
      return {
        ok: true,
        status: 'CORRECTION_READY',
        correction_session_id: correctionSessionId,
        replacement_upload_id: replacementUploadId,
        replacement_projection_publication_id: requiredUuid(
          published.publication_id,
          'Rebuilt replacement comparison',
        ),
        expected_authority_scope_version: expectedAuthorityScopeVersion,
        expected_row_manifest_hash: expectedRowManifestHash,
        expected_comparison_manifest_hash: requiredSha256(
          publishedContext.comparison_manifest_hash ?? published.comparison_manifest_hash,
          'Rebuilt comparison proof',
        ),
        expected_issue_set_hash: requiredSha256(
          publishedContext.issue_set_hash ?? published.issue_set_hash,
          'Rebuilt issue proof',
        ),
        version: readySessionVersion,
        idempotent_replay:
          published.rebuild_begin_idempotent === true || published.idempotent === true,
      };
    },
  });
}
