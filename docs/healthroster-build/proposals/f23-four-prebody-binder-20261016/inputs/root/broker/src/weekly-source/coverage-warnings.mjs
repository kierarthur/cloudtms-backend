const EXPORTS = new Set(['HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
  'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1', 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1']);

function invalid() {
  const error = new Error('The import period could not be checked. Refresh the upload before trying again.');
  error.code = 'WEEKLY_SOURCE_COVERAGE_CONTEXT_INVALID';
  throw error;
}
function date(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) invalid();
  const parsed = new Date(`${value}T12:00:00Z`);
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== value) invalid();
  return value;
}
const display = value => `${value.slice(8)}/${value.slice(5, 7)}/${value.slice(0, 4)}`;

function savedCoverage(context) {
  const facts = context?.coverage_context;
  if (facts == null) return null;
  if (facts.contract !== 'WEEKLY_SOURCE_UPLOAD_COVERAGE_CONTEXT_V1'
    || typeof facts.has_current_final_source !== 'boolean' || typeof facts.lookback_warning_enabled !== 'boolean') invalid();
  date(facts.coverage_floor_local_date);
  for (const key of ['oldest_protected_work_date', 'last_completed_contract_week']) {
    if (!Object.hasOwn(facts, key)) invalid();
    if (facts[key] != null) date(facts[key]);
  }
  return facts;
}

function range(parsed, body) {
  const dates = (parsed.rows ?? []).map(row => date(row.date ?? row.workDate)).sort();
  const supplied = body.coverage ?? {};
  const start = supplied.start_local_date ?? supplied.confirmed_start_local_date ?? dates[0];
  const end = supplied.end_local_date ?? supplied.confirmed_end_local_date ?? dates.at(-1);
  if (start == null || end == null) return null;
  date(start); date(end); if (start > end) invalid();
  return { start, end, first: dates[0], last: dates.at(-1) };
}

export function assertWeeklySourceCoverageFloor(parsed, body, context) {
  if (!EXPORTS.has(parsed?.profileId)) return;
  const facts = savedCoverage(context); if (!facts) return;
  const coverage = range(parsed, body);
  if (coverage && coverage.start < facts.coverage_floor_local_date) {
    const error = new Error(`Import coverage starts from ${display(facts.coverage_floor_local_date)}, the first week using these settings.`);
    error.code = 'WEEKLY_SOURCE_COVERAGE_BEFORE_RULE_SET';
    throw error;
  }
}

export function weeklySourceCoverageWarnings(parsed, body = {}, context = {}) {
  if (!EXPORTS.has(parsed?.profileId)) return [];
  const facts = savedCoverage(context); if (!facts || parsed?.ok !== true) return [];
  const coverage = range(parsed, body); if (!coverage) return [];
  const warnings = [];
  const add = (code, message) => warnings.push({ code, message, tone: 'warning', action_label: 'Upload anyway' });
  if (facts.lookback_warning_enabled && facts.oldest_protected_work_date
      && coverage.start > facts.oldest_protected_work_date) {
    add('WEEKLY_SOURCE_LOOKBACK_AFTER_PROTECTED',
      `This period starts after the oldest protected shift on ${display(facts.oldest_protected_work_date)}.`);
  }
  if (facts.last_completed_contract_week && coverage.end > facts.last_completed_contract_week) {
    add('WEEKLY_SOURCE_COVERAGE_AFTER_COMPLETED_WEEK',
      `This period ends after the last completed Contract week on ${display(facts.last_completed_contract_week)}.`);
  }
  if (facts.has_current_final_source && (!coverage.first
      || coverage.start < coverage.first || coverage.end > coverage.last)) {
    add('WEEKLY_SOURCE_WIDER_COVERAGE_CANCELLATIONS',
      'Finalised shifts missing from this file inside the wider period will be cancelled.');
  }
  return warnings;
}
