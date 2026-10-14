const excludedStatuses = new Set(['DRAFT', 'VOID', 'VOIDED', 'CANCELLED', 'CANCELED']);
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const object = value => Boolean(value && typeof value === 'object' && !Array.isArray(value));
const pageSize = 500;
const maximumRows = 100000;
const importedCollections = new Set(['HR_WEEKLY', 'HEALTHROSTER_WEEKLY', 'NHSP_WEEKLY', 'IMPORTED_WEEKLY', 'IMPORTED_WEEKLY_MIXED']);
const sourceMetadata = new Set(['source_system', 'source_systems', 'evidence_kind']);
const timesheetWitnesses = ['authorised_at_server', 'r2_nurse_key', 'r2_auth_key', 'img_sha256_nurse',
  'img_sha256_auth', 'qr_signed_at_utc', 'qr_signed_hash', 'candidate_manager_approved_at_utc'];
const financialWitnesses = ['processed_at_utc', 'authorised_at_utc', 'locked_by_invoice_id', 'paid_at_utc'];
const financialAmounts = ['total_hours', 'total_pay_ex_vat', 'total_charge_ex_vat'];

export async function readContractWorkedDatePage(url, { headers, timeoutMs = 8000, fetcher = globalThis.fetch }) {
  try {
    const response = await fetcher(url, { method: 'GET', headers: { ...headers, Prefer: 'count=exact' },
      signal: AbortSignal.timeout(timeoutMs) });
    if (!response.ok) unavailable();
    const rows = await response.json();
    const range = /\/(\d+)$/.exec(response.headers.get('content-range') || '');
    if (!Array.isArray(rows) || !range) unavailable();
    return { rows, total: Number(range[1]) };
  } catch { unavailable(); }
}

function unavailable() {
  const error = new Error('Could not verify the recorded work dates. Please try again before saving.');
  error.code = 'CONTRACT_WORKED_DATES_UNAVAILABLE';
  throw error;
}
function date(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) unavailable();
  const parsed = new Date(`${value}T12:00:00Z`);
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== value) unavailable();
  return value;
}
function json(value) {
  if (typeof value !== 'string') return value;
  try { return JSON.parse(value); } catch { unavailable(); }
}
function rowDate(row, keys = ['date', 'work_date']) {
  if (!object(row)) unavailable();
  const values = keys.filter(key => row[key] != null).map(key => date(row[key]));
  if (!values.length || new Set(values).size !== 1) unavailable();
  return values[0];
}
function dailyDate(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T/.test(value)
      || !/(?:Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value))) unavailable();
  const parts = new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit' })
    .formatToParts(new Date(value));
  const part = kind => parts.find(item => item.type === kind).value;
  return date(`${part('year')}-${part('month')}-${part('day')}`);
}

function hasWorkWitness(row, financials) {
  if (!Object.hasOwn(row, 'worked_minutes') || row.worked_minutes != null
      && (!Number.isInteger(row.worked_minutes) || row.worked_minutes < 0)) unavailable();
  if (timesheetWitnesses.some(key => !Object.hasOwn(row, key))) unavailable();
  if (!Object.hasOwn(row, 'worked_end_iso')) unavailable();
  let witness = row.worked_minutes > 0 || row.worked_end_iso != null || timesheetWitnesses.some(key => row[key] != null);
  for (const financial of financials) {
    if (financialWitnesses.some(key => !Object.hasOwn(financial, key))
        || financialAmounts.some(key => !Object.hasOwn(financial, key) || financial[key] == null
          || !Number.isFinite(Number(financial[key]))) || typeof financial.processing_status !== 'string') unavailable();
    witness ||= financialWitnesses.some(key => financial[key] != null)
      || financialAmounts.some(key => Number(financial[key]) !== 0)
      || financial.processing_status === 'READY_FOR_INVOICE';
  }
  return witness;
}

function financialDates(financial) {
  const dates = [];
  if (!Object.hasOwn(financial, 'actual_minutes_by_day_json') || !Object.hasOwn(financial, 'external_source_rows_json')) unavailable();
  const minutes = json(financial.actual_minutes_by_day_json);
  if (minutes != null) {
    if (Array.isArray(minutes)) {
      dates.push(...minutes.map(item => rowDate(item, ['date', 'work_date', 'ymd'])));
    } else if (object(minutes)) dates.push(...Object.keys(minutes).map(date));
    else unavailable();
  }
  const source = json(financial.external_source_rows_json);
  if (source == null) return dates;
  if (Array.isArray(source)) return [...dates, ...source.map(item => rowDate(item))];
  if (!object(source)) unavailable();
  if (source.schema_version === 'WEEKLY_SOURCE_ORDINARY_EXTERNAL_ROWS_V1') {
    if (!Array.isArray(source.segments)) unavailable();
    // This producer stores lineage and economic vectors, not arbitrary raw-row
    // dates. Read only its declared work clocks when present; actual schedule
    // and current financial day keys remain separate union members.
    for (const segment of source.segments) {
      if (!object(segment)) unavailable();
      for (const clocks of [segment.pay_vector?.clocks, segment.charge_vector?.clocks]) {
        if (clocks?.work_date != null || clocks?.date != null) dates.push(rowDate(clocks));
      }
    }
    return dates;
  }
  for (const [key, value] of Object.entries(source)) {
    if (sourceMetadata.has(key)) continue;
    if (!importedCollections.has(key) || !Array.isArray(value)) unavailable();
    dates.push(...value.map(item => rowDate(item)));
  }
  return dates;
}

/** Every source must be complete. Missing counts, truncated/changed pagination
 * or a failed read cannot turn the Contract date guard into an empty range. */
async function allRows(baseUrl, table, filters, identity, readPage) {
  const rows = []; const seen = new Set(); let expected;
  for (let offset = 0; offset < maximumRows;) {
    const url = new URL(`${baseUrl.replace(/\/$/, '')}/rest/v1/${table}`);
    for (const [key, value] of Object.entries({ ...filters, order: `${identity}.asc`, limit: pageSize, offset })) url.searchParams.set(key, String(value));
    const result = await readPage(url.toString(), { preferExactCount: true, timeoutMs: 8000 });
    if (!Array.isArray(result?.rows) || !Number.isSafeInteger(result.total) || result.total < 0 || result.total > maximumRows) unavailable();
    if (expected === undefined) expected = result.total;
    if (expected !== result.total || rows.length + result.rows.length > expected || result.rows.length > pageSize
        || (!result.rows.length && rows.length < expected)) unavailable();
    for (const row of result.rows) {
      if (!object(row) || !uuid.test(row[identity]) || seen.has(row[identity])) unavailable();
      seen.add(row[identity]); rows.push(row);
    }
    if (rows.length === expected) return rows;
    offset += result.rows.length;
  }
  unavailable();
}

export function protectedContractWorkedDates(timesheets, financials, shifts, contractId) {
  const records = [];
  const protectedRows = timesheets.filter(row => {
    if (row.contract_id !== contractId) unavailable();
    if (typeof row.is_current !== 'boolean' || typeof row.status !== 'string'
        || !['DAILY', 'WEEKLY'].includes(row.sheet_scope) || !['HOURS', 'EXPENSES', 'MILEAGE'].includes(row.line_type)
        || !Object.hasOwn(row, 'revoked_at') || !Object.hasOwn(row, 'archived_at_utc')) unavailable();
    return row.is_current === true && row.revoked_at == null && row.archived_at_utc == null
      && !excludedStatuses.has(String(row.status || '').trim().toUpperCase());
  });
  const protectedIds = new Set(protectedRows.map(row => row.timesheet_id));
  for (const shift of shifts) {
    if (shift.contract_id !== contractId || !Object.hasOwn(shift, 'cancelled_at_utc')) unavailable();
    if (shift.cancelled_at_utc != null) continue;
    records.push({ timesheet_id: shift.timesheet_id || null, date: date(shift.work_date), status: null, client_id: shift.client_id || null });
  }
  for (const row of protectedRows) {
    const dates = [];
    const schedule = json(row.actual_schedule_json);
    if (schedule != null) {
      if (!Array.isArray(schedule)) unavailable();
      dates.push(...schedule.map(item => rowDate(item)));
    }
    if (row.sheet_scope === 'DAILY' && row.worked_start_iso != null) dates.push(dailyDate(row.worked_start_iso));
    const currentFinancials = financials.filter(financial => financial.timesheet_id === row.timesheet_id && financial.is_current === true);
    for (const financial of currentFinancials) dates.push(...financialDates(financial));
    dates.push(...records.filter(record => record.timesheet_id === row.timesheet_id).map(record => record.date));
    // A completely read, explicitly empty unsigned record has no worked
    // interval (including a new RECEIVED P0). Historical hours that claim work
    // but have lost their dates remain refused; week ending is never a fallback.
    if (!dates.length && row.line_type === 'HOURS'
        && (!Array.isArray(schedule) || schedule.length || !Object.hasOwn(row, 'worked_start_iso')
          || row.worked_start_iso != null || hasWorkWitness(row, currentFinancials))) unavailable();
    for (const workDate of new Set(dates)) records.push({ timesheet_id: row.timesheet_id, date: workDate,
      status: row.status || null, client_id: currentFinancials.find(financial => financial.client_id)?.client_id || null });
  }
  for (const financial of financials) {
    if (!protectedIds.has(financial.timesheet_id) || typeof financial.is_current !== 'boolean') unavailable();
  }
  const unique = new Map(records.map(record => [`${record.timesheet_id || 'shift'}:${record.date}`, record]));
  const dates = [...unique.values()].map(record => record.date).sort();
  return { min_ts_date: dates[0] || null, max_ts_date: dates.at(-1) || null, records: [...unique.values()] };
}

export async function readContractWorkedDateBoundary({ baseUrl, contractId, readPage }) {
  try {
    if (!uuid.test(contractId) || typeof readPage !== 'function') unavailable();
    const timesheets = await allRows(baseUrl, 'timesheets', {
      contract_id: `eq.${contractId}`, is_current: 'eq.true', revoked_at: 'is.null', archived_at_utc: 'is.null',
      select: `timesheet_id,contract_id,status,is_current,revoked_at,archived_at_utc,sheet_scope,line_type,actual_schedule_json,worked_start_iso,worked_end_iso,worked_minutes,${timesheetWitnesses.join(',')}`
    }, 'timesheet_id', readPage);
    const protectedIds = timesheets.filter(row => !excludedStatuses.has(String(row.status || '').trim().toUpperCase())).map(row => row.timesheet_id);
    const financials = [];
    for (let offset = 0; offset < protectedIds.length; offset += 50) {
      financials.push(...await allRows(baseUrl, 'timesheets_financials', {
        timesheet_id: `in.(${protectedIds.slice(offset, offset + 50).join(',')})`, is_current: 'eq.true',
        select: `id,timesheet_id,is_current,client_id,actual_minutes_by_day_json,external_source_rows_json,processing_status,${financialWitnesses.join(',')},${financialAmounts.join(',')}`
      }, 'id', readPage));
    }
    const shifts = await allRows(baseUrl, 'nhsp_shifts', {
      contract_id: `eq.${contractId}`, cancelled_at_utc: 'is.null', select: 'id,contract_id,timesheet_id,client_id,work_date,cancelled_at_utc'
    }, 'id', readPage);
    return protectedContractWorkedDates(timesheets, financials, shifts, contractId);
  } catch { unavailable(); }
}

export function contractWorkedDateRefusal(boundary, startDate, endDate) {
  const start = date(startDate); const end = date(endDate);
  const display = value => `${value.slice(8, 10)}/${value.slice(5, 7)}/${value.slice(0, 4)}`;
  if (boundary.min_ts_date && start > boundary.min_ts_date) return `The Contract cannot start after ${display(boundary.min_ts_date)}, when work was recorded.`;
  if (boundary.max_ts_date && end < boundary.max_ts_date) return `The Contract cannot end before ${display(boundary.max_ts_date)}, when work was recorded.`;
  return null;
}
