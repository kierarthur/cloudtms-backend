import { sourceNativeClosed } from './source-native-contract.mjs';

// Exact current output of buildWeeklyScheduleSegmentsSnapshot. The optional
// three HR fields are authored by its existing enrichment owner. This decoder
// validates shape/current inputs and retains every value; it calculates no pay.
export const SOURCE_NATIVE_SNAPSHOT_KEYS = Object.freeze([
  'timesheet_id','timesheet_version','basis','candidate_id','client_id','role','band','pay_method',
  'policy_snapshot_json','rate_source_refs_json','hours_day','hours_night','hours_sat','hours_sun','hours_bh','total_hours',
  'pay_day','pay_night','pay_sat','pay_sun','pay_bh','charge_day','charge_night','charge_sat','charge_sun','charge_bh',
  'additional_units_json','additional_pay_ex_vat','additional_charge_ex_vat','additional_margin_ex_vat',
  'total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat','expenses_pay_ex_vat','expenses_charge_ex_vat',
  'expenses_description','expenses_evidence_r2_key','expenses_evidence_manifest','mileage_units','mileage_pay_ex_vat',
  'mileage_charge_ex_vat','mileage_evidence_r2_key','mileage_evidence_manifest','mileage_pay_rate','mileage_charge_rate',
  'candidate_assignment','processing_status','invoice_breakdown_json',
]);
const enrichment = ['hr_crosscheck_status','hr_crosscheck_issues','external_source_rows_json'];
const segmentKeys = ['segment_id','date','start','end','overnight','start_utc','end_utc','break_mins','ref_num','breaks',
  'hours_day','hours_night','hours_sat','hours_sun','hours_bh','pay_amount','charge_amount','is_reversal','exclude_from_pay'];
const buckets = ['day','night','sat','sun','bh'];
const uuid = v => typeof v === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(v);
const number = v => typeof v === 'number' && Number.isFinite(v);
const numeric = v => number(v) || typeof v === 'string' && /^-?(0|[1-9]\d*)(?:\.\d+)?$/.test(v);
const text = v => typeof v === 'string' && !v.includes('\0');
const nullable = (v, test) => v === null || test(v);
const object = v => v !== null && typeof v === 'object' && !Array.isArray(v);
const date = v => typeof v === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(v) && Number.isFinite(Date.parse(`${v}T00:00:00Z`))
  && new Date(`${v}T00:00:00Z`).toISOString().slice(0,10) === v;
const clock = v => typeof v === 'string' && /^(?:[01]\d|2[0-3]):[0-5]\d$/.test(v);
const instant = v => typeof v === 'string' && /(?:Z|[+-]\d\d:\d\d)$/.test(v) && Number.isFinite(Date.parse(v));
function invalid() { const e = new Error('The native Source calculation snapshot is invalid.');e.code = 'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID';e.status = 409;throw e; }
function closed(value, required, optional = []) {
  return object(value) && required.every(k => Object.hasOwn(value,k)) && Object.keys(value).every(k => required.includes(k) || optional.includes(k));
}
function sameJson(a,b) {
  if (a === b) return true;
  if (a === null || b === null || typeof a !== typeof b || typeof a !== 'object' || Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = Object.keys(a);return keys.length === Object.keys(b).length && keys.every(k => Object.hasOwn(b,k) && sameJson(a[k],b[k]));
}
function preview(value) {
  const keys = ['schema_version','calculator_owner','calculator_version','mode','category','sign','bucket_minutes','paid_minutes','break_minutes',
    'hours','rates','charge_rates','pay_ex_vat','charge_ex_vat'];
  if (!sourceNativeClosed(value,keys) || value.schema_version !== 'WEEKLY_PROTECTED_NEXT_PREVIEW_V1'
      || value.calculator_owner !== 'buildWeeklyScheduleSegmentsSnapshot' || value.calculator_version !== 'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1'
      || !['SPLIT_RATE_WINDOWS','WHOLE_SHIFT_START_DAY'].includes(value.mode)
      || (value.mode === 'SPLIT_RATE_WINDOWS' ? value.category !== null : !buckets.includes(value.category)) || ![1,-1].includes(value.sign)
      || !['paid_minutes','break_minutes'].every(k => Number.isSafeInteger(value[k]) && value[k] >= 0)
      || !['bucket_minutes','hours','rates','charge_rates'].every(k => sourceNativeClosed(value[k],buckets))
      || !buckets.every(k => Number.isSafeInteger(value.bucket_minutes[k]) && value.bucket_minutes[k] >= 0
        && typeof value.hours[k] === 'string' && numeric(value.hours[k])
        && nullable(value.rates[k],v => typeof v === 'string' && numeric(v))
        && nullable(value.charge_rates[k],v => typeof v === 'string' && numeric(v)))
      || !['pay_ex_vat','charge_ex_vat'].every(k => typeof value[k] === 'string' && /^-?(0|[1-9]\d*)\.\d{2}$/.test(value[k]))) invalid();
}

export function decodeSourceNativeCalculationSnapshot(value, { current, nextRoute = false } = {}) {
  if (!closed(value,SOURCE_NATIVE_SNAPSHOT_KEYS,enrichment) || !object(current)
      || !uuid(value.timesheet_id) || value.timesheet_id !== current.root_timesheet_id
      || !Number.isSafeInteger(value.timesheet_version) || value.timesheet_version < 1 || value.timesheet_version !== current.timesheet?.version
      || value.candidate_id !== current.candidate_id || value.client_id !== current.client_id || !uuid(value.candidate_id) || !uuid(value.client_id)
      || !text(value.basis) || !value.basis || !['role','band','pay_method','expenses_description','expenses_evidence_r2_key','mileage_evidence_r2_key'].every(k => nullable(value[k],text))
      || value.candidate_assignment !== 'ASSIGNED' || !['PENDING_AUTH','READY_FOR_INVOICE'].includes(value.processing_status)
      || !object(value.additional_units_json) || !sameJson(value.policy_snapshot_json,current.policy)
      || !sameJson(value.rate_source_refs_json,{ mode:'CONTRACT_RATES_JSON',contract_id:current.contract_id })) invalid();
  const rates = buckets.flatMap(k => [`pay_${k}`,`charge_${k}`]);
  const amounts = ['total_hours',...buckets.map(k => `hours_${k}`),'additional_pay_ex_vat','additional_charge_ex_vat','additional_margin_ex_vat',
    'total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat','expenses_pay_ex_vat','expenses_charge_ex_vat','mileage_units','mileage_pay_ex_vat','mileage_charge_ex_vat'];
  if (!amounts.every(k => number(value[k])) || !rates.every(k => nullable(value[k],number))
      || !['mileage_pay_rate','mileage_charge_rate'].every(k => nullable(value[k],numeric))
      || !['expenses_evidence_manifest','mileage_evidence_manifest'].every(k => value[k] === null || object(value[k]) || Array.isArray(value[k]))
      || Object.hasOwn(value,'hr_crosscheck_status') && !nullable(value.hr_crosscheck_status,text)
      || Object.hasOwn(value,'hr_crosscheck_issues') && !(value.hr_crosscheck_issues === null || Array.isArray(value.hr_crosscheck_issues) && value.hr_crosscheck_issues.every(text))
      || Object.hasOwn(value,'external_source_rows_json') && !object(value.external_source_rows_json)) invalid();
  const breakdown = value.invoice_breakdown_json;
  if (!closed(breakdown,['mode','segments','totals'],['additional']) || breakdown.mode !== 'SEGMENTS' || !Array.isArray(breakdown.segments)
      || !sourceNativeClosed(breakdown.totals,['total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat'])
      || !Object.keys(breakdown.totals).every(k => number(breakdown.totals[k]) && breakdown.totals[k] === value[k])) invalid();
  if (Object.hasOwn(breakdown,'additional') && (!sourceNativeClosed(breakdown.additional,['units','pay_ex_vat','charge_ex_vat','margin_ex_vat'])
      || !sameJson(breakdown.additional.units,value.additional_units_json)
      || !['pay_ex_vat','charge_ex_vat','margin_ex_vat'].every(k => number(breakdown.additional[k]) && breakdown.additional[k] === value[`additional_${k}`]))) invalid();
  const events = new Set();
  for (const segment of breakdown.segments) {
    const optional = ['invoice_target_week_start','invoice_locked_invoice_id',...(nextRoute ? ['protected_next_preview'] : [])];
    const required = [...segmentKeys,...(nextRoute ? ['work_event_id','protected_target_state'] : ['weekly_protected_component_identity'])];
    if (!closed(segment,required,optional) || !text(segment.segment_id) || !segment.segment_id || !date(segment.date)
        || !clock(segment.start) || !clock(segment.end) || !instant(segment.start_utc) || !instant(segment.end_utc)
        || !['overnight','is_reversal','exclude_from_pay'].every(k => typeof segment[k] === 'boolean')
        || !Number.isSafeInteger(segment.break_mins) || segment.break_mins < 0 || !nullable(segment.ref_num,text)
        || !Array.isArray(segment.breaks) || ![...buckets.map(k => `hours_${k}`),'pay_amount','charge_amount'].every(k => number(segment[k]))
        || Object.hasOwn(segment,'invoice_target_week_start') && !date(segment.invoice_target_week_start)
        || Object.hasOwn(segment,'invoice_locked_invoice_id') && !uuid(segment.invoice_locked_invoice_id)) invalid();
    for (const window of segment.breaks) if (!sourceNativeClosed(window,['start','end']) || !clock(window.start) || !clock(window.end)) invalid();
    const identity = segment.weekly_protected_component_identity;
    const event = nextRoute ? segment.work_event_id : identity?.work_event_id;
    if (!uuid(event) || events.has(event)) invalid();events.add(event);
    if (nextRoute) {
      if (!['WAIT','SOURCE','ACCEPTED_SOURCE'].includes(segment.protected_target_state)) invalid();
      if (segment.protected_target_state === 'WAIT') preview(segment.protected_next_preview);
      else if (Object.hasOwn(segment,'protected_next_preview')) invalid();
    } else if (!sourceNativeClosed(identity,['schema_version','work_event_id']) || identity.schema_version !== 'WEEKLY_PROTECTED_COMPONENT_IDENTITY_V1'
        || segment.segment_id !== `weekly-source-event:${event}`) invalid();
  }
  return value;
}
