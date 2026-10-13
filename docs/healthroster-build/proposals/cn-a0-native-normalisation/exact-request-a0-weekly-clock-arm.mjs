import {buildNativeWeeklyCorrectionSnapshot} from './native-weekly-correction-dispatch.mjs';
import {canonicalCorrectionWire} from './native-correction-snapshot-producers.mjs';
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const uuid=v=>typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);
const same=(a,b)=>uuid(a)&&uuid(b)&&a.toLowerCase()===b.toLowerCase();
const sha=v=>typeof v==='string'&&/^[0-9a-f]{64}$/.test(v);
const jsonEqual=(a,b)=>JSON.stringify(canonicalCorrectionWire(a))===JSON.stringify(canonicalCorrectionWire(b));
const scopeKeys=['contract','request_id','original_timesheet_id','original_version','original_booking_id','candidate_id','client_id',
  'contract_id','week_ending_date','pricing_origin','rule_set_version_id','settings_authority_fingerprint',
  'timesheet_authority_hash','contract_input','controlled_scope','scope_sha256'];
const controlledKeys=['source_group_id','source_group_version','membership_id','membership_valid_from','membership_valid_to',
  'assignment_id','assignment_version','activation_release_id','activation_request_id','activation_request_sha256','activation_receipt_sha256'];
const closed=(v,keys)=>object(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
function refuse(){const e=new Error('CORRECTION_A0_NATIVE_CLOCK_SOURCE_UNPROVED');e.code=e.message;e.status=409;throw e;}

// DISTINCT engineering review arm. Ordinary TSFIN dispatch and its refusal
// remain unchanged. The new private SQL scope owner is included in PREPARE's
// fingerprint and re-read by the same atomic unit. It qualifies only the real
// enabled NULL-rule SIGNED_CHECK_ONLY scope, never a Source-priced fallback.
// Root still owns first-use financial, lock, client and readiness acceptance.
export async function buildExactRequestA0WeeklyClockSnapshot(input,owners){
  if(input.correctionPurpose!=='MISSING_ORIGINAL_A0')return buildNativeWeeklyCorrectionSnapshot(input,owners);
  const {authority,ts,curFin,cw,contract,policySql,shifts,frozenOriginal}=input;
  if(!object(authority)||!object(frozenOriginal)||!object(ts)||!object(cw)||!object(contract)||
    authority.origin!=='SIGNED_TIMESHEET_CORRECTION'||
    !same(ts.timesheet_id,authority.original_timesheet_id)||ts.version!==authority.original_timesheet_version||
    !same(frozenOriginal.timesheet?.timesheet_id,ts.timesheet_id)||frozenOriginal.timesheet?.booking_id!==ts.booking_id||
    !jsonEqual(frozenOriginal.timesheet.actual_schedule_json,ts.actual_schedule_json)||
    !jsonEqual(frozenOriginal.timesheet.settings_authority_json,ts.settings_authority_json)||
    !same(contract.candidate_id,input.candidateId)||!same(contract.client_id,input.clientId)||
    !same(ts.contract_id,contract.id)||!same(cw.contract_id,contract.id)||!same(cw.timesheet_id,ts.timesheet_id)||
    ts.sheet_scope!=='WEEKLY'||ts.is_current!==true||ts.archived_at_utc!=null||ts.authorised_at_server!=null||
    curFin!=null||!Array.isArray(shifts)||shifts.length!==0||!object(policySql)||
    !object(ts.settings_authority_json?.values)||!Array.isArray(ts.actual_schedule_json)||ts.actual_schedule_json.length===0||
    authority.original_financial_id!==null||authority.original_financial_authority_hash!==null||
    authority.original_invoice_id!==null||authority.return_invoice_id!==null)refuse();
  // PREPARE proves the immutable workflow/evidence pair. Its new scope owner
  // proves absence of all FIN, active imports and physical Source lineage,
  // dated membership/activation, and the unchanged native frozen settings.
  const scope=authority.original_a0_clock_scope,controlled=scope?.controlled_scope;
  if(!object(frozenOriginal.manager_signed_witness)||!closed(scope,scopeKeys)||!closed(controlled,controlledKeys)||
    scope.contract!=='TIMESHEET_CORRECTION_A0_CLOCK_SCOPE_V1'||scope.pricing_origin!=='SIGNED_CHECK_ONLY_CONTRACT_RATES'||
    scope.rule_set_version_id!==null||!same(scope.request_id,authority.request_id)||
    !same(scope.original_timesheet_id,ts.timesheet_id)||scope.original_version!==ts.version||
    scope.original_booking_id!==ts.booking_id||!same(scope.candidate_id,input.candidateId)||!same(scope.client_id,input.clientId)||
    !same(scope.contract_id,contract.id)||scope.week_ending_date!==ts.week_ending_date||
    !sha(scope.settings_authority_fingerprint)||scope.settings_authority_fingerprint!==ts.settings_authority_json.authority_fingerprint||
    !sha(scope.timesheet_authority_hash)||!sha(scope.scope_sha256)||!jsonEqual(scope.contract_input,contract)||
    !uuid(controlled.source_group_id)||!uuid(controlled.membership_id)||!uuid(controlled.assignment_id)||
    !/^[1-9]\d*$/.test(controlled.source_group_version)||typeof controlled.source_group_version!=='string'||
    !/^[1-9]\d*$/.test(controlled.assignment_version)||typeof controlled.assignment_version!=='string'||
    typeof controlled.activation_release_id!=='string'||controlled.activation_release_id.length===0||
    !uuid(controlled.activation_request_id)||!sha(controlled.activation_request_sha256)||!sha(controlled.activation_receipt_sha256)||
    !/^\d{4}-\d{2}-\d{2}$/.test(controlled.membership_valid_from)||
    !(controlled.membership_valid_to===null||/^\d{4}-\d{2}-\d{2}$/.test(controlled.membership_valid_to)))refuse();
  if(typeof owners?.buildWeeklyScheduleSegmentsSnapshot!=='function'||typeof owners?.enrichTsfinWithHrCrosscheck!=='function')refuse();
  const res=await owners.buildWeeklyScheduleSegmentsSnapshot(input.env,ts,cw,contract,null,{
    outbox_id:null,write_now:false,policy_override:policySql,hr_eff_flags:input.effFlags,
    hr_preloaded_shifts:shifts,hr_validation_status:input.validation?.status??null,
    hr_validation_reason_code:input.validation?.reason_code??null
  });
  if(res?.ok!==true||!object(res.snapshot))refuse();
  const snapshot=res.snapshot;
  if(snapshot.occupant_key_norm==null&&ts.occupant_key_norm!=null)snapshot.occupant_key_norm=ts.occupant_key_norm;
  await owners.enrichTsfinWithHrCrosscheck(input.env,ts,snapshot,input.effFlags,shifts);
  return snapshot;
}
