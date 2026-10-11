/** WB-36 finite W2 snooze action contract. WB-19 owns transport, active admin,
 * module admission and the single public dispatcher. No new route or retry.
 */
export const BPAY_NEXT_SNOOZE_ACTIONS=Object.freeze([
  'SNOOZE_CHECK','SNOOZE_CONTEXT','SNOOZE_WARNING_ACK_ISSUE','SNOOZE_SET','SNOOZE_CLEAR'
]);
const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const VERSION=/^(?:0|[1-9]\d{0,18})$/;
const MAX_VERSION=9223372036854775807n;
const KINDS=new Set(['DO_NOT_PAY','BLOCKED_TIMESHEET','TIMESHEET_PAYMENT',
  'OVERPAYMENT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','MANUAL_DEBT_RECOVERY']);
const SCOPES=new Set(['WHOLE_TIMESHEET','SEGMENT','EXPENSE_LINE','EXPENSES_ALL','CASE']);
const OBJECT=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const invalid=()=>{const e=new Error('BPAY_NEXT_SNOOZE_REQUEST_INVALID');e.code=e.message;e.status=400;throw e;};
const version=value=>typeof value==='string'&&VERSION.test(value)&&BigInt(value)<=MAX_VERSION;
const uuid=value=>typeof value==='string'&&UUID.test(value);
const text=(value,max)=>typeof value==='string'&&!value.includes('\0')&&[...value].length<=max;
function date(value){
  if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value)||value.startsWith('0000-'))return false;
  const parsed=new Date(value+'T00:00:00.000Z');
  return Number.isFinite(parsed.getTime())&&parsed.toISOString().slice(0,10)===value;
}
/** Validate before W2 forwards/journals the exact normalised body. Stable keys
 * retain case, even when they look like UUIDs; only typed UUID fields normalise.
 */
export function normaliseBpayNextSnoozeAction(action,args){
  if(!BPAY_NEXT_SNOOZE_ACTIONS.includes(action)||!OBJECT(args)
    ||new TextEncoder().encode(JSON.stringify(args)).byteLength>4096)invalid();
  const allowed=new Set(action==='SNOOZE_CLEAR'
    ?['request_id','snooze_id','candidate_id','expected_version']
    :['request_id','candidate_id','work_id','expected_revision_id','physical_timesheet_id','physical_timesheet_version','scope_kind','segment_key','component_key',
      'case_id','case_component_id','snooze_kind','until_date','indefinite','note','expected_version',
      'worker_pay_basis','acknowledgement_token','phase','expected_case_revision']);
  if(Object.keys(args).some(key=>!allowed.has(key))||!uuid(args.candidate_id))invalid();
  const mutation=!['SNOOZE_CHECK','SNOOZE_CONTEXT'].includes(action);
  if(mutation&&(!uuid(args.request_id)||!version(args.expected_version)))invalid();
  if(action==='SNOOZE_CLEAR'){
    if(!uuid(args.snooze_id)||Object.keys(args).length!==4)invalid();
  }else{
    const kind=typeof args.snooze_kind==='string'?args.snooze_kind.trim().toUpperCase():'';
    const canonical=kind==='BLOCKED'?'BLOCKED_TIMESHEET':kind==='LOAN_REPAYMENT'?'PAYMENT_ADVANCE_REPAYMENT':kind;
    const opening=!mutation&&args.phase==='PRE_OPEN';
    if(!SCOPES.has(args.scope_kind)||!KINDS.has(canonical)||typeof args.indefinite!=='boolean'
      ||(!opening&&(args.indefinite!==(args.until_date==null)||(!args.indefinite&&!date(args.until_date))))
      ||(opening&&args.until_date!=null&&!date(args.until_date))
      ||(args.note!==undefined&&!text(args.note,500))
      ||(args.phase!==undefined&&!['PRE_OPEN','PRE_SAVE'].includes(args.phase)))invalid();
    if(args.scope_kind==='CASE'){
      if(!uuid(args.case_id)||!uuid(args.case_component_id)||args.work_id!==undefined
        ||args.expected_revision_id!==undefined||args.physical_timesheet_id!==undefined||args.physical_timesheet_version!==undefined||args.segment_key!==undefined||args.component_key!==undefined
        ||!['OVERPAYMENT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','MANUAL_DEBT_RECOVERY'].includes(canonical)
        ||(mutation&&!version(args.expected_case_revision)))invalid();
    }else{
      const physicalOpening=opening&&uuid(args.physical_timesheet_id)&&version(args.physical_timesheet_version)&&args.physical_timesheet_version!=='0'&&BigInt(args.physical_timesheet_version)<=2147483647n&&args.scope_kind==='WHOLE_TIMESHEET'
        &&args.work_id===undefined&&args.expected_revision_id===undefined;
      if((!physicalOpening&&(!uuid(args.work_id)||!uuid(args.expected_revision_id)))||(args.physical_timesheet_id!==undefined||args.physical_timesheet_version!==undefined)&&!physicalOpening||args.case_id!==undefined
        ||args.case_component_id!==undefined||args.expected_case_revision!==undefined
        ||!['DO_NOT_PAY','BLOCKED_TIMESHEET','TIMESHEET_PAYMENT'].includes(canonical))invalid();
      if(args.scope_kind==='SEGMENT'){
        if(!text(args.segment_key,245)||args.segment_key.length===0||args.segment_key.trim()!==args.segment_key
          ||args.component_key!==undefined)invalid();
      }else if(args.segment_key!==undefined)invalid();
      if(args.scope_kind==='EXPENSE_LINE'){
        if(!text(args.component_key,256)||args.component_key.length===0||canonical!=='DO_NOT_PAY')invalid();
      }else if(args.component_key!==undefined)invalid();
      if(args.scope_kind==='EXPENSES_ALL'&&canonical!=='DO_NOT_PAY')invalid();
    }
    if(mutation&&(typeof args.worker_pay_basis!=='string'||!/^[0-9a-f]{64}$/.test(args.worker_pay_basis)
      ||(action==='SNOOZE_SET'&&args.phase==='PRE_OPEN')))invalid();
    if(args.acknowledgement_token!==undefined&&!uuid(args.acknowledgement_token))invalid();
    args={...args,snooze_kind:canonical};
  }
  return Object.freeze(Object.fromEntries(Object.entries(args).map(([key,value])=>[
    key,key.endsWith('_id')||key==='acknowledgement_token'?typeof value==='string'?value.toLowerCase():value:value
  ])));
}

const CONTEXT_FIELDS=['agency_id','candidate_id','work_id','expected_revision_id','physical_timesheet_id','physical_timesheet_version','scope_kind','scope_identity','segment_key','approved_segment_id','component_key','expense_approved_line_id','case_id','case_component_id','case_kind','snooze_kind','worker_pay_basis','snooze_id','expected_version','financial_revision','stale_control','module_epoch','control_cut','expense_count','conflict','indefinite_wins','until_date','note','active','business_date','warning_context'];
const exact=(v,keys)=>OBJECT(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
const responseInvalid=()=>{throw Object.assign(new Error('BPAY_NEXT_WORKBENCH_RESPONSE_INVALID'),{code:'BPAY_NEXT_WORKBENCH_RESPONSE_INVALID',status:503});};
export function validateBpayNextSnoozeContext(value,args,acknowledgement=false){
  const keys=acknowledgement?[...CONTEXT_FIELDS,'acknowledgement_token','acknowledgement_expires_at_utc']:CONTEXT_FIELDS;
  if(!exact(value,keys)||!uuid(value.agency_id)||value.candidate_id!==args.candidate_id||value.scope_kind!==args.scope_kind
    ||(!args.physical_timesheet_id&&(value.work_id!==(args.work_id??null)||value.expected_revision_id!==(args.expected_revision_id??null)))
    ||args.physical_timesheet_id&&(value.physical_timesheet_id!==args.physical_timesheet_id||!uuid(value.work_id)||!uuid(value.expected_revision_id))
    ||args.physical_timesheet_version&&value.physical_timesheet_version!==args.physical_timesheet_version
    ||value.case_id!==(args.case_id??null)||value.case_component_id!==(args.case_component_id??null)
    ||value.segment_key!==(args.segment_key??null)||args.scope_kind==='EXPENSE_LINE'&&value.component_key!==args.component_key
    ||!SCOPES.has(value.scope_kind)||value.snooze_kind!==args.snooze_kind||!text(value.scope_identity,600)
    ||!version(value.expected_version)||!version(value.financial_revision)||!version(value.module_epoch)||value.module_epoch==='0'||!version(value.control_cut)||!version(value.expense_count)
    ||typeof value.worker_pay_basis!=='string'||!/^[0-9a-f]{64}$/.test(value.worker_pay_basis)
    ||!date(value.business_date)||value.until_date!==null&&!date(value.until_date)||!text(value.note,500)
    ||['active','stale_control','indefinite_wins'].some(k=>typeof value[k]!=='boolean')
    ||!['SEGMENT_SNOOZES_ALREADY_EXIST','WHOLE_TIMESHEET_ALREADY_SNOOZED',null].includes(value.conflict))responseInvalid();
  for(const k of ['physical_timesheet_id','approved_segment_id','expense_approved_line_id','snooze_id'])if(value[k]!==null&&!uuid(value[k]))responseInvalid();
  if(value.segment_key!==null&&!text(value.segment_key,245)||value.component_key!==null&&!text(value.component_key,256)
    ||![null,'OVERPAYMENT','LOAN','ADVANCE','MANUAL_DEBT'].includes(value.case_kind))responseInvalid();
  if(value.physical_timesheet_version!==null&&(!version(value.physical_timesheet_version)||value.physical_timesheet_version==='0'))responseInvalid();
  if(value.active!==(value.snooze_id!==null)||value.active===(value.expected_version==='0'))responseInvalid();
  const warning=value.warning_context;
  if(!exact(warning,['phase','until_date','business_date','next_official_pay_date','configuration_fingerprint','warning_code','warning_required'])
    ||!['PRE_OPEN','PRE_SAVE'].includes(warning.phase)||!date(warning.business_date)||!date(warning.next_official_pay_date)
    ||warning.until_date!==null&&!date(warning.until_date)||!text(warning.configuration_fingerprint,256)
    ||![null,'SNOOZE_DATE_BEFORE_NEXT_PAY_RUN','SNOOZE_INCLUDES_NEXT_PAY_RUN'].includes(warning.warning_code)
    ||typeof warning.warning_required!=='boolean'||warning.warning_required!==(warning.warning_code!==null)
    ||acknowledgement&&(!uuid(value.acknowledgement_token)||typeof value.acknowledgement_expires_at_utc!=='string'
      ||value.acknowledgement_expires_at_utc.length>40||!Number.isFinite(Date.parse(value.acknowledgement_expires_at_utc))))responseInvalid();
  return value;
}
export function validateBpayNextSnoozeReceipt(receipt,action,args){
  if(!exact(receipt,['request_id','candidate_id','received','replay','result'])||receipt.request_id!==args.request_id
    ||receipt.candidate_id!==args.candidate_id||typeof receipt.received!=='boolean'||typeof receipt.replay!=='boolean'
    ||!receipt.received&&(receipt.replay||receipt.result!==null))responseInvalid();
  if(receipt.received){const r=receipt.result;
    if(action==='SNOOZE_WARNING_ACK_ISSUE')validateBpayNextSnoozeContext(r,args,true);
    else if(!exact(r,['snooze_id','version','active','control_sequence','indefinite_wins'])||!uuid(r.snooze_id)
      ||!version(r.version)||r.version==='0'||BigInt(r.version)!==BigInt(args.expected_version)+1n||!version(r.control_sequence)||r.control_sequence==='0'
      ||typeof r.indefinite_wins!=='boolean'||r.active!==(action==='SNOOZE_SET')
      ||action==='SNOOZE_CLEAR'&&r.snooze_id!==args.snooze_id)responseInvalid();
  }
  return receipt;
}
export const BPAY_NEXT_SNOOZE_REFUSALS=Object.freeze([
  'BPAY_NEXT_SNOOZE_REQUEST_INVALID','BPAY_NEXT_SNOOZE_FORBIDDEN','BPAY_NEXT_MODULE_NOT_ACTIVE',
  'BPAY_NEXT_SNOOZE_ACTION_INVALID','BPAY_NEXT_SNOOZE_CLEAR_ARGS_INVALID','BPAY_NEXT_SNOOZE_CANDIDATE_MISMATCH',
  'BPAY_NEXT_SNOOZE_CASE_AUTHORITY_MISMATCH','BPAY_NEXT_SNOOZE_EXPENSE_IDENTITY_INVALID','BPAY_NEXT_SNOOZE_EXPENSE_SCOPE_UNSUPPORTED',
  'BPAY_NEXT_SNOOZE_FINANCIAL_VERSION_STALE','BPAY_NEXT_SNOOZE_FOREIGN_SCOPE_FIELD','BPAY_NEXT_SNOOZE_REVISION_STALE',
  'BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH','BPAY_NEXT_SNOOZE_SCOPE_OR_VALUE_INVALID','BPAY_NEXT_SNOOZE_SET_REQUIRES_PRE_SAVE',
  'BPAY_NEXT_SNOOZE_VERSION_REQUIRED','BPAY_NEXT_SNOOZE_VERSION_STALE','BPAY_NEXT_SNOOZE_WARNING_ACK_REQUIRED_OR_STALE',
  'BPAY_NEXT_SNOOZE_WORK_AUTHORITY_MISMATCH','BPAY_NEXT_SNOOZE_WORKER_BASIS_UNAVAILABLE',
  'SNOOZE_DATE_IN_PAST','SNOOZE_SEGMENT_SCOPE_UNSUPPORTED','SNOOZE_SEGMENT_STALE',
  'SEGMENT_SNOOZES_ALREADY_EXIST','WHOLE_TIMESHEET_ALREADY_SNOOZED',
  'BPAY_NEXT_SNOOZE_MANY_REQUEST_INVALID','BPAY_NEXT_SNOOZE_MANY_FIRST_PAGE_REQUIRED',
  'BPAY_NEXT_SNOOZE_MANY_RETRY_NOT_FAILED','BPAY_NEXT_SNOOZE_MANY_CURSOR_INVALID'
]);
export function normaliseBpayNextSnoozeMany(action,args){
  const keys=['request_id','candidate_id','work_id','expected_revision_id','worker_pay_basis','control_cut','until_date','indefinite','note','warning_context','page_no','after_line_no','retry_line_nos'];
  if(!['SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY'].includes(action)||!exact(args,keys)||new TextEncoder().encode(JSON.stringify(args)).byteLength>24576
    ||['request_id','candidate_id','work_id','expected_revision_id'].some(k=>!uuid(args[k]))
    ||typeof args.worker_pay_basis!=='string'||!/^[0-9a-f]{64}$/.test(args.worker_pay_basis)||!version(args.control_cut)
    ||typeof args.indefinite!=='boolean'||args.indefinite!==(args.until_date===null)||args.until_date!==null&&!date(args.until_date)
    ||!text(args.note,500)||!version(args.page_no)||args.page_no==='0'
    ||args.after_line_no!==null&&(!version(args.after_line_no)||BigInt(args.after_line_no)>2147483647n)
    ||!Array.isArray(args.retry_line_nos)||args.retry_line_nos.length>100||new Set(args.retry_line_nos).size!==args.retry_line_nos.length
    ||args.retry_line_nos.some(v=>!version(v)||v==='0'||BigInt(v)>2147483647n)
    ||args.retry_line_nos.length>0&&args.after_line_no!==null
    ||action==='SNOOZE_CLEAR_MANY'&&(args.until_date!==null||!args.indefinite||args.note!==''))invalid();
  const w=args.warning_context;
  if(!exact(w,['phase','until_date','business_date','next_official_pay_date','configuration_fingerprint','warning_code','warning_required'])
    ||w.phase!=='PRE_SAVE'||w.until_date!==args.until_date||!date(w.business_date)||!date(w.next_official_pay_date)
    ||!text(w.configuration_fingerprint,256)||![null,'SNOOZE_DATE_BEFORE_NEXT_PAY_RUN','SNOOZE_INCLUDES_NEXT_PAY_RUN'].includes(w.warning_code)
    ||typeof w.warning_required!=='boolean'||w.warning_required!==(w.warning_code!==null))invalid();
  return Object.freeze({...args,...Object.fromEntries(['request_id','candidate_id','work_id','expected_revision_id'].map(k=>[k,args[k].toLowerCase()])),retry_line_nos:Object.freeze([...args.retry_line_nos]),warning_context:Object.freeze({...w})});
}
export function validateBpayNextSnoozeManyReceipt(r,args){
  if(!exact(r,['request_id','candidate_id','page_no','received','replay','result'])||r.request_id!==args.request_id||r.candidate_id!==args.candidate_id
    ||r.page_no!==args.page_no||typeof r.received!=='boolean'||typeof r.replay!=='boolean'||!r.received&&(r.replay||r.result!==null))responseInvalid();
  if(r.received){const v=r.result;
    if(!exact(v,['rows','complete','next_after_line_no','total_count','successful_count','failed_count'])||!Array.isArray(v.rows)||v.rows.length===0||v.rows.length>100
      ||typeof v.complete!=='boolean'||['total_count','successful_count','failed_count'].some(k=>!version(v[k]))||v.total_count==='0'
      ||BigInt(v.successful_count)+BigInt(v.failed_count)>BigInt(v.total_count)
      ||v.complete!==(v.next_after_line_no===null)||v.next_after_line_no!==null&&(!version(v.next_after_line_no)||v.next_after_line_no==='0')
      ||args.retry_line_nos.length>0&&(!v.complete||v.rows.length!==args.retry_line_nos.length)
      ||args.retry_line_nos.length===0&&v.complete&&BigInt(v.successful_count)+BigInt(v.failed_count)!==BigInt(v.total_count))responseInvalid();
    let previous=0n;for(const row of v.rows){if(!exact(row,['line_no','approved_line_id','component_key','label','amount','successful','reason_code','whole_timesheet_snoozed'])
      ||!version(row.line_no)||BigInt(row.line_no)<=previous||!uuid(row.approved_line_id)||!text(row.component_key,256)||!text(row.label,1024)
      ||typeof row.amount!=='string'||!/^\-?(?:0|[1-9]\d{0,15})\.\d{2}$/.test(row.amount)||typeof row.successful!=='boolean'
      ||typeof row.whole_timesheet_snoozed!=='boolean'||row.reason_code!==null&&(typeof row.reason_code!=='string'||!/^[A-Z][A-Z0-9_]{0,240}$/.test(row.reason_code))
      ||row.successful!==(row.reason_code===null)||args.retry_line_nos.length>0&&!args.retry_line_nos.includes(row.line_no)
      ||args.retry_line_nos.length===0&&BigInt(row.line_no)<=BigInt(args.after_line_no??'0'))responseInvalid();previous=BigInt(row.line_no);}
    if(!v.complete&&v.next_after_line_no!==v.rows.at(-1)?.line_no)responseInvalid();
  }
  return r;
}
