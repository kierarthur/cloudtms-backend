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
    :['request_id','candidate_id','work_id','expected_revision_id','scope_kind','segment_key','component_key',
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
    if(!SCOPES.has(args.scope_kind)||!KINDS.has(canonical)||typeof args.indefinite!=='boolean'
      ||args.indefinite!==(args.until_date==null)||(!args.indefinite&&!date(args.until_date))
      ||(args.note!==undefined&&!text(args.note,500))
      ||(args.phase!==undefined&&!['PRE_OPEN','PRE_SAVE'].includes(args.phase)))invalid();
    if(args.scope_kind==='CASE'){
      if(!uuid(args.case_id)||!uuid(args.case_component_id)||args.work_id!==undefined
        ||args.expected_revision_id!==undefined||args.segment_key!==undefined||args.component_key!==undefined
        ||!['OVERPAYMENT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','MANUAL_DEBT_RECOVERY'].includes(canonical)
        ||(mutation&&!version(args.expected_case_revision)))invalid();
    }else{
      if(!uuid(args.work_id)||!uuid(args.expected_revision_id)||args.case_id!==undefined
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
