/** Closed Office preparation caller. Existing active/session Office guard is
 * injected; SQL rechecks service JWT/active admin. No economics, legacy session,
 * worker drain or automatic retries. Unknown mutations retry identical IDs/body.
 * Current approval pages are PRE-Draft; frozen review stays on accepted rows.
 */
export const BPAY_NEXT_PREPARATION_LIMITS=Object.freeze({requestBytes:32768,responseBytes:128*1024,timeoutMs:8000});
const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const INT=/^(?:0|[1-9]\d{0,18})$/;
const MONEY=/^-?(?:0|[1-9]\d{0,15})(?:\.\d{1,6})?$/;
const ENCODER=new TextEncoder();
export const BPAY_NEXT_PREPARATION_INPUTS=Object.freeze({
  CREATE:{run_id:'uuid',pay_date:'date'},
  WORK_PAGE:{run_id:'uuid',page_no:'positive',choices:'choices'},
  COMPONENT_PAGE:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',page_no:'positive',approved_line_ids:'ids'},
  COMPONENT_SEAL:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',expected_pages:'positive',expected_count:'positive'},
  CASE_PAGE:{run_id:'uuid',candidate_id:'uuid',selection_revision:'positive',page_no:'positive',request_id:'uuid',component_ids:'ids',selected:'booleans'},
  CASE_SEAL:{run_id:'uuid',candidate_id:'uuid',selection_revision:'positive',expected_pages:'positive',expected_items:'positive'},
  SEAL_ENQUEUE:{run_id:'uuid',command_id:'uuid',expected_pages:'integer',expected_count:'integer'},
  REVIEW:{run_id:'uuid'},CONFIRM:{run_id:'uuid',review_revision:'positive'}
});
export const BPAY_NEXT_PREPARATION_PAGE_INPUTS=Object.freeze({
  COMPONENTS:{work_id:'uuid',expected_revision_id:'uuid',after_line_no:'integer?',limit:'limit'},
  WORK_CHOICES:{run_id:'uuid',after_work_id:'uuid?',limit:'limit'},
  SELECTED_COMPONENTS:{run_id:'uuid',work_id:'uuid',after_selection_no:'integer?',limit:'limit'},
  WORKERS:{run_id:'uuid',after_candidate_id:'uuid?',limit:'limit'},
  REVISION_COMPARISON:{run_worker_id:'uuid',after_work_id:'uuid?',limit:'limit'},
  LINES:{run_worker_id:'uuid',after_line_no:'integer?',limit:'limit'},
  SHIFTS:{work_id:'uuid',expected_revision_id:'uuid',approved_line_id:'uuid',after_detail_no:'integer?',limit:'limit'},
  BREAKS:{work_id:'uuid',expected_revision_id:'uuid',approved_line_id:'uuid',shift_detail_id:'uuid',after_break_no:'integer?',limit:'limit'},
  RATES:{work_id:'uuid',expected_revision_id:'uuid',approved_line_id:'uuid',after_bucket:'bucket?',limit:'limit'},
  CURRENT_WORK:{candidate_id:'uuid',after_week:'date?',after_work_id:'uuid?',limit:'limit'},
  CURRENT_CASES:{candidate_id:'uuid',after_case_id:'uuid?',limit:'limit'},
  CURRENT_STORED_CREDITS:{candidate_id:'uuid',after_legacy_case_id:'uuid?',limit:'limit'},
  CASE_COMPONENTS:{candidate_id:'uuid',case_id:'uuid',after_component_ordinal:'integer?',limit:'limit'},
  STATUS:{run_id:'uuid',run_worker_id:'uuid',after_transfer_no:'transferNo?',limit:'limit'},
  RETURN_CASH:{run_id:'uuid',run_worker_id:'uuid',after_transfer_no:'transferNo?',limit:'limit'}
});
const OUTPUTS={
  CREATE:{run_id:'uuid',status:'runStatus',selection_state:'selectionState',selection_count:'integer',selected_candidate_count:'integer'},
  WORK_PAGE:{run_id:'uuid',page_no:'positive',item_count:'positive',selection_count:'integer',selected_candidate_count:'integer',replay:'boolean'},
  COMPONENT_PAGE:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',page_no:'positive',item_count:'positive',component_count:'positive',replay:'boolean'},
  COMPONENT_SEAL:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',sealed:'true',component_count:'positive',replay:'boolean'},
  CASE_PAGE:{run_id:'uuid',candidate_id:'uuid',selection_revision:'positive',page_no:'positive',item_count:'positive',replay:'boolean'},
  CASE_SEAL:{run_id:'uuid',candidate_id:'uuid',selection_revision:'positive',sealed:'true',selected_count:'integer',excluded_count:'integer',replay:'boolean'},
  SEAL_ENQUEUE:{run_id:'uuid',command_id:'uuid',agency_sequence:'positive',replay:'boolean'},
  REVIEW:{run_id:'uuid',phase:'reviewPhase',reason:'reviewReason?',review_revision:'integer',all_ready:'boolean?',replay:'boolean'},
  CONFIRM:{run_id:'uuid',phase:'draft',review_revision:'positive',replay:'boolean'}
};
const ROWS={
  COMPONENTS:{approved_line_id:'uuid',line_no:'positive',component_key:'key',component_kind:'componentKind',work_date:'date?',source_pay_channel:'channel',
    approved_source_ex_vat:'money',realised_source_ex_vat:'money?',held_source_ex_vat:'money?',residual_source_ex_vat:'money?',position_ready:'boolean'},
  WORK_CHOICES:{work_id:'uuid',expected_revision_id:'uuid?',selection_mode:'mode',selection_state:'selectionState',page_count:'integer',component_count:'integer'},
  SELECTED_COMPONENTS:{selection_no:'positive',approved_line_id:'uuid',expected_revision_id:'uuid',component_key:'key'},
  WORKERS:{run_worker_id:'uuid',candidate_id:'uuid',worker_status:'workerStatus',frozen_ex_vat:'money?',frozen_vat:'money?',frozen_inc_vat:'money?',review_issue_code:'issue?'},
  REVISION_COMPARISON:{run_work_id:'uuid',work_id:'uuid',original_timesheet_id:'uuid',captured_revision_id:'uuid',current_revision_id:'uuid?',newer_approval_available:'boolean'},
  LINES:{run_line_id:'uuid',line_no:'positive',original_timesheet_id:'uuid',captured_revision_id:'uuid',component_key:'key',component_kind:'componentKind',
    work_date:'date?',source_pay_channel:'channel',target_pay_channel:'channel',source_consumed_ex_vat:'money',frozen_ex_vat:'money',frozen_vat:'money',frozen_inc_vat:'money'},
  SHIFTS:{shift_detail_id:'uuid',detail_no:'positive',work_date:'date',shift_start_at:'timestamp?',shift_end_at:'timestamp?',shift_start_local:'clock?',shift_end_local:'clock?',
    shift_overnight:'boolean?',submitted_minutes:'integer?',approved_minutes:'integer?',approved_hours:'money?',detail_label:'label?',segment_pay_ex_vat:'money?',pay_excluded:'boolean?',
    hours_day:'money?',hours_night:'money?',hours_sat:'money?',hours_sun:'money?',hours_bh:'money?'},
  BREAKS:{break_detail_id:'uuid',break_no:'positive',break_start_at:'timestamp?',break_end_at:'timestamp?',break_start_local:'clock?',break_end_local:'clock?',break_minutes:'integer'},
  RATES:{rate_detail_id:'uuid',bucket:'bucket',approved_hours:'money',source_pay_rate:'money?'},
  CURRENT_WORK:{work_id:'uuid',candidate_id:'uuid',original_timesheet_id:'uuid',work_kind:'workKind',week_ending_date:'date',approval_state:'approvalState',
    current_revision_id:'uuid?',applied_revision_id:'uuid?',position_ready:'boolean',source_pay_channel:'channel?',timesheet_reference:'text?',client_display_name:'text?',approved_source_ex_vat:'money?'},
  STATUS:{transfer_id:'uuid',transfer_no:'transferNo',projection_id:'uuid?',execution_kind:'executionKind',status:'transferStatus',cash_amount:'money',original_transfer_id:'uuid?'},
  RETURN_CASH:{original_transfer_id:'uuid',original_transfer_no:'transferNo',original_transfer_status:'returned',original_cash_amount:'positiveCashMoney',
    beneficiary_kind:'beneficiaryKind',beneficiary_id:'uuid',return_command_id:'uuid',return_posting_complete:'boolean',return_cash_id:'uuid?',
    amount_owed:'positiveCashMoney?',amount_held:'cashMoney?',amount_reissued_paid:'cashMoney?',amount_available:'cashMoney?',
    pending_reissue_command_id:'uuid?',action_state:'returnCashAction'},
  CURRENT_CASES:{case_id:'uuid',candidate_id:'uuid',case_kind:'caseKind',tax_treatment:'taxTreatment',status:'caseStatus',
    principal_approved:'money',principal_funded:'money',principal_recovered:'money',principal_written_off:'money',active_hold_amount:'money'},
  CURRENT_STORED_CREDITS:{legacy_case_id:'uuid',candidate_id:'uuid',principal_source_ex_vat:'positiveMoney',
    original_source_pay_channel:'storedOriginChannel',original_tax_treatment:'storedOriginTax',original_routing_kind:'storedOriginRouting',
    original_created_at_utc:'timestamp',bank_version_at_utc:'timestamp',bank_details_hash:'bankHash',beneficiary_name:'beneficiary',account_last4:'last4'},
  CASE_COMPONENTS:{case_component_id:'uuid',case_id:'uuid',candidate_id:'uuid',component_ordinal:'positive',component_revision:'positive',
    case_kind:'caseKind',case_subtype:'caseSubtype',tax_treatment:'taxTreatment',instruction_kind:'instructionKind',direction:'direction',
    payroll_stage:'payrollStage',source_pay_channel:'channel',resolution_state:'resolution',approved_source_ex_vat:'money',funded_source_ex_vat:'money',
    recovered_source_ex_vat:'money',written_off_source_ex_vat:'money',active_payout_source_ex_vat:'money',active_recovery_source_ex_vat:'money'}
};
const STATUS_HEADER={run_id:'uuid',run_status:'runStatus',selection_state:'selectionState',review_revision:'integer',run_worker_id:'uuid',candidate_id:'uuid',worker_status:'workerStatus',
  target_pay_channel:'channel',umbrella_id:'uuid?',preparation_revision:'positive',case_selection_revision:'integer',net_request_revision:'integer',net_projection_revision:'integer',net_pending:'boolean',
  review_issue_code:'issue?',review_issue_work_id:'uuid?',frozen_gross_ex_vat:'money',frozen_gross_vat:'money',frozen_gross_inc_vat:'money',projection_id:'uuid?',
  projection_input_kind:'projectionKind?',entered_paye_net:'money?',accepted_recoveries:'money?',accepted_net_additions:'money?',cash_amount:'money?'};
const RETURN_CASH_HEADER={run_id:'uuid',run_worker_id:'uuid',candidate_id:'uuid',financial_view_revision:'integer'};
const CURSORS={COMPONENTS:['line_no','after_line_no'],WORK_CHOICES:['work_id','after_work_id'],SELECTED_COMPONENTS:['selection_no','after_selection_no'],
  WORKERS:['candidate_id','after_candidate_id'],REVISION_COMPARISON:['work_id','after_work_id'],LINES:['line_no','after_line_no'],
  SHIFTS:['detail_no','after_detail_no'],BREAKS:['break_no','after_break_no'],RATES:['bucket','after_bucket'],STATUS:['transfer_no','after_transfer_no'],
  CURRENT_CASES:['case_id','after_case_id'],CURRENT_STORED_CREDITS:['legacy_case_id','after_legacy_case_id'],CASE_COMPONENTS:['component_ordinal','after_component_ordinal']};
class PreparationError extends Error {
  constructor(code,status=503,retryable=false,outcomeUnknown=false){super(code);Object.assign(this,{code,status,retryable,outcomeUnknown});}
}
const fault=(suffix,status=503,retryable=false,unknown=false)=>new PreparationError(`BPAY_NEXT_PREPARATION_${suffix}`,status,retryable,unknown);
const record=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
function exact(v,schema){return record(v)&&Object.keys(v).length===Object.keys(schema).length&&Object.entries(schema).every(([k,t])=>Object.hasOwn(v,k)&&field(v[k],t));}
function field(v,t){
  if(t.endsWith('?'))return v===null||field(v,t.slice(0,-1));
  if(t==='integer'||t==='positive')return typeof v==='string'&&INT.test(v)&&BigInt(v)<=9223372036854775807n&&(t!=='positive'||v!=='0');
  if(t==='transferNo')return field(v,'positive')&&BigInt(v)<=2147483647n;
  if(t==='uuid')return typeof v==='string'&&UUID.test(v);
  if(t==='date'){if(typeof v!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(v)||v.startsWith('0000'))return false;
    const d=new Date(`${v}T00:00:00Z`);return Number.isFinite(d.getTime())&&d.toISOString().slice(0,10)===v;}
  if(t==='boolean'||t==='true')return typeof v==='boolean'&&(t!=='true'||v);
  if(t==='money')return typeof v==='string'&&MONEY.test(v);
  if(t==='cashMoney')return typeof v==='string'&&/^(?:0|[1-9]\d{0,15})\.\d{2}$/.test(v);
  if(t==='positiveCashMoney')return field(v,'cashMoney')&&/[1-9]/.test(v);
  if(t==='positiveMoney')return field(v,'money')&&!v.startsWith('-')&&/[1-9]/.test(v);
  if(t==='storedOriginChannel')return v==='UMBRELLA';
  if(t==='storedOriginTax')return v==='NON_TAXABLE';
  if(t==='storedOriginRouting')return v==='ONE_OFF_SPECIFIED_BANK_ACCOUNT';
  if(t==='last4')return typeof v==='string'&&/^[0-9]{4}$/.test(v);
  if(t==='bankHash')return typeof v==='string'&&!v.includes('\0')&&ENCODER.encode(v).byteLength>=1&&ENCODER.encode(v).byteLength<=256;
  if(t==='beneficiary')return field(v,'text')&&v.trim().length>0;
  if(t==='limit')return Number.isInteger(v)&&v>=1&&v<=100;
  if(t==='mode')return ['ALL','SUBSET'].includes(v);
  if(t==='selectionState')return ['OPEN','SEALED'].includes(v);
  if(t==='bucket')return ['DAY','NIGHT','SAT','SUN','BH'].includes(v);
  if(t==='channel')return ['PAYE','UMBRELLA'].includes(v);
  if(t==='componentKind')return ['WORK','PROTECTED_WORK','EXPENSE','MILEAGE','ADDITIONAL','ADJUSTMENT'].includes(v);
  if(t==='workKind')return ['ORDINARY','SOURCE','EXPENSE','ADJUSTMENT'].includes(v);
  if(t==='caseKind')return ['LOAN','ADVANCE','OVERPAYMENT','CREDIT','MANUAL_DEBT'].includes(v);
  if(t==='caseSubtype')return ['LOAN','PAYMENT_ADVANCE','OVERPAYMENT','UNDERPAYMENT','MANUAL_CREDIT','MANUAL_DEBT'].includes(v);
  if(t==='taxTreatment')return ['TAXABLE','NON_TAXABLE','NOT_APPLICABLE'].includes(v);
  if(t==='caseStatus')return ['OPEN','PAUSED'].includes(v);
  if(t==='instructionKind')return ['PAYOUT','RECOVERY','CREDIT'].includes(v);
  if(t==='direction')return ['PAYMENT','DEDUCTION'].includes(v);
  if(t==='payrollStage')return ['GROSS_ADD','GROSS_DEDUCT','NET_ADD','NET_DEDUCT'].includes(v);
  if(t==='resolution')return ['RESOLVED','REVIEW'].includes(v);
  if(t==='approvalState')return ['PENDING','APPROVED','WITHDRAWN'].includes(v);
  if(t==='projectionKind')return ['PAYE_MANUAL','PAYE_IMPORT','UMBRELLA','CASE_PAYOUT'].includes(v);
  if(t==='executionKind')return ['BANK','INTERNAL_ZERO'].includes(v);
  if(t==='transferStatus')return ['BUILDING','MEMBERS_READY','DRAFT','SCHEDULED','ISSUED_CSV','SUBMITTED','UNKNOWN','SETTLED','RETURNED','REFUSED','CANCELLED','INTERNAL_PROCESSING','INTERNAL_SETTLED'].includes(v);
  if(t==='returned')return v==='RETURNED';
  if(t==='beneficiaryKind')return ['CANDIDATE','UMBRELLA','OTHER_APPROVED'].includes(v);
  if(t==='returnCashAction')return ['RETURN_POSTING_PENDING','UNSUPPORTED_BENEFICIARY','REISSUE_REQUESTED','CASH_RESERVED','CASH_REPAID','CASH_AVAILABLE'].includes(v);
  if(t==='runStatus')return ['PREPARING','REVIEW','DRAFT','CANCELLING','CANCELLED','EXECUTING','COMPLETE'].includes(v);
  if(t==='workerStatus')return ['PREPARING','REVIEW','READY','DRAFT','CANCELLING','CANCELLED','ISSUED','COMPLETE'].includes(v);
  if(t==='reviewPhase')return ['PREPARING','REVIEW'].includes(v);
  if(t==='reviewReason')return ['ENROLLMENT_INCOMPLETE','WORKERS_INCOMPLETE'].includes(v);
  if(t==='draft')return v==='DRAFT';
  if(t==='key')return typeof v==='string'&&!v.includes('\0')&&[...v].length>=1&&[...v].length<=256;
  if(t==='issue')return typeof v==='string'&&/^[A-Z][A-Z0-9_]{0,255}$/.test(v);
  if(t==='label')return typeof v==='string'&&!v.includes('\0')&&ENCODER.encode(v).byteLength<=8192;
  if(t==='text')return typeof v==='string'&&!v.includes('\0')&&ENCODER.encode(v).byteLength<=BPAY_NEXT_PREPARATION_LIMITS.responseBytes;
  if(t==='clock')return typeof v==='string'&&/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(v);
  if(t==='timestamp')return typeof v==='string'&&/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.test(v)&&field(v.slice(0,10),'date')&&Number.isFinite(Date.parse(v));
  if(t==='ids')return Array.isArray(v)&&v.length>=1&&v.length<=100&&v.every(x=>field(x,'uuid'))&&new Set(v.map(x=>x.toLowerCase())).size===v.length;
  if(t==='booleans')return Array.isArray(v)&&v.length>=1&&v.length<=100&&v.every(x=>typeof x==='boolean');
  if(t==='choices')return Array.isArray(v)&&v.length>=1&&v.length<=100&&v.every(x=>exact(x,{work_id:'uuid',expected_revision_id:'uuid',mode:'mode'}))
    &&new Set(v.map(x=>x.work_id.toLowerCase())).size===v.length;
  return false;
}
function normalized(v){
  if(Array.isArray(v))return Object.freeze(v.map(normalized));
  if(record(v))return Object.freeze(Object.fromEntries(Object.entries(v).map(([k,x])=>[k,normalized(x)])));
  return typeof v==='string'&&UUID.test(v)?v.toLowerCase():v;
}
function commandCall(body){
  if(!record(body)||!Object.hasOwn(BPAY_NEXT_PREPARATION_INPUTS,body.action))throw fault('REQUEST_INVALID',400);
  const {action,...args}=body;
  if(!exact(args,BPAY_NEXT_PREPARATION_INPUTS[action])||(action==='CASE_PAGE'&&args.component_ids.length!==args.selected.length))throw fault('REQUEST_INVALID',400);
  return {action,args:normalized(args)};
}
function pageCall(request){
  const url=new URL(request.url);
  if(ENCODER.encode(url.search).byteLength>BPAY_NEXT_PREPARATION_LIMITS.requestBytes)throw fault('REQUEST_TOO_LARGE',413);
  const kind=url.searchParams.get('kind'),schema=BPAY_NEXT_PREPARATION_PAGE_INPUTS[kind];
  if(!schema||[...url.searchParams.keys()].some(k=>(k!=='kind'&&!Object.hasOwn(schema,k))||url.searchParams.getAll(k).length!==1))throw fault('REQUEST_INVALID',400);
  const args={};
  for(const [key,type]of Object.entries(schema)){
    let v=url.searchParams.get(key);
    if(key==='limit'){if(v!==null&&!/^[1-9]\d{0,2}$/.test(v))throw fault('REQUEST_INVALID',400);v=v===null?50:Number(v);}
    if(!field(v,type))throw fault('REQUEST_INVALID',400);args[key]=v;
  }
  if(kind==='CURRENT_WORK'&&(args.after_week===null)!==(args.after_work_id===null))throw fault('REQUEST_INVALID',400);
  return {kind,args:normalized(args)};
}
async function limitedJson(input,maximum,request,signal,mutation){
  const bad=()=>fault(request?'REQUEST_INVALID':'RESPONSE_INVALID',request?400:503,!request,!request&&mutation);
  const large=()=>fault(request?'REQUEST_TOO_LARGE':'RESPONSE_TOO_LARGE',request?413:503,!request,!request&&mutation);
  const length=input.headers.get('content-length');
  if(length!==null&&(!/^\d+$/.test(length)||Number(length)>maximum)){await input.body?.cancel().catch(()=>{});throw large();}
  if(!input.body)throw bad();
  const reader=input.body.getReader(),decoder=new TextDecoder('utf-8',{fatal:true}),parts=[];let bytes=0;
  const cancel=()=>{void reader.cancel().catch(()=>{});};signal?.addEventListener('abort',cancel,{once:true});
  try{
    if(signal?.aborted)throw fault('DEPENDENCY_TIMEOUT',503,true,mutation);
    while(true){const {done,value}=await reader.read();if(done)break;bytes+=value.byteLength;if(bytes>maximum)throw large();parts.push(decoder.decode(value,{stream:true}));}
    parts.push(decoder.decode());return JSON.parse(parts.join(''));
  }catch(e){await reader.cancel().catch(()=>{});if(e instanceof PreparationError)throw e;throw bad();}
  finally{signal?.removeEventListener('abort',cancel);reader.releaseLock();}
}
function result(payload,call,mutation){
  const bad=()=>{throw fault('RESPONSE_INVALID',503,true,mutation);};
  if(mutation){
    if(!exact(payload,OUTPUTS[call.action]))bad();
    for(const key of ['run_id','work_id','expected_revision_id','candidate_id','command_id','page_no','selection_revision'])
      if(Object.hasOwn(call.args,key)&&Object.hasOwn(payload,key)&&payload[key]!==call.args[key])bad();
    if(call.action==='REVIEW'&&((payload.phase==='PREPARING')!==(payload.reason!==null)))bad();
    if(call.action==='COMPONENT_SEAL'&&payload.component_count!==call.args.expected_count)bad();
    if(call.action==='CONFIRM'&&payload.review_revision!==call.args.review_revision)bad();
    if(['WORK_PAGE','COMPONENT_PAGE','CASE_PAGE'].includes(call.action)){
      const n=call.args.choices?.length??call.args.approved_line_ids?.length??call.args.component_ids?.length;
      if(payload.item_count!==String(n))bad();
    }
    return payload;
  }
  const storedCreditPage=call.kind==='CURRENT_STORED_CREDITS',returnCashPage=call.kind==='RETURN_CASH';
  if(!record(payload)||Object.keys(payload).length!==(returnCashPage?4:(call.kind==='STATUS'||storedCreditPage)?3:2)||!Array.isArray(payload.rows)||typeof payload.complete!=='boolean'
    ||payload.rows.length>call.args.limit||(!storedCreditPage&&!returnCashPage&&!payload.complete&&payload.rows.length===0))bad();
  if(returnCashPage){
    const h=payload.header,scan=payload.scan_cursor;
    if(!exact(h,RETURN_CASH_HEADER)||h.run_id!==call.args.run_id||h.run_worker_id!==call.args.run_worker_id
      ||!Object.hasOwn(payload,'scan_cursor')||!field(scan,'transferNo?')
      ||(scan===null&&(!payload.complete||payload.rows.length!==0))
      ||(scan!==null&&call.args.after_transfer_no!==null&&BigInt(scan)<=BigInt(call.args.after_transfer_no)))bad();
    let previous=call.args.after_transfer_no;const transfers=new Set(),cashIds=new Set();
    for(const row of payload.rows){
      if(!exact(row,ROWS.RETURN_CASH)||scan===null||BigInt(row.original_transfer_no)>BigInt(scan)
        ||(previous!==null&&BigInt(row.original_transfer_no)<=BigInt(previous))||transfers.has(row.original_transfer_id)
        ||row.return_posting_complete!==(row.return_cash_id!==null))bad();
      previous=row.original_transfer_no;transfers.add(row.original_transfer_id);
      const monies=['amount_owed','amount_held','amount_reissued_paid','amount_available'];
      if(row.return_cash_id===null){
        if(monies.some(k=>row[k]!==null)||row.pending_reissue_command_id!==null||row.action_state!=='RETURN_POSTING_PENDING')bad();
      }else{
        if(monies.some(k=>row[k]===null)||cashIds.has(row.return_cash_id))bad();cashIds.add(row.return_cash_id);
        const supported=row.beneficiary_kind==='CANDIDATE'&&row.beneficiary_id===h.candidate_id;
        // Validate interpretation, not economics: all balances/available are
        // authoritative SQL text. No subtraction, payroll or bank approval.
        const expected=!supported?'UNSUPPORTED_BENEFICIARY':row.pending_reissue_command_id!==null?'REISSUE_REQUESTED':
          /[1-9]/.test(row.amount_available)?'CASH_AVAILABLE':/[1-9]/.test(row.amount_held)?'CASH_RESERVED':'CASH_REPAID';
        if(row.action_state!==expected)bad();
      }
    }
    return {header:h,rows:payload.rows,complete:payload.complete,next_cursor:payload.complete?null:{after_transfer_no:scan}};
  }
  if(storedCreditPage){
    // The owner examines a bounded raw keyset window BEFORE filtering. Its
    // cursor may advance past bound/ineligible facts even with no output rows.
    // Deriving it from the last returned row would restart that same prefix.
    if(!Object.hasOwn(payload,'scan_cursor')||!field(payload.scan_cursor,'uuid?')
      ||(payload.scan_cursor===null&&(!payload.complete||payload.rows.length!==0))
      ||(payload.scan_cursor!==null&&call.args.after_legacy_case_id!==null&&payload.scan_cursor<=call.args.after_legacy_case_id))bad();
  }
  if(call.kind==='STATUS'){
    const h=payload.header;
    if(!exact(h,STATUS_HEADER)||h.run_id!==call.args.run_id||h.run_worker_id!==call.args.run_worker_id
      ||h.net_pending!==(h.net_request_revision!==h.net_projection_revision))bad();
    const facts=['projection_input_kind','entered_paye_net','accepted_recoveries','accepted_net_additions','cash_amount'];
    if(h.projection_id===null&&facts.some(k=>h[k]!==null))bad();
    if(h.projection_id!==null&&(h.projection_input_kind===null||['accepted_recoveries','accepted_net_additions','cash_amount'].some(k=>h[k]===null)
      ||(['UMBRELLA','CASE_PAYOUT'].includes(h.projection_input_kind)!==(h.entered_paye_net===null))))bad();
  }
  if(call.kind==='CURRENT_WORK'){
    let previousWeek=call.args.after_week,previousWork=call.args.after_work_id;
    for(const row of payload.rows){
      if(!exact(row,ROWS.CURRENT_WORK)||row.candidate_id!==call.args.candidate_id
        ||(previousWeek!==null&&!(row.week_ending_date>previousWeek||(row.week_ending_date===previousWeek&&row.work_id>previousWork)))
        ||(row.position_ready&&(row.approval_state!=='APPROVED'||row.current_revision_id===null||row.current_revision_id!==row.applied_revision_id)))bad();
      previousWeek=row.week_ending_date;previousWork=row.work_id;
    }
    return {rows:payload.rows,complete:payload.complete,next_cursor:payload.complete?null:{after_week:previousWeek,after_work_id:previousWork}};
  }
  const [key,inputKey]=CURSORS[call.kind];let previous=call.args[inputKey];const caseIds=new Set();
  for(const row of payload.rows){
    if(!exact(row,ROWS[call.kind]))bad();
    const value=row[key];
    if(previous!==null){const ordered=key.endsWith('_no')||key==='component_ordinal'?BigInt(value)>BigInt(previous):value>previous;if(!ordered)bad();}
    previous=value;
    if(['CURRENT_CASES','CURRENT_STORED_CREDITS','CASE_COMPONENTS'].includes(call.kind)&&row.candidate_id!==call.args.candidate_id)bad();
    if(storedCreditPage&&row.legacy_case_id>payload.scan_cursor)bad();
    if(call.kind==='CASE_COMPONENTS'&&row.case_id!==call.args.case_id)bad();
    if(call.kind==='CASE_COMPONENTS'){
      if(caseIds.has(row.case_component_id))bad();caseIds.add(row.case_component_id);
    }
    if(call.kind==='SELECTED_COMPONENTS'&&call.args.expected_revision_id&&row.expected_revision_id!==call.args.expected_revision_id)bad();
    if(call.kind==='WORK_CHOICES'&&row.selection_mode==='ALL'&&(row.selection_state!=='SEALED'||row.page_count!=='0'||row.component_count!=='0'))bad();
    if(call.kind==='SHIFTS'&&row.pay_excluded===true&&row.segment_pay_ex_vat!=='0.00')bad();
  }
  return {...(call.kind==='STATUS'?{header:payload.header}:{}),rows:payload.rows,complete:payload.complete,next_cursor:payload.complete?null:{[inputKey]:storedCreditPage?payload.scan_cursor:previous}};
}
function safeError(e,attempted,mutation){
  if(e instanceof PreparationError)return e;
  const db=e?.json??e;
  if(db?.code==='PGRST202')return fault('DEPENDENCY_UNAVAILABLE',503,true);
  if(db?.code==='42501'||[401,403].includes(e?.status))return fault('FORBIDDEN',403);
  if(db?.code==='P0002')return fault('SCOPE_NOT_FOUND',404);
  if(['22023','22003','22007','22008','22P02'].includes(db?.code))return fault('REQUEST_INVALID',400);
  if(['23514','23505','23503'].includes(db?.code))return fault('CONFLICT',409);
  if(db?.code==='55000')return db.message==='BPAY_NEXT_MODULE_NOT_ACTIVE'?fault('MODULE_NOT_ACTIVE',503):fault('NOT_ELIGIBLE',409);
  if(db?.code==='54000')return fault('RESPONSE_TOO_LARGE',503,true);
  if(e?.name==='AbortError'||e?.status===408)return fault('DEPENDENCY_TIMEOUT',503,true,attempted&&mutation);
  return fault('DEPENDENCY_UNAVAILABLE',503,true,attempted&&mutation);
}
const json=(status,body,wake=false)=>new Response(JSON.stringify(body),{status,headers:{'content-type':'application/json; charset=utf-8',
  'cache-control':'no-store','x-content-type-options':'nosniff',...(wake?{'x-bpay-next-preparation-wake':'ENROLL'}:{})}});
/** @typedef {{requireOfficeUser:(request:Request,roles:string[])=>Promise<{id:string,role:string}|null>,preparationRpc?:(action:string,args:object,options:object)=>Promise<Response>,preparationPageRpc?:(kind:string,args:object,options:object)=>Promise<Response>}} PreparationDependencies */
/** @param {Request} request @param {Partial<PreparationDependencies>} dependencies */
async function handle(request,dependencies,mutation){
  if(request.method!==(mutation?'POST':'GET'))return json(405,{ok:false,error_code:'BPAY_NEXT_PREPARATION_METHOD_NOT_ALLOWED',retryable:false,outcome_unknown:false});
  let timer,controller,attempted=false;
  try{
    if(typeof dependencies.requireOfficeUser!=='function')throw fault('DEPENDENCY_UNAVAILABLE',503,true);
    const user=await dependencies.requireOfficeUser(request,['admin']);
    if(!user||!field(user.id,'uuid'))throw fault('UNAUTHORIZED',401);
    if(user.role!=='admin')throw fault('FORBIDDEN',403);
    const call=mutation?commandCall(await limitedJson(request,BPAY_NEXT_PREPARATION_LIMITS.requestBytes,true,null,false)):pageCall(request);
    const rpc=mutation?dependencies.preparationRpc:dependencies.preparationPageRpc;
    if(typeof rpc!=='function')throw fault('DEPENDENCY_UNAVAILABLE',503,true);
    controller=new AbortController();
    const timeout=new Promise((resolve,reject)=>{timer=setTimeout(()=>{controller.abort();reject(fault('DEPENDENCY_TIMEOUT',503,true,attempted&&mutation));},BPAY_NEXT_PREPARATION_LIMITS.timeoutMs);});
    const send=async()=>{
      attempted=true;
      const response=await rpc(call.action??call.kind,call.args,{actorUserId:user.id.toLowerCase(),signal:controller.signal,
        timeoutMs:BPAY_NEXT_PREPARATION_LIMITS.timeoutMs,maxResponseBytes:BPAY_NEXT_PREPARATION_LIMITS.responseBytes,routeClass:mutation?'OPERATION_NUDGE':'READ'});
      if(!(response instanceof Response))throw fault('RESPONSE_INVALID',503,true,mutation);
      const payload=await limitedJson(response,BPAY_NEXT_PREPARATION_LIMITS.responseBytes,false,controller.signal,mutation);
      if(!response.ok)throw safeError({status:response.status,json:payload},true,mutation);
      const body=mutation?{ok:true,action:call.action,result:result(payload,call,true)}:{ok:true,kind:call.kind,...result(payload,call,false)};
      if(ENCODER.encode(JSON.stringify(body)).byteLength>BPAY_NEXT_PREPARATION_LIMITS.responseBytes)throw fault('RESPONSE_TOO_LARGE',503,true,mutation);
      return json(200,body,mutation&&call.action==='SEAL_ENQUEUE');
    };
    return await Promise.race([send(),timeout]);
  }catch(e){const safe=safeError(e,attempted,mutation);return json(safe.status,{ok:false,error_code:safe.code,retryable:safe.retryable,outcome_unknown:safe.outcomeUnknown});}
  finally{if(timer!==undefined)clearTimeout(timer);}
}
/** @param {Request} request @param {Partial<PreparationDependencies>} [dependencies] */
export const handleBpayNextPreparationCommand=(request,dependencies={})=>handle(request,dependencies,true);
/** @param {Request} request @param {Partial<PreparationDependencies>} [dependencies] */
export const handleBpayNextPreparationPage=(request,dependencies={})=>handle(request,dependencies,false);
