import {caseFinanceQuery,caseFinanceResult} from './bpay-next-case-audit-contract.js';
import {caseChannelInput,caseChannelToken,caseChannelCursor,caseChannelPreview,caseChannelReceipt,CHANNEL_ISSUES,CHANNEL_RECEIPT_SCHEMA,channelValue,caseRestructureInput,caseRestructurePreview,caseRestructureReceipt,RESTRUCTURE_ISSUES,caseInput,casePreview,caseEditState,caseEditReceipt,casePauseState,casePauseReceipt} from './bpay-next-case-contract.js';
import {readBpayNextReauthJson} from './bpay-next-command.js';
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


// DR27: identical closed directory/count read contract in Worker and Office.
const CHANNEL_DRAFT_CONTEXT={agency_id:'uuid',actor_user_id:'uuid',owner_epoch:'positive',status:'channelDraftFilter',generation:'integer'};
const CHANNEL_DRAFT_INPUT={agency_id:'uuid',owner_epoch:'positive',status:'channelDraftFilter',context:'channelDraftContext?'};
const CHANNEL_DRAFT_ROW={run_id:'uuid',pay_channel:'channel',type_label:'channelDraftType',created_at_utc:'timestamp',confirmed_at_utc:'timestamp',original_worker_count:'positive',original_timesheet_count:'integer',original_gross_ex_vat:'channelDraftMoney',original_gross_vat:'channelDraftMoney',original_gross_inc_vat:'channelDraftMoney',cash_known:'channelDraftMoney',cash_complete:'boolean',status_group:'channelDraftStatus',status_label:'channelDraftLabel',status_note:'label?',has_payment_issue:'boolean',execution_route:'channelDraftRoute?',pay_date:'date?',scheduled_at_utc:'timestamp?',reference:'label?',change_revision:'integer'};
const CHANNEL_DRAFT_LABELS={NOT_PAID:'Not paid',WAITING_APPROVAL:'Waiting for approval',PROCESSING:'Processing',PARTLY_PAID:'Partly paid',BANK_UNAVAILABLE:'Bank unavailable',PAID:'Paid',CANCELLED:'Cancelled'};
function channelDraftField(v,t,base,shape){
 if(t==='channelDraftStatus')return typeof v==='string'&&Object.hasOwn(CHANNEL_DRAFT_LABELS,v);
 if(t==='channelDraftFilter')return v==='ALL'||channelDraftField(v,'channelDraftStatus');
 if(t==='channelDraftType')return v==='PAYE'||v==='Umbrella';
 if(t==='channelDraftLabel')return Object.values(CHANNEL_DRAFT_LABELS).includes(v);
 if(t==='channelDraftRoute')return ['BANK_CSV','EXTERNAL','SCHEDULED'].includes(v);
 if(t==='channelDraftMoney')return typeof v==='string'&&/^-?(?:0|[1-9]\d{0,35})\.\d{2}$/.test(v);
 if(t==='channelDraftContext')return shape(v,CHANNEL_DRAFT_CONTEXT);
 if(t==='channelDraftCursor')return shape(v,{context:'channelDraftContext',created_at_utc:'timestamp',run_id:'uuid',pay_channel:'channel'})&&new TextEncoder().encode(JSON.stringify(v)).byteLength<=4096;
 if(t==='channelDraftRows')return Array.isArray(v)&&v.length<=100&&v.every(r=>shape(r,CHANNEL_DRAFT_ROW));
 return false;
}
function channelDraftTime(v){const fraction=v.match(/\.(\d{1,6})(?:Z|[+-]\d\d:\d\d)$/)?.[1]??'';return BigInt(Date.parse(v.replace(/\.\d{1,6}(?=Z|[+-]\d\d:\d\d$)/,'')))*1000n+BigInt(fraction.padEnd(6,'0'));}
function channelDraftOrder(a,b){const x=channelDraftTime(a.created_at_utc),y=channelDraftTime(b.created_at_utc);return x!==y?(x>y?-1:1):a.run_id!==b.run_id?(a.run_id<b.run_id?-1:1):a.pay_channel===b.pay_channel?0:a.pay_channel<b.pay_channel?-1:1;}
function channelDraftResult(p,kind,args,actor,shape,bad){
 const count=kind==='CHANNEL_DRAFT_COUNT';
 if(!shape(p,count?{context:'channelDraftContext',count:'integer'}:{context:'channelDraftContext',rows:'channelDraftRows',complete:'boolean',next_cursor:'channelDraftCursor?'}))bad();
 const c=p.context;if(c.agency_id!==args.agency_id||c.actor_user_id!==actor||c.owner_epoch!==args.owner_epoch||c.status!==args.status||args.context!==null&&!worksheetEqual(args.context,c))bad();
 if(count)return p;
 if(p.rows.length>args.limit||p.complete!==(p.next_cursor===null)||!p.complete&&p.rows.length===0||args.after!==null&&!worksheetEqual(args.after.context,c))bad();
 const ids=new Set();let previous=args.after;
 for(const r of p.rows){
  const id=r.run_id+':'+r.pay_channel,pence=v=>BigInt(v.replace('.',''));
  if(ids.has(id)||r.type_label!==(r.pay_channel==='PAYE'?'PAYE':'Umbrella')||r.status_label!==CHANNEL_DRAFT_LABELS[r.status_group]
   ||args.status!=='ALL'&&r.status_group!==args.status||pence(r.original_gross_inc_vat)!==pence(r.original_gross_ex_vat)+pence(r.original_gross_vat)
   ||pence(r.cash_known)<0n||channelDraftTime(r.confirmed_at_utc)<channelDraftTime(r.created_at_utc)
   ||r.execution_route===null&&(r.pay_date!==null||r.scheduled_at_utc!==null)
   ||r.execution_route!==null&&(r.pay_date===null||(r.execution_route==='SCHEDULED')!==(r.scheduled_at_utc!==null))
   ||previous!==null&&channelDraftOrder(previous,r)>=0)bad();
  ids.add(id);previous=r;
 }
 if(p.next_cursor!==null){const last=p.rows.at(-1);if(!worksheetEqual(p.next_cursor.context,c)||p.next_cursor.created_at_utc!==last.created_at_utc||p.next_cursor.run_id!==last.run_id||p.next_cursor.pay_channel!==last.pay_channel)bad();}
 return p;
}

// DR21: identical closed read contract in Worker and native Office.
const WORKSHEET_INPUT={run_id:'uuid',pay_channel:'worksheetChannel',candidate_id:'uuid?',query:'worksheetQuery',sort:'worksheetSort',after:'worksheetCursor?',limit:'limit'};
const WORKSHEET_CONTEXT={run_id:'uuid',pay_channel:'worksheetChannel',actor_user_id:'uuid',owner_epoch:'positive',review_revision:'integer',run_status:'worksheetRun',totals_revision:'integer',net_request_sequence:'integer',candidate_id:'uuid?',query:'worksheetQuery',sort:'worksheetSort'};
const WORKSHEET_ROW={net_replacement_pending_id:'uuid?',replacement_id:'uuid?',replacement_stage:'netReplacementStage?',net_request_high_water:'integer',requested_net:'worksheetMoney?',net_refused_code:'netRefusal?',replacement_request_no:'positive?',replacement_cursor:'positive?',replacement_expected_count:'integer?',replacement_staged_count:'integer?',replacement_validated_count:'integer?',replacement_committed_count:'integer?',run_worker_id:'uuid',candidate_id:'uuid',candidate_display_name:'label?',candidate_reference:'label?',worker_status:'workerStatus',captured_line_count:'integer',case_instruction_count:'integer',earnings_gross:'worksheetMoney?',payroll_gross:'worksheetMoney',gross_stage_deductions:'worksheetMoney?',advance_repayment:'worksheetMoney?',overpayment_recovery:'worksheetMoney?',gross_manual_debt:'worksheetMoney?',net_manual_debt:'worksheetMoney?',other_repayment:'worksheetMoney?',net_recovery:'worksheetMoney?',net_additions:'worksheetMoney?',deduction_basis:'worksheetBasis?',payout_only:'boolean',applicable_net:'boolean',projection_id:'uuid?',accepted_net:'worksheetMoney?',pending_net:'worksheetMoney?',projection_status:'worksheetProjection',expected_projection_revision:'integer',cash_amount:'worksheetMoney?',input_source:'worksheetSource?',net_state:'worksheetNetState',sort_key:'worksheetKey'};

const WORKSHEET_LINE={run_line_id:'uuid',line_no:'positive',original_timesheet_id:'uuid',captured_revision_id:'uuid',component_key:'key',component_kind:'componentKind',work_date:'date?',source_pay_channel:'channel',target_pay_channel:'channel',source_consumed_ex_vat:'money',frozen_ex_vat:'money',frozen_vat:'money',frozen_inc_vat:'money',client_display_name:'label',week_ending_date:'date',unit_label:'worksheetText?',approved_quantity:'worksheetDecimal?',approved_unit_rate:'worksheetDecimal?'};
function worksheetLinesResult(p,args,shape,bad){
 if(!shape(p,{rows:'worksheetLines',complete:'boolean',next_cursor:'worksheetLineCursor?'}))bad();
 let prior=BigInt(args.after_line_no??'0');const ids=new Set();
 for(const r of p.rows){if(BigInt(r.line_no)<=prior||ids.has(r.run_line_id)||r.target_pay_channel!=='PAYE')bad();prior=BigInt(r.line_no);ids.add(r.run_line_id);}
 if(p.rows.length>args.limit||p.complete!==(p.next_cursor===null)||(!p.complete&&(p.rows.length===0||p.next_cursor.after_line_no!==prior.toString()))||new TextEncoder().encode(JSON.stringify(p)).byteLength>128*1024)bad();return p;
}

function worksheetField(v,t,base,shape){
 if(t==='worksheetText')return typeof v==='string'&&!v.includes('\0')&&new TextEncoder().encode(v).byteLength<=120000;
 if(t==='worksheetDecimal')return typeof v==='string'&&/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(v)&&new TextEncoder().encode(v).byteLength<=120000;
 if(t==='worksheetLines')return Array.isArray(v)&&v.length<=100&&v.every(r=>shape(r,WORKSHEET_LINE));
 if(t==='worksheetLineCursor')return shape(v,{after_line_no:'positive'});
 if(t==='worksheetChannel')return v==='PAYE';
 if(t==='worksheetQuery')return typeof v==='string'&&!v.includes('\0')&&[...v].length<=120;
 if(t==='worksheetSort')return typeof v==='string'&&/^(CANDIDATE|GROSS|ADVANCE|OVERPAYMENT|NET|CASH|SOURCE|STATUS)_(ASC|DESC)$/.test(v);
 if(t==='worksheetMoney')return typeof v==='string'&&/^(0|[1-9]\d{0,15})\.\d{2}$/.test(v);
 if(t==='worksheetRun')return ['DRAFT','EXECUTING','COMPLETE','CANCELLED'].includes(v);
 if(t==='worksheetProjection')return ['NONE','REQUESTED','APPLYING','ACCEPTED','REFUSED'].includes(v);
 if(t==='worksheetNetState')return ['MISSING','ENTERED','ENTERED_ZERO','PAYOUT_ONLY'].includes(v);
 if(t==='worksheetSource')return ['PAYE_MANUAL','PAYE_IMPORT'].includes(v);
 if(t==='worksheetBasis')return ['RESERVED','ACCEPTED'].includes(v);
 if(t==='worksheetContext')return shape(v,WORKSHEET_CONTEXT);
 if(t==='worksheetKey')return Array.isArray(v)&&v.length===4&&typeof v[0]==='boolean'
  &&(v[0]?v[1]===null:base(v[1],'label'))&&base(v[2],'uuid')&&base(v[3],'uuid');
 if(t==='worksheetCursor')return shape(v,{context:'worksheetContext',candidate_id:'uuid',run_worker_id:'uuid'})&&new TextEncoder().encode(JSON.stringify(v)).byteLength<=4096;
 if(t==='worksheetRows')return Array.isArray(v)&&v.length<=100&&v.every(r=>shape(r,WORKSHEET_ROW));
 return false;
}
function worksheetEqual(a,b){if(a===b)return true;if(a===null||b===null||typeof a!=='object'||typeof b!=='object'||Array.isArray(a)!==Array.isArray(b))return false;return Object.keys(a).length===Object.keys(b).length&&Object.keys(a).every(k=>Object.hasOwn(b,k)&&worksheetEqual(a[k],b[k]));}
function worksheetCOrder(a,b){const e=new TextEncoder(),x=e.encode(a),y=e.encode(b);for(let i=0;i<Math.min(x.length,y.length);i++)if(x[i]!==y[i])return x[i]>y[i]?1:-1;return x.length===y.length?0:x.length>y.length?1:-1;}
function worksheetOrder(a,b,sort){
 if(a[0]!==b[0])return a[0]?1:-1;
 if(!a[0]&&a[1]!==b[1]){const numeric=/^(GROSS|ADVANCE|OVERPAYMENT|NET|CASH)_/.test(sort);
  let c;if(numeric){if(!worksheetField(a[1],'worksheetMoney')||!worksheetField(b[1],'worksheetMoney'))return null;const p=v=>BigInt(v.replace('.',''));c=p(a[1])===p(b[1])?0:p(a[1])>p(b[1])?1:-1;}else c=worksheetCOrder(a[1],b[1]);
  if(c)return sort.endsWith('_DESC')?-c:c;
 }
 return a[2]!==b[2]?(a[2]>b[2]?1:-1):a[3]===b[3]?0:a[3]>b[3]?1:-1;
}
function worksheetResult(p,args,actor,base,shape,bad){
 if(!shape(p,{context:'worksheetContext',rows:'worksheetRows',complete:'boolean',next_cursor:'worksheetCursor?'}))bad();
 const c=p.context;if(c.run_id!==args.run_id||c.pay_channel!=='PAYE'||c.actor_user_id!==actor
  ||c.candidate_id!==args.candidate_id||c.query!==args.query||c.sort!==args.sort||p.rows.length>args.limit
  ||(args.after!==null&&!worksheetEqual(args.after.context,c))||p.complete!==(p.next_cursor===null)
  ||(!p.complete&&p.rows.length===0))bad();
 let previous=null;const seen=new Set(),columns={GROSS:'earnings_gross',ADVANCE:'advance_repayment',OVERPAYMENT:'overpayment_recovery',NET:'accepted_net',CASH:'cash_amount',SOURCE:'input_source',STATUS:'projection_status'},column=columns[args.sort.split('_')[0]];
 for(const r of p.rows){
  if(seen.has(r.run_worker_id)||r.run_worker_id===args.after?.run_worker_id||r.sort_key[2]!==r.candidate_id||r.sort_key[3]!==r.run_worker_id
   ||(args.candidate_id!==null&&r.candidate_id!==args.candidate_id)
   ||(column&&(r.sort_key[1]!==r[column]||r.sort_key[0]!==(r[column]===null)))
   ||(previous!==null&&(worksheetOrder(r.sort_key,previous,args.sort)??0)<=0))bad();
  seen.add(r.run_worker_id);previous=r.sort_key;
  if(r.applicable_net!==(BigInt(r.captured_line_count)>0n||r.payroll_gross!=='0.00')
   ||r.payout_only!== (r.net_state==='PAYOUT_ONLY')
   ||(r.payout_only&&(r.applicable_net||r.accepted_net!==null||r.input_source!==null))
   ||(r.net_state==='ENTERED_ZERO')!==(r.accepted_net==='0.00')
   ||(r.net_state==='ENTERED'&&(r.accepted_net===null||r.accepted_net==='0.00'))
   ||(r.net_state==='MISSING'&&r.accepted_net!==null)
   ||(r.projection_id===null&&(r.accepted_net!==null||r.cash_amount!==null||r.input_source!==null))
   ||(r.projection_id!==null&&(r.cash_amount===null||r.expected_projection_revision==='0'))
   ||(r.accepted_net!==null&&(r.projection_id===null||r.input_source===null))
   ||(r.projection_status==='ACCEPTED'&&(r.projection_id===null||r.pending_net!==null))
   ||(r.projection_status==='NONE'&&(r.projection_id!==null||r.pending_net!==null))
   ||(r.pending_net!==null&&!['REQUESTED','APPLYING','REFUSED'].includes(r.projection_status))||!validNetReplacement(r))bad();
 }
 if(p.next_cursor!==null&&(!worksheetEqual(p.next_cursor.context,c)||p.next_cursor.candidate_id!==previous?.[2]||p.next_cursor.run_worker_id!==previous?.[3]))bad();
 if(new TextEncoder().encode(JSON.stringify(p)).byteLength>128*1024)bad();
 return p;
}

// DR16 explicit frozen review contracts, shared shape in Worker and Office.
const REVIEW_CONTEXT={run_id:'uuid',actor_user_id:'uuid',owner_epoch:'positive',review_revision:'integer',run_status:'reviewRun',selection_state:'reviewSelection',paye_totals_revision:'integer',umbrella_totals_revision:'integer',run_worker_id:'uuid?',candidate_id:'uuid?',worker_status:'reviewWorker?',preparation_revision:'positive?',captured_preparation_revision:'positive?',case_selection_revision:'integer?',net_request_revision:'integer?',net_projection_revision:'integer?',sort:'reviewCaseSort?'};
const REVIEW_TOTAL={candidate_count:'integer',ready_count:'integer',gross_ex_vat:'reviewMoney',gross_vat:'reviewMoney',gross_inc_vat:'reviewMoney',gross_stage_deductions:'reviewMoney',net_recovery_reserved:'reviewMoney',cash_known:'reviewMoney',net_missing_count:'integer',applicable_net_missing_count:'integer',net_replacement_pending_count:'integer',net_refused_code:'reviewNetRefusal?'};
const REVIEW_CASE={case_id:'uuid',case_component_id:'uuid',instruction_id:'uuid?',selected:'boolean',case_subtype:'label?',direction:'reviewDirection?',payroll_stage:'reviewStage?',source_pay_channel:'channel?',label:'label',due:'reviewAmount?',this_draft:'reviewAmount?',outstanding_after:'reviewAmount?',principal_outstanding:'reviewAmount?',active_reserved_before:'reviewAmount?',recoverable_this_run:'reviewAmount?',remaining_unreserved:'reviewAmount?',unreserved_limit_note:'reviewLimitNote?',result_basis:'reviewBasis?',reason_code:'issue?',note:'label',captured_at_utc:'timestamp?',other_run_capacity_reduction:'reviewAmount?',other_run_capacity_reason:'reviewForeign?',sort_key:'reviewKey'};
function reviewField(v,t,base,shape){
 if(t==='reviewAmount')return typeof v==='string'&&/^(?:0|[1-9]\d{0,15})(?:\.\d{1,2})?$/.test(v);
 if(t==='reviewMoney')return typeof v==='string'&&/^-?(?:0|[1-9]\d{0,35})(?:\.\d{1,2})?$/.test(v);
 if(t==='reviewRun')return ['PREPARING','REVIEW','DRAFT','EXECUTING','COMPLETE','CANCELLING','CANCELLED'].includes(v);
 if(t==='reviewSelection')return ['OPEN','SEALED'].includes(v);
 if(t==='reviewWorker')return ['PREPARING','REVIEW','READY','DRAFT','ISSUED','COMPLETE','CANCELLING','CANCELLED'].includes(v);
 if(t==='reviewCaseSort')return typeof v==='string'&&/^(DEDUCTION|DUE|THIS_DRAFT|OUTSTANDING|NOTE)_(ASC|DESC)$/.test(v);
 if(t==='reviewNetRefusal')return ['CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND'].includes(v);
 if(t==='reviewDirection')return ['PAYMENT','DEDUCTION'].includes(v);
 if(t==='reviewStage')return ['GROSS_ADD','GROSS_DEDUCT','NET_ADD','NET_DEDUCT'].includes(v);
 if(t==='reviewBasis')return ['DRAFT','NET_ACCEPTED'].includes(v);
 if(t==='reviewLimitNote')return v==='Shared balance limits this amount';
 if(t==='reviewForeign')return ['SAME_WEEK_INSTALMENT','CASE_BALANCE'].includes(v);
 if(t==='reviewContext')return shape(v,REVIEW_CONTEXT);
 if(t==='reviewKey')return Array.isArray(v)&&v.length===3&&typeof v[0]==='boolean'&&(v[0]?v[1]===null:base(v[1],'label'))&&base(v[2],'uuid');
 if(t==='reviewCursor')return shape(v,{context:'reviewContext',key:'reviewKey'})&&new TextEncoder().encode(JSON.stringify(v)).byteLength<=8192;
 if(t==='reviewCases')return Array.isArray(v)&&v.length<=100&&v.every(r=>shape(r,REVIEW_CASE));
 if(t==='reviewTotal')return shape(v,REVIEW_TOTAL);
 if(t==='reviewChannels')return Array.isArray(v)&&v.length===2&&v[0].pay_channel==='PAYE'&&v[1].pay_channel==='UMBRELLA'&&v.every(r=>shape(r,{pay_channel:'channel',...REVIEW_TOTAL,totals_revision:'integer',updated_at_utc:'timestamp'}));
 return false;
}
function reviewPence(v){const negative=v.startsWith('-'),[w,f='']=v.replace(/^-/,'').split('.');return (negative?-1n:1n)*(BigInt(w)*100n+BigInt(f.padEnd(2,'0')));}
function reviewSame(a,b){if(a===b)return true;if(a===null||b===null||typeof a!=='object'||typeof b!=='object'||Array.isArray(a)!==Array.isArray(b))return false;return Object.keys(a).length===Object.keys(b).length&&Object.keys(a).every(k=>Object.hasOwn(b,k)&&reviewSame(a[k],b[k]));}
function reviewCompare(a,b,sort){
 if(a[0]!==b[0])return a[0]?1:-1;
 const numeric=/^(DUE|THIS_DRAFT|OUTSTANDING)_/.test(sort),enc=new TextEncoder();
 const bytes=(x,y)=>{const a=enc.encode(x??''),b=enc.encode(y??'');for(let i=0;i<Math.min(a.length,b.length);i++)if(a[i]!==b[i])return a[i]<b[i]?-1:1;return a.length===b.length?0:a.length<b.length?-1:1;};
 let n=numeric?(reviewPence(a[1]??'0')===reviewPence(b[1]??'0')?0:reviewPence(a[1]??'0')<reviewPence(b[1]??'0')?-1:1):bytes(a[1],b[1]);
 if(n===0)n=bytes(a[2],b[2]);return sort.endsWith('_DESC')?-n:n;
}
function reviewResult(p,kind,args,actor,base,shape,bad){
 const totals=kind==='REVIEW_TOTALS';
 if(!shape(p,totals?{context:'reviewContext',whole:'reviewTotal',channels:'reviewChannels',captured_at_utc:'timestamp',observed_at_utc:'timestamp'}:{context:'reviewContext',rows:'reviewCases',complete:'boolean',next_cursor:'reviewCursor?',counts:'reviewCounts'}))bad();
 const c=p.context;if(c.run_id!==args.run_id||c.actor_user_id!==actor)bad();
 if(totals){
  if(['run_worker_id','candidate_id','worker_status','preparation_revision','captured_preparation_revision','case_selection_revision','net_request_revision','net_projection_revision','sort'].some(k=>c[k]!==null))bad();
  if(c.paye_totals_revision!==p.channels[0].totals_revision||c.umbrella_totals_revision!==p.channels[1].totals_revision)bad();
  for(const row of [p.whole,...p.channels]){
   if(BigInt(row.ready_count)>BigInt(row.candidate_count)||BigInt(row.applicable_net_missing_count)>BigInt(row.net_missing_count)||BigInt(row.net_missing_count)>BigInt(row.candidate_count)||BigInt(row.net_replacement_pending_count)>BigInt(row.candidate_count)||reviewPence(row.gross_inc_vat)!==reviewPence(row.gross_ex_vat)+reviewPence(row.gross_vat)||['gross_stage_deductions','net_recovery_reserved','cash_known'].some(k=>reviewPence(row[k])<0n))bad();
  }
  for(const k of Object.keys(REVIEW_TOTAL).filter(k=>k!=='net_refused_code')){const n=REVIEW_TOTAL[k]==='reviewMoney'?reviewPence:BigInt;if(n(p.whole[k])!==n(p.channels[0][k])+n(p.channels[1][k]))bad();}
  const codes=p.channels.map(r=>r.net_refused_code).filter(x=>x!==null).sort();if(p.whole.net_refused_code!==(codes[0]??null))bad();
  return p;
 }
 if(c.run_worker_id!==args.run_worker_id||c.candidate_id===null||c.worker_status===null||['preparation_revision','captured_preparation_revision','case_selection_revision','net_request_revision','net_projection_revision'].some(k=>c[k]===null)||c.sort!==args.sort||p.rows.length>args.limit||p.complete!==(p.next_cursor===null)||!p.complete&&p.rows.length===0||args.after!==null&&!reviewSame(args.after.context,c)||BigInt(p.counts.selected)+BigInt(p.counts.excluded)!==BigInt(p.counts.scope_count))bad();
 let previous=args.after?.key??null;const ids=new Set();
 for(const r of p.rows){
  if(r.recoverable_this_run!==(r.selected&&r.direction==='DEDUCTION'?r.this_draft:null)
   ||r.remaining_unreserved!==null&&(r.principal_outstanding===null||r.active_reserved_before===null||r.captured_at_utc===null)
   ||r.unreserved_limit_note!==null&&r.remaining_unreserved===null
   ||r.remaining_unreserved!==null&&reviewPence(r.remaining_unreserved)>((reviewPence(r.principal_outstanding)-reviewPence(r.active_reserved_before))>0n?reviewPence(r.principal_outstanding)-reviewPence(r.active_reserved_before):0n))bad();
  if((r.other_run_capacity_reduction===null||reviewPence(r.other_run_capacity_reduction)===0n)!==(r.other_run_capacity_reason===null)||ids.has(r.case_component_id)||r.sort_key[2]!==r.case_component_id||!r.selected&&(r.instruction_id!==null||r.this_draft!==null||r.result_basis!==null)||r.instruction_id===null&&r.result_basis!==null||r.outstanding_after!==null&&reviewPence(r.outstanding_after)<0n)bad();ids.add(r.case_component_id);
  const value=args.sort.startsWith('DEDUCTION_')?r.label:args.sort.startsWith('DUE_')?r.due:args.sort.startsWith('THIS_DRAFT_')?r.this_draft:args.sort.startsWith('OUTSTANDING_')?r.outstanding_after:r.note;
  if(!reviewSame(r.sort_key,[value===null,value,r.case_component_id])||previous!==null&&reviewCompare(r.sort_key,previous,args.sort)<=0)bad();previous=r.sort_key;
 }
 if(p.next_cursor!==null&&(!reviewSame(p.next_cursor.context,c)||!reviewSame(p.next_cursor.key,p.rows.at(-1).sort_key)))bad();return p;
}

const ACTIVE_PREPARATION_INPUT={agency_id:'uuid',owner_epoch:'positive',scope:'activeScope',after:'activeCursor?',limit:'limit'};
const ACTIVE_PREPARATION_CONTEXT={agency_id:'uuid',actor_user_id:'uuid',owner_epoch:'positive',scope:'activeScope'};
const ACTIVE_PREPARATION_ROW={run_id:'uuid',actor_user_id:'uuid',actor_display_name:'label',status:'activeStatus',selection_state:'activeSelection',started_at_utc:'timestamp',expires_at_utc:'timestamp',selected_candidates:'integer',ready:'integer',review:'integer',pending:'integer',preparing:'integer',removed:'integer',safe_to_close:'true'};
function activePreparationField(v,t,base,shape){
 if(t==='activeScope')return ['ALL','MINE'].includes(v);
 if(t==='activeStatus')return ['PREPARING','REVIEW'].includes(v);
 if(t==='activeSelection')return ['OPEN','SEALED'].includes(v);
 if(t==='activeContext')return shape(v,ACTIVE_PREPARATION_CONTEXT);
 if(t==='activeCursor')return shape(v,{context:'activeContext',created_at_utc:'timestamp',run_id:'uuid'})&&new TextEncoder().encode(JSON.stringify(v)).byteLength<=8192;
 if(t==='activeRows')return Array.isArray(v)&&v.length<=100&&v.every(r=>shape(r,ACTIVE_PREPARATION_ROW));
 return false;
}
function activePreparationMicros(value){
 const m=value.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(\d{1,6}))?(?:Z|[+-]\d{2}:\d{2})$/),ms=Date.parse(value);
 if(!m||!Number.isFinite(ms))return null;
 return BigInt(ms)*1000n+BigInt((m[1]??'').padEnd(6,'0').slice(3));
}
function activePreparationsResult(p,args,actor,base,shape,bad){
 if(!shape(p,{context:'activeContext',rows:'activeRows',complete:'boolean',next_cursor:'activeCursor?'}))bad();
 const c=p.context;
 if(c.actor_user_id!==actor||c.agency_id!==args.agency_id||c.owner_epoch!==args.owner_epoch||c.scope!==args.scope
  ||p.rows.length>args.limit||p.complete!==(p.next_cursor===null)||!p.complete&&p.rows.length===0
  ||args.after!==null&&!reviewSame(args.after.context,c))bad();
 let previous=args.after===null?null:{time:activePreparationMicros(args.after.created_at_utc),id:args.after.run_id.toLowerCase()};
 if(previous&&previous.time===null)bad();const ids=new Set();
 for(const r of p.rows){
  const time=activePreparationMicros(r.started_at_utc),expiry=activePreparationMicros(r.expires_at_utc),id=r.run_id.toLowerCase(),n=k=>BigInt(r[k]);
  if(time===null||expiry===null||expiry-time!==259200000000n||!r.actor_display_name.trim()||ids.has(id)
   ||c.scope==='MINE'&&r.actor_user_id!==actor||n('ready')+n('review')+n('pending')+n('removed')!==n('selected_candidates')
   ||n('preparing')>n('pending')||previous&&(time>previous.time||time===previous.time&&id>=previous.id))bad();
  ids.add(id);previous={time,id};
 }
 if(p.next_cursor!==null&&(!reviewSame(p.next_cursor.context,c)||p.next_cursor.run_id!==p.rows.at(-1).run_id
  ||p.next_cursor.created_at_utc!==p.rows.at(-1).started_at_utc))bad();return p;
}

const PREPARE_ISSUE_INPUT={run_id:'uuid',view:'prepareIssueView',sort:'prepareIssueSort',after:'prepareIssueCursor?',limit:'limit',row_key:'prepareIssueRowKey?',waiting_only:'boolean'};
const PREPARE_ISSUE_CONTEXT={run_id:'uuid',actor_user_id:'uuid',owner_epoch:'positive',review_revision:'integer',run_status:'runStatus',selection_state:'selectionState',selection_pages:'integer',selected_candidates:'integer',preparing:'integer',ready:'integer',review:'integer',removed:'integer',workbench_revision:'integer',adoption_revision:'integer',view:'prepareIssueView',sort:'prepareIssueSort',row_key:'prepareIssueRowKey?',waiting_only:'boolean',paye_totals_revision:'integer?',umbrella_totals_revision:'integer?'};
const PREPARE_ISSUE_ROW={row_key:'prepareIssueRowKey',origin:'prepareIssueOrigin',candidate_id:'uuid',run_worker_id:'uuid?',worker_status:'prepareIssueState',issue_code:'issue?',work_id:'uuid?',expected_revision_id:'uuid?',physical_timesheet_id:'uuid?',physical_timesheet_version:'positive?',candidate_display_name:'label?',candidate_reference:'label?',label_origin:'prepareIssueLabelOrigin',timesheet_reference:'label?',week_ending_date:'date?',frozen_ex_vat:'money?',preparation_revision:'positive?',discard_outcome:'discardOutcome?',discard_blocked_code:'discardBlockedCode?',waiting_on_other_run:'boolean',waiting_run_id:'uuid?',waiting_actor_display_name:'label?',capacity_reduced_by_other_run:'boolean',pay_channel:'channel?',deduction_total:'money?',frozen_offer_complete:'boolean',case_count:'integer?',sort_key:'prepareIssueKey'};
function prepareIssueField(v,t,base,shape){
  if(t==='prepareIssueView')return ['CANDIDATES','NOT_IN_DRAFT'].includes(v);
  if(t==='prepareIssueSort')return /^(CANDIDATE|PAYMENT|REASON|GROSS|DEDUCTIONS|CHANNEL|STATE)_(ASC|DESC)$/.test(v)&&typeof v==='string';
  if(t==='prepareIssueRowKey')return typeof v==='string'&&v.length<=160&&/^(?:(?:SKIP|RELEASE|CANDIDATE|EXPIRED|DISCARDED):|PAGE:[1-9][0-9]{0,18}:)[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(v);
  if(t==='prepareIssueKey')return Array.isArray(v)&&v.length===3&&typeof v[0]==='boolean'&&(v[0]?v[1]===null:base(v[1],'label'))&&prepareIssueField(v[2],'prepareIssueRowKey',base,shape);
  if(t==='prepareIssueContext')return shape(v,PREPARE_ISSUE_CONTEXT);
  if(t==='prepareIssueCursor')return shape(v,{context:'prepareIssueContext',key:'prepareIssueKey'});
  if(t==='prepareIssueOrigin')return ['CANDIDATE','SKIPPED_WORK','SYSTEM_RELEASE','OFFICE_RELEASE','EXPIRED','DISCARDED'].includes(v);
  if(t==='prepareIssueState')return ['PENDING','FAILED','NOT_IN_DRAFT','PREPARING','REVIEW','READY','DRAFT','CANCELLING','CANCELLED','ISSUED','COMPLETE'].includes(v);
  if(t==='prepareIssueLabelOrigin')return ['CAPTURED_WORK','CURRENT_CANDIDATE','UNKNOWN_ORIGINAL'].includes(v);
  return false;
}
function prepareIssuesResult(payload,args,actor,base,shape,bad){
  if(!shape(payload,{context:'prepareIssueContext',rows:'prepareIssueRows',complete:'boolean',next_cursor:'prepareIssueCursor?',counts:'prepareIssueCounts'}))bad();
  const h=payload.context,ids=new Set(),sameJson=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
  if(h.run_id!==args.run_id||h.actor_user_id!==actor||h.view!==args.view||h.sort!==args.sort||h.row_key!==args.row_key||h.waiting_only!==args.waiting_only
    ||payload.rows.length>args.limit||(!payload.complete&&payload.rows.length===0)||payload.complete!==(payload.next_cursor===null)
    ||args.after!==null&&!sameJson(h,args.after.context))bad();
  for(const r of payload.rows){
    if(ids.has(r.row_key)||r.sort_key[2]!==r.row_key||args.row_key!==null&&r.row_key!==args.row_key
      ||(r.physical_timesheet_id===null)!==(r.physical_timesheet_version===null)
      ||r.physical_timesheet_id!==null&&(r.work_id===null||r.expected_revision_id===null||r.week_ending_date===null)
      ||(r.discard_outcome==='BLOCKED')!==(r.discard_blocked_code!==null))bad();ids.add(r.row_key);
    if(/^(GROSS|DEDUCTIONS)_/.test(args.sort)&&r.sort_key[1]!==null&&!base(r.sort_key[1],'money'))bad();
    if(r.waiting_on_other_run!==(r.waiting_run_id!==null)||!r.waiting_on_other_run&&r.waiting_actor_display_name!==null||r.waiting_run_id===args.run_id||args.waiting_only&&!r.waiting_on_other_run)bad();
    if(r.origin==='CANDIDATE'){
      if(args.view!=='CANDIDATES'||r.row_key!=='CANDIDATE:'+r.candidate_id
        ||(r.run_worker_id===null)!==(['PENDING','FAILED'].includes(r.worker_status))
        ||r.run_worker_id!==null&&r.preparation_revision===null||['PENDING','PREPARING','CANCELLING','CANCELLED','FAILED'].includes(r.worker_status)&&r.frozen_ex_vat!==null||r.worker_status==='REVIEW'&&!r.frozen_offer_complete&&r.frozen_ex_vat!==null||r.frozen_offer_complete&&(!['READY','REVIEW','DRAFT','ISSUED','COMPLETE'].includes(r.worker_status)||r.run_worker_id===null||r.frozen_ex_vat===null||r.deduction_total===null||r.pay_channel===null)||!r.frozen_offer_complete&&r.deduction_total!==null)bad();
    }else{
      if(r.pay_channel!==null||r.deduction_total!==null||r.frozen_offer_complete||r.case_count!==null||r.waiting_on_other_run||r.capacity_reduced_by_other_run||args.view!=='NOT_IN_DRAFT'||r.worker_status!=='NOT_IN_DRAFT'||r.issue_code===null||r.frozen_ex_vat!==null)bad();
      if(r.origin==='SKIPPED_WORK'&&(r.run_worker_id!==null||r.work_id===null||!r.row_key.endsWith(':'+r.work_id)))bad();
      if(['SYSTEM_RELEASE','OFFICE_RELEASE'].includes(r.origin)&&(r.run_worker_id===null||r.row_key!=='RELEASE:'+r.run_worker_id||r.preparation_revision===null))bad();
      if(r.origin==='SYSTEM_RELEASE'&&!['SELECTED_WORK_NOT_APPROVED','SELECTED_WORK_REVISION_CHANGED','SELECTED_WORK_POSITION_PENDING','SOURCE_PAY_QUERY_OPEN','SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE','CASE_NO_PAYABLE_AMOUNT'].includes(r.issue_code))bad();
      if(r.origin==='OFFICE_RELEASE'&&r.issue_code!=='DRAFT_CANDIDATE_REMOVED'||r.origin==='EXPIRED'&&(r.issue_code!=='PREPARATION_EXPIRED'||r.row_key!=='EXPIRED:'+r.candidate_id)||r.origin==='DISCARDED'&&(r.issue_code!=='DRAFT_DISCARDED'||r.row_key!=='DISCARDED:'+r.candidate_id))bad();
    }
  }
  if(payload.next_cursor!==null&&(!sameJson(payload.next_cursor.context,h)||!sameJson(payload.next_cursor.key,payload.rows.at(-1).sort_key)))bad();
  return payload;
}

export const BPAY_NEXT_PREPARATION_INPUTS=Object.freeze({
  PREPARATION_DISCARD:{command_id:'uuid',run_id:'uuid',expected_preparation_revision:'integer'},
  WAKE:{run_id:'uuid'},
  EXCLUSION_PAGE:{run_id:'uuid',page_no:'positive',excluded_work_ids:'emptyIds',excluded_candidate_ids:'emptyIds'},
  MATCHING_PAGE:{run_id:'uuid',page_no:'positive',after_member_no:'positive?'},

  CREATE:{run_id:'uuid',command_id:'uuid',descriptor:'descriptor'},
  PAYE_GUARD_DECISION:{run_id:'uuid',command_id:'uuid',descriptor:'descriptor',decision:'guardDecision',reason:'guardReason?'},
  WORK_PAGE:{run_id:'uuid',page_no:'positive',choices:'choices'},
  COMPONENT_PAGE:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',page_no:'positive',approved_line_ids:'ids'},
  COMPONENT_SEAL:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',expected_pages:'positive',expected_count:'positive'},
  SEGMENT_PAGE:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',page_no:'positive',request_id:'uuid',segment_keys:'segmentKeys'},
  SEGMENT_SEAL:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',expected_pages:'integer',expected_count:'integer'},
  CASE_PAGE:{run_id:'uuid',candidate_id:'uuid',selection_revision:'positive',page_no:'positive',request_id:'uuid',component_ids:'ids',selected:'booleans'},
  CASE_SEAL:{run_id:'uuid',candidate_id:'uuid',selection_revision:'positive',expected_pages:'positive',expected_items:'positive'},
  SEAL_ENQUEUE:{run_id:'uuid',command_id:'uuid',expected_pages:'integer',expected_count:'integer'},
  WORKER_REPREPARE:{command_id:'uuid',run_worker_id:'uuid',expected_preparation_revision:'positive'},
  WORKER_RELEASE:{command_id:'uuid',run_worker_id:'uuid',expected_preparation_revision:'positive'},
  REVIEW:{run_id:'uuid'},CONFIRM:{run_id:'uuid',review_revision:'positive'}
});
export const BPAY_NEXT_PREPARATION_PAGE_INPUTS=Object.freeze({
  CHANNEL_DRAFT_LIST:{...CHANNEL_DRAFT_INPUT,after:'channelDraftCursor?',limit:'limit'},
  CHANNEL_DRAFT_COUNT:CHANNEL_DRAFT_INPUT,
  ACTIVE_PREPARATIONS:ACTIVE_PREPARATION_INPUT,
  REVIEW_TOTALS:{run_id:'uuid'},
  REVIEW_CASES:{run_id:'uuid',run_worker_id:'uuid',sort:'reviewCaseSort',after:'reviewCursor?',limit:'limit'},
  PREPARE_ISSUES:PREPARE_ISSUE_INPUT,
  CASE_AUDIT:{candidate_id:"uuid",case_id:"uuid",sort:"caseFinanceSort",direction:"caseFinanceDirection",limit:"limit",after:"caseFinanceCursor?",include_count:"boolean"},
  CANDIDATE_CASE_REPORT:{candidate_id:"uuid",active_only:"boolean",sort:"caseFinanceSort",direction:"caseFinanceDirection",limit:"limit",after:"caseFinanceCursor?",include_count:"boolean"},
  CASE_LIST:{filters:'caseListFilters',after:'caseListCursor?',sort:'caseListSort',limit:'caseListLimit'},
  CASE_SUMMARY:{filters:'caseListFilters',after:'caseListCursor?'},
  CASE_PREVIEW:{input:'caseInput'},
  GUARD:{run_id:'uuid'},
  PROGRESS:{run_id:'uuid'},
  ACCEPTED_CANDIDATES:{run_id:'uuid',after_candidate_id:'uuid?',limit:'limit'},
  ACCEPTED_WORK:{run_id:'uuid',candidate_id:'uuid',after_work_id:'uuid?',limit:'limit'},

  COMPONENTS:{work_id:'uuid',expected_revision_id:'uuid',after_line_no:'integer?',limit:'limit'},
  WORK_CHOICES:{run_id:'uuid',after_work_id:'uuid?',limit:'limit'},
  SELECTED_COMPONENTS:{run_id:'uuid',work_id:'uuid',after_selection_no:'integer?',limit:'limit'},
  CHANNEL_DRAFTS:{run_id:'uuid'},
  PAYE_WORKSHEET:WORKSHEET_INPUT,
  CHANNEL_WORKERS:{run_id:'uuid',pay_channel:'channel',after_candidate_id:'uuid?',limit:'limit'},
  CHANNEL_STATUS:{run_id:'uuid',pay_channel:'channel',run_worker_id:'uuid',after_transfer_no:'transferNo?',limit:'limit'},
  WORKERS:{run_id:'uuid',after_candidate_id:'uuid?',limit:'limit'},
  REVISION_COMPARISON:{run_worker_id:'uuid',after_work_id:'uuid?',limit:'limit'},
  LINES:{run_worker_id:'uuid',after_line_no:'integer?',limit:'limit'},
  SHIFTS:{work_id:'uuid',expected_revision_id:'uuid',approved_line_id:'uuid',after_detail_no:'integer?',limit:'limit'},
  BREAKS:{work_id:'uuid',expected_revision_id:'uuid',approved_line_id:'uuid',shift_detail_id:'uuid',after_break_no:'integer?',limit:'limit'},
  RATES:{work_id:'uuid',expected_revision_id:'uuid',approved_line_id:'uuid',after_bucket:'bucket?',limit:'limit'},
  CURRENT_WORK:{candidate_id:'uuid',after_week:'date?',after_work_id:'uuid?',limit:'limit'},
  CURRENT_CASES:{candidate_id:'uuid',after_case_id:'uuid?',limit:'limit'},
  CURRENT_STORED_CREDITS:{candidate_id:'uuid',after_legacy_case_id:'uuid?',limit:'limit'},
  CASE_CHANNEL_SUGGEST:{case_id:'uuid',expected_case_revision:'positive',input:'channelInput?',after:'channelCursor?',limit:'limit'},
  CASE_RESTRUCTURE_PREVIEW:{case_id:'uuid',expected_case_revision:'positive',input:'restructureInput?'},
  CASE_COMPONENTS:{candidate_id:'uuid',case_id:'uuid',after_component_ordinal:'integer?',limit:'limit'},
  STATUS:{run_id:'uuid',run_worker_id:'uuid',after_transfer_no:'transferNo?',limit:'limit'},
  RETURN_CASH:{run_id:'uuid',run_worker_id:'uuid',after_transfer_no:'transferNo?',limit:'limit'}
});
const OUTPUTS={
  PREPARATION_DISCARD:{command_id:'uuid',run_id:'uuid',sequence:'positive?',phase:'discardReceiptPhase',expected_candidates:'integer',completed_candidates:'integer',removed_candidates:'integer',already_removed_candidates:'integer',blocked_candidates:'integer',replay:'boolean'},
  WAKE:{run_id:'uuid',accepted:'boolean',throttled:'boolean'},
  EXCLUSION_PAGE:{run_id:'uuid',page_no:'positive',item_count:'integer',complete:'boolean',replay:'boolean'},
  MATCHING_PAGE:{run_id:'uuid',page_no:'positive',item_count:'integer',selection_count:'integer',selected_candidate_count:'integer',replay:'boolean',skipped:'skipped',complete:'boolean',next_after_member_no:'positive?'},

  CREATE:{run_id:'uuid',status:'runStatus',selection_state:'selectionState',selection_count:'integer',selected_candidate_count:'integer',official_pay_date:'date',command_id:'uuid',agency_sequence:'positive?',choice_work_count:'integer',choice_case_count:'integer',empty:'boolean',replay:'boolean'},
  PAYE_GUARD_DECISION:{run_id:'uuid',status:'runStatus',selection_state:'selectionState',selection_count:'integer',selected_candidate_count:'integer',official_pay_date:'date',command_id:'uuid',agency_sequence:'positive?',choice_work_count:'integer',choice_case_count:'integer',empty:'boolean',replay:'boolean',decision:'guardDecision',channel_scope:'guardChannel'},
  WORK_PAGE:{run_id:'uuid',page_no:'positive',item_count:'positive',selection_count:'integer',selected_candidate_count:'integer',replay:'boolean',skipped:'skipped'},
  COMPONENT_PAGE:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',page_no:'positive',item_count:'positive',component_count:'positive',replay:'boolean'},
  COMPONENT_SEAL:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',sealed:'true',component_count:'positive',replay:'boolean'},
  SEGMENT_PAGE:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',page_no:'positive',item_count:'positive',segment_count:'positive',replay:'boolean'},
  SEGMENT_SEAL:{run_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',sealed:'true',segment_count:'integer',replay:'boolean'},
  CASE_PAGE:{run_id:'uuid',candidate_id:'uuid',selection_revision:'positive',page_no:'positive',item_count:'positive',replay:'boolean'},
  CASE_SEAL:{run_id:'uuid',candidate_id:'uuid',selection_revision:'positive',sealed:'true',selected_count:'integer',excluded_count:'integer',replay:'boolean'},
  SEAL_ENQUEUE:{run_id:'uuid',command_id:'uuid',agency_sequence:'positive',selection_count:'integer',replay:'boolean'},
  WORKER_REPREPARE:{command_id:'uuid',run_id:'uuid',run_worker_id:'uuid',action:'workerAction',expected_preparation_revision:'positive',preparation_revision:'positive',review_revision:'positive',agency_sequence:'positive',phase:'workerActionPhase',replay:'boolean'},
  WORKER_RELEASE:{command_id:'uuid',run_id:'uuid',run_worker_id:'uuid',action:'workerAction',expected_preparation_revision:'positive',preparation_revision:'positive',review_revision:'positive',agency_sequence:'positive',phase:'workerActionPhase',replay:'boolean'},
  REVIEW:{run_id:'uuid',phase:'reviewPhase',reason:'reviewReason?',review_revision:'integer',all_ready:'boolean?',replay:'boolean'},
  CONFIRM:{run_id:'uuid',phase:'draft',review_revision:'positive',replay:'boolean',drafts:'drafts'}
};
const ROWS={
  PROGRESS:{needs_attention:'boolean',waiting_on_other_run:'boolean',run_id:'uuid',status:'runStatus',selection_state:'selectionState',actor_display_name:'label',started_at_utc:'timestamp',expires_at_utc:'timestamp',selected_candidates:'integer',ready:'integer',review:'integer',pending:'integer',preparing:'integer',removed:'integer',enrolled_candidates:'integer',enrolment_complete:'boolean',enrolment_failed:'boolean',safe_to_close:'true',expired_at_utc:'timestamp?',review_revision:'integer',discard_phase:'discardPhase?',discard_actor_display_name:'label?',discarded_at_utc:'timestamp?',discard_removed_candidates:'integer',discard_already_removed_candidates:'integer',discard_blocked_candidates:'integer'},
  ACCEPTED_CANDIDATES:{candidate_id:'uuid',accepted_input_id:'uuid',module_epoch:'positive',target_pay_channel:'channel',target_umbrella_id:'uuid?',pay_week_start:'date',case_scope:'acceptedCaseScope',case_selection_revision:'positive?',settings_witness_kind:'settingsWitness',input_evidence_sha256:'sha',prepare_job_id:'uuid?',prepare_job_status:'prepareJobStatus?',prepare_job_phase:'prepareJobPhase?',run_worker_id:'uuid?',worker_status:'workerStatus?',has_unsealed_work:'boolean',waiting_command_id:'uuid?',waiting_command_sequence:'positive?',progress_state:'acceptedProgress',progress_code:'acceptedProgressCode?',review_issue_code:'issue?'},
  ACCEPTED_WORK:{candidate_id:'uuid',accepted_input_id:'uuid',accepted_work_input_id:'uuid',work_id:'uuid',original_revision_id:'uuid',effective_revision_id:'uuid',selection_mode:'mode',selected_line_count:'integer',publication_command_id:'uuid',predecessor_sequence:'positive',source_kind:'acceptedSourceKind',query_disposition:'acceptedQuery?',query_state_sha256:'sha?',input_evidence_sha256:'sha',effective_cut_id:'uuid?',cut_state:'acceptedCutState'},
  CHANNEL_WORKERS:{run_worker_id:'uuid',candidate_id:'uuid',pay_channel:'channel',worker_status:'workerStatus',frozen_ex_vat:'money',frozen_vat:'money',frozen_inc_vat:'money',review_issue_code:'issue?'},
  COMPONENTS:{approved_line_id:'uuid',line_no:'positive',component_key:'key',component_kind:'componentKind',work_date:'date?',source_pay_channel:'channel',
    approved_source_ex_vat:'money',realised_source_ex_vat:'money?',held_source_ex_vat:'money?',residual_source_ex_vat:'money?',position_ready:'boolean',unit_label:'unit?',approved_quantity:'money?',approved_unit_rate:'money?',job_title:'label?',band_label:'label?',original_work_ref:'label?',original_timesheet_id:'uuid?',is_adjustment:'boolean?',correction_kind:'label?',associated_rate_schedule_id:'uuid?',associated_rate_family:'rateFamily?',associated_rate_code:'nativeRateCode?',available_paye_rate:'money?',available_umbrella_rate:'money?',available_charge_rate:'money?',approved_charge_rate:'money?',standard_rate_schedule:'standardSchedule'},
  WORK_CHOICES:{work_id:'uuid',expected_revision_id:'uuid?',selection_mode:'mode',selection_state:'selectionState',page_count:'integer',component_count:'integer'},
  SELECTED_COMPONENTS:{selection_no:'positive',approved_line_id:'uuid',expected_revision_id:'uuid',component_key:'key'},
  WORKERS:{run_worker_id:'uuid',candidate_id:'uuid',worker_status:'workerStatus',frozen_ex_vat:'money?',frozen_vat:'money?',frozen_inc_vat:'money?',review_issue_code:'issue?',discard_outcome:'discardOutcome?',discard_blocked_code:'discardBlockedCode?'},
  REVISION_COMPARISON:{run_work_id:'uuid',work_id:'uuid',original_timesheet_id:'uuid',captured_revision_id:'uuid',current_revision_id:'uuid?',newer_approval_available:'boolean'},
  LINES:{run_line_id:'uuid',line_no:'positive',original_timesheet_id:'uuid',captured_revision_id:'uuid',component_key:'key',component_kind:'componentKind',
    work_date:'date?',source_pay_channel:'channel',target_pay_channel:'channel',source_consumed_ex_vat:'money',frozen_ex_vat:'money',frozen_vat:'money',frozen_inc_vat:'money'},
  SHIFTS:{shift_detail_id:'uuid',detail_no:'positive',work_date:'date',shift_start_at:'timestamp?',shift_end_at:'timestamp?',shift_start_local:'clock?',shift_end_local:'clock?',
    shift_overnight:'boolean?',submitted_minutes:'integer?',approved_minutes:'integer?',approved_hours:'money?',detail_label:'label?',segment_pay_ex_vat:'money?',pay_excluded:'boolean?',
    hours_day:'money?',hours_night:'money?',hours_sat:'money?',hours_sun:'money?',hours_bh:'money?',segment_key:'key?',approved_segment_id:'uuid?',job_title:'label?',band_label:'label?'},
  BREAKS:{break_detail_id:'uuid',break_no:'positive',break_start_at:'timestamp?',break_end_at:'timestamp?',break_start_local:'clock?',break_end_local:'clock?',break_minutes:'integer'},
  RATES:{rate_detail_id:'uuid',bucket:'bucket',approved_hours:'money',source_pay_rate:'money?',rate_schedule_id:'uuid?',available_paye_rate:'money?',available_umbrella_rate:'money?',available_charge_rate:'money?',approved_charge_rate:'money?'},
  CURRENT_WORK:{work_id:'uuid',candidate_id:'uuid',original_timesheet_id:'uuid',work_kind:'workKind',week_ending_date:'date',approval_state:'approvalState',
    current_revision_id:'uuid?',applied_revision_id:'uuid?',position_ready:'boolean',source_pay_channel:'channel?',timesheet_reference:'text?',client_display_name:'text?',approved_source_ex_vat:'money?'},
  STATUS:{transfer_id:'uuid',transfer_no:'transferNo',projection_id:'uuid?',execution_kind:'executionKind',status:'transferStatus',cash_amount:'money',original_transfer_id:'uuid?',reservation_state:'reservationState',remaining_reservation_cash:'cashMoney?',reservation_holds_state:'reservationHolds'},
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
    recovered_source_ex_vat:'money',written_off_source_ex_vat:'money',active_payout_source_ex_vat:'money',active_recovery_source_ex_vat:'money',case_principal_approved:'money',rule_id:'uuid',current_rule_id:'uuid?',rule_revision:'positive?',weekly_due_source_ex_vat:'money?',minimum_earnings_threshold:'money?',take_home_floor_override:'money?',schedule_start_monday:'date?',next_due_monday:'date?',schedule_week_count:'positive?',outstanding_source_ex_vat:'money',amount_due:'money?',recoverable_this_run:'null',original_liability_reference:'caseReference?',edit_state:'caseEditState',edit_receipt:'caseEditReceipt?',pause_state:'casePauseState',pause_receipt:'casePauseReceipt?',restructure_receipt:'restructureReceipt?',channel_receipt:'channelReceipt?'}
};
const ACCEPTED_HEADER={run_id:'uuid',prepare_command_id:'uuid',accepted_sequence:'positive',accepted_module_epoch:'positive',run_status:'runStatus',command_status:'prepareCommandStatus',expected_candidate_count:'positive',enrolled_candidate_count:'integer'};
const STATUS_HEADER={run_id:'uuid',run_status:'runStatus',selection_state:'selectionState',review_revision:'integer',run_worker_id:'uuid',candidate_id:'uuid',worker_status:'workerStatus',
  target_pay_channel:'channel',umbrella_id:'uuid?',preparation_revision:'positive',case_selection_revision:'integer',net_request_revision:'integer',net_projection_revision:'integer',net_pending:'boolean',net_replacement_pending_id:'uuid?',replacement_id:'uuid?',replacement_stage:'netReplacementStage?',net_request_high_water:'integer',requested_net:'money?',net_refused_code:'netRefusal?',replacement_request_no:'positive?',replacement_cursor:'positive?',replacement_expected_count:'integer?',replacement_staged_count:'integer?',replacement_validated_count:'integer?',replacement_committed_count:'integer?',
  review_issue_code:'issue?',review_issue_work_id:'uuid?',frozen_gross_ex_vat:'money',frozen_gross_vat:'money',frozen_gross_inc_vat:'money',gross_stage_deductions:'money',net_recovery_reserved:'money',frozen_offer_complete:'boolean',projection_id:'uuid?',
  projection_input_kind:'projectionKind?',entered_paye_net:'money?',accepted_recoveries:'money?',accepted_net_additions:'money?',cash_amount:'money?',reservation_mode:'reservationMode',reservation_projection_id:'uuid?',remaining_reservation_cash:'cashMoney?',reservation_internal_zero:'boolean',reservation_holds_state:'reservationHolds'};
const RETURN_CASH_HEADER={run_id:'uuid',run_worker_id:'uuid',candidate_id:'uuid',financial_view_revision:'integer'};
const CURSORS={COMPONENTS:['line_no','after_line_no'],WORK_CHOICES:['work_id','after_work_id'],SELECTED_COMPONENTS:['selection_no','after_selection_no'],
  CHANNEL_WORKERS:['candidate_id','after_candidate_id'],WORKERS:['candidate_id','after_candidate_id'],REVISION_COMPARISON:['work_id','after_work_id'],LINES:['line_no','after_line_no'],
  SHIFTS:['detail_no','after_detail_no'],BREAKS:['break_no','after_break_no'],RATES:['bucket','after_bucket'],STATUS:['transfer_no','after_transfer_no'],
  CURRENT_CASES:['case_id','after_case_id'],CURRENT_STORED_CREDITS:['legacy_case_id','after_legacy_case_id'],CASE_COMPONENTS:['component_ordinal','after_component_ordinal']};
class PreparationError extends Error {
  constructor(code,status=503,retryable=false,outcomeUnknown=false,refusal=null){super(code);Object.assign(this,{code,status,retryable,outcomeUnknown,refusal});}
}
const fault=(suffix,status=503,retryable=false,unknown=false)=>new PreparationError(`BPAY_NEXT_PREPARATION_${suffix}`,status,retryable,unknown);
const record=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
function exact(v,schema){return record(v)&&Object.keys(v).length===Object.keys(schema).length&&Object.entries(schema).every(([k,t])=>Object.hasOwn(v,k)&&field(v[k],t));}

function validDiscardProgress(p){
  const removed=BigInt(p.discard_removed_candidates),already=BigInt(p.discard_already_removed_candidates),blocked=BigInt(p.discard_blocked_candidates);
  if(p.discard_phase===null)return p.discard_actor_display_name===null&&p.discarded_at_utc===null&&removed+already+blocked===0n;
  if(!p.discard_actor_display_name?.trim()||p.expired_at_utc!==null||removed+already+blocked>BigInt(p.selected_candidates))return false;
  if(p.discard_phase==='CANCELLING')return p.status==='CANCELLING'&&p.discarded_at_utc===null;
  return p.discarded_at_utc!==null&&Date.parse(p.discarded_at_utc)>=Date.parse(p.started_at_utc)
    &&BigInt(p.pending)===0n&&BigInt(p.preparing)===0n
    &&(p.discard_phase==='DISCARDED'?p.status==='CANCELLED'&&blocked===0n:p.status==='REVIEW'&&blocked>0n);
}
function validReservation(h,row=null){
  const amountModes=['ORIGINAL_OBLIGATION','CANCELLING','CANCELLED'];
  if(amountModes.includes(h.reservation_mode)!==(h.remaining_reservation_cash!==null))return false;
  if(h.reservation_mode==='ORIGINAL_LEGS'&&h.reservation_projection_id===null)return false;
  if(['CANCELLING','CANCELLED'].includes(h.reservation_mode)&&h.remaining_reservation_cash!=='0.00')return false;
  if(h.reservation_internal_zero&&(h.reservation_mode!=='ORIGINAL_OBLIGATION'||h.remaining_reservation_cash!=='0.00'))return false;
  if(row===null)return true;
  const s=row.reservation_state,c=row.remaining_reservation_cash;
  const nativeStatuses={RESERVED:['MEMBERS_READY','DRAFT','SCHEDULED','REFUSED'],ISSUED_AWAITING_RECEIPT:['ISSUED_CSV'],EXECUTION_UNCERTAIN:['SUBMITTED','UNKNOWN'],EXECUTED_PENDING_POSTING:['SETTLED'],POSTED:['SETTLED'],RETURNED:['RETURNED'],INTERNAL_RESERVED:['MEMBERS_READY','DRAFT','SCHEDULED','REFUSED'],INTERNAL_PENDING_POSTING:['INTERNAL_PROCESSING','INTERNAL_SETTLED'],CLOSED_INTERNAL:['INTERNAL_SETTLED']};
  if(Object.hasOwn(nativeStatuses,s)&&!nativeStatuses[s].includes(row.status))return false;
  if(['RESERVED','ISSUED_AWAITING_RECEIPT'].includes(s)&&row.reservation_holds_state!=='ACTIVE')return false;
  if(s==='EXECUTION_UNCERTAIN'&&h.reservation_mode!=='ORIGINAL_LEGS')return false;
  if(['CANCELLING','CANCELLED'].includes(s)&&s!==h.reservation_mode)return false;
  if((row.original_transfer_id!==null)!==(s==='REISSUE_EXCLUDED'))return false;
  if(['EVIDENCE_CONFLICT','SUPPRESSED_UNCONFIRMED','PENDING','INCLUDED_IN_ORIGINAL_OBLIGATION','EXECUTION_UNCERTAIN'].includes(s)){
    if(c!==null)return false;
    if(s==='INCLUDED_IN_ORIGINAL_OBLIGATION'&&h.reservation_mode!=='ORIGINAL_OBLIGATION')return false;
  }else if(['RESERVED','ISSUED_AWAITING_RECEIPT'].includes(s)){
    if(c!==row.cash_amount||row.execution_kind!=='BANK'||h.reservation_mode!=='ORIGINAL_LEGS')return false;
  }else if(['REISSUE_EXCLUDED','CANCELLING','CANCELLED','EXECUTED_PENDING_POSTING','POSTED','RETURNED','INTERNAL_RESERVED','INTERNAL_PENDING_POSTING','CLOSED_INTERNAL'].includes(s)){
    if(c!=='0.00')return false;
    if(s.startsWith('INTERNAL_')||s==='CLOSED_INTERNAL'){
      if(row.execution_kind!=='INTERNAL_ZERO'||row.cash_amount!=='0.00')return false;
    }
  }else return false;
  return true;
}

function validNetReplacement(r){
 const keys=['replacement_stage','replacement_request_no','replacement_expected_count','replacement_staged_count','replacement_validated_count','replacement_committed_count'];
 if(r.replacement_id===null)return keys.every(k=>r[k]===null)&&r.replacement_cursor===null&&r.net_refused_code===null&&r.net_replacement_pending_id===null;
 if(keys.some(k=>r[k]===null)||r.replacement_request_no!==r.net_request_high_water||r.net_replacement_pending_id!==null&&r.net_replacement_pending_id!==r.replacement_id)return false;
 if(['PRECHECK','STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING'].includes(r.replacement_stage)&&r.net_replacement_pending_id!==r.replacement_id)return false;
 if(['COMPLETE','REFUSED'].includes(r.replacement_stage)&&r.net_replacement_pending_id!==null)return false;
 const e=BigInt(r.replacement_expected_count),s=BigInt(r.replacement_staged_count),v=BigInt(r.replacement_validated_count),c=BigInt(r.replacement_committed_count);
 if(c>v||v>s||s>e||(r.replacement_stage==='REFUSED')!==(r.net_refused_code!==null))return false;
 if(r.replacement_stage==='COMPLETE'&&c!==e)return false;
 if(r.replacement_stage==='REFUSED'&&c!==0n)return false;
 return true;
}
function field(v,t){
 if(t==='netReplacementStage')return ['PRECHECK','STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING','COMPLETE','REFUSED','CANCELLED'].includes(v);
 if(t==='netRefusal')return ['CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND'].includes(v);
    if(t.startsWith('channelDraft'))return t.endsWith('?')?v===null||channelDraftField(v,t.slice(0,-1),field,exact):channelDraftField(v,t,field,exact);
 if(t.startsWith('worksheet')&&!t.endsWith('?'))return worksheetField(v,t,field,exact);
 if(t.startsWith('active')&&!t.endsWith('?'))return activePreparationField(v,t,field,exact);
 if(t==='reviewCounts')return exact(v,{selected:'integer',excluded:'integer',scope_count:'integer'});
 if(["reviewLimitNote","reviewAmount","reviewMoney","reviewRun","reviewSelection","reviewWorker","reviewCaseSort","reviewNetRefusal","reviewDirection","reviewStage","reviewBasis","reviewForeign","reviewContext","reviewKey","reviewCursor","reviewCases","reviewTotal","reviewChannels"].includes(t))return reviewField(v,t,field,exact);
  if(t==='prepareIssueRows')return Array.isArray(v)&&v.length<=100&&v.every(r=>exact(r,PREPARE_ISSUE_ROW));
  if(t==='prepareIssueCounts')return exact(v,{skipped_timesheets:'integer',removed_candidates:'integer',scope_count:'integer'});
  if(t.startsWith('prepareIssue')&&!t.endsWith('?'))return prepareIssueField(v,t,field,exact);
  if(t==='casePauseState')return casePauseState(v);
  if(t==='casePauseReceipt')return casePauseReceipt(v);
  if(t==='channelInput')return caseChannelInput(v);
  if(t==='channelCursor')return caseChannelCursor(v);
  if(t==='channelReceipt')return caseChannelReceipt(v);
  if(t==='restructureInput')return caseRestructureInput(v);
  if(t==='restructureReceipt')return caseRestructureReceipt(v);
  if(t==='caseEditState')return caseEditState(v);
  if(t==='caseEditReceipt')return caseEditReceipt(v);
  if(t==='caseReference')return typeof v==='string'&&v.trim().length>0&&!v.includes('\0')&&ENCODER.encode(v).byteLength<=2048;
  if(t==='guardDecision')return ['UMBRELLA_ONLY','VERIFY_BOTH','VERIFY_PAYE'].includes(v);
  if(t==='guardChannel')return ['ALL','PAYE','UMBRELLA'].includes(v);
  if(t==='guardReason')return typeof v==='string'&&v.trim().length>0&&!v.includes('\0')&&ENCODER.encode(v).byteLength<=500;
  if(t==='guardContext')return exact(v,{namespace:'choiceNamespace',workbench_revision:'integer',adoption_revision:'integer'});
  if(t==='guardRows')return Array.isArray(v)&&v.length===1&&exact(v[0],{run_id:'uuid',guard_applies:'boolean',unexecuted:'boolean',existing:'guardFacts',paye_candidates:'integer',umbrella_candidates:'integer',context:'guardContext',decision:'guardStoredDecision',channel_scope:'guardChannel'});
  if(t==='guardFacts')return Array.isArray(v)&&v.length<=100&&v.every(x=>exact(x,{run_id:'uuid',reference:'label?',legacy:'boolean',unexecuted:'boolean',status:'guardStatus'})
    &&(x.reference===null||x.reference.length<=80&&!UUID.test(x.reference))&&(x.status==='Paid')===!x.unexecuted)
    &&new Set(v.map(x=>x.run_id)).size===v.length;
  if(t==='guardStatus')return ['Paid','Not paid yet'].includes(v);
  if(t==='guardStoredDecision')return v==='NONE'||field(v,'guardDecision');
  if(t==='discardReason')return typeof v==='string'&&v.trim().length>0&&!v.includes('\0')&&ENCODER.encode(v).byteLength<=500;
  if(t==='discardReceiptPhase')return ['EMPTY','CANCELLING','DISCARDED','PARTIAL'].includes(v);
  if(t==='discardPhase')return ['CANCELLING','DISCARDED','PARTIAL'].includes(v);
  if(t==='discardOutcome')return ['REMOVED','ALREADY_REMOVED','BLOCKED'].includes(v);
  if(t==='discardBlockedCode')return ['PAYMENT_ALREADY_PROTECTED','PREPARATION_STATE_PROTECTED'].includes(v);
  if(t==='choiceMember')return ['WORK','CASE_COMPONENT'].includes(v);
    if(t==='emptyIds')return Array.isArray(v)&&v.length<=100&&v.every(x=>field(x,'uuid'))&&new Set(v.map(x=>x.toLowerCase())).size===v.length;
  if(t==='null')return v===null;
  if(t==='unit')return typeof v==='string'&&!v.includes('\0')&&ENCODER.encode(v).byteLength<=512;
  if(t==='nativeRateCode')return typeof v==='string'&&!v.includes('\0')&&[...v].length>=1&&[...v].length<=245;
  if(t==='rateFamily')return ['STANDARD','ADDITIONAL','MILEAGE'].includes(v);
  if(t==='standardSchedule')return Array.isArray(v)&&v.length<=5&&v.every(x=>exact(x,{rate_schedule_id:'uuid',bucket:'bucket',available_paye_rate:'money?',available_umbrella_rate:'money?',available_charge_rate:'money?',approved_charge_rate:'money?'}))&&new Set(v.map(x=>x.bucket)).size===v.length&&new Set(v.map(x=>x.rate_schedule_id.toLowerCase())).size===v.length;

  if(t==='choiceNamespace')return exact(v,{agency_id:'uuid',owner_epoch:'positive',actor_user_id:'uuid',official_pay_date:'date',scope:'choiceScope'});
  if(t==='choiceScope')return exact(v,{candidate_id:'uuid?',client_id:'uuid?',channel:'scopeChannel'});
  if(t==='choiceIntent'){
    if(!record(v)||Object.keys(v).sort().join()!=='candidates,cases,components,mode,works'||!field(v.mode,'choiceMode'))return false;
    const schemas={candidates:{candidate_id:'uuid',selected:'boolean'},works:{candidate_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',selected:'boolean',mode:'mode'},components:{candidate_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',approved_line_id:'uuid',selected:'boolean'},cases:{candidate_id:'uuid',case_component_id:'uuid',selected:'boolean'}};
    return Object.entries(schemas).every(([kind,schema])=>Array.isArray(v[kind])&&v[kind].length<=1000&&v[kind].every(row=>exact(row,schema))&&new Set(v[kind].map(row=>(row.case_component_id??row.approved_line_id??row.work_id??row.candidate_id).toLowerCase())).size===v[kind].length);
  }
  if(t==='descriptor')return validChoiceDescriptor(v);
  if(t==='scopeChannel')return ['ALL','PAYE','UMBRELLA'].includes(v);
  if(t==='localRevision')return Number.isSafeInteger(v)&&v>=0;
  if(t==='choiceMode')return ['ALL_MATCHING','NONE'].includes(v);
  if(t==='skipCode')return ['BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE','BPAY_NEXT_SELECTION_ON_HOLD','BPAY_NEXT_SELECTION_SNOOZED','BPAY_NEXT_SELECTION_WORK_NOT_APPROVED','BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT','BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE','PAYE_EXCLUDED_BY_SAME_WEEK_GUARD','SOURCE_PAY_QUERY_OPEN','SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE'].includes(v);
  if(t==='skipped')return Array.isArray(v)&&v.length<=100&&v.every(x=>exact(x,{work_id:'uuid',code:'skipCode'}))
    &&new Set(v.map(x=>x.work_id.toLowerCase())).size===v.length;
  const reservationEnums={"reservationMode":["UNCONFIRMED","PENDING_PREPARATION","PENDING_NET","ORIGINAL_OBLIGATION","ORIGINAL_LEGS","CANCELLING","CANCELLED","EVIDENCE_CONFLICT"],"reservationState":["EVIDENCE_CONFLICT","SUPPRESSED_UNCONFIRMED","PENDING","INCLUDED_IN_ORIGINAL_OBLIGATION","PAGED_ORIGINAL_LEGS","REISSUE_EXCLUDED","CANCELLING","CANCELLED","RESERVED","ISSUED_AWAITING_RECEIPT","EXECUTION_UNCERTAIN","EXECUTED_PENDING_POSTING","POSTED","RETURNED","INTERNAL_RESERVED","INTERNAL_PENDING_POSTING","CLOSED_INTERNAL"],"reservationHolds":["ACTIVE","NONE","RELEASING","POSTING"]};
  if(Object.hasOwn(reservationEnums,t))return reservationEnums[t].includes(v);
  if(t.endsWith('?'))return v===null||field(v,t.slice(0,-1));
  if(t==='sha')return typeof v==='string'&&/^[0-9a-f]{64}$/.test(v);
  const acceptedEnums={acceptedCaseScope:['NO_CASE','SEALED_SELECTION'],settingsWitness:['BASELINE_AT_ACCEPTANCE','ORDERED_VERSION'],
    prepareJobStatus:['READY','LEASED','BLOCKED','DONE','FAILED'],prepareJobPhase:['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','READY','DONE','EXPIRED'],
    acceptedProgress:['WAITING_ENROLLMENT','WAITING_EARLIER_COMMAND','PREPARING','READY','REVIEW','CANCELLING','CANCELLED','FAILED','EPOCH_PAUSED'],
    acceptedProgressCode:['ENROLLMENT_PENDING','EARLIER_COMMAND_PENDING','CUT_PENDING','PREPARATION_RUNNING','REVIEW_REQUIRED','CANCELLATION_PENDING','PREPARATION_EXPIRED','OWNER_EPOCH_STALE','JOB_FAILED'],
    acceptedSourceKind:['ORDINARY','SOURCE','PROTECTED','EXPENSE','ADJUSTMENT'],acceptedQuery:['CLEAR'],acceptedCutState:['WAITING_PREDECESSOR','WAITING_PIN','SEALED'],
    prepareCommandStatus:['RECEIVED','ENROLLING','SEALED','COMPLETE','FAILED']};
  if(Object.hasOwn(acceptedEnums,t))return acceptedEnums[t].includes(v);
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
  if(t==='drafts')return Array.isArray(v)&&v.length>=1&&v.length<=2
    &&v.every(x=>exact(x,{run_id:'uuid',pay_channel:'channel'}))
    &&(v.length<2||(v[0].pay_channel==='PAYE'&&v[1].pay_channel==='UMBRELLA'));
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
  if(t==='workerAction')return ['REPREPARE','RELEASE'].includes(v);
  if(t==='workerActionPhase')return ['ACCEPTED','RUNNING','COMPLETE','EXPIRED','DISCARDED'].includes(v);
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
  if(t==='segmentKeys')return Array.isArray(v)&&v.length>=1&&v.length<=100&&v.every(x=>typeof x==='string'&&!x.includes('\0')&&x.trim()===x&&[...x].length>=1&&[...x].length<=245)&&new Set(v).size===v.length;
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
function validChoiceDescriptor(v){
  const schema={namespace:'choiceNamespace',intent:'choiceIntent',revision:'localRevision',workbench_revision:'integer',adoption_revision:'integer',...(record(v)&&Object.hasOwn(v,'memory_baseline_run_id')?{memory_baseline_run_id:'uuid?'}:{})};
  if(!exact(v,schema)||ENCODER.encode(JSON.stringify(v)).byteLength>32768)return false;
  return true;
}
function commandCall(body){
  if(!record(body)||!Object.hasOwn(BPAY_NEXT_PREPARATION_INPUTS,body.action))throw fault('REQUEST_INVALID',400);
  const {action,...raw}=body;
  const token=action==='PAYE_GUARD_DECISION'&&Object.hasOwn(raw,'reauth_token')?raw.reauth_token:null;
  const args=action==='PAYE_GUARD_DECISION'?Object.fromEntries(Object.entries(raw).filter(([k])=>k!=='reauth_token')):raw;
  if(action==='PAYE_GUARD_DECISION'&&((args.decision==='UMBRELLA_ONLY'&&(token!==null||args.reason!==null))
    ||(args.decision!=='UMBRELLA_ONLY'&&(typeof token!=='string'||!token||ENCODER.encode(token).byteLength>1536))))throw fault('REQUEST_INVALID',400);
  const schema=BPAY_NEXT_PREPARATION_INPUTS[action];
  const checkedSchema=action==='PREPARATION_DISCARD'&&Object.hasOwn(args,'reason')?{...schema,reason:'discardReason'}:schema;
  if(!exact(args,checkedSchema)||(action==='EXCLUSION_PAGE'&&args.excluded_work_ids.length+args.excluded_candidate_ids.length>100)||(action==='CASE_PAGE'&&args.component_ids.length!==args.selected.length))throw fault('REQUEST_INVALID',400);
  return {action,token,args:action==='SEGMENT_PAGE'?Object.freeze({...normalized(args),segment_keys:Object.freeze([...args.segment_keys])}):normalized(args)};
}
const CASE_LIST_FILTERS={candidate_id:'uuid?',client_id:'uuid?',case_subtype:'caseListSubtype',view:'caseListView',snooze:'caseListSnooze',hide_completed:'boolean'};
const CASE_LIST_DEFAULT_FILTERS=Object.freeze({candidate_id:null,client_id:null,case_subtype:'ALL',view:'ALL',snooze:'ALL',hide_completed:true});
const CASE_LIST_CONTEXT={agency_id:'uuid',actor_user_id:'uuid',owner_epoch:'positive',local_date:'date',official_pay_date:'date',pay_week_start:'date',workbench_revision:'integer',adoption_revision:'integer',valuation_policy_id:'uuid?'};
const CASE_LIST_LAST={case_id:'uuid',created_at_utc:'timestamp?',sort_value:'label?'};
const CASE_LIST_CURSOR_LAST={case_id:'uuid',created_at_utc:'timestamp?',sort_value:'caseListEncodedSort?'};
const CASE_LIST_COUNTS=['payment_advances_active_count','overpayments_active_count','underpayments_active_count',
  'manual_debt_adjustments_active_count','manual_credit_adjustments_count','mixed_finance_cases_count',
  'unresolved_finance_cases_count','stale_finance_cases_count','finance_cases_with_active_snooze_count'];
const CASE_LIST_ROW={case_id:'uuid',candidate_id:'uuid',candidate_name:'label',candidate_reference:'label?',
  client_id:'uuid?',client_name:'label?',client_association:'caseListClient',case_kind:'issue',case_subtype:'issue?',case_type_label:'label',original_liability_reference:'caseReference?',
  tax_treatment:'issue',tax_label:'label',status:'caseListStatus',created_at_utc:'timestamp?',case_revision:'positive',
  principal_approved:'money',principal_funded:'money',principal_recovered:'money',principal_written_off:'money',
  outstanding:'money',pending_payout:'money?',amount_due:'money?',recoverable_this_run:'null',active_hold_amount:'money',
  active_hold_explanation:'label?',protected_run_id:'uuid?',protected_channel:'channel?',input_pending:'boolean',
  due_date:'date?',weekly_due:'money?',schedule_start_monday:'date?',next_due_monday:'date?',schedule_week_count:'positive?',
  minimum_earnings_threshold:'money?',take_home_floor:'money?',has_active_snooze:'boolean?',active_snooze_count:'integer?',
  indefinite_snooze_count:'integer?',snooze_until_date:'date?',stale_snooze_count:'integer?',is_mixed_case:'boolean?',
  unresolved_taxable:'boolean?',resolution_stale:'boolean?',gap_code:'issue?',sort_key:'caseListLast'};
function caseListField(value,type){
  if(type.endsWith('?'))return value===null||caseListField(value,type.slice(0,-1));
  if(type==='caseListEncodedSort'){
    if(typeof value!=='string'||value.length>11000||!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value))return false;
    try{const raw=atob(value),decoded=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(Uint8Array.from(raw,c=>c.charCodeAt(0)));
      return btoa(raw)===value&&!decoded.includes('\0')&&ENCODER.encode(decoded).byteLength<=8192;
    }catch{return false;}
  }
  const enums={caseListSubtype:['ALL','LOAN','PAYMENT_ADVANCE','OVERPAYMENT','UNDERPAYMENT','MANUAL_CREDIT','MANUAL_DEBT','UNMAPPED'],
    caseListView:['ALL','ACTIVE','SNOOZED','HISTORY'],caseListSnooze:['ALL','FINITE','INDEFINITE'],
    caseListSort:['CREATED_ASC','CREATED_DESC','CANDIDATE_ASC','CANDIDATE_DESC','TYPE_ASC','TYPE_DESC','STATUS_ASC','STATUS_DESC','ORIGINAL_ASC','ORIGINAL_DESC','OUTSTANDING_ASC','OUTSTANDING_DESC','WEEKLY_ASC','WEEKLY_DESC','FUNDED_ASC','FUNDED_DESC','RECOVERED_ASC','RECOVERED_DESC','WRITTEN_OFF_ASC','WRITTEN_OFF_DESC'],
    caseListStatus:['OPEN','PAUSED','CLOSED','WRITTEN_OFF'],caseListClient:['FROZEN','UNASSOCIATED','UNKNOWN']};
  if(Object.hasOwn(enums,type))return enums[type].includes(value);
  if(type==='caseListLast')return caseListExact(value,CASE_LIST_LAST);
  return field(value,type);
}
function caseListExact(value,schema){
  return record(value)&&Object.keys(value).length===Object.keys(schema).length
    &&Object.entries(schema).every(([key,type])=>Object.hasOwn(value,key)&&caseListField(value[key],type));
}
function caseListSame(a,b){
  if(a===b)return true;
  if(!record(a)||!record(b)||Object.keys(a).length!==Object.keys(b).length)return false;
  return Object.keys(a).every(key=>Object.hasOwn(b,key)&&caseListSame(a[key],b[key]));
}
function caseListCursor(cursor,kind,filters,sort){
  if(!record(cursor)||Object.keys(cursor).sort().join()!=='context,filters,last,sort'
    ||!caseListExact(cursor.context,CASE_LIST_CONTEXT)||!caseListExact(cursor.filters,CASE_LIST_FILTERS)
    ||!caseListSame(cursor.filters,filters)||cursor.sort!==(kind==='CASE_SUMMARY'?'CASE_ID_ASC':sort)
    ||!caseListExact(cursor.last,CASE_LIST_CURSOR_LAST)||ENCODER.encode(JSON.stringify(cursor)).byteLength>16384)return false;
  return kind==='CASE_SUMMARY'?cursor.last.created_at_utc===null&&cursor.last.sort_value===null:
    sort.startsWith('CREATED_')?cursor.last.sort_value===null:cursor.last.sort_value!==null;
}
function caseListCall(kind,params){
  const bad=()=>{throw fault('REQUEST_INVALID',400);};
  const parse=(key,fallback)=>{const raw=params.get(key);if(raw===null)return fallback;
    if(ENCODER.encode(raw).byteLength>32768)bad();try{return JSON.parse(raw);}catch{bad();}};
  const supplied=parse('filters',{});
  if(!record(supplied)||Object.keys(supplied).some(key=>!Object.hasOwn(CASE_LIST_FILTERS,key)))bad();
  const filters={...CASE_LIST_DEFAULT_FILTERS,...supplied};
  if(!caseListExact(filters,CASE_LIST_FILTERS))bad();
  const sort=params.get('sort')??'CREATED_DESC';
  const after=parse('after',null);
  if(kind==='CASE_LIST'&&!caseListField(sort,'caseListSort'))bad();
  if(after!==null&&!caseListCursor(after,kind,filters,sort))bad();
  const args={filters:normalized(filters),after:normalized(after)};
  if(kind==='CASE_LIST'){
    const raw=params.get('limit')??'10';if(!['5','10','20','50'].includes(raw))bad();
    args.sort=sort;args.limit=Number(raw);
  }
  if(ENCODER.encode(JSON.stringify(args)).byteLength>32768)bad();
  return {kind,args:Object.freeze(args)};
}
// PostgreSQL C collation compares UTF-8 bytes. JS UTF-16 order and millisecond
// timestamps would lose valid server tie-breaks.
function caseListTextCompare(a,b){
  const x=ENCODER.encode(a),y=ENCODER.encode(b);
  for(let i=0;i<Math.min(x.length,y.length);i++)if(x[i]!==y[i])return x[i]<y[i]?-1:1;
  return x.length===y.length?0:x.length<y.length?-1:1;
}
function caseListTime(value){
  const fraction=value.match(/\.(\d{1,6})/)?.[1]??'';
  return BigInt(Date.parse(value))*1000n+BigInt(fraction.padEnd(6,'0').slice(3));
}
function caseListCompare(a,b,sort){
  if(sort==='CASE_ID_ASC')return caseListTextCompare(a.case_id,b.case_id);
  if(!sort.startsWith('CREATED_')){
    let key;
    if(/^(ORIGINAL|OUTSTANDING|WEEKLY|FUNDED|RECOVERED|WRITTEN_OFF)_/.test(sort)){
      if((a.sort_value==='~')!==(b.sort_value==='~'))return a.sort_value==='~'?1:-1;
      const scaled=value=>{if(value==='~')return 0n;if(!/^-?\d{1,24}(?:\.\d{1,6})?$/.test(value))throw fault('RESPONSE_INVALID',502);
        const [whole,part='']=value.replace(/^-/,'').split('.');return BigInt(whole+part.padEnd(6,'0'))*(value.startsWith('-')?-1n:1n);};
      const x=scaled(a.sort_value),y=scaled(b.sort_value);key=x===y?0:x<y?-1:1;
    }else key=caseListTextCompare(a.sort_value,b.sort_value);
    if(key)return sort.endsWith('_DESC')?-key:key;
  }
  if((a.created_at_utc===null)!==(b.created_at_utc===null))return a.created_at_utc===null?1:-1;
  if(a.created_at_utc!==null){
    const x=caseListTime(a.created_at_utc),y=caseListTime(b.created_at_utc);
    if(x!==y){const direction=sort==='CREATED_ASC'?1:-1;return x<y?-direction:direction;}
  }
  return caseListTextCompare(a.case_id,b.case_id);
}
function caseListDecodedLast(cursor){
  if(cursor===null)return null;
  const raw=cursor.last.sort_value;
  return {...cursor.last,sort_value:raw===null?null:new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(Uint8Array.from(atob(raw),c=>c.charCodeAt(0)))};
}
function caseListResult(payload,call,bad){
  const summary=call.kind==='CASE_SUMMARY',limit=summary?100:call.args.limit,sort=summary?'CASE_ID_ASC':call.args.sort;
  const shape=summary?['context','filters','counts','scanned_count','matched_count','gap_case_count','complete','next_cursor']:
    ['context','filters','sort','rows','scanned_count','complete','next_cursor'];
  if(!record(payload)||Object.keys(payload).length!==shape.length||shape.some(key=>!Object.hasOwn(payload,key))
    ||!caseListExact(payload.context,CASE_LIST_CONTEXT)||payload.context.actor_user_id!==call.actorUserId
    ||!caseListSame(payload.filters,call.args.filters)||!field(payload.scanned_count,'integer')
    ||BigInt(payload.scanned_count)>BigInt(limit)||typeof payload.complete!=='boolean'
    ||call.args.after!==null&&!caseListSame(call.args.after.context,payload.context))bad();
  const next=payload.next_cursor;
  if(payload.complete?next!==null:!caseListCursor(next,call.kind,call.args.filters,call.args.sort)
    ||!caseListSame(next.context,payload.context)||payload.scanned_count==='0'
    ||call.args.after!==null&&caseListCompare(caseListDecodedLast(next),caseListDecodedLast(call.args.after),sort)<=0)bad();
  if(summary){
    const counts=Object.fromEntries(CASE_LIST_COUNTS.map((key,index)=>[key,index>=5?'integer?':'integer']));
    if(!caseListExact(payload.counts,counts)||!field(payload.matched_count,'integer')||!field(payload.gap_case_count,'integer')
      ||BigInt(payload.matched_count)>BigInt(payload.scanned_count)||BigInt(payload.gap_case_count)>BigInt(payload.matched_count)
      ||Object.values(payload.counts).some(value=>value===null)&&payload.gap_case_count==='0'
      ||Object.values(payload.counts).some(value=>value!==null&&BigInt(value)>BigInt(payload.matched_count))
      ||CASE_LIST_COUNTS.slice(0,5).reduce((total,key)=>total+BigInt(payload.counts[key]),0n)>BigInt(payload.matched_count))bad();
  }else{
    if(payload.sort!==sort||!Array.isArray(payload.rows)||payload.rows.length>limit
      ||BigInt(payload.rows.length)>BigInt(payload.scanned_count))bad();
    let previous=caseListDecodedLast(call.args.after);
    for(const row of payload.rows){
      if(!caseListExact(row,CASE_LIST_ROW)||ENCODER.encode(JSON.stringify(row)).byteLength>90000
        ||row.original_liability_reference!==null&&(row.case_kind!=='MANUAL_DEBT'||row.case_subtype!=='MANUAL_DEBT')
        ||row.sort_key.case_id!==row.case_id||row.sort_key.created_at_utc!==row.created_at_utc
        ||sort.startsWith('CREATED_')&&row.sort_key.sort_value!==null
        ||sort.startsWith('TYPE_')&&row.sort_key.sort_value!==row.case_type_label
        ||sort.startsWith('STATUS_')&&row.sort_key.sort_value!==row.status
        ||/^(ORIGINAL|OUTSTANDING|WEEKLY|FUNDED|RECOVERED|WRITTEN_OFF)_/.test(sort)&&caseListCompare({...row.sort_key,case_id:row.case_id},
          {...row.sort_key,sort_value:(sort.startsWith('ORIGINAL_')?row.principal_approved:sort.startsWith('OUTSTANDING_')?row.outstanding:sort.startsWith('FUNDED_')?row.principal_funded:sort.startsWith('RECOVERED_')?row.principal_recovered:sort.startsWith('WRITTEN_OFF_')?row.principal_written_off:row.weekly_due)??'~'},sort)!==0
        ||sort.startsWith('CANDIDATE_')&&row.sort_key.sort_value===null
        ||previous!==null&&caseListCompare(row.sort_key,previous,sort)<=0
        ||next!==null&&caseListCompare(row.sort_key,caseListDecodedLast(next),sort)>0
        ||(row.client_association==='FROZEN')!==(row.client_id!==null)
        ||(row.protected_run_id===null)!==(row.protected_channel===null)
        ||row.input_pending&&row.protected_run_id===null
        ||[row.resolution_stale,row.has_active_snooze,row.is_mixed_case,row.unresolved_taxable,row.pending_payout].some(value=>value===null)&&row.gap_code===null)bad();
      const membership=[row.pending_payout,row.is_mixed_case,row.unresolved_taxable,row.has_active_snooze];
      if(membership.some(value=>value===null)&&(!membership.every(value=>value===null)
        ||row.amount_due!==null||row.resolution_stale!==null||row.gap_code===null))bad();
      if(row.has_active_snooze===null){
        if([row.active_snooze_count,row.indefinite_snooze_count,row.stale_snooze_count,row.snooze_until_date].some(value=>value!==null)||row.gap_code===null)bad();
      }else if([row.active_snooze_count,row.indefinite_snooze_count,row.stale_snooze_count].some(value=>value===null)
        ||row.has_active_snooze!==(row.active_snooze_count!=='0')
        ||BigInt(row.indefinite_snooze_count)>BigInt(row.active_snooze_count)
        ||(BigInt(row.active_snooze_count)>BigInt(row.indefinite_snooze_count))!==(row.snooze_until_date!==null))bad();
      const f=call.args.filters,subtype=row.case_subtype,known=['LOAN','PAYMENT_ADVANCE','OVERPAYMENT','UNDERPAYMENT','MANUAL_CREDIT','MANUAL_DEBT'].includes(subtype);
      if(f.candidate_id!==null&&row.candidate_id!==f.candidate_id||f.client_id!==null&&row.client_id!==f.client_id
        ||f.case_subtype!=='ALL'&&!(f.case_subtype===subtype||f.case_subtype==='PAYMENT_ADVANCE'&&['LOAN','PAYMENT_ADVANCE'].includes(subtype)||f.case_subtype==='UNMAPPED'&&!known)
        ||!known&&(row.gap_code===null||row.tax_label!=='Tax treatment unavailable')
        ||f.hide_completed&&['CLOSED','WRITTEN_OFF'].includes(row.status)
        ||f.view==='ACTIVE'&&(!['OPEN','PAUSED'].includes(row.status)||row.has_active_snooze!==false)||f.view==='HISTORY'&&!['CLOSED','WRITTEN_OFF'].includes(row.status)
        ||f.view==='SNOOZED'&&row.has_active_snooze!==true||f.snooze==='FINITE'&&(row.active_snooze_count===null||BigInt(row.active_snooze_count)<=BigInt(row.indefinite_snooze_count))
        ||f.snooze==='INDEFINITE'&&(row.indefinite_snooze_count===null||row.indefinite_snooze_count==='0'))bad();
      previous=row.sort_key;
    }
  }
  return payload;
}
function pageCall(request){
  const url=new URL(request.url);
  if(ENCODER.encode(url.search).byteLength>(url.searchParams.get('kind')==='GUARD'?128*1024:BPAY_NEXT_PREPARATION_LIMITS.requestBytes))throw fault('REQUEST_TOO_LARGE',413);
  const kind=url.searchParams.get('kind');
  if(['CASE_AUDIT','CANDIDATE_CASE_REPORT'].includes(kind)){try{return caseFinanceQuery(kind,url.searchParams);}catch{throw fault('REQUEST_INVALID',400);}}
  const schema=kind==='LINES'&&url.searchParams.has('worksheet_run_id')?{...BPAY_NEXT_PREPARATION_PAGE_INPUTS.LINES,worksheet_run_id:'uuid'}:kind==='WORKERS'&&url.searchParams.has('attention_only')?{...BPAY_NEXT_PREPARATION_PAGE_INPUTS.WORKERS,attention_only:'true'}:kind==='CASE_PREVIEW'&&(url.searchParams.has('case_id')||url.searchParams.has('expected_case_revision'))?{input:'caseInput',case_id:'uuid',expected_case_revision:'positive'}:kind==='CASE_COMPONENTS'?{...BPAY_NEXT_PREPARATION_PAGE_INPUTS.CASE_COMPONENTS,...(url.searchParams.has('edit_command_id')?{edit_command_id:'uuid'}:{}),...(url.searchParams.has('pause_command_id')?{pause_command_id:'uuid'}:{}),...(url.searchParams.has('restructure_command_id')?{restructure_command_id:'uuid'}:{}),...(url.searchParams.has('channel_command_id')?{channel_command_id:'uuid'}:{})}:kind==='GUARD'&&url.searchParams.has('descriptor')?{run_id:'uuid',descriptor:'descriptor'}:kind==='WORK_CHOICES'&&url.searchParams.has('accepted_inputs')?{run_id:'uuid',accepted_inputs:'true',after_member_no:'positive?',limit:'limit',...(url.searchParams.has('member_kind')?{member_kind:'choiceMember'}:{})}:BPAY_NEXT_PREPARATION_PAGE_INPUTS[kind];
  if(!schema||[...url.searchParams.keys()].some(k=>(k!=='kind'&&!Object.hasOwn(schema,k))||url.searchParams.getAll(k).length!==1))throw fault('REQUEST_INVALID',400);
  if(kind==='CASE_LIST'||kind==='CASE_SUMMARY')return caseListCall(kind,url.searchParams);
  const args={};
  for(const [key,type]of Object.entries(schema)){
    let v=url.searchParams.get(key);if(key==='descriptor'||(kind==='CASE_CHANNEL_SUGGEST'&&key==='after'&&v!==null)||(key==='input'&&!(['CASE_RESTRUCTURE_PREVIEW','CASE_CHANNEL_SUGGEST'].includes(kind)&&v===null))||['PREPARE_ISSUES','REVIEW_CASES','ACTIVE_PREPARATIONS','PAYE_WORKSHEET','CHANNEL_DRAFT_LIST','CHANNEL_DRAFT_COUNT'].includes(kind)&&['after','context'].includes(key)&&v!==null){if(v===null||ENCODER.encode(v).byteLength>32768)throw fault('REQUEST_INVALID',400);try{v=JSON.parse(v);}catch{throw fault('REQUEST_INVALID',400);}}if(key==='waiting_only'){if(!['true','false'].includes(v))throw fault('REQUEST_INVALID',400);v=v==='true';}if(key==='accepted_inputs'||key==='attention_only'){if(v!=='true')throw fault('REQUEST_INVALID',400);v=true;}
    if(key==='limit'){if(v!==null&&!/^[1-9]\d{0,2}$/.test(v))throw fault('REQUEST_INVALID',400);v=v===null?50:Number(v);}
    if(!(type==='caseInput'?caseInput(v):field(v,type)))throw fault('REQUEST_INVALID',400);args[key]=v;
  }
  if(kind==='CURRENT_WORK'&&(args.after_week===null)!==(args.after_work_id===null))throw fault('REQUEST_INVALID',400);
  if(kind==='PAYE_WORKSHEET')return {kind,args:Object.freeze({...args,run_id:args.run_id.toLowerCase(),candidate_id:args.candidate_id?.toLowerCase()??null,after:args.after===null?null:{context:{...args.after.context,run_id:args.after.context.run_id.toLowerCase(),actor_user_id:args.after.context.actor_user_id.toLowerCase(),candidate_id:args.after.context.candidate_id?.toLowerCase()??null},candidate_id:args.after.candidate_id.toLowerCase(),run_worker_id:args.after.run_worker_id.toLowerCase()}})};
  return {kind,args:['CASE_PREVIEW','CASE_RESTRUCTURE_PREVIEW','CASE_CHANNEL_SUGGEST'].includes(kind)?Object.freeze(args):normalized(args)};
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
  if(!mutation&&call.kind==='CASE_CHANNEL_SUGGEST'){if(!caseChannelPreview(payload,call.args,call.actorUserId))bad();return payload;}
  if(!mutation&&call.kind==='CASE_RESTRUCTURE_PREVIEW'){if(!caseRestructurePreview(payload,call.args,call.actorUserId))bad();return payload;}
  if(!mutation&&['CASE_AUDIT','CANDIDATE_CASE_REPORT'].includes(call.kind)){try{return caseFinanceResult(payload,call.kind,call.args,call.actorUserId);}catch{bad();}}
  if(!mutation&&['REVIEW_TOTALS','REVIEW_CASES'].includes(call.kind))return reviewResult(payload,call.kind,call.args,call.actorUserId,field,exact,bad);
  if(!mutation&&call.kind==='LINES'&&Object.hasOwn(call.args,'worksheet_run_id'))return worksheetLinesResult(payload,call.args,exact,bad);
  if(!mutation&&call.kind==='PAYE_WORKSHEET')return worksheetResult(payload,call.args,call.actorUserId,field,exact,bad);
  if(!mutation&&['CHANNEL_DRAFT_LIST','CHANNEL_DRAFT_COUNT'].includes(call.kind))return channelDraftResult(payload,call.kind,call.args,call.actorUserId,exact,bad);
  if(!mutation&&call.kind==='ACTIVE_PREPARATIONS')return activePreparationsResult(payload,call.args,call.actorUserId,field,exact,bad);
  if(!mutation&&call.kind==='PREPARE_ISSUES')return prepareIssuesResult(payload,call.args,call.actorUserId,field,exact,bad);
  if(!mutation&&(call.kind==='CASE_LIST'||call.kind==='CASE_SUMMARY'))return caseListResult(payload,call,bad);
  if(mutation){
    const schema=OUTPUTS[call.action];
    if(!exact(payload,schema))bad();
    for(const key of ['run_id','work_id','expected_revision_id','candidate_id','command_id','page_no','selection_revision','run_worker_id','expected_preparation_revision'])
      if(Object.hasOwn(call.args,key)&&Object.hasOwn(payload,key)&&payload[key]!==call.args[key])bad();
    if(call.action==='PREPARATION_DISCARD'){
      const n=k=>BigInt(payload[k]),terminal=['DISCARDED','PARTIAL'].includes(payload.phase);
      if((payload.phase==='EMPTY')!==(payload.sequence===null)
        ||n('completed_candidates')!==n('removed_candidates')+n('already_removed_candidates')+n('blocked_candidates')
        ||n('completed_candidates')>n('expected_candidates')
        ||(terminal&&n('completed_candidates')!==n('expected_candidates'))
        ||(payload.phase==='DISCARDED'&&n('blocked_candidates')!==0n)
        ||(payload.phase==='PARTIAL'&&n('blocked_candidates')===0n)
        ||(payload.phase==='EMPTY'&&n('expected_candidates')!==0n))bad();
    }
    if(call.action==='WAKE'&&payload.accepted===payload.throttled)bad();
    if(['WORKER_REPREPARE','WORKER_RELEASE'].includes(call.action)
      &&(payload.action!==(call.action==='WORKER_REPREPARE'?'REPREPARE':'RELEASE')
        ||BigInt(payload.preparation_revision)!==BigInt(call.args.expected_preparation_revision)+1n))bad();
    if(call.action==='EXCLUSION_PAGE'&&payload.item_count!==String(call.args.excluded_work_ids.length+call.args.excluded_candidate_ids.length))bad();
    if(call.action==='MATCHING_PAGE'&&(BigInt(payload.item_count)>100n||payload.skipped.length>Number(payload.item_count)
      ||(payload.complete!==(payload.next_after_member_no===null))||(!payload.complete&&(payload.item_count==='0'
      ||BigInt(payload.next_after_member_no)!==BigInt(call.args.after_member_no??'0')+BigInt(payload.item_count)))))bad();
    if(['CREATE','PAYE_GUARD_DECISION'].includes(call.action)&&(payload.empty!==(payload.selected_candidate_count==='0')||payload.empty!==(payload.agency_sequence===null)||BigInt(payload.selection_count)>BigInt(payload.choice_work_count)||payload.official_pay_date!==call.args.descriptor.namespace.official_pay_date))bad();
    if(call.action==='PAYE_GUARD_DECISION'&&(payload.decision!==call.args.decision
      ||payload.channel_scope!==({UMBRELLA_ONLY:'UMBRELLA',VERIFY_BOTH:'ALL',VERIFY_PAYE:'PAYE'})[call.args.decision]))bad();
    if(call.action==='WORK_PAGE'){
      const ids=call.args.choices.map(x=>x.work_id),positions=payload.skipped.map(x=>ids.indexOf(x.work_id));
      if(positions.some((n,i)=>n<0||(i>0&&n<=positions[i-1])))bad();
    }
    if(call.action==='REVIEW'&&((payload.phase==='PREPARING')!==(payload.reason!==null)))bad();
    if(call.action==='SEAL_ENQUEUE'&&payload.selection_count!==call.args.expected_count)bad();
    if(call.action==='COMPONENT_SEAL'&&payload.component_count!==call.args.expected_count)bad();
    if(call.action==='SEGMENT_SEAL'&&payload.segment_count!==call.args.expected_count)bad();
    if(call.action==='CONFIRM'&&(payload.review_revision!==call.args.review_revision||payload.drafts.some(d=>d.run_id!==call.args.run_id)))bad();
    if(['WORK_PAGE','COMPONENT_PAGE','CASE_PAGE','SEGMENT_PAGE'].includes(call.action)){
      const n=call.args.choices?.length??call.args.approved_line_ids?.length??call.args.component_ids?.length??call.args.segment_keys?.length;
      if(payload.item_count!==String(n))bad();
    }
    return payload;
  }
  if(call.kind==='CASE_PREVIEW'){
    if(!record(payload)||Object.keys(payload).sort().join()!=='complete,rows'||payload.complete!==true
      ||!Array.isArray(payload.rows)||payload.rows.length!==1||!casePreview(payload.rows[0],call.args.input,call.args.case_id?call.args:null))bad();
    return {rows:payload.rows,complete:true,next_cursor:null};
  }
  if(call.kind==='GUARD'){
    if(!exact(payload,{rows:'guardRows',complete:'true'}))bad();
    const g=payload.rows[0];
    if(g.run_id!==call.args.run_id||g.guard_applies!==(g.paye_candidates!=='0'&&g.existing.length>0)
      ||g.unexecuted!==g.existing.some(x=>x.unexecuted)
      ||g.channel_scope!==({NONE:'ALL',UMBRELLA_ONLY:'UMBRELLA',VERIFY_BOTH:'ALL',VERIFY_PAYE:'PAYE'})[g.decision])bad();
    if(call.args.descriptor){const d=call.args.descriptor,c=g.context;
      if(c.workbench_revision!==d.workbench_revision||c.adoption_revision!==d.adoption_revision
        ||['agency_id','owner_epoch','actor_user_id','official_pay_date'].some(k=>c.namespace[k]!==d.namespace[k])
        ||['candidate_id','client_id','channel'].some(k=>c.namespace.scope[k]!==d.namespace.scope[k]))bad();}
    return {rows:payload.rows,complete:true,next_cursor:null};
  }
  if(call.kind==='PROGRESS'){
    if(!record(payload)||Object.keys(payload).sort().join()!=='complete,rows'||payload.complete!==true
      ||!Array.isArray(payload.rows)||payload.rows.length!==1||!exact(payload.rows[0],ROWS.PROGRESS))bad();
    const p=payload.rows[0],n=k=>BigInt(p[k]);
    if(p.run_id!==call.args.run_id||!p.actor_display_name.trim()
      ||n('ready')+n('review')+n('pending')+n('removed')!==n('selected_candidates')
      ||!validDiscardProgress(p)
      ||n('preparing')>n('pending')||n('enrolled_candidates')>n('selected_candidates')
      ||(p.enrolment_complete&&n('enrolled_candidates')!==n('selected_candidates'))
      ||Date.parse(p.expires_at_utc)-Date.parse(p.started_at_utc)!==72*60*60*1000
      ||(p.expired_at_utc!==null&&(p.status!=='CANCELLED'||n('pending')!==0n||n('preparing')!==0n
        ||Date.parse(p.expired_at_utc)<Date.parse(p.expires_at_utc))))bad();
    return {rows:payload.rows,complete:true,next_cursor:null};
  }
  if(['ACCEPTED_CANDIDATES','ACCEPTED_WORK'].includes(call.kind)){
    payload=normalized(payload);
    if(!record(payload)||Object.keys(payload).length!==3||!Object.hasOwn(payload,'header')||!Object.hasOwn(payload,'rows')||!Object.hasOwn(payload,'complete')
      ||!exact(payload.header,ACCEPTED_HEADER)||payload.header.run_id!==call.args.run_id
      ||BigInt(payload.header.enrolled_candidate_count)>BigInt(payload.header.expected_candidate_count)
      ||!Array.isArray(payload.rows)||payload.rows.length>call.args.limit||typeof payload.complete!=='boolean'
      ||(!payload.complete&&payload.rows.length===0))bad();
    const h=payload.header,candidates=call.kind==='ACCEPTED_CANDIDATES',key=candidates?'candidate_id':'work_id',inputKey=candidates?'after_candidate_id':'after_work_id';
    let previous=call.args[inputKey],workInput=null;const acceptedIds=new Set();
    for(const row of payload.rows){
      if(!exact(row,ROWS[call.kind])||(previous!==null&&row[key]<=previous)||acceptedIds.has(candidates?row.accepted_input_id:row.accepted_work_input_id))bad();
      previous=row[key];acceptedIds.add(candidates?row.accepted_input_id:row.accepted_work_input_id);
      if(candidates){
        if(row.module_epoch!==h.accepted_module_epoch||(row.target_pay_channel==='UMBRELLA')!==(row.target_umbrella_id!==null)
          ||(row.case_scope==='NO_CASE')!==(row.case_selection_revision===null)
          ||(row.prepare_job_id===null)!==(row.prepare_job_status===null)||(row.prepare_job_id===null)!==(row.prepare_job_phase===null)
          ||(row.run_worker_id===null)!==(row.worker_status===null)||(row.waiting_command_id===null)!==(row.waiting_command_sequence===null)
          ||(row.waiting_command_sequence!==null&&BigInt(row.waiting_command_sequence)>=BigInt(h.accepted_sequence)))bad();
        const codes={WAITING_ENROLLMENT:['ENROLLMENT_PENDING'],WAITING_EARLIER_COMMAND:['EARLIER_COMMAND_PENDING'],PREPARING:['CUT_PENDING','PREPARATION_RUNNING'],READY:[null],REVIEW:['REVIEW_REQUIRED'],CANCELLING:['CANCELLATION_PENDING'],CANCELLED:[null,'PREPARATION_EXPIRED'],FAILED:['JOB_FAILED'],EPOCH_PAUSED:['OWNER_EPOCH_STALE']};
        if(!codes[row.progress_state].includes(row.progress_code)
          ||(row.progress_state==='WAITING_ENROLLMENT'&&(row.prepare_job_id!==null||row.run_worker_id!==null||!['RECEIVED','ENROLLING'].includes(h.command_status)))
          ||(row.progress_state==='WAITING_EARLIER_COMMAND'&&(row.waiting_command_id===null||!['READY','LEASED','BLOCKED'].includes(row.prepare_job_status)))
          ||(row.progress_state==='READY'&&(row.prepare_job_status!=='DONE'||row.prepare_job_phase!=='READY'||!['READY','DRAFT','ISSUED','COMPLETE'].includes(row.worker_status)||h.run_status==='CANCELLING'||row.has_unsealed_work||!['SEALED','COMPLETE'].includes(h.command_status)||h.expected_candidate_count!==h.enrolled_candidate_count))
          ||(row.progress_state==='REVIEW'&&row.worker_status!=='REVIEW')
          ||(row.progress_state==='CANCELLED'&&!(row.worker_status==='CANCELLED'||(row.prepare_job_status==='DONE'&&row.prepare_job_phase==='EXPIRED')))
          ||(row.progress_state==='CANCELLING'&&row.worker_status!=='CANCELLING'&&h.run_status!=='CANCELLING')
          ||(row.progress_state==='FAILED'&&row.prepare_job_status!=='FAILED'&&h.command_status!=='FAILED')
          ||(row.progress_state==='PREPARING'&&(row.waiting_command_id!==null||!['READY','LEASED','BLOCKED'].includes(row.prepare_job_status)||!['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE'].includes(row.prepare_job_phase)||(row.worker_status!==null&&row.worker_status!=='PREPARING')||((row.progress_code==='CUT_PENDING')!==row.has_unsealed_work))))bad();
      }else {
        if(workInput!==null&&workInput!==row.accepted_input_id)bad();workInput=row.accepted_input_id;
        if(row.candidate_id!==call.args.candidate_id||BigInt(row.predecessor_sequence)>=BigInt(h.accepted_sequence)
        ||(['SOURCE','PROTECTED'].includes(row.source_kind)!==(row.query_disposition==='CLEAR'&&row.query_state_sha256!==null))
        ||(!['SOURCE','PROTECTED'].includes(row.source_kind)&&(row.query_disposition!==null||row.query_state_sha256!==null))
        ||(row.cut_state==='SEALED')!==(row.effective_cut_id!==null))bad();
      }
    }
    return {header:h,rows:payload.rows,complete:payload.complete,next_cursor:payload.complete?null:{[inputKey]:previous}};
  }
  if(call.kind==='WORK_CHOICES'&&call.args.accepted_inputs===true&&call.args.member_kind==='CASE_COMPONENT'){
    const schema={run_id:'uuid',candidate_id:'uuid?',selection_revision:'positive',selection_state:'selectionState',page_count:'integer',item_count:'integer',selected_count:'integer',excluded_count:'integer',expected_count:'integer',candidate_complete:'boolean'};
    if(!record(payload)||Object.keys(payload).sort().join()!=='complete,header,rows'||!exact(payload.header,schema)
      ||payload.header.run_id!==call.args.run_id||!Array.isArray(payload.rows)||payload.rows.length>call.args.limit
      ||typeof payload.complete!=='boolean'||!payload.complete&&!payload.rows.length)bad();
    const h=payload.header;let previous=BigInt(call.args.after_member_no??'0');const ids=new Set();
    if(h.selection_revision!=='1'||BigInt(h.item_count)!==BigInt(h.selected_count)+BigInt(h.excluded_count)
      ||BigInt(h.item_count)>BigInt(h.expected_count)||(h.page_count==='0')!==(h.item_count==='0')
      ||h.selection_state==='SEALED'&&h.item_count!==h.expected_count
      ||(h.candidate_id===null)!==(payload.rows.length===0)||h.candidate_id===null&&(!payload.complete||h.expected_count!=='0'||h.item_count!=='0'||!h.candidate_complete))bad();
    for(const row of payload.rows){if(!exact(row,{member_no:'positive',candidate_id:'uuid',case_component_id:'uuid',selected:'boolean',uploaded:'boolean'})
      ||row.candidate_id!==h.candidate_id||BigInt(row.member_no)!==previous+1n||ids.has(row.case_component_id)
      ||h.selection_state==='SEALED'&&!row.uploaded)bad();previous=BigInt(row.member_no);ids.add(row.case_component_id);}
    if(payload.complete&&!h.candidate_complete)bad();
    return {header:h,rows:payload.rows,complete:payload.complete,next_cursor:payload.complete?null:{after_member_no:String(previous)}};
  }
  if(call.kind==='WORK_CHOICES'&&call.args.accepted_inputs===true){
    if(!record(payload)||Object.keys(payload).sort().join()!=='complete,header,rows'||!exact(payload.header,{run_id:'uuid',uploaded_choice_work_count:'integer',selection_page_count:'integer',selection_count:'integer',exclusion_page_count:'integer'})
      ||payload.header.run_id!==call.args.run_id||!Array.isArray(payload.rows)||payload.rows.length>call.args.limit
      ||typeof payload.complete!=='boolean'||(!payload.complete&&!payload.rows.length))bad();
    let previous=BigInt(call.args.after_member_no??'0');const ids=new Set();
    for(const row of payload.rows){if(!exact(row,{member_no:'positive',candidate_id:'uuid',work_id:'uuid',expected_revision_id:'uuid',selection_mode:'mode',skip_code:'skipCode?',choice_state:'selectionState?',component_page_count:'integer?',component_count:'integer?'})||BigInt(row.member_no)!==previous+1n||ids.has(row.work_id.toLowerCase()))bad();if((row.skip_code!==null)!==(row.choice_state===null)||(row.choice_state===null)!==(row.component_page_count===null)||(row.choice_state===null)!==(row.component_count===null)||row.choice_state!==null&&row.selection_mode==='ALL'&&(row.choice_state!=='SEALED'||row.component_page_count!=='0'||row.component_count!=='0'))bad();previous=BigInt(row.member_no);ids.add(row.work_id.toLowerCase());}
    return {header:payload.header,rows:payload.rows,complete:payload.complete,next_cursor:payload.complete?null:{after_member_no:String(previous)}};
  }
  if(call.kind==='CHANNEL_DRAFTS'){
    if(!exact(payload,{rows:'drafts',complete:'true'})||payload.rows.some(d=>d.run_id!==call.args.run_id))bad();
    return {rows:payload.rows,complete:true,next_cursor:null};
  }
  if(call.kind==='CHANNEL_STATUS'){
    if(payload?.header?.target_pay_channel!==call.args.pay_channel)bad();
    return result(payload,{...call,kind:'STATUS'},false);
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
      ||h.net_pending!==(h.net_replacement_pending_id!==null||h.net_request_revision!==h.net_projection_revision)||!validNetReplacement(h)||h.net_replacement_pending_id!==null&&h.net_replacement_pending_id!==h.replacement_id||!validReservation(h))bad();
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
    if(!exact(row,call.kind==='WORKERS'&&call.args.attention_only===true?{run_worker_id:'uuid',candidate_id:'uuid',candidate_display_name:'label',pay_channel:'channel',worker_status:'workerStatus',job_id:'uuid',work_id:'null',module_epoch:'positive',owner_epoch:'positive',phase:'label',cursor:'label?',retry_count:'positive',last_error_code:'issue?',retry_eligible:'boolean'}:ROWS[call.kind]))bad();
    if(call.kind==='WORKERS'&&call.args.attention_only===true&&(row.retry_count!=='5'||!['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE'].includes(row.phase)))bad();
    if(call.kind==='WORKERS'&&!call.args.attention_only&&((row.discard_outcome==='BLOCKED')!==(row.discard_blocked_code!==null)))bad();
      if(call.kind==='COMPONENTS'){
        if((row.associated_rate_schedule_id===null)!==(row.associated_rate_family===null)||(row.associated_rate_schedule_id===null)!==(row.associated_rate_code===null)
          ||(row.associated_rate_schedule_id===null&&['available_paye_rate','available_umbrella_rate','available_charge_rate','approved_charge_rate'].some(k=>row[k]!==null))
          ||(row.original_timesheet_id===null&&row.original_work_ref!==null))bad();
      }
      if(call.kind==='RATES'&&row.rate_schedule_id===null&&['available_paye_rate','available_umbrella_rate','available_charge_rate','approved_charge_rate'].some(k=>row[k]!==null))bad();
      if(call.kind==='SHIFTS'&&row.segment_key===null&&row.approved_segment_id!==null)bad();
    if(call.kind==='STATUS'&&!validReservation(payload.header,row))bad();
    if(call.kind==='CHANNEL_WORKERS'&&row.pay_channel!==call.args.pay_channel)bad();
    const value=row[key];
    if(previous!==null){const ordered=key.endsWith('_no')||key==='component_ordinal'?BigInt(value)>BigInt(previous):value>previous;if(!ordered)bad();}
    previous=value;
    if(['CURRENT_CASES','CURRENT_STORED_CREDITS','CASE_COMPONENTS'].includes(call.kind)&&row.candidate_id!==call.args.candidate_id)bad();
    if(storedCreditPage&&row.legacy_case_id>payload.scan_cursor)bad();
    if(call.kind==='CASE_COMPONENTS'&&row.case_id!==call.args.case_id)bad();
    if(call.kind==='CASE_COMPONENTS'){
      if(call.args.channel_command_id?(!row.channel_receipt||row.channel_receipt.command_id!==call.args.channel_command_id||row.channel_receipt.case_id!==row.case_id||row.channel_receipt.candidate_id!==row.candidate_id):row.channel_receipt!==null)bad();
      if(call.args.restructure_command_id?(!row.restructure_receipt||row.restructure_receipt.command_id!==call.args.restructure_command_id||row.restructure_receipt.case_id!==row.case_id||row.restructure_receipt.candidate_id!==row.candidate_id):row.restructure_receipt!==null)bad();
      if(row.pause_state.case_id!==row.case_id||row.pause_state.candidate_id!==row.candidate_id
        ||(row.pause_receipt!==null&&(row.pause_receipt.case_id!==row.case_id||row.pause_receipt.candidate_id!==row.candidate_id||row.pause_receipt.command_id!==call.args.pause_command_id))
        ||(Object.hasOwn(call.args,'pause_command_id')?(row.pause_receipt===null):row.pause_receipt!==null))bad();
      if(row.edit_state.case_id!==row.case_id||row.edit_state.candidate_id!==row.candidate_id
        ||(row.edit_receipt!==null&&(row.edit_receipt.case_id!==row.case_id||row.edit_receipt.candidate_id!==row.candidate_id||row.edit_receipt.command_id!==call.args.edit_command_id))
        ||(Object.hasOwn(call.args,'edit_command_id')?(row.edit_receipt===null):row.edit_receipt!==null))bad();
      if(row.original_liability_reference!==null&&(row.case_kind!=='MANUAL_DEBT'||row.case_subtype!=='MANUAL_DEBT'))bad();
      if(caseIds.has(row.case_component_id))bad();caseIds.add(row.case_component_id);
    }
    if(call.kind==='SELECTED_COMPONENTS'&&call.args.expected_revision_id&&row.expected_revision_id!==call.args.expected_revision_id)bad();
    if(call.kind==='WORK_CHOICES'&&row.selection_mode==='ALL'&&(row.selection_state!=='SEALED'||row.page_count!=='0'||row.component_count!=='0'))bad();
    if(call.kind==='SHIFTS'&&row.pay_excluded===true&&row.segment_pay_ex_vat!=='0.00')bad();
  }
  return {...(call.kind==='STATUS'?{header:payload.header}:{}),rows:payload.rows,complete:payload.complete,next_cursor:payload.complete?null:{[inputKey]:storedCreditPage?payload.scan_cursor:previous}};
}
function safeError(e,attempted,mutation,action=null,args=null){
  if(e instanceof PreparationError)return e;
  if(['BPAY_NEXT_REAUTH_REQUIRED','BPAY_NEXT_REAUTH_STALE'].includes(e?.code))return new PreparationError(e.code,403,false,false);
  const db=e?.json??e;
  // FC-03 correction discovered by FC-04: preserve the named creation refusal.
  if(action==='CASE_CHANNEL_SUGGEST'&&[...CHANNEL_ISSUES,'BPAY_NEXT_CASE_CHANNEL_FIXED_AMOUNT'].includes(db?.message))return fault(db.message,409);
  if(action==='CASE_RESTRUCTURE_PREVIEW'&&RESTRUCTURE_ISSUES.includes(db?.message))return fault(db.message,409);
  if(action==='CASE_PREVIEW'&&['BPAY_NEXT_CASE_HAS_ACTIVITY','BPAY_NEXT_CASE_HELD_BY_DRAFT','BPAY_NEXT_CASE_EDIT_UNSUPPORTED','BPAY_NEXT_CASE_PROTECTION_CHANGED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID'].includes(db?.message))return fault(db.message,409);
  if(['CASE_PREVIEW','CASE_CREATE'].includes(action)&&db?.code==='55000'&&db.message==='BPAY_NEXT_CASE_CREATE_SOURCE_CHANNEL_UNBOUND')
    return new PreparationError(db.message,409,false,false);
  if(['REVIEW_TOTALS','REVIEW_CASES'].includes(action)&&db?.code==='55000'&&['BPAY_NEXT_REVIEW_CONTEXT_CHANGED','BPAY_NEXT_CASE_REVIEW_NOT_READY'].includes(db.message))return new PreparationError(db.message,409,false,false);
  if(action==='PAYE_WORKSHEET'&&db?.code==='55000'&&['BPAY_NEXT_PAYE_WORKSHEET_CONTEXT_CHANGED','BPAY_NEXT_PAYE_WORKSHEET_NOT_APPLICABLE','BPAY_NEXT_CHANNEL_DRAFT_NOT_AVAILABLE'].includes(db.message))return new PreparationError(db.message,409,false,false);
  if(action==='PREPARE_ISSUES'&&db?.code==='55000'&&db.message==='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED')return new PreparationError(db.message,409,false,false);
  if(['BPAY_NEXT_CASE_FINANCE_CONTEXT_CHANGED','BPAY_NEXT_CASE_FINANCE_NOT_FOUND','BPAY_NEXT_CASE_FINANCE_REQUEST_INVALID','BPAY_NEXT_CASE_FINANCE_ROW_TOO_LARGE'].includes(db?.message))
    return new PreparationError(db.message,db.code==='P0002'?404:db.code==='22023'?400:409,false,false);
  if(db?.code==='55000'&&['BPAY_NEXT_CASE_LIST_CONTEXT_CHANGED','BPAY_NEXT_CASE_LIST_DATE_UNAVAILABLE','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT'].includes(db.message))
    return new PreparationError(db.message,409,false,false);
  if(['BPAY_NEXT_CHOICE_CONTEXT_CHANGED','BPAY_NEXT_PAYE_GUARD_CHANGED','BPAY_NEXT_PAYE_SAME_WEEK_GUARD_UNRESOLVED','BPAY_NEXT_PAYE_SAME_WEEK_GUARD_UNEXECUTED','BPAY_NEXT_PAYE_GUARD_FACTS_UNAVAILABLE','BPAY_NEXT_REAUTH_REQUIRED','BPAY_NEXT_REAUTH_STALE'].includes(db?.message))
    return new PreparationError(db.message,db.message.startsWith('BPAY_NEXT_REAUTH_')?403:409,false,false);
  const manifestConflicts=['BPAY_NEXT_SELECTION_PAGE_CONFLICT','BPAY_NEXT_SELECTION_PAGE_REPLAY_CONFLICT','BPAY_NEXT_CHOICE_REPLAY_CONFLICT','BPAY_NEXT_CASE_SELECTION_REPLAY_CONFLICT'];
  if(mutation&&((db?.code==='23514'&&manifestConflicts.includes(db.message))
    ||(db?.code==='55000'&&db.message==='BPAY_NEXT_SELECTION_ALREADY_SEALED')))
    return new PreparationError(db.message,409,false,false);
  if(mutation&&action==='SEAL_ENQUEUE'&&typeof db?.details==='string'&&ENCODER.encode(db.details).byteLength<=1024){
    let refusal;try{refusal=JSON.parse(db.details);}catch{}
    const reasons={EARLIER_ECONOMIC_PENDING:true,QUERY_UNAVAILABLE:true,QUERY_BLOCKED:false,QUERY_BUSY:true,INPUT_UNAVAILABLE:false,SELECTION_INCOMPLETE:false,ORIGIN_UNAVAILABLE:false,TRANSACTION_NOT_FRESH:false};
    if(db.code==='55000'&&db.message==='BPAY_NEXT_PREPARATION_BEFORE_ACCEPTANCE'&&record(refusal)
      &&Object.keys(refusal).length===4&&['phase','reason','candidate_id','work_id'].every(k=>Object.hasOwn(refusal,k))
      &&refusal.phase==='BEFORE_ACCEPTANCE'&&Object.hasOwn(reasons,refusal.reason)
      &&field(refusal.candidate_id,'uuid?')&&field(refusal.work_id,'uuid?')&&(refusal.work_id===null||refusal.candidate_id!==null))
      return new PreparationError('BPAY_NEXT_PREPARATION_'+refusal.reason,409,reasons[refusal.reason],false,normalized(refusal));
    if(db.code==='23514'&&db.message==='BPAY_NEXT_PREPARATION_ACCEPTED_IDENTITY'&&record(refusal)
      &&Object.keys(refusal).length===5&&['phase','reason','run_id','command_id','agency_sequence'].every(k=>Object.hasOwn(refusal,k))
      &&refusal.phase==='ACCEPTED_IDENTITY'&&refusal.reason==='COMMAND_CONFLICT'
      &&field(refusal.run_id,'uuid')&&refusal.run_id.toLowerCase()===args?.run_id&&field(refusal.command_id,'uuid')&&field(refusal.agency_sequence,'positive'))
      return new PreparationError('BPAY_NEXT_PREPARATION_COMMAND_CONFLICT',409,false,false,normalized(refusal));
  }
  if(['PGRST202','42883','42P01','42703'].includes(db?.code))return fault('SCHEMA_UNAVAILABLE',503,false);
  if(db?.code==='42501'||[401,403].includes(e?.status))return fault('FORBIDDEN',403);
  if(db?.code==='P0002')return fault('SCOPE_NOT_FOUND',404);
  if(mutation&&action==='CREATE'&&db?.code==='22023'&&db.message==='BPAY_NEXT_PAY_DATE_NOT_OFFICIAL')
    return new PreparationError('BPAY_NEXT_PAY_DATE_NOT_OFFICIAL',400,false,false);
  if(['22023','22003','22007','22008','22P02'].includes(db?.code))return fault('REQUEST_INVALID',400);
  if(['23514','23505','23503'].includes(db?.code))return fault('CONFLICT',409);
  if(mutation&&action==='CONFIRM'&&db?.code==='55000'&&db.message==='BPAY_NEXT_CONFIRM_PREPARATION_EXPIRED_OR_UNBOUND')
    return new PreparationError('BPAY_NEXT_CONFIRM_PREPARATION_EXPIRED_OR_UNBOUND',409,false,false);
  if(mutation&&action==='CONFIRM'&&db?.code==='55000'&&db.message==='BPAY_NEXT_CONFIRM_PREPARATION_DISCARDED_OR_BLOCKED')
    return new PreparationError('BPAY_NEXT_CONFIRM_PREPARATION_DISCARDED_OR_BLOCKED',409,false,false);
  if(mutation&&action==='CONFIRM'&&db?.code==='55000'&&db.message==='BPAY_NEXT_CONFIRM_NO_PAYABLE_CANDIDATE')
    return new PreparationError('BPAY_NEXT_CONFIRM_NO_PAYABLE_CANDIDATE',409,false,false);
  if(!mutation&&db?.code==='55000'&&db.message==='BPAY_NEXT_CHANNEL_DRAFT_CONTEXT_CHANGED')return new PreparationError('BPAY_NEXT_CHANNEL_DRAFT_CONTEXT_CHANGED',409,false,false);
  if(!mutation&&db?.code==='55000'&&db.message==='BPAY_NEXT_ACTIVE_PREPARATIONS_CONTEXT_CHANGED')return new PreparationError('BPAY_NEXT_ACTIVE_PREPARATIONS_CONTEXT_CHANGED',409,false,false);
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
    const timeoutMs=!mutation&&['PROGRESS','ACTIVE_PREPARATIONS','CASE_LIST','CASE_SUMMARY'].includes(call.kind)?2000:BPAY_NEXT_PREPARATION_LIMITS.timeoutMs;
    const maxResponseBytes=!mutation&&['CASE_LIST','CASE_SUMMARY','CASE_AUDIT','CANDIDATE_CASE_REPORT'].includes(call.kind)?120000:BPAY_NEXT_PREPARATION_LIMITS.responseBytes;
    const timeout=new Promise((resolve,reject)=>{timer=setTimeout(()=>{controller.abort();reject(fault('DEPENDENCY_TIMEOUT',503,true,attempted&&mutation));},timeoutMs);});
    const send=async()=>{
      let args=call.args;
      if(mutation&&call.action==='PAYE_GUARD_DECISION'){
        let proofId=null;
        if(call.args.decision!=='UMBRELLA_ONLY'){
          if(typeof dependencies.verifyBpayNextActionReauth!=='function'||typeof dependencies.reauthRecordRpc!=='function')throw fault('DEPENDENCY_UNAVAILABLE',503,true);
          const verified=await dependencies.verifyBpayNextActionReauth(call.token,user,'PAYE_SAME_WEEK_OVERRIDE');
          if(!verified)throw new PreparationError('BPAY_NEXT_REAUTH_REQUIRED',403,false,false);
          const proof=await readBpayNextReauthJson(await dependencies.reauthRecordRpc(verified,{actorUserId:user.id,signal:controller.signal}),controller.signal);
          if(!exact(proof,{reauth_proof_id:'uuid',consumed_command_id:'uuid?'})||proof.consumed_command_id!==null&&proof.consumed_command_id.toLowerCase()!==call.args.command_id)throw new PreparationError('BPAY_NEXT_REAUTH_REQUIRED',403,false,false);
          if(proof.consumed_command_id===null&&verified.second_factor_used===false){
            const required=await dependencies.secondFactorRequired?.();
            if(typeof required!=='boolean'||required)throw new PreparationError('BPAY_NEXT_REAUTH_REQUIRED',403,false,false);
          }
          proofId=proof.reauth_proof_id;
        }
        args={...call.args,reauth_proof_id:proofId};
      }
      if(controller.signal.aborted)throw fault('DEPENDENCY_TIMEOUT',503,true,false);
      attempted=true;
      const response=await rpc(call.action??call.kind,args,{actorUserId:user.id.toLowerCase(),signal:controller.signal,
        timeoutMs,maxResponseBytes,routeClass:mutation?'OPERATION_NUDGE':'READ'});
      if(!(response instanceof Response))throw fault('RESPONSE_INVALID',503,true,mutation);
      const payload=await limitedJson(response,maxResponseBytes,false,controller.signal,mutation);
      if(!response.ok)throw safeError({status:response.status,json:payload},true,mutation,call.action??call.kind,call.args);
      const body=mutation?{ok:true,action:call.action,result:result(payload,call,true)}:{ok:true,kind:call.kind,...result(payload,{...call,actorUserId:user.id.toLowerCase()},false)};
      if(ENCODER.encode(JSON.stringify(body)).byteLength>maxResponseBytes)throw fault('RESPONSE_TOO_LARGE',503,true,mutation);
      return json(200,body,mutation&&(['SEAL_ENQUEUE','WORKER_REPREPARE','WORKER_RELEASE'].includes(call.action)||(call.action==='PREPARATION_DISCARD'&&body.result.sequence!==null)||(call.action==='WAKE'&&body.result.accepted===true)));
    };
    return await Promise.race([send(),timeout]);
  }catch(e){const safe=safeError(e,attempted,mutation);return json(safe.status,{ok:false,error_code:safe.code,retryable:safe.retryable,outcome_unknown:safe.outcomeUnknown,...(safe.refusal?{refusal:safe.refusal}:{})});}
  finally{if(timer!==undefined)clearTimeout(timer);}
}
/** @param {Request} request @param {Partial<PreparationDependencies>} [dependencies] */
export const handleBpayNextPreparationCommand=(request,dependencies={})=>handle(request,dependencies,true);
/** @param {Request} request @param {Partial<PreparationDependencies>} [dependencies] */
export const handleBpayNextPreparationPage=(request,dependencies={})=>handle(request,dependencies,false);
