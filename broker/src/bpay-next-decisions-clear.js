/** WB28 closed frozen-operation transport. Existing W2 owns admission and SQL. */
const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const exact=(v,keys)=>object(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
const same=(a,b)=>a===b||object(a)&&object(b)&&exact(a,Object.keys(b))&&Object.keys(a).every(k=>same(a[k],b[k]));
const id=v=>typeof v==='string'&&UUID.test(v);
const integer=(v,positive=false)=>typeof v==='string'&&(positive?/^[1-9][0-9]{0,18}$/:/^(0|[1-9][0-9]{0,18})$/).test(v)&&BigInt(v)<=9223372036854775807n;
const decimal=(v,scale=2,digits=36)=>typeof v==='string'&&new RegExp('^(0|[1-9][0-9]{0,'+(digits-1)+'})(\\.[0-9]{1,'+scale+'})?$').test(v);
const text=(v,n)=>typeof v==='string'&&!v.includes('\0')&&new TextEncoder().encode(v).length<=n;
const nullable=(v,fn)=>v===null||fn(v);
const date=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v+'T00:00:00Z'))&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
const bytes=v=>new TextEncoder().encode(JSON.stringify(v)).length;
const fail=(code='BPAY_NEXT_WORKBENCH_RESPONSE_INVALID',status=503,retryable=false)=>Object.assign(new Error(code),{code,status,retryable});
function scope(v){return exact(v,['candidate_id','client_id','channel'])&&nullable(v.candidate_id,id)&&nullable(v.client_id,id)&&['ALL','PAYE','UMBRELLA'].includes(v.channel);}
function cursor(v){return nullable(v,x=>exact(x,['member_no','owner_no'])&&integer(x.member_no,true)&&integer(x.owner_no,true)&&BigInt(x.owner_no)<=100n);}
export function normalizeDecisionsClear(args){
 if(!exact(args,['request_id','scope','owner_epoch','official_pay_date','expected_workbench_revision','phase','step','cursor','confirmed'])||!id(args.request_id)||!scope(args.scope)
  ||!integer(args.owner_epoch,true)||!date(args.official_pay_date)||!integer(args.expected_workbench_revision)||!integer(args.step)||!['FREEZE','VIEW','APPLY'].includes(args.phase)
  ||typeof args.confirmed!=='boolean'||args.confirmed!==(args.phase==='APPLY')||!cursor(args.cursor)||args.cursor!==null&&args.phase!=='VIEW'||bytes(args)>4096)
  throw fail('BPAY_NEXT_DECISIONS_CLEAR_REQUEST_INVALID',400);
 return args;
}
const RESULT_KEYS=['request_id','agency_id','actor_user_id','owner_epoch','official_pay_date','scope','expected_workbench_revision','state','scope_summary','candidate_count','work_count','owner_count','excluded_count','affected_amount','cleared_count','refused_count','cleared_amount','freeze_step','apply_step','reset_sequence','rows','next_cursor'];
const OWNER_KEYS=['owner_id','owner_version','approved_line_id','rate_family','rate_code','amount_ex_vat','clear_class','exclusion_code','target_pay_channel','resolution_mode','manual_replacement_rate','manual_amount_ex_vat'];
const reasons=[null,'IN_DRAFT','CHANGED','UNAVAILABLE'];
export function validateDecisionsClearReceipt(v,args,actorId){
 normalizeDecisionsClear(args);const bad=()=>{throw fail();};
 if(!exact(v,['request_id','phase','step','received','replay','processing','result'])||v.request_id!==args.request_id||v.phase!==args.phase||v.step!==args.step
  ||['received','replay','processing'].some(k=>typeof v[k]!=='boolean')||v.replay&&!v.received||v.processing&&(v.received||v.replay||args.phase!=='APPLY')||bytes(v)>120000)bad();
 if(!v.received&&!v.processing){if(v.result!==null)bad();return v;}
 const r=v.result;
 if(!exact(r,RESULT_KEYS)||r.request_id!==args.request_id||!id(r.agency_id)||r.actor_user_id!==actorId||!id(r.actor_user_id)||r.owner_epoch!==args.owner_epoch||r.official_pay_date!==args.official_pay_date
  ||!same(r.scope,args.scope)||r.expected_workbench_revision!==args.expected_workbench_revision||!['DISCOVERING','READY','APPLYING','COMPLETE','PARTIAL'].includes(r.state)
  ||!exact(r.scope_summary,['candidate_count'])||!integer(r.scope_summary.candidate_count)
  ||!['candidate_count','work_count','owner_count','excluded_count','cleared_count','refused_count','freeze_step','apply_step'].every(k=>integer(r[k]))
  ||!decimal(r.affected_amount)||!decimal(r.cleared_amount)||!nullable(r.reset_sequence,x=>integer(x,true))
  ||['COMPLETE','PARTIAL'].includes(r.state)!==(r.reset_sequence!==null)||!Array.isArray(r.rows)||r.rows.length>10||!cursor(r.next_cursor)
  ||BigInt(r.cleared_count)+BigInt(r.refused_count)>BigInt(r.owner_count)||BigInt(r.work_count)>BigInt(r.candidate_count)+BigInt(r.owner_count)+BigInt(r.excluded_count)
  ||r.state==='PARTIAL'&&r.refused_count==='0'||r.state==='COMPLETE'&&r.refused_count!=='0')bad();
 if(args.phase==='FREEZE'&&v.received&&(!['DISCOVERING','READY'].includes(r.state)||BigInt(r.freeze_step)!==BigInt(args.step)+1n)
  ||args.phase==='APPLY'&&(v.received?(!['APPLYING','COMPLETE','PARTIAL'].includes(r.state)||BigInt(r.apply_step)!==BigInt(args.step)+1n)
   :v.processing&&(!['READY','APPLYING'].includes(r.state)||r.apply_step!==args.step)))bad();
 let last=null;
 for(const row of r.rows){
  if(!exact(row,['member_no','owner_no','candidate_id','work_id','owner_family','exclusion_code','subject','outcome_code','cleared','owner'])
   ||!integer(row.member_no,true)||!integer(row.owner_no,true)||BigInt(row.owner_no)>100n||!id(row.candidate_id)||!id(row.work_id)
   ||!['GROSS','WORK_BUCKET'].includes(row.owner_family)||!reasons.includes(row.exclusion_code)||!reasons.includes(row.outcome_code)||typeof row.cleared!=='boolean'
   ||row.cleared&&row.outcome_code!==null||last&&(BigInt(row.member_no)<BigInt(last.member_no)||row.member_no===last.member_no&&BigInt(row.owner_no)<=BigInt(last.owner_no)))bad();
  const s=row.subject;
  if(!exact(s,['candidate_display_name','candidate_reference','client_display_name','timesheet_reference','week_ending_date'])
   ||!text(s.candidate_display_name,8192)||!nullable(s.candidate_reference,x=>text(x,8192))||!text(s.client_display_name,8192)
   ||!nullable(s.timesheet_reference,x=>text(x,8192))||!date(s.week_ending_date))bad();
  const o=row.owner;
  if(o!==null&&(!exact(o,OWNER_KEYS)||!id(o.owner_id)||!integer(o.owner_version,true)||!id(o.approved_line_id)||!['STANDARD','ADDITIONAL','ADJUSTMENT'].includes(o.rate_family)
   ||row.owner_family!==(o.rate_family==='STANDARD'?'WORK_BUCKET':'GROSS')||!text(o.rate_code,1024)||!nullable(o.amount_ex_vat,x=>decimal(x,2,16))
   ||!['RESOLVED_AND_CLEARABLE','NOT_CLEARABLE'].includes(o.clear_class)||!reasons.includes(o.exclusion_code)||!['PAYE','UMBRELLA'].includes(o.target_pay_channel)
   ||!nullable(o.resolution_mode,x=>['SUGGESTED_EQUIVALENT_BASIS','MANUAL_REPLACEMENT_RATE','MANUAL_AMOUNT'].includes(x))
   ||!nullable(o.manual_replacement_rate,x=>decimal(x,6,12))||!nullable(o.manual_amount_ex_vat,x=>decimal(x,2,16))
   ||o.rate_family==='STANDARD'&&!['DAY','NIGHT','SAT','SUN','BH'].includes(o.rate_code)
   ||(o.resolution_mode==='MANUAL_REPLACEMENT_RATE'?o.manual_replacement_rate===null||o.manual_amount_ex_vat!==null
     :o.resolution_mode==='MANUAL_AMOUNT'?o.manual_amount_ex_vat===null||o.manual_replacement_rate!==null
     :o.manual_replacement_rate!==null||o.manual_amount_ex_vat!==null)
   ||o.clear_class==='RESOLVED_AND_CLEARABLE'&&(o.amount_ex_vat===null||o.exclusion_code!==null||o.resolution_mode===null)))bad();
  last=row;
 }
 if(r.next_cursor!==null&&(!last||r.next_cursor.member_no!==last.member_no||r.next_cursor.owner_no!==last.owner_no))bad();
 if(args.phase==='VIEW'&&args.cursor!==null&&r.rows.length&&(BigInt(r.rows[0].member_no)<BigInt(args.cursor.member_no)
  ||r.rows[0].member_no===args.cursor.member_no&&BigInt(r.rows[0].owner_no)<=BigInt(args.cursor.owner_no)))bad();
 return v;
}
const nativeRules=new Map([
 ['22023:BPAY_NEXT_DECISIONS_CLEAR_REQUEST_INVALID',[400,false]],['23514:BPAY_NEXT_DECISIONS_CLEAR_REPLAY_CONFLICT',[409,false]],
 ['23514:BPAY_NEXT_DECISIONS_CLEAR_STALE',[409,false]],['23514:BPAY_NEXT_CHOICE_CONTEXT_CHANGED',[409,false]],
 ['54000:BPAY_NEXT_DECISIONS_CLEAR_SCOPE_TOO_LARGE',[413,false]],['55000:BPAY_NEXT_MODULE_NOT_ACTIVE',[409,false]],
 ['55P03:BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY',[409,true]],['42501:BPAY_NEXT_WORKBENCH_COMMAND_FORBIDDEN',[403,false]]
]);
export function decisionsClearNativeFault(v){
 const rule=nativeRules.get(v?.code+':'+v?.message);if(rule)return fail(v.message,...rule);
 // PostgreSQL transaction-abort classes prove that this exact page was not accepted.
 if(['55P03','40001','40P01','57014'].includes(v?.code))return fail('BPAY_NEXT_DECISIONS_CLEAR_BUSY',409,true);
 return null;
}
export function validateDecisionsClearFault(v){
 if(!exact(v,['ok','error_code','retryable','outcome_unknown'])||v.ok!==false||typeof v.retryable!=='boolean'||typeof v.outcome_unknown!=='boolean')throw fail();
 const known=[...nativeRules.keys()].some(k=>k.slice(6)===v.error_code)||v.error_code==='BPAY_NEXT_DECISIONS_CLEAR_BUSY';
 if(known&&v.outcome_unknown||!known&&!['BPAY_NEXT_WORKBENCH_UNAVAILABLE','BPAY_NEXT_WORKBENCH_RESPONSE_INVALID','BPAY_NEXT_WORKBENCH_UNAUTHORIZED','BPAY_NEXT_WORKBENCH_FORBIDDEN','BPAY_NEXT_ENVIRONMENT_MISMATCH','BPAY_NEXT_OFFICE_FORBIDDEN'].includes(v.error_code))throw fail();
 return v;
}
