/* WB-39 finite read contract, shared exactly with Office. */

const SR_UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const SR_VERSION=/^(?:0|[1-9]\d{0,18})$/;
const SR_DATE=/^\d{4}-\d{2}-\d{2}$/;
const SR_TIME=/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+]00:00)$/;
const srRecord=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const srExact=(v,keys)=>srRecord(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
const srEqual=(a,b)=>JSON.stringify(srCanonical(a))===JSON.stringify(srCanonical(b));
function srCanonical(v){if(Array.isArray(v))return v.map(srCanonical);if(srRecord(v))return Object.fromEntries(Object.keys(v).sort().map(k=>[k,srCanonical(v[k])]));return v;}
function srFail(code='BPAY_NEXT_WORKBENCH_RESPONSE_INVALID'){throw Object.assign(Error(code),{code});}
function srUuid(v,optional=false){return optional&&v===null||typeof v==='string'&&SR_UUID.test(v);}
function srVersion(v,positive=false){return typeof v==='string'&&SR_VERSION.test(v)&&BigInt(v)<=9223372036854775807n&&(!positive||BigInt(v)>0n);}
const SR_INPUT=['candidate_id','client_id','scope_kind','filter','snooze_id','sort','after','limit','view','count_only'];
function normalizeSnoozesInput(params={}){
 if(!srRecord(params)||Object.keys(params).some(k=>!SR_INPUT.includes(k)))srFail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID');
 const a={candidate_id:null,client_id:null,scope_kind:'ALL',filter:'ACTIVE',snooze_id:null,sort:'CREATED_DESC',after:null,limit:20,view:'SNOOZED',count_only:false,...params};
 if(!['candidate_id','client_id','snooze_id'].every(k=>srUuid(a[k],true))||!['ALL','WORK','CASE'].includes(a.scope_kind)
  ||!['ACTIVE','INDEFINITE','DATED'].includes(a.filter)||!['CREATED_ASC','CREATED_DESC','CANDIDATE_ASC','CANDIDATE_DESC','TIMESHEET_ASC','TIMESHEET_DESC','WHAT_ASC','WHAT_DESC','UNTIL_ASC','UNTIL_DESC','NOTE_ASC','NOTE_DESC'].includes(a.sort)
  ||!['ALL','ACTIVE','SNOOZED','HISTORY'].includes(a.view)||typeof a.count_only!=='boolean'||a.count_only&&(a.snooze_id!==null||!a.sort.startsWith('CREATED_'))
  ||!Number.isSafeInteger(a.limit)||a.limit<1||a.limit>50||a.snooze_id!==null&&a.after!==null)srFail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID');
 if(a.after!==null&&(!srExact(a.after,['context','key'])||!srContext(a.after.context)||!srKey(a.after.key)
  ||!srEqual(a.after.context.scope,srScope(a))))srFail('BPAY_NEXT_WORKBENCH_REQUEST_INVALID');
 return a;
}
function srScope(a){return Object.fromEntries(SR_INPUT.filter(k=>!['after','limit'].includes(k)).map(k=>[k,a[k]]));}
function srDecodeKey(k){const raw=k.sort_value;if(raw===null)return k;try{if(typeof raw!=='string'||raw.length>11000||!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(raw))return null;const bytes=atob(raw),value=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(Uint8Array.from(bytes,c=>c.charCodeAt(0)));if(btoa(bytes)!==raw||value.includes('\0')||new TextEncoder().encode(value).byteLength>8192)return null;return {...k,sort_value:value};}catch{return null;}}
function srKey(k){return srExact(k,['created_at_utc','id','sort_value'])&&srDecodeKey(k)!==null&&srUuid(k.id)&&typeof k.created_at_utc==='string'&&SR_TIME.test(k.created_at_utc)&&Number.isFinite(Date.parse(k.created_at_utc));}
function srContext(c){return srExact(c,['actor_user_id','agency_id','owner_epoch','workbench_revision','adoption_revision','control_cut','scope'])
 &&srUuid(c.actor_user_id)&&srUuid(c.agency_id)&&srVersion(c.owner_epoch,true)&&['workbench_revision','adoption_revision','control_cut'].every(k=>srVersion(c[k]))
 &&srExact(c.scope,SR_INPUT.filter(k=>!['after','limit'].includes(k)));}
const SR_ROW=['snooze_id','candidate_id','work_id','expected_revision_id','revision_id','physical_timesheet_id','case_id','case_component_id','case_kind','case_component_revision','scope_kind','scope_identity','segment_key','component_key','snooze_kind','version','control_sequence','active','until_date','note','created_at_utc','candidate_display_name','candidate_reference','client_id','client_display_name','timesheet_reference','week_ending_date','label','work_date','shift_start_local','shift_end_local','can_amend','can_unsnooze','amend_unavailable_reason','sort_key'];
function validateSnoozesRow(r){
 if(!srExact(r,SR_ROW)||!['snooze_id','candidate_id'].every(k=>srUuid(r[k]))||!['work_id','expected_revision_id','revision_id','physical_timesheet_id','case_id','case_component_id','client_id'].every(k=>srUuid(r[k],true))
  ||!srVersion(r.version,true)||!srVersion(r.control_sequence,true)||typeof r.active!=='boolean'||typeof r.can_amend!=='boolean'||r.can_unsnooze!==r.active||!r.active&&r.can_amend
  ||!['WHOLE_TIMESHEET','SEGMENT','EXPENSE_LINE','EXPENSES_ALL','CASE'].includes(r.scope_kind)
  ||!['DO_NOT_PAY','BLOCKED_TIMESHEET','TIMESHEET_PAYMENT','OVERPAYMENT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','MANUAL_DEBT_RECOVERY'].includes(r.snooze_kind)
  ||!['until_date','week_ending_date','work_date'].every(k=>r[k]===null||typeof r[k]==='string'&&SR_DATE.test(r[k])&&Number.isFinite(Date.parse(r[k])))
  ||!srKey({created_at_utc:r.created_at_utc,id:r.snooze_id,sort_value:null})||typeof r.note!=='string'||r.note.length>500
  ||typeof r.scope_identity!=='string'||r.scope_identity.length<1||r.scope_identity.length>500
  ||typeof r.label!=='string'||r.label.length<1||r.label.length>256
  ||typeof r.candidate_display_name!=='string'||new TextEncoder().encode(r.candidate_display_name).byteLength>8192
  ||!['candidate_reference','client_display_name','timesheet_reference','shift_start_local','shift_end_local','component_key','segment_key'].every(k=>r[k]===null||typeof r[k]==='string'&&new TextEncoder().encode(r[k]).byteLength<=8192)
  ||r.sort_key!==null&&(!srExact(r.sort_key,['created_at_utc','id','sort_value'])||r.sort_key.id!==r.snooze_id||r.sort_key.created_at_utc!==r.created_at_utc||r.sort_key.sort_value!==null&&(typeof r.sort_key.sort_value!=='string'||new TextEncoder().encode(r.sort_key.sort_value).byteLength>8192))
  ||r.amend_unavailable_reason!==(r.can_amend?null:'PAYMENT_CHANGED'))srFail();
 const isCase=r.scope_kind==='CASE';
 if(isCase!==(r.work_id===null)||isCase!==(r.case_id!==null)||isCase!==(r.case_component_id!==null)
  ||isCase!==(r.expected_revision_id===null)||isCase!==(r.physical_timesheet_id===null)
  ||isCase&&(!srVersion(r.case_component_revision,true)||!['LOAN','ADVANCE','MANUAL_DEBT','OVERPAYMENT'].includes(r.case_kind)
   ||!((['LOAN','ADVANCE'].includes(r.case_kind)&&r.snooze_kind==='PAYMENT_ADVANCE_REPAYMENT')||(r.case_kind==='MANUAL_DEBT'&&r.snooze_kind==='MANUAL_DEBT_RECOVERY')||(r.case_kind==='OVERPAYMENT'&&r.snooze_kind==='OVERPAYMENT_RECOVERY')))
  ||!isCase&&(r.case_kind!==null||r.case_component_revision!==null||!['DO_NOT_PAY','BLOCKED_TIMESHEET','TIMESHEET_PAYMENT'].includes(r.snooze_kind))
  ||r.scope_kind==='SEGMENT'&&(r.segment_key===null||r.component_key===null)
  ||r.scope_kind==='EXPENSE_LINE'&&r.component_key===null
  ||r.can_amend&&!isCase&&r.revision_id===null)srFail();
 return r;
}
function srRowKey(row,sort){const type=sort.split('_')[0];return {created_at_utc:row.created_at_utc,id:row.snooze_id,sort_value:type==='CANDIDATE'?row.candidate_display_name.toLowerCase():type==='TIMESHEET'?row.week_ending_date:type==='WHAT'?row.label:type==='UNTIL'?row.until_date:type==='NOTE'?row.note:null};}
function srCompare(a,b,sort){
 const native=v=>v.replace(/[+]00:00$/,'Z').replace(/(?:\.(\d+))?Z$/,(_,n)=>'.'+(n??'').padEnd(6,'0')+'Z');
 const text=(a,b)=>{const x=new TextEncoder().encode(a),y=new TextEncoder().encode(b);for(let i=0;i<Math.min(x.length,y.length);i++)if(x[i]!==y[i])return x[i]<y[i]?-1:1;return Math.sign(x.length-y.length);};
 if(!sort.startsWith('CREATED_')){if((a.sort_value===null)!==(b.sort_value===null))return a.sort_value===null?1:-1;const c=a.sort_value===null?0:text(a.sort_value,b.sort_value);if(c)return sort.endsWith('_DESC')?-c:c;}
 const t=text(native(a.created_at_utc),native(b.created_at_utc));if(t)return sort==='CREATED_ASC'?t:-t;return text(a.id,b.id);
}
function validateSnoozesResponse(p,args,actor){
 const a=normalizeSnoozesInput(args);
 if(!srExact(p,['ok','kind','context','rows','scanned_count','counts','complete','next_cursor'])||p.ok!==true||p.kind!=='SNOOZES'||!srContext(p.context)
  ||p.context.actor_user_id!==actor||!srEqual(p.context.scope,srScope(a))||a.after!==null&&!srEqual(a.after.context,p.context)
  ||!Array.isArray(p.rows)||p.rows.length>a.limit||!srVersion(p.scanned_count)||BigInt(p.scanned_count)>BigInt(a.limit)||typeof p.complete!=='boolean'||!p.complete&&p.scanned_count==='0')srFail();
 if(a.count_only){if(p.rows.length!==0||!srExact(p.counts,['timesheet_snoozes_count','timesheet_expense_snoozes_count'])||!Object.values(p.counts).every(v=>srVersion(v))||BigInt(p.counts.timesheet_snoozes_count)>BigInt(p.scanned_count)||BigInt(p.counts.timesheet_expense_snoozes_count)>BigInt(p.counts.timesheet_snoozes_count))srFail();}
 else if(p.counts!==null||BigInt(p.scanned_count)!==BigInt(p.rows.length))srFail();
 let last=a.after===null?null:srDecodeKey(a.after.key);const seen=new Set();
 for(const row of p.rows){validateSnoozesRow(row);if(a.snooze_id===null&&(a.view==='SNOOZED'&&!row.active||a.view==='HISTORY'&&row.active||a.view==='ACTIVE')||seen.has(row.snooze_id)||a.snooze_id!==null&&row.snooze_id!==a.snooze_id||a.candidate_id!==null&&row.candidate_id!==a.candidate_id
   ||a.client_id!==null&&row.client_id!==a.client_id||a.scope_kind!=='ALL'&&(a.scope_kind==='CASE')!==(row.scope_kind==='CASE')
   ||a.filter!=='ACTIVE'&&(a.filter==='INDEFINITE')!==(row.until_date===null))srFail();seen.add(row.snooze_id);
  const key=a.snooze_id===null?row.sort_key:srRowKey(row,a.sort);if(a.snooze_id===null&&(key===null||!a.sort.startsWith('CANDIDATE_')&&!srEqual(key,srRowKey(row,a.sort)))||a.snooze_id!==null&&row.sort_key!==null)srFail();if(last&&srCompare(key,last,a.sort)<=0)srFail();last=key;
 }
 if(p.complete){if(p.next_cursor!==null)srFail();}
 else if(!srExact(p.next_cursor,['context','key'])||!srEqual(p.next_cursor.context,p.context)||!srKey(p.next_cursor.key)||new TextEncoder().encode(JSON.stringify(p.next_cursor)).byteLength>16384||a.after!==null&&srCompare(srDecodeKey(p.next_cursor.key),srDecodeKey(a.after.key),a.sort)<=0||!a.count_only&&!srEqual(srDecodeKey(p.next_cursor.key),last))srFail();
 return p;
}

export {normalizeSnoozesInput,validateSnoozesRow,validateSnoozesResponse};
