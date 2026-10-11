/* FC14 closed read contracts, native scalar facts only. */
const CF_UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const CF_INT=/^(?:0|[1-9]\d{0,18})$/;
const CF_MONEY=/^-?(?:0|[1-9]\d{0,15})(?:\.\d{1,2})?$/;
const CF_SORTS=Object.freeze({CASE_AUDIT:['WHEN','EVENT','WHO','SOURCE','AMOUNT','REASON','DETAILS'],CANDIDATE_CASE_REPORT:['ITEM','CLIENT','ORIGINAL','OUTSTANDING','WEEKLY_AMOUNT','NEXT_DATE','SNOOZED_UNTIL','STATUS','NOTES']});
const CF_SOURCES=['CREATE_REQUEST','EDIT_REQUEST','PAUSE_REQUEST','PAUSE_OUTCOME','WRITE_OFF_REQUEST','CASE_EVENT','NET_REQUEST','HOLD_TRANSITION','FINANCIAL_EFFECT','SNOOZE_EVENT'];
const cfRecord=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const cfExact=(v,keys)=>cfRecord(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
const cfBytes=v=>new TextEncoder().encode(typeof v==='string'?v:JSON.stringify(v)).byteLength;
const cfSame=(a,b)=>JSON.stringify(cfCanonical(a))===JSON.stringify(cfCanonical(b));
function cfCanonical(v){if(Array.isArray(v))return v.map(cfCanonical);if(cfRecord(v))return Object.fromEntries(Object.keys(v).sort().map(k=>[k,cfCanonical(v[k])]));return v;}
function cfField(v,t){if(t.endsWith('?'))return v===null||cfField(v,t.slice(0,-1));
 if(t==='uuid')return typeof v==='string'&&CF_UUID.test(v);
 if(t==='integer')return typeof v==='string'&&CF_INT.test(v)&&BigInt(v)<=9223372036854775807n;
 if(t==='positive')return cfField(v,'integer')&&v!=='0';
 if(t==='money')return typeof v==='string'&&CF_MONEY.test(v);
 if(t==='aggregateMoney')return typeof v==='string'&&/^-?(?:0|[1-9]\d{0,35})(?:\.\d{1,2})?$/.test(v);
 if(t==='text')return typeof v==='string'&&cfBytes(v)<=8192;
 if(t==='boolean')return typeof v==='boolean';
 if(t==='timestamp')return typeof v==='string'&&v.length<=40&&/^\d{4}-\d{2}-\d{2}T/.test(v)&&Number.isFinite(Date.parse(v));
 if(t==='date')return typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v+'T00:00:00Z'));
 if(t==='sortKey')return Array.isArray(v)&&v.length===2&&typeof v[0]==='boolean'&&(v[0]?v[1]===null:typeof v[1]==='string'&&cfBytes(v[1])<=16384);
 return false;
}
function cfShape(v,schema){return cfExact(v,Object.keys(schema))&&Object.entries(schema).every(([k,t])=>cfField(v[k],t));}
const CF_IDENTITY={agency_id:'uuid',actor_user_id:'uuid',owner_epoch:'positive'};
const CF_REPORT_CONTEXT={...CF_IDENTITY,local_date:'date',official_pay_date:'date',pay_week_start:'date',workbench_revision:'integer',adoption_revision:'integer',valuation_policy_id:'uuid?'};
const CF_SCOPE={kind:'text',candidate_id:'uuid',case_id:'uuid?',active_only:'boolean?',sort:'text',direction:'text'};
function cfScope(kind,args){return {kind,candidate_id:args.candidate_id,case_id:kind==='CASE_AUDIT'?args.case_id:null,active_only:kind==='CANDIDATE_CASE_REPORT'?args.active_only:null,sort:args.sort,direction:args.direction};}
function cfSortKey(v,sort){if(!cfField(v,'sortKey'))return false;if(v[0])return true;
 if(['AMOUNT','ORIGINAL','OUTSTANDING','WEEKLY_AMOUNT'].includes(sort))return /^[01]:[0-9]{18}$/.test(v[1]);
 if(sort==='WHEN')return /^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}$/.test(v[1]);
 if(['NEXT_DATE','SNOOZED_UNTIL'].includes(sort))return cfField(v[1],'date');return /^(?:[0-9a-f]{2})*$/.test(v[1]);}
function cfCursor(v,kind,args){return cfExact(v,['context','scope','section','key','source','source_id','event_ordinal'])
 &&cfShape(v.context,kind==='CASE_AUDIT'?CF_IDENTITY:CF_REPORT_CONTEXT)&&cfShape(v.scope,CF_SCOPE)&&cfSame(v.scope,cfScope(kind,args))
 &&Number.isInteger(v.section)&&(kind==='CASE_AUDIT'?v.section===0:v.section>=1&&v.section<=7)
 &&cfSortKey(v.key,args.sort)&&cfField(v.source_id,'uuid')&&['1','2'].includes(v.event_ordinal)
 &&(kind==='CASE_AUDIT'?CF_SOURCES.includes(v.source):['CASE','TIMESHEET_SNOOZE'].includes(v.source))&&cfBytes(v)<=16384;}
export function caseFinanceInput(kind,args){
 if(!Object.hasOwn(CF_SORTS,kind)||!cfExact(args,['candidate_id',...(kind==='CASE_AUDIT'?['case_id']:['active_only']),'sort','direction','limit','after','include_count'])
  ||!cfField(args.candidate_id,'uuid')||(kind==='CASE_AUDIT'?!cfField(args.case_id,'uuid'):typeof args.active_only!=='boolean')
  ||!CF_SORTS[kind].includes(args.sort)||!['ASC','DESC'].includes(args.direction)||!Number.isInteger(args.limit)||args.limit<1||args.limit>100
  ||typeof args.include_count!=='boolean'||args.after!==null&&!cfCursor(args.after,kind,args)||cfBytes(args)>32768)return false;
 return true;
}
export function caseFinanceQuery(kind,params){
 const keys=['candidate_id',...(kind==='CASE_AUDIT'?['case_id']:['active_only']),'sort','direction','limit','after','include_count'];
 if([...params.keys()].some(k=>k!=='kind'&&!keys.includes(k)||params.getAll(k).length!==1)||keys.some(k=>!params.has(k)))throw TypeError('Invalid read');
 const args={};for(const k of keys){const value=params.get(k);if(['active_only','include_count','after','limit'].includes(k)){try{args[k]=JSON.parse(value);}catch{throw TypeError('Invalid read');}}else args[k]=value;}
 if(!caseFinanceInput(kind,args))throw TypeError('Invalid read');return {kind,args};
}
const CF_AUDIT_ROW={source:'text',source_id:'uuid',event_ordinal:'positive',case_id:'uuid',candidate_id:'uuid',occurred_at_utc:'timestamp',event_label:'text',actor_user_id:'uuid?',actor_kind:'text',actor_name:'text',source_label:'text',display_amount:'money?',source_amount_ex_vat:'money?',target_amount_ex_vat:'money?',target_amount_vat:'money?',target_amount_inc_vat:'money?',reason:'text?',details:'text',section:'section',sort_key:'sortKey'};
const CF_REPORT_ROW={row_id:'uuid',source:'text',case_id:'uuid?',candidate_id:'uuid',item:'text',created_at_utc:'timestamp?',client:'text?',original:'money?',funded:'money?',recovered:'money?',written_off:'money?',reserved:'money?',outstanding:'money?',pending_payout:'money?',weekly_amount:'money?',next_date:'date?',snoozed_until:'date?',has_active_snooze:'boolean?',indefinite_snooze:'boolean?',status:'text',notes:'text?',gap_code:'text?',work_id:'uuid?',physical_timesheet_id:'uuid?',section:'section',sort_key:'sortKey'};
function cfRow(v,kind){const schema=kind==='CASE_AUDIT'?CF_AUDIT_ROW:CF_REPORT_ROW;if(!cfExact(v,Object.keys(schema)))return false;
 return Object.entries(schema).every(([k,t])=>t==='section'?Number.isInteger(v[k])&&(kind==='CASE_AUDIT'?v[k]===0:v[k]>=1&&v[k]<=7):cfField(v[k],t));}
function cfCompare(a,b,direction){if(a.section!==b.section)return a.section-b.section;let c=(a.key[0]===b.key[0]?0:a.key[0]?1:-1);
 if(!c&&a.key[1]!==b.key[1])c=a.key[1]===null?-1:b.key[1]===null?1:a.key[1]>b.key[1]?1:-1;
 if(c)return direction==='DESC'?-c:c;
 if(a.source!==b.source)return a.source>b.source?1:-1;if(a.source_id!==b.source_id)return a.source_id>b.source_id?1:-1;
 return Number(a.event_ordinal)-Number(b.event_ordinal);}
export function caseFinanceResult(payload,kind,args,actorId){
 if(!caseFinanceInput(kind,args)||!cfExact(payload,['kind','context','scope','header','rows','complete','next_cursor','total_count','summary'])
  ||payload.kind!==kind||!cfShape(payload.context,kind==='CASE_AUDIT'?CF_IDENTITY:CF_REPORT_CONTEXT)
  ||payload.context.actor_user_id!==actorId||!cfSame(payload.scope,cfScope(kind,args))
  ||args.after!==null&&!cfSame(args.after.context,payload.context)
  ||!cfShape(payload.header,{candidate_name:'text?',candidate_reference:'text?',case_label:'text?'})
  ||!Array.isArray(payload.rows)||payload.rows.length>args.limit||typeof payload.complete!=='boolean'
  ||!cfField(payload.total_count,'integer?')||cfBytes(payload)>120000)throw TypeError('Invalid read');
 if(args.include_count?(payload.total_count===null||BigInt(payload.total_count)<BigInt(payload.rows.length)):payload.total_count!==null)throw TypeError('Invalid read');
 if(kind==='CASE_AUDIT'||!args.include_count){if(payload.summary!==null)throw TypeError('Invalid read');}
 else if(!cfShape(payload.summary,{payment_advances_outstanding:'aggregateMoney',overpayments_outstanding:'aggregateMoney',snoozed_items:'integer?'}))throw TypeError('Invalid read');
 let prior=args.after;const ids=new Set();for(const row of payload.rows){
  if(!cfRow(row,kind)||!cfSortKey(row.sort_key,args.sort)||row.candidate_id!==args.candidate_id||cfBytes(row)>90000)throw TypeError('Invalid read');
  const id=kind==='CASE_AUDIT'?row.source_id:row.row_id;
  if(kind==='CASE_AUDIT'&&(row.case_id!==args.case_id||!CF_SOURCES.includes(row.source)||!['1','2'].includes(row.event_ordinal)
   ||!['HUMAN','SYSTEM','UNKNOWN'].includes(row.actor_kind)||(row.actor_kind==='HUMAN')!==(row.actor_user_id!==null)
   ||row.actor_kind==='SYSTEM'&&row.actor_name!=='Banking Pay'||row.actor_kind==='UNKNOWN'&&row.actor_name!=='Unknown'))throw TypeError('Invalid read');
  if(kind==='CANDIDATE_CASE_REPORT'&&(!['CASE','TIMESHEET_SNOOZE'].includes(row.source)||(row.source==='CASE')!==(row.case_id!==null)
   ||row.source==='CASE'&&(row.work_id!==null||row.physical_timesheet_id!==null||['original','funded','recovered','written_off','reserved','outstanding'].some(k=>row[k]===null))
   ||row.source==='TIMESHEET_SNOOZE'&&(row.work_id===null||row.physical_timesheet_id===null||row.section!==7)
   ||row.source==='CASE'&&row.case_id!==row.row_id))throw TypeError('Invalid read');
  const tie={section:row.section,key:row.sort_key,source:row.source,source_id:id.toLowerCase(),event_ordinal:kind==='CASE_AUDIT'?row.event_ordinal:'1'};
  const identity=tie.source+':'+tie.source_id+':'+tie.event_ordinal;if(ids.has(identity)||prior!==null&&cfCompare(tie,prior,args.direction)<=0)throw TypeError('Invalid read');ids.add(identity);prior=tie;
 }
 if(payload.complete){if(payload.next_cursor!==null)throw TypeError('Invalid read');}
 else if(payload.rows.length===0||!cfCursor(payload.next_cursor,kind,args)||!cfSame(payload.next_cursor.context,payload.context)
  ||!cfSame(Object.fromEntries(['section','key','source','source_id','event_ordinal'].map(k=>[k,payload.next_cursor[k]])),prior))throw TypeError('Invalid read');
 return payload;
}
export const CASE_FINANCE_SORTS=CF_SORTS;
