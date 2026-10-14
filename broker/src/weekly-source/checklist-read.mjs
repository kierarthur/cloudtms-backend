// Closed transport/presentation boundary for the single planned native U3 reader.
// No scope discovery, reason selection, monetary calculation or mutation here.
import {projectChecklistRow,projectChecklistItem,ChecklistSourceError} from './healthroster-checklist-source.mjs';
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const HASH=/^[0-9a-f]{64}$/,MAIN_SORTS=new Set(['week_ending','client','group','checked','completed']);
const QUERY_KEYS=new Set(['command','source_group_id','client_id','source_cycle_id','week_ending','cursor','sort','direction','limit','anchor']);
export class ChecklistReadError extends Error{constructor(code,status=400){super(code);this.code=code;this.status=status;}}
const fail=(code,status)=>{throw new ChecklistReadError(code,status);};
function closed(value,keys){if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).sort().join('|')!==[...keys].sort().join('|'))fail('WEEKLY_SOURCE_CHECKLIST_RESPONSE_INVALID',502);}
function civilDate(value){if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value))return false;const date=new Date(value+'T00:00:00Z');return Number.isFinite(date.getTime())&&date.toISOString().slice(0,10)===value;}
export function checklistReadRequest(url){
 const request={};for(const [key,value] of url.searchParams){if(!QUERY_KEYS.has(key)||Object.hasOwn(request,key))fail('WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID');request[key]=value;}
 request.command??='CHECKLIST';if(!['CHECKLIST','CHECKLIST_ISSUES'].includes(request.command))fail('WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID');
 for(const key of ['source_group_id','client_id','source_cycle_id'])if(Object.hasOwn(request,key)){if(!UUID.test(request[key]))fail('WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID');request[key]=request[key].toLowerCase();}
 if(Object.hasOwn(request,'week_ending')&&!civilDate(request.week_ending))fail('WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID');
 for(const key of ['cursor','anchor'])if(Object.hasOwn(request,key)&&(request[key].length<1||request[key].length>4096||/[\x00-\x1f]/.test(request[key])))fail('WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID');
 request.sort??=request.command==='CHECKLIST'?'week_ending':'worker';request.direction??=request.command==='CHECKLIST'?'DESC':'ASC';
 if(!(request.command==='CHECKLIST'?MAIN_SORTS.has(request.sort):request.sort==='worker')||!['ASC','DESC'].includes(request.direction)||(request.command==='CHECKLIST_ISSUES'&&request.direction!=='ASC'))fail('WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID');
 const limit=request.limit??'50';if(!/^[1-9][0-9]{0,2}$/.test(limit)||Number(limit)>100)fail('WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID');request.limit=Number(limit);
 if(request.command==='CHECKLIST_ISSUES'&&!['source_group_id','client_id','source_cycle_id','week_ending'].every(k=>Object.hasOwn(request,k)))fail('WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID');
 if(request.cursor&&request.anchor)fail('WEEKLY_SOURCE_CHECKLIST_REQUEST_INVALID');
 return request;
}
export function decodeChecklistRead(value,request,user){
 const actor={actor_user_id:user.id,permission_view:user.payView?'PAY_READONLY':'ADMIN',active:true};
 closed(value,['ok','contract','command','actor_user_id','permission_view','binding','read_revision','rows','items','has_more','next_cursor','refresh_required','anchor']);
 if(value.ok!==true||value.contract!=='WEEKLY_SOURCE_CHECKLIST_V1'||value.command!==request.command||value.actor_user_id!==actor.actor_user_id||value.permission_view!==actor.permission_view
  ||!HASH.test(value.binding)||!HASH.test(value.read_revision)||!Array.isArray(value.rows)||!Array.isArray(value.items)||value.rows.length>request.limit||value.items.length>request.limit
  ||typeof value.has_more!=='boolean'||typeof value.refresh_required!=='boolean'||(value.anchor!==null&&(typeof value.anchor!=='string'||value.anchor.length<1||value.anchor.length>4096))
  ||(value.has_more?(typeof value.next_cursor!=='string'||!value.next_cursor||value.next_cursor.length>4096):value.next_cursor!==null))fail('WEEKLY_SOURCE_CHECKLIST_RESPONSE_INVALID',502);
 if(value.refresh_required)fail('WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED',409);
 if((request.command==='CHECKLIST'&&value.items.length)||(request.command==='CHECKLIST_ISSUES'&&value.rows.length))fail('WEEKLY_SOURCE_CHECKLIST_RESPONSE_INVALID',502);
 if(new TextEncoder().encode(JSON.stringify(value)).byteLength>200000)fail('WEEKLY_SOURCE_CHECKLIST_RESPONSE_TOO_LARGE',502);
 let rows,items;try{rows=value.rows.map(row=>projectChecklistRow(row,actor));items=value.items.map(item=>projectChecklistItem(item,actor));}
 catch(error){if(error instanceof ChecklistSourceError)fail('WEEKLY_SOURCE_CHECKLIST_RESPONSE_INVALID',502);throw error;}
 if(rows.some(r=>r===null)||items.some(r=>r===null))fail('WEEKLY_SOURCE_CHECKLIST_NATIVE_FILTER_NOT_APPLIED',502);
 if(new Set(rows.map(r=>r.row_id)).size!==rows.length||new Set(items.map(r=>r.item_id)).size!==items.length)fail('WEEKLY_SOURCE_CHECKLIST_RESPONSE_INVALID',502);
 for(const record of [...rows,...items])for(const key of ['source_group_id','client_id','source_cycle_id','week_ending'])if(request[key]&&record.scope[key]!==request[key])fail('WEEKLY_SOURCE_CHECKLIST_SCOPE_CHANGED',502);
 for(const row of rows){if(![true,false].includes(row.completed.value)||(!user.payView&&![true,false].includes(row.checked.value)))fail('WEEKLY_SOURCE_CHECKLIST_RESPONSE_INVALID',502);
  // A reduced native page must already be text-only. Do not hide leaked action
  // authority after pagination and claim that the native contract was safe.
  const original=value.rows.find(r=>r.row_id===row.row_id);
  if(user.payView&&(original.admin_actions.length||original.checked.link||original.completed.link||original.pay_completed.link))fail('WEEKLY_SOURCE_CHECKLIST_PAY_ACTION_FORBIDDEN',502);
 }
 if(user.payView&&value.items.some(item=>item.action!==null))fail('WEEKLY_SOURCE_CHECKLIST_PAY_ACTION_FORBIDDEN',502);
 return {...value,rows,items};
}
