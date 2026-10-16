// Generated from sole Health closed DTO source SHA256 57034c72103454ed956e7cbd7fbd9425b83171152a684237a94425147b33b087. Do not edit.
(function(root){'use strict';
// Health-owned shared closed DTO. V6 final content afaf162e59b2e7901ef0dbc3f484ce51c741f0971557f80fdc877bb8b9ecaa88.
// No pricing, cash conversion, actor authority or persistence lives in this decoder.
const CONTRACT='TIMESHEET_CONTRACT_CORRECTION_V3', DAILY='DAILY_RATE_CORRECTION_V1';
const MAX_I64=9223372036854775807n;
const invalid=p=>{throw new TypeError(`CORRECTION_CONTRACT_INVALID:${p}`);};
const text=(v,p,max=500)=>{if(typeof v!=='string'||v.trim()===''||[...v].length>max||/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/u.test(v))invalid(p);return v;};
function closed(v,keys,p='$'){
  if(v===null||typeof v!=='object'||Array.isArray(v)||![Object.prototype,null].includes(Object.getPrototypeOf(v)))invalid(p);
  const actual=Object.keys(v).sort(),expected=[...keys].sort();
  if(actual.length!==expected.length||actual.some((k,i)=>k!==expected[i]))invalid(p);return v;
}
const uuid=(v,p)=>{if(typeof v!=='string'||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(v))invalid(p);return v;};
const hash=(v,p)=>{if(typeof v!=='string'||!/^[0-9a-f]{64}$/.test(v))invalid(p);return v;};
const revision=(v,p='$',zero=true)=>{if(typeof v!=='string'||!/^(0|[1-9][0-9]*)$/.test(v)||BigInt(v)>MAX_I64||(!zero&&v==='0'))invalid(p);return v;};
const bool=(v,p)=>{if(typeof v!=='boolean')invalid(p);};
const choice=(v,values,p)=>{if(!values.includes(v))invalid(p);};
const nullable=(v,validator,p)=>{if(v!==null)validator(v,p);};
const date=(v,p)=>{if(typeof v!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(v)||v.slice(0,4)==='0000'||!Number.isFinite(Date.parse(v+'T00:00:00Z'))||new Date(v+'T00:00:00Z').toISOString().slice(0,10)!==v)invalid(p);};
const array=(v,validator,p,key=x=>JSON.stringify(x))=>{if(!Array.isArray(v))invalid(p);const seen=new Set();v.forEach((x,i)=>{validator(x,`${p}[${i}]`);const k=key(x);if(seen.has(k))invalid(p+'.duplicate');seen.add(k);});};
const VERSIONS=Object.freeze({CONTRACT,DAILY});
function normalizeReason(note,purpose='CONTRACT_CORRECTION'){
  if(!['CONTRACT_CORRECTION','DAILY_RATE_CORRECTION'].includes(purpose))invalid('$.purpose');
  if(note!==undefined&&typeof note!=='string')invalid('$.reason');
  note=(note??'').trim();if([...note].length>1969)invalid('$.reason');
  const prefix=purpose==='CONTRACT_CORRECTION'?'Correcting the Contract':'Applying updated rates';
  return prefix+(note?' — '+note:'');
}
function validatePreviewRequest(v,{daily=false,service=false}={}){
  closed(v,['version','request_id','timesheet_id',...(!daily?['to_contract_id']:[]),...(Object.hasOwn(v??{},'reason')?['reason']:[]),...(service?['actor_user_id']:[])]);
  if(v.version!==(daily?DAILY:CONTRACT))invalid('$.version');uuid(v.request_id,'$.request_id');uuid(v.timesheet_id,'$.timesheet_id');
  if(!daily)uuid(v.to_contract_id,'$.to_contract_id');if(service)uuid(v.actor_user_id,'$.actor_user_id');normalizeReason(v.reason,daily?'DAILY_RATE_CORRECTION':'CONTRACT_CORRECTION');return v;
}
function validateAcceptRequest(v,{daily=false,service=false}={}){
  closed(v,['version','request_id','operation_id','review_id','review_digest','expected_state_revision',...(service?['actor_user_id']:[])]);
  if(v.version!==(daily?DAILY:CONTRACT))invalid('$.version');for(const k of ['request_id','operation_id','review_id'])uuid(v[k],'$.'+k);
  if(service)uuid(v.actor_user_id,'$.actor_user_id');hash(v.review_digest,'$.review_digest');revision(v.expected_state_revision,'$.expected_state_revision',false);return v;
}
function validateReadRequest(v,{daily=false}={}){
  closed(v,['version','action','operation_id','request_id','actor_user_id']);if(v.version!==(daily?DAILY:CONTRACT)||v.action!=='READ_RESULT')invalid('$.action');
  for(const k of ['operation_id','request_id','actor_user_id'])uuid(v[k],'$.'+k);return v;
}
const variants=[
 ['READ_RESULT','CONTRACT_MOVE_OPERATION','Check saved result',[]],['READ_RESULT','SOURCE_PAY_DECISION','Check saved result',[]],
 ['READ_RESULT','NATIVE_UNPROTECT_RUN','Check saved Unprotect result',['decision_id','member_key']],
 ['OPEN_TIMESHEET','TIMESHEET','Open Timesheet',[]],['OPEN_INVOICE','INVOICE','Open invoice',[]],['OPEN_INVOICE','INVOICE','Open credit note',[]],
 ['OPEN_DRAFT','BANKING_RUN','Open Draft',[]],['CHECK_PAYMENT_RESULT','NATIVE_TRANSFER','Open payment',[]],
 ['REVIEW_EARLIER_PAYMENT','CONTRACT_MOVE_HISTORY','Review earlier payment',[]],['REVIEW_EARLIER_PAYMENT','SOURCE_PAY_DECISION_HISTORY','Review earlier payment',[]],
 ['REVIEW_RECOVERY','FINANCE_CASE','Review recovery',[]],['REVIEW_RATES','TIMESHEET','Review rates',[]],
 ['RESUME_FINANCIAL','CONTRACT_MOVE_OPERATION','Update pay result',[]],['RESUME_FINANCIAL','SOURCE_PAY_DECISION','Update pay result',[]],
 ['WAIT_FOR_FINAL_REPORT','SOURCE_REPORT_PERIOD','Open Imports',['client_id','period_start','period_end']],
 ['RESUME_UNPROTECT','NATIVE_UNPROTECT_RUN','Retry Unprotect',['decision_id','member_key']],['OPEN_SOURCE_REVIEW','SOURCE_ROW','Review shift',[]]
];
const ACTION_VARIANTS=Object.freeze(variants.map(v=>Object.freeze([v[0],v[1],v[2],Object.freeze(v[3])])));
function validateAction(v,p='$'){
  if(v===null||typeof v!=='object')invalid(p);const row=variants.find(r=>r[0]===v.kind&&r[1]===v.target_type&&r[2]===v.caption);if(!row)invalid(p+'.variant');
  closed(v,['kind','target_type','target_id','url','caption','enabled','disabled_reason',...row[3]],p);bool(v.enabled,p+'.enabled');
  text(v.url,p+'.url',2048);if(!v.url.startsWith('/')||v.url.startsWith('//')||/[\\\u0000-\u0020]/u.test(v.url))invalid(p+'.url');
  if(v.enabled){if(v.disabled_reason!==null)invalid(p+'.disabled_reason');}else text(v.disabled_reason,p+'.disabled_reason');
  if(v.kind==='WAIT_FOR_FINAL_REPORT'){if(v.target_id!==null)invalid(p+'.target_id');uuid(v.client_id,p+'.client_id');date(v.period_start,p+'.period_start');date(v.period_end,p+'.period_end');if(v.period_end<v.period_start)invalid(p+'.period');}
  else uuid(v.target_id,p+'.target_id');
  if(row[3].includes('decision_id')){uuid(v.decision_id,p+'.decision_id');text(v.member_key,p+'.member_key',200);}return v;
}
const actionKey=v=>JSON.stringify([v.kind,v.target_type,v.target_id,v.decision_id??null,v.member_key??null,v.client_id??null,v.period_start??null,v.period_end??null]);
const actions=(v,p)=>array(v,validateAction,p,actionKey);
function validateEffect(v,p='$'){if(v?.visibility==='REDACTED')closed(v,['visibility'],p);else{closed(v,['visibility','text'],p);if(v.visibility!=='FULL')invalid(p+'.visibility');text(v.text,p+'.text');}return v;}
function summary(v,p){closed(v,['worked_facts_preserved','pay_effect','invoice_effect'],p);bool(v.worked_facts_preserved,p+'.worked_facts_preserved');validateEffect(v.pay_effect,p+'.pay_effect');validateEffect(v.invoice_effect,p+'.invoice_effect');}
function blocker(v,p){closed(v,['code','native_owner','field_key','permitted_action'],p);text(v.code,p+'.code');choice(v.native_owner,['HEALTH','BANKING'],p+'.native_owner');nullable(v.field_key,text,p+'.field_key');nullable(v.permitted_action,validateAction,p+'.permitted_action');}
function confirm(v,p,version){closed(v,['request_version','review_id','review_digest','expected_state_revision'],p);if(v.request_version!==version)invalid(p+'.request_version');uuid(v.review_id,p+'.review_id');hash(v.review_digest,p+'.review_digest');revision(v.expected_state_revision,p+'.expected_state_revision',false);}
function validatePreview(v,{daily=false}={}){
  closed(v,['version','operation_id','review_id','review_digest','state_revision','ready_to_confirm','current_timesheet_id',...(!daily?['to_contract_id']:[]),'summary','known_blockers','confirm','links']);
  if(v.version!==(daily?DAILY:CONTRACT))invalid('$.version');for(const k of ['operation_id','review_id','current_timesheet_id'])uuid(v[k],'$.'+k);if(!daily)uuid(v.to_contract_id,'$.to_contract_id');
  hash(v.review_digest,'$.review_digest');revision(v.state_revision,'$.state_revision',false);bool(v.ready_to_confirm,'$.ready_to_confirm');summary(v.summary,'$.summary');array(v.known_blockers,blocker,'$.known_blockers');actions(v.links,'$.links');
  if(v.ready_to_confirm){confirm(v.confirm,'$.confirm',v.version);if(!v.summary.worked_facts_preserved||v.known_blockers.length||v.confirm.review_id!==v.review_id||v.confirm.review_digest!==v.review_digest||v.confirm.expected_state_revision!==v.state_revision)invalid('$.confirm.binding');}
  else if(v.confirm!==null)invalid('$.confirm');return v;
}
function validateResult(v,{financialVisible=true,billingVisible=true,retainedNativePending=false}={}){
  closed(v,['version','purpose','operation_id','state','state_revision','placement','financial','billing','next_actions','links']);
  if(!((v.version===CONTRACT&&v.purpose==='CONTRACT_CORRECTION')||(v.version===DAILY&&v.purpose==='DAILY_RATE_CORRECTION')))invalid('$.version.purpose');
  uuid(v.operation_id,'$.operation_id');choice(v.state,['REVIEWED','COMPLETE','SUPERSEDED',...(retainedNativePending?['PENDING_PLACEMENT']:[])],'$.state');revision(v.state_revision,'$.state_revision',false);
  const p=v.placement;closed(p,['status','original_timesheet_id','current_timesheet_id','placement_revision','receipt_id'],'$.placement');choice(p.status,['NOT_COMMITTED','COMMITTED',...(retainedNativePending?['NATIVE_PENDING']:[])],'$.placement.status');uuid(p.original_timesheet_id,'$.placement.original_timesheet_id');nullable(p.current_timesheet_id,uuid,'$.placement.current_timesheet_id');revision(p.placement_revision,'$.placement.placement_revision');nullable(p.receipt_id,uuid,'$.placement.receipt_id');
  if((p.status==='COMMITTED')!==(p.receipt_id!==null)||p.status!=='NATIVE_PENDING'&&p.current_timesheet_id===null||p.status==='COMMITTED'&&p.placement_revision==='0'||v.state==='COMPLETE'&&p.status!=='COMMITTED'||(p.status==='NATIVE_PENDING')!==(v.state==='PENDING_PLACEMENT'))invalid('$.placement.binding');
  if(v.purpose==='DAILY_RATE_CORRECTION'&&p.current_timesheet_id!==p.original_timesheet_id)invalid('$.placement.daily_same_identity');
  const f=v.financial;closed(f,['status','command_id','cut_id','summary'],'$.financial');choice(f.status,['NOT_REQUIRED','REGISTERED','PROCESSING','APPLIED','ACTION_REQUIRED'],'$.financial.status');nullable(f.command_id,uuid,'$.financial.command_id');nullable(f.cut_id,uuid,'$.financial.cut_id');nullable(f.summary,validateEffect,'$.financial.summary');
  if(f.status==='NOT_REQUIRED'&&(f.command_id!==null||f.cut_id!==null))invalid('$.financial.not_required');if(!financialVisible&&(f.command_id!==null||f.cut_id!==null||f.summary!==null&&f.summary.visibility!=='REDACTED'))invalid('$.financial.redaction');
  const b=v.billing;closed(b,['status','revision','group_id','visible_summary'],'$.billing');choice(b.status,['NOT_REQUIRED','PREPARING_DRAFTS','DRAFTS_READY','PARTIALLY_ISSUED','ISSUED','RETRY_WAIT','ACTION_REQUIRED'],'$.billing.status');revision(b.revision,'$.billing.revision');nullable(b.group_id,uuid,'$.billing.group_id');nullable(b.visible_summary,validateEffect,'$.billing.visible_summary');
  if(b.status==='NOT_REQUIRED'&&(b.group_id!==null||b.visible_summary!==null))invalid('$.billing.not_required');if(!billingVisible&&(b.group_id!==null||b.visible_summary!==null&&b.visible_summary.visibility!=='REDACTED'))invalid('$.billing.redaction');
  actions(v.next_actions,'$.next_actions');actions(v.links,'$.links');const all=[...v.next_actions,...v.links];if(new Set(all.map(actionKey)).size!==all.length)invalid('$.actions.duplicate');
  if(v.state==='COMPLETE'&&all.some(a=>['REVIEW_RATES','REVIEW_EARLIER_PAYMENT','RESUME_FINANCIAL'].includes(a.kind)))invalid('$.actions.completed_repair');return v;
}
function validateCurrentPlacement(v){closed(v,['version','requested_timesheet_id','family_root_id','current_timesheet_id','current_contract_id','placement_revision','placement_receipt_id','legacy_origin']);if(v.version!=='CURRENT_CONTRACT_PLACEMENT_V1')invalid('$.version');for(const k of ['requested_timesheet_id','family_root_id','current_timesheet_id'])uuid(v[k],'$.'+k);nullable(v.current_contract_id,uuid,'$.current_contract_id');revision(v.placement_revision,'$.placement_revision');nullable(v.placement_receipt_id,uuid,'$.placement_receipt_id');bool(v.legacy_origin,'$.legacy_origin');if(v.legacy_origin?(v.placement_revision!=='0'||v.placement_receipt_id!==null):(v.placement_revision==='0'||v.placement_receipt_id===null))invalid('$.placement.binding');return v;}

root.CloudTMSCorrectionContractV3=Object.freeze({closed,revision,VERSIONS,normalizeReason,validatePreviewRequest,validateAcceptRequest,validateReadRequest,ACTION_VARIANTS,validateAction,validateEffect,validatePreview,validateResult,validateCurrentPlacement});
})(typeof window!=='undefined'?window:globalThis);
