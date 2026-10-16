import {validateAction} from '../contract-correction-v3.mjs';

// Closed adaptation of the commissioned native manual enquiry DTO. None of
// these reads supplies sending authority or changes a native clock/budget.
const uuid=v=>typeof v==='string'&&/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(v);
const hash=v=>typeof v==='string'&&/^[0-9a-f]{64}$/.test(v);
const text=(v,max=500)=>typeof v==='string'&&v.trim().length>0&&[...v].length<=max;
const integer=(v,min,max)=>Number.isInteger(v)&&v>=min&&v<=max;
const revision=v=>typeof v==='string'&&/^(0|[1-9][0-9]*)$/.test(v)
 &&(v.length<19||v.length===19&&v<='9223372036854775807');
const closed=(v,keys)=>v!==null&&typeof v==='object'&&!Array.isArray(v)
 &&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
const nullable=(v,test)=>v===null||test(v);
const date=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)&&v.slice(0,4)!=='0000'
 &&Number.isFinite(Date.parse(v+'T00:00:00Z'))&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
const hhmm=v=>typeof v==='string'&&/^(?:[01][0-9]|2[0-3]):[0-5][0-9]$/.test(v);
const utc=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]\.\d{6}Z$/.test(v)
 &&date(v.slice(0,10))&&Number.isFinite(Date.parse(v));
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
function invalid(){const error=new Error('The enquiry result could not be confirmed.');error.code='CONTACT_MANUAL_ENQUIRY_RESULT_INVALID';error.status=502;throw error;}
function requireThat(condition){if(!condition)invalid();}
function list(value,min,max){requireThat(Array.isArray(value)&&Object.keys(value).length===value.length
 &&Object.keys(value).every((key,index)=>key===String(index))&&value.length>=min&&value.length<=max);return value;}
function action(value){
 if(value===null)return;
 requireThat(closed(value,['kind','target_type','target_id','url','caption','enabled','disabled_reason'])
  &&value.kind==='OPEN_SOURCE_REVIEW'&&value.target_type==='SOURCE_ROW'&&value.caption==='Review shift');
 try{validateAction(value);}catch{invalid();}
}
function cutoff(value,clientId){
 requireThat(closed(value,['version','client_id','contract_id','work_date','source_group_id','settings_authority_sha256','hard_restriction','report_cutoff','routine'])
  &&value.version==='CONTACT_CUTOFF_CLASSIFICATION_V1'&&value.client_id===clientId&&uuid(value.contract_id)
  &&date(value.work_date)&&uuid(value.source_group_id)&&hash(value.settings_authority_sha256));
 const hard=value.hard_restriction,report=value.report_cutoff,routine=value.routine;
 requireThat(closed(hard,['present','reason','resolution_action','source'])&&hard.present===false&&hard.reason===null
  &&hard.resolution_action===null&&hard.source==='NO_STRUCTURED_CONTACT_PROHIBITION_IN_REVIEWED_CURRENT_CLIENT_CONTRACT_SETTINGS');
 requireThat(closed(report,['classification','weekday','local_time','cutoff_at_utc'])&&report.classification==='REPORT_ONLY'
  &&integer(report.weekday,0,6)&&typeof report.local_time==='string'
  &&/^(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.\d{1,6})?$/.test(report.local_time)&&nullable(report.cutoff_at_utc,utc));
 requireThat(closed(routine,['in_scope','reason','manual_override_permitted','recent_completed_weeks','max_work_age_days','policy_revision'])
  &&typeof routine.in_scope==='boolean'&&routine.manual_override_permitted===true
  &&(routine.in_scope?routine.reason===null:['WEEK_NOT_COMPLETED','MAXIMUM_ROUTINE_WORK_AGE','OUTSIDE_RECENT_COMPLETED_WEEKS'].includes(routine.reason))
  &&integer(routine.recent_completed_weeks,1,52)&&integer(routine.max_work_age_days,1,366)&&revision(routine.policy_revision));
}
function scopeResults(value,request){
 const expectedScopes=new Map(),expectedIssues=new Map();
 for(const scope of list(request?.scopes,1,100)){
  requireThat(closed(scope,['candidate_id','client_id','selected_issue_ids','expected_scope_revision','expected_checking_revision'])
   &&uuid(scope.candidate_id)&&uuid(scope.client_id)&&revision(scope.expected_scope_revision)&&revision(scope.expected_checking_revision));
  const key=scope.candidate_id+'/'+scope.client_id;requireThat(!expectedScopes.has(key));expectedScopes.set(key,scope);
  for(const id of list(scope.selected_issue_ids,1,500)){requireThat(uuid(id)&&!expectedIssues.has(id));expectedIssues.set(id,{scope,eligible:null});}
 }
 requireThat(expectedIssues.size<=500);const seenScopes=new Set(),seenIssues=new Set();let revisionsMatch=true;
 for(const scope of list(value,1,100)){
  requireThat(closed(scope,['candidate_id','client_id','scope_revision','checking_revision','permission','members']));
  const key=scope.candidate_id+'/'+scope.client_id,expected=expectedScopes.get(key);
  requireThat(expected&&!seenScopes.has(key)&&revision(scope.scope_revision)&&revision(scope.checking_revision));seenScopes.add(key);
  revisionsMatch&&=scope.scope_revision===expected.expected_scope_revision&&scope.checking_revision===expected.expected_checking_revision;
  requireThat(closed(scope.permission,['allowed','reason'])&&scope.permission.allowed===true&&scope.permission.reason===null);
  for(const member of list(scope.members,1,500)){
   const selected=expectedIssues.get(member?.issue_id);
   requireThat(closed(member,['issue_id','eligible','exclusion_reason','resolution_action','same_purpose_reason','last_accepted_send','next_routine_step','cooldown_until_utc','cooldown_remaining_seconds','cutoff'])
    &&selected?.scope===expected&&!seenIssues.has(member.issue_id)&&typeof member.eligible==='boolean'
    &&(member.eligible?member.exclusion_reason===null:text(member.exclusion_reason))&&text(member.same_purpose_reason)
    &&nullable(member.cooldown_until_utc,utc)&&integer(member.cooldown_remaining_seconds,0,2147483647)
    &&(member.cooldown_until_utc!==null||member.cooldown_remaining_seconds===0));
   action(member.resolution_action);cutoff(member.cutoff,scope.client_id);
   requireThat(member.last_accepted_send===null||closed(member.last_accepted_send,['accepted_at_utc','send_ordinal'])
    &&utc(member.last_accepted_send.accepted_at_utc)&&integer(member.last_accepted_send.send_ordinal,1,2147483647));
   requireThat(member.next_routine_step===null||closed(member.next_routine_step,['sequence','mode','due_at_utc'])
    &&integer(member.next_routine_step.sequence,1,10)&&['MANUAL','AUTOMATIC'].includes(member.next_routine_step.mode)&&utc(member.next_routine_step.due_at_utc));
   selected.eligible=member.eligible;seenIssues.add(member.issue_id);
  }
 }
 requireThat(seenScopes.size===expectedScopes.size&&seenIssues.size===expectedIssues.size);
 return {issues:expectedIssues,revisionsMatch};
}
function managerRow(row,selected){
 requireThat(closed(row,['clientId','clientName','candidateId','displayName','issueId','workDate','sourceStartInstant','start','end','breakMinutes','systemStart','systemEnd','systemBreakMinutes','systemAbsent','issueFamily','candidateRequested'])
  &&row.clientId===selected.scope.client_id&&row.candidateId===selected.scope.candidate_id
  &&text(row.clientName)&&text(row.displayName)&&date(row.workDate)
  &&text(row.sourceStartInstant,100)&&date(row.sourceStartInstant.slice(0,10))&&Number.isFinite(Date.parse(row.sourceStartInstant))
  &&hhmm(row.start)&&hhmm(row.end)&&integer(row.breakMinutes,0,10080)&&nullable(row.systemStart,hhmm)
  &&nullable(row.systemEnd,hhmm)&&nullable(row.systemBreakMinutes,v=>integer(v,0,10080))
  &&typeof row.systemAbsent==='boolean'&&typeof row.candidateRequested==='boolean'
  &&['HOURS_DIFFER','NHSP_ABSENT','SOURCE_ABSENT','HEALTHROSTER_NOT_FINALISED'].includes(row.issueFamily));
 if(row.issueFamily==='HOURS_DIFFER')requireThat(!row.systemAbsent&&row.systemStart!==null&&row.systemEnd!==null&&row.systemBreakMinutes!==null);
}
function envelope(value,request,version,keys){
 requireThat(closed(value,keys)&&value.ok===true&&value.version===version&&value.request_id===request?.request_id
  &&uuid(value.request_id)&&['WORKER','MANAGER'].includes(value.audience)&&value.audience===request.audience
  &&value.reason===request.reason&&text(value.reason,1000));
}
function exactEligiblePartition(seen,issues){
 requireThat(seen.size===[...issues.values()].filter(v=>v.eligible).length);
}
export function decodeContactManualEnquiryRow(value,{candidate_id,client_id}){
 requireThat(closed(value,['version','candidate_id','client_id','scope_revision','checking_revision','checking_facts_sha256','available_issue_ids','actions'])
  &&value.version==='CONTACT_MANUAL_ENQUIRY_ROW_V1'&&uuid(candidate_id)&&uuid(client_id)
  &&value.candidate_id===candidate_id&&value.client_id===client_id&&revision(value.scope_revision)&&revision(value.checking_revision)
  &&hash(value.checking_facts_sha256));
 const issues=new Set();for(const id of list(value.available_issue_ids,0,1000)){requireThat(uuid(id)&&!issues.has(id));issues.add(id);}
 const audiences=new Set();for(const entry of list(value.actions,2,2)){
  requireThat(closed(entry,['operation','audience','label','enabled','disabled_reason'])&&entry.operation==='CONTACT_MANUAL_ENQUIRY_PREVIEW'
   &&['WORKER','MANAGER'].includes(entry.audience)&&!audiences.has(entry.audience)&&entry.label==='New enquiry'
   &&entry.enabled===(issues.size>0)&&entry.disabled_reason===(entry.enabled?null:'NO_OPEN_SOURCE_ISSUES'));audiences.add(entry.audience);
 }
 return value;
}
export function decodeContactManualEnquiryReview(value,request){
 envelope(value,request,'CONTACT_MANUAL_ENQUIRY_REVIEW_V1',['ok','version','request_id','review_id','review_digest','audience','reason','can_accept','scope_results','groups','expires_at_utc']);
 const {issues,revisionsMatch}=scopeResults(value.scope_results,request),seen=new Set(),groupKeys=new Set();
 requireThat(typeof value.can_accept==='boolean');
 for(const group of list(value.groups,0,500)){
  requireThat(closed(group,['group_key','source_cycle_id','recipient_route_id','candidate_generation_id','audience','channel','recipient','selected_issue_ids','canonical_content_sha256','preview'])
   &&hash(group.group_key)&&!groupKeys.has(group.group_key)&&uuid(group.source_cycle_id)&&group.audience===value.audience
   &&hash(group.canonical_content_sha256)&&closed(group.recipient,['candidate_id','manager_address']));groupKeys.add(group.group_key);
  const manager=group.audience==='MANAGER';
  requireThat(manager?group.channel==='EMAIL'&&nullable(group.recipient_route_id,uuid)&&group.candidate_generation_id===null
   &&group.recipient.candidate_id===null&&text(group.recipient.manager_address,320)&&/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(group.recipient.manager_address)
   :group.channel==='PUSH'&&group.recipient_route_id===null&&uuid(group.candidate_generation_id)&&uuid(group.recipient.candidate_id)&&group.recipient.manager_address===null);
  const selectedIds=new Set();
  for(const id of list(group.selected_issue_ids,1,500)){
   const selected=issues.get(id);requireThat(selected?.eligible===true&&!seen.has(id));seen.add(id);selectedIds.add(id);
   if(!manager)requireThat(group.recipient.candidate_id===selected.scope.candidate_id);
  }
  const model=group.preview;
  requireThat(closed(model,['kind','policy_version','renderer_version','structure_version','rows','plain_body','attachment_count'])&&model.attachment_count===0);
  if(manager){
   requireThat(model.kind==='MANAGER_QUERY_MODEL_V1'&&model.policy_version==='1.8.0'&&model.renderer_version==='1.4.0'&&model.structure_version==='1.0.0'&&model.plain_body===null);
   const rows=new Set();for(const row of list(model.rows,1,500)){
    requireThat(selectedIds.has(row?.issueId)&&!rows.has(row.issueId));managerRow(row,issues.get(row.issueId));rows.add(row.issueId);
   }requireThat(rows.size===selectedIds.size);
  }else requireThat(model.kind==='WORKER_QUERY_MODEL_V1'&&model.policy_version==='weekly-source-candidate-push-v1'&&model.renderer_version==='1.0.0'
   &&model.structure_version===null&&list(model.rows,0,0).length===0&&['Open MyTMS to check your Timesheet hours.','Open MyTMS to submit your Timesheet hours.'].includes(model.plain_body));
 }
 exactEligiblePartition(seen,issues);
 requireThat(value.can_accept===(value.groups.length>0&&revisionsMatch));
 requireThat(value.can_accept?uuid(value.review_id)&&hash(value.review_digest)&&utc(value.expires_at_utc)
  :value.review_id===null&&value.review_digest===null&&value.expires_at_utc===null);
 return value;
}
export function decodeContactManualEnquiryReceipt(value,request,review){
 envelope(value,request,'CONTACT_MANUAL_ENQUIRY_RECEIPT_V1',['ok','version','request_id','review_id','review_digest','audience','reason','accepted_at_utc','scope_results','groups','provider_submission_started']);
 requireThat(value.review_id===request.review_id&&uuid(value.review_id)&&value.review_digest===request.review_digest&&hash(value.review_digest)
  &&utc(value.accepted_at_utc)&&value.provider_submission_started===false);
 const {issues}=scopeResults(value.scope_results,request),seen=new Set(),keys=new Set(),logicalOwners=new Map();
 for(const group of list(value.groups,1,500)){
  requireThat(closed(group,['group_key','message_intent_id','members'])&&hash(group.group_key)&&!keys.has(group.group_key)&&uuid(group.message_intent_id));keys.add(group.group_key);
  for(const member of list(group.members,1,500)){
   requireThat(closed(member,['issue_id','logical_contact_key','send_ordinal','contact_kind'])&&issues.get(member.issue_id)?.eligible===true
    &&!seen.has(member.issue_id)&&hash(member.logical_contact_key)&&integer(member.send_ordinal,1,2147483647)&&member.contact_kind==='EXPLICIT_MANUAL');seen.add(member.issue_id);
   const previous=logicalOwners.get(member.logical_contact_key),owner={group:group.group_key,ordinal:member.send_ordinal};
   requireThat(!previous||same(previous,owner));logicalOwners.set(member.logical_contact_key,owner);
  }
 }
 exactEligiblePartition(seen,issues);
 if(review){
  decodeContactManualEnquiryReview(review,request);
  requireThat(review.can_accept&&review.review_id===value.review_id&&review.review_digest===value.review_digest&&same(review.scope_results,value.scope_results));
  requireThat(value.groups.length===review.groups.length);
  for(const group of value.groups){const original=review.groups.find(g=>g.group_key===group.group_key);
   requireThat(original&&same([...group.members.map(m=>m.issue_id)].sort(),[...original.selected_issue_ids].sort()));}
 }
 // An old immutable accepted receipt is valid after newer revisions or clocks.
 // No Date.now check, fresh ordinal, mutated replay flag or delivery claim.
 return value;
}
