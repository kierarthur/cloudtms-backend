import { sourceNativeClosed as closed, sourceNativePositiveBigint as revision,decodeSourceNativeComposedResult,decodeSourceNativeDeferredReleaseProof } from './source-native-contract.mjs';
import { validateAction as sharedValidateAction } from '../contract-correction-v3.mjs';

const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HASH=/^[0-9a-f]{64}$/;const DECIMAL=/^-?(0|[1-9][0-9]*)(\.[0-9]+)?$/;
const uuid=v=>typeof v==='string'&&UUID.test(v);const hash=v=>typeof v==='string'&&HASH.test(v);
const text=v=>typeof v==='string'&&v.trim().length>0&&[...v].length<=512;
const date=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)&&v.slice(0,4)!=='0000'
  &&Number.isFinite(Date.parse(v+'T00:00:00Z'))&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
const decimal=v=>typeof v==='string'&&v.length<=1000&&DECIMAL.test(v);
const nullable=(v,test)=>v===null||test(v);
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
function invalid(){const e=new Error('The Source pay decision result is invalid.');e.code='SOURCE_PAY_DECISION_RESULT_INVALID';e.status=502;throw e;}
function list(v,test,key=x=>JSON.stringify(x),max=10000){
 if(!Array.isArray(v)||v.length>max)invalid();const seen=new Set();
 for(const x of v){if(!test(x))invalid();const k=key(x);if(seen.has(k))invalid();seen.add(k);}return v;
}
function expected(v,e,keys){if(!keys.every(k=>e[k]===undefined||same(v[k],e[k])))invalid();}
function actions(v,validateAction){
 if(typeof validateAction!=='function')invalid();
 list(v,x=>{validateAction(x);return true;},x=>JSON.stringify([x.kind,x.target_type,x.target_id,x.decision_id??null,x.member_key??null,x.client_id??null,x.period_start??null,x.period_end??null]),250);
}
function prior(v){
 list(v,x=>closed(x,['member_key','component_key','disposition_id','decision_id','disposition_version'])
  &&text(x.member_key)&&nullable(x.component_key,text)&&(
    x.disposition_id===null&&x.decision_id===null&&x.disposition_version===null
    ||uuid(x.disposition_id)&&uuid(x.decision_id)&&revision(x.disposition_version)),x=>JSON.stringify([x.member_key,x.component_key]));
}
export function decodeSourceInvoiceCertificate(v){
 if(!closed(v,['phase','hash','source_row_ids','accepted_final_member_ids','original_invoice_timesheet_ids','invoice_ids','net','vat','gross'])
   ||!['NO_FINAL_REPORT_YET','ACCEPTED_UNINVOICED','INVOICED','UNRESOLVED'].includes(v.phase)||!hash(v.hash))invalid();
 for(const key of ['source_row_ids','accepted_final_member_ids','original_invoice_timesheet_ids','invoice_ids'])list(v[key],uuid);
 for(const key of ['net','vat','gross'])if(!nullable(v[key],decimal))invalid();
 if(v.phase==='NO_FINAL_REPORT_YET'&&(v.accepted_final_member_ids.length||v.invoice_ids.length||v.net!==null||v.vat!==null||v.gross!==null))invalid();
 if(v.phase==='INVOICED'&&(!v.invoice_ids.length||['net','vat','gross'].some(k=>v[k]===null)))invalid();
 if(v.phase==='ACCEPTED_UNINVOICED'&&(v.invoice_ids.length||!v.accepted_final_member_ids.length||['net','vat','gross'].some(k=>v[k]===null)))invalid();
 return v;
}
export function decodeSourceQuantity(v,mode){
 if(!closed(v,['version','mode','quantity_state','slices'])||v.version!=='PAY_DISPOSITION_QUANTITY_V1'||v.mode!==mode
   ||!['PRE_FINANCIAL','QUALIFIED'].includes(v.quantity_state))invalid();
 list(v.slices,s=>closed(s,['approved_line_id','component_key','segment_id','segment_key','work_date','unit_label','bucket','additional_code','quantity'])
  &&uuid(s.approved_line_id)&&text(s.component_key)&&nullable(s.segment_id,uuid)&&nullable(s.segment_key,text)&&date(s.work_date)
  &&text(s.unit_label)&&nullable(s.bucket,x=>['DAY','NIGHT','SAT','SUN','BH'].includes(x))&&nullable(s.additional_code,text)&&decimal(s.quantity));
 if(v.quantity_state==='PRE_FINANCIAL'&&(mode!=='WHOLE_SHIFT'||v.slices.length))invalid();return v;
}
const artifactKeys=['cut_id','member_no','artifact_kind','artifact_id','effect_id','work_hold_id','recovery_hold_id','case_event_id','run_line_id','transfer_id','outcome_id',
 'original_work_id','original_component_key','original_run_line_id','native_status','source_amount','target_ex','target_vat','target_inc','artifact_digest'];
const witness={ORIGINAL_EFFECT:'effect_id',WORK_HOLD:'work_hold_id',RECOVERY_HOLD:'recovery_hold_id',RECOVERY_EFFECT:'effect_id',FORGIVENESS:'case_event_id',RUN_LINE:'run_line_id',TRANSFER:'transfer_id',OUTCOME:'outcome_id'};
function artifact(v){
 if(!closed(v,artifactKeys)||!uuid(v.cut_id)||!revision(v.member_no)||!uuid(v.artifact_id)||!text(v.native_status)||!hash(v.artifact_digest)
  ||!Object.hasOwn(witness,v.artifact_kind)||!nullable(v.original_work_id,uuid)||!nullable(v.original_run_line_id,uuid)||!nullable(v.original_component_key,text)
  ||!['source_amount','target_ex','target_vat','target_inc'].every(k=>nullable(v[k],decimal)))return false;
 const keys=['effect_id','work_hold_id','recovery_hold_id','case_event_id','run_line_id','transfer_id','outcome_id'];
 return keys.every(k=>k===witness[v.artifact_kind]?v[k]===v.artifact_id:v[k]===null);
}
function memberIdentity(v,review){
 if(!text(v.member_key)||!['SOURCE_ROW','TIMESHEET_FAMILY'].includes(v.scope_kind)||!['WHOLE_SHIFT','COMPONENT_QUANTITIES'].includes(v.scope_mode)
  ||!nullable(v.family_root_id,uuid)||v.scope_kind==='TIMESHEET_FAMILY'&&!uuid(v.family_root_id)||!uuid(v.source_row_id)||!nullable(v.source_work_event_id,uuid))invalid();
 if(review){
  if(!uuid(v.candidate_id)||!uuid(v.actual_client_id)||!nullable(v.expected_work_revision,revision))invalid();
  list(v.component_keys,text);list(v.work_dates,date);
  if(!v.work_dates.length||!v.work_dates.every((d,i)=>!i||v.work_dates[i-1]<d)||v.scope_mode==='COMPONENT_QUANTITIES'&&!v.component_keys.length)invalid();
 }
}
export function decodeSourcePayDecisionReview(v,{validateAction=sharedValidateAction,...e}={}){
 if(!closed(v,['version','decision_id','request_id','review_id','review_digest','state_revision','expected_pay_cut_hash','expected_source_invoice_hash','expected_issue_set_hash',
  'decision','supersedes_decision_id','prior_dispositions','prior_dispositions_sha256','members','next_actions'])
  ||v.version!=='SOURCE_PAY_DECISION_REVIEW_V1'||!['decision_id','request_id','review_id'].every(k=>uuid(v[k]))||!revision(v.state_revision)
  ||!['review_digest','expected_pay_cut_hash','expected_source_invoice_hash','expected_issue_set_hash','prior_dispositions_sha256'].every(k=>hash(v[k]))
  ||!['EXCLUDE_FUTURE_WORK_PAY','ACCEPT_FINAL_SOURCE_PAY'].includes(v.decision)||!nullable(v.supersedes_decision_id,uuid))invalid();
 prior(v.prior_dispositions);
 list(v.members,m=>{
  if(!closed(m,['member_key','scope_kind','scope_mode','family_root_id','candidate_id','actual_client_id','source_row_id','source_work_event_id','component_keys','work_dates',
   'expected_work_revision','branch','qualification','supported_quantities','excluded_quantities','retained_commitments','invoice_certificate','selected_issue_ids','next_actions']))return false;
  memberIdentity(m,true);
  if(!['FUTURE_UNSIGNED','SIGNED_FINANCIAL','RETAINED_FINANCIAL'].includes(m.branch)||!['NOT_APPROVED','QUALIFIED','REVIEW_REQUIRED'].includes(m.qualification))return false;
  decodeSourceQuantity(m.supported_quantities,m.scope_mode);decodeSourceQuantity(m.excluded_quantities,m.scope_mode);decodeSourceInvoiceCertificate(m.invoice_certificate);
  list(m.retained_commitments,artifact,x=>JSON.stringify([x.cut_id,x.artifact_kind,x.artifact_id]));list(m.selected_issue_ids,uuid);actions(m.next_actions,validateAction);return true;
 },m=>m.member_key,250);
 if(!v.members.length||new Set(v.members.map(m=>m.actual_client_id)).size!==1)invalid();actions(v.next_actions,validateAction);
 expected(v,e,['decision_id','request_id','decision','expected_pay_cut_hash']);return v;
}
export function decodeSourcePreviewBegin(v,e={}){
 if(!closed(v,['version','decision_id','request_id','assembly_id','assembly_state','cut_id','cut_no','expected_pay_cut_hash','draft_scope_sha256','expires_at_utc','required_quote_members','sealed_review'])
   ||v.version!=='SOURCE_PAY_DECISION_PREVIEW_BEGIN_RESULT_V1'||!['decision_id','request_id','assembly_id','cut_id'].every(k=>uuid(v[k]))
   ||!['ASSEMBLING','READY','SEALED'].includes(v.assembly_state)||!revision(v.cut_no)||!hash(v.expected_pay_cut_hash)||!hash(v.draft_scope_sha256)
   ||typeof v.expires_at_utc!=='string'||!Number.isFinite(Date.parse(v.expires_at_utc)))invalid();
 list(v.required_quote_members,m=>closed(m,['member_key','family_id','work_event_id'])&&text(m.member_key)&&uuid(m.family_id)&&uuid(m.work_event_id),m=>m.member_key,250);
 if(!v.required_quote_members.every((m,i)=>!i||v.required_quote_members[i-1].member_key<m.member_key)
   ||e.decision==='ACCEPT_FINAL_SOURCE_PAY'&&v.required_quote_members.length)invalid();
 if(v.assembly_state==='SEALED')decodeSourcePayDecisionReview(v.sealed_review,{...e,expected_pay_cut_hash:v.expected_pay_cut_hash});
 else if(v.sealed_review!==null)invalid();
 expected(v,e,['decision_id','request_id']);return v;
}
export function decodeSourcePreviewQuoteRegistration(v,e={}){
 if(!closed(v,['version','assembly_id','member_key','quote_id','quote_sha256','economic_target_sha256'])
   ||v.version!=='SOURCE_PAY_DECISION_PREVIEW_QUOTE_RESULT_V1'||!uuid(v.assembly_id)||!text(v.member_key)||!uuid(v.quote_id)||!hash(v.quote_sha256)||!hash(v.economic_target_sha256))invalid();
 expected(v,e,['assembly_id','member_key','quote_sha256','economic_target_sha256']);return v;
}
export function decodeSourcePreviewReady(v,e={}){
 if(!closed(v,['version','assembly_id','assembly_sha256'])||v.version!=='SOURCE_PAY_DECISION_PREVIEW_READY_RESULT_V1'||!uuid(v.assembly_id)||!hash(v.assembly_sha256))invalid();
 expected(v,e,['assembly_id']);return v;
}
export function decodeSourcePreviewAbandon(v,e={}){
 if(!closed(v,['version','assembly_id','state'])||v.version!=='SOURCE_PAY_DECISION_PREVIEW_ABANDON_RESULT_V1'||!uuid(v.assembly_id)||!['CANCELLED','SEALED'].includes(v.state))invalid();
 expected(v,e,['assembly_id']);return v;
}
export function decodeSourceFinancialPublications(v,decisionId){
 if(!closed(v,['decision_id','cut_id','commands','publication_digest'])||v.decision_id!==decisionId||!uuid(v.cut_id)||!hash(v.publication_digest))invalid();
 list(v.commands,c=>closed(c,['candidate_id','command_id','member_count','manifest_sha256'])&&uuid(c.candidate_id)&&uuid(c.command_id)
  &&Number.isSafeInteger(c.member_count)&&c.member_count>0&&c.member_count<=2147483647&&hash(c.manifest_sha256),c=>c.candidate_id,250);
 if(new Set(v.commands.map(c=>c.command_id)).size!==v.commands.length)invalid();return v;
}
export function decodeSourcePayDecisionReceipt(v,{validateAction=sharedValidateAction,...e}={}){
 if(!closed(v,['version','decision_id','request_id','review_id','state_revision','accepted_at_utc','actor_user_id','reason','decision','supersedes_decision_id',
  'prior_dispositions','prior_dispositions_sha256','members','financial_publications','read_result_url'])||v.version!=='SOURCE_PAY_DECISION_RECEIPT_V1'
  ||!['decision_id','request_id','review_id','actor_user_id'].every(k=>uuid(v[k]))||!revision(v.state_revision)
  ||typeof v.accepted_at_utc!=='string'||!Number.isFinite(Date.parse(v.accepted_at_utc))||typeof v.reason!=='string'||!v.reason.trim()||[...v.reason].length>1000
  ||!['EXCLUDE_FUTURE_WORK_PAY','ACCEPT_FINAL_SOURCE_PAY'].includes(v.decision)||!nullable(v.supersedes_decision_id,uuid)||!hash(v.prior_dispositions_sha256)
  ||v.read_result_url!==`/api/weekly-source/pay-decisions/${v.decision_id}?request_id=${v.request_id}`)invalid();
 prior(v.prior_dispositions);decodeSourceFinancialPublications(v.financial_publications,v.decision_id);
 list(v.members,m=>{
  if(!closed(m,['member_key','scope_kind','scope_mode','family_root_id','source_row_id','source_work_event_id','pay_disposition_id','pay_result','invoice_result','invoice_certificate','protection_release','retired_issue_ids','next_actions']))return false;
  memberIdentity(m,false);
  if(!nullable(m.pay_disposition_id,uuid)||!['FUTURE_PAY_EXCLUDED','SOURCE_PAY_REVIEW_ACCEPTED','EXISTING_RECOVERY_REQUIRED','FROZEN_COMMITMENT_RETAINED','FINANCIAL_PENDING'].includes(m.pay_result)
   ||!['UNCHANGED_SOURCE_INVOICE','UNCHANGED_SOURCE_ALLOCATION','NO_FINAL_REPORT_YET','INVOICE_EVIDENCE_REQUIRED'].includes(m.invoice_result))return false;
  decodeSourceInvoiceCertificate(m.invoice_certificate);const p=m.protection_release;
  if(!closed(p,['status','preparation_id','orchestration_run_id','native_receipt_hash','next_action'])
   ||!['NOT_REQUIRED','RELEASED','PROTECTION_RELEASE_REQUIRED'].includes(p.status)||!nullable(p.preparation_id,uuid)||!nullable(p.orchestration_run_id,uuid)||!nullable(p.native_receipt_hash,hash))return false;
  if(p.status==='NOT_REQUIRED'&&(p.preparation_id!==null||p.orchestration_run_id!==null||p.native_receipt_hash!==null||p.next_action!==null))return false;
  if(p.status==='RELEASED'&&(!uuid(p.preparation_id)||!uuid(p.orchestration_run_id)||!hash(p.native_receipt_hash)||p.next_action!==null))return false;
  if(p.status==='PROTECTION_RELEASE_REQUIRED'&&(p.native_receipt_hash!==null||p.next_action===null))return false;
  if(p.next_action!==null)actions([p.next_action],validateAction);list(m.retired_issue_ids,uuid);actions(m.next_actions,validateAction);return true;
 },m=>m.member_key,250);
 if(!v.members.length)invalid();expected(v,e,['decision_id','request_id','review_id','decision','actor_user_id','prior_dispositions_sha256']);return v;
}

export function decodeSourcePayDecisionSourceRead(v,{validateAction=sharedValidateAction,...e}={}){
 if(!closed(v,['version','decision_id','request_id','accepted_receipt','request_receipt','state_revision','current_cut','applications','native_members'])
   ||v.version!=='SOURCE_PAY_DECISION_INTERNAL_SOURCE_READ_V1'||!uuid(v.decision_id)||!uuid(v.request_id)||!revision(v.state_revision))invalid();
 decodeSourcePayDecisionReceipt(v.accepted_receipt,{validateAction,decision_id:v.decision_id});
 decodeSourcePayDecisionReceipt(v.request_receipt,{validateAction,decision_id:v.decision_id,request_id:v.request_id});
 const cut=v.current_cut;
 if(!closed(cut,['cut_id','cut_no','review_id','qualification_state','pay_cut_sha256','source_invoice_sha256','issue_set_sha256'])
   ||!uuid(cut.cut_id)||!uuid(cut.review_id)||!revision(cut.cut_no)||!['NOT_APPROVED','QUALIFIED','REVIEW_REQUIRED'].includes(cut.qualification_state)
   ||!['pay_cut_sha256','source_invoice_sha256','issue_set_sha256'].every(k=>hash(cut[k])))invalid();
 list(v.applications,a=>closed(a,['application_id','request_id','review_id','cut_id','state','application_sha256','completed_at_utc'])
  &&['application_id','request_id','review_id','cut_id'].every(k=>uuid(a[k]))&&a.state==='COMMITTED'&&hash(a.application_sha256)
  &&typeof a.completed_at_utc==='string'&&Number.isFinite(Date.parse(a.completed_at_utc)),a=>a.application_id);
 if(!v.applications.some(a=>a.cut_id===cut.cut_id&&a.review_id===cut.review_id)
  ||!v.applications.some(a=>a.request_id===v.request_id&&a.review_id===v.request_receipt.review_id)
  ||BigInt(v.state_revision)<BigInt(v.request_receipt.state_revision))invalid();
 list(v.native_members,n=>{
  if(!closed(n,['application_id','request_id','member_key','preparation_id','route','origin','orchestration_run_id','native_result_sha256','original_native_result','failure_code','current_local_release_proof'])
   ||!['application_id','request_id','preparation_id'].every(k=>uuid(n[k]))||!text(n.member_key)||!['NEXT','SOURCE_LOCAL'].includes(n.route)
   ||!v.applications.some(a=>a.application_id===n.application_id&&a.request_id===n.request_id))return false;
  if(n.origin==='NOT_APPLIED_DEPENDENCY')return n.orchestration_run_id===null&&n.native_result_sha256===null&&n.original_native_result===null
    &&['DEPENDENCY_UNAVAILABLE','RESULT_UNKNOWN'].includes(n.failure_code)&&n.current_local_release_proof===null;
  if(!['APPLIED_IN_THIS_TRANSACTION','ADOPTED_EXISTING'].includes(n.origin)||!uuid(n.orchestration_run_id)||!hash(n.native_result_sha256)||n.failure_code!==null)return false;
  decodeSourceNativeComposedResult(n.original_native_result,{application_id:n.application_id,decision_id:v.decision_id,request_id:n.request_id,member_key:n.member_key,
   preparation_id:n.preparation_id,route:n.route});
  if(n.original_native_result.origin!==n.origin||n.original_native_result.native_receipt.orchestration_run_id!==n.orchestration_run_id)return false;
  if(n.current_local_release_proof!==null){
   if(n.route!=='SOURCE_LOCAL')return false;
   const receipt=n.original_native_result.native_receipt;
   if(receipt.entitlement?.kind!=='PENDING_BUNDLE'||receipt.local_state!=='PENDING_FREEZE')return false;
   decodeSourceNativeDeferredReleaseProof(n.current_local_release_proof,{application_id:n.application_id,member_key:n.member_key,
    local_publication_request_id:receipt.publication_request_id,orchestration_run_id:n.orchestration_run_id,generation_id:receipt.generation_id,
    pending_bundle_id:receipt.entitlement?.pending_bundle_id,decision_bundle_id:receipt.entitlement?.decision_bundle_id,bundle_revision:receipt.entitlement?.bundle_revision});
  }return true;
 },n=>JSON.stringify([n.application_id,n.member_key]));
 expected(v,e,['decision_id','request_id']);return v;
}
