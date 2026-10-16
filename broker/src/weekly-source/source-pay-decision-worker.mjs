import { decodeSourcePayDecisionBrowserRequest } from './source-pay-decision-request.mjs';
import { decodeSourcePreviewBegin,decodeSourcePreviewQuoteRegistration,decodeSourcePreviewReady,decodeSourcePreviewAbandon,
 decodeSourcePayDecisionReview,decodeSourcePayDecisionReceipt,decodeSourcePayDecisionSourceRead } from './source-pay-decision-contract.mjs';
import { calculateSourceUnprotectAssemblyQuote,prepareSourcePayDecisionServiceRequest } from './source-unprotect-worker.mjs';
import { hashSourceNativeJson } from './source-native-economic.mjs';

const uuid=v=>typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(v);
function dependencies(actor,provided,calculation=false){
 if(!uuid(actor)||typeof provided.dataRpc!=='function'||calculation&&typeof provided.calculateWeeklySnapshot!=='function'){
  const e=new Error('Source pay decision service dependencies are unavailable.');e.code='SOURCE_PAY_DECISION_DEPENDENCY_REQUIRED';e.status=503;throw e;
 }
 return provided;
}
// Each RPC owns one bounded transaction. The calculator runs after BEGIN has
// committed and before registration; no database lock spans a network call.
export async function previewSourcePayDecision({request,actorUserId,...provided}){
 const body=decodeSourcePayDecisionBrowserRequest(request,'PREVIEW');
 const owner=dependencies(actorUserId,provided,true);
 const begin=decodeSourcePreviewBegin(await owner.dataRpc('weekly_source_pay_decision_preview_begin_v1',{p_request:{
  version:'SOURCE_PAY_DECISION_PREVIEW_BEGIN_V1',actor_user_id:actorUserId,browser_request:body,
 }}),{decision_id:body.decision_id,request_id:body.request_id,decision:body.decision});
 if(begin.assembly_state==='SEALED')return begin.sealed_review;
 for(const member of begin.required_quote_members){
  const quote=await calculateSourceUnprotectAssemblyQuote({assemblyId:begin.assembly_id,decisionId:begin.decision_id,memberKey:member.member_key,
   actorUserId,cutId:begin.cut_id,expectedPayCutHash:begin.expected_pay_cut_hash,
   dataRpc:owner.dataRpc,calculateWeeklySnapshot:owner.calculateWeeklySnapshot});
  if(quote.economic_target.family_id!==member.family_id||quote.economic_target.work_event_id!==member.work_event_id){
   const e=new Error('The required Source protection member changed.');e.code='SOURCE_PAY_DECISION_REVIEW_CHANGED';e.status=409;throw e;
  }
  decodeSourcePreviewQuoteRegistration(await owner.dataRpc('weekly_source_pay_decision_preview_quote_v1',{p_request:{
   version:'SOURCE_PAY_DECISION_PREVIEW_QUOTE_V1',assembly_id:begin.assembly_id,actor_user_id:actorUserId,quote,
  }}),{assembly_id:begin.assembly_id,member_key:member.member_key,economic_target_sha256:quote.economic_target_sha256,
   quote_sha256:await hashSourceNativeJson('SOURCE_UNPROTECT_PREVIEW_QUOTE_V1',quote)});
 }
 const ready=decodeSourcePreviewReady(await owner.dataRpc('weekly_source_pay_decision_preview_ready_v1',{p_request:{
  version:'SOURCE_PAY_DECISION_PREVIEW_READY_V1',assembly_id:begin.assembly_id,actor_user_id:actorUserId,
  expected_draft_scope_sha256:begin.draft_scope_sha256,
 }}),{assembly_id:begin.assembly_id});
 // Unknown seal outcomes propagate unchanged. Recovery repeats this exact
 // original browser request; BEGIN then returns its immutable sealed review.
 return decodeSourcePayDecisionReview(await owner.dataRpc('weekly_source_pay_decision_preview_v1',{p_request:{
  ...body,actor_user_id:actorUserId,preview_assembly_ref:{assembly_id:ready.assembly_id,assembly_sha256:ready.assembly_sha256},
  unprotect_preparations:[],
 }}),{decision_id:body.decision_id,request_id:body.request_id,decision:body.decision,expected_pay_cut_hash:begin.expected_pay_cut_hash});
}
export async function confirmSourcePayDecision({request,actorUserId,...provided}){
 const body=decodeSourcePayDecisionBrowserRequest(request,request?.action);
 if(!['ACCEPT','RESUME_FINANCIAL'].includes(body.action)){
  const e=new Error('A Source financial confirmation is required.');e.code='SOURCE_PAY_DECISION_REQUEST_INVALID';e.status=400;throw e;
 }
 const owner=dependencies(actorUserId,provided,true);
 const service=await prepareSourcePayDecisionServiceRequest({request:body,actorUserId,...owner});
 return decodeSourcePayDecisionReceipt(await owner.dataRpc('weekly_source_pay_decision_accept_v1',{p_request:service}),{
  decision_id:body.decision_id,request_id:body.request_id,review_id:body.review_id,actor_user_id:actorUserId,
  ...(body.action==='ACCEPT'?{decision:body.decision}:{}),prior_dispositions_sha256:body.expected_prior_dispositions_sha256,
 });
}
export async function abandonSourcePayDecisionPreview({assemblyId,actorUserId,...provided}){
 const owner=dependencies(actorUserId,provided);
 if(!uuid(assemblyId)){const e=new Error('The Source preview identity is invalid.');e.code='SOURCE_PAY_DECISION_REQUEST_INVALID';e.status=400;throw e;}
 return decodeSourcePreviewAbandon(await owner.dataRpc('weekly_source_pay_decision_preview_abandon_v1',{p_request:{
  version:'SOURCE_PAY_DECISION_PREVIEW_ABANDON_V1',assembly_id:assemblyId,actor_user_id:actorUserId,
 }}),{assembly_id:assemblyId});
}

// Root combines this Source-owned closed projection with the genuine B051
// current-financial child only after that exact producer shape is published.
export async function readSourcePayDecisionSource({request,actorUserId,...provided}){
 const body=decodeSourcePayDecisionBrowserRequest(request,'READ_RESULT');const owner=dependencies(actorUserId,provided);
 return decodeSourcePayDecisionSourceRead(await owner.dataRpc('weekly_source_pay_decision_read_source_v1',{p_request:{
  version:'SOURCE_PAY_DECISION_INTERNAL_SOURCE_READ_V1',actor_user_id:actorUserId,decision_id:body.decision_id,request_id:body.request_id,
 }}),{decision_id:body.decision_id,request_id:body.request_id});
}
