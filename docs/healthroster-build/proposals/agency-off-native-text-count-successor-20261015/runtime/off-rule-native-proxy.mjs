// Server dependency adapter only. Existing authenticated handlers supply actor,
// agency and cycle; no browser qualifier/strategy/rule or finance field added.
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const fail=code=>{throw Object.assign(new Error(code),{code});};
export const sourceOperations=Object.freeze(['weekly_source_office_recheck_begin_v1','weekly_source_upload_context_v1','weekly_source_upload_stage_begin_atomic_v1',
 'weekly_source_upload_stage_rows_atomic_v1','weekly_source_upload_seal_atomic_v1','weekly_source_projection_begin_atomic_v1',
 'weekly_source_projection_rows_apply_atomic_v1','weekly_source_projection_publish_atomic_v1','weekly_source_import_prepare_atomic_v1',
 'weekly_source_finalise_atomic_v1','weekly_source_finalisation_pay_open_atomic_v1','weekly_source_finalisation_pay_task_start_atomic_v1',
 'weekly_source_ordinary_pay_projection_apply_atomic_v1','weekly_source_finalisation_pay_task_finish_atomic_v1','REPORT_FIN_AB_FINALISE','weekly_source_agency_timesheet_change_v1','weekly_source_agency_change_worker_context_v1','weekly_source_agency_change_worker_apply_v1','weekly_source_agency_validation_context_v1','weekly_source_agency_validation_apply_v1','weekly_source_agency_change_authorise_step_v1','weekly_source_agency_change_return_step_v1']);
export const automaticOperations=Object.freeze(['weekly_source_automatic_reconcile_atomic_v1','weekly_exceptional_pay_prepare_action_v1',
 'weekly_exceptional_pay_action_context_v1','weekly_exceptional_pay_action_publication_status_v1','weekly_exceptional_pay_stage_c1_request_v1','weekly_source_missing_accept_context_v1','weekly_source_missing_accept_prepare_atomic_v1','weekly_source_later_change_decide_atomic_v1']);
export const agencyOperations=Object.freeze(['weekly_source_agency_timesheet_change_v1','weekly_source_agency_change_worker_context_v1','weekly_source_agency_change_worker_apply_v1','weekly_source_agency_validation_context_v1','weekly_source_agency_validation_apply_v1','weekly_source_agency_change_authorise_step_v1','weekly_source_agency_change_return_step_v1']);
export function assertOffAgencyNativeRequest(name,req){
 if(!agencyOperations.includes(name))return;
 const exact=keys=>req&&typeof req==='object'&&!Array.isArray(req)&&Object.keys(req).sort().join('|')===keys.sort().join('|');
 const id=v=>typeof v==='string'&&uuid.test(v),text=(v,max)=>typeof v==='string'&&Array.from(v).length>=1&&Array.from(v).length<=max;
 if(name==='weekly_source_agency_timesheet_change_v1'){
  if(!exact(['p_contract_week_id','p_reason','p_expected_signature','p_request_key','p_actor_user_id'])
    ||!id(req.p_contract_week_id)||!id(req.p_actor_user_id)||!text(req.p_reason,2000)
    ||!text(req.p_expected_signature,512)||!text(req.p_request_key,200))fail('OFF_RULE_TRANSPORT_NATIVE_PAYLOAD_INVALID');
 }else if(['weekly_source_agency_change_authorise_step_v1','weekly_source_agency_change_return_step_v1'].includes(name)){
  if(!exact(['p_request_id','p_actor_user_id'])||!id(req.p_request_id)||!id(req.p_actor_user_id))fail('OFF_RULE_TRANSPORT_NATIVE_PAYLOAD_INVALID');
 }else if(['weekly_source_agency_change_worker_context_v1','weekly_source_agency_validation_context_v1'].includes(name)){
  if(!exact(['p_request_id'])||!id(req.p_request_id))fail('OFF_RULE_TRANSPORT_NATIVE_PAYLOAD_INVALID');
 }else if(!exact(['p_request_id','p_expected_request_version','p_expected_context_sha256','p_snapshot'])
   ||!id(req.p_request_id)||!Number.isSafeInteger(req.p_expected_request_version)||req.p_expected_request_version<1
   ||typeof req.p_expected_context_sha256!=='string'||!/^[0-9a-f]{64}$/.test(req.p_expected_context_sha256)
   ||!req.p_snapshot||typeof req.p_snapshot!=='object'||Array.isArray(req.p_snapshot)
   ||new TextEncoder().encode(JSON.stringify(req.p_snapshot)).length>1048576)fail('OFF_RULE_TRANSPORT_NATIVE_PAYLOAD_INVALID');
}
export async function createOffRuleNativeDependencies({actorUserId,agencyId,environment,sourceCycleId,purpose,dependencies}){
 if(!uuid.test(actorUserId)||!uuid.test(agencyId)||!uuid.test(sourceCycleId)||!['TEST','LIVE'].includes(environment)
  ||!['SOURCE_FIXTURE_SETUP','AUTOMATIC_JOURNEY'].includes(purpose)||typeof dependencies?.dataRpc!=='function')fail('OFF_RULE_PROXY_INPUT_INVALID');
 if(environment!=='TEST')return dependencies;
 const admitted=await dependencies.dataRpc('weekly_source_group_rule_feature_set_atomic_v1',{p_request:{
  actor_user_id:actorUserId,agency_id:agencyId,environment,qualification_action:'OFF_RULE_STATUS',source_cycle_id:sourceCycleId,purpose}});
 if(admitted?.contract!=='WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1'||typeof admitted.qualified_fixture!=='boolean'
  ||admitted.operational_ready!==false)fail('OFF_RULE_PROXY_ADMISSION_INVALID');
 if(!admitted.qualified_fixture)return dependencies;
 const permitted=purpose==='SOURCE_FIXTURE_SETUP'?sourceOperations:automaticOperations;
 if(admitted.purpose!==purpose||admitted.source_cycle_id!==sourceCycleId||admitted.actor_user_id!==actorUserId
  ||!Array.isArray(admitted.allowed_operations)||!admitted.allowed_operations.length
  ||admitted.allowed_operations.some(name=>!permitted.includes(name))||new Set(admitted.allowed_operations).size!==admitted.allowed_operations.length)
  fail('OFF_RULE_PROXY_ADMISSION_INVALID');
 return {...dependencies,dataRpc:async(name,args)=>{
  if(!permitted.includes(name))return dependencies.dataRpc(name,args);
  if(!admitted.allowed_operations.includes(name))fail('OFF_RULE_PROXY_OPERATION_REFUSED');
  if(!args||typeof args!=='object'||Array.isArray(args))fail('OFF_RULE_PROXY_NATIVE_PAYLOAD_INVALID');
  const positional=['weekly_source_projection_rows_apply_atomic_v1','weekly_source_agency_timesheet_change_v1','weekly_source_agency_change_worker_context_v1','weekly_source_agency_change_worker_apply_v1','weekly_source_agency_validation_context_v1','weekly_source_agency_validation_apply_v1','weekly_source_agency_change_authorise_step_v1','weekly_source_agency_change_return_step_v1'].includes(name);
  const nativeRequest=positional?args:args.p_request;
  if(!nativeRequest||typeof nativeRequest!=='object'||Array.isArray(nativeRequest)
   ||(!positional&&(Object.keys(args).length!==1||!Object.hasOwn(args,'p_request'))))
   fail('OFF_RULE_PROXY_NATIVE_PAYLOAD_INVALID');
  assertOffAgencyNativeRequest(name,nativeRequest);
  return dependencies.dataRpc('weekly_source_group_rule_feature_set_atomic_v1',{p_request:{actor_user_id:actorUserId,agency_id:agencyId,
   environment,qualification_action:'OFF_RULE_COMMAND',source_cycle_id:sourceCycleId,native_operation:name,native_request:nativeRequest}});
 }};
}
