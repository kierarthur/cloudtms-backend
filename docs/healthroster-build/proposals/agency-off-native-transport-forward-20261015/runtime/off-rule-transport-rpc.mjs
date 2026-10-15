import {sourceOperations,automaticOperations,assertOffAgencyNativeRequest} from './off-rule-native-proxy.mjs';
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const fail=code=>{throw Object.assign(new Error(code),{code});};
const unwrap=(value,name)=>{let v=value;if(Array.isArray(v)&&v.length===1)v=v[0];if(v&&typeof v==='object'&&Object.hasOwn(v,name))v=v[name];
 if(Array.isArray(v)&&v.length===1)v=v[0];return v;};
const exact=(v,keys)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join('|')===[...keys].sort().join('|');
// Actual transport dependency. The broker's existing requireUser result is the
// only actor provider. It wraps both the direct route and all nested Source
// upload/protected/FIN orchestrator RPCs; calculators/C1 raw RPC stay native.
export function createOffRuleTransportRpc({baseRpc,getActor,agencyId,environment}){
 if(typeof baseRpc!=='function'||typeof getActor!=='function')fail('OFF_RULE_TRANSPORT_DEPENDENCY_INVALID');
 if(environment!=='TEST')return baseRpc;
 return async(name,args,options)=>{
  if(![...sourceOperations,...automaticOperations].includes(name)||name==='REPORT_FIN_AB_FINALISE')return baseRpc(name,args,options);
  const worker=['weekly_source_agency_change_worker_context_v1','weekly_source_agency_change_worker_apply_v1','weekly_source_agency_validation_context_v1','weekly_source_agency_validation_apply_v1','weekly_source_agency_change_authorise_step_v1','weekly_source_agency_change_return_step_v1'].includes(name);
  const actor=getActor();if((!worker&&!uuid.test(actor))||(worker&&actor!==null&&!uuid.test(actor))||!uuid.test(agencyId))
   fail('OFF_RULE_TRANSPORT_AUTHENTICATED_SCOPE_REQUIRED');
  const positional=['weekly_source_projection_rows_apply_atomic_v1','weekly_source_agency_timesheet_change_v1','weekly_source_agency_change_worker_context_v1','weekly_source_agency_change_worker_apply_v1','weekly_source_agency_validation_context_v1','weekly_source_agency_validation_apply_v1','weekly_source_agency_change_authorise_step_v1','weekly_source_agency_change_return_step_v1'].includes(name),req=positional?args:args?.p_request;
  if(!req||typeof req!=='object'||Array.isArray(req)||(!positional&&!exact(args,['p_request']))
   ||(Object.hasOwn(req,'actor_user_id')&&req.actor_user_id!==actor)||(Object.hasOwn(req,'p_actor_user_id')&&(!worker||actor!==null)&&req.p_actor_user_id!==actor))
   fail('OFF_RULE_TRANSPORT_NATIVE_PAYLOAD_INVALID');
  assertOffAgencyNativeRequest(name,req);
  const owner='weekly_source_group_rule_feature_set_atomic_v1';
  const admission=unwrap(await baseRpc(owner,{p_request:{actor_user_id:actor,agency_id:agencyId,environment:'TEST',
   qualification_action:'OFF_RULE_TRANSPORT_STATUS',native_operation:name,native_request:req}},options),owner);
  if(admission?.qualified_fixture===false){
   if(!exact(admission,['contract','qualified_fixture','operational_ready'])||admission.contract!=='WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1'
    ||admission.operational_ready!==false)fail('OFF_RULE_TRANSPORT_ADMISSION_INVALID');
   return baseRpc(name,args,options);
  }
  if(!exact(admission,['contract','qualified_fixture','operational_ready','purpose','source_cycle_id','actor_user_id','allowed_operations','scope_id','authorization_sha256'])
   ||admission.contract!=='WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1'||admission.qualified_fixture!==true||admission.operational_ready!==false
   ||typeof admission.actor_user_id!=='string'||!uuid.test(admission.actor_user_id)||(!worker&&admission.actor_user_id!==actor)||(worker&&actor!==null&&admission.actor_user_id!==actor)||typeof admission.source_cycle_id!=='string'||!uuid.test(admission.source_cycle_id)||typeof admission.scope_id!=='string'||!uuid.test(admission.scope_id)
   ||typeof admission.authorization_sha256!=='string'||!/^[0-9a-f]{64}$/.test(admission.authorization_sha256)||!['SOURCE_FIXTURE_SETUP','AUTOMATIC_JOURNEY'].includes(admission.purpose)
   ||!Array.isArray(admission.allowed_operations)||!admission.allowed_operations.includes(name)
   ||new Set(admission.allowed_operations).size!==admission.allowed_operations.length
   ||admission.allowed_operations.some(op=>!(admission.purpose==='SOURCE_FIXTURE_SETUP'?sourceOperations:automaticOperations).includes(op)))
   fail('OFF_RULE_TRANSPORT_ADMISSION_INVALID');
  return unwrap(await baseRpc(owner,{p_request:{actor_user_id:admission.actor_user_id,agency_id:agencyId,environment:'TEST',qualification_action:'OFF_RULE_COMMAND',
   source_cycle_id:admission.source_cycle_id,native_operation:name,native_request:req}},options),owner);
 };
}
