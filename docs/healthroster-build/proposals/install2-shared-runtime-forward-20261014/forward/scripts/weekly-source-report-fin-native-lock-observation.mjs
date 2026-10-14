import {exact,sha256,pgJsonbText} from './weekly-source-protected-release-qualification.mjs';
const fail=code=>{throw Error(code);},uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,hex=/^[0-9a-f]{64}$/;
const decode=bytes=>{if(typeof bytes!=='string'||Buffer.byteLength(bytes)>256*1024)fail('REPORT_NATIVE_LOCK_BYTES_INVALID');return JSON.parse(bytes);};
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b),quote=v=>"'"+String(v).replaceAll("'","''")+"'";
const requestKeys=['actor_user_id','source_cycle_id','authority_scope_kind','report_scope_id','upload_id','projection_publication_id',
 'expected_authority_scope_version','expected_row_manifest_hash','expected_comparison_manifest_hash','expected_issue_set_hash','exclude_unfinalised_acknowledged'];
export function nativeOrdinaryRequestSha256(r){
 exact(r,requestKeys,'REPORT_NATIVE_ORDINARY_REQUEST_SHAPE_INVALID');
 if(!['CYCLE','NHSP_REPORT_SCOPE'].includes(r.authority_scope_kind)||r.authority_scope_kind==='CYCLE'&&r.report_scope_id!==null
  ||r.authority_scope_kind==='NHSP_REPORT_SCOPE'&&!uuid.test(r.report_scope_id)||!['actor_user_id','source_cycle_id','upload_id','projection_publication_id'].every(k=>uuid.test(r[k]))
  ||!Number.isSafeInteger(r.expected_authority_scope_version)||r.expected_authority_scope_version<1||typeof r.exclude_unfinalised_acknowledged!=='boolean'
  ||!requestKeys.filter(k=>k.endsWith('_hash')).every(k=>hex.test(r[k])))fail('REPORT_NATIVE_ORDINARY_REQUEST_INVALID');
 const text='{'+Object.keys(r).sort((a,b)=>Buffer.byteLength(a)-Buffer.byteLength(b)||Buffer.compare(Buffer.from(a),Buffer.from(b)))
  .map(k=>JSON.stringify(k)+': '+(k==='expected_authority_scope_version'?String(r[k]):pgJsonbText(r[k]))).join(', ')+'}';
 return sha256('WEEKLY_SOURCE_OFF_RULE_NATIVE_REQUEST_V1\x1f'+text);
}
export function reportNativeFinLockObservationSql(sourceCycleId,actorUserId){
 if(!uuid.test(sourceCycleId)||!uuid.test(actorUserId))fail('REPORT_NATIVE_LOCK_QUERY_SCOPE_INVALID');
 return `select coalesce(jsonb_agg(jsonb_build_object('id',a.id,'object_type',a.object_type,'object_id_text',a.object_id_text,
  'actor_user_id',a.actor_user_id,'event_sequence',a.event_sequence::text,'event_sequence_is_authoritative',a.event_sequence_is_authoritative,
  'before_bytes',a.before_json::text,'after_bytes',a.after_json::text,'frame_base_bytes',(a.after_json-'frame_sha256')::text,
  'command_base_bytes',(a.before_json-'command_sha256')::text,
  'completed_bytes',(select completed.after_json::text from public.audit_events completed where completed.object_type='weekly_source_off_rule_command'
   and completed.object_id_text=a.object_id_text and completed.action='WEEKLY_SOURCE_OFF_RULE_COMMAND_COMPLETED' and completed.event_sequence_is_authoritative),
  'completed_before_bytes',(select completed.before_json::text from public.audit_events completed where completed.object_type='weekly_source_off_rule_command'
   and completed.object_id_text=a.object_id_text and completed.action='WEEKLY_SOURCE_OFF_RULE_COMMAND_COMPLETED' and completed.event_sequence_is_authoritative),
  'completed_sequence',(select completed.event_sequence::text from public.audit_events completed where completed.object_type='weekly_source_off_rule_command'
   and completed.object_id_text=a.object_id_text and completed.action='WEEKLY_SOURCE_OFF_RULE_COMMAND_COMPLETED' and completed.event_sequence_is_authoritative))
  order by a.event_sequence),'[]'::jsonb)::text from public.audit_events a where a.object_type='weekly_source_off_rule_native_fin'
   and a.action='WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_OBSERVED' and a.event_sequence_is_authoritative
   and a.actor_user_id=${quote(actorUserId)}::uuid and a.after_json->>'source_cycle_id'=${quote(sourceCycleId)}
   and a.after_json->>'transaction_id'=pg_current_xact_id_if_assigned()::text and a.after_json->>'backend_pid'=pg_backend_pid()::text;`;
}
export const nativeFinLockFrameKeys=Object.freeze(['schema_version','execution','command_id','command_sha256','authorization_sha256','native_operation',
 'actor_user_id','source_cycle_id','upload_id','projection_publication_id','generation','authority_scope_kind','prepare_only','native_argument_bytes',
 'backend_pid','transaction_id','active_owner','original_root_ids','rechecked_root_ids','root_set_setting','operation_id','operation_setting',
 'operation','operation_members','lock_result','source_lock_discovery','source_lock_observed','window_lock_discovery','window_lock_observed',
 'source_cas','source_facts_bytes','operational_ready','frame_sha256']);
export function validateNativeFinLockObservation(record,context){
 exact(record,['id','object_type','object_id_text','actor_user_id','event_sequence','event_sequence_is_authoritative','before_bytes','after_bytes',
  'frame_base_bytes','command_base_bytes','completed_bytes','completed_before_bytes','completed_sequence'],'REPORT_NATIVE_LOCK_RECORD_INVALID');
 if(!uuid.test(record.id)||!uuid.test(record.object_id_text)||record.object_type!=='weekly_source_off_rule_native_fin'
  ||record.event_sequence_is_authoritative!==true||!/^([1-9][0-9]*)$/.test(record.event_sequence)||!/^([1-9][0-9]*)$/.test(record.completed_sequence)
  ||BigInt(record.completed_sequence)<=BigInt(record.event_sequence))fail('REPORT_NATIVE_LOCK_AUDIT_INVALID');
 const frame=decode(record.after_bytes),command=decode(record.before_bytes),completed=decode(record.completed_bytes),args=decode(frame.native_argument_bytes);
 exact(frame,nativeFinLockFrameKeys,'REPORT_NATIVE_LOCK_FRAME_INVALID');
 const {frame_sha256:frameHash,...frameBody}=frame,{command_sha256:commandHash,...commandBody}=command;
 if(!same(decode(record.frame_base_bytes),frameBody)||!same(decode(record.command_base_bytes),commandBody)
  ||sha256('WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_CONTEXT_V1\x1f'+record.frame_base_bytes)!==frameHash
  ||sha256('WEEKLY_SOURCE_OFF_RULE_COMMAND_V1\x1f'+record.command_base_bytes)!==commandHash
  ||record.completed_before_bytes!==record.before_bytes)fail('REPORT_NATIVE_LOCK_FRAME_HASH_CHANGED');
 exact(args,['p_request','p_correction_session_id','p_prepare_only','p_report_vat_rounding_variant'],'REPORT_NATIVE_LOCK_ARGUMENTS_INVALID');
 const r=args.p_request;
 if(frame.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_CONTEXT_V1'||!same(frame.execution,context.execution)
  ||frame.operational_ready!==false||frame.command_id!==record.object_id_text||frame.command_id!==command.command_id||frame.command_sha256!==commandHash
  ||frame.native_operation!=='weekly_source_finalise_atomic_v1'||command.native_operation!==frame.native_operation
  ||frame.actor_user_id!==record.actor_user_id||frame.actor_user_id!==context.actor_user_id||frame.actor_user_id!==command.actor_user_id
  ||frame.source_cycle_id!==r.source_cycle_id||frame.source_cycle_id!==command.source_cycle_id||frame.upload_id!==r.upload_id
  ||frame.projection_publication_id!==r.projection_publication_id||frame.authority_scope_kind!==r.authority_scope_kind
  ||frame.prepare_only!==false||args.p_prepare_only!==false||args.p_correction_session_id!==null
  ||frame.backend_pid!==context.backend_pid||frame.transaction_id!==context.transaction_id
  ||command.backend_pid!==frame.backend_pid||command.transaction_id!==frame.transaction_id||command.operational_ready!==false
  ||command.native_parent_scope?.schema_version!=='WEEKLY_SOURCE_OFF_RULE_NATIVE_PARENT_SCOPE_V1'
  ||command.native_parent_scope.source_cycle_id!==frame.source_cycle_id||command.native_parent_scope.ambiguous!==false
  ||frame.authorization_sha256!==command.authorization?.authorization_sha256||command.authorization.purpose!=='SOURCE_FIXTURE_SETUP'
  ||command.authorization.source_cycle_id!==frame.source_cycle_id||command.authorization.journey_actor_user_id!==frame.actor_user_id
  ||command.native_request_sha256!==nativeOrdinaryRequestSha256(r)
  ||completed.command_id!==frame.command_id||completed.command_sha256!==frame.command_sha256||completed.operational_ready!==false
  ||completed.native_result_sha256!==sha256('WEEKLY_SOURCE_OFF_RULE_NATIVE_RESULT_V1\x1f'+context.native_result_bytes))fail('REPORT_NATIVE_LOCK_SCOPE_UNPROVED');
 if(!same(args,decode(context.native_argument_bytes)))fail('REPORT_NATIVE_LOCK_ARGUMENTS_CHANGED');
 const facts=decode(frame.source_facts_bytes),cas=frame.source_cas;
 exact(cas,['schema_version','source_cycle_id','client_ids','source_cas_sha256'],'REPORT_NATIVE_LOCK_SOURCE_CAS_INVALID');
 if(cas.schema_version!=='WEEKLY_SOURCE_BULK_SOURCE_CAS_V1'||cas.source_cycle_id!==frame.source_cycle_id
  ||cas.source_cas_sha256!==sha256('WEEKLY_SOURCE_BULK_SOURCE_CAS_V1\x1f'+frame.source_facts_bytes)
  ||!same(cas.client_ids,facts.client_ids)||new Set(cas.client_ids).size!==cas.client_ids.length
  ||!same([...cas.client_ids].sort(),[...command.authorization.fixture_manifest.client_ids].sort()))fail('REPORT_NATIVE_LOCK_SOURCE_CAS_CHANGED');
 if(frame.window_lock_discovery!==null){if(!same(frame.window_lock_discovery,frame.window_lock_observed))fail('REPORT_NATIVE_LOCK_WINDOW_RECHECK_CHANGED');}
 else if(!same(frame.source_lock_discovery,frame.source_lock_observed)||frame.source_lock_discovery===null)fail('REPORT_NATIVE_LOCK_SOURCE_RECHECK_CHANGED');
 if(frame.active_owner==='NEXT'){
  const operation=frame.operation;exact(operation,['id','transaction_id','upload_id','projection_publication_id','generation','old_revision_id',
   'source_group_id','client_id','coverage_start','coverage_end'],'REPORT_NATIVE_LOCK_OPERATION_INVALID');
  if(!uuid.test(frame.operation_id)||frame.operation_setting!==frame.operation_id||operation.id!==frame.operation_id
   ||operation.transaction_id!==frame.transaction_id||operation.upload_id!==frame.upload_id||operation.projection_publication_id!==frame.projection_publication_id
   ||operation.generation!==frame.generation||operation.source_group_id!==command.authorization.source_group_id
   ||!command.authorization.fixture_manifest.client_ids.includes(operation.client_id)||frame.root_set_setting!==''
   ||frame.original_root_ids!==null||frame.rechecked_root_ids!==null||!Array.isArray(frame.operation_members)
   ||new Set(frame.operation_members.map(m=>m.root_timesheet_id)).size!==frame.operation_members.length)fail('REPORT_NATIVE_LOCK_OPERATION_SCOPE_CHANGED');
  for(const member of frame.operation_members){exact(member,['operation_id','root_timesheet_id','raw_booking_id','canonical_booking_id'],'REPORT_NATIVE_LOCK_MEMBER_INVALID');
   if(member.operation_id!==operation.id||!uuid.test(member.root_timesheet_id)||typeof member.raw_booking_id!=='string'
    ||!member.canonical_booking_id||member.canonical_booking_id!==member.raw_booking_id.trim())fail('REPORT_NATIVE_LOCK_MEMBER_INVALID');}
 }else{
  if(!['LEGACY','DISABLED'].includes(frame.active_owner)||frame.operation_id!==null||frame.operation!==null||frame.operation_setting!==''
   ||!Array.isArray(frame.operation_members)||frame.operation_members.length!==0||!same(frame.original_root_ids,frame.rechecked_root_ids))fail('REPORT_NATIVE_LEGACY_LOCK_CONTEXT_CHANGED');
  const roots=frame.original_root_ids;
  if(roots===null){if(frame.root_set_setting!=='EMPTY_LOCKED_SET'||frame.lock_result!==null)fail('REPORT_NATIVE_EMPTY_ROOT_GUARD_CHANGED');}
  else if(!Array.isArray(roots)||!roots.length||new Set(roots).size!==roots.length||roots.some(id=>!uuid.test(id))
    ||!same([...roots].sort(),roots)||frame.root_set_setting!==roots.join(',')||frame.lock_result?.ok!==true
    ||!Array.isArray(frame.lock_result.families)||frame.lock_result.families.length!==roots.length
    ||new Set(frame.lock_result.families.map(f=>f.requested_timesheet_id)).size!==roots.length
    ||frame.lock_result.families.some(f=>!roots.includes(f.requested_timesheet_id)))fail('REPORT_NATIVE_ORIGINAL_ROOT_SET_CHANGED');
 }
 return {frame,command,completed};
}
