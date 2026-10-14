import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),commit='03221cb237f59658ecde5217ff84e45d3ab48be8',
 file='supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker/04_complete_fin_overload.sqlinc',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const source=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',commit+':'+file],{cwd:own,encoding:'utf8',maxBuffer:16*1024*1024});
if(sha(source)!=='644b4537ce710ff47700e63561e569d09eeccdcb42e8025a1d09d7975fc874a7')throw Error('CURRENT_FIN4_SOURCE_CHANGED');
let output=source;const hunks=[];
function replace(before,after){if(output.split(before).length!==2)throw Error('FIN4_OBSERVATION_HUNK_NOT_UNIQUE');output=output.replace(before,()=>after);hunks.push({before,after,before_sha256:sha(before),after_sha256:sha(after)});}
replace('  v_finalisation_lock_result jsonb;','  v_finalisation_lock_result jsonb;\n  v_report_native_context jsonb;\n  v_report_native_command jsonb;\n  v_report_native_frame jsonb;\n  v_report_native_command_count integer;');
const anchor='  -- One transaction at a time may decide the final order for a logical';
const observation=`  -- Owner-only native observation at the actual under-lock recheck. The
  -- Report arm returned earlier; this captures the ordinary FIN4 ladder only.
  -- Existing command context validates the real OFF admission/XID/PID/stack.
  -- Normal calls have no context and acquire no additional audit/economic fact.
  v_report_native_context:=private.weekly_source_rule_qualification_context_v1(v_client_id,v_cycle.id);
  if v_report_native_context is not null then
    select count(*),jsonb_agg(audit.after_json)->0 into v_report_native_command_count,v_report_native_command
      from public.audit_events audit
      where audit.object_type='weekly_source_off_rule_command' and audit.action='WEEKLY_SOURCE_OFF_RULE_COMMAND_ADMITTED'
        and audit.event_sequence_is_authoritative and audit.actor_user_id=v_actor
        and audit.after_json->>'transaction_id'=pg_current_xact_id_if_assigned()::text
        and audit.after_json->>'backend_pid'=pg_backend_pid()::text
        and not exists(select 1 from public.audit_events completed where completed.object_type=audit.object_type
          and completed.object_id_text=audit.object_id_text and completed.action='WEEKLY_SOURCE_OFF_RULE_COMMAND_COMPLETED'
          and completed.event_sequence_is_authoritative);
    if v_report_native_command_count<>1 then
      raise exception 'REPORT_NATIVE_OFF_FIN_SCOPE_UNPROVED' using errcode='55000';
    end if;
    if v_report_native_command->>'native_operation'='weekly_source_finalise_atomic_v1' then
    if v_report_native_command->>'schema_version' is distinct from 'WEEKLY_SOURCE_OFF_RULE_COMMAND_V1'
      or v_report_native_command->>'native_operation' is distinct from 'weekly_source_finalise_atomic_v1'
      or v_report_native_command->>'actor_user_id' is distinct from v_actor::text
      or v_report_native_command->>'source_cycle_id' is distinct from v_cycle.id::text
      or v_report_native_command->'authorization' is distinct from v_report_native_context
      or v_report_native_command->'operational_ready' is distinct from 'false'::jsonb
      or v_report_native_command#>>'{native_parent_scope,schema_version}' is distinct from 'WEEKLY_SOURCE_OFF_RULE_NATIVE_PARENT_SCOPE_V1'
      or v_report_native_command#>>'{native_parent_scope,source_cycle_id}' is distinct from v_cycle.id::text
      or v_report_native_command#>'{native_parent_scope,ambiguous}' is distinct from 'false'::jsonb
      or v_report_native_command->>'native_request_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_OFF_RULE_NATIVE_REQUEST_V1',p_request),'hex')
      or v_report_native_command->>'command_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_OFF_RULE_COMMAND_V1',v_report_native_command-'command_sha256'),'hex') then
      raise exception 'REPORT_NATIVE_OFF_FIN_SCOPE_UNPROVED' using errcode='55000';
    end if;
    v_report_native_frame:=jsonb_build_object('schema_version','WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_CONTEXT_V1',
      'execution',private.weekly_source_protected_qualification_release_v1(),
      'command_id',v_report_native_command->>'command_id','command_sha256',v_report_native_command->>'command_sha256',
      'authorization_sha256',v_report_native_context->>'authorization_sha256','native_operation',v_report_native_command->>'native_operation',
      'actor_user_id',v_actor,'source_cycle_id',v_cycle.id,'upload_id',v_upload.id,'projection_publication_id',v_publication.id,
      'generation',v_generation,'authority_scope_kind',v_scope_kind,'prepare_only',p_prepare_only,
      'native_argument_bytes',jsonb_build_object('p_request',p_request,'p_correction_session_id',p_correction_session_id,
        'p_prepare_only',p_prepare_only,'p_report_vat_rounding_variant',p_report_vat_rounding_variant)::text,
      'backend_pid',pg_backend_pid()::text,'transaction_id',pg_current_xact_id_if_assigned()::text,
      'active_owner',(select active_owner from private.bpay_next_module_control where id=1),
      'original_root_ids',to_jsonb(v_finalisation_root_ids),
      'rechecked_root_ids',to_jsonb(v_finalisation_root_recheck),
      'root_set_setting',current_setting('cloudtms.weekly_source_finalisation_root_set',true),
      'operation_id',v_finalisation_lock_operation,'operation_setting',current_setting('cloudtms.weekly_source_finalisation_lock_operation',true),
      'operation',(select to_jsonb(operation) from private.bpay_next_source_current_lock_operations operation where operation.id=v_finalisation_lock_operation),
      'operation_members',coalesce((select jsonb_agg(to_jsonb(member) order by member.canonical_booking_id,member.raw_booking_id,member.root_timesheet_id)
        from private.bpay_next_source_current_lock_members member where member.operation_id=v_finalisation_lock_operation),'[]'::jsonb),
      'lock_result',v_finalisation_lock_result,'source_lock_discovery',v_lock_discovery,'source_lock_observed',v_lock_observed,
      'window_lock_discovery',v_window_discovery,'window_lock_observed',v_window_observed,
      'source_cas',private.weekly_source_bulk_source_cas_v1(v_cycle.id,
        array(select value::uuid from jsonb_array_elements_text(v_report_native_context#>'{fixture_manifest,client_ids}') value)),
      'source_facts_bytes',private.weekly_source_bulk_source_facts_v1(v_cycle.id,
        array(select value::uuid from jsonb_array_elements_text(v_report_native_context#>'{fixture_manifest,client_ids}') value))::text,
      'operational_ready',false);
    v_report_native_frame:=v_report_native_frame||jsonb_build_object('frame_sha256',encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_CONTEXT_V1',v_report_native_frame),'hex'));
    perform public._audit_insert('weekly_source_off_rule_native_fin',v_report_native_command->>'command_id',
      'WEEKLY_SOURCE_REPORT_FIN_NATIVE_LOCK_OBSERVED',v_report_native_command,v_report_native_frame,
      'Native ordinary FIN4 under-lock observation; non-operational fixture only',v_actor);
    end if;
  end if;

`;
replace(anchor,observation+anchor);
const target=path.join(dir,'forward',file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,output);
let reverse=output;for(const h of [...hunks].reverse()){if(reverse.split(h.after).length!==2)throw Error('FIN4_REVERSE_NOT_UNIQUE');reverse=reverse.replace(h.after,()=>h.before);}if(reverse!==source)throw Error('FIN4_ENTIRE_REVERSE_FAILED');
fs.writeFileSync(path.join(dir,'FIN4_NATIVE_LOCK_OBSERVATION_REVERSE.json'),JSON.stringify({schema_version:'REPORT_NATIVE_FIN4_OBSERVATION_ENTIRE_SOURCE_REVERSE_V1',
 input_commit:commit,input_file:file,input_sha256:sha(source),output_file:'forward/'+file,output_sha256:sha(output),hunks,
 complete_reverse_matches_current_git:true,report_early_leaf_and_variant_unchanged:true,new_signatures:0,new_gucs:0,new_selectors:0,
 normal_call_economic_and_lock_body_unchanged:true,no_native_execution:true},null,2)+'\n');
console.log(JSON.stringify({input_sha256:sha(source),output_sha256:sha(output),hunks:hunks.length,entire_reverse:true}));
