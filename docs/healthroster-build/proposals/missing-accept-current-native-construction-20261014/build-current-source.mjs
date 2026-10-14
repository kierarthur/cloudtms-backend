import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),proposals=path.dirname(dir),sha=b=>createHash('sha256').update(b).digest('hex'),proofs=[];
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')),'Never replace frozen construction');
function compose(input,file,hunks){const before=fs.readFileSync(input),eol=before.includes(Buffer.from('\r\n'))?'\r\n':'\n';let text=before.toString();const actual=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));for(const h of actual){assert.equal(text.split(h.before).length,2,h.name);text=text.replace(h.before,h.after);}const after=Buffer.from(text);for(const h of [...actual].reverse()){assert.equal(text.split(h.after).length,2,h.name);text=text.replace(h.after,h.before);}assert.deepEqual(Buffer.from(text),before);fs.mkdirSync(path.dirname(path.join(dir,'before',file)),{recursive:true});fs.mkdirSync(path.dirname(path.join(dir,'forward',file)),{recursive:true});fs.writeFileSync(path.join(dir,'before',file),before);fs.writeFileSync(path.join(dir,'forward',file),after);proofs.push({file,input:path.relative(root,input).replaceAll('\\','/'),before_bytes:before.length,before_sha256:sha(before),after_bytes:after.length,after_sha256:sha(after),unique_hunks:actual.length,hunks:actual,whole_reverse_verified:true});}
const original='missing-default-current-native-proposal-20261014';
compose(path.join(proposals,original,'before/public.weekly_source_later_change_decide_atomic_v1_jsonb_.sqlinc'),'05_complete_later_change_decide.sqlinc',[
 {name:'native missing proposal local',before:'  v_recorded jsonb;',after:'  v_recorded jsonb;\n  v_missing_accept jsonb;'},
 {name:'actual proof after I-1 and bundle admission',before:"  if v_decision='APPROVE_UPDATED_HOURS' then\n    -- The complete proposed entitlement",after:"  if v_decision='APPROVE_UPDATED_HOURS' then\n    v_missing_accept:=private.weekly_source_missing_accept_proposal_v1(v_bundle_id,v_bundle_revision,v_actor);\n  end if;\n\n  if v_decision='APPROVE_UPDATED_HOURS' then\n    -- The complete proposed entitlement"},
 {name:'only reserved purpose changes rebuilt vector',before:`    v_components:=private.weekly_source_retain_approved_additional_v2(
      v_root_timesheet_id,v_components
    );
  else`,after:`    v_components:=private.weekly_source_retain_approved_additional_v2(
      v_root_timesheet_id,v_components
    );
    if v_missing_accept is not null then
      if v_missing_accept#>>'{inputs,target_final_revision_id}' is distinct from v_final_revision_id::text then
        raise exception 'WEEKLY_SOURCE_LATER_CHANGE_REVIEW_STALE' using errcode='55000';
      end if;
      v_components:=v_missing_accept#>'{inputs,after_components}';
      v_chosen_segments:=v_missing_accept#>'{inputs,chosen_source_segments}';
    end if;
  else`}
]);
compose(path.join(proposals,'missing-correction-baseline-successor-20261014/forward/03_missing_cancel_basis.sqlinc'),'06_complete_missing_cancel_basis.sqlinc',[
 {name:'actual removal fact local',before:'  v_context jsonb; v_original jsonb; v_result jsonb; v_correction_baseline jsonb;',after:'  v_context jsonb; v_original jsonb; v_result jsonb; v_correction_baseline jsonb; v_accepted_removed boolean;'},
 {name:'real committed removal precedes ordinary financial classification',before:"  v_disposition:=case\n    when v_context->'invoiced'",after:"  v_accepted_removed:=v_context->'accepted_source_removed'='true'::jsonb;\n  v_disposition:=case\n    when v_accepted_removed then 'ACCEPTED_SOURCE_REMOVED'\n    when v_context->'invoiced'"},
 {name:'current accepted proof resolves position independently of old ledger',before:"  if v_original->>'movement_id' is distinct from v_context->>'current_pay_head_movement_id' then",after:"  if not coalesce(v_accepted_removed,false)\n    and v_original->>'movement_id' is distinct from v_context->>'current_pay_head_movement_id' then"},
 {name:'freeze real removal proof, no paid or cash assertion',before:'  if v_correction_baseline is not null then',after:"  if v_accepted_removed then\n    v_result:=v_result||jsonb_build_object('accepted_removals',v_context->'accepted_source_removal_proofs');\n  end if;\n  if v_correction_baseline is not null then"},
 {name:'only real keep dispositions exclude native cancellation',before:"'exclude_cancel',v_disposition<>'CANCEL_NOT_PAID_OR_INVOICED'",after:"'exclude_cancel',v_disposition in ('KEEP_INVOICED','KEEP_PAID_NOT_INVOICED')"}
]);
compose(path.join(proposals,'missing-correction-baseline-successor-20261014/forward/14_missing_prepared_census_recheck.sqlinc'),'07_complete_missing_prepared_census.sqlinc',[
 {name:'actual removal disposition still re-proves whole native context',before:`    elsif v_entry.value->>'disposition' is distinct from 'CANCEL_NOT_PAID_OR_INVOICED'`,after:`    elsif v_entry.value->>'disposition'='ACCEPTED_SOURCE_REMOVED' then
      if v_context->'accepted_source_removed' is distinct from 'true'::jsonb
        or v_context->'accepted_source_removal_proofs' is distinct from v_basis->'accepted_removals' then
        raise exception 'WEEKLY_SOURCE_MISSING_PREPARED_ACCEPTED_REMOVAL_CHANGED' using errcode='40001';
      end if;
    elsif v_entry.value->>'disposition' is distinct from 'CANCEL_NOT_PAID_OR_INVOICED'`}
]);
compose(path.join(proposals,original,'forward/01_target_publication_context.sqlinc'),'08_complete_kept_context.sqlinc',[
 {name:'current accepted removal locals',before:'  v_applies boolean:=false; v_ack_sequence bigint;',after:"  v_applies boolean:=false; v_ack_sequence bigint;\n  v_accept_complete boolean:=false;v_accept_proofs jsonb:='[]'::jsonb;v_accept_current jsonb;v_result jsonb;"},
 {name:'all current native roots must have the real committed removal',before:"  return jsonb_build_object('schema_version','WEEKLY_SOURCE_KEPT_AS_PAID_CONTEXT_V1',",after:`  if not v_source_present and cardinality(v_roots)>0 then
    v_accept_complete:=true;
    foreach v_root in array v_roots loop
      v_accept_current:=private.weekly_source_missing_accept_current_removal_v1(v_event.id,v_root,v_publication,p_actor_user_id);
      if v_accept_current is null then v_accept_complete:=false;v_accept_proofs:='[]'::jsonb;exit;end if;
      v_accept_proofs:=v_accept_proofs||jsonb_build_array(v_accept_current);
    end loop;
  end if;
  v_result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_KEPT_AS_PAID_CONTEXT_V1',`},
 {name:'accepted current removal cannot present default Keep',before:"'kept_by_default',not v_source_present and v_invoiced,",after:"'kept_by_default',not v_accept_complete and not v_source_present and v_invoiced,"},
 {name:'genuine completed decision does not ask for a new Missing choice',before:"'decision_available',not v_source_present and (v_invoiced or v_paid is true),",after:"'decision_available',not v_accept_complete and not v_source_present and (v_invoiced or v_paid is true),"},
 {name:'retain unknown paid and resolve only Source decision need',before:"'decision_needed',case when not v_source_present",after:"'decision_needed',case when v_accept_complete then false when not v_source_present"},
 {name:'existing row pay hold meaning only',before:"'holds_pay',case when not v_source_present",after:"'holds_pay',case when v_accept_complete then false when not v_source_present"},
 {name:'existing row invoice hold meaning only',before:"'holds_invoice',case when not v_source_present",after:"'holds_invoice',case when v_accept_complete then false when not v_source_present"},
 {name:'conditional real accepted proof preserves ordinary context domain',before:`    'holds_invoice',case when v_accept_complete then false when not v_source_present and (v_invoiced or v_paid is true) then false else null end);
end;`,after:`    'holds_invoice',case when v_accept_complete then false when not v_source_present and (v_invoiced or v_paid is true) then false else null end);
  if v_accept_complete then
    v_result:=v_result||jsonb_build_object('accepted_source_removed',true,'accepted_source_removal_proofs',v_accept_proofs);
  end if;
  return v_result;
end;`}
]);
const commit='33927fff2f7b90b90241d746d9e35aa21c57bd07',routeFile='broker/src/weekly-source/routes.js',route=execFileSync('git',['show',commit+':'+routeFile],{cwd:root,maxBuffer:1e6});assert.equal(sha(route),'8fb9728d252597d60512101ea6908dee7087f4c638d324f0aa9fc9cc9dae13ef');fs.mkdirSync(path.join(dir,'input'),{recursive:true});fs.writeFileSync(path.join(dir,'input/current_routes.js'),route);
compose(path.join(dir,'input/current_routes.js'),routeFile,[
 {name:'finite closed preparation parser import',before:"import {checklistReadRequest,decodeChecklistRead,ChecklistReadError} from './checklist-read.mjs';",after:"import {checklistReadRequest,decodeChecklistRead,ChecklistReadError} from './checklist-read.mjs';\nimport {missingAcceptPrepareRequest,missingAcceptContextRequest} from './missing-accept.mjs';"},
 {name:'real Office admin commands to literal Source context/producer only',before:"  if (action === 'LATER_CHANGE_DECISION') {",after:`  if (action === 'MISSING_ACCEPT_CONTEXT' || action === 'PREPARE_MISSING_ACCEPT') {
    if (body.action !== action || Object.keys(body).length !== 2 || !Object.hasOwn(body,'payload')) {
      fail('WEEKLY_SOURCE_MISSING_ACCEPT_PREPARE_INVALID','This proposal could not be prepared.',400);
    }
    let preparation;
    try { preparation=action==='MISSING_ACCEPT_CONTEXT'
      ? missingAcceptContextRequest(payload,user.id) : missingAcceptPrepareRequest(payload,user.id); }
    catch { fail('WEEKLY_SOURCE_MISSING_ACCEPT_PREPARE_INVALID','This proposal could not be prepared.',400); }
    let owner;
    try {
      if (typeof dependencies.bpayNextOwnerStatusRpc !== 'function') throw new Error();
      owner=normalizeBpayNextOwnerStatus(await dependencies.bpayNextOwnerStatusRpc(env));
    } catch { fail('BPAY_NEXT_OWNER_STATUS_UNAVAILABLE','This proposal is unavailable; try again shortly.',503); }
    if (owner.active_owner !== 'NEXT') {
      fail('WEEKLY_SOURCE_COMMAND_NOT_SUPPORTED','This proposal is not available for this agency yet.',409);
    }
    const rpc=action==='MISSING_ACCEPT_CONTEXT' ? 'weekly_source_missing_accept_context_v1'
      : 'weekly_source_missing_accept_prepare_atomic_v1';
    const result=await withRefusalRecording(dependencies,'broker:weekly-source:'+rpc,user.id,
      ()=>callRpc(dependencies,rpc,preparation));
    return jsonResponse(200,result);
  }

  if (action === 'LATER_CHANGE_DECISION') {`}
]);
fs.writeFileSync(path.join(dir,'COMPLETE_CURRENT_REVERSES.json'),JSON.stringify({source_only:true,unreviewed:true,current_input_commit:commit,current_route_before_sha256:sha(route),complete_owners:proofs.length,unique_hunks:proofs.reduce((n,p)=>n+p.unique_hunks,0),proofs,canonical_mounted:false,native_proof:false},null,2)+'\n');console.log(JSON.stringify({complete_reverses:proofs.length,unique_hunks:proofs.reduce((n,p)=>n+p.unique_hunks,0),outputs:proofs.map(p=>({file:p.file,sha256:p.after_sha256}))}));
const namespace=fs.readFileSync(path.join(dir,'one-time/00_missing_accept_reserved_namespace.sqlinc')),nativeGuard=fs.readFileSync(path.join(dir,'forward/02_missing_accept_proof_guard.sqlinc'));
fs.writeFileSync(path.join(dir,'one-time/01_complete_reserved_namespace_guard.sqlinc'),Buffer.concat([Buffer.from('-- Execute inside the one-time atomic migration. Hold the native table lock\n-- through empty-namespace admission and all reserved guard/index creation.\nlock table public.audit_events in share row exclusive mode;\n'),namespace,Buffer.from('\n'),nativeGuard]));
const inputFiles=['supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql','supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','supabase/migrations/04102026_2155_weekly_source_accepted_publication_actions_v1.sql','supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql','supabase/repeatable/17092026_0300_weekly_source_entitlement_publication_v1.sql','supabase/repeatable/04102026_2206_weekly_source_accepted_publication_evidence_v1.sql','supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10b_complete_remaining_manifest_consumers.sqlinc','broker/src/payment-actor-permission.js','broker/src/bpay-busy.js',routeFile];
const pins=inputFiles.map(file=>{const b=execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:4e6});return {commit,file,bytes:b.length,sha256:sha(b)};});
fs.writeFileSync(path.join(dir,'CURRENT_INPUT_PINS.json'),JSON.stringify({source_only:true,current_input_commit:commit,current_native_inputs:pins,reviewed_missing_prerequisite_commit:'be5d1a194b8d34b40209d29083359c308241fbcf',selected_before_delimiter:'same complete leaves from frozen reviewed packets, raw bytes retained',one_time_guard_sha256:sha(nativeGuard),one_time_guard_same_complete_definition:true,shared_banking_owner_changes:0,canonical_mounted:false,native_proof:false},null,2)+'\n');
