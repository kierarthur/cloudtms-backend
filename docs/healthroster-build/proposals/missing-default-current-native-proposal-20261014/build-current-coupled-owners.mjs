import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),commit='33927fff2f7b90b90241d746d9e35aa21c57bd07',sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));const proofs=[];
function owner(id,file,name,edit,proposedShared=false){const whole=execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:4e6});const e=functionEvents(whole.toString('utf8')).events.filter(e=>e.kind==='CREATE'&&e.id===id).at(-1);assert.ok(e,id);const old=e.definition+'\n',nl=old.includes('\r\n')?'\r\n':'\n',norm=s=>s.replaceAll('\r\n','\n').replaceAll('\n',nl);let source=old;const hunks=[];
const replace=(label,b,a)=>{b=norm(b);a=norm(a);assert.equal(source.split(b).length,2,label);assert.notEqual(a,b);source=source.replace(b,a);assert.equal(source.split(a).length,2,label+' after');hunks.push({name:label,before:b,after:a});};edit(replace);
let reverse=source;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,old);
const before='before/'+name+'.sqlinc',forward='forward/'+name+'.sqlinc';fs.writeFileSync(path.join(dir,before),old);fs.writeFileSync(path.join(dir,forward),source);
proofs.push({owner:id,input_current_commit:commit,input_file:file,input_file_bytes:whole.length,input_file_sha256:sha(whole),native_definition_sha256:sha(e.definition),selected_before_delimiter:'exact declaration plus one LF',before_file:before,before_sha256:sha(old),before_bytes:Buffer.byteLength(old),forward_file:forward,forward_sha256:sha(source),forward_bytes:Buffer.byteLength(source),unique_hunks:hunks.length,hunks,whole_reverse_verified:true,shared_banking_owner_change_proposed:proposedShared,shared_banking_approval:false,canonical_modified:false});}
owner('private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)',
 'supabase/repeatable/14102026_0541_healthroster_report_current_native_composition/04_complete_fin3_dispatch.sqlinc',
 '11_complete_fin3_current_fin4_dispatch',replace=>replace('ordinary current owner delegation after unchanged Report arm',
 `    return private.weekly_source_report_fin_dispatch_v1(p_request);
  end if;

  -- G1 plain scope discovery -> native complete I-1 -> group -> cycle.`,
 `    return private.weekly_source_report_fin_dispatch_v1(p_request);
  end if;

  -- All genuine ordinary callers share the one corrected complete FIN4 owner.
  -- The validated Report early arm and native payload/actor/error prefix above
  -- remain unchanged; rounding variant NULL is unused for ordinary scopes.
  return private.weekly_source_finalise_engine_v1(p_request,p_correction_session_id,p_prepare_only,null::text);

  -- G1 plain scope discovery -> native complete I-1 -> group -> cycle.`));
owner('private.bpay_next_source_current_inventory_v1(uuid,integer,uuid,uuid,uuid,date,date)',
 'supabase/repeatable/04102026_0505_banking_pay_next_source_current_hooks_v1.sql',
 '12_shared_current_inventory_proposal',replace=>replace('real Source prior/retained roots in the upload producer lock census',
 `  where u.upload_id=p_upload and cw.timesheet_id is not null
  union
  select s.root_timesheet_id`,
 `  where u.upload_id=p_upload and cw.timesheet_id is not null
  union
  -- Shared Banking owner PROPOSAL, not approved or mounted. Exact actual
  -- Source HR producer scope may include uncaptured pre-NEXT prior work.
  -- No p_upload means no added arm; NHSP and non-HR payloads remain inert.
  select source.root_timesheet_id from private.weekly_source_missing_finalisation_roots_v1(
    p_upload,p_generation,p_old_revision,p_group,p_client,p_start,p_end) source
  union
  select s.root_timesheet_id`),true);
owner('private.weekly_source_invoice_duty_v1(uuid)',
 'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10c_complete_root_report_consumer_union.sqlinc',
 '13_complete_invoice_duty_manifest_coupling',replace=>{replace('exact stored Source census and immutable rule-version manifest fields',
 `               'policy_fingerprint',encode(f.policy_fingerprint,'hex')))) then`,
 `               'policy_fingerprint',encode(f.policy_fingerprint,'hex'))
               ||case when f.missing_cancel_basis_census='{}'::jsonb then '{}'::jsonb else jsonb_build_object(
                 'missing_cancel_basis_census',f.missing_cancel_basis_census) end
               ||case when f.rule_set_version_id is null then '{}'::jsonb else jsonb_build_object(
                 'rule_set_version_id',f.rule_set_version_id,'rule_set_values_sha256',(
                   select version.values_sha256 from public.weekly_source_rule_set_versions version
                   where version.id=f.rule_set_version_id)) end)) then`);
 replace('invoice duty preserves full prepared-result census CAS',
 `      or v_context->'root_contexts' is distinct from v_session.prepare_result_json->'root_contexts'`,
 `      or v_context->'root_contexts' is distinct from v_session.prepare_result_json->'root_contexts'
      or coalesce(v_context->'missing_cancel_basis_census','{}'::jsonb)
        is distinct from coalesce(v_session.prepare_result_json->'missing_cancel_basis_census','{}'::jsonb)`);});
owner('private.weekly_source_correct_final_prepared_context_v1(uuid)',
 'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/09_complete_pay_consumers.sqlinc',
 '15_complete_prepared_context_missing_census',replace=>replace('actual prepared missing census separate from existing projection root contexts',
 `    'root_contexts',v_contexts
  );`,
 `    'root_contexts',v_contexts
  )||case when v_revision.missing_cancel_basis_census='{}'::jsonb then '{}'::jsonb else jsonb_build_object(
    'missing_cancel_basis_census',private.weekly_source_missing_prepared_census_recheck_v1(v_revision.id,v_session.actor_user_id)) end;`));
owner('public.weekly_source_correct_final_prepare_atomic_v1(jsonb)',
 'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10c_complete_root_report_consumer_union.sqlinc',
 '16_complete_prepare_result_missing_census',replace=>replace('prepared result hash binds the actual separately rechecked census',
 `    'idempotent_replay',false
  );
  v_result_hash:=`,
 `    'idempotent_replay',false
  )||case when v_context ? 'missing_cancel_basis_census' then jsonb_build_object(
    'missing_cancel_basis_census',v_context->'missing_cancel_basis_census') else '{}'::jsonb end;
  v_result_hash:=`));
owner('public.weekly_source_correct_final_apply_atomic_v1(jsonb)',
 'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10c_complete_root_report_consumer_union.sqlinc',
 '17_complete_apply_missing_census_recheck',replace=>replace('existing under-lock context CAS includes full actual prepared missing census',
 `  if v_server_roots is distinct from v_session.prepare_result_json->'root_contexts' then`,
 `  if v_server_roots is distinct from v_session.prepare_result_json->'root_contexts'
    or coalesce(v_prepared_context->'missing_cancel_basis_census','{}'::jsonb)
      is distinct from coalesce(v_session.prepare_result_json->'missing_cancel_basis_census','{}'::jsonb) then`));
fs.writeFileSync(path.join(dir,'CURRENT_COUPLED_OWNER_COMPLETE_REVERSES.json'),JSON.stringify({source_only:true,unfrozen_draft:true,source_current_commit:commit,proofs,sql_compiled:false,native_proof:false,canonical_modified:false,full_primary_complete:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,owners:proofs.length,full_reverses:proofs.length,shared_proposals:proofs.filter(p=>p.shared_banking_owner_change_proposed).length}));
