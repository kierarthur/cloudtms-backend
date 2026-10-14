import fs from 'node:fs';
import crypto from 'node:crypto';
const base='docs/healthroster-build/proposals/protected-source-capability',out=base+'/generated';
const root='../healthroster-v2-backend-13102026',protection=root+'/supabase/repeatable/13102026_1438_weekly_source_protection_queries';
fs.mkdirSync(out,{recursive:true});
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),inputs=[];
const read=file=>{const b=fs.readFileSync(file);inputs.push({file,bytes:b.length,sha256:sha(b)});return b.toString('utf8').replaceAll('\r\n','\n');};
export const literalReplace=(body,anchor,value,count=1)=>{if(body.split(anchor).length-1!==count)throw Error('ANCHOR_COUNT '+anchor.slice(0,100));return body.replaceAll(anchor,()=>value);};
const get=(body,name)=>{const found=[...body.matchAll(/create or replace function\s+([\w.]+)\s*\([\s\S]*?\bas\s+(\$[\w]*\$)[\s\S]*?\2\s*;/gi)].filter(m=>m[1]===name);if(found.length!==1)throw Error('OWNER_COUNT '+name);return found[0][0];};
const emit=(name,body)=>fs.writeFileSync(out+'/'+name,'-- Complete isolated source proposal. Root review and native proofs pending.\n'+body+'\n');
emit('01_schema.sqlinc',read(base+'/SCHEMA.sqlinc'));
emit('02_source_cas.sqlinc',read(base+'/SOURCE_CAS.sqlinc'));
emit('03_capability_owners.sqlinc',read(base+'/CAPABILITY_OWNERS.sqlinc'));
emit('04_original_clients.sqlinc',read(base+'/ORIGINAL_CLIENTS.sqlinc'));
emit('11_non_self_bill_admission.sqlinc',read(base+'/NONSELFBILL_ADMISSION.sqlinc'));
let activation=get(read(root+'/supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql'),'public.weekly_source_group_rule_feature_set_atomic_v1');
activation=literalReplace(activation,'  v_entry jsonb; v_apply_values jsonb; v_scope_date date; v_agency_invoice boolean:=false;',
 '  v_entry jsonb; v_apply_values jsonb; v_scope_date date; v_agency_invoice boolean:=false; v_qualification jsonb;');
activation=literalReplace(activation,"'client_settings_versions'],'WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID');", "'client_settings_versions','protected_capability_qualification_sha256'],'WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID');");
activation=literalReplace(activation,"v_purpose not in ('RULE_SET','SIGNED_CHECK_ONLY') or (v_version_id is null)<>(v_purpose='SIGNED_CHECK_ONLY')", "v_purpose not in ('RULE_SET','SIGNED_CHECK_ONLY','PROTECTED_SOURCE') or (v_version_id is null)<>(v_purpose in ('SIGNED_CHECK_ONLY','PROTECTED_SOURCE'))");
activation=literalReplace(activation,"  v_hash:=encode(extensions.digest(convert_to(p_request::text,'UTF8'),'sha256'),'hex');",`  if p_request ? 'protected_capability_qualification_sha256' and
    (jsonb_typeof(p_request->'protected_capability_qualification_sha256') is distinct from 'string'
      or coalesce(p_request->>'protected_capability_qualification_sha256','') !~ '^[0-9a-f]{64}$') then
    raise exception 'WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID' using errcode='22023'; end if;
  if v_purpose='PROTECTED_SOURCE' and v_enabled and not(p_request ? 'protected_capability_qualification_sha256') then
    raise exception 'WEEKLY_SOURCE_PROTECTED_CAPABILITY_RELEASE_UNQUALIFIED' using errcode='55000'; end if;
  if v_purpose='SIGNED_CHECK_ONLY' and p_request ? 'protected_capability_qualification_sha256' then
    raise exception 'WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID' using errcode='22023'; end if;
  v_hash:=encode(extensions.digest(convert_to(p_request::text,'UTF8'),'sha256'),'hex');`);
activation=literalReplace(activation,"  if v_assignment.feature_enabled=v_enabled then raise exception 'WEEKLY_SOURCE_RULE_FEATURE_ALREADY_SET' using errcode='22023'; end if;",`  -- Same owner requalification is metadata-only. Exact current assignment CAS,
  -- actor/deployment/latest VERIFIED and locked Client/Contract gates above stay.
  if v_enabled and p_request ? 'protected_capability_qualification_sha256' then
    v_qualification:=private.weekly_source_protected_capability_release_evidence_v1(v_group_id,v_from,
      v_assignment.effective_to_week_ending,p_request->>'protected_capability_qualification_sha256');
  elsif p_request ? 'protected_capability_qualification_sha256' then
    raise exception 'WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID' using errcode='22023'; end if;
  if v_assignment.feature_enabled=v_enabled then
    if not v_enabled or v_qualification is null or v_assignment.feature_purpose is distinct from v_purpose then
      raise exception 'WEEKLY_SOURCE_RULE_FEATURE_ALREADY_SET' using errcode='22023'; end if;
    if v_purpose='PROTECTED_SOURCE' then
      perform private.weekly_source_protected_source_activation_scope_v1(v_group_id,v_from,v_assignment.effective_to_week_ending);
    else
      select values_json into strict v_values from public.weekly_source_rule_set_versions where id=v_version_id;
      perform private._weekly_source_rule_group_context_v1(v_group_id,v_from,v_assignment.effective_to_week_ending,v_values,true);
    end if;
    if p_request ? 'client_settings_versions' and p_request->'client_settings_versions'<>'[]'::jsonb then
      raise exception 'WEEKLY_SOURCE_RULE_CLIENT_CAS_MANIFEST_INVALID' using errcode='22023'; end if;
    -- Preserve the exact AGENCY restore receipt, without calling Client save.
    v_receipt:=v_assignment.feature_receipt_json||jsonb_build_object('assignment_version',v_assignment.version+1,
      'release_id',v_release_id,'actor_user_id',v_actor,'changed_at_utc',transaction_timestamp(),'reason',v_reason,
      'request_id',v_request_id,'protected_capability_qualification',v_qualification,'requalification',true,'replayed',false);
    update public.weekly_source_group_rule_assignments set feature_changed_by_user_id=v_actor,
      feature_changed_at_utc=transaction_timestamp(),feature_change_reason=v_reason,feature_release_id=v_release_id,
      feature_request_id=v_request_id,feature_request_sha256=v_hash,feature_receipt_json=v_receipt,version=version+1 where id=v_assignment_id;
    return v_receipt;
  end if;`);
activation=literalReplace(activation,'    perform private._weekly_source_rule_group_context_v1(v_group_id,v_from,v_assignment.effective_to_week_ending,v_values,not v_agency_invoice);',`    if v_purpose='PROTECTED_SOURCE' then
      perform private.weekly_source_protected_source_activation_scope_v1(v_group_id,v_from,v_assignment.effective_to_week_ending);
    else
      perform private._weekly_source_rule_group_context_v1(v_group_id,v_from,v_assignment.effective_to_week_ending,v_values,not v_agency_invoice);
    end if;`);
activation=literalReplace(activation,"'client_changes',v_changes,'replayed',false);", "'client_changes',v_changes,'replayed',false)||case when v_qualification is null then '{}'::jsonb else\n      jsonb_build_object('protected_capability_qualification',v_qualification) end;");
emit('05_complete_existing_activation.sqlinc',activation);
let selection=get(read(protection+'/23_bulk_signed_selection.sqlinc'),'private.weekly_source_protect_all_signed_selection_v1');
selection=literalReplace(selection,'p_own_stage_context jsonb default null)', 'p_own_stage_context jsonb,p_original_client_ids uuid[])');
selection=literalReplace(selection,"  v_cas:=private.weekly_source_bulk_source_cas_assert_v1(v_cycle.id,v_clients);",`  if p_original_client_ids is not null then
    if array_position(p_original_client_ids,null::uuid) is not null or cardinality(p_original_client_ids)>100
      or exists(select 1 from unnest(v_clients) client where not(client=any(p_original_client_ids))) then
      raise exception 'WEEKLY_SOURCE_BULK_ORIGINAL_CLIENT_CENSUS_CHANGED' using errcode='40001'; end if;
    select coalesce(array_agg(distinct client order by client),'{}'::uuid[]) into v_clients from unnest(p_original_client_ids) client;
  end if;
  v_cas:=private.weekly_source_bulk_source_cas_assert_v1(v_cycle.id,v_clients);`);
const wrapper=`create or replace function private.weekly_source_protect_all_signed_selection_v1(p_source_cycle_id uuid,p_actor_user_id uuid,p_own_stage_context jsonb default null)
returns jsonb language sql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
  select private.weekly_source_protect_all_signed_selection_v1(p_source_cycle_id,p_actor_user_id,p_own_stage_context,null::uuid[]);
$function$;`;
emit('06_complete_selection.sqlinc',selection+'\n\n'+wrapper);
let stage=get(read(protection+'/25_bulk_stage_proof.sqlinc'),'private.weekly_source_bulk_stage_proof_v1');
stage=literalReplace(stage,'v_count integer; v_run public.weekly_exceptional_orchestration_runs%rowtype;', 'v_count integer; v_original_clients uuid[]; v_run public.weekly_exceptional_orchestration_runs%rowtype;');
stage=literalReplace(stage,"  v_selection:=private.weekly_source_protect_all_signed_selection_v1((p_stage->>'source_cycle_id')::uuid,(p_stage->>'actor_user_id')::uuid,v_context);",`  v_original_clients:=private.weekly_source_bulk_original_client_ids_v1((p_stage->>'source_cycle_id')::uuid,
    (p_stage->>'actor_user_id')::uuid,v_request->>'bulk_request_key');
  v_selection:=private.weekly_source_protect_all_signed_selection_v1((p_stage->>'source_cycle_id')::uuid,(p_stage->>'actor_user_id')::uuid,v_context,v_original_clients);`);
emit('07_complete_stage_proof.sqlinc',stage);
let command=get(read(protection+'/26_bulk_command.sqlinc'),'public.weekly_source_protect_all_signed_atomic_v1');
command=literalReplace(command,'v_completed boolean; v_initial_claim jsonb;', 'v_completed boolean; v_initial_claim jsonb; v_original_clients uuid[];');
command=literalReplace(command,"  if v_initial is null then raise exception 'WEEKLY_SOURCE_BULK_COMMAND_NOT_ADMITTED' using errcode='55000'; end if;",`  if v_initial is null then raise exception 'WEEKLY_SOURCE_BULK_COMMAND_NOT_ADMITTED' using errcode='55000'; end if;
  v_original_clients:=private.weekly_source_bulk_original_client_ids_v1(v_cycle_id,v_actor,v_key);`);
command=literalReplace(command,'private.weekly_source_protect_all_signed_selection_v1(v_cycle_id,v_actor,v_context)', 'private.weekly_source_protect_all_signed_selection_v1(v_cycle_id,v_actor,v_context,v_original_clients)',2);
emit('08_complete_bulk_command.sqlinc',command);
const privateSignatures=['weekly_source_bulk_source_facts_v1(uuid,uuid[])','weekly_source_bulk_source_cas_v1(uuid,uuid[])',
 'weekly_source_protected_capability_release_evidence_v1(uuid,date,date,text)','weekly_source_protected_source_activation_scope_v1(uuid,date,date)',
 'weekly_source_protected_capability_receipt_v1(uuid)','weekly_source_bulk_producer_ready_v1(uuid)',
 'weekly_source_automatic_reconcile_producer_ready_v1(uuid)','weekly_source_bulk_original_client_ids_v1(uuid,uuid,text)',
 'weekly_source_protect_all_signed_selection_v1(uuid,uuid,jsonb,uuid[])','weekly_source_protect_all_signed_selection_v1(uuid,uuid,jsonb)',
 'weekly_source_bulk_stage_proof_v1(jsonb,uuid)','weekly_source_protected_non_self_bill_mode_v1(uuid,uuid,date,uuid)'];
const publicSignatures=['weekly_source_group_rule_feature_set_atomic_v1(jsonb)','weekly_source_protect_all_signed_atomic_v1(jsonb)',
 'weekly_exceptional_pay_prepare_family_v1(jsonb)','weekly_exceptional_pay_prepare_action_v1(jsonb)',
 'weekly_exceptional_pay_action_context_v1(jsonb)','weekly_exceptional_pay_stage_c1_request_v1(jsonb)'];
const nonSelfBillOwners=[
 ['17_prepare_family.sqlinc','public.weekly_exceptional_pay_prepare_family_v1','v_client_id,v_contract_id,v_work_date,v_cycle.id'],
 ['18_prepare_action.sqlinc','public.weekly_exceptional_pay_prepare_action_v1','v_contract.client_id,v_contract.id,v_event.work_date,v_cycle.id'],
 ['19_action_context.sqlinc','public.weekly_exceptional_pay_action_context_v1','v_contract.client_id,v_contract.id,v_event.work_date,v_cycle.id'],
 ['20_stage_request.sqlinc','public.weekly_exceptional_pay_stage_c1_request_v1','v_client_id,v_family.contract_id,v_work_date,v_cycle.id']
];
for(const [file,name,args] of nonSelfBillOwners){
 let body=get(read(protection+'/'+file),name);
 body=literalReplace(body,'  v_source_mode text;', '  v_source_mode text; v_activated_source_mode text;');
 body=literalReplace(body,"  v_source_mode:=v_policy->>'c1_source_mode';", `  v_activated_source_mode:=private.weekly_source_protected_non_self_bill_mode_v1(${args});\n  v_source_mode:=coalesce(v_policy->>'c1_source_mode',v_activated_source_mode);`);
 body=literalReplace(body,"or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true", "or (coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true and v_activated_source_mode is null)");
 emit('12_complete_'+file,body);
}
emit('09_exact_acl_delta.sqlinc',privateSignatures.map(x=>`alter function private.${x} owner to postgres;\nrevoke all on function private.${x} from public,anon,authenticated,service_role;`).join('\n')+'\n'+publicSignatures.map(x=>`alter function public.${x} owner to postgres;\nrevoke all on function public.${x} from public,anon,authenticated;\ngrant execute on function public.${x} to service_role;`).join('\n'));
let managed=read(root+'/scripts/cloudtms-managed-release.mjs');
managed="import { buildQualification, assertManagedQualificationEvidence } from './weekly-source-protected-release-qualification.mjs';\n"+managed;
managed=literalReplace(managed,'  preapply, verify, log = ()=>{}, delay = ms=>new Promise(resolve=>setTimeout(resolve,ms)),',
 '  preapply, verify, protectedCapabilityJourneys = [], log = ()=>{}, delay = ms=>new Promise(resolve=>setTimeout(resolve,ms)),');
managed=literalReplace(managed,"    if (verified.sha256 !== context.expectedHash) throw Error('MANAGED_VERIFIED_CONTRACT_MISMATCH');",`    if (verified.sha256 !== context.expectedHash) throw Error('MANAGED_VERIFIED_CONTRACT_MISMATCH');
    // Root-only reviewed setup inputs; not CLI/env/browser flags. Native
    // prerequisite envelopes are byte-preserved and exact commit/contract-bound.
    if (!Array.isArray(protectedCapabilityJourneys)) throw Error('QUALIFICATION_INPUT_INVALID');
    if (protectedCapabilityJourneys.length) {
      if (verified.evidence?.weekly_source_protected_capability_qualifications !== undefined)
        throw Error('QUALIFICATION_EVIDENCE_REPLACEMENT_REFUSED');
      const rows=protectedCapabilityJourneys.map(journey=>{
        if (journey.context.release_id!==context.releaseId || journey.context.git_commit!==context.gitCommit
          || journey.context.contract_sha256!==context.expectedHash || journey.context.environment!==context.environment
          || journey.context.database!==context.expectedDatabase) throw Error('QUALIFICATION_MANAGED_SCOPE_INVALID');
        return buildQualification(journey.context,journey.receipts);
      });
      verified.evidence={...(verified.evidence??{}),weekly_source_protected_capability_qualifications:rows};
    }
    assertManagedQualificationEvidence(context,verified.evidence??{});`);
fs.writeFileSync(out+'/10_complete_managed_release.mjs',managed);
fs.writeFileSync(out+'/weekly-source-protected-release-qualification.mjs',read(base+'/release-qualification.mjs'));
const retained=[protection+'/25_bulk_stage_proof.sqlinc',protection+'/27_automatic_reconcile.sqlinc',
 root+'/supabase/repeatable/13102026_1431_weekly_source_invoice_money_owners_v1.sql',
 root+'/supabase/repeatable/13102026_1544_weekly_source_self_bill_reports_v1.sql',
 root+'/supabase/repeatable/13102026_1745_weekly_source_owned_stage_self_bill_facts_v1.sql',
 root+'/supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_binding.sqlinc',
 root+'/supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_pay_witness.sqlinc'];
for(const file of retained)if(fs.existsSync(file))read(file);
const artifacts=fs.readdirSync(out).filter(x=>x!== 'SOURCE_MANIFEST.json').sort().map(x=>{const b=fs.readFileSync(out+'/'+x);return {file:out+'/'+x,bytes:b.length,sha256:sha(b)};});
fs.writeFileSync(out+'/SOURCE_MANIFEST.json',JSON.stringify({schema_version:'PROTECTED_SOURCE_CAPABILITY_SOURCE_PROPOSAL_V1',inputs,artifacts,
 private_signatures:privateSignatures,public_signatures:publicSignatures,new_tables:0,new_columns:0,source_runtime_acceptance:false,
 sql_install_order:['01_schema.sqlinc','02_source_cas.sqlinc','03_capability_owners.sqlinc','04_original_clients.sqlinc',
  '11_non_self_bill_admission.sqlinc','05_complete_existing_activation.sqlinc','06_complete_selection.sqlinc',
  '07_complete_stage_proof.sqlinc','08_complete_bulk_command.sqlinc',...nonSelfBillOwners.map(([file])=>'12_complete_'+file),'09_exact_acl_delta.sqlinc'],
 registry_integration:'Root union only; retain every current private/helper/service row and grants; no replacement base census.'},null,2)+'\n');
process.stdout.write(JSON.stringify({generated:true,artifacts:artifacts.length,inputs:inputs.length,native_acceptance:false})+'\n');
