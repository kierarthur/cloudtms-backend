import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];
if(!root)throw Error('EXPLICIT_READ_ONLY_ROOT_REQUIRED');
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const read=file=>fs.readFileSync(path.join(root,file),'utf8');
const write=(file,value)=>fs.writeFileSync(path.join(out,file),value);
const native='supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/04_complete_fin.sqlinc';
const q='docs/healthroster-build/proposals/protected-source-qualification-operational-gate/';
const signatures=[
 'private.weekly_source_report_fin_certificate_assert_v1(jsonb,uuid,uuid,date,date)',
 'private.weekly_source_report_fin_release_evidence_v1(uuid,uuid,date,date)',
 'private.weekly_source_report_fin_feature_binding_v1(uuid,uuid,date,date)',
 'private.weekly_source_report_fin_strategy_v1(uuid)',
 'private.weekly_source_report_fin_dispatch_v1(jsonb)',
 'private.weekly_source_report_fin_native_comparisons_assert_v1(jsonb,jsonb,text)',
 'private.weekly_source_report_fin_qualifications_assert_v1(jsonb,jsonb,text)'];
const pinsFile=path.join(out,'CONTROLLING_INPUTS.json');
const pins=JSON.parse(fs.readFileSync(pinsFile,'utf8'));
for(const row of pins.inputs)if(sha(fs.readFileSync(path.join(root,row.path)))!==row.sha256)throw Error('INPUT_CHANGED: '+row.path);
const receipts=[];
function compose(input,output,hunks){
 const before=read(input);let after=before;
 for(const hunk of hunks){if(after.split(hunk.old).length!==2)throw Error('HUNK_NOT_UNIQUE: '+output+' '+hunk.id);
  after=after.replace(hunk.old,()=>hunk.new);}
 let reverse=after;for(const hunk of [...hunks].reverse()){
  if(reverse.split(hunk.new).length!==2)throw Error('REVERSE_NOT_UNIQUE: '+output+' '+hunk.id);
  reverse=reverse.replace(hunk.new,()=>hunk.old);}
 if(reverse!==before)throw Error('FULL_REVERSE_FAILED: '+output);
 write(output,after);receipts.push({input,output,input_sha256:sha(before),output_sha256:sha(after),full_reverse:true,hunks});
}
const reportArm=`
  -- Qualified operational Report arm AFTER unchanged payload/ID/hash/native admission.
  -- The exact explicit four-argument A/B qualification owner remains unchanged.
  if v_scope_kind='SELF_BILL_REPORT_SCOPE' then
    select * into v_scope from public.weekly_source_report_scopes where id=v_report_scope_id;
    select * into v_cycle from public.weekly_source_cycles where id=v_cycle_id;
    if v_scope.id is null or v_cycle.id is null or v_scope.self_bill_report_id is null
      or v_scope.source_cycle_id is distinct from v_cycle.id
      or v_scope.source_group_id is distinct from v_cycle.source_group_id then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_MISMATCH' using errcode='22023'; end if;
    perform private.weekly_source_office_authority_v1(v_actor,
      case when p_prepare_only then 'CORRECT_FINAL_SOURCE' else 'FINALISE_WEEK' end,
      v_cycle.source_group_id,v_scope.client_id,v_cycle.finalisation_week_ending);
    if p_prepare_only or p_correction_session_id is not null then
      raise exception 'WEEKLY_SOURCE_SELF_BILL_REPORT_CORRECT_FINAL_UNSUPPORTED' using errcode='55000'; end if;
    return private.weekly_source_report_fin_dispatch_v1(p_request);
  end if;
`;
compose(native,'04_complete_fin.sqlinc',[
 {id:'REPORT_SCOPE_ONLY',old:"     or v_scope_kind not in ('CYCLE','NHSP_REPORT_SCOPE')\n     or (v_scope_kind='NHSP_REPORT_SCOPE') is distinct from (v_report_scope_id is not null)",
  new:"     or v_scope_kind not in ('CYCLE','NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE')\n     or (v_scope_kind in ('NHSP_REPORT_SCOPE','SELF_BILL_REPORT_SCOPE')) is distinct from (v_report_scope_id is not null)"},
 {id:'POST_NATIVE_ADMISSION_REPORT_ARM',old:'  perform private.weekly_source_pay_query_admit_v2();\n',
  new:'  perform private.weekly_source_pay_query_admit_v2();\n'+reportArm}]);
const reportBind=`  -- Server-derived Report binding, using only the actual saved immutable
  -- version/range. No browser strategy/hash field and no default A/B.
  if v_enabled and v_purpose='RULE_SET' then
    v_report_qualification:=private.weekly_source_report_fin_feature_binding_v1(v_group_id,v_version_id,v_from,v_assignment.effective_to_week_ending);
  end if;
`;
compose(q+'07_complete_existing_feature_setup.sqlinc','05_complete_feature_setup.sqlinc',[
 {id:'REPORT_LOCAL',old:'v_agency_invoice boolean:=false; v_qualification jsonb;',new:'v_agency_invoice boolean:=false; v_qualification jsonb; v_report_qualification jsonb;'},
 {id:'REPLAY_REPORT_CERTIFICATE',old:"    return v_assignment.feature_receipt_json||jsonb_build_object('replayed',true);",
  new:"    if v_assignment.feature_enabled and v_assignment.feature_receipt_json ? 'report_fin_qualification' then\n      perform private.weekly_source_report_fin_certificate_assert_v1(v_assignment.feature_receipt_json->'report_fin_qualification',\n        v_group_id,v_version_id,v_from,v_assignment.effective_to_week_ending);\n    end if;\n    return v_assignment.feature_receipt_json||jsonb_build_object('replayed',true);"},
 {id:'LOCKED_ACTUAL_REPORT_BINDING',old:'  if v_assignment.feature_enabled=v_enabled then\n',new:reportBind+'  if v_assignment.feature_enabled=v_enabled then\n'},
 {id:'REPORT_REQUALIFICATION_ONLY',old:'    if not v_enabled or v_qualification is null or v_assignment.feature_purpose is distinct from v_purpose then',
  new:'    if not v_enabled or (v_qualification is null and v_report_qualification is null) or v_assignment.feature_purpose is distinct from v_purpose then'},
 {id:'PRESERVE_CAPABILITY_REPORT_REQUAL_RECEIPT',old:"      'request_id',v_request_id,'protected_capability_qualification',v_qualification,'requalification',true,'replayed',false);",
  new:"      'request_id',v_request_id,'requalification',true,'replayed',false)\n      ||case when v_qualification is null then '{}'::jsonb else jsonb_build_object('protected_capability_qualification',v_qualification) end\n      ||case when v_report_qualification is null then '{}'::jsonb else jsonb_build_object('report_fin_qualification',v_report_qualification) end;"},
 {id:'REPORT_INITIAL_RECEIPT',old:"      jsonb_build_object('protected_capability_qualification',v_qualification) end;",
  new:"      jsonb_build_object('protected_capability_qualification',v_qualification) end\n      ||case when v_report_qualification is null then '{}'::jsonb else jsonb_build_object('report_fin_qualification',v_report_qualification) end;"}]);
const writer=q+'14_complete_managed_release.mjs';
// Post-install evidence is recorded through Money's dedicated existing-owner
// action. Never apply/resume or change the installation writer for certificate
// retention. Full owners are retained for independent byte/reverse proof.
compose(writer,'06_complete_managed_release.mjs',[]);
compose(q+'weekly-source-protected-operational-evidence.mjs','09_complete_operational_evidence.mjs',[
 {id:'CLOSED_OPTIONAL_REPORT_BUNDLE_SECTION',old:" exact(bundle,['schema_version','qualifications'],'OPERATIONAL_BUNDLE_INVALID');",
  new:" exact(bundle,['schema_version','qualifications',...(bundle.report_fin_qualifications===undefined?[]:['report_fin_qualifications'])],'OPERATIONAL_BUNDLE_INVALID');"},
 {id:'REPORT_ONLY_BUNDLE_WITHOUT_CAPABILITY_FABRICATION',old:"  ||!bundle.qualifications.length||bundle.qualifications.length>100)fail('OPERATIONAL_BUNDLE_INVALID');",
  new:"  ||(!bundle.qualifications.length&&(bundle.report_fin_qualifications===undefined||!Array.isArray(bundle.report_fin_qualifications)||!bundle.report_fin_qualifications.length))\n  ||bundle.qualifications.length>100)fail('OPERATIONAL_BUNDLE_INVALID');"}]);
compose(q+'15_complete_current_release_caller.mjs','10_complete_current_release_caller.mjs',[]);
write('weekly-source-protected-release-qualification.mjs',read(q+'weekly-source-protected-release-qualification.mjs'));
write('weekly-source-protected-execution-tree.mjs',read('docs/healthroster-build/proposals/strict-qualification-git-path-parser-forward/weekly-source-protected-execution-tree.mjs'));
const prefix="create or replace function private._weekly_source_acl_private_helper_contract_v1()\nreturns table(function_signature text)\nlanguage sql\nimmutable\nset search_path to 'pg_catalog','pg_temp'\nas $function$\n  values\n";
for(const [input,output] of [
 ['supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/12_complete_private_registry_union.sqlinc','11_complete_registry_current_union.sqlinc'],
 ['supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/13_complete_final_acl_union.sqlinc','12_complete_acl_current_union.sqlinc']])
 compose(input,output,[{id:'ONLY_FIVE_REPORT_PRIVATE_HELPERS',old:prefix,new:prefix+signatures.map(sig=>"    ('"+sig+"'),\n").join('')}]);
write('PRIVATE_SIGNATURES.json',JSON.stringify({schema_version:'REPORT_AB_OWNER_ONLY_SIGNATURE_DELTA_V1',signatures,
 root_base_private:232,current_only_proposal_private:239,table_count:105,service_count:142,
 warning:'These complete current-root unions are reverse-proof inputs, not the future Q/A0/Prepared union. Root must merge literal sets with every later retained owner before installation.'},null,2)+'\n');
write('07_owner_acl.sqlinc',signatures.map(sig=>'alter function '+sig+' owner to postgres;\nrevoke all on function '+sig+' from public,anon,authenticated,service_role;\n').join('')+
 'alter function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean) owner to postgres;\nrevoke all on function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean) from public,anon,authenticated,service_role;\n');
write('FULL_REVERSE_RECEIPT.json',JSON.stringify({schema_version:'REPORT_AB_COMPLETE_SOURCE_REVERSE_V1',root_commit:pins.root_commit,
 status:'SOURCE_ONLY_NO_NATIVE_PROOF',owners:receipts},null,2)+'\n');
console.log(JSON.stringify({owners:receipts.length,full_reverse:receipts.every(r=>r.full_reverse)}));
