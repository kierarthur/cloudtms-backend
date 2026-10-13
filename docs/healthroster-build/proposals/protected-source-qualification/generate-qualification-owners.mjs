import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(process.argv[2]??'../healthroster-v2-backend-13102026');
const cap=path.resolve(out,'../protected-source-capability');
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const inputs=[],proofs=[];
const read=file=>{const bytes=fs.readFileSync(file);inputs.push({file:path.relative(process.cwd(),file).replaceAll('\\','/'),bytes:bytes.length,sha256:sha(bytes)});return bytes.toString('utf8').replaceAll('\r\n','\n');};
const get=(body,name)=>{const matches=[...body.matchAll(/create or replace function\s+([\w.]+)\s*\([\s\S]*?\bas\s+(\$[\w]*\$)[\s\S]*?\2\s*;/gi)].filter(x=>x[1]===name);if(matches.length!==1)throw Error('QUAL_OWNER_COUNT '+name);return matches[0][0];};
const replace=(body,anchor,value)=>{if(body.split(anchor).length!==2)throw Error('QUAL_ANCHOR_CHANGED');return body.replaceAll(anchor,()=>value);};
const emit=(file,body)=>fs.writeFileSync(path.join(out,file),body.trimEnd()+'\n');
const exportFile=read(path.join(root,'supabase/release/export_contract.sql'));
const nativeQuery=exportFile.slice(exportFile.indexOf('\nwith\napp_namespaces')+1);
if(!nativeQuery.startsWith('with\napp_namespaces')||!nativeQuery.trimEnd().endsWith(')::text;'))throw Error('QUAL_NATIVE_EXPORT_QUERY_CHANGED');
const nativeJsonbQuery=nativeQuery.replace(/\)::text;\s*$/,()=>');');
emit('00_complete_current_catalogue.sqlinc',`-- Complete native canonical catalogue exporter, no table-row reads.
create or replace function private.weekly_source_protected_qualification_current_catalogue_v1()
returns jsonb language sql stable security definer set search_path to 'pg_catalog','public'
as $function$
${nativeJsonbQuery}
$function$;`);
proofs.push({name:'private.weekly_source_protected_qualification_current_catalogue_v1',file:'00_complete_current_catalogue.sqlinc',
 complete_native_export_query_sha256:sha(nativeQuery),only_delta:'Native final ::text result is returned as jsonb inside an owner-only SQL function.',
 reverse_hunk_matches_entire_input_definition:true});
const replaceOwner=(file,name,anchor,delta,target)=>{
 const original=get(read(file),name),body=replace(original,anchor,delta);
 const reverse=replace(body,delta,anchor);if(reverse!==original)throw Error('QUAL_REVERSE_HUNK_FAILED');
 if((body.match(/\$function\$/g)||[]).length!==2||/\\(?:if|endif)\b/.test(body))throw Error('QUAL_BODY_CORRUPT');
 emit(target,body);proofs.push({name,file:target,input_definition_sha256:sha(original),output_definition_sha256:sha(body),reverse_hunk_matches_entire_input_definition:true,delta:delta});
};
replaceOwner(path.join(root,'supabase/repeatable/13102026_1438_weekly_source_protection_queries/25_bulk_stage_proof.sqlinc'),
 'private.weekly_source_bulk_source_cas_assert_v1',
 "  if v_ready is distinct from true then raise exception 'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE' using errcode='55000'; end if;",
 `  if v_ready is distinct from true then
    begin
      perform private.weekly_source_protected_qualification_assert_v1(null,p_cycle_id,v_clients,'BULK_SIGNED');
    exception when sqlstate '42501' or sqlstate '55000' then
      raise exception 'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE' using errcode='55000';
    end;
  end if;`, '03_complete_cas_admission.sqlinc');
replaceOwner(path.join(cap,'NONSELFBILL_ADMISSION.sqlinc'),
 'private.weekly_source_protected_non_self_bill_mode_v1',
 '  qualification:=private.weekly_source_protected_capability_receipt_v1(c.id);',
 `  begin
    qualification:=private.weekly_source_protected_capability_receipt_v1(c.id);
  exception when sqlstate '55000' or no_data_found or too_many_rows then
    begin
      qualification:=private.weekly_source_protected_qualification_assert_v1(null,c.id,null,'BULK_SIGNED');
    exception when sqlstate '42501' or sqlstate '55000' then return null;
    end;
    if not(qualification#>'{original_census,original_client_ids}' @> jsonb_build_array(p_client_id)) then return null; end if;
  end;`, '04_complete_non_self_bill_admission.sqlinc');
replaceOwner(path.join(root,'supabase/repeatable/13102026_1438_weekly_source_protection_queries/27_automatic_reconcile.sqlinc'),
 'private.weekly_source_automatic_reconcile_capability_assert_v1',
 `  if v_ready is distinct from true then
    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' using errcode='55000'; end if;`,
 `  if v_ready is distinct from true then
    begin
      perform private.weekly_source_protected_qualification_assert_v1(null,p_cycle_id,null,'AUTOMATIC_RECONCILE');
    exception when sqlstate '42501' or sqlstate '55000' then
      raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' using errcode='55000';
    end;
  end if;`, '05_complete_automatic_admission.sqlinc');
replaceOwner(path.join(cap,'CAPABILITY_OWNERS.sqlinc'),
 'private.weekly_source_protected_capability_release_evidence_v1',
 "  if count_rows<>1 or not private.weekly_exceptional_json_keys_exact_v1(q,array[",
 `  if count_rows=1 and q->>'schema_version'='WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2' then
    return private.weekly_source_protected_prerequisite_release_assert_v2(q,p_group_id,p_from,p_to,p_expected_sha256);
  end if;
  if count_rows<>1 or not private.weekly_exceptional_json_keys_exact_v1(q,array[`, '08_complete_capability_release_reader.sqlinc');
const signatures=[
 'private.weekly_source_protected_qualification_current_catalogue_v1()',
 'private.weekly_source_protected_qualification_owner_v1()',
 'private.weekly_source_protected_qualification_release_v1()',
 'private.weekly_source_protected_qualification_census_v1(uuid)',
 'private.weekly_source_protected_qualification_begin_v1(jsonb)',
 'private.weekly_source_protected_qualification_assert_v1(uuid,uuid,uuid[],text)',
 'private.weekly_source_protected_qualification_step_v1(uuid,text,jsonb)',
 'private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[])',
 'private.weekly_source_protected_non_self_bill_mode_v1(uuid,uuid,date,uuid)',
 'private.weekly_source_automatic_reconcile_capability_assert_v1(uuid)',
 'private.weekly_source_protected_prerequisite_release_assert_v2(jsonb,uuid,date,date,text)',
 'private.weekly_source_protected_capability_release_evidence_v1(uuid,date,date,text)'];
emit('06_owner_only_acl.sqlinc',signatures.map(s=>`alter function ${s} owner to postgres;\nrevoke all on function ${s} from public,anon,authenticated,service_role;`).join('\n'));
for(const name of ['QUALIFICATION_SCOPE.sqlinc','QUALIFICATION_STEP.sqlinc','QUALIFICATION_R2_READER.sqlinc'])read(path.join(out,name));
let managed=read(path.join(cap,'generated/10_complete_managed_release.mjs'));
const beforeManaged=managed;
managed=replace(managed,"import { buildQualification, assertManagedQualificationEvidence } from './weekly-source-protected-release-qualification.mjs';",
 "import { buildQualification, domainHash } from './weekly-source-protected-release-qualification.mjs';\nimport { buildPrerequisiteQualification, assertManagedPrerequisiteEvidence, proveCommittedSources, installedQualificationCatalogueEvidence } from './weekly-source-protected-prerequisite-evidence.mjs';");
managed=replace(managed,"    if (verified.sha256 !== context.expectedHash) throw Error('MANAGED_VERIFIED_CONTRACT_MISMATCH');",
 `    if (verified.sha256 !== context.expectedHash) throw Error('MANAGED_VERIFIED_CONTRACT_MISMATCH');
    if (context.environment==='TEST') verified.evidence={...(verified.evidence??{}),
      weekly_source_protected_qualification_catalogue:installedQualificationCatalogueEvidence(verified.actual,context.expectedHash)};`);
managed=replace(managed,'        return buildQualification(journey.context,journey.receipts);',
 `        if (journey.phase==='ROLLBACK_PREREQUISITE') {
          if (!journey.receipts.length) throw Error('PREREQUISITE_GATE_CENSUS_INVALID');
          const first=JSON.parse(journey.receipts[0].receipt_bytes.toString('utf8'));
          const sourceProof=proveCommittedSources(root,first.execution.source_commit,context.gitCommit);
          return buildPrerequisiteQualification(journey.context,journey.receipts,sourceProof);
        }
        if (journey.phase!==undefined) throw Error('QUALIFICATION_INPUT_INVALID');
        return buildQualification(journey.context,journey.receipts);`);
managed=replace(managed,'    assertManagedQualificationEvidence(context,verified.evidence??{});',
 `    assertManagedPrerequisiteEvidence(context,verified.evidence??{},root);
    if (protectedCapabilityJourneys.length) {
      const priorRows=JSON.parse(await execute('select coalesce(evidence_json->' + literal('weekly_source_protected_capability_qualifications')
        + ',' + literal('null') + '::jsonb)::text from private.cloudtms_database_releases where release_id=' + literal(context.releaseId)));
      if (priorRows!==null && domainHash('WEEKLY_SOURCE_REPLAY_SAFE_PREREQUISITE_EVIDENCE_V1',priorRows)!==
        domainHash('WEEKLY_SOURCE_REPLAY_SAFE_PREREQUISITE_EVIDENCE_V1',verified.evidence.weekly_source_protected_capability_qualifications))
        throw Error('QUALIFICATION_EVIDENCE_REPLACEMENT_REFUSED');
      for (const q of verified.evidence.weekly_source_protected_capability_qualifications) {
        if (q.schema_version!=='WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2') continue;
        const recorded=JSON.parse(await execute('select jsonb_build_object('+
          literal('release_id')+',r.release_id,'+literal('source_commit')+',r.git_commit,'+
          literal('contract_sha256')+',r.installed_contract_sha256,'+literal('installation_id')+',i.installation_id,'+
          literal('agency_id')+',g.agency_id,'+literal('environment')+',i.environment,'+literal('database')+',current_database(),'+
          literal('managed_manifest_sha256')+',r.evidence_json#>>'+literal('{managed_release,manifest_sha256}')+','+
          literal('installed_catalogue_jsonb_sha256')+',r.evidence_json#>>'+literal('{weekly_source_protected_qualification_catalogue,jsonb_sha256}')+
          ')::text from private.cloudtms_database_releases r cross join private.cloudtms_database_identity i '+
          'join public.weekly_source_groups g on g.id='+literal(q.source_group_id)+'::uuid and g.active and g.environment='+literal('TEST')+
          ' and g.source_family='+literal('ROSTER')+' where i.singleton and i.environment='+literal('TEST')+
          ' and r.release_id='+literal(q.execution_identity.release_id)+' and r.status='+literal('VERIFIED')+
          ' and r.completed_at_utc is not null and r.installed_contract_sha256=r.repository_contract_sha256'));
        if (domainHash('EXECUTION_IDENTITY',recorded)!==domainHash('EXECUTION_IDENTITY',q.execution_identity))
          throw Error('PREREQUISITE_RECORDED_EXECUTION_CHANGED');
      }
    }`);
emit('09_complete_managed_release.mjs',managed);
let cli=read(path.join(root,'scripts/cloudtms-db-release.mjs'));
const cliBefore=cli;
cli=replace(cli,'#!/usr/bin/env node\n',
 "#!/usr/bin/env node\nimport { loadCommittedPrerequisiteJourneys } from './weekly-source-protected-prerequisite-evidence.mjs';\n");
cli=replace(cli,'      preapply: runBankingPayCatalogPreapply,',
 `      preapply: runBankingPayCatalogPreapply,
      protectedCapabilityJourneys: loadCommittedPrerequisiteJourneys(repoRoot,release,{...releaseVerifierContext,mode,expectedHash}),`);
emit('10_complete_release_caller.mjs',cli);
const workflow=read(path.join(root,'.github/workflows/database-release.yml'));
const workflowAnchor='          ref: ${{ needs.source-gate.outputs.commit }}';
const workflowDelta=workflowAnchor+'\n          # Source-equivalence evidence reads actual historical R1 Git objects.\n          fetch-depth: 0';
const workflowAfter=replace(workflow,workflowAnchor,workflowDelta);
emit('11_complete_protected_release_workflow.yml',workflowAfter);
proofs.push({name:'Database Release workflow',file:'11_complete_protected_release_workflow.yml',
 input_definition_sha256:sha(workflow),output_definition_sha256:sha(workflowAfter),
 reverse_hunk_matches_entire_input_definition:replace(workflowAfter,workflowDelta,workflowAnchor)===workflow,
 delta:'Complete historical Git objects only; existing protected target/source approval/standing TEST authority unchanged.'});
emit('weekly-source-protected-prerequisite-evidence.mjs',replace(read(path.join(out,'prerequisite-evidence.mjs')),
 "from '../protected-source-capability/release-qualification.mjs'","from './weekly-source-protected-release-qualification.mjs'"));
emit('weekly-source-protected-release-qualification.mjs',read(path.join(cap,'release-qualification.mjs')));
proofs.push({name:'executeManagedRelease',file:'09_complete_managed_release.mjs',input_definition_sha256:sha(beforeManaged),
 output_definition_sha256:sha(managed),native_checkpoint_writer_unchanged:true,
 delta:'Closed additional prerequisite evidence schema; actual committed native file equality; preserved R1 execution identity.'});
proofs.push({name:'applyRelease',file:'10_complete_release_caller.mjs',input_definition_sha256:sha(cliBefore),output_definition_sha256:sha(cli),
 native_approval_target_verifiers_unchanged:true,delta:'One fixed-path commit/hash-bound root evidence input; no CLI/env/browser input.'});
emit('QUALIFICATION_NATIVE_OWNER_DELTA.json',JSON.stringify({schema_version:'PROTECTED_SOURCE_ROLLBACK_QUALIFICATION_OWNER_DELTA_V1',
 inputs,proofs,signatures,new_tables:0,new_columns:0,new_public_rpcs:0,new_service_grants:0,
 native_business_acceptance:false,operational_ready_owners_unchanged:true,
 install_order:['00_complete_current_catalogue.sqlinc','QUALIFICATION_SCOPE.sqlinc','QUALIFICATION_STEP.sqlinc','03_complete_cas_admission.sqlinc',
 '04_complete_non_self_bill_admission.sqlinc','05_complete_automatic_admission.sqlinc','QUALIFICATION_R2_READER.sqlinc',
 '08_complete_capability_release_reader.sqlinc','06_owner_only_acl.sqlinc'],
 unchanged_native_owners:['public.weekly_source_protect_all_signed_atomic_v1(jsonb)',
 'private.weekly_source_protect_all_signed_selection_v1(uuid,uuid,jsonb,uuid[])',
 'public.weekly_exceptional_pay_prepare_family_v1(jsonb)','public.weekly_exceptional_pay_prepare_action_v1(jsonb)',
 'public.weekly_exceptional_pay_action_context_v1(jsonb)','public.weekly_exceptional_pay_stage_c1_request_v1(jsonb)',
 'public.weekly_exceptional_pay_complete_local_v1(jsonb)','private.weekly_source_bulk_own_stage_v1(uuid,uuid,uuid,text,text,text)',
 'private.weekly_source_bulk_own_stage_context_v1(uuid,uuid,text,text,text)',
 'private.weekly_source_shift_on_self_bill_context_v1(uuid,uuid,jsonb)'],
 root_integration:'Union exact private ACL registry; preserve existing service inventory and frozen Protection1745/automatic-forward owners.'},null,2));
console.log(JSON.stringify({definitions:proofs.length,new_private_signatures:8,reverse_proofs:proofs.filter(p=>p.name.startsWith('private.')).every(p=>p.reverse_hunk_matches_entire_input_definition),runtime_acceptance:false}));
