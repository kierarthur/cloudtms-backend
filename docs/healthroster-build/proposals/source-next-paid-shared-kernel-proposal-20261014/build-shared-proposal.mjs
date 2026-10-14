import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),current='5db7abee7925fffbeaf7c4e885cce809980fa079';
const sha=b=>createHash('sha256').update(b).digest('hex');fs.mkdirSync(path.join(dir,'before'),{recursive:true});fs.mkdirSync(path.join(dir,'forward'),{recursive:true});
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')),'Frozen proposal is immutable');
const file='supabase/repeatable/05102026_1112_bpay_next_source_paid_evidence_page_v1.sql';
const bytes=execFileSync('git',['--no-replace-objects','show',current+':'+file],{cwd:root,maxBuffer:32*1024*1024});
const original=functionEvents(bytes.toString('utf8')).events.find(x=>x.kind==='CREATE'&&x.id==='public.bpay_next_source_paid_evidence_page_v1(jsonb)');assert.ok(original);
const prior=JSON.parse(fs.readFileSync(path.join(dir,'../kept-pay-identical-rekey-source-reconciliation-20261014/SOURCE_INPUTS_AND_OWNER_PINS.json'),'utf8')).complete_existing_owners.find(x=>x.id===original.id);assert.equal(sha(original.definition),prior.definition_sha256);
const before=original.definition;const first="  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS',null,null,current_date);";
const second="  perform private.weekly_source_office_authority_v1(v_actor,'VIEW_SOURCE_PROGRESS',\n    v_scope.group_id,v_scope.client_id,v_scope.week_ending_date);";
for(const anchor of [first,second])assert.equal(before.split(anchor).length,2,'Exact preserved current public admission');
const suffixAt=before.indexOf("  v_origin:=private.bpay_next_source_origin_state_v1(v_work,v_revision);");assert.ok(suffixAt>before.indexOf(second));
const suffix=before.slice(suffixAt),wrapperSuffix="  -- Proposed extraction: both actual original admin checks and native scope\n  -- validation above retain their exact order; all financial branches belong\n  -- to one STABLE owner-private kernel on this same command snapshot.\n  return private.bpay_next_source_paid_evidence_kernel_v1(p_request);\nend\n$function$;";
const wrapper=before.slice(0,suffixAt)+wrapperSuffix;assert.equal(wrapper.replace(wrapperSuffix,suffix),before);
const rename=['public.bpay_next_source_paid_evidence_page_v1(p_request jsonb)','private.bpay_next_source_paid_evidence_kernel_v1(p_request jsonb)'];assert.equal(before.split(rename[0]).length,2);
const kernel=before.replace(...rename).replace(first,'').replace(second,'');assert.ok(!kernel.includes('weekly_source_office_authority_v1('));
let reverse=kernel.replace(rename[1],rename[0]);const firstAt=before.indexOf(first),secondAt=before.indexOf(second);
// Complete reverse uses unique adjacent current source anchors, not offsets.
const firstAnchor="  -- Current physical permission is independent of historical captured money.";
const secondAnchor="  v_origin:=private.bpay_next_source_origin_state_v1(v_work,v_revision);";
assert.equal(reverse.split(firstAnchor).length,2);assert.equal(reverse.split(secondAnchor).length,2);
reverse=reverse.replace('\n'+firstAnchor,first+'\n'+firstAnchor).replace('\n'+secondAnchor,second+'\n'+secondAnchor);assert.equal(reverse,before);
const kernelTail="\nalter function private.bpay_next_source_paid_evidence_kernel_v1(jsonb) owner to postgres;\nrevoke all on function private.bpay_next_source_paid_evidence_kernel_v1(jsonb) from public,anon,authenticated,service_role;\n";
const wrapperTail="\nalter function public.bpay_next_source_paid_evidence_page_v1(jsonb) owner to postgres;\nrevoke all on function public.bpay_next_source_paid_evidence_page_v1(jsonb) from public,anon,authenticated;\ngrant execute on function public.bpay_next_source_paid_evidence_page_v1(jsonb) to service_role;\n";
fs.writeFileSync(path.join(dir,'before/01_complete_public_paid_page.sqlinc'),before+'\n');
fs.writeFileSync(path.join(dir,'forward/01_complete_native_paid_kernel.sqlinc'),kernel+kernelTail);
fs.writeFileSync(path.join(dir,'forward/02_complete_public_admin_wrapper.sqlinc'),wrapper+wrapperTail);
const keepDir=path.join(dir,'../kept-pay-identical-rekey-current-root-forward-20261014'),keepManifestBytes=fs.readFileSync(path.join(keepDir,'FROZEN_ARTIFACT_MANIFEST.json')),keepManifest=JSON.parse(keepManifestBytes);
assert.equal(sha(keepManifestBytes),'3e878999a0b04b24ca0bbd0776bb1f042bc805df142904f28b26c3403dc0aca8');
const contextFile='forward/06_kept_as_paid_context.sqlinc',contextBytes=fs.readFileSync(path.join(keepDir,contextFile));assert.equal(sha(contextBytes),keepManifest.artifacts.find(x=>x.file===contextFile).sha256);
const context=contextBytes.toString('utf8'),start=context.indexOf('      select count(*) into v_component_count'),end=context.indexOf("        v_paid:=null; v_paid_state:='UNKNOWN'; v_unknown:=true;",start);assert.ok(start>0&&end>start);
const guardBefore=context.slice(start,end),guardAfter="      if p_actor_user_id is null then\n        -- No actor substitution; the separate Source purpose owner requires\n        -- genuine VIEW_PAY_WORK and native root/work/component/revision.\n";
const callBefore="v_page:=private.weekly_source_office_next_paid_evidence_page_v1(p_actor_user_id,v_root,'COMPONENTS',null,v_after,100);";
const callAfter='v_page:=private.weekly_source_kept_paid_components_page_v1(p_actor_user_id,v_event.id,v_root,v_after,100);';
assert.equal(context.split(guardBefore).length,2);assert.equal(context.split(callBefore).length,2);const contextAfter=context.replace(guardBefore,guardAfter).replace(callBefore,callAfter);
assert.equal(contextAfter.replace(callAfter,callBefore).replace(guardAfter,guardBefore),context);
fs.writeFileSync(path.join(dir,'before/04_complete_frozen_keep_context.sqlinc'),contextBytes);fs.writeFileSync(path.join(dir,'forward/04_complete_source_keep_context.sqlinc'),contextAfter);
const nativeSuffix=before.slice(suffixAt);assert.equal(kernel.slice(kernel.indexOf('  v_origin:=private.bpay_next_source_origin_state_v1')),nativeSuffix);
const result={schema_version:'SHARED_NATIVE_PAID_KERNEL_REVIEW_PROPOSAL_V1',proposal_only:true,source_only:true,shared_banking_owner_change_proposed:true,shared_banking_owner_change_approved:false,canonical_sources_modified:false,mounted:false,native_proof:false,
 input:{commit:current,file,bytes:bytes.length,sha256:sha(bytes),signature:original.id,definition_sha256:sha(before),body_sha256:sha(original.body)},
 preserved_public_admissions:[{source: first,offset:firstAt},{source:second,offset:secondAt}],public_prefix_bytes_and_error_order_unchanged:true,
 wrapper:{before_file:'before/01_complete_public_paid_page.sqlinc',forward_file:'forward/02_complete_public_admin_wrapper.sqlinc',before_definition_sha256:sha(before),after_definition_sha256:sha(wrapper),hunks:[{before:suffix,after:wrapperSuffix}],full_reverse:true,service_acl_unchanged:true},
 kernel:{signature:'private.bpay_next_source_paid_evidence_kernel_v1(jsonb)',file:'forward/01_complete_native_paid_kernel.sqlinc',definition_sha256:sha(kernel),extraction_hunks:[{before:rename[0],after:rename[1]},{before:first,after:''},{before:second,after:''}],whole_original_reverse:true,financial_origin_header_three_queries_assembly_suffix_sha256:sha(nativeSuffix),all_three_financial_branches_single_copy:true,pure_native_read_kernel_no_office_permission_or_caller_flag:true,private_execute_all_revoked:true},
 source_purpose:{signature:'private.weekly_source_kept_paid_components_page_v1(uuid,uuid,uuid,jsonb,integer)',file:'forward/03_source_purpose_components_page.sqlinc',native_scope:'actual Source event/lineage/rotation root/Contract/Candidate/Client/date/VIEW_PAY_WORK/current work/revision/approved component',kind:'COMPONENTS_ONLY',private_execute_all_revoked:true},
 source_context:{input_packet_manifest_sha256:sha(keepManifestBytes),input_file:contextFile,input_bytes:contextBytes.length,input_sha256:sha(contextBytes),before_file:'before/04_complete_frozen_keep_context.sqlinc',forward_file:'forward/04_complete_source_keep_context.sqlinc',after_sha256:sha(contextAfter),hunks:[{before:guardBefore,after:guardAfter},{before:callBefore,after:callAfter}],whole_reverse:true},
 snapshot:'Every wrapper/kernel/purpose/context is STABLE; same command snapshot is a SOURCE premise requiring actual native proof',cash_amendment_or_settlement_authority:'NOT_GRANTED',remaining_missing_default_overlap_prepared_consumer_gaps:'UNCHANGED_SOURCE_ENGINEERING'};
fs.writeFileSync(path.join(dir,'COMPLETE_SHARED_EXTRACTION_AND_REVERSES.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({proposal_only:true,shared_bank_change_proposed_not_approved:true,canonical_modified:false,current,input:sha(before),kernel:sha(kernel),public_wrapper:sha(wrapper),full_reversals:3,all_three_financial_branches_identical:true,native_proof:false}));
