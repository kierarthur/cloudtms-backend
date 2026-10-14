// Source-only preparation. Reads exact authorized Git objects and the two
// immutable Report freezes; writes only this separate proposal.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(here,'../../../..');
const old=path.join(root,'docs/healthroster-build/proposals/report-ab-protected-dispatch');
const correction=path.join(root,'docs/healthroster-build/proposals/install2-shared-runtime-forward-20261014');
const base='a63455e878391cb3ee9732165e58c0db517f48a7',foundation='fe7b3dfc2946cf334ba88681ae425105f864c42f',frozen='c57746f82558dd698d13fe5b2fc43dd18d72af6c';
const folder='14102026_0440_healthroster_report_native_integration',sqlRoot='supabase/repeatable/'+folder;
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,maxBuffer:64*1024*1024});
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const write=(name,bytes)=>{const target=path.join(here,name);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,bytes);};
const json=(name,value)=>write(name,JSON.stringify(value,null,2)+'\n');
const savedOld=name=>{const bytes=fs.readFileSync(path.join(old,name));if(!bytes.equals(git('show',frozen+':docs/healthroster-build/proposals/report-ab-protected-dispatch/'+name)))throw Error('FROZEN49_SAVED_CHANGED '+name);return bytes;};
const frozenManifest=JSON.parse(savedOld('SOURCE_MANIFEST.json'));
for(const row of frozenManifest.artifacts??frozenManifest.files??[]){const name=row.path??row.file; if(name&&sha(savedOld(name))!==row.sha256)throw Error('FROZEN49_HASH_CHANGED '+name);}
const correctionManifestBytes=fs.readFileSync(path.join(correction,'REVIEW_MANIFEST.json')),correctionManifest=JSON.parse(correctionManifestBytes);
for(const row of correctionManifest.artifacts){if(sha(fs.readFileSync(path.join(correction,row.path)))!==row.sha256)throw Error('CORRECTION35_HASH_CHANGED '+row.path);}
const records=[],copies=[];
function unchangedCopy(name,out){const bytes=savedOld(name);write('forward/'+sqlRoot+'/'+out,bytes);copies.push({source_commit:frozen,source_path:'docs/healthroster-build/proposals/report-ab-protected-dispatch/'+name,destination:sqlRoot+'/'+out,sha256:sha(bytes),byte_identical:true});}
function compose(inputCommit,inputPath,outputName,hunks,expectedBytes){
 const before=git('show',inputCommit+':'+inputPath);let text=before.toString('utf8');
 for(const h of hunks){if(!h.old||text.split(h.old).length!==2)throw Error('CURRENT_OWNER_HUNK_NOT_UNIQUE '+outputName+' '+h.id);text=text.replace(h.old,()=>h.new);}
 const after=Buffer.from(text);if(expectedBytes&&!after.equals(expectedBytes))throw Error('REVIEWED_COMPLETE_FORWARD_MISMATCH '+outputName);
 let reversed=text;for(const h of [...hunks].reverse()){if(reversed.split(h.new).length!==2)throw Error('CURRENT_OWNER_REVERSE_NOT_UNIQUE '+h.id);reversed=reversed.replace(h.new,()=>h.old);}
 if(!Buffer.from(reversed).equals(before))throw Error('CURRENT_OWNER_COMPLETE_REVERSE_FAILED');
 write('forward/'+sqlRoot+'/'+outputName,after);records.push({input_commit:inputCommit,input:inputPath,output:sqlRoot+'/'+outputName,input_bytes:before.length,output_bytes:after.length,input_sha256:sha(before),output_sha256:sha(after),full_reverse:true,hunks});return text;
}
const originalReverse=JSON.parse(savedOld('FULL_REVERSE_RECEIPT.json'));
const fin3Recipe=originalReverse.owners.find(r=>r.output==='04_complete_fin.sqlinc');
const fin3=compose(base,fin3Recipe.input,'04_complete_fin3_dispatch.sqlinc',fin3Recipe.hunks,savedOld('04_complete_fin.sqlinc'));
const featureRecipe=originalReverse.owners.find(r=>r.output==='05_complete_feature_setup.sqlinc');
const featurePath='supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/30_09_complete_controlled_setup_transport.sqlinc';
const feature=compose(foundation,featurePath,'05_complete_current_feature_setup.sqlinc',featureRecipe.hunks);
for(const name of ['01_certificate.sqlinc','02_release_feature_strategy.sqlinc','03_operational_dispatch.sqlinc','13_slot_validators.sqlinc','07_owner_acl.sqlinc'])unchangedCopy(name,name);
const admissionRecipe=JSON.parse(savedOld('REPORT_ADMISSION_REVERSE.json'));
compose(base,admissionRecipe.input,'15_complete_native_report_admission.sqlinc',admissionRecipe.hunks,savedOld('15_complete_native_report_admission.sqlinc'));
const orderRecipe=JSON.parse(savedOld('REPORT_ORDER_REVERSE.json'));
compose(base,orderRecipe.input,'14_complete_native_snapshot_assembler.sqlinc',orderRecipe.hunks,savedOld('14_complete_native_snapshot_assembler.sqlinc'));
const fin4Recipe=JSON.parse(fs.readFileSync(path.join(correction,'FIN4_NATIVE_LOCK_OBSERVATION_REVERSE.json')));
const fin4Path='supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker/04_complete_fin_overload.sqlinc';
const fin4Bytes=fs.readFileSync(path.join(correction,'forward',fin4Path));
compose(base,fin4Path,'06_complete_fin4_lock_observation.sqlinc',fin4Recipe.hunks.map((h,i)=>({id:'FIN4_OBSERVATION_'+(i+1),old:h.before,new:h.after})),fin4Bytes);
if(!fin3.includes('return private.weekly_source_report_fin_dispatch_v1(p_request);')
 ||fin4Bytes.includes(Buffer.from('return private.weekly_source_report_fin_dispatch_v1(p_request);'))
 ||!savedOld('03_operational_dispatch.sqlinc').includes(Buffer.from('private.weekly_source_self_bill_report_finalise_v1(p_request,after_value->>')))throw Error('FIN3_FIN4_NATIVE_LEAF_CHAIN_INVALID');
const requiredFeature=['OFF_RULE_OPEN','OFF_RULE_CLOSE','OFF_RULE_REVOKE','OFF_RULE_COMMAND','OFF_RULE_TRANSPORT_STATUS','OFF_RULE_STATUS','activation_probe_scope_id','weekly_source_report_fin_feature_binding_v1'];
if(requiredFeature.some(s=>!feature.includes(s)))throw Error('CURRENT_MONEY_FEATURE_ARM_LOST');
const includeNames=['13_slot_validators.sqlinc','01_certificate.sqlinc','02_release_feature_strategy.sqlinc','03_operational_dispatch.sqlinc','04_complete_fin3_dispatch.sqlinc','05_complete_current_feature_setup.sqlinc','06_complete_fin4_lock_observation.sqlinc','15_complete_native_report_admission.sqlinc','14_complete_native_snapshot_assembler.sqlinc','07_owner_acl.sqlinc'];
write('forward/supabase/repeatable/'+folder+'_v1.sql',"-- Final Report native successors after canonical Money0430.\n-- Source presence supplies no native qualification or activation.\n\\set ON_ERROR_STOP on\nbegin;\n"+includeNames.map(n=>'\\ir '+folder+'/'+n+'\n').join('')+"notify pgrst, 'reload schema';\ncommit;\n");
write('forward/supabase/verification/'+folder+'_declarations.sql',savedOld('08_datafree_verifier.sqlinc'));
const helpers=JSON.parse(savedOld('PRIVATE_SIGNATURES.json')).signatures;
json('REPORT_PRIVATE_CLASSIFICATION_DELTA.json',{schema_version:'INSTALL2_REPORT_PRIVATE_CLASSIFICATION_DELTA_V1',new_owner_only_signatures:helpers,new_service_signatures:[],new_browser_signatures:[],new_tables:[],existing_fin3_acl:'Frozen07 retains its explicit owner-only reassertion; root must retain every other actual current signature classification in final literal union.',final_registry_gate:'Root unions these seven literal signatures into its exact final current Money/Bank/PP private registry and ACL after all later definitions. Frozen current-only239 registry/ACL are excluded.'});
const scriptFiles=['weekly-source-report-fin-ab-evidence.mjs','weekly-source-report-fin-native-collector.mjs','weekly-source-report-fin-native-lock-observation.mjs','weekly-source-report-fin-native-mapper.mjs','weekly-source-report-fin-native-seal.mjs'];
const scriptPins=scriptFiles.map(name=>{const relative='forward/scripts/'+name,bytes=fs.readFileSync(path.join(correction,relative));write(relative,bytes);return {destination:'scripts/'+name,origin:'CORRECTION35',origin_path:relative,sha256:sha(bytes)};});
const retainedScripts=['weekly-source-protected-execution-tree.mjs','weekly-source-protected-release-qualification.mjs','weekly-source-protected-operational-evidence.mjs','weekly-source-post-verified-evidence-controller.mjs'];
const retained=retainedScripts.map(name=>{const file='scripts/'+name,bytes=git('show',foundation+':'+file);return {source_commit:foundation,path:file,sha256:sha(bytes),action:'PRESERVE_CURRENT_ROOT_BYTES'};});
const codec=fs.readFileSync(path.join(correction,'forward/scripts/weekly-source-protected-release-qualification.mjs'));
if(sha(codec)!==retained.find(r=>r.path.endsWith('/weekly-source-protected-release-qualification.mjs')).sha256)throw Error('CURRENT_ROOT_CODEC_ABI_MISMATCH');
const rendererPath='broker/src/invoice-presentation-contract.js',renderer=git('show',foundation+':'+rendererPath);
const immutablePaths=['supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/06_complete_allocation_issue.sqlinc','supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/07_complete_header_move.sqlinc','supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/12_retained_final_issue_transport.sqlinc','supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/14_complete_legacy_render_manifest.sqlinc'];
json('CURRENT_NATIVE_REVERSE.json',{schema_version:'INSTALL2_REPORT_CURRENT_COMPLETE_NATIVE_REVERSE_V1',base_current_commit:base,current_foundation_commit:foundation,owners:records,byte_identical_frozen_copies:copies});
json('SCRIPT_AND_RECEIPT_DEPENDENCY_PINS.json',{schema_version:'INSTALL2_REPORT_SCRIPT_RECEIPT_DEPENDENCY_PINS_V1',correction_manifest_sha256:sha(correctionManifestBytes),correction_source_checks_sha256:correctionManifest.source_checks_sha256,corrected_source_files:scriptPins,preserved_root_scripts:retained,preserved_renderer:{source_commit:foundation,path:rendererPath,sha256:sha(renderer)},seal_literal_null:fs.readFileSync(path.join(here,'forward/scripts/weekly-source-report-fin-native-seal.mjs'),'utf8').includes('export const reportFinNativeSeal = null;'),receipt_bundle_paths:['supabase/release/weekly-source-protected-prerequisites.json','supabase/release/weekly-source-protected-operational.json','supabase/release/weekly-source-protected-test-setup.json'],protected_operational_required_qualifications:[],report_optional_member:'report_fin_native_artifacts',recorder_slots:['weekly_source_report_fin_native_comparisons','weekly_source_report_fin_qualifications'],normal_install_writer_or_caller_changes:0});
json('CURRENT_NATIVE_RETAINED_PINS.json',{schema_version:'INSTALL2_REPORT_CURRENT_NATIVE_RETAINED_PINS_V1',source_commit:foundation,unchanged_native_document_owners:immutablePaths.map(file=>({path:file,sha256:sha(git('show',foundation+':'+file))})),bank_pp_runtime_action:'Four reviewed ae499 forwards already mounted in a634/fe7. Preserve those entire current files; this proposal contains no controller/index forward.',money_nullable_clone:{source_commit:foundation,path:'supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/28_12_complete_report_nullable_rounding_owner.sqlinc',sha256:sha(git('show',foundation+':supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/28_12_complete_report_nullable_rounding_owner.sqlinc'))}});
json('INTEGRATION_ORDER.json',{schema_version:'INSTALL2_REPORT_CANONICAL_INTEGRATION_ORDER_V1',status:'SOURCE_ONLY_AWAITING_CORRECTION35_CRITIC_AND_ROOT_FINAL_COMPILE',base_current_commit:base,current_foundation_commit:foundation,prior_frozen_report_commit:frozen,correction_manifest_sha256:sha(correctionManifestBytes),canonical_repeatable:'supabase/repeatable/'+folder+'_v1.sql',literal_relative_include_order:includeNames.map(n=>folder+'/'+n),prior_root_stages:['14102026_0400_healthroster_original_prerequisites_v1.sql','14102026_0410_healthroster_protected_source_prerequisites_v1.sql','14102026_0415_healthroster_a0_source_context_v1.sql (root-owned separate A0 if present)','14102026_0420_healthroster_current_native_composition_v1.sql','14102026_0430_healthroster_money_native_interfaces_v1.sql'],after_stage:'Root actual final literal private/service/browser/table union and verifier classification; never old239 registry/ACL.',data_free_verifier:{path:'supabase/verification/'+folder+'_declarations.sql',standalone:true,uses_own_rollback_transaction:true,run_after_actual_final_acl:true,not_in_install_wrapper:true},native_seal:null,no_native_execution:true});
console.log(JSON.stringify({changed_current_owner_count:records.length,complete_reverse_hunks:records.reduce((n,r)=>n+r.hunks.length,0),canonical_include_count:includeNames.length,report_helper_count:helpers.length,script_files:scriptPins.length,normal_install_writer_or_caller_changes:0,old_freezes_preserved:true,no_native_execution:true}));
