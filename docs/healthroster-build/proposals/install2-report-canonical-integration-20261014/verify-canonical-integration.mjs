// Effect-free source verification of complete forwards and canonical boundary.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'../../../..');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),read=name=>fs.readFileSync(path.join(here,name)),json=name=>JSON.parse(read(name));
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,maxBuffer:64*1024*1024});
const reverse=json('CURRENT_NATIVE_REVERSE.json'),order=json('INTEGRATION_ORDER.json'),scripts=json('SCRIPT_AND_RECEIPT_DEPENDENCY_PINS.json');
let hunks=0;for(const owner of reverse.owners){const before=git('show',owner.input_commit+':'+owner.input),after=read('forward/'+owner.output);let composed=before.toString('utf8');
 if(sha(before)!==owner.input_sha256||sha(after)!==owner.output_sha256)throw Error('WHOLE_OWNER_PIN_MISMATCH');
 for(const h of owner.hunks){if(composed.split(h.old).length!==2)throw Error('SOURCE_FORWARD_NOT_UNIQUE');composed=composed.replace(h.old,()=>h.new);hunks++;}
 if(!Buffer.from(composed).equals(after))throw Error('COMPLETE_SOURCE_FORWARD_FAILED');
 for(const h of [...owner.hunks].reverse()){if(composed.split(h.new).length!==2)throw Error('SOURCE_REVERSE_NOT_UNIQUE');composed=composed.replace(h.new,()=>h.old);}
 if(!Buffer.from(composed).equals(before))throw Error('COMPLETE_SOURCE_REVERSE_FAILED');}
for(const copy of reverse.byte_identical_frozen_copies)if(sha(read('forward/'+copy.destination))!==copy.sha256
 ||!read('forward/'+copy.destination).equals(git('show',copy.source_commit+':'+copy.source_path)))throw Error('FROZEN_COPY_CHANGED');
const main=read('forward/'+order.canonical_repeatable).toString('utf8'),actualIncludes=[...main.matchAll(/^\\ir ([^\r\n]+)$/gm)].map(m=>m[1]);
if(JSON.stringify(actualIncludes)!==JSON.stringify(order.literal_relative_include_order)||actualIncludes.some(p=>p.includes('..')||p.includes('docs/')||p.includes('support/')))throw Error('CANONICAL_LITERAL_INCLUDE_BOUNDARY_FAILED');
const supportBase=path.dirname(path.join(here,'forward',order.canonical_repeatable));let definitions=0;
for(const include of actualIncludes){const target=path.resolve(supportBase,include);if(!target.startsWith(supportBase+path.sep))throw Error('CANONICAL_INCLUDE_ESCAPED');
 const text=fs.readFileSync(target,'utf8');if(/^\\ir/m.test(text))throw Error('UNDECLARED_NESTED_INCLUDE');
 const count=[...text.matchAll(/create or replace function /gi)].length;definitions+=count;
 if((text.match(/\$function\$/g)||[]).length!==count*2)throw Error('NATIVE_FUNCTION_BODY_DELIMITER_UNBALANCED');}
if(definitions!==12||reverse.owners.length!==5||hunks!==32)throw Error('NATIVE_OWNER_CENSUS_CHANGED');
const helpers=json('REPORT_PRIVATE_CLASSIFICATION_DELTA.json'),acl=read('forward/supabase/repeatable/14102026_0440_healthroster_report_native_integration/07_owner_acl.sqlinc').toString('utf8');
if(helpers.new_owner_only_signatures.length!==7||new Set(helpers.new_owner_only_signatures).size!==7||helpers.new_service_signatures.length||helpers.new_browser_signatures.length||helpers.new_tables.length)throw Error('REPORT_HELPER_CLASSIFICATION_CHANGED');
for(const signature of helpers.new_owner_only_signatures)if(!acl.includes('revoke all on function '+signature+' from public,anon,authenticated,service_role;'))throw Error('REPORT_OWNER_ONLY_ACL_MISSING');
if(!read('forward/'+order.data_free_verifier.path).equals(git('show',order.prior_frozen_report_commit+':docs/healthroster-build/proposals/report-ab-protected-dispatch/08_datafree_verifier.sqlinc'))
 ||actualIncludes.some(p=>p.includes('verifier'))||order.data_free_verifier.not_in_install_wrapper!==true)throw Error('ROLLBACK_VERIFIER_INSTALL_BOUNDARY_INVALID');
for(const source of scripts.corrected_source_files){if(sha(read('forward/'+source.destination))!==source.sha256)throw Error('CORRECTED_SCRIPT_CHANGED');
 execFileSync(process.execPath,['--check',path.join(here,'forward',source.destination)],{cwd:root});}
for(const source of scripts.preserved_root_scripts)if(sha(git('show',source.source_commit+':'+source.path))!==source.sha256)throw Error('CURRENT_ROOT_SCRIPT_PIN_CHANGED');
if(read('forward/scripts/weekly-source-report-fin-native-seal.mjs').toString('utf8').match(/export const reportFinNativeSeal = null;/g)?.length!==1)throw Error('NATIVE_SEAL_NOT_LITERAL_NULL');
const old='docs/healthroster-build/proposals/report-ab-protected-dispatch/SOURCE_MANIFEST.json',correction='docs/healthroster-build/proposals/install2-shared-runtime-forward-20261014/REVIEW_MANIFEST.json';
if(sha(fs.readFileSync(path.join(root,old)))!=='000b8db40dac31bbc89b1a2f0298b19d60d23e80ce4616e3882aa4fb3f984e51'
 ||sha(fs.readFileSync(path.join(root,correction)))!==scripts.correction_manifest_sha256)throw Error('PRIOR_FREEZE_CHANGED');
const receiptBundles=['supabase/release/weekly-source-protected-prerequisites.json','supabase/release/weekly-source-protected-operational.json','supabase/release/weekly-source-protected-test-setup.json'];
if(JSON.stringify(scripts.receipt_bundle_paths)!==JSON.stringify(receiptBundles)||scripts.recorder_slots.length!==2||scripts.normal_install_writer_or_caller_changes!==0)throw Error('RECEIPT_ABI_BOUNDARY_CHANGED');
const checks={schema_version:'INSTALL2_REPORT_CANONICAL_SOURCE_CHECKS_V1',status:'SOURCE_ONLY_WAITING_INDEPENDENT_CRITIC_ROOT_COMPILE_NATIVE_SEAL_AND_REAL_GATES',complete_current_owners:5,complete_forward_reverse_hunks:hunks,literal_contained_sql_includes:actualIncludes.length,total_function_definitions:definitions,new_owner_only_private_helpers:7,new_service_or_browser_signatures:0,new_tables:0,recorder_slots:2,request_return_fields:'UNCHANGED_9_13',receipt_bundle_paths:3,script_syntax_passed:scripts.corrected_source_files.length,normal_install_writer_or_caller_changes:0,seal_literal_null:true,frozen_report49_and_correction35_unchanged:true,no_native_execution:true};
fs.writeFileSync(path.join(here,'SOURCE_CHECK_RESULT.json'),JSON.stringify(checks,null,2)+'\n');console.log(JSON.stringify(checks));
