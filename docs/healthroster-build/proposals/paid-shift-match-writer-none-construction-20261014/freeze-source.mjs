// Creator-only source freeze. No SQL, native transport, fixture or cleanup.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync,spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
import {loadProjectionOwner} from './source-runtime-vm.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const relative=path.relative(root,dir).replaceAll('\\','/');
const commit='fa66183f01913e86651e82816f5e88f97e07123c';
const ownCommit='63b94142c3078ea39917bb680a6bc582ab05f1ec';
const sha=b=>createHash('sha256').update(b).digest('hex');
const read=f=>fs.readFileSync(path.join(dir,f));
const json=f=>JSON.parse(read(f));
const write=(f,o)=>fs.writeFileSync(path.join(dir,f),JSON.stringify(o,null,2)+'\n');
const git=(revision,file)=>execFileSync('git',['show',revision+':'+file],{cwd:root,maxBuffer:16e6});
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')),'immutable freeze already exists');
assert.equal(execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim(),ownCommit);
assert.equal(execFileSync('git',['diff','--name-only','HEAD'],{cwd:root,encoding:'utf8'}).trim(),'');

const reverseFiles=['CURRENT_WRITER_REVERSE.json','PROJECTION_REVERSES.json','FIN4_REVERSE.json','INVOICE_CONSUMER_REVERSES.json','PAY_CONSUMER_REVERSES.json','RUNTIME_REVERSE.json'];
const singletonOutputs={'CURRENT_WRITER_REVERSE.json':'02_complete_office_recheck.sqlinc','FIN4_REVERSE.json':'06_complete_fin4.sqlinc','RUNTIME_REVERSE.json':'broker/src/weekly-source/upload-publication-owner.mjs'};
const inputs=new Map(),reverses=[];
function pin(revision,file,expected){
 const key=revision+':'+file;if(inputs.has(key)){if(expected)assert.equal(inputs.get(key).sha256,expected);return inputs.get(key);}
 const raw=git(revision,file),record={commit:revision,file,bytes:raw.length,sha256:sha(raw)};
 if(expected)assert.equal(record.sha256,expected,file);inputs.set(key,record);return record;
}
for(const reverseFile of reverseFiles){
 const doc=json(reverseFile);
 for(const record of doc.records??[doc]){
  const output=record.output??singletonOutputs[reverseFile];assert.ok(output);
  const beforeFile='before/'+output.replace(/^one-time\//,'');
  const afterFile=(output.startsWith('one-time/')?'':'forward/')+output;
  const before=read(beforeFile);let after=read(afterFile).toString();
  assert.equal(sha(before),record.before_sha256??record.input_sha256,beforeFile);
  if(record.after_leaf_sha256)assert.equal(sha(after),record.after_leaf_sha256,afterFile);
  else if(record.after_sha256)assert.equal(sha(after),record.after_sha256,afterFile);
  if(record.after_definition_sha256)after=after.slice(0,after.indexOf('\nALTER FUNCTION'));
  if(output.startsWith('one-time/')){
   const marker='alter table public.weekly_source_billing_movements\n  add constraint';
   after=after.slice(after.indexOf(marker),after.indexOf('\ncreate unique index'));
  }
  if(record.after_definition_sha256)assert.equal(sha(after),record.after_definition_sha256);
  const forward=after;
  for(const h of [...record.hunks].reverse()){
   assert.equal(after.split(h.after).length-1,h.occurrences??1,h.name);
   after=after.replaceAll(h.after,h.before);
  }
  assert.deepEqual(Buffer.from(after),before,output);
  reverses.push({reverse_file:reverseFile,before_file:beforeFile,after_file:afterFile,
    before_bytes:before.length,before_sha256:sha(before),after_leaf_bytes:read(afterFile).length,
    after_leaf_sha256:sha(read(afterFile)),reversed_region_bytes:Buffer.byteLength(forward),
    reversed_region_sha256:sha(forward),hunks:record.hunks.length,
    occurrences:record.hunks.reduce((n,h)=>n+(h.occurrences??1),0),raw_byte_reverse:true,
    scope:output.startsWith('one-time/')?'COMPLETE_ORIGINAL_COUPLED_CHECK_REGION; NEW_PREFIX_AND_SUFFIX_EXPLICIT_SCHEMA_PROPOSAL':'COMPLETE_OWNER_OR_RUNTIME'});
  if(record.file)pin(record.commit??commit,record.file,record.input_file_sha256??record.input_sha256);
  if(record.input)assert.deepEqual(fs.readFileSync(path.join(root,record.input)),before);
  if(record.current_path)pin(record.commit,record.current_path,record.current_sha256);
 }
}
assert.equal(reverses.length,16);
assert.deepEqual(read('before/06_complete_fin4.sqlinc'),fs.readFileSync(path.join(dir,json('FIN4_REVERSE.json').selected_missing_before)));

const factsFolder='paid-shift-overlap-native-facts-construction-20261014';
for(const p of JSON.parse(fs.readFileSync(path.join(dir,'../'+factsFolder+'/CURRENT_NATIVE_INPUT_PINS.json'))).inputs)pin(p.commit,p.file,p.sha256);
pin(commit,'supabase/migrations/30092026_2129_weekly_source_office_resolution.sql');
for(const file of ['PINNED_A0_CURRENT_SOURCE_INDEX.mjs'])pin(ownCommit,'docs/healthroster-build/proposals/a0-current-root-install2-forward/'+file);
pin(ownCommit,'docs/healthroster-build/proposals/strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs');
const vm=loadProjectionOwner();
for(const p of vm.pins.filter(p=>p.file!=='broker/src/weekly-source/upload-publication-owner.mjs'))pin(commit,p.file,p.sha256);

const dependencyFolders=[
 'kept-pay-identical-rekey-current-root-forward-20261014',
 'kept-credit-batch-repeatable-successor-20261014',
 'missing-default-current-native-proposal-20261014',
 'missing-retained-snapshot-mapping-successor-20261014',
 'missing-correction-baseline-successor-20261014',
 'missing-correction-expanded-native-scope-successor-20261014',
 'missing-revision-census-guard-successor-20261014',
 'missing-accept-current-native-construction-20261014',
 'missing-accept-head-continuity-construction-20261014',
 'missing-accept-deferred-anchor-successor-20261014',
 'rekey-source-alias-current-native-successor-20261014',
 'rekey-source-alias-all-owner-successor-20261014',
 factsFolder
];
const dependencies=[];let preservedArtifacts=0;
for(const folder of dependencyFolders){
 const file='docs/healthroster-build/proposals/'+folder+'/FROZEN_ARTIFACT_MANIFEST.json';
 const raw=fs.readFileSync(path.join(root,file));assert.deepEqual(raw,git(ownCommit,file));
 const manifest=JSON.parse(raw);
 for(const item of manifest.artifacts){
  const p='docs/healthroster-build/proposals/'+folder+'/'+item.file;
  const bytes=fs.readFileSync(path.join(root,p));assert.equal(bytes.length,item.bytes,p);assert.equal(sha(bytes),item.sha256,p);
  assert.deepEqual(bytes,git(ownCommit,p),p);preservedArtifacts++;
 }
 dependencies.push({folder,manifest_sha256:sha(raw),artifact_count:manifest.artifacts.length,saved_and_committed_preserved:true});
}

const amendmentCommit='3c5090a7bdd2f64ebcfb0810f86693f10a0295e9';
const amendmentPath='docs/healthroster-build/proposals/root-effective-body-amendment-census-20261014/BODY_AMENDMENT_CENSUS.json';
const amendmentPin=pin(amendmentCommit,amendmentPath),amendments=JSON.parse(git(amendmentCommit,amendmentPath));
const amendmentSupplement={...amendmentPin,status:amendments.status,
 source_commit:amendments.sourceCommit,root_count:amendments.rootCount,input_count:amendments.sourceInputCount,
 source_occurrences:amendments.sourceOccurrences,limitations:amendments.limitations,
 authoritative_inventory:false,uppercase_SQL_inventory_successor_required:true,
 author_reported_missing_case_sensitive_paths:['supabase/migrations/26052026_1525_INDEX_OUTBOX.SQL','supabase/migrations/13072026_1648_MIGRATION.SQL'],
 purpose:'PRESERVE_GUARDED_BODY_AMENDMENTS; DOES_NOT_ESTABLISH_FINAL_EFFECTIVE_BODIES'};

const forwardOrder=fs.readdirSync(path.join(dir,'forward')).filter(f=>f.endsWith('.sqlinc')).sort();
assert.equal(forwardOrder.length,19);
const declarations=forwardOrder.flatMap(file=>functionEvents(read('forward/'+file).toString()).events.filter(e=>e.kind==='CREATE').map(e=>({file:'forward/'+file,signature:e.id,definition_sha256:sha(e.definition)})));
assert.equal(declarations.length,20);
const newPrivate=[
 'private.weekly_source_paid_overlap_choice_admit_v1(jsonb,jsonb)',
 'private.weekly_source_paid_overlap_choice_current_v1(uuid,uuid)',
 'private.weekly_source_paid_overlap_projection_lock_v1(uuid,uuid,uuid)',
 'private.weekly_source_paid_choice_revision_guard_v1()',
 'private.weekly_source_paid_match_invoice_movement_insert_v1(uuid,uuid)',
 'private.weekly_source_paid_match_invoice_origin_assert_v1(uuid)'
];
for(const signature of newPrivate)assert.ok(declarations.some(d=>d.signature===signature),signature);
const prerequisites=[
 {packet:'rekey-source-alias-all-owner-successor-20261014',file:'forward/00_known_source_key.sqlinc'},
 {packet:'rekey-source-alias-current-native-successor-20261014',file:'forward/01_complete_identical_rekey_prior.sqlinc'},
 ...['01_work_event_payment_facts.sqlinc','02_complete_keep_context.sqlinc','03_paid_overlap_source_inputs.sqlinc'].map(file=>({packet:factsFolder,file:'forward/'+file})),
 {packet:'missing-retained-snapshot-mapping-successor-20261014',file:'forward/09_complete_shared_current_capture_proposal.sqlinc'},
 {packet:'missing-correction-expanded-native-scope-successor-20261014',file:'forward/02_complete_missing_correction_baseline.sqlinc'},
 {packet:'missing-revision-census-guard-successor-20261014',file:'forward/04_revision_census_guard.sqlinc'},
 {packet:'missing-accept-deferred-anchor-successor-20261014',file:'forward/01_complete_current_removal.sqlinc'}
].map(p=>{const bytes=fs.readFileSync(path.join(dir,'..',p.packet,p.file));return{...p,bytes:bytes.length,sha256:sha(bytes)};});
write('CURRENT_SOURCE_INPUT_PINS.json',{source_only:true,current_SQL_input_commit:commit,current_runtime_input_commit:commit,
 exact_inputs:[...inputs.values()],dependency_manifest_pins:dependencies,preserved_dependency_artifacts:preservedArtifacts,
 raw_before_after_reverse_scope:'FULL_SELECTED_OWNERS; NOT_WHOLE_CURRENT_DECLARATION_OR_AMENDMENT_CLOSURE',
 guarded_amendment_supplement:amendmentSupplement,canonical_mounted:false,native_proof:false});
write('FULL_OWNER_REVERSAL_PROOF.json',{source_only:true,records:reverses,complete_SQL_owners:14,complete_runtime_owners:1,
 original_coupled_CHECK_regions:1,reverse_count:16,total_hunks:reverses.reduce((n,r)=>n+r.hunks,0),
 total_hunk_occurrences:reverses.reduce((n,r)=>n+r.occurrences,0),all_raw_byte_reverses:true,
 native_effective_body_closure:false,canonical_mounted:false,native_proof:false});
write('SELECTED_SOURCE_RECIPE.json',{source_only:true,status:'UNMOUNTED_CONSTRUCTION_FOR_BOUNDED_SOURCE_REVIEW',independent_review:'PENDING',
 current_input_commit:commit,own_published_prerequisite_commit:ownCommit,immutable_prerequisite_manifests:dependencies,
 finite_direct_dependencies:prerequisites,
 complete_dependency_composition_required:'RETAIN WHOLE ACCEPTED Keep/KC14 + Missing56/M1/M2/expanded/M3 + Accept30/continuity/deferred/source foundations. This finite direct list is not a replacement whole mount recipe. Later kept_context facts15 supersedes Accept30 context; selected current-removal repair supersedes original continuity reader; corrected all-owner known-key helper supersedes original rekey15 helper.',
 excluded_predecessors:['rekey-source-alias-current-native-successor-20261014/forward/00_known_source_key.sqlinc','missing-accept-head-continuity-construction-20261014/forward/01_complete_current_removal.sqlinc','missing-default-current-native-proposal-20261014/forward/06_complete_fin4_default_draft.sqlinc'],
 one_time_source_order:['one-time/00_paid_shift_choice_fields.sqlinc','one-time/01_invoice_snapshot_origins.sqlinc'],
 repeatable_source_order:forwardOrder.map(file=>'forward/'+file),
 complete_current_declarations:declarations,new_private_signatures:newPrivate,
 new_private_acl:'logical postgres; PUBLIC/anon/authenticated/service_role revoked; owner only',
 existing_public_service_signatures:declarations.filter(d=>d.signature.startsWith('public.')).map(d=>d.signature),
 existing_public_acl:'logical postgres; PUBLIC/anon/authenticated revoked; retain service_role EXECUTE only',
 runtime_forward:{file:'forward/broker/src/weekly-source/upload-publication-owner.mjs',sha256:sha(read('forward/broker/src/weekly-source/upload-publication-owner.mjs')),reverse:'RUNTIME_REVERSE.json'},
 unchanged_native_calculators_and_classifiers:true,policy_X:'Original settlement/economic scope, all-component native proof and frozen artifacts remain authoritative; no alternate calculator/key/provider/settlement path.',
 new_business_tables:0,existing_choices_store_only:true,new_columns:13,new_transition_outcomes:0,
 field_ABI:{resolution_snapshot:['pay_effect','paid_overlap_choice_id'],movement:['pay_effect','invoice_only_snapshot_line_id'],revision:['paid_overlap_choice_census'],transition:['pay_basis_kind','paid_match_source_rows'],invoice_origin_kind:'SOURCE_INVOICE_ONLY_SNAPSHOT',retained_basis:'RETAINED_PAID_MATCH'},
 shared_banking_owners_changed_in_this_packet:false,shared_capture_dependency:'Previously proposed mapping successor; current-owner shared compatibility remains required; no Banking canonical overwrite.',
 canonical_root_naming_and_mount:'ROOT-OWNED FUTURE CONTAINED SOURCE STAGE AFTER CURRENT OWNERS ONLY AFTER COMPLETE INTEGRATION REVIEW/CONTRACT/ACL/VERIFIERS; NO CANONICAL ROOT CREATED HERE',
 source_fixed_expenses_complete:false,full_pay_prepared_consumer_census_complete:false,Office_confirm_UI_complete:false,
 CorrectFinal_NHSP_protected_Legacy_complete:false,OFF_multi_period_fixture_complete:false,
 Report_companion_binding_complete:false,current_literal_registry_and_verifier_union_complete:false,
 canonical_mounted:false,SQL_compiled:false,native_proof:false,full_primary_complete:false,remaining_gates:'SOURCE_BOUNDARY_AND_REMAINING_CODE.md'});

function run(args){const r=spawnSync(process.execPath,args,{cwd:root,encoding:'utf8',maxBuffer:2e6});assert.equal(r.status,0,r.stderr+r.stdout);return r.stdout;}
const output=run(['--test',relative+'/writer-source.test.mjs']);assert.match(output,/tests 17/);assert.match(output,/pass 17/);assert.match(output,/fail 0/);
const files=[];function walk(prefix=''){for(const entry of fs.readdirSync(path.join(dir,prefix),{withFileTypes:true})){const file=prefix?prefix+'/'+entry.name:entry.name;if(entry.isDirectory())walk(file);else files.push(file);}}walk();
const scripts=files.filter(f=>f.endsWith('.mjs'));for(const file of scripts)run(['--check',relative+'/'+file]);
const whitespace=[];const credentialFindings=[];
const patterns=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/g,/\bgh[pousr]_[A-Za-z0-9]{20,}\b/g,/\bAKIA[0-9A-Z]{16}\b/g,/\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/g];
for(const file of files){const s=read(file).toString();for(const regex of patterns){regex.lastIndex=0;if(regex.test(s))credentialFindings.push(file);}const lines=s.split(/\r?\n/);for(let i=0;i<lines.length;i++)if(/[ \t]+$/.test(lines[i]))whitespace.push({file,line:i+1,kind:'TRAILING_SOURCE_WHITESPACE'});if(/(?:\r?\n){2,}$/.test(s))whitespace.push({file,kind:'EXACT_ARCHIVED_OR_PROPOSED_EOF_BLANK_LINES'});}
assert.equal(credentialFindings.length,0,'credential-pattern finding');
write('SOURCE_CHECK_RESULT.json',{source_only:true,independent_review:'PENDING',effect_free_cases:17,passed:17,failed:0,
 syntax_passed:scripts.length,complete_SQL_reversals:14,complete_Broker_reversals:1,exact_original_CHECK_region_reversal:1,
 full_reverse_records:16,total_hunks:reverses.reduce((n,r)=>n+r.hunks,0),all_hunk_occurrences_verified:true,
 private_additions:6,public_additions:0,selected_SQL_declarations:20,selected_SQL_leaves:19,
 one_time_source_leaves:2,exact_input_pins:inputs.size,preserved_dependency_manifests:dependencies.length,
 preserved_dependency_artifacts:preservedArtifacts,credential_pattern_findings:0,
 whitespace_disposition:whitespace.length?'EXCEPTIONS PRESERVED; NOT BLANKET PASS':'PASS',whitespace_exceptions:whitespace,
 native_SQL_queries:0,SQL_compiled:false,native_proof:false,API_calls:0,canonical_mounted:false,
 source_fixed_expenses_complete:false,full_pay_prepared_consumer_census_complete:false,Office_confirm_UI_complete:false,
 paid_same_separate_full_primary_complete:false,scratch_created:false,cleanup_retried:false,
 actual_preserved_scratch_receipt:'Prior managed-key-source-gCW1f5 refusal remains; this creator created no scratch and performs no deletion.',
 remaining_gates:'SOURCE_BOUNDARY_AND_REMAINING_CODE.md'});
files.push('SOURCE_CHECK_RESULT.json');
const artifacts=[...new Set(files)].sort().map(file=>{const bytes=read(file);return{file,bytes:bytes.length,sha256:sha(bytes)};});
write('FROZEN_ARTIFACT_MANIFEST.json',{schema_version:'PAID_SAME_SEPARATE_NONE_CURRENT_NATIVE_WRITER_CONSTRUCTION_FREEZE_V1',
 source_only:true,independent_review:'PENDING',canonical_mounted:false,SQL_compiled:false,native_proof:false,
 full_primary_complete:false,artifact_count:artifacts.length,artifacts});
console.log(JSON.stringify({artifact_count:artifacts.length,manifest_sha256:sha(read('FROZEN_ARTIFACT_MANIFEST.json')),
 check_sha256:sha(read('SOURCE_CHECK_RESULT.json')),recipe_sha256:sha(read('SELECTED_SOURCE_RECIPE.json')),
 full_reverse_records:16,cases:17,syntax_passed:scripts.length,input_pins:inputs.size,preserved_dependency_artifacts:preservedArtifacts,
 credential_pattern_findings:0,whitespace_exceptions:whitespace.length,native_proof:false,full_primary_complete:false}));
