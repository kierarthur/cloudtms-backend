import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),rel=path.relative(root,dir).replaceAll('\\','/');
const sha=b=>createHash('sha256').update(b).digest('hex'),read=f=>fs.readFileSync(path.join(dir,f)),write=(f,o)=>fs.writeFileSync(path.join(dir,f),JSON.stringify(o,null,2)+'\n');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const dependency='docs/healthroster-build/proposals/missing-accept-off-admission-construction-20261014',commit='8ca63151708e6482b5ecea580fbb34eec6a51ddb';
const raw=fs.readFileSync(path.join(root,dependency,'FROZEN_ARTIFACT_MANIFEST.json'));assert.deepEqual(raw,execFileSync('git',['show',commit+':'+dependency+'/FROZEN_ARTIFACT_MANIFEST.json'],{cwd:root,maxBuffer:2e6}));
const manifest=JSON.parse(raw);for(const a of manifest.artifacts){const b=fs.readFileSync(path.join(root,dependency,a.file));assert.equal(b.length,a.bytes);assert.equal(sha(b),a.sha256);}
const d=JSON.parse(read('COMPLETE_CONSUMER_REVERSES.json'));
for(const r of d.records){const b=fs.readFileSync(path.join(dir,'..',r.input));assert.equal(sha(b),r.before_sha256);assert.deepEqual(b,execFileSync('git',['show',commit+':docs/healthroster-build/proposals/'+r.input],{cwd:root,maxBuffer:2e6}));}
write('CURRENT_SOURCE_INPUT_PINS.json',{source_only:true,own_preceding_commit:commit,OFF23_manifest_sha256:sha(raw),OFF23_artifacts_preserved:manifest.artifact_count,
 inputs:d.records.map(r=>({commit,file:'docs/healthroster-build/proposals/'+r.input,sha256:r.before_sha256})),
 canonical_actual_OFF98_vs_OFF23_composition_still_required:true,native_proof:false});
write('SELECTED_SOURCE_RECIPE.json',{source_only:true,independent_review:'PENDING',preceding_OFF23_manifest_sha256:sha(raw),
 selected_SQL_order:['forward/03_complete_off_setup_shape.sqlinc','forward/04_complete_off_parent_scope.sqlinc','forward/01_complete_off_command.sqlinc','forward/02_complete_off_transport.sqlinc'],
 selected_runtime:'forward/broker/src/weekly-source/off-rule-native-proxy.mjs',whole_before_after_reverse:'COMPLETE_CONSUMER_REVERSES.json',
 existing_native_operation:'weekly_source_office_recheck_begin_v1',operation_purpose:'SOURCE_FIXTURE_SETUP',new_parent_field:'paid_overlap_target_work_event_id',
 private_or_public_signature_additions:[],new_columns_or_stores:0,new_receipt_or_bundle_or_artifact_slot:0,
 unchanged_native_owner_payload:true,existing_callee_current_actor_payment_Contract_and_choice_guards:true,
 same_scope_serialization_and_I1_order:true,original_purpose_cycle_fixture_CAS_and_event_census_unchanged:true,
 complete_current_actor_owner_ACL_assembly_required:true,actual_current_index_route_and_Office_wiring_complete:false,
 genuine_multi_period_setup_and_original_root_CAS_coverage_complete:false,settled_Original_native_fixture_or_proof_minted:false,
 canonical_mounted:false,SQL_compiled:false,native_proof:false,full_primary_complete:false,remaining_gates:'SOURCE_BOUNDARY_AND_REMAINING_CODE.md'});
function run(args){const r=spawnSync(process.execPath,args,{cwd:root,encoding:'utf8',maxBuffer:2e6});assert.equal(r.status,0,r.stdout+r.stderr);return r.stdout;}
const out=run(['--test',rel+'/off-source.test.mjs']);assert.match(out,/tests 8/);assert.match(out,/pass 8/);assert.match(out,/fail 0/);
const files=[];function walk(prefix=''){for(const e of fs.readdirSync(path.join(dir,prefix),{withFileTypes:true})){const f=prefix?prefix+'/'+e.name:e.name;if(e.isDirectory())walk(f);else files.push(f);}}walk();
const scripts=files.filter(f=>f.endsWith('.mjs'));for(const f of scripts)run(['--check',rel+'/'+f]);
const rx=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/\bgh[pousr]_[A-Za-z0-9]{20,}\b/,/\bAKIA[0-9A-Z]{16}\b/,/\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/],whitespace=[];
for(const f of files){const s=read(f).toString();assert.ok(!rx.some(r=>r.test(s)),'credential pattern '+f);s.split(/\r?\n/).forEach((line,i)=>{if(/[ \t]+$/.test(line))whitespace.push({file:f,line:i+1});});if(/(?:\r?\n){2,}$/.test(s))whitespace.push({file:f,kind:'EXACT_SOURCE_EOF_BLANK_LINES'});}
write('SOURCE_CHECK_RESULT.json',{source_only:true,independent_review:'PENDING',cases:8,passed:8,failed:0,syntax_passed:scripts.length,
 complete_SQL_owners_reversed:4,complete_runtime_owners_reversed:1,unique_hunks:7,private_or_public_signature_additions:0,
 existing_newly_dispatched_operation:'weekly_source_office_recheck_begin_v1',qualified_fresh_setup_allowlist_required:true,
 native_parent_target_actual_event_read:true,OFF23_preserved:manifest.artifact_count,credential_pattern_findings:0,
 whitespace_disposition:whitespace.length?'EXCEPTIONS PRESERVED; NO BLANKET PASS':'PASS',whitespace_exceptions:whitespace,
 genuine_multi_period_fixture_complete:false,settled_Original_fixture_created:false,complete_current_caller_wiring:false,
 canonical_mounted:false,SQL_compiled:false,native_proof:false,real_database_or_API_calls:0,scratch_created:false,cleanup_retried:false,
 full_primary_complete:false,remaining_gates:'SOURCE_BOUNDARY_AND_REMAINING_CODE.md'});
files.push('SOURCE_CHECK_RESULT.json');const artifacts=[...new Set(files)].sort().map(file=>{const b=read(file);return{file,bytes:b.length,sha256:sha(b)};});
write('FROZEN_ARTIFACT_MANIFEST.json',{schema_version:'PAID_MATCH_OFF_NATIVE_COMMAND_CONSUMER_CONSTRUCTION_FREEZE_V1',source_only:true,independent_review:'PENDING',
 canonical_mounted:false,SQL_compiled:false,native_proof:false,full_primary_complete:false,artifact_count:artifacts.length,artifacts});
console.log(JSON.stringify({artifacts:artifacts.length,manifest_sha256:sha(read('FROZEN_ARTIFACT_MANIFEST.json')),check_sha256:sha(read('SOURCE_CHECK_RESULT.json')),
 recipe_sha256:sha(read('SELECTED_SOURCE_RECIPE.json')),cases:8,syntax_passed:scripts.length,source_only:true,native_proof:false}));
