import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import{createHash}from'node:crypto';import{execFileSync,spawnSync}from'node:child_process';import{fileURLToPath}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),rel=path.relative(root,dir).replaceAll('\\','/');
const read=f=>fs.readFileSync(path.join(dir,f)),sha=b=>createHash('sha256').update(b).digest('hex'),write=(f,x)=>fs.writeFileSync(path.join(dir,f),JSON.stringify(x,null,2)+'\n');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const proof=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json')),office=JSON.parse(read('COMPLETE_OFFICE_REVERSE.json'));
const dependencies=[];let preserved=0;
for(const packet of ['paid-shift-overlap-native-facts-construction-20261014','paid-shift-match-writer-none-construction-20261014',
 'paid-match-source-expense-native-construction-20261014','paid-match-prepared-pay-consumer-construction-20261014',
 'paid-match-office-confirm-consumer-construction-20261014','paid-match-retained-current-capture-construction-20261014',
 'paid-match-prepared-empty-census-successor-20261014','paid-match-legacy-native-consumer-construction-20261014']){
 const base='docs/healthroster-build/proposals/'+packet+'/',raw=fs.readFileSync(path.join(root,base,'FROZEN_ARTIFACT_MANIFEST.json'));
 if(!packet.includes('legacy-native'))assert.deepEqual(raw,execFileSync('git',['show',proof.base+':'+base+'FROZEN_ARTIFACT_MANIFEST.json'],{cwd:root,maxBuffer:3e6}));
 else assert.equal(sha(raw),proof.legacy_unpublished_manifest_sha256);
 const m=JSON.parse(raw);for(const a of m.artifacts){const b=fs.readFileSync(path.join(root,base,a.file));assert.equal(b.length,a.bytes);assert.equal(sha(b),a.sha256);preserved++;}
 dependencies.push({packet,manifest_sha256:sha(raw),artifacts:m.artifact_count,independent_review:packet.includes('legacy-native')?'PENDING':'SEE_PARENT_BOUNDED_SOURCE_REPORT',published:!packet.includes('legacy-native')});
}
for(const p of proof.native_inputs){const b=execFileSync('git',['show',p.commit+':'+p.file],{cwd:root,maxBuffer:4e6});assert.deepEqual(b,read(p.saved));assert.equal(sha(b),p.sha256);}
for(const p of proof.native_definition_inputs){const b=execFileSync('git',['show',p.commit+':'+p.file],{cwd:root,maxBuffer:4e6});assert.equal(sha(b),p.whole_input_sha256);}
write('SELECTED_SOURCE_RECIPE.json',{source_only:true,independent_review:'PENDING',controlling_native_commit:proof.native,prior_own_checkpoint:proof.base,dependencies,
 selected_SQL_order:['forward/00_correction_choice_scope.sqlinc','forward/01_complete_overlap_inputs.sqlinc','forward/02_complete_upload_context.sqlinc',
  'forward/03_complete_paid_match_question.sqlinc','forward/04_complete_office_recheck.sqlinc','forward/05_complete_correct_final_office_preview.sqlinc','forward/06_existing_preview_acl.sqlinc'],
 selected_runtime:[{source:'forward/broker/src/weekly-source/correct-final-source-orchestrator.mjs',destination:'broker/src/weekly-source/correct-final-source-orchestrator.mjs'},
  {source:'forward/broker/src/weekly-source/upload-publication-owner.mjs',destination:'broker/src/weekly-source/upload-publication-owner.mjs'},
  {source:'forward/frontend/js/weekly-source/workspace-actions.js',destination:'js/weekly-source/workspace-actions.js'}],
 before_after_reverse_proofs:['COMPLETE_CURRENT_REVERSES.json','COMPLETE_OFFICE_REVERSE.json'],new_owner_private_signatures:['private.weekly_source_paid_match_correction_scope_v1(uuid,uuid,boolean)'],
 existing_public_signatures:['public.weekly_source_upload_context_v1(jsonb)','public.weekly_source_office_recheck_begin_v1(jsonb)'],new_public_signatures:[],
 owner_private_acl:'logical postgres; PUBLIC/anon/authenticated/service_role revoked',existing_public_acl:'retained service-only; final whole-owner ACL assembly still required',
 closed_correction_recheck_additional_keys:['correction_session_id','expected_correction_session_version','expected_comparison_manifest_hash','expected_issue_set_hash'],
 new_store_column_calculator:0,existing_question_keys:13,unchanged_native_correct_final_financial_guards:true,
 unchanged_native_rebuild_publisher_review_owners:true,root_Report32_Money28_NativeR254_SESSION_U3_Undo_whole_join_required:true,
 full_current_literal_private_service_table_verifier_union:false,all_CorrectFinal_paid_paths_qualified:false,all_Legacy_grains_complete:false,
 SQL_compiled:false,native_proof:false,canonical_mounted:false,whole_primary_complete:false});
function run(args){const r=spawnSync(process.execPath,args,{cwd:root,encoding:'utf8',maxBuffer:3e6});assert.equal(r.status,0,r.stdout+r.stderr);return r.stdout;}
const tests=run(['--test',rel+'/correction-source.test.mjs']);assert.match(tests,/tests 19/);assert.match(tests,/pass 19/);assert.match(tests,/fail 0/);
const files=[];function walk(p=''){for(const e of fs.readdirSync(path.join(dir,p),{withFileTypes:true})){const f=p?p+'/'+e.name:e.name;if(e.isDirectory())walk(f);else files.push(f);}}walk();
const syntax=files.filter(f=>/\.(mjs|js)$/.test(f));for(const f of syntax)run(['--check',rel+'/'+f]);
const credentials=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/\bgh[pousr]_[A-Za-z0-9]{20,}\b/,/\bAKIA[0-9A-Z]{16}\b/,/\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/];const whitespace=[];
for(const f of files){const s=read(f).toString();assert.ok(!credentials.some(r=>r.test(s)),'credential '+f);s.split(/\r?\n/).forEach((line,i)=>{if(/[ \t]+$/.test(line))whitespace.push({file:f,line:i+1});});if(/(?:\r?\n){2,}$/.test(s))whitespace.push({file:f,kind:'PRESERVED EOF BLANK LINES'});}
write('SOURCE_CHECK_RESULT.json',{source_only:true,independent_review:'PENDING',cases:19,pass:19,fail:0,syntax_pass:syntax.length,
 complete_owner_reversals:proof.reverses.length+1,unique_hunks:proof.reverses.reduce((n,r)=>n+r.hunks.length,0)+office.hunks.length,
 raw_native_pins:proof.native_inputs.length,extra_native_whole_definition_input_pins:proof.native_definition_inputs.length,
 preserved_dependency_artifacts:preserved,new_private_signatures:1,new_public_signatures:0,new_store_column_calculator:0,credential_findings:0,
 whitespace_disposition:whitespace.length?'EXACT INPUT/DECLARED SOURCE EXCEPTIONS RETAINED; NO BLANKET PASS':'PASS',whitespace_exceptions:whitespace,
 SQL_compiled:false,native_calls:0,canonical_mounted:false,whole_primary_complete:false,scratch_created:false,cleanup_retried:false});
files.push('SOURCE_CHECK_RESULT.json');const artifacts=[...new Set(files)].sort().map(file=>{const b=read(file);return{file,bytes:b.length,sha256:sha(b)};});
write('FROZEN_ARTIFACT_MANIFEST.json',{schema_version:'PAID_MATCH_CORRECT_FINAL_CHOICE_CONSTRUCTION_SOURCE_FREEZE_V1',source_only:true,independent_review:'PENDING',
 native_proof:false,canonical_mounted:false,whole_primary_complete:false,artifact_count:artifacts.length,artifacts});
console.log(JSON.stringify({artifacts:artifacts.length,manifest_sha256:sha(read('FROZEN_ARTIFACT_MANIFEST.json')),check_sha256:sha(read('SOURCE_CHECK_RESULT.json')),
 recipe_sha256:sha(read('SELECTED_SOURCE_RECIPE.json')),cases:19,syntax:syntax.length,whole_reverses:proof.reverses.length+1,unique_hunks:proof.reverses.reduce((n,r)=>n+r.hunks.length,0)+office.hunks.length,new_private_signatures:1,source_only:true}));
