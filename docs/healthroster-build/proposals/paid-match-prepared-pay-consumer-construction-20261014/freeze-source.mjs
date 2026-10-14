import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),rel=path.relative(root,dir).replaceAll('\\','/');
const sha=b=>createHash('sha256').update(b).digest('hex'),read=f=>fs.readFileSync(path.join(dir,f)),write=(f,o)=>fs.writeFileSync(path.join(dir,f),JSON.stringify(o,null,2)+'\n');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const proof=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json')),inputPins=[];
for(const r of proof.records){const b=execFileSync('git',['show',r.commit+':'+r.file],{cwd:root,maxBuffer:3e6});assert.equal(sha(b),r.input_sha256);if(!inputPins.some(p=>p.file===r.file))inputPins.push({commit:r.commit,file:r.file,bytes:b.length,sha256:sha(b)});}
const deps=['paid-shift-match-writer-none-construction-20261014','paid-match-source-expense-native-construction-20261014','missing-default-current-native-proposal-20261014'];
const dependencyPins=[];let preserved=0;
for(const packet of deps){const file='docs/healthroster-build/proposals/'+packet+'/FROZEN_ARTIFACT_MANIFEST.json',raw=fs.readFileSync(path.join(root,file));
 assert.deepEqual(raw,execFileSync('git',['show','8db419193eb6faf08e3c5adb1d3205326b347d83:'+file],{cwd:root,maxBuffer:2e6}));
 const manifest=JSON.parse(raw);for(const a of manifest.artifacts){const b=fs.readFileSync(path.join(root,'docs/healthroster-build/proposals',packet,a.file));assert.equal(b.length,a.bytes);assert.equal(sha(b),a.sha256);preserved++;}
 dependencyPins.push({packet,manifest_sha256:sha(raw),artifact_count:manifest.artifact_count,preserved:true});
}
write('CURRENT_SOURCE_INPUT_PINS.json',{source_only:true,inputs:inputPins,dependencies:dependencyPins,preserved_dependency_artifacts:preserved,
 actual_native_before_definitions_checked:true,guarded_body_amendment_and_native_catalogue_closure:false});
write('SELECTED_SOURCE_RECIPE.json',{source_only:true,independent_review:'PENDING',own_preceding_commit:'8db419193eb6faf08e3c5adb1d3205326b347d83',
 current_SQL_input_commit:proof.records[0].commit,preceding_frozen_dependencies:dependencyPins,
 selected_source_order:['forward/01_prepared_paid_choice_census.sqlinc','forward/02_complete_prepared_context.sqlinc','forward/03_complete_prepare_result.sqlinc','forward/04_complete_apply_census_recheck.sqlinc','forward/05_complete_current_pay_expenses.sqlinc'],
 excluded_complete_alternatives:proof.records.filter(r=>r.selected_input).map(r=>r.selected_input),
 new_private_signatures:['private.weekly_source_paid_match_prepared_census_recheck_v1(uuid,uuid)'],new_private_acl:'logical postgres; PUBLIC/anon/authenticated/service_role revoked; owner only',
 existing_service_signatures:proof.records.filter(r=>r.signature.startsWith('public.')).map(r=>r.signature),
 existing_service_acl:'logical postgres; PUBLIC/anon/authenticated revoked; service_role EXECUTE only',
 existing_private_signatures:proof.records.filter(r=>r.signature.startsWith('private.')).map(r=>r.signature),
 new_public_signatures:[],new_tables:0,new_columns:0,new_money_calculators:0,source_replay_actor_root_preflight_cash_guards_unchanged:true,
 CorrectFinal_paid_choice_admission_complete:false,complete_pay_consumer_census:false,whole_prepared_ROOT_scope_proof:false,
 complete_Office_OFF_NHSP_protected_Legacy_Report:false,final_current_union_and_native_contract:false,
 canonical_mounted:false,SQL_compiled:false,native_proof:false,full_primary_complete:false,remaining_gates:'SOURCE_BOUNDARY_AND_REMAINING_CODE.md'});
function run(args){const r=spawnSync(process.execPath,args,{cwd:root,encoding:'utf8',maxBuffer:2e6});assert.equal(r.status,0,r.stdout+r.stderr);return r.stdout;}
const out=run(['--test',rel+'/prepared-source.test.mjs']);assert.match(out,/tests 9/);assert.match(out,/pass 9/);assert.match(out,/fail 0/);
const files=[];function walk(prefix=''){for(const e of fs.readdirSync(path.join(dir,prefix),{withFileTypes:true})){const f=prefix?prefix+'/'+e.name:e.name;if(e.isDirectory())walk(f);else files.push(f);}}walk();
const scripts=files.filter(f=>f.endsWith('.mjs'));for(const f of scripts)run(['--check',rel+'/'+f]);
const rx=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/\bgh[pousr]_[A-Za-z0-9]{20,}\b/,/\bAKIA[0-9A-Z]{16}\b/,/\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/],whitespace=[];
for(const f of files){const s=read(f).toString();assert.ok(!rx.some(r=>r.test(s)),'credential pattern '+f);s.split(/\r?\n/).forEach((line,i)=>{if(/[ \t]+$/.test(line))whitespace.push({file:f,line:i+1});});if(/(?:\r?\n){2,}$/.test(s))whitespace.push({file:f,kind:'EXACT_SOURCE_EOF_BLANK_LINES'});}
write('SOURCE_CHECK_RESULT.json',{source_only:true,independent_review:'PENDING',cases:9,passed:9,failed:0,syntax_passed:scripts.length,
 complete_current_owner_reversals:4,unique_hunks:6,private_additions:1,public_additions:0,new_stores_or_columns:0,
 actual_current_native_definition_equality_rechecks:3,source_input_pins:inputPins.length,preserved_dependency_artifacts:preserved,
 credential_pattern_findings:0,whitespace_disposition:whitespace.length?'EXCEPTIONS PRESERVED; NO BLANKET PASS':'PASS',whitespace_exceptions:whitespace,
 canonical_mounted:false,SQL_compiled:false,native_proof:false,real_database_or_API_calls:0,scratch_created:false,cleanup_retried:false,
 CorrectFinal_paid_choice_admission_complete:false,complete_pay_consumer_census:false,full_primary_complete:false,remaining_gates:'SOURCE_BOUNDARY_AND_REMAINING_CODE.md'});
files.push('SOURCE_CHECK_RESULT.json');const artifacts=[...new Set(files)].sort().map(file=>{const b=read(file);return{file,bytes:b.length,sha256:sha(b)};});
write('FROZEN_ARTIFACT_MANIFEST.json',{schema_version:'PAID_MATCH_PREPARED_PAY_CONSUMER_CONSTRUCTION_FREEZE_V1',source_only:true,independent_review:'PENDING',
 canonical_mounted:false,SQL_compiled:false,native_proof:false,full_primary_complete:false,artifact_count:artifacts.length,artifacts});
console.log(JSON.stringify({artifacts:artifacts.length,manifest_sha256:sha(read('FROZEN_ARTIFACT_MANIFEST.json')),check_sha256:sha(read('SOURCE_CHECK_RESULT.json')),
 recipe_sha256:sha(read('SELECTED_SOURCE_RECIPE.json')),cases:9,syntax_passed:scripts.length,private_additions:1,source_only:true,native_proof:false}));
