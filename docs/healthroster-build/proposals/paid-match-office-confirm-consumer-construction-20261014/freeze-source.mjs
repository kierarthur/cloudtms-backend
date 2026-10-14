import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),rel=path.relative(root,dir).replaceAll('\\','/');
const sha=b=>createHash('sha256').update(b).digest('hex'),read=f=>fs.readFileSync(path.join(dir,f)),write=(f,x)=>fs.writeFileSync(path.join(dir,f),JSON.stringify(x,null,2)+'\n');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const input=JSON.parse(read('CURRENT_SOURCE_INPUT_PINS.json')),reverse=JSON.parse(read('COMPLETE_CURRENT_REVERSES.json'));
for(const p of input.inputs){const cwd=p.repository==='frontend'?path.resolve(root,'../../TEST-Frontend'):root;
 const b=execFileSync('git',['show',p.commit+':'+p.file],{cwd,maxBuffer:8e6});assert.equal(b.length,p.bytes);assert.equal(sha(b),p.sha256);assert.deepEqual(b,read(p.saved));}
const packets=['paid-shift-match-writer-none-construction-20261014','paid-match-source-expense-native-construction-20261014','paid-match-prepared-pay-consumer-construction-20261014','paid-shift-overlap-native-facts-construction-20261014','rekey-source-alias-namespace-census-successor-20261014'];
// The alias-census folder is resolved by its actual controlling accepted
// helper, never an invented spelling or a source-spelled older declaration.
packets.pop();
const helperRoot=path.join(root,'docs/healthroster-build/proposals');
const aliasFolder=fs.readdirSync(helperRoot,{withFileTypes:true}).filter(e=>e.isDirectory()&&/rekey.*alias.*successor/.test(e.name)).find(e=>{
 const f=path.join(helperRoot,e.name,'forward/00_known_source_key.sqlinc');return fs.existsSync(f)&&sha(fs.readFileSync(f))==='3efed0771093465c90faed992811b660b8a6f885bcbe7bfd17adfc36eb16d56d';});assert.ok(aliasFolder);packets.push(aliasFolder.name);
const dependencyPins=[];let preserved=0;
for(const packet of packets){const file=`docs/healthroster-build/proposals/${packet}/FROZEN_ARTIFACT_MANIFEST.json`,b=fs.readFileSync(path.join(root,file));
 assert.deepEqual(b,execFileSync('git',['show','8ca63151708e6482b5ecea580fbb34eec6a51ddb:'+file],{cwd:root,maxBuffer:2e6}));const manifest=JSON.parse(b);
 for(const a of manifest.artifacts){const bytes=fs.readFileSync(path.join(helperRoot,packet,a.file));assert.equal(bytes.length,a.bytes);assert.equal(sha(bytes),a.sha256);preserved++;}
 dependencyPins.push({packet,manifest_sha256:sha(b),artifacts:manifest.artifact_count});}
const decl=JSON.parse(read('CURRENT_DECLARATION_AND_BODY_PATCH_PINS.json'));
assert.deepEqual([decl.root_count,decl.input_paths,decl.occurrences],[1484,1745,2160]);
write('SELECTED_SOURCE_RECIPE.json',{source_only:true,independent_review:'PENDING',preceding_own_checkpoint:'8ca63151708e6482b5ecea580fbb34eec6a51ddb',
 native_source_commit:input.inputs[0].commit,Office_feature_source:'2d15d121ac888a2e2a076dc7a2c4be85f9958556',Office_installed_main:'8ea8c8af0435702d3bf032b3197fe0e0eda64d1c',
 dependency_pins:dependencyPins,selected_SQL_order:['forward/01_paid_match_question.sqlinc','forward/02_complete_office_workspace.sqlinc','forward/03_office_workspace_acl.sqlinc'],
 required_unmounted_native_dependencies:['accepted facts15 single payment-facts and overlap helpers','accepted rekey all-owner 3efed077 + identical helper','writer58 choice admission/current reader/closed office recheck/projection/context/native NONE consequences'],
 selected_frontend_forward_files:[{source:'forward/frontend/js/weekly-source/workspace-actions.js',destination:'js/weekly-source/workspace-actions.js'},{source:'forward/frontend/js/weekly-source/import-workspace.js',destination:'js/weekly-source/import-workspace.js'}],
 frontend_predecessor_current_composition_required_before_mount:true,unchanged_existing_runtime_dependency:'writer58 forward/broker/src/weekly-source/upload-publication-owner.mjs',
 unchanged_existing_runtime_route:'fa661 broker/src/weekly-source/routes.js (U3/Undo preserved; no new operation)',
 new_private_signatures:['private.weekly_source_paid_match_question_v1(uuid,uuid,uuid)'],new_private_acl:'logical postgres; PUBLIC/anon/authenticated/service_role revoked; owner only',
 existing_public_signatures:['public.weekly_source_office_workspace_v1(jsonb)'],existing_public_acl:'logical postgres; PUBLIC/anon/authenticated revoked; service_role EXECUTE only',
 new_public_signatures:[],new_money_classifiers:0,new_tables:0,new_columns:0,compiled_contract_and_current_verifier_union:false,
 canonical_mounted:false,native_proof:false,whole_pipeline_complete:false,remaining_gates:'SOURCE_BOUNDARY_AND_REMAINING_CODE.md'});
function run(args){const r=spawnSync(process.execPath,args,{cwd:root,encoding:'utf8',maxBuffer:2e6});assert.equal(r.status,0,r.stdout+r.stderr);return r.stdout;}
const result=run(['--test',rel+'/office-source.test.mjs']);assert.match(result,/tests 18/);assert.match(result,/pass 18/);assert.match(result,/fail 0/);
const files=[];function walk(prefix=''){for(const e of fs.readdirSync(path.join(dir,prefix),{withFileTypes:true})){const f=prefix?prefix+'/'+e.name:e.name;if(e.isDirectory())walk(f);else files.push(f);}}walk();
const syntax=files.filter(f=>/\.(mjs|js)$/.test(f));for(const f of syntax)run(['--check',rel+'/'+f]);
const rx=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/\bgh[pousr]_[A-Za-z0-9]{20,}\b/,/\bAKIA[0-9A-Z]{16}\b/,/\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/],whitespace=[];
for(const f of files){const s=read(f).toString();assert.ok(!rx.some(r=>r.test(s)),'credential pattern '+f);s.split(/\r?\n/).forEach((line,i)=>{if(/[ \t]+$/.test(line))whitespace.push({file:f,line:i+1});});if(/(?:\r?\n){2,}$/.test(s))whitespace.push({file:f,kind:'EXACT_SOURCE_EOF_BLANK_LINES'});}
write('SOURCE_CHECK_RESULT.json',{source_only:true,independent_review:'PENDING',cases:18,passed:18,failed:0,syntax_passed:syntax.length,
 complete_current_owner_reversals:reverse.records.length,unique_hunks:reverse.records.reduce((n,r)=>n+r.hunks.length,0),input_pins:input.inputs.length,
 current_literal_source_roots:1484,current_input_paths:1745,current_include_occurrences:2160,guarded_pg_get_functiondef_sites:22,native_current_body_proof:false,
 preserved_frozen_dependency_artifacts:preserved,private_additions:1,public_additions:0,store_column_money_calculator_additions:0,
 credential_pattern_findings:0,whitespace_disposition:whitespace.length?'EXACT INPUT/OWNER EXCEPTIONS RETAINED; NO BLANKET PASS':'PASS',whitespace_exceptions:whitespace,
 actual_SQL_compilation:false,native_database_API_calls:0,browser_or_native_UI_proof:false,canonical_mounted:false,whole_pipeline_complete:false,
 scratch_created:false,cleanup_retried:false,remaining_gates:'SOURCE_BOUNDARY_AND_REMAINING_CODE.md'});
files.push('SOURCE_CHECK_RESULT.json');const artifacts=[...new Set(files)].sort().map(file=>{const b=read(file);return{file,bytes:b.length,sha256:sha(b)};});
write('FROZEN_ARTIFACT_MANIFEST.json',{schema_version:'PAID_MATCH_OFFICE_CONFIRM_CONSUMER_CONSTRUCTION_FREEZE_V1',source_only:true,independent_review:'PENDING',
 canonical_mounted:false,native_proof:false,whole_pipeline_complete:false,artifact_count:artifacts.length,artifacts});
console.log(JSON.stringify({artifacts:artifacts.length,manifest_sha256:sha(read('FROZEN_ARTIFACT_MANIFEST.json')),check_sha256:sha(read('SOURCE_CHECK_RESULT.json')),
 recipe_sha256:sha(read('SELECTED_SOURCE_RECIPE.json')),cases:18,syntax:syntax.length,private_additions:1,source_only:true,native:false}));
