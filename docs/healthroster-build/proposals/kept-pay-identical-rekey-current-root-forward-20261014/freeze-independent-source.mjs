import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents,splitSqlStatements} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const current='5db7abee7925fffbeaf7c4e885cce809980fa079',sha=b=>createHash('sha256').update(b).digest('hex');
const read=file=>fs.readFileSync(path.join(dir,file)),gitCache=new Map();
function git(file){if(!gitCache.has(file))gitCache.set(file,execFileSync('git',['--no-replace-objects','show',current+':'+file],{cwd:root,maxBuffer:32*1024*1024}));return gitCache.get(file);}
function walk(base){return fs.readdirSync(base,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(base,e.name)):[path.relative(dir,path.join(base,e.name)).replaceAll('\\','/')]).sort();}
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')),'Never overwrite a frozen packet');
const rekey=JSON.parse(read('REKEY_CURRENT_CALLERS_AND_INPUT_PINS.json')),kept=JSON.parse(read('KEPT_CREDIT_CURRENT_OWNERS_AND_INPUT_PINS.json'));
assert.equal(rekey.current_sql_and_runtime_selected_commit,current);assert.equal(kept.current_selected_commit,current);
const census=JSON.parse(fs.readFileSync(path.join(dir,'../current-final-classification-union-20261014/CURRENT_FINAL_DECLARATION_OWNERS.json'),'utf8'));
const sourceFiles=walk(path.join(dir,'forward')).filter(f=>f.endsWith('.sqlinc')),decls=[];
const existingNames=new Set(kept.inputs.map(x=>x.id.slice(0,x.id.indexOf('('))).concat(rekey.changes.map(x=>x.signature.slice(0,x.signature.indexOf('(')))));
for(const file of sourceFiles){const bytes=read(file),source=bytes.toString('utf8');assert.equal(splitSqlStatements(source).meta.length,0,file);
 for(const e of functionEvents(source).events.filter(e=>e.kind==='CREATE'))decls.push({file,signature:e.id,definition_sha256:sha(e.definition),kind:existingNames.has(e.name)?'REPLACED_EXISTING_OWNER':'NEW_LITERAL_OWNER'});}
const additions=decls.filter(x=>x.kind==='NEW_LITERAL_OWNER');assert.equal(additions.length,6);
assert.equal(additions.filter(x=>x.signature.startsWith('private.')).length,5);assert.equal(additions.filter(x=>x.signature.startsWith('public.')).length,1);
const callNames=new Set(),relationSyntax=[];for(const file of sourceFiles){const s=read(file).toString('utf8');for(const m of s.matchAll(/\b((?:private|public)\.[a-z_][a-z_0-9]*)\s*\(/gi)){
 const prefix=s.slice(0,m.index);const relation=prefix.match(/\b(insert\s+into|references|create\s+table)\s*$/i)
  ||prefix.match(/\bcreate\s+(?:unique\s+)?index\s+[a-z_][a-z_0-9]*\s+on\s*$/i);
 if(relation){relationSyntax.push({file,name:m[1],offset:m.index,syntax:relation[0].trim()});continue;}callNames.add(m[1]);}}
const newNames=new Set(additions.map(x=>x.signature.slice(0,x.signature.indexOf('(')))),dependencies=[],unclassified=[];
for(const name of [...callNames].sort()){if(newNames.has(name))continue;const pins=census.functions.filter(x=>x.name===name);
 if(!pins.length){unclassified.push(name);continue;}for(const pin of pins){const bytes=git(pin.file),events=functionEvents(bytes.toString('utf8')).events.filter(e=>e.kind==='CREATE'&&e.id===pin.id);assert.ok(events.length,pin.id);
  const e=events.at(-1);assert.equal(sha(e.definition),pin.definition_sha256,'Selected current literal dependency '+pin.id);
  dependencies.push({signature:pin.id,commit:current,file:pin.file,input_bytes:bytes.length,input_sha256:sha(bytes),definition_sha256:sha(e.definition),body_sha256:sha(e.body),same_as_retained_finite_b5_checkpoint:true});}}
const controllingPins=[...kept.auxiliary,...rekey.auxiliary_inputs].map(pin=>{const b=git(pin.file);assert.equal(sha(b),pin.sha256);return pin;});
const dependencyResult={source_only:true,selected_commit:current,literal_declarations:decls,literal_new_owners:additions,literal_direct_consumed_existing_owners:dependencies,
 unclassified_literal_qualified_names:unclassified,nonroutine_literal_relation_syntax:relationSyntax,controlling_auxiliary_pin_occurrences:controllingPins,complete_transitive_or_physical_catalogue_claim:false,
 dependency_basis:'Exact selected Git definitions; finite retained b5 index is used only to locate directly named owners, never as a latest full catalogue',native_proof:false};
assert.equal(unclassified.length,0,'No guessed literal Source dependency: '+JSON.stringify(unclassified));
fs.writeFileSync(path.join(dir,'LITERAL_DECLARATIONS_AND_DIRECT_INPUT_PINS.json'),JSON.stringify(dependencyResult,null,2)+'\n');
const testOut=execFileSync(process.execPath,['--experimental-vm-modules','--test',path.join(dir,'kept-construction-source.test.mjs')],{cwd:root,maxBuffer:2*1024*1024,encoding:'utf8',stdio:['ignore','pipe','pipe']});
assert.match(testOut,/tests 18/);assert.match(testOut,/pass 18/);assert.match(testOut,/fail 0/);
const syntax=['build-rekey-callers.mjs','build-kept-current-owners.mjs','kept-construction-source.test.mjs','freeze-independent-source.mjs','forward/routes.js'];
for(const file of syntax)execFileSync(process.execPath,['--check',path.join(dir,file)],{cwd:root,maxBuffer:1024*1024,stdio:'pipe'});
const credentialRules=[['PRIVATE_KEY',/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/],['JWT_LITERAL',/\beyJ[A-Za-z0-9_-]{25,}\.[A-Za-z0-9_-]{25,}\.[A-Za-z0-9_-]{20,}\b/],['GITHUB_TOKEN',/\b(?:gh[pousr]_[A-Za-z0-9]{25,}|github_pat_[A-Za-z0-9_]{25,})\b/],['AWS_KEY',/\bAKIA[0-9A-Z]{16}\b/]];
const files=walk(dir),findings=[];for(const file of files){const s=read(file).toString('utf8');for(const [rule,regex]of credentialRules)if(regex.test(s))findings.push({file,rule});}
assert.equal(findings.length,0,'Credential scan returns file/rule only, never matched text');
const checks={schema_version:'SOURCE_INDEPENDENT_KEEP_REKEY_CREDIT_CHECK_V1',source_only:true,partial_implementation:true,root_input_commit:current,
 tests:{count:18,pass:18,fail:0,runner:'Inspected source + read-only in-memory current route VM; actual current permission module, mocked native transport, forbidden fetch',output_sha256:sha(testOut)},
 inert_syntax_files:syntax,full_unique_current_owner_reversals:6,new_private_owners:5,new_service_public_owners:1,new_planned_stores:1,declared_direct_native_dependency_count:dependencies.length,
 literal_sql_delimiter_lexing:true,sql_compiled:false,sql_executed:false,native_qualified:false,banking_owner_modified:false,current_fin4_762_modified:false,source39_modified:false,recorder9_13_or_three_bundles_or_tree_exceptions_modified:false,
 credential_scan:{files:files.length,rules:credentialRules.map(x=>x[0]),findings:[]},
 outstanding_source_gaps:['NONADMIN_NEXT_NATIVE_SINGLE_KERNEL_SHARED_OWNER_REVIEW','REAL_MISSING_CANCEL_EXCLUSION_NATIVE_CAPTURE_SELECTOR','REPEATED_ABSENCE_OLD_RESOLUTION_VAT_NATIVE_BASIS','REAL_ACCEPT_SOURCE_FULL_NATIVE_PROPOSAL_BUNDLE_GATE5','ALL_PAID_SAME_SEPARATE_PAY_EFFECT_DUPLICATE_SOURCE_CENSUS','REAL_PREPARED_FROZEN_FINGERPRINT_READ_OFFICE_CHECKLIST','LATEST_FINAL_CLASSIFICATION_FULL_MANDATORY_VERIFIER_OWNER_COUPLING'],
 all_native_compile_catalogue_installation_contract_tool_http_office_phone_financial_security_gates:'HOLD'};
fs.writeFileSync(path.join(dir,'SOURCE_CHECK_RESULT.json'),JSON.stringify(checks,null,2)+'\n');
const artifacts=walk(dir).map(file=>{const b=read(file);return {file,bytes:b.length,sha256:sha(b)};});
const manifest={schema_version:'INDEPENDENT_KEEP_REKEY_CREDIT_PARTIAL_SOURCE_FREEZE_V1',root_input_commit:current,source_only:true,partial_implementation:true,shared_owner_extraction_or_default_disposition_implemented:false,artifact_count:artifacts.length,artifacts};
fs.writeFileSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({source_only:true,partial:true,artifact_count:artifacts.length,manifest_sha256:sha(read('FROZEN_ARTIFACT_MANIFEST.json')),checks_sha256:sha(read('SOURCE_CHECK_RESULT.json')),new_private:5,new_public:1,complete_reversals:6,native_proof:false}));
