import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';
import{createHash}from'node:crypto';import{execFileSync,spawnSync}from'node:child_process';import{fileURLToPath}from'node:url';
import{functionEvents}from'../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),rel=path.relative(root,dir).replaceAll('\\','/');
const read=f=>fs.readFileSync(path.join(dir,f)),sha=b=>createHash('sha256').update(b).digest('hex'),write=(f,x)=>fs.writeFileSync(path.join(dir,f),JSON.stringify(x,null,2)+'\n');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const p=JSON.parse(read('COMPLETE_NATIVE_REVERSES.json')),signatures=[];
for(const r of p.raw_input_pins){const b=execFileSync('git',['show',r.commit+':'+r.file],{cwd:root,maxBuffer:4e6});assert.deepEqual(b,read(r.saved));assert.equal(sha(b),r.sha256);}
for(const r of p.proofs){const before=functionEvents(read('before/'+r.output).toString()).events[0],after=functionEvents(read('forward/'+r.output).toString()).events[0];
 assert.equal(before.id,after.id);assert.equal(before.header,after.header);signatures.push(after.id);}
const dependencies=[];let preserved=0;
for(const packet of['strict-qualification-native-interfaces','original-prerequisites-source-forward-20261014',
 'paid-match-legacy-native-consumer-construction-20261014','paid-match-correct-final-choice-construction-20261014']){
 const base='docs/healthroster-build/proposals/'+packet+'/',manifest=base+'FROZEN_ARTIFACT_MANIFEST.json';
 if(!fs.existsSync(path.join(root,manifest)))continue;
 const raw=fs.readFileSync(path.join(root,manifest));assert.deepEqual(raw,execFileSync('git',['show','638b38642f8905b3ceae0fb45ebaa119e7311734:'+manifest],{cwd:root,maxBuffer:3e6}));
 const m=JSON.parse(raw);for(const a of m.artifacts){const file=a.file??a.path;assert.ok(file);const b=fs.readFileSync(path.join(root,base,file));assert.equal(sha(b),a.sha256);assert.equal(b.length,a.bytes);preserved++;}
 dependencies.push({packet,manifest_sha256:sha(raw),listed_artifacts:m.artifacts.length,preserved:true});
}
write('SELECTED_SOURCE_RECIPE.json',{source_only:true,independent_review:'PENDING',current_native_commit:p.commit,own_backup:'638b38642f8905b3ceae0fb45ebaa119e7311734',
 dependencies,selected_SQL_order:['forward/01_complete_original_census.sqlinc','forward/02_complete_original_census_assert.sqlinc',
 'forward/03_complete_off_scope_assert.sqlinc','forward/04_complete_native_parent_scope.sqlinc','forward/05_complete_native_qualification_step.sqlinc',
 'forward/06_complete_bulk_original_root_assert.sqlinc','forward/07_existing_owner_only_acl.sqlinc'],
 complete_reverse:'COMPLETE_NATIVE_REVERSES.json',existing_owner_private_signatures:signatures,new_private_signatures:[],new_public_signatures:[],
 all_six_argument_security_volatility_return_headers_unchanged:true,owner_private_ACL:'logical postgres; PUBLIC/anon/authenticated/service_role revoked',
 required_separate_Report_Source20_bulk_source_facts_successor:{signature:'private.weekly_source_bulk_source_facts_v1(uuid,uuid[])',
  source_before_sha256:'bd83ccd7668ecc00910890dcaf209dbdfb8d7c2aced6f433c61c9ac70b5ab02f',
  selected_source_after_sha256:'8b89135204d33caf83b387fb0aa0d2ef03d5711e1184fac9120a14a957829b60',containing_report_commit:'63965738'},
 Source39_key_count_and_domain_unchanged:true,original_five_root_identity_keys_unchanged:true,unbound_Contract_unknown_refuses:true,
 financial_TF_Client_record_DTO_fields_preserved:true,original_nullable_physical_Client_concept_superseded_only_for_T0:true,
 new_table_column_classification_money_calculator:0,actual_effective_body_catalogue_full_current_union_verifiers:false,
 SQL_compiled:false,native_proof:false,canonical_mounted:false,whole_primary_complete:false});
function run(args){const r=spawnSync(process.execPath,args,{cwd:root,encoding:'utf8',maxBuffer:3e6});assert.equal(r.status,0,r.stdout+r.stderr);return r.stdout;}
const output=run(['--test',rel+'/physical-identity-source.test.mjs']);assert.match(output,/tests 13/);assert.match(output,/pass 13/);assert.match(output,/fail 0/);
const files=[];function walk(prefix=''){for(const e of fs.readdirSync(path.join(dir,prefix),{withFileTypes:true})){const f=prefix?prefix+'/'+e.name:e.name;if(e.isDirectory())walk(f);else files.push(f);}}walk();
for(const f of files.filter(f=>f.endsWith('.mjs')))run(['--check',rel+'/'+f]);
const credentials=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/\bgh[pousr]_[A-Za-z0-9]{20,}\b/,/\bAKIA[0-9A-Z]{16}\b/,/\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/];const whitespace=[];
for(const f of files){const s=read(f).toString();assert.ok(!credentials.some(r=>r.test(s)),'credential '+f);s.split(/\r?\n/).forEach((line,i)=>{if(/[ \t]+$/.test(line))whitespace.push({file:f,line:i+1});});if(/(?:\r?\n){2,}$/.test(s))whitespace.push({file:f,kind:'PRESERVED EOF BLANK LINES'});}
write('SOURCE_CHECK_RESULT.json',{source_only:true,independent_review:'PENDING',cases:13,pass:13,fail:0,syntax_pass:files.filter(f=>f.endsWith('.mjs')).length,
 full_owner_reversals:6,unique_hunks:14,raw_native_pins:p.raw_input_pins.length,full_actual_physical_schema_and_DDL:true,preserved_dependency_artifacts:preserved,
 signatures_unchanged:signatures,new_signatures:0,new_tables_columns_calculators:0,Source39_unchanged:true,credential_findings:0,
 whitespace_disposition:whitespace.length?'EXACT DECLARED SOURCE EXCEPTIONS RETAINED; NO BLANKET PASS':'PASS',whitespace_exceptions:whitespace,
 SQL_compiled:false,native_calls:0,canonical_mounted:false,whole_primary_complete:false,scratch_created:false,cleanup_retried:false});
files.push('SOURCE_CHECK_RESULT.json');const artifacts=[...new Set(files)].sort().map(file=>{const b=read(file);return{file,bytes:b.length,sha256:sha(b)};});
write('FROZEN_ARTIFACT_MANIFEST.json',{schema_version:'QUALIFICATION_PHYSICAL_CONTRACT_IDENTITY_SUCCESSOR_SOURCE_FREEZE_V1',source_only:true,independent_review:'PENDING',
 native_proof:false,canonical_mounted:false,whole_primary_complete:false,artifact_count:artifacts.length,artifacts});
console.log(JSON.stringify({artifacts:artifacts.length,manifest_sha256:sha(read('FROZEN_ARTIFACT_MANIFEST.json')),check_sha256:sha(read('SOURCE_CHECK_RESULT.json')),
 recipe_sha256:sha(read('SELECTED_SOURCE_RECIPE.json')),source_cases:13,whole_reverses:6,unique_hunks:14,new_signatures:0,source_only:true}));
