import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),rel=path.relative(root,dir).replaceAll('\\','/');
const read=f=>fs.readFileSync(path.join(dir,f)),sha=b=>createHash('sha256').update(b).digest('hex'),write=(f,x)=>fs.writeFileSync(path.join(dir,f),JSON.stringify(x,null,2)+'\n');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const p=JSON.parse(read('COMPLETE_REVERSE_AND_INPUT_PINS.json'));
const tests=spawnSync(process.execPath,['--test',rel+'/retained-legacy-source.test.mjs'],{cwd:root,encoding:'utf8',maxBuffer:3e6});
assert.equal(tests.status,0,tests.stdout+tests.stderr);assert.match(tests.stdout,/tests 12/);assert.match(tests.stdout,/pass 12/);
const preserved=[];
for(const packet of ['paid-match-legacy-native-consumer-construction-20261014','paid-match-correct-final-choice-construction-20261014','qualification-physical-contract-identity-successor-20261014','paid-match-retained-current-capture-construction-20261014']){
 const base='docs/healthroster-build/proposals/'+packet+'/',mraw=fs.readFileSync(path.join(root,base,'FROZEN_ARTIFACT_MANIFEST.json'));
 assert.deepEqual(mraw,execFileSync('git',['show',p.commit+':'+base+'FROZEN_ARTIFACT_MANIFEST.json'],{cwd:root,maxBuffer:3e6}));
 const m=JSON.parse(mraw);for(const a of m.artifacts){const b=fs.readFileSync(path.join(root,base,a.file));assert.equal(b.length,a.bytes);assert.equal(sha(b),a.sha256);}
 preserved.push({packet,artifacts:m.artifact_count,manifest_sha256:sha(mraw)});
}
write('SELECTED_SOURCE_RECIPE.json',{source_only:true,independent_review:'PENDING',containing_checkpoint:p.commit,controlling_native_commit:p.native_input.commit,selected_complete_owner:'forward/01_complete_retained_transition_assert.sqlinc',replaces_complete_leaf:p.source,prerequisites:p.dependencies,existing_signature:'private.weekly_source_paid_match_retained_transition_assert_v1(uuid)',new_private_signatures:[],new_public_signatures:[],new_store_column_calculator:0,NEXT_map_predicate_byte_exact:true,Legacy_native_capture_remains_inactive:true,return_ABI_unchanged:true,no_new_cash_or_writer_permission:true,preserved,root_Report_Office_U3_Undo_current_union_required:true,SQL_compiled:false,native_proof:false,canonical_mounted:false,whole_primary_complete:false});
const files=[];function walk(prefix=''){for(const e of fs.readdirSync(path.join(dir,prefix),{withFileTypes:true})){const f=prefix?prefix+'/'+e.name:e.name;if(e.isDirectory())walk(f);else files.push(f);}}walk();
let syntax=0;for(const f of files.filter(f=>f.endsWith('.mjs'))){const r=spawnSync(process.execPath,['--check',rel+'/'+f],{cwd:root,encoding:'utf8'});assert.equal(r.status,0,r.stderr);syntax++;}
const rx=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/\bgh[pousr]_[A-Za-z0-9]{20,}\b/,/\bAKIA[0-9A-Z]{16}\b/,/\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/];
const whitespace=[];for(const f of files){const s=read(f).toString();assert.ok(!rx.some(r=>r.test(s)),'credential '+f);s.split(/\r?\n/).forEach((line,i)=>{if(/[ \t]+$/.test(line))whitespace.push({file:f,line:i+1});});if(/(?:\r?\n){2,}$/.test(s))whitespace.push({file:f,kind:'PINNED NATIVE EOF BLANK'});}
write('SOURCE_CHECK_RESULT.json',{source_only:true,independent_review:'PENDING',cases:12,pass:12,fail:0,syntax_pass:syntax,whole_owner_reverse:1,unique_hunks:p.hunks.length,preserved_artifacts:preserved.reduce((n,x)=>n+x.artifacts,0),new_private_signatures:0,new_public_signatures:0,new_store_column_calculator:0,credential_findings:0,whitespace_disposition:whitespace.length?'PINNED NATIVE EXCEPTIONS RETAINED; NO BLANKET PASS':'PASS',whitespace_exceptions:whitespace,SQL_compiled:false,native_calls:0,canonical_mounted:false,whole_primary_complete:false,scratch_created:false,cleanup_retried:false});
files.push('SOURCE_CHECK_RESULT.json');const artifacts=[...new Set(files)].sort().map(file=>{const b=read(file);return{file,bytes:b.length,sha256:sha(b)};});
write('FROZEN_ARTIFACT_MANIFEST.json',{schema_version:'PAID_MATCH_LEGACY_RETAINED_BASIS_CONSUMER_SOURCE_FREEZE_V1',source_only:true,independent_review:'PENDING',native_proof:false,canonical_mounted:false,whole_primary_complete:false,artifact_count:artifacts.length,artifacts});
console.log(JSON.stringify({artifacts:artifacts.length,manifest_sha256:sha(read('FROZEN_ARTIFACT_MANIFEST.json')),check_sha256:sha(read('SOURCE_CHECK_RESULT.json')),recipe_sha256:sha(read('SELECTED_SOURCE_RECIPE.json')),cases:12,syntax,whole_reverse:1,hunks:p.hunks.length,after_sha256:p.after_sha256}));
