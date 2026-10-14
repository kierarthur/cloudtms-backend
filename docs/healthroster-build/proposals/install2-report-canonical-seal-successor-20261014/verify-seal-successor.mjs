// Effect-free source/hash compatibility checks; no constructor/native call.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const proof=JSON.parse(fs.readFileSync(path.join(here,'SEAL_CONSTRUCTOR_REVERSE.json'))),before=fs.readFileSync(path.join(root,proof.input)),after=fs.readFileSync(path.join(here,proof.output));
assert.equal(sha(before),proof.input_sha256);assert.equal(sha(after),proof.output_sha256);
let composed=before.toString('utf8');for(const h of proof.hunks){assert.equal(composed.split(h.old).length,2);composed=composed.replace(h.old,()=>h.new);}assert.equal(composed,after.toString('utf8'));
for(const h of [...proof.hunks].reverse()){assert.equal(composed.split(h.new).length,2);composed=composed.replace(h.new,()=>h.old);}assert.equal(composed,before.toString('utf8'));
for(const [name,pin] of [['install2-shared-runtime-forward-20261014',proof.prior35_manifest_sha256],['install2-report-canonical-integration-20261014',proof.canonical28_manifest_sha256]]){
 const directory=path.join(root,'docs/healthroster-build/proposals',name),bytes=fs.readFileSync(path.join(directory,'REVIEW_MANIFEST.json'));assert.equal(sha(bytes),pin);
 for(const row of JSON.parse(bytes).artifacts)assert.equal(sha(fs.readFileSync(path.join(directory,row.path))),row.sha256,name+' '+row.path);
}
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,maxBuffer:64*1024*1024});
const nativeOwner=proof.canonical_authority_hash_source,lib=git('show',nativeOwner.source_commit+':'+nativeOwner.path);
assert.equal(sha(lib),nativeOwner.sha256);assert.ok(lib.toString('utf8').includes('export function canonicalContractHash(contract) {\n  return sha256(JSON.stringify(contract));\n}'));
const contractPath='supabase/release/current-contract.json',raw=git('show',nativeOwner.source_commit+':'+contractPath),parsed=JSON.parse(raw),canonical=JSON.stringify(parsed),pretty=JSON.stringify(parsed,null,4)+'\n';
assert.notEqual(sha(raw),sha(canonical));assert.equal(sha(JSON.stringify(JSON.parse(pretty))),sha(canonical));
const text=after.toString('utf8');assert.ok(text.includes('const contractHash=sha(JSON.stringify(contract));'));
assert.ok(text.includes('catalogue.contract_sha256!==sha(JSON.stringify(contract))'));assert.ok(text.includes('contract_sha256:contractHash'));
assert.ok(text.includes('compiled_contract_raw_sha256:sha(contractBytes)'));assert.ok(!text.includes('contract_sha256:sha(contractBytes)'));
execFileSync(process.execPath,['--check',path.join(here,'build-native-seal.mjs')],{cwd:root});
const checks={schema_version:'INSTALL2_REPORT_SEAL_CANONICAL_SOURCE_CHECKS_V1',status:'SOURCE_ONLY_AWAITING_INDEPENDENT_REVIEW_ACTUAL_ROOT_COMPILE_CATALOGUE_TOOL_AND_NATIVE_SEAL',complete_constructor_reverse_hunks:proof.hunks.length,prior35_and_canonical28_all_manifest_hashes_unchanged:true,constructor_syntax_passed:true,dated_source_contract_vector:{source_commit:nativeOwner.source_commit,path:contractPath,raw_preparation_sha256:sha(raw),canonical_authority_formula_sha256:sha(canonical),formatting_changes_preserve_canonical_formula:true,not_an_install2_compile_or_acceptance:true},actual_future_execution_contract_sha256:null,literal_native_seals_changed:0,new_native_functions_or_gucs_or_slots_or_bundle_exceptions:0,no_native_execution:true};
fs.writeFileSync(path.join(here,'SOURCE_CHECK_RESULT.json'),JSON.stringify(checks,null,2)+'\n');console.log(JSON.stringify(checks));
